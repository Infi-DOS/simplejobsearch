from __future__ import annotations

import sqlite3
from datetime import date, datetime
from typing import Any

from .config import get_settings
from .db import scalar
from .search.queries import sync_queries

TERMINAL_POST_AI_STATUSES = ("SHORTLIST", "REVIEW", "REJECT")
FINAL_DECISIONS = ("ACCEPTED", "REJECTED")


def now_local() -> datetime:
    return datetime.now(get_settings().timezone)


def now_iso() -> str:
    return now_local().isoformat()


def batch_date_value(value: date | str | None = None) -> str:
    if value is None:
        return now_local().date().isoformat()
    return value.isoformat() if isinstance(value, date) else value


def get_or_create_batch(
    connection: sqlite3.Connection,
    batch_date: date | str | None = None,
) -> sqlite3.Row:
    day = batch_date_value(batch_date)
    timestamp = now_iso()
    connection.execute(
        """
        INSERT INTO daily_batches (batch_date, status, created_at, updated_at)
        VALUES (?, 'SEARCH_PENDING', ?, ?)
        ON CONFLICT(batch_date) DO NOTHING
        """,
        (day, timestamp, timestamp),
    )
    connection.commit()
    return connection.execute(
        "SELECT * FROM daily_batches WHERE batch_date = ?", (day,)
    ).fetchone()


def latest_batch(connection: sqlite3.Connection) -> sqlite3.Row | None:
    return connection.execute(
        "SELECT * FROM daily_batches ORDER BY batch_date DESC, batch_id DESC LIMIT 1"
    ).fetchone()


def latest_run_batch(connection: sqlite3.Connection) -> sqlite3.Row | None:
    """Return the workflow batch attached to the latest reviewable search run."""
    search_run_columns = {
        row["name"] if isinstance(row, sqlite3.Row) else row[1]
        for row in connection.execute("PRAGMA table_info(search_runs)")
    }
    status_filter = (
        "WHERE sr.status IN ('SUCCESS', 'PARTIAL')"
        if "status" in search_run_columns
        else ""
    )
    return connection.execute(
        f"""
        SELECT db.*
        FROM daily_batches db
        JOIN search_runs sr ON sr.run_id = db.search_run_id
        {status_filter}
        ORDER BY sr.started_at DESC, db.batch_id DESC
        LIMIT 1
        """
    ).fetchone()


def resolve_batch(
    connection: sqlite3.Connection,
    batch_date: date | str | None = None,
) -> sqlite3.Row:
    if batch_date is not None:
        row = connection.execute(
            "SELECT * FROM daily_batches WHERE batch_date = ?",
            (batch_date_value(batch_date),),
        ).fetchone()
        if row is None:
            raise LookupError(f"No daily batch exists for {batch_date_value(batch_date)}")
        return row
    row = latest_run_batch(connection) or latest_batch(connection)
    if row is None:
        raise LookupError("No daily batch exists; run a search first")
    return row


def update_batch(connection: sqlite3.Connection, batch_id: int, **values: Any) -> None:
    if not values:
        return
    values["updated_at"] = now_iso()
    allowed = {
        "search_run_id", "status", "search_started_at", "search_completed_at",
        "review_completed_at", "pipeline_started_at", "pipeline_completed_at",
        "new_job_count", "review_required_count", "details_fetched_count",
        "metadata_pass_count", "metadata_reject_count", "ai_processed_count",
        "shortlist_count", "final_review_count", "reject_count", "last_error",
        "final_review_status", "final_review_started_at",
        "final_review_completed_at", "final_pending_count",
        "final_accepted_count", "final_rejected_count",
        "updated_at",
    }
    unknown = set(values) - allowed
    if unknown:
        raise ValueError(f"Unsupported daily batch fields: {sorted(unknown)}")
    assignments = ", ".join(f"{name} = ?" for name in values)
    connection.execute(
        f"UPDATE daily_batches SET {assignments} WHERE batch_id = ?",
        (*values.values(), batch_id),
    )
    connection.commit()


def attach_search_run(
    connection: sqlite3.Connection,
    batch_id: int,
    run_id: str,
) -> dict[str, int]:
    run = connection.execute(
        "SELECT started_at, finished_at FROM search_runs WHERE run_id = ?", (run_id,)
    ).fetchone()
    if run is None:
        raise LookupError(f"Unknown search run: {run_id}")
    observed_at = run[1] or now_iso()
    connection.execute(
        """
        INSERT INTO daily_batch_jobs (batch_id, job_id, is_new, observed_at)
        SELECT ?, h.job_id,
               CASE WHEN j.first_seen_at >= ? AND j.first_seen_at <= COALESCE(?, ?) THEN 1 ELSE 0 END,
               ?
        FROM search_hits h
        JOIN jobs j ON j.job_id = h.job_id
        WHERE h.run_id = ?
        GROUP BY h.job_id
        ON CONFLICT(batch_id, job_id) DO UPDATE SET
            is_new = MAX(daily_batch_jobs.is_new, excluded.is_new),
            observed_at = excluded.observed_at
        """,
        (batch_id, run[0], run[1], observed_at, observed_at, run_id),
    )
    counts = refresh_batch_counts(connection, batch_id)
    pending = counts["review_required_count"]
    update_batch(
        connection,
        batch_id,
        search_run_id=run_id,
        search_started_at=run[0],
        search_completed_at=run[1] or observed_at,
        status="WAITING_FOR_REVIEW" if pending else "READY_TO_CONTINUE",
        new_job_count=counts["new_job_count"],
        review_required_count=pending,
        review_completed_at=None if pending else observed_at,
        last_error=None,
    )
    return counts


def pending_review_count(connection: sqlite3.Connection, batch_id: int) -> int:
    return int(
        scalar(
            connection,
            """
            SELECT COUNT(*)
            FROM daily_batch_jobs b
            JOIN jobs j ON j.job_id = b.job_id
            WHERE b.batch_id = ? AND b.is_new = 1
              AND j.classifier_status = 'REVIEW'
              AND (j.human_decision IS NULL OR TRIM(j.human_decision) = '')
            """,
            (batch_id,),
        )
        or 0
    )


def refresh_batch_counts(connection: sqlite3.Connection, batch_id: int) -> dict[str, int]:
    row = connection.execute(
        """
        SELECT
            SUM(CASE WHEN b.is_new = 1 THEN 1 ELSE 0 END) AS new_job_count,
            SUM(CASE WHEN b.is_new = 1 AND j.classifier_status = 'REVIEW'
                      AND (j.human_decision IS NULL OR TRIM(j.human_decision) = '')
                     THEN 1 ELSE 0 END) AS review_required_count,
            SUM(CASE WHEN j.details_status = 'FETCHED' THEN 1 ELSE 0 END) AS details_fetched_count,
            SUM(CASE WHEN j.details_status = 'UNAVAILABLE' THEN 1 ELSE 0 END)
                AS details_unavailable_count,
            SUM(CASE WHEN j.metadata_gate_status = 'PASS' THEN 1 ELSE 0 END) AS metadata_pass_count,
            SUM(CASE WHEN j.metadata_gate_status = 'REJECT' THEN 1 ELSE 0 END) AS metadata_reject_count,
            SUM(CASE WHEN j.ai_status = 'EXTRACTED' THEN 1 ELSE 0 END) AS ai_processed_count,
            SUM(CASE WHEN j.post_ai_status = 'SHORTLIST' THEN 1 ELSE 0 END) AS shortlist_count,
            SUM(CASE WHEN j.post_ai_status = 'REVIEW' THEN 1 ELSE 0 END) AS final_review_count,
            SUM(CASE WHEN j.post_ai_status = 'REJECT' THEN 1 ELSE 0 END) AS reject_count,
            SUM(CASE WHEN j.post_ai_status IN ('SHORTLIST', 'REVIEW', 'REJECT')
                      AND (j.final_decision IS NULL OR TRIM(j.final_decision) = '')
                     THEN 1 ELSE 0 END) AS final_pending_count,
            SUM(CASE WHEN j.final_decision = 'ACCEPTED' THEN 1 ELSE 0 END)
                AS final_accepted_count,
            SUM(CASE WHEN j.final_decision = 'REJECTED' THEN 1 ELSE 0 END)
                AS final_rejected_count
        FROM daily_batch_jobs b
        JOIN jobs j ON j.job_id = b.job_id
        WHERE b.batch_id = ?
        """,
        (batch_id,),
    ).fetchone()
    keys = (
        "new_job_count", "review_required_count", "details_fetched_count",
        "details_unavailable_count",
        "metadata_pass_count", "metadata_reject_count", "ai_processed_count",
        "shortlist_count", "final_review_count", "reject_count",
        "final_pending_count", "final_accepted_count", "final_rejected_count",
    )
    counts = {key: int(row[key] or 0) for key in keys}
    counts["ai_failed_count"] = count_failed_ai_jobs(connection, batch_id)
    counts["unfinished_count"] = count_unfinished_pipeline_jobs(connection, batch_id)
    return counts


def unfinished_ai_job_ids(
    connection: sqlite3.Connection,
    batch_id: int,
) -> list[str]:
    """Return AI-eligible batch members without a terminal POST_AI result."""
    placeholders = ", ".join("?" for _ in TERMINAL_POST_AI_STATUSES)
    return [
        row[0]
        for row in connection.execute(
            f"""
            SELECT b.job_id
            FROM daily_batch_jobs b
            JOIN jobs j ON j.job_id = b.job_id
            WHERE b.batch_id = ?
              AND j.details_status = 'FETCHED'
              AND j.metadata_gate_status = 'PASS'
              AND (
                    j.post_ai_status IS NULL
                    OR TRIM(j.post_ai_status) = ''
                    OR j.post_ai_status NOT IN ({placeholders})
              )
            ORDER BY b.observed_at, b.job_id
            """,
            (batch_id, *TERMINAL_POST_AI_STATUSES),
        )
    ]


def count_unfinished_ai_jobs(connection: sqlite3.Connection, batch_id: int) -> int:
    """Count unfinished AI-eligible jobs using daily-batch membership."""
    return len(unfinished_ai_job_ids(connection, batch_id))


def unfinished_pipeline_job_ids(
    connection: sqlite3.Connection,
    batch_id: int,
) -> list[str]:
    """Return approved batch jobs that have not reached a terminal outcome."""
    placeholders = ", ".join("?" for _ in TERMINAL_POST_AI_STATUSES)
    return [
        row[0]
        for row in connection.execute(
            f"""
            SELECT b.job_id
            FROM daily_batch_jobs b
            JOIN jobs j ON j.job_id = b.job_id
            WHERE b.batch_id = ?
              AND (
                    UPPER(TRIM(COALESCE(j.human_decision, ''))) = 'KEEP'
                    OR (
                        TRIM(COALESCE(j.human_decision, '')) = ''
                        AND j.classifier_status = 'AUTO_KEEP'
                    )
              )
              AND (
                    UPPER(TRIM(COALESCE(j.details_status, ''))) <> 'UNAVAILABLE'
                    AND (
                    UPPER(TRIM(COALESCE(j.details_status, ''))) <> 'FETCHED'
                    OR UPPER(TRIM(COALESCE(j.metadata_gate_status, '')))
                       NOT IN ('PASS', 'REJECT')
                    OR (
                        UPPER(TRIM(COALESCE(j.metadata_gate_status, ''))) = 'PASS'
                        AND (
                            j.post_ai_status IS NULL
                            OR TRIM(j.post_ai_status) = ''
                            OR j.post_ai_status NOT IN ({placeholders})
                        )
                    )
                    )
              )
            ORDER BY b.observed_at, b.job_id
            """,
            (batch_id, *TERMINAL_POST_AI_STATUSES),
        )
    ]


def count_unfinished_pipeline_jobs(
    connection: sqlite3.Connection,
    batch_id: int,
) -> int:
    return len(unfinished_pipeline_job_ids(connection, batch_id))


def final_review_counts(
    connection: sqlite3.Connection,
    batch_id: int,
) -> dict[str, int]:
    row = connection.execute(
        """
        SELECT
            SUM(CASE WHEN j.post_ai_status IN ('SHORTLIST', 'REVIEW', 'REJECT')
                      AND (j.final_decision IS NULL OR TRIM(j.final_decision) = '')
                     THEN 1 ELSE 0 END) AS pending,
            SUM(CASE WHEN j.final_decision = 'ACCEPTED' THEN 1 ELSE 0 END) AS accepted,
            SUM(CASE WHEN j.final_decision = 'REJECTED' THEN 1 ELSE 0 END) AS rejected
        FROM daily_batch_jobs b
        JOIN jobs j ON j.job_id = b.job_id
        WHERE b.batch_id = ?
        """,
        (batch_id,),
    ).fetchone()
    return {
        "pending": int(row["pending"] or 0),
        "accepted": int(row["accepted"] or 0),
        "rejected": int(row["rejected"] or 0),
    }


def refresh_final_review_state(
    connection: sqlite3.Connection,
    batch_id: int,
) -> dict[str, Any]:
    counts = final_review_counts(connection, batch_id)
    current = connection.execute(
        """
        SELECT status, final_review_started_at, final_review_completed_at
        FROM daily_batches
        WHERE batch_id = ?
        """,
        (batch_id,),
    ).fetchone()
    if current is None:
        raise LookupError(f"Unknown daily batch: {batch_id}")
    if current["status"] != "COMPLETE":
        update_batch(
            connection,
            batch_id,
            final_review_status="NOT_STARTED",
            final_review_started_at=None,
            final_review_completed_at=None,
            final_pending_count=counts["pending"],
            final_accepted_count=counts["accepted"],
            final_rejected_count=counts["rejected"],
        )
        return {**counts, "status": "NOT_STARTED"}
    timestamp = now_iso()
    pending = counts["pending"]
    status = "AWAITING_REVIEW" if pending else "FINALIZED"
    update_batch(
        connection,
        batch_id,
        final_review_status=status,
        final_review_started_at=current["final_review_started_at"] or timestamp,
        final_review_completed_at=(
            None
            if pending
            else current["final_review_completed_at"] or timestamp
        ),
        final_pending_count=pending,
        final_accepted_count=counts["accepted"],
        final_rejected_count=counts["rejected"],
    )
    return {**counts, "status": status}


def refresh_all_final_review_states(
    connection: sqlite3.Connection,
) -> dict[str, int]:
    states = {"NOT_STARTED": 0, "AWAITING_REVIEW": 0, "FINALIZED": 0}
    batch_ids = [
        row[0]
        for row in connection.execute(
            "SELECT batch_id FROM daily_batches ORDER BY batch_id"
        )
    ]
    for batch_id in batch_ids:
        state = refresh_final_review_state(connection, batch_id)
        states[state["status"]] += 1
    return states


def set_final_decisions(
    connection: sqlite3.Connection,
    batch_id: int,
    job_ids: list[str],
    decision: str,
    *,
    notes: str | None = None,
) -> dict[str, Any]:
    normalized = decision.strip().upper()
    if normalized not in FINAL_DECISIONS:
        raise ValueError(f"Final decision must be one of: {', '.join(FINAL_DECISIONS)}")
    selected = list(dict.fromkeys(job_ids))
    if not selected:
        state = refresh_final_review_state(connection, batch_id)
        return {"requested": 0, "saved": 0, **state}

    placeholders = ", ".join("?" for _ in selected)
    eligible = connection.execute(
        f"""
        SELECT j.job_id, j.final_decision, j.final_notes
        FROM daily_batch_jobs b
        JOIN jobs j ON j.job_id = b.job_id
        JOIN daily_batches d ON d.batch_id = b.batch_id
        WHERE b.batch_id = ?
          AND d.status = 'COMPLETE'
          AND j.job_id IN ({placeholders})
          AND (
                j.post_ai_status IN ('SHORTLIST', 'REVIEW', 'REJECT')
                OR j.final_decision IN ('ACCEPTED', 'REJECTED')
          )
        """,
        (batch_id, *selected),
    ).fetchall()
    timestamp = now_iso()
    changed_ids = [
        row["job_id"]
        for row in eligible
        if row["final_decision"] != normalized
        or (notes is not None and (row["final_notes"] or "") != notes)
    ]
    if changed_ids:
        changed_placeholders = ", ".join("?" for _ in changed_ids)
        connection.execute(
            f"""
            UPDATE jobs
            SET final_decision = ?,
                final_decided_at = ?,
                final_notes = CASE WHEN ? IS NULL THEN final_notes ELSE ? END
            WHERE job_id IN ({changed_placeholders})
            """,
            (normalized, timestamp, notes, notes, *changed_ids),
        )
        connection.commit()
        affected_batch_ids = [
            row[0]
            for row in connection.execute(
                f"""
                SELECT DISTINCT batch_id
                FROM daily_batch_jobs
                WHERE job_id IN ({changed_placeholders})
                """,
                changed_ids,
            )
        ]
        for affected_batch_id in affected_batch_ids:
            refresh_final_review_state(connection, affected_batch_id)
            run_id = connection.execute(
                "SELECT search_run_id FROM daily_batches WHERE batch_id = ?",
                (affected_batch_id,),
            ).fetchone()[0]
            if run_id:
                record_query_metrics(connection, run_id)
    state = refresh_final_review_state(connection, batch_id)
    return {
        "requested": len(selected),
        "eligible": len(eligible),
        "saved": len(changed_ids),
        **state,
    }


def count_failed_ai_jobs(connection: sqlite3.Connection, batch_id: int) -> int:
    """Count unfinished eligible jobs whose latest AI attempt failed."""
    placeholders = ", ".join("?" for _ in TERMINAL_POST_AI_STATUSES)
    return int(
        scalar(
            connection,
            f"""
            SELECT COUNT(*)
            FROM daily_batch_jobs b
            JOIN jobs j ON j.job_id = b.job_id
            WHERE b.batch_id = ?
              AND j.details_status = 'FETCHED'
              AND j.metadata_gate_status = 'PASS'
              AND j.ai_status = 'FAILED'
              AND (
                    j.post_ai_status IS NULL
                    OR TRIM(j.post_ai_status) = ''
                    OR j.post_ai_status NOT IN ({placeholders})
              )
            """,
            (batch_id, *TERMINAL_POST_AI_STATUSES),
        )
        or 0
    )


def mark_review_state(connection: sqlite3.Connection, batch_id: int) -> int:
    pending = pending_review_count(connection, batch_id)
    current = connection.execute(
        "SELECT status FROM daily_batches WHERE batch_id = ?", (batch_id,)
    ).fetchone()
    if current and current[0] in {"WAITING_FOR_REVIEW", "READY_TO_CONTINUE"}:
        update_batch(
            connection,
            batch_id,
            status="WAITING_FOR_REVIEW" if pending else "READY_TO_CONTINUE",
            review_required_count=pending,
            review_completed_at=None if pending else now_iso(),
        )
    return pending


def reconcile_batch_summaries(connection: sqlite3.Connection) -> list[dict]:
    """Refresh derived batch state from jobs without fetching or deciding jobs."""
    refreshed = []
    batches = connection.execute("SELECT * FROM daily_batches ORDER BY batch_id").fetchall()
    for batch in batches:
        if batch["status"] in {"SEARCH_PENDING", "SEARCHING", "PROCESSING"}:
            continue
        if (batch["status"] == "FAILED" and not batch["search_run_id"]
                and not batch["pipeline_started_at"]):
            # A discovery failure is not a reviewed batch ready for continuation.
            continue
        batch_id = batch["batch_id"]
        counts = refresh_batch_counts(connection, batch_id)
        pending = pending_review_count(connection, batch_id)
        unfinished = unfinished_pipeline_job_ids(connection, batch_id)
        if pending:
            status = "WAITING_FOR_REVIEW"
        elif unfinished:
            status = "FAILED" if batch["pipeline_started_at"] else "READY_TO_CONTINUE"
        else:
            status = "COMPLETE" if batch["pipeline_started_at"] else "READY_TO_CONTINUE"
        values = {key: value for key, value in counts.items() if key not in {
            "details_unavailable_count", "ai_failed_count", "unfinished_count",
        }}
        values.update(
            status=status,
            review_required_count=pending,
            review_completed_at=None if pending else (batch["review_completed_at"] or now_iso()),
            pipeline_completed_at=(batch["pipeline_completed_at"] or now_iso())
            if status == "COMPLETE" else None,
            last_error=f"{len(unfinished)} approved pipeline jobs remain unfinished"
            if status == "FAILED" else None,
        )
        update_batch(connection, batch_id, **values)
        refresh_final_review_state(connection, batch_id)
        if batch["search_run_id"]:
            record_query_metrics(connection, batch["search_run_id"])
        refreshed.append({
            "batch_id": batch_id, "batch_date": batch["batch_date"], "status": status,
            "pending_title_reviews": pending, "unfinished": len(unfinished),
        })
    return refreshed


def record_query_metrics(connection: sqlite3.Connection, run_id: str) -> None:
    timestamp = now_iso()
    sync_queries(connection, timestamp)
    run = connection.execute(
        "SELECT started_at, COALESCE(finished_at, ?) FROM search_runs WHERE run_id = ?",
        (timestamp, run_id),
    ).fetchone()
    if run is None:
        raise LookupError(f"Unknown search run: {run_id}")
    connection.execute(
        """
        INSERT INTO search_query_metrics (
            run_id, query_key, hits, unique_jobs, new_jobs,
            auto_keep, review, auto_exclude,
            metadata_pass, metadata_reject,
            final_shortlist, final_review, final_reject,
            human_final_accepted, human_final_rejected, updated_at
        )
        SELECT h.run_id, h.search_name,
               COUNT(*), COUNT(DISTINCT h.job_id),
               COUNT(DISTINCT CASE WHEN j.first_seen_at >= ? AND j.first_seen_at <= ? THEN h.job_id END),
               COUNT(DISTINCT CASE WHEN j.classifier_status = 'AUTO_KEEP' THEN h.job_id END),
               COUNT(DISTINCT CASE WHEN j.classifier_status = 'REVIEW' THEN h.job_id END),
               COUNT(DISTINCT CASE WHEN j.classifier_status = 'AUTO_EXCLUDE' THEN h.job_id END),
               COUNT(DISTINCT CASE WHEN j.metadata_gate_status = 'PASS' THEN h.job_id END),
               COUNT(DISTINCT CASE WHEN j.metadata_gate_status = 'REJECT' THEN h.job_id END),
               COUNT(DISTINCT CASE WHEN j.post_ai_status = 'SHORTLIST' THEN h.job_id END),
               COUNT(DISTINCT CASE WHEN j.post_ai_status = 'REVIEW' THEN h.job_id END),
               COUNT(DISTINCT CASE WHEN j.post_ai_status = 'REJECT' THEN h.job_id END),
               COUNT(DISTINCT CASE WHEN j.final_decision = 'ACCEPTED' THEN h.job_id END),
               COUNT(DISTINCT CASE WHEN j.final_decision = 'REJECTED' THEN h.job_id END),
               ?
        FROM search_hits h
        JOIN jobs j ON j.job_id = h.job_id
        JOIN search_queries q ON q.query_key = h.search_name
        WHERE h.run_id = ?
        GROUP BY h.run_id, h.search_name
        ON CONFLICT(run_id, query_key) DO UPDATE SET
            hits=excluded.hits, unique_jobs=excluded.unique_jobs, new_jobs=excluded.new_jobs,
            auto_keep=excluded.auto_keep, review=excluded.review, auto_exclude=excluded.auto_exclude,
            metadata_pass=excluded.metadata_pass, metadata_reject=excluded.metadata_reject,
            final_shortlist=excluded.final_shortlist, final_review=excluded.final_review,
            final_reject=excluded.final_reject,
            human_final_accepted=excluded.human_final_accepted,
            human_final_rejected=excluded.human_final_rejected,
            updated_at=excluded.updated_at
        """,
        (run[0], run[1], timestamp, run_id),
    )
    connection.commit()


def batch_job_ids(connection: sqlite3.Connection, batch_id: int) -> list[str]:
    return [
        row[0]
        for row in connection.execute(
            "SELECT job_id FROM daily_batch_jobs WHERE batch_id = ? ORDER BY observed_at, job_id",
            (batch_id,),
        )
    ]


def approved_prefetch_backlog_count(
    connection: sqlite3.Connection,
    batch_id: int,
    *,
    max_detail_attempts: int,
) -> int:
    """Count fetchable approved jobs that are not yet part of this batch."""
    return int(
        scalar(
            connection,
            """
            SELECT COUNT(*)
            FROM jobs j
            WHERE (
                    UPPER(TRIM(COALESCE(j.human_decision, ''))) = 'KEEP'
                    OR (
                        TRIM(COALESCE(j.human_decision, '')) = ''
                        AND j.classifier_status = 'AUTO_KEEP'
                    )
                  )
              AND (
                    j.details_status IS NULL
                    OR TRIM(j.details_status) = ''
                    OR j.details_status IN ('NOT_FETCHED', 'FAILED_OR_EMPTY')
                  )
              AND LOWER(COALESCE(j.site, '')) = 'linkedin'
              AND COALESCE(j.details_attempt_count, 0) < ?
              AND NOT EXISTS (
                    SELECT 1
                    FROM daily_batch_jobs b
                    WHERE b.batch_id = ? AND b.job_id = j.job_id
                  )
            """,
            (max_detail_attempts, batch_id),
        )
        or 0
    )


def attach_approved_prefetch_backlog(
    connection: sqlite3.Connection,
    batch_id: int,
    *,
    max_detail_attempts: int,
) -> int:
    """Attach all fetchable approved pre-fetch jobs to one resumable batch."""
    observed_at = now_iso()
    before = connection.total_changes
    connection.execute(
        """
        INSERT INTO daily_batch_jobs (batch_id, job_id, is_new, observed_at)
        SELECT ?, j.job_id, 0, ?
        FROM jobs j
        WHERE (
                UPPER(TRIM(COALESCE(j.human_decision, ''))) = 'KEEP'
                OR (
                    TRIM(COALESCE(j.human_decision, '')) = ''
                    AND j.classifier_status = 'AUTO_KEEP'
                )
              )
          AND (
                j.details_status IS NULL
                OR TRIM(j.details_status) = ''
                OR j.details_status IN ('NOT_FETCHED', 'FAILED_OR_EMPTY')
              )
          AND LOWER(COALESCE(j.site, '')) = 'linkedin'
          AND COALESCE(j.details_attempt_count, 0) < ?
          AND NOT EXISTS (
                SELECT 1
                FROM daily_batch_jobs b
                WHERE b.batch_id = ? AND b.job_id = j.job_id
              )
        """,
        (batch_id, observed_at, max_detail_attempts, batch_id),
    )
    attached = connection.total_changes - before
    connection.commit()
    return attached


def recover_interrupted_ai_jobs(
    connection: sqlite3.Connection,
    batch_id: int,
) -> int:
    """Make AI work abandoned in PROCESSING eligible for a safe retry."""
    placeholders = ", ".join("?" for _ in TERMINAL_POST_AI_STATUSES)
    job_columns = {
        row["name"] if isinstance(row, sqlite3.Row) else row[1]
        for row in connection.execute("PRAGMA table_info(jobs)")
    }
    assignments = "ai_status = 'FAILED'"
    if "ai_last_error" in job_columns:
        assignments += (
            ", ai_last_error = "
            "'Previous pipeline worker stopped during AI processing'"
        )
    before = connection.total_changes
    connection.execute(
        f"""
        UPDATE jobs
        SET {assignments}
        WHERE job_id IN (
                SELECT job_id FROM daily_batch_jobs WHERE batch_id = ?
              )
          AND ai_status = 'PROCESSING'
          AND (
                post_ai_status IS NULL
                OR TRIM(post_ai_status) = ''
                OR post_ai_status NOT IN ({placeholders})
              )
        """,
        (batch_id, *TERMINAL_POST_AI_STATUSES),
    )
    recovered = connection.total_changes - before
    connection.commit()
    return recovered


def batch_summary(connection: sqlite3.Connection, batch_id: int) -> dict[str, Any]:
    row = connection.execute(
        "SELECT * FROM daily_batches WHERE batch_id = ?", (batch_id,)
    ).fetchone()
    if row is None:
        raise LookupError(f"Unknown daily batch: {batch_id}")
    return dict(row)


def bootstrap_latest_batch(connection: sqlite3.Connection) -> sqlite3.Row | None:
    """Derive one workflow batch for an existing pre-refactor database."""
    current = latest_batch(connection)
    if current is not None:
        sync_queries(connection, now_iso())
        return current
    run = connection.execute(
        """
        SELECT run_id, started_at
        FROM search_runs
        ORDER BY started_at DESC
        LIMIT 1
        """
    ).fetchone()
    if run is None:
        sync_queries(connection, now_iso())
        return None
    batch = get_or_create_batch(connection, str(run["started_at"])[:10])
    attach_search_run(connection, batch["batch_id"], run["run_id"])
    record_query_metrics(connection, run["run_id"])
    return connection.execute(
        "SELECT * FROM daily_batches WHERE batch_id = ?", (batch["batch_id"],)
    ).fetchone()
