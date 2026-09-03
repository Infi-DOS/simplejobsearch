from __future__ import annotations

import sqlite3
from datetime import date, datetime
from typing import Any

from .config import get_settings
from .db import scalar
from .search.queries import sync_queries

TERMINAL_POST_AI_STATUSES = ("SHORTLIST", "REVIEW", "REJECT")


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
            SUM(CASE WHEN j.metadata_gate_status = 'PASS' THEN 1 ELSE 0 END) AS metadata_pass_count,
            SUM(CASE WHEN j.metadata_gate_status = 'REJECT' THEN 1 ELSE 0 END) AS metadata_reject_count,
            SUM(CASE WHEN j.ai_status = 'EXTRACTED' THEN 1 ELSE 0 END) AS ai_processed_count,
            SUM(CASE WHEN j.post_ai_status = 'SHORTLIST' THEN 1 ELSE 0 END) AS shortlist_count,
            SUM(CASE WHEN j.post_ai_status = 'REVIEW' THEN 1 ELSE 0 END) AS final_review_count,
            SUM(CASE WHEN j.post_ai_status = 'REJECT' THEN 1 ELSE 0 END) AS reject_count
        FROM daily_batch_jobs b
        JOIN jobs j ON j.job_id = b.job_id
        WHERE b.batch_id = ?
        """,
        (batch_id,),
    ).fetchone()
    keys = (
        "new_job_count", "review_required_count", "details_fetched_count",
        "metadata_pass_count", "metadata_reject_count", "ai_processed_count",
        "shortlist_count", "final_review_count", "reject_count",
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
            final_shortlist, final_review, final_reject, updated_at
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
            final_reject=excluded.final_reject, updated_at=excluded.updated_at
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
