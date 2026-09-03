from __future__ import annotations

import sqlite3

import pytest

from simplejobsearch import cli, config
from simplejobsearch.pipeline import orchestrator
from simplejobsearch.workflow import (
    count_unfinished_ai_jobs,
    count_unfinished_pipeline_jobs,
)

TIMESTAMP = "2026-08-31T00:00:00+02:00"


def prepare_recovery_database(path, *, status: str) -> int:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE jobs (
            job_id TEXT PRIMARY KEY,
            classifier_status TEXT,
            human_decision TEXT,
            details_status TEXT,
            metadata_gate_status TEXT,
            ai_status TEXT,
            ai_attempt_count INTEGER NOT NULL DEFAULT 0,
            ai_last_error TEXT,
            post_ai_status TEXT,
            first_seen_at TEXT
        );
        CREATE TABLE daily_batches (
            batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_date TEXT NOT NULL UNIQUE,
            search_run_id TEXT,
            status TEXT NOT NULL,
            search_started_at TEXT,
            search_completed_at TEXT,
            review_completed_at TEXT,
            pipeline_started_at TEXT,
            pipeline_completed_at TEXT,
            new_job_count INTEGER NOT NULL DEFAULT 0,
            review_required_count INTEGER NOT NULL DEFAULT 0,
            details_fetched_count INTEGER NOT NULL DEFAULT 0,
            metadata_pass_count INTEGER NOT NULL DEFAULT 0,
            metadata_reject_count INTEGER NOT NULL DEFAULT 0,
            ai_processed_count INTEGER NOT NULL DEFAULT 0,
            shortlist_count INTEGER NOT NULL DEFAULT 0,
            final_review_count INTEGER NOT NULL DEFAULT 0,
            reject_count INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE daily_batch_jobs (
            batch_id INTEGER NOT NULL,
            job_id TEXT NOT NULL,
            is_new INTEGER NOT NULL DEFAULT 0,
            observed_at TEXT NOT NULL,
            PRIMARY KEY (batch_id, job_id)
        );
        """
    )
    connection.execute(
        """
        INSERT INTO daily_batches (
            batch_date, status, pipeline_started_at, pipeline_completed_at,
            created_at, updated_at
        ) VALUES ('2026-08-31', ?, ?, ?, ?, ?)
        """,
        (
            status,
            TIMESTAMP if status in {"FAILED", "COMPLETE"} else None,
            TIMESTAMP if status == "COMPLETE" else None,
            TIMESTAMP,
            TIMESTAMP,
        ),
    )
    batch_id = connection.execute("SELECT batch_id FROM daily_batches").fetchone()[0]
    connection.commit()
    connection.close()
    return batch_id


def add_job(
    path,
    batch_id: int,
    job_id: str,
    *,
    details: str = "FETCHED",
    metadata: str = "PASS",
    ai: str = "EXTRACTED",
    post_ai: str | None = "REJECT",
    attempts: int = 1,
    attach: bool = True,
) -> None:
    connection = sqlite3.connect(path)
    connection.execute(
        """
        INSERT INTO jobs (
            job_id, classifier_status, details_status, metadata_gate_status,
            ai_status, ai_attempt_count, post_ai_status, first_seen_at
        ) VALUES (?, 'AUTO_KEEP', ?, ?, ?, ?, ?, ?)
        """,
        (job_id, details, metadata, ai, attempts, post_ai, TIMESTAMP),
    )
    if attach:
        connection.execute(
            "INSERT INTO daily_batch_jobs VALUES (?, ?, 1, ?)",
            (batch_id, job_id, TIMESTAMP),
        )
    connection.commit()
    connection.close()


@pytest.fixture
def recovery_database(tmp_path, monkeypatch):
    path = tmp_path / "jobs.db"
    monkeypatch.setenv("JOBSEARCH_DB_PATH", str(path))
    config.reset_settings_cache()
    monkeypatch.setattr(orchestrator, "apply_migrations", list)
    yield path
    config.reset_settings_cache()


def empty_stage(**_kwargs):
    return {
        "processed": 0,
        "fetched": 0,
        "failed": 0,
        "attempted": 0,
        "extracted": 0,
        "PASS": 0,
        "REJECT": 0,
        "SHORTLIST": 0,
        "REVIEW": 0,
    }


def test_unfinished_eligible_job_prevents_complete(recovery_database):
    path = recovery_database
    batch_id = prepare_recovery_database(path, status="READY_TO_CONTINUE")
    for index in range(10):
        add_job(path, batch_id, f"done-{index}")
    add_job(path, batch_id, "failed", ai="FAILED", post_ai=None)
    add_job(path, batch_id, "historical-unattached", ai="FAILED", post_ai=None, attach=False)
    emailed = []

    result = orchestrator.continue_after_review(
        batch_date="2026-08-31",
        details_stage=empty_stage,
        metadata_stage=empty_stage,
        ai_stage=empty_stage,
        post_ai_stage=empty_stage,
        email_sender=emailed.append,
    )

    assert result["status"] == "FAILED"
    assert result["this_run"]["ai_processed"] == 0
    assert result["this_run"]["ai_failed"] == 0
    assert result["batch_totals"]["ai_failed"] == 1
    assert result["batch_totals"]["unfinished"] == 1
    assert result["notification"]["status"] == "NOT_ATTEMPTED"
    assert emailed == []
    with orchestrator.database() as connection:
        row = connection.execute(
            "SELECT status, pipeline_completed_at, last_error FROM daily_batches"
        ).fetchone()
        assert row["status"] == "FAILED"
        assert row["pipeline_completed_at"] is None
        assert "1 approved pipeline job remains unfinished: failed" == row["last_error"]
        assert count_unfinished_ai_jobs(connection, batch_id) == 1


def test_approved_unfetched_job_prevents_complete(recovery_database):
    path = recovery_database
    batch_id = prepare_recovery_database(path, status="READY_TO_CONTINUE")
    add_job(path, batch_id, "unfetched", details="NOT_FETCHED")
    emailed = []

    result = orchestrator.continue_after_review(
        batch_date="2026-08-31",
        details_stage=empty_stage,
        metadata_stage=empty_stage,
        ai_stage=empty_stage,
        post_ai_stage=empty_stage,
        email_sender=emailed.append,
    )

    assert result["status"] == "FAILED"
    assert result["batch_totals"]["unfinished"] == 1
    assert result["notification"]["status"] == "NOT_ATTEMPTED"
    assert emailed == []
    with orchestrator.database() as connection:
        assert count_unfinished_pipeline_jobs(connection, batch_id) == 1


def test_failed_resume_retries_only_unfinished_and_completes(recovery_database):
    path = recovery_database
    batch_id = prepare_recovery_database(path, status="FAILED")
    add_job(path, batch_id, "already-done", post_ai="SHORTLIST", attempts=1)
    add_job(path, batch_id, "retry-me", ai="FAILED", post_ai=None, attempts=1)
    add_job(
        path,
        batch_id,
        "metadata-rejected",
        metadata="REJECT",
        ai="PENDING",
        post_ai=None,
        attempts=0,
    )
    ai_calls = []
    post_ai_calls = []
    emailed = []

    def retry_ai(*, job_ids):
        ai_calls.append(job_ids)
        with orchestrator.database() as connection:
            connection.execute(
                """
                UPDATE jobs
                SET ai_status='EXTRACTED', ai_attempt_count=ai_attempt_count + 1
                WHERE job_id='retry-me'
                """
            )
            connection.commit()
        return {"attempted": 1, "extracted": 1, "failed": 0}

    def finish_post_ai(*, job_ids):
        post_ai_calls.append(job_ids)
        with orchestrator.database() as connection:
            connection.execute(
                "UPDATE jobs SET post_ai_status='REVIEW' WHERE job_id='retry-me'"
            )
            connection.commit()
        return {"processed": 1, "SHORTLIST": 0, "REVIEW": 1, "REJECT": 0}

    result = orchestrator.continue_after_review(
        batch_date="2026-08-31",
        details_stage=empty_stage,
        metadata_stage=empty_stage,
        ai_stage=retry_ai,
        post_ai_stage=finish_post_ai,
        email_sender=emailed.append,
    )

    assert result["status"] == "COMPLETE"
    assert result["this_run"]["ai_processed"] == 1
    assert result["batch_totals"]["unfinished"] == 0
    assert ai_calls == [["retry-me"]]
    assert post_ai_calls == [["retry-me"]]
    assert emailed == [result]
    with orchestrator.database() as connection:
        rows = {
            row["job_id"]: (row["ai_status"], row["ai_attempt_count"], row["post_ai_status"])
            for row in connection.execute(
                "SELECT job_id, ai_status, ai_attempt_count, post_ai_status FROM jobs"
            )
        }
        batch = connection.execute(
            "SELECT status, pipeline_started_at, pipeline_completed_at, last_error "
            "FROM daily_batches"
        ).fetchone()
    assert rows["already-done"] == ("EXTRACTED", 1, "SHORTLIST")
    assert rows["retry-me"] == ("EXTRACTED", 2, "REVIEW")
    assert rows["metadata-rejected"] == ("PENDING", 0, None)
    assert batch["status"] == "COMPLETE"
    assert batch["pipeline_started_at"] == TIMESTAMP
    assert batch["pipeline_completed_at"] is not None
    assert batch["last_error"] is None

    completed_at = batch["pipeline_completed_at"]

    def must_not_run(**_kwargs):
        raise AssertionError("terminal work was reprocessed")

    no_op = orchestrator.continue_after_review(
        batch_date="2026-08-31",
        details_stage=must_not_run,
        metadata_stage=must_not_run,
        ai_stage=must_not_run,
        post_ai_stage=must_not_run,
        email_sender=lambda _summary: (_ for _ in ()).throw(
            AssertionError("completion email was duplicated")
        ),
    )
    assert no_op["already_complete"] is True
    with orchestrator.database() as connection:
        assert connection.execute(
            "SELECT pipeline_completed_at FROM daily_batches"
        ).fetchone()[0] == completed_at


def test_legacy_incomplete_complete_batch_is_recovered(recovery_database):
    path = recovery_database
    batch_id = prepare_recovery_database(path, status="COMPLETE")
    add_job(path, batch_id, "legacy-failed", ai="FAILED", post_ai=None)

    def retry_ai(*, job_ids):
        assert job_ids == ["legacy-failed"]
        with orchestrator.database() as connection:
            connection.execute(
                "UPDATE jobs SET ai_status='EXTRACTED' WHERE job_id='legacy-failed'"
            )
            connection.commit()
        return {"attempted": 1, "extracted": 1, "failed": 0}

    def finish_post_ai(*, job_ids):
        assert job_ids == ["legacy-failed"]
        with orchestrator.database() as connection:
            connection.execute(
                "UPDATE jobs SET post_ai_status='REJECT' WHERE job_id='legacy-failed'"
            )
            connection.commit()
        return {"processed": 1, "SHORTLIST": 0, "REVIEW": 0, "REJECT": 1}

    result = orchestrator.continue_after_review(
        batch_date="2026-08-31",
        details_stage=empty_stage,
        metadata_stage=empty_stage,
        ai_stage=retry_ai,
        post_ai_stage=finish_post_ai,
        email_sender=lambda _summary: True,
    )

    assert result["status"] == "COMPLETE"
    assert result["already_complete"] is False
    assert result["batch_totals"]["unfinished"] == 0


def test_continue_cli_returns_nonzero_for_incomplete_batch(monkeypatch, capsys):
    monkeypatch.setattr(
        cli,
        "continue_after_review",
        lambda: {"status": "FAILED", "message": "unfinished"},
    )

    assert cli.main(["continue"]) == 1
    assert '"status": "FAILED"' in capsys.readouterr().out
