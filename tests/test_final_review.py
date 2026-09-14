from __future__ import annotations

import sqlite3

import pytest

from simplejobsearch import config
from simplejobsearch.db import apply_migrations, database
from simplejobsearch.workflow import refresh_final_review_state, set_final_decisions


def _prepare(path) -> int:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE search_runs (
            run_id TEXT PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT
        );
        CREATE TABLE jobs (
            job_id TEXT PRIMARY KEY, classifier_status TEXT, human_decision TEXT,
            details_status TEXT, metadata_gate_status TEXT, ai_status TEXT,
            post_ai_status TEXT, first_seen_at TEXT
        );
        CREATE TABLE search_hits (
            run_id TEXT, job_id TEXT, search_name TEXT, observed_at TEXT,
            PRIMARY KEY (run_id, job_id, search_name)
        );
        """
    )
    connection.close()
    return 1


@pytest.fixture
def final_review_database(tmp_path, monkeypatch):
    path = tmp_path / "jobs.db"
    _prepare(path)
    monkeypatch.setenv("JOBSEARCH_DB_PATH", str(path))
    config.reset_settings_cache()
    apply_migrations()
    timestamp = "2026-09-03T12:00:00+02:00"
    with database() as connection:
        connection.execute(
            "INSERT INTO search_runs VALUES ('run-final', ?, ?)",
            (timestamp, timestamp),
        )
        connection.execute(
            """
            INSERT INTO daily_batches (
                batch_date, status, pipeline_completed_at, created_at, updated_at
            ) VALUES ('2026-09-03', 'COMPLETE', ?, ?, ?)
            """,
            (timestamp, timestamp, timestamp),
        )
        batch_id = connection.execute("SELECT batch_id FROM daily_batches").fetchone()[0]
        connection.execute(
            "UPDATE daily_batches SET search_run_id = 'run-final' WHERE batch_id = ?",
            (batch_id,),
        )
        connection.executemany(
            """
            INSERT INTO jobs (
                job_id, classifier_status, details_status, metadata_gate_status,
                ai_status, post_ai_status, first_seen_at
            ) VALUES (?, 'AUTO_KEEP', 'FETCHED', 'PASS', 'EXTRACTED', ?, ?)
            """,
            [
                ("short-1", "SHORTLIST", timestamp),
                ("short-2", "SHORTLIST", timestamp),
                ("review-1", "REVIEW", timestamp),
                ("auto-reject", "REJECT", timestamp),
            ],
        )
        connection.executemany(
            "INSERT INTO daily_batch_jobs VALUES (?, ?, 1, ?)",
            [
                (batch_id, job_id, timestamp)
                for job_id in ("short-1", "short-2", "review-1", "auto-reject")
            ],
        )
        connection.executemany(
            "INSERT INTO search_hits VALUES ('run-final', ?, 'ai', ?)",
            [
                (job_id, timestamp)
                for job_id in ("short-1", "short-2", "review-1", "auto-reject")
            ],
        )
        connection.commit()
    yield batch_id
    config.reset_settings_cache()


def test_final_review_requires_a_decision_for_every_post_ai_proposal(
    final_review_database,
):
    batch_id = final_review_database
    with database() as connection:
        assert refresh_final_review_state(connection, batch_id) == {
            "pending": 4,
            "accepted": 0,
            "rejected": 0,
            "status": "AWAITING_REVIEW",
        }
        first = set_final_decisions(connection, batch_id, ["short-1"], "accepted")
        assert first["saved"] == 1
        assert first["pending"] == 3
        assert first["status"] == "AWAITING_REVIEW"

        last = set_final_decisions(
            connection,
            batch_id,
            ["short-2", "review-1", "auto-reject"],
            "REJECTED",
        )
        assert last["pending"] == 0
        assert last["accepted"] == 1
        assert last["rejected"] == 3
        assert last["status"] == "FINALIZED"
        batch = connection.execute(
            """
            SELECT final_review_status, final_review_started_at,
                   final_review_completed_at, final_pending_count,
                   final_accepted_count, final_rejected_count
            FROM daily_batches WHERE batch_id = ?
            """,
            (batch_id,),
        ).fetchone()
        assert batch["final_review_status"] == "FINALIZED"
        assert batch["final_review_started_at"] is not None
        assert batch["final_review_completed_at"] is not None
        assert tuple(batch)[3:] == (0, 1, 3)
        completed_at = batch["final_review_completed_at"]
        assert refresh_final_review_state(connection, batch_id)["status"] == "FINALIZED"
        assert connection.execute(
            "SELECT final_review_completed_at FROM daily_batches WHERE batch_id = ?",
            (batch_id,),
        ).fetchone()[0] == completed_at
        metric = connection.execute(
            """
            SELECT human_final_accepted, human_final_rejected
            FROM search_query_metrics
            WHERE run_id = 'run-final' AND query_key = 'ai'
            """
        ).fetchone()
        assert tuple(metric) == (1, 3)


def test_final_decision_can_override_any_post_ai_proposal_and_be_changed(
    final_review_database,
):
    batch_id = final_review_database
    with database() as connection:
        overridden = set_final_decisions(
            connection,
            batch_id,
            ["auto-reject"],
            "ACCEPTED",
        )
        assert overridden["eligible"] == 1
        assert overridden["saved"] == 1
        reviewed = set_final_decisions(
            connection,
            batch_id,
            ["review-1"],
            "ACCEPTED",
        )
        assert reviewed["eligible"] == 1
        assert reviewed["saved"] == 1

        set_final_decisions(connection, batch_id, ["short-1"], "ACCEPTED")
        changed = set_final_decisions(connection, batch_id, ["short-1"], "REJECTED")
        assert changed["saved"] == 1
        assert connection.execute(
            "SELECT final_decision FROM jobs WHERE job_id = 'short-1'"
        ).fetchone()[0] == "REJECTED"

        with pytest.raises(ValueError, match="ACCEPTED, REJECTED"):
            set_final_decisions(connection, batch_id, ["short-2"], "MAYBE")


def test_incomplete_batch_cannot_record_a_final_decision(final_review_database):
    with database() as connection:
        timestamp = "2026-09-04T09:00:00+02:00"
        connection.execute(
            """
            INSERT INTO daily_batches (batch_date, status, created_at, updated_at)
            VALUES ('2026-09-04', 'PROCESSING', ?, ?)
            """,
            (timestamp, timestamp),
        )
        incomplete_id = connection.execute(
            "SELECT batch_id FROM daily_batches WHERE batch_date = '2026-09-04'"
        ).fetchone()[0]
        connection.execute(
            "INSERT INTO daily_batch_jobs VALUES (?, 'short-1', 0, ?)",
            (incomplete_id, timestamp),
        )
        connection.commit()

        result = set_final_decisions(
            connection,
            incomplete_id,
            ["short-1"],
            "ACCEPTED",
        )

        assert result["eligible"] == 0
        assert result["saved"] == 0
        assert result["status"] == "NOT_STARTED"
        assert connection.execute(
            "SELECT final_decision FROM jobs WHERE job_id = 'short-1'"
        ).fetchone()[0] is None
