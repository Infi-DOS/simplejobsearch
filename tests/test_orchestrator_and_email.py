from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from html import unescape

from jobsimplesearch import cli, config
from jobsimplesearch.notifications import email
from jobsimplesearch.pipeline import orchestrator


def prepare_database(path):
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
            PRIMARY KEY (run_id, job_id, search_name),
            FOREIGN KEY (run_id) REFERENCES search_runs(run_id),
            FOREIGN KEY (job_id) REFERENCES jobs(job_id)
        );
        INSERT INTO search_runs VALUES ('run-1', '2026-08-30T00:00:00+02:00', '2026-08-30T00:01:00+02:00');
        INSERT INTO jobs VALUES (
            'li-1', 'AUTO_KEEP', NULL, NULL, NULL, NULL, NULL,
            '2026-08-30T00:00:30+02:00'
        );
        """
    )
    connection.close()


def test_orchestrator_calls_stages_in_order(tmp_path, monkeypatch):
    database = tmp_path / "jobs.db"
    prepare_database(database)
    monkeypatch.setenv("JOBSEARCH_DB_PATH", str(database))
    config.reset_settings_cache()
    orchestrator.apply_migrations()
    with orchestrator.database() as connection:
        timestamp = "2026-08-30T00:01:00+02:00"
        connection.execute(
            """
            INSERT INTO daily_batches (
                batch_date, search_run_id, status, created_at, updated_at
            ) VALUES ('2026-08-30', 'run-1', 'READY_TO_CONTINUE', ?, ?)
            """,
            (timestamp, timestamp),
        )
        batch_id = connection.execute("SELECT batch_id FROM daily_batches").fetchone()[0]
        connection.execute(
            "INSERT INTO daily_batch_jobs VALUES (?, 'li-1', 1, ?)",
            (batch_id, timestamp),
        )
        connection.commit()

    order = []

    def stage(name, result):
        def call(**_kwargs):
            order.append(name)
            return result
        return call

    result = orchestrator.continue_after_review(
        batch_date="2026-08-30",
        details_stage=stage("details", {"failed": 0}),
        metadata_stage=stage("metadata", {}),
        ai_stage=stage("ai", {"failed": 0}),
        post_ai_stage=stage("post_ai", {}),
        email_sender=lambda _summary: order.append("email"),
    )

    assert order == ["details", "metadata", "ai", "post_ai", "email"]
    assert result["batch_id"] == batch_id
    assert result["already_complete"] is False
    assert result["no_op"] is False
    assert result["this_run"]["ai_processed"] == 0
    assert result["batch_totals"]["ai_extracted"] == 0
    with orchestrator.database() as connection:
        assert connection.execute("SELECT status FROM daily_batches").fetchone()[0] == "COMPLETE"
    config.reset_settings_cache()


def test_email_disabled_never_crashes(monkeypatch):
    settings = config.get_settings()
    disabled = replace(settings.email, enabled=False)
    monkeypatch.setattr(email, "get_settings", lambda: replace(settings, email=disabled))
    assert email.send_search_complete_email({"new_jobs": 3}) is False
    assert email.send_review_reminder_email({"pending": 2}) is False
    assert email.send_pipeline_complete_email({"shortlist": 1}) is False


def test_all_three_smtp_messages_can_be_sent_in_isolation(monkeypatch):
    settings = config.get_settings()
    enabled = replace(
        settings.email,
        enabled=True,
        smtp_host="smtp.example.test",
        smtp_port=587,
        username="test-user",
        password="test-password",
        sender="jobs@example.test",
        recipients=("owner@example.test",),
        smtp_security="starttls",
    )
    messages = []
    smtp_events = []
    public_base_url = "https://jobs-example.ngrok-free.app"

    class FakeSMTP:
        def __init__(self, host, port, timeout):
            smtp_events.append(("connect", host, port, timeout))

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def starttls(self):
            smtp_events.append(("starttls",))

        def ehlo(self):
            smtp_events.append(("ehlo",))

        def login(self, username, password):
            smtp_events.append(("login", username, password))

        def send_message(self, message):
            messages.append(message)

    configured = replace(
        settings,
        email=enabled,
        web=replace(settings.web, public_base_url=public_base_url),
    )
    monkeypatch.setattr(email, "get_settings", lambda: configured)
    monkeypatch.setattr(email.smtplib, "SMTP", FakeSMTP)

    assert email.send_search_complete_email({"new_job_count": 3}) is True
    assert email.send_review_reminder_email({"pending_review_count": 2}) is True
    assert email.send_pipeline_complete_email(
        {
            "batch_id": 1,
            "batch_date": "2026-08-30",
            "status": "COMPLETE",
            "message": "Pipeline completed.",
            "batch_totals": {"ai_extracted": 8},
            "this_run": {"ai_processed": 0},
        }
    ) is True

    assert [message["Subject"] for message in messages] == [
        "Job search complete",
        "Job review reminder",
        "Job pipeline complete",
    ]
    assert sum(event[0] == "connect" for event in smtp_events) == 3
    assert sum(event[0] == "starttls" for event in smtp_events) == 3
    assert sum(event[0] == "ehlo" for event in smtp_events) == 6
    assert sum(event[0] == "login" for event in smtp_events) == 3
    expected_urls = [
        f"{public_base_url}/review",
        f"{public_base_url}/review",
        f"{public_base_url}/results",
    ]
    expected_actions = ["View today's jobs", "Review Jobs", "View Results"]
    for message, expected_url, expected_action in zip(
        messages,
        expected_urls,
        expected_actions,
        strict=True,
    ):
        assert message.get_content_type() == "multipart/alternative"
        plain = message.get_body(preferencelist=("plain",)).get_content()
        html = message.get_body(preferencelist=("html",)).get_content()
        assert expected_url in plain
        assert expected_url in html
        assert expected_action in unescape(html)


def test_missing_public_base_url_sends_multipart_email_without_link(
    monkeypatch, caplog
):
    settings = config.get_settings()
    enabled = replace(
        settings.email,
        enabled=True,
        smtp_host="smtp.example.test",
        smtp_port=25,
        username=None,
        password=None,
        sender="jobs@example.test",
        recipients=("owner@example.test",),
        smtp_security="none",
    )
    messages = []

    class FakeSMTP:
        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def ehlo(self):
            pass

        def send_message(self, message):
            messages.append(message)

    configured = replace(
        settings,
        email=enabled,
        web=replace(settings.web, public_base_url=None),
    )
    monkeypatch.setattr(email, "get_settings", lambda: configured)
    monkeypatch.setattr(email.smtplib, "SMTP", FakeSMTP)

    assert email.send_search_complete_email({"new_job_count": 3}) is True
    assert email.send_review_reminder_email({"pending_review_count": 2}) is True
    assert email.send_pipeline_complete_email(
        {"batch_totals": {"shortlist": 1, "review": 1, "reject": 0}}
    ) is True

    assert len(messages) == 3
    assert all(message.get_content_type() == "multipart/alternative" for message in messages)
    assert all("href=" not in message.get_body(preferencelist=("html",)).get_content() for message in messages)
    assert caplog.text.count("PUBLIC_BASE_URL is not configured") == 3


def test_completed_batch_is_idempotent(tmp_path, monkeypatch, capsys):
    database = tmp_path / "jobs.db"
    prepare_database(database)
    monkeypatch.setenv("JOBSEARCH_DB_PATH", str(database))
    config.reset_settings_cache()
    orchestrator.apply_migrations()
    timestamp = "2026-08-30T00:01:00+02:00"
    with orchestrator.database() as connection:
        connection.execute(
            """
            INSERT INTO daily_batches (
                batch_date, search_run_id, status, pipeline_started_at,
                pipeline_completed_at, created_at, updated_at
            ) VALUES ('2026-08-30', 'run-1', 'COMPLETE', ?, ?, ?, ?)
            """,
            (timestamp, timestamp, timestamp, timestamp),
        )
        batch_id = connection.execute("SELECT batch_id FROM daily_batches").fetchone()[0]
        connection.execute(
            "INSERT INTO daily_batch_jobs VALUES (?, 'li-1', 1, ?)",
            (batch_id, timestamp),
        )
        connection.commit()

    def must_not_run(**_kwargs):
        raise AssertionError("A completed batch re-entered the pipeline")

    summary = orchestrator.continue_after_review(
        batch_date="2026-08-30",
        details_stage=must_not_run,
        metadata_stage=must_not_run,
        ai_stage=must_not_run,
        post_ai_stage=must_not_run,
        email_sender=lambda _summary: (_ for _ in ()).throw(
            AssertionError("Completion email was resent")
        ),
    )

    assert summary["batch_id"] == batch_id
    assert summary["already_complete"] is True
    assert summary["no_op"] is True
    assert "No pipeline work performed" in summary["message"]
    assert all(value == 0 for value in summary["this_run"].values())

    assert cli.main(["continue"]) == 0
    cli_summary = json.loads(capsys.readouterr().out)
    assert cli_summary["already_complete"] is True
    assert cli_summary["no_op"] is True
    assert all(value == 0 for value in cli_summary["this_run"].values())

    with orchestrator.database() as connection:
        row = connection.execute(
            "SELECT status, pipeline_started_at, pipeline_completed_at FROM daily_batches"
        ).fetchone()
        assert tuple(row) == ("COMPLETE", timestamp, timestamp)
    config.reset_settings_cache()


def test_zero_ai_queue_reports_run_zero_and_existing_batch_total(
    tmp_path, monkeypatch
):
    database = tmp_path / "jobs.db"
    prepare_database(database)
    monkeypatch.setenv("JOBSEARCH_DB_PATH", str(database))
    config.reset_settings_cache()
    orchestrator.apply_migrations()
    timestamp = "2026-08-30T00:01:00+02:00"

    with orchestrator.database() as connection:
        connection.execute(
            """
            UPDATE jobs
            SET details_status = 'FETCHED', metadata_gate_status = 'PASS',
                ai_status = 'EXTRACTED', post_ai_status = 'SHORTLIST'
            WHERE job_id = 'li-1'
            """
        )
        for index in range(2, 9):
            outcome = "SHORTLIST" if index == 2 else "REVIEW" if index == 3 else "REJECT"
            connection.execute(
                """
                INSERT INTO jobs (
                    job_id, classifier_status, details_status,
                    metadata_gate_status, ai_status, post_ai_status, first_seen_at
                ) VALUES (?, 'AUTO_KEEP', 'FETCHED', 'PASS', 'EXTRACTED', ?, ?)
                """,
                (f"li-{index}", outcome, timestamp),
            )
        connection.execute(
            """
            INSERT INTO daily_batches (
                batch_date, search_run_id, status, created_at, updated_at
            ) VALUES ('2026-08-30', 'run-1', 'READY_TO_CONTINUE', ?, ?)
            """,
            (timestamp, timestamp),
        )
        batch_id = connection.execute("SELECT batch_id FROM daily_batches").fetchone()[0]
        connection.executemany(
            "INSERT INTO daily_batch_jobs VALUES (?, ?, 1, ?)",
            [(batch_id, f"li-{index}", timestamp) for index in range(1, 9)],
        )
        connection.commit()

    emailed = []
    result = orchestrator.continue_after_review(
        batch_date="2026-08-30",
        details_stage=lambda **_kwargs: {"fetched": 0, "failed": 0},
        metadata_stage=lambda **_kwargs: {
            "processed": 0,
            "PASS": 0,
            "REJECT": 0,
        },
        ai_stage=lambda **_kwargs: {
            "attempted": 0,
            "processed": 0,
            "extracted": 0,
            "failed": 0,
        },
        post_ai_stage=lambda **_kwargs: {
            "processed": 0,
            "SHORTLIST": 0,
            "REVIEW": 0,
            "REJECT": 0,
        },
        email_sender=emailed.append,
    )

    assert result["this_run"]["ai_processed"] == 0
    assert result["this_run"]["ai_extracted"] == 0
    assert result["batch_totals"]["ai_extracted"] == 8
    assert result["batch_totals"]["shortlist"] == 2
    assert result["batch_totals"]["review"] == 1
    assert result["batch_totals"]["reject"] == 5
    assert result["batch_totals"]["post_ai_classified"] == 8
    assert sum(
        result["batch_totals"][key] for key in ("shortlist", "review", "reject")
    ) == result["batch_totals"]["post_ai_classified"]
    assert emailed == [result]
    config.reset_settings_cache()


def test_pipeline_email_separates_batch_totals_from_run_activity():
    body = email._format_pipeline_summary(
        {
            "batch_id": 1,
            "batch_date": "2026-08-30",
            "status": "COMPLETE",
            "message": "Pipeline completed.",
            "batch_totals": {"ai_extracted": 8, "shortlist": 2},
            "this_run": {"ai_processed": 0, "ai_extracted": 0},
        }
    )

    assert body.index("Batch totals:") < body.index("Activity this invocation:")
    assert "Ai Extracted: 8" in body
    assert "Ai Processed: 0" in body


def test_pipeline_email_failure_does_not_change_completed_batch(tmp_path, monkeypatch):
    database = tmp_path / "jobs.db"
    prepare_database(database)
    monkeypatch.setenv("JOBSEARCH_DB_PATH", str(database))
    config.reset_settings_cache()
    orchestrator.apply_migrations()
    timestamp = "2026-08-30T00:01:00+02:00"
    with orchestrator.database() as connection:
        connection.execute(
            """
            INSERT INTO daily_batches (
                batch_date, search_run_id, status, created_at, updated_at
            ) VALUES ('2026-08-30', 'run-1', 'READY_TO_CONTINUE', ?, ?)
            """,
            (timestamp, timestamp),
        )
        batch_id = connection.execute("SELECT batch_id FROM daily_batches").fetchone()[0]
        connection.execute(
            "INSERT INTO daily_batch_jobs VALUES (?, 'li-1', 1, ?)",
            (batch_id, timestamp),
        )
        connection.commit()

    result = orchestrator.continue_after_review(
        batch_date="2026-08-30",
        details_stage=lambda **_kwargs: {},
        metadata_stage=lambda **_kwargs: {},
        ai_stage=lambda **_kwargs: {},
        post_ai_stage=lambda **_kwargs: {},
        email_sender=lambda _summary: (_ for _ in ()).throw(
            OSError("SMTP unavailable")
        ),
    )

    assert result["status"] == "COMPLETE"
    assert result["notification"]["status"] == "FAILED"
    assert result["notification"]["error"] == "OSError: SMTP unavailable"
    with orchestrator.database() as connection:
        row = connection.execute(
            "SELECT status, pipeline_completed_at, last_error FROM daily_batches"
        ).fetchone()
        assert row["status"] == "COMPLETE"
        assert row["pipeline_completed_at"] is not None
        assert row["last_error"] is None
    config.reset_settings_cache()
