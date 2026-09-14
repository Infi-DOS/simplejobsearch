from __future__ import annotations

import sqlite3
from datetime import date

import pytest

from simplejobsearch import cli, config, operations, windows_automation
from simplejobsearch.notifications import email
from simplejobsearch.scheduler import tasks


@pytest.fixture
def history_db(tmp_path, monkeypatch):
    path = tmp_path / "history.db"
    sqlite3.connect(path).close()
    monkeypatch.setenv("JOBSEARCH_DB_PATH", str(path))
    config.reset_settings_cache()
    return path


def test_smtp_failure_is_durable_and_does_not_fail_successful_task(history_db):
    def smtp_failure(_summary):
        with sqlite3.connect(history_db) as connection:
            assert connection.execute("SELECT status FROM notification_events").fetchone()[0] == "SENDING"
        raise OSError("SMTP unavailable")

    @operations.recorded_task("pipeline-test")
    def workflow():
        summary = {"batch_id": 42, "batch_date": "2026-09-11", "status": "COMPLETE"}
        summary["notification"] = email.deliver_notification(
            smtp_failure, summary, notification_type="pipeline_complete",
        )
        return summary

    result = workflow()
    history = operations.recent_history()
    assert result["status"] == "COMPLETE"
    assert operations.result_exit_code(result) == 2
    task = history["task_runs"][0]
    event = history["notification_events"][0]
    assert task["status"] == "COMPLETED"
    assert task["notification_status"] == "FAILED"
    assert task["batch_id"] == 42
    assert event["task_run_id"] == task["task_run_id"]
    assert event["status"] == "FAILED"
    assert event["error"] == "OSError: SMTP unavailable"
    assert event["completed_at"] is not None


@pytest.mark.parametrize(("sender_result", "status"), [(True, "SENT"), (False, "SKIPPED")])
def test_successful_and_disabled_notifications_are_recorded(history_db, sender_result, status):
    email.deliver_notification(
        lambda _summary: sender_result, {}, notification_type="search_complete",
    )
    event = operations.recent_history()["notification_events"][0]
    assert event["status"] == status
    assert event["sent"] == int(sender_result)


def test_no_op_notification_is_recorded(history_db):
    result = email.notification_not_attempted("review_reminder", "no_pending_review_jobs")
    event = operations.recent_history()["notification_events"][0]
    assert result["status"] == event["status"] == "NOT_ATTEMPTED"
    assert event["reason"] == "no_pending_review_jobs"


def test_history_failure_does_not_change_delivery(monkeypatch):
    # The default test DB does not exist; recording must not create it or fail delivery.
    result = email.deliver_notification(lambda _: True, {}, notification_type="search_complete")
    assert result["status"] == "SENT"
    assert not config.get_settings().database_path.exists()


def test_duplicate_worker_is_blocked_and_exception_releases_lock(history_db):
    calls = []

    @operations.recorded_task("exclusive-test")
    def worker():
        calls.append(1)
        raise ValueError("worker failed")

    with operations.task_lock("exclusive-test") as acquired:
        assert acquired
        assert worker()["status"] == "BUSY"
    assert calls == []
    with pytest.raises(ValueError, match="worker failed"):
        worker()
    with operations.task_lock("exclusive-test") as acquired:
        assert acquired
    assert operations.recent_history()["task_runs"][0]["status"] == "FAILED"
    assert operations.CURRENT_TASK.get() is None


def test_secrets_are_redacted_in_persisted_errors(history_db, monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "private-test-api-key")
    config.reset_settings_cache()
    email.deliver_notification(
        lambda _: (_ for _ in ()).throw(OSError("private-test-api-key failed")),
        {}, notification_type="test",
    )
    assert operations.recent_history()["notification_events"][0]["error"] == "OSError: [REDACTED] failed"


def test_continue_cli_can_target_an_older_batch(monkeypatch):
    calls = []
    monkeypatch.setattr(cli, "continue_after_review", lambda **kw: calls.append(kw) or {"status": "COMPLETE"})
    assert cli.main(["continue", "--batch-date", "2026-09-11"]) == 0
    assert calls == [{"batch_date": date(2026, 9, 11)}]


def test_nightly_preserves_a_successful_search_when_continuation_failed(history_db, monkeypatch):
    from contextlib import contextmanager

    with sqlite3.connect(history_db) as connection:
        connection.execute("CREATE TABLE search_runs (run_id TEXT, status TEXT)")
        connection.execute("INSERT INTO search_runs VALUES ('done','SUCCESS')")

    @contextmanager
    def connection_context():
        connection = sqlite3.connect(history_db)
        try:
            yield connection
        finally:
            connection.close()

    batch = {"batch_id": 1, "status": "FAILED", "search_run_id": "done"}
    monkeypatch.setattr(tasks, "apply_migrations", list)
    monkeypatch.setattr(tasks, "database", connection_context)
    monkeypatch.setattr(tasks, "get_or_create_batch", lambda *_: batch)
    monkeypatch.setattr(tasks, "batch_summary", lambda *_: batch.copy())
    monkeypatch.setattr(tasks, "run_daily_search", lambda **_: pytest.fail("Successful search repeated"))
    result = tasks.nightly_search_task()
    assert result["task_status"] == "NO_OP"
    assert result["status"] == "FAILED"
    assert operations.result_exit_code(result) == 0
    assert operations.recent_history()["task_runs"][0]["status"] == "NO_OP"


def test_restart_marks_abandoned_task_and_unconfirmed_delivery(history_db):
    run_id = operations._write(
        "INSERT INTO task_runs(task_name,process_id,started_at,status) "
        "VALUES ('restart-test',123,'2026-09-12','RUNNING')",
    )
    operations._write(
        "INSERT INTO notification_events(task_run_id,notification_type,started_at,status) "
        "VALUES (?,'pipeline_complete','2026-09-12','SENDING')", (run_id,),
    )

    @operations.recorded_task("restart-test")
    def restarted():
        return {"status": "COMPLETE"}

    restarted()
    history = operations.recent_history()
    assert history["task_runs"][0]["status"] == "COMPLETED"
    assert history["task_runs"][1]["status"] == "INTERRUPTED"
    assert history["notification_events"][0]["status"] == "UNKNOWN"


def test_login_recovery_never_continues_ai_or_sends_second_email_for_new_search(history_db, monkeypatch):
    monkeypatch.setattr(windows_automation, "apply_migrations", list)
    monkeypatch.setattr(windows_automation, "run_windows_nightly_worker", lambda: {
        "task_status": "COMPLETED", "notification": {"status": "SENT"},
    })
    monkeypatch.setattr(windows_automation, "continue_after_review", lambda **_: pytest.fail("AI launched"))
    monkeypatch.setattr(windows_automation, "run_windows_review_worker", lambda: pytest.fail("Duplicate email"))
    result = windows_automation.run_windows_recovery_worker()
    assert result["notification"]["status"] == "SENT"


@pytest.mark.parametrize("search_active", [False, True])
def test_nightly_recovers_interrupted_search_but_leaves_active_discovery_alone(
    history_db, monkeypatch, search_active,
):
    from contextlib import contextmanager, nullcontext

    @contextmanager
    def fake_database():
        yield object()

    calls = []
    monkeypatch.setattr(tasks, "apply_migrations", list)
    monkeypatch.setattr(tasks, "database", fake_database)
    monkeypatch.setattr(tasks, "get_or_create_batch", lambda *_: {
        "batch_id": 1, "status": "SEARCHING", "search_run_id": None,
    })
    monkeypatch.setattr(tasks, "run_daily_search", lambda **_: calls.append(1) or {
        "batch_id": 1, "status": "WAITING_FOR_REVIEW",
    })
    lock = operations.task_lock("discovery") if search_active else nullcontext()
    with lock:
        result = tasks.nightly_search_task(email_sender=lambda _: False)
    assert calls == ([] if search_active else [1])
    assert result["task_status"] == ("NO_OP" if search_active else "COMPLETED")


def test_nightly_does_not_email_search_complete_when_discovery_becomes_busy(history_db, monkeypatch):
    from contextlib import contextmanager

    @contextmanager
    def fake_database():
        yield object()

    monkeypatch.setattr(tasks, "apply_migrations", list)
    monkeypatch.setattr(tasks, "database", fake_database)
    monkeypatch.setattr(tasks, "get_or_create_batch", lambda *_: {
        "batch_id": 1, "status": "SEARCH_PENDING",
    })
    monkeypatch.setattr(tasks, "run_daily_search", lambda **_: {
        "status": "BUSY", "no_op": True, "reason": "task_already_running",
    })
    result = tasks.nightly_search_task(email_sender=lambda _: pytest.fail("False search email"))
    assert result["task_status"] == "NO_OP"
    assert operations.result_exit_code(result) == 0
