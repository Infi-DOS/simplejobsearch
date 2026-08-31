from __future__ import annotations

from contextlib import contextmanager

from simplejobsearch import cli
from simplejobsearch.scheduler import scheduler, tasks


def test_run_nightly_dispatches_to_scheduler_task(monkeypatch, capsys):
    calls = []
    monkeypatch.setattr(
        tasks,
        "nightly_search_task",
        lambda: calls.append("nightly") or {"status": "WAITING_FOR_REVIEW"},
    )

    assert cli.main(["run-nightly"]) == 0
    assert calls == ["nightly"]
    assert "WAITING_FOR_REVIEW" in capsys.readouterr().out


def test_run_review_reminder_dispatches_to_scheduler_task(monkeypatch, capsys):
    calls = []
    monkeypatch.setattr(
        tasks,
        "morning_review_reminder_task",
        lambda: calls.append("reminder") or None,
    )

    assert cli.main(["run-review-reminder"]) == 0
    assert calls == ["reminder"]
    assert capsys.readouterr().out.strip() == "null"


def test_nightly_task_is_no_op_when_todays_batch_already_ran(monkeypatch):
    @contextmanager
    def fake_database():
        yield object()

    monkeypatch.setattr(tasks, "apply_migrations", list)
    monkeypatch.setattr(tasks, "database", fake_database)
    monkeypatch.setattr(
        tasks,
        "get_or_create_batch",
        lambda _connection: {"batch_id": 7, "status": "COMPLETE"},
    )
    monkeypatch.setattr(
        tasks,
        "batch_summary",
        lambda _connection, _batch_id: {
            "batch_id": 7,
            "batch_date": "2026-08-30",
            "status": "COMPLETE",
        },
    )
    monkeypatch.setattr(
        tasks,
        "run_daily_search",
        lambda: (_ for _ in ()).throw(AssertionError("Search reran")),
    )

    result = tasks.nightly_search_task()

    assert result["task_status"] == "NO_OP"
    assert result["reason"] == "batch_already_complete"
    assert result["notification"]["status"] == "NOT_ATTEMPTED"


def test_nightly_email_failure_is_reported_separately(monkeypatch):
    @contextmanager
    def fake_database():
        yield object()

    monkeypatch.setattr(tasks, "apply_migrations", list)
    monkeypatch.setattr(tasks, "database", fake_database)
    monkeypatch.setattr(
        tasks,
        "get_or_create_batch",
        lambda _connection: {"batch_id": 8, "status": "SEARCH_PENDING"},
    )
    monkeypatch.setattr(
        tasks,
        "run_daily_search",
        lambda: {"batch_id": 8, "status": "WAITING_FOR_REVIEW"},
    )
    monkeypatch.setattr(
        tasks,
        "send_search_complete_email",
        lambda _summary: (_ for _ in ()).throw(OSError("SMTP unavailable")),
    )

    result = tasks.nightly_search_task()

    assert result["status"] == "WAITING_FOR_REVIEW"
    assert result["task_status"] == "COMPLETED"
    assert result["notification"]["status"] == "FAILED"
    assert result["notification"]["error"] == "OSError: SMTP unavailable"


def test_nightly_task_completes_when_email_is_disabled(monkeypatch):
    @contextmanager
    def fake_database():
        yield object()

    monkeypatch.setattr(tasks, "apply_migrations", list)
    monkeypatch.setattr(tasks, "database", fake_database)
    monkeypatch.setattr(
        tasks,
        "get_or_create_batch",
        lambda _connection: {"batch_id": 10, "status": "SEARCH_PENDING"},
    )
    monkeypatch.setattr(
        tasks,
        "run_daily_search",
        lambda: {"batch_id": 10, "status": "READY_TO_CONTINUE"},
    )
    monkeypatch.setattr(tasks, "send_search_complete_email", lambda _summary: False)

    result = tasks.nightly_search_task()

    assert result["status"] == "READY_TO_CONTINUE"
    assert result["task_status"] == "COMPLETED"
    assert result["notification"]["status"] == "SKIPPED"
    assert result["notification"]["sent"] is False


def test_review_reminder_no_op_is_explicit(monkeypatch):
    @contextmanager
    def fake_database():
        yield object()

    monkeypatch.setattr(tasks, "apply_migrations", list)
    monkeypatch.setattr(tasks, "database", fake_database)
    monkeypatch.setattr(
        tasks,
        "resolve_batch",
        lambda _connection: {"batch_id": 9},
    )
    monkeypatch.setattr(tasks, "mark_review_state", lambda _connection, _batch_id: 0)
    monkeypatch.setattr(
        tasks,
        "batch_summary",
        lambda _connection, _batch_id: {
            "batch_id": 9,
            "status": "READY_TO_CONTINUE",
        },
    )
    monkeypatch.setattr(
        tasks,
        "send_review_reminder_email",
        lambda _summary: (_ for _ in ()).throw(AssertionError("Reminder sent")),
    )

    result = tasks.morning_review_reminder_task()

    assert result["task_status"] == "NO_OP"
    assert result["reason"] == "no_pending_review_jobs"
    assert result["pending_review_count"] == 0
    assert result["notification"]["status"] == "NOT_ATTEMPTED"


def test_review_reminder_completes_when_email_is_disabled(monkeypatch):
    @contextmanager
    def fake_database():
        yield object()

    monkeypatch.setattr(tasks, "apply_migrations", list)
    monkeypatch.setattr(tasks, "database", fake_database)
    monkeypatch.setattr(tasks, "resolve_batch", lambda _connection: {"batch_id": 11})
    monkeypatch.setattr(tasks, "mark_review_state", lambda _connection, _batch_id: 2)
    monkeypatch.setattr(
        tasks,
        "batch_summary",
        lambda _connection, _batch_id: {
            "batch_id": 11,
            "status": "WAITING_FOR_REVIEW",
        },
    )
    monkeypatch.setattr(tasks, "send_review_reminder_email", lambda _summary: False)

    result = tasks.morning_review_reminder_task()

    assert result["status"] == "WAITING_FOR_REVIEW"
    assert result["task_status"] == "COMPLETED"
    assert result["pending_review_count"] == 2
    assert result["notification"]["status"] == "SKIPPED"
    assert result["notification"]["sent"] is False


def test_scheduler_registers_only_nightly_and_review_jobs():
    service = scheduler.build_scheduler()
    jobs = {job.id: job for job in service.get_jobs()}

    assert set(jobs) == {"daily_discovery", "review_reminder"}
    assert jobs["daily_discovery"].func is tasks.nightly_search_task
    assert jobs["review_reminder"].func is tasks.morning_review_reminder_task
    assert "hour='0'" in str(jobs["daily_discovery"].trigger)
    assert "minute='0'" in str(jobs["daily_discovery"].trigger)
    assert "hour='8'" in str(jobs["review_reminder"].trigger)
    assert "minute='0'" in str(jobs["review_reminder"].trigger)
