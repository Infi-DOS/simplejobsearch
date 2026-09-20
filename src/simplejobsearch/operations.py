"""Persistent operational history; recording failures never fail a workflow."""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from collections.abc import Iterable, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime
from functools import wraps

from .config import get_settings

LOGGER = logging.getLogger(__name__)
CURRENT_TASK: ContextVar[int | None] = ContextVar("current_task", default=None)


def timestamp() -> str:
    return datetime.now(get_settings().timezone).isoformat()


def _redact(value):
    settings = get_settings()
    text = json.dumps(value, default=str)
    for secret in (settings.ai.api_key, settings.email.password):
        if secret:
            text = text.replace(json.dumps(secret)[1:-1], "[REDACTED]")
    return json.loads(text)


def _write(sql: str, params: tuple = ()) -> int | None:
    connection = None
    try:
        settings = get_settings()
        # Never create an empty production database as a side effect of logging.
        connection = sqlite3.connect(
            settings.database_path.as_uri() + "?mode=rw", uri=True, timeout=5,
        )
        if connection.execute(
            "SELECT 1 FROM sqlite_master WHERE name='notification_events'"
        ).fetchone() is None:
            schema = settings.project_root / "migrations" / "006_operational_history.sql"
            connection.executescript(schema.read_text(encoding="utf-8"))
        cursor = connection.execute(sql, _redact(params))
        connection.commit()
        return cursor.lastrowid
    except Exception as exc:  # noqa: BLE001 - history must not fail workflow work
        LOGGER.warning("Could not persist operational history: %s", type(exc).__name__)
        return None
    finally:
        if connection is not None:
            connection.close()


@contextmanager
def task_lock(task_name: str):
    """OS-owned lock shared by CLI, UI and scheduled workers, released on exit."""
    path = get_settings().database_path
    if not path.exists():
        # Let the normal workflow report missing database/configuration errors.
        yield True
        return
    with path.with_name(f"{path.name}.{task_name}.lock").open("a+b") as handle:
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        acquired = False
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
        except OSError:
            pass
        try:
            yield acquired
        finally:
            if acquired:
                handle.seek(0)
                if os.name == "nt":
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(handle, fcntl.LOCK_UN)


def active_recorded_tasks(task_names: Iterable[str]) -> set[str]:
    """Return genuinely active recorded tasks and reconcile abandoned rows.

    A worker can be terminated before ``recorded_task`` writes its outcome. The
    database row then remains RUNNING, while the OS-owned task lock is released.
    Treat the lock as authoritative and make the interrupted state durable.
    """

    requested = tuple(dict.fromkeys(str(name) for name in task_names if name))
    if not requested:
        return set()
    path = get_settings().database_path
    if not path.exists():
        return set()

    connection = sqlite3.connect(path.as_uri() + "?mode=rw", uri=True, timeout=5)
    try:
        placeholders = ",".join("?" for _ in requested)
        candidates = {
            str(row[0])
            for row in connection.execute(
                "SELECT DISTINCT task_name FROM task_runs "
                f"WHERE status='RUNNING' AND task_name IN ({placeholders})",
                requested,
            )
        }
        active: set[str] = set()
        for task_name in candidates:
            with task_lock(task_name) as available:
                if not available:
                    active.add(task_name)
                    continue
                interrupted_at = timestamp()
                connection.execute(
                    "UPDATE task_runs SET status='INTERRUPTED',completed_at=?,error=? "
                    "WHERE task_name=? AND status='RUNNING'",
                    (
                        interrupted_at,
                        "Worker ended without recording an outcome",
                        task_name,
                    ),
                )
                connection.execute(
                    "UPDATE notification_events SET status='UNKNOWN',completed_at=?,reason=? "
                    "WHERE status='SENDING' AND task_run_id IN "
                    "(SELECT task_run_id FROM task_runs "
                    "WHERE task_name=? AND status='INTERRUPTED')",
                    (
                        interrupted_at,
                        "worker_interrupted_delivery_unconfirmed",
                        task_name,
                    ),
                )
        connection.commit()
        return active
    finally:
        connection.close()


def recorded_task(task_name: str):
    def decorate(function):
        @wraps(function)
        def wrapped(*args, **kwargs):
            with task_lock(task_name) as acquired:
                if not acquired:
                    return {
                        "status": "BUSY", "task_status": "NO_OP", "no_op": True,
                        "reason": "task_already_running",
                        "message": f"{task_name} is already running; no duplicate work started.",
                    }
                # Owning the OS lock proves no previous worker of this kind is active.
                _write(
                    "UPDATE task_runs SET status='INTERRUPTED',completed_at=?,error=? "
                    "WHERE task_name=? AND status='RUNNING'",
                    (timestamp(), "Worker ended without recording an outcome", task_name),
                )
                _write(
                    "UPDATE notification_events SET status='UNKNOWN',completed_at=?,reason=? "
                    "WHERE status='SENDING' AND task_run_id IN "
                    "(SELECT task_run_id FROM task_runs WHERE task_name=? AND status='INTERRUPTED')",
                    (timestamp(), "worker_interrupted_delivery_unconfirmed", task_name),
                )
                run_id = _write(
                    "INSERT INTO task_runs (task_name,batch_date,process_id,started_at,status) "
                    "VALUES (?,?,?,?,'RUNNING')",
                    (task_name, kwargs.get("batch_date"), os.getpid(), timestamp()),
                )
                token = CURRENT_TASK.set(run_id)
                try:
                    result = function(*args, **kwargs)
                except BaseException as exc:
                    _write(
                        "UPDATE task_runs SET status='FAILED',completed_at=?,error=? "
                        "WHERE task_run_id=?",
                        (timestamp(), f"{type(exc).__name__}: {exc}", run_id),
                    )
                    raise
                else:
                    summary = result if isinstance(result, Mapping) else {}
                    status = (
                        "NO_OP" if summary.get("no_op") or summary.get("task_status") == "NO_OP"
                        else "FAILED" if summary.get("status") == "FAILED"
                        else "COMPLETED"
                    )
                    notification = summary.get("notification") or {}
                    _write(
                        "UPDATE task_runs SET status=?,completed_at=?,batch_id=?,"
                        "batch_date=COALESCE(?,batch_date),notification_status=?,error=?,"
                        "summary_json=? WHERE task_run_id=?",
                        (status, timestamp(), summary.get("batch_id"),
                         summary.get("batch_date"), notification.get("status"),
                         summary.get("last_error"), json.dumps(_redact(summary), default=str), run_id),
                    )
                    _write(
                        "UPDATE notification_events SET batch_id=COALESCE(batch_id,?),"
                        "batch_date=COALESCE(batch_date,?) WHERE task_run_id=?",
                        (summary.get("batch_id"), summary.get("batch_date"), run_id),
                    )
                    return result
                finally:
                    CURRENT_TASK.reset(token)
        return wrapped
    return decorate


def notification_started(notification_type: str, summary: Mapping) -> int | None:
    return _write(
        "INSERT INTO notification_events "
        "(task_run_id,batch_id,batch_date,notification_type,started_at,status) "
        "VALUES (?,?,?,?,?,'SENDING')",
        (CURRENT_TASK.get(), summary.get("batch_id"), summary.get("batch_date"),
         notification_type, timestamp()),
    )


def notification_finished(notification_id: int | None, result: Mapping) -> None:
    _write(
        "UPDATE notification_events SET completed_at=?,status=?,sent=?,reason=?,error=? "
        "WHERE notification_id=?",
        (timestamp(), result["status"], int(result["sent"]), result.get("reason"),
         result.get("error"), notification_id),
    )


def recent_history(limit: int = 20) -> dict:
    connection = sqlite3.connect(
        get_settings().database_path.as_uri() + "?mode=ro", uri=True, timeout=5,
    )
    connection.row_factory = sqlite3.Row
    try:
        return {
            name: [dict(row) for row in connection.execute(
                f"SELECT * FROM {name} ORDER BY {key} DESC LIMIT ?", (limit,),
            )]
            for name, key in (
                ("task_runs", "task_run_id"), ("notification_events", "notification_id"),
            )
        }
    finally:
        connection.close()


def result_exit_code(result: Mapping) -> int:
    if result is None:
        return 0
    if result.get("status") == "FAILED" and result.get("task_status") != "NO_OP":
        return 1
    if (result.get("notification") or {}).get("status") == "FAILED":
        return 2
    return 0
