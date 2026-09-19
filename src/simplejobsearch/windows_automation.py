from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from collections.abc import Callable, Mapping
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.request import urlopen

from .config import get_settings
from .db import apply_migrations, database
from .notifications.email import (
    deliver_notification,
    notification_not_attempted,
    send_pipeline_complete_email,
    send_review_reminder_email,
    send_search_complete_email,
)
from .operations import recorded_task, task_lock
from .pipeline.orchestrator import continue_after_review
from .scheduler.tasks import (
    afternoon_quoted_search_task,
    morning_review_reminder_task,
    nightly_batch_date,
    nightly_search_task,
)
from .search.collector import run_daily_search

LOGGER = logging.getLogger(__name__)


def _require_windows() -> None:
    if os.name != "nt":
        raise RuntimeError("Windows Task Scheduler automation is only available on Windows")


def _script_path(name: str) -> Path:
    path = get_settings().project_root / "scripts" / "windows" / name
    if not path.exists():
        raise FileNotFoundError(f"Windows automation script not found: {path}")
    return path


def _powershell_script_command(name: str, *arguments: str) -> list[str]:
    return [
        "powershell.exe",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(_script_path(name)),
        *arguments,
    ]


def start_review_portal() -> dict[str, Any]:
    """Start NiceGUI and ngrok and wait until their local health checks pass."""
    _require_windows()
    settings = get_settings()
    log_dir = settings.project_root / "data" / "windows-runtime" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    startup_log = log_dir / "portal-start-worker.log"
    with startup_log.open("a", encoding="utf-8") as log_file:
        subprocess.run(
            _powershell_script_command("Start-ReviewPortal.ps1"),
            cwd=settings.project_root,
            check=True,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=150,
        )
    return {
        "status": "STARTED",
        "message": f"Review portal is ready. Startup log: {startup_log}",
    }


def review_portal_is_ready(*, timeout: float = 10.0) -> bool:
    """Return whether both NiceGUI and the configured ngrok tunnel are healthy."""
    settings = get_settings()
    public_base_url = settings.web.public_base_url
    if not public_base_url:
        return False

    try:
        with urlopen(
            f"http://127.0.0.1:{settings.web.port}/review",
            timeout=timeout,
        ) as response:
            if response.status != 200:
                return False
        with urlopen("http://127.0.0.1:4040/api/tunnels", timeout=timeout) as response:
            tunnels = json.load(response).get("tunnels", [])
    except (OSError, ValueError):
        return False

    expected_url = public_base_url.rstrip("/")
    return any(
        str(tunnel.get("public_url", "")).rstrip("/") == expected_url
        for tunnel in tunnels
    )


def ensure_review_portal() -> dict[str, Any]:
    """Reuse a healthy portal; start it only when either service is missing."""
    if review_portal_is_ready():
        LOGGER.info("Review portal is already healthy; startup skipped")
        return {
            "status": "ALREADY_RUNNING",
            "message": "NiceGUI and ngrok are already healthy.",
        }
    return start_review_portal()


def schedule_review_portal_shutdown() -> str:
    """Trigger the independent on-demand Windows portal-stop task."""
    _require_windows()
    task_name = get_settings().windows_automation.portal_stop_task_name
    if not task_name:
        raise RuntimeError("WINDOWS_PORTAL_STOP_TASK_NAME is empty")
    subprocess.run(
        ["schtasks.exe", "/Run", "/TN", task_name],
        cwd=get_settings().project_root,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return task_name


def start_pipeline_scheduled_task() -> str:
    """Start the independent continuation worker registered in Task Scheduler."""
    _require_windows()
    task_name = get_settings().windows_automation.pipeline_task_name
    if not task_name:
        raise RuntimeError("WINDOWS_PIPELINE_TASK_NAME is empty")
    subprocess.run(
        ["schtasks.exe", "/Run", "/TN", task_name],
        cwd=get_settings().project_root,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return task_name


def start_search_again_worker() -> dict[str, Any]:
    """Launch forced discovery independently while keeping the portal online."""
    _require_windows()
    settings = get_settings()
    log_dir = settings.project_root / "data" / "windows-runtime" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(settings.timezone).strftime("%Y%m%d-%H%M%S")
    log_path = log_dir / f"windows-search-again-{stamp}.log"
    creationflags = (
        getattr(subprocess, "CREATE_NO_WINDOW", 0)
        | getattr(subprocess, "DETACHED_PROCESS", 0)
        | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    )
    with log_path.open("a", encoding="utf-8") as log_file:
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "simplejobsearch.cli",
                "windows-search-again-worker",
            ],
            cwd=settings.project_root,
            stdin=subprocess.DEVNULL,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            creationflags=creationflags,
        )
    return {
        "status": "STARTED",
        "process_id": process.pid,
        "log_path": str(log_path),
    }


def _send_after_portal_ready(
    sender: Callable[[Mapping[str, Any]], bool],
) -> Callable[[Mapping[str, Any]], bool]:
    def wrapped(summary: Mapping[str, Any]) -> bool:
        ensure_review_portal()
        return sender(summary)

    return wrapped


def run_windows_nightly_worker() -> dict:
    """Search first, then expose the review UI before sending its email."""
    return nightly_search_task(
        email_sender=_send_after_portal_ready(send_search_complete_email),
    )


def run_windows_afternoon_quoted_search_worker() -> dict:
    """Run all ten searches quoted at 16:00 and keep review access online."""

    return afternoon_quoted_search_task(
        email_sender=_send_after_portal_ready(send_search_complete_email),
    )


@recorded_task("search_again")
def run_windows_search_again_worker() -> dict:
    """Force a fresh discovery run for the current review date."""
    with task_lock("continue_pipeline") as pipeline_available:
        if not pipeline_available:
            return {
                "status": "BUSY",
                "task_status": "NO_OP",
                "reason": "pipeline_already_running",
                "message": "Search was not started because the pipeline is running.",
                "notification": notification_not_attempted(
                    "search_complete",
                    "pipeline_already_running",
                ),
            }
        try:
            result = run_daily_search(batch_date=nightly_batch_date())
        except Exception:
            ensure_review_portal()
            raise

    result["task"] = "search_again"
    if result.get("status") == "BUSY":
        result["task_status"] = "NO_OP"
        result["reason"] = "discovery_already_running"
        result["notification"] = notification_not_attempted(
            "search_complete",
            "discovery_already_running",
        )
        return result

    result["task_status"] = "COMPLETED"
    result["reason"] = None
    result["portal"] = ensure_review_portal()
    result["notification"] = deliver_notification(
        send_search_complete_email,
        result,
        notification_type="search_complete",
    )
    return result


def run_windows_review_worker() -> dict:
    """Ensure the review UI is public before sending the morning reminder."""
    return morning_review_reminder_task(
        email_sender=_send_after_portal_ready(send_review_reminder_email),
    )


def run_windows_pipeline_worker(*, batch_date=None) -> dict:
    """Run independently, reopen the portal, then send the Post-AI-review email."""
    portal_started = False

    def send_results(summary: Mapping[str, Any]) -> bool:
        nonlocal portal_started
        start_review_portal()
        portal_started = True
        return send_pipeline_complete_email(summary)

    try:
        selected = {"batch_date": batch_date} if batch_date else {}
        result = continue_after_review(email_sender=send_results, **selected)
    except Exception:
        try:
            start_review_portal()
        except Exception:
            LOGGER.exception("Could not restore review portal after pipeline exception")
        raise

    if not portal_started:
        try:
            result["portal"] = start_review_portal()
        except Exception as exc:
            LOGGER.exception("Could not restore review portal after incomplete pipeline")
            result["portal"] = {
                "status": "FAILED",
                "error": f"{type(exc).__name__}: {exc}",
            }
    else:
        result["portal"] = {"status": "STARTED"}
    return result


@recorded_task("login_recovery")
def run_windows_recovery_worker() -> dict:
    """Recover discovery/review access after login; continuation stays manual."""
    apply_migrations()
    # Uses the same date resolution and duplicate-search guard as the nightly task.
    search = run_windows_nightly_worker()
    result = {
        "status": "COMPLETE", "search": search,
        "notification": search.get("notification", {}),
    }
    if not search.get("no_op") and search.get("task_status") == "COMPLETED":
        return result  # A fresh search already sent its summary.
    result["portal"] = ensure_review_portal()
    settings = get_settings()
    now = datetime.now(settings.timezone)
    if (now.hour, now.minute) < (
        settings.scheduler.reminder_hour, settings.scheduler.reminder_minute,
    ):
        return result
    with database() as connection:
        sent_today = connection.execute(
            "SELECT 1 FROM notification_events WHERE notification_type='review_reminder' "
            "AND status='SENT' AND substr(completed_at,1,10)=? LIMIT 1",
            (now.date().isoformat(),),
        ).fetchone()
    if sent_today is None:
        reminder = run_windows_review_worker()
        result["reminder"] = reminder
        result["notification"] = reminder.get("notification", {})
    return result
