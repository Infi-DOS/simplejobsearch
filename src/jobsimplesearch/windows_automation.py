from __future__ import annotations

from collections.abc import Callable, Mapping
import json
import logging
import os
from pathlib import Path
import subprocess
from typing import Any
from urllib.request import urlopen

from .config import get_settings
from .notifications.email import (
    send_pipeline_complete_email,
    send_review_reminder_email,
    send_search_complete_email,
)
from .pipeline.orchestrator import continue_after_review
from .scheduler.tasks import morning_review_reminder_task, nightly_search_task

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
    completed = subprocess.run(
        _powershell_script_command("Start-ReviewPortal.ps1"),
        cwd=get_settings().project_root,
        check=True,
        capture_output=True,
        text=True,
        timeout=90,
    )
    return {
        "status": "STARTED",
        "message": completed.stdout.strip() or "Review portal is ready.",
    }


def review_portal_is_ready(*, timeout: float = 2.0) -> bool:
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


def run_windows_review_worker() -> dict:
    """Ensure the review UI is public before sending the morning reminder."""
    return morning_review_reminder_task(
        email_sender=_send_after_portal_ready(send_review_reminder_email),
    )


def run_windows_pipeline_worker() -> dict:
    """Run independently, reopen the portal, then send the results email."""
    portal_started = False

    def send_results(summary: Mapping[str, Any]) -> bool:
        nonlocal portal_started
        start_review_portal()
        portal_started = True
        return send_pipeline_complete_email(summary)

    try:
        result = continue_after_review(email_sender=send_results)
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
