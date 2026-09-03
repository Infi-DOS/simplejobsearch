from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from datetime import date, datetime, timedelta
from typing import Any

from ..config import get_settings
from ..db import apply_migrations, database
from ..notifications.email import (
    deliver_notification,
    notification_not_attempted,
    send_review_reminder_email,
    send_search_complete_email,
)
from ..search.collector import run_daily_search
from ..workflow import (
    batch_summary,
    get_or_create_batch,
    mark_review_state,
    resolve_batch,
)

LOGGER = logging.getLogger(__name__)


def nightly_batch_date(now: datetime | None = None) -> date:
    """Return the morning review date owned by this nightly invocation."""
    settings = get_settings()
    local_now = now or datetime.now(settings.timezone)
    scheduled_at = (
        settings.scheduler.search_hour,
        settings.scheduler.search_minute,
    )
    if (local_now.hour, local_now.minute) >= scheduled_at:
        return local_now.date() + timedelta(days=1)
    return local_now.date()


def nightly_search_task(
    *,
    email_sender: Callable[[Mapping[str, Any]], bool] | None = None,
    batch_date: date | str | None = None,
) -> dict:
    apply_migrations()
    target_batch_date = nightly_batch_date() if batch_date is None else batch_date
    with database() as connection:
        batch = get_or_create_batch(connection, target_batch_date)
        if batch["status"] not in {"SEARCH_PENDING", "FAILED"}:
            summary = batch_summary(connection, batch["batch_id"])
            summary.update(
                {
                    "task": "nightly_search",
                    "task_status": "NO_OP",
                    "reason": f"batch_already_{batch['status'].lower()}",
                    "notification": notification_not_attempted(
                        "search_complete",
                        "nightly_search_no_op",
                    ),
                }
            )
            LOGGER.info(
                "Nightly search skipped: batch %s is already %s",
                batch["batch_id"],
                batch["status"],
            )
            return summary

    summary = run_daily_search(batch_date=target_batch_date)
    summary["task"] = "nightly_search"
    summary["task_status"] = "COMPLETED"
    summary["reason"] = None
    summary["notification"] = deliver_notification(
        email_sender or send_search_complete_email,
        summary,
        notification_type="search_complete",
    )
    return summary


def morning_review_reminder_task(
    *,
    email_sender: Callable[[Mapping[str, Any]], bool] | None = None,
) -> dict:
    apply_migrations()
    with database() as connection:
        try:
            batch = resolve_batch(connection)
        except LookupError:
            LOGGER.info("Review reminder skipped: no daily batch exists")
            return {
                "task": "review_reminder",
                "task_status": "NO_OP",
                "reason": "no_daily_batch",
                "pending_review_count": 0,
                "notification": notification_not_attempted(
                    "review_reminder",
                    "no_daily_batch",
                ),
            }
        pending = mark_review_state(connection, batch["batch_id"])
        summary = batch_summary(connection, batch["batch_id"])
    summary["task"] = "review_reminder"
    summary["pending_review_count"] = pending
    if summary["status"] == "WAITING_FOR_REVIEW" and pending:
        summary["task_status"] = "COMPLETED"
        summary["reason"] = None
        summary["notification"] = deliver_notification(
            email_sender or send_review_reminder_email,
            summary,
            notification_type="review_reminder",
        )
        return summary
    LOGGER.info("Review reminder skipped: no pending review jobs")
    summary["task_status"] = "NO_OP"
    summary["reason"] = "no_pending_review_jobs"
    summary["notification"] = notification_not_attempted(
        "review_reminder",
        "no_pending_review_jobs",
    )
    return summary
