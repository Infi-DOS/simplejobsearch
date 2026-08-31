from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from ..workflow import now_iso
from .email import (
    deliver_notification,
    send_pipeline_complete_email,
    send_review_reminder_email,
    send_search_complete_email,
)

EMAIL_TEST_TYPES = ("search", "review", "complete")


def _synthetic_summary(message_type: str) -> dict[str, Any]:
    timestamp = now_iso()
    common = {
        "email_test": True,
        "generated_at": timestamp,
        "message": "SMTP transport test only; no workflow state was changed.",
    }
    if message_type == "search":
        return {
            **common,
            "status": "WAITING_FOR_REVIEW",
            "new_job_count": 3,
            "review_required_count": 2,
            "search_failures": 0,
        }
    if message_type == "review":
        return {
            **common,
            "status": "WAITING_FOR_REVIEW",
            "pending_review_count": 2,
        }
    if message_type == "complete":
        return {
            **common,
            "batch_id": "SMTP-TEST",
            "batch_date": timestamp[:10],
            "status": "COMPLETE",
            "this_run": {
                "details_fetched": 3,
                "metadata_evaluated": 3,
                "metadata_pass": 2,
                "metadata_reject": 1,
                "ai_processed": 2,
                "ai_failed": 0,
            },
            "batch_totals": {
                "details_fetched": 3,
                "metadata_pass": 2,
                "metadata_reject": 1,
                "ai_extracted": 2,
                "shortlist": 1,
                "review": 1,
                "reject": 0,
            },
        }
    raise ValueError(f"Unsupported email test type: {message_type}")


def send_email_test(message_type: str) -> dict[str, Any]:
    """Send one production email template with synthetic, state-free data."""
    senders: dict[str, Callable[[Mapping[str, Any]], bool]] = {
        "search": send_search_complete_email,
        "review": send_review_reminder_email,
        "complete": send_pipeline_complete_email,
    }
    if message_type not in senders:
        raise ValueError(f"Unsupported email test type: {message_type}")
    summary = _synthetic_summary(message_type)
    return {
        "email_test": message_type,
        "workflow_state_changed": False,
        "notification": deliver_notification(
            senders[message_type],
            summary,
            notification_type=f"{message_type}_email_test",
        ),
        "summary": summary,
    }
