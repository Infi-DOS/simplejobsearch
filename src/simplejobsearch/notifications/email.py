from __future__ import annotations

import logging
import smtplib
from collections.abc import Callable, Mapping
from email.message import EmailMessage
from html import escape
from typing import Any

from ..config import EmailSettings, get_settings
from .links import results_url, review_url

LOGGER = logging.getLogger(__name__)


def notification_not_attempted(notification_type: str, reason: str) -> dict[str, Any]:
    return {
        "type": notification_type,
        "status": "NOT_ATTEMPTED",
        "sent": False,
        "reason": reason,
        "error": None,
    }


def deliver_notification(
    sender: Callable[[Mapping[str, Any]], bool],
    summary: Mapping[str, Any],
    *,
    notification_type: str,
) -> dict[str, Any]:
    """Send a notification without changing the outcome of completed workflow work."""
    try:
        sent = bool(sender(summary))
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        LOGGER.exception("%s notification failed", notification_type)
        return {
            "type": notification_type,
            "status": "FAILED",
            "sent": False,
            "reason": None,
            "error": error,
        }
    return {
        "type": notification_type,
        "status": "SENT" if sent else "SKIPPED",
        "sent": sent,
        "reason": None if sent else "sender_returned_false",
        "error": None,
    }


def _format_summary(summary: Mapping[str, Any]) -> str:
    lines = []
    for key, value in summary.items():
        label = key.replace("_", " ").title()
        if isinstance(value, Mapping):
            lines.append(f"{label}:")
            lines.extend(
                f"  {nested_key.replace('_', ' ').title()}: {nested_value}"
                for nested_key, nested_value in value.items()
            )
        elif not isinstance(value, (list, tuple, set)):
            lines.append(f"{label}: {value}")
    return "\n".join(lines)


def _count(summary: Mapping[str, Any], *keys: str) -> int:
    for key in keys:
        if key in summary:
            return int(summary[key] or 0)
    return 0


def _format_search_summary(
    summary: Mapping[str, Any],
    action_url: str | None = None,
) -> str:
    lines = [
        "Job search complete",
        "",
        f"New jobs: {_count(summary, 'new_job_count', 'new_jobs')}",
        f"Need review: {_count(summary, 'review_required_count', 'pending_review_count')}",
        "",
        "Review is available from your job-search dashboard.",
    ]
    if action_url:
        lines.extend(["", "View today's jobs:", action_url])
    return "\n".join(lines)


def _format_review_summary(
    summary: Mapping[str, Any],
    action_url: str | None = None,
) -> str:
    pending = _count(summary, "pending_review_count", "review_required_count", "pending")
    lines = [
        "Job review reminder",
        "",
        f"{pending} jobs are waiting for your review.",
    ]
    if action_url:
        lines.extend(["", "Review Jobs:", action_url])
    return "\n".join(lines)


def _format_pipeline_summary(
    summary: Mapping[str, Any],
    action_url: str | None = None,
) -> str:
    totals = summary.get("batch_totals", summary)
    lines = [
        "Daily job pipeline complete",
        "",
        f"Shortlist: {_count(totals, 'shortlist', 'shortlist_count')}",
        f"Review: {_count(totals, 'review', 'final_review_count')}",
        f"Rejected: {_count(totals, 'reject', 'reject_count')}",
        "",
        "Batch totals:",
    ]
    lines.extend(
        f"  {key.replace('_', ' ').title()}: {value}"
        for key, value in totals.items()
    )
    lines.extend(["", "Activity this invocation:"])
    lines.extend(
        f"  {key.replace('_', ' ').title()}: {value}"
        for key, value in summary.get("this_run", {}).items()
    )
    if action_url:
        lines.extend(["", "View Results:", action_url])
    return "\n".join(lines)


def _format_action_html(
    title: str,
    rows: list[tuple[str, Any]],
    *,
    message: str | None = None,
    action_label: str | None = None,
    action_url: str | None = None,
) -> str:
    metrics = "".join(
        f'<p style="margin:4px 0"><strong>{escape(label)}:</strong> '
        f"{escape(str(value))}</p>"
        for label, value in rows
    )
    note = f"<p>{escape(message)}</p>" if message else ""
    action = ""
    if action_label and action_url:
        safe_url = escape(action_url, quote=True)
        action = (
            '<p style="margin-top:24px">'
            f'<a href="{safe_url}" style="background:#0369a1;color:#ffffff;'
            'padding:12px 18px;border-radius:8px;text-decoration:none;'
            f'font-weight:700">{escape(action_label)}</a></p>'
            f'<p style="font-size:12px;color:#64748b">{safe_url}</p>'
        )
    return (
        '<div style="font-family:Arial,sans-serif;color:#0f172a;line-height:1.5">'
        f"<h2>{escape(title)}</h2>{metrics}{note}{action}</div>"
    )


def _format_search_html(summary: Mapping[str, Any], action_url: str | None) -> str:
    return _format_action_html(
        "Job search complete",
        [
            ("New jobs", _count(summary, "new_job_count", "new_jobs")),
            (
                "Need review",
                _count(summary, "review_required_count", "pending_review_count"),
            ),
        ],
        message="Review is available from your job-search dashboard.",
        action_label="View today's jobs",
        action_url=action_url,
    )


def _format_review_html(summary: Mapping[str, Any], action_url: str | None) -> str:
    pending = _count(summary, "pending_review_count", "review_required_count", "pending")
    return _format_action_html(
        "Job review reminder",
        [("Waiting for review", pending)],
        message=f"{pending} jobs are waiting for your review.",
        action_label="Review Jobs",
        action_url=action_url,
    )


def _format_pipeline_html(summary: Mapping[str, Any], action_url: str | None) -> str:
    totals = summary.get("batch_totals", summary)
    return _format_action_html(
        "Daily job pipeline complete",
        [
            ("Shortlist", _count(totals, "shortlist", "shortlist_count")),
            ("Review", _count(totals, "review", "final_review_count")),
            ("Rejected", _count(totals, "reject", "reject_count")),
        ],
        action_label="View Results",
        action_url=action_url,
    )


def _send(
    subject: str,
    summary: Mapping[str, Any],
    settings: EmailSettings | None = None,
    formatter=_format_summary,
    html_formatter: Callable[[Mapping[str, Any]], str] | None = None,
) -> bool:
    config = settings or get_settings().email
    if not config.enabled:
        LOGGER.info("Email disabled; skipped: %s", subject)
        return False
    missing = []
    if not config.smtp_host:
        missing.append("SMTP_HOST")
    if not config.sender:
        missing.append("EMAIL_FROM")
    if not config.recipients:
        missing.append("EMAIL_TO")
    if missing:
        raise RuntimeError("Email is enabled but settings are missing: " + ", ".join(missing))

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = config.sender
    message["To"] = ", ".join(config.recipients)
    message.set_content(formatter(summary))
    if html_formatter is not None:
        message.add_alternative(html_formatter(summary), subtype="html")

    client_factory = (
        smtplib.SMTP_SSL if config.smtp_security == "ssl" else smtplib.SMTP
    )
    with client_factory(config.smtp_host, config.smtp_port, timeout=30) as client:
        client.ehlo()
        if config.smtp_security == "starttls":
            client.starttls()
            client.ehlo()
        if config.username:
            client.login(config.username, config.password or "")
        client.send_message(message)
    return True


def send_search_complete_email(summary: Mapping[str, Any]) -> bool:
    settings = get_settings()
    action_url = review_url(settings.web.public_base_url)
    if action_url is None:
        LOGGER.warning("PUBLIC_BASE_URL is not configured; search email has no link")
    return _send(
        "Job search complete",
        summary,
        settings=settings.email,
        formatter=lambda data: _format_search_summary(data, action_url),
        html_formatter=lambda data: _format_search_html(data, action_url),
    )


def send_review_reminder_email(summary: Mapping[str, Any]) -> bool:
    settings = get_settings()
    action_url = review_url(settings.web.public_base_url)
    if action_url is None:
        LOGGER.warning("PUBLIC_BASE_URL is not configured; review email has no link")
    return _send(
        "Job review reminder",
        summary,
        settings=settings.email,
        formatter=lambda data: _format_review_summary(data, action_url),
        html_formatter=lambda data: _format_review_html(data, action_url),
    )


def send_pipeline_complete_email(summary: Mapping[str, Any]) -> bool:
    settings = get_settings()
    action_url = results_url(settings.web.public_base_url)
    if action_url is None:
        LOGGER.warning("PUBLIC_BASE_URL is not configured; pipeline email has no link")
    return _send(
        "Job pipeline complete",
        summary,
        settings=settings.email,
        formatter=lambda data: _format_pipeline_summary(data, action_url),
        html_formatter=lambda data: _format_pipeline_html(data, action_url),
    )
