from __future__ import annotations

import json

import pytest

from jobsimplesearch import cli
from jobsimplesearch.notifications import diagnostics


@pytest.mark.parametrize(
    ("message_type", "sender_name"),
    [
        ("search", "send_search_complete_email"),
        ("review", "send_review_reminder_email"),
        ("complete", "send_pipeline_complete_email"),
    ],
)
def test_email_test_uses_production_sender_with_synthetic_summary(
    monkeypatch, message_type, sender_name
):
    sent = []
    monkeypatch.setattr(
        diagnostics,
        sender_name,
        lambda summary: sent.append(summary) or True,
    )

    result = diagnostics.send_email_test(message_type)

    assert result["email_test"] == message_type
    assert result["workflow_state_changed"] is False
    assert result["notification"]["status"] == "SENT"
    assert len(sent) == 1
    assert sent[0]["email_test"] is True
    assert "SMTP transport test only" in sent[0]["message"]


@pytest.mark.parametrize(("status", "exit_code"), [("SENT", 0), ("FAILED", 1)])
def test_email_test_cli_exit_code_reflects_delivery(monkeypatch, capsys, status, exit_code):
    monkeypatch.setattr(
        diagnostics,
        "send_email_test",
        lambda message_type: {
            "email_test": message_type,
            "workflow_state_changed": False,
            "notification": {
                "type": f"{message_type}_email_test",
                "status": status,
                "sent": status == "SENT",
                "reason": None,
                "error": None if status == "SENT" else "SMTP failed",
            },
            "summary": {},
        },
    )

    assert cli.main(["email-test", "complete"]) == exit_code
    result = json.loads(capsys.readouterr().out)
    assert result["email_test"] == "complete"
    assert result["notification"]["status"] == status


def test_email_test_rejects_unknown_message_type():
    with pytest.raises(ValueError, match="Unsupported email test type"):
        diagnostics.send_email_test("unknown")
