from __future__ import annotations

from dataclasses import replace

import pytest

from jobsimplesearch import config
from jobsimplesearch.notifications import email


@pytest.mark.parametrize(
    ("security", "expected_client", "expected_starttls", "expected_ehlo"),
    [
        ("ssl", "ssl", 0, 1),
        ("starttls", "plain", 1, 2),
        ("none", "plain", 0, 1),
    ],
)
def test_smtp_security_selects_transport_and_tls_behavior(
    monkeypatch,
    security,
    expected_client,
    expected_starttls,
    expected_ehlo,
):
    events = []

    class FakeSMTP:
        client_type = "plain"

        def __init__(self, host, port, timeout):
            events.append(("connect", self.client_type, host, port, timeout))

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def ehlo(self):
            events.append(("ehlo",))

        def starttls(self):
            events.append(("starttls",))

        def login(self, username, password):
            events.append(("login", username, password))

        def send_message(self, message):
            events.append(("send", message["Subject"]))

    class FakeSMTPSSL(FakeSMTP):
        client_type = "ssl"

    monkeypatch.setattr(email.smtplib, "SMTP", FakeSMTP)
    monkeypatch.setattr(email.smtplib, "SMTP_SSL", FakeSMTPSSL)
    settings = replace(
        config.get_settings().email,
        enabled=True,
        smtp_host="smtp.example.test",
        smtp_port=465 if security == "ssl" else 587,
        smtp_security=security,
        username="test-user",
        password="test-password",
        sender="sender@example.test",
        recipients=("recipient@example.test",),
    )

    assert email._send("Transport test", {"ok": True}, settings=settings) is True

    connect = next(event for event in events if event[0] == "connect")
    assert connect[1] == expected_client
    assert sum(event[0] == "starttls" for event in events) == expected_starttls
    assert sum(event[0] == "ehlo" for event in events) == expected_ehlo
    assert ("login", "test-user", "test-password") in events
    assert ("send", "Transport test") in events


def test_invalid_smtp_security_fails_during_configuration_loading(monkeypatch):
    monkeypatch.setenv("SMTP_SECURITY", "opportunistic")
    config.reset_settings_cache()

    with pytest.raises(
        ValueError,
        match="SMTP_SECURITY must be one of: none, ssl, starttls",
    ):
        config.get_settings()

    config.reset_settings_cache()


def test_smtp_security_is_normalized(monkeypatch):
    monkeypatch.setenv("SMTP_SECURITY", " SSL ")
    config.reset_settings_cache()

    assert config.get_settings().email.smtp_security == "ssl"

    config.reset_settings_cache()
