from __future__ import annotations

from io import BytesIO
from types import SimpleNamespace

from simplejobsearch import config, windows_automation


def test_windows_automation_settings_are_opt_in(monkeypatch):
    monkeypatch.setenv("WINDOWS_TASK_AUTOMATION", "true")
    monkeypatch.setenv("WINDOWS_PIPELINE_TASK_NAME", "Test-Pipeline")
    monkeypatch.setenv("WINDOWS_PORTAL_SHUTDOWN_DELAY_SECONDS", "7")
    config.reset_settings_cache()

    settings = config.get_settings().windows_automation

    assert settings.enabled is True
    assert settings.pipeline_task_name == "Test-Pipeline"
    assert settings.portal_stop_task_name == "JobSimpleSearch-ClosePortal"
    assert settings.portal_shutdown_delay_seconds == 7
    config.reset_settings_cache()


def test_review_portal_health_requires_local_web_and_matching_tunnel(monkeypatch):
    calls = []

    def fake_urlopen(url, *, timeout):
        calls.append((url, timeout))
        if url.endswith("/review"):
            response = BytesIO(b"")
            response.status = 200
            return response
        return BytesIO(
            b'{"tunnels": [{"public_url": "https://jobs.example.test"}]}'
        )

    fake_settings = SimpleNamespace(
        web=SimpleNamespace(
            port=5099,
            public_base_url="https://jobs.example.test/",
        ),
    )
    monkeypatch.setattr(windows_automation, "get_settings", lambda: fake_settings)
    monkeypatch.setattr(windows_automation, "urlopen", fake_urlopen)

    assert windows_automation.review_portal_is_ready() is True
    assert calls == [
        ("http://127.0.0.1:5099/review", 2.0),
        ("http://127.0.0.1:4040/api/tunnels", 2.0),
    ]


def test_ensure_review_portal_does_not_start_a_healthy_portal(monkeypatch):
    monkeypatch.setattr(windows_automation, "review_portal_is_ready", lambda: True)
    monkeypatch.setattr(
        windows_automation,
        "start_review_portal",
        lambda: (_ for _ in ()).throw(AssertionError("portal restarted")),
    )

    result = windows_automation.ensure_review_portal()

    assert result["status"] == "ALREADY_RUNNING"


def test_start_review_portal_uses_file_logging_without_output_pipes(
    monkeypatch,
    tmp_path,
):
    calls = []
    fake_settings = SimpleNamespace(project_root=tmp_path)
    monkeypatch.setattr(windows_automation, "_require_windows", lambda: None)
    monkeypatch.setattr(
        windows_automation,
        "_powershell_script_command",
        lambda _name: ["powershell.exe", "Start-ReviewPortal.ps1"],
    )
    monkeypatch.setattr(windows_automation, "get_settings", lambda: fake_settings)
    monkeypatch.setattr(
        windows_automation.subprocess,
        "run",
        lambda command, **kwargs: calls.append((command, kwargs)),
    )

    result = windows_automation.start_review_portal()

    assert result["status"] == "STARTED"
    assert "portal-start-worker.log" in result["message"]
    assert "capture_output" not in calls[0][1]
    assert calls[0][1]["stderr"] is windows_automation.subprocess.STDOUT
    assert calls[0][1]["stdout"].name.endswith("portal-start-worker.log")


def test_start_pipeline_scheduled_task_uses_configured_name(monkeypatch):
    calls = []
    fake_settings = SimpleNamespace(
        project_root="C:/project",
        windows_automation=SimpleNamespace(
            pipeline_task_name="JobSimpleSearch-Test",
        ),
    )
    monkeypatch.setattr(windows_automation, "_require_windows", lambda: None)
    monkeypatch.setattr(windows_automation, "get_settings", lambda: fake_settings)
    monkeypatch.setattr(
        windows_automation.subprocess,
        "run",
        lambda command, **kwargs: calls.append((command, kwargs)),
    )

    task_name = windows_automation.start_pipeline_scheduled_task()

    assert task_name == "JobSimpleSearch-Test"
    assert calls[0][0] == [
        "schtasks.exe",
        "/Run",
        "/TN",
        "JobSimpleSearch-Test",
    ]
    assert calls[0][1]["check"] is True


def test_portal_shutdown_uses_independent_scheduled_task(monkeypatch):
    calls = []
    fake_settings = SimpleNamespace(
        project_root="C:/project",
        windows_automation=SimpleNamespace(
            portal_stop_task_name="JobSimpleSearch-Test-Close",
        ),
    )
    monkeypatch.setattr(windows_automation, "_require_windows", lambda: None)
    monkeypatch.setattr(windows_automation, "get_settings", lambda: fake_settings)
    monkeypatch.setattr(
        windows_automation.subprocess,
        "run",
        lambda command, **kwargs: calls.append((command, kwargs)),
    )

    task_name = windows_automation.schedule_review_portal_shutdown()

    assert task_name == "JobSimpleSearch-Test-Close"
    assert calls[0][0] == [
        "schtasks.exe",
        "/Run",
        "/TN",
        "JobSimpleSearch-Test-Close",
    ]
    assert calls[0][1]["check"] is True


def test_windows_pipeline_reopens_portal_before_results_email(monkeypatch):
    events = []

    monkeypatch.setattr(
        windows_automation,
        "start_review_portal",
        lambda: events.append("portal") or {"status": "STARTED"},
    )
    monkeypatch.setattr(
        windows_automation,
        "send_pipeline_complete_email",
        lambda _summary: events.append("email") or True,
    )

    def fake_continue_after_review(*, email_sender):
        assert email_sender({"status": "COMPLETE"}) is True
        events.append("pipeline_return")
        return {"status": "COMPLETE", "notification": {"status": "SENT"}}

    monkeypatch.setattr(
        windows_automation,
        "continue_after_review",
        fake_continue_after_review,
    )

    result = windows_automation.run_windows_pipeline_worker()

    assert events == ["portal", "email", "pipeline_return"]
    assert result["portal"] == {"status": "STARTED"}
