from __future__ import annotations

import asyncio
import json
import sys
from types import SimpleNamespace

import pytest

import ai_extractor as legacy
from jobsimplesearch import config
from jobsimplesearch.pipeline.retry import (
    is_transient_ai_error,
    retry_delay_seconds,
)

legacy_runtime = sys.modules[legacy.process_job.__module__]


class ServerDisconnectedError(Exception):
    pass


class ProviderError(Exception):
    def __init__(self, status_code):
        super().__init__(f"provider returned {status_code}")
        self.status_code = status_code


def test_transient_retry_classification_is_selective():
    assert is_transient_ai_error(ServerDisconnectedError("server disconnected"))
    assert is_transient_ai_error(ProviderError(429))
    assert is_transient_ai_error(ProviderError(503))
    assert not is_transient_ai_error(ProviderError(401))
    assert not is_transient_ai_error(
        RuntimeError("Pydantic validation failed after configured repair attempts")
    )


def test_retry_delay_is_bounded_exponential_with_jitter():
    assert retry_delay_seconds(
        1, base_seconds=2, max_seconds=5, random_value=lambda: 0
    ) == 1
    assert retry_delay_seconds(
        2, base_seconds=2, max_seconds=5, random_value=lambda: 1
    ) == 4
    assert retry_delay_seconds(
        4, base_seconds=2, max_seconds=5, random_value=lambda: 1
    ) == 5


def test_invalid_retry_configuration_fails_clearly(monkeypatch):
    monkeypatch.setenv("AI_RETRY_BASE_SECONDS", "10")
    monkeypatch.setenv("AI_RETRY_MAX_SECONDS", "5")
    config.reset_settings_cache()
    with pytest.raises(ValueError, match="AI_RETRY_MAX_SECONDS"):
        config.get_settings()
    config.reset_settings_cache()


def test_transient_failure_retries_and_counts_real_provider_attempts(monkeypatch):
    events = []
    calls = 0

    class DummyConnection:
        def close(self):
            pass

    async def fake_extract(
        _aclient,
        _handler,
        _job,
        on_request_acquired=None,
    ):
        nonlocal calls
        calls += 1
        await on_request_acquired(123)
        if calls == 1:
            raise ServerDisconnectedError("Server disconnected")
        facts = SimpleNamespace(
            primary_role_family="machine_learning",
            seniority="entry",
            minimum_years_experience=None,
            student_status_required=False,
            languages_required=["English"],
        )
        return object(), facts

    async def no_sleep(_delay):
        events.append("slept")

    monkeypatch.setattr(legacy_runtime, "MAX_ATTEMPTS_PER_JOB", 3)
    monkeypatch.setattr(legacy_runtime, "connect_database", DummyConnection)
    monkeypatch.setattr(
        legacy_runtime,
        "mark_attempt",
        lambda _connection, job_id: events.append(("attempt", job_id)),
    )
    monkeypatch.setattr(
        legacy_runtime,
        "save_failure",
        lambda _connection, job_id, reason: events.append(("failure", job_id, reason)),
    )
    monkeypatch.setattr(
        legacy_runtime,
        "save_success",
        lambda _connection, job_id, _response, _facts: events.append(("success", job_id)),
    )
    monkeypatch.setattr(legacy_runtime, "extract_facts", fake_extract)
    monkeypatch.setattr(
        legacy_runtime,
        "response_usage",
        lambda _response: {"input_tokens": 1, "output_tokens": 2, "thought_tokens": 3},
    )
    monkeypatch.setattr(
        legacy_runtime, "retry_delay_seconds", lambda *_args, **_kwargs: 0
    )
    monkeypatch.setattr(legacy_runtime.asyncio, "sleep", no_sleep)

    result = asyncio.run(
        legacy.process_job(
            object(),
            object(),
            asyncio.Lock(),
            {
                "job_id": "li-retry",
                "title": "ML Engineer",
                "company": "Example",
                "ai_attempt_count": 0,
            },
            1,
            1,
            run_post_ai_after_extraction=False,
        )
    )

    assert result == ("EXTRACTED", "li-retry")
    assert calls == 2
    assert [event for event in events if isinstance(event, tuple) and event[0] == "attempt"] == [
        ("attempt", "li-retry"),
        ("attempt", "li-retry"),
    ]
    assert sum(isinstance(event, tuple) and event[0] == "failure" for event in events) == 1
    assert ("success", "li-retry") in events
    assert "slept" in events


def test_deterministic_failure_is_not_retried(monkeypatch):
    attempts = []

    class DummyConnection:
        def close(self):
            pass

    async def deterministic_failure(
        _aclient,
        _handler,
        _job,
        on_request_acquired=None,
    ):
        await on_request_acquired(123)
        raise RuntimeError("Pydantic validation failed after repair")

    monkeypatch.setattr(legacy_runtime, "MAX_ATTEMPTS_PER_JOB", 3)
    monkeypatch.setattr(legacy_runtime, "connect_database", DummyConnection)
    monkeypatch.setattr(
        legacy_runtime,
        "mark_attempt",
        lambda _connection, job_id: attempts.append(job_id),
    )
    monkeypatch.setattr(legacy_runtime, "save_failure", lambda *_args: None)
    monkeypatch.setattr(legacy_runtime, "extract_facts", deterministic_failure)

    result = asyncio.run(
        legacy.process_job(
            object(),
            object(),
            asyncio.Lock(),
            {
                "job_id": "li-invalid",
                "title": "AI Engineer",
                "company": "Example",
                "ai_attempt_count": 0,
            },
            1,
            1,
            run_post_ai_after_extraction=False,
        )
    )

    assert result == ("FAILED", "li-invalid")
    assert attempts == ["li-invalid"]


def test_schema_repair_is_counted_as_a_provider_attempt(monkeypatch):
    valid_facts = {
        "primary_role_family": "machine_learning",
        "role_families": ["machine_learning"],
        "seniority": "entry",
        "degree_required": False,
        "minimum_degree_level": "none_specified",
        "degree_fields": [],
        "phd_preferred": False,
        "student_status_required": False,
        "languages_required": [],
        "languages_preferred": [],
        "languages_bonus": [],
        "technical_skills_required": [],
        "technical_skills_preferred": [],
        "role_summary": "Build ML systems.",
        "core_responsibilities": ["Build models"],
        "remote_mode": "unspecified",
        "management_responsibility": False,
        "security_clearance_required": False,
        "extraction_confidence": 0.9,
        "evidence": [],
    }
    responses = iter(
        [
            SimpleNamespace(text="{}"),
            SimpleNamespace(text=json.dumps(valid_facts)),
        ]
    )
    attempts = []

    async def fake_generate(
        _aclient,
        _handler,
        _prompt,
        on_acquired=None,
    ):
        await on_acquired(100)
        return next(responses)

    monkeypatch.setattr(legacy_runtime, "initial_prompt", lambda _job: "initial")
    monkeypatch.setattr(legacy_runtime, "repair_prompt", lambda *_args: "repair")
    monkeypatch.setattr(legacy_runtime, "rate_limited_generate", fake_generate)
    monkeypatch.setattr(legacy_runtime, "MAX_SCHEMA_REPAIR_ATTEMPTS", 1)

    _response, facts = asyncio.run(
        legacy.extract_facts(
            object(),
            object(),
            {},
            on_request_acquired=lambda estimated: _record_attempt(attempts, estimated),
        )
    )

    assert facts.primary_role_family == "machine_learning"
    assert attempts == [100, 100]


async def _record_attempt(attempts, estimated):
    attempts.append(estimated)
