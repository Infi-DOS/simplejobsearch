from __future__ import annotations

import asyncio
from types import SimpleNamespace

from simplejobsearch.pipeline import ai_extractor


def test_ai_processed_counts_attempt_delta(monkeypatch):
    snapshots = iter([{"li-1": 2, "li-2": 4}, {"li-1": 3, "li-2": 4}])

    async def async_main(**_kwargs):
        return {"processed": 1, "extracted": 1, "failed": 0}

    monkeypatch.setattr(ai_extractor, "_attempt_counts", lambda _job_ids: next(snapshots))
    monkeypatch.setitem(
        __import__("sys").modules,
        "ai_extractor",
        SimpleNamespace(async_main=async_main),
    )

    result = asyncio.run(ai_extractor.run_ai_extraction_async(job_ids=["li-1", "li-2"]))

    assert result["attempted"] == 1


def test_ai_processed_is_zero_when_no_model_attempt_occurs(monkeypatch):
    snapshots = iter([{"li-1": 7}, {"li-1": 7}])

    async def async_main(**_kwargs):
        return {"processed": 0, "extracted": 0, "failed": 0}

    monkeypatch.setattr(ai_extractor, "_attempt_counts", lambda _job_ids: next(snapshots))
    monkeypatch.setitem(
        __import__("sys").modules,
        "ai_extractor",
        SimpleNamespace(async_main=async_main),
    )

    result = asyncio.run(ai_extractor.run_ai_extraction_async(job_ids=["li-1"]))

    assert result["attempted"] == 0
