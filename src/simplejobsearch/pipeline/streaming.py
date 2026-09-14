from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Iterable
from typing import Any

from ..db import database
from ..workflow import TERMINAL_POST_AI_STATUSES
from .ai_extractor import run_ai_job_stream_async
from .details import fetch_approved_details
from .metadata_gate import run_metadata_gate
from .post_ai import run_post_ai


def _selected_rows(job_ids: tuple[str, ...], columns: str) -> list:
    if not job_ids:
        return []
    placeholders = ",".join("?" for _ in job_ids)
    with database() as connection:
        return connection.execute(
            f"SELECT job_id, {columns} FROM jobs WHERE job_id IN ({placeholders})",
            job_ids,
        ).fetchall()


def _initial_fetched_ids(job_ids: tuple[str, ...]) -> list[str]:
    return [
        row["job_id"]
        for row in _selected_rows(
            job_ids,
            "details_status, classifier_status, human_decision",
        )
        if row["details_status"] == "FETCHED"
        and (
            str(row["human_decision"] or "").strip().upper() == "KEEP"
            or (
                not str(row["human_decision"] or "").strip()
                and row["classifier_status"] == "AUTO_KEEP"
            )
        )
    ]


def _approved_count(job_ids: tuple[str, ...]) -> int:
    return sum(
        1
        for row in _selected_rows(job_ids, "classifier_status, human_decision")
        if str(row["human_decision"] or "").strip().upper() == "KEEP"
        or (
            not str(row["human_decision"] or "").strip()
            and row["classifier_status"] == "AUTO_KEEP"
        )
    )


def _metadata_status(job_id: str) -> str:
    with database() as connection:
        row = connection.execute(
            "SELECT metadata_gate_status FROM jobs WHERE job_id = ?",
            (job_id,),
        ).fetchone()
    return str(row["metadata_gate_status"] or "") if row else ""


def _post_ai_snapshot(job_ids: tuple[str, ...]) -> dict[str, str]:
    return {
        row["job_id"]: str(row["post_ai_status"] or "")
        for row in _selected_rows(job_ids, "post_ai_status")
    }


def _remaining_post_ai_ids(job_ids: tuple[str, ...]) -> list[str]:
    terminal = set(TERMINAL_POST_AI_STATUSES)
    return [
        row["job_id"]
        for row in _selected_rows(
            job_ids,
            "details_status, metadata_gate_status, ai_status, post_ai_status",
        )
        if row["details_status"] == "FETCHED"
        and row["metadata_gate_status"] == "PASS"
        and row["ai_status"] == "EXTRACTED"
        and str(row["post_ai_status"] or "").strip() not in terminal
    ]


async def run_streaming_pipeline_async(
    *,
    job_ids: Iterable[str],
    details_stage: Callable = fetch_approved_details,
    metadata_stage: Callable = run_metadata_gate,
    ai_stream_stage: Callable = run_ai_job_stream_async,
    post_ai_stage: Callable = run_post_ai,
) -> dict[str, Any]:
    """Overlap detail fetching with metadata, AI extraction, and Post-AI."""
    selected = tuple(dict.fromkeys(job_ids))
    ready_queue: asyncio.Queue[str | None] = asyncio.Queue()
    metadata = {"processed": 0, "PASS": 0, "REJECT": 0, "job_ids": []}
    metadata_errors: list[str] = []
    before_post_ai = _post_ai_snapshot(selected)
    loop = asyncio.get_running_loop()

    def on_fetched(job_id: str) -> None:
        asyncio.run_coroutine_threadsafe(ready_queue.put(job_id), loop).result()

    async def produce() -> dict:
        for job_id in _initial_fetched_ids(selected):
            await ready_queue.put(job_id)
        try:
            return await asyncio.to_thread(
                details_stage,
                job_ids=selected,
                on_fetched=on_fetched,
            )
        finally:
            await ready_queue.put(None)

    async def metadata_pass_jobs() -> AsyncIterator[str]:
        seen: set[str] = set()
        while True:
            job_id = await ready_queue.get()
            if job_id is None:
                break
            if job_id in seen:
                continue
            seen.add(job_id)
            try:
                result = await asyncio.to_thread(metadata_stage, job_ids=[job_id])
            except Exception as exc:  # noqa: BLE001 - drain independent jobs first
                metadata_errors.append(f"{job_id}: {type(exc).__name__}: {exc}")
                continue
            metadata["processed"] += int(result.get("processed", 0))
            metadata["PASS"] += int(result.get("PASS", 0))
            metadata["REJECT"] += int(result.get("REJECT", 0))
            metadata["job_ids"].extend(result.get("job_ids", []))
            if _metadata_status(job_id) == "PASS":
                yield job_id

    producer = asyncio.create_task(produce())
    try:
        ai = await ai_stream_stage(
            job_ids=metadata_pass_jobs(),
            total_hint=_approved_count(selected),
        )
    finally:
        details = await producer

    if metadata_errors:
        raise RuntimeError("Metadata gate failed for " + "; ".join(metadata_errors))

    remaining = _remaining_post_ai_ids(selected)
    sweep = await asyncio.to_thread(post_ai_stage, job_ids=remaining)
    after_post_ai = _post_ai_snapshot(selected)
    transitions = [
        status
        for job_id, status in after_post_ai.items()
        if status in TERMINAL_POST_AI_STATUSES
        and before_post_ai.get(job_id, "") not in TERMINAL_POST_AI_STATUSES
    ]
    post_ai = {
        "processed": len(transitions),
        "SHORTLIST": transitions.count("SHORTLIST"),
        "REVIEW": transitions.count("REVIEW"),
        "REJECT": transitions.count("REJECT"),
        "job_ids": list(dict.fromkeys(ai.get("job_ids", []) + sweep.get("job_ids", []))),
    }
    return {
        "details": details,
        "metadata": metadata,
        "ai": ai,
        "post_ai": post_ai,
    }


def run_streaming_pipeline(**kwargs) -> dict[str, Any]:
    return asyncio.run(run_streaming_pipeline_async(**kwargs))
