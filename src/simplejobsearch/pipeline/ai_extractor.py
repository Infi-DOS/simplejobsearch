from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, Iterable

from ..db import database


def _attempt_counts(job_ids: list[str] | None) -> dict[str, int]:
    where = ""
    params: list[object] = []
    if job_ids is not None:
        if not job_ids:
            return {}
        where = f"WHERE job_id IN ({','.join('?' for _ in job_ids)})"
        params.extend(job_ids)
    with database() as connection:
        return {
            row["job_id"]: int(row["ai_attempt_count"] or 0)
            for row in connection.execute(
                f"SELECT job_id, ai_attempt_count FROM jobs {where}",
                params,
            )
        }


async def run_ai_extraction_async(*, job_ids: Iterable[str] | None = None) -> dict:
    import ai_extractor as legacy

    selected = list(dict.fromkeys(job_ids)) if job_ids is not None else None
    before = _attempt_counts(selected)
    result = await legacy.async_main(
        job_ids=selected,
        run_post_ai_after_extraction=False,
    )
    after = _attempt_counts(selected)
    result["attempted"] = sum(
        max(0, after.get(job_id, 0) - before.get(job_id, 0))
        for job_id in after
    )
    return result


async def run_ai_job_stream_async(
    *,
    job_ids: AsyncIterable[str],
    total_hint: int = 0,
) -> dict:
    """Process job IDs as they become ready using one shared provider limiter."""
    import ai_extractor as legacy

    legacy.require_api_key()
    handler = legacy.RollingRateHandler(
        rpm=legacy.TARGET_RPM,
        input_tpm=legacy.EFFECTIVE_TPM,
        max_concurrency=legacy.MAX_CONCURRENCY,
    )
    db_lock = asyncio.Lock()
    results: list[tuple[str, str]] = []
    pending: set[asyncio.Task] = set()
    seen: set[str] = set()
    before_attempts: dict[str, int] = {}
    started_ids: list[str] = []
    skipped_due_limit = 0
    max_started = 1 if legacy.AI_PROBE_MODE else legacy.MAX_JOBS_PER_RUN
    # A window of one in serial mode prevents another job from using the
    # provider while the current job is in its retry backoff.
    task_window = max(1, legacy.MAX_CONCURRENCY)

    async def drain_one() -> None:
        nonlocal pending
        done, pending = await asyncio.wait(
            pending,
            return_when=asyncio.FIRST_COMPLETED,
        )
        results.extend(task.result() for task in done)

    async with legacy.genai.Client(api_key=legacy.SETTINGS.ai.api_key).aio as aclient:
        async for job_id in job_ids:
            if job_id in seen:
                continue
            seen.add(job_id)
            if max_started > 0 and len(started_ids) >= max_started:
                skipped_due_limit += 1
                continue

            connection = legacy.connect_database()
            try:
                queue = legacy.load_queue(connection, job_ids=[job_id])
            finally:
                connection.close()
            if not queue:
                continue

            job = queue[0]
            started_ids.append(job_id)
            before_attempts[job_id] = int(job["ai_attempt_count"] or 0)
            pending.add(
                asyncio.create_task(
                    legacy.process_job(
                        aclient,
                        handler,
                        db_lock,
                        job,
                        len(started_ids),
                        total_hint or len(started_ids),
                        run_post_ai_after_extraction=True,
                    )
                )
            )
            if len(pending) >= task_window:
                await drain_one()

        if pending:
            done = await asyncio.gather(*pending)
            results.extend(done)

    after_attempts = _attempt_counts(started_ids)
    extracted = sum(status == "EXTRACTED" for status, _job_id in results)
    failed = sum(status == "FAILED" for status, _job_id in results)
    return {
        "processed": len(results),
        "attempted": sum(
            max(0, after_attempts.get(job_id, 0) - before_attempts.get(job_id, 0))
            for job_id in started_ids
        ),
        "extracted": extracted,
        "failed": failed,
        "job_ids": [job_id for _status, job_id in results],
        "skipped_due_limit": skipped_due_limit,
    }


def run_ai_extraction(*, job_ids: Iterable[str] | None = None) -> dict:
    return asyncio.run(run_ai_extraction_async(job_ids=job_ids))
