from __future__ import annotations

import asyncio
from collections.abc import Iterable

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


def run_ai_extraction(*, job_ids: Iterable[str] | None = None) -> dict:
    return asyncio.run(run_ai_extraction_async(job_ids=job_ids))
