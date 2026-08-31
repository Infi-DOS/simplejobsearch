from __future__ import annotations

from typing import Callable, Iterable

import details_fetcher as legacy

from ..config import get_settings
from ..db import connect


def fetch_approved_details(
    *,
    job_ids: Iterable[str] | None = None,
    client_factory: Callable = legacy.build_linkedin_client,
    fetcher: Callable = legacy.fetch_job_details,
    wait: Callable[[], None] | None = None,
) -> dict:
    """Fetch approved jobs without re-running PRE_DESCRIPTION classification."""
    selected = set(job_ids) if job_ids is not None else None
    settings = get_settings().search
    connection = connect()
    try:
        queue = legacy.load_detail_queue(connection)
        if selected is not None:
            queue = [row for row in queue if row["job_id"] in selected]
        queue = queue[: settings.details_max_jobs_per_run]
        if not queue:
            return {
                "processed": 0, "fetched": 0, "failed": 0,
                "stopped_early": False, "fetched_job_ids": [], "failed_job_ids": [],
            }

        client = client_factory()
        summary = {
            "processed": 0, "fetched": 0, "failed": 0,
            "stopped_early": False, "fetched_job_ids": [], "failed_job_ids": [],
        }
        consecutive_empty = 0
        wait_between = wait or legacy.wait_random
        for index, job in enumerate(queue, start=1):
            legacy.mark_attempt(connection, job["job_id"])
            try:
                details = fetcher(client, job["job_id"])
                description = details.get("description") if details else None
                if not description or not str(description).strip():
                    raise ValueError("LinkedIn returned no description/details")
                legacy.save_success(connection, job["job_id"], details)
            except Exception as exc:
                legacy.save_failure(
                    connection,
                    job["job_id"],
                    f"{type(exc).__name__}: {exc}",
                )
                summary["failed"] += 1
                summary["failed_job_ids"].append(job["job_id"])
                consecutive_empty += 1
            else:
                summary["fetched"] += 1
                summary["fetched_job_ids"].append(job["job_id"])
                consecutive_empty = 0
            summary["processed"] += 1
            if consecutive_empty >= settings.details_stop_after_empty:
                summary["stopped_early"] = True
                break
            if index < len(queue):
                wait_between()
        return summary
    finally:
        connection.close()

