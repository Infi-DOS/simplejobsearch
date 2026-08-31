from __future__ import annotations

from datetime import date
from typing import Any, Callable

from ..db import apply_migrations, database
from ..workflow import (
    attach_search_run,
    batch_summary,
    get_or_create_batch,
    now_iso,
    record_query_metrics,
    update_batch,
)
from .queries import sync_queries


def run_daily_search(
    *,
    batch_date: date | str | None = None,
    collector: Callable[[], dict[str, Any] | None] | None = None,
) -> dict[str, Any]:
    """Run discovery once and persist its measurable daily workflow state."""
    apply_migrations()
    with database() as connection:
        sync_queries(connection, now_iso())
        batch = get_or_create_batch(connection, batch_date)
        update_batch(
            connection,
            batch["batch_id"],
            status="SEARCHING",
            search_started_at=now_iso(),
            last_error=None,
        )

    if collector is None:
        import daily_collector as legacy_collector
        from .queries import as_legacy_searches

        legacy_collector.SEARCHES = as_legacy_searches()
        collector = legacy_collector.main

    try:
        result = collector() or {}
        run_id = result.get("run_id")
        if not run_id:
            with database() as connection:
                row = connection.execute(
                    "SELECT run_id FROM search_runs ORDER BY started_at DESC LIMIT 1"
                ).fetchone()
                run_id = row[0] if row else None
        if not run_id:
            raise RuntimeError("Collector completed without creating a search run")

        with database() as connection:
            counts = attach_search_run(connection, batch["batch_id"], run_id)
            record_query_metrics(connection, run_id)
            summary = batch_summary(connection, batch["batch_id"])
            summary.update(counts)
            summary["run_id"] = run_id
            summary["search_failures"] = len(result.get("failures", []))
            return summary
    except Exception as exc:
        with database() as connection:
            update_batch(
                connection,
                batch["batch_id"],
                status="FAILED",
                search_completed_at=now_iso(),
                last_error=f"{type(exc).__name__}: {exc}",
            )
        raise
