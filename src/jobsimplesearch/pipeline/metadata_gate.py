from __future__ import annotations

import sqlite3
from typing import Iterable

from metadata_gate import METADATA_FIELDS, evaluate_and_persist, load_metadata_job
from rule_engine import classify_metadata_gate, load_metadata_rules

from ..db import connect


def run_metadata_gate(
    *,
    job_ids: Iterable[str] | None = None,
    connection: sqlite3.Connection | None = None,
    reapply: bool = False,
) -> dict:
    """Evaluate structured metadata only; descriptions are never selected."""
    owns_connection = connection is None
    connection = connection or connect()
    try:
        params: list[object] = []
        where = ["details_status = 'FETCHED'"]
        selected = list(dict.fromkeys(job_ids or []))
        if job_ids is not None:
            if not selected:
                return {"processed": 0, "PASS": 0, "REJECT": 0, "job_ids": []}
            where.append(f"job_id IN ({','.join('?' for _ in selected)})")
            params.extend(selected)
        if not reapply:
            where.append("(metadata_gate_status IS NULL OR TRIM(metadata_gate_status) = '')")
        rows = connection.execute(
            f"SELECT job_id FROM jobs WHERE {' AND '.join(where)} ORDER BY details_fetched_at, job_id",
            params,
        ).fetchall()
        rules = load_metadata_rules(connection)
        counts = {"processed": 0, "PASS": 0, "REJECT": 0, "job_ids": []}
        for row in rows:
            result = evaluate_and_persist(connection, row["job_id"], rules=rules)
            counts["processed"] += 1
            counts[result.metadata_gate_status] += 1
            counts["job_ids"].append(row["job_id"])
        return counts
    finally:
        if owns_connection:
            connection.close()


__all__ = [
    "METADATA_FIELDS",
    "classify_metadata_gate",
    "load_metadata_job",
    "run_metadata_gate",
]

