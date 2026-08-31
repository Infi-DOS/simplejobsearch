from __future__ import annotations

import sqlite3
from typing import Iterable

from post_ai_engine import (
    derive_ai_data_hybrid,
    evaluate_and_persist_post_ai,
    load_job_for_post_ai,
)
from rule_engine import classify_post_ai, load_post_rules

from ..db import connect


def run_post_ai(
    *,
    job_ids: Iterable[str] | None = None,
    connection: sqlite3.Connection | None = None,
    reapply: bool = False,
) -> dict:
    owns_connection = connection is None
    connection = connection or connect()
    try:
        selected = list(dict.fromkeys(job_ids or []))
        where = ["j.ai_status = 'EXTRACTED'", "f.schema_version = 'job_facts_v2'"]
        params: list[object] = []
        if job_ids is not None:
            if not selected:
                return {
                    "processed": 0, "SHORTLIST": 0, "REVIEW": 0,
                    "REJECT": 0, "job_ids": [],
                }
            where.append(f"j.job_id IN ({','.join('?' for _ in selected)})")
            params.extend(selected)
        if not reapply:
            where.append("(j.post_ai_status IS NULL OR TRIM(j.post_ai_status) = '')")
        rows = connection.execute(
            f"""
            SELECT j.job_id FROM jobs j
            JOIN job_ai_facts f ON f.job_id = j.job_id
            WHERE {' AND '.join(where)}
            ORDER BY j.ai_extracted_at, j.job_id
            """,
            params,
        ).fetchall()
        rules = load_post_rules(connection)
        counts = {
            "processed": 0, "SHORTLIST": 0, "REVIEW": 0,
            "REJECT": 0, "job_ids": [],
        }
        connection.execute("BEGIN IMMEDIATE")
        for row in rows:
            result = evaluate_and_persist_post_ai(connection, row["job_id"], rules=rules)
            counts["processed"] += 1
            counts[result.post_ai_status] += 1
            counts["job_ids"].append(row["job_id"])
        connection.commit()
        return counts
    except Exception:
        connection.rollback()
        raise
    finally:
        if owns_connection:
            connection.close()


__all__ = [
    "classify_post_ai",
    "derive_ai_data_hybrid",
    "load_job_for_post_ai",
    "run_post_ai",
]

