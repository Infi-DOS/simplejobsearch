from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from rule_engine import (
    MetadataClassification,
    classify_metadata_gate,
    load_metadata_rules,
)
from simplejobsearch.config import get_settings
from simplejobsearch.db import connect as shared_connect


PROJECT_DIR = Path(__file__).resolve().parent
SETTINGS = get_settings()
DATABASE_PATH = SETTINGS.database_path
TIMEZONE = SETTINGS.timezone

METADATA_GATE_VERSION = "metadata_gate_v1"

# Hard allow-list. Description is deliberately absent.
METADATA_FIELDS = (
    "job_id",
    "title",
    "company",
    "location",
    "is_remote",
    "job_type",
    "job_level",
    "job_function",
    "company_industry",
    "human_decision",
    "classifier_status",
)


def now_local() -> str:
    return datetime.now(TIMEZONE).isoformat()


def connect_database() -> sqlite3.Connection:
    return shared_connect(DATABASE_PATH)


def load_metadata_job(
    connection: sqlite3.Connection,
    job_id: str,
) -> dict:
    """
    Load only structured metadata.

    Do not replace the explicit field list with a wildcard query. The job
    description is intentionally unavailable to the metadata-gate classifier.
    """

    row = connection.execute(
        """
        SELECT
            job_id,
            title,
            company,
            location,
            is_remote,
            job_type,
            job_level,
            job_function,
            company_industry,
            human_decision,
            classifier_status

        FROM jobs
        WHERE job_id = ?
        """,
        (job_id,),
    ).fetchone()

    if row is None:
        raise KeyError(
            f"Unknown job_id: {job_id}"
        )

    job = dict(row)

    if set(job) != set(METADATA_FIELDS):
        raise RuntimeError(
            "Metadata-gate field whitelist mismatch."
        )

    if "description" in job:
        raise RuntimeError(
            "Metadata gate must never receive description."
        )

    return job


def persist_metadata_gate_result(
    connection: sqlite3.Connection,
    job_id: str,
    result: MetadataClassification,
) -> None:

    connection.execute(
        """
        UPDATE jobs
        SET
            metadata_gate_status = ?,
            metadata_gate_reason = ?,
            metadata_gate_rule_id = ?,
            metadata_gate_rule_key = ?,
            metadata_gate_evaluated_at = ?,
            metadata_gate_version = ?,

            ai_status =
                CASE
                    WHEN ? = 'REJECT'
                         AND (
                            ai_status IS NULL
                            OR TRIM(ai_status) = ''
                            OR ai_status = 'PENDING'
                            OR ai_status = 'FAILED'
                            OR ai_status = 'BLOCKED_BY_GATE'
                            OR ai_status = 'BLOCKED_BY_METADATA_GATE'
                            OR ai_status = 'WAITING_GATE_REVIEW'
                         )
                    THEN 'BLOCKED_BY_METADATA_GATE'

                    WHEN ? = 'PASS'
                         AND (
                            ai_status IS NULL
                            OR TRIM(ai_status) = ''
                            OR ai_status = 'BLOCKED_BY_GATE'
                            OR ai_status = 'BLOCKED_BY_METADATA_GATE'
                            OR ai_status = 'WAITING_GATE_REVIEW'
                         )
                    THEN 'PENDING'

                    ELSE ai_status
                END

        WHERE job_id = ?
        """,
        (
            result.metadata_gate_status,
            result.reason,
            result.matched_rule_id,
            result.matched_rule_key,
            now_local(),
            METADATA_GATE_VERSION,
            result.metadata_gate_status,
            result.metadata_gate_status,
            job_id,
        ),
    )

    connection.commit()


def evaluate_and_persist(
    connection: sqlite3.Connection,
    job_id: str,
    rules: list[dict] | None = None,
) -> MetadataClassification:

    if rules is None:
        rules = load_metadata_rules(
            connection
        )

    job = load_metadata_job(
        connection,
        job_id,
    )

    result = classify_metadata_gate(
        job,
        rules,
    )

    persist_metadata_gate_result(
        connection,
        job_id,
        result,
    )

    return result


def run_all_fetched_jobs(
    connection: sqlite3.Connection | None = None,
) -> dict:

    owns_connection = connection is None

    if connection is None:
        connection = connect_database()

    try:
        rules = load_metadata_rules(
            connection
        )

        job_ids = [
            row["job_id"]
            for row in connection.execute(
                """
                SELECT job_id
                FROM jobs
                WHERE details_status = 'FETCHED'
                ORDER BY details_fetched_at, job_id
                """
            ).fetchall()
        ]

        counts = {
            "processed": 0,
            "PASS": 0,
            "REJECT": 0,
        }

        for job_id in job_ids:

            result = evaluate_and_persist(
                connection,
                job_id,
                rules=rules,
            )

            counts["processed"] += 1
            counts[
                result.metadata_gate_status
            ] += 1

        return counts

    finally:
        if owns_connection:
            connection.close()
