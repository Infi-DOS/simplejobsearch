from __future__ import annotations

import json
import random
import sqlite3
import time
from datetime import datetime
from enum import Enum
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from jobspy.linkedin import LinkedIn
from jobspy.model import DescriptionFormat

from rule_engine import (
    classify_pre_description,
    load_category_defaults,
    load_pre_rules,
)

from metadata_gate import (
    evaluate_and_persist,
)
from jobsimplesearch.config import get_settings
from jobsimplesearch.db import connect as shared_connect


# =============================================================================
# CONFIG
# =============================================================================

PROJECT_DIR = Path(__file__).resolve().parent
SETTINGS = get_settings()
DATABASE_PATH = SETTINGS.database_path

TIMEZONE = SETTINGS.timezone

# Detail requests are more expensive and LinkedIn can rate-limit aggressively.
MAX_JOBS_PER_RUN = SETTINGS.search.details_max_jobs_per_run
MAX_ATTEMPTS_PER_JOB = SETTINGS.search.details_max_attempts_per_job

DELAY_MIN_SECONDS = SETTINGS.search.details_delay_min_seconds
DELAY_MAX_SECONDS = SETTINGS.search.details_delay_max_seconds

# Several consecutive empty detail pages can indicate temporary blocking or a
# LinkedIn page-format problem. Stop this run rather than hammering the site.
STOP_AFTER_CONSECUTIVE_EMPTY = SETTINGS.search.details_stop_after_empty


# =============================================================================
# HELPERS
# =============================================================================

def now_local() -> str:
    return datetime.now(
        TIMEZONE
    ).isoformat()


def wait_random() -> None:

    delay = random.uniform(
        DELAY_MIN_SECONDS,
        DELAY_MAX_SECONDS,
    )

    print(
        f"Waiting {delay:.1f}s..."
    )

    time.sleep(
        delay
    )


def normalize_detail_value(
    value,
):
    """
    Normalize JobSpy enums/containers into compact values.

    Examples:
      [JobType.FULL_TIME] -> "FULL_TIME"
      [JobType.CONTRACT]  -> "CONTRACT"
      JobType.INTERNSHIP  -> "INTERNSHIP"
    """
    if value is None:
        return None

    if isinstance(
        value,
        Enum,
    ):
        return value.name

    if isinstance(
        value,
        (
            str,
            int,
            float,
            bool,
        ),
    ):
        return value

    if isinstance(
        value,
        (
            list,
            tuple,
            set,
        ),
    ):
        cleaned = [
            normalize_detail_value(
                item
            )
            for item in value
        ]

        cleaned = [
            item
            for item in cleaned
            if item is not None
        ]

        if not cleaned:
            return None

        if len(cleaned) == 1:
            return cleaned[0]

        return json.dumps(
            cleaned,
            ensure_ascii=False,
            default=str,
        )

    if isinstance(
        value,
        dict,
    ):
        cleaned = {
            str(key):
                normalize_detail_value(
                    item
                )
            for key, item
            in value.items()
        }

        return json.dumps(
            cleaned,
            ensure_ascii=False,
            default=str,
        )

    enum_name = getattr(
        value,
        "name",
        None,
    )

    if enum_name:
        return str(
            enum_name
        )

    return str(
        value
    )


def sqlite_text(value):
    return normalize_detail_value(
        value
    )


def connect_database() -> sqlite3.Connection:
    return shared_connect(DATABASE_PATH)


def verify_database(
    connection: sqlite3.Connection,
) -> None:

    required_columns = {
        "details_attempt_count",
        "details_last_attempt_at",
        "details_last_error",

        "metadata_gate_status",
        "metadata_gate_reason",
        "metadata_gate_rule_id",
        "metadata_gate_rule_key",
        "metadata_gate_evaluated_at",
        "metadata_gate_version",

        "ai_status",
    }

    columns = {
        row["name"]
        for row
        in connection.execute(
            "PRAGMA table_info(jobs)"
        ).fetchall()
    }

    missing = (
        required_columns
        - columns
    )

    if missing:
        raise RuntimeError(
            "Missing detail-tracking columns: "
            + ", ".join(
                sorted(
                    missing
                )
            )
            + "\nRun setup_details_tracking.py, setup_ai_pipeline.py, "
              "setup_pipeline_v2.py, and setup_metadata_gate.py first."
        )


    required_tables = {
        "review_categories",
        "pipeline_rules",
    }

    tables = {
        row["name"]
        for row
        in connection.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
            """
        ).fetchall()
    }

    missing_tables = (
        required_tables
        - tables
    )

    if missing_tables:
        raise RuntimeError(
            "Missing rule-engine tables: "
            + ", ".join(
                sorted(
                    missing_tables
                )
            )
            + "\nRun setup_rule_engine.py first."
        )


def linkedin_numeric_id(
    job_id: str,
) -> str:

    value = str(
        job_id
        or ""
    ).strip()

    if value.startswith(
        "li-"
    ):
        value = value[3:]

    if not value.isdigit():
        raise ValueError(
            f"Unexpected LinkedIn job ID: {job_id}"
        )

    return value


# =============================================================================
# QUEUE
# =============================================================================

def load_detail_queue(
    connection: sqlite3.Connection,
) -> list[sqlite3.Row]:

    rows = connection.execute(
        """
        SELECT
            j.job_id,
            j.site,
            j.title,
            j.company,
            j.location,
            j.date_posted,
            j.job_url,
            j.is_remote,

            j.human_decision,
            j.classifier_status,
            j.review_category,
            j.review_reason,
            j.suggested_action,

            j.details_status,
            j.details_attempt_count,

            j.first_seen_at

        FROM approved_for_details a

        JOIN jobs j
            ON j.job_id = a.job_id

        WHERE
            LOWER(
                COALESCE(
                    j.site,
                    ''
                )
            ) = 'linkedin'

            AND COALESCE(
                j.details_attempt_count,
                0
            ) < ?

        ORDER BY
            CASE
                WHEN j.human_decision = 'KEEP'
                    THEN 0
                ELSE 1
            END,

            j.first_seen_at,
            j.job_id

        LIMIT ?
        """,
        (
            MAX_ATTEMPTS_PER_JOB,
            MAX_JOBS_PER_RUN,
        ),
    ).fetchall()

    return rows



# =============================================================================
# CURRENT PRE-DESCRIPTION RULE CHECK
# =============================================================================

def refresh_pre_classification(
    connection: sqlite3.Connection,
    job: sqlite3.Row,
    rules: list[dict],
    category_defaults: dict[str, dict],
) -> str:
    """
    Re-evaluate current automatic rules immediately before an expensive detail
    request. Explicit human KEEP still wins and is never overridden here.
    """
    if str(
        job["human_decision"]
        or ""
    ).strip().upper() == "KEEP":
        return "HUMAN_KEEP"

    result = classify_pre_description(
        job=dict(
            job
        ),
        rules=rules,
        category_defaults=category_defaults,
    )

    connection.execute(
        """
        UPDATE jobs

        SET
            classifier_status = ?,
            review_category = ?,
            review_reason = ?,
            suggested_action = ?

        WHERE job_id = ?
        """,
        (
            result.classifier_status,
            result.review_category,
            result.review_reason,
            result.suggested_action,
            job["job_id"],
        ),
    )

    connection.commit()

    return result.classifier_status


# =============================================================================
# JOBSPY WRAPPER
#
# JobSpy does not expose a public "fetch this exact LinkedIn job ID" API.
# Its LinkedIn scraper already has the private method used internally when
# linkedin_fetch_description=True. We isolate that dependency here so a future
# JobSpy update requires changing one function rather than the pipeline.
# =============================================================================

def build_linkedin_client() -> LinkedIn:

    client = LinkedIn()

    # _get_job_details() consults scraper_input.description_format.
    client.scraper_input = (
        SimpleNamespace(
            description_format=(
                DescriptionFormat.MARKDOWN
            )
        )
    )

    return client


def fetch_job_details(
    client: LinkedIn,
    job_id: str,
) -> dict:

    numeric_id = (
        linkedin_numeric_id(
            job_id
        )
    )

    details = (
        client._get_job_details(
            numeric_id
        )
    )

    if not isinstance(
        details,
        dict,
    ):
        return {}

    return details


# =============================================================================
# PERSISTENCE
# =============================================================================

def mark_attempt(
    connection: sqlite3.Connection,
    job_id: str,
) -> None:

    connection.execute(
        """
        UPDATE jobs

        SET
            details_attempt_count =
                COALESCE(
                    details_attempt_count,
                    0
                ) + 1,

            details_last_attempt_at = ?,

            details_last_error = NULL

        WHERE job_id = ?
        """,
        (
            now_local(),
            job_id,
        ),
    )

    connection.commit()


def save_success(
    connection: sqlite3.Connection,
    job_id: str,
    details: dict,
) -> None:

    description = (
        details.get(
            "description"
        )
    )

    connection.execute(
        """
        UPDATE jobs

        SET
            description =
                COALESCE(
                    ?,
                    description
                ),

            job_url_direct =
                COALESCE(
                    ?,
                    job_url_direct
                ),

            job_type =
                COALESCE(
                    ?,
                    job_type
                ),

            job_level =
                COALESCE(
                    ?,
                    job_level
                ),

            job_function =
                COALESCE(
                    ?,
                    job_function
                ),

            company_industry =
                COALESCE(
                    ?,
                    company_industry
                ),

            details_status =
                'FETCHED',

            details_fetched_at =
                ?,

            details_last_error =
                NULL

        WHERE job_id = ?
        """,
        (
            sqlite_text(
                description
            ),
            sqlite_text(
                details.get(
                    "job_url_direct"
                )
            ),
            sqlite_text(
                details.get(
                    "job_type"
                )
            ),
            sqlite_text(
                details.get(
                    "job_level"
                )
            ),
            sqlite_text(
                details.get(
                    "job_function"
                )
            ),
            sqlite_text(
                details.get(
                    "company_industry"
                )
            ),
            now_local(),
            job_id,
        ),
    )

    connection.commit()


def save_failure(
    connection: sqlite3.Connection,
    job_id: str,
    reason: str,
) -> None:

    connection.execute(
        """
        UPDATE jobs

        SET
            details_status =
                'FAILED_OR_EMPTY',

            details_last_error =
                ?

        WHERE job_id = ?
        """,
        (
            reason,
            job_id,
        ),
    )

    connection.commit()


# =============================================================================
# SUMMARY
# =============================================================================

def scalar(
    connection: sqlite3.Connection,
    query: str,
    params: tuple = (),
):

    row = connection.execute(
        query,
        params,
    ).fetchone()

    return row[0]


def print_summary(
    connection: sqlite3.Connection,
    processed: int,
    fetched: int,
    failed: int,
    skipped_by_current_rules: int,
    stopped_early: bool,
) -> None:

    remaining = scalar(
        connection,
        """
        SELECT COUNT(*)

        FROM approved_for_details a

        JOIN jobs j
            ON j.job_id = a.job_id

        WHERE
            LOWER(
                COALESCE(
                    j.site,
                    ''
                )
            ) = 'linkedin'

            AND COALESCE(
                j.details_attempt_count,
                0
            ) < ?
        """,
        (
            MAX_ATTEMPTS_PER_JOB,
        ),
    )

    exhausted = scalar(
        connection,
        """
        SELECT COUNT(*)

        FROM approved_for_details a

        JOIN jobs j
            ON j.job_id = a.job_id

        WHERE
            LOWER(
                COALESCE(
                    j.site,
                    ''
                )
            ) = 'linkedin'

            AND COALESCE(
                j.details_attempt_count,
                0
            ) >= ?
        """,
        (
            MAX_ATTEMPTS_PER_JOB,
        ),
    )

    print()
    print("=" * 90)
    print("DETAIL FETCH SUMMARY")
    print("=" * 90)

    print(
        f"Processed this run:        {processed}"
    )

    print(
        f"Descriptions fetched:      {fetched}"
    )

    print(
        f"Failed / empty:             {failed}"
    )

    print(
        f"Skipped by current rules:   {skipped_by_current_rules}"
    )

    print(
        f"Eligible queue remaining:   {remaining}"
    )

    print(
        f"Retry limit exhausted:      {exhausted}"
    )

    print(
        f"Stopped early:              {stopped_early}"
    )


# =============================================================================
# MAIN
# =============================================================================

def main():

    print()
    print("=" * 90)
    print("APPROVED LINKEDIN DETAIL FETCHER")
    print("=" * 90)

    connection = (
        connect_database()
    )

    try:
        verify_database(
            connection
        )

        queue = load_detail_queue(
            connection
        )

        print(
            f"Jobs queued this run: "
            f"{len(queue)}"
        )

        if not queue:
            print(
                "Nothing to fetch."
            )
            return {
                "processed": 0,
                "fetched": 0,
                "failed": 0,
                "skipped_by_current_rules": 0,
                "stopped_early": False,
            }

        client = (
            build_linkedin_client()
        )

        processed = 0
        fetched = 0
        failed = 0
        skipped_by_current_rules = 0

        consecutive_empty = 0
        stopped_early = False

        for index, job in enumerate(
            queue,
            start=1,
        ):

            print()
            print("-" * 90)

            print(
                f"[{index}/{len(queue)}] "
                f"{job['title']} "
                f"— {job['company']}"
            )

            print(
                f"Job ID: "
                f"{job['job_id']}"
            )

            mark_attempt(
                connection,
                job["job_id"],
            )

            try:
                details = fetch_job_details(
                    client,
                    job["job_id"],
                )

            except Exception as exc:

                reason = (
                    f"{type(exc).__name__}: "
                    f"{exc}"
                )

                save_failure(
                    connection,
                    job["job_id"],
                    reason,
                )

                failed += 1
                processed += 1

                consecutive_empty += 1

                print(
                    f"FAILED: {reason}"
                )

            else:

                description = (
                    details.get(
                        "description"
                    )
                    if details
                    else None
                )

                if (
                    description
                    and str(
                        description
                    ).strip()
                ):

                    save_success(
                        connection,
                        job["job_id"],
                        details,
                    )

                    fetched += 1
                    processed += 1

                    consecutive_empty = 0

                    print(
                        "FETCHED"
                    )

                    print(
                        f"Description chars: "
                        f"{len(str(description))}"
                    )

                    print(
                        "Direct apply URL: "
                        + (
                            "yes"
                            if details.get(
                                "job_url_direct"
                            )
                            else "no"
                        )
                    )

                    print(
                        "Job type: "
                        + str(
                            details.get(
                                "job_type"
                            )
                        )
                    )

                else:

                    reason = (
                        "LinkedIn returned no "
                        "description/details."
                    )

                    save_failure(
                        connection,
                        job["job_id"],
                        reason,
                    )

                    failed += 1
                    processed += 1

                    consecutive_empty += 1

                    print(
                        "EMPTY DETAILS"
                    )

            if (
                consecutive_empty
                >= STOP_AFTER_CONSECUTIVE_EMPTY
            ):

                print()
                print(
                    "Stopping: "
                    f"{consecutive_empty} consecutive "
                    "empty/failed detail requests."
                )

                stopped_early = True
                break

            if index < len(queue):
                wait_random()

        print_summary(
            connection=connection,
            processed=processed,
            fetched=fetched,
            failed=failed,
            skipped_by_current_rules=(
                skipped_by_current_rules
            ),
            stopped_early=stopped_early,
        )

    finally:
        connection.close()

    print()
    print("RUN COMPLETE")

    return {
        "processed": processed,
        "fetched": fetched,
        "failed": failed,
        "skipped_by_current_rules": skipped_by_current_rules,
        "stopped_early": stopped_early,
    }


if __name__ == "__main__":
    main()
