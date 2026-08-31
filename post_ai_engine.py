from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from rule_engine import (
    classify_post_ai,
    load_post_rules,
)
from jobsimplesearch.config import get_settings
from jobsimplesearch.db import connect as shared_connect

PROJECT_DIR = Path(__file__).resolve().parent
SETTINGS = get_settings()
DATABASE_PATH = SETTINGS.database_path
TIMEZONE = SETTINGS.timezone

POST_AI_VERSION = "post_ai_rules_v2"

TARGET_AI_FAMILIES = {
    "data_science",
    "machine_learning",
    "ai_engineering",
    "computer_vision",
    "deep_learning",
}


def now_local() -> str:
    return datetime.now(TIMEZONE).isoformat()


def connect_database() -> sqlite3.Connection:
    return shared_connect(DATABASE_PATH)


def parse_json_list(value) -> list:
    if value is None:
        return []

    if isinstance(value, list):
        return value

    try:
        parsed = json.loads(str(value))
    except (json.JSONDecodeError, TypeError):
        return []

    return parsed if isinstance(parsed, list) else []


def canonical_language(value: str) -> str:
    text = str(value or "").strip().casefold()

    aliases = {
        "english": "english",
        "engels": "english",

        "dutch": "dutch",
        "nederlands": "dutch",
        "nederlandse": "dutch",
        "netherlands": "dutch",

        "german": "german",
        "deutsch": "german",
        "duits": "german",
        "duitse": "german",
    }

    return aliases.get(text, text)


def normalize_languages(values: list) -> list[str]:
    result = []

    for item in values:
        language = canonical_language(item)

        if language and language not in result:
            result.append(language)

    return result


def derive_ai_data_hybrid(role_families: list[str]) -> bool:
    families = {
        str(item or "").strip().casefold()
        for item in role_families
        if str(item or "").strip()
    }

    return (
        "data_engineering" in families
        and bool(
            families
            & TARGET_AI_FAMILIES
        )
    )


def load_job_for_post_ai(
    connection: sqlite3.Connection,
    job_id: str,
) -> dict | None:

    row = connection.execute(
        """
        SELECT
            j.job_id,
            j.title,
            j.company,
            j.human_decision,
            j.ai_status,

            f.role_family,
            f.role_families_json,
            f.ai_data_engineering_hybrid,

            f.seniority,

            f.minimum_years_experience,
            f.experience_requirement_text,
            f.preferred_years_experience,

            f.degree_requirement,
            f.degree_required,
            f.minimum_degree_level,
            f.phd_required,
            f.phd_preferred,

            f.student_status_required,

            f.languages_required_json,
            f.languages_preferred_json,
            f.languages_bonus_json,

            f.technical_skills_required_json,
            f.technical_skills_preferred_json,

            f.remote_mode,
            f.management_responsibility,
            f.security_clearance_required,
            f.extraction_confidence,

            m.technical_match,
            m.experience_match,
            m.education_match

        FROM jobs j

        JOIN job_ai_facts f
            ON f.job_id = j.job_id

        LEFT JOIN job_fit_assessments m
            ON m.job_id = j.job_id

        WHERE
            j.job_id = ?
            AND j.ai_status = 'EXTRACTED'
        """,
        (job_id,),
    ).fetchone()

    if row is None:
        return None

    result = dict(row)

    role_families = parse_json_list(
        result.pop(
            "role_families_json",
            None,
        )
    )

    if not role_families and result.get("role_family"):
        role_families = [
            result["role_family"]
        ]

    result["role_families"] = [
        str(item).strip().casefold()
        for item in role_families
        if str(item).strip()
    ]

    result["ai_data_engineering_hybrid"] = (
        derive_ai_data_hybrid(
            result["role_families"]
        )
    )

    result["languages_required"] = normalize_languages(
        parse_json_list(
            result.pop(
                "languages_required_json",
                None,
            )
        )
    )

    result["languages_preferred"] = normalize_languages(
        parse_json_list(
            result.pop(
                "languages_preferred_json",
                None,
            )
        )
    )

    result["languages_bonus"] = normalize_languages(
        parse_json_list(
            result.pop(
                "languages_bonus_json",
                None,
            )
        )
    )

    # Deterministic language-policy facts derived from the model's
    # requirement-level lists. These are intentionally computed in Python,
    # not guessed by the model.
    result["other_required_language_present"] = any(
        language not in {
            "english",
            "dutch",
            "german",
        }
        for language in result[
            "languages_required"
        ]
    )

    result["non_english_preferred_language_present"] = any(
        language != "english"
        for language in result[
            "languages_preferred"
        ]
    )

    result["non_english_bonus_language_present"] = any(
        language != "english"
        for language in result[
            "languages_bonus"
        ]
    )

    result["technical_skills_required"] = parse_json_list(
        result.pop(
            "technical_skills_required_json",
            None,
        )
    )

    result["technical_skills_preferred"] = parse_json_list(
        result.pop(
            "technical_skills_preferred_json",
            None,
        )
    )

    return result


def evaluate_and_persist_post_ai(
    connection: sqlite3.Connection,
    job_id: str,
    rules: list[dict] | None = None,
):
    job = load_job_for_post_ai(
        connection,
        job_id,
    )

    if job is None:
        raise ValueError(
            f"Job {job_id} is not AI-extracted or has no job_ai_facts row."
        )

    if rules is None:
        rules = load_post_rules(
            connection
        )

    result = classify_post_ai(
        job,
        rules,
    )

    connection.execute(
        """
        UPDATE jobs
        SET
            post_ai_status = ?,
            post_ai_reason = ?,
            post_ai_review_category = ?,
            post_ai_rule_id = ?,
            post_ai_rule_key = ?,
            post_ai_version = ?,
            post_ai_evaluated_at = ?
        WHERE job_id = ?
        """,
        (
            result.post_ai_status,
            result.reason,
            result.review_category,
            result.matched_rule_id,
            result.matched_rule_key,
            POST_AI_VERSION,
            now_local(),
            job_id,
        ),
    )

    return result


def run_all_extracted_jobs(
    connection: sqlite3.Connection | None = None,
) -> dict:

    owns_connection = (
        connection is None
    )

    if connection is None:
        connection = connect_database()

    try:
        rules = load_post_rules(
            connection
        )

        job_ids = [
            row["job_id"]
            for row in connection.execute(
                """
                SELECT j.job_id
                FROM jobs j
                JOIN job_ai_facts f
                    ON f.job_id = j.job_id
                WHERE
                    j.ai_status='EXTRACTED'
                    AND f.schema_version='job_facts_v2'
                ORDER BY j.ai_extracted_at, j.job_id
                """
            ).fetchall()
        ]

        counts = {
            "processed": 0,
            "SHORTLIST": 0,
            "REVIEW": 0,
            "REJECT": 0,
        }

        connection.execute(
            "BEGIN IMMEDIATE"
        )

        for job_id in job_ids:
            result = evaluate_and_persist_post_ai(
                connection,
                job_id,
                rules=rules,
            )

            counts["processed"] += 1
            counts[result.post_ai_status] += 1

        connection.commit()

        return counts

    except Exception:
        connection.rollback()
        raise

    finally:
        if owns_connection:
            connection.close()
