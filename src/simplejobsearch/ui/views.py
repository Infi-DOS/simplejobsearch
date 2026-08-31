from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from fastapi import Request
from nicegui import run, ui

from metadata_gate import run_all_fetched_jobs
from post_ai_engine import run_all_extracted_jobs
from simplejobsearch.config import get_settings
from simplejobsearch.db import apply_migrations, connect as shared_connect, database
from simplejobsearch.pipeline.orchestrator import (
    PendingReviewError,
    continue_after_review,
)
from simplejobsearch.workflow import bootstrap_latest_batch, mark_review_state, resolve_batch


# =============================================================================
# CONFIG
# =============================================================================

PROJECT_DIR = Path(__file__).resolve().parent
SETTINGS = get_settings()
DATABASE_PATH = SETTINGS.database_path

TIMEZONE = SETTINGS.timezone

SCOPE_OPTIONS = [
    "Latest run",
    "All pending",
]

PREFETCH_SCOPE_OPTIONS = [
    "Latest run",
    "All pre-fetch",
]

PREFETCH_VIEW_LABELS = {
    "AUTO_REJECTED": "Automatic rejections",
    "HUMAN_REVIEWED": "Human-reviewed",
    "ALL": "All results",
}

PREFETCH_DETAILS_STATUSES = {
    "",
    "NOT_FETCHED",
    "FAILED_OR_EMPTY",
}

BATCH_SIZE_OPTIONS = [
    25,
    50,
    100,
]

# Pinned workflow columns are useful on a desktop, but the selection column,
# status, and title together are wider than a typical phone viewport. AG Grid
# would therefore leave no usable space for the horizontally scrollable area.
# Keep the desktop layout and unpin normal columns only on narrow clients.
MOBILE_GRID_EVENT_HANDLERS = {
    ":onGridReady": (
        "params => { if (window.matchMedia('(max-width: 700px)').matches) "
        "params.api.applyColumnState({defaultState: {pinned: null}}); }"
    ),
    ":onGridSizeChanged": (
        "params => { if ((params.clientWidth || window.innerWidth) <= 700) "
        "params.api.applyColumnState({defaultState: {pinned: null}}); }"
    ),
}

CATEGORY_ACTION_OPTIONS = [
    "KEEP",
    "REVIEW",
    "EXCLUDE",
]

YES_NO_OPTIONS = [
    "YES",
    "NO",
]

PRE_RULE_FIELD_OPTIONS = [
    "company",
    "title",
    "location",
    "is_remote",
    "date_posted",
    "site",
]

METADATA_RULE_FIELD_OPTIONS = [
    "job_type",
    "job_level",
    "job_function",
    "company_industry",
    "title",
    "company",
    "location",
    "is_remote",
]

METADATA_RULE_ACTION_OPTIONS = [
    "PASS",
    "REJECT",
]

POST_AI_FIELD_OPTIONS = [
    "role_family",
    "role_families",
    "ai_data_engineering_hybrid",

    "seniority",

    "minimum_years_experience",
    "preferred_years_experience",

    "degree_required",
    "minimum_degree_level",
    "phd_required",
    "phd_preferred",

    "student_status_required",

    "languages_required",
    "languages_preferred",
    "languages_bonus",

    "other_required_language_present",
    "non_english_preferred_language_present",
    "non_english_bonus_language_present",

    "technical_skills_required",
    "technical_skills_preferred",

    "remote_mode",
    "management_responsibility",
    "security_clearance_required",

    "extraction_confidence",

    "technical_match",
    "experience_match",
    "education_match",
]

RULE_OPERATOR_OPTIONS = [
    "equals",
    "not_equals",
    "contains",
    "not_contains",
    "regex",
    "in",
    "not_in",
    "gt",
    "gte",
    "lt",
    "lte",
    "is_true",
    "is_false",
]

PRE_RULE_ACTION_OPTIONS = [
    "AUTO_KEEP",
    "REVIEW",
    "AUTO_EXCLUDE",
]

POST_AI_ACTION_OPTIONS = [
    "SHORTLIST",
    "REVIEW",
    "REJECT",
]


# =============================================================================
# STATE
#
# Local single-user MVP. We can move this into per-client storage later if the
# app ever needs multiple simultaneous users.
# =============================================================================

scope = "Latest run"
batch_size = 25
batch_index = 0

grid = None
status_label = None
batch_label = None
pending_jobs_label = None
pending_groups_label = None
visible_groups_label = None
continue_pipeline_button = None
pipeline_status_label = None

# Fetched Jobs UI references
fetched_grid = None
fetched_count_label = None
fetched_metadata_pass_label = None
fetched_metadata_reject_label = None
fetched_ai_pending_label = None

# Post-AI Extraction UI references
ai_extracted_grid = None
ai_extracted_count_label = None
ai_shortlist_count_label = None
ai_review_count_label = None
ai_reject_count_label = None
ai_post_pending_count_label = None

# Rules / Settings UI references
company_grid = None
category_grid = None
advanced_rules_grid = None

company_count_label = None
enabled_company_count_label = None
category_count_label = None
advanced_count_label = None

new_company_input = None
category_selector = None
category_action_selector = None
category_enabled_switch = None


# =============================================================================
# DATABASE
# =============================================================================

def now_local() -> str:
    return datetime.now(TIMEZONE).isoformat()


def connect_database() -> sqlite3.Connection:
    return shared_connect(DATABASE_PATH)


def verify_database() -> None:
    required = {
        "jobs",
        "search_runs",
        "search_hits",
        "review_events",
        "review_categories",
        "pipeline_rules",
    }

    connection = connect_database()

    try:
        rows = connection.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
            """
        ).fetchall()

        existing = {
            row["name"]
            for row in rows
        }

        missing = required - existing

        if missing:
            raise RuntimeError(
                "Database is missing required tables: "
                + ", ".join(sorted(missing))
            )

        job_columns = {
            row["name"]
            for row in connection.execute(
                "PRAGMA table_info(jobs)"
            ).fetchall()
        }

        required_metadata_columns = {
            "metadata_gate_status",
            "metadata_gate_reason",
            "metadata_gate_rule_id",
            "metadata_gate_rule_key",
            "metadata_gate_evaluated_at",
            "metadata_gate_version",
        }

        missing_metadata = (
            required_metadata_columns
            - job_columns
        )

        if missing_metadata:
            raise RuntimeError(
                "Metadata-gate database fields are missing: "
                + ", ".join(sorted(missing_metadata))
                + "\nRun setup_metadata_gate.py first."
            )

    finally:
        connection.close()


def latest_run_id(
    connection: sqlite3.Connection,
) -> str | None:

    row = connection.execute(
        """
        SELECT run_id
        FROM search_runs
        WHERE status IN ('SUCCESS', 'PARTIAL')
        ORDER BY started_at DESC
        LIMIT 1
        """
    ).fetchone()

    return row["run_id"] if row else None



# =============================================================================
# RULE ENGINE DATABASE HELPERS
# =============================================================================

def load_review_categories() -> list[dict]:
    connection = connect_database()

    try:
        rows = connection.execute(
            """
            SELECT
                category_key,
                display_name,
                default_action,
                enabled,
                sort_order,
                created_at,
                updated_at

            FROM review_categories

            ORDER BY
                sort_order,
                category_key
            """
        ).fetchall()

        return [
            {
                "category_key": row["category_key"],
                "display_name": row["display_name"],
                "default_action": row["default_action"],
                "enabled": "YES" if int(row["enabled"]) else "NO",
                "sort_order": row["sort_order"],
                "updated_at": row["updated_at"],
            }
            for row in rows
        ]

    finally:
        connection.close()


def load_pipeline_rules(
    stage: str,
) -> list[dict]:

    connection = connect_database()

    try:
        rows = connection.execute(
            """
            SELECT
                rule_id,
                rule_key,
                rule_name,
                stage,
                field_name,
                operator,
                match_value,
                action,
                review_category,
                priority,
                enabled,
                editable,
                source,
                notes,
                legacy_filter_rule_id,
                updated_at

            FROM pipeline_rules

            WHERE stage = ?

            ORDER BY
                priority,
                rule_id
            """,
            (
                stage,
            ),
        ).fetchall()

        return [
            {
                "rule_id": row["rule_id"],
                "rule_key": row["rule_key"],
                "rule_name": row["rule_name"],
                "field_name": row["field_name"],
                "operator": row["operator"],
                "match_value": row["match_value"] or "",
                "action": row["action"],
                "review_category": row["review_category"] or "",
                "priority": row["priority"],
                "enabled": "YES" if int(row["enabled"]) else "NO",
                "source": row["source"] or "",
                "notes": row["notes"] or "",
                "legacy_filter_rule_id": row["legacy_filter_rule_id"],
                "updated_at": row["updated_at"] or "",
            }
            for row in rows
        ]

    finally:
        connection.close()


def slugify(value: str) -> str:
    value = str(value or "").strip().casefold()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = value.strip("_")
    return value or "rule"


def add_or_update_category(
    category_key: str,
    display_name: str,
    default_action: str,
    enabled: bool,
    sort_order: int,
) -> None:

    category_key = slugify(
        category_key
    )

    display_name = (
        str(
            display_name
            or ""
        ).strip()
        or category_key.replace(
            "_",
            " ",
        ).title()
    )

    default_action = str(
        default_action
        or "REVIEW"
    ).strip().upper()

    if default_action not in {
        "KEEP",
        "REVIEW",
        "EXCLUDE",
    }:
        raise ValueError(
            f"Invalid category action: {default_action}"
        )

    timestamp = now_local()

    connection = connect_database()

    try:
        connection.execute(
            """
            INSERT INTO review_categories (
                category_key,
                display_name,
                default_action,
                enabled,
                sort_order,
                created_at,
                updated_at
            )

            VALUES (?, ?, ?, ?, ?, ?, ?)

            ON CONFLICT(category_key)
            DO UPDATE SET
                display_name = excluded.display_name,
                default_action = excluded.default_action,
                enabled = excluded.enabled,
                sort_order = excluded.sort_order,
                updated_at = excluded.updated_at
            """,
            (
                category_key,
                display_name,
                default_action,
                1 if enabled else 0,
                int(sort_order),
                timestamp,
                timestamp,
            ),
        )

        connection.commit()

    finally:
        connection.close()


def set_categories_enabled(
    category_keys: list[str],
    enabled: bool,
) -> int:

    if not category_keys:
        return 0

    connection = connect_database()

    try:
        connection.execute(
            "BEGIN IMMEDIATE"
        )

        updated = 0

        for category_key in (
            category_keys
        ):
            cursor = connection.execute(
                """
                UPDATE review_categories

                SET
                    enabled = ?,
                    updated_at = ?

                WHERE category_key = ?
                """,
                (
                    1 if enabled else 0,
                    now_local(),
                    category_key,
                ),
            )

            updated += cursor.rowcount

        connection.commit()

        return updated

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


def make_unique_rule_key(
    connection: sqlite3.Connection,
    stage: str,
    rule_name: str,
) -> str:

    base = (
        f"{stage.casefold()}_"
        f"{slugify(rule_name)}"
    )

    key = base
    suffix = 2

    while connection.execute(
        """
        SELECT 1
        FROM pipeline_rules
        WHERE rule_key = ?
        """,
        (
            key,
        ),
    ).fetchone():
        key = f"{base}_{suffix}"
        suffix += 1

    return key


def add_pipeline_rule(
    *,
    stage: str,
    rule_name: str,
    field_name: str,
    operator: str,
    match_value: str | None,
    action: str,
    review_category: str | None,
    priority: int,
    enabled: bool,
    notes: str | None = None,
) -> str:

    stage = str(
        stage
    ).strip().upper()

    if stage not in {
        "PRE_DESCRIPTION",
        "METADATA_GATE",
        "POST_AI",
    }:
        raise ValueError(
            f"Invalid stage: {stage}"
        )

    rule_name = str(
        rule_name
        or ""
    ).strip()

    if not rule_name:
        raise ValueError(
            "Rule name cannot be empty."
        )

    field_name = str(
        field_name
        or ""
    ).strip()

    if not field_name:
        raise ValueError(
            "Field cannot be empty."
        )

    operator = str(
        operator
        or ""
    ).strip()

    if operator not in RULE_OPERATOR_OPTIONS:
        raise ValueError(
            f"Invalid operator: {operator}"
        )

    action = str(
        action
        or ""
    ).strip().upper()

    if stage == "PRE_DESCRIPTION":
        valid_actions = set(
            PRE_RULE_ACTION_OPTIONS
        )
    elif stage == "METADATA_GATE":
        valid_actions = set(
            METADATA_RULE_ACTION_OPTIONS
        )
    else:
        valid_actions = set(
            POST_AI_ACTION_OPTIONS
        )

    if action not in valid_actions:
        raise ValueError(
            f"Invalid action for {stage}: {action}"
        )

    review_category = (
        str(
            review_category
            or ""
        ).strip()
        or None
    )

    if action == "REVIEW" and not review_category:
        raise ValueError(
            "REVIEW rules must specify a review category."
        )

    if operator in {
        "is_true",
        "is_false",
    }:
        match_value = None

    timestamp = now_local()

    connection = connect_database()

    try:
        rule_key = make_unique_rule_key(
            connection,
            stage,
            rule_name,
        )

        connection.execute(
            """
            INSERT INTO pipeline_rules (
                rule_key,
                rule_name,
                stage,
                field_name,
                operator,
                match_value,
                action,
                review_category,
                priority,
                enabled,
                editable,
                source,
                notes,
                legacy_filter_rule_id,
                created_at,
                updated_at
            )

            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                rule_key,
                rule_name,
                stage,
                field_name,
                operator,
                match_value,
                action,
                review_category,
                int(priority),
                1 if enabled else 0,
                1,
                "user",
                notes,
                None,
                timestamp,
                timestamp,
            ),
        )

        connection.commit()

        return rule_key

    finally:
        connection.close()


def set_pipeline_rules_enabled(
    rule_ids: list[int],
    enabled: bool,
) -> int:

    if not rule_ids:
        return 0

    connection = connect_database()

    try:
        connection.execute(
            "BEGIN IMMEDIATE"
        )

        updated = 0

        for rule_id in rule_ids:
            cursor = connection.execute(
                """
                UPDATE pipeline_rules

                SET
                    enabled = ?,
                    updated_at = ?

                WHERE rule_id = ?
                """,
                (
                    1 if enabled else 0,
                    now_local(),
                    int(rule_id),
                ),
            )

            updated += cursor.rowcount

        connection.commit()

        return updated

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


# =============================================================================
# GROUPING
# =============================================================================

def normalize_company(value: str) -> str:
    value = str(value or "").strip().casefold()
    value = re.sub(r"[^\w\s]", "", value)
    value = re.sub(r"\s+", " ", value)
    return value


def normalize_title(value: str) -> str:
    value = str(value or "").strip().casefold()
    value = re.sub(r"[^\w\s]", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def make_group_key(
    company: str,
    title: str,
) -> str:

    payload = (
        normalize_company(company)
        + "|"
        + normalize_title(title)
    )

    return hashlib.sha1(
        payload.encode("utf-8")
    ).hexdigest()[:16]


def compact_locations(
    values: list[str],
    limit: int = 4,
) -> str:

    locations = sorted(
        {
            str(value).strip()
            for value in values
            if str(value or "").strip()
        }
    )

    if not locations:
        return ""

    shown = locations[:limit]
    remaining = len(locations) - len(shown)

    text = ", ".join(shown)

    if remaining > 0:
        text += f" +{remaining} more"

    return text


def load_pending_groups(
    requested_scope: str,
    requested_batch_size: int,
    requested_batch_index: int,
) -> dict:

    connection = connect_database()

    try:
        run_id = (
            latest_run_id(connection)
            if requested_scope == "Latest run"
            else None
        )

        params: list[str] = []
        run_filter = ""

        if run_id:
            run_filter = """
                AND EXISTS (
                    SELECT 1
                    FROM search_hits sh
                    WHERE sh.job_id = j.job_id
                      AND sh.run_id = ?
                )
            """
            params.append(run_id)

        query = f"""
            SELECT
                j.job_id,
                j.title,
                j.company,
                j.location,
                j.date_posted,
                j.job_url,

                j.review_category,
                j.review_reason,
                j.suggested_action,

                j.title_has_phd

            FROM jobs j

            WHERE
                j.classifier_status = 'REVIEW'

                AND (
                    j.human_decision IS NULL
                    OR TRIM(j.human_decision) = ''
                )

                {run_filter}

            ORDER BY
                j.company,
                j.title,
                j.location,
                j.job_id
        """

        raw = pd.read_sql_query(
            query,
            connection,
            params=params,
        )

    finally:
        connection.close()

    total_pending_jobs = len(raw)

    if raw.empty:
        return {
            "run_id": run_id,
            "total_pending_jobs": 0,
            "total_pending_groups": 0,
            "batch_count": 1,
            "batch_index": 0,
            "rows": [],
        }

    raw["norm_company"] = (
        raw["company"]
        .fillna("")
        .map(normalize_company)
    )

    raw["norm_title"] = (
        raw["title"]
        .fillna("")
        .map(normalize_title)
    )

    grouped_rows: list[dict] = []

    for (_, _), frame in raw.groupby(
        [
            "norm_company",
            "norm_title",
        ],
        dropna=False,
        sort=False,
    ):
        first = frame.iloc[0]

        title = str(
            first.get("title")
            or ""
        ).strip()

        company = str(
            first.get("company")
            or ""
        ).strip()

        suggestions = {
            str(value).strip().upper()
            for value
            in frame[
                "suggested_action"
            ].dropna().tolist()
            if str(value).strip()
        }

        categories = {
            str(value).strip()
            for value
            in frame[
                "review_category"
            ].dropna().tolist()
            if str(value).strip()
        }

        suggested = (
            next(iter(suggestions))
            if len(suggestions) == 1
            else "REVIEW"
        )

        category = (
            next(iter(categories))
            if len(categories) == 1
            else "mixed"
        )

        dates = sorted(
            {
                str(value).strip()
                for value
                in frame[
                    "date_posted"
                ].dropna().tolist()
                if str(value).strip()
            },
            reverse=True,
        )

        job_ids = [
            str(value)
            for value
            in frame["job_id"].tolist()
        ]

        urls = [
            str(value)
            for value
            in frame["job_url"].dropna().tolist()
            if str(value).strip()
        ]

        grouped_rows.append(
            {
                "group_key":
                    make_group_key(
                        company,
                        title,
                    ),

                "suggested":
                    suggested,

                "title":
                    title,

                "company":
                    company,

                "listings":
                    len(job_ids),

                "locations":
                    compact_locations(
                        frame[
                            "location"
                        ].fillna("").tolist()
                    ),

                "posted":
                    dates[0]
                    if dates
                    else "",

                "category":
                    category,

                "phd":
                    "Yes"
                    if int(
                        frame[
                            "title_has_phd"
                        ]
                        .fillna(0)
                        .max()
                    ) == 1
                    else "",

                "job_url":
                    urls[0]
                    if urls
                    else "",

                # Hidden from grid but returned by get_selected_rows().
                "job_ids_json":
                    json.dumps(
                        job_ids
                    ),
            }
        )

    priority = {
        "KEEP": 1,
        "REVIEW": 2,
        "EXCLUDE": 3,
    }

    grouped = pd.DataFrame(
        grouped_rows
    )

    grouped["_suggested_order"] = (
        grouped[
            "suggested"
        ]
        .map(priority)
        .fillna(9)
    )

    grouped = (
        grouped
        .sort_values(
            [
                "category",
                "_suggested_order",
                "company",
                "title",
            ],
            kind="stable",
        )
        .drop(
            columns=[
                "_suggested_order"
            ]
        )
        .reset_index(
            drop=True
        )
    )

    total_pending_groups = len(
        grouped
    )

    batch_count = max(
        (
            total_pending_groups
            - 1
        )
        // int(
            requested_batch_size
        )
        + 1,
        1,
    )

    requested_batch_index = min(
        max(
            int(
                requested_batch_index
            ),
            0,
        ),
        batch_count - 1,
    )

    start = (
        requested_batch_index
        * int(
            requested_batch_size
        )
    )

    stop = (
        start
        + int(
            requested_batch_size
        )
    )

    visible = (
        grouped
        .iloc[
            start:stop
        ]
        .copy()
    )

    return {
        "run_id":
            run_id,

        "total_pending_jobs":
            total_pending_jobs,

        "total_pending_groups":
            total_pending_groups,

        "batch_count":
            batch_count,

        "batch_index":
            requested_batch_index,

        "rows":
            visible.to_dict(
                orient="records"
            ),
    }


# =============================================================================
# REVIEW DECISION
# =============================================================================

def save_review_decisions(
    selected_rows: list[dict],
    decision: str,
) -> dict:

    selected_job_ids: list[str] = []

    for row in selected_rows:
        selected_job_ids.extend(
            json.loads(
                row[
                    "job_ids_json"
                ]
            )
        )

    selected_job_ids = list(
        dict.fromkeys(
            selected_job_ids
        )
    )

    if not selected_job_ids:
        return {
            "saved_jobs": 0,
            "groups": 0,
            "already_reviewed": 0,
        }

    connection = connect_database()

    reviewed_at = now_local()

    saved_jobs = 0
    already_reviewed = 0

    try:
        connection.execute(
            "BEGIN IMMEDIATE"
        )

        for job_id in (
            selected_job_ids
        ):

            existing = (
                connection.execute(
                    """
                    SELECT
                        human_decision,
                        review_category

                    FROM jobs

                    WHERE job_id = ?
                    """,
                    (
                        job_id,
                    ),
                )
                .fetchone()
            )

            if existing is None:
                raise RuntimeError(
                    "Job disappeared "
                    f"from database: "
                    f"{job_id}"
                )

            previous = (
                existing[
                    "human_decision"
                ]
            )

            if previous not in (
                None,
                "",
            ):
                already_reviewed += 1
                continue

            connection.execute(
                """
                UPDATE jobs

                SET
                    human_decision = ?,
                    human_reviewed_at = ?

                WHERE job_id = ?
                """,
                (
                    decision,
                    reviewed_at,
                    job_id,
                ),
            )

            connection.execute(
                """
                INSERT INTO review_events (
                    job_id,
                    previous_decision,
                    decision,
                    review_category,
                    source,
                    reviewer,
                    reviewed_at,
                    note
                )

                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    previous,
                    decision,
                    existing[
                        "review_category"
                    ],
                    "nicegui_app",
                    "local_user",
                    reviewed_at,
                    (
                        f"{decision} "
                        "applied from "
                        "NiceGUI grouped "
                        "Review Inbox"
                    ),
                ),
            )

            saved_jobs += 1

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()

    return {
        "saved_jobs":
            saved_jobs,

        "groups":
            len(
                selected_rows
            ),

        "already_reviewed":
            already_reviewed,
    }


# =============================================================================
# GRID
# =============================================================================

def make_grid_options(
    rows: list[dict],
) -> dict:

    return {
        **MOBILE_GRID_EVENT_HANDLERS,
        "defaultColDef": {
            "sortable": True,
            "filter": True,
            "resizable": True,
        },

        "columnDefs": [
            {
                "headerName":
                    "Suggested",
                "field":
                    "suggested",
                "width":
                    125,
                "pinned":
                    "left",

                "cellClassRules": {
                    "suggest-keep":
                        "value === 'KEEP'",

                    "suggest-exclude":
                        "value === 'EXCLUDE'",

                    "suggest-review":
                        "value === 'REVIEW'",
                },
            },
            {
                "headerName":
                    "Job title",
                "field":
                    "title",
                "minWidth":
                    250,
                "flex":
                    2,
                "pinned":
                    "left",
            },
            {
                "headerName":
                    "Company",
                "field":
                    "company",
                "minWidth":
                    180,
                "flex":
                    1,
            },
            {
                "headerName":
                    "Category",
                "field":
                    "category",
                "minWidth":
                    170,
            },
            {
                "headerName":
                    "Locations",
                "field":
                    "locations",
                "minWidth":
                    260,
                "flex":
                    2,
            },
            {
                "headerName":
                    "Posted",
                "field":
                    "posted",
                "width":
                    130,
            },
            {
                "headerName":
                    "Listings",
                "field":
                    "listings",
                "width":
                    100,
            },
            {
                "headerName":
                    "PhD",
                "field":
                    "phd",
                "width":
                    80,
            },

            # Hidden state needed when applying a decision.
            {
                "field":
                    "group_key",
                "hide":
                    True,
            },
            {
                "field":
                    "job_ids_json",
                "hide":
                    True,
            },
            {
                "field":
                    "job_url",
                "hide":
                    True,
            },
        ],

        "rowData":
            rows,

        "rowSelection": {
            "mode":
                "multiRow",

            "checkboxes":
                True,

            "headerCheckbox":
                True,

            # Header Select All respects active grid filters.
            "selectAll":
                "filtered",

            # Clicking a row also selects/deselects it.
            "enableClickSelection":
                True,

            "enableSelectionWithoutKeys":
                True,
        },

        "selectionColumnDef": {
            "width":
                54,

            "pinned":
                "left",
        },

        "rowClassRules": {
            "row-keep":
                "data.suggested === 'KEEP'",

            "row-exclude":
                "data.suggested === 'EXCLUDE'",

            "row-review":
                "data.suggested === 'REVIEW'",
        },

        "animateRows":
            False,

        "suppressCellFocus":
            True,
    }


# =============================================================================
# PRE-FETCH RESULTS AUDIT
# =============================================================================

def _one_group_value(
    values,
    *,
    default: str = "",
    mixed: str = "MIXED",
    include_empty: bool = False,
) -> str:
    cleaned = {
        str(value or "").strip()
        for value in values
        if include_empty or str(value or "").strip()
    }
    if not cleaned:
        return default
    if len(cleaned) == 1:
        return next(iter(cleaned))
    return mixed


def _group_prefetch_rows(
    raw: pd.DataFrame,
) -> list[dict]:
    if raw.empty:
        return []

    raw = raw.copy()
    raw["norm_company"] = raw["company"].fillna("").map(normalize_company)
    raw["norm_title"] = raw["title"].fillna("").map(normalize_title)
    grouped_rows: list[dict] = []

    for (_, _), frame in raw.groupby(
        ["norm_company", "norm_title"],
        dropna=False,
        sort=False,
    ):
        first = frame.iloc[0]
        title = str(first.get("title") or "").strip()
        company = str(first.get("company") or "").strip()
        dates = sorted(
            {
                str(value).strip()
                for value in frame["date_posted"].dropna().tolist()
                if str(value).strip()
            },
            reverse=True,
        )
        job_ids = [str(value) for value in frame["job_id"].tolist()]
        urls = [
            str(value)
            for value in frame["job_url"].dropna().tolist()
            if str(value).strip()
        ]
        grouped_rows.append(
            {
                "group_key": make_group_key(company, title),
                "decision": _one_group_value(frame["effective_decision"]),
                "classifier_status": _one_group_value(frame["classifier_status"]),
                "human_decision": _one_group_value(
                    frame["human_decision"],
                    default="—",
                    include_empty=True,
                ),
                "details_status": _one_group_value(
                    frame["details_status"],
                    default="NOT_FETCHED",
                ),
                "title": title,
                "company": company,
                "listings": len(job_ids),
                "locations": compact_locations(frame["location"].fillna("").tolist()),
                "posted": dates[0] if dates else "",
                "category": _one_group_value(
                    frame["review_category"],
                    default="—",
                    mixed="mixed",
                ),
                "phd": (
                    "Yes"
                    if int(frame["title_has_phd"].fillna(0).max()) == 1
                    else ""
                ),
                "job_url": urls[0] if urls else "",
                "job_ids_json": json.dumps(job_ids),
            }
        )

    priority = {"KEEP": 1, "REVIEW": 2, "EXCLUDE": 3, "MIXED": 4}
    grouped = pd.DataFrame(grouped_rows)
    grouped["_decision_order"] = grouped["decision"].map(priority).fillna(9)
    grouped = grouped.sort_values(
        ["_decision_order", "category", "company", "title"],
        kind="stable",
    ).drop(columns=["_decision_order"])
    return grouped.to_dict(orient="records")


def load_prefetch_groups(
    view_key: str,
    requested_scope: str,
    requested_batch_size: int,
    requested_batch_index: int,
) -> dict:
    if view_key not in PREFETCH_VIEW_LABELS:
        raise ValueError(f"Unknown pre-fetch view: {view_key}")

    connection = connect_database()
    try:
        run_id = latest_run_id(connection) if requested_scope == "Latest run" else None
        params: list[str] = []
        filters = [
            "(j.details_status IS NULL OR TRIM(j.details_status) = '' "
            "OR j.details_status IN ('NOT_FETCHED', 'FAILED_OR_EMPTY'))"
        ]
        if view_key == "AUTO_REJECTED":
            filters.append("j.classifier_status = 'AUTO_EXCLUDE'")
        elif view_key == "HUMAN_REVIEWED":
            filters.append(
                "j.human_decision IS NOT NULL AND TRIM(j.human_decision) <> ''"
            )
        if run_id:
            filters.append(
                "EXISTS (SELECT 1 FROM search_hits sh "
                "WHERE sh.job_id = j.job_id AND sh.run_id = ?)"
            )
            params.append(run_id)

        where_sql = " AND ".join(filters)
        raw = pd.read_sql_query(
            f"""
            SELECT
                j.job_id,
                j.title,
                j.company,
                j.location,
                j.date_posted,
                j.job_url,
                j.review_category,
                j.classifier_status,
                j.suggested_action,
                j.human_decision,
                j.details_status,
                j.title_has_phd,
                CASE
                    WHEN j.human_decision IN ('KEEP', 'EXCLUDE')
                        THEN j.human_decision
                    WHEN j.classifier_status = 'AUTO_KEEP' THEN 'KEEP'
                    WHEN j.classifier_status = 'AUTO_EXCLUDE' THEN 'EXCLUDE'
                    ELSE 'REVIEW'
                END AS effective_decision
            FROM jobs j
            WHERE {where_sql}
            ORDER BY j.company, j.title, j.location, j.job_id
            """,
            connection,
            params=params,
        )
    finally:
        connection.close()

    grouped_rows = _group_prefetch_rows(raw)
    total_jobs = len(raw)
    total_groups = len(grouped_rows)
    batch_size_value = max(int(requested_batch_size), 1)
    batch_count = max((total_groups - 1) // batch_size_value + 1, 1)
    batch_index_value = min(max(int(requested_batch_index), 0), batch_count - 1)
    start = batch_index_value * batch_size_value
    stop = start + batch_size_value

    return {
        "run_id": run_id,
        "total_jobs": total_jobs,
        "total_groups": total_groups,
        "batch_count": batch_count,
        "batch_index": batch_index_value,
        "rows": grouped_rows[start:stop],
    }


def save_prefetch_decisions(
    selected_rows: list[dict],
    decision: str,
) -> dict[str, int]:
    decision = str(decision or "").strip().upper()
    if decision not in {"KEEP", "EXCLUDE"}:
        raise ValueError("Pre-fetch decision must be KEEP or EXCLUDE")

    job_ids: list[str] = []
    for row in selected_rows:
        job_ids.extend(json.loads(row["job_ids_json"]))
    job_ids = list(dict.fromkeys(str(job_id) for job_id in job_ids))
    if not job_ids:
        return {"changed": 0, "unchanged": 0, "locked": 0, "groups": 0}

    connection = connect_database()
    changed = 0
    unchanged = 0
    locked = 0
    affected_batches: set[int] = set()
    reviewed_at = now_local()
    try:
        connection.execute("BEGIN IMMEDIATE")
        for job_id in job_ids:
            existing = connection.execute(
                """
                SELECT human_decision, review_category, details_status
                FROM jobs
                WHERE job_id = ?
                """,
                (job_id,),
            ).fetchone()
            if existing is None:
                raise RuntimeError(f"Job disappeared from database: {job_id}")

            details_status = str(existing["details_status"] or "").strip()
            if details_status not in PREFETCH_DETAILS_STATUSES:
                locked += 1
                continue
            previous = str(existing["human_decision"] or "").strip() or None
            if previous == decision:
                unchanged += 1
                continue

            connection.execute(
                """
                UPDATE jobs
                SET human_decision = ?, human_reviewed_at = ?
                WHERE job_id = ?
                """,
                (decision, reviewed_at, job_id),
            )
            connection.execute(
                """
                INSERT INTO review_events (
                    job_id, previous_decision, decision, review_category,
                    source, reviewer, reviewed_at, note
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    previous,
                    decision,
                    existing["review_category"],
                    "nicegui_prefetch_audit",
                    "local_user",
                    reviewed_at,
                    f"{decision} applied from NiceGUI Pre-Fetch Results",
                ),
            )
            changed += 1
            affected_batches.update(
                int(row[0])
                for row in connection.execute(
                    "SELECT batch_id FROM daily_batch_jobs WHERE job_id = ?",
                    (job_id,),
                )
            )
        connection.commit()

        for batch_id in affected_batches:
            mark_review_state(connection, batch_id)
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    return {
        "changed": changed,
        "unchanged": unchanged,
        "locked": locked,
        "groups": len(selected_rows),
    }


def prefetch_grid_options(
    rows: list[dict],
) -> dict:
    return {
        **MOBILE_GRID_EVENT_HANDLERS,
        "defaultColDef": {
            "sortable": True,
            "filter": True,
            "resizable": True,
        },
        "columnDefs": [
            {
                "headerName": "Decision",
                "field": "decision",
                "width": 120,
                "pinned": "left",
                "cellClassRules": {
                    "suggest-keep": "value === 'KEEP'",
                    "suggest-exclude": "value === 'EXCLUDE'",
                    "suggest-review": "value === 'REVIEW' || value === 'MIXED'",
                },
            },
            {
                "headerName": "Job title",
                "field": "title",
                "minWidth": 250,
                "flex": 2,
                "pinned": "left",
            },
            {"headerName": "Company", "field": "company", "minWidth": 180, "flex": 1},
            {"headerName": "PRE result", "field": "classifier_status", "width": 150},
            {"headerName": "Human override", "field": "human_decision", "width": 145},
            {"headerName": "Category", "field": "category", "minWidth": 170},
            {"headerName": "Locations", "field": "locations", "minWidth": 260, "flex": 1},
            {"headerName": "Posted", "field": "posted", "width": 130},
            {"headerName": "Listings", "field": "listings", "width": 100},
            {"headerName": "Fetch state", "field": "details_status", "width": 145},
            {"headerName": "PhD", "field": "phd", "width": 80},
            {"field": "group_key", "hide": True},
            {"field": "job_ids_json", "hide": True},
            {"field": "job_url", "hide": True},
        ],
        "rowData": rows,
        "rowSelection": {
            "mode": "multiRow",
            "checkboxes": True,
            "headerCheckbox": True,
            "selectAll": "filtered",
            "enableClickSelection": True,
            "enableSelectionWithoutKeys": True,
        },
        "selectionColumnDef": {"width": 54, "pinned": "left"},
        "rowClassRules": {
            "row-keep": "data.decision === 'KEEP'",
            "row-exclude": "data.decision === 'EXCLUDE'",
            "row-review": "data.decision === 'REVIEW' || data.decision === 'MIXED'",
        },
        "animateRows": False,
        "suppressCellFocus": True,
    }




# =============================================================================
# FETCHED JOBS
# =============================================================================

def clean_stored_job_type(
    value,
) -> str:
    """
    Display both old v2 serialized values such as ["JobType.FULL_TIME"]
    and the new normalized scalar values cleanly.
    """
    text = str(
        value
        or ""
    ).strip()

    if not text:
        return ""

    try:
        parsed = json.loads(
            text
        )

        if isinstance(
            parsed,
            list,
        ):
            cleaned = []

            for item in parsed:
                item = str(
                    item
                    or ""
                ).strip()

                item = re.sub(
                    r"^JobType\.",
                    "",
                    item,
                )

                if item:
                    cleaned.append(
                        item
                    )

            if len(cleaned) == 1:
                return cleaned[0]

            if cleaned:
                return ", ".join(
                    cleaned
                )

    except (
        json.JSONDecodeError,
        TypeError,
    ):
        pass

    return re.sub(
        r"^JobType\.",
        "",
        text,
    )


def load_fetched_jobs() -> list[dict]:

    connection = connect_database()

    try:
        rows = connection.execute(
            """
            SELECT
                j.job_id,
                j.title,
                j.company,
                j.location,
                j.date_posted,

                j.job_url,
                j.job_url_direct,

                j.job_type,
                j.job_level,
                j.job_function,
                j.company_industry,

                j.description,

                j.classifier_status,
                j.review_category,
                j.human_decision,

                j.details_fetched_at,
                j.details_attempt_count,

                j.metadata_gate_status,
                j.metadata_gate_reason,
                j.metadata_gate_rule_key,
                j.ai_status,
                j.post_ai_status,
                j.post_ai_reason,
                j.post_ai_review_category,
                j.post_ai_rule_key,
                j.post_ai_evaluated_at,

                f.role_family AS ai_role_family,
                f.role_families_json AS ai_role_families_json,
                f.ai_data_engineering_hybrid AS ai_data_engineering_hybrid,

                f.seniority AS ai_seniority,

                f.minimum_years_experience AS ai_minimum_years_experience,
                f.experience_requirement_text AS ai_experience_requirement_text,
                f.preferred_years_experience AS ai_preferred_years_experience,
                f.preferred_experience_text AS ai_preferred_experience_text,

                f.degree_required AS ai_degree_required,
                f.minimum_degree_level AS ai_minimum_degree_level,
                f.degree_requirement_text AS ai_degree_requirement_text,
                f.degree_fields_json AS ai_degree_fields_json,
                f.phd_required AS ai_phd_required,
                f.phd_preferred AS ai_phd_preferred,

                f.student_status_required AS ai_student_status_required,
                f.student_requirement_text AS ai_student_requirement_text,

                f.languages_required_json AS ai_languages_required_json,
                f.languages_preferred_json AS ai_languages_preferred_json,
                f.languages_bonus_json AS ai_languages_bonus_json,
                f.language_requirement_notes AS ai_language_requirement_notes,

                f.technical_skills_required_json AS ai_skills_required_json,
                f.technical_skills_preferred_json AS ai_skills_preferred_json,

                f.role_summary AS ai_role_summary,
                f.core_responsibilities_json AS ai_core_responsibilities_json,

                f.remote_mode AS ai_remote_mode,
                f.management_responsibility AS ai_management_responsibility,
                f.security_clearance_required AS ai_security_clearance_required,
                f.extraction_confidence AS ai_extraction_confidence,
                f.evidence_json AS ai_evidence_json

            FROM jobs j

            LEFT JOIN job_ai_facts f
                ON f.job_id = j.job_id

            WHERE j.details_status = 'FETCHED'

            ORDER BY
                details_fetched_at DESC,
                company,
                title
            """
        ).fetchall()

        result = []

        for row in rows:
            job_type = clean_stored_job_type(
                row["job_type"]
            )

            description = str(
                row["description"]
                or ""
            )

            preview = re.sub(
                r"\s+",
                " ",
                description,
            ).strip()

            if len(preview) > 180:
                preview = (
                    preview[:177]
                    + "..."
                )

            result.append(
                {
                    "job_id":
                        row["job_id"],

                    "title":
                        row["title"],

                    "company":
                        row["company"],

                    "location":
                        row["location"] or "",

                    "job_type":
                        job_type,

                    "job_level":
                        row["job_level"] or "",

                    "job_function":
                        row["job_function"] or "",

                    "industry":
                        row["company_industry"] or "",

                    "date_posted":
                        row["date_posted"] or "",

                    "fetched_at":
                        row["details_fetched_at"] or "",

                    "description_chars":
                        len(
                            description
                        ),

                    "description_preview":
                        preview,

                    "description":
                        description,

                    "job_url":
                        row["job_url"] or "",

                    "job_url_direct":
                        row["job_url_direct"] or "",

                    "classifier_status":
                        row["classifier_status"] or "",

                    "review_category":
                        row["review_category"] or "",

                    "human_decision":
                        row["human_decision"] or "",

                    "metadata_gate_status":
                        row["metadata_gate_status"] or "PENDING",

                    "metadata_gate_reason":
                        row["metadata_gate_reason"] or "",

                    "metadata_gate_rule_key":
                        row["metadata_gate_rule_key"] or "",

                    "ai_status":
                        row["ai_status"] or "",

                    "post_ai_status":
                        row["post_ai_status"] or "",

                    "post_ai_reason":
                        row["post_ai_reason"] or "",

                    "post_ai_review_category":
                        row["post_ai_review_category"] or "",

                    "post_ai_rule_key":
                        row["post_ai_rule_key"] or "",

                    "post_ai_evaluated_at":
                        row["post_ai_evaluated_at"] or "",

                    "ai_role_family":
                        row["ai_role_family"] or "",

                    "ai_role_families":
                        json.loads(
                            row["ai_role_families_json"]
                            or "[]"
                        ),

                    "ai_data_engineering_hybrid":
                        bool(
                            row["ai_data_engineering_hybrid"]
                        )
                        if row["ai_data_engineering_hybrid"] is not None
                        else None,

                    "ai_seniority":
                        row["ai_seniority"] or "",

                    "ai_minimum_years_experience":
                        row["ai_minimum_years_experience"],

                    "ai_experience_requirement_text":
                        row["ai_experience_requirement_text"] or "",

                    "ai_preferred_years_experience":
                        row["ai_preferred_years_experience"],

                    "ai_preferred_experience_text":
                        row["ai_preferred_experience_text"] or "",

                    "ai_degree_required":
                        bool(
                            row["ai_degree_required"]
                        )
                        if row["ai_degree_required"] is not None
                        else None,

                    "ai_minimum_degree_level":
                        row["ai_minimum_degree_level"] or "",

                    "ai_degree_requirement_text":
                        row["ai_degree_requirement_text"] or "",

                    "ai_degree_fields":
                        json.loads(
                            row["ai_degree_fields_json"]
                            or "[]"
                        ),

                    "ai_phd_required":
                        bool(
                            row["ai_phd_required"]
                        )
                        if row["ai_phd_required"] is not None
                        else None,

                    "ai_phd_preferred":
                        bool(
                            row["ai_phd_preferred"]
                        )
                        if row["ai_phd_preferred"] is not None
                        else None,

                    "ai_student_status_required":
                        bool(
                            row["ai_student_status_required"]
                        )
                        if row["ai_student_status_required"] is not None
                        else None,

                    "ai_student_requirement_text":
                        row["ai_student_requirement_text"] or "",

                    "ai_languages_required":
                        json.loads(
                            row["ai_languages_required_json"]
                            or "[]"
                        ),

                    "ai_languages_preferred":
                        json.loads(
                            row["ai_languages_preferred_json"]
                            or "[]"
                        ),

                    "ai_languages_bonus":
                        json.loads(
                            row["ai_languages_bonus_json"]
                            or "[]"
                        ),

                    "ai_language_requirement_notes":
                        row["ai_language_requirement_notes"] or "",

                    "ai_skills_required":
                        json.loads(
                            row["ai_skills_required_json"]
                            or "[]"
                        ),

                    "ai_skills_preferred":
                        json.loads(
                            row["ai_skills_preferred_json"]
                            or "[]"
                        ),

                    "ai_role_summary":
                        row["ai_role_summary"] or "",

                    "ai_core_responsibilities":
                        json.loads(
                            row["ai_core_responsibilities_json"]
                            or "[]"
                        ),

                    "ai_remote_mode":
                        row["ai_remote_mode"] or "",

                    "ai_management_responsibility":
                        bool(
                            row["ai_management_responsibility"]
                        )
                        if row["ai_management_responsibility"] is not None
                        else None,

                    "ai_security_clearance_required":
                        bool(
                            row["ai_security_clearance_required"]
                        )
                        if row["ai_security_clearance_required"] is not None
                        else None,

                    "ai_extraction_confidence":
                        row["ai_extraction_confidence"],

                    "ai_evidence":
                        json.loads(
                            row["ai_evidence_json"]
                            or "[]"
                        ),
                }
            )

        return result

    finally:
        connection.close()


def fetched_grid_options(
    rows: list[dict],
) -> dict:

    return {
        **MOBILE_GRID_EVENT_HANDLERS,
        "defaultColDef": {
            "sortable": True,
            "filter": True,
            "resizable": True,
        },

        "columnDefs": [
            {
                "headerName": "Job title",
                "field": "title",
                "minWidth": 260,
                "flex": 2,
                "pinned": "left",
            },
            {
                "headerName": "Company",
                "field": "company",
                "minWidth": 180,
                "flex": 1,
            },
            {
                "headerName": "Metadata Gate",
                "field": "metadata_gate_status",
                "width": 120,
                "cellClassRules": {
                    "suggest-keep":
                        "value === 'PASS'",

                    "suggest-exclude":
                        "value === 'REJECT'",

                    "suggest-review":
                        "value === 'REVIEW' || value === 'PENDING'",
                },
            },
            {
                "headerName": "AI",
                "field": "ai_status",
                "width": 155,
                "cellClassRules": {
                    "suggest-keep":
                        "value === 'EXTRACTED'",

                    "suggest-exclude":
                        "value === 'BLOCKED_BY_METADATA_GATE'",

                    "suggest-review":
                        "value === 'PENDING'",
                },
            },
            {
                "headerName": "Location",
                "field": "location",
                "minWidth": 220,
                "flex": 1,
            },
            {
                "headerName": "Type",
                "field": "job_type",
                "width": 130,
                "cellClassRules": {
                    "suggest-exclude":
                        "value === 'INTERNSHIP'",

                    "suggest-review":
                        "value === 'CONTRACT'",
                },
            },
            {
                "headerName": "Level",
                "field": "job_level",
                "minWidth": 150,
            },
            {
                "headerName": "Function",
                "field": "job_function",
                "minWidth": 220,
                "flex": 1,
            },
            {
                "headerName": "AI role",
                "field": "ai_role_family",
                "minWidth": 165,
            },
            {
                "headerName": "AI seniority",
                "field": "ai_seniority",
                "width": 140,
                "cellClassRules": {
                    "suggest-exclude":
                        "['senior','staff','principal','lead','manager','director','executive'].includes(value)",
                },
            },
            {
                "headerName": "Min yrs",
                "field": "ai_minimum_years_experience",
                "width": 105,
            },
            {
                "headerName": "Description",
                "field": "description_preview",
                "minWidth": 360,
                "flex": 2,
            },
            {
                "headerName": "Chars",
                "field": "description_chars",
                "width": 90,
            },
            {
                "headerName": "Fetched",
                "field": "fetched_at",
                "minWidth": 210,
            },

            # Hidden fields remain available in selected row data.
            {
                "field": "description",
                "hide": True,
            },
            {
                "field": "job_id",
                "hide": True,
            },
            {
                "field": "job_url",
                "hide": True,
            },
            {
                "field": "job_url_direct",
                "hide": True,
            },
            {
                "field": "classifier_status",
                "hide": True,
            },
            {
                "field": "review_category",
                "hide": True,
            },
            {
                "field": "human_decision",
                "hide": True,
            },
            {
                "field": "metadata_gate_reason",
                "hide": True,
            },
            {
                "field": "metadata_gate_rule_key",
                "hide": True,
            },
            {
                "field": "post_ai_status",
                "hide": True,
            },
            {
                "field": "ai_degree_requirement",
                "hide": True,
            },
            {
                "field": "ai_phd_required",
                "hide": True,
            },
            {
                "field": "ai_languages_required",
                "hide": True,
            },
            {
                "field": "ai_skills_required",
                "hide": True,
            },
            {
                "field": "ai_remote_mode",
                "hide": True,
            },
            {
                "field": "ai_management_responsibility",
                "hide": True,
            },
            {
                "field": "ai_security_clearance_required",
                "hide": True,
            },
            {
                "field": "ai_extraction_confidence",
                "hide": True,
            },
            {
                "field": "ai_evidence",
                "hide": True,
            },
        ],

        "rowData":
            rows,

        "rowSelection": {
            "mode":
                "singleRow",

            "checkboxes":
                True,

            "headerCheckbox":
                False,

            "enableClickSelection":
                True,
        },

        "selectionColumnDef": {
            "width":
                54,

            "pinned":
                "left",
        },

        "suppressCellFocus":
            True,
    }



def load_ai_extracted_jobs() -> list[dict]:
    """
    Dedicated Post-AI dataset.

    This is intentionally separate from the Fetched Jobs UX. Only jobs that
    completed structured AI extraction appear here.
    """

    return [
        row
        for row
        in load_fetched_jobs()
        if row["ai_status"] == "EXTRACTED"
    ]


def ai_extracted_grid_options(
    rows: list[dict],
) -> dict:

    return {
        **MOBILE_GRID_EVENT_HANDLERS,
        "defaultColDef": {
            "sortable": True,
            "filter": True,
            "resizable": True,
        },

        "columnDefs": [
            {
                "headerName": "Post-AI",
                "field": "post_ai_status",
                "width": 120,
                "pinned": "left",
                "cellClassRules": {
                    "suggest-keep":
                        "value === 'SHORTLIST'",

                    "suggest-exclude":
                        "value === 'REJECT'",

                    "suggest-review":
                        "value === 'REVIEW' || !value",
                },
            },
            {
                "headerName": "Job title",
                "field": "title",
                "minWidth": 250,
                "flex": 2,
                "pinned": "left",
            },
            {
                "headerName": "Company",
                "field": "company",
                "minWidth": 170,
                "flex": 1,
            },
            {
                "headerName": "Location",
                "field": "location",
                "minWidth": 210,
            },
            {
                "headerName": "Primary role",
                "field": "ai_role_family",
                "minWidth": 160,
            },
            {
                "headerName": "Seniority",
                "field": "ai_seniority",
                "width": 125,
                "cellClassRules": {
                    "suggest-exclude":
                        "['senior','staff','principal','lead','manager','director','executive'].includes(value)",
                },
            },
            {
                "headerName": "Min req yrs",
                "field": "ai_minimum_years_experience",
                "width": 115,
                "cellClassRules": {
                    "suggest-exclude":
                        "value != null && value >= 3",
                },
            },
            {
                "headerName": "Student req",
                "field": "ai_student_status_required",
                "width": 115,
                "cellClassRules": {
                    "suggest-exclude":
                        "value === true",
                },
            },
            {
                "headerName": "Min degree",
                "field": "ai_minimum_degree_level",
                "width": 125,
            },
            {
                "headerName": "PhD req",
                "field": "ai_phd_required",
                "width": 100,
                "cellClassRules": {
                    "suggest-exclude":
                        "value === true",
                },
            },
            {
                "headerName": "POST_AI reason",
                "field": "post_ai_reason",
                "minWidth": 250,
                "flex": 1,
            },
            {
                "headerName": "Required langs",
                "field": "ai_languages_display",
                "minWidth": 180,
            },
            {
                "headerName": "Role families",
                "field": "ai_role_families_display",
                "minWidth": 260,
                "flex": 1,
            },
            {
                "headerName": "AI+Data hybrid",
                "field": "ai_data_engineering_hybrid",
                "width": 130,
            },
            {
                "headerName": "Pref yrs",
                "field": "ai_preferred_years_experience",
                "width": 100,
            },
            {
                "headerName": "Preferred langs",
                "field": "ai_languages_preferred_display",
                "minWidth": 180,
            },
            {
                "headerName": "Bonus langs",
                "field": "ai_languages_bonus_display",
                "minWidth": 170,
            },
            {
                "headerName": "Confidence",
                "field": "ai_extraction_confidence",
                "width": 110,
            },

            # Hidden detail fields.
            {
                "field": "job_id",
                "hide": True,
            },
            {
                "field": "description",
                "hide": True,
            },
            {
                "field": "job_url",
                "hide": True,
            },
            {
                "field": "job_url_direct",
                "hide": True,
            },
            {
                "field": "ai_role_families",
                "hide": True,
            },
            {
                "field": "ai_experience_requirement_text",
                "hide": True,
            },
            {
                "field": "ai_preferred_experience_text",
                "hide": True,
            },
            {
                "field": "ai_degree_required",
                "hide": True,
            },
            {
                "field": "ai_degree_requirement_text",
                "hide": True,
            },
            {
                "field": "ai_degree_fields",
                "hide": True,
            },
            {
                "field": "ai_phd_preferred",
                "hide": True,
            },
            {
                "field": "ai_student_requirement_text",
                "hide": True,
            },
            {
                "field": "ai_languages_required",
                "hide": True,
            },
            {
                "field": "ai_languages_preferred",
                "hide": True,
            },
            {
                "field": "ai_languages_bonus",
                "hide": True,
            },
            {
                "field": "ai_language_requirement_notes",
                "hide": True,
            },
            {
                "field": "ai_skills_required",
                "hide": True,
            },
            {
                "field": "ai_skills_preferred",
                "hide": True,
            },
            {
                "field": "ai_role_summary",
                "hide": True,
            },
            {
                "field": "ai_core_responsibilities",
                "hide": True,
            },
            {
                "field": "ai_evidence",
                "hide": True,
            },
            {
                "field": "post_ai_rule_key",
                "hide": True,
            },
            {
                "field": "post_ai_review_category",
                "hide": True,
            },
        ],

        "rowData": rows,

        "rowSelection": {
            "mode": "singleRow",
            "enableClickSelection": True,
        },

        "selectionColumnDef": {
            "width": 54,
            "pinned": "left",
        },

        "suppressCellFocus": True,
    }



def show_job_detail_dialog(
    row: dict,
):

    with ui.dialog() as dialog:

        with ui.card().classes(
            "w-[1100px] max-w-[95vw] "
            "max-h-[90vh]"
        ):

            ui.label(
                row["title"]
            ).classes(
                "text-2xl font-bold"
            )

            ui.label(
                (
                    f"{row['company']} "
                    f"— {row['location']}"
                )
            ).classes(
                "text-gray-300"
            )

            with ui.row().classes(
                "gap-3"
            ):

                ui.badge(
                    row["job_type"]
                    or "TYPE UNKNOWN"
                ).classes(
                    (
                        "bg-red-800"
                        if row["job_type"]
                        == "INTERNSHIP"
                        else "bg-slate-700"
                    )
                )

                if row[
                    "job_level"
                ]:
                    ui.badge(
                        row[
                            "job_level"
                        ]
                    ).classes(
                        "bg-slate-700"
                    )

                if row[
                    "classifier_status"
                ]:
                    ui.badge(
                        row[
                            "classifier_status"
                        ]
                    ).classes(
                        "bg-slate-700"
                    )

                if row[
                    "metadata_gate_status"
                ]:
                    ui.badge(
                        "METADATA "
                        + row[
                            "metadata_gate_status"
                        ]
                    ).classes(
                        (
                            "bg-red-800"
                            if row[
                                "metadata_gate_status"
                            ] == "REJECT"
                            else "bg-emerald-800"
                            if row[
                                "metadata_gate_status"
                            ] == "PASS"
                            else "bg-amber-800"
                        )
                    )

                if row[
                    "ai_status"
                ]:
                    ui.badge(
                        "AI "
                        + row[
                            "ai_status"
                        ]
                    ).classes(
                        "bg-slate-700"
                    )

            if row[
                "metadata_gate_reason"
            ]:
                ui.label(
                    "Metadata reason: "
                    + row[
                        "metadata_gate_reason"
                    ]
                ).classes(
                    "text-sm text-gray-400"
                )

            if row[
                "ai_status"
            ] == "EXTRACTED":

                ui.separator()

                ui.label(
                    "AI extracted facts"
                ).classes(
                    "text-lg font-bold"
                )

                with ui.grid(
                    columns=3
                ).classes(
                    "w-full gap-3"
                ):

                    ui.label(
                        "Primary role: "
                        + (
                            row[
                                "ai_role_family"
                            ]
                            or "unknown"
                        )
                    )

                    ui.label(
                        "Role families: "
                        + (
                            ", ".join(
                                row[
                                    "ai_role_families"
                                ]
                            )
                            or "unknown"
                        )
                    )

                    ui.label(
                        "AI + Data hybrid: "
                        + str(
                            row[
                                "ai_data_engineering_hybrid"
                            ]
                        )
                    )

                    ui.label(
                        "Seniority: "
                        + (
                            row[
                                "ai_seniority"
                            ]
                            or "unknown"
                        )
                    )

                    ui.label(
                        "Mandatory min years: "
                        + str(
                            row[
                                "ai_minimum_years_experience"
                            ]
                        )
                    )

                    ui.label(
                        "Preferred years: "
                        + str(
                            row[
                                "ai_preferred_years_experience"
                            ]
                        )
                    )

                    ui.label(
                        "Student status required: "
                        + str(
                            row[
                                "ai_student_status_required"
                            ]
                        )
                    )

                    ui.label(
                        "Degree required: "
                        + str(
                            row[
                                "ai_degree_required"
                            ]
                        )
                    )

                    ui.label(
                        "Minimum degree: "
                        + (
                            row[
                                "ai_minimum_degree_level"
                            ]
                            or "none"
                        )
                    )

                    ui.label(
                        "PhD required: "
                        + str(
                            row[
                                "ai_phd_required"
                            ]
                        )
                    )

                    ui.label(
                        "PhD preferred: "
                        + str(
                            row[
                                "ai_phd_preferred"
                            ]
                        )
                    )

                    ui.label(
                        "Remote mode: "
                        + (
                            row[
                                "ai_remote_mode"
                            ]
                            or "unspecified"
                        )
                    )

                    ui.label(
                        "Management: "
                        + str(
                            row[
                                "ai_management_responsibility"
                            ]
                        )
                    )

                    ui.label(
                        "Security clearance: "
                        + str(
                            row[
                                "ai_security_clearance_required"
                            ]
                        )
                    )

                    ui.label(
                        "Confidence: "
                        + str(
                            row[
                                "ai_extraction_confidence"
                            ]
                        )
                    )

                if row[
                    "ai_role_summary"
                ]:
                    ui.label(
                        "Role summary: "
                        + row[
                            "ai_role_summary"
                        ]
                    ).classes(
                        "text-sm text-gray-300"
                    )

                if row[
                    "ai_experience_requirement_text"
                ]:
                    ui.label(
                        "Mandatory experience evidence: "
                        + row[
                            "ai_experience_requirement_text"
                        ]
                    ).classes(
                        "text-sm"
                    )

                if row[
                    "ai_student_requirement_text"
                ]:
                    ui.label(
                        "Student requirement: "
                        + row[
                            "ai_student_requirement_text"
                        ]
                    ).classes(
                        "text-sm"
                    )

                ui.label(
                    "Required languages: "
                    + (
                        ", ".join(
                            row[
                                "ai_languages_required"
                            ]
                        )
                        or "none"
                    )
                ).classes(
                    "text-sm"
                )

                ui.label(
                    "Preferred languages: "
                    + (
                        ", ".join(
                            row[
                                "ai_languages_preferred"
                            ]
                        )
                        or "none"
                    )
                ).classes(
                    "text-sm"
                )

                ui.label(
                    "Bonus languages: "
                    + (
                        ", ".join(
                            row[
                                "ai_languages_bonus"
                            ]
                        )
                        or "none"
                    )
                ).classes(
                    "text-sm"
                )

                if row[
                    "ai_skills_required"
                ]:
                    ui.label(
                        "Required technical skills: "
                        + ", ".join(
                            row[
                                "ai_skills_required"
                            ]
                        )
                    ).classes(
                        "text-sm"
                    )

                if row[
                    "ai_skills_preferred"
                ]:
                    ui.label(
                        "Preferred technical skills: "
                        + ", ".join(
                            row[
                                "ai_skills_preferred"
                            ]
                        )
                    ).classes(
                        "text-sm text-gray-400"
                    )

                if row[
                    "ai_core_responsibilities"
                ]:
                    ui.label(
                        "Core responsibilities:"
                    ).classes(
                        "font-semibold"
                    )

                    for responsibility in row[
                        "ai_core_responsibilities"
                    ]:
                        ui.label(
                            "• " + responsibility
                        ).classes(
                            "text-sm text-gray-300"
                        )

                if row[
                    "ai_evidence"
                ]:
                    ui.label(
                        "Extraction evidence:"
                    ).classes(
                        "font-semibold"
                    )

                    for item in row[
                        "ai_evidence"
                    ]:
                        ui.label(
                            (
                                f"{item.get('field', '')}: "
                                f"{item.get('evidence', '')}"
                            )
                        ).classes(
                            "text-sm text-gray-400"
                        )

            ui.separator()

            with ui.scroll_area().classes(
                "w-full h-[58vh]"
            ):

                ui.markdown(
                    row[
                        "description"
                    ]
                    or "_No description stored._"
                ).classes(
                    "w-full"
                )

            with ui.row().classes(
                "w-full gap-3"
            ):

                if row[
                    "job_url"
                ]:
                    ui.link(
                        "Open LinkedIn job",
                        row[
                            "job_url"
                        ],
                        new_tab=True,
                    )

                if row[
                    "job_url_direct"
                ]:
                    ui.link(
                        "Open direct apply",
                        row[
                            "job_url_direct"
                        ],
                        new_tab=True,
                    )

                ui.space()

                ui.button(
                    "Close",
                    on_click=dialog.close,
                ).props(
                    "unelevated"
                ).classes(
                    "btn-neutral"
                )

    dialog.open()


async def show_selected_fetched_job():

    if fetched_grid is None:
        return

    rows = (
        await fetched_grid
        .get_selected_rows()
    )

    if not rows:
        ui.notify(
            "Select one fetched job first.",
            type="warning",
        )
        return

    show_job_detail_dialog(
        rows[0]
    )


async def show_selected_ai_extracted_job(
    selected_grid=None,
):

    target_grid = (
        selected_grid
        if selected_grid is not None
        else ai_extracted_grid
    )

    if target_grid is None:
        return

    rows = (
        await target_grid
        .get_selected_rows()
    )

    if not rows:
        ui.notify(
            "Select one AI-extracted job first.",
            type="warning",
        )
        return

    show_job_detail_dialog(
        rows[0]
    )


@ui.refreshable
def fetched_jobs_area():

    global fetched_grid
    global fetched_count_label
    global fetched_metadata_pass_label
    global fetched_metadata_reject_label
    global fetched_ai_pending_label

    rows = load_fetched_jobs()

    gate_pass_count = sum(
        1
        for row in rows
        if row[
            "metadata_gate_status"
        ] == "PASS"
    )

    gate_reject_count = sum(
        1
        for row in rows
        if row[
            "metadata_gate_status"
        ] == "REJECT"
    )

    ai_pending_count = sum(
        1
        for row in rows
        if row[
            "ai_status"
        ] in {
            "",
            "PENDING",
        }
    )

    fetched_count_label.set_text(
        str(
            len(rows)
        )
    )

    fetched_metadata_pass_label.set_text(
        str(
            gate_pass_count
        )
    )

    fetched_metadata_reject_label.set_text(
        str(
            gate_reject_count
        )
    )

    fetched_ai_pending_label.set_text(
        str(
            ai_pending_count
        )
    )

    with ui.row().classes(
        "w-full items-center gap-3 mobile-action-bar"
    ):

        ui.button(
            "Refresh",
            on_click=fetched_jobs_area.refresh,
        ).props(
            "unelevated"
        ).classes(
            "btn-neutral"
        )

        ui.button(
            "View selected description",
            on_click=show_selected_fetched_job,
        ).props(
            "unelevated"
        ).classes(
            "btn-nav"
        )

    fetched_grid = (
        ui.aggrid(
            fetched_grid_options(
                rows
            )
        )
        .classes(
            "w-full"
        )
        .style(
            "height: 68vh;"
        )
    )



def reapply_metadata_rules_from_ui():

    try:
        counts = run_all_fetched_jobs()

    except Exception as exc:
        ui.notify(
            f"Metadata-gate evaluation failed: {exc}",
            type="negative",
        )
        return

    ui.notify(
        (
            f"Metadata gate: {counts['PASS']} pass, "
            f"{counts['REJECT']} reject"
        ),
        type="positive",
    )

    fetched_jobs_area.refresh()


def reapply_post_ai_rules_from_ui():

    try:
        counts = run_all_extracted_jobs()

    except Exception as exc:
        ui.notify(
            f"POST_AI evaluation failed: {exc}",
            type="negative",
        )
        return

    ui.notify(
        (
            f"POST_AI: {counts['SHORTLIST']} shortlist, "
            f"{counts['REVIEW']} review, "
            f"{counts['REJECT']} reject"
        ),
        type="positive",
    )

    ai_extracted_jobs_area.refresh()


@ui.refreshable
def ai_extracted_jobs_area():

    global ai_extracted_grid
    global ai_extracted_count_label
    global ai_shortlist_count_label
    global ai_review_count_label
    global ai_reject_count_label
    global ai_post_pending_count_label

    rows = load_ai_extracted_jobs()
    client_grid = None

    async def show_selected_for_this_client():
        # NiceGUI serves multiple clients in one Python process. Bind this
        # callback to the grid created for this page instead of relying on the
        # module-global reference, which another browser session can replace.
        await show_selected_ai_extracted_job(
            client_grid
        )

    shortlist_count = sum(
        1
        for row in rows
        if row[
            "post_ai_status"
        ] == "SHORTLIST"
    )

    review_count = sum(
        1
        for row in rows
        if row[
            "post_ai_status"
        ] == "REVIEW"
    )

    reject_count = sum(
        1
        for row in rows
        if row[
            "post_ai_status"
        ] == "REJECT"
    )

    post_pending_count = sum(
        1
        for row in rows
        if not row[
            "post_ai_status"
        ]
    )

    # Add compact display-only values for AG Grid.
    for row in rows:

        row[
            "ai_role_families_display"
        ] = ", ".join(
            row[
                "ai_role_families"
            ]
        )

        row[
            "ai_languages_display"
        ] = ", ".join(
            row[
                "ai_languages_required"
            ]
        )

        row[
            "ai_languages_preferred_display"
        ] = ", ".join(
            row[
                "ai_languages_preferred"
            ]
        )

        row[
            "ai_languages_bonus_display"
        ] = ", ".join(
            row[
                "ai_languages_bonus"
            ]
        )

    ai_extracted_count_label.set_text(
        str(
            len(rows)
        )
    )

    ai_shortlist_count_label.set_text(
        str(
            shortlist_count
        )
    )

    ai_review_count_label.set_text(
        str(
            review_count
        )
    )

    ai_reject_count_label.set_text(
        str(
            reject_count
        )
    )

    ai_post_pending_count_label.set_text(
        str(
            post_pending_count
        )
    )

    with ui.row().classes(
        "w-full items-center gap-3 mobile-action-bar"
    ):

        ui.button(
            "Refresh",
            on_click=(
                ai_extracted_jobs_area
                .refresh
            ),
        ).props(
            "unelevated"
        ).classes(
            "btn-neutral"
        )

        ui.button(
            "Reapply POST_AI rules",
            on_click=(
                reapply_post_ai_rules_from_ui
            ),
        ).props(
            "unelevated"
        ).classes(
            "btn-nav"
        )

        ui.button(
            "View selected extraction",
            on_click=(
                show_selected_for_this_client
            ),
        ).props(
            "unelevated"
        ).classes(
            "btn-nav"
        )

    client_grid = (
        ui.aggrid(
            ai_extracted_grid_options(
                rows
            )
        )
        .classes(
            "w-full"
        )
        .style(
            "height: 68vh;"
        )
    )

    # Retain this reference for compatibility with existing single-client
    # callers. The button above deliberately uses the per-client closure.
    ai_extracted_grid = client_grid

    if SETTINGS.windows_automation.enabled:
        with ui.row().classes("w-full justify-end mt-3"):
            ui.button(
                "Close review portal",
                on_click=close_review_portal_from_ui,
            ).props("unelevated").classes("btn-exclude")



# =============================================================================
# GENERIC RULE ENGINE UI
# =============================================================================

def category_grid_options(
    rows: list[dict],
) -> dict:

    return {
        "defaultColDef": {
            "sortable": True,
            "filter": True,
            "resizable": True,
        },

        "columnDefs": [
            {
                "headerName": "Enabled",
                "field": "enabled",
                "width": 105,
                "pinned": "left",
                "cellClassRules": {
                    "rule-enabled": "value === 'YES'",
                    "rule-disabled": "value === 'NO'",
                },
            },
            {
                "headerName": "Order",
                "field": "sort_order",
                "width": 90,
            },
            {
                "headerName": "Display name",
                "field": "display_name",
                "minWidth": 220,
                "flex": 1,
            },
            {
                "headerName": "Default",
                "field": "default_action",
                "width": 125,
                "cellClassRules": {
                    "suggest-keep": "value === 'KEEP'",
                    "suggest-exclude": "value === 'EXCLUDE'",
                    "suggest-review": "value === 'REVIEW'",
                },
            },
            {
                "headerName": "Key",
                "field": "category_key",
                "minWidth": 190,
            },
            {
                "headerName": "Updated",
                "field": "updated_at",
                "minWidth": 220,
            },
        ],

        "rowData": rows,

        "rowSelection": {
            "mode": "multiRow",
            "checkboxes": True,
            "headerCheckbox": True,
            "selectAll": "filtered",
            "enableClickSelection": True,
            "enableSelectionWithoutKeys": True,
        },

        "selectionColumnDef": {
            "width": 54,
            "pinned": "left",
        },

        "suppressCellFocus": True,
    }


def pipeline_rule_grid_options(
    rows: list[dict],
) -> dict:

    return {
        "defaultColDef": {
            "sortable": True,
            "filter": True,
            "resizable": True,
        },

        "columnDefs": [
            {
                "headerName": "Enabled",
                "field": "enabled",
                "width": 105,
                "pinned": "left",
                "cellClassRules": {
                    "rule-enabled": "value === 'YES'",
                    "rule-disabled": "value === 'NO'",
                },
            },
            {
                "headerName": "Priority",
                "field": "priority",
                "width": 100,
            },
            {
                "headerName": "Rule",
                "field": "rule_name",
                "minWidth": 230,
                "flex": 1,
            },
            {
                "headerName": "Field",
                "field": "field_name",
                "minWidth": 180,
            },
            {
                "headerName": "Operator",
                "field": "operator",
                "width": 140,
            },
            {
                "headerName": "Value",
                "field": "match_value",
                "minWidth": 260,
                "flex": 1,
            },
            {
                "headerName": "Action",
                "field": "action",
                "width": 145,
                "cellClassRules": {
                    "suggest-keep": (
                        "value === 'AUTO_KEEP' || "
                        "value === 'SHORTLIST'"
                    ),
                    "suggest-exclude": (
                        "value === 'AUTO_EXCLUDE' || "
                        "value === 'REJECT'"
                    ),
                    "suggest-review": "value === 'REVIEW'",
                },
            },
            {
                "headerName": "Review category",
                "field": "review_category",
                "minWidth": 180,
            },
            {
                "headerName": "Notes",
                "field": "notes",
                "minWidth": 220,
                "flex": 1,
            },
            {
                "headerName": "Source",
                "field": "source",
                "width": 110,
            },
            {
                "field": "rule_id",
                "hide": True,
            },
            {
                "field": "rule_key",
                "hide": True,
            },
        ],

        "rowData": rows,

        "rowSelection": {
            "mode": "multiRow",
            "checkboxes": True,
            "headerCheckbox": True,
            "selectAll": "filtered",
            "enableClickSelection": True,
            "enableSelectionWithoutKeys": True,
        },

        "selectionColumnDef": {
            "width": 54,
            "pinned": "left",
        },

        "suppressCellFocus": True,
    }


@ui.refreshable
def rules_area():

    categories = load_review_categories()
    pre_rules = load_pipeline_rules(
        "PRE_DESCRIPTION"
    )
    metadata_rules = load_pipeline_rules(
        "METADATA_GATE"
    )
    post_rules = load_pipeline_rules(
        "POST_AI"
    )

    category_keys = [
        row["category_key"]
        for row in categories
        if row["enabled"] == "YES"
    ]

    with ui.row().classes(
        "w-full gap-4"
    ):

        with ui.card().classes(
            "metric-card"
        ):
            ui.label(
                "Categories"
            ).classes(
                "text-sm text-gray-400"
            )
            ui.label(
                str(len(categories))
            ).classes(
                "text-2xl font-bold"
            )

        with ui.card().classes(
            "metric-card"
        ):
            ui.label(
                "Pre-description rules"
            ).classes(
                "text-sm text-gray-400"
            )
            ui.label(
                str(len(pre_rules))
            ).classes(
                "text-2xl font-bold"
            )

        with ui.card().classes(
            "metric-card"
        ):
            ui.label(
                "Metadata-gate rules"
            ).classes(
                "text-sm text-gray-400"
            )
            ui.label(
                str(len(metadata_rules))
            ).classes(
                "text-2xl font-bold"
            )

        with ui.card().classes(
            "metric-card"
        ):
            ui.label(
                "Post-AI rules"
            ).classes(
                "text-sm text-gray-400"
            )
            ui.label(
                str(len(post_rules))
            ).classes(
                "text-2xl font-bold"
            )

    with ui.tabs().classes(
        "w-full mt-4"
    ) as rule_tabs:

        categories_tab = ui.tab(
            "Categories"
        )

        pre_tab = ui.tab(
            "Pre-description"
        )

        metadata_tab = ui.tab(
            "Metadata gate"
        )

        post_tab = ui.tab(
            "Post-AI"
        )

    with ui.tab_panels(
        rule_tabs,
        value=categories_tab,
    ).classes(
        "w-full bg-transparent"
    ):

        # =====================================================================
        # CATEGORIES
        # =====================================================================

        with ui.tab_panel(
            categories_tab
        ).classes(
            "p-0"
        ):

            ui.label(
                "Review Categories"
            ).classes(
                "text-2xl font-semibold"
            )

            ui.label(
                (
                    "Categories are now database-managed. "
                    "You can add new categories without editing Python."
                )
            ).classes(
                "text-gray-300"
            )

            category_grid = (
                ui.aggrid(
                    category_grid_options(
                        categories
                    )
                )
                .classes(
                    "w-full"
                )
                .style(
                    "height: 360px;"
                )
            )

            with ui.card().classes(
                "w-full bg-[#111827] "
                "border border-[#2a3a52]"
            ):

                ui.label(
                    "Add or update category"
                ).classes(
                    "text-lg font-semibold"
                )

                with ui.row().classes(
                    "w-full items-end gap-3"
                ):

                    key_input = ui.input(
                        "Category key",
                        placeholder="e.g. robotics",
                    ).classes(
                        "w-64"
                    )

                    name_input = ui.input(
                        "Display name",
                        placeholder="e.g. Robotics",
                    ).classes(
                        "w-64"
                    )

                    action_select = ui.select(
                        [
                            "KEEP",
                            "REVIEW",
                            "EXCLUDE",
                        ],
                        value="REVIEW",
                        label="Default action",
                    ).classes(
                        "w-48"
                    )

                    order_input = ui.number(
                        "Sort order",
                        value=100,
                        min=0,
                        precision=0,
                    ).classes(
                        "w-36"
                    )

                    enabled_switch = ui.switch(
                        "Enabled",
                        value=True,
                    )

                async def save_category():
                    try:
                        add_or_update_category(
                            key_input.value,
                            name_input.value,
                            action_select.value,
                            bool(
                                enabled_switch.value
                            ),
                            int(
                                order_input.value
                                or 100
                            ),
                        )

                    except Exception as exc:
                        ui.notify(
                            f"Could not save category: {exc}",
                            type="negative",
                        )
                        return

                    ui.notify(
                        "Category saved.",
                        type="positive",
                    )

                    rules_area.refresh()

                async def set_selected_categories(
                    enabled: bool,
                ):
                    selected = (
                        await category_grid
                        .get_selected_rows()
                    )

                    if not selected:
                        ui.notify(
                            "Select at least one category.",
                            type="warning",
                        )
                        return

                    count = set_categories_enabled(
                        [
                            row["category_key"]
                            for row in selected
                        ],
                        enabled,
                    )

                    ui.notify(
                        (
                            f"{'Enabled' if enabled else 'Disabled'} "
                            f"{count} categor"
                            f"{'y' if count == 1 else 'ies'}."
                        ),
                        type="positive",
                    )

                    rules_area.refresh()

                with ui.row().classes(
                    "w-full gap-3"
                ):

                    ui.button(
                        "Save category",
                        on_click=save_category,
                    ).props(
                        "unelevated"
                    ).classes(
                        "btn-neutral"
                    )

                    ui.button(
                        "Enable selected",
                        on_click=lambda:
                            set_selected_categories(
                                True
                            ),
                    ).props(
                        "unelevated"
                    ).classes(
                        "btn-neutral"
                    )

                    ui.button(
                        "Disable selected",
                        on_click=lambda:
                            set_selected_categories(
                                False
                            ),
                    ).props(
                        "unelevated"
                    ).classes(
                        "btn-neutral"
                    )

        # =====================================================================
        # PRE-DESCRIPTION RULES
        # =====================================================================

        with ui.tab_panel(
            pre_tab
        ).classes(
            "p-0"
        ):

            ui.label(
                "Pre-description Rules"
            ).classes(
                "text-2xl font-semibold"
            )

            ui.label(
                (
                    "These rules run on cheap discovery fields before "
                    "we fetch the full job description."
                )
            ).classes(
                "text-gray-300"
            )

            pre_grid = (
                ui.aggrid(
                    pipeline_rule_grid_options(
                        pre_rules
                    )
                )
                .classes(
                    "w-full"
                )
                .style(
                    "height: 420px;"
                )
            )

            with ui.card().classes(
                "w-full bg-[#111827] "
                "border border-[#2a3a52]"
            ):

                ui.label(
                    "Add pre-description rule"
                ).classes(
                    "text-lg font-semibold"
                )

                with ui.row().classes(
                    "w-full items-end gap-3"
                ):

                    pre_name = ui.input(
                        "Rule name",
                        placeholder="e.g. Block CIMSOLUTIONS",
                    ).classes(
                        "w-72"
                    )

                    pre_field = ui.select(
                        PRE_RULE_FIELD_OPTIONS,
                        value="company",
                        label="Field",
                    ).classes(
                        "w-52"
                    )

                    pre_operator = ui.select(
                        RULE_OPERATOR_OPTIONS,
                        value="equals",
                        label="Operator",
                    ).classes(
                        "w-52"
                    )

                    pre_value = ui.input(
                        "Value",
                        placeholder="e.g. CIMSOLUTIONS",
                    ).classes(
                        "w-72"
                    )

                with ui.row().classes(
                    "w-full items-end gap-3"
                ):

                    pre_action = ui.select(
                        PRE_RULE_ACTION_OPTIONS,
                        value="AUTO_EXCLUDE",
                        label="Action",
                    ).classes(
                        "w-52"
                    )

                    pre_category = ui.select(
                        [""] + category_keys,
                        value="",
                        label="Review category",
                    ).classes(
                        "w-64"
                    )

                    pre_priority = ui.number(
                        "Priority",
                        value=500,
                        min=0,
                        precision=0,
                    ).classes(
                        "w-36"
                    )

                    pre_enabled = ui.switch(
                        "Enabled",
                        value=True,
                    )

                    pre_notes = ui.input(
                        "Notes",
                    ).classes(
                        "w-80"
                    )

                async def add_pre_rule():
                    try:
                        key = add_pipeline_rule(
                            stage="PRE_DESCRIPTION",
                            rule_name=pre_name.value,
                            field_name=pre_field.value,
                            operator=pre_operator.value,
                            match_value=pre_value.value,
                            action=pre_action.value,
                            review_category=pre_category.value,
                            priority=int(
                                pre_priority.value
                                or 500
                            ),
                            enabled=bool(
                                pre_enabled.value
                            ),
                            notes=pre_notes.value,
                        )

                    except Exception as exc:
                        ui.notify(
                            f"Could not add rule: {exc}",
                            type="negative",
                        )
                        return

                    ui.notify(
                        f"Rule added: {key}",
                        type="positive",
                    )

                    rules_area.refresh()

                async def set_pre_enabled(
                    enabled: bool,
                ):
                    rows = (
                        await pre_grid
                        .get_selected_rows()
                    )

                    if not rows:
                        ui.notify(
                            "Select at least one rule.",
                            type="warning",
                        )
                        return

                    count = set_pipeline_rules_enabled(
                        [
                            int(
                                row["rule_id"]
                            )
                            for row in rows
                        ],
                        enabled,
                    )

                    ui.notify(
                        (
                            f"{'Enabled' if enabled else 'Disabled'} "
                            f"{count} rule(s)."
                        ),
                        type="positive",
                    )

                    rules_area.refresh()

                with ui.row().classes(
                    "w-full gap-3"
                ):

                    ui.button(
                        "Add rule",
                        on_click=add_pre_rule,
                    ).props(
                        "unelevated"
                    ).classes(
                        "btn-neutral"
                    )

                    ui.button(
                        "Enable selected",
                        on_click=lambda:
                            set_pre_enabled(
                                True
                            ),
                    ).props(
                        "unelevated"
                    ).classes(
                        "btn-neutral"
                    )

                    ui.button(
                        "Disable selected",
                        on_click=lambda:
                            set_pre_enabled(
                                False
                            ),
                    ).props(
                        "unelevated"
                    ).classes(
                        "btn-neutral"
                    )

        # =====================================================================
        # METADATA-GATE RULES
        # =====================================================================

        with ui.tab_panel(
            metadata_tab
        ).classes(
            "p-0"
        ):

            ui.label(
                "Metadata Gate Rules"
            ).classes(
                "text-2xl font-semibold"
            )

            ui.label(
                (
                    "Runs after detail fetching using structured metadata "
                    "only. The job description is never passed to this gate."
                )
            ).classes(
                "text-gray-300"
            )

            metadata_grid = (
                ui.aggrid(
                    pipeline_rule_grid_options(
                        metadata_rules
                    )
                )
                .classes(
                    "w-full"
                )
                .style(
                    "height: 420px;"
                )
            )

            with ui.card().classes(
                "w-full bg-[#111827] "
                "border border-[#2a3a52]"
            ):

                ui.label(
                    "Add metadata-gate rule"
                ).classes(
                    "text-lg font-semibold"
                )

                with ui.row().classes(
                    "w-full items-end gap-3"
                ):

                    metadata_name = ui.input(
                        "Rule name",
                        placeholder="e.g. Reject internships",
                    ).classes(
                        "w-72"
                    )

                    metadata_field = ui.select(
                        METADATA_RULE_FIELD_OPTIONS,
                        value="job_type",
                        label="Metadata field",
                    ).classes(
                        "w-60"
                    )

                    metadata_operator = ui.select(
                        RULE_OPERATOR_OPTIONS,
                        value="contains",
                        label="Operator",
                    ).classes(
                        "w-52"
                    )

                    metadata_value = ui.input(
                        "Value",
                        placeholder="e.g. INTERNSHIP",
                    ).classes(
                        "w-60"
                    )

                with ui.row().classes(
                    "w-full items-end gap-3"
                ):

                    metadata_action = ui.select(
                        METADATA_RULE_ACTION_OPTIONS,
                        value="REJECT",
                        label="Action",
                    ).classes(
                        "w-52"
                    )

                    metadata_priority = ui.number(
                        "Priority",
                        value=500,
                        min=0,
                        precision=0,
                    ).classes(
                        "w-36"
                    )

                    metadata_enabled = ui.switch(
                        "Enabled",
                        value=True,
                    )

                    metadata_notes = ui.input(
                        "Notes",
                    ).classes(
                        "w-80"
                    )

                async def add_metadata_rule():
                    try:
                        key = add_pipeline_rule(
                            stage="METADATA_GATE",
                            rule_name=metadata_name.value,
                            field_name=metadata_field.value,
                            operator=metadata_operator.value,
                            match_value=metadata_value.value,
                            action=metadata_action.value,
                            review_category=None,
                            priority=int(
                                metadata_priority.value
                                or 500
                            ),
                            enabled=bool(
                                metadata_enabled.value
                            ),
                            notes=metadata_notes.value,
                        )
                    except Exception as exc:
                        ui.notify(
                            f"Could not add metadata rule: {exc}",
                            type="negative",
                        )
                        return

                    ui.notify(
                        f"Metadata rule added: {key}",
                        type="positive",
                    )
                    rules_area.refresh()

                async def set_metadata_enabled(
                    enabled: bool,
                ):
                    rows = (
                        await metadata_grid
                        .get_selected_rows()
                    )

                    if not rows:
                        ui.notify(
                            "Select at least one rule.",
                            type="warning",
                        )
                        return

                    count = set_pipeline_rules_enabled(
                        [
                            int(row["rule_id"])
                            for row in rows
                        ],
                        enabled,
                    )

                    ui.notify(
                        (
                            f"{'Enabled' if enabled else 'Disabled'} "
                            f"{count} metadata rule(s)."
                        ),
                        type="positive",
                    )
                    rules_area.refresh()

                with ui.row().classes(
                    "w-full gap-3"
                ):

                    ui.button(
                        "Add rule",
                        on_click=add_metadata_rule,
                    ).props(
                        "unelevated"
                    ).classes(
                        "btn-neutral"
                    )

                    ui.button(
                        "Enable selected",
                        on_click=lambda:
                            set_metadata_enabled(True),
                    ).props(
                        "unelevated"
                    ).classes(
                        "btn-neutral"
                    )

                    ui.button(
                        "Disable selected",
                        on_click=lambda:
                            set_metadata_enabled(False),
                    ).props(
                        "unelevated"
                    ).classes(
                        "btn-neutral"
                    )

                ui.button(
                    "Reapply metadata rules to fetched jobs",
                    on_click=reapply_metadata_rules_from_ui,
                ).props(
                    "unelevated"
                ).classes(
                    "btn-nav mt-2"
                )

        # =====================================================================
        # POST-AI RULES
        # =====================================================================

        with ui.tab_panel(
            post_tab
        ).classes(
            "p-0"
        ):

            ui.label(
                "Post-AI Rules"
            ).classes(
                "text-2xl font-semibold"
            )

            ui.label(
                (
                    "These rules run after structured AI extraction. Lower priority numbers "
                    "run first; the first matching rule wins."
                )
            ).classes(
                "text-gray-300"
            )

            post_grid = (
                ui.aggrid(
                    pipeline_rule_grid_options(
                        post_rules
                    )
                )
                .classes(
                    "w-full"
                )
                .style(
                    "height: 420px;"
                )
            )

            with ui.card().classes(
                "w-full bg-[#111827] "
                "border border-[#2a3a52]"
            ):

                ui.label(
                    "Add post-AI rule"
                ).classes(
                    "text-lg font-semibold"
                )

                with ui.row().classes(
                    "w-full items-end gap-3"
                ):

                    post_name = ui.input(
                        "Rule name",
                        placeholder="e.g. Reject 8+ years required",
                    ).classes(
                        "w-72"
                    )

                    post_field = ui.select(
                        POST_AI_FIELD_OPTIONS,
                        value="minimum_years_experience",
                        label="AI field",
                    ).classes(
                        "w-64"
                    )

                    post_operator = ui.select(
                        RULE_OPERATOR_OPTIONS,
                        value="gt",
                        label="Operator",
                    ).classes(
                        "w-52"
                    )

                    post_value = ui.input(
                        "Value",
                        placeholder="e.g. 7",
                    ).classes(
                        "w-60"
                    )

                with ui.row().classes(
                    "w-full items-end gap-3"
                ):

                    post_action = ui.select(
                        POST_AI_ACTION_OPTIONS,
                        value="REVIEW",
                        label="Action",
                    ).classes(
                        "w-52"
                    )

                    post_category = ui.select(
                        [""] + category_keys,
                        value="",
                        label="Review category",
                    ).classes(
                        "w-64"
                    )

                    post_priority = ui.number(
                        "Priority",
                        value=500,
                        min=0,
                        precision=0,
                    ).classes(
                        "w-36"
                    )

                    post_enabled = ui.switch(
                        "Enabled",
                        value=False,
                    )

                    post_notes = ui.input(
                        "Notes",
                    ).classes(
                        "w-80"
                    )

                async def add_post_rule():
                    try:
                        key = add_pipeline_rule(
                            stage="POST_AI",
                            rule_name=post_name.value,
                            field_name=post_field.value,
                            operator=post_operator.value,
                            match_value=post_value.value,
                            action=post_action.value,
                            review_category=post_category.value,
                            priority=int(
                                post_priority.value
                                or 500
                            ),
                            enabled=bool(
                                post_enabled.value
                            ),
                            notes=post_notes.value,
                        )

                    except Exception as exc:
                        ui.notify(
                            f"Could not add rule: {exc}",
                            type="negative",
                        )
                        return

                    ui.notify(
                        f"Post-AI rule added: {key}",
                        type="positive",
                    )

                    rules_area.refresh()

                async def set_post_enabled(
                    enabled: bool,
                ):
                    rows = (
                        await post_grid
                        .get_selected_rows()
                    )

                    if not rows:
                        ui.notify(
                            "Select at least one rule.",
                            type="warning",
                        )
                        return

                    count = set_pipeline_rules_enabled(
                        [
                            int(
                                row["rule_id"]
                            )
                            for row in rows
                        ],
                        enabled,
                    )

                    ui.notify(
                        (
                            f"{'Enabled' if enabled else 'Disabled'} "
                            f"{count} rule(s)."
                        ),
                        type="positive",
                    )

                    rules_area.refresh()

                with ui.row().classes(
                    "w-full gap-3"
                ):

                    ui.button(
                        "Add rule",
                        on_click=add_post_rule,
                    ).props(
                        "unelevated"
                    ).classes(
                        "btn-neutral"
                    )

                    ui.button(
                        "Enable selected",
                        on_click=lambda:
                            set_post_enabled(
                                True
                            ),
                    ).props(
                        "unelevated"
                    ).classes(
                        "btn-neutral"
                    )

                    ui.button(
                        "Disable selected",
                        on_click=lambda:
                            set_post_enabled(
                                False
                            ),
                    ).props(
                        "unelevated"
                    ).classes(
                        "btn-neutral"
                    )


def build_prefetch_panel(
    view_key: str,
) -> None:
    state = {
        "scope": "Latest run",
        "batch_size": 25,
        "batch_index": 0,
        "batch_count": 1,
    }

    def change_scope(event):
        state["scope"] = event.value
        state["batch_index"] = 0
        panel.refresh()

    def change_batch_size(event):
        state["batch_size"] = int(event.value)
        state["batch_index"] = 0
        panel.refresh()

    def previous_page():
        if state["batch_index"] > 0:
            state["batch_index"] -= 1
            panel.refresh()

    def next_page():
        if state["batch_index"] + 1 < state["batch_count"]:
            state["batch_index"] += 1
            panel.refresh()

    @ui.refreshable
    def panel():
        data = load_prefetch_groups(
            view_key,
            state["scope"],
            state["batch_size"],
            state["batch_index"],
        )
        state["batch_index"] = data["batch_index"]
        state["batch_count"] = data["batch_count"]

        with ui.row().classes("w-full gap-4 mobile-metric-row"):
            with ui.card().classes("metric-card"):
                ui.label("Listings").classes("text-sm text-gray-400")
                ui.label(str(data["total_jobs"])).classes("text-2xl font-bold")
            with ui.card().classes("metric-card"):
                ui.label("Review groups").classes("text-sm text-gray-400")
                ui.label(str(data["total_groups"])).classes("text-2xl font-bold")
            with ui.card().classes("metric-card"):
                ui.label("Visible groups").classes("text-sm text-gray-400")
                ui.label(str(len(data["rows"]))).classes("text-2xl font-bold")

        with ui.row().classes("w-full items-end gap-4 mobile-controls"):
            ui.select(
                PREFETCH_SCOPE_OPTIONS,
                value=state["scope"],
                label="Review scope",
                on_change=change_scope,
            ).classes("w-64")
            ui.select(
                BATCH_SIZE_OPTIONS,
                value=state["batch_size"],
                label="Groups per batch",
                on_change=change_batch_size,
            ).classes("w-48")
            ui.button("Refresh", on_click=panel.refresh).props(
                "unelevated"
            ).classes("btn-neutral")
            ui.space()
            previous_button = ui.button(
                "← Previous",
                on_click=previous_page,
            ).props("unelevated").classes("btn-nav")
            next_button = ui.button(
                "Next →",
                on_click=next_page,
            ).props("unelevated").classes("btn-nav")
            if state["batch_index"] <= 0:
                previous_button.disable()
            if state["batch_index"] + 1 >= state["batch_count"]:
                next_button.disable()

        ui.label(
            (
                f"Run: {data['run_id']}"
                if data["run_id"]
                else "All jobs still before description fetching"
            )
        ).classes("text-sm text-gray-400")
        ui.label(
            f"Batch {data['batch_index'] + 1} of {data['batch_count']}"
        ).classes("font-semibold")

        client_grid = ui.aggrid(
            prefetch_grid_options(data["rows"])
        ).classes("w-full").style("height: 58vh;")

        async def select_all_visible():
            await client_grid.run_grid_method("selectAll", "filtered")

        async def clear_selected():
            await client_grid.run_grid_method("deselectAll")

        async def apply_selected(decision: str):
            selected = await client_grid.get_selected_rows()
            if not selected:
                ui.notify("Select at least one row first.", type="warning")
                return
            try:
                result = save_prefetch_decisions(selected, decision)
            except Exception as exc:
                ui.notify(
                    f"{decision} failed: {exc}",
                    type="negative",
                    timeout=8000,
                )
                return

            message = (
                f"{decision}: {result['changed']} changed, "
                f"{result['unchanged']} already set"
            )
            if result["locked"]:
                message += f", {result['locked']} locked after fetching"
            ui.notify(message, type="positive")
            panel.refresh()
            review_grid_area.refresh()
            refresh_continue_pipeline_state()

        with ui.row().classes("w-full items-center gap-3 mobile-action-bar"):
            ui.button(
                "Select all visible",
                on_click=select_all_visible,
            ).props("unelevated").classes("btn-neutral")
            ui.button(
                "Clear selection",
                on_click=clear_selected,
            ).props("unelevated").classes("btn-neutral")
            ui.space()
            ui.button(
                "KEEP SELECTED",
                on_click=lambda: apply_selected("KEEP"),
            ).props("unelevated").classes("btn-keep")
            ui.button(
                "EXCLUDE SELECTED",
                on_click=lambda: apply_selected("EXCLUDE"),
            ).props("unelevated").classes("btn-exclude")

    panel()


def build_prefetch_results_tab() -> None:
    ui.label("Pre-Fetch Results").classes("text-2xl font-bold mt-2")
    ui.label(
        "Audit and override discovery decisions before job descriptions are fetched. "
        "Already-fetched jobs are intentionally excluded from these views."
    ).classes("text-gray-300")

    with ui.tabs().props("mobile-arrows align=left").classes(
        "w-full mt-2 mobile-scroll-tabs"
    ) as prefetch_tabs:
        auto_rejected_tab = ui.tab(
            "prefetch_auto_rejected",
            label=PREFETCH_VIEW_LABELS["AUTO_REJECTED"],
        )
        human_reviewed_tab = ui.tab(
            "prefetch_human_reviewed",
            label=PREFETCH_VIEW_LABELS["HUMAN_REVIEWED"],
        )
        all_results_tab = ui.tab(
            "prefetch_all",
            label=PREFETCH_VIEW_LABELS["ALL"],
        )

    with ui.tab_panels(
        prefetch_tabs,
        value=auto_rejected_tab,
    ).classes("w-full bg-transparent"):
        with ui.tab_panel(auto_rejected_tab).classes("p-0"):
            build_prefetch_panel("AUTO_REJECTED")
        with ui.tab_panel(human_reviewed_tab).classes("p-0"):
            build_prefetch_panel("HUMAN_REVIEWED")
        with ui.tab_panel(all_results_tab).classes("p-0"):
            build_prefetch_panel("ALL")


# =============================================================================
# UI REFRESH
# =============================================================================

@ui.refreshable
def review_grid_area(
    pending_jobs_target=None,
    pending_groups_target=None,
    visible_groups_target=None,
    batch_target=None,
    status_target=None,
):

    global grid
    global batch_index

    pending_jobs_target = pending_jobs_target or pending_jobs_label
    pending_groups_target = pending_groups_target or pending_groups_label
    visible_groups_target = visible_groups_target or visible_groups_label
    batch_target = batch_target or batch_label
    status_target = status_target or status_label

    data = load_pending_groups(
        scope,
        batch_size,
        batch_index,
    )

    batch_index = (
        data[
            "batch_index"
        ]
    )

    pending_jobs_target.set_text(
        str(
            data[
                "total_pending_jobs"
            ]
        )
    )

    pending_groups_target.set_text(
        str(
            data[
                "total_pending_groups"
            ]
        )
    )

    visible_groups_target.set_text(
        str(
            len(
                data[
                    "rows"
                ]
            )
        )
    )

    batch_target.set_text(
        (
            f"Batch "
            f"{data['batch_index'] + 1} "
            f"of {data['batch_count']} "
            f"— "
            f"{len(data['rows'])} "
            f"visible groups"
        )
    )

    status_target.set_text(
        (
            f"Run: "
            f"{data['run_id']}"
            if data[
                "run_id"
            ]
            else "All pending jobs"
        )
    )

    client_grid = None
    action_bar(lambda: client_grid)

    client_grid = (
        ui.aggrid(
            make_grid_options(
                data[
                    "rows"
                ]
            )
        )
        .classes(
            "w-full"
        )
        .style(
            "height: 62vh;"
        )
    )
    action_bar(lambda: client_grid)

    # Retain this reference for compatibility with existing single-client
    # callers. Review actions above deliberately use their per-client grid.
    grid = client_grid


def refresh_continue_pipeline_state():
    if continue_pipeline_button is None:
        return

    connection = connect_database()
    try:
        try:
            batch = resolve_batch(connection)
        except LookupError:
            continue_pipeline_button.disable()
            pipeline_status_label.set_text(
                "Run a daily search before continuing the pipeline."
            )
            return

        pending = mark_review_state(
            connection,
            batch["batch_id"],
        )
        if pending:
            continue_pipeline_button.disable()
            pipeline_status_label.set_text(
                f"{pending} review decision(s) still pending for {batch['batch_date']}."
            )
        elif batch["status"] == "PROCESSING":
            continue_pipeline_button.disable()
            pipeline_status_label.set_text("Pipeline is currently processing.")
        elif batch["status"] == "COMPLETE":
            continue_pipeline_button.disable()
            pipeline_status_label.set_text(
                f"Pipeline completed for {batch['batch_date']}."
            )
        elif batch["status"] == "FAILED":
            continue_pipeline_button.enable()
            pipeline_status_label.set_text(
                f"Pipeline failed for {batch['batch_date']}; continue retries unfinished work."
            )
        else:
            continue_pipeline_button.enable()
            pipeline_status_label.set_text(
                f"Review complete for {batch['batch_date']}; pipeline is ready."
            )
    finally:
        connection.close()


async def continue_pipeline_from_ui():
    continue_pipeline_button.disable()

    if SETTINGS.windows_automation.enabled:
        pipeline_status_label.set_text(
            "Starting the independent Windows pipeline worker..."
        )
        try:
            from simplejobsearch.windows_automation import (
                schedule_review_portal_shutdown,
                start_pipeline_scheduled_task,
            )

            task_name = start_pipeline_scheduled_task()
            schedule_review_portal_shutdown()
        except Exception as exc:
            continue_pipeline_button.enable()
            pipeline_status_label.set_text(
                f"Could not start Windows pipeline worker: {exc}"
            )
            ui.notify(
                f"Could not start pipeline worker: {exc}",
                type="negative",
                timeout=10000,
            )
            return

        pipeline_status_label.set_text(
            f"Task {task_name} started. The review portal will close shortly. "
            "It will reopen before the results email is sent."
        )
        ui.notify(
            "Pipeline worker started; this portal will close shortly.",
            type="positive",
            timeout=8000,
        )
        return

    pipeline_status_label.set_text(
        "Processing: fetching details, metadata gate, AI extraction, and final rules..."
    )
    try:
        summary = await run.io_bound(
            continue_after_review
        )
    except PendingReviewError as exc:
        ui.notify(str(exc), type="warning", timeout=8000)
    except Exception as exc:
        pipeline_status_label.set_text(
            f"Pipeline failed: {type(exc).__name__}: {exc}"
        )
        ui.notify(
            f"Pipeline failed: {exc}",
            type="negative",
            timeout=10000,
        )
    else:
        totals = summary["batch_totals"]
        activity = summary["this_run"]
        pipeline_status_label.set_text(
            summary["message"]
            + " This invocation: "
            f"{activity['details_fetched']} details, "
            f"{activity['ai_processed']} AI attempts. "
            "Batch totals: "
            f"{totals['shortlist']} shortlisted, "
            f"{totals['review']} review, "
            f"{totals['reject']} rejected."
        )
        ui.notify(
            summary["message"],
            type="negative" if summary["status"] == "FAILED" else "positive",
            timeout=10000 if summary["status"] == "FAILED" else 5000,
        )
        fetched_jobs_area.refresh()
        ai_extracted_jobs_area.refresh()
        review_grid_area.refresh()
    finally:
        refresh_continue_pipeline_state()


def close_review_portal_from_ui():
    if not SETTINGS.windows_automation.enabled:
        ui.notify("Windows portal automation is disabled.", type="warning")
        return
    try:
        from simplejobsearch.windows_automation import schedule_review_portal_shutdown

        schedule_review_portal_shutdown()
    except Exception as exc:
        ui.notify(
            f"Could not close review portal: {exc}",
            type="negative",
            timeout=8000,
        )
        return
    ui.notify(
        "Closing NiceGUI and ngrok shortly. You can close this browser tab.",
        type="positive",
        timeout=8000,
    )


def refresh_review():
    review_grid_area.refresh()
    refresh_continue_pipeline_state()


async def select_all(target_grid=None):
    target_grid = target_grid or grid
    if target_grid is None:
        return

    await target_grid.run_grid_method(
        "selectAll",
        "filtered",
    )


async def clear_selection(target_grid=None):
    target_grid = target_grid or grid
    if target_grid is None:
        return

    await target_grid.run_grid_method(
        "deselectAll",
    )


async def apply_decision(
    decision: str,
    target_grid=None,
):

    target_grid = target_grid or grid
    if target_grid is None:
        return

    selected = (
        await target_grid
        .get_selected_rows()
    )

    if not selected:
        ui.notify(
            "Select at least one row first.",
            type="warning",
        )
        return

    try:
        result = save_review_decisions(
            selected,
            decision,
        )

    except Exception as exc:
        ui.notify(
            f"{decision} failed: {exc}",
            type="negative",
            timeout=8000,
        )
        return

    ui.notify(
        (
            f"{decision}: "
            f"{result['saved_jobs']} listing(s) "
            f"across {result['groups']} group(s)."
        ),
        type="positive",
    )

    refresh_review()


def previous_batch():
    global batch_index

    if batch_index <= 0:
        return

    batch_index -= 1
    refresh_review()


def next_batch():
    global batch_index

    data = load_pending_groups(
        scope,
        batch_size,
        batch_index,
    )

    if (
        batch_index
        + 1
        >= data[
            "batch_count"
        ]
    ):
        return

    batch_index += 1
    refresh_review()


def scope_changed(event):
    global scope
    global batch_index

    scope = event.value
    batch_index = 0

    refresh_review()


def batch_size_changed(event):
    global batch_size
    global batch_index

    batch_size = int(
        event.value
    )

    batch_index = 0

    refresh_review()


# =============================================================================
# ACTION BAR
# =============================================================================

def action_bar(target_grid_getter=None):

    def current_grid():
        return target_grid_getter() if target_grid_getter else grid

    with ui.row().classes(
        "w-full items-center gap-3 mobile-action-bar"
    ):

        ui.button(
            "Select all visible",
            on_click=lambda: select_all(current_grid()),
        ).props(
            "unelevated"
        ).classes(
            "btn-neutral"
        )

        ui.button(
            "Clear selection",
            on_click=lambda: clear_selection(current_grid()),
        ).props(
            "unelevated"
        ).classes(
            "btn-neutral"
        )

        ui.space()

        ui.button(
            "KEEP SELECTED",
            on_click=lambda:
                apply_decision(
                    "KEEP",
                    current_grid(),
                ),
        ).props(
            "unelevated"
        ).classes(
            "btn-keep"
        )

        ui.button(
            "EXCLUDE SELECTED",
            on_click=lambda:
                apply_decision(
                    "EXCLUDE",
                    current_grid(),
                ),
        ).props(
            "unelevated"
        ).classes(
            "btn-exclude"
        )


# =============================================================================
# PAGE
# =============================================================================

def _initial_ui_view(request: Request | None) -> str:
    if request is None:
        return "review"
    return "results" if request.url.path.rstrip("/") == "/results" else "review"


def build_ui(request: Request | None = None) -> None:
    global grid, status_label, batch_label
    global pending_jobs_label, pending_groups_label, visible_groups_label
    global continue_pipeline_button, pipeline_status_label
    global fetched_grid, fetched_count_label
    global fetched_metadata_pass_label, fetched_metadata_reject_label
    global fetched_ai_pending_label, ai_extracted_grid
    global ai_extracted_count_label, ai_shortlist_count_label
    global ai_review_count_label, ai_reject_count_label, ai_post_pending_count_label

    apply_migrations()
    with database(DATABASE_PATH) as migration_connection:
        bootstrap_latest_batch(migration_connection)
    verify_database()
    initial_view = _initial_ui_view(request)

    ui.dark_mode().enable()

    ui.add_css(
        """
        :root {
            --app-bg: #0b1220;
            --surface: #111827;
            --surface-2: #172033;
            --surface-3: #1e293b;
            --border: #2a3a52;

            --text: #e5edf7;
            --muted: #93a4ba;

            --accent: #38bdf8;
            --accent-soft: rgba(56, 189, 248, 0.13);

            --keep: #22c55e;
            --keep-dark: #15803d;
            --keep-soft: rgba(34, 197, 94, 0.10);

            --exclude: #ef4444;
            --exclude-dark: #b91c1c;
            --exclude-soft: rgba(239, 68, 68, 0.10);

            --review: #f59e0b;
            --review-dark: #b45309;
            --review-soft: rgba(245, 158, 11, 0.10);
        }

        html,
        body,
        .q-page {
            background: var(--app-bg) !important;
            color: var(--text) !important;
        }

        body {
            font-family:
                Inter,
                ui-sans-serif,
                system-ui,
                -apple-system,
                BlinkMacSystemFont,
                "Segoe UI",
                sans-serif;
        }

        .metric-card {
            min-width: 190px;
            background: var(--surface) !important;
            border: 1px solid var(--border);
            border-radius: 14px;
            box-shadow: none !important;
        }

        .q-field__control {
            background: var(--surface) !important;
            border-radius: 10px !important;
        }

        .q-field__native,
        .q-field__label,
        .q-field__marginal {
            color: var(--text) !important;
        }

        .q-tab {
            color: var(--muted) !important;
        }

        .q-tab--active {
            color: var(--accent) !important;
        }

        .btn-neutral {
            background: var(--surface-3) !important;
            color: var(--text) !important;
            border: 1px solid var(--border) !important;
            border-radius: 9px !important;
            font-weight: 600;
        }

        .btn-neutral:hover {
            background: #26364d !important;
            border-color: #405775 !important;
        }

        .btn-nav {
            background: transparent !important;
            color: var(--accent) !important;
            border: 1px solid rgba(56, 189, 248, 0.35) !important;
            border-radius: 9px !important;
        }

        .btn-nav:hover {
            background: var(--accent-soft) !important;
        }

        .btn-keep {
            background: var(--keep-dark) !important;
            color: white !important;
            border-radius: 9px !important;
            font-weight: 700;
        }

        .btn-keep:hover {
            background: var(--keep) !important;
        }

        .btn-exclude {
            background: var(--exclude-dark) !important;
            color: white !important;
            border-radius: 9px !important;
            font-weight: 700;
        }

        .btn-exclude:hover {
            background: var(--exclude) !important;
        }

        .ag-root-wrapper {
            border: 1px solid var(--border) !important;
            border-radius: 12px !important;
            overflow: hidden;
        }

        .ag-header {
            background: #162238 !important;
            border-bottom: 1px solid var(--border) !important;
        }

        .ag-header-cell {
            color: #dbe7f5 !important;
            font-weight: 700 !important;
        }

        .ag-row {
            border-bottom-color: rgba(64, 87, 117, 0.28) !important;
        }

        .ag-row.row-keep {
            background: var(--keep-soft) !important;
        }

        .ag-row.row-exclude {
            background: var(--exclude-soft) !important;
        }

        .ag-row.row-review {
            background: var(--review-soft) !important;
        }

        .ag-row:hover {
            background: rgba(56, 189, 248, 0.08) !important;
        }

        .ag-row-selected {
            background: rgba(56, 189, 248, 0.22) !important;
            box-shadow: inset 4px 0 0 var(--accent);
        }

        .suggest-keep {
            color: #86efac !important;
            font-weight: 800 !important;
        }

        .suggest-exclude {
            color: #fca5a5 !important;
            font-weight: 800 !important;
        }

        .suggest-review {
            color: #fcd34d !important;
            font-weight: 800 !important;
        }

        .rule-enabled {
            color: #86efac !important;
            font-weight: 800 !important;
        }

        .rule-disabled {
            color: #94a3b8 !important;
            font-weight: 700 !important;
        }

        .ag-checkbox-input-wrapper.ag-checked::after {
            color: var(--accent) !important;
        }

        .ag-checkbox-input-wrapper {
            color: #8ea3bd !important;
        }

        .ag-theme-quartz-dark {
            --ag-background-color: var(--surface);
            --ag-foreground-color: var(--text);
            --ag-header-background-color: #162238;
            --ag-header-foreground-color: #dbe7f5;
            --ag-border-color: var(--border);
            --ag-row-border-color: rgba(64, 87, 117, 0.28);
            --ag-selected-row-background-color: rgba(56, 189, 248, 0.22);
            --ag-row-hover-color: rgba(56, 189, 248, 0.08);
            --ag-checkbox-checked-color: var(--accent);
            --ag-input-focus-border-color: var(--accent);
            --ag-font-size: 14px;
            --ag-row-height: 48px;
        }

        .mobile-scroll-tabs .q-tabs__content {
            justify-content: flex-start;
        }

        @media (max-width: 700px) {
            body {
                overflow-x: hidden;
            }

            .app-shell {
                padding: 0.5rem !important;
                gap: 0.75rem !important;
            }

            .mobile-scroll-tabs .q-tabs__content {
                overflow-x: auto;
                scroll-behavior: smooth;
            }

            .mobile-scroll-tabs .q-tab {
                flex: 0 0 auto;
                min-width: max-content;
                min-height: 44px;
                padding-left: 0.75rem;
                padding-right: 0.75rem;
            }

            .mobile-metric-row {
                flex-wrap: wrap !important;
                gap: 0.5rem !important;
            }

            .metric-card {
                flex: 1 1 145px;
                min-width: 0;
                padding: 0.75rem !important;
            }

            .mobile-controls,
            .mobile-action-bar {
                align-items: stretch !important;
                flex-wrap: wrap !important;
                gap: 0.5rem !important;
            }

            .mobile-controls > .q-field {
                flex: 1 1 100%;
                width: 100% !important;
            }

            .mobile-controls > .q-space,
            .mobile-action-bar > .q-space {
                display: none;
            }

            .mobile-controls > .q-btn,
            .mobile-action-bar > .q-btn {
                flex: 1 1 calc(50% - 0.5rem);
                min-height: 44px;
                white-space: normal;
            }

            .nicegui-aggrid {
                min-width: 0 !important;
            }

            .ag-theme-quartz-dark {
                --ag-font-size: 13px;
                --ag-row-height: 52px;
            }
        }
        """
    )

    with ui.column().classes(
        "w-full max-w-[1800px] "
        "mx-auto p-4 gap-3 app-shell"
    ):

        ui.label(
            "Job Search"
        ).classes(
            "text-3xl font-bold"
        )

        with ui.tabs().props("mobile-arrows align=left").classes(
            "w-full mobile-scroll-tabs"
        ) as tabs:

            review_tab = ui.tab(
                "review",
                label="Review Inbox",
            )

            prefetch_tab = ui.tab(
                "prefetch",
                label="Pre-Fetch Results",
            )

            fetched_tab = ui.tab(
                "fetched",
                label="Fetched Jobs",
            )

            post_ai_tab = ui.tab(
                "results",
                label="Post-AI Extraction",
            )

            rules_tab = ui.tab(
                "rules",
                label="Rules / Settings",
            )

        with ui.tab_panels(
            tabs,
            value=post_ai_tab if initial_view == "results" else review_tab,
        ).classes(
            "w-full bg-transparent"
        ):

            # =====================================================================
            # REVIEW INBOX
            # =====================================================================

            with ui.tab_panel(
                review_tab
            ).classes(
                "p-0"
            ):

                ui.label(
                    "Job Review Inbox"
                ).classes(
                    "text-2xl font-bold mt-2"
                )

                ui.label(
                    (
                        "One listing = one LinkedIn job ID. "
                        "Near-identical listings with the same company + title "
                        "are combined into one review group. "
                        "A decision on a group applies to every listing inside it."
                    )
                ).classes(
                    "text-gray-300"
                )

                with ui.row().classes(
                    "w-full gap-4 mobile-metric-row"
                ):

                    with ui.card().classes(
                        "metric-card"
                    ):
                        ui.label(
                            "Pending listings (LinkedIn IDs)"
                        ).classes(
                            "text-sm text-gray-400"
                        )

                        pending_jobs_label = (
                            ui.label("0")
                            .classes(
                                "text-2xl font-bold"
                            )
                        )

                    with ui.card().classes(
                        "metric-card"
                    ):
                        ui.label(
                            "Pending groups (review rows)"
                        ).classes(
                            "text-sm text-gray-400"
                        )

                        pending_groups_label = (
                            ui.label("0")
                            .classes(
                                "text-2xl font-bold"
                            )
                        )

                    with ui.card().classes(
                        "metric-card"
                    ):
                        ui.label(
                            "Visible groups"
                        ).classes(
                            "text-sm text-gray-400"
                        )

                        visible_groups_label = (
                            ui.label("0")
                            .classes(
                                "text-2xl font-bold"
                            )
                        )

                with ui.row().classes(
                    "w-full items-end gap-4 mobile-controls"
                ):

                    ui.select(
                        SCOPE_OPTIONS,
                        value=scope,
                        label="Review scope",
                        on_change=scope_changed,
                    ).classes(
                        "w-64"
                    )

                    ui.select(
                        BATCH_SIZE_OPTIONS,
                        value=batch_size,
                        label="Groups per batch",
                        on_change=batch_size_changed,
                    ).classes(
                        "w-48"
                    )

                    ui.button(
                        "Refresh",
                        on_click=refresh_review,
                    ).props(
                        "unelevated"
                    ).classes(
                        "btn-neutral"
                    )

                    ui.space()

                    ui.button(
                        "← Previous",
                        on_click=previous_batch,
                    ).props(
                        "unelevated"
                    ).classes(
                        "btn-nav"
                    )

                    ui.button(
                        "Next →",
                        on_click=next_batch,
                    ).props(
                        "unelevated"
                    ).classes(
                        "btn-nav"
                    )

                status_label = (
                    ui.label("")
                    .classes(
                        "text-sm text-gray-400"
                    )
                )

                batch_label = (
                    ui.label("")
                    .classes(
                        "font-semibold"
                    )
                )

                review_grid_area(
                    pending_jobs_label,
                    pending_groups_label,
                    visible_groups_label,
                    batch_label,
                    status_label,
                )

                with ui.card().classes(
                    "w-full mt-4"
                ):
                    ui.label("Continue reviewed batch").classes(
                        "text-lg font-bold"
                    )
                    pipeline_status_label = ui.label("").classes(
                        "text-sm text-gray-300"
                    )
                    continue_pipeline_button = ui.button(
                        "Continue Pipeline",
                        on_click=continue_pipeline_from_ui,
                    ).props("unelevated").classes("btn-keep")
                    refresh_continue_pipeline_state()

            # =====================================================================
            # PRE-FETCH RESULTS
            # =====================================================================

            with ui.tab_panel(
                prefetch_tab
            ).classes(
                "p-0"
            ):

                build_prefetch_results_tab()

            # =====================================================================
            # FETCHED JOBS
            # =====================================================================

            with ui.tab_panel(
                fetched_tab
            ).classes(
                "p-0"
            ):

                ui.label(
                    "Fetched Jobs"
                ).classes(
                    "text-2xl font-bold mt-2"
                )

                ui.label(
                    (
                        "These are jobs whose full LinkedIn description has "
                        "already been downloaded. Metadata PASS means the job is "
                        "eligible for AI extraction; Metadata REJECT stops it before AI. "
                        "Select a row to inspect the full description."
                    )
                ).classes(
                    "text-gray-300"
                )

                with ui.row().classes(
                    "w-full gap-4 mobile-metric-row"
                ):

                    with ui.card().classes(
                        "metric-card"
                    ):
                        ui.label(
                            "Descriptions fetched"
                        ).classes(
                            "text-sm text-gray-400"
                        )

                        fetched_count_label = (
                            ui.label("0")
                            .classes(
                                "text-2xl font-bold"
                            )
                        )

                    with ui.card().classes(
                        "metric-card"
                    ):
                        ui.label(
                            "Metadata PASS"
                        ).classes(
                            "text-sm text-gray-400"
                        )

                        fetched_metadata_pass_label = (
                            ui.label("0")
                            .classes(
                                "text-2xl font-bold"
                            )
                        )

                    with ui.card().classes(
                        "metric-card"
                    ):
                        ui.label(
                            "Metadata REJECT"
                        ).classes(
                            "text-sm text-gray-400"
                        )

                        fetched_metadata_reject_label = (
                            ui.label("0")
                            .classes(
                                "text-2xl font-bold"
                            )
                        )

                    with ui.card().classes(
                        "metric-card"
                    ):
                        ui.label(
                            "AI pending"
                        ).classes(
                            "text-sm text-gray-400"
                        )

                        fetched_ai_pending_label = (
                            ui.label("0")
                            .classes(
                                "text-2xl font-bold"
                            )
                        )

                fetched_jobs_area()

            # =====================================================================
            # POST-AI EXTRACTION
            # =====================================================================

            with ui.tab_panel(
                post_ai_tab
            ).classes(
                "p-0"
            ):

                ui.label(
                    "Post-AI Extraction"
                ).classes(
                    "text-2xl font-bold mt-2"
                )

                ui.label(
                    (
                        "Only jobs with ai_status=EXTRACTED appear here. "
                        "This view is dedicated to the structured facts produced "
                        "by Gemma/Pydantic and is separate from the description-"
                        "fetching workflow."
                    )
                ).classes(
                    "text-gray-300"
                )

                with ui.row().classes(
                    "w-full gap-4 mobile-metric-row"
                ):

                    with ui.card().classes(
                        "metric-card"
                    ):

                        ui.label(
                            "AI extracted"
                        ).classes(
                            "text-sm text-gray-400"
                        )

                        ai_extracted_count_label = (
                            ui.label("0")
                            .classes(
                                "text-2xl font-bold"
                            )
                        )

                    with ui.card().classes(
                        "metric-card"
                    ):

                        ui.label(
                            "SHORTLIST"
                        ).classes(
                            "text-sm text-gray-400"
                        )

                        ai_shortlist_count_label = (
                            ui.label("0")
                            .classes(
                                "text-2xl font-bold"
                            )
                        )

                    with ui.card().classes(
                        "metric-card"
                    ):

                        ui.label(
                            "REVIEW"
                        ).classes(
                            "text-sm text-gray-400"
                        )

                        ai_review_count_label = (
                            ui.label("0")
                            .classes(
                                "text-2xl font-bold"
                            )
                        )

                    with ui.card().classes(
                        "metric-card"
                    ):

                        ui.label(
                            "REJECT"
                        ).classes(
                            "text-sm text-gray-400"
                        )

                        ai_reject_count_label = (
                            ui.label("0")
                            .classes(
                                "text-2xl font-bold"
                            )
                        )

                    with ui.card().classes(
                        "metric-card"
                    ):

                        ui.label(
                            "POST_AI pending"
                        ).classes(
                            "text-sm text-gray-400"
                        )

                        ai_post_pending_count_label = (
                            ui.label("0")
                            .classes(
                                "text-2xl font-bold"
                            )
                        )

                ai_extracted_jobs_area()

            # =====================================================================
            # RULES / SETTINGS
            # =====================================================================

            with ui.tab_panel(
                rules_tab
            ).classes(
                "p-0"
            ):

                ui.label(
                    "Rules / Settings"
                ).classes(
                    "text-2xl font-bold mt-2"
                )

                ui.label(
                    (
                        "Categories and rules are stored in the generic rule engine. "
                        "PRE_DESCRIPTION runs at discovery, METADATA_GATE runs after detail fetching "
                        "without reading the description, and POST_AI runs after structured AI extraction."
                    )
                ).classes(
                    "text-gray-300"
                )

                rules_area()



__all__ = ["build_ui"]

