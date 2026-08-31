from __future__ import annotations

import json
import random
import re
import sqlite3
import time
import uuid
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from jobspy import scrape_jobs

from rule_engine import (
    classify_pre_description,
    load_category_defaults,
    load_pre_rules,
)
from simplejobsearch.config import get_settings
from simplejobsearch.db import connect as shared_connect
from simplejobsearch.search.queries import as_legacy_searches


# =============================================================================
# CONFIG
# =============================================================================

PROJECT_DIR = Path(__file__).resolve().parent
SETTINGS = get_settings()
DATABASE_PATH = SETTINGS.database_path

TIMEZONE = SETTINGS.timezone

# Backward-compatible default for query rows without an explicit location.
LOCATION = SETTINGS.search.location

# Daily production window
HOURS_OLD = SETTINGS.search.hours_old

# High ceiling:
# JobSpy continues until:
# - LinkedIn returns no more results
# - results_wanted reached
# - internal ~1000 offset cap
# - request error / rate limit
RESULTS_WANTED = SETTINGS.search.results_wanted

# Discovery only.
# Descriptions are fetched later after human review.
FETCH_DESCRIPTIONS = SETTINGS.search.fetch_descriptions

# Delay BETWEEN separate search queries.
# JobSpy itself already delays internally between pagination requests.
SEARCH_DELAY_MIN = SETTINGS.search.delay_min_seconds
SEARCH_DELAY_MAX = SETTINGS.search.delay_max_seconds


# =============================================================================
# SEARCH STRATEGY
#
# Keep the current 11-query baseline for now.
# We will test the 5 Boolean categories separately later.
# =============================================================================

SEARCHES = as_legacy_searches()


# =============================================================================
# NON-DECISION LABELS
# =============================================================================

PHD_PATTERN = re.compile(
    r"(?<!\w)(?:ph\.?d\.?|phd)(?!\w)",
    re.IGNORECASE,
)


# =============================================================================
# GENERAL HELPERS
# =============================================================================

def now_local() -> str:
    return datetime.now(TIMEZONE).isoformat()


def wait_random(
    minimum: float,
    maximum: float,
) -> None:

    delay = random.uniform(
        minimum,
        maximum,
    )

    print(
        f"\nWaiting {delay:.1f} seconds..."
    )

    time.sleep(delay)


def clean_value(value):

    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    if isinstance(value, pd.Timestamp):
        return value.isoformat()

    return value


def clean_text(value) -> str | None:

    value = clean_value(value)

    if value is None:
        return None

    value = str(value).strip()

    return value if value else None


def clean_bool(value) -> int:

    value = clean_value(value)

    if value is None:
        return 0

    if isinstance(value, bool):
        return int(value)

    return int(
        str(value)
        .strip()
        .casefold()
        in {
            "true",
            "1",
            "yes",
            "y",
        }
    )


def normalize_company(
    company: str,
) -> str:

    value = str(company).strip().casefold()

    value = re.sub(
        r"[^\w\s]",
        "",
        value,
    )

    value = re.sub(
        r"\s+",
        " ",
        value,
    )

    return value


def row_to_json(
    row: pd.Series,
) -> str:

    payload = {}

    for column, value in row.items():

        cleaned = clean_value(value)

        if isinstance(cleaned, (str, int, float, bool)) or cleaned is None:
            payload[str(column)] = cleaned

        else:
            payload[str(column)] = str(cleaned)

    return json.dumps(
        payload,
        ensure_ascii=False,
        default=str,
    )


# =============================================================================
# DATABASE
# =============================================================================

def connect_database() -> sqlite3.Connection:
    return shared_connect(DATABASE_PATH)


# =============================================================================
# VERIFY DATABASE COMPONENTS
# =============================================================================

def verify_database(
    connection: sqlite3.Connection,
) -> None:

    required_tables = {
        "jobs",
        "search_runs",
        "search_hits",
        "review_events",
        "review_categories",
        "pipeline_rules",
    }

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

    missing = (
        required_tables
        - existing
    )

    if missing:

        raise RuntimeError(
            "Database is missing required tables:\n"
            + "\n".join(
                sorted(missing)
            )
            + "\n\nRun setup_database.py and "
              "setup_rule_engine.py first."
        )


# =============================================================================
# REBUILD RUNTIME VIEWS
#
# Human decisions have precedence over automatic classifier state.
# =============================================================================

def create_runtime_views(
    connection: sqlite3.Connection,
) -> None:

    # Existing views from the earlier setup are replaced.
    connection.execute(
        """
        DROP VIEW IF EXISTS pending_human_review
        """
    )

    connection.execute(
        """
        DROP VIEW IF EXISTS approved_for_details
        """
    )

    connection.execute(
        """
        DROP VIEW IF EXISTS excluded_jobs
        """
    )

    connection.execute(
        """
        DROP VIEW IF EXISTS job_discovery_summary
        """
    )

    # =========================================================================
    # PENDING HUMAN REVIEW
    # =========================================================================

    connection.execute(
        """
        CREATE VIEW pending_human_review AS

        SELECT
            job_id,
            title,
            company,
            location,
            date_posted,
            job_url,

            review_category,
            review_reason,
            suggested_action,

            first_seen_at,
            last_seen_at

        FROM jobs

        WHERE
            classifier_status = 'REVIEW'

            AND (
                human_decision IS NULL
                OR TRIM(human_decision) = ''
            )
        """
    )

    # =========================================================================
    # APPROVED FOR DETAIL FETCHING
    #
    # Human decision overrides automatic classification.
    # =========================================================================

    connection.execute(
        """
        CREATE VIEW approved_for_details AS

        SELECT *
        FROM jobs

        WHERE
            (
                human_decision = 'KEEP'

                OR (
                    (
                        human_decision IS NULL
                        OR TRIM(human_decision) = ''
                    )

                    AND classifier_status = 'AUTO_KEEP'
                )
            )

            AND (
                details_status IS NULL
                OR TRIM(details_status) = ''
                OR details_status = 'NOT_FETCHED'
                OR details_status = 'FAILED_OR_EMPTY'
            )
        """
    )

    # =========================================================================
    # EFFECTIVELY EXCLUDED
    #
    # Human decision overrides automatic classification.
    # =========================================================================

    connection.execute(
        """
        CREATE VIEW excluded_jobs AS

        SELECT *
        FROM jobs

        WHERE
            human_decision = 'EXCLUDE'

            OR (
                (
                    human_decision IS NULL
                    OR TRIM(human_decision) = ''
                )

                AND classifier_status = 'AUTO_EXCLUDE'
            )
        """
    )

    # =========================================================================
    # DISCOVERY SUMMARY
    #
    # search_hits is authoritative.
    #
    # These values are CALCULATED instead of duplicated into jobs.
    # =========================================================================

    connection.execute(
        """
        CREATE VIEW job_discovery_summary AS

        SELECT
            job_id,

            COUNT(*) AS total_search_hits,

            COUNT(
                DISTINCT search_name
            ) AS distinct_searches,

            COUNT(
                DISTINCT run_id
            ) AS runs_seen,

            GROUP_CONCAT(
                DISTINCT search_name
            ) AS searches_found,

            GROUP_CONCAT(
                DISTINCT search_family
            ) AS families_found

        FROM search_hits

        GROUP BY job_id
        """
    )

    connection.commit()


# =============================================================================
# PRE-DESCRIPTION CLASSIFIER ADAPTER
#
# The authoritative rules now live in:
#
#   review_categories
#   pipeline_rules WHERE stage = 'PRE_DESCRIPTION'
#
# rule_engine.py owns matching, precedence, category defaults, and the
# validated Research Scientist/Engineer + target-tech composite.
# =============================================================================

def classify_job(
    row: pd.Series,
    rules: list[dict],
    category_defaults: dict[str, dict],
) -> dict:

    result = classify_pre_description(
        job=row.to_dict(),
        rules=rules,
        category_defaults=category_defaults,
    )

    # Keep the existing collector/upsert interface unchanged.
    return {
        "classifier_status":
            result.classifier_status,

        "review_category":
            result.review_category,

        "review_reason":
            result.review_reason,

        "suggested_action":
            result.suggested_action,

        "matched_rule_id":
            result.matched_rule_id,

        "matched_rule_key":
            result.matched_rule_key,
    }


# =============================================================================
# SEARCH RUN
# =============================================================================

def create_search_run(
    connection: sqlite3.Connection,
) -> str:

    countries = sorted(
        {
            str(
                search.get("country")
                or search.get("location")
                or LOCATION
            ).strip()
            for search in SEARCHES
            if str(
                search.get("country")
                or search.get("location")
                or LOCATION
            ).strip()
        }
    )

    run_id = (
        "run_"
        + datetime.now(
            TIMEZONE
        ).strftime(
            "%Y%m%d_%H%M%S"
        )
        + "_"
        + uuid.uuid4().hex[:6]
    )

    connection.execute(
        """
        INSERT INTO search_runs (
            run_id,
            started_at,
            finished_at,

            country,
            hours_old,

            search_strategy,

            status,
            notes
        )

        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            now_local(),
            None,

            ", ".join(countries) or LOCATION,
            HOURS_OLD,

            "five_categories_netherlands_switzerland+pipeline_rules_v1+new_jobs_only",

            "RUNNING",
            None,
        ),
    )

    connection.commit()

    return run_id


def finish_search_run(
    connection: sqlite3.Connection,
    run_id: str,
    status: str,
    notes: str | None = None,
) -> None:

    connection.execute(
        """
        UPDATE search_runs

        SET
            finished_at = ?,
            status = ?,
            notes = ?

        WHERE run_id = ?
        """,
        (
            now_local(),
            status,
            notes,
            run_id,
        ),
    )

    connection.commit()


# =============================================================================
# RUN ONE LINKEDIN SEARCH
# =============================================================================

def run_search(
    search: dict,
    *,
    scraper=None,
) -> tuple[pd.DataFrame, str | None]:

    location = str(
        search.get("location")
        or LOCATION
    ).strip()

    country = str(
        search.get("country")
        or location
    ).strip()

    scraper = scraper or scrape_jobs

    print()
    print("=" * 100)

    print(
        f"SEARCH:  "
        f"{search['name']}"
    )

    print(
        f"FAMILY:  "
        f"{search['family']}"
    )

    print(
        f"QUERY:   "
        f"{search['query']}"
    )

    print(
        f"MARKET:  "
        f"{country} ({location})"
    )

    print("=" * 100)

    try:

        jobs = scraper(
            site_name="linkedin",

            search_term=(
                search["query"]
            ),

            location=location,

            hours_old=HOURS_OLD,

            results_wanted=(
                RESULTS_WANTED
            ),

            linkedin_fetch_description=(
                FETCH_DESCRIPTIONS
            ),

            verbose=2,
        )

    except Exception as exc:

        error = str(exc)

        print(
            f"\nSEARCH FAILED:\n"
            f"{error}"
        )

        return (
            pd.DataFrame(),
            error,
        )

    if (
        jobs is None
        or jobs.empty
    ):

        print(
            "\nNo jobs returned."
        )

        return (
            pd.DataFrame(),
            None,
        )

    jobs = jobs.copy()

    jobs[
        "search_name"
    ] = search["name"]

    jobs[
        "search_family"
    ] = search["family"]

    jobs[
        "search_query"
    ] = search["query"]

    jobs[
        "search_country"
    ] = country

    jobs[
        "search_location"
    ] = location

    jobs[
        "observed_at"
    ] = now_local()

    print()
    print(
        f"Rows returned: "
        f"{len(jobs)}"
    )

    print(
        f"Unique IDs:    "
        f"{jobs['id'].nunique()}"
    )

    return (
        jobs,
        None,
    )


# =============================================================================
# RUN ALL SEARCHES
#
# We gather everything first.
# Classification/upsert happens only after GLOBAL dedupe.
# =============================================================================

def run_all_searches() -> tuple[
    pd.DataFrame,
    list[dict],
]:

    collected = []
    failures = []

    total = len(
        SEARCHES
    )

    for index, search in enumerate(
        SEARCHES,
        start=1,
    ):

        print()
        print(
            f"QUERY {index}/{total}"
        )

        jobs, error = run_search(
            search
        )

        if error:

            failures.append(
                {
                    "search_name":
                        search["name"],

                    "location":
                        search.get("location")
                        or LOCATION,

                    "error":
                        error,
                }
            )

        if not jobs.empty:

            collected.append(
                jobs
            )

        if index < total:

            wait_random(
                SEARCH_DELAY_MIN,
                SEARCH_DELAY_MAX,
            )

    if not collected:

        return (
            pd.DataFrame(),
            failures,
        )

    raw_hits = pd.concat(
        collected,
        ignore_index=True,
    )

    return (
        raw_hits,
        failures,
    )


# =============================================================================
# GLOBAL DEDUPLICATION
# =============================================================================

def build_unique_jobs(
    raw_hits: pd.DataFrame,
) -> pd.DataFrame:

    if raw_hits.empty:

        return pd.DataFrame()

    valid = raw_hits[
        raw_hits["id"].notna()
    ].copy()

    unique_jobs = (
        valid
        .drop_duplicates(
            subset=["id"],
            keep="first",
        )
        .copy()
    )

    return unique_jobs


# =============================================================================
# JOB PERSISTENCE
#
# IMPORTANT STATE-MACHINE RULE
# ----------------------------
# A LinkedIn job ID enters the classification pipeline only once:
#
#   NEW job_id
#       -> PRE_DESCRIPTION classification
#       -> INSERT job
#
#   EXISTING job_id
#       -> update last_seen_at only
#       -> classification / human / details / AI state remain untouched
#
# Re-applying rules to historical jobs must be an explicit maintenance action,
# never an automatic side effect of normal discovery.
# =============================================================================

def job_exists(
    connection: sqlite3.Connection,
    job_id: str,
) -> bool:

    row = connection.execute(
        """
        SELECT 1
        FROM jobs
        WHERE job_id = ?
        LIMIT 1
        """,
        (
            job_id,
        ),
    ).fetchone()

    return row is not None


def touch_existing_job(
    connection: sqlite3.Connection,
    job_id: str,
) -> None:

    connection.execute(
        """
        UPDATE jobs

        SET
            last_seen_at = ?

        WHERE job_id = ?
        """,
        (
            now_local(),
            job_id,
        ),
    )


def insert_new_job(
    connection: sqlite3.Connection,
    row: pd.Series,
    classification: dict,
) -> None:

    job_id = clean_text(
        row.get("id")
    )

    if not job_id:
        return

    timestamp = now_local()

    title = clean_text(
        row.get("title")
    ) or "UNKNOWN"

    title_has_phd = int(
        bool(
            PHD_PATTERN.search(
                title
            )
        )
    )

    values = {
        "job_id":
            job_id,

        "site":
            clean_text(
                row.get("site")
            ),

        "title":
            title,

        "company":
            clean_text(
                row.get("company")
            ),

        "location":
            clean_text(
                row.get("location")
            ),

        "date_posted":
            clean_text(
                row.get("date_posted")
            ),

        "job_url":
            clean_text(
                row.get("job_url")
            ),

        "job_url_direct":
            clean_text(
                row.get(
                    "job_url_direct"
                )
            ),

        "is_remote":
            clean_bool(
                row.get("is_remote")
            ),

        "job_type":
            clean_text(
                row.get("job_type")
            ),

        "job_level":
            clean_text(
                row.get("job_level")
            ),

        "job_function":
            clean_text(
                row.get("job_function")
            ),

        "company_industry":
            clean_text(
                row.get(
                    "company_industry"
                )
            ),

        "description":
            clean_text(
                row.get("description")
            ),

        "title_has_phd":
            title_has_phd,

        "classifier_status":
            classification[
                "classifier_status"
            ],

        "review_category":
            classification[
                "review_category"
            ],

        "review_reason":
            classification[
                "review_reason"
            ],

        "suggested_action":
            classification[
                "suggested_action"
            ],

        "human_decision":
            None,

        "human_reviewed_at":
            None,

        "details_status":
            "NOT_FETCHED",

        "details_fetched_at":
            None,

        "first_seen_at":
            timestamp,

        "last_seen_at":
            timestamp,

        "raw_json":
            row_to_json(row),
    }

    connection.execute(
        """
        INSERT INTO jobs (
            job_id,
            site,

            title,
            company,
            location,

            date_posted,

            job_url,
            job_url_direct,

            is_remote,

            job_type,
            job_level,
            job_function,
            company_industry,

            description,

            title_has_phd,

            classifier_status,
            review_category,
            review_reason,
            suggested_action,

            human_decision,
            human_reviewed_at,

            details_status,
            details_fetched_at,

            first_seen_at,
            last_seen_at,

            raw_json
        )

        VALUES (
            :job_id,
            :site,

            :title,
            :company,
            :location,

            :date_posted,

            :job_url,
            :job_url_direct,

            :is_remote,

            :job_type,
            :job_level,
            :job_function,
            :company_industry,

            :description,

            :title_has_phd,

            :classifier_status,
            :review_category,
            :review_reason,
            :suggested_action,

            :human_decision,
            :human_reviewed_at,

            :details_status,
            :details_fetched_at,

            :first_seen_at,
            :last_seen_at,

            :raw_json
        )
        """,
        values,
    )


# =============================================================================
# INSERT SEARCH HITS
#
# One row = one run + one job + one search.
# =============================================================================

def insert_search_hits(
    connection: sqlite3.Connection,
    run_id: str,
    raw_hits: pd.DataFrame,
) -> int:

    if raw_hits.empty:

        return 0

    # Protect against duplicate cards inside one query.
    hits = (
        raw_hits
        .dropna(
            subset=["id"]
        )
        .drop_duplicates(
            subset=[
                "id",
                "search_name",
            ],
            keep="first",
        )
    )

    inserted = 0

    for _, row in hits.iterrows():

        job_id = clean_text(
            row.get("id")
        )

        if not job_id:
            continue

        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO search_hits (
                run_id,
                job_id,

                search_name,
                search_family,
                search_query,

                observed_at
            )

            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                job_id,

                clean_text(
                    row.get(
                        "search_name"
                    )
                ),

                clean_text(
                    row.get(
                        "search_family"
                    )
                ),

                clean_text(
                    row.get(
                        "search_query"
                    )
                ),

                clean_text(
                    row.get(
                        "observed_at"
                    )
                )
                or now_local(),
            ),
        )

        if cursor.rowcount == 1:
            inserted += 1

    return inserted


# =============================================================================
# STORE THIS RUN
# =============================================================================

def store_run_results(
    connection: sqlite3.Connection,
    run_id: str,
    raw_hits: pd.DataFrame,
    rules: list[dict],
    category_defaults: dict[str, dict],
) -> dict:

    unique_jobs = build_unique_jobs(
        raw_hits
    )

    counters = {
        "unique_jobs": 0,
        "new_jobs": 0,
        "existing_jobs": 0,

        "auto_keep": 0,
        "auto_exclude": 0,
        "review": 0,

        "search_hits": 0,
    }

    # =========================================================================
    # 1. Persist globally unique jobs
    #
    # NEW:
    #   classify once, then insert.
    #
    # EXISTING:
    #   touch last_seen_at only.
    #
    # This prevents normal discovery from re-running PRE rules or moving an
    # already processed job backward/sideways through the pipeline.
    # =========================================================================

    for _, row in unique_jobs.iterrows():

        job_id = clean_text(
            row.get("id")
        )

        if not job_id:
            continue

        counters[
            "unique_jobs"
        ] += 1

        if job_exists(
            connection,
            job_id,
        ):

            touch_existing_job(
                connection,
                job_id,
            )

            counters[
                "existing_jobs"
            ] += 1

            continue

        classification = classify_job(
            row=row,
            rules=rules,
            category_defaults=category_defaults,
        )

        insert_new_job(
            connection=connection,
            row=row,
            classification=classification,
        )

        counters[
            "new_jobs"
        ] += 1

        status = classification[
            "classifier_status"
        ]

        if status == "AUTO_KEEP":

            counters[
                "auto_keep"
            ] += 1

        elif status == "AUTO_EXCLUDE":

            counters[
                "auto_exclude"
            ] += 1

        elif status == "REVIEW":

            counters[
                "review"
            ] += 1

    # =========================================================================
    # 2. Store ALL search-hit relationships
    #
    # Existing jobs still receive this run/search observation. That preserves
    # runs_seen, distinct_searches, searches_found, families_found, etc.
    # =========================================================================

    counters[
        "search_hits"
    ] = insert_search_hits(
        connection,
        run_id,
        raw_hits,
    )

    connection.commit()

    return counters


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
    run_id: str,
    counters: dict,
    failures: list[dict],
) -> None:

    total_jobs = scalar(
        connection,
        """
        SELECT COUNT(*)
        FROM jobs
        """
    )

    pending_review = scalar(
        connection,
        """
        SELECT COUNT(*)
        FROM pending_human_review
        """
    )

    approved = scalar(
        connection,
        """
        SELECT COUNT(*)
        FROM approved_for_details
        """
    )

    excluded = scalar(
        connection,
        """
        SELECT COUNT(*)
        FROM excluded_jobs
        """
    )

    run_hits = scalar(
        connection,
        """
        SELECT COUNT(*)
        FROM search_hits
        WHERE run_id = ?
        """,
        (run_id,),
    )

    run_unique = scalar(
        connection,
        """
        SELECT COUNT(
            DISTINCT job_id
        )
        FROM search_hits
        WHERE run_id = ?
        """,
        (run_id,),
    )

    print()
    print("=" * 100)
    print("DAILY COLLECTOR SUMMARY")
    print("=" * 100)

    print(
        f"Run ID:                   "
        f"{run_id}"
    )

    print()
    print(
        f"Search hits this run:      "
        f"{run_hits}"
    )

    print(
        f"Unique jobs this run:      "
        f"{run_unique}"
    )

    print()
    print(
        f"New jobs this run:          "
        f"{counters['new_jobs']}"
    )

    print(
        f"Existing jobs re-seen:      "
        f"{counters['existing_jobs']}"
    )

    print()
    print(
        f"NEW -> AUTO_KEEP:           "
        f"{counters['auto_keep']}"
    )

    print(
        f"NEW -> AUTO_EXCLUDE:        "
        f"{counters['auto_exclude']}"
    )

    print(
        f"NEW -> REVIEW:              "
        f"{counters['review']}"
    )

    print()
    print(
        f"Total jobs in DB:          "
        f"{total_jobs}"
    )

    print(
        f"Pending human review:      "
        f"{pending_review}"
    )

    print(
        f"Approved for details:      "
        f"{approved}"
    )

    print(
        f"Excluded jobs:             "
        f"{excluded}"
    )

    # =========================================================================
    # Per-search contribution
    # =========================================================================

    print()
    print("=" * 100)
    print("THIS RUN BY SEARCH")
    print("=" * 100)

    rows = connection.execute(
        """
        SELECT
            search_name,
            search_family,

            COUNT(*) AS hits,

            COUNT(
                DISTINCT job_id
            ) AS unique_jobs

        FROM search_hits

        WHERE run_id = ?

        GROUP BY
            search_name,
            search_family

        ORDER BY
            search_family,
            search_name
        """,
        (run_id,),
    ).fetchall()

    for row in rows:

        print(
            f"{row['search_family']:<5} "
            f"{row['search_name']:<30} "
            f"{row['unique_jobs']:>5}"
        )

    # =========================================================================
    # Failures
    # =========================================================================

    if failures:

        print()
        print("=" * 100)
        print("SEARCH FAILURES")
        print("=" * 100)

        for failure in failures:

            print(
                f"{failure['search_name']}: "
                f"{failure['error']}"
            )


# =============================================================================
# MAIN
# =============================================================================

def main():

    print()
    print("=" * 100)
    print("DAILY LINKEDIN JOB COLLECTOR")
    print("=" * 100)

    configured_locations = sorted(
        {
            str(
                search.get("location")
                or LOCATION
            ).strip()
            for search in SEARCHES
        }
    )

    print(
        f"Locations:      "
        f"{', '.join(configured_locations)}"
    )

    print(
        f"Window:         "
        f"{HOURS_OLD} hours"
    )

    print(
        f"Searches:       "
        f"{len(SEARCHES)}"
    )

    print(
        f"Descriptions:   "
        f"{FETCH_DESCRIPTIONS}"
    )

    print(
        "Rule engine:    "
        "pipeline_rules / PRE_DESCRIPTION"
    )

    connection = (
        connect_database()
    )

    run_id = None

    try:

        # =====================================================================
        # Database setup verification
        # =====================================================================

        verify_database(
            connection
        )

        create_runtime_views(
            connection
        )

        # =====================================================================
        # Load rules
        # =====================================================================

        rules = load_pre_rules(
            connection
        )

        category_defaults = (
            load_category_defaults(
                connection
            )
        )

        enabled_categories = sum(
            1
            for item
            in category_defaults.values()
            if item["enabled"]
        )

        print()
        print(
            f"Enabled PRE_DESCRIPTION rules: "
            f"{len(rules)}"
        )

        print(
            f"Review categories:             "
            f"{len(category_defaults)} "
            f"({enabled_categories} enabled)"
        )

        # =====================================================================
        # Create search run
        # =====================================================================

        run_id = create_search_run(
            connection
        )

        print(
            f"Run ID: "
            f"{run_id}"
        )

        # =====================================================================
        # Discovery
        # =====================================================================

        raw_hits, failures = (
            run_all_searches()
        )

        if raw_hits.empty:

            finish_search_run(
                connection,
                run_id,
                status=(
                    "FAILED"
                    if failures
                    else "EMPTY"
                ),
                notes=(
                    "No jobs collected."
                ),
            )

            print()
            print(
                "No jobs collected."
            )

            return {
                "run_id": run_id,
                "status": "FAILED" if failures else "EMPTY",
                "counters": {},
                "failures": failures,
            }

        print()
        print("=" * 100)
        print("GLOBAL DISCOVERY")
        print("=" * 100)

        print(
            f"Raw search hits: "
            f"{len(raw_hits)}"
        )

        print(
            f"Unique IDs:      "
            f"{raw_hits['id'].nunique()}"
        )

        # =====================================================================
        # Classify + persist
        # =====================================================================

        counters = store_run_results(
            connection=connection,
            run_id=run_id,
            raw_hits=raw_hits,
            rules=rules,
            category_defaults=category_defaults,
        )

        # =====================================================================
        # Finish run
        # =====================================================================

        if failures:

            status = "PARTIAL"

            notes = (
                f"{len(failures)} "
                f"search(es) failed."
            )

        else:

            status = "SUCCESS"

            notes = (
                f"{counters['unique_jobs']} "
                f"unique jobs observed; "
                f"{counters['new_jobs']} new; "
                f"{counters['existing_jobs']} re-seen; "
                f"{counters['search_hits']} "
                f"search-hit relationships."
            )

        finish_search_run(
            connection,
            run_id,
            status=status,
            notes=notes,
        )

        # =====================================================================
        # Summary
        # =====================================================================

        print_summary(
            connection=connection,
            run_id=run_id,
            counters=counters,
            failures=failures,
        )

    except Exception as exc:

        if run_id:

            try:

                finish_search_run(
                    connection,
                    run_id,
                    status="FAILED",
                    notes=str(exc),
                )

            except Exception:
                pass

        raise

    finally:

        connection.close()

    print()
    print("=" * 100)
    print("RUN COMPLETE")
    print("=" * 100)

    print()
    print(
        f"Database:\n"
        f"{DATABASE_PATH.resolve()}"
    )

    return {
        "run_id": run_id,
        "status": status,
        "counters": counters,
        "failures": failures,
    }


if __name__ == "__main__":
    main()
