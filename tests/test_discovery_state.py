from __future__ import annotations

import sqlite3

import pandas as pd

import daily_collector


def make_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript(
        """
        CREATE TABLE jobs (
            job_id TEXT PRIMARY KEY, site TEXT, title TEXT NOT NULL, company TEXT,
            location TEXT, date_posted TEXT, job_url TEXT, job_url_direct TEXT,
            is_remote INTEGER, job_type TEXT, job_level TEXT, job_function TEXT,
            company_industry TEXT, description TEXT, title_has_phd INTEGER,
            classifier_status TEXT, review_category TEXT, review_reason TEXT,
            suggested_action TEXT, human_decision TEXT, human_reviewed_at TEXT,
            details_status TEXT, details_fetched_at TEXT, first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL, raw_json TEXT, ai_status TEXT
        );
        CREATE TABLE search_runs (run_id TEXT PRIMARY KEY);
        CREATE TABLE search_hits (
            run_id TEXT NOT NULL, job_id TEXT NOT NULL, search_name TEXT NOT NULL,
            search_family TEXT, search_query TEXT, observed_at TEXT NOT NULL,
            PRIMARY KEY (run_id, job_id, search_name)
        );
        INSERT INTO search_runs VALUES ('run-2');
        INSERT INTO jobs (
            job_id, title, classifier_status, human_decision, details_status,
            first_seen_at, last_seen_at, ai_status
        ) VALUES (
            'li-existing', 'Data Scientist', 'REVIEW', 'KEEP', 'FETCHED',
            '2026-08-01T00:00:00+02:00', '2026-08-01T00:00:00+02:00', 'EXTRACTED'
        );
        """
    )
    return connection


def auto_keep_rule() -> dict:
    return {
        "rule_id": 1,
        "rule_key": "data-science",
        "field_name": "title",
        "operator": "contains",
        "match_value": "data scientist",
        "action": "AUTO_KEEP",
        "review_category": None,
        "priority": 200,
    }


def test_existing_job_only_touches_last_seen_and_records_observation():
    connection = make_connection()
    hits = pd.DataFrame(
        [
            {
                "id": "li-existing", "site": "linkedin", "title": "Director Data Scientist",
                "search_name": "data_scientist", "search_family": "DS",
                "search_query": "data scientist", "observed_at": "2026-08-30T01:00:00+02:00",
            },
            {
                "id": "li-new", "site": "linkedin", "title": "Data Scientist",
                "search_name": "data_scientist", "search_family": "DS",
                "search_query": "data scientist", "observed_at": "2026-08-30T01:00:00+02:00",
            },
        ]
    )

    counts = daily_collector.store_run_results(
        connection, "run-2", hits, [auto_keep_rule()], {}
    )

    existing = dict(connection.execute(
        "SELECT * FROM jobs WHERE job_id = 'li-existing'"
    ).fetchone())
    new = dict(connection.execute(
        "SELECT * FROM jobs WHERE job_id = 'li-new'"
    ).fetchone())

    assert existing["first_seen_at"] == "2026-08-01T00:00:00+02:00"
    assert existing["last_seen_at"] != "2026-08-01T00:00:00+02:00"
    assert existing["classifier_status"] == "REVIEW"
    assert existing["human_decision"] == "KEEP"
    assert existing["details_status"] == "FETCHED"
    assert existing["ai_status"] == "EXTRACTED"
    assert new["classifier_status"] == "AUTO_KEEP"
    assert counts == {
        "unique_jobs": 2, "new_jobs": 1, "existing_jobs": 1,
        "auto_keep": 1, "auto_exclude": 0, "review": 0, "search_hits": 2,
    }
    assert connection.execute("SELECT COUNT(*) FROM search_hits").fetchone()[0] == 2

