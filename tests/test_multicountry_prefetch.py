from __future__ import annotations

import sqlite3

import pandas as pd

import daily_collector
from jobsimplesearch.search.queries import as_legacy_searches, sync_queries
from jobsimplesearch.ui import views


def test_search_queries_include_independent_swiss_market():
    connection = sqlite3.connect(":memory:")
    connection.execute(
        """
        CREATE TABLE search_queries (
            query_key TEXT PRIMARY KEY,
            query_text TEXT NOT NULL,
            role_family TEXT NOT NULL,
            enabled INTEGER NOT NULL,
            country TEXT,
            location TEXT,
            sort_order INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )

    sync_queries(connection, "2026-08-31T12:00:00+02:00")
    searches = as_legacy_searches(connection)

    assert len(searches) == 10
    assert {item["name"] for item in searches if item["location"] == "Switzerland"} == {
        "ai_ch",
        "data_science_ch",
        "machine_learning_ch",
        "computer_vision_ch",
        "deep_learning_ch",
    }


def test_collector_uses_each_query_location():
    captured = {}

    def fake_scrape_jobs(**kwargs):
        captured.update(kwargs)
        return pd.DataFrame([{"id": "li-ch-1", "title": "ML Engineer"}])

    jobs, error = daily_collector.run_search(
        {
            "name": "machine_learning_ch",
            "family": "ML",
            "query": "machine learning",
            "country": "Switzerland",
            "location": "Switzerland",
        },
        scraper=fake_scrape_jobs,
    )

    assert error is None
    assert captured["location"] == "Switzerland"
    assert jobs.iloc[0]["search_country"] == "Switzerland"
    assert jobs.iloc[0]["search_location"] == "Switzerland"


def _create_prefetch_database(path):
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE jobs (
            job_id TEXT PRIMARY KEY,
            title TEXT,
            company TEXT,
            location TEXT,
            date_posted TEXT,
            job_url TEXT,
            review_category TEXT,
            classifier_status TEXT,
            suggested_action TEXT,
            human_decision TEXT,
            human_reviewed_at TEXT,
            details_status TEXT,
            title_has_phd INTEGER
        );
        CREATE TABLE search_runs (
            run_id TEXT PRIMARY KEY,
            started_at TEXT NOT NULL,
            status TEXT NOT NULL
        );
        CREATE TABLE search_hits (
            run_id TEXT NOT NULL,
            job_id TEXT NOT NULL
        );
        CREATE TABLE review_events (
            review_id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT,
            previous_decision TEXT,
            decision TEXT,
            review_category TEXT,
            source TEXT,
            reviewer TEXT,
            reviewed_at TEXT,
            note TEXT
        );
        CREATE TABLE daily_batch_jobs (
            batch_id INTEGER NOT NULL,
            job_id TEXT NOT NULL
        );
        INSERT INTO search_runs VALUES (
            'run-latest', '2026-08-31T00:00:00+02:00', 'SUCCESS'
        );
        INSERT INTO jobs VALUES (
            'auto-1', 'ML Engineer', 'Swiss AI', 'Zürich', '2026-08-31',
            'https://example.test/auto', 'seniority', 'AUTO_EXCLUDE', 'EXCLUDE',
            NULL, NULL, 'NOT_FETCHED', 0
        );
        INSERT INTO jobs VALUES (
            'human-1', 'Data Scientist', 'Basel Data', 'Basel', '2026-08-31',
            'https://example.test/human', 'general', 'REVIEW', 'REVIEW',
            'KEEP', '2026-08-31T08:00:00+02:00', 'NOT_FETCHED', 0
        );
        INSERT INTO jobs VALUES (
            'pending-1', 'AI Researcher', 'Research AG', 'Lausanne', '2026-08-31',
            'https://example.test/pending', 'research', 'REVIEW', 'REVIEW',
            NULL, NULL, 'NOT_FETCHED', 0
        );
        INSERT INTO jobs VALUES (
            'fetched-1', 'Vision Engineer', 'Vision AG', 'Bern', '2026-08-31',
            'https://example.test/fetched', 'general', 'AUTO_EXCLUDE', 'EXCLUDE',
            NULL, NULL, 'FETCHED', 0
        );
        INSERT INTO search_hits VALUES ('run-latest', 'auto-1');
        INSERT INTO search_hits VALUES ('run-latest', 'human-1');
        INSERT INTO search_hits VALUES ('run-latest', 'pending-1');
        INSERT INTO search_hits VALUES ('run-latest', 'fetched-1');
        """
    )
    connection.commit()
    connection.close()


def test_prefetch_views_and_decision_override(tmp_path, monkeypatch):
    database_path = tmp_path / "prefetch.db"
    _create_prefetch_database(database_path)
    monkeypatch.setattr(views, "DATABASE_PATH", database_path)

    automatic = views.load_prefetch_groups("AUTO_REJECTED", "Latest run", 25, 0)
    reviewed = views.load_prefetch_groups("HUMAN_REVIEWED", "Latest run", 25, 0)
    all_results = views.load_prefetch_groups("ALL", "Latest run", 25, 0)

    assert automatic["total_jobs"] == 1
    assert reviewed["total_jobs"] == 1
    assert all_results["total_jobs"] == 3
    assert automatic["rows"][0]["decision"] == "EXCLUDE"

    result = views.save_prefetch_decisions(automatic["rows"], "KEEP")

    assert result == {"changed": 1, "unchanged": 0, "locked": 0, "groups": 1}
    connection = sqlite3.connect(database_path)
    assert connection.execute(
        "SELECT human_decision FROM jobs WHERE job_id = 'auto-1'"
    ).fetchone()[0] == "KEEP"
    event = connection.execute(
        "SELECT previous_decision, decision, source FROM review_events"
    ).fetchone()
    assert event == (None, "KEEP", "nicegui_prefetch_audit")
    connection.close()
