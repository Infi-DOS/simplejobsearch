from __future__ import annotations

import sqlite3

import pandas as pd

import daily_collector
from simplejobsearch.search.queries import (
    afternoon_quoted_searches,
    as_legacy_searches,
    sync_queries,
)
from simplejobsearch.ui import views


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
            review_reason TEXT,
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
        CREATE TABLE pipeline_rules (
            rule_key TEXT PRIMARY KEY,
            source TEXT NOT NULL
        );
        INSERT INTO search_runs VALUES (
            'run-latest', '2026-08-31T00:00:00+02:00', 'SUCCESS'
        );
        INSERT INTO jobs VALUES (
            'auto-1', 'ML Engineer', 'Swiss AI', 'Zürich', '2026-08-31',
            'https://example.test/auto', 'seniority', 'pipeline_rule:base-reject',
            'AUTO_EXCLUDE', 'EXCLUDE',
            NULL, NULL, 'NOT_FETCHED', 0
        );
        INSERT INTO jobs VALUES (
            'human-1', 'Data Scientist', 'Basel Data', 'Basel', '2026-08-31',
            'https://example.test/human', 'general', 'pipeline_rule:review-data',
            'REVIEW', 'REVIEW',
            'KEEP', '2026-08-31T08:00:00+02:00', 'NOT_FETCHED', 0
        );
        INSERT INTO jobs VALUES (
            'pending-1', 'AI Researcher', 'Research AG', 'Lausanne', '2026-08-31',
            'https://example.test/pending', 'research', 'pipeline_rule:review-research',
            'REVIEW', 'REVIEW',
            NULL, NULL, 'NOT_FETCHED', 0
        );
        INSERT INTO jobs VALUES (
            'review-keep', 'Applied AI Engineer', 'Keep Review AG', 'Bern', '2026-08-31',
            'https://example.test/review-keep', 'general', 'pipeline_rule:review-keep',
            'REVIEW', 'KEEP',
            NULL, NULL, 'NOT_FETCHED', 0
        );
        INSERT INTO jobs VALUES (
            'review-exclude', 'AI Consultant', 'Exclude Review AG', 'Basel', '2026-08-31',
            'https://example.test/review-exclude', 'consulting', 'pipeline_rule:review-exclude',
            'REVIEW', 'EXCLUDE',
            NULL, NULL, 'NOT_FETCHED', 0
        );
        INSERT INTO jobs VALUES (
            'fetched-1', 'Vision Engineer', 'Vision AG', 'Bern', '2026-08-31',
            'https://example.test/fetched', 'general', 'pipeline_rule:base-reject',
            'AUTO_EXCLUDE', 'EXCLUDE',
            NULL, NULL, 'FETCHED', 0
        );
        INSERT INTO jobs VALUES (
            'keep-1', 'AI Engineer', 'Target AI', 'Amsterdam', '2026-08-31',
            'https://example.test/keep', NULL, 'pipeline_rule:auto-keep-ai',
            'AUTO_KEEP', 'KEEP', NULL, NULL, 'NOT_FETCHED', 0
        );
        INSERT INTO jobs VALUES (
            'learned-1', 'Data Engineer', 'Blocked Ltd', 'Rotterdam', '2026-08-31',
            'https://example.test/learned', NULL, 'pipeline_rule:learned-company',
            'AUTO_EXCLUDE', 'EXCLUDE', NULL, NULL, 'NOT_FETCHED', 0
        );
        INSERT INTO pipeline_rules VALUES ('learned-company', 'review_inbox');
        INSERT INTO search_hits VALUES ('run-latest', 'auto-1');
        INSERT INTO search_hits VALUES ('run-latest', 'human-1');
        INSERT INTO search_hits VALUES ('run-latest', 'pending-1');
        INSERT INTO search_hits VALUES ('run-latest', 'review-keep');
        INSERT INTO search_hits VALUES ('run-latest', 'review-exclude');
        INSERT INTO search_hits VALUES ('run-latest', 'fetched-1');
        INSERT INTO search_hits VALUES ('run-latest', 'keep-1');
        INSERT INTO search_hits VALUES ('run-latest', 'learned-1');
        """
    )
    connection.commit()
    connection.close()


def test_prefetch_views_and_decision_override(tmp_path, monkeypatch):
    database_path = tmp_path / "prefetch.db"
    _create_prefetch_database(database_path)
    monkeypatch.setattr(views, "DATABASE_PATH", database_path)

    automatic = views.load_prefetch_groups("AUTO_REJECTED", "Latest run", 25, 0)
    forward = views.load_prefetch_groups("FORWARD", "Latest run", 25, 0)
    excluded = views.load_prefetch_groups("EXCLUDED", "Latest run", 25, 0)
    auto_kept = views.load_prefetch_groups("AUTO_KEPT", "Latest run", 25, 0)
    learned = views.load_prefetch_groups(
        "LEARNED_EXCLUSIONS",
        "Latest run",
        25,
        0,
    )
    reviewed = views.load_prefetch_groups("HUMAN_REVIEWED", "Latest run", 25, 0)
    all_results = views.load_prefetch_groups("ALL", "Latest run", 25, 0)
    pending = views.load_pending_groups("Latest run", 25, 0)

    assert automatic["total_jobs"] == 2
    assert forward["total_jobs"] == 2
    assert excluded["total_jobs"] == 2
    assert auto_kept["total_jobs"] == 1
    assert learned["total_jobs"] == 1
    assert reviewed["total_jobs"] == 1
    assert all_results["total_jobs"] == 7
    assert pending["total_pending_jobs"] == 3
    assert {row["suggested"] for row in pending["rows"]} == {
        "KEEP",
        "REVIEW",
        "EXCLUDE",
    }
    assert {row["company"] for row in pending["rows"]}.isdisjoint(
        {"Blocked Ltd", "Swiss AI"}
    )
    assert automatic["rows"][0]["decision"] == "EXCLUDE"
    assert {row["company"] for row in forward["rows"]} == {
        "Basel Data",
        "Target AI",
    }
    assert {row["company"] for row in excluded["rows"]} == {
        "Blocked Ltd",
        "Swiss AI",
    }
    assert auto_kept["rows"][0]["decision"] == "KEEP"
    assert learned["rows"][0]["company"] == "Blocked Ltd"
    assert learned["rows"][0]["reason"] == "pipeline_rule:learned-company"

    filtered_excluded = views.load_prefetch_groups(
        "EXCLUDED",
        "Latest run",
        25,
        0,
        "company",
        "blocked",
    )
    assert filtered_excluded["total_jobs"] == 2
    assert filtered_excluded["total_groups"] == 2
    assert filtered_excluded["total_matching_groups"] == 1
    assert filtered_excluded["rows"][0]["company"] == "Blocked Ltd"

    selected_automatic = [
        row
        for row in automatic["rows"]
        if row["company"] == "Swiss AI"
    ]
    result = views.save_prefetch_decisions(selected_automatic, "KEEP")

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


def test_afternoon_quotes_all_ten_queries_for_both_markets():
    searches = afternoon_quoted_searches()

    assert len(searches) == 10
    assert {item["query"] for item in searches} == {
        '"artificial intelligence"',
        '"data science"',
        '"machine learning"',
        '"computer vision"',
        '"deep learning"',
    }
    assert {item["location"] for item in searches} == {
        "Netherlands",
        "Switzerland",
    }
    assert {item["name"] for item in searches} == {
        "ai_exact",
        "ai_ch_exact",
        "data_science_exact",
        "data_science_ch_exact",
        "machine_learning_exact",
        "machine_learning_ch_exact",
        "computer_vision_exact",
        "computer_vision_ch_exact",
        "deep_learning_exact",
        "deep_learning_ch_exact",
    }
