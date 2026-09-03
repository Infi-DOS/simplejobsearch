from __future__ import annotations

import sqlite3

from simplejobsearch.workflow import resolve_batch


def test_resolve_batch_follows_latest_search_run_not_calendar_date():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript(
        """
        CREATE TABLE search_runs (
            run_id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL
        );
        CREATE TABLE daily_batches (
            batch_id INTEGER PRIMARY KEY,
            batch_date TEXT NOT NULL UNIQUE,
            search_run_id TEXT,
            status TEXT NOT NULL
        );
        INSERT INTO search_runs VALUES
            ('older', 'SUCCESS', '2026-09-03T00:10:00+02:00'),
            ('newer', 'PARTIAL', '2026-09-03T22:30:00+02:00');
        INSERT INTO daily_batches VALUES
            (1, '2026-09-03', 'older', 'COMPLETE'),
            (2, '2026-09-04', 'newer', 'WAITING_FOR_REVIEW');
        """
    )

    batch = resolve_batch(connection)

    assert batch["batch_id"] == 2
    assert batch["search_run_id"] == "newer"


def test_resolve_batch_keeps_explicit_batch_date_authoritative():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript(
        """
        CREATE TABLE search_runs (
            run_id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL
        );
        CREATE TABLE daily_batches (
            batch_id INTEGER PRIMARY KEY,
            batch_date TEXT NOT NULL UNIQUE,
            search_run_id TEXT,
            status TEXT NOT NULL
        );
        INSERT INTO search_runs VALUES
            ('older', 'SUCCESS', '2026-09-03T00:10:00+02:00'),
            ('newer', 'SUCCESS', '2026-09-03T22:30:00+02:00');
        INSERT INTO daily_batches VALUES
            (1, '2026-09-03', 'older', 'COMPLETE'),
            (2, '2026-09-04', 'newer', 'WAITING_FOR_REVIEW');
        """
    )

    batch = resolve_batch(connection, "2026-09-03")

    assert batch["batch_id"] == 1
    assert batch["search_run_id"] == "older"
