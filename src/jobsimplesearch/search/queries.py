from __future__ import annotations

from dataclasses import dataclass
import sqlite3

from ..config import get_settings


@dataclass(frozen=True)
class SearchQuery:
    key: str
    query_text: str
    role_family: str
    enabled: bool = True
    country: str | None = None
    location: str | None = None


# Production 5-category, 2-country discovery baseline.
#
# Keep LinkedIn discovery simple. PRE_DESCRIPTION rules are responsible for
# filtering titles/seniority/company noise after discovery. The existing keys
# remain the Netherlands baseline for metrics continuity; ``*_ch`` keys track
# Switzerland independently.
SEARCH_QUERIES: tuple[SearchQuery, ...] = (
    SearchQuery(
        key="ai",
        query_text="artificial intelligence",
        role_family="AI",
    ),
    SearchQuery(
        key="data_science",
        query_text="data science",
        role_family="DS",
    ),
    SearchQuery(
        key="machine_learning",
        query_text="machine learning",
        role_family="ML",
    ),
    SearchQuery(
        key="computer_vision",
        query_text="computer vision",
        role_family="CV",
    ),
    SearchQuery(
        key="deep_learning",
        query_text="deep learning",
        role_family="DL",
    ),
    SearchQuery(
        key="ai_ch",
        query_text="artificial intelligence",
        role_family="AI",
        country="Switzerland",
        location="Switzerland",
    ),
    SearchQuery(
        key="data_science_ch",
        query_text="data science",
        role_family="DS",
        country="Switzerland",
        location="Switzerland",
    ),
    SearchQuery(
        key="machine_learning_ch",
        query_text="machine learning",
        role_family="ML",
        country="Switzerland",
        location="Switzerland",
    ),
    SearchQuery(
        key="computer_vision_ch",
        query_text="computer vision",
        role_family="CV",
        country="Switzerland",
        location="Switzerland",
    ),
    SearchQuery(
        key="deep_learning_ch",
        query_text="deep learning",
        role_family="DL",
        country="Switzerland",
        location="Switzerland",
    ),
)


def load_queries(
    connection: sqlite3.Connection | None = None,
) -> tuple[SearchQuery, ...]:
    """Load query configuration from SQLite, falling back to code defaults."""

    owns_connection = connection is None

    if connection is None:
        try:
            from ..db import connect

            connection = connect()
        except (FileNotFoundError, sqlite3.OperationalError):
            return SEARCH_QUERIES

    try:
        exists = connection.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table'
              AND name = 'search_queries'
            """
        ).fetchone()

        if not exists:
            return SEARCH_QUERIES

        rows = connection.execute(
            """
            SELECT
                query_key,
                query_text,
                role_family,
                enabled,
                country,
                location
            FROM search_queries
            ORDER BY sort_order, query_key
            """
        ).fetchall()

        if not rows:
            return SEARCH_QUERIES

        return tuple(
            SearchQuery(
                key=row[0],
                query_text=row[1],
                role_family=row[2],
                enabled=bool(row[3]),
                country=row[4],
                location=row[5],
            )
            for row in rows
        )

    finally:
        if owns_connection and connection is not None:
            connection.close()


def as_legacy_searches(
    connection: sqlite3.Connection | None = None,
) -> list[dict[str, str]]:
    """Adapt enabled query definitions to the collector's stable contract."""

    settings = get_settings().search

    return [
        {
            "name": item.key,
            "family": item.role_family,
            "query": item.query_text,
            "country": item.country or settings.country,
            "location": item.location or settings.location,
        }
        for item in load_queries(connection)
        if item.enabled
    ]


def sync_queries(
    connection: sqlite3.Connection,
    timestamp: str,
) -> None:
    """
    Seed code-default queries into a database that does not already have them.

    Existing database configuration is deliberately preserved. Production
    changes to an existing database should be made through a migration.
    """

    settings = get_settings().search

    for sort_order, item in enumerate(
        SEARCH_QUERIES,
        start=10,
    ):
        connection.execute(
            """
            INSERT INTO search_queries (
                query_key,
                query_text,
                role_family,
                enabled,
                country,
                location,
                sort_order,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)

            ON CONFLICT(query_key) DO NOTHING
            """,
            (
                item.key,
                item.query_text,
                item.role_family,
                int(item.enabled),
                item.country or settings.country,
                item.location or settings.location,
                sort_order,
                timestamp,
                timestamp,
            ),
        )

    connection.commit()
