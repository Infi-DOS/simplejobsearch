from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import sqlite3
from typing import Iterator

from .config import get_settings


MIGRATIONS_DIR = get_settings().project_root / "migrations"


def connect(
    database_path: str | Path | None = None,
    *,
    must_exist: bool = True,
) -> sqlite3.Connection:
    path = Path(database_path) if database_path is not None else get_settings().database_path
    if must_exist and not path.exists():
        raise FileNotFoundError(
            f"Job database not found at {path}. Copy the existing jobs.db there explicitly."
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA busy_timeout = 30000")
    return connection


@contextmanager
def database(database_path: str | Path | None = None) -> Iterator[sqlite3.Connection]:
    connection = connect(database_path)
    try:
        yield connection
    finally:
        connection.close()


@contextmanager
def transaction(connection: sqlite3.Connection, *, immediate: bool = False) -> Iterator[sqlite3.Connection]:
    connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield connection
    except Exception:
        connection.rollback()
        raise
    else:
        connection.commit()


def apply_migrations(
    connection: sqlite3.Connection | None = None,
    migrations_dir: str | Path | None = None,
) -> list[str]:
    owns_connection = connection is None
    connection = connection or connect()
    directory = Path(migrations_dir) if migrations_dir else MIGRATIONS_DIR
    applied: list[str] = []
    try:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                migration_id TEXT PRIMARY KEY,
                applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        existing = {
            row[0]
            for row in connection.execute("SELECT migration_id FROM schema_migrations")
        }
        for path in sorted(directory.glob("*.sql")):
            if path.name in existing:
                continue
            connection.executescript(path.read_text(encoding="utf-8"))
            connection.execute(
                "INSERT INTO schema_migrations (migration_id) VALUES (?)",
                (path.name,),
            )
            connection.commit()
            applied.append(path.name)
        return applied
    finally:
        if owns_connection:
            connection.close()


def scalar(connection: sqlite3.Connection, sql: str, params: tuple = ()):
    row = connection.execute(sql, params).fetchone()
    return None if row is None else row[0]
