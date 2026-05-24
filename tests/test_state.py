from __future__ import annotations

import sqlite3
from pathlib import Path

from pipeline.state import MIGRATIONS, SCHEMA_VERSION, migrate


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def _indexes(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA index_list({table})")}


def test_fresh_migrate_creates_full_schema(tmp_path: Path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    conn = sqlite3.connect(db_path)
    try:
        events_cols = _columns(conn, "events")
        assert {"keywords_json", "entities_json", "article_count", "last_editorial_at", "confidence"} <= events_cols
        assert "retry_count" in _columns(conn, "item_errors")
        assert _indexes(conn, "articles") >= {"idx_articles_canonical_url", "idx_articles_unassigned"}
        assert "idx_events_status_updated" in _indexes(conn, "events")
        version = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()[0]
        assert version == SCHEMA_VERSION
    finally:
        conn.close()


def test_migrate_upgrades_existing_v1_database(tmp_path: Path) -> None:
    db_path = tmp_path / "pipeline.db"
    # Apply only the v1 migration to simulate a previously-deployed database.
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            "CREATE TABLE IF NOT EXISTS schema_version ("
            " version INTEGER PRIMARY KEY,"
            " applied_at TEXT NOT NULL"
            ");"
        )
        v1_sql = next(sql for version, sql in MIGRATIONS if version == 1)
        conn.executescript(v1_sql)
        conn.execute("INSERT INTO schema_version(version, applied_at) VALUES (1, '2026-05-24T00:00:00Z')")
        conn.commit()
        assert "keywords_json" not in _columns(conn, "events")
        assert "retry_count" not in _columns(conn, "item_errors")
    finally:
        conn.close()

    migrate(db_path)

    conn = sqlite3.connect(db_path)
    try:
        assert "keywords_json" in _columns(conn, "events")
        assert "retry_count" in _columns(conn, "item_errors")
        versions = [row[0] for row in conn.execute("SELECT version FROM schema_version ORDER BY version")]
        assert versions == [1, 2]
    finally:
        conn.close()


def test_migrate_is_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    migrate(db_path)
    conn = sqlite3.connect(db_path)
    try:
        versions = [row[0] for row in conn.execute("SELECT version FROM schema_version ORDER BY version")]
    finally:
        conn.close()
    assert versions == [v for v, _ in MIGRATIONS]
