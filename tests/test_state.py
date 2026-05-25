from __future__ import annotations

import sqlite3
from pathlib import Path

from pipeline.state import MIGRATIONS, SCHEMA_VERSION, StateDB, migrate


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
        assert {
            "keywords_json",
            "entities_json",
            "article_count",
            "last_editorial_at",
            "confidence",
            "newsworthiness_global",
            "newsworthiness_category",
            "newsworthiness_json",
        } <= events_cols
        assert "retry_count" in _columns(conn, "item_errors")
        assert {
            "digest_status",
            "digest_generated_at",
            "digest_model",
            "digest_prompt_version",
            "digest_error",
        } <= _columns(conn, "articles")
        assert {"window_start", "window_end", "status", "stats_json"} <= _columns(conn, "aggregation_windows")
        assert _indexes(conn, "articles") >= {"idx_articles_canonical_url", "idx_articles_unassigned"}
        assert "idx_articles_digest_status_published" in _indexes(conn, "articles")
        assert "idx_events_status_updated" in _indexes(conn, "events")
        assert "idx_events_newsworthiness_global" in _indexes(conn, "events")
        assert "idx_aggregation_windows_status_end" in _indexes(conn, "aggregation_windows")
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
        assert "aggregation_windows" in {
            row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        assert "newsworthiness_json" in _columns(conn, "events")
        assert "digest_status" in _columns(conn, "articles")
        assert versions == [1, 2, 3, 4, 5]
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


def test_state_tracks_aggregation_window_status(tmp_path: Path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)

    with StateDB(db_path) as state:
        state.start_aggregation_window(
            window_start="2026-05-24T16:00:00Z",
            window_end="2026-05-24T22:00:00Z",
            run_id="run-1",
            prompt_version="aggregation-experiment-v6",
            model="gemini-3.1-flash-lite",
        )
        assert state.aggregation_window_status("2026-05-24T16:00:00Z", "2026-05-24T22:00:00Z") == "running"

        state.finish_aggregation_window(
            window_start="2026-05-24T16:00:00Z",
            window_end="2026-05-24T22:00:00Z",
            status="completed",
            article_count=565,
            stats={"groups": 395},
        )

        assert state.aggregation_window_status("2026-05-24T16:00:00Z", "2026-05-24T22:00:00Z") == "completed"
        assert state.latest_completed_aggregation_window() == (
            "2026-05-24T16:00:00Z",
            "2026-05-24T22:00:00Z",
        )


def test_state_upserts_event_and_assigns_articles(tmp_path: Path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)

    with StateDB(db_path) as state:
        article = {
            "article_id": "article-1",
            "source_id": "source",
            "source_name": "Source",
            "url": "https://example.com/story",
            "headline": "Headline",
            "summary": "Summary",
            "published_at": "2026-05-24T16:00:00Z",
            "publish_date_estimated": False,
            "fetched_at": "2026-05-24T16:00:00Z",
            "content_type": "unknown",
            "language": "en",
            "collection": {},
            "fingerprints": {},
        }
        state.insert_article(article, tmp_path / "article-1.json")

        event = {
            "event_id": "2026-05-24-headline",
            "title": "Headline",
            "category": "world",
            "thread": None,
            "status": "active",
            "created_at": "2026-05-24T16:00:00Z",
            "updated_at": "2026-05-24T16:30:00Z",
            "keywords": ["headline"],
            "entities": [],
            "article_count": 1,
            "confidence": 0.7,
            "newsworthiness": {
                "global": 0.8,
                "category": 0.9,
                "rationale_codes": ["test"],
            },
        }
        state.upsert_event(event, tmp_path / "event.json")
        state.assign_articles_to_event(["article-1"], "2026-05-24-headline")

        row = state.conn.execute("SELECT event_id FROM articles WHERE article_id = 'article-1'").fetchone()
        assert row["event_id"] == "2026-05-24-headline"
        event_row = state.conn.execute(
            "SELECT newsworthiness_global, newsworthiness_category FROM events WHERE event_id = '2026-05-24-headline'"
        ).fetchone()
        assert state.event_exists("2026-05-24-headline")
        assert event_row["newsworthiness_global"] == 0.8
        assert event_row["newsworthiness_category"] == 0.9


def test_state_updates_metadata_and_logs_usage(tmp_path: Path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)

    with StateDB(db_path) as state:
        # Test update_article_content_type
        article = {
            "article_id": "art-1",
            "source_id": "source",
            "source_name": "Source",
            "url": "https://example.com/art1",
            "headline": "Headline",
            "published_at": "2026-05-24T16:00:00Z",
            "fetched_at": "2026-05-24T16:00:00Z",
            "article_path": "art-1.json",
            "collection": {},
        }
        state.insert_article(article, tmp_path / "art-1.json")
        state.update_article_content_type("art-1", "opinion")
        row = state.conn.execute("SELECT content_type FROM articles WHERE article_id = 'art-1'").fetchone()
        assert row["content_type"] == "opinion"

        # Test delete_event and reassign_event_articles
        state.conn.execute(
            "INSERT INTO events(event_id, title, category, status, created_at, updated_at) "
            "VALUES ('e-1', 'Title', 'world', 'active', '...', '...')"
        )
        state.conn.execute(
            "INSERT INTO events(event_id, title, category, status, created_at, updated_at) "
            "VALUES ('e-2', 'Title 2', 'world', 'active', '...', '...')"
        )
        state.conn.execute("UPDATE articles SET event_id = 'e-2' WHERE article_id = 'art-1'")

        state.reassign_event_articles("e-2", "e-1")
        row = state.conn.execute("SELECT event_id FROM articles WHERE article_id = 'art-1'").fetchone()
        assert row["event_id"] == "e-1"

        state.delete_event("e-2")
        assert state.conn.execute("SELECT 1 FROM events WHERE event_id = 'e-2'").fetchone() is None

        # Test record_llm_usage
        state.record_llm_usage("run-usage", "aggregation", "gemini-3.1-flash-lite", "v1", 100, 200, 0.001)
        usage_row = state.conn.execute(
            "SELECT model, input_tokens, output_tokens, cost_usd FROM llm_usage WHERE run_id = 'run-usage'"
        ).fetchone()
        assert usage_row["model"] == "gemini-3.1-flash-lite"
        assert usage_row["input_tokens"] == 100
        assert usage_row["output_tokens"] == 200
        assert usage_row["cost_usd"] == 0.001
