from __future__ import annotations

import json
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
            "aggregation_status",
            "aggregation_reason",
            "is_filtered",
            "collection_run_id",
        } <= _columns(conn, "articles")
        assert {"window_start", "window_end", "status", "stats_json"} <= _columns(conn, "aggregation_windows")
        assert {
            "run_id",
            "source_id",
            "feed_status",
            "entries_seen",
            "articles_written",
            "articles_skipped_duplicate",
            "error_count",
            "stats_json",
        } <= _columns(conn, "source_run_stats")
        assert _indexes(conn, "articles") >= {"idx_articles_canonical_url", "idx_articles_unassigned"}
        assert "idx_articles_digest_status_published" in _indexes(conn, "articles")
        assert "idx_articles_aggregation_status_published" in _indexes(conn, "articles")
        assert "idx_articles_is_filtered_published" in _indexes(conn, "articles")
        assert "idx_articles_collection_run" in _indexes(conn, "articles")
        assert "idx_events_status_updated" in _indexes(conn, "events")
        assert "idx_events_newsworthiness_global" in _indexes(conn, "events")
        assert "idx_aggregation_windows_status_end" in _indexes(conn, "aggregation_windows")
        assert "idx_source_run_stats_source_finished" in _indexes(conn, "source_run_stats")
        assert "deduplication_reviews" in {
            row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        assert "idx_deduplication_reviews_reviewed_at" in _indexes(
            conn, "deduplication_reviews"
        )
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
            "CREATE TABLE IF NOT EXISTS schema_version ( version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);"
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
        assert "aggregation_status" in _columns(conn, "articles")
        assert "is_filtered" in _columns(conn, "articles")
        assert "collection_run_id" in _columns(conn, "articles")
        assert "source_run_stats" in {
            row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        assert versions == [1, 2, 3, 4, 5, 6, 7, 8, 9]
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


def test_state_records_source_run_stats(tmp_path: Path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)

    with StateDB(db_path) as state:
        state.upsert_source_run_stats(
            [
                {
                    "run_id": "collection-run",
                    "source_id": "fixture",
                    "source_name": "Fixture",
                    "started_at": "2026-06-01T00:00:00Z",
                    "finished_at": "2026-06-01T00:01:00Z",
                    "feed_status": "fetched",
                    "feed_http_status": 200,
                    "entries_seen": 3,
                    "articles_written": 2,
                    "articles_synced_existing": 1,
                    "articles_skipped": 1,
                    "articles_skipped_old": 0,
                    "articles_skipped_duplicate": 1,
                    "articles_failed": 0,
                    "images_fetched": 1,
                    "images_skipped": 1,
                    "images_failed": 0,
                    "error_count": 0,
                    "stats_json": {"feed_url": "https://example.test/feed.xml"},
                }
            ]
        )
        row = state.conn.execute(
            "SELECT * FROM source_run_stats WHERE run_id = ? AND source_id = ?",
            ("collection-run", "fixture"),
        ).fetchone()

    assert row["feed_status"] == "fetched"
    assert row["entries_seen"] == 3
    assert row["articles_written"] == 2
    assert row["articles_skipped_duplicate"] == 1
    assert row["images_fetched"] == 1
    assert row["stats_json"] == '{"feed_url": "https://example.test/feed.xml"}'


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


def test_unassigned_window_count_ignores_filtered_articles(tmp_path: Path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)

    with StateDB(db_path) as state:
        for article_id in ("pending", "filtered"):
            article = {
                "article_id": article_id,
                "source_id": "source",
                "source_name": "Source",
                "url": f"https://example.com/{article_id}",
                "headline": f"Headline {article_id}",
                "summary": "Summary",
                "published_at": "2026-05-24T16:30:00Z",
                "publish_date_estimated": False,
                "fetched_at": "2026-05-24T16:31:00Z",
                "content_type": "unknown",
                "language": "en",
                "collection": {},
                "fingerprints": {},
            }
            state.insert_article(article, tmp_path / f"{article_id}.json")
        state.update_article_aggregation_status("filtered", status="filtered_non_news")

        # Verify that is_filtered is updated to 1
        filtered_row = state.conn.execute("SELECT is_filtered FROM articles WHERE article_id = 'filtered'").fetchone()
        assert filtered_row["is_filtered"] == 1

        assert (
            state.unassigned_article_count_in_window(
                "2026-05-24T16:00:00Z",
                "2026-05-24T22:00:00Z",
            )
            == 1
        )
        assert state.article_time_bounds(unassigned_only=True) == (
            "2026-05-24T16:30:00Z",
            "2026-05-24T16:30:00Z",
        )
        assert state.unassigned_article_published_times(
            "2026-05-24T00:00:00Z",
            "2026-05-25T00:00:00Z",
        ) == ["2026-05-24T16:30:00Z"]
        state.update_article_aggregation_status("pending", status="filtered_low_impact")

        pending_row = state.conn.execute("SELECT is_filtered FROM articles WHERE article_id = 'pending'").fetchone()
        assert pending_row["is_filtered"] == 1

        assert state.article_time_bounds(unassigned_only=True) is None
        assert (
            state.unassigned_article_published_times(
                "2026-05-24T00:00:00Z",
                "2026-05-25T00:00:00Z",
            )
            == []
        )


def test_article_queries_respect_snapshot_rowid(tmp_path: Path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    with StateDB(db_path) as state:
        for article_id, published_at in (
            ("before", "2026-05-24T16:30:00Z"),
            ("after", "2026-05-24T17:30:00Z"),
        ):
            state.insert_article(
                {
                    "article_id": article_id,
                    "source_id": "source",
                    "source_name": "Source",
                    "url": f"https://example.com/{article_id}",
                    "headline": article_id,
                    "published_at": published_at,
                    "fetched_at": published_at,
                    "collection": {},
                    "fingerprints": {},
                },
                tmp_path / f"{article_id}.json",
            )
            if article_id == "before":
                snapshot_rowid = state.conn.execute(
                    "SELECT rowid FROM articles WHERE article_id = 'before'"
                ).fetchone()[0]

        assert state.article_time_bounds(
            max_article_rowid=snapshot_rowid
        ) == ("2026-05-24T16:30:00Z", "2026-05-24T16:30:00Z")
        assert state.unassigned_article_published_times(
            "2026-05-24T00:00:00Z",
            "2026-05-25T00:00:00Z",
            max_article_rowid=snapshot_rowid,
        ) == ["2026-05-24T16:30:00Z"]
        assert state.unassigned_article_count_in_window(
            "2026-05-24T16:00:00Z",
            "2026-05-24T22:00:00Z",
            max_article_rowid=snapshot_rowid,
        ) == 1


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


def test_update_article_aggregation_status_preserves_assigned_articles(tmp_path: Path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)

    with StateDB(db_path) as state:
        article = {
            "article_id": "assigned",
            "source_id": "source",
            "source_name": "Source",
            "url": "https://example.com/assigned",
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
        state.insert_article(article, tmp_path / "assigned.json")
        state.upsert_event(
            {
                "event_id": "event-1",
                "title": "Event",
                "category": "world",
                "thread": None,
                "status": "active",
                "created_at": "2026-05-24T16:00:00Z",
                "updated_at": "2026-05-24T16:00:00Z",
                "article_ids": ["assigned"],
                "article_count": 1,
                "confidence": 0.7,
                "newsworthiness": None,
                "llm_metadata": {},
            },
            tmp_path / "event-1.json",
        )
        state.assign_articles_to_event(["assigned"], "event-1")

        state.update_article_aggregation_status(
            "assigned",
            status="filtered_low_impact",
            reason="below threshold",
        )

        row = state.conn.execute(
            "SELECT aggregation_status, aggregation_reason, is_filtered, event_id "
            "FROM articles WHERE article_id = 'assigned'"
        ).fetchone()
        assert row["aggregation_status"] == "assigned"
        assert row["aggregation_reason"] is None
        assert row["is_filtered"] == 0
        assert row["event_id"] == "event-1"


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


def test_set_article_aggregation_pending_if_unassigned(tmp_path: Path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)

    def _make(article_id: str) -> dict[str, object]:
        return {
            "article_id": article_id,
            "source_id": "source",
            "source_name": "Source",
            "url": f"https://example.com/{article_id}",
            "headline": f"Headline {article_id}",
            "summary": "Summary",
            "published_at": "2026-05-24T16:30:00Z",
            "publish_date_estimated": False,
            "fetched_at": "2026-05-24T16:31:00Z",
            "content_type": "unknown",
            "language": "en",
            "collection": {},
            "fingerprints": {},
        }

    with StateDB(db_path) as state:
        for article_id in ("unassigned", "assigned", "filtered"):
            state.insert_article(_make(article_id), tmp_path / f"{article_id}.json")

        # Setup: one assigned to an event, one filtered, one untouched.
        state.upsert_event(
            {
                "event_id": "ev-1",
                "title": "Event",
                "category": "world",
                "thread": None,
                "status": "active",
                "created_at": "2026-05-24T16:30:00Z",
                "updated_at": "2026-05-24T16:30:00Z",
                "article_ids": ["assigned"],
                "article_count": 1,
                "confidence": 0.7,
                "newsworthiness": None,
                "llm_metadata": {},
            },
            tmp_path / "events" / "ev-1.json",
        )
        state.assign_articles_to_event(["assigned"], "ev-1")
        state.update_article_aggregation_status("filtered", status="filtered_low_impact", reason="below cutoff")

        # Assigned: must NOT be reset to pending.
        state.set_article_aggregation_pending_if_unassigned("assigned")
        row = state.conn.execute(
            "SELECT aggregation_status, event_id FROM articles WHERE article_id = 'assigned'"
        ).fetchone()
        assert row["aggregation_status"] == "assigned"
        assert row["event_id"] == "ev-1"

        # Filtered (event_id IS NULL): SHOULD be reset to pending and unfilter.
        state.set_article_aggregation_pending_if_unassigned("filtered")
        row = state.conn.execute(
            "SELECT aggregation_status, aggregation_reason, is_filtered FROM articles WHERE article_id = 'filtered'"
        ).fetchone()
        assert row["aggregation_status"] == "pending"
        assert row["aggregation_reason"] is None
        assert row["is_filtered"] == 0

        # Already-pending row: no-op. Plant a sentinel reason that the helper
        # would clear if it ran, then assert it survives the call.
        state.conn.execute("UPDATE articles SET aggregation_reason = 'sentinel' WHERE article_id = 'unassigned'")
        state.conn.commit()
        state.set_article_aggregation_pending_if_unassigned("unassigned")
        row = state.conn.execute(
            "SELECT aggregation_status, aggregation_reason FROM articles WHERE article_id = 'unassigned'"
        ).fetchone()
        assert row["aggregation_status"] == "pending"
        assert row["aggregation_reason"] == "sentinel"


def test_deduplication_review_cache_is_invalidated_by_event_updates(tmp_path: Path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    with StateDB(db_path) as state:
        state.record_deduplication_review(
            event_a="b",
            event_b="a",
            event_a_updated_at="2026-08-24T02:00:00Z",
            event_b_updated_at="2026-08-24T01:00:00Z",
            should_merge=False,
            confidence=0.9,
            rationale="different events",
            model="gemini-3.6-flash",
            prompt_version="deduplication-review-v1",
        )

        cached = state.get_cached_deduplication_review(
            event_a="a",
            event_b="b",
            event_a_updated_at="2026-08-24T01:00:00Z",
            event_b_updated_at="2026-08-24T02:00:00Z",
            prompt_version="deduplication-review-v1",
        )
        invalidated = state.get_cached_deduplication_review(
            event_a="a",
            event_b="b",
            event_a_updated_at="2026-08-24T03:00:00Z",
            event_b_updated_at="2026-08-24T02:00:00Z",
            prompt_version="deduplication-review-v1",
        )

        assert cached is not None
        assert cached["model"] == "gemini-3.6-flash"
        assert invalidated is None


def test_fail_stale_running_pipeline_runs(tmp_path: Path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    with StateDB(db_path) as state:
        state.start_run("old-run", "aggregation")
        assert state.fail_stale_running_pipeline_runs() == 1
        row = state.conn.execute(
            "SELECT status, finished_at, stats_json FROM pipeline_runs WHERE run_id = 'old-run'"
        ).fetchone()
        assert row["status"] == "failed"
        assert row["finished_at"] is not None
        assert json.loads(row["stats_json"]) == {"recovered_as_stale": True}
