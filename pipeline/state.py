from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from pipeline.paths import DB_PATH, PROJECT_ROOT, STATE_DIR
from pipeline.util import isoformat_z


def _relative_to_project(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


SCHEMA_VERSION = 7


# Each migration is the SQL needed to take the database from the previous
# version up to its own. Migrations are appended in order and run exactly once
# per database (recorded in `schema_version`). They must be kept immutable once
# released; later schema changes go in a new migration.
MIGRATIONS: tuple[tuple[int, str], ...] = (
    (
        1,
        """
        PRAGMA journal_mode=WAL;
        PRAGMA foreign_keys=ON;

        CREATE TABLE IF NOT EXISTS feeds (
          source_id TEXT PRIMARY KEY,
          source_name TEXT NOT NULL,
          feed_url TEXT NOT NULL,
          site_url TEXT,
          enabled INTEGER NOT NULL DEFAULT 1,
          default_category TEXT,
          config_json TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS feed_state (
          source_id TEXT PRIMARY KEY REFERENCES feeds(source_id) ON DELETE CASCADE,
          etag TEXT,
          last_modified TEXT,
          last_status INTEGER,
          last_fetched_at TEXT,
          consecutive_failures INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS articles (
          article_id TEXT PRIMARY KEY,
          source_id TEXT NOT NULL,
          source_name TEXT NOT NULL,
          url TEXT NOT NULL,
          canonical_url TEXT,
          guid TEXT,
          headline TEXT NOT NULL,
          summary TEXT,
          published_at TEXT,
          publish_date_estimated INTEGER NOT NULL DEFAULT 0,
          fetched_at TEXT NOT NULL,
          article_path TEXT NOT NULL,
          content_type TEXT NOT NULL DEFAULT 'unknown',
          language TEXT,
          event_id TEXT,
          collection_json TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_articles_event ON articles(event_id);
        CREATE INDEX IF NOT EXISTS idx_articles_source ON articles(source_id);
        CREATE INDEX IF NOT EXISTS idx_articles_published ON articles(published_at);

        CREATE TABLE IF NOT EXISTS article_fingerprints (
          article_id TEXT PRIMARY KEY REFERENCES articles(article_id) ON DELETE CASCADE,
          canonical_url_hash TEXT,
          headline_hash TEXT,
          summary_hash TEXT,
          content_hash TEXT
        );

        CREATE TABLE IF NOT EXISTS events (
          event_id TEXT PRIMARY KEY,
          title TEXT NOT NULL,
          category TEXT NOT NULL,
          thread TEXT,
          status TEXT NOT NULL DEFAULT 'active',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          event_path TEXT
        );

        CREATE TABLE IF NOT EXISTS pipeline_runs (
          run_id TEXT PRIMARY KEY,
          stage TEXT NOT NULL,
          status TEXT NOT NULL,
          started_at TEXT NOT NULL,
          finished_at TEXT,
          stats_json TEXT NOT NULL DEFAULT '{}'
        );

        CREATE TABLE IF NOT EXISTS item_errors (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          run_id TEXT,
          stage TEXT NOT NULL,
          item_type TEXT NOT NULL,
          item_id TEXT,
          source_id TEXT,
          error_type TEXT NOT NULL,
          error_message TEXT NOT NULL,
          occurred_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS llm_usage (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          run_id TEXT,
          stage TEXT NOT NULL,
          model TEXT NOT NULL,
          prompt_version TEXT,
          input_tokens INTEGER,
          output_tokens INTEGER,
          cost_usd REAL,
          occurred_at TEXT NOT NULL
        );
        """,
    ),
    (
        2,
        """
        ALTER TABLE events ADD COLUMN keywords_json TEXT NOT NULL DEFAULT '[]';
        ALTER TABLE events ADD COLUMN entities_json TEXT NOT NULL DEFAULT '[]';
        ALTER TABLE events ADD COLUMN article_count INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE events ADD COLUMN last_editorial_at TEXT;
        ALTER TABLE events ADD COLUMN confidence REAL;

        ALTER TABLE item_errors ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0;

        CREATE INDEX IF NOT EXISTS idx_articles_canonical_url ON articles(canonical_url);
        CREATE INDEX IF NOT EXISTS idx_articles_unassigned ON articles(event_id) WHERE event_id IS NULL;
        CREATE INDEX IF NOT EXISTS idx_events_status_updated ON events(status, updated_at);
        CREATE INDEX IF NOT EXISTS idx_item_errors_item ON item_errors(item_type, item_id, retry_count);
        """,
    ),
    (
        3,
        """
        CREATE TABLE IF NOT EXISTS aggregation_windows (
          window_start TEXT NOT NULL,
          window_end TEXT NOT NULL,
          status TEXT NOT NULL,
          run_id TEXT,
          article_count INTEGER NOT NULL DEFAULT 0,
          prompt_version TEXT,
          model TEXT,
          stats_json TEXT NOT NULL DEFAULT '{}',
          started_at TEXT,
          finished_at TEXT,
          updated_at TEXT NOT NULL,
          PRIMARY KEY (window_start, window_end)
        );

        CREATE INDEX IF NOT EXISTS idx_aggregation_windows_status_end
          ON aggregation_windows(status, window_end);
        """,
    ),
    (
        4,
        """
        ALTER TABLE events ADD COLUMN newsworthiness_global REAL;
        ALTER TABLE events ADD COLUMN newsworthiness_category REAL;
        ALTER TABLE events ADD COLUMN newsworthiness_json TEXT NOT NULL DEFAULT '{}';

        CREATE INDEX IF NOT EXISTS idx_events_newsworthiness_global
          ON events(newsworthiness_global);
        CREATE INDEX IF NOT EXISTS idx_events_newsworthiness_category
          ON events(category, newsworthiness_category);
        """,
    ),
    (
        5,
        """
        ALTER TABLE articles ADD COLUMN digest_status TEXT NOT NULL DEFAULT 'pending';
        ALTER TABLE articles ADD COLUMN digest_generated_at TEXT;
        ALTER TABLE articles ADD COLUMN digest_model TEXT;
        ALTER TABLE articles ADD COLUMN digest_prompt_version TEXT;
        ALTER TABLE articles ADD COLUMN digest_error TEXT;

        CREATE INDEX IF NOT EXISTS idx_articles_digest_status_published
          ON articles(digest_status, published_at);
        """,
    ),
    (
        6,
        """
        ALTER TABLE articles ADD COLUMN aggregation_status TEXT NOT NULL DEFAULT 'pending';
        ALTER TABLE articles ADD COLUMN aggregation_reason TEXT;

        CREATE INDEX IF NOT EXISTS idx_articles_aggregation_status_published
          ON articles(aggregation_status, published_at);
        """,
    ),
    (
        7,
        """
        ALTER TABLE articles ADD COLUMN is_filtered INTEGER NOT NULL DEFAULT 0;
        UPDATE articles SET is_filtered = 1 WHERE aggregation_status LIKE 'filtered_%';
        CREATE INDEX IF NOT EXISTS idx_articles_is_filtered_published ON articles(is_filtered, published_at);
        """,
    ),
)


class StateDB:
    def __init__(self, path: Path = DB_PATH) -> None:
        self.path = path
        self.conn = connect(path)

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> StateDB:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def sync_feeds(self, feeds: list[Any]) -> None:
        now = isoformat_z()
        with self.conn:
            for feed in feeds:
                self.conn.execute(
                    """
                    INSERT INTO feeds (
                      source_id, source_name, feed_url, site_url, enabled,
                      default_category, config_json, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(source_id) DO UPDATE SET
                      source_name=excluded.source_name,
                      feed_url=excluded.feed_url,
                      site_url=excluded.site_url,
                      enabled=excluded.enabled,
                      default_category=excluded.default_category,
                      config_json=excluded.config_json,
                      updated_at=excluded.updated_at
                    """,
                    (
                        feed.source_id,
                        feed.source_name,
                        feed.feed_url,
                        feed.site_url,
                        1 if feed.enabled else 0,
                        feed.default_category,
                        json.dumps(feed.__dict__, sort_keys=True),
                        now,
                    ),
                )
                self.conn.execute(
                    "INSERT OR IGNORE INTO feed_state(source_id) VALUES (?)",
                    (feed.source_id,),
                )

    def feed_headers(self, source_id: str) -> dict[str, str]:
        row = self.conn.execute(
            "SELECT etag, last_modified FROM feed_state WHERE source_id = ?",
            (source_id,),
        ).fetchone()
        headers: dict[str, str] = {}
        if row:
            if row["etag"]:
                headers["If-None-Match"] = row["etag"]
            if row["last_modified"]:
                headers["If-Modified-Since"] = row["last_modified"]
        return headers

    def update_feed_state(
        self,
        source_id: str,
        *,
        status: int | None,
        etag: str | None = None,
        last_modified: str | None = None,
        failed: bool = False,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO feed_state (
                  source_id, etag, last_modified, last_status, last_fetched_at,
                  consecutive_failures
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_id) DO UPDATE SET
                  etag=COALESCE(excluded.etag, feed_state.etag),
                  last_modified=COALESCE(excluded.last_modified, feed_state.last_modified),
                  last_status=excluded.last_status,
                  last_fetched_at=excluded.last_fetched_at,
                  consecutive_failures=CASE
                    WHEN ? THEN feed_state.consecutive_failures + 1
                    ELSE 0
                  END
                """,
                (
                    source_id,
                    etag,
                    last_modified,
                    status,
                    isoformat_z(),
                    1 if failed else 0,
                    1 if failed else 0,
                ),
            )

    def article_exists(self, article_id: str) -> bool:
        return (
            self.conn.execute(
                "SELECT 1 FROM articles WHERE article_id = ?",
                (article_id,),
            ).fetchone()
            is not None
        )

    def find_article_by_url_or_guid(
        self,
        url: str,
        canonical_url: str | None = None,
        guid: str | None = None,
        source_id: str | None = None,
    ) -> tuple[str, str] | None:
        conditions = ["url = ?"]
        params = [url]
        if canonical_url:
            conditions.extend(["canonical_url = ?", "url = ?", "canonical_url = ?"])
            params.extend([canonical_url, canonical_url, url])
        if guid and source_id:
            conditions.append("(source_id = ? AND guid = ?)")
            params.extend([source_id, guid])

        query = f"SELECT article_id, article_path FROM articles WHERE {' OR '.join(conditions)}"
        row = self.conn.execute(query, params).fetchone()
        if row:
            return row["article_id"], row["article_path"]
        return None

    def insert_article(self, article: dict[str, Any], article_path: Path) -> None:
        collection = article.get("collection", {})
        with self.conn:
            self.conn.execute(
                """
                INSERT OR IGNORE INTO articles (
                  article_id, source_id, source_name, url, canonical_url, guid,
                  headline, summary, published_at, publish_date_estimated,
                  fetched_at, article_path, content_type, language, collection_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    article["article_id"],
                    article["source_id"],
                    article["source_name"],
                    article["url"],
                    article.get("canonical_url"),
                    article.get("guid"),
                    article["headline"],
                    article.get("summary"),
                    article.get("published_at"),
                    1 if article.get("publish_date_estimated") else 0,
                    article["fetched_at"],
                    _relative_to_project(article_path),
                    article.get("content_type", "unknown"),
                    article.get("language"),
                    json.dumps(collection, sort_keys=True),
                ),
            )
            fp = article.get("fingerprints", {})
            self.conn.execute(
                """
                INSERT OR IGNORE INTO article_fingerprints (
                  article_id, canonical_url_hash, headline_hash, summary_hash, content_hash
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    article["article_id"],
                    fp.get("canonical_url_hash"),
                    fp.get("headline_hash"),
                    fp.get("summary_hash"),
                    fp.get("content_hash"),
                ),
            )

    def article_time_bounds(self, *, unassigned_only: bool = True) -> tuple[str, str] | None:
        where = "WHERE published_at IS NOT NULL"
        if unassigned_only:
            where += " AND event_id IS NULL AND is_filtered = 0"
        row = self.conn.execute(
            f"""
            SELECT MIN(published_at) AS min_published_at, MAX(published_at) AS max_published_at
            FROM articles
            {where}
            """
        ).fetchone()
        if not row or not row["min_published_at"] or not row["max_published_at"]:
            return None
        return row["min_published_at"], row["max_published_at"]

    def unassigned_article_published_times(self, range_start: str, range_end: str) -> list[str]:
        rows = self.conn.execute(
            """
            SELECT published_at
            FROM articles
            WHERE event_id IS NULL
              AND is_filtered = 0
              AND published_at >= ?
              AND published_at < ?
            ORDER BY published_at
            """,
            (range_start, range_end),
        ).fetchall()
        return [row["published_at"] for row in rows if row["published_at"]]

    def event_exists(self, event_id: str) -> bool:
        return (
            self.conn.execute(
                "SELECT 1 FROM events WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            is not None
        )

    def upsert_event(self, event: dict[str, Any], event_path: Path) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO events (
                  event_id, title, category, thread, status, created_at, updated_at,
                  event_path, keywords_json, entities_json, article_count, confidence,
                  newsworthiness_global, newsworthiness_category, newsworthiness_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_id) DO UPDATE SET
                  title=excluded.title,
                  category=excluded.category,
                  thread=excluded.thread,
                  status=excluded.status,
                  updated_at=excluded.updated_at,
                  event_path=excluded.event_path,
                  keywords_json=excluded.keywords_json,
                  entities_json=excluded.entities_json,
                  article_count=excluded.article_count,
                  confidence=excluded.confidence,
                  newsworthiness_global=excluded.newsworthiness_global,
                  newsworthiness_category=excluded.newsworthiness_category,
                  newsworthiness_json=excluded.newsworthiness_json
                """,
                (
                    event["event_id"],
                    event["title"],
                    event["category"],
                    event.get("thread"),
                    event.get("status", "active"),
                    event["created_at"],
                    event["updated_at"],
                    _relative_to_project(event_path),
                    json.dumps(event.get("keywords", []), sort_keys=True),
                    json.dumps(event.get("entities", []), sort_keys=True),
                    int(event.get("article_count", 0)),
                    event.get("confidence"),
                    (event.get("newsworthiness") or {}).get("global"),
                    (event.get("newsworthiness") or {}).get("category"),
                    json.dumps(event.get("newsworthiness", {}), sort_keys=True),
                ),
            )

    def assign_articles_to_event(self, article_ids: list[str], event_id: str) -> int:
        if not article_ids:
            return 0
        with self.conn:
            cursor = self.conn.executemany(
                """
                UPDATE articles
                SET event_id = ?,
                    aggregation_status = 'assigned',
                    aggregation_reason = NULL,
                    is_filtered = 0
                WHERE article_id = ?
                """,
                [(event_id, article_id) for article_id in article_ids],
            )
        return cursor.rowcount

    def update_article_content_type(self, article_id: str, content_type: str) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE articles SET content_type = ? WHERE article_id = ?",
                (content_type, article_id),
            )

    def update_article_aggregation_status(
        self,
        article_id: str,
        *,
        status: str,
        reason: str | None = None,
    ) -> None:
        is_filtered = 1 if status.startswith("filtered_") else 0
        with self.conn:
            self.conn.execute(
                """
                UPDATE articles
                SET aggregation_status = ?,
                    aggregation_reason = ?,
                    is_filtered = ?
                WHERE article_id = ?
                  AND event_id IS NULL
                """,
                (status, reason, is_filtered, article_id),
            )

    # Idempotent helper used after digest completion and on every sliding-window
    # eligibility pass. Guarded by event_id IS NULL so it does not stomp an
    # already-assigned article back to pending, and guarded by current status so
    # repeated window passes do not generate redundant writes.
    def set_article_aggregation_pending_if_unassigned(self, article_id: str) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE articles
                SET aggregation_status = 'pending',
                    aggregation_reason = NULL,
                    is_filtered = 0
                WHERE article_id = ?
                  AND event_id IS NULL
                  AND (aggregation_status IS NULL OR aggregation_status != 'pending')
                """,
                (article_id,),
            )

    def update_article_digest_status(
        self,
        article_id: str,
        *,
        status: str,
        generated_at: str | None = None,
        model: str | None = None,
        prompt_version: str | None = None,
        error: str | None = None,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE articles
                SET digest_status = ?,
                    digest_generated_at = ?,
                    digest_model = ?,
                    digest_prompt_version = ?,
                    digest_error = ?
                WHERE article_id = ?
                """,
                (status, generated_at, model, prompt_version, error, article_id),
            )

    def delete_event(self, event_id: str) -> None:
        with self.conn:
            self.conn.execute("DELETE FROM events WHERE event_id = ?", (event_id,))

    def reassign_event_articles(self, old_event_id: str, new_event_id: str) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE articles SET event_id = ? WHERE event_id = ?",
                (new_event_id, old_event_id),
            )

    def merge_events_into(self, loser_event_ids: Sequence[str], winner_event_id: str) -> None:
        if not loser_event_ids:
            return
        with self.conn:
            for loser_id in loser_event_ids:
                if loser_id == winner_event_id:
                    continue
                self.conn.execute(
                    "UPDATE articles SET event_id = ? WHERE event_id = ?",
                    (winner_event_id, loser_id),
                )
                self.conn.execute("DELETE FROM events WHERE event_id = ?", (loser_id,))

    def record_llm_usage(
        self,
        run_id: str,
        stage: str,
        model: str,
        prompt_version: str | None,
        input_tokens: int | None,
        output_tokens: int | None,
        cost_usd: float | None = None,
    ) -> None:
        now = isoformat_z()
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO llm_usage (
                  run_id, stage, model, prompt_version, input_tokens, output_tokens, cost_usd, occurred_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (run_id, stage, model, prompt_version, input_tokens, output_tokens, cost_usd, now),
            )

    def start_run(self, run_id: str, stage: str) -> None:
        self.conn.execute(
            "INSERT INTO pipeline_runs(run_id, stage, status, started_at) VALUES (?, ?, 'running', ?)",
            (run_id, stage, isoformat_z()),
        )
        self.conn.commit()

    def finish_run(self, run_id: str, status: str, stats: dict[str, Any]) -> None:
        self.conn.execute(
            "UPDATE pipeline_runs SET status = ?, finished_at = ?, stats_json = ? WHERE run_id = ?",
            (status, isoformat_z(), json.dumps(stats, sort_keys=True), run_id),
        )
        self.conn.commit()

    def record_error(
        self,
        run_id: str,
        stage: str,
        item_type: str,
        item_id: str | None,
        source_id: str | None,
        error: Exception | str,
    ) -> None:
        message = str(error)
        self.conn.execute(
            """
            INSERT INTO item_errors (
              run_id, stage, item_type, item_id, source_id, error_type, error_message, occurred_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                stage,
                item_type,
                item_id,
                source_id,
                type(error).__name__ if isinstance(error, Exception) else "Error",
                message[:2000],
                isoformat_z(),
            ),
        )
        self.conn.commit()

    def fail_stale_running_aggregation_windows(self) -> int:
        now = isoformat_z()
        with self.conn:
            cursor = self.conn.execute(
                """
                UPDATE aggregation_windows
                SET status = 'failed',
                    stats_json = COALESCE(stats_json, '{}'),
                    finished_at = ?,
                    updated_at = ?
                WHERE status = 'running'
                """,
                (now, now),
            )
            return cursor.rowcount or 0

    def aggregation_window_status(self, window_start: str, window_end: str) -> str | None:
        row = self.conn.execute(
            """
            SELECT status
            FROM aggregation_windows
            WHERE window_start = ? AND window_end = ?
            """,
            (window_start, window_end),
        ).fetchone()
        return row["status"] if row else None

    def unassigned_article_count_in_window(self, window_start: str, window_end: str) -> int:
        row = self.conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM articles
            WHERE event_id IS NULL
              AND is_filtered = 0
              AND published_at >= ?
              AND published_at < ?
            """,
            (window_start, window_end),
        ).fetchone()
        return int(row["count"]) if row else 0

    def latest_completed_aggregation_window(self) -> tuple[str, str] | None:
        row = self.conn.execute(
            """
            SELECT window_start, window_end
            FROM aggregation_windows
            WHERE status = 'completed'
            ORDER BY window_end DESC, window_start DESC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            return None
        return row["window_start"], row["window_end"]

    def start_aggregation_window(
        self,
        *,
        window_start: str,
        window_end: str,
        run_id: str,
        prompt_version: str,
        model: str,
    ) -> None:
        now = isoformat_z()
        self.conn.execute(
            """
            INSERT INTO aggregation_windows (
              window_start, window_end, status, run_id, prompt_version, model,
              started_at, updated_at
            )
            VALUES (?, ?, 'running', ?, ?, ?, ?, ?)
            ON CONFLICT(window_start, window_end) DO UPDATE SET
              status='running',
              run_id=excluded.run_id,
              prompt_version=excluded.prompt_version,
              model=excluded.model,
              started_at=excluded.started_at,
              updated_at=excluded.updated_at
            """,
            (window_start, window_end, run_id, prompt_version, model, now, now),
        )
        self.conn.commit()

    def finish_aggregation_window(
        self,
        *,
        window_start: str,
        window_end: str,
        status: str,
        article_count: int,
        stats: dict[str, Any],
    ) -> None:
        now = isoformat_z()
        self.conn.execute(
            """
            UPDATE aggregation_windows
            SET status = ?,
                article_count = ?,
                stats_json = ?,
                finished_at = ?,
                updated_at = ?
            WHERE window_start = ? AND window_end = ?
            """,
            (status, article_count, json.dumps(stats, sort_keys=True), now, now, window_start, window_end),
        )
        self.conn.commit()


def connect(path: Path = DB_PATH) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


_ADD_COLUMN_RE = re.compile(
    r"^\s*ALTER\s+TABLE\s+(\w+)\s+ADD\s+COLUMN\s+(\w+)\b",
    re.IGNORECASE,
)


def _existing_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {row[1] for row in rows}


def _apply_migration_script(conn: sqlite3.Connection, sql: str) -> None:
    statements = [stmt.strip() for stmt in sql.split(";") if stmt.strip()]
    for statement in statements:
        match = _ADD_COLUMN_RE.match(statement)
        if match:
            table, column = match.group(1), match.group(2)
            if column in _existing_columns(conn, table):
                continue
        conn.execute(statement)


def migrate(path: Path = DB_PATH) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    conn = connect(path)
    try:
        conn.executescript(
            "CREATE TABLE IF NOT EXISTS schema_version ( version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);"
        )
        row = conn.execute("SELECT COALESCE(MAX(version), 0) FROM schema_version").fetchone()
        current = int(row[0]) if row else 0
        for target, sql in MIGRATIONS:
            if target <= current:
                continue
            _apply_migration_script(conn, sql)
            conn.execute(
                "INSERT INTO schema_version(version, applied_at) VALUES (?, ?)",
                (target, isoformat_z()),
            )
            conn.commit()
    finally:
        conn.close()
