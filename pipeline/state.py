from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from pipeline.paths import DB_PATH, PROJECT_ROOT, STATE_DIR
from pipeline.util import isoformat_z


def _relative_to_project(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


SCHEMA_VERSION = 2


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


def connect(path: Path = DB_PATH) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def migrate(path: Path = DB_PATH) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    conn = connect(path)
    try:
        conn.executescript(
            "CREATE TABLE IF NOT EXISTS schema_version ("
            " version INTEGER PRIMARY KEY,"
            " applied_at TEXT NOT NULL"
            ");"
        )
        row = conn.execute("SELECT COALESCE(MAX(version), 0) FROM schema_version").fetchone()
        current = int(row[0]) if row else 0
        for target, sql in MIGRATIONS:
            if target <= current:
                continue
            conn.executescript(sql)
            conn.execute(
                "INSERT INTO schema_version(version, applied_at) VALUES (?, ?)",
                (target, isoformat_z()),
            )
            conn.commit()
    finally:
        conn.close()
