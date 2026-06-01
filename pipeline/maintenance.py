from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from contextlib import nullcontext
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from typing import Any

from pipeline.aggregate import (
    ArticleForAggregation,
    _build_event_payload,
    _event_path_from_state_row,
    _load_digest_fields,
    _read_event,
)
from pipeline.config import load_feeds, load_pipeline_config
from pipeline.lock import PipelineLock
from pipeline.paths import DB_PATH, EVENT_DIR, LOCK_PATH, PROJECT_ROOT
from pipeline.state import StateDB
from pipeline.util import atomic_write_json, isoformat_z, utc_now

MAINTENANCE_STAGE = "maintenance"
EXPIRED_AGGREGATION_STATUS = "filtered_expired"
CONTENT_EXCERPT_CHARS = 1000


def maintenance_once(
    *,
    now: datetime | None = None,
    dry_run: bool = False,
    progress: Callable[[str], None] | None = None,
    acquire_lock: bool = True,
    db_path: Path = DB_PATH,
    event_dir: Path = EVENT_DIR,
) -> dict[str, Any]:
    config = load_pipeline_config()
    stale_threshold_hours = int(config.aggregation.get("stale_threshold_hours", 48))
    archived_event_days = int(config.retention.get("archived_event_days", 30))
    staging_article_days = int(config.retention.get("staging_article_days", 3))
    lock_timeout = timedelta(minutes=int(config.pipeline.get("watchdog_timeout_minutes", 30)))
    run_id = f"maintenance-{uuid.uuid4().hex}"
    now_dt = (now or utc_now()).astimezone(UTC)

    stats: dict[str, Any] = {
        "dry_run": dry_run,
        "events_marked_stale": 0,
        "events_archived": 0,
        "expired_articles": 0,
        "events_reconciled": 0,
        "events_deleted": 0,
        "article_json_compacted": 0,
        "article_json_already_compacted": 0,
        "errors": 0,
    }

    state = StateDB(db_path)
    try:
        lock_context = PipelineLock(LOCK_PATH, lock_timeout, run_id=run_id) if acquire_lock else nullcontext()
        with lock_context:
            if progress:
                progress("maintenance: starting retention and artifact reconciliation")
            started_run = False
            if not dry_run:
                state.start_run(run_id, MAINTENANCE_STAGE)
                started_run = True
            status = "success"
            try:
                feeds_by_source = {feed.source_id: feed for feed in load_feeds(enabled_only=False)}
                lifecycle_stats = _apply_event_lifecycle(
                    state,
                    now=now_dt,
                    stale_threshold_hours=stale_threshold_hours,
                    archived_event_days=archived_event_days,
                    dry_run=dry_run,
                    event_dir=event_dir,
                )
                stats.update(lifecycle_stats)
                if progress:
                    progress(
                        "maintenance: lifecycle "
                        f"stale={stats['events_marked_stale']} archived={stats['events_archived']}"
                    )

                expired = _expire_old_pending_articles(state, now=now_dt, dry_run=dry_run)
                stats["expired_articles"] = expired
                if progress and expired:
                    progress(f"maintenance: expired {expired} old unassigned article(s)")

                reconcile_stats = _reconcile_event_artifacts(
                    state,
                    feeds_by_source=feeds_by_source,
                    dry_run=dry_run,
                    event_dir=event_dir,
                    progress=progress,
                    run_id=None if dry_run else run_id,
                )
                stats["events_reconciled"] = reconcile_stats["events_reconciled"]
                stats["events_deleted"] = reconcile_stats["events_deleted"]

                compact_stats = _compact_old_article_json(
                    state,
                    now=now_dt,
                    staging_article_days=staging_article_days,
                    dry_run=dry_run,
                    run_id=None if dry_run else run_id,
                )
                stats["article_json_compacted"] = compact_stats["article_json_compacted"]
                stats["article_json_already_compacted"] = compact_stats["article_json_already_compacted"]
                stats["errors"] += reconcile_stats["errors"] + compact_stats["errors"]
                if progress:
                    progress(
                        "maintenance: reconciled "
                        f"{stats['events_reconciled']} event(s), deleted {stats['events_deleted']} empty event(s), "
                        f"compacted {stats['article_json_compacted']} article JSON file(s)"
                    )
            except Exception:
                status = "failed"
                raise
            finally:
                if started_run:
                    state.finish_run(run_id, status, stats)
    finally:
        state.close()

    return stats


def _apply_event_lifecycle(
    state: StateDB,
    *,
    now: datetime,
    stale_threshold_hours: int,
    archived_event_days: int,
    dry_run: bool,
    event_dir: Path,
) -> dict[str, int]:
    stale_before = _format(now - timedelta(hours=stale_threshold_hours))
    archive_before = _format(now - timedelta(days=archived_event_days))
    rows = state.conn.execute(
        """
        SELECT event_id, status, updated_at, event_path
        FROM events
        WHERE status IN ('active', 'stale')
          AND updated_at < ?
        ORDER BY updated_at, event_id
        """,
        (stale_before,),
    ).fetchall()

    updates: list[tuple[str, str, str | None]] = []
    marked_stale = 0
    archived = 0
    for row in rows:
        next_status = row["status"]
        if row["updated_at"] < archive_before:
            next_status = "archived"
        elif row["status"] == "active":
            next_status = "stale"
        if next_status == row["status"]:
            continue
        updates.append((row["event_id"], next_status, row["event_path"]))
        if next_status == "stale":
            marked_stale += 1
        elif next_status == "archived":
            archived += 1

    if dry_run:
        return {"events_marked_stale": marked_stale, "events_archived": archived}

    with state.conn:
        for event_id, next_status, _event_path in updates:
            state.conn.execute("UPDATE events SET status = ? WHERE event_id = ?", (next_status, event_id))

    for event_id, next_status, event_path in updates:
        path = _resolve_event_path(event_id, event_path, event_dir)
        payload = _read_event(path)
        if not payload:
            continue
        payload["status"] = next_status
        atomic_write_json(path, payload)

    return {"events_marked_stale": marked_stale, "events_archived": archived}


def _expire_old_pending_articles(state: StateDB, *, now: datetime, dry_run: bool) -> int:
    horizon_start = _default_processing_horizon_start(now)
    horizon = _format(horizon_start)
    row = state.conn.execute(
        """
        SELECT COUNT(*) AS count
        FROM articles
        WHERE event_id IS NULL
          AND is_filtered = 0
          AND published_at IS NOT NULL
          AND published_at < ?
        """,
        (horizon,),
    ).fetchone()
    count = int(row["count"]) if row else 0
    if dry_run or count == 0:
        return count

    with state.conn:
        state.conn.execute(
            """
            UPDATE articles
            SET aggregation_status = ?,
                aggregation_reason = ?,
                is_filtered = 1
            WHERE event_id IS NULL
              AND is_filtered = 0
              AND published_at IS NOT NULL
              AND published_at < ?
            """,
            (
                EXPIRED_AGGREGATION_STATUS,
                f"outside aggregation horizon before {horizon}",
                horizon,
            ),
        )
    return count


def _reconcile_event_artifacts(
    state: StateDB,
    *,
    feeds_by_source: dict[str, Any],
    dry_run: bool,
    event_dir: Path,
    progress: Callable[[str], None] | None,
    run_id: str | None,
) -> dict[str, int]:
    rows = state.conn.execute(
        """
        SELECT event_id, title, category, thread, status, created_at, updated_at, event_path,
               keywords_json, entities_json, article_count, confidence, newsworthiness_json
        FROM events
        WHERE status IN ('active', 'stale')
        ORDER BY updated_at, event_id
        """
    ).fetchall()

    reconciled = 0
    deleted = 0
    errors = 0
    for row in rows:
        event_id = row["event_id"]
        try:
            article_rows = state.conn.execute(
                """
                SELECT article_id, source_id, source_name, headline, summary, published_at, article_path, event_id
                FROM articles
                WHERE event_id = ?
                  AND is_filtered = 0
                ORDER BY published_at, article_id
                """,
                (event_id,),
            ).fetchall()
            path = _resolve_event_path(event_id, row["event_path"], event_dir)
            if not article_rows:
                deleted += 1
                if dry_run:
                    continue
                with state.conn:
                    state.conn.execute("DELETE FROM events WHERE event_id = ?", (event_id,))
                path.unlink(missing_ok=True)
                continue

            articles = [_article_from_row(article_row) for article_row in article_rows]
            exact_article_ids = sorted(article.article_id for article in articles)
            existing_payload = _existing_payload_from_row(dict(row), path)
            ids_changed = sorted(existing_payload.get("article_ids", [])) != exact_article_ids
            existing_for_build = dict(existing_payload)
            existing_for_build["article_ids"] = exact_article_ids
            if ids_changed:
                existing_for_build.pop("newsworthiness", None)

            rebuilt = _build_event_payload(
                event_id=event_id,
                event_path=path,
                articles=articles,
                existing=existing_for_build,
                feeds_by_source=feeds_by_source,
            )
            rebuilt["title"] = row["title"]
            rebuilt["category"] = row["category"]
            rebuilt["thread"] = row["thread"]
            rebuilt["created_at"] = row["created_at"]
            rebuilt["updated_at"] = row["updated_at"]
            rebuilt["status"] = row["status"]
            rebuilt["article_ids"] = exact_article_ids
            rebuilt["article_count"] = len(exact_article_ids)
            rebuilt["event_path"] = _relative_event_path(path)

            if _event_payload_needs_write(existing_payload, rebuilt, path):
                reconciled += 1
                if not dry_run:
                    atomic_write_json(path, rebuilt)
                    state.upsert_event(rebuilt, path)
        except Exception as exc:
            errors += 1
            if progress:
                progress(f"maintenance: failed to reconcile event {event_id}: {exc}")
            if run_id:
                state.record_error(run_id, MAINTENANCE_STAGE, "event", event_id, None, exc)

    return {"events_reconciled": reconciled, "events_deleted": deleted, "errors": errors}


def _compact_old_article_json(
    state: StateDB,
    *,
    now: datetime,
    staging_article_days: int,
    dry_run: bool,
    run_id: str | None,
) -> dict[str, int]:
    compact_before = _format(now - timedelta(days=staging_article_days))
    rows = state.conn.execute(
        """
        SELECT a.article_id, a.article_path
        FROM articles a
        LEFT JOIN events e ON e.event_id = a.event_id
        WHERE COALESCE(a.published_at, a.fetched_at) < ?
          AND (
            a.is_filtered = 1
            OR e.status = 'archived'
          )
        ORDER BY COALESCE(a.published_at, a.fetched_at), a.article_id
        """,
        (compact_before,),
    ).fetchall()

    compacted = 0
    already_compacted = 0
    errors = 0
    for row in rows:
        path = _resolve_project_path(row["article_path"])
        try:
            if not path.exists():
                continue
            with path.open("r", encoding="utf-8") as f:
                payload = json.load(f)
            content = payload.get("content_text")
            if not isinstance(content, str):
                if payload.get("content_text_compacted_at"):
                    already_compacted += 1
                continue
            compacted += 1
            if dry_run:
                continue
            payload.setdefault("content_excerpt", _compact_excerpt(content))
            payload["content_text_compacted_at"] = isoformat_z(now)
            payload.pop("content_text", None)
            atomic_write_json(path, payload)
        except Exception as exc:
            errors += 1
            if run_id:
                state.record_error(run_id, MAINTENANCE_STAGE, "article", row["article_id"], None, exc)

    return {
        "article_json_compacted": compacted,
        "article_json_already_compacted": already_compacted,
        "errors": errors,
    }


def _article_from_row(row: Any) -> ArticleForAggregation:
    return ArticleForAggregation(
        article_id=row["article_id"],
        source_id=row["source_id"],
        source_name=row["source_name"],
        headline=row["headline"],
        summary=row["summary"],
        published_at=row["published_at"],
        article_path=row["article_path"],
        event_id=row["event_id"],
        **_load_digest_fields(row["article_path"]),
    )


def _existing_payload_from_row(row: dict[str, Any], path: Path) -> dict[str, Any]:
    payload = _read_event(path) or {}
    payload["_stored_event_path"] = payload.get("event_path")
    payload["_stored_status"] = payload.get("status")
    payload.update({
        "event_id": row["event_id"],
        "title": row["title"],
        "category": row["category"],
        "thread": row["thread"],
        "status": row["status"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "article_count": row["article_count"],
        "confidence": row["confidence"],
        "event_path": _relative_event_path(path),
    })
    payload.setdefault("keywords", _loads_json_list(row.get("keywords_json")))
    payload.setdefault("entities", _loads_json_list(row.get("entities_json")))
    newsworthiness = _loads_json_object(row.get("newsworthiness_json"))
    if newsworthiness:
        payload.setdefault("newsworthiness", newsworthiness)
    return payload


def _event_payload_needs_write(existing: dict[str, Any], rebuilt: dict[str, Any], path: Path) -> bool:
    if not path.exists():
        return True
    if existing.get("_stored_event_path", existing.get("event_path")) != rebuilt.get("event_path"):
        return True
    if existing.get("_stored_status", existing.get("status")) != rebuilt.get("status"):
        return True
    keys = (
        "article_ids",
        "article_count",
        "keywords",
        "newsworthiness",
    )
    return any(existing.get(key) != rebuilt.get(key) for key in keys)


def _resolve_event_path(event_id: str, event_path: str | None, event_dir: Path) -> Path:
    if event_path:
        path = Path(event_path)
        if path.is_absolute():
            return path
        return PROJECT_ROOT / path
    if event_dir == EVENT_DIR:
        return _event_path_from_state_row(event_id, None)
    return event_dir / f"{event_id}.json"


def _resolve_project_path(path: str) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return PROJECT_ROOT / candidate


def _relative_event_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


def _loads_json_list(value: Any) -> list[Any]:
    try:
        parsed = json.loads(value or "[]")
    except (TypeError, ValueError):
        return []
    return parsed if isinstance(parsed, list) else []


def _loads_json_object(value: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _compact_excerpt(content: str) -> str:
    excerpt = " ".join(content.split())
    if len(excerpt) <= CONTENT_EXCERPT_CHARS:
        return excerpt
    return excerpt[:CONTENT_EXCERPT_CHARS].rsplit(" ", 1)[0]


def _default_processing_horizon_start(now: datetime) -> datetime:
    today_start = datetime.combine(now.astimezone(UTC).date(), time.min, tzinfo=UTC)
    return today_start - timedelta(days=1)


def _format(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
