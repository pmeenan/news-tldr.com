"""Rolling, static research packets for external briefing experiments."""
from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from pipeline.config import load_pipeline_config, load_source_policy
from pipeline.lock import LockError, PipelineLock, lock_holder
from pipeline.paths import ACTIVE_STORIES_PATH, DB_PATH, LOCK_PATH, PROJECT_ROOT, STORY_DIR
from pipeline.present import _safe_publish_target, _write_public_bytes
from pipeline.sources import publisher_id
from pipeline.util import isoformat_z, sanitize_id, utc_now

BRIEF_VERSION = "brief-v1"
BRIEF_WINDOW_HOURS = 12
# Public content only: never copy collection config, local paths, or error strings.
ARTICLE_FIELDS = (
    "article_id", "source_id", "source_name", "url", "canonical_url", "headline",
    "summary", "authors", "published_at", "fetched_at", "publish_date_estimated",
    "content_text", "content_excerpt", "content_text_compacted_at", "content_type",
    "language", "tags", "paywall", "llm_digest",
)


def _timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def build_brief(
    *, now: datetime, db_path: Path = DB_PATH,
    active_path: Path = ACTIVE_STORIES_PATH, story_dir: Path = STORY_DIR,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    """Read under the caller's pipeline lock; omit globally filtered articles."""
    if now.tzinfo is None:
        raise ValueError("brief time must be timezone-aware")
    end = now.astimezone(UTC)
    start = end - timedelta(hours=BRIEF_WINDOW_HOURS)
    edition_id = isoformat_z(end)
    index = json.loads(active_path.read_text(encoding="utf-8"))
    with sqlite3.connect(f"{db_path.resolve().as_uri()}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM articles WHERE is_filtered = 0 AND julianday(fetched_at) <= julianday(?)",
            (isoformat_z(end),),
        ).fetchall()
        latest_collection = conn.execute(
            "SELECT started_at, finished_at, status FROM pipeline_runs "
            "WHERE stage = 'collection' ORDER BY started_at DESC LIMIT 1"
        ).fetchone()

    window_ids = set()
    event_ids = set()
    for row in rows:
        published = _timestamp(row["published_at"])
        fetched = _timestamp(row["fetched_at"])
        if (published and start < published <= end) or (fetched and start < fetched <= end):
            window_ids.add(row["article_id"])
            if row["event_id"]:
                event_ids.add(row["event_id"])

    stories = []
    eligible_rows = {row["article_id"]: dict(row) for row in rows}
    eligible_ids = set(eligible_rows)
    for rank in index["stories"]:
        story_id = rank["story_id"]
        if sanitize_id(story_id) != story_id:
            raise ValueError("invalid story ID in briefing index")
        story = json.loads((story_dir / f"{story_id}.json").read_text(encoding="utf-8"))
        revision_at = _timestamp(story.get("revision_at") or story.get("created_at"))
        if story.get("event_id", story_id) not in event_ids and not (revision_at and start < revision_at <= end):
            continue
        # A late bootstrap/retry may encounter a summary already regenerated
        # from post-cutoff or newly filtered reports. Export the eligible raw
        # reports, but never label that summary as part of this edition.
        if any(source["article_id"] not in eligible_ids for source in story.get("sources", [])):
            continue
        publishers = {publisher_id({**source, **eligible_rows[source["article_id"]]})
                      for source in story.get("sources", [])}
        publishers.discard("unknown")
        if len(publishers) < 2:
            continue
        stories.append({
            "publisher_count": len(publishers), "publisher_ids": sorted(publishers),
            "ranking": rank,
            "story": {key: value for key, value in story.items() if not key.startswith("_")},
            "article_ids": [],
        })

    event_ids = {item["story"].get("event_id", item["story"]["story_id"]) for item in stories}
    articles = []
    event_articles: dict[str, list[str]] = {}
    for row in rows:
        if row["event_id"] not in event_ids:
            continue
        path = Path(row["article_path"])
        if not path.is_absolute():
            path = project_root / path
        raw = json.loads(path.read_text(encoding="utf-8"))
        article = {key: raw[key] for key in ARTICLE_FIELDS if key in raw}
        article.update({
            "article_id": row["article_id"], "event_id": row["event_id"],
            "in_window": row["article_id"] in window_ids,
            "digest_status": row["digest_status"], "aggregation_status": row["aggregation_status"],
            "full_text_available": bool(raw.get("content_text")),
        })
        articles.append(article)
        event_articles.setdefault(row["event_id"], []).append(row["article_id"])
    allowed_ids = {article["article_id"] for article in articles}
    for item in stories:
        story = item["story"]
        item["article_ids"] = sorted(event_articles.get(story.get("event_id", story["story_id"]), []))
        # Source lists can predate a filtering decision; do not export excluded reports.
        story["sources"] = [source for source in story.get("sources", []) if source["article_id"] in allowed_ids]
    stories.sort(key=lambda item: (-float(item["ranking"].get("homepage_rank_score", 0)), item["story"]["story_id"]))
    articles.sort(key=lambda article: article["article_id"])
    policy = load_source_policy()
    source_ids = sorted({article["source_id"] for article in articles})
    return {
        "version": BRIEF_VERSION, "snapshot_id": edition_id, "timezone": "UTC", "window_hours": BRIEF_WINDOW_HOURS,
        "window_start": isoformat_z(start), "window_end": isoformat_z(end),
        "generated_at": isoformat_z(now), "minimum_publishers": 2,
        "selection": "(window_start, window_end]: published or first fetched in window, plus story revisions; "
                     "older articles on selected events are context (in_window=false).",
        "snapshot_note": "Article arrival cutoff is fixed; processing and rankings reflect generated_at. "
                         "Only stories citing two or more publishers qualify. Additional attached reports may "
                         "not yet be reflected in the summary. Full text is the available extraction.",
        "active_index_generated_at": index.get("generated_at"),
        "latest_collection": dict(latest_collection) if latest_collection else None,
        "latest_included_fetch_at": max((article["fetched_at"] for article in articles), default=None),
        "story_count": len(stories), "article_count": len(articles),
        "window_article_count": sum(article["in_window"] for article in articles),
        "sources": [{"source_id": source_id, **{key: policy.get(source_id, {}).get(key) for key in (
            "publisher_id", "source_name", "reliability", "bias",
        )}} for source_id in source_ids],
        "stories": stories, "articles": articles,
    }


def brief_once(
    *, now: datetime | None = None, publish_dir: Path | None = None,
    output: Path | None = None, progress: Callable[[str], None] | None = None,
    db_path: Path = DB_PATH, active_path: Path = ACTIVE_STORIES_PATH,
    story_dir: Path = STORY_DIR, lock_path: Path = LOCK_PATH, project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    """Refresh after a job; preview output never changes the public packet.

    The standalone file is deliberately not owned by presentation's manifest.
    """
    now = now or utc_now()
    if output is not None:
        target = output
    else:
        root = publish_dir or Path(load_pipeline_config().presentation["publish_dir"])
        if not root.is_absolute() or root.is_symlink() or root.resolve() in {Path("/"), Path.home()}:
            raise ValueError("unsafe briefing publish directory")
        target = _safe_publish_target(root.resolve(), "api/brief.json")
    if lock_holder(lock_path):
        return {"published": False, "reason": "pipeline_busy"}
    try:
        with PipelineLock(lock_path, timedelta(days=365), run_id="brief"):
            if progress:
                progress("brief: building rolling 12-hour packet")
            payload = build_brief(now=now, db_path=db_path, active_path=active_path,
                                  story_dir=story_dir, project_root=project_root)
            _write_public_bytes(target, (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode())
            if progress:
                progress(f"brief: wrote {payload['story_count']} stories and {payload['article_count']} articles")
            return {"published": output is None, "output": str(target), "snapshot_id": isoformat_z(now),
                    "story_count": payload["story_count"], "article_count": payload["article_count"]}
    except LockError:
        return {"published": False, "reason": "pipeline_busy"}
