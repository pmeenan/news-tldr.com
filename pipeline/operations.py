from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from pipeline.config import load_feeds, load_pipeline_config
from pipeline.paths import (
    ACTIVE_STORIES_PATH,
    ARTICLE_DIR,
    CONFIG_DIR,
    DB_PATH,
    DIST_DIR,
    EVENT_DIR,
    HEALTH_PATH,
    STORY_DIR,
)
from pipeline.state import StateDB
from pipeline.util import atomic_write_json, isoformat_z, sanitize_id, utc_now

VALIDATION_VERSION = "artifact-validation-v1"
HEALTH_VERSION = "operations-health-v1"
REQUIRED_PIPELINE_STAGES = (
    "maintenance",
    "collection",
    "article_digest",
    "aggregation",
    "editorial",
    "presentation",
)


def preflight_report(
    *,
    db_path: Path = DB_PATH,
    progress: Callable[[str], None] | None = None,
    validation_kwargs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Plan a complete run without network, LLM, database, artifact, or publish mutations."""
    from pipeline.digest import ARTICLE_DIGEST_PROMPT_VERSION
    from pipeline.maintenance import maintenance_once

    config = load_pipeline_config()
    reference = utc_now()
    today_start = datetime.combine(reference.date(), datetime.min.time(), tzinfo=UTC)
    lookback_days = max(1, int(config.retention.get("staging_article_days", 1)))
    range_start = isoformat_z(today_start - timedelta(days=lookback_days))
    if progress:
        progress("preflight: previewing maintenance")
    maintenance = maintenance_once(dry_run=True, progress=progress, acquire_lock=False)
    with StateDB(db_path) as state:
        digest_candidates = state.conn.execute(
            """
            SELECT COUNT(*) FROM articles
            WHERE published_at >= ?
              AND is_filtered = 0
              AND (
                digest_status NOT IN ('completed', 'skipped')
                OR digest_prompt_version IS NULL
                OR digest_prompt_version != ?
              )
            """,
            (range_start, ARTICLE_DIGEST_PROMPT_VERSION),
        ).fetchone()[0]
        aggregation_candidates = state.conn.execute(
            """
            SELECT COUNT(*) FROM articles
            WHERE published_at >= ?
              AND is_filtered = 0
              AND event_id IS NULL
              AND digest_status = 'completed'
            """,
            (range_start,),
        ).fetchone()[0]
        editorial_candidates = state.conn.execute(
            """
            SELECT COUNT(*) FROM events
            WHERE status IN ('active', 'stale')
              AND (last_editorial_at IS NULL OR last_editorial_at < updated_at)
            """
        ).fetchone()[0]
    if progress:
        progress("preflight: validating current artifacts")
    validation = validate_artifacts(progress=progress, **(validation_kwargs or {}))
    index = _read_json_unchecked(ACTIVE_STORIES_PATH)
    return {
        "dry_run": True,
        "network_calls": 0,
        "llm_calls": 0,
        "mutations": 0,
        "planned_at": isoformat_z(reference),
        "stages": {
            "maintenance": maintenance,
            "collection": {"feeds_enabled": len(load_feeds(enabled_only=True))},
            "article_digest": {"candidates": int(digest_candidates), "range_start": range_start},
            "aggregation": {"candidate_articles": int(aggregation_candidates)},
            "editorial": {"candidate_events": int(editorial_candidates)},
            "presentation": {
                "indexed_stories": len(index.get("stories", [])),
                "publish_enabled": bool(config.presentation.get("publish_enabled", False)),
                "publish_dir": config.presentation.get("publish_dir"),
            },
        },
        "validation": validation,
    }


def validate_artifacts(
    *,
    article_dir: Path = ARTICLE_DIR,
    event_dir: Path = EVENT_DIR,
    story_dir: Path = STORY_DIR,
    active_stories_path: Path = ACTIVE_STORIES_PATH,
    config_dir: Path = CONFIG_DIR,
    db_path: Path = DB_PATH,
    dist_dir: Path = DIST_DIR,
    progress: Callable[[str], None] | None = None,
    max_reported_errors: int = 100,
) -> dict[str, Any]:
    errors: list[dict[str, str]] = []
    error_count = 0

    def report(path: Path | str, message: str) -> None:
        nonlocal error_count
        error_count += 1
        if len(errors) < max_reported_errors:
            errors.append({"path": str(path), "error": message})

    categories, category_names = _validate_configs(config_dir, report)
    article_count = 0
    digest_count = 0
    if progress:
        progress("validate: checking article artifacts")
    for path in sorted(article_dir.rglob("*.json")) if article_dir.exists() else []:
        article_count += 1
        data = _read_object(path, report)
        if data is None:
            continue
        article_id = _required_string(data, "article_id", path, report)
        _required_string(data, "source_id", path, report)
        _required_string(data, "source_name", path, report)
        _required_string(data, "headline", path, report)
        _required_string(data, "published_at", path, report)
        _required_string(data, "fetched_at", path, report)
        if article_id and path.stem != article_id:
            report(path, f"article_id does not match filename: {article_id}")
        if article_id and sanitize_id(article_id) != article_id:
            report(path, f"unsafe article_id: {article_id}")
        _validate_web_url(data.get("canonical_url") or data.get("url"), path, "article URL", report)
        digest = data.get("llm_digest")
        if digest is not None:
            digest_count += 1
            if not isinstance(digest, dict):
                report(path, "llm_digest must be an object or null")
            else:
                _required_string(digest, "summary", path, report, prefix="llm_digest.")
                _required_string(digest, "model", path, report, prefix="llm_digest.")
                _required_string(digest, "prompt_version", path, report, prefix="llm_digest.")
                _required_string(digest, "generated_at", path, report, prefix="llm_digest.")
                if not isinstance(digest.get("key_facts"), list):
                    report(path, "llm_digest.key_facts must be a list")

    if progress:
        progress("validate: checking event artifacts")
    event_count = 0
    event_ids: set[str] = set()
    for path in sorted(event_dir.glob("*.json")) if event_dir.exists() else []:
        event_count += 1
        data = _read_object(path, report)
        if data is None:
            continue
        event_id = _required_string(data, "event_id", path, report)
        if event_id:
            if event_id in event_ids:
                report(path, f"duplicate event_id: {event_id}")
            event_ids.add(event_id)
            if path.stem != event_id:
                report(path, f"event_id does not match filename: {event_id}")
            if sanitize_id(event_id) != event_id:
                report(path, f"unsafe event_id: {event_id}")
        _required_string(data, "title", path, report)
        category = _required_string(data, "category", path, report)
        if category and category not in categories:
            report(path, f"unknown category: {category}")
        if data.get("status") not in {"active", "stale", "archived"}:
            report(path, f"invalid event status: {data.get('status')!r}")
        article_ids = data.get("article_ids")
        if not isinstance(article_ids, list) or not all(isinstance(item, str) and item for item in article_ids):
            report(path, "article_ids must be a list of non-empty strings")
        elif data.get("article_count") != len(set(article_ids)):
            report(path, "article_count does not match unique article_ids")
        metadata = data.get("llm_metadata")
        if not isinstance(metadata, dict):
            report(path, "llm_metadata must be an object")
        else:
            _required_string(metadata, "stage", path, report, prefix="llm_metadata.")
            _required_string(metadata, "prompt_version", path, report, prefix="llm_metadata.")
        newsworthiness = data.get("newsworthiness")
        if not isinstance(newsworthiness, dict):
            report(path, "newsworthiness must be an object")
        else:
            _required_string(
                newsworthiness, "prompt_version", path, report, prefix="newsworthiness."
            )
            _required_string(newsworthiness, "model", path, report, prefix="newsworthiness.")

    if progress:
        progress("validate: checking editorial story artifacts")
    story_count = 0
    stories: dict[str, dict[str, Any]] = {}
    for path in sorted(story_dir.glob("*.json")) if story_dir.exists() else []:
        story_count += 1
        data = _read_object(path, report)
        if data is None:
            continue
        story_id = _required_string(data, "story_id", path, report)
        event_id = _required_string(data, "event_id", path, report)
        if story_id:
            stories[story_id] = data
            if path.stem != story_id:
                report(path, f"story_id does not match filename: {story_id}")
            if sanitize_id(story_id) != story_id:
                report(path, f"unsafe story_id: {story_id}")
        if story_id and event_id and story_id != event_id:
            report(path, "story_id and event_id must match")
        category = _required_string(data, "category", path, report)
        if category and category not in categories:
            report(path, f"unknown category: {category}")
        for key in ("headline", "dek", "created_at", "updated_at"):
            _required_string(data, key, path, report)
        for key in ("tldr", "key_facts", "uncertainties", "sources"):
            if not isinstance(data.get(key), list):
                report(path, f"{key} must be a list")
        metadata = data.get("llm_metadata")
        if not isinstance(metadata, dict):
            report(path, "llm_metadata must be an object")
        else:
            for key in ("model", "prompt_version", "generated_at", "event_updated_at"):
                _required_string(metadata, key, path, report, prefix="llm_metadata.")
        source_ids: set[str] = set()
        for source in data.get("sources") if isinstance(data.get("sources"), list) else []:
            if not isinstance(source, dict):
                report(path, "each source must be an object")
                continue
            source_id = source.get("article_id")
            if not isinstance(source_id, str) or not source_id:
                report(path, "source article_id must be a non-empty string")
            else:
                source_ids.add(source_id)
            _validate_web_url(source.get("url"), path, "source URL", report)
        for section in ("key_facts", "uncertainties"):
            for item in data.get(section) if isinstance(data.get(section), list) else []:
                citations = item.get("source_article_ids") if isinstance(item, dict) else None
                if not isinstance(citations, list) or not citations:
                    report(path, f"{section} entries must have source_article_ids")
                elif not set(citations) <= source_ids:
                    report(path, f"{section} cites an article absent from sources")

    index_count, indexed_story_ids = _validate_active_index(
        active_stories_path,
        stories=stories,
        categories=categories,
        report=report,
    )
    _validate_database(db_path, report)
    static_file_count = _validate_static_output(
        dist_dir,
        story_ids=indexed_story_ids,
        report=report,
    )

    return {
        "validation_version": VALIDATION_VERSION,
        "valid": error_count == 0,
        "errors_total": error_count,
        "errors": errors,
        "counts": {
            "articles": article_count,
            "article_digests": digest_count,
            "events": event_count,
            "stories": story_count,
            "active_index_stories": index_count,
            "static_files": static_file_count,
            "categories": len(category_names),
        },
        "validated_at": isoformat_z(),
    }


def llm_usage_report(
    *,
    db_path: Path = DB_PATH,
    hours: int = 24,
    now: datetime | None = None,
) -> dict[str, Any]:
    if hours < 1:
        raise ValueError("hours must be at least 1")
    reference = (now or utc_now()).astimezone(UTC)
    cutoff = isoformat_z(reference - timedelta(hours=hours))
    with StateDB(db_path) as state:
        rows = state.conn.execute(
            """
            SELECT stage, model, prompt_version, COUNT(*) AS calls,
                   SUM(COALESCE(input_tokens, 0)) AS input_tokens,
                   SUM(COALESCE(output_tokens, 0)) AS output_tokens,
                   SUM(COALESCE(cost_usd, 0)) AS cost_usd,
                   COUNT(DISTINCT run_id) AS run_count
            FROM llm_usage
            WHERE occurred_at >= ?
            GROUP BY stage, model, prompt_version
            ORDER BY stage, model, prompt_version
            """,
            (cutoff,),
        ).fetchall()
    groups = [dict(row) for row in rows]
    return {
        "hours": hours,
        "since": cutoff,
        "generated_at": isoformat_z(reference),
        "calls": sum(int(row["calls"]) for row in groups),
        "input_tokens": sum(int(row["input_tokens"]) for row in groups),
        "output_tokens": sum(int(row["output_tokens"]) for row in groups),
        "cost_usd": round(sum(float(row["cost_usd"]) for row in groups), 6),
        "groups": groups,
    }


def health_report(
    *,
    db_path: Path = DB_PATH,
    check_live_site: bool = True,
    site_url: str | None = None,
    max_age_hours: int | None = None,
    validate: bool = True,
    validation_kwargs: dict[str, Any] | None = None,
    now: datetime | None = None,
    progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    config = load_pipeline_config()
    operations = config.operations
    selected_max_age = int(max_age_hours or operations.get("max_pipeline_age_hours", 3))
    if selected_max_age < 1:
        raise ValueError("max pipeline age must be at least 1 hour")
    reference = (now or utc_now()).astimezone(UTC)
    checks: list[dict[str, Any]] = []

    if progress:
        progress("health: checking SQLite and pipeline runs")
    try:
        with StateDB(db_path) as state:
            quick = state.conn.execute("PRAGMA quick_check").fetchone()[0]
            checks.append(_check("sqlite", quick == "ok", {"quick_check": quick}))
            for stage in REQUIRED_PIPELINE_STAGES:
                row = state.conn.execute(
                    """
                    SELECT status, started_at, finished_at
                    FROM pipeline_runs
                    WHERE stage = ?
                    ORDER BY COALESCE(finished_at, started_at) DESC
                    LIMIT 1
                    """,
                    (stage,),
                ).fetchone()
                age_hours = None
                ok = False
                if row and row["finished_at"]:
                    finished = _parse_time(row["finished_at"])
                    age_hours = max(0.0, (reference - finished).total_seconds() / 3600)
                    ok = row["status"] == "success" and age_hours <= selected_max_age
                checks.append(
                    _check(
                        f"stage:{stage}",
                        ok,
                        {
                            "status": row["status"] if row else "missing",
                            "finished_at": row["finished_at"] if row else None,
                            "age_hours": round(age_hours, 3) if age_hours is not None else None,
                            "max_age_hours": selected_max_age,
                        },
                    )
                )

            stale_cutoff = isoformat_z(
                reference
                - timedelta(minutes=int(config.pipeline.get("watchdog_timeout_minutes", 30)))
            )
            stale_running = state.conn.execute(
                "SELECT COUNT(*) FROM pipeline_runs WHERE status = 'running' AND started_at < ?",
                (stale_cutoff,),
            ).fetchone()[0]
            checks.append(_check("stale_running_runs", stale_running == 0, {"count": stale_running}))

            pending_editorial = state.conn.execute(
                """
                SELECT COUNT(*) FROM events
                WHERE status IN ('active', 'stale')
                  AND (last_editorial_at IS NULL OR last_editorial_at < updated_at)
                """
            ).fetchone()[0]
            checks.append(
                _check(
                    "pending_editorial_events",
                    pending_editorial == 0,
                    {"count": pending_editorial},
                )
            )

            latest_collection = state.conn.execute(
                """
                SELECT run_id FROM pipeline_runs
                WHERE stage = 'collection' AND status = 'success'
                ORDER BY finished_at DESC LIMIT 1
                """
            ).fetchone()
            source_failures = 0
            article_failures = 0
            if latest_collection:
                source_row = state.conn.execute(
                    """
                    SELECT SUM(CASE WHEN feed_status = 'failed' OR error_count > 0 THEN 1 ELSE 0 END),
                           SUM(articles_failed)
                    FROM source_run_stats WHERE run_id = ?
                    """,
                    (latest_collection["run_id"],),
                ).fetchone()
                source_failures = int(source_row[0] or 0)
                article_failures = int(source_row[1] or 0)
            max_failed_feeds = int(operations.get("max_failed_feeds", 5))
            max_article_failures = int(operations.get("max_article_failures", 25))
            checks.append(
                _check(
                    "latest_collection_failures",
                    source_failures <= max_failed_feeds and article_failures <= max_article_failures,
                    {
                        "failed_feeds": source_failures,
                        "max_failed_feeds": max_failed_feeds,
                        "failed_articles": article_failures,
                        "max_failed_articles": max_article_failures,
                    },
                )
            )
    except (OSError, sqlite3.Error, ValueError) as exc:
        checks.append(_check("sqlite", False, {"error": str(exc)}))

    if validate:
        if progress:
            progress("health: validating artifacts")
        validation = validate_artifacts(progress=progress, **(validation_kwargs or {}))
        checks.append(
            _check(
                "artifacts",
                bool(validation["valid"]),
                {
                    "errors_total": validation["errors_total"],
                    "counts": validation["counts"],
                    "sample_errors": validation["errors"][:5],
                },
            )
        )

    if check_live_site:
        selected_url = site_url or str(
            config.presentation.get("site_url") or "https://news-tldr.com"
        )
        if progress:
            progress(f"health: checking {selected_url}")
        checks.append(_live_site_check(selected_url))

    healthy = all(check["ok"] for check in checks)
    return {
        "health_version": HEALTH_VERSION,
        "status": "healthy" if healthy else "unhealthy",
        "checked_at": isoformat_z(reference),
        "checks": checks,
    }


def write_health_report(report: dict[str, Any], path: Path = HEALTH_PATH) -> None:
    atomic_write_json(path, report)


def _validate_configs(
    config_dir: Path,
    report: Callable[[Path | str, str], None],
) -> tuple[set[str], dict[str, str]]:
    categories_data = _read_object(config_dir / "categories.json", report) or {}
    categories: set[str] = set()
    category_names: dict[str, str] = {}
    sort_orders: set[int] = set()
    for item in categories_data.get("categories", []):
        if not isinstance(item, dict) or not isinstance(item.get("id"), str):
            report(config_dir / "categories.json", "each category must have an id")
            continue
        category_id = item["id"]
        if category_id in categories or sanitize_id(category_id) != category_id:
            report(config_dir / "categories.json", f"duplicate or unsafe category id: {category_id}")
        categories.add(category_id)
        category_names[category_id] = str(item.get("name") or category_id)
        sort_order = item.get("sort_order")
        if not isinstance(sort_order, int) or sort_order in sort_orders:
            report(
                config_dir / "categories.json",
                f"category {category_id} must have a unique integer sort_order",
            )
        else:
            sort_orders.add(sort_order)

    feeds_data = _read_object(config_dir / "feeds.json", report) or {}
    policy_data = _read_object(config_dir / "source-policy.json", report) or {}
    feed_ids = {
        item.get("source_id")
        for item in feeds_data.get("feeds", [])
        if isinstance(item, dict) and item.get("source_id")
    }
    policy_ids = {
        item.get("source_id")
        for item in policy_data.get("sources", [])
        if isinstance(item, dict) and item.get("source_id")
    }
    if feed_ids != policy_ids:
        report(
            config_dir,
            "feed/source-policy source_id mismatch: "
            f"missing_policy={sorted(feed_ids - policy_ids)}, missing_feed={sorted(policy_ids - feed_ids)}",
        )
    _read_object(config_dir / "pipeline.json", report)
    return categories, category_names


def _validate_active_index(
    path: Path,
    *,
    stories: dict[str, dict[str, Any]],
    categories: set[str],
    report: Callable[[Path | str, str], None],
) -> tuple[int, set[str]]:
    data = _read_object(path, report)
    if data is None:
        return 0, set()
    rows = data.get("stories")
    if not isinstance(rows, list):
        report(path, "stories must be a list")
        return 0, set()
    seen: set[str] = set()
    previous_rank: float | None = None
    for row in rows:
        if not isinstance(row, dict):
            report(path, "index rows must be objects")
            continue
        story_id = row.get("story_id")
        if not isinstance(story_id, str) or not story_id:
            report(path, "index story_id must be a non-empty string")
            continue
        if story_id in seen:
            report(path, f"duplicate index story_id: {story_id}")
        seen.add(story_id)
        story = stories.get(story_id)
        if story is None:
            report(path, f"index references missing story: {story_id}")
        elif row.get("category") != story.get("category"):
            report(path, f"index category mismatch for {story_id}")
        if row.get("category") not in categories:
            report(path, f"index has unknown category for {story_id}")
        try:
            importance = float(row.get("importance_score"))
            rank = float(row.get("homepage_rank_score", importance))
        except (TypeError, ValueError):
            report(path, f"invalid importance or homepage rank score for {story_id}")
            continue
        if previous_rank is not None and rank > previous_rank + 1e-9:
            report(path, "stories are not sorted by descending homepage rank")
        previous_rank = rank
    return len(rows), seen


def _validate_database(db_path: Path, report: Callable[[Path | str, str], None]) -> None:
    if not db_path.exists():
        report(db_path, "SQLite database is missing")
        return
    try:
        uri = f"file:{db_path.resolve()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True)
        try:
            result = connection.execute("PRAGMA quick_check").fetchone()[0]
            if result != "ok":
                report(db_path, f"SQLite quick_check failed: {result}")
            missing_prompt = connection.execute(
                "SELECT COUNT(*) FROM llm_usage WHERE prompt_version IS NULL OR trim(prompt_version) = ''"
            ).fetchone()[0]
            if missing_prompt:
                report(db_path, f"{missing_prompt} LLM usage rows lack prompt_version")
        finally:
            connection.close()
    except sqlite3.Error as exc:
        report(db_path, f"SQLite validation failed: {exc}")


def _validate_static_output(
    dist_dir: Path,
    *,
    story_ids: set[str],
    report: Callable[[Path | str, str], None],
) -> int:
    if not dist_dir.exists():
        report(dist_dir, "static output directory is missing")
        return 0
    required = (
        dist_dir / "index.html",
        dist_dir / "archive" / "index.html",
        dist_dir / "api" / "active-stories.json",
        dist_dir / "assets" / "site.css",
        dist_dir / "assets" / "site.js",
        dist_dir / "assets" / "social-card.png",
        dist_dir / "robots.txt",
        dist_dir / "sitemap.xml",
    )
    for path in required:
        if not path.is_file():
            report(path, "required static file is missing")
    count = 0
    for path in dist_dir.rglob("*"):
        if path.is_symlink():
            report(path, "static output must not contain symlinks")
        elif path.is_file():
            count += 1
    for story_id in story_ids:
        if not (dist_dir / "stories" / story_id / "index.html").is_file():
            report(dist_dir, f"missing static story page: {story_id}")
        if not (dist_dir / "api" / "stories" / f"{story_id}.json").is_file():
            report(dist_dir, f"missing static story API file: {story_id}")
    return count


def _read_object(
    path: Path,
    report: Callable[[Path | str, str], None],
) -> dict[str, Any] | None:
    try:
        with path.open("r", encoding="utf-8") as file:
            data = json.load(file)
    except (OSError, json.JSONDecodeError) as exc:
        report(path, f"invalid JSON: {exc}")
        return None
    if not isinstance(data, dict):
        report(path, "top-level JSON value must be an object")
        return None
    return data


def _read_json_unchecked(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as file:
            data = json.load(file)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _required_string(
    data: dict[str, Any],
    key: str,
    path: Path,
    report: Callable[[Path | str, str], None],
    *,
    prefix: str = "",
) -> str | None:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        report(path, f"{prefix}{key} must be a non-empty string")
        return None
    return value


def _validate_web_url(
    value: Any,
    path: Path,
    label: str,
    report: Callable[[Path | str, str], None],
) -> None:
    if not isinstance(value, str):
        report(path, f"{label} must be a string")
        return
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        report(path, f"{label} must be an HTTP(S) URL")


def _check(name: str, ok: bool, details: dict[str, Any]) -> dict[str, Any]:
    return {"name": name, "ok": ok, "details": details}


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _live_site_check(site_url: str) -> dict[str, Any]:
    checks = [site_url.rstrip("/") + "/", site_url.rstrip("/") + "/api/active-stories.json"]
    results: list[dict[str, Any]] = []
    ok = True
    for url in checks:
        request = Request(url, headers={"User-Agent": "news-tldr-health/1.0"})
        try:
            with urlopen(request, timeout=15) as response:
                status = int(response.status)
                content_type = response.headers.get_content_type()
                response.read(256)
            result = {"url": url, "status": status, "content_type": content_type}
            ok = ok and status == 200
        except (HTTPError, URLError, OSError, TimeoutError) as exc:
            result = {"url": url, "error": str(exc)}
            ok = False
        results.append(result)
    return _check("live_site", ok, {"requests": results})
