from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from pipeline.paths import CONFIG_DIR
from pipeline.util import sanitize_id


@dataclass(frozen=True)
class FeedConfig:
    source_id: str
    source_name: str
    feed_url: str
    site_url: str | None
    enabled: bool
    default_category: str
    category_hints: list[str]
    content_hints: dict[str, Any]
    fetch: dict[str, Any]
    feed_type: str = "rss"
    notes: str | None = None


@dataclass(frozen=True)
class PipelineConfig:
    collection: dict[str, Any]
    aggregation: dict[str, Any]
    retention: dict[str, Any]
    pipeline: dict[str, Any]
    digest: dict[str, Any] = field(default_factory=dict)
    editorial: dict[str, Any] = field(default_factory=dict)
    presentation: dict[str, Any] = field(default_factory=dict)
    operations: dict[str, Any] = field(default_factory=dict)
    llm: dict[str, Any] = field(default_factory=dict)


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_pipeline_config(path: Path | None = None) -> PipelineConfig:
    data = _load_json(path or CONFIG_DIR / "pipeline.json")
    return PipelineConfig(
        collection=data.get("collection", {}),
        aggregation=data.get("aggregation", {}),
        retention=data.get("retention", {}),
        pipeline=data.get("pipeline", {}),
        digest=data.get("digest", {}),
        editorial=data.get("editorial", {}),
        presentation=data.get("presentation", {}),
        operations=data.get("operations", {}),
        llm=data.get("llm", {}),
    )


def load_feeds(path: Path | None = None, *, enabled_only: bool = True) -> list[FeedConfig]:
    data = _load_json(path or CONFIG_DIR / "feeds.json")
    feeds: list[FeedConfig] = []
    seen_source_ids: set[str] = set()
    for item in data.get("feeds", []):
        source_id = item.get("source_id")
        if not source_id:
            raise ValueError(f"feed item is missing source_id: {item}")
        if source_id != sanitize_id(source_id):
            raise ValueError(f"invalid source_id: {source_id}")
        if source_id in seen_source_ids:
            raise ValueError(f"duplicate source_id: {source_id}")
        seen_source_ids.add(source_id)
        feed_url = item.get("feed_url")
        if not feed_url:
            raise ValueError(f"feed {source_id} is missing feed_url")
        parsed = urlparse(feed_url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError(f"invalid feed_url for {source_id}: {feed_url}")
        feed = FeedConfig(
            source_id=source_id,
            source_name=item.get("source_name", source_id),
            feed_url=feed_url,
            site_url=item.get("site_url"),
            enabled=bool(item.get("enabled", True)),
            default_category=item.get("default_category", "world"),
            category_hints=list(item.get("category_hints", [])),
            content_hints=dict(item.get("content_hints", {})),
            fetch=dict(item.get("fetch", {})),
            feed_type=item.get("feed_type", "rss"),
            notes=item.get("notes"),
        )
        if feed.enabled or not enabled_only:
            feeds.append(feed)
    return feeds


def load_source_policy(path: Path | None = None) -> dict[str, dict[str, Any]]:
    data = _load_json(path or CONFIG_DIR / "source-policy.json")
    return {item["source_id"]: item for item in data.get("sources", [])}


def load_categories(path: Path | None = None) -> list[str]:
    data = _load_json(path or CONFIG_DIR / "categories.json")
    return [item["id"] for item in data.get("categories", [])]
