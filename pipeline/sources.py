"""Publisher identity is distinct from feed identity and reporting provenance."""

from __future__ import annotations

import re
from functools import lru_cache
from typing import Any
from urllib.parse import urlsplit

from pipeline.config import load_feeds, load_source_policy


@lru_cache(maxsize=1)
def _publishers() -> dict[str, str]:
    policy = load_source_policy()
    result = {}
    for feed in load_feeds(enabled_only=False):
        publisher = str(policy.get(feed.source_id, {}).get("publisher_id") or feed.source_name.split(" - ")[0])
        result[feed.source_id] = publisher
        result[feed.source_name] = publisher
    return result


def publisher_id(source: dict[str, Any]) -> str:
    explicit = source.get("publisher_id")
    if explicit:
        return str(explicit)
    identities = _publishers()
    for key in (source.get("source_id"), source.get("source_name")):
        if key in identities:
            return identities[key]
    name = str(source.get("source_name") or "").split(" - ")[0].strip()
    return name or (urlsplit(str(source.get("url") or "")).hostname or "unknown")


def reporting_origin(text: str, publisher: str) -> str | None:
    """Only recognize explicit wire bylines; missing provenance is unknown, not independent."""
    lead = text[:1800]
    if re.search(r"\(AP\)|(?m:^\s*(?:By [^\n]{0,100}[, ]+)?(?:The )?Associated Press\s*$)", lead):
        return "associated-press"
    if re.search(r"\(Reuters\)|\bBy [^\n]{0,100}Reuters\b", lead):
        return "reuters"
    if publisher in {"associated-press", "reuters"}:
        return publisher
    return None
