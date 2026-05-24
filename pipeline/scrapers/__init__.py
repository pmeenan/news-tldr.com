from __future__ import annotations

import importlib
from typing import Any

from pipeline.config import FeedConfig
from pipeline.http_client import PoliteHTTPClient


async def run_scraper(client: PoliteHTTPClient, feed: FeedConfig) -> list[dict[str, Any]]:
    """
    Run the scraper configured for the given feed.
    Returns a list of dictionaries that mimic feedparser entries.
    """
    module_name = feed.fetch.get("scraper_module")
    if not module_name:
        raise ValueError(f"No scraper_module configured for {feed.source_id}")
    if not module_name.startswith("pipeline.scrapers."):
        raise ValueError(
            f"scraper_module must live under pipeline.scrapers.*: {module_name} ({feed.source_id})"
        )

    try:
        module = importlib.import_module(module_name)
        scrape_func = getattr(module, "scrape")
    except (ImportError, AttributeError) as e:
        raise ImportError(f"Failed to load scraper {module_name} for {feed.source_id}") from e

    return await scrape_func(client, feed)
