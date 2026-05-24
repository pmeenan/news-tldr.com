from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup

from pipeline.config import FeedConfig
from pipeline.http_client import PoliteHTTPClient
from pipeline.util import isoformat_z

DATE_RE = re.compile(r"\b([A-Z][a-z]+ \d{1,2}, \d{4})\b")
ARTICLE_PATH_RE = re.compile(r"^/(?:news|features|reviews)/")
HEADLINE_ANCESTOR_TAGS = {"article", "h1", "h2", "h3", "h4", "h5", "h6"}


def _looks_like_headline_link(anchor: Any) -> bool:
    # Accept the anchor if it lives inside (or contains) a headline container.
    # This filters site-chrome links that share the article path prefix shape
    # only by accident (e.g. "Sign in", subscribe CTAs).
    node = anchor.parent
    for _ in range(6):
        if node is None:
            break
        name = getattr(node, "name", None)
        if name in HEADLINE_ANCESTOR_TAGS:
            return True
        node = node.parent
    if anchor.find(["h1", "h2", "h3", "h4", "h5", "h6"]) is not None:
        return True
    return False


def _title_from_card(anchor: Any) -> str:
    heading = anchor.find(["h1", "h2", "h3", "h4"])
    if heading:
        return heading.get_text(" ", strip=True)
    img = anchor.find("img", alt=True)
    if img and img["alt"]:
        return str(img["alt"]).strip()
    return anchor.get_text(" ", strip=True)


def _published_from_card(anchor: Any) -> str | None:
    match = DATE_RE.search(anchor.get_text(" ", strip=True))
    if not match:
        return None
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return isoformat_z(datetime.strptime(match.group(1), fmt).replace(tzinfo=UTC))
        except ValueError:
            continue
    return None


def _image_from_card(anchor: Any, base_url: str) -> str | None:
    img = anchor.find("img")
    if not img:
        return None
    raw_url = (
        img.get("src")
        or img.get("data-src")
        or img.get("data-original")
        or img.get("data-lazy-src")
    )
    return urljoin(base_url, raw_url) if raw_url else None


async def scrape(client: PoliteHTTPClient, feed: FeedConfig) -> list[dict[str, Any]]:
    response = await client.get(feed.feed_url, check_robots=False)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    feed_host = (urlsplit(feed.site_url or feed.feed_url).hostname or "").lower()
    entries = []
    seen_urls = set()

    for a in soup.find_all("a", href=True):
        url = urljoin(feed.feed_url, a["href"])
        parts = urlsplit(url)
        if (parts.hostname or "").lower() != feed_host:
            continue
        if not ARTICLE_PATH_RE.match(parts.path or ""):
            continue
        if url in seen_urls:
            continue
        if not _looks_like_headline_link(a):
            continue

        title = _title_from_card(a)
        if title and len(title) > 10:
            entries.append(
                {
                    "id": url,
                    "link": url,
                    "title": title,
                    "image_url": _image_from_card(a, feed.feed_url),
                    "summary": "",
                    "published": _published_from_card(a),
                }
            )
            seen_urls.add(url)

    return entries
