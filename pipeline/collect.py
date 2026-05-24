from __future__ import annotations

import asyncio
import hashlib
import html
import re
import uuid
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlsplit, urlunsplit

import feedparser
import httpx
import trafilatura

from pipeline.config import FeedConfig, load_feeds, load_pipeline_config
from pipeline.http_client import PoliteHTTPClient
from pipeline.lock import PipelineLock
from pipeline.paths import ARTICLE_DIR, FETCH_LOG_DIR, LOCK_PATH
from pipeline.state import StateDB, migrate
from pipeline.util import atomic_append_jsonl, atomic_write_json, isoformat_z, sanitize_id, utc_now

TAG_RE = re.compile(r"<[^>]+>")
WHITESPACE_RE = re.compile(r"\s+")
DTD_RE = re.compile(r"<!\s*(doctype|entity|element|attlist|notation)\b", re.IGNORECASE)
PAYWALL_PATTERNS = (
    "subscribe to continue",
    "subscription required",
    "sign in to continue",
    "create a free account",
    "already a subscriber",
    "metered paywall",
)


def _clean_text(value: str | None) -> str:
    if not value:
        return ""
    text = TAG_RE.sub(" ", value)
    return WHITESPACE_RE.sub(" ", html.unescape(text)).strip()


def _hash(value: str | None) -> str | None:
    if not value:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _canonicalize_url(url: str) -> str:
    parts = urlsplit(url.strip())
    scheme = parts.scheme.lower()
    netloc = parts.netloc.lower()
    path = parts.path or "/"
    query_items = []
    for item in parts.query.split("&"):
        if not item:
            continue
        key = item.split("=", 1)[0].lower()
        if key.startswith("utm_") or key in {"fbclid", "gclid", "mc_cid", "mc_eid"}:
            continue
        query_items.append(item)
    query_items.sort()
    return urlunsplit((scheme, netloc, path, "&".join(query_items), ""))


def _entry_datetime(entry: Any, key: str) -> datetime | None:
    parsed = entry.get(f"{key}_parsed")
    if parsed:
        return datetime(*parsed[:6], tzinfo=UTC)
    raw = entry.get(key)
    if raw:
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            return None
    return None


def _entry_authors(entry: Any) -> list[str]:
    authors: list[str] = []
    for author in entry.get("authors", []) or []:
        name = _clean_text(author.get("name"))
        if name:
            authors.append(name)
    direct = _clean_text(entry.get("author"))
    if direct and direct not in authors:
        authors.append(direct)
    return authors


def _entry_tags(entry: Any) -> list[str]:
    tags = []
    for tag in entry.get("tags", []) or []:
        term = _clean_text(tag.get("term"))
        if term:
            tags.append(term)
    return sorted(set(tags))


def _feed_content(entry: Any) -> tuple[str, str]:
    content_items = entry.get("content") or []
    if content_items:
        value = content_items[0].get("value")
        text = _clean_text(value)
        if text:
            return text, "feed"
    summary = _clean_text(entry.get("summary") or entry.get("description"))
    return summary, "feed"


def _is_incomplete_feed_content(text: str) -> bool:
    return len(text) < 600


def _article_id(canonical_url: str | None, source_id: str, guid: str | None, fallback_key: str) -> tuple[str, str]:
    if canonical_url:
        return hashlib.sha256(canonical_url.encode("utf-8")).hexdigest(), "canonical_url"
    if guid:
        fallback = f"{source_id}:{guid}"
        return hashlib.sha256(fallback.encode("utf-8")).hexdigest(), "guid"
    fallback = f"{source_id}:{fallback_key}"
    return hashlib.sha256(fallback.encode("utf-8")).hexdigest(), "entry_fingerprint"


def _article_path(article_id: str, published_at: str) -> Path:
    date = datetime.fromisoformat(published_at.replace("Z", "+00:00")).date()
    return ARTICLE_DIR / f"{date:%Y}" / f"{date:%m}" / f"{date:%d}" / f"{sanitize_id(article_id)}.json"


def _paywall_status(source_hint: str | None, status_code: int | None, text: str) -> dict[str, Any]:
    signals: list[str] = []
    hint = (source_hint or "unknown").lower()
    if hint in {"metered", "hard", "subscription"}:
        signals.append(f"source_paywall_hint:{hint}")
    if status_code in {401, 402, 403}:
        signals.append(f"http_status:{status_code}")
    lowered = text.lower()
    for pattern in PAYWALL_PATTERNS:
        if pattern in lowered:
            signals.append(f"text:{pattern}")
            break
    if any(signal.startswith("http_status") for signal in signals) or hint == "hard":
        status = "confirmed"
    elif signals:
        status = "suspected"
    else:
        status = "none"
    return {"status": status, "signals": signals}


def _parse_feed_bytes(content: bytes) -> Any:
    decoded = None
    for encoding in ("utf-8-sig", "utf-16", "utf-16be", "utf-16le", "utf-32", "utf-32be", "utf-32le", "latin-1"):
        try:
            text = content.decode(encoding)
            if "\x00" not in text and text.strip().startswith("<"):
                decoded = text
                break
        except Exception:
            continue
    if decoded is None:
        decoded = content.decode("utf-8", errors="replace")

    if DTD_RE.search(decoded):
        raise ValueError("feed contains DTD/entity declarations")
    return feedparser.parse(content)


class Collector:
    def __init__(
        self,
        state: StateDB,
        client: PoliteHTTPClient,
        feeds: list[FeedConfig],
        *,
        run_id: str,
        max_feed_concurrency: int = 8,
        max_article_concurrency: int = 12,
    ) -> None:
        self.state = state
        self.client = client
        self.feeds = feeds
        self.run_id = run_id
        self.feed_sem = asyncio.Semaphore(max_feed_concurrency)
        self.article_sem = asyncio.Semaphore(max_article_concurrency)
        self.article_id_lock = asyncio.Lock()
        self.seen_article_ids: set[str] = set()
        self.log_rows: list[dict[str, Any]] = []
        self.stats = {
            "feeds_seen": len(feeds),
            "feeds_fetched": 0,
            "feeds_not_modified": 0,
            "feeds_failed": 0,
            "entries_seen": 0,
            "articles_skipped": 0,
            "articles_written": 0,
            "articles_failed": 0,
        }

    async def run(self) -> dict[str, int]:
        self.state.sync_feeds(self.feeds)
        await asyncio.gather(*(self._collect_feed(feed) for feed in self.feeds))
        log_path = FETCH_LOG_DIR / f"{utc_now():%Y-%m-%d}.jsonl"
        atomic_append_jsonl(log_path, self.log_rows)
        return self.stats

    async def _collect_feed(self, feed: FeedConfig) -> None:
        async with self.feed_sem:
            headers = self.state.feed_headers(feed.source_id)
            try:
                response = await self.client.get(feed.feed_url, headers=headers)
                if response.status_code == 304:
                    self.stats["feeds_not_modified"] += 1
                    self.state.update_feed_state(feed.source_id, status=304)
                    self._log(feed.source_id, "feed", feed.feed_url, 304, "not_modified")
                    return
                response.raise_for_status()
                self.stats["feeds_fetched"] += 1
                self.state.update_feed_state(
                    feed.source_id,
                    status=response.status_code,
                    etag=response.headers.get("etag"),
                    last_modified=response.headers.get("last-modified"),
                )
                parsed = _parse_feed_bytes(response.content)
                entries = list(parsed.entries or [])
                self.stats["entries_seen"] += len(entries)
                await asyncio.gather(*(self._collect_entry(feed, entry) for entry in entries))
            except Exception as exc:
                self.stats["feeds_failed"] += 1
                self.state.update_feed_state(feed.source_id, status=None, failed=True)
                self.state.record_error(self.run_id, "collection", "feed", feed.feed_url, feed.source_id, exc)
                self._log(feed.source_id, "feed", feed.feed_url, None, "failed", str(exc))

    async def _collect_entry(self, feed: FeedConfig, entry: Any) -> None:
        async with self.article_sem:
            try:
                article = await self._article_from_entry(feed, entry)
                async with self.article_id_lock:
                    if article["article_id"] in self.seen_article_ids or self.state.article_exists(
                        article["article_id"]
                    ):
                        self.stats["articles_skipped"] += 1
                        return
                    self.seen_article_ids.add(article["article_id"])
                article_path = _article_path(article["article_id"], article["published_at"])
                atomic_write_json(article_path, article)
                self.state.insert_article(article, article_path)
                self.stats["articles_written"] += 1
                self._log(
                    feed.source_id,
                    "article",
                    article["url"],
                    article["collection"].get("http_status"),
                    "written",
                    article["article_id"],
                )
            except Exception as exc:
                self.stats["articles_failed"] += 1
                item_id = entry.get("id") or entry.get("guid") or entry.get("link")
                self.state.record_error(self.run_id, "collection", "article", item_id, feed.source_id, exc)
                self._log(feed.source_id, "article", str(item_id), None, "failed", str(exc))

    async def _article_from_entry(self, feed: FeedConfig, entry: Any) -> dict[str, Any]:
        fetched_at = isoformat_z()
        raw_url = entry.get("link") or entry.get("id") or entry.get("guid")
        if not raw_url:
            raise ValueError("feed entry has no link, id, or guid")
        entry_url = urljoin(feed.feed_url, str(raw_url).strip())
        canonical_url = _canonicalize_url(entry_url) if entry_url.lower().startswith(("http://", "https://")) else None
        guid = entry.get("id") or entry.get("guid")
        headline = _clean_text(entry.get("title")) or "(untitled)"
        summary = _clean_text(entry.get("summary") or entry.get("description"))
        content_text, extractor = _feed_content(entry)
        status_code: int | None = None
        language = entry.get("language") or None

        if canonical_url and _is_incomplete_feed_content(content_text):
            try:
                page = await self._fetch_article_page(canonical_url)
                status_code = page.status_code
                if status_code < 400:
                    canonical_url = _canonicalize_url(str(page.url))
                    extracted = trafilatura.extract(page.text, url=canonical_url, favor_precision=True)
                    if extracted:
                        content_text = _clean_text(extracted)
                        extractor = "readability"
                    language = language or page.headers.get("content-language")
                else:
                    self.state.record_error(
                        self.run_id,
                        "collection",
                        "article_fetch_http_error",
                        canonical_url,
                        feed.source_id,
                        f"HTTP status {status_code}",
                    )
            except Exception as exc:
                self.state.record_error(
                    self.run_id,
                    "collection",
                    "article_fetch_exception",
                    canonical_url,
                    feed.source_id,
                    exc,
                )

        published = _entry_datetime(entry, "published") or _entry_datetime(entry, "updated")
        publish_date_estimated = published is None
        if published is None:
            published = datetime.now(UTC)
        published_at = isoformat_z(published)
        fallback_key = f"{entry_url}:{headline}:{published_at}"
        article_id, article_id_source = _article_id(canonical_url, feed.source_id, guid, fallback_key)
        source_paywall = feed.content_hints.get("paywall")
        article = {
            "article_id": article_id,
            "source_id": feed.source_id,
            "source_name": feed.source_name,
            "url": entry_url,
            "canonical_url": canonical_url,
            "guid": guid,
            "headline": headline,
            "summary": summary or (content_text[:300] if content_text else ""),
            "content_text": content_text,
            "published_at": published_at,
            "publish_date_estimated": publish_date_estimated,
            "fetched_at": fetched_at,
            "authors": _entry_authors(entry),
            "tags": _entry_tags(entry),
            "paywall": _paywall_status(source_paywall, status_code, content_text),
            "content_type": feed.content_hints.get("default_content_type", "unknown"),
            "language": language,
            "collection": {
                "feed_url": feed.feed_url,
                "http_status": status_code,
                "extractor": extractor,
                "article_id_source": article_id_source,
                "source_config": asdict(feed),
            },
            "fingerprints": {
                "canonical_url_hash": _hash(canonical_url),
                "headline_hash": _hash(headline.lower()),
                "summary_hash": _hash((summary or "").lower()),
                "content_hash": _hash(content_text),
            },
        }
        return article

    async def _fetch_article_page(self, url: str) -> httpx.Response:
        response = await self.client.get(url)
        if response.status_code >= 400:
            return response
        return response

    def _log(
        self,
        source_id: str,
        item_type: str,
        item_id: str,
        http_status: int | None,
        status: str,
        detail: str | None = None,
    ) -> None:
        self.log_rows.append(
            {
                "run_id": self.run_id,
                "source_id": source_id,
                "item_type": item_type,
                "item_id": item_id,
                "http_status": http_status,
                "status": status,
                "detail": detail,
                "logged_at": isoformat_z(),
            }
        )


async def collect_once() -> dict[str, int]:
    pipeline_config = load_pipeline_config()
    feeds = load_feeds(enabled_only=True)
    migrate()
    collection = pipeline_config.collection
    run_id = f"collection-{utc_now():%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    lock_timeout = pipeline_config.pipeline.get("watchdog_timeout_minutes", 30)
    async with PipelineLock(LOCK_PATH, timeout_seconds_to_timedelta(lock_timeout), run_id=run_id):
        with StateDB() as state:
            state.start_run(run_id, "collection")
            status = "success"
            async with PoliteHTTPClient(
                rate_limit_seconds=float(collection.get("rate_limit_seconds", 2)),
                connection_timeout_seconds=float(collection.get("connection_timeout_seconds", 10)),
                read_timeout_seconds=float(collection.get("read_timeout_seconds", 30)),
                max_retries=int(collection.get("max_retries", 3)),
                backoff_initial_seconds=float(collection.get("backoff_initial_seconds", 5)),
                backoff_max_seconds=float(collection.get("backoff_max_seconds", 60)),
            ) as client:
                collector = Collector(state, client, feeds, run_id=run_id)
                try:
                    stats = await collector.run()
                except Exception:
                    status = "failed"
                    raise
                finally:
                    if "stats" not in locals():
                        stats = {}
                    state.finish_run(run_id, status, stats)
                return stats


def timeout_seconds_to_timedelta(minutes: float) -> Any:
    from datetime import timedelta

    return timedelta(minutes=float(minutes))
