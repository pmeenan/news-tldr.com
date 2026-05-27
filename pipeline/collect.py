from __future__ import annotations

import asyncio
import hashlib
import html
import json
import re
import time
import uuid
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlsplit, urlunsplit

import feedparser
import trafilatura
from bs4 import BeautifulSoup

from pipeline.config import FeedConfig, load_feeds, load_pipeline_config
from pipeline.http_client import PoliteHTTPClient
from pipeline.lock import PipelineLock
from pipeline.paths import ARTICLE_DIR, DB_PATH, FETCH_LOG_DIR, LOCK_PATH, PROJECT_ROOT
from pipeline.state import StateDB, migrate
from pipeline.util import atomic_append_jsonl, atomic_write_bytes, atomic_write_json, isoformat_z, sanitize_id, utc_now

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
MIN_FULL_TEXT_CHARS = 600
SUMMARY_MARGIN_CHARS = 200
TRAFILATURA_EXTRACTION_MODES: tuple[tuple[str, dict[str, Any]], ...] = (
    ("trafilatura_recall", {"favor_recall": True, "include_comments": False}),
    ("trafilatura_default", {"include_comments": False}),
    ("trafilatura_precision", {"favor_precision": True, "include_comments": False}),
)
IMAGE_CONTENT_TYPES = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
}
IMAGE_SOURCE_PRIORITY = {
    "article_meta": 100,
    "entry_image": 90,
    "media_content": 80,
    "media_thumbnail": 70,
    "enclosure": 60,
    "html_img": 50,
}
MAX_IMAGE_BYTES = 10 * 1024 * 1024
ProgressCallback = Callable[[str], None]


def _clean_text(value: str | None) -> str:
    if not value:
        return ""
    text = TAG_RE.sub(" ", value)
    return WHITESPACE_RE.sub(" ", html.unescape(text)).strip()


def _summary_fallback(content_text: str, max_chars: int = 300) -> str:
    if not content_text:
        return ""
    if len(content_text) <= max_chars:
        return content_text
    snippet = content_text[:max_chars]
    last_space = snippet.rfind(" ")
    if last_space > max_chars // 2:
        snippet = snippet[:last_space]
    return f"{snippet}…"


def _short_label(value: Any | None, *, max_chars: int = 100) -> str:
    text = _clean_text(str(value) if value is not None else None)
    if len(text) <= max_chars:
        return text
    return f"{text[: max_chars - 3]}..."


def _hash(value: str | None) -> str | None:
    if not value:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _relative_to_project(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


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


def _is_supported_image_url(url: str) -> bool:
    try:
        path = urlsplit(url).path.lower()
    except Exception:
        return False
    if path.endswith((".svg", ".svgz", ".ico", ".css", ".js", ".json")):
        return False
    dot_idx = path.rfind(".")
    slash_idx = path.rfind("/")
    if dot_idx > slash_idx:
        ext = path[dot_idx:]
        if ext not in {".jpg", ".jpeg", ".png", ".webp", ".gif"}:
            return False
    return True


def _add_image_candidate(
    candidates: list[dict[str, Any]],
    seen: set[str],
    raw_url: str | None,
    base_url: str,
    source: str,
    *,
    width: Any = None,
    height: Any = None,
) -> None:
    if not raw_url:
        return
    url = urljoin(base_url, str(raw_url).strip())
    if not _is_supported_image_url(url):
        return
    parts = urlsplit(url)
    if parts.scheme not in {"http", "https"} or not parts.netloc:
        return
    if url in seen:
        return
    seen.add(url)

    def int_or_none(value: Any) -> int | None:
        try:
            parsed = int(str(value).strip())
        except (TypeError, ValueError):
            return None
        return parsed if parsed > 0 else None

    candidates.append(
        {
            "url": url,
            "source": source,
            "width": int_or_none(width),
            "height": int_or_none(height),
        }
    )


def _url_has_image_extension(raw_url: str | None) -> bool:
    if not raw_url:
        return False
    path = urlsplit(str(raw_url)).path.lower()
    return path.endswith((".jpg", ".jpeg", ".png", ".webp", ".gif"))


def _best_srcset_url(value: str | None) -> str | None:
    if not value:
        return None
    best_url = None
    best_score = -1
    for item in value.split(","):
        parts = item.strip().split()
        if not parts:
            continue
        score = 0
        if len(parts) > 1:
            descriptor = parts[1]
            try:
                score = int(float(descriptor[:-1]) * 1000) if descriptor.endswith("x") else int(descriptor[:-1])
            except ValueError:
                score = 0
        if score >= best_score:
            best_url = parts[0]
            best_score = score
    return best_url


def _html_image_candidates(html_text: str | None, base_url: str, source: str) -> list[dict[str, Any]]:
    if not html_text:
        return []
    soup = BeautifulSoup(html_text, "html.parser")
    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()

    for meta_name in ("og:image", "og:image:url", "twitter:image", "twitter:image:src"):
        meta = soup.find("meta", attrs={"property": meta_name}) or soup.find("meta", attrs={"name": meta_name})
        if meta:
            _add_image_candidate(candidates, seen, meta.get("content"), base_url, source)

    # Restrict general image parsing to the main article/body content if present
    body = (
        soup.find("article")
        or soup.find("main")
        or soup.find(class_=re.compile(r"article-body|entry-content|post-content|main-content", re.IGNORECASE))
    )
    search_context = body if body else soup

    for img in search_context.find_all("img"):
        raw_url = (
            img.get("src")
            or img.get("data-src")
            or img.get("data-original")
            or img.get("data-lazy-src")
            or _best_srcset_url(img.get("srcset"))
        )
        _add_image_candidate(
            candidates,
            seen,
            raw_url,
            base_url,
            "html_img",
            width=img.get("width"),
            height=img.get("height"),
        )

    for source_tag in search_context.find_all("source"):
        _add_image_candidate(
            candidates,
            seen,
            _best_srcset_url(source_tag.get("srcset")),
            base_url,
            "html_img",
        )

    return candidates


def _entry_image_candidates(entry: Any, base_url: str) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()

    image_url = entry.get("image_url")
    _add_image_candidate(candidates, seen, image_url, base_url, "entry_image")

    image = entry.get("image")
    if isinstance(image, dict):
        _add_image_candidate(candidates, seen, image.get("href") or image.get("url"), base_url, "entry_image")
    elif isinstance(image, str):
        _add_image_candidate(candidates, seen, image, base_url, "entry_image")

    for item in entry.get("media_content", []) or []:
        try:
            if not isinstance(item, dict):
                continue
            medium = str(item.get("medium") or "").lower()
            mime_type = str(item.get("type") or "").lower()
            if medium == "image" or mime_type.startswith("image/") or _url_has_image_extension(item.get("url")):
                _add_image_candidate(
                    candidates,
                    seen,
                    item.get("url"),
                    base_url,
                    "media_content",
                    width=item.get("width"),
                    height=item.get("height"),
                )
        except Exception:
            continue

    for item in entry.get("media_thumbnail", []) or []:
        try:
            if not isinstance(item, dict):
                continue
            _add_image_candidate(
                candidates,
                seen,
                item.get("url"),
                base_url,
                "media_thumbnail",
                width=item.get("width"),
                height=item.get("height"),
            )
        except Exception:
            continue

    for item in (entry.get("links", []) or []) + (entry.get("enclosures", []) or []):
        try:
            if not isinstance(item, dict):
                continue
            mime_type = str(item.get("type") or "").lower()
            rel = str(item.get("rel") or "").lower()
            href = item.get("href")
            if mime_type.startswith("image/") or rel == "thumbnail" or _url_has_image_extension(href):
                _add_image_candidate(candidates, seen, item.get("href"), base_url, "enclosure")
        except Exception:
            continue

    for html_field in (entry.get("summary"), entry.get("description")):
        try:
            for candidate in _html_image_candidates(html_field, base_url, "html_img"):
                _add_image_candidate(
                    candidates,
                    seen,
                    candidate.get("url"),
                    base_url,
                    candidate.get("source", "html_img"),
                    width=candidate.get("width"),
                    height=candidate.get("height"),
                )
        except Exception:
            continue

    for content_item in entry.get("content", []) or []:
        try:
            if not isinstance(content_item, dict):
                continue
            for candidate in _html_image_candidates(content_item.get("value"), base_url, "html_img"):
                _add_image_candidate(
                    candidates,
                    seen,
                    candidate.get("url"),
                    base_url,
                    candidate.get("source", "html_img"),
                    width=candidate.get("width"),
                    height=candidate.get("height"),
                )
        except Exception:
            continue

    return sorted(candidates, key=_image_candidate_score, reverse=True)


def _image_candidate_score(candidate: dict[str, Any]) -> tuple[int, int]:
    width = candidate.get("width") or 0
    height = candidate.get("height") or 0
    return (IMAGE_SOURCE_PRIORITY.get(candidate.get("source"), 0), width * height)


def _image_extension(content_type: str | None, content: bytes) -> tuple[str, str] | tuple[None, None]:
    mime_type = (content_type or "").split(";", 1)[0].strip().lower()
    if mime_type in IMAGE_CONTENT_TYPES:
        return IMAGE_CONTENT_TYPES[mime_type], mime_type
    if content.startswith(b"\xff\xd8\xff"):
        return ".jpg", "image/jpeg"
    if content.startswith(b"\x89PNG\r\n\x1a\n"):
        return ".png", "image/png"
    if content.startswith((b"GIF87a", b"GIF89a")):
        return ".gif", "image/gif"
    if len(content) >= 12 and content[:4] == b"RIFF" and content[8:12] == b"WEBP":
        return ".webp", "image/webp"
    return None, None


def _is_incomplete_feed_content(text: str, summary: str) -> bool:
    if len(text) < MIN_FULL_TEXT_CHARS:
        return True
    if text == summary:
        return True
    if len(text) <= len(summary) + SUMMARY_MARGIN_CHARS:
        return True
    return False


def _extract_article_text(
    page_text: str,
    url: str,
    *,
    current_text: str,
    summary: str,
) -> tuple[str | None, str | None]:
    candidates: list[tuple[str, str]] = []
    seen_text: set[str] = set()
    for name, options in TRAFILATURA_EXTRACTION_MODES:
        try:
            extracted = trafilatura.extract(page_text, url=url, **options)
        except Exception:
            continue
        text = _clean_text(extracted)
        if text and text not in seen_text:
            candidates.append((name, text))
            seen_text.add(text)
    if not candidates:
        return None, None

    def score(candidate: tuple[str, str]) -> tuple[bool, bool, int]:
        _, text = candidate
        return (
            not _is_incomplete_feed_content(text, summary),
            len(text) > len(current_text),
            len(text),
        )

    name, text = max(candidates, key=score)
    if current_text and len(text) <= len(current_text):
        return None, None
    return text, name


def _article_id(canonical_url: str | None, source_id: str, guid: str | None, fallback_key: str) -> tuple[str, str]:
    if canonical_url:
        return hashlib.sha256(canonical_url.encode("utf-8")).hexdigest(), "canonical_url"
    if guid:
        fallback = f"{source_id}:{guid}"
        return hashlib.sha256(fallback.encode("utf-8")).hexdigest(), "guid"
    fallback = f"{source_id}:{fallback_key}"
    return hashlib.sha256(fallback.encode("utf-8")).hexdigest(), "entry_fingerprint"


def _entry_url_and_identity(feed: FeedConfig, entry: Any) -> tuple[str, str | None, str | None]:
    raw_url = entry.get("link") or entry.get("id") or entry.get("guid")
    if not raw_url:
        raise ValueError("feed entry has no link, id, or guid")
    entry_url = urljoin(feed.feed_url, str(raw_url).strip())
    canonical_url = _canonicalize_url(entry_url) if entry_url.lower().startswith(("http://", "https://")) else None
    guid = entry.get("id") or entry.get("guid")
    return entry_url, canonical_url, guid


def _stable_entry_article_id(feed: FeedConfig, entry: Any) -> str | None:
    _, canonical_url, guid = _entry_url_and_identity(feed, entry)
    if canonical_url or guid:
        article_id, _ = _article_id(canonical_url, feed.source_id, guid, "")
        return article_id
    return None


def _article_path(article_id: str, published_at: str) -> Path:
    date = datetime.fromisoformat(published_at.replace("Z", "+00:00")).date()
    return ARTICLE_DIR / f"{date:%Y}" / f"{date:%m}" / f"{date:%d}" / f"{sanitize_id(article_id)}.json"


def _article_sidecar_paths(article_path: Path) -> list[Path]:
    return [article_path.with_suffix(extension) for extension in IMAGE_CONTENT_TYPES.values()]


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
        max_feed_concurrency: int = 100,
        max_article_concurrency: int = 1000,
        max_article_age_days: int = 3,
        progress: ProgressCallback | None = None,
    ) -> None:
        self.state = state
        self.client = client
        self.feeds = feeds
        self.run_id = run_id
        self.progress = progress
        self.feed_sem = asyncio.Semaphore(max_feed_concurrency)
        self.article_sem = asyncio.Semaphore(max_article_concurrency)
        self.max_article_age_days = max_article_age_days
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
            "images_fetched": 0,
            "images_skipped": 0,
            "images_failed": 0,
        }
        self.last_activity_time = time.time()
        self.active_tasks: dict[str, str] = {}
        self.task_counter = 0
        self.image_origin_failures: dict[str, int] = {}
        self.disabled_image_origins: set[str] = set()

    async def run(self) -> dict[str, int]:
        self._progress(f"collector: syncing {len(self.feeds)} feeds")
        self.state.sync_feeds(self.feeds)
        watchdog_task = asyncio.create_task(self._watchdog())
        try:
            await asyncio.gather(*(self._collect_feed(feed) for feed in self.feeds))
        finally:
            watchdog_task.cancel()
            try:
                await watchdog_task
            except asyncio.CancelledError:
                pass
        log_path = FETCH_LOG_DIR / f"{utc_now():%Y-%m-%d}.jsonl"
        atomic_append_jsonl(log_path, self.log_rows)
        self._progress(f"collector: wrote fetch log {log_path}")
        return self.stats

    def _register_task(self, description: str) -> str:
        self.task_counter += 1
        task_id = f"task-{self.task_counter}"
        self.active_tasks[task_id] = description
        return task_id

    def _deregister_task(self, task_id: str) -> None:
        self.active_tasks.pop(task_id, None)

    async def _watchdog(self) -> None:
        while True:
            await asyncio.sleep(10)
            if time.time() - self.last_activity_time > 30:
                if self.active_tasks:
                    pending = sorted(self.active_tasks.values())
                    if len(pending) > 10:
                        pending_str = ", ".join(pending[:10]) + f" ... and {len(pending) - 10} more"
                    else:
                        pending_str = ", ".join(pending)
                    self._progress(f"watchdog: no output for 30s. pending operations: {pending_str}")
                else:
                    self._progress("watchdog: no output for 30s. no active operations registered.")

    async def _gather_entries(self, feed: FeedConfig, entries: list[Any]) -> None:
        sem = asyncio.Semaphore(5)

        async def run_one(entry: Any) -> None:
            async with sem:
                await self._collect_entry(feed, entry)

        await asyncio.gather(*(run_one(entry) for entry in entries))

    async def _collect_feed(self, feed: FeedConfig) -> None:
        task_id = self._register_task(f"feed:{feed.source_id}")
        try:
            async with self.feed_sem:
                headers = self.state.feed_headers(feed.source_id)
                try:
                    if getattr(feed, "feed_type", "rss") == "scraper":
                        from pipeline.scrapers import run_scraper

                        self._progress(f"feed {feed.source_id}: scraping {feed.feed_url}")
                        entries = await run_scraper(self.client, feed)
                        self.stats["feeds_fetched"] += 1
                        self.state.update_feed_state(feed.source_id, status=200)
                        self._log(feed.source_id, "feed", feed.feed_url, 200, "fetched")
                        self.stats["entries_seen"] += len(entries)
                        self._progress(f"feed {feed.source_id}: fetched {len(entries)} scraper entries")
                        await self._gather_entries(feed, entries)
                        self._progress(f"feed {feed.source_id}: complete")
                        return

                    self._progress(f"feed {feed.source_id}: fetching {feed.feed_url}")
                    response = await self.client.get(feed.feed_url, headers=headers, check_robots=False)
                    if response.status_code == 304:
                        self.stats["feeds_not_modified"] += 1
                        self.state.update_feed_state(feed.source_id, status=304)
                        self._log(feed.source_id, "feed", feed.feed_url, 304, "not_modified")
                        self._progress(f"feed {feed.source_id}: not modified")
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
                    self._progress(f"feed {feed.source_id}: fetched {len(entries)} entries")
                    await self._gather_entries(feed, entries)
                    self._progress(f"feed {feed.source_id}: complete")
                except Exception as exc:
                    self.stats["feeds_failed"] += 1
                    self.state.update_feed_state(feed.source_id, status=None, failed=True)
                    self.state.record_error(self.run_id, "collection", "feed", feed.feed_url, feed.source_id, exc)
                    self._log(feed.source_id, "feed", feed.feed_url, None, "failed", str(exc))
                    self._progress(f"feed {feed.source_id}: failed {type(exc).__name__}: {exc}")
        finally:
            self._deregister_task(task_id)

    async def _collect_entry(self, feed: FeedConfig, entry: Any) -> None:
        entry_label = _short_label(entry.get("title") or entry.get("id") or entry.get("link"))
        task_id = self._register_task(f"article:{feed.source_id}:{entry_label}")
        try:
            async with self.article_sem:
                try:
                    published = _entry_datetime(entry, "published") or _entry_datetime(entry, "updated")
                    if published is not None:
                        if published < utc_now() - timedelta(days=self.max_article_age_days):
                            self.stats["articles_skipped"] += 1
                            self._progress(f"article {feed.source_id}: skipped old {entry_label}")
                            return
                    entry_url, canonical_url, guid = _entry_url_and_identity(feed, entry)
                    preflight_article_id = None
                    if canonical_url or guid:
                        preflight_article_id, _ = _article_id(canonical_url, feed.source_id, guid, "")

                    # Check DB by identity to find if we already have it under any (possibly redirected) URL or GUID
                    db_match = self.state.find_article_by_url_or_guid(
                        entry_url,
                        canonical_url,
                        guid,
                        source_id=feed.source_id,
                    )
                    if db_match:
                        matched_id, matched_path_str = db_match
                        matched_path = PROJECT_ROOT / matched_path_str
                        async with self.article_id_lock:
                            in_seen = matched_id in self.seen_article_ids
                        if in_seen or matched_path.exists():
                            async with self.article_id_lock:
                                if matched_id not in self.seen_article_ids:
                                    self.seen_article_ids.add(matched_id)
                            self.stats["articles_skipped"] += 1
                            self._progress(
                                f"article {feed.source_id}: skipped duplicate "
                                f"(already on disk via DB identity) {entry_label}"
                            )
                            return

                    preflight_reserved = False
                    if preflight_article_id:
                        published_dt = published or utc_now()
                        published_at_str = isoformat_z(published_dt)
                        article_path = _article_path(preflight_article_id, published_at_str)
                        if article_path.exists():
                            async with self.article_id_lock:
                                if preflight_article_id not in self.seen_article_ids:
                                    self.seen_article_ids.add(preflight_article_id)
                                    if not self.state.article_exists(preflight_article_id):
                                        try:
                                            with article_path.open("r", encoding="utf-8") as f:
                                                article_data = json.load(f)
                                            self.state.insert_article(article_data, article_path)
                                            self.stats["articles_written"] += 1
                                            self._progress(
                                                f"article {feed.source_id}: synced existing file "
                                                f"{preflight_article_id} to DB"
                                            )
                                        except Exception as db_exc:
                                            self._progress(
                                                f"article {feed.source_id}: failed to sync existing file "
                                                f"to DB: {db_exc}"
                                            )
                            self.stats["articles_skipped"] += 1
                            self._progress(
                                f"article {feed.source_id}: skipped duplicate (already on disk) {entry_label}"
                            )
                            return

                        async with self.article_id_lock:
                            if preflight_article_id in self.seen_article_ids or self.state.article_exists(
                                preflight_article_id
                            ):
                                self.stats["articles_skipped"] += 1
                                self._progress(f"article {feed.source_id}: skipped duplicate {entry_label}")
                                return
                            self.seen_article_ids.add(preflight_article_id)
                            preflight_reserved = True
                    article, candidates = await self._article_from_entry(feed, entry)
                    article_path = _article_path(article["article_id"], article["published_at"])
                    if article_path.exists():
                        async with self.article_id_lock:
                            if article["article_id"] not in self.seen_article_ids:
                                self.seen_article_ids.add(article["article_id"])
                                if not self.state.article_exists(article["article_id"]):
                                    try:
                                        with article_path.open("r", encoding="utf-8") as f:
                                            article_data = json.load(f)
                                        self.state.insert_article(article_data, article_path)
                                        self.stats["articles_written"] += 1
                                        self._progress(
                                            f"article {feed.source_id}: synced existing file "
                                            f"{article['article_id']} to DB"
                                        )
                                    except Exception as db_exc:
                                        self._progress(
                                            f"article {feed.source_id}: failed to sync existing file to DB: {db_exc}"
                                        )
                        self.stats["articles_skipped"] += 1
                        self._progress(
                            f"article {feed.source_id}: skipped duplicate (already on disk after fetch) {entry_label}"
                        )
                        return

                    async with self.article_id_lock:
                        if article["article_id"] != preflight_article_id:
                            if article["article_id"] in self.seen_article_ids or self.state.article_exists(
                                article["article_id"]
                            ):
                                self.stats["articles_skipped"] += 1
                                self._progress(f"article {feed.source_id}: skipped duplicate {entry_label}")
                                return
                            self.seen_article_ids.add(article["article_id"])
                        elif not preflight_reserved:
                            self.seen_article_ids.add(article["article_id"])
                    image_metadata = await self._fetch_article_image(candidates, article, article_path)
                    if image_metadata:
                        article["image"] = image_metadata
                    try:
                        atomic_write_json(article_path, article)
                    except Exception:
                        if image_metadata:
                            sidecar = PROJECT_ROOT / image_metadata["path"]
                            try:
                                sidecar.unlink(missing_ok=True)
                            except OSError:
                                pass
                        raise
                    try:
                        self.state.insert_article(article, article_path)
                    except Exception:
                        try:
                            article_path.unlink(missing_ok=True)
                        except OSError:
                            pass
                        if image_metadata:
                            sidecar = PROJECT_ROOT / image_metadata["path"]
                            try:
                                sidecar.unlink(missing_ok=True)
                            except OSError:
                                pass
                        raise
                    self.stats["articles_written"] += 1
                    self._progress(f"article {feed.source_id}: written {article['article_id']} {entry_label}")
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
                    self._progress(f"article {feed.source_id}: failed {type(exc).__name__}: {exc}")
        finally:
            self._deregister_task(task_id)

    async def _article_from_entry(self, feed: FeedConfig, entry: Any) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        fetched_at = isoformat_z()
        entry_url, canonical_url, guid = _entry_url_and_identity(feed, entry)
        headline = _clean_text(entry.get("title")) or "(untitled)"
        summary = _clean_text(entry.get("summary") or entry.get("description"))
        content_text, extractor = _feed_content(entry)
        image_candidates = _entry_image_candidates(entry, entry_url)
        status_code: int | None = None
        article_fetch_attempted = False
        extractor_detail: str | None = None
        language = entry.get("language") or None

        if canonical_url and _is_incomplete_feed_content(content_text, summary):
            article_fetch_attempted = True
            try:
                page = await self.client.get(canonical_url)
                status_code = page.status_code
                self._progress(f"article {feed.source_id}: fetched page {canonical_url} status={status_code}")
                if status_code < 400:
                    canonical_url = _canonicalize_url(str(page.url))
                    image_candidates.extend(_html_image_candidates(page.text, canonical_url, "article_meta"))
                    image_candidates = sorted(
                        {candidate["url"]: candidate for candidate in image_candidates}.values(),
                        key=_image_candidate_score,
                        reverse=True,
                    )
                    extracted_text, extraction_mode = _extract_article_text(
                        page.text,
                        canonical_url,
                        current_text=content_text,
                        summary=summary,
                    )
                    if extracted_text:
                        content_text = extracted_text
                        extractor = "readability"
                        extractor_detail = extraction_mode
                    else:
                        self.state.record_error(
                            self.run_id,
                            "collection",
                            "article_extraction_empty",
                            canonical_url,
                            feed.source_id,
                            "trafilatura produced no text longer than the feed content",
                        )
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
                self._progress(
                    f"article {feed.source_id}: failed to fetch page {canonical_url} - {type(exc).__name__}: {exc}"
                )
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
            "summary": summary or _summary_fallback(content_text),
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
                "extractor_detail": extractor_detail,
                "article_fetch_attempted": article_fetch_attempted,
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
        return article, image_candidates

    async def _fetch_article_image(
        self,
        candidates: list[dict[str, Any]],
        article: dict[str, Any],
        article_path: Path,
    ) -> dict[str, Any] | None:
        if not candidates:
            self.stats["images_skipped"] += 1
            return None

        # If we have any explicit primary lead images, discard general body images (html_img)
        has_primary = any(c.get("source") != "html_img" for c in candidates)
        if has_primary:
            candidates = [c for c in candidates if c.get("source") != "html_img"]

        for candidate in candidates:
            image_url = candidate.get("url")
            if not image_url:
                continue

            parsed = urlsplit(image_url)
            origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else None
            if origin and origin in self.disabled_image_origins:
                self._progress(f"image {article['source_id']}: skipping {image_url} - origin {origin} is disabled")
                self.stats["images_skipped"] += 1
                if has_primary:
                    break
                continue

            task_id = self._register_task(f"image:{article['source_id']}:{image_url}")
            try:
                response = await self.client.get(image_url)
                self._progress(f"image {article['source_id']}: fetched {image_url} status={response.status_code}")
                if response.status_code >= 400:
                    self.state.record_error(
                        self.run_id,
                        "collection",
                        "image_fetch_http_error",
                        image_url,
                        article["source_id"],
                        f"HTTP status {response.status_code}",
                    )
                    if origin:
                        self.image_origin_failures[origin] = self.image_origin_failures.get(origin, 0) + 1
                        if self.image_origin_failures[origin] >= 3:
                            self.disabled_image_origins.add(origin)
                            self._progress(
                                f"image {article['source_id']}: disabled origin {origin} after "
                                f"{self.image_origin_failures[origin]} consecutive failures"
                            )
                    if has_primary:
                        break
                    continue
                content = response.content
                if len(content) > MAX_IMAGE_BYTES:
                    raise ValueError(f"image exceeds {MAX_IMAGE_BYTES} byte cap")
                extension, content_type = _image_extension(response.headers.get("content-type"), content)
                if not extension or not content_type:
                    raise ValueError(f"unsupported image content type: {response.headers.get('content-type')}")

                image_path = article_path.with_suffix(extension)
                atomic_write_bytes(image_path, content)
                self.stats["images_fetched"] += 1
                self._progress(f"image {article['source_id']}: saved {image_path.name}")
                if origin:
                    self.image_origin_failures[origin] = 0
                return {
                    "url": image_url,
                    "path": _relative_to_project(image_path),
                    "content_type": content_type,
                    "bytes": len(content),
                    "source": candidate.get("source"),
                    "width": candidate.get("width"),
                    "height": candidate.get("height"),
                }
            except Exception as exc:
                self._progress(
                    f"image {article['source_id']}: failed to fetch {image_url} - {type(exc).__name__}: {exc}"
                )
                self.state.record_error(
                    self.run_id,
                    "collection",
                    "image_fetch_exception",
                    image_url,
                    article["source_id"],
                    exc,
                )
                if origin:
                    self.image_origin_failures[origin] = self.image_origin_failures.get(origin, 0) + 1
                    if self.image_origin_failures[origin] >= 3:
                        self.disabled_image_origins.add(origin)
                        self._progress(
                            f"image {article['source_id']}: disabled origin {origin} after "
                            f"{self.image_origin_failures[origin]} consecutive failures"
                        )
                if has_primary:
                    break
            finally:
                self._deregister_task(task_id)

        self.stats["images_failed"] += 1
        self._progress(f"image {article['source_id']}: failed {article['article_id']}")
        return None

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

    def _progress(self, message: str) -> None:
        self.last_activity_time = time.time()
        if self.progress:
            self.progress(message)


def cleanup_old_staging_data(days: int = 3, db_path: Path = DB_PATH) -> None:
    threshold = isoformat_z(utc_now() - timedelta(days=days))

    with StateDB(db_path) as state:
        rows = state.conn.execute(
            """
            SELECT articles.article_id, articles.article_path
            FROM articles
            JOIN events ON events.event_id = articles.event_id
            WHERE articles.published_at < ? AND events.status = 'archived'
            """,
            (threshold,),
        ).fetchall()

        for row in rows:
            art_path_rel = row["article_path"]
            art_path = PROJECT_ROOT / art_path_rel
            try:
                if art_path.exists():
                    art_path.unlink()
                for sidecar_path in _article_sidecar_paths(art_path):
                    if sidecar_path.exists():
                        sidecar_path.unlink()
            except Exception as e:
                state.record_error("cleanup", "cleanup", "article_delete", str(art_path_rel), None, e)

    def remove_empty_dirs(path: Path):
        for child in list(path.iterdir()):
            if child.is_dir():
                remove_empty_dirs(child)
        if path.is_dir() and not list(path.iterdir()) and path != ARTICLE_DIR:
            try:
                path.rmdir()
            except OSError:
                pass

    if ARTICLE_DIR.exists():
        remove_empty_dirs(ARTICLE_DIR)


@asynccontextmanager
async def _noop_async_context():
    yield


async def collect_once(
    progress: ProgressCallback | None = None,
    *,
    acquire_lock: bool = True,
) -> dict[str, int]:
    pipeline_config = load_pipeline_config()
    feeds = load_feeds(enabled_only=True)
    collection = pipeline_config.collection
    retention_days = int(pipeline_config.retention.get("staging_article_days", 3))
    run_id = f"collection-{utc_now():%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    lock_timeout = pipeline_config.pipeline.get("watchdog_timeout_minutes", 30)
    if progress:
        progress(f"collect: starting {run_id} feeds={len(feeds)} retention_days={retention_days}")
    lock_context = (
        PipelineLock(LOCK_PATH, _minutes_to_timedelta(lock_timeout), run_id=run_id)
        if acquire_lock
        else _noop_async_context()
    )
    async with lock_context:
        if progress:
            progress("collect: acquired pipeline lock" if acquire_lock else "collect: using existing pipeline lock")
            progress("collect: migrating schema")
        migrate()
        if progress:
            progress("collect: cleaning old staging data")
        cleanup_old_staging_data(days=retention_days)
        with StateDB() as state:
            state.start_run(run_id, "collection")
            status = "success"
            stats: dict[str, int] = {}
            async with PoliteHTTPClient(
                rate_limit_seconds=float(collection.get("rate_limit_seconds", 2)),
                connection_timeout_seconds=float(collection.get("connection_timeout_seconds", 10)),
                read_timeout_seconds=float(collection.get("read_timeout_seconds", 10)),
                total_timeout_seconds=float(collection.get("total_timeout_seconds", 15)),
                max_retries=int(collection.get("max_retries", 3)),
                backoff_initial_seconds=float(collection.get("backoff_initial_seconds", 5)),
                backoff_max_seconds=float(collection.get("backoff_max_seconds", 60)),
                progress=progress,
            ) as client:
                collector = Collector(
                    state,
                    client,
                    feeds,
                    run_id=run_id,
                    max_article_age_days=retention_days,
                    progress=progress,
                )
                try:
                    stats = await collector.run()
                except Exception:
                    status = "failed"
                    if progress:
                        progress(f"collect: failed {run_id}")
                    raise
                finally:
                    try:
                        state.finish_run(run_id, status, stats)
                    except Exception as finish_exc:
                        if progress:
                            progress(f"collect: finish_run failed: {finish_exc}")
                        if status == "success":
                            raise
                    if progress:
                        progress(f"collect: {status} {json.dumps(stats, sort_keys=True)}")
                return stats


def _minutes_to_timedelta(minutes: float) -> timedelta:
    return timedelta(minutes=float(minutes))
