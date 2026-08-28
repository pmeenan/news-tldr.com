from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest

from pipeline.collect import Collector, cleanup_old_staging_data
from pipeline.config import FeedConfig, PipelineConfig
from pipeline.http_client import PoliteHTTPClient
from pipeline.security import ResolvedURL
from pipeline.state import StateDB, migrate


@pytest.fixture(autouse=True)
def _stable_collection_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep fixed May 2026 feed fixtures inside the collector age horizon."""
    monkeypatch.setattr(
        "pipeline.collect.utc_now",
        lambda: datetime(2026, 6, 1, 13, 0, tzinfo=UTC),
    )

FEED_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Fixture</title>
    <item>
      <guid>fixture-1</guid>
      <title>Fixture headline</title>
      <link>https://example.test/story</link>
      <description>Short summary.</description>
      <pubDate>Sun, 31 May 2026 12:30:00 GMT</pubDate>
      <category>technology</category>
    </item>
  </channel>
</rss>
"""

FULL_FEED_XML = (
    b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">
  <channel>
    <title>Fixture</title>
    <item>
      <guid>fixture-1</guid>
      <title>Fixture headline</title>
      <link>https://example.test/story</link>
      <description>Short summary.</description>
      <content:encoded><![CDATA[
        <p>This is complete feed article text.</p>
        <p>"""
    + (b"Full article sentence. " * 40)
    + b"""</p>
      ]]></content:encoded>
      <pubDate>Sun, 31 May 2026 12:30:00 GMT</pubDate>
      <category>technology</category>
    </item>
  </channel>
</rss>
"""
)

IMAGE_FEED_XML = (
    b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"
     xmlns:content="http://purl.org/rss/1.0/modules/content/"
     xmlns:media="http://search.yahoo.com/mrss/">
  <channel>
    <title>Fixture</title>
    <item>
      <guid>fixture-image-1</guid>
      <title>Fixture image headline</title>
      <link>https://example.test/image-story</link>
      <description>Short summary.</description>
      <content:encoded><![CDATA[
        <p>This is complete feed article text.</p>
        <p>"""
    + (b"Full article sentence. " * 40)
    + b"""</p>
      ]]></content:encoded>
      <media:content url="https://example.test/images/story.jpg" medium="image"
                     type="image/jpeg" width="1200" height="800" />
      <pubDate>Sun, 31 May 2026 12:30:00 GMT</pubDate>
      <category>technology</category>
    </item>
  </channel>
</rss>
"""
)


@pytest.mark.asyncio
async def test_collects_feed_entry_to_article_json(tmp_path, monkeypatch):
    db_path = tmp_path / "pipeline.db"
    article_dir = tmp_path / "articles"
    log_dir = tmp_path / "fetch-log"
    migrate(db_path)
    monkeypatch.setattr("pipeline.collect.ARTICLE_DIR", article_dir)
    monkeypatch.setattr("pipeline.collect.FETCH_LOG_DIR", log_dir)

    async def fake_validate(url: str) -> ResolvedURL:
        return ResolvedURL(url=url, hostname=httpx.URL(url).host or "example.test", port=443, ips=("93.184.216.34",))

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        if request.url.path == "/feed.xml":
            return httpx.Response(200, content=FEED_XML, headers={"etag": '"abc"'})
        if request.url.path == "/story":
            return httpx.Response(
                200,
                text=(
                    "<html><body><article><h1>Fixture headline</h1>"
                    "<p>This is the full article body with enough detail for extraction.</p>"
                    "</article></body></html>"
                ),
            )
        return httpx.Response(404)

    monkeypatch.setattr("pipeline.http_client.validate_url", fake_validate)
    transport = httpx.MockTransport(handler)
    feed = FeedConfig(
        source_id="fixture",
        source_name="Fixture News",
        feed_url="https://example.test/feed.xml",
        site_url="https://example.test",
        enabled=True,
        default_category="technology",
        category_hints=["technology"],
        content_hints={"default_content_type": "news", "paywall": "none"},
        fetch={},
    )
    progress_messages: list[str] = []
    async with PoliteHTTPClient(rate_limit_seconds=0, max_retries=0, transport=transport) as client:
        with StateDB(db_path) as state:
            collector = Collector(state, client, [feed], run_id="test-run", progress=progress_messages.append)
            stats = await collector.run()

            rows = state.conn.execute(
                "SELECT article_id, headline, article_path, collection_run_id, collection_json FROM articles"
            ).fetchall()
            source_stats = state.conn.execute(
                "SELECT * FROM source_run_stats WHERE run_id = ? AND source_id = ?",
                ("test-run", "fixture"),
            ).fetchone()

    assert stats["articles_written"] == 1
    assert rows[0]["headline"] == "Fixture headline"
    assert rows[0]["collection_run_id"] == "test-run"
    assert json.loads(rows[0]["collection_json"])["run_id"] == "test-run"
    assert source_stats["feed_status"] == "fetched"
    assert source_stats["feed_http_status"] == 200
    assert source_stats["entries_seen"] == 1
    assert source_stats["articles_written"] == 1
    assert source_stats["images_fetched"] == 0
    assert source_stats["images_skipped"] == 0
    assert source_stats["images_failed"] == 0
    assert source_stats["error_count"] == 0
    assert article_dir.glob("*")
    assert list(article_dir.rglob("*.json"))
    assert list(log_dir.glob("*.jsonl"))
    assert any(message.startswith("feed fixture: fetching ") for message in progress_messages)
    assert any(message.startswith("feed fixture: fetched 1 entries") for message in progress_messages)
    assert any(message.startswith("article fixture: written ") for message in progress_messages)


def test_feed_parser_rejects_dtd():
    from pipeline.collect import _parse_feed_bytes

    with pytest.raises(ValueError):
        _parse_feed_bytes(b'<?xml version="1.0"?><!DOCTYPE foo><rss></rss>')


def test_feed_parser_rejects_dtd_after_large_prefix():
    from pipeline.collect import _parse_feed_bytes

    with pytest.raises(ValueError):
        _parse_feed_bytes(
            b"<?xml version='1.0'?>" + b" " * 4096 + b"<!ENTITY xxe SYSTEM 'file:///etc/passwd'><rss></rss>"
        )


def test_feed_parser_rejects_element_declaration():
    from pipeline.collect import _parse_feed_bytes

    with pytest.raises(ValueError):
        _parse_feed_bytes(b"<?xml version='1.0'?><!ELEMENT rss ANY><rss></rss>")


def test_feed_parser_rejects_dtd_utf16():
    from pipeline.collect import _parse_feed_bytes

    content_be = '<?xml version="1.0" encoding="utf-16"?><!DOCTYPE foo><rss></rss>'.encode("utf-16be")
    content_le = '<?xml version="1.0" encoding="utf-16"?><!DOCTYPE foo><rss></rss>'.encode("utf-16le")

    with pytest.raises(ValueError):
        _parse_feed_bytes(content_be)
    with pytest.raises(ValueError):
        _parse_feed_bytes(content_le)


def test_canonicalize_url_sorts_query_and_drops_tracking():
    from pipeline.collect import _canonicalize_url

    assert _canonicalize_url("https://EXAMPLE.test/a?b=2&a=1") == "https://example.test/a?a=1&b=2"
    assert _canonicalize_url("https://example.test/x?utm_source=foo&z=3&a=1") == "https://example.test/x?a=1&z=3"
    # Same article URL with reordered + extra tracking params canonicalizes identically.
    a = _canonicalize_url("https://example.test/p?id=7&fbclid=xyz&order=asc")
    b = _canonicalize_url("https://example.test/p?order=asc&id=7")
    assert a == b


def test_extract_article_text_prefers_recall_candidate(monkeypatch):
    from pipeline.collect import _extract_article_text

    def fake_extract(page_text: str, url: str, **kwargs):
        if kwargs.get("favor_recall"):
            return " ".join(["full article body"] * 80)
        if kwargs.get("favor_precision"):
            return "short precision result"
        return " ".join(["medium article body"] * 30)

    monkeypatch.setattr("pipeline.collect.trafilatura.extract", fake_extract)

    text, mode = _extract_article_text(
        "<html></html>",
        "https://example.test/story",
        current_text="Short summary.",
        summary="Short summary.",
    )

    assert mode == "trafilatura_recall"
    assert text is not None
    assert len(text) > 600


def test_extract_article_text_keeps_longer_feed_content(monkeypatch):
    from pipeline.collect import _extract_article_text

    monkeypatch.setattr("pipeline.collect.trafilatura.extract", lambda *args, **kwargs: "short extraction")

    text, mode = _extract_article_text(
        "<html></html>",
        "https://example.test/story",
        current_text=" ".join(["feed article body"] * 80),
        summary="Short summary.",
    )

    assert text is None
    assert mode is None


@pytest.mark.asyncio
async def test_collect_bypasses_robots_for_configured_feed(tmp_path, monkeypatch):
    db_path = tmp_path / "pipeline.db"
    article_dir = tmp_path / "articles"
    log_dir = tmp_path / "fetch-log"
    migrate(db_path)
    monkeypatch.setattr("pipeline.collect.ARTICLE_DIR", article_dir)
    monkeypatch.setattr("pipeline.collect.FETCH_LOG_DIR", log_dir)

    async def fake_validate(url: str) -> ResolvedURL:
        return ResolvedURL(url=url, hostname=httpx.URL(url).host or "example.test", port=443, ips=("93.184.216.34",))

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(200, text="User-agent: *\nDisallow: /\n")
        if request.url.path == "/feed.xml":
            return httpx.Response(200, content=FULL_FEED_XML)
        return httpx.Response(404)

    monkeypatch.setattr("pipeline.http_client.validate_url", fake_validate)
    transport = httpx.MockTransport(handler)
    feed = FeedConfig(
        source_id="fixture",
        source_name="Fixture News",
        feed_url="https://example.test/feed.xml",
        site_url="https://example.test",
        enabled=True,
        default_category="technology",
        category_hints=["technology"],
        content_hints={"default_content_type": "news", "paywall": "none"},
        fetch={},
    )
    async with PoliteHTTPClient(rate_limit_seconds=0, max_retries=0, transport=transport) as client:
        with StateDB(db_path) as state:
            collector = Collector(state, client, [feed], run_id="test-run")
            stats = await collector.run()

    assert stats["feeds_fetched"] == 1
    assert stats["articles_written"] == 1


@pytest.mark.asyncio
async def test_collect_ignores_feed_image_without_downloading(tmp_path, monkeypatch):
    db_path = tmp_path / "pipeline.db"
    article_dir = tmp_path / "articles"
    log_dir = tmp_path / "fetch-log"
    migrate(db_path)
    monkeypatch.setattr("pipeline.collect.ARTICLE_DIR", article_dir)
    monkeypatch.setattr("pipeline.collect.FETCH_LOG_DIR", log_dir)

    async def fake_validate(url: str) -> ResolvedURL:
        return ResolvedURL(url=url, hostname=httpx.URL(url).host or "example.test", port=443, ips=("93.184.216.34",))

    image_requested = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal image_requested
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        if request.url.path == "/feed.xml":
            return httpx.Response(200, content=IMAGE_FEED_XML)
        if request.url.path == "/images/story.jpg":
            image_requested = True
            return httpx.Response(500)
        return httpx.Response(404)

    monkeypatch.setattr("pipeline.http_client.validate_url", fake_validate)
    feed = FeedConfig(
        source_id="fixture",
        source_name="Fixture News",
        feed_url="https://example.test/feed.xml",
        site_url="https://example.test",
        enabled=True,
        default_category="technology",
        category_hints=["technology"],
        content_hints={"default_content_type": "news", "paywall": "none"},
        fetch={},
    )
    async with PoliteHTTPClient(rate_limit_seconds=0, max_retries=0, transport=httpx.MockTransport(handler)) as client:
        with StateDB(db_path) as state:
            collector = Collector(state, client, [feed], run_id="test-run")
            stats = await collector.run()

    article_path = next(article_dir.rglob("*.json"))
    image_path = article_path.with_suffix(".jpg")
    article = json.loads(article_path.read_text(encoding="utf-8"))

    assert stats["articles_written"] == 1
    assert stats["images_fetched"] == 0
    assert stats["images_skipped"] == 0
    assert stats["images_failed"] == 0
    assert not image_requested
    assert not image_path.exists()
    assert "image" not in article


@pytest.mark.asyncio
async def test_existing_url_article_skips_page_fetch_before_extraction(tmp_path, monkeypatch):
    db_path = tmp_path / "pipeline.db"
    article_dir = tmp_path / "articles"
    log_dir = tmp_path / "fetch-log"
    migrate(db_path)
    monkeypatch.setattr("pipeline.collect.ARTICLE_DIR", article_dir)
    monkeypatch.setattr("pipeline.collect.FETCH_LOG_DIR", log_dir)

    async def fake_validate(url: str) -> ResolvedURL:
        return ResolvedURL(url=url, hostname=httpx.URL(url).host or "example.test", port=443, ips=("93.184.216.34",))

    page_fetches = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal page_fetches
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        if request.url.path == "/feed.xml":
            return httpx.Response(200, content=FEED_XML)
        if request.url.path == "/story":
            page_fetches += 1
            return httpx.Response(200, text="<article>Already known article</article>")
        return httpx.Response(404)

    monkeypatch.setattr("pipeline.http_client.validate_url", fake_validate)
    canonical_url = "https://example.test/story"
    existing_article_id = hashlib.sha256(canonical_url.encode("utf-8")).hexdigest()
    with StateDB(db_path) as state:
        state.insert_article(
            {
                "article_id": existing_article_id,
                "source_id": "fixture",
                "source_name": "Fixture News",
                "url": canonical_url,
                "canonical_url": canonical_url,
                "guid": "fixture-1",
                "headline": "Fixture headline",
                "summary": "Short summary.",
                "content_text": "Existing article body",
                "published_at": "2026-05-31T12:30:00Z",
                "publish_date_estimated": False,
                "fetched_at": "2026-05-31T12:31:00Z",
                "authors": [],
                "tags": [],
                "paywall": {"status": "none", "signals": []},
                "content_type": "news",
                "language": "en",
                "collection": {},
                "fingerprints": {},
            },
            article_dir / "existing.json",
        )

    feed = FeedConfig(
        source_id="fixture",
        source_name="Fixture News",
        feed_url="https://example.test/feed.xml",
        site_url="https://example.test",
        enabled=True,
        default_category="technology",
        category_hints=["technology"],
        content_hints={"default_content_type": "news", "paywall": "none"},
        fetch={},
    )
    async with PoliteHTTPClient(rate_limit_seconds=0, max_retries=0, transport=httpx.MockTransport(handler)) as client:
        with StateDB(db_path) as state:
            collector = Collector(state, client, [feed], run_id="test-run")
            stats = await collector.run()

    assert stats["articles_skipped"] == 1
    assert stats["articles_written"] == 0
    assert page_fetches == 0


@pytest.mark.asyncio
async def test_scraper_feed_updates_state_and_logs_success(tmp_path, monkeypatch):
    db_path = tmp_path / "pipeline.db"
    article_dir = tmp_path / "articles"
    log_dir = tmp_path / "fetch-log"
    migrate(db_path)
    monkeypatch.setattr("pipeline.collect.ARTICLE_DIR", article_dir)
    monkeypatch.setattr("pipeline.collect.FETCH_LOG_DIR", log_dir)

    async def fake_scraper(client: PoliteHTTPClient, feed: FeedConfig):
        return [
            {
                "id": "https://example.test/scraped-story",
                "link": "https://example.test/scraped-story",
                "title": "Scraped headline",
                "summary": "Scraped summary.",
                "content": [{"value": " ".join(["Complete scraped article."] * 80)}],
                "published": "2026-05-31T12:30:00Z",
            }
        ]

    monkeypatch.setattr("pipeline.scrapers.run_scraper", fake_scraper)
    feed = FeedConfig(
        source_id="scraper-fixture",
        source_name="Scraper Fixture",
        feed_url="https://example.test/",
        site_url="https://example.test",
        enabled=True,
        default_category="technology",
        category_hints=["technology"],
        content_hints={"default_content_type": "news", "paywall": "none"},
        fetch={"scraper_module": "unused"},
        feed_type="scraper",
    )
    transport = httpx.MockTransport(lambda _: httpx.Response(404))
    async with PoliteHTTPClient(rate_limit_seconds=0, max_retries=0, transport=transport) as client:
        with StateDB(db_path) as state:
            collector = Collector(state, client, [feed], run_id="test-run")
            stats = await collector.run()
            feed_state = state.conn.execute(
                "SELECT last_status, consecutive_failures FROM feed_state WHERE source_id = ?",
                (feed.source_id,),
            ).fetchone()
            source_stats = state.conn.execute(
                "SELECT feed_status, feed_http_status, entries_seen, articles_written FROM source_run_stats "
                "WHERE source_id = ?",
                (feed.source_id,),
            ).fetchone()

    log_rows = [json.loads(line) for line in next(log_dir.glob("*.jsonl")).read_text(encoding="utf-8").splitlines()]

    assert stats["feeds_fetched"] == 1
    assert stats["articles_written"] == 1
    assert feed_state["last_status"] == 200
    assert feed_state["consecutive_failures"] == 0
    assert dict(source_stats) == {
        "feed_status": "fetched",
        "feed_http_status": 200,
        "entries_seen": 1,
        "articles_written": 1,
    }
    assert any(row["item_type"] == "feed" and row["status"] == "fetched" for row in log_rows)


@pytest.mark.asyncio
async def test_ap_scraper_bypasses_robots_for_configured_homepage(monkeypatch):
    from pipeline.scrapers.ap import scrape

    async def fake_validate(url: str) -> ResolvedURL:
        return ResolvedURL(url=url, hostname=httpx.URL(url).host or "example.test", port=443, ips=("93.184.216.34",))

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(200, text="User-agent: *\nDisallow: /\n")
        if request.url.path == "/":
            return httpx.Response(
                200,
                text=(
                    '<div class="card">'
                    '<a href="/promo">Sign in to continue</a>'
                    '<h2><a href="/article/story-id">Useful AP headline</a></h2>'
                    "</div>"
                ),
            )
        return httpx.Response(404)

    monkeypatch.setattr("pipeline.http_client.validate_url", fake_validate)
    feed = FeedConfig(
        source_id="ap-news",
        source_name="Associated Press",
        feed_url="https://example.test/",
        site_url="https://example.test",
        enabled=True,
        default_category="world",
        category_hints=["world"],
        content_hints={"default_content_type": "news", "paywall": "none"},
        fetch={},
        feed_type="scraper",
    )
    async with PoliteHTTPClient(
        rate_limit_seconds=0,
        max_retries=0,
        transport=httpx.MockTransport(handler),
    ) as client:
        entries = await scrape(client, feed)

    # The /promo anchor is filtered (no headline ancestor / no /article/ path),
    # and only the headline-wrapped /article/ link is kept.
    assert entries == [
        {
            "id": "https://example.test/article/story-id",
            "link": "https://example.test/article/story-id",
            "title": "Useful AP headline",
            "image_url": None,
            "summary": "",
            "published": None,
        }
    ]


@pytest.mark.asyncio
async def test_motortrend_scraper_extracts_heading_and_date(monkeypatch):
    from pipeline.scrapers.motortrend import scrape

    async def fake_validate(url: str) -> ResolvedURL:
        return ResolvedURL(url=url, hostname=httpx.URL(url).host or "example.test", port=443, ips=("93.184.216.34",))

    html = """
    <a href="/reviews/first-drive-example">
      <img src="/images/drive.jpg" alt="Driven: Clean Headline">
      <h2>Driven: Clean Headline</h2>
      <p>Reporter Name | May 21, 2026</p>
    </a>
    """

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/":
            return httpx.Response(200, text=html)
        return httpx.Response(404)

    monkeypatch.setattr("pipeline.http_client.validate_url", fake_validate)
    feed = FeedConfig(
        source_id="motortrend",
        source_name="MotorTrend",
        feed_url="https://example.test/",
        site_url="https://example.test",
        enabled=True,
        default_category="automotive",
        category_hints=["automotive"],
        content_hints={"default_content_type": "news", "paywall": "none"},
        fetch={},
        feed_type="scraper",
    )
    async with PoliteHTTPClient(
        rate_limit_seconds=0,
        max_retries=0,
        transport=httpx.MockTransport(handler),
    ) as client:
        entries = await scrape(client, feed)

    assert entries[0]["title"] == "Driven: Clean Headline"
    assert entries[0]["published"] == "2026-05-21T00:00:00Z"
    assert entries[0]["image_url"] == "https://example.test/images/drive.jpg"


def test_cleanup_preserves_article_rows_and_non_archived_files(tmp_path, monkeypatch):
    db_path = tmp_path / "pipeline.db"
    article_dir = tmp_path / "articles"
    migrate(db_path)
    monkeypatch.setattr("pipeline.collect.ARTICLE_DIR", article_dir)
    article_dir.mkdir()
    unprocessed_path = article_dir / "old-unprocessed.json"
    active_path = article_dir / "old-active.json"
    archived_path = article_dir / "old-archived.json"
    archived_image_path = archived_path.with_suffix(".jpg")
    unprocessed_path.write_text("{}", encoding="utf-8")
    active_path.write_text("{}", encoding="utf-8")
    archived_path.write_text("{}", encoding="utf-8")
    archived_image_path.write_bytes(b"\xff\xd8\xff")

    with StateDB(db_path) as state:
        state.conn.executemany(
            """
            INSERT INTO events (
              event_id, title, category, status, created_at, updated_at
            )
            VALUES (?, 'Event', 'world', ?, '2026-05-01T00:00:00Z', '2026-05-01T00:00:00Z')
            """,
            [
                ("active-event", "active"),
                ("archived-event", "archived"),
            ],
        )
        state.conn.executemany(
            """
            INSERT INTO articles (
              article_id, source_id, source_name, url, headline, published_at,
              publish_date_estimated, fetched_at, article_path, content_type,
              event_id, collection_json
            )
            VALUES (?, 'fixture', 'Fixture', 'https://example.test/story', 'Headline',
                    '2026-05-01T00:00:00Z', 0, '2026-05-01T00:00:00Z', ?,
                    'news', ?, '{}')
            """,
            [
                ("old-unprocessed", str(unprocessed_path), None),
                ("old-active", str(active_path), "active-event"),
                ("old-archived", str(archived_path), "archived-event"),
            ],
        )
        state.conn.commit()

    cleanup_old_staging_data(days=7, db_path=db_path)

    with StateDB(db_path) as state:
        rows = state.conn.execute("SELECT article_id FROM articles ORDER BY article_id").fetchall()

    assert [row["article_id"] for row in rows] == ["old-active", "old-archived", "old-unprocessed"]
    assert unprocessed_path.exists()
    assert active_path.exists()
    assert not archived_path.exists()
    assert not archived_image_path.exists()


@pytest.mark.asyncio
async def test_collects_feed_entry_with_failed_article_fetch_fallback(tmp_path, monkeypatch):
    db_path = tmp_path / "pipeline.db"
    article_dir = tmp_path / "articles"
    log_dir = tmp_path / "fetch-log"
    migrate(db_path)
    monkeypatch.setattr("pipeline.collect.ARTICLE_DIR", article_dir)
    monkeypatch.setattr("pipeline.collect.FETCH_LOG_DIR", log_dir)

    async def fake_validate(url: str) -> ResolvedURL:
        return ResolvedURL(url=url, hostname=httpx.URL(url).host or "example.test", port=443, ips=("93.184.216.34",))

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        if request.url.path == "/feed.xml":
            return httpx.Response(200, content=FEED_XML, headers={"etag": '"abc"'})
        if request.url.path == "/story":
            return httpx.Response(500, text="Internal Server Error")
        return httpx.Response(404)

    monkeypatch.setattr("pipeline.http_client.validate_url", fake_validate)
    transport = httpx.MockTransport(handler)
    feed = FeedConfig(
        source_id="fixture",
        source_name="Fixture News",
        feed_url="https://example.test/feed.xml",
        site_url="https://example.test",
        enabled=True,
        default_category="technology",
        category_hints=["technology"],
        content_hints={"default_content_type": "news", "paywall": "none"},
        fetch={},
    )
    async with PoliteHTTPClient(rate_limit_seconds=0, max_retries=0, transport=transport) as client:
        with StateDB(db_path) as state:
            collector = Collector(state, client, [feed], run_id="test-run")
            stats = await collector.run()

            rows = state.conn.execute("SELECT article_id, headline, summary FROM articles").fetchall()
            errors = state.conn.execute("SELECT item_type, error_type, error_message FROM item_errors").fetchall()
            source_stats = state.conn.execute(
                "SELECT error_count, articles_written FROM source_run_stats WHERE source_id = ?",
                ("fixture",),
            ).fetchone()

    assert stats["articles_written"] == 1
    assert rows[0]["headline"] == "Fixture headline"
    assert rows[0]["summary"] == "Short summary."
    assert len(errors) == 1
    assert errors[0]["item_type"] == "article_fetch_http_error"
    assert "500" in errors[0]["error_message"]
    assert source_stats["error_count"] == 1
    assert source_stats["articles_written"] == 1


@pytest.mark.asyncio
async def test_collect_unlinks_files_if_db_insert_fails(tmp_path, monkeypatch):
    import sqlite3

    db_path = tmp_path / "pipeline.db"
    article_dir = tmp_path / "articles"
    log_dir = tmp_path / "fetch-log"
    migrate(db_path)
    monkeypatch.setattr("pipeline.collect.ARTICLE_DIR", article_dir)
    monkeypatch.setattr("pipeline.collect.FETCH_LOG_DIR", log_dir)

    async def fake_validate(url: str) -> ResolvedURL:
        return ResolvedURL(url=url, hostname=httpx.URL(url).host or "example.test", port=443, ips=("93.184.216.34",))

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        if request.url.path == "/feed.xml":
            return httpx.Response(200, content=IMAGE_FEED_XML)
        return httpx.Response(404)

    monkeypatch.setattr("pipeline.http_client.validate_url", fake_validate)

    # Mock insert_article to fail
    def fake_insert(self_obj, article, article_path):
        raise sqlite3.Error("Mock database error")

    monkeypatch.setattr(StateDB, "insert_article", fake_insert)

    feed = FeedConfig(
        source_id="fixture",
        source_name="Fixture News",
        feed_url="https://example.test/feed.xml",
        site_url="https://example.test",
        enabled=True,
        default_category="technology",
        category_hints=["technology"],
        content_hints={"default_content_type": "news", "paywall": "none"},
        fetch={},
    )
    async with PoliteHTTPClient(rate_limit_seconds=0, max_retries=0, transport=httpx.MockTransport(handler)) as client:
        with StateDB(db_path) as state:
            collector = Collector(state, client, [feed], run_id="test-run")
            stats = await collector.run()

    # The collector handles the exception and records it as an article failure
    assert stats["articles_failed"] == 1
    assert stats["articles_written"] == 0
    # Files should have been unlinked
    assert not list(article_dir.rglob("*.json"))
    assert not list(article_dir.rglob("*.jpg"))


@pytest.mark.asyncio
async def test_scraper_site_url_fallback_to_feed_url(monkeypatch):
    from pipeline.scrapers.ap import scrape as ap_scrape
    from pipeline.scrapers.motortrend import scrape as mt_scrape

    async def fake_validate(url: str) -> ResolvedURL:
        return ResolvedURL(url=url, hostname=httpx.URL(url).host or "example.test", port=443, ips=("93.184.216.34",))

    monkeypatch.setattr("pipeline.http_client.validate_url", fake_validate)

    def ap_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/":
            return httpx.Response(
                200,
                text='<h2><a href="/article/story-id">Useful AP headline</a></h2>',
            )
        return httpx.Response(404)

    # ap_feed has site_url=None, but feed_url="https://example.test/"
    ap_feed = FeedConfig(
        source_id="ap-news",
        source_name="Associated Press",
        feed_url="https://example.test/",
        site_url=None,
        enabled=True,
        default_category="world",
        category_hints=["world"],
        content_hints={"default_content_type": "news", "paywall": "none"},
        fetch={},
        feed_type="scraper",
    )

    async with PoliteHTTPClient(
        rate_limit_seconds=0,
        max_retries=0,
        transport=httpx.MockTransport(ap_handler),
    ) as client:
        ap_entries = await ap_scrape(client, ap_feed)
    assert len(ap_entries) == 1

    def mt_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/":
            return httpx.Response(
                200,
                text='<a href="/reviews/first-drive"><h2>Driven: Clean Headline</h2><p>May 21, 2026</p></a>',
            )
        return httpx.Response(404)

    # mt_feed has site_url=None, but feed_url="https://example.test/"
    mt_feed = FeedConfig(
        source_id="motortrend",
        source_name="MotorTrend",
        feed_url="https://example.test/",
        site_url=None,
        enabled=True,
        default_category="automotive",
        category_hints=["automotive"],
        content_hints={"default_content_type": "news", "paywall": "none"},
        fetch={},
        feed_type="scraper",
    )

    async with PoliteHTTPClient(
        rate_limit_seconds=0,
        max_retries=0,
        transport=httpx.MockTransport(mt_handler),
    ) as client:
        mt_entries = await mt_scrape(client, mt_feed)
    assert len(mt_entries) == 1


def test_load_feeds_validates_missing_keys(tmp_path):
    from pipeline.config import load_feeds

    # Test missing source_id
    config_path = tmp_path / "feeds_missing_id.json"
    config_path.write_text(
        json.dumps({"feeds": [{"source_name": "No ID Feed", "feed_url": "https://example.test/feed.xml"}]})
    )
    with pytest.raises(ValueError, match="missing source_id"):
        load_feeds(config_path)

    # Test missing feed_url
    config_path = tmp_path / "feeds_missing_url.json"
    config_path.write_text(json.dumps({"feeds": [{"source_id": "no-url-feed", "source_name": "No URL Feed"}]}))
    with pytest.raises(ValueError, match="missing feed_url"):
        load_feeds(config_path)


@pytest.mark.asyncio
async def test_collect_skips_fetch_if_file_exists_on_disk(tmp_path, monkeypatch):
    db_path = tmp_path / "pipeline.db"
    article_dir = tmp_path / "articles"
    log_dir = tmp_path / "fetch-log"
    migrate(db_path)
    monkeypatch.setattr("pipeline.collect.ARTICLE_DIR", article_dir)
    monkeypatch.setattr("pipeline.collect.FETCH_LOG_DIR", log_dir)

    async def fake_validate(url: str) -> ResolvedURL:
        return ResolvedURL(url=url, hostname=httpx.URL(url).host or "example.test", port=443, ips=("93.184.216.34",))

    article_fetched = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal article_fetched
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        if request.url.path == "/feed.xml":
            return httpx.Response(200, content=FEED_XML)
        if request.url.path == "/story":
            article_fetched = True
            return httpx.Response(200, text="fresh content")
        return httpx.Response(404)

    monkeypatch.setattr("pipeline.http_client.validate_url", fake_validate)

    canonical_url = "https://example.test/story"
    article_id = hashlib.sha256(canonical_url.encode("utf-8")).hexdigest()
    article_data = {
        "article_id": article_id,
        "source_id": "fixture",
        "source_name": "Fixture News",
        "url": canonical_url,
        "canonical_url": canonical_url,
        "guid": "fixture-1",
        "headline": "Pre-existing headline",
        "summary": "Pre-existing summary",
        "published_at": "2026-05-31T12:30:00Z",
        "fetched_at": "2026-05-31T12:30:00Z",
        "collection": {},
    }

    from pipeline.collect import _article_path

    article_path = _article_path(article_id, "2026-05-31T12:30:00Z")
    article_path.parent.mkdir(parents=True, exist_ok=True)
    with article_path.open("w", encoding="utf-8") as f:
        json.dump(article_data, f)

    feed = FeedConfig(
        source_id="fixture",
        source_name="Fixture News",
        feed_url="https://example.test/feed.xml",
        site_url="https://example.test",
        enabled=True,
        default_category="technology",
        category_hints=["technology"],
        content_hints={"default_content_type": "news", "paywall": "none"},
        fetch={},
    )
    async with PoliteHTTPClient(rate_limit_seconds=0, max_retries=0, transport=httpx.MockTransport(handler)) as client:
        with StateDB(db_path) as state:
            assert not state.article_exists(article_id)

            collector = Collector(state, client, [feed], run_id="test-run")
            stats = await collector.run()

            assert state.article_exists(article_id)
            db_row = state.conn.execute("SELECT headline FROM articles WHERE article_id = ?", (article_id,)).fetchone()
            assert db_row["headline"] == "Pre-existing headline"

    assert stats["articles_skipped"] == 1
    assert stats["articles_written"] == 1
    assert not article_fetched


@pytest.mark.asyncio
async def test_watchdog_reports_active_tasks_on_inactivity(monkeypatch):
    import asyncio
    import time

    progress_messages = []
    collector = Collector(None, None, [], run_id="test-run", progress=progress_messages.append)

    collector._register_task("feed:mock-feed")
    collector._register_task("article:mock-feed:http://example.com/story")

    collector.last_activity_time = time.time() - 35

    sleep_called = False

    async def mock_sleep(seconds):
        nonlocal sleep_called
        if sleep_called:
            raise asyncio.CancelledError()
        sleep_called = True

    monkeypatch.setattr(asyncio, "sleep", mock_sleep)

    try:
        await collector._watchdog()
    except asyncio.CancelledError:
        pass

    assert any("watchdog: no output for 30s. pending operations:" in msg for msg in progress_messages)
    assert any("feed:mock-feed" in msg for msg in progress_messages)
    assert any("article:mock-feed:http://example.com/story" in msg for msg in progress_messages)


@pytest.mark.asyncio
async def test_collect_skips_fetch_if_db_identity_matches(tmp_path, monkeypatch):
    db_path = tmp_path / "pipeline.db"
    article_dir = tmp_path / "articles"
    log_dir = tmp_path / "fetch-log"
    migrate(db_path)
    monkeypatch.setattr("pipeline.collect.ARTICLE_DIR", article_dir)
    monkeypatch.setattr("pipeline.collect.FETCH_LOG_DIR", log_dir)

    async def fake_validate(url: str) -> ResolvedURL:
        return ResolvedURL(url=url, hostname=httpx.URL(url).host or "example.test", port=443, ips=("93.184.216.34",))

    article_fetched = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal article_fetched
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        if request.url.path == "/feed.xml":
            return httpx.Response(200, content=FEED_XML)
        if request.url.path == "/story":
            article_fetched = True
            return httpx.Response(200, text="fresh content")
        return httpx.Response(404)

    monkeypatch.setattr("pipeline.http_client.validate_url", fake_validate)

    feed_url = "https://example.test/story"
    final_url = "https://example.test/story/final"
    final_id = hashlib.sha256(final_url.encode("utf-8")).hexdigest()

    from pipeline.collect import _article_path

    article_path = _article_path(final_id, "2026-05-31T12:30:00Z")

    article_data = {
        "article_id": final_id,
        "source_id": "fixture",
        "source_name": "Fixture News",
        "url": feed_url,
        "canonical_url": final_url,
        "guid": "fixture-1",
        "headline": "Pre-existing headline",
        "summary": "Pre-existing summary",
        "published_at": "2026-05-31T12:30:00Z",
        "fetched_at": "2026-05-31T12:30:00Z",
        "collection": {},
    }

    article_path.parent.mkdir(parents=True, exist_ok=True)
    with article_path.open("w", encoding="utf-8") as f:
        json.dump(article_data, f)

    with StateDB(db_path) as state:
        state.insert_article(article_data, article_path)
        assert state.article_exists(final_id)

    feed = FeedConfig(
        source_id="fixture",
        source_name="Fixture News",
        feed_url="https://example.test/feed.xml",
        site_url="https://example.test",
        enabled=True,
        default_category="technology",
        category_hints=["technology"],
        content_hints={"default_content_type": "news", "paywall": "none"},
        fetch={},
    )
    async with PoliteHTTPClient(rate_limit_seconds=0, max_retries=0, transport=httpx.MockTransport(handler)) as client:
        with StateDB(db_path) as state:
            collector = Collector(state, client, [feed], run_id="test-run")
            stats = await collector.run()

    assert stats["articles_skipped"] == 1
    assert stats["articles_written"] == 0
    assert not article_fetched


def _insert_minimal_article(state: StateDB, **overrides) -> str:
    article_id = overrides.get("article_id", "art-" + hashlib.sha256(overrides["url"].encode()).hexdigest()[:12])
    article = {
        "article_id": article_id,
        "source_id": overrides.get("source_id", "fixture"),
        "source_name": "Fixture",
        "url": overrides["url"],
        "canonical_url": overrides.get("canonical_url"),
        "guid": overrides.get("guid"),
        "headline": overrides.get("headline", "Headline"),
        "summary": "",
        "content_text": "",
        "published_at": "2026-05-31T12:00:00Z",
        "publish_date_estimated": False,
        "fetched_at": "2026-05-31T12:00:00Z",
        "authors": [],
        "tags": [],
        "paywall": {"status": "none", "signals": []},
        "content_type": "news",
        "language": "en",
        "collection": {},
        "fingerprints": {},
    }
    state.insert_article(article, Path("data/staging/articles") / f"{article_id}.json")
    return article_id


def test_find_article_matches_by_stored_url(tmp_path):
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    with StateDB(db_path) as state:
        aid = _insert_minimal_article(state, url="https://ex.test/a", canonical_url="https://ex.test/a")
        match = state.find_article_by_url_or_guid("https://ex.test/a")
        assert match is not None
        assert match[0] == aid


def test_find_article_matches_by_canonical_url(tmp_path):
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    with StateDB(db_path) as state:
        aid = _insert_minimal_article(state, url="https://ex.test/redir", canonical_url="https://ex.test/canon")
        match = state.find_article_by_url_or_guid("https://other.test/unrelated", canonical_url="https://ex.test/canon")
        assert match is not None
        assert match[0] == aid


def test_find_article_cross_match_stored_url_equals_new_canonical(tmp_path):
    # A previously-stored row has url=X and no canonical. A new entry arrives whose
    # canonical_url is X (after redirect resolution). They should match.
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    with StateDB(db_path) as state:
        aid = _insert_minimal_article(state, url="https://ex.test/story", canonical_url=None)
        match = state.find_article_by_url_or_guid(
            "https://feed.test/redirect-tracker",
            canonical_url="https://ex.test/story",
        )
        assert match is not None
        assert match[0] == aid


def test_find_article_cross_match_stored_canonical_equals_new_url(tmp_path):
    # A previously-stored row has url=feed-link, canonical=resolved. A new entry
    # arrives whose url field is the resolved link (with a separate canonical).
    # The match should fire on the "stored canonical_url == requested url" cross.
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    with StateDB(db_path) as state:
        aid = _insert_minimal_article(
            state,
            url="https://feed.test/wrapped?utm=x",
            canonical_url="https://ex.test/story",
        )
        # Pass a distinct canonical so the OR-cross-match branch is added.
        match = state.find_article_by_url_or_guid(
            "https://ex.test/story",
            canonical_url="https://other.test/different",
        )
        assert match is not None
        assert match[0] == aid


def test_find_article_matches_by_guid(tmp_path):
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    with StateDB(db_path) as state:
        aid = _insert_minimal_article(state, url="https://ex.test/g", canonical_url=None, guid="unique-guid-1")
        match = state.find_article_by_url_or_guid(
            "https://different.test/no-match",
            canonical_url=None,
            guid="unique-guid-1",
            source_id="fixture",
        )
        assert match is not None
        assert match[0] == aid


def test_find_article_guid_match_is_source_scoped(tmp_path):
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    with StateDB(db_path) as state:
        _insert_minimal_article(
            state,
            url="https://source-a.test/story",
            canonical_url=None,
            guid="shared-guid",
            source_id="source-a",
        )
        assert (
            state.find_article_by_url_or_guid(
                "https://different.test/no-match",
                guid="shared-guid",
                source_id="source-b",
            )
            is None
        )
        match = state.find_article_by_url_or_guid(
            "https://different.test/no-match",
            guid="shared-guid",
            source_id="source-a",
        )
        assert match is not None


def test_find_article_returns_none_when_no_match(tmp_path):
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    with StateDB(db_path) as state:
        _insert_minimal_article(state, url="https://ex.test/x", canonical_url="https://ex.test/x")
        assert state.find_article_by_url_or_guid("https://missing.test/y") is None
        assert (
            state.find_article_by_url_or_guid(
                "https://missing.test/y",
                canonical_url="https://missing.test/y",
                guid="no-such-guid",
            )
            is None
        )


def _install_collect_once_fakes(
    monkeypatch,
    *,
    collector_error: Exception | None,
    finish_error: Exception | None,
) -> list[str]:
    progress_messages: list[str] = []

    class FakePipelineLock:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

    class FakeStateDB:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def start_run(self, run_id: str, stage: str) -> None:
            pass

        def finish_run(self, run_id: str, status: str, stats: dict[str, int]) -> None:
            if finish_error:
                raise finish_error

    class FakeHTTPClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

    class FakeCollector:
        def __init__(self, *args, **kwargs):
            pass

        async def run(self) -> dict[str, int]:
            if collector_error:
                raise collector_error
            return {"feeds_seen": 0}

    monkeypatch.setattr(
        "pipeline.collect.load_pipeline_config",
        lambda: PipelineConfig(collection={}, aggregation={}, retention={}, pipeline={}),
    )
    monkeypatch.setattr("pipeline.collect.load_feeds", lambda enabled_only=True: [])
    monkeypatch.setattr("pipeline.collect.PipelineLock", FakePipelineLock)
    monkeypatch.setattr("pipeline.collect.migrate", lambda: None)
    monkeypatch.setattr("pipeline.collect.cleanup_old_staging_data", lambda days: None)
    monkeypatch.setattr("pipeline.collect.StateDB", FakeStateDB)
    monkeypatch.setattr("pipeline.collect.PoliteHTTPClient", FakeHTTPClient)
    monkeypatch.setattr("pipeline.collect.Collector", FakeCollector)
    return progress_messages


@pytest.mark.asyncio
async def test_collect_once_raises_finish_run_error_after_success(monkeypatch):
    from pipeline.collect import collect_once

    progress_messages = _install_collect_once_fakes(
        monkeypatch,
        collector_error=None,
        finish_error=RuntimeError("finish failed"),
    )

    with pytest.raises(RuntimeError, match="finish failed"):
        await collect_once(progress=progress_messages.append)

    assert any("collect: finish_run failed: finish failed" in message for message in progress_messages)


@pytest.mark.asyncio
async def test_collect_once_preserves_collection_error_if_finish_run_also_fails(monkeypatch):
    from pipeline.collect import collect_once

    progress_messages = _install_collect_once_fakes(
        monkeypatch,
        collector_error=ValueError("collection failed"),
        finish_error=RuntimeError("finish failed"),
    )

    with pytest.raises(ValueError, match="collection failed"):
        await collect_once(progress=progress_messages.append)

    assert any("collect: failed collection-" in message for message in progress_messages)
    assert any("collect: finish_run failed: finish failed" in message for message in progress_messages)
