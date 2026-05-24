from __future__ import annotations

import httpx
import pytest

from pipeline.collect import Collector
from pipeline.config import FeedConfig
from pipeline.http_client import PoliteHTTPClient
from pipeline.security import ResolvedURL
from pipeline.state import StateDB, migrate

FEED_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Fixture</title>
    <item>
      <guid>fixture-1</guid>
      <title>Fixture headline</title>
      <link>https://example.test/story</link>
      <description>Short summary.</description>
      <pubDate>Sun, 24 May 2026 12:30:00 GMT</pubDate>
      <category>technology</category>
    </item>
  </channel>
</rss>
"""


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
    async with PoliteHTTPClient(rate_limit_seconds=0, transport=transport) as client:
        with StateDB(db_path) as state:
            collector = Collector(state, client, [feed], run_id="test-run")
            stats = await collector.run()

            rows = state.conn.execute("SELECT article_id, headline, article_path FROM articles").fetchall()

    assert stats["articles_written"] == 1
    assert rows[0]["headline"] == "Fixture headline"
    assert article_dir.glob("*")
    assert list(article_dir.rglob("*.json"))
    assert list(log_dir.glob("*.jsonl"))


def test_feed_parser_rejects_dtd():
    from pipeline.collect import _parse_feed_bytes

    with pytest.raises(ValueError):
        _parse_feed_bytes(b'<?xml version="1.0"?><!DOCTYPE foo><rss></rss>')


def test_feed_parser_rejects_dtd_after_large_prefix():
    from pipeline.collect import _parse_feed_bytes

    with pytest.raises(ValueError):
        _parse_feed_bytes(b"<?xml version='1.0'?>" + b" " * 4096 + b"<!ENTITY xxe SYSTEM 'file:///etc/passwd'><rss></rss>")


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
    async with PoliteHTTPClient(rate_limit_seconds=0, transport=transport) as client:
        with StateDB(db_path) as state:
            collector = Collector(state, client, [feed], run_id="test-run")
            stats = await collector.run()

            rows = state.conn.execute("SELECT article_id, headline, summary FROM articles").fetchall()
            errors = state.conn.execute("SELECT item_type, error_type, error_message FROM item_errors").fetchall()

    assert stats["articles_written"] == 1
    assert rows[0]["headline"] == "Fixture headline"
    assert rows[0]["summary"] == "Short summary."
    assert len(errors) == 1
    assert errors[0]["item_type"] == "article_fetch_http_error"
    assert "500" in errors[0]["error_message"]


