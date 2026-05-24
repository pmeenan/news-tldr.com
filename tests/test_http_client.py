from __future__ import annotations

import gzip

import httpx
import pytest

from pipeline.http_client import PoliteHTTPClient, ResponseTooLarge
from pipeline.security import ResolvedURL


@pytest.fixture(autouse=True)
def mock_state_dir(tmp_path, monkeypatch):
    monkeypatch.setattr("pipeline.http_client.STATE_DIR", tmp_path / "state")


@pytest.mark.asyncio
async def test_redirect_target_is_validated(monkeypatch):
    seen: list[str] = []

    async def fake_validate(url: str) -> ResolvedURL:
        seen.append(url)
        return ResolvedURL(url=url, hostname=httpx.URL(url).host or "example.test", port=80, ips=("93.184.216.34",))

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        if request.url.path == "/start":
            return httpx.Response(302, headers={"location": "https://example.test/final"})
        return httpx.Response(200, text="ok")

    monkeypatch.setattr("pipeline.http_client.validate_url", fake_validate)
    transport = httpx.MockTransport(handler)
    async with PoliteHTTPClient(rate_limit_seconds=0, transport=transport) as client:
        response = await client.get("https://example.test/start")

    assert response.status_code == 200
    assert "https://example.test/start" in seen
    assert "https://example.test/final" in seen


@pytest.mark.asyncio
async def test_response_size_cap_rejects_oversized_body(monkeypatch):
    async def fake_validate(url: str) -> ResolvedURL:
        return ResolvedURL(url=url, hostname=httpx.URL(url).host or "example.test", port=80, ips=("93.184.216.34",))

    huge = b"x" * 4096

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        return httpx.Response(200, content=huge)

    monkeypatch.setattr("pipeline.http_client.validate_url", fake_validate)
    transport = httpx.MockTransport(handler)
    async with PoliteHTTPClient(
        rate_limit_seconds=0,
        transport=transport,
        max_response_bytes=1024,
    ) as client:
        with pytest.raises(ResponseTooLarge):
            await client.get("https://example.test/big")


@pytest.mark.asyncio
async def test_response_size_cap_rejects_oversized_content_length(monkeypatch):
    async def fake_validate(url: str) -> ResolvedURL:
        return ResolvedURL(url=url, hostname=httpx.URL(url).host or "example.test", port=80, ips=("93.184.216.34",))

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        return httpx.Response(200, content=b"small", headers={"content-length": "999999999"})

    monkeypatch.setattr("pipeline.http_client.validate_url", fake_validate)
    transport = httpx.MockTransport(handler)
    async with PoliteHTTPClient(
        rate_limit_seconds=0,
        transport=transport,
        max_response_bytes=1024,
    ) as client:
        with pytest.raises(ResponseTooLarge):
            await client.get("https://example.test/big")


@pytest.mark.asyncio
async def test_capped_response_strips_decompression_headers(monkeypatch):
    async def fake_validate(url: str) -> ResolvedURL:
        return ResolvedURL(url=url, hostname=httpx.URL(url).host or "example.test", port=80, ips=("93.184.216.34",))

    compressed = gzip.compress(b"decoded body")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        return httpx.Response(
            200,
            content=compressed,
            headers={
                "content-encoding": "gzip",
                "content-length": str(len(compressed)),
                "transfer-encoding": "chunked",
            },
        )

    monkeypatch.setattr("pipeline.http_client.validate_url", fake_validate)
    transport = httpx.MockTransport(handler)
    async with PoliteHTTPClient(rate_limit_seconds=0, transport=transport) as client:
        response = await client.get("https://example.test/compressed")

    assert response.text == "decoded body"
    assert "content-encoding" not in response.headers
    assert response.headers["content-length"] == str(len(b"decoded body"))
    assert "transfer-encoding" not in response.headers


@pytest.mark.asyncio
async def test_robots_disallow_blocks_fetch(monkeypatch):
    async def fake_validate(url: str) -> ResolvedURL:
        return ResolvedURL(url=url, hostname=httpx.URL(url).host or "example.test", port=80, ips=("93.184.216.34",))

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(200, text="User-agent: *\nDisallow: /blocked\n")
        return httpx.Response(200, text="ok")

    monkeypatch.setattr("pipeline.http_client.validate_url", fake_validate)
    transport = httpx.MockTransport(handler)
    async with PoliteHTTPClient(rate_limit_seconds=0, transport=transport) as client:
        with pytest.raises(PermissionError):
            await client.get("https://example.test/blocked/story")


@pytest.mark.asyncio
async def test_retry_after_capped(monkeypatch, capsys):
    async def fake_validate(url: str) -> ResolvedURL:
        return ResolvedURL(url=url, hostname=httpx.URL(url).host or "example.test", port=80, ips=("93.184.216.34",))

    called_sleeps = []
    async def fake_sleep(seconds):
        called_sleeps.append(seconds)

    monkeypatch.setattr("asyncio.sleep", fake_sleep)
    monkeypatch.setattr("pipeline.http_client.validate_url", fake_validate)
    progress_messages = []

    attempts = 0
    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        attempts += 1
        if attempts == 1:
            return httpx.Response(429, headers={"retry-after": "9999"})
        return httpx.Response(200, text="ok")

    transport = httpx.MockTransport(handler)
    async with PoliteHTTPClient(
        rate_limit_seconds=0,
        backoff_max_seconds=10,
        transport=transport,
        progress=progress_messages.append,
    ) as client:
        response = await client.get("https://example.test/retry")
        assert response.status_code == 200

    assert called_sleeps == [10]
    captured = capsys.readouterr()
    assert captured.err == ""
    assert progress_messages == [
        "HTTP request to https://example.test/retry failed with 429. Retrying in 10.0s (attempt 1/3)"
    ]


@pytest.mark.asyncio
async def test_robots_policy_uses_zero_retries(monkeypatch):
    async def fake_validate(url: str) -> ResolvedURL:
        return ResolvedURL(url=url, hostname=httpx.URL(url).host or "example.test", port=80, ips=("93.184.216.34",))

    monkeypatch.setattr("pipeline.http_client.validate_url", fake_validate)

    robots_attempts = 0
    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal robots_attempts
        if request.url.path == "/robots.txt":
            robots_attempts += 1
            return httpx.Response(500, text="Error")
        return httpx.Response(200, text="ok")

    transport = httpx.MockTransport(handler)
    async with PoliteHTTPClient(rate_limit_seconds=0, max_retries=3, transport=transport) as client:
        response = await client.get("https://example.test/story")
        assert response.status_code == 200

    assert robots_attempts == 1


@pytest.mark.asyncio
async def test_robots_policy_deduplicates_concurrent_fetches(monkeypatch):
    import asyncio
    async def fake_validate(url: str) -> ResolvedURL:
        return ResolvedURL(url=url, hostname=httpx.URL(url).host or "example.test", port=80, ips=("93.184.216.34",))

    monkeypatch.setattr("pipeline.http_client.validate_url", fake_validate)

    robots_attempts = 0
    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal robots_attempts
        if request.url.path == "/robots.txt":
            robots_attempts += 1
            await asyncio.sleep(0.05)
            return httpx.Response(200, text="User-agent: *\nDisallow:\n")
        return httpx.Response(200, text="ok")

    transport = httpx.MockTransport(handler)
    async with PoliteHTTPClient(rate_limit_seconds=0, transport=transport) as client:
        await asyncio.gather(
            client.get("https://example.test/a"),
            client.get("https://example.test/b"),
            client.get("https://example.test/c"),
        )

    assert robots_attempts == 1


@pytest.mark.asyncio
async def test_crawl_delay_is_capped_at_10_seconds(monkeypatch):
    async def fake_validate(url: str) -> ResolvedURL:
        return ResolvedURL(url=url, hostname=httpx.URL(url).host or "example.test", port=80, ips=("93.184.216.34",))

    called_sleeps = []
    async def fake_sleep(seconds):
        called_sleeps.append(seconds)

    monkeypatch.setattr("asyncio.sleep", fake_sleep)
    monkeypatch.setattr("pipeline.http_client.validate_url", fake_validate)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(200, text="User-agent: *\nCrawl-delay: 99\n")
        return httpx.Response(200, text="ok")

    transport = httpx.MockTransport(handler)
    async with PoliteHTTPClient(rate_limit_seconds=1, transport=transport) as client:
        await client.get("https://example.test/first")
        await client.get("https://example.test/second")

    assert len(called_sleeps) == 2
    assert all(s == pytest.approx(10.0, abs=0.5) for s in called_sleeps)


@pytest.mark.asyncio
async def test_network_backend_decrements_timeout_across_ips(monkeypatch):
    import asyncio

    import httpcore

    from pipeline.http_client import SSRFProtectedAsyncNetworkBackend

    backend = SSRFProtectedAsyncNetworkBackend()

    async def fake_resolve(host, port):
        return ("1.1.1.1", "2.2.2.2")
    monkeypatch.setattr("pipeline.http_client.resolve_host_port", fake_resolve)

    called_timeouts = []
    async def fake_connect_tcp(ip, port, timeout=None, local_address=None, socket_options=None):
        called_timeouts.append(timeout)
        if ip == "1.1.1.1":
            await asyncio.sleep(1.0)
            raise httpcore.ConnectTimeout("first timed out")
        else:
            return "stream"

    monkeypatch.setattr(backend._backend, "connect_tcp", fake_connect_tcp)

    stream = await backend.connect_tcp("example.com", 80, timeout=5.0)
    assert stream == "stream"
    assert len(called_timeouts) == 2
    assert called_timeouts[0] == pytest.approx(5.0, abs=0.1)
    assert called_timeouts[1] == pytest.approx(4.0, abs=0.1)


@pytest.mark.asyncio
async def test_http_client_enables_http2():
    async with PoliteHTTPClient(rate_limit_seconds=0) as client:
        assert client.client._transport._pool._http2 is True
