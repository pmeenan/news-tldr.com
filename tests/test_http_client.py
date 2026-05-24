from __future__ import annotations

import httpx
import pytest

from pipeline.http_client import PoliteHTTPClient, ResponseTooLarge
from pipeline.security import ResolvedURL


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
async def test_retry_after_capped(monkeypatch):
    async def fake_validate(url: str) -> ResolvedURL:
        return ResolvedURL(url=url, hostname=httpx.URL(url).host or "example.test", port=80, ips=("93.184.216.34",))

    called_sleeps = []
    async def fake_sleep(seconds):
        called_sleeps.append(seconds)

    monkeypatch.setattr("asyncio.sleep", fake_sleep)
    monkeypatch.setattr("pipeline.http_client.validate_url", fake_validate)

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
    async with PoliteHTTPClient(rate_limit_seconds=0, backoff_max_seconds=10, transport=transport) as client:
        response = await client.get("https://example.test/retry")
        assert response.status_code == 200

    assert called_sleeps == [10]


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

