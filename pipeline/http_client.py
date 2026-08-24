from __future__ import annotations

import asyncio
import email.utils
import typing
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import httpcore
import httpx
from httpcore._backends.auto import AutoBackend
from httpx._config import DEFAULT_LIMITS, Limits, create_ssl_context
from httpx._transports.default import AsyncResponseStream, map_httpcore_exceptions

from pipeline.paths import STATE_DIR
from pipeline.security import UnsafeURL, resolve_host_port, validate_url
from pipeline.util import sanitize_id

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36"
)

MAX_RESPONSE_BYTES = 25 * 1024 * 1024


class ResponseTooLarge(httpx.HTTPError):
    pass


BROWSER_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "application/rss+xml;q=0.8,application/atom+xml;q=0.8,*/*;q=0.7"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}


@dataclass
class RobotsPolicy:
    parser: RobotFileParser
    crawl_delay: float | None


class SSRFProtectedAsyncNetworkBackend(httpcore.AsyncNetworkBackend):
    def __init__(self) -> None:
        self._backend = AutoBackend()

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: typing.Iterable[typing.Any] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        import time

        start_time = time.time()
        ips = await resolve_host_port(host, port)
        last_exc: Exception | None = None
        for ip in ips:
            current_timeout = timeout
            if timeout is not None:
                elapsed = time.time() - start_time
                current_timeout = timeout - elapsed
                if current_timeout <= 0:
                    break
            try:
                return await self._backend.connect_tcp(
                    ip,
                    port,
                    timeout=current_timeout,
                    local_address=local_address,
                    socket_options=socket_options,
                )
            except Exception as exc:  # pragma: no cover - depends on live network path ordering.
                last_exc = exc
        if last_exc:
            raise last_exc
        if timeout is not None and (time.time() - start_time) >= timeout:
            raise httpcore.ConnectTimeout("Connection timed out across all resolved IP addresses")
        raise UnsafeURL(f"host resolution returned no addresses: {host}")

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,
        socket_options: typing.Iterable[typing.Any] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        raise UnsafeURL("unix socket connections are not allowed")

    async def sleep(self, seconds: float) -> None:
        await self._backend.sleep(seconds)


class SSRFProtectedAsyncHTTPTransport(httpx.AsyncBaseTransport):
    def __init__(self, *, limits: Limits = DEFAULT_LIMITS, http2: bool = False) -> None:
        ssl_context = create_ssl_context(verify=True, cert=None, trust_env=False)
        self._pool = httpcore.AsyncConnectionPool(
            ssl_context=ssl_context,
            max_connections=limits.max_connections,
            max_keepalive_connections=limits.max_keepalive_connections,
            keepalive_expiry=limits.keepalive_expiry,
            http1=True,
            http2=http2,
            retries=0,
            network_backend=SSRFProtectedAsyncNetworkBackend(),
        )

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        req = httpcore.Request(
            method=request.method,
            url=httpcore.URL(
                scheme=request.url.raw_scheme,
                host=request.url.raw_host,
                port=request.url.port,
                target=request.url.raw_path,
            ),
            headers=request.headers.raw,
            content=request.stream,
            extensions=request.extensions,
        )
        with map_httpcore_exceptions():
            resp = await self._pool.handle_async_request(req)
        return httpx.Response(
            status_code=resp.status,
            headers=resp.headers,
            stream=AsyncResponseStream(resp.stream),
            extensions=resp.extensions,
        )

    async def aclose(self) -> None:
        await self._pool.aclose()


class PoliteHTTPClient:
    def __init__(
        self,
        *,
        rate_limit_seconds: float = 1,
        connection_timeout_seconds: float = 10,
        read_timeout_seconds: float = 10,
        total_timeout_seconds: float = 15,
        max_retries: int = 3,
        backoff_initial_seconds: float = 5,
        backoff_max_seconds: float = 60,
        max_response_bytes: int = MAX_RESPONSE_BYTES,
        transport: httpx.AsyncBaseTransport | None = None,
        robots_cache_dir: Path | None = None,
        progress: Callable[[str], None] | None = None,
    ) -> None:
        self.rate_limit_seconds = rate_limit_seconds
        self.total_timeout_seconds = total_timeout_seconds
        self.max_retries = max_retries
        self.backoff_initial_seconds = backoff_initial_seconds
        self.backoff_max_seconds = backoff_max_seconds
        self.max_response_bytes = max_response_bytes
        self.robots_cache_dir = robots_cache_dir or (STATE_DIR / "robots")
        self.progress = progress
        timeout = httpx.Timeout(
            connect=connection_timeout_seconds,
            read=read_timeout_seconds,
            write=connection_timeout_seconds,
            pool=connection_timeout_seconds,
        )
        self.client = httpx.AsyncClient(
            headers=BROWSER_HEADERS,
            follow_redirects=False,
            timeout=timeout,
            transport=transport or SSRFProtectedAsyncHTTPTransport(),
            trust_env=False,
            http2=False,
        )
        self._domain_locks: dict[str, asyncio.Lock] = {}
        self._last_request: dict[str, float] = {}
        self._robots: dict[tuple[str, str, int], RobotsPolicy] = {}
        self._robots_locks: dict[tuple[str, str, int], asyncio.Lock] = {}

    async def aclose(self) -> None:
        await self.client.aclose()

    async def __aenter__(self) -> PoliteHTTPClient:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()

    async def get(
        self, url: str, *, headers: dict[str, str] | None = None, check_robots: bool = True
    ) -> httpx.Response:
        current_url = url
        for _ in range(6):
            resolved = await validate_url(current_url)
            if check_robots:
                await self._enforce_robots(current_url)
            response = await self._request_with_retries(current_url, resolved.hostname, headers=headers)
            if response.status_code not in {301, 302, 303, 307, 308}:
                return response
            location = response.headers.get("location")
            if not location:
                return response
            current_url = urljoin(current_url, location)
            headers = None
        raise httpx.TooManyRedirects("redirect chain exceeded 5 hops")

    async def _request_with_retries(
        self, url: str, domain: str, *, headers: dict[str, str] | None, max_retries: int | None = None
    ) -> httpx.Response:
        attempt = 0
        limit = self.max_retries if max_retries is None else max_retries
        backoff = self.backoff_initial_seconds
        while True:
            await self._respect_rate_limit(domain)
            try:
                response = await self._send_capped(url, headers)
            except httpx.TransportError as exc:
                if attempt >= limit:
                    raise
                sleep_time = min(backoff, self.backoff_max_seconds)
                self._progress(
                    f"HTTP request to {url} failed with {type(exc).__name__}: {exc}. "
                    f"Retrying in {sleep_time:.1f}s (attempt {attempt + 1}/{limit})"
                )
                await asyncio.sleep(sleep_time)
                backoff = min(backoff * 2, self.backoff_max_seconds)
                attempt += 1
                continue
            if response.status_code not in {429, 500, 502, 503, 504} or attempt >= limit:
                return response
            retry_after = self._retry_after_seconds(response.headers.get("retry-after"))
            sleep_time = retry_after if retry_after is not None else backoff
            sleep_time = min(sleep_time, self.backoff_max_seconds)
            self._progress(
                f"HTTP request to {url} failed with {response.status_code}. "
                f"Retrying in {sleep_time:.1f}s (attempt {attempt + 1}/{limit})"
            )
            await asyncio.sleep(sleep_time)
            backoff = min(backoff * 2, self.backoff_max_seconds)
            attempt += 1

    async def _send_capped(self, url: str, headers: dict[str, str] | None) -> httpx.Response:
        async def _do_send() -> tuple[httpx.Response, bytes]:
            request = self.client.build_request("GET", url, headers=headers)
            response = await self.client.send(request, stream=True)
            body = bytearray()
            try:
                cl = response.headers.get("content-length")
                if cl and cl.isdigit() and int(cl) > self.max_response_bytes:
                    raise ResponseTooLarge(
                        f"response Content-Length {cl} exceeds {self.max_response_bytes} byte cap: {url}"
                    )
                async for chunk in response.aiter_bytes():
                    body.extend(chunk)
                    if len(body) > self.max_response_bytes:
                        raise ResponseTooLarge(f"response exceeded {self.max_response_bytes} byte cap: {url}")
            finally:
                await response.aclose()
            return response, bytes(body)

        try:
            response, body_bytes = await asyncio.wait_for(_do_send(), timeout=self.total_timeout_seconds)
        except TimeoutError as exc:
            raise httpx.ReadTimeout(
                f"Total download time exceeded {self.total_timeout_seconds:g} seconds: {url}"
            ) from exc

        resp_headers = httpx.Headers(response.headers)
        resp_headers.pop("content-encoding", None)
        resp_headers.pop("content-length", None)
        resp_headers.pop("transfer-encoding", None)
        return httpx.Response(
            status_code=response.status_code,
            headers=resp_headers,
            content=body_bytes,
            request=response.request,
            extensions=response.extensions,
        )

    async def _respect_rate_limit(self, domain: str) -> None:
        # The per-domain lock is intentionally held across `asyncio.sleep(wait_for)`
        # so concurrent coroutines targeting the same domain serialize and honor
        # the configured delay (and any robots.txt Crawl-delay). With high article
        # concurrency a single slow domain can queue many tasks behind this lock;
        # the cap below (10s) bounds the per-wait latency, but total wait grows
        # linearly with queue depth. Add a per-domain queue cap here if that
        # becomes a problem in practice.
        lock = self._domain_locks.setdefault(domain, asyncio.Lock())
        async with lock:
            delay = self.rate_limit_seconds
            crawl_delays = [
                policy.crawl_delay
                for key, policy in self._robots.items()
                if key[1] == domain and policy.crawl_delay is not None
            ]
            if crawl_delays:
                delay = max(delay, *crawl_delays)
            delay = min(delay, 10.0)  # cap delay at 10 seconds to avoid pathological hangs
            now = asyncio.get_running_loop().time()
            last = self._last_request.get(domain)
            if last is not None:
                wait_for = delay - (now - last)
                if wait_for > 0:
                    await asyncio.sleep(wait_for)
            self._last_request[domain] = asyncio.get_running_loop().time()

    async def _enforce_robots(self, url: str) -> None:
        parsed = urlparse(url)
        domain = parsed.hostname or ""
        policy = await self._robots_policy(parsed.scheme, domain, parsed.port)
        if not policy.parser.can_fetch(USER_AGENT, url):
            raise PermissionError(f"robots.txt disallows fetching {url}")

    async def _robots_policy(self, scheme: str, domain: str, port: int | None) -> RobotsPolicy:
        actual_port = port or (443 if scheme == "https" else 80)
        key = (scheme, domain.lower(), actual_port)
        if key in self._robots:
            return self._robots[key]
        lock = self._robots_locks.setdefault(key, asyncio.Lock())
        async with lock:
            if key in self._robots:
                return self._robots[key]

            import time

            cache_dir = self.robots_cache_dir
            cache_file = cache_dir / f"{scheme}_{sanitize_id(domain.lower())}_{actual_port}.txt"

            parser = RobotFileParser()
            crawl_delay = None
            cached_text = None

            if cache_file.exists():
                try:
                    mtime = cache_file.stat().st_mtime
                    if time.time() - mtime < 24 * 3600:
                        cached_text = cache_file.read_text(encoding="utf-8")
                except Exception:
                    pass

            if cached_text is not None:
                parser.parse(cached_text.splitlines())
                crawl_delay = parser.crawl_delay(USER_AGENT) or parser.crawl_delay("*")
            else:
                host = f"[{domain}]" if ":" in domain and not domain.startswith("[") else domain
                default_port = 443 if scheme == "https" else 80
                netloc = host if actual_port == default_port else f"{host}:{actual_port}"
                robots_url = f"{scheme}://{netloc}/robots.txt"
                parser = RobotFileParser(robots_url)
                try:
                    await validate_url(robots_url)
                    response = await self._request_with_retries(robots_url, domain, headers=None, max_retries=0)
                    if response.status_code < 400:
                        text = response.text
                        parser.parse(text.splitlines())
                        crawl_delay = parser.crawl_delay(USER_AGENT) or parser.crawl_delay("*")
                        try:
                            cache_dir.mkdir(parents=True, exist_ok=True)
                            cache_file.write_text(text, encoding="utf-8")
                        except Exception:
                            pass
                    else:
                        parser.parse([])
                        try:
                            cache_dir.mkdir(parents=True, exist_ok=True)
                            cache_file.write_text("", encoding="utf-8")
                        except Exception:
                            pass
                except Exception:
                    parser.parse([])

            policy = RobotsPolicy(parser=parser, crawl_delay=float(crawl_delay) if crawl_delay else None)
            self._robots[key] = policy
            return policy

    def _progress(self, message: str) -> None:
        if self.progress:
            self.progress(message)

    @staticmethod
    def _retry_after_seconds(value: str | None) -> float | None:
        if not value:
            return None
        if value.isdigit():
            return float(value)
        try:
            dt = email.utils.parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return max(0.0, (dt.astimezone(UTC) - datetime.now(UTC)).total_seconds())
