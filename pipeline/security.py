from __future__ import annotations

import asyncio
import ipaddress
import socket
from dataclasses import dataclass
from urllib.parse import urlparse


class UnsafeURL(ValueError):
    pass


BLOCKED_PORTS = {
    0,
    1,
    7,
    9,
    11,
    13,
    15,
    17,
    19,
    20,
    21,
    22,
    23,
    25,
    53,
    110,
    111,
    135,
    139,
    143,
    389,
    445,
    465,
    587,
    993,
    995,
    11211,
}


@dataclass(frozen=True)
class ResolvedURL:
    url: str
    hostname: str
    port: int
    ips: tuple[str, ...]


def _is_blocked_ip(ip: str) -> bool:
    parsed = ipaddress.ip_address(ip)
    return any(
        (
            parsed.is_loopback,
            parsed.is_private,
            parsed.is_link_local,
            parsed.is_multicast,
            parsed.is_reserved,
            parsed.is_unspecified,
            not parsed.is_global,
        )
    )


async def resolve_host_port(hostname: str, port: int) -> tuple[str, ...]:
    if port < 1 or port > 65535:
        raise UnsafeURL(f"invalid URL port: {port}")
    if port in BLOCKED_PORTS:
        raise UnsafeURL(f"blocked URL port: {port}")
    try:
        addrinfo = await asyncio.wait_for(
            asyncio.to_thread(socket.getaddrinfo, hostname, port, type=socket.SOCK_STREAM),
            timeout=5.0,
        )
    except TimeoutError as exc:
        raise UnsafeURL(f"host resolution timed out: {hostname}") from exc
    except socket.gaierror as exc:
        raise UnsafeURL(f"host resolution failed: {hostname}") from exc
    ips = tuple(sorted({item[4][0] for item in addrinfo}))
    if not ips:
        raise UnsafeURL(f"host resolution returned no addresses: {hostname}")
    blocked = [ip for ip in ips if _is_blocked_ip(ip)]
    if blocked:
        raise UnsafeURL(f"blocked resolved IP for {hostname}: {blocked[0]}")
    return ips


async def validate_url(url: str) -> ResolvedURL:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise UnsafeURL(f"blocked URL scheme: {parsed.scheme or '<empty>'}")
    if parsed.username or parsed.password:
        raise UnsafeURL("userinfo in URLs is not allowed")
    hostname = parsed.hostname
    if not hostname:
        raise UnsafeURL("URL is missing a hostname")
    try:
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
    except ValueError as exc:
        raise UnsafeURL(str(exc)) from exc
    ips = await resolve_host_port(hostname, port)
    return ResolvedURL(url=url, hostname=hostname.lower(), port=port, ips=ips)
