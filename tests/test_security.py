from __future__ import annotations

import socket

import pytest

from pipeline.security import UnsafeURL, validate_url


@pytest.mark.asyncio
async def test_validate_url_blocks_private_ip(monkeypatch):
    def fake_getaddrinfo(*args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("169.254.169.254", 80))]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)

    with pytest.raises(UnsafeURL):
        await validate_url("http://example.test/latest")


@pytest.mark.asyncio
async def test_validate_url_blocks_non_global_shared_address(monkeypatch):
    def fake_getaddrinfo(*args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("100.64.0.1", 80))]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)

    with pytest.raises(UnsafeURL):
        await validate_url("http://example.test/latest")


@pytest.mark.asyncio
async def test_validate_url_wraps_invalid_port():
    with pytest.raises(UnsafeURL):
        await validate_url("http://example.test:99999/latest")


@pytest.mark.asyncio
async def test_validate_url_allows_public_http(monkeypatch):
    def fake_getaddrinfo(*args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("93.184.216.34", 80))]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)

    resolved = await validate_url("http://example.test/latest")
    assert resolved.hostname == "example.test"
    assert resolved.port == 80
