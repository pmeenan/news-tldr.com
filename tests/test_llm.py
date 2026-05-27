from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from pipeline.llm import (
    GeminiClient,
    GeminiTruncatedError,
    parse_dotenv,
)


def test_parse_dotenv_handles_comments_quotes_and_export() -> None:
    text = (
        "# top-level comment\n"
        "GEMINI_API_KEY=plain-value\n"
        "GEMINI_MODEL=gemini-3.1-flash-lite # inline comment\n"
        'QUOTED_DOUBLE="hello world"\n'
        "QUOTED_SINGLE='another value'\n"
        "export EXPORTED_KEY=exported-value\n"
        "EMPTY_VALUE=\n"
        "\n"
        "  KEY_WITH_WHITESPACE   =  trimmed  \n"
    )
    parsed = parse_dotenv(text)
    assert parsed == {
        "GEMINI_API_KEY": "plain-value",
        "GEMINI_MODEL": "gemini-3.1-flash-lite",
        "QUOTED_DOUBLE": "hello world",
        "QUOTED_SINGLE": "another value",
        "EXPORTED_KEY": "exported-value",
        "EMPTY_VALUE": "",
        "KEY_WITH_WHITESPACE": "trimmed",
    }


def _make_client(monkeypatch: pytest.MonkeyPatch, **overrides: Any) -> GeminiClient:
    monkeypatch.setenv("GEMINI_API_KEY", "fake-key")
    return GeminiClient(
        api_key="fake-key",
        model="fake-model",
        sleep=lambda _seconds: None,
        **overrides,
    )


def _gemini_response(payload: dict[str, Any] | None = None) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "candidates": [
                {
                    "finishReason": "STOP",
                    "content": {"parts": [{"text": json.dumps(payload or {"ok": True})}]},
                }
            ],
            "usageMetadata": {"promptTokenCount": 5, "candidatesTokenCount": 1},
        },
    )


def test_generate_json_retries_on_429(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(1)
        if len(attempts) < 3:
            return httpx.Response(429, text="slow down")
        return _gemini_response()

    with _make_client(
        monkeypatch,
        max_attempts=3,
        backoff_base_seconds=0,
        transport=httpx.MockTransport(handler),
    ) as client:
        result = client.generate_json(
            system_instruction="sys",
            prompt="p",
            response_schema={"type": "OBJECT"},
        )
    assert len(attempts) == 3
    assert result.payload == {"ok": True}


def test_generate_json_raises_on_max_tokens(monkeypatch: pytest.MonkeyPatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "candidates": [
                    {
                        "finishReason": "MAX_TOKENS",
                        "content": {"parts": [{"text": '{"partial":'}]},
                    }
                ]
            },
        )

    with _make_client(monkeypatch, transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(GeminiTruncatedError):
            client.generate_json(
                system_instruction="sys",
                prompt="p",
                response_schema={"type": "OBJECT"},
            )


def test_generate_json_includes_max_output_tokens(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content.decode("utf-8"))
        return _gemini_response()

    with _make_client(
        monkeypatch,
        max_output_tokens=12345,
        transport=httpx.MockTransport(handler),
    ) as client:
        client.generate_json(
            system_instruction="sys",
            prompt="p",
            response_schema={"type": "OBJECT"},
        )

    assert captured["body"]["generationConfig"]["maxOutputTokens"] == 12345


def test_generate_json_reuses_injected_httpx_client(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return _gemini_response()

    http_client = httpx.Client(transport=httpx.MockTransport(handler), http2=True)
    try:
        client = _make_client(monkeypatch, http_client=http_client)
        client.generate_json(system_instruction="sys", prompt="one", response_schema={"type": "OBJECT"})
        client.generate_json(system_instruction="sys", prompt="two", response_schema={"type": "OBJECT"})
    finally:
        http_client.close()

    assert len(calls) == 2


def test_gemini_client_defaults_to_http11(monkeypatch: pytest.MonkeyPatch) -> None:
    with _make_client(monkeypatch) as client:
        assert client._http_client._transport._pool._http2 is False
