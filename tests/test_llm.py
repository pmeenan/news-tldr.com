from __future__ import annotations

import io
import json
import urllib.error
from typing import Any

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


def test_generate_json_retries_on_429(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client(monkeypatch, max_attempts=3, backoff_base_seconds=0)

    attempts: list[int] = []

    class FakeResponse:
        def __init__(self, body: bytes) -> None:
            self._body = body

        def __enter__(self) -> FakeResponse:
            return self

        def __exit__(self, *args: Any) -> None:
            return None

        def read(self) -> bytes:
            return self._body

    def fake_urlopen(request: Any, timeout: float) -> Any:
        attempts.append(1)
        if len(attempts) < 3:
            raise urllib.error.HTTPError(request.full_url, 429, "rate limited", {}, io.BytesIO(b"slow down"))
        body = json.dumps(
            {
                "candidates": [
                    {
                        "finishReason": "STOP",
                        "content": {"parts": [{"text": json.dumps({"ok": True})}]},
                    }
                ],
                "usageMetadata": {"promptTokenCount": 5, "candidatesTokenCount": 1},
            }
        ).encode("utf-8")
        return FakeResponse(body)

    monkeypatch.setattr("pipeline.llm.urllib.request.urlopen", fake_urlopen)

    result = client.generate_json(
        system_instruction="sys",
        prompt="p",
        response_schema={"type": "OBJECT"},
    )
    assert len(attempts) == 3
    assert result.payload == {"ok": True}


def test_generate_json_raises_on_max_tokens(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client(monkeypatch)

    class FakeResponse:
        def __enter__(self) -> FakeResponse:
            return self

        def __exit__(self, *args: Any) -> None:
            return None

        def read(self) -> bytes:
            return json.dumps(
                {
                    "candidates": [
                        {
                            "finishReason": "MAX_TOKENS",
                            "content": {"parts": [{"text": '{"partial":'}]},
                        }
                    ]
                }
            ).encode("utf-8")

    monkeypatch.setattr("pipeline.llm.urllib.request.urlopen", lambda *a, **kw: FakeResponse())

    with pytest.raises(GeminiTruncatedError):
        client.generate_json(
            system_instruction="sys",
            prompt="p",
            response_schema={"type": "OBJECT"},
        )


def test_generate_json_includes_max_output_tokens(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client(monkeypatch, max_output_tokens=12345)
    captured: dict[str, Any] = {}

    class FakeResponse:
        def __enter__(self) -> FakeResponse:
            return self

        def __exit__(self, *args: Any) -> None:
            return None

        def read(self) -> bytes:
            return json.dumps(
                {
                    "candidates": [
                        {
                            "finishReason": "STOP",
                            "content": {"parts": [{"text": json.dumps({"ok": True})}]},
                        }
                    ]
                }
            ).encode("utf-8")

    def fake_urlopen(request: Any, timeout: float) -> Any:
        captured["body"] = json.loads(request.data.decode("utf-8"))
        return FakeResponse()

    monkeypatch.setattr("pipeline.llm.urllib.request.urlopen", fake_urlopen)

    client.generate_json(
        system_instruction="sys",
        prompt="p",
        response_schema={"type": "OBJECT"},
    )
    assert captured["body"]["generationConfig"]["maxOutputTokens"] == 12345
