from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from pipeline.llm import (
    FallbackGeminiClient,
    GeminiClient,
    GeminiEmptyResponseError,
    GeminiResult,
    GeminiRetryableError,
    GeminiTruncatedError,
    gemini_model_for_stage,
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


def test_generate_json_uses_thinking_level_without_deprecated_sampling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content.decode("utf-8"))
        return _gemini_response()

    with _make_client(
        monkeypatch,
        thinking_level="minimal",
        transport=httpx.MockTransport(handler),
    ) as client:
        client.generate_json(
            system_instruction="sys",
            prompt="p",
            response_schema={"type": "OBJECT"},
        )

    generation_config = captured["body"]["generationConfig"]
    assert generation_config["thinkingConfig"] == {"thinkingLevel": "minimal"}
    assert "temperature" not in generation_config
    assert "topP" not in generation_config
    assert "topK" not in generation_config


def test_stage_specific_model_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GEMINI_MODEL", "fallback-model")
    monkeypatch.setenv("GEMINI_BULK_MODEL", "bulk-model")
    monkeypatch.setenv("GEMINI_REVIEW_MODEL", "review-model")

    assert gemini_model_for_stage("bulk") == "bulk-model"
    assert gemini_model_for_stage("review") == "review-model"


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


def test_fallback_client_uses_next_model_and_opens_circuit() -> None:
    calls: list[str] = []

    class FakeClient:
        def __init__(self, model: str, *, fail: bool = False) -> None:
            self.model = model
            self.fail = fail

        def generate_json(self, **kwargs: Any) -> GeminiResult:
            calls.append(self.model)
            if self.fail:
                raise GeminiRetryableError("capacity", status_code=503)
            return GeminiResult(
                payload={"ok": True},
                model=self.model,
                elapsed_ms=1,
                usage={},
            )

        def close(self) -> None:
            pass

    client = FallbackGeminiClient(
        [FakeClient("gemini-3.7-flash", fail=True), FakeClient("gemini-3.6-flash")],
        cooldown_seconds=300,
        monotonic=lambda: 100.0,
    )
    first = client.generate_json(system_instruction="s", prompt="p", response_schema={})
    second = client.generate_json(system_instruction="s", prompt="p", response_schema={})

    assert first.model == "gemini-3.6-flash"
    assert second.model == "gemini-3.6-flash"
    assert calls == ["gemini-3.7-flash", "gemini-3.6-flash", "gemini-3.6-flash"]


def test_fallback_client_does_not_mask_nonretryable_errors() -> None:
    class BadClient:
        model = "gemini-3.7-flash"

        def generate_json(self, **kwargs: Any) -> GeminiResult:
            raise RuntimeError("invalid schema")

        def close(self) -> None:
            pass

    class UnusedClient(BadClient):
        model = "gemini-3.6-flash"

    client = FallbackGeminiClient([BadClient(), UnusedClient()])
    with pytest.raises(RuntimeError, match="invalid schema"):
        client.generate_json(system_instruction="s", prompt="p", response_schema={})


def test_fallback_client_tries_next_model_for_empty_response_without_opening_circuit() -> None:
    calls: list[str] = []

    class EmptyClient:
        model = "gemini-3.7-flash"

        def generate_json(self, **kwargs: Any) -> GeminiResult:
            calls.append(self.model)
            raise GeminiEmptyResponseError("no candidates")

        def close(self) -> None:
            pass

    class GoodClient(EmptyClient):
        model = "gemini-3.6-flash"

        def generate_json(self, **kwargs: Any) -> GeminiResult:
            calls.append(self.model)
            return GeminiResult(payload={"ok": True}, model=self.model, elapsed_ms=1, usage={})

    client = FallbackGeminiClient([EmptyClient(), GoodClient()])
    client.generate_json(system_instruction="s", prompt="p", response_schema={})
    client.generate_json(system_instruction="s", prompt="p", response_schema={})

    assert calls == [
        "gemini-3.7-flash",
        "gemini-3.6-flash",
        "gemini-3.7-flash",
        "gemini-3.6-flash",
    ]
