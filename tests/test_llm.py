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


def test_generate_json_sends_service_tier_and_labels_client() -> None:
    from pipeline.llm import GeminiClient

    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "candidates": [{"content": {"parts": [{"text": '{"ok": true}'}]}, "finishReason": "STOP"}],
                "usageMetadata": {"promptTokenCount": 3, "candidatesTokenCount": 2,
                                  "thoughtsTokenCount": 5, "serviceTier": "flex"},
            },
        )

    client = GeminiClient(
        api_key="k", model="gemini-3.7-flash", transport=httpx.MockTransport(handler),
        service_tier="flex", timeout_seconds=7,
    )
    result = client.generate_json(system_instruction="s", prompt="p", response_schema={"type": "OBJECT"})
    assert captured["body"]["serviceTier"] == "flex"
    assert client.label == "gemini-3.7-flash:flex"
    assert result.usage["serviceTier"] == "flex" and result.usage["thoughtsTokenCount"] == 5
    with pytest.raises(ValueError, match="service_tier"):
        GeminiClient(api_key="k", service_tier="bogus", transport=httpx.MockTransport(handler))


def test_flex_chain_falls_back_to_standard_and_keys_cooldown_by_tier() -> None:
    calls: list[str] = []

    class FakeClient:
        def __init__(self, model: str, tier: str | None, *, fail: bool = False) -> None:
            self.model, self.service_tier, self.fail = model, tier, fail

        @property
        def label(self) -> str:
            return f"{self.model}:{self.service_tier or 'standard'}"

        def generate_json(self, **kwargs: Any) -> GeminiResult:
            calls.append(self.label)
            if self.fail:
                raise GeminiRetryableError("shed", status_code=503)
            return GeminiResult(payload={"ok": True}, model=self.model, elapsed_ms=1, usage={})

        def close(self) -> None:
            pass

    chain = FallbackGeminiClient(
        [FakeClient("gemini-3.7-flash", "flex", fail=True), FakeClient("gemini-3.8-flash", None),
         FakeClient("gemini-3.7-flash", None)],
        cooldown_seconds=300, monotonic=lambda: 100.0,
    )
    assert chain.labels == ("gemini-3.7-flash:flex", "gemini-3.8-flash:standard", "gemini-3.7-flash:standard")
    assert chain.models == ("gemini-3.7-flash", "gemini-3.8-flash")
    first = chain.generate_json(system_instruction="s", prompt="p", response_schema={})
    second = chain.generate_json(system_instruction="s", prompt="p", response_schema={})
    assert first.model == second.model == "gemini-3.8-flash"
    # The shed flex tier is bypassed on the second call; standard 3.7 is a distinct label.
    assert calls == ["gemini-3.7-flash:flex", "gemini-3.8-flash:standard", "gemini-3.8-flash:standard"]


def test_create_gemini_client_builds_flex_then_standard_chains(monkeypatch: pytest.MonkeyPatch) -> None:
    from pipeline.llm import GeminiClient, create_gemini_client

    monkeypatch.setenv("GEMINI_API_KEY", "k")
    monkeypatch.setenv("GEMINI_REVIEW_MODEL", "gemini-3.8-flash")
    monkeypatch.setenv("GEMINI_BULK_MODEL", "gemini-3.5-flash-lite")
    for name in ("GEMINI_REVIEW_FALLBACK_MODELS", "GEMINI_REVIEW_LAST_RESORT_MODELS",
                 "GEMINI_REVIEW_FLEX_MODEL", "GEMINI_FLEX_DISABLED"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(
        "pipeline.llm.flex_budget_seconds",
        lambda purpose: {"editorial": 240, "deduplication": 900}.get(purpose or "", 0),
    )

    editorial = create_gemini_client("review", purpose="editorial")
    assert editorial.labels == ("gemini-3.7-flash:flex", "gemini-3.8-flash:standard", "gemini-3.7-flash:standard")
    assert editorial._clients[0].timeout_seconds == 240 and editorial._clients[0].max_attempts == 1
    verification = create_gemini_client("review", purpose="editorial", last_resort=True)
    assert verification.models == ("gemini-3.7-flash", "gemini-3.8-flash", "gemini-3.5-flash")
    standard_only = create_gemini_client("review", purpose="digest")
    assert standard_only.labels == ("gemini-3.8-flash:standard", "gemini-3.7-flash:standard")
    dedup = create_gemini_client("review", include_lite=True, purpose="deduplication")
    assert dedup.labels[-1] == "gemini-3.5-flash-lite:standard" and dedup._clients[0].timeout_seconds == 900
    assert isinstance(create_gemini_client("bulk", purpose="digest"), GeminiClient)
    bulk_flex = create_gemini_client("bulk", purpose="deduplication")
    assert bulk_flex.labels == ("gemini-3.5-flash-lite:flex", "gemini-3.5-flash-lite:standard")
    for chain in (editorial, verification, standard_only, dedup, bulk_flex):
        chain.close()


def test_flex_budget_reads_config_and_disable_switch(monkeypatch: pytest.MonkeyPatch) -> None:
    from pipeline.config import PipelineConfig
    from pipeline.llm import flex_budget_seconds

    monkeypatch.setattr(
        "pipeline.config.load_pipeline_config",
        lambda: PipelineConfig(collection={}, aggregation={}, retention={}, pipeline={},
                               llm={"flex_budget_seconds": {"curation": 900}}),
    )
    monkeypatch.delenv("GEMINI_FLEX_DISABLED", raising=False)
    assert flex_budget_seconds("curation") == 900
    assert flex_budget_seconds("digest") == 0
    assert flex_budget_seconds(None) == 0
    monkeypatch.setenv("GEMINI_FLEX_DISABLED", "1")
    assert flex_budget_seconds("curation") == 0


def test_estimate_cost_and_usage_fields() -> None:
    from pipeline.llm import estimate_cost_usd, usage_fields

    prices = {"m": {"input": 1.0, "output": 4.0, "cached_input": 0.1, "flex_input": 0.5, "flex_output": 2.0}}
    assert estimate_cost_usd(
        model="m", service_tier=None, input_tokens=1_000_000, output_tokens=0, prices=prices
    ) == 1.0
    assert estimate_cost_usd(
        model="m", service_tier="flex", input_tokens=1_000_000, output_tokens=500_000,
        thinking_tokens=500_000, prices=prices,
    ) == 2.5
    assert estimate_cost_usd(
        model="m", service_tier=None, input_tokens=1_000_000, cached_tokens=500_000, output_tokens=0, prices=prices,
    ) == 0.55
    assert estimate_cost_usd(model="unknown", service_tier=None, input_tokens=5, output_tokens=5, prices=prices) is None
    assert usage_fields({"promptTokenCount": 10, "candidatesTokenCount": 2, "thoughtsTokenCount": 7,
                         "cachedContentTokenCount": 3, "serviceTier": "FLEX"}) == {
        "input_tokens": 10, "output_tokens": 2, "thinking_tokens": 7, "cached_tokens": 3, "service_tier": "flex",
    }
    assert usage_fields(None)["service_tier"] is None


def test_flex_clients_use_a_short_cooldown_of_their_own() -> None:
    calls: list[str] = []

    class FakeClient:
        def __init__(self, model: str, tier: str | None, *, cooldown: float | None = None, fail: bool = False) -> None:
            self.model, self.service_tier, self.cooldown_seconds, self.fail = model, tier, cooldown, fail

        @property
        def label(self) -> str:
            return f"{self.model}:{self.service_tier or 'standard'}"

        def generate_json(self, **kwargs: Any) -> GeminiResult:
            calls.append(self.label)
            if self.fail:
                raise GeminiRetryableError("shed", status_code=503)
            return GeminiResult(payload={}, model=self.model, elapsed_ms=1, usage={})

        def close(self) -> None:
            pass

    clock = {"now": 100.0}
    chain = FallbackGeminiClient(
        [FakeClient("m", "flex", cooldown=45.0, fail=True), FakeClient("m", None)],
        cooldown_seconds=300, monotonic=lambda: clock["now"],
    )
    chain.generate_json(system_instruction="s", prompt="p", response_schema={})
    clock["now"] = 130.0
    chain.generate_json(system_instruction="s", prompt="p", response_schema={})
    clock["now"] = 150.0
    chain.generate_json(system_instruction="s", prompt="p", response_schema={})
    assert calls == ["m:flex", "m:standard", "m:standard", "m:flex", "m:standard"]
