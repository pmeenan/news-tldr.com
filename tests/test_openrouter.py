from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
from typing import Any

import httpx
import pytest

from pipeline.llm import (
    GeminiEmptyResponseError,
    GeminiResult,
    GeminiRetryableError,
    GeminiTruncatedError,
    create_llm_client,
)
from pipeline.openrouter import OpenRouterClient, json_schema, normalized_usage
from pipeline.state import StateDB, migrate

SCHEMA = {
    "type": "OBJECT", "properties": {"answer": {"type": "STRING"}, "optional": {"type": "STRING"}},
    "required": ["answer"],
}


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("pipeline.llm.load_dotenv", lambda: {})
    monkeypatch.setattr("pipeline.openrouter.load_dotenv", lambda: {})
    for name in list(os.environ):
        if name.startswith(("LLM_", "OPENROUTER_")):
            monkeypatch.delenv(name)
    monkeypatch.setenv("OPENROUTER_API_KEY", "fake-openrouter-key")


def response_data(**overrides: Any) -> dict[str, Any]:
    return {
        "id": "gen-test", "model": "vendor/actual-model", "provider": "test-provider",
        "choices": [{"finish_reason": "stop", "message": {"content": '{"answer":"ok","optional":null}'}}],
        "usage": {"prompt_tokens": 100, "completion_tokens": 25, "cost": 0.0123,
                  "completion_tokens_details": {"reasoning_tokens": 20},
                  "prompt_tokens_details": {"cached_tokens": 40}},
        **overrides,
    }


def request(client: OpenRouterClient) -> GeminiResult:
    return client.generate_json(system_instruction="system", prompt="prompt", response_schema=SCHEMA)


def test_request_preserves_schema_and_provider_constraints() -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        assert str(req.url) == "https://openrouter.ai/api/v1/chat/completions"
        assert req.headers["authorization"] == "Bearer fake-openrouter-key"
        body = json.loads(req.content)
        assert body["model"] == "vendor/test:free"
        assert "models" not in body  # No accidental model or paid fallback.
        assert body["provider"] == {"only": ["one"], "allow_fallbacks": False, "require_parameters": True}
        assert body["reasoning"] == {"effort": "low", "exclude": True}
        assert body["max_tokens"] == 256
        schema = body["response_format"]["json_schema"]["schema"]
        assert schema["type"] == "object"
        assert schema["required"] == ["answer", "optional"]
        assert schema["additionalProperties"] is False
        assert schema["properties"]["optional"]["anyOf"][-1] == {"type": "null"}
        return httpx.Response(200, json=response_data())

    with OpenRouterClient(
        model="vendor/test:free", max_output_tokens=256, reasoning_effort="low",
        provider_preferences={"only": ["one"], "allow_fallbacks": False, "require_parameters": False},
        transport=httpx.MockTransport(handler),
    ) as client:
        result = request(client)
    assert result.payload == {"answer": "ok"}
    assert result.model == "openrouter/vendor/actual-model"
    assert result.usage["provider"] == "test-provider"
    assert result.usage["generationId"] == "gen-test"
    assert result.usage["candidatesTokenCount"] == 5
    assert result.usage["thoughtsTokenCount"] == 20
    assert SCHEMA["type"] == "OBJECT"
    assert "additionalProperties" not in SCHEMA


def test_schema_conversion_preserves_property_names_and_enum_values() -> None:
    schema = {"type": "OBJECT", "properties": {"type": {"type": "STRING", "enum": ["OBJECT"]},
              "items": {"type": "ARRAY", "items": {"type": "INTEGER"}}}, "required": ["type", "items"]}
    result = json_schema(schema)
    assert result["properties"]["type"] == {"type": "string", "enum": ["OBJECT"]}
    assert result["properties"]["items"]["items"] == {"type": "integer"}


def test_explicit_json_object_mode_still_supplies_schema() -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        body = json.loads(req.content)
        assert body["response_format"] == {"type": "json_object"}
        assert '"required": ["answer", "optional"]' in body["messages"][0]["content"]
        assert "reasoning" not in body
        return httpx.Response(200, json=response_data())
    with OpenRouterClient(model="vendor/test", response_format="json_object",
                          transport=httpx.MockTransport(handler)) as client:
        result = client.generate_json(
            system_instruction="system", prompt="prompt", response_schema=SCHEMA, thinking_level="low",
        )
        assert result.payload == {"answer": "ok"}


@pytest.mark.parametrize(("status", "body"), [(429, {}), (503, {}), (200, {"error": {"code": 429}})])
def test_capacity_errors_retry_same_model(status: int, body: dict[str, Any]) -> None:
    calls = []
    sleeps = []
    def handler(req: httpx.Request) -> httpx.Response:
        calls.append(json.loads(req.content)["model"])
        if len(calls) == 1:
            return httpx.Response(status, json=body, headers={"Retry-After": "4"})
        return httpx.Response(200, json=response_data())
    with OpenRouterClient(model="vendor/test:free", transport=httpx.MockTransport(handler),
                          sleep=sleeps.append) as client:
        request(client)
    assert calls == ["vendor/test:free", "vendor/test:free"]
    assert sleeps == [4]


def test_daily_limit_is_not_retried_early() -> None:
    sleeps = []
    with OpenRouterClient(model="vendor/test:free", sleep=sleeps.append,
                          transport=httpx.MockTransport(lambda req: httpx.Response(
                              429, headers={"Retry-After": "3600"}, json={"error": "quota"},
                          ))) as client, pytest.raises(GeminiRetryableError, match="deferred"):
        request(client)
    assert sleeps == []


@pytest.mark.parametrize("status", [400, 401, 402, 403])
def test_fatal_errors_do_not_retry(status: int) -> None:
    calls = []
    def handler(req: httpx.Request) -> httpx.Response:
        calls.append(req)
        return httpx.Response(status, json={"error": {"message": "unavailable"}})
    with OpenRouterClient(model="vendor/test", transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(RuntimeError, match=f"HTTP {status}"):
            request(client)
    assert len(calls) == 1


@pytest.mark.parametrize(("choice", "error"), [
    ({"finish_reason": "length", "message": {"content": "{}"}}, GeminiTruncatedError),
    ({"finish_reason": "stop", "message": {"content": None, "reasoning": "hidden"}}, GeminiEmptyResponseError),
    ({"finish_reason": "stop", "message": {"refusal": "no"}}, GeminiEmptyResponseError),
    ({"finish_reason": "stop", "message": {"content": "not JSON"}}, ValueError),
    ({"finish_reason": "stop", "message": {"content": "[]"}}, ValueError),
    ({"finish_reason": "content_filter", "message": {"content": "{}"}}, RuntimeError),
])
def test_invalid_answers_fail_with_usage_retained(choice: dict[str, Any], error: type[Exception]) -> None:
    with OpenRouterClient(model="vendor/test", transport=httpx.MockTransport(
        lambda req: httpx.Response(200, json=response_data(choices=[choice])),
    )) as client, pytest.raises(error) as caught:
        request(client)
    assert caught.value.llm_result.usage["costUsd"] == 0.0123


@pytest.mark.parametrize("cost", [0, 0.0123, None])
def test_usage_stores_real_cost_without_double_counting_reasoning(tmp_path: Path, cost: float | None) -> None:
    data = response_data()
    data["usage"]["cost"] = cost
    usage = normalized_usage(data, "vendor/test")
    path = tmp_path / "state.db"
    migrate(path)
    with StateDB(path) as state:
        state.record_llm_usage("test", "editorial", "openrouter/vendor/test", "v1", usage=usage)
        row = state.conn.execute("SELECT * FROM llm_usage").fetchone()
        assert row["cost_usd"] == cost
        assert row["input_tokens"] == 100
        assert row["output_tokens"] == 5
        assert row["thinking_tokens"] == 20
        assert row["cached_tokens"] == 40
        assert row["service_tier"] == "openrouter"
        assert row["model"] == "openrouter/vendor/actual-model"


def test_factory_preserves_gemini_defaults_and_supports_tier_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    sentinel = object()
    calls = []
    def gemini(stage: str, **kwargs: Any) -> Any:
        calls.append((stage, kwargs))
        return sentinel
    monkeypatch.setattr("pipeline.llm.create_gemini_client", gemini)
    assert create_llm_client("review", purpose="editorial", last_resort=True) is sentinel
    assert calls[0][1]["last_resort"] is True
    monkeypatch.setenv("LLM_BULK_BACKEND", "openrouter")
    monkeypatch.setenv("OPENROUTER_BULK_MODEL", "vendor/bulk:free")
    with create_llm_client("bulk") as client:
        assert client.requested_model == "vendor/bulk:free"
    assert create_llm_client("review") is sentinel
    assert create_llm_client("bulk", backend="gemini") is sentinel
    with create_llm_client("review", backend="openrouter", model="vendor/pinned") as client:
        assert client.requested_model == "vendor/pinned"


def test_factory_rejects_unconfigured_backend_or_model() -> None:
    with pytest.raises(ValueError, match="backend"):
        create_llm_client("bulk", backend="typo")
    with pytest.raises(RuntimeError, match="explicit model"):
        create_llm_client("review", backend="openrouter")


def test_evaluation_retains_usage_for_failed_story_and_does_not_publish(monkeypatch: pytest.MonkeyPatch) -> None:
    path = Path(__file__).resolve().parents[1] / "scripts/evaluate-editorial.py"
    spec = importlib.util.spec_from_file_location("editorial_evaluation", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    class Client:
        model = "openrouter/vendor/test"

        def generate_json(self, **kwargs: Any) -> GeminiResult:
            return GeminiResult({"answer": "ok"}, self.model, 100, {"costUsd": 0.01})

    def fail(event: Any, **kwargs: Any) -> None:
        kwargs["client"].generate_json()
        raise ValueError("unsupported claim")

    monkeypatch.setattr(module, "generate_story", fail)
    row = module.evaluate_case({"id": "test", "title": "test", "category": "world",
                                "reports": ["report"], "expected": {}},
                               dict.fromkeys(["draft", "evidence", "verification"], Client()))
    assert row["automatic_validation"] == "failed"
    assert row["recorded_cost_usd"] == 0.01
    assert len(row["usage_records"]) == 1
    assert "story" not in row


@pytest.mark.parametrize("date_header", [False, True])
def test_single_attempt_preserves_rate_limit_retry_hint(date_header: bool) -> None:
    from datetime import UTC, datetime, timedelta
    from email.utils import format_datetime
    header = format_datetime(datetime.now(UTC) + timedelta(hours=1)) if date_header else "3600"
    with OpenRouterClient(model="vendor/test", max_attempts=1, transport=httpx.MockTransport(
        lambda req: httpx.Response(429, headers={"Retry-After": header}, json={"error": "busy"}),
    )) as client, pytest.raises(GeminiRetryableError) as caught:
        request(client)
    assert caught.value.status_code == 429
    assert 3598 <= caught.value.retry_after_seconds <= 3600
