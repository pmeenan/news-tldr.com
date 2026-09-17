from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import httpx
import pytest

from pipeline.free_routing import (
    NEMOTRON,
    UNION,
    ZERO_PRICE,
    FreeFirstClient,
    FreeModelPolicy,
    validate_shape,
    zero_priced,
)
from pipeline.llm import GeminiResult, GeminiRetryableError, create_llm_client
from pipeline.state import StateDB, migrate

SCHEMA = {"type": "OBJECT", "properties": {"answer": {"type": "STRING"}}, "required": ["answer"]}
PARAMS = ["response_format", "structured_outputs", "reasoning"]


@pytest.fixture(autouse=True)
def environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("pipeline.llm.load_dotenv", lambda: {})
    monkeypatch.setattr("pipeline.openrouter.load_dotenv", lambda: {})
    for name in list(os.environ):
        if name.startswith(("LLM_", "OPENROUTER_")):
            monkeypatch.delenv(name)
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-key")


class Metadata:
    def __init__(self, *, remaining: int = 10) -> None:
        self.remaining = remaining
        self.prices = {UNION: {"prompt": "0", "completion": "0"}, NEMOTRON: {"prompt": "0", "completion": "0"}}
        self.calls: list[str] = []
        self.fail: str | None = None
        self.now = 100.0
        self.day = "2026-09-17"

    def handle(self, req: httpx.Request) -> httpx.Response:
        path = req.url.path.rsplit("/", 1)[-1]
        self.calls.append(path)
        if self.fail == path:
            return httpx.Response(503, json={"error": "unavailable"})
        if path == "models":
            return httpx.Response(200, json={"data": [
                {"id": model, "pricing": price, "supported_parameters": PARAMS}
                for model, price in self.prices.items()
            ]})
        assert path == "key"
        return httpx.Response(200, json={"data": {"free_model_daily_requests": {"remaining": self.remaining}}})

    def policy(self, **settings: Any) -> FreeModelPolicy:
        return FreeModelPolicy(api_key="test-key", settings=settings, transport=httpx.MockTransport(self.handle),
                               monotonic=lambda: self.now, utc_day=lambda: self.day)


class Client:
    def __init__(self, model: str, outputs: list[Any] | None = None) -> None:
        self.model = model
        self.outputs = outputs or []
        self.calls = 0
        self.closed = False

    def generate_json(self, **kwargs: Any) -> GeminiResult:
        self.calls += 1
        if self.outputs:
            value = self.outputs.pop(0)
            if isinstance(value, Exception):
                raise value
            return value
        return GeminiResult({"answer": "ok"}, self.model, 1, {"model": self.model, "costUsd": 0})

    def close(self) -> None:
        self.closed = True


def route(stage: str, metadata: Metadata, *, policy: FreeModelPolicy | None = None,
          settings: dict[str, Any] | None = None) -> FreeFirstClient:
    return FreeFirstClient(stage=stage, fallback=Client("gemini-review" if stage == "review" else "gemini-lite"),
                           settings=settings or {}, policy=policy or metadata.policy(),
                           candidates={m: Client(f"openrouter/{m}") for m in (NEMOTRON, UNION)}, sleep=lambda _: None)


def request(client: FreeFirstClient) -> GeminiResult:
    return client.generate_json(system_instruction="system", prompt="prompt", response_schema=SCHEMA)


@pytest.mark.parametrize("stage,expected", [("bulk", NEMOTRON), ("review", UNION)])
def test_role_selects_expected_model(stage: str, expected: str) -> None:
    metadata = Metadata()
    with route(stage, metadata) as client:
        result = request(client)
        assert result.model == f"openrouter/{expected}"
        assert client.fallback.calls == 0
        if stage == "review":
            assert "key" not in metadata.calls
    assert client.fallback.closed
    assert all(c.closed for c in client.candidates.values())


@pytest.mark.parametrize("quota", [0, None, -1, "10", True])
def test_exhausted_or_unknown_quota_skips_nemotron(quota: Any) -> None:
    metadata = Metadata(remaining=quota)
    with route("bulk", metadata) as client:
        assert request(client).model == f"openrouter/{UNION}"
        assert client.candidates[NEMOTRON].calls == 0


def test_unknown_quota_lookup_still_allows_union() -> None:
    metadata = Metadata()
    metadata.fail = "key"
    with route("bulk", metadata) as client:
        assert request(client).model == f"openrouter/{UNION}"


def test_unknown_price_lookup_goes_directly_to_gemini() -> None:
    metadata = Metadata()
    metadata.fail = "models"
    with route("bulk", metadata) as client:
        result = request(client)
        assert result.model == "gemini-lite"
        assert sum(c.calls for c in client.candidates.values()) == 0
        assert metadata.calls == ["models"]


@pytest.mark.parametrize("prices", [{}, {"prompt": "0"}, {"prompt": "0", "completion": None},
                                    {"prompt": "0.01", "completion": "0"},
                                    {"prompt": "0", "completion": "0", "request": "0.1"},
                                    {"prompt": "NaN", "completion": "0"},
                                    {"prompt": False, "completion": "0"},
                                    {"prompt": "-1", "completion": "0"}])
def test_nonzero_missing_and_invalid_prices_are_rejected(prices: dict[str, Any]) -> None:
    assert not zero_priced({"pricing": prices})
    metadata = Metadata()
    metadata.prices[UNION] = prices
    with route("review", metadata) as client:
        assert request(client).model == "gemini-review"
        assert client.candidates[UNION].calls == 0


def test_catalog_price_change_and_removed_model_fail_closed() -> None:
    metadata = Metadata()
    with route("review", metadata) as client:
        assert request(client).model == f"openrouter/{UNION}"
        metadata.prices[UNION]["completion"] = "0.01"
        metadata.now += 61
        assert request(client).model == "gemini-review"
        del metadata.prices[UNION]
        metadata.now += 61
        assert request(client).model == "gemini-review"
        assert client.candidates[UNION].calls == 1


def test_daily_reservations_are_atomic_and_do_not_reuse_stale_quota() -> None:
    metadata = Metadata(remaining=2)
    policy = metadata.policy(max_in_flight_per_model=40)
    def take(_: int) -> bool:
        if policy.acquire(NEMOTRON):
            return False
        try:
            return policy.reserve_attempt(NEMOTRON) is None
        finally:
            policy.release(NEMOTRON)
    with ThreadPoolExecutor(max_workers=20) as pool:
        assert sum(pool.map(take, range(40))) == 2
    assert metadata.calls.count("key") == 1
    metadata.now += 61
    assert take(0) is False  # Endpoint still claims two remaining; locally reserved already.
    metadata.day = "2026-09-18"
    assert take(0) is True


def test_per_minute_budget_routes_to_union_then_recovers() -> None:
    metadata = Metadata(remaining=100)
    policy = metadata.policy(free_requests_per_minute=1)
    with route("bulk", metadata, policy=policy) as client:
        assert request(client).model == f"openrouter/{NEMOTRON}"
        assert request(client).model == f"openrouter/{UNION}"
        metadata.now += 61
        assert request(client).model == f"openrouter/{NEMOTRON}"


def test_busy_model_does_not_queue_pipeline_workers() -> None:
    metadata = Metadata()
    policy = metadata.policy(max_in_flight_per_model=1)
    assert policy.acquire(UNION) is None
    with route("review", metadata, policy=policy) as client:
        assert request(client).model == "gemini-review"
        policy.release(UNION)
        assert request(client).model == f"openrouter/{UNION}"


def test_bounded_retries_then_shared_cooldown_and_recovery() -> None:
    metadata = Metadata()
    policy = metadata.policy(cooldown_seconds=120)
    with route("bulk", metadata, policy=policy) as first, route("bulk", metadata, policy=policy) as second:
        first.candidates[NEMOTRON].outputs = [GeminiRetryableError("overloaded", status_code=503)] * 2
        assert request(first).model == f"openrouter/{UNION}"
        assert first.candidates[NEMOTRON].calls == 2
        assert request(second).model == f"openrouter/{UNION}"
        assert second.candidates[NEMOTRON].calls == 0
        metadata.now += 121
        assert request(second).model == f"openrouter/{NEMOTRON}"


@pytest.mark.parametrize("status", [429, 503])
def test_capacity_error_honors_retry_after_and_moves_to_gemini_without_waiting(status: int) -> None:
    metadata = Metadata()
    error = GeminiRetryableError("busy", status_code=status)
    error.retry_after_seconds = 3600
    with route("review", metadata) as client:
        client.candidates[UNION].outputs = [error]
        assert request(client).model == "gemini-review"
        assert client.candidates[UNION].calls == 1
        metadata.now += 300
        assert request(client).model == "gemini-review"
        assert client.candidates[UNION].calls == 1
        metadata.now += 3301
        assert request(client).model == f"openrouter/{UNION}"


def test_quota_refresh_recovers_after_midnight_metadata_outage() -> None:
    metadata = Metadata(remaining=0)
    policy = metadata.policy()
    assert policy.acquire(NEMOTRON) is not None
    metadata.day = "2026-09-18"
    metadata.fail = "key"
    assert policy.acquire(NEMOTRON) is not None
    metadata.fail = None
    metadata.remaining = 1000
    metadata.now += 61
    assert policy.acquire(NEMOTRON) is None
    policy.release(NEMOTRON)


def test_both_candidates_fail_then_use_lite_and_account_rejected_output(tmp_path: Path) -> None:
    metadata = Metadata()
    with route("bulk", metadata) as client:
        invalid = GeminiResult({}, f"openrouter/{NEMOTRON}", 10, {"costUsd": 0, "promptTokenCount": 123})
        client.candidates[NEMOTRON].outputs = [invalid, invalid]
        client.candidates[UNION].outputs = [RuntimeError("endpoint unavailable")]
        result = request(client)
        assert result.model == "gemini-lite"
        assert len(result.usage["routingAttempts"]) == 2
        path = tmp_path / "usage.db"
        migrate(path)
        with StateDB(path) as db:
            db.record_llm_usage("test", "digest", client.model, "v1", usage=result.usage)
            rows = db.conn.execute("SELECT model,cost_usd,input_tokens FROM llm_usage ORDER BY id").fetchall()
            assert len(rows) == 3
            assert rows[0]["model"] == f"openrouter/{NEMOTRON}"
            assert rows[0]["input_tokens"] == 123
            assert rows[2]["model"] == "gemini-lite"
            assert sum(row["cost_usd"] for row in rows) == 0


@pytest.mark.parametrize("cost", [None, 0.1])
def test_unexpected_or_unknown_cost_disables_model(cost: float | None) -> None:
    metadata = Metadata()
    with route("review", metadata) as client:
        client.candidates[UNION].outputs = [GeminiResult({"answer": "ok"}, f"openrouter/{UNION}", 1,
                                                        {"costUsd": cost})]
        result = request(client)
        assert result.model == "gemini-review"
        assert result.usage["routingAttempts"][0]["usage"]["costUsd"] == cost
        assert request(client).model == "gemini-review"
        assert client.candidates[UNION].calls == 1


def test_fallback_errors_propagate_without_fabricated_success() -> None:
    metadata = Metadata(remaining=0)
    metadata.prices[UNION]["prompt"] = "1"
    with route("bulk", metadata) as client:
        client.fallback.outputs = [GeminiRetryableError("all Gemini models unavailable")]
        with pytest.raises(GeminiRetryableError):
            request(client)


def test_factory_opt_in_and_explicit_gemini_override(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []
    def fallback(stage: str, **kwargs: Any) -> Client:
        calls.append((stage, kwargs))
        return Client("gemini-test")
    monkeypatch.setattr("pipeline.llm.create_gemini_client", fallback)
    monkeypatch.setenv("LLM_BACKEND", "free-first")
    monkeypatch.setenv("OPENROUTER_REASONING_EFFORT", "high")
    with create_llm_client("review", include_lite=True, purpose="editorial", last_resort=True) as client:
        assert client.order == (UNION,)
        assert client.candidates[UNION].reasoning_effort is None
        assert client.candidates[UNION].provider_preferences["max_price"] == ZERO_PRICE
        assert client.candidates[UNION].response_format == "json_object"
        assert client.candidates[UNION].max_attempts == 1  # Router owns retries.
    assert calls[0] == ("review", {"include_lite": True, "purpose": "editorial", "last_resort": True,
                                   "progress": None})
    with create_llm_client("bulk") as client:
        assert client.order == (NEMOTRON, UNION)
        assert client.candidates[NEMOTRON].reasoning_effort == "low"
        assert client.candidates[NEMOTRON].response_format == "json_schema"
    assert isinstance(create_llm_client("review", backend="gemini"), Client)
    with pytest.raises(ValueError, match="explicit backend"):
        create_llm_client("review", model="vendor/model")


def test_missing_key_goes_direct_to_gemini(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPENROUTER_API_KEY")
    with FreeFirstClient(stage="review", fallback=Client("gemini-test"), settings={}) as client:
        assert request(client).model == "gemini-test"


def test_shape_validation_checks_required_nested_enum_and_numbers() -> None:
    schema = {"type": "OBJECT", "properties": {"rows": {"type": "ARRAY", "items": {
        "type": "OBJECT", "properties": {"score": {"type": "NUMBER"},
        "label": {"type": "STRING", "enum": ["good"]}}, "required": ["score", "label"]}}}, "required": ["rows"]}
    validate_shape({"rows": [{"score": 0.5, "label": "good"}]}, schema)
    for bad in ({}, {"rows": [{}]}, {"rows": [{"score": True, "label": "good"}]},
                {"rows": [{"score": float("nan"), "label": "good"}]},
                {"rows": [{"score": 0.5, "label": "bad"}]}):
        with pytest.raises(ValueError):
            validate_shape(bad, schema)


def test_real_adapter_sends_zero_price_ceiling(monkeypatch: pytest.MonkeyPatch) -> None:
    metadata = Metadata()
    with FreeFirstClient(stage="review", fallback=Client("gemini-test"), settings={},
                         policy=metadata.policy()) as client:
        def handle(req: httpx.Request) -> httpx.Response:
            body = json.loads(req.content)
            assert body["provider"]["max_price"] == ZERO_PRICE
            assert body["model"] == UNION
            assert "models" not in body and "reasoning" not in body
            return httpx.Response(200, json={"model": UNION, "usage": {"cost": 0}, "choices": [
                {"finish_reason": "stop", "message": {"content": '{"answer":"ok"}'}}]})
        with httpx.Client(transport=httpx.MockTransport(handle)) as http:
            monkeypatch.setattr(client.candidates[UNION], "_http_client", http)
            assert request(client).model == f"openrouter/{UNION}"
