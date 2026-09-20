from __future__ import annotations

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import httpx
import pytest

from pipeline.free_routing import (
    NEMOTRON,
    ZERO_PRICE,
    FreeFirstClient,
    FreeModelPolicy,
    validate_shape,
    zero_priced,
)
from pipeline.llm import GeminiResult, GeminiRetryableError, create_llm_client, generate_validated_json
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
        self.prices = {NEMOTRON: {"prompt": "0", "completion": "0"}}
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

    def sleep(self, seconds: float) -> None:
        self.now += seconds


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
    policy = policy or metadata.policy(**(settings or {}))
    return FreeFirstClient(stage=stage, fallback=Client("gemini-review" if stage == "review" else "gemini-lite"),
                           settings=settings if settings is not None else policy.settings, policy=policy,
                           candidates={NEMOTRON: Client(f"openrouter/{NEMOTRON}")}, sleep=metadata.sleep)


def request(client: FreeFirstClient) -> GeminiResult:
    return client.generate_json(system_instruction="system", prompt="prompt", response_schema=SCHEMA)


@pytest.mark.parametrize("stage,expected", [("bulk", f"openrouter/{NEMOTRON}"), ("review", "gemini-review")])
def test_role_selects_expected_model(stage: str, expected: str) -> None:
    metadata = Metadata()
    with route(stage, metadata) as client:
        result = request(client)
        assert result.model == expected
        assert client.fallback.calls == (1 if stage == "review" else 0)
        if stage == "review":
            assert metadata.calls == []
            assert client.candidates[NEMOTRON].calls == 0
    assert client.fallback.closed
    assert all(c.closed for c in client.candidates.values())


@pytest.mark.parametrize("paid_repair", [False, True])
def test_incomplete_free_grouping_uses_lite_without_consuming_paid_repair(tmp_path, paid_repair):
    from pipeline.aggregate import ArticleForAggregation, group_articles_with_gemini

    def response(count, model):
        payload = {"articles": [{"article_index": i, "content_type": "news", "category": "world",
                                 "grouping_confidence": 0.9} for i in range(count)],
                   "groups": [{"article_indexes": [i], "existing_event_id": ""} for i in range(count)]}
        return GeminiResult(payload, model, 1, {"model": model, "costUsd": 0 if "openrouter" in model else 0.01})

    articles = [ArticleForAggregation(str(i), "source", "Source", f"Story {i}", "Reporting.",
                                     "2026-09-18T00:00:00Z", "unused") for i in range(34)]
    with route("bulk", Metadata()) as client:
        client.candidates[NEMOTRON].outputs = [response(32, f"openrouter/{NEMOTRON}")]
        client.fallback.outputs = ([response(33, "gemini-lite")] if paid_repair else []) + [response(34, "gemini-lite")]
        result = group_articles_with_gemini(articles, mode="titles_summaries", client=client)
        assert len(result["article_classifications"]) == 34
        assert client.candidates[NEMOTRON].calls == 1
        assert client.fallback.calls == 1 + paid_repair
        # Another task must still be eligible for the free model.
        assert request(client).model == f"openrouter/{NEMOTRON}"
    db = tmp_path / "usage.db"
    migrate(db)
    with StateDB(db) as state:
        state.record_llm_usage("test", "aggregation", "gemini-lite", "test", usage=result["usage"])
        rows = state.conn.execute("SELECT model,cost_usd FROM llm_usage").fetchall()
    assert len(rows) == 2 + paid_repair
    assert sum(r["cost_usd"] for r in rows) == pytest.approx(0.01 * (1 + paid_repair))


def test_domain_fallback_propagates_invalid_paid_answer_and_retains_usage():
    def validate(payload):
        if payload["answer"] != "valid":
            raise ValueError("invalid answer")

    with route("bulk", Metadata()) as client:
        session = client.validation_session()
        with pytest.raises(ValueError) as error:
            generate_validated_json(session, validate, system_instruction="s", prompt="p", response_schema=SCHEMA)
        assert error.value.llm_result.model == "gemini-lite"
        assert len(error.value.llm_result.usage["routingAttempts"]) == 1
        with pytest.raises(ValueError):
            generate_validated_json(session, validate, system_instruction="s", prompt="p", response_schema=SCHEMA)
        assert client.candidates[NEMOTRON].calls == 1
        assert client.fallback.calls == 2


def test_invalid_shape_goes_to_lite_without_second_free_attempt():
    with route("bulk", Metadata()) as client:
        client.candidates[NEMOTRON].outputs = [GeminiResult({}, f"openrouter/{NEMOTRON}", 1, {"costUsd": 0})]
        assert request(client).model == "gemini-lite"
        assert client.candidates[NEMOTRON].calls == 1


def test_direct_model_validation_never_invokes_an_unrequested_fallback():
    client = Client("openrouter/pinned-evaluation")
    def reject(payload):
        raise ValueError("invalid")
    with pytest.raises(ValueError):
        generate_validated_json(client, reject, system_instruction="s", prompt="p", response_schema=SCHEMA)
    assert client.calls == 1


def test_unknown_evidence_passage_uses_lite_and_preserves_exact_quote():
    from types import SimpleNamespace

    from pipeline.evidence import collect_evidence

    event = SimpleNamespace(title="Case count", articles=[SimpleNamespace(
        article_id="article", source_name="Outlet", headline="Case count", published_at="2026-09-18",
        content="Officials reported 12 cases.",
    )])
    def response(passage, model):
        return GeminiResult({"claims": [{"text": "Officials reported 12 cases.", "status": "reported",
                                         "passage_ids": [passage]}]}, model, 1, {"model": model, "costUsd": 0})
    with route("bulk", Metadata()) as client:
        client.candidates[NEMOTRON].outputs = [response("invented", f"openrouter/{NEMOTRON}")]
        client.fallback.outputs = [response("a0p0", "gemini-lite")]
        ledger, result = collect_evidence(event, client)
        assert result.model == "gemini-lite"
        assert ledger[0]["evidence"] == [{"article_id": "article", "quote": event.articles[0].content}]
        assert client.candidates[NEMOTRON].calls == client.fallback.calls == 1
        assert len(result.usage["routingAttempts"]) == 1


def test_domain_fallback_transport_failure_keeps_free_attempt_for_accounting():
    with route("bulk", Metadata()) as client:
        client.fallback.outputs = [GeminiRetryableError("unavailable")]
        def reject(payload):
            raise ValueError("invalid answer")
        with pytest.raises(GeminiRetryableError) as error:
            generate_validated_json(client, reject, system_instruction="s", prompt="p", response_schema=SCHEMA)
        assert error.value.routing_attempts[0]["model"] == f"openrouter/{NEMOTRON}"
        assert client.candidates[NEMOTRON].calls == client.fallback.calls == 1


@pytest.mark.parametrize("quota", [0, None, -1, "10", True])
def test_exhausted_or_unknown_quota_skips_nemotron(quota: Any) -> None:
    metadata = Metadata(remaining=quota)
    with route("bulk", metadata) as client:
        assert request(client).model == "gemini-lite"
        assert client.candidates[NEMOTRON].calls == 0


def test_unknown_quota_lookup_goes_directly_to_gemini() -> None:
    metadata = Metadata()
    metadata.fail = "key"
    with route("bulk", metadata) as client:
        assert request(client).model == "gemini-lite"
        assert client.candidates[NEMOTRON].calls == 0


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
    metadata.prices[NEMOTRON] = prices
    with route("bulk", metadata) as client:
        assert request(client).model == "gemini-lite"
        assert client.candidates[NEMOTRON].calls == 0


def test_catalog_price_change_and_removed_model_fail_closed() -> None:
    metadata = Metadata()
    with route("bulk", metadata) as client:
        assert request(client).model == f"openrouter/{NEMOTRON}"
        metadata.prices[NEMOTRON]["completion"] = "0.01"
        metadata.now += 61
        assert request(client).model == "gemini-lite"
        del metadata.prices[NEMOTRON]
        metadata.now += 61
        assert request(client).model == "gemini-lite"
        assert client.candidates[NEMOTRON].calls == 1


def test_retired_union_is_ignored_even_if_catalog_lists_it() -> None:
    metadata = Metadata(remaining=0)
    retired = "stealth/union-alpha"
    metadata.prices[retired] = {"prompt": "0", "completion": "0"}
    policy = metadata.policy()
    for stage in ("bulk", "review"):
        with route(stage, metadata, policy=policy) as client:
            retired_client = Client(f"openrouter/{retired}")
            client.candidates[retired] = retired_client
            assert request(client).model == client.fallback.model
            assert retired_client.calls == 0
            assert f"openrouter/{retired}" not in client.models
    assert retired not in policy._catalog


def test_daily_reservations_are_atomic_and_do_not_reuse_stale_quota() -> None:
    metadata = Metadata(remaining=2)
    policy = metadata.policy(max_in_flight_per_model=40)
    def take(_: int) -> bool:
        if policy.reserve_attempt(NEMOTRON):
            return False
        try:
            return True
        finally:
            policy.release(NEMOTRON)
    with ThreadPoolExecutor(max_workers=20) as pool:
        assert sum(pool.map(take, range(40))) == 1  # Pacing is atomic too.
        metadata.now += 3.1
        assert sum(pool.map(take, range(40))) == 1
        metadata.now += 3.1
        assert sum(pool.map(take, range(40))) == 0
    assert metadata.calls.count("key") == 1
    metadata.now += 61
    assert take(0) is False  # Endpoint still claims two remaining; locally reserved already.
    metadata.day = "2026-09-18"
    assert take(0) is True


def test_per_minute_budget_waits_for_nemotron_without_paid_fallback() -> None:
    metadata = Metadata(remaining=100)
    policy = metadata.policy(free_requests_per_minute=1)
    with route("bulk", metadata, policy=policy) as client:
        assert request(client).model == f"openrouter/{NEMOTRON}"
        first = metadata.now
        assert request(client).model == f"openrouter/{NEMOTRON}"
        assert metadata.now - first == pytest.approx(60.05)
        assert client.fallback.calls == 0


def test_busy_model_waits_only_within_budget() -> None:
    metadata = Metadata()
    policy = metadata.policy(max_in_flight_per_model=1)
    assert policy.reserve_attempt(NEMOTRON) is None
    with route("bulk", metadata, policy=policy, settings={"max_wait_seconds": 5}) as client:
        assert request(client).model == "gemini-lite"
        assert metadata.now == 105
        policy.release(NEMOTRON)
        assert request(client).model == f"openrouter/{NEMOTRON}"


def test_bounded_retries_then_shared_cooldown_and_recovery() -> None:
    metadata = Metadata()
    policy = metadata.policy(cooldown_seconds=120)
    with route("bulk", metadata, policy=policy) as first, route("bulk", metadata, policy=policy) as second:
        first.candidates[NEMOTRON].outputs = [GeminiRetryableError("overloaded", status_code=503)] * 2
        assert request(first).model == "gemini-lite"
        assert first.candidates[NEMOTRON].calls == 2
        assert request(second).model == "gemini-lite"
        assert second.candidates[NEMOTRON].calls == 0
        metadata.now += 121
        assert request(second).model == f"openrouter/{NEMOTRON}"


@pytest.mark.parametrize("status", [429, 503])
def test_capacity_error_honors_retry_after_and_moves_to_gemini_without_waiting(status: int) -> None:
    metadata = Metadata()
    error = GeminiRetryableError("busy", status_code=status)
    error.retry_after_seconds = 3600
    with route("bulk", metadata) as client:
        client.candidates[NEMOTRON].outputs = [error]
        assert request(client).model == "gemini-lite"
        assert client.candidates[NEMOTRON].calls == 1
        metadata.now += 300
        assert request(client).model == "gemini-lite"
        assert client.candidates[NEMOTRON].calls == 1
        metadata.now += 3301
        assert request(client).model == f"openrouter/{NEMOTRON}"


def test_all_attempts_are_spaced_within_rolling_minute() -> None:
    metadata = Metadata(remaining=100)
    with route("bulk", metadata) as client:
        timestamps = []
        for _ in range(40):
            assert request(client).model == f"openrouter/{NEMOTRON}"
            timestamps.append(metadata.now)
        assert all(b - a >= 3.049 for a, b in zip(timestamps, timestamps[1:], strict=False))
        assert max(sum(t <= other < t + 60 for other in timestamps) for t in timestamps) == 20
        assert client.fallback.calls == 0
        assert client.policy._remaining == 60


def test_concurrent_bulk_stage_clients_share_nemotron_queue() -> None:
    metadata = Metadata(remaining=100)
    policy = metadata.policy()
    # Accelerate real monotonic time so real threads exercise the complete queue
    # without taking two minutes. No external requests are made.
    start = time.monotonic()
    policy._clock = lambda: 100 + (time.monotonic() - start) * 100
    logs: list[str] = []
    with route("bulk", metadata, policy=policy) as first, route("bulk", metadata, policy=policy) as second:
        for client in (first, second):
            client._sleep = lambda seconds: time.sleep(seconds / 100)
            client._progress = logs.append
        barrier = threading.Barrier(40)
        def run(index: int) -> str:
            barrier.wait(timeout=5)
            return request(first if index % 2 else second).model
        with ThreadPoolExecutor(max_workers=40) as pool:
            models = list(pool.map(run, range(40)))
        assert models == [f"openrouter/{NEMOTRON}"] * 40
        assert first.fallback.calls == second.fallback.calls == 0
        assert policy._active[NEMOTRON] == 0
        assert policy._remaining == 60
        assert not policy._queues[NEMOTRON]
        assert any("wait for" in entry for entry in logs)
        assert policy._clock() - 100 >= 39 * 3.05


def test_429_waits_and_retries_without_gemini() -> None:
    metadata = Metadata()
    with route("bulk", metadata) as client:
        client.candidates[NEMOTRON].outputs = [GeminiRetryableError("busy", status_code=429)]
        assert request(client).model == f"openrouter/{NEMOTRON}"
        assert metadata.now == 160
        assert client.candidates[NEMOTRON].calls == 2
        assert client.fallback.calls == 0


def test_429_retry_hint_is_shared_and_retry_count_remains_bounded() -> None:
    metadata = Metadata()
    policy = metadata.policy()
    error = GeminiRetryableError("busy", status_code=429)
    error.retry_after_seconds = 75
    with route("bulk", metadata, policy=policy) as first, route("bulk", metadata, policy=policy) as second:
        first.candidates[NEMOTRON].outputs = [error, error]
        assert request(first).model == "gemini-lite"
        assert metadata.now == 175
        assert first.candidates[NEMOTRON].calls == 2
        assert request(second).model == f"openrouter/{NEMOTRON}"
        assert metadata.now == 250
        assert second.fallback.calls == 0


def test_wait_budget_is_shared_across_retries() -> None:
    metadata = Metadata()
    policy = metadata.policy(max_wait_seconds=100, max_attempts=3)
    with route("bulk", metadata, policy=policy) as client:
        error = GeminiRetryableError("busy", status_code=429)
        client.candidates[NEMOTRON].outputs = [error, error]
        result = request(client)
        assert result.model == "gemini-lite"
        assert metadata.now == 160  # A further 60-second wait would exceed the original 100-second budget.
        assert client.candidates[NEMOTRON].calls == 2
        assert "wait budget" in result.usage["routingDecisions"][-1]["reason"]
        assert not any(policy._active.values())
        assert not any(policy._queues.values())


@pytest.mark.parametrize("change", ["price", "quota", "metadata_failure"])
def test_queued_nemotron_rechecks_eligibility_without_consuming_waiting_quota(change: str) -> None:
    metadata = Metadata()
    policy = metadata.policy(metadata_ttl_seconds=1, free_requests_per_minute=1)
    with route("bulk", metadata, policy=policy) as client:
        assert request(client).model == f"openrouter/{NEMOTRON}"
        assert policy._remaining == 9
        def sleep(seconds: float) -> None:
            assert policy._remaining == 9
            assert policy._active[NEMOTRON] == 0
            if change == "price":
                metadata.prices[NEMOTRON]["completion"] = "1"
            elif change == "quota":
                metadata.remaining = 0
            else:
                metadata.fail = "key"
            metadata.sleep(seconds)
        client._sleep = sleep
        assert request(client).model == "gemini-lite"
        assert client.candidates[NEMOTRON].calls == 1
        assert not policy._queues[NEMOTRON]


def test_cancelled_wait_removes_ticket_and_preserves_quota() -> None:
    metadata = Metadata()
    policy = metadata.policy()
    assert policy.reserve_attempt(NEMOTRON) is None
    policy.release(NEMOTRON)
    def interrupt(seconds: float) -> None:
        raise KeyboardInterrupt
    with pytest.raises(KeyboardInterrupt):
        policy.reserve_attempt(NEMOTRON, deadline=200, sleep=interrupt)
    assert not policy._queues[NEMOTRON]
    assert policy._remaining == 9
    assert policy._active[NEMOTRON] == 0
    metadata.now += 4
    assert policy.reserve_attempt(NEMOTRON) is None
    policy.release(NEMOTRON)


def test_busy_model_recovers_while_waiting_without_fallback() -> None:
    metadata = Metadata()
    policy = metadata.policy(max_in_flight_per_model=1)
    assert policy.reserve_attempt(NEMOTRON) is None
    with route("bulk", metadata, policy=policy) as client:
        released = False
        def sleep(seconds: float) -> None:
            nonlocal released
            if not released:
                policy.release(NEMOTRON)
                released = True
            metadata.sleep(seconds)
        client._sleep = sleep
        assert request(client).model == f"openrouter/{NEMOTRON}"
        assert client.fallback.calls == 0
        assert policy._active[NEMOTRON] == 0


def test_late_wakeup_does_not_admit_expired_waiter() -> None:
    metadata = Metadata()
    policy = metadata.policy()
    assert policy.reserve_attempt(NEMOTRON) is None
    policy.release(NEMOTRON)
    reason = policy.reserve_attempt(NEMOTRON, deadline=105, sleep=lambda _: metadata.sleep(10))
    assert reason == "request queue exceeds free wait budget"
    assert policy._remaining == 9
    assert not policy._queues[NEMOTRON]


def test_quota_refresh_recovers_after_midnight_metadata_outage() -> None:
    metadata = Metadata(remaining=0)
    policy = metadata.policy()
    assert policy.reserve_attempt(NEMOTRON) is not None
    metadata.day = "2026-09-18"
    metadata.fail = "key"
    assert policy.reserve_attempt(NEMOTRON) is not None
    metadata.fail = None
    metadata.remaining = 1000
    metadata.now += 61
    assert policy.reserve_attempt(NEMOTRON) is None
    policy.release(NEMOTRON)


def test_nemotron_fails_then_use_lite_and_account_rejected_output(tmp_path: Path) -> None:
    metadata = Metadata()
    with route("bulk", metadata) as client:
        invalid = GeminiResult({}, f"openrouter/{NEMOTRON}", 10, {"costUsd": 0, "promptTokenCount": 123})
        client.candidates[NEMOTRON].outputs = [invalid, invalid]
        result = request(client)
        assert result.model == "gemini-lite"
        assert len(result.usage["routingAttempts"]) == 1
        path = tmp_path / "usage.db"
        migrate(path)
        with StateDB(path) as db:
            db.record_llm_usage("test", "digest", client.model, "v1", usage=result.usage)
            rows = db.conn.execute("SELECT model,cost_usd,input_tokens FROM llm_usage ORDER BY id").fetchall()
            assert len(rows) == 2
            assert rows[0]["model"] == f"openrouter/{NEMOTRON}"
            assert rows[0]["input_tokens"] == 123
            assert rows[1]["model"] == "gemini-lite"
            assert sum(row["cost_usd"] for row in rows) == 0


@pytest.mark.parametrize("cost", [None, 0.1])
def test_unexpected_or_unknown_cost_disables_model(cost: float | None) -> None:
    metadata = Metadata()
    with route("bulk", metadata) as client:
        client.candidates[NEMOTRON].outputs = [GeminiResult({"answer": "ok"}, f"openrouter/{NEMOTRON}", 1,
                                                        {"costUsd": cost})]
        result = request(client)
        assert result.model == "gemini-lite"
        assert result.usage["routingAttempts"][0]["usage"]["costUsd"] == cost
        assert request(client).model == "gemini-lite"
        assert client.candidates[NEMOTRON].calls == 1


def test_fallback_errors_propagate_without_fabricated_success() -> None:
    metadata = Metadata(remaining=0)
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
        assert client.order == ()
        assert client.candidates == {}
        assert client.policy is None
        assert client.models == ("gemini-test",)
        assert request(client).model == "gemini-test"
    assert calls[0] == ("review", {"include_lite": True, "purpose": "editorial", "last_resort": True,
                                   "progress": None})
    with create_llm_client("bulk") as client:
        assert client.order == (NEMOTRON,)
        assert set(client.candidates) == {NEMOTRON}
        assert client.candidates[NEMOTRON].provider_preferences["max_price"] == ZERO_PRICE
        assert client.candidates[NEMOTRON].max_attempts == 1  # Router owns retries.
        assert client.candidates[NEMOTRON].reasoning_effort == "low"
        assert client.candidates[NEMOTRON].response_format == "json_schema"
    assert isinstance(create_llm_client("review", backend="gemini"), Client)
    with pytest.raises(ValueError, match="explicit backend"):
        create_llm_client("review", model="vendor/model")


@pytest.mark.parametrize("stage", ["bulk", "review"])
def test_missing_key_goes_direct_to_gemini(monkeypatch: pytest.MonkeyPatch, stage: str) -> None:
    monkeypatch.delenv("OPENROUTER_API_KEY")
    with FreeFirstClient(stage=stage, fallback=Client("gemini-test"), settings={}) as client:
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
    with FreeFirstClient(stage="bulk", fallback=Client("gemini-test"), settings={},
                         policy=metadata.policy()) as client:
        def handle(req: httpx.Request) -> httpx.Response:
            body = json.loads(req.content)
            assert body["provider"]["max_price"] == ZERO_PRICE
            assert body["model"] == NEMOTRON
            assert "models" not in body
            assert body["reasoning"] == {"effort": "low", "exclude": True}
            assert body["response_format"]["type"] == "json_schema"
            return httpx.Response(200, json={"model": NEMOTRON, "usage": {"cost": 0}, "choices": [
                {"finish_reason": "stop", "message": {"content": '{"answer":"ok"}'}}]})
        with httpx.Client(transport=httpx.MockTransport(handle)) as http:
            monkeypatch.setattr(client.candidates[NEMOTRON], "_http_client", http)
            assert request(client).model == f"openrouter/{NEMOTRON}"
