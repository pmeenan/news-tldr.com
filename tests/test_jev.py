from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from pipeline.aggregate import _evaluate_deduplication_pair
from pipeline.jev import JevDuplicateClient
from pipeline.llm import GeminiResult
from pipeline.state import StateDB, migrate


class Flash:
    model = "gemini-3.6-flash"

    def __init__(self, fail: bool = False) -> None:
        self.calls = 0
        self.fail = fail

    def generate_json(self, **kwargs: Any) -> GeminiResult:
        self.calls += 1
        if self.fail:
            raise RuntimeError("Flash unavailable")
        return GeminiResult({"should_merge": False, "confidence": 0.9, "rationale": "separate"},
                            self.model, 1, {"promptTokenCount": 50, "costUsd": 0.001})


def raw(yes: bool = True) -> dict[str, Any]:
    return {"model": "typesafe/jev-1.13-20260917", "provider": "TypeSafe",
            "usage": {"input_tokens": 100, "output_tokens": 0, "cost": 0.0000042},
            "answers": {"merge": {"type": "choice", "choice": "yes" if yes else "no", "confidence": 0.6,
                                  "probabilities": {"yes": 0.6 if yes else 0.4, "no": 0.4 if yes else 0.6}}}}


def make(monkeypatch: pytest.MonkeyPatch, handler: Any, *, fail: bool = False) -> JevDuplicateClient:
    monkeypatch.setenv("OPENROUTER_API_KEY", "test")
    return JevDuplicateClient(Flash(fail), {}, client=httpx.Client(transport=httpx.MockTransport(handler)))


def evaluate(client: JevDuplicateClient) -> Any:
    event = {"event_id": "event", "title": "Title", "articles": [{"headline": "Headline", "summary": "Text"}]}
    return _evaluate_deduplication_pair(client=client, payload1=event, payload2=event)


@pytest.mark.parametrize("yes", [True, False])
def test_native_decisions_preserve_confidence_and_usage(monkeypatch: pytest.MonkeyPatch, yes: bool) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        assert set(body["state"]) == {"event_a", "event_b"}
        assert body["provider"]["only"] == ["typesafe"]
        assert request.extensions["timeout"]["read"] == 20
        return httpx.Response(200, json=raw(yes))
    client = make(monkeypatch, handler)
    decision = evaluate(client)
    assert decision.should_merge is yes and decision.confidence == 0.6
    assert decision.model == "openrouter/typesafe/jev-1.13-20260917"
    assert decision.usage["costUsd"] == 0.0000042
    assert client.fallback.calls == 0


@pytest.mark.parametrize("status", [400, 401, 402, 403, 413, 422, 429, 500, 503, 529])
def test_failure_falls_back_once_and_cools_queued_requests(monkeypatch: pytest.MonkeyPatch, status: int) -> None:
    calls = []
    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(status, json={"error": "unavailable"}, headers={"retry-after": "120"})
    client = make(monkeypatch, handler)
    assert evaluate(client).model == Flash.model
    assert evaluate(client).model == Flash.model
    assert len(calls) == 1 and client.fallback.calls == 2


def test_timeout_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("secret provider details", request=request)
    client = make(monkeypatch, handler)
    messages = []
    client.progress = messages.append
    assert evaluate(client).model == Flash.model
    assert "secret" not in str(messages)


@pytest.mark.parametrize("kind", ["non_json", "list", "error", "wrong_model", "bad_choice"])
def test_invalid_response_falls_back(monkeypatch: pytest.MonkeyPatch, kind: str) -> None:
    data = raw()
    if kind == "wrong_model":
        data["model"] = "another-model"
    elif kind == "bad_choice":
        data["answers"]["merge"]["choice"] = "no"
    elif kind == "error":
        data["error"] = "provider failed"
    client = make(monkeypatch, lambda _: httpx.Response(200, text="bad") if kind == "non_json" else
                  httpx.Response(200, json=[] if kind == "list" else data))
    assert evaluate(client).model == Flash.model


def test_rejected_paid_response_and_flash_are_both_accounted(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    data = raw()
    data["answers"] = {}
    client = make(monkeypatch, lambda _: httpx.Response(200, json=data))
    result = evaluate(client)
    db = tmp_path / "state.db"
    migrate(db)
    with StateDB(db) as state:
        state.record_llm_usage("test", "deduplication_review", result.model, "test", usage=result.usage)
        costs = [r[0] for r in state.conn.execute("SELECT cost_usd FROM llm_usage")]
    assert sorted(costs) == [0.0000042, 0.001]


def test_terminal_flash_error_preserves_jev_usage(monkeypatch: pytest.MonkeyPatch) -> None:
    data = raw()
    data["answers"] = {}
    client = make(monkeypatch, lambda _: httpx.Response(200, json=data), fail=True)
    with pytest.raises(RuntimeError) as caught:
        evaluate(client)
    assert caught.value.jev_usage[0]["usage"]["costUsd"] == 0.0000042


def test_missing_key_uses_flash(monkeypatch: pytest.MonkeyPatch) -> None:
    client = make(monkeypatch, lambda _: pytest.fail("unexpected Jev request"))
    client.api_key = ""
    assert evaluate(client).model == Flash.model


@pytest.mark.parametrize("yes", [True, False])
def test_native_choice_controls_real_merge_path_without_shadow(monkeypatch: pytest.MonkeyPatch, yes: bool) -> None:
    from types import SimpleNamespace

    from pipeline import aggregate

    client = make(monkeypatch, lambda _: httpx.Response(200, json=raw(yes)))
    events = [{"event_id": name, "title": name, "updated_at": "now"} for name in ("a", "b")]
    articles = [{"headline": "Headline", "summary": "Summary"}]
    monkeypatch.setattr(aggregate, "_load_events_articles_summary", lambda *_: {"a": articles, "b": articles})
    monkeypatch.setattr(aggregate, "_load_event_articles_summary", lambda *_: articles)
    monkeypatch.setattr(aggregate.ShadowRecorder, "for_state", lambda *_: pytest.fail("duplicate shadow request"))
    merged = []
    reviews = []
    monkeypatch.setattr(aggregate, "merge_events", lambda a, b, *_: merged.append((a, b)))
    state = SimpleNamespace(event_exists=lambda _: True, record_deduplication_review=lambda **kw: reviews.append(kw))
    count = aggregate._evaluate_and_apply_deduplication_candidates(
        candidates=[tuple(events)], state=state, client=client, feeds_by_source={},
    )
    assert count == int(yes) and len(merged) == int(yes)
    assert reviews[0]["confidence"] == 0.6 and reviews[0]["should_merge"] is yes
