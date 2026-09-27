from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from pipeline.editorial import generate_homepage_curation
from pipeline.llm import GeminiResult
from pipeline.local_curation import LocalTopNewsClient, create_top_news_client


class API:
    model = "gemini-test"

    def __init__(self, payload: dict[str, Any] | None = None, fail: bool = False) -> None:
        self.calls: list[dict[str, Any]] = []
        self.closed = False
        self.payload = payload or {"top_news": ["b", "a"]}
        self.fail = fail

    def generate_json(self, **kwargs: Any) -> GeminiResult:
        self.calls.append(kwargs)
        if self.fail:
            raise RuntimeError("API unavailable")
        return GeminiResult(self.payload, self.model, 1, {"costUsd": 0.01})

    def close(self) -> None:
        self.closed = True


def response(payload: Any, *, finish: str = "stop", model: str = "test-iq3") -> dict[str, Any]:
    return {"model": model, "choices": [{"finish_reason": finish, "message": {"content": json.dumps(payload)}}],
            "usage": {"prompt_tokens": 100, "completion_tokens": 20}}


def request() -> dict[str, Any]:
    return {"system_instruction": "Rank stories", "prompt": "cards", "max_output_tokens": 2000,
            "thinking_level": "low", "response_schema": {"type": "OBJECT", "properties": {
                "top_news": {"type": "ARRAY", "items": {"type": "STRING"}}}, "required": ["top_news"]}}


def test_local_ranking_is_lazy_and_has_local_provenance() -> None:
    def handle(req: httpx.Request) -> httpx.Response:
        body = json.loads(req.content)
        assert req.url == "http://127.0.0.1:11434/v1/chat/completions"
        assert "authorization" not in req.headers
        assert body["model"] == "test-iq3" and body["max_tokens"] == 2000
        assert body["reasoning_effort"] == "none" and body["seed"] == 42
        assert "schema" in body["messages"][0]["content"]
        return httpx.Response(200, json=response({"top_news": ["a", "b"]}))

    def forbidden() -> Any:
        pytest.fail("Successful local ranking must not construct a paid client")

    client = LocalTopNewsClient(settings={"model": "test-iq3"}, fallback_factory=forbidden,
                               transport=httpx.MockTransport(handle))
    try:
        result = client.generate_top_news(valid_story_ids={"a", "b"}, target_top_count=2, **request())
        assert result.model == "localai/test-iq3"
        assert result.usage["costUsd"] == 0 and result.usage["serviceTier"] == "localai"
        assert result.usage["promptTokenCount"] == 100
    finally:
        client.close()


@pytest.mark.parametrize("raw", [
    response({"top_news": ["a", "invented"]}),
    response({"top_news": ["a", "a"]}),
    response({"top_news": ["a"]}),
    response({"top_news": ["a", 2]}),
    response({"top_news": ["a", "b"]}, finish="length"),
    response({"top_news": ["a", "b"]}, model="wrong-model"),
    response([]),
])
def test_bad_local_results_fall_back_without_losing_usage(raw: dict[str, Any]) -> None:
    api = API()
    client = LocalTopNewsClient(settings={"model": "test-iq3"}, fallback_factory=lambda: api,
                               transport=httpx.MockTransport(lambda req: httpx.Response(200, json=raw)))
    result = client.generate_top_news(valid_story_ids={"a", "b"}, target_top_count=2, **request())
    assert result.payload == api.payload and result.model == api.model and len(api.calls) == 1
    assert result.usage["costUsd"] == 0.01
    assert result.usage["routingAttempts"][0]["usage"]["costUsd"] == 0
    assert result.usage["routingDecisions"][0]["model"] == "localai/test-iq3"
    client.close()
    assert api.closed


def test_transport_failure_falls_back() -> None:
    def timeout(req: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("local unavailable", request=req)

    api = API()
    client = LocalTopNewsClient(settings={}, fallback_factory=lambda: api, transport=httpx.MockTransport(timeout))
    try:
        result = client.generate_top_news(valid_story_ids={"a", "b"}, target_top_count=2, **request())
        assert result.model == api.model and len(api.calls) == 1
        assert result.usage["routingAttempts"] == []
    finally:
        client.close()


def test_curation_keeps_sections_on_api_and_deterministic_last_fallback() -> None:
    api = API(fail=True)
    sections = API({"sections": []})
    client = LocalTopNewsClient(settings={"model": "test-iq3"}, fallback_factory=lambda: api,
                               transport=httpx.MockTransport(lambda req: httpx.Response(
                                   200, json=response({"top_news": ["invalid"]}))))
    stories = [{"story_id": sid, "headline": sid, "category": "world", "source_count": 2,
                "homepage_rank_score": rank, "category_rank_score": rank,
                "event_updated_at": "2026-09-27T00:00:00Z"} for sid, rank in [("a", 0.9), ("b", 0.8)]]
    try:
        result = generate_homepage_curation(stories=stories, story_details={}, client=client,
                                           sections_client=sections, generated_at=datetime(2026, 9, 27, tzinfo=UTC),
                                           rolling_window_hours=72)
        assert result["top_news"] == ["a", "b"]
        assert result["partial_failures"] == 1
        assert len(sections.calls) == 1 and len(api.calls) == 1
        assert result["usage_records"][0]["model"] == "localai/test-iq3"
    finally:
        client.close()


def test_factory_can_disable_only_local_top_news(monkeypatch: pytest.MonkeyPatch) -> None:
    api = API()
    monkeypatch.setattr("pipeline.local_curation.load_pipeline_config",
                        lambda: SimpleNamespace(llm={"local_top_news": {"enabled": False}}))
    monkeypatch.setattr("pipeline.local_curation.create_llm_client", lambda *args, **kwargs: api)
    assert create_top_news_client() is api
