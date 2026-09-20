from __future__ import annotations

import copy
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from pipeline import aggregate, jev_shadow
from pipeline.jev_shadow import ShadowRecorder, Worker, build_request, encoded, read, report
from pipeline.llm import GeminiResult
from pipeline.state import StateDB, migrate
from pipeline.util import atomic_write_json


def pair(long: bool = False) -> dict[str, Any]:
    return {name: {"event_id": name, "title": "Headline " + name, "articles": [
        {"headline": f"Article {i}",
         "summary": (f"Opening {name} {i}. " + "世界🚀 abc " * 1500) if long else "Short news"}
        for i in range(3)]} for name in ("event_a", "event_b")}


def response(yes: bool = False) -> dict[str, Any]:
    return {"model": "typesafe/jev-1.13-20260917", "provider": "TypeSafe",
            "usage": {"input_tokens": 100, "output_tokens": 0, "cost": 0.0000042},
            "answers": {"merge": {"type": "choice", "choice": "yes" if yes else "no", "confidence": 0.9,
                                  "probabilities": {"yes": 0.99 if yes else 0.01, "no": 0.01 if yes else 0.99}}}}


def seed(root: Path, state: dict[str, Any] | None = None) -> Path:
    data = state or pair()
    path = ShadowRecorder(root).capture(data["event_a"], data["event_b"], run_id="production-test",
                                      prompt_version="production-v1", priority=4, input_signature="signature")
    assert path is not None
    return path


def worker(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, handler: Any, **options: Any) -> Worker:
    db = tmp_path / "usage.db"
    migrate(db)
    result = Worker(root=tmp_path, options={"concurrency": 1, **options}, api_key="test-key", db_path=db,
                    client=httpx.Client(transport=httpx.MockTransport(handler)))
    monkeypatch.setattr(result, "pace", lambda: None)
    return result


def test_prefix_truncation_keeps_both_clusters_and_all_reports() -> None:
    data = pair(long=True)
    original = copy.deepcopy(data)
    full, unchanged = build_request(data, 1_000_000)
    assert full["state"] == data and not unchanged["truncated"]
    truncated, metadata = build_request(data, 8000)
    assert len(encoded(truncated)) <= 8000
    assert len(encoded(truncated)) > 7950
    assert metadata["truncated"] and 0 < metadata["retained_fraction"] < 1
    assert data == original
    for name, event in truncated["state"].items():
        assert len(event["articles"]) == len(data[name]["articles"])
        for i, article in enumerate(event["articles"]):
            assert data[name]["articles"][i]["summary"].startswith(article["summary"])
            assert article["summary"].startswith(f"Opening {name} {i}.")
            assert len(article["summary"]) < len(data[name]["articles"][i]["summary"])
    with pytest.raises(ValueError, match="minimum article prefixes"):
        build_request(data, 100)


@pytest.mark.parametrize("malformation", ["nan", "extra", "mismatch", "provider", "confidence", "missing"])
def test_invalid_answers_never_become_shadow_decisions(malformation: str) -> None:
    raw = response()
    answer = raw["answers"]["merge"]
    if malformation == "nan":
        answer["probabilities"]["yes"] = float("nan")
    elif malformation == "extra":
        raw["answers"]["extra"] = {}
    elif malformation == "mismatch":
        answer["choice"] = "yes"
    elif malformation == "provider":
        raw["provider"] = "other"
    elif malformation == "confidence":
        answer["confidence"] = True
    else:
        raw.pop("answers")
    with pytest.raises(ValueError):
        jev_shadow.validate_response(raw)


def test_context_retry_is_shorter_and_accounts_all_returned_usage(tmp_path, monkeypatch) -> None:
    path = seed(tmp_path, pair(long=True))
    sent = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(json.loads(request.content))
        if len(sent) == 1:
            return httpx.Response(400, json={"error": {"message": "Maximum context tokens exceeded"},
                                           "usage": {"input_tokens": 2, "cost": 0.000000084}})
        return httpx.Response(200, json=response())

    w = worker(tmp_path, monkeypatch, handler, max_request_bytes=8000)
    w.run()
    result = read(path / "result.json")
    assert result["status"] == "completed" and result["attempts"] == 2
    assert len(encoded(sent[1])) <= len(encoded(sent[0])) * 0.70
    assert result["high_confidence_reject"]
    assert all(len(e["articles"]) == 3 for e in sent[1]["state"].values())
    with StateDB(w.db_path) as db:
        rows = db.conn.execute("SELECT * FROM llm_usage").fetchall()
        assert len(rows) == 2 and all(r["stage"] == "routing_evaluation" for r in rows)
    # Recover the response-to-accounting gap without issuing a request or duplicating usage.
    attempt_path = path / "attempt-2.json"
    attempt = read(attempt_path)
    attempt.pop("accounted")
    atomic_write_json(attempt_path, attempt)
    w.run()
    assert len(sent) == 2
    with StateDB(w.db_path) as db:
        assert db.conn.execute("SELECT COUNT(*) FROM llm_usage").fetchone()[0] == 2


def test_daily_budget_and_unknown_charge_survive_restart(tmp_path, monkeypatch) -> None:
    path = seed(tmp_path)
    sent = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        raise httpx.ReadTimeout("test transport interruption")

    w = worker(tmp_path, monkeypatch, handler, max_attempts=1)
    w.run()
    attempt = read(path / "attempt-1.json")
    assert len(sent) == 1 and attempt["usage"] is None
    second = seed(tmp_path)
    other = worker(tmp_path, monkeypatch, handler, daily_budget_usd=attempt["reservation_usd"] * 1.01)
    other.run()
    assert len(sent) == 1 and not (second / "result.json").exists()
    assert report(tmp_path)["unknown_charge_attempts"] == 1


def test_interrupted_reservation_is_not_reissued(tmp_path, monkeypatch) -> None:
    path = seed(tmp_path)
    w = worker(tmp_path, monkeypatch, lambda _: pytest.fail("Must not issue an uncertain request"))
    body, metadata = build_request(pair())
    w.reserve(path / "attempt-1.json", body, metadata)
    w.run()
    assert read(path / "result.json")["error"] == "interrupted_unknown_charge"


def test_rate_limit_stops_admissions_and_persists_cooldown(tmp_path, monkeypatch) -> None:
    seed(tmp_path)
    seed(tmp_path)
    sent = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        return httpx.Response(429, headers={"Retry-After": "600"}, json={"error": "rate limit"})

    w = worker(tmp_path, monkeypatch, handler)
    w.run()
    assert len(sent) == 1
    assert read(tmp_path / "cooldown.json")["until"] > datetime.now(UTC).timestamp() + 590
    worker(tmp_path, monkeypatch, handler).run()
    assert len(sent) == 1


def test_report_uses_production_threshold_and_keeps_truncation_visible(tmp_path) -> None:
    path = seed(tmp_path)
    recorder = ShadowRecorder(tmp_path)
    recorder.reference(path, {"status": "completed", "model": "gemini-3.8-flash",
                              "should_merge": True, "confidence": 0.99})
    atomic_write_json(path / "result.json", {"status": "completed", **jev_shadow.validate_response(response()),
                                            "truncation": {"truncated": True}})
    result = report(tmp_path)
    assert result["counts"]["high_confidence_reject_disagreement"] == 1
    assert result["counts"]["truncated"] == 1
    recorder.reference(path, {"status": "completed", "model": "gemini-3.5-flash-lite",
                              "should_merge": True, "confidence": 0.99})
    assert report(tmp_path)["counts"]["production_merge_approved"] == 0


@pytest.mark.parametrize("broken_capture", [False, True])
def test_live_hook_captures_before_merge_and_cannot_change_production(tmp_path, monkeypatch, broken_capture) -> None:
    recorder = ShadowRecorder(tmp_path)
    monkeypatch.setattr(ShadowRecorder, "for_state", lambda *_: recorder)
    data = pair()
    events = [{**e, "updated_at": "2026-09-18T00:00:00Z"} for e in data.values()]
    monkeypatch.setattr(aggregate, "_load_events_articles_summary", lambda *_: {
        e["event_id"]: e["articles"] for e in events})
    monkeypatch.setattr(aggregate, "_load_event_articles_summary", lambda *_: [])
    if broken_capture:
        monkeypatch.setattr(jev_shadow, "atomic_write_json", lambda *_: (_ for _ in ()).throw(OSError("disk full")))
    merges = []

    def merge(winner: str, loser: str, *_: Any) -> None:
        if not broken_capture:
            inputs = list(tmp_path.glob("jobs/*/input.json"))
            assert len(inputs) == 1
            assert read(inputs[0])["state"] == data
            assert read(inputs[0].parent / "reference.json")["should_merge"]
        merges.append((winner, loser))

    monkeypatch.setattr(aggregate, "merge_events", merge)

    class Client:
        def generate_json(self, **_: Any) -> GeminiResult:
            if not broken_capture:
                assert list(tmp_path.glob("jobs/*/input.json"))
            return GeminiResult(payload={"should_merge": True, "confidence": 0.99, "rationale": "Same event"},
                                model="gemini-3.8-flash", elapsed_ms=1, usage={})

    state = SimpleNamespace(event_exists=lambda _: True, record_deduplication_review=lambda **_: None)
    assert aggregate._evaluate_and_apply_deduplication_candidates(
        candidates=[tuple(events)], state=state, client=Client(), feeds_by_source={},
    ) == 1
    assert len(merges) == 1


def test_automatic_capture_excludes_temporary_database(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(jev_shadow, "settings", lambda: {"enabled": True})
    assert ShadowRecorder.for_state(SimpleNamespace(path=tmp_path / "db.sqlite"), None) is None
