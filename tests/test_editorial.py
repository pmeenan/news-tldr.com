from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from pipeline.editorial import (
    EDITORIAL_PROMPT_VERSION,
    EditorialArticle,
    EditorialEvent,
    _build_editorial_prompt,
    build_story_payload,
    editorial_candidate_rows,
    generate_editorial_stories,
    validate_editorial_response,
    write_active_stories_index,
)
from pipeline.llm import GeminiResult
from pipeline.state import StateDB, migrate


class FakeEditorialClient:
    model = "gemini-3.7-flash"

    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload
        self.calls: list[dict[str, Any]] = []

    def generate_json(self, **kwargs: Any) -> GeminiResult:
        self.calls.append(kwargs)
        return GeminiResult(
            payload=self.payload,
            model=self.model,
            elapsed_ms=25,
            usage={"promptTokenCount": 100, "candidatesTokenCount": 40},
        )


def _article(article_id: str, bias: str = "center", source_id: str | None = None) -> EditorialArticle:
    return EditorialArticle(
        article_id=article_id,
        source_id=source_id or f"source-{article_id}",
        source_name=f"Source {article_id}",
        headline=f"Headline {article_id}",
        url=f"https://example.test/{article_id}",
        published_at="2026-08-24T00:00:00Z",
        content=f"Article body for {article_id}.",
        digest_summary=f"Summary for {article_id}.",
        digest_key_facts=(f"Fact for {article_id}.",),
        bias_label=bias,
        reliability="high",
    )


def _event(*articles: EditorialArticle, category: str = "world") -> EditorialEvent:
    return EditorialEvent(
        event_id="event-1",
        title="Officials announce a material policy change",
        category=category,
        thread=None,
        status="active",
        created_at="2026-08-24T00:00:00Z",
        updated_at="2026-08-24T01:00:00Z",
        newsworthiness={
            "global": 0.8,
            "category": 0.9,
            "rationale_codes": ["major_policy"],
        },
        articles=tuple(articles or (_article("a1"),)),
    )


def _response(*article_ids: str) -> dict[str, Any]:
    ids = list(article_ids or ("a1",))
    return {
        "headline": "Officials announce a material policy change",
        "dek": "The change takes effect next month, while implementation details remain open.",
        "tldr": [
            "Officials announced a policy change.",
            "Implementation details remain unresolved.",
        ],
        "key_facts": [
            {"text": "The policy takes effect next month.", "source_article_ids": ids}
        ],
        "uncertainties": [
            {"text": "Detailed guidance has not been published.", "source_article_ids": [ids[0]]}
        ],
        "editorial_score": 0.85,
        "importance_signals": ["Public impact", "policy_change"],
    }


def _insert_event_and_articles(
    state: StateDB,
    tmp_path: Path,
    *,
    event_id: str = "event-1",
    status: str = "active",
    filtered_article: bool = False,
) -> None:
    event_path = tmp_path / f"{event_id}.event.json"
    event_payload = {
        "event_id": event_id,
        "title": "Officials announce a material policy change",
        "category": "world",
        "thread": None,
        "status": status,
        "created_at": "2026-08-24T00:00:00Z",
        "updated_at": "2026-08-24T01:00:00Z",
        "keywords": [],
        "entities": [],
        "article_count": 2 if filtered_article else 1,
        "confidence": 0.9,
        "newsworthiness": {
            "global": 0.8,
            "category": 0.9,
            "rationale_codes": ["major_policy"],
        },
    }
    event_path.write_text(json.dumps(event_payload), encoding="utf-8")
    state.upsert_event(event_payload, event_path)

    article_ids = ["a1", "filtered"] if filtered_article else ["a1"]
    for article_id in article_ids:
        article_path = tmp_path / f"{article_id}.article.json"
        article = {
            "article_id": article_id,
            "source_id": "ap-news",
            "source_name": "AP News",
            "url": f"https://example.test/{article_id}",
            "headline": f"Headline {article_id}",
            "summary": "Collected summary.",
            "published_at": "2026-08-24T00:30:00Z",
            "publish_date_estimated": False,
            "fetched_at": "2026-08-24T00:31:00Z",
            "content_type": "news",
            "language": "en",
            "collection": {},
            "fingerprints": {},
            "content_text": f"Full reporting for {article_id}.",
            "llm_digest": {
                "summary": f"Digest for {article_id}.",
                "key_facts": [f"Fact for {article_id}."],
            },
        }
        article_path.write_text(json.dumps(article), encoding="utf-8")
        state.insert_article(article, article_path)
        state.assign_articles_to_event([article_id], event_id)
    if filtered_article:
        state.conn.execute("UPDATE articles SET is_filtered = 1 WHERE article_id = 'filtered'")
        state.conn.commit()


def test_editorial_candidate_query_is_incremental_and_forceable(tmp_path: Path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    with StateDB(db_path) as state:
        _insert_event_and_articles(state, tmp_path)
        assert [row["event_id"] for row in editorial_candidate_rows(state=state)] == ["event-1"]

        state.mark_event_editorial_completed("event-1", "2026-08-24T02:00:00Z")
        assert editorial_candidate_rows(state=state) == []
        assert [row["event_id"] for row in editorial_candidate_rows(state=state, force=True)] == [
            "event-1"
        ]


def test_editorial_candidate_query_excludes_archived_events(tmp_path: Path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    with StateDB(db_path) as state:
        _insert_event_and_articles(state, tmp_path, status="archived")
        assert editorial_candidate_rows(state=state, force=True) == []


def test_validate_editorial_response_rejects_unknown_citation() -> None:
    response = _response("made-up-id")
    with pytest.raises(ValueError, match="valid source article"):
        validate_editorial_response(response, _event(_article("a1")))


def test_validate_editorial_response_normalizes_fields() -> None:
    validated = validate_editorial_response(_response("a1"), _event(_article("a1")))
    assert validated["headline"] == "Officials announce a material policy change"
    assert validated["importance_signals"] == ["policy_change", "public_impact"]
    assert validated["political_framing"] is None


def test_political_framing_requires_balanced_policy_labels() -> None:
    event = _event(_article("left", "center-left"), _article("right", "right"), category="politics")
    response = _response("left", "right")
    response["political_framing_present"] = True
    response["political_framing"] = {
        "summary": "The outlets emphasize different consequences.",
        "left_perspective": {
            "summary": "The left source emphasizes affected households.",
            "source_article_ids": ["left"],
        },
        "right_perspective": {
            "summary": "The right source emphasizes implementation costs.",
            "source_article_ids": ["right"],
        },
    }
    validated = validate_editorial_response(response, event)
    assert validated["political_framing"]["left_perspective"]["source_article_ids"] == ["left"]

    response["political_framing"]["left_perspective"]["source_article_ids"] = ["right"]
    with pytest.raises(ValueError, match="valid source article"):
        validate_editorial_response(response, event)

    response = _response("left", "right")
    response["political_framing_present"] = False
    assert validate_editorial_response(response, event)["political_framing"] is None


def test_prompt_only_enables_framing_with_left_and_right_sources() -> None:
    balanced = _event(_article("left", "left"), _article("right", "right"), category="politics")
    one_sided = _event(_article("left", "left"), category="politics")
    assert "Political framing is eligible" in _build_editorial_prompt(balanced)
    assert "political_framing_present" in _build_editorial_prompt(balanced)
    assert "Omit political_framing" in _build_editorial_prompt(one_sided)


def test_build_story_preserves_created_at_and_has_auditable_importance() -> None:
    event = _event(_article("a1"), _article("a2"))
    generated = {
        "payload": validate_editorial_response(_response("a1", "a2"), event),
        "model": "gemini-3.7-flash",
        "usage": {},
    }
    story = build_story_payload(
        event,
        generated,
        generated_at="2026-08-24T02:00:00Z",
        existing_story={"created_at": "2026-08-24T01:30:00Z"},
    )
    assert story["created_at"] == "2026-08-24T01:30:00Z"
    assert story["llm_metadata"]["prompt_version"] == EDITORIAL_PROMPT_VERSION
    assert story["importance"]["score"] > 0.7
    assert story["importance"]["components"]["stage2_global"] == 0.8
    assert story["sources"][0]["article_id"] == "a1"


def test_generate_stories_persists_checkpoint_usage_and_ignores_filtered_articles(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "pipeline.db"
    story_dir = tmp_path / "stories"
    migrate(db_path)
    client = FakeEditorialClient(_response("a1"))
    with StateDB(db_path) as state:
        _insert_event_and_articles(state, tmp_path, filtered_article=True)
        state.start_run("run-1", "editorial")
        stats = generate_editorial_stories(
            state=state,
            run_id="run-1",
            client=client,
            concurrency=1,
            story_dir=story_dir,
        )
        checkpoint = state.conn.execute(
            "SELECT last_editorial_at FROM events WHERE event_id = 'event-1'"
        ).fetchone()["last_editorial_at"]
        usage = state.conn.execute(
            "SELECT stage, model, prompt_version FROM llm_usage WHERE run_id = 'run-1'"
        ).fetchone()

    assert stats["completed"] == 1
    assert stats["failed"] == 0
    assert checkpoint is not None
    assert usage["stage"] == "editorial"
    assert usage["model"] == "gemini-3.7-flash"
    assert usage["prompt_version"] == EDITORIAL_PROMPT_VERSION
    story = json.loads((story_dir / "event-1.json").read_text(encoding="utf-8"))
    assert [source["article_id"] for source in story["sources"]] == ["a1"]
    assert "filtered" not in client.calls[0]["prompt"]


def test_active_index_includes_only_current_events_with_story_files(tmp_path: Path) -> None:
    db_path = tmp_path / "pipeline.db"
    story_dir = tmp_path / "stories"
    output_path = tmp_path / "active-stories.json"
    story_dir.mkdir()
    migrate(db_path)
    with StateDB(db_path) as state:
        _insert_event_and_articles(state, tmp_path, event_id="event-1", status="active")
        _insert_event_and_articles(state, tmp_path, event_id="event-2", status="stale")
        _insert_event_and_articles(state, tmp_path, event_id="event-3", status="archived")
        for event_id, score in (("event-1", 0.7), ("event-2", 0.9), ("event-3", 1.0)):
            (story_dir / f"{event_id}.json").write_text(
                json.dumps(
                    {
                        "story_id": event_id,
                        "category": "world",
                        "headline": event_id,
                        "sources": [
                            {"article_id": "a1", "source_name": "Wire Service"},
                            {"article_id": "a2", "source_name": "Wire Service"},
                        ],
                        "importance": {"score": score},
                        "created_at": "2026-08-24T00:00:00Z",
                        "updated_at": "2026-08-24T01:00:00Z",
                    }
                ),
                encoding="utf-8",
            )
        stats = write_active_stories_index(
            state=state,
            story_dir=story_dir,
            output_path=output_path,
        )

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert [story["story_id"] for story in payload["stories"]] == ["event-2", "event-1"]
    assert payload["stories"][0]["source_count"] == 1
    assert payload["stories"][0]["event_updated_at"] == "2026-08-24T01:00:00Z"
    assert stats == {"active_index_stories": 2, "active_index_missing": 0}
