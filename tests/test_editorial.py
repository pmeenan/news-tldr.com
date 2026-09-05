from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from pipeline.editorial import (
    EDITORIAL_COMPACT_PROMPT_VERSION,
    EDITORIAL_PROMPT_VERSION,
    EditorialArticle,
    EditorialEvent,
    _build_editorial_prompt,
    _display_rank_scores,
    _homepage_coverage_priority,
    _validate_homepage_curation,
    build_story_payload,
    editorial_candidate_rows,
    generate_editorial_stories,
    generate_homepage_curation,
    generate_story,
    validate_editorial_response,
    write_active_stories_index,
)
from pipeline.llm import GeminiEmptyResponseError, GeminiResult
from pipeline.state import StateDB, migrate


class FakeEditorialClient:
    model = "gemini-3.7-flash"

    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload
        self.calls: list[dict[str, Any]] = []

    def generate_json(self, **kwargs: Any) -> GeminiResult:
        self.calls.append(kwargs)
        properties = kwargs["response_schema"]["properties"]
        payload = self.payload
        if "claims" in properties:
            articles = json.loads(kwargs["prompt"].rsplit("\n", 1)[1])
            payload = {"claims": [{"text": "A supported claim.", "status": "reported", "evidence": [
                {"article_id": a["article_id"], "quote": a["text"][:80]} for a in articles
            ]}]}
        elif "approved" in properties:
            payload = {"approved": True, "reason": "", "material_update": False, "change_summary": ""}
        return GeminiResult(
            payload=payload,
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
        "dek": "According to the report, the change takes effect next month, while implementation details remain open.",
        "tldr": [
            "Officials announced a policy change.",
            "Implementation details remain unresolved.",
        ],
        "briefing": [
            "Officials announced a policy change, according to the report.",
            "Implementation details remain unresolved.",
        ],
        "headline_claim_ids": ["c1"], "dek_claim_ids": ["c1"],
        "tldr_claim_ids": [["c1"], ["c1"]], "briefing_claim_ids": [["c1"], ["c1"]],
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


def test_generate_story_retries_empty_responses_with_compact_digest_context() -> None:
    event = _event(
        EditorialArticle(
            **{
                **_article("a1").__dict__,
                "content": "Sensitive full article text " * 500,
            }
        )
    )

    class CompactRetryClient(FakeEditorialClient):
        def generate_json(self, **kwargs: Any) -> GeminiResult:
            if len(self.calls) == 1:
                self.calls.append(kwargs)
                raise GeminiEmptyResponseError("no candidates")
            result = super().generate_json(**kwargs)
            return GeminiResult(payload=result.payload, model="gemini-3.6-flash", elapsed_ms=25, usage=result.usage)

    client = CompactRetryClient(_response("a1"))
    generated = generate_story(event, client=client)

    assert len(client.calls) == 4
    # Evidence extraction reads the full text; the ledger-backed draft does not repeat it.
    assert "Sensitive full article text" in client.calls[0]["prompt"]
    assert '"article_text"' not in client.calls[1]["prompt"]
    assert len(client.calls[2]["prompt"]) <= len(client.calls[1]["prompt"])
    assert generated["model"] == "gemini-3.6-flash"
    assert generated["prompt_version"] == EDITORIAL_COMPACT_PROMPT_VERSION


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


def test_display_ranking_balances_freshness_and_view_specific_impact() -> None:
    now = datetime(2026, 8, 24, 12, tzinfo=UTC)
    global_first = _display_rank_scores(
        {
            "score": 0.8,
            "components": {
                "stage2_global": 0.95,
                "stage2_category": 0.45,
                "editorial": 0.8,
                "source_quality": 0.9,
                "source_count": 0.8,
            },
        },
        event_updated_at="2026-08-24T10:00:00Z",
        now=now,
    )
    category_first = _display_rank_scores(
        {
            "score": 0.8,
            "components": {
                "stage2_global": 0.45,
                "stage2_category": 0.95,
                "editorial": 0.8,
                "source_quality": 0.9,
                "source_count": 0.8,
            },
        },
        event_updated_at="2026-08-24T10:00:00Z",
        now=now,
    )
    older = _display_rank_scores(
        {
            "score": 0.8,
            "components": {
                "stage2_global": 0.95,
                "stage2_category": 0.45,
                "editorial": 0.8,
                "source_quality": 0.9,
                "source_count": 0.8,
            },
        },
        event_updated_at="2026-08-22T10:00:00Z",
        now=now,
    )

    assert global_first["homepage"] > category_first["homepage"]
    assert category_first["category"] > global_first["category"]
    assert global_first["homepage"] > older["homepage"]


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
            "SELECT stage, model, prompt_version FROM llm_usage WHERE run_id = 'run-1' "
            "AND prompt_version = ?", (EDITORIAL_PROMPT_VERSION,)
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
            generated_at=datetime(2026, 8, 24, 2, tzinfo=UTC),
        )

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert [story["story_id"] for story in payload["stories"]] == ["event-2", "event-1"]
    assert payload["stories"][0]["source_count"] == 1
    assert payload["stories"][0]["source_article_count"] == 2
    assert payload["stories"][0]["multi_angle_source_count"] == 1
    assert payload["stories"][0]["source_coverage_score"] == 1.5
    assert payload["stories"][0]["category_source_pool"] == 1
    assert payload["stories"][0]["source_coverage_ratio"] == 1.0
    assert payload["stories"][0]["homepage_rank_score"] > 0
    assert payload["stories"][0]["category_rank_score"] > 0
    assert payload["ranking_version"] == "display-ranking-v2"
    assert payload["curation"]["model"] == "deterministic-ranking-fallback"
    assert payload["curation"]["top_news"] == ["event-2", "event-1"]
    assert payload["stories"][0]["event_updated_at"] == "2026-08-24T01:00:00Z"
    assert stats == {
        "active_index_stories": 2,
        "active_index_missing": 0,
        "curation_mode": "fallback",
        "curation_sections": 0,
        "curation_top_news": 2,
    }


def test_homepage_curation_validates_top_news_and_meaningful_sections() -> None:
    client = FakeEditorialClient(
        {
            "top_news": ["event-2", "event-2", "unknown"],
            "sections": [
                {"title": "Ukraine War", "story_ids": ["event-1", "event-2", "unknown"]},
                {"title": "Singleton", "story_ids": ["event-3"]},
                {"title": "Repeated", "story_ids": ["event-2", "event-3"]},
            ],
        }
    )
    stories = [
        {
            "story_id": f"event-{index}",
            "category": "world",
            "headline": f"Story {index}",
            "homepage_rank_score": 1 - index / 10,
            "category_rank_score": 1 - index / 10,
            "source_count": 2,
            "event_updated_at": "2026-08-24T11:00:00Z",
        }
        for index in range(1, 4)
    ]

    curation = generate_homepage_curation(
        stories=stories,
        story_details={
            story["story_id"]: {"dek": f"Dek for {story['story_id']}"} for story in stories
        },
        client=client,
        generated_at=datetime(2026, 8, 24, 12, tzinfo=UTC),
        rolling_window_hours=72,
    )

    assert curation["top_news"] == ["event-2", "event-1", "event-3"]
    assert curation["sections"] == [
        {"title": "Ukraine War", "story_ids": ["event-1", "event-2"]}
    ]
    assert curation["model"] == "gemini-3.7-flash"
    assert client.calls[0]["thinking_level"] == "low"
    assert len(client.calls) == 2
    assert "category-specific remainder section" in client.calls[1]["prompt"]
    assert "Middle East" in client.calls[1]["prompt"]
    assert "coverage_priority" in client.calls[0]["prompt"]
    assert "feed inventories" in client.calls[0]["prompt"]


def test_homepage_curation_does_not_append_fallback_after_top_news_is_full() -> None:
    curation = _validate_homepage_curation(
        {"top_news": ["event-1", "event-2"], "sections": []},
        valid_story_ids={"event-1", "event-2", "event-3"},
        fallback_top=["event-3"],
        target_top_count=2,
    )

    assert curation["top_news"] == ["event-1", "event-2"]


def test_homepage_curation_uses_one_high_ranked_candidate_set_per_category() -> None:
    client = FakeEditorialClient({"top_news": [], "sections": []})
    stories = [
        {
            "story_id": f"event-{index}",
            "category": "world",
            "headline": f"Story {index}",
            "homepage_rank_score": index / 300,
            "category_rank_score": index / 205,
            "source_count": 2,
            "source_coverage_score": 2,
            "source_coverage_ratio": 0.1,
            "event_updated_at": "2026-08-24T11:00:00Z",
        }
        for index in range(205)
    ]

    generate_homepage_curation(
        stories=stories,
        story_details={},
        client=client,
        generated_at=datetime(2026, 8, 24, 12, tzinfo=UTC),
        rolling_window_hours=72,
    )

    assert len(client.calls) == 2
    category_prompt = client.calls[1]["prompt"]
    assert category_prompt.count('"id":') == 50
    assert '"id":"event-204"' in category_prompt
    assert '"id":"event-155"' in category_prompt
    assert '"id":"event-154"' not in category_prompt
    assert '"dek"' not in category_prompt and '"source_coverage_ratio"' not in category_prompt


def test_homepage_coverage_priority_normalizes_category_supply_and_expires() -> None:
    generated_at = datetime(2026, 8, 24, 12, tzinfo=UTC)
    thin_category_story = {
        "event_updated_at": "2026-08-24T11:00:00Z",
        "homepage_rank_score": 0.7,
        "source_coverage_score": 4,
        "source_coverage_ratio": 0.5,
    }
    rich_category_story = {
        "event_updated_at": "2026-08-24T11:00:00Z",
        "homepage_rank_score": 0.7,
        "source_coverage_score": 6,
        "source_coverage_ratio": 0.1,
    }
    expired_story = {
        **thin_category_story,
        "event_updated_at": "2026-08-23T10:00:00Z",
    }

    thin_priority = _homepage_coverage_priority(
        thin_category_story,
        generated_at=generated_at,
    )
    assert thin_priority > _homepage_coverage_priority(
        rich_category_story,
        generated_at=generated_at,
    )
    assert thin_priority > _homepage_coverage_priority(
        expired_story,
        generated_at=generated_at,
    )


def test_meaningful_revisions_preserve_history_and_ignore_cosmetic_refreshes() -> None:
    event = _event()
    generated = {"payload": validate_editorial_response(_response(), event), "model": "test", "usage": {},
                 "review": {"approved": True, "material_update": False, "change_summary": ""}}
    first = build_story_payload(event, generated, generated_at="2026-08-24T02:00:00Z")
    refreshed = build_story_payload(event, generated, generated_at="2026-08-24T03:00:00Z", existing_story=first)
    assert refreshed["revision"] == first["revision"] == 1
    assert refreshed["revision_at"] == first["created_at"]
    generated["review"].update(material_update=True, change_summary="Officials published implementation dates.")
    updated = build_story_payload(event, generated, generated_at="2026-08-24T04:00:00Z", existing_story=refreshed)
    assert updated["revision"] == 2
    assert updated["revision_at"] == "2026-08-24T04:00:00Z"
    assert updated["created_at"] == first["created_at"]


def test_verifier_rejection_keeps_previous_artifact_and_checkpoint(tmp_path: Path) -> None:
    class RejectingClient(FakeEditorialClient):
        def generate_json(self, **kwargs: Any) -> GeminiResult:
            result = super().generate_json(**kwargs)
            if "approved" in kwargs["response_schema"]["properties"]:
                result.payload["approved"] = False
                result.payload["reason"] = "Unsupported date in headline"
            return result

    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    story_dir = tmp_path / "stories"
    story_dir.mkdir()
    path = story_dir / "event-1.json"
    path.write_text('{"headline": "Previous verified story"}')
    with StateDB(db_path) as state:
        _insert_event_and_articles(state, tmp_path)
        state.start_run("review-failure", "editorial")
        stats = generate_editorial_stories(state=state, run_id="review-failure", concurrency=1,
                                          client=RejectingClient(_response()), story_dir=story_dir)
        assert stats["completed"] == 0 and stats["failed"] == 1
        assert stats["rejected_event_ids"] == ["event-1"]
        assert state.conn.execute("SELECT last_editorial_at FROM events").fetchone()[0] is None
        assert state.conn.execute("SELECT COUNT(*) FROM llm_usage").fetchone()[0] == 5
    assert json.loads(path.read_text())["headline"] == "Previous verified story"


def test_mixed_valid_and_unknown_citations_are_rejected() -> None:
    response = _response("a1", "invented")
    with pytest.raises(ValueError, match="only valid source"):
        validate_editorial_response(response, _event())


def test_evidence_retry_preserves_usage_and_constrains_draft_citations() -> None:
    class RepairClient(FakeEditorialClient):
        def generate_json(self, **kwargs: Any) -> GeminiResult:
            if "Repair the previous extraction:" in kwargs["prompt"]:
                kwargs = {**kwargs, "prompt": kwargs["prompt"].split("\nRepair the previous extraction:")[0]}
            result = super().generate_json(**kwargs)
            if len(self.calls) == 1:
                result.payload["claims"][0]["evidence"][0]["quote"] = "Invented passage that must fail."
            return result

    client = RepairClient(_response())
    result = generate_story(_event(), client=client)
    assert len(result["usage_records"]) == 4
    draft_schema = client.calls[2]["response_schema"]
    citation_schema = draft_schema["properties"]["key_facts"]["items"]["properties"]["source_article_ids"]
    assert citation_schema["items"]["enum"] == ["a1"]


def test_malformed_verifier_retry_is_accounted_for() -> None:
    class MalformedClient(FakeEditorialClient):
        def generate_json(self, **kwargs: Any) -> GeminiResult:
            result = super().generate_json(**kwargs)
            if len(self.calls) == 3:
                result.payload["approved"] = "yes"
            return result

    result = generate_story(_event(), client=MalformedClient(_response()))
    assert len(result["usage_records"]) == 5


def test_headline_validation_rejects_title_case_and_source_copies() -> None:
    event = _event(_article("a1"))
    response = _response()
    response["headline"] = (
        "Defense Secretary Gutted Civilian Protection Program Despite Objections From Top Commanders"
    )
    with pytest.raises(ValueError, match="title case"):
        validate_editorial_response(response, event)
    response["headline"] = "Las Vegas jury convicts Duane Davis of murder in 1996 killing of Tupac Shakur"
    validate_editorial_response(response, event)
    response["headline"] = "Headline a1"
    with pytest.raises(ValueError, match="copies the"):
        validate_editorial_response(response, event)


def test_single_publisher_stories_must_attribute_in_dek_and_first_bullet() -> None:
    event = _event(_article("a1"))
    assert "Only one publisher, Source a1" in _build_editorial_prompt(event)
    assert "Only one publisher" not in _build_editorial_prompt(_event(_article("a1"), _article("a2")))
    response = _response()
    response["dek"] = "The change takes effect next month."
    with pytest.raises(ValueError, match="attribute this single-outlet reporting"):
        validate_editorial_response(response, event)
    response["dek"] = "Source a1 reports the change takes effect next month."
    response["briefing"][0] = "Officials announced a policy change."
    with pytest.raises(ValueError, match="first briefing bullet"):
        validate_editorial_response(response, event)
    response["briefing"][0] = "Officials announced a policy change, Source a1 reports."
    validated = validate_editorial_response(response, event)
    assert validated["briefing"][0].startswith("Officials")
    unattributed = _response("a1", "a2")
    unattributed["dek"] = "The change takes effect next month."
    unattributed["briefing"][0] = "Officials announced a policy change."
    validate_editorial_response(unattributed, _event(_article("a1"), _article("a2")))


def test_briefing_bullets_are_capped_for_card_density() -> None:
    response = _response()
    response["briefing"][1] = "word " * 60
    with pytest.raises(ValueError, match="briefing item exceeds 230"):
        validate_editorial_response(response, _event())
    assert "15-22 words each" in _build_editorial_prompt(_event())


def test_changelog_style_summary_retries_verifier_and_records_usage() -> None:
    class ChangelogClient(FakeEditorialClient):
        def generate_json(self, **kwargs: Any) -> GeminiResult:
            result = super().generate_json(**kwargs)
            if "approved" in kwargs["response_schema"]["properties"]:
                first = len(self.calls) == 3
                result.payload.update(
                    material_update=True,
                    change_summary="Added new details about the policy." if first
                    else "The policy now takes effect in March.",
                )
            return result

    result = generate_story(_event(), client=ChangelogClient(_response()), previous={"headline": "Old"})
    assert len(result["usage_records"]) == 4
    assert result["review"]["change_summary"] == "The policy now takes effect in March."


def test_backfill_rows_select_unverified_current_stories_and_skip_recent_failures(tmp_path: Path) -> None:
    from datetime import timedelta

    from pipeline.editorial import editorial_backfill_rows

    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    story_dir = tmp_path / "stories"
    story_dir.mkdir()
    with StateDB(db_path) as state:
        for event_id, score in (("event-1", 0.5), ("event-2", 0.9), ("event-3", 0.7), ("event-4", 0.6)):
            payload = {
                "event_id": event_id, "title": f"Title {event_id}", "category": "world", "thread": None,
                "status": "active", "created_at": "2026-08-24T00:00:00Z",
                "updated_at": "2026-08-24T01:00:00Z", "keywords": [], "entities": [],
                "article_count": 1, "confidence": 0.9,
                "newsworthiness": {"global": score, "category": score, "rationale_codes": []},
            }
            path = tmp_path / f"{event_id}.event.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            state.upsert_event(payload, path)
            state.mark_event_editorial_completed(event_id, "2026-08-24T02:00:00Z")
        (story_dir / "event-1.json").write_text(json.dumps({"headline": "pre-evidence"}))
        (story_dir / "event-2.json").write_text(
            json.dumps({"headline": "verified", "evidence_verification": {"approved": True}})
        )
        (story_dir / "event-3.json").write_text(json.dumps({"headline": "pre-evidence but failing"}))
        state.record_error("run", "editorial", "event", "event-3", None, "boom")

        now = datetime.now(UTC)
        window = int((now - datetime(2026, 8, 20, tzinfo=UTC)).total_seconds() // 3600)
        rows = editorial_backfill_rows(state=state, story_dir=story_dir, limit=10, window_hours=window, now=now)
        assert [row["event_id"] for row in rows] == ["event-1"]
        later = now + timedelta(hours=30)
        rows = editorial_backfill_rows(
            state=state, story_dir=story_dir, limit=10, window_hours=window + 30, now=later
        )
        assert [row["event_id"] for row in rows] == ["event-3", "event-1"]
        rows = editorial_backfill_rows(
            state=state, story_dir=story_dir, limit=1, window_hours=window + 30, now=later
        )
        assert [row["event_id"] for row in rows] == ["event-3"]
        assert editorial_backfill_rows(state=state, story_dir=story_dir, limit=10, window_hours=1, now=now) == []


def test_backfill_regenerates_pre_evidence_stories_within_limit(tmp_path: Path) -> None:
    from pipeline.editorial import backfill_editorial_stories

    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    story_dir = tmp_path / "stories"
    story_dir.mkdir()
    path = story_dir / "event-1.json"
    with StateDB(db_path) as state:
        _insert_event_and_articles(state, tmp_path)
        state.mark_event_editorial_completed("event-1", "2026-08-24T02:00:00Z")
        path.write_text(json.dumps({"headline": "Old v2 story", "created_at": "2026-08-24T02:00:00Z"}))
        state.start_run("backfill", "editorial")
        window = 24 * 1000
        stats = backfill_editorial_stories(
            state=state, run_id="backfill", client=FakeEditorialClient(_response()), limit=5,
            concurrency=1, window_hours=window, story_dir=story_dir,
        )
        assert stats["backfill_candidates"] == 1
        assert stats["backfill_completed"] == 1 and stats["backfill_failed"] == 0
        assert stats["backfill_deferred"] == 0
        story = json.loads(path.read_text())
        assert story["evidence_verification"]["approved"] is True
        assert story["created_at"] == "2026-08-24T02:00:00Z"
        assert state.conn.execute("SELECT COUNT(*) FROM llm_usage").fetchone()[0] == 3
        again = backfill_editorial_stories(
            state=state, run_id="backfill", client=FakeEditorialClient(_response()), limit=5,
            concurrency=1, window_hours=window, story_dir=story_dir,
        )
        assert again["backfill_candidates"] == 0


def test_update_gate_skips_regeneration_when_new_reports_add_nothing() -> None:
    from pipeline.editorial import UPDATE_GATE_PROMPT_VERSION

    class GateClient(FakeEditorialClient):
        model = "gemini-3.5-flash-lite"

        def __init__(self, material: bool) -> None:
            super().__init__({})
            self.material = material

        def generate_json(self, **kwargs: Any) -> GeminiResult:
            self.calls.append(kwargs)
            assert "material" in kwargs["response_schema"]["properties"]
            return GeminiResult(payload={"material": self.material, "reason": "r"}, model=self.model,
                                elapsed_ms=1, usage={"promptTokenCount": 5, "candidatesTokenCount": 1})

    previous = {
        "headline": "Old", "evidence_verification": {"approved": True},
        "sources": [{"article_id": "a1"}], "claim_sources": {"c1": ["a1"]},
        "key_facts": [{"text": "Old fact"}], "uncertainties": [], "briefing": ["Old bullet", "Old caveat"],
    }
    event = _event(_article("a1"), _article("a2"))
    gate = GateClient(material=False)
    result = generate_story(event, client=FakeEditorialClient(_response("a1", "a2")), previous=previous,
                            gate_client=gate)
    assert result["skipped"] == "no_material_update"
    assert [r["prompt_version"] for r in result["usage_records"]] == [UPDATE_GATE_PROMPT_VERSION]
    assert '"Old fact"' in gate.calls[0]["prompt"] and "Headline a2" in gate.calls[0]["prompt"]

    unchanged = generate_story(_event(_article("a1")), client=FakeEditorialClient(_response()), previous=previous,
                               gate_client=gate)
    assert unchanged["skipped"] == "no_new_articles" and len(gate.calls) == 1

    generated = generate_story(event, client=FakeEditorialClient(_response("a1", "a2")), previous=previous,
                               gate_client=GateClient(material=True))
    assert "payload" in generated and generated["usage_records"][0]["prompt_version"] == UPDATE_GATE_PROMPT_VERSION

    legacy_previous = {**previous}
    del legacy_previous["evidence_verification"]
    strict_gate = GateClient(material=False)
    regenerated = generate_story(event, client=FakeEditorialClient(_response("a1", "a2")),
                                 previous=legacy_previous, gate_client=strict_gate)
    assert "payload" in regenerated and strict_gate.calls == []


def test_skipped_stories_advance_checkpoint_without_rewriting(tmp_path: Path) -> None:
    class GateOnly(FakeEditorialClient):
        def generate_json(self, **kwargs: Any) -> GeminiResult:
            self.calls.append(kwargs)
            assert "material" in kwargs["response_schema"]["properties"]
            return GeminiResult(payload={"material": False, "reason": "same"}, model="lite", elapsed_ms=1,
                                usage={"promptTokenCount": 5, "candidatesTokenCount": 1})

    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    story_dir = tmp_path / "stories"
    story_dir.mkdir()
    path = story_dir / "event-1.json"
    path.write_text(json.dumps({"headline": "Verified story", "evidence_verification": {"approved": True},
                                "sources": [], "claim_sources": {}, "key_facts": [], "briefing": []}))
    with StateDB(db_path) as state:
        _insert_event_and_articles(state, tmp_path)
        state.start_run("gate", "editorial")
        gate = GateOnly({})
        stats = generate_editorial_stories(state=state, run_id="gate", concurrency=1,
                                          client=FakeEditorialClient(_response()), gate_client=gate,
                                          story_dir=story_dir)
        assert stats["skipped_unchanged"] == 1 and stats["completed"] == 0 and stats["failed"] == 0
        assert state.conn.execute("SELECT last_editorial_at FROM events").fetchone()[0] is not None
        assert state.conn.execute("SELECT COUNT(*) FROM llm_usage").fetchone()[0] == 1
    assert json.loads(path.read_text())["headline"] == "Verified story"


def test_evidence_extractor_falls_back_to_full_client_after_two_failures() -> None:
    from pipeline.evidence import EVIDENCE_VERSION

    class RepairAware(FakeEditorialClient):
        def generate_json(self, **kwargs: Any) -> GeminiResult:
            if "\nRepair the previous extraction:" in kwargs["prompt"]:
                kwargs = {**kwargs, "prompt": kwargs["prompt"].split("\nRepair the previous extraction:")[0]}
            return super().generate_json(**kwargs)

    class BadLite(RepairAware):
        model = "gemini-3.5-flash-lite"

        def generate_json(self, **kwargs: Any) -> GeminiResult:
            result = super().generate_json(**kwargs)
            if "claims" in kwargs["response_schema"]["properties"]:
                result.payload["claims"][0]["evidence"][0]["quote"] = "Not present in the article."
            return result

    lite = BadLite(_response())
    full = RepairAware(_response())
    result = generate_story(_event(), client=full, evidence_client=lite)
    assert len(lite.calls) == 2
    models = [record["model"] for record in result["usage_records"]]
    assert models[:3] == ["gemini-3.5-flash-lite", "gemini-3.5-flash-lite", "gemini-3.7-flash"]
    assert result["usage_records"][2]["prompt_version"] == EVIDENCE_VERSION
    assert "payload" in result


def test_active_index_reuses_curation_for_unchanged_story_set(tmp_path: Path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    story_dir = tmp_path / "stories"
    story_dir.mkdir()
    story = {
        "story_id": "event-1", "event_id": "event-1", "category": "world", "headline": "Story one",
        "dek": "Dek", "tldr": ["a", "b"], "briefing": ["a", "b"], "key_facts": [], "uncertainties": [],
        "sources": [{"article_id": "a1", "source_name": "AP News"}],
        "importance": {"score": 0.6, "components": {}},
        "created_at": "2026-08-24T02:00:00Z", "updated_at": "2026-08-24T02:00:00Z",
    }
    path = story_dir / "event-1.json"
    path.write_text(json.dumps(story))
    output = tmp_path / "active.json"
    client = FakeEditorialClient({"top_news": ["event-1"], "sections": []})
    when = datetime(2026, 8, 24, 3, tzinfo=UTC)
    with StateDB(db_path) as state:
        _insert_event_and_articles(state, tmp_path)
        first = write_active_stories_index(state=state, story_dir=story_dir, output_path=output,
                                           curation_client=client, run_id="r", generated_at=when)
        assert first["curation_mode"] == "llm" and first["curation_top_news"] == 1
        calls = len(client.calls)
        second = write_active_stories_index(state=state, story_dir=story_dir, output_path=output,
                                            curation_client=client, run_id="r", generated_at=when)
        assert second["curation_mode"] == "reused" and len(client.calls) == calls
        story["updated_at"] = "2026-08-24T02:30:00Z"
        path.write_text(json.dumps(story))
        third = write_active_stories_index(state=state, story_dir=story_dir, output_path=output,
                                           curation_client=client, run_id="r", generated_at=when,
                                           reuse_previous_curation=True)
        assert third["curation_mode"] == "reused" and len(client.calls) == calls
        fourth = write_active_stories_index(state=state, story_dir=story_dir, output_path=output,
                                            curation_client=client, run_id="r", generated_at=when)
        assert fourth["curation_mode"] == "llm" and len(client.calls) > calls
    index = json.loads(output.read_text())
    assert index["curation"]["top_news"] == ["event-1"] and index["curation"]["input_signature"]


def test_single_source_events_are_held_before_first_story(tmp_path: Path) -> None:
    from datetime import timedelta

    from pipeline.editorial import pending_editorial_sql

    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    with StateDB(db_path) as state:
        _insert_event_and_articles(state, tmp_path)
        now = datetime(2026, 8, 24, 0, 30, tzinfo=UTC)
        assert editorial_candidate_rows(state=state, hold_minutes=60, now=now) == []
        assert [r["event_id"] for r in editorial_candidate_rows(state=state, hold_minutes=0, now=now)] == ["event-1"]
        later = now + timedelta(hours=1)
        assert [r["event_id"] for r in editorial_candidate_rows(state=state, hold_minutes=60, now=later)] == ["event-1"]
        clause, params = pending_editorial_sql(hold_minutes=60, now=now)
        assert state.conn.execute(f"SELECT COUNT(*) FROM events WHERE {clause}", params).fetchone()[0] == 0
        clause, params = pending_editorial_sql(hold_minutes=60, now=later)
        assert state.conn.execute(f"SELECT COUNT(*) FROM events WHERE {clause}", params).fetchone()[0] == 1


def test_verifier_receives_publisher_names_for_attribution_checks() -> None:
    client = FakeEditorialClient(_response())
    generate_story(_event(_article("a1")), client=client)
    verify_call = next(c for c in client.calls if "approved" in c["response_schema"]["properties"])
    assert '"publishers": {"a1": "Source a1"}' in verify_call["prompt"]
    assert "must not be rejected" in verify_call["prompt"]
