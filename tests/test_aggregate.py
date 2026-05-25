from __future__ import annotations

import json
from datetime import UTC, datetime, time, timedelta
from typing import Any

import pytest

from pipeline.aggregate import (
    ArticleForAggregation,
    _floor_hour,
    _format_iso_timestamp,
    aggregate_once,
    apply_grouping_result,
    compare_groupings,
    group_articles_with_gemini,
    load_unprocessed_articles,
    load_window_articles,
    plan_sliding_windows,
    score_groups_newsworthiness,
    validate_grouping_response,
    validate_newsworthiness_response,
)
from pipeline.llm import GeminiResult
from pipeline.state import StateDB, migrate


class FakeJsonGenerator:
    model = "fake-model"

    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload
        self.prompts: list[str] = []

    def generate_json(self, **kwargs: Any) -> GeminiResult:
        self.prompts.append(kwargs["prompt"])
        return GeminiResult(
            payload=self.payload,
            model=self.model,
            elapsed_ms=12,
            usage={"promptTokenCount": 30, "candidatesTokenCount": 12},
        )


class FailingJsonGenerator:
    model = "fake-model"

    def generate_json(self, **kwargs: Any) -> GeminiResult:
        raise RuntimeError("fake LLM failure")


def _articles() -> list[ArticleForAggregation]:
    return [
        ArticleForAggregation(
            article_id="a1",
            source_id="source-a",
            source_name="Source A",
            headline="Company announces new phone",
            summary="The company announced a new phone with a faster chip.",
            published_at="2026-05-24T10:00:00Z",
            article_path="data/staging/articles/a1.json",
        ),
        ArticleForAggregation(
            article_id="a2",
            source_id="source-b",
            source_name="Source B",
            headline="New phone unveiled by company",
            summary="The same company unveiled its new phone at an event.",
            published_at="2026-05-24T10:01:00Z",
            article_path="data/staging/articles/a2.json",
        ),
        ArticleForAggregation(
            article_id="a3",
            source_id="source-c",
            source_name="Source C",
            headline="Team wins playoff game",
            summary="The team won a playoff game on Sunday.",
            published_at="2026-05-24T10:02:00Z",
            article_path="data/staging/articles/a3.json",
        ),
    ]


def test_validate_grouping_response_accepts_complete_groups() -> None:
    groups, classifications = validate_grouping_response(
        {
            "articles": [
                {"article_index": 0, "content_type": "news", "category": "world"},
                {"article_index": 1, "content_type": "news", "category": "world"},
                {"article_index": 2, "content_type": "opinion", "category": "us"},
            ],
            "groups": [{"article_indexes": [1, 0]}, {"article_indexes": [2]}],
        },
        article_count=3,
        valid_categories=["world", "us"],
    )

    assert groups == [{"article_indexes": [0, 1]}, {"article_indexes": [2]}]
    assert classifications[0]["content_type"] == "news"
    assert classifications[2]["content_type"] == "opinion"


@pytest.mark.parametrize(
    "payload,match",
    [
        ({}, "groups list"),
        ({"groups": []}, "articles list"),
        (
            {
                "articles": [
                    {"article_index": 0, "content_type": "news", "category": "world"},
                    {"article_index": 0, "content_type": "news", "category": "world"},
                    {"article_index": 2, "content_type": "news", "category": "world"},
                ],
                "groups": [{"article_indexes": [0, 1]}, {"article_indexes": [2]}],
            },
            "duplicate",
        ),
        (
            {
                "articles": [
                    {"article_index": 0, "content_type": "invalid", "category": "world"},
                    {"article_index": 1, "content_type": "news", "category": "world"},
                    {"article_index": 2, "content_type": "news", "category": "world"},
                ],
                "groups": [{"article_indexes": [0, 1]}, {"article_indexes": [2]}],
            },
            "content_type",
        ),
        (
            {
                "articles": [
                    {"article_index": 0, "content_type": "news", "category": "world"},
                    {"article_index": 1, "content_type": "news", "category": "world"},
                    {"article_index": 2, "content_type": "news", "category": "world"},
                ],
                "groups": [{"article_indexes": [3]}],
            },
            "out of range",
        ),
    ],
)
def test_validate_grouping_response_rejects_invalid_output(payload: dict[str, Any], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        validate_grouping_response(payload, article_count=3, valid_categories=["world"])


def test_group_articles_with_gemini_validates_and_summarizes_response() -> None:
    client = FakeJsonGenerator(
        {
            "articles": [
                {"article_index": 0, "content_type": "news", "category": "world"},
                {"article_index": 1, "content_type": "news", "category": "world"},
                {"article_index": 2, "content_type": "news", "category": "world"},
            ],
            "groups": [{"article_indexes": [0, 1]}, {"article_indexes": [2]}],
        }
    )

    result = group_articles_with_gemini(_articles(), mode="titles_summaries", client=client)

    assert result["group_count"] == 2
    assert result["multi_article_group_count"] == 1
    assert result["singleton_count"] == 1
    assert result["usage"] == {"promptTokenCount": 30, "candidatesTokenCount": 12}
    assert '"summary"' in client.prompts[0]
    assert "reader-facing story clusters" in client.prompts[0]
    assert "The editorial stage will separate those angles later" in client.prompts[0]


def test_titles_mode_omits_summaries_from_prompt() -> None:
    client = FakeJsonGenerator(
        {
            "articles": [
                {"article_index": 0, "content_type": "news", "category": "world"},
                {"article_index": 1, "content_type": "news", "category": "world"},
                {"article_index": 2, "content_type": "news", "category": "world"},
            ],
            "groups": [{"article_indexes": [0]}, {"article_indexes": [1]}, {"article_indexes": [2]}],
        }
    )

    group_articles_with_gemini(_articles(), mode="titles", client=client)

    assert '"summary"' not in client.prompts[0]


def test_grouping_prompt_prefers_digest_summary_and_key_facts() -> None:
    client = FakeJsonGenerator(
        {
            "articles": [
                {"article_index": 0, "content_type": "news", "category": "world"},
            ],
            "groups": [{"article_indexes": [0]}],
        }
    )
    article = ArticleForAggregation(
        article_id="a1",
        source_id="source-a",
        source_name="Source A",
        headline="Policy changes announced",
        summary="Site teaser summary.",
        published_at="2026-05-24T10:00:00Z",
        article_path="data/staging/articles/a1.json",
        digest_summary="The government announced a policy change with specific implementation dates.",
        digest_key_facts=("The change starts in June.", "The agency said costs will rise."),
    )

    group_articles_with_gemini([article], mode="titles_summaries", client=client)

    assert "The government announced a policy change" in client.prompts[0]
    assert "The change starts in June." in client.prompts[0]
    assert "Site teaser summary" not in client.prompts[0]


def test_compare_groupings_reports_pair_agreement() -> None:
    comparison = compare_groupings(
        [{"article_indexes": [0, 1]}, {"article_indexes": [2]}],
        [{"article_indexes": [0, 1, 2]}],
        article_count=3,
    )

    assert comparison["shared_multi_article_pairs"] == 1
    assert comparison["pairs_only_with_summaries"] == [[0, 2], [1, 2]]
    assert comparison["pairs_only_with_titles"] == []
    assert comparison["pair_jaccard"] == 0.3333


def test_validate_newsworthiness_response_accepts_scores() -> None:
    scores = validate_newsworthiness_response(
        {
            "scores": [
                {
                    "group_index": 0,
                    "global_score": 0.91,
                    "category_score": 0.97,
                    "rationale_codes": ["Geopolitical Escalation"],
                }
            ]
        },
        valid_group_indexes={0},
    )

    assert scores == {
        0: {
            "global": 0.91,
            "category": 0.97,
            "rationale_codes": ["geopolitical-escalation"],
        }
    }


@pytest.mark.parametrize(
    "payload,match",
    [
        ({}, "scores list"),
        (
            {"scores": [{"group_index": 1, "global_score": 0.5, "category_score": 0.5, "rationale_codes": []}]},
            "invalid or out of range",
        ),
        (
            {"scores": [{"group_index": 0, "global_score": 1.5, "category_score": 0.5, "rationale_codes": []}]},
            "between",
        ),
        (
            {"scores": [{"group_index": 0, "global_score": 0.5, "category_score": "0.5", "rationale_codes": []}]},
            "numeric",
        ),
    ],
)
def test_validate_newsworthiness_response_rejects_invalid_scores(payload: dict[str, Any], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        validate_newsworthiness_response(payload, valid_group_indexes={0})


def test_score_groups_newsworthiness_merges_model_scores_with_baseline() -> None:
    client = FakeJsonGenerator(
        {
            "scores": [
                {
                    "group_index": 0,
                    "global_score": 0.6,
                    "category_score": 0.8,
                    "rationale_codes": ["major_sports_result"],
                }
            ]
        }
    )

    result = score_groups_newsworthiness(
        articles=_articles(),
        groups=[
            {"group_index": 0, "article_indexes": [0, 1], "sources": ["Source A", "Source B"]},
            {"group_index": 1, "article_indexes": [2], "sources": ["Source C"]},
        ],
        client=client,
    )

    assert result["fallback_count"] == 1
    assert result["scores_by_group_index"][0]["global"] == 0.6
    assert result["scores_by_group_index"][0]["category"] == 0.8
    assert result["scores_by_group_index"][0]["model"] == "fake-model"
    assert result["scores_by_group_index"][1]["model"] == "deterministic-baseline"


def test_score_groups_newsworthiness_uses_digest_impact_without_model_call() -> None:
    client = FakeJsonGenerator({"scores": []})
    article = ArticleForAggregation(
        article_id="a1",
        source_id="source-a",
        source_name="Source A",
        headline="Major safety warning issued",
        summary="Summary",
        published_at="2026-05-24T10:00:00Z",
        article_path="data/staging/articles/a1.json",
        digest_impact={
            "global": 0.72,
            "category": 0.91,
            "scope": "national",
            "novelty": "breaking",
            "rationale_codes": ["public_safety"],
        },
    )

    result = score_groups_newsworthiness(
        articles=[article],
        groups=[{"group_index": 0, "article_indexes": [0], "sources": ["Source A"]}],
        client=client,
    )

    assert client.prompts == []
    assert result["fallback_count"] == 0
    assert result["usage"] == {}
    assert result["scores_by_group_index"][0]["global"] == 0.72
    assert result["scores_by_group_index"][0]["category"] == 0.91
    assert result["scores_by_group_index"][0]["model"] == "deterministic-digest-impact"
    assert "public_safety" in result["scores_by_group_index"][0]["rationale_codes"]


def test_load_unprocessed_articles_filters_time_window(tmp_path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    with StateDB(db_path) as state:
        for index, published_at in enumerate(
            ["2026-05-24T15:59:59Z", "2026-05-24T16:00:00Z", "2026-05-24T21:59:59Z", "2026-05-24T22:00:00Z"]
        ):
            article = {
                "article_id": f"a{index}",
                "source_id": "source",
                "source_name": "Source",
                "url": f"https://example.com/{index}",
                "headline": f"Headline {index}",
                "summary": f"Summary {index}",
                "published_at": published_at,
                "publish_date_estimated": False,
                "fetched_at": published_at,
                "content_type": "unknown",
                "language": "en",
                "collection": {},
                "fingerprints": {},
            }
            state.insert_article(article, tmp_path / f"a{index}.json")

        articles = load_unprocessed_articles(
            limit=None,
            published_after="2026-05-24T16:00:00Z",
            published_before="2026-05-24T22:00:00Z",
            db=state,
        )

    assert [article.article_id for article in articles] == ["a2", "a1"]


def test_load_window_articles_filters_low_impact_and_marks_status(tmp_path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    with StateDB(db_path) as state:
        for article_id, global_impact, category_impact in (
            ("high", 0.1, 0.7),
            ("low", 0.9, 0.1),
            ("missing", None, None),
        ):
            article_path = tmp_path / f"{article_id}.json"
            payload = {
                "article_id": article_id,
                "source_id": "source",
                "source_name": "Source",
                "url": f"https://example.com/{article_id}",
                "headline": f"Headline {article_id}",
                "summary": f"Summary {article_id}",
                "published_at": "2026-05-24T16:30:00Z",
                "publish_date_estimated": False,
                "fetched_at": "2026-05-24T16:31:00Z",
                "content_type": "unknown",
                "language": "en",
                "collection": {},
                "fingerprints": {},
            }
            if category_impact is not None:
                payload["llm_digest"] = {
                    "summary": "Generated digest.",
                    "key_facts": ["A key fact."],
                    "impact": {"global": global_impact, "category": category_impact},
                }
            article_path.write_text(json.dumps(payload), encoding="utf-8")
            state.insert_article(payload, article_path)

        articles = load_window_articles(
            window_start="2026-05-24T16:00:00Z",
            window_end="2026-05-24T22:00:00Z",
            min_category_impact=0.25,
            db=state,
        )
        statuses = {
            row["article_id"]: (row["aggregation_status"], row["is_filtered"])
            for row in state.conn.execute(
                "SELECT article_id, aggregation_status, is_filtered FROM articles ORDER BY article_id"
            )
        }

    assert [article.article_id for article in articles] == ["high"]
    assert statuses == {
        "high": ("pending", 0),
        "low": ("filtered_low_impact", 1),
        "missing": ("pending", 0),
    }


def test_load_window_articles_filters_non_news_and_spammy_content(tmp_path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    with StateDB(db_path) as state:
        cases = (
            ("eligible", "Real news headline", "ok", ["public_safety"], "pending"),
            ("non-news", "Product roundup", "non_news", ["product_recommendation"], "filtered_non_news"),
            ("promo", "Sports betting picks", "ok", ["gambling_advice"], "filtered_low_signal_content"),
            ("video", "(untitled)", "ok", ["video_page"], "filtered_video_or_carousel"),
            ("gallery", "Campaign photos", "ok", ["gallery_page"], "filtered_video_or_carousel"),
            (
                "background",
                "Candidate profile",
                "ok",
                ["profile_or_background"],
                "filtered_low_signal_content",
            ),
        )
        for article_id, headline, content_quality, rationale_codes, _expected_status in cases:
            article_path = tmp_path / f"{article_id}.json"
            payload = {
                "article_id": article_id,
                "source_id": "source",
                "source_name": "Source",
                "url": f"https://example.com/{article_id}",
                "headline": headline,
                "summary": f"Summary {article_id}",
                "published_at": "2026-05-24T16:30:00Z",
                "publish_date_estimated": False,
                "fetched_at": "2026-05-24T16:31:00Z",
                "content_type": "unknown",
                "language": "en",
                "collection": {},
                "fingerprints": {},
                "llm_digest": {
                    "summary": "Generated digest.",
                    "key_facts": ["A key fact."],
                    "content_quality": content_quality,
                    "impact": {
                        "global": 0.9,
                        "category": 0.9,
                        "rationale_codes": rationale_codes,
                    },
                },
            }
            article_path.write_text(json.dumps(payload), encoding="utf-8")
            state.insert_article(payload, article_path)

        articles = load_window_articles(
            window_start="2026-05-24T16:00:00Z",
            window_end="2026-05-24T22:00:00Z",
            min_category_impact=0.25,
            db=state,
        )
        statuses = {
            row["article_id"]: (row["aggregation_status"], row["is_filtered"])
            for row in state.conn.execute(
                "SELECT article_id, aggregation_status, is_filtered FROM articles ORDER BY article_id"
            )
        }

    assert [article.article_id for article in articles] == ["eligible"]
    assert statuses == {
        "background": ("filtered_low_signal_content", 1),
        "eligible": ("pending", 0),
        "gallery": ("filtered_video_or_carousel", 1),
        "non-news": ("filtered_non_news", 1),
        "promo": ("filtered_low_signal_content", 1),
        "video": ("filtered_video_or_carousel", 1),
    }


def test_load_window_articles_dry_run_does_not_mutate_status(tmp_path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    with StateDB(db_path) as state:
        for article_id, content_quality, rationale_codes in (
            ("eligible", "ok", ["public_safety"]),
            ("non-news", "non_news", ["product_recommendation"]),
        ):
            article_path = tmp_path / f"{article_id}.json"
            payload = {
                "article_id": article_id,
                "source_id": "source",
                "source_name": "Source",
                "url": f"https://example.com/{article_id}",
                "headline": f"Headline {article_id}",
                "summary": "Summary",
                "published_at": "2026-05-24T16:30:00Z",
                "publish_date_estimated": False,
                "fetched_at": "2026-05-24T16:31:00Z",
                "content_type": "unknown",
                "language": "en",
                "collection": {},
                "fingerprints": {},
                "llm_digest": {
                    "summary": "Generated digest.",
                    "key_facts": ["A key fact."],
                    "content_quality": content_quality,
                    "impact": {
                        "global": 0.9,
                        "category": 0.9,
                        "rationale_codes": rationale_codes,
                    },
                },
            }
            article_path.write_text(json.dumps(payload), encoding="utf-8")
            state.insert_article(payload, article_path)

        # Plant a sentinel reason that the helper would clear if it ran.
        state.conn.execute("UPDATE articles SET aggregation_reason = 'sentinel'")
        state.conn.commit()

        articles = load_window_articles(
            window_start="2026-05-24T16:00:00Z",
            window_end="2026-05-24T22:00:00Z",
            min_category_impact=0.25,
            mark_filtered=False,
            db=state,
        )
        statuses = {
            row["article_id"]: (row["aggregation_status"], row["aggregation_reason"], row["is_filtered"])
            for row in state.conn.execute(
                "SELECT article_id, aggregation_status, aggregation_reason, is_filtered "
                "FROM articles ORDER BY article_id"
            )
        }

    assert [article.article_id for article in articles] == ["eligible"]
    # No DB writes happened: both rows retain the sentinel and their default
    # pending/0 status. The non-news row is excluded from the returned list
    # but not flipped to filtered.
    assert statuses == {
        "eligible": ("pending", "sentinel", 0),
        "non-news": ("pending", "sentinel", 0),
    }


def test_plan_sliding_windows_skips_completed_except_latest(tmp_path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    with StateDB(db_path) as state:
        for index, window_start in enumerate(("2026-05-24T00:00:00Z", "2026-05-24T01:00:00Z")):
            window_end = f"2026-05-24T0{index + 6}:00:00Z"
            state.start_aggregation_window(
                window_start=window_start,
                window_end=window_end,
                run_id=f"run-{index}",
                prompt_version="aggregation-experiment-v6",
                model="gemini-3.1-flash-lite",
            )
            state.finish_aggregation_window(
                window_start=window_start,
                window_end=window_end,
                status="completed",
                article_count=10,
                stats={},
            )

        windows = plan_sliding_windows(
            range_start="2026-05-24T00:00:00Z",
            range_end="2026-05-24T09:00:00Z",
            window_hours=6,
            step_hours=1,
            db=state,
        )

    assert [(window.window_start, window.window_end) for window in windows] == [
        ("2026-05-24T01:00:00Z", "2026-05-24T07:00:00Z"),
        ("2026-05-24T02:00:00Z", "2026-05-24T08:00:00Z"),
        ("2026-05-24T03:00:00Z", "2026-05-24T09:00:00Z"),
    ]


def test_plan_sliding_windows_reruns_completed_windows_with_unassigned_articles(tmp_path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    with StateDB(db_path) as state:
        for index, window_start in enumerate(("2026-05-24T00:00:00Z", "2026-05-24T01:00:00Z", "2026-05-24T02:00:00Z")):
            window_end = f"2026-05-24T0{index + 6}:00:00Z"
            state.start_aggregation_window(
                window_start=window_start,
                window_end=window_end,
                run_id=f"run-{index}",
                prompt_version="aggregation-v6",
                model="gemini-3.1-flash-lite",
            )
            state.finish_aggregation_window(
                window_start=window_start,
                window_end=window_end,
                status="completed",
                article_count=10,
                stats={},
            )
        state.insert_article(
            {
                "article_id": "late-article",
                "source_id": "source",
                "source_name": "Source",
                "url": "https://example.com/late",
                "headline": "Late arriving article",
                "summary": "Summary",
                "published_at": "2026-05-24T02:30:00Z",
                "publish_date_estimated": False,
                "fetched_at": "2026-05-24T08:00:00Z",
                "content_type": "unknown",
                "language": "en",
                "collection": {},
                "fingerprints": {},
            },
            tmp_path / "late-article.json",
        )

        windows = plan_sliding_windows(
            range_start="2026-05-24T00:00:00Z",
            range_end="2026-05-24T08:00:00Z",
            window_hours=6,
            step_hours=1,
            db=state,
        )

    pairs = [(window.window_start, window.window_end) for window in windows]
    assert ("2026-05-24T00:00:00Z", "2026-05-24T06:00:00Z") in pairs
    assert ("2026-05-24T01:00:00Z", "2026-05-24T07:00:00Z") in pairs


def test_apply_grouping_result_creates_event_and_assigns_articles(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "pipeline.db"
    event_dir = tmp_path / "events"
    migrate(db_path)
    monkeypatch.setattr("pipeline.aggregate.EVENT_DIR", event_dir)

    with StateDB(db_path) as state:
        articles = _articles()[:2]
        for article in articles:
            state.insert_article(
                {
                    "article_id": article.article_id,
                    "source_id": article.source_id,
                    "source_name": article.source_name,
                    "url": f"https://example.com/{article.article_id}",
                    "headline": article.headline,
                    "summary": article.summary,
                    "published_at": article.published_at,
                    "publish_date_estimated": False,
                    "fetched_at": article.published_at,
                    "content_type": "unknown",
                    "language": "en",
                    "collection": {},
                    "fingerprints": {},
                },
                tmp_path / f"{article.article_id}.json",
            )

        stats = apply_grouping_result(
            articles=articles,
            groups=[{"article_indexes": [0, 1]}],
            state=state,
            article_classifications={
                0: {"content_type": "news", "category": "technology"},
                1: {"content_type": "news", "category": "technology"},
            },
            scores_by_group_index={
                0: {
                    "global": 0.42,
                    "category": 0.88,
                    "rationale_codes": ["technology_vertical"],
                    "scored_at": "2026-05-24T10:03:00Z",
                    "model": "fake-model",
                    "prompt_version": "newsworthiness-v1",
                }
            },
        )

        assert stats["events_created"] == 1
        event_rows = state.conn.execute(
            "SELECT event_id, article_count, newsworthiness_global, newsworthiness_category FROM events"
        ).fetchall()
        assert len(event_rows) == 1
        assert event_rows[0]["article_count"] == 2
        assert event_rows[0]["newsworthiness_global"] == 0.42
        assert event_rows[0]["newsworthiness_category"] == 0.88
        assert state.conn.execute("SELECT category FROM events").fetchone()["category"] == "technology"
        assigned = {
            row["event_id"]
            for row in state.conn.execute("SELECT event_id FROM articles WHERE article_id IN ('a1', 'a2')")
        }
        assert assigned == {event_rows[0]["event_id"]}
        assert list(event_dir.glob("*.json"))


def test_apply_grouping_result_opinion_filtering(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "pipeline.db"
    event_dir = tmp_path / "events"
    migrate(db_path)
    monkeypatch.setattr("pipeline.aggregate.EVENT_DIR", event_dir)

    with StateDB(db_path) as state:
        articles = _articles()[:2]
        for article in articles:
            state.insert_article(
                {
                    "article_id": article.article_id,
                    "source_id": article.source_id,
                    "source_name": article.source_name,
                    "url": f"https://example.com/{article.article_id}",
                    "headline": article.headline,
                    "summary": article.summary,
                    "published_at": article.published_at,
                    "publish_date_estimated": False,
                    "fetched_at": article.published_at,
                    "content_type": "unknown",
                    "language": "en",
                    "collection": {},
                    "fingerprints": {},
                },
                tmp_path / f"{article.article_id}.json",
            )

        # Test: All opinions -> should NOT create event
        stats = apply_grouping_result(
            articles=articles,
            groups=[{"article_indexes": [0, 1]}],
            state=state,
            article_classifications={
                0: {"content_type": "opinion", "category": "world"},
                1: {"content_type": "opinion", "category": "world"},
            },
        )
        assert stats["events_created"] == 0
        assert len(state.conn.execute("SELECT event_id FROM events").fetchall()) == 0


def test_apply_grouping_result_event_merging(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "pipeline.db"
    event_dir = tmp_path / "events"
    migrate(db_path)
    monkeypatch.setattr("pipeline.aggregate.EVENT_DIR", event_dir)

    with StateDB(db_path) as state:
        articles = _articles()
        # Insert articles: a1 (assigned to Event A), a2 (assigned to Event B), a3 (unassigned)
        for article in articles:
            state.insert_article(
                {
                    "article_id": article.article_id,
                    "source_id": article.source_id,
                    "source_name": article.source_name,
                    "url": f"https://example.com/{article.article_id}",
                    "headline": article.headline,
                    "summary": article.summary,
                    "published_at": article.published_at,
                    "publish_date_estimated": False,
                    "fetched_at": article.published_at,
                    "content_type": "unknown",
                    "language": "en",
                    "collection": {},
                    "fingerprints": {},
                },
                tmp_path / f"{article.article_id}.json",
            )

        # Pre-assign in DB and create event JSON files
        state.conn.execute("UPDATE articles SET event_id = 'event-A' WHERE article_id = 'a1'")
        state.conn.execute("UPDATE articles SET event_id = 'event-B' WHERE article_id = 'a2'")

        # Create dummy old articles a0 (in Event A history) and b0 (in Event B history)
        event_dir.mkdir(parents=True, exist_ok=True)
        # Event A: contains historical a0 and current a1
        (event_dir / "event-A.json").write_text(
            json.dumps(
                {
                    "event_id": "event-A",
                    "title": "Event A",
                    "category": "world",
                    "article_ids": ["a0", "a1"],
                }
            ),
            encoding="utf-8",
        )
        state.conn.execute(
            "INSERT INTO events(event_id, title, category, status, created_at, updated_at) "
            "VALUES ('event-A', 'Event A', 'world', 'active', '...', '...')"
        )

        # Event B: contains historical b0 and current a2
        (event_dir / "event-B.json").write_text(
            json.dumps(
                {
                    "event_id": "event-B",
                    "title": "Event B",
                    "category": "world",
                    "article_ids": ["b0", "a2"],
                }
            ),
            encoding="utf-8",
        )
        state.conn.execute(
            "INSERT INTO events(event_id, title, category, status, created_at, updated_at) "
            "VALUES ('event-B', 'Event B', 'world', 'active', '...', '...')"
        )

        # Mock articles in memory loaded by aggregator
        # Let's say we have current window containing a1 (Event A), a2 (Event B), and a3 (new).
        # We simulate ArticleForAggregation loaded with event_id
        mem_articles = [
            ArticleForAggregation(
                article_id="a1",
                source_id="source-a",
                source_name="Source A",
                headline="H1",
                summary="",
                published_at="2026-05-24T10:00:00Z",
                article_path="",
                event_id="event-A",
            ),
            ArticleForAggregation(
                article_id="a2",
                source_id="source-b",
                source_name="Source B",
                headline="H2",
                summary="",
                published_at="2026-05-24T10:01:00Z",
                article_path="",
                event_id="event-B",
            ),
            ArticleForAggregation(
                article_id="a3",
                source_id="source-c",
                source_name="Source C",
                headline="H3",
                summary="",
                published_at="2026-05-24T10:02:00Z",
                article_path="",
                event_id=None,
            ),
        ]

        # Group them all together
        # The code will choose event-A as winner (since ties choose winner, A comes first alphabetically or is common)
        apply_grouping_result(
            articles=mem_articles,
            groups=[{"article_indexes": [0, 1, 2]}],
            state=state,
            article_classifications={
                0: {"content_type": "news", "category": "world"},
                1: {"content_type": "news", "category": "world"},
                2: {"content_type": "news", "category": "world"},
            },
        )

        # Assert e-B is deleted, e-A has all merged articles
        assert not (event_dir / "event-B.json").exists()
        assert (event_dir / "event-A.json").exists()

        with (event_dir / "event-A.json").open("r", encoding="utf-8") as f:
            winner_event = json.load(f)

        # Expected articles: historical a0, historical b0, current a1, current a2, current a3
        assert set(winner_event["article_ids"]) == {"a0", "b0", "a1", "a2", "a3"}

        # Verify SQLite reassignments
        assigned_A = {
            row["article_id"]
            for row in state.conn.execute("SELECT article_id FROM articles WHERE event_id = 'event-A'")
        }
        assert "a1" in assigned_A
        assert "a2" in assigned_A
        assert "a3" in assigned_A

        # Verify e-B row is deleted
        assert state.conn.execute("SELECT 1 FROM events WHERE event_id = 'event-B'").fetchone() is None


def test_aggregate_once_dry_run_does_not_mutate_window_or_run_state(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    with StateDB(db_path) as state:
        state.start_aggregation_window(
            window_start="2026-05-24T00:00:00Z",
            window_end="2026-05-24T06:00:00Z",
            run_id="run-stale",
            prompt_version="aggregation-v6",
            model="gemini-3.1-flash-lite",
        )

    monkeypatch.setattr("pipeline.aggregate.StateDB", lambda: StateDB(db_path))
    monkeypatch.setattr("pipeline.aggregate.LOCK_PATH", tmp_path / "pipeline.lock")

    stats = aggregate_once(dry_run=True, client=FakeJsonGenerator({}))

    assert stats["stale_windows_recovered"] == 0
    with StateDB(db_path) as state:
        assert (
            state.aggregation_window_status(
                "2026-05-24T00:00:00Z",
                "2026-05-24T06:00:00Z",
            )
            == "running"
        )
        assert state.conn.execute("SELECT COUNT(*) AS count FROM pipeline_runs").fetchone()["count"] == 0


def test_aggregate_once_marks_failed_run_when_all_windows_fail(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    with StateDB(db_path) as state:
        article = _articles()[0]
        article_payload = {
            "article_id": article.article_id,
            "source_id": article.source_id,
            "source_name": article.source_name,
            "url": f"https://example.com/{article.article_id}",
            "headline": article.headline,
            "summary": article.summary,
            "published_at": "2026-05-24T01:00:00Z",
            "publish_date_estimated": False,
            "fetched_at": "2026-05-24T01:01:00Z",
            "content_type": "unknown",
            "language": "en",
            "collection": {},
            "fingerprints": {},
            "llm_digest": {
                "summary": "Eligible generated digest.",
                "key_facts": ["A key fact."],
                "impact": {"global": 0.7, "category": 0.7},
            },
        }
        article_path = tmp_path / "a1.json"
        article_path.write_text(json.dumps(article_payload), encoding="utf-8")
        state.insert_article(
            article_payload,
            article_path,
        )

    monkeypatch.setattr("pipeline.aggregate.StateDB", lambda: StateDB(db_path))
    monkeypatch.setattr("pipeline.aggregate.LOCK_PATH", tmp_path / "pipeline.lock")

    stats = aggregate_once(
        range_start="2026-05-24T00:00:00Z",
        range_end="2026-05-24T06:00:00Z",
        client=FailingJsonGenerator(),
    )

    assert stats["windows_failed"] == 1
    with StateDB(db_path) as state:
        assert state.conn.execute("SELECT status FROM pipeline_runs").fetchone()["status"] == "failed"


def test_aggregate_once_rejects_partial_range() -> None:
    with pytest.raises(ValueError, match="must be provided together"):
        aggregate_once(range_start="2026-05-24T00:00:00Z", range_end=None)

    with pytest.raises(ValueError, match="must be provided together"):
        aggregate_once(range_start=None, range_end="2026-05-24T06:00:00Z")


def test_plan_sliding_windows_emits_trailing_partial(tmp_path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    with StateDB(db_path) as state:
        windows = plan_sliding_windows(
            range_start="2026-05-24T00:00:00Z",
            range_end="2026-05-24T08:30:00Z",
            window_hours=6,
            step_hours=1,
            db=state,
        )

    pairs = [(w.window_start, w.window_end) for w in windows]
    # Full step-aligned windows end at 06:00, 07:00, 08:00; the trailing partial
    # window covers articles between 08:00 and 08:30.
    assert ("2026-05-24T02:30:00Z", "2026-05-24T08:30:00Z") in pairs


def test_fail_stale_running_aggregation_windows(tmp_path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    with StateDB(db_path) as state:
        state.start_aggregation_window(
            window_start="2026-05-24T00:00:00Z",
            window_end="2026-05-24T06:00:00Z",
            run_id="run-stale",
            prompt_version="aggregation-v6",
            model="gemini-3.1-flash-lite",
        )
        assert state.aggregation_window_status("2026-05-24T00:00:00Z", "2026-05-24T06:00:00Z") == "running"

        recovered = state.fail_stale_running_aggregation_windows()
        assert recovered == 1

        assert state.aggregation_window_status("2026-05-24T00:00:00Z", "2026-05-24T06:00:00Z") == "failed"

        assert state.fail_stale_running_aggregation_windows() == 0


def test_merge_events_into(tmp_path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    with StateDB(db_path) as state:
        for article in _articles():
            state.insert_article(
                {
                    "article_id": article.article_id,
                    "source_id": article.source_id,
                    "source_name": article.source_name,
                    "url": f"https://example.com/{article.article_id}",
                    "headline": article.headline,
                    "summary": article.summary,
                    "published_at": article.published_at,
                    "publish_date_estimated": False,
                    "fetched_at": article.published_at,
                    "content_type": "unknown",
                    "language": "en",
                    "collection": {},
                    "fingerprints": {},
                },
                tmp_path / f"{article.article_id}.json",
            )
        state.conn.execute(
            "INSERT INTO events(event_id, title, category, status, created_at, updated_at) "
            "VALUES ('winner', 'W', 'world', 'active', '2026-05-24T00:00:00Z', '2026-05-24T00:00:00Z')"
        )
        state.conn.execute(
            "INSERT INTO events(event_id, title, category, status, created_at, updated_at) "
            "VALUES ('loser', 'L', 'world', 'active', '2026-05-24T00:00:00Z', '2026-05-24T00:00:00Z')"
        )
        state.conn.execute("UPDATE articles SET event_id = 'loser' WHERE article_id = 'a1'")
        state.conn.commit()

        state.merge_events_into(["loser"], "winner")

        assert (
            state.conn.execute("SELECT event_id FROM articles WHERE article_id = 'a1'").fetchone()["event_id"]
            == "winner"
        )
        assert state.conn.execute("SELECT 1 FROM events WHERE event_id = 'loser'").fetchone() is None
        assert state.conn.execute("SELECT 1 FROM events WHERE event_id = 'winner'").fetchone() is not None


def test_validate_newsworthiness_filters_collapsed_rationale_codes() -> None:
    payload = {
        "scores": [
            {
                "group_index": 0,
                "global_score": 0.5,
                "category_score": 0.5,
                "rationale_codes": ["valid_code", "!!!", "", "  ", "Another-Valid"],
            }
        ]
    }
    scores = validate_newsworthiness_response(payload, valid_group_indexes={0})
    # "!!!" sanitizes to "unknown" and is dropped; whitespace-only codes dropped.
    assert scores[0]["rationale_codes"] == ["valid_code", "another-valid"]


def test_aggregate_once_default_range(tmp_path, monkeypatch) -> None:
    from pipeline.util import utc_now

    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    state = StateDB(db_path)

    # Insert an unassigned article older than limit_dt (e.g. 2 days ago)
    ref = utc_now()
    today_start = datetime.combine(ref.date(), time.min, tzinfo=UTC)
    limit_dt = today_start - timedelta(days=1)

    # 2 days ago
    old_time = limit_dt - timedelta(hours=12)
    old_time_str = old_time.isoformat().replace("+00:00", "Z")

    # Yesterday 12 hours ago (within range)
    new_time = limit_dt + timedelta(hours=12)
    new_time_str = new_time.isoformat().replace("+00:00", "Z")

    state.insert_article({
        "article_id": "old-article",
        "source_id": "src",
        "source_name": "Src",
        "url": "https://example.com/old",
        "headline": "Old article",
        "summary": "Summary",
        "published_at": old_time_str,
        "publish_date_estimated": False,
        "fetched_at": old_time_str,
        "content_type": "unknown",
        "language": "en",
        "collection": {},
        "fingerprints": {},
        "llm_digest": {
            "summary": "Old summary",
            "key_facts": ["fact"],
            "impact": {"global": 0.5, "category": 0.5},
        },
    }, tmp_path / "old.json")

    state.insert_article({
        "article_id": "new-article",
        "source_id": "src",
        "source_name": "Src",
        "url": "https://example.com/new",
        "headline": "New article",
        "summary": "Summary",
        "published_at": new_time_str,
        "publish_date_estimated": False,
        "fetched_at": new_time_str,
        "content_type": "unknown",
        "language": "en",
        "collection": {},
        "fingerprints": {},
        "llm_digest": {
            "summary": "New summary",
            "key_facts": ["fact"],
            "impact": {"global": 0.5, "category": 0.5},
        },
    }, tmp_path / "new.json")

    monkeypatch.setattr("pipeline.aggregate.StateDB", lambda: StateDB(db_path))
    monkeypatch.setattr("pipeline.aggregate.LOCK_PATH", tmp_path / "pipeline.lock")

    planned_windows = []

    def fake_plan_sliding_windows(*args, **kwargs):
        planned_windows.append((kwargs.get("range_start"), kwargs.get("range_end")))
        return []

    monkeypatch.setattr("pipeline.aggregate.plan_sliding_windows", fake_plan_sliding_windows)

    aggregate_once(client=FakeJsonGenerator({}))

    assert len(planned_windows) == 1
    expected_start = _format_iso_timestamp(_floor_hour(limit_dt))
    assert planned_windows[0][0] == expected_start
