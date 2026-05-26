from __future__ import annotations

import json
import re
from datetime import UTC, datetime, time, timedelta
from typing import Any

import pytest

from pipeline.aggregate import (
    ArticleForAggregation,
    _category_batches_for_articles,
    _floor_utc_interval,
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


def test_group_articles_with_gemini_splits_weakly_connected_large_groups() -> None:
    articles = [
        ArticleForAggregation(
            article_id="tanker",
            source_id="source-a",
            source_name="Source A",
            headline="Mass tanker blackout rattles Gulf ahead of oil transfer amid US-Iran talks",
            summary=None,
            published_at="2026-05-24T10:00:00Z",
            article_path="tanker.json",
        ),
        ArticleForAggregation(
            article_id="netanyahu",
            source_id="source-b",
            source_name="Source B",
            headline="Trump-Netanyahu public rift masked unified front against Iran",
            summary=None,
            published_at="2026-05-24T10:01:00Z",
            article_path="netanyahu.json",
        ),
        ArticleForAggregation(
            article_id="talks",
            source_id="source-c",
            source_name="Source C",
            headline="Each side spins a different story about the US-Iran peace talks",
            summary=None,
            published_at="2026-05-24T10:02:00Z",
            article_path="talks.json",
        ),
        ArticleForAggregation(
            article_id="hajj",
            source_id="source-d",
            source_name="Source D",
            headline="Muslims begin the annual Hajj in sweltering heat against backdrop of war concerns",
            summary=None,
            published_at="2026-05-24T10:03:00Z",
            article_path="hajj.json",
        ),
    ]
    client = FakeJsonGenerator(
        {
            "articles": [
                {"article_index": 0, "content_type": "news", "category": "world"},
                {"article_index": 1, "content_type": "news", "category": "world"},
                {"article_index": 2, "content_type": "analysis", "category": "world"},
                {"article_index": 3, "content_type": "news", "category": "world"},
            ],
            "groups": [{"article_indexes": [0, 1, 2, 3]}],
        }
    )

    result = group_articles_with_gemini(articles, mode="titles_summaries", client=client)

    assert [group["article_indexes"] for group in result["groups"]] == [[0, 2], [1], [3]]


def test_group_articles_with_gemini_splits_weakly_connected_pairs() -> None:
    articles = [
        ArticleForAggregation(
            article_id="pope-ai",
            source_id="source-a",
            source_name="Source A",
            headline="Pope calls for robust regulation of AI in manifesto",
            summary=None,
            published_at="2026-05-24T10:00:00Z",
            article_path="pope-ai.json",
        ),
        ArticleForAggregation(
            article_id="pope-slavery",
            source_id="source-b",
            source_name="Source B",
            headline="Pope Leo XIV makes historic apology for Vatican role in slavery",
            summary=None,
            published_at="2026-05-24T10:01:00Z",
            article_path="pope-slavery.json",
        ),
    ]
    client = FakeJsonGenerator(
        {
            "articles": [
                {"article_index": 0, "content_type": "news", "category": "world"},
                {"article_index": 1, "content_type": "news", "category": "world"},
            ],
            "groups": [{"article_indexes": [0, 1]}],
        }
    )

    result = group_articles_with_gemini(articles, mode="titles_summaries", client=client)

    assert [group["article_indexes"] for group in result["groups"]] == [[0], [1]]


def test_group_articles_with_gemini_splits_fact_focus_format_group() -> None:
    articles = [
        ArticleForAggregation(
            article_id="ballots",
            source_id="ap",
            source_name="AP",
            headline="FACT FOCUS: Trump falsely accuses Maryland of sending illegal mail-in ballots to voters",
            summary=None,
            published_at="2026-05-24T10:00:00Z",
            article_path="ballots.json",
        ),
        ArticleForAggregation(
            article_id="climate",
            source_id="ap",
            source_name="AP",
            headline="FACT FOCUS: Trump distorts recent revisions of scientific projections of global warming",
            summary=None,
            published_at="2026-05-24T10:01:00Z",
            article_path="climate.json",
        ),
        ArticleForAggregation(
            article_id="snap",
            source_id="ap",
            source_name="AP",
            headline="FACT FOCUS: Why nearly 4.3 million people are no longer receiving food stamps",
            summary=None,
            published_at="2026-05-24T10:02:00Z",
            article_path="snap.json",
        ),
    ]
    client = FakeJsonGenerator(
        {
            "articles": [
                {"article_index": 0, "content_type": "news", "category": "us"},
                {"article_index": 1, "content_type": "news", "category": "us"},
                {"article_index": 2, "content_type": "news", "category": "us"},
            ],
            "groups": [{"article_indexes": [0, 1, 2]}],
        }
    )

    result = group_articles_with_gemini(articles, mode="titles_summaries", client=client)

    assert [group["article_indexes"] for group in result["groups"]] == [[0], [1], [2]]


def test_group_articles_with_gemini_splits_generic_social_media_pair() -> None:
    articles = [
        ArticleForAggregation(
            article_id="meta",
            source_id="ap",
            source_name="AP",
            headline="Meta settles social media addiction case brought by rural Kentucky school district",
            summary=None,
            published_at="2026-05-24T10:00:00Z",
            article_path="meta.json",
        ),
        ArticleForAggregation(
            article_id="health-advice",
            source_id="ap",
            source_name="AP",
            headline="Health advice is all over social media. Here's how to vet claims",
            summary=None,
            published_at="2026-05-24T10:01:00Z",
            article_path="health-advice.json",
        ),
    ]
    client = FakeJsonGenerator(
        {
            "articles": [
                {"article_index": 0, "content_type": "news", "category": "health"},
                {"article_index": 1, "content_type": "news", "category": "health"},
            ],
            "groups": [{"article_indexes": [0, 1]}],
        }
    )

    result = group_articles_with_gemini(articles, mode="titles_summaries", client=client)

    assert [group["article_indexes"] for group in result["groups"]] == [[0], [1]]


def test_group_articles_with_gemini_splits_shared_holiday_context() -> None:
    articles = [
        ArticleForAggregation(
            article_id="travel",
            source_id="source-a",
            source_name="Source A",
            headline="Memorial Day: Higher fuel prices have some Americans scaling back travel plans",
            summary=None,
            published_at="2026-05-24T10:00:00Z",
            article_path="travel.json",
        ),
        ArticleForAggregation(
            article_id="ice",
            source_id="source-b",
            source_name="Source B",
            headline="New Jersey governor spends Memorial Day protesting ICE facility",
            summary=None,
            published_at="2026-05-24T10:01:00Z",
            article_path="ice.json",
        ),
    ]
    client = FakeJsonGenerator(
        {
            "articles": [
                {"article_index": 0, "content_type": "news", "category": "business"},
                {"article_index": 1, "content_type": "news", "category": "politics"},
            ],
            "groups": [{"article_indexes": [0, 1]}],
        }
    )

    result = group_articles_with_gemini(articles, mode="titles_summaries", client=client)

    assert [group["article_indexes"] for group in result["groups"]] == [[0], [1]]


def test_group_articles_with_gemini_keeps_two_named_anchor_pair() -> None:
    articles = [
        ArticleForAggregation(
            article_id="ferrari-luce",
            source_id="car-and-driver",
            source_name="Car and Driver",
            headline="2027 Ferrari Luce",
            summary=None,
            published_at="2026-05-24T10:00:00Z",
            article_path="ferrari-luce.json",
        ),
        ArticleForAggregation(
            article_id="ferrari-jony-ive",
            source_id="source-b",
            source_name="Source B",
            headline="Ferrari Luce EV debuts with Jony Ive-designed cockpit",
            summary=None,
            published_at="2026-05-24T10:01:00Z",
            article_path="ferrari-jony-ive.json",
        ),
    ]
    client = FakeJsonGenerator(
        {
            "articles": [
                {"article_index": 0, "content_type": "news", "category": "automotive"},
                {"article_index": 1, "content_type": "news", "category": "automotive"},
            ],
            "groups": [{"article_indexes": [0, 1]}],
        }
    )

    result = group_articles_with_gemini(articles, mode="titles_summaries", client=client)

    assert [group["article_indexes"] for group in result["groups"]] == [[0, 1]]


def test_group_articles_with_gemini_drops_existing_event_id_from_weak_split() -> None:
    articles = [
        ArticleForAggregation(
            article_id="pope-ai",
            source_id="source-a",
            source_name="Source A",
            headline="Pope calls for robust regulation of AI in manifesto",
            summary=None,
            published_at="2026-05-24T10:00:00Z",
            article_path="pope-ai.json",
        ),
        ArticleForAggregation(
            article_id="pope-slavery",
            source_id="source-b",
            source_name="Source B",
            headline="Pope Leo XIV makes historic apology for Vatican role in slavery",
            summary=None,
            published_at="2026-05-24T10:01:00Z",
            article_path="pope-slavery.json",
        ),
    ]
    client = FakeJsonGenerator(
        {
            "articles": [
                {"article_index": 0, "content_type": "news", "category": "world"},
                {"article_index": 1, "content_type": "news", "category": "world"},
            ],
            "groups": [{"article_indexes": [0, 1], "existing_event_id": "pope-slavery-event"}],
        }
    )

    result = group_articles_with_gemini(
        articles,
        mode="titles_summaries",
        client=client,
        active_events=[
            {
                "event_id": "pope-slavery-event",
                "title": "Pope Leo XIV makes historic apology for Vatican role in slavery",
                "category": "world",
            }
        ],
    )

    assert result["groups"] == [
        {
            "group_index": 0,
            "article_indexes": [0],
            "category": "world",
            "headlines": ["Pope calls for robust regulation of AI in manifesto"],
            "sources": ["Source A"],
        },
        {
            "group_index": 1,
            "article_indexes": [1],
            "category": "world",
            "headlines": ["Pope Leo XIV makes historic apology for Vatican role in slavery"],
            "sources": ["Source B"],
            "existing_event_id": "pope-slavery-event",
        },
    ]


def test_group_articles_with_gemini_drops_existing_event_id_from_mismatched_component() -> None:
    articles = [
        ArticleForAggregation(
            article_id="hezbollah-bbc",
            source_id="source-a",
            source_name="Source A",
            headline="Netanyahu says Israel will intensify strikes against Hezbollah",
            summary=None,
            published_at="2026-05-24T10:00:00Z",
            article_path="hezbollah-bbc.json",
        ),
        ArticleForAggregation(
            article_id="hezbollah-ap",
            source_id="source-b",
            source_name="Source B",
            headline="Israel military strikes Hezbollah sites as Netanyahu vows more blows",
            summary=None,
            published_at="2026-05-24T10:01:00Z",
            article_path="hezbollah-ap.json",
        ),
    ]
    client = FakeJsonGenerator(
        {
            "articles": [
                {"article_index": 0, "content_type": "news", "category": "world"},
                {"article_index": 1, "content_type": "news", "category": "world"},
            ],
            "groups": [{"article_indexes": [0, 1], "existing_event_id": "iran-deal-event"}],
        }
    )

    result = group_articles_with_gemini(
        articles,
        mode="titles_summaries",
        client=client,
        active_events=[
            {
                "event_id": "iran-deal-event",
                "title": "Iran war Trump will not rush deal with Tehran",
                "category": "world",
            }
        ],
    )

    assert "existing_event_id" not in result["groups"][0]
    assert result["groups"][0]["article_indexes"] == [0, 1]


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
                    "rationale_codes": ["entertainment_major_release"],
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
        for index, window_start in enumerate(("2026-05-24T00:00:00Z", "2026-05-24T06:00:00Z")):
            window_end = f"2026-05-24T{index * 6 + 7:02d}:00:00Z"
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
            range_end="2026-05-24T18:00:00Z",
            window_hours=6,
            step_hours=6,
            overlap_hours=1,
            db=state,
        )

    assert [(window.window_start, window.window_end) for window in windows] == [
        ("2026-05-24T06:00:00Z", "2026-05-24T13:00:00Z"),
        ("2026-05-24T12:00:00Z", "2026-05-24T19:00:00Z"),
    ]


def test_plan_sliding_windows_reruns_completed_windows_with_unassigned_articles(tmp_path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    with StateDB(db_path) as state:
        for index, window_start in enumerate(
            ("2026-05-24T00:00:00Z", "2026-05-24T06:00:00Z", "2026-05-24T12:00:00Z")
        ):
            window_end = f"2026-05-24T{index * 6 + 7:02d}:00:00Z"
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
            range_end="2026-05-24T18:00:00Z",
            window_hours=6,
            step_hours=6,
            overlap_hours=1,
            db=state,
        )

    pairs = [(window.window_start, window.window_end) for window in windows]
    assert ("2026-05-24T00:00:00Z", "2026-05-24T07:00:00Z") in pairs
    assert ("2026-05-24T12:00:00Z", "2026-05-24T19:00:00Z") in pairs


def test_plan_sliding_windows_with_force(tmp_path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    with StateDB(db_path) as state:
        for index, window_start in enumerate(("2026-05-24T00:00:00Z", "2026-05-24T06:00:00Z")):
            window_end = f"2026-05-24T{index * 6 + 7:02d}:00:00Z"
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

        # Without force: the first window is completed and skipped
        windows_no_force = plan_sliding_windows(
            range_start="2026-05-24T00:00:00Z",
            range_end="2026-05-24T18:00:00Z",
            window_hours=6,
            step_hours=6,
            overlap_hours=1,
            db=state,
            force=False,
        )
        assert ("2026-05-24T00:00:00Z", "2026-05-24T07:00:00Z") not in [
            (w.window_start, w.window_end) for w in windows_no_force
        ]

        # With force: all windows in range are planned
        windows_force = plan_sliding_windows(
            range_start="2026-05-24T00:00:00Z",
            range_end="2026-05-24T18:00:00Z",
            window_hours=6,
            step_hours=6,
            overlap_hours=1,
            db=state,
            force=True,
        )
        assert ("2026-05-24T00:00:00Z", "2026-05-24T07:00:00Z") in [
            (w.window_start, w.window_end) for w in windows_force
        ]


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
        statuses = {
            row["article_id"]: (row["aggregation_status"], row["aggregation_reason"], row["is_filtered"])
            for row in state.conn.execute(
                "SELECT article_id, aggregation_status, aggregation_reason, is_filtered "
                "FROM articles ORDER BY article_id"
            )
        }
        assert statuses == {
            "a1": ("filtered_standalone_opinion", "standalone_opinion", 1),
            "a2": ("filtered_standalone_opinion", "standalone_opinion", 1),
        }


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

    from pipeline.config import PipelineConfig

    monkeypatch.setattr("pipeline.aggregate.StateDB", lambda: StateDB(db_path))
    monkeypatch.setattr("pipeline.aggregate.LOCK_PATH", tmp_path / "pipeline.lock")
    monkeypatch.setattr(
        "pipeline.aggregate.load_pipeline_config",
        lambda: PipelineConfig(
            collection={},
            aggregation={"window_hours": 6, "window_overlap_hours": 1, "window_step_hours": 6},
            retention={},
            pipeline={},
            digest={},
        ),
    )

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


def test_plan_sliding_windows_last_window_extends_past_range_end(tmp_path) -> None:
    # The last aligned window is allowed to extend past range_end via the overlap, so
    # articles published in the overlap zone after range_end are still captured by the
    # final window. (No "trailing partial" window is emitted any more — the loop simply
    # stops when current >= end_dt.)
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    with StateDB(db_path) as state:
        windows = plan_sliding_windows(
            range_start="2026-05-24T00:00:00Z",
            range_end="2026-05-24T08:30:00Z",
            window_hours=6,
            step_hours=6,
            overlap_hours=1,
            db=state,
        )

    pairs = [(w.window_start, w.window_end) for w in windows]
    assert pairs == [
        ("2026-05-24T00:00:00Z", "2026-05-24T07:00:00Z"),
        ("2026-05-24T06:00:00Z", "2026-05-24T13:00:00Z"),
    ]


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

    from pipeline.config import PipelineConfig

    monkeypatch.setattr("pipeline.aggregate.StateDB", lambda: StateDB(db_path))
    monkeypatch.setattr("pipeline.aggregate.LOCK_PATH", tmp_path / "pipeline.lock")
    monkeypatch.setattr(
        "pipeline.aggregate.load_pipeline_config",
        lambda: PipelineConfig(
            collection={},
            aggregation={"window_hours": 6, "window_overlap_hours": 1, "window_step_hours": 6},
            retention={},
            pipeline={},
            digest={},
        ),
    )

    planned_windows = []

    def fake_plan_sliding_windows(*args, **kwargs):
        planned_windows.append((kwargs.get("range_start"), kwargs.get("range_end")))
        return []

    monkeypatch.setattr("pipeline.aggregate.plan_sliding_windows", fake_plan_sliding_windows)

    aggregate_once(client=FakeJsonGenerator({}))

    assert len(planned_windows) == 1
    expected_start = _format_iso_timestamp(_floor_utc_interval(limit_dt, 6))
    assert planned_windows[0][0] == expected_start


def test_aggregate_once_default_range_snaps_to_fixed_utc_boundaries(tmp_path, monkeypatch) -> None:
    from pipeline.util import utc_now

    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    state = StateDB(db_path)

    ref = utc_now()
    today_start = datetime.combine(ref.date(), time.min, tzinfo=UTC)
    published_at = today_start - timedelta(hours=10, minutes=23)
    published_at_str = published_at.isoformat().replace("+00:00", "Z")

    state.insert_article({
        "article_id": "off-boundary-article",
        "source_id": "src",
        "source_name": "Src",
        "url": "https://example.com/off-boundary",
        "headline": "Off boundary article",
        "summary": "Summary",
        "published_at": published_at_str,
        "publish_date_estimated": False,
        "fetched_at": published_at_str,
        "content_type": "unknown",
        "language": "en",
        "collection": {},
        "fingerprints": {},
        "llm_digest": {
            "summary": "New summary",
            "key_facts": ["fact"],
            "impact": {"global": 0.5, "category": 0.5},
        },
    }, tmp_path / "off-boundary.json")

    from pipeline.config import PipelineConfig

    monkeypatch.setattr("pipeline.aggregate.StateDB", lambda: StateDB(db_path))
    monkeypatch.setattr("pipeline.aggregate.LOCK_PATH", tmp_path / "pipeline.lock")
    monkeypatch.setattr(
        "pipeline.aggregate.load_pipeline_config",
        lambda: PipelineConfig(
            collection={},
            aggregation={"window_hours": 6, "window_overlap_hours": 1, "window_step_hours": 6},
            retention={},
            pipeline={},
            digest={},
        ),
    )

    planned_windows = []

    def fake_plan_sliding_windows(*args, **kwargs):
        planned_windows.append((kwargs.get("range_start"), kwargs.get("range_end")))
        return []

    monkeypatch.setattr("pipeline.aggregate.plan_sliding_windows", fake_plan_sliding_windows)

    aggregate_once(client=FakeJsonGenerator({}))

    assert len(planned_windows) == 1
    expected_start = _format_iso_timestamp(_floor_utc_interval(published_at, 6))
    expected_end = _format_iso_timestamp(_floor_utc_interval(published_at, 6) + timedelta(hours=6))
    assert planned_windows[0] == (expected_start, expected_end)


def test_group_articles_with_gemini_with_active_events() -> None:
    client = FakeJsonGenerator(
        {
            "articles": [
                {"article_index": 0, "content_type": "news", "category": "world"},
                {"article_index": 1, "content_type": "news", "category": "world"},
            ],
            "groups": [
                {"article_indexes": [0, 1], "existing_event_id": "event-123"}
            ],
        }
    )

    active_events = [
        {"event_id": "event-123", "title": "Company announces new phone", "category": "world"}
    ]

    result = group_articles_with_gemini(
        _articles()[:2],
        mode="titles_summaries",
        client=client,
        active_events=active_events
    )

    assert result["groups"][0]["existing_event_id"] == "event-123"
    assert "Existing Active Events" in client.prompts[0]
    assert "event-123" in client.prompts[0]


def test_group_articles_with_gemini_rejects_existing_event_id_not_in_active_events() -> None:
    client = FakeJsonGenerator(
        {
            "articles": [
                {"article_index": 0, "content_type": "news", "category": "world"},
            ],
            "groups": [{"article_indexes": [0], "existing_event_id": "event-not-in-prompt"}],
        }
    )

    with pytest.raises(ValueError, match="not offered in active events"):
        group_articles_with_gemini(
            _articles()[:1],
            mode="titles_summaries",
            client=client,
            active_events=[{"event_id": "event-allowed", "title": "Allowed event", "category": "world"}],
        )

    assert len(client.prompts) == 2


@pytest.mark.parametrize("value", [None, "", " ", "null", "NULL", "none", "n/a"])
def test_group_articles_with_gemini_treats_null_existing_event_id_as_absent(value: object) -> None:
    client = FakeJsonGenerator(
        {
            "articles": [
                {"article_index": 0, "content_type": "news", "category": "world"},
            ],
            "groups": [{"article_indexes": [0], "existing_event_id": value}],
        }
    )

    result = group_articles_with_gemini(
        _articles()[:1],
        mode="titles_summaries",
        client=client,
        active_events=[],
    )

    assert "existing_event_id" not in result["groups"][0]


def test_filter_active_events_with_llm() -> None:
    from pipeline.aggregate import ArticleForAggregation, filter_active_events_with_llm

    articles = [
        ArticleForAggregation(
            article_id="art1",
            source_id="src",
            source_name="Src",
            headline="Cuba receives China rice shipment amid US threats",
            summary=None,
            published_at=None,
            article_path="art1.json"
        )
    ]

    active_events = [
        {"event_id": "cuba-event", "title": "Cuba receives China rice shipment", "category": "world"},
        {"event_id": "unrelated-event", "title": "Hottest May day in the UK", "category": "world"}
    ]

    # Mock LLM response that only returns "cuba-event" as matched
    client = FakeJsonGenerator({"matched_event_ids": ["cuba-event"]})

    result = filter_active_events_with_llm(
        articles=articles,
        active_events=active_events,
        client=client
    )

    assert len(result) == 1
    assert result[0]["event_id"] == "cuba-event"
    assert "matched_event_ids" in client.prompts[0]


def test_category_compatibility_bridges_politics_with_us_and_world() -> None:
    from pipeline.aggregate import _candidate_categories_for_group, _in_same_category_group

    assert _in_same_category_group("politics", "us")
    assert _in_same_category_group("politics", "world")
    assert not _in_same_category_group("politics", "entertainment")
    assert _candidate_categories_for_group(["politics"]) == {"politics", "us", "world"}
    assert "politics" in _candidate_categories_for_group(["us", "world", "business"])


def test_category_batches_split_and_chunk_oversized_news_business() -> None:
    from pipeline.config import FeedConfig

    feeds = {}
    for category in ("world", "us", "business", "technology"):
        feed = FeedConfig(
            source_id=f"src-{category}",
            source_name=f"{category.title()} Source",
            feed_url=f"https://example.com/{category}.xml",
            site_url=None,
            enabled=True,
            default_category=category,
            category_hints=[],
            content_hints={},
            fetch={},
        )
        feeds[feed.source_id] = feed
    articles = [
        ArticleForAggregation(
            article_id=f"world-{index}",
            source_id="src-world",
            source_name="World Source",
            headline=f"World headline {index}",
            summary=None,
            published_at="2026-05-25T10:00:00Z",
            article_path=f"world-{index}.json",
        )
        for index in range(5)
    ]
    articles += [
        ArticleForAggregation(
            article_id=f"us-{index}",
            source_id="src-us",
            source_name="US Source",
            headline=f"US headline {index}",
            summary=None,
            published_at="2026-05-25T10:00:00Z",
            article_path=f"us-{index}.json",
        )
        for index in range(2)
    ]
    articles += [
        ArticleForAggregation(
            article_id="biz-1",
            source_id="src-business",
            source_name="Business Source",
            headline="Business headline",
            summary=None,
            published_at="2026-05-25T10:00:00Z",
            article_path="biz-1.json",
        ),
        ArticleForAggregation(
            article_id="tech-1",
            source_id="src-technology",
            source_name="Tech Source",
            headline="Tech headline",
            summary=None,
            published_at="2026-05-25T10:00:00Z",
            article_path="tech-1.json",
        ),
    ]

    batches = _category_batches_for_articles(articles, feeds, max_articles=2)

    assert [batch["name"] for batch in batches] == [
        "news_business_world-1",
        "news_business_world-2",
        "news_business_world-3",
        "news_business_us",
        "news_business_business",
        "sci_tech",
    ]
    assert [len(batch["articles"]) for batch in batches] == [2, 2, 1, 2, 1, 1]
    assert all(len(batch["articles"]) <= 2 for batch in batches)
    assert batches[0]["categories"] == ["world"]
    assert batches[3]["categories"] == ["us"]
    assert batches[5]["categories"] == ["technology", "science", "health", "environment"]


def test_deduplicate_active_events_llm(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)

    event_dir = tmp_path / "events"
    event_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("pipeline.aggregate.EVENT_DIR", event_dir)

    with StateDB(db_path) as state:
        event1 = {
            "event_id": "2026-05-25-sudans-war-economy",
            "title": "In Sudans war economy gold keeps flowing",
            "category": "world",
            "created_at": "2026-05-25T10:00:00Z",
            "updated_at": "2026-05-25T10:00:00Z",
            "article_ids": ["art1"],
            "article_count": 1,
        }
        event2 = {
            "event_id": "2026-05-25-sudans-war-economy-2",
            "title": "In Sudans war economy gold keeps flowing",
            "category": "world",
            "created_at": "2026-05-25T11:00:00Z",
            "updated_at": "2026-05-25T11:00:00Z",
            "article_ids": ["art2"],
            "article_count": 1,
        }

        (event_dir / "2026-05-25-sudans-war-economy.json").write_text(json.dumps(event1), encoding="utf-8")
        (event_dir / "2026-05-25-sudans-war-economy-2.json").write_text(json.dumps(event2), encoding="utf-8")

        state.upsert_event(event1, event_dir / "2026-05-25-sudans-war-economy.json")
        state.upsert_event(event2, event_dir / "2026-05-25-sudans-war-economy-2.json")

        state.insert_article({
            "article_id": "art1",
            "source_id": "src",
            "source_name": "Src",
            "url": "https://example.com/1",
            "headline": "In Sudan's war economy, gold keeps flowing",
            "summary": "Summary",
            "published_at": "2026-05-25T10:00:00Z",
            "publish_date_estimated": False,
            "fetched_at": "2026-05-25T10:00:00Z",
            "content_type": "unknown",
            "language": "en",
            "collection": {},
            "fingerprints": {},
        }, tmp_path / "art1.json")

        state.insert_article({
            "article_id": "art2",
            "source_id": "src",
            "source_name": "Src",
            "url": "https://example.com/2",
            "headline": "In Sudan's war economy, gold keeps flowing",
            "summary": "Summary",
            "published_at": "2026-05-25T11:00:00Z",
            "publish_date_estimated": False,
            "fetched_at": "2026-05-25T11:00:00Z",
            "content_type": "unknown",
            "language": "en",
            "collection": {},
            "fingerprints": {},
        }, tmp_path / "art2.json")

        state.conn.execute("UPDATE articles SET event_id = '2026-05-25-sudans-war-economy' WHERE article_id = 'art1'")
        state.conn.execute("UPDATE articles SET event_id = '2026-05-25-sudans-war-economy-2' WHERE article_id = 'art2'")
        state.conn.commit()

        client = FakeJsonGenerator({"should_merge": True, "confidence": 0.95, "rationale": "Same story"})
        feeds_by_source = {}

        from pipeline.aggregate import deduplicate_active_events_llm
        deduplicate_active_events_llm(
            state=state,
            client=client,
            feeds_by_source=feeds_by_source,
        )

        assert not (event_dir / "2026-05-25-sudans-war-economy-2.json").exists()
        assert (event_dir / "2026-05-25-sudans-war-economy.json").exists()

        event1_data = json.loads((event_dir / "2026-05-25-sudans-war-economy.json").read_text(encoding="utf-8"))
        assert sorted(event1_data["article_ids"]) == ["art1", "art2"]

        reassigned_rows = state.conn.execute("SELECT article_id, event_id FROM articles").fetchall()
        assert all(row["event_id"] == "2026-05-25-sudans-war-economy" for row in reassigned_rows)


def test_deduplicate_active_events_llm_requires_high_confidence(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)

    event_dir = tmp_path / "events"
    event_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("pipeline.aggregate.EVENT_DIR", event_dir)

    with StateDB(db_path) as state:
        event1 = {
            "event_id": "2026-05-25-sudans-war-economy",
            "title": "In Sudans war economy gold keeps flowing",
            "category": "world",
            "created_at": "2026-05-25T10:00:00Z",
            "updated_at": "2026-05-25T10:00:00Z",
            "article_ids": ["art1"],
            "article_count": 1,
        }
        event2 = {
            "event_id": "2026-05-25-sudans-war-economy-2",
            "title": "In Sudans war economy gold keeps flowing",
            "category": "world",
            "created_at": "2026-05-25T11:00:00Z",
            "updated_at": "2026-05-25T11:00:00Z",
            "article_ids": ["art2"],
            "article_count": 1,
        }
        (event_dir / "2026-05-25-sudans-war-economy.json").write_text(json.dumps(event1), encoding="utf-8")
        (event_dir / "2026-05-25-sudans-war-economy-2.json").write_text(json.dumps(event2), encoding="utf-8")
        state.upsert_event(event1, event_dir / "2026-05-25-sudans-war-economy.json")
        state.upsert_event(event2, event_dir / "2026-05-25-sudans-war-economy-2.json")

        for article_id, event_id, published_at in (
            ("art1", "2026-05-25-sudans-war-economy", "2026-05-25T10:00:00Z"),
            ("art2", "2026-05-25-sudans-war-economy-2", "2026-05-25T11:00:00Z"),
        ):
            state.insert_article(
                {
                    "article_id": article_id,
                    "source_id": "src",
                    "source_name": "Src",
                    "url": f"https://example.com/{article_id}",
                    "headline": "In Sudan's war economy, gold keeps flowing",
                    "summary": "Summary",
                    "published_at": published_at,
                    "publish_date_estimated": False,
                    "fetched_at": published_at,
                    "content_type": "unknown",
                    "language": "en",
                    "collection": {},
                    "fingerprints": {},
                },
                tmp_path / f"{article_id}.json",
            )
            state.conn.execute("UPDATE articles SET event_id = ? WHERE article_id = ?", (event_id, article_id))
        state.conn.commit()

        from pipeline.aggregate import deduplicate_active_events_llm

        deduplicate_active_events_llm(
            state=state,
            client=FakeJsonGenerator({"should_merge": True, "confidence": 0.5, "rationale": "Too uncertain"}),
            feeds_by_source={},
        )

        assert (event_dir / "2026-05-25-sudans-war-economy-2.json").exists()
        assert state.conn.execute("SELECT 1 FROM events WHERE event_id = ?", (event2["event_id"],)).fetchone()


def test_deduplicate_active_events_llm_cross_category_same_group(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)

    event_dir = tmp_path / "events"
    event_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("pipeline.aggregate.EVENT_DIR", event_dir)

    with StateDB(db_path) as state:
        # event1: category world (group: news_business)
        event1 = {
            "event_id": "2026-05-25-sudans-war-economy",
            "title": "In Sudans war economy gold keeps flowing",
            "category": "world",
            "created_at": "2026-05-25T10:00:00Z",
            "updated_at": "2026-05-25T10:00:00Z",
            "article_ids": ["art1"],
            "article_count": 1,
        }
        # event2: category us (group: news_business)
        event2 = {
            "event_id": "2026-05-25-sudans-war-economy-2",
            "title": "In Sudans war economy gold keeps flowing",
            "category": "us",
            "created_at": "2026-05-25T11:00:00Z",
            "updated_at": "2026-05-25T11:00:00Z",
            "article_ids": ["art2"],
            "article_count": 1,
        }
        # event3: category entertainment (group: leisure)
        event3 = {
            "event_id": "2026-05-25-mandalorian-grogu-opens-disney-box-office",
            "title": "Mandalorian and Grogu opens at Disney box office",
            "category": "entertainment",
            "created_at": "2026-05-25T12:00:00Z",
            "updated_at": "2026-05-25T12:00:00Z",
            "article_ids": ["art3"],
            "article_count": 1,
        }

        (event_dir / "2026-05-25-sudans-war-economy.json").write_text(json.dumps(event1), encoding="utf-8")
        (event_dir / "2026-05-25-sudans-war-economy-2.json").write_text(json.dumps(event2), encoding="utf-8")
        (event_dir / f"{event3['event_id']}.json").write_text(json.dumps(event3), encoding="utf-8")

        state.upsert_event(event1, event_dir / "2026-05-25-sudans-war-economy.json")
        state.upsert_event(event2, event_dir / "2026-05-25-sudans-war-economy-2.json")
        state.upsert_event(event3, event_dir / f"{event3['event_id']}.json")

        state.insert_article({
            "article_id": "art1",
            "source_id": "src",
            "source_name": "Src",
            "url": "https://example.com/1",
            "headline": "In Sudan's war economy, gold keeps flowing",
            "summary": "Summary",
            "published_at": "2026-05-25T10:00:00Z",
            "publish_date_estimated": False,
            "fetched_at": "2026-05-25T10:00:00Z",
            "content_type": "unknown",
            "language": "en",
            "collection": {},
            "fingerprints": {},
        }, tmp_path / "art1.json")

        state.insert_article({
            "article_id": "art2",
            "source_id": "src",
            "source_name": "Src",
            "url": "https://example.com/2",
            "headline": "In Sudan's war economy, gold keeps flowing",
            "summary": "Summary",
            "published_at": "2026-05-25T11:00:00Z",
            "publish_date_estimated": False,
            "fetched_at": "2026-05-25T11:00:00Z",
            "content_type": "unknown",
            "language": "en",
            "collection": {},
            "fingerprints": {},
        }, tmp_path / "art2.json")

        state.insert_article({
            "article_id": "art3",
            "source_id": "src",
            "source_name": "Src",
            "url": "https://example.com/3",
            "headline": "Mandalorian and Grogu opens at Disney box office",
            "summary": "Summary",
            "published_at": "2026-05-25T12:00:00Z",
            "publish_date_estimated": False,
            "fetched_at": "2026-05-25T12:00:00Z",
            "content_type": "unknown",
            "language": "en",
            "collection": {},
            "fingerprints": {},
        }, tmp_path / "art3.json")

        state.conn.execute("UPDATE articles SET event_id = '2026-05-25-sudans-war-economy' WHERE article_id = 'art1'")
        state.conn.execute("UPDATE articles SET event_id = '2026-05-25-sudans-war-economy-2' WHERE article_id = 'art2'")
        state.conn.execute("UPDATE articles SET event_id = ? WHERE article_id = 'art3'", (event3["event_id"],))
        state.conn.commit()

        client = FakeJsonGenerator({"should_merge": True, "confidence": 0.95, "rationale": "Same story"})
        feeds_by_source = {}

        from pipeline.aggregate import deduplicate_active_events_llm
        deduplicate_active_events_llm(
            state=state,
            client=client,
            feeds_by_source=feeds_by_source,
        )

        # event2 (us) should be merged into event1 (world) because they are in the same category group (news_business)
        assert not (event_dir / "2026-05-25-sudans-war-economy-2.json").exists()
        # event3 (entertainment) should NOT be merged because it is in a different category group (leisure)
        assert (event_dir / f"{event3['event_id']}.json").exists()


def test_deduplicate_active_events_llm_cross_category_title_match(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)

    event_dir = tmp_path / "events"
    event_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("pipeline.aggregate.EVENT_DIR", event_dir)

    event1 = {
        "event_id": "2026-05-25-pope-leo-ai-encyclical",
        "title": "Pope Leo takes aim at big tech in AI encyclical",
        "category": "world",
        "created_at": "2026-05-25T10:00:00Z",
        "updated_at": "2026-05-25T10:00:00Z",
        "article_ids": ["art-world"],
        "article_count": 1,
    }
    event2 = {
        "event_id": "2026-05-25-pope-leo-letter-ai-takeaways",
        "title": "Pope Leo letter on AI has big tech takeaways",
        "category": "technology",
        "created_at": "2026-05-25T11:00:00Z",
        "updated_at": "2026-05-25T11:00:00Z",
        "article_ids": ["art-tech"],
        "article_count": 1,
    }

    with StateDB(db_path) as state:
        for event in (event1, event2):
            (event_dir / f"{event['event_id']}.json").write_text(json.dumps(event), encoding="utf-8")
            state.upsert_event(event, event_dir / f"{event['event_id']}.json")

        for article_id, event_id, headline in (
            ("art-world", event1["event_id"], event1["title"]),
            ("art-tech", event2["event_id"], event2["title"]),
        ):
            state.insert_article(
                {
                    "article_id": article_id,
                    "source_id": "src",
                    "source_name": "Src",
                    "url": f"https://example.com/{article_id}",
                    "headline": headline,
                    "summary": "Summary",
                    "published_at": "2026-05-25T10:00:00Z",
                    "publish_date_estimated": False,
                    "fetched_at": "2026-05-25T10:00:00Z",
                    "content_type": "unknown",
                    "language": "en",
                    "collection": {},
                    "fingerprints": {},
                },
                tmp_path / f"{article_id}.json",
            )
            state.conn.execute("UPDATE articles SET event_id = ? WHERE article_id = ?", (event_id, article_id))
        state.conn.commit()

        from pipeline.aggregate import deduplicate_active_events_llm

        deduplicate_active_events_llm(
            state=state,
            client=FakeJsonGenerator({"should_merge": True, "confidence": 0.95, "rationale": "Same event"}),
            feeds_by_source={},
        )

        remaining = {
            row["event_id"]
            for row in state.conn.execute("SELECT event_id FROM events")
        }
        assert remaining == {event1["event_id"]}
        assert not (event_dir / f"{event2['event_id']}.json").exists()


def test_deduplicate_active_events_llm_cross_group_title_cohesion(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)

    event_dir = tmp_path / "events"
    event_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("pipeline.aggregate.EVENT_DIR", event_dir)

    event1 = {
        "event_id": "2026-05-25-enhanced-games-launch",
        "title": "Las Vegas Enhanced Games launch sparks controversy and ethical debate",
        "category": "entertainment",
        "created_at": "2026-05-25T10:00:00Z",
        "updated_at": "2026-05-25T10:00:00Z",
        "article_ids": ["art-entertainment"],
        "article_count": 1,
        "keywords": ["enhanced", "games", "vegas", "controversy"],
    }
    event2 = {
        "event_id": "2026-05-25-swimmer-record-enhanced-games",
        "title": "A Swimmer Broke a World Record at the Enhanced Games",
        "category": "health",
        "created_at": "2026-05-25T11:00:00Z",
        "updated_at": "2026-05-25T11:00:00Z",
        "article_ids": ["art-health"],
        "article_count": 1,
        "keywords": ["enhanced", "games", "swimmer", "record"],
    }

    with StateDB(db_path) as state:
        for event in (event1, event2):
            path = event_dir / f"{event['event_id']}.json"
            path.write_text(json.dumps(event), encoding="utf-8")
            state.upsert_event(event, path)

        for article_id, event_id, headline, published_at in (
            ("art-entertainment", event1["event_id"], event1["title"], event1["created_at"]),
            ("art-health", event2["event_id"], event2["title"], event2["created_at"]),
        ):
            state.insert_article(
                {
                    "article_id": article_id,
                    "source_id": "src",
                    "source_name": "Src",
                    "url": f"https://example.com/{article_id}",
                    "headline": headline,
                    "summary": "Summary",
                    "published_at": published_at,
                    "publish_date_estimated": False,
                    "fetched_at": published_at,
                    "content_type": "unknown",
                    "language": "en",
                    "collection": {},
                    "fingerprints": {},
                },
                tmp_path / f"{article_id}.json",
            )
            state.conn.execute("UPDATE articles SET event_id = ? WHERE article_id = ?", (event_id, article_id))
        state.conn.commit()

        from pipeline.aggregate import deduplicate_active_events_llm

        deduplicate_active_events_llm(
            state=state,
            client=FakeJsonGenerator({"should_merge": True, "confidence": 0.95, "rationale": "Same event"}),
            feeds_by_source={},
        )

        remaining = sorted(row["event_id"] for row in state.conn.execute("SELECT event_id FROM events"))
        assert remaining == [event1["event_id"]]
        assert not (event_dir / f"{event2['event_id']}.json").exists()


def test_aggregate_once_splits_articles_into_category_groups(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    state = StateDB(db_path)

    # Insert articles from three different category groups:
    # 1. politics (group: politics_gov)
    # 2. technology (group: sci_tech)
    # 3. entertainment (group: leisure)

    from pipeline.config import FeedConfig
    mock_feeds = [
        FeedConfig(
            source_id="src-pol",
            source_name="Politics Source",
            feed_url="http://pol",
            site_url=None,
            enabled=True,
            default_category="politics",
            category_hints=[],
            content_hints={},
            fetch={}
        ),
        FeedConfig(
            source_id="src-tech",
            source_name="Tech Source",
            feed_url="http://tech",
            site_url=None,
            enabled=True,
            default_category="technology",
            category_hints=[],
            content_hints={},
            fetch={}
        ),
        FeedConfig(
            source_id="src-ent",
            source_name="Entertainment Source",
            feed_url="http://ent",
            site_url=None,
            enabled=True,
            default_category="entertainment",
            category_hints=[],
            content_hints={},
            fetch={}
        ),
    ]
    monkeypatch.setattr("pipeline.aggregate.load_feeds", lambda enabled_only=False: mock_feeds)
    monkeypatch.setattr("pipeline.aggregate.load_categories", lambda: ["politics", "technology", "entertainment"])

    t = "2026-05-25T10:00:00Z"

    art_pol = {
        "article_id": "art-pol",
        "source_id": "src-pol",
        "source_name": "Politics Source",
        "url": "https://example.com/pol",
        "headline": "Politics headline",
        "summary": "Summary",
        "published_at": t,
        "publish_date_estimated": False,
        "fetched_at": t,
        "content_type": "unknown",
        "language": "en",
        "collection": {},
        "fingerprints": {},
        "llm_digest": {
            "summary": "Pol digest",
            "key_facts": ["fact"],
            "impact": {"global": 0.5, "category": 0.5},
        },
    }
    (tmp_path / "art-pol.json").write_text(json.dumps(art_pol), encoding="utf-8")
    state.insert_article(art_pol, tmp_path / "art-pol.json")

    art_tech = {
        "article_id": "art-tech",
        "source_id": "src-tech",
        "source_name": "Tech Source",
        "url": "https://example.com/tech",
        "headline": "Tech headline",
        "summary": "Summary",
        "published_at": t,
        "publish_date_estimated": False,
        "fetched_at": t,
        "content_type": "unknown",
        "language": "en",
        "collection": {},
        "fingerprints": {},
        "llm_digest": {
            "summary": "Tech digest",
            "key_facts": ["fact"],
            "impact": {"global": 0.5, "category": 0.5},
        },
    }
    (tmp_path / "art-tech.json").write_text(json.dumps(art_tech), encoding="utf-8")
    state.insert_article(art_tech, tmp_path / "art-tech.json")

    art_ent = {
        "article_id": "art-ent",
        "source_id": "src-ent",
        "source_name": "Entertainment Source",
        "url": "https://example.com/ent",
        "headline": "Entertainment headline",
        "summary": "Summary",
        "published_at": t,
        "publish_date_estimated": False,
        "fetched_at": t,
        "content_type": "unknown",
        "language": "en",
        "collection": {},
        "fingerprints": {},
        "llm_digest": {
            "summary": "Ent digest",
            "key_facts": ["fact"],
            "impact": {"global": 0.5, "category": 0.5},
        },
    }
    (tmp_path / "art-ent.json").write_text(json.dumps(art_ent), encoding="utf-8")
    state.insert_article(art_ent, tmp_path / "art-ent.json")

    monkeypatch.setattr("pipeline.aggregate.StateDB", lambda: StateDB(db_path))
    monkeypatch.setattr("pipeline.aggregate.LOCK_PATH", tmp_path / "pipeline.lock")

    event_dir = tmp_path / "events"
    event_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("pipeline.aggregate.EVENT_DIR", event_dir)

    generator = FakeJsonGenerator({})
    called_prompts = []

    def mock_generate_json(*args, **kwargs):
        prompt = kwargs.get("prompt", "")
        called_prompts.append(prompt)
        if "Politics headline" in prompt:
            payload = {
                "articles": [{"article_index": 0, "content_type": "news", "category": "politics"}],
                "groups": [{"article_indexes": [0]}]
            }
        elif "Tech headline" in prompt:
            payload = {
                "articles": [{"article_index": 0, "content_type": "news", "category": "technology"}],
                "groups": [{"article_indexes": [0]}]
            }
        elif "Entertainment headline" in prompt:
            payload = {
                "articles": [{"article_index": 0, "content_type": "news", "category": "entertainment"}],
                "groups": [{"article_indexes": [0]}]
            }
        else:
            payload = {
                "articles": [{"article_index": 0, "content_type": "news", "category": "politics"}],
                "groups": [{"article_indexes": [0]}]
            }
        from pipeline.llm import GeminiResult
        return GeminiResult(
            payload=payload,
            elapsed_ms=10,
            usage={"promptTokenCount": 50, "candidatesTokenCount": 50},
            model="mock",
        )

    generator.generate_json = mock_generate_json

    _ = aggregate_once(
        range_start="2026-05-25T06:00:00Z",
        range_end="2026-05-25T12:00:00Z",
        client=generator,
    )

    assert len(called_prompts) == 3
    assert any("Politics headline" in p for p in called_prompts)
    assert any("Tech headline" in p for p in called_prompts)
    assert any("Entertainment headline" in p for p in called_prompts)
    for p in called_prompts:
        count = sum(h in p for h in ["Politics headline", "Tech headline", "Entertainment headline"])
        assert count == 1


def test_aggregate_once_isolates_per_category_group_failures(tmp_path, monkeypatch) -> None:
    """A failure in one category group must not throw away work from the other groups
    (or mark the whole window as failed). The window should land as 'partial_failure'
    with the successful groups' events persisted."""
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    state = StateDB(db_path)

    from pipeline.config import FeedConfig
    mock_feeds = [
        FeedConfig(
            source_id="src-pol",
            source_name="Politics Source",
            feed_url="http://pol",
            site_url=None,
            enabled=True,
            default_category="politics",
            category_hints=[],
            content_hints={},
            fetch={},
        ),
        FeedConfig(
            source_id="src-tech",
            source_name="Tech Source",
            feed_url="http://tech",
            site_url=None,
            enabled=True,
            default_category="technology",
            category_hints=[],
            content_hints={},
            fetch={},
        ),
    ]
    monkeypatch.setattr("pipeline.aggregate.load_feeds", lambda enabled_only=False: mock_feeds)
    monkeypatch.setattr(
        "pipeline.aggregate.load_categories", lambda: ["politics", "technology"]
    )

    t = "2026-05-25T10:00:00Z"
    for source_id, source_name, headline, article_id in [
        ("src-pol", "Politics Source", "Politics headline", "art-pol"),
        ("src-tech", "Tech Source", "Tech headline", "art-tech"),
    ]:
        article = {
            "article_id": article_id,
            "source_id": source_id,
            "source_name": source_name,
            "url": f"https://example.com/{article_id}",
            "headline": headline,
            "summary": "Summary",
            "published_at": t,
            "publish_date_estimated": False,
            "fetched_at": t,
            "content_type": "unknown",
            "language": "en",
            "collection": {},
            "fingerprints": {},
            "llm_digest": {
                "summary": "Digest",
                "key_facts": ["fact"],
                "impact": {"global": 0.5, "category": 0.5},
            },
        }
        (tmp_path / f"{article_id}.json").write_text(json.dumps(article), encoding="utf-8")
        state.insert_article(article, tmp_path / f"{article_id}.json")

    from pipeline.config import PipelineConfig

    monkeypatch.setattr("pipeline.aggregate.StateDB", lambda: StateDB(db_path))
    monkeypatch.setattr("pipeline.aggregate.LOCK_PATH", tmp_path / "pipeline.lock")
    monkeypatch.setattr(
        "pipeline.aggregate.load_pipeline_config",
        lambda: PipelineConfig(
            collection={},
            aggregation={"window_hours": 6, "window_overlap_hours": 0, "window_step_hours": 6},
            retention={},
            pipeline={},
            digest={},
        ),
    )
    event_dir = tmp_path / "events"
    event_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("pipeline.aggregate.EVENT_DIR", event_dir)

    generator = FakeJsonGenerator({})

    def fake_generate_json(*args, **kwargs):
        prompt = kwargs.get("prompt", "")
        # Active-events filter prompts pass through with no matches.
        if "matched_event_ids" in prompt:
            return GeminiResult(
                payload={"matched_event_ids": []},
                elapsed_ms=1,
                usage={},
                model="mock",
            )
        # Politics group succeeds; Tech group simulates the duplicate-index validation
        # failure we hit in the live run (model returns the same article in two groups).
        if "Politics headline" in prompt:
            payload = {
                "articles": [{"article_index": 0, "content_type": "news", "category": "politics"}],
                "groups": [{"article_indexes": [0]}],
            }
        elif "Tech headline" in prompt:
            payload = {
                "articles": [{"article_index": 0, "content_type": "news", "category": "technology"}],
                "groups": [{"article_indexes": [0]}, {"article_indexes": [0]}],
            }
        else:
            payload = {"articles": [], "groups": []}
        return GeminiResult(
            payload=payload,
            elapsed_ms=10,
            usage={"promptTokenCount": 10, "candidatesTokenCount": 10},
            model="mock",
        )

    generator.generate_json = fake_generate_json

    stats = aggregate_once(
        range_start="2026-05-25T06:00:00Z",
        range_end="2026-05-25T12:00:00Z",
        client=generator,
    )

    # Window is partial — politics succeeded, tech failed. windows_processed counts the
    # window (not failed), and articles_seen reflects only the successful group.
    assert stats["windows_processed"] == 1
    assert stats["windows_failed"] == 0
    assert stats["windows_partial_failed"] == 1
    assert stats["articles_seen"] == 1  # politics only

    with StateDB(db_path) as check:
        assert check.conn.execute("SELECT status FROM pipeline_runs").fetchone()["status"] == "partial_failure"
        # Politics article was assigned to an event; tech article was left alone.
        pol = check.conn.execute(
            "SELECT event_id FROM articles WHERE article_id = 'art-pol'"
        ).fetchone()
        tech = check.conn.execute(
            "SELECT event_id FROM articles WHERE article_id = 'art-tech'"
        ).fetchone()
        assert pol["event_id"] is not None
        assert tech["event_id"] is None

        # Window recorded as partial_failure so the next run retries the failing group.
        row = check.conn.execute(
            "SELECT status, stats_json FROM aggregation_windows "
            "WHERE window_start = '2026-05-25T06:00:00Z'"
        ).fetchone()
        assert row["status"] == "partial_failure"
        window_stats = json.loads(row["stats_json"])
        assert window_stats["processed_article_count"] == 1
        assert len(window_stats["group_errors"]) == 1
        assert window_stats["group_errors"][0]["category_group"] == "sci_tech"


def test_apply_grouping_result_ignores_hallucinated_existing_event_id(tmp_path, monkeypatch) -> None:
    """The LLM can return an existing_event_id that wasn't in active_events. Without a
    guard, we'd silently create an event with that hallucinated ID."""
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    event_dir = tmp_path / "events"
    event_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("pipeline.aggregate.EVENT_DIR", event_dir)

    with StateDB(db_path) as state:
        article = ArticleForAggregation(
            article_id="art-x",
            source_id="src",
            source_name="Src",
            headline="A real news headline today",
            summary="Body",
            published_at="2026-05-25T10:00:00Z",
            article_path="art-x.json",
        )
        state.insert_article(
            {
                "article_id": article.article_id,
                "source_id": article.source_id,
                "source_name": article.source_name,
                "url": "https://example.com/x",
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
            tmp_path / "art-x.json",
        )

        groups = [{
            "group_index": 0,
            "article_indexes": [0],
            "existing_event_id": "completely-made-up-event-id",
        }]
        apply_grouping_result(
            articles=[article],
            groups=groups,
            state=state,
            feeds_by_source={},
        )

        rows = state.conn.execute("SELECT event_id FROM events").fetchall()
        # Exactly one event was created, and its id is NOT the hallucinated one.
        assert len(rows) == 1
        assert rows[0]["event_id"] != "completely-made-up-event-id"


def test_filter_active_events_with_llm_returns_empty_on_error() -> None:
    """On LLM failure, the filter must not fall back to the full candidate list —
    that defeats the whole point of filtering."""
    from pipeline.aggregate import filter_active_events_with_llm

    class FailingClient:
        model = "fake"

        def generate_json(self, **kwargs):
            raise RuntimeError("simulated LLM failure")

    articles = [
        ArticleForAggregation(
            article_id="a",
            source_id="s",
            source_name="S",
            headline="Something",
            summary=None,
            published_at=None,
            article_path="a.json",
        )
    ]
    active_events = [
        {"event_id": "e1", "title": "T1", "category": "world"},
        {"event_id": "e2", "title": "T2", "category": "world"},
    ]
    result = filter_active_events_with_llm(
        articles=articles, active_events=active_events, client=FailingClient()
    )
    assert result == []


def test_active_event_candidate_filter_requires_two_word_overlap() -> None:
    """The pre-LLM candidate filter applied inside aggregate_once should require >=2
    non-stopword overlaps with an *individual* headline (or 1 if the event title has
    a single non-stopword), not just any overlap with the union of all headlines."""
    # We exercise the same logic the per-group block uses, in isolation.
    from pipeline.aggregate import _KEYWORD_STOPWORDS

    headlines = [
        "Cuba receives China rice shipment amid US threats",
        "Pakistan train blast kills several",
    ]
    article_word_sets = [
        set(re.findall(r"[A-Za-z0-9]+", h.lower())) - _KEYWORD_STOPWORDS for h in headlines
    ]

    def matches(event_title: str) -> bool:
        title_words = set(re.findall(r"[A-Za-z0-9]+", event_title.lower())) - _KEYWORD_STOPWORDS
        if not title_words:
            return False
        required = 1 if len(title_words) == 1 else 2
        return any(
            len(title_words & art_words) >= required for art_words in article_word_sets
        )

    # Directly related event: shares >=2 words with Cuba headline.
    assert matches("Cuba receives China rice shipment")
    # Pakistan-related event: shares >=2 words with Pakistan headline.
    assert matches("Pakistan train blast latest")
    # Spurious one-word overlap with union ("US") must NOT match (old buggy behavior).
    assert not matches("US economy slumps in May")
    # Single-word event title is allowed to match on 1 word (special case).
    assert matches("Pakistan")
    # Empty/stopwords-only title doesn't match.
    assert not matches("the and of")


def test_dynamic_keyword_stopwords_marks_hot_words_above_threshold() -> None:
    from pipeline.aggregate import _dynamic_keyword_stopwords

    events = [
        ("e1", ["trump", "iran", "deal", "abraham"]),
        ("e2", ["trump", "iran", "tehran", "abraham"]),
        ("e3", ["trump", "iran", "strait", "hormuz"]),
        ("e4", ["trump", "graham", "abraham"]),
        ("e5", ["trump", "lapid", "israel"]),
        ("e6", ["trump", "wapo", "editorial"]),
        ("e7", ["trump", "pelosi", "speech"]),
        ("e8", ["trump", "campaign", "donation"]),
    ]
    stopwords = _dynamic_keyword_stopwords(events, threshold=0.2)
    # trump appears in 8/8 events (well above absolute_floor=4) → stopworded.
    assert "trump" in stopwords
    # distinctive words only appearing once should not be stopworded
    assert "tehran" not in stopwords
    assert "hormuz" not in stopwords
    assert "lapid" not in stopwords


def test_dynamic_keyword_stopwords_skips_small_batches() -> None:
    from pipeline.aggregate import _dynamic_keyword_stopwords

    # Below min_events guard — even a word appearing in every event isn't stopworded.
    events = [
        ("e1", ["ferrari", "luce"]),
        ("e2", ["ferrari", "ive"]),
    ]
    assert _dynamic_keyword_stopwords(events) == set()


def test_dynamic_keyword_stopwords_respects_absolute_floor() -> None:
    """A distinctive entity that appears in only 2 events of a 10-event batch must NOT
    be stopworded just because 2 is above the 20% threshold — the absolute_floor of 4
    keeps it visible so the keyword-overlap gate can match it."""
    from pipeline.aggregate import _dynamic_keyword_stopwords

    # 10 events, "ferrari" in 2 of them — 20% of batch.
    events = [
        ("ferrari-1", ["ferrari", "luce", "ev"]),
        ("ferrari-2", ["ferrari", "luce", "ive"]),
        ("e3", ["disney", "marvel"]),
        ("e4", ["netflix", "queue"]),
        ("e5", ["spotify", "music"]),
        ("e6", ["youtube", "video"]),
        ("e7", ["hbo", "show"]),
        ("e8", ["paramount", "film"]),
        ("e9", ["apple", "tv"]),
        ("e10", ["roku", "stick"]),
    ]
    stopwords = _dynamic_keyword_stopwords(events, threshold=0.2)
    assert "ferrari" not in stopwords
    assert "luce" not in stopwords


def test_filtered_event_keywords_drops_static_and_dynamic_stopwords() -> None:
    from pipeline.aggregate import _filtered_event_keywords

    keywords = ["the", "trump", "ferrari", "luce", "ive", "jony", "ev", "electric"]
    dynamic = {"trump"}
    # Static stopword "the" + dynamic "trump" stripped; rest kept up to max_n=6.
    filtered = _filtered_event_keywords(keywords, dynamic, max_n=6)
    assert filtered == ["ferrari", "luce", "ive", "jony", "ev", "electric"]


def test_filtered_event_keywords_deduplicates_case_insensitive() -> None:
    from pipeline.aggregate import _filtered_event_keywords

    keywords = ["Ferrari", "ferrari", "FERRARI", "Luce"]
    filtered = _filtered_event_keywords(keywords, set(), max_n=6)
    assert filtered == ["ferrari", "luce"]


def test_keyword_overlap_candidates_pairs_events_with_shared_distinctive_keywords() -> None:
    from pipeline.aggregate import _keyword_overlap_candidates

    events = [
        {"event_id": "ferrari-1", "keywords": ["ferrari", "luce", "ev", "electric"]},
        {"event_id": "ferrari-2", "keywords": ["ferrari", "luce", "ive", "jony"]},
        {"event_id": "unrelated", "keywords": ["pope", "encyclical", "ai"]},
    ]
    pairs = _keyword_overlap_candidates(events, set(), min_overlap=2)
    assert ("ferrari-1", "ferrari-2") in pairs
    # No spurious pair with the unrelated event.
    assert all("unrelated" not in p for p in pairs)


def test_keyword_overlap_candidates_respects_dynamic_stopwords() -> None:
    from pipeline.aggregate import _keyword_overlap_candidates

    # Both events share "trump"+"iran" but those are stopworded; only "tehran"
    # is shared distinctive, which is below min_overlap=2.
    events = [
        {"event_id": "e1", "keywords": ["trump", "iran", "tehran", "deal"]},
        {"event_id": "e2", "keywords": ["trump", "iran", "tehran", "abraham"]},
    ]
    dynamic = {"trump", "iran"}
    pairs = _keyword_overlap_candidates(events, dynamic, min_overlap=2)
    assert pairs == []


def test_llm_prescreen_candidates_returns_pairs_from_model_payload() -> None:
    from pipeline.aggregate import _llm_prescreen_candidates

    events = [
        {
            "event_id": "ferrari-1",
            "title": "Ferrari Goes Electric: The Luce",
            "keywords": ["ferrari", "luce", "ev"],
        },
        {
            "event_id": "ferrari-2",
            "title": "Ferrari reveals first EV with Jony Ive",
            "keywords": ["ferrari", "luce", "ive"],
        },
        {
            "event_id": "pope",
            "title": "Pope Leo encyclical on AI",
            "keywords": ["pope", "ai", "encyclical"],
        },
    ]
    headlines = {
        "ferrari-1": ["Ferrari Goes Electric: The Luce Is Here!"],
        "ferrari-2": ["Ferrari reveals its first EV"],
        "pope": ["Pope Leo takes aim at big tech in AI encyclical"],
    }
    client = FakeJsonGenerator({
        "candidate_pairs": [
            {"event_a": "ferrari-1", "event_b": "ferrari-2", "reason": "Same Ferrari Luce launch"},
            # Invalid pair (unknown id) — should be dropped silently.
            {"event_a": "ferrari-1", "event_b": "bogus", "reason": "should drop"},
            # Self-pair — should be dropped.
            {"event_a": "pope", "event_b": "pope", "reason": "self"},
        ]
    })

    pairs = _llm_prescreen_candidates(
        events,
        headlines,
        set(),
        client=client,
        batch_label="leisure",
    )
    assert pairs == [("ferrari-1", "ferrari-2")]
    # Prompt should include event ids and headlines for the LLM to compare.
    prompt = client.prompts[0]
    assert "ferrari-1" in prompt and "ferrari-2" in prompt
    assert "Ferrari Goes Electric" in prompt
    assert "Jony Ive" in prompt


def test_llm_prescreen_candidates_short_circuits_on_too_few_events() -> None:
    from pipeline.aggregate import _llm_prescreen_candidates

    client = FakeJsonGenerator({"candidate_pairs": []})
    pairs = _llm_prescreen_candidates(
        [{"event_id": "only-one", "title": "Only one", "keywords": []}],
        {},
        set(),
        client=client,
        batch_label="leisure",
    )
    assert pairs == []
    # LLM must not be invoked for a single-event batch.
    assert client.prompts == []


def test_llm_prescreen_candidates_handles_llm_failure_gracefully() -> None:
    from pipeline.aggregate import _llm_prescreen_candidates

    client = FailingJsonGenerator()
    progress_msgs: list[str] = []

    pairs = _llm_prescreen_candidates(
        [
            {"event_id": "a", "title": "A", "keywords": []},
            {"event_id": "b", "title": "B", "keywords": []},
        ],
        {},
        set(),
        client=client,
        batch_label="leisure",
        progress=progress_msgs.append,
    )
    assert pairs == []
    assert any("prescreen[leisure] failed" in m for m in progress_msgs)


def test_deduplicate_active_events_llm_uses_keyword_overlap_gate(tmp_path, monkeypatch) -> None:
    """Two events that share distinctive keywords but no title-word overlap should be paired
    by the keyword-overlap gate and then merged by the strict per-pair merge LLM."""
    from pipeline.aggregate import deduplicate_active_events_llm

    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    state = StateDB(db_path)

    event_dir = tmp_path / "events"
    event_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("pipeline.aggregate.EVENT_DIR", event_dir)

    # Two events about the same Ferrari Luce launch with no title-word overlap.
    event1 = {
        "event_id": "2026-05-25-ferrari-goes-electric-luce",
        "title": "Ferrari Goes Electric: The Luce Is Here!",
        "category": "automotive",
        "created_at": "2026-05-25T10:00:00Z",
        "updated_at": "2026-05-25T10:00:00Z",
        "article_ids": ["art1"],
        "article_count": 1,
        "keywords": ["ferrari", "luce", "electric", "ev", "polarizing"],
    }
    event2 = {
        "event_id": "2026-05-25-reveals-first-design-help-jony-ive",
        "title": "Reveals first EV with design help from Jony Ive",
        "category": "automotive",
        "created_at": "2026-05-25T11:00:00Z",
        "updated_at": "2026-05-25T11:00:00Z",
        "article_ids": ["art2"],
        "article_count": 1,
        "keywords": ["ferrari", "luce", "ive", "jony", "ev"],
    }
    e1_path = event_dir / f"{event1['event_id']}.json"
    e2_path = event_dir / f"{event2['event_id']}.json"
    e1_path.write_text(json.dumps(event1), encoding="utf-8")
    e2_path.write_text(json.dumps(event2), encoding="utf-8")
    state.upsert_event(event1, e1_path)
    state.upsert_event(event2, e2_path)

    state.insert_article({
        "article_id": "art1",
        "source_id": "car-and-driver",
        "source_name": "Car and Driver",
        "url": "https://example.com/1",
        "headline": "Ferrari Goes Electric: The Luce Is Here!",
        "summary": "Ferrari Luce EV details.",
        "published_at": "2026-05-25T10:00:00Z",
        "publish_date_estimated": False,
        "fetched_at": "2026-05-25T10:00:00Z",
        "content_type": "unknown",
        "language": "en",
        "collection": {},
        "fingerprints": {},
    }, tmp_path / "art1.json")
    state.insert_article({
        "article_id": "art2",
        "source_id": "the-verge",
        "source_name": "The Verge",
        "url": "https://example.com/2",
        "headline": "Ferrari reveals its first EV, with design help from Jony Ive",
        "summary": "Ferrari unveils Luce with Jony Ive design.",
        "published_at": "2026-05-25T11:00:00Z",
        "publish_date_estimated": False,
        "fetched_at": "2026-05-25T11:00:00Z",
        "content_type": "unknown",
        "language": "en",
        "collection": {},
        "fingerprints": {},
    }, tmp_path / "art2.json")
    state.conn.execute("UPDATE articles SET event_id = ? WHERE article_id = 'art1'", (event1["event_id"],))
    state.conn.execute("UPDATE articles SET event_id = ? WHERE article_id = 'art2'", (event2["event_id"],))
    state.conn.commit()

    class PrescreenAndMergeClient:
        model = "fake-model"

        def __init__(self) -> None:
            self.prompts: list[str] = []

        def generate_json(self, **kwargs: Any) -> GeminiResult:
            prompt = kwargs["prompt"]
            self.prompts.append(prompt)
            if "candidate_pairs" in prompt:
                # Prescreen call — return empty so we know the merge happened via the
                # deterministic keyword-overlap gate, not via LLM prescreen.
                return GeminiResult(
                    payload={"candidate_pairs": []},
                    model=self.model,
                    elapsed_ms=5,
                    usage={"promptTokenCount": 20, "candidatesTokenCount": 5},
                )
            # Per-pair merge call.
            return GeminiResult(
                payload={"should_merge": True, "confidence": 0.95, "rationale": "Same Ferrari Luce launch."},
                model=self.model,
                elapsed_ms=10,
                usage={"promptTokenCount": 40, "candidatesTokenCount": 10},
            )

    client = PrescreenAndMergeClient()
    deduplicate_active_events_llm(
        state=state,
        client=client,
        feeds_by_source={},
    )

    # Loser event file should be gone; winner should remain.
    remaining = sorted(p.name for p in event_dir.glob("*.json"))
    assert len(remaining) == 1
    # Both articles should now belong to the surviving event.
    row = state.conn.execute(
        "SELECT COUNT(*) FROM articles WHERE event_id = ?",
        (remaining[0].replace(".json", ""),),
    ).fetchone()
    assert row[0] == 2


def test_deduplicate_active_events_llm_uses_prescreen_when_keywords_miss(tmp_path, monkeypatch) -> None:
    """When keyword overlap is below threshold, the LLM prescreen should still surface
    the candidate pair and feed it to the strict per-pair merge call."""
    from pipeline.aggregate import deduplicate_active_events_llm

    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    state = StateDB(db_path)

    event_dir = tmp_path / "events"
    event_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("pipeline.aggregate.EVENT_DIR", event_dir)

    # Two events with only 1 shared keyword — keyword gate (min_overlap=2) won't match,
    # but LLM prescreen should surface them.
    event1 = {
        "event_id": "2026-05-25-event-one",
        "title": "First framing of the news",
        "category": "technology",
        "created_at": "2026-05-25T10:00:00Z",
        "updated_at": "2026-05-25T10:00:00Z",
        "article_ids": ["art1"],
        "article_count": 1,
        "keywords": ["something", "alpha", "framing"],
    }
    event2 = {
        "event_id": "2026-05-25-event-two",
        "title": "Second framing of the news",
        "category": "technology",
        "created_at": "2026-05-25T11:00:00Z",
        "updated_at": "2026-05-25T11:00:00Z",
        "article_ids": ["art2"],
        "article_count": 1,
        "keywords": ["something", "beta", "different"],
    }
    e1_path = event_dir / f"{event1['event_id']}.json"
    e2_path = event_dir / f"{event2['event_id']}.json"
    e1_path.write_text(json.dumps(event1), encoding="utf-8")
    e2_path.write_text(json.dumps(event2), encoding="utf-8")
    state.upsert_event(event1, e1_path)
    state.upsert_event(event2, e2_path)

    state.insert_article({
        "article_id": "art1",
        "source_id": "src",
        "source_name": "Src",
        "url": "https://example.com/1",
        "headline": "First framing of the news",
        "summary": "Summary",
        "published_at": "2026-05-25T10:00:00Z",
        "publish_date_estimated": False,
        "fetched_at": "2026-05-25T10:00:00Z",
        "content_type": "unknown",
        "language": "en",
        "collection": {},
        "fingerprints": {},
    }, tmp_path / "art1.json")
    state.insert_article({
        "article_id": "art2",
        "source_id": "src",
        "source_name": "Src",
        "url": "https://example.com/2",
        "headline": "Second framing of the news",
        "summary": "Summary",
        "published_at": "2026-05-25T11:00:00Z",
        "publish_date_estimated": False,
        "fetched_at": "2026-05-25T11:00:00Z",
        "content_type": "unknown",
        "language": "en",
        "collection": {},
        "fingerprints": {},
    }, tmp_path / "art2.json")
    state.conn.execute("UPDATE articles SET event_id = ? WHERE article_id = 'art1'", (event1["event_id"],))
    state.conn.execute("UPDATE articles SET event_id = ? WHERE article_id = 'art2'", (event2["event_id"],))
    state.conn.commit()

    prescreen_calls: list[str] = []
    merge_calls: list[str] = []

    class Client:
        model = "fake-model"

        def generate_json(self, **kwargs: Any) -> GeminiResult:
            prompt = kwargs["prompt"]
            if "candidate_pairs" in prompt:
                prescreen_calls.append(prompt)
                return GeminiResult(
                    payload={
                        "candidate_pairs": [
                            {
                                "event_a": event1["event_id"],
                                "event_b": event2["event_id"],
                                "reason": "Same underlying story, different framing.",
                            }
                        ]
                    },
                    model=self.model,
                    elapsed_ms=5,
                    usage={"promptTokenCount": 20, "candidatesTokenCount": 5},
                )
            merge_calls.append(prompt)
            return GeminiResult(
                payload={"should_merge": True, "confidence": 0.95, "rationale": "Same story."},
                model=self.model,
                elapsed_ms=10,
                usage={"promptTokenCount": 40, "candidatesTokenCount": 10},
            )

    deduplicate_active_events_llm(
        state=state,
        client=Client(),
        feeds_by_source={},
    )

    assert len(prescreen_calls) == 1  # one prescreen per category-group batch
    assert len(merge_calls) == 1  # the prescreen surfaced exactly one pair
    remaining = sorted(p.name for p in event_dir.glob("*.json"))
    assert len(remaining) == 1
