from __future__ import annotations

import json
from typing import Any

import pytest

from pipeline.digest import (
    ArticleForDigest,
    digest_articles_for_aggregation,
    generate_article_digest,
    validate_digest_response,
)
from pipeline.llm import GeminiResult
from pipeline.state import StateDB, migrate


class FakeJsonGenerator:
    model = "fake-model"

    def __init__(self, payload: dict[str, Any] | None = None) -> None:
        self.prompts: list[str] = []
        self.payload = payload

    def generate_json(self, **kwargs: Any) -> GeminiResult:
        self.prompts.append(kwargs["prompt"])
        return GeminiResult(
            payload=self.payload
            or {
                "summary": (
                    "The article reports the core facts, names the people involved, and preserves "
                    "the key numbers without copying page boilerplate."
                ),
                "key_facts": ["One key fact", "Another key fact"],
                "content_quality": "ok",
                "impact": {
                    "global": 0.7,
                    "category": 0.9,
                    "scope": "national",
                    "novelty": "breaking",
                    "rationale_codes": ["public_safety"],
                },
            },
            model=self.model,
            elapsed_ms=34,
            usage={"promptTokenCount": 100, "candidatesTokenCount": 30},
        )


def _article(
    article_id: str,
    *,
    summary: str,
    content: str,
    published_at: str = "2026-05-24T10:00:00Z",
) -> dict[str, Any]:
    return {
        "article_id": article_id,
        "source_id": "source-a",
        "source_name": "Source A",
        "url": f"https://example.com/{article_id}",
        "canonical_url": f"https://example.com/{article_id}",
        "guid": article_id,
        "headline": f"Headline {article_id}",
        "summary": summary,
        "content_text": content,
        "published_at": published_at,
        "fetched_at": "2026-05-24T10:05:00Z",
        "collection": {},
    }


def test_validate_digest_response_accepts_clean_payload() -> None:
    digest = validate_digest_response(
        {
            "summary": "A factual summary.",
            "key_facts": [" Fact one. ", "Fact two."],
            "content_quality": "ok",
            "impact": {
                "global": 0.2,
                "category": 0.4,
                "scope": "niche",
                "novelty": "evergreen",
                "rationale_codes": ["low_public_impact"],
            },
        }
    )

    assert digest == {
        "summary": "A factual summary.",
        "key_facts": ["Fact one.", "Fact two."],
        "content_quality": "ok",
        "impact": {
            "global": 0.2,
            "category": 0.4,
            "scope": "niche",
            "novelty": "evergreen",
            "rationale_codes": ["low_public_impact"],
        },
    }


def test_validate_digest_response_normalizes_common_impact_scales() -> None:
    digest = validate_digest_response(
        {
            "summary": "A factual summary.",
            "key_facts": ["Fact one."],
            "content_quality": "ok",
            "impact": {
                "global": 7,
                "category": 85,
                "scope": "national",
                "novelty": "breaking",
                "rationale_codes": ["Public Safety"],
            },
        }
    )

    assert digest["impact"]["global"] == 0.7
    assert digest["impact"]["category"] == 0.85
    assert digest["impact"]["rationale_codes"] == ["public_safety"]


def test_validate_digest_response_caps_non_news_impact() -> None:
    digest = validate_digest_response(
        {
            "summary": "A promotional product roundup.",
            "key_facts": ["It recommends products."],
            "content_quality": "non_news",
            "impact": {
                "global": 1.0,
                "category": 1.0,
                "scope": "niche",
                "novelty": "evergreen",
                "rationale_codes": ["low_public_interest", "product_recommendation"],
            },
        }
    )

    assert digest["impact"]["global"] == 0.1
    assert digest["impact"]["category"] == 0.1
    assert "impact_capped" in digest["impact"]["rationale_codes"]


def test_validate_digest_response_caps_promotional_impact() -> None:
    digest = validate_digest_response(
        {
            "summary": "A sports betting advice page.",
            "key_facts": ["It lists picks and odds."],
            "content_quality": "ok",
            "impact": {
                "global": 0.8,
                "category": 0.9,
                "scope": "niche",
                "novelty": "evergreen",
                "rationale_codes": ["gambling_advice"],
            },
        }
    )

    assert digest["impact"]["global"] == 0.15
    assert digest["impact"]["category"] == 0.15


def test_validate_digest_response_caps_consumer_review() -> None:
    digest = validate_digest_response(
        {
            "summary": "A how-to guide about a consumer device.",
            "key_facts": ["The guide explains how to set up the device."],
            "content_quality": "ok",
            "impact": {
                "global": 0.7,
                "category": 0.85,
                "scope": "niche",
                "novelty": "evergreen",
                "rationale_codes": ["consumer_review"],
            },
        }
    )

    assert digest["impact"]["global"] == 0.15
    assert digest["impact"]["category"] == 0.15
    assert "impact_capped" in digest["impact"]["rationale_codes"]


@pytest.mark.parametrize(
    "payload,match",
    [
        ({}, "summary"),
        ({"summary": "ok", "key_facts": [], "content_quality": "ok"}, "key_facts"),
        ({"summary": "ok", "key_facts": ["fact"], "content_quality": "bad"}, "content_quality"),
        ({"summary": "ok", "key_facts": ["fact"], "content_quality": "ok"}, "impact"),
    ],
)
def test_validate_digest_response_rejects_bad_payload(payload: dict[str, Any], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        validate_digest_response(payload)


def test_validate_digest_response_caps_paywalled_content() -> None:
    digest = validate_digest_response(
        {
            "summary": "Truncated article body after a paywall.",
            "key_facts": ["Lede paragraph cut off."],
            "content_quality": "paywalled",
            "impact": {
                "global": 0.85,
                "category": 0.95,
                "scope": "national",
                "novelty": "update",
                "rationale_codes": ["low_public_interest"],
            },
        }
    )

    # low_public_interest already caps at 0.15, so cap is min(noise=0.65, promo=0.15) = 0.15
    assert digest["impact"]["global"] == 0.15
    assert digest["impact"]["category"] == 0.15


def test_validate_digest_response_paywalled_noise_cap_without_promo_code() -> None:
    digest = validate_digest_response(
        {
            "summary": "Truncated paywalled feature.",
            "key_facts": ["Body cuts off mid-sentence."],
            "content_quality": "paywalled",
            "impact": {
                "global": 0.85,
                "category": 0.9,
                "scope": "international",
                "novelty": "update",
                "rationale_codes": [],
            },
        }
    )

    assert digest["impact"]["global"] == 0.65
    assert digest["impact"]["category"] == 0.65
    assert "impact_capped" in digest["impact"]["rationale_codes"]


def test_validate_digest_response_paywalled_noise_cap_skipped_for_high_impact() -> None:
    digest = validate_digest_response(
        {
            "summary": "Paywalled wire on a major public-health emergency.",
            "key_facts": ["WHO escalation, hundreds of cases."],
            "content_quality": "paywalled",
            "impact": {
                "global": 0.85,
                "category": 0.95,
                "scope": "international",
                "novelty": "update",
                "rationale_codes": ["public_health"],
            },
        }
    )

    assert digest["impact"]["global"] == 0.85
    assert digest["impact"]["category"] == 0.95


def test_validate_digest_response_caps_vendor_announcement_asymmetric() -> None:
    digest = validate_digest_response(
        {
            "summary": "Acme unveils its new product at its annual keynote.",
            "key_facts": ["Acme announced a new product."],
            "content_quality": "ok",
            "impact": {
                "global": 0.85,
                "category": 0.90,
                "scope": "international",
                "novelty": "update",
                "rationale_codes": ["vendor_announcement"],
            },
        }
    )

    assert digest["impact"]["global"] == 0.55
    assert digest["impact"]["category"] == 0.75
    assert "impact_capped" in digest["impact"]["rationale_codes"]


def test_validate_digest_response_vendor_cap_skipped_for_high_impact() -> None:
    digest = validate_digest_response(
        {
            "summary": "Vendor reports a critical security flaw exploited in the wild.",
            "key_facts": ["Vendor disclosed an actively exploited zero-day."],
            "content_quality": "ok",
            "impact": {
                "global": 0.90,
                "category": 0.95,
                "scope": "international",
                "novelty": "breaking",
                "rationale_codes": ["vendor_announcement", "public_safety"],
            },
        }
    )

    assert digest["impact"]["global"] == 0.9
    assert digest["impact"]["category"] == 0.95
    assert "impact_capped" not in digest["impact"]["rationale_codes"]


def test_validate_digest_response_caps_multi_topic_roundup() -> None:
    digest = validate_digest_response(
        {
            "summary": "Daily news roundup covering several unrelated topics.",
            "key_facts": ["Item one.", "Item two.", "Item three."],
            "content_quality": "extraction_noise",
            "impact": {
                "global": 0.7,
                "category": 0.8,
                "scope": "national",
                "novelty": "update",
                "rationale_codes": ["live_blog"],
            },
        }
    )

    assert digest["impact"]["global"] == 0.30
    assert digest["impact"]["category"] == 0.30


def test_validate_digest_response_caps_unconfirmed_injury_global_only() -> None:
    digest = validate_digest_response(
        {
            "summary": "Star athlete appeared to grab a hamstring during the match.",
            "key_facts": ["Athlete left the match in the 30th minute."],
            "content_quality": "ok",
            "impact": {
                "global": 0.85,
                "category": 0.95,
                "scope": "international",
                "novelty": "breaking",
                "rationale_codes": ["unconfirmed_injury"],
            },
        }
    )

    assert digest["impact"]["global"] == 0.6
    assert digest["impact"]["category"] == 0.95
    assert "impact_capped" in digest["impact"]["rationale_codes"]


def test_validate_digest_response_caps_recycled_content() -> None:
    digest = validate_digest_response(
        {
            "summary": "Reposted feature dated several years ago.",
            "key_facts": ["Article body is dated 2024 but delivered with a 2026 timestamp."],
            "content_quality": "ok",
            "impact": {
                "global": 0.8,
                "category": 0.85,
                "scope": "national",
                "novelty": "update",
                "rationale_codes": ["recycled_content"],
            },
        }
    )

    assert digest["impact"]["global"] == 0.15
    assert digest["impact"]["category"] == 0.15


def test_validate_digest_response_persists_study_stage() -> None:
    digest = validate_digest_response(
        {
            "summary": "A retrospective social-media study on side effects.",
            "key_facts": ["The authors analyzed 70,000 Reddit posts."],
            "content_quality": "ok",
            "study_stage": "observational",
            "impact": {
                "global": 0.4,
                "category": 0.6,
                "scope": "national",
                "novelty": "update",
                "rationale_codes": [],
            },
        }
    )

    assert digest["study_stage"] == "observational"


def test_validate_digest_response_drops_invalid_study_stage() -> None:
    digest = validate_digest_response(
        {
            "summary": "A feature about local food trucks.",
            "key_facts": ["Several trucks opened on Main Street."],
            "content_quality": "ok",
            "study_stage": "made_up_stage",
            "impact": {
                "global": 0.1,
                "category": 0.3,
                "scope": "local",
                "novelty": "low_signal",
                "rationale_codes": [],
            },
        }
    )

    assert "study_stage" not in digest


def test_validate_digest_response_drops_not_applicable_study_stage() -> None:
    digest = validate_digest_response(
        {
            "summary": "A regular news article.",
            "key_facts": ["A thing happened."],
            "content_quality": "ok",
            "study_stage": "not_applicable",
            "impact": {
                "global": 0.5,
                "category": 0.6,
                "scope": "national",
                "novelty": "update",
                "rationale_codes": [],
            },
        }
    )

    assert "study_stage" not in digest


def test_generate_article_digest_builds_variable_length_summary_prompt() -> None:
    client = FakeJsonGenerator()
    article = ArticleForDigest(
        article_id="a1",
        source_id="source-a",
        source_name="Source A",
        headline="A complicated policy story",
        summary="Read more...",
        published_at="2026-05-24T10:00:00Z",
        publish_date_estimated=True,
        url="https://example.com/2024/01/08/politics/gallery/background/index.html",
        canonical_url="https://example.com/2024/01/08/politics/gallery/background/index.html",
        article_path="data/staging/articles/a1.json",
        content_text="Important fact. " * 100,
        selection_reason="feed_boilerplate",
    )

    result = generate_article_digest(article, client=client)

    assert result["digest"]["content_quality"] == "ok"
    assert result["usage"] == {"promptTokenCount": 100, "candidatesTokenCount": 30}
    assert "If a complex article needs more than two sentences" in client.prompts[0]
    assert "Do not recreate the full article" in client.prompts[0]
    assert "Preserve uncertainty" in client.prompts[0]
    assert "standalone, source-grounded editorial notes" in client.prompts[0]
    assert "even if widely known" in client.prompts[0]
    assert "unnamed role, keep it unnamed" in client.prompts[0]
    assert "both impact scores must be low" in client.prompts[0]
    assert "preclinical, animal, early_human" in client.prompts[0]
    assert "vendor_announcement" in client.prompts[0]
    assert "profile_or_background" in client.prompts[0]
    assert "critical_infrastructure" in client.prompts[0]
    assert "consumer_review" in client.prompts[0]
    assert '"publish_date_estimated":true' in client.prompts[0]
    assert (
        '"canonical_url":"https://example.com/2024/01/08/politics/gallery/background/index.html"' in client.prompts[0]
    )
    assert "controlled vocabulary" in client.prompts[0]
    assert "Do not strengthen the source's language" in client.prompts[0]
    assert "published_at value supplied above is metadata only" in client.prompts[0]


def test_generate_article_digest_drops_study_stage_for_uncovered_research() -> None:
    client = FakeJsonGenerator(
        payload={
            "summary": "Researchers reported a marine heatwave finding from ocean observations.",
            "key_facts": ["The article describes ocean temperature research."],
            "content_quality": "ok",
            "study_stage": "observational",
            "impact": {
                "global": 0.4,
                "category": 0.7,
                "scope": "international",
                "novelty": "analysis",
                "rationale_codes": [],
            },
        }
    )
    article = ArticleForDigest(
        article_id="climate-study",
        source_id="science-news",
        source_name="Science News",
        headline="Marine heatwave study finds warming pattern",
        summary="Researchers analyzed ocean temperature records.",
        published_at="2026-05-24T10:00:00Z",
        article_path="data/staging/articles/climate-study.json",
        content_text="Researchers analyzed marine heatwave records and ocean temperatures. " * 80,
        selection_reason="control",
    )

    result = generate_article_digest(article, client=client)

    assert "study_stage" not in result["digest"]


def test_generate_article_digest_drops_study_stage_for_substring_only_matches() -> None:
    client = FakeJsonGenerator(
        payload={
            "summary": "Researchers analyzed constraint decay in backend code generation.",
            "key_facts": ["The paper studies LLM agents used for backend code generation."],
            "content_quality": "ok",
            "study_stage": "lab_bench",
            "impact": {
                "global": 0.2,
                "category": 0.5,
                "scope": "niche",
                "novelty": "analysis",
                "rationale_codes": [],
            },
        }
    )
    article = ArticleForDigest(
        article_id="codegen-study",
        source_id="hacker-news",
        source_name="Hacker News",
        headline="Constraint Decay: The Fragility of LLM Agents in Backend Code Generation",
        summary="The paper studies failures in code generation agents.",
        published_at="2026-05-24T10:00:00Z",
        article_path="data/staging/articles/codegen-study.json",
        content_text=(
            "An international software engineering paper studies backend code generation "
            "and LLM agents under production constraints. "
        )
        * 60,
        selection_reason="control",
    )

    result = generate_article_digest(article, client=client)

    assert "study_stage" not in result["digest"]


def test_generate_article_digest_drops_study_stage_for_excluded_domain_with_weak_health_term() -> None:
    client = FakeJsonGenerator(
        payload={
            "summary": "Researchers analyzed sea level rise and public health risks.",
            "key_facts": ["The article reports climate research on ocean warming."],
            "content_quality": "ok",
            "study_stage": "observational",
            "impact": {
                "global": 0.5,
                "category": 0.7,
                "scope": "international",
                "novelty": "analysis",
                "rationale_codes": ["public_health"],
            },
        }
    )
    article = ArticleForDigest(
        article_id="sea-level-health",
        source_id="science-daily-environment",
        source_name="Science Daily Environment",
        headline="Sea level rise is speeding up and scientists now know why",
        summary="The study links ocean warming to sea level rise and public health risks.",
        published_at="2026-05-24T10:00:00Z",
        article_path="data/staging/articles/sea-level-health.json",
        content_text="Climate researchers analyzed sea level rise, ocean warming, and public health risks. "
        * 80,
        selection_reason="control",
    )

    result = generate_article_digest(article, client=client)

    assert "study_stage" not in result["digest"]


def test_generate_article_digest_keeps_study_stage_for_biomedical_research() -> None:
    client = FakeJsonGenerator(
        payload={
            "summary": "The article reports an animal study of a cancer therapy.",
            "key_facts": ["The study tested a therapy in mice."],
            "content_quality": "ok",
            "study_stage": "animal",
            "impact": {
                "global": 0.5,
                "category": 0.8,
                "scope": "national",
                "novelty": "analysis",
                "rationale_codes": [],
            },
        }
    )
    article = ArticleForDigest(
        article_id="mouse-study",
        source_id="health-news",
        source_name="Health News",
        headline="Cancer therapy study reports mouse results",
        summary="The study tested an experimental treatment in mice.",
        published_at="2026-05-24T10:00:00Z",
        article_path="data/staging/articles/mouse-study.json",
        content_text="The cancer therapy was tested in mice before any human clinical trial. " * 80,
        selection_reason="control",
    )

    result = generate_article_digest(article, client=client)

    assert result["digest"]["study_stage"] == "animal"


def test_generate_article_digest_keeps_study_stage_for_materials_research() -> None:
    client = FakeJsonGenerator(
        payload={
            "summary": "The article reports laboratory findings from lunar material samples.",
            "key_facts": ["Researchers analyzed lunar material in a lab."],
            "content_quality": "ok",
            "study_stage": "lab_bench",
            "impact": {
                "global": 0.35,
                "category": 0.75,
                "scope": "international",
                "novelty": "analysis",
                "rationale_codes": [],
            },
        }
    )
    article = ArticleForDigest(
        article_id="lunar-material",
        source_id="nasa",
        source_name="NASA",
        headline="Researchers analyze lunar material samples",
        summary="Scientists studied lunar samples returned from the moon.",
        published_at="2026-05-24T10:00:00Z",
        article_path="data/staging/articles/lunar-material.json",
        content_text="Scientists analyzed lunar material, minerals, and regolith samples. " * 80,
        selection_reason="control",
    )

    result = generate_article_digest(article, client=client)

    assert result["digest"]["study_stage"] == "lab_bench"


def test_digest_articles_for_aggregation_persists_digest_and_usage(tmp_path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    state = StateDB(db_path)
    article_path = tmp_path / "article.json"
    article = _article(
        "digest-me",
        summary="Read more...",
        content="Important body text with facts and context. " * 80,
    )
    article_path.write_text(json.dumps(article), encoding="utf-8")
    state.insert_article(article, article_path)
    state.start_run("run-digest", "aggregation")

    stats = digest_articles_for_aggregation(
        state=state,
        run_id="run-digest",
        concurrency=1,
        client=FakeJsonGenerator(),
    )

    saved = json.loads(article_path.read_text(encoding="utf-8"))
    row = state.conn.execute(
        "SELECT digest_status, digest_model, digest_prompt_version FROM articles WHERE article_id = ?",
        ("digest-me",),
    ).fetchone()
    usage = state.conn.execute(
        "SELECT stage, input_tokens, output_tokens FROM llm_usage WHERE run_id = 'run-digest'"
    ).fetchone()
    state.finish_run("run-digest", "success", {})
    state.close()

    assert stats["completed"] == 1
    assert saved["llm_digest"]["summary"].startswith("The article reports")
    assert saved["llm_digest"]["key_facts"] == ["One key fact", "Another key fact"]
    assert saved["llm_digest"]["impact"]["global"] == 0.7
    assert row["digest_status"] == "completed"
    assert row["digest_model"] == "fake-model"
    assert row["digest_prompt_version"] == "article-digest-v6"
    assert dict(usage) == {"stage": "article_digest", "input_tokens": 100, "output_tokens": 30}


def test_digest_articles_for_aggregation_skips_existing_digest(tmp_path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    state = StateDB(db_path)
    article_path = tmp_path / "article.json"
    article = _article(
        "already",
        summary="Useful summary with enough context to be a control article.",
        content="Important body text with facts and context. " * 80,
    )
    article["llm_digest"] = {
        "summary": "Existing digest.",
        "key_facts": ["Existing fact."],
        "content_quality": "ok",
        "generated_at": "2026-05-24T10:06:00Z",
        "model": "fake-model",
        "prompt_version": "article-digest-v6",
        "impact": {
            "global": 0.3,
            "category": 0.6,
            "scope": "niche",
            "novelty": "evergreen",
            "rationale_codes": ["low_public_impact"],
        },
    }
    article_path.write_text(json.dumps(article), encoding="utf-8")
    state.insert_article(article, article_path)

    stats = digest_articles_for_aggregation(
        state=state,
        run_id="run-digest",
        concurrency=1,
        client=FakeJsonGenerator(),
    )

    row = state.conn.execute(
        "SELECT digest_status, digest_prompt_version FROM articles WHERE article_id = ?",
        ("already",),
    ).fetchone()
    state.close()

    assert stats["candidates"] == 0
    assert stats["existing_digest"] == 1
    assert row["digest_status"] == "completed"
    assert row["digest_prompt_version"] == "article-digest-v6"


def test_digest_articles_for_aggregation_force_regenerates_existing_digest(tmp_path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    state = StateDB(db_path)
    article_path = tmp_path / "article.json"
    article = _article(
        "force-existing",
        summary="Useful summary with enough context to be a control article.",
        content="Important body text with facts and context. " * 80,
    )
    article["llm_digest"] = {
        "summary": "Old digest.",
        "key_facts": ["Old fact."],
        "content_quality": "ok",
        "generated_at": "2026-05-24T10:06:00Z",
        "model": "old-model",
        "prompt_version": "article-digest-v6",
        "impact": {
            "global": 0.3,
            "category": 0.6,
            "scope": "niche",
            "novelty": "evergreen",
            "rationale_codes": ["low_public_impact"],
        },
    }
    article_path.write_text(json.dumps(article), encoding="utf-8")
    state.insert_article(article, article_path)

    stats = digest_articles_for_aggregation(
        state=state,
        run_id="run-digest",
        concurrency=1,
        force=True,
        client=FakeJsonGenerator(),
    )

    saved = json.loads(article_path.read_text(encoding="utf-8"))
    state.close()

    assert stats["candidates"] == 1
    assert stats["completed"] == 1
    assert stats["existing_digest"] == 0
    assert stats["forced"] is True
    assert saved["llm_digest"]["summary"].startswith("The article reports")
    assert saved["llm_digest"]["key_facts"] == ["One key fact", "Another key fact"]


class FakeFailingJsonGenerator:
    model = "fake-model"

    def generate_json(self, **kwargs: Any) -> GeminiResult:
        raise RuntimeError("API error")


def test_digest_articles_for_aggregation_handles_failure(tmp_path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    state = StateDB(db_path)
    article_path = tmp_path / "article.json"
    article = _article(
        "fail-me",
        summary="Read more...",
        content="Important body text with facts and context. " * 80,
    )
    article_path.write_text(json.dumps(article), encoding="utf-8")
    state.insert_article(article, article_path)
    state.start_run("run-digest-fail", "aggregation")

    stats = digest_articles_for_aggregation(
        state=state,
        run_id="run-digest-fail",
        concurrency=1,
        client=FakeFailingJsonGenerator(),
    )

    row = state.conn.execute(
        "SELECT digest_status, digest_error FROM articles WHERE article_id = ?",
        ("fail-me",),
    ).fetchone()
    err = state.conn.execute("SELECT error_message FROM item_errors WHERE run_id = 'run-digest-fail'").fetchone()
    state.finish_run("run-digest-fail", "success", {})
    state.close()

    assert stats["failed"] == 1
    assert row["digest_status"] == "failed"
    assert "API error" in row["digest_error"]
    assert "API error" in err["error_message"]


def test_digest_articles_for_aggregation_reprint_immediate_copy(tmp_path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    state = StateDB(db_path)

    # Article A: already completed with digest
    path_a = tmp_path / "art_a.json"
    art_a = _article("a", summary="Summary A", content="Body of reprint article.")
    art_a["llm_digest"] = {
        "summary": "Existing digest summary.",
        "key_facts": ["Existing fact."],
        "content_quality": "ok",
        "generated_at": "2026-05-24T10:06:00Z",
        "model": "fake-model",
        "prompt_version": "article-digest-v6",
        "impact": {
            "global": 0.3,
            "category": 0.6,
            "scope": "niche",
            "novelty": "evergreen",
            "rationale_codes": ["low_public_impact"],
        },
    }
    art_a["fingerprints"] = {"content_hash": "hash123"}
    path_a.write_text(json.dumps(art_a), encoding="utf-8")
    state.insert_article(art_a, path_a)
    state.update_article_digest_status("a", status="completed", prompt_version="article-digest-v6")

    # Article B: reprint of A, not completed
    path_b = tmp_path / "art_b.json"
    art_b = _article("b", summary="Summary B", content="Body of reprint article.")
    art_b["fingerprints"] = {"content_hash": "hash123"}
    path_b.write_text(json.dumps(art_b), encoding="utf-8")
    state.insert_article(art_b, path_b)

    state.start_run("run-immediate", "aggregation")
    stats = digest_articles_for_aggregation(
        state=state,
        run_id="run-immediate",
        concurrency=1,
        client=FakeJsonGenerator(),
    )

    row_b = state.conn.execute("SELECT digest_status, digest_model FROM articles WHERE article_id = 'b'").fetchone()
    saved_b = json.loads(path_b.read_text(encoding="utf-8"))

    state.finish_run("run-immediate", "success", {})
    state.close()

    assert stats["candidates"] == 0
    assert stats["reprints_copied_persisted"] == 1
    assert row_b["digest_status"] == "completed"
    assert row_b["digest_model"] == "copied"
    assert saved_b["llm_digest"]["summary"] == "Existing digest summary."


def test_digest_articles_for_aggregation_does_not_copy_stale_prompt_reprint(tmp_path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    state = StateDB(db_path)

    path_a = tmp_path / "art_a.json"
    art_a = _article("a", summary="Summary A", content="Old body.")
    art_a["llm_digest"] = {
        "summary": "Old prompt digest summary.",
        "key_facts": ["Old prompt fact."],
        "content_quality": "ok",
        "generated_at": "2026-05-24T10:06:00Z",
        "model": "fake-model",
        "prompt_version": "article-digest-v1",
        "impact": {
            "global": 0.3,
            "category": 0.6,
            "scope": "niche",
            "novelty": "evergreen",
            "rationale_codes": ["low_public_impact"],
        },
    }
    art_a["fingerprints"] = {"content_hash": "stale-hash"}
    path_a.write_text(json.dumps(art_a), encoding="utf-8")
    state.insert_article(art_a, path_a)
    state.update_article_digest_status("a", status="completed", prompt_version="article-digest-v1")
    state.update_article_aggregation_status("a", status="filtered_low_impact", reason="old digest below threshold")

    path_b = tmp_path / "art_b.json"
    art_b = _article(
        "b",
        summary="Summary B",
        content="Fresh body content for the same exact reprint. " * 80,
    )
    art_b["fingerprints"] = {"content_hash": "stale-hash"}
    path_b.write_text(json.dumps(art_b), encoding="utf-8")
    state.insert_article(art_b, path_b)

    state.start_run("run-stale-copy", "aggregation")
    stats = digest_articles_for_aggregation(
        state=state,
        run_id="run-stale-copy",
        concurrency=1,
        client=FakeJsonGenerator(),
    )

    saved_b = json.loads(path_b.read_text(encoding="utf-8"))
    state.finish_run("run-stale-copy", "success", {})
    state.close()

    assert stats["reprints_copied_persisted"] == 0
    assert stats["completed"] == 1
    assert saved_b["llm_digest"]["prompt_version"] == "article-digest-v6"
    assert saved_b["llm_digest"]["summary"] != "Old prompt digest summary."


def test_digest_articles_for_aggregation_reprint_deferred_copy(tmp_path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    state = StateDB(db_path)

    # Article A: candidate, needs digest
    path_a = tmp_path / "art_a.json"
    art_a = _article("a", summary="Summary A", content="Identical content body. " * 50)
    art_a["fingerprints"] = {"content_hash": "dup_hash"}
    path_a.write_text(json.dumps(art_a), encoding="utf-8")
    state.insert_article(art_a, path_a)

    # Article B: reprint of A in the same batch, needs digest
    path_b = tmp_path / "art_b.json"
    art_b = _article("b", summary="Summary B", content="Identical content body. " * 50)
    art_b["fingerprints"] = {"content_hash": "dup_hash"}
    path_b.write_text(json.dumps(art_b), encoding="utf-8")
    state.insert_article(art_b, path_b)

    state.start_run("run-deferred", "aggregation")
    generator = FakeJsonGenerator()
    stats = digest_articles_for_aggregation(
        state=state,
        run_id="run-deferred",
        concurrency=1,
        client=generator,
    )

    row_a = state.conn.execute("SELECT digest_status, digest_model FROM articles WHERE article_id = 'a'").fetchone()
    row_b = state.conn.execute("SELECT digest_status, digest_model FROM articles WHERE article_id = 'b'").fetchone()

    saved_a = json.loads(path_a.read_text(encoding="utf-8"))
    saved_b = json.loads(path_b.read_text(encoding="utf-8"))

    state.finish_run("run-deferred", "success", {})
    state.close()

    assert stats["candidates"] == 1  # Only one is processed as candidate
    assert stats["completed"] == 1
    assert stats["reprints_copied_in_batch"] == 1
    assert row_a["digest_status"] == "completed"
    assert row_b["digest_status"] == "completed"
    assert {row_a["digest_model"], row_b["digest_model"]} == {"fake-model", "copied"}
    assert saved_b["llm_digest"]["summary"] == saved_a["llm_digest"]["summary"]


def test_digest_articles_for_aggregation_retry_limit_skipped(tmp_path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    state = StateDB(db_path)
    article_path = tmp_path / "article.json"
    article = _article(
        "retry-limit-me",
        summary="Read more...",
        content="Body content text that fails. " * 80,
    )
    article_path.write_text(json.dumps(article), encoding="utf-8")
    state.insert_article(article, article_path)

    # Insert 3 failures
    for i in range(3):
        state.record_error(
            run_id=f"run-{i}",
            stage="article_digest",
            item_type="article",
            item_id="retry-limit-me",
            source_id="source-a",
            error="API rate limit exceeded",
        )

    state.start_run("run-retry-limit", "aggregation")
    stats = digest_articles_for_aggregation(
        state=state,
        run_id="run-retry-limit",
        concurrency=1,
        client=FakeJsonGenerator(),
    )

    row = state.conn.execute(
        "SELECT digest_status, digest_error, aggregation_status, aggregation_reason, is_filtered "
        "FROM articles WHERE article_id = 'retry-limit-me'"
    ).fetchone()
    state.finish_run("run-retry-limit", "success", {})
    state.close()

    assert stats["candidates"] == 0
    assert stats["skipped"] == 1
    assert row["digest_status"] == "skipped"
    assert "skipped: failed 3 times" in row["digest_error"]
    assert row["aggregation_status"] == "filtered_max_retries_exceeded"
    assert "skipped: failed 3 times" in row["aggregation_reason"]
    assert row["is_filtered"] == 1


def test_digest_articles_for_aggregation_does_not_copy_by_headline_hash(tmp_path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    state = StateDB(db_path)

    # Article A: completed digest, only headline_hash fingerprint
    path_a = tmp_path / "art_a.json"
    art_a = _article(
        "a",
        summary="Morning briefing summary A.",
        content="Body content for story A. " * 60,
    )
    art_a["llm_digest"] = {
        "summary": "Digest about story A only.",
        "key_facts": ["A specific fact about story A."],
        "content_quality": "ok",
        "generated_at": "2026-05-24T10:06:00Z",
        "model": "fake-model",
        "prompt_version": "article-digest-v6",
        "impact": {
            "global": 0.3,
            "category": 0.6,
            "scope": "niche",
            "novelty": "evergreen",
            "rationale_codes": ["low_public_impact"],
        },
    }
    art_a["fingerprints"] = {"headline_hash": "shared_headline"}
    path_a.write_text(json.dumps(art_a), encoding="utf-8")
    state.insert_article(art_a, path_a)
    state.update_article_digest_status("a", status="completed", prompt_version="article-digest-v6")

    # Article B: unrelated story, same generic headline_hash, no content/url fingerprint
    path_b = tmp_path / "art_b.json"
    art_b = _article(
        "b",
        summary="Morning briefing summary B.",
        content="Body content for unrelated story B. " * 60,
    )
    art_b["fingerprints"] = {"headline_hash": "shared_headline"}
    path_b.write_text(json.dumps(art_b), encoding="utf-8")
    state.insert_article(art_b, path_b)

    state.start_run("run-headline", "aggregation")
    stats = digest_articles_for_aggregation(
        state=state,
        run_id="run-headline",
        concurrency=1,
        client=FakeJsonGenerator(),
    )

    saved_b = json.loads(path_b.read_text(encoding="utf-8"))
    state.finish_run("run-headline", "success", {})
    state.close()

    # B must have been processed independently, not copied from A
    assert stats["reprints_copied_persisted"] == 0
    assert stats["reprints_copied_in_batch"] == 0
    assert stats["completed"] == 1
    assert saved_b["llm_digest"]["summary"] != "Digest about story A only."


def test_digest_articles_for_aggregation_missing_json_does_not_retry(tmp_path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    state = StateDB(db_path)

    # Insert an article row but never write the JSON to disk.
    article = _article("ghost", summary="Summary", content="Body.")
    state.insert_article(article, tmp_path / "ghost.json")

    state.start_run("run-missing", "aggregation")
    stats = digest_articles_for_aggregation(
        state=state,
        run_id="run-missing",
        concurrency=1,
        client=FakeJsonGenerator(),
    )

    row = state.conn.execute(
        "SELECT digest_status, digest_prompt_version, digest_error, "
        "       aggregation_status, aggregation_reason, is_filtered "
        "FROM articles WHERE article_id = 'ghost'"
    ).fetchone()
    state.finish_run("run-missing", "success", {})

    assert stats["skipped"] == 1
    assert row["digest_status"] == "skipped"
    assert row["digest_prompt_version"] == "article-digest-v6"
    assert "article JSON not found" in row["digest_error"]
    assert row["aggregation_status"] == "filtered_missing_article_json"
    assert "article JSON not found" in row["aggregation_reason"]
    assert row["is_filtered"] == 1

    # Second pass: the row must not be re-selected as a candidate.
    state.start_run("run-missing-2", "aggregation")
    stats2 = digest_articles_for_aggregation(
        state=state,
        run_id="run-missing-2",
        concurrency=1,
        client=FakeJsonGenerator(),
    )
    state.finish_run("run-missing-2", "success", {})
    state.close()

    assert stats2["candidates"] == 0
    assert stats2["skipped"] == 0


def test_digest_articles_for_aggregation_thin_content_skipped(tmp_path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    state = StateDB(db_path)
    article_path = tmp_path / "article.json"
    article = _article(
        "thin-content",
        summary="Summary",
        content="Short body.",  # shorter than 500 chars
    )
    article_path.write_text(json.dumps(article), encoding="utf-8")
    state.insert_article(article, article_path)

    state.start_run("run-thin", "aggregation")
    stats = digest_articles_for_aggregation(
        state=state,
        run_id="run-thin",
        concurrency=1,
        client=FakeJsonGenerator(),
    )

    row = state.conn.execute(
        "SELECT digest_status, digest_error, aggregation_status, aggregation_reason, is_filtered "
        "FROM articles WHERE article_id = 'thin-content'"
    ).fetchone()
    state.finish_run("run-thin", "success", {})
    state.close()

    assert stats["candidates"] == 0
    assert stats["skipped"] == 1
    assert row["digest_status"] == "skipped"
    assert "content_text shorter than 500 chars" in row["digest_error"]
    assert row["aggregation_status"] == "filtered_thin_content"
    assert "content_text shorter than 500 chars" in row["aggregation_reason"]
    assert row["is_filtered"] == 1


def test_digest_articles_for_aggregation_video_url_skipped(tmp_path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    state = StateDB(db_path)

    # Test video/gallery patterns in either the URL or canonical URL.
    for index, (art_id, url, canonical) in enumerate(
        [
            (
                "video-url",
                "https://edition.cnn.com/videos/business/2026/05/crowdstrike-outage-transcript.cnn",
                "https://edition.cnn.com/videos/business/2026/05/crowdstrike-outage-transcript.cnn",
            ),
            (
                "video-singular",
                "https://www.cbsnews.com/video/new-details-on-white-house-shooting/",
                "https://www.cbsnews.com/video/new-details-on-white-house-shooting/",
            ),
            (
                "video-canonical",
                "https://example.com/some-ref",
                "https://edition.cnn.com/videos/tech/news.html",
            ),
            (
                "gallery-url",
                "https://www.cnn.com/2021/01/08/politics/gallery/joe-biden/index.html",
                "https://www.cnn.com/2021/01/08/politics/gallery/joe-biden/index.html",
            ),
        ]
    ):
        article_path = tmp_path / f"art_{index}.json"
        content = "Body text long enough to pass 500 character check. " * 20
        article = _article(art_id, summary="Summary", content=content)
        article["url"] = url
        article["canonical_url"] = canonical
        article_path.write_text(json.dumps(article), encoding="utf-8")
        state.insert_article(article, article_path)

        state.start_run(f"run-video-{index}", "aggregation")
        stats = digest_articles_for_aggregation(
            state=state,
            run_id=f"run-video-{index}",
            concurrency=1,
            client=FakeJsonGenerator(),
        )

        row = state.conn.execute(
            "SELECT digest_status, digest_error, aggregation_status, aggregation_reason, is_filtered "
            "FROM articles WHERE article_id = ?",
            (art_id,),
        ).fetchone()
        state.finish_run(f"run-video-{index}", "success", {})

        assert stats["candidates"] == 0
        assert stats["skipped"] == 1
        assert row["digest_status"] == "skipped"
        assert "skipped: media page URL" in row["digest_error"]
        assert row["aggregation_status"] == "filtered_video_or_carousel"
        assert "detected by URL pattern" in row["aggregation_reason"]
        assert row["is_filtered"] == 1


def test_digest_articles_for_aggregation_stale_estimated_live_page_skipped(tmp_path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    state = StateDB(db_path)
    article_path = tmp_path / "article.json"
    article = _article(
        "stale-live",
        summary="Summary",
        content="Body text long enough to pass the content threshold. " * 20,
        published_at="2026-05-25T10:00:00Z",
    )
    article["publish_date_estimated"] = True
    article["url"] = (
        "https://www.cnn.com/business/live-news/fox-news-dominion-trial-04-18-23/h_8d51e3ae2714edaa0dace837305d03b8"
    )
    article["canonical_url"] = article["url"]
    article_path.write_text(json.dumps(article), encoding="utf-8")
    state.insert_article(article, article_path)

    state.start_run("run-stale-live", "aggregation")
    stats = digest_articles_for_aggregation(
        state=state,
        run_id="run-stale-live",
        concurrency=1,
        client=FakeJsonGenerator(),
    )

    row = state.conn.execute(
        "SELECT digest_status, digest_error, aggregation_status, aggregation_reason, is_filtered "
        "FROM articles WHERE article_id = 'stale-live'"
    ).fetchone()
    state.finish_run("run-stale-live", "success", {})
    state.close()

    assert stats["candidates"] == 0
    assert stats["skipped"] == 1
    assert row["digest_status"] == "skipped"
    assert "stale estimated-date page dated 2023-04-18" in row["digest_error"]
    assert row["aggregation_status"] == "filtered_recycled_content"
    assert "detected before digest generation" in row["aggregation_reason"]
    assert row["is_filtered"] == 1


def test_digest_articles_for_aggregation_current_estimated_article_not_stale(tmp_path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    state = StateDB(db_path)
    article_path = tmp_path / "article.json"
    article = _article(
        "current-estimated",
        summary="Summary",
        content="Body text long enough to pass the content threshold. " * 20,
        published_at="2026-05-25T10:00:00Z",
    )
    article["publish_date_estimated"] = True
    article["url"] = "https://apnews.com/article/current-breaking-news"
    article["canonical_url"] = article["url"]
    article_path.write_text(json.dumps(article), encoding="utf-8")
    state.insert_article(article, article_path)

    stats = digest_articles_for_aggregation(
        state=state,
        run_id="run-current-estimated",
        concurrency=1,
        client=FakeJsonGenerator(),
    )

    row = state.conn.execute(
        "SELECT digest_status, aggregation_status, is_filtered FROM articles WHERE article_id = 'current-estimated'"
    ).fetchone()
    state.close()

    assert stats["candidates"] == 1
    assert stats["completed"] == 1
    assert stats["skipped"] == 0
    assert row["digest_status"] == "completed"
    assert row["aggregation_status"] == "pending"
    assert row["is_filtered"] == 0


def test_digest_force_preserves_assigned_aggregation_status(tmp_path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    state = StateDB(db_path)
    article_path = tmp_path / "article.json"
    article = _article(
        "already-assigned",
        summary="Useful summary with enough context to be a control article.",
        content="Important body text with facts and context. " * 80,
    )
    article["llm_digest"] = {
        "summary": "Old digest.",
        "key_facts": ["Old fact."],
        "content_quality": "ok",
        "generated_at": "2026-05-24T10:06:00Z",
        "model": "old-model",
        "prompt_version": "article-digest-v6",
        "impact": {
            "global": 0.3,
            "category": 0.6,
            "scope": "niche",
            "novelty": "evergreen",
            "rationale_codes": ["low_public_impact"],
        },
    }
    article_path.write_text(json.dumps(article), encoding="utf-8")
    state.insert_article(article, article_path)
    state.upsert_event(
        {
            "event_id": "ev-pre",
            "title": "Pre-existing event",
            "category": "world",
            "thread": None,
            "status": "active",
            "created_at": "2026-05-24T10:06:00Z",
            "updated_at": "2026-05-24T10:06:00Z",
            "article_ids": ["already-assigned"],
            "article_count": 1,
            "confidence": 0.7,
            "newsworthiness": None,
            "llm_metadata": {},
        },
        tmp_path / "events" / "ev-pre.json",
    )
    state.assign_articles_to_event(["already-assigned"], "ev-pre")

    digest_articles_for_aggregation(
        state=state,
        run_id="run-force-assigned",
        concurrency=1,
        force=True,
        client=FakeJsonGenerator(),
    )

    row = state.conn.execute(
        "SELECT aggregation_status, event_id, digest_status FROM articles WHERE article_id = 'already-assigned'"
    ).fetchone()
    state.close()

    # The digest was regenerated, but the article must remain assigned to its
    # existing event — not reset to "pending" with event_id still populated.
    assert row["digest_status"] == "completed"
    assert row["event_id"] == "ev-pre"
    assert row["aggregation_status"] == "assigned"


def test_digest_force_bypasses_max_retries(tmp_path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    state = StateDB(db_path)
    article_path = tmp_path / "article.json"
    article = _article(
        "retry-forced",
        summary="Read more...",
        content="Body content text that previously failed. " * 80,
    )
    article_path.write_text(json.dumps(article), encoding="utf-8")
    state.insert_article(article, article_path)

    for i in range(3):
        state.record_error(
            run_id=f"run-prev-{i}",
            stage="article_digest",
            item_type="article",
            item_id="retry-forced",
            source_id="source-a",
            error="API rate limit exceeded",
        )
    state.update_article_digest_status(
        "retry-forced",
        status="skipped",
        prompt_version="article-digest-v6",
        error="skipped: failed 3 times",
    )
    state.update_article_aggregation_status(
        "retry-forced",
        status="filtered_max_retries_exceeded",
        reason="skipped: failed 3 times",
    )

    stats = digest_articles_for_aggregation(
        state=state,
        run_id="run-force-retry",
        concurrency=1,
        force=True,
        client=FakeJsonGenerator(),
    )

    row = state.conn.execute(
        "SELECT digest_status, aggregation_status, is_filtered FROM articles WHERE article_id = 'retry-forced'"
    ).fetchone()
    state.close()

    assert stats["candidates"] == 1
    assert stats["completed"] == 1
    assert stats["skipped"] == 0
    assert row["digest_status"] == "completed"
    assert row["aggregation_status"] == "pending"
    assert row["is_filtered"] == 0


def test_digest_once_default_range(tmp_path, monkeypatch) -> None:
    from datetime import datetime, time, timedelta

    from pipeline.digest import digest_once
    from pipeline.util import isoformat_z, utc_now

    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    monkeypatch.setattr("pipeline.digest.StateDB", lambda: StateDB(db_path))
    monkeypatch.setattr("pipeline.digest.LOCK_PATH", tmp_path / "pipeline.lock")

    params_passed = {}

    def fake_digest_articles_for_aggregation(*args, **kwargs):
        params_passed.update(kwargs)
        return {"completed": 1}

    monkeypatch.setattr(
        "pipeline.digest.digest_articles_for_aggregation",
        fake_digest_articles_for_aggregation,
    )

    digest_once(client=FakeJsonGenerator())

    ref = utc_now()
    expected_today_start = datetime.combine(ref.date(), time.min, tzinfo=ref.tzinfo)
    expected_prev_day_start = expected_today_start - timedelta(days=1)
    expected_range_start = isoformat_z(expected_prev_day_start)

    assert params_passed.get("published_after") == expected_range_start
    assert params_passed.get("published_before") is None
