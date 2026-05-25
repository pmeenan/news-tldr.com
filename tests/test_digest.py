from __future__ import annotations

import json
from typing import Any

import pytest

from pipeline.digest import (
    ArticleForDigest,
    digest_articles_for_aggregation,
    generate_article_digest,
    run_article_digest_experiment,
    select_digest_experiment_articles,
    validate_digest_response,
)
from pipeline.llm import GeminiResult
from pipeline.state import StateDB, migrate


class FakeJsonGenerator:
    model = "fake-model"

    def __init__(self) -> None:
        self.prompts: list[str] = []

    def generate_json(self, **kwargs: Any) -> GeminiResult:
        self.prompts.append(kwargs["prompt"])
        return GeminiResult(
            payload={
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


def test_generate_article_digest_builds_variable_length_summary_prompt() -> None:
    client = FakeJsonGenerator()
    article = ArticleForDigest(
        article_id="a1",
        source_id="source-a",
        source_name="Source A",
        headline="A complicated policy story",
        summary="Read more...",
        published_at="2026-05-24T10:00:00Z",
        article_path="data/staging/articles/a1.json",
        content_text="Important fact. " * 100,
        selection_reason="feed_boilerplate",
    )

    result = generate_article_digest(article, client=client)

    assert result["digest"]["content_quality"] == "ok"
    assert result["usage"] == {"promptTokenCount": 100, "candidatesTokenCount": 30}
    assert "If a complex article needs more than two sentences" in client.prompts[0]
    assert "Do not recreate the full article" in client.prompts[0]


def test_select_digest_experiment_articles_prefers_low_signal_then_controls(tmp_path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    state = StateDB(db_path)
    low_path = tmp_path / "low.json"
    control_path = tmp_path / "control.json"
    low = _article(
        "low",
        summary="Comments",
        content="Detailed reporting. " * 80,
        published_at="2026-05-24T10:01:00Z",
    )
    control = _article(
        "control",
        summary=(
            "This source-provided summary explains the article's central development, "
            "names the participants, and includes enough factual context for grouping."
        ),
        content="Detailed reporting. " * 80,
    )
    low_path.write_text(json.dumps(low), encoding="utf-8")
    control_path.write_text(json.dumps(control), encoding="utf-8")
    state.insert_article(low, low_path)
    state.insert_article(control, control_path)

    selected = select_digest_experiment_articles(limit=2, control_count=1, db=state)

    state.close()
    assert [article.article_id for article in selected] == ["low", "control"]
    assert selected[0].selection_reason == "very_short_summary"
    assert selected[1].selection_reason == "control"


def test_run_article_digest_experiment_accumulates_usage(tmp_path, monkeypatch) -> None:
    articles = [
        ArticleForDigest(
            article_id="a1",
            source_id="source-a",
            source_name="Source A",
            headline="Headline",
            summary="Comments",
            published_at="2026-05-24T10:00:00Z",
            article_path="data/staging/articles/a1.json",
            content_text="Body " * 200,
            selection_reason="very_short_summary",
        )
    ]
    monkeypatch.setattr("pipeline.digest.select_digest_experiment_articles", lambda **kwargs: articles)
    client = FakeJsonGenerator()

    result = run_article_digest_experiment(limit=1, control_count=0, client=client)

    assert result["article_count"] == 1
    assert result["usage"] == {"promptTokenCount": 100, "candidatesTokenCount": 30}
    assert result["articles"][0]["digest"]["summary"].startswith("The article reports")


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
    assert row["digest_prompt_version"] == "article-digest-v2"
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
        "prompt_version": "article-digest-v2",
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
    assert row["digest_prompt_version"] == "article-digest-v2"


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
    err = state.conn.execute(
        "SELECT error_message FROM item_errors WHERE run_id = 'run-digest-fail'"
    ).fetchone()
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
        "prompt_version": "article-digest-v2",
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
    state.update_article_digest_status(
        "a", status="completed", prompt_version="article-digest-v2"
    )

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

    row_b = state.conn.execute(
        "SELECT digest_status, digest_model FROM articles WHERE article_id = 'b'"
    ).fetchone()
    saved_b = json.loads(path_b.read_text(encoding="utf-8"))

    state.finish_run("run-immediate", "success", {})
    state.close()

    assert stats["candidates"] == 0
    assert stats["reprints_copied_persisted"] == 1
    assert row_b["digest_status"] == "completed"
    assert row_b["digest_model"] == "copied"
    assert saved_b["llm_digest"]["summary"] == "Existing digest summary."


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

    row_a = state.conn.execute(
        "SELECT digest_status, digest_model FROM articles WHERE article_id = 'a'"
    ).fetchone()
    row_b = state.conn.execute(
        "SELECT digest_status, digest_model FROM articles WHERE article_id = 'b'"
    ).fetchone()

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
        "SELECT digest_status, digest_error FROM articles WHERE article_id = 'retry-limit-me'"
    ).fetchone()
    state.finish_run("run-retry-limit", "success", {})
    state.close()

    assert stats["candidates"] == 0
    assert stats["skipped"] == 1
    assert row["digest_status"] == "skipped"
    assert "skipped: failed 3 times" in row["digest_error"]


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
        "prompt_version": "article-digest-v2",
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
    state.update_article_digest_status(
        "a", status="completed", prompt_version="article-digest-v2"
    )

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
        "SELECT digest_status, digest_prompt_version, digest_error "
        "FROM articles WHERE article_id = 'ghost'"
    ).fetchone()
    state.finish_run("run-missing", "success", {})

    assert stats["skipped"] == 1
    assert row["digest_status"] == "skipped"
    assert row["digest_prompt_version"] == "article-digest-v2"
    assert "article JSON not found" in row["digest_error"]

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
