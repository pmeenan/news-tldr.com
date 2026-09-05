from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from pipeline.operations import health_report, llm_usage_report, validate_artifacts
from pipeline.state import StateDB, migrate


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _valid_fixture(tmp_path: Path) -> dict[str, Path]:
    config_dir = tmp_path / "config"
    article_dir = tmp_path / "articles"
    event_dir = tmp_path / "events"
    story_dir = tmp_path / "stories"
    dist_dir = tmp_path / "dist"
    db_path = tmp_path / "pipeline.db"
    article_id = "article-1"
    event_id = "2026-08-24-test-event"
    css_asset = "assets/site.1111111111111111.css"
    js_asset = "assets/site.2222222222222222.js"
    theme_asset = "assets/theme.3333333333333333.js"

    _write_json(
        config_dir / "categories.json",
        {"categories": [{"id": "world", "name": "World", "sort_order": 1}]},
    )
    _write_json(
        config_dir / "feeds.json",
        {"feeds": [{"source_id": "source-1"}]},
    )
    _write_json(
        config_dir / "source-policy.json",
        {"sources": [{"source_id": "source-1"}]},
    )
    _write_json(config_dir / "pipeline.json", {"version": 1})
    _write_json(
        article_dir / f"{article_id}.json",
        {
            "article_id": article_id,
            "source_id": "source-1",
            "source_name": "Source One",
            "headline": "A valid article",
            "url": "https://example.test/article",
            "canonical_url": "https://example.test/article",
            "published_at": "2026-08-24T00:00:00Z",
            "fetched_at": "2026-08-24T00:01:00Z",
            "llm_digest": {
                "summary": "Summary",
                "key_facts": ["Fact"],
                "model": "gemini-test",
                "prompt_version": "digest-v1",
                "generated_at": "2026-08-24T00:02:00Z",
            },
        },
    )
    _write_json(
        event_dir / f"{event_id}.json",
        {
            "event_id": event_id,
            "title": "A valid event",
            "category": "world",
            "status": "active",
            "article_ids": [article_id],
            "article_count": 1,
            "llm_metadata": {"stage": "aggregation", "prompt_version": "aggregation-v1"},
            "newsworthiness": {
                "model": "deterministic",
                "prompt_version": "newsworthiness-v1",
            },
        },
    )
    story = {
        "story_id": event_id,
        "event_id": event_id,
        "category": "world",
        "headline": "A valid story",
        "dek": "A valid dek",
        "tldr": ["A summary"],
        "key_facts": [{"text": "Fact", "source_article_ids": [article_id]}],
        "uncertainties": [],
        "sources": [
            {
                "article_id": article_id,
                "source_name": "Source One",
                "headline": "A valid article",
                "url": "https://example.test/article",
            }
        ],
        "created_at": "2026-08-24T00:03:00Z",
        "updated_at": "2026-08-24T00:03:00Z",
        "llm_metadata": {
            "model": "gemini-test",
            "prompt_version": "editorial-v1",
            "generated_at": "2026-08-24T00:03:00Z",
            "event_updated_at": "2026-08-24T00:02:00Z",
        },
    }
    _write_json(story_dir / f"{event_id}.json", story)
    _write_json(
        tmp_path / "active-stories.json",
        {
            "stories": [
                {
                    "story_id": event_id,
                    "category": "world",
                    "importance_score": 0.8,
                }
            ]
        },
    )
    for relative in (
        "index.html",
        "favicon.ico",
        "archive/index.html",
        css_asset,
        js_asset,
        theme_asset,
        "assets/social-card.png",
        "robots.txt",
        "sitemap.xml",
        f"stories/{event_id}/index.html",
    ):
        path = dist_dir / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.suffix == ".png":
            path.write_bytes(b"png")
        elif relative == "index.html":
            path.write_text(
                f'<script src="/{theme_asset}"></script>'
                f'<link rel="stylesheet" href="/{css_asset}">'
                f'<script src="/{js_asset}" defer></script>',
                encoding="utf-8",
            )
        else:
            path.write_text("ok", encoding="utf-8")
    _write_json(dist_dir / "api" / "active-stories.json", {"stories": []})
    _write_json(dist_dir / "api" / "stories" / f"{event_id}.json", story)
    migrate(db_path)
    return {
        "article_dir": article_dir,
        "event_dir": event_dir,
        "story_dir": story_dir,
        "active_stories_path": tmp_path / "active-stories.json",
        "config_dir": config_dir,
        "db_path": db_path,
        "dist_dir": dist_dir,
    }


def test_validate_artifacts_accepts_complete_fixture(tmp_path: Path) -> None:
    stats = validate_artifacts(**_valid_fixture(tmp_path))

    assert stats["valid"] is True
    assert stats["errors_total"] == 0
    assert stats["counts"] == {
        "articles": 1,
        "article_digests": 1,
        "events": 1,
        "stories": 1,
        "active_index_stories": 1,
        "static_files": 12,
        "categories": 1,
    }


def test_validate_artifacts_reports_missing_prompt_metadata(tmp_path: Path) -> None:
    paths = _valid_fixture(tmp_path)
    article_path = paths["article_dir"] / "article-1.json"
    article = json.loads(article_path.read_text(encoding="utf-8"))
    del article["llm_digest"]["prompt_version"]
    _write_json(article_path, article)

    stats = validate_artifacts(**paths)

    assert stats["valid"] is False
    assert any("llm_digest.prompt_version" in item["error"] for item in stats["errors"])


def test_llm_usage_report_groups_tokens_by_run(tmp_path: Path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    with StateDB(db_path) as state:
        state.record_llm_usage("run-1", "editorial", "model-a", "prompt-v1", 100, 20, 0.5)
        state.record_llm_usage("run-1", "editorial", "model-a", "prompt-v1", 50, 10, 0.25)

    report = llm_usage_report(
        db_path=db_path,
        hours=24,
        now=datetime.now(UTC),
    )

    assert report["calls"] == 2
    assert report["input_tokens"] == 150
    assert report["output_tokens"] == 30
    assert report["groups"][0]["run_count"] == 1


def test_health_report_accepts_recent_successful_stage_runs(tmp_path: Path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    with StateDB(db_path) as state:
        for stage in (
            "maintenance",
            "collection",
            "article_digest",
            "aggregation",
            "editorial",
            "presentation",
        ):
            run_id = f"run-{stage}"
            state.start_run(run_id, stage)
            state.finish_run(run_id, "success", {})

    report = health_report(
        db_path=db_path,
        check_live_site=False,
        validate=False,
        now=datetime.now(UTC),
    )

    assert report["status"] == "healthy"
    assert all(check["ok"] for check in report["checks"])


def test_llm_usage_report_includes_tier_thinking_and_cache_totals(tmp_path: Path) -> None:
    db_path = tmp_path / "pipeline.db"
    migrate(db_path)
    with StateDB(db_path) as state:
        state.record_llm_usage("r", "editorial", "m", "v", input_tokens=10, output_tokens=2,
                               thinking_tokens=5, cached_tokens=1, service_tier="flex", cost_usd=0.5)
        state.record_llm_usage("r", "editorial", "m", "v", input_tokens=10, output_tokens=2, cost_usd=0.25)
    report = llm_usage_report(db_path=db_path, hours=24)
    assert report["thinking_tokens"] == 5 and report["cached_tokens"] == 1
    assert report["flex_calls"] == 1 and report["cost_usd"] == 0.75
    assert report["groups"][0]["flex_calls"] == 1 and report["groups"][0]["thinking_tokens"] == 5
