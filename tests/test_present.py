from __future__ import annotations

import json
import stat
from datetime import UTC, datetime
from pathlib import Path

import pytest

from pipeline.present import DEPLOY_MANIFEST, build_static_site, deploy_static_site


def _story(story_id: str, *, updated_at: str, headline: str = "A test headline") -> dict:
    article_id = f"article-{story_id}"
    return {
        "story_id": story_id,
        "event_id": story_id,
        "category": "world",
        "thread": None,
        "headline": headline,
        "dek": "A concise explanation of what changed.",
        "tldr": ["The first fact.", "The second fact."],
        "key_facts": [{"text": "A sourced fact.", "source_article_ids": [article_id]}],
        "uncertainties": [
            {"text": "An open question remains.", "source_article_ids": [article_id]}
        ],
        "political_framing": None,
        "sources": [
            {
                "article_id": article_id,
                "source_name": "Associated Press",
                "headline": "Original report",
                "url": "https://example.test/report",
            }
        ],
        "importance": {"score": 0.8, "signals": ["public_impact"]},
        "created_at": updated_at,
        "updated_at": updated_at,
        "llm_metadata": {
            "model": "gemini-3.7-flash",
            "prompt_version": "editorial-v2",
            "generated_at": updated_at,
            "event_updated_at": updated_at,
        },
    }


def _write_published_fixture(
    tmp_path: Path,
    stories: list[dict],
) -> tuple[Path, Path, Path]:
    published_dir = tmp_path / "published"
    story_dir = published_dir / "stories"
    story_dir.mkdir(parents=True)
    rows = []
    for story in stories:
        (story_dir / f"{story['story_id']}.json").write_text(
            json.dumps(story), encoding="utf-8"
        )
        rows.append(
            {
                "story_id": story["story_id"],
                "category": story["category"],
                "headline": story["headline"],
                "importance_score": story["importance"]["score"],
                "source_count": 1,
                "status": "active",
                "event_created_at": story["created_at"],
                "event_updated_at": story["llm_metadata"]["event_updated_at"],
                "created_at": story["created_at"],
                "updated_at": story["updated_at"],
            }
        )
    active_path = published_dir / "active-stories.json"
    active_path.write_text(
        json.dumps({"generated_at": "2026-08-24T00:00:00Z", "stories": rows}),
        encoding="utf-8",
    )
    return published_dir, story_dir, active_path


def test_build_static_site_renders_current_stories_and_keeps_old_detail_pages(tmp_path: Path) -> None:
    current = _story(
        "2026-08-24-current-story",
        updated_at="2026-08-24T00:00:00Z",
        headline="Current <script>alert(1)</script> story",
    )
    current["sources"][0]["url"] = "javascript:alert(1)"
    old = _story("2026-08-18-old-story", updated_at="2026-08-18T00:00:00Z")
    _, story_dir, active_path = _write_published_fixture(tmp_path, [current, old])
    output_dir = tmp_path / "dist"

    stats = build_static_site(
        output_dir=output_dir,
        story_dir=story_dir,
        active_stories_path=active_path,
        site_url="https://news-tldr.com",
        rolling_window_hours=72,
        now=datetime(2026, 8, 24, 1, tzinfo=UTC),
    )

    home = (output_dir / "index.html").read_text(encoding="utf-8")
    detail = (output_dir / "stories" / current["story_id"] / "index.html").read_text(
        encoding="utf-8"
    )
    assert stats["stories_rendered"] == 2
    assert stats["homepage_stories"] == 1
    assert "Current &lt;script&gt;alert(1)&lt;/script&gt; story" in home
    assert "2026-08-18-old-story" not in home
    assert (output_dir / "stories" / old["story_id"] / "index.html").exists()
    assert 'href="#"' in detail
    assert "javascript:alert" not in detail
    assert "Content-Security-Policy" in detail
    assert (output_dir / "api" / "active-stories.json").exists()
    assert (output_dir / "sitemap.xml").exists()


def test_build_static_site_rejects_path_traversal_story_id(tmp_path: Path) -> None:
    story = _story("valid-story", updated_at="2026-08-24T00:00:00Z")
    _, story_dir, active_path = _write_published_fixture(tmp_path, [story])
    payload = json.loads(active_path.read_text(encoding="utf-8"))
    payload["stories"][0]["story_id"] = "../escape"
    active_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="invalid story_id"):
        build_static_site(
            output_dir=tmp_path / "dist",
            story_dir=story_dir,
            active_stories_path=active_path,
            now=datetime(2026, 8, 24, 1, tzinfo=UTC),
        )


def test_deploy_static_site_preserves_unknown_files_and_removes_only_stale_managed_files(
    tmp_path: Path,
) -> None:
    source = tmp_path / "dist"
    (source / "assets").mkdir(parents=True)
    (source / "index.html").write_text("new home", encoding="utf-8")
    (source / "assets" / "site.css").write_text("new css", encoding="utf-8")

    target = tmp_path / "production"
    (target / "old").mkdir(parents=True)
    (target / "old" / "managed.html").write_text("old", encoding="utf-8")
    (target / "server-note.txt").write_text("preserve me", encoding="utf-8")
    (target / DEPLOY_MANIFEST).write_text(
        json.dumps({"files": ["index.html", "old/managed.html"]}),
        encoding="utf-8",
    )

    stats = deploy_static_site(source_dir=source, publish_dir=target.resolve())

    assert stats["published"] is True
    assert stats["stale_files_removed"] == 1
    assert (target / "index.html").read_text(encoding="utf-8") == "new home"
    assert (target / "assets" / "site.css").read_text(encoding="utf-8") == "new css"
    assert not (target / "old" / "managed.html").exists()
    assert (target / "server-note.txt").read_text(encoding="utf-8") == "preserve me"
    mode = stat.S_IMODE((target / "index.html").stat().st_mode)
    assert mode == 0o644
    manifest = json.loads((target / DEPLOY_MANIFEST).read_text(encoding="utf-8"))
    assert manifest["files"] == ["assets/site.css", "index.html"]


def test_deploy_static_site_requires_absolute_safe_destination(tmp_path: Path) -> None:
    source = tmp_path / "dist"
    source.mkdir()
    (source / "index.html").write_text("home", encoding="utf-8")

    with pytest.raises(ValueError, match="absolute"):
        deploy_static_site(source_dir=source, publish_dir=Path("relative-site"))
