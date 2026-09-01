from __future__ import annotations

import hashlib
import json
import stat
import struct
from datetime import UTC, datetime
from pathlib import Path

import pytest

from pipeline.present import DEPLOY_MANIFEST, build_static_site, deploy_static_site


def _site_asset(output_dir: Path, extension: str) -> Path:
    matches = list((output_dir / "assets").glob(f"site.*.{extension}"))
    assert len(matches) == 1
    return matches[0]


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
        reader_sync_enabled=True,
        now=datetime(2026, 8, 24, 1, tzinfo=UTC),
    )

    home = (output_dir / "index.html").read_text(encoding="utf-8")
    detail = (output_dir / "stories" / current["story_id"] / "index.html").read_text(
        encoding="utf-8"
    )
    css_asset = _site_asset(output_dir, "css")
    js_asset = _site_asset(output_dir, "js")
    assert stats["stories_rendered"] == 2
    assert stats["homepage_stories"] == 1
    assert "Current &lt;script&gt;alert(1)&lt;/script&gt; story" in home
    assert "2026-08-18-old-story" not in home
    assert (output_dir / "stories" / old["story_id"] / "index.html").exists()
    assert 'href="#"' in detail
    assert "javascript:alert" not in detail
    assert "Content-Security-Policy" in detail
    assert "connect-src 'self'" in home
    assert '<meta name="referrer" content="no-referrer">' in home
    assert '<meta name="robots" content="noindex,follow,noarchive,max-image-preview:large">' in detail
    assert '<meta property="og:type" content="article">' in detail
    assert (
        '<meta property="og:title" content="Current &lt;script&gt;alert(1)&lt;/script&gt; '
        'story — news-tldr.com">'
    ) in detail
    assert '<meta name="twitter:card" content="summary_large_image">' in detail
    assert (
        '<a href="https://github.com/pmeenan/news-tldr.com" '
        'rel="noopener noreferrer">About</a>'
    ) in home
    assert "About</a> · <a href=\"/archive/\">Archive</a>" in detail
    assert (output_dir / "api" / "active-stories.json").exists()
    favicon = (output_dir / "favicon.ico").read_bytes()
    assert struct.unpack("<HHH", favicon[:6]) == (0, 1, 4)
    favicon_sizes = {
        (favicon[6 + index * 16] or 256, favicon[7 + index * 16] or 256)
        for index in range(4)
    }
    assert favicon_sizes == {(16, 16), (32, 32), (48, 48), (64, 64)}
    assert '<link rel="icon" href="/favicon.ico" type="image/x-icon"' in home
    assert '<link rel="icon" href="/favicon.ico" type="image/x-icon"' in detail
    assert (output_dir / "assets" / "social-card.png").is_file()
    assert (output_dir / "sitemap.xml").exists()
    assert not (output_dir / "assets" / "site.css").exists()
    assert not (output_dir / "assets" / "site.js").exists()
    assert css_asset.name == f"site.{hashlib.sha256(css_asset.read_bytes()).hexdigest()[:16]}.css"
    assert js_asset.name == f"site.{hashlib.sha256(js_asset.read_bytes()).hexdigest()[:16]}.js"
    assert f'href="/assets/{css_asset.name}"' in home
    assert f'src="/assets/{js_asset.name}"' in home
    assert f'href="/assets/{css_asset.name}"' in detail
    assert f'src="/assets/{js_asset.name}"' not in detail
    assert "data-sync-button" in home
    assert "data-sync-dialog" in home
    assert "data-sync-button" not in detail


def test_home_renders_compact_navigation_revisit_controls_and_ranked_tints(
    tmp_path: Path,
) -> None:
    story = _story("2026-08-24-polish", updated_at="2026-08-24T00:00:00Z")
    _, story_dir, active_path = _write_published_fixture(tmp_path, [story])
    payload = json.loads(active_path.read_text(encoding="utf-8"))
    payload["stories"][0]["source_coverage_score"] = 3.5
    payload["stories"][0]["source_coverage_ratio"] = 0.4
    payload["stories"][0].update(
        {"homepage_rank_score": 0.82, "category_rank_score": 0.91, "source_count": 2}
    )
    active_path.write_text(json.dumps(payload), encoding="utf-8")
    output_dir = tmp_path / "dist"

    build_static_site(
        output_dir=output_dir,
        story_dir=story_dir,
        active_stories_path=active_path,
        reader_sync_enabled=True,
        now=datetime(2026, 8, 24, 1, tzinfo=UTC),
    )

    home = (output_dir / "index.html").read_text(encoding="utf-8")
    script = _site_asset(output_dir, "js").read_text(encoding="utf-8")
    assert '>World</button>' in home
    assert '>Business</button>' in home
    assert '>Tech</button>' in home
    assert '>Climate</button>' in home
    assert 'data-view-filter="new" aria-pressed="true"' in home
    assert 'data-view-filter="all" aria-pressed="false"' in home
    assert 'data-coverage-filter="top" aria-pressed="true"' in home
    assert 'data-coverage-filter="all" aria-pressed="false"' in home
    assert 'data-source-count="2"' in home
    assert '<span data-count-label>new</span> · <time data-site-updated' in home
    assert 'data-generated-at="2026-08-24T01:00:00Z"' in home
    assert 'datetime="2026-08-24T01:00:00Z">Updated 1m ago</time>' in home
    assert 'data-rank-all="0.8200"' in home
    assert 'data-rank-category="0.9100"' in home
    assert "source-tone-" not in home
    assert "shade-" in home
    assert "Updated Aug 24, 2026 · 01:00 UTC" not in home
    assert "Last 72 hours" not in home
    assert "· 72h ·" not in home
    assert 'property="og:image" content="https://news-tldr.com/assets/social-card.png"' in home
    assert "let savedView = 'new'" in script
    assert "localStorage.setItem(VIEW_MODE_KEY, activeView)" in script
    assert "if (activeView === 'new') next.searchParams.delete('view')" in script
    assert "let savedCoverage = 'top'" in script
    assert "localStorage.setItem(COVERAGE_MODE_KEY, activeCoverage)" in script
    assert "if (activeCoverage === 'top') next.searchParams.delete('coverage')" in script
    assert "cardSourceCount(card) >= MIN_TOP_SOURCE_COUNT" in script
    assert "MIN_TOP_SOURCE_COUNT = 2" in script
    assert "VIEW_THRESHOLD_MS = 1 * 1000" in script
    assert "VIEWED_RETENTION_MS = 3 * 24 * 60 * 60 * 1000" in script
    assert "SYNC_TOKEN_KEY = 'newsTldrSyncTokenV1'" in script
    assert "SYNC_FRAGMENT_PREFIX = 'sync=v1.'" in script
    assert "history.replaceState(null, '', `${location.pathname}${location.search}`)" in script
    assert "syncRequest('/api/sync/v1/merge'" in script
    assert "SYNC_DEBOUNCE_MS = 4 * 1000" in script
    assert "SYNC_MAX_RETRY_MS = 5 * 60 * 1000" in script
    assert "SYNC_MAX_READS = 2000" in script
    assert "if (syncDirty && syncToken)" in script
    assert "function handleSyncFragment()" in script
    assert "window.addEventListener('hashchange'" in script
    assert "if (syncButton) handleSyncFragment();" in script
    assert "const fragmentHandled = handleSyncFragment();" in script
    assert "Anyone with the private link can join this sync group" in home
    assert "Read stories are synchronized automatically in the background" in home
    assert "Sync now" not in home
    assert "data-sync-now" not in script
    assert "Disconnect this browser" in home
    assert "Delete synced data" in home
    assert "IntersectionObserver" in script
    assert "Boolean(viewed[card.dataset.storyId])" in script
    assert "activeCoverage === 'top' ? 'top' : 'stories'" in script
    assert "function relativeUpdatedLabel(timestamp)" in script
    assert "window.setInterval(updateSiteFreshness, 60 * 1000)" in script
    assert "viewedBeforeLoad" not in script
    assert "createStorySection('Top News'" in script
    assert "createStorySection('Everything Else'" in script
    assert "cardCoveragePriority" in script
    assert "COVERAGE_WINDOW_MS = 24 * 60 * 60 * 1000" in script
    assert "const topNews = visibleCards" in script
    assert "window.matchMedia('(max-width: 820px)')" in script
    assert "expandedSectionKeys" in script
    assert "toggleSectionsButton.textContent = allExpanded ? 'Collapse all' : 'Expand all'" in script
    assert "grid.hidden = !expanded" in script
    assert "if (activeView === 'new') renderStories();" in script
    assert "data-mark-view-read" in home
    assert "Mark read" in home
    assert "Mark all visible stories as read" in home
    assert "Mark view read" not in home
    assert "data-toggle-sections" in home
    assert "Expand all" in home
    assert "data-story-title" in home
    assert 'class="read-indicator"' in home
    assert "data-source-coverage=" in home
    assert "data-source-share=" in home
    assert 'data-source-coverage="3.5000"' in home
    assert 'data-source-share="0.4000"' in home
    assert '<div class="reader-toolbar"><nav class="category-nav"' in home
    assert home.index('<div class="reader-toolbar">') < home.index('Latest briefing')
    assert home.index('Latest briefing') < home.index('<main class="home-main">')
    assert "if (nextCategory === activeCategory) return;" in script
    assert "window.scrollTo({ top: 0, behavior: reducedMotion ? 'auto' : 'smooth' })" in script

    css = _site_asset(output_dir, "css").read_text(encoding="utf-8")
    assert ".reader-toolbar { position: sticky; top: 0; z-index: 20; background: var(--paper); }" in css
    assert ".reader-toolbar:not(:has(.edition)) { border-bottom: 1px solid var(--ink); }" in css
    assert ".home-main { padding-top: 0; }" in css
    assert "@media (prefers-reduced-motion: reduce)" in css
    assert "html { scroll-behavior: auto; }" in css
    assert '.story-sections[data-active-category="all"] .category-world' in css
    assert '.story-sections[data-active-category="all"] .category-business' in css
    assert '.story-sections[data-active-category="all"] .category-automotive' in css
    assert '.story-sections:not([data-active-category="all"]) .story-card' in css
    assert ".story-card.lead { grid-column: span 8; }" in css
    assert "letter-spacing: normal;" in css
    assert "letter-spacing: -.025em;" not in css
    assert ".story-card.is-read .read-indicator" in css
    assert ".sync-button.is-connected" in css
    assert ".sync-dialog::backdrop" in css
    assert ".edition > p { white-space: nowrap; }" in css
    assert '.section-toggle[aria-expanded="true"]::after' in css
    assert ".story-grid[hidden] { display: none; }" in css


def test_home_renders_curation_metadata_for_top_news_and_topic_sections(tmp_path: Path) -> None:
    stories = [
        _story(
            f"2026-08-24-story-{index}",
            updated_at="2026-08-24T00:00:00Z",
            headline=f"Story {index}",
        )
        for index in range(1, 4)
    ]
    _, story_dir, active_path = _write_published_fixture(tmp_path, stories)
    payload = json.loads(active_path.read_text(encoding="utf-8"))
    payload["curation"] = {
        "prompt_version": "homepage-curation-v1",
        "model": "gemini-3.7-flash",
        "top_news": [stories[1]["story_id"]],
        "sections": [
            {
                "title": "Ukraine War",
                "story_ids": [stories[0]["story_id"], stories[1]["story_id"]],
            }
        ],
    }
    active_path.write_text(json.dumps(payload), encoding="utf-8")

    output_dir = tmp_path / "dist"
    build_static_site(
        output_dir=output_dir,
        story_dir=story_dir,
        active_stories_path=active_path,
        now=datetime(2026, 8, 24, 1, tzinfo=UTC),
    )

    home = (output_dir / "index.html").read_text(encoding="utf-8")
    assert f'data-story-id="{stories[1]["story_id"]}"' in home
    assert 'data-top-order="0"' in home
    assert 'data-topic-title="Ukraine War"' in home
    assert 'data-topic-order="0"' in home


def test_robots_blocks_search_indexers_without_blocking_social_crawlers(tmp_path: Path) -> None:
    story = _story("2026-08-24-robots", updated_at="2026-08-24T00:00:00Z")
    _, story_dir, active_path = _write_published_fixture(tmp_path, [story])
    output_dir = tmp_path / "dist"

    build_static_site(
        output_dir=output_dir,
        story_dir=story_dir,
        active_stories_path=active_path,
        now=datetime(2026, 8, 24, 1, tzinfo=UTC),
    )

    robots = (output_dir / "robots.txt").read_text(encoding="utf-8")
    home = (output_dir / "index.html").read_text(encoding="utf-8")
    assert "User-agent: Googlebot\nDisallow: /" in robots
    assert "User-agent: bingbot\nDisallow: /" in robots
    assert "User-agent: DuckDuckBot\nDisallow: /" in robots
    assert "User-agent: *\nAllow: /" in robots
    assert "User-agent: *\nDisallow: /" not in robots
    assert "Sitemap:" not in robots
    assert "data-sync-button" not in home


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


def test_deploy_static_site_retains_previous_and_legacy_site_assets(tmp_path: Path) -> None:
    old_asset = "assets/site.1111111111111111.css"
    new_asset = "assets/site.2222222222222222.css"
    legacy_asset = "assets/site.js"
    source = tmp_path / "dist"
    (source / "assets").mkdir(parents=True)
    (source / "index.html").write_text("new home", encoding="utf-8")
    (source / new_asset).write_text("new css", encoding="utf-8")

    target = tmp_path / "production"
    (target / "assets").mkdir(parents=True)
    (target / old_asset).write_text("old css", encoding="utf-8")
    (target / legacy_asset).write_text("legacy js", encoding="utf-8")
    (target / "obsolete.txt").write_text("obsolete", encoding="utf-8")
    (target / DEPLOY_MANIFEST).write_text(
        json.dumps({"files": ["index.html", old_asset, legacy_asset, "obsolete.txt"]}),
        encoding="utf-8",
    )

    stats = deploy_static_site(source_dir=source, publish_dir=target.resolve())

    assert stats["stale_files_removed"] == 1
    assert stats["site_assets_retained"] == 2
    assert (target / old_asset).read_text(encoding="utf-8") == "old css"
    assert (target / new_asset).read_text(encoding="utf-8") == "new css"
    assert (target / legacy_asset).read_text(encoding="utf-8") == "legacy js"
    assert not (target / "obsolete.txt").exists()
    manifest = json.loads((target / DEPLOY_MANIFEST).read_text(encoding="utf-8"))
    assert manifest["files"] == [
        "assets/site.1111111111111111.css",
        "assets/site.2222222222222222.css",
        "assets/site.js",
        "index.html",
    ]
