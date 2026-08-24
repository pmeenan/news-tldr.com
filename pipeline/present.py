# ruff: noqa: E501 -- embedded CSS/HTML stays readable as complete declarations.

from __future__ import annotations

import hashlib
import html
import json
import os
import shutil
import tempfile
import uuid
from collections.abc import Callable
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlparse

from pipeline.config import load_feeds, load_pipeline_config, load_source_policy
from pipeline.lock import PipelineLock
from pipeline.paths import (
    ACTIVE_STORIES_PATH,
    CONFIG_DIR,
    DIST_DIR,
    LOCK_PATH,
    STORY_DIR,
)
from pipeline.state import StateDB
from pipeline.util import isoformat_z, sanitize_id, utc_now

PRESENTATION_VERSION = "presentation-v2"
DEPLOY_MANIFEST = ".news-tldr-managed.json"
DEFAULT_SITE_URL = "https://news-tldr.com"
DEFAULT_ROLLING_WINDOW_HOURS = 72


SITE_CSS = """
:root {
  --paper: #f4f0e8;
  --paper-deep: #e9e1d3;
  --ink: #17201d;
  --muted: #65706b;
  --line: #cfc7b8;
  --accent: #c84938;
  --accent-dark: #873026;
  --card: #fffdf8;
  --world: #285f72;
  --world-rgb: 40, 95, 114;
  --us: #835c26;
  --us-rgb: 131, 92, 38;
  --politics: #8a3d3d;
  --politics-rgb: 138, 61, 61;
  --business: #51662d;
  --business-rgb: 81, 102, 45;
  --technology: #564d91;
  --technology-rgb: 86, 77, 145;
  --science: #326b62;
  --science-rgb: 50, 107, 98;
  --health: #9a4f6f;
  --health-rgb: 154, 79, 111;
  --environment: #477249;
  --environment-rgb: 71, 114, 73;
  --automotive: #66584a;
  --automotive-rgb: 102, 88, 74;
  --entertainment: #8b536f;
  --entertainment-rgb: 139, 83, 111;
  color-scheme: light;
  font-family: Georgia, "Times New Roman", serif;
  background: var(--paper);
  color: var(--ink);
}
* { box-sizing: border-box; }
html { scroll-behavior: smooth; }
body { margin: 0; background: var(--paper); }
a { color: inherit; }
.site-header { border-top: 7px solid var(--accent); border-bottom: 1px solid var(--ink); }
.masthead { max-width: 1180px; margin: 0 auto; padding: 1.25rem 1.25rem 1rem; display: flex; gap: 2rem; align-items: end; justify-content: space-between; }
.brand { text-decoration: none; font-size: clamp(2rem, 6vw, 4.4rem); line-height: .9; letter-spacing: -.065em; font-weight: 800; }
.brand span { color: var(--accent); }
.tagline { max-width: 30rem; margin: 0; color: var(--muted); font: 600 .78rem/1.4 system-ui, sans-serif; text-transform: uppercase; letter-spacing: .1em; text-align: right; }
.category-nav { max-width: 1180px; margin: 0 auto; padding: .65rem 1.25rem; display: flex; justify-content: center; gap: .35rem; overflow: visible; }
.category-nav button, .category-nav a { border: 1px solid var(--line); border-radius: 999px; background: transparent; padding: .43rem .62rem; white-space: nowrap; color: var(--muted); text-decoration: none; font: 700 .69rem/1 system-ui, sans-serif; text-transform: uppercase; letter-spacing: .035em; cursor: pointer; }
.category-nav button:hover, .category-nav button[aria-pressed="true"], .category-nav a:hover { color: white; border-color: var(--ink); background: var(--ink); }
main { max-width: 1180px; margin: 0 auto; padding: 1.5rem 1.25rem 4rem; }
.edition { display: flex; justify-content: space-between; gap: 1rem; align-items: center; padding-bottom: .75rem; border-bottom: 3px double var(--ink); }
.edition-heading { display: flex; align-items: center; gap: .8rem; min-width: 0; }
.edition h1 { margin: 0; font-size: 1rem; text-transform: uppercase; letter-spacing: .13em; }
.edition p { margin: 0; color: var(--muted); font: .78rem/1.4 system-ui, sans-serif; }
.view-switch { display: inline-flex; padding: .16rem; border: 1px solid var(--line); border-radius: 999px; background: rgba(255,255,255,.32); }
.view-switch button { border: 0; border-radius: 999px; padding: .32rem .62rem; background: transparent; color: var(--muted); font: 750 .67rem/1 system-ui, sans-serif; text-transform: uppercase; letter-spacing: .055em; cursor: pointer; }
.view-switch button:hover { color: var(--ink); }
.view-switch button[aria-pressed="true"] { color: white; background: var(--ink); }
.story-grid { display: grid; grid-template-columns: repeat(12, 1fr); }
.story-card { --card-tint: 100, 112, 106; --shade: .055; grid-column: span 4; padding: 1.5rem; border-bottom: 1px solid var(--line); border-right: 1px solid var(--line); background: linear-gradient(rgba(var(--card-tint), var(--shade)), rgba(var(--card-tint), var(--shade))), var(--card); transition: opacity .18s ease, background-color .18s ease; }
.story-card:nth-child(3n) { border-right: 0; }
.story-card.lead { grid-column: span 8; --shade: .14; }
.story-card.secondary { grid-column: span 4; --shade: .105; }
.story-card[hidden] { display: none; }
.source-tone-0 { --card-tint: 48, 94, 112; }
.source-tone-1 { --card-tint: 127, 72, 62; }
.source-tone-2 { --card-tint: 82, 105, 57; }
.source-tone-3 { --card-tint: 91, 78, 132; }
.source-tone-4 { --card-tint: 50, 111, 98; }
.source-tone-5 { --card-tint: 143, 88, 64; }
.source-tone-6 { --card-tint: 133, 76, 105; }
.source-tone-7 { --card-tint: 92, 92, 86; }
.shade-1 { --shade: .04; }
.shade-2 { --shade: .065; }
.shade-3 { --shade: .09; }
.shade-4 { --shade: .12; }
.shade-5 { --shade: .15; }
.story-card.lead.shade-1, .story-card.lead.shade-2 { --shade: .11; }
.story-card.secondary.shade-1 { --shade: .08; }
.story-grid[data-active-category="world"] .story-card { --card-tint: var(--world-rgb); }
.story-grid[data-active-category="us"] .story-card { --card-tint: var(--us-rgb); }
.story-grid[data-active-category="politics"] .story-card { --card-tint: var(--politics-rgb); }
.story-grid[data-active-category="business"] .story-card { --card-tint: var(--business-rgb); }
.story-grid[data-active-category="technology"] .story-card { --card-tint: var(--technology-rgb); }
.story-grid[data-active-category="science"] .story-card { --card-tint: var(--science-rgb); }
.story-grid[data-active-category="health"] .story-card { --card-tint: var(--health-rgb); }
.story-grid[data-active-category="environment"] .story-card { --card-tint: var(--environment-rgb); }
.story-grid[data-active-category="automotive"] .story-card { --card-tint: var(--automotive-rgb); }
.story-grid[data-active-category="entertainment"] .story-card { --card-tint: var(--entertainment-rgb); }
.category-world .kicker { color: var(--world); }
.category-us .kicker { color: var(--us); }
.category-politics .kicker { color: var(--politics); }
.category-business .kicker { color: var(--business); }
.category-technology .kicker { color: var(--technology); }
.category-science .kicker { color: var(--science); }
.category-health .kicker { color: var(--health); }
.category-environment .kicker { color: var(--environment); }
.category-automotive .kicker { color: var(--automotive); }
.category-entertainment .kicker { color: var(--entertainment); }
.kicker { margin: 0 0 .6rem; color: var(--accent-dark); font: 800 .7rem/1.2 system-ui, sans-serif; text-transform: uppercase; letter-spacing: .11em; }
.story-card h2 { margin: 0 0 .7rem; font-size: clamp(1.25rem, 2.4vw, 1.85rem); line-height: 1.08; letter-spacing: -.025em; }
.story-card.lead h2 { font-size: clamp(2rem, 4.3vw, 3.6rem); }
.story-card h2 a { text-decoration: none; }
.story-card h2 a:hover { color: var(--accent-dark); }
.dek { margin: 0 0 1rem; color: #3d4743; font-size: 1rem; line-height: 1.5; }
.tldr-list { margin: 0 0 1rem; padding-left: 1.1rem; }
.tldr-list li { margin: .35rem 0; line-height: 1.4; }
.story-meta { display: flex; gap: .65rem; flex-wrap: wrap; color: var(--muted); font: 650 .72rem/1.3 system-ui, sans-serif; }
.story-meta span + span::before { content: "•"; margin-right: .65rem; }
.empty-state { grid-column: 1 / -1; padding: 4rem 1rem; text-align: center; color: var(--muted); }
.story-page { max-width: 880px; }
.story-page .back { display: inline-block; margin-bottom: 1.5rem; color: var(--muted); font: 700 .8rem/1 system-ui, sans-serif; text-transform: uppercase; letter-spacing: .08em; }
.story-page h1 { margin: .3rem 0 1rem; font-size: clamp(2.35rem, 7vw, 5.2rem); line-height: .98; letter-spacing: -.045em; }
.story-page .standfirst { margin: 0 0 1.2rem; color: #3d4743; font-size: clamp(1.15rem, 2vw, 1.45rem); line-height: 1.5; }
.story-page section { margin-top: 2.4rem; padding-top: 1.2rem; border-top: 1px solid var(--line); }
.story-page section h2 { margin: 0 0 1rem; font-size: .85rem; font-family: system-ui, sans-serif; text-transform: uppercase; letter-spacing: .12em; }
.story-page .tldr-list { font-size: 1.2rem; }
.fact-list { list-style: none; margin: 0; padding: 0; }
.fact-list li { padding: .9rem 0; border-bottom: 1px dotted var(--line); font-size: 1.05rem; line-height: 1.55; }
.citations { margin-left: .4rem; white-space: nowrap; font: 700 .68rem/1 system-ui, sans-serif; }
.citations a { color: var(--accent-dark); text-decoration: none; }
.uncertainty { border-left: 4px solid var(--accent); padding-left: 1rem; }
.framing-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }
.framing-panel { padding: 1.15rem; background: var(--card); border: 1px solid var(--line); }
.framing-panel h3 { margin-top: 0; font: 800 .78rem/1 system-ui, sans-serif; text-transform: uppercase; letter-spacing: .1em; }
.source-list { list-style: none; padding: 0; }
.source-list li { padding: .75rem 0; border-bottom: 1px dotted var(--line); }
.source-list a { font-weight: 700; text-decoration-thickness: 1px; text-underline-offset: 3px; }
.source-name { display: block; margin-top: .25rem; color: var(--muted); font: .75rem/1.3 system-ui, sans-serif; }
.badge { display: inline-block; margin-left: .35rem; padding: .14rem .34rem; border: 1px solid var(--line); border-radius: 3px; font: 700 .6rem/1 system-ui, sans-serif; text-transform: uppercase; letter-spacing: .05em; }
.archive-list { list-style: none; padding: 0; }
.archive-list li { display: grid; grid-template-columns: 8rem 1fr; gap: 1rem; padding: .8rem 0; border-bottom: 1px solid var(--line); }
.archive-list time { color: var(--muted); font: .75rem/1.5 system-ui, sans-serif; }
.archive-list a { font-size: 1.1rem; font-weight: 700; text-decoration: none; }
.site-footer { border-top: 1px solid var(--ink); padding: 1.2rem; color: var(--muted); font: .75rem/1.5 system-ui, sans-serif; }
.site-footer div { max-width: 1180px; margin: auto; display: flex; justify-content: space-between; gap: 1rem; }
.site-footer a { text-underline-offset: 3px; }
@media (max-width: 820px) {
  .masthead { display: block; }
  .tagline { margin-top: .8rem; text-align: left; }
  .story-card, .story-card.lead, .story-card.secondary { grid-column: span 12; border-right: 0; }
  .framing-grid { grid-template-columns: 1fr; }
  .category-nav { justify-content: flex-start; overflow-x: auto; scrollbar-width: thin; }
}
@media (max-width: 520px) {
  main { padding-inline: .85rem; }
  .masthead, .category-nav { padding-inline: .85rem; }
  .story-card { padding: 1.25rem .4rem; }
  .edition { align-items: flex-start; }
  .edition-heading { align-items: flex-start; flex-direction: column; gap: .55rem; }
  .edition > p { max-width: 9.5rem; text-align: right; }
  .archive-list li { grid-template-columns: 1fr; gap: .2rem; }
  .site-footer div { display: block; }
}
""".strip()


SITE_JS = """
const VIEWED_KEY = 'newsTldrViewedStoriesV1';
const VIEW_MODE_KEY = 'newsTldrViewModeV1';
const VIEWED_RETENTION_MS = 3 * 24 * 60 * 60 * 1000;
const VIEW_THRESHOLD_MS = 10 * 1000;
const categoryButtons = Array.from(document.querySelectorAll('[data-category-filter]'));
const viewButtons = Array.from(document.querySelectorAll('[data-view-filter]'));
const cards = Array.from(document.querySelectorAll('[data-story-category]'));
const count = document.querySelector('[data-visible-count]');
const countLabel = document.querySelector('[data-count-label]');
const grid = document.querySelector('[data-story-grid]');
const emptyState = document.querySelector('[data-empty-state]');
const timers = new Map();

function loadViewed() {
  const now = Date.now();
  let viewed = {};
  try {
    const parsed = JSON.parse(localStorage.getItem(VIEWED_KEY) || '{}');
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) viewed = parsed;
  } catch (_) {
    viewed = {};
  }
  for (const [storyId, timestamp] of Object.entries(viewed)) {
    if (!Number.isFinite(timestamp) || now - timestamp > VIEWED_RETENTION_MS) delete viewed[storyId];
  }
  try { localStorage.setItem(VIEWED_KEY, JSON.stringify(viewed)); } catch (_) {}
  return viewed;
}

const viewed = loadViewed();
const viewedBeforeLoad = new Set(Object.keys(viewed));
const params = new URLSearchParams(location.search);
let activeCategory = params.get('category') || 'all';
if (!categoryButtons.some((button) => button.dataset.categoryFilter === activeCategory)) {
  activeCategory = 'all';
}
let savedView = 'all';
try { savedView = localStorage.getItem(VIEW_MODE_KEY) || 'all'; } catch (_) {}
let activeView = params.get('view') || (viewedBeforeLoad.size ? savedView : 'all');
if (!['new', 'all'].includes(activeView)) activeView = 'all';

function cardRank(card, category) {
  const value = category === 'all' ? card.dataset.rankAll : card.dataset.rankCategory;
  const parsed = Number.parseFloat(value || '0');
  return Number.isFinite(parsed) ? parsed : 0;
}

function updateUrl() {
  const next = new URL(location.href);
  if (activeCategory === 'all') next.searchParams.delete('category');
  else next.searchParams.set('category', activeCategory);
  if (activeView === 'all') next.searchParams.delete('view');
  else next.searchParams.set('view', activeView);
  history.replaceState(null, '', `${next.pathname}${next.search}${next.hash}`);
}

function renderStories() {
  const ordered = [...cards].sort((left, right) => {
    const rankDifference = cardRank(right, activeCategory) - cardRank(left, activeCategory);
    if (rankDifference) return rankDifference;
    return (right.dataset.eventUpdated || '').localeCompare(left.dataset.eventUpdated || '');
  });
  for (const card of ordered) grid.insertBefore(card, emptyState);

  const visibleCards = [];
  for (const card of ordered) {
    const matchesCategory = activeCategory === 'all' || card.dataset.storyCategory === activeCategory;
    const isPreviouslyViewed = viewedBeforeLoad.has(card.dataset.storyId);
    const show = matchesCategory && (activeView === 'all' || !isPreviouslyViewed);
    card.hidden = !show;
    card.classList.remove('lead', 'secondary');
    if (show) visibleCards.push(card);
  }
  if (visibleCards[0]) visibleCards[0].classList.add('lead');
  if (visibleCards[1]) visibleCards[1].classList.add('secondary');
  for (const button of categoryButtons) {
    button.setAttribute('aria-pressed', String(button.dataset.categoryFilter === activeCategory));
  }
  for (const button of viewButtons) {
    button.setAttribute('aria-pressed', String(button.dataset.viewFilter === activeView));
  }
  grid.dataset.activeCategory = activeCategory;
  if (count) count.textContent = String(visibleCards.length);
  if (countLabel) countLabel.textContent = activeView === 'new' ? 'new stories' : 'stories';
  if (emptyState) {
    emptyState.hidden = visibleCards.length !== 0;
    emptyState.textContent = activeView === 'new'
      ? 'You’re caught up. Switch to All to revisit stories from the last three days.'
      : 'No stories fall within the current news window.';
  }
  updateUrl();
}

for (const button of categoryButtons) {
  button.addEventListener('click', () => {
    activeCategory = button.dataset.categoryFilter;
    renderStories();
  });
}

for (const button of viewButtons) {
  button.addEventListener('click', () => {
    activeView = button.dataset.viewFilter;
    try { localStorage.setItem(VIEW_MODE_KEY, activeView); } catch (_) {}
    renderStories();
  });
}

function markViewed(card) {
  timers.delete(card);
  const storyId = card.dataset.storyId;
  if (!storyId) return;
  viewed[storyId] = Date.now();
  try { localStorage.setItem(VIEWED_KEY, JSON.stringify(viewed)); } catch (_) {}
}

if ('IntersectionObserver' in window) {
  const observer = new IntersectionObserver((entries) => {
    for (const entry of entries) {
      const card = entry.target;
      if (entry.isIntersecting && entry.intersectionRatio >= 0.55 && !timers.has(card)) {
        timers.set(card, window.setTimeout(() => markViewed(card), VIEW_THRESHOLD_MS));
      } else if ((!entry.isIntersecting || entry.intersectionRatio < 0.55) && timers.has(card)) {
        window.clearTimeout(timers.get(card));
        timers.delete(card);
      }
    }
  }, { threshold: [0.55] });
  for (const card of cards) observer.observe(card);
}

window.addEventListener('pagehide', () => {
  for (const timer of timers.values()) window.clearTimeout(timer);
  timers.clear();
});

renderStories();
""".strip()


def presentation_once(
    *,
    publish: bool | None = None,
    publish_dir: Path | None = None,
    output_dir: Path = DIST_DIR,
    progress: Callable[[str], None] | None = None,
    acquire_lock: bool = True,
) -> dict[str, Any]:
    config = load_pipeline_config()
    presentation_config = config.presentation
    selected_publish = (
        bool(presentation_config.get("publish_enabled", False)) if publish is None else publish
    )
    selected_publish_dir = publish_dir or Path(
        str(presentation_config.get("publish_dir") or "/var/www/news-tldr.com")
    )
    site_url = str(presentation_config.get("site_url") or DEFAULT_SITE_URL).rstrip("/")
    rolling_window_hours = int(
        presentation_config.get("rolling_window_hours", DEFAULT_ROLLING_WINDOW_HOURS)
    )
    if rolling_window_hours < 1:
        raise ValueError("presentation rolling_window_hours must be at least 1")

    lock_timeout = timedelta(minutes=int(config.pipeline.get("watchdog_timeout_minutes", 30)))
    run_id = f"presentation-{uuid.uuid4().hex}"
    state = StateDB()
    stats: dict[str, Any] = {
        "run_id": run_id,
        "presentation_version": PRESENTATION_VERSION,
        "publish_enabled": selected_publish,
    }
    try:
        lock_context = PipelineLock(LOCK_PATH, lock_timeout, run_id=run_id) if acquire_lock else nullcontext()
        with lock_context:
            state.start_run(run_id, "presentation")
            status = "success"
            try:
                if progress:
                    progress("presentation: building static site")
                stats.update(
                    build_static_site(
                        output_dir=output_dir,
                        site_url=site_url,
                        rolling_window_hours=rolling_window_hours,
                    )
                )
                if selected_publish:
                    if progress:
                        progress(f"presentation: publishing to {selected_publish_dir}")
                    stats.update(
                        deploy_static_site(
                            source_dir=output_dir,
                            publish_dir=selected_publish_dir,
                        )
                    )
                else:
                    stats["published"] = False
            except Exception:
                status = "failed"
                raise
            finally:
                state.finish_run(run_id, status, stats)
        return stats
    finally:
        state.close()


def build_static_site(
    *,
    output_dir: Path = DIST_DIR,
    story_dir: Path = STORY_DIR,
    active_stories_path: Path = ACTIVE_STORIES_PATH,
    site_url: str = DEFAULT_SITE_URL,
    rolling_window_hours: int = DEFAULT_ROLLING_WINDOW_HOURS,
    now: datetime | None = None,
) -> dict[str, Any]:
    generated_at = now or utc_now()
    if generated_at.tzinfo is None:
        generated_at = generated_at.replace(tzinfo=UTC)
    generated_at = generated_at.astimezone(UTC)
    index = _read_json(active_stories_path)
    index_rows = index.get("stories")
    if not isinstance(index_rows, list):
        raise ValueError("active stories index must contain a stories list")

    categories = _load_categories()
    category_names = {item["id"]: item["name"] for item in categories}
    source_metadata = _source_metadata_by_name()
    stories: list[dict[str, Any]] = []
    for row in index_rows:
        if not isinstance(row, dict):
            raise ValueError("active stories index rows must be objects")
        story_id = str(row.get("story_id") or "")
        if not story_id or sanitize_id(story_id) != story_id:
            raise ValueError(f"invalid story_id in active index: {story_id!r}")
        story = _read_json(story_dir / f"{story_id}.json")
        if story.get("story_id") != story_id or story.get("event_id") != story_id:
            raise ValueError(f"story artifact identity mismatch: {story_id}")
        if story.get("category") not in category_names:
            raise ValueError(f"story has invalid category: {story_id}")
        story["_index"] = row
        stories.append(story)

    cutoff = generated_at - timedelta(hours=rolling_window_hours)
    current_stories = [story for story in stories if _event_time(story) >= cutoff]
    temporary_parent = output_dir.parent
    temporary_parent.mkdir(parents=True, exist_ok=True)
    temporary_dir = Path(
        tempfile.mkdtemp(prefix=f".{output_dir.name}-build-", dir=temporary_parent)
    )
    try:
        _write_site_files(
            root=temporary_dir,
            stories=stories,
            current_stories=current_stories,
            categories=categories,
            category_names=category_names,
            source_metadata=source_metadata,
            site_url=site_url,
            generated_at=generated_at,
            rolling_window_hours=rolling_window_hours,
            active_index=index,
        )
        _replace_generated_directory(temporary_dir, output_dir)
    except Exception:
        shutil.rmtree(temporary_dir, ignore_errors=True)
        raise

    file_count = sum(1 for path in output_dir.rglob("*") if path.is_file())
    return {
        "built": True,
        "output_dir": str(output_dir),
        "stories_rendered": len(stories),
        "homepage_stories": len(current_stories),
        "files_built": file_count,
        "generated_at": isoformat_z(generated_at),
        "rolling_window_hours": rolling_window_hours,
    }


def deploy_static_site(*, source_dir: Path, publish_dir: Path) -> dict[str, Any]:
    source_root = source_dir.resolve()
    if not source_root.is_dir():
        raise ValueError(f"static build directory does not exist: {source_dir}")
    if not publish_dir.is_absolute():
        raise ValueError("publish_dir must be an absolute path")
    if publish_dir.is_symlink():
        raise ValueError("publish_dir must not be a symlink")
    publish_dir.mkdir(parents=True, exist_ok=True)
    publish_root = publish_dir.resolve()
    if publish_root in {Path("/"), Path.home().resolve(), source_root} or len(publish_root.parts) < 3:
        raise ValueError(f"refusing unsafe publish directory: {publish_dir}")

    source_files: dict[str, Path] = {}
    for path in source_root.rglob("*"):
        if path.is_symlink():
            raise ValueError(f"static build contains a symlink: {path}")
        if path.is_file():
            relative = path.relative_to(source_root).as_posix()
            _validate_managed_path(relative)
            source_files[relative] = path
    if "index.html" not in source_files:
        raise ValueError("static build is missing index.html")

    previous_files = _read_deploy_manifest(publish_root / DEPLOY_MANIFEST)
    copied = 0
    ordered_paths = sorted(source_files, key=lambda value: (value == "index.html", value))
    for relative in ordered_paths:
        if relative == "index.html":
            continue
        _copy_public_file(source_files[relative], publish_root, relative)
        copied += 1

    stale_files = sorted(previous_files - set(source_files), reverse=True)
    removed = 0
    for relative in stale_files:
        target = _safe_publish_target(publish_root, relative)
        if target.is_file() or target.is_symlink():
            target.unlink()
            removed += 1
            _remove_empty_managed_parents(target.parent, publish_root)

    _copy_public_file(source_files["index.html"], publish_root, "index.html")
    copied += 1
    manifest = {
        "version": 1,
        "presentation_version": PRESENTATION_VERSION,
        "published_at": isoformat_z(),
        "files": sorted(source_files),
    }
    _write_public_bytes(
        publish_root / DEPLOY_MANIFEST,
        (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8"),
    )
    return {
        "published": True,
        "publish_dir": str(publish_root),
        "files_published": copied,
        "stale_files_removed": removed,
    }


def _write_site_files(
    *,
    root: Path,
    stories: list[dict[str, Any]],
    current_stories: list[dict[str, Any]],
    categories: list[dict[str, Any]],
    category_names: dict[str, str],
    source_metadata: dict[str, dict[str, Any]],
    site_url: str,
    generated_at: datetime,
    rolling_window_hours: int,
    active_index: dict[str, Any],
) -> None:
    _write_text(root / "assets" / "site.css", SITE_CSS + "\n")
    _write_text(root / "assets" / "site.js", SITE_JS + "\n")
    _write_text(
        root / "index.html",
        _render_home(
            stories=current_stories,
            categories=categories,
            category_names=category_names,
            site_url=site_url,
            generated_at=generated_at,
            rolling_window_hours=rolling_window_hours,
        ),
    )
    _write_text(
        root / "archive" / "index.html",
        _render_archive(stories, category_names=category_names, site_url=site_url),
    )
    _write_text(root / "404.html", _render_not_found(site_url))
    _write_text(root / "robots.txt", f"User-agent: *\nAllow: /\nSitemap: {site_url}/sitemap.xml\n")

    sitemap_urls = [f"{site_url}/", f"{site_url}/archive/"]
    for story in stories:
        story_id = story["story_id"]
        route = f"/stories/{story_id}/"
        _write_text(
            root / "stories" / story_id / "index.html",
            _render_story(
                story,
                category_name=category_names[story["category"]],
                source_metadata=source_metadata,
                site_url=site_url,
            ),
        )
        _write_json(root / "api" / "stories" / f"{story_id}.json", _public_story(story))
        sitemap_urls.append(f"{site_url}{route}")
    _write_json(root / "api" / "active-stories.json", active_index)
    _write_text(root / "sitemap.xml", _render_sitemap(sitemap_urls))


def _render_home(
    *,
    stories: list[dict[str, Any]],
    categories: list[dict[str, Any]],
    category_names: dict[str, str],
    site_url: str,
    generated_at: datetime,
    rolling_window_hours: int,
) -> str:
    buttons = [
        '<button type="button" data-category-filter="all" aria-pressed="true">All</button>'
    ]
    for category in categories:
        buttons.append(
            '<button type="button" data-category-filter="{}" aria-pressed="false">{}</button>'.format(
                _e(category["id"]), _e(category.get("short_name") or category["name"])
            )
        )
    cards = []
    for index, story in enumerate(stories):
        variant = " lead" if index == 0 else " secondary" if index == 1 else ""
        tldr = story.get("tldr") if isinstance(story.get("tldr"), list) else []
        bullets = "".join(f"<li>{_e(item)}</li>" for item in tldr[:2])
        row = story["_index"]
        source_count = int(row.get("source_count") or len(story.get("sources") or []))
        all_rank = _score(row.get("homepage_rank_score", row.get("importance_score")))
        category_rank = _score(row.get("category_rank_score", row.get("importance_score")))
        source_tone = _source_tone(story)
        shade_level = _shade_level(
            rank=max(all_rank, category_rank), source_count=source_count
        )
        cards.append(
            f'<article class="story-card category-{_e(story["category"])} source-tone-{source_tone} '
            f'shade-{shade_level}{variant}" data-story-id="{_e(story["story_id"])}" '
            f'data-story-category="{_e(story["category"])}" data-rank-all="{all_rank:.4f}" '
            f'data-rank-category="{category_rank:.4f}" '
            f'data-event-updated="{_e(_event_time(story).isoformat())}">'
            f'<p class="kicker">{_e(category_names[story["category"]])}</p>'
            f'<h2><a href="/stories/{_e(story["story_id"])}/">{_e(story["headline"])}</a></h2>'
            f'<p class="dek">{_e(story["dek"])}</p>'
            f'<ul class="tldr-list">{bullets}</ul>'
            f'<div class="story-meta"><span>{_relative_time(_event_time(story), generated_at)}</span>'
            f'<span>{source_count} sources</span></div>'
            "</article>"
        )
    empty_hidden = "" if not cards else " hidden"
    cards.append(
        f'<p class="empty-state" data-empty-state{empty_hidden}>'
        "No stories fall within the current news window.</p>"
    )
    content = (
        '<div class="edition"><div class="edition-heading"><h1>Latest briefing</h1>'
        '<div class="view-switch" role="group" aria-label="Choose which stories to show">'
        '<button type="button" data-view-filter="new" aria-pressed="false" '
        'title="Hide stories seen for at least 10 seconds on earlier visits">New</button>'
        '<button type="button" data-view-filter="all" aria-pressed="true">All</button></div></div>'
        f'<p><span data-visible-count>{len(stories)}</span> <span data-count-label>stories</span> · Last {rolling_window_hours} hours · '
        f'Built {_display_datetime(generated_at)}</p></div>'
        f'<section class="story-grid" data-story-grid data-active-category="all">{"".join(cards)}</section>'
    )
    return _page(
        title="news-tldr.com — The news, distilled",
        description="Neutral, source-attributed summaries of the stories shaping the day.",
        canonical=f"{site_url}/",
        nav="".join(buttons),
        content=content,
        script=True,
    )


def _render_story(
    story: dict[str, Any],
    *,
    category_name: str,
    source_metadata: dict[str, dict[str, Any]],
    site_url: str,
) -> str:
    source_lookup = {
        source["article_id"]: source
        for source in story.get("sources", [])
        if isinstance(source, dict) and source.get("article_id")
    }
    tldr = "".join(f"<li>{_e(item)}</li>" for item in story.get("tldr", []))
    facts = "".join(
        f'<li>{_e(item["text"])}{_citations(item.get("source_article_ids"), source_lookup)}</li>'
        for item in story.get("key_facts", [])
    )
    uncertainties = story.get("uncertainties") or []
    uncertainty_section = ""
    if uncertainties:
        rows = "".join(
            f'<li>{_e(item["text"])}{_citations(item.get("source_article_ids"), source_lookup)}</li>'
            for item in uncertainties
        )
        uncertainty_section = (
            '<section class="uncertainty"><h2>What remains uncertain</h2>'
            f'<ul class="fact-list">{rows}</ul></section>'
        )

    framing = story.get("political_framing")
    framing_section = ""
    if isinstance(framing, dict):
        left = framing["left_perspective"]
        right = framing["right_perspective"]
        framing_section = (
            '<section><h2>How coverage diverges</h2>'
            f'<p>{_e(framing["summary"])}</p><div class="framing-grid">'
            '<div class="framing-panel"><h3>Left / center-left perspective</h3>'
            f'<p>{_e(left["summary"])}{_citations(left.get("source_article_ids"), source_lookup)}</p></div>'
            '<div class="framing-panel"><h3>Right / center-right perspective</h3>'
            f'<p>{_e(right["summary"])}{_citations(right.get("source_article_ids"), source_lookup)}</p>'
            "</div></div></section>"
        )

    source_rows = []
    for source in story.get("sources", []):
        if not isinstance(source, dict):
            continue
        metadata = source_metadata.get(str(source.get("source_name")), {})
        paywall = metadata.get("paywall")
        badge = f'<span class="badge">{_e(paywall)}</span>' if paywall in {"metered", "hard"} else ""
        source_rows.append(
            '<li><a href="{}" rel="noopener noreferrer">{}</a>'
            '<span class="source-name">{} {}</span></li>'.format(
                _safe_url(source.get("url")),
                _e(source.get("headline")),
                _e(source.get("source_name")),
                badge,
            )
        )

    updated = _event_time(story)
    content = (
        '<article class="story-page"><a class="back" href="/">← Latest briefing</a>'
        f'<p class="kicker">{_e(category_name)}</p><h1>{_e(story["headline"])}</h1>'
        f'<p class="standfirst">{_e(story["dek"])}</p>'
        f'<div class="story-meta"><span>Updated {_display_datetime(updated)}</span>'
        f'<span>{len(story.get("sources") or [])} reports</span></div>'
        f'<section><h2>The short version</h2><ul class="tldr-list">{tldr}</ul></section>'
        f'<section><h2>Key facts</h2><ul class="fact-list">{facts}</ul></section>'
        f'{uncertainty_section}{framing_section}'
        f'<section><h2>Sources</h2><ul class="source-list">{"".join(source_rows)}</ul></section>'
        "</article>"
    )
    return _page(
        title=f'{story["headline"]} — news-tldr.com',
        description=str(story["dek"]),
        canonical=f'{site_url}/stories/{story["story_id"]}/',
        nav='<a href="/">Latest</a><a href="/archive/">Archive</a>',
        content=content,
    )


def _render_archive(
    stories: list[dict[str, Any]], *, category_names: dict[str, str], site_url: str
) -> str:
    rows = []
    for story in stories:
        rows.append(
            '<li><time datetime="{}">{}</time><div><p class="kicker">{}</p>'
            '<a href="/stories/{}/">{}</a></div></li>'.format(
                _e(_event_time(story).isoformat()),
                _display_date(_event_time(story)),
                _e(category_names[story["category"]]),
                _e(story["story_id"]),
                _e(story["headline"]),
            )
        )
    content = (
        '<div class="edition"><h1>Active story archive</h1>'
        f'<p>{len(stories)} current stories</p></div><ul class="archive-list">{"".join(rows)}</ul>'
    )
    return _page(
        title="Active story archive — news-tldr.com",
        description="Browse current source-attributed news summaries.",
        canonical=f"{site_url}/archive/",
        nav='<a href="/">Latest</a><a href="/archive/">Archive</a>',
        content=content,
    )


def _render_not_found(site_url: str) -> str:
    return _page(
        title="Page not found — news-tldr.com",
        description="The requested news summary could not be found.",
        canonical=f"{site_url}/404.html",
        nav='<a href="/">Latest</a><a href="/archive/">Archive</a>',
        content=(
            '<div class="empty-state"><p class="kicker">404</p>'
            '<h1>That story is not on this page.</h1><p><a href="/">Return to the latest briefing</a></p></div>'
        ),
    )


def _page(
    *,
    title: str,
    description: str,
    canonical: str,
    nav: str,
    content: str,
    script: bool = False,
) -> str:
    script_tag = '<script src="/assets/site.js" defer></script>' if script else ""
    return (
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">"
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        '<meta http-equiv="Content-Security-Policy" content="default-src \'self\'; '
        "script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'none'; "
        "object-src 'none'; base-uri 'none'; form-action 'none'\">"
        f"<title>{_e(title)}</title><meta name=\"description\" content=\"{_attr(description)}\">"
        f'<link rel="canonical" href="{_attr(canonical)}">'
        f'<meta property="og:type" content="website"><meta property="og:title" content="{_attr(title)}">'
        f'<meta property="og:description" content="{_attr(description)}">'
        f'<meta property="og:url" content="{_attr(canonical)}">'
        '<meta name="twitter:card" content="summary">'
        '<link rel="stylesheet" href="/assets/site.css">'
        f"{script_tag}</head><body><header class=\"site-header\"><div class=\"masthead\">"
        '<a class="brand" href="/">news<span>-tldr</span>.com</a>'
        '<p class="tagline">The important facts, the open questions, and the sources — without the churn.</p>'
        f'</div><nav class="category-nav" aria-label="Story categories">{nav}</nav></header>'
        f"<main>{content}</main><footer class=\"site-footer\"><div>"
        '<span>Automated, source-attributed news summaries. Verify important details with the linked reporting.</span>'
        '<span><a href="/archive/">Archive</a> · <a href="/api/active-stories.json">JSON</a></span>'
        "</div></footer></body></html>\n"
    )


def _citations(article_ids: Any, source_lookup: dict[str, dict[str, Any]]) -> str:
    if not isinstance(article_ids, list):
        return ""
    links = []
    for article_id in article_ids:
        source = source_lookup.get(str(article_id))
        if not source:
            continue
        name = str(source.get("source_name") or "source")
        short_name = name.split(" - ", 1)[0]
        links.append(
            f'<a href="{_safe_url(source.get("url"))}" rel="noopener noreferrer">{_e(short_name)}</a>'
        )
    return f'<span class="citations">[{" · ".join(links)}]</span>' if links else ""


def _public_story(story: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in story.items() if not key.startswith("_")}


def _score(value: Any) -> float:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return 0.0
    if score != score:
        return 0.0
    return max(0.0, min(1.0, score))


def _source_tone(story: dict[str, Any]) -> int:
    source_names = sorted(
        {
            str(source.get("source_name"))
            for source in story.get("sources", [])
            if isinstance(source, dict) and source.get("source_name")
        }
    )
    source_key = "|".join(source_names) if source_names else str(story.get("story_id") or "news")
    return hashlib.sha256(source_key.encode("utf-8")).digest()[0] % 8


def _shade_level(*, rank: float, source_count: int) -> int:
    strength = rank * 0.8 + min(1.0, max(0, source_count) / 5.0) * 0.2
    if strength >= 0.84:
        return 5
    if strength >= 0.72:
        return 4
    if strength >= 0.60:
        return 3
    if strength >= 0.45:
        return 2
    return 1


def _load_categories() -> list[dict[str, Any]]:
    payload = _read_json(CONFIG_DIR / "categories.json")
    categories = payload.get("categories")
    if not isinstance(categories, list):
        raise ValueError("categories config must contain a categories list")
    clean = []
    for item in categories:
        if not isinstance(item, dict) or not item.get("id") or not item.get("name"):
            raise ValueError("category entries require id and name")
        clean.append(dict(item))
    return sorted(clean, key=lambda item: int(item.get("sort_order", 999)))


def _source_metadata_by_name() -> dict[str, dict[str, Any]]:
    policy = load_source_policy()
    result = {}
    for feed in load_feeds(enabled_only=False):
        result[feed.source_name] = policy.get(feed.source_id, {})
    return result


def _event_time(story: dict[str, Any]) -> datetime:
    index = story.get("_index") if isinstance(story.get("_index"), dict) else {}
    value = index.get("event_updated_at") or story.get("llm_metadata", {}).get("event_updated_at")
    if not isinstance(value, str):
        value = story.get("updated_at")
    if not isinstance(value, str):
        raise ValueError(f"story is missing an updated timestamp: {story.get('story_id')}")
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _relative_time(value: datetime, now: datetime) -> str:
    seconds = max(0, int((now - value).total_seconds()))
    if seconds < 3600:
        minutes = max(1, seconds // 60)
        return f"Updated {minutes}m ago"
    if seconds < 86400:
        return f"Updated {seconds // 3600}h ago"
    return f"Updated {seconds // 86400}d ago"


def _display_date(value: datetime) -> str:
    return f"{value.strftime('%b')} {value.day}, {value.year}"


def _display_datetime(value: datetime) -> str:
    return f"{_display_date(value)} · {value.strftime('%H:%M')} UTC"


def _safe_url(value: Any) -> str:
    if not isinstance(value, str):
        return "#"
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return "#"
    return _attr(value)


def _render_sitemap(urls: list[str]) -> str:
    rows = "".join(f"<url><loc>{_e(url)}</loc></url>" for url in urls)
    return '<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">' + rows + "</urlset>\n"


def _replace_generated_directory(temporary_dir: Path, output_dir: Path) -> None:
    output_dir = output_dir.resolve()
    if output_dir in {Path("/"), Path.home().resolve()} or len(output_dir.parts) < 3:
        raise ValueError(f"refusing unsafe build output directory: {output_dir}")
    backup = output_dir.parent / f".{output_dir.name}-backup-{uuid.uuid4().hex}"
    had_output = output_dir.exists()
    if had_output:
        os.replace(output_dir, backup)
    try:
        os.replace(temporary_dir, output_dir)
    except Exception:
        if had_output and backup.exists():
            os.replace(backup, output_dir)
        raise
    if backup.exists():
        shutil.rmtree(backup)


def _read_deploy_manifest(path: Path) -> set[str]:
    if not path.exists():
        return set()
    payload = _read_json(path)
    files = payload.get("files")
    if not isinstance(files, list):
        raise ValueError(f"deployment manifest is invalid: {path}")
    clean = set()
    for value in files:
        relative = str(value)
        _validate_managed_path(relative)
        clean.add(relative)
    return clean


def _validate_managed_path(relative: str) -> None:
    path = PurePosixPath(relative)
    if not relative or path.is_absolute() or ".." in path.parts or "." in path.parts:
        raise ValueError(f"unsafe managed path: {relative!r}")


def _safe_publish_target(root: Path, relative: str) -> Path:
    _validate_managed_path(relative)
    target = root.joinpath(*PurePosixPath(relative).parts)
    parent = target.parent
    parent.mkdir(parents=True, exist_ok=True)
    resolved_parent = parent.resolve()
    try:
        resolved_parent.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"managed path escapes publish directory: {relative}") from exc
    return target


def _copy_public_file(source: Path, publish_root: Path, relative: str) -> None:
    target = _safe_publish_target(publish_root, relative)
    _write_public_bytes(target, source.read_bytes())


def _write_public_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        os.fchmod(fd, 0o644)
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    except Exception:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def _remove_empty_managed_parents(path: Path, root: Path) -> None:
    current = path
    while current != root:
        try:
            current.rmdir()
        except OSError:
            break
        current = current.parent


def _read_json(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"could not read JSON artifact {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"JSON artifact must be an object: {path}")
    return value


def _write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _write_json(path: Path, value: dict[str, Any]) -> None:
    _write_text(path, json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def _e(value: Any) -> str:
    return html.escape(str(value or ""), quote=True)


def _attr(value: Any) -> str:
    return html.escape(str(value or ""), quote=True)
