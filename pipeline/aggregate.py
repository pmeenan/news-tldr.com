from __future__ import annotations

import json
import re
import uuid
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from pipeline.config import load_categories, load_feeds, load_pipeline_config
from pipeline.llm import GeminiClient, GeminiResult
from pipeline.lock import PipelineLock
from pipeline.paths import EVENT_DIR, LOCK_PATH, PROJECT_ROOT
from pipeline.state import StateDB
from pipeline.util import atomic_write_json, isoformat_z, sanitize_id

AGGREGATION_PROMPT_VERSION = "aggregation-v6"
AGGREGATION_EXPERIMENT_PROMPT_VERSION = "aggregation-experiment-v6"
NEWSWORTHINESS_PROMPT_VERSION = "newsworthiness-v1"
GROUPING_MODES = ("titles", "titles_summaries")
DEDUPLICATION_MERGE_CONFIDENCE_THRESHOLD = 0.8
MAX_CATEGORY_GROUP_ARTICLES = 50
NULL_EXISTING_EVENT_ID_VALUES = {"null", "none", "nil", "n/a", "na", "unknown"}
CATEGORY_GROUPS = [
    {"name": "politics_gov", "categories": ["politics"]},
    {"name": "news_business", "categories": ["us", "world", "business"]},
    {"name": "sci_tech", "categories": ["technology", "science", "health", "environment"]},
    {"name": "leisure", "categories": ["sports", "entertainment", "automotive"]},
]
CATEGORY_COMPATIBILITY_BRIDGES = (
    frozenset({"politics", "us"}),
    frozenset({"politics", "world"}),
)


def _in_same_category_group(cat1: str, cat2: str) -> bool:
    for group in CATEGORY_GROUPS:
        if cat1 in group["categories"] and cat2 in group["categories"]:
            return True
    return any(cat1 in bridge and cat2 in bridge for bridge in CATEGORY_COMPATIBILITY_BRIDGES)


def _candidate_categories_for_group(categories: Sequence[str]) -> set[str]:
    compatible = set(categories)
    for category in categories:
        for bridge in CATEGORY_COMPATIBILITY_BRIDGES:
            if category in bridge:
                compatible.update(bridge)
    return compatible


def _category_group_for_category(category: str) -> dict[str, Any]:
    for group in CATEGORY_GROUPS:
        if category in group["categories"]:
            return group
    return CATEGORY_GROUPS[1]


def _category_batches_for_articles(
    articles: Sequence[ArticleForAggregation],
    feeds_by_source: dict[str, Any],
    *,
    max_articles: int = MAX_CATEGORY_GROUP_ARTICLES,
) -> list[dict[str, Any]]:
    if max_articles <= 0:
        raise ValueError("max_articles must be positive")

    article_categories = [
        (article, _category_for_articles([article], feeds_by_source)) for article in articles
    ]
    bucket_order: list[str] = []
    buckets: dict[str, dict[str, Any]] = {}

    def add_to_bucket(name: str, categories: Sequence[str], article: ArticleForAggregation) -> None:
        if name not in buckets:
            bucket_order.append(name)
            buckets[name] = {"name": name, "categories": list(categories), "articles": []}
        buckets[name]["articles"].append(article)

    for group in CATEGORY_GROUPS:
        for article, category in article_categories:
            if _category_group_for_category(category)["name"] != group["name"]:
                continue
            if group["name"] == "news_business" and category in group["categories"]:
                add_to_bucket(f"news_business_{category}", [category], article)
            else:
                add_to_bucket(group["name"], group["categories"], article)

    batches: list[dict[str, Any]] = []
    for bucket_name in bucket_order:
        bucket = buckets[bucket_name]
        bucket_articles = bucket["articles"]
        chunks = [
            bucket_articles[index : index + max_articles]
            for index in range(0, len(bucket_articles), max_articles)
        ]
        for chunk_index, chunk in enumerate(chunks, start=1):
            batch_name = bucket_name
            if len(chunks) > 1:
                batch_name = f"{bucket_name}-{chunk_index}"
            batches.append({
                "name": batch_name,
                "categories": bucket["categories"],
                "articles": chunk,
            })
    return batches


AGGREGATION_EXCLUDED_RATIONALE_CODES = {
    "advertorial",
    "affiliate_content",
    "affiliate_deals",
    "aggregation_noise",
    "archive_index",
    "archival_index",
    "carousel",
    "deal_content",
    "gambling_advice",
    "gambling_content",
    "gallery_page",
    "impact_capped",
    "index_page",
    "media_transcript",
    "no_substantive_content",
    "product_advice",
    "product_deal",
    "product_deals",
    "product_recommendation",
    "profile_or_background",
    "promotional",
    "promotional_content",
    "promotional_links",
    "promotional_material",
    "puzzle_guide",
    "shopping_content",
    "video_carousel",
    "video_page",
}
VIDEO_OR_CAROUSEL_RATIONALE_CODES = {
    "carousel",
    "gallery_page",
    "media_transcript",
    "no_substantive_content",
    "video_carousel",
    "video_page",
}


class JsonGenerator(Protocol):
    model: str

    def generate_json(
        self,
        *,
        system_instruction: str,
        prompt: str,
        response_schema: dict[str, Any],
        temperature: float = 0,
    ) -> GeminiResult: ...


@dataclass(frozen=True)
class ArticleForAggregation:
    article_id: str
    source_id: str
    source_name: str
    headline: str
    summary: str | None
    published_at: str | None
    article_path: str
    event_id: str | None = None
    digest_summary: str | None = None
    digest_key_facts: tuple[str, ...] = ()
    digest_content_quality: str | None = None
    digest_impact: dict[str, Any] | None = None


@dataclass(frozen=True)
class AggregationWindow:
    window_start: str
    window_end: str


def load_unprocessed_articles(
    *,
    limit: int | None,
    published_date: str | None = None,
    published_after: str | None = None,
    published_before: str | None = None,
    db: StateDB | None = None,
) -> list[ArticleForAggregation]:
    if published_date is not None:
        datetime.strptime(published_date, "%Y-%m-%d")
    if published_after is not None:
        _validate_iso_timestamp(published_after)
    if published_before is not None:
        _validate_iso_timestamp(published_before)
    if published_date is not None and (published_after is not None or published_before is not None):
        raise ValueError("published_date cannot be combined with published_after or published_before")
    close_db = db is None
    state = db or StateDB()
    try:
        params: list[Any] = []
        where = "event_id IS NULL AND is_filtered = 0"
        if published_date is not None:
            where += " AND substr(published_at, 1, 10) = ?"
            params.append(published_date)
        if published_after is not None:
            where += " AND published_at >= ?"
            params.append(published_after)
        if published_before is not None:
            where += " AND published_at < ?"
            params.append(published_before)
        limit_clause = ""
        if limit is not None:
            limit_clause = "LIMIT ?"
            params.append(limit)
        rows = state.conn.execute(
            f"""
            SELECT article_id, source_id, source_name, headline, summary, published_at, article_path
            FROM articles
            WHERE {where}
            ORDER BY published_at DESC, fetched_at DESC
            {limit_clause}
            """,
            params,
        ).fetchall()
        return [
            ArticleForAggregation(
                article_id=row["article_id"],
                source_id=row["source_id"],
                source_name=row["source_name"],
                headline=row["headline"],
                summary=row["summary"],
                published_at=row["published_at"],
                article_path=row["article_path"],
                event_id=None,
                **_load_digest_fields(row["article_path"]),
            )
            for row in rows
        ]
    finally:
        if close_db:
            state.close()


def load_window_articles(
    *,
    window_start: str,
    window_end: str,
    min_category_impact: float | None = None,
    mark_filtered: bool = True,
    db: StateDB | None = None,
) -> list[ArticleForAggregation]:
    _validate_iso_timestamp(window_start)
    _validate_iso_timestamp(window_end)
    close_db = db is None
    state = db or StateDB()
    try:
        rows = state.conn.execute(
            """
            SELECT article_id, source_id, source_name, headline, summary, published_at, article_path, event_id
            FROM articles
            WHERE published_at >= ? AND published_at < ?
              AND is_filtered = 0
            ORDER BY published_at DESC, fetched_at DESC
            """,
            (window_start, window_end),
        ).fetchall()
        articles = [
            ArticleForAggregation(
                article_id=row["article_id"],
                source_id=row["source_id"],
                source_name=row["source_name"],
                headline=row["headline"],
                summary=row["summary"],
                published_at=row["published_at"],
                article_path=row["article_path"],
                event_id=row["event_id"],
                **_load_digest_fields(row["article_path"]),
            )
            for row in rows
        ]
        if min_category_impact is None:
            return articles
        included = []
        for article in articles:
            exclusion_reason = _article_aggregation_exclusion_reason(article)
            if exclusion_reason:
                if mark_filtered:
                    state.update_article_aggregation_status(
                        article.article_id,
                        status=f"filtered_{exclusion_reason}",
                        reason=exclusion_reason,
                    )
                continue
            score = _article_category_impact(article)
            if score is None:
                continue
            if score < min_category_impact:
                if mark_filtered:
                    state.update_article_aggregation_status(
                        article.article_id,
                        status="filtered_low_impact",
                        reason=f"category_impact {score:.3f} below {min_category_impact:.3f}",
                    )
                continue
            if mark_filtered:
                state.set_article_aggregation_pending_if_unassigned(article.article_id)
            included.append(article)
        return included
    finally:
        if close_db:
            state.close()


def run_grouping_experiment(
    *,
    limit: int | None = 40,
    published_date: str | None = None,
    published_after: str | None = None,
    published_before: str | None = None,
    modes: Sequence[str] = GROUPING_MODES,
    client: JsonGenerator | None = None,
    progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    selected_modes = tuple(_normalize_mode(mode) for mode in modes)
    articles = load_unprocessed_articles(
        limit=limit,
        published_date=published_date,
        published_after=published_after,
        published_before=published_before,
    )
    if not articles:
        return {"article_count": 0, "modes": {}, "comparison": None}
    generator = client or GeminiClient()

    results_by_mode: dict[str, dict[str, Any]] = {}
    for mode in selected_modes:
        if progress:
            progress(f"aggregation experiment: running {mode} with {len(articles)} articles")
        result = group_articles_with_gemini(articles, mode=mode, client=generator)
        results_by_mode[mode] = result
        if progress:
            progress(
                "aggregation experiment: "
                f"{mode} produced {result['group_count']} groups, "
                f"{result['singleton_count']} singletons in {result['elapsed_ms']} ms"
            )

    comparison = None
    if set(selected_modes) == set(GROUPING_MODES):
        comparison = compare_groupings(
            results_by_mode["titles"]["groups"],
            results_by_mode["titles_summaries"]["groups"],
            len(articles),
        )
    return {
        "prompt_version": AGGREGATION_EXPERIMENT_PROMPT_VERSION,
        "model": generator.model,
        "article_count": len(articles),
        "published_date": published_date,
        "published_after": published_after,
        "published_before": published_before,
        "limit": limit,
        "articles": [_article_preview(article, index) for index, article in enumerate(articles)],
        "modes": results_by_mode,
        "comparison": comparison,
    }


def plan_sliding_windows(
    *,
    range_start: str,
    range_end: str,
    window_hours: int = 6,
    step_hours: int | None = None,
    overlap_hours: int = 1,
    db: StateDB | None = None,
    rerun_latest_completed: bool = True,
    force: bool = False,
) -> list[AggregationWindow]:
    if window_hours <= 0:
        raise ValueError("window_hours must be positive")
    if step_hours is None:
        step_hours = window_hours
    if step_hours <= 0:
        raise ValueError("step_hours must be positive")
    if overlap_hours < 0:
        raise ValueError("overlap_hours cannot be negative")
    start_dt = _parse_iso_timestamp(range_start)
    end_dt = _parse_iso_timestamp(range_end)
    if end_dt <= start_dt:
        raise ValueError("range_end must be after range_start")

    close_db = db is None
    state = db or StateDB()
    try:
        latest_completed = state.latest_completed_aggregation_window() if rerun_latest_completed else None

        def should_skip_window(window: AggregationWindow) -> bool:
            if force:
                return False
            status = state.aggregation_window_status(window.window_start, window.window_end)
            if status != "completed":
                return False
            if latest_completed is not None and (window.window_start, window.window_end) == latest_completed:
                return False
            return state.unassigned_article_count_in_window(window.window_start, window.window_end) == 0

        windows: list[AggregationWindow] = []
        current = start_dt
        window_delta = timedelta(hours=window_hours + overlap_hours)
        step_delta = timedelta(hours=step_hours)
        while current < end_dt:
            window_end_dt = current + window_delta
            window = AggregationWindow(
                window_start=_format_iso_timestamp(current),
                window_end=_format_iso_timestamp(window_end_dt),
            )
            current += step_delta
            if not should_skip_window(window):
                windows.append(window)

        return windows
    finally:
        if close_db:
            state.close()


def aggregate_once(
    *,
    range_start: str | None = None,
    range_end: str | None = None,
    limit_windows: int | None = None,
    dry_run: bool = False,
    client: JsonGenerator | None = None,
    progress: Callable[[str], None] | None = None,
    force: bool = False,
) -> dict[str, Any]:
    if (range_start is None) != (range_end is None):
        raise ValueError("range_start and range_end must be provided together or both omitted")
    config = load_pipeline_config()
    window_hours = int(config.aggregation.get("window_hours", 6))
    step_hours = int(config.aggregation.get("window_step_hours", window_hours))
    overlap_hours = int(config.aggregation.get("window_overlap_hours", 1))
    min_category_impact = float(config.aggregation.get("min_category_impact", 0.25))
    lock_timeout = timedelta(minutes=int(config.pipeline.get("watchdog_timeout_minutes", 30)))
    run_id = f"aggregation-{uuid.uuid4().hex}"
    state = StateDB()
    generator = client or GeminiClient()
    stats: dict[str, Any] = {
        "windows_planned": 0,
        "windows_processed": 0,
        "windows_failed": 0,
        "windows_partial_failed": 0,
        "articles_seen": 0,
        "groups_seen": 0,
        "events_created": 0,
        "events_updated": 0,
        "article_assignments": 0,
        "newsworthiness_scored": 0,
        "newsworthiness_fallbacks": 0,
        "min_category_impact": min_category_impact,
        "window_hours": window_hours,
        "window_overlap_hours": overlap_hours,
        "window_step_hours": step_hours,
        "dry_run": dry_run,
        "force": force,
    }
    try:
        with PipelineLock(LOCK_PATH, lock_timeout, run_id=run_id):
            recovered = 0 if dry_run else state.fail_stale_running_aggregation_windows()
            if recovered and progress:
                progress(f"aggregate: marked {recovered} stale running window(s) as failed")
            stats["stale_windows_recovered"] = recovered
            feeds_by_source = {feed.source_id: feed for feed in load_feeds(enabled_only=False)}
            started_run = False
            if not dry_run:
                state.start_run(run_id, "aggregation")
                started_run = True
            status = "success"
            try:
                if range_start is None:
                    bounds = state.article_time_bounds(unassigned_only=True)
                    if not bounds:
                        return stats
                    from datetime import time

                    from pipeline.util import utc_now

                    ref = utc_now()
                    today_start = datetime.combine(ref.date(), time.min, tzinfo=UTC)
                    limit_dt = today_start - timedelta(days=1)

                    bounds_start = _parse_iso_timestamp(bounds[0])
                    bounds_end = _parse_iso_timestamp(bounds[1])

                    if bounds_end < limit_dt:
                        if progress:
                            progress("aggregate: no unassigned articles in the current and previous day")
                        return stats

                    if bounds_start < limit_dt:
                        bounds_start = limit_dt

                    range_start = _format_iso_timestamp(_floor_utc_interval(bounds_start, step_hours))
                    range_end = _format_iso_timestamp(
                        _floor_utc_interval(bounds_end, step_hours) + timedelta(hours=step_hours)
                    )

                windows = plan_sliding_windows(
                    range_start=range_start,
                    range_end=range_end,
                    window_hours=window_hours,
                    step_hours=step_hours,
                    overlap_hours=overlap_hours,
                    db=state,
                    force=force,
                )
                if limit_windows is not None:
                    windows = windows[:limit_windows]
                stats["windows_planned"] = len(windows)
                if progress:
                    progress(f"aggregate: planned {len(windows)} windows")

                for window in windows:
                    if progress:
                        progress(f"aggregate: processing {window.window_start} to {window.window_end}")
                    try:
                        if not dry_run:
                            state.start_aggregation_window(
                                window_start=window.window_start,
                                window_end=window.window_end,
                                run_id=run_id,
                                prompt_version=AGGREGATION_PROMPT_VERSION,
                                model=generator.model,
                            )
                        articles = load_window_articles(
                            window_start=window.window_start,
                            window_end=window.window_end,
                            min_category_impact=min_category_impact,
                            mark_filtered=not dry_run,
                            db=state,
                        )
                        if not articles:
                            if not dry_run:
                                state.finish_aggregation_window(
                                    window_start=window.window_start,
                                    window_end=window.window_end,
                                    status="completed",
                                    article_count=0,
                                    stats={"groups": 0},
                                )
                            stats["windows_processed"] += 1
                            continue

                        total_articles_in_window = len(articles)
                        category_batches = _category_batches_for_articles(articles, feeds_by_source)

                        # Pre-fetch active events once per window, partition by category in Python.
                        since_dt = datetime.now(UTC) - timedelta(hours=48)
                        since = since_dt.isoformat().replace("+00:00", "Z")
                        all_active_rows = state.conn.execute(
                            """
                            SELECT event_id, title, category, updated_at
                            FROM events
                            WHERE status = 'active'
                              AND updated_at >= ?
                            ORDER BY updated_at DESC
                            """,
                            (since,),
                        ).fetchall()
                        active_rows_by_category: dict[str, list[dict[str, Any]]] = {}
                        for row in all_active_rows:
                            active_rows_by_category.setdefault(row["category"], []).append(dict(row))

                        window_group_count = 0
                        window_singleton_count = 0
                        window_multi_article_group_count = 0
                        window_validation_attempts = 0
                        window_elapsed_ms = 0
                        window_prompt_tokens = 0
                        window_candidates_tokens = 0

                        window_news_scored = 0
                        window_news_fallback_count = 0
                        window_news_elapsed_ms = 0
                        window_news_prompt_tokens = 0
                        window_news_candidates_tokens = 0

                        window_events_created = 0
                        window_events_updated = 0
                        window_article_assignments = 0

                        group_errors: list[dict[str, Any]] = []
                        non_empty_group_count = len(category_batches)
                        processed_articles_count = 0

                        for batch in category_batches:
                            group_articles = batch["articles"]

                            try:
                                # Tightened candidate filter: an active event is a candidate only if
                                # its title shares at least 2 non-stopword words with at least one
                                # individual article headline (or shares the single word if the event
                                # title only has one). This avoids the union-of-all-words false-positive
                                # mode that pulled ~all active events into the prompt on large windows.
                                article_word_sets = [
                                    set(re.findall(r"[A-Za-z0-9]+", art.headline.lower()))
                                    - _KEYWORD_STOPWORDS
                                    for art in group_articles
                                ]
                                candidates: list[dict[str, Any]] = []
                                for cat in _candidate_categories_for_group(batch["categories"]):
                                    for ev in active_rows_by_category.get(cat, []):
                                        title_words = (
                                            set(re.findall(r"[A-Za-z0-9]+", ev["title"].lower()))
                                            - _KEYWORD_STOPWORDS
                                        )
                                        if not title_words:
                                            continue
                                        required = 1 if len(title_words) == 1 else 2
                                        if any(
                                            len(title_words & art_words) >= required
                                            for art_words in article_word_sets
                                        ):
                                            candidates.append(ev)

                                active_events = filter_active_events_with_llm(
                                    articles=group_articles,
                                    active_events=candidates,
                                    client=generator,
                                    run_id=run_id,
                                    state=state if not dry_run else None,
                                )

                                result = group_articles_with_gemini(
                                    group_articles,
                                    mode="titles_summaries",
                                    client=generator,
                                    active_events=active_events,
                                )
                                if not dry_run and result.get("usage"):
                                    state.record_llm_usage(
                                        run_id=run_id,
                                        stage="aggregation",
                                        model=generator.model,
                                        prompt_version=AGGREGATION_PROMPT_VERSION,
                                        input_tokens=result["usage"].get("promptTokenCount"),
                                        output_tokens=result["usage"].get("candidatesTokenCount"),
                                    )
                                    if result["usage"].get("promptTokenCount"):
                                        window_prompt_tokens += result["usage"]["promptTokenCount"]
                                    if result["usage"].get("candidatesTokenCount"):
                                        window_candidates_tokens += result["usage"]["candidatesTokenCount"]

                                window_group_count += result["group_count"]
                                window_singleton_count += result["singleton_count"]
                                window_multi_article_group_count += result["multi_article_group_count"]
                                window_validation_attempts += result["validation_attempts"]
                                val_elapsed = result.get("elapsed_ms")
                                if val_elapsed is not None:
                                    window_elapsed_ms += val_elapsed

                                groups_to_score = []
                                scores_by_group_index: dict[int, dict[str, Any]] = {}
                                for group in result["groups"]:
                                    grp_arts = [group_articles[idx] for idx in group["article_indexes"]]
                                    event_id = _event_id_for_group(grp_arts)
                                    reused_score = None
                                    if event_id:
                                        event_path = EVENT_DIR / f"{event_id}.json"
                                        existing = _read_event(event_path)
                                        if existing and existing.get("newsworthiness"):
                                            existing_ids = set(existing.get("article_ids", []))
                                            current_ids = set(art.article_id for art in grp_arts)
                                            if existing_ids == current_ids:
                                                reused_score = existing["newsworthiness"]
                                    if reused_score:
                                        scores_by_group_index[int(group["group_index"])] = reused_score
                                    else:
                                        groups_to_score.append(group)

                                if groups_to_score:
                                    scores_result = score_groups_newsworthiness(
                                        articles=group_articles,
                                        groups=groups_to_score,
                                        client=generator,
                                        feeds_by_source=feeds_by_source,
                                    )
                                    if not dry_run and scores_result.get("usage"):
                                        state.record_llm_usage(
                                            run_id=run_id,
                                            stage="newsworthiness",
                                            model=generator.model,
                                            prompt_version=NEWSWORTHINESS_PROMPT_VERSION,
                                            input_tokens=scores_result["usage"].get("promptTokenCount"),
                                            output_tokens=scores_result["usage"].get("candidatesTokenCount"),
                                        )
                                        p_tokens = scores_result["usage"].get("promptTokenCount")
                                        if p_tokens:
                                            window_news_prompt_tokens += p_tokens
                                        c_tokens = scores_result["usage"].get("candidatesTokenCount")
                                        if c_tokens:
                                            window_news_candidates_tokens += c_tokens
                                    api_scores = scores_result["scores_by_group_index"]
                                    fallback_count = scores_result["fallback_count"]
                                    elapsed_ms = scores_result.get("elapsed_ms")
                                else:
                                    api_scores = {}
                                    fallback_count = 0
                                    elapsed_ms = 0

                                merged_scores = {**scores_by_group_index, **api_scores}
                                window_news_scored += len(merged_scores)
                                window_news_fallback_count += fallback_count
                                if elapsed_ms is not None:
                                    window_news_elapsed_ms += elapsed_ms

                                if not dry_run:
                                    applied = apply_grouping_result(
                                        articles=group_articles,
                                        groups=result["groups"],
                                        state=state,
                                        scores_by_group_index=merged_scores,
                                        article_classifications=result.get("article_classifications"),
                                        feeds_by_source=feeds_by_source,
                                        run_id=run_id,
                                        progress=progress,
                                    )
                                    window_events_created += applied["events_created"]
                                    window_events_updated += applied["events_updated"]
                                    window_article_assignments += applied["article_assignments"]

                                processed_articles_count += len(group_articles)
                            except Exception as group_exc:
                                group_errors.append({
                                    "category_group": batch["name"],
                                    "article_count": len(group_articles),
                                    "error": str(group_exc),
                                })
                                if progress:
                                    progress(
                                        f"aggregate: window {window.window_start} group "
                                        f"{batch['name']} ({len(group_articles)} articles) failed: {group_exc}"
                                    )
                                if not dry_run:
                                    try:
                                        state.record_error(
                                            run_id,
                                            "aggregation",
                                            "category_group",
                                            f"{window.window_start}_{window.window_end}_{batch['name']}",
                                            None,
                                            group_exc,
                                        )
                                    except Exception:
                                        pass

                        stats["articles_seen"] += processed_articles_count
                        stats["groups_seen"] += window_group_count
                        stats["newsworthiness_scored"] += window_news_scored
                        stats["newsworthiness_fallbacks"] += window_news_fallback_count

                        all_groups_failed = (
                            non_empty_group_count > 0 and len(group_errors) >= non_empty_group_count
                        )

                        window_stats = {
                            "article_count": total_articles_in_window,
                            "processed_article_count": processed_articles_count,
                            "category_batch_count": non_empty_group_count,
                            "group_count": window_group_count,
                            "singleton_count": window_singleton_count,
                            "multi_article_group_count": window_multi_article_group_count,
                            "validation_attempts": window_validation_attempts,
                            "elapsed_ms": window_elapsed_ms if window_elapsed_ms > 0 else None,
                            "usage": {
                                "promptTokenCount": window_prompt_tokens,
                                "candidatesTokenCount": window_candidates_tokens,
                            },
                            "newsworthiness": {
                                "scored": window_news_scored,
                                "fallback_count": window_news_fallback_count,
                                "elapsed_ms": window_news_elapsed_ms if window_news_elapsed_ms > 0 else None,
                                "usage": {
                                    "promptTokenCount": window_news_prompt_tokens,
                                    "candidatesTokenCount": window_news_candidates_tokens,
                                },
                            },
                        }
                        if group_errors:
                            window_stats["group_errors"] = group_errors

                        if not dry_run:
                            stats["events_created"] += window_events_created
                            stats["events_updated"] += window_events_updated
                            stats["article_assignments"] += window_article_assignments
                            window_stats.update({
                                "events_created": window_events_created,
                                "events_updated": window_events_updated,
                                "article_assignments": window_article_assignments,
                            })
                            # Mark the window failed only if every non-empty category group failed.
                            # Partial failures use "partial_failure" so the window is rerun next pass
                            # (idempotent: already-assigned groups are no-ops).
                            if all_groups_failed:
                                final_status = "failed"
                            elif group_errors:
                                final_status = "partial_failure"
                            else:
                                final_status = "completed"
                            state.finish_aggregation_window(
                                window_start=window.window_start,
                                window_end=window.window_end,
                                status=final_status,
                                article_count=processed_articles_count,
                                stats=window_stats,
                            )

                        if all_groups_failed:
                            stats["windows_failed"] += 1
                        else:
                            stats["windows_processed"] += 1
                            if group_errors:
                                stats["windows_partial_failed"] += 1

                        if progress:
                            if group_errors and not all_groups_failed:
                                progress(
                                    "aggregate: "
                                    f"{window.window_start} grouped {processed_articles_count}/"
                                    f"{total_articles_in_window} articles into {window_group_count} groups "
                                    f"({len(group_errors)} category group(s) failed)"
                                )
                            elif all_groups_failed:
                                progress(
                                    f"aggregate: window {window.window_start} to "
                                    f"{window.window_end} failed: all category groups failed"
                                )
                            else:
                                progress(
                                    "aggregate: "
                                    f"{window.window_start} grouped {processed_articles_count} articles "
                                    f"into {window_group_count} groups"
                                )
                    except Exception as exc:
                        stats["windows_failed"] += 1
                        if progress:
                            progress(f"aggregate: window {window.window_start} to {window.window_end} failed: {exc}")
                        if not dry_run:
                            state.record_error(
                                run_id,
                                "aggregation",
                                "window",
                                f"{window.window_start}_{window.window_end}",
                                None,
                                exc,
                            )
                        if not dry_run:
                            state.finish_aggregation_window(
                                window_start=window.window_start,
                                window_end=window.window_end,
                                status="failed",
                                article_count=0,
                                stats={"error": str(exc)},
                            )
                if not dry_run:
                    try:
                        deduplicate_active_events_llm(
                            state=state,
                            client=generator,
                            feeds_by_source=feeds_by_source,
                            progress=progress,
                            run_id=run_id,
                        )
                    except Exception as exc:
                        if progress:
                            progress(f"aggregate: post-aggregation deduplication failed: {exc}")
                        state.record_error(
                            run_id,
                            "aggregation",
                            "deduplication_run",
                            None,
                            None,
                            exc,
                        )

                if stats["windows_failed"]:
                    status = "failed" if stats["windows_processed"] == 0 else "partial_failure"
                elif stats["windows_partial_failed"]:
                    status = "partial_failure"
            except Exception:
                status = "failed"
                raise
            finally:
                if started_run:
                    state.finish_run(run_id, status, stats)
        return stats
    finally:
        state.close()


def apply_grouping_result(
    *,
    articles: Sequence[ArticleForAggregation],
    groups: Sequence[dict[str, Any]],
    state: StateDB,
    scores_by_group_index: dict[int, dict[str, Any]] | None = None,
    article_classifications: dict[int, dict[str, Any]] | None = None,
    feeds_by_source: dict[str, Any] | None = None,
    run_id: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> dict[str, int]:
    if article_classifications:
        for idx, classification in article_classifications.items():
            article = articles[idx]
            content_type = classification.get("content_type", "unknown")
            state.update_article_content_type(article.article_id, content_type)
            abs_path = PROJECT_ROOT / article.article_path
            if abs_path.exists():
                try:
                    with abs_path.open("r", encoding="utf-8") as f:
                        data = json.load(f)
                    data["content_type"] = content_type
                    atomic_write_json(abs_path, data)
                except Exception as exc:
                    if progress:
                        progress(f"aggregate: content_type backfill failed for {article.article_id}: {exc}")
                    if run_id:
                        try:
                            state.record_error(
                                run_id,
                                "aggregation",
                                "article",
                                article.article_id,
                                article.source_id,
                                exc,
                            )
                        except Exception:
                            pass

    if feeds_by_source is None:
        feeds_by_source = {feed.source_id: feed for feed in load_feeds(enabled_only=False)}
    stats = {"events_created": 0, "events_updated": 0, "article_assignments": 0}
    for fallback_group_index, group in enumerate(groups):
        group_articles = [articles[index] for index in group["article_indexes"]]
        group_index = int(group.get("group_index", fallback_group_index))

        is_all_opinions = False
        if article_classifications:
            is_all_opinions = all(
                article_classifications.get(index, {}).get("content_type") == "opinion"
                for index in group["article_indexes"]
            )

        llm_event_id = _normalize_existing_event_id(group.get("existing_event_id"))
        # A second line of defense for direct callers/tests: group_articles_with_gemini
        # validates that LLM-supplied IDs came from the prompt's active_events list.
        # Without this guard, a caller could still create an event with a model-chosen ID.
        if llm_event_id and not state.event_exists(llm_event_id):
            if progress:
                progress(
                    f"aggregate: ignoring hallucinated existing_event_id={llm_event_id!r} "
                    f"(no such event in state)"
                )
            llm_event_id = None
        existing_event_ids_set = set(art.event_id for art in group_articles if art.event_id)
        if llm_event_id:
            existing_event_ids_set.add(llm_event_id)
        existing_event_ids = sorted(existing_event_ids_set)

        if not existing_event_ids:
            if is_all_opinions:
                for article in group_articles:
                    state.update_article_aggregation_status(
                        article.article_id,
                        status="filtered_standalone_opinion",
                        reason="standalone_opinion",
                    )
                continue
            event_id = _generate_new_event_id(group_articles, state)
            winner_event_ids = []
            created = True
            existing = None
        else:
            winner_id = _event_id_for_group(group_articles) or llm_event_id
            assert winner_id is not None
            event_id = winner_id
            winner_event_ids = [eid for eid in existing_event_ids if eid != winner_id]
            created = False
            event_path = EVENT_DIR / f"{event_id}.json"
            existing = _read_event(event_path)

        merged_article_ids = set()
        if existing:
            merged_article_ids.update(existing.get("article_ids", []))

        loser_paths: list[Path] = []
        for other_id in winner_event_ids:
            other_path = EVENT_DIR / f"{other_id}.json"
            other_event = _read_event(other_path)
            if other_event:
                merged_article_ids.update(other_event.get("article_ids", []))
            loser_paths.append(other_path)

        if winner_event_ids:
            state.merge_events_into(winner_event_ids, event_id)
            for other_path in loser_paths:
                try:
                    other_path.unlink(missing_ok=True)
                except Exception:
                    pass

        merged_article_ids.update(art.article_id for art in group_articles)
        event_path = EVENT_DIR / f"{event_id}.json"

        if existing is None:
            existing_payload = None
        else:
            existing_payload = dict(existing)
            existing_payload["article_ids"] = sorted(merged_article_ids)

        event_payload = _build_event_payload(
            event_id=event_id,
            event_path=event_path,
            articles=group_articles,
            existing=existing_payload,
            feeds_by_source=feeds_by_source,
            category_override=group.get("category")
            or _category_from_classifications(group["article_indexes"], article_classifications or {}),
            newsworthiness=(scores_by_group_index or {}).get(group_index),
        )
        event_payload["article_ids"] = sorted(merged_article_ids)
        event_payload["article_count"] = len(merged_article_ids)

        atomic_write_json(event_path, event_payload)
        state.upsert_event(event_payload, event_path)

        assignments = state.assign_articles_to_event([article.article_id for article in group_articles], event_id)
        stats["article_assignments"] += max(assignments, 0)
        if created:
            stats["events_created"] += 1
        else:
            stats["events_updated"] += 1
    return stats


def score_groups_newsworthiness(
    *,
    articles: Sequence[ArticleForAggregation],
    groups: Sequence[dict[str, Any]],
    client: JsonGenerator | None = None,
    feeds_by_source: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if feeds_by_source is None:
        feeds_by_source = {feed.source_id: feed for feed in load_feeds(enabled_only=False)}
    baseline = {
        int(group["group_index"]): _baseline_newsworthiness(
            [articles[index] for index in group["article_indexes"]],
            source_count=len(set(group["sources"])),
            feeds_by_source=feeds_by_source,
        )
        for group in groups
    }
    if not client or not groups:
        return {
            "scores_by_group_index": baseline,
            "fallback_count": len(baseline),
            "elapsed_ms": None,
            "usage": {},
        }
    groups_for_model = [
        group for group in groups if baseline[int(group["group_index"])].get("model") != "deterministic-digest-impact"
    ]
    if not groups_for_model:
        return {
            "scores_by_group_index": baseline,
            "fallback_count": 0,
            "elapsed_ms": None,
            "usage": {},
        }

    try:
        result = client.generate_json(
            system_instruction=(
                "You score story clusters for editorial ranking. Return only valid JSON matching the schema."
            ),
            prompt=_build_newsworthiness_prompt(articles, groups_for_model, feeds_by_source=feeds_by_source),
            response_schema=_newsworthiness_response_schema(),
            temperature=0,
        )
        model_scores = validate_newsworthiness_response(
            result.payload,
            valid_group_indexes={int(group["group_index"]) for group in groups_for_model},
        )
    except Exception:
        return {
            "scores_by_group_index": baseline,
            "fallback_count": len(groups_for_model),
            "elapsed_ms": None,
            "usage": {},
        }

    merged = dict(baseline)
    for group_index, score in model_scores.items():
        merged[group_index] = {
            **score,
            "scored_at": isoformat_z(),
            "model": client.model,
            "prompt_version": NEWSWORTHINESS_PROMPT_VERSION,
            "baseline": baseline[group_index],
        }
    return {
        "scores_by_group_index": merged,
        "fallback_count": len({int(group["group_index"]) for group in groups_for_model} - set(model_scores)),
        "elapsed_ms": result.elapsed_ms,
        "usage": result.usage,
    }


def group_articles_with_gemini(
    articles: Sequence[ArticleForAggregation],
    *,
    mode: str,
    client: JsonGenerator,
    active_events: Sequence[dict[str, Any]] = (),
) -> dict[str, Any]:
    mode = _normalize_mode(mode)
    if mode not in GROUPING_MODES:
        raise ValueError(f"unsupported grouping mode: {mode}")
    valid_categories = load_categories()
    valid_existing_event_ids = {
        str(event.get("event_id", "")).strip() for event in active_events if str(event.get("event_id", "")).strip()
    }
    active_events_by_id = {
        str(event.get("event_id", "")).strip(): event
        for event in active_events
        if str(event.get("event_id", "")).strip()
    }
    base_prompt = _build_grouping_prompt(
        articles, mode=mode, valid_categories=valid_categories, active_events=active_events
    )
    last_error: ValueError | None = None
    for attempt in range(2):
        prompt = base_prompt
        if attempt:
            prompt += (
                "\n\nYour previous response failed validation: "
                f"{last_error}. Retry from scratch. Audit the final JSON so every integer from 0 through "
                f"{len(articles) - 1} appears exactly once in both 'articles' list and the 'groups' lists."
            )
        result = client.generate_json(
            system_instruction=(
                "You group news articles by the same underlying real-world event "
                "and classify their content type and category. "
                "Return only valid JSON matching the schema. Use article indexes only."
            ),
            prompt=prompt,
            response_schema=_grouping_response_schema(valid_categories),
            temperature=0.2 if attempt else 0,
        )
        try:
            groups, classifications = validate_grouping_response(
                result.payload,
                article_count=len(articles),
                valid_categories=valid_categories,
                valid_existing_event_ids=valid_existing_event_ids,
            )
            groups = _split_weakly_connected_groups(
                groups,
                articles,
                active_events_by_id=active_events_by_id,
            )
            break
        except ValueError as exc:
            last_error = exc
            if attempt == 1:
                raise
    return {
        "mode": mode,
        "validation_attempts": attempt + 1,
        "group_count": len(groups),
        "singleton_count": sum(1 for group in groups if len(group["article_indexes"]) == 1),
        "multi_article_group_count": sum(1 for group in groups if len(group["article_indexes"]) > 1),
        "groups": [
            {
                "group_index": index,
                "article_indexes": group["article_indexes"],
                "category": _category_from_classifications(group["article_indexes"], classifications),
                "headlines": [articles[i].headline for i in group["article_indexes"]],
                "sources": [articles[i].source_name for i in group["article_indexes"]],
                **({"existing_event_id": group["existing_event_id"]} if "existing_event_id" in group else {}),
            }
            for index, group in enumerate(groups)
        ],
        "article_classifications": classifications,
        "elapsed_ms": result.elapsed_ms,
        "usage": result.usage,
    }


def validate_grouping_response(
    payload: dict[str, Any],
    *,
    article_count: int,
    valid_categories: list[str],
    valid_existing_event_ids: set[str] | None = None,
) -> tuple[list[dict[str, Any]], dict[int, dict[str, Any]]]:
    raw_groups = payload.get("groups")
    if not isinstance(raw_groups, list):
        raise ValueError("grouping response must contain a groups list")

    raw_articles = payload.get("articles")
    if not isinstance(raw_articles, list):
        raise ValueError("grouping response must contain an articles list")
    if len(raw_articles) != article_count:
        raise ValueError(
            f"articles classification count ({len(raw_articles)}) must match article count ({article_count})"
        )

    classifications: dict[int, dict[str, Any]] = {}
    for item in raw_articles:
        if not isinstance(item, dict):
            raise ValueError("each article classification must be an object")
        idx = item.get("article_index")
        if not isinstance(idx, int) or idx < 0 or idx >= article_count:
            raise ValueError(f"invalid or out-of-range article_index: {idx}")
        if idx in classifications:
            raise ValueError(f"duplicate article_index in classifications: {idx}")

        content_type = item.get("content_type")
        if content_type not in ("news", "opinion", "analysis", "review", "unknown"):
            raise ValueError(f"invalid content_type: {content_type}")

        category = item.get("category")
        if category not in valid_categories:
            raise ValueError(f"invalid category: {category}")

        classifications[idx] = {
            "content_type": content_type,
            "category": category,
        }

    if len(classifications) != article_count:
        missing_idxs = sorted(set(range(article_count)) - set(classifications.keys()))
        raise ValueError(f"missing classification for article indexes: {missing_idxs}")

    seen: set[int] = set()
    groups: list[dict[str, Any]] = []
    for raw_group in raw_groups:
        if not isinstance(raw_group, dict):
            raise ValueError("each group must be an object")
        indexes = raw_group.get("article_indexes")
        if not isinstance(indexes, list) or not indexes:
            raise ValueError("each group must contain a non-empty article_indexes list")
        cleaned: list[int] = []
        for value in indexes:
            if not isinstance(value, int):
                raise ValueError("article indexes must be integers")
            if value < 0 or value >= article_count:
                raise ValueError(f"article index out of range: {value}")
            if value in seen:
                raise ValueError(f"article index appears in multiple groups: {value}")
            seen.add(value)
            cleaned.append(value)

        existing_event_id = _normalize_existing_event_id(raw_group.get("existing_event_id"))
        group_data = {"article_indexes": sorted(cleaned)}
        if existing_event_id:
            if valid_existing_event_ids is not None and existing_event_id not in valid_existing_event_ids:
                raise ValueError(f"existing_event_id was not offered in active events: {existing_event_id}")
            group_data["existing_event_id"] = existing_event_id
        groups.append(group_data)

    missing = sorted(set(range(article_count)) - seen)
    if missing:
        raise ValueError(f"grouping response omitted article indexes in groups: {missing}")
    return sorted(groups, key=lambda group: group["article_indexes"][0]), classifications


def _normalize_existing_event_id(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if not normalized or normalized.lower() in NULL_EXISTING_EVENT_ID_VALUES:
        return None
    return normalized


def _split_weakly_connected_groups(
    groups: Sequence[dict[str, Any]],
    articles: Sequence[ArticleForAggregation],
    *,
    active_events_by_id: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    split_groups: list[dict[str, Any]] = []
    for group in groups:
        indexes = group["article_indexes"]
        existing_event_id = group.get("existing_event_id")
        if len(indexes) <= 1:
            split_groups.append(group)
            continue

        components = _headline_cohesion_components(indexes, articles)
        if len(components) <= 1:
            if existing_event_id and not _component_matches_existing_event(
                indexes,
                articles,
                existing_event_id,
                active_events_by_id or {},
            ):
                group = {key: value for key, value in group.items() if key != "existing_event_id"}
            split_groups.append(group)
            continue

        for component in components:
            split_group = {"article_indexes": component}
            if existing_event_id and _component_matches_existing_event(
                component,
                articles,
                existing_event_id,
                active_events_by_id or {},
            ):
                split_group["existing_event_id"] = existing_event_id
            split_groups.append(split_group)
    return sorted(split_groups, key=lambda group: group["article_indexes"][0])


def _component_matches_existing_event(
    indexes: Sequence[int],
    articles: Sequence[ArticleForAggregation],
    existing_event_id: str,
    active_events_by_id: dict[str, dict[str, Any]],
) -> bool:
    if any(articles[index].event_id == existing_event_id for index in indexes):
        return True
    event = active_events_by_id.get(existing_event_id)
    if not event:
        return True
    event_words = _headline_word_set(str(event.get("title", "")))
    if not event_words:
        return False
    return any(len(event_words & _headline_word_set(articles[index].headline)) >= 2 for index in indexes)


def _headline_cohesion_components(
    indexes: Sequence[int],
    articles: Sequence[ArticleForAggregation],
) -> list[list[int]]:
    word_sets = {
        index: _headline_word_set(articles[index].headline)
        for index in indexes
    }
    neighbors = {index: set() for index in indexes}
    for left_pos, left in enumerate(indexes):
        for right in indexes[left_pos + 1 :]:
            if len(word_sets[left] & word_sets[right]) >= 2:
                neighbors[left].add(right)
                neighbors[right].add(left)

    components: list[list[int]] = []
    visited: set[int] = set()
    for index in indexes:
        if index in visited:
            continue
        stack = [index]
        component: list[int] = []
        visited.add(index)
        while stack:
            current = stack.pop()
            component.append(current)
            for neighbor in neighbors[current]:
                if neighbor in visited:
                    continue
                visited.add(neighbor)
                stack.append(neighbor)
        components.append(sorted(component))
    return sorted(components, key=lambda component: component[0])


def _headline_word_set(text: str) -> set[str]:
    return {
        word
        for word in re.findall(r"[A-Za-z0-9]+", text.lower())
        if len(word) >= 3 and word not in _KEYWORD_STOPWORDS
    }


def validate_newsworthiness_response(
    payload: dict[str, Any],
    *,
    valid_group_indexes: set[int],
) -> dict[int, dict[str, Any]]:
    raw_scores = payload.get("scores")
    if not isinstance(raw_scores, list):
        raise ValueError("newsworthiness response must contain a scores list")

    scores: dict[int, dict[str, Any]] = {}
    for raw_score in raw_scores:
        if not isinstance(raw_score, dict):
            raise ValueError("each newsworthiness score must be an object")
        group_index = raw_score.get("group_index")
        if not isinstance(group_index, int) or group_index not in valid_group_indexes:
            raise ValueError(f"newsworthiness group index invalid or out of range: {group_index}")
        if group_index in scores:
            raise ValueError(f"duplicate newsworthiness group index: {group_index}")
        global_score = _validated_score(raw_score.get("global_score"), "global_score")
        category_score = _validated_score(raw_score.get("category_score"), "category_score")
        rationale_codes = raw_score.get("rationale_codes", [])
        if not isinstance(rationale_codes, list):
            raise ValueError("rationale_codes must be a list")
        cleaned_codes: list[str] = []
        for code in rationale_codes[:8]:
            raw = str(code).strip()
            if not raw:
                continue
            normalized = sanitize_id(raw).lower()
            if not normalized or normalized == "unknown":
                continue
            cleaned_codes.append(normalized)
        scores[group_index] = {
            "global": global_score,
            "category": category_score,
            "rationale_codes": cleaned_codes,
        }
    return scores


def _validated_score(value: object, field: str) -> float:
    if not isinstance(value, int | float):
        raise ValueError(f"{field} must be numeric")
    if value < 0 or value > 1:
        raise ValueError(f"{field} must be between 0 and 1")
    return round(float(value), 3)


def compare_groupings(
    title_groups: Sequence[dict[str, Any]],
    summary_groups: Sequence[dict[str, Any]],
    article_count: int,
) -> dict[str, Any]:
    title_pairs = _paired_indexes(title_groups)
    summary_pairs = _paired_indexes(summary_groups)
    intersection = title_pairs & summary_pairs
    union = title_pairs | summary_pairs
    return {
        "title_group_count": len(title_groups),
        "titles_summaries_group_count": len(summary_groups),
        "title_multi_article_pairs": len(title_pairs),
        "titles_summaries_multi_article_pairs": len(summary_pairs),
        "shared_multi_article_pairs": len(intersection),
        "pair_jaccard": round(len(intersection) / len(union), 4) if union else 1.0,
        "pairs_only_with_summaries": sorted([list(pair) for pair in summary_pairs - title_pairs]),
        "pairs_only_with_titles": sorted([list(pair) for pair in title_pairs - summary_pairs]),
        "article_count": article_count,
    }


def write_experiment_result(result: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def default_experiment_output_path() -> Path:
    return PROJECT_ROOT / "data" / "staging" / "aggregation-experiments" / "latest.json"


def filter_active_events_with_llm(
    *,
    articles: Sequence[ArticleForAggregation],
    active_events: Sequence[dict[str, Any]],
    client: JsonGenerator,
    run_id: str | None = None,
    state: StateDB | None = None,
) -> list[dict[str, Any]]:
    if not articles or not active_events:
        return []

    prompt = _build_active_events_filter_prompt(articles, active_events)
    schema = _active_events_filter_response_schema()

    try:
        result = client.generate_json(
            system_instruction=(
                "You are an expert news editor. Identify which active events from the list "
                "represent the same news stories/threads as any of the current articles."
            ),
            prompt=prompt,
            response_schema=schema,
            temperature=0,
        )
        if state and run_id and result.usage:
            state.record_llm_usage(
                run_id=run_id,
                stage="active_events_filter",
                model=client.model,
                prompt_version="active-events-filter-v1",
                input_tokens=result.usage.get("promptTokenCount"),
                output_tokens=result.usage.get("candidatesTokenCount"),
            )
        matched_ids = set(result.payload.get("matched_event_ids", []))
        return [ev for ev in active_events if ev["event_id"] in matched_ids]
    except Exception:
        # On LLM error, skip proactive matching for this group rather than flooding the
        # grouping prompt with the full unfiltered candidate list. Post-aggregation
        # deduplication will still catch any cross-window duplicates.
        return []


def _build_active_events_filter_prompt(
    articles: Sequence[ArticleForAggregation],
    active_events: Sequence[dict[str, Any]],
) -> str:
    headlines = [art.headline for art in articles]
    event_list = [
        {"event_id": ev["event_id"], "title": ev["title"], "category": ev["category"]}
        for ev in active_events
    ]
    return (
        "We have a list of new article headlines in the current time window, "
        "and a list of active news events from the last 48 hours.\n"
        "Identify which active events from the list cover the same underlying "
        "news story/thread as any of the new articles.\n"
        "Be conservative: only match if an event is directly related to at least one article headline.\n"
        "Return a JSON object containing the matched active event IDs in the 'matched_event_ids' list.\n\n"
        f"New Articles:\n{json.dumps(headlines, ensure_ascii=False, separators=(',', ':'))}\n\n"
        f"Active Events:\n{json.dumps(event_list, ensure_ascii=False, separators=(',', ':'))}"
    )


def _active_events_filter_response_schema() -> dict[str, Any]:
    return {
        "type": "OBJECT",
        "properties": {
            "matched_event_ids": {
                "type": "ARRAY",
                "items": {"type": "STRING"}
            }
        },
        "required": ["matched_event_ids"]
    }


def _build_grouping_prompt(
    articles: Sequence[ArticleForAggregation],
    *,
    mode: str,
    valid_categories: list[str],
    active_events: Sequence[dict[str, Any]] = (),
) -> str:
    fields = "index, source, published_at, headline"
    if mode == "titles_summaries":
        fields += ", summary, optional key_facts"
    rows: list[dict[str, Any]] = []
    for index, article in enumerate(articles):
        row: dict[str, Any] = {
            "i": index,
            "source": article.source_name,
            "published_at": article.published_at,
            "headline": article.headline,
        }
        if mode == "titles_summaries":
            row["summary"] = _brief_summary(article)
            if article.digest_key_facts:
                row["key_facts"] = list(article.digest_key_facts[:6])
        rows.append(row)

    active_events_section = ""
    if active_events:
        active_events_list = [
            {"event_id": ev["event_id"], "title": ev["title"], "category": ev["category"]}
            for ev in active_events
        ]
        active_events_section = (
            "\n\nExisting Active Events (from the last 48 hours):\n"
            f"{json.dumps(active_events_list, ensure_ascii=False, separators=(',', ':'))}\n"
            "If any article in the input list belongs to one of these existing events, assign it to that event "
            "by returning its event_id in the 'existing_event_id' property of the group."
        )

    return (
        "Group articles into reader-facing story clusters for summarization, and classify their metadata.\n"
        "A story cluster may include multiple angles on the same developing news subject: the core report, "
        "updates, official statements, reactions, analysis, explainers, market impact, political criticism, "
        "and local/international framing. The editorial stage will separate those angles later.\n"
        "Be eager to group articles when a reader would expect one combined story page linking all sources, "
        "especially inside this time window.\n"
        "A cluster still needs a specific shared news anchor. Ask whether a human editor could write one concrete "
        "headline for the combined cluster. If the only possible headline is a generic roundup like 'AI news', "
        "'French Open updates', 'Premier League stories', or 'Iran developments', split into smaller clusters.\n"
        "Clusters must be mutually exclusive. If one article could plausibly fit more than one cluster, assign it "
        "only to the strongest matching cluster. If no strongest cluster is obvious, make that article a singleton.\n"
        "A valid cluster should share a central named subject or incident: the same negotiation, attack, trial, "
        "election development, company announcement, product launch, disaster, sports result, death, recall, "
        "lawsuit, or official decision. Articles can emphasize different facts or perspectives and still belong "
        "together.\n"
        "Keep articles separate when they only share a broad beat or setting without a shared central subject: "
        "for example different companies in the same industry, different games in the same league, different "
        "celebrities at the same festival, unrelated statements by the same politician, or separate incidents "
        "in the same country.\n"
        "For ongoing wars, negotiations, elections, trials, disasters, tournaments, and major political stories, "
        "group related developments if they are part of the same immediate news arc in this window. Split only "
        "when they would require a clearly different headline and summary.\n"
        "Do not group unrelated stories merely because they share a keyword or domain such as AI, climate, "
        "markets, a sports tournament, a holiday, a party, or a country.\n"
        "For sports, group previews, live/result coverage, reactions, and analysis for the same game/race/match "
        "or same league-table outcome. Do not group separate games unless the shared story is the combined "
        "standings/relegation/title outcome.\n"
        "For politics and policy, group reactions and analysis with the same bill, ruling, negotiation, "
        "investigation, appointment, statement, or incident they discuss.\n"
        "Exact duplicates, syndications, translations, or near-identical wire reprints of the same article should "
        "be grouped, including repeated copies from the same source.\n"
        "Do not group ads, shopping pages, explainers, rankings, guides, fantasy advice, or evergreen features "
        "with real news unless they directly explain or analyze the same active story. Group exact repeats of "
        "those items with each other.\n\n"
        "For each article, you must also classify its content type and category:\n"
        "- content_type: Choose from: 'news', 'opinion', 'analysis', 'review', 'unknown'. "
        "Use 'opinion' for editorial columns, op-eds, or heavily biased commentary. "
        "Use 'analysis' for explanatory/deep-dive reports. Use 'news' for standard factual reporting.\n"
        f"- category: Choose the strongest category from this exact list: {', '.join(valid_categories)}.\n\n"
        f"Input fields: {fields}.\n"
        "Every article index must appear exactly once in exactly one group. Singletons are allowed.\n"
        f"Audit the final JSON: every integer from 0 through {len(articles) - 1} must appear "
        "exactly once in both 'articles' and the combined 'groups.article_indexes'. "
        "No missing indexes. No repeated indexes.\n"
        "Return compact JSON only."
        f"{active_events_section}\n\n"
        f"Articles:\n{json.dumps(rows, ensure_ascii=False, separators=(',', ':'))}"
    )


def _grouping_response_schema(valid_categories: list[str]) -> dict[str, Any]:
    return {
        "type": "OBJECT",
        "properties": {
            "articles": {
                "type": "ARRAY",
                "items": {
                    "type": "OBJECT",
                    "properties": {
                        "article_index": {"type": "INTEGER"},
                        "content_type": {
                            "type": "STRING",
                            "enum": ["news", "opinion", "analysis", "review", "unknown"],
                        },
                        "category": {
                            "type": "STRING",
                            "enum": valid_categories,
                        },
                    },
                    "required": ["article_index", "content_type", "category"],
                },
            },
            "groups": {
                "type": "ARRAY",
                "items": {
                    "type": "OBJECT",
                    "properties": {
                        "article_indexes": {
                            "type": "ARRAY",
                            "items": {"type": "INTEGER"},
                        },
                        "existing_event_id": {
                            "type": "STRING",
                        },
                    },
                    "required": ["article_indexes"],
                },
            },
        },
        "required": ["articles", "groups"],
    }


def _build_newsworthiness_prompt(
    articles: Sequence[ArticleForAggregation],
    groups: Sequence[dict[str, Any]],
    feeds_by_source: dict[str, Any] | None = None,
) -> str:
    rows: list[dict[str, Any]] = []
    if feeds_by_source is None:
        feeds_by_source = {feed.source_id: feed for feed in load_feeds(enabled_only=False)}
    for group in groups:
        group_articles = [articles[index] for index in group["article_indexes"]]
        rows.append(
            {
                "group_index": group["group_index"],
                "category_hint": _group_category_hint(group, group_articles, feeds_by_source),
                "article_count": len(group_articles),
                "source_count": len(set(group["sources"])),
                "headlines": [article.headline for article in group_articles[:12]],
                "summaries": [_brief_summary(article) for article in group_articles[:8] if article.summary],
            }
        )
    return (
        "Score each story cluster for editorial newsworthiness.\n"
        "Return two numeric scores from 0.0 to 1.0.\n"
        "global_score: importance to a general worldwide news homepage. "
        "Wars, major geopolitical escalation, mass casualties, public safety emergencies, democratic crises, "
        "major economic shocks, major legal rulings, and high-impact health/science events score high. "
        "Entertainment releases, routine sports results, product updates, lifestyle items, and ads score lower "
        "unless they have unusually broad impact.\n"
        "category_score: importance within the story's own vertical/category. A film release may be low global "
        "but high entertainment; a playoff final may be high sports but moderate global.\n"
        "Use compact rationale_codes such as geopolitical_escalation, public_safety, mass_casualty_risk, "
        "major_policy, economic_impact, major_sports_result, entertainment_major_release, niche_update, "
        "low_public_impact, duplicate_reprint.\n"
        "Do not include prose explanations. Return every group_index exactly once.\n\n"
        f"Clusters:\n{json.dumps(rows, ensure_ascii=False, separators=(',', ':'))}"
    )


def _newsworthiness_response_schema() -> dict[str, Any]:
    return {
        "type": "OBJECT",
        "properties": {
            "scores": {
                "type": "ARRAY",
                "items": {
                    "type": "OBJECT",
                    "properties": {
                        "group_index": {"type": "INTEGER"},
                        "global_score": {"type": "NUMBER"},
                        "category_score": {"type": "NUMBER"},
                        "rationale_codes": {
                            "type": "ARRAY",
                            "items": {"type": "STRING"},
                        },
                    },
                    "required": ["group_index", "global_score", "category_score", "rationale_codes"],
                },
            }
        },
        "required": ["scores"],
    }


def _brief_summary(article: ArticleForAggregation) -> str:
    summary = (article.digest_summary or article.summary or "").strip()
    if len(summary) <= 500:
        return summary
    return summary[:500].rsplit(" ", 1)[0]


def _load_digest_fields(article_path: str) -> dict[str, Any]:
    path = PROJECT_ROOT / article_path
    if not path.exists():
        return {"digest_summary": None, "digest_key_facts": (), "digest_content_quality": None}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {"digest_summary": None, "digest_key_facts": (), "digest_content_quality": None}
    digest = data.get("llm_digest")
    if not isinstance(digest, dict):
        return {"digest_summary": None, "digest_key_facts": (), "digest_content_quality": None}
    summary = digest.get("summary")
    key_facts = digest.get("key_facts")
    if not isinstance(summary, str) or not isinstance(key_facts, list):
        return {"digest_summary": None, "digest_key_facts": (), "digest_content_quality": None}
    clean_facts = tuple(str(fact).strip() for fact in key_facts if str(fact).strip())
    content_quality = digest.get("content_quality")
    if not isinstance(content_quality, str):
        content_quality = None
    impact = digest.get("impact")
    if not isinstance(impact, dict):
        impact = None
    return {
        "digest_summary": summary.strip() or None,
        "digest_key_facts": clean_facts,
        "digest_content_quality": content_quality,
        "digest_impact": impact,
    }


def _article_aggregation_exclusion_reason(article: ArticleForAggregation) -> str | None:
    content_quality = (article.digest_content_quality or "").strip().lower()
    rationale_codes: set[str] = set()
    if isinstance(article.digest_impact, dict):
        rationale_codes = {
            sanitize_id(str(code)).lower()
            for code in article.digest_impact.get("rationale_codes", [])
            if str(code).strip()
        }
    headline = article.headline.strip().lower()
    if content_quality == "non_news":
        return "non_news"
    if headline == "(untitled)" and (
        content_quality in {"thin", "extraction_noise"} or rationale_codes & VIDEO_OR_CAROUSEL_RATIONALE_CODES
    ):
        return "video_or_carousel"
    if rationale_codes & VIDEO_OR_CAROUSEL_RATIONALE_CODES:
        return "video_or_carousel"
    if rationale_codes & AGGREGATION_EXCLUDED_RATIONALE_CODES:
        return "low_signal_content"
    return None


def _article_category_impact(article: ArticleForAggregation) -> float | None:
    if not isinstance(article.digest_impact, dict):
        return None
    value = article.digest_impact.get("category")
    if not isinstance(value, int | float):
        return None
    return max(0.0, min(1.0, float(value)))


def _normalize_mode(mode: str) -> str:
    return mode.replace("-", "_")


def _validate_iso_timestamp(value: str) -> None:
    _parse_iso_timestamp(value)


def _parse_iso_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _format_iso_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _floor_utc_interval(value: datetime, interval_hours: int) -> datetime:
    if interval_hours <= 0:
        raise ValueError("interval_hours must be positive")
    value = value.astimezone(UTC)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    interval_seconds = interval_hours * 60 * 60
    elapsed_seconds = int((value - epoch).total_seconds())
    return epoch + timedelta(seconds=(elapsed_seconds // interval_seconds) * interval_seconds)


def _event_id_for_group(articles: Sequence[ArticleForAggregation]) -> str | None:
    existing = [article.event_id for article in articles if article.event_id]
    if existing:
        return Counter(existing).most_common(1)[0][0]
    return None


def _generate_new_event_id(articles: Sequence[ArticleForAggregation], state: StateDB) -> str:
    title = _event_title(articles)
    earliest = min(
        (_parse_iso_timestamp(article.published_at) for article in articles if article.published_at),
        default=None,
    )
    date_prefix = (earliest or datetime.now(UTC)).strftime("%Y-%m-%d")
    slug = sanitize_id(_slug_text(title)).lower()
    event_id = f"{date_prefix}-{slug}"[:140].rstrip(".-")
    if not event_id or state.event_exists(event_id):
        base = event_id or f"{date_prefix}-story"
        for index in range(2, 1000):
            candidate = f"{base}-{index}"[:160].rstrip(".-")
            if not state.event_exists(candidate):
                return candidate
    return event_id


def _read_event(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _build_event_payload(
    *,
    event_id: str,
    event_path: Path,
    articles: Sequence[ArticleForAggregation],
    existing: dict[str, Any] | None,
    feeds_by_source: dict[str, Any],
    category_override: str | None = None,
    newsworthiness: dict[str, Any] | None = None,
) -> dict[str, Any]:
    now = isoformat_z()
    article_ids = sorted(set((existing or {}).get("article_ids", [])) | {article.article_id for article in articles})
    title = (existing or {}).get("title") or _event_title(articles)
    category = (
        (existing or {}).get("category") or category_override or _category_for_articles(articles, feeds_by_source)
    )
    created_at = (existing or {}).get("created_at") or _earliest_published_at(articles) or now
    return {
        "event_id": event_id,
        "title": title,
        "category": category,
        "thread": (existing or {}).get("thread"),
        "keywords": _keywords_for_articles(articles),
        "entities": (existing or {}).get("entities", []),
        "created_at": created_at,
        "updated_at": now,
        "status": "active",
        "article_ids": article_ids,
        "article_count": len(article_ids),
        "confidence": (existing or {}).get("confidence", 0.7),
        "newsworthiness": newsworthiness
        or (existing or {}).get("newsworthiness")
        or _baseline_newsworthiness(
            articles,
            source_count=len({article.source_id for article in articles}),
            feeds_by_source=feeds_by_source,
        ),
        "event_path": _relative_event_path(event_path),
        "llm_metadata": {
            "stage": "aggregation",
            "prompt_version": AGGREGATION_PROMPT_VERSION,
        },
    }


def _event_title(articles: Sequence[ArticleForAggregation]) -> str:
    if not articles:
        return "Untitled story"
    headlines = [article.headline.strip() for article in articles if article.headline.strip()]
    if not headlines:
        return "Untitled story"
    # Prefer the shortest headline that is at least 20 chars (descriptive but not bloated);
    # fall back to the absolute shortest only if every option is too short.
    return min(headlines, key=lambda headline: (len(headline) < 20, len(headline)))


def _relative_event_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


def _category_from_classifications(
    article_indexes: Sequence[int],
    article_classifications: dict[int, dict[str, Any]],
) -> str | None:
    valid = set(load_categories())
    categories: list[str] = []
    for index in article_indexes:
        category = article_classifications.get(index, {}).get("category")
        if category in valid:
            categories.append(category)
    if not categories:
        return None
    return Counter(categories).most_common(1)[0][0]


def _group_category_hint(
    group: dict[str, Any],
    articles: Sequence[ArticleForAggregation],
    feeds_by_source: dict[str, Any],
) -> str:
    valid = set(load_categories())
    category = group.get("category") or group.get("category_hint")
    if category in valid:
        return category
    return _category_for_articles(articles, feeds_by_source)


def _category_for_articles(articles: Sequence[ArticleForAggregation], feeds_by_source: dict[str, Any]) -> str:
    valid = set(load_categories())
    categories = [
        feeds_by_source[article.source_id].default_category
        for article in articles
        if article.source_id in feeds_by_source and feeds_by_source[article.source_id].default_category
    ]
    filtered = [cat for cat in categories if cat in valid]
    if not filtered:
        return "world"
    return Counter(filtered).most_common(1)[0][0]


def _baseline_newsworthiness(
    articles: Sequence[ArticleForAggregation],
    *,
    source_count: int,
    feeds_by_source: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if feeds_by_source is None:
        feeds_by_source = {feed.source_id: feed for feed in load_feeds(enabled_only=False)}
    category = _category_for_articles(articles, feeds_by_source)
    digest_impacts = [article.digest_impact for article in articles if isinstance(article.digest_impact, dict)]
    if digest_impacts:
        global_scores = [_coerce_impact_score(impact.get("global")) for impact in digest_impacts]
        category_scores = [_coerce_impact_score(impact.get("category")) for impact in digest_impacts]
        global_score = max(global_scores) if global_scores else 0.0
        category_score = max(category_scores) if category_scores else 0.0
        if len(digest_impacts) > 1:
            global_score = max(global_score, sum(global_scores) / len(global_scores) + 0.04)
            category_score = max(category_score, sum(category_scores) / len(category_scores) + 0.04)
        rationale_codes = ["digest_impact"]
        for impact in digest_impacts:
            rationale_codes.extend(str(code) for code in impact.get("rationale_codes", []) if str(code).strip())
            scope = impact.get("scope")
            novelty = impact.get("novelty")
            if scope:
                rationale_codes.append(f"scope_{scope}")
            if novelty:
                rationale_codes.append(f"novelty_{novelty}")
        if source_count >= 3:
            global_score += 0.04
            category_score += 0.04
            rationale_codes.append("multi_source")
        if source_count >= 8:
            global_score += 0.04
            category_score += 0.03
            rationale_codes.append("broad_coverage")
        return {
            "global": round(max(0.0, min(1.0, global_score)), 3),
            "category": round(max(0.0, min(1.0, category_score)), 3),
            "rationale_codes": sorted(
                {sanitize_id(str(code)).lower() for code in rationale_codes if str(code).strip()}
            ),
            "scored_at": isoformat_z(),
            "model": "deterministic-digest-impact",
            "prompt_version": NEWSWORTHINESS_PROMPT_VERSION,
            "article_impact_count": len(digest_impacts),
        }

    text = " ".join(f"{article.headline} {article.summary or ''}" for article in articles).lower()
    global_score = 0.15
    category_score = 0.25
    rationale_codes: list[str] = ["baseline"]

    global_weight = {
        "world": 0.18,
        "us": 0.16,
        "politics": 0.15,
        "business": 0.1,
        "health": 0.12,
        "science": 0.08,
        "technology": 0.06,
        "environment": 0.1,
        "automotive": 0.04,
        "entertainment": 0.03,
        "sports": 0.03,
    }.get(category, 0.05)
    global_score += global_weight
    category_score += min(0.25, global_weight + 0.08)

    if source_count >= 8:
        global_score += 0.16
        category_score += 0.14
        rationale_codes.append("multi_source_breaking")
    elif source_count >= 3:
        global_score += 0.08
        category_score += 0.08
        rationale_codes.append("multi_source")
    if len(articles) >= 8:
        global_score += 0.08
        category_score += 0.08
        rationale_codes.append("high_article_count")

    impact_terms = {
        "geopolitical_escalation": ("war", "invasion", "missile", "nuclear", "ceasefire", "sanctions"),
        "public_safety": ("shooting", "explosion", "evacuation", "chemical", "wildfire", "hurricane"),
        "mass_casualty_risk": ("killed", "dead", "deaths", "casualties", "outbreak", "ebola"),
        "economic_impact": ("markets", "inflation", "tariff", "recession", "oil", "rates"),
        "major_policy": ("supreme court", "congress", "president", "election", "bill", "ruling"),
    }
    for code, terms in impact_terms.items():
        if any(_term_matches(text, term) for term in terms):
            global_score += 0.08
            category_score += 0.06
            rationale_codes.append(code)

    if category in {"entertainment", "sports", "automotive"} and not set(rationale_codes) & {
        "public_safety",
        "mass_casualty_risk",
        "economic_impact",
    }:
        global_score -= 0.08
        rationale_codes.append("lower_global_vertical")

    return {
        "global": round(max(0.0, min(1.0, global_score)), 3),
        "category": round(max(0.0, min(1.0, category_score)), 3),
        "rationale_codes": sorted(set(rationale_codes)),
        "scored_at": isoformat_z(),
        "model": "deterministic-baseline",
        "prompt_version": NEWSWORTHINESS_PROMPT_VERSION,
    }


def _coerce_impact_score(value: Any) -> float:
    if not isinstance(value, int | float):
        return 0.0
    return max(0.0, min(1.0, float(value)))


def _term_matches(text: str, term: str) -> bool:
    pattern = r"\b" + re.escape(term.lower()) + r"\b"
    return re.search(pattern, text) is not None


def _earliest_published_at(articles: Sequence[ArticleForAggregation]) -> str | None:
    published = [article.published_at for article in articles if article.published_at]
    return min(published) if published else None


_KEYWORD_STOPWORDS = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "as", "at",
    "be", "because", "been", "before", "being", "below", "between", "both", "but", "by",
    "can", "could", "did", "do", "does", "doing", "down", "during",
    "each", "few", "for", "from", "further", "had", "has", "have", "having", "he", "her",
    "here", "hers", "him", "his", "how", "i", "if", "in", "into", "is", "it", "its", "itself",
    "just", "me", "more", "most", "my", "myself", "no", "nor", "not", "of", "off", "on", "once",
    "only", "or", "other", "our", "ours", "ourselves", "out", "over", "own", "same", "she",
    "should", "so", "some", "such", "than", "that", "the", "their", "theirs", "them",
    "themselves", "then", "there", "these", "they", "this", "those", "through", "to", "too",
    "under", "until", "up", "very", "was", "we", "were", "what", "when", "where", "which",
    "while", "who", "whom", "why", "with", "would", "you", "your", "yours", "yourself",
    "yourselves",
    # domain specific
    "news", "says", "said", "amid", "report", "reports", "according", "u", "s", "us"
}


def _keywords_for_articles(articles: Sequence[ArticleForAggregation]) -> list[str]:
    counter: Counter[str] = Counter()
    for article in articles:
        text = f"{article.headline} {article.summary or ''}".lower()
        for word in re.findall(r"[a-z][a-z0-9-]{3,}", text):
            if word not in _KEYWORD_STOPWORDS:
                counter[word] += 1
    return [word for word, _ in counter.most_common(12)]


def _slug_text(title: str) -> str:
    words = re.findall(r"[A-Za-z0-9]+", title.lower())
    filtered = [word for word in words if word not in _KEYWORD_STOPWORDS]
    return "-".join(filtered[:8]) or "story"


def _article_preview(article: ArticleForAggregation, index: int) -> dict[str, Any]:
    return {
        "index": index,
        "article_id": article.article_id,
        "source": article.source_name,
        "published_at": article.published_at,
        "headline": article.headline,
        "summary": _brief_summary(article),
        "article_path": article.article_path,
    }


def _paired_indexes(groups: Sequence[dict[str, Any]]) -> set[tuple[int, int]]:
    pairs: set[tuple[int, int]] = set()
    for group in groups:
        indexes = sorted(group["article_indexes"])
        for left_index, left in enumerate(indexes):
            for right in indexes[left_index + 1 :]:
                pairs.add((left, right))
    return pairs


def _base_slug(event_id: str) -> str:
    if len(event_id) > 11 and event_id[4] == "-" and event_id[7] == "-" and event_id[10] == "-":
        slug = event_id[11:]
    else:
        slug = event_id
    return re.sub(r"-\d+$", "", slug)


def _titles_similar(title1: str, title2: str) -> bool:
    w1 = set(re.findall(r"[A-Za-z0-9]+", title1.lower())) - _KEYWORD_STOPWORDS
    w2 = set(re.findall(r"[A-Za-z0-9]+", title2.lower())) - _KEYWORD_STOPWORDS
    common = w1 & w2
    return len(common) >= 4 or (len(common) >= 3 and min(len(w1), len(w2)) <= 4)


def _titles_share_at_least(title1: str, title2: str, count: int) -> bool:
    w1 = set(re.findall(r"[A-Za-z0-9]+", title1.lower())) - _KEYWORD_STOPWORDS
    w2 = set(re.findall(r"[A-Za-z0-9]+", title2.lower())) - _KEYWORD_STOPWORDS
    return len(w1 & w2) >= count


def _events_have_similar_article_headline(
    event_id1: str,
    event_id2: str,
    article_headlines_by_event: dict[str, list[str]],
) -> bool:
    headlines1 = article_headlines_by_event.get(event_id1, [])
    headlines2 = article_headlines_by_event.get(event_id2, [])
    for headline1 in headlines1:
        for headline2 in headlines2:
            if _normalized_title(headline1) == _normalized_title(headline2):
                return True
            if _titles_similar(headline1, headline2):
                return True
    return False


def _normalized_title(title: str) -> str:
    return " ".join(re.findall(r"[A-Za-z0-9]+", title.lower()))


def merge_events(
    winner_id: str,
    loser_id: str,
    state: StateDB,
    feeds_by_source: dict[str, Any],
) -> None:
    winner_path = EVENT_DIR / f"{winner_id}.json"
    loser_path = EVENT_DIR / f"{loser_id}.json"
    winner_event = _read_event(winner_path)
    if not winner_event:
        return

    state.merge_events_into([loser_id], winner_id)

    try:
        loser_path.unlink(missing_ok=True)
    except Exception:
        pass

    rows = state.conn.execute(
        """
        SELECT article_id, source_id, source_name, headline, summary, published_at, article_path, event_id
        FROM articles
        WHERE event_id = ? AND is_filtered = 0
        """,
        (winner_id,),
    ).fetchall()

    articles = [
        ArticleForAggregation(
            article_id=row["article_id"],
            source_id=row["source_id"],
            source_name=row["source_name"],
            headline=row["headline"],
            summary=row["summary"],
            published_at=row["published_at"],
            article_path=row["article_path"],
            event_id=row["event_id"],
            **_load_digest_fields(row["article_path"]),
        )
        for row in rows
    ]

    # Drop the pre-merge newsworthiness so _build_event_payload recomputes it across
    # the combined article set — picking up multi-source/broad-coverage bonuses that
    # the winner's pre-merge score didn't see.
    existing_for_payload = {k: v for k, v in winner_event.items() if k != "newsworthiness"}
    event_payload = _build_event_payload(
        event_id=winner_id,
        event_path=winner_path,
        articles=articles,
        existing=existing_for_payload,
        feeds_by_source=feeds_by_source,
    )
    atomic_write_json(winner_path, event_payload)
    state.upsert_event(event_payload, winner_path)


def _load_event_articles_summary(event_id: str, state: StateDB) -> list[dict[str, str]]:
    rows = state.conn.execute(
        "SELECT headline, summary, article_path FROM articles WHERE event_id = ? AND is_filtered = 0",
        (event_id,),
    ).fetchall()
    summaries = []
    for row in rows:
        digest = _load_digest_fields(row["article_path"])
        summary = digest.get("digest_summary") or row["summary"] or ""
        summaries.append({
            "headline": row["headline"],
            "summary": summary[:300] + "..." if len(summary) > 300 else summary
        })
    return summaries


def _build_event_merge_prompt(event1: dict[str, Any], event2: dict[str, Any]) -> str:
    return (
        "Compare these two news event clusters and decide if they represent the exact same "
        "underlying real-world news event and should be merged into a single event.\n"
        "They should be merged if they cover the same development, announcement, decision, "
        "sports game, or incident.\n"
        "They should remain separate if they are different incidents, different sports games, "
        "unrelated actions by the same actor, or distinct developments in a broader topic "
        "(e.g., separate space launches, separate policy announcements, separate trials).\n\n"
        "Event 1:\n"
        f"  ID: {event1['event_id']}\n"
        f"  Title: {event1['title']}\n"
        f"  Articles:\n{json.dumps(event1['articles'], ensure_ascii=False, indent=2)}\n\n"
        "Event 2:\n"
        f"  ID: {event2['event_id']}\n"
        f"  Title: {event2['title']}\n"
        f"  Articles:\n{json.dumps(event2['articles'], ensure_ascii=False, indent=2)}\n\n"
        "Return a JSON object matching this schema:\n"
        "{\n"
        "  \"should_merge\": boolean,\n"
        "  \"confidence\": number (0.0 to 1.0),\n"
        "  \"rationale\": string\n"
        "}"
    )


def _event_merge_response_schema() -> dict[str, Any]:
    return {
        "type": "OBJECT",
        "properties": {
            "should_merge": {"type": "BOOLEAN"},
            "confidence": {"type": "NUMBER"},
            "rationale": {"type": "STRING"},
        },
        "required": ["should_merge", "confidence", "rationale"],
    }


def deduplicate_active_events_llm(
    *,
    state: StateDB,
    client: JsonGenerator,
    feeds_by_source: dict[str, Any],
    progress: Callable[[str], None] | None = None,
    run_id: str | None = None,
) -> None:
    since_dt = datetime.now(UTC) - timedelta(hours=48)
    since = since_dt.isoformat().replace("+00:00", "Z")

    rows = state.conn.execute(
        """
        SELECT event_id, title, category, updated_at, article_count, created_at
        FROM events
        WHERE status = 'active' AND updated_at >= ?
        ORDER BY category, updated_at DESC
        """,
        (since,),
    ).fetchall()

    events = [dict(row) for row in rows]
    if len(events) < 2:
        return
    article_headlines_by_event: dict[str, list[str]] = {}
    for row in state.conn.execute(
        """
        SELECT event_id, headline
        FROM articles
        WHERE event_id IS NOT NULL
          AND is_filtered = 0
        """
    ).fetchall():
        article_headlines_by_event.setdefault(row["event_id"], []).append(row["headline"])

    candidates = []
    for i in range(len(events)):
        for j in range(i + 1, len(events)):
            e1 = events[i]
            e2 = events[j]
            slug_match = _base_slug(e1["event_id"]) == _base_slug(e2["event_id"])
            title_match = _titles_similar(e1["title"], e2["title"])
            headline_match = False
            if not (slug_match or title_match) and _titles_share_at_least(e1["title"], e2["title"], 2):
                headline_match = _events_have_similar_article_headline(
                    e1["event_id"],
                    e2["event_id"],
                    article_headlines_by_event,
                )
            if slug_match or title_match or headline_match:
                candidates.append((e1, e2))

    if not candidates:
        return

    if progress:
        progress(f"deduplicate: found {len(candidates)} candidate event pair(s) for LLM evaluation")

    merges_count = 0
    for e1, e2 in candidates:
        if not state.event_exists(e1["event_id"]) or not state.event_exists(e2["event_id"]):
            continue

        articles1 = _load_event_articles_summary(e1["event_id"], state)
        articles2 = _load_event_articles_summary(e2["event_id"], state)

        if not articles1 or not articles2:
            continue

        payload1 = {"event_id": e1["event_id"], "title": e1["title"], "articles": articles1}
        payload2 = {"event_id": e2["event_id"], "title": e2["title"], "articles": articles2}

        try:
            result = client.generate_json(
                system_instruction=(
                    "You are an expert news editor. Determine if two event clusters represent "
                    "the same news event and should be merged."
                ),
                prompt=_build_event_merge_prompt(payload1, payload2),
                response_schema=_event_merge_response_schema(),
                temperature=0,
            )
            if run_id:
                try:
                    state.record_llm_usage(
                        run_id=run_id,
                        stage="deduplication",
                        model=client.model,
                        prompt_version="deduplication-v1",
                        input_tokens=result.usage.get("promptTokenCount"),
                        output_tokens=result.usage.get("candidatesTokenCount"),
                    )
                except Exception:
                    pass

            should_merge = result.payload.get("should_merge") is True
            raw_confidence = result.payload.get("confidence")
            confidence = (
                float(raw_confidence)
                if isinstance(raw_confidence, int | float) and not isinstance(raw_confidence, bool)
                else 0.0
            )
            rationale = result.payload.get("rationale", "")

            if should_merge and confidence >= DEDUPLICATION_MERGE_CONFIDENCE_THRESHOLD:
                winner = e1
                loser = e2
                if e2.get("article_count", 0) > e1.get("article_count", 0):
                    winner, loser = e2, e1
                elif e2.get("article_count", 0) == e1.get("article_count", 0):
                    if e2.get("created_at", "") < e1.get("created_at", ""):
                        winner, loser = e2, e1
                    elif e2.get("created_at", "") == e1.get("created_at", ""):
                        if len(e2["event_id"]) < len(e1["event_id"]):
                            winner, loser = e2, e1

                winner_id = winner["event_id"]
                loser_id = loser["event_id"]

                if progress:
                    progress(
                        f"deduplicate: merging {loser_id} into {winner_id} "
                        f"(confidence: {confidence:.2f}; rationale: {rationale})"
                    )

                merge_events(winner_id, loser_id, state, feeds_by_source)
                merges_count += 1
            elif should_merge and progress:
                progress(
                    f"deduplicate: skipped low-confidence merge for {e1['event_id']} and {e2['event_id']} "
                    f"(confidence: {confidence:.2f})"
                )
        except Exception as exc:
            if progress:
                progress(f"deduplicate: failed to evaluate pair {e1['event_id']} and {e2['event_id']}: {exc}")
            if run_id:
                try:
                    state.record_error(
                        run_id,
                        "deduplication",
                        "event_pair",
                        f"{e1['event_id']}_{e2['event_id']}",
                        None,
                        exc,
                    )
                except Exception:
                    pass

    if progress and merges_count > 0:
        progress(f"deduplicate: completed merging {merges_count} event(s)")
