from __future__ import annotations

import hashlib
import json
import math
import re
import uuid
from collections import Counter
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from pipeline.config import load_categories, load_feeds, load_pipeline_config
from pipeline.llm import GeminiResult, create_gemini_client
from pipeline.lock import PipelineLock
from pipeline.paths import EVENT_DIR, LOCK_PATH, PROJECT_ROOT, STORY_DIR
from pipeline.state import StateDB
from pipeline.util import atomic_write_json, isoformat_z, sanitize_id

AGGREGATION_PROMPT_VERSION = "aggregation-v7"
AGGREGATION_EXPERIMENT_PROMPT_VERSION = "aggregation-experiment-v6"
NEWSWORTHINESS_PROMPT_VERSION = "newsworthiness-v1"
DEDUPLICATION_PRESCREEN_PROMPT_VERSION = "deduplication-prescreen-v1"
DEDUPLICATION_REVIEW_PROMPT_VERSION = "deduplication-review-v3"
GROUPING_MODES = ("titles", "titles_summaries")
DEDUPLICATION_MERGE_CONFIDENCE_THRESHOLD = 0.8
DEDUPLICATION_KEYWORD_OVERLAP_MIN = 2
DEDUPLICATION_KEYWORDS_PER_EVENT = 6
DEDUPLICATION_HEADLINES_PER_EVENT = 3
DEDUPLICATION_HOT_STOPWORD_THRESHOLD = 0.2
DEDUPLICATION_MAX_EVENTS_PER_PRESCREEN_BATCH = 40
DEDUPLICATION_PRESCREEN_ANCHOR_EVENTS = 6
DEDUPLICATION_MAX_PASSES = 3
DEFAULT_AGGREGATION_CATEGORY_BATCH_CONCURRENCY = 8
DEFAULT_DEDUPLICATION_CONCURRENCY = 16
DEFAULT_DEDUPLICATION_MAX_PAIRS_PER_RUN = 120
# Candidate review order: strongest deterministic signals first, weak title
# cohesion last so it cannot crowd out keyword and prescreen candidates.
DEDUPLICATION_PRIORITY_TITLE = 4
DEDUPLICATION_PRIORITY_HEADLINE = 3
DEDUPLICATION_PRIORITY_KEYWORD = 3
DEDUPLICATION_PRIORITY_PRESCREEN = 3
DEDUPLICATION_PRIORITY_COHESION = 2
DEDUPLICATION_PRIORITY_WEAK_COHESION = 0
DEFAULT_DEDUPLICATION_MAX_PASSES_PER_RUN = 1
DEFAULT_DEDUPLICATION_LOOKBACK_HOURS = 72
FORCE_RESET_AGGREGATION_STATUSES = (
    "assigned",
    "filtered_low_impact",
    "filtered_low_signal_content",
    "filtered_non_news",
    "filtered_standalone_opinion",
    "filtered_video_or_carousel",
    "filtered_expired",
)
MAX_CATEGORY_GROUP_ARTICLES = 50
NULL_EXISTING_EVENT_ID_VALUES = {"null", "none", "nil", "n/a", "na", "unknown"}
CATEGORY_GROUPS = [
    {"name": "politics_gov", "categories": ["politics"]},
    {"name": "news_business", "categories": ["us", "world", "business"]},
    {"name": "sci_tech", "categories": ["technology", "science", "health", "environment"]},
    {"name": "leisure", "categories": ["entertainment", "automotive"]},
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


def _recent_event_cutoff(hours: int = 48) -> str:
    return (datetime.now(UTC) - timedelta(hours=hours)).isoformat().replace("+00:00", "Z")


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
    "index_page",
    "live_blog",
    "media_transcript",
    "newsletter_roundup",
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


@dataclass(frozen=True)
class LlmUsageRecord:
    stage: str
    prompt_version: str
    input_tokens: int | None
    output_tokens: int | None
    usage: dict[str, Any] | None = None


@dataclass(frozen=True)
class ActiveEventsFilterResult:
    active_events: list[dict[str, Any]]
    usage: dict[str, Any]


@dataclass(frozen=True)
class CategoryBatchProcessingResult:
    batch: dict[str, Any]
    grouping_result: dict[str, Any]
    scores_by_group_index: dict[int, dict[str, Any]]
    usage_records: tuple[LlmUsageRecord, ...]
    group_count: int
    singleton_count: int
    multi_article_group_count: int
    validation_attempts: int
    elapsed_ms: int | None
    prompt_tokens: int
    candidates_tokens: int
    news_scored: int
    news_fallback_count: int
    news_elapsed_ms: int | None
    news_prompt_tokens: int
    news_candidates_tokens: int


@dataclass(frozen=True)
class PrescreenChunkResult:
    chunk_label: str
    event_count: int
    pairs: tuple[tuple[str, str], ...]
    usage: dict[str, Any]


@dataclass(frozen=True)
class PrescreenChunkSpec:
    chunk_label: str
    chunk: tuple[dict[str, Any], ...]
    valid_ids: frozenset[str]
    dynamic_stopwords: frozenset[str]


@dataclass(frozen=True)
class DeduplicationPairDecision:
    should_merge: bool
    confidence: float
    rationale: str
    usage: dict[str, Any]
    model: str


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


def category_impact_floors(aggregation_config: dict[str, Any]) -> dict[str, float]:
    """Per-category minimum category-impact overrides from configuration."""
    raw = aggregation_config.get("min_category_impact_overrides") or {}
    if not isinstance(raw, dict):
        raise ValueError("aggregation.min_category_impact_overrides must be an object")
    valid = set(load_categories())
    floors: dict[str, float] = {}
    for category, value in raw.items():
        if category not in valid:
            raise ValueError(f"unknown category in min_category_impact_overrides: {category}")
        floor = float(value)
        if not 0.0 <= floor <= 1.0:
            raise ValueError(f"min_category_impact_overrides.{category} must be within 0-1")
        floors[str(category)] = floor
    return floors


def category_impact_floor_for_source(
    source_id: str,
    *,
    feeds_by_source: dict[str, Any],
    min_category_impact: float,
    floors: dict[str, float] | None,
) -> float:
    """Resolve the impact threshold for an article from its feed's default category."""
    if not floors:
        return min_category_impact
    feed = feeds_by_source.get(source_id)
    category = getattr(feed, "default_category", None) if feed is not None else None
    return floors.get(str(category or ""), min_category_impact)


def load_window_articles(
    *,
    window_start: str,
    window_end: str,
    min_category_impact: float | None = None,
    mark_filtered: bool = True,
    db: StateDB | None = None,
    max_article_rowid: int | None = None,
    category_impact_floors: dict[str, float] | None = None,
    feeds_by_source: dict[str, Any] | None = None,
) -> list[ArticleForAggregation]:
    _validate_iso_timestamp(window_start)
    _validate_iso_timestamp(window_end)
    close_db = db is None
    state = db or StateDB()
    if category_impact_floors and feeds_by_source is None:
        feeds_by_source = {feed.source_id: feed for feed in load_feeds(enabled_only=False)}
    try:
        rowid_clause = " AND rowid <= ?" if max_article_rowid is not None else ""
        params: list[Any] = [window_start, window_end]
        if max_article_rowid is not None:
            params.append(max_article_rowid)
        rows = state.conn.execute(
            f"""
            SELECT article_id, source_id, source_name, headline, summary, published_at, article_path, event_id
            FROM articles
            WHERE published_at >= ? AND published_at < ?
              AND is_filtered = 0
              {rowid_clause}
            ORDER BY published_at DESC, fetched_at DESC
            """,
            params,
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
            threshold = category_impact_floor_for_source(
                article.source_id,
                feeds_by_source=feeds_by_source or {},
                min_category_impact=min_category_impact,
                floors=category_impact_floors,
            )
            if score < threshold:
                if mark_filtered:
                    state.update_article_aggregation_status(
                        article.article_id,
                        status="filtered_low_impact",
                        reason=f"category_impact {score:.3f} below {threshold:.3f}",
                    )
                continue
            if mark_filtered:
                state.set_article_aggregation_pending_if_unassigned(article.article_id)
            included.append(article)
        return included
    finally:
        if close_db:
            state.close()


def _positive_int_config(value: object, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(1, parsed)


def _usage_record(stage: str, prompt_version: str, usage: dict[str, Any]) -> LlmUsageRecord | None:
    if not usage:
        return None
    return LlmUsageRecord(
        stage=stage,
        prompt_version=prompt_version,
        input_tokens=usage.get("promptTokenCount"),
        output_tokens=usage.get("candidatesTokenCount"),
        usage=dict(usage),
    )


def _record_llm_usage_records(
    state: StateDB,
    *,
    run_id: str,
    model: str,
    usage_records: Sequence[LlmUsageRecord],
) -> None:
    for record in usage_records:
        state.record_llm_usage(
            run_id=run_id,
            stage=record.stage,
            model=model,
            prompt_version=record.prompt_version,
            input_tokens=record.input_tokens,
            output_tokens=record.output_tokens,
            usage=record.usage,
        )


def _completed_digest_article_time_bounds(
    state: StateDB,
    *,
    published_at_or_after: str,
    max_article_rowid: int | None = None,
) -> tuple[str, str] | None:
    rowid_clause = " AND rowid <= ?" if max_article_rowid is not None else ""
    params: list[Any] = [published_at_or_after]
    if max_article_rowid is not None:
        params.append(max_article_rowid)
    row = state.conn.execute(
        f"""
        SELECT MIN(published_at) AS min_published_at, MAX(published_at) AS max_published_at
        FROM articles
        WHERE published_at IS NOT NULL
          AND published_at >= ?
          AND digest_status = 'completed'
          {rowid_clause}
        """,
        params,
    ).fetchone()
    if not row or not row["min_published_at"] or not row["max_published_at"]:
        return None
    return row["min_published_at"], row["max_published_at"]


def _event_path_from_state_row(event_id: str, event_path: str | None) -> Path:
    if event_path:
        path = Path(event_path)
        if path.is_absolute():
            return path
        return PROJECT_ROOT / path
    return EVENT_DIR / f"{event_id}.json"


def _force_reset_aggregation_range(
    state: StateDB,
    *,
    range_start: str,
    range_end: str,
) -> dict[str, int]:
    _validate_iso_timestamp(range_start)
    _validate_iso_timestamp(range_end)
    if _parse_iso_timestamp(range_end) <= _parse_iso_timestamp(range_start):
        raise ValueError("range_end must be after range_start")

    affected_event_rows = state.conn.execute(
        """
        SELECT DISTINCT a.event_id AS event_id, e.event_path AS event_path
        FROM articles a
        LEFT JOIN events e ON e.event_id = a.event_id
        WHERE a.event_id IS NOT NULL
          AND a.published_at >= ?
          AND a.published_at < ?
        ORDER BY a.event_id
        """,
        (range_start, range_end),
    ).fetchall()

    event_updates: list[tuple[str, Path, list[str]]] = []
    event_deletes: list[tuple[str, Path]] = []
    for row in affected_event_rows:
        event_id = row["event_id"]
        path = _event_path_from_state_row(event_id, row["event_path"])
        remaining_rows = state.conn.execute(
            """
            SELECT article_id
            FROM articles
            WHERE event_id = ?
              AND (published_at IS NULL OR published_at < ? OR published_at >= ?)
            ORDER BY published_at, article_id
            """,
            (event_id, range_start, range_end),
        ).fetchall()
        remaining_article_ids = [remaining_row["article_id"] for remaining_row in remaining_rows]
        if remaining_article_ids:
            event_updates.append((event_id, path, remaining_article_ids))
        else:
            event_deletes.append((event_id, path))

    status_placeholders = ",".join("?" for _ in FORCE_RESET_AGGREGATION_STATUSES)
    reset_params: list[Any] = [
        range_start,
        range_end,
        *FORCE_RESET_AGGREGATION_STATUSES,
    ]

    with state.conn:
        for event_id, _path, remaining_article_ids in event_updates:
            state.conn.execute(
                """
                UPDATE events
                SET article_count = ?
                WHERE event_id = ?
                """,
                (len(remaining_article_ids), event_id),
            )
        for event_id, _path in event_deletes:
            state.conn.execute("DELETE FROM events WHERE event_id = ?", (event_id,))

        cursor = state.conn.execute(
            f"""
            UPDATE articles
            SET event_id = NULL,
                aggregation_status = 'pending',
                aggregation_reason = NULL,
                is_filtered = 0
            WHERE published_at >= ?
              AND published_at < ?
              AND (
                event_id IS NOT NULL
                OR (
                  digest_status = 'completed'
                  AND aggregation_status IN ({status_placeholders})
                )
              )
            """,
            reset_params,
        )
        articles_reset = cursor.rowcount or 0

    for _event_id, path, remaining_article_ids in event_updates:
        try:
            event_payload = _read_event(path)
            if not event_payload:
                continue
            event_payload["article_ids"] = sorted(remaining_article_ids)
            event_payload["article_count"] = len(remaining_article_ids)
            atomic_write_json(path, event_payload)
        except Exception:
            pass

    for _event_id, path in event_deletes:
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass

    return {
        "articles_reset": articles_reset,
        "events_deleted": len(event_deletes),
        "events_trimmed": len(event_updates),
    }


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
    generator = client or create_gemini_client("bulk")

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
    sparse: bool = False,
    max_article_rowid: int | None = None,
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
        sparse_window_starts: set[str] | None = None
        if sparse and not force:
            sparse_window_starts = {
                _format_iso_timestamp(_floor_utc_interval(_parse_iso_timestamp(published_at), step_hours))
                for published_at in state.unassigned_article_published_times(
                    range_start,
                    range_end,
                    max_article_rowid=max_article_rowid,
                )
            }
            sparse_window_starts = {
                window_start
                for window_start in sparse_window_starts
                if start_dt <= _parse_iso_timestamp(window_start) < end_dt
            }
            if latest_completed is not None:
                latest_start_dt = _parse_iso_timestamp(latest_completed[0])
                if start_dt <= latest_start_dt < end_dt:
                    sparse_window_starts.add(latest_completed[0])

        def should_skip_window(window: AggregationWindow) -> bool:
            if force:
                return False
            status = state.aggregation_window_status(window.window_start, window.window_end)
            if status != "completed":
                return False
            if latest_completed is not None and (window.window_start, window.window_end) == latest_completed:
                return False
            return (
                state.unassigned_article_count_in_window(
                    window.window_start,
                    window.window_end,
                    max_article_rowid=max_article_rowid,
                )
                == 0
            )

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
            if sparse_window_starts is not None and window.window_start not in sparse_window_starts:
                continue
            if not should_skip_window(window):
                windows.append(window)

        return windows
    finally:
        if close_db:
            state.close()


def _process_category_batch_llm(
    *,
    batch: dict[str, Any],
    active_rows_by_category: dict[str, list[dict[str, Any]]],
    client: JsonGenerator,
    feeds_by_source: dict[str, Any],
) -> CategoryBatchProcessingResult:
    group_articles = batch["articles"]
    usage_records: list[LlmUsageRecord] = []

    # Tightened candidate filter: active events need a real headline
    # cohesion edge with at least one article, not just a broad shared beat.
    candidates: list[dict[str, Any]] = []
    for cat in _candidate_categories_for_group(batch["categories"]):
        for ev in active_rows_by_category.get(cat, []):
            if any(_headlines_have_cohesion_edge(ev["title"], art.headline) for art in group_articles):
                candidates.append(ev)

    active_filter = _filter_active_events_with_llm_result(
        articles=group_articles,
        active_events=candidates,
        client=client,
    )
    if record := _usage_record(
        "active_events_filter",
        "active-events-filter-v1",
        active_filter.usage,
    ):
        usage_records.append(record)

    result = group_articles_with_gemini(
        group_articles,
        mode="titles_summaries",
        client=client,
        active_events=active_filter.active_events,
    )
    if record := _usage_record("aggregation", AGGREGATION_PROMPT_VERSION, result.get("usage") or {}):
        usage_records.append(record)

    prompt_tokens = int((result.get("usage") or {}).get("promptTokenCount") or 0)
    candidates_tokens = int((result.get("usage") or {}).get("candidatesTokenCount") or 0)

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
                current_ids = {art.article_id for art in grp_arts}
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
            client=client,
            feeds_by_source=feeds_by_source,
        )
        if record := _usage_record(
            "newsworthiness",
            NEWSWORTHINESS_PROMPT_VERSION,
            scores_result.get("usage") or {},
        ):
            usage_records.append(record)
        api_scores = scores_result["scores_by_group_index"]
        fallback_count = scores_result["fallback_count"]
        news_elapsed_ms = scores_result.get("elapsed_ms")
        news_prompt_tokens = int((scores_result.get("usage") or {}).get("promptTokenCount") or 0)
        news_candidates_tokens = int((scores_result.get("usage") or {}).get("candidatesTokenCount") or 0)
    else:
        api_scores = {}
        fallback_count = 0
        news_elapsed_ms = 0
        news_prompt_tokens = 0
        news_candidates_tokens = 0

    merged_scores = {**scores_by_group_index, **api_scores}
    return CategoryBatchProcessingResult(
        batch=batch,
        grouping_result=result,
        scores_by_group_index=merged_scores,
        usage_records=tuple(usage_records),
        group_count=result["group_count"],
        singleton_count=result["singleton_count"],
        multi_article_group_count=result["multi_article_group_count"],
        validation_attempts=result["validation_attempts"],
        elapsed_ms=result.get("elapsed_ms"),
        prompt_tokens=prompt_tokens,
        candidates_tokens=candidates_tokens,
        news_scored=len(merged_scores),
        news_fallback_count=fallback_count,
        news_elapsed_ms=news_elapsed_ms,
        news_prompt_tokens=news_prompt_tokens,
        news_candidates_tokens=news_candidates_tokens,
    )


def _process_category_batches_llm(
    *,
    category_batches: Sequence[dict[str, Any]],
    active_rows_by_category: dict[str, list[dict[str, Any]]],
    client: JsonGenerator,
    feeds_by_source: dict[str, Any],
    concurrency: int,
) -> tuple[list[CategoryBatchProcessingResult | None], list[BaseException | None]]:
    results: list[CategoryBatchProcessingResult | None] = [None] * len(category_batches)
    errors: list[BaseException | None] = [None] * len(category_batches)
    if not category_batches:
        return results, errors
    worker_count = min(max(1, concurrency), len(category_batches))
    if worker_count == 1:
        for index, batch in enumerate(category_batches):
            try:
                results[index] = _process_category_batch_llm(
                    batch=batch,
                    active_rows_by_category=active_rows_by_category,
                    client=client,
                    feeds_by_source=feeds_by_source,
                )
            except BaseException as exc:
                errors[index] = exc
        return results, errors

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {
            executor.submit(
                _process_category_batch_llm,
                batch=batch,
                active_rows_by_category=active_rows_by_category,
                client=client,
                feeds_by_source=feeds_by_source,
            ): index
            for index, batch in enumerate(category_batches)
        }
        for future in as_completed(futures):
            index = futures[future]
            try:
                results[index] = future.result()
            except BaseException as exc:
                errors[index] = exc
    return results, errors


def aggregate_once(
    *,
    range_start: str | None = None,
    range_end: str | None = None,
    limit_windows: int | None = None,
    dry_run: bool = False,
    client: JsonGenerator | None = None,
    review_client: JsonGenerator | None = None,
    progress: Callable[[str], None] | None = None,
    force: bool = False,
    acquire_lock: bool = True,
    max_article_rowid: int | None = None,
    post_review: bool = True,
) -> dict[str, Any]:
    """Aggregate planned windows. ``post_review=False`` skips the coherence and
    deduplication passes so an intermediate run does not repeat them."""
    if (range_start is None) != (range_end is None):
        raise ValueError("range_start and range_end must be provided together or both omitted")
    config = load_pipeline_config()
    window_hours = int(config.aggregation.get("window_hours", 6))
    step_hours = int(config.aggregation.get("window_step_hours", window_hours))
    overlap_hours = int(config.aggregation.get("window_overlap_hours", 1))
    min_category_impact = float(config.aggregation.get("min_category_impact", 0.25))
    impact_floors = category_impact_floors(config.aggregation)
    category_batch_concurrency = _positive_int_config(
        config.aggregation.get("category_batch_concurrency"),
        DEFAULT_AGGREGATION_CATEGORY_BATCH_CONCURRENCY,
    )
    deduplication_concurrency = _positive_int_config(
        config.aggregation.get("deduplication_concurrency"),
        DEFAULT_DEDUPLICATION_CONCURRENCY,
    )
    deduplication_max_pairs = _positive_int_config(
        config.aggregation.get("deduplication_max_pairs_per_run"),
        DEFAULT_DEDUPLICATION_MAX_PAIRS_PER_RUN,
    )
    deduplication_max_passes = _positive_int_config(
        config.aggregation.get("deduplication_max_passes_per_run"),
        DEFAULT_DEDUPLICATION_MAX_PASSES_PER_RUN,
    )
    deduplication_lookback_hours = _positive_int_config(
        config.aggregation.get("deduplication_lookback_hours"),
        DEFAULT_DEDUPLICATION_LOOKBACK_HOURS,
    )
    lock_timeout = timedelta(minutes=int(config.pipeline.get("watchdog_timeout_minutes", 30)))
    run_id = f"aggregation-{uuid.uuid4().hex}"
    state = StateDB()
    owns_generator = client is None and not dry_run
    generator = client or (None if dry_run else create_gemini_client("bulk", purpose="aggregation"))
    owns_review_generator = review_client is None and client is None and not dry_run
    review_generator = review_client or (
        generator
        if client is not None or dry_run
        else create_gemini_client("review", include_lite=True, purpose="aggregation")
    )
    # Deduplication and coherence tolerate minutes of latency, so they get their
    # own clients with longer flex budgets when this run owns its clients.
    post_review_clients: list[Any] = []
    if owns_generator and post_review:
        dedup_generator = create_gemini_client("bulk", purpose="deduplication")
        dedup_review_generator = create_gemini_client("review", include_lite=True, purpose="deduplication")
        coherence_generator = create_gemini_client("review", purpose="coherence")
        post_review_clients = [dedup_generator, dedup_review_generator, coherence_generator]
    else:
        dedup_generator, dedup_review_generator, coherence_generator = (
            generator, review_generator, review_generator,
        )
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
        "min_category_impact_overrides": impact_floors,
        "window_hours": window_hours,
        "window_overlap_hours": overlap_hours,
        "window_step_hours": step_hours,
        "category_batch_concurrency": category_batch_concurrency,
        "deduplication_concurrency": deduplication_concurrency,
        "deduplication_max_pairs_per_run": deduplication_max_pairs,
        "deduplication_max_passes_per_run": deduplication_max_passes,
        "deduplication_lookback_hours": deduplication_lookback_hours,
        "events_merged": 0,
        "dry_run": dry_run,
        "force": force,
        "force_articles_reset": 0,
        "force_events_deleted": 0,
        "force_events_trimmed": 0,
        "bulk_model": getattr(generator, "model", None),
        "review_model": getattr(review_generator, "model", None),
        "max_article_rowid": max_article_rowid,
    }
    try:
        lock_context = PipelineLock(LOCK_PATH, lock_timeout, run_id=run_id) if acquire_lock else nullcontext()
        with lock_context:
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
                skip_window_planning = False
                if range_start is None:
                    from datetime import time

                    from pipeline.util import utc_now

                    ref = utc_now()
                    today_start = datetime.combine(ref.date(), time.min, tzinfo=UTC)
                    lookback_days = max(1, int(config.retention.get("staging_article_days", 1)))
                    limit_dt = today_start - timedelta(days=lookback_days)
                    if force:
                        bounds = _completed_digest_article_time_bounds(
                            state,
                            published_at_or_after=_format_iso_timestamp(limit_dt),
                            max_article_rowid=max_article_rowid,
                        )
                        if not bounds:
                            if progress:
                                progress("aggregate: no completed digest articles in the retention window")
                            skip_window_planning = True
                    else:
                        bounds = state.article_time_bounds(
                            unassigned_only=True,
                            max_article_rowid=max_article_rowid,
                        )
                        if not bounds:
                            skip_window_planning = True

                    if not skip_window_planning:
                        assert bounds is not None
                        bounds_start = _parse_iso_timestamp(bounds[0])
                        bounds_end = _parse_iso_timestamp(bounds[1])

                        if not force and bounds_end < limit_dt:
                            if progress:
                                progress("aggregate: no unassigned articles in the retention window")
                            skip_window_planning = True

                    if not skip_window_planning:
                        if bounds_start < limit_dt:
                            bounds_start = limit_dt

                        range_start = _format_iso_timestamp(
                            _floor_utc_interval(bounds_start, step_hours)
                        )
                        range_end = _format_iso_timestamp(
                            _floor_utc_interval(bounds_end, step_hours)
                            + timedelta(hours=step_hours)
                        )

                if skip_window_planning:
                    windows = []
                else:
                    assert range_start is not None and range_end is not None
                    windows = plan_sliding_windows(
                        range_start=range_start,
                        range_end=range_end,
                        window_hours=window_hours,
                        step_hours=step_hours,
                        overlap_hours=overlap_hours,
                        db=state,
                        force=force,
                        sparse=not force,
                        max_article_rowid=max_article_rowid,
                    )
                if limit_windows is not None:
                    windows = windows[:limit_windows]
                stats["windows_planned"] = len(windows)
                if progress:
                    progress(f"aggregate: planned {len(windows)} windows")

                if force and windows and not dry_run:
                    reset_start = windows[0].window_start
                    reset_end = max(windows, key=lambda w: _parse_iso_timestamp(w.window_end)).window_end
                    reset_stats = _force_reset_aggregation_range(
                        state,
                        range_start=reset_start,
                        range_end=reset_end,
                    )
                    stats["force_articles_reset"] = reset_stats["articles_reset"]
                    stats["force_events_deleted"] = reset_stats["events_deleted"]
                    stats["force_events_trimmed"] = reset_stats["events_trimmed"]
                    if progress:
                        progress(
                            "aggregate: force reset "
                            f"{reset_stats['articles_reset']} article assignment/filter state(s), "
                            f"deleted {reset_stats['events_deleted']} event(s), "
                            f"trimmed {reset_stats['events_trimmed']} event(s)"
                        )

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
                            max_article_rowid=max_article_rowid,
                            category_impact_floors=impact_floors,
                            feeds_by_source=feeds_by_source,
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
                        if dry_run:
                            stats["articles_seen"] += total_articles_in_window
                            stats["windows_processed"] += 1
                            stats["category_batches_planned"] = int(
                                stats.get("category_batches_planned", 0)
                            ) + len(category_batches)
                            if progress:
                                progress(
                                    "aggregate: dry-run planned "
                                    f"{len(category_batches)} category batch(es) for "
                                    f"{total_articles_in_window} article(s); no LLM calls made"
                                )
                            continue

                        # Pre-fetch active events once per window, partition by category in Python.
                        since = _recent_event_cutoff()
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

                        batch_results, batch_errors = _process_category_batches_llm(
                            category_batches=category_batches,
                            active_rows_by_category=active_rows_by_category,
                            client=generator,
                            feeds_by_source=feeds_by_source,
                            concurrency=category_batch_concurrency,
                        )

                        for batch, batch_result, batch_error in zip(
                            category_batches, batch_results, batch_errors, strict=True
                        ):
                            group_articles = batch["articles"]
                            if batch_error is not None:
                                group_exc = batch_error
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
                                continue

                            assert batch_result is not None
                            result = batch_result.grouping_result
                            if not dry_run:
                                _record_llm_usage_records(
                                    state,
                                    run_id=run_id,
                                    model=generator.model,
                                    usage_records=batch_result.usage_records,
                                )

                            window_group_count += batch_result.group_count
                            window_singleton_count += batch_result.singleton_count
                            window_multi_article_group_count += batch_result.multi_article_group_count
                            window_validation_attempts += batch_result.validation_attempts
                            if batch_result.elapsed_ms is not None:
                                window_elapsed_ms += batch_result.elapsed_ms
                            window_prompt_tokens += batch_result.prompt_tokens
                            window_candidates_tokens += batch_result.candidates_tokens
                            window_news_scored += batch_result.news_scored
                            window_news_fallback_count += batch_result.news_fallback_count
                            if batch_result.news_elapsed_ms is not None:
                                window_news_elapsed_ms += batch_result.news_elapsed_ms
                            window_news_prompt_tokens += batch_result.news_prompt_tokens
                            window_news_candidates_tokens += batch_result.news_candidates_tokens

                            if not dry_run:
                                applied = apply_grouping_result(
                                    articles=group_articles,
                                    groups=result["groups"],
                                    review_client=review_generator,
                                    state=state,
                                    scores_by_group_index=batch_result.scores_by_group_index,
                                    article_classifications=result.get("article_classifications"),
                                    feeds_by_source=feeds_by_source,
                                    run_id=run_id,
                                    progress=progress,
                                )
                                window_events_created += applied["events_created"]
                                window_events_updated += applied["events_updated"]
                                window_article_assignments += applied["article_assignments"]

                            processed_articles_count += len(group_articles)

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
                if not dry_run and not post_review:
                    stats["post_review_skipped"] = True
                    if progress:
                        progress("aggregate: coherence and deduplication deferred to the final pass")
                if not dry_run and post_review:
                    from pipeline.coherence import review_event_coherence
                    stats["coherence"] = review_event_coherence(
                        state=state, client=coherence_generator, run_id=run_id,
                        limit=int(config.aggregation.get("coherence_reviews_per_run", 10)), progress=progress,
                    )
                    try:
                        stats["events_merged"] = deduplicate_active_events_llm(
                            state=state,
                            client=dedup_generator,
                            review_client=dedup_review_generator,
                            feeds_by_source=feeds_by_source,
                            progress=progress,
                            run_id=run_id,
                            concurrency=deduplication_concurrency,
                            max_pairs=deduplication_max_pairs,
                            max_passes=deduplication_max_passes,
                            lookback_hours=deduplication_lookback_hours,
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
        if owns_review_generator and review_generator is not None:
            close = getattr(review_generator, "close", None)
            if callable(close):
                close()
        if owns_generator and generator is not None:
            close = getattr(generator, "close", None)
            if callable(close):
                close()
        for owned in post_review_clients:
            close = getattr(owned, "close", None)
            if callable(close):
                close()


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
    review_client: JsonGenerator | None = None,
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
    guarded_groups = []
    for group in groups:
        known = {articles[i].event_id for i in group["article_indexes"] if articles[i].event_id}
        target = _normalize_existing_event_id(group.get("existing_event_id"))
        if len(known | ({target} if target else set())) <= 1:
            guarded_groups.append(group)
            continue
        buckets: dict[str | None, list[int]] = {}
        for i in group["article_indexes"]:
            owner = articles[i].event_id or target
            buckets.setdefault(owner, []).append(i)
        for owner, indexes in buckets.items():
            guarded_groups.append({**group, "article_indexes": indexes, "existing_event_id": owner})
    groups = guarded_groups
    if review_client is not None:
        from pipeline.coherence import guard_event_extensions
        groups = guard_event_extensions(groups=groups, articles=articles, state=state,
                                         client=review_client, run_id=run_id)
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
            for other_id in winner_event_ids:
                try:
                    (STORY_DIR / f"{other_id}.json").unlink(missing_ok=True)
                except Exception:
                    pass

        merged_article_ids.update(art.article_id for art in group_articles)
        event_path = EVENT_DIR / f"{event_id}.json"

        # Sparse planning intentionally revisits the newest completed window so
        # late articles can attach to existing events. Do not turn that replay
        # into an event update when every grouped article is already present in
        # the same event: changing updated_at would trigger needless editorial
        # regeneration on every hourly run.
        if (
            not created
            and not winner_event_ids
            and existing is not None
            and {article.article_id for article in group_articles}
            <= set(existing.get("article_ids", []))
        ):
            continue

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
            if valid_existing_event_ids is None or existing_event_id in valid_existing_event_ids:
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
    title = str(event.get("title", ""))
    if not _headline_word_set(title):
        return False
    return any(_headlines_have_cohesion_edge(title, articles[index].headline) for index in indexes)


def _headline_cohesion_components(
    indexes: Sequence[int],
    articles: Sequence[ArticleForAggregation],
) -> list[list[int]]:
    neighbors = {index: set() for index in indexes}
    for left_pos, left in enumerate(indexes):
        for right in indexes[left_pos + 1 :]:
            if _headlines_have_cohesion_edge(articles[left].headline, articles[right].headline):
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
        for raw, word in _headline_token_pairs(text)
        if _is_headline_cohesion_word(raw, word)
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
    result = _filter_active_events_with_llm_result(
        articles=articles,
        active_events=active_events,
        client=client,
    )
    if state and run_id and result.usage:
        state.record_llm_usage(
            run_id=run_id,
            stage="active_events_filter",
            model=client.model,
            prompt_version="active-events-filter-v1",
            usage=result.usage,
        )
    return result.active_events


def _filter_active_events_with_llm_result(
    *,
    articles: Sequence[ArticleForAggregation],
    active_events: Sequence[dict[str, Any]],
    client: JsonGenerator,
) -> ActiveEventsFilterResult:
    if not articles or not active_events:
        return ActiveEventsFilterResult(active_events=[], usage={})

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
        )
        matched_ids = set(result.payload.get("matched_event_ids", []))
        return ActiveEventsFilterResult(
            active_events=[ev for ev in active_events if ev["event_id"] in matched_ids],
            usage=result.usage,
        )
    except Exception:
        # On LLM error, skip proactive matching for this group rather than flooding the
        # grouping prompt with the full unfiltered candidate list. Post-aggregation
        # deduplication will still catch any cross-window duplicates.
        return ActiveEventsFilterResult(active_events=[], usage={})


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
        "A cluster must describe ONE specific real-world development, decision, announcement or incident. "
        "Direct reactions and consequences belong with that development, but a shared company, person, "
        "country or topic is insufficient. Editorial cannot repair mixed event boundaries. "
        "A product rumor, CEO transition and investment are separate events even when all mention Apple. "
        "One payroll release and its reactions belong together; fuel prices do not belong to that event.\n"
        "A cluster still needs a specific shared news anchor. Ask whether a human editor could write one concrete "
        "headline for the combined cluster. If the only possible headline is a generic roundup like 'AI news', "
        "'French Open updates', 'Premier League stories', or 'Iran developments', split into smaller clusters.\n"
        "Clusters must be mutually exclusive. If one article could plausibly fit more than one cluster, assign it "
        "only to the strongest matching cluster. If no strongest cluster is obvious, make that article a singleton.\n"
        "A valid cluster should share a central named subject or incident: the same negotiation, attack, trial, "
        "election development, company announcement, product launch, disaster, death, recall, "
        "lawsuit, or official decision. Articles can emphasize different facts or perspectives and still belong "
        "together.\n"
        "Keep articles separate when they only share a broad beat or setting without a shared central subject: "
        "for example different companies in the same industry, different "
        "celebrities at the same festival, unrelated statements by the same politician, or separate incidents "
        "in the same country.\n"
        "For ongoing wars, negotiations, elections, trials, disasters, and major political stories, "
        "group related developments if they are part of the same immediate news arc in this window. Split only "
        "when they would require a clearly different headline and summary.\n"
        "Do not group unrelated stories merely because they share a keyword or domain such as AI, climate, "
        "markets, a holiday, a party, or a country.\n"
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
        "Entertainment releases, product updates, lifestyle items, and ads score lower "
        "unless they have unusually broad impact.\n"
        "category_score: importance within the story's own vertical/category. A film release may be low global "
        "but high entertainment; a major recall may be high automotive but moderate global.\n"
        "Use compact rationale_codes such as geopolitical_escalation, public_safety, mass_casualty_risk, "
        "major_policy, economic_impact, entertainment_major_release, niche_update, "
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

    if category in {"entertainment", "automotive"} and not set(rationale_codes) & {
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

_HEADLINE_COHESION_STOPWORDS = _KEYWORD_STOPWORDS | {
    "analysis",
    "briefing",
    "check",
    "explainer",
    "fact",
    "focus",
    "here",
    "live",
    "old",
    "opinion",
    "photo",
    "photos",
    "picture",
    "pictures",
    "takeaway",
    "takeaways",
    "thing",
    "things",
    "update",
    "updates",
    "video",
    "watch",
    "year",
}
_HEADLINE_COHESION_GENERIC_WORDS = {
    "administration",
    "advice",
    "america",
    "american",
    "americans",
    "case",
    "cases",
    "cells",
    "city",
    "claim",
    "claims",
    "concern",
    "concerns",
    "crisis",
    "decision",
    "district",
    "day",
    "dead",
    "deadly",
    "death",
    "deaths",
    "executive",
    "government",
    "health",
    "issue",
    "issues",
    "killed",
    "killing",
    "kills",
    "letter",
    "man",
    "memorial",
    "media",
    "open",
    "order",
    "people",
    "plan",
    "plans",
    "policy",
    "president",
    "probe",
    "program",
    "proposal",
    "researchers",
    "review",
    "school",
    "scientists",
    "social",
    "story",
    "warning",
    "warnings",
    "war",
    "weekend",
}
_HEADLINE_FORMAT_PREFIX_RE = re.compile(
    r"^\s*(?:"
    r"fact\s+focus|fact\s+check|explainer|analysis|opinion|live\s+updates?|"
    r"in\s+pictures|photos?|video|watch|what\s+to\s+know|things\s+to\s+know|"
    r"\d+\s+(?:big\s+)?takeaways?"
    r")\s*(?::|--|-|\.)?\s*",
    re.IGNORECASE,
)


def _normalize_headline_for_cohesion(text: str) -> str:
    previous = text.strip()
    while True:
        normalized = _HEADLINE_FORMAT_PREFIX_RE.sub("", previous, count=1).strip()
        if normalized == previous:
            return normalized
        previous = normalized


def _headline_token_pairs(text: str) -> list[tuple[str, str]]:
    normalized = _normalize_headline_for_cohesion(text)
    return [(match.group(0), match.group(0).lower()) for match in re.finditer(r"[A-Za-z0-9]+", normalized)]


def _is_headline_cohesion_word(raw: str, word: str) -> bool:
    if word in _HEADLINE_COHESION_STOPWORDS:
        return False
    if len(word) >= 3:
        return True
    if len(word) >= 2 and raw.isupper():
        return True
    return len(word) >= 2 and any(char.isdigit() for char in word)


def _is_headline_anchor_word(raw: str, word: str) -> bool:
    if not _is_headline_cohesion_word(raw, word):
        return False
    if word in _HEADLINE_COHESION_GENERIC_WORDS:
        return False
    if any(char.isdigit() for char in word):
        return len(word) >= 2
    if raw.isupper() and len(raw) >= 2:
        return True
    return raw[:1].isupper()


def _headline_anchor_word_set(text: str) -> set[str]:
    return {
        word
        for raw, word in _headline_token_pairs(text)
        if _is_headline_anchor_word(raw, word)
    }


def _shared_words_are_only_generic(shared_words: set[str]) -> bool:
    return bool(shared_words) and shared_words <= _HEADLINE_COHESION_GENERIC_WORDS


def _headline_cohesion_strength(left: str, right: str) -> int:
    """Return 2 for three or more shared non-generic words, 1 for the weaker
    two-word anchor match, and 0 when the headlines do not cohere."""
    left_words = _headline_word_set(left)
    right_words = _headline_word_set(right)
    shared_words = left_words & right_words
    if len(shared_words) >= 3:
        return 0 if _shared_words_are_only_generic(shared_words) else 2
    if len(shared_words) < 2:
        return 0
    if _shared_words_are_only_generic(shared_words):
        return 0

    shared_anchors = (
        _headline_anchor_word_set(left)
        & _headline_anchor_word_set(right)
        & shared_words
    )
    if len(shared_anchors) >= 2:
        return 1
    weak = bool(shared_anchors) and any(
        word not in _HEADLINE_COHESION_GENERIC_WORDS for word in shared_words - shared_anchors
    )
    return 1 if weak else 0


def _headlines_have_cohesion_edge(left: str, right: str) -> bool:
    return _headline_cohesion_strength(left, right) > 0


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
    loser_event = _read_event(loser_path) or {}
    if not winner_event:
        return

    state.merge_events_into([loser_id], winner_id)

    try:
        loser_path.unlink(missing_ok=True)
    except Exception:
        pass
    try:
        (STORY_DIR / f"{loser_id}.json").unlink(missing_ok=True)
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
    event_payload["updated_at"] = max(winner_event["updated_at"], loser_event.get("updated_at", ""))
    atomic_write_json(winner_path, event_payload)
    state.upsert_event(event_payload, winner_path)
    with state.conn:
        state.conn.execute("UPDATE events SET last_editorial_at = NULL WHERE event_id = ?", (winner_id,))


def _load_event_articles_summary(event_id: str, state: StateDB) -> list[dict[str, str]]:
    return _load_events_articles_summary({event_id}, state).get(event_id, [])


def _load_events_articles_summary(
    event_ids: set[str],
    state: StateDB,
) -> dict[str, list[dict[str, str]]]:
    if not event_ids:
        return {}

    placeholders = ",".join("?" for _ in event_ids)
    rows = state.conn.execute(
        f"""
        SELECT event_id, headline, summary, article_path
        FROM articles
        WHERE event_id IN ({placeholders})
          AND is_filtered = 0
        ORDER BY event_id, published_at, article_id
        """,
        tuple(sorted(event_ids)),
    ).fetchall()
    summaries_by_event: dict[str, list[dict[str, str]]] = {}
    digest_cache: dict[str, dict[str, Any]] = {}
    for row in rows:
        digest = digest_cache.get(row["article_path"])
        if digest is None:
            digest = _load_digest_fields(row["article_path"])
            digest_cache[row["article_path"]] = digest
        summary = digest.get("digest_summary") or row["summary"] or ""
        summaries_by_event.setdefault(row["event_id"], []).append({
            "headline": row["headline"],
            "summary": summary[:300] + "..." if len(summary) > 300 else summary
        })
    return summaries_by_event


def _build_event_merge_prompt(event1: dict[str, Any], event2: dict[str, Any]) -> str:
    return (
        "Compare these two news event clusters and decide if a careful news editor would cover "
        "them as one evolving homepage story rather than two entries.\n"
        "Merge them when they cover the same core development, announcement, decision, or "
        "incident, including immediate reaction, consequences, implementation details, or a "
        "different reporting angle that can be summarized accurately in one entry. Do not require "
        "matching headlines or identical emphasis.\n"
        "A pair is mergeable only if ALL constituent reports fit that core development. "
        "Reject a merge that would absorb unrelated reports from an already contaminated cluster. "
        "They should remain separate if they are different incidents, "
        "unrelated actions by the same actor, or materially distinct developments in a broader "
        "topic that deserve separate headlines (e.g., separate attacks, policy announcements, "
        "court rulings, product launches, or trials). Do not merge merely because both belong to "
        "the same war, region, company, election, or continuing issue.\n\n"
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


def _evaluate_deduplication_pair(
    *,
    client: JsonGenerator,
    payload1: dict[str, Any],
    payload2: dict[str, Any],
) -> DeduplicationPairDecision:
    result = client.generate_json(
        system_instruction=(
            "You are an expert news editor. Determine if two event clusters belong in "
            "one evolving homepage story and should be merged."
        ),
        prompt=_build_event_merge_prompt(payload1, payload2),
        response_schema=_event_merge_response_schema(),
    )
    should_merge = result.payload.get("should_merge") is True
    raw_confidence = result.payload.get("confidence")
    confidence = (
        float(raw_confidence)
        if isinstance(raw_confidence, int | float) and not isinstance(raw_confidence, bool)
        else 0.0
    )
    rationale = result.payload.get("rationale", "")
    return DeduplicationPairDecision(
        should_merge=should_merge,
        confidence=confidence,
        rationale=str(rationale),
        usage=result.usage,
        model=result.model,
    )


def _evaluate_and_apply_deduplication_candidates(
    *,
    candidates: Sequence[tuple[dict[str, Any], dict[str, Any]]],
    state: StateDB,
    client: JsonGenerator,
    feeds_by_source: dict[str, Any],
    progress: Callable[[str], None] | None = None,
    run_id: str | None = None,
    concurrency: int = DEFAULT_DEDUPLICATION_CONCURRENCY,
    usage_stage: str = "deduplication",
    prompt_version: str = "deduplication-v1",
) -> int:
    pending = list(candidates)
    merges_count = 0
    worker_count = max(1, concurrency)
    event_articles_by_id = _load_events_articles_summary(
        {
            event["event_id"]
            for candidate in candidates
            for event in candidate
        },
        state,
    )

    while pending:
        round_candidates: list[tuple[dict[str, Any], dict[str, Any]]] = []
        deferred: list[tuple[dict[str, Any], dict[str, Any]]] = []
        event_ids_in_round: set[str] = set()
        for e1, e2 in pending:
            pair_ids = {e1["event_id"], e2["event_id"]}
            if event_ids_in_round & pair_ids:
                deferred.append((e1, e2))
                continue
            round_candidates.append((e1, e2))
            event_ids_in_round.update(pair_ids)

        payloads: list[tuple[dict[str, Any], dict[str, Any]] | None] = [None] * len(round_candidates)
        for index, (e1, e2) in enumerate(round_candidates):
            if not state.event_exists(e1["event_id"]) or not state.event_exists(e2["event_id"]):
                continue

            articles1 = event_articles_by_id.get(e1["event_id"], [])
            articles2 = event_articles_by_id.get(e2["event_id"], [])

            if not articles1 or not articles2:
                continue

            payloads[index] = (
                {"event_id": e1["event_id"], "title": e1["title"], "articles": articles1},
                {"event_id": e2["event_id"], "title": e2["title"], "articles": articles2},
            )

        decisions: list[DeduplicationPairDecision | None] = [None] * len(round_candidates)
        errors: list[BaseException | None] = [None] * len(round_candidates)
        submittable = [(index, payload) for index, payload in enumerate(payloads) if payload is not None]
        current_workers = min(worker_count, len(submittable))
        if current_workers <= 1:
            for index, (payload1, payload2) in submittable:
                try:
                    decisions[index] = _evaluate_deduplication_pair(
                        client=client,
                        payload1=payload1,
                        payload2=payload2,
                    )
                except BaseException as exc:
                    errors[index] = exc
        elif submittable:
            with ThreadPoolExecutor(max_workers=current_workers) as executor:
                futures = {
                    executor.submit(
                        _evaluate_deduplication_pair,
                        client=client,
                        payload1=payload[0],
                        payload2=payload[1],
                    ): index
                    for index, payload in submittable
                }
                for future in as_completed(futures):
                    index = futures[future]
                    try:
                        decisions[index] = future.result()
                    except BaseException as exc:
                        errors[index] = exc

        for index, (e1, e2) in enumerate(round_candidates):
            error = errors[index]
            if error is not None:
                if progress:
                    progress(
                        f"deduplicate: failed to evaluate pair {e1['event_id']} and {e2['event_id']}: {error}"
                    )
                if run_id:
                    try:
                        state.record_error(
                            run_id,
                            "deduplication",
                            "event_pair",
                            f"{e1['event_id']}_{e2['event_id']}",
                            None,
                            error if isinstance(error, Exception) else RuntimeError(str(error)),
                        )
                    except Exception:
                        pass
                continue

            decision = decisions[index]
            if decision is None:
                continue

            if run_id and decision.usage:
                try:
                    state.record_llm_usage(
                        run_id=run_id,
                        stage=usage_stage,
                        model=decision.model,
                        prompt_version=prompt_version,
                        usage=decision.usage,
                    )
                except Exception:
                    pass

            lite_decision = decision.model.endswith("-lite")
            if not lite_decision:
                state.record_deduplication_review(
                    event_a=e1["event_id"],
                    event_b=e2["event_id"],
                    event_a_updated_at=e1["updated_at"],
                    event_b_updated_at=e2["updated_at"],
                    should_merge=decision.should_merge,
                    confidence=decision.confidence,
                    rationale=decision.rationale,
                    model=decision.model,
                    prompt_version=prompt_version,
                )
            if (
                decision.should_merge
                and decision.confidence >= DEDUPLICATION_MERGE_CONFIDENCE_THRESHOLD
                and not lite_decision
            ):
                if not state.event_exists(e1["event_id"]) or not state.event_exists(e2["event_id"]):
                    continue

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
                        f"(confidence: {decision.confidence:.2f}; rationale: {decision.rationale})"
                    )

                merge_events(winner_id, loser_id, state, feeds_by_source)
                event_articles_by_id[winner_id] = _load_event_articles_summary(winner_id, state)
                event_articles_by_id.pop(loser_id, None)
                merges_count += 1
            elif decision.should_merge and progress:
                reason = (
                    "deferred Flash-Lite merge for full-Flash review"
                    if lite_decision
                    else "skipped low-confidence merge"
                )
                progress(
                    f"deduplicate: {reason} for {e1['event_id']} and {e2['event_id']} "
                    f"(confidence: {decision.confidence:.2f})"
                )

        pending = deferred

    return merges_count


def _dynamic_keyword_stopwords(
    events_with_keywords: Sequence[tuple[str, Sequence[str]]],
    *,
    threshold: float = DEDUPLICATION_HOT_STOPWORD_THRESHOLD,
    min_events: int = 8,
    absolute_floor: int = 4,
) -> set[str]:
    """Compute per-batch stopwords: any keyword appearing in more than `threshold` of events
    in this batch is treated as too generic to distinguish duplicates.

    Guards against over-stripping on small batches:
    - `min_events`: batches with fewer than this many events get no dynamic stopwords at all.
    - `absolute_floor`: a keyword must appear in at least this many events to be considered hot,
      regardless of the percentage. This keeps distinctive entity words (e.g. "ferrari" in a
      10-event leisure batch where it appears in exactly the 2 candidate duplicates) from being
      stripped before they can be matched."""
    if len(events_with_keywords) < min_events:
        return set()
    counts: Counter[str] = Counter()
    for _, keywords in events_with_keywords:
        seen: set[str] = set()
        for kw in keywords:
            word = kw.lower().strip()
            if word and word not in seen:
                counts[word] += 1
                seen.add(word)
    cutoff = max(absolute_floor, int(len(events_with_keywords) * threshold))
    return {word for word, count in counts.items() if count >= cutoff}


def _filtered_event_keywords(
    keywords: Sequence[str],
    dynamic_stopwords: set[str],
    *,
    max_n: int = DEDUPLICATION_KEYWORDS_PER_EVENT,
) -> list[str]:
    filtered: list[str] = []
    seen: set[str] = set()
    for kw in keywords:
        word = kw.lower().strip()
        if not word or word in seen:
            continue
        if word in _KEYWORD_STOPWORDS or word in dynamic_stopwords:
            continue
        filtered.append(word)
        seen.add(word)
        if len(filtered) >= max_n:
            break
    return filtered


def _keyword_overlap_candidates(
    events: Sequence[dict[str, Any]],
    dynamic_stopwords: set[str],
    *,
    min_overlap: int = DEDUPLICATION_KEYWORD_OVERLAP_MIN,
    max_keywords: int = DEDUPLICATION_KEYWORDS_PER_EVENT,
) -> list[tuple[str, str]]:
    """Return pairs of event_ids whose filtered keyword sets share at least `min_overlap` tokens.
    Caller is responsible for restricting `events` to a single category group."""
    filtered_by_id: dict[str, set[str]] = {}
    for event in events:
        kws = _filtered_event_keywords(
            event.get("keywords") or [], dynamic_stopwords, max_n=max_keywords
        )
        filtered_by_id[event["event_id"]] = set(kws)

    pairs: list[tuple[str, str]] = []
    ids = list(filtered_by_id.keys())
    for i, eid1 in enumerate(ids):
        kw1 = filtered_by_id[eid1]
        if len(kw1) < min_overlap:
            continue
        for eid2 in ids[i + 1 :]:
            kw2 = filtered_by_id[eid2]
            if len(kw1 & kw2) >= min_overlap:
                pairs.append((eid1, eid2))
    return pairs


def _build_prescreen_prompt(events_payload: Sequence[dict[str, Any]]) -> str:
    return (
        "You are screening a list of active news events for potential duplicates that should "
        "be reviewed by a strict per-pair merge step.\n"
        "Each event has an ID, title, a short list of distinctive keywords, and the top article "
        "headlines.\n"
        "Return ALL pairs of events that MIGHT describe the same underlying news event: the same "
        "incident, same launch, same announcement, same negotiation, or same development covered "
        "from a different angle or framing. Err on the side of inclusion when in doubt; a separate "
        "strict review will reject false positives.\n"
        "INCLUDE pairs when:\n"
        "- One outlet's framing (e.g. consumer-tech blog) and another's (e.g. trade press) "
        "appear to describe the same product launch, court ruling, or incident.\n"
        "- The same named entity is the subject of both events and the underlying event sounds "
        "like the same news beat.\n"
        "- Two events plausibly cover the same incident in different sub-stories (e.g. an attack "
        "and reactions to that attack on the same day).\n"
        "EXCLUDE pairs when:\n"
        "- The events are different incidents within the same broader topic (e.g. two separate "
        "attacks in the same conflict, two separate games in the same league, two separate "
        "product launches by the same company).\n"
        "- The events share only a general topic, beat, or actor without a shared specific event.\n"
        "Return JSON only matching this schema:\n"
        "{ \"candidate_pairs\": [ {\"event_a\": \"<id>\", \"event_b\": \"<id>\", "
        "\"reason\": \"<short>\"} ] }\n"
        "If no candidates, return {\"candidate_pairs\": []}.\n\n"
        f"Events:\n{json.dumps(list(events_payload), ensure_ascii=False, separators=(',', ':'))}"
    )


def _prescreen_response_schema() -> dict[str, Any]:
    return {
        "type": "OBJECT",
        "properties": {
            "candidate_pairs": {
                "type": "ARRAY",
                "items": {
                    "type": "OBJECT",
                    "properties": {
                        "event_a": {"type": "STRING"},
                        "event_b": {"type": "STRING"},
                        "reason": {"type": "STRING"},
                    },
                    "required": ["event_a", "event_b"],
                },
            },
        },
        "required": ["candidate_pairs"],
    }


def _run_prescreen_chunk(
    *,
    chunk_label: str,
    chunk: Sequence[dict[str, Any]],
    valid_ids: set[str],
    article_headlines_by_event: dict[str, list[str]],
    dynamic_stopwords: set[str],
    client: JsonGenerator,
) -> PrescreenChunkResult:
    payload = [
        {
            "id": event["event_id"],
            "title": event["title"],
            "keywords": _filtered_event_keywords(event.get("keywords") or [], dynamic_stopwords),
            "headlines": list(article_headlines_by_event.get(event["event_id"], []))[
                :DEDUPLICATION_HEADLINES_PER_EVENT
            ],
        }
        for event in chunk
    ]

    result = client.generate_json(
        system_instruction=(
            "You are an expert news editor screening event clusters for possible "
            "duplicates. Recall matters more than precision at this stage."
        ),
        prompt=_build_prescreen_prompt(payload),
        response_schema=_prescreen_response_schema(),
    )

    pairs: list[tuple[str, str]] = []
    raw_pairs = result.payload.get("candidate_pairs") or []
    for entry in raw_pairs:
        if not isinstance(entry, dict):
            continue
        a = entry.get("event_a")
        b = entry.get("event_b")
        if not isinstance(a, str) or not isinstance(b, str):
            continue
        if a == b or a not in valid_ids or b not in valid_ids:
            continue
        pairs.append((a, b))
    return PrescreenChunkResult(
        chunk_label=chunk_label,
        event_count=len(chunk),
        pairs=tuple(pairs),
        usage=result.usage,
    )


def _prescreen_chunk_specs_for_events(
    events: Sequence[dict[str, Any]],
    dynamic_stopwords: set[str],
    *,
    batch_label: str,
    anchor_count: int = DEDUPLICATION_PRESCREEN_ANCHOR_EVENTS,
) -> list[PrescreenChunkSpec]:
    if len(events) < 2:
        return []

    valid_ids = frozenset(event["event_id"] for event in events)
    chunk_size = DEDUPLICATION_MAX_EVENTS_PER_PRESCREEN_BATCH
    if len(events) <= chunk_size:
        chunks = [tuple(sorted(events, key=lambda event: event["event_id"]))]
    else:
        anchors = _prescreen_anchor_events(events, anchor_count=anchor_count)
        anchor_ids = {event["event_id"] for event in anchors}
        non_anchor_events = [event for event in events if event["event_id"] not in anchor_ids]
        non_anchor_chunk_size = max(1, chunk_size - len(anchors))
        # Bucket by a hash of the event ID so one new event perturbs only its own
        # chunk; sequential slicing would shift every chunk and defeat the cache.
        bucket_count = max(1, math.ceil(len(non_anchor_events) / non_anchor_chunk_size))
        buckets: list[list[dict[str, Any]]] = [[] for _ in range(bucket_count)]
        for event in sorted(non_anchor_events, key=lambda item: item["event_id"]):
            digest = hashlib.sha256(str(event["event_id"]).encode("utf-8")).hexdigest()
            buckets[int(digest[:8], 16) % bucket_count].append(event)
        chunks = []
        for bucket in buckets:
            for start in range(0, len(bucket), non_anchor_chunk_size):
                chunks.append(tuple([*anchors, *bucket[start : start + non_anchor_chunk_size]]))

    return [
        PrescreenChunkSpec(
            chunk_label=batch_label if len(chunks) == 1 else f"{batch_label}-{chunk_index}",
            chunk=chunk,
            valid_ids=valid_ids,
            dynamic_stopwords=frozenset(dynamic_stopwords),
        )
        for chunk_index, chunk in enumerate(chunks, start=1)
    ]


def _prescreen_anchor_events(
    events: Sequence[dict[str, Any]],
    *,
    anchor_count: int = DEDUPLICATION_PRESCREEN_ANCHOR_EVENTS,
) -> list[dict[str, Any]]:
    if anchor_count <= 0:
        return []
    return sorted(
        events,
        key=lambda event: (
            -(int(event.get("article_count") or 0)),
            event.get("created_at") or "",
            event["event_id"],
        ),
    )[: min(anchor_count, len(events) - 1)]


def _prescreen_chunk_signature(
    spec: PrescreenChunkSpec, article_headlines_by_event: dict[str, list[str]]
) -> str:
    """Content signature of the chunk: IDs, titles, raw keywords and headlines.

    Raw keywords are used instead of the stopword-filtered list because the
    per-batch dynamic stopwords shift whenever the batch changes; that would
    invalidate every chunk each hour for a cosmetic prompt difference."""
    payload = [
        [
            event["event_id"],
            event.get("title"),
            [str(k).lower() for k in (event.get("keywords") or [])[:DEDUPLICATION_KEYWORDS_PER_EVENT]],
            sorted(article_headlines_by_event.get(event["event_id"], []))[:DEDUPLICATION_HEADLINES_PER_EVENT],
        ]
        for event in spec.chunk
    ]
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()


def _run_prescreen_chunk_spec(
    *,
    spec: PrescreenChunkSpec,
    article_headlines_by_event: dict[str, list[str]],
    client: JsonGenerator,
) -> PrescreenChunkResult:
    return _run_prescreen_chunk(
        chunk_label=spec.chunk_label,
        chunk=spec.chunk,
        valid_ids=set(spec.valid_ids),
        article_headlines_by_event=article_headlines_by_event,
        dynamic_stopwords=set(spec.dynamic_stopwords),
        client=client,
    )


def _execute_prescreen_chunk_specs(
    specs: Sequence[PrescreenChunkSpec],
    article_headlines_by_event: dict[str, list[str]],
    *,
    client: JsonGenerator,
    progress: Callable[[str], None] | None = None,
    state: StateDB | None = None,
    run_id: str | None = None,
    concurrency: int = DEFAULT_DEDUPLICATION_CONCURRENCY,
) -> list[tuple[str, str]]:
    if not specs:
        return []

    collected: list[tuple[str, str]] = []
    signatures = {spec.chunk_label: _prescreen_chunk_signature(spec, article_headlines_by_event) for spec in specs}
    # Unchanged chunks (same events, titles, keywords and headlines) reuse the
    # cached prescreen instead of paying for an identical call every hour.
    cache_hits = 0
    pending: list[PrescreenChunkSpec] = []
    for spec in specs:
        cached = (
            state.get_cached_prescreen_pairs(
                chunk_signature=signatures[spec.chunk_label],
                prompt_version=DEDUPLICATION_PRESCREEN_PROMPT_VERSION,
            )
            if state is not None
            else None
        )
        if cached is not None:
            cache_hits += 1
            collected.extend(pair for pair in cached if pair[0] in spec.valid_ids and pair[1] in spec.valid_ids)
        else:
            pending.append(spec)
    if progress and cache_hits:
        progress(f"deduplicate: prescreen cache reused {cache_hits} chunk(s); {len(pending)} to run")
    specs = pending
    if not specs:
        return collected

    def record_success(result: PrescreenChunkResult) -> None:
        if state is not None and run_id and result.usage:
            try:
                state.record_llm_usage(
                    run_id=run_id,
                    stage="deduplication_prescreen",
                    model=client.model,
                    prompt_version=DEDUPLICATION_PRESCREEN_PROMPT_VERSION,
                    usage=result.usage,
                )
            except Exception:
                pass
        if state is not None:
            try:
                state.put_cached_prescreen_pairs(
                    chunk_signature=signatures[result.chunk_label],
                    prompt_version=DEDUPLICATION_PRESCREEN_PROMPT_VERSION,
                    model=client.model,
                    pairs=result.pairs,
                )
            except Exception:
                pass
        collected.extend(result.pairs)
        if progress:
            progress(
                f"deduplicate: prescreen[{result.chunk_label}] over {result.event_count} events returned "
                f"{len(result.pairs)} candidate pair(s)"
            )

    def record_error(spec: PrescreenChunkSpec, exc: BaseException) -> None:
        if progress:
            progress(f"deduplicate: prescreen[{spec.chunk_label}] failed: {exc}")
        if state is not None and run_id:
            try:
                state.record_error(
                    run_id,
                    "deduplication_prescreen",
                    "batch",
                    spec.chunk_label,
                    None,
                    exc if isinstance(exc, Exception) else RuntimeError(str(exc)),
                )
            except Exception:
                pass

    worker_count = min(max(1, concurrency), len(specs))
    if worker_count == 1:
        for spec in specs:
            try:
                record_success(
                    _run_prescreen_chunk_spec(
                        spec=spec,
                        article_headlines_by_event=article_headlines_by_event,
                        client=client,
                    )
                )
            except BaseException as exc:
                record_error(spec, exc)
        return collected

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {
            executor.submit(
                _run_prescreen_chunk_spec,
                spec=spec,
                article_headlines_by_event=article_headlines_by_event,
                client=client,
            ): spec
            for spec in specs
        }
        for future in as_completed(futures):
            spec = futures[future]
            try:
                record_success(future.result())
            except BaseException as exc:
                record_error(spec, exc)

    return collected


def _llm_prescreen_candidates(
    events: Sequence[dict[str, Any]],
    article_headlines_by_event: dict[str, list[str]],
    dynamic_stopwords: set[str],
    *,
    client: JsonGenerator,
    batch_label: str,
    progress: Callable[[str], None] | None = None,
    state: StateDB | None = None,
    run_id: str | None = None,
    concurrency: int = DEFAULT_DEDUPLICATION_CONCURRENCY,
) -> list[tuple[str, str]]:
    """Run a loose LLM pre-screen over a single category-group batch of events.
    Returns event_id pairs the LLM thinks MIGHT be duplicates. The per-pair merge LLM
    is the strict filter that prevents over-merging."""
    specs = _prescreen_chunk_specs_for_events(events, dynamic_stopwords, batch_label=batch_label)
    return _execute_prescreen_chunk_specs(
        specs,
        article_headlines_by_event,
        client=client,
        progress=progress,
        state=state,
        run_id=run_id,
        concurrency=concurrency,
    )


def deduplicate_active_events_llm(
    *,
    state: StateDB,
    client: JsonGenerator,
    review_client: JsonGenerator | None = None,
    feeds_by_source: dict[str, Any],
    progress: Callable[[str], None] | None = None,
    run_id: str | None = None,
    concurrency: int = DEFAULT_DEDUPLICATION_CONCURRENCY,
    max_pairs: int | None = None,
    max_passes: int = DEDUPLICATION_MAX_PASSES,
    lookback_hours: int = DEFAULT_DEDUPLICATION_LOOKBACK_HOURS,
) -> int:
    total_merges = 0
    for pass_index in range(1, max(1, max_passes) + 1):
        merges_count = _deduplicate_active_events_llm_pass(
            state=state,
            client=client,
            review_client=review_client,
            feeds_by_source=feeds_by_source,
            progress=progress,
            run_id=run_id,
            concurrency=concurrency,
            max_pairs=max_pairs,
            lookback_hours=lookback_hours,
        )
        total_merges += merges_count
        if merges_count == 0:
            break
        if progress and pass_index < max(1, max_passes):
            progress(
                f"deduplicate: pass {pass_index} merged {merges_count} event(s); "
                "checking for newly exposed duplicates"
            )
    return total_merges


def _deduplicate_active_events_llm_pass(
    *,
    state: StateDB,
    client: JsonGenerator,
    review_client: JsonGenerator | None = None,
    feeds_by_source: dict[str, Any],
    progress: Callable[[str], None] | None = None,
    run_id: str | None = None,
    concurrency: int = DEFAULT_DEDUPLICATION_CONCURRENCY,
    max_pairs: int | None = None,
    lookback_hours: int = DEFAULT_DEDUPLICATION_LOOKBACK_HOURS,
) -> int:
    since = _recent_event_cutoff(lookback_hours)
    try:
        state.prune_cached_prescreens(older_than=_recent_event_cutoff(24 * 7))
    except Exception:
        pass

    rows = state.conn.execute(
        """
        SELECT event_id, title, category, updated_at, article_count, created_at, keywords_json
        FROM events
        WHERE status IN ('active', 'stale') AND updated_at >= ?
        ORDER BY category, updated_at DESC
        """,
        (since,),
    ).fetchall()

    events = [dict(row) for row in rows]
    if len(events) < 2:
        return 0
    for event in events:
        published = _read_event(STORY_DIR / f"{event['event_id']}.json")
        if published and isinstance(published.get("headline"), str):
            event["original_title"] = event["title"]
            event["title"] = published["headline"]
        try:
            event["keywords"] = json.loads(event.get("keywords_json") or "[]") or []
        except (TypeError, ValueError):
            event["keywords"] = []
    article_headlines_by_event: dict[str, list[str]] = {}
    for row in state.conn.execute(
        """
        SELECT event_id, headline
        FROM articles
        WHERE event_id IS NOT NULL
          AND is_filtered = 0
        ORDER BY event_id, published_at, article_id
        """
    ).fetchall():
        article_headlines_by_event.setdefault(row["event_id"], []).append(row["headline"])

    candidate_pairs: set[frozenset[str]] = set()
    candidate_priorities: dict[frozenset[str], int] = {}
    events_by_id = {event["event_id"]: event for event in events}

    # Heuristic candidates: slug match, title overlap, title cohesion,
    # or article-headline match. Title cohesion can cross category groups; the
    # strict per-pair merge review remains the precision gate.
    for i in range(len(events)):
        for j in range(i + 1, len(events)):
            e1 = events[i]
            e2 = events[j]
            slug_match = _base_slug(e1["event_id"]) == _base_slug(e2["event_id"])
            title_match = _titles_similar(e1["title"], e2["title"])
            cohesion_strength = 0
            if not (slug_match or title_match):
                cohesion_strength = _headline_cohesion_strength(e1["title"], e2["title"])
            headline_match = False
            if (
                not (slug_match or title_match)
                and _titles_share_at_least(e1["title"], e2["title"], 2)
            ):
                headline_match = _events_have_similar_article_headline(
                    e1["event_id"],
                    e2["event_id"],
                    article_headlines_by_event,
                )
            if slug_match or title_match:
                priority = DEDUPLICATION_PRIORITY_TITLE
            elif headline_match:
                priority = DEDUPLICATION_PRIORITY_HEADLINE
            elif cohesion_strength >= 2:
                priority = DEDUPLICATION_PRIORITY_COHESION
            elif cohesion_strength == 1:
                priority = DEDUPLICATION_PRIORITY_WEAK_COHESION
            else:
                continue
            pair = frozenset((e1["event_id"], e2["event_id"]))
            candidate_pairs.add(pair)
            candidate_priorities[pair] = max(candidate_priorities.get(pair, -1), priority)
    heuristic_count = len(candidate_pairs)

    # Keyword-overlap candidates + LLM pre-screen are scoped to category-group
    # batches. The pre-screen chunks are collected here and executed below in
    # one shared worker pool.
    events_by_group: dict[str, list[dict[str, Any]]] = {}
    parent_events_by_group: dict[str, list[dict[str, Any]]] = {}
    for event in events:
        cat = event.get("category") or ""
        group = _category_group_for_category(cat)
        parent_events_by_group.setdefault(group["name"], []).append(event)
        if group["name"] == "news_business" and cat in group["categories"]:
            batch_key = f"news_business_{cat}"
        else:
            batch_key = group["name"]
        events_by_group.setdefault(batch_key, []).append(event)

    keyword_added = 0
    prescreen_added = 0
    prescreen_specs: list[PrescreenChunkSpec] = []
    for batch_key, batch_events in events_by_group.items():
        if len(batch_events) < 2:
            continue
        kw_events = [(event["event_id"], event.get("keywords") or []) for event in batch_events]
        dynamic_stopwords = _dynamic_keyword_stopwords(kw_events)
        for eid1, eid2 in _keyword_overlap_candidates(batch_events, dynamic_stopwords):
            pair = frozenset((eid1, eid2))
            if pair not in candidate_pairs:
                candidate_pairs.add(pair)
                keyword_added += 1
            candidate_priorities[pair] = max(
                candidate_priorities.get(pair, -1), DEDUPLICATION_PRIORITY_KEYWORD
            )
        prescreen_specs.extend(
            _prescreen_chunk_specs_for_events(
                batch_events,
                dynamic_stopwords,
                batch_label=batch_key,
            )
        )

    # news_business is intentionally split by category for regular grouping to avoid
    # giant prompts, but duplicate events can straddle world/business/us framing
    # (for example, a market-reaction story and the underlying geopolitical event).
    # Add a high-recall parent prescreen pass; the strict per-pair merge LLM still
    # decides whether any surfaced pair is actually safe to merge.
    news_business_events = parent_events_by_group.get("news_business", [])
    news_business_categories = {event.get("category") for event in news_business_events}
    if len(news_business_categories) > 1:
        kw_events = [
            (event["event_id"], event.get("keywords") or []) for event in news_business_events
        ]
        dynamic_stopwords = _dynamic_keyword_stopwords(kw_events)
        prescreen_specs.extend(
            _prescreen_chunk_specs_for_events(
                news_business_events,
                dynamic_stopwords,
                batch_label="news_business_cross_category",
            )
        )

    prescreen_pairs = _execute_prescreen_chunk_specs(
        prescreen_specs,
        article_headlines_by_event,
        client=client,
        progress=progress,
        state=state,
        run_id=run_id,
        concurrency=concurrency,
    )
    for eid1, eid2 in prescreen_pairs:
        pair = frozenset((eid1, eid2))
        if pair not in candidate_pairs:
            candidate_pairs.add(pair)
            prescreen_added += 1
        candidate_priorities[pair] = max(
            candidate_priorities.get(pair, -1), DEDUPLICATION_PRIORITY_PRESCREEN
        )

    if not candidate_pairs:
        return 0

    if progress:
        progress(
            f"deduplicate: candidate pairs total={len(candidate_pairs)} "
            f"(heuristic={heuristic_count}, keyword_overlap_new={keyword_added}, "
            f"prescreen_new={prescreen_added})"
        )

    candidates: list[tuple[dict[str, Any], dict[str, Any]]] = []
    cached_count = 0
    ordered_pairs = _ordered_deduplication_candidate_pairs(
        candidate_pairs,
        candidate_priorities=candidate_priorities,
        events_by_id=events_by_id,
    )
    adjudicator = review_client or client
    is_second_pass_review = adjudicator is not client or adjudicator.model != client.model
    prompt_version = (
        DEDUPLICATION_REVIEW_PROMPT_VERSION
        if is_second_pass_review
        else "deduplication-v1"
    )
    for pair in ordered_pairs:
        ids = sorted(pair)
        if len(ids) != 2:
            continue
        e1 = events_by_id.get(ids[0])
        e2 = events_by_id.get(ids[1])
        if e1 is None or e2 is None:
            continue
        if state.get_cached_deduplication_review(
            event_a=e1["event_id"],
            event_b=e2["event_id"],
            event_a_updated_at=e1["updated_at"],
            event_b_updated_at=e2["updated_at"],
            prompt_version=prompt_version,
        ):
            cached_count += 1
            continue
        candidates.append((e1, e2))

    deferred_count = 0
    if max_pairs is not None and len(candidates) > max_pairs:
        deferred_count = len(candidates) - max_pairs
        candidates = candidates[:max_pairs]
    if progress and (cached_count or deferred_count):
        progress(
            f"deduplicate: review work new={len(candidates)}, cached={cached_count}, "
            f"deferred={deferred_count}"
        )
    if not candidates:
        return 0

    merges_count = _evaluate_and_apply_deduplication_candidates(
        candidates=candidates,
        state=state,
        client=adjudicator,
        feeds_by_source=feeds_by_source,
        progress=progress,
        run_id=run_id,
        concurrency=concurrency,
        usage_stage="deduplication_review" if is_second_pass_review else "deduplication",
        prompt_version=prompt_version,
    )

    if progress and merges_count > 0:
        progress(f"deduplicate: completed merging {merges_count} event(s)")
    return merges_count


def _ordered_deduplication_candidate_pairs(
    candidate_pairs: Sequence[frozenset[str]],
    *,
    candidate_priorities: dict[frozenset[str], int],
    events_by_id: dict[str, dict[str, Any]],
) -> list[frozenset[str]]:
    return sorted(
        candidate_pairs,
        key=lambda pair: (
            candidate_priorities.get(pair, 0),
            max(events_by_id[event_id]["updated_at"] for event_id in pair),
            tuple(sorted(pair)),
        ),
        reverse=True,
    )
