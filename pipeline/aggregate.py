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
from pipeline.digest import digest_articles_for_aggregation
from pipeline.llm import GeminiClient, GeminiResult
from pipeline.lock import PipelineLock
from pipeline.paths import EVENT_DIR, LOCK_PATH, PROJECT_ROOT
from pipeline.state import StateDB
from pipeline.util import atomic_write_json, isoformat_z, sanitize_id

AGGREGATION_PROMPT_VERSION = "aggregation-v6"
AGGREGATION_EXPERIMENT_PROMPT_VERSION = "aggregation-experiment-v6"
NEWSWORTHINESS_PROMPT_VERSION = "newsworthiness-v1"
GROUPING_MODES = ("titles", "titles_summaries")


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
        where = "event_id IS NULL"
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
            ORDER BY published_at DESC, fetched_at DESC
            """,
            (window_start, window_end),
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
                event_id=row["event_id"],
                **_load_digest_fields(row["article_path"]),
            )
            for row in rows
        ]
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
    step_hours: int = 1,
    db: StateDB | None = None,
    rerun_latest_completed: bool = True,
) -> list[AggregationWindow]:
    if window_hours <= 0:
        raise ValueError("window_hours must be positive")
    if step_hours <= 0:
        raise ValueError("step_hours must be positive")
    start_dt = _parse_iso_timestamp(range_start)
    end_dt = _parse_iso_timestamp(range_end)
    if end_dt <= start_dt:
        raise ValueError("range_end must be after range_start")

    close_db = db is None
    state = db or StateDB()
    try:
        latest_completed = state.latest_completed_aggregation_window() if rerun_latest_completed else None

        def should_skip_window(window: AggregationWindow) -> bool:
            status = state.aggregation_window_status(window.window_start, window.window_end)
            if status != "completed":
                return False
            if latest_completed is not None and (window.window_start, window.window_end) == latest_completed:
                return False
            return state.unassigned_article_count_in_window(window.window_start, window.window_end) == 0

        windows: list[AggregationWindow] = []
        current = start_dt
        window_delta = timedelta(hours=window_hours)
        step_delta = timedelta(hours=step_hours)
        last_full_end_dt: datetime | None = None
        while current + window_delta <= end_dt:
            window_end_dt = current + window_delta
            window = AggregationWindow(
                window_start=_format_iso_timestamp(current),
                window_end=_format_iso_timestamp(window_end_dt),
            )
            last_full_end_dt = window_end_dt
            current += step_delta
            if not should_skip_window(window):
                windows.append(window)

        if last_full_end_dt is None or last_full_end_dt < end_dt:
            tail_start_dt = max(start_dt, end_dt - window_delta)
            tail_window = AggregationWindow(
                window_start=_format_iso_timestamp(tail_start_dt),
                window_end=_format_iso_timestamp(end_dt),
            )
            already_present = any(
                w.window_start == tail_window.window_start and w.window_end == tail_window.window_end
                for w in windows
            )
            if not already_present:
                if not should_skip_window(tail_window):
                    windows.append(tail_window)

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
) -> dict[str, Any]:
    if (range_start is None) != (range_end is None):
        raise ValueError("range_start and range_end must be provided together or both omitted")
    config = load_pipeline_config()
    window_hours = int(config.aggregation.get("window_hours", 6))
    step_hours = int(config.aggregation.get("window_step_hours", 1))
    digest_enabled = bool(config.digest.get("enabled_before_aggregation", True))
    digest_concurrency = int(config.digest.get("concurrency", 8))
    digest_content_char_limit = int(config.digest.get("content_char_limit", 12000))
    lock_timeout = timedelta(minutes=int(config.pipeline.get("watchdog_timeout_minutes", 30)))
    run_id = f"aggregation-{uuid.uuid4().hex}"
    state = StateDB()
    generator = client or GeminiClient()
    stats: dict[str, Any] = {
        "windows_planned": 0,
        "windows_processed": 0,
        "windows_failed": 0,
        "articles_seen": 0,
        "groups_seen": 0,
        "events_created": 0,
        "events_updated": 0,
        "article_assignments": 0,
        "newsworthiness_scored": 0,
        "newsworthiness_fallbacks": 0,
        "article_digests": {},
        "dry_run": dry_run,
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
                    range_start = _format_iso_timestamp(_floor_hour(_parse_iso_timestamp(bounds[0])))
                    range_end = _format_iso_timestamp(
                        _ceil_hour(_parse_iso_timestamp(bounds[1])) + timedelta(hours=window_hours)
                    )

                windows = plan_sliding_windows(
                    range_start=range_start,
                    range_end=range_end,
                    window_hours=window_hours,
                    step_hours=step_hours,
                    db=state,
                )
                if limit_windows is not None:
                    windows = windows[:limit_windows]
                stats["windows_planned"] = len(windows)
                if progress:
                    progress(f"aggregate: planned {len(windows)} windows")

                if windows and digest_enabled and not dry_run:
                    digest_range_start = min(window.window_start for window in windows)
                    digest_range_end = max(window.window_end for window in windows)
                    if progress:
                        progress(
                            "aggregate: preparing article digests "
                            f"from {digest_range_start} to {digest_range_end}"
                        )
                    stats["article_digests"] = digest_articles_for_aggregation(
                        state=state,
                        run_id=run_id,
                        published_after=digest_range_start,
                        published_before=digest_range_end,
                        concurrency=digest_concurrency,
                        content_char_limit=digest_content_char_limit,
                        client=generator,
                        progress=progress,
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

                        result = group_articles_with_gemini(articles, mode="titles_summaries", client=generator)
                        if not dry_run and result.get("usage"):
                            state.record_llm_usage(
                                run_id=run_id,
                                stage="aggregation",
                                model=generator.model,
                                prompt_version=AGGREGATION_PROMPT_VERSION,
                                input_tokens=result["usage"].get("promptTokenCount"),
                                output_tokens=result["usage"].get("candidatesTokenCount"),
                            )

                        stats["articles_seen"] += len(articles)
                        stats["groups_seen"] += result["group_count"]
                        window_stats = {
                            "article_count": len(articles),
                            "group_count": result["group_count"],
                            "singleton_count": result["singleton_count"],
                            "multi_article_group_count": result["multi_article_group_count"],
                            "validation_attempts": result["validation_attempts"],
                            "elapsed_ms": result["elapsed_ms"],
                            "usage": result["usage"],
                        }

                        groups_to_score = []
                        scores_by_group_index = {}
                        for group in result["groups"]:
                            group_articles = [articles[idx] for idx in group["article_indexes"]]
                            event_id = _event_id_for_group(group_articles)
                            reused_score = None
                            if event_id:
                                event_path = EVENT_DIR / f"{event_id}.json"
                                existing = _read_event(event_path)
                                if existing and existing.get("newsworthiness"):
                                    existing_ids = set(existing.get("article_ids", []))
                                    current_ids = set(art.article_id for art in group_articles)
                                    if existing_ids == current_ids:
                                        reused_score = existing["newsworthiness"]
                            if reused_score:
                                scores_by_group_index[int(group["group_index"])] = reused_score
                            else:
                                groups_to_score.append(group)

                        if groups_to_score:
                            scores_result = score_groups_newsworthiness(
                                articles=articles,
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
                            api_scores = scores_result["scores_by_group_index"]
                            fallback_count = scores_result["fallback_count"]
                            elapsed_ms = scores_result.get("elapsed_ms")
                            usage = scores_result.get("usage", {})
                        else:
                            api_scores = {}
                            fallback_count = 0
                            elapsed_ms = 0
                            usage = {}

                        merged_scores = {**scores_by_group_index, **api_scores}
                        stats["newsworthiness_scored"] += len(merged_scores)
                        stats["newsworthiness_fallbacks"] += fallback_count
                        window_stats["newsworthiness"] = {
                            "scored": len(merged_scores),
                            "fallback_count": fallback_count,
                            "elapsed_ms": elapsed_ms,
                            "usage": usage,
                        }

                        if not dry_run:
                            applied = apply_grouping_result(
                                articles=articles,
                                groups=result["groups"],
                                state=state,
                                scores_by_group_index=merged_scores,
                                article_classifications=result.get("article_classifications"),
                                feeds_by_source=feeds_by_source,
                                run_id=run_id,
                                progress=progress,
                            )
                            for key in ("events_created", "events_updated", "article_assignments"):
                                stats[key] += applied[key]
                            window_stats.update(applied)
                            state.finish_aggregation_window(
                                window_start=window.window_start,
                                window_end=window.window_end,
                                status="completed",
                                article_count=len(articles),
                                stats=window_stats,
                            )
                        stats["windows_processed"] += 1
                        if progress:
                            progress(
                                "aggregate: "
                                f"{window.window_start} grouped {len(articles)} articles "
                                f"into {result['group_count']} groups"
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
                if stats["windows_failed"]:
                    status = "failed" if stats["windows_processed"] == 0 else "partial_failure"
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
                        progress(
                            f"aggregate: content_type backfill failed for {article.article_id}: {exc}"
                        )
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

        existing_event_ids = sorted(set(art.event_id for art in group_articles if art.event_id))

        if not existing_event_ids:
            if is_all_opinions:
                continue
            event_id = _generate_new_event_id(group_articles, state)
            winner_event_ids = []
            created = True
            existing = None
        else:
            winner_id = _event_id_for_group(group_articles)
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
        group
        for group in groups
        if baseline[int(group["group_index"])].get("model") != "deterministic-digest-impact"
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
                "You score story clusters for editorial ranking. "
                "Return only valid JSON matching the schema."
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
) -> dict[str, Any]:
    mode = _normalize_mode(mode)
    if mode not in GROUPING_MODES:
        raise ValueError(f"unsupported grouping mode: {mode}")
    valid_categories = load_categories()
    base_prompt = _build_grouping_prompt(articles, mode=mode, valid_categories=valid_categories)
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
) -> tuple[list[dict[str, list[int]]], dict[int, dict[str, Any]]]:
    raw_groups = payload.get("groups")
    if not isinstance(raw_groups, list):
        raise ValueError("grouping response must contain a groups list")

    raw_articles = payload.get("articles")
    if not isinstance(raw_articles, list):
        raise ValueError("grouping response must contain an articles list")
    if len(raw_articles) != article_count:
        raise ValueError(
            f"articles classification count ({len(raw_articles)}) "
            f"must match article count ({article_count})"
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
    groups: list[dict[str, list[int]]] = []
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
        groups.append({"article_indexes": sorted(cleaned)})

    missing = sorted(set(range(article_count)) - seen)
    if missing:
        raise ValueError(f"grouping response omitted article indexes in groups: {missing}")
    return sorted(groups, key=lambda group: group["article_indexes"][0]), classifications


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


def _build_grouping_prompt(
    articles: Sequence[ArticleForAggregation],
    *,
    mode: str,
    valid_categories: list[str],
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
        "Return compact JSON only.\n\n"
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
                        }
                    },
                    "required": ["article_indexes"],
                },
            }
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
        return {"digest_summary": None, "digest_key_facts": ()}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {"digest_summary": None, "digest_key_facts": ()}
    digest = data.get("llm_digest")
    if not isinstance(digest, dict):
        return {"digest_summary": None, "digest_key_facts": ()}
    summary = digest.get("summary")
    key_facts = digest.get("key_facts")
    if not isinstance(summary, str) or not isinstance(key_facts, list):
        return {"digest_summary": None, "digest_key_facts": ()}
    clean_facts = tuple(str(fact).strip() for fact in key_facts if str(fact).strip())
    impact = digest.get("impact")
    if not isinstance(impact, dict):
        impact = None
    return {"digest_summary": summary.strip() or None, "digest_key_facts": clean_facts, "digest_impact": impact}


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


def _floor_hour(value: datetime) -> datetime:
    return value.replace(minute=0, second=0, microsecond=0)


def _ceil_hour(value: datetime) -> datetime:
    floored = _floor_hour(value)
    if value == floored:
        return floored
    return floored + timedelta(hours=1)


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
        (existing or {}).get("category")
        or category_override
        or _category_for_articles(articles, feeds_by_source)
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
            "rationale_codes": sorted({
                sanitize_id(str(code)).lower()
                for code in rationale_codes
                if str(code).strip()
            }),
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
    "about",
    "after",
    "amid",
    "and",
    "are",
    "from",
    "have",
    "into",
    "more",
    "news",
    "over",
    "says",
    "that",
    "the",
    "their",
    "this",
    "with",
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
