from __future__ import annotations

import argparse
import asyncio
import json
import shutil
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pipeline.aggregate import (
    aggregate_once,
    default_experiment_output_path,
    run_grouping_experiment,
    write_experiment_result,
)
from pipeline.collect import collect_once
from pipeline.config import load_feeds, load_pipeline_config
from pipeline.digest import ARTICLE_DIGEST_PROMPT_VERSION, digest_once
from pipeline.editorial import editorial_once, pending_editorial_sql
from pipeline.lock import PipelineLock
from pipeline.maintenance import maintenance_once
from pipeline.operations import (
    health_report,
    llm_usage_report,
    preflight_report,
    validate_artifacts,
    write_health_report,
)
from pipeline.paths import ARTICLE_DIR, DATA_DIR, DB_PATH, FETCH_LOG_DIR, LOCK_PATH
from pipeline.present import presentation_once
from pipeline.state import StateDB, migrate
from pipeline.util import isoformat_z


def _assert_data_path(path: Path, data_dir: Path) -> None:
    resolved_path = path.resolve()
    resolved_data_dir = data_dir.resolve()
    try:
        resolved_path.relative_to(resolved_data_dir)
    except ValueError as exc:
        raise ValueError(f"refusing to remove path outside data directory: {path}") from exc


def _remove_path(path: Path, data_dir: Path) -> str | None:
    _assert_data_path(path, data_dir)
    if not path.exists() and not path.is_symlink():
        return None
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    else:
        path.unlink()
    return str(path)


def clean_data(
    *,
    confirm: bool,
    include_fetch_logs: bool = True,
    ignore_lock: bool = False,
    db_path: Path = DB_PATH,
    article_dir: Path = ARTICLE_DIR,
    fetch_log_dir: Path = FETCH_LOG_DIR,
    event_dir: Path | None = None,
    published_dir: Path | None = None,
    lock_path: Path = LOCK_PATH,
    data_dir: Path = DATA_DIR,
) -> list[str]:
    if not confirm:
        raise ValueError("clean-data is destructive; rerun with --yes to confirm")
    if lock_path.exists() and not ignore_lock:
        raise RuntimeError(f"pipeline lock exists at {lock_path}; refusing to clean while a run may be active")

    event_dir = event_dir or (data_dir / "events")
    published_dir = published_dir or (data_dir / "published")

    targets = [
        db_path,
        Path(f"{db_path}-wal"),
        Path(f"{db_path}-shm"),
        Path(f"{db_path}-journal"),
        article_dir,
        event_dir,
        published_dir,
    ]
    if include_fetch_logs:
        targets.append(fetch_log_dir)
    if ignore_lock:
        targets.append(lock_path)

    removed = []
    for target in targets:
        removed_path = _remove_path(target, data_dir)
        if removed_path:
            removed.append(removed_path)
    return removed


def _stderr_progress(message: str) -> None:
    timestamp = datetime.now(UTC).strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}", file=sys.stderr, flush=True)


def _pending_editorial_count(*, excluded_event_ids: list[str] | None = None) -> int:
    excluded = sorted(set(excluded_event_ids or []))
    exclusion = f" AND event_id NOT IN ({','.join('?' for _ in excluded)})" if excluded else ""
    hold_minutes = int(load_pipeline_config().editorial.get("single_source_hold_minutes", 0) or 0)
    clause, params = pending_editorial_sql(hold_minutes=hold_minutes)
    with StateDB() as state:
        row = state.conn.execute(
            f"SELECT COUNT(*) FROM events WHERE {clause}{exclusion}",
            [*params, *excluded],
        ).fetchone()
        return int(row[0]) if row else 0


def _blocking_editorial_count(stats: dict) -> int:
    # Validation failures remain pending and unhealthy, but do not starve unrelated news.
    rejected = stats.get("rejected_event_ids")
    return _pending_editorial_count(excluded_event_ids=rejected) if rejected else _pending_editorial_count()


def pipeline_lock_status() -> dict:
    """Report whether a verified live process currently holds the pipeline lock."""
    from pipeline.lock import lock_holder

    holder = lock_holder(LOCK_PATH)
    return {"held": holder is not None, "holder": holder}


def _recover_stale_pipeline_runs() -> int:
    with StateDB() as state:
        return state.fail_stale_running_pipeline_runs()


def _pending_upstream_counts(*, max_article_rowid: int | None = None) -> dict[str, int]:
    config = load_pipeline_config()
    today_start = datetime.combine(datetime.now(UTC).date(), datetime.min.time(), tzinfo=UTC)
    lookback_days = max(1, int(config.retention.get("staging_article_days", 1)))
    range_start = isoformat_z(today_start - timedelta(days=lookback_days))
    rowid_clause = " AND rowid <= ?" if max_article_rowid is not None else ""
    with StateDB() as state:
        digest_params: list[object] = [range_start, ARTICLE_DIGEST_PROMPT_VERSION]
        aggregation_params: list[object] = [range_start]
        if max_article_rowid is not None:
            digest_params.append(max_article_rowid)
            aggregation_params.append(max_article_rowid)
        digest = state.conn.execute(
            f"""
            SELECT COUNT(*) FROM articles
            WHERE published_at >= ?
              AND is_filtered = 0
              AND (
                digest_status NOT IN ('completed', 'skipped')
                OR digest_prompt_version IS NULL
                OR digest_prompt_version != ?
              )
              {rowid_clause}
            """,
            digest_params,
        ).fetchone()[0]
        aggregation = state.conn.execute(
            f"""
            SELECT COUNT(*) FROM articles
            WHERE published_at >= ?
              AND is_filtered = 0
              AND event_id IS NULL
              AND digest_status = 'completed'
              {rowid_clause}
            """,
            aggregation_params,
        ).fetchone()[0]
    return {"digest": int(digest), "aggregation": int(aggregation)}


def _max_article_rowid() -> int:
    with StateDB() as state:
        row = state.conn.execute("SELECT COALESCE(MAX(rowid), 0) FROM articles").fetchone()
        return int(row[0]) if row else 0


def _run_collection(progress) -> dict[str, int]:
    return asyncio.run(collect_once(progress=progress, acquire_lock=False))


def run_completed_pipeline(
    *,
    force: bool = False,
    publish: bool | None = None,
    dry_run: bool = False,
    progress=None,
) -> dict[str, object]:
    config = load_pipeline_config()
    lock_timeout = timedelta(minutes=int(config.pipeline.get("watchdog_timeout_minutes", 30)))
    run_id = f"pipeline-run-{uuid.uuid4().hex}"

    with PipelineLock(LOCK_PATH, lock_timeout, run_id=run_id):
        if progress:
            progress("run: acquired pipeline lock")
        if dry_run:
            if progress:
                progress("run: starting non-mutating preflight; network and LLM calls are disabled")
            return {
                "force": force,
                "dry_run": True,
                "preflight": preflight_report(progress=progress),
            }
        if progress:
            progress("run: starting maintenance")
        migrate()
        recovered_runs = _recover_stale_pipeline_runs()
        if recovered_runs and progress:
            progress(f"run: marked {recovered_runs} interrupted stage run(s) as failed")
        maintenance_stats = maintenance_once(progress=progress, acquire_lock=False)

        backlog_article_rowid = _max_article_rowid()
        initial_backlog = _pending_editorial_count()
        upstream_backlog = _pending_upstream_counts(
            max_article_rowid=backlog_article_rowid
        )
        prior_work_stages: dict[str, object] = {}
        with ThreadPoolExecutor(max_workers=1, thread_name_prefix="collection") as executor:
            if progress:
                progress("run: starting collection alongside backlog processing")
            collection_future = executor.submit(_run_collection, progress)

            if initial_backlog:
                if progress:
                    progress(
                        f"run: draining {initial_backlog} pending editorial event(s) "
                        "while collection runs"
                    )
                backlog_editorial_stats = editorial_once(
                    force=False,
                    progress=progress,
                    acquire_lock=False,
                    backfill_limit=0,
                    curate=False,
                )
                remaining_backlog = _blocking_editorial_count(backlog_editorial_stats)
                if progress:
                    progress("run: publishing editorial backlog progress")
                backlog_presentation_stats = presentation_once(
                    publish=publish,
                    progress=progress,
                    acquire_lock=False,
                )
                if remaining_backlog:
                    collect_stats = collection_future.result()
                    if progress:
                        progress(
                            f"run: {remaining_backlog} editorial event(s) remain; "
                            "collection completed but downstream work is deferred"
                        )
                    return {
                        "force": force,
                        "backlog_blocked": True,
                        "pending_editorial": remaining_backlog,
                        "stages": {
                            "maintenance": maintenance_stats,
                            "collect": collect_stats,
                            "backlog_editorial": backlog_editorial_stats,
                            "backlog_presentation": backlog_presentation_stats,
                        },
                    }

            if upstream_backlog["digest"] or upstream_backlog["aggregation"]:
                if progress:
                    progress(
                        "run: draining snapshotted upstream work while collection runs "
                        f"(digest={upstream_backlog['digest']}, "
                        f"aggregation={upstream_backlog['aggregation']}, "
                        f"max_rowid={backlog_article_rowid})"
                    )
                prior_work_stages["prior_digest"] = digest_once(
                    force=False,
                    progress=progress,
                    acquire_lock=False,
                    max_article_rowid=backlog_article_rowid,
                )
                prior_work_stages["prior_aggregate"] = aggregate_once(
                    force=False,
                    progress=progress,
                    acquire_lock=False,
                    max_article_rowid=backlog_article_rowid,
                    post_review=False,
                )
                prior_work_stages["prior_editorial"] = editorial_once(
                    force=False,
                    progress=progress,
                    acquire_lock=False,
                    backfill_limit=0,
                    curate=False,
                )
                prior_work_stages["prior_presentation"] = presentation_once(
                    publish=publish,
                    progress=progress,
                    acquire_lock=False,
                )
                remaining_upstream = _pending_upstream_counts(
                    max_article_rowid=backlog_article_rowid
                )
                remaining_editorial = _blocking_editorial_count(prior_work_stages["prior_editorial"])
                if any(remaining_upstream.values()) or remaining_editorial:
                    collect_stats = collection_future.result()
                    if progress:
                        progress(
                            "run: snapshotted prior work remains; collection completed but "
                            "downstream work is deferred "
                            f"(digest={remaining_upstream['digest']}, "
                            f"aggregation={remaining_upstream['aggregation']}, "
                            f"editorial={remaining_editorial})"
                        )
                    return {
                        "force": force,
                        "backlog_blocked": True,
                        "pending_digest": remaining_upstream["digest"],
                        "pending_aggregation": remaining_upstream["aggregation"],
                        "pending_editorial": remaining_editorial,
                        "stages": {
                            "maintenance": maintenance_stats,
                            "collect": collect_stats,
                            **prior_work_stages,
                        },
                    }

            collect_stats = collection_future.result()
            if progress:
                progress("run: collection complete; starting downstream work")

        if progress:
            progress("run: starting digest")
        migrate()
        digest_stats = digest_once(force=force, progress=progress, acquire_lock=False)

        if progress:
            progress("run: starting aggregate")
        migrate()
        aggregate_stats = aggregate_once(force=force, progress=progress, acquire_lock=False)

        if progress:
            progress("run: starting editorial")
        migrate()
        editorial_stats = editorial_once(force=force, progress=progress, acquire_lock=False)

        if progress:
            progress("run: starting presentation and publish")
        migrate()
        presentation_stats = presentation_once(
            publish=publish,
            progress=progress,
            acquire_lock=False,
        )

    stages = {
        "maintenance": maintenance_stats,
        "collect": collect_stats,
        "digest": digest_stats,
        "aggregate": aggregate_stats,
        "editorial": editorial_stats,
        "presentation": presentation_stats,
    }
    if initial_backlog:
        stages["backlog_editorial"] = backlog_editorial_stats
        stages["backlog_presentation"] = backlog_presentation_stats
    stages.update(prior_work_stages)
    return {"force": force, "stages": stages}


def main() -> None:
    parser = argparse.ArgumentParser(prog="news-tldr-pipeline")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init-db", help="Initialize or migrate the SQLite state database.")
    sub.add_parser(
        "lock-status",
        help="Exit 0 when the pipeline lock is free or stale, 3 when a verified live run holds it.",
    )
    run_parser = sub.add_parser(
        "run",
        help="Run completed pipeline stages through presentation and publishing.",
    )
    run_parser.add_argument(
        "--force",
        action="store_true",
        help="Pass force mode through to stages that support it.",
    )
    run_parser.add_argument("--verbose", action="store_true", help="Print incremental progress to stderr.")
    run_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and plan the run without network, LLM, database, artifact, or publish mutations.",
    )
    run_parser.add_argument(
        "--no-publish",
        action="store_true",
        help="Build the static site but do not copy it to the configured production directory.",
    )
    collect_parser = sub.add_parser("collect", help="Run stage 1 data collection.")
    collect_parser.add_argument("--verbose", action="store_true", help="Print incremental progress to stderr.")
    aggregate_parser = sub.add_parser("aggregate", help="Run stage 2 story aggregation.")
    aggregate_parser.add_argument("--range-start", help="UTC start timestamp for aggregation window planning.")
    aggregate_parser.add_argument("--range-end", help="UTC end timestamp for aggregation window planning.")
    aggregate_parser.add_argument("--limit-windows", type=int, help="Maximum number of planned windows to process.")
    aggregate_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Plan windows and category batches without LLM calls or mutations.",
    )
    aggregate_parser.add_argument(
        "--force",
        action="store_true",
        help="Force aggregation of all windows in range, ignoring database completed state.",
    )
    aggregate_parser.add_argument("--verbose", action="store_true", help="Print incremental progress to stderr.")
    digest_stage_parser = sub.add_parser("digest", help="Run the article digest stage before aggregation.")
    digest_stage_parser.add_argument("--range-start", help="UTC start timestamp for articles to digest.")
    digest_stage_parser.add_argument("--range-end", help="UTC end timestamp for articles to digest.")
    digest_stage_parser.add_argument("--limit", type=int, help="Maximum number of articles to digest.")
    digest_stage_parser.add_argument("--concurrency", type=int, help="Number of parallel article digest calls.")
    digest_stage_parser.add_argument(
        "--force",
        action="store_true",
        help="Regenerate digests even when an article already has the current digest prompt version.",
    )
    digest_stage_parser.add_argument("--verbose", action="store_true", help="Print incremental progress to stderr.")
    editorial_parser = sub.add_parser("editorial", help="Run stage 3 editorial story generation.")
    editorial_parser.add_argument("--limit", type=int, help="Maximum number of events to publish.")
    editorial_parser.add_argument("--concurrency", type=int, help="Number of parallel per-event LLM calls.")
    editorial_parser.add_argument(
        "--event-id",
        action="append",
        dest="event_ids",
        help="Publish only this event ID; may be repeated.",
    )
    editorial_parser.add_argument(
        "--force",
        action="store_true",
        help="Regenerate stories even when events have not changed.",
    )
    editorial_parser.add_argument(
        "--backfill",
        type=int,
        help=(
            "Also regenerate up to N current-window stories that predate evidence "
            "verification, highest rank first (default: editorial.backfill_per_run; 0 disables)."
        ),
    )
    editorial_parser.add_argument("--verbose", action="store_true", help="Print incremental progress to stderr.")
    presentation_parser = sub.add_parser(
        "present",
        help="Build the static site and publish it to the configured production directory.",
    )
    presentation_parser.add_argument(
        "--build-only",
        action="store_true",
        help="Build dist/ without publishing it.",
    )
    presentation_parser.add_argument(
        "--publish-dir",
        type=Path,
        help="Override the configured absolute production directory.",
    )
    presentation_parser.add_argument("--verbose", action="store_true", help="Print incremental progress to stderr.")
    validate_parser = sub.add_parser(
        "validate-data",
        help="Validate config, SQLite, pipeline JSON artifacts, and generated static output.",
    )
    validate_parser.add_argument("--verbose", action="store_true", help="Print incremental progress to stderr.")
    health_parser = sub.add_parser(
        "health",
        help="Check recent pipeline stages, collection failures, artifacts, and the live site.",
    )
    health_parser.add_argument(
        "--max-age-hours",
        type=int,
        help="Override the maximum age of the latest successful stage run.",
    )
    health_parser.add_argument(
        "--no-network",
        action="store_true",
        help="Skip HTTPS checks against the configured public site.",
    )
    health_parser.add_argument(
        "--no-validate",
        action="store_true",
        help="Skip the full artifact validation pass.",
    )
    health_parser.add_argument("--verbose", action="store_true", help="Print incremental progress to stderr.")
    usage_parser = sub.add_parser("llm-usage", help="Summarize recorded LLM tokens and cost by stage/model.")
    usage_parser.add_argument("--hours", type=int, default=24, help="Reporting lookback in hours.")
    maintenance_parser = sub.add_parser(
        "maintenance",
        help="Run retention cleanup, event lifecycle updates, and artifact reconciliation.",
    )
    maintenance_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report maintenance work without mutating SQLite state or JSON artifacts.",
    )
    maintenance_parser.add_argument("--verbose", action="store_true", help="Print incremental progress to stderr.")
    experiment_parser = sub.add_parser(
        "aggregation-experiment",
        help="Compare Gemini event grouping with titles only versus titles plus summaries.",
    )
    experiment_parser.add_argument("--limit", type=int, help="Number of unassigned articles to test.")
    experiment_parser.add_argument(
        "--published-date",
        help="Restrict the experiment to articles published on this UTC date (YYYY-MM-DD).",
    )
    experiment_parser.add_argument(
        "--published-after",
        help="Restrict the experiment to articles published at or after this UTC timestamp.",
    )
    experiment_parser.add_argument(
        "--published-before",
        help="Restrict the experiment to articles published before this UTC timestamp.",
    )
    experiment_parser.add_argument(
        "--mode",
        choices=["both", "titles", "titles-summaries"],
        default="both",
        help="Which prompt mode to run.",
    )
    experiment_parser.add_argument("--output", type=Path, help="Optional path for the experiment JSON artifact.")
    experiment_parser.add_argument("--verbose", action="store_true", help="Print incremental progress to stderr.")
    sub.add_parser("list-feeds", help="List enabled feed source IDs.")
    clean_parser = sub.add_parser("clean-data", help="Remove local pipeline DB and staged article data.")
    clean_parser.add_argument("--yes", action="store_true", help="Confirm deletion of local generated data.")
    clean_parser.add_argument("--keep-fetch-log", action="store_true", help="Keep data/staging/fetch-log/.")
    clean_parser.add_argument("--ignore-lock", action="store_true", help="Remove data even if pipeline.lock exists.")
    args = parser.parse_args()

    if args.command == "init-db":
        migrate()
        feeds = load_feeds(enabled_only=False)
        with StateDB() as state:
            state.sync_feeds(feeds)
        print("initialized data/state/pipeline.db")
    elif args.command == "run":
        if args.force and args.dry_run:
            parser.error("--force and --dry-run cannot be combined")
        progress = _stderr_progress if args.verbose else None
        stats = run_completed_pipeline(
            force=args.force,
            publish=False if args.no_publish else None,
            dry_run=args.dry_run,
            progress=progress,
        )
        print(json.dumps(stats, indent=2, sort_keys=True))
        if args.dry_run and not stats["preflight"]["validation"]["valid"]:
            raise SystemExit(1)
        if stats.get("backlog_blocked"):
            raise SystemExit(1)
    elif args.command == "collect":
        progress = _stderr_progress if args.verbose else None
        stats = asyncio.run(collect_once(progress=progress))
        print(json.dumps(stats, indent=2, sort_keys=True))
    elif args.command == "aggregate":
        progress = _stderr_progress if args.verbose else None
        if (args.range_start is None) != (args.range_end is None):
            parser.error("--range-start and --range-end must be provided together")
        if args.limit_windows is not None and args.limit_windows < 1:
            parser.error("--limit-windows must be at least 1")
        migrate()
        stats = aggregate_once(
            range_start=args.range_start,
            range_end=args.range_end,
            limit_windows=args.limit_windows,
            dry_run=args.dry_run,
            progress=progress,
            force=args.force,
        )
        print(json.dumps(stats, indent=2, sort_keys=True))
    elif args.command == "digest":
        progress = _stderr_progress if args.verbose else None
        if (args.range_start is None) != (args.range_end is None):
            parser.error("--range-start and --range-end must be provided together")
        if args.limit is not None and args.limit < 1:
            parser.error("--limit must be at least 1")
        if args.concurrency is not None and args.concurrency < 1:
            parser.error("--concurrency must be at least 1")
        migrate()
        stats = digest_once(
            range_start=args.range_start,
            range_end=args.range_end,
            limit=args.limit,
            concurrency=args.concurrency,
            force=args.force,
            progress=progress,
        )
        print(json.dumps(stats, indent=2, sort_keys=True))
    elif args.command == "lock-status":
        status = pipeline_lock_status()
        print(json.dumps(status, indent=2, sort_keys=True))
        if status["held"]:
            sys.exit(3)
    elif args.command == "editorial":
        progress = _stderr_progress if args.verbose else None
        if args.limit is not None and args.limit < 1:
            parser.error("--limit must be at least 1")
        if args.concurrency is not None and args.concurrency < 1:
            parser.error("--concurrency must be at least 1")
        if args.backfill is not None and args.backfill < 0:
            parser.error("--backfill must not be negative")
        migrate()
        stats = editorial_once(
            limit=args.limit,
            concurrency=args.concurrency,
            force=args.force,
            event_ids=args.event_ids,
            progress=progress,
            backfill_limit=args.backfill,
        )
        print(json.dumps(stats, indent=2, sort_keys=True))
    elif args.command == "present":
        progress = _stderr_progress if args.verbose else None
        migrate()
        stats = presentation_once(
            publish=False if args.build_only else None,
            publish_dir=args.publish_dir,
            progress=progress,
        )
        print(json.dumps(stats, indent=2, sort_keys=True))
    elif args.command == "validate-data":
        progress = _stderr_progress if args.verbose else None
        stats = validate_artifacts(progress=progress)
        print(json.dumps(stats, indent=2, sort_keys=True))
        if not stats["valid"]:
            raise SystemExit(1)
    elif args.command == "health":
        if args.max_age_hours is not None and args.max_age_hours < 1:
            parser.error("--max-age-hours must be at least 1")
        progress = _stderr_progress if args.verbose else None
        stats = health_report(
            check_live_site=not args.no_network,
            max_age_hours=args.max_age_hours,
            validate=not args.no_validate,
            progress=progress,
        )
        write_health_report(stats)
        print(json.dumps(stats, indent=2, sort_keys=True))
        if stats["status"] != "healthy":
            raise SystemExit(1)
    elif args.command == "llm-usage":
        if args.hours < 1:
            parser.error("--hours must be at least 1")
        print(json.dumps(llm_usage_report(hours=args.hours), indent=2, sort_keys=True))
    elif args.command == "maintenance":
        progress = _stderr_progress if args.verbose else None
        migrate()
        stats = maintenance_once(dry_run=args.dry_run, progress=progress)
        print(json.dumps(stats, indent=2, sort_keys=True))
    elif args.command == "aggregation-experiment":
        progress = _stderr_progress if args.verbose else None
        if args.limit is not None and args.limit < 1:
            parser.error("--limit must be at least 1")
        if args.published_date and (args.published_after or args.published_before):
            parser.error("--published-date cannot be combined with --published-after or --published-before")
        modes = ("titles", "titles_summaries") if args.mode == "both" else (args.mode.replace("-", "_"),)
        has_time_filter = args.published_date or args.published_after or args.published_before
        limit = args.limit if args.limit is not None else (None if has_time_filter else 40)
        result = run_grouping_experiment(
            limit=limit,
            published_date=args.published_date,
            published_after=args.published_after,
            published_before=args.published_before,
            modes=modes,
            progress=progress,
        )
        output_path = args.output or default_experiment_output_path()
        write_experiment_result(result, output_path)
        print(json.dumps({"output": str(output_path), "result": result}, indent=2, sort_keys=True))
    elif args.command == "list-feeds":
        for feed in load_feeds(enabled_only=True):
            print(f"{feed.source_id}\t{feed.feed_url}")
    elif args.command == "clean-data":
        try:
            removed = clean_data(
                confirm=args.yes,
                include_fetch_logs=not args.keep_fetch_log,
                ignore_lock=args.ignore_lock,
            )
        except (RuntimeError, ValueError) as exc:
            parser.error(str(exc))
        print(json.dumps({"removed": removed}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
