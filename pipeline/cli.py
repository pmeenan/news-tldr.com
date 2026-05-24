from __future__ import annotations

import argparse
import asyncio
import json
import shutil
import sys
from pathlib import Path

from pipeline.collect import collect_once
from pipeline.config import load_feeds
from pipeline.paths import ARTICLE_DIR, DATA_DIR, DB_PATH, FETCH_LOG_DIR, LOCK_PATH
from pipeline.state import StateDB, migrate


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
    lock_path: Path = LOCK_PATH,
    data_dir: Path = DATA_DIR,
) -> list[str]:
    if not confirm:
        raise ValueError("clean-data is destructive; rerun with --yes to confirm")
    if lock_path.exists() and not ignore_lock:
        raise RuntimeError(f"pipeline lock exists at {lock_path}; refusing to clean while a run may be active")

    targets = [
        db_path,
        Path(f"{db_path}-wal"),
        Path(f"{db_path}-shm"),
        Path(f"{db_path}-journal"),
        article_dir,
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


def main() -> None:
    parser = argparse.ArgumentParser(prog="news-tldr-pipeline")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init-db", help="Initialize or migrate the SQLite state database.")
    collect_parser = sub.add_parser("collect", help="Run stage 1 data collection.")
    collect_parser.add_argument("--verbose", action="store_true", help="Print incremental progress to stderr.")
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
    elif args.command == "collect":
        progress = None
        if args.verbose:
            def progress(message: str) -> None:
                from datetime import UTC, datetime
                timestamp = datetime.now(UTC).strftime("%H:%M:%S")
                print(f"[{timestamp}] {message}", file=sys.stderr, flush=True)
        stats = asyncio.run(collect_once(progress=progress))
        print(json.dumps(stats, indent=2, sort_keys=True))
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
