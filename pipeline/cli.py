from __future__ import annotations

import argparse
import asyncio
import json

from pipeline.collect import collect_once
from pipeline.config import load_feeds
from pipeline.state import StateDB, migrate


def main() -> None:
    parser = argparse.ArgumentParser(prog="news-tldr-pipeline")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init-db", help="Initialize or migrate the SQLite state database.")
    sub.add_parser("collect", help="Run stage 1 data collection.")
    sub.add_parser("list-feeds", help="List enabled feed source IDs.")
    args = parser.parse_args()

    if args.command == "init-db":
        migrate()
        feeds = load_feeds(enabled_only=False)
        with StateDB() as state:
            state.sync_feeds(feeds)
        print("initialized data/state/pipeline.db")
    elif args.command == "collect":
        stats = asyncio.run(collect_once())
        print(json.dumps(stats, indent=2, sort_keys=True))
    elif args.command == "list-feeds":
        for feed in load_feeds(enabled_only=True):
            print(f"{feed.source_id}\t{feed.feed_url}")


if __name__ == "__main__":
    main()
