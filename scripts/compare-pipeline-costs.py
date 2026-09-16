#!/usr/bin/env python3
"""Read-only, equal-window cost comparison for the incremental-grouping decision."""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path


def timestamp(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise argparse.ArgumentTypeError("timestamps must include a timezone")
    return result.astimezone(UTC)


def window(conn: sqlite3.Connection, start: datetime, end: datetime) -> dict:
    bounds = (start.isoformat().replace("+00:00", "Z"), end.isoformat().replace("+00:00", "Z"))
    rows = conn.execute(
        "SELECT stage, COUNT(*), SUM(cost_usd), SUM(cost_usd IS NULL) FROM llm_usage "
        "WHERE occurred_at >= ? AND occurred_at < ? AND stage != 'routing_evaluation' GROUP BY stage", bounds,
    ).fetchall()
    stages = {stage: {"calls": calls, "cost_usd": round(cost or 0, 6)} for stage, calls, cost, _ in rows}
    work = {}
    for stage, raw in conn.execute(
        "SELECT stage, stats_json FROM pipeline_runs WHERE started_at >= ? AND started_at < ?", bounds,
    ):
        stats = json.loads(raw)
        totals = work.setdefault(stage, {"runs": 0})
        totals["runs"] += 1
        for key in ("articles_written", "completed", "failed", "article_assignments",
                    "events_created", "events_merged"):
            if isinstance(stats.get(key), int):
                totals[key] = totals.get(key, 0) + stats[key]
    return {"start": bounds[0], "end": bounds[1], "cost_usd": round(sum(r[2] or 0 for r in rows), 6),
            "unpriced_calls": sum(r[3] for r in rows), "stages": stages, "work": work}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=Path(__file__).resolve().parents[1] / "data/state/pipeline.db")
    parser.add_argument("--baseline-start", type=timestamp, required=True)
    parser.add_argument("--candidate-start", type=timestamp, required=True)
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    if args.hours < 1:
        parser.error("hours must be positive")
    if args.verbose:
        print("Comparing recorded costs and processing volume; no API calls or database writes.", file=sys.stderr)
    duration = timedelta(hours=args.hours)
    with sqlite3.connect(args.db.resolve().as_uri() + "?mode=ro", uri=True) as conn:
        baseline = window(conn, args.baseline_start, args.baseline_start + duration)
        candidate = window(conn, args.candidate_start, args.candidate_start + duration)
    complete = datetime.now(UTC) >= max(args.baseline_start, args.candidate_start) + duration
    comparable = complete and bool(baseline["stages"] and candidate["stages"]) and not (
        baseline["unpriced_calls"] or candidate["unpriced_calls"]
    )
    beats = candidate["cost_usd"] < baseline["cost_usd"] if comparable else None
    print(json.dumps({"baseline": baseline, "candidate": candidate, "complete_windows": complete,
                      "beats_previous_architecture": beats,
                      "recommendation": "wait_for_complete_priced_windows" if beats is None else (
                          "review_throughput_before_accepting_savings" if beats else "revert_incremental_grouping"),
                      "rollback_setting": {"aggregation.incremental_grouping": False},
                      "note": "Compare total spend, not just grouping. Run counts include partial/failed runs; "
                              "completed editorial work includes updates, not only new stories. "
                              "No automatic rollback."},
                     indent=2))


if __name__ == "__main__":
    main()
