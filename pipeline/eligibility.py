"""Persistent category gap admissions, selected before paid editorial work."""
from __future__ import annotations

import json
from collections import Counter, defaultdict
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from pipeline.config import load_pipeline_config
from pipeline.paths import STORY_DIR
from pipeline.sources import publisher_id
from pipeline.state import StateDB
from pipeline.util import isoformat_z


def enabled() -> bool:
    return bool(load_pipeline_config().editorial.get("gap_fill_enabled", False))


def _time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _events(state: StateDB) -> list[dict[str, Any]]:
    publishers: dict[str, set[str]] = defaultdict(set)
    for row in state.conn.execute(
        """SELECT a.event_id, a.source_id, a.source_name, a.url
           FROM articles a JOIN events e ON e.event_id = a.event_id
           WHERE a.is_filtered = 0 AND e.status IN ('active', 'stale')"""
    ):
        publishers[row["event_id"]].add(publisher_id(dict(row)))
    return [
        {**dict(row), "publishers": len(publishers[row["event_id"]])}
        for row in state.conn.execute("SELECT * FROM events WHERE status IN ('active', 'stale')")
        if publishers[row["event_id"]]
    ]


def single_admissions(state: StateDB) -> dict[str, dict[str, Any]]:
    if not state.conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'editorial_admissions'"
    ).fetchone():
        return {}
    return {row["event_id"]: dict(row) for row in state.conn.execute("SELECT * FROM editorial_admissions")}


def _select(
    events: list[dict[str, Any]], admissions: dict[str, dict[str, Any]], now: datetime,
) -> list[dict[str, Any]]:
    config = load_pipeline_config().editorial
    target = max(0, int(config.get("gap_fill_target", 12)))
    targets = config.get("gap_fill_category_targets", {})
    cutoff = now - timedelta(hours=24)
    hold = now - timedelta(minutes=max(0, int(config.get("single_source_hold_minutes", 0))))
    # Count new events or published material revisions, never processing updates.
    multis = set()
    for event in events:
        material = event.get("editorial_material_at")
        fresh_at = _time(material) if material and _time(material) <= now else _time(event["created_at"])
        if event["publishers"] >= 2 and cutoff < fresh_at <= now:
            multis.add(event["event_id"])
    counts = Counter(e["category"] for e in events if e["event_id"] in multis)
    by_id = {event["event_id"]: event for event in events}
    for eid, admission in admissions.items():
        event = by_id.get(eid, {})
        material = event.get("editorial_material_at")
        fresh_at = _time(admission["admitted_at"])
        if material and _time(material) <= now:
            fresh_at = max(fresh_at, _time(material))
        if eid not in multis and cutoff < fresh_at <= now:
            counts[event.get("category", admission["category"])] += 1
    candidates = sorted(
        (e for e in events if e["publishers"] == 1 and e["event_id"] not in admissions
         and cutoff < _time(e["created_at"]) <= hold),
        key=lambda e: (-(e["newsworthiness_category"] or 0), -(e["newsworthiness_global"] or 0),
                       e["created_at"], e["event_id"]),
    )
    selected = []
    for event in candidates:
        category = event["category"]
        if counts[category] >= max(0, int(targets.get(category, target))):
            continue
        selected.append({"event_id": event["event_id"], "category": category, "admitted_at": isoformat_z(now)})
        counts[category] += 1
    return selected


def eligible_event_ids(state: StateDB, *, now: datetime | None = None, preview: bool = True) -> set[str] | None:
    """Read-only selection shared by generation, preflight and run/health gates."""
    if not enabled():
        return None
    events = _events(state)
    admissions = single_admissions(state)
    ids = {e["event_id"] for e in events if e["publishers"] >= 2 or e["event_id"] in admissions}
    if preview:
        ids.update(a["event_id"] for a in _select(events, admissions, now or datetime.now(UTC)))
    return ids


def admit_gap_stories(
    state: StateDB, *, now: datetime | None = None, retrospective: bool = False,
    dry_run: bool = False, progress: Callable[[str], None] | None = None, story_dir: Path = STORY_DIR,
) -> dict[str, Any]:
    """Reserve single-publisher slots atomically, including failed attempts.

    Retrospective initialization replays hourly selection over existing published
    events using today's publisher memberships. It never changes prior admissions.
    Callers hold the pipeline lock; no LLM calls or artifact deletion occur here.
    """
    if not enabled():
        return {"gap_fill_enabled": False, "gap_fill_admitted": 0}
    reference = now or datetime.now(UTC)
    events = _events(state)
    admissions = single_admissions(state)
    selected = []
    material_dates = []
    initialized = False
    if state.conn.execute("SELECT 1 FROM sqlite_master WHERE name = 'editorial_policy_state'").fetchone():
        initialized = bool(state.conn.execute(
            "SELECT 1 FROM editorial_policy_state WHERE policy = 'category-gap-v1'"
        ).fetchone())
    if retrospective and not initialized:
        for event in events:
            path = story_dir / f"{event['event_id']}.json"
            if not path.is_file():
                continue
            story = json.loads(path.read_text())
            material_at = story.get("revision_at") or story.get("created_at")
            if material_at:
                event["editorial_material_at"] = material_at
                material_dates.append((material_at, event["event_id"]))
        historical = [e for e in events if e["last_editorial_at"] is not None]
        if historical:
            tick = min(_time(e["created_at"]) for e in historical).replace(minute=0, second=0, microsecond=0)
            while tick < reference:
                tick = min(tick + timedelta(hours=1), reference)
                batch = _select(historical, admissions, tick)
                selected.extend(batch)
                admissions.update((a["event_id"], a) for a in batch)
                if progress and tick.hour == 0:
                    progress(f"eligibility: replayed through {isoformat_z(tick)}, {len(selected)} singles admitted")
    batch = _select(events, admissions, reference)
    selected.extend(batch)
    if not dry_run:
        with state.conn:
            state.conn.executemany(
                "UPDATE events SET editorial_material_at = ? WHERE event_id = ?", material_dates,
            )
            state.conn.executemany(
                "INSERT OR IGNORE INTO editorial_admissions(event_id, category, admitted_at) VALUES (?, ?, ?)",
                [(a["event_id"], a["category"], a["admitted_at"]) for a in selected],
            )
            if retrospective and not initialized:
                state.conn.execute(
                    "INSERT INTO editorial_policy_state(policy, initialized_at) VALUES ('category-gap-v1', ?)",
                    (isoformat_z(reference),),
                )
    counts = Counter(a["category"] for a in selected)
    if progress:
        progress(f"eligibility: {'would admit' if dry_run else 'admitted'} {len(selected)} gap-filling stories")
    return {"gap_fill_enabled": True, "gap_fill_admitted": len(selected), "gap_fill_by_category": dict(counts)}
