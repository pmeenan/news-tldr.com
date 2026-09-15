from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from pipeline.config import load_pipeline_config
from pipeline.editorial import editorial_backfill_rows, editorial_candidate_rows, write_active_stories_index
from pipeline.eligibility import admit_gap_stories, eligible_event_ids, single_admissions
from pipeline.present import build_static_site, deploy_static_site
from pipeline.state import StateDB, migrate
from pipeline.util import isoformat_z
from tests.test_present import _story

NOW = datetime(2026, 9, 13, 12, tzinfo=UTC)


@pytest.fixture
def state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    config = load_pipeline_config()
    config = replace(config, editorial={**config.editorial, "gap_fill_enabled": True,
                                       "gap_fill_target": 2, "single_source_hold_minutes": 0})
    monkeypatch.setattr("pipeline.eligibility.load_pipeline_config", lambda: config)
    migrate(tmp_path / "state.db")
    with StateDB(tmp_path / "state.db") as db:
        yield db


def add_event(state: StateDB, eid: str, *, category: str = "world", score: float = 0.5,
              publishers: tuple[str, ...] = ("A",), when: datetime | None = None,
              filtered: tuple[int, ...] = ()) -> None:
    stamp = isoformat_z(when or NOW - timedelta(hours=2))
    state.upsert_event({"event_id": eid, "title": eid, "category": category, "status": "active",
                        "created_at": stamp, "updated_at": stamp, "article_count": len(publishers),
                        "newsworthiness": {"global": score, "category": score}}, Path(f"{eid}.json"))
    with state.conn:
        for i, publisher in enumerate(publishers):
            state.conn.execute(
                """INSERT INTO articles(article_id, source_id, source_name, url, headline, fetched_at,
                   article_path, collection_json, event_id, is_filtered)
                   VALUES (?, ?, ?, ?, ?, ?, ?, '{}', ?, ?)""",
                (f"{eid}-{i}", f"feed-{eid}-{i}", publisher, f"https://example.test/{eid}/{i}",
                 eid, stamp, f"{eid}-{i}.json", eid, int(i in filtered)),
            )


def test_ranked_gap_reserves_slots_across_runs_and_survives_more_multis(state: StateDB) -> None:
    add_event(state, "multi", publishers=("A", "B"))
    add_event(state, "low", score=0.4)
    add_event(state, "high", score=0.9)
    assert eligible_event_ids(state, now=NOW) == {"multi", "high"}
    assert single_admissions(state) == {}  # Preflight never reserves slots.
    assert admit_gap_stories(state, now=NOW)["gap_fill_admitted"] == 1
    assert admit_gap_stories(state, now=NOW + timedelta(hours=1))["gap_fill_admitted"] == 0
    add_event(state, "multi2", publishers=("A", "B"))
    assert eligible_event_ids(state, now=NOW) == {"multi", "multi2", "high"}
    assert {r["event_id"] for r in editorial_candidate_rows(state=state, now=NOW, force=True)} == {
        "multi", "multi2", "high",
    }


def test_publisher_identity_and_filtered_articles_cannot_manufacture_eligibility(state: StateDB) -> None:
    for i in range(2):
        add_event(state, f"multi{i}", publishers=("A", "B"))
    add_event(state, "same-publisher", publishers=("Outlet - Science", "Outlet - World"))
    add_event(state, "filtered-second", publishers=("A", "B"), filtered=(1,))
    add_event(state, "all-filtered", publishers=("A", "B"), filtered=(0, 1))
    assert eligible_event_ids(state, now=NOW) == {"multi0", "multi1"}


def test_rolling_expiry_does_not_use_processing_updates(state: StateDB) -> None:
    add_event(state, "old1")
    add_event(state, "old2")
    admit_gap_stories(state, now=NOW)
    later = NOW + timedelta(hours=23)
    add_event(state, "new", when=later - timedelta(hours=1))
    assert "new" not in eligible_event_ids(state, now=later)
    with state.conn:
        state.conn.execute("UPDATE events SET updated_at = ?", (isoformat_z(later),))
    assert "new" in eligible_event_ids(state, now=NOW + timedelta(hours=24))
    assert admit_gap_stories(state, now=NOW + timedelta(hours=24))["gap_fill_admitted"] == 1
    assert {"old1", "old2", "new"} <= eligible_event_ids(state, now=NOW + timedelta(hours=24))


def test_category_gaps_are_independent_and_rejected_attempts_keep_reservations(state: StateDB) -> None:
    for i in range(3):
        add_event(state, f"world-{i}", publishers=("A", "B"))
    add_event(state, "world-single")
    for i in range(3):
        add_event(state, f"auto-{i}", category="automotive", score=0.9-i/10)
    admit_gap_stories(state, now=NOW)
    assert set(single_admissions(state)) == {"auto-0", "auto-1"}
    # No checkpoint is written: even failed/unprocessed admissions consume slots.
    assert admit_gap_stories(state, now=NOW + timedelta(hours=1))["gap_fill_admitted"] == 0


def test_hold_and_per_category_override(state: StateDB, monkeypatch: pytest.MonkeyPatch) -> None:
    config = load_pipeline_config()
    monkeypatch.setattr("pipeline.eligibility.load_pipeline_config", lambda: replace(
        config, editorial={"gap_fill_enabled": True, "gap_fill_target": 2,
                           "gap_fill_category_targets": {"automotive": 1}, "single_source_hold_minutes": 60},
    ))
    add_event(state, "fresh", when=NOW - timedelta(minutes=30))
    add_event(state, "auto1", category="automotive", score=0.9)
    add_event(state, "auto2", category="automotive", score=0.8)
    assert eligible_event_ids(state, now=NOW) == {"auto1"}


def test_retrospective_is_idempotent_and_removes_public_pages(state: StateDB, tmp_path: Path) -> None:
    story_dir = tmp_path / "stories"
    story_dir.mkdir()
    for eid, score in [("best", 0.9), ("second", 0.8), ("excluded", 0.4)]:
        add_event(state, eid, score=score)
        state.mark_event_editorial_completed(eid, isoformat_z(NOW - timedelta(hours=1)))
        story = _story(eid, updated_at=isoformat_z(NOW - timedelta(hours=1)))
        story["sources"] = [{"article_id": eid+"-0", "source_id": "feed-"+eid+"-0",
                             "source_name": "A", "url": "https://example.test/"+eid}]
        (story_dir / f"{eid}.json").write_text(json.dumps(story))
    before = state.conn.total_changes
    preview = admit_gap_stories(state, now=NOW, retrospective=True, dry_run=True)
    assert preview["gap_fill_admitted"] == 2
    assert state.conn.total_changes == before
    assert admit_gap_stories(state, now=NOW, retrospective=True)["gap_fill_admitted"] == 2
    assert admit_gap_stories(state, now=NOW, retrospective=True)["gap_fill_admitted"] == 0
    output = tmp_path / "index.json"
    stats = write_active_stories_index(state=state, story_dir=story_dir, output_path=output, generated_at=NOW)
    assert stats["active_index_excluded_coverage"] == 1
    assert {r["story_id"] for r in json.loads(output.read_text())["stories"]} == {"best", "second"}
    # Old published paths are managed files and must disappear on deployment.
    published = tmp_path / "public"
    excluded = published / "stories/excluded/index.html"
    excluded.parent.mkdir(parents=True)
    excluded.write_text("old story")
    (published / ".news-tldr-managed.json").write_text(json.dumps({"files": ["stories/excluded/index.html"]}))
    dist = tmp_path / "dist"
    build_static_site(output_dir=dist, story_dir=story_dir, active_stories_path=output, now=NOW)
    deploy_static_site(source_dir=dist, publish_dir=published)
    assert not excluded.exists()
    assert (published / "stories/best/index.html").exists()
    assert 'data-coverage-filter' not in (published / "index.html").read_text()
    assert (story_dir / "excluded.json").exists()  # Retain private source/story history.


def test_backfill_cannot_bypass_gap_policy(state: StateDB, tmp_path: Path) -> None:
    for i in range(2):
        add_event(state, f"multi{i}", publishers=("A", "B"))
    add_event(state, "excluded")
    state.mark_event_editorial_completed("excluded", isoformat_z(NOW - timedelta(hours=1)))
    (tmp_path / "excluded.json").write_text('{}')
    assert editorial_backfill_rows(state=state, story_dir=tmp_path, limit=10, now=NOW) == []
    assert "excluded" not in {r["event_id"] for r in editorial_candidate_rows(state=state, force=True, now=NOW)}


def test_material_revisions_count_but_routine_updates_do_not(state: StateDB) -> None:
    old = NOW - timedelta(days=3)
    for i in range(2):
        add_event(state, f"old-multi{i}", publishers=("A", "B"), when=old)
    add_event(state, "single")
    with state.conn:
        state.conn.execute("UPDATE events SET updated_at = ?", (isoformat_z(NOW),))
    assert "single" in eligible_event_ids(state, now=NOW)
    with state.conn:
        state.conn.execute("UPDATE events SET editorial_material_at = ? WHERE event_id LIKE 'old-multi%'",
                           (isoformat_z(NOW - timedelta(hours=1)),))
    assert "single" not in eligible_event_ids(state, now=NOW)


def test_multisource_event_can_publish_evidence_from_one_publisher(
    state: StateDB, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from concurrent.futures import Future

    from pipeline.editorial import _finish_story
    from tests.test_editorial import _event

    add_event(state, "event-1", publishers=("A", "B"))
    monkeypatch.setattr("pipeline.editorial.build_story_payload", lambda *args, **kwargs: {
        "sources": [{"publisher_id": "A"}], "created_at": isoformat_z(NOW),
    })
    future = Future()
    future.set_result({"usage_records": [], "usage": {}, "model": "test", "prompt_version": "test"})
    stats = {"failed": 0, "completed": 0, "rejected_event_ids": [], "usage": {}}
    assert _finish_story(_event(), future, state=state, run_id="test", stats=stats,
                             story_dir=tmp_path, progress=None, label="test")
    assert stats["failed"] == 0 and stats["completed"] == 1
    assert (tmp_path / "event-1.json").exists()
    assert state.conn.execute("SELECT last_editorial_at FROM events WHERE event_id='event-1'").fetchone()[0] is not None
