from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

from pipeline.brief import brief_once, build_brief
from pipeline.lock import PipelineLock
from pipeline.present import deploy_static_site


@pytest.fixture
def packet(tmp_path: Path) -> dict:
    db_path = tmp_path / "pipeline.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript("""
            CREATE TABLE articles (article_id TEXT, event_id TEXT, source_id TEXT, article_path TEXT,
                published_at TEXT, fetched_at TEXT, is_filtered INTEGER,
                digest_status TEXT, aggregation_status TEXT);
            CREATE TABLE pipeline_runs (stage TEXT, started_at TEXT, finished_at TEXT, status TEXT);
        """)
        for article_id, event_id, published, fetched, filtered in [
            ("new", "event", "2026-09-06T09:00:00Z", "2026-09-06T09:46:00Z", 0),
            ("context", "event", "2026-09-04T12:00:00Z", "2026-09-04T13:00:00Z", 0),
            ("late", None, "2026-09-04T12:00:00Z", "2026-09-06T09:46:00Z", 0),
            ("spam", "event", "2026-09-06T09:00:00Z", "2026-09-06T09:46:00Z", 1),
            ("future", "event", "2026-09-06T11:00:00Z", "2026-09-06T11:46:00Z", 0),
            ("old", None, "2026-09-05T12:00:00Z", "2026-09-05T13:00:00Z", 0),
            ("boundary", None, "2026-09-05T22:00:00Z", "2026-09-05T22:00:00Z", 0),
        ]:
            source_id = "cnn-us" if article_id == "context" else "abc-news-us"
            path = tmp_path / f"{article_id}.json"
            path.write_text(json.dumps({"article_id": article_id, "source_id": source_id,
                                        "content_text": f"Full {article_id} text", "fetched_at": fetched,
                                        "collection": {"secret": "not public"}, "llm_digest": {"impact": 0.9}}))
            conn.execute("INSERT INTO articles VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                         (article_id, event_id, source_id, str(path), published, fetched,
                          filtered, "completed", "assigned"))
    stories = tmp_path / "stories"
    stories.mkdir()
    (stories / "event.json").write_text(json.dumps({
        "story_id": "event", "event_id": "event", "created_at": "2026-09-05T12:00:00Z",
        "sources": [{"article_id": "new"}, {"article_id": "context"}], "_evidence": {"private": True},
    }))
    active = tmp_path / "active.json"
    active.write_text(json.dumps({"generated_at": "2026-09-06T09:50:00Z", "stories": [
        {"story_id": "event", "homepage_rank_score": 0.95},
    ]}))
    return {"now": datetime.fromisoformat("2026-09-06T10:00:00+00:00"),
            "db_path": db_path, "active_path": active, "story_dir": stories}


def test_full_packet_window_context_late_arrivals_and_exclusions(packet: dict) -> None:
    result = build_brief(**packet)
    articles = {article["article_id"]: article for article in result["articles"]}
    assert set(articles) == {"new", "context"}
    assert articles["context"]["in_window"] is False
    assert articles["new"]["content_text"] == "Full new text"
    assert "collection" not in articles["new"]
    assert result["stories"][0]["article_ids"] == ["context", "new"]
    assert result["stories"][0]["ranking"]["homepage_rank_score"] == 0.95
    assert "_evidence" not in result["stories"][0]["story"]


def test_late_snapshot_omits_summary_using_post_cutoff_report(packet: dict) -> None:
    path = packet["story_dir"] / "event.json"
    story = json.loads(path.read_text())
    story["sources"].append({"article_id": "future"})
    path.write_text(json.dumps(story))
    result = build_brief(**packet)
    assert result["stories"] == []
    assert result["articles"] == []


def test_revision_selects_existing_story_without_new_report(packet: dict) -> None:
    packet["now"] = datetime.fromisoformat("2026-09-07T20:00:00+00:00")
    path = packet["story_dir"] / "event.json"
    story = json.loads(path.read_text())
    story["revision_at"] = "2026-09-07T15:00:00Z"
    path.write_text(json.dumps(story))
    # Remove the unrelated later article so only the revision can select this event.
    with sqlite3.connect(packet["db_path"]) as conn:
        conn.execute("DELETE FROM articles WHERE article_id = 'future'")
    result = build_brief(**packet)
    assert result["window_article_count"] == 0
    assert result["story_count"] == 1


def test_publish_refreshes_atomic_readable_and_busy_retryable(packet: dict, tmp_path: Path) -> None:
    lock = tmp_path / "pipeline.lock"
    root = tmp_path / "www"
    with PipelineLock(lock):
        result = brief_once(**packet, publish_dir=root, lock_path=lock)
        assert result["reason"] == "pipeline_busy"
        assert not (root / "api/brief.json").exists()
    assert brief_once(**packet, publish_dir=root, lock_path=lock)["published"]
    target = root / "api/brief.json"
    original = target.read_bytes()
    assert target.stat().st_mode & 0o777 == 0o644
    packet["now"] = datetime.fromisoformat("2026-09-06T19:59:00+00:00")
    assert brief_once(**packet, publish_dir=root, lock_path=lock)["published"]
    assert target.read_bytes() != original
    packet["now"] = datetime.fromisoformat("2026-09-06T20:00:00+00:00")
    assert brief_once(**packet, publish_dir=root, lock_path=lock)["published"]
    assert json.loads(target.read_bytes())["window_end"] == "2026-09-06T20:00:00Z"


def test_failed_build_retains_previous_edition(packet: dict, tmp_path: Path) -> None:
    root = tmp_path / "www"
    lock = tmp_path / "pipeline.lock"
    brief_once(**packet, publish_dir=root, lock_path=lock)
    target = root / "api/brief.json"
    original = target.read_bytes()
    packet["now"] = datetime.fromisoformat("2026-09-06T20:00:00+00:00")
    packet["active_path"].write_text("invalid")
    with pytest.raises(json.JSONDecodeError):
        brief_once(**packet, publish_dir=root, lock_path=lock)
    assert target.read_bytes() == original
    assert not lock.exists()


def test_site_deployment_preserves_standalone_brief(tmp_path: Path) -> None:
    source = tmp_path / "build"
    source.mkdir()
    (source / "index.html").write_text("homepage")
    target = tmp_path / "www"
    (target / "api").mkdir(parents=True)
    (target / "api/brief.json").write_text('{"edition_id":"test"}')
    deploy_static_site(source_dir=source, publish_dir=target)
    deploy_static_site(source_dir=source, publish_dir=target)
    assert json.loads((target / "api/brief.json").read_text())["edition_id"] == "test"


def test_two_feeds_from_one_publisher_do_not_qualify(packet: dict) -> None:
    with sqlite3.connect(packet["db_path"]) as conn:
        conn.execute("UPDATE articles SET source_id = 'abc-news-top' WHERE article_id = 'context'")
    result = build_brief(**packet)
    assert result["stories"] == []
    assert result["articles"] == []


def test_filtered_second_publisher_does_not_qualify(packet: dict) -> None:
    with sqlite3.connect(packet["db_path"]) as conn:
        conn.execute("UPDATE articles SET is_filtered = 1 WHERE article_id = 'context'")
    result = build_brief(**packet)
    assert result["stories"] == []
    assert result["articles"] == []


def test_late_arrival_on_qualifying_story_is_in_window(packet: dict) -> None:
    with sqlite3.connect(packet["db_path"]) as conn:
        conn.execute("UPDATE articles SET event_id = 'event' WHERE article_id = 'late'")
    result = build_brief(**packet)
    assert next(article for article in result["articles"] if article["article_id"] == "late")["in_window"]


def test_rolling_window_is_12_hours_across_dst(packet: dict) -> None:
    packet["now"] = datetime.fromisoformat("2026-11-01T06:00:00+00:00")
    result = build_brief(**packet)
    assert result["window_start"] == "2026-10-31T18:00:00Z"
    assert result["window_end"] == "2026-11-01T06:00:00Z"
