from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from pipeline.maintenance import maintenance_once
from pipeline.state import StateDB, migrate


def _article_payload(article_id: str, *, headline: str | None = None, content_text: str | None = None) -> dict:
    payload = {
        "article_id": article_id,
        "source_id": "src",
        "source_name": "Source",
        "url": f"https://example.com/{article_id}",
        "canonical_url": f"https://example.com/{article_id}",
        "guid": article_id,
        "headline": headline or f"Headline {article_id}",
        "summary": f"Summary {article_id}",
        "published_at": "2026-05-31T12:00:00Z",
        "publish_date_estimated": False,
        "fetched_at": "2026-05-31T12:01:00Z",
        "content_type": "news",
        "language": "en",
        "collection": {},
        "fingerprints": {},
    }
    if content_text is not None:
        payload["content_text"] = content_text
    return payload


def _insert_article(
    state: StateDB,
    tmp_path: Path,
    article_id: str,
    *,
    published_at: str,
    event_id: str | None = None,
    is_filtered: int = 0,
    aggregation_status: str = "pending",
    content_text: str | None = "Full article text " * 100,
) -> Path:
    article = _article_payload(article_id, content_text=content_text)
    article["published_at"] = published_at
    article["fetched_at"] = published_at
    article_path = tmp_path / "articles" / f"{article_id}.json"
    article_path.parent.mkdir(parents=True, exist_ok=True)
    article_path.write_text(json.dumps(article), encoding="utf-8")
    state.insert_article(article, article_path)
    with state.conn:
        state.conn.execute(
            """
            UPDATE articles
            SET event_id = ?,
                is_filtered = ?,
                aggregation_status = ?,
                digest_status = 'completed'
            WHERE article_id = ?
            """,
            (event_id, is_filtered, aggregation_status, article_id),
        )
    return article_path


def _event_payload(
    event_id: str,
    *,
    status: str,
    updated_at: str,
    article_ids: list[str],
) -> dict:
    return {
        "event_id": event_id,
        "title": f"Event {event_id}",
        "category": "world",
        "thread": None,
        "status": status,
        "created_at": "2026-05-20T00:00:00Z",
        "updated_at": updated_at,
        "article_ids": article_ids,
        "article_count": len(article_ids),
        "keywords": ["event"],
        "entities": [],
        "confidence": 0.7,
        "newsworthiness": {"global": 0.5, "category": 0.6, "rationale_codes": ["test"]},
        "llm_metadata": {"stage": "aggregation", "prompt_version": "aggregation-v6"},
    }


def _insert_event(state: StateDB, event_dir: Path, payload: dict) -> Path:
    event_path = event_dir / f"{payload['event_id']}.json"
    event_path.parent.mkdir(parents=True, exist_ok=True)
    event_path.write_text(json.dumps(payload), encoding="utf-8")
    state.upsert_event(payload, event_path)
    return event_path


def test_maintenance_expires_old_pending_articles_without_touching_current(tmp_path: Path) -> None:
    db_path = tmp_path / "pipeline.db"
    event_dir = tmp_path / "events"
    migrate(db_path)
    now = datetime(2026, 5, 31, 12, tzinfo=UTC)

    with StateDB(db_path) as state:
        _insert_article(state, tmp_path, "old-pending", published_at="2026-05-29T23:59:00Z")
        _insert_article(state, tmp_path, "current-pending", published_at="2026-05-30T00:00:00Z")

    stats = maintenance_once(now=now, db_path=db_path, event_dir=event_dir, acquire_lock=False)

    with StateDB(db_path) as state:
        rows = {
            row["article_id"]: (row["aggregation_status"], row["is_filtered"], row["aggregation_reason"])
            for row in state.conn.execute(
                """
                SELECT article_id, aggregation_status, is_filtered, aggregation_reason
                FROM articles
                ORDER BY article_id
                """
            )
        }

    assert stats["expired_articles"] == 1
    assert rows["old-pending"][0] == "filtered_expired"
    assert rows["old-pending"][1] == 1
    assert "outside aggregation horizon" in rows["old-pending"][2]
    assert rows["current-pending"] == ("pending", 0, None)


def test_maintenance_lifecycle_and_event_artifact_reconciliation(tmp_path: Path) -> None:
    db_path = tmp_path / "pipeline.db"
    event_dir = tmp_path / "events"
    migrate(db_path)
    now = datetime(2026, 5, 31, 12, tzinfo=UTC)

    with StateDB(db_path) as state:
        _insert_article(
            state,
            tmp_path,
            "keep",
            published_at="2026-05-29T12:00:00Z",
            event_id="stale-event",
            aggregation_status="assigned",
        )
        stale_payload = _event_payload(
            "stale-event",
            status="active",
            updated_at="2026-05-28T00:00:00Z",
            article_ids=["keep", "stale-extra"],
        )
        stale_path = _insert_event(state, event_dir, stale_payload)

        empty_payload = _event_payload(
            "empty-event",
            status="active",
            updated_at="2026-05-28T00:00:00Z",
            article_ids=["missing"],
        )
        empty_path = _insert_event(state, event_dir, empty_payload)

        _insert_article(
            state,
            tmp_path,
            "archived-article",
            published_at="2026-04-20T12:00:00Z",
            event_id="archive-event",
            aggregation_status="assigned",
        )
        archive_payload = _event_payload(
            "archive-event",
            status="stale",
            updated_at="2026-04-20T12:00:00Z",
            article_ids=["archived-article"],
        )
        archive_path = _insert_event(state, event_dir, archive_payload)

    stats = maintenance_once(now=now, db_path=db_path, event_dir=event_dir, acquire_lock=False)

    with StateDB(db_path) as state:
        stale_row = state.conn.execute(
            "SELECT status, article_count FROM events WHERE event_id = 'stale-event'"
        ).fetchone()
        empty_row = state.conn.execute("SELECT 1 FROM events WHERE event_id = 'empty-event'").fetchone()
        archive_row = state.conn.execute("SELECT status FROM events WHERE event_id = 'archive-event'").fetchone()

    stale_json = json.loads(stale_path.read_text(encoding="utf-8"))
    archive_json = json.loads(archive_path.read_text(encoding="utf-8"))
    archived_article = json.loads((tmp_path / "articles" / "archived-article.json").read_text(encoding="utf-8"))

    assert stats["events_marked_stale"] == 2
    assert stats["events_archived"] == 1
    assert stats["events_reconciled"] == 1
    assert stats["events_deleted"] == 1
    assert stale_row["status"] == "stale"
    assert stale_row["article_count"] == 1
    assert stale_json["status"] == "stale"
    assert stale_json["article_ids"] == ["keep"]
    assert stale_json["article_count"] == 1
    assert empty_row is None
    assert not empty_path.exists()
    assert archive_row["status"] == "archived"
    assert archive_json["status"] == "archived"
    assert "content_text" not in archived_article
    assert archived_article["content_excerpt"].startswith("Full article text")


def test_maintenance_compacts_only_filtered_or_archived_article_text(tmp_path: Path) -> None:
    db_path = tmp_path / "pipeline.db"
    event_dir = tmp_path / "events"
    migrate(db_path)
    now = datetime(2026, 5, 31, 12, tzinfo=UTC)

    with StateDB(db_path) as state:
        active_path = _insert_article(
            state,
            tmp_path,
            "active-old",
            published_at="2026-05-20T12:00:00Z",
            event_id="active-event",
            aggregation_status="assigned",
        )
        active_event = _event_payload(
            "active-event",
            status="active",
            updated_at="2026-05-31T11:00:00Z",
            article_ids=["active-old"],
        )
        _insert_event(state, event_dir, active_event)

        filtered_path = _insert_article(
            state,
            tmp_path,
            "filtered-old",
            published_at="2026-05-20T12:00:00Z",
            is_filtered=1,
            aggregation_status="filtered_low_impact",
        )

    stats = maintenance_once(now=now, db_path=db_path, event_dir=event_dir, acquire_lock=False)

    active_json = json.loads(active_path.read_text(encoding="utf-8"))
    filtered_json = json.loads(filtered_path.read_text(encoding="utf-8"))

    assert stats["article_json_compacted"] == 1
    assert "content_text" in active_json
    assert "content_text" not in filtered_json
    assert "content_text_compacted_at" in filtered_json
