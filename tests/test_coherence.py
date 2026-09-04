from __future__ import annotations

from pathlib import Path

import pytest

from pipeline.coherence import validate_partition
from pipeline.state import StateDB, migrate


@pytest.mark.parametrize("groups", [[[0], [0, 1]], [[0]], [[0], [2]], [[True], [1]], [[], [0, 1]]])
def test_coherence_rejects_incomplete_or_ambiguous_partitions(groups: list[list[int]]) -> None:
    with pytest.raises(ValueError):
        validate_partition({"confidence": 0.95, "groups": [{"article_indexes": g} for g in groups]}, 2)


def test_coherence_requires_high_confidence() -> None:
    with pytest.raises(ValueError, match="confidence"):
        validate_partition({"confidence": 0.7, "groups": [{"article_indexes": [0, 1]}]}, 2)


def test_partition_is_atomic_and_keeps_filtered_articles_excluded(tmp_path: Path) -> None:
    db = tmp_path / "state.db"
    migrate(db)
    base = {
        "event_id": "original",
        "title": "Apple stylus rumor",
        "category": "technology",
        "created_at": "2026-09-04T00:00:00Z",
        "updated_at": "2026-09-04T00:00:00Z",
        "article_ids": ["a1", "a2"],
        "article_count": 2,
    }
    with StateDB(db) as state:
        state.upsert_event(base, tmp_path / "original.json")
        for aid in ["a1", "a2", "excluded"]:
            state.insert_article(
                {
                    "article_id": aid,
                    "source_id": "source",
                    "source_name": "Source",
                    "url": f"https://example.test/{aid}",
                    "headline": aid,
                    "published_at": "2026-09-04T00:00:00Z",
                    "fetched_at": "2026-09-04T00:00:00Z",
                    "content_type": "news",
                    "language": "en",
                    "collection": {},
                    "fingerprints": {},
                },
                tmp_path / f"{aid}.json",
            )
        state.assign_articles_to_event(["a1", "a2", "excluded"], "original")
        with state.conn:
            state.conn.execute("UPDATE articles SET is_filtered = 1 WHERE article_id = ?", ("excluded",))
        first = {**base, "article_ids": ["a1"], "article_count": 1}
        second = {
            **base,
            "event_id": "leadership",
            "title": "Apple leadership transition",
            "article_ids": ["a2"],
            "article_count": 1,
        }
        with pytest.raises(ValueError, match="exactly once"):
            state.replace_event_partition("original", [(first, tmp_path / "original.json")])
        assert state.conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
        state.replace_event_partition(
            "original", [(first, tmp_path / "original.json"), (second, tmp_path / "leadership.json")]
        )
        rows = {
            r["article_id"]: (r["event_id"], r["is_filtered"])
            for r in state.conn.execute("SELECT article_id, event_id, is_filtered FROM articles")
        }
        assert rows == {"a1": ("original", 0), "a2": ("leadership", 0), "excluded": ("original", 1)}
        assert state.conn.execute("SELECT COUNT(*) FROM events WHERE last_editorial_at IS NULL").fetchone()[0] == 2


def test_membership_guard_separates_unrelated_new_developments(tmp_path: Path) -> None:
    from types import SimpleNamespace

    from pipeline.coherence import guard_event_extensions
    from pipeline.llm import GeminiResult

    db = tmp_path / "membership.db"
    migrate(db)
    event = {
        "event_id": "apple-stylus",
        "title": "Apple reportedly tests a phone stylus",
        "category": "technology",
        "created_at": "2026-09-04T00:00:00Z",
        "updated_at": "2026-09-04T00:00:00Z",
    }
    articles = [
        SimpleNamespace(event_id="apple-stylus", headline="Original report"),
        SimpleNamespace(
            event_id=None, headline="Apple appoints a CEO", summary="A leadership transition.", digest_summary=None
        ),
        SimpleNamespace(
            event_id=None, headline="Stylus release reportedly unlikely", summary="Same prototype.", digest_summary=None
        ),
    ]

    class Client:
        def generate_json(self, **kwargs):
            return GeminiResult(
                payload={"attachments": [{"index": 1, "keep": False}, {"index": 2, "keep": True}]},
                model="gemini-3.7-flash",
                elapsed_ms=1,
                usage={},
            )

    with StateDB(db) as state:
        state.upsert_event(event, tmp_path / "event.json")
        result = guard_event_extensions(
            groups=[{"article_indexes": [0, 1, 2], "existing_event_id": "apple-stylus"}],
            articles=articles,
            state=state,
            client=Client(),
        )
    assert result == [
        {"article_indexes": [0, 2], "existing_event_id": "apple-stylus", "group_index": -1},
        {"article_indexes": [1], "group_index": -1},
    ]


def test_split_preserves_prior_read_identity_and_underlying_news_dates(tmp_path: Path, monkeypatch) -> None:
    import json
    from datetime import UTC, datetime, timedelta

    import pipeline.coherence as coherence
    from pipeline.llm import GeminiResult
    from pipeline.util import isoformat_z

    event_dir = tmp_path / "events"
    story_dir = tmp_path / "stories"
    event_dir.mkdir()
    story_dir.mkdir()
    monkeypatch.setattr(coherence, "EVENT_DIR", event_dir)
    monkeypatch.setattr(coherence, "STORY_DIR", story_dir)
    now = datetime.now(UTC)
    old_date = isoformat_z(now - timedelta(days=4))
    recent_date = isoformat_z(now - timedelta(hours=1))
    db = tmp_path / "partition.db"
    migrate(db)
    original = {
        "event_id": "original",
        "title": "Apple tests a foldable phone stylus",
        "category": "technology",
        "created_at": old_date,
        "updated_at": isoformat_z(now),
        "article_ids": ["stylus", "ceo"],
        "article_count": 2,
        "status": "active",
    }
    (event_dir / "original.json").write_text(json.dumps(original))
    prior = {"story_id": "original", "created_at": old_date, "revision": 2, "revision_at": recent_date}
    (story_dir / "original.json").write_text(json.dumps(prior))

    class Client:
        def generate_json(self, **kwargs):
            return GeminiResult(
                payload={"confidence": 0.99, "groups": [{"article_indexes": [0]}, {"article_indexes": [1]}]},
                model="gemini-3.7-flash",
                elapsed_ms=1,
                usage={},
            )

    with StateDB(db) as state:
        state.upsert_event(original, event_dir / "original.json")
        for aid, headline, published in [
            ("stylus", original["title"], old_date),
            ("ceo", "Apple appoints a new chief executive", recent_date),
        ]:
            path = tmp_path / f"{aid}.json"
            path.write_text("{}")
            state.insert_article(
                {
                    "article_id": aid,
                    "source_id": "fixture",
                    "source_name": "Fixture",
                    "url": f"https://example.test/{aid}",
                    "headline": headline,
                    "published_at": published,
                    "fetched_at": published,
                    "content_type": "news",
                    "collection": {},
                    "fingerprints": {},
                },
                path,
            )
        state.assign_articles_to_event(["stylus", "ceo"], "original")
        state.start_run("partition", "aggregation")
        stats = coherence.review_event_coherence(state=state, client=Client(), run_id="partition", limit=1)
        assert stats == {"reviewed": 1, "split": 1, "failed": 0}
        original_row = state.conn.execute("SELECT updated_at,status FROM events WHERE event_id='original'").fetchone()
        assert tuple(original_row) == (old_date, "stale")
        assert state.conn.execute("SELECT event_id FROM articles WHERE article_id='ceo'").fetchone()[0] != "original"
    retained = json.loads((story_dir / "original.json").read_text())
    assert retained == {**prior, "_pending_coherence": True}
