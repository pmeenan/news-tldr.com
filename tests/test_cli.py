from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from pipeline.cli import clean_data, main, run_completed_pipeline


def test_clean_data_requires_confirmation(tmp_path):
    with pytest.raises(ValueError, match="--yes"):
        clean_data(confirm=False, data_dir=tmp_path)


def test_clean_data_removes_database_articles_and_fetch_logs(tmp_path):
    db_path = tmp_path / "state" / "pipeline.db"
    article_dir = tmp_path / "staging" / "articles"
    fetch_log_dir = tmp_path / "staging" / "fetch-log"
    event_dir = tmp_path / "events"
    published_dir = tmp_path / "published"
    lock_path = tmp_path / "state" / "pipeline.lock"
    for path in (db_path, Path(f"{db_path}-wal"), Path(f"{db_path}-shm"), Path(f"{db_path}-journal")):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("sqlite", encoding="utf-8")
    article_dir.mkdir(parents=True)
    (article_dir / "article.json").write_text("{}", encoding="utf-8")
    fetch_log_dir.mkdir(parents=True)
    (fetch_log_dir / "run.jsonl").write_text("{}", encoding="utf-8")
    event_dir.mkdir(parents=True)
    (event_dir / "event.json").write_text("{}", encoding="utf-8")
    published_dir.mkdir(parents=True)
    (published_dir / "active-stories.json").write_text("{}", encoding="utf-8")

    removed = clean_data(
        confirm=True,
        db_path=db_path,
        article_dir=article_dir,
        fetch_log_dir=fetch_log_dir,
        event_dir=event_dir,
        published_dir=published_dir,
        lock_path=lock_path,
        data_dir=tmp_path,
    )

    assert str(db_path) in removed
    assert str(Path(f"{db_path}-wal")) in removed
    assert str(article_dir) in removed
    assert str(fetch_log_dir) in removed
    assert str(event_dir) in removed
    assert str(published_dir) in removed
    assert not db_path.exists()
    assert not article_dir.exists()
    assert not fetch_log_dir.exists()
    assert not event_dir.exists()
    assert not published_dir.exists()


def test_clean_data_can_keep_fetch_logs(tmp_path):
    db_path = tmp_path / "state" / "pipeline.db"
    article_dir = tmp_path / "staging" / "articles"
    fetch_log_dir = tmp_path / "staging" / "fetch-log"
    lock_path = tmp_path / "state" / "pipeline.lock"
    db_path.parent.mkdir(parents=True)
    db_path.write_text("sqlite", encoding="utf-8")
    article_dir.mkdir(parents=True)
    fetch_log_dir.mkdir(parents=True)
    (fetch_log_dir / "run.jsonl").write_text("{}", encoding="utf-8")

    clean_data(
        confirm=True,
        include_fetch_logs=False,
        db_path=db_path,
        article_dir=article_dir,
        fetch_log_dir=fetch_log_dir,
        lock_path=lock_path,
        data_dir=tmp_path,
    )

    assert not db_path.exists()
    assert not article_dir.exists()
    assert fetch_log_dir.exists()


def test_clean_data_refuses_active_lock(tmp_path):
    db_path = tmp_path / "state" / "pipeline.db"
    article_dir = tmp_path / "staging" / "articles"
    fetch_log_dir = tmp_path / "staging" / "fetch-log"
    lock_path = tmp_path / "state" / "pipeline.lock"
    lock_path.parent.mkdir(parents=True)
    lock_path.write_text("locked", encoding="utf-8")

    with pytest.raises(RuntimeError, match="pipeline lock exists"):
        clean_data(
            confirm=True,
            db_path=db_path,
            article_dir=article_dir,
            fetch_log_dir=fetch_log_dir,
            lock_path=lock_path,
            data_dir=tmp_path,
        )


def test_clean_data_ignore_lock_removes_lock(tmp_path):
    db_path = tmp_path / "state" / "pipeline.db"
    article_dir = tmp_path / "staging" / "articles"
    fetch_log_dir = tmp_path / "staging" / "fetch-log"
    lock_path = tmp_path / "state" / "pipeline.lock"
    lock_path.parent.mkdir(parents=True)
    lock_path.write_text("locked", encoding="utf-8")

    removed = clean_data(
        confirm=True,
        ignore_lock=True,
        db_path=db_path,
        article_dir=article_dir,
        fetch_log_dir=fetch_log_dir,
        lock_path=lock_path,
        data_dir=tmp_path,
    )

    assert str(lock_path) in removed
    assert not lock_path.exists()


def test_clean_data_refuses_paths_outside_data_dir(tmp_path):
    db_path = tmp_path / "state" / "pipeline.db"
    article_dir = tmp_path.parent / "outside-articles"
    fetch_log_dir = tmp_path / "staging" / "fetch-log"
    lock_path = tmp_path / "state" / "pipeline.lock"
    article_dir.mkdir()

    with pytest.raises(ValueError, match="outside data directory"):
        clean_data(
            confirm=True,
            db_path=db_path,
            article_dir=article_dir,
            fetch_log_dir=fetch_log_dir,
            lock_path=lock_path,
            data_dir=tmp_path,
        )

    article_dir.rmdir()


def test_collect_verbose_writes_progress_to_stderr(monkeypatch, capsys):
    async def fake_collect_once(*, progress=None):
        assert progress is not None
        progress("collect: progress message")
        return {"feeds_seen": 1}

    monkeypatch.setattr("pipeline.cli.collect_once", fake_collect_once)
    monkeypatch.setattr("sys.argv", ["news-tldr-pipeline", "collect", "--verbose"])

    main()

    captured = capsys.readouterr()
    assert "collect: progress message" in captured.err
    assert json.loads(captured.out) == {"feeds_seen": 1}


def test_collect_without_verbose_keeps_stderr_quiet(monkeypatch, capsys):
    async def fake_collect_once(*, progress=None):
        assert progress is None
        return {"feeds_seen": 1}

    monkeypatch.setattr("pipeline.cli.collect_once", fake_collect_once)
    monkeypatch.setattr("sys.argv", ["news-tldr-pipeline", "collect"])

    main()

    captured = capsys.readouterr()
    assert captured.err == ""
    assert json.loads(captured.out) == {"feeds_seen": 1}


def test_run_completed_pipeline_runs_through_presentation(monkeypatch):
    calls = []
    lock_events = []
    progress_messages = []

    class FakePipelineLock:
        def __init__(self, path, timeout, run_id=None):
            lock_events.append(("init", path.name, run_id.startswith("pipeline-run-")))

        def __enter__(self):
            lock_events.append("enter")
            return self

        def __exit__(self, *args):
            lock_events.append("exit")

    async def fake_collect_once(*, progress=None, acquire_lock=True):
        calls.append(("collect", progress is not None, acquire_lock))
        progress("collect: fake progress")
        return {"feeds_seen": 1}

    def fake_digest_once(*, force=False, progress=None, acquire_lock=True, **kwargs):
        assert kwargs == {}
        calls.append(("digest", force, progress is not None, acquire_lock))
        progress("article digest: fake progress")
        return {"completed": 2}

    def fake_maintenance_once(*, progress=None, acquire_lock=True, **kwargs):
        assert kwargs == {}
        calls.append(("maintenance", progress is not None, acquire_lock))
        progress("maintenance: fake progress")
        return {"expired_articles": 1}

    def fake_aggregate_once(*, force=False, progress=None, acquire_lock=True, **kwargs):
        assert kwargs == {}
        calls.append(("aggregate", force, progress is not None, acquire_lock))
        progress("aggregate: fake progress")
        return {"windows_processed": 3}

    def fake_editorial_once(*, force=False, progress=None, acquire_lock=True, **kwargs):
        assert kwargs == {}
        calls.append(("editorial", force, progress is not None, acquire_lock))
        progress("editorial: fake progress")
        return {"completed": 4}

    def fake_presentation_once(*, publish=None, progress=None, acquire_lock=True, **kwargs):
        assert kwargs == {}
        calls.append(("presentation", publish, progress is not None, acquire_lock))
        progress("presentation: fake progress")
        return {"published": True}

    monkeypatch.setattr("pipeline.cli.PipelineLock", FakePipelineLock)
    monkeypatch.setattr("pipeline.cli.collect_once", fake_collect_once)
    monkeypatch.setattr("pipeline.cli.digest_once", fake_digest_once)
    monkeypatch.setattr("pipeline.cli.maintenance_once", fake_maintenance_once)
    monkeypatch.setattr("pipeline.cli.aggregate_once", fake_aggregate_once)
    monkeypatch.setattr("pipeline.cli.editorial_once", fake_editorial_once)
    monkeypatch.setattr("pipeline.cli.presentation_once", fake_presentation_once)
    monkeypatch.setattr("pipeline.cli.migrate", lambda: None)
    monkeypatch.setattr("pipeline.cli._recover_stale_pipeline_runs", lambda: 0)
    monkeypatch.setattr("pipeline.cli._pending_editorial_count", lambda: 0)
    monkeypatch.setattr(
        "pipeline.cli._pending_upstream_counts",
        lambda **kwargs: {"digest": 0, "aggregation": 0},
    )
    monkeypatch.setattr("pipeline.cli._max_article_rowid", lambda: 42)

    result = run_completed_pipeline(force=True, progress=progress_messages.append)

    assert calls == [
        ("maintenance", True, False),
        ("collect", True, False),
        ("digest", True, True, False),
        ("aggregate", True, True, False),
        ("editorial", True, True, False),
        ("presentation", None, True, False),
    ]
    assert lock_events == [("init", "pipeline.lock", True), "enter", "exit"]
    assert progress_messages == [
        "run: acquired pipeline lock",
        "run: starting maintenance",
        "maintenance: fake progress",
        "run: starting collection alongside backlog processing",
        "collect: fake progress",
        "run: collection complete; starting downstream work",
        "run: starting digest",
        "article digest: fake progress",
        "run: starting aggregate",
        "aggregate: fake progress",
        "run: starting editorial",
        "editorial: fake progress",
        "run: starting presentation and publish",
        "presentation: fake progress",
    ]
    assert result == {
        "force": True,
        "stages": {
            "maintenance": {"expired_articles": 1},
            "collect": {"feeds_seen": 1},
            "digest": {"completed": 2},
            "aggregate": {"windows_processed": 3},
            "editorial": {"completed": 4},
            "presentation": {"published": True},
        },
    }


@pytest.mark.parametrize("validation_rejection", [False, True])
def test_run_completed_pipeline_defers_new_work_while_editorial_backlog_remains(
    monkeypatch, validation_rejection,
):
    calls = []

    class FakePipelineLock:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    backlog_counts = iter((12, 0 if validation_rejection else 3))
    exclusions = []

    def pending_count(**kwargs):
        exclusions.append(kwargs.get("excluded_event_ids"))
        return next(backlog_counts)
    collection_started = threading.Event()

    async def fake_collect_once(*, progress=None, acquire_lock=True):
        calls.append("collect")
        collection_started.set()
        return {"feeds_seen": 1}

    def fake_editorial_once(**kwargs):
        assert collection_started.wait(1)
        calls.append(("editorial", kwargs["force"]))
        return {"completed": 9, **({"failed": 3, "rejected_event_ids": ["a", "b", "c"]}
                                  if validation_rejection else {})}

    monkeypatch.setattr("pipeline.cli.PipelineLock", FakePipelineLock)
    monkeypatch.setattr("pipeline.cli.migrate", lambda: None)
    monkeypatch.setattr("pipeline.cli._recover_stale_pipeline_runs", lambda: 0)
    monkeypatch.setattr("pipeline.cli._pending_editorial_count", pending_count)
    monkeypatch.setattr(
        "pipeline.cli._pending_upstream_counts",
        lambda **kwargs: {"digest": 0, "aggregation": 0},
    )
    monkeypatch.setattr("pipeline.cli._max_article_rowid", lambda: 42)
    monkeypatch.setattr(
        "pipeline.cli.maintenance_once",
        lambda **kwargs: calls.append("maintenance") or {"ok": True},
    )
    monkeypatch.setattr(
        "pipeline.cli.editorial_once",
        fake_editorial_once,
    )
    monkeypatch.setattr(
        "pipeline.cli.presentation_once",
        lambda **kwargs: calls.append("presentation") or {"published": True},
    )
    monkeypatch.setattr(
        "pipeline.cli.collect_once",
        fake_collect_once,
    )
    monkeypatch.setattr(
        "pipeline.cli.digest_once",
        lambda **kwargs: calls.append("digest") or {} if validation_rejection
        else pytest.fail("digestion must be deferred"),
    )
    monkeypatch.setattr(
        "pipeline.cli.aggregate_once",
        lambda **kwargs: calls.append("aggregate") or {} if validation_rejection
        else pytest.fail("aggregation must be deferred"),
    )

    result = run_completed_pipeline(progress=lambda _message: None)

    initial_calls = ["maintenance", "collect", ("editorial", False), "presentation"]
    if validation_rejection:
        assert calls == initial_calls + ["digest", "aggregate", ("editorial", False), "presentation"]
        assert not result.get("backlog_blocked")
        assert result["stages"]["backlog_editorial"]["failed"] == 3
        assert exclusions == [None, ["a", "b", "c"]]
    else:
        assert calls == initial_calls
        assert result["backlog_blocked"] is True
        assert result["pending_editorial"] == 3
    assert result["stages"]["collect"] == {"feeds_seen": 1}


def test_run_completed_pipeline_drains_prior_upstream_work_before_collection(monkeypatch):
    calls = []

    class FakePipelineLock:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    upstream_counts = iter(
        (
            {"digest": 4, "aggregation": 2},
            {"digest": 1, "aggregation": 0},
        )
    )
    collection_started = threading.Event()

    async def fake_collect_once(*, progress=None, acquire_lock=True):
        calls.append("collect")
        collection_started.set()
        return {"feeds_seen": 1}

    def fake_digest_once(**kwargs):
        assert collection_started.wait(1)
        assert kwargs["max_article_rowid"] == 42
        calls.append(("digest", kwargs["force"]))
        return {"completed": 3}

    monkeypatch.setattr("pipeline.cli.PipelineLock", FakePipelineLock)
    monkeypatch.setattr("pipeline.cli.migrate", lambda: None)
    monkeypatch.setattr("pipeline.cli._recover_stale_pipeline_runs", lambda: 0)
    monkeypatch.setattr("pipeline.cli._pending_editorial_count", lambda: 0)
    monkeypatch.setattr(
        "pipeline.cli._pending_upstream_counts",
        lambda **kwargs: next(upstream_counts),
    )
    monkeypatch.setattr("pipeline.cli._max_article_rowid", lambda: 42)
    monkeypatch.setattr(
        "pipeline.cli.maintenance_once",
        lambda **kwargs: calls.append("maintenance") or {"ok": True},
    )
    monkeypatch.setattr(
        "pipeline.cli.digest_once",
        fake_digest_once,
    )
    monkeypatch.setattr(
        "pipeline.cli.aggregate_once",
        lambda **kwargs: calls.append(("aggregate", kwargs["force"])) or {"processed": 2},
    )
    monkeypatch.setattr(
        "pipeline.cli.editorial_once",
        lambda **kwargs: calls.append(("editorial", kwargs["force"])) or {"completed": 2},
    )
    monkeypatch.setattr(
        "pipeline.cli.presentation_once",
        lambda **kwargs: calls.append("presentation") or {"published": True},
    )
    monkeypatch.setattr(
        "pipeline.cli.collect_once",
        fake_collect_once,
    )

    result = run_completed_pipeline(progress=lambda _message: None)

    assert calls == [
        "maintenance",
        "collect",
        ("digest", False),
        ("aggregate", False),
        ("editorial", False),
        "presentation",
    ]
    assert result["backlog_blocked"] is True
    assert result["pending_digest"] == 1
    assert result["pending_aggregation"] == 0


def test_run_command_writes_progress_and_combined_stats(monkeypatch, capsys):
    class FakePipelineLock:
        def __init__(self, path, timeout, run_id=None):
            assert path.name == "pipeline.lock"
            assert run_id.startswith("pipeline-run-")

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

    async def fake_collect_once(*, progress=None, acquire_lock=True):
        assert progress is not None
        assert acquire_lock is False
        progress("collect: fake progress")
        return {"feeds_seen": 1}

    def fake_digest_once(*, force=False, progress=None, acquire_lock=True, **kwargs):
        assert kwargs == {}
        assert force is True
        assert progress is not None
        assert acquire_lock is False
        progress("article digest: fake progress")
        return {"completed": 2}

    def fake_maintenance_once(*, progress=None, acquire_lock=True, **kwargs):
        assert kwargs == {}
        assert progress is not None
        assert acquire_lock is False
        progress("maintenance: fake progress")
        return {"expired_articles": 1}

    def fake_aggregate_once(*, force=False, progress=None, acquire_lock=True, **kwargs):
        assert kwargs == {}
        assert force is True
        assert progress is not None
        assert acquire_lock is False
        progress("aggregate: fake progress")
        return {"windows_processed": 3}

    def fake_editorial_once(*, force=False, progress=None, acquire_lock=True, **kwargs):
        assert kwargs == {}
        assert force is True
        assert progress is not None
        assert acquire_lock is False
        progress("editorial: fake progress")
        return {"completed": 4}

    def fake_presentation_once(*, publish=None, progress=None, acquire_lock=True, **kwargs):
        assert kwargs == {}
        assert publish is None
        assert progress is not None
        assert acquire_lock is False
        progress("presentation: fake progress")
        return {"published": True}

    monkeypatch.setattr("pipeline.cli.PipelineLock", FakePipelineLock)
    monkeypatch.setattr("pipeline.cli.collect_once", fake_collect_once)
    monkeypatch.setattr("pipeline.cli.digest_once", fake_digest_once)
    monkeypatch.setattr("pipeline.cli.maintenance_once", fake_maintenance_once)
    monkeypatch.setattr("pipeline.cli.aggregate_once", fake_aggregate_once)
    monkeypatch.setattr("pipeline.cli.editorial_once", fake_editorial_once)
    monkeypatch.setattr("pipeline.cli.presentation_once", fake_presentation_once)
    monkeypatch.setattr("pipeline.cli.migrate", lambda: None)
    monkeypatch.setattr("pipeline.cli._recover_stale_pipeline_runs", lambda: 0)
    monkeypatch.setattr("pipeline.cli._pending_editorial_count", lambda: 0)
    monkeypatch.setattr(
        "pipeline.cli._pending_upstream_counts",
        lambda **kwargs: {"digest": 0, "aggregation": 0},
    )
    monkeypatch.setattr("pipeline.cli._max_article_rowid", lambda: 42)
    monkeypatch.setattr("sys.argv", ["news-tldr-pipeline", "run", "--verbose", "--force"])

    main()

    captured = capsys.readouterr()
    assert "run: acquired pipeline lock" in captured.err
    assert "run: starting maintenance" in captured.err
    assert "maintenance: fake progress" in captured.err
    assert "run: starting collect" in captured.err
    assert "collect: fake progress" in captured.err
    assert "article digest: fake progress" in captured.err
    assert "aggregate: fake progress" in captured.err
    assert "editorial: fake progress" in captured.err
    assert "presentation: fake progress" in captured.err
    assert json.loads(captured.out) == {
        "force": True,
        "stages": {
            "maintenance": {"expired_articles": 1},
            "collect": {"feeds_seen": 1},
            "digest": {"completed": 2},
            "aggregate": {"windows_processed": 3},
            "editorial": {"completed": 4},
            "presentation": {"published": True},
        },
    }


def test_run_command_no_publish_overrides_config(monkeypatch, capsys):
    def fake_run_completed_pipeline(*, force=False, publish=None, dry_run=False, progress=None):
        assert force is False
        assert publish is False
        assert dry_run is False
        assert progress is None
        return {"force": False, "stages": {"presentation": {"published": False}}}

    monkeypatch.setattr("pipeline.cli.run_completed_pipeline", fake_run_completed_pipeline)
    monkeypatch.setattr("sys.argv", ["news-tldr-pipeline", "run", "--no-publish"])

    main()

    captured = capsys.readouterr()
    assert captured.err == ""
    assert json.loads(captured.out) == {
        "force": False,
        "stages": {"presentation": {"published": False}},
    }


def test_run_command_dry_run_uses_non_mutating_preflight(monkeypatch, capsys):
    def fake_run_completed_pipeline(*, force=False, publish=None, dry_run=False, progress=None):
        assert force is False
        assert publish is None
        assert dry_run is True
        assert progress is not None
        progress("preflight: fake progress")
        return {
            "force": False,
            "dry_run": True,
            "preflight": {"validation": {"valid": True}},
        }

    monkeypatch.setattr("pipeline.cli.run_completed_pipeline", fake_run_completed_pipeline)
    monkeypatch.setattr(
        "sys.argv",
        ["news-tldr-pipeline", "run", "--dry-run", "--verbose"],
    )

    main()

    captured = capsys.readouterr()
    assert "preflight: fake progress" in captured.err
    assert json.loads(captured.out)["dry_run"] is True


def test_editorial_verbose_passes_selection_and_writes_progress(monkeypatch, capsys):
    def fake_editorial_once(
        *,
        limit=None,
        concurrency=None,
        force=False,
        event_ids=None,
        progress=None,
    ):
        assert limit == 2
        assert concurrency == 3
        assert force is True
        assert event_ids == ["event-1", "event-2"]
        assert progress is not None
        progress("editorial: fake progress")
        return {"completed": 2}

    monkeypatch.setattr("pipeline.cli.editorial_once", fake_editorial_once)
    monkeypatch.setattr("pipeline.cli.migrate", lambda: None)
    monkeypatch.setattr(
        "sys.argv",
        [
            "news-tldr-pipeline",
            "editorial",
            "--limit",
            "2",
            "--concurrency",
            "3",
            "--event-id",
            "event-1",
            "--event-id",
            "event-2",
            "--force",
            "--verbose",
        ],
    )

    main()

    captured = capsys.readouterr()
    assert "editorial: fake progress" in captured.err
    assert json.loads(captured.out) == {"completed": 2}


def test_present_build_only_writes_progress_and_skips_publish(monkeypatch, capsys, tmp_path):
    def fake_presentation_once(*, publish=None, publish_dir=None, progress=None):
        assert publish is False
        assert publish_dir == tmp_path / "production"
        assert progress is not None
        progress("presentation: fake progress")
        return {"built": True, "published": False}

    monkeypatch.setattr("pipeline.cli.presentation_once", fake_presentation_once)
    monkeypatch.setattr("pipeline.cli.migrate", lambda: None)
    monkeypatch.setattr(
        "sys.argv",
        [
            "news-tldr-pipeline",
            "present",
            "--build-only",
            "--publish-dir",
            str(tmp_path / "production"),
            "--verbose",
        ],
    )

    main()

    captured = capsys.readouterr()
    assert "presentation: fake progress" in captured.err
    assert json.loads(captured.out) == {"built": True, "published": False}


def test_aggregate_verbose_writes_progress_to_stderr(monkeypatch, capsys):
    def fake_aggregate_once(
        *,
        range_start=None,
        range_end=None,
        limit_windows=None,
        dry_run=False,
        progress=None,
        force=False,
    ):
        assert range_start == "2026-05-24T16:00:00Z"
        assert range_end == "2026-05-24T22:00:00Z"
        assert limit_windows == 1
        assert dry_run is True
        assert progress is not None
        assert force is False
        progress("aggregate: fake progress")
        return {"windows_processed": 1}

    monkeypatch.setattr("pipeline.cli.aggregate_once", fake_aggregate_once)
    monkeypatch.setattr("pipeline.cli.migrate", lambda: None)
    monkeypatch.setattr(
        "sys.argv",
        [
            "news-tldr-pipeline",
            "aggregate",
            "--range-start",
            "2026-05-24T16:00:00Z",
            "--range-end",
            "2026-05-24T22:00:00Z",
            "--limit-windows",
            "1",
            "--dry-run",
            "--verbose",
        ],
    )

    main()

    captured = capsys.readouterr()
    assert "aggregate: fake progress" in captured.err
    assert json.loads(captured.out) == {"windows_processed": 1}


def test_aggregate_force_passes_force_to_aggregate_once(monkeypatch, capsys):
    def fake_aggregate_once(
        *,
        range_start=None,
        range_end=None,
        limit_windows=None,
        dry_run=False,
        progress=None,
        force=False,
    ):
        assert force is True
        return {"windows_processed": 2}

    monkeypatch.setattr("pipeline.cli.aggregate_once", fake_aggregate_once)
    monkeypatch.setattr("pipeline.cli.migrate", lambda: None)
    monkeypatch.setattr(
        "sys.argv",
        [
            "news-tldr-pipeline",
            "aggregate",
            "--range-start",
            "2026-05-24T16:00:00Z",
            "--range-end",
            "2026-05-24T22:00:00Z",
            "--force",
        ],
    )

    main()

    captured = capsys.readouterr()
    assert json.loads(captured.out) == {"windows_processed": 2}


def test_maintenance_verbose_dry_run_writes_progress_to_stderr(monkeypatch, capsys):
    def fake_maintenance_once(*, dry_run=False, progress=None):
        assert dry_run is True
        assert progress is not None
        progress("maintenance: fake progress")
        return {"dry_run": True, "expired_articles": 2}

    monkeypatch.setattr("pipeline.cli.maintenance_once", fake_maintenance_once)
    monkeypatch.setattr("pipeline.cli.migrate", lambda: None)
    monkeypatch.setattr("sys.argv", ["news-tldr-pipeline", "maintenance", "--verbose", "--dry-run"])

    main()

    captured = capsys.readouterr()
    assert "maintenance: fake progress" in captured.err
    assert json.loads(captured.out) == {"dry_run": True, "expired_articles": 2}


def test_digest_verbose_writes_progress_to_stderr(monkeypatch, capsys):
    def fake_digest_once(
        *,
        range_start=None,
        range_end=None,
        limit=None,
        concurrency=None,
        force=False,
        progress=None,
    ):
        assert range_start == "2026-05-24T16:00:00Z"
        assert range_end == "2026-05-24T22:00:00Z"
        assert limit == 5
        assert concurrency == 2
        assert force is True
        assert progress is not None
        progress("article digest: fake progress")
        return {"completed": 5}

    monkeypatch.setattr("pipeline.cli.digest_once", fake_digest_once)
    monkeypatch.setattr("pipeline.cli.migrate", lambda: None)
    monkeypatch.setattr(
        "sys.argv",
        [
            "news-tldr-pipeline",
            "digest",
            "--range-start",
            "2026-05-24T16:00:00Z",
            "--range-end",
            "2026-05-24T22:00:00Z",
            "--limit",
            "5",
            "--concurrency",
            "2",
            "--force",
            "--verbose",
        ],
    )

    main()

    captured = capsys.readouterr()
    assert "article digest: fake progress" in captured.err
    assert json.loads(captured.out) == {"completed": 5}


def test_aggregation_experiment_writes_progress_and_output(monkeypatch, capsys, tmp_path):
    output_path = tmp_path / "experiment.json"

    def fake_run_grouping_experiment(
        *,
        limit,
        published_date=None,
        published_after=None,
        published_before=None,
        modes=("titles", "titles_summaries"),
        progress=None,
    ):
        assert limit == 3
        assert published_date is None
        assert published_after is None
        assert published_before is None
        assert modes == ("titles", "titles_summaries")
        assert progress is not None
        progress("aggregation experiment: fake progress")
        return {"article_count": 3}

    written = {}

    def fake_write_experiment_result(result, path):
        written["result"] = result
        written["path"] = path

    monkeypatch.setattr("pipeline.cli.run_grouping_experiment", fake_run_grouping_experiment)
    monkeypatch.setattr("pipeline.cli.write_experiment_result", fake_write_experiment_result)
    monkeypatch.setattr(
        "sys.argv",
        ["news-tldr-pipeline", "aggregation-experiment", "--limit", "3", "--output", str(output_path), "--verbose"],
    )

    main()

    captured = capsys.readouterr()
    assert "aggregation experiment: fake progress" in captured.err
    assert written == {"result": {"article_count": 3}, "path": output_path}
    payload = json.loads(captured.out)
    assert payload["output"] == str(output_path)
    assert payload["result"] == {"article_count": 3}


def test_aggregation_experiment_date_mode_defaults_to_full_day(monkeypatch, capsys, tmp_path):
    output_path = tmp_path / "experiment.json"

    def fake_run_grouping_experiment(
        *,
        limit,
        published_date=None,
        published_after=None,
        published_before=None,
        modes=("titles", "titles_summaries"),
        progress=None,
    ):
        assert limit is None
        assert published_date == "2026-05-24"
        assert published_after is None
        assert published_before is None
        assert modes == ("titles_summaries",)
        assert progress is None
        return {"article_count": 945}

    monkeypatch.setattr("pipeline.cli.run_grouping_experiment", fake_run_grouping_experiment)
    monkeypatch.setattr("pipeline.cli.write_experiment_result", lambda result, path: None)
    monkeypatch.setattr(
        "sys.argv",
        [
            "news-tldr-pipeline",
            "aggregation-experiment",
            "--published-date",
            "2026-05-24",
            "--mode",
            "titles-summaries",
            "--output",
            str(output_path),
        ],
    )

    main()

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["result"] == {"article_count": 945}


def test_aggregation_experiment_time_window_defaults_to_uncapped(monkeypatch, capsys, tmp_path):
    output_path = tmp_path / "experiment.json"

    def fake_run_grouping_experiment(
        *,
        limit,
        published_date=None,
        published_after=None,
        published_before=None,
        modes=("titles", "titles_summaries"),
        progress=None,
    ):
        assert limit is None
        assert published_date is None
        assert published_after == "2026-05-24T16:00:00Z"
        assert published_before == "2026-05-24T22:00:00Z"
        assert modes == ("titles_summaries",)
        assert progress is None
        return {"article_count": 565}

    monkeypatch.setattr("pipeline.cli.run_grouping_experiment", fake_run_grouping_experiment)
    monkeypatch.setattr("pipeline.cli.write_experiment_result", lambda result, path: None)
    monkeypatch.setattr(
        "sys.argv",
        [
            "news-tldr-pipeline",
            "aggregation-experiment",
            "--published-after",
            "2026-05-24T16:00:00Z",
            "--published-before",
            "2026-05-24T22:00:00Z",
            "--mode",
            "titles-summaries",
            "--output",
            str(output_path),
        ],
    )

    main()

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["result"] == {"article_count": 565}
