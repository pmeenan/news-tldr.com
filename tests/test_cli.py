from __future__ import annotations

import json
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


def test_run_completed_pipeline_runs_collect_digest_and_aggregate(monkeypatch):
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

    def fake_aggregate_once(*, force=False, progress=None, acquire_lock=True, **kwargs):
        assert kwargs == {}
        calls.append(("aggregate", force, progress is not None, acquire_lock))
        progress("aggregate: fake progress")
        return {"windows_processed": 3}

    monkeypatch.setattr("pipeline.cli.PipelineLock", FakePipelineLock)
    monkeypatch.setattr("pipeline.cli.collect_once", fake_collect_once)
    monkeypatch.setattr("pipeline.cli.digest_once", fake_digest_once)
    monkeypatch.setattr("pipeline.cli.aggregate_once", fake_aggregate_once)
    monkeypatch.setattr("pipeline.cli.migrate", lambda: None)

    result = run_completed_pipeline(force=True, progress=progress_messages.append)

    assert calls == [
        ("collect", True, False),
        ("digest", True, True, False),
        ("aggregate", True, True, False),
    ]
    assert lock_events == [("init", "pipeline.lock", True), "enter", "exit"]
    assert progress_messages == [
        "run: acquired pipeline lock",
        "run: starting collect",
        "collect: fake progress",
        "run: starting digest",
        "article digest: fake progress",
        "run: starting aggregate",
        "aggregate: fake progress",
    ]
    assert result == {
        "force": True,
        "stages": {
            "collect": {"feeds_seen": 1},
            "digest": {"completed": 2},
            "aggregate": {"windows_processed": 3},
        },
    }


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

    def fake_aggregate_once(*, force=False, progress=None, acquire_lock=True, **kwargs):
        assert kwargs == {}
        assert force is True
        assert progress is not None
        assert acquire_lock is False
        progress("aggregate: fake progress")
        return {"windows_processed": 3}

    monkeypatch.setattr("pipeline.cli.PipelineLock", FakePipelineLock)
    monkeypatch.setattr("pipeline.cli.collect_once", fake_collect_once)
    monkeypatch.setattr("pipeline.cli.digest_once", fake_digest_once)
    monkeypatch.setattr("pipeline.cli.aggregate_once", fake_aggregate_once)
    monkeypatch.setattr("pipeline.cli.migrate", lambda: None)
    monkeypatch.setattr("sys.argv", ["news-tldr-pipeline", "run", "--verbose", "--force"])

    main()

    captured = capsys.readouterr()
    assert "run: acquired pipeline lock" in captured.err
    assert "run: starting collect" in captured.err
    assert "collect: fake progress" in captured.err
    assert "article digest: fake progress" in captured.err
    assert "aggregate: fake progress" in captured.err
    assert json.loads(captured.out) == {
        "force": True,
        "stages": {
            "collect": {"feeds_seen": 1},
            "digest": {"completed": 2},
            "aggregate": {"windows_processed": 3},
        },
    }


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
