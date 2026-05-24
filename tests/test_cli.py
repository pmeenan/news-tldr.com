from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.cli import clean_data, main


def test_clean_data_requires_confirmation(tmp_path):
    with pytest.raises(ValueError, match="--yes"):
        clean_data(confirm=False, data_dir=tmp_path)


def test_clean_data_removes_database_articles_and_fetch_logs(tmp_path):
    db_path = tmp_path / "state" / "pipeline.db"
    article_dir = tmp_path / "staging" / "articles"
    fetch_log_dir = tmp_path / "staging" / "fetch-log"
    lock_path = tmp_path / "state" / "pipeline.lock"
    for path in (db_path, Path(f"{db_path}-wal"), Path(f"{db_path}-shm"), Path(f"{db_path}-journal")):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("sqlite", encoding="utf-8")
    article_dir.mkdir(parents=True)
    (article_dir / "article.json").write_text("{}", encoding="utf-8")
    fetch_log_dir.mkdir(parents=True)
    (fetch_log_dir / "run.jsonl").write_text("{}", encoding="utf-8")

    removed = clean_data(
        confirm=True,
        db_path=db_path,
        article_dir=article_dir,
        fetch_log_dir=fetch_log_dir,
        lock_path=lock_path,
        data_dir=tmp_path,
    )

    assert str(db_path) in removed
    assert str(Path(f"{db_path}-wal")) in removed
    assert str(article_dir) in removed
    assert str(fetch_log_dir) in removed
    assert not db_path.exists()
    assert not article_dir.exists()
    assert not fetch_log_dir.exists()


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
