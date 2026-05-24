from __future__ import annotations

import json
import os
from datetime import timedelta

import pytest

from pipeline.lock import LockError, PipelineLock, _process_start_time
from pipeline.util import isoformat_z, utc_now


def test_lock_rejects_active_same_process(tmp_path):
    lock_path = tmp_path / "pipeline.lock"
    lock_path.write_text(
        json.dumps(
            {
                "pid": os.getpid(),
                "pid_start_time": _process_start_time(os.getpid()),
                "acquired_at": isoformat_z(),
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(LockError):
        PipelineLock(lock_path, timeout=timedelta(minutes=30)).acquire()


def test_lock_recovers_stale_dead_process(tmp_path):
    lock_path = tmp_path / "pipeline.lock"
    lock_path.write_text(
        json.dumps(
            {
                "pid": 99999999,
                "pid_start_time": "old",
                "acquired_at": isoformat_z(utc_now() - timedelta(hours=2)),
            }
        ),
        encoding="utf-8",
    )

    lock = PipelineLock(lock_path, timeout=timedelta(minutes=30))
    lock.acquire()
    assert lock_path.exists()
    lock.release()
    assert not lock_path.exists()


def test_lock_refuses_foreign_host_even_when_expired(tmp_path):
    lock_path = tmp_path / "pipeline.lock"
    lock_path.write_text(
        json.dumps(
            {
                "pid": 12345,
                "pid_start_time": "irrelevant",
                "hostname": "some-other-host.example",
                "acquired_at": isoformat_z(utc_now() - timedelta(hours=12)),
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(LockError):
        PipelineLock(lock_path, timeout=timedelta(minutes=30)).acquire()
    # Lock file must remain so the operator can investigate.
    assert lock_path.exists()


def test_lock_recovers_when_boot_id_changed(tmp_path):
    import socket as _socket

    lock_path = tmp_path / "pipeline.lock"
    lock_path.write_text(
        json.dumps(
            {
                "pid": 99999999,
                "pid_start_time": "old",
                "hostname": _socket.gethostname(),
                "boot_id": "0000-different-boot-id",
                "acquired_at": isoformat_z(utc_now() - timedelta(minutes=5)),
            }
        ),
        encoding="utf-8",
    )

    lock = PipelineLock(lock_path, timeout=timedelta(minutes=30))
    lock.acquire()
    assert lock_path.exists()
    lock.release()
    assert not lock_path.exists()


@pytest.mark.asyncio
async def test_lock_async_context_manager_acquires_and_releases(tmp_path):
    lock_path = tmp_path / "pipeline.lock"
    lock = PipelineLock(lock_path, timeout=timedelta(minutes=30), run_id="async-run")
    async with lock:
        assert lock_path.exists()
    assert not lock_path.exists()


def test_lock_release_does_not_remove_replaced_lock(tmp_path):
    lock_path = tmp_path / "pipeline.lock"
    lock = PipelineLock(lock_path, timeout=timedelta(minutes=30), run_id="first-run")
    lock.acquire()
    lock_path.write_text(
        json.dumps(
            {
                "pid": os.getpid(),
                "pid_start_time": _process_start_time(os.getpid()),
                "acquired_at": isoformat_z(),
                "run_id": "second-run",
            }
        ),
        encoding="utf-8",
    )

    lock.release()

    assert lock_path.exists()
    lock_path.unlink()
