from __future__ import annotations

import asyncio
import json
import os
import signal
import socket
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from pipeline.paths import LOCK_PATH
from pipeline.util import isoformat_z, utc_now


class LockError(RuntimeError):
    pass


def _process_start_time(pid: int) -> str | None:
    stat = Path(f"/proc/{pid}/stat")
    if not stat.exists():
        return None
    try:
        # Field 22 is the kernel clock tick start time. It is stable enough to
        # distinguish a reused PID from the process that wrote the lock.
        parts = stat.read_text(encoding="utf-8").split()
        return parts[21]
    except (OSError, IndexError):
        return None


def _process_state(pid: int) -> str | None:
    stat = Path(f"/proc/{pid}/stat")
    if not stat.exists():
        return None
    try:
        # Field 3 is the one-character process state. Zombies still have a
        # /proc entry but cannot own a useful pipeline lock.
        return stat.read_text(encoding="utf-8").split()[2]
    except (OSError, IndexError):
        return None


def _boot_id() -> str | None:
    path = Path("/proc/sys/kernel/random/boot_id")
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return None


def _pid_is_same_process(pid: int, start_time: str | None) -> bool:
    current = _process_start_time(pid)
    if current is None or _process_state(pid) == "Z":
        return False
    return start_time is None or current == start_time


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _terminate_verified_process(pid: int, start_time: str | None) -> None:
    if pid <= 0 or pid == os.getpid() or not _pid_is_same_process(pid, start_time):
        raise LockError(f"refusing to terminate unverified pid {pid}")
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            return
        except PermissionError as exc:
            raise LockError(f"permission denied terminating expired pipeline pid {pid}") from exc
        for _ in range(20):
            if not _pid_is_same_process(pid, start_time):
                return
            time.sleep(0.1)
    if _pid_is_same_process(pid, start_time):
        raise LockError(f"expired pipeline pid {pid} did not exit")


@dataclass
class PipelineLock:
    path: Path = LOCK_PATH
    timeout: timedelta = timedelta(minutes=30)
    run_id: str | None = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "pid": os.getpid(),
            "pid_start_time": _process_start_time(os.getpid()),
            "hostname": socket.gethostname(),
            "boot_id": _boot_id(),
            "cwd": str(Path.cwd()),
            "acquired_at": isoformat_z(),
            "run_id": self.run_id,
        }
        while True:
            try:
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            except FileExistsError:
                self._recover_or_raise()
                continue
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, sort_keys=True)
                f.write("\n")
            return

    def release(self) -> None:
        try:
            data = self._read()
        except FileNotFoundError:
            return
        if (
            data.get("pid") == os.getpid()
            and data.get("pid_start_time") == _process_start_time(os.getpid())
            and data.get("run_id") == self.run_id
        ):
            self.path.unlink(missing_ok=True)

    def __enter__(self) -> PipelineLock:
        self.acquire()
        return self

    def __exit__(self, *args: object) -> None:
        self.release()

    async def __aenter__(self) -> PipelineLock:
        # Acquire offloads to a worker thread so the calling event loop is not
        # blocked by signal-and-wait probing in `_terminate_verified_process`.
        await asyncio.to_thread(self.acquire)
        return self

    async def __aexit__(self, *args: object) -> None:
        await asyncio.to_thread(self.release)

    def _read(self) -> dict[str, Any]:
        with self.path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def _recover_or_raise(self) -> None:
        try:
            data = self._read()
        except (FileNotFoundError, json.JSONDecodeError):
            self.path.unlink(missing_ok=True)
            return
        pid = int(data.get("pid") or -1)
        acquired_at = _parse_ts(data.get("acquired_at"))
        expired = acquired_at is None or utc_now() - acquired_at > self.timeout
        current_hostname = socket.gethostname()
        lock_hostname = data.get("hostname")
        same_host = lock_hostname in {None, current_hostname}
        if not same_host:
            # We cannot verify a process on another host, so we cannot decide
            # the lock is safe to break even if the timestamp looks old.
            # Operator must intervene per the design's single-node assumption.
            raise LockError(
                f"pipeline lock belongs to host {lock_hostname!r}; refusing to claim without operator action"
            )
        current_boot_id = _boot_id()
        lock_boot_id = data.get("boot_id")
        same_boot = lock_boot_id in {None, current_boot_id}
        if not same_boot:
            # The recorded process cannot have survived a reboot on this host.
            self.path.unlink(missing_ok=True)
            return
        alive = pid > 0 and _pid_is_same_process(pid, data.get("pid_start_time"))
        if alive and not expired:
            raise LockError(f"pipeline already running under pid {pid}")
        if alive and expired:
            _terminate_verified_process(pid, data.get("pid_start_time"))
        self.path.unlink(missing_ok=True)
