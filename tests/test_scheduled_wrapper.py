from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    "schedule_status,lock_status,expected,stages",
    [(0, 0, 0, True), (3, 0, 0, False), (1, 0, 1, False),
     (0, 3, 0, False), (0, 1, 1, False), (0, 127, 127, False)],
)
def test_scheduler_distinguishes_skips_from_failures(
    tmp_path: Path, schedule_status: int, lock_status: int, expected: int, stages: bool,
) -> None:
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    wrapper = scripts / "run-scheduled.sh"
    wrapper.write_bytes((Path(__file__).parents[1] / "scripts/run-scheduled.sh").read_bytes())
    interpreter = tmp_path / ".venv/bin/python"
    interpreter.parent.mkdir(parents=True)
    interpreter.write_text(
        '#!/bin/bash\n'
        'if [[ "$1" == */schedule.py ]]; then exit "$SCHEDULE_STATUS"; fi\n'
        'if [[ "$3" == lock-status ]]; then\n'
        '  if [[ "$LOCK_STATUS" == 1 ]]; then echo "ModuleNotFoundError: httpx" >&2; fi\n'
        '  exit "$LOCK_STATUS"\n'
        'fi\n'
        'echo "$3" >> stages.txt\n',
    )
    interpreter.chmod(0o755)
    result = subprocess.run(
        ["bash", str(wrapper), "--scheduled"], capture_output=True, text=True,
        env={**os.environ, "SCHEDULE_STATUS": str(schedule_status), "LOCK_STATUS": str(lock_status)},
        timeout=10,
    )
    assert result.returncode == expected
    assert (tmp_path / "stages.txt").exists() is stages
    if stages:
        assert (tmp_path / "stages.txt").read_text().splitlines() == ["run", "brief", "health"]
    if expected:
        assert "check failed" in result.stderr
    if schedule_status == 0 and lock_status == 1:
        log = (tmp_path / "data/state/scheduled-pipeline.log").read_text()
        assert "ModuleNotFoundError: httpx" in log
        assert "previous run still holds" not in log
