#!/usr/bin/env bash

set -uo pipefail

project_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
state_dir="$project_dir/data/state"
log_file="$state_dir/scheduled-pipeline.log"
old_log_file="$state_dir/scheduled-pipeline.log.1"
max_log_bytes=10485760

mkdir -p "$state_dir"
if [[ -f "$log_file" ]] && (( $(stat -c %s "$log_file") >= max_log_bytes )); then
  mv -f "$log_file" "$old_log_file"
fi

run_status=0
health_status=0
{
  echo "[$(date --iso-8601=seconds)] scheduled pipeline starting"
  cd "$project_dir" || exit 1
  # A slow hour (flex latency, large backfill) may still be running. Skip
  # quietly instead of failing the lock and mailing an error.
  if ! PYTHONPATH=. ./.venv/bin/python -m pipeline.cli lock-status >/dev/null 2>&1; then
    echo "[$(date --iso-8601=seconds)] previous run still holds the pipeline lock; skipping this hour"
    exit 0
  fi
  PYTHONPATH=. ./.venv/bin/python -m pipeline.cli run --verbose || run_status=$?
  PYTHONPATH=. ./.venv/bin/python -m pipeline.cli health --verbose || health_status=$?
  echo "[$(date --iso-8601=seconds)] scheduled pipeline finished run=$run_status health=$health_status"
} >>"$log_file" 2>&1

if (( run_status != 0 )); then
  exit "$run_status"
fi
exit "$health_status"
