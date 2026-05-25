from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_DIR = PROJECT_ROOT / "data"
STATE_DIR = DATA_DIR / "state"
STAGING_DIR = DATA_DIR / "staging"
ARTICLE_DIR = STAGING_DIR / "articles"
FETCH_LOG_DIR = STAGING_DIR / "fetch-log"
EVENT_DIR = DATA_DIR / "events"
DB_PATH = STATE_DIR / "pipeline.db"
LOCK_PATH = STATE_DIR / "pipeline.lock"
