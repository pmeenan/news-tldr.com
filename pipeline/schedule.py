"""Eastern-time batch schedule, independent of the cron daemon timezone."""
from datetime import UTC, datetime
from zoneinfo import ZoneInfo


def scheduled_hour(now: datetime) -> bool:
    local = now.astimezone(ZoneInfo("America/New_York"))
    return local.hour in {2, 6, 8, 10, 12, 14, 16, 18, 20, 22}


if __name__ == "__main__":
    raise SystemExit(0 if scheduled_hour(datetime.now(UTC)) else 1)
