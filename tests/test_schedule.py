from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from pipeline.schedule import scheduled_hour


def test_eastern_schedule_in_winter_and_summer():
    for month in (1, 7):
        start = datetime(2026, month, 15, tzinfo=UTC)
        due = [start + timedelta(hours=h) for h in range(24)
               if scheduled_hour(start + timedelta(hours=h))]
        assert len(due) == 10
        assert {d.astimezone(ZoneInfo("America/New_York")).hour for d in due} == {
            2, 6, 8, 10, 12, 14, 16, 18, 20, 22}


def test_dst_transition_uses_local_hour():
    # Spring skips 2am; fall repeats 1am, which is not scheduled.
    assert not scheduled_hour(datetime(2026, 3, 8, 7, tzinfo=UTC))
    assert scheduled_hour(datetime(2026, 3, 8, 10, tzinfo=UTC))
    assert not scheduled_hour(datetime(2026, 11, 1, 5, tzinfo=UTC))
    assert not scheduled_hour(datetime(2026, 11, 1, 6, tzinfo=UTC))
    assert scheduled_hour(datetime(2026, 11, 1, 7, tzinfo=UTC))
