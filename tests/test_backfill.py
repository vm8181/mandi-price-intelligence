"""
Tests for which days backfill_history.py downloads again on a rerun.

Run from the project folder:   python -m pytest -v
"""
from datetime import date, datetime, timedelta

from backfill_history import REFRESH_HOURS, needs_download, time_is_up

NOW = datetime(2026, 10, 23, 23, 40)
DAY = date(2026, 10, 20)


def test_new_day_is_downloaded(tmp_path):
    assert needs_download(tmp_path, DAY, NOW)


def test_finished_day_is_never_downloaded_again(tmp_path):
    (tmp_path / "_DONE").write_text("x")
    assert not needs_download(tmp_path, DAY, NOW)


def test_recent_day_refreshed_an_hour_ago_is_skipped(tmp_path):
    """Restarting a run must not re-download the last 30 days every time."""
    (tmp_path / "_REFRESHED").write_text((NOW - timedelta(hours=1)).isoformat())
    assert not needs_download(tmp_path, DAY, NOW)


def test_recent_day_refreshed_yesterday_is_refreshed_again(tmp_path):
    """Once a day is still needed: mandis keep uploading for weeks."""
    (tmp_path / "_REFRESHED").write_text((NOW - timedelta(hours=REFRESH_HOURS + 1)).isoformat())
    assert needs_download(tmp_path, DAY, NOW)


def test_incomplete_or_failed_day_is_retried(tmp_path):
    """No marker is written for an incomplete or failed day, so it is always retried."""
    (tmp_path / "state_Goa.json.gz").write_bytes(b"partial")
    assert needs_download(tmp_path, DAY, NOW)


def test_unreadable_marker_means_refresh(tmp_path):
    (tmp_path / "_REFRESHED").write_text("not a date")
    assert needs_download(tmp_path, DAY, NOW)


def test_time_limit():
    """A long backfill stops before the next night's run, which refreshes recent days first."""
    assert not time_is_up(0, None, 10 ** 9)          # no limit
    assert not time_is_up(0, 20, 19.9 * 3600)
    assert time_is_up(0, 20, 20 * 3600)
