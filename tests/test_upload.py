"""
Tests for which days upload_to_databricks.py sends. No network: only the local
selection logic is tested.

Run from the project folder:   python -m pytest -v
"""
import gzip
import json
import os
import time

from upload_to_databricks import finished_not_uploaded, provisional_candidates


def make_day(root, day, rows=3, duplicate=False, marker=True, done=False):
    """A day folder as the downloader leaves it: state files, then _REFRESHED."""
    folder = root / f"arrival_date={day}" / "commodity=ALL"
    folder.mkdir(parents=True)
    records = [{"State": "Goa", "Market": f"M{i}", "Modal_Price": 1000 + i} for i in range(rows)]
    if duplicate:
        records[-1] = dict(records[0])
    with gzip.open(folder / "state_Goa.json.gz", "wt", encoding="utf-8") as fh:
        json.dump({"status": "ok", "total": rows, "records": records}, fh)
    past = time.time() - 60
    os.utime(folder / "state_Goa.json.gz", (past, past))      # files written a minute ago
    if marker:
        (folder / "_REFRESHED").write_text("2026-10-24T21:00:00")  # checks passed after that
    if done:
        (folder / "_DONE").write_text("x")
    return folder


def test_complete_recent_day_is_sent(tmp_path):
    folder = make_day(tmp_path, "2026-10-20", rows=3)
    [(f, version, rows)] = provisional_candidates(tmp_path)
    assert f == folder and rows == 3 and len(version) == 15      # e.g. 20261024T210000


def test_same_version_is_not_sent_twice(tmp_path):
    folder = make_day(tmp_path, "2026-10-20")
    [(_, version, _)] = provisional_candidates(tmp_path)
    (folder / "_PROVISIONAL_UPLOADED").write_text(version)
    assert provisional_candidates(tmp_path) == []


def test_new_version_is_sent_after_late_uploads(tmp_path):
    """Late reports arrive, the day is re-downloaded: the new version must be sent."""
    folder = make_day(tmp_path, "2026-10-20")
    (folder / "_PROVISIONAL_UPLOADED").write_text("20261001T000000")   # an older version
    assert len(provisional_candidates(tmp_path)) == 1


def test_day_being_rewritten_is_not_sent(tmp_path):
    """The downloader removes _REFRESHED before rewriting a day."""
    make_day(tmp_path, "2026-10-20", marker=False)
    assert provisional_candidates(tmp_path) == []


def test_files_newer_than_the_check_are_not_sent(tmp_path):
    folder = make_day(tmp_path, "2026-10-20")
    os.utime(folder / "_REFRESHED", (time.time() - 3600, time.time() - 3600))
    assert provisional_candidates(tmp_path) == []


def test_duplicated_rows_are_not_sent(tmp_path):
    make_day(tmp_path, "2026-10-20", duplicate=True)
    assert provisional_candidates(tmp_path) == []


def test_final_day_goes_to_final_upload_only(tmp_path):
    folder = make_day(tmp_path, "2026-09-01", done=True)
    assert provisional_candidates(tmp_path) == []
    assert finished_not_uploaded(tmp_path) == [folder]


def test_newest_day_first(tmp_path):
    make_day(tmp_path, "2026-10-18")
    make_day(tmp_path, "2026-10-21")
    days = [f.parent.name for f, _, _ in provisional_candidates(tmp_path)]
    assert days == ["arrival_date=2026-10-21", "arrival_date=2026-10-18"]
