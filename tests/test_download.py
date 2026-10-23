"""
Tests for mandi_pipeline/download.py: a day is only COMPLETE when it provably is.
A fake client stands in for the API, so no network is used.

Run from the project folder:   python -m pytest -v
"""
import gzip
import json
from datetime import date

import pytest

from mandi_pipeline.api import ApiFailed
from mandi_pipeline.download import download_day


class FakeClient:
    """Answers like the API: a day total, and one answer per State filter.
    `empty_first` lists states that answer 'ok' with no rows the first time."""

    def __init__(self, states: dict, empty_first=(), duplicate_in=None, day_total=None):
        self.states = states
        self.empty_first = set(empty_first)
        self.duplicate_in = duplicate_in
        self.day_total = day_total if day_total is not None else sum(states.values())
        self.asked = []

    def get(self, params, label):
        state = params.get("filters[State]")
        self.asked.append(state or "DAY")
        if state is None:
            return {"status": "ok", "total": self.day_total, "records": []}
        if state in self.empty_first:
            self.empty_first.discard(state)
            return {"status": "ok", "total": 0, "records": []}     # the Tamil Nadu hiccup
        n = self.states.get(state, 0)
        records = [{"State": state, "Market": f"M{i}", "Modal_Price": 1000 + i} for i in range(min(n, params["limit"]))]
        if state == self.duplicate_in and records:
            records[-1] = dict(records[0])                           # one row twice, another missing
        return {"status": "ok", "total": n, "records": records}


DAY = date(2026, 10, 1)


def test_complete_day(tmp_path):
    c = FakeClient({"Tamil Nadu": 8455, "Uttar Pradesh": 1631, "Goa": 12})
    r = download_day(c, DAY, "ALL", tmp_path, workers=4)
    assert r.complete and r.rows_saved == r.day_total == 10098
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "state_Goa.json.gz", "state_Tamil_Nadu.json.gz", "state_Uttar_Pradesh.json.gz"]


def test_empty_answer_is_retried(tmp_path):
    """Incident 23 Oct: Tamil Nadu and Uttar Pradesh answered 'ok' with 0 rows."""
    c = FakeClient({"Tamil Nadu": 8455, "Uttar Pradesh": 1631, "Goa": 12},
                   empty_first={"Tamil Nadu", "Uttar Pradesh"})
    r = download_day(c, DAY, "ALL", tmp_path, workers=4)
    assert r.complete
    assert {"Tamil Nadu", "Uttar Pradesh"} <= set(r.retried_states)
    # Biggest first and stop once the day adds up: one batch, not every empty state.
    assert len(r.retried_states) <= 4
    assert c.asked.count("Tamil Nadu") == 2


def test_old_state_spelling_is_found(tmp_path):
    """Older years may use other names; they're tried only when the sum falls short."""
    c = FakeClient({"Tamil Nadu": 100, "Puducherry": 7})
    r = download_day(c, DAY, "ALL", tmp_path)
    assert r.complete and (tmp_path / "state_Puducherry.json.gz").exists()


def test_unknown_state_name_is_not_complete(tmp_path):
    c = FakeClient({"Tamil Nadu": 100}, day_total=125)     # 25 rows under a name we don't know
    r = download_day(c, DAY, "ALL", tmp_path)
    assert not r.complete and "day total 125" in r.problem


def test_duplicate_rows_mean_not_complete(tmp_path):
    """Pigeonhole: with the total matched, a missing row hides behind a duplicate."""
    c = FakeClient({"Tamil Nadu": 100}, duplicate_in="Tamil Nadu")
    r = download_day(c, DAY, "ALL", tmp_path)
    assert r.rows_saved == r.day_total
    assert not r.complete and r.duplicate_rows == 1


def test_state_too_big_for_one_request_fails_loudly(tmp_path):
    c = FakeClient({"Tamil Nadu": 8455})
    with pytest.raises(ApiFailed, match="limit must be above"):
        download_day(c, DAY, "ALL", tmp_path, limit=5000)


def test_rerun_replaces_old_files(tmp_path):
    (tmp_path / "page_0001.json.gz").write_bytes(b"old paged download")
    (tmp_path / "state_Goa.json.gz").write_bytes(b"old attempt")
    c = FakeClient({"Tamil Nadu": 3})
    download_day(c, DAY, "ALL", tmp_path)
    assert sorted(p.name for p in tmp_path.iterdir()) == ["state_Tamil_Nadu.json.gz"]


def test_saved_files_are_exactly_what_the_api_sent(tmp_path):
    c = FakeClient({"Goa": 2})
    download_day(c, DAY, "ALL", tmp_path)
    with gzip.open(tmp_path / "state_Goa.json.gz", "rt") as fh:
        saved = json.load(fh)
    assert saved == {"status": "ok", "total": 2,
                     "records": [{"State": "Goa", "Market": "M0", "Modal_Price": 1000},
                                 {"State": "Goa", "Market": "M1", "Modal_Price": 1001}]}


def test_day_with_no_rows(tmp_path):
    c = FakeClient({}, day_total=0)                      # e.g. a national holiday
    r = download_day(c, DAY, "ALL", tmp_path)
    assert r.complete and r.rows_saved == 0 and c.asked == ["DAY"]
