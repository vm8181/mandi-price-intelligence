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
from mandi_pipeline.download import already_complete, download_day


class FakeClient:
    """Answers like the API: a day total, and one answer per State filter.
    `empty_first` lists states that answer 'ok' with no rows the first time."""

    def __init__(self, states: dict, empty_first=(), duplicate_in=None, day_total=None,
                 source_duplicate_in=None):
        self.states = states
        self.empty_first = set(empty_first)
        self.duplicate_in = duplicate_in                  # a fetch problem: a row replaced by a copy
        self.source_duplicate_in = source_duplicate_in    # the source itself holds one row twice
        self.day_total = day_total if day_total is not None else sum(states.values())
        self.asked = []

    def _rows(self, state, limit):
        n = self.states.get(state, 0)
        rows = [{"State": state, "Market": f"M{i}", "Modal_Price": 1000 + i} for i in range(min(n, limit))]
        if state == self.source_duplicate_in and rows:
            rows[-1] = dict(rows[0])                      # in the source: every answer has it
        return rows

    def get(self, params, label):
        state = params.get("filters[State]")
        if state is None and params["limit"] > 1:         # the whole day in one request
            self.asked.append("WHOLE DAY")
            rows = [r for s in self.states for r in self._rows(s, params["limit"])]
            return {"status": "ok", "total": self.day_total, "records": rows}
        self.asked.append(state or "DAY")
        if state is None:
            return {"status": "ok", "total": self.day_total, "records": []}
        if state in self.empty_first:
            self.empty_first.discard(state)
            return {"status": "ok", "total": 0, "records": []}     # the Tamil Nadu hiccup
        n = self.states.get(state, 0)
        records = self._rows(state, params["limit"])
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


def test_check_first_skips_a_day_already_complete(tmp_path):
    """Restart after a stop: a day fully on disk is verified with one small request,
    not downloaded again."""
    c = FakeClient({"Tamil Nadu": 30, "Goa": 5})
    download_day(c, DAY, "ALL", tmp_path)
    c.asked.clear()
    complete, on_disk, api_total = already_complete(c, DAY, "ALL", tmp_path)
    assert complete and on_disk == api_total == 35
    assert c.asked == ["DAY"]                            # one total request, no state downloads


def test_check_first_finds_late_uploads(tmp_path):
    download_day(FakeClient({"Tamil Nadu": 30}), DAY, "ALL", tmp_path)
    later = FakeClient({"Tamil Nadu": 32})               # 2 rows uploaded late
    complete, on_disk, api_total = already_complete(later, DAY, "ALL", tmp_path)
    assert not complete and (on_disk, api_total) == (30, 32)


def test_check_first_finds_a_partly_written_day(tmp_path):
    """A run stopped mid-write can leave a truncated file: that day must be redone."""
    download_day(FakeClient({"Tamil Nadu": 30}), DAY, "ALL", tmp_path)
    f = tmp_path / "state_Tamil_Nadu.json.gz"
    f.write_bytes(f.read_bytes()[:40])                   # cut the file short
    complete, _, _ = already_complete(FakeClient({"Tamil Nadu": 30}), DAY, "ALL", tmp_path)
    assert not complete


def test_check_first_with_nothing_on_disk(tmp_path):
    c = FakeClient({"Tamil Nadu": 30})
    complete, on_disk, api_total = already_complete(c, DAY, "ALL", tmp_path)
    assert not complete and api_total is None and c.asked == []   # no request wasted


def test_day_with_no_rows(tmp_path):
    c = FakeClient({}, day_total=0)                      # e.g. a national holiday
    r = download_day(c, DAY, "ALL", tmp_path)
    assert r.complete and r.rows_saved == 0 and c.asked == ["DAY"]


def test_misspelt_state_is_found(tmp_path):
    """Oct-Nov 2025: some rows come under "Uttrakhand" (sic), beside "Uttarakhand"."""
    c = FakeClient({"Tamil Nadu": 100, "Uttarakhand": 20, "Uttrakhand": 9})
    r = download_day(c, DAY, "ALL", tmp_path)
    assert r.complete and (tmp_path / "state_Uttrakhand.json.gz").exists()


def test_duplicates_in_the_source_are_proven_and_kept(tmp_path):
    """3 Nov 2025: the source itself holds identical rows. The whole day, asked for once,
    has exactly the same rows the same number of times, so the day is complete."""
    c = FakeClient({"Tamil Nadu": 100, "Goa": 5}, source_duplicate_in="Tamil Nadu")
    r = download_day(c, DAY, "ALL", tmp_path)
    assert r.duplicate_rows == 1 and r.source_duplicates
    assert r.complete and r.problem == ""
    assert c.asked.count("WHOLE DAY") == 1


def test_fetch_duplicate_hiding_a_missing_row_is_still_caught(tmp_path):
    """The whole-day check must not excuse a copy that replaced a real row."""
    c = FakeClient({"Tamil Nadu": 100}, duplicate_in="Tamil Nadu")
    r = download_day(c, DAY, "ALL", tmp_path)
    assert not r.source_duplicates and not r.complete
