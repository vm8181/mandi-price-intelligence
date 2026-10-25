"""
Downloading one day of mandi prices completely and provably.

Why state by state: the API's paging (offset/limit) is unreliable. Its order is not
stable between requests, so pages overlap: some rows arrive twice and others never,
while the row count still matches. Asking for one state per request, with a limit no
state reaches, has no page boundaries to overlap.
See docs/incidents/2026-10-23-pagination-overlap.md.

A day counts as COMPLETE only if both checks pass:
  1. The states add up exactly to the day's own total (catches missing states and
     empty answers).
  2. No row appears twice (pigeonhole: with the total matched, a missing row can only
     hide behind a duplicate).
     Exception: the source itself sometimes holds identical rows (19 on 3 Nov 2025).
     Then the whole day is asked for once more in a single request, and the day passes
     only if that answer holds exactly the same rows, copies included. Duplicates the
     source really has are kept as received; silver keeps one copy of each.
"""
import gzip
import json
import re
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from .api import ApiFailed, DataGovClient

# States as named in the API, largest first (from bronze, Mar to Oct 2026).
# Kerala appears under two names over that period.
STATES = ["Tamil Nadu", "Uttar Pradesh", "Madhya Pradesh", "Maharashtra", "Keralam",
          "Karnataka", "Punjab", "Gujarat", "Haryana", "Rajasthan", "Kerala",
          "Himachal Pradesh", "West Bengal", "Telangana", "Odisha", "Chattisgarh",
          "Tripura", "Uttarakhand", "Andhra Pradesh", "NCT of Delhi", "Jammu and Kashmir",
          "Assam", "Bihar", "Manipur", "Andaman and Nicobar", "Meghalaya", "Goa",
          "Nagaland", "Chandigarh", "Pondicherry", "Arunachal Pradesh", "Mizoram"]

# Other spellings that may appear in older years. Only asked for when the states above
# don't add up, so they cost nothing on normal days. "Uttrakhand" (sic) carries 65 to 111
# rows a day in Oct and Nov 2025, sometimes beside "Uttarakhand" on the same day.
EXTRA_STATE_NAMES = ["Chhattisgarh", "Puducherry", "Jharkhand", "Sikkim", "Ladakh",
                     "Delhi", "Orissa", "Uttrakhand", "Jammu & Kashmir", "Andaman and Nicobar Islands",
                     "Dadra and Nagar Haveli", "Daman and Diu",
                     "Dadra and Nagar Haveli and Daman and Diu", "Lakshadweep"]


@dataclass
class DayResult:
    day: date
    commodity: str
    day_total: int = 0              # rows the API says the day has
    rows_saved: int = 0             # rows we received and saved
    files: int = 0                  # state files written
    duplicate_rows: int = 0         # extra copies of identical rows (should be 0)
    retried_states: list = field(default_factory=list)   # states asked again after an empty answer
    source_duplicates: bool = False # duplicates proven to be in the source (whole-day answer matches)

    @property
    def complete(self) -> bool:
        return self.rows_saved == self.day_total and (self.duplicate_rows == 0 or self.source_duplicates)

    @property
    def problem(self) -> str:
        """Plain description of why the day isn't complete ('' if it is)."""
        issues = []
        if self.rows_saved != self.day_total:
            issues.append(f"states sum to {self.rows_saved:,}, day total {self.day_total:,}")
        if self.duplicate_rows and not self.source_duplicates:
            issues.append(f"{self.duplicate_rows:,} duplicate rows")
        return "; ".join(issues)


def canonical(record: dict) -> str:
    """One record as comparable text: all values as text, keys in a fixed order.
    (The API sends the same price as 4025 one day and "4025" another.)"""
    return json.dumps({k: str(v) for k, v in record.items()}, sort_keys=True)


def slug(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_")


def _ask_states(client: DataGovClient, base: dict, states: list, label: str,
                limit: int, workers: int) -> dict:
    """Ask for several states at once. Returns {state: answer}. Raises ApiFailed if any
    request fails for good, or if a state didn't fit in one request."""
    def one(state):
        body = client.get({**base, "filters[State]": state, "limit": limit, "offset": 0},
                          f"{label} {state}")
        records, state_total = body.get("records", []), int(body.get("total") or 0)
        if len(records) < state_total:
            raise ApiFailed(f"{state}: got {len(records):,} of {state_total:,} rows in one request; "
                            f"the limit must be above {state_total:,}")
        return state, body

    with ThreadPoolExecutor(max_workers=workers) as pool:
        return dict(pool.map(one, states))      # re-raises the first failure, if any


def _base(day: date, commodity: str) -> dict:
    base = {"format": "json", "filters[Arrival_Date]": day.strftime("%d-%m-%Y")}   # dd-mm-yyyy for the filter
    if commodity != "ALL":
        base["filters[Commodity]"] = commodity
    return base


def rows_on_disk(folder: Path):
    """Count the rows already saved for a day, and any exact duplicates.
    Returns (rows, duplicates), or None if there are no files or a file is unreadable
    (for example cut short when a run was stopped mid-write)."""
    files = sorted(folder.glob("state_*.json.gz"))
    if not files:
        return None
    records = []
    try:
        for f in files:
            with gzip.open(f, "rt", encoding="utf-8") as fh:
                records.extend(json.load(fh).get("records", []))
    except (OSError, EOFError, ValueError):
        return None
    duplicates = sum(n - 1 for n in Counter(map(canonical, records)).values() if n > 1)
    return len(records), duplicates


def already_complete(client: DataGovClient, day: date, commodity: str, folder: Path):
    """Check before downloading: do the files on disk already hold everything the API
    has for this day? One small request (the day's current total) against a local count.

    Returns (complete, rows_on_disk, api_total). Not complete if there are no files, a
    file is unreadable, rows are duplicated, or the API now has more (or fewer) rows,
    e.g. late uploads since the last download."""
    saved = rows_on_disk(folder)
    if saved is None:
        return False, 0, None
    rows, duplicates = saved
    label = f"{day:%d-%m-%Y} {commodity} check"
    api_total = int(client.get({**_base(day, commodity), "limit": 1, "offset": 0}, label).get("total") or 0)
    return rows == api_total and duplicates == 0, rows, api_total


def download_day(client: DataGovClient, day: date, commodity: str, folder: Path,
                 limit: int = 50000, workers: int = 4) -> DayResult:
    """Download one day (optionally one commodity), save each state's answer as
    received, and report whether the day is provably complete."""
    result = DayResult(day=day, commodity=commodity)
    base = _base(day, commodity)
    label = f"{day:%d-%m-%Y} {commodity}"

    # 1. The day's own total: what the states must add up to.
    result.day_total = int(client.get({**base, "limit": 1, "offset": 0}, f"{label} day total")
                           .get("total") or 0)

    answers = {}
    if result.day_total:
        # 2. Every known state, a few at a time.
        answers = _ask_states(client, base, STATES, label, limit, workers)

        def received():
            return sum(len(a.get("records", [])) for a in answers.values())

        # 3. Short? Ask again for states that came back empty: the API sometimes
        #    answers "ok" with no rows (seen for Tamil Nadu on 1 Oct 2026). Biggest
        #    states first, a batch at a time, stopping once the day adds up, so a
        #    hiccup costs a few requests rather than one per small, genuinely empty state.
        if received() < result.day_total:
            empty = [s for s in STATES if not answers.get(s, {}).get("records")]
            for i in range(0, len(empty), workers):
                batch = empty[i:i + workers]
                result.retried_states += batch
                answers.update(_ask_states(client, base, batch, label + " retry", limit, workers))
                if received() >= result.day_total:
                    break

        # 4. Still short? Try older spellings of state names.
        if received() < result.day_total:
            extra = _ask_states(client, base, EXTRA_STATE_NAMES, label, limit, workers)
            answers.update({s: a for s, a in extra.items() if a.get("records")})

    # 5. Checks: totals, and duplicates (pigeonhole).
    all_records = [r for a in answers.values() for r in a.get("records", [])]
    result.rows_saved = len(all_records)
    copies = Counter(map(canonical, all_records))
    result.duplicate_rows = sum(n - 1 for n in copies.values() if n > 1)

    # 5b. Totals match but some rows appear twice: is that the source, or a missing row
    #     hiding behind a copy? Ask for the whole day in one request (no pages, so nothing
    #     can overlap) and require exactly the same rows, the same number of times.
    if result.duplicate_rows and result.rows_saved == result.day_total:
        whole = client.get({**base, "limit": max(limit, result.day_total + 1), "offset": 0},
                           f"{label} whole day (duplicate check)").get("records", [])
        result.source_duplicates = (len(whole) == result.day_total
                                    and Counter(map(canonical, whole)) == copies)

    # 6. Save: replace whatever an earlier attempt left, then write each state as received.
    folder.mkdir(parents=True, exist_ok=True)
    for old in list(folder.glob("page_*.json*")) + list(folder.glob("state_*.json*")):
        old.unlink()
    for state, answer in answers.items():
        if answer.get("records"):
            with gzip.open(folder / f"state_{slug(state)}.json.gz", "wt", encoding="utf-8") as fh:
                json.dump(answer, fh)
            result.files += 1
    return result
