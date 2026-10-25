"""
Why are some days INCOMPLETE? Find the rows that no state request returned.

For each day given, this asks the API for the WHOLE day in one request (no state
filter, a limit above the day's total, so there are no pages to overlap), counts the
rows per state name, and compares that with the state files already on disk.
Any state name that appears in the API answer but has no file (or fewer rows) is
the reason the day doesn't add up. It also reports exact duplicate rows inside the
API's own answer, to tell "the source itself repeats rows" from "we fetched twice".

Downloads nothing to the data folder; read-only.

    python diagnose_incomplete_days.py 01-10-2025 15-10-2025 03-11-2025
"""
import gzip
import json
import os
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

from mandi_pipeline.api import DataGovClient
from mandi_pipeline.download import canonical

OUT_DIR = Path("data/raw/mandi_v2")


def on_disk(day):
    folder = OUT_DIR / f"arrival_date={day:%Y-%m-%d}" / "commodity=ALL"
    counts = Counter()
    for f in sorted(folder.glob("state_*.json.gz")):
        with gzip.open(f, "rt", encoding="utf-8") as fh:
            for r in json.load(fh).get("records", []):
                counts[r.get("State", "?")] += 1
    return counts


def main():
    key = os.environ.get("DATAGOV_API_KEY")
    if not key:
        sys.exit("Set DATAGOV_API_KEY first (same key the nightly run uses).")
    client = DataGovClient(key, silence_timeout=120)
    for text in sys.argv[1:] or ["01-10-2025"]:
        day = datetime.strptime(text, "%d-%m-%Y").date()
        body = client.get({"format": "json", "filters[Arrival_Date]": text,
                           "limit": 100000, "offset": 0}, f"{text} whole day")
        records = body.get("records", [])
        total = int(body.get("total") or 0)
        api = Counter(r.get("State", "?") for r in records)
        disk = on_disk(day)
        dupes = sum(n - 1 for n in Counter(map(canonical, records)).values() if n > 1)

        print(f"\n=== {text}: API total {total:,}, one request returned {len(records):,}, "
              f"on disk {sum(disk.values()):,}, duplicates inside the API answer {dupes:,}")
        if len(records) < total:
            print("    (the API capped this request, so the per-state view below is partial)")
        diff = [(s, api[s], disk.get(s, 0)) for s in api if api[s] != disk.get(s, 0)]
        if not diff:
            print("    every state name matches the files on disk")
        for s, a, d in sorted(diff, key=lambda x: -(x[1] - x[2])):
            print(f"    state name {s!r}: API {a:,} rows, on disk {d:,}")


if __name__ == "__main__":
    main()
