"""
Compare version 1 (paged) and version 2 (state by state) downloads of the same day.

Proves, row by row, what the paging overlap did: which rows version 1 got twice and
which rows it missed entirely, while its row count still matched the API total.

Run (after downloading the day with the new backfill_history.py):
    python compare_download.py --date 2026-04-01

Rows are compared on every field, with all values as text, because the API sometimes
sends a price as a number and sometimes as text ("4025" vs 4025).
"""
import argparse
import gzip
import json
import sys
from collections import Counter
from pathlib import Path


def load(folder: Path, pattern: str):
    """All records from the matching files, each as one canonical text line."""
    rows = []
    for f in sorted(folder.glob(pattern)):
        with gzip.open(f, "rt", encoding="utf-8") as fh:
            for rec in json.load(fh).get("records", []):
                rows.append(json.dumps({k: str(v) for k, v in rec.items()}, sort_keys=True))
    return rows


def describe(rows: list) -> str:
    counts = Counter(rows)
    extra_copies = sum(n - 1 for n in counts.values() if n > 1)
    return f"{len(rows):,} rows, {len(counts):,} distinct, {extra_copies:,} extra exact copies"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="yyyy-mm-dd, e.g. 2026-04-01")
    args = parser.parse_args()

    part = Path(f"arrival_date={args.date}") / "commodity=ALL"
    v1_dir, v2_dir = Path("data/raw/mandi") / part, Path("data/raw/mandi_v2") / part
    for d, what in ((v1_dir, "version 1"), (v2_dir, "version 2")):
        if not d.exists():
            sys.exit(f"No {what} download for {args.date} in {d}")

    v1 = load(v1_dir, "page_*.json.gz")
    v2 = load(v2_dir, "state_*.json.gz")
    c1, c2 = Counter(v1), Counter(v2)

    missed_by_v1 = c2 - c1          # rows the source has that version 1 never received
    extra_in_v1 = c1 - c2           # rows version 1 has more often than the source

    print(f"Day {args.date}")
    print(f"  Version 1 (paged):          {describe(v1)}")
    print(f"  Version 2 (state by state): {describe(v2)}")
    print()
    print(f"  Rows version 1 MISSED:              {sum(missed_by_v1.values()):,}")
    print(f"  Rows version 1 got EXTRA times:     {sum(extra_in_v1.values()):,}")
    print(f"  Row count difference (v2 - v1):     {len(v2) - len(v1):,}")
    print()
    if missed_by_v1:
        print("  Examples of rows version 1 missed:")
        for row in list(missed_by_v1)[:5]:
            r = json.loads(row)
            print(f"    {r.get('State')} | {r.get('Market')} | {r.get('Commodity')} | {r.get('Variety')} | "
                  f"modal {r.get('Modal_Price')}")
    print()
    print("Reading the result: if MISSED and EXTRA are similar and the counts match, the paged")
    print("download swapped some real rows for duplicates, which a count check can't see.")


if __name__ == "__main__":
    main()
