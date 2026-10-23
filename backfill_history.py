"""
Download mandi prices from data.gov.in into data/raw/mandi_v2, one folder per day.

Two modes, same command:
    $env:DATAGOV_API_KEY = "your-key"          # or saved once by setup_scheduled_task.ps1

    1. Historical backfill, once: the last 10 years up to yesterday
       python backfill_history.py --last-years 10 --all-commodities

    2. Daily update (what daily_run.ps1 does every night): the same command. Finished
       days are skipped in a moment; recent days are re-pulled to catch late uploads;
       any gap, however old, is filled.

Other examples
    python backfill_history.py --start 01-10-2026 --end 07-10-2026 --all-commodities
    python backfill_history.py --start 01-10-2026 --end 07-10-2026 --all-commodities --count-only

How each day is downloaded and proved complete: see mandi_pipeline/download.py.
Safe to stop at any time; finished days carry a _DONE marker and are never re-downloaded.

Options
    --last-years N / --last-days N / --start dd-mm-yyyy [--end dd-mm-yyyy]
    --all-commodities | --commodity NAME (repeatable)
    --workers 4        states requested at the same time
    --limit 50000      row limit per request; must exceed any state's rows in a day
    --oldest-first     work forwards in time instead of newest first
    --count-only       only report each day's total; download nothing
"""
import argparse
import csv
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

from mandi_pipeline.api import SAMPLE_KEY, ApiFailed, ApiKeyRefused, DataGovClient, say
from mandi_pipeline.download import download_day

OUT_DIR = Path("data/raw/mandi_v2")
MANIFEST = OUT_DIR / "_manifest.csv"
MANIFEST_FIELDS = ["arrival_date", "commodity", "rows_expected", "rows_saved", "files",
                   "duplicate_rows", "retried_states", "status", "pulled_at"]
STAPLES = ["Onion", "Tomato", "Potato", "Wheat", "Paddy(Common)"]

# Mandis keep uploading for weeks: Tamil Nadu's 1 Oct 2026 grew by 221 rows (2.6%)
# within 22 days. Days younger than this are re-pulled every run and never marked
# finished. See docs/decisions.md.
LATE_WINDOW_DAYS = 30


def acquire_lock(path: Path):
    """Only one download at a time. The OS releases the lock if this process ends,
    so a leftover lock file never blocks a later run."""
    fh = open(path, "a+")
    try:
        if os.name == "nt":
            import msvcrt
            fh.seek(0)
            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        sys.exit("Another download is already running (for example the scheduled task). "
                 "This run has stopped without changing anything.")
    return fh


def write_manifest(row: dict) -> None:
    """Append one line to the manifest. If an older manifest has different columns,
    keep it aside instead of mixing formats."""
    if MANIFEST.exists():
        with MANIFEST.open(encoding="utf-8") as fh:
            header = fh.readline().strip().split(",")
        if header != MANIFEST_FIELDS:
            MANIFEST.rename(MANIFEST.with_name(f"_manifest_old_{datetime.now():%Y%m%d_%H%M%S}.csv"))
    new = not MANIFEST.exists()
    with MANIFEST.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=MANIFEST_FIELDS)
        if new:
            writer.writeheader()
        writer.writerow(row)


def resolve_range(args):
    """Work out start and end dates. The end is never later than yesterday: today's
    prices are still arriving."""
    yesterday = date.today() - timedelta(days=1)
    end = datetime.strptime(args.end, "%d-%m-%Y").date() if args.end else yesterday
    if end > yesterday:
        print(f"End date capped at yesterday ({yesterday:%d-%m-%Y}): today's data is still arriving.\n")
        end = yesterday
    if args.start:
        start = datetime.strptime(args.start, "%d-%m-%Y").date()
    elif args.last_days:
        start = end - timedelta(days=args.last_days - 1)
    elif args.last_years:
        try:
            start = end.replace(year=end.year - args.last_years) + timedelta(days=1)
        except ValueError:                       # 29 Feb with no 29 Feb N years earlier
            start = end.replace(year=end.year - args.last_years, day=28) + timedelta(days=1)
    else:
        sys.exit("Give --start dd-mm-yyyy, --last-years N or --last-days N.")
    if end < start:
        sys.exit("The end date is before the start date.")
    return start, end


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--start"); p.add_argument("--end")
    p.add_argument("--last-years", type=int); p.add_argument("--last-days", type=int)
    p.add_argument("--commodity", action="append")
    p.add_argument("--all-commodities", action="store_true")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--limit", "--page-size", dest="limit", type=int, default=50000)
    p.add_argument("--timeout", type=int, default=45, help="seconds of silence before a request fails")
    p.add_argument("--oldest-first", action="store_true")
    p.add_argument("--count-only", action="store_true")
    args, _ = p.parse_known_args()

    start, end = resolve_range(args)
    days = [start + timedelta(d) for d in range((end - start).days + 1)]
    if not args.oldest_first:
        days.reverse()                           # newest first: the most useful data lands first
    commodities = ["ALL"] if args.all_commodities else (args.commodity or STAPLES)

    api_key = os.environ.get("DATAGOV_API_KEY")
    if not api_key:
        if not args.count_only:
            sys.exit("Set DATAGOV_API_KEY (the sample key returns only 10 rows), or use --count-only.")
        api_key = SAMPLE_KEY
        print("No DATAGOV_API_KEY set: using the public sample key for counting.\n")
    client = DataGovClient(api_key, silence_timeout=args.timeout)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    lock = None if args.count_only else acquire_lock(OUT_DIR / "_downloader.lock")  # held until exit

    def folder(d, c):
        return OUT_DIR / f"arrival_date={d.isoformat()}" / f"commodity={c}"

    todo = [(d, c) for d in days for c in commodities
            if args.count_only or not (folder(d, c) / "_DONE").exists()]
    print(f"Range: {start:%d %b %Y} to {end:%d %b %Y} | {len(days) * len(commodities):,} day(s), "
          f"{len(todo):,} to do, {len(days) * len(commodities) - len(todo):,} already finished\n")

    started, failures, total_rows = time.time(), [], 0
    for i, (d, c) in enumerate(todo, 1):
        label = f"{d:%d-%m-%Y} {c}"
        try:
            if args.count_only:
                base = {"format": "json", "filters[Arrival_Date]": f"{d:%d-%m-%Y}", "limit": 1, "offset": 0}
                if c != "ALL":
                    base["filters[Commodity]"] = c
                n = int(client.get(base, f"{label} day total").get("total") or 0)
                total_rows += n
                say(f"{label}: {n:,} rows")
                continue
            r = download_day(client, d, c, folder(d, c), limit=args.limit, workers=args.workers)
        except ApiKeyRefused as err:
            sys.exit(str(err))                   # fail fast: nothing else can succeed
        except ApiFailed as err:
            failures.append(label)
            write_manifest({"arrival_date": d.isoformat(), "commodity": c, "rows_expected": "",
                            "rows_saved": "", "files": "", "duplicate_rows": "", "retried_states": "",
                            "status": f"FAILED: {err}", "pulled_at": datetime.now().isoformat(timespec="seconds")})
            say(f"{label}: FAILED, will retry next run ({err})")
            continue

        recent = d > date.today() - timedelta(days=LATE_WINDOW_DAYS)
        if not r.complete:
            status = f"INCOMPLETE ({r.problem})"
        elif recent:
            status = "complete, re-pulled while recent"
        else:
            status = "complete"
            (folder(d, c) / "_DONE").write_text(datetime.now().isoformat(), encoding="utf-8")
        write_manifest({"arrival_date": d.isoformat(), "commodity": c, "rows_expected": r.day_total,
                        "rows_saved": r.rows_saved, "files": r.files, "duplicate_rows": r.duplicate_rows,
                        "retried_states": "|".join(r.retried_states), "status": status,
                        "pulled_at": datetime.now().isoformat(timespec="seconds")})
        total_rows += r.rows_saved
        per_day = (time.time() - started) / i
        left = (len(todo) - i) * per_day
        left_text = f"{left / 3600:.1f} h" if left >= 3600 else f"{left / 60:.0f} min"
        retried = f", retried empty: {', '.join(r.retried_states)}" if r.retried_states else ""
        say(f"{label}: {r.rows_saved:,} of {r.day_total:,} rows in {r.files} state files, "
            f"{status}{retried}   [{i:,}/{len(todo):,}, {per_day:.0f}s a day, about {left_text} left]")

    print(f"\nTotal rows {'available' if args.count_only else 'saved'}: {total_rows:,}")
    if failures:
        print(f"{len(failures)} day(s) failed and will be retried next run: {', '.join(failures[:10])}")
        sys.exit(1)


if __name__ == "__main__":
    main()
