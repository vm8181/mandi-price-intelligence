"""
Show the state of the mandi price data on this laptop: what is downloaded, what is
uploaded to Databricks, what is missing, what failed, and whether a run is going now.

Run:   python status.py
(status.ps1 runs this and adds the scheduled task's status and the latest log.)
"""
import csv
import os
from datetime import date, timedelta
from pathlib import Path

ROOT = Path("data/raw/mandi_v2")   # version 2: downloaded state by state
YEARS = 10                 # same window as the nightly run
LATE_WINDOW_DAYS = 7       # recent days stay open for late uploads (same as backfill_history.py)


def lock_is_held(path: Path) -> bool:
    """True if a download or upload is running right now (it holds this lock)."""
    if not path.exists():
        return False
    try:
        fh = open(path, "a+")
    except OSError:
        return True
    try:
        if os.name == "nt":
            import msvcrt
            fh.seek(0)
            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            fh.seek(0)
            msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(fh, fcntl.LOCK_UN)
        return False                       # we could take it, so nobody holds it
    except OSError:
        return True
    finally:
        fh.close()


def bar(done: int, total: int, width: int = 30) -> str:
    filled = int(width * done / total) if total else 0
    return "[" + "#" * filled + "-" * (width - filled) + f"] {done:,} of {total:,} ({done / total:.1%})"


def main() -> None:
    if not ROOT.exists():
        print("No data folder yet: nothing has been downloaded.")
        return

    yesterday = date.today() - timedelta(days=1)
    try:
        start = yesterday.replace(year=yesterday.year - YEARS) + timedelta(days=1)
    except ValueError:
        start = yesterday.replace(year=yesterday.year - YEARS, day=28) + timedelta(days=1)
    window = [start + timedelta(d) for d in range((yesterday - start).days + 1)]

    def folder(d):
        return ROOT / f"arrival_date={d.isoformat()}" / "commodity=ALL"

    done = {d for d in window if (folder(d) / "_DONE").exists()}
    uploaded = {d for d in done if (folder(d) / "_UPLOADED").exists()}
    recent_cutoff = date.today() - timedelta(days=LATE_WINDOW_DAYS)
    recent = [d for d in window if d > recent_cutoff]                     # still collecting late uploads
    recent_have = [d for d in recent if any(folder(d).glob("*.json.gz"))]
    gaps = [d for d in window if d not in done and d <= recent_cutoff]    # older days not finished yet

    size_mb = sum(f.stat().st_size for f in ROOT.rglob("*") if f.is_file()) / 1e6

    print("=" * 64)
    print(f"Mandi price data on this laptop   (window {start:%d %b %Y} to {yesterday:%d %b %Y})")
    print("=" * 64)
    print("Downloaded (finished days):", bar(len(done), len(window)))
    print("Uploaded to Databricks:    ", bar(len(uploaded), len(window)))
    if done:
        print(f"Finished days run from {min(done):%d %b %Y} to {max(done):%d %b %Y}")
    print(f"Disk used: {size_mb:,.0f} MB")
    print()
    print(f"Waiting to upload: {len(done) - len(uploaded):,} finished day(s)")
    print(f"Recent days still collecting late uploads: {len(recent_have)} of {len(recent)} downloaded so far")

    if gaps:
        newest_gap = max(gaps)
        print(f"Older days not finished yet: {len(gaps):,}  (newest: {newest_gap:%d %b %Y}, oldest: {min(gaps):%d %b %Y})")
    else:
        print("Older days not finished yet: none, the 10-year backfill is complete")

    # Days whose most recent attempt failed and that still aren't finished.
    manifest = ROOT / "_manifest.csv"
    if manifest.exists():
        last = {}
        with manifest.open(encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                last[row["arrival_date"]] = row
        failed = sorted(d for d, r in last.items()
                        if r["status"] in ("FAILED", "INCOMPLETE") and not (ROOT / f"arrival_date={d}").joinpath("commodity=ALL", "_DONE").exists())
        if failed:
            print(f"Failed on the last attempt (retried automatically next run): {len(failed)}  e.g. {', '.join(failed[:5])}")
        else:
            print("Failed days waiting for retry: none")

    print()
    dl, up = lock_is_held(ROOT / "_downloader.lock"), lock_is_held(ROOT / "_uploader.lock")
    print("Running right now:", ", ".join(x for x, on in (("download", dl), ("upload", up)) if on) or "nothing")


if __name__ == "__main__":
    main()
