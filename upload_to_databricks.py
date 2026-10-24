"""
Upload raw mandi prices from this laptop to the Databricks volume: finished days, and
the latest complete version of each recent day.

Why this script exists
  The data must be downloaded on this laptop (data.gov.in refuses cloud servers),
  then copied into Databricks. Copying the whole folder each time would also send
  half-downloaded days and re-send everything every night. This script sends only what
  is new, and checks each upload arrived.

Two kinds of upload
  1. Final days (older than 30 days, marked _DONE): uploaded once to
       mandi_v2/arrival_date=D/commodity=ALL/
     Pages first, _DONE last; sizes verified; then _UPLOADED is written on the laptop.

  2. Provisional days (the last 30 days, still receiving late reports): each time a day's
     data changes and the new download passes both completeness checks, that version is
     uploaded to
       mandi_v2_provisional/arrival_date=D/pulled_at=YYYYMMDDTHHMMSS/expected_rows=N/
     The folder name carries the row count, so Databricks can tell a fully uploaded
     version from a partly uploaded one and only ever uses complete versions. Silver uses
     the final day if it exists, otherwise the latest complete provisional version.
     A day whose data hasn't changed isn't uploaded again.

  Anything that fails is left unmarked and retried on the next run (safe to re-run).

Setup (once): the Databricks CLI must already be configured (databricks configure),
because this script reads the same connection file, C:\\Users\\<you>\\.databrickscfg.
    python -m pip install databricks-sdk

Run:
    python upload_to_databricks.py --dry-run          # list what would be uploaded, send nothing
    python upload_to_databricks.py                    # upload everything new
    python upload_to_databricks.py --limit 5          # at most 5 final days (a quick test)
    python upload_to_databricks.py --no-provisional   # final days only
"""
import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from mandi_pipeline.download import rows_on_disk

LOCAL_ROOT = Path("data/raw/mandi_v2")      # version 2: downloaded state by state
DEFAULT_VOLUME = "/Volumes/mandi_dev/bronze/raw_files/mandi_v2"
DEFAULT_PROVISIONAL_VOLUME = "/Volumes/mandi_dev/bronze/raw_files/mandi_v2_provisional"
MAX_RETRIES = 3


def acquire_lock(path: Path):
    """Only one upload at a time. The operating system releases the lock automatically
    if this script stops or crashes, so a leftover lock file never blocks a later run.
    Returns None if another upload holds it."""
    fh = open(path, "a+")
    try:
        if os.name == "nt":                       # Windows
            import msvcrt
            fh.seek(0)
            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
        else:                                     # Mac / Linux
            import fcntl
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        fh.close()
        return None
    return fh


def finished_not_uploaded(root: Path):
    """Day/commodity folders that are fully downloaded but not yet uploaded, newest first."""
    folders = [p.parent for p in root.glob("arrival_date=*/commodity=*/_DONE")]
    pending = [f for f in folders if not (f / "_UPLOADED").exists()]
    return sorted(pending, key=lambda f: f.parent.name, reverse=True)


def snapshot(folder: Path):
    """Names, sizes and modification times of a day's data files: if any of these
    change during an upload, the day was being rewritten and the upload is not trusted."""
    return sorted((f.name, f.stat().st_size, f.stat().st_mtime_ns) for f in folder.glob("state_*.json.gz"))


def provisional_candidates(root: Path):
    """Recent days whose current files are a complete version not yet uploaded.

    A day qualifies only if:
      - it is not final (no _DONE);
      - _REFRESHED exists and was written after the newest data file, i.e. the files on
        disk are the ones that passed both completeness checks (the downloader removes
        _REFRESHED before rewriting a day);
      - the files hold no duplicate rows;
      - this version (the newest file's time) hasn't been uploaded already.
    Returns [(folder, version, expected_rows)], newest day first."""
    found = []
    for marker in root.glob("arrival_date=*/commodity=*/_REFRESHED"):
        folder = marker.parent
        if (folder / "_DONE").exists():
            continue
        files = list(folder.glob("state_*.json.gz"))
        if not files:
            continue
        newest = max(f.stat().st_mtime for f in files)
        if marker.stat().st_mtime < newest:
            continue                              # files changed after the last complete check
        version = datetime.fromtimestamp(newest).strftime("%Y%m%dT%H%M%S")
        sent = folder / "_PROVISIONAL_UPLOADED"
        if sent.exists() and sent.read_text(encoding="utf-8").strip() == version:
            continue                              # this version is already in Databricks
        saved = rows_on_disk(folder)
        if saved is None or saved[1]:
            continue                              # unreadable or duplicated: not a complete version
        found.append((folder, version, saved[0]))
    return sorted(found, key=lambda x: x[0].parent.name, reverse=True)


def with_retries(action, what: str):
    """Run an upload step, retrying with growing waits if the network hiccups."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return action()
        except Exception as err:   # network errors, Databricks errors
            if attempt == MAX_RETRIES:
                raise
            wait = 5 * attempt
            print(f"      {what} failed ({type(err).__name__}: {str(err)[:100]}), retrying in {wait}s")
            time.sleep(wait)


def send_files(w, files: list, remote_dir: str) -> None:
    """Upload files in order, then check every one arrived with the same size."""
    with_retries(lambda: w.files.create_directory(remote_dir), "create folder")
    for local_file in files:
        def send(f=local_file):
            with f.open("rb") as fh:
                w.files.upload(f"{remote_dir}/{f.name}", fh, overwrite=True)
        with_retries(send, f"upload {local_file.name}")
    remote = {e.name: e.file_size for e in w.files.list_directory_contents(remote_dir)}
    for local_file in files:
        size = local_file.stat().st_size
        if remote.get(local_file.name) != size:
            raise RuntimeError(f"{local_file.name}: laptop {size} bytes, Databricks {remote.get(local_file.name)}")


def upload_day(w, folder: Path, volume: str) -> None:
    """Upload one finished day and verify it. Raises if anything does not match."""
    remote_dir = f"{volume}/{folder.parent.name}/{folder.name}"   # .../arrival_date=X/commodity=ALL
    pages = sorted(list(folder.glob("page_*.json*")) + list(folder.glob("state_*.json*")))
    if not pages:
        raise RuntimeError("no data files in a folder marked _DONE")
    # Pages first, _DONE last: if the upload stops halfway, the day doesn't look finished.
    send_files(w, pages + [folder / "_DONE"], remote_dir)


def upload_provisional(w, folder: Path, version: str, expected_rows: int, volume: str) -> None:
    """Upload one complete version of a recent day. Raises if the day changed meanwhile."""
    before = snapshot(folder)
    remote_dir = f"{volume}/{folder.parent.name}/pulled_at={version}/expected_rows={expected_rows}"
    send_files(w, sorted(folder.glob("state_*.json.gz")), remote_dir)
    if snapshot(folder) != before:
        raise RuntimeError("the day was re-downloaded during the upload; it will be sent next run")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--volume", default=DEFAULT_VOLUME, help="folder for final days")
    parser.add_argument("--provisional-volume", default=DEFAULT_PROVISIONAL_VOLUME, help="folder for recent days")
    parser.add_argument("--limit", type=int, help="upload at most this many final days")
    parser.add_argument("--no-provisional", action="store_true", help="upload final days only")
    parser.add_argument("--dry-run", action="store_true", help="only list what would be uploaded")
    args, _ = parser.parse_known_args()

    final = finished_not_uploaded(LOCAL_ROOT)
    if args.limit:
        final = final[: args.limit]
    provisional = [] if args.no_provisional else provisional_candidates(LOCAL_ROOT)
    size_mb = sum(f.stat().st_size for d in final for f in d.iterdir() if f.is_file()) / 1e6
    print(f"{len(final)} final day(s) to upload ({size_mb:.1f} MB), "
          f"{len(provisional)} recent day(s) with a new complete version\n")
    if args.dry_run or not (final or provisional):
        for d in final[:10]:
            print("  final:      ", d.parent.name)
        if len(final) > 10:
            print(f"  ... and {len(final) - 10} more final days")
        for d, version, rows in provisional:
            print(f"  provisional: {d.parent.name}  version {version}, {rows:,} rows")
        return

    lock = acquire_lock(LOCAL_ROOT / "_uploader.lock")   # kept open until the script ends
    if lock is None:
        # Not an error: the scheduled upload is already doing this work.
        print("Another upload is already running. This run has stopped without changing anything.")
        return
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        sys.exit("databricks-sdk is not installed. Run:  python -m pip install databricks-sdk")
    w = WorkspaceClient()   # reads host and token from .databrickscfg, like the CLI

    started, failed = time.time(), []
    for i, folder in enumerate(final, 1):
        day = folder.parent.name.replace("arrival_date=", "")
        try:
            upload_day(w, folder, args.volume)
        except Exception as err:
            failed.append(day)
            print(f"{day}  FAILED, will retry next run ({type(err).__name__}: {str(err)[:120]})")
            continue
        (folder / "_UPLOADED").write_text(datetime.now().isoformat(), encoding="utf-8")
        left = (len(final) - i) * (time.time() - started) / i
        print(f"{day}  final, uploaded and verified   [{i}/{len(final)}, about {left / 60:.0f} min left]")

    for folder, version, rows in provisional:
        day = folder.parent.name.replace("arrival_date=", "")
        try:
            upload_provisional(w, folder, version, rows, args.provisional_volume)
        except Exception as err:
            failed.append(day)
            print(f"{day}  provisional FAILED, will retry next run ({type(err).__name__}: {str(err)[:120]})")
            continue
        (folder / "_PROVISIONAL_UPLOADED").write_text(version, encoding="utf-8")
        print(f"{day}  provisional version {version} ({rows:,} rows), uploaded and verified")

    print(f"\nUploaded and verified: {len(final) + len(provisional) - len(failed)}. Failed: {len(failed)}.")
    if failed:
        print("They will be retried on the next run:", ", ".join(failed[:10]))
        sys.exit(1)


if __name__ == "__main__":
    main()
