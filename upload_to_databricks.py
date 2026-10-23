"""
Upload finished days of raw mandi prices from this laptop to the Databricks volume.

Why this script exists
  The data must be downloaded on this laptop (data.gov.in refuses cloud servers),
  then copied into Databricks. Copying the whole folder each time would also send
  half-downloaded days and re-send everything every night. This script uploads only
  days that are finished and not yet uploaded, and checks each one arrived.

How it works, for each day folder (newest first)
  1. Skip it unless it has _DONE (fully downloaded) and lacks _UPLOADED (not yet sent).
  2. Upload every page file, then _DONE last, so a half-uploaded day never looks finished.
  3. List the folder in Databricks and compare file names and sizes with the laptop.
  4. Only if they match, write _UPLOADED on the laptop. If anything fails, the day is
     left unmarked and the next run simply tries it again (safe to re-run).

Setup (once): the Databricks CLI must already be configured (databricks configure),
because this script reads the same connection file, C:\\Users\\<you>\\.databrickscfg.
    python -m pip install databricks-sdk

Run:
    python upload_to_databricks.py --dry-run      # list what would be uploaded, send nothing
    python upload_to_databricks.py                # upload all finished, not-yet-uploaded days
    python upload_to_databricks.py --limit 5      # only the next 5 days (a quick test)
"""
import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path

LOCAL_ROOT = Path("data/raw/mandi_v2")      # version 2: downloaded state by state
DEFAULT_VOLUME = "/Volumes/mandi_dev/bronze/raw_files/mandi_v2"   # kept apart from version 1
MAX_RETRIES = 3


def acquire_lock(path: Path):
    """Only one upload at a time. The operating system releases the lock automatically
    if this script stops or crashes, so a leftover lock file never blocks a later run."""
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
        sys.exit("Another upload is already running. This run has stopped without changing anything.")
    return fh


def finished_not_uploaded(root: Path):
    """Day/commodity folders that are fully downloaded but not yet uploaded, newest first."""
    folders = [p.parent for p in root.glob("arrival_date=*/commodity=*/_DONE")]
    pending = [f for f in folders if not (f / "_UPLOADED").exists()]
    return sorted(pending, key=lambda f: f.parent.name, reverse=True)


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


def upload_day(w, folder: Path, volume: str) -> None:
    """Upload one day folder and verify it. Raises if anything does not match."""
    remote_dir = f"{volume}/{folder.parent.name}/{folder.name}"   # .../arrival_date=X/commodity=ALL
    pages = sorted(list(folder.glob("page_*.json*")) + list(folder.glob("state_*.json*")))
    if not pages:
        raise RuntimeError("no data files in a folder marked _DONE")

    with_retries(lambda: w.files.create_directory(remote_dir), "create folder")
    # Pages first, _DONE last: if the upload stops halfway, the day doesn't look finished.
    for local_file in pages + [folder / "_DONE"]:
        def send(f=local_file):
            with f.open("rb") as fh:
                w.files.upload(f"{remote_dir}/{f.name}", fh, overwrite=True)
        with_retries(send, f"upload {local_file.name}")

    # Verify: every local file is in Databricks with the same size.
    remote = {e.name: e.file_size for e in w.files.list_directory_contents(remote_dir)}
    for local_file in pages + [folder / "_DONE"]:
        size = local_file.stat().st_size
        if remote.get(local_file.name) != size:
            raise RuntimeError(f"{local_file.name}: laptop {size} bytes, Databricks {remote.get(local_file.name)}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--volume", default=DEFAULT_VOLUME, help="target folder in the Databricks volume")
    parser.add_argument("--limit", type=int, help="upload at most this many days")
    parser.add_argument("--dry-run", action="store_true", help="only list what would be uploaded")
    args, _ = parser.parse_known_args()

    pending = finished_not_uploaded(LOCAL_ROOT)
    if args.limit:
        pending = pending[: args.limit]
    size_mb = sum(f.stat().st_size for d in pending for f in d.iterdir() if f.is_file()) / 1e6
    print(f"{len(pending)} finished day(s) to upload, {size_mb:.1f} MB, to {args.volume}\n")
    if not pending or args.dry_run:
        for d in pending[:10]:
            print("  would upload:", d.parent.name)
        if len(pending) > 10:
            print(f"  ... and {len(pending) - 10} more")
        return

    lock = acquire_lock(LOCAL_ROOT / "_uploader.lock")   # kept open until the script ends
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        sys.exit("databricks-sdk is not installed. Run:  python -m pip install databricks-sdk")
    w = WorkspaceClient()   # reads host and token from .databrickscfg, like the CLI

    started, done, failed = time.time(), 0, []
    for i, folder in enumerate(pending, 1):
        day = folder.parent.name.replace("arrival_date=", "")
        try:
            upload_day(w, folder, args.volume)
        except Exception as err:
            failed.append(day)
            print(f"{day}  FAILED, will retry next run ({type(err).__name__}: {str(err)[:120]})")
            continue
        (folder / "_UPLOADED").write_text(datetime.now().isoformat(), encoding="utf-8")
        done += 1
        per_day = (time.time() - started) / i
        left = (len(pending) - i) * per_day
        print(f"{day}  uploaded and verified   [{i}/{len(pending)}, about {left / 60:.0f} min left]")

    print(f"\nUploaded and verified: {done} day(s). Failed: {len(failed)}.")
    if failed:
        print("Run the same command again to retry:", ", ".join(failed[:10]))


if __name__ == "__main__":
    main()
