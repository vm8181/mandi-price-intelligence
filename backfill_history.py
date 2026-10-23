"""
Backfill historical mandi prices from data.gov.in, one day and one commodity at a time.

Dataset: "Variety-wise Daily Market Prices Data of Commodity" (from AGMARKNET)

What it does
  - For every date in a range and every chosen commodity, asks the API for that
    day's rows one state at a time (no paging; see download_day), checks the states
    add up to the day's total, and saves each state's answer exactly as received (raw JSON,
    gzip-compressed to save about 90% of disk space; Databricks reads .json.gz directly).
  - Files land in a date/commodity folder layout that later becomes the bronze layer:
        data/raw/mandi_v2/arrival_date=2026-10-20/commodity=ALL/state_Tamil_Nadu.json.gz
  - Safe to stop and restart: a day/commodity that already finished is skipped
    (a "_DONE" marker file records completion), so nothing is downloaded twice.
  - Every request is logged to data/raw/mandi/_manifest.csv (date, commodity,
    rows expected, rows saved, status) so you can check completeness.
  - Never pulls today: the end date is capped at yesterday (T-1), because today's
    prices are still arriving.
  - Works newest day first, so the most useful recent data lands first.

Two modes, same script:
    $env:DATAGOV_API_KEY = "your-key"          # Windows PowerShell, once per terminal

    1. Historical backfill, once: the last 10 years up to yesterday, every commodity
       (about 49M rows; about 1.2 GB as compressed pages instead of about 13 GB raw)
       python backfill_history.py --last-years 10 --all-commodities

    2. Daily update, every day: the last 7 days up to yesterday. Recent days are
       re-pulled each time to catch late uploads; older days are already finished.
       python backfill_history.py --last-days 7 --all-commodities

Other examples
    python backfill_history.py --start 01-10-2026 --end 07-10-2026 --all-commodities --count-only
    python backfill_history.py --start 01-10-2026 --all-commodities        (end defaults to yesterday)

Options
    --last-years 10                        start N years before yesterday (instead of --start)
    --last-days 7                          only the last N days up to yesterday (daily update)
    --commodity Onion --commodity Tomato   choose commodities (default: STAPLES below)
    --all-commodities                      filter by date only, every commodity at once
    --oldest-first                         work forwards in time instead of newest first
    --timeout 180                          wait longer for a slow server (default 45 seconds)
    --page-size 50000                      row limit per request (above any state's daily rows)
    --pause 0.5                            seconds to wait between requests
"""
import argparse
import csv
import gzip
import json
import os
import re
import socket
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

RESOURCE_ID = "35985678-0d79-46b4-9ed6-6f13308a1d24"
BASE_URL = f"https://api.data.gov.in/resource/{RESOURCE_ID}"
SAMPLE_KEY = "579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b"  # public test key

# data.gov.in does not answer requests labelled "python-requests" (Python's default
# User-Agent): they time out, while the same request with a browser-style label answers
# in about 2 seconds (diagnosed 22 Oct 2026). This label uses the accepted format and
# says honestly which project is calling.
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; mandi-price-intelligence/1.0)"}

# Starting set of staples. Names must match the API exactly; run --count-only
# first - a commodity that shows 0 rows on every day is probably misspelled.
STAPLES = ["Onion", "Tomato", "Potato", "Wheat", "Paddy(Common)"]

# Version 2 of the raw data: downloaded state by state (see download_day). Version 1
# (data/raw/mandi) used offset paging, which overlapped: some rows came twice and others
# were missed. See docs/incidents/2026-10-23-pagination-overlap.md.
OUT_DIR = Path("data/raw/mandi_v2")

# States as named in the API, largest first (from bronze, 2 Mar to 15 Oct 2026).
# Kerala appears under two names over this period.
STATES = ["Tamil Nadu", "Uttar Pradesh", "Madhya Pradesh", "Maharashtra", "Keralam",
          "Karnataka", "Punjab", "Gujarat", "Haryana", "Rajasthan", "Kerala",
          "Himachal Pradesh", "West Bengal", "Telangana", "Odisha", "Chattisgarh",
          "Tripura", "Uttarakhand", "Andhra Pradesh", "NCT of Delhi", "Jammu and Kashmir",
          "Assam", "Bihar", "Manipur", "Andaman and Nicobar", "Meghalaya", "Goa",
          "Nagaland", "Chandigarh", "Pondicherry", "Arunachal Pradesh", "Mizoram"]
# Other spellings that may appear in older years. Only tried when the states above
# don't add up to the day's total, so they cost nothing on normal days.
EXTRA_STATE_NAMES = ["Chhattisgarh", "Puducherry", "Jharkhand", "Sikkim", "Ladakh",
                     "Delhi", "Orissa", "Jammu & Kashmir", "Andaman and Nicobar Islands",
                     "Dadra and Nagar Haveli", "Daman and Diu",
                     "Dadra and Nagar Haveli and Daman and Diu", "Lakshadweep"]
MANIFEST = OUT_DIR / "_manifest.csv"
MAX_RETRIES = 4
# Mandis often upload prices days late, so the most recent days are never marked
# finished: every run re-pulls them fresh until they are older than this window.
LATE_WINDOW_DAYS = 7


def parse_day(text: str) -> date:
    """Accept dd-mm-yyyy, the format the API's date filter uses."""
    return datetime.strptime(text, "%d-%m-%Y").date()


REQUEST_TIMEOUT = 45        # seconds of silence from the server before giving up
MAX_REQUEST_SECONDS = 180   # total time allowed for one page, however slowly it trickles in
NETWORK_WAIT_MINUTES = 30   # how long to wait for the internet to come back before stopping


class ApiFailed(Exception):
    """The API kept failing for one request, even after all retries."""


def redact(text: str) -> str:
    """Blank out the API key wherever it appears, e.g. inside an error's URL.
    Error messages can contain the full request URL, which includes the key."""
    return re.sub(r"(api-key=)[^&\s'\"]+", r"\1***", str(text))


def describe(err: Exception) -> str:
    """A short, plain reason for a failure, with any API key removed."""
    text = str(err)
    if "getaddrinfo" in text or "NameResolution" in text or "Failed to resolve" in text:
        return "cannot look up api.data.gov.in: internet or DNS is down"
    if isinstance(err, requests.exceptions.ChunkedEncodingError) or "IncompleteRead" in text:
        return "connection dropped in the middle of the download"
    if isinstance(err, requests.ConnectTimeout):
        return "could not connect to the server in time"
    if isinstance(err, requests.Timeout):
        return str(err) if "longer than" in text else f"no data from the server for {REQUEST_TIMEOUT}s"
    if isinstance(err, requests.ConnectionError):
        return "connection to the server failed"
    return redact(text)[:160]


def network_is_down(err: Exception) -> bool:
    """True when the failure is our own internet connection, not the server."""
    text = str(err)
    return "getaddrinfo" in text or "NameResolution" in text or "Failed to resolve" in text


def wait_for_network() -> None:
    """Pause until api.data.gov.in can be looked up again, checking every 30 seconds.
    Stops the whole run if the internet stays down, rather than failing day after day."""
    deadline = time.time() + NETWORK_WAIT_MINUTES * 60
    print(f"    internet looks down; checking every 30s for up to {NETWORK_WAIT_MINUTES} min", flush=True)
    while time.time() < deadline:
        time.sleep(30)
        try:
            socket.getaddrinfo("api.data.gov.in", 443)
            print("    internet is back, continuing", flush=True)
            return
        except OSError:
            print("    still offline ...", flush=True)
    sys.exit(f"Internet was down for {NETWORK_WAIT_MINUTES} minutes. Stopped. "
             "Run the same command again when you're back online; finished days are skipped.")


def fetch(params: dict) -> requests.Response:
    """GET one page, enforcing a total time limit as well as the silence timeout.
    A server that sends data very slowly could otherwise hold one request for many minutes."""
    started = time.time()
    resp = requests.get(BASE_URL, params=params, headers=HEADERS,
                        timeout=(15, REQUEST_TIMEOUT), stream=True)
    chunks = []
    for chunk in resp.iter_content(chunk_size=65536):
        chunks.append(chunk)
        if time.time() - started > MAX_REQUEST_SECONDS:
            resp.close()
            raise requests.Timeout(f"page took longer than {MAX_REQUEST_SECONDS}s to download")
    resp._content = b"".join(chunks)          # make resp.json() / resp.text work as usual
    return resp


def call_api(params: dict, label: str) -> dict:
    """One API request, retried with growing waits if the network or server fails."""
    attempt, network_waits = 0, 0
    while attempt < MAX_RETRIES:
        attempt += 1
        print(f"    asking API: {label} (attempt {attempt}) ...", end=" ", flush=True)
        started = time.time()
        try:
            resp = fetch(params)
            if resp.status_code == 200:
                body = resp.json()
                if body.get("status") in (None, "ok"):
                    print(f"ok in {time.time() - started:.1f}s", flush=True)
                    return body
                raise RuntimeError(f"API status={body.get('status')} message={body.get('message')}")
            if resp.status_code in (401, 403):
                sys.exit(f"HTTP {resp.status_code}: the API key was refused. Check DATAGOV_API_KEY.")
            if resp.status_code == 429:
                # Rate limit: wait as long as the server asks, or a minute per attempt.
                retry_after = resp.headers.get("Retry-After", "")
                wait = int(retry_after) if retry_after.isdigit() else 60 * attempt
                print(f"rate-limited (HTTP 429), waiting {wait}s", flush=True)
                if attempt == MAX_RETRIES:
                    raise ApiFailed("HTTP 429: rate limit still in force after all retries")
                time.sleep(wait)
                continue
            raise RuntimeError(f"HTTP {resp.status_code}: {redact(resp.text[:200])}")
        except (requests.RequestException, ValueError, RuntimeError) as err:
            print(f"failed after {time.time() - started:.1f}s ({describe(err)})", flush=True)
            if network_is_down(err) and network_waits < 10:
                # Our own connection is down: wait for it, and don't count this attempt.
                network_waits += 1
                wait_for_network()
                attempt -= 1
                continue
            if attempt == MAX_RETRIES:
                raise ApiFailed(describe(err))
            wait = 5 * 2 ** (attempt - 1)          # 5s, 10s, 20s
            print(f"    waiting {wait}s before retrying", flush=True)
            time.sleep(wait)
    raise ApiFailed("all retries used")


def acquire_lock(path: Path):
    """Make sure only one download runs at a time.

    Two downloads at once (say the nightly scheduled task and a manual run) could both
    delete and rewrite the same day's pages. The lock is held by the operating system,
    so it is released automatically if this script stops or crashes; a stale lock file
    left on disk does not block the next run."""
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
        sys.exit("Another download is already running (for example the scheduled task). "
                 "Let it finish; this run has stopped without changing anything.")
    return fh


def download_day(base: dict, folder: Path, label: str, limit: int, pause: float):
    """Download one day, one state per request, with no paging.

    Why: paging by offset relies on the server returning rows in the same order every
    time. data.gov.in doesn't (rows that tie on its sort order get reshuffled), so pages
    overlapped: some rows came twice and others were missed, while the count still
    matched. One request per state, with a limit no state reaches, has no page
    boundaries to overlap. The states must add up to the day's own total, which catches
    a state name we don't know.

    Returns (day_total, rows_saved, files_written)."""
    day_total = int(call_api({**base, "limit": 1, "offset": 0}, f"{label} day total").get("total") or 0)
    saved, files = 0, 0
    for names in (STATES, EXTRA_STATE_NAMES):
        for state in names:
            if saved >= day_total:
                break                        # everything is accounted for: the rest must be empty
            body = call_api({**base, "filters[State]": state, "limit": limit, "offset": 0},
                            f"{label} {state}")
            records = body.get("records", [])
            state_total = int(body.get("total") or 0)
            if len(records) < state_total:
                raise ApiFailed(f"{state}: got {len(records):,} of {state_total:,} rows in one request; "
                                f"raise --page-size above {state_total:,}")
            if records:
                slug = re.sub(r"[^A-Za-z0-9]+", "_", state).strip("_")
                # Save the state's answer exactly as received, gzip-compressed.
                with gzip.open(folder / f"state_{slug}.json.gz", "wt", encoding="utf-8") as fh:
                    json.dump(body, fh)
                saved += len(records)
                files += 1
            time.sleep(pause)
        if saved >= day_total:
            break
    return day_total, saved, files


def log(row: dict) -> None:
    """Append one line to the manifest, writing the header the first time."""
    new = not MANIFEST.exists()
    with MANIFEST.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(row))
        if new:
            writer.writeheader()
        writer.writerow(row)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", help="first date, dd-mm-yyyy")
    parser.add_argument("--end", help="last date, dd-mm-yyyy (default and maximum: yesterday)")
    parser.add_argument("--last-years", type=int, help="start this many years before the end date")
    parser.add_argument("--last-days", type=int,
                        help="daily update: the last N days up to yesterday, e.g. 7 to catch late uploads")
    parser.add_argument("--oldest-first", action="store_true", help="go forwards in time")
    parser.add_argument("--commodity", action="append", help="repeatable; default is the STAPLES list")
    parser.add_argument("--page-size", type=int, default=50000,
                        help="row limit per request; must exceed any single state's rows in a day")
    parser.add_argument("--pause", type=float, default=0.5)
    parser.add_argument("--count-only", action="store_true", help="only report row counts, save nothing")
    parser.add_argument("--all-commodities", action="store_true",
                        help="filter by date only and take every commodity (fewer, larger requests)")
    parser.add_argument("--timeout", type=int, default=REQUEST_TIMEOUT,
                        help=f"seconds to wait for one answer (default {REQUEST_TIMEOUT})")
    args, _ = parser.parse_known_args()   # ignore extra arguments notebooks add
    globals()["REQUEST_TIMEOUT"] = args.timeout   # call_api reads this setting

    # End date: yesterday (T-1) unless an earlier one is given. Today is never pulled,
    # because its prices are still arriving through the day.
    yesterday = date.today() - timedelta(days=1)
    end = parse_day(args.end) if args.end else yesterday
    if end > yesterday:
        print(f"End date capped at yesterday ({yesterday:%d-%m-%Y}): today's data is still arriving.\n")
        end = yesterday

    # Start date: given directly, or N years before the end date.
    if args.start:
        start = parse_day(args.start)
    elif args.last_days:
        start = end - timedelta(days=args.last_days - 1)
    elif args.last_years:
        try:
            start = end.replace(year=end.year - args.last_years) + timedelta(days=1)
        except ValueError:          # end is 29 Feb and the target year has no 29 Feb
            start = end.replace(year=end.year - args.last_years, day=28) + timedelta(days=1)
    else:
        sys.exit("Give --start dd-mm-yyyy, --last-years N or --last-days N.")
    if end < start:
        sys.exit("The end date is before the start date.")
    print(f"Range: {start:%d %b %Y} to {end:%d %b %Y}")
    # "ALL" means: no commodity filter, every commodity for that day.
    commodities = ["ALL"] if args.all_commodities else (args.commodity or STAPLES)

    api_key = os.environ.get("DATAGOV_API_KEY")
    if not api_key:
        if not args.count_only:
            sys.exit("Downloading needs your own key (the sample key returns only 10 rows).\n"
                     "Set DATAGOV_API_KEY, or add --count-only to estimate the size first.")
        api_key = SAMPLE_KEY
        print("No DATAGOV_API_KEY set: using the public sample key for counting.\n")

    days = [start + timedelta(d) for d in range((end - start).days + 1)]
    if not args.oldest_first:
        days.reverse()                  # newest first: the most useful data lands first
    print(f"{len(days)} days x {len(commodities)} commodities = {len(days) * len(commodities)} day-commodity pairs\n")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if not args.count_only:
        lock = acquire_lock(OUT_DIR / "_downloader.lock")   # kept open until the script ends

    grand_total, counted_days, failures = 0, set(), []

    # Progress and time-left estimate: count what still needs downloading, then time
    # each day as it finishes and project the average over the days that remain.
    def is_done(d, c):
        return (OUT_DIR / f"arrival_date={d.isoformat()}" / f"commodity={c}" / "_DONE").exists()
    pending_total = sum(1 for d in days for c in commodities if args.count_only or not is_done(d, c))
    if not args.count_only:
        print(f"{pending_total:,} still to download, {len(days) * len(commodities) - pending_total:,} already finished\n")
    processed, run_start = 0, time.time()

    def show_progress():
        elapsed = time.time() - run_start
        per_day = elapsed / processed
        left = (pending_total - processed) * per_day
        left_text = f"{left / 3600:.1f} h" if left >= 3600 else f"{left / 60:.0f} min"
        print(f"    progress: {processed:,} of {pending_total:,} done this run, "
              f"about {per_day:.0f}s a day, about {left_text} left", flush=True)

    for day in days:
        day_api = day.strftime("%d-%m-%Y")          # format the filter expects
        for commodity in commodities:
            folder = OUT_DIR / f"arrival_date={day.isoformat()}" / f"commodity={commodity}"
            done_marker = folder / "_DONE"
            if not args.count_only and done_marker.exists():
                continue        # finished earlier; counted in the summary printed at the start

            base = {"api-key": api_key, "format": "json", "filters[Arrival_Date]": day_api}
            if commodity != "ALL":
                base["filters[Commodity]"] = commodity
            label = f"{day_api} {commodity}"

            if args.count_only:
                try:
                    body = call_api({**base, "limit": 1, "offset": 0}, label)
                except ApiFailed as err:
                    failures.append(label)
                    print(f"{day_api}  {commodity:<16} FAILED, skipped ({err})")
                    continue
                total = int(body.get("total") or 0)
                grand_total += total
                counted_days.add(day)
                print(f"{day_api}  {commodity:<16} {total:>7,} rows")
                time.sleep(args.pause)
                continue

            # Download the day one state at a time (see download_day). Clear files from an
            # earlier, unfinished pull first, so old and new files never mix.
            folder.mkdir(parents=True, exist_ok=True)
            for old in list(folder.glob("page_*.json*")) + list(folder.glob("state_*.json*")):
                old.unlink()
            total, saved, page = 0, 0, 1        # page = files written + 1 (manifest column "pages")
            try:
                total, saved, files = download_day(base, folder, label, args.page_size, args.pause)
                page = files + 1
            except ApiFailed as err:
                # Keep going with the next pair; this one is left unfinished, so the
                # next run picks it up again automatically.
                failures.append(label)
                log({"arrival_date": day.isoformat(), "commodity": commodity, "rows_expected": total,
                     "rows_saved": saved, "pages": page - 1, "status": "FAILED",
                     "pulled_at": datetime.now().isoformat(timespec="seconds")})
                print(f"{day_api}  {commodity:<16} FAILED after {saved:,} rows, skipped ({err})")
                processed += 1
                show_progress()
                continue

            # A day stays "recent" while it is among the last LATE_WINDOW_DAYS - 1 days before
            # today. With a daily run of --last-days 7, each day is pulled on 7 consecutive
            # runs, and the 7th pull (when it is 7 days old) marks it finished.
            recent = day > date.today() - timedelta(days=LATE_WINDOW_DAYS)
            if saved != total:
                # States didn't add up to the day's total: a state name we don't know,
                # or rows added while we were downloading. Never marked finished.
                status = f"INCOMPLETE (states sum to {saved:,}, day total {total:,})"
            elif recent:
                status = "complete, will re-pull (recent day)"
            else:
                status = "complete"
                done_marker.write_text(datetime.now().isoformat(), encoding="utf-8")
            log({"arrival_date": day.isoformat(), "commodity": commodity, "rows_expected": total,
                 "rows_saved": saved, "pages": page - 1, "status": status,
                 "pulled_at": datetime.now().isoformat(timespec="seconds")})
            grand_total += saved
            print(f"{day_api}  {commodity:<16} {saved:>7,} of {total:,} rows  {status}")
            processed += 1
            show_progress()

    label = "rows available" if args.count_only else "rows saved"
    print(f"\nTotal {label}: {grand_total:,}")
    if args.count_only and grand_total and counted_days:
        per_day = grand_total / len(counted_days)
        print(f"About {per_day:,.0f} rows a day for these commodities, "
              f"about {per_day * 365:,.0f} a year.")
    if failures:
        print(f"\n{len(failures)} request(s) failed and were skipped: {', '.join(failures)}")
        print("Run the same command again later to retry them.")


if __name__ == "__main__":
    main()
