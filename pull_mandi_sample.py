"""
Pull a sample of variety-wise daily mandi prices from data.gov.in and save it as CSV.

Dataset: "Variety-wise Daily Market Prices Data of Commodity"
         (Department of Agriculture and Farmers Welfare, from AGMARKNET)

Setup (once):
    pip install requests pandas
    # Put your data.gov.in API key in an environment variable, never in the code:
    #   Windows (PowerShell):  $env:DATAGOV_API_KEY = "your-key"
    #   Mac / Linux:           export DATAGOV_API_KEY="your-key"
    #   Colab:                 store it in Secrets, then copy into os.environ

Run:
    python pull_mandi_sample.py                    # 500 rows, no filters
    python pull_mandi_sample.py --limit 1000
    python pull_mandi_sample.py --filter "State=Maharashtra" --filter "Commodity=Onion"

The first run prints the dataset's exact field names. Use those names
in --filter (they may differ in capitals or spelling from the examples above).
"""
import argparse
import os
import re
import sys
from datetime import datetime

import pandas as pd
import requests

# Two data.gov.in datasets carry the same AGMARKNET mandi prices:
#   history: "Variety-wise Daily Market Prices Data of Commodity" - years of data
#   today:   "Current Daily Price of Various Commodities from Various Markets" - today only
RESOURCES = {
    "history": "35985678-0d79-46b4-9ed6-6f13308a1d24",
    "today": "9ef84268-d588-465a-a308-a864a43d0070",
}

# Settings used when you paste this code straight into a notebook cell
# (where command-line options can't be given). Edit these two lines there.
DEFAULT_LIMIT = 500
DEFAULT_FILTERS = []   # e.g. ["State=Maharashtra", "Commodity=Onion"]

# Public sample key published on the data.gov.in API page, for testing only.
# Used automatically when DATAGOV_API_KEY isn't set. Your own key never goes here.
SAMPLE_KEY = "579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b"

# data.gov.in does not answer requests labelled "python-requests" (Python's default
# User-Agent): they time out, while the same request with a browser-style label answers
# in about 2 seconds (diagnosed 22 Oct 2026). This label uses the accepted format and
# says honestly which project is calling.
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; mandi-price-intelligence/1.0)"}


def clean_name(name: str) -> str:
    """Turn API field names like 'Modal_x0020_Price' into 'modal_price'."""
    name = name.replace("_x0020_", "_")          # encoded spaces
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name)   # other symbols -> _
    return name.strip("_").lower()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="rows to pull")
    parser.add_argument("--filter", action="append", default=None,
                        help='field filter, e.g. --filter "State=Maharashtra" (repeatable)')
    parser.add_argument("--dataset", choices=list(RESOURCES), default="history",
                        help="history (default) or today")
    parser.add_argument("--timeout", type=int, default=60, help="seconds to wait for an answer")
    # parse_known_args ignores the extra arguments a notebook adds behind the scenes
    # (Colab and Jupyter pass "-f <kernel file>"), so the code also runs pasted into a cell.
    args, _ = parser.parse_known_args()
    if args.filter is None:
        args.filter = list(DEFAULT_FILTERS)

    api_key = os.environ.get("DATAGOV_API_KEY")
    if not api_key:
        # No personal key set: use the public sample key that data.gov.in prints on its
        # API page for testing. It returns at most 10 rows per request.
        api_key = SAMPLE_KEY
        args.limit = min(args.limit, 10)
        print("No DATAGOV_API_KEY set: using data.gov.in's public sample key (10 rows max).\n")

    params = {"api-key": api_key, "format": "json", "limit": args.limit, "offset": 0}
    for f in args.filter:
        if "=" not in f:
            sys.exit(f'Filter "{f}" must look like Field=Value')
        field, value = f.split("=", 1)
        params[f"filters[{field.strip()}]"] = value.strip()

    base_url = f"https://api.data.gov.in/resource/{RESOURCES[args.dataset]}"
    print(f"Asking data.gov.in ({args.dataset} dataset), waiting up to {args.timeout}s ...", flush=True)
    started = datetime.now()
    try:
        resp = requests.get(base_url, params=params, headers=HEADERS, timeout=args.timeout)
    except requests.Timeout:
        sys.exit(f"No answer within {args.timeout}s: the data.gov.in server is too slow right now. "
                 "Nothing is wrong on your side; try again later.")
    except requests.ConnectionError as err:
        # The error text can contain the full request URL, which includes the API key.
        safe = re.sub(r"(api-key=)[^&\s'\"]+", r"\1***", str(err))
        sys.exit(f"Could not connect to data.gov.in: {safe[:300]}")
    print(f"Answered in {(datetime.now() - started).total_seconds():.1f}s\n")
    if resp.status_code == 429:
        wait = resp.headers.get("Retry-After")
        sys.exit("HTTP 429: too many requests with this key; the API is rate-limiting it. "
                 + (f"The server asks to wait {wait} seconds. " if wait else "Wait a few minutes. ")
                 + ("You are on the shared sample key; set DATAGOV_API_KEY to your own key."
                    if params["api-key"] == SAMPLE_KEY else ""))
    if resp.status_code != 200:
        sys.exit(f"Request failed: HTTP {resp.status_code}\n{resp.text[:500]}")

    try:
        body = resp.json()
    except ValueError:
        sys.exit(f"The API did not return JSON. First part of the reply:\n{resp.text[:500]}")
    if body.get("status") not in (None, "ok"):
        sys.exit(f"The API reported a problem: status={body.get('status')} message={body.get('message')}")
    records = body.get("records", [])

    print(f"Dataset:        {body.get('title')}")
    print(f"Last updated:   {body.get('updated_date')}")
    print(f"Total rows:     {body.get('total')}")
    print(f"Rows pulled:    {len(records)}")

    # The API describes its own columns. These exact names are what --filter needs.
    fields = body.get("field", [])
    if fields:
        print("\nFields (use the 'id' in --filter):")
        for f in fields:
            print(f"  id={f.get('id'):<28} name={f.get('name'):<28} type={f.get('type')}")

    if not records:
        sys.exit("\nNo rows returned. Try without filters, or check the field names above.")

    df = pd.DataFrame(records)
    df.columns = [clean_name(c) for c in df.columns]
    for col in ("min_price", "max_price", "modal_price"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    out = f"mandi_sample_{datetime.now():%Y%m%d_%H%M}.csv"
    df.to_csv(out, index=False)
    print(f"\nSaved to {out}")

    print("\nFirst rows:")
    print(df.head(10).to_string(index=False))

    # Quick profile: the shape of the data and the problems the pipeline must handle.
    print("\n--- Profile ---")
    for col in ("state", "district", "market", "commodity", "variety", "grade"):
        if col in df.columns:
            print(f"Distinct {col:<10} values: {df[col].nunique()}")
    date_col = next((c for c in df.columns if "date" in c), None)
    if date_col:
        # Dates arrive as text like "21/02/2023". Text sorts like words, not dates,
        # so convert to real dates before finding the earliest and latest.
        dates = pd.to_datetime(df[date_col], format="%d/%m/%Y", errors="coerce")
        print(f"Dates in sample:  {dates.min():%d %b %Y} to {dates.max():%d %b %Y} "
              f"({dates.nunique()} distinct)")
        if dates.isna().any():
            print(f"Dates not in dd/mm/yyyy format: {dates.isna().sum()} rows")

    # Did the API apply our filters? Some APIs ignore a filter they don't understand
    # and quietly return unfiltered rows, so check every row against each filter.
    for f in args.filter:
        field, value = (s.strip() for s in f.split("=", 1))
        col = clean_name(field)
        if col not in df.columns:
            continue
        if col == date_col:
            wanted = pd.to_datetime(value, dayfirst=True, errors="coerce")
            matches = (dates == wanted)
        else:
            matches = df[col].astype(str).str.lower() == value.lower()
        status = "applied" if matches.all() else f"NOT applied ({(~matches).sum()} rows don't match)"
        print(f"Filter {field}={value}: {status}")
    if {"min_price", "modal_price", "max_price"} <= set(df.columns):
        bad = df[(df.min_price > df.modal_price) | (df.modal_price > df.max_price)
                 | (df.modal_price <= 0) | df.modal_price.isna()]
        print(f"Rows breaking min <= modal <= max, or zero/blank price: {len(bad)}")
    key_cols = [c for c in ("state", "district", "market", "commodity", "variety", "grade") if c in df.columns]
    if date_col:
        key_cols.append(date_col)
    if key_cols:
        print(f"Duplicate rows on {', '.join(key_cols)}: {df.duplicated(subset=key_cols).sum()}")


if __name__ == "__main__":
    main()
