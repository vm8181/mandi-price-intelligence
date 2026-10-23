"""
Profile the downloaded mandi price archive: what's in it and how clean it is.

Reads every CSV file in a folder (default: data/raw/kaggle_mandi_2023_2025)
and prints, for each file and for all files together:
  - rows and columns
  - date range and how many days are covered (and which days are missing)
  - states, districts, markets, commodities
  - blank values per column
  - rows breaking the price rule  min <= modal <= max, or with zero prices
  - duplicate rows on market + commodity + variety + grade + date

Run:
    python profile_archive.py
    python profile_archive.py --folder "data/raw/some_other_folder"
"""
import argparse
import re
import sys
from pathlib import Path

import pandas as pd


def clean_name(name: str) -> str:
    """'District Name' -> 'district_name', 'Min_price' -> 'min_price'."""
    return re.sub(r"[^0-9a-zA-Z]+", "_", str(name)).strip("_").lower()


def find_col(df: pd.DataFrame, *candidates: str):
    """Return the first column whose cleaned name matches one of the candidates."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def parse_dates(series: pd.Series) -> pd.Series:
    """Dates in these files may be '06-06-2023', '06/06/2023' or '2023-06-06'.
    Try day-first formats, then ISO; keep whichever parses the most rows."""
    best = None
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%b-%Y", "%d %b %Y"):
        parsed = pd.to_datetime(series.astype(str).str.strip(), format=fmt, errors="coerce")
        if best is None or parsed.notna().sum() > best.notna().sum():
            best = parsed
    return best


def profile(df: pd.DataFrame, title: str) -> None:
    print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")
    print(f"Rows: {len(df):,}    Columns: {len(df.columns)}")
    print("Columns:", ", ".join(df.columns))

    state = find_col(df, "state")
    district = find_col(df, "district_name", "district")
    market = find_col(df, "market_name", "market")
    commodity = find_col(df, "commodity")
    variety = find_col(df, "variety")
    grade = find_col(df, "grade")
    date_col = find_col(df, "price_date", "arrival_date", "date")
    pmin = find_col(df, "min_price")
    pmax = find_col(df, "max_price")
    pmodal = find_col(df, "modal_price")

    for label, col in (("States", state), ("Districts", district), ("Markets", market),
                       ("Commodities", commodity)):
        if col:
            print(f"{label:<12} {df[col].nunique():,}")
    if commodity:
        print("\nRows per commodity:")
        print(df[commodity].value_counts().to_string())

    if date_col:
        dates = parse_dates(df[date_col])
        bad = dates.isna().sum()
        print(f"\nDates: {dates.min():%d %b %Y} to {dates.max():%d %b %Y}, "
              f"{dates.dt.normalize().nunique():,} distinct days")
        if bad:
            print(f"Rows whose date is blank or in a different format: {bad:,}  "
                  f"e.g. {df.loc[dates.isna(), date_col].head(3).tolist()}")
        all_days = pd.date_range(dates.min(), dates.max(), freq="D")
        missing = all_days.difference(dates.dt.normalize().dropna().unique())
        print(f"Calendar days with no rows at all: {len(missing):,}"
              + (f"  e.g. {[d.strftime('%d %b %Y') for d in missing[:5]]}" if len(missing) else ""))
        rows_per_day = dates.dt.normalize().value_counts()
        print(f"Rows per day: median {rows_per_day.median():,.0f}, "
              f"lowest {rows_per_day.min():,}, highest {rows_per_day.max():,}")

    blanks = df.isna().sum()
    blanks = blanks[blanks > 0]
    print("\nBlank values per column:", "none" if blanks.empty else "")
    if not blanks.empty:
        print(blanks.to_string())

    if pmin and pmax and pmodal:
        lo, hi, mid = (pd.to_numeric(df[c], errors="coerce") for c in (pmin, pmax, pmodal))
        not_numeric = (lo.isna() | hi.isna() | mid.isna()).sum()
        broken = ((lo > mid) | (mid > hi)).sum()
        zero = ((lo <= 0) | (mid <= 0) | (hi <= 0)).sum()
        print(f"\nPrice checks (Rs per quintal):")
        print(f"  not a number:            {not_numeric:,}")
        print(f"  min > modal or modal > max: {broken:,}")
        print(f"  zero or negative:        {zero:,}")
        print(f"  modal price range:       {mid.min():,.0f} to {mid.max():,.0f}, median {mid.median():,.0f}")

    key = [c for c in (state, district, market, commodity, variety, grade, date_col) if c]
    if key:
        dupes = df.duplicated(subset=key).sum()
        print(f"\nDuplicate rows on {', '.join(key)}: {dupes:,}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--folder", default="data/raw/kaggle_mandi_2023_2025")
    args, _ = parser.parse_known_args()

    folder = Path(args.folder)
    files = sorted(folder.rglob("*.csv"))
    if not files:
        sys.exit(f"No CSV files found in {folder.resolve()}\n"
                 "Unzip the Kaggle download into that folder first, or pass --folder.")

    frames = []
    for f in files:
        df = pd.read_csv(f, low_memory=False)
        df.columns = [clean_name(c) for c in df.columns]
        profile(df, f"File: {f.name}")
        df["source_file"] = f.name
        frames.append(df)

    if len(frames) > 1:
        profile(pd.concat(frames, ignore_index=True).drop(columns="source_file"),
                f"ALL {len(files)} FILES TOGETHER")


if __name__ == "__main__":
    main()
