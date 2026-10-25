"""Proves that an incremental silver + gold run gives exactly the same tables as a full rebuild.

Runs the real notebook files on local Spark (no Databricks needed):
  1. bronze with Aug-Oct 2026 -> silver and gold, first run (full)
  2. new data arrives: three days turn final, a newer provisional version (one incomplete),
     and a backfilled June -> silver and gold, incremental (only the changed months)
  3. snapshot every table, then rebuild silver and gold in full from the same bronze
  4. compare row by row: the incremental result must equal the full rebuild
  5. a run with no bronze change must skip; a rules change must force a full rebuild

Delta-only features are stood in by incremental.py when the Spark conf mandi.local is true:
table versions and changed days come from Spark conf set here; replaceWhere becomes
"keep the other months + add the new ones".

Run:  python tests/local_incremental_test.py   (needs PySpark 4, pandas and Java 17+; about 2 minutes)
"""
import json
import os
import re
import shutil
import sys
from datetime import date, timedelta

from pyspark.sql import SparkSession

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NB = os.path.join(ROOT, "notebooks")
WH = "/tmp/mandi_local_wh"
shutil.rmtree(WH, ignore_errors=True)

base = (SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", WH)
        .config("spark.sql.ansi.enabled", "false")        # try_cast / bad dates give NULL, as on Databricks
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate())
base.sparkContext.setLogLevel("ERROR")
base.conf.set("mandi.local", "true")
C = "spark_catalog"
for s in ["bronze", "silver", "gold", "ops"]:
    base.sql(f"CREATE DATABASE IF NOT EXISTS {s}")


class LocalSpark:
    """Passes everything to Spark, translating the few Databricks-only SQL bits."""
    def __init__(self, s):
        self._s = s

    def sql(self, q, *a, **k):
        q = q.replace("try_to_date(", "to_date(")
        m = re.match(r"\s*CREATE OR REPLACE TABLE\s+(\S+)", q)
        if m:
            self._s.sql(f"DROP TABLE IF EXISTS {m.group(1)}")
            q = re.sub(r"CREATE OR REPLACE TABLE", "CREATE TABLE", q, count=1)
        return self._s.sql(q, *a, **k)

    def __getattr__(self, name):
        return getattr(self._s, name)


spark = LocalSpark(base)


class Widgets:
    def __init__(self):
        self.values = {}

    def text(self, name, default):
        self.values.setdefault(name, default)

    def dropdown(self, name, default, choices):
        self.values.setdefault(name, default)

    def get(self, name):
        return self.values[name]


class Notebook:
    def __init__(self):
        self.result = None

    def exit(self, value):           # on Databricks this ends the notebook with this result
        self.result = value


class DBUtils:
    def __init__(self):
        self.widgets = Widgets()
        self.notebook = Notebook()


def run_notebook(name, mode="auto"):
    dbutils = DBUtils()
    dbutils.widgets.values.update({"catalog": C, "mode": mode})
    code = open(os.path.join(NB, name), encoding="utf-8").read()
    cwd = os.getcwd()
    os.chdir(NB)
    try:
        exec(compile(code, name, "exec"),
             {"spark": spark, "dbutils": dbutils, "display": lambda df: None, "__name__": "__main__"})
    finally:
        os.chdir(cwd)
    assert dbutils.notebook.result and dbutils.notebook.result.startswith(("silver:", "gold:")), dbutils.notebook.result
    print("Task result:", dbutils.notebook.result)


# --------------------------------------------------------------------------- synthetic bronze
FIELDS = ["Arrival_Date", "Commodity", "Commodity_Code", "District", "Grade", "Market",
          "Max_Price", "Min_Price", "Modal_Price", "State", "Variety"]
MARKETS = [("Kerala", "Ernakulam", "Aluva"), ("Keralam", "Palakad", "Palakkad"),      # alias spelling
           ("Maharashtra", "Kolhapur", "APMC Kolhapur"), ("Maharashtra", "Pune", "Pune"),
           ("Karnataka", "Bangalore", "Binny Mill"), ("Odisha", "Dhenkanal", "Kamakhyanagar")]
CROPS = [("Onion", "23", 2500), ("Tomato", "78", 1800), ("Methi(Leaves)", "46", 3000)]


def rec(day, state, district, market, crop, code, mn, mx, modal, variety="Other", grade="FAQ", date_txt=None):
    return {"Arrival_Date": date_txt or day.strftime("%d/%m/%Y"), "Commodity": crop, "Commodity_Code": code,
            "District": district, "Grade": grade, "Market": market, "Max_Price": str(mx),
            "Min_Price": str(mn), "Modal_Price": str(modal), "State": state, "Variety": variety}


def day_records(day):
    out = []
    for i, (st, di, mk) in enumerate(MARKETS):
        for j, (crop, code, level) in enumerate(CROPS):
            modal = level + (day.day * 7 + i * 31 + j * 13) % 300
            spread = 100
            if mk == "Pune" and crop == "Methi(Leaves)":
                modal, spread = 12 + day.day % 5, 2            # priced per bunch: unit-suspect series
            out.append(rec(day, st, di, mk, crop, code, modal - spread, modal + spread, modal))
    if day.day == 10:                                         # one-off outlier (25x its usual price)
        out.append(rec(day, "Kerala", "Ernakulam", "Aluva", "Onion", "23", 60000, 70000, 65000, variety="Big"))
    if day.day % 4 == 0:                                      # published twice: trailing space after cleaning
        out.append(rec(day, "Maharashtra", "Kolhapur", "APMC Kolhapur ", "Onion", "23", 2400, 2600, 2500, variety="Red"))
        out.append(rec(day, "Maharashtra", "Kolhapur", "APMC Kolhapur", "Onion", "23", 2400, 2600, 2500, variety="Red"))
    if day.day == 5:                                          # quarantine: min > modal, under Rs 1, bad date
        out.append(rec(day, "Odisha", "Dhenkanal", "Kamakhyanagar", "Tomato", "78", 3000, 3500, 2000, variety="Hybrid"))
        out.append(rec(day, "Karnataka", "Bangalore", "Binny Mill", "Tomato", "78", 0.05, 0.1, 0.08, variety="Local"))
        out.append(rec(day, "Kerala", "Ernakulam", "Aluva", "Tomato", "78", 1000, 1200, 1100, date_txt="2026-13-45"))
    return out


def rows_for(day, recs, pulled_at=None, expected=None, ingested="2026-10-25 01:00:00"):
    rows = []
    for r in recs:
        row = dict(r)
        row.update({"record_json": json.dumps(r), "api_state_total": str(len(recs)),
                    "folder_arrival_date": day.isoformat(),
                    "source_file": f"/v/arrival_date={day}/{pulled_at or 'final'}/state_x.json.gz",
                    "source_file_modified_at": ingested, "ingested_at": ingested})
        if pulled_at is not None:
            row.update({"pulled_at": pulled_at, "expected_rows": expected})
        rows.append(row)
    return rows


FINAL_COLS = FIELDS + ["record_json", "api_state_total", "folder_arrival_date", "source_file",
                       "source_file_modified_at", "ingested_at"]
PROV_COLS = FIELDS + ["record_json", "api_state_total", "folder_arrival_date", "pulled_at", "expected_rows",
                      "source_file", "source_file_modified_at", "ingested_at"]


def to_df(rows, cols):
    import pandas as pd
    pdf = pd.DataFrame(rows, columns=cols)
    df = base.createDataFrame(pdf)
    for c in ["source_file_modified_at", "ingested_at"]:
        df = df.withColumn(c, df[c].cast("timestamp"))
    if "expected_rows" in cols:
        df = df.withColumn("expected_rows", df["expected_rows"].cast("int"))
    return df


def days(a, b):
    d = a
    while d <= b:
        yield d
        d += timedelta(days=1)


FINAL_T, PROV_T = f"{C}.bronze.mandi_prices_raw", f"{C}.bronze.mandi_prices_provisional_raw"


def write_bronze(final_rows, prov_rows):
    to_df(final_rows, FINAL_COLS).write.mode("overwrite").saveAsTable(FINAL_T)
    to_df(prov_rows, PROV_COLS).write.mode("overwrite").saveAsTable(PROV_T)


def set_changes(version, changed_final=(), changed_prov=()):
    base.conf.set(f"mandi.local.version.{FINAL_T}", str(version))
    base.conf.set(f"mandi.local.version.{PROV_T}", str(version))
    base.conf.set(f"mandi.local.changed.{FINAL_T}", ",".join(d.isoformat() for d in changed_final))
    base.conf.set(f"mandi.local.changed.{PROV_T}", ",".join(d.isoformat() for d in changed_prov))


# Step 1: Aug 1 - Oct 15 final; Oct 16-24 provisional (version A, complete)
final_rows = [r for d in days(date(2026, 8, 1), date(2026, 10, 15)) for r in rows_for(d, day_records(d))]
prov_rows = []
for d in days(date(2026, 10, 16), date(2026, 10, 24)):
    recs = day_records(d)
    prov_rows += rows_for(d, recs, pulled_at="20261025T010000", expected=len(recs))
write_bronze(final_rows, prov_rows)
set_changes(1)

TABLES = [f"{C}.silver.mandi_prices", f"{C}.silver.mandi_prices_quarantine", f"{C}.ops.silver_month_ledger",
          f"{C}.gold.dim_market", f"{C}.gold.dim_commodity", f"{C}.gold.fact_market_day_price",
          f"{C}.gold.agg_price_weekly", f"{C}.gold.agg_price_monthly"]


def runs():
    return [(r["layer"], r["mode"], r["months"]) for r in
            base.sql(f"SELECT layer, mode, months FROM {C}.ops.pipeline_runs ORDER BY finished_at, layer DESC").collect()]


print("\n=== Step 1: first run ===")
run_notebook("20_silver_mandi_prices.py")
run_notebook("30_gold_mandi_prices.py")
assert runs()[-2:] == [("silver", "full", ""), ("gold", "full", "")], runs()

# Step 2: new data. Oct 16-18 become final (their provisional rows are removed, as bronze's housekeeping
# does); a newer complete provisional version for Oct 19-25; an incomplete even newer version for Oct 25
# (must be ignored); and a backfilled June 10-12 (older history arriving later).
newly_final = list(days(date(2026, 10, 16), date(2026, 10, 18)))
backfill = list(days(date(2026, 6, 10), date(2026, 6, 12)))
final_rows += [r for d in newly_final + backfill for r in rows_for(d, day_records(d), ingested="2026-10-26 01:00:00")]
prov_rows = [r for r in prov_rows if date.fromisoformat(r["folder_arrival_date"]) not in newly_final]
for d in days(date(2026, 10, 19), date(2026, 10, 25)):
    recs = day_records(d)
    for r in recs:                                         # late reports raise prices a little
        r["Modal_Price"] = str(int(float(r["Modal_Price"])) + 5)
        r["Max_Price"] = str(int(float(r["Max_Price"])) + 5)
    prov_rows += rows_for(d, recs, pulled_at="20261026T010000", expected=len(recs), ingested="2026-10-26 01:00:00")
recs = day_records(date(2026, 10, 25))
prov_rows += rows_for(date(2026, 10, 25), recs[:5], pulled_at="20261026T090000", expected=len(recs),
                      ingested="2026-10-26 09:00:00")             # incomplete: 5 of n rows
write_bronze(final_rows, prov_rows)
set_changes(2, changed_final=newly_final + backfill,
            changed_prov=list(days(date(2026, 10, 16), date(2026, 10, 25))))

print("\n=== Step 2: incremental run ===")
run_notebook("20_silver_mandi_prices.py")
run_notebook("30_gold_mandi_prices.py")
assert runs()[-2:] == [("silver", "incremental", "2026-06-01,2026-10-01"),
                       ("gold", "incremental", "2026-06-01,2026-10-01")], runs()

# Snapshot the incremental result
snap = {t: base.table(t).drop("updated_at").collect() for t in TABLES}
schemas = {t: base.table(t).drop("updated_at").columns for t in TABLES}

print("\n=== Step 3: full rebuild from the same bronze ===")
run_notebook("20_silver_mandi_prices.py", mode="full")
run_notebook("30_gold_mandi_prices.py", mode="full")

print("\n=== Step 4: compare ===")
ok = True
for t in TABLES:
    inc_df = base.createDataFrame(snap[t], base.table(t).drop("updated_at").schema) if snap[t] else None
    full_df = base.table(t).drop("updated_at").select(*schemas[t])
    if inc_df is None:
        same = full_df.count() == 0
        extra = missing = 0
    else:
        inc_df = inc_df.select(*schemas[t])
        extra = inc_df.exceptAll(full_df).count()
        missing = full_df.exceptAll(inc_df).count()
        same = extra == 0 and missing == 0
    print(f"{'OK  ' if same else 'DIFF'} {t.split('.', 1)[1]:40s} rows {full_df.count():>6}  "
          f"only-in-incremental {extra}  only-in-full {missing}")
    ok &= same
assert ok, "incremental result differs from the full rebuild"

# Content sanity: the rules still did their job
sv = base.table(f"{C}.silver.mandi_prices")
assert sv.where("state_raw = 'Keralam' AND state <> 'Kerala'").count() == 0
assert sv.where("is_unit_suspect").select("market", "commodity").distinct().collect() == [("Pune", "Methi(Leaves)")]
assert sv.where("is_price_outlier").count() > 0
assert sv.where("arrival_date = DATE'2026-10-25'").where("data_status = 'provisional'").count() == len(CROPS) * len(MARKETS)
q = base.table(f"{C}.silver.mandi_prices_quarantine")
assert q.where("quality_issues LIKE '%under Rs 1%'").count() > 0 and q.where("quality_issues LIKE '%dd/MM/yyyy%'").count() > 0
print("Rules check passed: alias, unit flag, outliers, provisional version choice, quarantine reasons.")

print("\n=== Step 5: no change, then a rules change ===")
run_notebook("20_silver_mandi_prices.py")
run_notebook("30_gold_mandi_prices.py")
assert runs()[-2:] == [("silver", "skip", ""), ("gold", "skip", "")], runs()
state = {r["name"]: r["value"] for r in base.table(f"{C}.ops.pipeline_state").collect()}
state["silver.rules"] = "old-rules"
rows = ", ".join(f"('{k}', '{v}', current_timestamp())" for k, v in state.items())
base.sql(f"CREATE TABLE {C}.ops.pipeline_state__x USING parquet AS SELECT * FROM VALUES {rows} AS t(name, value, updated_at)")
base.sql(f"DROP TABLE {C}.ops.pipeline_state")
base.sql(f"ALTER TABLE {C}.ops.pipeline_state__x RENAME TO ops.pipeline_state")
run_notebook("20_silver_mandi_prices.py")
run_notebook("30_gold_mandi_prices.py")
assert runs()[-2:] == [("silver", "full", ""), ("gold", "full", "")], runs()
print(base.sql(f"SELECT layer, mode, reason, months, rows_read, rows_written FROM {C}.ops.pipeline_runs ORDER BY finished_at").toPandas().to_string())
print("\nALL CHECKS PASSED: incremental = full rebuild; skip and rules-change behave.")
