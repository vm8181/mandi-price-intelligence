"""Helpers for incremental runs of the silver and gold notebooks.

Why this exists
---------------
Silver and gold used to be rebuilt from all history on every run. That is simple, but the daily
cost grows with total history instead of with what changed. These helpers let a run:

1. find out what changed in bronze since the last run, using Delta's Change Data Feed (CDF);
2. turn changed days into changed months (silver's quality rules compare a price with its
   market's monthly median, so the month is the smallest safe unit to recompute);
3. replace only those months in a table, atomically (Delta `replaceWhere`);
4. remember what was processed (`ops.pipeline_state`) and log every run (`ops.pipeline_runs`),
   including how many rows it read and how long it took, so the saving can be measured.

Local test mode
---------------
The notebooks are tested on plain local Spark, where Delta features don't exist. When the Spark
conf `mandi.local` is "true", the three Delta-only operations (table version, change feed,
replaceWhere) use simple stand-ins. Everything else is the same code that runs on Databricks.
"""
import hashlib
import time
import uuid
from datetime import date, timedelta


# --------------------------------------------------------------------------- basics
def _local(spark):
    """True only inside the local test, which sets this Spark conf. Databricks serverless refuses to
    read a conf it doesn't know, even with a default (CONFIG_NOT_AVAILABLE), so a refusal means
    "not the local test"."""
    try:
        return spark.conf.get("mandi.local", "false") == "true"
    except Exception:
        return False


def fingerprint(*parts):
    """Short hash of the logic that decides a table's contents (SQL text, mapping rows).
    If it changes, old months were built with different rules, so the next run rebuilds everything."""
    return hashlib.sha256("\n".join(str(p) for p in parts).encode()).hexdigest()[:16]


def month_start(d):
    return date(d.year, d.month, 1)


def next_month(d):
    return date(d.year + (d.month == 12), d.month % 12 + 1, 1)


def week_start(d):
    return d - timedelta(days=d.weekday())          # Monday, as date_trunc('WEEK') in Spark


def months_of(days):
    return sorted({month_start(d) for d in days})


def month_ranges(months):
    """Months -> list of [start, end) date ranges, merging consecutive months: fewer, simpler predicates."""
    ranges = []
    for m in sorted(months):
        if ranges and ranges[-1][1] == m:
            ranges[-1][1] = next_month(m)
        else:
            ranges.append([m, next_month(m)])
    return [tuple(r) for r in ranges]


def week_ranges(months):
    """Every week touching the given months -> [start, end) ranges of week starts.
    A week that straddles two months (e.g. 29 Sep - 5 Oct) is recomputed when either month changes."""
    ranges = []
    for a, b in month_ranges(months):
        ws, we = week_start(a), week_start(b - timedelta(days=1)) + timedelta(days=7)
        if ranges and ranges[-1][1] >= ws:
            ranges[-1][1] = max(ranges[-1][1], we)
        else:
            ranges.append([ws, we])
    return [tuple(r) for r in ranges]


def range_predicate(col, ranges, text=False):
    """SQL predicate selecting the ranges, e.g. (arrival_date >= DATE'2026-09-01' AND arrival_date < DATE'2026-11-01').

    text=True compares a text column holding ISO dates ('2026-09-01') with text literals. Same rows, but
    the column is not wrapped in a function, so Delta can use each file's min/max statistics and skip
    files that hold no matching day (data skipping)."""
    if not ranges:
        return "false"
    q = (lambda d: f"'{d}'") if text else (lambda d: f"DATE'{d}'")
    return " OR ".join(f"({col} >= {q(a)} AND {col} < {q(b)})" for a, b in ranges)


# --------------------------------------------------------------------------- state and run log
def ensure_ops_tables(spark, C):
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {C}.ops.pipeline_state (
      name STRING COMMENT 'What is remembered, e.g. silver.bronze_final_version',
      value STRING, updated_at TIMESTAMP
    ) COMMENT 'What each layer has already processed, so the next run only does what is new'
    """)
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {C}.ops.pipeline_runs (
      run_id STRING, layer STRING,
      mode STRING COMMENT 'full, incremental or skip (nothing changed)',
      reason STRING COMMENT 'Why this mode was chosen',
      months STRING COMMENT 'Months processed, comma separated (empty for a full run)',
      months_processed INT, rows_read BIGINT, rows_written BIGINT,
      seconds DOUBLE, started_at TIMESTAMP, finished_at TIMESTAMP
    ) COMMENT 'One row per silver or gold run: what it processed and how long it took'
    """)


def get_state(spark, C):
    return {r["name"]: r["value"] for r in spark.table(f"{C}.ops.pipeline_state").collect()}


def set_state(spark, C, values):
    """Upsert name/value pairs. Small table: rewritten in full (works the same locally and on Delta)."""
    current = get_state(spark, C)
    current.update({k: str(v) for k, v in values.items()})
    rows = ", ".join(f"('{k}', '{v}', current_timestamp())" for k, v in sorted(current.items()))
    _overwrite_from_sql(spark, f"{C}.ops.pipeline_state",
                        f"SELECT * FROM VALUES {rows} AS t(name, value, updated_at)")


def log_run(spark, C, layer, mode, reason, months, rows_read, rows_written, started):
    run_id = str(uuid.uuid4())
    months_txt = ",".join(str(m) for m in months)
    seconds = round(time.time() - started, 1)
    reason = reason.replace("'", "")
    spark.sql(f"""
    INSERT INTO {C}.ops.pipeline_runs
    SELECT '{run_id}', '{layer}', '{mode}', '{reason}', '{months_txt}', {len(months)},
           {int(rows_read)}, {int(rows_written)}, {seconds},
           timestampadd(MILLISECOND, -{int(seconds * 1000)}, current_timestamp()), current_timestamp()
    """)
    print(f"{layer}: {mode} run ({reason}). Months: {months_txt or ('all' if mode == 'full' else 'none')}. "
          f"Rows read {int(rows_read):,}, written {int(rows_written):,}, in {seconds} s.")
    return run_id


# --------------------------------------------------------------------------- change detection
def table_version(spark, table):
    """Latest Delta version of a table (each write adds one)."""
    if _local(spark):
        return int(spark.conf.get(f"mandi.local.version.{table}", "0"))
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"]


def changed_days(spark, table, from_version, to_version):
    """Days (folder_arrival_date) inserted or deleted in a bronze table between two versions,
    read from the Change Data Feed. Raises if the feed is not available for that range."""
    if to_version <= from_version:
        return set()
    if _local(spark):
        txt = spark.conf.get(f"mandi.local.changed.{table}", "")
        return {date.fromisoformat(d) for d in txt.split(",") if d}
    rows = (spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", from_version + 1)
            .option("endingVersion", to_version)
            .table(table)
            .where("_change_type IN ('insert', 'delete')")
            .selectExpr("to_date(folder_arrival_date) AS day")
            .distinct()
            .collect())
    return {r["day"] for r in rows if r["day"] is not None}


# --------------------------------------------------------------------------- writing
def table_exists(spark, table):
    return spark.catalog.tableExists(table)


def _overwrite_from_sql(spark, table, select_sql):
    if _local(spark):
        tmp = table + "__tmp"
        spark.sql(f"DROP TABLE IF EXISTS {tmp}")
        spark.sql(f"CREATE TABLE {tmp} USING parquet AS {select_sql}")
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        two_part = ".".join(table.split(".")[-2:])     # local Spark renames with schema.table only
        spark.sql(f"ALTER TABLE {tmp} RENAME TO {two_part}")
    else:
        spark.sql(f"INSERT OVERWRITE {table} {select_sql}")


def replace_ranges(spark, df, table, col, ranges):
    """Atomically replace the rows of `table` whose `col` falls in `ranges` with `df`.

    On Delta this is one transaction (`replaceWhere`): readers see either the old months or the
    new ones, never a mix, and Delta refuses the write if `df` holds a row outside the ranges.
    Other months are not touched. Returns the number of rows written."""
    pred = range_predicate(col, ranges)
    if _local(spark):
        spark.table(table).where(f"NOT ({pred})").unionByName(df).createOrReplaceTempView("_kept_plus_new")
        _overwrite_from_sql(spark, table, "SELECT * FROM _kept_plus_new")
    else:
        df.write.format("delta").mode("overwrite").option("replaceWhere", pred).saveAsTable(table)
    # Counted from the table, not from df, so the (expensive) DataFrame is evaluated only once.
    return spark.table(table).where(pred).count()


def create_or_replace(spark, df, table, cluster_by=None, comment=None):
    """Full rebuild of a table from a DataFrame. Returns the number of rows written."""
    df.createOrReplaceTempView("_full_rebuild")
    if _local(spark):
        _overwrite_from_sql(spark, table, "SELECT * FROM _full_rebuild")
    else:
        spark.sql(f"""
        CREATE OR REPLACE TABLE {table}
        {f'CLUSTER BY ({cluster_by})' if cluster_by else ''}
        {f"COMMENT '{comment}'" if comment else ''}
        AS SELECT * FROM _full_rebuild
        """)
    return spark.table(table).count()
