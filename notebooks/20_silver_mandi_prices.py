# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: clean, typed, trusted mandi prices
# MAGIC
# MAGIC Turns bronze's raw text into data you can analyse, under rules we control.
# MAGIC Every rule here comes from profiling bronze (see `docs/data_quality.md`).
# MAGIC
# MAGIC | Step | What | Why |
# MAGIC | --- | --- | --- |
# MAGIC | Final or provisional | Each day comes from its final data if it exists, otherwise from its latest **complete** provisional version | Recent days still receive late reports; they are shown, marked `data_status = 'provisional'` |
# MAGIC | Types | Dates parsed as day/month/year; prices as decimals | Bronze keeps text; `01/10/2026` is ambiguous unless the format is stated |
# MAGIC | Standard names | One name per state, via a reference table | The source uses "Kerala" and "Keralam", "Pondicherry", "Chattisgarh" |
# MAGIC | Quality rules | Rows breaking a rule go to quarantine, with the reason | Bad rows are kept and visible, never silently dropped. Includes prices under Rs 1, which are not real in any unit |
# MAGIC | Unit flag | A market's whole monthly series for a crop more than 10x off the typical market | Some markets fill "Rs per quintal" per piece, bunch or kg (Pune leafy vegetables per bunch, Mapusa coconut per nut). Fine for that market's own trend, not for comparing markets |
# MAGIC | Outlier flag | One price more than 10x off its own market's usual price that month | A one-off entry error; kept, but flagged so gold leaves it out |
# MAGIC | Copies after cleaning | Rows identical once spaces are trimmed are kept once | Some Maharashtra and Telangana markets are published twice, as "APMC Kolhapur" and "APMC Kolhapur " (see data_quality.md) |
# MAGIC | Grain | One row per price **report** | Some markets genuinely report several prices a day; gold summarises per market-day |
# MAGIC | Reconciliation | rows used from bronze = silver + quarantine + copies removed, for every month | Every row used is accounted for |
# MAGIC
# MAGIC ## Incremental by month
# MAGIC A run rebuilds only the **months whose bronze data changed**, not the whole history:
# MAGIC
# MAGIC 1. **What changed?** Bronze's Change Data Feed lists every row added or removed since the version
# MAGIC    this notebook last processed (kept in `ops.pipeline_state`). Their days give the changed months.
# MAGIC 2. **Why months, not days?** The unit and outlier flags compare a price with its market's
# MAGIC    **monthly** median, so one new day can change every flag in its month. The month is the
# MAGIC    smallest unit that can be recomputed on its own and still give the same answer as a full rebuild.
# MAGIC 3. **Replace atomically.** Each changed month is swapped in one Delta transaction (`replaceWhere`):
# MAGIC    readers never see half a month, and other months are not touched.
# MAGIC 4. **Full rebuild when it must:** the first run, `mode = full`, a change to any rule or to the state
# MAGIC    mapping (detected automatically by a fingerprint of the rules), or a change feed that can't be read.
# MAGIC
# MAGIC Every run is logged in `ops.pipeline_runs` (mode, months, rows read, seconds), so the saving is measured, not claimed.

# COMMAND ----------

import os, sys, time
sys.path.append(os.getcwd())          # the notebook's folder, where incremental.py lives
import incremental as inc

STARTED = time.time()
dbutils.widgets.text("catalog", "mandi_dev")
dbutils.widgets.dropdown("mode", "auto", ["auto", "full"])
C = dbutils.widgets.get("catalog")
MODE = dbutils.widgets.get("mode")

FINAL = f"{C}.bronze.mandi_prices_raw"
PROV = f"{C}.bronze.mandi_prices_provisional_raw"
SILVER = f"{C}.silver.mandi_prices"
QUARANTINE = f"{C}.silver.mandi_prices_quarantine"
LEDGER = f"{C}.ops.silver_month_ledger"
inc.ensure_ops_tables(spark, C)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reference table: one standard name per state
# MAGIC A mapping table, not a CASE statement in code: when the source introduces a new spelling,
# MAGIC you add a row; no code changes. The original name is kept in silver as `state_raw`.
# MAGIC Changing this table changes every month, so it is part of the rules fingerprint.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {C}.silver.ref_state_alias (
  state_raw STRING COMMENT 'State name exactly as the source sends it',
  state     STRING COMMENT 'Standard name used in silver and gold'
) COMMENT 'Maps every spelling of a state in the source to one standard name'
""")
spark.sql(f"""
INSERT INTO {C}.silver.ref_state_alias VALUES
  ('Kerala', 'Kerala'), ('Keralam', 'Kerala'),
  ('Pondicherry', 'Puducherry'), ('Puducherry', 'Puducherry'),
  ('Chattisgarh', 'Chhattisgarh'), ('Chhattisgarh', 'Chhattisgarh'),
  ('NCT of Delhi', 'Delhi'), ('Delhi', 'Delhi'),
  ('Orissa', 'Odisha'), ('Odisha', 'Odisha'),
  ('Jammu and Kashmir', 'Jammu and Kashmir'), ('Jammu & Kashmir', 'Jammu and Kashmir'),
  ('Andaman and Nicobar', 'Andaman and Nicobar Islands'), ('Andaman and Nicobar Islands', 'Andaman and Nicobar Islands'),
  ('Andhra Pradesh', 'Andhra Pradesh'), ('Arunachal Pradesh', 'Arunachal Pradesh'), ('Assam', 'Assam'),
  ('Bihar', 'Bihar'), ('Chandigarh', 'Chandigarh'), ('Goa', 'Goa'), ('Gujarat', 'Gujarat'),
  ('Haryana', 'Haryana'), ('Himachal Pradesh', 'Himachal Pradesh'), ('Jharkhand', 'Jharkhand'),
  ('Karnataka', 'Karnataka'), ('Ladakh', 'Ladakh'), ('Madhya Pradesh', 'Madhya Pradesh'),
  ('Maharashtra', 'Maharashtra'), ('Manipur', 'Manipur'), ('Meghalaya', 'Meghalaya'),
  ('Mizoram', 'Mizoram'), ('Nagaland', 'Nagaland'), ('Punjab', 'Punjab'), ('Rajasthan', 'Rajasthan'),
  ('Sikkim', 'Sikkim'), ('Tamil Nadu', 'Tamil Nadu'), ('Telangana', 'Telangana'), ('Tripura', 'Tripura'),
  ('Uttar Pradesh', 'Uttar Pradesh'), ('Uttarakhand', 'Uttarakhand'), ('West Bengal', 'West Bengal')
""")
ALIASES = sorted(tuple(r) for r in spark.table(f"{C}.silver.ref_state_alias").collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ### The rules, as SQL templates
# MAGIC `{scope}` limits a run to the months being rebuilt (`true` for a full rebuild).
# MAGIC The templates are hashed into the **rules fingerprint**: edit any rule and the next run rebuilds
# MAGIC every month, so old and new months never follow different rules.
# MAGIC
# MAGIC **Which bronze rows to use:** final days, plus the latest complete version of each recent day.
# MAGIC A provisional version is **complete** when all `expected_rows` (from its folder name) have loaded;
# MAGIC a partly uploaded version is ignored until the laptop finishes it. A day that is final never
# MAGIC uses provisional data.
# MAGIC
# MAGIC **Typing:** `try_cast` / `try_to_date` return NULL instead of failing on a bad value, so one bad row
# MAGIC can't stop the whole run; the NULL is then caught by a rule and the row quarantined with its reason.

# COMMAND ----------

COLS = ["Arrival_Date", "Commodity", "Commodity_Code", "District", "Grade", "Market", "Max_Price",
        "Min_Price", "Modal_Price", "State", "Variety", "record_json", "folder_arrival_date",
        "source_file", "ingested_at"]

IN_SCOPE_SQL = """
WITH final_days AS (
  SELECT DISTINCT to_date(folder_arrival_date) AS day FROM {final} WHERE {scope}
),
versions AS (
  SELECT to_date(folder_arrival_date) AS day, pulled_at,
         COUNT(*) AS rows_loaded, MAX(expected_rows) AS expected_rows
  FROM {prov} WHERE {scope}
  GROUP BY ALL
),
chosen AS (                                   -- latest complete version of each non-final day
  SELECT day, max(pulled_at) AS pulled_at
  FROM versions
  WHERE rows_loaded = expected_rows
    AND day NOT IN (SELECT day FROM final_days)
  GROUP BY day
)
SELECT {cols}, 'final' AS data_status, CAST(NULL AS STRING) AS pulled_at
FROM {final} WHERE {scope}
UNION ALL
SELECT {p_cols}, 'provisional' AS data_status, p.pulled_at
FROM {prov} p
JOIN chosen c ON to_date(p.folder_arrival_date) = c.day AND p.pulled_at = c.pulled_at
"""

CANDIDATES_SQL = """
WITH typed AS (
  SELECT
    try_to_date(b.Arrival_Date, 'dd/MM/yyyy')     AS arrival_date,      -- explicit format: 01/10/2026 = 1 Oct
    to_date(b.folder_arrival_date)                AS folder_date,
    trim(b.State)                                 AS state_raw,
    coalesce(a.state, trim(b.State))              AS state,
    trim(b.District)                              AS district,
    trim(b.Market)                                AS market,
    trim(b.Commodity)                             AS commodity,
    try_cast(b.Commodity_Code AS INT)             AS commodity_code,
    trim(b.Variety)                               AS variety,
    trim(b.Grade)                                 AS grade,
    try_cast(b.Min_Price   AS DECIMAL(12,2))      AS min_price,        -- Rs per quintal
    try_cast(b.Max_Price   AS DECIMAL(12,2))      AS max_price,
    try_cast(b.Modal_Price AS DECIMAL(12,2))      AS modal_price,
    b.record_json, b.source_file, b.ingested_at, b.data_status, b.pulled_at
  FROM bronze_in_scope b
  LEFT JOIN {C}.silver.ref_state_alias a ON trim(b.State) = a.state_raw
),
series AS (                                        -- one market's price level for one crop in one month
  SELECT state, market, commodity, date_trunc('MONTH', arrival_date) AS month,
         percentile_approx(modal_price, 0.5)       AS series_median,
         COUNT(*)                                  AS series_reports
  FROM typed
  WHERE modal_price > 0 AND arrival_date IS NOT NULL
  GROUP BY ALL
),
crop_level AS (                                    -- the typical market's level: each market counts once
  SELECT commodity, month,
         percentile_approx(series_median, 0.5)     AS crop_median,
         COUNT(*)                                  AS crop_markets
  FROM series
  GROUP BY ALL
)
SELECT t.*,
  concat_ws('; ',
    CASE WHEN t.arrival_date IS NULL                          THEN 'date not dd/MM/yyyy' END,
    CASE WHEN t.arrival_date <> t.folder_date                 THEN 'date differs from its folder' END,
    CASE WHEN t.min_price IS NULL OR t.max_price IS NULL
              OR t.modal_price IS NULL                         THEN 'price missing or not a number' END,
    CASE WHEN t.min_price <= 0 OR t.max_price <= 0
              OR t.modal_price <= 0                            THEN 'price zero or negative' END,
    -- Under Rs 1 is not a real price in any unit (per quintal, kg, piece or bunch),
    -- e.g. Patti APMC tomato at Rs 0.08. Per-bunch prices like Rs 8 still pass.
    CASE WHEN t.modal_price > 0 AND t.modal_price < 1          THEN 'price under Rs 1: not plausible in any unit' END,
    CASE WHEN t.min_price > t.modal_price
              OR t.modal_price > t.max_price                   THEN 'min <= modal <= max broken' END,
    CASE WHEN t.state IS NULL OR t.market IS NULL OR t.market = ''
              OR t.commodity IS NULL                           THEN 'key field missing' END
  )                                                            AS quality_issues,   -- '' means the row passed
  -- How far this market's level is from the typical market's (1 = typical).
  round(CAST(s.series_median AS DOUBLE) / CAST(c.crop_median AS DOUBLE), 4) AS level_ratio,
  -- Whole series 10x off the typical market: likely priced per piece, bunch or kg, or a
  -- different product. Needs 3+ markets: with 2 we can't tell which one is right.
  coalesce(c.crop_markets >= 3
           AND (s.series_median > 10 * c.crop_median
                OR s.series_median < c.crop_median / 10), false) AS is_unit_suspect,
  -- One price 10x off its own market's usual price that month: a one-off error.
  -- Needs 5+ reports so the market's usual price is meaningful.
  coalesce(s.series_reports >= 5
           AND (t.modal_price > 10 * s.series_median
                OR t.modal_price < s.series_median / 10), false) AS is_price_outlier
FROM typed t
LEFT JOIN series s
  ON  t.state = s.state AND t.market = s.market AND t.commodity = s.commodity
  AND date_trunc('MONTH', t.arrival_date) = s.month
LEFT JOIN crop_level c
  ON t.commodity = c.commodity AND date_trunc('MONTH', t.arrival_date) = c.month
"""

# Names are already trimmed in silver_candidates, so a report published under both "APMC Kolhapur"
# and "APMC Kolhapur " counts once.
SILVER_SQL = """
SELECT report_id, arrival_date, state, state_raw, district, market, commodity, commodity_code,
       variety, grade, min_price, max_price, modal_price,
       is_price_outlier, is_unit_suspect, level_ratio, data_status, pulled_at, source_file, ingested_at
FROM (
  SELECT *,
    sha2(concat_ws('|', arrival_date, state_raw, district, market, commodity, variety, grade,
                   min_price, max_price, modal_price), 256)            AS report_id,   -- stable row key
    ROW_NUMBER() OVER (
      PARTITION BY arrival_date, state_raw, district, market, commodity, variety, grade,
                   min_price, max_price, modal_price
      ORDER BY ingested_at DESC, source_file)                          AS copy_no
  FROM silver_candidates
  WHERE quality_issues = ''
)
WHERE copy_no = 1
"""

QUARANTINE_SQL = """
SELECT quality_issues, folder_date, arrival_date, state_raw, district, market, commodity, variety, grade,
       min_price, max_price, modal_price, data_status, record_json, source_file, ingested_at
FROM silver_candidates
WHERE quality_issues <> ''
"""

RULES = inc.fingerprint(IN_SCOPE_SQL, CANDIDATES_SQL, SILVER_SQL, QUARANTINE_SQL, ALIASES)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full, incremental or nothing to do?

# COMMAND ----------

v_final, v_prov = inc.table_version(spark, FINAL), inc.table_version(spark, PROV)
state = inc.get_state(spark, C)
tables_ready = (all(inc.table_exists(spark, t) for t in [SILVER, QUARANTINE, LEDGER])
                and "folder_date" in spark.table(QUARANTINE).columns)
months = []

if MODE == "full":
    run_mode, reason = "full", "full rebuild requested"
elif not tables_ready or not {"silver.bronze_final_version", "silver.bronze_prov_version",
                              "silver.rules"} <= state.keys():
    run_mode, reason = "full", "first run: nothing processed yet"
elif state["silver.rules"] != RULES:
    run_mode, reason = "full", "rules or state mapping changed: every month must follow the new rules"
else:
    try:
        days = (inc.changed_days(spark, FINAL, int(state["silver.bronze_final_version"]), v_final)
                | inc.changed_days(spark, PROV, int(state["silver.bronze_prov_version"]), v_prov))
        months = inc.months_of(days)
        run_mode = "incremental" if months else "skip"
        reason = (f"{len(days)} changed day(s) in bronze" if months else "no change in bronze")
    except Exception as err:
        run_mode, reason = "full", f"change feed not readable ({type(err).__name__}): rebuilding to be safe"

ranges = inc.month_ranges(months)
FULL = run_mode == "full"
print(f"Mode: {run_mode} ({reason}). Months: {', '.join(str(m) for m in months) or ('all' if FULL else 'none')}")

# Scope predicates: which rows this run reads and replaces.
bronze_scope = "true" if FULL else inc.range_predicate("folder_arrival_date", ranges, text=True)
silver_scope = "true" if FULL else inc.range_predicate("arrival_date", ranges)
quarantine_scope = "true" if FULL else inc.range_predicate("folder_date", ranges)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean the months in scope

# COMMAND ----------

def build_views(scope):
    spark.sql("CREATE OR REPLACE TEMP VIEW bronze_in_scope AS " + IN_SCOPE_SQL.format(
        final=FINAL, prov=PROV, scope=scope, cols=", ".join(COLS),
        p_cols=", ".join("p." + c for c in COLS)))
    spark.sql("CREATE OR REPLACE TEMP VIEW silver_candidates AS " + CANDIDATES_SQL.format(C=C))

if run_mode != "skip":
    build_views(bronze_scope)
    display(spark.sql("""
    SELECT data_status, COUNT(DISTINCT folder_arrival_date) AS days, COUNT(*) AS rows,
           MIN(folder_arrival_date) AS first_day, MAX(folder_arrival_date) AS last_day
    FROM bronze_in_scope GROUP BY data_status
    """))

    # Any state in bronze that the mapping doesn't know? Silver keeps its raw name; list them to add.
    unmapped = spark.sql(f"""
        SELECT DISTINCT trim(b.State) AS state_raw
        FROM bronze_in_scope b
        LEFT ANTI JOIN {C}.silver.ref_state_alias a ON trim(b.State) = a.state_raw
    """)
    if unmapped.count():
        print("State names not in the mapping yet (add them to ref_state_alias):")
        display(unmapped)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write silver and quarantine: whole table (full) or only the changed months (incremental)
# MAGIC **Liquid clustering** on date and commodity: queries filtering by them, including this notebook's
# MAGIC own month replacement, read only the files that hold those values.

# COMMAND ----------

rows_written = 0
if run_mode != "skip":
    silver_df, quarantine_df = spark.sql(SILVER_SQL), spark.sql(QUARANTINE_SQL)
    if FULL:
        rows_written = inc.create_or_replace(
            spark, silver_df, SILVER, cluster_by="arrival_date, commodity",
            comment="One row per mandi price report: typed, standard state names, quality-checked. Rs per quintal.")
        inc.create_or_replace(
            spark, quarantine_df, QUARANTINE,
            comment="Rows that failed a silver quality rule, with the reason. Kept for review, never silently dropped.")
    else:
        rows_written = inc.replace_ranges(spark, silver_df, SILVER, "arrival_date", ranges)
        inc.replace_ranges(spark, quarantine_df, QUARANTINE, "folder_date", ranges)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reconcile, month by month, and over the whole history
# MAGIC `ops.silver_month_ledger` holds one row per month: bronze rows used, silver rows, quarantined rows,
# MAGIC copies removed. Each run replaces the ledger rows of the months it rebuilt. Two checks:
# MAGIC 1. **Each month rebuilt:** bronze = silver + quarantine + copies removed.
# MAGIC 2. **All history:** the ledger's totals equal the row counts of the silver and quarantine tables and
# MAGIC    of the bronze rows in use, so months not touched by this run are still accounted for.

# COMMAND ----------

LEDGER_SQL = f"""
WITH b AS (SELECT date_trunc('MONTH', to_date(folder_arrival_date))::date AS month, COUNT(*) AS bronze_rows
           FROM bronze_in_scope GROUP BY 1),
     p AS (SELECT date_trunc('MONTH', folder_date)::date AS month, COUNT(*) AS passed
           FROM silver_candidates WHERE quality_issues = '' GROUP BY 1),
     s AS (SELECT date_trunc('MONTH', arrival_date)::date AS month, COUNT(*) AS silver_rows,
                  COUNT_IF(is_price_outlier) AS outlier_rows, COUNT_IF(is_unit_suspect) AS unit_suspect_rows,
                  COUNT_IF(data_status = 'provisional') AS provisional_rows
           FROM {SILVER} WHERE {silver_scope} GROUP BY 1),
     q AS (SELECT date_trunc('MONTH', folder_date)::date AS month, COUNT(*) AS quarantined_rows
           FROM {QUARANTINE} WHERE {quarantine_scope} GROUP BY 1)
SELECT b.month, b.bronze_rows,
       coalesce(s.silver_rows, 0)                          AS silver_rows,
       coalesce(q.quarantined_rows, 0)                     AS quarantined_rows,
       coalesce(p.passed, 0) - coalesce(s.silver_rows, 0)  AS copies_removed,
       coalesce(s.outlier_rows, 0)                         AS outlier_rows,
       coalesce(s.unit_suspect_rows, 0)                    AS unit_suspect_rows,
       coalesce(s.provisional_rows, 0)                     AS provisional_rows,
       current_timestamp()                                 AS updated_at
FROM b LEFT JOIN p USING (month) LEFT JOIN s USING (month) LEFT JOIN q USING (month)
"""

rows_read = 0
if run_mode != "skip":
    ledger_df = spark.sql(LEDGER_SQL)
    if FULL:
        inc.create_or_replace(spark, ledger_df, LEDGER,
                              comment="One row per month: bronze rows used, silver, quarantined, copies removed")
    else:
        inc.replace_ranges(spark, ledger_df, LEDGER, "month", ranges)

    # 1. Each month rebuilt balances.
    this_run = spark.table(LEDGER).where("true" if FULL else inc.range_predicate("month", ranges))
    bad = this_run.where("bronze_rows <> silver_rows + quarantined_rows + copies_removed OR copies_removed < 0")
    if bad.count():
        display(bad)
        raise Exception("Reconciliation failed for the month(s) above")
    rows_read = this_run.agg({"bronze_rows": "sum"}).first()[0] or 0

    # 2. The whole history balances.
    build_views("true")                                   # bronze rows in use, all months
    totals = spark.sql(f"""
    SELECT
      (SELECT COUNT(*) FROM bronze_in_scope)     AS bronze_rows,
      (SELECT COUNT(*) FROM {SILVER})            AS silver_rows,
      (SELECT COUNT(*) FROM {QUARANTINE})        AS quarantined_rows,
      (SELECT SUM(bronze_rows) FROM {LEDGER})    AS ledger_bronze,
      (SELECT SUM(silver_rows) FROM {LEDGER})    AS ledger_silver,
      (SELECT SUM(quarantined_rows) FROM {LEDGER}) AS ledger_quarantined
    """).first()
    print(totals.asDict())
    if (totals.bronze_rows, totals.silver_rows, totals.quarantined_rows) != \
       (totals.ledger_bronze, totals.ledger_silver, totals.ledger_quarantined):
        raise Exception("Reconciliation failed: the month ledger and the tables disagree")
    print(f"Reconciliation passed: all {totals.bronze_rows:,} bronze rows in use are accounted for, month by month.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log the run
# MAGIC `ops.silver_quality_log` keeps one row per run with whole-history totals (the dashboard's Data health
# MAGIC page reads it). Its column `exact_copies_removed` keeps its name so earlier runs stay comparable;
# MAGIC it counts copies after cleaning. `ops.pipeline_runs` records what the run processed and how long it took.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {C}.ops.silver_quality_log (
  run_at TIMESTAMP, bronze_rows BIGINT, silver_rows BIGINT, quarantined_rows BIGINT,
  exact_copies_removed BIGINT, outlier_rows BIGINT
) COMMENT 'One row per silver run: how many rows passed, failed, were de-duplicated or flagged'
""")
# Columns added later: older runs keep NULL there (they didn't measure it), new runs fill it.
existing = spark.table(f"{C}.ops.silver_quality_log").columns
for col, typ in [("unit_suspect_rows", "BIGINT"), ("provisional_rows", "BIGINT"),
                 ("run_mode", "STRING"), ("months_processed", "INT")]:
    if col not in existing:
        spark.sql(f"ALTER TABLE {C}.ops.silver_quality_log ADD COLUMNS ({col} {typ})")

if run_mode != "skip":
    spark.sql(f"""
    INSERT INTO {C}.ops.silver_quality_log
      (run_at, bronze_rows, silver_rows, quarantined_rows, exact_copies_removed, outlier_rows,
       unit_suspect_rows, provisional_rows, run_mode, months_processed)
    SELECT current_timestamp(), SUM(bronze_rows), SUM(silver_rows), SUM(quarantined_rows),
           SUM(copies_removed), SUM(outlier_rows), SUM(unit_suspect_rows), SUM(provisional_rows),
           '{run_mode}', {len(months) if not FULL else 'COUNT(*)'}
    FROM {LEDGER}
    """)

SUMMARY = inc.log_run(spark, C, "silver", run_mode, reason, months, rows_read, rows_written, STARTED)
# Remember what was processed, only after everything above succeeded.
inc.set_state(spark, C, {"silver.bronze_final_version": v_final,
                         "silver.bronze_prov_version": v_prov,
                         "silver.rules": RULES})

# COMMAND ----------

# MAGIC %md
# MAGIC ### For review: what failed, units, outliers (months processed by this run)

# COMMAND ----------

if run_mode != "skip":
    display(spark.sql(f"""
    SELECT issue, COUNT(*) AS rows
    FROM (SELECT explode(split(quality_issues, '; ')) AS issue      -- a row can fail several rules
          FROM {QUARANTINE} WHERE {quarantine_scope})
    GROUP BY issue
    ORDER BY rows DESC
    """))

    # Whole market-crop series far from the typical market. A ratio near 0.005 usually means priced
    # per piece or bunch; near 0.01, per kg. Usable for that market's own trend, not for comparing markets.
    display(spark.sql(f"""
    SELECT state, market, commodity,
           COUNT(*)                    AS rows,
           COUNT(DISTINCT arrival_date) AS days,
           percentile_approx(modal_price, 0.5) AS median_modal,
           round(avg(level_ratio), 4)  AS level_ratio
    FROM {SILVER}
    WHERE is_unit_suspect AND {silver_scope}
    GROUP BY ALL
    ORDER BY rows DESC
    LIMIT 30
    """))

    # Prices more than 10x away from their own market's usual price that month.
    display(spark.sql(f"""
    SELECT arrival_date, state, market, commodity, variety, modal_price, level_ratio
    FROM {SILVER}
    WHERE is_price_outlier AND {silver_scope}
    ORDER BY modal_price DESC
    LIMIT 20
    """))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task result
# MAGIC The one-line summary becomes this task's result, shown on its page in the job run,
# MAGIC so a run's mode, months, rows and time are visible without opening the output.

# COMMAND ----------

dbutils.notebook.exit(SUMMARY)
