# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: mandi prices for reporting
# MAGIC
# MAGIC A **star schema** built from `silver.mandi_prices`, for Power BI, dashboards and the public tracker.
# MAGIC
# MAGIC | Table | Grain (one row per...) | Notes |
# MAGIC | --- | --- | --- |
# MAGIC | `dim_market` | market (state, district, market) | Stable key: `xxhash64` of the names, the same on every rebuild |
# MAGIC | `dim_commodity` | crop | |
# MAGIC | `dim_date` | calendar day, 2016 to 2027 | Week (Monday start), month, quarter; days with no trade still exist |
# MAGIC | `fact_market_day_price` | day x market x crop x variety x grade | Lowest min, highest max, average modal, from non-outlier reports; `data_status` final or provisional |
# MAGIC | `agg_price_weekly`, `agg_price_monthly` | week/month x crop x state, plus "All India" | Median of market prices; unit-suspect series left out; `provisional_days` shows how much may still change |
# MAGIC
# MAGIC **How silver's flags are used**
# MAGIC - One-off **outliers** are left out of every price but counted in `outlier_reports`, so every silver row is accounted for.
# MAGIC - **Unit-suspect** series (priced per piece, bunch or kg) stay in the fact table for that market's own trend,
# MAGIC   and are left out of the weekly and monthly comparisons across markets.
# MAGIC
# MAGIC ## Incremental, following silver
# MAGIC Gold rebuilds only what silver rebuilt: it reads `ops.pipeline_runs` for the silver runs it hasn't
# MAGIC processed yet and takes their months.
# MAGIC - **Fact and monthly prices:** those months are replaced.
# MAGIC - **Weekly prices:** every week touching those months is replaced (a week like 29 Sep to 5 Oct belongs to two months).
# MAGIC - **Dimensions:** new markets and crops are added; existing keys never change, because they are hashes of the names.
# MAGIC - **Full rebuild** on the first run, when `mode = full`, when silver ran in full, or when gold's own logic changes (fingerprint).
# MAGIC
# MAGIC No arrival quantities exist in this dataset, so every market-day counts equally.

# COMMAND ----------

import os, sys, time
sys.path.append(os.getcwd())          # the notebook's folder, where incremental.py lives
import incremental as inc

STARTED = time.time()
dbutils.widgets.text("catalog", "mandi_dev")
dbutils.widgets.dropdown("mode", "auto", ["auto", "full"])
C = dbutils.widgets.get("catalog")
MODE = dbutils.widgets.get("mode")

SILVER = f"{C}.silver.mandi_prices"
DIM_MARKET, DIM_COMMODITY, DIM_DATE = f"{C}.gold.dim_market", f"{C}.gold.dim_commodity", f"{C}.gold.dim_date"
FACT = f"{C}.gold.fact_market_day_price"
AGG = {"weekly": f"{C}.gold.agg_price_weekly", "monthly": f"{C}.gold.agg_price_monthly"}
inc.ensure_ops_tables(spark, C)

# COMMAND ----------

# MAGIC %md
# MAGIC ### The logic, as SQL templates
# MAGIC `{scope}` limits a run to the months (or weeks) being rebuilt; `true` means everything.
# MAGIC The templates are hashed into gold's fingerprint: change one and the next run rebuilds gold in full.

# COMMAND ----------

DIM_MARKET_SQL = """
SELECT DISTINCT xxhash64(state, district, market) AS market_key, state, district, market
FROM {silver} WHERE {scope}
"""

DIM_COMMODITY_SQL = """
SELECT xxhash64(commodity)            AS commodity_key,
       commodity,
       min(commodity_code)            AS commodity_code,
       COUNT(DISTINCT commodity_code) AS codes_seen          -- should be 1; checked below
FROM {silver} WHERE {scope}
GROUP BY commodity
"""

# Several reports a day are combined: lowest minimum, highest maximum, average modal.
# Outlier reports are counted but left out of the prices.
FACT_SQL = """
SELECT
  arrival_date,
  xxhash64(state, district, market)                                  AS market_key,
  xxhash64(commodity)                                                AS commodity_key,
  variety,
  grade,
  COUNT_IF(NOT is_price_outlier)                                     AS report_count,
  COUNT_IF(is_price_outlier)                                         AS outlier_reports,
  min(CASE WHEN NOT is_price_outlier THEN min_price END)             AS min_price,
  max(CASE WHEN NOT is_price_outlier THEN max_price END)             AS max_price,
  round(avg(CASE WHEN NOT is_price_outlier THEN modal_price END), 2) AS modal_price,
  bool_or(is_unit_suspect)                                           AS is_unit_suspect,
  round(avg(level_ratio), 4)                                         AS level_ratio,
  max(data_status)                                                   AS data_status   -- one status per day
FROM {silver}
WHERE {scope}
GROUP BY arrival_date, state, district, market, commodity, variety, grade
"""

# GROUPING SETS computes the state level and the national level in one pass.
# grouping(m.state) = 1 marks the national rows, which are labelled "All India".
# Only comparable series are used: no unit-suspect series, no market-days without a price.
AGG_SQL = """
SELECT
  date_trunc('{unit}', f.arrival_date)::date                     AS period_start,
  f.commodity_key,
  CASE WHEN grouping(m.state) = 1 THEN 'All India' ELSE m.state END AS state,
  COUNT(DISTINCT f.arrival_date)                                AS days_covered,   -- a partial period has fewer
  COUNT(DISTINCT CASE WHEN f.data_status = 'provisional'
                      THEN f.arrival_date END)                  AS provisional_days, -- may still change
  COUNT(DISTINCT f.market_key)                                  AS markets_reporting,
  SUM(f.report_count)                                           AS reports,
  percentile_approx(f.modal_price, 0.5)                         AS median_modal_price,
  round(avg(f.modal_price), 2)                                  AS avg_modal_price,
  min(f.min_price)                                              AS lowest_price,
  max(f.max_price)                                              AS highest_price
FROM {fact} f
JOIN {dim_market} m ON m.market_key = f.market_key
WHERE NOT f.is_unit_suspect AND f.modal_price IS NOT NULL AND ({scope})
GROUP BY GROUPING SETS (
  (date_trunc('{unit}', f.arrival_date), f.commodity_key, m.state),
  (date_trunc('{unit}', f.arrival_date), f.commodity_key)
)
"""

LOGIC = inc.fingerprint(DIM_MARKET_SQL, DIM_COMMODITY_SQL, FACT_SQL, AGG_SQL)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full, incremental or nothing to do?

# COMMAND ----------

state = inc.get_state(spark, C)
tables_ready = all(inc.table_exists(spark, t) for t in [DIM_MARKET, DIM_COMMODITY, DIM_DATE, FACT, *AGG.values()])
until = state.get("gold.silver_runs_until")
# Times are compared and stored as Spark-formatted text, never as Python datetimes: Python converts
# timestamps to the machine's time zone, which can differ from Spark's and shift them by hours.
pending = spark.sql(f"""
    SELECT mode, months, date_format(finished_at, 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS finished
    FROM {C}.ops.pipeline_runs
    WHERE layer = 'silver' {f"AND finished_at > CAST('{until}' AS TIMESTAMP)" if until else ""}
    ORDER BY finished_at
""").collect()
months = []

if MODE == "full":
    run_mode, reason = "full", "full rebuild requested"
elif not tables_ready or until is None or "gold.logic" not in state:
    run_mode, reason = "full", "first run: nothing processed yet"
elif state["gold.logic"] != LOGIC:
    run_mode, reason = "full", "gold logic changed: every month must follow it"
elif any(r["mode"] == "full" for r in pending):
    run_mode, reason = "full", "silver was rebuilt in full"
else:
    from datetime import date
    months = sorted({date.fromisoformat(m) for r in pending for m in (r["months"] or "").split(",") if m})
    run_mode = "incremental" if months else "skip"
    reason = (f"{len(pending)} silver run(s) to follow" if months
              else ("no new silver run" if not pending else "silver changed nothing"))

FULL = run_mode == "full"
m_ranges, w_ranges = inc.month_ranges(months), inc.week_ranges(months)
silver_scope = "true" if FULL else inc.range_predicate("arrival_date", m_ranges)
print(f"Mode: {run_mode} ({reason}). Months: {', '.join(str(m) for m in months) or ('all' if FULL else 'none')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimensions
# MAGIC Full: rebuilt. Incremental: markets and crops seen for the first time are appended.

# COMMAND ----------

if run_mode != "skip":
    markets = spark.sql(DIM_MARKET_SQL.format(silver=SILVER, scope=silver_scope))
    crops = spark.sql(DIM_COMMODITY_SQL.format(silver=SILVER, scope=silver_scope))
    if FULL:
        inc.create_or_replace(spark, markets, DIM_MARKET,
                              comment="One row per market. market_key is a hash of the names, so it never changes between rebuilds.")
        inc.create_or_replace(spark, crops, DIM_COMMODITY, comment="One row per crop.")
        dates = spark.sql("""
        SELECT
          d                                   AS date,
          year(d)                             AS year,
          quarter(d)                          AS quarter,
          month(d)                            AS month,
          date_format(d, 'MMM')               AS month_name,
          date_trunc('MONTH', d)::date        AS month_start,
          date_trunc('WEEK', d)::date         AS week_start,          -- Monday
          weekofyear(d)                       AS iso_week,
          date_format(d, 'EEE')               AS day_name,
          dayofweek(d) IN (1, 7)              AS is_weekend           -- 1 = Sunday, 7 = Saturday
        FROM (SELECT explode(sequence(DATE'2016-01-01', DATE'2027-12-31', INTERVAL 1 DAY)) AS d)
        """)
        inc.create_or_replace(spark, dates, DIM_DATE, comment="One row per calendar day, 2016 to 2027, including days with no trade.")
    else:
        new_markets = markets.join(spark.table(DIM_MARKET).select("market_key"), "market_key", "left_anti")
        new_crops = crops.join(spark.table(DIM_COMMODITY).select("commodity_key"), "commodity_key", "left_anti")
        n_markets, n_crops = new_markets.count(), new_crops.count()
        if n_markets:
            new_markets.write.mode("append").saveAsTable(DIM_MARKET)
        if n_crops:
            new_crops.write.mode("append").saveAsTable(DIM_COMMODITY)
        print(f"New markets: {n_markets}, new crops: {n_crops}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fact, weekly and monthly prices

# COMMAND ----------

rows_written = 0
if run_mode != "skip":
    fact_df = spark.sql(FACT_SQL.format(silver=SILVER, scope=silver_scope))
    if FULL:
        rows_written += inc.create_or_replace(
            spark, fact_df, FACT, cluster_by="arrival_date, commodity_key",
            comment="One row per day, market, crop, variety and grade. Rs per quintal, except unit-suspect series.")
    else:
        rows_written += inc.replace_ranges(spark, fact_df, FACT, "arrival_date", m_ranges)

    for name, unit, ranges in [("weekly", "WEEK", w_ranges), ("monthly", "MONTH", m_ranges)]:
        scope = "true" if FULL else inc.range_predicate("f.arrival_date", ranges)
        agg_df = spark.sql(AGG_SQL.format(unit=unit, fact=FACT, dim_market=DIM_MARKET, scope=scope))
        if FULL:
            rows_written += inc.create_or_replace(
                spark, agg_df, AGG[name], cluster_by="period_start, commodity_key",
                comment=f"{name.capitalize()} price by crop and state, plus All India. Median of market-day modal prices, Rs per quintal.")
        else:
            rows_written += inc.replace_ranges(spark, agg_df, AGG[name], "period_start", ranges)

# COMMAND ----------

# MAGIC %md
# MAGIC ### A readable view for ad-hoc questions
# MAGIC The star schema keeps names in the dimensions; this view joins them back for quick SQL.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {C}.gold.v_market_day_price AS
SELECT f.arrival_date, m.state, m.district, m.market, c.commodity, f.variety, f.grade,
       f.min_price, f.max_price, f.modal_price, f.report_count, f.outlier_reports,
       f.is_unit_suspect, f.level_ratio, f.data_status
FROM {FACT} f
JOIN {DIM_MARKET}    m ON m.market_key    = f.market_key
JOIN {C}.gold.dim_commodity c ON c.commodity_key = f.commodity_key
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checks: the run fails loudly if any of these is wrong
# MAGIC Run on the months processed (all of them for a full rebuild); the dimensions are checked whole, they are small.

# COMMAND ----------

rows_read = 0
if run_mode != "skip":
    fact_scope = "true" if FULL else inc.range_predicate("f.arrival_date", m_ranges)
    checks = spark.sql(f"""
    SELECT
      (SELECT COUNT(*) FROM {SILVER} WHERE {silver_scope})                                  AS silver_rows,
      (SELECT SUM(report_count + outlier_reports) FROM {FACT} f WHERE {fact_scope})         AS fact_reports,
      (SELECT COUNT(*) - COUNT(DISTINCT market_key)    FROM {DIM_MARKET})                   AS market_key_clashes,
      (SELECT COUNT(*) - COUNT(DISTINCT commodity_key) FROM {DIM_COMMODITY})                AS commodity_key_clashes,
      (SELECT COUNT(*) FROM (
         SELECT 1 FROM {FACT} f WHERE {fact_scope}
         GROUP BY arrival_date, market_key, commodity_key, variety, grade
         HAVING COUNT(*) > 1))                                                              AS fact_grain_breaks,
      (SELECT COUNT(*) FROM {FACT} f
         LEFT ANTI JOIN {DIM_MARKET} m ON m.market_key = f.market_key WHERE {fact_scope})   AS facts_without_market,
      (SELECT COUNT(*) FROM {FACT} f
         LEFT ANTI JOIN {DIM_COMMODITY} c ON c.commodity_key = f.commodity_key WHERE {fact_scope}) AS facts_without_crop,
      (SELECT COUNT(*) FROM {FACT} f
         LEFT ANTI JOIN {DIM_DATE} d ON d.date = f.arrival_date WHERE {fact_scope})         AS facts_without_date,
      (SELECT COUNT(*) FROM {DIM_COMMODITY} WHERE codes_seen > 1)                           AS crops_with_several_codes
    """).first().asDict()
    print(checks)
    rows_read = checks["silver_rows"]

    problems = []
    if (checks["fact_reports"] or 0) != checks["silver_rows"]:
        problems.append(f"fact holds {checks['fact_reports']:,} reports but silver has {checks['silver_rows']:,}")
    for k in ["market_key_clashes", "commodity_key_clashes", "fact_grain_breaks",
              "facts_without_market", "facts_without_crop", "facts_without_date"]:
        if checks[k]:
            problems.append(f"{k} = {checks[k]}")
    if problems:
        raise Exception("Gold checks failed: " + "; ".join(problems))
    print(f"Gold checks passed: all {checks['silver_rows']:,} silver reports in scope are in the fact table.")
    if checks["crops_with_several_codes"]:
        print(f"Note: {checks['crops_with_several_codes']} crop name(s) appear with more than one code (kept the lowest).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log the run and remember what was processed

# COMMAND ----------

inc.log_run(spark, C, "gold", run_mode, reason, months, rows_read, rows_written, STARTED)
new_state = {"gold.logic": LOGIC}
if pending:
    new_state["gold.silver_runs_until"] = pending[-1]["finished"]       # latest silver run followed
elif until is None:          # first run with no silver run logged yet: start from now
    new_state["gold.silver_runs_until"] = spark.sql(
        "SELECT date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss.SSSSSS')").first()[0]
inc.set_state(spark, C, new_state)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Summary

# COMMAND ----------

display(spark.sql(f"""
SELECT 'dim_market' AS table_name, COUNT(*) AS rows FROM {DIM_MARKET}
UNION ALL SELECT 'dim_commodity', COUNT(*) FROM {DIM_COMMODITY}
UNION ALL SELECT 'dim_date', COUNT(*) FROM {DIM_DATE}
UNION ALL SELECT 'fact_market_day_price', COUNT(*) FROM {FACT}
UNION ALL SELECT 'agg_price_weekly', COUNT(*) FROM {AGG['weekly']}
UNION ALL SELECT 'agg_price_monthly', COUNT(*) FROM {AGG['monthly']}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Every run so far: what it processed and how long it took

# COMMAND ----------

display(spark.sql(f"""
SELECT layer, mode, reason, months_processed, rows_read, rows_written, seconds, finished_at
FROM {C}.ops.pipeline_runs
ORDER BY finished_at DESC
LIMIT 20
"""))
