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
# MAGIC Rebuilt in full on each run. No arrival quantities exist in this dataset, so every market-day counts equally.

# COMMAND ----------

dbutils.widgets.text("catalog", "mandi_dev")
C = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimensions

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {C}.gold.dim_market
COMMENT 'One row per market. market_key is a hash of the names, so it never changes between rebuilds.'
AS
SELECT DISTINCT
  xxhash64(state, district, market) AS market_key,
  state, district, market
FROM {C}.silver.mandi_prices
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {C}.gold.dim_commodity
COMMENT 'One row per crop.'
AS
SELECT
  xxhash64(commodity)              AS commodity_key,
  commodity,
  min(commodity_code)              AS commodity_code,
  COUNT(DISTINCT commodity_code)   AS codes_seen          -- should be 1; checked below
FROM {C}.silver.mandi_prices
GROUP BY commodity
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {C}.gold.dim_date
COMMENT 'One row per calendar day, 2016 to 2027, including days with no trade.'
AS
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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fact: one price per market, crop, variety and grade per day
# MAGIC Several reports a day are combined: lowest minimum, highest maximum, average modal.
# MAGIC Outlier reports are counted but left out of the prices.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {C}.gold.fact_market_day_price
CLUSTER BY (arrival_date, commodity_key)
COMMENT 'One row per day, market, crop, variety and grade. Rs per quintal, except unit-suspect series.'
AS
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
FROM {C}.silver.mandi_prices
GROUP BY arrival_date, state, district, market, commodity, variety, grade
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Weekly and monthly prices by crop and state, plus All India
# MAGIC `GROUPING SETS` computes the state level and the national level in one pass.
# MAGIC `grouping(m.state) = 1` marks the national rows, which are labelled "All India".
# MAGIC Only comparable series are used: no unit-suspect series, no market-days without a price.

# COMMAND ----------

for name, unit in [("weekly", "WEEK"), ("monthly", "MONTH")]:
    spark.sql(f"""
    CREATE OR REPLACE TABLE {C}.gold.agg_price_{name}
    CLUSTER BY (period_start, commodity_key)
    COMMENT '{name.capitalize()} price by crop and state, plus All India. Median of market-day modal prices, Rs per quintal.'
    AS
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
    FROM {C}.gold.fact_market_day_price f
    JOIN {C}.gold.dim_market m ON m.market_key = f.market_key
    WHERE NOT f.is_unit_suspect AND f.modal_price IS NOT NULL
    GROUP BY GROUPING SETS (
      (date_trunc('{unit}', f.arrival_date), f.commodity_key, m.state),
      (date_trunc('{unit}', f.arrival_date), f.commodity_key)
    )
    """)

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
FROM {C}.gold.fact_market_day_price f
JOIN {C}.gold.dim_market    m ON m.market_key    = f.market_key
JOIN {C}.gold.dim_commodity c ON c.commodity_key = f.commodity_key
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checks: the run fails loudly if any of these is wrong

# COMMAND ----------

checks = spark.sql(f"""
SELECT
  (SELECT COUNT(*) FROM {C}.silver.mandi_prices)                                        AS silver_rows,
  (SELECT SUM(report_count + outlier_reports) FROM {C}.gold.fact_market_day_price)      AS fact_reports,
  (SELECT COUNT(*) - COUNT(DISTINCT market_key)    FROM {C}.gold.dim_market)            AS market_key_clashes,
  (SELECT COUNT(*) - COUNT(DISTINCT commodity_key) FROM {C}.gold.dim_commodity)         AS commodity_key_clashes,
  (SELECT COUNT(*) FROM (
     SELECT 1 FROM {C}.gold.fact_market_day_price
     GROUP BY arrival_date, market_key, commodity_key, variety, grade
     HAVING COUNT(*) > 1))                                                              AS fact_grain_breaks,
  (SELECT COUNT(*) FROM {C}.gold.fact_market_day_price f
     LEFT ANTI JOIN {C}.gold.dim_market m ON m.market_key = f.market_key)               AS facts_without_market,
  (SELECT COUNT(*) FROM {C}.gold.fact_market_day_price f
     LEFT ANTI JOIN {C}.gold.dim_date d ON d.date = f.arrival_date)                     AS facts_without_date,
  (SELECT COUNT(*) FROM {C}.gold.dim_commodity WHERE codes_seen > 1)                    AS crops_with_several_codes
""").first().asDict()
print(checks)

problems = []
if checks["fact_reports"] != checks["silver_rows"]:
    problems.append(f"fact holds {checks['fact_reports']:,} reports but silver has {checks['silver_rows']:,}")
for k in ["market_key_clashes", "commodity_key_clashes", "fact_grain_breaks",
          "facts_without_market", "facts_without_date"]:
    if checks[k]:
        problems.append(f"{k} = {checks[k]}")
if problems:
    raise Exception("Gold checks failed: " + "; ".join(problems))
print(f"Gold checks passed: all {checks['silver_rows']:,} silver reports are in the fact table.")
if checks["crops_with_several_codes"]:
    print(f"Note: {checks['crops_with_several_codes']} crop name(s) appear with more than one code (kept the lowest).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Summary

# COMMAND ----------

display(spark.sql(f"""
SELECT 'dim_market' AS table_name, COUNT(*) AS rows FROM {C}.gold.dim_market
UNION ALL SELECT 'dim_commodity', COUNT(*) FROM {C}.gold.dim_commodity
UNION ALL SELECT 'dim_date', COUNT(*) FROM {C}.gold.dim_date
UNION ALL SELECT 'fact_market_day_price', COUNT(*) FROM {C}.gold.fact_market_day_price
UNION ALL SELECT 'agg_price_weekly', COUNT(*) FROM {C}.gold.agg_price_weekly
UNION ALL SELECT 'agg_price_monthly', COUNT(*) FROM {C}.gold.agg_price_monthly
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### First look: All India onion and tomato, week by week

# COMMAND ----------

display(spark.sql(f"""
SELECT w.period_start AS week, c.commodity, w.median_modal_price, w.markets_reporting,
       w.days_covered, w.provisional_days
FROM {C}.gold.agg_price_weekly w
JOIN {C}.gold.dim_commodity c ON c.commodity_key = w.commodity_key
WHERE w.state = 'All India' AND c.commodity IN ('Onion', 'Tomato', 'Potato')
ORDER BY c.commodity, week
"""))
