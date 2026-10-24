# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: clean, typed, trusted mandi prices
# MAGIC
# MAGIC Turns bronze's raw text into data you can analyse, under rules we control.
# MAGIC Every rule here comes from profiling bronze (see `docs/data_quality.md`).
# MAGIC
# MAGIC | Step | What | Why |
# MAGIC | --- | --- | --- |
# MAGIC | Types | Dates parsed as day/month/year; prices as decimals | Bronze keeps text; `01/10/2026` is ambiguous unless the format is stated |
# MAGIC | Standard names | One name per state, via a reference table | The source uses "Kerala" and "Keralam", "Pondicherry", "Chattisgarh" |
# MAGIC | Quality rules | Rows breaking a rule go to quarantine, with the reason | Bad rows are kept and visible, never silently dropped |
# MAGIC | Unit flag | A market's whole monthly series for a crop more than 10x off the typical market | Some markets fill "Rs per quintal" per piece, bunch or kg (Pune leafy vegetables per bunch, Mapusa coconut per nut). Fine for that market's own trend, not for comparing markets |
# MAGIC | Outlier flag | One price more than 10x off its own market's usual price that month | A one-off entry error; kept, but flagged so gold leaves it out |
# MAGIC | Copies after cleaning | Rows identical once spaces are trimmed are kept once | Some Maharashtra and Telangana markets are published twice, as "APMC Kolhapur" and "APMC Kolhapur " (see data_quality.md) |
# MAGIC | Grain | One row per price **report** | Some markets genuinely report several prices a day; gold summarises per market-day |
# MAGIC | Reconciliation | bronze = silver + quarantine + copies removed | Every bronze row is accounted for |
# MAGIC
# MAGIC Rebuilt in full from bronze on each run (simple and always consistent at this size).
# MAGIC Switch to incremental MERGE if run time becomes a problem.

# COMMAND ----------

dbutils.widgets.text("catalog", "mandi_dev")
C = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reference table: one standard name per state
# MAGIC A mapping table, not a CASE statement in code: when the source introduces a new spelling,
# MAGIC you add a row; no code changes. The original name is kept in silver as `state_raw`.

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

# Any state in bronze that the mapping doesn't know? Silver would keep its raw name; list them to add.
unmapped = spark.sql(f"""
    SELECT DISTINCT trim(b.State) AS state_raw
    FROM {C}.bronze.mandi_prices_raw b
    LEFT ANTI JOIN {C}.silver.ref_state_alias a ON trim(b.State) = a.state_raw
""")
if unmapped.count():
    print("State names not in the mapping yet (add them to ref_state_alias):")
    display(unmapped)
else:
    print("Every state name in bronze is mapped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Type, standardise, check every row
# MAGIC `try_cast` / `try_to_date` return NULL instead of failing on a bad value, so one bad row can't stop
# MAGIC the whole run; the NULL is then caught by a rule and the row quarantined with its reason.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW silver_candidates AS
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
    b.record_json, b.source_file, b.ingested_at
  FROM {C}.bronze.mandi_prices_raw b
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
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver table: rows that passed, copies after cleaning kept once
# MAGIC Names are already trimmed in `silver_candidates`, so a report published under both
# MAGIC "APMC Kolhapur" and "APMC Kolhapur " counts once. The column `exact_copies_removed` in
# MAGIC `ops.silver_quality_log` keeps its name so earlier runs stay comparable; it counts copies after cleaning.
# MAGIC **Liquid clustering** on date and commodity: queries filtering by them read only the data they need.
# MAGIC It replaces hand-picked partitions and can be changed later without rewriting the table.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {C}.silver.mandi_prices
CLUSTER BY (arrival_date, commodity)
COMMENT 'One row per mandi price report: typed, standard state names, quality-checked. Rs per quintal.'
AS
SELECT report_id, arrival_date, state, state_raw, district, market, commodity, commodity_code,
       variety, grade, min_price, max_price, modal_price,
       is_price_outlier, is_unit_suspect, level_ratio, source_file, ingested_at
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
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {C}.silver.mandi_prices_quarantine
COMMENT 'Rows that failed a silver quality rule, with the reason. Kept for review, never silently dropped.'
AS
SELECT quality_issues, arrival_date, state_raw, district, market, commodity, variety, grade,
       min_price, max_price, modal_price, record_json, source_file, ingested_at
FROM silver_candidates
WHERE quality_issues <> ''
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reconcile and log: every bronze row is accounted for
# MAGIC bronze = silver + quarantine + copies removed (after cleaning). If not, the run fails loudly.
# MAGIC Each run's numbers are appended to `ops.silver_quality_log`, so quality can be tracked over time.

# COMMAND ----------

counts = spark.sql(f"""
SELECT
  (SELECT COUNT(*) FROM {C}.bronze.mandi_prices_raw)                        AS bronze_rows,
  (SELECT COUNT(*) FROM {C}.silver.mandi_prices)                            AS silver_rows,
  (SELECT COUNT(*) FROM {C}.silver.mandi_prices_quarantine)                 AS quarantined_rows,
  (SELECT COUNT(*) FROM silver_candidates WHERE quality_issues = '')
    - (SELECT COUNT(*) FROM {C}.silver.mandi_prices)                        AS exact_copies_removed,
  (SELECT COUNT(*) FROM {C}.silver.mandi_prices WHERE is_price_outlier)     AS outlier_rows,
  (SELECT COUNT(*) FROM {C}.silver.mandi_prices WHERE is_unit_suspect)      AS unit_suspect_rows
""").first()

print(counts.asDict())
accounted = counts.silver_rows + counts.quarantined_rows + counts.exact_copies_removed
if accounted != counts.bronze_rows:
    raise Exception(f"Reconciliation failed: bronze {counts.bronze_rows:,} vs accounted {accounted:,}")
print(f"Reconciliation passed: all {counts.bronze_rows:,} bronze rows accounted for.")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {C}.ops.silver_quality_log (
  run_at TIMESTAMP, bronze_rows BIGINT, silver_rows BIGINT, quarantined_rows BIGINT,
  exact_copies_removed BIGINT, outlier_rows BIGINT
) COMMENT 'One row per silver run: how many rows passed, failed, were de-duplicated or flagged'
""")
# Added later: older runs keep NULL here (they didn't measure it), new runs fill it.
if "unit_suspect_rows" not in spark.table(f"{C}.ops.silver_quality_log").columns:
    spark.sql(f"ALTER TABLE {C}.ops.silver_quality_log ADD COLUMNS (unit_suspect_rows BIGINT)")
spark.sql(f"""
INSERT INTO {C}.ops.silver_quality_log
  (run_at, bronze_rows, silver_rows, quarantined_rows, exact_copies_removed, outlier_rows, unit_suspect_rows)
VALUES (current_timestamp(),
  {counts.bronze_rows}, {counts.silver_rows}, {counts.quarantined_rows},
  {counts.exact_copies_removed}, {counts.outlier_rows}, {counts.unit_suspect_rows})
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### What failed, and why

# COMMAND ----------

display(spark.sql(f"""
SELECT issue, COUNT(*) AS rows
FROM (SELECT explode(split(quality_issues, '; ')) AS issue      -- a row can fail several rules
      FROM {C}.silver.mandi_prices_quarantine)
GROUP BY issue
ORDER BY rows DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Series in a different unit (for review)
# MAGIC Whole market-crop series far from the typical market. A ratio near 0.005 usually means priced
# MAGIC per piece or bunch; near 0.01, per kg. Usable for that market's own trend, not for comparing markets.

# COMMAND ----------

display(spark.sql(f"""
SELECT state, market, commodity,
       COUNT(*)                    AS rows,
       COUNT(DISTINCT arrival_date) AS days,
       percentile_approx(modal_price, 0.5) AS median_modal,
       round(avg(level_ratio), 4)  AS level_ratio
FROM {C}.silver.mandi_prices
WHERE is_unit_suspect
GROUP BY ALL
ORDER BY rows DESC
LIMIT 30
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### One-off outliers (for review)
# MAGIC Prices more than 10x away from their own market's usual price that month.

# COMMAND ----------

display(spark.sql(f"""
SELECT arrival_date, state, market, commodity, variety, modal_price, level_ratio
FROM {C}.silver.mandi_prices
WHERE is_price_outlier
ORDER BY modal_price DESC
LIMIT 20
"""))
