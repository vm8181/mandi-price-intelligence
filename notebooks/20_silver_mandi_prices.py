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
# MAGIC | Outlier flag | Prices more than 10x above or below the crop's monthly median | E.g. cherry at Rs 16.5 lakh per quintal; kept, but flagged so gold can exclude them from averages |
# MAGIC | Exact copies | Identical rows kept once | Defensive: version 2 downloads have none |
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
monthly_median AS (                                -- each crop's typical price that month
  SELECT commodity, date_trunc('MONTH', arrival_date) AS month,
         percentile_approx(modal_price, 0.5)       AS median_modal
  FROM typed
  WHERE modal_price > 0
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
  coalesce(t.modal_price > 10 * m.median_modal
           OR t.modal_price < m.median_modal / 10, false)      AS is_price_outlier
FROM typed t
LEFT JOIN monthly_median m
  ON t.commodity = m.commodity AND date_trunc('MONTH', t.arrival_date) = m.month
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver table: rows that passed, exact copies kept once
# MAGIC **Liquid clustering** on date and commodity: queries filtering by them read only the data they need.
# MAGIC It replaces hand-picked partitions and can be changed later without rewriting the table.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {C}.silver.mandi_prices
CLUSTER BY (arrival_date, commodity)
COMMENT 'One row per mandi price report: typed, standard state names, quality-checked. Rs per quintal.'
AS
SELECT report_id, arrival_date, state, state_raw, district, market, commodity, commodity_code,
       variety, grade, min_price, max_price, modal_price, is_price_outlier, source_file, ingested_at
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
# MAGIC bronze = silver + quarantine + exact copies removed. If not, the run fails loudly.
# MAGIC Each run's numbers are appended to `ops.silver_quality_log`, so quality can be tracked over time.

# COMMAND ----------

counts = spark.sql(f"""
SELECT
  (SELECT COUNT(*) FROM {C}.bronze.mandi_prices_raw)                        AS bronze_rows,
  (SELECT COUNT(*) FROM {C}.silver.mandi_prices)                            AS silver_rows,
  (SELECT COUNT(*) FROM {C}.silver.mandi_prices_quarantine)                 AS quarantined_rows,
  (SELECT COUNT(*) FROM silver_candidates WHERE quality_issues = '')
    - (SELECT COUNT(*) FROM {C}.silver.mandi_prices)                        AS exact_copies_removed,
  (SELECT COUNT(*) FROM {C}.silver.mandi_prices WHERE is_price_outlier)     AS outlier_rows
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
spark.sql(f"""
INSERT INTO {C}.ops.silver_quality_log VALUES (current_timestamp(),
  {counts.bronze_rows}, {counts.silver_rows}, {counts.quarantined_rows},
  {counts.exact_copies_removed}, {counts.outlier_rows})
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
# MAGIC ### Most extreme flagged prices (for review)

# COMMAND ----------

display(spark.sql(f"""
SELECT arrival_date, state, market, commodity, variety, modal_price
FROM {C}.silver.mandi_prices
WHERE is_price_outlier
ORDER BY modal_price DESC
LIMIT 20
"""))
