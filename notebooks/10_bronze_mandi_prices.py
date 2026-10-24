# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: raw mandi prices
# MAGIC
# MAGIC Loads the raw API answers uploaded from the laptop into Delta tables, exactly as received.
# MAGIC
# MAGIC | Table | Source folder | What |
# MAGIC | --- | --- | --- |
# MAGIC | `bronze.mandi_prices_raw` | `raw_files/mandi_v2/arrival_date=D/commodity=ALL/` | **Final** days: older than 30 days, no longer changing |
# MAGIC | `bronze.mandi_prices_provisional_raw` | `raw_files/mandi_v2_provisional/arrival_date=D/pulled_at=T/expected_rows=N/` | **Provisional** versions of the last 30 days, which still receive late reports |
# MAGIC
# MAGIC - **Auto Loader** reads only files it hasn't loaded before (its checkpoint remembers them),
# MAGIC   so rerunning this notebook never duplicates rows.
# MAGIC - **Every value is kept as text**, plus the original record JSON, so a change in the source's
# MAGIC   types or fields can never break the load. Silver converts types.
# MAGIC - **Lineage**: every row records its source file and load time.
# MAGIC - A provisional version's folder name carries its row count (`expected_rows`), so silver can tell
# MAGIC   a fully uploaded version from a partly uploaded one.

# COMMAND ----------

# Which catalog to use: mandi_dev while building, mandi_prod later. Same code for both.
dbutils.widgets.text("catalog", "mandi_dev")
CATALOG    = dbutils.widgets.get("catalog")

SOURCE     = f"/Volumes/{CATALOG}/bronze/raw_files/mandi_v2/"
CHECKPOINT = f"/Volumes/{CATALOG}/bronze/checkpoints/mandi_prices_raw_v2"
TARGET     = f"{CATALOG}.bronze.mandi_prices_raw"

PROV_SOURCE     = f"/Volumes/{CATALOG}/bronze/raw_files/mandi_v2_provisional/"
PROV_CHECKPOINT = f"/Volumes/{CATALOG}/bronze/checkpoints/mandi_prices_provisional_raw"
PROV_TARGET     = f"{CATALOG}.bronze.mandi_prices_provisional_raw"

# The fields in each record, all kept as text.
RECORD_FIELDS = ["Arrival_Date", "Commodity", "Commodity_Code", "District", "Grade",
                 "Market", "Max_Price", "Min_Price", "Modal_Price", "State", "Variety"]
RECORD_SCHEMA = "struct<" + ",".join(f"{f}:string" for f in RECORD_FIELDS) + ">"

# COMMAND ----------

# MAGIC %md
# MAGIC ### One-time reset (only when rebuilding bronze from scratch)
# MAGIC Set `RESET_BRONZE = True` and run this cell once, then set it back to `False`.
# MAGIC Safe because bronze can always be rebuilt from the files in the volume.

# COMMAND ----------

RESET_BRONZE = False

if RESET_BRONZE:
    for table in [TARGET, PROV_TARGET]:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    for path in [CHECKPOINT, PROV_CHECKPOINT,
                 f"/Volumes/{CATALOG}/bronze/checkpoints/mandi_prices_raw",   # version 1 checkpoint
                 f"/Volumes/{CATALOG}/bronze/raw_files/mandi"]:               # version 1 files
        try:
            dbutils.fs.rm(path, True)
        except Exception as err:
            print("nothing to remove at", path)
    print("Bronze reset. Set RESET_BRONZE back to False.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load new final days

# COMMAND ----------

from pyspark.sql import functions as F

def records_of(pages, *extra):
    """One row per record: each record's fields as text, its original JSON, and lineage.
    File details come from _metadata, which is only available in this first select."""
    return (pages
        # "records" arrives as text: split it into one item per record, keeping each record's original JSON.
        .select(
            F.explode(F.from_json("records", "array<string>")).alias("record_json"),
            F.col("total").alias("api_state_total"),          # rows the API said this state had that day
            F.col("_metadata.file_path").alias("source_file"),
            F.col("_metadata.file_modification_time").alias("source_file_modified_at"),
            *extra)
        .withColumn("r", F.from_json("record_json", RECORD_SCHEMA)))

pages = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", CHECKPOINT + "/_schema")
    .option("cloudFiles.partitionColumns", "arrival_date,commodity")   # read from the folder names
    .option("pathGlobFilter", "state_*.json.gz")                        # data files only, not markers
    .load(SOURCE))

bronze = (records_of(pages, F.col("arrival_date").alias("folder_arrival_date"))
    .select("r.*", "record_json", "api_state_total", "folder_arrival_date",
            "source_file", "source_file_modified_at")
    .withColumn("ingested_at", F.current_timestamp()))

(bronze.writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)                           # load everything new, then stop
    .toTable(TARGET)
    .awaitTermination())

print("Rows in bronze (final):", spark.table(TARGET).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load new provisional versions
# MAGIC The schema is given rather than inferred, because the folder can be empty (no recent days
# MAGIC uploaded yet) and Auto Loader can't infer a schema from nothing. Only `total` and `records` are
# MAGIC needed; JSON values read into a text column keep their original JSON text.
# MAGIC The day, version time and expected row count are read from the folder names.

# COMMAND ----------

dbutils.fs.mkdirs(PROV_SOURCE)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {PROV_TARGET} (
  {", ".join(f"{f} STRING" for f in RECORD_FIELDS)},
  record_json STRING, api_state_total STRING,
  folder_arrival_date STRING, pulled_at STRING, expected_rows INT,
  source_file STRING, source_file_modified_at TIMESTAMP, ingested_at TIMESTAMP
) COMMENT 'Provisional versions of recent days, as received. Superseded versions are removed after each load.'
""")

prov_pages = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("pathGlobFilter", "state_*.json.gz")
    .schema("total STRING, records STRING")
    .load(PROV_SOURCE))

path = F.col("source_file")
prov = (records_of(prov_pages)
    .select("r.*", "record_json", "api_state_total",
            F.regexp_extract(path, r"arrival_date=([0-9-]+)", 1).alias("folder_arrival_date"),
            F.regexp_extract(path, r"pulled_at=([0-9T]+)", 1).alias("pulled_at"),
            F.regexp_extract(path, r"expected_rows=([0-9]+)", 1).cast("int").alias("expected_rows"),
            "source_file", "source_file_modified_at")
    .withColumn("ingested_at", F.current_timestamp()))

(prov.writeStream
    .option("checkpointLocation", PROV_CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(PROV_TARGET)
    .awaitTermination())

# Housekeeping: provisional rows are only needed until something better exists.
# 1. Days that are now final.
spark.sql(f"""
DELETE FROM {PROV_TARGET}
WHERE to_date(folder_arrival_date) IN (SELECT DISTINCT to_date(folder_arrival_date) FROM {TARGET})
""")
# 2. Older versions of a day once a newer complete version (all expected rows loaded) exists.
spark.sql(f"""
DELETE FROM {PROV_TARGET}
WHERE concat(folder_arrival_date, '|', pulled_at) IN (
  SELECT concat(v.folder_arrival_date, '|', v.pulled_at)
  FROM (SELECT folder_arrival_date, pulled_at FROM {PROV_TARGET} GROUP BY ALL) v
  JOIN (SELECT folder_arrival_date, max(pulled_at) AS latest_complete
        FROM (SELECT folder_arrival_date, pulled_at, COUNT(*) AS loaded, MAX(expected_rows) AS expected
              FROM {PROV_TARGET} GROUP BY ALL)
        WHERE loaded = expected
        GROUP BY folder_arrival_date) c
    ON v.folder_arrival_date = c.folder_arrival_date AND v.pulled_at < c.latest_complete
)
""")
print("Rows in bronze (provisional, after housekeeping):", spark.table(PROV_TARGET).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reconcile: every file must hold exactly the rows the API said it had
# MAGIC Day-level completeness (states add up to the day's total, no duplicates) was already
# MAGIC enforced on the laptop before a day was uploaded. This checks nothing was lost on the way.

# COMMAND ----------

mismatches = spark.sql(f"""
    SELECT source_file, COUNT(*) AS rows_loaded, MAX(CAST(api_state_total AS INT)) AS rows_expected
    FROM (SELECT source_file, api_state_total FROM {TARGET}
          UNION ALL
          SELECT source_file, api_state_total FROM {PROV_TARGET})
    GROUP BY source_file
    HAVING COUNT(*) <> MAX(CAST(api_state_total AS INT))
""")
n = mismatches.count()
if n:
    display(mismatches)
    raise Exception(f"{n} file(s) don't match their API totals: see the table above")
print("Reconciliation passed: every file matches its API total.")

display(spark.sql(f"""
    SELECT 'final' AS data, COUNT(DISTINCT folder_arrival_date) AS days, COUNT(DISTINCT source_file) AS files,
           COUNT(*) AS rows, MIN(folder_arrival_date) AS first_day, MAX(folder_arrival_date) AS last_day
    FROM {TARGET}
    UNION ALL
    SELECT 'provisional', COUNT(DISTINCT folder_arrival_date), COUNT(DISTINCT source_file),
           COUNT(*), MIN(folder_arrival_date), MAX(folder_arrival_date)
    FROM {PROV_TARGET}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Provisional versions loaded
# MAGIC A version is **complete** when every expected row has arrived. Silver only uses complete versions;
# MAGIC an incomplete one is a partly uploaded version, which the laptop finishes on its next upload.

# COMMAND ----------

display(spark.sql(f"""
    SELECT folder_arrival_date AS day, pulled_at, MAX(expected_rows) AS expected_rows,
           COUNT(*) AS rows_loaded, COUNT(*) = MAX(expected_rows) AS complete
    FROM {PROV_TARGET}
    GROUP BY ALL
    ORDER BY day DESC, pulled_at DESC
"""))
