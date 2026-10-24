# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: raw mandi prices
# MAGIC
# MAGIC Loads the raw API answers uploaded from the laptop into the Delta table
# MAGIC `bronze.mandi_prices_raw`, exactly as received.
# MAGIC
# MAGIC - **Auto Loader** reads only files it hasn't loaded before (its checkpoint remembers them),
# MAGIC   so rerunning this notebook never duplicates rows.
# MAGIC - **Every value is kept as text**, plus the original record JSON, so a change in the source's
# MAGIC   types or fields can never break the load. Silver converts types.
# MAGIC - **Lineage**: every row records its source file and load time.
# MAGIC - Source files: one per state per day (version 2 download, no paging), in
# MAGIC   `raw_files/mandi_v2/arrival_date=YYYY-MM-DD/commodity=ALL/state_*.json.gz`.

# COMMAND ----------

# Which catalog to use: mandi_dev while building, mandi_prod later. Same code for both.
dbutils.widgets.text("catalog", "mandi_dev")
CATALOG    = dbutils.widgets.get("catalog")

SOURCE     = f"/Volumes/{CATALOG}/bronze/raw_files/mandi_v2/"
CHECKPOINT = f"/Volumes/{CATALOG}/bronze/checkpoints/mandi_prices_raw_v2"
TARGET     = f"{CATALOG}.bronze.mandi_prices_raw"

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
    spark.sql(f"DROP TABLE IF EXISTS {TARGET}")
    for path in [CHECKPOINT,
                 f"/Volumes/{CATALOG}/bronze/checkpoints/mandi_prices_raw",   # version 1 checkpoint
                 f"/Volumes/{CATALOG}/bronze/raw_files/mandi"]:               # version 1 files
        try:
            dbutils.fs.rm(path, True)
        except Exception as err:
            print("nothing to remove at", path)
    print("Bronze reset. Set RESET_BRONZE back to False.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load new files

# COMMAND ----------

from pyspark.sql import functions as F

pages = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", CHECKPOINT + "/_schema")
    .option("cloudFiles.partitionColumns", "arrival_date,commodity")   # read from the folder names
    .option("pathGlobFilter", "state_*.json.gz")                        # data files only, not markers
    .load(SOURCE))

bronze = (pages
    # "records" arrives as text: split it into one item per record, keeping each record's original JSON.
    .select(
        F.explode(F.from_json("records", "array<string>")).alias("record_json"),
        F.col("total").alias("api_state_total"),          # rows the API said this state had that day
        F.col("arrival_date").alias("folder_arrival_date"),
        F.col("_metadata.file_path").alias("source_file"),
        F.col("_metadata.file_modification_time").alias("source_file_modified_at"),
    )
    .withColumn("r", F.from_json("record_json", RECORD_SCHEMA))
    .select("r.*", "record_json", "api_state_total", "folder_arrival_date",
            "source_file", "source_file_modified_at")
    .withColumn("ingested_at", F.current_timestamp()))

(bronze.writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)                           # load everything new, then stop
    .toTable(TARGET)
    .awaitTermination())

print("Rows in bronze:", spark.table(TARGET).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reconcile: every file must hold exactly the rows the API said it had
# MAGIC Day-level completeness (states add up to the day's total, no duplicates) was already
# MAGIC enforced on the laptop before a day was marked finished. This checks nothing was lost on the way.

# COMMAND ----------

mismatches = spark.sql(f"""
    SELECT source_file, COUNT(*) AS rows_loaded, MAX(CAST(api_state_total AS INT)) AS rows_expected
    FROM {TARGET}
    GROUP BY source_file
    HAVING COUNT(*) <> MAX(CAST(api_state_total AS INT))
""")
n = mismatches.count()
if n:
    display(mismatches)
    raise Exception(f"{n} file(s) don't match their API totals: see the table above")
print("Reconciliation passed: every file matches its API total.")

display(spark.sql(f"""
    SELECT COUNT(DISTINCT folder_arrival_date) AS days, COUNT(DISTINCT source_file) AS files,
           COUNT(*) AS rows, MIN(folder_arrival_date) AS first_day, MAX(folder_arrival_date) AS last_day
    FROM {TARGET}
"""))
