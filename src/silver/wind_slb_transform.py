# Databricks notebook source
# MAGIC %md
# MAGIC # SLB computation
# MAGIC This notebook allows to compute Stop Log Book (SLB)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

def end_of_day(col):
    return F.date_trunc("day", col) + F.expr("INTERVAL 1 DAY") - F.expr("INTERVAL 1 SECOND")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load variables

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
ltsa_scope_regex = dbutils.widgets.get("ltsa_scope_regex")
# number of months to lookback default "13"
lookback_n_months = int(dbutils.widgets.get("lookback_n_months"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading input data

# COMMAND ----------

ltsa_wind_turbine_df = spark.table(f"{catalog}.default.wind_turbine_ltsa_scope")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute SLB for month

# COMMAND ----------

raw_df = spark.read.format("delta").load("/Volumes/rfgbu_it_cdh_common_sources/shared_data/telemetrystoplogbookwind/stop_wind/data")
raw_df = raw_df.filter(raw_df.farm_code.rlike(ltsa_scope_regex)) \
    .withColumn("_input_file_name", F.col("_metadata.file_path"))

# fix when in SLB an end date < start date (can happen)
raw_df = raw_df.filter(F.col("start_date") <= F.col("end_date"))
raw_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute SLB for the last {lookback_n_months}
# MAGIC
# MAGIC In the next cells, we compute the Stop Log Book (SLB) for the last `{lookback_n_months}` months. This involves applying a *lookback window* to select and aggregate data from a specific period in the past, relative to the current date.
# MAGIC
# MAGIC The lookback window ensures that only events within the desired time frame are considered. For example, if `lookback_n_months = 13` and today is April 2026, the window covers January, February, and March 2026.
# MAGIC
# MAGIC **Timeline schema:**
# MAGIC
# MAGIC
# MAGIC | Month            | Feb (prev) | Mar (prev) | Apr      | May      | Jun      | Jul      | Aug      | Sep      | Oct      | Nov      | Dec      | Jan      | Feb      | Mar      | Apr (current) |
# MAGIC ------------------|:----------:|:----------:|:--------:|:--------:|:--------:|:--------:|:--------:|:--------:|:--------:|:--------:|:--------:|:--------:|:--------:|:--------:|:-------------:|
# MAGIC  **Included?**    | ⬜         | ✅         | ✅       | ✅       | ✅       | ✅       | ✅       | ✅       | ✅       | ✅       | ✅       | ✅       | ✅       | ✅       | ⬜            |
# MAGIC
# MAGIC <sub>**Lookback window:** last `{lookback_n_months}` months (highlighted with ✅)</sub>
# MAGIC
# MAGIC
# MAGIC All computations in the following cells are restricted to this window, ensuring the SLB metrics reflect only the recent operational period.

# COMMAND ----------

today = F.current_date()
n_month_ago = F.date_trunc("month", F.add_months(today, -lookback_n_months))
last_month = F.date_trunc("month", F.add_months(today, -1))

window_start = (F.col("start_date") <= end_of_day(F.last_day(last_month)))
window_end = (F.col("end_date") >= n_month_ago)

raw_df = raw_df.filter((window_start & (window_end | F.col("end_date").isNull())))

# COMMAND ----------

raw_df = raw_df.withColumn(
  "target_months",
  F.sequence(
    F.date_trunc("month", F.col("start_date")),
    # we want to handle end_date = NULL so we take the first value non null between end_date and last_month + we take the smallest one
    # by security in case of a stop date logged in the "future"
    F.date_trunc("month", F.least(F.coalesce(F.col("end_date"), last_month), last_month)),
    F.expr("INTERVAL 1 MONTH")
  )
)

# COMMAND ----------

raw_df = raw_df.withColumn(
  "target_month",
  F.explode(F.col("target_months"))
).withColumn(
  "slb_month", F.month(F.col("target_month"))
).withColumn(
  "slb_year", F.year(F.col("target_month"))
)

raw_df = raw_df.withColumn(
  "first_day_of_month", F.col("target_month")
).withColumn(
  "last_day_of_month", end_of_day(F.last_day(F.col("target_month")))
).withColumn(
  "start_date_in_target_month", F.greatest(F.col("start_date"), F.col("first_day_of_month"))
).withColumn(
  "end_date_in_target_month", F.least(F.coalesce(F.col("end_date"), F.col("last_day_of_month")), F.col("last_day_of_month"))
).withColumn(
  # give number of days between first_day_of_month and last_day_of_month + add 1 day because last_day_of_month is exclusive
  "month_time_in_seconds", (F.datediff(F.col("last_day_of_month"), F.col("first_day_of_month")) + 1) * 24 * 60 * 60
)

# filtering data after explode that reintroduced potential rows out of the window
raw_df = raw_df.filter(
  (F.col("target_month") >= n_month_ago) & (F.col("target_month") <= last_month)
)

raw_df = raw_df.filter(
    (F.col("first_day_of_month") <= F.col("end_date_in_target_month")) &
    (F.col("start_date_in_target_month") <= end_of_day(F.col("last_day_of_month")))
)

# COMMAND ----------

raw_df = raw_df.withColumn(
    "stop_duration_min_original", F.col("stop_duration_min")
).withColumn(
    "loss_value_mwh_original", F.col("loss_value_mwh")
).withColumn(
    "stop_duration_total_sec",
    F.unix_timestamp(F.coalesce(F.col("end_date"), F.col("write_time"))) - F.unix_timestamp(F.col("start_date"))
).withColumn(
    "stop_duration_during_month_sec",
    F.unix_timestamp(F.col("end_date_in_target_month")) - F.unix_timestamp(F.col("start_date_in_target_month"))
).withColumn(
    "prorata", F.when(
        F.col("stop_duration_total_sec") > 0,
        F.col("stop_duration_during_month_sec") / F.col("stop_duration_total_sec")
    ).otherwise(0)
).withColumn(
    "stop_duration_min", F.col("stop_duration_during_month_sec") / F.lit(60)
).withColumn(
    "loss_value_mwh", F.col("loss_value_mwh") * F.col("prorata")
)

raw_df.createOrReplaceTempView("raw_df")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join loss_10min_wind to replace prorata computation
# MAGIC Joining with `loss_10min_wind` will help to have more precise prorata of loss in mwh (not necessarly better for time because time is easy to "prorata")

# COMMAND ----------

slb_silver = spark.sql(
    f"""
    WITH slb_data_with_loss10min AS (
    SELECT r.*, l.stop_duration_min as stop_duration_min_10min, l.loss_value_mwh as loss_value_mwh_10min
    FROM raw_df as r
    LEFT JOIN {catalog}.gold.loss_10min_wind as l
    ON r.stop_id = l.stop_id
    AND r.slb_year = l.year
    AND r.slb_month = l.month
    )

    SELECT /*+ BROADCAST(wtg) */ slb_loss.*, wtg.wind_turbine_code, wtg.substation_code
    FROM slb_data_with_loss10min as slb_loss
    LEFT JOIN {catalog}.gold.wind_turbine_ltsa_scope as wtg
    ON slb_loss.ent_code = wtg.wind_turbine_code
    """
).withColumn(
    "stop_duration_min", F.when(
        F.col("stop_duration_min_10min").isNotNull(),
        F.col("stop_duration_min_10min")).otherwise(F.col("stop_duration_min"))
).withColumn(
    "loss_value_mwh", F.when(
        F.col("loss_value_mwh_10min").isNotNull(),
        F.col("loss_value_mwh_10min")).otherwise(F.col("loss_value_mwh"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writting data in Delta Table

# COMMAND ----------

"""
TODO export df to s3 ? to volume ?

df.write \
  .mode("overwrite") \
  .format("delta") \
  .option("delta.enableDeletionVectors", "false") \
  .option("mergeSchema", "true")  \
.save("s3://.../.../data")
"""

# COMMAND ----------

slb_silver_table = f"{schema}.turbine_slb"

# Absorb upstream schema evolution: the Telemetry SLB source occasionally gains new
# columns (e.g. is_acknowledged, is_curtailment). writeTo().overwrite() requires
# a matching schema, so without schema merge the write fails with
# DELTA_METADATA_MISMATCH and breaks the whole SLB chain.
# NB: the spark.databricks.delta.schema.autoMerge.enabled conf is NOT supported
# on serverless — use the per-write .option("mergeSchema", "true") instead.

if not spark.catalog.tableExists(slb_silver_table):
    slb_silver.writeTo(slb_silver_table)\
        .option("mergeSchema", "true")\
        .partitionedBy("slb_year", "slb_month")\
        .create()
else:
    # Partition overwrite — replaces data for the targeted months in [window_start, window_end]
    filter_window_start = spark.sql(f"SELECT date_trunc('month', add_months(current_date() , -{lookback_n_months}))").first()[0]
    filter_window_end = spark.sql("SELECT date_trunc('month', add_months(current_date() , -1))").first()[0]
    slb_silver.writeTo(slb_silver_table)\
        .option("mergeSchema", "true")\
        .overwrite(
            (F.col("target_month") >= F.lit(filter_window_start)) & (F.col("target_month") <= F.lit(filter_window_end))
        )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Goal: How many stops there are by month ?
# MAGIC -- select date_part('month', slb.target_month) as month, date_part('year', slb.target_month) as year, count(slb.stop_id) as stop_count
# MAGIC -- from ltsa_dev.default.slb_silver as slb
# MAGIC -- group by slb.target_month
# MAGIC -- sort by slb.target_month desc
