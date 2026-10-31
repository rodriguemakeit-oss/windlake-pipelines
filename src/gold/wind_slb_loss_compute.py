# Databricks notebook source
# MAGIC %md
# MAGIC # Loading loss stop

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Env variables

# COMMAND ----------

schema = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading loss

# COMMAND ----------

ltsa_wind_turbine_df = spark.table(f"{schema}.wind_turbine_ltsa_scope")
partition_farm = (
    ltsa_wind_turbine_df.select("farm_code")
    .distinct()
    .orderBy("farm_code")
    .collect()
)

last_year = datetime.now().year - 1
current_year = datetime.now().year

years = [str(last_year), str(current_year)]  # cast to string because your Delta 'year' is string

# COMMAND ----------

# === Final aggregated output over all partitions ===
loss_10min_wind_new_df = None

for farm in partition_farm:
    for y in years:
        # ---------------------------------------------------------
        # 🔥 OPTIMIZED READ:
        # Spark will ONLY read the specific Delta partition
        # farm_code=<farm>/year=<y>/ thanks to partition pruning.
        # ---------------------------------------------------------
        df_partition_raw = (
            spark.read.format("delta")
            .load("/Volumes/rfgbu_it_cdh_common_sources/shared_data/telemetrystoplogbookwind/loss_10min_wind/data")
            .where(F.col("farm_code") == farm.farm_code)
            .where(F.col("year") == y)
        )

        # Skip empty partitions to avoid unnecessary processing
        if df_partition_raw.limit(1).count() == 0:
            continue

        # ---------------------------------------------------------
        # 1. Select required columns
        # ---------------------------------------------------------
        df_partition_clean = df_partition_raw.select(
            "stop_id",
            "year",
            "month",
            "ent_code",
            "stop_duration_sec",
            "loss_value_kwh",
        )

        # ---------------------------------------------------------
        # 2. Append partition result to final dataframe
        # ---------------------------------------------------------
        loss_10min_wind_new_df = df_partition_clean if loss_10min_wind_new_df is None else loss_10min_wind_new_df.unionByName(df_partition_clean)

# COMMAND ----------

loss_10min_wind_new_join_pic_df = loss_10min_wind_new_df.join(
    ltsa_wind_turbine_df.select("wind_turbine_code", "substation_code", "farm_code"),
    loss_10min_wind_new_df["ent_code"] == ltsa_wind_turbine_df["wind_turbine_code"],
    how="left",
)

# COMMAND ----------

loss_10min_wind_new_join_pic_df = loss_10min_wind_new_join_pic_df.groupBy(
    ["stop_id", "year", "month"]
).agg(
  F.first("farm_code").alias("farm_code"),
  F.first("ent_code").alias("ent_code"),
  F.sum("stop_duration_sec").alias("stop_duration_sec"),
  F.sum("loss_value_kwh").alias("loss_value_kwh")
)

# COMMAND ----------

# ---------------------------------------------------------
# Convert to expected units to be able to compare with SLB
# ---------------------------------------------------------
loss_10min_wind_new_join_pic_df = loss_10min_wind_new_join_pic_df.withColumn(
    "loss_value_mwh", F.col("loss_value_kwh") / 1000
).withColumn(
    "stop_duration_min", F.col("stop_duration_sec") / 60
)

columns_to_keep = [
    "stop_id",
    "year",
    "month",
    "ent_code",
    "stop_duration_min",
    "loss_value_mwh",
]
loss_10min_wind_new_join_pic_df = loss_10min_wind_new_join_pic_df.select(*columns_to_keep)

# COMMAND ----------

loss_10min_wind_table = f"{schema}.loss_10min_wind"

if not spark.catalog.tableExists(loss_10min_wind_table):
    loss_10min_wind_new_join_pic_df.writeTo(loss_10min_wind_table)\
        .partitionedBy("year")\
        .create()
else:
    # Partition overwrite — replaces data for the targeted years only
    loss_10min_wind_new_join_pic_df.writeTo(loss_10min_wind_table)\
        .overwrite(F.col("year").isin(str(last_year), str(current_year)))
