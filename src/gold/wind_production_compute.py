# Databricks notebook source
# MAGIC %md
# MAGIC # Benchmark reading cds_10min_wind

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC Env variables

# COMMAND ----------

schema = dbutils.widgets.get("schema")
gold_bucket = dbutils.widgets.get("gold_bucket")

# COMMAND ----------

# MAGIC %md
# MAGIC ## External location volumes

# COMMAND ----------

ltsa_wind_turbine_df = spark.table(f"{schema}.wind_turbine_ltsa_scope")

# COMMAND ----------

partition_farm = (
    ltsa_wind_turbine_df.select("farm_code")
    .distinct()
    .orderBy("farm_code")
    .collect()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Partitions definition

# COMMAND ----------

last_year = datetime.now().year - 1
current_year = datetime.now().year

years = [str(last_year), str(current_year)]  # cast to string because your Delta 'year' is string

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute turbine production

# COMMAND ----------

# === Final aggregated output over all partitions ===
turbine_production_data_df = None

for farm in partition_farm:
    for y in years:
        # ---------------------------------------------------------
        # 🔥 OPTIMIZED READ:
        # Spark will ONLY read the specific Delta partition
        # farm_code=<farm>/year=<y>/ thanks to partition pruning.
        # ---------------------------------------------------------
        cds_10min_wind_df_part = (
            spark.read.format("delta")
            .load("/Volumes/rfgbu_it_cdh_common_sources/shared_data/telemetryscada/cds_10min_wind/data")
            .where(F.col("farm_code") == farm.farm_code)
            .where(F.col("year") == y)
        )

        # Skip empty partitions to avoid unnecessary processing
        if cds_10min_wind_df_part.limit(1).count() == 0:
            continue

        # ---------------------------------------------------------
        # 1. Filter rows for ActivePower tag
        # ---------------------------------------------------------
        df = cds_10min_wind_df_part.filter(F.col("telemetry_tag_name") == "ActivePower")

        # ---------------------------------------------------------
        # 2. Fix empty farm_code values using ent_code logic
        # ---------------------------------------------------------
        df = df.withColumn(
            "farm_code",
            F.when(
                (F.col("farm_code").isNull()) | (F.col("farm_code") == ""),
                F.concat(F.lit("WIF_"), F.split(F.col("ent_code"), "_").getItem(1))
            ).otherwise(F.col("farm_code"))
        )

        # ---------------------------------------------------------
        # 3. Extract year and month from timestamp
        # (Note: 'year' already exists as partition but we recompute to ensure consistency)
        # ---------------------------------------------------------
        df = df.withColumn("year", F.year(F.col("timestamp")).cast("string")) \
               .withColumn("month", F.month(F.col("timestamp")).cast("string"))

        # ---------------------------------------------------------
        # 4. Select only the required columns
        # ---------------------------------------------------------
        columns_to_keep = [
            "farm_code", "timestamp", "year", "month", "ent_code",
            "telemetry_tag_name", "val", "is_data_quality_fallback"
        ]
        df = df.select(*columns_to_keep)

        # ---------------------------------------------------------
        # 5. Group by turbine and aggregate values
        # LTSA-169: negative active power (turbine self-consumption, or Telemetry
        # outliers down to -1e7) is forced to 0 BEFORE aggregation — LTSA
        # contracts only count produced energy (LTSA Performance Tracker spec).
        # Nulls stay null (not counted as 0).
        # ---------------------------------------------------------
        df = (
            df.filter(F.col("farm_code") != F.col("ent_code"))
            .withColumn("val", F.when(F.col("val") < 0, F.lit(0.0)).otherwise(F.col("val")))
            .groupBy("farm_code", "ent_code", "year", "month", "telemetry_tag_name")
            .agg(
                F.sum("val").alias("actpow_10min"),
            )
        )

        # ---------------------------------------------------------
        # 6. Append partition result to final dataframe
        # ---------------------------------------------------------
        turbine_production_data_df = df if turbine_production_data_df is None else turbine_production_data_df.unionByName(df)

# COMMAND ----------


cols_to_drop = ["counts", "telemetry_tag_name"]

turbine_production_schema = (
    F.col("ent_code").cast("string"),
    F.col("wind_turbine_code").cast("string"),
    F.col("substation_code").cast("string"),
    F.col("farm_code").cast("string"),
    F.col("year").cast("string"),
    F.col("month").cast("string"),
    F.col("production_kwh").cast("double")
)

turbine_production_df = (
    turbine_production_data_df.filter(F.col("farm_code") != F.col("ent_code"))
    .join(
        ltsa_wind_turbine_df.select("wind_turbine_code", "substation_code"),
        turbine_production_data_df["ent_code"] == ltsa_wind_turbine_df["wind_turbine_code"],
        how="inner"
    ).drop(*cols_to_drop)
    .withColumn("production_kwh", F.col("actpow_10min") / F.lit(6))
    .drop("actpow_10min")
    .select(*turbine_production_schema)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute substation production

# COMMAND ----------

substation_production_df = (
    turbine_production_df.groupBy("substation_code", "farm_code", "year", "month")
    .agg(F.sum("production_kwh").alias("production_kwh"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute farm production

# COMMAND ----------

farm_production_df = (
    substation_production_df.groupBy("farm_code", "year", "month")
    .agg(F.sum("production_kwh").alias("production_kwh"))
)

# COMMAND ----------

turbine_production_table = f"{schema}.turbine_production"

if not spark.catalog.tableExists(turbine_production_table):
    turbine_production_df.writeTo(turbine_production_table)\
        .partitionedBy("year")\
        .create()
else:
    # Partition overwrite — replaces data for the targeted years only
    turbine_production_df.writeTo(turbine_production_table)\
        .overwrite(F.col("year").isin(str(last_year), str(current_year)))

# COMMAND ----------

substation_production_table = f"{schema}.substation_production"

if not spark.catalog.tableExists(substation_production_table):
    substation_production_df.writeTo(substation_production_table)\
        .partitionedBy("year")\
        .create()
else:
    # Partition overwrite — replaces data for the targeted years only
    substation_production_df.writeTo(substation_production_table)\
        .overwrite(F.col("year").isin(str(last_year), str(current_year)))

# COMMAND ----------

farm_production_table = f"{schema}.farm_production"

if not spark.catalog.tableExists(farm_production_table):
    farm_production_df.writeTo(farm_production_table)\
        .partitionedBy("year")\
        .create()
else:
    # Partition overwrite — replaces data for the targeted years only
    farm_production_df.writeTo(farm_production_table)\
        .overwrite(F.col("year").isin(str(last_year), str(current_year)))

# COMMAND ----------

_outputs = {
    "turbine_production":       turbine_production_df,
    "substation_production":      substation_production_df,
    "farm_production":            farm_production_df
}

for _name, _df in _outputs.items():
    _df.write \
        .mode("overwrite") \
        .format("delta") \
        .option("delta.enableDeletionVectors", "false") \
        .option("mergeSchema", "true") \
        .save(f"s3://{gold_bucket}/{_name}/data")
