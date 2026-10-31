# Databricks notebook source
# MAGIC %md
# MAGIC ## Cleaning responsible_party + IEC

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
gold_schema = dbutils.widgets.get("gold_schema")
suffix_duration_col = dbutils.widgets.get("suffix_duration_col")
suffix_mwh_col = dbutils.widgets.get("suffix_mwh_col")

# COMMAND ----------

slb_silver = spark.table(f"{catalog}.silver.turbine_slb")

# COMMAND ----------

slb_silver = slb_silver.withColumn(
  "responsible_party", F.when(
      F.col("responsible_party").isNull() | (F.lower(F.col("responsible_party")) != "maintainer"), "other"
    ).otherwise(F.lower(F.col("responsible_party")))
)

slb_silver = slb_silver.withColumn(
  "iec_stop_category_label", F.when(
    F.col("iec_stop_category_label").isNull() | (F.col("iec_stop_category_label") == ""), "IU"
  ).otherwise(F.col("iec_stop_category_label"))
)

# COMMAND ----------

slb_silver_gby_iec_resp = slb_silver.groupby(
  "ent_code",
  "iec_stop_category_label",
  "responsible_party",
  "slb_year",
  "slb_month"
).agg(
  F.sum("stop_duration_min").alias("stop_duration_min"),
  F.sum("loss_value_mwh").alias("loss_value_mwh"),
  F.first("month_time_in_seconds").alias("month_time_in_seconds")
)

# COMMAND ----------

slb_silver_pivot = slb_silver_gby_iec_resp.withColumn(
    "iec_responsible",
    F.concat_ws("_", F.lower(F.col("iec_stop_category_label")), F.lower(F.col("responsible_party")))
)\
.groupby(["ent_code", "slb_year", "slb_month", "month_time_in_seconds"])\
.pivot("iec_responsible").agg(
    F.sum("stop_duration_min").alias("stop_duration_min"),
    F.sum("loss_value_mwh").alias("loss_value_mwh"),
)

# COMMAND ----------

iec_categories = {"IANOPCA", "IANOFO", "IANOSM", "IANOS", "IAFM", "IU", "IAOGFP", "IAOGPP", "IAONGTS", "IAONGEN", "IAONGRS", "IAONGEL"}

responsible_party_simplified = {"Maintainer", "Other"}

units = {suffix_duration_col, suffix_mwh_col}

iec_responsible_col = [
    f"{iec_category.lower()}_{responsible.lower()}_{unit}"
    for iec_category in iec_categories
    for responsible in responsible_party_simplified
    for unit in units
]

missing_iec_responsible_col = [col for col in iec_responsible_col if col not in slb_silver_pivot.columns]

slb_silver_pivot_clean = slb_silver_pivot.select(
    "*",
    *[F.lit(None).cast("double").alias(c) for c in missing_iec_responsible_col]
)
slb_silver_pivot_clean = slb_silver_pivot_clean.fillna(0, subset=iec_responsible_col)
slb_silver_pivot_clean = slb_silver_pivot_clean.withColumnsRenamed(
    {
        "ent_code": "wind_turbine_code",
        "slb_month": "month",
        "slb_year": "year"
    }
)

slb_silver_pivot_clean = slb_silver_pivot_clean.select(sorted(slb_silver_pivot_clean.columns))

# COMMAND ----------

slb_silver_pivot_clean.createOrReplaceTempView("slb_silver_pivot_clean")

# COMMAND ----------

slb_and_production = spark.sql(
    f"""
    SELECT tb.farm_code, tb.substation_code, slb.*, tb.production_kwh
    FROM slb_silver_pivot_clean AS slb
    LEFT JOIN {gold_schema}.turbine_production AS tb
    ON slb.wind_turbine_code = tb.wind_turbine_code
    AND slb.year = tb.year
    AND slb.month = tb.month
    """
)

# COMMAND ----------

slb_and_production.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(f"{gold_schema}.slb_and_production")
