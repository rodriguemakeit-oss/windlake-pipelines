# Databricks notebook source
# MAGIC %md
# MAGIC # PM Budget Tracker — PowerBI-friendly view
# MAGIC
# MAGIC Inputs:
# MAGIC - `{catalog}.gold.ianosm_budget_tracker` — produit par `wind_availability_compute`
# MAGIC
# MAGIC Outputs:
# MAGIC - UC table : `{catalog}.gold.pm_budget_tracker`
# MAGIC - CDH (S3) : `s3://{gold_bucket}/pm_budget_tracker/data`
# MAGIC
# MAGIC Grain: 1 row per (farm/substation/turbine, year, month, contract_id).
# MAGIC Parent dimensions are always filled (enriched via dim_turbine / dim_substation / dim_farm):
# MAGIC  - `WIF_*` (farm-level contract)      → farm filled, substation NULL, turbine NULL
# MAGIC  - `WIS_*` (substation-level contract)→ farm + substation filled, turbine NULL
# MAGIC  - `WIT_*` (turbine-level contract)   → farm + substation + turbine all filled

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalog", "ltsa_dev")
dbutils.widgets.text("run_date", "")
dbutils.widgets.text("gold_bucket", "")

CATALOG = dbutils.widgets.get("catalog")
GOLD_BUCKET = dbutils.widgets.get("gold_bucket")

# COMMAND ----------

src = spark.table(f"{CATALOG}.gold.ianosm_budget_tracker")

# COMMAND ----------

# Split unique `code` column into 3 separate columns (farm / substation / turbine)
# enriched via the dim_* hierarchy so parents are always filled:
#  - WIF_* (farm contract)      → farm
#  - WIS_* (substation contract)→ substation + parent farm  (via dim_substation.farm_id)
#  - WIT_* (turbine contract)   → turbine + parent substation + parent farm
#                                 (via dim_turbine.substation_id / farm_id)
dim_turbine = spark.table(f"{CATALOG}.gold.dim_turbine")
dim_substation = spark.table(f"{CATALOG}.gold.dim_substation")
dim_farm = spark.table(f"{CATALOG}.gold.dim_farm")

# turbine_code → (substation_code, farm_code)
turbine_lookup = (
    dim_turbine.select("code", "substation_id", "farm_id")
    .join(
        dim_substation.select(F.col("id").alias("_s_id"), F.col("code").alias("_t_substation")),
        F.col("substation_id") == F.col("_s_id"),
        "left",
    )
    .join(
        dim_farm.select(F.col("id").alias("_f_id"), F.col("code").alias("_t_farm")),
        F.col("farm_id") == F.col("_f_id"),
        "left",
    )
    .select(F.col("code").alias("_t_code"), "_t_substation", "_t_farm")
)

# substation_code → farm_code
substation_lookup = (
    dim_substation.select("code", "farm_id")
    .join(
        dim_farm.select(F.col("id").alias("_f_id"), F.col("code").alias("_s_farm")),
        F.col("farm_id") == F.col("_f_id"),
        "left",
    )
    .select(F.col("code").alias("_s_code"), "_s_farm")
)

src_enriched = (
    src
    .join(turbine_lookup, F.col("code") == F.col("_t_code"), "left")
    .join(substation_lookup, F.col("code") == F.col("_s_code"), "left")
)

farm_expr = F.coalesce(
    F.when(F.col("code").startswith("WIF_"), F.col("code")),
    F.col("_t_farm"),       # WIT_* → parent farm via dim_turbine.farm_id
    F.col("_s_farm"),       # WIS_* → parent farm via dim_substation.farm_id
)

substation_expr = F.coalesce(
    F.when(F.col("code").startswith("WIS_"), F.col("code")),
    F.col("_t_substation"), # WIT_* → parent substation via dim_turbine.substation_id
)

turbine_expr = F.when(
    F.col("code").startswith("WIT_"), F.col("code")
).otherwise(F.lit(None).cast("string"))

df_out = (
    src_enriched
    .withColumn("farm", farm_expr)
    .withColumn("substation", substation_expr)
    .withColumn("turbine", turbine_expr)
    .select(
        F.col("farm"),
        F.col("substation"),
        F.col("turbine"),
        F.col("year"),
        F.col("month"),
        F.col("contract_technical_id").alias("contract_id"),
        F.col("schedule_maintenance_contract_level").alias("contract_level"),
        F.col("guarantee_start_date"),
        F.col("guarantee_end_date"),
        F.col("pm_budget").alias("ianosm_budget_guarantee_period"),
        F.col("pm_unit").alias("ianosm_budget_unit"),
        F.col("ianosm_m_hours").alias("ianosm_consumed_hours_this_month"),
        F.col("ianosm_m_mwh").alias("ianosm_consumed_mwh_this_month"),
        F.col("ianosm_m_above_cap_hours").alias("ianosm_above_cap_hours_this_month"),
        F.col("ianosm_m_above_cap_mwh").alias("ianosm_above_cap_mwh_this_month"),
        F.col("ianosm_m_hours_cumsum").alias("ianosm_consumed_hours_guarantee_period_so_far"),
        (F.col("ianosm_m_mwh_above_cap_cumsum") + F.col("ianosm_m_mwh_below_cap_cumsum")).alias("ianosm_consumed_mwh_guarantee_period_so_far"),
        F.col("ianosm_m_hours_above_cap_cumsum").alias("ianosm_above_cap_hours_guarantee_period_so_far"),
        F.col("ianosm_m_mwh_above_cap_cumsum").alias("ianosm_above_cap_mwh_guarantee_period_so_far"),
        F.col("ianosm_m_hours_below_cap_cumsum").alias("ianosm_below_cap_hours_guarantee_period_so_far"),
        F.col("ianosm_m_mwh_below_cap_cumsum").alias("ianosm_below_cap_mwh_guarantee_period_so_far"),
    )
    .withColumn(
        "is_above_cap",
        (F.col("ianosm_above_cap_hours_guarantee_period_so_far") > 0)
        | (F.col("ianosm_above_cap_mwh_guarantee_period_so_far") > 0),
    )
)

# COMMAND ----------

df_out.filter('is_above_cap = true').show(3, truncate=False)

# COMMAND ----------

target_uc = f"{CATALOG}.gold.pm_budget_tracker"
df_out.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_uc)
print(f"Written: {target_uc}")

# COMMAND ----------

if GOLD_BUCKET:
    s3_path = f"s3://{GOLD_BUCKET}/pm_budget_tracker/data"
    df_out.write \
        .mode("overwrite") \
        .format("delta") \
        .option("delta.enableDeletionVectors", "false") \
        .option("overwriteSchema", "true") \
        .option("mergeSchema", "true") \
        .save(s3_path)
    print(f"Written: {s3_path}")
else:
    print("GOLD_BUCKET empty -> skipping S3 export")
