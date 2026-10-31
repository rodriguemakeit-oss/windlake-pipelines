# Databricks notebook source
# MAGIC %md
# MAGIC # WTG Contracts
# MAGIC
# MAGIC Wind turbine contracts

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")
gold_bucket = dbutils.widgets.get("gold_bucket")
ltsa_scope_regex = dbutils.widgets.get("ltsa_scope_regex")

# COMMAND ----------

maintenance_contract_general = spark.read.format("parquet").load("/Volumes/rfgbu_it_cdh_common_sources/shared_data/telemetrystaticdata/maintenance_contract_general/data")
maintenance_contract_guarantee = spark.read.format("parquet").load("/Volumes/rfgbu_it_cdh_common_sources/shared_data/telemetrystaticdata/maintenance_contract_guarantee/data")
# avoid duplicated rows on flate rate
maintenance_contract_contractual_availability_flate_rate = spark.sql(
  """
  SELECT cf.contract_technical_id, first(cf.unit, true) as unit, first(cf.schedule_preventive_maintenance, true) as schedule_preventive_maintenance, first(cf.flate_rate_until, true) as flate_rate_until
  FROM parquet.`/Volumes/rfgbu_it_cdh_common_sources/shared_data/telemetrystaticdata/maintenance_contract_contractual_availability_flate_rate/data` as cf
  GROUP BY cf.contract_technical_id
  """
)

# COMMAND ----------

import unicodedata

from pyspark.sql.types import StringType

@F.udf(returnType=StringType())
def normalize_text(text):
    if text is None:
        return None
    # Unicode normalization + deleting accents
    text = unicodedata.normalize("NFKD", text)
    text = "".join([c for c in text if not unicodedata.combining(c)])
    return text.lower()

# COMMAND ----------

# if(isNotNull(enforcement_date), contract_enforcement_date, contract_start_date)
mc_general_schema = [
    "substation_code",
    "farm_code",
    "contract_technical_id",
    "maintenance_company",
    "status",
    "start_date",
    "end_date"
]
mc_general = maintenance_contract_general.\
filter("technology = 'Wind'").\
withColumn(
    "start_date", 
    F.when(
        F.col("enforcement_date").isNotNull(),F.col("enforcement_date")).otherwise(F.col("start_date")
    )
).\
withColumn(
    "status_cleaned", normalize_text(F.col("status"))
).\
filter(F.col("status_cleaned").isin('active', 'terminated')).\
drop("status_cleaned").select(mc_general_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare sources
# MAGIC
# MAGIC Build:
# MAGIC - `guarantee_prepped` — one row per guarantee year per contract  
# MAGIC - `mc_general_full` — one row per contract (technology=Wind, status=active|terminated, enforcement_date overrides start_date)

# COMMAND ----------


# Guarantee: keep only the columns needed downstream, with proper types
guarantee_prepped = maintenance_contract_guarantee \
    .select(
        "contract_technical_id",
        F.col("start_date").alias("guarantee_start_date"),
        F.col("end_date").alias("guarantee_end_date"),
        "farm_availability_type",
        F.col("farm_availability").cast("double").alias("farm_availability_guaranteed"),
        F.col("WT_INV_availability_type").alias("turbine_availability_type"),
        F.col("WT_INV_availability").cast("double").alias("turbine_availability_guaranteed"),
    )

# General: full schema with converted dates and status/technology filters
mc_general_full = maintenance_contract_general \
    .filter(F.col("technology") == "Wind") \
    .withColumn("contract_end_date", F.col("end_date")) \
    .withColumn("contract_start_date",
        F.when(F.col("enforcement_date").isNotNull(), F.col("enforcement_date"))
         .otherwise(F.col("start_date"))) \
    .withColumn("_status_norm", normalize_text(F.col("status"))) \
    .filter(F.col("_status_norm").isin("active", "terminated")) \
    .drop("_status_norm")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build `maintenance_substation` and `maintenance_farm`
# MAGIC
# MAGIC Join general × guarantee (one row per guarantee year per contract), clean availability fields, then DISTINCT.

# COMMAND ----------

def clean_availability(df):
    """
    - Replace "None" strings with null in availability columns.
    - Cross-fill: if farm_availability_type is null, use turbine_availability_type (and vice-versa).
      Same cross-fill for the guaranteed percentages.
    """
    for col in ["farm_availability_type", "turbine_availability_type",
                "farm_availability_guaranteed", "turbine_availability_guaranteed"]:
        df = df.withColumn(col,
            F.when(F.lower(F.col(col).cast("string")) == "none", F.lit(None))
             .otherwise(F.col(col)))
    return df \
        .withColumn("farm_availability_type",
            F.coalesce(F.col("farm_availability_type"), F.col("turbine_availability_type"))) \
        .withColumn("turbine_availability_type",
            F.coalesce(F.col("turbine_availability_type"), F.col("farm_availability_type"))) \
        .withColumn("farm_availability_guaranteed",
            F.coalesce(F.col("farm_availability_guaranteed"), F.col("turbine_availability_guaranteed"))) \
        .withColumn("turbine_availability_guaranteed",
            F.coalesce(F.col("turbine_availability_guaranteed"), F.col("farm_availability_guaranteed")))

mc_prepared = clean_availability(
    mc_general_full.join(guarantee_prepped, on="contract_technical_id", how="inner")
)

# ── maintenance_substation ─────────────────────────────────────────────────────
# One distinct row per (farm, substation, contract, guarantee year, status)
SUBSTATION_COLS = [
    "farm_code", "contract_start_date", "contract_end_date", "substation_code",
    "contract_technical_id", "maintenance_company",
    "farm_availability_guaranteed", "turbine_availability_guaranteed",
    "guarantee_start_date", "guarantee_end_date", "status",
]

maintenance_substation_df = mc_prepared \
    .filter(F.col("substation_code").isNotNull() & (F.trim(F.col("substation_code")) != "")) \
    .select(*SUBSTATION_COLS) \
    .distinct()

# ── maintenance_farm ───────────────────────────────────────────────────────────
# Same as substation but collapsed across substations sharing the same contract
FARM_COLS = [c for c in SUBSTATION_COLS if c != "substation_code"]

maintenance_farm_df = mc_prepared \
    .select(*FARM_COLS) \
    .distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build `referential_guarantee_farm`
# MAGIC
# MAGIC Filtered view (contracts active within the last 13 months), grouped to one row per (farm, contract), then keep only the most recent contract per farm within the LTSA scope.

# COMMAND ----------

from pyspark.sql import Window

# Filter: contracts whose window overlaps "today − 13 months → today"
mc_general_filtered = mc_general_full \
    .filter(F.col("contract_end_date") >= F.add_months(F.current_date(), -13)) \
    .filter(F.col("contract_start_date") <= F.current_date())

# Join with guarantee, keep rows with a non-empty substation name
mc_filtered_gg = mc_general_filtered \
    .join(guarantee_prepped, on="contract_technical_id", how="inner") \
    .filter(F.col("substation").isNotNull() & (F.trim(F.col("substation")) != ""))

mc_filtered_prepared = clean_availability(mc_filtered_gg)

# Group by (farm_code, contract_technical_id): collapse guarantee years into one row per contract
mc_by_farm = mc_filtered_prepared.groupBy("farm_code", "contract_technical_id").agg(
    F.first("contract_start_date",  ignorenulls=True).alias("contract_start_date"),
    F.first("contract_end_date",    ignorenulls=True).alias("contract_end_date"),
    F.first("maintenance_company",  ignorenulls=True).alias("maintenance_company"),
    F.first("farm_availability_type",    ignorenulls=True).alias("farm_availability_type"),
    F.first("turbine_availability_type", ignorenulls=True).alias("turbine_availability_type"),
    F.first("maintenance_strategy", ignorenulls=True).alias("maintenance_strategy"),
)

# Keep the most recent contract per farm_code (deterministic tiebreaker on contract_technical_id ASC)
_w = Window.partitionBy("farm_code").orderBy(
    F.col("contract_start_date").desc(),
    F.col("contract_technical_id").asc(),
)

referential_guarantee_farm_df = mc_by_farm \
    .withColumn("_rn", F.row_number().over(_w)) \
    .filter(F.col("_rn") == 1) \
    .drop("_rn") \
    .filter(F.col("farm_code").rlike(ltsa_scope_regex)) \
    .withColumn("substation_availability_type",
        F.coalesce(F.col("turbine_availability_type"), F.col("farm_availability_type"))) \
    .withColumnRenamed("maintenance_company", "maintainer") \
    .select(
        "farm_code", "contract_technical_id",
        "contract_start_date", "contract_end_date",
        "farm_availability_type", "substation_availability_type", "turbine_availability_type",
        "maintainer", "maintenance_strategy",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build `maintenance_contract_scheduled_pm`

# COMMAND ----------

from pyspark.sql import Window as _Window

UNIT_MAPPING = {
    "HoursPerWT": "hours_per_wt", "HoursWT": "hours_per_wt",
    "MwhWT": "mwh_per_wt",
    "HoursPerFarm": "hours_per_farm",
    "Mwh": "mwh_per_farm", "MwhPerFarm": "mwh_per_farm",
}
unit_map_expr = F.create_map([F.lit(x) for pair in UNIT_MAPPING.items() for x in pair])

# Load raw flate_rate (need flate_rate_until column absent from the deduped version)

flate_rate_pm = maintenance_contract_contractual_availability_flate_rate \
    .filter(F.col("unit").isNull() | (F.lower(F.trim(F.col("unit"))) != "percent")) \
    .withColumn("pm_unit", unit_map_expr[F.col("unit")]) \
    .withColumnRenamed("schedule_preventive_maintenance", "pm_budget") \
    .withColumn("is_unlimited",
        F.col("pm_budget").isNull() | F.col("unit").isNull() | (F.trim(F.col("unit")) == "")) \
    .withColumn("rate_type",
        F.when(F.upper(F.trim(F.col("flate_rate_until"))) == "FLATE RATE", F.lit("flate_rate"))
         .otherwise(F.lit("until")))

# Deduplicate: prefer rows with non-null pm_budget
_w_pm = _Window.partitionBy("contract_technical_id").orderBy(F.col("pm_budget").asc_nulls_last())
flate_rate_pm = flate_rate_pm \
    .withColumn("_rn", F.row_number().over(_w_pm)) \
    .filter(F.col("_rn") == 1).drop("_rn") \
    .select("contract_technical_id", "pm_budget", "pm_unit", "is_unlimited", "rate_type")

# Guarantee: one row per (contract, guarantee year)
guarantee_pm = maintenance_contract_guarantee \
    .withColumnRenamed("year", "guarantee_year") \
    .select(
        "contract_technical_id", "guarantee_year",
        F.col("start_date").alias("guarantee_start_date"),
        F.col("end_date").alias("guarantee_end_date"),
    ).distinct()

# General contract base
general_pm = mc_general_full.select(
    "contract_technical_id", "farm_code", "substation_code",
    "maintenance_company", "contract_start_date", "contract_end_date",
).distinct()

# Joins + derivations
maintenance_contract_scheduled_pm_df = general_pm \
    .join(guarantee_pm, on="contract_technical_id", how="left") \
    .join(flate_rate_pm, on="contract_technical_id", how="left") \
    .withColumn("schedule_maintenance_contract_level",
        F.when(F.col("pm_unit").isin("hours_per_wt", "mwh_per_wt"), F.lit("wtg"))
         .otherwise(F.lit("farm"))) \
    .withColumn("is_unlimited", F.coalesce(F.col("is_unlimited"), F.lit(True))) \
    .withColumn("rate_type", F.coalesce(F.col("rate_type"), F.lit("flate_rate"))) \
    .withColumn("guarantee_year", F.coalesce(F.col("guarantee_year").cast("int"), F.lit(-1))) \
    .select(
        "contract_technical_id", "farm_code", "substation_code",
        "maintenance_company", "contract_start_date", "contract_end_date",
        "guarantee_year", "guarantee_start_date", "guarantee_end_date",
        "schedule_maintenance_contract_level", "pm_budget", "pm_unit",
        "is_unlimited", "rate_type",
    )

maintenance_contract_scheduled_pm_df.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(f"{silver_schema}.turbine_scheduled_pm")
maintenance_substation_df.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(f"{silver_schema}.maintenance_substation")
maintenance_farm_df.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(f"{silver_schema}.maintenance_farm")
print("Written: silver.turbine_scheduled_pm, silver.maintenance_substation, silver.maintenance_farm")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to S3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build `maintenance_contract_scheduled_pm`

# COMMAND ----------

_outputs = {
    "maintenance_substation":       maintenance_substation_df,
    "maintenance_farm":             maintenance_farm_df,
    "referential_guarantee_farm":   referential_guarantee_farm_df,
}

for _name, _df in _outputs.items():
    _df.write \
        .mode("overwrite") \
        .format("delta") \
        .option("delta.enableDeletionVectors", "false") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(f"s3://{gold_bucket}/{_name}/data")
