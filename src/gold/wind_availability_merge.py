# Databricks notebook source
# MAGIC %md
# MAGIC # Computed + Generative AI availability
# MAGIC
# MAGIC Produit les 3 tables de livraison gold au schéma iso-Dataiku :
# MAGIC - `gold.turbine_availability`
# MAGIC - `gold.substation_availability`
# MAGIC - `gold.farm_availability`
# MAGIC
# MAGIC Sources :
# MAGIC - `gold.fact_turbine_availability` — disponibilités portfolio calculées (wind_compute_availability)
# MAGIC - `gold.fact_genai_turbine/substation/farm_availability` — disponibilités maintainer GenAI
# MAGIC - `gold.dim_turbine / dim_substation / dim_farm` — dimensions pour dénormaliser les codes

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType
from pyspark.sql.window import Window

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
gold_bucket = dbutils.widgets.get("gold_bucket")

# COMMAND ----------


# =============================================================================
# CONTRACT DEDUPLICATION HELPERS — faithful to Dataiku ContractManagement logic
#
# Dataiku rules (compute_keep_one_contract/__init__.py):
#   Rule 1: if EXACTLY ONE contract with status="active" → keep it
#   Rule 2: otherwise → sort by contract_start_date DESC, contract_technical_id ASC
#
# _add_ref_date: builds last day of month as reference date for contract range check
#   (Dataiku shaker: inc(inc(asDate(concat(year,"-",month),"yyyy-MM"),1,"month"),-1,"day"))
# =============================================================================

def _add_ref_date(df):
    """Add _ref_date = last day of (year, month) — matches Dataiku shaker formula."""
    # Use make_date to avoid concat_ws skipping nulls (would produce "01" → CAST error)
    return df.withColumn(
        "_ref_date",
        F.last_day(F.make_date(F.col("year").cast("int"), F.col("month").cast("int"), F.lit(1))),
    )


def _keep_one_contract(df, partition_cols):
    """
    Keep one contract per entity × month × year.

    Priority (mirrors Dataiku ContractManagement.select_one_contract):
      1. If exactly ONE active contract in the partition → it wins
      2. Otherwise → most recent contract_start_date DESC, then contract_technical_id ASC
    """
    w_base = Window.partitionBy(*partition_cols)

    # Count active contracts per partition to apply Rule 1
    df = df.withColumn(
        "_active_count",
        F.count(F.when(F.lower(F.col("_c_status")) == "active", 1)).over(w_base),
    )

    w_order = w_base.orderBy(
        # Rule 1: exactly-one-active gets rank 0, everything else rank 1
        F.when(
            (F.lower(F.col("_c_status")) == "active") & (F.col("_active_count") == 1),
            0,
        ).otherwise(1),
        # Rule 2: most recent contract_start_date first
        F.col("_c_cstart").desc_nulls_last(),
        # Tiebreaker: smallest contract_technical_id (deterministic)
        F.col("_c_contract_id").asc_nulls_last(),
    )

    return (
        df.withColumn("_rn", F.row_number().over(w_order))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_active_count")
    )


def _dedup_genai_latest(df, id_col):
    """
    Keep one GenAI row per (id_col, year, month) — most recent reporting_date,
    then most recent run_date as tie-breaker.

    Mirrors the Dataiku TopN recipe (compute_availability_genai_telemetry_no_match_refresh):
      partitionBy(turbine_id, maintainer_date), orderBy(reporting_date ASC) lastRows=1
    """
    w = Window.partitionBy(id_col, "year", "month").orderBy(
        F.col("reporting_date").desc_nulls_last(),
        F.col("run_date").desc_nulls_last(),
    )
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 1 — turbine_availability

# COMMAND ----------

# Load sources
fact_portfolio   = spark.table(f"{catalog}.gold.fact_turbine_availability")
fact_genai_t = spark.table(f"{catalog}.gold.fact_genai_turbine_availability")
# Miroir du TopN Dataiku : garder le reporting_date le plus récent par (turbine_id, year, month)
fact_genai_t = _dedup_genai_latest(fact_genai_t, "turbine_id")

# --- Enercon FARM availability (synthesised) --------------------------------
# Enercon monthly reports give per-WTG technical (TIME) availability but no
# aggregated FARM value, so gold.fact_genai_farm_availability is empty for them.
# Reconstruct the FARM value as the simple arithmetic mean of the matched WTG
# availabilities per (farm, month), computed on the already-deduplicated
# turbine set above (one row per turbine/month — no blanket distinct).
# Notes:
#   * Enercon only (manufacturer filter) — other maintainers keep their reported
#     FARM value.
#   * No availability type is stored on the GenAI side; Enercon reports are TIME
#     by nature, so this is the TIME farm availability.
#   * Unweighted mean: no per-WTG hours/production are available to weight it.
# The result is applied in Phase 3, and only where the report gave no FARM value.
enercon_farm_maintainer_avail = (
    fact_genai_t
    .filter(F.lower(F.col("manufacturer")).like("%enercon%"))
    .groupBy("farm_id", "year", "month")
    .agg(F.avg(F.col("availability").cast("double")).alias("_enercon_farm_avail"))
    .select(
        F.col("farm_id").alias("_en_farm_id"),
        F.col("year").cast("string").alias("_en_year"),
        F.col("month").cast("string").alias("_en_month"),
        "_enercon_farm_avail",
    )
)

# --- Enercon SUBSTATION availability (synthesised) --------------------------
# Same rationale as the FARM value above, one level down: Enercon reports give
# per-WTG technical (TIME) availability but no aggregated SUBSTATION value, so
# gold.fact_genai_substation_availability is empty for them. Reconstruct the
# SUBSTATION value as the simple arithmetic mean of the matched WTG availabilities
# per (substation, month), on the already-deduplicated turbine set above.
# Same notes apply: Enercon only, TIME by nature, unweighted mean, and applied in
# Phase 2 via coalesce() so a real reported SUBSTATION value is never overwritten.
enercon_substation_maintainer_avail = (
    fact_genai_t
    .filter(F.lower(F.col("manufacturer")).like("%enercon%"))
    .groupBy("substation_id", "year", "month")
    .agg(F.avg(F.col("availability").cast("double")).alias("_enercon_sub_avail"))
    .select(
        F.col("substation_id").alias("_en_sub_id"),
        F.col("year").cast("string").alias("_en_sub_year"),
        F.col("month").cast("string").alias("_en_sub_month"),
        "_enercon_sub_avail",
    )
)

dim_turbine    = spark.table(f"{catalog}.gold.dim_turbine")
dim_substation = spark.table(f"{catalog}.gold.dim_substation")
dim_farm       = spark.table(f"{catalog}.gold.dim_farm")

print(f"fact_turbine_availability (portfolio)  : {fact_portfolio.count()}")
print(f"fact_genai_turbine_availability    : {fact_genai_t.count()}")

# COMMAND ----------

# FULL OUTER JOIN portfolio + GenAI on (turbine_id, year, month)
# run_date: keep the most recent between both sources
joined_turbine = (
    fact_portfolio.withColumnRenamed("run_date", "_run_date_portfolio")
    .join(
        fact_genai_t.select(
            "turbine_id", "year", "month",
            F.col("availability").alias("availability_maintainer"),
            F.lit(None).cast("string").alias("maintenance_company"),
            F.when(F.col("file").isNull(), F.lit(None))
             .when(F.col("file").startswith("/"), F.col("file"))
             .otherwise(F.concat(F.lit("/"), F.col("file"))).alias("file"),
            F.col("run_date").alias("_run_date_genai"),
        ),
        on=["turbine_id", "year", "month"],
        how="full",
    )
    .withColumn("run_date", F.greatest("_run_date_portfolio", "_run_date_genai"))
    .drop("_run_date_portfolio", "_run_date_genai")
)

# Resolve turbine_id → wind_turbine_code, substation_code, farm_code
turbine_delivery = (
    joined_turbine
    .join(
        dim_turbine.select(
            F.col("id").alias("turbine_id"),
            F.col("code").alias("wind_turbine_code"),
            F.col("substation_id").alias("_sub_id"),
            F.col("farm_id").alias("_farm_id"),
        ),
        on="turbine_id",
        how="left",
    )
    .join(
        dim_substation.select(
            F.col("id").alias("_sub_id"),
            F.col("code").alias("substation_code"),
        ),
        on="_sub_id",
        how="left",
    )
    .drop("_sub_id")
    .join(
        dim_farm.select(
            F.col("id").alias("_farm_id"),
            F.col("code").alias("farm_code"),
        ),
        on="_farm_id",
        how="left",
    )
    .drop("_farm_id")
    # computed_availabilities: stored as JSON string → cast to MapType iso Dataiku
    .withColumn(
        "computed_availabilities",
        F.from_json(F.col("computed_availabilities"), MapType(StringType(), StringType()))
    )
    .select(
        "wind_turbine_code",
        "substation_code",
        "farm_code",
        F.col("month").cast("bigint").alias("month"),
        F.col("year").cast("bigint").alias("year"),
        "availability_maintainer",
        F.col("time_availability").alias("availability_portfolio_time"),
        F.col("energy_availability").alias("availability_portfolio_energy"),
        "computed_availabilities",
        "maintenance_company",
        F.lit(None).cast("double").alias("maintainer_guarantee"),
        F.lit("turbine").alias("type"),
        "file",
        "run_date",
    )
)

# Enrich from maintenance_substation — Dataiku: join on substation_code + date in [guarantee_start_date, guarantee_end_date]
# date = last day of month (Dataiku shaker formula), deduplicate with ContractManagement rules
maint_sub = (
    spark.table(f"{catalog}.silver.maintenance_substation")
    .select(
        F.col("substation_code").alias("_c_sub_code"),
        F.col("maintenance_company").alias("_c_maint_company"),
        F.col("turbine_availability_guaranteed").alias("_c_turbine_guarantee"),
        F.col("guarantee_start_date").alias("_c_gstart"),
        F.col("guarantee_end_date").alias("_c_gend"),
        F.col("contract_start_date").alias("_c_cstart"),
        F.col("contract_technical_id").alias("_c_contract_id"),
        F.col("status").alias("_c_status"),
    )
)
turbine_delivery = (
    _add_ref_date(turbine_delivery)
    .join(
        maint_sub,
        (F.col("substation_code") == F.col("_c_sub_code")) &
        (F.col("_ref_date") >= F.col("_c_gstart")) &
        (F.col("_ref_date") <= F.col("_c_gend")),
        how="left",
    )
    .transform(lambda df: _keep_one_contract(df, ["wind_turbine_code", "year", "month"]))
    .withColumn("maintenance_company", F.coalesce(F.col("_c_maint_company"), F.col("maintenance_company")))
    .withColumn("maintainer_guarantee", F.col("_c_turbine_guarantee").cast("double"))
    .drop("_ref_date", "_c_sub_code", "_c_maint_company", "_c_turbine_guarantee", "_c_gstart", "_c_gend", "_c_cstart", "_c_contract_id", "_c_status")
)

turbine_delivery.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(
    f"{catalog}.gold.turbine_availability"
)
print(f"Written gold.turbine_availability: {turbine_delivery.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 2 — substation_availability

# COMMAND ----------

fact_portfolio_s = spark.table(f"{catalog}.gold.fact_substation_availability")
fact_genai_s = spark.table(f"{catalog}.gold.fact_genai_substation_availability")
# Miroir du TopN Dataiku : garder le reporting_date le plus récent par (substation_id, year, month)
fact_genai_s = _dedup_genai_latest(fact_genai_s, "substation_id")
print(f"fact_substation_availability (portfolio) : {fact_portfolio_s.count()}")
print(f"fact_genai_substation_availability   : {fact_genai_s.count()}")

# FULL OUTER JOIN portfolio + GenAI on (substation_id, year, month)
# run_date: keep the most recent between both sources
joined_substation = (
    fact_portfolio_s.withColumnRenamed("run_date", "_run_date_portfolio")
    .join(
        fact_genai_s.select(
            "substation_id", "year", "month",
            F.col("availability").alias("availability_maintainer"),
            F.lit(None).cast("string").alias("maintenance_company"),
            F.when(F.col("file").isNull(), F.lit(None))
             .when(F.col("file").startswith("/"), F.col("file"))
             .otherwise(F.concat(F.lit("/"), F.col("file"))).alias("file"),
            F.col("run_date").alias("_run_date_genai"),
        ),
        on=["substation_id", "year", "month"],
        how="full",
    )
    .withColumn("run_date", F.greatest("_run_date_portfolio", "_run_date_genai"))
    .drop("_run_date_portfolio", "_run_date_genai")
)

# Enercon: fill the SUBSTATION maintainer availability from the per-WTG mean
# (computed in Phase 1) when the report provided no aggregated SUBSTATION value.
# coalesce() only fills nulls, so a real reported SUBSTATION value is never
# overwritten. Mirrors the FARM logic applied in Phase 3.
joined_substation = (
    joined_substation
    .join(
        F.broadcast(enercon_substation_maintainer_avail),
        (F.col("substation_id") == F.col("_en_sub_id"))
        & (F.col("year").cast("string") == F.col("_en_sub_year"))
        & (F.col("month").cast("string") == F.col("_en_sub_month")),
        how="left",
    )
    .withColumn(
        "availability_maintainer",
        F.coalesce(F.col("availability_maintainer"), F.col("_enercon_sub_avail")),
    )
    .drop("_en_sub_id", "_en_sub_year", "_en_sub_month", "_enercon_sub_avail")
)

# Resolve substation_id → substation_code, farm_code
substation_delivery = (
    joined_substation
    .join(
        dim_substation.select(
            F.col("id").alias("substation_id"),
            F.col("code").alias("substation_code"),
            F.col("farm_id").alias("_farm_id"),
        ),
        on="substation_id",
        how="left",
    )
    .join(
        dim_farm.select(
            F.col("id").alias("_farm_id"),
            F.col("code").alias("farm_code"),
        ),
        on="_farm_id",
        how="left",
    )
    .drop("_farm_id")
    .withColumn(
        "computed_availabilities",
        F.from_json(F.col("computed_availabilities"), MapType(StringType(), StringType()))
    )
    .select(
        "substation_code",
        "farm_code",
        F.col("month").cast("bigint").alias("month"),
        F.col("year").cast("bigint").alias("year"),
        "availability_maintainer",
        F.col("time_availability").alias("availability_portfolio_time"),
        F.col("energy_availability").alias("availability_portfolio_energy"),
        "computed_availabilities",
        "maintenance_company",
        F.lit(None).cast("double").alias("maintainer_guarantee"),
        F.lit("substation").alias("type"),
        "file",
        "run_date",
    )
)

# Enrich from maintenance_substation — Dataiku: coalesce(turbine_availability_guaranteed, farm_availability_guaranteed)
maint_sub_2 = (
    spark.table(f"{catalog}.silver.maintenance_substation")
    .select(
        F.col("substation_code").alias("_c_sub_code"),
        F.col("maintenance_company").alias("_c_maint_company"),
        F.coalesce(
            F.col("turbine_availability_guaranteed"),
            F.col("farm_availability_guaranteed"),
        ).alias("_c_guarantee"),
        F.col("guarantee_start_date").alias("_c_gstart"),
        F.col("guarantee_end_date").alias("_c_gend"),
        F.col("contract_start_date").alias("_c_cstart"),
        F.col("contract_technical_id").alias("_c_contract_id"),
        F.col("status").alias("_c_status"),
    )
)
substation_delivery = (
    _add_ref_date(substation_delivery)
    .join(
        maint_sub_2,
        (F.col("substation_code") == F.col("_c_sub_code")) &
        (F.col("_ref_date") >= F.col("_c_gstart")) &
        (F.col("_ref_date") <= F.col("_c_gend")),
        how="left",
    )
    .transform(lambda df: _keep_one_contract(df, ["substation_code", "year", "month"]))
    .withColumn("maintenance_company", F.coalesce(F.col("_c_maint_company"), F.col("maintenance_company")))
    .withColumn("maintainer_guarantee", F.col("_c_guarantee").cast("double"))
    .drop("_ref_date", "_c_sub_code", "_c_maint_company", "_c_guarantee", "_c_gstart", "_c_gend", "_c_cstart", "_c_contract_id", "_c_status")
)

substation_delivery.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(
    f"{catalog}.gold.substation_availability"
)
print(f"Written gold.substation_availability: {substation_delivery.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 3 — farm_availability

# COMMAND ----------

fact_portfolio_f = spark.table(f"{catalog}.gold.fact_farm_availability")
fact_genai_f = spark.table(f"{catalog}.gold.fact_genai_farm_availability")
# Miroir du TopN Dataiku : garder le reporting_date le plus récent par (farm_id, year, month)
fact_genai_f = _dedup_genai_latest(fact_genai_f, "farm_id")
print(f"fact_farm_availability (portfolio) : {fact_portfolio_f.count()}")
print(f"fact_genai_farm_availability   : {fact_genai_f.count()}")

# FULL OUTER JOIN portfolio + GenAI on (farm_id, year, month)
# run_date: keep the most recent between both sources
joined_farm = (
    fact_portfolio_f.withColumnRenamed("run_date", "_run_date_portfolio")
    .join(
        fact_genai_f.select(
            "farm_id", "year", "month",
            F.col("availability").alias("availability_maintainer"),
            F.lit(None).cast("string").alias("maintenance_company"),
            F.when(F.col("file").isNull(), F.lit(None))
             .when(F.col("file").startswith("/"), F.col("file"))
             .otherwise(F.concat(F.lit("/"), F.col("file"))).alias("file"),
            F.col("run_date").alias("_run_date_genai"),
        ),
        on=["farm_id", "year", "month"],
        how="full",
    )
    .withColumn("run_date", F.greatest("_run_date_portfolio", "_run_date_genai"))
    .drop("_run_date_portfolio", "_run_date_genai")
)

# Enercon: fill the FARM maintainer availability from the per-WTG mean (computed
# in Phase 1) when the report provided no aggregated FARM value. coalesce() only
# fills nulls, so a real reported FARM value is never overwritten.
joined_farm = (
    joined_farm
    .join(
        F.broadcast(enercon_farm_maintainer_avail),
        (F.col("farm_id") == F.col("_en_farm_id"))
        & (F.col("year").cast("string") == F.col("_en_year"))
        & (F.col("month").cast("string") == F.col("_en_month")),
        how="left",
    )
    .withColumn(
        "availability_maintainer",
        F.coalesce(F.col("availability_maintainer"), F.col("_enercon_farm_avail")),
    )
    .drop("_en_farm_id", "_en_year", "_en_month", "_enercon_farm_avail")
)

# Resolve farm_id → farm_code
farm_delivery = (
    joined_farm
    .join(
        dim_farm.select(
            F.col("id").alias("farm_id"),
            F.col("code").alias("farm_code"),
        ),
        on="farm_id",
        how="left",
    )
    .withColumn(
        "computed_availabilities",
        F.from_json(F.col("computed_availabilities"), MapType(StringType(), StringType()))
    )
    .select(
        "farm_code",
        F.col("month").cast("bigint").alias("month"),
        F.col("year").cast("bigint").alias("year"),
        "availability_maintainer",
        F.col("time_availability").alias("availability_portfolio_time"),
        F.col("energy_availability").alias("availability_portfolio_energy"),
        "computed_availabilities",
        "maintenance_company",
        F.lit(None).cast("double").alias("maintainer_guarantee"),
        F.lit("site").alias("type"),
        "file",
        "run_date",
    )
)

# Enrich from maintenance_farm — last day of month reference, deduplicate with ContractManagement rules
maint_farm = (
    spark.table(f"{catalog}.silver.maintenance_farm")
    .select(
        F.col("farm_code").alias("_c_farm_code"),
        F.col("maintenance_company").alias("_c_maint_company"),
        F.col("farm_availability_guaranteed").alias("_c_farm_guarantee"),
        F.col("guarantee_start_date").alias("_c_gstart"),
        F.col("guarantee_end_date").alias("_c_gend"),
        F.col("contract_start_date").alias("_c_cstart"),
        F.col("contract_technical_id").alias("_c_contract_id"),
        F.col("status").alias("_c_status"),
    )
)
farm_delivery = (
    _add_ref_date(farm_delivery)
    .join(
        maint_farm,
        (F.col("farm_code") == F.col("_c_farm_code")) &
        (F.col("_ref_date") >= F.col("_c_gstart")) &
        (F.col("_ref_date") <= F.col("_c_gend")),
        how="left",
    )
    .transform(lambda df: _keep_one_contract(df, ["farm_code", "year", "month"]))
    .withColumn("maintenance_company", F.coalesce(F.col("_c_maint_company"), F.col("maintenance_company")))
    .withColumn("maintainer_guarantee", F.col("_c_farm_guarantee").cast("double"))
    .drop("_ref_date", "_c_farm_code", "_c_maint_company", "_c_farm_guarantee", "_c_gstart", "_c_gend", "_c_cstart", "_c_contract_id", "_c_status")
)

farm_delivery.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(
    f"{catalog}.gold.farm_availability"
)
print(f"Written gold.farm_availability: {farm_delivery.count()} rows")

# COMMAND ----------


_outputs = {
    "turbine_availability":       turbine_delivery,
    "substation_availability":      substation_delivery,
    "farm_availability":            farm_delivery
}

for _name, _df in _outputs.items():
    _df.write \
        .mode("overwrite") \
        .format("delta") \
        .option("delta.enableDeletionVectors", "false") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(f"s3://{gold_bucket}/{_name}/data")
