# Databricks notebook source
# MAGIC %md
# MAGIC # LTSA wind turbines scope
# MAGIC This script will build wind_turbine_ltsa_scope dataset

# COMMAND ----------

from utils.kimball import add_id_column
from utils.helpers import reorder_columns

# COMMAND ----------

schema = dbutils.widgets.get("schema")
gold_bucket = dbutils.widgets.get("gold_bucket")
ltsa_scope_regex = dbutils.widgets.get("ltsa_scope_regex")

# Colonnes livrées sur S3 (CDH) = schéma enregistré dans Glue/Athena. Figé pour
# qu'une nouvelle colonne côté Telemetry (ex. substation.max_rated_power, 2026-09-30)
# ne casse pas Athena (INVALID_GLUE_SCHEMA). Les tables UC restent des copies complètes.
S3_WIND_TURBINE_COLS = [
    "wind_turbine_code", "wind_turbine", "substation_code", "substation", "farm_code", "farm",
    "manufacturer", "model", "rated_power", "rotor_diameter", "model_version", "installation_kind",
    "transmission", "transformer_location", "cutin_wind_speed", "cutout_wind_speed",
    "recutin_wind_speed", "serial_number", "legal_authority_id", "standard_mode", "status",
    "hub_height", "wind_class", "hub_height_including_foundation", "tower_material",
    "inspection_frequency", "last_inspection_date", "first_grid_injection_date",
    "decommissioning_date", "has_original_cranage_platform", "original_cranage_platform_area",
    "latitude", "longitude", "altitude", "linked_ppc_code", "linked_ppc_name",
]
S3_SUBSTATION_COLS = [
    "substation_code", "substation", "farm_code", "farm", "global_business_unit",
    "main_source_energy", "main_fuel", "main_fuel_family", "sub_technology", "technology",
    "perform_unit_code", "perform_unit", "other_substation_name", "internal_code", "analytic_code",
    "status", "unit_status", "provisional_acceptance_date", "final_investment_decision_date",
    "notice_to_proceed_date", "acquisition_date", "commissioning_completion_date",
    "first_synchronization_date", "scheduled_commercial_operation_date",
    "commercial_operation_date", "warranty_end_date", "contract_end_date",
    "estimated_strategic_end_date", "strategic_end_date", "economic_end_date",
    "commercial_out_of_service_date", "decommissioning_date", "rated_power",
    "rated_power_without_grid_limitation", "peak_power", "energy_ac_usable_bol",
    "energy_ac_usable_eol", "asset_type", "maintenance_strategy", "operation_strategy",
    "configuration", "structure", "equipment_count", "module_count", "monitoring_type",
    "farm_area", "timezone", "latitude", "longitude", "altitude", "summer_time", "district",
    "state", "city", "zip_code", "address", "region", "country", "continental_region",
    "is_perform_report", "is_gpc_report", "is_grc_unit_input", "grc_asset_group_ID",
    "telemetry_connection_status", "smart_entity_code", "smart_entity_label", "smart_package",
    "asset_currency", "revenue_type_classification", "subfarm_name_for_maintainer",
    "net_own_percentage", "conso_rate_type", "conso_rate_percentage", "conso_meth_type",
    "conso_meth_percentage", "org_id", "datawin_id", "site_id", "site_description_name",
    "ground_coverage_ratio", "perform_report_sent_with_telemetry", "metering_situation",
    "invoicing_situation",
]

# COMMAND ----------

wind_turbine_df = spark.read.format("parquet").load("/Volumes/rfgbu_it_cdh_common_sources/shared_data/telemetrystaticdata/wind_turbine/data")
ltsa_wind_turbine_df = wind_turbine_df.filter(wind_turbine_df.farm_code.rlike(ltsa_scope_regex))

# COMMAND ----------

ltsa_wind_turbine_df.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(f"{schema}.wind_turbine_ltsa_scope")

# COMMAND ----------

ltsa_wind_turbine_df.select(*S3_WIND_TURBINE_COLS).write \
  .mode("overwrite") \
  .format("delta") \
  .option("delta.enableDeletionVectors", "false") \
  .option("mergeSchema", "true") \
  .option("overwriteSchema", "true") \
.save(f"s3://{gold_bucket}/wind_turbine_ltsa_scope/data")

# COMMAND ----------

# MAGIC %md
# MAGIC # Compute dim_farm, dim_substation, dim_turbine

# COMMAND ----------

# Source Telemetry complète = source de vérité. La table `substation` livrée
# (gold + S3) doit en être une copie EXACTE : pas de filtre, mêmes colonnes,
# mêmes lignes, mêmes valeurs.
substation_full = spark.read.format("parquet").load("/Volumes/rfgbu_it_cdh_common_sources/shared_data/telemetrystaticdata/substation/data")

# COMMAND ----------

substation_full.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(f"{schema}.substation")

# COMMAND ----------

substation_full.select(*S3_SUBSTATION_COLS).write \
  .mode("overwrite") \
  .format("delta") \
  .option("delta.enableDeletionVectors", "false") \
  .option("mergeSchema", "true") \
  .option("overwriteSchema", "true") \
.save(f"s3://{gold_bucket}/substation/data")

# COMMAND ----------

# Périmètre LTSA (éolien, 10 pays dont Australie) — sert UNIQUEMENT à construire les dimensions
# ci-dessous (dim_farm / dim_substation / dim_turbine), pas la table livrée.
substation_df = substation_full.filter(substation_full.farm_code.rlike(ltsa_scope_regex))

# COMMAND ----------

dim_farm = reorder_columns(
    df=add_id_column(substation_df.select("farm_code", "farm").distinct()),
    first_cols=["id"]
).withColumnsRenamed({
    "farm_code": "code"
})

dim_farm.createOrReplaceTempView("dim_farm")

# COMMAND ----------

dim_substation_raw = add_id_column(substation_df).withColumnsRenamed({
    "substation_code": "code"
})

# Use a distinct view name to avoid re-resolution issues under Spark Connect:
# if we reused "dim_substation" and then replaced it with the SQL result below,
# the SQL's plan (referencing ds.farm_code) would re-resolve against the new
# (schema-less farm_code) view at saveAsTable time and fail.
dim_substation_raw.createOrReplaceTempView("dim_substation_raw")

dim_substation = spark.sql(
    """
    SELECT ds.*, df.id AS farm_id
    FROM dim_substation_raw as ds
    LEFT JOIN dim_farm as df
    ON ds.farm_code = df.code
    """
).drop("farm_code", "farm")

dim_substation = reorder_columns(df=dim_substation, first_cols=["id", "code", "substation", "farm_id"])

dim_substation.createOrReplaceTempView("dim_substation")

# COMMAND ----------

dim_turbine = add_id_column(ltsa_wind_turbine_df)
dim_turbine = dim_turbine.withColumnsRenamed(
    {
        "wind_turbine_code": "code"
    }
)

dim_turbine.createOrReplaceTempView("dim_turbine")

dim_turbine = spark.sql(
    """
    SELECT dt.*, ds.id AS substation_id, ds.farm_id
    FROM dim_turbine as dt
    LEFT JOIN dim_substation as ds
    ON dt.substation_code = ds.code
    """
).drop("substation_code", "substation", "farm_code", "farm")

dim_turbine = reorder_columns(df=dim_turbine, first_cols=["id", "code", "wind_turbine", "serial_number", "substation_id", "farm_id"])

# COMMAND ----------

dim_farm.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(f"{schema}.dim_farm")
dim_substation.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(f"{schema}.dim_substation")
dim_turbine.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(f"{schema}.dim_turbine")
