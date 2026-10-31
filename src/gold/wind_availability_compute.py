# Databricks notebook source
# =============================================================================
# IMPORTS
# =============================================================================
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from utils.turbine_availability import (
    safe_col,
    calculate_availabilities,
    calculate_v2_availabilities,
    aggregate_and_calculate,
    build_computed_availabilities_json,
    schedule_maintenance_prepared_model,
    compute_iec_above_cap_algorithm_by_turbine,
    compute_iec_above_cap_algorithm_by_farm,
    compute_schedule_maintenance_at_higher_level,
)

# COMMAND ----------

# =============================================================================
# VARIABLES
# =============================================================================
dbutils.widgets.text("catalog", "ltsa_dev")
dbutils.widgets.text("suffix_duration_col", "stop_duration_min")
dbutils.widgets.text("suffix_mwh_col", "loss_value_mwh")

catalog = dbutils.widgets.get("catalog")
suffix_duration_col = dbutils.widgets.get("suffix_duration_col")
suffix_mwh_col = dbutils.widgets.get("suffix_mwh_col")

# COMMAND ----------

slb_production_gold = spark.table(f"{catalog}.gold.slb_and_production")
scheduled_pm_silver = spark.table(f"{catalog}.silver.turbine_scheduled_pm")

# COMMAND ----------

# =============================================================================
# LOAD DIMENSION REFERENCES (for Kimball FK resolution)
# =============================================================================
dim_turbine_ref = spark.table(f"{catalog}.gold.dim_turbine").select(
    F.col("id").alias("turbine_id"), F.col("code").alias("_wt_code")
)
dim_substation_ref = spark.table(f"{catalog}.gold.dim_substation").select(
    F.col("id").alias("substation_id"), F.col("code").alias("_sub_code")
)
dim_farm_ref = spark.table(f"{catalog}.gold.dim_farm").select(
    F.col("id").alias("farm_id"), F.col("code").alias("_farm_code")
)

# COMMAND ----------

# Add run_date column to slb_production_gold
slb_production_gold = slb_production_gold.withColumn("run_date", F.current_timestamp())

# Remove rows where farm_code is empty
df = slb_production_gold.filter(F.col("farm_code").isNotNull() & (F.col("farm_code") != ""))

# COMMAND ----------

# =============================================================================
# PREPARE PM REFERENCE DATA
# =============================================================================
print("Preparing PM reference data...")
# PM columns to keep
pm_cols = [
    "contract_technical_id",
    "substation_code",
    "farm_code",
    "maintenance_company",
    "guarantee_year",
    "contract_start_date",
    "guarantee_start_date",
    "guarantee_end_date",
    "schedule_maintenance_contract_level",
    "pm_budget",
    "pm_unit",
    "is_unlimited",
    "rate_type",
]

# Filter PM ref: drop rows with no guarantee dates
pm_ref_filtered = scheduled_pm_silver.select(pm_cols).filter(
    F.col("guarantee_start_date").isNotNull() & F.col("guarantee_end_date").isNotNull()
)

# COMMAND ----------

# Columns to use for the join (no farm_code to avoid conflict)
pm_join_cols = [
    "contract_technical_id",
    "substation_code",
    "maintenance_company",
    "guarantee_year",
    "guarantee_start_date",
    "guarantee_end_date",
    "schedule_maintenance_contract_level",
    "pm_budget",
    "pm_unit",
    "is_unlimited",
    "rate_type",
]

# Keep one row per (substation_code, guarantee_start_date): most recent contract_start_date first
w_pm = Window.partitionBy("substation_code", "guarantee_start_date").orderBy(
    F.col("contract_start_date").desc()
)

pm_for_join = (
    pm_ref_filtered
    .withColumn("_rn", F.row_number().over(w_pm))
    .filter(F.col("_rn") == 1)
    .drop("_rn", "farm_code")
    .select(pm_join_cols)
)

# COMMAND ----------

# Build slb_date from year / month for the date-window join
df = df.withColumn(
    "slb_date",
    F.to_date(
        F.concat_ws("-", F.col("year").cast("int"), F.col("month").cast("int"), F.lit(1)),
        "yyyy-M-d",
    ),
)

# Left join then filter to keep only rows within the guarantee window
df = df.join(pm_for_join, on="substation_code", how="left").filter(
    F.col("guarantee_start_date").isNotNull()
    & F.col("guarantee_end_date").isNotNull()
    & (F.col("slb_date") >= F.col("guarantee_start_date"))
    & (F.col("slb_date") <= F.col("guarantee_end_date"))
)

# Substations without any matching contract → is_unlimited = True
df = df.withColumn(
    "is_unlimited",
    F.when(F.col("is_unlimited").isNull(), F.lit(True)).otherwise(F.col("is_unlimited")),
)

print(f"Turbine data after PM join: {df.count()} rows")

# COMMAND ----------

# =============================================================================
# DETERMINE CASE @A vs @B PER FARM
# @A: all substations of a farm have the same maintenance_company → v2 possible at farm level
# @B: different maintenance_companies → v1 & v2 = null at farm level
# NOTE: some n_maintainers = 0 (ex: WIF_PTWFA) → farms without contract or with dates 1970
# =============================================================================
farm_maintainer_count = (
    df.groupBy("farm_code")
    .agg(F.countDistinct("maintenance_company").alias("n_maintainers"))
)

df = df.join(farm_maintainer_count, on="farm_code", how="left")

# COMMAND ----------

# =============================================================================
# COMPUTE IANOSM HOURS / MWH PER TURBINE
# =============================================================================
print("Computing IANOSM above cap...")

dur_col_ianosm = "ianosm_maintainer_" + suffix_duration_col  # minutes
mwh_col_ianosm = "ianosm_maintainer_" + suffix_mwh_col

# Convert minutes → hours for tracking
df = (
    df
    .withColumn("ianosm_m_hours", safe_col(dur_col_ianosm) / 60)
    .withColumn("ianosm_m_mwh", safe_col(mwh_col_ianosm))
)

# COMMAND ----------

# =============================================================================
# COMPUTE SCHEDULE MAINTENANCE — ALGO 2: Contract per WTG
# =============================================================================

# wt_columns: columns passed to the algorithm (no slb_date needed, built inside the function)
wt_columns = [
    "wind_turbine_code",
    "substation_code",
    "farm_code",
    "month",
    "year",
    "slb_date",
    "contract_technical_id",
    "guarantee_year",
    "guarantee_start_date",
    "guarantee_end_date",
    "schedule_maintenance_contract_level",
    "pm_budget",
    "pm_unit",
    "is_unlimited",
    "rate_type",
    "n_maintainers",
    "ianosm_m_hours",
    "ianosm_m_mwh",
]

# Mask: WTG-level contract, not unlimited.
# Also restrict to real WTG entities — slb_and_production may contain rows
# whose `wind_turbine_code` is actually a substation (SUT_*) or farm (WIF_*)
# aggregate from the source SLB report. Without this filter, those aggregates
# would be treated as turbines, polluting ianosm_budget_tracker and
# pm_budget_tracker (rows with contract_level=wtg but no parent turbine).
wtg_mask = (
    (F.col("schedule_maintenance_contract_level") == "wtg")
    & (~F.col("is_unlimited"))
    & F.col("wind_turbine_code").startswith("WIT_")
)

# --- Schedule maintenance at turbine level ---
wt_schedule_maintenance = compute_iec_above_cap_algorithm_by_turbine(
    data=df,
    mask=wtg_mask,
    sort_values=["wind_turbine_code", "year", "month"],
    groupby_keys=["wind_turbine_code", "substation_code", "farm_code", "contract_technical_id", "guarantee_year"],
    groupby_selected_columns=wt_columns,
)

farm_columns = [
    "farm_code",
    "month",
    "year",
    "slb_date",
    "contract_technical_id",
    "guarantee_year",
    "guarantee_start_date",
    "guarantee_end_date",
    "schedule_maintenance_contract_level",
    "pm_budget",
    "pm_unit",
    "is_unlimited",
    "rate_type",
    "n_maintainers",
    "ianosm_m_hours",
    "ianosm_m_mwh",
    "ianosm_m_above_cap_hours",
    "ianosm_m_above_cap_mwh",
    "ianosm_m_current_hours_cumsum",
    "ianosm_m_hours_above_cap_cumsum",
    "prev_ianosm_m_hours_above_cap_cumsum",
    "ianosm_m_current_mwh_cumsum",
    "ianosm_m_mwh_above_cap_cumsum",
    "prev_ianosm_m_mwh_above_cap_cumsum",
    "ianosm_m_mwh_below_cap_cumsum",
    "prev_ianosm_m_mwh_below_cap_cumsum",
    "ianosm_m_below_cap_mwh",
    "ianosm_m_hours_cumsum",
    "ianosm_m_hours_below_cap_cumsum",
    "prev_ianosm_m_hours_below_cap_cumsum",
    "ianosm_m_below_cap_hours",
]

# --- Schedule maintenance aggregated at farm level ---
farm_schedule_maintenance = compute_schedule_maintenance_at_higher_level(
    wt_schedule_maintenance=wt_schedule_maintenance,
    columns=farm_columns,
    groupby_keys=["farm_code", "contract_technical_id", "guarantee_year"],
    aggregation_groupby_keys=["farm_code", "year", "month"],
)

# --- Schedule maintenance aggregated at substation level ---
substation_schedule_maintenance = compute_schedule_maintenance_at_higher_level(
    wt_schedule_maintenance=wt_schedule_maintenance,
    columns=["substation_code"] + farm_columns,
    groupby_keys=["substation_code", "farm_code", "contract_technical_id", "guarantee_year"],
    aggregation_groupby_keys=["substation_code", "farm_code", "year", "month"],
)

# COMMAND ----------

# =============================================================================
# PREPARE IANOSM BUDGET TRACKER — WTG algo (Algo 2)
# =============================================================================
tracker_schema = [
    "code",
    "month",
    "year",
    "contract_technical_id",
    "guarantee_year",
    "guarantee_start_date",
    "guarantee_end_date",
    "schedule_maintenance_contract_level",
    "pm_budget",
    "pm_unit",
    "is_unlimited",
    "rate_type",
    "n_maintainers",
    "ianosm_m_hours",
    "ianosm_m_mwh",
    "ianosm_m_above_cap_hours",
    "ianosm_m_above_cap_mwh",
    "ianosm_m_current_hours_cumsum",
    "ianosm_m_hours_above_cap_cumsum",
    "prev_ianosm_m_hours_above_cap_cumsum",
    "ianosm_m_current_mwh_cumsum",
    "ianosm_m_mwh_above_cap_cumsum",
    "prev_ianosm_m_mwh_above_cap_cumsum",
    "ianosm_m_mwh_below_cap_cumsum",
    "prev_ianosm_m_mwh_below_cap_cumsum",
    "ianosm_m_below_cap_mwh",
    "ianosm_m_hours_cumsum",
    "ianosm_m_hours_below_cap_cumsum",
    "prev_ianosm_m_hours_below_cap_cumsum",
    "ianosm_m_below_cap_hours",
]

wt_tracker = schedule_maintenance_prepared_model(
    data=wt_schedule_maintenance,
    rename_columns={"wind_turbine_code": "code"},
    schema=tracker_schema,
    pic_type="turbine",
)

farm_tracker = schedule_maintenance_prepared_model(
    data=farm_schedule_maintenance,
    rename_columns={"farm_code": "code"},
    schema=tracker_schema,
    pic_type="farm",
)

substation_tracker = schedule_maintenance_prepared_model(
    data=substation_schedule_maintenance,
    rename_columns={"substation_code": "code"},
    schema=tracker_schema,
    pic_type="substation",
)

ianosm_budget_tracker_updated = wt_tracker.union(substation_tracker).union(farm_tracker)

# COMMAND ----------

# =============================================================================
# COMPUTE SCHEDULE MAINTENANCE — ALGO 1: Contract per FARM
# For farm-level contracts, correction is at farm level.
# At turbine level → above_cap stays 0. Correction applied after aggregation.
# =============================================================================

# Aggregate df to farm level for algo 1
df_farm_agg = df.groupBy("farm_code", "contract_technical_id", "month", "year").agg(
    F.first("guarantee_year").alias("guarantee_year"),
    F.first("guarantee_start_date").alias("guarantee_start_date"),
    F.first("guarantee_end_date").alias("guarantee_end_date"),
    F.first("slb_date").alias("slb_date"),
    F.first("schedule_maintenance_contract_level").alias("schedule_maintenance_contract_level"),
    F.first("pm_budget").alias("pm_budget"),
    F.first("pm_unit").alias("pm_unit"),
    F.first("is_unlimited").alias("is_unlimited"),
    F.first("rate_type").alias("rate_type"),
    F.first("n_maintainers").alias("n_maintainers"),
    F.sum("ianosm_m_hours").alias("ianosm_m_hours"),
    F.sum("ianosm_m_mwh").alias("ianosm_m_mwh"),
)

farm_mask = (
    (F.col("schedule_maintenance_contract_level") == "farm") & (~F.col("is_unlimited"))
)

# wt_columns[2:] → skip wind_turbine_code and substation_code (farm algo)
farm_algo_columns = wt_columns[2:]

farm_schedule_maintenance_farm_algorithm = compute_iec_above_cap_algorithm_by_farm(
    data=df_farm_agg,
    mask=farm_mask,
    sort_values=["farm_code", "year", "month"],
    groupby_keys=["farm_code", "contract_technical_id", "guarantee_year"],
    groupby_selected_columns=farm_algo_columns,
)

# Add farm algo results to tracker
farm_algo_tracker = schedule_maintenance_prepared_model(
    data=farm_schedule_maintenance_farm_algorithm,
    rename_columns={"farm_code": "code"},
    schema=tracker_schema,
    pic_type="farm",
)

ianosm_budget_tracker_updated = ianosm_budget_tracker_updated.union(farm_algo_tracker)

# COMMAND ----------

farm_schedule_maintenance_farm_algorithm.write \
    .mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog}.gold.debug_pm_farm_before_redist")

# COMMAND ----------

# =============================================================================
# REDISTRIBUTE FARM ABOVE_CAP TO TURBINE LEVEL — LTSA-150
# For per_farm contracts, above_cap is computed at farm level only.
# We redistribute: above_cap_farm / n_turbines → each turbine in that farm,
# so that calculate_v2_availabilities() at turbine level picks up the right value.
# =============================================================================

n_wt_per_farm = (
    df
    .filter(F.col("schedule_maintenance_contract_level") == "farm")
    .select("wind_turbine_code", "farm_code", "contract_technical_id", "month", "year")
    .distinct()
    .groupBy("farm_code", "contract_technical_id", "month", "year")
    .agg(F.count("*").alias("n_turbines"))
)

farm_redist = (
    farm_schedule_maintenance_farm_algorithm
    .join(n_wt_per_farm, on=["farm_code", "contract_technical_id", "month", "year"], how="left")
    .withColumn("n_turbines", F.coalesce(F.col("n_turbines"), F.lit(1)))
    .select(
        F.col("farm_code"),
        F.col("contract_technical_id"),
        F.col("month"),
        F.col("year"),
        (F.col("ianosm_m_above_cap_hours") / F.col("n_turbines")).alias("ianosm_m_above_cap_hours"),
        (F.col("ianosm_m_above_cap_mwh") / F.col("n_turbines")).alias("ianosm_m_above_cap_mwh"),
        (F.col("ianosm_m_below_cap_hours") / F.col("n_turbines")).alias("ianosm_m_below_cap_hours"),
        (F.col("ianosm_m_below_cap_mwh") / F.col("n_turbines")).alias("ianosm_m_below_cap_mwh"),
    )
)

farm_per_turbine_tracker = (
    df
    .filter(F.col("schedule_maintenance_contract_level") == "farm")
    .select("wind_turbine_code", "farm_code", "contract_technical_id", "month", "year")
    .distinct()
    .join(farm_redist, on=["farm_code", "contract_technical_id", "month", "year"], how="left")
    .select(
        F.col("wind_turbine_code").alias("code"),
        F.col("month").alias("tracker_month"),
        F.col("year").alias("tracker_year"),
        "ianosm_m_above_cap_hours",
        "ianosm_m_above_cap_mwh",
        "ianosm_m_below_cap_hours",
        "ianosm_m_below_cap_mwh",
    )
)

# COMMAND ----------

# =============================================================================
# ADD PER-FARM TURBINE ROWS TO ianosm_budget_tracker (LTSA-150 follow-up)
# LTSA-150 fed farm_per_turbine_tracker into the availability join only.
# Fix: also union it (full tracker_schema) into ianosm_budget_tracker_updated,
# so WIT rows for per_farm contracts appear in ianosm_budget_tracker.
# pm_budget: equal split (farm_budget / n_turbines).
# cap/cumsum hours cols: proportional to turbine's share of farm ianosm_m_hours.
# cap/cumsum mwh cols:   proportional to turbine's share of farm ianosm_m_mwh.
# ianosm_m_hours / ianosm_m_mwh = individual turbine values from df.
# =============================================================================

farm_turbine_hours = (
    df
    .filter((F.col("schedule_maintenance_contract_level") == "farm") & (~F.col("is_unlimited")))
    .groupBy("wind_turbine_code", "farm_code", "contract_technical_id", "month", "year")
    .agg(
        F.sum("ianosm_m_hours").alias("ianosm_m_hours"),
        F.sum("ianosm_m_mwh").alias("ianosm_m_mwh"),
    )
)

farm_totals = (
    farm_turbine_hours
    .groupBy("farm_code", "contract_technical_id", "month", "year")
    .agg(
        F.sum("ianosm_m_hours").alias("_farm_total_hours"),
        F.sum("ianosm_m_mwh").alias("_farm_total_mwh"),
    )
)

farm_turbine_with_shares = (
    farm_turbine_hours
    .join(farm_totals, on=["farm_code", "contract_technical_id", "month", "year"], how="left")
    .withColumn("_hours_share",
        F.when(F.col("_farm_total_hours") > 0, F.col("ianosm_m_hours") / F.col("_farm_total_hours"))
        .otherwise(F.lit(0.0))
    )
    .withColumn("_mwh_share",
        F.when(F.col("_farm_total_mwh") > 0, F.col("ianosm_m_mwh") / F.col("_farm_total_mwh"))
        .otherwise(F.lit(0.0))
    )
    .drop("_farm_total_hours", "_farm_total_mwh")
)

farm_algo_for_wt = (
    farm_schedule_maintenance_farm_algorithm
    .join(n_wt_per_farm, on=["farm_code", "contract_technical_id", "month", "year"], how="left")
    .withColumn("_n", F.coalesce(F.col("n_turbines"), F.lit(1)))
    .withColumn("pm_budget", F.col("pm_budget") / F.col("_n"))
    .drop("_n", "n_turbines", "ianosm_m_hours", "ianosm_m_mwh")
)

_hours_cap_cols = [
    "ianosm_m_above_cap_hours", "ianosm_m_below_cap_hours",
    "ianosm_m_hours_above_cap_cumsum", "prev_ianosm_m_hours_above_cap_cumsum",
    "ianosm_m_hours_below_cap_cumsum", "prev_ianosm_m_hours_below_cap_cumsum",
    "ianosm_m_hours_cumsum",
]
_mwh_cap_cols = [
    "ianosm_m_above_cap_mwh", "ianosm_m_below_cap_mwh",
    "ianosm_m_mwh_above_cap_cumsum", "prev_ianosm_m_mwh_above_cap_cumsum",
    "ianosm_m_mwh_below_cap_cumsum", "prev_ianosm_m_mwh_below_cap_cumsum",
]

_joined = farm_turbine_with_shares.join(
    farm_algo_for_wt, on=["farm_code", "contract_technical_id", "month", "year"], how="left"
)
for _c in _hours_cap_cols:
    _joined = _joined.withColumn(_c, F.col(_c) * F.col("_hours_share"))
for _c in _mwh_cap_cols:
    _joined = _joined.withColumn(_c, F.col(_c) * F.col("_mwh_share"))

farm_wt_tracker = schedule_maintenance_prepared_model(
    data=_joined.drop("_hours_share", "_mwh_share"),
    rename_columns={"wind_turbine_code": "code"},
    schema=tracker_schema,
    pic_type="turbine",
)

ianosm_budget_tracker_updated = ianosm_budget_tracker_updated.union(farm_wt_tracker)

# COMMAND ----------

# =============================================================================
# MERGE ABOVE/BELOW CAP BACK TO df (turbine level)
# Join on wind_turbine_code / month / year.
# NOTE: schedule_maintenance_prepared_model drops the "type" column (not in tracker_schema).
# The join naturally matches only turbine codes since wind_turbine_codes ≠ substation/farm codes.
# Also includes per_farm redistribution (LTSA-150): farm_above_cap / n_turbines per turbine.
# =============================================================================
turbine_tracker = (
    ianosm_budget_tracker_updated.select(
        F.col("code"),
        F.col("month").alias("tracker_month"),
        F.col("year").alias("tracker_year"),
        "ianosm_m_above_cap_hours",
        "ianosm_m_above_cap_mwh",
        "ianosm_m_below_cap_hours",
        "ianosm_m_below_cap_mwh",
    )
    .union(farm_per_turbine_tracker)
)

df = df.join(
    turbine_tracker,
    on=(
        (F.col("wind_turbine_code") == F.col("code"))
        & (F.col("month") == F.col("tracker_month"))
        & (F.col("year") == F.col("tracker_year"))
    ),
    how="left",
).drop("code", "tracker_month", "tracker_year")

# COMMAND ----------

# =============================================================================
# TURBINE AVAILABILITY
# =============================================================================
print("Computing turbine availability...")

df_turbine = calculate_availabilities(df, suffix_duration_col=suffix_duration_col, suffix_mwh_col=suffix_mwh_col)
df_turbine = calculate_v2_availabilities(df_turbine, suffix_duration_col=suffix_duration_col, suffix_mwh_col=suffix_mwh_col)
df_turbine = build_computed_availabilities_json(df_turbine)

# Resolve FK: wind_turbine_code → turbine_id (dim_turbine)
df_turbine_out = (
    df_turbine
    .join(dim_turbine_ref, F.col("wind_turbine_code") == F.col("_wt_code"), "left")
    .drop("_wt_code")
)

turbine_output_cols = [
    "turbine_id",
    F.col("month").cast("int").alias("month"),
    F.col("year").cast("int").alias("year"),
    "time_availability",
    "energy_availability",
    "computed_availabilities",
    "run_date",
]

df_turbine_out.select(turbine_output_cols).write.mode("overwrite").saveAsTable(
    f"{catalog}.gold.fact_turbine_availability"
)
print(f"Written: {catalog}.gold.fact_turbine_availability")

# COMMAND ----------

# =============================================================================
# SUBSTATION AVAILABILITY
# =============================================================================
print("Computing substation availability...")

substation_grouping_keys = ["substation_code", "farm_code", "year", "month"]

df_substation = aggregate_and_calculate(
    dataframe=df,
    grouping_keys=substation_grouping_keys,
    suffix_duration_col=suffix_duration_col,
    suffix_mwh_col=suffix_mwh_col,
)
df_substation = build_computed_availabilities_json(df_substation)

# Resolve FK: substation_code → substation_id (dim_substation)
df_substation_out = (
    df_substation
    .join(dim_substation_ref, F.col("substation_code") == F.col("_sub_code"), "left")
    .drop("_sub_code")
)

substation_output_cols = [
    "substation_id",
    F.col("month").cast("int").alias("month"),
    F.col("year").cast("int").alias("year"),
    "time_availability",
    "energy_availability",
    "computed_availabilities",
    "run_date",
]

df_substation_out.select(substation_output_cols).write.mode("overwrite").saveAsTable(
    f"{catalog}.gold.fact_substation_availability"
)
print(f"Written: {catalog}.gold.fact_substation_availability")

# COMMAND ----------

# =============================================================================
# FARM AVAILABILITY
# =============================================================================
print("Computing farm availability...")

farm_grouping_keys = ["farm_code", "year", "month"]

df_farm_avail = aggregate_and_calculate(
    dataframe=df,
    grouping_keys=farm_grouping_keys,
    suffix_duration_col=suffix_duration_col,
    suffix_mwh_col=suffix_mwh_col,
)
df_farm_avail = build_computed_availabilities_json(df_farm_avail)

# Resolve FK: farm_code → farm_id (dim_farm)
df_farm_out = (
    df_farm_avail
    .join(dim_farm_ref, F.col("farm_code") == F.col("_farm_code"), "left")
    .drop("_farm_code")
)

farm_output_cols = [
    "farm_id",
    F.col("month").cast("int").alias("month"),
    F.col("year").cast("int").alias("year"),
    "time_availability",
    "energy_availability",
    "computed_availabilities",
    "run_date",
]

df_farm_out.select(farm_output_cols).write.mode("overwrite").saveAsTable(
    f"{catalog}.gold.fact_farm_availability"
)
print(f"Written: {catalog}.gold.fact_farm_availability")

# COMMAND ----------

# =============================================================================
# WRITE IANOSM BUDGET TRACKER
# =============================================================================
print("Writing ianosm_budget_tracker...")

# Drop internal "type" column before write if not needed, or keep for debugging
ianosm_budget_tracker_updated.write.mode("overwrite").saveAsTable(
    f"{catalog}.gold.ianosm_budget_tracker"
)
print(f"Written: {catalog}.gold.ianosm_budget_tracker")
print("Done.")
