from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List, Dict, Optional


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def safe_col(col_name: str, default=0.0) -> F.Column:
    """Returns coalesce(col, default) — equivalent of pandas fillna."""
    return F.coalesce(F.col(col_name), F.lit(default))


# =============================================================================
# AVAILABILITY FUNCTIONS
# =============================================================================
def calculate_availabilities(
    dataframe: DataFrame, suffix_duration_col: str, suffix_mwh_col: str
) -> DataFrame:
    """Calculate time_availability (v1) and energy_availability (v1) for a dataframe."""
    dur = suffix_duration_col
    mwh = suffix_mwh_col

    # Calculate time_availability (v1)
    numerator_time = (
        safe_col("ianopca_maintainer_" + dur) / 60
        + safe_col("ianofo_maintainer_" + dur) / 60
    )

    denominator_time = (
        safe_col("month_time_in_seconds") / 3600
        - (safe_col("ianosm_maintainer_" + dur) + safe_col("ianosm_other_" + dur)) / 60
        - (safe_col("ianos_maintainer_" + dur) + safe_col("ianos_other_" + dur)) / 60
        - (safe_col("iafm_maintainer_" + dur) + safe_col("iafm_other_" + dur)) / 60
        - (safe_col("iu_maintainer_" + dur) + safe_col("iu_other_" + dur)) / 60
    )

    time_v1 = F.when(
        denominator_time != 0, (F.lit(1) - numerator_time / denominator_time) * 100
    ).otherwise(F.lit(100.0))

    # Calculate energy_availability (v1)
    numerator_energy = (
        safe_col("iaogpp_maintainer_" + mwh) * 1000
        + safe_col("ianopca_maintainer_" + mwh) * 1000
        + safe_col("ianofo_maintainer_" + mwh) * 1000
    )

    denominator_energy = (
        safe_col("production_kwh")
        + (safe_col("iaongts_maintainer_" + mwh) + safe_col("iaongts_other_" + mwh)) * 1000
        + (safe_col("iaongen_maintainer_" + mwh) + safe_col("iaongen_other_" + mwh)) * 1000
        + (safe_col("iaongrs_maintainer_" + mwh) + safe_col("iaongrs_other_" + mwh)) * 1000
        + (safe_col("iaongel_maintainer_" + mwh) + safe_col("iaongel_other_" + mwh)) * 1000
        + (safe_col("iaogpp_maintainer_" + mwh) + safe_col("iaogpp_other_" + mwh)) * 1000
        + (safe_col("ianopca_maintainer_" + mwh) + safe_col("ianopca_other_" + mwh)) * 1000
        + (safe_col("ianofo_maintainer_" + mwh) + safe_col("ianofo_other_" + mwh)) * 1000
    )

    energy_v1 = F.when(
        denominator_energy != 0,
        (F.lit(1) - numerator_energy / denominator_energy) * 100,
    ).otherwise(F.lit(0.0))

    return (
        dataframe
        .withColumn("time_v1", time_v1)
        .withColumn("time_availability", F.col("time_v1"))
        .withColumn("energy_v1", energy_v1)
        .withColumn("energy_availability", F.col("energy_v1"))
    )


def calculate_v2_availabilities(
    dataframe: DataFrame, suffix_duration_col: str, suffix_mwh_col: str
) -> DataFrame:
    """
    Calculate time_v2 (Eq.12) and energy_v2 (Eq.16) using IANOSM_Maintainer_above/below_cap.
    Expects columns 'ianosm_m_above_cap_hours' and 'ianosm_m_above_cap_mwh' to exist.
    """
    dur = suffix_duration_col
    mwh = suffix_mwh_col

    # ---- Time v2 (Equation 12) ----
    # Numerator: IANOPCA_M + IANOFO_M + IAONGRS_M + IAONGEL_M + IANOS_M + IANOSM_M_above_cap
    numerator_time_v2 = (
        safe_col("ianopca_maintainer_" + dur) / 60
        + safe_col("ianofo_maintainer_" + dur) / 60
        + safe_col("iaongrs_maintainer_" + dur) / 60
        + safe_col("iaongel_maintainer_" + dur) / 60
        + safe_col("ianos_maintainer_" + dur) / 60
        + safe_col("ianosm_m_above_cap_hours")  # already in hours
    )

    # Denominator: total_period_duration - IANOSM_M_below_cap - IANOS_other - IAFM_all - IU_all
    # ianos_all - ianos_maintainer = ianos_other
    denominator_time_v2 = (
        safe_col("month_time_in_seconds") / 3600
        - safe_col("ianosm_m_below_cap_hours")  # already in hours
        - safe_col("ianos_other_" + dur) / 60
        - (safe_col("iafm_maintainer_" + dur) + safe_col("iafm_other_" + dur)) / 60
        - (safe_col("iu_maintainer_" + dur) + safe_col("iu_other_" + dur)) / 60
    )

    time_v2 = F.when(
        denominator_time_v2 != 0,
        (F.lit(1) - numerator_time_v2 / denominator_time_v2) * 100,
    ).otherwise(F.lit(100.0))

    # ---- Energy v2 (Equation 16) ----
    # Numerator: IAOGPP_M + IANOPCA_M + IANOFO_M + IAONGRS_M + IAONGEL_M + IANOS_M + IANOSM_M_above_cap
    numerator_energy_v2 = (
        safe_col("iaogpp_maintainer_" + mwh) * 1000
        + safe_col("ianopca_maintainer_" + mwh) * 1000
        + safe_col("ianofo_maintainer_" + mwh) * 1000
        + safe_col("iaongrs_maintainer_" + mwh) * 1000
        + safe_col("iaongel_maintainer_" + mwh) * 1000
        + safe_col("ianos_maintainer_" + mwh) * 1000
        + safe_col("ianosm_m_above_cap_mwh") * 1000
    )

    # Denominator: (ActPow_10min/6 + IAONGTS_all + IAONGEN_all + IAONGRS_all + IAONGEL_all)
    #            + (IAOGPP_all + IANOPCA_all + IANOFO_all + IANOS_M + IANOSM_M_above_cap)
    denominator_energy_v2 = (
        safe_col("production_kwh")
        + (safe_col("iaongts_maintainer_" + mwh) + safe_col("iaongts_other_" + mwh)) * 1000
        + (safe_col("iaongen_maintainer_" + mwh) + safe_col("iaongen_other_" + mwh)) * 1000
        + (safe_col("iaongrs_maintainer_" + mwh) + safe_col("iaongrs_other_" + mwh)) * 1000
        + (safe_col("iaongel_maintainer_" + mwh) + safe_col("iaongel_other_" + mwh)) * 1000
        + (safe_col("iaogpp_maintainer_" + mwh) + safe_col("iaogpp_other_" + mwh)) * 1000
        + (safe_col("ianopca_maintainer_" + mwh) + safe_col("ianopca_other_" + mwh)) * 1000
        + (safe_col("ianofo_maintainer_" + mwh) + safe_col("ianofo_other_" + mwh)) * 1000
        + safe_col("ianos_maintainer_" + mwh) * 1000
        + safe_col("ianosm_m_above_cap_mwh") * 1000
    )

    energy_v2 = F.when(
        denominator_energy_v2 != 0,
        (F.lit(1) - numerator_energy_v2 / denominator_energy_v2) * 100,
    ).otherwise(F.lit(0.0))

    return (
        dataframe
        .withColumn("time_v2", time_v2)
        .withColumn("energy_v2", energy_v2)
    )


def aggregate_and_calculate(
    dataframe: DataFrame,
    grouping_keys: List[str],
    suffix_duration_col: str,
    suffix_mwh_col: str,
) -> DataFrame:
    """
    Group dataframe by keys, sum IA* columns and recalculate availabilities (v1 + v2).
    """
    # Filter rows with non-empty farm_code
    df_agg = dataframe.filter(
        F.col("farm_code").isNotNull() & (F.col("farm_code") != "")
    )

    # Identify columns to sum: those ending with suffix_duration_col or suffix_mwh_col
    all_cols = df_agg.columns
    cols_to_sum = [
        c for c in all_cols
        if (c.endswith(suffix_duration_col) or c.endswith(suffix_mwh_col))
        and c not in grouping_keys
    ]

    # Add extra fixed columns to sum if present
    for extra_col in [
        "month_time_in_seconds",
        "production_kwh",
        "ianosm_m_above_cap_hours",
        "ianosm_m_above_cap_mwh",
        "ianosm_m_below_cap_hours",
        "ianosm_m_below_cap_mwh",
    ]:
        if extra_col in all_cols and extra_col not in cols_to_sum:
            cols_to_sum.append(extra_col)

    agg_exprs = [F.sum(c).alias(c) for c in cols_to_sum]
    if "run_date" in all_cols:
        agg_exprs.append(F.first("run_date").alias("run_date"))

    df_grouped = df_agg.groupBy(grouping_keys).agg(*agg_exprs)

    # Recalculate v1 and v2 availabilities
    df_grouped = calculate_availabilities(df_grouped, suffix_duration_col, suffix_mwh_col)
    df_grouped = calculate_v2_availabilities(df_grouped, suffix_duration_col, suffix_mwh_col)

    # Post-filter
    df_grouped = df_grouped.filter(
        F.col("farm_code").isNotNull() & (F.col("farm_code") != "")
    )

    return df_grouped


def build_computed_availabilities_json(dataframe: DataFrame) -> DataFrame:
    """Add computed_availabilities JSON column to the dataframe."""
    return dataframe.withColumn(
        "computed_availabilities",
        F.to_json(
            F.struct(
                F.round("time_v1", 3).alias("time_v1"),
                F.round("energy_v1", 3).alias("energy_v1"),
                F.round("time_v2", 3).alias("time_v2"),
                F.round("energy_v2", 3).alias("energy_v2"),
            )
        ),
    )


# =============================================================================
# COMPUTE SCHEDULE MAINTENANCE FUNCTIONS
# =============================================================================
def schedule_maintenance_prepared_model(
    data: DataFrame,
    rename_columns: Dict[str, str],
    schema: List[str],
    pic_type: str,
) -> DataFrame:
    if data is None:
        return None

    df = data.withColumn("type", F.lit(pic_type))
    for old_name, new_name in rename_columns.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df.select(schema)


def compute_iec_above_cap_algorithm_by_turbine(
    data: DataFrame,
    mask: Optional[F.Column],
    sort_values: List[str],
    groupby_keys: List[str],
    groupby_selected_columns: List[str],
) -> DataFrame:
    """
    Compute IANOSM above/below cap per guarantee group using Window functions.
    Equivalent of the pandas cumsum/shift loop.
    """
    if mask is not None:
        df_filtered = data.filter(mask)
    else:
        df_filtered = data

    df_filtered = df_filtered.select(groupby_selected_columns)

    w = Window.partitionBy(*groupby_keys).orderBy(*sort_values)
    w_cum = (
        Window.partitionBy(*groupby_keys)
        .orderBy(*sort_values)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    is_hours = F.col("pm_unit").isin("hours_per_wt", "hours_per_farm")
    is_mwh = F.col("pm_unit").isin("mwh_per_wt", "mwh_per_farm")

    # ----- hours branch -----
    hours_cumsum = F.sum(safe_col("ianosm_m_hours")).over(w_cum)
    hours_above_cap_cumsum = F.greatest(F.lit(0.0), hours_cumsum - F.col("pm_budget"))
    prev_hours_above_cap_cumsum = F.coalesce(F.lag(hours_above_cap_cumsum).over(w), F.lit(0.0))
    hours_above_cap = hours_above_cap_cumsum - prev_hours_above_cap_cumsum
    hours_below_cap_cumsum = hours_cumsum - hours_above_cap_cumsum
    prev_hours_below_cap_cumsum = F.coalesce(F.lag(hours_below_cap_cumsum).over(w), F.lit(0.0))
    hours_below_cap = hours_below_cap_cumsum - prev_hours_below_cap_cumsum

    # ----- mwh branch -----
    mwh_cumsum = F.sum(safe_col("ianosm_m_mwh")).over(w_cum)
    mwh_above_cap_cumsum = F.greatest(F.lit(0.0), mwh_cumsum - F.col("pm_budget"))
    prev_mwh_above_cap_cumsum = F.coalesce(F.lag(mwh_above_cap_cumsum).over(w), F.lit(0.0))
    mwh_above_cap = mwh_above_cap_cumsum - prev_mwh_above_cap_cumsum
    mwh_below_cap_cumsum = mwh_cumsum - mwh_above_cap_cumsum
    prev_mwh_below_cap_cumsum = F.coalesce(F.lag(mwh_below_cap_cumsum).over(w), F.lit(0.0))
    mwh_below_cap = mwh_below_cap_cumsum - prev_mwh_below_cap_cumsum

    return (
        df_filtered
        # hours branch
        .withColumn("ianosm_m_hours_cumsum", F.when(is_hours, hours_cumsum).otherwise(F.lit(0.0)))
        .withColumn("ianosm_m_hours_above_cap_cumsum", F.when(is_hours, hours_above_cap_cumsum).otherwise(F.lit(0.0)))
        .withColumn("prev_ianosm_m_hours_above_cap_cumsum", F.when(is_hours, prev_hours_above_cap_cumsum).otherwise(F.lit(0.0)))
        .withColumn("ianosm_m_above_cap_hours", F.when(is_hours, hours_above_cap).otherwise(F.lit(0.0)))
        .withColumn("ianosm_m_hours_below_cap_cumsum", F.when(is_hours, hours_below_cap_cumsum).otherwise(F.lit(0.0)))
        .withColumn("prev_ianosm_m_hours_below_cap_cumsum", F.when(is_hours, prev_hours_below_cap_cumsum).otherwise(F.lit(0.0)))
        .withColumn("ianosm_m_below_cap_hours", F.when(is_hours, hours_below_cap).otherwise(F.lit(0.0)))
        # mwh branch
        .withColumn("ianosm_m_mwh_cumsum", F.when(is_mwh, mwh_cumsum).otherwise(F.lit(0.0)))
        .withColumn("ianosm_m_mwh_above_cap_cumsum", F.when(is_mwh, mwh_above_cap_cumsum).otherwise(F.lit(0.0)))
        .withColumn("prev_ianosm_m_mwh_above_cap_cumsum", F.when(is_mwh, prev_mwh_above_cap_cumsum).otherwise(F.lit(0.0)))
        .withColumn("ianosm_m_above_cap_mwh", F.when(is_mwh, mwh_above_cap).otherwise(F.lit(0.0)))
        .withColumn("ianosm_m_mwh_below_cap_cumsum", F.when(is_mwh, mwh_below_cap_cumsum).otherwise(F.lit(0.0)))
        .withColumn("prev_ianosm_m_mwh_below_cap_cumsum", F.when(is_mwh, prev_mwh_below_cap_cumsum).otherwise(F.lit(0.0)))
        .withColumn("ianosm_m_below_cap_mwh", F.when(is_mwh, mwh_below_cap).otherwise(F.lit(0.0)))
        # zero-init columns not computed by either branch
        .withColumn("ianosm_m_current_hours_cumsum", F.lit(0.0))
        .withColumn("ianosm_m_current_mwh_cumsum", F.lit(0.0))
    )


def compute_iec_above_cap_algorithm_by_farm(
    data: DataFrame,
    mask: Optional[F.Column],
    sort_values: List[str],
    groupby_keys: List[str],
    groupby_selected_columns: List[str],
) -> DataFrame:
    """
    This function calls compute_iec_above_cap_algorithm_by_turbine, why because it should be able to differentiate this algorithm from the other one
    in case stakeholders changes the rules
    But right now I agreed with the stakeholder that we can considere that compute_iec_above_cap_algorithm_by_farm is a sub case of compute_iec_above_cap_algorithm_by_turbine
    """
    return compute_iec_above_cap_algorithm_by_turbine(
        data=data,
        mask=mask,
        sort_values=sort_values,
        groupby_keys=groupby_keys,
        groupby_selected_columns=groupby_selected_columns,
    )


def compute_schedule_maintenance_at_higher_level(
    wt_schedule_maintenance: DataFrame,
    columns: List[str],
    groupby_keys: List[str],
    aggregation_groupby_keys: List[str],
) -> DataFrame:
    """
    Aggregate wt_schedule_maintenance to a higher level (farm or substation).
    Replaces the pandas loop + concat pattern.
    """
    if wt_schedule_maintenance is None:
        return None

    df = wt_schedule_maintenance.select(columns)

    return df.groupBy(aggregation_groupby_keys).agg(
        F.first("contract_technical_id").alias("contract_technical_id"),
        F.first("guarantee_year").alias("guarantee_year"),
        F.first("guarantee_start_date").alias("guarantee_start_date"),
        F.first("guarantee_end_date").alias("guarantee_end_date"),
        F.first("schedule_maintenance_contract_level").alias("schedule_maintenance_contract_level"),
        F.sum("pm_budget").alias("pm_budget"),
        F.first("pm_unit").alias("pm_unit"),
        F.first("is_unlimited").alias("is_unlimited"),
        F.first("rate_type").alias("rate_type"),
        F.first("n_maintainers").alias("n_maintainers"),
        F.sum("ianosm_m_hours").alias("ianosm_m_hours"),
        F.sum("ianosm_m_mwh").alias("ianosm_m_mwh"),
        F.sum("ianosm_m_above_cap_hours").alias("ianosm_m_above_cap_hours"),
        F.sum("ianosm_m_above_cap_mwh").alias("ianosm_m_above_cap_mwh"),
        F.sum("ianosm_m_current_hours_cumsum").alias("ianosm_m_current_hours_cumsum"),
        F.sum("ianosm_m_hours_above_cap_cumsum").alias("ianosm_m_hours_above_cap_cumsum"),
        F.sum("prev_ianosm_m_hours_above_cap_cumsum").alias("prev_ianosm_m_hours_above_cap_cumsum"),
        F.sum("ianosm_m_current_mwh_cumsum").alias("ianosm_m_current_mwh_cumsum"),
        F.sum("ianosm_m_mwh_above_cap_cumsum").alias("ianosm_m_mwh_above_cap_cumsum"),
        F.sum("prev_ianosm_m_mwh_above_cap_cumsum").alias("prev_ianosm_m_mwh_above_cap_cumsum"),
        F.sum("ianosm_m_hours_cumsum").alias("ianosm_m_hours_cumsum"),
        F.sum("ianosm_m_hours_below_cap_cumsum").alias("ianosm_m_hours_below_cap_cumsum"),
        F.sum("prev_ianosm_m_hours_below_cap_cumsum").alias("prev_ianosm_m_hours_below_cap_cumsum"),
        F.sum("ianosm_m_below_cap_hours").alias("ianosm_m_below_cap_hours"),
        F.sum("ianosm_m_mwh_below_cap_cumsum").alias("ianosm_m_mwh_below_cap_cumsum"),
        F.sum("prev_ianosm_m_mwh_below_cap_cumsum").alias("prev_ianosm_m_mwh_below_cap_cumsum"),
        F.sum("ianosm_m_below_cap_mwh").alias("ianosm_m_below_cap_mwh"),
    )
