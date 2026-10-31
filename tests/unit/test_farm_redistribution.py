"""
Unit tests for per-farm → per-turbine redistribution in ianosm_budget_tracker.

Verifies that after the LTSA-150 follow-up fix:
- WIT rows exist in the tracker for per_farm contracts (not just WIF rows)
- above_cap/below_cap values are redistributed proportionally to each turbine's
  share of farm ianosm_m_hours (hours) / ianosm_m_mwh (mwh)
- pm_budget is split equally across n_turbines
- ianosm_m_hours/mwh reflect individual turbine values
"""

import datetime
import pathlib
import sys

import pytest
from pyspark.sql import functions as F

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent.parent / "src" / "gold"))
from utils.turbine_availability import schedule_maintenance_prepared_model


TRACKER_SCHEMA = [
    "code", "month", "year", "contract_technical_id", "guarantee_year",
    "guarantee_start_date", "guarantee_end_date",
    "schedule_maintenance_contract_level", "pm_budget", "pm_unit",
    "is_unlimited", "rate_type", "n_maintainers",
    "ianosm_m_hours", "ianosm_m_mwh",
    "ianosm_m_above_cap_hours", "ianosm_m_above_cap_mwh",
    "ianosm_m_current_hours_cumsum",
    "ianosm_m_hours_above_cap_cumsum", "prev_ianosm_m_hours_above_cap_cumsum",
    "ianosm_m_current_mwh_cumsum",
    "ianosm_m_mwh_above_cap_cumsum", "prev_ianosm_m_mwh_above_cap_cumsum",
    "ianosm_m_mwh_below_cap_cumsum", "prev_ianosm_m_mwh_below_cap_cumsum",
    "ianosm_m_below_cap_mwh",
    "ianosm_m_hours_cumsum",
    "ianosm_m_hours_below_cap_cumsum", "prev_ianosm_m_hours_below_cap_cumsum",
    "ianosm_m_below_cap_hours",
]

_HOURS_CAP_COLS = [
    "ianosm_m_above_cap_hours", "ianosm_m_below_cap_hours",
    "ianosm_m_hours_above_cap_cumsum", "prev_ianosm_m_hours_above_cap_cumsum",
    "ianosm_m_hours_below_cap_cumsum", "prev_ianosm_m_hours_below_cap_cumsum",
    "ianosm_m_hours_cumsum",
]
_MWH_CAP_COLS = [
    "ianosm_m_above_cap_mwh", "ianosm_m_below_cap_mwh",
    "ianosm_m_mwh_above_cap_cumsum", "prev_ianosm_m_mwh_above_cap_cumsum",
    "ianosm_m_mwh_below_cap_cumsum", "prev_ianosm_m_mwh_below_cap_cumsum",
]


def _build_farm_wt_tracker(spark, farm_algo, n_wt_per_farm, farm_turbine_hours):
    """Reproduce the farm_wt_tracker block from wind_availability_compute."""
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
        farm_algo
        .join(n_wt_per_farm, on=["farm_code", "contract_technical_id", "month", "year"], how="left")
        .withColumn("_n", F.coalesce(F.col("n_turbines"), F.lit(1)))
        .withColumn("pm_budget", F.col("pm_budget") / F.col("_n"))
        .drop("_n", "n_turbines", "ianosm_m_hours", "ianosm_m_mwh")
    )
    joined = farm_turbine_with_shares.join(
        farm_algo_for_wt, on=["farm_code", "contract_technical_id", "month", "year"], how="left"
    )
    for c in _HOURS_CAP_COLS:
        joined = joined.withColumn(c, F.col(c) * F.col("_hours_share"))
    for c in _MWH_CAP_COLS:
        joined = joined.withColumn(c, F.col(c) * F.col("_mwh_share"))

    return schedule_maintenance_prepared_model(
        data=joined.drop("_hours_share", "_mwh_share"),
        rename_columns={"wind_turbine_code": "code"},
        schema=TRACKER_SCHEMA,
        pic_type="turbine",
    )


# ── fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def farm_algo(spark):
    """Farm: WIF_TEST, above_cap_hours=120, above_cap_mwh=24, below_cap=30/6."""
    return spark.createDataFrame([{
        "farm_code": "WIF_TEST", "contract_technical_id": "CTR001",
        "month": 1, "year": 2025,
        "slb_date": datetime.date(2025, 1, 1),
        "guarantee_year": 1,
        "guarantee_start_date": datetime.date(2024, 1, 1),
        "guarantee_end_date": datetime.date(2026, 12, 31),
        "schedule_maintenance_contract_level": "farm",
        "pm_budget": 120.0, "pm_unit": "hours_per_farm",
        "is_unlimited": False, "rate_type": "fixed", "n_maintainers": 1,
        "ianosm_m_hours": 150.0, "ianosm_m_mwh": 30.0,
        "ianosm_m_above_cap_hours": 120.0, "ianosm_m_above_cap_mwh": 24.0,
        "ianosm_m_below_cap_hours": 30.0, "ianosm_m_below_cap_mwh": 6.0,
        "ianosm_m_hours_above_cap_cumsum": 120.0,
        "prev_ianosm_m_hours_above_cap_cumsum": 0.0,
        "ianosm_m_hours_below_cap_cumsum": 30.0,
        "prev_ianosm_m_hours_below_cap_cumsum": 0.0,
        "ianosm_m_mwh_above_cap_cumsum": 24.0,
        "prev_ianosm_m_mwh_above_cap_cumsum": 0.0,
        "ianosm_m_mwh_below_cap_cumsum": 6.0,
        "prev_ianosm_m_mwh_below_cap_cumsum": 0.0,
        "ianosm_m_hours_cumsum": 150.0,
        "ianosm_m_current_hours_cumsum": 0.0,
        "ianosm_m_current_mwh_cumsum": 0.0,
    }])


@pytest.fixture()
def n_wt_per_farm(spark):
    return spark.createDataFrame([{
        "farm_code": "WIF_TEST", "contract_technical_id": "CTR001",
        "month": 1, "year": 2025, "n_turbines": 3,
    }])


@pytest.fixture()
def farm_turbine_hours_equal(spark):
    """3 turbines with equal hours (50h each)."""
    rows = [
        {"wind_turbine_code": f"WIT_TEST_{i}", "farm_code": "WIF_TEST",
         "contract_technical_id": "CTR001", "month": 1, "year": 2025,
         "ianosm_m_hours": 50.0, "ianosm_m_mwh": 10.0}
        for i in (1, 2, 3)
    ]
    return spark.createDataFrame(rows)


@pytest.fixture()
def farm_turbine_hours_one_active(spark):
    """Only WT_1 has maintenance hours (like BEBAZ in SLB)."""
    return spark.createDataFrame([
        {"wind_turbine_code": "WIT_TEST_1", "farm_code": "WIF_TEST",
         "contract_technical_id": "CTR001", "month": 1, "year": 2025,
         "ianosm_m_hours": 150.0, "ianosm_m_mwh": 30.0},
        {"wind_turbine_code": "WIT_TEST_2", "farm_code": "WIF_TEST",
         "contract_technical_id": "CTR001", "month": 1, "year": 2025,
         "ianosm_m_hours": 0.0, "ianosm_m_mwh": 0.0},
        {"wind_turbine_code": "WIT_TEST_3", "farm_code": "WIF_TEST",
         "contract_technical_id": "CTR001", "month": 1, "year": 2025,
         "ianosm_m_hours": 0.0, "ianosm_m_mwh": 0.0},
    ])


# ── tests ─────────────────────────────────────────────────────────────────────

def test_wit_rows_exist(spark, farm_algo, n_wt_per_farm, farm_turbine_hours_equal):
    result = _build_farm_wt_tracker(spark, farm_algo, n_wt_per_farm, farm_turbine_hours_equal)
    assert result.filter(F.col("code").startswith("WIT_TEST")).count() == 3


def test_pm_budget_split_equally(spark, farm_algo, n_wt_per_farm, farm_turbine_hours_equal):
    """pm_budget = farm_budget / n_turbines = 120 / 3 = 40."""
    result = _build_farm_wt_tracker(spark, farm_algo, n_wt_per_farm, farm_turbine_hours_equal)
    rows = {r["code"]: r for r in result.collect()}
    for t in ("WIT_TEST_1", "WIT_TEST_2", "WIT_TEST_3"):
        assert abs(rows[t]["pm_budget"] - 40.0) < 0.001


def test_above_cap_proportional_equal_hours(spark, farm_algo, n_wt_per_farm, farm_turbine_hours_equal):
    """Equal hours → each turbine gets 1/3 of above_cap = 40h."""
    result = _build_farm_wt_tracker(spark, farm_algo, n_wt_per_farm, farm_turbine_hours_equal)
    rows = {r["code"]: r for r in result.collect()}
    for t in ("WIT_TEST_1", "WIT_TEST_2", "WIT_TEST_3"):
        assert abs(rows[t]["ianosm_m_above_cap_hours"] - 40.0) < 0.001
        assert abs(rows[t]["ianosm_m_above_cap_mwh"] - 8.0) < 0.001


def test_above_cap_proportional_one_active(spark, farm_algo, n_wt_per_farm, farm_turbine_hours_one_active):
    """Only WT_1 has hours → WT_1 gets 100% of above_cap, others get 0."""
    result = _build_farm_wt_tracker(spark, farm_algo, n_wt_per_farm, farm_turbine_hours_one_active)
    rows = {r["code"]: r for r in result.collect()}
    assert abs(rows["WIT_TEST_1"]["ianosm_m_above_cap_hours"] - 120.0) < 0.001
    assert abs(rows["WIT_TEST_1"]["ianosm_m_above_cap_mwh"] - 24.0) < 0.001
    assert rows["WIT_TEST_2"]["ianosm_m_above_cap_hours"] == 0.0
    assert rows["WIT_TEST_3"]["ianosm_m_above_cap_hours"] == 0.0


def test_below_cap_proportional(spark, farm_algo, n_wt_per_farm, farm_turbine_hours_equal):
    """below_cap = farm_below_cap × share = 30 / 3 = 10h."""
    result = _build_farm_wt_tracker(spark, farm_algo, n_wt_per_farm, farm_turbine_hours_equal)
    rows = {r["code"]: r for r in result.collect()}
    for t in ("WIT_TEST_1", "WIT_TEST_2", "WIT_TEST_3"):
        assert abs(rows[t]["ianosm_m_below_cap_hours"] - 10.0) < 0.001
        assert abs(rows[t]["ianosm_m_below_cap_mwh"] - 2.0) < 0.001


def test_individual_hours_preserved(spark, farm_algo, n_wt_per_farm):
    """ianosm_m_hours = individual turbine SLB hours, not farm/n."""
    unequal = spark.createDataFrame([
        {"wind_turbine_code": "WIT_TEST_1", "farm_code": "WIF_TEST",
         "contract_technical_id": "CTR001", "month": 1, "year": 2025,
         "ianosm_m_hours": 30.0, "ianosm_m_mwh": 3.0},
        {"wind_turbine_code": "WIT_TEST_2", "farm_code": "WIF_TEST",
         "contract_technical_id": "CTR001", "month": 1, "year": 2025,
         "ianosm_m_hours": 50.0, "ianosm_m_mwh": 5.0},
        {"wind_turbine_code": "WIT_TEST_3", "farm_code": "WIF_TEST",
         "contract_technical_id": "CTR001", "month": 1, "year": 2025,
         "ianosm_m_hours": 70.0, "ianosm_m_mwh": 7.0},
    ])
    result = _build_farm_wt_tracker(spark, farm_algo, n_wt_per_farm, unequal)
    rows = {r["code"]: r for r in result.collect()}
    assert abs(rows["WIT_TEST_1"]["ianosm_m_hours"] - 30.0) < 0.001
    assert abs(rows["WIT_TEST_2"]["ianosm_m_hours"] - 50.0) < 0.001
    assert abs(rows["WIT_TEST_3"]["ianosm_m_hours"] - 70.0) < 0.001


def test_all_zero_hours_gives_zero_above_cap(spark, farm_algo, n_wt_per_farm):
    """If all turbines have 0h, above_cap = 0 for all (no division by zero)."""
    all_zero = spark.createDataFrame([
        {"wind_turbine_code": f"WIT_TEST_{i}", "farm_code": "WIF_TEST",
         "contract_technical_id": "CTR001", "month": 1, "year": 2025,
         "ianosm_m_hours": 0.0, "ianosm_m_mwh": 0.0}
        for i in (1, 2, 3)
    ])
    result = _build_farm_wt_tracker(spark, farm_algo, n_wt_per_farm, all_zero)
    rows = {r["code"]: r for r in result.collect()}
    for t in ("WIT_TEST_1", "WIT_TEST_2", "WIT_TEST_3"):
        assert rows[t]["ianosm_m_above_cap_hours"] == 0.0
