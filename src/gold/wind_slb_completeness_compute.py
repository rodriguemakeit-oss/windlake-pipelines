# Databricks notebook source
# MAGIC %md
# MAGIC ## Compute SLB completeness

# COMMAND ----------

from utils.kimball import add_id_column
from utils.helpers import reorder_columns

# COMMAND ----------

silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")
gold_bucket = dbutils.widgets.get("gold_bucket")

# COMMAND ----------

slb_completeness_turbine_refreshed = spark.sql(f"""
    with slb_data_by_wtg as (
    select 
        ent_code,
        ent_code as wind_turbine_code,
        substation_code,
        farm_code,
        slb_year,
        slb_month,
        sum(case when iec_stop_category_label is null then 1 else 0 end) as missing_iec,
        count(*) as total_rows,
        sum(case when iec_stop_category_label is null then stop_duration_min else 0 end) as missing_iec_duration_min,
        sum(stop_duration_min) as total_duration_min
    from {silver_schema}.turbine_slb as slb
    where ent_code is not null and slb_year is not null and slb_month is not null
      and ent_code like 'WIT_%'
    group by ent_code, substation_code, farm_code, slb_year, slb_month
    ),

    slb_completeness_by_wtg as (
        select 
            *,
            (1 - coalesce(try_divide(missing_iec, total_rows), 0)) * 100.0 as completeness_percentage_row,
            (1 - coalesce(try_divide(missing_iec_duration_min, total_duration_min), 0)) * 100.0 as completeness_percentage,
            current_timestamp() as run_date 
        from slb_data_by_wtg
    )

    select * from slb_completeness_by_wtg
""")

slb_completeness_turbine_refreshed.createOrReplaceTempView("slb_turbine")

# COMMAND ----------

fact_slb_completeness_turbine = spark.sql(
    f"""
    SELECT st.*, dt.id AS turbine_id
    FROM slb_turbine as st
    INNER JOIN {gold_schema}.dim_turbine as dt
    ON st.wind_turbine_code = dt.code
    """
).drop("ent_code", "wind_turbine_code", "substation_code", "farm_code")

fact_slb_completeness_turbine = reorder_columns(fact_slb_completeness_turbine, first_cols=["turbine_id"])
fact_slb_completeness_turbine.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(f"{gold_schema}.fact_slb_completeness_turbine")

# COMMAND ----------

slb_completeness_turbine_refreshed.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(f"{gold_schema}.slb_completeness_turbine")

# COMMAND ----------

slb_completeness_turbine_refreshed.write \
  .mode("overwrite") \
  .format("delta") \
  .option("delta.enableDeletionVectors", "false") \
  .option("mergeSchema", "true") \
  .option("overwriteSchema", "true") \
.save(f"s3://{gold_bucket}/slb_completeness_turbine/data")
