# Databricks notebook source
# Create silver and gold schema
catalog = dbutils.widgets.get("catalog")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")

# COMMAND ----------

# files_genai_ignored — allowlist of reports that must not be raised as errors
# in the Teams alert. Owned by the business: it is loaded from the
# `_ignored_errors` tab of SGRE_Mapping_Table.xlsx by src/silver/turbine_alias_load.py.
# This step only guarantees the table exists (with the expected columns) so the
# alert query never fails on a fresh environment.
# Matching is a case-insensitive substring of the report path: a full path
# targets one file, a partial text (e.g. "Event report") targets every file
# whose path contains it.
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {catalog}.silver.files_genai_ignored (
        pattern    STRING    COMMENT 'Full report path, or partial text matched against it',
        reason     STRING    COMMENT 'Why this report is not an error',
        error_type STRING    COMMENT 'unprocessed | unmatch — which alert family it silences',
        run_date   TIMESTAMP COMMENT 'Last load from the SharePoint workbook'
    ) USING DELTA
    """
)

# Tables created before the Excel-driven design carry (pattern, is_regex,
# reason, added_at): add the new column so the alert query keeps working until
# the first load from the workbook replaces the schema.
try:
    spark.sql(
        f"ALTER TABLE {catalog}.silver.files_genai_ignored ADD COLUMNS (error_type STRING)"
    )
except Exception:
    pass  # column already there
spark.sql(
    f"""
    UPDATE {catalog}.silver.files_genai_ignored
    SET error_type = 'unprocessed'
    WHERE error_type IS NULL
    """
)
