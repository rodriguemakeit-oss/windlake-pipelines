# Databricks notebook source
# MAGIC %md
# MAGIC # Unavailability Cost Sync
# MAGIC Syncs `ait_preprod_3_gold.unavailability_cost.unavailability_cost` to S3 in Delta format.

# COMMAND ----------

dbutils.widgets.text(
    "source_table",
    "ait_preprod_3_gold.unavailability_cost.unavailability_cost",
)
dbutils.widgets.text("gold_bucket", "")

source_table = dbutils.widgets.get("source_table").strip()
gold_bucket = dbutils.widgets.get("gold_bucket").strip()

if not source_table:
    raise ValueError("source_table widget is required")

if not gold_bucket:
    raise ValueError("gold_bucket widget is required")

table_name = source_table.split(".")[-1]
output_base_path = f"s3://{gold_bucket}/"
output_path = f"{output_base_path}{table_name}"

print(f"Source table : {source_table}")
print(f"Output path  : {output_path}")

# COMMAND ----------

df = spark.table(source_table)

# COMMAND ----------

(
    df.write
    .mode("overwrite")
    .format("delta")
    .option("delta.enableDeletionVectors", "false")
    .option("mergeSchema", "true")
    .save(output_path)
)

print(f"Written Delta export to {output_path}")
