# Databricks notebook source
# MAGIC %md
# MAGIC # GenAI ↔ Telemetry — Turbine Matching
# MAGIC
# MAGIC Match turbines extracted from PDF/Excel reports (GenAI pipeline) with Telemetry reference turbines.
# MAGIC
# MAGIC ## Flow
# MAGIC 1. **Phase 1** — Read `silver.availability_genai`, prepare serial numbers and country codes → `silver.availability_genai_prepared`
# MAGIC 2. **Phase 2** — Exact + fuzzy matching against Telemetry (`gold.dim_turbine`) → `silver.availability_genai_telemetry_match` / `silver.availability_genai_telemetry_no_match`
# MAGIC 3. **Phase 3** — Site-level availability (type == `site`) → `silver.availability_genai_site_availability`
# MAGIC 4. **Phase 4** — File mapping classification → 4 silver mapping tables
# MAGIC 5. **Phase 5** — Fact tables with dimension FKs → gold fact tables

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from utils.helpers import drop_months_after_report
from utils.serial_match import match_serial
from utils.turbine_alias import prepare_turbine_match_keys

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 1 — Prepare serial numbers
# MAGIC
# MAGIC Match keys come from `utils.turbine_alias`: when `dim_turbine_report_alias`
# MAGIC covers a `(country, report-label)` pair its Telemetry serial overrides
# MAGIC `turbine_id`, otherwise the legacy `turbine_id` parsing is used.

# COMMAND ----------

_df_genai_raw = spark.table(f"{CATALOG}.silver.availability_genai")
# Rows dated after the report month are LLM date mislabelling — drop them so
# they cannot feed months the report does not cover.
df_genai = drop_months_after_report(_df_genai_raw)
print(f"Rows dated after their report month (dropped): {_df_genai_raw.count() - df_genai.count()}")
df_alias = spark.table(f"{CATALOG}.silver.dim_turbine_report_alias")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 1b — LLM disambiguation of reused report labels (cache-first)
# MAGIC
# MAGIC Some report labels (e.g. `A01`) are reused across several farms in the same
# MAGIC country, so `(country, label)` maps to multiple Telemetry serials. The report
# MAGIC file name carries the site, so an LLM picks the right serial out of the
# MAGIC closed candidate list — abstaining when unsure. Results are cached in
# MAGIC `silver.turbine_alias_llm_resolution`, so each `(country, label, file_key)`
# MAGIC is resolved only once (reruns/backfills do a plain join, no LLM call).

# COMMAND ----------

import json
import datetime

from utils.turbine_alias import pic_country_col, site_file_key, label_key
from utils import turbine_disambiguation as tdis

RESOLUTION_TABLE = f"{CATALOG}.silver.turbine_alias_llm_resolution"


def _widget(name, default=""):
    try:
        return dbutils.widgets.get(name)
    except Exception:
        return default


IA_MODEL = _widget("ia_model", "gpt-4o")
SERVICE_CREDENTIAL_NAME = _widget("service_credential_name")
SECRETS_REGION = _widget("secrets_manager_region", "eu-west-1")
SECRETS_ID = _widget("secrets_manager_id")


def _make_openai_client():
    """Build an Azure OpenAI client from AWS Secrets Manager (same secret as extract)."""
    import boto3
    from openai import AzureOpenAI

    provider = dbutils.credentials.getServiceCredentialsProvider(SERVICE_CREDENTIAL_NAME)
    session = boto3.Session(botocore_session=provider, region_name=SECRETS_REGION)
    sm = session.client("secretsmanager")
    secrets = json.loads(sm.get_secret_value(SecretId=SECRETS_ID)["SecretString"])
    return AzureOpenAI(
        api_key=secrets["AZURE_OPENAI_KEY"],
        azure_endpoint=secrets["AZURE_OPENAI_ENDPOINT"],
        api_version="2024-02-01",
    )


# Cache table — created on first run, append-only.
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {RESOLUTION_TABLE} (
        country_pic         STRING,
        report_turbine_name STRING,
        file_key            STRING,
        candidates_hash     STRING,
        resolved_serial     STRING,
        reason              STRING,
        model               STRING,
        run_date            TIMESTAMP
    ) USING delta
    """
)

# Ambiguous (country, label) keys = more than one distinct serial in the mapping
# Human-readable site names from Telemetry — the alias mapping only carries the
# cryptic farm code (e.g. WIF_PTSED), but report file names use the real site
# name (e.g. "Videmonte - Serra do Ralo"). Attaching dim_farm.farm lets the LLM
# match on the site name instead of guessing from the code.
dim_farm_names = spark.table(f"{CATALOG}.gold.dim_farm").select(
    F.col("code").alias("_df_code"),
    F.col("farm").alias("_df_name"),
)

alias_keyed = (
    df_alias.select(
        pic_country_col(F.col("country")).alias("country_pic"),
        label_key(F.col("report_turbine_name")).alias("label"),
        F.col("serial_number").cast("string").alias("serial"),
        F.col("farm").alias("farm"),
    )
    # Mapping farm codes are inconsistent ("PTMOU" vs "WIF_PTNAV") — normalise to
    # the WIF_ prefix before joining dim_farm.
    .withColumn(
        "_farm_code_norm",
        F.when(F.col("farm").startswith("WIF_"), F.col("farm"))
        .otherwise(F.concat(F.lit("WIF_"), F.col("farm"))),
    )
    .join(F.broadcast(dim_farm_names), F.col("_farm_code_norm") == F.col("_df_code"), "left")
    .withColumn("farm_name", F.coalesce(F.col("_df_name"), F.col("farm")))
    .drop("_farm_code_norm", "_df_code", "_df_name")
)
ambiguous_keys = (
    alias_keyed.groupBy("country_pic", "label")
    .agg(F.countDistinct("serial").alias("n"))
    .filter(F.col("n") > 1)
    .select("country_pic", "label")
)
ambiguous_candidates = (
    alias_keyed.join(F.broadcast(ambiguous_keys), ["country_pic", "label"], "inner")
    .groupBy("country_pic", "label")
    .agg(F.collect_set(F.struct("serial", "farm", "farm_name")).alias("candidates"))
)

# Distinct (country, label, file_key) of ambiguous turbine reports that need resolving
needed = (
    df_genai.filter(F.col("type") == "turbine")
    .select(
        pic_country_col(F.col("country")).alias("country_pic"),
        # Canonical label: strip any " (serial)" / "| serial" suffix so the
        # report label matches the alias crosswalk (present on either side).
        label_key(F.col("turbine_id")).alias("label"),
        F.col("file").alias("file"),
        site_file_key(F.col("file")).alias("file_key"),
    )
    .join(F.broadcast(ambiguous_candidates), ["country_pic", "label"], "inner")
    .groupBy("country_pic", "label", "file_key")
    .agg(F.first("file").alias("file"), F.first("candidates").alias("candidates"))
)

needed_pd = needed.toPandas()
cache_pd = spark.table(RESOLUTION_TABLE).select(
    "country_pic", "report_turbine_name", "file_key", "candidates_hash"
).toPandas()
already = set(
    zip(cache_pd.country_pic, cache_pd.report_turbine_name,
        cache_pd.file_key, cache_pd.candidates_hash)
)

print(f"Ambiguous (label, file) to resolve: {len(needed_pd)} | cache rows: {len(cache_pd)}")

new_rows = []
client = None
for _, r in needed_pd.iterrows():
    candidates = [
        {"serial": c["serial"], "farm": c["farm"], "farm_name": c["farm_name"]}
        for c in r["candidates"]
    ]
    chash = tdis.candidates_hash(candidates)
    if (r["country_pic"], r["label"], r["file_key"], chash) in already:
        continue  # cache hit — no LLM call
    if client is None:
        if not (SERVICE_CREDENTIAL_NAME and SECRETS_ID):
            print("   ⚠️  No OpenAI secrets configured — skipping LLM resolution (cache-only).")
            break
        client = _make_openai_client()
    result = tdis.resolve_one(
        client, IA_MODEL, r["country_pic"], r["label"], r["file"], candidates
    )
    # A transport/access error (e.g. 403 VNet/firewall, 429 rate-limit) is NOT a
    # real abstention — don't cache it, otherwise the (label, file) is treated as
    # resolved-miss and never retried once the endpoint is reachable again.
    if result["serial"] is None and str(result["reason"]).startswith("LLM error:"):
        print(f"   ⚠️  transport error for {r['country_pic']}/{r['label']} — not cached: {result['reason'][:80]}")
        continue
    new_rows.append({
        "country_pic": r["country_pic"],
        "report_turbine_name": r["label"],
        "file_key": r["file_key"],
        "candidates_hash": chash,
        "resolved_serial": result["serial"],
        "reason": result["reason"],
        "model": IA_MODEL,
        "run_date": datetime.datetime.now(),
    })

if new_rows:
    # Explicit schema: resolved_serial/reason can be all-None in a batch (LLM
    # abstentions), which breaks Spark's type inference (CANNOT_DETERMINE_TYPE).
    from pyspark.sql.types import (
        StructType, StructField, StringType, TimestampType,
    )
    _resolution_schema = StructType([
        StructField("country_pic", StringType()),
        StructField("report_turbine_name", StringType()),
        StructField("file_key", StringType()),
        StructField("candidates_hash", StringType()),
        StructField("resolved_serial", StringType()),
        StructField("reason", StringType()),
        StructField("model", StringType()),
        StructField("run_date", TimestampType()),
    ])
    (
        spark.createDataFrame(new_rows, schema=_resolution_schema)
        .write.format("delta").mode("append").option("mergeSchema", "true")
        .saveAsTable(RESOLUTION_TABLE)
    )
    print(f"   ✅ Resolved {len(new_rows)} new (label, file) via LLM → {RESOLUTION_TABLE}")
else:
    print("   ℹ️  Nothing new to resolve (all cached or no ambiguous keys).")

df_resolution = (
    spark.table(RESOLUTION_TABLE)
    # The cache is append-only: a (country, label, file_key) can have several
    # rows (re-resolved after a prompt/candidate change). Keep only the most
    # recent one per key — otherwise the resolution join fans out turbine rows.
    .withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("country_pic", "report_turbine_name", "file_key")
            .orderBy(F.col("run_date").desc())
        ),
    )
    .filter(F.col("_rn") == 1)
    .select("country_pic", "report_turbine_name", "file_key", "resolved_serial")
)

# COMMAND ----------

df_prepared = prepare_turbine_match_keys(df_genai, df_alias, df_resolution)

# Keep only turbine rows for matching (site rows handled in Phase 3)
df_turbines = df_prepared.filter(F.col("type") == "turbine")

print(f"Total rows in availability_genai : {df_genai.count()}")
print(f"Turbine rows prepared for matching: {df_turbines.count()}")

# COMMAND ----------

df_turbines.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(
    f"{CATALOG}.silver.availability_genai_prepared"
)
print(f"Written to {CATALOG}.silver.availability_genai_prepared")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 2 — Telemetry matching (exact)

# COMMAND ----------

# Reload from Delta so Phase 2 can be re-run independently
df_turbines = spark.table(f"{CATALOG}.silver.availability_genai_prepared")

# COMMAND ----------

telemetry_ref_cols = ["wind_turbine_code", "substation_code", "farm_code", "manufacturer", "serial_number"]

telemetry_ref = (
    spark.table(f"{CATALOG}.gold.wind_turbine_ltsa_scope")
    .select(*telemetry_ref_cols)
    # serial_number_country: serial_number + '_WIF_' + chars 5-6 of farm_code
    .withColumn(
        "serial_number_country",
        F.concat(F.col("serial_number"), F.lit("_WIF_"), F.substring("farm_code", 5, 2))
    )
)

print(f"Telemetry reference turbines: {telemetry_ref.count()}")

# COMMAND ----------

telemetry_ref.display()

# COMMAND ----------

output_cols = [
    "type", "turbine_id", "file",
    "maintainer_month", "maintainer_year", "maintainer_availability",
    "maintainer_date", "reporting_date", "run_date",
    "wind_turbine_code", "substation_code", "farm_code", "manufacturer",
]
no_match_cols = [c for c in output_cols
                 if c not in {"wind_turbine_code", "substation_code", "farm_code", "manufacturer"}]

# Broadcast telemetry_ref (small dimension table)
telemetry_bc = F.broadcast(
    telemetry_ref.select(
        "wind_turbine_code", "substation_code", "farm_code", "manufacturer",
        F.col("serial_number_country").alias("_sc"),
    )
)

# Step 1 — match on serial_number_1_country
matched_sn1 = (
    df_turbines
    .join(telemetry_bc, F.col("serial_number_1_country") == F.col("_sc"), "inner")
    .drop("_sc")
)
unmatched_sn1 = (
    df_turbines
    .join(telemetry_bc, F.col("serial_number_1_country") == F.col("_sc"), "anti")
)

# Step 2 — match remaining rows on serial_number_2_country
matched_sn2 = (
    unmatched_sn1
    .join(telemetry_bc, F.col("serial_number_2_country") == F.col("_sc"), "inner")
    .drop("_sc")
)
unmatched = (
    unmatched_sn1
    .join(telemetry_bc, F.col("serial_number_2_country") == F.col("_sc"), "anti")
)

final_matched = matched_sn1.unionByName(matched_sn2, allowMissingColumns=True)

print(f"Matched  : {final_matched.count()}")
print(f"Unmatched: {unmatched.count()}")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

# ---------------------------------------------------------------------------
# Step 3 & 4 — Tolerant matching on the exact-match residuals
# Replaces the former rapidfuzz partial_ratio (substring) step, which attached
# short report labels to the wrong farm ("101" → Guijos WT001 from Alentisque
# reports, "LRT01" → Alentisque T01 from Les Rotes, "61002982" → Jaulín from
# Alisio). Rules live in utils.serial_match (normalised equality / whole-token /
# shared >=5-digit run, unique candidate only — never guess).
# Resolved on the DRIVER per distinct (country, sn1, sn2) key: the key set is
# small even when the residual has ~100k rows, then joined back.
# ---------------------------------------------------------------------------

_DARWIN_EXTRA_COLS = ["wind_turbine_code", "substation_code", "farm_code", "manufacturer"]

telemetry_pd = (
    telemetry_ref
    .select("serial_number", *_DARWIN_EXTRA_COLS)
    .dropna(subset=["serial_number"])
    .toPandas()
)
telemetry_pd["_cc"] = telemetry_pd["farm_code"].str[4:6]               # "WIF_FR" → "FR"
candidates_by_cc = {
    cc: grp.drop(columns="_cc").to_dict("records")
    for cc, grp in telemetry_pd.groupby("_cc")
}

_alentisque_file = F.when(
    F.lower(F.col("file")).rlike(r"(^|/)spain/vestas/.*/[^/]*alentisque[^/]*\.pdf$"),
    F.col("file"),
)
# No leading underscore: itertuples() renames such fields (_match_file → _0).
keys_pd = unmatched.select(
    _alentisque_file.alias("match_file"),
    "pdf_country", "serial_number_1", "serial_number_2",
).distinct().toPandas()

_resolved = []
for k in keys_pd.itertuples(index=False):
    cands = candidates_by_cc.get(k.pdf_country, [])
    ref = (
        match_serial(k.serial_number_1, cands, report_file=k.match_file)
        or match_serial(k.serial_number_2, cands, report_file=k.match_file)
    )
    if ref is not None:
        _resolved.append({
            "_k_file": k.match_file, "_k_cc": k.pdf_country,
            "_k_sn1": k.serial_number_1, "_k_sn2": k.serial_number_2,
            **{c: ref[c] for c in _DARWIN_EXTRA_COLS},
        })
print(f"Residual keys: {len(keys_pd)} | resolved by tolerant match: {len(_resolved)}")

_resolved_schema = StructType(
    [StructField(c, StringType(), True) for c in ["_k_file", "_k_cc", "_k_sn1", "_k_sn2", *_DARWIN_EXTRA_COLS]]
)
resolved_df = spark.createDataFrame(_resolved, schema=_resolved_schema)

_key_join = (
    _alentisque_file.eqNullSafe(F.col("_k_file"))
    & F.col("pdf_country").eqNullSafe(F.col("_k_cc"))
    & F.col("serial_number_1").eqNullSafe(F.col("_k_sn1"))
    & F.col("serial_number_2").eqNullSafe(F.col("_k_sn2"))
)
fuzzy_matched = (
    unmatched.join(F.broadcast(resolved_df), _key_join, "inner")
    .drop("_k_file", "_k_cc", "_k_sn1", "_k_sn2")
)
unmatched = (
    unmatched.join(F.broadcast(resolved_df), _key_join, "left_anti")
    .select(*no_match_cols)
)
print(f"Fuzzy matched : {fuzzy_matched.count()}")

final_matched = final_matched.unionByName(
    fuzzy_matched.select(output_cols), allowMissingColumns=True
)

print(f"Total matched (exact + fuzzy): {final_matched.count()}")
print(f"Remaining unmatched          : {unmatched.count()}")

# COMMAND ----------

final_matched.select(output_cols).write\
    .format("delta").mode("overwrite").option("mergeSchema","true")\
    .saveAsTable(f"{CATALOG}.silver.availability_genai_telemetry_match")
print(f"Written to {CATALOG}.silver.availability_genai_telemetry_match")

unmatched.select(no_match_cols).write\
    .format("delta").mode("overwrite").option("mergeSchema","true")\
    .saveAsTable(f"{CATALOG}.silver.availability_genai_telemetry_no_match")
print(f"Written to {CATALOG}.silver.availability_genai_telemetry_no_match")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 3 — Site availability

# COMMAND ----------

# Site rows (type == 'site'): re-apply same renames as Phase 1
df_site = (
    drop_months_after_report(spark.table(f"{CATALOG}.silver.availability_genai"))
    .withColumnRenamed("value", "maintainer_availability")
    .withColumnRenamed("date",  "maintainer_date")
    .withColumn("maintainer_year",  F.substring("maintainer_date", 1, 4))
    .withColumn("maintainer_month", F.substring("maintainer_date", 6, 2))
    .filter(F.col("type") == "site")
)

print(f"Site rows: {df_site.count()}")

# COMMAND ----------

df_site.write.format("delta").mode("overwrite").option("mergeSchema","true").saveAsTable(
    f"{CATALOG}.silver.availability_genai_site_availability"
)
print(f"Written to {CATALOG}.silver.availability_genai_site_availability")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 4 — File mapping classification

# COMMAND ----------

# Case constants (mirrors FileMappingCase enum from Dataiku compute_mapping_file_pic)
CASE_SINGLE_SUB = "FILE_SINGLE_SUBSTATION"    # 1 farm, 1 sub, 1 maintainer in farm → farm + substation
CASE_MULTI_SUB  = "FILE_MULTIPLE_SUBSTATIONS"  # 1 farm, N subs, 1 maintainer in farm → farm only
CASE_SUB_ONLY   = "FILE_SUBSTATION_ONLY"       # 1 farm, 1 sub, N maintainers in farm → substation only
CASE_UNMATCHED  = "FILE_UNMATCHED"

df_match = spark.table(f"{CATALOG}.silver.availability_genai_telemetry_match")
print(f"telemetry_match rows: {df_match.count()}, unique files: {df_match.select('file').distinct().count()}")

# COMMAND ----------

# Count distinct maintainers per farm from the global reference (wind_turbine_ltsa_scope),
# NOT from the matched rows — mirrors Dataiku compute_mapping_file_pic behaviour.
# This ensures a file covering only 1 substation in a multi-maintainer farm is correctly
# classified as FILE_SUBSTATION_ONLY instead of FILE_SINGLE_SUBSTATION.
farm_maint_count = F.broadcast(
    spark.table(f"{CATALOG}.gold.wind_turbine_ltsa_scope")
    .groupBy("farm_code")
    .agg(F.countDistinct("manufacturer").alias("n_maintainers_in_farm"))
)

# Compute per-file stats then classify
file_stats = (
    df_match
    .groupBy("file")
    .agg(
        F.countDistinct("farm_code").alias("n_farms"),
        F.countDistinct("substation_code").alias("n_substations"),
        F.first("farm_code").alias("farm_code"),  # safe: only used when n_farms == 1
    )
    .join(farm_maint_count, on="farm_code", how="left")
    .fillna({"n_maintainers_in_farm": 0})
    .withColumn(
        "mapping_case",
        F.when(F.col("n_farms") > 1, CASE_UNMATCHED)
         .when(
             (F.col("n_farms") == 1) & (F.col("n_substations") == 1)
             & (F.col("n_maintainers_in_farm") == 1),
             CASE_SINGLE_SUB)
         .when(
             (F.col("n_farms") == 1) & (F.col("n_substations") == 1)
             & (F.col("n_maintainers_in_farm") > 1),
             CASE_SUB_ONLY)
         .when(
             (F.col("n_farms") == 1) & (F.col("n_substations") > 1)
             & (F.col("n_maintainers_in_farm") == 1),
             CASE_MULTI_SUB)
         .otherwise(CASE_UNMATCHED)
    )
    .select("file", "mapping_case")
)

df_match = df_match.join(F.broadcast(file_stats), on="file", how="left")

print("File classification summary:")
df_match.groupBy("mapping_case").count().show()

# COMMAND ----------

# mapping_file_turbine: all telemetry_match rows, wind_turbine_code → ent_code
df_turbine_map = (
    df_match
    .drop("mapping_case")
    .withColumnRenamed("wind_turbine_code", "ent_code")
    .withColumn("maintainer_month", F.col("maintainer_month").cast("int"))
    .withColumn("maintainer_year",  F.col("maintainer_year").cast("int"))
)

df_turbine_map.write.format("delta").mode("overwrite").option("mergeSchema","true").saveAsTable(
    f"{CATALOG}.silver.mapping_file_turbine"
)
print(f"Written to {CATALOG}.silver.mapping_file_turbine: {df_turbine_map.count()} rows")

# COMMAND ----------

farm_cases = [CASE_SINGLE_SUB, CASE_MULTI_SUB]
farm_cols  = ["file", "farm_code", "manufacturer",
              "maintainer_availability", "maintainer_date",
              "maintainer_year", "maintainer_month",
              "reporting_date", "run_date", "mapping_case"]

# mapping_file_farm: one row per file — FILE_SINGLE_SUBSTATION + FILE_MULTIPLE_SUBSTATIONS
df_farm_out = (
    df_match
    .filter(F.col("mapping_case").isin(farm_cases))
    .select(*farm_cols)
    .dropDuplicates(["file"])
    .withColumn("type", F.lit("site"))
)

df_farm_out.write.format("delta").mode("overwrite").option("mergeSchema","true").saveAsTable(
    f"{CATALOG}.silver.mapping_file_farm"
)
print(f"Written to {CATALOG}.silver.mapping_file_farm: {df_farm_out.count()} rows")

# mapping_file_substation: FILE_SINGLE_SUBSTATION + FILE_SUBSTATION_ONLY
# (FILE_SUBSTATION_ONLY = 1 farm, 1 sub, multi-maintainer → substation only, no farm availability)
sub_cases = [CASE_SINGLE_SUB, CASE_SUB_ONLY]
sub_cols = ["file", "substation_code", "farm_code", "manufacturer",
            "maintainer_availability", "maintainer_date",
            "maintainer_year", "maintainer_month",
            "reporting_date", "run_date", "mapping_case"]

df_substation_out = (
    df_match
    .filter(F.col("mapping_case").isin(sub_cases))
    .select(*sub_cols)
    .dropDuplicates(["file", "substation_code", "manufacturer"])
    .withColumn("type", F.lit("substation"))
)

df_substation_out.write.format("delta").mode("overwrite").option("mergeSchema","true").saveAsTable(
    f"{CATALOG}.silver.mapping_file_substation"
)
print(f"Written to {CATALOG}.silver.mapping_file_substation: {df_substation_out.count()} rows")

# mapping_file_classify_fail: FILE_UNMATCHED rows
df_classify_fail = df_match.filter(F.col("mapping_case") == CASE_UNMATCHED)

df_classify_fail.write.format("delta").mode("overwrite").option("mergeSchema","true").saveAsTable(
    f"{CATALOG}.silver.mapping_file_classify_fail"
)
print(f"Written to {CATALOG}.silver.mapping_file_classify_fail: {df_classify_fail.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 5 — Gold fact tables with dimension FKs

# COMMAND ----------

# Load dimension references for Kimball FK resolution
dim_turbine_ref = spark.table(f"{CATALOG}.gold.dim_turbine").select(
    F.col("id").alias("turbine_id"), F.col("code").alias("_wt_code")
)
dim_substation_ref = spark.table(f"{CATALOG}.gold.dim_substation").select(
    F.col("id").alias("substation_id"), F.col("code").alias("_sub_code")
)
dim_farm_ref = spark.table(f"{CATALOG}.gold.dim_farm").select(
    F.col("id").alias("farm_id"), F.col("code").alias("_farm_code")
)

# COMMAND ----------

# fact_genai_turbine_availability
# Source: mapping_file_turbine (ent_code = wind_turbine_code, year/month as Int64)
df_map_turbine = spark.table(f"{CATALOG}.silver.mapping_file_turbine")

fact_turbine = (
    df_map_turbine
    .withColumnRenamed("turbine_id", "turbine_sn")
    .join(dim_turbine_ref, F.col("ent_code") == F.col("_wt_code"), "left")
    .drop("_wt_code")
    .join(dim_substation_ref, F.col("substation_code") == F.col("_sub_code"), "left")
    .drop("_sub_code")
    .join(dim_farm_ref, F.col("farm_code") == F.col("_farm_code"), "left")
    .drop("_farm_code")
    .select(
        "turbine_id",
        "substation_id",
        "farm_id",
        F.col("maintainer_year").cast("int").alias("year"),
        F.col("maintainer_month").cast("int").alias("month"),
        F.col("maintainer_availability").alias("availability"),
        "maintainer_date",
        "file",
        "manufacturer",
        "reporting_date",
        "run_date",
    )
)

fact_turbine.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.gold.fact_genai_turbine_availability"
)
print(f"Written fact_genai_turbine_availability: {fact_turbine.count()} rows")

# COMMAND ----------

# fact_genai_substation_availability
# Source: mapping_file_substation (file × substation_code mapping)
#         + availability_genai_site_availability (site-level availability from the LLM)
# NOTE: availability_genai has no type='substation' rows — the LLM only produces
#       type='turbine' and type='site'. Site-level availability is the correct
#       source for substation granularity (mirrors Dataiku compute_mapping_file_pic).
df_map_sub = spark.table(f"{CATALOG}.silver.mapping_file_substation")

sub_avail = (
    spark.table(f"{CATALOG}.silver.availability_genai_site_availability")
    .select(
        "file",
        F.col("maintainer_availability").alias("_sub_availability"),
        F.col("maintainer_date").alias("_sub_date"),
        F.col("maintainer_year").alias("_sub_year"),
        F.col("maintainer_month").alias("_sub_month"),
    )
)

fact_substation = (
    df_map_sub
    .drop("maintainer_availability", "maintainer_date", "maintainer_year", "maintainer_month")
    .join(sub_avail, on="file", how="left")
    .join(dim_substation_ref, F.col("substation_code") == F.col("_sub_code"), "left")
    .drop("_sub_code")
    .join(dim_farm_ref, F.col("farm_code") == F.col("_farm_code"), "left")
    .drop("_farm_code")
    .select(
        "substation_id",
        "farm_id",
        F.col("_sub_year").cast("int").alias("year"),
        F.col("_sub_month").cast("string").alias("month"),
        F.col("_sub_availability").alias("availability"),
        F.col("_sub_date").alias("maintainer_date"),
        "file",
        "manufacturer",
        "reporting_date",
        "run_date",
        "mapping_case",
    )
)

fact_substation.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.gold.fact_genai_substation_availability"
)
print(f"Written fact_genai_substation_availability: {fact_substation.count()} rows")

# COMMAND ----------

# fact_genai_farm_availability
# Source: mapping_file_farm (file → farm_code / manufacturer / mapping_case)
#         + availability_genai_site_availability (correct site-level availability)
# NOTE: mapping_file_farm.maintainer_availability is turbine-level → NOT used here
df_map_farm = spark.table(f"{CATALOG}.silver.mapping_file_farm")

# Pull site-level availability from availability_genai (type='site')
site_avail = (
    spark.table(f"{CATALOG}.silver.availability_genai_site_availability")
    .select(
        "file",
        F.col("maintainer_availability").alias("_site_availability"),
        F.col("maintainer_date").alias("_site_date"),
        F.col("maintainer_year").alias("_site_year"),
        F.col("maintainer_month").alias("_site_month"),
    )
)

fact_farm = (
    df_map_farm
    .drop("maintainer_availability", "maintainer_date", "maintainer_year", "maintainer_month")
    .join(site_avail, on="file", how="left")
    .join(dim_farm_ref, F.col("farm_code") == F.col("_farm_code"), "left")
    .drop("_farm_code")
    .select(
        "farm_id",
        F.col("_site_year").cast("int").alias("year"),
        F.col("_site_month").cast("string").alias("month"),
        F.col("_site_availability").alias("availability"),
        F.col("_site_date").alias("maintainer_date"),
        "file",
        "manufacturer",
        "reporting_date",
        "run_date",
        "mapping_case",
    )
)

fact_farm.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.gold.fact_genai_farm_availability"
)
print(f"Written fact_genai_farm_availability: {fact_farm.count()} rows")

# COMMAND ----------

df_turbine_map.display()
