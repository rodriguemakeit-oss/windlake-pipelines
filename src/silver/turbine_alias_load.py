# Databricks notebook source
# MAGIC %md
# MAGIC # Turbine Alias Mapping — Silver Layer
# MAGIC
# MAGIC Loads, from `SGRE_Mapping_Table.xlsx` (SharePoint, edited by the business):
# MAGIC * the report-label → Telemetry crosswalk → `silver.dim_turbine_report_alias`
# MAGIC * the `_ignored_errors` tab (reports not to be alerted on)
# MAGIC   → `silver.files_genai_ignored`
# MAGIC
# MAGIC Both come from the same workbook, downloaded once.

# COMMAND ----------

import json

import pandas as pd
from datetime import datetime
from openpyxl import load_workbook

from helpers.sharepoint_adapter import SharePointAdapter
from helpers.genai_ignored_mapping import (
    find_ignored_sheet,
    normalize_ignored_records,
    with_structural_seeds,
)
from helpers.turbine_alias_mapping import (
    normalize_mapping_records,
    validate_mapping,
    assert_non_empty,
    find_mapping_sheet,
    pick_single_mapping_file,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import boto3

catalog = dbutils.widgets.get("catalog")

SERVICE_CREDENTIAL_NAME = dbutils.widgets.get("service_credential_name")
REGION = dbutils.widgets.get("secrets_manager_region")

provider = dbutils.credentials.getServiceCredentialsProvider(SERVICE_CREDENTIAL_NAME)
session = boto3.Session(botocore_session=provider, region_name=REGION)
sm = session.client("secretsmanager")
response = sm.get_secret_value(SecretId=dbutils.widgets.get("secrets_manager_id"))
secrets = json.loads(response["SecretString"])

sharepoint_tenant_name = dbutils.widgets.get("sharepoint_tenant_name")
sharepoint_site_name = dbutils.widgets.get("sharepoint_site_name")
mapping_folder_path = dbutils.widgets.get("mapping_folder_path")
mapping_file_name = dbutils.widgets.get("mapping_file_name")

ALIAS_TABLE = f"{catalog}.silver.dim_turbine_report_alias"

print(f"   Catalog       : {catalog}")
print(f"   SP Site       : {sharepoint_site_name}")
print(f"   SP Folder     : {mapping_folder_path}")
print(f"   Mapping file  : {mapping_file_name}")
print(f"   Target table  : {ALIAS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download the Excel from SharePoint

# COMMAND ----------

print("   🔗 Connecting to SharePoint...")
adapter = SharePointAdapter.from_secrets(
    secrets=secrets,
    tenant_name=sharepoint_tenant_name,
    site_name=sharepoint_site_name,
    folder_path=mapping_folder_path,
)
print(f"   ✅ SharePointAdapter ready — {mapping_folder_path}")


MAPPING_PREFERRED_SUBDIR = "0-ACCEPTED FORMATS/SGRE"
# Folders searched, in order: new preferred location, then legacy root.
MAPPING_FOLDERS = [MAPPING_PREFERRED_SUBDIR, ""]


def _open_workbook(folder_adapter, rel_path):
    with folder_adapter.get_download_stream(rel_path) as stream:
        return load_workbook(stream, read_only=True, data_only=True)


def _load_mapping_workbook(folder_adapter, file_name, folders):
    """Return (resolved_path, workbook) for the mapping Excel.

    1. Fast path: ``file_name`` in each folder (current behaviour).
    2. Fallback, so a RENAMED file does not break the pipeline: list the .xlsx
       directly inside those folders (non-recursive) and keep the one whose
       content holds the mapping sheet (identified by its column headers). Zero
       or several hits → explicit error, never a guess.
    """
    for folder in folders:
        rel_path = f"{folder}/{file_name}" if folder else file_name
        try:
            workbook = _open_workbook(folder_adapter, rel_path)
            print(f"   📄 Mapping file resolved at: {rel_path}")
            return rel_path, workbook
        except Exception as exc:  # noqa: BLE001 — try the next location
            print(f"   ⚠️  Not found at '{rel_path}' ({type(exc).__name__})")

    print(f"   🔎 '{file_name}' not found — searching the mapping by content (renamed file?)")
    hits = {}
    for folder in folders:
        for name in folder_adapter.list_folder_files(folder):
            if not name.lower().endswith(".xlsx") or name.startswith("~$"):
                continue
            rel_path = f"{folder}/{name}" if folder else name
            try:
                workbook = _open_workbook(folder_adapter, rel_path)
            except Exception as exc:  # noqa: BLE001 — unreadable file, skip it
                print(f"   ⚠️  Skipping unreadable '{rel_path}' ({type(exc).__name__})")
                continue
            if find_mapping_sheet(workbook) is not None:
                hits[rel_path] = workbook
    rel_path = pick_single_mapping_file(sorted(hits), folders)
    print(f"   📄 Mapping file found by content at: {rel_path} "
          f"(update mapping_file_name to skip this search)")
    return rel_path, hits[rel_path]


resolved_mapping_path, wb = _load_mapping_workbook(adapter, mapping_file_name, MAPPING_FOLDERS)


def _select_mapping_sheet(workbook):
    """Pick the worksheet whose header row contains the expected mapping columns.

    Do NOT trust ``wb.active``: a stray/extra tab (e.g. someone pasting an
    unrelated export into the workbook) can become the active sheet and break the
    load. Fall back to the active sheet so ``normalize_mapping_records`` still
    raises its clear, actionable error when no mapping sheet is present.
    """
    return find_mapping_sheet(workbook) or workbook.active


ws = _select_mapping_sheet(wb)
print(f"   \U0001F4C4 Using sheet: {ws.title!r}")
raw_rows = list(ws.iter_rows(values_only=True))
if not raw_rows:
    raise ValueError(f"Mapping Excel {resolved_mapping_path} is empty.")
header, data_rows = raw_rows[0], raw_rows[1:]
print(f"   📄 Read {len(data_rows)} row(s) from {resolved_mapping_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Normalise + validate

# COMMAND ----------

mapping_df = normalize_mapping_records(header, data_rows)
valid_df, quarantine_df = validate_mapping(mapping_df)

if not quarantine_df.empty:
    print(f"   ⚠️  {len(quarantine_df)} row(s) quarantined (duplicate keys):")
    print(quarantine_df.to_string(index=False))

assert_non_empty(valid_df, min_rows=1)
print(f"   ✅ {len(valid_df)} valid mapping row(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write `silver.dim_turbine_report_alias`

# COMMAND ----------

valid_df = valid_df.copy()
valid_df["run_date"] = datetime.now()

alias_spark = spark.createDataFrame(valid_df)
(
    alias_spark.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(ALIAS_TABLE)
)
print(f"   ✅ {ALIAS_TABLE} updated ({len(valid_df)} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write `silver.files_genai_ignored` (business-managed allowlist)
# MAGIC Reads the `_ignored_errors` tab of the **same** workbook (already
# MAGIC downloaded above — no extra network call) and refreshes the allowlist of
# MAGIC reports that must not be raised as errors in the Teams alert.

# COMMAND ----------

IGNORED_TABLE = f"{catalog}.silver.files_genai_ignored"
ignored_sheet_name = dbutils.widgets.get("ignored_sheet_name")

# The workbook is shared and edited by the business: a missing tab, a renamed
# header or an emptied sheet must NEVER wipe the allowlist (that would flood the
# alert with hundreds of known-bad files). In any of those cases we log and keep
# the table as-is.
ignored_sheet = find_ignored_sheet(wb, preferred_name=ignored_sheet_name)
if ignored_sheet is None:
    print(f"   ⚠️  Sheet {ignored_sheet_name!r} not found — {IGNORED_TABLE} left unchanged")
else:
    print(f"   📄 Using ignored sheet: {ignored_sheet.title!r}")
    ignored_raw = list(ignored_sheet.iter_rows(values_only=True))
    if not ignored_raw:
        print(f"   ⚠️  Sheet {ignored_sheet.title!r} is empty — {IGNORED_TABLE} left unchanged")
    else:
        try:
            ignored_df = normalize_ignored_records(ignored_raw[0], ignored_raw[1:])
        except ValueError as exc:
            ignored_df = None
            print(f"   ⚠️  {exc} — {IGNORED_TABLE} left unchanged")

        if ignored_df is None or ignored_df.empty:
            if ignored_df is not None:
                print(f"   ⚠️  No usable row — {IGNORED_TABLE} left unchanged")
        else:
            # Always enforce the structural out-of-scope patterns on top of the
            # business rows: the overwrite below would otherwise wipe them.
            ignored_df = with_structural_seeds(ignored_df)
            ignored_df = ignored_df.copy()
            ignored_df["run_date"] = datetime.now()
            (
                spark.createDataFrame(ignored_df).write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(IGNORED_TABLE)
            )
            counts = ignored_df["error_type"].value_counts().to_dict()
            print(f"   ✅ {IGNORED_TABLE} updated ({len(ignored_df)} rows) — {counts}")
