# Databricks notebook source
# MAGIC %md
# MAGIC # LLM Document to DataFrame — Silver Layer
# MAGIC Extracts turbine availability from PDF/Excel maintainer reports using Azure Document Intelligence + OpenAI GPT-4o.
# MAGIC
# MAGIC **Flow:**
# MAGIC 1. Read config from Databricks widgets
# MAGIC 2. Identify new files to process (Delta tracking table)
# MAGIC 3. Run LLM extraction (`LLMExtractor`)
# MAGIC 4. Write availability data → `silver.availability_genai` (Delta)
# MAGIC 5. Update tracking → `silver.files_genai_processed` (Delta)

# COMMAND ----------

# Databricks: add the silver src directory to sys.path so local modules are importable
import sys
import os
import json

# When running on Databricks, __file__ is not defined — use a fixed path to the
# silver directory (adjust the catalog/schema/volume if needed).
# _SILVER_DIR = os.path.dirname(os.path.abspath("llm_document_to_dataframe.ipynb"))
# if _SILVER_DIR not in sys.path:
#    sys.path.insert(0, _SILVER_DIR)

import json
import pandas as pd
from datetime import datetime

from helpers.volume_adapter import VolumeAdapter
from helpers.document_intelligence_helper import get_document_intelligence_client
from helpers.openai_helper import get_openai_client
from helpers.tesseract_helper import build_ocr_extract

from compute_generative_ai.enums import Maintainers, ExcelMode
from compute_generative_ai.extractors import LLMExtractor
from compute_generative_ai.processors import ResultAggregator, FileTracker
from compute_generative_ai.utils import get_prompt_yaml

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC Widgets Databricks — à renseigner dans le menu "Run > Edit Parameters" ou via `dbutils.widgets`.

# COMMAND ----------

import boto3

SERVICE_CREDENTIAL_NAME = dbutils.widgets.get("service_credential_name")
REGION = dbutils.widgets.get("secrets_manager_region")
 
provider = dbutils.credentials.getServiceCredentialsProvider(SERVICE_CREDENTIAL_NAME)
 
session = boto3.Session(
    botocore_session=provider,
    region_name=REGION
)
 
sm = session.client("secretsmanager")
 
response = sm.get_secret_value(SecretId=dbutils.widgets.get("secrets_manager_id"))
secrets = json.loads(response["SecretString"])

# COMMAND ----------

# Read widget values
AZURE_DI_ENDPOINT = secrets.get("AZURE_DI_ENDPOINT")
AZURE_DI_KEY = secrets.get("AZURE_DI_KEY")
AZURE_OPENAI_ENDPOINT = secrets.get("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_KEY = secrets.get("AZURE_OPENAI_KEY")

DEFAULT_MODEL = dbutils.widgets.get("ia_model")
max_retries = int(dbutils.widgets.get("ia_max_retries"))
ia_files_limit_per_run = int(dbutils.widgets.get("ia_files_limit_per_run"))
sleep_time = int(dbutils.widgets.get("ia_sleep_sec"))
allowed_file_extensions = json.loads(dbutils.widgets.get("allowed_file_extensions"))
debug = dbutils.widgets.get("debug").lower() in ["true", "1"]
load_pickle = dbutils.widgets.get("load_pickle").lower() in ["true", "1"]

# Tesseract
use_tesseract = dbutils.widgets.get("use_tesseract").lower() in ["true", "1"]
tesseract_lang = dbutils.widgets.get("tesseract_lang")
tesseract_dpi = int(dbutils.widgets.get("tesseract_dpi"))

reports_volume_path = dbutils.widgets.get("reports_volume_path")
data_volume_path = dbutils.widgets.get("data_volume_path")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# Excel mode — fall back to openpyxl if invalid value provided
try:
    excel_mode = ExcelMode(dbutils.widgets.get("excel_mode")).value
except ValueError:
    excel_mode = ExcelMode.OPENPYXL.value
    print(f"   ⚠️  Invalid excel_mode, falling back to {excel_mode}")

# SharePoint source (optional — set use_sharepoint=true to bypass Volume)
use_sharepoint = dbutils.widgets.get("use_sharepoint").lower() in ["true", "1"]
sharepoint_tenant_name = dbutils.widgets.get("sharepoint_tenant_name")
sharepoint_site_name = dbutils.widgets.get("sharepoint_site_name")
sharepoint_folder_path = dbutils.widgets.get("sharepoint_folder_path")

print(f"   Model         : {DEFAULT_MODEL}")
print(f"   Excel mode    : {excel_mode}")
print(f"   Extensions    : {allowed_file_extensions}")
print(f"   Files limit   : {ia_files_limit_per_run}")
print(f"   Debug         : {debug}")
print(f"   Use Tesseract : {use_tesseract}")
print(f"   Use SharePoint: {use_sharepoint}")
if use_sharepoint:
    print(f"   SP Site       : {sharepoint_site_name}")
    print(f"   SP Folder     : {sharepoint_folder_path}")
else:
    print(f"   Reports path  : {reports_volume_path}")
print(f"   Data path     : {data_volume_path}")
print(f"   Catalog       : {catalog}")
print(f"   Schema        : {schema}")
print(f"   Load pickle   : {load_pickle}")
print(f"   Max retries   : {max_retries}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Azure Clients

# COMMAND ----------

print("   ├─ 🔧 Creating Azure Document Intelligence client")
ocr_client = get_document_intelligence_client(
    azure_di_endpoint=AZURE_DI_ENDPOINT,
    azure_di_key=AZURE_DI_KEY,
)

print("   ├─ 🔧 Creating Azure OpenAI client")
generative_ai_client = get_openai_client(
    openai_endpoint=AZURE_OPENAI_ENDPOINT,
    openai_key=AZURE_OPENAI_KEY,
)

print("✅ Azure clients ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fichiers à traiter
# MAGIC Lecture du tracking Delta pour ne traiter que les nouveaux fichiers du Volume.

# COMMAND ----------

# ---------------------------------------------------------------------------
# Load previously processed files from Delta tracking table.
# Schema: file (STRING), run_date (TIMESTAMP), error (STRING)
# ---------------------------------------------------------------------------
TRACKING_TABLE = f"{catalog}.{schema}.files_genai_processed"

try:
    files_genai_processed_df = (
        spark.table(TRACKING_TABLE)
        .select("file")
        .toPandas()
    )
    already_processed = set(files_genai_processed_df["file"].tolist())
    print(f"   ℹ️  {len(already_processed)} files already processed (from {TRACKING_TABLE})")
except Exception as e:
    already_processed = set()
    print(f"   ⚠️  Tracking table not found or empty ({e}). All files will be processed.")

# COMMAND ----------

# ---------------------------------------------------------------------------
# List available files and filter out already processed ones.
# files_metadata is a DataFrame with columns:
#   file, maintainer, country, year, month, extension
#
# reports_adapter is either a VolumeAdapter (default) or a SharePointAdapter
# depending on the use_sharepoint widget. Both implement the FolderAdapter
# Protocol so the rest of the notebook is unchanged.
# ---------------------------------------------------------------------------
if use_sharepoint:
    from helpers.sharepoint_adapter import SharePointAdapter
    print("   🔗 Connecting to SharePoint...")
    reports_adapter = SharePointAdapter.from_secrets(
        secrets=secrets,
        tenant_name=sharepoint_tenant_name,
        site_name=sharepoint_site_name,
        folder_path=sharepoint_folder_path,
    )
    print(f"   ✅ SharePointAdapter ready — {sharepoint_folder_path}")
else:
    reports_adapter = VolumeAdapter(reports_volume_path)
    print(f"   ✅ VolumeAdapter ready — {reports_volume_path}")

all_files = reports_adapter.list_files()

# COMMAND ----------

# ---------------------------------------------------------------------------
# Build files_metadata DataFrame from the Volume file paths.
#
# Expected Volume structure (one folder per country/maintainer):
#   <country>/<maintainer>/<year>/<month>-.../<filename>.<ext>
#   e.g.  france/vestas/2024/01-JAN/report_jan2024.pdf
#
# Month segment may be a plain integer ("01") or have a label suffix ("01-JAN").
# If your Volume uses a different structure, adjust _parse_file_path() below.
#
# For GE Excel files, year/month is also read from the "Cover" sheet which is
# more reliable than the folder structure (users may upload to the wrong folder).
# The scanner looks for a "Month Year" pattern in any cell (not hardcoded to B13)
# and accepts multiple date formats (full/abbreviated names, numeric, etc.).
# ---------------------------------------------------------------------------

import re as _re
import calendar as _calendar
from openpyxl import load_workbook as _load_workbook

# Build month-name lookup (English): january->1, jan->1, ...
_MONTH_NAMES = {}
for _i in range(1, 13):
    _MONTH_NAMES[_calendar.month_name[_i].lower()] = _i
    _MONTH_NAMES[_calendar.month_abbr[_i].lower()] = _i

# Regex: "January 2025" | "Jan 2025" | "01/2025" | "2025-01" etc.
_MONTH_YEAR_RE = _re.compile(
    r"(?:"
    r"(?P<mname>[A-Za-z]+)\s+(?P<y1>\d{4})"
    r"|(?P<m2>\d{1,2})[/\-.](?P<y2>\d{4})"
    r"|(?P<y3>\d{4})[/\-.](?P<m3>\d{1,2})"
    r")",
    _re.IGNORECASE,
)


def _parse_month_year(text) -> tuple:
    """Try to extract (year, month) from a free-text string.

    Supports: 'January 2025', 'Jan 2025', '01/2025', '2025-06', etc.
    Returns (0, 0) on failure.
    """
    if not isinstance(text, str):
        return 0, 0
    m = _MONTH_YEAR_RE.search(text.strip())
    if not m:
        return 0, 0
    if m.group("mname"):
        month = _MONTH_NAMES.get(m.group("mname").lower(), 0)
        return (int(m.group("y1")), month) if month else (0, 0)
    if m.group("m2"):
        month = int(m.group("m2"))
        return (int(m.group("y2")), month) if 1 <= month <= 12 else (0, 0)
    if m.group("m3"):
        month = int(m.group("m3"))
        return (int(m.group("y3")), month) if 1 <= month <= 12 else (0, 0)
    return 0, 0


def _parse_file_path(file_path: str) -> dict:
    """Extract metadata from the Volume file path.

    Expected path format: <country>/<maintainer>/<year>/<month>/<filename>.<ext>
    Month segment may be a plain integer ('01') or have a label suffix ('01-JAN').
    Missing segments default to "unknown" / 0 so the extractor never crashes.
    """
    parts = file_path.replace("\\", "/").split("/")
    ext = file_path.rsplit(".", 1)[-1].lower() if "." in file_path else "unknown"
    _month_raw = parts[3].split("-")[0] if len(parts) > 3 else ""
    return {
        "file": file_path,
        "country": parts[0] if len(parts) > 0 else "unknown",
        "maintainer":    parts[1] if len(parts) > 1 else "unknown",
        "year":       int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0,
        "month":      int(_month_raw) if _month_raw.isdigit() else 0,
        "extension":  ext,
    }

# COMMAND ----------

# Keep only supported extensions
new_files = [
    f for f in all_files
    if f.split(".")[-1].lower() in allowed_file_extensions
    and f not in already_processed
]

print(f"   📋 Total files in source : {len(all_files)}")
print(f"   📋 New files to process  : {len(new_files)}")

# Early exit if nothing to do — avoids touching files_metadata before it has the right columns
if not new_files:
    print("   ℹ️  No new files to process. Stopping.")
    dbutils.notebook.exit("NO_NEW_FILES")


def _extract_month_year_from_cover(file_path: str, adapter) -> tuple:
    """Scan the 'Cover' sheet of a GE Excel file for a month/year value.

    Not tied to a specific cell — scans all cells in the first 50 rows.
    Returns (year: int, month: int) or (0, 0) on failure.
    """
    try:
        with adapter.get_download_stream(file_path) as stream:
            wb = _load_workbook(stream, read_only=True, data_only=True)
            if "Cover" not in wb.sheetnames:
                wb.close()
                return 0, 0
            ws = wb["Cover"]
            for row in ws.iter_rows(max_row=50, max_col=10, values_only=True):
                for val in row:
                    if val is None:
                        continue
                    year, month = _parse_month_year(str(val))
                    if year and month:
                        wb.close()
                        return year, month
            wb.close()
        return 0, 0
    except Exception:
        return 0, 0


# Build files_metadata from path
files_metadata = pd.DataFrame([_parse_file_path(f) for f in new_files])

# For GE Excel files, override year/month from the Cover sheet (more reliable
# than the folder path, since users may upload files to the wrong folder).
ge_excel_mask = (
    (files_metadata["maintainer"] == "ge")
    & (files_metadata["extension"].isin(["xlsx", "xls"]))
)
if ge_excel_mask.any():
    print("   📖 Reading year/month from GE Cover sheets...")
    for idx in files_metadata[ge_excel_mask].index:
        fp = files_metadata.at[idx, "file"]
        cover_year, cover_month = _extract_month_year_from_cover(fp, reports_adapter)
        if cover_year and cover_month:
            path_year = files_metadata.at[idx, "year"]
            path_month = files_metadata.at[idx, "month"]
            if (cover_year, cover_month) != (path_year, path_month):
                print(f"   ⚠️  Cover sheet override for {fp}: "
                      f"{path_year}/{path_month:02d} → {cover_year}/{cover_month:02d}")
            files_metadata.at[idx, "year"] = cover_year
            files_metadata.at[idx, "month"] = cover_month
        else:
            print(f"   ⚠️  Cannot read Cover sheet for {fp}, using path-based year/month")

# Single validation pass — AFTER Cover override so GE Excel files with bad paths
# but valid Cover sheets are kept.
# Files whose year/month cannot be derived are recorded here so they get written
# to the tracking table below — otherwise they are re-listed as "new" and
# re-attempted on every single run (endless reprocessing).
skipped_metadata_errors: dict = {}
invalid_meta = files_metadata[(files_metadata["month"] == 0) | (files_metadata["year"] == 0)]
if not invalid_meta.empty:
    print(f"   ⚠️  {len(invalid_meta)} file(s) skipped — cannot extract year/month:")
    for f in invalid_meta["file"].tolist():
        skipped_metadata_errors[f] = "Skipped — cannot derive year/month from path"
        print(f"       • {f}")
    files_metadata = files_metadata[
        (files_metadata["month"] != 0) & (files_metadata["year"] != 0)
    ].reset_index(drop=True)

# Apply per-run limit
files_metadata = files_metadata.head(ia_files_limit_per_run).reset_index(drop=True)
new_files_subset = files_metadata["file"].tolist()

print(f"   📋 Processing this run   : {len(new_files_subset)}")

if not new_files_subset:
    print("   ℹ️  No valid files to process. Stopping.")
    dbutils.notebook.exit("NO_NEW_FILES")

print(files_metadata.head(10).to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## (Optionnel) Tesseract OCR en amont
# MAGIC Activé via le widget `use_tesseract=true`.  
# MAGIC Produit un DataFrame `ocr_extract` (`file`, `page`, `metadata`, `text`) identique au dataset Dataiku d'origine.  
# MAGIC Permet à `filter_data()` de cibler uniquement les pages pertinentes avant d'appeler Document Intelligence.
# MAGIC
# MAGIC > **Prérequis cluster** : init script avec `apt-get install -y tesseract-ocr tesseract-ocr-fra tesseract-ocr-spa` + `pip install pytesseract pdf2image`.

# COMMAND ----------

if use_tesseract:
    print("   🔍 Running Tesseract OCR pre-processing...")
    ocr_extract = build_ocr_extract(
        files=new_files_subset,
        folder=reports_adapter,
        lang=tesseract_lang,
        dpi=tesseract_dpi,
    )
    print(f"   ✅ Tesseract done — {len(ocr_extract)} page rows for {ocr_extract['file'].nunique()} file(s)")
else:
    ocr_extract = None
    print("   ℹ️  Tesseract disabled — Document Intelligence will process full documents")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prompt config & custom prompts

# COMMAND ----------

# Loaded from compute_generative_ai/resources/prompts.yaml (bundled in this repo)
prompt_config = get_prompt_yaml()

custom_prompts_yaml = prompt_config.get("custom_prompts")
custom_prompts = {Maintainers(m): v for m, v in custom_prompts_yaml.items()}

print(f"✅ Prompt config loaded — maintainers: {list(custom_prompts.keys())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extraction LLM

# COMMAND ----------

data_adapter = VolumeAdapter(data_volume_path)


def _build_expected_turbines_fn():
    """(country, turbine_ids) -> Telemetry WTG count of the matched substation(s).

    Lets the completeness pass skip its extra LLM call when the first answer
    already has as many turbines as Telemetry. Reuses the gold matcher
    (utils.serial_match) so extraction and matching agree. Any failure → None
    (the completeness call is then always made, as before).
    """
    try:
        _gold_dir = os.path.abspath(os.path.join(os.getcwd(), "..", "gold"))
        if _gold_dir not in sys.path:
            sys.path.append(_gold_dir)
        from utils.serial_match import match_serial
        from utils.turbine_alias import COUNTRY_TO_PIC_CODE
        from compute_generative_ai.completeness import expected_turbine_count

        telemetry_pd = (
            spark.table(f"{catalog}.gold.wind_turbine_ltsa_scope")
            .select("serial_number", "wind_turbine_code", "substation_code", "farm_code")
            .dropna(subset=["serial_number"])
            .toPandas()
        )
        telemetry_pd["_cc"] = telemetry_pd["farm_code"].str[4:6]
        by_cc = {cc: g.to_dict("records") for cc, g in telemetry_pd.groupby("_cc")}

        def _fn(country, turbine_ids):
            cc = COUNTRY_TO_PIC_CODE.get(str(country).strip().lower())
            cands = by_cc.get(cc, [])
            if not cands:
                return None
            return expected_turbine_count(turbine_ids, cands, match_serial)

        print(f"   ✅ Telemetry turbine counts loaded ({len(telemetry_pd)} WTG) for the completeness pass")
        return _fn
    except Exception as exc:  # noqa: BLE001
        print(f"   ⚠️  Telemetry turbine counts unavailable ({exc}) — completeness pass always runs")
        return None


extractor = LLMExtractor(
    files=new_files_subset,
    maintainers_report=reports_adapter,
    data_folder=data_adapter,
    generative_ai_client=generative_ai_client,
    ocr_client=ocr_client,
    prompt_config=prompt_config,
    custom_prompts=custom_prompts,
    allowed_file_extensions=allowed_file_extensions,
    excel_mode=excel_mode,
    model=DEFAULT_MODEL,
    files_metadata=files_metadata,
    # ocr_extract: populated by Tesseract cell if use_tesseract=true, else None
    # When None → Document Intelligence processes the full document (no page filtering)
    # When set → filter_data() targets only relevant pages before calling DI
    ocr_extract=ocr_extract,
    max_retries=max_retries,
    debug=debug,
    load_pickle=load_pickle,
    sleep_time=sleep_time,
    expected_turbines_fn=_build_expected_turbines_fn(),
)

results, errors = extractor.run()

print(f"\n📊 Results  : {len(results)} files processed")
print(f"❌ Errors   : {len(errors)} files failed")
if errors:
    for file, err in errors.items():
        print(f"   • {file}: {err}")

# COMMAND ----------


# ---------------------------------------------------------------------------
# Quarantine — log files that failed extraction to a Delta table.
# Any file whose extraction raised an exception (SharePoint error, DI timeout,
# OpenAI failure, …) is recorded here so operators can track and reprocess.
# The corresponding .pkl was NOT written (guarded in LLMExtractor).
# ---------------------------------------------------------------------------
QUARANTINE_TABLE = f"{catalog}.{schema}.files_genai_quarantine"

if errors:
    quarantine_df = spark.createDataFrame(
        [
            {"file": f, "error": str(e), "run_date": datetime.now()}
            for f, e in errors.items()
        ]
    )
    (
        quarantine_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(QUARANTINE_TABLE)
    )
    print(f"   ⚠️  {len(errors)} file(s) quarantined → {QUARANTINE_TABLE}")
    for f, e in errors.items():
        print(f"       • {f}: {e}")
else:
    print("   ✅ No errors — quarantine table not updated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Écriture des résultats — `availability_genai`

# COMMAND ----------

print("OK")

# COMMAND ----------

import re

AVAILABILITY_TABLE = f"{catalog}.{schema}.availability_genai"

# Track files quarantined here due to invalid date format from the LLM.
# Merged into `errors` before writing the tracking table.
date_format_errors: dict = {}

# Track files quarantined because the LLM produced availability for a month AFTER
# the report's own month (date hallucination — see SGRE/Gamesa contractual-year
# tables mis-anchored +1 year). Merged into the tracking exclusion like the above.
future_date_errors: dict = {}

# Files that successfully wrote at least one availability row this run.
# Used below to detect "silent drop" extractions (df produced but 0 usable rows).
empty_extraction_errors: dict = {}
_written_files: set = set()

if len(results) > 0:
    availability_data = ResultAggregator.prepare_availability_data(results=results)

    if not availability_data.empty:
        # -----------------------------------------------------------------------
        # Validate date column: the LLM is prompted to return YYYY-MM keys but
        # sometimes produces 2025-Jan, 01-2025, Jan-25, etc.
        # Files with any non-conforming date are quarantined entirely — partial
        # data from the same file is considered unreliable.
        # -----------------------------------------------------------------------
        _valid_date = availability_data["date"].fillna("").str.match(r"^\d{4}-\d{2}$")
        _bad_rows = availability_data[~_valid_date]

        if not _bad_rows.empty:
            _bad_files = _bad_rows["file"].unique().tolist()
            print(f"   ⚠️  {len(_bad_files)} file(s) with invalid LLM date format — quarantining:")
            for _f in _bad_files:
                _invalid_dates = (
                    _bad_rows.loc[_bad_rows["file"] == _f, "date"]
                    .unique().tolist()
                )
                _msg = f"Invalid date format(s) returned by LLM (expected YYYY-MM): {_invalid_dates}"
                date_format_errors[_f] = _msg
                print(f"       • {_f}: {_invalid_dates}")

            # Write bad files to quarantine table
            _quarantine_date_df = spark.createDataFrame(
                [
                    {"file": _f, "error": date_format_errors[_f], "run_date": datetime.now()}
                    for _f in _bad_files
                ]
            )
            (
                _quarantine_date_df.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(QUARANTINE_TABLE)
            )
            print(f"   ⚠️  {len(_bad_files)} file(s) quarantined → {QUARANTINE_TABLE}")

            # Remove bad files from availability data
            availability_data = availability_data[
                ~availability_data["file"].isin(_bad_files)
            ].reset_index(drop=True)

        # -----------------------------------------------------------------------
        # GUARD-RAIL — future-dated availability.
        # A maintainer report can NEVER contain availability for a month after
        # its own report month. When the LLM mis-anchors a contractual-year
        # table it shifts months into the future (observed on SGRE/Gamesa
        # reports: a Sep-2025 report produced 2025-10 … 2026-09). We quarantine
        # any file that produced at least one future-dated row — the whole file
        # is considered unreliable (same policy as an invalid date format).
        # Report month is derived from the Volume/SharePoint path:
        #   <country>/<maintainer>/<year>/<month>[-LABEL]/<filename>
        # -----------------------------------------------------------------------
        if not availability_data.empty:
            def _report_ym_from_path(path: str) -> str:
                p = str(path).replace("\\", "/")
                # Prefer an explicit YYYY[-_]MM at the START of the filename: some
                # maintainers (e.g. DWT) file a report in the wrong month folder
                # (a "2026_06_..." report dropped in the .../5-MAY/ folder), which
                # made the folder-based month wrongly flag the report's own month
                # as "future". The filename prefix is authoritative when present.
                fname = p.split("/")[-1]
                m = re.match(r"(20\d{2})[-_](0[1-9]|1[0-2])", fname)
                if m:
                    return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}"
                parts = p.split("/")
                if len(parts) < 4:
                    return ""
                _y = parts[2]
                _m = parts[3].split("-")[0]
                if _y.isdigit() and _m.isdigit():
                    return f"{int(_y):04d}-{int(_m):02d}"
                return ""

            availability_data["_report_ym"] = availability_data["file"].map(
                _report_ym_from_path
            )
            _future_mask = (availability_data["_report_ym"] != "") & (
                availability_data["date"] > availability_data["_report_ym"]
            )
            _future_rows = availability_data[_future_mask]

            if not _future_rows.empty:
                _future_files = _future_rows["file"].unique().tolist()
                print(
                    f"   ⚠️  {len(_future_files)} file(s) with FUTURE-dated "
                    f"availability (LLM date hallucination) — quarantining:"
                )
                for _f in _future_files:
                    _fdates = sorted(
                        _future_rows.loc[_future_rows["file"] == _f, "date"]
                        .unique().tolist()
                    )
                    _rym = _report_ym_from_path(_f)
                    future_date_errors[_f] = (
                        f"Future-dated availability vs report month {_rym} "
                        f"(LLM date hallucination): {_fdates}"
                    )
                    print(f"       • {_f} (report {_rym}): {_fdates}")

                _quarantine_future_df = spark.createDataFrame(
                    [
                        {
                            "file": _f,
                            "error": future_date_errors[_f],
                            "run_date": datetime.now(),
                        }
                        for _f in _future_files
                    ]
                )
                (
                    _quarantine_future_df.write
                    .format("delta")
                    .mode("append")
                    .option("mergeSchema", "true")
                    .saveAsTable(QUARANTINE_TABLE)
                )
                print(f"   ⚠️  {len(_future_files)} file(s) quarantined → {QUARANTINE_TABLE}")

                availability_data = availability_data[
                    ~availability_data["file"].isin(_future_files)
                ].reset_index(drop=True)

            availability_data = availability_data.drop(
                columns=["_report_ym"], errors="ignore"
            )

        if not availability_data.empty:
            print(f"   📊 Writing {len(availability_data)} availability records → {AVAILABILITY_TABLE}")
            _written_files = set(availability_data["file"].unique())

            # Convert Pandas → Spark and append to the Delta table
            availability_spark = spark.createDataFrame(availability_data)
            (
                availability_spark.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(AVAILABILITY_TABLE)
            )
            print(f"   ✅ availability_genai updated")
        else:
            print("   ⚠️  No availability data to write (all records had invalid date format or extractions failed)")
    else:
        print("   ⚠️  No availability data to write (all extractions may have failed)")
else:
    print("   ℹ️  No results to write")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mise à jour du tracking — `files_genai_processed`

# COMMAND ----------


empty_extraction_files = [
    output.file
    for output in results
    if output.file not in _written_files
    and output.file not in errors
    and output.file not in date_format_errors
    and output.file not in future_date_errors
]

if empty_extraction_files:
    print(
        f"   ⚠️  {len(empty_extraction_files)} file(s) extracted but produced 0 "
        f"availability rows — quarantining:"
    )
    for _f in empty_extraction_files:
        empty_extraction_errors[_f] = (
            "Extraction produced 0 availability rows (empty or unparsable content)"
        )
        print(f"       • {_f}")

    _quarantine_empty_df = spark.createDataFrame(
        [
            {"file": _f, "error": empty_extraction_errors[_f], "run_date": datetime.now()}
            for _f in empty_extraction_files
        ]
    )
    (
        _quarantine_empty_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(QUARANTINE_TABLE)
    )
    print(f"   ⚠️  {len(empty_extraction_files)} file(s) quarantined → {QUARANTINE_TABLE}")


all_errors = {**errors, **date_format_errors, **future_date_errors, **empty_extraction_errors, **skipped_metadata_errors}

file_tracking = FileTracker.create_tracking_dataframe(
    results=results,
    errors=all_errors,
    previous_tracking=None,  # we append to Delta, no need to reload previous rows
)

print(f"   📝 Writing {len(file_tracking)} tracking records → {TRACKING_TABLE}")

if not file_tracking.empty:
    tracking_spark = spark.createDataFrame(file_tracking)
    (
        tracking_spark.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(TRACKING_TABLE)
    )

print(f"   ✅ files_genai_processed updated")
print(f"\n🏁 Run complete — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
