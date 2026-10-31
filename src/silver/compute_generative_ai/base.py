"""
Base module for generative AI computations.

Databricks-compatible version: no dataiku dependency.

The FolderAdapter Protocol replaces dataiku.Folder throughout this module.
Any object implementing get_download_stream / get_writer / upload_data
can be passed as a folder (see helpers/volume_adapter.py for the concrete impl).

Key Components:
    - FolderAdapter: Protocol defining the file-access interface
    - Excel data extraction functions (pandas, openpyxl, Document Intelligence)
    - Data filtering and processing utilities
    - Result persistence helpers
"""

import io
import os
import re
import json
import pickle
import shutil
import subprocess
import tempfile
import time
from typing import List, Union, Any, Optional, Protocol, runtime_checkable

import pandas as pd
from openpyxl import load_workbook
from azure.ai.documentintelligence._client import DocumentIntelligenceClient
from openai import ChatCompletion

from helpers.document_intelligence_helper import analyze_report
from helpers.openai_helper import call_model, call_model_followup
from compute_generative_ai.completeness import (
    returned_turbine_ids,
    merge_missing_turbines,
    needs_completion,
)
from compute_generative_ai.enums import TextExtraction, Maintainers
from compute_generative_ai.constants import (
    keyword_per_maintainer,
    stop_keyword_per_maintainer,
    priority_keyword_per_maintainer,
    shift_per_maintainer,
    sheets_per_maintainer,
    PAGE_LIMIT,
    file_formats,
)
from compute_generative_ai.prompt_generator import (
    generate_system_prompt_di,
    generate_user_prompt,
    generate_completion_prompt,
)
from compute_generative_ai.page_selection import select_pages
from compute_generative_ai.report_postprocess import (
    attach_report_serials,
    drop_copied_turbine_values,
    keep_table_turbine_values,
)
from compute_generative_ai.utils import file_exists_in_folder

# Max follow-up calls asking the LLM for turbines it skipped (see completeness.py).
# The loop stops as soon as a round brings nothing new or Telemetry's count is reached;
# 5 rounds needed for West Coast One (47 WTG x 12 months: ~10 turbines per answer).
MAX_COMPLETION_ROUNDS = 5


################################################################################
#                              FOLDER ADAPTER PROTOCOL                         #
################################################################################


@runtime_checkable
class FolderAdapter(Protocol):
    """Protocol defining the file-access interface used throughout this module.

    Any concrete implementation (VolumeAdapter, SharepointAdapter, …) must
    expose these three methods with the same signatures.
    """

    def get_download_stream(self, path: str):
        """Return a context manager / file-like object open for reading."""
        ...

    def get_writer(self, path: str):
        """Return a context manager / file-like object open for writing."""
        ...

    def upload_data(self, path: str, data: bytes) -> None:
        """Write raw bytes to the given path."""
        ...


################################################################################
#                                 FUNCTIONS                                    #
################################################################################


def _materialize_excel_formulas(file_bytes: bytes) -> Optional[bytes]:
    """Re-save the workbook through LibreOffice so all formulas are evaluated.

    Returns the recalculated bytes, or None if soffice is unavailable or fails.
    """
    soffice = shutil.which("soffice") or shutil.which("libreoffice")
    if not soffice:
        print("   ├─ ⚠️  LibreOffice (soffice) not found on PATH")
        return None

    work_dir = tempfile.mkdtemp(prefix="xlsx_recalc_")
    try:
        in_path = os.path.join(work_dir, "input.xlsx")
        with open(in_path, "wb") as f:
            f.write(file_bytes)

        print("   ├─ 🔄 Re-evaluating formulas with LibreOffice headless...")
        t0 = time.time()
        result = subprocess.run(
            [
                soffice,
                "--headless",
                "--calc",
                "--convert-to",
                "xlsx",
                "--outdir",
                work_dir,
                in_path,
            ],
            capture_output=True,
            timeout=300,
            env={**os.environ, "HOME": work_dir},
        )
        elapsed = time.time() - t0
        if result.returncode != 0:
            print(
                f"   ├─ ❌ LibreOffice conversion failed ({elapsed:.1f}s): "
                f"{result.stderr.decode('utf-8', errors='replace')[:300]}"
            )
            return None

        out_path = os.path.join(work_dir, "input.xlsx")
        if not os.path.exists(out_path):
            print(f"   ├─ ❌ Expected output file not found: {out_path}")
            return None

        with open(out_path, "rb") as f:
            recalc_bytes = f.read()
        print(f"   ├─ ✅ Formulas materialized in {elapsed:.1f}s")
        return recalc_bytes
    except subprocess.TimeoutExpired:
        print("   ├─ ⏱️  LibreOffice timeout (>300s) — skipping materialization")
        return None
    except Exception as e:
        print(f"   ├─ ❌ Materialization error: {e}")
        return None
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


def _read_sheets_content(
    file_bytes: bytes, sheets: Optional[List[Union[int, str]]]
) -> str:
    wb = load_workbook(io.BytesIO(file_bytes), data_only=True)
    # sheets=None (maintainer not in sheets_per_maintainer) → read ALL sheets.
    # Previously `for s in sheets` iterated over None → "TypeError: 'NoneType'
    # object is not iterable", mis-bucketed as "NoneType (parsing)" (this was
    # the root cause of the DWT/Vestas .xlsx failures).
    if sheets is None:
        sheets_data = list(wb.worksheets)
    else:
        # Resolve requested sheets, skipping named sheets that are absent
        # (e.g. a GE export without the expected "Turbine Summary" tab —
        # previously raised "Worksheet ... does not exist"). Out-of-range
        # integer indices are skipped too. If nothing resolves, read all.
        sheets_data = []
        for s in sheets:
            if isinstance(s, int):
                if 0 <= s < len(wb.worksheets):
                    sheets_data.append(wb.worksheets[s])
            elif s in wb.sheetnames:
                sheets_data.append(wb[s])
        if not sheets_data:
            sheets_data = list(wb.worksheets)
    cell_content = ""
    for sheet_data in sheets_data:
        for row in sheet_data.iter_rows(values_only=True):
            for cell in row:
                if cell is not None:
                    cell_content += str(cell) + " "
    return cell_content


def _get_excel_data_from_openpyxl(
    doc: str,
    sheets: List[Union[int, str]],
    folder: FolderAdapter,
    maintainer: Optional[Maintainers] = None,
) -> str:
    print("   ├─ 📊 Getting Excel data using openpyxl")
    with folder.get_download_stream(doc) as f:
        file_bytes = f.read()

    cell_content = _read_sheets_content(file_bytes, sheets)

    # Some GE files are saved with formulas but no cached values; if the
    # targeted sheets contain no numbers, recompute via LibreOffice and re-read.
    if maintainer == Maintainers.GE and not _contains_numeric(cell_content):
        print("   ├─ ⚠️  GE file has no numeric data — triggering LibreOffice fallback")
        recalc = _materialize_excel_formulas(file_bytes)
        if recalc is not None:
            cell_content = _read_sheets_content(recalc, sheets)

    return cell_content


_NUMERIC_RE = re.compile(r"\d")


def _contains_numeric(text: str) -> bool:
    return bool(_NUMERIC_RE.search(text))



def _get_excel_data_from_pandas(
    doc: str, sheets: List[Union[int, str]], folder: FolderAdapter
) -> str:
    print("   ├─ 📊 Getting Excel data using pandas")
    with folder.get_download_stream(doc) as f:
        return pd.concat(pd.read_excel(f.read(), sheet_name=sheets)).to_html()


def _get_excel_data_with_pandas_plus_di(
    doc: str,
    sheets: List[Union[int, str]],
    folder: FolderAdapter,
    ocr_client: DocumentIntelligenceClient,
) -> str:
    print("   ├─ 📊 Getting Excel data with pandas + Document Intelligence")
    with folder.get_download_stream(doc) as f:
        df_xlsx = pd.concat(pd.read_excel(f.read(), sheet_name=sheets))

    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as temp_file:
        temp_file_name = temp_file.name
        with pd.ExcelWriter(temp_file_name) as writer:
            df_xlsx.to_excel(writer, index=False)
        with open(temp_file_name, "rb") as doc_file:
            result = analyze_report(doc=doc_file, ocr_client=ocr_client)
    return result.content


def get_excel_data(
    doc: str,
    sheets: List[Union[int, str]],
    folder: FolderAdapter,
    mode: str,
    ocr_client: Optional[DocumentIntelligenceClient] = None,
    maintainer: Optional[Maintainers] = None,
) -> str:
    if mode == "pandas":
        return _get_excel_data_from_pandas(doc=doc, sheets=sheets, folder=folder)
    elif mode == "openpyxl":
        return _get_excel_data_from_openpyxl(
            doc=doc, sheets=sheets, folder=folder, maintainer=maintainer
        )
    elif mode == "pandas_plus_di":
        return _get_excel_data_with_pandas_plus_di(
            doc=doc, sheets=sheets, folder=folder, ocr_client=ocr_client
        )
    else:
        raise ValueError(f"Unknown mode: {mode}")


def get_file_data_from_dataiku(
    ocr_extract: pd.DataFrame, file_name: str
) -> pd.DataFrame:
    """Filter the pre-computed OCR dataframe for a given file.

    In Databricks without Tesseract, ocr_extract is None and this function
    is not called (see get_document_data).
    """
    print(f"   ├─ 📥 Getting file data from OCR extract: {file_name}")
    return ocr_extract[ocr_extract["file"] == file_name]


def filter_data(
    target_dataiku: pd.DataFrame,
    tokens: str,
    shift_before: int = 1,
    shift_after: int = 1,
    stop_token: str = None,
) -> pd.DataFrame:
    print("   ├─ 🔍 Filtering data by tokens")
    if tokens is None:
        return target_dataiku[["metadata", "text"]]

    target_dataiku["text_lower"] = target_dataiku.text.str.lower()
    mask = target_dataiku["text_lower"].str.contains(tokens, regex=True)

    if stop_token is not None:
        stop_mask = ~target_dataiku["text_lower"].str.contains(stop_token, regex=True)

        found = False
        for i in range(len(stop_mask)):
            if not stop_mask.iloc[i]:
                found = True
            if found:
                stop_mask.iloc[i] = False
                mask.iloc[i] = False

        previous_lines = target_dataiku[mask.shift(-shift_before, fill_value=False)]
        next_lines = target_dataiku[mask.shift(shift_after, fill_value=False)]

        mask = mask | stop_mask

        all_data = (
            pd.concat([target_dataiku, previous_lines, next_lines])
            .drop_duplicates()
            .sort_index()
        )
        all_data = all_data[mask]
    else:
        previous_lines = target_dataiku[mask.shift(-shift_before, fill_value=False)]
        next_lines = target_dataiku[mask.shift(shift_after, fill_value=False)]

        all_data = (
            pd.concat([target_dataiku[mask], previous_lines, next_lines])
            .drop_duplicates()
            .sort_index()
        )

    return all_data[["metadata", "text"]]


def get_dataframe_from_openai(model_answer_4o: ChatCompletion) -> pd.DataFrame:
    print("   ├─ 📊 Extracting DataFrame from OpenAI response")
    content = model_answer_4o.choices[0].message.content or ""
    json_match = re.search(r"```(.*?)```", content, re.DOTALL)
    # The LLM is prompted to return a fenced ```json ... ``` block. When it
    # answers in prose (no fenced block) json_match is None — previously this
    # crashed with "'NoneType' object has no attribute 'group'" and was
    # misclassified as a parsing bug. Raise a clear, classifiable message so the
    # file is quarantined as "empty or unparsable content" (→ "0 ligne extraite").
    if json_match is None:
        raise ValueError(
            "empty or unparsable content — no JSON code block in LLM response"
        )
    clean_json = json_match.group(1).replace("\n", "").replace("json", "")
    availability_4o = json.loads(clean_json)
    df_4o = pd.DataFrame.from_dict(availability_4o)

    if df_4o.empty:
        return df_4o

    if "monthly_availability" not in df_4o.columns:
        raise ValueError(
            "empty or unparsable content — 'monthly_availability' missing in LLM JSON"
        )
    monthly_availability_4o = pd.json_normalize(df_4o["monthly_availability"])
    return pd.concat(
        [
            df_4o.drop(columns=["monthly_availability"])[["type", "turbine_id"]],
            monthly_availability_4o,
        ],
        axis=1,
    )


def save_results(
    data_folder: FolderAdapter,
    doc: str,
    result: dict,
    debug: bool = False,
):
    export_folder_path = doc
    with data_folder.get_writer(f"{export_folder_path}.pkl") as writer:
        print(f"   ├─ 💾 Saving results to pickle: {export_folder_path}.pkl")
        pickle.dump(result, writer)

    if debug:
        if result["df"] is not None:
            data_folder.upload_data(
                f"{export_folder_path}_df.csv",
                result["df"].to_csv(index=False).encode("utf-8"),
            )
            print(f"   ├─ 💾 Saving dataframe to csv: {export_folder_path}_df.csv")
        else:
            data_folder.upload_data(
                f"{export_folder_path}_df.csv",
                pd.DataFrame().to_csv(index=False).encode("utf-8"),
            )
            print(f"   ├─ ⚠️  Empty dataframe saved: {export_folder_path}_df.csv")

        data_folder.upload_data(
            f"{export_folder_path}_system_prompt.txt",
            str(result["system_prompt"]).encode("utf-8"),
        )
        print(
            f"   ├─ 💾 Saving system prompt: {export_folder_path}_system_prompt.txt"
        )

        data_folder.upload_data(
            f"{export_folder_path}_user_prompt.txt",
            str(result["user_prompt"]).encode("utf-8"),
        )
        print(f"   ├─ 💾 Saving user prompt: {export_folder_path}_user_prompt.txt")

        data_folder.upload_data(
            f"{export_folder_path}_document_intelligence_data.txt",
            str(result["data"]).encode("utf-8"),
        )
        print(
            f"   ├─ 💾 Saving document intelligence data: {export_folder_path}_document_intelligence_data.txt"
        )

        data_folder.upload_data(
            f"{export_folder_path}_llm.txt",
            str(result["llm"]).encode("utf-8"),
        )
        print(f"   ├─ 💾 Saving llm response: {export_folder_path}_llm.txt")

        metadata_dict = {
            "file": [doc],
            "maintainer": [result["maintainer"]],
            "country": [result["country"]],
            "year": [result["year"]],
            "month": [result["month"]],
            "prompt_tokens": [result.get("prompt_tokens")],
            "completion_tokens": [result.get("completion_tokens")],
            "total_tokens": [result.get("total_tokens")],
            "di_runtime_seconds": [result.get("di_runtime")],
            "openai_runtime_seconds": [result.get("openai_runtime")],
            "error": [result.get("error")],
        }
        metadata_df = pd.DataFrame(metadata_dict)
        data_folder.upload_data(
            f"{export_folder_path}_metadata.csv",
            metadata_df.to_csv(index=False).encode("utf-8"),
        )
        print(f"   ├─ 💾 Saving metadata: {export_folder_path}_metadata.csv")


def get_keyword_for_maintainer(maintainer: Maintainers) -> Optional[str]:
    print(f"   ├─ 🔍 Getting keyword for maintainer: {maintainer}")
    return keyword_per_maintainer.get(maintainer, None)


def get_document_data(
    doc: str,
    folder: FolderAdapter,
    maintainer: Maintainers,
    ocr_client: DocumentIntelligenceClient,
    tool: TextExtraction = TextExtraction.DOCUMENT_INTELLIGENCE,
    ocr_extract: Optional[pd.DataFrame] = None,
    tokens_filter: Optional[str] = None,
    target_pages: Optional[List[str]] = None,
    page_limit: int = PAGE_LIMIT,
    stop_token: Optional[str] = None,
) -> str:
    """Extract text content from a document.

    When ocr_extract is None (no Tesseract pre-processing), Document
    Intelligence processes the entire document without page filtering.
    When ocr_extract is provided (Tesseract rows), page filtering is
    applied before calling Document Intelligence.
    """
    print(f"   ├─ 📄 Getting document data for file: {doc}")

    pages = target_pages

    if ocr_extract is not None:
        file_data = get_file_data_from_dataiku(
            ocr_extract=ocr_extract, file_name=doc
        )
        if file_data.empty:
            # Tesseract produced no rows for this file (unreadable scan / OCR
            # failure). Previously `file_data.iloc[[0]]` raised
            # "IndexError: positional indexers are out-of-bounds" ("df vide
            # (OCR)"). Skip page filtering and let Document Intelligence analyze
            # the whole document instead.
            print(f"   ├─ ⚠️  No OCR rows for {doc} — analyzing full document via DI")
        else:
            first_page = file_data.iloc[[0]]
            print(f"   ├─ Filtering keywords: {tokens_filter}")

            shift_after = shift_per_maintainer.get(maintainer, 1)
            filtered_data = filter_data(
                file_data,
                tokens_filter,
                shift_before=0,
                shift_after=shift_after,
                stop_token=stop_token,
            )

            if tool == TextExtraction.DOCUMENT_INTELLIGENCE:
                if not target_pages and tokens_filter:
                    filtered_data["page"] = filtered_data["metadata"].apply(
                        lambda x: str(json.loads(x.replace("'", '"'))["page"])
                    )
                    target_pages = select_pages(
                        filtered_data.page.unique(),
                        file_data,
                        page_limit,
                        priority_keyword_per_maintainer.get(maintainer),
                    )
                    if target_pages:
                        pages = ",".join(sorted(set(["1"] + list(target_pages))))
                    else:
                        # The keyword filter matched no page — e.g. a Spanish/
                        # Portuguese report ("disponibilidad") OCR'd in English,
                        # or a differently-labelled availability section. Sending
                        # only page 1 (the cover) yields no availability data
                        # ("0 ligne extraite"). Fall back to the whole document.
                        print(
                            "   ├─ ⚠️  No keyword-matching page — "
                            "analyzing full document via DI"
                        )
                        pages = None
                    filtered_data.drop(columns=["page"], inplace=True)
            else:
                return f"""
                    ---
                    {pd.concat([first_page, filtered_data])[["metadata", "text"]].reset_index(drop=True).to_json(orient='index')}
                    """

    with folder.get_download_stream(doc) as f:
        print(f"   ├─ 🤖 Document Intelligence analyzing [{doc}]")
        print(f"   └─ Pages: {pages if pages else 'All'}")
        di_res = analyze_report(doc=f, pages=pages, ocr_client=ocr_client)
        return di_res.content


def compute_doc(
    doc: str,
    maintainer: Maintainers,
    metadata: dict,
    folder: FolderAdapter,
    ocr_client: DocumentIntelligenceClient,
    generative_ai_client: Any,
    prompt_config: dict,
    custom_prompts: dict,
    allowed_file_extensions: List[str],
    excel_mode: str,
    model: str,
    ocr_extract: Optional[pd.DataFrame] = None,
    tool: TextExtraction = TextExtraction.DOCUMENT_INTELLIGENCE,
    debug: bool = True,
    report_year: int = None,
    report_month: int = None,
    expected_turbines_fn=None,
) -> tuple:
    print("\n" + "=" * 80)
    print(f"{'COMPUTING DOCUMENT':^80}")
    print(f"{doc:^80}")
    print("=" * 80 + "\n")

    data = metadata.get("data")
    system_prompt = metadata.get("system_prompt")
    user_prompt = metadata.get("user_prompt")
    llm = metadata.get("llm")
    df = metadata.get("df")
    extension = metadata.get("extension")
    di_runtime = None
    openai_runtime = None

    if system_prompt is None or debug:
        system_prompt = generate_system_prompt_di(
            prompt_config=prompt_config,
            maintainer_custom_prompt_parts=custom_prompts[Maintainers(maintainer)],
        )

    if data is None or debug:
        if extension in allowed_file_extensions and extension in file_formats["excel"]:
            print(f"   ├─ 📊 Processing {extension} file using {excel_mode}")
            di_start_time = time.time()
            data = get_excel_data(
                doc=doc,
                sheets=sheets_per_maintainer.get(maintainer),
                folder=folder,
                mode=excel_mode,
                ocr_client=ocr_client,
                maintainer=maintainer,
            )
            di_runtime = time.time() - di_start_time
            print(f"   ├─ ⏱️  Document Intelligence runtime: {di_runtime:.2f}s")
        elif extension in allowed_file_extensions and extension in file_formats["pdf"]:
            print(f"   ├─ 📊 Processing {extension} file with Document Intelligence")
            di_start_time = time.time()
            data = get_document_data(
                doc=doc,
                folder=folder,
                maintainer=maintainer,
                ocr_client=ocr_client,
                tool=tool,
                ocr_extract=ocr_extract,
                tokens_filter=get_keyword_for_maintainer(maintainer=maintainer),
                stop_token=stop_keyword_per_maintainer.get(maintainer),
            )
            di_runtime = time.time() - di_start_time
            print(f"   ├─ ⏱️  Document Intelligence runtime: {di_runtime:.2f}s")

            system_prompt = generate_system_prompt_di(
                prompt_config=prompt_config,
                maintainer_custom_prompt_parts=custom_prompts[Maintainers(maintainer)],
            )
        else:
            raise ValueError(f"Unsupported file extension: {extension} for file {doc}")

    if user_prompt is None or debug:
        user_prompt = generate_user_prompt(
            prompt_config=prompt_config,
            data=data,
            file_type=extension,
            report_year=report_year,
            report_month=report_month,
        )

    prompt_tokens = None
    completion_tokens = None
    total_tokens = None
    openai_error = None

    print(f"   ├─ 🤖 Calling model: {model}")
    openai_start_time = time.time()
    try:
        llm = call_model(
            client=generative_ai_client,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            model=model,
        )
        openai_runtime = time.time() - openai_start_time
        print(f"   ├─ ⏱️  OpenAI runtime: {openai_runtime:.2f}s")

        if hasattr(llm, "usage") and llm.usage:
            prompt_tokens = llm.usage.prompt_tokens
            completion_tokens = llm.usage.completion_tokens
            total_tokens = llm.usage.total_tokens
            print(
                f"   ├─ 📊 Tokens used: {total_tokens} (prompt: {prompt_tokens}, completion: {completion_tokens})"
            )

        df = get_dataframe_from_openai(llm)
        df["file"] = doc

        # Fallback: a page-filtered PDF extraction produced 0 availability rows.
        # The keyword page-filter likely selected the wrong page (e.g. Enercon
        # reports whose availability table sits a page after the matched
        # "Availability analysis" header), so the LLM never saw the data. Retry
        # ONCE on the FULL document (ocr_extract=None disables page filtering).
        # Low-risk: only fires when the first pass already yielded nothing, so it
        # never degrades an extraction that already succeeded.
        if (
            df is not None
            and df.empty
            and extension in file_formats["pdf"]
            and ocr_extract is not None
        ):
            print("   ├─ ⚠️  0 rows with page-filtered OCR — retrying on FULL document")
            data = get_document_data(
                doc=doc,
                folder=folder,
                maintainer=maintainer,
                ocr_client=ocr_client,
                tool=tool,
                ocr_extract=None,  # None => no page filtering, whole document
                tokens_filter=get_keyword_for_maintainer(maintainer=maintainer),
                stop_token=stop_keyword_per_maintainer.get(maintainer),
            )
            user_prompt = generate_user_prompt(
                prompt_config=prompt_config,
                data=data,
                file_type=extension,
                report_year=report_year,
                report_month=report_month,
            )
            llm = call_model(
                client=generative_ai_client,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                model=model,
            )
            if hasattr(llm, "usage") and llm.usage:
                prompt_tokens = llm.usage.prompt_tokens
                completion_tokens = llm.usage.completion_tokens
                total_tokens = llm.usage.total_tokens
            df = get_dataframe_from_openai(llm)
            df["file"] = doc

        # GE short labels ('WTG T01'): serial joined from the report's own
        # 'WTG no. | Serial #' table; site value copied to every turbine and
        # values read from charts dropped (see report_postprocess.py).
        if df is not None and Maintainers(maintainer) is Maintainers.GE:
            df = attach_report_serials(df, data)
            df = keep_table_turbine_values(drop_copied_turbine_values(df), data)

        # Completeness pass: on large tables the model sometimes stops after a
        # few turbines (e.g. Canunda: 9/23 returned while the OCR text holds all
        # 23). Ask ONLY for the missing turbines, up to MAX_COMPLETION_ROUNDS;
        # recovered labels must appear in the source text (no invention).
        # Skipped when Telemetry says nothing can be missing (as many turbines as
        # the matched substation(s) hold). Never fatal: on error the first
        # extraction is kept as is.
        expected = None
        if expected_turbines_fn is not None and df is not None:
            try:
                expected = expected_turbines_fn(returned_turbine_ids(df))
            except Exception as count_error:  # noqa: BLE001 — fall back to calling
                print(f"   ├─ ⚠️  Telemetry turbine count unavailable: {count_error}")
        if df is not None and returned_turbine_ids(df) and not needs_completion(
            returned_turbine_ids(df), expected
        ):
            print(f"   ├─ ✅ {len(returned_turbine_ids(df))} turbine(s) >= Telemetry "
                  f"({expected}) — completeness pass skipped")
        elif df is not None and returned_turbine_ids(df):
            print(f"   ├─ 🔎 {len(returned_turbine_ids(df))} turbine(s) extracted, "
                  f"Telemetry expects {expected if expected is not None else 'unknown'}")
            for round_no in range(1, MAX_COMPLETION_ROUNDS + 1):
                try:
                    followup = call_model_followup(
                        client=generative_ai_client,
                        system_prompt=system_prompt,
                        user_prompt=user_prompt,
                        assistant_answer=llm.choices[0].message.content or "",
                        followup_prompt=generate_completion_prompt(
                            prompt_config, returned_turbine_ids(df)
                        ),
                        model=model,
                    )
                    if hasattr(followup, "usage") and followup.usage and total_tokens is not None:
                        prompt_tokens += followup.usage.prompt_tokens
                        completion_tokens += followup.usage.completion_tokens
                        total_tokens += followup.usage.total_tokens
                    extra = get_dataframe_from_openai(followup)
                    df, n_new = merge_missing_turbines(df, extra, data)
                except Exception as completion_error:  # noqa: BLE001 — keep first answer
                    print(f"   ├─ ⚠️  Completeness pass {round_no} skipped: {completion_error}")
                    break
                print(f"   ├─ 🔁 Completeness pass {round_no}: +{n_new} turbine(s)")
                if n_new == 0 or not needs_completion(returned_turbine_ids(df), expected):
                    break
            if Maintainers(maintainer) is Maintainers.GE:
                df = keep_table_turbine_values(attach_report_serials(df, data), data)
            df["file"] = doc

    except Exception as e:
        openai_error = e
        openai_runtime = time.time() - openai_start_time
        print(f"   ├─ ❌ OpenAI call failed after {openai_runtime:.2f}s: {openai_error}")
        llm = None
        df = None

    return (
        data,
        system_prompt,
        llm,
        df,
        user_prompt,
        prompt_tokens,
        completion_tokens,
        total_tokens,
        di_runtime,
        openai_runtime,
        openai_error,
    )
