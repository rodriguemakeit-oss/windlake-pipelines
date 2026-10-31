"""Extraction classes for generative AI computations.

Databricks-compatible version: no dataiku dependency.
dataiku.Folder is replaced by the FolderAdapter protocol from base.py.
"""

import pickle
import time
from datetime import datetime
from typing import List, Optional, Any

import pandas as pd
from IPython.display import display
from azure.ai.documentintelligence._client import DocumentIntelligenceClient
from openai import ChatCompletion

from compute_generative_ai.enums import Maintainers
from compute_generative_ai.utils import file_exists_in_folder
from compute_generative_ai.base import (
    FolderAdapter,
    compute_doc,
    save_results,
)


def _rate_limit_wait_seconds(error, attempt: int, base_sleep: int) -> Optional[int]:
    """If ``error`` is a 429 rate-limit, return seconds to wait before retrying.

    Honors the Azure OpenAI ``Retry-After`` response header when present,
    otherwise falls back to exponential backoff capped at 5 minutes.
    Returns ``None`` when ``error`` is not a rate-limit (caller retries as before).
    """
    if error is None:
        return None
    status = getattr(error, "status_code", None)
    msg = str(error).lower()
    is_rate_limit = status == 429 or "429" in msg or "too many requests" in msg
    if not is_rate_limit:
        return None
    # Prefer the server-provided Retry-After (seconds).
    resp = getattr(error, "response", None)
    if resp is not None:
        try:
            headers = getattr(resp, "headers", {}) or {}
            retry_after = headers.get("retry-after") or headers.get("Retry-After")
            if retry_after is not None:
                return max(1, int(float(retry_after)))
        except (TypeError, ValueError, AttributeError):
            pass
    # Exponential backoff: base_sleep * 2^(attempt-1), capped at 300s.
    return min(base_sleep * (2 ** max(0, attempt - 1)), 300)


class LLMExtractorOutput:
    def __init__(
        self,
        file: str,
        maintainer: str,
        country: str,
        year: str,
        month: str,
        data: Optional[str] = None,
        system_prompt: Optional[str] = None,
        user_prompt: Optional[str] = None,
        llm: Optional[ChatCompletion] = None,
        df: Optional[pd.DataFrame] = None,
        prompt_tokens: Optional[int] = None,
        completion_tokens: Optional[int] = None,
        total_tokens: Optional[int] = None,
        di_runtime: Optional[float] = None,
        openai_runtime: Optional[float] = None,
        run_date: Optional[datetime] = None,
        error: Optional[Exception] = None,
    ):
        self.file = file
        self.maintainer = maintainer
        self.country = country
        self.year = year
        self.month = month
        self.data = data
        self.system_prompt = system_prompt
        self.user_prompt = user_prompt
        self.llm = llm
        self.df = df
        self.prompt_tokens = prompt_tokens
        self.completion_tokens = completion_tokens
        self.total_tokens = total_tokens
        self.di_runtime = di_runtime
        self.openai_runtime = openai_runtime
        self.run_date = run_date if run_date is not None else datetime.now()
        self.error = error

    def save(self, data_folder: FolderAdapter, debug: bool = False) -> None:
        result = {
            "data": self.data,
            "system_prompt": self.system_prompt,
            "user_prompt": self.user_prompt,
            "llm": self.llm,
            "df": self.df,
            "maintainer": self.maintainer,
            "country": self.country,
            "year": self.year,
            "month": self.month,
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
            "di_runtime": self.di_runtime,
            "openai_runtime": self.openai_runtime,
            "run_date": self.run_date,
            "error": self.error,
        }
        save_results(data_folder=data_folder, doc=self.file, result=result, debug=debug)

    def to_dict(self) -> dict:
        return {
            "file": self.file,
            "maintainer": self.maintainer,
            "country": self.country,
            "year": self.year,
            "month": self.month,
            "data": self.data,
            "system_prompt": self.system_prompt,
            "user_prompt": self.user_prompt,
            "llm": self.llm,
            "df": self.df,
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
            "di_runtime": self.di_runtime,
            "openai_runtime": self.openai_runtime,
            "run_date": self.run_date,
            "error": self.error,
        }


class LLMExtractor:
    def __init__(
        self,
        files: List[str],
        maintainers_report: FolderAdapter,
        data_folder: FolderAdapter,
        generative_ai_client: Any,
        ocr_client: DocumentIntelligenceClient,
        prompt_config: dict,
        custom_prompts: dict,
        allowed_file_extensions: List[str],
        excel_mode: str,
        model: str,
        files_metadata: pd.DataFrame = None,
        ocr_extract: Optional[pd.DataFrame] = None,
        max_retries: int = 3,
        debug: bool = False,
        load_pickle: bool = False,
        sleep_time: int = 60,
        expected_turbines_fn=None,
    ):
        """
        Args:
            files: List of file paths to process.
            maintainers_report: FolderAdapter giving access to the source PDF/Excel files.
            data_folder: FolderAdapter used to persist intermediate pickle/debug files.
            generative_ai_client: Azure OpenAI client.
            ocr_client: Azure Document Intelligence client.
            prompt_config: Loaded prompts.yaml dict.
            custom_prompts: Dict mapping Maintainers enum → custom prompt parts.
            allowed_file_extensions: Whitelisted extensions (e.g. ["pdf", "xlsx"]).
            excel_mode: One of "pandas", "openpyxl", "pandas_plus_di".
            model: Azure OpenAI deployment name (e.g. "gpt-4o").
            files_metadata: DataFrame with columns [file, maintainer, country, year, month, extension].
                            Replaces the Dataiku `files_to_process` dataset.
                            If None, metadata must be inferable from the file path.
            ocr_extract: Optional pre-computed OCR DataFrame (Tesseract/EasyOCR rows).
                             Schema: [file, page, metadata, text].
                             When None, Document Intelligence processes documents without page filtering.
            max_retries: Number of LLM call retries on failure.
            debug: If True, saves intermediate artifacts (prompts, DI output, CSV).
            load_pickle: If True, loads previously saved pickle instead of recomputing.
            sleep_time: Seconds to wait between files to avoid API throttling.
        """
        self.files = files
        self.maintainers_report = maintainers_report
        self.data_folder = data_folder
        self.generative_ai_client = generative_ai_client
        self.ocr_client = ocr_client
        self.prompt_config = prompt_config
        self.custom_prompts = custom_prompts
        self.allowed_file_extensions = allowed_file_extensions
        self.excel_mode = excel_mode
        self.model = model
        self.files_metadata = files_metadata
        self.ocr_extract = ocr_extract
        self.max_retries = max_retries
        self.debug = debug
        self.load_pickle = load_pickle
        self.sleep_time = sleep_time
        # Optional callable (country, turbine_ids) -> expected Telemetry WTG count;
        # lets compute_doc skip the completeness LLM call when nothing is missing.
        self.expected_turbines_fn = expected_turbines_fn

    def run(
        self, files: Optional[List[str]] = None
    ) -> tuple:
        files_to_process = files if files is not None else self.files
        results = []
        errors = {}

        print("\n" + "=" * 80)
        print(f"🚀 JOB STARTED: Analyzing {len(files_to_process)} documents")
        print("=" * 80 + "\n")

        for idx, doc in enumerate(files_to_process):
            print("\n" + "-" * 80)
            print(f"📁 Processing file [{idx + 1}/{len(files_to_process)}]: {doc}")
            print("-" * 80)

            output = None
            loaded_from_pickle = False

            if self.load_pickle and file_exists_in_folder(self.data_folder, f"{doc}.pkl"):
                print("   💾 Loading from pickle backup")
                loaded_from_pickle = True
                try:
                    with self.data_folder.get_download_stream(f"{doc}.pkl") as stream:
                        result_dict = pickle.load(stream)
                        output = LLMExtractorOutput(
                            file=doc,
                            maintainer=result_dict.get("maintainer"),
                            country=result_dict.get("country"),
                            year=result_dict.get("year"),
                            month=result_dict.get("month"),
                            data=result_dict.get("data"),
                            system_prompt=result_dict.get("system_prompt"),
                            user_prompt=result_dict.get("user_prompt"),
                            llm=result_dict.get("llm"),
                            df=result_dict.get("df"),
                            prompt_tokens=result_dict.get("prompt_tokens"),
                            completion_tokens=result_dict.get("completion_tokens"),
                            total_tokens=result_dict.get("total_tokens"),
                            di_runtime=result_dict.get("di_runtime"),
                            openai_runtime=result_dict.get("openai_runtime"),
                            run_date=datetime.now(),
                            error=result_dict.get("error"),
                        )
                except Exception as e:
                    error_msg = f"Failed to load pickle: {e}"
                    print(f"   ⚠️  {error_msg}")
                    print(f"   ⏭️  Skipping [{idx + 1}/{len(files_to_process)}]")
                    errors[doc] = error_msg
                    continue
            else:
                print("   ⚙️  Computing from scratch")

                # Resolve file metadata
                if self.files_metadata is not None:
                    metadata_row = self.files_metadata[
                        self.files_metadata["file"] == doc
                    ]
                    if metadata_row.empty:
                        error_msg = f"No metadata found for file: {doc}"
                        print(f"   ❌ {error_msg}")
                        errors[doc] = error_msg
                        continue
                    metadata_doc = metadata_row.iloc[0][
                        ["maintainer", "country", "year", "month", "extension"]
                    ]
                elif self.ocr_extract is not None:
                    metadata_doc = self.ocr_extract[
                        self.ocr_extract["file"] == doc
                    ].iloc[0][["maintainer", "country", "year", "month", "extension"]]
                else:
                    error_msg = (
                        f"No files_metadata or ocr_extract provided for: {doc}"
                    )
                    print(f"   ❌ {error_msg}")
                    errors[doc] = error_msg
                    continue

                if not any(
                    metadata_doc["extension"] == ext
                    for ext in self.allowed_file_extensions
                ):
                    error_msg = f"Unknown file format (only {self.allowed_file_extensions} supported): {doc}"
                    print(f"   ❌ {error_msg}")
                    errors[doc] = error_msg
                    continue

                try:
                    maintainer = Maintainers(metadata_doc["maintainer"].lower())
                except Exception:
                    maintainer = Maintainers.UNKNOWN

                print(f"   🏢 Maintainer: {maintainer}")

                iretry = 1
                data, system_prompt, llm, df, user_prompt = None, None, None, None, None
                prompt_tokens, completion_tokens, total_tokens = None, None, None
                di_runtime, openai_runtime = None, None
                error = None

                while iretry <= self.max_retries:
                    print(f"   🔄 Attempt {iretry}/{self.max_retries}")
                    try:
                        (
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
                        ) = compute_doc(
                            doc=doc,
                            maintainer=maintainer,
                            metadata={
                                "data": data,
                                "system_prompt": system_prompt,
                                "llm": llm,
                                "df": df,
                                "user_prompt": user_prompt,
                                "extension": metadata_doc["extension"],
                            },
                            folder=self.maintainers_report,
                            ocr_extract=self.ocr_extract,
                            ocr_client=self.ocr_client,
                            generative_ai_client=self.generative_ai_client,
                            prompt_config=self.prompt_config,
                            custom_prompts=self.custom_prompts,
                            allowed_file_extensions=self.allowed_file_extensions,
                            excel_mode=self.excel_mode,
                            model=self.model,
                            debug=self.debug,
                            report_year=int(metadata_doc["year"]) if metadata_doc["year"] else None,
                            report_month=int(metadata_doc["month"]) if metadata_doc["month"] else None,
                            expected_turbines_fn=(
                                (lambda ids, _c=metadata_doc["country"]: self.expected_turbines_fn(_c, ids))
                                if self.expected_turbines_fn else None
                            ),
                        )
                        if df is not None:
                            break
                        else:
                            print("   ⚠️  OpenAI call returned no data, retrying...")
                            error = openai_error
                            wait_s = _rate_limit_wait_seconds(error, iretry, self.sleep_time)
                            if wait_s is not None and iretry < self.max_retries:
                                print(
                                    f"   ⏳ 429 rate-limit — waiting {wait_s}s before "
                                    f"retry {iretry + 1}/{self.max_retries}"
                                )
                                time.sleep(wait_s)
                            iretry += 1
                    except Exception as e:
                        error = e
                        print(f"   ⚠️  Error: {e}")
                        wait_s = _rate_limit_wait_seconds(e, iretry, self.sleep_time)
                        if wait_s is not None and iretry < self.max_retries:
                            print(
                                f"   ⏳ 429 rate-limit — waiting {wait_s}s before "
                                f"retry {iretry + 1}/{self.max_retries}"
                            )
                            time.sleep(wait_s)
                        iretry += 1

                if df is None:
                    errors[doc] = str(error) if error else "Unknown error"

                output = LLMExtractorOutput(
                    file=doc,
                    maintainer=metadata_doc["maintainer"],
                    country=metadata_doc["country"],
                    year=metadata_doc["year"],
                    month=metadata_doc["month"],
                    data=data,
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    llm=llm,
                    df=df,
                    prompt_tokens=prompt_tokens,
                    completion_tokens=completion_tokens,
                    total_tokens=total_tokens,
                    di_runtime=di_runtime,
                    openai_runtime=openai_runtime,
                    run_date=datetime.now(),
                    error=str(error) if error else None,
                )

                if output.df is not None:
                    output.save(data_folder=self.data_folder, debug=self.debug)
                else:
                    print(f"   ⚠️  Skipping pickle save for {doc} (no data extracted)")

            if output:
                results.append(output)
                if self.debug and output.df is not None:
                    display(output.df)

            print(f"   ✅ Completed [{idx + 1}/{len(files_to_process)}]")

            if idx < len(files_to_process) - 1 and not loaded_from_pickle:
                print(f"   ⏸️  Sleeping for {self.sleep_time}s before next file...")
                time.sleep(self.sleep_time)

        print("\n" + "=" * 80)
        print(
            f"✅ JOB COMPLETED: Finished analyzing {len(files_to_process)} documents"
        )
        print("=" * 80 + "\n")

        return results, errors
