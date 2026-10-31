"""Result processing classes for generative AI computations."""

from typing import List, Optional
from datetime import datetime
import calendar

import pandas as pd

from compute_generative_ai.extractors import LLMExtractorOutput


class ResultAggregator:
    @staticmethod
    def aggregate_results(results: List[LLMExtractorOutput]) -> pd.DataFrame:
        all_data = pd.DataFrame()

        for output in results:
            if output.df is not None:
                result_df = output.df.copy()
                result_df["file"] = output.file
                result_df["maintainer"] = output.maintainer
                result_df["country"] = output.country
                try:
                    last_day = calendar.monthrange(int(output.year), int(output.month))[1]
                    result_df["reporting_date"] = pd.to_datetime(
                        f"{output.year}-{int(output.month):02d}-{last_day}"
                    )
                except Exception:
                    result_df["reporting_date"] = pd.NaT
                result_df["run_date"] = output.run_date
                all_data = pd.concat([all_data, result_df], ignore_index=True)

        return all_data

    @staticmethod
    def transform_to_long_format(
        df: pd.DataFrame,
        id_vars: List[str],
        var_name: str = "date",
        value_name: str = "value",
    ) -> pd.DataFrame:
        if df.empty:
            return df

        long_df = df.melt(id_vars=id_vars, var_name=var_name, value_name=value_name)

        # Coerce to numeric: the LLM may return string-formatted numbers ("95.3") or
        # non-numeric tokens ("N/A", "-"). pd.to_numeric(..., errors="coerce") converts
        # them to NaN so dropna removes them cleanly, and Arrow always
        # receives a float64 column instead of a mixed-type object series.
        long_df[value_name] = pd.to_numeric(long_df[value_name], errors="coerce")

        long_df = (
            long_df
            .dropna(subset=[value_name])
            .sort_values(by=["turbine_id", var_name, "reporting_date"])
        )
        return long_df

    @staticmethod
    def prepare_availability_data(results: List[LLMExtractorOutput]) -> pd.DataFrame:
        aggregated = ResultAggregator.aggregate_results(results)

        if aggregated.empty:
            return aggregated

        id_columns = [
            "type",
            "turbine_id",
            "file",
            "maintainer",
            "country",
            "reporting_date",
            "run_date",
        ]

        transformed = ResultAggregator.transform_to_long_format(
            df=aggregated, id_vars=id_columns, var_name="date", value_name="value"
        )
        return transformed


class FileTracker:
    @staticmethod
    def create_tracking_dataframe(
        results: List[LLMExtractorOutput],
        errors: dict,
        previous_tracking: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        new_tracking = pd.DataFrame(
            {
                "file": [output.file for output in results],
                "run_date": [output.run_date for output in results],
            }
        )

        # Also track files that were attempted this run but never produced an
        # LLMExtractorOutput (invalid metadata, unknown format, early errors, …).
        # Without this they stay absent from files_genai_processed and are
        # re-listed as "new" on every subsequent run — an endless reprocessing
        # loop that slows the extract task down.
        _result_files = {output.file for output in results}
        _extra_files = [f for f in errors.keys() if f not in _result_files]
        if _extra_files:
            _now = datetime.now()
            extra_tracking = pd.DataFrame(
                {"file": _extra_files, "run_date": [_now] * len(_extra_files)}
            )
            new_tracking = pd.concat([new_tracking, extra_tracking], ignore_index=True)

        if previous_tracking is not None and not previous_tracking.empty:
            all_tracking = pd.concat([previous_tracking, new_tracking], ignore_index=True)
        else:
            all_tracking = new_tracking

        all_tracking["error"] = all_tracking.apply(
            lambda row: errors.get(row["file"], row.get("error", "")), axis=1
        )

        return all_tracking
