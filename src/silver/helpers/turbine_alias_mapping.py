"""Parse and validate the SGRE turbine-alias mapping Excel (pure pandas)."""

from typing import List, Optional, Sequence, Tuple

import pandas as pd

# Excel header → normalised column name.
COLUMN_RENAMES = {
    "Country": "country",
    "Farm": "farm",
    "code": "wind_turbine_code",
    "wind_turbine (Report)": "report_turbine_name",
    "serial_number (Telemetry)": "serial_number",
    "manufacturer": "manufacturer",
}

EXPECTED_COLUMNS = list(COLUMN_RENAMES.values())

# Must be unique for the join to be deterministic.
KEY_COLUMNS = ["country", "report_turbine_name"]


def find_mapping_sheet(workbook):
    """Return the worksheet whose header row holds all mapping columns, else None.

    Identifies the mapping by its CONTENT (column headers), so neither the tab
    name nor the workbook file name matter.
    """
    expected = set(COLUMN_RENAMES)
    for sheet in workbook.worksheets:
        try:
            first_row = next(sheet.iter_rows(values_only=True))
        except StopIteration:
            continue
        hdr = {str(h).strip() for h in first_row if h is not None}
        if expected <= hdr:
            return sheet
    return None


def pick_single_mapping_file(paths: List[str], searched: Sequence[str]) -> str:
    """Return the only workbook holding the mapping; refuse to guess otherwise.

    Used when the configured file name is not found (file renamed on
    SharePoint). Several hits (e.g. a copy/backup left next to it) is an error:
    loading the wrong copy would silently corrupt the crosswalk.
    """
    if not paths:
        raise FileNotFoundError(
            f"No workbook holding the mapping columns {list(COLUMN_RENAMES)} "
            f"found in {list(searched)}."
        )
    if len(paths) > 1:
        raise ValueError(
            f"Several workbooks hold the mapping columns: {paths}. Keep only one "
            "(or set mapping_file_name to the right one)."
        )
    return paths[0]


def normalize_mapping_records(
    header: Sequence, rows: Sequence[Sequence]
) -> pd.DataFrame:
    """Rename columns, coerce every value to a trimmed string, drop empty rows.

    serial_number is forced to string because it is numeric for Romania
    (2305791) but textual for Portugal (PA/0380-001).
    """
    header = [str(h).strip() if h is not None else "" for h in header]
    missing = [c for c in COLUMN_RENAMES if c not in header]
    if missing:
        raise ValueError(
            f"Mapping Excel is missing expected column(s): {missing}. "
            f"Found header: {header}"
        )

    df = pd.DataFrame(list(rows), columns=header)
    df = df[list(COLUMN_RENAMES.keys())].rename(columns=COLUMN_RENAMES)

    def _to_str(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        s = str(v).strip()
        return s if s != "" else None

    for col in EXPECTED_COLUMNS:
        df[col] = df[col].map(_to_str)

    df = df.dropna(subset=KEY_COLUMNS).reset_index(drop=True)
    return df


def validate_mapping(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Split the mapping into valid rows and quarantine.

    A ``(country, report_turbine_name)`` key may legitimately map to several
    Telemetry serials when the same report label is reused across farms in the same
    country — these are KEPT as candidates and disambiguated downstream (per
    report file) by the LLM resolution step. Only exact duplicate rows (identical
    on every column) are removed, to avoid writing redundant data.
    """
    exact_dup_mask = df.duplicated(subset=EXPECTED_COLUMNS, keep="first")
    valid = df.loc[~exact_dup_mask].reset_index(drop=True)
    quarantine = df.loc[exact_dup_mask].copy()
    if not quarantine.empty:
        quarantine["_reason"] = "exact duplicate row"
    quarantine = quarantine.reset_index(drop=True)
    return valid, quarantine


def assert_non_empty(df: pd.DataFrame, min_rows: int = 1) -> None:
    """Guard against silently overwriting a good mapping with a broken Excel."""
    if len(df) < min_rows:
        raise ValueError(
            f"Turbine alias mapping has {len(df)} valid row(s) (< {min_rows}). "
            "Refusing to overwrite silver.dim_turbine_report_alias — check the "
            "SharePoint Excel."
        )
