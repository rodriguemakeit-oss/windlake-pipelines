"""Deterministic post-processing of LLM turbine rows, for reports that name turbines
by a short label (GE Willogoleche: 'WTG T01', 'WTG-001').

1. ``attach_report_serials``: the serial of each label is read from the report
   itself — any HTML table row (Document Intelligence output) holding a label cell
   immediately followed by a serial cell, e.g. the 'WTG no. | Serial #'
   maintenance table ``<td>T01</td><td>38171870</td>``. Asking the LLM to join the
   two tables proved unreliable (shifted serials from T25 on, serial dropped from
   T14 on), so the join is done here and the LLM serial is overridden.
2. ``drop_copied_turbine_values``: when the report has no per-turbine table, the
   LLM sometimes copies the site / dashboard value to every turbine (32 x 90.1).
   Such a uniform month (>= 5 turbines, one single value, not 100) is dropped.
3. ``keep_table_turbine_values``: in those short-label reports the turbine values
   must be table cells; values read from the 'Availability by WTG' chart are dropped.

Pure pandas / re — unit-testable without the Azure SDK.
"""

import re
from typing import Dict, Optional

import pandas as pd

# 'T01', 'WTG T01', 'WTG-001' — and OCR-damaged 'WIG-U24' (W?G + one stray letter)
_LABEL = r"(?:W[A-Z]G\s*-?\s*[A-Z]?|T)\s*-?\s*0*(\d{1,3})"
_PAIR = re.compile(r"<td>\s*" + _LABEL + r"\s*</td>\s*<td>\s*(\d{6,})\s*</td>", re.I)
_TURBINE_LABEL = re.compile(r"^\s*" + _LABEL + r"\s*$", re.I)
_MONTH = re.compile(r"^\d{4}-\d{2}$")
_MIN_UNIFORM = 5


def report_serial_crosswalk(source_text: str) -> Dict[int, str]:
    """Turbine number -> serial read from the report tables (conflicting numbers dropped)."""
    seen: Dict[int, set] = {}
    for num, serial in _PAIR.findall(source_text or ""):
        seen.setdefault(int(num), set()).add(serial)
    return {n: next(iter(s)) for n, s in seen.items() if len(s) == 1}


def _label_number(turbine_id) -> Optional[int]:
    m = _TURBINE_LABEL.match(str(turbine_id).split("|")[0])
    return int(m.group(1)) if m else None


def attach_report_serials(df: pd.DataFrame, source_text: str) -> pd.DataFrame:
    """Rewrite short-label turbine ids as 'T<nn>|<serial from the report>'."""
    if df is None or df.empty or "turbine_id" not in df.columns:
        return df
    crosswalk = report_serial_crosswalk(source_text)
    if not crosswalk:
        return df

    def fix(row):
        if row.get("type") != "turbine" or pd.isna(row["turbine_id"]):
            return row["turbine_id"]
        n = _label_number(row["turbine_id"])
        if n is None or n not in crosswalk:
            return row["turbine_id"]
        return f"T{n:02d}|{crosswalk[n]}"

    df = df.copy()
    df["turbine_id"] = df.apply(fix, axis=1)
    # two labels of the same turbine (e.g. first answer + completion pass) -> keep first
    dup = (df["type"] == "turbine") & df["turbine_id"].duplicated()
    return df.loc[~dup].reset_index(drop=True)


_TD_NUMBER = re.compile(r"<td>\s*(\d{1,3}(?:[.,]\d+)?)\s*%?\s*</td>", re.I)


def keep_table_turbine_values(df: pd.DataFrame, source_text: str) -> pd.DataFrame:
    """Blank turbine values that are not a table cell of the report.

    Only for short-label reports (a serial crosswalk exists in the text): there the
    per-turbine availability is always a table cell (ASSET OVERVIEW / Plant
    Performance Overview), while values the LLM reads from the 'Availability by
    WTG' bar chart (figure text, labels and bars unaligned) are not.
    """
    if df is None or df.empty or "type" not in df.columns or not report_serial_crosswalk(source_text):
        return df
    cells = {round(float(v.replace(",", ".")), 2) for v in _TD_NUMBER.findall(source_text)}
    df = df.copy()
    turbines = df["type"] == "turbine"
    for col in [c for c in df.columns if _MONTH.match(str(c))]:
        vals = pd.to_numeric(df[col], errors="coerce")
        outside = turbines & vals.notna() & ~vals.round(2).isin(cells)
        df.loc[outside, col] = None
    return df


def drop_copied_turbine_values(df: pd.DataFrame) -> pd.DataFrame:
    """Blank a month where every turbine carries the same (non-100) value."""
    if df is None or df.empty or "type" not in df.columns:
        return df
    df = df.copy()
    turbines = df["type"] == "turbine"
    for col in [c for c in df.columns if _MONTH.match(str(c))]:
        vals = pd.to_numeric(df.loc[turbines, col], errors="coerce").dropna()
        if len(vals) >= _MIN_UNIFORM and vals.nunique() == 1 and vals.iloc[0] != 100:
            df.loc[turbines, col] = None
    return df
