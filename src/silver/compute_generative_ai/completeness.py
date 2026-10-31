"""Completeness pass helpers: recover turbines the LLM silently dropped.

On large tables (e.g. Vestas cumulative "Turbine / Month" tables with 23
turbines x 10 months) the model sometimes stops after a few turbines and jumps
to the site row, although the whole table is in the OCR text. A follow-up call
asks ONLY for the missing turbines; these helpers merge its answer safely.

Pure pandas / re — no Azure SDK import, unit-testable locally.
"""

import re
from typing import Callable, Dict, Iterable, List, Optional, Tuple

import pandas as pd


def expected_turbine_count(
    turbine_ids: Iterable[str],
    candidates: List[Dict],
    matcher: Callable[[str, List[Dict]], Optional[Dict]],
) -> Optional[int]:
    """Telemetry WTG count of the substation(s) the extracted turbines belong to.

    Each extracted label (and each ``|`` part of it) is matched against the
    Telemetry turbines of the report's country; the expected count is the number of
    Telemetry turbines in the matched substation(s). Substation (not farm) level so
    a report covering one substation of a multi-substation farm (Guijo I / II)
    is not judged incomplete. Returns None when nothing matches (alias-only
    labels, unknown farm) — the caller then keeps the completion call.
    """
    substations = set()
    for tid in turbine_ids:
        for query in [tid] + [p.strip() for p in str(tid).split("|")]:
            ref = matcher(query, candidates)
            if ref is not None:
                substations.add(ref.get("substation_code"))
                break
    substations.discard(None)
    if not substations:
        return None
    return len({c["wind_turbine_code"] for c in candidates
                if c.get("substation_code") in substations})


def needs_completion(returned_ids: List[str], expected: Optional[int]) -> bool:
    """Ask the LLM for missing turbines unless Telemetry says nothing can be missing."""
    if not returned_ids:
        return False
    return expected is None or len(returned_ids) < expected


def returned_turbine_ids(df: pd.DataFrame) -> List[str]:
    """Distinct turbine_id of the 'turbine' rows already extracted."""
    if df is None or df.empty or "type" not in df.columns or "turbine_id" not in df.columns:
        return []
    ids = df.loc[df["type"] == "turbine", "turbine_id"].dropna().astype(str)
    return sorted(set(ids))


def _id_tokens(turbine_id) -> List[str]:
    """Identifying tokens of a turbine label: alphanumeric runs holding a digit."""
    return [
        t for t in re.findall(r"[0-9A-Za-z]+", str(turbine_id))
        if any(c.isdigit() for c in t)
    ]


def label_in_source(turbine_id, source_text: str) -> bool:
    """True when every identifying token of the label appears in the source text.

    Guards the completion pass against invented turbines: a recovered turbine is
    kept only if what identifies it (serial, WTG number...) is really in the report.
    """
    tokens = _id_tokens(turbine_id)
    return bool(tokens) and all(t in (source_text or "") for t in tokens)


def merge_missing_turbines(
    df: pd.DataFrame, extra: pd.DataFrame, source_text: str
) -> Tuple[pd.DataFrame, int]:
    """Append the turbines of ``extra`` not yet in ``df`` and present in the source.

    Returns (merged_df, number_of_new_turbines). Site rows of ``extra`` are ignored
    (the first answer already owns the site row).
    """
    if extra is None or extra.empty or "type" not in extra.columns:
        return df, 0
    already = set(returned_turbine_ids(df))
    ids = extra["turbine_id"].astype(str)
    keep = (
        (extra["type"] == "turbine")
        & extra["turbine_id"].notna()
        & ~ids.isin(already)
        & ids.map(lambda t: label_in_source(t, source_text))
    )
    new_rows = extra.loc[keep].drop_duplicates(subset=["turbine_id"])
    if new_rows.empty:
        return df, 0
    return pd.concat([df, new_rows], ignore_index=True), len(new_rows)
