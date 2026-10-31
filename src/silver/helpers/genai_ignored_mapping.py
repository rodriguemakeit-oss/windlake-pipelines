"""Normalisation of the business-managed "_ignored_errors" tab.

The business maintains, in the same SharePoint workbook as the turbine alias
mapping (``SGRE_Mapping_Table.xlsx``), a tab listing the reports that must NOT
be reported as errors in the Teams alert.

Expected columns (header names are matched leniently — case/accent/spacing
insensitive, several accepted spellings):

    Fichier | Raison | Errors

* **Fichier**  a full report path (targets one file) *or* a partial text
  (targets every file whose path contains it, e.g. ``Event report``).
  Matching is always a case-insensitive substring — no regex, so characters
  like ``[`` in a file name are never interpreted.
* **Raison**   free text, for traceability.
* **Errors**   informational only: which error family the row was added for
  (``Unprocessed`` = never extracted, ``Unmatched`` = extracted but not matched
  to Telemetry). It is stored for the business but does NOT restrict the
  suppression: a listed report is silenced in both categories.

This list only drives **alerting**: every report is still downloaded, extracted
and matched as usual. Consumer: the Teams alert query, via
``silver.files_genai_ignored``.
"""

import re
import unicodedata
from typing import Iterable, List, Sequence, Tuple

import pandas as pd

# Accepted header spellings per output column.
PATTERN_HEADERS = {
    "fichier", "file", "filename", "nom du fichier", "nom fichier",
    "pattern", "motif", "fichier ou motif",
}
REASON_HEADERS = {"raison", "reason", "commentaire", "comment", "motif exclusion"}
ERROR_HEADERS = {"errors", "error", "erreur", "erreurs", "type", "type erreur", "scope"}

# Error families, aligned with the `tag` values of the alert query.
UNPROCESSED = "unprocessed"
UNMATCH = "unmatch"
DEFAULT_ERROR_TYPE = UNPROCESSED

_UNMATCH_ALIASES = {"unmatched", "unmatch", "non matche", "non matche telemetry", "no match"}

# Structural, code-owned out-of-scope patterns. These documents are never
# monthly availability reports (event logs, component reports, annual/yearly
# reports, flagged bad-quality uploads) and must ALWAYS be suppressed in the
# Teams alert, regardless of the business Excel content. They are merged into
# every allowlist refresh: since ``silver.files_genai_ignored`` is fully
# overwritten from the Excel tab, a business edit would otherwise wipe them and
# make these files reappear as false "unprocessed" alerts.
STRUCTURAL_IGNORED: List[Tuple[str, str]] = [
    ("Event report", "Vestas event log — not an availability report"),
    ("Logbook Entries", "DWT logbook — not an availability report"),
    ("Annual_report", "Yearly report — wrong granularity for the monthly pipeline"),
    ("_Blade_", "Component (blade) report — out of scope"),
    ("_Generator_", "Component (generator) report — out of scope"),
    ("_Transformer_", "Component (transformer) report — out of scope"),
    ("_Gearbox_", "Component (gearbox) report — out of scope"),
    ("bad_data_quality", "Flagged bad data quality — out of scope"),
]


def _norm(value) -> str:
    """Lower-case and strip accents/punctuation so values match leniently."""
    if value is None:
        return ""
    text = unicodedata.normalize("NFKD", str(value))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[_\-/.]+", " ", text).strip().lower()
    return re.sub(r"\s+", " ", text)


def find_ignored_sheet(workbook, preferred_name: str = "_ignored_errors"):
    """Return the worksheet holding the ignored list, or None.

    Looks up ``preferred_name`` first (case-insensitive), then falls back to any
    sheet whose header row exposes a recognisable file/pattern column. Never
    trusts ``workbook.active`` — a stray tab must not be mistaken for this one.
    """
    wanted = _norm(preferred_name)
    for sheet in workbook.worksheets:
        if _norm(sheet.title) == wanted:
            return sheet
    for sheet in workbook.worksheets:
        try:
            first_row = next(sheet.iter_rows(values_only=True))
        except StopIteration:
            continue
        if {_norm(h) for h in first_row if h is not None} & PATTERN_HEADERS:
            return sheet
    return None


def normalize_error_type(value) -> str:
    """Map the free-text Errors cell to ``unprocessed`` or ``unmatch``."""
    return UNMATCH if _norm(value) in _UNMATCH_ALIASES else DEFAULT_ERROR_TYPE


def normalize_ignored_records(
    header: Sequence, rows: Iterable[Sequence]
) -> pd.DataFrame:
    """Build the ``files_genai_ignored`` frame from the raw Excel tab.

    Returns:
        DataFrame with columns ``pattern``, ``reason``, ``error_type``. Blank
        patterns are dropped and duplicates removed (first occurrence wins).

    Raises:
        ValueError: when no file/pattern column can be identified — the sheet is
            not the expected one, so the caller must not wipe the table.
    """
    normalized = [_norm(h) for h in header]

    def _index_of(accepted: set) -> int:
        for idx, name in enumerate(normalized):
            if name in accepted:
                return idx
        return -1

    pattern_idx = _index_of(PATTERN_HEADERS)
    if pattern_idx == -1:
        raise ValueError(
            "Ignored-errors sheet has no file/pattern column "
            f"(accepted names: {sorted(PATTERN_HEADERS)})"
        )
    reason_idx = _index_of(REASON_HEADERS)
    error_idx = _index_of(ERROR_HEADERS)

    def _cell(row, idx):
        if 0 <= idx < len(row) and row[idx] is not None:
            return str(row[idx]).strip()
        return ""

    records: List[Tuple[str, str, str]] = []
    for row in rows:
        if row is None:
            continue
        pattern = _cell(row, pattern_idx)
        if not pattern:
            continue  # blank / spacer line
        records.append(
            (pattern, _cell(row, reason_idx), normalize_error_type(_cell(row, error_idx)))
        )

    df = pd.DataFrame(records, columns=["pattern", "reason", "error_type"])
    return df.drop_duplicates(subset=["pattern"]).reset_index(drop=True)


def with_structural_seeds(df: pd.DataFrame) -> pd.DataFrame:
    """Merge the code-owned structural out-of-scope patterns into ``df``.

    Guarantees the always-suppressed documents (event/component/annual reports,
    logbooks…) survive the full-table overwrite driven by the business Excel
    tab. Business rows win on duplicate patterns (they keep their own reason).
    """
    seed = pd.DataFrame(
        [(pattern, reason, UNPROCESSED) for pattern, reason in STRUCTURAL_IGNORED],
        columns=["pattern", "reason", "error_type"],
    )
    combined = pd.concat([df, seed], ignore_index=True)
    return combined.drop_duplicates(subset=["pattern"]).reset_index(drop=True)
