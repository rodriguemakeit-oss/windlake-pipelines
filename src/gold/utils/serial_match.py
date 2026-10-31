"""Deterministic fallback match between a report serial and Telemetry serials.

Replaces the former rapidfuzz ``partial_ratio`` step, which is a *substring*
score: any short report label scored 100 against a longer Telemetry serial that
contained it, silently attaching turbines to the wrong farm (``"101"`` →
``"101262433"`` Guijos, ``"LRT01"`` → ``"T01"`` Alentisque,
``"61002982"`` → ``"82982"`` Jaulín…).

A report serial matches a Telemetry serial when one of these holds:

A. both are equal once reduced to upper-case alphanumerics
   (``"PA0379-001"`` ↔ ``"PA 0379-001"``);
B. the whole Telemetry serial (>= 3 chars) is one alphanumeric token of the report
   serial (``"26086-T28"`` → ``"T28"``);
C. they share a digit run of >= 5 digits (``"V20644"`` ↔ ``"20644"``,
   ``"93091"`` ↔ ``"NX 93091"``, ``"246385-1"`` ↔ ``"246385"``);
D. the whole Telemetry serial (>= 6 chars) is contained in the alphanumeric-reduced
   report serial — the serial split by separators, e.g. Schkortleben
   ``"T2300_980 12"`` → ``"2300980"``. Only the long Telemetry serial may sit
   inside the report label, never the reverse (that was the partial_ratio bug).

Only a UNIQUE candidate is returned — when several Telemetry turbines qualify the
row is left unmatched rather than guessed.
"""

import re
from typing import Dict, List, Optional

_MIN_DIGIT_RUN = 5
_MIN_TOKEN_LEN = 3
_MIN_CONTAINED_LEN = 6
_ALENTISQUE_FARM_CODE = "WIF_ESALE"
_ALENTISQUE_REPORT = re.compile(
    r"(?:^|/)Spain/Vestas/.*/[^/]*alentisque[^/]*\.pdf$", re.IGNORECASE
)


def _norm(s: str) -> str:
    return re.sub(r"[^0-9A-Z]", "", s.upper())


def _tokens(s: str) -> set:
    return set(re.findall(r"[0-9A-Z]+", s.upper()))


def _digit_runs(s: str) -> set:
    return {d for d in re.findall(r"\d+", s) if len(d) >= _MIN_DIGIT_RUN}


def _alentisque_serial(query: Optional[str]) -> Optional[str]:
    """Translate Alentisque's 101..131 report labels to Telemetry's T01..T31."""
    if query is None:
        return None
    match = re.fullmatch(r"1(0[1-9]|[12]\d|3[01])", str(query).strip())
    if not match:
        return None
    return f"T{int(str(query).strip()) - 100:02d}"


def is_serial_match(query: Optional[str], telemetry_serial: Optional[str]) -> bool:
    """True when ``query`` (report side) designates ``telemetry_serial``."""
    if not query or not telemetry_serial:
        return False
    query, telemetry_serial = str(query), str(telemetry_serial)
    nq, nd = _norm(query), _norm(telemetry_serial)
    if not nq or not nd:
        return False
    if nq == nd:
        return True
    if len(nd) >= _MIN_TOKEN_LEN and nd in _tokens(query):
        return True
    if _digit_runs(query) & _digit_runs(telemetry_serial):
        return True
    return len(nd) >= _MIN_CONTAINED_LEN and nd in nq


def match_serial(query: Optional[str], candidates: List[Dict],
                 serial_key: str = "serial_number",
                 id_key: str = "wind_turbine_code",
                 report_file: Optional[str] = None) -> Optional[Dict]:
    """Return the single candidate matching ``query``, else ``None``.

    ``candidates`` are Telemetry rows (dicts) of the report's country. Several rows
    pointing to the same turbine count as one; several distinct turbines make
    the match ambiguous → ``None``. Alentisque's numeric labels are translated
    only when the source file is its Spain/Vestas report, and candidates are
    restricted to that farm to prevent cross-farm matches.
    """
    if report_file and _ALENTISQUE_REPORT.search(str(report_file)):
        candidates = [
            c for c in candidates if c.get("farm_code") == _ALENTISQUE_FARM_CODE
        ]
        query = _alentisque_serial(query) or query

    hits = {}
    for c in candidates:
        if is_serial_match(query, c.get(serial_key)):
            hits.setdefault(c.get(id_key), c)
    return next(iter(hits.values())) if len(hits) == 1 else None
