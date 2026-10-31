"""Unit tests for the turbine report-label → Telemetry alias mapping.

Covers:
- the pure Excel normalisation/validation helpers (silver loader), and
- the alias-aware serial-key derivation consumed by Phase 2 matching (gold).
"""

import pathlib
import sys

import pandas as pd
import pytest

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent.parent / "src" / "silver"))
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent.parent / "src" / "gold"))

from helpers.turbine_alias_mapping import (  # noqa: E402
    normalize_mapping_records,
    validate_mapping,
    assert_non_empty,
)
from utils.turbine_alias import prepare_turbine_match_keys  # noqa: E402


HEADER = [
    "Country", "Farm", "code", "wind_turbine (Report)",
    "serial_number (Telemetry)", "manufacturer",
]
ROWS = [
    ("Romania", "ROGEM", "WIT_ROGEM_SS001_WT001", "WTG1_01", 2305791, "Siemens"),
    ("Portugal", "PTMOU", "WIT_PTMOU_SS001_WT001", "A01", "PA/0380-001", "SGRE"),
]


# ---------------------------------------------------------------------------
# normalize_mapping_records
# ---------------------------------------------------------------------------
def test_normalize_renames_and_casts_serial_to_string():
    df = normalize_mapping_records(HEADER, ROWS)
    assert list(df.columns) == [
        "country", "farm", "wind_turbine_code",
        "report_turbine_name", "serial_number", "manufacturer",
    ]
    # numeric (Romania) and string (Portugal) serials are both kept as strings
    serials = dict(zip(df["report_turbine_name"], df["serial_number"]))
    assert serials["WTG1_01"] == "2305791"
    assert serials["A01"] == "PA/0380-001"
    assert all(isinstance(s, str) for s in df["serial_number"])


def test_normalize_missing_column_raises():
    bad_header = [h for h in HEADER if h != "serial_number (Telemetry)"]
    bad_rows = [r[:4] + r[5:] for r in ROWS]
    with pytest.raises(ValueError, match="missing expected column"):
        normalize_mapping_records(bad_header, bad_rows)


def test_normalize_drops_rows_without_key():
    rows = ROWS + [(None, None, None, None, None, None)]
    df = normalize_mapping_records(HEADER, rows)
    assert len(df) == 2


# ---------------------------------------------------------------------------
# validate_mapping / assert_non_empty
# ---------------------------------------------------------------------------
def test_validate_quarantines_duplicate_keys():
    df = normalize_mapping_records(
        HEADER,
        ROWS + [("Romania", "ROGEM", "WIT_X", "WTG1_01", 9999, "Siemens")],
    )
    valid, quarantine = validate_mapping(df)
    # both WTG1_01 (Romania) rows are pulled out; only A01 remains valid
    assert set(valid["report_turbine_name"]) == {"A01"}
    assert set(quarantine["report_turbine_name"]) == {"WTG1_01"}
    assert (quarantine["_reason"] == "duplicate (country, report_turbine_name) key").all()


def test_assert_non_empty_raises_on_empty():
    with pytest.raises(ValueError, match="Refusing to overwrite"):
        assert_non_empty(pd.DataFrame())


# ---------------------------------------------------------------------------
# prepare_turbine_match_keys (Spark)
# ---------------------------------------------------------------------------
def test_prepare_match_keys_uses_alias_when_mapped_and_falls_back(spark):
    genai = spark.createDataFrame(
        [
            # mapped: report label WTG1_01 in Romania → Telemetry serial 2305791
            ("turbine", "WTG1_01", "2025-01", 99.7, "Romania"),
            # unmapped: keep the raw turbine_id as the serial
            ("turbine", "RAWSERIAL42", "2025-01", 98.0, "Romania"),
        ],
        ["type", "turbine_id", "date", "value", "country"],
    )
    alias = spark.createDataFrame(
        [("Romania", "ROGEM", "WIT_ROGEM_SS001_WT001", "WTG1_01", "2305791", "Siemens")],
        ["country", "farm", "wind_turbine_code", "report_turbine_name",
         "serial_number", "manufacturer"],
    )

    out = {
        r["turbine_id"]: r
        for r in prepare_turbine_match_keys(genai, alias).collect()
    }

    # mapped row → serial comes from the alias, with country suffix
    assert out["WTG1_01"]["serial_number_1"] == "2305791"
    assert out["WTG1_01"]["pdf_country"] == "RO"
    assert out["WTG1_01"]["serial_number_1_country"] == "2305791_WIF_RO"

    # unmapped row → falls back to the raw turbine_id
    assert out["RAWSERIAL42"]["serial_number_1"] == "RAWSERIAL42"
    assert out["RAWSERIAL42"]["serial_number_1_country"] == "RAWSERIAL42_WIF_RO"


def test_prepare_match_keys_strips_pipe_serial_suffix(spark):
    # Report label carries a " | <serial>" suffix (e.g. "A01 | 61002393"); the
    # alias crosswalk is keyed on the bare label ("A01"), so the join must strip
    # the pipe suffix before matching, otherwise the Telemetry serial is never attached.
    genai = spark.createDataFrame(
        [("turbine", "A01 | 61002393", "2026-05", 98.66, "Portugal")],
        ["type", "turbine_id", "date", "value", "country"],
    )
    alias = spark.createDataFrame(
        [("Portugal", "WIF_PTSED", "WIT_PTSED_SS001_WT001", "A01", "PA/0280-001", "SGRE")],
        ["country", "farm", "wind_turbine_code", "report_turbine_name",
         "serial_number", "manufacturer"],
    )
    out = prepare_turbine_match_keys(genai, alias).collect()[0]
    # alias serial is attached despite the pipe suffix in turbine_id
    assert out["serial_number_1"] == "PA/0280-001"
    assert out["serial_number_1_country"] == "PA/0280-001_WIF_PT"


def test_label_key_matches_regardless_of_parenthesised_serial(spark):
    # The " (serial)" / "| serial" suffix may appear on the report side, the alias
    # side, both, or neither. label_key() normalises both sides so they still join.
    genai = spark.createDataFrame(
        [
            # paren on report, not on alias
            ("turbine", "PA01 (61031670)", "2026-05", 99.0, "Spain"),
            # paren on alias, not on report
            ("turbine", "AG01", "2026-05", 98.0, "Spain"),
            # paren on both (+ hyphen in label)
            ("turbine", "V-1 (61028561)", "2026-05", 97.0, "Spain"),
        ],
        ["type", "turbine_id", "date", "value", "country"],
    )
    alias = spark.createDataFrame(
        [
            ("Spain", "WIF_ESPAS", "WIT_ESPAS_SS001_WT001", "PA01", "100188200", "SGRE"),
            ("Spain", "WIF_ESVAL", "WIT_ESVAL_SS001_WT001", "AG01 (61028550)", "100187958", "SGRE"),
            ("Spain", "WIF_ESVAL", "WIT_ESVAL_SS001_WT013", "V-1 (61028562)", "100187957", "SGRE"),
        ],
        ["country", "farm", "wind_turbine_code", "report_turbine_name",
         "serial_number", "manufacturer"],
    )
    out = {r["turbine_id"]: r for r in prepare_turbine_match_keys(genai, alias).collect()}
    assert out["PA01 (61031670)"]["serial_number_1"] == "100188200"
    assert out["AG01"]["serial_number_1"] == "100187958"
    assert out["V-1 (61028561)"]["serial_number_1"] == "100187957"


def test_bare_report_serial_matches_alias_parenthesised_serial(spark):
    # Gamesa PT/ES extractions sometimes emit only the bare report serial
    # ("61010441") with no "A01" label. The alias stores it as "A01 (61010441)",
    # so matching on the report serial (not just the label) attaches the Telemetry
    # serial. Labels that also carry a model code after a pipe ("A12 | G87") must
    # still match on the label, never on the (non-numeric) model token.
    genai = spark.createDataFrame(
        [
            ("turbine", "61010441", "2026-05", 99.0, "Portugal"),   # bare serial
            ("turbine", "A12 | G87", "2026-05", 98.0, "Portugal"),  # label + model
            ("turbine", "99999999", "2026-05", 97.0, "Portugal"),   # unknown serial
        ],
        ["type", "turbine_id", "date", "value", "country"],
    )
    alias = spark.createDataFrame(
        [
            ("Portugal", "PTMOU", "WIT_PTMOU_SS001_WT001", "A01 (61010441)", "PA/0380-001", "SGRE"),
            ("Portugal", "PTMOU", "WIT_PTMOU_SS001_WT012", "A12 (61010451)", "PA/0380-012", "SGRE"),
        ],
        ["country", "farm", "wind_turbine_code", "report_turbine_name",
         "serial_number", "manufacturer"],
    )
    out = {r["turbine_id"]: r for r in prepare_turbine_match_keys(genai, alias).collect()}
    # bare serial resolves via the report-serial key
    assert out["61010441"]["serial_number_1"] == "PA/0380-001"
    # label + model resolves via the label key (A12), not the model token
    assert out["A12 | G87"]["serial_number_1"] == "PA/0380-012"
    # unknown serial stays unmapped (raw value kept)
    assert out["99999999"]["serial_number_1"] == "99999999"
