"""Unit tests for the deterministic report-serial → Telemetry fallback match.

Cases are real prod pairs from silver._tmp_genai_telemetry_fuzzy: the legitimate
ones must still match, the wrong-farm ones produced by the former rapidfuzz
partial_ratio (substring) step must not.
"""

import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent.parent / "src" / "gold"))

from utils.serial_match import is_serial_match, match_serial  # noqa: E402


@pytest.mark.parametrize("query, telemetry", [
    ("PA0379-001", "PA 0379-001"),   # formatting only
    ("26086-T28", "T28"),            # Alentisque 2024: Telemetry serial is a token
    ("93091", "NX 93091"),           # Nordex prefix
    ("V20644", "20644"),             # Vestas prefix
    ("E825382", "825382"),           # Enercon prefix
    ("246385-1", "246385"),          # suffix
    ("M01-33738", "33738"),
    ("16875-BUE2", "V16875"),
    ("T2300_980 12", "2300980"),    # Schkortleben: serial split by separators
    ("T2300_697_ 04", "2300697"),
    ("T2300_69603", "2300696"),
])
def test_legitimate_pairs_match(query, telemetry):
    assert is_serial_match(query, telemetry)


@pytest.mark.parametrize("query, telemetry", [
    ("101", "101262433"),            # Alentisque 2026 label → Guijos WT001
    ("126", "101262433"),
    ("128", "38181288"),
    ("LRT01", "T01"),                # Les Rotes → Alentisque
    ("LRT01-33753", "T01"),
    ("JerezT01", "T01"),             # Jerez → Alentisque
    ("1", "T01"),                    # El Moral → Alentisque
    ("10", "T10"),
    ("61002982", "82982"),           # Alisio 2026 → Jaulín
    ("82952", "82951"),              # neighbour serial
    ("PA0379-002", "PA 0379-001"),   # other turbine of the same farm
    ("T2300_97608", "2300697"),
    ("T2300_981 01", "1501"),        # Schkortleben → Karstädt (short serial)
    ("9405300", "94053"),            # 5-char Telemetry serial: not eligible for D
    (None, "T01"),
    ("", "T01"),
    ("T01", None),
])
def test_wrong_farm_pairs_rejected(query, telemetry):
    assert not is_serial_match(query, telemetry)


def test_match_serial_returns_unique_candidate():
    cands = [
        {"serial_number": "T28", "wind_turbine_code": "WIT_ESALE_SS001_WT028"},
        {"serial_number": "101262433", "wind_turbine_code": "WIT_ESGUI_SS001_WT001"},
    ]
    assert match_serial("26086-T28", cands)["wind_turbine_code"] == "WIT_ESALE_SS001_WT028"
    assert match_serial("101", cands) is None


@pytest.mark.parametrize("label, turbine_number", [
    ("101", 1),
    ("107", 7),
    ("109", 9),
    ("124", 24),
    ("126", 26),
    ("128", 28),
    ("130", 30),
])
def test_alentisque_report_labels_match_only_their_farm(label, turbine_number):
    cands = [
        {
            "serial_number": f"T{i:02d}",
            "wind_turbine_code": f"WIT_ESALE_SS001_WT{i:03d}",
            "farm_code": "WIF_ESALE",
        }
        for i in range(1, 32)
    ]
    cands.append({
        "serial_number": "T01",
        "wind_turbine_code": "WIT_ESOTHER_SS001_WT001",
        "farm_code": "WIF_ESOTHER",
    })
    report_file = "Spain/Vestas/2026/6-JUN/2026-06 alentisque.pdf"

    ref = match_serial(label, cands, report_file=report_file)

    assert ref is not None
    assert ref["wind_turbine_code"] == f"WIT_ESALE_SS001_WT{turbine_number:03d}"


def test_alentisque_numeric_label_mapping_is_report_scoped():
    cands = [
        {"serial_number": "T01", "wind_turbine_code": "WIT_ESALE_SS001_WT001", "farm_code": "WIF_ESALE"},
        {"serial_number": "101262433", "wind_turbine_code": "WIT_ESGUI_SS001_WT001", "farm_code": "WIF_ESGUI"},
    ]

    assert match_serial("101", cands) is None
    assert match_serial(
        "101", cands, report_file="Spain/Vestas/2026/6-JUN/2026-06 guijos.pdf"
    ) is None
    assert match_serial(
        "100", cands, report_file="Spain/Vestas/2026/6-JUN/2026-06 alentisque.pdf"
    ) is None
    assert match_serial(
        "132", cands, report_file="Spain/Vestas/2026/6-JUN/2026-06 alentisque.pdf"
    ) is None


def test_alentisque_report_already_normalized_turbine_id_matches():
    cands = [
        {"serial_number": "T01", "wind_turbine_code": "WIT_ESALE_SS001_WT001", "farm_code": "WIF_ESALE"},
    ]
    assert match_serial(
        "T01", cands, report_file="Spain/Vestas/2026/6-JUN/2026-06 alentisque.pdf"
    )["wind_turbine_code"] == "WIT_ESALE_SS001_WT001"


def test_match_serial_schkortleben_picks_the_right_turbine():
    cands = [
        {"serial_number": s, "wind_turbine_code": f"WT_{s}"}
        for s in ["2300980", "2300981", "2300696", "2300697", "2300976"]
    ]
    assert match_serial("T2300_980 12", cands)["wind_turbine_code"] == "WT_2300980"
    assert match_serial("T2300_981_01", cands)["wind_turbine_code"] == "WT_2300981"
    assert match_serial("T2300_97608", cands)["wind_turbine_code"] == "WT_2300976"


def test_match_serial_abstains_when_ambiguous():
    cands = [
        {"serial_number": "T01", "wind_turbine_code": "WIT_ESAAA_SS001_WT001"},
        {"serial_number": "T01", "wind_turbine_code": "WIT_ESBBB_SS001_WT001"},
    ]
    assert match_serial("26114-T01", cands) is None


def test_match_serial_same_turbine_listed_twice_is_not_ambiguous():
    cands = [
        {"serial_number": "NX 93091", "wind_turbine_code": "WIT_DEQU2_SS001_WT001"},
        {"serial_number": "93091", "wind_turbine_code": "WIT_DEQU2_SS001_WT001"},
    ]
    assert match_serial("93091", cands)["wind_turbine_code"] == "WIT_DEQU2_SS001_WT001"


# ---------------------------------------------------------------------------
# drop_months_after_report (Spark)
# ---------------------------------------------------------------------------
def test_drop_months_after_report(spark):
    import datetime as dt
    from utils.helpers import drop_months_after_report

    sep = dt.datetime(2025, 9, 30)
    df = spark.createDataFrame(
        [
            ("alisio_sep", "2025-08", sep),   # past month → kept
            ("alisio_sep", "2025-09", sep),   # report month → kept
            ("alisio_sep", "2025-10", sep),   # +1 (misfiled tolerance) → kept
            ("alisio_sep", "2025-11", sep),   # +2 → dropped
            ("alisio_sep", "2026-09", sep),   # year shift → dropped
            ("no_report_date", "2030-01", None),  # unknown report month → kept
        ],
        "file string, date string, reporting_date timestamp",
    )
    kept = {(r.file, r.date) for r in drop_months_after_report(df).collect()}
    assert kept == {
        ("alisio_sep", "2025-08"),
        ("alisio_sep", "2025-09"),
        ("alisio_sep", "2025-10"),
        ("no_report_date", "2030-01"),
    }
