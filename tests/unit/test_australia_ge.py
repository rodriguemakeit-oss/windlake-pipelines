"""Australia / GE onboarding (Willogoleche, WIF_AUWIL).

Telemetry (2026-10): WIF_AUWIL = 32 GE WTG named T01..T32. Reports name turbines
'WTG T01' only; the serial comes from the report's 'WTG no. / Serial #'
maintenance table (identical to Telemetry), joined deterministically
(report_postprocess.attach_report_serials) -> 'T01|38171870'.
The per-WTG contractual table ('Contractual Availability Monthly summary')
is appended at the END of the PDF (page 25-54), beyond PAGE_LIMIT.
"""

import importlib.util
import json
import pathlib
import re
import sys

import pandas as pd
import pytest
import yaml

ROOT = pathlib.Path(__file__).parent.parent.parent
GAI = ROOT / "src" / "silver" / "compute_generative_ai"
sys.path.insert(0, str(ROOT / "src" / "gold"))

from utils.serial_match import match_serial  # noqa: E402


def _load(name, path):
    # Load modules by path: the compute_generative_ai package __init__ pulls the
    # Azure SDK, not needed (nor installed) for these tests.
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_enums = _load("gai_enums", GAI / "enums.py")
_pages = _load("gai_pages", GAI / "page_selection.py")
_prompts = _load("gai_prompt_generator", GAI / "prompt_generator.py")
_complete = _load("gai_completeness", GAI / "completeness.py")
_post = _load("gai_report_postprocess", GAI / "report_postprocess.py")
Maintainers = _enums.Maintainers

# Telemetry T-name -> serial (== report 'WTG no. / Serial #' table)
WILLOGOLECHE_SERIALS = [
    "38171870", "38171871", "34172984", "34172985", "34172986", "38171868", "38171877", "38171869",
    "38171864", "38171865", "38171866", "38171867", "34172987", "34172988", "34172983", "34172982",
    "38171875", "38171874", "38171873", "38171872", "38171876", "34172989", "38171879", "38171878",
    "38171880", "38171881", "38171882", "38171883", "38171884", "38171885", "38171886", "38171887",
]
WILLOGOLECHE = [
    {"serial_number": s, "wind_turbine_code": f"WIT_AUWIL_SS001_WT{i:03d}",
     "farm_code": "WIF_AUWIL", "substation_code": "SUT_AUWIL_SS001"}
    for i, s in enumerate(WILLOGOLECHE_SERIALS, start=1)
]
CANUNDA = [
    {"serial_number": str(s), "wind_turbine_code": f"WIT_AUCAN_SS001_WT{i:03d}",
     "farm_code": "WIF_AUCAN", "substation_code": "SUT_AUCAN_SS001"}
    for i, s in enumerate(range(19214, 19237), start=1)
]
AU_CANDIDATES = CANUNDA + WILLOGOLECHE


def test_ge_folder_maps_to_ge_maintainer():
    assert Maintainers("GE".lower()) is Maintainers.GE


@pytest.mark.parametrize("n", range(1, 33))
def test_label_with_report_serial_matches_the_same_telemetry_turbine(n):
    serial = WILLOGOLECHE_SERIALS[n - 1]
    for label in (f"T{n:02d}|{serial}", f"T{n:02d} | {serial}", serial):
        ref = match_serial(label, AU_CANDIDATES)
        assert ref is not None and ref["wind_turbine_code"] == f"WIT_AUWIL_SS001_WT{n:03d}"


@pytest.mark.parametrize("label", ["WTG T01", "T01", "T-001", "WTG-001", "1"])
def test_bare_wtg_labels_are_not_guessed(label):
    # without the serial no AU turbine may be picked (never collide with Canunda)
    assert match_serial(label, AU_CANDIDATES) is None


def test_full_extraction_skips_completion_call():
    ids = [f"T{n:02d}|{s}" for n, s in enumerate(WILLOGOLECHE_SERIALS, start=1)]
    expected = _complete.expected_turbine_count(ids, AU_CANDIDATES, match_serial)
    assert expected == 32
    assert not _complete.needs_completion(ids, expected)
    assert _complete.needs_completion(ids[:20], expected)


def _ocr(pages):
    return pd.DataFrame({
        "metadata": [json.dumps({"page": p}) for p in pages],
        "text": [t for t in pages.values()],
    })


GE_PRIORITY = r"contractual\s+availability\s+monthly\s+summary|serial\s*#"


def test_priority_pages_kept_beyond_page_limit():
    file_data = _ocr({
        14: "Turbine availability 96.99% and below",
        15: "WTG no.  Serial #  CG #  Commissioning",
        25: "Contractual Availability Monthly summary  Page 1/3",
        26: "Contractual Availability Monthly summary  ASSET OVERVIEW 32 entries",
        40: "GE appendix availability [raw]",
    })
    pages = _pages.select_pages(["14", "25", "26", "40"], file_data, 23, GE_PRIORITY)
    assert pages == ["14", "15", "25", "26"]          # annex page 40 still dropped


def test_page_after_priority_page_is_kept():
    # Jun/Jul-2026: 'Serial #' table continues on the next page (T14-T32, no header)
    file_data = _ocr({14: "WTG no.  Serial #  CG #", 15: "T14 34172988 ... T32 38171887", 16: "BOP"})
    assert _pages.select_pages([], file_data, 23, GE_PRIORITY) == ["14", "15"]


def test_without_priority_pattern_behaviour_is_unchanged():
    file_data = _ocr({5: "availability", 30: "Contractual Availability Monthly summary"})
    assert _pages.select_pages(["5", "30"], file_data, 23, None) == ["5"]


def test_ge_priority_pattern_is_configured():
    src = (GAI / "constants.py").read_text()
    assert "priority_keyword_per_maintainer" in src
    assert re.search(re.escape(r"contractual\s+availability\s+monthly\s+summary"), src)


@pytest.fixture(scope="module")
def ge_system_prompt():
    cfg = yaml.safe_load((GAI / "resources" / "prompts.yaml").read_text())
    return _prompts.generate_system_prompt_di(cfg, cfg["custom_prompts"]["ge"])


def test_ge_prompt_handles_willogoleche_format(ge_system_prompt):
    assert "ASSET OVERVIEW" in ge_system_prompt
    assert "REFERENCE PERIOD HISTORY" in ge_system_prompt
    assert "'Turbine | System Number | ... | RPPBA ... Final'" in ge_system_prompt
    assert "return ONLY the site row" in ge_system_prompt
    assert "Never copy the site or dashboard value to the turbines" in ge_system_prompt


def test_ge_prompt_keeps_existing_ge_instructions(ge_system_prompt):
    # non-regression for Spain / Italy / Poland / Romania GE reports
    assert "system number" in ge_system_prompt
    assert "Availability (%) monthly evolution for the contractual year aerogenerator" in ge_system_prompt
    assert "Production Based Availability (After MAL Adjustments) (%)" in ge_system_prompt
    assert "Final calculation" in ge_system_prompt


# Document Intelligence HTML, as in the Jun-2026 report: maintenance table
# 'WTG no. | Serial #' + ASSET OVERVIEW rows without serial.
DI_TEXT = "".join(
    f"<tr><td>T{n:02d}</td>\n<td>{s}</td>\n<td>1</td><td>30-Apr-19</td></tr>"
    for n, s in enumerate(WILLOGOLECHE_SERIALS, start=1)
) + "<table><tr><td>WTG T01</td><td>1013.15</td><td>0.04</td><td>100.00</td></tr></table>"


def test_crosswalk_read_from_report_matches_telemetry():
    cw = _post.report_serial_crosswalk(DI_TEXT)
    assert cw == {n: s for n, s in enumerate(WILLOGOLECHE_SERIALS, start=1)}


def test_serials_attached_and_llm_shifted_serials_overridden():
    # Jun-2026: LLM shifted serials from T25 ; Jul-2026: dropped them from T14
    df = pd.DataFrame({
        "type": ["turbine", "turbine", "turbine", "turbine", "site"],
        "turbine_id": ["T25|34172986", "WTG T14", "WTG-001|38171870", "WIG-U24|381/1878", None],
        "2026-06": [99.1, 98.0, 100.0, 97.0, 99.4],
    })
    out = _post.attach_report_serials(df, DI_TEXT)
    assert out["turbine_id"].tolist()[:4] == [
        "T25|38171880", "T14|34172988", "T01|38171870", "T24|38171878",  # OCR 'WIG-U24' repaired
    ]
    assert out["turbine_id"].isna().tolist()[-1]  # site row untouched
    for tid in out["turbine_id"][:4]:
        n = int(re.search(r"(\d+)\|", tid).group(1))
        assert match_serial(tid, AU_CANDIDATES)["wind_turbine_code"] == f"WIT_AUWIL_SS001_WT{n:03d}"


def test_bare_serial_labels_of_other_ge_countries_untouched():
    # Spain / Italy / Poland / Romania GE ids are bare serials or 'serial | model'
    df = pd.DataFrame({"type": ["turbine"] * 3,
                       "turbine_id": ["34181180", "27120183 | 2.5-103 DFIG", "28140306|2.75-120 DFIG"],
                       "2024-01": [99.0, 98.0, 97.0]})
    out = _post.attach_report_serials(df, DI_TEXT)
    assert out["turbine_id"].tolist() == df["turbine_id"].tolist()


def test_duplicate_turbine_after_rewrite_is_dropped():
    df = pd.DataFrame({"type": ["turbine", "turbine"], "turbine_id": ["T05|38171868", "WTG T05"],
                       "2026-06": [99.0, 98.0]})
    out = _post.attach_report_serials(df, DI_TEXT)
    assert len(out) == 1 and out["turbine_id"].iloc[0] == "T05|34172986"


def test_site_value_copied_to_every_turbine_is_dropped():
    # Draft Aug-2025: no per-turbine table, LLM returned 32 x 90.1 (= site)
    df = pd.DataFrame({"type": ["turbine"] * 32 + ["site"], "turbine_id": [f"T{n:02d}" for n in range(1, 33)] + [None],
                       "2025-08": [90.1] * 33})
    out = _post.drop_copied_turbine_values(df)
    assert out.loc[out["type"] == "turbine", "2025-08"].isna().all()
    assert out.loc[out["type"] == "site", "2025-08"].tolist() == [90.1]


def test_real_turbine_values_kept():
    all_100 = pd.DataFrame({"type": ["turbine"] * 6, "turbine_id": list("ABCDEF"), "2026-03": [100.0] * 6})
    varied = pd.DataFrame({"type": ["turbine"] * 6, "turbine_id": list("ABCDEF"),
                           "2026-03": [99.1, 98.0, 100.0, 97.2, 99.9, 96.0]})
    few = pd.DataFrame({"type": ["turbine"] * 3, "turbine_id": list("ABC"), "2026-03": [95.0] * 3})
    for df in (all_100, varied, few):
        assert _post.drop_copied_turbine_values(df)["2026-03"].tolist() == df["2026-03"].tolist()



def test_chart_values_dropped_table_values_kept():
    # Jan-2026 / Dec-2025: LLM read the "Availability by WTG" bar chart (figure text)
    di = DI_TEXT + "<figure>Availability by WTG 100 90 85.49 85.85</figure>" \
        + "<tr><td>WTG T02</td><td>1009.20</td><td>0.03</td><td>99.46</td></tr>"
    df = pd.DataFrame({"type": ["turbine", "turbine", "turbine", "site"],
                       "turbine_id": ["T01|38171870", "T14|34172988", "T02|38171871", None],
                       "2026-01": [85.49, 85.85, 99.46, 83.7]})
    out = _post.keep_table_turbine_values(df, di)
    assert out["2026-01"].isna().tolist()[:2] == [True, True]   # chart values dropped
    assert out["2026-01"].tolist()[2:] == [99.46, 83.7]   # table value + site row untouched


def test_table_guard_inactive_without_crosswalk():
    # other GE countries (xlsx / Spain PDFs): no "T01 | serial" table -> untouched
    df = pd.DataFrame({"type": ["turbine"], "turbine_id": ["34181180"], "2024-01": [97.12]})
    assert _post.keep_table_turbine_values(df, "no table here")["2024-01"].tolist() == [97.12]
