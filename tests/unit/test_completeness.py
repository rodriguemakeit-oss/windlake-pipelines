"""Completeness pass: recover turbines the LLM skipped on large tables."""

import importlib.util
import pathlib

import pandas as pd
import yaml

ROOT = pathlib.Path(__file__).parent.parent.parent
GAI = ROOT / "src" / "silver" / "compute_generative_ai"


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


completeness = _load("gai_completeness", GAI / "completeness.py")
prompt_generator = _load("gai_prompt_generator", GAI / "prompt_generator.py")

# OCR text excerpt of a Canunda report (serials + WTG numbers of 3 turbines).
SOURCE = """<table><tr><th>Turbine / Month</th><th>2026-03</th><th>2026-04</th></tr>
<tr><td>19214 - WTG07</td><td>100.00</td><td>100.00</td></tr>
<tr><td>19215 - WTG23</td><td>98.95</td><td>88.67</td></tr>
<tr><td>19223 - WTG09</td><td>99.10</td><td>97.40</td></tr></table>"""


def _df(rows):
    return pd.DataFrame(rows)


FIRST = _df([
    {"type": "turbine", "turbine_id": "19214 - WTG07", "2026-03": 100.0, "2026-04": 100.0},
    {"type": "turbine", "turbine_id": "19215 - WTG23", "2026-03": 98.95, "2026-04": 88.67},
    {"type": "site", "turbine_id": None, "2026-03": 98.53, "2026-04": 95.78},
])


def test_returned_turbine_ids_ignores_site():
    assert completeness.returned_turbine_ids(FIRST) == ["19214 - WTG07", "19215 - WTG23"]
    assert completeness.returned_turbine_ids(pd.DataFrame()) == []


def test_label_in_source_requires_every_identifying_token():
    assert completeness.label_in_source("19223 - WTG09", SOURCE)
    assert not completeness.label_in_source("19299 - WTG09", SOURCE)   # invented serial
    assert not completeness.label_in_source("19223 - WTG31", SOURCE)   # wrong WTG number
    assert not completeness.label_in_source("Site", SOURCE)            # no identifying token


def test_merge_adds_only_new_real_turbines():
    extra = _df([
        {"type": "turbine", "turbine_id": "19223 - WTG09", "2026-03": 99.10, "2026-04": 97.40},
        {"type": "turbine", "turbine_id": "19214 - WTG07", "2026-03": 1.0, "2026-04": 1.0},   # duplicate
        {"type": "turbine", "turbine_id": "19299 - WTG42", "2026-03": 90.0, "2026-04": 90.0},  # invented
        {"type": "site", "turbine_id": None, "2026-03": 0.0, "2026-04": 0.0},                 # site again
    ])
    merged, n_new = completeness.merge_missing_turbines(FIRST, extra, SOURCE)
    assert n_new == 1
    assert completeness.returned_turbine_ids(merged) == ["19214 - WTG07", "19215 - WTG23", "19223 - WTG09"]
    # first answer untouched (duplicate did not overwrite WTG07, single site row kept)
    wtg07 = merged[merged.turbine_id == "19214 - WTG07"]
    assert len(wtg07) == 1 and wtg07["2026-03"].iloc[0] == 100.0
    assert (merged["type"] == "site").sum() == 1


def test_merge_with_empty_answer_is_noop():
    merged, n_new = completeness.merge_missing_turbines(FIRST, pd.DataFrame(), SOURCE)
    assert n_new == 0 and merged is FIRST


def test_completion_prompt_lists_already_returned_turbines():
    config = yaml.safe_load((GAI / "resources" / "prompts.yaml").read_text())
    prompt = prompt_generator.generate_completion_prompt(config, ["19214 - WTG07", "19215 - WTG23"])
    assert "<already>19214 - WTG07 ; 19215 - WTG23</already>" in prompt
    assert "Never invent a turbine" in prompt


# ---------------------------------------------------------------------------
# Skip the completion call when Telemetry says nothing can be missing
# ---------------------------------------------------------------------------
serial_match = _load("gold_serial_match", ROOT / "src" / "gold" / "utils" / "serial_match.py")

CANUNDA = [
    {"serial_number": str(s), "wind_turbine_code": f"WIT_AUCAN_SS001_WT{i:03d}",
     "substation_code": "SUT_AUCAN_SS001", "farm_code": "WIF_AUCAN"}
    for i, s in enumerate(range(19214, 19237), start=1)
]
# Guijos-like farm: 2 substations reported in separate files (19 + 13 WTG)
GUIJOS = (
    [{"serial_number": str(101262400 + i), "wind_turbine_code": f"WIT_ESGUI_SS001_WT{i:03d}",
      "substation_code": "SUT_ESGUI_SS001", "farm_code": "WIF_ESGUI"} for i in range(1, 20)]
    + [{"serial_number": str(101262500 + i), "wind_turbine_code": f"WIT_ESGUI_SS002_WT{i:03d}",
        "substation_code": "SUT_ESGUI_SS002", "farm_code": "WIF_ESGUI"} for i in range(1, 14)]
)


def _expected(ids, cands):
    return completeness.expected_turbine_count(ids, cands, serial_match.match_serial)


def test_expected_count_is_telemetry_count_of_matched_substation():
    nine = [f"{19214 + i} - WTG{i + 1:02d}" for i in range(9)]
    assert _expected(nine, CANUNDA) == 23


def test_expected_count_per_substation_not_per_farm():
    # a Guijo I report only covers SS001 (19 WTG), not the whole farm (32)
    guijo1 = [f"G1-{i} | {101262400 + i}" for i in range(1, 20)]
    assert _expected(guijo1, GUIJOS) == 19


def test_expected_count_unknown_when_nothing_matches():
    assert _expected(["AL01", "AL02"], CANUNDA) is None   # alias-only labels


def test_needs_completion_rules():
    full = [f"{19214 + i} - WTG{i + 1:02d}" for i in range(23)]
    assert not completeness.needs_completion(full, 23)       # complete -> no 2nd call
    assert not completeness.needs_completion(full, 20)       # more than Telemetry -> no call
    assert completeness.needs_completion(full[:9], 23)       # 9/23 -> call
    assert completeness.needs_completion(full[:9], None)     # unknown -> call (safe)
    assert not completeness.needs_completion([], 23)         # nothing extracted -> nothing to complete
