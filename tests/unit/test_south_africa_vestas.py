"""South Africa / Vestas onboarding (West Coast One, WIF_ZAWCO).

Telemetry (2026-10): WIF_ZAWCO = 47 Vestas WTG named WCO_01..WCO_47, serials
202684..202730. Reports label turbines '<serial> - C<n>T<nn>' (e.g. '202718 - C5T35').
"""

import pathlib
import re
import sys

import pytest

ROOT = pathlib.Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT / "src" / "gold"))

from utils.serial_match import match_serial  # noqa: E402
from utils.turbine_alias import COUNTRY_TO_PIC_CODE  # noqa: E402

WEST_COAST = [
    {"serial_number": str(s), "wind_turbine_code": f"WIT_ZAWCO_SS001_WT{i:03d}",
     "farm_code": "WIF_ZAWCO", "substation_code": "SUT_ZAWCO_SS001"}
    for i, s in enumerate(range(202684, 202731), start=1)
]


def test_south_africa_country_code():
    # folder "South Africa/Vestas/..." -> PIC code used in the Telemetry match key
    assert COUNTRY_TO_PIC_CODE["south africa"] == "ZA"


def test_scope_regex_includes_south_african_wind_farms():
    yml = (ROOT / "databricks.yml").read_text()
    default = re.search(r"ltsa_scope_regex:.*?default:\s*\"([^\"]+)\"", yml, re.S).group(1)
    assert re.search(default, "WIF_ZAWCO")
    assert not re.search(default, "SOF_ZAKAU")   # ZA solar stays out of scope


@pytest.mark.parametrize("label, serial", [
    ("202718 - C5T35", "202718"),
    ("202684 - C1T01", "202684"),
    ("202730", "202730"),
    ("C4T46 | 202729", "202729"),
])
def test_west_coast_labels_match_the_right_turbine(label, serial):
    ref = match_serial(label, WEST_COAST)
    assert ref is not None and ref["serial_number"] == serial


@pytest.mark.parametrize("label", ["C5T35", "T35", "35", "SC-020139", "020139"])
def test_bare_labels_and_contract_number_are_not_guessed(label):
    assert match_serial(label, WEST_COAST) is None
