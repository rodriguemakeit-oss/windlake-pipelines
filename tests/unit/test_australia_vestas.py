"""Australia / Vestas onboarding (Canunda, WIF_AUCAN).

Telemetry (2026-10): WIF_AUCAN = 23 Vestas WTG, serials 19214..19236.
WIF_AUWIL (Willogoleche, GE) is the other AU wind farm and must never collide.
"""

import importlib.util
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT / "src" / "gold"))

from utils.serial_match import match_serial  # noqa: E402
from utils.turbine_alias import COUNTRY_TO_PIC_CODE  # noqa: E402

# Load enums.py directly: the compute_generative_ai package __init__ pulls the
# Azure SDK, not needed (nor installed) for this test.
_spec = importlib.util.spec_from_file_location(
    "gai_enums", ROOT / "src" / "silver" / "compute_generative_ai" / "enums.py"
)
_enums = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_enums)
Maintainers = _enums.Maintainers

CANUNDA = [
    {"serial_number": str(s), "wind_turbine_code": f"WIT_AUCAN_SS001_WT{i:03d}", "farm_code": "WIF_AUCAN"}
    for i, s in enumerate(range(19214, 19237), start=1)
]
WILLOGOLECHE = [
    {"serial_number": s, "wind_turbine_code": f"WIT_AUWIL_SS001_WT{i:03d}", "farm_code": "WIF_AUWIL"}
    for i, s in enumerate(["34172982", "34172983", "38171864", "38171865"], start=1)
]
AU_CANDIDATES = CANUNDA + WILLOGOLECHE


def test_australia_country_code():
    assert COUNTRY_TO_PIC_CODE["australia"] == "AU"


def test_scope_regex_includes_australian_wind_farms():
    import re
    yml = (ROOT / "databricks.yml").read_text()
    default = re.search(r"ltsa_scope_regex:.*?default:\s*\"([^\"]+)\"", yml, re.S).group(1)
    assert re.search(default, "WIF_AUCAN") and re.search(default, "WIF_AUWIL")
    assert not re.search(default, "SOF_AUJUN")   # AU solar stays out of scope


def test_vestas_folder_maps_to_vestas_maintainer():
    # extraction derives the maintainer from "Australia/Vestas/..." (2nd segment)
    assert Maintainers("Vestas".lower()) is Maintainers.VESTAS


@pytest.mark.parametrize("label, expected_serial", [
    ("19214", "19214"),            # bare serial
    ("V19214", "19214"),           # Vestas prefix
    ("19220 - T07", "19220"),      # "serial - label" (as Alentisque/Moervaart reports)
    ("T07 | 19220", "19220"),      # "label | serial" (SERIAL_NUMBER_SEP)
    ("19236-WTG23", "19236"),
])
def test_canunda_labels_match_the_right_turbine(label, expected_serial):
    ref = match_serial(label, AU_CANDIDATES)
    assert ref is not None and ref["serial_number"] == expected_serial
    assert ref["farm_code"] == "WIF_AUCAN"


@pytest.mark.parametrize("label", ["T07", "WTG07", "7", "07", "SC-040540", "22300"])
def test_canunda_ambiguous_or_foreign_labels_are_not_guessed(label):
    # bare labels / contract numbers must not attach to any AU turbine
    assert match_serial(label, AU_CANDIDATES) is None
