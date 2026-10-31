"""Unit tests for the business-managed `_ignored_errors` tab parsing."""

import pathlib
import sys

import pytest

sys.path.insert(
    0, str(pathlib.Path(__file__).parent.parent.parent / "src" / "silver")
)

from helpers.genai_ignored_mapping import (  # noqa: E402
    normalize_error_type,
    normalize_ignored_records,
    with_structural_seeds,
    STRUCTURAL_IGNORED,
)


HEADER = ["Fichier", "Raison", "Errors"]
ROWS = [
    ("Event report", "Rapport d'evenements", "Unprocessed"),
    ("Belgium/DWT/2026/5-MAY/x.pdf", "Fichier vide", "Unprocessed"),
    ("Spain/Gamesa/2026/1-JAN/y.pdf", "Pas matche", "Unmatched"),
]


def test_normalizes_columns_and_error_types():
    df = normalize_ignored_records(HEADER, ROWS)
    assert list(df.columns) == ["pattern", "reason", "error_type"]
    assert df["error_type"].tolist() == ["unprocessed", "unprocessed", "unmatch"]
    assert df["pattern"].tolist()[0] == "Event report"


def test_blank_and_duplicate_rows_are_dropped():
    rows = list(ROWS) + [("", "ligne vide", ""), ("Event report", "doublon", "")]
    df = normalize_ignored_records(HEADER, rows)
    assert len(df) == 3  # blank skipped, duplicate pattern kept once


def test_missing_errors_column_defaults_to_unprocessed():
    df = normalize_ignored_records(["Fichier", "Raison"], [("_Blade_", "composant")])
    assert df["error_type"].tolist() == ["unprocessed"]


def test_header_matching_is_lenient():
    # accents / casing / alternative spellings must still resolve
    df = normalize_ignored_records(
        ["FICHIER OU MOTIF", "Commentaire", "Erreur"],
        [("Logbook Entries", "journal", "unmatched")],
    )
    assert df["pattern"].tolist() == ["Logbook Entries"]
    assert df["reason"].tolist() == ["journal"]
    assert df["error_type"].tolist() == ["unmatch"]


def test_unknown_error_value_falls_back_to_unprocessed():
    assert normalize_error_type("n'importe quoi") == "unprocessed"
    assert normalize_error_type(None) == "unprocessed"
    assert normalize_error_type("Unmatched") == "unmatch"


def test_structural_seeds_always_present_after_merge():
    # A business tab that dropped the structural patterns must not make them
    # disappear from the final allowlist.
    df = normalize_ignored_records(
        HEADER, [("Belgium/DWT/2026/5-MAY/x.pdf", "vide", "Unprocessed")]
    )
    merged = with_structural_seeds(df)
    patterns = merged["pattern"].tolist()
    for seed_pattern, _ in STRUCTURAL_IGNORED:
        assert seed_pattern in patterns
    # business row is preserved
    assert "Belgium/DWT/2026/5-MAY/x.pdf" in patterns


def test_structural_seed_business_row_wins_on_duplicate():
    # If the business also lists a structural pattern, keep their reason.
    df = normalize_ignored_records(
        HEADER, [("Event report", "raison metier", "Unmatched")]
    )
    merged = with_structural_seeds(df)
    row = merged[merged["pattern"] == "Event report"]
    assert len(row) == 1
    assert row["reason"].iloc[0] == "raison metier"
    assert row["error_type"].iloc[0] == "unmatch"


def test_sheet_without_file_column_raises():
    with pytest.raises(ValueError, match="no file/pattern column"):
        normalize_ignored_records(["Turbine", "Serial"], [("A01", "123")])
