"""Unit tests for the GenAI extraction response parsing.

Focus: `get_dataframe_from_openai` must fail with a clear, classifiable message
when the LLM answer has no fenced ```json``` block (previously crashed with
"'NoneType' object has no attribute 'group'" and was mis-bucketed as a parsing
bug instead of "0 ligne extraite" / empty-or-unparsable content).

The extraction module pulls in heavy Azure/OpenAI deps at import time, so the
whole test is skipped when those aren't installed in the local env.
"""

import pathlib
import sys
import types

import pytest

sys.path.insert(
    0, str(pathlib.Path(__file__).parent.parent.parent / "src" / "silver")
)

base = pytest.importorskip(
    "compute_generative_ai.base",
    reason="Azure/OpenAI deps not available in local test env",
)


def _fake_answer(content):
    """Build a minimal object shaped like an openai ChatCompletion."""
    message = types.SimpleNamespace(content=content)
    choice = types.SimpleNamespace(message=message)
    return types.SimpleNamespace(choices=[choice])


def test_no_json_block_raises_empty_or_unparsable():
    answer = _fake_answer("Sorry, I could not find any availability table.")
    with pytest.raises(ValueError, match="empty or unparsable content"):
        base.get_dataframe_from_openai(answer)


def test_none_content_raises_empty_or_unparsable():
    answer = _fake_answer(None)
    with pytest.raises(ValueError, match="empty or unparsable content"):
        base.get_dataframe_from_openai(answer)


def test_missing_monthly_availability_key_raises():
    # Valid fenced JSON but without the expected "monthly_availability" column.
    answer = _fake_answer('```json\n[{"type": "turbine", "turbine_id": "A01"}]\n```')
    with pytest.raises(ValueError, match="empty or unparsable content"):
        base.get_dataframe_from_openai(answer)


class _StubFolder:
    """Minimal FolderAdapter returning fixed bytes as a readable stream."""

    def __init__(self, data: bytes):
        self._data = data

    def get_download_stream(self, path):  # noqa: ARG002
        import io

        return io.BytesIO(self._data)

    def get_writer(self, path):  # pragma: no cover - unused here
        raise NotImplementedError

    def upload_data(self, path, data):  # pragma: no cover - unused here
        raise NotImplementedError


def test_openpyxl_reads_all_sheets_when_sheets_none():
    # Regression: sheets=None (maintainer absent from sheets_per_maintainer, e.g.
    # DWT/Vestas) must read ALL sheets instead of crashing on `for s in None`.
    import io

    from openpyxl import Workbook

    wb = Workbook()
    ws1 = wb.active
    ws1.title = "Sheet1"
    ws1["A1"] = "availability"
    ws1["B1"] = 97.5
    ws2 = wb.create_sheet("Sheet2")
    ws2["A1"] = "turbine"
    ws2["B1"] = "WTG1"
    buf = io.BytesIO()
    wb.save(buf)

    content = base._get_excel_data_from_openpyxl("x.xlsx", None, _StubFolder(buf.getvalue()))
    assert "availability" in content  # sheet 1 read
    assert "WTG1" in content  # sheet 2 read too

