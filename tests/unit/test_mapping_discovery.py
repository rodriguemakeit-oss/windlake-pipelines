"""Mapping workbook discovery: by configured name, else by content (renamed file)."""

import contextlib
import io
import pathlib
import sys

import pytest
from openpyxl import Workbook, load_workbook

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent.parent / "src" / "silver"))

from helpers.turbine_alias_mapping import (  # noqa: E402
    COLUMN_RENAMES,
    find_mapping_sheet,
    pick_single_mapping_file,
)


def _xlsx(header, tab="Mapping", extra_tab=None):
    wb = Workbook()
    ws = wb.active
    if extra_tab:
        ws.title = extra_tab
        ws.append(["something", "else"])
        ws = wb.create_sheet(tab)
    else:
        ws.title = tab
    ws.append(header)
    ws.append(["Spain", "WIF_ESALI", "WIT_X", "AL01", "100187034", "SGRE"])
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


MAPPING = _xlsx(list(COLUMN_RENAMES), tab="Feuil1", extra_tab="_ignored_errors")
OTHER = _xlsx(["foo", "bar"])


def _load(data):
    return load_workbook(io.BytesIO(data), read_only=True, data_only=True)


def test_find_mapping_sheet_by_headers_whatever_the_tab_name():
    sheet = find_mapping_sheet(_load(MAPPING))
    assert sheet is not None and sheet.title == "Feuil1"
    assert find_mapping_sheet(_load(OTHER)) is None


def test_pick_single_mapping_file():
    assert pick_single_mapping_file(["a.xlsx"], ["x"]) == "a.xlsx"
    with pytest.raises(FileNotFoundError):
        pick_single_mapping_file([], ["0-ACCEPTED FORMATS/SGRE", ""])
    with pytest.raises(ValueError, match="Several workbooks"):
        pick_single_mapping_file(["a.xlsx", "copy of a.xlsx"], ["x"])


class _FakeAdapter:
    """Minimal SharePointAdapter stand-in: {folder: {name: bytes}}."""

    def __init__(self, tree):
        self.tree = tree

    def list_folder_files(self, subdir=""):
        return list(self.tree.get(subdir, {}))

    @contextlib.contextmanager
    def get_download_stream(self, rel_path):
        folder, _, name = rel_path.rpartition("/")
        try:
            yield io.BytesIO(self.tree[folder][name])
        except KeyError:
            raise FileNotFoundError(rel_path)


def _discover(adapter, file_name, folders):
    """Same algorithm as turbine_alias_load._load_mapping_workbook (notebook)."""
    for folder in folders:
        rel = f"{folder}/{file_name}" if folder else file_name
        try:
            with adapter.get_download_stream(rel) as s:
                return rel, _load(s.read())
        except FileNotFoundError:
            pass
    hits = {}
    for folder in folders:
        for name in adapter.list_folder_files(folder):
            if not name.lower().endswith(".xlsx") or name.startswith("~$"):
                continue
            rel = f"{folder}/{name}" if folder else name
            with adapter.get_download_stream(rel) as s:
                wb = _load(s.read())
            if find_mapping_sheet(wb) is not None:
                hits[rel] = wb
    rel = pick_single_mapping_file(sorted(hits), folders)
    return rel, hits[rel]


FOLDERS = ["0-ACCEPTED FORMATS/SGRE", ""]


def test_configured_name_still_works():
    a = _FakeAdapter({"0-ACCEPTED FORMATS/SGRE": {"SGRE_Mapping_Table.xlsx": MAPPING}})
    assert _discover(a, "SGRE_Mapping_Table.xlsx", FOLDERS)[0] == \
        "0-ACCEPTED FORMATS/SGRE/SGRE_Mapping_Table.xlsx"


def test_renamed_file_found_by_content_among_other_xlsx():
    a = _FakeAdapter({
        "0-ACCEPTED FORMATS/SGRE": {"Some report.xlsx": OTHER, "LTSA mapping v2.xlsx": MAPPING,
                                    "notes.pdf": b"%PDF"},
        "": {"~$LTSA mapping v2.xlsx": b"lock"},
    })
    assert _discover(a, "SGRE_Mapping_Table.xlsx", FOLDERS)[0] == \
        "0-ACCEPTED FORMATS/SGRE/LTSA mapping v2.xlsx"


def test_two_copies_refused():
    a = _FakeAdapter({"0-ACCEPTED FORMATS/SGRE": {"A.xlsx": MAPPING, "A - Copy.xlsx": MAPPING}})
    with pytest.raises(ValueError, match="Several workbooks"):
        _discover(a, "SGRE_Mapping_Table.xlsx", FOLDERS)
