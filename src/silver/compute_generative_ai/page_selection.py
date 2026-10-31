"""Page selection before Document Intelligence (Tesseract pre-filter).

Pages matching the maintainer keyword are kept only below PAGE_LIMIT (the tail of
long reports is mostly annexes). Some maintainers put the availability table
itself at the end (GE Willogoleche: "Contractual Availability Monthly summary"
appended after page 25-54) or the label → serial crosswalk on a page without the
keyword ("WTG no. / Serial #" maintenance table): pages matching the maintainer
*priority* pattern are always kept, whatever their position.

Pure pandas — unit-testable without the Azure SDK.
"""

import json
from typing import Iterable, List, Optional

import pandas as pd


def page_of(metadata) -> str:
    """Page number (as str) stored in the OCR row metadata ({"page": n})."""
    return str(json.loads(str(metadata).replace("'", '"'))["page"])


def select_pages(
    matched_pages: Iterable,
    file_data: pd.DataFrame,
    page_limit: int,
    priority_pattern: Optional[str] = None,
) -> List[str]:
    """Keyword pages below ``page_limit`` + every page matching ``priority_pattern``
    and the page right after it (tables continued over a page break, e.g. the
    'WTG no. | Serial #' table: T01-T13 on one page, T14-T32 on the next).
    """
    keep = {str(p) for p in matched_pages if int(p) < page_limit}
    if priority_pattern and file_data is not None and not file_data.empty:
        hit = file_data["text"].fillna("").str.lower().str.contains(priority_pattern, regex=True)
        all_pages = {page_of(m) for m in file_data["metadata"]}
        for p in {page_of(m) for m in file_data.loc[hit, "metadata"]}:
            keep |= {p, str(int(p) + 1)} & all_pages
    return sorted(keep, key=int)
