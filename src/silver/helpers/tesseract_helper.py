"""Tesseract helper for PDF → OCR DataFrame pre-processing.

Replaces the Tesseract/EasyOCR step that was done upstream in Dataiku.
Produces a DataFrame with the same schema as the original `ocr_extract`:

    Columns: file (str), page (int), metadata (str JSON), text (str)

The `text` column contains the OCR-extracted text for each page.
The `metadata` column is a JSON string with at minimum {"page": <int>},
matching the format expected by `filter_data()` and `get_document_data()`
in compute_generative_ai/base.py.

Requirements (Databricks cluster init script):
    #!/bin/bash
    apt-get install -y tesseract-ocr tesseract-ocr-fra tesseract-ocr-spa
    pip install pytesseract pdf2image

Usage:
    from helpers.tesseract_helper import pdf_to_page_dataframe

    df = pdf_to_page_dataframe(
        file_path="vestas/france/2024/01/report.pdf",
        folder=reports_adapter,
    )
    # df.columns → ["file", "page", "metadata", "text"]
"""

import json
import io
import tempfile
import os
from typing import Optional, List, TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from compute_generative_ai.base import FolderAdapter


# Minimum characters a page's native text layer must hold to be trusted (and thus
# skip rendering + Tesseract OCR). Digitally-generated reports easily clear this;
# scanned/image pages fall below it and go through the OCR fallback.
MIN_NATIVE_CHARS = 20


def _ocr_page(page_image, lang: str) -> str:
    """Run Tesseract OCR on a single PIL image.

    Import is deferred so the module can be imported on machines where
    pytesseract / tesseract binary is not installed (e.g. local Mac dev).
    """
    try:
        import pytesseract  # type: ignore
    except ImportError as exc:
        raise ImportError(
            "pytesseract is required for Tesseract OCR. "
            "Install it via: pip install pytesseract  "
            "and ensure the tesseract binary is on PATH."
        ) from exc

    return pytesseract.image_to_string(page_image, lang=lang)


def _iter_pdf_pages(pdf_bytes: bytes, dpi: int, min_native_chars: int = MIN_NATIVE_CHARS):
    """Yield ``(native_text, image)`` for each PDF page.

    For each page the PDF's own text layer is read first (PyMuPDF
    ``page.get_text()``). When that layer already holds enough characters the
    page is emitted as ``(text, None)`` and **no image is rendered and no OCR is
    run** — the common case for digitally-generated reports and where almost all
    the speed-up comes from. Only pages with little/no native text (scanned /
    image pages) are rendered to a PIL image and returned as ``(text, image)``
    for the Tesseract fallback.

    Generator (not a list) so a single large document is never fully held in
    memory. Each pixmap is freed right after the image is produced.
    """
    try:
        import fitz  # type: ignore  # PyMuPDF
    except ImportError as exc:
        raise ImportError(
            "pymupdf is required for PDF → image conversion. "
            "Install it via: pip install pymupdf"
        ) from exc

    from PIL import Image

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    zoom = dpi / 72  # 72 is PDF default DPI
    mat = fitz.Matrix(zoom, zoom)
    try:
        for page in doc:
            native_text = page.get_text() or ""
            if len(native_text.strip()) >= min_native_chars:
                # Native text layer is sufficient — skip rendering + OCR.
                yield native_text, None
                continue
            pix = page.get_pixmap(matrix=mat, alpha=False)
            # Build the PIL image straight from the raw RGB buffer. This avoids a
            # PNG encode+decode round-trip per page, which was the dominant
            # per-page CPU cost of the OCR step.
            img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
            pix = None  # release the raw pixmap buffer immediately
            yield native_text, img
    finally:
        doc.close()


def _page_count(pdf_bytes: bytes) -> int:
    """Return the number of pages in a PDF without rendering it."""
    try:
        import fitz  # type: ignore  # PyMuPDF
    except ImportError as exc:
        raise ImportError(
            "pymupdf is required for PDF → image conversion. "
            "Install it via: pip install pymupdf"
        ) from exc

    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        return doc.page_count


def _row(file_path: str, page_num: int, text: str) -> dict:
    """Build one `ocr_extract`-schema row."""
    return {
        "file": file_path,
        "page": page_num,
        "metadata": json.dumps({"page": page_num}),
        "text": text or "",
    }


def _ocr_rows_all_at_once(
    pdf_bytes: bytes, file_path: str, lang: str, dpi: int,
    page_limit: Optional[int], n_pages: int, progress_prefix: str,
    min_native_chars: int = MIN_NATIVE_CHARS,
) -> List[dict]:
    """Fast path: use native text where present, OCR only the image pages.

    Pages with a usable native text layer are used as-is (no image, no OCR);
    only image pages are held in memory, so ``MemoryError`` is far less likely.
    If it still happens the caller falls back to the page-by-page path.
    """
    collected = []  # (page_num, native_text, image_or_None)
    images = []
    try:
        for page_num, (native_text, image) in enumerate(
            _iter_pdf_pages(pdf_bytes, dpi=dpi, min_native_chars=min_native_chars),
            start=1,
        ):
            if page_limit and page_num > page_limit:
                break
            collected.append((page_num, native_text, image))
            if image is not None:
                images.append(image)
        rows = []
        for page_num, native_text, image in collected:
            if image is None:
                rows.append(_row(file_path, page_num, native_text))
                print(f"   ├─ ✅ {progress_prefix} page {page_num}/{n_pages} texte natif")
            else:
                text = _ocr_page(image, lang=lang)
                rows.append(_row(file_path, page_num, text))
                print(f"   ├─ 🔍 {progress_prefix} page {page_num}/{n_pages} OCR (scannée)")
        return rows
    finally:
        for image in images:
            try:
                image.close()
            except Exception:
                pass


def _ocr_rows_page_by_page(
    pdf_bytes: bytes, file_path: str, lang: str, dpi: int,
    page_limit: Optional[int], n_pages: int, progress_prefix: str,
    min_native_chars: int = MIN_NATIVE_CHARS,
) -> List[dict]:
    """Low-memory path: one page at a time (native text, else render + OCR).

    Never holds the whole document in RAM — safe for very large PDFs that OOM
    in the all-at-once path.
    """
    rows = []
    for page_num, (native_text, image) in enumerate(
        _iter_pdf_pages(pdf_bytes, dpi=dpi, min_native_chars=min_native_chars),
        start=1,
    ):
        if page_limit and page_num > page_limit:
            break
        if image is None:
            rows.append(_row(file_path, page_num, native_text))
            print(f"   ├─ ✅ {progress_prefix} page {page_num}/{n_pages} texte natif")
        else:
            text = _ocr_page(image, lang=lang)
            image.close()  # free the page image before rendering the next one
            rows.append(_row(file_path, page_num, text))
            print(f"   ├─ 🔍 {progress_prefix} page {page_num}/{n_pages} OCR (scannée, page-by-page)")
    return rows


def pdf_to_page_dataframe(
    file_path: str,
    folder: "FolderAdapter",
    lang: str = "eng+fra+spa",
    dpi: int = 150,
    page_limit: Optional[int] = None,
    progress_prefix: str = "",
    low_memory: bool = False,
    max_pages_all_at_once: int = 10,
    min_native_chars: int = MIN_NATIVE_CHARS,
) -> pd.DataFrame:
    """Extract text from each PDF page using Tesseract OCR.

    Produces a DataFrame compatible with the `ocr_extract` schema
    used by `filter_data()` and `get_document_data()` in base.py.

    Small documents are rendered all at once and OCR'd (fast). Documents with
    more than ``max_pages_all_at_once`` pages go straight to the page-by-page
    path to avoid rendering the whole document in memory. As a last safety net,
    if the all-at-once path still raises ``MemoryError`` the file is retried one
    page at a time. Set ``low_memory=True`` to force page-by-page for any size.

    Args:
        file_path: Relative path of the PDF inside the FolderAdapter.
        folder: FolderAdapter giving access to the source files.
        lang: Tesseract language string (e.g. "eng+fra+spa").
        dpi: Resolution for PDF → image conversion. 200 is a good default.
        page_limit: If set, only process the first N pages.
        low_memory: Force the page-by-page path (skip the all-at-once attempt).
        max_pages_all_at_once: Above this page count, use page-by-page directly.

    Returns:
        DataFrame with columns: file, page, metadata (JSON str), text.
    """
    print(f"   ├─ 🔍 Tesseract OCR {progress_prefix}: reading [{file_path}]")
    with folder.get_download_stream(file_path) as f:
        pdf_bytes = f.read()

    total_pages = _page_count(pdf_bytes)
    n_pages = min(total_pages, page_limit) if page_limit else total_pages
    print(f"   ├─ 📄 {progress_prefix} {n_pages} page(s) to OCR")

    # Big documents skip the all-at-once attempt entirely: rendering every page
    # up front is what risks the OOM, so we don't even try it past the threshold.
    _use_batch = not low_memory and n_pages <= max_pages_all_at_once
    if not _use_batch and not low_memory:
        print(
            f"   ├─ 🧮 {progress_prefix} {n_pages} page(s) > {max_pages_all_at_once} "
            f"— mode page-par-page (mémoire réduite)"
        )

    rows = None
    if _use_batch:
        try:
            rows = _ocr_rows_all_at_once(
                pdf_bytes, file_path, lang, dpi, page_limit, n_pages, progress_prefix,
                min_native_chars,
            )
        except MemoryError:
            print(
                f"   ├─ ⚠️  {progress_prefix} OOM en mode 'toutes les pages' — "
                f"fallback page-par-page (mémoire réduite)"
            )
            rows = None

    if rows is None:
        rows = _ocr_rows_page_by_page(
            pdf_bytes, file_path, lang, dpi, page_limit, n_pages, progress_prefix,
            min_native_chars,
        )

    return pd.DataFrame(rows, columns=["file", "page", "metadata", "text"])


def build_ocr_extract(
    files: List[str],
    folder: "FolderAdapter",
    lang: str = "eng+fra+spa",
    dpi: int = 150,
    page_limit: Optional[int] = None,
    low_memory: bool = False,
    max_pages_all_at_once: int = 10,
    min_native_chars: int = MIN_NATIVE_CHARS,
) -> pd.DataFrame:
    """Run `pdf_to_page_dataframe` for a list of PDF files and concatenate.

    Non-PDF files are silently skipped (Excel files do not need OCR).

    Args:
        files: List of relative file paths inside the FolderAdapter.
        folder: FolderAdapter giving access to the source files.
        lang: Tesseract language string.
        dpi: Resolution for PDF → image conversion.
        page_limit: Max pages per file.
        low_memory: Force the page-by-page OCR path for every file (skip the
            faster all-at-once attempt). By default each file uses all-at-once
            and only falls back to page-by-page on ``MemoryError``.
        max_pages_all_at_once: Above this page count, a file uses page-by-page
            directly instead of rendering every page in memory.

    Returns:
        Concatenated DataFrame with columns: file, page, metadata, text.
        Empty DataFrame if no PDF files in the list.
    """
    pdf_files = [f for f in files if f.rsplit(".", 1)[-1].lower() == "pdf"]

    if not pdf_files:
        print("   ℹ️  No PDF files — skipping Tesseract step")
        return pd.DataFrame(columns=["file", "page", "metadata", "text"])

    print(f"   ├─ 🔧 Tesseract: processing {len(pdf_files)} PDF file(s)")
    frames = []
    n_total = len(pdf_files)
    n_ok = 0
    n_failed = 0
    n_pages_total = 0
    for idx, file_path in enumerate(pdf_files, start=1):
        prefix = f"[file {idx}/{n_total}]"
        try:
            df = pdf_to_page_dataframe(
                file_path=file_path,
                folder=folder,
                lang=lang,
                dpi=dpi,
                page_limit=page_limit,
                progress_prefix=prefix,
                low_memory=low_memory,
                max_pages_all_at_once=max_pages_all_at_once,
                min_native_chars=min_native_chars,
            )
            frames.append(df)
            n_ok += 1
            n_pages_total += len(df)
            print(
                f"   ✅ {prefix} done — {len(df)} page(s) | "
                f"progress: {n_ok} ok, {n_failed} failed, {n_total - idx} remaining"
            )
        except Exception as e:
            n_failed += 1
            print(
                f"   ⚠️  {prefix} Tesseract failed for [{file_path}]: {e} — skipping | "
                f"progress: {n_ok} ok, {n_failed} failed, {n_total - idx} remaining"
            )

    print(
        f"   🏁 Tesseract done — {n_ok}/{n_total} file(s) OCR'd "
        f"({n_failed} failed), {n_pages_total} page(s) total"
    )

    if not frames:
        return pd.DataFrame(columns=["file", "page", "metadata", "text"])

    return pd.concat(frames, ignore_index=True)
