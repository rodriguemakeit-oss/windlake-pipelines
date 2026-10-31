from typing import List
from azure.core.credentials import AzureKeyCredential
from azure.core.exceptions import HttpResponseError
from azure.ai.documentintelligence.models import (
    DocumentContentFormat as ContentFormat,
    AnalyzeDocumentRequest,
    AnalyzeResult,
)
from azure.ai.documentintelligence._client import DocumentIntelligenceClient


def get_document_intelligence_client(
    azure_di_endpoint: str, azure_di_key: str
) -> DocumentIntelligenceClient:
    """
    Creates and returns an instance of DocumentIntelligenceClient.
    This function initializes a DocumentIntelligenceClient using the endpoint and key
    specified in the environment variables 'AZURE_DI_ENDPOINT' and 'AZURE_DI_KEY'.
    Args:
        azure_di_endpoint (str): The endpoint URL for the Azure Document Intelligence service.
        azure_di_key (str): The API key for authenticating with the Azure Document Intelligence service.
    Returns:
        DocumentIntelligenceClient: An instance of DocumentIntelligenceClient initialized with the specified endpoint and credential.
    """
    print("   ├─ 🔧 Creating Document Intelligence client")
    return DocumentIntelligenceClient(
        endpoint=azure_di_endpoint,
        credential=AzureKeyCredential(azure_di_key),
    )


def analyze_report(
    doc: bytes,
    ocr_client: DocumentIntelligenceClient = None,
    model: str = "prebuilt-layout",
    pages: List[int] = None,
    azure_di_endpoint: str = None,
    azure_di_key: str = None,
) -> AnalyzeResult:
    """
    Analyzes a document using the specified model and returns the result.

    Args:
        doc: A file-like object containing the document to be analyzed.
        client (DocumentIntelligenceClient, optional): An instance of DocumentIntelligenceClient. If not provided, a new client will be created.
        model (str, optional): The model to use for analysis. Defaults to "prebuilt-layout".
        pages (list, optional): A list of page numbers to analyze. If None, all pages will be analyzed.

    Returns:
        The result of the document analysis.
    """
    print(f"      ├─ 🔍 Analyzing report with Document Intelligence")
    if not ocr_client:
        ocr_client = get_document_intelligence_client(
            azure_di_endpoint=azure_di_endpoint, azure_di_key=azure_di_key
        )

    # Read the document bytes once so we can safely retry (the stream would be
    # exhausted after the first read).
    content = doc.read()

    # Empty source file (0-byte placeholder on SharePoint). Document Intelligence
    # would answer with a confusing "(InvalidRequest) InvalidContent — the file is
    # corrupted or format is unsupported"; raise a clear, classifiable error
    # instead so these show up as "empty file (0 bytes)" rather than "DI invalid".
    if not content:
        raise ValueError("empty file (0 bytes) — nothing to analyze")

    def _analyze(page_arg):
        poller = ocr_client.begin_analyze_document(
            model,
            AnalyzeDocumentRequest(bytes_source=content),
            output_content_format=ContentFormat.MARKDOWN,
            pages=page_arg,
        )
        return poller.result()

    try:
        return _analyze(pages)
    except HttpResponseError as exc:
        msg = str(exc).lower()
        # Corrupt file / not a real PDF (a non-PDF renamed .pdf, a truncated
        # export…): Document Intelligence answers "InvalidContent — the file is
        # corrupted or format is unsupported" and PyMuPDF fails too. Raise a
        # clear, classifiable error instead of the opaque "invalid request".
        if "invalidcontent" in msg or "corrupted" in msg or "unsupported" in msg:
            raise ValueError(
                "corrupt or unsupported file — not a readable PDF"
            ) from exc
        # A common cause of "invalid request" is a page range computed from the
        # OCR pre-pass that exceeds the document's real page count (observed on
        # short DWT "Appendix 1 - Technical Availability" PDFs). Fall back to
        # analyzing the whole document instead of failing the file.
        if pages and "invalid" in msg:
            print(
                f"      ├─ ⚠️  DI invalid request with pages={pages} — "
                f"retrying on the full document"
            )
            return _analyze(None)
        raise
