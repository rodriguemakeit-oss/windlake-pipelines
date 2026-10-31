"""SharePointAdapter: FolderAdapter implementation for Microsoft SharePoint via Graph API.

Implements the FolderAdapter Protocol defined in compute_generative_ai/base.py.
All file I/O is rooted under a base_folder_path on the SharePoint site's default drive.

This adapter is a drop-in replacement for VolumeAdapter: both expose the same
interface (list_files, get_download_stream, upload_data, get_writer) so callers
do not need to know which backend is in use.

Usage on Databricks::

    from helpers.sharepoint_adapter import SharePointAdapter

    # Build from secrets dict (AWS Secrets Manager style)
    reports = SharePointAdapter.from_secrets(
        secrets=secrets,                          # dict with SHAREPOINT_* keys
        tenant_name="example.sharepoint.com",
        site_name="RGBU-EUROPE-TEAMOPERATION",
        folder_path="2_Operations/.../Monthly O&M Reporting",
    )

    # Identical interface as VolumeAdapter
    files = reports.list_files()                  # List[str] of relative paths
    with reports.get_download_stream("fr/vestas/2024/01-JAN/report.pdf") as f:
        data = f.read()
"""

import contextlib
import io
import time
from typing import Dict, Iterator, List, Optional
from urllib.parse import quote

import requests


class SharePointAdapter:
    """File-access adapter backed by a SharePoint drive via Microsoft Graph API.

    File paths are expressed relative to ``base_folder_path`` on the site's
    default drive, using forward slashes — exactly the same convention as
    ``VolumeAdapter``.

    Args:
        access_token:     A valid Microsoft Graph API OAuth 2.0 bearer token.
        site_id:          The fully-qualified SharePoint site ID (Graph format,
                          e.g. ``"example.sharepoint.com,<guid>,<guid>"``).
        base_folder_path: Path to the root folder on the drive, relative to the
                          drive root.  Must NOT start or end with ``/``.
                          Example: ``"2_Operations/04_WIND/Monthly O&M Reporting"``
    """

    _GRAPH_BASE = "https://graph.microsoft.com/v1.0"
    # Re-acquire the token this many seconds before its advertised expiry, so a
    # long-running job never sends a token that expires mid-request.
    _TOKEN_REFRESH_MARGIN_SEC = 300

    def __init__(
        self,
        access_token: str,
        site_id: str,
        base_folder_path: str,
        tenant_id: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        token_expires_at: Optional[float] = None,
    ):
        self._token = access_token
        self._site_id = site_id
        self._base_folder_path = base_folder_path.strip("/")
        # Credentials kept so the token can be silently re-acquired on long runs
        # (Graph tokens live ~1h; a 371-file backfill with sleeps runs much longer).
        self._tenant_id = tenant_id
        self._client_id = client_id
        self._client_secret = client_secret
        # Epoch seconds at which the current token must be refreshed. When the
        # credentials are unknown (token passed directly) refresh is disabled.
        self._token_expires_at = token_expires_at
        # Cache: relative path → temporary download URL populated by list_files()
        self._download_url_cache: Dict[str, str] = {}

    @classmethod
    def _acquire_token(cls, tenant_id: str, client_id: str, client_secret: str) -> tuple:
        """Acquire a Graph API token via client credentials.

        Returns:
            (access_token, expires_at_epoch_seconds)
        """
        token_resp = requests.post(
            f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": "https://graph.microsoft.com/.default",
            },
            timeout=30,
        )
        token_resp.raise_for_status()
        body = token_resp.json()
        expires_in = int(body.get("expires_in", 3599))
        return body["access_token"], time.time() + expires_in

    def _ensure_token(self) -> None:
        """Refresh the token if it is missing, expired, or about to expire.

        No-op when refresh credentials were not provided (token-only construction).
        """
        if not (self._tenant_id and self._client_id and self._client_secret):
            return
        if (
            self._token_expires_at is None
            or time.time() >= self._token_expires_at - self._TOKEN_REFRESH_MARGIN_SEC
        ):
            self._token, self._token_expires_at = self._acquire_token(
                self._tenant_id, self._client_id, self._client_secret
            )
            # Pre-signed download URLs are tied to the previous token window and
            # go stale alongside it — drop the cache so they are re-resolved.
            self._download_url_cache.clear()

    def _auth_headers(self) -> Dict[str, str]:
        """Return Authorization headers, refreshing the token first if needed."""
        self._ensure_token()
        return {"Authorization": f"Bearer {self._token}"}

    # ------------------------------------------------------------------
    # Factory helpers
    # ------------------------------------------------------------------

    @classmethod
    def from_secrets(
        cls,
        secrets: dict,
        tenant_name: str,
        site_name: str,
        folder_path: str,
    ) -> "SharePointAdapter":
        """Build a SharePointAdapter by authenticating with client credentials.

        Reads ``SHAREPOINT_TENANT_ID``, ``SHAREPOINT_CLIENT_ID`` and
        ``SHAREPOINT_CLIENT_SECRET`` from ``secrets``, acquires a Graph API
        token, and resolves the SharePoint ``site_id`` automatically.

        Args:
            secrets:     Dict containing the SharePoint credentials (e.g. from
                         AWS Secrets Manager).
            tenant_name: SharePoint tenant hostname, e.g. ``"example.sharepoint.com"``.
            site_name:   Site path segment, e.g. ``"RGBU-EUROPE-TEAMOPERATION"``.
            folder_path: Path inside the site's default drive to use as root,
                         e.g. ``"2_Operations/.../Monthly O&M Reporting"``.

        Returns:
            A fully initialised SharePointAdapter.
        """
        tenant_id = secrets["SHAREPOINT_TENANT_ID"]
        client_id = secrets["SHAREPOINT_CLIENT_ID"]
        client_secret = secrets["SHAREPOINT_CLIENT_SECRET"]

        # 1. Acquire OAuth token (with expiry, so it can be auto-refreshed)
        access_token, token_expires_at = cls._acquire_token(
            tenant_id, client_id, client_secret
        )

        # 2. Resolve site_id
        site_resp = requests.get(
            f"https://graph.microsoft.com/v1.0/sites/{tenant_name}:/sites/{site_name}",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=30,
        )
        site_resp.raise_for_status()
        site_id = site_resp.json()["id"]

        return cls(
            access_token=access_token,
            site_id=site_id,
            base_folder_path=folder_path,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            token_expires_at=token_expires_at,
        )

    # ------------------------------------------------------------------
    # FolderAdapter Protocol
    # ------------------------------------------------------------------

    def list_files(self, subdir: str = "") -> List[str]:
        """List all files recursively under ``base_folder_path`` (or a subdir).

        Handles Graph API pagination via ``@odata.nextLink`` automatically.
        Also populates the internal download-URL cache so that subsequent calls
        to ``get_download_stream()`` avoid an extra round-trip.

        Args:
            subdir: Optional subdirectory relative to ``base_folder_path``.
                    Defaults to root (all files under ``base_folder_path``).

        Returns:
            List of relative file paths using forward slashes, relative to
            ``base_folder_path``.  Same format as ``VolumeAdapter.list_files()``.
            Example: ``["france/vestas/2024/01-JAN/report.pdf"]``
        """
        folder = (
            f"{self._base_folder_path}/{subdir.strip('/')}"
            if subdir
            else self._base_folder_path
        )
        results: List[str] = []
        self._crawl_folder(folder, folder, results)
        return results

    def list_folder_files(self, subdir: str = "") -> List[str]:
        """Names of the files directly inside one folder (NOT recursive).

        Cheap alternative to ``list_files()`` when only one folder matters (the
        reports root holds thousands of files in sub-folders). Download URLs are
        cached under ``"<subdir>/<name>"`` like ``list_files()`` does.
        """
        sub = subdir.strip("/")
        folder = f"{self._base_folder_path}/{sub}" if sub else self._base_folder_path
        url: Optional[str] = (
            f"{self._GRAPH_BASE}/sites/{self._site_id}"
            f"/drive/root:/{quote(folder, safe='/')}:/children"
            f"?$select=name,folder,file,@microsoft.graph.downloadUrl"
        )
        names: List[str] = []
        while url:
            resp = requests.get(url, headers=self._auth_headers(), timeout=60)
            resp.raise_for_status()
            payload = resp.json()
            for item in payload.get("value", []):
                if "folder" in item:
                    continue
                names.append(item["name"])
                download_url = item.get("@microsoft.graph.downloadUrl")
                if download_url:
                    rel = f"{sub}/{item['name']}" if sub else item["name"]
                    self._download_url_cache[rel] = download_url
            url = payload.get("@odata.nextLink")
        return names

    @contextlib.contextmanager
    def get_download_stream(self, path: str) -> Iterator[io.RawIOBase]:
        """Download a file and expose it as a binary file-like object.

        Uses the cached download URL from ``list_files()`` when available,
        otherwise fetches the item metadata from Graph API.

        Args:
            path: Relative path under ``base_folder_path``, e.g.
                  ``"france/vestas/2024/01-JAN/report.pdf"``.

        Yields:
            A binary readable file-like object (``io.BytesIO``).
        """
        url = self._download_url_cache.get(path)
        if url is None:
            url = self._fetch_download_url(path)

        resp = requests.get(url, timeout=120)
        # A cached pre-signed download URL expires ~1h after list_files(); on a
        # long backfill it goes stale and returns 401/403. Re-resolve a fresh URL
        # (which refreshes the token if needed) once and retry.
        if resp.status_code in (401, 403):
            self._download_url_cache.pop(path, None)
            url = self._fetch_download_url(path)
            resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        yield io.BytesIO(resp.content)

    def upload_data(self, path: str, data: bytes) -> None:
        """Not supported — SharePointAdapter is read-only."""
        raise NotImplementedError(
            "SharePointAdapter is read-only. Use VolumeAdapter for write operations."
        )

    @contextlib.contextmanager
    def get_writer(self, path: str) -> Iterator[io.BytesIO]:
        """Not supported — SharePointAdapter is read-only."""
        raise NotImplementedError(
            "SharePointAdapter is read-only. Use VolumeAdapter for write operations."
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _crawl_folder(
        self,
        current_folder_path: str,
        base_folder_path: str,
        results: List[str],
    ) -> None:
        """Recursively crawl a folder and collect file paths.

        Args:
            current_folder_path: Absolute drive path of the folder to crawl.
            base_folder_path:    Root drive path used to compute relative paths.
            results:             List to append discovered file paths into.
        """
        encoded = quote(current_folder_path, safe="/")
        url: Optional[str] = (
            f"{self._GRAPH_BASE}/sites/{self._site_id}"
            f"/drive/root:/{encoded}:/children"
            f"?$select=name,folder,file,@microsoft.graph.downloadUrl,size"
        )

        while url:
            resp = requests.get(url, headers=self._auth_headers(), timeout=60)
            resp.raise_for_status()
            payload = resp.json()

            for item in payload.get("value", []):
                item_drive_path = f"{current_folder_path}/{item['name']}"

                if "folder" in item:
                    # Recurse into sub-folder
                    self._crawl_folder(item_drive_path, base_folder_path, results)
                else:
                    # Compute path relative to base_folder_path
                    relative = item_drive_path[len(base_folder_path):].lstrip("/")
                    results.append(relative)
                    # Cache the temporary download URL if provided by Graph
                    download_url = item.get("@microsoft.graph.downloadUrl")
                    if download_url:
                        self._download_url_cache[relative] = download_url

            # Follow pagination if present
            url = payload.get("@odata.nextLink")

    def _fetch_download_url(self, path: str) -> str:
        """Fetch the download URL for a single item (cache miss fallback).

        Args:
            path: Relative path under ``base_folder_path``.

        Returns:
            Temporary download URL string.
        """
        full_path = f"{self._base_folder_path}/{path}"
        encoded = quote(full_path, safe="/")
        resp = requests.get(
            f"{self._GRAPH_BASE}/sites/{self._site_id}"
            f"/drive/root:/{encoded}"
            f"?$select=@microsoft.graph.downloadUrl",
            headers=self._auth_headers(),
            timeout=30,
        )
        resp.raise_for_status()
        url = resp.json().get("@microsoft.graph.downloadUrl")
        if not url:
            raise FileNotFoundError(
                f"Could not retrieve download URL for '{path}'. "
                "Ensure the app has Files.Read permissions on the site."
            )
        self._download_url_cache[path] = url
        return url
