"""VolumeAdapter: FolderAdapter implementation for Unity Catalog Volumes.

Implements the FolderAdapter Protocol defined in compute_generative_ai/base.py.
All file I/O is rooted under a base_path (e.g. /Volumes/catalog/schema/volume/).

Designed to be swappable: a future SharepointAdapter can replace this class
without any change to base.py or extractors.py.

Usage on Databricks:
    from helpers.volume_adapter import VolumeAdapter

    reports = VolumeAdapter("/Volumes/my_catalog/my_schema/maintainer_reports")
    data    = VolumeAdapter("/Volumes/my_catalog/my_schema/genai_data")
"""

import contextlib
import io
import os
from pathlib import Path
from typing import Iterator, List


class VolumeAdapter:
    """File-access adapter backed by a Unity Catalog Volume (local filesystem path).

    The Volume is mounted as a regular POSIX path on Databricks clusters, so
    standard `open()` calls work transparently.

    Args:
        base_path: Root directory of the volume (e.g. /Volumes/cat/schema/vol).
    """

    def __init__(self, base_path: str):
        self.base_path = Path(base_path)

    def _resolve(self, path: str) -> Path:
        """Resolve a relative path under the base directory."""
        full = self.base_path / path
        # Safety: ensure the resolved path stays within base_path
        full = full.resolve()
        if not str(full).startswith(str(self.base_path.resolve())):
            raise ValueError(
                f"Path traversal detected: '{path}' resolves outside base_path."
            )
        return full

    @contextlib.contextmanager
    def get_download_stream(self, path: str) -> Iterator[io.BufferedReader]:
        """Open a file for reading (binary mode).

        Compatible with the FolderAdapter Protocol.
        Use as a context manager::

            with adapter.get_download_stream("subdir/file.pdf") as f:
                data = f.read()
        """
        full_path = self._resolve(path)
        with open(full_path, "rb") as f:
            yield f

    @contextlib.contextmanager
    def get_writer(self, path: str) -> Iterator[io.BufferedWriter]:
        """Open a file for writing (binary mode), creating parent dirs as needed.

        Compatible with the FolderAdapter Protocol.
        Use as a context manager::

            with adapter.get_writer("subdir/output.pkl") as f:
                pickle.dump(data, f)
        """
        full_path = self._resolve(path)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        with open(full_path, "wb") as f:
            yield f

    def upload_data(self, path: str, data: bytes) -> None:
        """Write raw bytes to the given path, creating parent dirs as needed.

        Compatible with the FolderAdapter Protocol.

        Args:
            path: Relative path under base_path.
            data: Raw bytes to write.
        """
        full_path = self._resolve(path)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_bytes(data)

    def file_exists(self, path: str) -> bool:
        """Check whether a file exists at the given relative path."""
        try:
            return self._resolve(path).is_file()
        except ValueError:
            return False

    def list_files(self, subdir: str = "") -> List[str]:
        """List all files recursively under a subdirectory (relative paths).

        Args:
            subdir: Optional subdirectory relative to base_path. Defaults to root.

        Returns:
            List of relative file paths (using forward slashes).
        """
        root = self._resolve(subdir) if subdir else self.base_path
        if not root.is_dir():
            return []
        return [
            str(p.relative_to(self.base_path)).replace(os.sep, "/")
            for p in root.rglob("*")
            if p.is_file()
        ]
