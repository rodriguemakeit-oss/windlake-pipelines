"""Utilities for generative AI computations.

Databricks-compatible version: no dataiku dependency.
- get_prompt_yaml: loads prompts.yaml from a path relative to this module
- file_exists_in_folder: uses FolderAdapter protocol (see base.py)
"""

import os
import yaml
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from compute_generative_ai.base import FolderAdapter


def get_prompt_yaml(prompts_yaml_path: str = None) -> dict:
    """Load prompts.yaml config.

    Args:
        prompts_yaml_path: Absolute or relative path to prompts.yaml.
            Defaults to the bundled resources/prompts.yaml next to this module.

    Returns:
        dict: Parsed YAML content.
    """
    if prompts_yaml_path is None:
        prompts_yaml_path = os.path.join(
            os.path.dirname(__file__), "resources", "prompts.yaml"
        )
    with open(prompts_yaml_path, "r") as f:
        return yaml.safe_load(f)


def file_exists_in_folder(folder: "FolderAdapter", file_name: str) -> bool:
    """Check if a file exists in the given folder adapter.

    Args:
        folder: Any object implementing the FolderAdapter protocol.
        file_name: Relative path of the file to check.

    Returns:
        bool: True if the file exists, False otherwise.
    """
    print(f"   ├─ 🔍 Checking if file exists: {file_name}")
    try:
        with folder.get_download_stream(file_name):
            pass
        return True
    except Exception:
        return False
