"""Convert Databricks .ipynb notebooks to Databricks .py source format.

Databricks .py format spec:
- File starts with: `# Databricks notebook source`
- Cells separated by: `\n# COMMAND ----------\n\n`
- Markdown cells: `# MAGIC %md\n# MAGIC <line1>\n# MAGIC <line2>\n...`
- Magic in code cells (lines starting with `%run`, `%sql`, `%pip`, `%fs`, `%sh`, `%md`):
  rendered as `# MAGIC %xxx\n# MAGIC <continuation>` lines.
- Otherwise code cell content is kept verbatim.

Usage:
    python3 tools/convert_ipynb_to_databricks_py.py SRC_DIR [SRC_DIR ...]

Operates in-place: writes <name>.py next to each <name>.ipynb and removes the .ipynb.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

MAGICS = ("%run", "%sql", "%pip", "%fs", "%sh", "%md", "%scala", "%r")


def _lines(source) -> list[str]:
    if isinstance(source, list):
        text = "".join(source)
    else:
        text = source
    # Strip trailing newline; we manage line endings ourselves.
    return text.split("\n")


def _render_magic_block(lines: list[str]) -> str:
    """Render a block where every line is prefixed with `# MAGIC `."""
    return "\n".join(("# MAGIC " + ln) if ln else "# MAGIC" for ln in lines)


def cell_to_py(cell: dict) -> str | None:
    ctype = cell.get("cell_type")
    src_lines = _lines(cell.get("source", []))
    # Drop trailing empty lines
    while src_lines and src_lines[-1] == "":
        src_lines.pop()
    if not src_lines:
        return None

    if ctype == "markdown":
        return _render_magic_block(["%md"] + src_lines)

    if ctype == "code":
        first = src_lines[0].lstrip()
        if any(first.startswith(m) for m in MAGICS):
            # Entire cell is a magic
            return _render_magic_block(src_lines)
        return "\n".join(src_lines)

    # raw / unknown → skip
    return None


def convert(ipynb_path: Path) -> Path:
    nb = json.loads(ipynb_path.read_text())
    rendered_cells: list[str] = []
    for cell in nb.get("cells", []):
        rendered = cell_to_py(cell)
        if rendered is None:
            continue
        rendered_cells.append(rendered)
    sep = "\n\n# COMMAND ----------\n\n"
    if rendered_cells:
        body = "# Databricks notebook source\n" + rendered_cells[0]
        if len(rendered_cells) > 1:
            body += sep + sep.join(rendered_cells[1:])
        body += "\n"
    else:
        body = "# Databricks notebook source\n"
    py_path = ipynb_path.with_suffix(".py")
    py_path.write_text(body)
    return py_path


def main(roots: list[str]) -> None:
    targets: list[Path] = []
    for root in roots:
        p = Path(root)
        if p.is_file() and p.suffix == ".ipynb":
            targets.append(p)
        else:
            targets.extend(sorted(p.rglob("*.ipynb")))
    for ipynb in targets:
        py = convert(ipynb)
        print(f"  {ipynb} -> {py}")
        ipynb.unlink()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    main(sys.argv[1:])
