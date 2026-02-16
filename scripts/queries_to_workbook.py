#!/usr/bin/env python3
"""
Convert docs/queries.md to a Snowflake-compatible Jupyter workbook (docs/workbook.ipynb).
Run from repo root: python scripts/queries_to_workbook.py

Snowflake Notebooks import .ipynb; use SQL cells for the queries and Markdown for sections.
"""

import json
import re
import os

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
QUERIES_MD = os.path.join(REPO_ROOT, "docs", "queries.md")
WORKBOOK_IPYNB = os.path.join(REPO_ROOT, "docs", "workbook.ipynb")


def md_to_cells(content: str) -> list:
    """Parse queries.md into a list of { "type": "markdown"|"sql", "source": str }."""
    cells = []
    # Split by ```sql ... ``` blocks (non-greedy)
    pattern = re.compile(r"```sql\s*\n(.*?)```", re.DOTALL)
    parts = pattern.split(content)
    # parts[0] = intro markdown, parts[1] = first sql, parts[2] = markdown between 1st and 2nd sql, ...
    if not parts:
        return cells
    # Intro (before first ```sql)
    intro = parts[0].strip()
    if intro:
        cells.append({"type": "markdown", "source": intro})
    i = 1
    while i < len(parts):
        sql_block = parts[i].strip()
        if sql_block:
            cells.append({"type": "sql", "source": sql_block})
        i += 1
        if i < len(parts):
            md_block = parts[i].strip()
            # Drop lone --- and empty
            md_block = re.sub(r"^---\s*$", "", md_block, flags=re.MULTILINE).strip()
            if md_block:
                cells.append({"type": "markdown", "source": md_block})
        i += 1
    return cells


def build_notebook(cells: list) -> dict:
    """Build Jupyter nbformat v4 notebook structure. Snowflake uses SQL/metadata for cell type."""
    nb_cells = []
    # Optional: one setup cell so workbook runs in BOOKS_DB.BOOKS
    nb_cells.append({
        "cell_type": "code",
        "metadata": {"snowflake": {"language": "sql"}},
        "source": ["USE DATABASE BOOKS_DB;\n", "USE SCHEMA BOOKS;\n"],
        "outputs": [],
        "execution_count": None,
    })
    for c in cells:
        src = c["source"]
        if isinstance(src, str):
            src = [line if line.endswith("\n") else line + "\n" for line in src.split("\n")]
            if src and not src[-1].endswith("\n"):
                src[-1] = src[-1].rstrip("\n") + "\n"
        if c["type"] == "markdown":
            if isinstance(src, str):
                src = [line + "\n" for line in src.split("\n")]
                if src and not src[-1].endswith("\n"):
                    src[-1] = src[-1].rstrip("\n") + "\n"
            nb_cells.append({
                "cell_type": "markdown",
                "metadata": {},
                "source": src,
            })
        else:
            if isinstance(src, str):
                src = [line + "\n" for line in src.split("\n")]
                if src and not src[-1].endswith("\n"):
                    src[-1] = src[-1].rstrip("\n") + "\n"
            nb_cells.append({
                "cell_type": "code",
                "metadata": {"snowflake": {"language": "sql"}},
                "source": src,
                "outputs": [],
                "execution_count": None,
            })
    return {
        "nbformat": 4,
        "nbformat_minor": 4,
        "metadata": {
            "kernelspec": {"display_name": "Snowflake SQL", "language": "sql", "name": "sql"},
            "language_info": {"name": "sql"},
        },
        "cells": nb_cells,
    }


def main():
    with open(QUERIES_MD, "r", encoding="utf-8") as f:
        content = f.read()
    cells = md_to_cells(content)
    nb = build_notebook(cells)
    with open(WORKBOOK_IPYNB, "w", encoding="utf-8") as f:
        json.dump(nb, f, indent=1, ensure_ascii=False)
    print(f"Wrote {WORKBOOK_IPYNB} ({len(nb['cells'])} cells)")


if __name__ == "__main__":
    main()
