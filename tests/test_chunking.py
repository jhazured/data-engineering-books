"""
Tests for chunk config and section-title fallback logic (no Snowflake or PDFs required).
"""
import os
import pytest

# Import chunk config and section-title helpers from loader (without running partition_pdf)
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# We test the loader's _chunk_config and _get_section_title / _looks_like_heading by
# importing after mocking or by testing the behaviour via the public partition_and_chunk
# contract. To avoid requiring unstructured at test time, we test _chunk_config only
# (pure env reading) and the heading regex.


def test_chunk_config_defaults(monkeypatch):
    """CHUNK_MAX_CHARS and CHUNK_OVERLAP default to 2000 and 300 when env is unset."""
    from scripts.load_books_to_snowflake import _chunk_config
    monkeypatch.delenv("CHUNK_MAX_CHARS", raising=False)
    monkeypatch.delenv("CHUNK_OVERLAP", raising=False)
    monkeypatch.delenv("CHUNK_NEW_AFTER_N_CHARS", raising=False)
    monkeypatch.delenv("CHUNK_COMBINE_UNDER_N_CHARS", raising=False)
    max_c, new_after, overlap, combine = _chunk_config()
    assert max_c == 2000
    assert overlap == 300
    assert 0 <= overlap < max_c
    assert 1 <= new_after <= max_c


def test_chunk_config_env_override(monkeypatch):
    """Chunk config can be overridden via env."""
    from scripts.load_books_to_snowflake import _chunk_config
    monkeypatch.setenv("CHUNK_MAX_CHARS", "1000")
    monkeypatch.setenv("CHUNK_OVERLAP", "200")
    max_c, new_after, overlap, combine = _chunk_config()
    assert max_c == 1000
    assert overlap == 200


def test_chunk_config_overlap_capped(monkeypatch):
    """Overlap is capped to max_chars - 1."""
    from scripts.load_books_to_snowflake import _chunk_config
    monkeypatch.setenv("CHUNK_MAX_CHARS", "500")
    monkeypatch.setenv("CHUNK_OVERLAP", "600")
    max_c, _, overlap, _ = _chunk_config()
    assert overlap <= max_c - 1


def test_looks_like_heading():
    """First-line heading detection: chapter/part patterns and short lines."""
    from scripts.load_books_to_snowflake import _looks_like_heading
    assert _looks_like_heading("Chapter 5: Replication") is True
    assert _looks_like_heading("Part I. Foundations") is True
    assert _looks_like_heading("This is a long sentence that goes on and on and should not be treated as a heading.") is False
    assert _looks_like_heading("Short.") is False  # ends with period
    assert _looks_like_heading("") is False
