"""
Tests for chunk config and section-title fallback logic (no Snowflake or PDFs required).
Path setup is in tests/conftest.py.
"""
import os
import pytest
from pathlib import Path

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
    assert _looks_like_heading("CHAPTER FIVE") is True  # all-caps
    assert _looks_like_heading("This is a long sentence that goes on and on and should not be treated as a heading.") is False
    assert _looks_like_heading("Short.") is False  # ends with period
    assert _looks_like_heading("") is False


def test_book_id_from_path():
    """Stable book ID is filename without extension."""
    from scripts.load_books_to_snowflake import _book_id_from_path
    assert _book_id_from_path(Path("designing-data-intensive-applications.pdf")) == "designing-data-intensive-applications"
    assert _book_id_from_path(Path("/some/dir/book.pdf")) == "book"


def test_get_section_title_from_metadata():
    """Section title comes from Unstructured Title in orig_elements when present (not first-line fallback)."""
    from types import SimpleNamespace
    from scripts.load_books_to_snowflake import _get_section_title
    title_el = SimpleNamespace(category="Title", text="Replication")
    meta = SimpleNamespace(orig_elements=[title_el])
    el = SimpleNamespace(metadata=meta, text="Some body.")  # first line not a heading; forces orig_elements path
    assert _get_section_title(el) == "Replication"


def test_get_section_title_fallback_heading():
    """Section title fallback: first line used when it looks like a heading."""
    from scripts.load_books_to_snowflake import _get_section_title
    meta = type("M", (), {"orig_elements": []})()
    el = type("E", (), {"metadata": meta, "text": "Chapter 5: Replication\n\nContent here."})()
    assert _get_section_title(el) == "Chapter 5: Replication"


def test_get_section_title_fallback_empty():
    """Section title fallback: empty when no Title and first line not a heading."""
    from scripts.load_books_to_snowflake import _get_section_title
    meta = type("M", (), {"orig_elements": []})()
    el = type("E", (), {"metadata": meta, "text": "This is normal paragraph text."})()
    assert _get_section_title(el) == ""
