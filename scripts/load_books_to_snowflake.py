#!/usr/bin/env python3
"""
Load PDF books into Snowflake for semantic search.

- Chunks PDFs with Unstructured.io best practice (by_title, max_characters, overlap).
- Inserts text into Snowflake staging; embeddings are computed in Snowflake via AI_EMBED
  so query-time semantic search uses the same model (snowflake-arctic-embed-m-v1.5).

Usage:
  Set env vars (see .env.example), then:
    python scripts/load_books_to_snowflake.py [--pdf-dir books_pdf_folder] [--resume]

Requires: BOOKS_DB.BOOKS.book_chunks_staging and book_embeddings (run scripts/schema.sql first).
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

# Add project root for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    from unstructured.partition.pdf import partition_pdf
except ImportError:
    partition_pdf = None

try:
    import snowflake.connector
except ImportError:
    snowflake = None

try:
    from scripts import snowflake_helper
except ImportError:
    import snowflake_helper


# --- Chunking: aligned with Snowflake snowflake-arctic-embed-m-v1.5 (512-token context) ---
# Unstructured best practice: by_title preserves section boundaries; soft max keeps chunks
# within embedding model limits; overlap improves retrieval at boundaries.
CHUNK_MAX_CHARS = 2000
CHUNK_NEW_AFTER_N_CHARS = 1800
CHUNK_OVERLAP = 300
CHUNK_COMBINE_UNDER_N_CHARS = 200

EMBED_MODEL = "snowflake-arctic-embed-m-v1.5"
DEFAULT_PDF_DIR = "books_pdf_folder"
STAGING_TABLE = "book_chunks_staging"
EMBEDDINGS_TABLE = "book_embeddings"


def _get_section_title(element) -> str:
    """Derive section title from chunk element (e.g. first Title in orig_elements)."""
    try:
        orig = getattr(element.metadata, "orig_elements", None) or []
        for e in orig:
            if getattr(e, "category", None) == "Title":
                t = (e.text or "").strip()
                if t:
                    return t[:500]
    except Exception:
        pass
    return ""


def _book_id_from_path(path: Path) -> str:
    """Stable book identifier from filename (no extension)."""
    return path.stem


def _author_from_metadata(elements) -> str:
    """Try to get author from first element metadata (e.g. PDF metadata)."""
    try:
        first_el = elements[0] if elements else None
        if first_el and hasattr(first_el, "metadata") and first_el.metadata:
            # Unstructured can put filename; author often in PDF metadata
            return getattr(first_el.metadata, "parent_id", "") or ""
    except Exception:
        pass
    return ""


def partition_and_chunk(pdf_path: Path) -> list[tuple[str, str, str, int, int]]:
    """
    Partition PDF and chunk with Unstructured best practice.
    Returns list of (section_title, content, page_number, chunk_index).
    """
    if partition_pdf is None:
        raise ImportError("unstructured is required. pip install unstructured[pdf]")

    elements = partition_pdf(
        filename=str(pdf_path),
        strategy="auto",
        infer_table_structure=False,
        chunking_strategy="by_title",
        max_characters=CHUNK_MAX_CHARS,
        new_after_n_chars=CHUNK_NEW_AFTER_N_CHARS,
        overlap=CHUNK_OVERLAP,
        combine_text_under_n_chars=CHUNK_COMBINE_UNDER_N_CHARS,
    )

    rows = []
    for idx, el in enumerate(elements):
        text = (getattr(el, "text", None) or "").strip()
        if not text:
            continue
        section_title = _get_section_title(el)
        page = getattr(getattr(el, "metadata", None), "page_number", None) or 0
        rows.append((section_title, text, page, idx))
    return rows


def load_one_book(
    pdf_path: Path,
    conn,
    book_id: str,
    author: str,
    resume_existing: bool,
) -> int:
    """Process one PDF and insert into staging, then run embedding insert. Returns chunks inserted."""
    chunks = partition_and_chunk(pdf_path)
    if not chunks:
        return 0

    if resume_existing:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM book_embeddings WHERE book_id = %s LIMIT 1",
                (book_id,),
            )
            if cur.fetchone():
                print(f"  (resume) skipping {book_id} (already in book_embeddings)")
                return 0

    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {STAGING_TABLE} WHERE book_id = %s", (book_id,))
        for idx, (section_title, content, page_number, _) in enumerate(chunks):
            cur.execute(
                f"""
                INSERT INTO {STAGING_TABLE}
                (book_id, author, section_title, content, page_number, chunk_index)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (book_id, author, section_title, content, page_number, idx),
            )

    # Compute embeddings in Snowflake and insert into book_embeddings (same model as query-time)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {EMBEDDINGS_TABLE}
            (book_id, author, section_title, content, page_number, chunk_index, vector)
            SELECT book_id, author, section_title, content, page_number, chunk_index,
                   AI_EMBED('{EMBED_MODEL}', content) AS vector
            FROM {STAGING_TABLE}
            WHERE book_id = %s
            """,
            (book_id,),
        )

    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {STAGING_TABLE} WHERE book_id = %s", (book_id,))

    return len(chunks)


def main() -> int:
    parser = argparse.ArgumentParser(description="Load PDF books into Snowflake for semantic search.")
    parser.add_argument(
        "--pdf-dir",
        default=DEFAULT_PDF_DIR,
        help=f"Directory containing PDFs (default: {DEFAULT_PDF_DIR})",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip books already present in book_embeddings",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parent.parent
    pdf_dir = root / args.pdf_dir
    if not pdf_dir.is_dir():
        print(f"Error: PDF directory not found: {pdf_dir}", file=sys.stderr)
        return 1

    if snowflake is None:
        print("Error: snowflake-connector-python is required.", file=sys.stderr)
        return 1

    try:
        __import__("unstructured")
        print("✓ Unstructured.io available")
    except ImportError:
        print("Error: unstructured is required (e.g. pip install 'unstructured[pdf]').", file=sys.stderr)
        return 1

    config = {
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE", "BOOKS_DB"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", "BOOKS"),
    }
    if not config.get("user") or not config.get("password") or not config.get("account"):
        print("Error: Set SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT (and optionally WAREHOUSE, DATABASE, SCHEMA).", file=sys.stderr)
        return 1

    pdfs = sorted(pdf_dir.glob("*.pdf"))
    if not pdfs:
        print(f"No PDFs found in {pdf_dir}")
        return 0

    print("Connecting to Snowflake...")
    print(f"Using database={config.get('database')}, schema={config.get('schema')}")
    if args.resume:
        print("Resume mode: skipping books already in book_embeddings.")
    print(f"Chunking: max={CHUNK_MAX_CHARS}, soft_max={CHUNK_NEW_AFTER_N_CHARS}, overlap={CHUNK_OVERLAP}")
    print(f"Books to process: {len(pdfs)}\n")

    total_chunks = 0
    with snowflake.connector.connect(**config) as conn:
        for pdf_path in pdfs:
            book_id = _book_id_from_path(pdf_path)
            author = ""  # optional: parse from PDF metadata if needed
            print(f"Processing: {pdf_path.name}")
            try:
                n = load_one_book(pdf_path, conn, book_id, author, args.resume)
                total_chunks += n
                if n:
                    print(f"  → {n} chunks loaded.")
            except Exception as e:
                print(f"  Error: {e}", file=sys.stderr)
                raise

    print(f"\nDone. Total chunks: {total_chunks}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
