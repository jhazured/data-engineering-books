#!/usr/bin/env python3
"""
Load PDF books into Snowflake for semantic search.

- Chunks PDFs with Unstructured.io best practice (by_title, max_characters, overlap).
- Inserts text into Snowflake staging; embeddings are computed in Snowflake via AI_EMBED
  so query-time semantic search uses the same model (snowflake-arctic-embed-m-v1.5).

Usage:
  Set env vars (see .env.example), then:
    python scripts/load_books_to_snowflake.py [--pdf-dir DIR] [--mode incremental|full_reload] [--force]
  Optional env: CHUNK_MAX_CHARS (2000), CHUNK_OVERLAP (300), CHUNK_NEW_AFTER_N_CHARS, CHUNK_COMBINE_UNDER_N_CHARS.

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
# Overlap preserves context at chunk boundaries (standard RAG practice). Overridable via env.
def _chunk_config():
    max_c = int(os.getenv("CHUNK_MAX_CHARS", "2000"))
    overlap = int(os.getenv("CHUNK_OVERLAP", "300"))
    overlap = min(max(0, overlap), max_c - 1)
    new_after = int(os.getenv("CHUNK_NEW_AFTER_N_CHARS", str(max_c - 200)))
    new_after = min(max(1, new_after), max_c)
    combine = int(os.getenv("CHUNK_COMBINE_UNDER_N_CHARS", "200"))
    return max_c, new_after, overlap, min(combine, new_after)

EMBED_MODEL = "snowflake-arctic-embed-m-v1.5"
DEFAULT_PDF_DIR = "books_pdf_folder"
STAGING_TABLE = "book_chunks_staging"
EMBEDDINGS_TABLE = "book_embeddings"


# Patterns that often indicate a chapter/section heading (for fallback when Unstructured has no Title).
_HEADING_PATTERN = re.compile(
    r"^(?:(?:Chapter|Part|Section|Appendix)\s*(?:\d+[\.:]?|\d*[IVXLCDM]+\.?)?\s*[-–:]?\s*)?(.{1,200})$",
    re.IGNORECASE,
)


def _looks_like_heading(line: str) -> bool:
    """True if the first line of a chunk looks like a section heading (short, no sentence end)."""
    line = (line or "").strip()
    if not line or len(line) > 200:
        return False
    # Often headings don't end with . ! ?
    if line.endswith((".", "!", "?")):
        return False
    # Numbered chapter/part/section
    if _HEADING_PATTERN.match(line):
        return True
    # Short all-caps or title-case line
    if len(line) <= 80 and (line.isupper() or line.istitle()):
        return True
    return False


def _get_section_title(element) -> str:
    """Derive section title from chunk: Unstructured Title in orig_elements, or first-line fallback."""
    # 1) From Unstructured's Title elements in the chunk's orig_elements
    try:
        orig = getattr(element.metadata, "orig_elements", None) or []
        for e in orig:
            if getattr(e, "category", None) == "Title":
                t = (e.text or "").strip()
                if t:
                    return t[:500]
    except Exception:
        pass

    # 2) Fallback: first line of chunk if it looks like a heading (chapter/section)
    text = (getattr(element, "text", None) or "").strip()
    first_line = text.split("\n")[0].strip() if text else ""
    if first_line and _looks_like_heading(first_line):
        return first_line[:500]
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
    Partition PDF and chunk with Unstructured best practice (by_title + overlap).
    Returns list of (section_title, content, page_number, chunk_index).
    """
    if partition_pdf is None:
        raise ImportError("unstructured is required. pip install unstructured[pdf]")
    max_characters, new_after_n_chars, overlap, combine_text_under_n_chars = _chunk_config()
    elements = partition_pdf(
        filename=str(pdf_path),
        strategy="auto",
        infer_table_structure=False,
        chunking_strategy="by_title",
        max_characters=max_characters,
        new_after_n_chars=new_after_n_chars,
        overlap=overlap,
        combine_text_under_n_chars=combine_text_under_n_chars,
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
    mode: str,
) -> int:
    """Process one PDF and insert into staging, then run embedding insert. Returns chunks inserted.
    mode: 'incremental' = skip if book already in book_embeddings; 'full_reload' = delete then load.
    """
    chunks = partition_and_chunk(pdf_path)
    if not chunks:
        return 0

    if mode == "incremental":
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM book_embeddings WHERE book_id = %s LIMIT 1",
                (book_id,),
            )
            if cur.fetchone():
                print(f"  (incremental) skipping {book_id} (already in book_embeddings)")
                return 0
    else:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {EMBEDDINGS_TABLE} WHERE book_id = %s", (book_id,))

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
        "--mode",
        choices=("incremental", "full_reload"),
        default="incremental",
        help="incremental: skip books already in book_embeddings; full_reload: delete then re-load each book",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Required for --mode full_reload (confirms destructive re-load)",
    )
    args = parser.parse_args()

    if args.mode == "full_reload" and not args.force:
        print("Error: --mode full_reload is destructive. Add --force to confirm.", file=sys.stderr)
        return 1

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
        **snowflake_helper._get_config(),
        "database": os.getenv("SNOWFLAKE_DATABASE", "BOOKS_DB"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", "BOOKS"),
    }
    if not config.get("user") or not config.get("password") or not config.get("account"):
        print("Error: Set SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT (see .env.example).", file=sys.stderr)
        return 1
    if config.get("user") == "YOUR_USER" or config.get("password") == "YOUR_PASSWORD" or config.get("account") == "YOUR_ACCOUNT":
        print("Error: Replace placeholder SNOWFLAKE_* values in .env with your credentials.", file=sys.stderr)
        return 1

    pdfs = sorted(pdf_dir.glob("*.pdf"))
    if not pdfs:
        print(f"No PDFs found in {pdf_dir}")
        return 0

    print("Connecting to Snowflake...")
    print(f"Using database={config.get('database')}, schema={config.get('schema')}")
    if args.mode == "incremental":
        print("Mode: incremental (skipping books already in book_embeddings).")
    else:
        print("Mode: full_reload (re-loading each book; existing chunks for that book are deleted).")
    max_c, new_after, overlap, _ = _chunk_config()
    print(f"Chunking: max={max_c}, soft_max={new_after}, overlap={overlap}")
    print(f"Books to process: {len(pdfs)}\n")

    total_chunks = 0
    with snowflake.connector.connect(**config) as conn:
        for pdf_path in pdfs:
            book_id = _book_id_from_path(pdf_path)
            author = ""  # optional: parse from PDF metadata if needed
            print(f"Processing: {pdf_path.name}")
            try:
                n = load_one_book(pdf_path, conn, book_id, author, args.mode)
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
