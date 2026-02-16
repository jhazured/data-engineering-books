"""
Production-grade PDF loader using Unstructured.io for semantic chunking.

Processes and uploads each book to Snowflake one at a time (resumable).
Use --resume to skip books already in BOOKS and continue from the next.

Requirements:
    pip install unstructured[pdf] snowflake-connector-python pandas python-dotenv

Optional for better quality (recommended):
    pip install "unstructured[local-inference]"
    
    This requires:
    - tesseract (for OCR)
    - poppler (for PDF processing)
    
    Installation:
    - macOS: brew install tesseract poppler
    - Ubuntu: apt-get install tesseract-ocr poppler-utils
    - Windows: see https://unstructured-io.github.io/unstructured/installing.html
"""

import os
import sys
import contextlib
import io
import snowflake.connector
import snowflake.connector.errors as snowflake_errors
import pandas as pd

INSERT_BATCH_SIZE = 500


def _log(msg: str) -> None:
    """Print and flush so progress appears immediately."""
    print(msg)
    sys.stdout.flush()


# Ensure we can resolve snowflake_helper for config
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPT_DIR)
try:
    from scripts import snowflake_helper
except ImportError:
    import sys
    if _REPO_ROOT not in sys.path:
        sys.path.insert(0, _REPO_ROOT)
    if _SCRIPT_DIR not in sys.path:
        sys.path.insert(0, _SCRIPT_DIR)
    import snowflake_helper

# Load .env if available
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(_REPO_ROOT, ".env"))
except ImportError:
    pass

PDF_FOLDER = os.path.join(_REPO_ROOT, "books_pdf_folder")

# UNSTRUCTURED CHUNKING PARAMETERS
# These are optimized for technical books with clear section structure
CHUNK_MAX_CHARACTERS = 2000  # Hard maximum
CHUNK_NEW_AFTER = 1800  # Soft maximum - start new chunk after this many chars
CHUNK_COMBINE_UNDER = 500  # Combine small elements under this size
CHUNK_OVERLAP = 300  # Overlap between chunks for context preservation

# Snowflake Cortex embedding model
EMBEDDING_MODEL = "snowflake-arctic-embed-m-v1.5"


def get_snowflake_config():
    """Unify config with snowflake_helper (env vars or placeholders)."""
    return snowflake_helper._get_config()


def get_pdf_metadata(pdf_path):
    """Extract author and publication year from PDF metadata using pypdf."""
    author = "Unknown"
    publication_year = None
    
    try:
        from pypdf import PdfReader
        with open(pdf_path, 'rb') as f:
            pdf = PdfReader(f)
            meta = pdf.metadata or {}
            
            # Try different metadata fields
            author = meta.get('/Author') or meta.get('Author') or author
            if isinstance(author, bytes):
                author = author.decode("utf-8", errors="replace")
            
            # Extract year from creation date
            creation = meta.get('/CreationDate') or meta.get('CreationDate') or ""
            if isinstance(creation, str) and creation.startswith("D:") and len(creation) >= 6:
                year_str = creation[2:6]
                if year_str.isdigit():
                    publication_year = int(year_str)
    except Exception as e:
        _log(f"    Warning: Could not extract metadata: {e}")
    
    return author, publication_year


def partition_and_chunk_pdf(pdf_path):
    """
    Use Unstructured.io to partition PDF and chunk by title.
    
    This approach:
    1. Detects document structure (titles, paragraphs, lists, tables)
    2. Preserves section boundaries when chunking
    3. Combines related elements intelligently
    4. Maintains hierarchical context
    """
    try:
        from unstructured.partition.pdf import partition_pdf
        from unstructured.chunking.title import chunk_by_title
    except ImportError as e:
        _log("ERROR: unstructured library not installed properly.")
        _log("Install with: pip install 'unstructured[pdf]'")
        _log("For better quality, also install: pip install 'unstructured[local-inference]'")
        raise e
    
    _log(f"    Partitioning PDF (detecting structure)...")
    
    # Partition the PDF - this detects titles, paragraphs, lists, tables, etc.
    # strategy="auto" will use the best available method
    # For production, consider "hi_res" with local-inference installed
    # Suppress PDF library stderr (e.g. "Cannot set non-stroke color") during partition
    def _run_partition(**kwargs):
        with open(os.devnull, "w") as devnull:
            with contextlib.redirect_stderr(devnull):
                return partition_pdf(filename=pdf_path, **kwargs)

    try:
        elements = _run_partition(
            strategy="auto",
            infer_table_structure=True,
            include_page_breaks=False,
        )
        _log(f"    Detected {len(elements)} document elements")
    except Exception as e:
        err_msg = str(e).lower()
        if "tesseract" in err_msg or "hi_res" in err_msg:
            _log(f"    Note: hi_res unavailable (tesseract not installed). Using fast strategy.")
        else:
            _log(f"    Partitioning issue: {e}")
            _log(f"    Falling back to fast strategy...")
        elements = _run_partition(strategy="fast", include_page_breaks=False)
        _log(f"    Detected {len(elements)} document elements (fast mode)")
    
    # Chunk by title - this preserves section boundaries
    _log(f"    Chunking by title (preserving section structure)...")
    chunks = chunk_by_title(
        elements=elements,
        max_characters=CHUNK_MAX_CHARACTERS,  # Hard max
        new_after_n_chars=CHUNK_NEW_AFTER,  # Soft max - prefer breaking here
        combine_text_under_n_chars=CHUNK_COMBINE_UNDER,  # Combine tiny elements
        overlap=CHUNK_OVERLAP,  # Overlap for context
        overlap_all=True,  # Apply overlap between all chunks
    )
    
    _log(f"    Created {len(chunks)} semantic chunks")
    
    # Extract chunk data
    chunk_data = []
    for idx, chunk in enumerate(chunks):
        # Get the text content
        content = chunk.text
        
        # Get metadata
        metadata = chunk.metadata
        
        # Extract section title from metadata if available
        # Unstructured metadata can be a dict or object; support both
        def _get(obj, key, default=None):
            if obj is None:
                return default
            if isinstance(obj, dict):
                return obj.get(key, default)
            return getattr(obj, key, default)

        section_title = None
        cat = _get(metadata, 'category') or getattr(chunk, 'category', None)
        if cat == 'Title':
            section_title = (content or '')[:120]
        if not section_title:
            parent_id = _get(metadata, 'parent_id')
            if parent_id:
                for elem in elements:
                    eid = _get(getattr(elem, 'metadata', None), 'element_id')
                    if eid == parent_id:
                        elem_cat = getattr(elem, 'category', None) or _get(getattr(elem, 'metadata', None), 'category')
                        if elem_cat == 'Title':
                            section_title = (getattr(elem, 'text', None) or '')[:120]
                            break
        
        chunk_data.append({
            'chunk_id': idx,
            'content': content,
            'section_title': section_title,
        })
    
    return chunk_data


def get_existing_book_ids(conn):
    """Return set of book_id already in BOOKS table (for resume)."""
    try:
        with conn.cursor() as cs:
            cs.execute("SELECT DISTINCT book_id FROM books")
            return {row[0] for row in cs.fetchall()}
    except snowflake_errors.ProgrammingError:
        return set()


def insert_book_chunks(conn, book_id, author, publication_year, chunks):
    """Insert one book's chunks into BOOKS in batches."""
    cols = ["book_id", "chunk_id", "content", "author", "publication_year", "section_title"]
    rows = [
        (book_id, c["chunk_id"], c["content"], author, publication_year, c["section_title"])
        for c in chunks
    ]
    with conn.cursor() as cs:
        for i in range(0, len(rows), INSERT_BATCH_SIZE):
            batch = rows[i : i + INSERT_BATCH_SIZE]
            cs.executemany(
                "INSERT INTO books (book_id, chunk_id, content, author, publication_year, section_title) VALUES (%s, %s, %s, %s, %s, %s)",
                batch,
            )
    _log(f"    ✓ Uploaded {len(rows)} rows to BOOKS")


def main():
    resume = "--resume" in sys.argv or "-r" in sys.argv
    _log("=" * 80)
    _log("PDF LOADER - Using Unstructured.io for semantic chunking")
    _log("Processing and uploading each book individually (resumable).")
    if resume:
        _log("Resume mode: skipping books already in BOOKS.")
    _log("=" * 80)
    _log("")
    
    try:
        import unstructured
        _log(f"✓ Unstructured.io version: {getattr(unstructured, '__version__', '?')}")
    except ImportError:
        _log("✗ ERROR: unstructured library not found")
        _log("  Install with: pip install 'unstructured[pdf]'")
        return
    
    _log("")
    _log("Connecting to Snowflake...")
    config = get_snowflake_config()
    database = (config.get("database") or "").strip().upper()
    schema = (config.get("schema") or "PUBLIC").strip().upper()
    if not database or not schema:
        _log("ERROR: SNOWFLAKE_DATABASE and SNOWFLAKE_SCHEMA must be set in .env")
        return
    if not all(c.isalnum() or c == "_" for c in database + schema):
        _log("ERROR: SNOWFLAKE_DATABASE and SNOWFLAKE_SCHEMA must be alphanumeric (or underscore)")
        return
    
    conn = snowflake.connector.connect(**config)
    try:
        with conn.cursor() as cs:
            cs.execute(f"USE DATABASE {database}")
            cs.execute(f"USE SCHEMA {schema}")
        _log(f"Using database={database}, schema={schema}")
        _log("")
        
        books_schema_sql = """
            CREATE OR REPLACE TABLE books (
                book_id STRING,
                chunk_id INT,
                content STRING,
                author STRING,
                publication_year INT,
                section_title STRING
            )
        """
        if resume:
            with conn.cursor() as cs:
                cs.execute("CREATE TABLE IF NOT EXISTS books (book_id STRING, chunk_id INT, content STRING, author STRING, publication_year INT, section_title STRING)")
            existing = get_existing_book_ids(conn)
            _log(f"Found {len(existing)} book(s) already in BOOKS (will skip).")
        else:
            with conn.cursor() as cs:
                cs.execute(books_schema_sql)
            existing = set()
        _log("")
        
        if not os.path.isdir(PDF_FOLDER):
            _log(f"PDF folder not found: {PDF_FOLDER}")
            return
        pdf_files = sorted(f for f in os.listdir(PDF_FOLDER) if f.lower().endswith(".pdf"))
        if not pdf_files:
            _log(f"No PDF files in {PDF_FOLDER}")
            return
        
        to_process = [f for f in pdf_files if os.path.splitext(f)[0] not in existing]
        skipped = len(pdf_files) - len(to_process)
        if skipped:
            _log(f"Skipping {skipped} already-loaded book(s).")
        _log(f"Books to process: {len(to_process)}")
        _log(f"Chunking settings: max={CHUNK_MAX_CHARACTERS}, soft_max={CHUNK_NEW_AFTER}, overlap={CHUNK_OVERLAP}")
        _log("")
        
        total_chunks = 0
        for filename in to_process:
            pdf_path = os.path.join(PDF_FOLDER, filename)
            book_id = os.path.splitext(filename)[0]
            try:
                _log(f"Processing: {filename}")
                author, publication_year = get_pdf_metadata(pdf_path)
                _log(f"    Author: {author}, Year: {publication_year or 'Unknown'}")
                chunks = partition_and_chunk_pdf(pdf_path)
                if not chunks:
                    _log(f"    No chunks; skipping.")
                    _log("")
                    continue
                insert_book_chunks(conn, book_id, author, publication_year, chunks)
                total_chunks += len(chunks)
                _log("")
            except Exception as e:
                _log(f"    ✗ Error: {e}")
                _log("")
                continue
        
        if total_chunks == 0 and not existing:
            _log("No chunks uploaded. Add PDFs and run again (or use without --resume to start fresh).")
            return
        
        _log("Creating embeddings with AI_EMBED (this may take several minutes)...")
        try:
            with conn.cursor() as cs:
                cs.execute(f"""
                    CREATE OR REPLACE TABLE book_embeddings AS
                    SELECT
                        book_id,
                        chunk_id,
                        content,
                        author,
                        publication_year,
                        section_title,
                        AI_EMBED('{EMBEDDING_MODEL}', content) AS vector
                    FROM books
                """)
            _log("")
            _log("=" * 80)
            _log("✓ SUCCESS! All books loaded and embeddings created.")
            _log("=" * 80)
            _log("Next steps: Test semantic search with docs/queries.md")
        except snowflake_errors.ProgrammingError as e:
            err = str(e)
            if "AI_EMBED" in err or "Unknown function" in err or "CORTEX" in err.upper():
                _log("✗ Cortex AI not available. See docs/cortex-setup.md. Books are in BOOKS table.")
            else:
                raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
