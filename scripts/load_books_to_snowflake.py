"""
Production-grade PDF loader using Unstructured.io for semantic chunking.

This script uses Unstructured's document partitioning and chunking capabilities
to properly handle technical books with complex structure.

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
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
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
    try:
        elements = partition_pdf(
            filename=pdf_path,
            strategy="auto",  # Use "hi_res" if you have local-inference installed
            infer_table_structure=True,  # Detect tables properly
            include_page_breaks=False,
        )
        _log(f"    Detected {len(elements)} document elements")
    except Exception as e:
        _log(f"    Error during partitioning: {e}")
        _log(f"    Falling back to fast strategy...")
        elements = partition_pdf(
            filename=pdf_path,
            strategy="fast",
            include_page_breaks=False,
        )
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


def load_books():
    """Extract PDFs using Unstructured.io and return a DataFrame."""
    data_rows = []
    
    if not os.path.isdir(PDF_FOLDER):
        _log(f"PDF folder not found: {PDF_FOLDER}")
        return pd.DataFrame(data_rows)
    
    pdf_files = sorted(f for f in os.listdir(PDF_FOLDER) if f.lower().endswith(".pdf"))
    if not pdf_files:
        _log(f"No PDF files in {PDF_FOLDER}")
        return pd.DataFrame(data_rows)
    
    _log(f"Found {len(pdf_files)} PDF(s).")
    _log(f"Chunking settings: max={CHUNK_MAX_CHARACTERS}, soft_max={CHUNK_NEW_AFTER}, overlap={CHUNK_OVERLAP}")
    _log("")
    
    for filename in pdf_files:
        pdf_path = os.path.join(PDF_FOLDER, filename)
        try:
            _log(f"Processing: {filename}")
            book_id = os.path.splitext(filename)[0]
            
            # Get metadata
            author, publication_year = get_pdf_metadata(pdf_path)
            _log(f"    Author: {author}, Year: {publication_year or 'Unknown'}")
            
            # Partition and chunk with Unstructured
            chunks = partition_and_chunk_pdf(pdf_path)
            
            # Add to data rows
            for chunk in chunks:
                data_rows.append({
                    "book_id": book_id,
                    "chunk_id": chunk['chunk_id'],
                    "content": chunk['content'],
                    "author": author,
                    "publication_year": publication_year,
                    "section_title": chunk['section_title'],
                })
            
            _log(f"    ✓ Completed: {len(chunks)} chunks")
            _log("")
            
        except Exception as e:
            _log(f"    ✗ Error processing {filename}: {e}")
            _log("")
            continue
    
    return pd.DataFrame(data_rows)


def main():
    _log("=" * 80)
    _log("PDF LOADER - Using Unstructured.io for semantic chunking")
    _log("=" * 80)
    _log("")
    
    # Check if unstructured is installed
    try:
        import unstructured
        _log(f"✓ Unstructured.io version: {getattr(unstructured, '__version__', '?')}")
    except ImportError:
        _log("✗ ERROR: unstructured library not found")
        _log("  Install with: pip install 'unstructured[pdf]'")
        _log("  For better quality: pip install 'unstructured[local-inference]'")
        return
    
    _log("")
    
    df = load_books()
    if df.empty:
        _log("No PDFs processed. Add PDFs to books_pdf_folder and run again.")
        return
    
    _log("=" * 80)
    _log(f"Total chunks extracted: {len(df)}")
    _log("=" * 80)
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
        _log("Creating BOOKS table...")
        
        with conn.cursor() as cs:
            cs.execute("""
                CREATE OR REPLACE TABLE books (
                    book_id STRING,
                    chunk_id INT,
                    content STRING,
                    author STRING,
                    publication_year INT,
                    section_title STRING
                )
            """)
        
        _log("Uploading chunks to Snowflake...")
        
        # Upload data to BOOKS table
        try:
            write_pandas(conn, df, "BOOKS")
            _log(f"  ✓ Uploaded {len(df)} rows to BOOKS")
        except (snowflake_errors.MissingDependencyError, Exception) as e:
            if "pandas" in str(e).lower() or isinstance(e, snowflake_errors.MissingDependencyError):
                _log("  write_pandas unavailable; inserting in batches...")
            
            cols = ["book_id", "chunk_id", "content", "author", "publication_year", "section_title"]
            rows = list(df[cols].itertuples(index=False, name=None))
            
            with conn.cursor() as cs:
                for i in range(0, len(rows), INSERT_BATCH_SIZE):
                    batch = rows[i : i + INSERT_BATCH_SIZE]
                    cs.executemany(
                        "INSERT INTO books (book_id, chunk_id, content, author, publication_year, section_title) VALUES (%s, %s, %s, %s, %s, %s)",
                        batch,
                    )
                    _log(f"  Inserted rows {i + 1}-{i + len(batch)} of {len(df)}...")
            
            _log(f"  ✓ Uploaded {len(df)} rows to BOOKS")
        
        _log("")
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
            _log("")
            _log("Chunking approach used:")
            _log(f"  • Structure-aware partitioning (titles, paragraphs, lists, tables)")
            _log(f"  • Semantic chunking by title (preserves section boundaries)")
            _log(f"  • Max chunk size: {CHUNK_MAX_CHARACTERS} characters")
            _log(f"  • Overlap: {CHUNK_OVERLAP} characters")
            _log("")
            _log("Next steps:")
            _log("  1. Test semantic search with queries from docs/queries.md")
            _log("  2. Check section_title quality: SELECT DISTINCT section_title FROM book_embeddings;")
            _log("  3. Compare results to previous chunking approach")
            
        except snowflake_errors.ProgrammingError as e:
            err = str(e)
            if "AI_EMBED" in err or "Unknown function" in err or "CORTEX" in err.upper():
                _log("")
                _log("✗ Cortex AI embeddings not available. See docs/cortex-setup.md to enable.")
                _log("✓ Books loaded into BOOKS table (query directly; book_embeddings not created)")
            else:
                raise
                
    finally:
        conn.close()


if __name__ == "__main__":
    main()
