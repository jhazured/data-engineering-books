import os
import re
import sys
import pdfplumber
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

# Load .env if available (unify with snowflake_helper config)
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(_REPO_ROOT, ".env"))
except ImportError:
    pass

PDF_FOLDER = os.path.join(_REPO_ROOT, "books_pdf_folder")

# IMPROVED CHUNKING PARAMETERS
CHUNK_SIZE = 2000  # Larger chunks preserve more complete concepts
CHUNK_OVERLAP = 400  # Larger overlap prevents concept splitting
MAX_SECTION_TITLE_LEN = 120

# Snowflake Cortex embedding model (use AI_EMBED). See docs/cortex-setup.md to enable Cortex.
EMBEDDING_MODEL = "snowflake-arctic-embed-m-v1.5"


def get_snowflake_config():
    """Unify config with snowflake_helper (env vars or placeholders)."""
    return snowflake_helper._get_config()


def get_pdf_metadata(pdf_path):
    """Extract author and publication year from PDF metadata."""
    author = "Unknown"
    publication_year = None
    try:
        with pdfplumber.open(pdf_path) as pdf:
            meta = getattr(pdf, "metadata", None) or {}
            author = meta.get("/Author") or meta.get("Author") or author
            if isinstance(author, bytes):
                author = author.decode("utf-8", errors="replace")
            creation = meta.get("/CreationDate") or meta.get("CreationDate") or ""
            if creation and isinstance(creation, str) and creation.startswith("D:") and len(creation) >= 6:
                year_str = creation[2:6]
                if year_str.isdigit():
                    publication_year = int(year_str)
    except Exception:
        pass
    return author, publication_year


def _extract_section_title(text, max_len=MAX_SECTION_TITLE_LEN):
    """
    Extract section title from text - CONSERVATIVE approach.
    Only accepts clear headers: Chapter/Section/Part + number/name
    Otherwise returns None rather than guessing.
    """
    if not text or not text.strip():
        return None
    
    lines = [line.strip() for line in text.strip().split("\n") if line.strip()]
    if not lines:
        return None
    
    # Only check first 5 lines for headers
    for line in lines[:5]:
        # STRICT Pattern: Only Chapter/Section/Part/Appendix with number or clear title
        # Examples: "Chapter 5", "Chapter 5: Replication", "Part II: Distributed Data"
        chapter_pattern = re.match(
            r"^(Chapter|Section|Part|Appendix|CHAPTER|SECTION|PART|APPENDIX)\s+(\d+|[IVX]+)(\s*[:.\-]\s*.+)?$", 
            line, 
            re.IGNORECASE
        )
        if chapter_pattern:
            # Clean up the title
            title = line.strip()
            # Remove excessive punctuation at the end
            title = re.sub(r'[.,:;]+$', '', title)
            return title[:max_len]
    
    # Fallback: Look for lines that are ALL CAPS and reasonably short (likely major section headers)
    for line in lines[:3]:
        if line.isupper() and 5 <= len(line) <= 50 and not re.search(r'\d{3,}', line):
            return line[:max_len]
    
    # No clear header found - return None rather than garbage
    return None


def extract_pages_with_sections(pdf_path):
    """Extract text from each page along with detected section titles."""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            section_title = _extract_section_title(page_text)
            yield page_text, section_title


def chunk_text_with_overlap(pages_with_sections, chunk_size=CHUNK_SIZE, overlap=CHUNK_OVERLAP):
    """
    Chunk text with overlap to preserve context across boundaries.
    Includes section title as context ONLY when we have a valid section title.
    Tries to break at paragraph boundaries when possible.
    """
    full_parts = []
    section_by_start = {}
    start = 0
    
    for page_text, section_title in pages_with_sections:
        full_parts.append(page_text)
        if page_text.strip() and section_title:  # Only track valid section titles
            section_by_start[start] = section_title
        start += len(page_text) + 1
    
    full_text = "\n".join(full_parts)
    
    chunks = []
    pos = 0
    chunk_index = 0
    
    while pos < len(full_text):
        # Calculate chunk boundaries
        chunk_end = min(pos + chunk_size, len(full_text))
        chunk = full_text[pos:chunk_end]
        
        if not chunk.strip():
            pos += chunk_size - overlap
            continue
        
        # Try to break at paragraph boundary (double newline) if we're within 200 chars of target
        if chunk_end < len(full_text):
            # Look for paragraph break in the last 200 chars of chunk
            search_start = max(0, len(chunk) - 200)
            paragraph_break = chunk.rfind('\n\n', search_start)
            if paragraph_break > search_start:
                chunk = chunk[:paragraph_break].strip()
        
        # Find the most recent VALID section title for this position
        section_title = None
        for s, title in sorted(section_by_start.items(), reverse=True):
            if s <= pos:
                section_title = title
                break
        
        # Only add section context if we have a valid section title
        # This prevents garbage like "[Section: delivered out of order...]"
        if section_title:
            content_with_context = f"[{section_title}]\n\n{chunk}"
        else:
            content_with_context = chunk
        
        chunks.append((content_with_context, section_title))
        chunk_index += 1
        
        # Move forward by (chunk_size - overlap) to create overlapping chunks
        pos += chunk_size - overlap
    
    return chunks


def load_books():
    """Extract PDFs from books_pdf_folder, chunk with overlap, and return a DataFrame."""
    data_rows = []
    if not os.path.isdir(PDF_FOLDER):
        _log(f"PDF folder not found: {PDF_FOLDER}")
        return pd.DataFrame(data_rows)
    
    pdf_files = sorted(f for f in os.listdir(PDF_FOLDER) if f.lower().endswith(".pdf"))
    if not pdf_files:
        _log(f"No PDF files in {PDF_FOLDER}")
        return pd.DataFrame(data_rows)
    
    _log(f"Found {len(pdf_files)} PDF(s). Extracting text and chunking...")
    _log(f"Chunk settings: size={CHUNK_SIZE} chars, overlap={CHUNK_OVERLAP} chars")
    
    for filename in pdf_files:
        pdf_path = os.path.join(PDF_FOLDER, filename)
        try:
            _log(f"  Processing {filename}...")
            book_id = os.path.splitext(filename)[0]
            author, publication_year = get_pdf_metadata(pdf_path)
            
            pages = list(extract_pages_with_sections(pdf_path))
            chunks_with_sections = chunk_text_with_overlap(pages, CHUNK_SIZE, CHUNK_OVERLAP)
            
            n_chunks = 0
            for idx, (chunk, section_title) in enumerate(chunks_with_sections):
                data_rows.append({
                    "book_id": book_id,
                    "chunk_id": idx,
                    "content": chunk,
                    "author": author,
                    "publication_year": publication_year if publication_year is not None else None,
                    "section_title": section_title,
                })
                n_chunks += 1
            
            _log(f"  Loaded {filename}: {n_chunks} chunks (with {CHUNK_OVERLAP}-char overlap)")
        except Exception as e:
            _log(f"  Skipping {filename}: {e}")
            continue
    
    return pd.DataFrame(data_rows)


def main():
    df = load_books()
    if df.empty:
        _log("No PDFs found in books_pdf_folder. Add PDFs and run again.")
        return

    _log(f"Total chunks: {len(df)}. Connecting to Snowflake...")
    config = get_snowflake_config()
    database = (config.get("database") or "").strip().upper()
    schema = (config.get("schema") or "PUBLIC").strip().upper()
    
    if not database or not schema:
        _log("Error: SNOWFLAKE_DATABASE and SNOWFLAKE_SCHEMA must be set in .env")
        return
    if not all(c.isalnum() or c == "_" for c in database + schema):
        _log("Error: SNOWFLAKE_DATABASE and SNOWFLAKE_SCHEMA must be alphanumeric (or underscore)")
        return
    
    conn = snowflake.connector.connect(**config)
    try:
        with conn.cursor() as cs:
            cs.execute(f"USE DATABASE {database}")
            cs.execute(f"USE SCHEMA {schema}")
        
        _log(f"Using database={database}, schema={schema}. Creating BOOKS table and uploading chunks...")
        
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
        
        # Upload data to BOOKS table
        try:
            write_pandas(conn, df, "BOOKS")
            _log(f"  Uploaded {len(df)} rows to BOOKS.")
        except (snowflake_errors.MissingDependencyError, Exception) as e:
            if "pandas" in str(e).lower() or isinstance(e, snowflake_errors.MissingDependencyError):
                _log("  write_pandas skipped (pandas dependency); inserting in batches...")
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
            _log(f"  Uploaded {len(df)} rows to BOOKS.")
        
        _log("Creating embeddings (AI_EMBED); this may take several minutes...")
        
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
            _log("Done. All books loaded and embeddings created with improved chunking strategy.")
            _log(f"Chunk settings used: size={CHUNK_SIZE}, overlap={CHUNK_OVERLAP}")
        except snowflake_errors.ProgrammingError as e:
            err = str(e)
            if "AI_EMBED" in err or "Unknown function" in err or "CORTEX" in err.upper():
                _log("  Skipped: Cortex AI embeddings not available. See docs/cortex-setup.md to enable.")
                _log("Done. All books loaded into BOOKS table. Query BOOKS directly; book_embeddings was not created.")
            else:
                raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
