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
CHUNK_SIZE = 1000
MAX_SECTION_TITLE_LEN = 120
EMBEDDING_MODEL = "text-embedding-3-large"


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


def _first_short_line(text, max_len=MAX_SECTION_TITLE_LEN):
    if not text or not text.strip():
        return None
    line = text.strip().split("\n")[0].strip()
    if not line or len(line) > max_len:
        return None
    if re.match(r"^(Chapter|Section|Part)\s+\d+", line, re.IGNORECASE):
        return line
    return line if len(line) <= max_len else None


def extract_pages_with_sections(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            section_title = _first_short_line(page_text)
            yield page_text, section_title


def chunk_text_with_sections(pages_with_sections, chunk_size=CHUNK_SIZE):
    full_parts = []
    section_by_start = {}
    start = 0
    for page_text, section_title in pages_with_sections:
        full_parts.append(page_text)
        if page_text.strip():
            section_by_start[start] = section_title
        start += len(page_text) + 1
    full_text = "\n".join(full_parts)

    chunks = []
    pos = 0
    while pos < len(full_text):
        chunk = full_text[pos : pos + chunk_size]
        if not chunk.strip():
            pos += chunk_size
            continue
        section_title = None
        for s, title in sorted(section_by_start.items(), reverse=True):
            if s <= pos:
                section_title = title
                break
        chunks.append((chunk, section_title))
        pos += chunk_size
    return chunks


def load_books():
    """Extract PDFs from books_pdf_folder, chunk, and return a DataFrame. One bad PDF does not stop the run."""
    data_rows = []
    if not os.path.isdir(PDF_FOLDER):
        _log(f"PDF folder not found: {PDF_FOLDER}")
        return pd.DataFrame(data_rows)
    pdf_files = sorted(f for f in os.listdir(PDF_FOLDER) if f.lower().endswith(".pdf"))
    if not pdf_files:
        _log(f"No PDF files in {PDF_FOLDER}")
        return pd.DataFrame(data_rows)
    _log(f"Found {len(pdf_files)} PDF(s). Extracting text and chunking...")
    for filename in pdf_files:
        pdf_path = os.path.join(PDF_FOLDER, filename)
        try:
            _log(f"  Processing {filename}...")
            book_id = os.path.splitext(filename)[0]
            author, publication_year = get_pdf_metadata(pdf_path)
            pages = list(extract_pages_with_sections(pdf_path))
            chunks_with_sections = chunk_text_with_sections(pages, CHUNK_SIZE)
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
            _log(f"  Loaded {filename}: {n_chunks} chunks")
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
        _log("Creating embeddings (AI_EMBED_TEXT); this may take several minutes...")
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
                        AI_EMBED_TEXT(content, '{EMBEDDING_MODEL}') AS vector
                    FROM books
                """)
            _log("Done. All books loaded and embeddings created.")
        except snowflake_errors.ProgrammingError as e:
            if "AI_EMBED_TEXT" in str(e) or "Unknown function" in str(e):
                _log("  Skipped: AI_EMBED_TEXT is not available on this account (Cortex AI may be disabled or in a different region).")
                _log("Done. All books loaded into BOOKS table. Query BOOKS directly; book_embeddings was not created.")
            else:
                raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
