import os
import re
import pdfplumber
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd

# -----------------------------
# 1️⃣ Configuration
# -----------------------------
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPT_DIR)
PDF_FOLDER = os.path.join(_REPO_ROOT, "books_pdf_folder")  # folder containing PDFs
SNOWFLAKE_CONFIG = {
    "user": "YOUR_USER",
    "password": "YOUR_PASSWORD",
    "account": "YOUR_ACCOUNT",
    "warehouse": "YOUR_WAREHOUSE",
    "database": "YOUR_DATABASE",
    "schema": "PUBLIC",
}

CHUNK_SIZE = 1000  # number of characters per chunk
MAX_SECTION_TITLE_LEN = 120  # first line shorter than this may be used as section/chapter title

# -----------------------------
# 2️⃣ PDF metadata (author, publication_year)
# -----------------------------
def get_pdf_metadata(pdf_path):
    """Extract author and publication year from PDF metadata. Returns (author, publication_year)."""
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


# -----------------------------
# 3️⃣ Extract text per page with optional section title (first short line)
# -----------------------------
def _first_short_line(text, max_len=MAX_SECTION_TITLE_LEN):
    """Use first non-empty line as section title if short enough."""
    if not text or not text.strip():
        return None
    line = text.strip().split("\n")[0].strip()
    if not line or len(line) > max_len:
        return None
    # Optionally treat "Chapter N" / "Section N" style as section
    if re.match(r"^(Chapter|Section|Part)\s+\d+", line, re.IGNORECASE):
        return line
    return line if len(line) <= max_len else None


def extract_pages_with_sections(pdf_path):
    """Yield (page_text, section_title) for each page."""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            section_title = _first_short_line(page_text)
            yield page_text, section_title


# -----------------------------
# 4️⃣ Chunk full text and assign section_title by page
# -----------------------------
def chunk_text_with_sections(pages_with_sections, chunk_size=CHUNK_SIZE):
    """Build chunks from concatenated page text; assign section_title from page."""
    full_parts = []
    section_by_start = {}  # start index -> section_title
    start = 0
    for page_text, section_title in pages_with_sections:
        full_parts.append(page_text)
        if page_text.strip():
            section_by_start[start] = section_title
        start += len(page_text) + 1  # +1 for newline
    full_text = "\n".join(full_parts)

    chunks = []
    pos = 0
    while pos < len(full_text):
        chunk = full_text[pos : pos + chunk_size]
        if not chunk.strip():
            pos += chunk_size
            continue
        # Find section_title for this chunk (page that contains pos)
        section_title = None
        for s, title in sorted(section_by_start.items(), reverse=True):
            if s <= pos:
                section_title = title
                break
        chunks.append((chunk, section_title))
        pos += chunk_size
    return chunks


# -----------------------------
# 5️⃣ Prepare data for Snowflake
# -----------------------------
data_rows = []
for filename in sorted(os.listdir(PDF_FOLDER)):
    if not filename.lower().endswith(".pdf"):
        continue
    pdf_path = os.path.join(PDF_FOLDER, filename)
    book_id = os.path.splitext(filename)[0]
    author, publication_year = get_pdf_metadata(pdf_path)
    pages = list(extract_pages_with_sections(pdf_path))
    chunks_with_sections = chunk_text_with_sections(pages, CHUNK_SIZE)
    for idx, (chunk, section_title) in enumerate(chunks_with_sections):
        data_rows.append({
            "book_id": book_id,
            "chunk_id": idx,
            "content": chunk,
            "author": author,
            "publication_year": publication_year if publication_year is not None else None,
            "section_title": section_title,
        })

df = pd.DataFrame(data_rows)

# -----------------------------
# 6️⃣ Connect to Snowflake and create table
# -----------------------------
if df.empty:
    print("⚠️ No PDFs found in books_pdf_folder. Add PDFs and run again.")
    exit(0)

conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
try:
    cs = conn.cursor()
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

    write_pandas(conn, df, "BOOKS")

    cs.execute("""
    CREATE OR REPLACE TABLE book_embeddings AS
    SELECT
        book_id,
        chunk_id,
        content,
        author,
        publication_year,
        section_title,
        AI_EMBED_TEXT(content, 'text-embedding-3-large') AS vector
    FROM books
    """)
    print("✅ All books loaded and embeddings created!")
finally:
    conn.close()
