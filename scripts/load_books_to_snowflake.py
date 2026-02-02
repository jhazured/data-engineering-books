import os
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

# -----------------------------
# 2️⃣ Function to extract text from PDF
# -----------------------------
def extract_text_from_pdf(pdf_path):
    text_content = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text_content += page_text + "\n"
    return text_content

# -----------------------------
# 3️⃣ Function to split text into chunks
# -----------------------------
def chunk_text(text, chunk_size=CHUNK_SIZE):
    chunks = []
    start = 0
    while start < len(text):
        chunks.append(text[start:start+chunk_size])
        start += chunk_size
    return chunks

# -----------------------------
# 4️⃣ Prepare data for Snowflake
# -----------------------------
data_rows = []
for filename in os.listdir(PDF_FOLDER):
    if filename.lower().endswith(".pdf"):
        pdf_path = os.path.join(PDF_FOLDER, filename)
        book_id = os.path.splitext(filename)[0]
        full_text = extract_text_from_pdf(pdf_path)
        chunks = chunk_text(full_text)
        for idx, chunk in enumerate(chunks):
            data_rows.append({
                "book_id": book_id,
                "chunk_id": idx,
                "content": chunk
            })

df = pd.DataFrame(data_rows)

# -----------------------------
# 5️⃣ Connect to Snowflake and create table
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
        content STRING
    )
    """)

    # -----------------------------
    # 6️⃣ Load data into Snowflake
    # -----------------------------
    write_pandas(conn, df, 'BOOKS')

    # -----------------------------
    # 7️⃣ Create embeddings table
    # -----------------------------
    cs.execute("""
    CREATE OR REPLACE TABLE book_embeddings AS
    SELECT
        book_id,
        chunk_id,
        content,
        AI_EMBED_TEXT(content, 'text-embedding-3-large') AS vector
    FROM books
    """)
    print("✅ All books loaded and embeddings created!")
finally:
    conn.close()
