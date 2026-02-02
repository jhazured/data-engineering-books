# Load Books into Snowflake AI

This repository contains a Python script to ingest PDF books into Snowflake, split them into text chunks, and create embeddings using Snowflake AI for semantic queries.

## Files

* `load_books_to_snowflake.py` - Python script to extract text, chunk, upload to Snowflake, and generate embeddings.
* `books_pdf_folder/` - Folder to store your PDF books.

## Requirements

* Python 3.8+
* Packages:

  ```bash
  pip install pdfplumber snowflake-connector-python pandas
  ```
* Snowflake account with:

  * Database and schema access
  * Warehouse
  * AI feature enabled (`AI_EMBED_TEXT`)

## Configuration

1. Place your PDFs in the `books_pdf_folder`.
2. Update `SNOWFLAKE_CONFIG` in `load_books_to_snowflake.py` with your Snowflake credentials:

```python
SNOWFLAKE_CONFIG = {
    "user": "YOUR_USER",
    "password": "YOUR_PASSWORD",
    "account": "YOUR_ACCOUNT",
    "warehouse": "YOUR_WAREHOUSE",
    "database": "YOUR_DATABASE",
    "schema": "PUBLIC",
}
```

3. Adjust `CHUNK_SIZE` if you want smaller or larger text chunks.

## Usage

Run the script:

```bash
python load_books_to_snowflake.py
```

The script will:

1. Extract text from each PDF
2. Split text into chunks
3. Load chunks into Snowflake table `books`
4. Generate embeddings into table `book_embeddings`

## Querying the Data

You can query the most relevant text chunks using Snowflake AI and semantic search:

```sql
SELECT content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(
    AI_EMBED_TEXT('Explain ETL concepts'), vector
) DESC
LIMIT 3;
```

This will return the top 3 text chunks most relevant to your query.

## Notes

* For scanned PDFs, you may need to run OCR before ingesting.
* Consider keeping metadata like book title and author for better organization.
* Adjust chunk size to balance performance and accuracy of AI queries.

## Optional Enhancements

* Automatically detect and OCR scanned PDFs.
* Include chapter/section titles in the table.
* Add book metadata columns (`author`, `publication_year`) for richer
