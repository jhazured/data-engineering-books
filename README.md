# Load Books into Snowflake AI

This repository contains a Python script to ingest PDF books into Snowflake, split them into text chunks, and create embeddings using Snowflake AI for semantic queries.

## Files

* `load_books_to_snowflake.py` - Python script to extract text, chunk, upload to Snowflake, and generate embeddings.
* `books_pdf_folder/` - Folder to store your PDF books.
* `mistral_agent.py` - Python script to interact with Mistral LLM, query vector DB, execute SQL in Snowflake, and work with CSV/Pandas data.

## Requirements

* Python 3.8+
* Packages:

  ```bash
  pip install pdfplumber snowflake-connector-python pandas langchain langchain-communities langchain-experimental
  ```
* Snowflake account with:

  * Database and schema access
  * Warehouse
  * AI feature enabled (`AI_EMBED_TEXT`)
* Hugging Face account and API token for Mistral model

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

3. Set your Hugging Face API token as an environment variable:

```bash
export HUGGINGFACEHUB_API_TOKEN="hf_your_token_here"
```

4. Replace the Mistral repo in `mistral_agent.py` with your model repository:

```python
repo_id = "YOUR_MISTRAL_MODEL_REPO"
```

5. Adjust `CHUNK_SIZE` if you want smaller or larger text chunks.

## Usage

### Load books and create embeddings:

```bash
python load_books_to_snowflake.py
```

### Query Snowflake embeddings:

```sql
SELECT content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(
    AI_EMBED_TEXT('Explain ETL concepts'), vector
) DESC
LIMIT 3;
```

### Use Mistral agent for questions:

```python
from mistral_agent import ask_mistral, personal_mistral, personal_mistral_snowflake, mistral_csv

# Normal QA
answer = ask_mistral("What is ETL?")

# RAG QA from vector DB
answer_context = personal_mistral("How do I orchestrate data pipelines with Airflow?", vector_db)

# Generate and execute SQL in Snowflake
sql_results = personal_mistral_snowflake("Get total users per region", vector_db)

# Query CSV / DataFrame
csv_result = mistral_csv(my_df, "What is the average value of column X?")
```

## Notes

* For scanned PDFs, you may need to run OCR before ingesting.
* Keep metadata like book title and author for better organization.
* Adjust chunk size to balance performance and accuracy of AI queries.
* Ensure your Hugging Face token and Mistral repo are correctly configured.

## Optional Enhancements

* Automatically detect and OCR scanned PDFs.
* Include chapter/section titles in the table.
* Add book metadata columns (`author`, `publication_year`) for richer queries.
* Expand Mistral agent
