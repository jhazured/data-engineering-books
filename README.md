# Load Books into Snowflake AI

[![Verify](https://github.com/jhazured/data-engineering-books/actions/workflows/verify.yml/badge.svg)](https://github.com/jhazured/data-engineering-books/actions/workflows/verify.yml)
[![Last commit](https://img.shields.io/github/last-commit/jhazured/data-engineering-books?color=blue)](https://github.com/jhazured/data-engineering-books)
[![Snowflake](https://img.shields.io/badge/Snowflake-Compatible-blue.svg)](https://www.snowflake.com/)
[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![Mistral LLM](https://img.shields.io/badge/Mistral-7B_Instruct-green.svg)](https://huggingface.co/mistralai)

## Introduction

**Load Books into Snowflake AI** is a small pipeline that ingests PDF books into Snowflake, chunks the text, and builds vector embeddings with Snowflake’s built-in AI. You can then run semantic search in SQL or use an optional Mistral-based agent (LangChain + Hugging Face) for Q&A, RAG over your book corpus, and SQL generation against Snowflake.

The project is aimed at data engineers and learners who want to combine **Snowflake** (tables, `AI_EMBED_TEXT`, vector search) with **Python** (PDF extraction, chunking, metadata) and **LLM/RAG** (Mistral agent). It demonstrates practical data engineering—ingestion, metadata, chunking—and modern analytics AI: embeddings, vector similarity, and retrieval-augmented generation. The repo also includes a short **book collection analysis** (strengths and gaps) for data engineering reading.

Use it to run semantic search over your own PDFs in Snowflake, to experiment with RAG and vector search, or as a portfolio piece showing Snowflake AI, Python pipelines, and documentation.

---

## Quick start

From the **repository root**:

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure credentials (see Setup below)
cp .env.example .env
# Edit .env with your Snowflake and Hugging Face values.

# 3. Place your PDFs in books_pdf_folder/

# 4. Load books and create embeddings in Snowflake
python scripts/load_books_to_snowflake.py

# 5. Verify setup (optional)
python scripts/verify_setup.py
```

Then query in Snowflake (see [Query embeddings](#query-embeddings)) or use the [Mistral agent](#mistral-agent) from Python.

---

## Project structure

| Path | Description |
|------|-------------|
| `books_pdf_folder/` | PDF books to ingest (place your `.pdf` files here). |
| `scripts/load_books_to_snowflake.py` | Extract text from PDFs, chunk, upload to Snowflake; adds metadata (author, publication_year, section_title) from PDF metadata and per-page headings. Creates `books` and `book_embeddings` tables. |
| `scripts/mistral_snowflake_agent.py` | Mistral LLM agent: Q&A, RAG from vector DB, SQL execution in Snowflake, Pandas/CSV agent. |
| `scripts/snowflake_helper.py` | Snowflake helper used by the Mistral agent to run SQL (reads config from `.env` or env vars). |
| `.env.example` | Template for Snowflake and Hugging Face credentials; copy to `.env` and fill in. |
| `requirements.txt` | Python dependencies (key versions pinned for reproducibility; see [Dependencies](#dependencies)). |
| `scripts/verify_setup.py` | Verify Python packages and optional Snowflake connectivity. |

---

## Architecture

```
PDFs (books_pdf_folder/)
    → load_books_to_snowflake.py (extract text, chunk)
    → Snowflake: books (chunks) → book_embeddings (AI_EMBED_TEXT vectors)
    → Vector search (SQL) or Mistral agent (RAG, Q&A, SQL over Snowflake)
```

- **PDFs** → Python script extracts text and PDF metadata (author, publication year), infers section/chapter titles from the first short line per page, splits into chunks (default 1000 chars).
- **Snowflake** → `books` table stores chunks; `book_embeddings` adds vectors via Snowflake AI (`AI_EMBED_TEXT`, default model `text-embedding-3-large`; vector dimensionality is model-dependent—see Snowflake docs).
- **Consumption** → Query embeddings with `VECTOR_SIMILARITY` in SQL, or use the Mistral agent for RAG, Q&A, and SQL generation.

---

## Data engineering book collection: strengths and gaps

The `books_pdf_folder/` contains a curated set of data engineering and analytics books. Below is a short **strength and gap analysis** of the collection for learning data engineering.

### Strengths

| Area | Books | Why it helps |
|------|--------|----------------|
| **Foundations** | *Designing Data-Intensive Applications* (Kleppmann), *Fundamentals of Data Engineering* (Reis & Housley) | DDIA: storage, replication, consistency, batch/stream. FDE: modern DE end-to-end (modeling, ingestion, storage, transform, orchestration, quality). |
| **Data warehousing & modeling** | *The Data Warehouse Toolkit* (Kimball) | Star schema, dimensions, facts, ETL for analytics. Standard reference for dimensional modeling. |
| **Batch & big data** | *Spark: The Definitive Guide*, *Hadoop: The Definitive Guide* | Spark: modern batch + structured streaming. Hadoop: legacy context, HDFS/YARN. |
| **Streaming** | *Streaming Systems* (Akidau et al.), *Big Data* (Marz & Warren) | Event time, watermarks, exactly-once; Lambda architecture. Strong conceptual base. |
| **Distributed systems** | *Designing Distributed Systems* (Burns) | Patterns for scalable, reliable services. Complements DDIA. |
| **Modern pipelines** | *Rebuilding Reliable Data Pipelines Through Modern Tools* | Aligns with current tooling and practices. |
| **Python** | *Data Engineering with Python* | Practical DE with Python (APIs, DBs, pipelines). |
| **Cloud** | *Data Science on AWS* | Cloud-native analytics and data on AWS. |
| **Event-driven & Kafka** | *Designing Event-Driven Systems*, *Kafka: The Definitive Guide* | Event-driven architecture and Kafka in depth. |
| **Databricks** | *Data Engineering with Databricks* (Verma) | Lakehouse and Spark on Databricks. |

Overall, the collection is **strong** on foundations, warehousing, batch/streaming, distributed systems, Python, cloud, event-driven/Kafka, and modern pipelines.

### Gaps (optional additions)

| Gap | Why it matters | Possible additions |
|-----|----------------|--------------------|
| **DW 2.0 / Inmon** | You have Kimball (dimensional); no Inmon view of warehousing architecture (corporate information factory, top-down EDW). | *DW 2.0: The Architecture for the Next Generation of Data Warehousing* (Inmon et al.). |
| **Orchestration** | No book focused on Airflow/Prefect/Dagster. | *Data Pipelines with Apache Airflow* or similar. |
| **Analytics engineering / dbt** | Transformations-in-SQL, testing, docs. | *Analytics Engineering with dbt* or dbt Labs docs. |
| **Data quality & reliability** | Contracts, testing, monitoring, SLOs. | *Data Quality Fundamentals* (Barr Moses et al.) or *Data Contracts* (Andrew Jones). |
| **Governance / catalog / lineage** | Discovery, lineage, ownership. | *Data Governance* (O’Reilly) or *Data Mesh* (Dehghani). |
| **Data modeling (3NF / Data Vault)** | Kimball is dimensional; no dedicated coverage of 3NF, canonical modeling, or Data Vault. | *Building a Scalable Data Warehouse with Data Vault 2.0* (Linstedt) or similar. |
| **MLOps / feature stores** | Building and serving features for ML; not in the collection. | *Feature Store for Machine Learning* (O’Reilly) or vendor docs (Feast, Tecton). |
| **Real-time analytics** | Streaming is covered; real-time dashboards, time-series, and operational analytics are a distinct focus. | *Real-Time Analytics* (O’Reilly) or platform docs (e.g. ksqlDB, Pinot). |
| **Data observability** | Monitoring data health, freshness, and lineage in production. | *Data Observability* (Barr Moses) or *Data Reliability Engineering* (O’Reilly). |

These are **optional**; the current set already covers core DE well. Add books in the gaps above if you want deeper coverage of Inmon/DW 2.0, orchestration, dbt, data quality, governance, data vault, MLOps, real-time analytics, or observability.

---

## Requirements

- **Python 3.8+**
- **Snowflake account** with database/schema access, a warehouse, and **AI feature** enabled (`AI_EMBED_TEXT` for embeddings). Snowflake AI usage (e.g. embeddings) may incur additional cost; see [Snowflake pricing](https://www.snowflake.com/pricing/) and your warehouse size for compute. A small warehouse is usually sufficient for small/medium book sets.
- **Hugging Face account** and API token (for the Mistral agent only).

---

## Setup (first-time)

Do this once before loading books or using the Mistral agent.

**⚠️ Never commit credentials.** This project uses `.env` for local development only. Do not commit `.env` or put real credentials in code.

### 1. Install Python packages

Use **requirements.txt** for a reproducible install (recommended):

```bash
pip install -r requirements.txt
```

**Dependencies:** Key packages are pinned in `requirements.txt` for reproducibility: `snowflake-connector-python`, `pdfplumber`, `pandas`, and the LangChain set (`langchain`, `langchain-community`, `langchain-experimental`). See the file for exact version ranges.

Use **manual install** only if you need to install a subset (e.g. loader only, without LangChain):

```bash
pip install pdfplumber snowflake-connector-python pandas langchain langchain-community langchain-experimental
```

**Verify installation:**

```bash
python scripts/verify_setup.py
```

This checks that required packages are installed, `.env` exists, and optionally tests the Snowflake connection.

### 2. Snowflake credentials

**Option A – Recommended (env / .env)**  
Copy the example file and edit with your values:

```bash
cp .env.example .env
```

Set in `.env`:

- `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`

The **Mistral agent** (via `snowflake_helper.py`) uses these env vars. Load the env before running (e.g. `source .env` or use `python-dotenv`).

**Option B – Inline in the loader**  
Edit `scripts/load_books_to_snowflake.py` and set `SNOWFLAKE_CONFIG`:

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

The **loader script** uses this dict; the Mistral agent still uses env vars (or you can pass a config into the helper).

### 3. Hugging Face (Mistral agent only)

Set your token (e.g. in `.env` or in the shell):

```bash
export HUGGINGFACEHUB_API_TOKEN="hf_your_token_here"
```

In `scripts/mistral_snowflake_agent.py`, set your model repo:

```python
repo_id = "YOUR_MISTRAL_MODEL_REPO"  # e.g. mistralai/Mistral-7B-Instruct-v0.2
```

### 4. PDFs

Place your PDF files in `books_pdf_folder/`. The loader reads all `.pdf` files in that folder. If there are no PDFs, the script exits without connecting to Snowflake.

### 5. Chunk size (optional)

In `scripts/load_books_to_snowflake.py`, adjust `CHUNK_SIZE` (default 1000 characters) to tune chunk size for embeddings.

---

## Usage

### Load books and create embeddings

Run from the **repository root**:

```bash
python scripts/load_books_to_snowflake.py
```

The script will:

1. Read all PDFs from `books_pdf_folder/`
2. Extract text and split into chunks
3. Create or replace the `books` table and load chunks
4. Create or replace the `book_embeddings` table with Snowflake `AI_EMBED_TEXT` vectors

If no PDFs are found, it prints a message and exits without connecting to Snowflake.

**Expected output (success):**

```
✅ All books loaded and embeddings created!
```

**Expected results:** Tables `books` and `book_embeddings` are created or replaced with columns: `book_id`, `chunk_id`, `content`, `author`, `publication_year`, `section_title`, and `vector` (embeddings only). Author and publication year come from PDF metadata; section/chapter titles are inferred from the first short line per page. Row count in `books` equals total chunks across all PDFs.

### Query embeddings

In Snowflake (worksheet or CLI), run semantic search over the embedded chunks:

```sql
SELECT content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(
    AI_EMBED_TEXT('Explain ETL concepts'), vector
) DESC
LIMIT 3;
```

**Expected results:** Returns the top 3 text chunks most similar to the query (e.g. "Explain ETL concepts"). Each row is a chunk of book content; use `content` for display or downstream RAG.

Filter or display by metadata (author, publication_year, section_title):

```sql
SELECT author, publication_year, section_title, content
FROM book_embeddings
WHERE author ILIKE '%Kleppmann%'
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('Explain ETL concepts'), vector) DESC
LIMIT 5;
```

### Mistral agent

From the repo root (so that `scripts` is on `PYTHONPATH`), or from `scripts/`:

```python
from scripts.mistral_snowflake_agent import (
    ask_mistral,
    personal_mistral,
    personal_mistral_snowflake,
    mistral_csv,
)

# Simple Q&A (no context)
answer = ask_mistral("What is ETL?")
print(answer)
```

**Example output** (actual response will vary by model):

```
ETL stands for Extract, Transform, Load. It's a process used in data warehousing and analytics:
1. Extract — pull data from source systems (databases, APIs, files).
2. Transform — clean, validate, and reshape the data (e.g. deduplication, type casting).
3. Load — write the transformed data into a target (e.g. warehouse, lake).

ETL pipelines are often scheduled (batch) or event-driven and are foundational for analytics and ML.
```

```python
# RAG: answer using your book chunks from a vector DB
answer_context = personal_mistral(
    "How do I orchestrate data pipelines with Airflow?", vector_db
)

# Generate SQL from context and run in Snowflake
sql_results = personal_mistral_snowflake("Get total users per region", vector_db)

# Query a pandas DataFrame
csv_result = mistral_csv(my_df, "What is the average value of column X?")
```

The agent uses `scripts/snowflake_helper.py` to run SQL in Snowflake; configure Snowflake via `.env` or environment variables (see [Setup](#setup-first-time)).

**Note:** The Mistral agent uses LangChain’s `LLMChain`, which is deprecated in newer LangChain versions. You may see a deprecation warning; the code still works. Migrating to LCEL (LangChain Expression Language) is planned for a future update.

---

## Performance

Performance depends on warehouse size, PDF count, and chunk size. Run the loader with your own data and measure extract time, embedding throughput, and query latency; scale warehouse or adjust `CHUNK_SIZE` as needed.

---

## Troubleshooting

| Issue | What to do |
|-------|------------|
| **PDF extraction fails or returns empty text** | Ensure PDFs are text-based, not scanned images. Use OCR for image-only PDFs. |
| **Snowflake connection timeout** | Verify warehouse is running (not suspended), credentials in `.env` or script are correct, and network allows outbound to Snowflake. |
| **Embeddings creation slow** | Check warehouse size; consider scaling up. Reduce `CHUNK_SIZE` or number of PDFs per run to test. |
| **`AI_EMBED_TEXT` not found** | Enable Snowflake AI features on your account; confirm model name (e.g. `text-embedding-3-large`) is available in your region. |
| **Mistral agent import error** | Install optional packages: `pip install langchain langchain-community langchain-experimental`. Set `HUGGINGFACEHUB_API_TOKEN` and `repo_id` in the agent script. |
| **Corrupted or password-protected PDFs** | Remove or fix PDFs; the loader skips or fails on unreadable files. |

---

## Notes

- **Secrets:** Do not commit `.env`. It is listed in `.gitignore`.
- **Scanned PDFs:** Image-only PDFs may need OCR before ingesting.
- **Metadata:** The loader adds `author`, `publication_year`, and `section_title` (from PDF metadata and per-page first line). Use these in SQL for filtering and display (e.g. `WHERE author = '...'`, `ORDER BY publication_year`).
- **Chunk size:** Smaller chunks give more precise retrieval; larger chunks give more context per chunk.
- **Chunk overlap:** Standard RAG practice often uses overlapping chunks (e.g. 1000 chars with 200 char stride) to preserve context at boundaries. This implementation uses non-overlapping chunks; overlap can be added in the loader for improved retrieval.

---

## Loader robustness and error handling

The loader handles common edge cases: empty or missing PDF metadata (author/year default to "Unknown" / NULL); pages with no text (section_title remains NULL); and per-PDF extraction wrapped in iteration so one bad file does not stop the run. Encoding is handled via pdfplumber’s default UTF-8 extraction. Corrupted or password-protected PDFs can cause a single-file failure; remove or fix those files and re-run. For production, consider try/except per file and logging failed paths.

---

## Production / next steps

This project is set up for **local development**. To productionize or harden:

- **Orchestration:** Run the loader on a schedule (e.g. Airflow DAG, Prefect flow, or cron).
- **Containerization:** Dockerize the loader and run in a container (e.g. Azure Container Instances, AWS ECS).
- **CI/CD:** Add GitHub Actions to run `verify_setup.py` and optional Snowflake connectivity checks; add data quality checks (duplicate detection, chunk size validation).
- **Secrets:** Use a secrets manager (e.g. Azure Key Vault, AWS Secrets Manager) instead of `.env` in production.

---

## What this demonstrates

- **Snowflake:** Tables, AI features (`AI_EMBED_TEXT`), vector search, SQL.
- **Python data engineering:** PDF processing, chunking, Snowflake connector.
- **Modern AI/ML:** Embeddings, RAG, vector search, LLM integration (Mistral).
- **Documentation:** Setup, architecture, troubleshooting, expected results.

**Possible extensions:** CI/CD with GitHub Actions (test Snowflake connection, data quality); data quality checks (duplicate detection, chunk size validation); production deployment (Airflow, containers).

---

## Optional enhancements

- Detect and run OCR on image-only PDFs.
- Chapter/section titles and author/publication_year are now included (see [Load books](#load-books-and-create-embeddings)).
- Extend the Mistral agent (e.g. more tools, different models).
