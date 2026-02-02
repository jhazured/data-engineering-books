# Load Books into Snowflake AI

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Snowflake](https://img.shields.io/badge/Snowflake-Compatible-blue.svg)](https://www.snowflake.com/)
[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![Mistral LLM](https://img.shields.io/badge/Mistral-7B_Instruct-green.svg)](https://huggingface.co/mistralai)

Ingest PDF books into Snowflake, split them into text chunks, and create embeddings using Snowflake AI for semantic search. Optionally use the Mistral agent (LangChain + Hugging Face) for Q&A, RAG, and SQL over your book content.

---

## Quick start

From the **repository root**:

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure credentials (see Setup below)
cp .env.example .env
# Edit .env with your Snowflake and Hugging Face values.

# 3. Add PDFs to books_pdf_folder/ (or use the ones already there)

# 4. Load books and create embeddings in Snowflake
python scripts/load_books_to_snowflake.py
```

Then query in Snowflake (see [Query embeddings](#query-embeddings)) or use the [Mistral agent](#mistral-agent) from Python.

---

## Project structure

| Path | Description |
|------|-------------|
| `books_pdf_folder/` | PDF books to ingest (place your `.pdf` files here). |
| `scripts/load_books_to_snowflake.py` | Extract text from PDFs, chunk, upload to Snowflake, create `books` and `book_embeddings` tables. |
| `scripts/mistral_snowflake_agent.py` | Mistral LLM agent: Q&A, RAG from vector DB, SQL execution in Snowflake, Pandas/CSV agent. |
| `scripts/snowflake_helper.py` | Snowflake helper used by the Mistral agent to run SQL (reads config from `.env` or env vars). |
| `.env.example` | Template for Snowflake and Hugging Face credentials; copy to `.env` and fill in. |
| `requirements.txt` | Python dependencies. |

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
- **Snowflake account** with database/schema access, a warehouse, and **AI feature** enabled (`AI_EMBED_TEXT` for embeddings).
- **Hugging Face account** and API token (for the Mistral agent only).

---

## Setup (first-time)

Do this once before loading books or using the Mistral agent.

### 1. Install Python packages

```bash
pip install -r requirements.txt
```

Optional manual install:

```bash
pip install pdfplumber snowflake-connector-python pandas langchain langchain-community langchain-experimental
```

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

Put PDF files in `books_pdf_folder/`. The loader reads all `.pdf` files in that folder. If there are no PDFs, the script exits without connecting to Snowflake.

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

---

## Notes

- **Secrets:** Do not commit `.env`. It is listed in `.gitignore`.
- **Scanned PDFs:** Image-only PDFs may need OCR before ingesting.
- **Metadata:** Consider adding book title, author, or other metadata for richer queries.
- **Chunk size:** Smaller chunks give more precise retrieval; larger chunks give more context per chunk.

---

## Optional enhancements

- Detect and run OCR on image-only PDFs.
- Add chapter/section titles to chunks or metadata.
- Add columns such as `author`, `publication_year` for filtering and display.
- Extend the Mistral agent (e.g. more tools, different models).
