# Data Engineering Books – Full Project Review

## Purpose

Pipeline to ingest PDF books into Snowflake, chunk with Unstructured.io, build vector embeddings with Snowflake Cortex (`AI_EMBED`), and support:

1. **Semantic search in SQL** (workbook / `docs/queries.md`)
2. **Chat-style Q&A** – ask a question, get one synthesized answer from your books (`scripts/ask_books.py`)

---

## Current Status: Complete for Core Flows

| Flow | Status | How |
|------|--------|-----|
| Load PDFs → Snowflake | ✅ | `scripts/load_books_to_snowflake.py` (Unstructured `by_title` chunking → `book_chunks_staging` → `book_embeddings` with `AI_EMBED`) |
| Semantic search (SQL) | ✅ | Query `book_embeddings` with `VECTOR_COSINE_SIMILARITY` + `AI_EMBED`; examples in `docs/queries.md` and `docs/workbook.ipynb` |
| Chat-style answer from books | ✅ | `scripts/ask_books.py` → Snowflake retriever (`scripts/snowflake_retriever.py`) → `personal_mistral()` in `scripts/mistral_snowflake_agent.py` |
| Section/chapter labels | ✅ Improved | Load script uses Unstructured `Title` when present, plus first-line fallback for headings (e.g. "Chapter 5", short title-case lines) |

---

## Project Layout

```
data-engineering-books/
├── .env.example              # Env template (Snowflake, Hugging Face); copy to .env
├── .gitignore
├── README.md                 # Main docs, quick start, project structure, setup
├── requirements.txt         # Full deps (unstructured[pdf], langchain, snowflake, etc.)
├── requirements-agent-only.txt   # Slim venv when not running PDF load (agent + Snowflake only)
│
├── .github/workflows/
│   └── verify.yml            # CI: install deps, run scripts/verify_setup.py
│
├── books_pdf_folder/         # Put PDFs here (gitignore if desired; not in repo by default)
│
├── docs/
│   ├── REVIEW.md             # This file – full project review
│   ├── cortex-setup.md       # Snowflake Cortex (AI_EMBED) grants and checks
│   ├── queries.md            # Semantic search SQL examples
│   ├── unstructured-setup.md # Unstructured.io install (hi_res, tesseract, poppler)
│   ├── workbook.ipynb        # Snowflake notebook (import into Snowsight) – same queries as queries.md
│   └── images/
│       └── architecture.png # Diagram referenced by README
│
└── scripts/
    ├── ask_books.py          # CLI: ask a question → one answer from book embeddings (RAG)
    ├── load_books_to_snowflake.py  # Ingest PDFs → chunk → Snowflake book_chunks_staging + book_embeddings
    ├── mistral_snowflake_agent.py   # Mistral LLM: personal_mistral (RAG), personal_mistral_snowflake (SQL), mistral_csv
    ├── queries_to_workbook.py      # Generate docs/workbook.ipynb from docs/queries.md
    ├── schema.sql            # CREATE TABLE book_chunks_staging, book_embeddings (run once in Snowflake)
    ├── snowflake_helper.py   # Run SQL in Snowflake (config from env)
    ├── snowflake_retriever.py      # Retriever over book_embeddings for RAG (similarity_search)
    ├── snowflake_startup.py  # One-time: create warehouse, database, schema
    ├── snowflake_teardown.py # Drop database/warehouse (with confirmation)
    └── verify_setup.py       # Check Python packages and optional Snowflake connection
```

---

## File Roles (Quick Reference)

| File | Role |
|------|------|
| **ask_books.py** | Entry point for “ask and get one answer”; uses snowflake_retriever + personal_mistral. |
| **load_books_to_snowflake.py** | Partition PDFs (Unstructured), chunk by_title, insert staging → book_embeddings with AI_EMBED. |
| **snowflake_retriever.py** | Implements similarity_search over book_embeddings so RAG can use Snowflake as the vector store. |
| **mistral_snowflake_agent.py** | personal_mistral (RAG), personal_mistral_snowflake (SQL gen + run), mistral_csv (pandas agent). |
| **snowflake_helper.py** | Generic Snowflake run-SQL helper; used by retriever and agent. |
| **schema.sql** | Defines book_chunks_staging and book_embeddings; run once in BOOKS_DB.BOOKS. |
| **snowflake_startup.py** | Create warehouse/db/schema if missing. |
| **snowflake_teardown.py** | Drop project db/warehouse. |
| **verify_setup.py** | Verify deps and optional Snowflake connectivity. |
| **queries_to_workbook.py** | Turn docs/queries.md into docs/workbook.ipynb for Snowsight. |

---

## Data Flow

1. **Ingest:** PDFs in `books_pdf_folder/` → `load_books_to_snowflake.py` → Unstructured partition + chunk → insert into `book_chunks_staging` → `INSERT INTO book_embeddings SELECT ..., AI_EMBED(...) FROM book_chunks_staging` (same DB/schema as schema.sql).
2. **Query (SQL):** Run queries from `docs/queries.md` or `docs/workbook.ipynb` against `book_embeddings`.
3. **Query (Chat):** `ask_books.py` → Snowflake retriever similarity_search → top-k chunks → personal_mistral(question, retriever) → one answer (+ optional sources).

---

## Dependencies

- **requirements.txt** – Full stack: `unstructured[pdf]`, `snowflake-connector-python`, `langchain`, `langchain-community`, `langchain-experimental`, `pandas`, `python-dotenv`. Needed for load + agent + ask_books.
- **requirements-agent-only.txt** – Same minus Unstructured; use when you only run the agent / ask_books (no PDF load). Much smaller venv.

---

## Cleanup Done

- **load_books.log** – Removed from repo (runtime log; `*.log` added to `.gitignore`).
- **scripts/unstructured-setup.md** – Moved to **docs/unstructured-setup.md** so all docs live under `docs/`; README and verify_setup.py references updated.

---

## Optional Next Steps

- Add **author** extraction from PDF metadata in the loader if you want it populated in `book_embeddings`.
- Add **.cursorignore** with `.venv/` (and optionally `books_pdf_folder/`) if you want to reduce Cursor indexing.
- Regenerate **workbook.ipynb** after editing `docs/queries.md`: `python scripts/queries_to_workbook.py`.
