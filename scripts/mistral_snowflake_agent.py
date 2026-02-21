"""
RAG and Q&A using Snowflake Cortex COMPLETE() — no external LLM or API token.
Uses snowflake_helper.snowflake_run_new() to run SELECT SNOWFLAKE.CORTEX.COMPLETE(model, prompt).
Requires CORTEX_USER (or equivalent) and a running warehouse (SNOWFLAKE_WAREHOUSE in .env).
"""

import os
from typing import Any

try:
    from scripts import snowflake_helper
except ImportError:
    import snowflake_helper

# Overridable via env; must be a Cortex COMPLETE model name (e.g. mistral-large2, mixtral-8x7b, snowflake-arctic).
CORTEX_MODEL = os.getenv("CORTEX_MODEL", "mistral-large2")

_RAG_SYSTEM = (
    "Answer the question based only on the following context. "
    "If the context does not contain enough information, say so."
)


def _cortex_complete(prompt: str, config: Any = None) -> str:
    """Call SNOWFLAKE.CORTEX.COMPLETE(model, prompt); return response string."""
    sql = "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s)"
    rows = snowflake_helper.snowflake_run_new(sql, params=(CORTEX_MODEL, prompt), config=config)
    if not rows or not isinstance(rows, list):
        return ""
    row = rows[0]
    return (row[0] or "").strip() if row else ""


def _run_rag(question: str, context_str: str, config: Any = None) -> str:
    """Build RAG prompt and call Cortex COMPLETE."""
    prompt = f"{_RAG_SYSTEM}\n\nContext:\n{context_str}\n\nQuestion: {question}\n\nAnswer:"
    return _cortex_complete(prompt, config=config)


def ask_mistral(question: str, config: Any = None) -> str:
    """Simple Q&A (no context)."""
    return _cortex_complete(question, config=config)


def personal_mistral(question: str, db: Any, docs: Any = None, config: Any = None) -> str:
    """RAG: answer using your book chunks from a vector DB. If docs is provided, use them (one less Snowflake round-trip)."""
    if docs is None:
        docs = db.similarity_search(query=question, k=4)
    else:
        docs = docs[:4] if len(docs) > 4 else docs
    context_str = "\n".join([getattr(doc, "page_content", str(doc)) for doc in docs])
    return _run_rag(question, context_str, config=config)


# personal_mistral_snowflake and mistral_csv removed: SQL generation executed LLM output against
# Snowflake with no sandboxing (security risk); mistral_csv depended on langchain-experimental.
# Use ask_books.py + personal_mistral for RAG over book_embeddings, or run COMPLETE() in SQL directly.
