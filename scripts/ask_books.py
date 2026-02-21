#!/usr/bin/env python3
"""
Ask a question and get a single, considered answer based on your book embeddings in Snowflake
(ChatGPT/Claude-style). Uses semantic search over book_embeddings and the Mistral RAG chain.

Usage:
  python scripts/ask_books.py "How does exactly-once delivery work in streaming?"
  python scripts/ask_books.py "What is the star schema?"

Requires: SNOWFLAKE_* and HUGGINGFACEHUB_API_TOKEN (and optionally MISTRAL_REPO_ID) in .env or env.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Project root on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    from scripts.snowflake_retriever import get_retriever
    from scripts.mistral_snowflake_agent import personal_mistral
except ImportError:
    get_retriever = None  # type: ignore
    personal_mistral = None  # type: ignore


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python scripts/ask_books.py \"Your question?\"", file=sys.stderr)
        return 1

    question = " ".join(sys.argv[1:]).strip()
    if not question:
        print("Usage: python scripts/ask_books.py \"Your question?\"", file=sys.stderr)
        return 1

    if get_retriever is None or personal_mistral is None:
        print("Error: scripts.snowflake_retriever and scripts.mistral_snowflake_agent are required.", file=sys.stderr)
        return 1

    config = {
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE", "BOOKS_DB"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", "BOOKS"),
    }
    retriever = get_retriever(config=config)
    docs = retriever.similarity_search(question, k=5)

    if not docs:
        print("No relevant chunks found in book_embeddings. Check that you've run load_books_to_snowflake.py.", file=sys.stderr)
        return 1

    answer = personal_mistral(question, retriever, docs=docs)
    print(answer)

    # Optional: print sources (book + section)
    if docs and hasattr(docs[0], "metadata"):
        seen = set()
        sources = []
        for d in docs:
            m = getattr(d, "metadata", {}) or {}
            key = (m.get("book_id"), m.get("section_title"))
            if key not in seen and (key[0] or key[1]):
                seen.add(key)
                sources.append(f"  - {m.get('book_id', '')} | {m.get('section_title', '') or '(no section)'}")
        if sources:
            print("\nSources:", *sources, sep="\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
