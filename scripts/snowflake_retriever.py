"""
Snowflake-backed retriever for book_embeddings.
Exposes similarity_search(query, k) so LangChain RAG (e.g. personal_mistral) can use
Snowflake book_embeddings as the vector store.
"""

from __future__ import annotations

from typing import Any, List, Optional

try:
    from langchain_core.documents import Document
except ImportError:
    Document = None  # type: ignore

try:
    from scripts import snowflake_helper
except ImportError:
    import snowflake_helper

EMBED_MODEL = "snowflake-arctic-embed-m-v1.5"
TABLE = "book_embeddings"


def _run_vector_search(query: str, k: int = 5, config: Optional[dict] = None) -> List[tuple]:
    """Return rows (book_id, section_title, content, page_number, similarity_score) for top-k by similarity."""
    # Bind only the query; model and LIMIT are safe literals (k is integer we control).
    k = max(1, min(k, 20))
    sql = f"""
        SELECT book_id, section_title, content, page_number,
               VECTOR_COSINE_SIMILARITY(AI_EMBED('{EMBED_MODEL}', %s), vector) AS similarity_score
        FROM {TABLE}
        ORDER BY similarity_score DESC
        LIMIT {k}
    """
    rows = snowflake_helper.snowflake_run_new(sql, params=(query,), config=config)
    return rows if isinstance(rows, list) else []


class SnowflakeBookRetriever:
    """
    Retriever that uses Snowflake book_embeddings for semantic search.
    Compatible with LangChain's VectorStoreRetriever interface (similarity_search).
    """

    def __init__(self, config: Optional[dict] = None):
        self.config = config

    def similarity_search(self, query: str, k: int = 5, **kwargs: Any) -> List[Any]:
        """
        Return top-k chunks as LangChain Documents (page_content, metadata).
        TODO: support filter kwarg for book_id/author filtering.
        So personal_mistral(question, this_retriever) works for RAG over your books.
        """
        rows = _run_vector_search(query, k=k, config=self.config)
        # row: (book_id, section_title, content, page_number, similarity_score)
        def meta(row):
            return {
                "book_id": row[0],
                "section_title": row[1] or "",
                "page_number": row[3],
                "similarity_score": row[4] if len(row) > 4 else None,
            }
        if Document:
            return [Document(page_content=row[2] or "", metadata=meta(row)) for row in rows]
        # Fallback: simple object with .page_content and .metadata for personal_mistral
        class Doc:
            def __init__(self, page_content: str, metadata: dict):
                self.page_content = page_content
                self.metadata = metadata
        return [Doc(row[2] or "", meta(row)) for row in rows]


def get_retriever(config: Optional[dict] = None) -> SnowflakeBookRetriever:
    """Return a retriever instance for use with personal_mistral(question, retriever)."""
    return SnowflakeBookRetriever(config=config)
