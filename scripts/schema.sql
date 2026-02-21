-- Run in Snowflake (BOOKS_DB / BOOKS schema) to create tables for semantic search.
-- Uses snowflake-arctic-embed-m-v1.5 (768 dimensions). Requires CORTEX_EMBED_USER (or CORTEX_USER).

USE DATABASE BOOKS_DB;
USE SCHEMA BOOKS;

-- Staging: text chunks only. Embeddings are computed in Snowflake via AI_EMBED.
CREATE TABLE IF NOT EXISTS book_chunks_staging (
  book_id         VARCHAR,
  author          VARCHAR,
  section_title   VARCHAR,
  content         VARCHAR,
  page_number     INT,
  chunk_index     INT
);

-- Final table: chunks + vector for VECTOR_COSINE_SIMILARITY with AI_EMBED at query time.
CREATE TABLE IF NOT EXISTS book_embeddings (
  book_id         VARCHAR,
  author          VARCHAR,
  section_title   VARCHAR,
  content         VARCHAR,
  page_number     INT,
  chunk_index     INT,
  vector          VECTOR(FLOAT, 768)
);

-- After loading into book_chunks_staging, run:
-- INSERT INTO book_embeddings (book_id, author, section_title, content, page_number, chunk_index, vector)
-- SELECT book_id, author, section_title, content, page_number, chunk_index,
--        AI_EMBED('snowflake-arctic-embed-m-v1.5', content) AS vector
-- FROM book_chunks_staging;
