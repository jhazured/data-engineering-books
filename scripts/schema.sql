-- Run in Snowflake (BOOKS_DB / BOOKS schema) to create tables for semantic search.
-- Uses snowflake-arctic-embed-m-v1.5 (768 dimensions). Requires CORTEX_EMBED_USER (or CORTEX_USER).
-- If you already have these tables without publication_year or title, see ALTER block at bottom.

USE DATABASE BOOKS_DB;
USE SCHEMA BOOKS;

-- Staging: text chunks only. Embeddings are computed in Snowflake via AI_EMBED.
CREATE TABLE IF NOT EXISTS book_chunks_staging (
  book_id          VARCHAR,
  author           VARCHAR,
  publication_year INT,
  title            VARCHAR,
  section_title    VARCHAR,
  content          VARCHAR,
  page_number      INT,
  chunk_index      INT
);

-- Final table: chunks + vector for VECTOR_COSINE_SIMILARITY with AI_EMBED at query time.
-- Note: Snowflake has no traditional B-tree indexes; use clustering keys for large tables if needed.
CREATE TABLE IF NOT EXISTS book_embeddings (
  book_id          VARCHAR,
  author           VARCHAR,
  publication_year INT,
  title            VARCHAR,
  section_title    VARCHAR,
  content          VARCHAR,
  page_number      INT,
  chunk_index      INT,
  vector           VECTOR(FLOAT, 768)
);

-- If you ran schema.sql before publication_year/title were added, run (once):
-- ALTER TABLE book_chunks_staging ADD COLUMN publication_year INT;
-- ALTER TABLE book_embeddings ADD COLUMN publication_year INT;
-- ALTER TABLE book_chunks_staging ADD COLUMN title VARCHAR;
-- ALTER TABLE book_embeddings ADD COLUMN title VARCHAR;

-- After loading into book_chunks_staging, run:
-- INSERT INTO book_embeddings (book_id, author, publication_year, title, section_title, content, page_number, chunk_index, vector)
-- SELECT book_id, author, publication_year, title, section_title, content, page_number, chunk_index,
--        AI_EMBED('snowflake-arctic-embed-m-v1.5', content) AS vector
-- FROM book_chunks_staging;
