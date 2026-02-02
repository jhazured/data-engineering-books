# Common Queries

Useful queries for semantic search over the book embeddings. Run these in a Snowflake worksheet or via the Snowflake CLI.

---

## Basic semantic search

Find chunks most relevant to a concept:
```sql
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('exactly-once delivery semantics'), vector) DESC
LIMIT 5;
```

---

## Filter by author

Search within a specific book:
```sql
SELECT section_title, content
FROM book_embeddings
WHERE book_id ILIKE '%kleppmann%' OR author ILIKE '%kleppmann%'
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('write-ahead logging'), vector) DESC
LIMIT 5;
```

---

## Compare across books

Find how different authors cover the same topic:
```sql
SELECT book_id, author, section_title, content
FROM book_embeddings
WHERE VECTOR_SIMILARITY(AI_EMBED_TEXT('batch vs stream processing'), vector) > 0.7
ORDER BY book_id, VECTOR_SIMILARITY(AI_EMBED_TEXT('batch vs stream processing'), vector) DESC;
```

---

## Topic-specific queries

### Data modeling
```sql
-- Dimensional modeling (Kimball)
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('star schema fact and dimension tables'), vector) DESC
LIMIT 5;

-- Slowly changing dimensions
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('slowly changing dimension type 2 SCD'), vector) DESC
LIMIT 5;
```

### Streaming
```sql
-- Event time vs processing time
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('event time processing time watermarks'), vector) DESC
LIMIT 5;

-- Exactly-once semantics
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('exactly-once delivery guarantees idempotent'), vector) DESC
LIMIT 5;
```

### Distributed systems
```sql
-- CAP theorem
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('CAP theorem consistency availability partition tolerance'), vector) DESC
LIMIT 5;

-- Replication strategies
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('leader follower replication consensus'), vector) DESC
LIMIT 5;
```

### ETL and pipelines
```sql
-- ETL vs ELT
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('ETL ELT extract transform load'), vector) DESC
LIMIT 5;

-- Data quality
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('data quality validation testing'), vector) DESC
LIMIT 5;
```

### Spark
```sql
-- Partitioning and shuffles
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('spark shuffle partition skew'), vector) DESC
LIMIT 5;

-- Catalyst optimizer
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('catalyst optimizer query plan'), vector) DESC
LIMIT 5;
```

---

## Interview prep queries
```sql
-- "Explain the Lambda architecture"
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('lambda architecture batch layer speed layer'), vector) DESC
LIMIT 5;

-- "How do you handle late-arriving data?"
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('late arriving data watermark trigger'), vector) DESC
LIMIT 5;

-- "What is a data lakehouse?"
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('lakehouse delta lake iceberg'), vector) DESC
LIMIT 5;
```

---

## Aggregate stats
```sql
-- Chunks per book
SELECT book_id, author, COUNT(*) AS chunk_count
FROM book_embeddings
GROUP BY book_id, author
ORDER BY chunk_count DESC;

-- Books by publication year
SELECT DISTINCT book_id, author, publication_year
FROM book_embeddings
WHERE publication_year IS NOT NULL
ORDER BY publication_year DESC;
```

---

## Notes

- Adjust `LIMIT` based on how much context you need
- Similarity threshold (e.g., `> 0.7`) filters out weak matches but may miss relevant content — experiment with your data
- Combine with `section_title` filtering if you know the rough area (e.g., `WHERE section_title ILIKE '%Chapter 5%'`)
