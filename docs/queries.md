# Common Queries

Useful queries for semantic search over the book embeddings. Run these in a Snowflake worksheet or via the Snowflake CLI.

## Quick reference

| Category | Jump to |
|----------|---------|
| Basics | [Basic search](#basic-semantic-search) · [Filter by author](#filter-by-author) · [Compare books](#compare-across-books) |
| Topics | [Data modeling](#data-modeling) · [Streaming](#streaming) · [Distributed systems](#distributed-systems) · [ETL](#etl-and-pipelines) · [Spark](#spark) |
| Kafka | [Event-driven](#kafka-and-event-driven) |
| Architecture | [Data warehouse](#data-warehouse-architecture) · [Cloud/modern](#cloud-and-modern-stack) |
| Operations | [Performance](#performance-and-optimization) · [Reliability](#reliability-and-operations) · [Debugging](#debugging-and-troubleshooting) |
| Interview | [Interview prep](#interview-prep-queries) · [Common concepts](#concepts-youll-get-asked-about) |
| Meta | [Aggregate stats](#aggregate-stats) |

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

## Kafka and event-driven
```sql
-- Consumer groups and offsets
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('kafka consumer group offset commit'), vector) DESC
LIMIT 5;

-- Event sourcing vs CDC
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('event sourcing change data capture CDC'), vector) DESC
LIMIT 5;

-- Kafka partitioning strategies
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('kafka partition key ordering guarantees'), vector) DESC
LIMIT 5;
```

---

## Data warehouse architecture
```sql
-- Kimball vs Inmon (you'll mainly get Kimball, but useful to see what's covered)
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('enterprise data warehouse dimensional modeling conformed dimensions'), vector) DESC
LIMIT 5;

-- Fact table grain
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('fact table grain atomic transaction'), vector) DESC
LIMIT 5;

-- Surrogate keys
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('surrogate key natural key dimension'), vector) DESC
LIMIT 5;
```

---

## Cloud and modern stack
```sql
-- Data lake organization
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('data lake bronze silver gold medallion'), vector) DESC
LIMIT 5;

-- Object storage patterns
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('S3 object storage parquet partitioning'), vector) DESC
LIMIT 5;
```

---

## Performance and optimization
```sql
-- Query optimization
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('query optimization indexing predicate pushdown'), vector) DESC
LIMIT 5;

-- Data skew
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('data skew hot partition salting'), vector) DESC
LIMIT 5;

-- Caching strategies
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('caching materialized view precompute'), vector) DESC
LIMIT 5;
```

---

## Reliability and operations
```sql
-- Idempotency
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('idempotent operation retry deduplication'), vector) DESC
LIMIT 5;

-- Backfilling
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('backfill historical data reprocessing'), vector) DESC
LIMIT 5;

-- Schema evolution
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('schema evolution backward forward compatibility'), vector) DESC
LIMIT 5;
```

---

## Concepts you'll get asked about
```sql
-- "What's the difference between OLTP and OLAP?"
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('OLTP OLAP transactional analytical workload'), vector) DESC
LIMIT 5;

-- "Explain normalization vs denormalization"
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('normalization denormalization third normal form'), vector) DESC
LIMIT 5;

-- "How do distributed transactions work?"
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('two-phase commit distributed transaction coordinator'), vector) DESC
LIMIT 5;

-- "What is eventual consistency?"
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('eventual consistency strong consistency linearizable'), vector) DESC
LIMIT 5;
```

---

## Debugging and troubleshooting

Useful when you're stuck on something at work:
```sql
-- Generic "why is my job slow"
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('performance bottleneck slow job optimization'), vector) DESC
LIMIT 5;

-- Spark memory issues
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('spark out of memory executor driver spill'), vector) DESC
LIMIT 5;

-- Data pipeline failures
SELECT book_id, section_title, content
FROM book_embeddings
ORDER BY VECTOR_SIMILARITY(AI_EMBED_TEXT('pipeline failure recovery checkpoint restart'), vector) DESC
LIMIT 5;
```

---

## Notes

- Adjust `LIMIT` based on how much context you need
- Similarity threshold (e.g., `> 0.7`) filters out weak matches but may miss relevant content — experiment with your data
- Combine with `section_title` filtering if you know the rough area (e.g., `WHERE section_title ILIKE '%Chapter 5%'`)
