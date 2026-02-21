# Common Queries

Useful queries for semantic search over the book embeddings. Run these in a Snowflake worksheet or via the Snowflake CLI.

> **Note**: With the improved chunking strategy (larger chunks + overlap + section context), these queries use natural language phrasing instead of keyword stuffing for better semantic search results.

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
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'How do exactly-once delivery semantics work?'), 
    vector
) DESC
LIMIT 5;
```

---

## Filter by author

Search within a specific book:
```sql
SELECT section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
WHERE book_id ILIKE '%kleppmann%' OR author ILIKE '%kleppmann%'
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'write-ahead logging'), 
    vector
) DESC
LIMIT 5;
```

---

## Compare across books

Find how different authors cover the same topic:
```sql
SELECT book_id, author, section_title, LEFT(content, 300) AS content_preview
FROM book_embeddings
WHERE VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'What are the tradeoffs between batch and stream processing?'), 
    vector
) > 0.65
ORDER BY book_id, VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'What are the tradeoffs between batch and stream processing?'), 
    vector
) DESC;
```

---

## Topic-specific queries

### Data modeling
```sql
-- Dimensional modeling (Kimball)
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'What is a star schema in data warehousing?'), 
    vector
) DESC
LIMIT 5;

-- Slowly changing dimensions
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'How do you handle slowly changing dimensions?'), 
    vector
) DESC
LIMIT 5;

-- Data vault modeling
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'What is data vault modeling?'), 
    vector
) DESC
LIMIT 5;
```

### Streaming
```sql
-- Event time vs processing time
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'How do watermarks handle late-arriving events?'), 
    vector
) DESC
LIMIT 5;

-- Exactly-once semantics
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'How do you achieve exactly-once delivery guarantees?'), 
    vector
) DESC
LIMIT 5;

-- Windowing strategies
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'What are tumbling and sliding windows in stream processing?'), 
    vector
) DESC
LIMIT 5;
```

### Distributed systems
```sql
-- CAP theorem
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'Explain the CAP theorem and its tradeoffs'), 
    vector
) DESC
LIMIT 5;

-- Replication strategies
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'How does leader-follower replication work?'), 
    vector
) DESC
LIMIT 5;

-- Consensus algorithms
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'How do Paxos and Raft consensus algorithms work?'), 
    vector
) DESC
LIMIT 5;

-- Partition tolerance
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'How do distributed systems handle network partitions?'), 
    vector
) DESC
LIMIT 5;
```

### ETL and pipelines
```sql
-- ETL vs ELT
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'What is the difference between ETL and ELT?'), 
    vector
) DESC
LIMIT 5;

-- Data quality
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'How do you validate data quality in pipelines?'), 
    vector
) DESC
LIMIT 5;

-- Incremental processing
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'How do you implement incremental data processing?'), 
    vector
) DESC
LIMIT 5;
```

### Spark
```sql
-- Partitioning and shuffles
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'How does Spark handle partition shuffling and data skew?'), 
    vector
) DESC
LIMIT 5;

-- Catalyst optimizer
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'How does the Catalyst optimizer work in Spark?'), 
    vector
) DESC
LIMIT 5;

-- RDD vs DataFrame vs Dataset
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'What are the differences between RDD, DataFrame, and Dataset?'), 
    vector
) DESC
LIMIT 5;
```

---

## Interview prep queries
```sql
-- "Explain the Lambda architecture"
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'What is the Lambda architecture for data processing?'), 
    vector
) DESC
LIMIT 5;

-- "How do you handle late-arriving data?"
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'How do you handle late-arriving data in streaming systems?'), 
    vector
) DESC
LIMIT 5;

-- "What is a data lakehouse?"
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'What is a data lakehouse architecture?'), 
    vector
) DESC
LIMIT 5;

-- "Explain data partitioning strategies"
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'What are common data partitioning strategies in distributed systems?'), 
    vector
) DESC
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

-- Average chunk length
SELECT 
    book_id,
    AVG(LENGTH(content)) AS avg_chunk_length,
    MIN(LENGTH(content)) AS min_chunk_length,
    MAX(LENGTH(content)) AS max_chunk_length
FROM book_embeddings
GROUP BY book_id
ORDER BY avg_chunk_length DESC;
```

---

## Kafka and event-driven
```sql
-- Consumer groups and offsets
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'How do Kafka consumer groups manage offsets?'), 
    vector
) DESC
LIMIT 5;

-- Event sourcing vs CDC
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'What is the difference between event sourcing and change data capture?'), 
    vector
) DESC
LIMIT 5;

-- Kafka partitioning strategies
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'How does Kafka partition messages and maintain ordering?'), 
    vector
) DESC
LIMIT 5;

-- Kafka delivery guarantees
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'What are Kafka delivery guarantees: at-most-once, at-least-once, exactly-once?'), 
    vector
) DESC
LIMIT 5;
```

---

## Data warehouse architecture
```sql
-- Kimball dimensional modeling
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'What is Kimball dimensional modeling with conformed dimensions?'), 
    vector
) DESC
LIMIT 5;

-- Fact table grain
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'How do you define the grain of a fact table?'), 
    vector
) DESC
LIMIT 5;

-- Surrogate keys
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'What are surrogate keys versus natural keys in dimensions?'), 
    vector
) DESC
LIMIT 5;

-- Aggregate tables
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'How do you design aggregate fact tables for performance?'), 
    vector
) DESC
LIMIT 5;
```

---

## Cloud and modern stack
```sql
-- Data lake organization
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'What is the medallion architecture for data lakes?'), 
    vector
) DESC
LIMIT 5;

-- Object storage patterns
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'How do you organize Parquet files in S3 for optimal performance?'), 
    vector
) DESC
LIMIT 5;

-- Table formats
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'What are Delta Lake, Iceberg, and Hudi table formats?'), 
    vector
) DESC
LIMIT 5;
```

---

## Performance and optimization
```sql
-- Query optimization
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'How do you optimize slow database queries?'), 
    vector
) DESC
LIMIT 5;

-- Data skew
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'How do you handle data skew in distributed processing?'), 
    vector
) DESC
LIMIT 5;

-- Caching strategies
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'What are effective caching strategies for data pipelines?'), 
    vector
) DESC
LIMIT 5;

-- Indexing strategies
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'What types of database indexes should I use?'), 
    vector
) DESC
LIMIT 5;
```

---

## Reliability and operations
```sql
-- Idempotency
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'How do you make data pipelines idempotent?'), 
    vector
) DESC
LIMIT 5;

-- Backfilling
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'How do you backfill historical data safely?'), 
    vector
) DESC
LIMIT 5;

-- Schema evolution
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'How do you handle schema evolution with backward compatibility?'), 
    vector
) DESC
LIMIT 5;

-- Monitoring and alerting
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'What should you monitor in data pipelines?'), 
    vector
) DESC
LIMIT 5;
```

---

## Concepts you'll get asked about
```sql
-- "What's the difference between OLTP and OLAP?"
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'What is the difference between OLTP and OLAP databases?'), 
    vector
) DESC
LIMIT 5;

-- "Explain normalization vs denormalization"
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'When should you normalize versus denormalize data?'), 
    vector
) DESC
LIMIT 5;

-- "How do distributed transactions work?"
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'How does two-phase commit work in distributed transactions?'), 
    vector
) DESC
LIMIT 5;

-- "What is eventual consistency?"
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'What is eventual consistency versus strong consistency?'), 
    vector
) DESC
LIMIT 5;

-- "Explain ACID properties"
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'What are ACID properties in database transactions?'), 
    vector
) DESC
LIMIT 5;

-- "What is BASE in distributed systems?"
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'What is BASE in distributed systems?'), 
    vector
) DESC
LIMIT 5;
```

---

## Debugging and troubleshooting

Useful when you're stuck on something at work:
```sql
-- Generic "why is my job slow"
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'Why is my data pipeline running slowly?'), 
    vector
) DESC
LIMIT 5;

-- Spark memory issues
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'How do I fix Spark out of memory errors?'), 
    vector
) DESC
LIMIT 5;

-- Data pipeline failures
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'How do you recover from pipeline failures?'), 
    vector
) DESC
LIMIT 5;

-- Duplicate data issues
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'Why am I seeing duplicate records in my pipeline?'), 
    vector
) DESC
LIMIT 5;
```

---

## Hybrid search (keyword + semantic)

For best results when you know specific technical terms:
```sql
-- Combine keyword filtering with semantic ranking
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
WHERE LOWER(content) LIKE '%replication%' 
   OR LOWER(section_title) LIKE '%replication%'
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'How does leader-follower replication work?'), 
    vector
) DESC
LIMIT 10;

-- Find content about Kafka specifically
SELECT book_id, section_title, LEFT(content, 500) AS content_preview
FROM book_embeddings
WHERE LOWER(content) LIKE '%kafka%'
ORDER BY VECTOR_COSINE_SIMILARITY(
    AI_EMBED('snowflake-arctic-embed-m-v1.5', 'How does Kafka achieve high throughput?'), 
    vector
) DESC
LIMIT 10;
```

---

## Tips for better results

### Query phrasing guidelines
- **✅ Use natural questions**: "How does X work?", "What is the difference between X and Y?"
- **✅ Be specific about context**: "How do you handle data skew in Spark?" not just "data skew"
- **❌ Avoid keyword stuffing**: Not "ETL ELT extract transform load" 
- **❌ Don't repeat synonyms**: Not "leader follower replication consensus"

### Content preview
- Most queries use `LEFT(content, 500)` to preview first 500 characters
- Adjust based on your needs: use full `content` if you want complete chunks
- Or reduce to `LEFT(content, 200)` for quick scanning

### Similarity thresholds
- Typical good matches: 0.65-0.85 similarity score
- Lower threshold (0.60) catches more results but may include less relevant content
- Higher threshold (0.75) ensures quality but may miss relevant passages
- Experiment with your specific dataset

### Combining with filters
- Use `WHERE book_id = '...'` to search within a specific book
- Use `WHERE section_title ILIKE '%chapter%'` to narrow by section
- Combine keyword `LIKE` filters with semantic search for precision

### Performance notes
- Vector similarity is computationally expensive
- Add a `LIMIT` clause to every query (typically 5-10 results)
- For production use, consider creating a similarity score threshold to filter before sorting
