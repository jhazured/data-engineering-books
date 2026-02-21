# Enabling Snowflake Cortex AI for embeddings and COMPLETE

The book loader uses **Snowflake Cortex** to create vector embeddings via `AI_EMBED`. The Q&A agent (`ask_books.py`) uses **COMPLETE()** for RAG. If you see "Unknown function" or embeddings are skipped, enable Cortex as below. For `ask_books` you need **CORTEX_USER** (not just CORTEX_EMBED_USER) and a **running warehouse** (COMPLETE runs on the warehouse).

## 1. Grant Cortex privileges

Your user’s role needs one of these **database roles** in the `SNOWFLAKE` database:

- **`SNOWFLAKE.CORTEX_USER`** – full Cortex (LLMs, embeddings, etc.). Often already granted to `PUBLIC`.
- **`SNOWFLAKE.CORTEX_EMBED_USER`** – embedding-only (`AI_EMBED`, `EMBED_TEXT_768`, `EMBED_TEXT_1024`). Not granted to `PUBLIC` by default.

An account admin (e.g. `ACCOUNTADMIN`) can run:

```sql
USE ROLE ACCOUNTADMIN;

-- Option A: Give your role embedding access (replace YOUR_ROLE with the role your user uses)
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_EMBED_USER TO ROLE YOUR_ROLE;

-- Option B: Give all users embedding access
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_EMBED_USER TO ROLE PUBLIC;

-- Option C: If you prefer full Cortex (embeddings + LLMs), use CORTEX_USER
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE YOUR_ROLE;
```

To see which role your user has: in Snowsight go to **Admin → Users and Roles**, or run `SELECT CURRENT_ROLE();` in a worksheet.

## 2. Check account allowlist (optional)

Admins can restrict which Cortex models are allowed:

```sql
-- As ACCOUNTADMIN: allow all models (typical for dev)
ALTER ACCOUNT SET CORTEX_MODELS_ALLOWLIST = 'All';

-- Or allow only specific embedding models
ALTER ACCOUNT SET CORTEX_MODELS_ALLOWLIST = 'snowflake-arctic-embed-m-v1.5,snowflake-arctic-embed-l-v2.0';
```

If the allowlist is `'None'` or doesn’t include the embedding model, embeddings will fail until it’s updated.

## 3. Region and cloud

Cortex embedding functions are available in [specific regions](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions#regional-availability). If your account is in a region that doesn’t support `AI_EMBED` for text, you may need to use [cross-region inference](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cross-region-inference) or run in a supported region.

## 4. Verify

In a Snowflake worksheet (with a role that has Cortex access):

```sql
SELECT AI_EMBED('snowflake-arctic-embed-m-v1.5', 'test');
```

If this returns a VECTOR value, embeddings are enabled. Re-run the loader:

```bash
python scripts/load_books_to_snowflake.py
```

## Model used by this project

The loader uses **`snowflake-arctic-embed-m-v1.5`** by default (set in `scripts/load_books_to_snowflake.py` as `EMBEDDING_MODEL`). Other supported text models include:

- `snowflake-arctic-embed-l-v2.0`
- `snowflake-arctic-embed-l-v2.0-8k`
- `snowflake-arctic-embed-m`
- `multilingual-e5-large`
- `e5-base-v2`

See [Snowflake Cortex LLM functions](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions) for availability and costs.
