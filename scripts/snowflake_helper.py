"""
Snowflake helper: run SQL and return results.
Used by mistral_snowflake_agent.py for Snowflake SQL execution.
Configure via environment variables or a config dict.
"""

import os
from typing import Any, List, Optional, Tuple, Union

try:
    import snowflake.connector
except ImportError:
    snowflake = None


def safe_id(name: str) -> str:
    """Validate and normalize warehouse/database/schema name for safe use in DDL (alphanumeric + underscore)."""
    s = (name or "").strip().upper()
    if not s or not all(c.isalnum() or c == "_" for c in s):
        raise ValueError("Warehouse/database/schema names must be alphanumeric or underscore.")
    return s


def _get_config() -> dict:
    """Build Snowflake config from env or default placeholder."""
    return {
        "user": os.getenv("SNOWFLAKE_USER", "YOUR_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD", "YOUR_PASSWORD"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT", "YOUR_ACCOUNT"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "YOUR_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE", "YOUR_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
    }


def snowflake_run_new(
    sql: str,
    params: Optional[tuple] = None,
    config: Optional[dict] = None,
    include_headers: bool = False,
) -> Union[List[Any], Tuple[List[str], List[Any]]]:
    """
    Execute SQL in Snowflake and return result rows.
    Uses context manager for connection/cursor. Supports parameterized queries (params).
    If include_headers=True, returns (column_names, rows).
    """
    if snowflake is None:
        raise ImportError("snowflake-connector-python is required. pip install snowflake-connector-python")
    cfg = config or _get_config()
    with snowflake.connector.connect(**cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            rows = cur.fetchall()
            if include_headers and cur.description:
                columns = [desc[0] for desc in cur.description]
                return columns, rows
            return rows


# Alias for callers that expect run_sql
run_sql = snowflake_run_new
