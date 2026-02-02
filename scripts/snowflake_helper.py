"""
Snowflake helper: run SQL and return results.
Used by mistral_snowflake_agent.py for Snowflake SQL execution.
Configure via environment variables or a config dict.
"""

import os
from typing import Any, List, Optional

try:
    import snowflake.connector
except ImportError:
    snowflake = None


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


def snowflake_run_new(sql: str, config: Optional[dict] = None) -> List[Any]:
    """
    Execute SQL in Snowflake and return result rows.
    """
    if snowflake is None:
        raise ImportError("snowflake-connector-python is required. pip install snowflake-connector-python")
    cfg = config or _get_config()
    conn = snowflake.connector.connect(**cfg)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        conn.close()
