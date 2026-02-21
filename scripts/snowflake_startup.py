#!/usr/bin/env python3
"""
Snowflake startup script: create warehouse, database, and schema if they don't exist.
Run once before load_books_to_snowflake.py (e.g. new account or fresh project).
Uses SNOWFLAKE_* from .env; see .env.example.

Run from repo root: python scripts/snowflake_startup.py
"""

import os
import sys

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPT_DIR)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(_REPO_ROOT, ".env"))
except ImportError:
    pass

try:
    import snowflake.connector
except ImportError:
    print("snowflake-connector-python is required. pip install snowflake-connector-python")
    sys.exit(1)

try:
    import snowflake_helper
    safe_id = snowflake_helper.safe_id
except ImportError:
    print("snowflake_helper not found. Run from repo root: python scripts/snowflake_startup.py")
    sys.exit(1)


def main():
    cfg = snowflake_helper._get_config()
    user = cfg.get("user", "")
    account = cfg.get("account", "")
    warehouse = (cfg.get("warehouse") or "").strip()
    database = (cfg.get("database") or "").strip()
    schema = (cfg.get("schema") or "PUBLIC").strip()

    if not warehouse or not database:
        print("Set SNOWFLAKE_WAREHOUSE and SNOWFLAKE_DATABASE in .env (see .env.example).")
        sys.exit(1)
    if account in (None, "", "YOUR_ACCOUNT") or user in (None, "", "YOUR_USER"):
        print("Set SNOWFLAKE_USER, SNOWFLAKE_ACCOUNT (and SNOWFLAKE_PASSWORD) in .env.")
        sys.exit(1)

    try:
        wh, db, sc = safe_id(warehouse), safe_id(database), safe_id(schema)
    except ValueError as e:
        print(e)
        sys.exit(1)

    conn = snowflake.connector.connect(**cfg)
    try:
        cur = conn.cursor()
        # Warehouse: create if not exists (may require ACCOUNTADMIN; skip if you use an existing warehouse)
        try:
            cur.execute(
                f"CREATE WAREHOUSE IF NOT EXISTS {wh} WITH WAREHOUSE_SIZE = 'X-SMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE"
            )
            print(f"  OK  Warehouse: {warehouse}")
        except snowflake.connector.errors.ProgrammingError as e:
            if "does not exist" in str(e) or "insufficient privileges" in str(e).lower():
                print(f"  -   Warehouse '{warehouse}' not created (use existing or create in UI): {e}")
            else:
                raise
        cur.execute(f"USE WAREHOUSE {wh}")

        # Database
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
        print(f"  OK  Database: {database}")
        cur.execute(f"USE DATABASE {db}")

        # Schema
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {sc}")
        print(f"  OK  Schema: {schema}")
        cur.execute(f"USE SCHEMA {sc}")

        cur.close()
        print("\nSnowflake startup complete. You can run: python scripts/load_books_to_snowflake.py")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
