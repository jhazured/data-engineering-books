#!/usr/bin/env python3
"""
Snowflake teardown script: drop the project warehouse and database (and all objects inside).
Uses SNOWFLAKE_* from .env; see .env.example. Run after you no longer need the BOOKS data.

Run from repo root: python scripts/snowflake_teardown.py [--force]
Use --force to skip the confirmation prompt.
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
except ImportError:
    print("snowflake_helper not found. Run from repo root: python scripts/snowflake_teardown.py")
    sys.exit(1)


def main():
    force = "--force" in sys.argv or "-f" in sys.argv
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

    def safe_id(name):
        s = (name or "").strip().upper()
        if not s or not all(c.isalnum() or c == "_" for c in s):
            raise ValueError("Warehouse/database/schema names must be alphanumeric or underscore.")
        return s

    try:
        wh, db, sc = safe_id(warehouse), safe_id(database), safe_id(schema)
    except ValueError as e:
        print(e)
        sys.exit(1)

    if not force:
        print(f"This will DROP (delete) the following in Snowflake:")
        print(f"  - Database: {database} (and all schemas/tables inside)")
        print(f"  - Warehouse: {warehouse}")
        print("This cannot be undone.")
        try:
            reply = input("Type 'yes' to continue: ").strip().lower()
        except EOFError:
            reply = ""
        if reply != "yes":
            print("Teardown cancelled.")
            sys.exit(0)

    conn = snowflake.connector.connect(**cfg)
    try:
        cur = conn.cursor()
        # Drop database and everything in it (schemas, tables, etc.)
        try:
            cur.execute(f"DROP DATABASE IF EXISTS {db} CASCADE")
            print(f"  OK  Dropped database: {database}")
        except snowflake.connector.errors.ProgrammingError as e:
            if "insufficient privileges" in str(e).lower():
                print(f"  -   Could not drop database '{database}' (insufficient privileges): {e}")
            else:
                raise

        # Drop warehouse (only the one we created for this project)
        try:
            cur.execute(f"DROP WAREHOUSE IF EXISTS {wh}")
            print(f"  OK  Dropped warehouse: {warehouse}")
        except snowflake.connector.errors.ProgrammingError as e:
            if "insufficient privileges" in str(e).lower():
                print(f"  -   Could not drop warehouse '{warehouse}' (insufficient privileges): {e}")
            else:
                raise

        cur.close()
        print("\nSnowflake teardown complete.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
