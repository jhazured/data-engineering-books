#!/usr/bin/env python3
"""
Verify setup: check Python packages and optional Snowflake connectivity.
Run from repo root: python scripts/verify_setup.py
"""

import sys
import os

# Ensure we can resolve repo root and scripts (run from repo root: python scripts/verify_setup.py)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

# Auto-load .env before Snowflake check if python-dotenv is available
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(REPO_ROOT, ".env"))
except ImportError:
    pass


def check_packages():
    """Check required packages are installed."""
    required = [
        ("unstructured.partition.pdf", "unstructured[pdf]"),
        ("pypdf", "pypdf"),
        ("snowflake.connector", "snowflake-connector-python"),
        ("pandas", "pandas"),
    ]
    optional = [
        ("langchain_core.documents", "langchain-core"),
    ]
    all_ok = True
    for mod, pkg in required:
        try:
            __import__(mod)
            if mod == "unstructured.partition.pdf":
                import unstructured
                print(f"  OK  {pkg} ({getattr(unstructured, '__version__', '?')})")
            else:
                print(f"  OK  {pkg}")
        except ImportError:
            print(f"  MISSING  {pkg}  (pip install {pkg})")
            all_ok = False
    for mod, pkg in optional:
        try:
            __import__(mod)
            print(f"  OK  {pkg} (optional, for retriever)")
        except ImportError:
            print(f"  -   {pkg} (optional, for ask_books retriever)")
    return all_ok


def check_tesseract():
    """Optional: tesseract enables hi_res PDF partitioning (better chunk quality)."""
    import shutil
    if shutil.which("tesseract"):
        print("  OK  tesseract (optional, for hi_res PDF chunking)")
    else:
        print("  -   tesseract not installed (optional; install for better PDF structure detection)")
        print("      See docs/unstructured-setup.md — macOS: brew install tesseract poppler")
    return True


def check_env():
    """Check .env exists and has placeholders replaced (basic check)."""
    env_path = os.path.join(REPO_ROOT, ".env")
    if not os.path.isfile(env_path):
        print("  -   .env not found (copy .env.example to .env and set credentials)")
        return False
    with open(env_path) as f:
        content = f.read()
    if "YOUR_USER" in content or "YOUR_PASSWORD" in content or "YOUR_ACCOUNT" in content:
        print("  WARN .env exists but contains placeholders; replace with your credentials")
        return False
    print("  OK  .env present (credentials set)")
    return True


def check_cortex_model():
    """Check Cortex COMPLETE model (agent uses SNOWFLAKE.CORTEX.COMPLETE)."""
    model = os.environ.get("CORTEX_MODEL", "").strip()
    if not model:
        print("  OK  CORTEX_MODEL not set (agent will use default: mistral-large2)")
    else:
        print(f"  OK  CORTEX_MODEL={model}")
    return True


def check_cortex_complete():
    """Optional: try Cortex COMPLETE() if Snowflake is configured (smoke test)."""
    try:
        import snowflake_helper
        cfg = snowflake_helper._get_config()
        if cfg.get("account") in (None, "", "YOUR_ACCOUNT") or not cfg.get("warehouse"):
            print("  -   Cortex COMPLETE: not configured (set SNOWFLAKE_* and SNOWFLAKE_WAREHOUSE to verify)")
            return True
        model = os.environ.get("CORTEX_MODEL", "mistral-large2")
        sql = "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s)"
        rows = snowflake_helper.snowflake_run_new(sql, params=(model, "Reply with exactly: OK"))
        if rows and len(rows) > 0 and rows[0][0]:
            print("  OK  Cortex COMPLETE() smoke test passed")
        else:
            print("  WARN Cortex COMPLETE() returned empty (check CORTEX_USER and warehouse)")
        return True
    except Exception as e:
        print(f"  WARN Cortex COMPLETE(): {e}")
        return True


def check_snowflake_connection():
    """Optional: try Snowflake connection if snowflake_helper is available."""
    try:
        import snowflake_helper
        import snowflake.connector
        cfg = snowflake_helper._get_config()
        if cfg.get("account") in (None, "", "YOUR_ACCOUNT"):
            print("  -   Snowflake: not configured (set SNOWFLAKE_* in .env to verify connection)")
            return True
        conn = snowflake.connector.connect(**cfg)
        conn.close()
        print("  OK  Snowflake connection successful")
        return True
    except Exception as e:
        print(f"  WARN Snowflake connection: {e}")
        return True  # Don't fail verify for optional check


def main():
    print("Verify setup")
    print("-" * 40)
    print("Packages:")
    pkgs_ok = check_packages()
    print("\nOptional (Tesseract for PDF quality):")
    check_tesseract()
    print("\nConfiguration:")
    check_env()
    print("\nOptional (Cortex COMPLETE agent):")
    check_cortex_model()
    print("\nOptional (Snowflake):")
    check_snowflake_connection()
    print("\nOptional (Cortex COMPLETE smoke test):")
    check_cortex_complete()
    print("-" * 40)
    if not pkgs_ok:
        print("Fix missing packages, then run again.")
        return 1
    print("Setup verification complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
