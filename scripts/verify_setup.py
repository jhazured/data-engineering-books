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
        ("pdfplumber", "pdfplumber"),
        ("snowflake.connector", "snowflake-connector-python"),
        ("pandas", "pandas"),
    ]
    optional = [
        ("langchain_community.llms", "langchain-community"),
        ("langchain_experimental.agents", "langchain-experimental"),
    ]
    all_ok = True
    for mod, pkg in required:
        try:
            __import__(mod)
            if mod == "pdfplumber":
                import pdfplumber
                print(f"  OK  {pkg} ({getattr(pdfplumber, '__version__', '?')})")
            else:
                print(f"  OK  {pkg}")
        except ImportError:
            print(f"  MISSING  {pkg}  (pip install {pkg})")
            all_ok = False
    for mod, pkg in optional:
        try:
            __import__(mod)
            print(f"  OK  {pkg} (optional)")
        except ImportError:
            print(f"  -   {pkg} (optional, for Mistral agent)")
    return all_ok


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


def check_huggingface():
    """Check Hugging Face token (needed for Mistral agent)."""
    token = os.environ.get("HUGGINGFACEHUB_API_TOKEN", "")
    if token and not token.startswith("hf_"):
        print("  WARN HUGGINGFACEHUB_API_TOKEN set but doesn't look like a valid token (expect hf_...)")
    elif token:
        print("  OK  HUGGINGFACEHUB_API_TOKEN set")
    else:
        print("  -   HUGGINGFACEHUB_API_TOKEN not set (needed for Mistral agent)")
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
    print("\nConfiguration:")
    check_env()
    print("\nOptional (Hugging Face):")
    check_huggingface()
    print("\nOptional (Snowflake):")
    check_snowflake_connection()
    print("-" * 40)
    if not pkgs_ok:
        print("Fix missing packages, then run again.")
        return 1
    print("Setup verification complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
