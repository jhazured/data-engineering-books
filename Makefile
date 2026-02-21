# Convenience targets for common tasks. Run from repo root.
.PHONY: load verify test workbook teardown dry-run

load:
	python scripts/load_books_to_snowflake.py --mode incremental

dry-run:
	python scripts/load_books_to_snowflake.py --dry-run

verify:
	python scripts/verify_setup.py

test:
	pytest tests/ -v --tb=short

workbook:
	python scripts/queries_to_workbook.py

teardown:
	python scripts/snowflake_teardown.py
