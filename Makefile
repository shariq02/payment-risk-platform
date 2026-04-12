# Payment Risk Platform - Makefile
# Common commands for development workflow

.PHONY: help setup bronze dbt-run dbt-test dbt-snapshot dbt-docs api lint test clean

help:
	@echo "Payment Risk Platform - Available Commands"
	@echo "==========================================="
	@echo "setup          - Install Python dependencies"
	@echo "bronze         - Load CSV files into bronze schema"
	@echo "dbt-run        - Run all dbt models"
	@echo "dbt-test       - Run all dbt tests"
	@echo "dbt-snapshot   - Run dbt snapshots (SCD2)"
	@echo "dbt-docs       - Generate and serve dbt documentation"
	@echo "api            - Start FastAPI server"
	@echo "lint           - Run ruff linter"
	@echo "test           - Run pytest"
	@echo "clean          - Remove logs and dbt artifacts"

setup:
	pip install -r requirements.txt

bronze:
	python3.11 ingestion/loader/load_bronze.py

dbt-run:
	cd dbt && dbt run --profiles-dir ~/.dbt --project-dir .

dbt-test:
	cd dbt && dbt test --profiles-dir ~/.dbt --project-dir .

dbt-snapshot:
	cd dbt && dbt snapshot --profiles-dir ~/.dbt --project-dir .

dbt-docs:
	cd dbt && dbt docs generate --profiles-dir ~/.dbt --project-dir .
	cd dbt && dbt docs serve --profiles-dir ~/.dbt --project-dir .

api:
	cd apps/risk_api && uvicorn main:app --reload --port 8000

lint:
	ruff check .

test:
	pytest tests/ -v

clean:
	rm -rf logs/*.log
	rm -rf dbt/target/
	rm -rf dbt/dbt_packages/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
