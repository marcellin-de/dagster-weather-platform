SHELL := /bin/bash

# ==============================================================================
# Configuration
# ==============================================================================

PROJECT_ROOT := $(CURDIR)
DBT_DIR := dbt/weather_dbt

DAGSTER_HOME ?= $(PROJECT_ROOT)/.dg
export DAGSTER_HOME
export DAGSTER_PROJECT_ROOT := $(PROJECT_ROOT)

UV := uv
UV_CACHE_DIR ?= $(PROJECT_ROOT)/.uv-cache
export UV_CACHE_DIR

# ==============================================================================
# Meta
# ==============================================================================

.PHONY: help sync bootstrap check-db-lock up list check dbt-deps dbt-run dbt-test dbt-build test lint format verify pipeline clean compose-up compose-down compose-logs compose-ps

help:
	@echo ""
	@echo "Dagster Weather Intelligence Platform (uv)"
	@echo ""
	@echo "Setup:"
	@echo "  sync           Install dependencies via uv"
	@echo "  bootstrap      Sync + dbt deps"
	@echo ""
	@echo "Dagster:"
	@echo "  up             Start Dagster dev server"
	@echo "  check-db-lock  Check write access to DuckDB file"
	@echo "  list           List Dagster definitions"
	@echo "  check          Validate Dagster definitions"
	@echo ""
	@echo "Docker Compose:"
	@echo "  compose-up     Build and start full platform (Dagster, DuckDB, MLflow, Evidence)"
	@echo "  compose-down   Stop and remove containers"
	@echo "  compose-logs   Tail all service logs"
	@echo "  compose-ps     Show service status"
	@echo ""
	@echo "dbt:"
	@echo "  dbt-deps       Install dbt packages"
	@echo "  dbt-run        Run models"
	@echo "  dbt-test       Run tests"
	@echo "  dbt-build      Run + test"
	@echo ""
	@echo "Quality:"
	@echo "  test           Run Python tests"
	@echo "  lint           Run ruff lint"
	@echo "  format         Format code"
	@echo ""
	@echo "Workflow:"
	@echo "  verify         check + test"
	@echo "  pipeline       Full local pipeline"
	@echo ""
	@echo "Maintenance:"
	@echo "  clean          Clean artifacts"
	@echo ""

# ==============================================================================
# Setup
# ==============================================================================

sync:
	$(UV) sync

bootstrap: sync dbt-deps

# ==============================================================================
# Dagster
# ==============================================================================

check-db-lock:
	@$(UV) run python -c "exec('''import duckdb\nfrom pathlib import Path\n\ndb_path = Path(\"src/weather_ingest.duckdb\")\nif not db_path.exists():\n    print(f\"[ok] DuckDB file does not exist yet: {db_path}\")\n    raise SystemExit(0)\n\ntry:\n    con = duckdb.connect(str(db_path), read_only=False)\n    con.close()\n    print(f\"[ok] DuckDB is writable: {db_path}\")\nexcept Exception as exc:\n    print(f\"[error] DuckDB lock detected on {db_path}\")\n    print(\"Close any external DuckDB client (CLI/IDE) and retry.\")\n    print(f\"Details: {exc}\")\n    raise SystemExit(1)\n''')"

up: check-db-lock
	$(UV) run dg dev

list:
	$(UV) run dg list defs

check:
	$(UV) run dg check defs

# ==============================================================================
# dbt
# ==============================================================================

dbt-deps:
	cd $(DBT_DIR) && $(UV) run dbt deps

dbt-run:
	cd $(DBT_DIR) && $(UV) run dbt run

dbt-test:
	cd $(DBT_DIR) && $(UV) run dbt test

dbt-build:
	cd $(DBT_DIR) && $(UV) run dbt build

# ==============================================================================
# Quality
# ==============================================================================

test:
	$(UV) run python -m unittest discover -s tests -p "test_*.py" -v

lint:
	$(UV) run ruff check .

format:
	$(UV) run ruff format .

# ==============================================================================
# Workflow
# ==============================================================================

verify: check test

pipeline: check test dbt-build

# ==============================================================================
# Maintenance
# ==============================================================================

clean:
	rm -rf $(DBT_DIR)/target
	rm -rf $(DBT_DIR)/logs
	rm -rf .pytest_cache
	rm -rf .ruff_cache

# ==============================================================================
# Docker Compose
# ==============================================================================

compose-up:
	docker compose up --build -d

compose-down:
	docker compose down

compose-logs:
	docker compose logs -f

compose-ps:
	docker compose ps
