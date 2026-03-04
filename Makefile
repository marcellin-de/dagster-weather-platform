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

# ==============================================================================
# Meta
# ==============================================================================

.PHONY: help

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
	@echo "  list           List Dagster definitions"
	@echo "  check          Validate Dagster definitions"
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

up:
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