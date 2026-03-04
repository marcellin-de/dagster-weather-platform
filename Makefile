SHELL := /bin/bash

DAGSTER_HOME ?= $(CURDIR)/.dg
VENV_BIN := $(CURDIR)/.venv/bin
DG ?= $(VENV_BIN)/dg
PYTHON ?= $(VENV_BIN)/python

# Fallback when running without a local virtual environment.
ifeq ($(wildcard $(DG)),)
DG := dg
endif
ifeq ($(wildcard $(PYTHON)),)
PYTHON := python
endif

.PHONY: list check test verify up help defs check-defs list-defs

help:
	@echo "Available targets:"
	@echo "  list    - List all Dagster definitions"
	@echo "  check   - Check the validity of Dagster definitions"
	@echo "  test    - Run unit tests"
	@echo "  verify  - Run both check and test targets"
	@echo "  check-defs - Alias of check"
	@echo "  list-defs  - Alias of list"
	@echo "  up      - Start the Dagster instance "


list:
	DAGSTER_PROJECT_ROOT="$(CURDIR)" DAGSTER_HOME="$(DAGSTER_HOME)" $(DG) list defs

check:
	DAGSTER_PROJECT_ROOT="$(CURDIR)" DAGSTER_HOME="$(DAGSTER_HOME)" $(DG) check defs

test:
	$(PYTHON) -m unittest discover -s tests -p "test_*.py" -v

verify: check test

# Compatibility target so `make check defs` / `make list defs` do not fail.
defs:
	@:

check-defs: check

list-defs: list

up:
	DAGSTER_PROJECT_ROOT="$(CURDIR)" DAGSTER_HOME="$(DAGSTER_HOME)" $(DG) dev
