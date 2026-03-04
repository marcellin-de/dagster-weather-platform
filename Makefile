SHELL := /bin/bash

DAGSTER_HOME ?= $(CURDIR)/.dg
PYTHON ?= python

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
	DAGSTER_HOME="$(DAGSTER_HOME)" dg list defs

check:
	DAGSTER_HOME="$(DAGSTER_HOME)" dg check defs

test:
	$(PYTHON) -m unittest discover -s tests -p "test_*.py" -v

verify: check test

# Compatibility target so `make check defs` / `make list defs` do not fail.
defs:
	@:

check-defs: check

list-defs: list

up:
	DAGSTER_HOME="$(DAGSTER_HOME)" dg dev

