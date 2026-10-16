# Developer entry points. Every target is a thin wrapper around `uv run`, so you can also run
# the underlying command directly. `make help` lists the targets.

.DEFAULT_GOAL := help
.PHONY: help sync test test-fast bench lint format typecheck arch check

help:  ## Show this help
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

sync:  ## Create/refresh the virtual environment from uv.lock
	uv sync

test:  ## Run the whole test suite
	uv run pytest

test-fast:  ## Run the tests that need no Docker
	uv run pytest -m "not docker"

bench:  ## Measure chunk store throughput (needs Docker; prints a table)
	uv run pytest benchmarks -s

lint:  ## Lint and check formatting (no changes made)
	uv run ruff check .
	uv run ruff format --check .

format:  ## Auto-fix lint issues and format the code
	uv run ruff check --fix .
	uv run ruff format .

typecheck:  ## Static type check (strict)
	uv run mypy packages tests benchmarks scripts

arch:  ## Check the architecture rules (who may import whom)
	uv run lint-imports

check: lint typecheck arch test  ## Everything CI runs
