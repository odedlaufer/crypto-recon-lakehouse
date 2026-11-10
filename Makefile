.DEFAULT_GOAL := help
TARGET ?= dev

.PHONY: help setup lint test validate deploy dbt-debug run-producer backfill

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-14s %s\n", $$1, $$2}'

# pre-commit refuses to install when a global core.hooksPath is set,
# so the install runs with the global git config hidden.
setup: ## Install Python deps (dev + dbt) and git hooks
	uv sync --all-packages --group dbt
	GIT_CONFIG_GLOBAL=/dev/null uv run pre-commit install

lint: ## Run all pre-commit hooks on every file
	uv run pre-commit run --all-files

# pytest exit code 5 means "no tests collected"; treated as a pass until tests exist.
test: ## Run pytest
	uv run pytest || [ $$? -eq 5 ]

validate: ## Validate the bundle (TARGET=dev|prd)
	databricks bundle validate -t $(TARGET)

deploy: ## Deploy the bundle (TARGET=dev|prd)
	databricks bundle deploy -t $(TARGET)

dbt-debug: ## Check the dbt connection (TARGET=dev|prd)
	cd dbt && uv run --group dbt dbt debug --target $(TARGET)

run-producer: ## Run the WebSocket producer, e.g. ARGS="--duration-seconds 60"
	uv run python -m crypto_recon_producer $(ARGS)

backfill: ## Backfill official candles, START= END= (Phase 2)
	@echo "Not implemented yet (Phase 2)"; exit 1
