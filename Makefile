SHELL := /bin/bash
.DEFAULT_GOAL := help

COMPOSE := docker compose
TOOLS := $(COMPOSE) run --rm -T tools
UV ?= uv

# Only used to print/curl the API URL; every setting still lives in .env.
-include .env
API_URL := http://localhost:$(API_HOST_PORT)

.PHONY: help env up down reset ps logs trace seed simulate inject-bad-events replay rebuild \
        restart-consumer check-config stats events quarantine snowflake-keypair demo \
        install lint fmt typecheck test test-unit test-integration check schema

help: ## List the available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# --- Stack -------------------------------------------------------------------

env: .env ## Create .env from .env.example (never overwrites an existing .env)

.env:
	cp .env.example .env
	@echo "Created .env: set SNOWFLAKE_ACCOUNT and credentials (or enable the emulator block)."

up: .env ## Build and start broker, Postgres, adapter, consumer and API
	$(COMPOSE) up --build --detach
	@echo "API docs: $(API_URL)/docs"

down: ## Stop the stack (data is kept)
	$(COMPOSE) down

reset: ## Stop the stack and delete all data: topic, sink and sync state
	$(COMPOSE) down --volumes --remove-orphans

ps: ## Show service status and health
	$(COMPOSE) ps

logs: ## Follow the pipeline logs
	$(COMPOSE) logs --follow --tail=50 adapter consumer api

trace: ## Every log line for one id across services: make trace ID=<batch_id|event_id>
	@test -n "$(ID)" || { echo "usage: make trace ID=<batch_id or event_id>"; exit 2; }
	@$(COMPOSE) logs --no-log-prefix --no-color adapter consumer | grep -F -- '$(ID)'

# --- Pipeline operations -------------------------------------------------------

seed: .env ## Create and fill the Snowflake source table (skips if it has rows)
	$(TOOLS) seed

simulate: .env ## Insert and update rows in Snowflake, including a few invalid ones
	$(TOOLS) simulate

inject-bad-events: ## Publish malformed / duplicate / conflicting records to the topic
	$(TOOLS) inject-bad-events

replay: ## Replay the topic from offset 0 over the sink and verify nothing changes
	$(COMPOSE) stop consumer
	$(TOOLS) replay; status=$$?; $(COMPOSE) start consumer; exit $$status

rebuild: ## Truncate the sink, rebuild it from the topic and verify identical checksums
	$(COMPOSE) stop consumer
	$(TOOLS) replay --rebuild; status=$$?; $(COMPOSE) start consumer; exit $$status

restart-consumer: ## Restart the consumer, e.g. after editing config/quality_rules.yaml
	$(COMPOSE) restart consumer

check-config: .env ## Validate .env and the quality rules file
	$(TOOLS) check-config

stats: ## GET /stats
	@curl -fsS "$(API_URL)/stats" | python3 -m json.tool

events: ## GET /events (10 most recent)
	@curl -fsS "$(API_URL)/events?limit=10" | python3 -m json.tool

quarantine: ## GET /quarantine
	@curl -fsS "$(API_URL)/quarantine" | python3 -m json.tool

snowflake-keypair: ## Create secrets/snowflake_rsa_key.p8 (+ .pub) for key-pair auth
	@test ! -e secrets/snowflake_rsa_key.p8 || { echo "secrets/snowflake_rsa_key.p8 exists"; exit 1; }
	openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out secrets/snowflake_rsa_key.p8
	openssl pkey -in secrets/snowflake_rsa_key.p8 -pubout -out secrets/snowflake_rsa_key.pub
	@# Owner-only. Docker Desktop maps it to the container user; on Linux hosts
	@# run `sudo chown 10001 secrets/snowflake_rsa_key.p8` (the image's user).
	chmod 600 secrets/snowflake_rsa_key.p8
	@echo "Paste into snowflake/setup.sql (RSA_PUBLIC_KEY):"
	@grep -v -- '-----' secrets/snowflake_rsa_key.pub | tr -d '\n'; echo

demo: ## Scripted walkthrough for the screen recording
	./scripts/demo.sh

# --- Development (needs uv: https://docs.astral.sh/uv/) ---------------------------

install: ## Install the project and dev tools into .venv
	$(UV) sync

lint: ## Ruff lint and format check
	$(UV) run ruff check src tests
	$(UV) run ruff format --check src tests

fmt: ## Auto-format and fix lint
	$(UV) run ruff format src tests
	$(UV) run ruff check --fix src tests

typecheck: ## mypy --strict
	$(UV) run mypy src

test-unit: ## Unit tests (no Docker)
	$(UV) run pytest tests/unit

test-integration: ## Integration tests (Docker: Postgres, Redpanda; Snowflake emulated)
	$(UV) run pytest tests/integration

test: ## All tests
	$(UV) run pytest

check: lint typecheck test ## Everything CI runs

schema: ## Regenerate schemas/change_event.v1.schema.json
	$(UV) run switch-pipeline export-schema
