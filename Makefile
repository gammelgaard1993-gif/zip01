# Teton backend — developer & grader entrypoints.
#
# `make run` is the single command that brings up Mosquitto + Redis (via Docker)
# and starts the service. The other targets drive the event generator / test suite.
#
# Windows note: `make`/`docker` may not be installed. Equivalent PowerShell
# commands are documented in README.md ("Running on Windows").

PYTHON   ?= python3
API      ?= http://localhost:8080
DEVICES  ?= 500
DURATION ?= 30
RPS      ?= 1.0
COMPOSE  ?= docker compose

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

.PHONY: deps
deps: ## Install pinned Python dependencies
	$(PYTHON) -m pip install -r requirements.txt

.PHONY: infra-up
infra-up: ## Start Mosquitto + Redis (detached)
	$(COMPOSE) up -d
	@echo "waiting for redis + mosquitto health..."
	@$(COMPOSE) ps

.PHONY: infra-down
infra-down: ## Stop Mosquitto + Redis
	$(COMPOSE) down

.PHONY: serve
serve: ## Start the FastAPI service only (assumes infra already up)
	$(PYTHON) main.py

.PHONY: run
run: infra-up ## Start infra + service (single command)
	$(PYTHON) main.py

.PHONY: test
test: ## Run the unit + integration test suite
	$(PYTHON) -m unittest discover -s tests -v

.PHONY: smoke
smoke: ## Quick end-to-end check (service must be running)
	$(PYTHON) event_generator/generate.py --mode baseline --target $(API) --devices 50 --duration 5 --rps-per-device 1.0
	@sleep 2
	@echo "--- /metrics ---";              curl -s $(API)/metrics
	@echo "\n--- /devices/dev_0001/health ---"; curl -s $(API)/devices/dev_0001/health
	@echo "\n--- /rooms/room_000/occupancy?window=1m ---"; curl -s "$(API)/rooms/room_000/occupancy?window=1m"

.PHONY: burst
burst: ## Burst load (two 10x/30s spikes) — verify no drops + alarm p95 <= 1s (service must be running)
	$(PYTHON) event_generator/generate.py --mode burst --target $(API) --devices $(DEVICES) --duration $(DURATION) --rps-per-device $(RPS)
	@sleep 2
	@echo "--- /metrics (check fall p95 + queue depths) ---"; curl -s $(API)/metrics

.PHONY: offline
offline: ## 20% of devices go offline then replay a backlog of late events (service must be running)
	$(PYTHON) event_generator/generate.py --mode offline --target $(API) --devices $(DEVICES) --duration 120 --rps-per-device $(RPS)
	@sleep 2
	@echo "--- /rooms/room_000/occupancy?window=1h (backfilled) ---"; curl -s "$(API)/rooms/room_000/occupancy?window=1h"

.PHONY: adversarial
adversarial: ## Burst + offline + clock skew combined — full stress scenario (service must be running)
	$(PYTHON) event_generator/generate.py --mode adversarial --target $(API) --devices $(DEVICES) --duration 120 --rps-per-device $(RPS)
	@sleep 2
	@echo "--- /metrics ---"; curl -s $(API)/metrics

