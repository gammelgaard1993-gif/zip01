# Teton backend — developer & grader entrypoints.
#
# `make run` is the single command that brings up Mosquitto + Redis (via Docker)
# and starts the service. The other targets drive the simulator / test suite.
#
# Windows note: `make`/`docker` may not be installed. Equivalent PowerShell
# commands are documented in README.md ("Running on Windows").

PYTHON   ?= python3
HOST     ?= localhost
MQTT_PORT?= 1883
API      ?= http://localhost:8080
DEVICES  ?= 500
DURATION ?= 30
RATE     ?= 50000
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
	$(PYTHON) tools/simulator.py steady --host $(HOST) --port $(MQTT_PORT) --devices 50 --duration 5 --rate 1
	@sleep 2
	@echo "--- /metrics ---";              curl -s $(API)/metrics
	@echo "\n--- /devices/dev_0001/health ---"; curl -s $(API)/devices/dev_0001/health
	@echo "\n--- /rooms/room_00/occupancy?window=1m ---"; curl -s "$(API)/rooms/room_00/occupancy?window=1m"

.PHONY: burst
burst: ## 10x burst load — verify no drops + alarm p95 <= 1s (service must be running)
	$(PYTHON) tools/simulator.py burst --host $(HOST) --port $(MQTT_PORT) --devices $(DEVICES) --duration $(DURATION) --rate $(RATE)
	@sleep 2
	@echo "--- /metrics (check fall p95 + queue depths) ---"; curl -s $(API)/metrics

.PHONY: offline
offline: ## Offline device replays a backlog of late events (service must be running)
	$(PYTHON) tools/simulator.py offline --host $(HOST) --port $(MQTT_PORT) --offline-minutes 20 --events 1200
	@sleep 2
	@echo "--- /rooms/room_00/occupancy?window=1h (backfilled) ---"; curl -s "$(API)/rooms/room_00/occupancy?window=1h"
