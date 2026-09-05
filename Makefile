# Crypto Streaming Lakehouse — developer entrypoints.
# Phase 0 = default services; Phase 1 = the `lake` profile (MinIO, catalog, Spark).

.DEFAULT_GOAL := help
COMPOSE := docker compose

.PHONY: help up down logs ps register status psql topics trades book clean \
        lake lake-down lake-logs

help: ## show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

up: ## start the source+cdc stack, then register the connector
	$(COMPOSE) up -d --build
	@echo "waiting for Kafka Connect to be ready..."
	@$(MAKE) --no-print-directory register

down: ## stop all services (keep volumes)
	$(COMPOSE) down

clean: ## stop and delete volumes (fresh start)
	$(COMPOSE) down -v

logs: ## tail logs from all services
	$(COMPOSE) logs -f --tail=100

ps: ## list running services
	$(COMPOSE) ps

register: ## register/update the Debezium connector
	./infra/debezium/register.sh

status: ## show Debezium connector status
	@curl -s http://localhost:8083/connectors/crypto-connector/status | python3 -m json.tool

psql: ## open a psql shell on the source db
	$(COMPOSE) exec postgres psql -U crypto -d crypto

topics: ## list Redpanda topics
	$(COMPOSE) exec redpanda rpk topic list

trades: ## show the latest trade CDC messages
	$(COMPOSE) exec redpanda rpk topic consume crypto.public.trades --num 5 --offset end

book: ## show the latest orderbook CDC messages (insert/update/delete)
	$(COMPOSE) exec redpanda rpk topic consume crypto.public.orderbook_levels --num 5 --offset end

# --- Phase 1: lakehouse (needs the default stack from `make up` running) ------

lake: ## start the lake profile (MinIO, catalog, Spark Bronze/Silver)
	$(COMPOSE) --profile lake up -d
	@echo "Spark is running the Bronze+Silver streams — follow with: make lake-logs"

lake-down: ## stop the lake profile services (keep volumes)
	$(COMPOSE) --profile lake down

lake-logs: ## tail the Spark driver logs (Bronze/Silver progress)
	$(COMPOSE) logs -f --tail=100 spark
