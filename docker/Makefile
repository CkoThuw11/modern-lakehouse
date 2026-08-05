SHELL := /bin/bash
COMPOSE := docker compose -f docker-compose.yaml --env-file ../.env
PROFILES ?=
PROFILE_FLAGS := $(foreach p,$(PROFILES),--profile $(p))

.PHONY: up down logs ps reset register-source register-sink build dbt-run dbt-test bootstrap

# One command to build and run the entire platform from scratch:
# .env → download deps → build images → up (all profiles) → register connectors →
# wait for bronze → dbt run/test. See bootstrap.sh.
bootstrap:
	@./bootstrap.sh

up: build
	@if [ -z "$$($(COMPOSE) $(PROFILE_FLAGS) config --services)" ]; then \
		echo "no services defined for the selected profile(s); nothing to start"; \
	else \
		$(COMPOSE) $(PROFILE_FLAGS) up -d; \
	fi

build:
	@./build-all-images.sh

down:
	$(COMPOSE) --profile '*' down

logs:
	$(COMPOSE) logs -f

ps:
	$(COMPOSE) ps

reset:
	$(COMPOSE) --profile '*' down -v

register-source:
	set -a; source ../.env; set +a; \
	envsubst '$$POSTGRES_PORT $$POSTGRES_USER $$POSTGRES_PASSWORD $$POSTGRES_DB' < kafka-connect/connectors/debezium-postgres-source.json | \
	curl -s -X POST -H "Content-Type: application/json" \
	--data @- http://localhost:8083/connectors | jq .

register-sink:
	curl -s -X POST -H "Content-Type: application/json" \
	--data '{"namespace": ["bronze"]}' \
	http://localhost:8181/v1/namespaces > /dev/null; \
	set -a; source ../.env; set +a; \
	envsubst '$$MINIO_PORT $$MINIO_ROOT_USER $$MINIO_ROOT_PASSWORD' < kafka-connect/connectors/iceberg-sink.json | \
	curl -s -X POST -H "Content-Type: application/json" \
	--data @- http://localhost:8083/connectors | jq .

dbt-run:
	$(COMPOSE) run --rm dbt-spark run

dbt-test:
	$(COMPOSE) run --rm dbt-spark test
