#!/bin/bash
set -euo pipefail

# One-command bring-up of the whole platform:
#   .env -> download deps -> build images -> up (every profile) -> register
#   connectors -> wait for bronze to materialize -> dbt run/test.
# Idempotent: safe to re-run (existing .env/downloads/connectors are reused).
#
# Usage:
#   ./start-all.sh          start (or resume) the full platform
#   ./start-all.sh reset    stop everything and wipe all containers + volumes

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

BLUE='\033[0;34m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
log_info()    { echo -e "${BLUE}[START]${NC} $1"; }
log_success() { echo -e "${GREEN}[START]${NC} $1"; }
log_error()   { echo -e "${RED}[START]${NC} $1"; }

COMPOSE=(docker compose -f docker-compose.yaml --env-file ../.env)
ALL_PROFILES="core lakehouse streaming bi orchestration"
PROFILE_FLAGS=()
for p in $ALL_PROFILES; do PROFILE_FLAGS+=(--profile "$p"); done

# Wait until a shell condition succeeds, or give up after a timeout.
wait_for() {
    local desc="$1" timeout="$2"; shift 2
    local waited=0
    log_info "Waiting for ${desc} (timeout ${timeout}s)..."
    until "$@" >/dev/null 2>&1; do
        sleep 5; waited=$((waited + 5))
        if [ "$waited" -ge "$timeout" ]; then
            log_error "Timed out waiting for ${desc} after ${timeout}s"
            return 1
        fi
    done
    log_success "${desc} is ready"
}

if [ "${1:-}" = "reset" ]; then
    log_info "Stopping every service and wiping all containers + volumes..."
    "${COMPOSE[@]}" --profile '*' down -v
    log_success "Reset complete. Run ./start-all.sh to start fresh."
    exit 0
fi

# 1. .env (first run only) ---------------------------------------------------
if [ ! -f ../.env ]; then
    log_info "No ../.env found — creating it from ../.env.example"
    cp ../.env.example ../.env
fi

# 2. Download dependencies + build every custom image ------------------------
log_info "Downloading dependencies..."
./download-dependencies.sh

log_info "Building images..."
./build-all-images.sh

log_info "Starting all profiles (${ALL_PROFILES})..."
"${COMPOSE[@]}" "${PROFILE_FLAGS[@]}" up -d

# 3. Register the Debezium source + Iceberg sink once Kafka Connect is ready -
wait_for "Kafka Connect REST API" 180 \
    curl -sf http://localhost:8083/connectors
wait_for "Iceberg sink plugin to load" 180 \
    bash -c "curl -sf http://localhost:8083/connector-plugins | grep -q IcebergSinkConnector"

log_info "Registering Debezium source connector..."
set -a; source ../.env; set +a
envsubst '$POSTGRES_PORT $POSTGRES_USER $POSTGRES_PASSWORD $POSTGRES_DB' \
    < kafka-connect/connectors/debezium-postgres-source.json | \
    curl -s -X POST -H "Content-Type: application/json" --data @- http://localhost:8083/connectors | jq .

log_info "Registering Iceberg sink connector..."
curl -s -X POST -H "Content-Type: application/json" \
    --data '{"namespace": ["bronze"]}' http://localhost:8181/v1/namespaces > /dev/null
envsubst '$MINIO_PORT $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD' \
    < kafka-connect/connectors/iceberg-sink.json | \
    curl -s -X POST -H "Content-Type: application/json" --data @- http://localhost:8083/connectors | jq .

# 4. Wait for the sink's first commit to auto-create all four bronze tables --
wait_for "all four bronze tables to materialize" 300 \
    bash -c '[ "$(curl -sf http://localhost:8181/v1/namespaces/bronze/tables | jq -r ".identifiers | length")" -ge 4 ]'

# 5. Transform: bronze -> silver -> gold, then assert ------------------------
# Runs through the Airflow image's own dbt venv (/opt/dbt-venv), the same one the
# lakehouse_pipeline DAG uses, against the dbt project bind-mounted at /opt/airflow/dbt.
DBT_RUN_FLAGS=(--rm --no-deps -e DBT_TARGET_PATH=/tmp/dbt-target -e DBT_LOG_PATH=/tmp/dbt-logs airflow-scheduler)
DBT_DIR_FLAGS=(--profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt)

log_info "Running dbt (silver -> gold)..."
"${COMPOSE[@]}" run "${DBT_RUN_FLAGS[@]}" /opt/dbt-venv/bin/dbt run "${DBT_DIR_FLAGS[@]}"
log_info "Running dbt tests..."
"${COMPOSE[@]}" run "${DBT_RUN_FLAGS[@]}" /opt/dbt-venv/bin/dbt test "${DBT_DIR_FLAGS[@]}"

log_success "Platform is up end-to-end."
echo ""
log_info "Endpoints:"
echo "  - MinIO console     http://localhost:9001"
echo "  - AKHQ (Kafka UI)   http://localhost:8080"
echo "  - Kafka Connect     http://localhost:8083"
echo "  - Iceberg REST      http://localhost:8181"
echo "  - Spark Thrift      localhost:10000 (beeline/JDBC)"
echo "  - Trino             http://localhost:8085"
echo "  - Airflow           http://localhost:8088"
