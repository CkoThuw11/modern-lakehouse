#!/bin/bash
set -euo pipefail

# One-command, from-scratch bring-up of the whole platform:
#   .env → download deps → build images → up (all profiles) → register connectors →
#   wait for bronze to materialize → dbt run/test.
# Idempotent: safe to re-run (existing .env/downloads/connectors are reused, not clobbered).

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

BLUE='\033[0;34m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
log_info()    { echo -e "${BLUE}[BOOTSTRAP]${NC} $1"; }
log_success() { echo -e "${GREEN}[BOOTSTRAP]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[BOOTSTRAP]${NC} $1"; }
log_error()   { echo -e "${RED}[BOOTSTRAP]${NC} $1"; }

# Every layer needed for a full end-to-end run.
PROFILES="core lakehouse streaming bi"

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

# 1. .env (first run only) --------------------------------------------------
if [ ! -f ../.env ]; then
    log_info "No ../.env found — creating it from ../.env.example"
    cp ../.env.example ../.env
fi

# 2. Download dependencies + build every custom image ------------------------
log_info "Downloading dependencies..."
./download-dependencies.sh

log_info "Building images and starting all profiles (${PROFILES})..."
# `make up` runs build-all-images.sh as a prerequisite, then `docker compose up -d`.
make up PROFILES="$PROFILES"

# 3. Register the Debezium source once Kafka Connect has loaded its plugins --
wait_for "Kafka Connect REST API" 180 \
    curl -sf http://localhost:8083/connectors
wait_for "Iceberg sink plugin to load" 180 \
    bash -c "curl -sf http://localhost:8083/connector-plugins | grep -q IcebergSinkConnector"

log_info "Registering Debezium source connector..."
make register-source

log_info "Registering Iceberg sink connector..."
make register-sink

# 4. Wait for the sink's first commit to auto-create all four bronze tables --
wait_for "all four bronze tables to materialize" 300 \
    bash -c '[ "$(curl -sf http://localhost:8181/v1/namespaces/bronze/tables | jq -r ".identifiers | length")" -ge 4 ]'

# 5. Transform: bronze → silver → gold, then assert -------------------------
log_info "Running dbt (silver → gold)..."
make dbt-run
log_info "Running dbt tests..."
make dbt-test

log_success "🎉 Platform is up end-to-end."
echo ""
log_info "Endpoints:"
echo "  • MinIO console     http://localhost:9001"
echo "  • AKHQ (Kafka UI)   http://localhost:8080"
echo "  • Kafka Connect     http://localhost:8083"
echo "  • Iceberg REST      http://localhost:8181"
echo "  • Spark Thrift      localhost:10000 (beeline/JDBC)"
echo "  • Trino             http://localhost:8085"
echo ""
log_info "Try:  docker exec -it trino trino --execute 'SELECT count(*) FROM iceberg.bronze.orders'"
