#!/bin/bash
set -e

# Data Platform Docker Image Build Script
# Builds all custom Docker images using pre-downloaded dependencies

# Colors for output
BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

DOCKER_BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE="docker compose -f $DOCKER_BASE_DIR/docker-compose.yaml --env-file $DOCKER_BASE_DIR/../.env"

# Check if downloads directory exists
DOWNLOAD_DIR="$DOCKER_BASE_DIR/downloads"
if [ ! -d "$DOWNLOAD_DIR" ]; then
    log_error "Downloads directory not found: $DOWNLOAD_DIR"
    echo "Please run: ./download-dependencies.sh first"
    exit 1
fi

# Check if we have the required files
required_files=(
    "iceberg-spark-runtime-3.5_2.12-1.9.1.jar"
    "iceberg-aws-bundle-1.9.1.jar"
    "hadoop-aws-3.3.4.jar"
    "aws-java-sdk-bundle-1.12.262.jar"
    "postgresql.jar"
    "iceberg-kafka-connect/iceberg-kafka-connect-1.9.1.jar"
    "iceberg-kafka-connect/iceberg-kafka-connect-events-1.9.1.jar"
    "iceberg-kafka-connect/iceberg-core-1.9.1.jar"
    "iceberg-kafka-connect/iceberg-parquet-1.9.1.jar"
    "iceberg-kafka-connect/iceberg-aws-bundle-1.9.1.jar"
    "iceberg-kafka-connect/hadoop-client-api-3.3.4.jar"
    "iceberg-kafka-connect/hadoop-client-runtime-3.3.4.jar"
    "jmx_prometheus_javaagent-0.20.0.jar"
)
required_dirs=(
    "debezium-debezium-connector-postgresql"
)

log_info "Checking for required download files..."
missing=()
for file in "${required_files[@]}"; do
    if [ ! -f "$DOWNLOAD_DIR/$file" ]; then
        missing+=("$file")
    fi
done
for dir in "${required_dirs[@]}"; do
    if [ ! -d "$DOWNLOAD_DIR/$dir" ]; then
        missing+=("$dir/")
    fi
done

if [ ${#missing[@]} -ne 0 ]; then
    log_error "Missing required files:"
    for file in "${missing[@]}"; do
        echo "  - $file"
    done
    echo ""
    echo "Please run: ./download-dependencies.sh"
    exit 1
fi

log_success "All required files are available"

build_image() {
    local service_name="$1"

    log_info "🐳 Building $service_name..."
    if $COMPOSE build "$service_name"; then
        log_success "✅ Successfully built $service_name"
        return 0
    else
        log_error "❌ Failed to build $service_name"
        return 1
    fi
}

# Track build results
build_errors=0

for service in postgres iceberg-rest kafka-connect spark; do
    if ! build_image "$service"; then
        ((build_errors++))
    fi
done

# Summary
echo ""
if [ "$build_errors" -eq 0 ]; then
    log_success "🎉 All Docker images built successfully!"
    echo ""
    log_info "📋 Built images:"
    docker images --format "table {{.Repository}}:{{.Tag}}\t{{.Size}}\t{{.CreatedAt}}" \
      | grep -E "^data-platform-(postgres|iceberg-rest|kafka-connect|spark):" || true
else
    log_error "❌ $build_errors image(s) failed to build"
    exit 1
fi

echo ""
log_success "✅ Ready for data platform deployment!"
echo "Run: make up PROFILES=\"core lakehouse streaming\""
