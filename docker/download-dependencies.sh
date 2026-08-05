#!/bin/bash
set -e

# Data Platform Dependency Download Script
# Downloads all necessary JAR files and Kafka Connect plugins for custom Docker images

# Directory to store downloads
DOWNLOAD_DIR="$(dirname "$0")/downloads"
mkdir -p "$DOWNLOAD_DIR"

# Colors for output
BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
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

usage() {
    echo "Data Platform Dependency Download Script"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Downloads all necessary dependencies for:"
    echo "  • Iceberg REST Catalog (Postgres JDBC driver)"
    echo "  • Spark Engine (Iceberg runtime, S3A/AWS SDK)"
    echo "  • Kafka Connect (Debezium Postgres source, Iceberg sink)"
    echo ""
    echo "Options:"
    echo "  -h, --help    Show this help message"
    echo "  --clean       Remove downloads directory before downloading"
    echo ""
    echo "Downloaded files will be stored in: $DOWNLOAD_DIR"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            exit 0
            ;;
        --clean)
            log_info "Cleaning downloads directory..."
            rm -rf "$DOWNLOAD_DIR"
            mkdir -p "$DOWNLOAD_DIR"
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
    shift
done

download_file() {
    local url=$1
    local filename="${2:-$(basename "$url")}"
    local filepath="$DOWNLOAD_DIR/$filename"

    if [ -f "$filepath" ]; then
        log_info "File already exists: $filename"
    else
        log_info "Downloading: $filename"
        curl -sfL -o "$filepath" "$url"
        log_success "Downloaded: $filename"
    fi
}

install_connect_plugin() {
    local plugin=$1
    local dirname=$2

    if [ -d "$DOWNLOAD_DIR/$dirname" ]; then
        log_info "Plugin already installed: $dirname"
    else
        log_info "Installing Kafka Connect plugin: $plugin"
        # confluent-hub runs as a container user that doesn't own $DOWNLOAD_DIR on
        # the host, so it needs the mount to be world-writable.
        chmod 777 "$DOWNLOAD_DIR"
        docker run --rm -v "$DOWNLOAD_DIR:/downloads" \
            --entrypoint confluent-hub confluentinc/cp-kafka-connect:7.6.1 \
            install --no-prompt --component-dir /downloads --worker-configs "" "$plugin"
        log_success "Installed: $plugin"
    fi
}

# Iceberg REST Catalog Dependencies
log_info "--- Downloading Iceberg REST Catalog Dependencies ---"
download_file "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar" "postgresql.jar"

# Spark Engine Dependencies
log_info "--- Downloading Spark Engine Dependencies ---"
download_file "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.9.1/iceberg-spark-runtime-3.5_2.12-1.9.1.jar"
download_file "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.9.1/iceberg-aws-bundle-1.9.1.jar"
download_file "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
download_file "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"

# Kafka Connect Dependencies (Debezium Postgres source, Iceberg sink)
log_info "--- Installing Kafka Connect Plugins ---"
install_connect_plugin "debezium/debezium-connector-postgresql:2.5.4-2" "debezium-debezium-connector-postgresql"

# Apache-native Iceberg Kafka Connect sink. The databricks fork froze at 0.6.19 (open multi-table
# routing bug apache/iceberg#13457) and Apache publishes no ready-to-use plugin zip, so we assemble
# the plugin from the connector's exact Maven RUNTIME closure: the standalone, NON-shaded iceberg
# jars plus plain Avro/Parquet. (The shaded iceberg-spark-runtime fat jar returns shaded Avro types
# from AvroSchemaUtil and is binary-incompatible with the connector's event classes — it fails at
# commit time with NoSuchMethodError.) iceberg-parquet/iceberg-orc are optional deps of iceberg-data
# so they're requested explicitly; the AWS SDK bundle (S3FileIO) and Hadoop's shaded client (the
# Parquet writer's org.apache.hadoop.conf.Configuration, which Spark provides at runtime but a Kafka
# Connect worker does not) are added on top.
ICEBERG_CONNECT_DIR="$DOWNLOAD_DIR/iceberg-kafka-connect"
if [ -f "$ICEBERG_CONNECT_DIR/iceberg-kafka-connect-1.9.1.jar" ]; then
    log_info "Plugin already assembled: iceberg-kafka-connect"
else
    log_info "Resolving Apache Iceberg Kafka Connect runtime closure via Maven..."
    mkdir -p "$ICEBERG_CONNECT_DIR"
    chmod 777 "$ICEBERG_CONNECT_DIR"
    pomdir="$(mktemp -d)"
    cat > "$pomdir/pom.xml" <<'POM'
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>data-platform</groupId>
  <artifactId>iceberg-kafka-connect-deps</artifactId>
  <version>1</version>
  <packaging>pom</packaging>
  <dependencies>
    <dependency><groupId>org.apache.iceberg</groupId><artifactId>iceberg-kafka-connect</artifactId><version>1.9.1</version></dependency>
    <dependency><groupId>org.apache.iceberg</groupId><artifactId>iceberg-kafka-connect-events</artifactId><version>1.9.1</version></dependency>
    <dependency><groupId>org.apache.iceberg</groupId><artifactId>iceberg-aws</artifactId><version>1.9.1</version></dependency>
    <dependency><groupId>org.apache.iceberg</groupId><artifactId>iceberg-parquet</artifactId><version>1.9.1</version></dependency>
    <dependency><groupId>org.apache.iceberg</groupId><artifactId>iceberg-orc</artifactId><version>1.9.1</version></dependency>
  </dependencies>
</project>
POM
    docker run --rm -v "$pomdir:/w" -v "$ICEBERG_CONNECT_DIR:/out" -w /w maven:3.9-eclipse-temurin-17 \
        mvn -q -B dependency:copy-dependencies -DincludeScope=runtime -DexcludeScope=provided -DoutputDirectory=/out
    rm -rf "$pomdir"
    # AWS SDK v2 bundle for S3FileIO (iceberg-aws classes come from the closure; the SDK does not).
    cp "$DOWNLOAD_DIR/iceberg-aws-bundle-1.9.1.jar" "$ICEBERG_CONNECT_DIR/"
    log_success "Assembled iceberg-kafka-connect plugin ($(ls -1 "$ICEBERG_CONNECT_DIR"/*.jar | wc -l) jars)"
fi
download_file "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.4/hadoop-client-api-3.3.4.jar" "iceberg-kafka-connect/hadoop-client-api-3.3.4.jar"
download_file "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.4/hadoop-client-runtime-3.3.4.jar" "iceberg-kafka-connect/hadoop-client-runtime-3.3.4.jar"

log_success "All dependencies downloaded successfully to $DOWNLOAD_DIR"

# Show summary
echo ""
log_info "📋 Download Summary:"
echo "  Directory: $DOWNLOAD_DIR"
if command -v du &> /dev/null; then
    echo "  Total size: $(du -sh "$DOWNLOAD_DIR" 2>/dev/null | cut -f1 || echo "Unknown")"
fi
echo "  Files downloaded: $(find "$DOWNLOAD_DIR" -type f | wc -l)"
echo ""
log_success "✅ All dependencies ready for data platform image builds!"
echo "Run: ./build-all-images.sh"
