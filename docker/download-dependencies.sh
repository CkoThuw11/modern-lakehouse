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
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
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
log_info "--- Downloading Kafka Connect Plugins ---"
download_file "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.5.4.Final/debezium-connector-postgres-2.5.4.Final-plugin.tar.gz"

# Iceberg sink: Apache publishes no runtime bundle, so every jar the connector needs
# is listed here (the runtime deps of iceberg-kafka-connect 1.9.1). Do not swap in the
# Spark runtime fat jar: it relocates Avro, which breaks the commit coordinator
# (NoSuchMethodError in StartCommit). Keep versions in step with Iceberg 1.9.1.
SINK_DIR="iceberg-kafka-connect"
mkdir -p "$DOWNLOAD_DIR/$SINK_DIR"
download_sink_jar() {
    download_file "$1" "$SINK_DIR/$(basename "$1")"
}

# Iceberg connector + core
download_sink_jar "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-api/1.9.1/iceberg-api-1.9.1.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-bundled-guava/1.9.1/iceberg-bundled-guava-1.9.1.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-common/1.9.1/iceberg-common-1.9.1.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-core/1.9.1/iceberg-core-1.9.1.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-data/1.9.1/iceberg-data-1.9.1.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-kafka-connect-events/1.9.1/iceberg-kafka-connect-events-1.9.1.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-kafka-connect/1.9.1/iceberg-kafka-connect-1.9.1.jar"

# File formats (Parquet, ORC, Avro) + compression
download_sink_jar "https://repo1.maven.org/maven2/com/github/luben/zstd-jni/1.5.6-6/zstd-jni-1.5.6-6.jar"
download_sink_jar "https://repo1.maven.org/maven2/io/airlift/aircompressor/0.27/aircompressor-0.27.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/apache/avro/avro/1.12.0/avro-1.12.0.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/apache/commons/commons-compress/1.26.2/commons-compress-1.26.2.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-orc/1.9.1/iceberg-orc-1.9.1.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-parquet/1.9.1/iceberg-parquet-1.9.1.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/apache/orc/orc-core/1.9.5/orc-core-1.9.5-nohive.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/apache/orc/orc-shims/1.9.5/orc-shims-1.9.5.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/apache/parquet/parquet-avro/1.15.2/parquet-avro-1.15.2.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/apache/parquet/parquet-column/1.15.2/parquet-column-1.15.2.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/apache/parquet/parquet-common/1.15.2/parquet-common-1.15.2.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/apache/parquet/parquet-encoding/1.15.2/parquet-encoding-1.15.2.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/apache/parquet/parquet-format-structures/1.15.2/parquet-format-structures-1.15.2.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/apache/parquet/parquet-hadoop/1.15.2/parquet-hadoop-1.15.2.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/apache/parquet/parquet-jackson/1.15.2/parquet-jackson-1.15.2.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/roaringbitmap/RoaringBitmap/1.3.0/RoaringBitmap-1.3.0.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/threeten/threeten-extra/1.7.1/threeten-extra-1.7.1.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.7/snappy-java-1.1.10.7.jar"

# S3FileIO (MinIO)
download_sink_jar "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.9.1/iceberg-aws-bundle-1.9.1.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/1.9.1/iceberg-aws-1.9.1.jar"

# Hadoop Configuration classes the sink references
download_sink_jar "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.4/hadoop-client-api-3.3.4.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.4/hadoop-client-runtime-3.3.4.jar"

# Shared transitive libraries
download_sink_jar "https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.18.3/jackson-annotations-2.18.3.jar"
download_sink_jar "https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.18.3/jackson-core-2.18.3.jar"
download_sink_jar "https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.18.3/jackson-databind-2.18.3.jar"
download_sink_jar "https://repo1.maven.org/maven2/com/github/ben-manes/caffeine/caffeine/2.9.3/caffeine-2.9.3.jar"
download_sink_jar "https://repo1.maven.org/maven2/com/google/errorprone/error_prone_annotations/2.10.0/error_prone_annotations-2.10.0.jar"
download_sink_jar "https://repo1.maven.org/maven2/commons-codec/commons-codec/1.17.0/commons-codec-1.17.0.jar"
download_sink_jar "https://repo1.maven.org/maven2/commons-io/commons-io/2.16.1/commons-io-2.16.1.jar"
download_sink_jar "https://repo1.maven.org/maven2/commons-pool/commons-pool/1.6/commons-pool-1.6.jar"
download_sink_jar "https://repo1.maven.org/maven2/dev/failsafe/failsafe/3.3.2/failsafe-3.3.2.jar"
download_sink_jar "https://repo1.maven.org/maven2/javax/annotation/javax.annotation-api/1.3.2/javax.annotation-api-1.3.2.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/apache/commons/commons-lang3/3.12.0/commons-lang3-3.12.0.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/apache/httpcomponents/client5/httpclient5/5.4.3/httpclient5-5.4.3.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/apache/httpcomponents/core5/httpcore5-h2/5.3.4/httpcore5-h2-5.3.4.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/apache/httpcomponents/core5/httpcore5/5.3.4/httpcore5-5.3.4.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/checkerframework/checker-qual/3.19.0/checker-qual-3.19.0.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/jetbrains/annotations/17.0.0/annotations-17.0.0.jar"
download_sink_jar "https://repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.17/slf4j-api-2.0.17.jar"

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
