# 🔍 Troubleshooting

Commands use the default ports from `.env.example`; run `docker compose` commands from `docker/`.

### Common issues

1. Source/Sink Kafka connection failed
```bash
curl localhost:8083/connectors/debezium-postgres-source/status
curl localhost:8083/connectors/iceberg-sink/status
```
Look for "state":"RUNNING" on the connector and on every task.

Then follow the data along the path:
```bash
# Source writes to Kafka: topics exist, offsets grow after a change in Postgres
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic bikestores.sales.orders

# Sink reads from Kafka: LAG should be ~0
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group connect-iceberg-sink
```

2. Iceberg REST catalog down, or namespaces missing
```bash
curl localhost:8181/v1/config
curl localhost:8181/v1/namespaces       # expect bronze, silver, gold, default
```

3. Postgres not ready for CDC (source connector fails right away)
```bash
docker exec postgres psql -U bikestores -d bikestores -c "SHOW wal_level;"    # must be: logical
docker exec postgres psql -U bikestores -d bikestores -c "SELECT * FROM pg_replication_slots;"
```

4. MinIO bucket missing (sink or Spark cannot write)
```bash
docker logs minio-init                  # expect: Bucket created successfully `local/lakehouse`
```
MinIO console: http://localhost:9001

5. dbt cannot connect to Spark (`failed to connect`)
```bash
docker logs spark | grep SCHEMA_NOT_FOUND
curl -X POST -H "Content-Type: application/json" -d '{"namespace":["default"]}' localhost:8181/v1/namespaces
```
Every Spark Thrift session opens with `USE default`, so the `default` namespace must exist.

6. dbt model or test fails
```bash
docker compose --env-file ../.env run --rm --no-deps airflow-scheduler /opt/dbt-venv/bin/dbt run
docker compose --env-file ../.env run --rm --no-deps airflow-scheduler /opt/dbt-venv/bin/dbt test
```
Read the first `ERROR` / `FAIL` line: it names the model or test.

7. Airflow DAG not shown or not running
```bash
docker exec airflow-scheduler airflow dags list-import-errors
docker exec airflow-scheduler airflow dags list
```
New DAGs start **paused**: unpause `lakehouse_pipeline` in the UI (http://localhost:8088).

8. Trino query fails
```bash
docker exec -it trino trino --execute "SHOW SCHEMAS FROM iceberg"
```
Use `iceberg.silver.silver_orders`, not `lakehouse.silver...`.

9. Clean up/Start fresh

Clean up 
```bash
./start-all.sh reset                               
```
Start fresh
```bash
./start-all.sh
```
