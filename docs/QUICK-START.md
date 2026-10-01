# 🚀 Quick Start

All commands run from the `docker/` folder.

## Prerequisites

- Docker + Docker Compose v2
- Internet access for the first build (dependencies, MinIO source build)
- 32 GB RAM recommended; 40 GB+ free disk

## 1. Start everything

```bash
cd docker
./start-all.sh
```

Creates `../.env` from `.env.example` if missing, downloads dependencies, builds the images,
starts every service, registers the connectors, waits for bronze, then runs `dbt run` + `dbt test`.

## 2. Open the UIs

| UI | URL | Login (defaults from `.env.example`) |
|---|---|---|
| Airflow | http://localhost:8088 | `admin` / `admin` |
| MinIO console | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| AKHQ (Kafka) | http://localhost:8080 | none |
| Trino | http://localhost:8085 | any username |
| Spark | http://localhost:4040 | none |

In Airflow, unpause `lakehouse_pipeline` to run dbt every hour.

## 3. Simulate data changes

```bash
docker build -t data-generator ./data-generator
docker run --rm --network data-platform --env-file ../.env data-generator
```

## 4. Query with Trino

```bash
docker exec -it trino trino
```

```sql
-- bronze: CDC events per operation
SELECT op, count(*) AS events FROM iceberg.bronze.orders GROUP BY op ORDER BY op;

-- bronze: latest events
SELECT op, coalesce(after.order_id, before.order_id) AS order_id, after.order_status,
       from_unixtime(source.ts_ms / 1000) AS committed_at
FROM iceberg.bronze.orders ORDER BY source.lsn DESC LIMIT 10;

-- silver: current state
SELECT order_status_name, count(*) AS orders FROM iceberg.silver.silver_orders GROUP BY 1 ORDER BY 2 DESC;

-- gold: marts
SELECT * FROM iceberg.gold.gold_daily_revenue ORDER BY order_date DESC LIMIT 10;
SELECT customer_id, first_name, last_name, order_count, lifetime_revenue
FROM iceberg.gold.gold_customer_lifetime_value ORDER BY lifetime_revenue DESC LIMIT 10;
```

## 5. Run dbt manually

```bash
docker compose --env-file ../.env run --rm --no-deps airflow-scheduler /opt/dbt-venv/bin/dbt run
docker compose --env-file ../.env run --rm --no-deps airflow-scheduler /opt/dbt-venv/bin/dbt test
```

## 6. Stop or reset

```bash
docker compose --env-file ../.env --profile '*' stop   # stop, keep data
./start-all.sh reset                                    # remove containers + volumes (deletes all data)
```

## Run a single layer

```bash
docker compose --env-file ../.env --profile core --profile lakehouse up -d
```

Profiles: `core` (Postgres, MinIO), `streaming` (Kafka, Schema Registry, AKHQ, Kafka Connect),
`lakehouse` (MinIO, Iceberg REST, Spark), `bi` (Trino), `orchestration` (Airflow).

---

Something not working? See [TROUBLESHOOTING.md](TROUBLESHOOTING.md).
