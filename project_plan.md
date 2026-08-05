# Data Platform — Project Plan

A local CDC → lakehouse platform built with Docker Compose. Postgres changes are streamed via Debezium into Kafka, sunk directly into `bronze` Iceberg tables (no intermediate file landing zone) by a single Kafka Connect Iceberg sink connector, then transformed with dbt running on a Spark Thrift server into `silver`/`gold` Iceberg tables cataloged via an Iceberg REST Catalog, queried through Trino, orchestrated by Airflow, and monitored with Prometheus + Grafana.

**Deployment:** Docker Compose, single host
**Purpose:** POC + portfolio piece
**Ingestion:** Kafka Connect only (no Spark Streaming) — land every CDC event in bronze as-is, deduplicate later in dbt
**Transformation:** dbt with the Spark engine via the Spark Thrift server
**Query engine:** Trino (dedicated)

---

## 1. Architecture

```
                    ┌─────────────┐
                    │  Postgres   │  (source OLTP, wal_level=logical)
                    └──────┬──────┘
                           │ logical replication
                    ┌──────▼───────────┐
                    │   Kafka Connect  │
                    │  Debezium source │  (Postgres → Kafka)
                    └──────┬───────────┘
                           │ CDC events
      ┌────────────────────▼────────────────────┐
      │  Kafka  ──  Schema Registry  ──  AKHQ    │
      └────────────────────┬────────────────────┘
                           │
                    ┌──────▼───────────┐
                    │   Kafka Connect  │
                    │  Iceberg sink    │  (Kafka → lakehouse.bronze.<table>, directly, one
                    │   (1 → 4 tables) │   connector fanning out to four tables)
                    └──────┬───────────┘
                           │ append every CDC event as-is (no transformation)
                    ┌──────▼──────────┐        ┌──────────────────────┐
                    │      MinIO      │◄──────►│  Iceberg REST Catalog│ (JdbcCatalog)
                    │  lakehouse/     │        │  + its Postgres      │
                    │  warehouse/     │        └──────────────────────┘
                    └──────┬──────────┘
                           │ read bronze, write silver/gold
                    ┌──────▼──────────────┐
                    │  Spark Thrift srv   │  (SQL execution engine)
                    └──────┬──────────────┘
                           │ dbt-spark (thrift)
                    ┌──────▼──────┐
                    │     dbt     │  bronze → dedup → silver → gold
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │    Trino    │  (interactive query over gold)
                    └─────────────┘

  Airflow orchestrates: verify connectors → dbt run/test
  Prometheus scrapes all → Grafana dashboards (lag, job health, duration)
```

**Ingestion model:** Debezium emits every change; a single Kafka Connect Iceberg sink connector appends those events directly into `lakehouse.bronze.<table>` **as-is** — one row per CDC event, no dedup at write time, no intermediate file landing zone. Deduplication (latest record per key) happens later in the dbt **silver** layer.

**Layering:** **bronze** (raw CDC event log, Iceberg, Kafka-Connect-managed) → **silver** (dedup, cleaned, typed, dbt-managed) → **gold** (business marts, dbt-managed).

---

## 2. Prerequisites & resource budget

This is a **heavy** stack — 15+ containers. Plan for it before you start.

- **RAM:** 32 GB strongly recommended. 16 GB is the painful floor (run services in groups, not all at once). JVM services (Kafka, Spark Thrift, Trino, Iceberg REST Catalog) are the memory hogs.
- **Disk:** 40 GB+ free (Docker images ~15 GB; MinIO data grows).
- **CPU:** 4+ cores.
- **Software:** Docker + Docker Compose v2, `make`, `git`, an editor. Optional: `jq`, `kcat` for Kafka debugging.

**Single-host survival tactic:** use Compose **profiles** so layers come up independently (`--profile core`, `--profile streaming`, `--profile lakehouse`, `--profile bi`, `--profile obs`). Don't try to bring up everything at once.

---

## 3. Repository structure

Every service lives under `docker/`. Services that need customization get their own folder with a **custom Dockerfile** plus their config/asset files. Services that need no customization (akhq, kafka, schema-registry, minio, postgres/statsd exporters, and the Iceberg REST Catalog's backend Postgres) are declared **directly in `docker-compose.yaml`** with no folder. Trino, Prometheus, and Grafana are the exceptions: config folders but **no Dockerfile**. `dbt/` and `airflow/` sit at the top level, outside `docker/`.

```
data-platform/
├── README.md
├── PROJECT_PLAN.md
├── .env.example
│
├── docker/
│   ├── docker-compose.yaml
│   ├── Makefile
│   │
│   ├── postgres/                     # CUSTOM (Dockerfile)
│   │   ├── Dockerfile
│   │   └── init.sql
│   │
│   ├── iceberg-rest/                 # CUSTOM (Dockerfile)
│   │   └── Dockerfile                # adds Postgres JDBC driver to apache/iceberg-rest-fixture classpath
│   │
│   ├── kafka-connect/                # CUSTOM (Dockerfile)
│   │   ├── Dockerfile
│   │   └── connectors/
│   │       ├── debezium-postgres-source.json
│   │       └── iceberg-sink.json
│   │
│   ├── spark/                        # CUSTOM (Dockerfile)
│   │   ├── Dockerfile
│   │   └── configs/
│   │       └── spark-defaults.conf
│   │
│   ├── trino/                        # config only — NO Dockerfile
│   │   └── configs/catalog/iceberg.properties
│   │
│   ├── airflow/                      # CUSTOM (Dockerfile) — apache/airflow + dbt venv
│   │   └── Dockerfile
│   │
│   ├── prometheus/                   # config only — NO Dockerfile
│   │   └── configs/                  # prometheus.yml, jmx/*.yml, statsd-mapping.yml
│   │
│   └── grafana/                      # config only — NO Dockerfile
│       ├── provisioning/            # datasource + dashboard providers
│       └── dashboards/             # *.json
│
├── dbt/                              # top level, outside docker/
│   ├── dbt_project.yml
│   ├── profiles.yml                  # spark adapter, thrift method
│   └── models/
│       ├── bronze/
│       ├── silver/
│       └── gold/
│
└── airflow/                          # top level, outside docker/
    └── dags/
        ├── pipeline_dag.py
        └── operators/                # reusable DbtSparkOperator
```

Notes:
- **akhq, kafka, schema-registry, minio (+ bucket-init), iceberg-rest-db (catalog backend Postgres), postgres-exporter, statsd-exporter** → declared inline in `docker-compose.yaml`, no folders. **prometheus** and **grafana** are inline services but mount config from `docker/prometheus/` and `docker/grafana/` (config only, no Dockerfile — like Trino).
- **kafka-connect/connectors/** holds the two connectors: the Debezium Postgres **source** and the Apache Iceberg **sink** (one connector fanning out to all four bronze tables).
- **dbt/profiles.yml** lives in the project; set `DBT_PROFILES_DIR` to the dbt folder so it's picked up.

---

## 4. Execution phases

Each phase lists its **goal**, **tasks**, and **acceptance criteria** — the test that proves it works before you move on.

### Phase 0 — Scaffolding
**Goal:** empty repo that runs its `make` targets.
- Create the structure in Section 3, `.env.example`, `.gitignore`.
- Skeleton `docker/docker-compose.yaml` with one shared network + named volumes.
- `docker/Makefile` targets: `up`, `down`, `logs`, `ps`, `reset`, with profile support. (Run make from inside `docker/`, or set the compose file path.)
- **Acceptance:** `make up` / `make down` run cleanly on the empty compose file; repo pushes to GitHub.

### Phase 1 — Foundation: MinIO
**Goal:** object store with a single lakehouse bucket.
- Add MinIO + Console inline in compose. Add a one-shot `mc` init container that creates one `lakehouse` bucket. Every Iceberg table (bronze/silver/gold namespaces alike) lives under one `warehouse/` prefix, managed entirely by the Iceberg REST Catalog — not pre-created, materializes on first write. Buckets aren't free to manage at cloud scale (per-bucket policy/lifecycle/replication config, account bucket-count limits); prefixes are.
- **Acceptance:** Console (`:9001`) reachable; the `lakehouse` bucket exists; test file uploads/downloads under a prefix.

### Phase 2 — Metadata: Iceberg REST Catalog
**Goal:** Iceberg catalog on MinIO, validated before streaming exists.
- Add the `iceberg-rest-db` Postgres backend inline. Build the custom **iceberg-rest** image: `FROM apache/iceberg-rest-fixture`, add the Postgres JDBC driver to the classpath (the fixture only bundles SQLite). Configure via `CATALOG_*` env vars: `CATALOG_CATALOG__IMPL=org.apache.iceberg.jdbc.JdbcCatalog`, JDBC URI/user/password pointing at `iceberg-rest-db`, `CATALOG_WAREHOUSE=s3://lakehouse/warehouse`, `CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO`, and MinIO endpoint/credentials/path-style settings.
- Create a throwaway test table via the REST API to confirm MinIO wiring end-to-end.
- **Acceptance:** REST catalog starts clean (`/v1/config` responds); test table creates via the REST API and its files land under the `lakehouse` bucket's `warehouse/` prefix. *(Isolates MinIO/S3 wiring bugs now, before streaming sits on top.)*

### Phase 3 — Source database: Postgres
**Goal:** OLTP source with CDC enabled and real-ish data.
- Build the custom **postgres** image with `wal_level=logical`, `max_replication_slots`, `max_wal_senders`. `init.sql` creates the schema + loads seed data.
- diving to 3 script files in bike-store-data folder, merge these into one unified script file init.sql and make sure the script works for PostgreSQL
- Pick a dataset with a natural update pattern (orders, inventory, users) so CDC has interesting changes to show.
- **Acceptance:** Postgres up; `SHOW wal_level;` → `logical`; seed tables populated.

### Phase 4 — Streaming backbone
**Goal:** Kafka + schema management + visibility.
- Add Kafka (KRaft mode — no Zookeeper), Schema Registry, and AKHQ inline in compose.
- **Acceptance:** AKHQ (`:8080`) shows the broker healthy; a test topic is visible; Schema Registry `/subjects` responds.

### Phase 5 — CDC source: Debezium
**Goal:** Postgres row changes flowing into Kafka as Avro.
- Build the custom **kafka-connect** image with both the Debezium Postgres plugin and the Iceberg Kafka Connect sink plugin installed. Add `connectors/debezium-postgres-source.json` (Avro + Schema Registry). Add a `make register-source` target.
- Register the source; change a row in Postgres; watch the topic in AKHQ.
- **Acceptance:** insert/update/delete in Postgres → matching CDC event in the Kafka topic (AKHQ), with a registered Avro schema.

### Phase 6 — Sink to lake: Iceberg sink connector
**Goal:** land CDC events directly in `bronze` Iceberg tables (no transformation, no intermediate file landing zone).
- Add a single Apache-native Iceberg Kafka Connect sink connector (`connectors/iceberg-sink.json`) that fans out to all four `bronze.<table>` targets via `iceberg.tables` + `iceberg.tables.route-field` (`_cdc_topic`, injected by an `InsertField$Value` SMT) + per-table `route-regex`, over the Iceberg REST Catalog. Add a `make register-sink` target (also creates the `bronze` namespace). Use the Apache-native `org.apache.iceberg.connect` connector (1.9.1), not the databricks fork's 0.6.19 — multi-table routing silently fails there (apache/iceberg#13457).
- **Acceptance:** CDC events for a topic appear as rows in the corresponding `lakehouse.bronze.<table>` Iceberg table; a new Postgres row produces a new row within the commit interval.

### Phase 7 — Compute engine: Spark + Thrift server
**Goal:** a SQL execution engine dbt can drive, wired to Iceberg + the REST Catalog + MinIO.
- Build the custom **spark** image with Iceberg runtime jars, S3A, and the Iceberg REST Catalog configured in `configs/spark-defaults.conf` (`spark.sql.catalog.<name>.type=rest`, `uri`, S3FileIO settings). Start the **Spark Thrift server** (port 10000).
- Confirm Spark can create an Iceberg table via the REST catalog and read/write it through the Thrift server.
- **Acceptance:** connect to the Thrift server (beeline or a JDBC client); a test Iceberg table creates via SQL and its files land in MinIO.

### Phase 8 — Transformation: dbt via Spark Thrift
**Goal:** bronze → dedup → silver → gold as Iceberg tables. (Bronze itself is populated directly by the Phase 6 Kafka Connect Iceberg sink connector, not by dbt.)
- Configure `dbt/profiles.yml` with the **spark** adapter, `method: thrift`, pointing at the Spark Thrift server. `dbt_project.yml` and `profiles.yml` sit beside `models/`.
- Build models under `models/`:
  - **silver** — read `bronze`, deduplicate to the latest CDC record per key (window by key, order by CDC ts/LSN desc), clean, type, conform.
  - **gold** — 1–2 business marts.
- Add `dbt test` assertions (uniqueness, not-null).
- **Acceptance:** `dbt run` builds silver/gold as Iceberg tables; `dbt test` passes; silver correctly collapses multiple CDC events for one key to a single latest row.

### Phase 9 — Query engine: Trino
**Goal:** fast interactive SQL over gold.
- Add Trino inline in compose, mounting `trino/configs/catalog/iceberg.properties` (Iceberg catalog → REST Catalog + MinIO). No Dockerfile.
- **Acceptance:** Trino queries a gold table and returns rows; interactive latency noticeably better than the Spark Thrift path.

### Phase 10 — Orchestration: Airflow
**Goal:** one DAG runs the pipeline.
- Airflow (top-level folder) with its own metadata DB. DAG: verify source + sink connector running → `dbt run` → `dbt test`. Add sensors/retries.
- **Acceptance:** triggering the DAG runs the chain green; a scheduled run works unattended.

> **Note:** an earlier plan had a Metabase BI phase here. Metabase was dropped — the
> platform's serving layer is Trino (interactive SQL over `gold`), and its visualization
> surface is Grafana on Prometheus (operational metrics). There is no separate BI tool.

### Phase 11 — Observability: Prometheus + Grafana
**Goal:** ops visibility, with metrics that actually mean something.
- Prometheus (inline, `obs` profile) scraping: Kafka + Kafka Connect (JMX exporter agent), source Postgres (`postgres_exporter`), Airflow (StatsD → `statsd_exporter`), Spark (native PrometheusServlet), MinIO (built-in metrics). Grafana (inline) with a provisioned datasource + dashboards: **CDC Pipeline Health** (connector task counts, sink consumer lag, replication-slot lag, broker throughput, MinIO usage) and **Airflow Orchestration** (scheduler heartbeat, task success/failure, run/task duration).
- **Acceptance:** Grafana shows live metrics from at least Kafka + Airflow; the sink consumer-lag panel reacts when you pause the Iceberg sink connector.

### Phase 12 — End-to-end validation *(the money shot)*
**Goal:** prove one change flows through every layer.
- Update one row in Postgres, then confirm in order:
  1. Debezium event in Kafka (AKHQ)
  2. New row for that change in `lakehouse.bronze.<table>`
  3. After `dbt run`: silver dedup → gold reflect it
  4. Trino query over `gold` shows the new value
  5. Grafana shows the pipeline activity
- **Acceptance:** all five confirmed. Capture it as a GIF/screenshots for the README — the single most compelling portfolio artifact.

### Phase 13 — Portfolio polish
**Goal:** make it hireable, not just runnable.
- Strong `README.md`: architecture diagram, one-command run, tech rationale, screenshots, the E2E demo.
- `docs/runbook.md`: start/stop per profile, common failures + fixes.
- Short demo recording (Loom/GIF). A paragraph on *why* each tool was chosen and what you'd change at production scale.
- **Acceptance:** a stranger can clone, read the README, run it, and understand the design in under 10 minutes.

---

## 5. Sequenced checklist

```
[ ] 0.  Repo scaffold + Makefile (inside docker/)
[ ] 1.  MinIO + single lakehouse bucket (warehouse/ prefix, all Iceberg tables)
[ ] 2.  Iceberg REST Catalog (custom, Postgres-backed) — validate MinIO wiring with test table
[ ] 3.  Source Postgres (custom, wal_level=logical) + init.sql seed
[x] 4.  Kafka (KRaft) + Schema Registry + AKHQ
[x] 5.  Kafka Connect (custom) — Debezium source → CDC in AKHQ
[x] 6.  Iceberg sink connector — lands CDC events directly in bronze Iceberg tables
[x] 7.  Spark (custom) + Thrift server — Iceberg + REST catalog
[x] 8.  dbt via Spark Thrift — bronze (dedup) → silver → gold + tests
[x] 9.  Trino (config only) — query gold
[x] 10. Airflow — DAG runs connectors check → dbt run → dbt test
[x] 11. Prometheus + Grafana — ops dashboards (metrics that make sense)
[ ] 12. End-to-end validation (record it)
[ ] 13. README + runbook + demo
```

---

## 6. Risks & watch-items

- **Memory pressure** is the #1 failure mode on a single host. Use profiles; shut down layers you're not actively working on.
- **MinIO endpoint / path-style config** trips up the REST Catalog, Spark, and Trino constantly — validate it once in Phase 2 and reuse the exact same settings everywhere.
- **CDC deduplication is now central.** Since the Iceberg sink connector lands every change into `bronze` with no dedup, the silver layer *must* collapse to the latest state per key (order by CDC timestamp/LSN, keep last). Get this logic right and test it in Phase 8.
- **Sink commit timing** — the Iceberg sink connector batches commits on an interval; with defaults you may wait a while for new snapshots. Tune `iceberg.control.commit.interval-ms` so a POC feels responsive.
- **Multi-table routing depends on the connector build.** `iceberg.tables.route-field` + SMT-based routing reproducibly failed silently on the databricks fork's frozen 0.6.19 (open upstream bug apache/iceberg#13457: files written, no snapshot committed, zero rows, zero errors). Use the Apache-native `org.apache.iceberg.connect` connector (1.9.1), whose rewritten commit coordinator supports one connector fanning out to all four tables. If snapshots ever stop committing, verify the sink is the Apache-native class and not the tabular/databricks one.
- **Version drift** between Iceberg, Spark, and the metastore causes cryptic errors; pin a known-compatible set and note it in the README. Pin all image versions from the start.