# Data Platform

A local CDC → lakehouse platform, built with Docker Compose. Postgres row changes are
streamed via Debezium into Kafka, sunk directly into Iceberg `bronze` tables by a Kafka Connect
Iceberg sink (no intermediate file landing zone), transformed with dbt running on a Spark Thrift
server into `silver`/`gold` Iceberg tables, queried through Trino, orchestrated by Airflow, and
monitored with Prometheus + Grafana.

> **Status: work in progress.** This README tracks the platform as it stands today (Phases 0–11
> of 13 complete). See [`project_plan.md`](project_plan.md) for the full phase-by-phase plan and
> [`AGENTS.md`](AGENTS.md) for the guidelines this build follows.

---

## Architecture (current + planned)

```
                    ┌─────────────┐
                    │  Postgres   │  (source OLTP, wal_level=logical)      ✅ done
                    └──────┬──────┘
                           │ logical replication
                    ┌──────▼───────────┐
                    │   Kafka Connect  │
                    │  Debezium source │  (Postgres → Kafka)                ✅ done
                    └──────┬───────────┘
                           │ CDC events
      ┌────────────────────▼────────────────────┐
      │  Kafka  ──  Schema Registry  ──  AKHQ    │                          ✅ done
      └────────────────────┬────────────────────┘
                           │
                    ┌──────▼───────────┐
                    │   Kafka Connect  │
                    │  Iceberg sink    │  (Kafka → Iceberg bronze, one       ✅ done
                    │  (1 → 4 tables)  │   connector fanning out to 4 tables)
                    └──────┬───────────┘
                           │ append every CDC event as-is (no transformation)
                    ┌──────▼──────────┐        ┌──────────────────────┐
                    │      MinIO      │◄──────►│ Iceberg REST Catalog │      ✅ done
                    │  lakehouse/     │        │  (JdbcCatalog +      │
                    │  warehouse/     │        │   backend Postgres)  │
                    └──────┬──────────┘        └──────────────────────┘
                           │ read bronze, write silver/gold
                    ┌──────▼──────────────┐
                    │  Spark Thrift srv   │  (SQL execution engine)         ✅ done
                    └──────┬──────────────┘
                           │ dbt-spark (thrift)
                    ┌──────▼──────┐
                    │     dbt     │  bronze → dedup → silver → gold         ✅ done
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │    Trino    │  (interactive query over gold)          ✅ done
                    └─────────────┘

  Airflow orchestrates: verify connectors → dbt run/test                    ✅ done
  Prometheus scrapes all → Grafana dashboards (lag, connector, jobs)        ✅ done
```

**Ingestion model:** Debezium emits every change; a single Kafka Connect Iceberg sink connector
fans those events out (by topic) directly into four `lakehouse.bronze.<table>` Iceberg tables
**as-is** — one row per CDC event, no dedup and no intermediate file landing zone. Deduplication (latest record per key)
happens later in the dbt **silver** layer.

**Layering:** **bronze** (raw CDC event log, Iceberg, Kafka-Connect-managed) → **silver** (dedup +
cleaned + typed, dbt-managed) → **gold** (business marts, dbt-managed).

---

## What's built so far

| # | Phase | Status |
|---|-------|--------|
| 0 | Repo scaffold + Makefile | ✅ |
| 1 | MinIO (single `lakehouse` bucket) | ✅ |
| 2 | Iceberg REST Catalog (Postgres-backed) | ✅ |
| 3 | Source Postgres (BikeStores, CDC-enabled) | ✅ |
| 4 | Kafka (KRaft) + Schema Registry + AKHQ | ✅ |
| 5 | Debezium CDC source | ✅ |
| 6 | Iceberg sink connector — CDC lands directly in `bronze` | ✅ |
| 7 | Spark + Thrift server | ✅ |
| 8 | dbt silver/gold (bronze is Kafka-Connect-managed) | ✅ |
| 9 | Trino — interactive query over gold | ✅ |
| 10 | Airflow — DAG orchestrates connector checks → dbt run/test | ✅ |
| 11 | Prometheus + Grafana — observability (metrics that make sense) | ✅ |
| 12–13 | E2E validation → portfolio polish | not started |

### Services running today

| Service | Image | Profile | Purpose |
|---|---|---|---|
| `minio` | `minio/minio:RELEASE.2025-04-22T22-12-26Z` | `core`, `lakehouse` | S3-compatible object store, single `lakehouse` bucket |
| `minio-init` | `minio/mc:RELEASE.2025-04-16T18-13-26Z` | `core`, `lakehouse` | One-shot bucket creation |
| `iceberg-rest` | custom (`apache/iceberg-rest-fixture:1.9.1` + Postgres JDBC driver) | `lakehouse` | Iceberg REST Catalog, JdbcCatalog backend |
| `iceberg-rest-db` | `postgres:15` | `lakehouse` | Catalog metadata store |
| `postgres` | custom (`postgres:15` + BikeStores schema/seed) | `core` | Source OLTP database, `wal_level=logical` |
| `kafka` | `apache/kafka:3.7.1` | `streaming` | Kafka broker, KRaft mode (no Zookeeper) |
| `schema-registry` | `confluentinc/cp-schema-registry:7.6.1` | `streaming` | Avro schema management for Kafka topics |
| `akhq` | `tchiotludo/akhq:0.24.0` | `streaming` | Web UI for Kafka topics/schemas |
| `kafka-connect` | custom (`confluentinc/cp-kafka-connect:7.6.1` + Debezium Postgres + Apache Iceberg sink plugins) | `streaming` | Kafka Connect worker — 1 Debezium source + 1 Iceberg sink connector (fans out to 4 bronze tables) |
| `spark` | custom (`apache/spark:3.5.3` + Iceberg runtime + Hadoop-AWS/S3A) | `lakehouse` | Spark Thrift server (`:10000`), SQL engine for dbt |
| `dbt-spark` | custom (`python:3.11.9-slim-bookworm` + `dbt-core`/`dbt-spark[PyHive]`) | `lakehouse` | One-shot dbt runner — `models/` mounted from `../dbt`, drives the Spark Thrift server |
| `trino` | `trinodb/trino:462` (config-mounted, no Dockerfile) | `bi` | Interactive SQL over the same Iceberg REST Catalog + MinIO; `iceberg` catalog exposes `bronze`/`silver`/`gold` |
| `airflow-db` | `postgres:15` | `orchestration` | Airflow metadata database |
| `airflow-init` | custom (`apache/airflow:2.10.4` + dbt in a venv) | `orchestration` | One-shot: `db migrate` + create admin user, then exits |
| `airflow-webserver` | custom (`apache/airflow:2.10.4` + dbt in a venv) | `orchestration` | Airflow UI (`:8088`), LocalExecutor |
| `airflow-scheduler` | custom (`apache/airflow:2.10.4` + dbt in a venv) | `orchestration` | Runs the `lakehouse_pipeline` DAG: connector checks → dbt run → dbt test |
| `prometheus` | `prom/prometheus:v2.54.1` (config-mounted, no Dockerfile) | `obs` | Scrapes Kafka/Connect (JMX), Postgres, Airflow, Spark, MinIO; UI on `:9090` |
| `grafana` | `grafana/grafana:11.2.0` (provisioned, no Dockerfile) | `obs` | Dashboards on Prometheus (`:3000`): CDC Pipeline Health + Airflow Orchestration |
| `postgres-exporter` | `quay.io/prometheuscommunity/postgres-exporter:v0.15.0` | `obs` | Source Postgres metrics — CDC replication-slot lag, connections |
| `statsd-exporter` | `prom/statsd-exporter:v0.27.1` | `obs` | Receives Airflow StatsD, exposes it to Prometheus |

### One command, from scratch

To build and run the **entire** platform end-to-end with a single command:

```bash
cd docker
make bootstrap
```

`make bootstrap` (see [`docker/bootstrap.sh`](docker/bootstrap.sh)) does everything in order:
creates `../.env` from `.env.example` if missing → `./download-dependencies.sh` →
`./build-all-images.sh` → `make up PROFILES="core lakehouse streaming bi"` → waits for the Kafka
Connect worker to load its plugins, then `make register-source` + `make register-sink` → waits for
the sink's first commit to auto-create all four `bronze` tables → `make dbt-run` + `make dbt-test`.
It's idempotent — re-running reuses the existing `.env`, downloads, and connectors. When it
finishes it prints every service endpoint (including Trino on `:8085`).

### Bringing layers up in isolation

```bash
cd docker
cp ../.env.example ../.env        # first time only
make up PROFILES=core             # postgres + minio
make up PROFILES=lakehouse        # minio + iceberg-rest(+db) + spark + dbt-spark
make up PROFILES=streaming        # kafka + schema-registry + akhq + kafka-connect
make up PROFILES="lakehouse bi"   # lakehouse + trino (Trino needs the REST Catalog + MinIO)
make up PROFILES=orchestration    # airflow-db + init + webserver + scheduler
make up PROFILES=obs              # prometheus + grafana + postgres/statsd exporters
make ps
make down                         # or: make reset (also wipes volumes)
```

The first `make up` (or `make build`) runs `./download-dependencies.sh` automatically, fetching
every jar/plugin the custom images need into `docker/downloads/` (gitignored — see below), then
`./build-all-images.sh` builds every custom image via `docker compose build`.

**Registering the Debezium source connector** (needs `core` + `streaming` both up, since it
bridges Postgres and Kafka):

```bash
make up PROFILES="core streaming"
make register-source              # registers debezium-postgres-source via the Connect REST API
```

**Registering the Iceberg sink connector** (needs `core` + `lakehouse` + `streaming`, since it
bridges Kafka and the Iceberg REST Catalog + MinIO). This creates the `bronze` namespace, then
registers the single `iceberg-sink` connector, which routes each source topic to its own
`bronze.<table>`:

```bash
make up PROFILES="core lakehouse streaming"
make register-source
make register-sink                # creates the bronze namespace, registers the iceberg-sink connector
```

> **Known quirk:** on a cold start with a large initial Debezium snapshot in flight, the Iceberg
> sink connector can stall at "committed to 0 table(s)" for a while (near-zero CPU, no errors —
> just idle). If bronze row counts haven't started climbing within a couple of minutes of
> registering, `docker restart kafka-connect` reliably unsticks it. This appears to be cold-start
> flakiness under initial-backlog load, not a config issue.

**Running dbt** (needs `lakehouse` up, plus `core` + `streaming` if you want fresh CDC data
flowing into `bronze`). dbt runs in its own one-shot container (`docker/dbt-spark/`), which
bind-mounts the top-level `dbt/` project — no separate install step, no host Python required:

```bash
make up PROFILES="core lakehouse streaming"
make dbt-run     # builds silver → gold as Iceberg tables (bronze already exists via Kafka Connect)
make dbt-test    # uniqueness/not-null assertions
```

**Querying gold with Trino** (needs `lakehouse` up so the REST Catalog + MinIO are reachable).
Trino exposes a single `iceberg` catalog pointed at the same REST Catalog, so `bronze`, `silver`,
and `gold` all show up as schemas:

```bash
make up PROFILES="lakehouse bi"
docker exec -it trino trino     # interactive Trino CLI (or point any JDBC client at :8085)
```
```sql
SHOW SCHEMAS FROM iceberg;                                   -- bronze, silver, gold, ...
SELECT * FROM iceberg.gold.gold_daily_revenue ORDER BY 1 DESC LIMIT 10;
SELECT count(*) FROM iceberg.bronze.orders;
```

The Trino web UI is on `:8085`. Interactive latency over `gold` is noticeably better than the
Spark Thrift path.

**Orchestrating the pipeline with Airflow** (profile `orchestration`; the DAG drives the
`lakehouse` + `streaming` layers, so bring those up too for a full run):

```bash
make up PROFILES="core lakehouse streaming orchestration"
make register-source && make register-sink   # so the connector checks pass
# open the UI, log in (admin/admin by default), unpause "lakehouse_pipeline", trigger it
```

The Airflow UI is on **http://localhost:8088**. The single DAG, `lakehouse_pipeline`, runs:
`start` → (`wait_for_spark_thrift` + `check_source_connector` + `check_sink_connector`,
reschedule-mode sensors) → `validate_dbt_connection` → `dbt_run` → `dbt_test` → `end`, with retries.
It's scheduled `@hourly` (`catchup=False`) and can also be triggered manually. The dbt tasks use a
custom **`DbtSparkOperator`** (`airflow/dags/operators/`) that runs the dbt baked into the Airflow
image (`/opt/dbt-venv`) against the Spark Thrift server — no Docker socket, no docker exec.

**Observability with Prometheus + Grafana** (profile `obs`; scrapes the other layers, so bring the
relevant ones up too):

```bash
make up PROFILES="core lakehouse streaming orchestration obs"
```

- **Prometheus** — http://localhost:9090 (Status → Targets shows all scrape jobs healthy).
- **Grafana** — http://localhost:3000 (login `admin`/`admin` by default), with two provisioned
  dashboards under the *Data Platform* folder:
  - **CDC Pipeline Health** — Debezium/Iceberg connector running-task counts, **Iceberg sink
    consumer lag**, Postgres **replication-slot retained WAL** (CDC health), Kafka broker
    throughput, MinIO storage usage.
  - **Airflow Orchestration** — scheduler heartbeat, task success/failure counts, per-task and
    DAG-run durations.

Try the "lag reacts" demo: pause the sink (`curl -X PUT localhost:8083/connectors/iceberg-sink/pause`),
generate CDC (`UPDATE production.stocks SET quantity = quantity + 1;` in Postgres), and watch the
sink consumer-lag panel climb; `resume` it and watch it drain.

---

## Key design decisions

**Single MinIO bucket, not five.** Cloud object stores don't charge per bucket, but bucket *count*
has real costs: per-bucket policy/lifecycle/replication config, and provider-side bucket-count
limits. Every Iceberg table (`bronze`, `silver`, `gold` namespaces alike) lives under one
`lakehouse` bucket's `warehouse/` prefix, managed entirely by the Iceberg REST Catalog — no other
prefixes are pre-created or hand-managed.

**Iceberg REST Catalog instead of Hive Metastore.** The plan originally called for Hive
Metastore. We swapped to a REST catalog (`apache/iceberg-rest-fixture`) backed by Postgres via
its `JdbcCatalog` implementation. This drops the entire Hadoop/`hadoop-aws`/AWS-SDK
jar-compatibility surface that Hive Metastore + S3A is notorious for, in exchange for a single
lightweight service. The fixture image only bundles a SQLite JDBC driver, so `docker/iceberg-rest/`
has a small Dockerfile that layers the Postgres JDBC driver onto its classpath.

**PostgreSQL config as a real file, not inline flags.** `docker/postgres/configs/postgresql.conf`
sets `wal_level=logical`, `max_wal_senders`, `max_replication_slots` (required for Debezium),
plus sane connection/memory/logging defaults — loaded via `-c config_file=...` rather than a long
list of `-c key=value` CMD args.

**BikeStores dataset, ported from SQL Server to PostgreSQL.** The seed data
(`docker/postgres/configs/init.sql`) merges the three original SQL Server scripts
(`bike-store-data/`) into one script: `IDENTITY` → `GENERATED BY DEFAULT AS IDENTITY`,
`tinyint` → `SMALLINT`, stripped `SET IDENTITY_INSERT`/`USE` statements, fixed a batch of missing
statement terminators, and added `setval()` calls to resync sequences after the explicit-ID
inserts. Orders/order_items/stocks give CDC a natural, ongoing update pattern once Debezium is
wired up in Phase 5.

**Kafka in KRaft mode, no Zookeeper.** `apache/kafka:3.7.1` runs broker + controller roles in one
node with a static `CLUSTER_ID` (so `make reset` + rebuild is reproducible, not a random UUID each
start). Two listeners: `PLAINTEXT` for in-network services (Kafka Connect, Schema Registry, AKHQ)
and `PLAINTEXT_HOST` on `:9094` for host-side debugging (`kcat`, etc). Schema Registry
(`confluentinc/cp-schema-registry`) and AKHQ (`tchiotludo/akhq`, configured entirely via the
`AKHQ_CONFIGURATION` env var — no config folder needed) both point at it. Broker tuning
(`auto.create.topics.enable=true`, `min.insync.replicas=1`, `log.retention.hours=168`) matches a
single-broker POC: auto-create keeps ad-hoc topics low-friction, ISR=1 is the only valid value with
one broker, and a 7-day retention bounds disk growth without discarding data before you can inspect
it.

**Debezium Postgres source, connector config templated (not committed with secrets).**
`docker/kafka-connect/` builds `confluentinc/cp-kafka-connect:7.6.1` with the Debezium Postgres
plugin (`2.5.4-2`) and the Apache Iceberg Kafka Connect sink plugin (`1.9.1`, see below). The connector config
(`kafka-connect/connectors/debezium-postgres-source.json`) uses `${POSTGRES_USER}` /
`${POSTGRES_PASSWORD}` / `${POSTGRES_PORT}` / `${POSTGRES_DB}` placeholders instead of literal
credentials; `make register-source` sources `.env` and runs it through `envsubst` before POSTing
to the Connect REST API, so no secret ever lands in git. It uses `pgoutput` (Postgres's built-in
logical decoding plugin, no extra Postgres-side install needed) and scopes CDC to `sales.orders`,
`sales.order_items`, `production.stocks`, `sales.customers`. Both key and value are Avro,
registered against Schema Registry.

**`listen_addresses = '*'` had to be added to Postgres.** The custom `postgresql.conf` from Phase 3
never set it, so Postgres defaulted to listening on `localhost` only — invisible from inside the
container itself (`pg_isready` still passed), but refused connections from any other container.
This didn't surface until Kafka Connect tried to reach it over the Docker network in this phase.

**`make down`/`make reset` were silently no-ops for every profiled service.** `docker compose down`
without `--profile` flags only resolves containers in the default (no-profile) service set — since
every service in this project carries a `profiles:` entry, a bare `down -v` left everything running
and, worse, `reset` didn't actually wipe volumes. Fixed by passing `--profile '*'` (activates every
profile defined in the file) on both targets.

**No hardcoded ports/credentials in connections or compose configs.** Every port a service
advertises to *other* services (not just its host mapping) is a `.env` variable —
`POSTGRES_PORT`, `MINIO_PORT`, `MINIO_CONSOLE_PORT` — substituted at compose-parse time, including
inside the MinIO `command:` itself so the container's actual listening port and the variable never
drift apart. Connector JSON configs (which Compose can't substitute into) go through `envsubst` at
registration time instead (`make register-source`, `make register-sink`), so credentials and ports
never get committed as literal values.

**No `raw/` landing zone — Kafka Connect writes Iceberg tables directly.** The original design had
a Confluent S3 sink connector land plain JSON files in a `raw/` prefix, reparsed into Iceberg by
Spark/dbt. That's been replaced with the Apache Iceberg Kafka Connect sink connector
(`org.apache.iceberg.connect.IcebergSinkConnector`, Apache Iceberg 1.9.1 — see below), which
writes structured Iceberg rows straight into `lakehouse.bronze.*` tables via the REST Catalog. There's no file-parsing step, no JSON schema-inference risk, and one
fewer layer to reason about: bronze is now a real, directly-queryable Iceberg table the moment CDC
events land, not a folder of JSON blobs waiting to be reparsed.

**One Iceberg sink connector fanning out to four tables, on the Apache-native connector.** The
sink routes many topics to many tables from a single connector instance via `iceberg.tables`
(the four target tables) + `iceberg.tables.route-field` + per-table `iceberg.table.<t>.route-regex`,
matched against a `_cdc_topic` field injected by a Kafka Connect `InsertField$Value` SMT (the sink
needs a record field to route on; the raw Debezium topic name is the natural key). This exact
routing path reproducibly failed silently on the **databricks fork's** pinned `v0.6.19`
(`io.tabular.iceberg.connect.IcebergSinkConnector`) — data files written, zero snapshots ever
committed (`committed to 0 table(s)` every cycle), matching upstream bug
[apache/iceberg#13457](https://github.com/apache/iceberg/issues/13457). The databricks fork is
frozen at 0.6.19 and Apache publishes no ready-to-use plugin zip, so we moved to the **Apache-native**
connector (`org.apache.iceberg.connect.IcebergSinkConnector`) at 1.9.1 — its rewritten commit
coordinator is the maintained continuation of that code (see the downloads note below for how the
plugin is assembled). Config keys are otherwise identical to the databricks fork; only the
`connector.class` package changes. The `_cdc_topic` field lands as an extra bronze column carrying
CDC lineage; silver ignores it.

**The Iceberg sink uses a single `iceberg.control.topic`.** With exactly one connector instance
there is one coordinator/worker group, so a single control topic (`control-iceberg`) is correct —
no cross-connector contention to isolate. (When this was four separate connectors, each needed its
own control topic to avoid consumer-group rebalance churn; collapsing to one connector removes that
requirement.)

**`iceberg.tables` values must NOT include the `lakehouse` prefix.** Spark's `lakehouse.bronze.orders`
is a *Spark-local catalog alias* (`spark.sql.catalog.lakehouse`) — it doesn't exist as an actual
REST Catalog namespace segment. The Kafka Connect sink talks to the REST Catalog directly (no Spark
layer in between), so `iceberg.tables: "lakehouse.bronze.orders"` gets parsed as a *nested*
namespace `["lakehouse","bronze"]`, silently landing tables one level deeper than Spark/dbt ever
look for them. The correct value is just `"bronze.orders"`.

**One shared `docker/downloads/` folder, not one per service.** Every custom image needs jars or
plugins fetched over the network at build time (Spark's Iceberg/Hadoop-AWS jars, the Iceberg REST
Catalog's Postgres JDBC driver, Kafka Connect's Debezium plugin via `confluent-hub` and the
Iceberg sink plugin assembled from Maven jars). Rather than `RUN curl`/`confluent-hub install`
inside each Dockerfile — which re-downloads on every rebuild and requires network access during
`docker build` — `download-dependencies.sh` pre-fetches everything into one gitignored
`docker/downloads/` folder, and each Dockerfile just `COPY`s from it. This means
`docker/iceberg-rest`, `docker/kafka-connect`, and `docker/spark` all build with `context: .` (the
`docker/` root, not their own subfolder) and an explicit `dockerfile:` path, since Docker can't
`COPY` from outside a build's context. `build-all-images.sh` verifies the expected files/plugin
directories exist before building anything, and fails fast with a clear message (`Please run:
./download-dependencies.sh`) if they don't. Apache publishes no ready-to-use Iceberg Kafka Connect
plugin zip (its build instructions require cloning the repo and running Gradle), and the databricks
prebuilt fork is frozen at 0.6.19 with an open multi-table-routing bug — so `download-dependencies.sh`
**assembles the plugin from the connector's exact Maven runtime closure** instead: it runs a pinned
`maven` container (`dependency:copy-dependencies`, runtime scope) over the Apache-native
`iceberg-kafka-connect` + `-events` + `iceberg-aws` + `iceberg-parquet` + `iceberg-orc` 1.9.1
coordinates, dumping the standalone (NON-shaded) iceberg jars and their deps (plain Avro/Parquet,
`httpclient5`, etc.) into `downloads/iceberg-kafka-connect/`, then adds the `iceberg-aws-bundle`
(AWS SDK for S3FileIO) and Hadoop's shaded client jars (`hadoop-client-api`/`-runtime`, which supply
`org.apache.hadoop.conf.Configuration` for Iceberg's Parquet writer). The kafka-connect Dockerfile
`COPY`s that whole directory as one plugin. **Note:** the shaded `iceberg-spark-runtime` fat jar is
*not* reused here — it relocates Avro under `org.apache.iceberg.shaded`, so the connector's event
classes hit a `NoSuchMethodError` at commit time; the standalone jars are required. No Gradle build,
no third-party prebuilt zip — just pinned Maven artifacts.

**Spark's Thrift server can't default to the `lakehouse` Iceberg catalog.** Setting
`spark.sql.defaultCatalog=lakehouse` seemed natural, but `HiveThriftServer2` opens every new
session in `<default catalog>.default`, and Iceberg REST catalogs don't have a `default` namespace
until one is explicitly created — so every connection failed at handshake with
`NoSuchNamespaceException`, before any query could run. Left the default catalog as Spark's
built-in `spark_catalog` (whose `default` database always exists) and fully qualify Iceberg tables
as `lakehouse.<namespace>.<table>` instead.

**dbt-spark (thrift) has no real 3-level `catalog.schema.table` addressing.** The `dbt-spark`
adapter (v1.8, generic thrift target — not Databricks) hard-codes `database == schema`; there's no
`catalog:` profile field that gets wired into the relation. Workaround: set the dbt custom
`schema` itself to `lakehouse.silver` / `lakehouse.gold` (a literal string containing a dot).
`SparkRelation` never quotes the schema, so it renders unmodified into `create table
lakehouse.silver.silver_orders ...` and `create schema if not exists lakehouse.silver` — which is
exactly a 3-level Iceberg identifier and namespace-create call, even though dbt-spark has no
first-class concept of "catalog." `macros/generate_schema_name.sql` overrides dbt's default
`<target_schema>_<custom_schema>` concatenation so the dotted string passes through untouched.
(`bronze` doesn't need this treatment — Kafka Connect creates its own namespace directly against
the REST Catalog and never goes through dbt/Spark at all.)

**Silver reads `before`/`after` as native Iceberg struct columns — no JSON parsing needed.**
Because the Iceberg sink connector auto-creates each bronze table's schema from the Avro schema
Debezium registered (not from sampling JSON values), `before`/`after` are always real `STRUCT`
columns from row one, even before any delete/update event has ever occurred. (An earlier iteration
of this project read raw JSON files with `spark.read.json`, whose schema inference collapses an
all-null `before` field to `STRING` instead of `STRUCT` — that whole class of problem doesn't
exist once ingestion is Iceberg-native.)

**`decimal.handling.mode: double` added to the Debezium source connector.** Debezium's default
("precise") mode encodes Postgres `DECIMAL` columns (`list_price`, `discount`) as unscaled
`BigDecimal` bytes in the Avro schema — awkward to do arithmetic on directly. Switching to
`double` mode emits them as Avro doubles, which land as ordinary `DOUBLE` bronze columns; silver
casts them back to `DECIMAL` for the actual dollar math. Precision loss is negligible at this
scale and acceptable for a POC.

**Silver is `table` (full refresh), not `incremental`.** Every `dbt run` re-scans all of `bronze`
and recomputes the dedup window from scratch. Bronze volumes are small enough for a POC that this
is simpler and safer than incremental upserts — no merge-key edge cases, and correctness (exactly
one latest row per key) is trivially guaranteed by construction every run instead of needing to be
proven for a merge strategy.

**dbt runs in its own container (`docker/dbt-spark/`), not a host venv.** This matches the rest of
the repo's convention (every custom service gets `docker/<service>/` with its own Dockerfile) more
closely than a host-installed CLI would. `docker/dbt-spark/Dockerfile` is `python:3.11.9-slim-bookworm`
+ `pip install dbt-core==1.8.9 "dbt-spark[PyHive]==1.8.0"` (pinned directly in a `RUN` line — no
separate `requirements.txt`, since there's only one image consuming these two pins and a second
file just to hold them would add indirection without adding reuse). The top-level `dbt/` folder
(models, `profiles.yml`, `dbt_project.yml`) is bind-mounted into the container at
`/usr/app/dbt` rather than baked into the image, so editing a model doesn't require a rebuild —
only `docker compose build dbt-spark` needs to run again, and only when the Python dependencies
change. `entrypoint.sh` just execs `dbt "$@"`, so `make dbt-run` / `make dbt-test` are thin
wrappers around `docker compose run --rm dbt-spark run` / `... test`. `profiles.yml` has no
secrets (the Thrift server has no auth), so it's committed; host/port come from
`SPARK_THRIFT_HOST`/`SPARK_THRIFT_PORT` env vars — `spark:10000` inside the container network
(set in `docker-compose.yaml`), defaulting to `localhost:10000` so `profiles.yml` still works for
ad-hoc host-side `dbt` runs if someone pip-installs it locally for debugging.

**Trino is config-only — no Dockerfile, no credentials in git.** The stock `trinodb/trino:462`
image already runs a single-node coordinator out of the box, so `docker/trino/` ships only
`configs/catalog/iceberg.properties` (bind-mounted read-only at `/etc/trino/catalog`) — the built-in
`/etc/trino/{config,node,jvm}` defaults are left untouched. The Iceberg catalog uses `iceberg.catalog.type=rest`
pointed at the *same* `iceberg-rest:8181` REST Catalog Spark and Kafka Connect use (no Hive
Metastore), and Trino's **native S3** filesystem (`fs.native-s3.enabled=true`) against MinIO. The
MinIO endpoint/credentials aren't literals in the properties file — they're `${ENV:MINIO_PORT}` /
`${ENV:MINIO_ROOT_USER}` / `${ENV:MINIO_ROOT_PASSWORD}`, resolved from the container environment at
startup (Trino's built-in env substitution), so the settings stay byte-identical to the REST
Catalog / Spark / Kafka Connect wiring and no secret is committed. Because it depends on the
`lakehouse` services, Trino (profile `bi`) is brought up as `PROFILES="lakehouse bi"`. Host port
`8085` avoids the `:8080` AKHQ already uses.

**Airflow bakes dbt into a dedicated virtualenv, not its own Python env.** The DAG runs dbt via a
custom `DbtSparkOperator` — no Docker socket, no `docker exec`, no `DockerOperator`. That means dbt
has to live *inside* the Airflow image, but co-installing dbt-core 1.8 into Airflow 2.10's
environment reliably breaks it (they pin conflicting ranges of `click`/`jinja2`/`protobuf`/etc.). So
`docker/airflow/Dockerfile` installs `dbt-core` + `dbt-spark[PyHive]` + `pyspark` (same pins as
`docker/dbt-spark/`) into a separate `/opt/dbt-venv`, and the operator invokes `/opt/dbt-venv/bin/dbt`
directly — the two dependency trees never touch. dbt writes its `target/`/`logs/` to `/tmp` (via the
`DBT_TARGET_PATH`/`DBT_LOG_PATH` env vars, which apply to every dbt command unlike the `--target-path`
flag) so it never needs write access to the host-mounted, uid-mismatched project dir. `git` is
installed (dbt's `debug` check requires it). The connector "are you RUNNING?" checks and the
Spark-Thrift readiness gate are plain `PythonSensor`s (reschedule mode) hitting the Kafka Connect
REST API and a TCP probe — no extra provider packages. Airflow is a real deployment (webserver +
scheduler + `LocalExecutor` + its own `airflow-db` Postgres, sharing `AIRFLOW_FERNET_KEY` across
processes), not `standalone`, so it's representative of how you'd actually run it.

**dbt orchestration goes through a custom `DbtSparkOperator`, not inline `BashOperator`s.** The dbt
tasks are built from a small reusable operator library (`airflow/dags/operators/dbt_spark_operator.py`):
a `DbtSparkConfig` dataclass (project/profiles dirs, dbt binary, Spark Thrift host/port, artifact
paths — with a `from_env()` builder), a base `DbtSparkOperator` that assembles the dbt CLI
invocation (`--select`/`--exclude`/`--vars`/`--full-refresh`/`--threads` gated by which verb supports
them) and streams child output into the task log, and thin per-verb subclasses
(`DbtSparkRunOperator`, `DbtSparkTestOperator`, `DbtSparkDebugOperator`, `…Seed/Snapshot/Compile/Deps/Docs`).
The config is shared across all dbt tasks via `default_args`. This is the Docker-Compose adaptation
of a Kubernetes/Kyuubi `KubernetesPodOperator` design — same clean interface (`template_fields`,
configurable, environment-aware), minus the pod/Kyuubi machinery this single-host stack doesn't have.

**No Metabase — Grafana/Prometheus is the only visualization surface.** An earlier plan had a
Metabase BI phase; it was dropped. The *serving* layer is Trino (interactive SQL over `gold`), and
the *visualization* layer is Grafana on Prometheus for **operational** metrics. Adding a second
dashboarding tool for business charts wasn't worth the extra service for a POC whose story is the
pipeline itself, not the BI.

**Observability instruments each layer with the lightest thing that yields meaningful metrics.**
Rather than one heavyweight approach everywhere, each service uses whatever is idiomatic for it, and
the dashboards are curated to signals that mean something (not a metric firehose):
- **Kafka + Kafka Connect** — the `jmx_prometheus_javaagent` is *mounted* (from `downloads/`, not
  baked) into each JVM via `KAFKA_OPTS`, with curated JMX→Prometheus rules
  (`docker/prometheus/configs/jmx/`): broker throughput, and — the ones that matter for a pipeline —
  per-connector **running/failed/paused task counts** and **sink consumer lag**. (Gotcha handled: the
  agent in `KAFKA_OPTS` also loads for Kafka CLI tools, so the broker healthcheck clears `KAFKA_OPTS`
  to avoid a second JVM fighting for the agent's port.)
- **Source Postgres** — `postgres_exporter` surfaces the **CDC replication-slot lag** (retained WAL),
  the single best "is CDC keeping up?" signal.
- **Airflow** — emits **StatsD**, converted to Prometheus by `statsd_exporter` with a curated mapping
  (`statsd-mapping.yml`) that labels dag/task metrics and *drops* everything unmapped.
- **Spark** — native `PrometheusServlet` (config-only, no agent) on the driver UI.
- **MinIO** — built-in `/minio/v2/metrics` (just flip `MINIO_PROMETHEUS_AUTH_TYPE=public`).
Prometheus and Grafana are **config-only** (no Dockerfile, like Trino); Grafana's datasource and
dashboards are **provisioned** from files so they exist on first boot. Prometheus's one `.env`-driven
target (MinIO's port) is rendered from `${MINIO_PORT}` at container start, so nothing is hardcoded.

---

## Repository structure

```
.
├── README.md                # this file
├── project_plan.md          # full phase-by-phase plan
├── AGENTS.md                # guidelines this build follows
├── .env.example
├── bike-store-data/         # original SQL Server source scripts (reference only)
├── docker/
│   ├── docker-compose.yaml
│   ├── Makefile
│   ├── download-dependencies.sh  # fetches every jar/plugin into downloads/ (gitignored)
│   ├── build-all-images.sh       # verifies downloads/, then `docker compose build`s every custom image
│   ├── bootstrap.sh              # one command: download → build → up → register → dbt (make bootstrap)
│   ├── downloads/                # gitignored — jars + confluent-hub plugin dirs
│   ├── postgres/             # CUSTOM: Dockerfile + configs/{init.sql,postgresql.conf}
│   ├── iceberg-rest/         # CUSTOM: Dockerfile (context: .., COPYs downloads/postgresql.jar)
│   ├── kafka-connect/        # CUSTOM: Dockerfile (context: ..) + connectors/
│   │   └── connectors/{debezium-postgres-source,iceberg-sink}.json
│   ├── spark/                # CUSTOM: Dockerfile (context: ..) + configs/{spark-defaults,metrics}.properties
│   ├── dbt-spark/             # CUSTOM: Dockerfile + entrypoint.sh — one-shot dbt runner
│   ├── airflow/              # CUSTOM: Dockerfile (apache/airflow + dbt in /opt/dbt-venv)
│   ├── trino/                # CONFIG ONLY — no Dockerfile — configs/catalog/iceberg.properties
│   ├── prometheus/           # CONFIG ONLY — configs/{prometheus.yml, jmx/*.yml, statsd-mapping.yml}
│   └── grafana/              # CONFIG ONLY — provisioning/{datasources,dashboards} + dashboards/*.json
├── dbt/                       # top level — project only, no Dockerfile here
│   ├── dbt_project.yml
│   ├── profiles.yml           # spark adapter, method: thrift, no secrets
│   ├── macros/generate_schema_name.sql
│   └── models/{silver,gold}/  # *.sql + schema.yml (not_null/unique tests) — bronze is Kafka-Connect-managed
└── airflow/                   # top level — dags/ (bind-mounted into the Airflow image)
    └── dags/
        ├── pipeline_dag.py            # lakehouse_pipeline: readiness gates → dbt run → dbt test
        └── operators/                 # reusable custom operators
            └── dbt_spark_operator.py  # DbtSparkConfig + DbtSpark{Run,Test,Debug,…}Operator
```

---

## Prerequisites

- Docker + Docker Compose v2, `make`
- 32 GB RAM recommended (16 GB is a painful floor); 40 GB+ free disk
- Compose **profiles** (`core`, `streaming`, `lakehouse`, `bi`, `orchestration`, `obs`) let you
  bring up one layer at a time instead of all ~15+ containers at once

---

## Next up

Phase 12 — End-to-end validation: update one row in Postgres and confirm it flows through every
layer (Debezium → bronze → dbt silver/gold → Trino query → Grafana activity), captured as a
GIF/screenshots for the README.
