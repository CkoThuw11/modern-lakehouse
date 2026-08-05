<Context>
    <Philosophy>
        This is a portfolio-grade data platform, not a throwaway POC. It is judged on
        reproducibility, isolation, and clarity — a stranger should clone it, run one command
        per layer, and understand the design.

        We prioritize:
        1. Reproducibility over convenience (pinned versions, no manual state).
        2. Isolation over "just run everything" (one layer proven before the next stacks on).
        3. Explicit contracts over magic (connectors, catalogs, and profiles are declared, not implied).
        4. Correctness of data over speed of ingestion (land every event, deduplicate deliberately).
    </Philosophy>
    <Scope>
        A single-host Docker Compose platform: Postgres CDC → Debezium → Kafka → Kafka Connect
        Iceberg sink (→ bronze, directly) → dbt-on-Spark-Thrift (silver/gold Iceberg tables) →
        Trino (interactive query), orchestrated by Airflow.
    </Scope>
    <Actors>
        - The Operator (human): makes design/dataset decisions, verifies acceptance criteria.
        - The Agent (you): writes compose, Dockerfiles, configs, connectors, SQL/dbt, DAGs; proves each phase.
    </Actors>
    <Non_Goals>
        - No Kubernetes, no cloud managed services. Single-host Compose only.
        - No PostgREST (removed from scope).
        - No Spark Structured Streaming for ingestion. Ingestion is Kafka Connect only.
    </Non_Goals>
</Context>

<Behavioral_Guidelines>
    <Think_Before_Coding>
        - State assumptions explicitly before writing config or code. If uncertain, ask.
        - If multiple interpretations exist (image choice, connector format, catalog wiring), present them — don't pick silently.
        - If a simpler wiring exists, say so. Push back when a request adds complexity the plan doesn't need.
        - If something is unclear (a version compatibility, an S3A setting), stop and name it. Do not guess at infra config.
    </Think_Before_Coding>
    <Simplicity_First>
        - Minimum config that makes the phase's acceptance criteria pass. Nothing speculative.
        - No services, connectors, or profiles beyond the current phase.
        - No "flexibility" (extra catalogs, unused exporters, spare buckets) that wasn't requested.
        - No error handling or retries for scenarios that can't occur in a single-host POC.
        - If a compose service or SQL model is bloated, rewrite it smaller.
    </Simplicity_First>
    <Surgical_Changes>
        - Touch only the service/config the task requires. Don't "improve" adjacent compose blocks, other connectors, or unrelated SQL.
        - Match existing style (indentation, naming, env-var patterns) even if you'd do it differently.
        - When your change orphans an env var, volume, or import, remove that orphan — but leave pre-existing unused config alone; mention it instead.
        - Every changed line should trace to the current phase's task.
    </Surgical_Changes>
    <Goal_Driven_Execution>
        - Every phase already has acceptance criteria (see PROJECT_PLAN.md). Treat them as the pass/fail test; loop until they verify.
        - For multi-step work, state a brief plan with a verify step each:
          1. [step] → verify: [check]
          2. [step] → verify: [check]
        - Do not proceed to the next phase until the current phase's acceptance criteria pass. Debugging one layer beats debugging fifteen.
        - Commit after each green phase — each is a natural, revertable checkpoint.
    </Goal_Driven_Execution>
    <Tradeoff>
        These guidelines bias toward caution and isolation over speed. For trivial edits (a typo in a
        config comment, bumping one pinned version), use judgment.
    </Tradeoff>
</Behavioral_Guidelines>

<Architecture>
    <Data_Flow>
        [ Postgres (wal_level=logical) ]
               ⬇  logical replication
        [ Kafka Connect: Debezium source ]  → Postgres to Kafka (Avro + Schema Registry)
               ⬇
        [ Kafka ── Schema Registry ── AKHQ ]
               ⬇
        [ Kafka Connect: Iceberg sink (1 → 4) ]  → Kafka to `lakehouse.bronze.<table>` Iceberg tables
               ⬇  append every CDC event as-is, NO transformation                directly, via the
                                                                                  REST Catalog
        [ MinIO ]  single `lakehouse` bucket: warehouse/ (all Iceberg table data + metadata)
                       ◄──► [ Iceberg REST Catalog (JdbcCatalog + backend Postgres) ]
               ⬇  read bronze, write silver/gold
        [ Spark Thrift server ]  → SQL execution engine driven by dbt
               ⬇  dbt-spark (method: thrift)
        [ dbt ]  bronze → dedup → silver → gold  (Iceberg tables)
               ⬇
        [ Trino ]  → interactive query over gold

        Airflow orchestrates: verify connectors → dbt run → dbt test.
    </Data_Flow>
    <Ingestion_Model>
        Debezium emits every change. A Kafka Connect Iceberg sink connector appends events into
        `bronze` AS-IS — one row per CDC event, no intermediate file landing zone. Deduplication
        (latest record per key) happens ONLY in the dbt silver layer. There is no transformation,
        filtering, or dedup at sink time.
    </Ingestion_Model>
    <Layering>
        bronze  = raw CDC event log, one row per event, materialized as Iceberg
                  (Kafka-Connect-managed, NOT dbt-managed).
        silver  = dedup to latest CDC record per key, cleaned, typed, conformed (dbt-managed).
        gold    = business marts for BI (dbt-managed).
        The two dbt-managed layers are silver / gold; bronze is populated directly by ingestion.
    </Layering>
</Architecture>

<Requirements>
    <Deployment>
        - Orchestration: Docker Compose v2, single host.
        - Every image version PINNED. No `:latest`, ever.
        - Compose profiles group services: core, streaming, lakehouse, bi, orchestration.
        - All secrets/endpoints via `.env` (referenced in compose). Commit only `.env.example`.
    </Deployment>
    <Infrastructure_Stack>
        - Object store: MinIO (S3-compatible), single `lakehouse` bucket, `warehouse/` prefix (all Iceberg tables, every namespace).
        - Catalog: Iceberg REST Catalog (JdbcCatalog implementation), backed by a dedicated Postgres.
        - Source DB: PostgreSQL, wal_level=logical.
    </Infrastructure_Stack>
    <Ingestion_Stack>
        - Kafka in KRaft mode (no Zookeeper).
        - Schema Registry (Avro).
        - AKHQ for topic/schema visibility.
        - Kafka Connect with TWO connectors: Debezium Postgres source + one Apache-native Iceberg
          sink connector that fans out to all four bronze tables (topic-based route-field routing).
    </Ingestion_Stack>
    <Lakehouse_Stack>
        - Table format: Apache Iceberg, cataloged via the Iceberg REST Catalog, stored on MinIO.
        - Compute: Spark with the Thrift server (port 10000) as dbt's execution engine.
        - Transformation: dbt with the `dbt-spark` adapter, `method: thrift`.
        - Query engine: Trino (Iceberg catalog → REST Catalog + MinIO).
    </Lakehouse_Stack>
    <Serving_Stack>
        - Orchestration: Airflow (own metadata DB).
        - Serving/query: Trino (interactive SQL over gold). No separate BI tool (Metabase dropped).
    </Serving_Stack>
</Requirements>

<Repository_Structure>
    Every service lives under `docker/`. Services needing customization get a folder with a
    custom Dockerfile plus config/asset files. Services needing no customization are declared
    INLINE in `docker-compose.yaml` with no folder. Trino is the one exception: config folder,
    NO Dockerfile. `dbt/` and `airflow/` sit at the top level, outside `docker/`.

    ```
    data-platform/
    ├── README.md
    ├── PROJECT_PLAN.md
    ├── AGENTS.md
    ├── .env.example
    ├── docker/
    │   ├── docker-compose.yaml
    │   ├── Makefile
    │   ├── download-dependencies.sh  # fetches every jar/plugin into downloads/ (gitignored)
    │   ├── build-all-images.sh       # verifies downloads/, then builds every custom image
    │   ├── downloads/        # gitignored — jars + confluent-hub/downloaded plugin dirs, shared by all custom images
    │   ├── postgres/         # CUSTOM: Dockerfile + init.sql
    │   ├── iceberg-rest/     # CUSTOM: Dockerfile (context: .., COPYs downloads/postgresql.jar)
    │   ├── kafka-connect/    # CUSTOM: Dockerfile (context: ..) + connectors/{debezium-postgres-source.json,iceberg-sink.json}
    │   ├── spark/            # CUSTOM: Dockerfile (context: ..) + configs/spark-defaults.conf
    │   └── trino/            # CONFIG ONLY — NO Dockerfile — configs/catalog/iceberg.properties
    ├── dbt/                  # top level — project only, no Dockerfile
    │   ├── dbt_project.yml
    │   ├── profiles.yml      # spark adapter, thrift method
    │   └── models/{silver,gold}/   # bronze is Kafka-Connect-managed, not a dbt model
    └── airflow/
        └── dags/
    ```

    INLINE-in-compose (no folder): akhq, kafka, schema-registry, minio (+ bucket-init mc container),
    iceberg-rest-db (catalog backend Postgres).
</Repository_Structure>

<Constraints>
    <DO>
        <Universal>
            - DO make every phase independently verifiable; prove acceptance criteria before proceeding.
            - DO pin every image version and record the compatible set in the README.
            - DO reference all credentials/endpoints from `.env`; keep `.env.example` current.
            - DO commit after each green phase.
        </Universal>
        <Docker>
            - DO place custom services in `docker/<service>/` with their own Dockerfile.
            - DO declare non-custom services inline in `docker-compose.yaml`.
            - DO assign every service to the correct Compose profile.
            - DO add healthchecks and `depends_on` conditions so layers start in dependency order.
        </Docker>
        <Data>
            - DO land CDC into `bronze` unchanged (one row per event) via the Iceberg sink connector.
            - DO deduplicate in the dbt silver layer: order by CDC timestamp/LSN, keep the latest per key.
            - DO validate MinIO/S3 connectivity once in the Iceberg REST Catalog phase and reuse the IDENTICAL endpoint/credentials in Spark, Kafka Connect, and Trino.
            - DO validate schema evolution deliberately (add a source column; confirm nothing breaks downstream).
            - DO keep `dbt_project.yml` and `profiles.yml` beside `models/`; set `DBT_PROFILES_DIR` to the dbt folder.
            - DO give the Iceberg sink connector an explicit `iceberg.control.topic` (`control-iceberg`);
              with one connector instance a single control topic is correct. If ever split back into
              multiple connectors, each needs its own to avoid cross-connector consumer-group interference.
        </Data>
    </DO>
    <DO_NOT>
        <Docker>
            - DO NOT use `:latest` for any image.
            - DO NOT give Trino a Dockerfile — it is config-mounted only.
            - DO NOT create a folder for a service that needs no customization.
            - DO NOT bring up all services at once on a single host; use profiles.
        </Docker>
        <Data>
            - DO NOT use Spark Structured Streaming for ingestion. Ingestion is Kafka Connect only.
            - DO NOT transform, filter, or deduplicate the CDC payload at sink time — `bronze` is a
              faithful, append-only event log. (The one permitted SMT is `InsertField$Value` adding a
              `_cdc_topic` routing key so the single sink connector can fan topics out to their tables;
              it adds a lineage column, it does not alter the CDC data.)
            - DO NOT let dbt silver emit duplicate keys; a key must resolve to exactly one latest row.
            - DO NOT diverge MinIO/S3 endpoint or credential settings between the REST Catalog, Spark, Kafka Connect, and Trino.
            - DO NOT use the databricks fork `io.tabular.iceberg.connect` `v0.6.19` for multi-table
              routing — its `iceberg.tables.route-field`/SMT path reproducibly fails silently (upstream
              apache/iceberg#13457: data files written, no snapshot committed, zero rows, zero errors),
              and that fork is frozen at 0.6.19. Use the Apache-native `org.apache.iceberg.connect`
              connector (1.9.1), whose rewritten commit coordinator supports one connector fanning out
              to the four bronze tables.
        </Data>
        <Repo>
            - DO NOT commit a real `.env` or any secret. 
            - DO NOT modify running-container state by hand as a "fix"; change the config/Dockerfile and rebuild.
            - DO NOT hardcord the port or credentials in the connections or the docker-compose configs, put the real value for example PORT 5432 with ${POSTGRES_PORT} and add the actual value in .env* file
        </Repo>
    </DO_NOT>
    <SUCCESS_METRICS>
        1. "The Isolation Test": Can you bring up any single layer via its profile and pass that layer's acceptance criteria with the others down?
        2. "The Replay Test": After `make reset`, can you rebuild from scratch to a green end-to-end run following the phase sequence, with no manual state?
        3. "The Dedup Test": After N CDC events for one key land in `bronze`, does silver resolve to exactly one latest row?
        4. "The Swap Test": Can you replace a peripheral (e.g. Grafana, or MinIO with real S3) by changing only that service's config, without touching upstream layers?
    </SUCCESS_METRICS>
</Constraints>

<Coding_Styles>
    <Docker_Compose>
        - Service names: `kebab-case`, matching the folder name for custom services.
        - Images pinned: `image: name:x.y.z`. Custom services `build:` their folder.
        - Env from `.env`; no inline secrets.
        - Every service gets a `profiles:` entry and, where meaningful, a `healthcheck:`.
    </Docker_Compose>
    <SQL_and_dbt>
        - SQL keywords UPPERCASE (e.g. `SELECT ... FROM ... WHERE ...`).
        - Tables `snake_case`, plural.
        - dbt models named by layer intent (e.g. `silver_orders`, `gold_daily_revenue`). No `bronze_*`
          dbt models — bronze tables are created and populated directly by Kafka Connect.
        - silver dedup pattern: window by key, order by CDC ts/LSN desc, keep row_number = 1.
        - Every model has `dbt test` assertions (uniqueness on key, not-null on required columns).
        - Materialize dbt layers as Iceberg tables (incremental where it earns its keep).
    </SQL_and_dbt>
    <Connectors_JSON>
        - One JSON file per connector under `kafka-connect/connectors/`.
        - Connector `name` matches the file stem (e.g. `debezium-postgres-source`, `iceberg-sink`).
        - Register via Make targets (`register-source`, `register-sink`), not ad-hoc curl in docs.
    </Connectors_JSON>
    <Airflow_Python>
        - DAG ids `snake_case`; tasks idempotent and re-runnable.
        - No business logic in DAGs beyond orchestration: verify connectors → `dbt run` → `dbt test`.
        - Sensors/retries only where a real dependency or transient failure justifies them.
        - Reusable custom operators live in `airflow/dags/operators/` (e.g. `DbtSparkOperator`);
          DAGs compose them, not inline `BashOperator`/`docker exec`. dbt is baked into the Airflow
          image (`/opt/dbt-venv`) and driven over Spark Thrift — no Docker socket, no `DockerOperator`.
    </Airflow_Python>
</Coding_Styles>

<Workflow>
    <Phase_Execution_Protocol>
        1. Read the phase in PROJECT_PLAN.md; restate its goal and acceptance criteria.
        2. State a short plan (step → verify) before writing anything.
        3. Implement only what the phase requires (custom service folder or inline compose entry).
        4. Bring up the relevant profile; run the acceptance check.
        5. If it fails, fix the config/Dockerfile (never hand-patch a container) and re-verify.
        6. On green, commit with a message naming the phase. Then, and only then, proceed.
    </Phase_Execution_Protocol>
    <New_Service_Checklist>
        1. Decide: does it need customization (custom image, mounted configs, init assets)?
           - Yes → create `docker/<service>/` with a Dockerfile (exception: Trino = config only).
           - No  → declare it inline in `docker-compose.yaml`, no folder.
        2. Assign it to the correct profile.
        3. Add pinned image, env from `.env`, healthcheck, and `depends_on` ordering.
        4. Add/extend `.env.example` with any new keys.
        5. Define the acceptance check that proves the service works in isolation.
        6. Update the README service table and, if relevant, the runbook.
    </New_Service_Checklist>
</Workflow>