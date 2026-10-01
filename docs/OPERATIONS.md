# ⚙️ Operations

Run Spark SQL via beeline (default catalog `lakehouse`, so `bronze.orders` = `lakehouse.bronze.orders`):

```bash
docker exec -it spark /opt/spark/bin/beeline -u jdbc:hive2://localhost:10000
```

## 1. Maintenance Tasks

| Task | Frequency | Command |
|------|-----------|---------|
| **Compaction (rewrite_data_files)** | Daily (bronze), weekly (silver) | `CALL lakehouse.system.rewrite_data_files('bronze.orders');` |
| **Expire snapshots** | Weekly | `CALL lakehouse.system.expire_snapshots(table => 'bronze.orders', retain_last => 10);` |
| **Rewrite manifests** | Weekly | `CALL lakehouse.system.rewrite_manifests('bronze.orders');` |

The sink commits every 15 s, so bronze collects small files and snapshots fastest. Repeat per table.

---

## 2. Schema Evolution Matrix

Changes made **in Postgres**, carried Debezium → Schema Registry (`BACKWARD` compatibility) →
bronze (`evolve-schema-enabled`) → silver. Nullable columns are optional Avro fields (default
`null`); `NOT NULL` columns are required fields without a default.

| Change Type | Supported? | Notes |
|-------------|-----------|-------|
| Add nullable column | ✅ | Bronze adds the field. Silver selects explicit columns: add it to the model, then `dbt run --full-refresh --select silver+` |
| Drop column | ✅ | Bronze keeps it (`NULL` for new events). Remove it from the silver model |
| Rename nullable column | ⚠️ | Seen as drop + add: the old column stops filling. Update the silver model |
| Rename or add `NOT NULL` column | 🚫 | Required field without a default is not `BACKWARD`-compatible → Debezium task fails |
| Widen type (`int` → `bigint`) | ✅ | Avro `int` → `long` promotion is compatible |
| Incompatible type (`int` → `text`) | 🚫 | Not an Avro promotion → Debezium task fails |
| `ADD COLUMN ... DEFAULT` on existing rows | ⚠️ | No CDC events for existing rows: they stay `NULL` until updated |

CDC events (default replica identity): **updates** carry `before = null`; **deletes** carry the
key in `before`, with `NOT NULL` columns as placeholders (`0`) and nullable ones `NULL`.

---

## 3. Future Improvements

| Area | Improvement |
|------|-------------|
| Maintenance | Weekly Airflow DAG running the section 1 tasks |
| Data | Data simulator for continuous CDC traffic |
| Data quality | Validity, referential and reconciliation tests; `dbt build` so bad silver blocks gold; alerts on failure |
| Modeling | SCD2 history from bronze (customers, products); gold revenue on completed orders only |
| Ingestion | Add `production.products`; Debezium signalling for incremental snapshots |
| Operations | Runbooks for backfill, adding a table, Kafka replay, and volume backup/restore |
| Serving | BI dashboard on gold (Trino) |
