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
bronze (`evolve-schema-enabled`) → silver.

`BACKWARD` = the new schema must be able to read data written with the old schema.

| Change | Supported? | Note |
|---|---|---|
| Remove a field | ✅ | The new reader just ignores it in old data |
| Add a field **with** an Avro default | ✅ | When old data lacks the field, the reader fills in the default |
| Add a field **without** a default | 🚫 | Old data has no value for it, so the reader fails |
| `int` → `long` | ✅ | Avro allows this type promotion |
| `int` → `string` | 🚫 | Avro doesn't allow this promotion |

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
