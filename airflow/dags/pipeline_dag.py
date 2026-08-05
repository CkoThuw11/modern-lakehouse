"""Lakehouse transformation pipeline — clean architecture.

Readiness gates (Spark Thrift up + Debezium source and Iceberg sink connectors
RUNNING)  →  validate dbt connection  →  dbt run  →  dbt test.

Transformation tasks use the custom ``DbtSparkOperator`` (see
``operators/dbt_spark_operator.py``), which runs the dbt baked into the Airflow
image (``/opt/dbt-venv``) against the Spark Thrift server — no Docker socket, no
docker exec. The connector/readiness checks are plain ``PythonSensor``s.
"""

from __future__ import annotations

import os
import socket
from datetime import timedelta

import requests
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.dates import days_ago

from operators.dbt_spark_operator import (
    DbtSparkConfig,
    DbtSparkDebugOperator,
    DbtSparkRunOperator,
    DbtSparkTestOperator,
)

# --- readiness-check targets (same service names / ports as the rest of the stack) ---
SPARK_THRIFT_HOST = os.environ.get("SPARK_THRIFT_HOST", "spark")
SPARK_THRIFT_PORT = int(os.environ.get("SPARK_THRIFT_PORT", "10000"))
CONNECT_URL = os.environ.get("KAFKA_CONNECT_URL", "http://kafka-connect:8083")
SOURCE_CONNECTOR = "debezium-postgres-source"
SINK_CONNECTOR = "iceberg-sink"

# One config, shared by every dbt task via default_args (environment-aware).
dbt_spark_config = DbtSparkConfig.from_env()


def _spark_thrift_is_up() -> bool:
    """True once the Spark Thrift server accepts a TCP connection on :10000."""
    try:
        with socket.create_connection((SPARK_THRIFT_HOST, SPARK_THRIFT_PORT), timeout=5):
            return True
    except OSError:
        return False


def _connector_running(name: str) -> bool:
    """True once the named connector AND all its tasks report RUNNING."""
    try:
        resp = requests.get(f"{CONNECT_URL}/connectors/{name}/status", timeout=5)
        resp.raise_for_status()
    except requests.RequestException:
        return False
    status = resp.json()
    if status.get("connector", {}).get("state") != "RUNNING":
        return False
    tasks = status.get("tasks", [])
    return bool(tasks) and all(t.get("state") == "RUNNING" for t in tasks)


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    # Shared by all DbtSparkOperator tasks.
    "config": dbt_spark_config,
}

with DAG(
    dag_id="lakehouse_pipeline",
    description="Readiness gates → dbt run → dbt test (custom DbtSparkOperator)",
    default_args=default_args,
    start_date=days_ago(1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "spark", "iceberg", "cdc", "clean-architecture"],
) as dag:

    start_pipeline = EmptyOperator(task_id="start_pipeline")

    # Readiness gates. `reschedule` frees the worker slot between pokes; each gate
    # gives up after ~10 min so a genuinely-down dependency fails the run.
    sensor_kwargs = dict(mode="reschedule", poke_interval=30, timeout=60 * 10)

    wait_spark_thrift = PythonSensor(
        task_id="wait_for_spark_thrift",
        python_callable=_spark_thrift_is_up,
        **sensor_kwargs,
    )
    check_source = PythonSensor(
        task_id="check_source_connector",
        python_callable=_connector_running,
        op_args=[SOURCE_CONNECTOR],
        **sensor_kwargs,
    )
    check_sink = PythonSensor(
        task_id="check_sink_connector",
        python_callable=_connector_running,
        op_args=[SINK_CONNECTOR],
        **sensor_kwargs,
    )

    # dbt tasks read host/port from the shared config (target must match profiles.yml).
    validate_dbt_connection = DbtSparkDebugOperator(
        task_id="validate_dbt_connection",
        target="dev",
    )
    dbt_run = DbtSparkRunOperator(
        task_id="dbt_run",
        target="dev",
    )
    dbt_test = DbtSparkTestOperator(
        task_id="dbt_test",
        target="dev",
    )

    end_pipeline = EmptyOperator(task_id="end_pipeline")

    (
        start_pipeline
        >> [wait_spark_thrift, check_source, check_sink]
        >> validate_dbt_connection
        >> dbt_run
        >> dbt_test
        >> end_pipeline
    )
