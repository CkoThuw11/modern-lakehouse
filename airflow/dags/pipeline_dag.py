"""lakehouse_pipeline: wait for Spark Thrift, then dbt run -> dbt test.

dbt runs from the Airflow image's own venv. Its project/profiles/target paths and the
Spark Thrift host come from env vars set in docker-compose.yaml.
"""

from __future__ import annotations

import os
import socket
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor

SPARK_THRIFT_HOST = os.environ["SPARK_THRIFT_HOST"]
SPARK_THRIFT_PORT = int(os.environ["SPARK_THRIFT_PORT"])
DBT = "/opt/dbt-venv/bin/dbt"


def _spark_thrift_is_up() -> bool:
    """True once the Spark Thrift server accepts a TCP connection."""
    try:
        with socket.create_connection((SPARK_THRIFT_HOST, SPARK_THRIFT_PORT), timeout=5):
            return True
    except OSError:
        return False


with DAG(
    dag_id="lakehouse_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=1)},
    tags=["dbt", "cdc"],
) as dag:
    # reschedule: free the worker slot between pokes; give up after 10 min.
    wait_for_spark_thrift = PythonSensor(
        task_id="wait_for_spark_thrift",
        python_callable=_spark_thrift_is_up,
        mode="reschedule",
        poke_interval=30,
        timeout=600,
    )
    dbt_run = BashOperator(task_id="dbt_run", bash_command=f"{DBT} run")
    dbt_test = BashOperator(task_id="dbt_test", bash_command=f"{DBT} test")

    wait_for_spark_thrift >> dbt_run >> dbt_test
