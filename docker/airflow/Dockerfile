FROM apache/airflow:2.10.4-python3.11

USER root
RUN install -d -o airflow -g 0 /opt/dbt-venv
USER airflow
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir \
        dbt-core==1.8.9 \
        "dbt-spark[PyHive]==1.8.0"

ENV DBT_VENV=/opt/dbt-venv

# `dbt debug` (the DAG's validate_dbt_connection task) fails without git.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
USER airflow
