FROM apache/airflow:2.10.4-python3.11

USER root
RUN install -d -o airflow -g 0 /opt/dbt-venv
USER airflow
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir \
        dbt-core==1.8.9 \
        "dbt-spark[PyHive]==1.8.0"
