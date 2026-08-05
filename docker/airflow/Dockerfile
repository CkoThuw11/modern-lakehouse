FROM apache/airflow:2.10.4-python3.11

# dbt is baked into the image (run by the DAG's BashOperator — no Docker socket, no
# docker exec). It goes into a DEDICATED virtualenv, NOT Airflow's own Python env:
# Airflow 2.10 tightly pins click/jinja2/pydantic/protobuf and dbt-core 1.8 pins its
# own conflicting ranges, so co-installing them into one env reliably breaks Airflow.
# The venv isolates the two dependency trees; the DAG calls /opt/dbt-venv/bin/dbt.
# Versions mirror docker/dbt-spark exactly (dbt-spark[PyHive] → pure-sasl, no OS deps).
# /opt is root-owned, so create the venv dir as root and hand it to the airflow user
# before installing (the image must ultimately run as the unprivileged airflow user).
USER root
RUN install -d -o airflow -g 0 /opt/dbt-venv
USER airflow
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir \
        dbt-core==1.8.9 \
        "dbt-spark[PyHive]==1.8.0" \
        pyspark==3.5.3

ENV DBT_VENV=/opt/dbt-venv

# git: dbt runs a git check in `dbt debug` (fails the command without it) and needs
# it for `dbt deps` (git-based packages). Added last so the pip layer stays cached.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
USER airflow
