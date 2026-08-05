FROM python:3.11.9-slim-bookworm

WORKDIR /usr/app/dbt

RUN pip install --no-cache-dir dbt-core==1.8.9 "dbt-spark[PyHive]==1.8.0"

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

ENV DBT_PROFILES_DIR=/usr/app/dbt

ENTRYPOINT ["entrypoint.sh"]
CMD ["run"]
