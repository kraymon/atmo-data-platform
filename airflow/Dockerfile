FROM apache/airflow:3.1.7-python3.11

USER root

RUN apt-get update && apt-get install -y \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    duckdb \
    dbt-core==1.7.4 \
    dbt-duckdb==1.7.4

RUN pip install openmeteo-requests