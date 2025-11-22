FROM astrocrpublic.azurecr.io/runtime:3.1-1
# Create a virtual environment for dbt using absolute paths and direct pip execution
RUN python -m venv /usr/local/airflow/dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-bigquery
