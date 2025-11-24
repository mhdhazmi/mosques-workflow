from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from datetime import datetime
import os

# --- CONFIGURATION ---
DBT_PROJECT_PATH = os.path.join(
    os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"), "dags/dbt"
)

# --- LOAD ENV VARS MANUALLY (Fallback) ---
# Because Airflow might not load .env automatically in all contexts
env_path = os.path.join(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"), ".env")
if os.path.exists(env_path):
    with open(env_path, "r") as f:
        for line in f:
            if line.strip() and not line.startswith("#"):
                key, value = line.strip().split("=", 1)
                os.environ[key] = value.strip('"').strip("'")

default_args = {
    "owner": "airflow",
    "retries": 2,  # tests require >= 2
}

with DAG(
    dag_id="meter_data_pipeline",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["meter_data", "dbt", "gcp"],  # makes tests happy, and self-docs
) as dag:

    # 1. Local Processing (The Grinder)
    run_etl = BashOperator(
        task_id="run_local_etl",
        bash_command=(
            "python "
            f"{os.environ.get('AIRFLOW_HOME', '/usr/local/airflow')}/include/etl_processor.py"
        ),
        env={
            "GOOGLE_API_KEY": os.environ.get("GOOGLE_API_KEY", ""),
            "ETL_LOW_MEMORY_MODE": os.environ.get("ETL_LOW_MEMORY_MODE", "false"),
            "ETL_ROW_GROUP_SIZE": os.environ.get("ETL_ROW_GROUP_SIZE", "250000"),
            "AIRFLOW_HOME": os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"),
            "PARQUET_COMPRESSION": os.environ.get("PARQUET_COMPRESSION", "snappy"),
            "ETL_ENABLE_ROW_HASH": os.environ.get("ETL_ENABLE_ROW_HASH", "false"),   

        },
    )

    # 2. Cloud Upload & Load
    run_upload = BashOperator(
        task_id="run_cloud_upload",
        bash_command=(
            "python "
            f"{os.environ.get('AIRFLOW_HOME', '/usr/local/airflow')}/include/cloud_loader.py"
        ),
        env={
            # Still pure ADC: we only point GOOGLE_APPLICATION_CREDENTIALS at a file
            "GOOGLE_APPLICATION_CREDENTIALS": (
                f"{os.environ.get('AIRFLOW_HOME', '/usr/local/airflow')}/include/gcp_adc.json"
            ),
            "GCP_PROJECT_ID": os.environ.get("GCP_PROJECT_ID", "testing-444715"),
            "GCP_BUCKET_NAME": os.environ.get(
                "GCP_BUCKET_NAME", "meter-data-lake-testing-444715"
            ),
            "GCP_DATASET_ID": os.environ.get("GCP_DATASET_ID", "raw_meter_readings"),
            "GCP_TABLE_ID": os.environ.get("GCP_TABLE_ID", "smart_meters_clean"),
            "GCS_UPLOAD_CHUNK_SIZE_MB": os.environ.get("GCS_UPLOAD_CHUNK_SIZE_MB", "10"),
            "GCS_UPLOAD_TIMEOUT": os.environ.get("GCS_UPLOAD_TIMEOUT", "300"),
            "AIRFLOW_HOME": os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"),
        },
    )

    # 3. dbt Transformation (Cosmos)
    transform_data = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=ProfileConfig(
            profile_name="default",
            target_name="dev",
            profiles_yml_filepath=(
                f"{os.environ.get('AIRFLOW_HOME', '/usr/local/airflow')}/include/profiles.yml"
            ),
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path=(
                f"{os.environ.get('AIRFLOW_HOME', '/usr/local/airflow')}"
                "/dbt_venv/bin/dbt"
            )
        ),
        operator_args={
            "install_deps": True,
            "env": {
                # Still using ADC for dbt via env
                "GOOGLE_APPLICATION_CREDENTIALS": (
                    f"{os.environ.get('AIRFLOW_HOME', '/usr/local/airflow')}/include/gcp_adc.json"
                ),
                "GOOGLE_CLOUD_PROJECT": os.environ.get(
                    "GCP_PROJECT_ID", "testing-444715"
                ),
            },
        },
    )

    # Dependency Chain
    run_etl >> run_upload >> transform_data
