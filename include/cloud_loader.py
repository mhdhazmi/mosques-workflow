import os
import glob
import logging
import sys
import time
import uuid

from google.cloud import storage
from google.cloud import bigquery

# --- CONFIGURATION ---
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "testing-444715")
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME", "meter-data-lake-testing-444715")
DATASET_ID = os.getenv("GCP_DATASET_ID", "raw_meter_readings")
TABLE_ID = os.getenv("GCP_TABLE_ID", "smart_meters_clean")

# Performance Tuning
UPLOAD_CHUNK_SIZE_MB = int(os.getenv("GCS_UPLOAD_CHUNK_SIZE_MB", "10"))
UPLOAD_TIMEOUT = int(os.getenv("GCS_UPLOAD_TIMEOUT", "300"))

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/usr/local/airflow")
LOCAL_OUTPUT_DIR = os.path.join(AIRFLOW_HOME, "include/processed_data")
GCS_PREFIX = "uploads/"

# --- LOGGING SETUP ---
logger = logging.getLogger("cloud_loader")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}',
    datefmt='%Y-%m-%dT%H:%M:%S%z',
)
handler.setFormatter(formatter)
logger.addHandler(handler)

logger.info(
    f"Cloud Config: Project={PROJECT_ID}, Bucket={BUCKET_NAME}, ChunkSize={UPLOAD_CHUNK_SIZE_MB}MB"
)


def upload_to_gcs(bucket_name, source_file, destination_blob_name):
    """Uploads a file to the bucket with high-speed chunking, skipping if exists."""
    try:
        # ADC: uses default credentials from environment (GOOGLE_APPLICATION_CREDENTIALS)
        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        if blob.exists():
            logger.info(
                f"Skipping {source_file}: Already exists at gs://{bucket_name}/{destination_blob_name}"
            )
            return f"gs://{bucket_name}/{destination_blob_name}"

        logger.info(
            f"Uploading {source_file} to gs://{bucket_name}/{destination_blob_name}..."
        )

        blob.chunk_size = UPLOAD_CHUNK_SIZE_MB * 1024 * 1024
        blob.upload_from_filename(source_file, timeout=UPLOAD_TIMEOUT)
        logger.info("Upload complete.")
        return f"gs://{bucket_name}/{destination_blob_name}"
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        return None


def load_gcs_to_bigquery(uri, dataset_id, table_id):
    """Loads data from GCS into BigQuery using MERGE on ROW_HASH when possible."""
    logger.info(f"Loading {uri} into BigQuery with deduplication...")

    try:
        # ADC: uses default credentials
        bq_client = bigquery.Client(project=PROJECT_ID)

        target_table_id = f"{PROJECT_ID}.{dataset_id}.{table_id}"
        temp_table_id = f"{PROJECT_ID}.{dataset_id}.temp_{uuid.uuid4().hex[:8]}"

        # 1. Load to Temp Table
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            # schema_update_options removed as it conflicts with WRITE_TRUNCATE
        )

        load_job = bq_client.load_table_from_uri(uri, temp_table_id, job_config=job_config)
        load_job.result()
        logger.info(f"Loaded temp table {temp_table_id} from {uri}")

        # 2. Discover schema for dynamic SQL
        temp_table = bq_client.get_table(temp_table_id)
        temp_columns = [field.name for field in temp_table.schema]

        if not temp_columns:
            raise RuntimeError("Temp table has no columns; aborting load.")

        columns_str = ", ".join(f"`{c}`" for c in temp_columns)
        values_str = ", ".join(f"S.`{c}`" for c in temp_columns)

        # 3. MERGE or INSERT into Target
        try:
            # Check if target exists
            bq_client.get_table(target_table_id)

            if "ROW_HASH" in temp_columns:
                merge_query = f"""
                MERGE `{target_table_id}` T
                USING `{temp_table_id}` S
                ON T.ROW_HASH = S.ROW_HASH
                WHEN NOT MATCHED THEN
                  INSERT ({columns_str})
                  VALUES ({values_str})
                """
                logger.info("Running MERGE to deduplicate on ROW_HASH...")
                query_job = bq_client.query(merge_query)
                query_job.result()
                logger.info(f"Merged data from {temp_table_id} into {target_table_id}.")
            else:
                logger.warning(
                    "ROW_HASH column not found in temp table; falling back to append-only INSERT."
                )
                insert_query = f"""
                INSERT `{target_table_id}` ({columns_str})
                SELECT {values_str}
                FROM `{temp_table_id}` AS S
                """
                query_job = bq_client.query(insert_query)
                query_job.result()
                logger.info(
                    f"Appended data from {temp_table_id} into {target_table_id} without dedup."
                )
        except Exception as e:
            # Target table doesn't exist yet; create from temp
            logger.info(
                f"Target table {target_table_id} does not exist. Creating from temp table. ({e})"
            )
            bq_client.copy_table(temp_table_id, target_table_id).result()
            logger.info(f"Created {target_table_id} from {temp_table_id}.")

        # 4. Cleanup Temp Table
        bq_client.delete_table(temp_table_id, not_found_ok=True)
        logger.info(f"Cleaned up temp table {temp_table_id}.")

        return True
    except Exception as e:
        logger.error(f"BigQuery Load failed: {e}")
        return False


if __name__ == "__main__":
    # Check if local directory exists
    if not os.path.exists(LOCAL_OUTPUT_DIR):
        logger.error(
            f"Local directory {LOCAL_OUTPUT_DIR} not found. Run etl_processor.py first."
        )
        exit(1)

    parquet_files = glob.glob(os.path.join(LOCAL_OUTPUT_DIR, "**/*.parquet"), recursive=True)

    if not parquet_files:
        logger.warning(f"No Parquet files found in {LOCAL_OUTPUT_DIR}")
        exit(0)

    logger.info(f"Found {len(parquet_files)} files to upload.")

    for local_file in parquet_files:
        relative_path = os.path.relpath(local_file, LOCAL_OUTPUT_DIR)
        gcs_path = f"{GCS_PREFIX}{relative_path}"

        gcs_uri = upload_to_gcs(BUCKET_NAME, local_file, gcs_path)

        if gcs_uri:
            load_gcs_to_bigquery(gcs_uri, DATASET_ID, TABLE_ID)
