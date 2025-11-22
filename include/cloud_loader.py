import os
import glob
import logging
import sys
import time
from google.cloud import storage
from google.cloud import bigquery

# --- CONFIGURATION ---
# Load from Environment Variables with Defaults
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
    datefmt='%Y-%m-%dT%H:%M:%S%z'
)
handler.setFormatter(formatter)
logger.addHandler(handler)

logger.info(f"Cloud Config: Project={PROJECT_ID}, Bucket={BUCKET_NAME}, ChunkSize={UPLOAD_CHUNK_SIZE_MB}MB")

def upload_to_gcs(bucket_name, source_file, destination_blob_name):
    """Uploads a file to the bucket with high-speed chunking, skipping if exists."""
    try:
        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        if blob.exists():
            logger.info(f"Skipping {source_file}: Already exists at gs://{bucket_name}/{destination_blob_name}")
            return f"gs://{bucket_name}/{destination_blob_name}"

        logger.info(f"Uploading {source_file} to gs://{bucket_name}/{destination_blob_name}...")

        # Optimization: Set chunk size based on env var
        blob.chunk_size = UPLOAD_CHUNK_SIZE_MB * 1024 * 1024 
        
        # Set timeout based on env var
        blob.upload_from_filename(source_file, timeout=UPLOAD_TIMEOUT)
        logger.info("Upload complete.")
        return f"gs://{bucket_name}/{destination_blob_name}"
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        return None

def load_gcs_to_bigquery(uri, dataset_id, table_id):
    """Loads data using a MERGE strategy to prevent duplicates."""
    logger.info(f"Loading {uri} into BigQuery with deduplication...")
    
    try:
        bq_client = bigquery.Client(project=PROJECT_ID)
        
        target_table_id = f"{PROJECT_ID}.{dataset_id}.{table_id}"
        temp_table_id = f"{PROJECT_ID}.{dataset_id}.temp_{int(time.time())}"
        
        # 1. Load to Temp Table
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        )

        load_job = bq_client.load_table_from_uri(
            uri, temp_table_id, job_config=job_config
        )
        load_job.result() # Wait for load

        # 2. MERGE into Target Table
        merge_query = f"""
        MERGE  T
        USING  S
        ON T.ROW_HASH = S.ROW_HASH
        WHEN NOT MATCHED THEN
          INSERT ROW
        """
        
        # Create target table if it doesn't exist (first run)
        try:
            bq_client.get_table(target_table_id)
            # Run Merge
            query_job = bq_client.query(merge_query)
            query_job.result()
            logger.info(f"Merged data from {uri} into {target_table_id}.")
        except Exception:
            # Table doesn't exist, just copy temp to target
            logger.info(f"Target table {target_table_id} does not exist. Creating from temp...")
            bq_client.copy_table(temp_table_id, target_table_id).result()

        # 3. Cleanup Temp Table
        bq_client.delete_table(temp_table_id, not_found_ok=True)
        
        return True
    except Exception as e:
        logger.error(f"BigQuery Load failed: {e}")
        return False

if __name__ == "__main__":
    # Check if local directory exists
    if not os.path.exists(LOCAL_OUTPUT_DIR):
        logger.error(f"Local directory {LOCAL_OUTPUT_DIR} not found. Run etl_processor.py first.")
        exit(1)

    # Find all Parquet files recursively (to catch subfolders like 2023-Q4)
    parquet_files = glob.glob(os.path.join(LOCAL_OUTPUT_DIR, "**/*.parquet"), recursive=True)
    
    if not parquet_files:
        logger.warning(f"No Parquet files found in {LOCAL_OUTPUT_DIR}")
        exit(0)

    logger.info(f"Found {len(parquet_files)} files to upload.")

    for local_file in parquet_files:
        # Calculate relative path to preserve folder structure (e.g., 2023-Q4/file.parquet)
        relative_path = os.path.relpath(local_file, LOCAL_OUTPUT_DIR)
        gcs_path = f"{GCS_PREFIX}{relative_path}"
        
        # 1. Upload
        gcs_uri = upload_to_gcs(BUCKET_NAME, local_file, gcs_path)
        
        if gcs_uri:
            # 2. Load to Warehouse
            load_gcs_to_bigquery(gcs_uri, DATASET_ID, TABLE_ID)
