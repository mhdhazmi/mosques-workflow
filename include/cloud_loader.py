import os
import glob
import json
import logging
import sys
import time
import uuid
from datetime import datetime
from typing import Optional

from google.cloud import storage
from google.cloud import bigquery

from pipeline_stats import (
    StageStats,
    insert_stats_batch,
    ensure_stats_table_exists,
    get_run_id,
)

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


def create_partitioned_table(bq_client, target_table_id, schema):
    """Creates a partitioned and clustered table for optimal query performance."""
    table = bigquery.Table(target_table_id, schema=schema)
    
    # Partition by DATA_TIME (day-level partitioning)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="DATA_TIME",
    )
    
    # Cluster by ROW_HASH for fast MERGE lookups
    table.clustering_fields = ["ROW_HASH"]
    
    table = bq_client.create_table(table)
    logger.info(f"Created partitioned table {target_table_id} (partitioned by DATA_TIME, clustered by ROW_HASH)")
    return table


def load_gcs_to_bigquery(
    uri: str,
    dataset_id: str,
    table_id: str,
    source_filename: str,
    run_id: str,
    run_timestamp: datetime,
) -> tuple[bool, Optional[StageStats]]:
    """
    Loads data from GCS into BigQuery using INSERT with dedup for better performance.
    
    Returns:
        Tuple of (success: bool, stats: StageStats or None)
    """
    logger.info(f"Loading {uri} into BigQuery with deduplication...")
    t0 = time.time()
    
    rows_input = 0
    rows_inserted = 0
    rows_duplicates_skipped = 0

    try:
        # ADC: uses default credentials
        bq_client = bigquery.Client(project=PROJECT_ID)

        target_table_id = f"{PROJECT_ID}.{dataset_id}.{table_id}"
        temp_table_id = f"{PROJECT_ID}.{dataset_id}.temp_{uuid.uuid4().hex[:8]}"

        # 1. Load to Temp Table
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        load_job = bq_client.load_table_from_uri(uri, temp_table_id, job_config=job_config)
        load_job.result()
        
        # Verify rows were actually loaded
        loaded_rows = load_job.output_rows or 0
        rows_input = loaded_rows
        if loaded_rows == 0:
            raise RuntimeError(f"BigQuery load completed but 0 rows loaded from {uri}")
        logger.info(f"Loaded {loaded_rows} rows to temp table {temp_table_id} from {uri}")

        # 2. Discover schema for dynamic SQL
        temp_table = bq_client.get_table(temp_table_id)
        temp_columns = [field.name for field in temp_table.schema]

        if not temp_columns:
            raise RuntimeError("Temp table has no columns; aborting load.")

        columns_str = ", ".join(f"`{c}`" for c in temp_columns)
        values_str = ", ".join(f"S.`{c}`" for c in temp_columns)

        # 3. Check if target table exists
        target_exists = False
        try:
            bq_client.get_table(target_table_id)
            target_exists = True
        except Exception:
            pass

        if not target_exists:
            # Create partitioned/clustered table from scratch
            logger.info(f"Target table {target_table_id} does not exist. Creating with partitioning...")
            create_partitioned_table(bq_client, target_table_id, temp_table.schema)
            
            # Insert all data from temp table
            insert_query = f"""
            INSERT `{target_table_id}` ({columns_str})
            SELECT {values_str}
            FROM `{temp_table_id}` AS S
            """
            query_job = bq_client.query(insert_query)
            query_job.result()
            rows_inserted = loaded_rows
            logger.info(f"Inserted {loaded_rows} rows into new table {target_table_id}.")
        else:
            # Target exists - use INSERT with NOT EXISTS for deduplication (faster than MERGE)
            if "ROW_HASH" in temp_columns:
                # Use INSERT...SELECT with NOT EXISTS - much faster than MERGE for large tables
                insert_dedup_query = f"""
                INSERT `{target_table_id}` ({columns_str})
                SELECT {values_str}
                FROM `{temp_table_id}` AS S
                WHERE NOT EXISTS (
                    SELECT 1 FROM `{target_table_id}` T 
                    WHERE T.ROW_HASH = S.ROW_HASH
                )
                """
                logger.info("Running INSERT with deduplication on ROW_HASH...")
                query_job = bq_client.query(insert_dedup_query)
                query_job.result()
                
                # Get rows inserted
                rows_inserted = query_job.num_dml_affected_rows or 0
                rows_duplicates_skipped = loaded_rows - rows_inserted
                logger.info(f"Inserted {rows_inserted} new rows into {target_table_id} (skipped {rows_duplicates_skipped} duplicates).")
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
                rows_inserted = loaded_rows
                logger.info(
                    f"Appended data from {temp_table_id} into {target_table_id} without dedup."
                )

        # 4. Cleanup Temp Table
        bq_client.delete_table(temp_table_id, not_found_ok=True)
        logger.info(f"Cleaned up temp table {temp_table_id}.")

        processing_seconds = time.time() - t0
        
        # Create stats record
        stats = StageStats(
            run_id=run_id,
            run_timestamp=run_timestamp,
            source_filename=source_filename,
            stage_name="upload",
            rows_input=rows_input,
            rows_output=rows_inserted,
            rows_filtered=rows_duplicates_skipped,
            rows_duplicates_skipped=rows_duplicates_skipped,
            filter_reason="duplicate_row_hash" if rows_duplicates_skipped > 0 else None,
            processing_seconds=processing_seconds,
            status="success",
        )
        
        return True, stats
        
    except Exception as e:
        logger.error(f"BigQuery Load failed: {e}")
        processing_seconds = time.time() - t0
        
        stats = StageStats(
            run_id=run_id,
            run_timestamp=run_timestamp,
            source_filename=source_filename,
            stage_name="upload",
            rows_input=rows_input,
            processing_seconds=processing_seconds,
            status="failed",
            error_message=str(e)[:500],
        )
        
        return False, stats


def load_etl_stats() -> list[dict]:
    """Load ETL stats from JSON file created by etl_processor."""
    stats_path = os.path.join(LOCAL_OUTPUT_DIR, "_etl_stats.json")
    if os.path.exists(stats_path):
        try:
            with open(stats_path, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load ETL stats: {e}")
    return []


def main() -> int:
    """
    Main cloud upload orchestration function.
    Uploads parquet files to GCS, loads to BigQuery, and records stats.
    """
    run_id = get_run_id()
    run_timestamp = datetime.now()
    logger.info(f"Starting cloud upload with run_id: {run_id}")
    
    # Check if local directory exists
    if not os.path.exists(LOCAL_OUTPUT_DIR):
        logger.error(
            f"Local directory {LOCAL_OUTPUT_DIR} not found. Run etl_processor.py first."
        )
        return 1

    parquet_files = glob.glob(os.path.join(LOCAL_OUTPUT_DIR, "**/*.parquet"), recursive=True)

    if not parquet_files:
        logger.warning(f"No Parquet files found in {LOCAL_OUTPUT_DIR}")
        return 0

    logger.info(f"Found {len(parquet_files)} files to upload.")
    
    # Load ETL stats from file
    etl_stats_data = load_etl_stats()
    
    # Collect all stats (ETL + upload)
    all_stats: list[StageStats] = []
    
    # Convert ETL stats dicts back to StageStats objects
    for stat_dict in etl_stats_data:
        try:
            # Parse timestamp back to datetime
            if isinstance(stat_dict.get("run_timestamp"), str):
                stat_dict["run_timestamp"] = datetime.fromisoformat(stat_dict["run_timestamp"])
            all_stats.append(StageStats(**stat_dict))
        except Exception as e:
            logger.warning(f"Failed to parse ETL stat: {e}")

    for local_file in parquet_files:
        relative_path = os.path.relpath(local_file, LOCAL_OUTPUT_DIR)
        gcs_path = f"{GCS_PREFIX}{relative_path}"
        
        # Get source filename from parquet path (remove quarter folder prefix)
        source_filename = os.path.basename(local_file).replace(".parquet", ".csv")

        gcs_uri = upload_to_gcs(BUCKET_NAME, local_file, gcs_path)

        if gcs_uri:
            success, stats = load_gcs_to_bigquery(
                gcs_uri, DATASET_ID, TABLE_ID,
                source_filename=source_filename,
                run_id=run_id,
                run_timestamp=run_timestamp,
            )
            if stats:
                all_stats.append(stats)
            
            if not success:
                logger.error(f"Failed to load {gcs_uri} to BigQuery")
                # Continue with other files, but record the failure
    
    # Write all stats to BigQuery
    if all_stats:
        logger.info(f"Writing {len(all_stats)} stats records to BigQuery...")
        try:
            bq_client = bigquery.Client(project=PROJECT_ID)
            ensure_stats_table_exists(bq_client)
            insert_stats_batch(all_stats, bq_client)
        except Exception as e:
            logger.error(f"Failed to write stats to BigQuery: {e}")
    
    logger.info("Cloud upload completed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
