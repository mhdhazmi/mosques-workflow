"""
Pipeline Stats Module

Handles collection and storage of pipeline statistics to BigQuery.
Tracks row counts, filtering results, and processing times at each stage.
"""

import os
import logging
import sys
from datetime import datetime
from typing import Optional
from dataclasses import dataclass, field, asdict

from google.cloud import bigquery

# --- CONFIGURATION ---
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "testing-444715")
DATASET_ID = os.getenv("GCP_DATASET_ID", "raw_meter_readings")
STATS_TABLE_ID = "pipeline_stats"

# --- LOGGING SETUP ---
logger = logging.getLogger("pipeline_stats")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}',
        datefmt='%Y-%m-%dT%H:%M:%S%z',
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)


# Schema for pipeline_stats table
STATS_TABLE_SCHEMA = [
    bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("run_timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("source_filename", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("quarter", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("stage_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("rows_input", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("rows_output", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("rows_filtered", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("filter_reason", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("unique_meters", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("min_reading_date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("max_reading_date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("processing_seconds", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("file_size_bytes", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("rows_with_null_power", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("rows_with_zero_power", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("rows_duplicates_skipped", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
]


@dataclass
class StageStats:
    """Data class for collecting stats at each pipeline stage."""
    run_id: str
    run_timestamp: datetime
    source_filename: str
    stage_name: str
    status: str = "success"
    quarter: Optional[str] = None
    rows_input: Optional[int] = None
    rows_output: Optional[int] = None
    rows_filtered: Optional[int] = None
    filter_reason: Optional[str] = None
    unique_meters: Optional[int] = None
    min_reading_date: Optional[str] = None  # ISO format date string
    max_reading_date: Optional[str] = None  # ISO format date string
    processing_seconds: Optional[float] = None
    file_size_bytes: Optional[int] = None
    rows_with_null_power: Optional[int] = None
    rows_with_zero_power: Optional[int] = None
    rows_duplicates_skipped: Optional[int] = None
    error_message: Optional[str] = None

    def to_bq_row(self) -> dict:
        """Convert to BigQuery row format."""
        row = asdict(self)
        # Convert datetime to ISO string for BigQuery
        if isinstance(row["run_timestamp"], datetime):
            row["run_timestamp"] = row["run_timestamp"].isoformat()
        return row


def ensure_stats_table_exists(bq_client: Optional[bigquery.Client] = None) -> bool:
    """
    Creates the pipeline_stats table if it doesn't exist.
    Returns True if table exists or was created successfully.
    """
    try:
        if bq_client is None:
            bq_client = bigquery.Client(project=PROJECT_ID)

        table_id = f"{PROJECT_ID}.{DATASET_ID}.{STATS_TABLE_ID}"

        try:
            bq_client.get_table(table_id)
            logger.info(f"Stats table {table_id} already exists.")
            return True
        except Exception:
            pass  # Table doesn't exist, create it

        table = bigquery.Table(table_id, schema=STATS_TABLE_SCHEMA)
        
        # Partition by run_timestamp for efficient queries
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="run_timestamp",
        )
        
        # Cluster by stage_name and source_filename for fast filtering
        table.clustering_fields = ["stage_name", "source_filename"]

        bq_client.create_table(table)
        logger.info(f"Created stats table {table_id}")
        return True

    except Exception as e:
        logger.error(f"Failed to ensure stats table exists: {e}")
        return False


def insert_stats(stats: StageStats, bq_client: Optional[bigquery.Client] = None) -> bool:
    """
    Inserts a single stats record into BigQuery.
    Returns True if successful.
    """
    try:
        if bq_client is None:
            bq_client = bigquery.Client(project=PROJECT_ID)

        table_id = f"{PROJECT_ID}.{DATASET_ID}.{STATS_TABLE_ID}"

        # Ensure table exists
        ensure_stats_table_exists(bq_client)

        # Insert the row
        rows_to_insert = [stats.to_bq_row()]
        errors = bq_client.insert_rows_json(table_id, rows_to_insert)

        if errors:
            logger.error(f"Failed to insert stats: {errors}")
            return False

        logger.info(f"Inserted stats for {stats.source_filename} stage={stats.stage_name}")
        return True

    except Exception as e:
        logger.error(f"Failed to insert stats: {e}")
        return False


def insert_stats_batch(stats_list: list[StageStats], bq_client: Optional[bigquery.Client] = None) -> bool:
    """
    Inserts multiple stats records into BigQuery in a single batch.
    Returns True if successful.
    """
    if not stats_list:
        return True

    try:
        if bq_client is None:
            bq_client = bigquery.Client(project=PROJECT_ID)

        table_id = f"{PROJECT_ID}.{DATASET_ID}.{STATS_TABLE_ID}"

        # Ensure table exists
        ensure_stats_table_exists(bq_client)

        # Insert the rows
        rows_to_insert = [s.to_bq_row() for s in stats_list]
        errors = bq_client.insert_rows_json(table_id, rows_to_insert)

        if errors:
            logger.error(f"Failed to insert stats batch: {errors}")
            return False

        logger.info(f"Inserted {len(stats_list)} stats records")
        return True

    except Exception as e:
        logger.error(f"Failed to insert stats batch: {e}")
        return False


def get_run_id() -> str:
    """
    Gets the current DAG run ID from environment, or generates a unique one.
    """
    return os.getenv("AIRFLOW_RUN_ID", f"local_{datetime.now().strftime('%Y%m%d_%H%M%S')}")

