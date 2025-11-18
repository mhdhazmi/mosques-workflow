"""
Dummy CSV → Parquet → Clean → GCS/BigQuery pipeline.

This DAG intentionally contains placeholder tasks (raise NotImplementedError)
to outline the end-to-end flow. Fill each task with real logic later.

Flow:
- List CSV files in `include/raw_data/`
- Upload originals to GCS (one per file)
- Plan chunking for all files (returns a flat list of chunk descriptors)
- Convert each chunk to Parquet
- Clean/process each Parquet
- Load cleaned outputs to BigQuery
"""

from __future__ import annotations

from typing import Any, Dict, List
import glob
import math
import os

from pendulum import datetime

# Using the same import pattern as the example DAG for consistency
from airflow.sdk import dag, task


@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    default_args={"owner": "DataEng", "retries": 3},
    tags=["dummy", "csv-ingest", "parquet", "bigquery"],
    doc_md=__doc__,
)
def csv_chunk_pipeline():
    """
    Dummy pipeline that maps over raw CSV files and processes them in chunks.
    No external libraries required; tasks are placeholders to be implemented.
    """

    @task
    def list_raw_csv_files(base_dir: str = "include/raw_data") -> List[str]:
        """
        Return a list of CSV paths to process.
        Replace with actual discovery logic (e.g., glob). For now this is a
        placeholder to be implemented.
        """
        # Discover CSVs recursively under the base directory.
        pattern = os.path.join(base_dir, "**", "*.csv")
        files = sorted(glob.glob(pattern, recursive=True))
        return files

    @task
    def upload_original_csv_to_gcs(csv_path: str, bucket: str = "your-gcs-bucket") -> str:
        """
        Upload the original CSV to GCS and return its GCS URI.
        """
        raise NotImplementedError(
            "Upload the CSV at csv_path to GCS and return gs://... URI"
        )

    @task
    def plan_all_chunks(
        csv_paths: List[str], *, chunk_size_rows: int = 100_000
    ) -> List[Dict[str, Any]]:
        """
        Build a flat list of chunk descriptors across all files.
        Each descriptor minimally contains {"csv_path": str, "chunk_index": int}.
        Add any metadata you need later (e.g., start/end rows).
        """
        plan: List[Dict[str, Any]] = []
        for path in csv_paths:
            if not os.path.isfile(path):
                continue
            # Count data rows (exclude header if present). This is minimal and
            # can be optimized/replaced later with faster row counting.
            try:
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    # Try to skip header row; if no header, this only reduces count by 1
                    next(f, None)
                    data_rows = sum(1 for _ in f)
            except Exception:
                # In case of read issues, fall back to a single chunk descriptor
                data_rows = chunk_size_rows

            if data_rows <= 0:
                continue

            n_chunks = max(1, math.ceil(data_rows / chunk_size_rows))
            for i in range(n_chunks):
                plan.append(
                    {
                        "csv_path": path,
                        "chunk_index": i,
                        "chunk_size_rows": chunk_size_rows,
                    }
                )
        return plan

    @task
    def chunk_to_parquet(chunk: Dict[str, Any]) -> str:
        """
        Convert a single chunk of a CSV into a Parquet file.
        Return the local path to the generated Parquet.
        """
        raise NotImplementedError(
            "Read chunk from CSV and write a Parquet file; return its path"
        )

    @task
    def clean_parquet(parquet_path: str) -> str:
        """
        Apply cleaning/transformations to the Parquet file and return a path to
        the cleaned Parquet.
        """
        raise NotImplementedError(
            "Clean/transform Parquet and return path to cleaned Parquet"
        )

    @task
    def load_cleaned_to_bigquery(
        parquet_path: str,
        *,
        project: str = "your-gcp-project",
        dataset: str = "your_dataset",
        table: str = "your_table",
        write_disposition: str = "WRITE_APPEND",
    ) -> str:
        """
        Load a cleaned Parquet into BigQuery.
        Return a reference to the destination table (e.g., project.dataset.table).
        """
        raise NotImplementedError(
            "Load the cleaned Parquet to BigQuery and return the table id"
        )

    # Graph wiring
    csv_files = list_raw_csv_files()

    # Upload originals in parallel, independent of downstream processing
    _ = upload_original_csv_to_gcs.expand(csv_path=csv_files)

    # Plan a single flat list of chunks across all files
    chunk_plan = plan_all_chunks(csv_files)

    # Per-chunk processing
    parquet_paths = chunk_to_parquet.expand(chunk=chunk_plan)
    cleaned_paths = clean_parquet.expand(parquet_path=parquet_paths)

    # Load each cleaned parquet to BigQuery
    _ = load_cleaned_to_bigquery.expand(parquet_path=cleaned_paths)


# Instantiate the DAG
csv_chunk_pipeline()
