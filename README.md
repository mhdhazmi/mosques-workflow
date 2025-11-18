Project: mosques — Airflow/Astronomer example with CSV → Parquet → BigQuery dummy pipeline.

Overview
- Contains a sample Astronomer/Airflow project and a dummy ingestion DAG.
- The DAG `dags/csv_chunk_pipeline.py` outlines an end-to-end flow but tasks are placeholders you can fill in.

Dummy Pipeline (csv_chunk_pipeline)
- list_raw_csv_files: return list of CSVs under `include/raw_data/`.
- upload_original_csv_to_gcs: upload each original CSV to a GCS bucket.
- plan_all_chunks: produce a flat list of chunk descriptors (csv_path + chunk_index) for all CSVs.
- chunk_to_parquet: convert each chunk to a Parquet file.
- clean_parquet: apply cleaning/transformations to each Parquet file.
- load_cleaned_to_bigquery: load each cleaned Parquet to BigQuery (e.g., WRITE_APPEND).

Notes
- All tasks are intentionally `NotImplementedError` stubs; implement real logic as needed.
- Example DAG `dags/exampledag.py` remains for reference on TaskFlow and dynamic mapping.
- Tests expect every DAG to have at least one tag and retries >= 2 (already satisfied).

Getting Started (Astronomer)
- Use Astronomer CLI for local dev: `astro dev start`, `astro dev parse`.
- Run tests (optional): `pytest`.
