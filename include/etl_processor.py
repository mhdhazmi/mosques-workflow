import os
import sys
import glob
import json
import logging
import time
from datetime import datetime

import polars as pl
import google.generativeai as genai


# =========================
# CONFIGURATION
# =========================

# API key for Gemini (LLM)
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "YOUR_API_KEY_HERE")
genai.configure(api_key=GOOGLE_API_KEY)

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/usr/local/airflow")
INPUT_DIR = os.path.join(AIRFLOW_HOME, "include/raw_data")
OUTPUT_DIR = os.path.join(AIRFLOW_HOME, "include/processed_data")

# Tunables from environment
ROW_GROUP_SIZE = int(os.getenv("ETL_ROW_GROUP_SIZE", "50000"))
LOW_MEMORY_MODE = os.getenv("ETL_LOW_MEMORY_MODE", "true").lower() == "true"
PARQUET_COMPRESSION = os.getenv("PARQUET_COMPRESSION", "snappy")
ENABLE_ROW_HASH = os.getenv("ETL_ENABLE_ROW_HASH", "true").lower() == "true"
INFER_SCHEMA_LENGTH = int(os.getenv("ETL_INFER_SCHEMA_LENGTH", "1000"))

# The schema we EXPECT to see based on actual data inspection
EXPECTED_HEADERS = [
    "METER_ID",
    "ID",
    "METER_NO",
    "DATA_TIME",
    "IMPORT_ACTIVE_POWER",
    "IMPORT_REACTIVE_POWER",
    "IMPORT_APPARENT_POWER",
    "REACTIVE_POWER_IMPORT",
    "EXPORT_ACTIVE_POWER",
    "EXPORT_REACTIVE_POWER",
    "EXPORT_APPARENT_POWER",
    "REACTIVE_POWER_EXPORT",
    "IMPORT_FACTOR_POWER",
    "EXPORT_FACTOR_POWER",
    "STATUS",
]


# =========================
# LOGGING SETUP
# =========================

logger = logging.getLogger("etl_processor")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}',
    datefmt='%Y-%m-%dT%H:%M:%S%z',
)
handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(handler)


# =========================
# HELPER FUNCTIONS
# =========================

def get_csv_headers(filepath: str, separator: str = ",") -> list[str]:
    """Reads only the first line of the CSV efficiently to get headers."""
    logger.info(f"Reading headers from {filepath}")
    try:
        with open(filepath, "r", encoding="utf-8-sig") as f:  # Handle BOM
            header_line = f.readline().strip()
        headers = [h.replace('"', "") for h in header_line.split(separator) if h]
        logger.info("Completed Reading Headers")
        return headers
    except Exception as e:
        logger.error(f"Failed to read headers from {filepath}: {e}")
        return []


def validate_schema_with_gemini(current_headers: list[str], expected_headers: list[str]) -> dict:
    """
    Asks Gemini to map current headers to expected headers.
    Returns a dict with keys: status, mapping, reason.
    """
    logger.info("Asking Gemini to validate headers with LLM...")

    prompt = f"""
    I am a data engineer pipeline.

    My Expected Columns: {json.dumps(expected_headers)}
    Incoming CSV Columns: {json.dumps(current_headers)}

    Task:
    1. Compare the Incoming columns to the Expected columns.
    2. If they match exactly, status is "MATCH".
    3. If they are different (typos, reordering, case sensitivity), provide a mapping dictionary where key=IncomingName and value=ExpectedName. Status is "RENAME".
    4. If the file is totally wrong/unknown, status is "ABORT".

    Return ONLY valid JSON in this format:
    {{
        "status": "MATCH" | "RENAME" | "ABORT",
        "mapping": {{ "old_col_name": "new_col_name" }},
        "reason": "Brief explanation"
    }}
    """

    models_to_try = ["gemini-2.5-flash"]

    for model_name in models_to_try:
        try:
            model = genai.GenerativeModel(model_name)
            response = model.generate_content(prompt)
            cleaned_text = (
                response.text.replace("```json", "").replace("```", "").strip()
            )
            result = json.loads(cleaned_text)
            logger.info("Completed LLM Validation")
            return result
        except Exception as e:
            if "404" in str(e) and "models/" in str(e):
                logger.warning(
                    f"Model {model_name} not found or not supported. Trying next..."
                )
                continue
            else:
                logger.error(f"LLM Validation failed with {model_name}: {e}")
                continue

    logger.error("All LLM models failed or are unavailable.")
    return {
        "status": "ABORT",
        "mapping": {},
        "reason": "All LLM models failed or unavailable",
    }


def determine_schema_mapping(current_headers: list[str], expected_headers: list[str]) -> dict:
    """
    First try deterministic matching, then fall back to LLM.
    Ensures that after mapping we have all expected headers and no duplicates.
    Returns {"mapping": dict, "reason": str}.
    """
    logger.info("Determining schema mapping...")

    normalized_current = [h.strip().upper() for h in current_headers]
    normalized_expected = [h.strip().upper() for h in expected_headers]

    # Fast path: exact match ignoring case
    if normalized_current == normalized_expected:
        reason = "Exact header match ignoring case"
        logger.info("Headers match expected schema (case-insensitive); skipping LLM validation.")
        logger.info("Completed Determining Schema Mapping")
        return {"mapping": {}, "reason": reason}

    # Otherwise ask the LLM
    validation = validate_schema_with_gemini(current_headers, expected_headers)
    status = validation.get("status", "ABORT")
    reason = validation.get("reason", "No reason provided")

    if status == "ABORT":
        logger.error(f"Schema validation ABORT: {reason}")
        raise ValueError(f"Schema validation ABORT: {reason}")

    mapping = validation.get("mapping", {}) if status == "RENAME" else {}

    # Apply mapping to see resulting header set
    final_headers = [mapping.get(h, h) for h in current_headers]

    # Check for duplicates
    if len(final_headers) != len(set(final_headers)):
        logger.error(f"Duplicate column names after mapping: {final_headers}")
        raise ValueError(f"Duplicate column names after mapping: {final_headers}")

    # Ensure we have all expected columns
    missing = set(expected_headers) - set(final_headers)
    if missing:
        msg = f"Missing expected columns after mapping: {sorted(missing)}"
        logger.error(msg)
        raise ValueError(msg)

    logger.info("Completed Determining Schema Mapping")
    return {"mapping": mapping, "reason": reason}


def get_file_quarter(input_path: str) -> str:
    """
    Reads a small sample of the file to determine the quarter for file organization.
    Returns string like '2023-Q4', or 'UNKNOWN_QUARTER' if not determinable.
    """
    logger.info("Determining file quarter...")
    try:
        df = pl.read_csv(
            input_path,
            separator=",",
            quote_char='"',
            n_rows=1,
            ignore_errors=True,
        )

        if "DATA_TIME" in df.columns:
            date_val = datetime.strptime(df["DATA_TIME"][0], "%Y-%m-%d %H:%M:%S")
            quarter = (date_val.month - 1) // 3 + 1
            logger.info("Completed Determining File Quarter")
            return f"{date_val.year}-Q{quarter}"
    except Exception as e:
        logger.warning(f"Could not determine quarter from file content: {e}")

    logger.info("Completed Determining File Quarter (fallback to UNKNOWN_QUARTER)")
    return "UNKNOWN_QUARTER"


def build_lazy_pipeline(
    input_path: str,
    col_mapping: dict,
    quarter_str: str | None,
) -> pl.LazyFrame:
    """
    Build the Polars lazy pipeline: scan CSV, apply mapping, parse dates,
    add derived columns, cast numeric types, optional row hash.
    """
    # 1. Lazy Scan (no data loaded yet)
    t_scan_start = time.perf_counter()
    q = pl.scan_csv(
        input_path,
        separator=",",
        quote_char='"',
        infer_schema_length=INFER_SCHEMA_LENGTH,
        ignore_errors=True,
        low_memory=LOW_MEMORY_MODE,
        rechunk=False,
    )
    t_scan_end = time.perf_counter()
    logger.info(f"scan_csv setup took {t_scan_end - t_scan_start:.2f} seconds")

    # 2. Apply LLM-driven column renames (if any)
    if col_mapping:
        q = q.rename(col_mapping)

    # 3. Parse DATA_TIME to datetime (full timestamp and date-only)
    q = q.with_columns(
        [
            pl.coalesce(
                pl.col("DATA_TIME").str.strptime(
                    pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False
                ),
                pl.col("DATA_TIME").str.strptime(
                    pl.Datetime, format="%Y-%m-%d", strict=False
                ),
            ).alias("DATA_TIME"),
        ]
    )

    # 4. Add DATA_TIME_STR and QUARTER column
    derived_cols = [
        pl.col("DATA_TIME")
        .dt.strftime("%Y-%m-%d %H:%M:%S")
        .alias("DATA_TIME_STR"),
    ]

    if quarter_str and quarter_str != "UNKNOWN_QUARTER":
        # Use constant quarter string if we already know it from get_file_quarter
        derived_cols.append(pl.lit(quarter_str).alias("QUARTER"))
    else:
        # Fallback: derive per-row
        derived_cols.append(
            (
                pl.col("DATA_TIME").dt.year().cast(pl.Utf8)
                + "-Q"
                + pl.col("DATA_TIME").dt.quarter().cast(pl.Utf8)
            ).alias("QUARTER")
        )

    q = q.with_columns(derived_cols)

    # 5. Ensure numeric types for power columns
    # Strip whitespace first, then cast to Float64
    q = q.with_columns(
        [
            pl.col("IMPORT_ACTIVE_POWER").cast(pl.Utf8).str.strip_chars().cast(pl.Float64, strict=False),
            pl.col("EXPORT_ACTIVE_POWER").cast(pl.Utf8).str.strip_chars().cast(pl.Float64, strict=False),
            pl.col("IMPORT_REACTIVE_POWER").cast(pl.Utf8).str.strip_chars().cast(pl.Float64, strict=False),
            pl.col("EXPORT_REACTIVE_POWER").cast(pl.Utf8).str.strip_chars().cast(pl.Float64, strict=False),
        ]
    )

    # 6. Optional deterministic ROW_HASH (can be heavy on very large files)
    logger.info(f"Row hash enabled: {ENABLE_ROW_HASH}")
    if ENABLE_ROW_HASH:
        q = q.with_columns(
            [
                (
                    pl.concat_str(
                        [
                            pl.col("METER_ID").cast(pl.Utf8),
                            pl.col("DATA_TIME_STR"),
                            pl.col("IMPORT_ACTIVE_POWER").cast(pl.Utf8),
                        ]
                    ).hash()
                    % (2**63 - 1)
                )
                .cast(pl.Int64)
                .alias("ROW_HASH")
            ]
        )

    return q


def process_file_streaming(input_path: str, output_dir: str, col_mapping: dict) -> bool:
    """
    Streams data from CSV to Parquet using a Polars lazy pipeline.
    The heavy work (read, transform, write) executes at sink_parquet.
    """
    logger.info(f"Starting stream processing for {input_path}...")
    t0 = time.perf_counter()

    try:
        # 1. Determine Output Path based on Quarter
        quarter_folder = get_file_quarter(input_path)
        quarter_str = quarter_folder if quarter_folder != "UNKNOWN_QUARTER" else None

        target_dir = os.path.join(output_dir, quarter_folder)
        os.makedirs(target_dir, exist_ok=True)

        filename = os.path.basename(input_path).replace(".csv", ".parquet")
        output_path = os.path.join(target_dir, filename)

        logger.info("2")  # simple progress marker
        q = build_lazy_pipeline(input_path, col_mapping, quarter_str)
        logger.info("3")  # after building lazy pipeline

        # 2. Sink to Parquet (this executes the whole lazy plan)
        logger.info("4")
        logger.info(
            f"Starting sink_parquet to {output_path} with compression={PARQUET_COMPRESSION}, "
            f"row_group_size={ROW_GROUP_SIZE}"
        )
        t_sink_start = time.perf_counter()
        q.sink_parquet(
            output_path,
            compression=PARQUET_COMPRESSION,
            row_group_size=ROW_GROUP_SIZE,
        )
        t_sink_end = time.perf_counter()
        logger.info(f"sink_parquet execution took {t_sink_end - t_sink_start:.2f} seconds")

        logger.info(f"Success! Processed data saved to {output_path}")
        logger.info("Completed Streaming")

        t1 = time.perf_counter()
        logger.info(f"Total process_file_streaming runtime: {t1 - t0:.2f} seconds")
        return True

    except Exception as e:
        logger.error(f"Streaming failed for {input_path}: {e}")
        return False


# =========================
# MAIN ORCHESTRATION
# =========================

def main() -> int:
    # Ensure directories exist
    if not os.path.exists(INPUT_DIR):
        os.makedirs(INPUT_DIR, exist_ok=True)
        logger.warning(f"Created input directory {INPUT_DIR}. Please place CSV files there.")
        return 0

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Find all CSV files
    input_files = glob.glob(os.path.join(INPUT_DIR, "*.csv"))

    if not input_files:
        logger.warning(f"No CSV files found in {INPUT_DIR}")
        return 0

    logger.info(f"Found {len(input_files)} files to process.")

    for input_file in input_files:
        filename = os.path.basename(input_file)
        logger.info(f"Processing {filename}...")

        # 1. Read Headers
        current_headers = get_csv_headers(input_file)
        if not current_headers:
            logger.error(f"Skipping {filename} due to missing/invalid headers.")
            continue

        # 2. Validate schema (deterministic first, then LLM)
        try:
            schema_result = determine_schema_mapping(current_headers, EXPECTED_HEADERS)
        except Exception as e:
            logger.error(f"Schema validation failed for {filename}: {e}")
            # Fail fast so Airflow marks the task as failed
            return 1

        logger.info(
            f"Schema analysis for {filename}: "
            f"{schema_result.get('reason', 'No reason provided')}"
        )

        mapping = schema_result.get("mapping", {})

        # 3. Process file with streaming pipeline
        success = process_file_streaming(input_file, OUTPUT_DIR, mapping)
        if not success:
            logger.error(f"ETL failed for {filename}. Aborting run.")
            return 1

    logger.info("All files processed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
