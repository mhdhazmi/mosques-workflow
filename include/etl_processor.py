import polars as pl
import google.generativeai as genai
import json
import os
import glob
import logging
import sys
import gc
from datetime import datetime

# --- CONFIGURATION ---
# Set your API key in your environment variables or paste it here (not recommended for prod)
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "YOUR_API_KEY_HERE")
genai.configure(api_key=GOOGLE_API_KEY)

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/usr/local/airflow")
INPUT_DIR = os.path.join(AIRFLOW_HOME, "include/raw_data")
OUTPUT_DIR = os.path.join(AIRFLOW_HOME, "include/processed_data")

# Tunables from .env
ROW_GROUP_SIZE = int(os.getenv("ETL_ROW_GROUP_SIZE", "50000"))
LOW_MEMORY_MODE = os.getenv("ETL_LOW_MEMORY_MODE", "true").lower() == "true"

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

# --- LOGGING SETUP ---
logger = logging.getLogger("etl_processor")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}',
    datefmt='%Y-%m-%dT%H:%M:%S%z',
)
handler.setFormatter(formatter)
logger.addHandler(handler)


def get_csv_headers(filepath, separator=","):
    """Reads only the first line of the CSV efficiently."""
    try:
        with open(filepath, "r", encoding="utf-8-sig") as f:  # Handle BOM
            header_line = f.readline().strip()
        headers = [h.replace('"', "") for h in header_line.split(separator) if h]
        return headers
    except Exception as e:
        logger.error(f"Failed to read headers from {filepath}: {e}")
        return []


def validate_schema_with_gemini(current_headers, expected_headers):
    """Asks Gemini to map current headers to expected headers."""
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
            return json.loads(cleaned_text)
        except Exception as e:
            if "404" in str(e) and "models/" in str(e):
                logger.warning(
                    f"Model {model_name} not found or not supported. Trying next..."
                )
                continue
            else:
                logger.error(f"LLM Validation failed with {model_name}: {e}")
                continue

    logger.error("All LLM models failed.")
    try:
        logger.info("Listing available models for debugging:")
        for m in genai.list_models():
            if "generateContent" in m.supported_generation_methods:
                logger.info(f"- {m.name}")
    except Exception as e:
        logger.error(f"Could not list models: {e}")

    return {"status": "ABORT", "reason": "All LLM Models Failed"}


def determine_schema_mapping(current_headers, expected_headers):
    """
    First try deterministic matching, then fall back to LLM.
    Ensures that after mapping we have all expected headers and no duplicates.
    """
    normalized_current = [h.strip().upper() for h in current_headers]
    normalized_expected = [h.strip().upper() for h in expected_headers]

    # Fast path: exact match ignoring case
    if normalized_current == normalized_expected:
        logger.info(
            "Headers match expected schema (case-insensitive); skipping LLM validation."
        )
        return {"mapping": {}, "reason": "Exact header match ignoring case"}

    # Otherwise ask the LLM
    validation = validate_schema_with_gemini(current_headers, expected_headers)
    status = validation.get("status", "ABORT")
    mapping = validation.get("mapping", {}) if status == "RENAME" else {}

    # Apply mapping to see resulting header set
    final_headers = [mapping.get(h, h) for h in current_headers]

    # Check for duplicates
    if len(final_headers) != len(set(final_headers)):
        raise ValueError(f"Duplicate column names after mapping: {final_headers}")

    # Ensure we have all expected columns
    missing = set(expected_headers) - set(final_headers)
    if missing:
        raise ValueError(
            f"Missing expected columns after mapping: {sorted(missing)}"
        )

    return {"mapping": mapping, "reason": validation.get("reason", "")}


def get_file_quarter(input_path):
    """
    Peeks at the first row to determine the quarter for file organization.
    Returns string like '2023-Q4'.
    """
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
            return f"{date_val.year}-Q{quarter}"
    except Exception as e:
        logger.warning(f"Could not determine quarter from file content: {e}")

    return "UNKNOWN_QUARTER"


def process_file_streaming(input_path, output_dir, col_mapping):
    """
    Streams data from CSV to Parquet without loading file into RAM.
    """
    logger.info(f"Starting stream processing for {input_path}...")

    try:
        # 1. Determine Output Path based on Quarter
        quarter_folder = get_file_quarter(input_path)
        target_dir = os.path.join(output_dir, quarter_folder)
        os.makedirs(target_dir, exist_ok=True)

        filename = os.path.basename(input_path).replace(".csv", ".parquet")
        output_path = os.path.join(target_dir, filename)

        # 2. Lazy Scan (Does not load data yet)
        q = pl.scan_csv(
            input_path,
            separator=",",
            quote_char='"',
            infer_schema_length=10000,
            ignore_errors=True,
            low_memory=LOW_MEMORY_MODE,
            rechunk=False,
        )

        # 3. Apply LLM-driven column renames (if any)
        if col_mapping:
            q = q.rename(col_mapping)

        # 4. Parse DATA_TIME to datetime (handle both full timestamp and date-only)
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

        # 5. Add a string copy and quarter column
        q = q.with_columns(
            [
                pl.col("DATA_TIME")
                .dt.strftime("%Y-%m-%d %H:%M:%S")
                .alias("DATA_TIME_STR"),
                (
                    pl.col("DATA_TIME").dt.year().cast(pl.Utf8)
                    + "-Q"
                    + pl.col("DATA_TIME").dt.quarter().cast(pl.Utf8)
                ).alias("QUARTER"),
            ]
        )

        # 6. Ensure numeric types
        q = q.with_columns(
            [
                pl.col("IMPORT_ACTIVE_POWER").str.strip_chars().cast(pl.Float64),
                pl.col("EXPORT_ACTIVE_POWER").str.strip_chars().cast(pl.Float64),
                pl.col("IMPORT_REACTIVE_POWER").str.strip_chars().cast(pl.Float64),
                pl.col("EXPORT_REACTIVE_POWER").str.strip_chars().cast(pl.Float64),
            ]
        )

        # 7. Deterministic ROW_HASH using stable string representation
        # Use modulo to ensure it fits in BigQuery INT64 (signed)
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

        # 8. Sink to Parquet
        q.sink_parquet(
            output_path, compression="zstd", row_group_size=ROW_GROUP_SIZE
        )
        logger.info(f"Success! Processed data saved to {output_path}")

        del q
        gc.collect()

        return True
    except Exception as e:
        logger.error(f"Streaming failed for {input_path}: {e}")
        return False


# --- MAIN ORCHESTRATION ---
if __name__ == "__main__":
    # Ensure directories exist
    if not os.path.exists(INPUT_DIR):
        os.makedirs(INPUT_DIR)
        logger.warning(f"Created input directory {INPUT_DIR}. Please place CSV files there.")
        exit(0)

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # Find all CSV files
    input_files = glob.glob(os.path.join(INPUT_DIR, "*.csv"))

    if not input_files:
        logger.warning(f"No CSV files found in {INPUT_DIR}")
        exit(0)

    logger.info(f"Found {len(input_files)} files to process.")

    for input_file in input_files:
        filename = os.path.basename(input_file)
        logger.info(f"Processing {filename}...")

        # 1. Read Headers
        current_headers = get_csv_headers(input_file)
        if not current_headers:
            continue

        # 2. Validate schema (deterministic first, then LLM)
        try:
            schema_result = determine_schema_mapping(current_headers, EXPECTED_HEADERS)
        except Exception as e:
            logger.error(f"Schema validation failed for {filename}: {e}")
            # Fail fast so Airflow marks the task as failed instead of silently skipping
            raise

        logger.info(
            f"Schema analysis for {filename}: "
            f"{schema_result.get('reason', 'No reason provided')}"
        )

        mapping = schema_result.get("mapping", {})

        # 3. Process
        process_file_streaming(input_file, OUTPUT_DIR, mapping)
