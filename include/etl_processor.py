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

# Critical columns that must be present for the pipeline to work
IMPORTANT_COLUMNS = [
    "METER_ID",
    "DATA_TIME",
    "IMPORT_ACTIVE_POWER",
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

def detect_csv_separator(filepath: str) -> str:
    """
    Detects the separator used in a CSV file by analyzing the first line.
    Returns the detected separator (comma, semicolon, tab, or pipe).
    """
    try:
        with open(filepath, "r", encoding="utf-8-sig") as f:
            header_line = f.readline().strip()
        
        # Count occurrences of common separators
        separators = {
            ",": header_line.count(","),
            ";": header_line.count(";"),
            "\t": header_line.count("\t"),
            "|": header_line.count("|"),
        }
        
        # Find the separator with most occurrences
        detected = max(separators, key=separators.get)
        
        # Only use detected separator if it appears at least once
        if separators[detected] > 0:
            logger.info(f"Detected CSV separator: '{detected}' (count: {separators[detected]})")
            return detected
        
        # Default to comma if no separator found
        logger.info("No separator detected, defaulting to comma")
        return ","
    except Exception as e:
        logger.warning(f"Failed to detect separator: {e}. Defaulting to comma.")
        return ","


def get_csv_headers(filepath: str, separator: str | None = None) -> tuple[list[str], str]:
    """
    Reads only the first line of the CSV efficiently to get headers.
    Auto-detects separator if not provided.
    Returns tuple of (headers, separator).
    """
    logger.info(f"Reading headers from {filepath}")
    try:
        # Auto-detect separator if not provided
        if separator is None:
            separator = detect_csv_separator(filepath)
        
        with open(filepath, "r", encoding="utf-8-sig") as f:  # Handle BOM
            header_line = f.readline().strip()
        headers = [h.replace('"', "").strip() for h in header_line.split(separator) if h.strip()]
        logger.info(f"Completed Reading Headers: {headers}")
        return headers, separator
    except Exception as e:
        logger.error(f"Failed to read headers from {filepath}: {e}")
        return [], ","


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


def map_important_columns_with_gemini(current_headers: list[str], important_columns: list[str]) -> dict:
    """
    Asks Gemini to map current headers to important columns only using structured output.
    Returns a dict with keys: status, mapping, reason.
    """
    logger.info("Asking Gemini to map important columns with LLM (structured output)...")

    prompt = f"""
    I am a data engineer pipeline.

    I need to map these Important Columns: {json.dumps(important_columns)}
    Incoming CSV Columns: {json.dumps(current_headers)}

    Task:
    1. For each Important Column, find the best matching column in Incoming CSV Columns.
    2. Consider variations like: typos, case differences, abbreviations, similar names.
       Examples: "READING_DATETIME" matches "DATA_TIME", "ACTIVE_IMP_POWER" matches "IMPORT_ACTIVE_POWER"
    3. Return the column_mappings array with each mapping having incoming_column (exactly as it appears in CSV) and target_column (the important column name).
    4. Only map columns that you are confident match. Do not force mappings.
    5. If you cannot find matches for all important columns, still return the mappings you found.
    
    Status meanings:
    - "SUCCESS": All important columns were mapped
    - "PARTIAL": Some important columns were mapped  
    - "FAILED": No important columns could be mapped
    """

    # Define the response schema for structured output
    # Using array of objects since Gemini doesn't support additionalProperties
    response_schema = {
        "type": "object",
        "properties": {
            "status": {
                "type": "string",
                "enum": ["SUCCESS", "PARTIAL", "FAILED"],
            },
            "column_mappings": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "incoming_column": {"type": "string"},
                        "target_column": {"type": "string"}
                    },
                    "required": ["incoming_column", "target_column"]
                }
            },
            "reason": {
                "type": "string",
            }
        },
        "required": ["status", "column_mappings", "reason"]
    }

    models_to_try = ["gemini-2.0-flash"]

    for model_name in models_to_try:
        try:
            model = genai.GenerativeModel(
                model_name,
                generation_config={
                    "response_mime_type": "application/json",
                    "response_schema": response_schema,
                }
            )
            response = model.generate_content(prompt)
            result = json.loads(response.text)
            logger.info("Completed LLM Important Columns Mapping")
            logger.info(f"LLM structured response: {json.dumps(result)}")
            
            # Convert column_mappings array to mapping dict for compatibility
            mapping = {}
            for item in result.get("column_mappings", []):
                incoming = item.get("incoming_column")
                target = item.get("target_column")
                if incoming and target:
                    mapping[incoming] = target
            
            return {
                "status": result.get("status", "FAILED"),
                "mapping": mapping,
                "reason": result.get("reason", "No reason provided"),
            }
        except Exception as e:
            if "404" in str(e) and "models/" in str(e):
                logger.warning(
                    f"Model {model_name} not found or not supported. Trying next..."
                )
                continue
            else:
                logger.error(f"LLM Important Columns Mapping failed with {model_name}: {e}")
                continue

    logger.error("All LLM models failed or are unavailable.")
    return {
        "status": "FAILED",
        "mapping": {},
        "reason": "All LLM models failed or unavailable",
    }


def validate_and_fix_mapping(mapping: dict, actual_headers: list[str]) -> dict:
    """
    Validates LLM-returned column names against actual CSV headers.
    Fixes case mismatches and removes invalid mappings.
    Returns corrected mapping dict.
    """
    if not mapping:
        return {}
    
    # Build case-insensitive lookup: normalized_name -> actual_name
    header_lookup = {h.strip().upper(): h for h in actual_headers}
    
    validated_mapping = {}
    for incoming_col, target_col in mapping.items():
        normalized_incoming = incoming_col.strip().upper()
        
        if incoming_col in actual_headers:
            # Exact match - use as is
            validated_mapping[incoming_col] = target_col
        elif normalized_incoming in header_lookup:
            # Case-insensitive match - use actual column name from CSV
            actual_col = header_lookup[normalized_incoming]
            logger.info(f"Fixed column name case: '{incoming_col}' -> '{actual_col}'")
            validated_mapping[actual_col] = target_col
        else:
            # Column not found in CSV - skip it
            logger.warning(f"LLM returned column '{incoming_col}' but it doesn't exist in CSV. Skipping.")
    
    return validated_mapping


def try_deterministic_important_mapping(current_headers: list[str], important_columns: list[str]) -> dict | None:
    """
    Attempts to map important columns using known variations without LLM.
    Returns mapping dict if all important columns can be matched, None otherwise.
    """
    # Known column name variations for important columns
    column_variations = {
        "METER_ID": ["METER_ID", "METERID", "METER", "MTR_ID", "ID_METER"],
        "DATA_TIME": ["DATA_TIME", "DATATIME", "DATETIME", "DATE_TIME", "READING_DATETIME", 
                      "READ_TIME", "TIMESTAMP", "TIME", "DT", "READING_TIME", "READ_DATETIME"],
        "IMPORT_ACTIVE_POWER": ["IMPORT_ACTIVE_POWER", "IMPORTACTIVEPOWER", "ACTIVE_IMPORT_POWER",
                                "ACTIVE_IMP_POWER", "IMP_ACTIVE_POWER", "IMPORT_POWER", 
                                "ACTIVE_POWER", "KW_IMPORT", "KW_IMP", "POWER_IMPORT"],
    }
    
    # Build case-insensitive lookup: normalized_name -> actual_name
    header_lookup = {h.strip().upper(): h for h in current_headers}
    
    mapping = {}
    for important_col in important_columns:
        variations = column_variations.get(important_col, [important_col])
        matched = False
        
        for variation in variations:
            normalized_variation = variation.strip().upper()
            if normalized_variation in header_lookup:
                actual_header = header_lookup[normalized_variation]
                # Only add to mapping if name differs from target
                if actual_header != important_col:
                    mapping[actual_header] = important_col
                matched = True
                break
        
        if not matched:
            logger.info(f"Deterministic mapping: Could not find match for '{important_col}'")
            return None  # Could not match this important column
    
    logger.info(f"Deterministic mapping succeeded: {json.dumps(mapping)}")
    return mapping


def determine_schema_mapping(current_headers: list[str], expected_headers: list[str]) -> dict:
    """
    First tries deterministic mapping, then falls back to LLM if needed.
    If important columns can be mapped, proceeds with only those columns 
    instead of requiring all expected headers.
    Returns {"mapping": dict, "reason": str, "important_only": bool}.
    """
    logger.info("Determining schema mapping...")
    
    # Log original headers
    logger.info(f"Original CSV headers: {json.dumps(current_headers)}")

    normalized_current = [h.strip().upper() for h in current_headers]
    normalized_expected = [h.strip().upper() for h in expected_headers]

    # Fast path: exact match ignoring case
    if normalized_current == normalized_expected:
        reason = "Exact header match ignoring case"
        logger.info("Headers match expected schema (case-insensitive); skipping LLM validation.")
        logger.info("Completed Determining Schema Mapping")
        return {"mapping": {}, "reason": reason, "important_only": False}

    # Try deterministic mapping first (no LLM needed)
    logger.info("Attempting deterministic mapping of important columns...")
    deterministic_mapping = try_deterministic_important_mapping(current_headers, IMPORTANT_COLUMNS)
    
    if deterministic_mapping is not None:
        return {
            "mapping": deterministic_mapping,
            "reason": "Important columns mapped deterministically using known variations",
            "important_only": True
        }
    
    logger.info("Deterministic mapping failed, falling back to LLM...")
    
    # Fall back to LLM with structured output to map important columns
    important_mapping_result = map_important_columns_with_gemini(current_headers, IMPORTANT_COLUMNS)
    important_status = important_mapping_result.get("status", "FAILED")
    important_mapping = important_mapping_result.get("mapping", {})
    
    # Validate and fix the mapping against actual CSV headers
    important_mapping = validate_and_fix_mapping(important_mapping, current_headers)
    
    # Log the important columns mapping
    logger.info(f"Important columns mapping result: {json.dumps(important_mapping_result)}")
    logger.info(f"Validated column mapping: {json.dumps(important_mapping)}")
    
    # Check if all important columns can be mapped
    mapped_important_cols = set(important_mapping.values())
    required_important_cols = set(IMPORTANT_COLUMNS)
    
    if important_status == "SUCCESS" or mapped_important_cols == required_important_cols:
        # All important columns mapped - proceed with important columns only
        logger.info(f"Successfully mapped all important columns. Proceeding with important columns only.")
        logger.info(f"Mapped columns: {json.dumps(important_mapping)}")
        return {
            "mapping": important_mapping,
            "reason": f"Important columns mapped successfully: {important_mapping_result.get('reason', 'No reason')}",
            "important_only": True
        }
    
    # Check if we have partial mapping that covers all important columns
    # (LLM might return "PARTIAL" but still map everything we need)
    if len(mapped_important_cols) >= len(required_important_cols) and required_important_cols.issubset(mapped_important_cols):
        logger.info(f"LLM returned {important_status} but all important columns are covered. Proceeding.")
        return {
            "mapping": important_mapping,
            "reason": f"All important columns mapped (status was {important_status})",
            "important_only": True
        }
    
    # If important columns can't be mapped, try full schema validation
    logger.warning(f"Could not map all important columns (status: {important_status}, mapped: {mapped_important_cols}). Trying full schema validation...")
    validation = validate_schema_with_gemini(current_headers, expected_headers)
    status = validation.get("status", "ABORT")
    reason = validation.get("reason", "No reason provided")

    if status == "ABORT":
        logger.error(f"Schema validation ABORT: {reason}")
        raise ValueError(f"Schema validation ABORT: {reason}")

    mapping = validation.get("mapping", {}) if status == "RENAME" else {}
    
    # Validate and fix the mapping against actual CSV headers
    mapping = validate_and_fix_mapping(mapping, current_headers)

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
    return {"mapping": mapping, "reason": reason, "important_only": False}


def get_file_quarter(input_path: str, col_mapping: dict = None, separator: str = ",") -> str:
    """
    Reads a small sample of the file to determine the quarter for file organization.
    Returns string like '2023-Q4', or 'UNKNOWN_QUARTER' if not determinable.
    
    Args:
        input_path: Path to CSV file
        col_mapping: Optional mapping dictionary to find original column name for DATA_TIME
        separator: CSV separator character (default: comma)
    """
    logger.info("Determining file quarter...")
    try:
        df = pl.read_csv(
            input_path,
            separator=separator,
            quote_char='"',
            n_rows=1,
            ignore_errors=True,
        )

        # Find the column name that maps to DATA_TIME (or use DATA_TIME directly if no mapping)
        data_time_col = None
        if col_mapping:
            # Reverse lookup: find original column that maps to DATA_TIME
            for orig_col, mapped_col in col_mapping.items():
                if mapped_col == "DATA_TIME":
                    data_time_col = orig_col
                    break
        
        # If no mapping found or no mapping provided, check for DATA_TIME directly
        if not data_time_col:
            data_time_col = "DATA_TIME" if "DATA_TIME" in df.columns else None
        
        # If still not found, check if any column maps to DATA_TIME after applying mapping
        if not data_time_col and col_mapping:
            for col in df.columns:
                if col_mapping.get(col) == "DATA_TIME":
                    data_time_col = col
                    break

        if data_time_col and data_time_col in df.columns:
            date_val = datetime.strptime(df[data_time_col][0], "%Y-%m-%d %H:%M:%S")
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
    important_only: bool = False,
    separator: str = ",",
) -> pl.LazyFrame:
    """
    Build the Polars lazy pipeline: scan CSV, apply mapping, parse dates,
    add derived columns, cast numeric types, optional row hash.
    
    Args:
        input_path: Path to input CSV file
        col_mapping: Dictionary mapping original column names to expected names
        quarter_str: Quarter string for file organization
        important_only: If True, select only important columns after mapping
        separator: CSV separator character
    """
    # 1. Lazy Scan (no data loaded yet)
    t_scan_start = time.perf_counter()
    q = pl.scan_csv(
        input_path,
        separator=separator,
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
    
    # 2.5. If important_only mode, select only important columns
    if important_only:
        logger.info(f"Selecting only important columns: {IMPORTANT_COLUMNS}")
        q = q.select(IMPORTANT_COLUMNS)

    # 3. Parse DATA_TIME to datetime (strip fractional seconds, truncate to minute)
    q = q.with_columns(
        [
            pl.coalesce(
                # Handle format with fractional seconds: "2025-08-02 00:30:00.4600000"
                pl.col("DATA_TIME").str.replace(r"\.\d+$", "").str.strptime(
                    pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False
                ),
                # Standard format without fractional seconds
                pl.col("DATA_TIME").str.strptime(
                    pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False
                ),
                # Date only
                pl.col("DATA_TIME").str.strptime(
                    pl.Datetime, format="%Y-%m-%d", strict=False
                ),
            ).dt.truncate("1m").alias("DATA_TIME"),  # Truncate to minute
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
    # Only cast columns that exist in the dataframe
    power_columns_to_cast = ["IMPORT_ACTIVE_POWER"]
    if not important_only:
        # Add additional power columns only when processing full schema
        power_columns_to_cast.extend([
            "EXPORT_ACTIVE_POWER",
            "IMPORT_REACTIVE_POWER",
            "EXPORT_REACTIVE_POWER",
        ])
    
    power_casts = [
        pl.col(col).cast(pl.Utf8).str.strip_chars().cast(pl.Float64, strict=False)
        for col in power_columns_to_cast
    ]
    q = q.with_columns(power_casts)

    # 6. Optional deterministic ROW_HASH (can be heavy on very large files)
    # NOTE: ROW_HASH is based on METER_ID + DATA_TIME_STR only (natural key).
    # This aligns with dbt deduplication logic in stg_meter_readings.sql which uses
    # FARM_FINGERPRINT(CONCAT(METER_ID, DATA_TIME)). Including IMPORT_ACTIVE_POWER
    # would cause records with same meter/time but different power values to be
    # treated as distinct in BigQuery MERGE but duplicates in dbt.
    logger.info(f"Row hash enabled: {ENABLE_ROW_HASH}")
    if ENABLE_ROW_HASH:
        q = q.with_columns(
            [
                (
                    pl.concat_str(
                        [
                            pl.col("METER_ID").cast(pl.Utf8),
                            pl.col("DATA_TIME_STR"),
                        ]
                    ).hash()
                    % (2**63 - 1)
                )
                .cast(pl.Int64)
                .alias("ROW_HASH")
            ]
        )

    return q


def process_file_streaming(
    input_path: str, 
    output_dir: str, 
    col_mapping: dict, 
    important_only: bool = False,
    separator: str = ",",
) -> bool:
    """
    Streams data from CSV to Parquet using a Polars lazy pipeline.
    The heavy work (read, transform, write) executes at sink_parquet.
    
    Args:
        input_path: Path to input CSV file
        output_dir: Directory to write output Parquet files
        col_mapping: Dictionary mapping original column names to expected names
        important_only: If True, process only important columns
        separator: CSV separator character
    """
    logger.info(f"Starting stream processing for {input_path}...")
    t0 = time.perf_counter()

    try:
        # 1. Determine Output Path based on Quarter
        quarter_folder = get_file_quarter(input_path, col_mapping, separator)
        quarter_str = quarter_folder if quarter_folder != "UNKNOWN_QUARTER" else None

        target_dir = os.path.join(output_dir, quarter_folder)
        os.makedirs(target_dir, exist_ok=True)

        filename = os.path.basename(input_path).replace(".csv", ".parquet")
        output_path = os.path.join(target_dir, filename)

        logger.info("2")  # simple progress marker
        q = build_lazy_pipeline(input_path, col_mapping, quarter_str, important_only, separator)
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

        # 3. CRITICAL: Validate that data was actually written (prevent silent data loss)
        if not os.path.exists(output_path):
            raise RuntimeError(f"Parquet file was not created at {output_path}")

        # Read parquet metadata to verify row count without loading data into memory
        parquet_metadata = pl.scan_parquet(output_path)
        row_count = parquet_metadata.select(pl.len()).collect().item()

        if row_count == 0:
            # Remove the empty file to avoid downstream confusion
            os.remove(output_path)
            raise RuntimeError(
                f"CRITICAL: Zero rows written to {output_path}. "
                f"Input file: {input_path}. This indicates a data loss condition "
                "(e.g., date range mismatch, filter eliminating all rows, or corrupt source data)."
            )

        logger.info(f"Validated: {row_count:,} rows written to parquet file")
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

        # 1. Read Headers (auto-detects separator)
        current_headers, separator = get_csv_headers(input_file)
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
        important_only = schema_result.get("important_only", False)

        # 3. Process file with streaming pipeline
        success = process_file_streaming(input_file, OUTPUT_DIR, mapping, important_only, separator)
        if not success:
            logger.error(f"ETL failed for {filename}. Aborting run.")
            return 1

    logger.info("All files processed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
