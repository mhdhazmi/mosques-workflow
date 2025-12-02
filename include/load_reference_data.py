import pandas as pd
import os
from google.cloud import bigquery
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Config
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "testing-444715")
DATASET_ID = os.getenv("GCP_DATASET_ID", "raw_meter_readings")
CREDENTIALS_PATH = "include/gcp_adc.json"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH

def load_excel_to_bq(file_path, table_name):
    logger.info(f"Processing {file_path} -> {table_name}...")
    try:
        # Read Excel
        df = pd.read_excel(file_path)
        
        # Clean column names (remove spaces, special chars)
        df.columns = [c.strip().replace(" ", "_").replace("(", "").replace(")", "").upper() for c in df.columns]
        
        # Add load timestamp
        df["_LOADED_AT"] = pd.Timestamp.now()

        # Load to BigQuery
        full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        
        client = bigquery.Client(project=PROJECT_ID)
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE", # Overwrite existing reference data
            autodetect=True,
        )
        
        job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
        job.result() # Wait for completion
        
        logger.info(f"Successfully loaded {len(df)} rows to {full_table_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to load {file_path}: {e}")
        return False

if __name__ == "__main__":
    # 1. Load Prayer Times
    load_excel_to_bq(
        "include/utils_data/prayer_times_2024.xlsx", 
        "raw_prayer_times"
    )
    
    # 2. Load Meter Locations (Industry Code)
    load_excel_to_bq(
        "include/utils_data/Industry Code with Population.xlsx", 
        "raw_meter_locations"
    )
