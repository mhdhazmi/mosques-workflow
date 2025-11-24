# Smart Meter Data Pipeline Walkthrough

This guide provides a complete end-to-end walkthrough for setting up, configuring, and running the Smart Meter Data Pipeline.

## 1. Prerequisites & Installation

Before starting, ensure you have the following installed:

### A. Docker Desktop
Required for running Airflow locally.
- [Install Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Ensure it is running and allocated at least 4GB of RAM.

### B. Astro CLI
The command-line interface for running Airflow.
- **Mac/Linux**: `curl -sSL install.astronomer.io | sudo bash -s`
- **Windows**: `winget install -e --id Astronomer.Astro` (or download from [GitHub](https://github.com/astronomer/astro-cli/releases))

### C. Google Cloud SDK (gcloud)
Required for authentication and interacting with GCP.
- [Install Google Cloud SDK](https://cloud.google.com/sdk/docs/install)

### D. Terraform
Required for provisioning cloud infrastructure.
- [Install Terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)

---

## 2. Authentication (Critical Step)

This pipeline uses **Application Default Credentials (ADC)**. Since Airflow runs in a Docker container, we must generate credentials on your host machine and then **copy them** into the project so the container can see them.

1.  **Login to Google Cloud**:
    ```bash
    gcloud auth application-default login
    ```
    This opens a browser. Log in with your Google account.
    *Result*: A JSON key file is created at `~/.config/gcloud/application_default_credentials.json` (Linux/Mac) or `%APPDATA%\gcloud\application_default_credentials.json` (Windows).

2.  **Copy Credentials to Project**:
    You **MUST** copy this file to the `include/` directory of this project and name it `gcp_adc.json`.
    
    **Linux/Mac/WSL**:
    ```bash
    cp ~/.config/gcloud/application_default_credentials.json include/gcp_adc.json
    ```
    
    **Windows (PowerShell)**:
    ```powershell
    copy "$env:APPDATA\gcloud\application_default_credentials.json" include\gcp_adc.json
    ```

    > **Note**: `include/gcp_adc.json` is git-ignored to prevent accidental commits of secrets.

---

## 3. Infrastructure Setup (Terraform)

Provision the BigQuery dataset and GCS bucket.

1.  **Navigate to Infra**:
    ```bash
    cd infra
    ```

2.  **Configure Variables**:
    Create `terraform.tfvars` from the example:
    ```bash
    cp terraform.tfvars.example terraform.tfvars
    ```
    Edit `terraform.tfvars` with your unique values:
    ```hcl
    project_id  = "your-gcp-project-id"
    region      = "me-central2"  # or your preferred region
    bucket_name = "your-unique-bucket-name"
    ```

3.  **Deploy**:
    ```bash
    terraform init
    terraform apply
    ```
    *Type `yes` when prompted.*

4.  **Return to Root**:
    ```bash
    cd ..
    ```

---

## 4. Configuration (.env)

The pipeline behavior is controlled by environment variables.

1.  **Create .env**:
    ```bash
    cp .env.example .env
    ```

2.  **Edit .env**:
    Update the values to match your Terraform outputs and preferences.
    
    *   **GCP Config**: Set `GCP_PROJECT_ID`, `GCP_BUCKET_NAME`, etc.
    *   **API Key**: Set `GOOGLE_API_KEY` (for Gemini LLM).
    *   **Performance Tunables**:
        *   `ETL_LOW_MEMORY_MODE="true"`: Recommended for local dev.
        *   `ETL_ROW_GROUP_SIZE="50000"`: Adjust based on RAM.
        *   `PARQUET_COMPRESSION="snappy"`: Fast compression.
        *   `ETL_ENABLE_ROW_HASH="true"`: Enables deduplication logic.

---

## 5. Running the Pipeline

1.  **Start Airflow**:
    ```bash
    astro dev start
    ```
    *Tip: This may take 2-5 minutes on the first run as it builds the Docker image and installs dependencies.*

2.  **Access UI**:
    Open [http://localhost:8080](http://localhost:8080) in your browser.
    *   **User**: `admin`
    *   **Password**: `admin`

3.  **Trigger DAG**:
    *   Find `meter_data_pipeline` in the DAGs list.
    *   Click the **Play** button (Trigger DAG) -> **Trigger**.

4.  **Monitor Progress**:
    *   Click on the DAG name to see the Grid view.
    *   Watch tasks turn dark green (Success).
    *   **Execution Time**:
        *   **ETL**: ~1-3 minutes per GB of data (depends on CPU).
        *   **Upload**: Depends on internet upload speed.
        *   **Load & dbt**: Usually < 1 minute.

---

## 6. Pipeline Architecture & Data Schema

### A. ETL Processor (`etl_processor.py`)
*   **Input**: CSV files in `include/raw_data/`.
*   **Schema Validation**:
    *   First checks for exact header matches (fast).
    *   Falls back to **Gemini LLM** to map columns if headers are messy/renamed.
*   **Date Parsing**: Handles `YYYY-MM-DD HH:MM:SS` and `YYYY-MM-DD`.
*   **Hashing**: Generates a deterministic `ROW_HASH` (Int64) for deduplication.
*   **Output**: Parquet files in `include/processed_data/`, partitioned by Quarter (e.g., `2023-Q4`).

### B. Cloud Loader (`cloud_loader.py`)
*   **Upload**: Moves Parquet files to GCS bucket.
*   **BigQuery Load**:
    *   Uses **MERGE** statement to insert new rows and skip duplicates based on `ROW_HASH`.
    *   Uses **UUID-named temp tables** to ensure safety during parallel runs.
    *   Automatically creates the target table if it doesn't exist.

### C. dbt Transformations (`dbt_pipeline.py`)
*   **Models**:
    *   `smart_meters_clean`: The raw table loaded from GCS.
    *   `consumption_stats`: Daily aggregation (Avg, Min, Max power).
*   **Tests**:
    *   `not_null`: Ensures critical fields (METER_ID, DATE) are present.
    *   `unique`: Ensures no duplicate daily stats per meter.

---

## 7. Troubleshooting

*   **dbt Auth Failure**:
    *   *Error*: `Unable to generate access token`.
    *   *Fix*: Ensure `quota_project_id` is **NOT** set in `include/profiles.yml` when using `gcp_adc.json`.
*   **Schema Mismatch**:
    *   *Error*: `Provided Schema does not match Table...`
    *   *Fix*: Drop the table in BigQuery (`DROP TABLE ...`) and let the pipeline recreate it with the correct schema.
*   **Missing Credentials**:
    *   *Error*: `DefaultCredentialsError: Could not automatically determine credentials.`
    *   *Fix*: Re-run the copy command in **Step 2** to ensure `include/gcp_adc.json` exists.
