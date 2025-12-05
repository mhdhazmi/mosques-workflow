# Smart Meter Data Pipeline - Mosque Energy Consumption Analysis

This guide provides a complete end-to-end walkthrough for setting up, configuring, and running the Smart Meter Data Pipeline for analyzing mosque energy consumption patterns based on prayer times.

## Table of Contents
- [Prerequisites & Installation](#1-prerequisites--installation)
- [Authentication](#2-authentication-critical-step)
- [Infrastructure Setup](#3-infrastructure-setup-terraform)
- [Configuration](#4-configuration-env)
- [Loading Reference Data](#5-loading-reference-data-dbt-seeds)
- [Running the Pipeline](#6-running-the-pipeline)
- [Pipeline Architecture](#7-pipeline-architecture--data-flow)
- [dbt Models Documentation](#8-dbt-models-documentation)
- [Troubleshooting](#9-troubleshooting)

---

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

### E. Python & uv (for local dbt development)
Required for running dbt commands locally outside of Airflow.
- Python 3.10+
- uv package manager: `pip install uv`

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
    *   **API Key**: Set `GOOGLE_API_KEY` (for Gemini LLM schema validation).
    *   **Performance Tunables**:
        *   `ETL_LOW_MEMORY_MODE="true"`: Recommended for local dev.
        *   `ETL_ROW_GROUP_SIZE="50000"`: Adjust based on RAM.
        *   `PARQUET_COMPRESSION="snappy"`: Fast compression.
        *   `ETL_ENABLE_ROW_HASH="true"`: Enables deduplication logic.

---

## 5. Loading Reference Data (dbt Seeds)

**IMPORTANT**: Before running the main pipeline, you must load reference data (prayer times and meter locations) into BigQuery.

### What are dbt Seeds?
Seeds are CSV files in `dags/dbt/seeds/` that contain static reference data. They are loaded directly into BigQuery as tables.

### Reference Data Files:
- **`prayer_times.csv`**: Contains prayer times (Fajr, Dhuhr, Isha) for different coordinates and dates
- **`industry_codes.csv`**: Contains meter metadata including multiplication factors, coordinates, regions, and provinces

### Steps to Load Seeds:

1.  **Navigate to dbt directory** (if running locally):
    ```bash
    cd dags/dbt
    ```

2.  **Load seeds into BigQuery**:
    ```bash
    dbt seed --project-dir . --profiles-dir ../../include --target dev
    ```

    Or from project root:
    ```bash
    dbt seed --project-dir ./dags/dbt --profiles-dir ./include --target dev
    ```

3.  **Verify seeds loaded**:
    ```bash
    dbt show --select prayer_times --project-dir ./dags/dbt --profiles-dir ./include --target dev
    ```

### What Gets Created:
- `raw_meter_readings.prayer_times` - Prayer schedule data
- `raw_meter_readings.industry_codes` - Meter location and metadata

**Note**: Seeds should be re-run whenever the CSV files are updated (e.g., new year's prayer times, new meter installations).

---

## 6. Running the Pipeline

### Prerequisites:
1. ✅ Infrastructure provisioned (Terraform)
2. ✅ Credentials copied to `include/gcp_adc.json`
3. ✅ `.env` file configured
4. ✅ **Reference data loaded via `dbt seed`**
5. ✅ Raw meter data CSV files placed in `include/raw_data/`

### Steps:

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

## 7. Pipeline Architecture & Data Flow

```
┌─────────────────────┐
│  Raw CSV Files      │  ← Smart meter readings (include/raw_data/)
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  ETL Processor      │  ← Schema validation (Gemini LLM), deduplication,
│  (etl_processor.py) │     outlier filtering, parquet conversion
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Cloud Loader       │  ← Upload to GCS, load to BigQuery with MERGE
│  (cloud_loader.py)  │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────┐
│              BigQuery Raw Tables                         │
│  - smart_meters_clean (fact: meter readings)            │
│  - prayer_times (dimension: from dbt seed)              │
│  - industry_codes (dimension: from dbt seed)            │
└──────────┬──────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────┐
│  dbt Transformations│  ← Staging → Intermediate → Marts
│  (Cosmos/dbt)       │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────┐
│              Analytics Tables (Marts)                    │
│  - consumption_analysis (final metrics by meter)        │
└─────────────────────────────────────────────────────────┘
```

### Component Breakdown:

#### A. ETL Processor (`include/etl_processor.py`)
**Purpose**: Clean, validate, and prepare raw CSV data for loading.

**Key Features**:
- **Schema Validation**:
  - Fast exact match check first
  - Falls back to **Gemini LLM** for fuzzy column mapping
- **Data Quality**:
  - Filters outliers: readings > 1GW or < 0 set to NULL
  - Rounds values to 3 decimal places
  - Deduplicates using `ROW_HASH`
- **Optimizations**:
  - Streaming pipeline (low memory mode)
  - Polars for fast processing
  - Partitioned by quarter (e.g., `2023-Q4`)
- **Output**: Parquet files in `include/processed_data/`

#### B. Cloud Loader (`include/cloud_loader.py`)
**Purpose**: Upload processed data to cloud and load into BigQuery.

**Key Features**:
- **Upload**: Moves Parquet files to GCS bucket with chunked uploads
- **BigQuery Load**:
  - Uses **MERGE** on `ROW_HASH` to avoid duplicates
  - UUID-named temp tables for safe parallel runs
  - Automatically creates target table if missing
- **Output**: `raw_meter_readings.smart_meters_clean` table

#### C. dbt Transformations (via Airflow Cosmos)
**Purpose**: Transform raw data into analytics-ready tables.

See detailed [dbt Models Documentation](#8-dbt-models-documentation) below.

---

## 8. dbt Models Documentation

The dbt pipeline follows the **medallion architecture**: Bronze (raw) → Silver (staging/intermediate) → Gold (marts).

### Data Flow Overview

```
Seeds (CSV → BigQuery)
├── prayer_times.csv      → prayer_times table
└── industry_codes.csv    → industry_codes table

Source Tables (from ETL)
└── smart_meters_clean    → Raw meter readings

          ↓

Staging Layer (stg_*)
├── stg_meter_readings       ← Dedupe, filter outliers, parse dates
├── stg_prayer_times         ← Parse coordinates & times
└── stg_industry_codes       ← Parse meter locations & metadata

          ↓

Intermediate Layer (int_*)
├── int_meter_locations      ← Match meters to nearest prayer location
└── int_meter_readings_with_periods ← Join readings with prayer periods

          ↓

Marts Layer
└── consumption_analysis     ← Final aggregated metrics
```

---

### 🌱 Seeds (Reference Data)

Located in: `dags/dbt/seeds/`

#### `prayer_times.csv`
**Purpose**: Static prayer schedule for the year.

| Column | Type | Description |
|--------|------|-------------|
| Coordinate | String | Geographic coordinate in format "(lat, lon)" |
| Date | String | Date in format "DD-MM-YYYY" |
| Fajr | String | Fajr prayer time "HH:MM" |
| Dhuhr | String | Dhuhr prayer time "HH:MM" |
| Asr | String | Asr prayer time "HH:MM" |
| Maghrib | String | Maghrib prayer time "HH:MM" |
| Isha | String | Isha prayer time "HH:MM" |

**Note**: Prayer times repeat annually based on solar position.

#### `industry_codes.csv`
**Purpose**: Meter location and configuration metadata.

| Column | Type | Description |
|--------|------|-------------|
| Meter Number | String | Unique meter identifier |
| Multiplication Factor | Float | Scaling factor for power readings |
| X Coordinates | Float | Longitude |
| Y Coordinates | Float | Latitude |
| Region | String | Geographic region |
| Province | String | Province name |
| Department Name | String | Administrative department |
| Office Name | String | Managing office |

---

### 📊 Staging Models (`dags/dbt/models/staging/`)

Staging models clean and standardize raw data with minimal transformations.

#### `stg_meter_readings.sql`
**Purpose**: Clean meter readings with deduplication and outlier filtering.

**Transformations**:
1. **Deduplication**: Uses `ROW_HASH` with `QUALIFY ROW_NUMBER()` to remove duplicates
2. **Outlier Filtering**: Sets readings > 1GW or < 0 to NULL
3. **Precision**: Rounds to 3 decimal places
4. **Date Parsing**: Extracts `reading_date` and `reading_time` from timestamp

**Output Columns**:
- `meter_id` (STRING)
- `reading_at` (TIMESTAMP)
- `active_power_watts` (FLOAT64, rounded to 3 decimals)
- `reading_date` (DATE)
- `reading_time` (TIME)

**Key Logic**:
```sql
-- Remove 60% duplicates from raw data
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ROW_HASH
    ORDER BY DATA_TIME DESC
) = 1

-- Filter outliers to NULL
ROUND(CASE
    WHEN IMPORT_ACTIVE_POWER > 1000000000 THEN NULL  -- > 1GW
    WHEN IMPORT_ACTIVE_POWER < 0 THEN NULL
    ELSE IMPORT_ACTIVE_POWER
END, 3)
```

---

#### `stg_prayer_times.sql`
**Purpose**: Parse and standardize prayer schedule data.

**Transformations**:
1. **Coordinate Parsing**: Converts "(lat, lon)" string to `GEOGRAPHY` point
2. **Date Parsing**: Converts "DD-MM-YYYY" to DATE
3. **Time Parsing**: Converts "HH:MM" strings to TIME type

**Output Columns**:
- `location` (GEOGRAPHY) - Geographic point
- `original_coordinate` (STRING) - Raw coordinate for joining
- `date` (DATE)
- `fajr_time`, `dhuhr_time`, `asr_time`, `maghrib_time`, `isha_time` (TIME)

---

#### `stg_industry_codes.sql`
**Purpose**: Parse meter metadata and create geographic points.

**Transformations**:
1. **Geographic Points**: Creates `GEOGRAPHY` from X/Y coordinates
2. **Multiplication Factor**: Handles '#' values by defaulting to 1.0
3. **Type Casting**: Standardizes meter_id as STRING

**Output Columns**:
- `meter_id` (STRING)
- `multiplication_factor` (FLOAT64, default 1.0)
- `location` (GEOGRAPHY)
- `region`, `province`, `department_name`, `office_name` (STRING)

---

### 🔄 Intermediate Models (`dags/dbt/models/marts/`)

Intermediate models join and enrich data for analysis.

#### `int_meter_locations.sql`
**Purpose**: Match each meter to its nearest prayer time location.

**Logic**:
1. Cross join meters with all prayer locations
2. Calculate `ST_DISTANCE` between meter and prayer locations
3. Rank by distance and select nearest (`ROW_NUMBER() = 1`)

**Output Columns**:
- `meter_id`
- `multiplication_factor`
- `meter_location` (GEOGRAPHY)
- `region`, `province`
- `nearest_prayer_coordinate` - Coordinate string for joining with prayer times
- `distance_meters` - Distance to nearest prayer location

**Performance Note**: CROSS JOIN can be expensive. For 30k meters × 100 locations = 3M intermediate rows. Consider optimizing with spatial indexing for larger datasets.

---

#### `int_meter_readings_with_periods.sql`
**Purpose**: Enrich meter readings with prayer-based time periods.

**Logic**:
1. Join readings → meter locations (to get nearest prayer coordinate)
2. Join with prayer times on:
   - **Month & Day** (prayer times repeat yearly)
   - Nearest prayer coordinate
3. Calculate period boundaries:
   - **Morning**: Fajr + 100 min to Dhuhr - 80 min
   - **Evening**: Isha + 90 min to Fajr - 80 min (wraps midnight)

**Output Columns**:
- All reading columns from `stg_meter_readings`
- `fajr_time`, `dhuhr_time`, `isha_time`
- `morning_start_time`, `morning_end_time`
- `evening_start_time`, `evening_end_time`

**Key Join Logic**:
```sql
-- Match on month/day regardless of year (prayer times repeat)
on EXTRACT(MONTH FROM r.reading_date) = EXTRACT(MONTH FROM p.date)
   and EXTRACT(DAY FROM r.reading_date) = EXTRACT(DAY FROM p.date)
   and m.nearest_prayer_coordinate = p.original_coordinate
```

---

### 🎯 Marts Models (`dags/dbt/models/marts/`)

Marts are business-ready tables for reporting and analysis.

#### `consumption_analysis.sql`
**Purpose**: Final aggregated consumption metrics per meter with period analysis.

**Materialization**: `table` (for query performance)

**Aggregations**:

1. **Total Consumption** (all readings):
   - `total_avg_consumption` - Average power (W)
   - `total_sum_consumption` - Total power sum (W)
   - `total_reading_count` - Number of readings
   - `total_energy_kwh` - Total energy consumption
   - `total_cost_sar` - Total cost @ 0.32 SAR/kWh

2. **Morning Period** (excludes Fridays):
   - `morning_avg_consumption` - Average during morning periods
   - `morning_sum_consumption` - Sum during morning periods
   - `morning_reading_count` - Readings in morning period
   - `morning_energy_kwh` - Energy consumed
   - `morning_cost_sar` - Cost

3. **Evening Period** (wraps midnight):
   - `evening_avg_consumption` - Average during evening periods
   - `evening_sum_consumption` - Sum during evening periods
   - `evening_reading_count` - Readings in evening period
   - `evening_energy_kwh` - Energy consumed
   - `evening_cost_sar` - Cost

4. **Metadata**:
   - `min_reading_date`, `max_reading_date` - Date range
   - `readings_with_prayer_times` - Count of matched readings
   - `multiplication_factor`, `region`, `province` - Meter info
   - `data_quality_flag` - Quality indicator

**Data Quality Flag**:
- `COMPLETE` - Both morning & evening data (99.82%)
- `NO_MORNING_DATA` - Only evening data (0.15%)
- `NO_EVENING_DATA` - Only morning data (0.03%)

**Filtering**:
```sql
-- Only keep meters with at least one period of data
where morning_avg_consumption IS NOT NULL
   OR evening_avg_consumption IS NOT NULL
```
Removes ~1.3% of meters with no prayer time matches.

**Energy Calculation Formula**:
```sql
-- Readings are 30-min intervals
-- (Sum_Watts * Multiplication_Factor / 2) / 1000 = kWh
(total_sum_consumption * multiplication_factor / 2) / 1000 as total_energy_kwh
```

**Cost Calculation**:
```sql
-- Energy (kWh) × 0.32 SAR/kWh
(total_energy_kwh) * 0.32 as total_cost_sar
```

---

### 🔍 Understanding the Analysis

**Why exclude Fridays from morning analysis?**
Friday is Jumah (congregational prayer day) when mosque usage patterns differ significantly. Morning exclusion ensures typical weekday patterns aren't skewed.

**Why do periods wrap midnight?**
Evening period is "after Isha prayer until before Fajr prayer" which spans across midnight (e.g., 20:19 to 03:32 next day).

**What's the multiplication factor?**
Some meters have current transformers (CTs) that scale down readings. The multiplication factor (typically 1, 60, 120, or 160) scales readings back to actual consumption.

---

## 9. Data Quality & Metrics

After running the full pipeline, expect:

| Metric | Value |
|--------|-------|
| **Total meters** | ~28,000 |
| **Raw readings** | ~115M |
| **After deduplication** | ~46M (60% duplicates removed) |
| **Outliers filtered** | Any readings > 1GW or < 0 |
| **Prayer time match rate** | 98.7% |
| **Complete period data** | 99.82% |
| **Total energy analyzed** | ~25M kWh |
| **Total cost** | ~8M SAR |

---

## 10. Troubleshooting

### dbt Seed Issues

**Error**: `Compilation Error: 'prayer_times' was not found`
- **Fix**: Run `dbt seed` before running dbt models

**Error**: `Table already exists`
- **Fix**: Use `dbt seed --full-refresh` to drop and recreate seed tables

### dbt Auth Failure

**Error**: `Unable to generate access token`
- **Fix**: Ensure `quota_project_id` is **NOT** set in `include/profiles.yml` when using `gcp_adc.json`

### Schema Mismatch

**Error**: `Provided Schema does not match Table...`
- **Fix**: Drop the table in BigQuery (`DROP TABLE ...`) and let the pipeline recreate it with the correct schema

### Missing Credentials

**Error**: `DefaultCredentialsError: Could not automatically determine credentials.`
- **Fix**: Re-run the copy command in **Step 2** to ensure `include/gcp_adc.json` exists

### No Prayer Time Matches

**Error**: All consumption values are NULL
- **Fix**:
  1. Verify seeds loaded: `dbt show --select prayer_times`
  2. Check prayer times cover your meter reading dates
  3. Verify coordinate matching in `int_meter_locations`

### High Consumption Values

If seeing consumption > 100kW:
- Check multiplication factors in industry_codes.csv
- Verify outlier filtering is working (should cap at 1GW)
- Check for data entry errors in raw CSV files

---

## 11. Development Commands

### Local dbt Development (without Airflow):

```bash
# From project root:

# Load reference data
dbt seed --project-dir ./dags/dbt --profiles-dir ./include --target dev

# Run all models
dbt run --project-dir ./dags/dbt --profiles-dir ./include --target dev

# Run specific model and its dependencies
dbt run --select +consumption_analysis --project-dir ./dags/dbt --profiles-dir ./include --target dev

# Run tests
dbt test --project-dir ./dags/dbt --profiles-dir ./include --target dev

# Preview data
dbt show --select consumption_analysis --limit 10 --project-dir ./dags/dbt --profiles-dir ./include --target dev

# Generate documentation
dbt docs generate --project-dir ./dags/dbt --profiles-dir ./include --target dev
dbt docs serve
```

### Viewing Results in BigQuery:

```sql
-- Check consumption analysis
SELECT * FROM `raw_meter_readings.consumption_analysis`
WHERE data_quality_flag = 'COMPLETE'
ORDER BY total_energy_kwh DESC
LIMIT 10;

-- Regional summary
SELECT
    region,
    COUNT(*) as meter_count,
    ROUND(AVG(total_avg_consumption), 2) as avg_consumption_w,
    ROUND(SUM(total_energy_kwh), 2) as total_energy_kwh,
    ROUND(SUM(total_cost_sar), 2) as total_cost_sar
FROM `raw_meter_readings.consumption_analysis`
GROUP BY region
ORDER BY total_energy_kwh DESC;
```

---

## 12. Project Structure

```
.
├── dags/
│   ├── dbt/
│   │   ├── models/
│   │   │   ├── staging/           # Bronze → Silver transformations
│   │   │   │   ├── stg_meter_readings.sql
│   │   │   │   ├── stg_prayer_times.sql
│   │   │   │   └── stg_industry_codes.sql
│   │   │   └── marts/             # Silver → Gold analytics
│   │   │       ├── int_meter_locations.sql
│   │   │       ├── int_meter_readings_with_periods.sql
│   │   │       └── consumption_analysis.sql
│   │   ├── seeds/                 # Reference data CSVs
│   │   │   ├── prayer_times.csv
│   │   │   └── industry_codes.csv
│   │   ├── sources.yml            # Source table definitions
│   │   └── dbt_project.yml
│   └── dbt_pipeline.py            # Airflow DAG
├── include/
│   ├── etl_processor.py           # CSV → Parquet ETL
│   ├── cloud_loader.py            # Parquet → BigQuery
│   ├── gcp_adc.json              # GCP credentials (git-ignored)
│   ├── profiles.yml               # dbt BigQuery connection
│   └── raw_data/                  # Input CSV files
├── infra/                         # Terraform configs
├── .env                           # Environment variables
├── issues.md                      # Known issues and resolutions
└── README.md                      # This file
```

---

## 13. Next Steps

After completing the setup:

1. ✅ **Verify Data Quality**: Check `issues.md` for resolved and outstanding issues
2. 📊 **Build Dashboards**: Connect Looker/Tableau to `consumption_analysis` table
3. 🔔 **Set Up Alerts**: Configure Airflow email alerts for pipeline failures
4. 🧪 **Add dbt Tests**: Implement data quality tests (see `issues.md` recommendations)
5. 📈 **Optimize Performance**: Review CROSS JOIN in `int_meter_locations` for large datasets
6. 🔄 **Schedule Updates**: Add prayer times for new years, update meter metadata

---

## 14. Contributing

When making changes:
1. Update this README if adding new models or changing the pipeline
2. Document any new environment variables in `.env.example`
3. Run `dbt test` before committing
4. Update `issues.md` when fixing bugs or adding features

---

## License

[Your License Here]

## Contact

[Your Contact Information]
