# BigQuery Integration Summary for Mosque Dashboard

## Overview

This document summarizes the changes made to integrate the Streamlit dashboard (`mosques-dashboard.v2/`) with BigQuery to load data from the dbt pipeline tables (`violators`, `industry_codes`) instead of local Excel/Parquet files.

The integration supports both:
- **Local development**: Uses local Excel/Parquet files
- **Cloud deployment**: Uses BigQuery tables

---

## Files Changed

### 1. NEW: `mosques/data/bigquery_client.py`

A new module that handles all BigQuery interactions:
- Connects to BigQuery using Google Cloud credentials
- Queries `violators` and `industry_codes` tables
- Maps BigQuery column names to Arabic UI column names
- Handles quarter format conversion (e.g., `2025-Q3` ↔ `الربع الثالث 2025`)
- Uses Streamlit caching (`@st.cache_data(ttl=3600)`) to minimize query costs

### 2. MODIFIED: `mosques/config.py`

Added BigQuery configuration variables:

```python
# ===== BigQuery Configuration =====

USE_BIGQUERY = os.environ.get("USE_BIGQUERY", "false").lower() == "true"
BQ_PROJECT_ID = os.environ.get("BQ_PROJECT_ID", "")
BQ_DATASET_ID = os.environ.get("BQ_DATASET_ID", "raw_meter_readings")
```

Also updated path resolution to gracefully fall back when BigQuery mode is enabled and local paths don't exist.

### 3. MODIFIED: `mosques/data/loaders.py`

Added conditional logic to three key functions:

```python
def load_industry_meta(...):
    if USE_BIGQUERY:
        from data.bigquery_client import load_industry_codes_from_bq
        return load_industry_codes_from_bq(meter_id=meter_id)
    # ... existing local file logic

def load_all_violator_data(...):
    if USE_BIGQUERY:
        from data.bigquery_client import load_all_violators_by_quarter_from_bq
        return load_all_violators_by_quarter_from_bq(meter_id=meter_id)
    # ... existing local file logic

def load_single_quarter_data(...):
    if USE_BIGQUERY:
        from data.bigquery_client import load_violators_from_bq, QUARTER_AR_TO_BQ
        # ... BigQuery loading
    # ... existing local file logic
```

### 4. MODIFIED: `mosques/requirements.txt`

Added BigQuery dependencies:

```
google-cloud-bigquery==3.25.0
db-dtypes==1.3.0
pandas-gbq==0.23.1
```

### 5. MODIFIED: `mosques/env.example`

Added documentation for BigQuery environment variables:

```env
# ===== BigQuery Configuration (for cloud deployment) =====

USE_BIGQUERY=false
BQ_PROJECT_ID=your-gcp-project-id
BQ_DATASET_ID=raw_meter_readings
```

### 6. MODIFIED: `.gitignore`

Added `.venv/` for the uv-managed virtual environment.

---

## Configuration

### For Local Development (default)

```env
USE_BIGQUERY=false
# Uses local Excel/Parquet files
```

### For Cloud Deployment (Cloud Run)

```env
USE_BIGQUERY=true
BQ_PROJECT_ID=your-gcp-project-id
BQ_DATASET_ID=raw_meter_readings
```

---

## BigQuery Tables Required

The dashboard reads from these tables in the `raw_meter_readings` dataset:

| Table | Source | Description |
|-------|--------|-------------|
| `violators` | dbt model | Meters exceeding 3000W threshold |
| `industry_codes` | dbt seed | Meter metadata (location, province, etc.) |

### `violators` Table Schema (from dbt model)

| Column | Type | Description |
|--------|------|-------------|
| `meter_id` | STRING | Meter identifier |
| `quarter` | STRING | Quarter (e.g., "2025-Q3") |
| `province` | STRING | Province name |
| `region` | STRING | Region name |
| `morning_avg_mf` | FLOAT | Morning avg consumption × multiplication factor |
| `evening_avg_mf` | FLOAT | Evening avg consumption × multiplication factor |
| `over_in_morning` | BOOLEAN | Exceeds 3000W in morning |
| `over_in_evening` | BOOLEAN | Exceeds 3000W in evening |
| `over_in_both` | BOOLEAN | Exceeds in both periods |
| `over_in_either` | BOOLEAN | Exceeds in at least one period |
| `violation_category` | STRING | BOTH_PERIODS, MORNING_ONLY, EVENING_ONLY, COMPLIANT |
| `total_cost_sar` | FLOAT | Total energy cost |
| `total_potential_savings_sar` | FLOAT | Potential savings if reduced to 500W |

### `industry_codes` Table Schema (from dbt seed)

| Column | Type | Description |
|--------|------|-------------|
| `Meter Number` | STRING | Meter identifier |
| `Province` | STRING | Province name |
| `Region` | STRING | Region name |
| `X Coordinates` | FLOAT | Longitude |
| `Y Coordinates` | FLOAT | Latitude |
| `Multiplication Factor` | STRING | Scaling factor for readings |
| `Department Name` | STRING | Administrative department |
| `Office Name` | STRING | Managing office |

---

## Quarter Mapping

The dashboard uses Arabic quarter names, while BigQuery uses standard format:

| BigQuery Format | Arabic UI Format |
|-----------------|------------------|
| `2024-Q4` | `الربع الرابع 2024` |
| `2025-Q1` | `الربع الأول 2025` |
| `2025-Q2` | `الربع الثاني 2025` |
| `2025-Q3` | `الربع الثالث 2025` |
| `2025-Q4` | `الربع الرابع 2025` |

---

## Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    Local Development                         │
│  Excel/Parquet Files → loaders.py → Streamlit UI            │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                    Cloud Deployment                          │
│  BigQuery Tables → bigquery_client.py → loaders.py → UI     │
│                                                              │
│  Tables:                                                     │
│  - raw_meter_readings.violators (from dbt model)            │
│  - raw_meter_readings.industry_codes (from dbt seed)        │
└─────────────────────────────────────────────────────────────┘
```

---

## Testing Locally with BigQuery

### 1. Install Dependencies

```bash
cd mosques-dashboard.v2
uv venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
uv pip install -r mosques/requirements.txt
```

### 2. Authenticate with GCP

```bash
gcloud auth application-default login
```

### 3. Configure Environment

```bash
cd mosques
cp env.example .env
```

Edit `.env`:
```env
USE_BIGQUERY=true
BQ_PROJECT_ID=your-project-id
BQ_DATASET_ID=raw_meter_readings
# Comment out MOSQUES_DATA_DIR if it points to a non-existent path
# MOSQUES_DATA_DIR=...
```

### 4. Run the App

```bash
streamlit run app.py
```

---

## Performance

| Data Source | Rows | Load Time |
|-------------|------|-----------|
| Regions (local GeoJSON) | 13 | ~5s |
| Industry Codes (BigQuery) | 76,225 | ~20s |
| Violators (BigQuery) | 13,879 | ~5s |

- **First load**: ~30-40 seconds
- **Subsequent loads**: Instant (Streamlit cache with 1-hour TTL)

---

## Cloud Run Deployment

When deploying to Cloud Run:

1. **Service Account Permissions**: Ensure the Cloud Run service account has:
   - `BigQuery Data Viewer`
   - `BigQuery Job User`

2. **Environment Variables**: Set in Cloud Run:
   ```
   USE_BIGQUERY=true
   BQ_PROJECT_ID=your-project-id
   BQ_DATASET_ID=raw_meter_readings
   ```

3. **Authentication**: Automatic via service account (no credentials file needed)

---

## Files to Copy

To update the original project, copy these files:

1. **New file**: `mosques/data/bigquery_client.py`
2. **Modified**: `mosques/config.py`
3. **Modified**: `mosques/data/loaders.py`
4. **Modified**: `mosques/requirements.txt`
5. **Modified**: `mosques/env.example`
6. **Modified**: `.gitignore`

---

## Integration with dbt Pipeline

This dashboard now reads from the same BigQuery tables populated by the dbt pipeline:

```
Airflow DAG
    │
    ▼
ETL (CSV → Parquet → BigQuery)
    │
    ▼
dbt Models
    ├── staging/stg_meter_readings
    ├── staging/stg_industry_codes (seed)
    ├── marts/consumption_analysis
    └── marts/violators  ◄── Dashboard reads this
    │
    ▼
Streamlit Dashboard (mosques-dashboard.v2)
```

The dashboard automatically picks up new data as dbt models are refreshed.

