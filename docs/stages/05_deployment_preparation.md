# Stage 5: Deployment Preparation

## Purpose
Package, test, and prepare for production deployment.

---

## Key Activities

### 5.1 Model Packaging

#### Analytics Pipeline (dbt)

**Project Structure**:
```
dags/dbt/
├── models/
│   ├── staging/
│   │   ├── stg_meter_readings.sql
│   │   ├── stg_prayer_times.sql
│   │   ├── stg_industry_codes.sql
│   │   └── int_exclude_ramadan.sql
│   └── marts/
│       ├── int_meter_locations.sql
│       ├── int_meter_quality.sql
│       ├── int_meter_readings_with_periods.sql
│       ├── consumption_analysis.sql
│       ├── violators.sql
│       └── schema.yml
├── seeds/
│   ├── prayer_times.csv
│   └── industry_codes.csv
├── sources.yml
└── dbt_project.yml
```

**dbt Package Manifest**:
```yaml
# dbt_project.yml
name: 'my_energy_project'
version: '1.0.0'
config-version: 2
profile: 'default'

models:
  my_energy_project:
    +materialized: view
```

#### Mosque Classifier (Illustrative)

**Model Serialization**:
```python
import joblib
from datetime import datetime

# Save model with version tag
version = datetime.now().strftime("%Y%m%d_%H%M%S")
model_path = f"models/mosque_classifier_v{version}.joblib"

joblib.dump(model, model_path)
print(f"Model saved to: {model_path}")

# Save feature list for validation
feature_list = X_train.columns.tolist()
joblib.dump(feature_list, f"models/features_v{version}.joblib")
```

**Model Artifacts**:
| Artifact | Path | Size |
|----------|------|------|
| Trained Model | `models/mosque_classifier_v*.joblib` | ~5MB |
| Feature List | `models/features_v*.joblib` | ~1KB |
| Scaler (if any) | `models/scaler_v*.joblib` | ~10KB |

### 5.2 Infrastructure

**Terraform Resources** (`infra/main.tf`):

```hcl
# GCS Bucket for data storage
resource "google_storage_bucket" "data_bucket" {
  name     = var.bucket_name
  location = var.region
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
}

# BigQuery Dataset
resource "google_bigquery_dataset" "meter_data" {
  dataset_id = "raw_meter_readings"
  project    = var.project_id
  location   = var.region
}
```

**Infrastructure Outputs**:
| Resource | Name | Purpose |
|----------|------|---------|
| GCS Bucket | `mosque-energy-data` | Store Parquet files |
| BigQuery Dataset | `raw_meter_readings` | Data warehouse |
| BigQuery Tables | `smart_meters_clean`, `consumption_analysis`, etc. | Analytics |

### 5.3 API/Interface

#### Airflow DAG

**Pipeline Orchestration** (`dags/dbt_pipeline.py`):
```python
from airflow.decorators import dag, task
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig

@dag(
    dag_id='meter_data_pipeline',
    schedule_interval='@daily',
    catchup=False
)
def pipeline():
    @task
    def etl_task():
        """Process CSV files to Parquet"""
        from include.etl_processor import process_files
        return process_files()

    @task
    def cloud_upload_task(etl_result):
        """Upload to GCS and load to BigQuery"""
        from include.cloud_loader import upload_and_load
        return upload_and_load(etl_result)

    dbt_tasks = DbtTaskGroup(
        project_config=ProjectConfig("dags/dbt"),
        profile_config=ProfileConfig(...)
    )

    etl_result = etl_task()
    upload_result = cloud_upload_task(etl_result)
    upload_result >> dbt_tasks

pipeline()
```

#### Streamlit Dashboard (Optional)

**Dashboard Features**:
- Violator list with filtering
- Regional consumption charts
- Potential savings calculator
- Data quality overview

**Connection**:
```python
from google.cloud import bigquery

client = bigquery.Client()
query = """
    SELECT * FROM `project.raw_meter_readings.violators`
    ORDER BY total_potential_savings_sar DESC
    LIMIT 100
"""
df = client.query(query).to_dataframe()
```

### 5.4 Monitoring Setup

**Pipeline Statistics Table**:
```sql
CREATE TABLE raw_meter_readings.pipeline_stats (
    run_id STRING,
    run_timestamp TIMESTAMP,
    source_filename STRING,
    quarter STRING,
    stage_name STRING,
    rows_input INT64,
    rows_output INT64,
    rows_filtered INT64,
    filter_reason STRING,
    unique_meters INT64,
    processing_seconds FLOAT64,
    status STRING,
    error_message STRING
);
```

**Monitoring Queries**:
```sql
-- Latest pipeline run status
SELECT
    stage_name,
    status,
    rows_input,
    rows_output,
    processing_seconds
FROM `raw_meter_readings.pipeline_stats`
WHERE run_id = (SELECT MAX(run_id) FROM `raw_meter_readings.pipeline_stats`)
ORDER BY run_timestamp;

-- Processing time trends
SELECT
    DATE(run_timestamp) as run_date,
    AVG(processing_seconds) as avg_processing_seconds
FROM `raw_meter_readings.pipeline_stats`
WHERE stage_name = 'etl_processing'
GROUP BY run_date
ORDER BY run_date DESC
LIMIT 30;
```

**Model Drift Monitoring** (Classifier):
```python
def monitor_predictions(new_predictions, historical_baseline):
    """
    Monitor for distribution drift in predictions.
    """
    new_positive_rate = new_predictions.mean()
    baseline_rate = historical_baseline

    drift = abs(new_positive_rate - baseline_rate)

    if drift > 0.05:  # 5% threshold
        alert(f"Prediction drift detected: {drift:.2%}")

    return {
        "new_positive_rate": new_positive_rate,
        "baseline_rate": baseline_rate,
        "drift": drift
    }
```

### 5.5 Documentation

**Documentation Artifacts**:

| Document | Location | Purpose |
|----------|----------|---------|
| README.md | `/README.md` | Setup and usage guide |
| issues.md | `/issues.md` | Known issues and resolutions |
| streamlit_to_bigquery.md | `/docs/streamlit_to_bigquery.md` | Dashboard setup |
| Stage docs | `/docs/stages/*.md` | ML lifecycle documentation |

**API Documentation** (if applicable):
```yaml
# Prediction endpoint (hypothetical)
POST /api/v1/classify
Content-Type: application/json

Request:
{
  "meter_id": "MTR001",
  "morning_avg_consumption": 2500.5,
  "evening_avg_consumption": 1800.2,
  "friday_consumption_ratio": 1.45,
  "daily_variance": 350.0,
  "evening_to_morning_ratio": 0.72,
  "weekend_pattern": 1.02
}

Response:
{
  "meter_id": "MTR001",
  "prediction": "mosque",
  "confidence": 0.94,
  "model_version": "v20250115_120000"
}
```

### 5.6 Staging Tests

**Astronomer Dev Environment**:
```bash
# Start local Airflow
astro dev start

# Run pipeline manually
# Access UI at http://localhost:8080
# Trigger DAG: meter_data_pipeline
```

**Test Checklist**:
- [x] ETL processes sample files correctly
- [x] Parquet files upload to GCS
- [x] BigQuery MERGE handles duplicates
- [x] dbt models build without errors
- [x] Incremental logic works correctly
- [x] Quality filtering applies correctly
- [x] Statistics are recorded

**Sample Test Data**:
```bash
# Create test dataset
head -1000 include/raw_data/large_file.csv > include/raw_data/test_sample.csv
```

### 5.7 Rollback Plan

**Model Versioning**:
```python
# Models are versioned with timestamps
models/
├── mosque_classifier_v20250110_090000.joblib  # Previous
├── mosque_classifier_v20250115_120000.joblib  # Current
└── mosque_classifier_latest.joblib -> v20250115_120000.joblib
```

**Rollback Procedure**:
1. **Identify Issue**: Monitor alerts or manual detection
2. **Verify**: Confirm issue with test queries
3. **Rollback Model**: Update symlink to previous version
4. **Rollback Data**: dbt incremental models preserve history

```bash
# Rollback to previous model version
ln -sf mosque_classifier_v20250110_090000.joblib mosque_classifier_latest.joblib

# For dbt models, full refresh from previous state
dbt run --full-refresh --select consumption_analysis
```

**dbt Incremental Rollback**:
```sql
-- Delete recent data and reprocess
DELETE FROM raw_meter_readings.consumption_analysis
WHERE max_reading_date >= '2025-01-15';

-- Re-run dbt
-- dbt run --select consumption_analysis
```

---

## Deliverables

- [x] Model serialized and versioned
- [x] dbt project packaged
- [x] Infrastructure provisioned (Terraform)
- [x] Airflow DAG configured
- [x] Monitoring queries created
- [x] Documentation complete
- [x] Staging tests passed
- [x] Rollback plan documented

---

## Who's Involved

| Role | Involvement |
|------|-------------|
| ML Engineer | Model packaging, versioning |
| Data Engineer | dbt packaging, Airflow DAG |
| DevOps/Platform | Infrastructure, monitoring |
| QA | Staging tests |

---

## Gate: Staging Approval

**Decision**: Approve for Production Deployment

**Staging Test Results**:

| Test | Status |
|------|--------|
| ETL Processing | Pass |
| Cloud Upload | Pass |
| dbt Build | Pass |
| Data Quality | Pass |
| Monitoring | Pass |

**Infrastructure Checklist**:
- [x] GCS bucket created
- [x] BigQuery dataset created
- [x] Airflow running on Astronomer
- [x] Credentials configured

**Sign-off**: Ready for production deployment
