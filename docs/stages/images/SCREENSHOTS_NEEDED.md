# Screenshots You Need to Capture

The following screenshots need to be captured manually from your systems.
Save them in this `images/` directory with the suggested filenames.

---

## Stage 2: Data Preparation

### 1. BigQuery Query Results - Duplicate Analysis
**File:** `bq_duplicate_analysis.png`
**Source:** BigQuery Console
**Query to run:**
```sql
SELECT
    COUNT(*) as total_rows,
    COUNT(DISTINCT ROW_HASH) as unique_rows,
    ROUND((COUNT(*) - COUNT(DISTINCT ROW_HASH)) / COUNT(*) * 100, 2) as duplicate_pct
FROM raw_meter_readings.smart_meters_clean;
```
**How:** Run query → Screenshot the results panel

---

## Stage 3: Model Development

### 2. Airflow DAG Graph View
**File:** `airflow_dag_graph.png`
**Source:** Airflow UI
**How:**
1. Go to http://localhost:8080
2. Click on `meter_data_pipeline` DAG
3. Click "Graph" tab
4. Screenshot the full DAG graph

---

## Stage 5: Deployment Preparation

### 3. GCP Resources View
**File:** `gcp_resources.png`
**Source:** GCP Console
**How:**
1. Go to https://console.cloud.google.com
2. Navigate to BigQuery → Your dataset
3. Screenshot showing tables: `smart_meters_clean`, `consumption_analysis`, `violators`

### 4. Airflow Success View
**File:** `airflow_success.png`
**Source:** Airflow UI
**How:**
1. Go to http://localhost:8080
2. Click on a successful DAG run
3. Screenshot Grid view with all green (success) tasks

---

## Stage 6: Production Deployment

### 5. BigQuery Quarter Verification
**File:** `bq_quarter_verification.png`
**Source:** BigQuery Console
**Query to run:**
```sql
SELECT
    quarter,
    COUNT(*) as meter_count,
    MAX(max_reading_date) as latest_date
FROM `raw_meter_readings.consumption_analysis`
GROUP BY quarter
ORDER BY quarter DESC;
```
**How:** Run query → Screenshot the results

### 6. Docker Stats Output
**File:** `docker_stats.png`
**Source:** Terminal
**Command:**
```bash
docker stats --no-stream
```
**How:** Run during pipeline execution → Screenshot the output

---

## Stage 7: Monitoring & Maintenance

### 7. Airflow Dashboard
**File:** `airflow_dashboard.png`
**Source:** Airflow UI
**How:**
1. Go to http://localhost:8080
2. Screenshot the main DAGs list showing `meter_data_pipeline` status

### 8. Streamlit Dashboard (Optional)
**File:** `streamlit_dashboard.png`
**Source:** Your Streamlit app (if configured)
**How:** Screenshot your dashboard showing violator data

---

## Quick Checklist

| # | Screenshot | Source | Priority |
|---|-----------|--------|----------|
| 1 | bq_duplicate_analysis.png | BigQuery | Medium |
| 2 | airflow_dag_graph.png | Airflow UI | High |
| 3 | gcp_resources.png | GCP Console | High |
| 4 | airflow_success.png | Airflow UI | High |
| 5 | bq_quarter_verification.png | BigQuery | High |
| 6 | docker_stats.png | Terminal | Medium |
| 7 | airflow_dashboard.png | Airflow UI | High |
| 8 | streamlit_dashboard.png | Streamlit | Optional |

---

## After Capturing Screenshots

Once you've captured the screenshots, add them to the markdown files:

### Stage 2 (`02_data_preparation.md`)
```markdown
![BigQuery Duplicate Analysis](images/bq_duplicate_analysis.png)
```

### Stage 3 (`03_model_development.md`)
```markdown
![Airflow DAG Graph](images/airflow_dag_graph.png)
```

### Stage 5 (`05_deployment_preparation.md`)
```markdown
![GCP Resources](images/gcp_resources.png)
![Airflow Success](images/airflow_success.png)
```

### Stage 6 (`06_production_deployment.md`)
```markdown
![BigQuery Verification](images/bq_quarter_verification.png)
![Docker Stats](images/docker_stats.png)
```

### Stage 7 (`07_monitoring_maintenance.md`)
```markdown
![Airflow Dashboard](images/airflow_dashboard.png)
![Streamlit Dashboard](images/streamlit_dashboard.png)
```
