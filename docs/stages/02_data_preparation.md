# Stage 2: Data Preparation

## Purpose
Collect, explore, clean, and prepare data for model development and analytics.

---

## Key Activities

### 2.1 Data Sources

| Source | Format | Records | Size | Description |
|--------|--------|---------|------|-------------|
| Smart Meter Readings | CSV | ~115M | ~12GB | Raw meter consumption data |
| Prayer Times | CSV (seed) | ~36,500 | ~2MB | Daily prayer schedules by location |
| Industry Codes | CSV (seed) | ~28,000 | ~3MB | Meter metadata with locations |

**Data Schema - Smart Meter Readings**:
```
METER_ID          STRING    Unique meter identifier
DATA_TIME         TIMESTAMP Reading timestamp
IMPORT_ACTIVE_POWER FLOAT64  Active power in watts
ROW_HASH          STRING    Deduplication hash (MD5)
```

**Data Schema - Prayer Times**:
```
Coordinate        STRING    "(lat, lon)" format
Date              STRING    "DD-MM-YYYY" format
Fajr              STRING    "HH:MM" prayer time
Dhuhr             STRING    "HH:MM" prayer time
Asr               STRING    "HH:MM" prayer time
Maghrib           STRING    "HH:MM" prayer time
Isha              STRING    "HH:MM" prayer time
```

**Data Schema - Industry Codes**:
```
Meter Number          STRING    Meter identifier
Multiplication Factor FLOAT64   Power scaling factor
X Coordinates         FLOAT64   Longitude
Y Coordinates         FLOAT64   Latitude
Region                STRING    Geographic region
Province              STRING    Province name
```

### 2.2 Data Exploration

**Raw Data Quality Assessment**:

| Issue | Count | Percentage | Impact |
|-------|-------|------------|--------|
| Duplicate rows | ~69M | ~60% | Inflated aggregations |
| Outlier readings (>1GW) | ~1,000 | <0.01% | Skewed averages |
| Negative readings | ~500 | <0.01% | Invalid data |
| Missing multiplication factors | ~2,000 | ~7% | Underestimated consumption |
| No prayer time match | ~400 | ~1.3% | Excluded from analysis |

**Exploratory Queries**:
```sql
-- Check duplicate rate
SELECT
    COUNT(*) as total_rows,
    COUNT(DISTINCT ROW_HASH) as unique_rows,
    ROUND((COUNT(*) - COUNT(DISTINCT ROW_HASH)) / COUNT(*) * 100, 2) as duplicate_pct
FROM raw_meter_readings.smart_meters_clean;

-- Check reading distribution
SELECT
    CASE
        WHEN IMPORT_ACTIVE_POWER > 1000000000 THEN '>1GW (outlier)'
        WHEN IMPORT_ACTIVE_POWER < 0 THEN 'Negative (invalid)'
        WHEN IMPORT_ACTIVE_POWER = 0 THEN 'Zero'
        ELSE 'Valid'
    END as category,
    COUNT(*) as count
FROM raw_meter_readings.smart_meters_clean
GROUP BY 1;
```

### 2.3 Data Cleaning

**Cleaning Pipeline** (`etl_processor.py`):

```python
# 1. Deduplication using ROW_HASH
df = df.unique(subset=['ROW_HASH'])

# 2. Outlier filtering
df = df.with_columns([
    pl.when(pl.col('IMPORT_ACTIVE_POWER') > 1_000_000_000)
      .then(None)
      .when(pl.col('IMPORT_ACTIVE_POWER') < 0)
      .then(None)
      .otherwise(pl.col('IMPORT_ACTIVE_POWER'))
      .round(3)
      .alias('IMPORT_ACTIVE_POWER')
])

# 3. Timestamp parsing with microsecond support
df = df.with_columns([
    pl.col('DATA_TIME').str.to_datetime(
        format='%Y-%m-%dT%H:%M:%S%.f',  # Handles microseconds
        time_zone='UTC'
    )
])
```

**Data Quality After Cleaning**:

| Metric | Before | After | Change |
|--------|--------|-------|--------|
| Total Rows | ~115M | ~46M | -60% (duplicates removed) |
| Outliers (>1GW) | ~1,000 | 0 | Set to NULL |
| Negative Values | ~500 | 0 | Set to NULL |
| Precision | Variable | 3 decimals | Standardized |

### 2.4 Data Labeling

**Prayer Period Classification**:

The pipeline classifies each reading into prayer-based periods:

```sql
-- Morning Period: Fajr + 100 min to Dhuhr - 80 min (excludes Fridays)
morning_start_time = TIME_ADD(fajr_time, INTERVAL 100 MINUTE)
morning_end_time = TIME_SUB(dhuhr_time, INTERVAL 80 MINUTE)

-- Evening Period: Isha + 90 min to Fajr - 80 min (wraps midnight)
evening_start_time = TIME_ADD(isha_time, INTERVAL 90 MINUTE)
evening_end_time = TIME_SUB(fajr_time, INTERVAL 80 MINUTE)
```

**Period Breakdown**:
| Period | Definition | Friday Excluded |
|--------|------------|-----------------|
| Morning | Fajr+100min to Dhuhr-80min | Yes |
| Evening | Isha+90min to Fajr-80min | No |

**Facility Type Labeling** (for Mosque Classifier):

| Label | Source | Count |
|-------|--------|-------|
| Mosque (1) | Industry code = mosque-related | ~28,000 |
| Non-Mosque (0) | Other facility types | Hypothetical |

> **Note**: For the illustrative classifier, we assume facility type labels are derived from the industry_codes data.

### 2.5 Train/Validation/Test Split

**For Mosque Classifier** (Illustrative):

| Split | Percentage | Records | Purpose |
|-------|------------|---------|---------|
| Training | 70% | ~19,600 | Model training |
| Validation | 15% | ~4,200 | Hyperparameter tuning |
| Test | 15% | ~4,200 | Final evaluation |

**Splitting Strategy**:
```python
from sklearn.model_selection import train_test_split

# Stratified split to maintain class balance
X_train, X_temp, y_train, y_temp = train_test_split(
    X, y, test_size=0.30, random_state=42, stratify=y
)
X_val, X_test, y_val, y_test = train_test_split(
    X_temp, y_temp, test_size=0.50, random_state=42, stratify=y_temp
)
```

### 2.6 License Verification

| Data Source | License | Status |
|-------------|---------|--------|
| Smart Meter Data | Internal/Proprietary | Authorized |
| Prayer Times | Public domain (Islamic calendar) | Clear |
| Industry Codes | Internal/Proprietary | Authorized |

---

## Feature Engineering Preview

**Features for Mosque Classifier**:

| Feature | Type | Derivation |
|---------|------|------------|
| `morning_avg_consumption` | Numeric | AVG(power) during morning period |
| `evening_avg_consumption` | Numeric | AVG(power) during evening period |
| `friday_consumption_ratio` | Numeric | Friday AVG / Weekday AVG |
| `daily_variance` | Numeric | STDDEV(daily consumption) |
| `evening_to_morning_ratio` | Numeric | evening_avg / morning_avg |
| `weekend_pattern` | Numeric | Weekend AVG / Weekday AVG |

---

## Deliverables

- [x] Data sources documented with schemas
- [x] Data exploration completed with quality assessment
- [x] Data cleaning pipeline implemented
- [x] Data labeling logic defined
- [x] Train/Val/Test split defined (for classifier)
- [x] License verification completed

---

## Who's Involved

| Role | Involvement |
|------|-------------|
| Data Engineer | ETL pipeline development |
| Data Scientist | Exploration, feature engineering |
| Domain Expert | Labeling logic, business rules |

---

## Gate: Data Quality Approval

**Decision**: Proceed to Model Development

**Data Quality Summary**:

| Metric | Value | Status |
|--------|-------|--------|
| Duplicate Removal | 60% removed | Pass |
| Outlier Handling | Set to NULL | Pass |
| Prayer Time Match | 98.7% | Pass |
| Minimum Quality Score | 50% threshold | Pass |

**Sign-off**: Data preparation complete, quality meets requirements.

---

## Appendix: ETL Statistics

**Sample Pipeline Statistics**:

```json
{
  "run_id": "manual__2025-01-15T10:30:00",
  "stage_name": "etl_processing",
  "rows_input": 115234567,
  "rows_output": 46093827,
  "rows_filtered": 69140740,
  "filter_reason": "duplicates",
  "unique_meters": 28456,
  "processing_seconds": 245.7,
  "status": "success"
}
```

**Quality Score Distribution**:

| Quality Range | Meter Count | Percentage |
|---------------|-------------|------------|
| 90-100% | 18,500 | 65% |
| 70-90% | 5,700 | 20% |
| 50-70% | 2,850 | 10% |
| <50% (excluded) | 1,425 | 5% |
