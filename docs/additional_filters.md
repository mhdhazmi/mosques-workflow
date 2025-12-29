# Additional Filters from Notebook Analysis

This document describes the additional data filters applied in the notebook (`52. To build cloud pipeline- 2025-Q3.ipynb`) and their implementation status in the dbt models.

## Implementation Status Overview

| Filter | Status | Model | Notes |
|--------|--------|-------|-------|
| Quality Filter (>50%) | ✅ **IMPLEMENTED** | `int_meter_quality`, `violators` | Removes 7.5% bad meters |
| Over-consumer Flagging | ✅ **IMPLEMENTED** | `violators` | Flags >3000W consumption |
| Category Aggregation | ✅ **IMPLEMENTED** | `violators` | Calculates potential savings |
| Quarter Filtering | ❌ Not implemented | - | Can be done in queries |
| Ramadan Removal | ❌ Not implemented | - | Optional filter |
| Region Filtering | ❌ Not implemented | - | Can be done in queries |

---

## 1. Filter by Quarter (Step 7)

**Purpose:** Extract data for a specific quarter only

**Notebook Code:**
```python
def get_quarter_data(df, year=2025, quarter=3):
    start_month = (quarter - 1) * 3 + 1
    end_month = quarter * 3 + 1

    start_date = pd.Timestamp(f"{year}-{start_month:02d}-01")
    if end_month == 13:
        end_date = pd.Timestamp(f"{year+1}-01-01")
    else:
        end_date = pd.Timestamp(f"{year}-{end_month:02d}-01")

    filtered_data = df[(df['READING_DATETIME'] >= start_date) & (df['READING_DATETIME'] < end_date)]
    return filtered_data
```

**Current Status:** ❌ Not implemented
**Note:** QUARTER column exists but no filtering by quarter happens

**SQL Equivalent (if needed):**
```sql
WHERE reading_date >= '2025-04-01'
  AND reading_date < '2025-07-01'
```

---

## 2. Remove Ramadan Period (Step 8)

**Purpose:** Exclude Ramadan month from analysis (consumption patterns differ significantly)

**Notebook Code:**
```python
# Ramadan was in 2025 Q1, so for Q3 this step doesn't apply
# But for other quarters:
year = 2025
start_month = 4  # After Ramadan
end_month = 7

start_date = pd.Timestamp(f"{year}-{start_month:02d}-01")
end_date = pd.Timestamp(f"{year}-{end_month:02d}-01")

df = df[(df['READING_DATETIME'] >= start_date) & (df['READING_DATETIME'] < end_date)]
```

**Current Status:** ❌ Not implemented

**SQL Equivalent:**
```sql
-- Example: Remove Ramadan 2024 (March 11 - April 9)
WHERE NOT (reading_date BETWEEN '2024-03-11' AND '2024-04-09')

-- Ramadan dates vary by year (lunar calendar)
-- 2025: March 1 - March 29
-- 2024: March 11 - April 9
-- 2023: March 23 - April 21
```

**Implementation Notes:**
- Ramadan dates change yearly (lunar calendar)
- Would need a lookup table or hardcoded dates per year
- Significant impact: mosque usage is 2-3x higher during Ramadan

---

## 3. Filter by Region - Riyadh Only (Step 9)

**Purpose:** Keep only meters in Riyadh region

**Notebook Code:**
```python
# Load Riyadh meter list
dm = pd.read_csv('Riyadh_Region_meters_list.csv')

# Filter to keep only Riyadh meters
df = df[df['METER_ID'].isin(dm['Meter Number'])]

# Count: 14,094 meters in Riyadh before quality filter
```

**Current Status:** ❌ Not implemented
**Current Behavior:** Includes ALL regions (Western, Southern, etc.)

**SQL Equivalent:**
```sql
WHERE region = 'الرياض'  -- Arabic for 'Riyadh'
-- OR use English region name from industry_codes table
WHERE region = 'Central' -- If using English names
```

**Impact:**
- Notebook: 14,094 Riyadh meters
- Current pipeline: 28,035 meters (all regions)
- **Result difference: ~2x meters**

---

## 4. Remove Bad Meters - Quality Filter (Step 10) ✅ IMPLEMENTED

**Purpose:** Remove meters with >50% missing or zero readings

**Implementation Status:** ✅ **IMPLEMENTED** in `int_meter_quality` and `violators` models

**Current Results (2022-Q4 data):**
- Total meters: 28,376
- Good quality (>50%): 26,255 (92.5%)
- Bad quality (removed): 2,121 (7.5%)
- Average quality: 95.55%

**Notebook Code:**
```python
# Calculate missing/zero percentage per meter
missing_report = mq.calculate_percentage_of_missing_and_zero_values(
    df,
    meter_col='METER_ID',
    date_col='READING_DATETIME',
    value_col='ACTIVE_IMP_POWER'
)

# Remove meters with >50% missing/zero
df = mq.remove_high_missing_meters(df, missing_report, threshold=50)
```

**Function Logic (from mosq_functions.py):**
```python
def calculate_percentage_of_missing_and_zero_values(df, meter_col, date_col, value_col):
    """
    For each meter, calculate:
    - Expected readings (based on date range, 30-min intervals = 48 readings/day)
    - Actual readings count
    - Zero readings count
    - Missing readings count
    - Percentage of missing+zero
    """
    results = []
    for meter_id in df[meter_col].unique():
        meter_data = df[df[meter_col] == meter_id]

        # Calculate expected readings
        date_range = (meter_data[date_col].max() - meter_data[date_col].min()).days
        expected_readings = date_range * 48  # 48 readings per day (30-min intervals)

        actual_readings = len(meter_data)
        zero_readings = (meter_data[value_col] == 0).sum()
        missing_readings = expected_readings - actual_readings

        quality_pct = ((missing_readings + zero_readings) / expected_readings) * 100

        results.append({
            'meter_id': meter_id,
            'expected_readings': expected_readings,
            'actual_readings': actual_readings,
            'zero_readings': zero_readings,
            'missing_readings': missing_readings,
            'quality_percentage': 100 - quality_pct
        })

    return pd.DataFrame(results)

def remove_high_missing_meters(df, missing_report, threshold=50):
    """Remove meters with quality < threshold%"""
    good_meters = missing_report[missing_report['quality_percentage'] >= threshold]['meter_id']
    return df[df['METER_ID'].isin(good_meters)]
```

**Current Status:** ✅ **IMPLEMENTED**

**dbt Implementation:** See `dags/dbt/models/marts/int_meter_quality.sql`

**SQL Implementation:**
```sql
-- Step 1: Calculate quality per meter
WITH meter_quality AS (
    SELECT
        meter_id,
        MIN(reading_date) as min_date,
        MAX(reading_date) as max_date,
        DATE_DIFF(MAX(reading_date), MIN(reading_date), DAY) as date_range_days,
        DATE_DIFF(MAX(reading_date), MIN(reading_date), DAY) * 48 as expected_readings,
        COUNT(*) as actual_readings,
        SUM(CASE WHEN active_power_watts = 0 THEN 1 ELSE 0 END) as zero_readings,
        -- Quality % = (actual - zero) / expected * 100
        ((COUNT(*) - SUM(CASE WHEN active_power_watts = 0 THEN 1 ELSE 0 END)) /
         (DATE_DIFF(MAX(reading_date), MIN(reading_date), DAY) * 48.0)) * 100 as quality_pct
    FROM stg_meter_readings
    GROUP BY meter_id
)
-- Step 2: Filter out bad meters
SELECT *
FROM consumption_analysis ca
WHERE meter_id IN (
    SELECT meter_id
    FROM meter_quality
    WHERE quality_pct >= 50
)
```

**Impact:**
- Notebook: 14,094 → 12,673 meters (removed 1,421 bad meters = 10%)
- Current: All 28,035 meters kept
- **Quality improvement: ~10% cleaner data**

---

## 5. Flag "Over Consumers" (Step 16) ✅ IMPLEMENTED

**Purpose:** Identify meters consuming >3000W during prayer periods (indicates lights left on)

**Implementation Status:** ✅ **IMPLEMENTED** in `violators` model

**Current Results (2022-Q4 data):**
- Total meters (after quality filter): 25,926
- Total violators: 2,830 (10.9%)
- Morning only: 206 (7.3% of violators)
- Evening only: 1,282 (45.3% of violators)
- Both periods: 1,342 (47.4% of violators)
- Potential savings: **1.28 million SAR**

**Notebook Code:**
```python
def Quarter_Report(merged_df, threshold=3000):
    """
    Flag meters that consume over threshold watts in morning/evening periods

    Returns stats on:
    - overs in morning
    - overs in evening
    - overs in both
    - overs in either
    """
    merged_df['over_in_Morning'] = merged_df['Morning_Avg_Consume_MF'] > threshold
    merged_df['over_in_Evening'] = merged_df['Evening_Avg_Consume_MF'] > threshold

    overs_morning = merged_df['over_in_Morning'].sum()
    overs_evening = merged_df['over_in_Evening'].sum()
    overs_both = (merged_df['over_in_Morning'] & merged_df['over_in_Evening']).sum()
    overs_either = (merged_df['over_in_Morning'] | merged_df['over_in_Evening']).sum()

    print(f'overs in morning: {overs_morning}')
    print(f'overs in evening: {overs_evening}')
    print(f'overs in both: {overs_both}')
    print(f'overs in either: {overs_either}')

    return merged_df
```

**Notebook Results (2025-Q3):**
```
overs in morning: 6,591
overs in evening: 7,027
overs in both: 5,959
overs in either: 7,659
```

**Current Status:** ✅ **IMPLEMENTED**

**dbt Implementation:** See `dags/dbt/models/marts/violators.sql`

**SQL Implementation:**
```sql
-- Implemented in violators model
CASE
    WHEN morning_avg_consumption * multiplication_factor > 3000 THEN TRUE
    ELSE FALSE
END as over_in_morning,

CASE
    WHEN evening_avg_consumption * multiplication_factor > 3000 THEN TRUE
    ELSE FALSE
END as over_in_evening,

-- Combined categories
CASE
    WHEN morning_avg_consumption * multiplication_factor > 3000
        AND evening_avg_consumption * multiplication_factor > 3000
    THEN 'BOTH_PERIODS'
    WHEN morning_avg_consumption * multiplication_factor > 3000
    THEN 'MORNING_ONLY'
    WHEN evening_avg_consumption * multiplication_factor > 3000
    THEN 'EVENING_ONLY'
    ELSE 'COMPLIANT'
END as violation_category
```

**Business Logic:**
- Mosques typically use 100-500W when empty
- >3000W suggests:
  - Lights/AC left on after prayer
  - Equipment malfunction
  - Unauthorized usage
- Used to identify energy waste opportunities

---

## 6. Calculate Total Energy Cost by Category (Step 17) ✅ IMPLEMENTED

**Purpose:** Separate analysis for over-consumers vs. regular consumers

**Implementation Status:** ✅ **IMPLEMENTED** in `violators` model

**Current Results:**
The `violators` table includes potential savings calculations per meter. To get aggregated totals:

```sql
-- Total by violator status
SELECT
  'Violators' as category,
  COUNT(*) as meter_count,
  ROUND(SUM(total_energy_kwh) / 1000000, 2) as total_gwh,
  ROUND(SUM(total_cost_sar) / 1000000, 2) as total_cost_million_sar,
  ROUND(SUM(total_potential_savings_sar) / 1000000, 2) as potential_savings_million_sar
FROM `testing-444715.raw_meter_readings.violators`

UNION ALL

SELECT
  'Compliant' as category,
  COUNT(*) as meter_count,
  ROUND(SUM(total_energy_kwh) / 1000000, 2) as total_gwh,
  ROUND(SUM(total_cost_sar) / 1000000, 2) as total_cost_million_sar,
  0 as potential_savings_million_sar
FROM `testing-444715.raw_meter_readings.consumption_analysis` ca
INNER JOIN `testing-444715.raw_meter_readings.int_meter_quality` q
  ON ca.meter_id = q.meter_id
WHERE q.is_good_quality = TRUE
  AND ca.meter_id NOT IN (SELECT meter_id FROM `testing-444715.raw_meter_readings.violators`)
```

**Notebook Code:**
```python
def calc_watt_SAR(merged_df, is_flagged=True):
    """Calculate total watt and SAR for over consumers or regular consumers"""
    morning_consumes = merged_df[merged_df['over_in_Morning'] == is_flagged]['Morning_Sum_Consume_MF']
    evening_consumes = merged_df[merged_df['over_in_Evening'] == is_flagged]['Evening_Sum_Consume_MF']

    # Total consumed watt
    morning_Consumed_watt = morning_consumes.sum()
    evening_Consumed_watt = evening_consumes.sum()
    total_Consumed_watt = (morning_Consumed_watt + evening_Consumed_watt)

    # Total cost in million SAR
    cost = (total_Consumed_watt / 1000 * 0.32) / 1000 / 1000

    return total_Consumed_watt, cost

# Over Consumers:
# total Consumed watt: 60.34 GWh
# total Consumed cost: 19.31 million SAR

# Regular Consumers:
# total Consumed watt: 6.42 GWh
# total Consumed cost: 2.06 million SAR
```

**Current Status:** ✅ **IMPLEMENTED**

**dbt Implementation:** Potential savings calculated per meter in `violators` model

---

## Summary of Filters

| Filter | Status | Impact | Implementation |
|--------|--------|--------|----------------|
| Quality filter (>50% missing) | ✅ **DONE** | 7.5% cleaner data | `int_meter_quality` model |
| Over-consumer flagging | ✅ **DONE** | Key business insight | `violators` model |
| Category aggregation | ✅ **DONE** | Executive summary ready | `violators` model |
| Quarter filtering | ❌ Not needed | Time period control | Handle via WHERE clause in queries |
| Ramadan removal | ❌ Optional | Different usage patterns | Only needed if analyzing Ramadan periods |
| Region filtering (Riyadh) | ❌ Optional | Regional analysis | Handle via WHERE clause in queries |

---

## Implementation Details

### ✅ Implemented Models

#### 1. `int_meter_quality` (VIEW)
Calculates data quality metrics for each meter:
- `quality_percentage`: (actual_readings - zero_readings) / expected_readings * 100
- `is_good_quality`: TRUE if quality >= 50%
- `missing_readings`, `zero_readings`, `actual_readings`

**Usage:**
```sql
SELECT * FROM `testing-444715.raw_meter_readings.int_meter_quality`
WHERE is_good_quality = FALSE
ORDER BY quality_percentage ASC
```

#### 2. `violators` (TABLE)
Contains only over-consumers (>3000W) with good data quality:
- Flags: `over_in_morning`, `over_in_evening`, `over_in_both`, `over_in_either`
- Categories: `violation_category` ('MORNING_ONLY', 'EVENING_ONLY', 'BOTH_PERIODS')
- Potential savings: `potential_savings_morning_sar`, `potential_savings_evening_sar`, `total_potential_savings_sar`

**Usage:**
```sql
-- Top violators by potential savings
SELECT meter_id, region, violation_category, total_potential_savings_sar
FROM `testing-444715.raw_meter_readings.violators`
ORDER BY total_potential_savings_sar DESC
LIMIT 10

-- Summary by region
SELECT
  region,
  COUNT(*) as violators,
  ROUND(SUM(total_potential_savings_sar) / 1000000, 2) as savings_million_sar
FROM `testing-444715.raw_meter_readings.violators`
GROUP BY region
ORDER BY violators DESC
```

---

## Remaining Optional Filters

### Region Filtering
Not implemented as a hard filter, but can be easily applied:

```sql
-- Example: Riyadh only
SELECT * FROM `testing-444715.raw_meter_readings.violators`
WHERE region = 'الرياض'  -- Arabic for 'Riyadh'

-- Example: Western region
SELECT * FROM `testing-444715.raw_meter_readings.violators`
WHERE region = 'Western'
```

### Ramadan Filtering
Only implement if analyzing periods that include Ramadan:

```sql
-- Example: Exclude Ramadan 2024 (March 11 - April 9)
SELECT * FROM `testing-444715.raw_meter_readings.consumption_analysis`
WHERE min_reading_date < '2024-03-11'
   OR max_reading_date > '2024-04-09'
```

### Quarter Filtering
Use the existing `quarter` column:

```sql
-- Example: 2022-Q4 only
SELECT * FROM `testing-444715.raw_meter_readings.violators`
WHERE quarter = '2022-Q4'
```

---

## Next Steps

All core business logic filters are now implemented! 🎉

To extend the analysis:
1. **Regional analysis**: Use WHERE clauses on `region` column
2. **Quarterly reports**: Group by `quarter` column
3. **Ramadan handling**: Add date range filters if needed
4. **Custom dashboards**: Use the `violators` table for energy waste insights
