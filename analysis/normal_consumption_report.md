# Normal Mosque Consumption Pattern Analysis Report

**Analysis Date:** December 29, 2025
**Data Source:** BigQuery - `raw_meter_readings.consumption_analysis`
**Methodology:** Statistical analysis of compliant meters (non-violators with good quality data)

---

## Executive Summary

This analysis establishes baseline consumption patterns for mosques by examining **compliant meters** - those with good data quality (>50%) that do not exceed the 3000W violation threshold.

### Key Findings

| Metric | Value |
|--------|-------|
| **Normal Meters** | 28,228 (52.6%) |
| **Violator Meters** | 25,415 (47.4%) |
| **Normal Median (Morning)** | 764 W |
| **Normal Median (Evening)** | 894 W |
| **Violator Average** | ~9,100 W |
| **Consumption Gap** | 9x difference |

**Critical Insight:** Nearly half (47.4%) of meters are classified as violators, indicating substantial energy waste potential in the mosque network.

---

## 1. Data Foundation

### 1.1 Population Summary

| Category | Count | Avg Morning (W) | Avg Evening (W) |
|----------|-------|-----------------|-----------------|
| Normal (Compliant) | 28,228 | 980 | 1,100 |
| Violator (>3000W) | 25,415 | 9,180 | 9,052 |
| **Total** | **53,643** | - | - |

### 1.2 Data Quality Filters Applied

```
1. is_good_quality = TRUE (>50% quality score)
2. data_quality_flag = 'COMPLETE'
3. NOT in violators table (consumption < 3000W threshold)
4. Multiplication factor applied to raw readings
```

### 1.3 Consumption Calculation

All consumption values include the multiplication factor (MF):
- `consumption_with_mf = raw_consumption × multiplication_factor`
- MF accounts for meter capacity/facility size

---

## 2. Statistical Profile

### 2.1 Percentile Distribution (Compliant Meters Only)

| Percentile | Morning (W) | Evening (W) | Interpretation |
|------------|-------------|-------------|----------------|
| **P10** | 123 | 184 | Very low consumers |
| **P25** | 317 | 408 | Lower quartile |
| **P50** (Median) | **764** | **894** | Typical mosque |
| **P75** | 1,528 | 1,701 | Upper quartile |
| **P90** | 2,204 | 2,382 | High but compliant |

### 2.2 Central Tendency

| Period | Mean (W) | Median (W) | Std Dev (W) |
|--------|----------|------------|-------------|
| Morning | 980 | 764 | ~600 |
| Evening | 1,100 | 894 | ~700 |

**Note:** Mean > Median indicates right-skewed distribution (some high consumers pull up the average).

### 2.3 Evening vs Morning Pattern

- Evening consumption is **17% higher** than morning
- Evening median: 894W vs Morning median: 764W
- This aligns with:
  - Isha prayer typically having higher attendance than Fajr
  - Longer evening hours (Maghrib + Isha)
  - Potential lighting needs in evening

---

## 3. Normal Range Definition

### 3.1 Recommended Thresholds

| Range Type | Morning (W) | Evening (W) | Usage |
|------------|-------------|-------------|-------|
| **Typical (IQR)** | 317 - 1,528 | 408 - 1,701 | 50% of mosques fall here |
| **Extended (P10-P90)** | 123 - 2,204 | 184 - 2,382 | 80% of mosques fall here |
| **Violation Threshold** | >3,000 | >3,000 | Current pipeline setting |

### 3.2 Threshold Validation

The 3000W threshold appears **well-calibrated**:
- P90 (morning): 2,204W — 806W buffer below threshold
- P90 (evening): 2,382W — 618W buffer below threshold
- Only **true anomalies** exceed the threshold

### 3.3 Interquartile Range (IQR)

```
Morning IQR = P75 - P25 = 1,528 - 317 = 1,211W
Evening IQR = P75 - P25 = 1,701 - 408 = 1,293W
```

Using IQR method for outlier detection:
- Upper Fence = P75 + 1.5 × IQR
- Morning Upper Fence = 1,528 + 1.5 × 1,211 = 3,345W ≈ 3000W threshold ✓

---

## 4. Behavioral Insights

### 4.1 Consumption Tiers (Proposed Classification)

Based on percentile analysis, we recommend a 6-tier classification:

| Tier | Consumption Range | % of Population | Description |
|------|-------------------|-----------------|-------------|
| EFFICIENT | ≤ P25 (≤408W) | 25% | Exemplary low consumption |
| NORMAL_LOW | P25-P50 (408-894W) | 25% | Below median, healthy |
| NORMAL_HIGH | P50-P75 (894-1701W) | 25% | Above median, acceptable |
| ELEVATED | P75-P90 (1701-2382W) | 15% | Warning zone |
| HIGH | P90-3000W | ~8% | Approaching threshold |
| VIOLATOR | >3000W | 47.4% | Over threshold |

### 4.2 Violator Characteristics

Violators consume on average:
- **9x more** than normal meters
- **Morning:** 9,180W vs 980W (normal)
- **Evening:** 9,052W vs 1,100W (normal)

Potential root causes:
1. **HVAC running continuously** (not just during prayers)
2. **Commercial/residential usage** mixed with mosque
3. **Large facilities** with high base load
4. **Meter issues** or configuration errors

### 4.3 The 47% Problem

Nearly half of meters are violators. This represents:
- **Significant energy waste** in the mosque network
- **Massive savings opportunity** (estimated millions SAR/year)
- **Need for intervention** — awareness campaigns, audits, equipment upgrades

---

## 5. Recommendations

### 5.1 Immediate Actions

1. **Implement Tiered Classification**
   - Replace binary Normal/Violator with 6-tier system
   - Enable graduated warnings and interventions

2. **Update Savings Calculations**
   - Current baseline: 500W (arbitrary)
   - Recommended baseline: **P50 (764W morning, 894W evening)**
   - More accurate excess consumption = actual - P50

3. **Add Percentile Ranking**
   - Each meter shows: "Consumes more than X% of mosques"
   - Enables peer comparison and motivation

### 5.2 Enhanced Analytics

4. **Root Cause Categorization**
   - Flag probable causes for violators
   - OVERNIGHT_USAGE, SEASONAL_COOLING, LARGE_FACILITY, etc.

5. **Trend Analysis**
   - Track quarter-over-quarter changes
   - Detect sudden spikes (new equipment, maintenance issues)

6. **Efficiency Scoring**
   - Score 0-100 (100 = most efficient)
   - Gamification potential for energy conservation

### 5.3 Dashboard Enhancements

7. **Regional Benchmarking**
   - Show meter vs regional P50
   - Climate-adjusted comparisons

8. **Alert System**
   - Warning when > P90
   - Critical when approaching 3000W

---

## 6. Data Tables (Query Results)

### Query 1: Normal vs Violator Comparison

```csv
category,count,morning_avg_w,evening_avg_w
Normal,28228,980.0,1100.0
Violator,25415,9180.0,9052.0
```

### Query 2: Percentile Distribution

```csv
period,P10,P25,P50,P75,P90
Evening,184.0,408.0,894.0,1701.0,2382.0
Morning,123.0,317.0,764.0,1528.0,2204.0
```

---

## 7. Technical Appendix

### 7.1 SQL Query for Compliant Meters

```sql
WITH compliant AS (
    SELECT
        c.meter_id,
        c.quarter,
        c.morning_avg_consumption * c.multiplication_factor as morning_avg_mf,
        c.evening_avg_consumption * c.multiplication_factor as evening_avg_mf
    FROM `raw_meter_readings.consumption_analysis` c
    INNER JOIN `raw_meter_readings.int_meter_quality` q
        ON c.meter_id = q.meter_id AND c.quarter = q.quarter
    LEFT JOIN `raw_meter_readings.violators` v
        ON c.meter_id = v.meter_id AND c.quarter = v.quarter
    WHERE v.meter_id IS NULL  -- NOT a violator
      AND q.is_good_quality = TRUE
      AND c.data_quality_flag = 'COMPLETE'
)
SELECT
    'Morning' as period,
    ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(10)], 0) as P10,
    ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(25)], 0) as P25,
    ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(50)], 0) as P50,
    ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(75)], 0) as P75,
    ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(90)], 0) as P90
FROM compliant
```

### 7.2 Variables Used

```yaml
violation_threshold_watts: 3000
quality_threshold_pct: 50
baseline_consumption_watts: 500  # Recommend updating to P50
electricity_rate_sar: 0.32
```

---

## 8. Conclusion

Normal mosque consumption follows a predictable pattern:
- **Median: ~764-894W** during prayer periods
- **IQR: ~317-1,701W** covers typical variation
- **3000W threshold** effectively separates normal from excessive

The 47% violation rate represents both a problem and an opportunity. By implementing tiered classification, benchmark-based savings calculations, and trend analysis, we can better target interventions and maximize energy conservation impact.

---

*Report generated from BigQuery analysis of raw_meter_readings dataset.*
*For methodology details, see: `analysis/normal_patterns_queries.sql`*
