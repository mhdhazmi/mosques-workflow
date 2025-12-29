"""
Normal Mosque Consumption Pattern Analysis

This script analyzes consumption data to identify normal patterns for mosques,
excluding over-consumers (violators) to establish baseline consumption profiles.
"""

from google.cloud import bigquery
import pandas as pd
import numpy as np
import os

# Set credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../include/gcp_adc.json'

client = bigquery.Client()

# =============================================================================
# STEP 1: Get Compliant Meters (Non-Violators with Good Quality)
# =============================================================================

print("=" * 80)
print("NORMAL MOSQUE CONSUMPTION PATTERN ANALYSIS")
print("=" * 80)

# Query to get compliant meters (NOT in violators, good quality)
compliant_query = """
WITH quality_meters AS (
    SELECT
        meter_id,
        quarter,
        quality_percentage,
        is_good_quality
    FROM `raw_meter_readings.int_meter_quality`
    WHERE is_good_quality = TRUE
),

violator_meters AS (
    SELECT DISTINCT meter_id, quarter
    FROM `raw_meter_readings.violators`
),

compliant AS (
    SELECT
        c.*,
        q.quality_percentage
    FROM `raw_meter_readings.consumption_analysis` c
    INNER JOIN quality_meters q
        ON c.meter_id = q.meter_id AND c.quarter = q.quarter
    LEFT JOIN violator_meters v
        ON c.meter_id = v.meter_id AND c.quarter = v.quarter
    WHERE v.meter_id IS NULL  -- NOT a violator
      AND c.data_quality_flag = 'COMPLETE'
)

SELECT
    meter_id,
    quarter,
    morning_avg_consumption,
    evening_avg_consumption,
    total_avg_consumption,
    morning_reading_count,
    evening_reading_count,
    total_reading_count,
    multiplication_factor,
    region,
    province,
    morning_avg_consumption * multiplication_factor as morning_avg_with_mf,
    evening_avg_consumption * multiplication_factor as evening_avg_with_mf,
    total_avg_consumption * multiplication_factor as total_avg_with_mf,
    quality_percentage
FROM compliant
"""

print("\n1. QUERYING COMPLIANT METERS (Non-Violators, Good Quality)...")
compliant_df = client.query(compliant_query).to_dataframe()
print(f"   Found {len(compliant_df):,} compliant meter-quarter records")

# =============================================================================
# STEP 2: Basic Statistics
# =============================================================================

print("\n" + "=" * 80)
print("2. BASIC CONSUMPTION STATISTICS (Compliant Meters)")
print("=" * 80)

# Raw consumption (without multiplication factor)
print("\n--- Raw Consumption (Watts) ---")
for col in ['morning_avg_consumption', 'evening_avg_consumption', 'total_avg_consumption']:
    data = compliant_df[col].dropna()
    print(f"\n{col}:")
    print(f"  Count:  {len(data):,}")
    print(f"  Mean:   {data.mean():.2f} W")
    print(f"  Median: {data.median():.2f} W")
    print(f"  Std:    {data.std():.2f} W")
    print(f"  Min:    {data.min():.2f} W")
    print(f"  Max:    {data.max():.2f} W")

# With multiplication factor
print("\n--- Consumption WITH Multiplication Factor (Watts) ---")
for col in ['morning_avg_with_mf', 'evening_avg_with_mf', 'total_avg_with_mf']:
    data = compliant_df[col].dropna()
    print(f"\n{col}:")
    print(f"  Count:  {len(data):,}")
    print(f"  Mean:   {data.mean():.2f} W")
    print(f"  Median: {data.median():.2f} W")
    print(f"  Std:    {data.std():.2f} W")
    print(f"  Min:    {data.min():.2f} W")
    print(f"  Max:    {data.max():.2f} W")

# =============================================================================
# STEP 3: Percentile Distribution
# =============================================================================

print("\n" + "=" * 80)
print("3. PERCENTILE DISTRIBUTION (with Multiplication Factor)")
print("=" * 80)

percentiles = [5, 10, 25, 50, 75, 90, 95, 99]

print("\nMorning Consumption Percentiles (Watts):")
morning_data = compliant_df['morning_avg_with_mf'].dropna()
for p in percentiles:
    val = np.percentile(morning_data, p)
    print(f"  P{p:02d}: {val:,.0f} W")

print("\nEvening Consumption Percentiles (Watts):")
evening_data = compliant_df['evening_avg_with_mf'].dropna()
for p in percentiles:
    val = np.percentile(evening_data, p)
    print(f"  P{p:02d}: {val:,.0f} W")

print("\nTotal Consumption Percentiles (Watts):")
total_data = compliant_df['total_avg_with_mf'].dropna()
for p in percentiles:
    val = np.percentile(total_data, p)
    print(f"  P{p:02d}: {val:,.0f} W")

# =============================================================================
# STEP 4: Regional Analysis
# =============================================================================

print("\n" + "=" * 80)
print("4. REGIONAL CONSUMPTION PATTERNS")
print("=" * 80)

regional = compliant_df.groupby('region').agg({
    'meter_id': 'count',
    'morning_avg_with_mf': ['mean', 'median', 'std'],
    'evening_avg_with_mf': ['mean', 'median', 'std'],
    'total_avg_with_mf': ['mean', 'median', 'std']
}).round(2)

regional.columns = ['_'.join(col).strip() for col in regional.columns.values]
print("\n" + regional.to_string())

# =============================================================================
# STEP 5: Size-Based Analysis (by Multiplication Factor)
# =============================================================================

print("\n" + "=" * 80)
print("5. SIZE-BASED PATTERNS (by Multiplication Factor)")
print("=" * 80)

# Group by multiplication factor
mf_analysis = compliant_df.groupby('multiplication_factor').agg({
    'meter_id': 'count',
    'morning_avg_consumption': ['mean', 'median'],
    'evening_avg_consumption': ['mean', 'median'],
    'morning_avg_with_mf': ['mean', 'median'],
    'evening_avg_with_mf': ['mean', 'median']
}).round(2)

mf_analysis.columns = ['_'.join(col).strip() for col in mf_analysis.columns.values]
print("\n" + mf_analysis.to_string())

# =============================================================================
# STEP 6: Quarterly Comparison
# =============================================================================

print("\n" + "=" * 80)
print("6. QUARTERLY PATTERNS (Seasonal Variation)")
print("=" * 80)

quarterly = compliant_df.groupby('quarter').agg({
    'meter_id': 'count',
    'morning_avg_with_mf': ['mean', 'median', 'std'],
    'evening_avg_with_mf': ['mean', 'median', 'std'],
}).round(2)

quarterly.columns = ['_'.join(col).strip() for col in quarterly.columns.values]
print("\n" + quarterly.to_string())

# =============================================================================
# STEP 7: Evening-to-Morning Ratio
# =============================================================================

print("\n" + "=" * 80)
print("7. EVENING-TO-MORNING RATIO ANALYSIS")
print("=" * 80)

compliant_df['evening_morning_ratio'] = (
    compliant_df['evening_avg_with_mf'] /
    compliant_df['morning_avg_with_mf'].replace(0, np.nan)
)

ratio_data = compliant_df['evening_morning_ratio'].dropna()
ratio_data = ratio_data[ratio_data < 10]  # Filter extreme outliers

print(f"\nEvening/Morning Ratio (compliant meters):")
print(f"  Mean:   {ratio_data.mean():.3f}")
print(f"  Median: {ratio_data.median():.3f}")
print(f"  Std:    {ratio_data.std():.3f}")
print(f"  P25:    {np.percentile(ratio_data, 25):.3f}")
print(f"  P75:    {np.percentile(ratio_data, 75):.3f}")

# =============================================================================
# STEP 8: Define Normal Ranges
# =============================================================================

print("\n" + "=" * 80)
print("8. RECOMMENDED NORMAL CONSUMPTION RANGES")
print("=" * 80)

# Using IQR method: Normal = P25 to P75, Extended = P10 to P90
print("\n--- MORNING PERIOD (with MF) ---")
m_p10 = np.percentile(morning_data, 10)
m_p25 = np.percentile(morning_data, 25)
m_p50 = np.percentile(morning_data, 50)
m_p75 = np.percentile(morning_data, 75)
m_p90 = np.percentile(morning_data, 90)

print(f"  Typical Range (IQR):    {m_p25:,.0f} - {m_p75:,.0f} W")
print(f"  Extended Range (10-90): {m_p10:,.0f} - {m_p90:,.0f} W")
print(f"  Median (Normal):        {m_p50:,.0f} W")

print("\n--- EVENING PERIOD (with MF) ---")
e_p10 = np.percentile(evening_data, 10)
e_p25 = np.percentile(evening_data, 25)
e_p50 = np.percentile(evening_data, 50)
e_p75 = np.percentile(evening_data, 75)
e_p90 = np.percentile(evening_data, 90)

print(f"  Typical Range (IQR):    {e_p25:,.0f} - {e_p75:,.0f} W")
print(f"  Extended Range (10-90): {e_p10:,.0f} - {e_p90:,.0f} W")
print(f"  Median (Normal):        {e_p50:,.0f} W")

print("\n--- TOTAL CONSUMPTION (with MF) ---")
t_p10 = np.percentile(total_data, 10)
t_p25 = np.percentile(total_data, 25)
t_p50 = np.percentile(total_data, 50)
t_p75 = np.percentile(total_data, 75)
t_p90 = np.percentile(total_data, 90)

print(f"  Typical Range (IQR):    {t_p25:,.0f} - {t_p75:,.0f} W")
print(f"  Extended Range (10-90): {t_p10:,.0f} - {t_p90:,.0f} W")
print(f"  Median (Normal):        {t_p50:,.0f} W")

# =============================================================================
# STEP 9: Summary Table
# =============================================================================

print("\n" + "=" * 80)
print("9. SUMMARY: NORMAL MOSQUE CONSUMPTION PROFILE")
print("=" * 80)

summary = f"""
┌──────────────────────────────────────────────────────────────────┐
│                NORMAL MOSQUE CONSUMPTION PROFILE                  │
├──────────────────────────────────────────────────────────────────┤
│ Based on {len(compliant_df):,} compliant meter-quarter records               │
│ (Excludes violators and low-quality data)                        │
├──────────────────────────────────────────────────────────────────┤
│ MORNING PERIOD (Fajr+100min to Dhuhr-80min, excludes Fridays)    │
│   • Median:        {m_p50:>6,.0f} W                                     │
│   • Normal Range:  {m_p25:>6,.0f} - {m_p75:>6,.0f} W (IQR)                    │
│   • Extended:      {m_p10:>6,.0f} - {m_p90:>6,.0f} W (P10-P90)                │
├──────────────────────────────────────────────────────────────────┤
│ EVENING PERIOD (Isha+90min to Fajr-80min)                        │
│   • Median:        {e_p50:>6,.0f} W                                     │
│   • Normal Range:  {e_p25:>6,.0f} - {e_p75:>6,.0f} W (IQR)                    │
│   • Extended:      {e_p10:>6,.0f} - {e_p90:>6,.0f} W (P10-P90)                │
├──────────────────────────────────────────────────────────────────┤
│ OVERALL CONSUMPTION                                              │
│   • Median:        {t_p50:>6,.0f} W                                     │
│   • Normal Range:  {t_p25:>6,.0f} - {t_p75:>6,.0f} W (IQR)                    │
│   • Extended:      {t_p10:>6,.0f} - {t_p90:>6,.0f} W (P10-P90)                │
├──────────────────────────────────────────────────────────────────┤
│ VIOLATION THRESHOLD: 3,000 W (current pipeline setting)         │
│   • {len(compliant_df[compliant_df['morning_avg_with_mf'] < 3000]):,} meters below threshold in morning               │
│   • {len(compliant_df[compliant_df['evening_avg_with_mf'] < 3000]):,} meters below threshold in evening               │
└──────────────────────────────────────────────────────────────────┘
"""
print(summary)

# =============================================================================
# STEP 10: Compare Violators vs Compliant
# =============================================================================

print("\n" + "=" * 80)
print("10. VIOLATOR vs COMPLIANT COMPARISON")
print("=" * 80)

violator_query = """
SELECT
    morning_avg_mf,
    evening_avg_mf,
    violation_category,
    region
FROM `raw_meter_readings.violators`
"""

violator_df = client.query(violator_query).to_dataframe()

print(f"\nCompliant Meters: {len(compliant_df):,}")
print(f"Violator Meters:  {len(violator_df):,}")
print(f"Violation Rate:   {len(violator_df) / (len(compliant_df) + len(violator_df)) * 100:.1f}%")

print("\n--- Morning Consumption Comparison ---")
print(f"  Compliant Mean:   {compliant_df['morning_avg_with_mf'].mean():,.0f} W")
print(f"  Violator Mean:    {violator_df['morning_avg_mf'].mean():,.0f} W")
print(f"  Difference:       {violator_df['morning_avg_mf'].mean() - compliant_df['morning_avg_with_mf'].mean():,.0f} W")

print("\n--- Evening Consumption Comparison ---")
print(f"  Compliant Mean:   {compliant_df['evening_avg_with_mf'].mean():,.0f} W")
print(f"  Violator Mean:    {violator_df['evening_avg_mf'].mean():,.0f} W")
print(f"  Difference:       {violator_df['evening_avg_mf'].mean() - compliant_df['evening_avg_with_mf'].mean():,.0f} W")

print("\n--- Violation Category Breakdown ---")
print(violator_df['violation_category'].value_counts().to_string())

print("\n" + "=" * 80)
print("ANALYSIS COMPLETE")
print("=" * 80)
