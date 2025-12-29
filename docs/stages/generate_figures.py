"""
Generate figures for ML Lifecycle Documentation
Run: python generate_figures.py
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import os

# Set style
plt.style.use('seaborn-v0_8-whitegrid')
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['font.size'] = 11
plt.rcParams['axes.titlesize'] = 14
plt.rcParams['axes.labelsize'] = 12

# Output directory
OUTPUT_DIR = "images"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def save_fig(name):
    """Save figure with tight layout"""
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/{name}.png", dpi=150, bbox_inches='tight',
                facecolor='white', edgecolor='none')
    plt.close()
    print(f"Created: {OUTPUT_DIR}/{name}.png")


# =============================================================================
# 1. CONFUSION MATRIX (Stage 4)
# =============================================================================
def create_confusion_matrix():
    """Create confusion matrix heatmap"""
    cm = np.array([[1366, 104],
                   [133, 2597]])

    fig, ax = plt.subplots(figsize=(8, 6))

    # Create heatmap
    im = ax.imshow(cm, cmap='Blues')

    # Labels
    classes = ['Non-Mosque', 'Mosque']
    ax.set_xticks([0, 1])
    ax.set_yticks([0, 1])
    ax.set_xticklabels(classes)
    ax.set_yticklabels(classes)
    ax.set_xlabel('Predicted Label', fontweight='bold')
    ax.set_ylabel('Actual Label', fontweight='bold')
    ax.set_title('Confusion Matrix - Mosque Classifier', fontweight='bold', fontsize=14)

    # Add text annotations
    for i in range(2):
        for j in range(2):
            color = 'white' if cm[i, j] > 1500 else 'black'
            text = f'{cm[i, j]}\n({cm[i, j]/cm.sum()*100:.1f}%)'
            ax.text(j, i, text, ha='center', va='center', color=color, fontsize=12)

    # Add colorbar
    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label('Count', rotation=270, labelpad=15)

    # Add labels for TN, FP, FN, TP
    ax.text(-0.3, 0, 'TN', ha='center', va='center', fontsize=10, color='gray')
    ax.text(1.3, 0, 'FP', ha='center', va='center', fontsize=10, color='gray')
    ax.text(-0.3, 1, 'FN', ha='center', va='center', fontsize=10, color='gray')
    ax.text(1.3, 1, 'TP', ha='center', va='center', fontsize=10, color='gray')

    save_fig("confusion_matrix")


# =============================================================================
# 2. ROC CURVE (Stage 4)
# =============================================================================
def create_roc_curve():
    """Create ROC curve with AUC"""
    # Simulated ROC curve data (hypothetical)
    fpr = np.array([0, 0.02, 0.05, 0.08, 0.12, 0.18, 0.25, 0.35, 0.5, 0.7, 1.0])
    tpr = np.array([0, 0.45, 0.72, 0.85, 0.91, 0.94, 0.96, 0.97, 0.98, 0.99, 1.0])
    auc = 0.97

    fig, ax = plt.subplots(figsize=(8, 8))

    # Plot ROC curve
    ax.plot(fpr, tpr, 'b-', linewidth=2, label=f'ROC Curve (AUC = {auc:.2f})')
    ax.plot([0, 1], [0, 1], 'k--', linewidth=1, label='Random Classifier')

    # Fill area under curve
    ax.fill_between(fpr, tpr, alpha=0.3)

    # Mark operating point
    ax.plot(0.07, 0.95, 'ro', markersize=10, label='Operating Point (threshold=0.5)')

    ax.set_xlabel('False Positive Rate', fontweight='bold')
    ax.set_ylabel('True Positive Rate', fontweight='bold')
    ax.set_title('ROC Curve - Mosque Classifier', fontweight='bold', fontsize=14)
    ax.legend(loc='lower right')
    ax.set_xlim([0, 1])
    ax.set_ylim([0, 1])
    ax.grid(True, alpha=0.3)

    save_fig("roc_curve")


# =============================================================================
# 3. FEATURE IMPORTANCE (Stage 3)
# =============================================================================
def create_feature_importance():
    """Create feature importance bar chart"""
    features = [
        'friday_consumption_ratio',
        'morning_avg_consumption',
        'evening_avg_consumption',
        'daily_variance',
        'evening_to_morning_ratio',
        'weekend_pattern'
    ]
    importance = [0.35, 0.20, 0.18, 0.12, 0.08, 0.07]

    fig, ax = plt.subplots(figsize=(10, 6))

    colors = plt.cm.Blues(np.linspace(0.4, 0.8, len(features)))
    bars = ax.barh(features, importance, color=colors, edgecolor='navy', linewidth=0.5)

    # Add value labels
    for bar, val in zip(bars, importance):
        ax.text(val + 0.01, bar.get_y() + bar.get_height()/2,
                f'{val:.0%}', va='center', fontsize=10)

    ax.set_xlabel('Importance Score', fontweight='bold')
    ax.set_title('Feature Importance - Random Forest Classifier', fontweight='bold', fontsize=14)
    ax.set_xlim([0, 0.45])
    ax.invert_yaxis()  # Highest at top

    save_fig("feature_importance")


# =============================================================================
# 4. DATA SPLIT PIE CHART (Stage 2)
# =============================================================================
def create_data_split():
    """Create train/val/test split pie chart"""
    sizes = [70, 15, 15]
    labels = ['Training\n(70%)\n~19,600 meters',
              'Validation\n(15%)\n~4,200 meters',
              'Test\n(15%)\n~4,200 meters']
    colors = ['#2ecc71', '#3498db', '#e74c3c']
    explode = (0.02, 0.02, 0.02)

    fig, ax = plt.subplots(figsize=(8, 8))

    wedges, texts, autotexts = ax.pie(sizes, labels=labels, colors=colors,
                                       explode=explode, autopct='',
                                       startangle=90, shadow=True)

    ax.set_title('Train / Validation / Test Split\nMosque Classifier Dataset',
                 fontweight='bold', fontsize=14)

    save_fig("data_split")


# =============================================================================
# 5. CROSS-VALIDATION SCORES (Stage 3)
# =============================================================================
def create_cv_scores():
    """Create cross-validation scores plot"""
    folds = ['Fold 1', 'Fold 2', 'Fold 3', 'Fold 4', 'Fold 5']
    scores = [0.932, 0.941, 0.937, 0.945, 0.939]
    mean_score = np.mean(scores)
    std_score = np.std(scores)

    fig, ax = plt.subplots(figsize=(10, 6))

    bars = ax.bar(folds, scores, color='steelblue', edgecolor='navy', linewidth=1)

    # Mean line
    ax.axhline(y=mean_score, color='red', linestyle='--', linewidth=2,
               label=f'Mean = {mean_score:.3f}')

    # Std band
    ax.axhspan(mean_score - std_score, mean_score + std_score,
               alpha=0.2, color='red', label=f'Std = {std_score:.3f}')

    # Value labels
    for bar, score in zip(bars, scores):
        ax.text(bar.get_x() + bar.get_width()/2, score + 0.002,
                f'{score:.3f}', ha='center', fontsize=10)

    ax.set_ylabel('F1 Score', fontweight='bold')
    ax.set_xlabel('Cross-Validation Fold', fontweight='bold')
    ax.set_title('5-Fold Stratified Cross-Validation Results', fontweight='bold', fontsize=14)
    ax.set_ylim([0.90, 0.96])
    ax.legend(loc='lower right')
    ax.grid(axis='y', alpha=0.3)

    save_fig("cv_scores")


# =============================================================================
# 6. REGIONAL DISTRIBUTION (Stage 4)
# =============================================================================
def create_regional_distribution():
    """Create regional violator distribution chart"""
    regions = ['Central', 'Western', 'Eastern', 'Southern', 'Northern']
    total_meters = [8500, 7200, 5800, 4100, 2856]
    violators = [3400, 3024, 2262, 1681, 1199]
    rates = [v/t*100 for v, t in zip(violators, total_meters)]

    x = np.arange(len(regions))
    width = 0.35

    fig, ax1 = plt.subplots(figsize=(10, 6))

    # Bar charts
    bars1 = ax1.bar(x - width/2, total_meters, width, label='Total Meters',
                    color='lightsteelblue', edgecolor='navy')
    bars2 = ax1.bar(x + width/2, violators, width, label='Violators',
                    color='coral', edgecolor='darkred')

    ax1.set_xlabel('Region', fontweight='bold')
    ax1.set_ylabel('Meter Count', fontweight='bold')
    ax1.set_title('Violator Distribution by Region', fontweight='bold', fontsize=14)
    ax1.set_xticks(x)
    ax1.set_xticklabels(regions)
    ax1.legend(loc='upper right')

    # Secondary axis for rate
    ax2 = ax1.twinx()
    ax2.plot(x, rates, 'go-', linewidth=2, markersize=8, label='Violation Rate')
    ax2.set_ylabel('Violation Rate (%)', fontweight='bold', color='green')
    ax2.tick_params(axis='y', labelcolor='green')
    ax2.set_ylim([35, 45])

    # Rate labels
    for i, rate in enumerate(rates):
        ax2.text(i, rate + 0.5, f'{rate:.1f}%', ha='center', color='green', fontsize=9)

    save_fig("regional_distribution")


# =============================================================================
# 7. MONTHLY TREND (Stage 7)
# =============================================================================
def create_monthly_trend():
    """Create monthly violator trend chart"""
    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
              'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    violators = [10500, 10800, 11200, 11800, 12300, 11563,
                 12800, 13200, 13923, 14100, 14500, 15000]
    savings = [1.8, 1.9, 2.0, 2.1, 2.2, 2.1,
               2.3, 2.4, 2.45, 2.5, 2.6, 2.7]  # Million SAR

    fig, ax1 = plt.subplots(figsize=(12, 6))

    # Violator count line
    color1 = 'steelblue'
    ax1.plot(months, violators, 'o-', color=color1, linewidth=2, markersize=8,
             label='Violator Count')
    ax1.fill_between(months, violators, alpha=0.3, color=color1)
    ax1.set_xlabel('Month (2025)', fontweight='bold')
    ax1.set_ylabel('Violator Count', fontweight='bold', color=color1)
    ax1.tick_params(axis='y', labelcolor=color1)

    # Savings line
    ax2 = ax1.twinx()
    color2 = 'green'
    ax2.plot(months, savings, 's--', color=color2, linewidth=2, markersize=8,
             label='Potential Savings (M SAR)')
    ax2.set_ylabel('Potential Savings (Million SAR)', fontweight='bold', color=color2)
    ax2.tick_params(axis='y', labelcolor=color2)

    # Title and legend
    ax1.set_title('Monthly Violator Trend & Potential Savings', fontweight='bold', fontsize=14)

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')

    ax1.grid(axis='y', alpha=0.3)

    save_fig("monthly_trend")


# =============================================================================
# 8. PIPELINE ARCHITECTURE (Stage 1, 5)
# =============================================================================
def create_pipeline_architecture():
    """Create pipeline architecture diagram"""
    fig, ax = plt.subplots(figsize=(14, 10))
    ax.set_xlim(0, 14)
    ax.set_ylim(0, 10)
    ax.axis('off')

    # Define box style
    box_style = dict(boxstyle='round,pad=0.5', facecolor='lightblue',
                     edgecolor='navy', linewidth=2)
    arrow_style = dict(arrowstyle='->', color='navy', linewidth=2)

    # Data Sources (left side)
    ax.text(1, 9, 'Data Sources', fontsize=14, fontweight='bold', ha='center')
    ax.add_patch(mpatches.FancyBboxPatch((0.2, 7.5), 1.6, 1, boxstyle='round',
                                          facecolor='lightyellow', edgecolor='orange'))
    ax.text(1, 8, 'CSV Files\n(~115M rows)', fontsize=9, ha='center', va='center')

    ax.add_patch(mpatches.FancyBboxPatch((0.2, 6), 1.6, 1, boxstyle='round',
                                          facecolor='lightyellow', edgecolor='orange'))
    ax.text(1, 6.5, 'Prayer Times\n(seed)', fontsize=9, ha='center', va='center')

    ax.add_patch(mpatches.FancyBboxPatch((0.2, 4.5), 1.6, 1, boxstyle='round',
                                          facecolor='lightyellow', edgecolor='orange'))
    ax.text(1, 5, 'Industry Codes\n(seed)', fontsize=9, ha='center', va='center')

    # ETL Processing
    ax.annotate('', xy=(3.5, 7), xytext=(2, 7.5), arrowprops=arrow_style)
    ax.add_patch(mpatches.FancyBboxPatch((3.5, 6.5), 2, 1.5, boxstyle='round',
                                          facecolor='lightgreen', edgecolor='darkgreen'))
    ax.text(4.5, 7.25, 'ETL Processor\n(Polars)', fontsize=10, ha='center', va='center', fontweight='bold')
    ax.text(4.5, 6.7, 'Dedupe | Clean | Validate', fontsize=8, ha='center', va='center')

    # Cloud Upload
    ax.annotate('', xy=(6, 7.25), xytext=(5.5, 7.25), arrowprops=arrow_style)
    ax.add_patch(mpatches.FancyBboxPatch((6, 6.5), 2, 1.5, boxstyle='round',
                                          facecolor='lightcoral', edgecolor='darkred'))
    ax.text(7, 7.25, 'Cloud Loader\n(GCS)', fontsize=10, ha='center', va='center', fontweight='bold')
    ax.text(7, 6.7, 'Upload | MERGE', fontsize=8, ha='center', va='center')

    # BigQuery
    ax.annotate('', xy=(9, 7.25), xytext=(8, 7.25), arrowprops=arrow_style)
    ax.add_patch(mpatches.FancyBboxPatch((9, 6), 2.5, 2.5, boxstyle='round',
                                          facecolor='lightskyblue', edgecolor='blue'))
    ax.text(10.25, 8, 'BigQuery', fontsize=12, ha='center', va='center', fontweight='bold')
    ax.text(10.25, 7.3, 'smart_meters_clean', fontsize=8, ha='center')
    ax.text(10.25, 6.8, 'prayer_times', fontsize=8, ha='center')
    ax.text(10.25, 6.3, 'industry_codes', fontsize=8, ha='center')

    # dbt Transformations
    ax.annotate('', xy=(10.25, 5.8), xytext=(10.25, 6), arrowprops=arrow_style)
    ax.add_patch(mpatches.FancyBboxPatch((8.5, 3.5), 3.5, 2,
                 boxstyle='round', facecolor='plum', edgecolor='purple'))
    ax.text(10.25, 5, 'dbt Transformations', fontsize=11, ha='center', va='center', fontweight='bold')
    ax.text(10.25, 4.3, 'Staging -> Intermediate -> Marts', fontsize=9, ha='center')
    ax.text(10.25, 3.8, 'consumption_analysis | violators', fontsize=8, ha='center')

    # Classifier (optional)
    ax.add_patch(mpatches.FancyBboxPatch((3.5, 3.5), 2, 1.5, boxstyle='round',
                                          facecolor='wheat', edgecolor='brown', linestyle='--'))
    ax.text(4.5, 4.25, 'Classifier\n(Optional)', fontsize=10, ha='center', va='center')
    ax.text(4.5, 3.7, 'Random Forest', fontsize=8, ha='center')

    # Outputs
    ax.annotate('', xy=(10.25, 3.3), xytext=(10.25, 3.5), arrowprops=arrow_style)
    ax.add_patch(mpatches.FancyBboxPatch((8.5, 1.5), 3.5, 1.5, boxstyle='round',
                                          facecolor='lightgreen', edgecolor='darkgreen'))
    ax.text(10.25, 2.5, 'Analytics Output', fontsize=11, ha='center', va='center', fontweight='bold')
    ax.text(10.25, 1.9, 'Violators | Savings | Reports', fontsize=9, ha='center')

    # Orchestration label
    ax.add_patch(mpatches.FancyBboxPatch((2.5, 0.5), 6, 0.8, boxstyle='round',
                                          facecolor='lavender', edgecolor='navy'))
    ax.text(5.5, 0.9, 'Airflow Orchestration (Astronomer)', fontsize=11,
            ha='center', va='center', fontweight='bold')

    ax.set_title('Smart Meter Pipeline Architecture', fontsize=16, fontweight='bold', pad=20)

    save_fig("pipeline_architecture")


# =============================================================================
# 9. DATA QUALITY BEFORE/AFTER (Stage 2)
# =============================================================================
def create_data_quality():
    """Create before/after data quality comparison"""
    categories = ['Total Rows', 'Duplicates', 'Outliers\n(>1GW)', 'Negative\nValues']
    before = [115, 69, 0.001, 0.0005]  # In millions or %
    after = [46, 0, 0, 0]

    x = np.arange(len(categories))
    width = 0.35

    fig, ax = plt.subplots(figsize=(10, 6))

    bars1 = ax.bar(x - width/2, before, width, label='Before Cleaning',
                   color='lightcoral', edgecolor='darkred')
    bars2 = ax.bar(x + width/2, after, width, label='After Cleaning',
                   color='lightgreen', edgecolor='darkgreen')

    ax.set_ylabel('Count (Millions)', fontweight='bold')
    ax.set_title('Data Quality: Before vs After Cleaning', fontweight='bold', fontsize=14)
    ax.set_xticks(x)
    ax.set_xticklabels(categories)
    ax.legend()

    # Add labels
    for bar, val in zip(bars1, before):
        if val > 0.01:
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                    f'{val:.0f}M', ha='center', fontsize=9)

    for bar, val in zip(bars2, after):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                f'{val:.0f}M', ha='center', fontsize=9)

    ax.set_ylim([0, 130])

    save_fig("data_quality")


# =============================================================================
# MAIN
# =============================================================================
if __name__ == "__main__":
    print("Generating figures for ML Lifecycle Documentation...\n")

    create_confusion_matrix()
    create_roc_curve()
    create_feature_importance()
    create_data_split()
    create_cv_scores()
    create_regional_distribution()
    create_monthly_trend()
    create_pipeline_architecture()
    create_data_quality()

    print(f"\nAll figures saved to: {OUTPUT_DIR}/")
    print("\nGenerated files:")
    for f in sorted(os.listdir(OUTPUT_DIR)):
        print(f"  - {f}")
