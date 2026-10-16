#!/usr/bin/env python
# coding: utf-8
"""
Step 3: pre-closure QUERY FEATURE creation.

Reads the cohort case data (from 01_cohort_data_pull.py) and a frozen category
map (categories.json), then engineers a wide per-loan feature table of
customer-service case behaviour BEFORE the snapshot date: numerical stats
(case age, reply count), categorical volume/mix/recency/friction across time
windows, meta diversity/loop features, and velocity ratios.

Output: ./data/updated_<cohort>_customer_query_features.csv
Run from feature_creation/query_and_flag_creation/.
"""

import os
import json
from datetime import datetime
import gc
import numpy as np
import pandas as pd

pd.set_option('display.max_columns', None)


# ---------------------------------------------------------------------------
# Cohort (one knob) + paths (anchored to this script's location)
# ---------------------------------------------------------------------------
COHORT = "jan_26"
_MON = {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12}
_ms, _yy = COHORT.split("_")
_cohort_month = datetime(2000 + int(_yy), _MON[_ms[:3].lower()], 1)
_COHORT_TAG = f"{_ms[:3].lower()}{_yy}"                          # e.g. "jan26"
# snapshot = last day of the month before the cohort month, as %Y-%m-%d
SNAPSHOT_DATE_STR = (_cohort_month - pd.Timedelta(days=1)).strftime("%Y-%m-%d")

_QFC_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(_QFC_DIR, "data")
CATEGORIES_JSON = os.path.join(_QFC_DIR, "categories.json")
CASE_DATA_FILE = os.path.join(DATA_DIR, f"dim_case_data_{_COHORT_TAG}_ids.csv")
FEATURES_OUT = os.path.join(DATA_DIR, f"updated_{_COHORT_TAG}_customer_query_features.csv")

os.makedirs(DATA_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# NOTE: categories.json is produced ONCE by the (commented) anchor-map builder
# below. It freezes which category values are kept per facet (>20 unique ->
# keep those with >= THRESHOLD records + "Miscellaneous"; else keep all).
# Re-run this block only when you need to regenerate categories.json.
# ---------------------------------------------------------------------------
# THRESHOLD = 10000
# facets = ['qrc', 'case_origin', 'closure_remarks', 'type', 'sub_type', 'case_category']
#
# def create_anchor_map(df_input, facet_list, threshold_val):
#     anchor_map = {}
#     temp_df = df_input[facet_list].copy()
#     print(f"Analyzing {len(temp_df)} records to freeze the mapping...")
#     for col in facet_list:
#         temp_df[col] = temp_df[col].replace('Miscellaneous1', 'Miscellaneous')
#         unique_vals = temp_df[col].nunique()
#         if unique_vals > 20:
#             counts = temp_df[col].value_counts()
#             retained = counts[counts >= threshold_val].index.tolist()
#             if "Miscellaneous" not in retained:
#                 retained.append("Miscellaneous")
#             anchor_map[col] = retained
#         else:
#             anchor_map[col] = temp_df[col].unique().tolist()
#     return anchor_map
#
# FINAL_CATEGORIES_MAP = create_anchor_map(df_data_jan26_ids, facets, THRESHOLD)
# with open(CATEGORIES_JSON, "w") as f:
#     json.dump(FINAL_CATEGORIES_MAP, f, indent=4)


def create_preclosure_features_v4(df_input, snapshot_date_str, FINAL_CATEGORIES_MAP):
    # --- 1. SETUP & LEAKAGE CONTROL ---
    snap_date = pd.to_datetime(snapshot_date_str)
    print(f"Engineering features for snapshot: {snapshot_date_str}")

    df = df_input.copy()
    # Convert dates immediately
    df['createddate'] = pd.to_datetime(df['createddate'])
    # Strict temporal filter
    df = df[df['createddate'] < snap_date]

    # Standardize Categories (Miscellaneous1 -> Miscellaneous)
    for col in FINAL_CATEGORIES_MAP.keys():
        df[col] = df[col].astype(str).replace('Miscellaneous1', 'Miscellaneous')
        df[col] = df[col].where(df[col].isin(FINAL_CATEGORIES_MAP[col]), "Miscellaneous")

    # Initialize Master Table with all unique loans
    loans = df['loan_number'].unique()
    master_features = pd.DataFrame(index=loans)
    master_features.index.name = 'loan_number'

    windows = {"7d": 7, "30d": 30, "60d": 60, "90d": 90, "180d": 180}
    ratio_pairs = [("7d", "30d"), ("30d", "60d"), ("30d", "90d"), ("30d", "180d"), ("60d", "180d"), ("90d", "180d")]

    # --- 2. NUMERICAL ROOTS (Mean & Max) ---
    print("Step 2: Calculating Numerical Stats...")
    for col in ["case_age_in_days", "reply_count"]:
        # Ensure columns are numeric
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # Lifetime (LTH)
        master_features[f'stat_{col}_mean_lth'] = df.groupby('loan_number')[col].mean()
        master_features[f'stat_{col}_max_lth'] = df.groupby('loan_number')[col].max()

        for label, days in windows.items():
            mask = df['createddate'] >= (snap_date - pd.Timedelta(days=days))
            win_df = df[mask]
            master_features[f'stat_{col}_mean_{label}'] = win_df.groupby('loan_number')[col].mean()
            master_features[f'stat_{col}_max_{label}'] = win_df.groupby('loan_number')[col].max()

    # --- 3. CATEGORICAL ROOTS (Vol, Mix, Recency, Friction) ---
    print("Step 3: Expanding Categorical Facets...")
    for facet, categories in FINAL_CATEGORIES_MAP.items():
        # Pre-calculate window counts for Mix calculation to save time
        win_counts = {
            label: df[df['createddate'] >= (snap_date - pd.Timedelta(days=days))].groupby('loan_number')[facet].count()
            for label, days in windows.items()
        }

        for cat in categories:
            clean_cat = f"{facet}_{str(cat).replace(' ', '_').lower()}"
            is_cat_mask = (df[facet] == cat)

            # Recency
            last_date = df[is_cat_mask].groupby('loan_number')['createddate'].max()
            master_features[f'days_since_{clean_cat}'] = (snap_date - last_date).dt.days

            for label, days in windows.items():
                win_mask = df['createddate'] >= (snap_date - pd.Timedelta(days=days))
                vol_col = f"vol_{clean_cat}_{label}"

                # Volume
                master_features[vol_col] = df[win_mask & is_cat_mask].groupby('loan_number')[facet].count()

                # Mix %
                master_features[f'mix_{clean_cat}_{label}'] = master_features[vol_col] / win_counts[label].replace(0, 1)

            # Friction (30d)
            mask_30 = df['createddate'] >= (snap_date - pd.Timedelta(days=30))
            master_features[f'frict_avg_age_{clean_cat}_30d'] = df[mask_30 & is_cat_mask].groupby('loan_number')['case_age_in_days'].mean()

        print(f"  Done: {facet}")
        gc.collect()

    # --- 4. META FEATURES (Diversity & Loops) ---
    print("Step 4: Calculating Meta Features...")
    mask_60 = df['createddate'] >= (snap_date - pd.Timedelta(days=60))
    df_60 = df[mask_60]

    master_features['unique_subtype_count_60d'] = df_60.groupby('loan_number')['sub_type'].nunique()
    master_features['unique_origin_count_60d'] = df_60.groupby('loan_number')['case_origin'].nunique()

    for col in ['sub_type', 'case_category']:
        loop_val = df_60.groupby(['loan_number', col]).size().reset_index(name='c')
        master_features[f'loop_{col}_60d'] = loop_val.groupby('loan_number')['c'].max()

    # --- 5. VELOCITY RATIOS (SAFE & DEFRAGMENTED) ---
    print("Step 5: Calculating True Velocity Ratios...")

    # Critical Checkpoint: Defragment dataframe blocks to drop PerformanceWarnings
    master_features = master_features.copy()

    # 1. Pure Pandas Safe Ratio for Case Age and Reply Counts
    for col in ["case_age_in_days", "reply_count"]:
        for w1, w2 in ratio_pairs:
            m1 = master_features[f'stat_{col}_mean_{w1}'].fillna(0)
            m2 = master_features[f'stat_{col}_mean_{w2}'].fillna(0)

            # Elegant math handle: div-by-zero maps safely without warnings
            master_features[f'ratio_{col}_{w1}_{w2}'] = m1.div(m2).fillna(0).replace([np.inf, -np.inf], 0)

    # 2. Pure Pandas Safe Ratio for Categorical Facet Volumes
    for facet, cats in FINAL_CATEGORIES_MAP.items():
        for cat in cats:
            clean_cat = f"{facet}_{str(cat).replace(' ', '_').lower()}"
            for w1, w2 in ratio_pairs:
                v1 = f"vol_{clean_cat}_{w1}"
                v2 = f"vol_{clean_cat}_{w2}"

                v1_series = master_features[v1].fillna(0)
                v2_series = master_features[v2].fillna(0)

                # Elegant math handle: 0/0 -> 0.0, 0/x -> 0.0, x/0 -> 0.0
                master_features[f'vel_{clean_cat}_{w1}_{w2}'] = v1_series.div(v2_series).fillna(0).replace([np.inf, -np.inf], 0)

    # Final cleanup conversion to float32 to compress RAM footprint
    return master_features.fillna(0).astype('float32')


def main():
    df_data = pd.read_csv(CASE_DATA_FILE)
    print("case data shape:", df_data.shape)
    print("head:\n", df_data.head())
    print("tail:\n", df_data.tail(10))

    if not os.path.exists(CATEGORIES_JSON):
        raise SystemExit(
            f"categories.json not found at {CATEGORIES_JSON}\n"
            "It is the frozen category vocabulary this step needs. Generate it ONCE "
            "(then reuse across cohorts) with:\n"
            "    python make_categories_json.py\n"
            "or anchor on a specific reference file:\n"
            "    python make_categories_json.py --input <case_data.csv> --threshold 10000")
    with open(CATEGORIES_JSON, 'r') as f:
        FINAL_CATEGORIES_MAP = json.load(f)
    print("loaded categories.json facets:", list(FINAL_CATEGORIES_MAP.keys()))

    master_features_final = create_preclosure_features_v4(
        df_data, SNAPSHOT_DATE_STR, FINAL_CATEGORIES_MAP)
    print("master_features_final shape:", master_features_final.shape)

    master_features_final.to_csv(FEATURES_OUT)
    print("wrote:", FEATURES_OUT)

    return master_features_final


if __name__ == '__main__':
    main()
