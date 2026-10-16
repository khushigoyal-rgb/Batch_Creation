#!/usr/bin/env python
# coding: utf-8
"""
Generate categories.json -- the FROZEN category vocabulary used by
0_3_feature_creation_cohort_updated_code.py.

Run this ONCE against a reference case-data CSV to "anchor" which category
values get their own feature columns. Reusing the SAME categories.json across
all cohorts keeps the feature matrix column-aligned cohort-to-cohort
(important if you train on one cohort and score another).

Rule (per facet):
  - > 20 unique values -> keep values with >= THRESHOLD records, plus "Miscellaneous"
  - <= 20 unique values -> keep all values
  ("Miscellaneous1" is normalized to "Miscellaneous" first.)

Usage:
  python make_categories_json.py                      # uses default reference CSV
  python make_categories_json.py --input <case.csv>   # anchor on a specific file
  python make_categories_json.py --threshold 10000
"""

import argparse
import json
import os
import pandas as pd

_QFC_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(_QFC_DIR, "data")
CATEGORIES_JSON = os.path.join(_QFC_DIR, "categories.json")

FACETS = ['qrc', 'case_origin', 'closure_remarks', 'type', 'sub_type', 'case_category']
DEFAULT_THRESHOLD = 10000


def create_anchor_map(df_input, facet_list, threshold_val):
    anchor_map = {}
    temp_df = df_input[facet_list].copy()
    print(f"Analyzing {len(temp_df)} records to freeze the mapping...")

    for col in facet_list:
        # Normalize: merge Miscellaneous1 into Miscellaneous
        temp_df[col] = temp_df[col].replace('Miscellaneous1', 'Miscellaneous')

        unique_vals = temp_df[col].nunique()
        if unique_vals > 20:
            counts = temp_df[col].value_counts()
            retained = counts[counts >= threshold_val].index.tolist()
            if "Miscellaneous" not in retained:
                retained.append("Miscellaneous")
            anchor_map[col] = retained
            print(f"Facet '{col}': {unique_vals} unique -> retained {len(retained)} (threshold >= {threshold_val}).")
        else:
            retained = temp_df[col].unique().tolist()
            anchor_map[col] = retained
            print(f"Facet '{col}': {unique_vals} unique -> retained all (count <= 20).")

    return anchor_map


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=None,
                    help="reference case-data CSV to anchor on "
                         "(default: a dim_case_data_*_ids.csv under ./data)")
    ap.add_argument("--threshold", type=int, default=DEFAULT_THRESHOLD,
                    help="min record count to keep a value when a facet has >20 uniques")
    args = ap.parse_args()

    input_path = args.input
    if input_path is None:
        import glob
        candidates = sorted(glob.glob(os.path.join(DATA_DIR, "dim_case_data_*_ids.csv")))
        if not candidates:
            raise SystemExit(
                "No reference CSV given and no dim_case_data_*_ids.csv found under "
                f"{DATA_DIR}. Run 01_cohort_data_pull.py first or pass --input.")
        input_path = candidates[0]
        print("using reference CSV:", input_path)

    df = pd.read_csv(input_path)
    print("reference shape:", df.shape)

    final_map = create_anchor_map(df, FACETS, args.threshold)

    print("\n--- FINAL ANCHOR MAP ---")
    for facet, cats in final_map.items():
        print(f"{facet}: {cats}")

    with open(CATEGORIES_JSON, "w") as f:
        json.dump(final_map, f, indent=4)
    print("\nwrote:", CATEGORIES_JSON)


if __name__ == '__main__':
    main()
