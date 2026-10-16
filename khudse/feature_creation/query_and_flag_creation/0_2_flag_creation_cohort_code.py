#!/usr/bin/env python
# coding: utf-8
"""
Step 2: pre-closure QUERY FLAG creation.

Reads the cohort case data (from 01_cohort_data_pull.py), keeps cases created in
the 90 days AFTER the snapshot date, and flags loans whose cases match known
pre-closure (qrc, type, sub_type, case_status, case_category) combinations.

Output: one row per associated_loan with a preclosure_flag (0/1).
Run from feature_creation/query_and_flag_creation/.
"""

import os
from datetime import datetime
import pandas as pd

# ---------------------------------------------------------------------------
# Cohort (one knob) + paths (anchored to this script's location)
# ---------------------------------------------------------------------------
COHORT = "jan_26"
_MON = {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12}
_ms, _yy = COHORT.split("_")
_cohort_month = datetime(2000 + int(_yy), _MON[_ms[:3].lower()], 1)
_COHORT_TAG = f"{_ms[:3].lower()}{_yy}"                         # e.g. "jan26"
# snapshot = last day of the month before the cohort month, as %Y-%m-%d
SNAPSHOT_DATE_STR = (_cohort_month - pd.Timedelta(days=1)).strftime("%Y-%m-%d")

_QFC_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(_QFC_DIR, "data")
CASE_DATA_FILE = os.path.join(DATA_DIR, f"dim_case_data_{_COHORT_TAG}_ids.csv")
FLAG_OUT = os.path.join(DATA_DIR, f"updated_{_COHORT_TAG}_customer_query_flag.csv")

# Valid (qrc, type, sub_type, case_status, case_category) combos = pre-closure intent
PRECLOSURE_COMBOS = [
    ('Request', 'Pre-closure Related', 'Loan Pre-closure Request', 'Closed', 'Preclosure Shared'),
    ('Request', 'Pre-closure Related', 'Loan Pre-closure Request', 'Closed', 'Not Elgible'),
    ('Request', 'Pre-closure Related', 'Loan Pre-closure Request', 'Closed', 'Not Contacted-Pre-closure shared'),
    ('Request', 'Pre-closure Related', 'Alternate Mode', 'Closed Resolved', 'Details Uploaded'),
    ('Request', 'Pre-closure Related', 'Loan Pre-closure Request', 'Closed', 'WA-Customer Retained'),
    ('Request', 'Pre-closure Related', 'Loan Pre-closure Request', 'Closed', 'RD-Customer Retained'),
    ('Request', 'Pre-closure Related', 'Loan Pre-closure Request', 'Closed', 'Customer Not Retained'),
    ('Query', 'Pre-closure Related', 'Loan Pre-closure', 'Closed', 'Pre-closure Query'),
    ('Request', 'Pre-closure Related', 'Alternate Mode', 'Closed', 'Details Uploaded'),
    ('Request', 'Pre-closure Related', 'Loan Pre-closure Request', 'Closed Resolved', 'Preclosure Shared'),
    ('Request', 'Pre-closure Related', 'Loan Pre-closure Request', 'Closed Resolved', 'Not Contacted-Pre-closure shared'),
    ('Request', 'Pre-closure Related', 'Loan Pre-closure Request', 'Closed', 'Customer Retained'),
    ('Request', 'Pre-closure Related', 'Loan Pre-closure Request', 'Closed', 'Error in Pre-closure'),
    ('Request', 'Pre-closure Related', 'Alternate Mode', 'Closed', 'Pending with customer'),
    ('Request', 'Pre-closure Related', 'Loan Pre-closure Request', 'Closed', 'Customer Not Convinced'),
    ('Request', 'Pre-closure Related', 'Loan Pre-closure Request', 'Closed Resolved', 'Customer Not Retained'),
    ('Request', 'Pre-closure Related', 'Alternate Mode', 'Closed Resolved', 'Pending with customer'),
    ('Request', 'Pre-closure Related', 'Loan Pre-closure Request', 'Closed', 'Request In-progress'),
]


def main():
    df_data = pd.read_csv(CASE_DATA_FILE)
    print("case data shape:", df_data.shape)
    print("head:\n", df_data.head())
    print("tail:\n", df_data.tail(10))

    snap_date = pd.to_datetime(SNAPSHOT_DATE_STR)
    max_date = snap_date + pd.Timedelta(days=90)
    print("snapshot:", SNAPSHOT_DATE_STR, "| max_date (snap+90d):", max_date)

    df = df_data.copy()
    df['createddate'] = pd.to_datetime(df['createddate'])
    df = df[df['createddate'] > snap_date]
    df = df[df['createddate'] <= max_date]
    print("cases in (snap, snap+90d] window:", df.shape)

    print("columns:", list(df.columns))
    # NOTE: the original notebook copied this to clipboard for manual review;
    # not available in a headless script -> left for reference.
    # df[['case_category','qrc','type','sub_type','case_status']].drop_duplicates().to_clipboard()
    print("case_category value_counts (top 50):\n",
          pd.DataFrame(df['case_category'].value_counts()).head(50))
    print("qrc value_counts:\n", df['qrc'].value_counts())
    print("type value_counts:\n", df['type'].value_counts())

    _closure = df[df['type'].isin(['Pre-closure Related', 'Loan Closure Request'])]
    print("closure-type qrc value_counts:\n", _closure['qrc'].value_counts())
    print("closure-type sub_type value_counts:\n", _closure['sub_type'].value_counts())
    print("sub_type value_counts (top 50):\n",
          pd.DataFrame(df['sub_type'].value_counts()).head(50))
    print("closure-type associated_loan nunique:", _closure['associated_loan'].nunique())

    combo_set = set(PRECLOSURE_COMBOS)
    df['preclosure_flag'] = df[['qrc', 'type', 'sub_type', 'case_status', 'case_category']].apply(
        lambda row: 1 if tuple(row) in combo_set else 0, axis=1
    )
    print("preclosure_flag value_counts:\n", df['preclosure_flag'].value_counts())

    df_2 = df[['associated_loan', 'preclosure_flag']].drop_duplicates().reset_index(drop=True)
    print("df_2 shape:", df_2.shape)

    df_2_non_null = df_2[df_2['associated_loan'].notnull()]
    df_2_non_null.to_csv(FLAG_OUT, index=False)
    print("wrote:", FLAG_OUT, "| rows:", df_2_non_null.shape[0])

    return df_2_non_null


if __name__ == '__main__':
    main()
