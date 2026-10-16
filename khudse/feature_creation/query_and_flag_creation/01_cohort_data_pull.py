#!/usr/bin/env python
# coding: utf-8
"""
Step 1 of query/flag/feature pipeline: cohort case-data pull.

Runs AFTER base_selection/01_04_sanoj_base_selection.py. It:
  1. reads the base, filters to the cohort month,
  2. maps cohort opp_ids -> loan_number (dim_loan),
  3. pulls dim_cases (customer service / query cases) for those loans,
  4. writes the case data for the flag/feature steps (0_2, 0_3).

Run from feature_creation/query_and_flag_creation/ (paths anchored to this file).
"""

import os
from datetime import datetime
import pandas as pd
import psycopg2
from tqdm import tqdm

pd.set_option('display.max_columns', None)


# ---------------------------------------------------------------------------
# Cohort (one knob) + paths (anchored to this script's location)
# ---------------------------------------------------------------------------
COHORT = "jan_26"
_MON = {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12}
_ms, _yy = COHORT.split("_")
_cohort_month = datetime(2000 + int(_yy), _MON[_ms[:3].lower()], 1)
COHORT_YEAR_MONTH = _cohort_month.strftime("%Y-%m")        # e.g. "2026-01"
_COHORT_TAG = f"{_ms[:3].lower()}{_yy}"                     # e.g. "jan26"

_QFC_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_QFC_DIR, "..", ".."))

BASE_CSV = os.path.join(_REPO_ROOT, "base_selection", "Data",
                        "df_complete_base_with_partner_filtered.csv")
DATA_DIR = os.path.join(_QFC_DIR, "data")

OPP_LOAN_FILE = os.path.join(DATA_DIR, f"opp_id_with_loan_number_{_COHORT_TAG}_ids.csv")
CASE_DATA_FILE = os.path.join(DATA_DIR, f"dim_case_data_{_COHORT_TAG}_ids.csv")

db_params = {
    'dbname': 'dwh_dmi',
    'user': 'hemant.agarwal@dmifinance.in',
    'password': 'HemantAgarwal@123',
    'host': 'dmi-prod.cqmjwdcqo0bv.ap-south-1.redshift.amazonaws.com',
    # 'host': '127.0.0.1',
    'port': '5439',
}

os.makedirs(DATA_DIR, exist_ok=True)


def split(list_a, chunk_size):
    for i in tqdm(range(0, len(list_a), chunk_size)):
        yield list_a[i:i + chunk_size]


def main():
    df = pd.read_csv(BASE_CSV)
    print("base df shape:", df.shape)

    df_f = df[df['year_month'] == COHORT_YEAR_MONTH]
    print(f"cohort ({COHORT_YEAR_MONTH}) df_f shape:", df_f.shape)
    print("partner_name value_counts:\n", df_f['partner_name'].value_counts())

    conn = psycopg2.connect(**db_params)

    opp_unique_ids = df_f['opp_id'].to_list()
    print("opp_unique_ids sample:", opp_unique_ids[:3], "| len:", len(opp_unique_ids))

    # opp_id -> loan_number mapping from dim_loan
    opp_df = pd.DataFrame()
    for id_subset in split(opp_unique_ids, 100000):
        qry_opp_data = f"""SELECT
        O.loan_id as opp_id,
        O.loan_number,
        O.loan_type
    FROM dwh_dmi.lms.dim_loan O
    WHERE O.loan_id in  ({','.join([f"'{x}'" for x in id_subset])})
    """
        df_id_temp = pd.read_sql(qry_opp_data, conn)
        opp_df = pd.concat([df_id_temp, opp_df])

    opp_df.to_csv(OPP_LOAN_FILE, index=False)
    print("wrote:", OPP_LOAN_FILE)

    df_jan_opps_2 = pd.merge(df_f, opp_df, on='opp_id')
    print("df_jan_opps_2 agg:\n", df_jan_opps_2.agg(['count', 'nunique']))

    unique_loan_number = df_jan_opps_2['loan_number'].to_list()
    print("unique_loan_number len:", len(unique_loan_number))

    # dim_cases keyed by loan_number
    cases_df = pd.DataFrame()
    for id_subset in split(unique_loan_number, 100000):
        qry_case_data = f"""select *
        from lms.dim_cases dc
        where dc.loan_number in  ({','.join([f"'{x}'" for x in id_subset])})
    """
        df_id_temp = pd.read_sql(qry_case_data, conn)
        cases_df = pd.concat([cases_df, df_id_temp])

    cases_df.to_csv(CASE_DATA_FILE, index=False)
    print("wrote:", CASE_DATA_FILE)
    print("cases_df loan_number nunique:", cases_df['loan_number'].nunique())

    # part_2: cases associated via associated_loan where loan_number is NULL
    unique_opp_id = df_jan_opps_2['opp_id'].to_list()
    print("unique_opp_id len:", len(unique_opp_id))

    cases_df_2 = pd.DataFrame()
    for id_subset in split(unique_loan_number, 100000):
        qry_case_data = f"""select *
        from lms.dim_cases dc
        where dc.associated_loan in  ({','.join([f"'{x}'" for x in id_subset])})
        and dc.loan_number IS NULL
    """
        df_id_temp = pd.read_sql(qry_case_data, conn)
        cases_df_2 = pd.concat([cases_df_2, df_id_temp])

    print("cases_df_2 shape:", cases_df_2.shape)
    return cases_df, cases_df_2


if __name__ == '__main__':
    main()
