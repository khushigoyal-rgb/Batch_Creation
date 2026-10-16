#!/usr/bin/env python
# coding: utf-8
"""
ONUS (CRN) delinquency/payment feature creation.

Runs AFTER base_selection/01_04_sanoj_base_selection.py, which writes:
    base_selection/Data/df_complete_base_with_partner_filtered.csv

This script:
  1. reads that base, filters to the cohort month,
  2. pulls loan (dim_loan) + payment-schedule (fact_repayment_schedule) data
     from Redshift for the cohort opp_ids,
  3. runs FIFO delinquency logic and builds onus payment features,
  4. writes intermediates to onus/data and the feature output to onus/features.

Run from feature_creation/onus/ (relative paths are anchored to this file).
"""

import os
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import psycopg2
from tqdm import tqdm
from dateutil.relativedelta import relativedelta

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)


# ---------------------------------------------------------------------------
# Configuration (paths anchored to this script's location)
# ---------------------------------------------------------------------------
_ONUS_DIR = os.path.dirname(os.path.abspath(__file__))                 # .../feature_creation/onus
_REPO_ROOT = os.path.abspath(os.path.join(_ONUS_DIR, "..", ".."))      # .../khudse

# Input base produced by base_selection/01_04_sanoj_base_selection.py
BASE_CSV = os.path.join(_REPO_ROOT, "base_selection", "Data",
                        "df_complete_base_with_partner_filtered.csv")
# Local onus I/O dirs
DATA_DIR = os.path.join(_ONUS_DIR, "data")
FEATURES_DIR = os.path.join(_ONUS_DIR, "features")

# ---------------------------------------------------------------------------
# Cohort (the one knob to change per run) -- everything below is derived from it.
# Must match the COHORT used in base_selection/01_04_sanoj_base_selection.py.
# ---------------------------------------------------------------------------
COHORT = "jan_26"
_MON = {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12}
_ms, _yy = COHORT.split("_")
_cohort_month = datetime(2000 + int(_yy), _MON[_ms[:3].lower()], 1)

# df['year_month'] filter value, e.g. "2026-01"
COHORT_YEAR_MONTH = _cohort_month.strftime("%Y-%m")
# reference "today" for delinquency/POS = last day of the month BEFORE the
# cohort month, in %d-%m-%Y (e.g. jan_26 -> "31-12-2025")
SNAPSHOT_DATE = (_cohort_month - timedelta(days=1)).strftime("%d-%m-%Y")
# short cohort tag used in filenames, e.g. "jan26"
_COHORT_TAG = f"{_ms[:3].lower()}{_yy}"

OPP_FILE = os.path.join(DATA_DIR, f"opp_df_oot_sf_{_COHORT_TAG}.csv")
PAYMENT_FILE = os.path.join(DATA_DIR, f"payment_df_oot_sf_{_COHORT_TAG}.csv")
FEATURES_OUT = os.path.join(
    FEATURES_DIR,
    f"final_feats_onus_oot_sf_{_COHORT_TAG}_with_3+_and_7+_dpd_additional_features.csv")

db_params = {
    'dbname': 'dwh_dmi',
    'user': 'hemant.agarwal@dmifinance.in',
    'password': 'HemantAgarwal@123',
    'host': 'dmi-prod.cqmjwdcqo0bv.ap-south-1.redshift.amazonaws.com',
    # 'host': '127.0.0.1',
    'port': '5439',
}

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(FEATURES_DIR, exist_ok=True)


def split(list_a, chunk_size):
    for i in tqdm(range(0, len(list_a), chunk_size)):
        yield list_a[i:i + chunk_size]


# ---------------------------------------------------------------------------
# FIFO pre-processing
# ---------------------------------------------------------------------------
def fifo(df_opp_path, df_payment_path, date):
    df_opp = pd.read_csv(df_opp_path)
    df_payments = pd.read_csv(df_payment_path)
    current_date = pd.Timestamp(date)

    previous_month_date = current_date.replace(day=20) - relativedelta(months=1)
    previous_month_timestamp = pd.Timestamp(previous_month_date)
    current_date_timestamp = pd.Timestamp(current_date)

    # fill payment_received_date__c null values and create cols list
    today_str = current_date.strftime('%Y-%m-%d')

    # to select ids whoes closedate was before previous_month_date
    print('previous_month_date', previous_month_date)
    # select all-due dates before current_date
    print('current_date_timestamp', current_date_timestamp)

    df_opp['createddate_opp'] = pd.to_datetime(df_opp['createddate_opp'])
    df_opp['closedate'] = pd.to_datetime(df_opp['closedate'])
    df_opp = df_opp[~df_opp['stagename'].isin(['Canceled', 'Rejected', 'Disbursement Approved', 'Restructured', 'InitialApproach', 'Credit Approved'])]
    df_opp['maturity_date__c'] = pd.to_datetime(df_opp['maturity_date__c'], format='%Y-%m-%d')
    df_opp['actual_maturity_date__c'] = pd.to_datetime(df_opp['actual_maturity_date__c'], format='%Y-%m-%d')
    df_opp['closedate'] = pd.to_datetime(df_opp['closedate'], format='%Y-%m-%d')
    df_opp = df_opp[df_opp['closedate'] <= previous_month_timestamp]

    df_opps_payments = pd.merge(df_payments, df_opp, left_on='opportunity__c', right_on='opp_id', how='inner')
    df_opps_payments['date__c'] = pd.to_datetime(df_opps_payments['date__c'])
    df_opps_payments.sort_values(by=['opp_id', 'date__c'], inplace=True)
    df_opps_payments['closedate'] = pd.to_datetime(df_opps_payments['closedate'])
    df_opps_payments = df_opps_payments[df_opps_payments['date__c'] < current_date_timestamp]

    print('max_date__c', df_opps_payments['date__c'].max())
    print('Maximum due date(date__c) should not more than today date')

    df_opps_payments['payment_received_date__c'] = pd.to_datetime(df_opps_payments['payment_received_date__c'])
    df_opps_payments.sort_values(by=['opp_id', 'date__c'], inplace=True)
    df_opps_payments.reset_index(drop=True, inplace=True)

    df_opps_payments['payment_received_date__c'] = np.where((df_opps_payments['stagename'].isin(['Matured', 'Matured Closed'])) & (df_opps_payments['payment_received_date__c'].isnull()), df_opps_payments.actual_maturity_date__c, df_opps_payments.payment_received_date__c)
    df_opps_payments['payment_received_date__c'].fillna(pd.to_datetime(today_str), inplace=True)

    ps_data_date__c = df_opps_payments.drop(['payment_received_date__c'], axis=1)
    ps_data_prd = df_opps_payments[['opportunity__c', 'payment_received_date__c']]

    # sorting each dataframe
    ps_data_prd.sort_values(by=['opportunity__c', 'payment_received_date__c'], inplace=True)
    ps_data_date__c.sort_values(['opportunity__c', 'date__c'], inplace=True)

    ps_data_prd.reset_index(drop=True, inplace=True)
    ps_data_date__c.reset_index(drop=True, inplace=True)
    df_opps_payments = pd.concat([ps_data_date__c, ps_data_prd], ignore_index=False, axis=1)
    df_opps_payments_2 = df_opps_payments[['opp_id', 'date__c',
                                           'payment_received_date__c',
                                           'actual_maturity_date__c', 'loan_tenor_in_month__c',
                                           'sanction_amount__c', 'net_payment__c', 'closingprincipalbal__c', 'stagename']].copy(deep=True)

    df_opps_payments_2.reset_index(drop=True, inplace=True)
    df_opps_payments_final = df_opps_payments_2.copy(deep=True)
    df_opps_payments_final.sort_values(by=['opp_id', 'date__c'], inplace=True)
    df_opps_payments_final_2 = df_opps_payments_final.copy(deep=True)
    df_opps_payments_final_2['delinquency'] = (df_opps_payments_final_2['payment_received_date__c'] - df_opps_payments_final_2['date__c']).dt.days
    df_opps_payments_final_w_date = pd.merge(df_opps_payments_final_2, df_opps_payments[['opp_id', 'closedate']].drop_duplicates(), on='opp_id', how='inner')
    df_opps_payments_final_w_date.sort_values(['opp_id', 'date__c'], ascending=True, inplace=True)
    df_opps_payments_final_w_date['payment_number'] = df_opps_payments_final_w_date.groupby(['opp_id'])['date__c'].cumcount() + 1
    df_opps_payments_final_w_date['closedate'] = pd.to_datetime(df_opps_payments_final_w_date['closedate'])
    df_opps_payments_final_w_date['closedate'] = pd.to_datetime(df_opps_payments_final_w_date['closedate'], utc=True)

    df_opps_payments_final_w_date['payment_received_date__c'] = pd.to_datetime(df_opps_payments_final_w_date['payment_received_date__c'])

    df_x = df_opps_payments_final_w_date[df_opps_payments_final_w_date['payment_received_date__c'] > today_str]
    df_y = df_opps_payments_final_w_date[~(df_opps_payments_final_w_date['payment_received_date__c'] > today_str)]
    df_x['payment_received_date__c'] = today_str
    df_opps_payments_final_w_date = pd.concat([df_x, df_y])
    df_opps_payments_final_w_date['payment_received_date__c'] = pd.to_datetime(df_opps_payments_final_w_date['payment_received_date__c'])
    df_opps_payments_final_w_date['date__c'] = pd.to_datetime(df_opps_payments_final_w_date['date__c'])

    df_opps_payments_final_w_date['delinquency'] = (df_opps_payments_final_w_date['payment_received_date__c'] - df_opps_payments_final_w_date['date__c']).dt.days
    df_opps_payments_final_w_date.reset_index(drop=True, inplace=True)
    return df_opps_payments_final_w_date


# ---------------------------------------------------------------------------
# Delinquency / payment feature generation
# ---------------------------------------------------------------------------
def generate_delinquency_features(df_opp_path, df_payment_path, date, mob_months=[1, 3, 6, 9, 12]):
    df = fifo(df_opp_path, df_payment_path, date).copy()

    df['closedate'] = pd.to_datetime(df['closedate'], errors='coerce').dt.tz_localize(None)
    df['payment_received_date__c'] = pd.to_datetime(df['payment_received_date__c'], errors='coerce')
    df['date__c'] = pd.to_datetime(df['date__c'], errors='coerce')

    # Replace negative delinquencies with 0
    df['delinquency'] = np.where(df['delinquency'] >= 0, df['delinquency'], 0)

    df['on_time_payment_flag'] = (df['delinquency'] == 0).astype(int)
    on_time_payment_ratio = df.groupby('opp_id').agg(
        on_time_payments=('on_time_payment_flag', 'sum'),
        total_payments=('opp_id', 'count')
    )

    on_time_payment_ratio['on_time_payment_ratio'] = (on_time_payment_ratio['on_time_payments'] / on_time_payment_ratio['total_payments']) * 100
    on_time_payment_ratio.reset_index(inplace=True)

    # --- Delinquency ratio flags ---
    for threshold in [0, 3, 7, 15, 30, 60, 90]:
        df[f'delinq_ever{threshold}plus_flag'] = (df['delinquency'] > threshold).astype(int)

    delinq_ratio = df.groupby('opp_id').agg(
        times_ever0plus=('delinq_ever0plus_flag', 'sum'),
        times_ever3plus=('delinq_ever3plus_flag', 'sum'),
        times_ever7plus=('delinq_ever7plus_flag', 'sum'),
        times_ever15plus=('delinq_ever15plus_flag', 'sum'),
        times_ever30plus=('delinq_ever30plus_flag', 'sum'),
        times_ever60plus=('delinq_ever60plus_flag', 'sum'),
        times_ever90plus=('delinq_ever90plus_flag', 'sum'),
        total_payments=('opp_id', 'count')
    )

    for threshold in [0, 3, 7, 15, 30, 60, 90]:
        delinq_ratio[f'ratio_{threshold}plus'] = (delinq_ratio[f'times_ever{threshold}plus'] / delinq_ratio['total_payments']) * 100
    # on time payments ratio
    delinq_ratio.reset_index(inplace=True)

    # --- Ever flags ---
    df_ever_flag = df.groupby('opp_id').agg({
        'sanction_amount__c': 'max',
        'payment_number': 'max',
        'delinquency': ['max', 'min', 'mean'],
        'loan_tenor_in_month__c': 'max'
    })
    df_ever_flag.columns = ['_'.join(col).strip() for col in df_ever_flag.columns.values]

    for threshold in [0, 3, 7, 15, 30, 60, 90]:
        df_ever_flag[f'ever{threshold}+'] = (df_ever_flag['delinquency_max'] > threshold).astype(int)

    # --- MOB flags ---
    reference_date = pd.to_datetime(date, utc=True)
    for months in mob_months:
        date_threshold = (reference_date - pd.DateOffset(months=months)).replace(day=21).tz_localize(None)
        df_mob = df[(df['payment_number'] <= months) & (df['closedate'] < date_threshold)]

        df_mob_flag = df_mob.groupby('opp_id').agg({
            'payment_number': 'max',
            'delinquency': ['max', 'min'],
            'loan_tenor_in_month__c': 'max'
        })
        df_mob_flag.columns = ['_'.join(col).strip() for col in df_mob_flag.columns.values]

        df_mob_flag_filtered = df_mob_flag[
            (df_mob_flag['payment_number_max'] == months) |
            (df_mob_flag['loan_tenor_in_month__c_max'] < months)
        ]

        for threshold in [0, 3, 7, 15, 30, 60, 90]:
            df_mob_flag_filtered[f'{months}mob{threshold}+'] = (df_mob_flag_filtered['delinquency_max'] > threshold).astype(int)

        df_mob_flag_filtered.drop(columns=[
            'payment_number_max', 'delinquency_max', 'delinquency_min', 'loan_tenor_in_month__c_max'
        ], inplace=True)

        df_ever_flag = df_ever_flag.merge(df_mob_flag_filtered, on='opp_id', how='left')

    # --- POS calculation ---
    df_pos = df.sort_values(['opp_id', 'date__c']).drop_duplicates('opp_id', keep='last')
    today = pd.to_datetime(date)
    today_str = today.strftime('%Y-%m-%d')

    df_pos['payment_received_date__c'] = df_pos['payment_received_date__c'].dt.strftime('%Y-%m-%d')
    df_pos['payment_received_date__c'] = np.where(df_pos['payment_received_date__c'] == today_str, np.nan, df_pos['payment_received_date__c'])

    df_pos['pos'] = np.where(
        df_pos['payment_received_date__c'].isnull(),
        df_pos['closingprincipalbal__c'] + df_pos['net_payment__c'],
        df_pos['closingprincipalbal__c']
    )

    df_final = pd.merge(df_pos[['opp_id', 'pos']], df_ever_flag, on='opp_id', how='left')

    # --- Last 3 payment flags ---
    latest_3 = df.sort_values(['payment_received_date__c', 'date__c'], ascending=False).groupby('opp_id').head(3)
    latest_3_grouped = latest_3.groupby('opp_id').agg({
        'delinquency': ['sum', 'max', 'mean', 'min']
    }).reset_index()
    latest_3_grouped.columns = ['opp_id' if col == 'opp_id' else f'last_3_pay_{col[1]}_{col[0]}' for col in latest_3_grouped.columns.values]
    latest_3_grouped['latest_3_ever0+'] = (latest_3_grouped['last_3_pay_max_delinquency'] > 0).astype(int)
    latest_3_grouped['latest_3_ever3+'] = (latest_3_grouped['last_3_pay_max_delinquency'] > 3).astype(int)
    latest_3_grouped['latest_3_ever7+'] = (latest_3_grouped['last_3_pay_max_delinquency'] > 7).astype(int)
    latest_3_grouped['latest_3_ever15+'] = (latest_3_grouped['last_3_pay_max_delinquency'] > 15).astype(int)
    latest_3_grouped['latest_3_ever30+'] = (latest_3_grouped['last_3_pay_max_delinquency'] > 30).astype(int)

    # --- Final merge ---
    df_final_fet = df_final.merge(latest_3_grouped, left_on='opp_id', right_on='last_3_pay__opp_id', how='left')
    final_payment_feats = df_final_fet.merge(delinq_ratio, on='opp_id', how='left')
    final_payment_feats.drop('last_3_pay__opp_id', axis=1, inplace=True)
    final_payment_feats.rename(columns={'sanction_amount__c_max': 'sanction_amt',
                                        'loan_tenor_in_month__c_max': 'loan_tenor'}, inplace=True)
    final_features_df = pd.merge(final_payment_feats, on_time_payment_ratio[['opp_id', 'on_time_payment_ratio']], on='opp_id', how='left')
    final_features_df['pos_percentage'] = final_features_df['pos'] / final_features_df['sanction_amt']
    return final_features_df


# ---------------------------------------------------------------------------
# Data pull from Redshift
# ---------------------------------------------------------------------------
def fetch_opp_and_payment(conn, opp_unique_ids):
    """Pull dim_loan attributes and fact_repayment_schedule for the cohort ids."""
    opp_df = pd.DataFrame()
    for id_subset in split(opp_unique_ids, 100000):
        qry_opp_data = f"""SELECT
        O.loan_id as opp_id,
        O.created_date as createddate_opp,
        O.stage as stagename,
        O.maturity_date as maturity_date__c,
        O.actual_maturity_date as actual_maturity_date__c,
        O.parent_loan_id as parent_opportunity__c,
        O.product_type as product_type__c,
        O.closed_date as closedate,
        O.loantenor_in_month as loan_tenor_in_month__c,
        O.sanction_amount as sanction_amount__c
    FROM dwh_dmi.lms.dim_loan O
    WHERE O.loan_id in  ({','.join([f"'{x}'" for x in id_subset])})
    """
        df_id_temp = pd.read_sql(qry_opp_data, conn)
        opp_df = pd.concat([df_id_temp, opp_df])

    payment_df = pd.DataFrame()
    for id_subset in split(opp_unique_ids, 100000):
        qry_payment_data = f"""select
                    ps.ps_id as id_pay_sch,
                    ps.created_date as createddate_paysch ,
                    ps.loan_id as opportunity__c,
                    ps.net_pmt as net_payment__c,
                    ps.emi_due_date as date__c,
                    ps.net_payment as netpayment__c,
                    CAST(ps.pmt_rec_date AS VARCHAR) as payment_received_date__c,
                    ps.closingprincipal_bal_pos as closingprincipalbal__c
                FROM
                    dwh_dmi.lms.fact_repayment_schedule as ps
                    where loan_id in ({','.join([f"'{x}'" for x in id_subset])})
                    AND (ps.ps_id not in (select distinct record_id from lms.dim_purge_records where object_name = 'Payment Schedule'))
                """
        df_id_temp = pd.read_sql(qry_payment_data, conn)
        payment_df = pd.concat([df_id_temp, payment_df])

    return opp_df, payment_df


def main():
    # 1. read base + filter to cohort month
    df = pd.read_csv(BASE_CSV)
    print("base df shape:", df.shape)

    df_f = df[df['year_month'] == COHORT_YEAR_MONTH]
    print(f"cohort ({COHORT_YEAR_MONTH}) df_f shape:", df_f.shape)
    print("partner_name value_counts:\n", df_f['partner_name'].value_counts())

    opp_unique_ids = df_f['opp_id'].to_list()
    print("opp_unique_ids len:", len(opp_unique_ids))

    # 2. pull loan + payment data from Redshift
    conn = psycopg2.connect(**db_params)
    opp_df, payment_df = fetch_opp_and_payment(conn, opp_unique_ids)
    print("payment_df opp nunique:", payment_df['opportunity__c'].nunique(),
          "| df_f shape:", df_f.shape, "| opp_df shape:", opp_df.shape)

    opp_df.to_csv(OPP_FILE, index=None)
    payment_df.to_csv(PAYMENT_FILE, index=None)
    print("wrote:", OPP_FILE)
    print("wrote:", PAYMENT_FILE)
    print("payment_df head:\n", payment_df[:3])

    # 3. sanity read-back
    opps_df = pd.read_csv(OPP_FILE)
    payments_df = pd.read_csv(PAYMENT_FILE)
    print("read-back opps_df shape:", opps_df.shape,
          "| payments opp nunique:", payments_df['opportunity__c'].nunique())

    # 4. generate features
    final_feats = generate_delinquency_features(OPP_FILE, PAYMENT_FILE, SNAPSHOT_DATE,
                                                mob_months=[1, 3, 6, 9, 12])
    print("final_feats head:\n", final_feats[:3])
    print("delinquency_max max:", final_feats['delinquency_max'].max(),
          "| last_3_pay_max_delinquency max:", final_feats['last_3_pay_max_delinquency'].max())
    print("rows with last_3_pay_max_delinquency > 0:",
          final_feats[final_feats['last_3_pay_max_delinquency'] > 0].shape)

    # 5. filter: keep last-3 max delinquency <= 30 and non-zero POS
    final_feats_df = final_feats[final_feats['last_3_pay_max_delinquency'] <= 30]
    print("after last_3 <= 30 filter:", final_feats_df.shape)
    final_feats_df = final_feats_df[final_feats_df['pos'] != 0]
    print("pos describe:\n", final_feats_df['pos'].describe())
    print("final_feats columns:\n", list(final_feats.columns))
    print("final_feats_df shape:", final_feats_df.shape)
    print("final_feats_df head:\n", final_feats_df[:3])
    print("delinquency_min min:", final_feats_df['delinquency_min'].min())

    # 6. write features
    final_feats_df.to_csv(FEATURES_OUT, index=None)
    print("wrote:", FEATURES_OUT)
    print("final_feats_df shape:", final_feats_df.shape)
    print("final_feats_df opp nunique:", final_feats_df['opp_id'].nunique())

    # ids from the cohort base that did NOT make it into the final features
    missing = df_f[~df_f['opp_id'].isin(final_feats_df['opp_id'].tolist())]
    print("cohort opp_ids missing from final features:", missing.shape)

    return final_feats_df


if __name__ == '__main__':
    main()
