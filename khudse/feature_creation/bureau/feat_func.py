#how to run -> 
# cd feature_creation/bureau
# python run_parts.py --workers 4



import pandas as pd
import numpy as np
from tqdm import tqdm
import time
from datetime import datetime
from itertools import product
import psycopg2
import warnings
warnings.filterwarnings('ignore')
pd.set_option('max_colwidth', 500)
pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", None)
from collections import OrderedDict
from datetime import date
import json
import math
import os

# ---------------------------------------------------------------------------
# Data directories (anchored to this script's location so the process can be
# launched from ANY working directory / run in parallel per part).
# ---------------------------------------------------------------------------
_BUREAU_DIR = os.path.dirname(os.path.abspath(__file__))              # .../feature_creation/bureau
_REPO_ROOT = os.path.abspath(os.path.join(_BUREAU_DIR, "..", ".."))   # .../khudse

# SHARED, read-only inputs (same for every part) -- absolute so parallel
# workers (each in its own CWD) all resolve them correctly.
SCRUB_DIR = os.path.join(_REPO_ROOT, "Output")                        # scrub CSVs from scrub_data.py
SPLIT_IDS_DIR = os.path.join(_BUREAU_DIR, "split_ids")                # part_<N>.csv chunks

# SHARED output dir where each part writes its final part_<PART>_Final_features.parquet.
FINAL_FEATURES_DIR = os.path.join(_BUREAU_DIR, "data", "features")

# Which 50k-ID chunk (split_ids/part_<PART>.csv) this run processes.
# The whole pipeline is chunked for memory; run one part per process
# (see run_parts.py) so parts can run in parallel.
PART = int(os.environ.get("PART", 2))

# PER-PART working directory: every intermediate the body writes with a
# RELATIVE path (bare "overall_acc_feats.csv" and "../data/features/*.csv")
# lands here, isolated from other parts. We chdir into it so no body paths
# need changing. FEATURES_DIR therefore stays the relative "../data/features"
# the body already uses -> resolves to <WORK_DIR>/../data/features.
WORK_DIR = os.path.join(_BUREAU_DIR, "runs", f"part_{PART}")
FEATURES_DIR = "../data/features"

os.makedirs(SPLIT_IDS_DIR, exist_ok=True)
os.makedirs(FINAL_FEATURES_DIR, exist_ok=True)
os.makedirs(WORK_DIR, exist_ok=True)
os.makedirs(os.path.join(WORK_DIR, FEATURES_DIR), exist_ok=True)
os.chdir(WORK_DIR)


# In[4]:


# CRIF Account Mapping

def product_group_dict():

    product_group = {}


    product_group['secured'] = ["Tractor Loan", "Used Car Loan", "Auto Loan (Personal)", "Commercial Vehicle Loan", "Commercial Vehicle Loan", "Used Tractor Loan", "Gold Loan", "GECL Loan secured", "Priority Sector Gold Loan", "Housing Loan", "Pradhan Mantri Awas Yojana - CLSS", "Loan Against Bank Deposits", "Property Loan", "Loan Against Property", "Loan Against Shares / Securities", "Leasing", "Commercial Equipment Loan", "Construction Equipment Loan", "Equipment Loan", "Business Loan Against Bank Deposits", "Business Loan - Secured", "Secured Credit Card", "Kisan Credit Card", "Two-Wheeler Loan", "TW"]
    product_group['unsecured'] =  ["Business Loan - Income Generating", "Advances against Export Cash Incentives / Import Bills", "Commercial Loan", "GECL Loan unsecured", "Bank Guarantee", "Non-Funded Credit Facility", "Business Loan General", "Business Loan Priority Sector Small Business", "Business Loan Priority Sector Agriculture", "Business Loan Priority Sector Others", "Business Non-Funded Credit Facility General", "Business Non-Funded Credit Facility-Priority Sector- Small Business", "Business Non-Funded Credit Facility-Priority Sector-Agriculture", "Business Non-Funded Credit Facility-Priority Sector-Others", "Business Loan Unsecured", "Microfinance Business Loan", "Loan to Professional", "Credit Card", "Fleet Card", "Corporate Credit Card", "Charge Card", "Consumer Loan", "Education Loan", "Microfinance Housing Loan", "Microfinance Others", "Mudra Working Capital", "Mudra Loans ? Shishu / Kishor / Tarun", "Mudra Term Loan", "Mudra Loans - Shishu / Kishor / Tarun", "JLG Individual", "JLG Group", "SHG Individual", "SHG Group", "Mudra Loans – Shishu / Kishor / Tarun", "Seller Financing", "SHG Group- Govt", "SHG Intra- Group", "Microfinance Personal Loan", "Cash Credit", "Current Account", "Prime Minister Jaan Dhan Yojana - Overdraft", "Auto Overdraft", "Overdraft", "OD on Savings Account", "Prime Minister Jana Dhan Yojana - Overdraft", "Temporary Overdraft", "Other", "OTHER", "Term Loans", "Staff Loan", "Loan on Credit Card", "Individual", "Personal Loan", "Short Term Personal Loan", "Loan against Card", "Working Capital Loans", "Telco Broadband", "Telco Landline", "Telco Wireless", "Telco Wireless"]

    product_group['pl'] = ["Personal Loan", "Short Term Personal Loan", "Loan on Credit Card", "Individual"]
    product_group['cc'] = ['Credit Card' , 'Fleet Card' , 'Corporate Credit Card', 'Charge Card' ]
    product_group['secured_cc'] = ['Secured Credit Card', 'Kisan Credit Card']
    product_group['od'] = ["Cash Credit" , "Current Account" , "Prime Minister Jaan Dhan Yojana - Overdraft" , "Auto Overdraft" , "Overdraft" , "OD on Savings Account" , "Prime Minister Jana Dhan Yojana - Overdraft" , "Temporary Overdraft"]
    product_group['gl'] = ['Gold Loan', 'Priority Sector Gold Loan']
    product_group['hl'] = ['Housing Loan', 'Pradhan Mantri Awas Yojana - CLSS']
    product_group['lap'] = ['Property Loan', 'Loan Against Property']
    product_group['las'] = ["Loan Against Shares / Securities"]
    product_group['al'] = ["Tractor Loan", "Used Car Loan", "Auto Loan (Personal)",  "Commercial Vehicle Loan", "Commercial Vehicle Loan", "Used Tractor Loan"]
    product_group['twl'] = ["Two-Wheeler Loan", "TW"]
    product_group['cl'] = ['Consumer Loan']
    product_group['el'] = ['Education Loan']
    product_group['bl'] = ['Business Loan - Income Generating', 'Advances against Export Cash Incentives / Import Bills', 'Commercial Loan', 'GECL Loan unsecured', 'GECL Loan secured', 'Bank Guarantee', 'Non-Funded Credit Facility', 'Business Loan General', 'Business Loan Priority Sector Small Business', 'Business Loan Priority Sector Agriculture', 'Business Loan Priority Sector Others', 'Business Non-Funded Credit Facility General', 'Business Non-Funded Credit Facility-Priority Sector- Small Business', 'Business Non-Funded Credit Facility-Priority Sector-Agriculture', 'Business Non-Funded Credit Facility-Priority Sector-Others', 'Business Loan Unsecured', 'Microfinance Business Loan', "Loan to Professional"]
    product_group['secured_bl'] = ['Commercial Equipment Loan' , 'Construction Equipment Loan', 'Business Loan - Secured', 'Equipment Loan', 'Business Loan Against Bank Deposits']
    product_group['scf'] = ['Working Capital Loans']
    product_group['mf'] = ["Microfinance Housing Loan", "Microfinance Others", "Microfinance Other" , "Mudra Working Capital" , "Mudra Loans ? Shishu / Kishor / Tarun" , "Mudra Term Loan" , "Mudra Loans - Shishu / Kishor / Tarun" , "JLG Individual" , "JLG Group" , "SHG Individual" , "SHG Group" , "Mudra Loans – Shishu / Kishor / Tarun" , "Seller Financing" , "SHG Group- Govt" , "SHG Intra- Group", "Microfinance Personal Loan"]
    product_group['labd'] = ['Loan Against Bank Deposits']
    product_group['others'] = ["Leasing" ,  "Other" , "OTHER" , "Term Loans" , "Staff Loan"]
    product_group['telco'] = ["Telco Broadband" , "Telco Landline" , "Telco Wireless" , "Telco Wireless"]

    all_products = []

    for k, v in product_group.items():
        all_products.extend(v)
    product_group['overall'] = all_products

    product_group['overall'] = list(set(all_products))


    product_key = {value: key for key, values in list(product_group.items())[2:] for value in values}

    return product_group, product_key


# In[5]:


# CRIF Account Mapping

def product_group_dict_2():

    product_group = {}


    product_group['secured'] = ["Tractor Loan", "Used Car Loan", "Auto Loan (Personal)", "Commercial Vehicle Loan", "Commercial Vehicle Loan", "Used Tractor Loan", "Gold Loan", "GECL Loan secured", "Priority Sector Gold Loan", "Housing Loan", "Pradhan Mantri Awas Yojana - CLSS", "Loan Against Bank Deposits", "Property Loan", "Loan Against Property", "Loan Against Shares / Securities", "Leasing", "Commercial Equipment Loan", "Construction Equipment Loan", "Equipment Loan", "Business Loan Against Bank Deposits", "Business Loan - Secured", "Secured Credit Card", "Kisan Credit Card", "Two-Wheeler Loan", "TW"]
    product_group['unsecured'] =  ["Business Loan - Income Generating", "Advances against Export Cash Incentives / Import Bills", "Commercial Loan", "GECL Loan unsecured", "Bank Guarantee", "Non-Funded Credit Facility", "Business Loan General", "Business Loan Priority Sector Small Business", "Business Loan Priority Sector Agriculture", "Business Loan Priority Sector Others", "Business Non-Funded Credit Facility General", "Business Non-Funded Credit Facility-Priority Sector- Small Business", "Business Non-Funded Credit Facility-Priority Sector-Agriculture", "Business Non-Funded Credit Facility-Priority Sector-Others", "Business Loan Unsecured", "Microfinance Business Loan", "Loan to Professional", "Credit Card", "Fleet Card", "Corporate Credit Card", "Charge Card", "Consumer Loan", "Education Loan", "Microfinance Housing Loan", "Microfinance Others", "Mudra Working Capital", "Mudra Loans ? Shishu / Kishor / Tarun", "Mudra Term Loan", "Mudra Loans - Shishu / Kishor / Tarun", "JLG Individual", "JLG Group", "SHG Individual", "SHG Group", "Mudra Loans – Shishu / Kishor / Tarun", "Seller Financing", "SHG Group- Govt", "SHG Intra- Group", "Microfinance Personal Loan", "Cash Credit", "Current Account", "Prime Minister Jaan Dhan Yojana - Overdraft", "Auto Overdraft", "Overdraft", "OD on Savings Account", "Prime Minister Jana Dhan Yojana - Overdraft", "Temporary Overdraft", "Other", "OTHER", "Term Loans", "Staff Loan", "Loan on Credit Card", "Individual", "Personal Loan", "Short Term Personal Loan", "Loan against Card", "Working Capital Loans", "Telco Broadband", "Telco Landline", "Telco Wireless", "Telco Wireless"]

    product_group['pl'] = ["Personal Loan", "Short Term Personal Loan", "Loan on Credit Card", "Individual"]
    product_group['cc'] = ['Credit Card' , 'Fleet Card' , 'Corporate Credit Card', 'Charge Card' ]
    product_group['secured_cc'] = ['Secured Credit Card', 'Kisan Credit Card']
    product_group['od'] = ["Cash Credit" , "Current Account" , "Prime Minister Jaan Dhan Yojana - Overdraft" , "Auto Overdraft" , "Overdraft" , "OD on Savings Account" , "Prime Minister Jana Dhan Yojana - Overdraft" , "Temporary Overdraft"]
    product_group['gl'] = ['Gold Loan', 'Priority Sector Gold Loan']
    product_group['hl'] = ['Housing Loan', 'Pradhan Mantri Awas Yojana - CLSS']
    product_group['lap'] = ['Property Loan', 'Loan Against Property']
    product_group['las'] = ["Loan Against Shares / Securities"]
    product_group['al'] = ["Tractor Loan", "Used Car Loan", "Auto Loan (Personal)",  "Commercial Vehicle Loan", "Commercial Vehicle Loan", "Used Tractor Loan"]
    product_group['twl'] = ["Two-Wheeler Loan", "TW"]
    product_group['cl'] = ['Consumer Loan']
    product_group['el'] = ['Education Loan']
    product_group['bl'] = ['Business Loan - Income Generating', 'Advances against Export Cash Incentives / Import Bills', 'Commercial Loan', 'GECL Loan unsecured', 'GECL Loan secured', 'Bank Guarantee', 'Non-Funded Credit Facility', 'Business Loan General', 'Business Loan Priority Sector Small Business', 'Business Loan Priority Sector Agriculture', 'Business Loan Priority Sector Others', 'Business Non-Funded Credit Facility General', 'Business Non-Funded Credit Facility-Priority Sector- Small Business', 'Business Non-Funded Credit Facility-Priority Sector-Agriculture', 'Business Non-Funded Credit Facility-Priority Sector-Others', 'Business Loan Unsecured', 'Microfinance Business Loan', "Loan to Professional"]
    product_group['secured_bl'] = ['Commercial Equipment Loan' , 'Construction Equipment Loan', 'Business Loan - Secured', 'Equipment Loan', 'Business Loan Against Bank Deposits']
    product_group['scf'] = ['Working Capital Loans']
    product_group['mf'] = ["Microfinance Housing Loan", "Microfinance Others", "Microfinance Other" , "Mudra Working Capital" , "Mudra Loans ? Shishu / Kishor / Tarun" , "Mudra Term Loan" , "Mudra Loans - Shishu / Kishor / Tarun" , "JLG Individual" , "JLG Group" , "SHG Individual" , "SHG Group" , "Mudra Loans – Shishu / Kishor / Tarun" , "Seller Financing" , "SHG Group- Govt" , "SHG Intra- Group", "Microfinance Personal Loan"]
    product_group['labd'] = ['Loan Against Bank Deposits']
    product_group['others'] = ["Leasing" ,  "Other" , "OTHER" , "Term Loans" , "Staff Loan"]
    product_group['telco'] = ["Telco Broadband" , "Telco Landline" , "Telco Wireless" , "Telco Wireless"]

    #all_products = []

    # for k, v in product_group.items():
    #     all_products.extend(v)
    # product_group['overall'] = all_products

    # product_group['overall'] = list(set(all_products))


    product_key = {value: key for key, values in list(product_group.items())[2:] for value in values}
    product_key_sec_unsec = {value: key for key, values in list(product_group.items())[:2] for value in values}

    return product_group, product_key, product_key_sec_unsec


# In[6]:


# ensure to pass the list of values here in increasing order of enquiry amount

def enq_amt_cuts_dict():
    enq_amt_cuts = {}
    enq_amt_cuts['pl'] = [1, 100, 10000, 15000, 20000, 50000, 100000]
    enq_amt_cuts['hl'] = [500000, 1000000, 3000000, 5000000, 7500000]
    enq_amt_cuts['cl'] = [1, 5000, 10000, 20000, 30000, 50000]
    enq_amt_cuts['bl'] = [1, 50000, 200000, 500000, 1000000]
    enq_amt_cuts['cc'] = [1, 100, 15000, 30000, 50000]

    return enq_amt_cuts



# In[8]:


def months_to_days_dict(months_list):
    return {str(m): m * 30 for m in months_list}



# In[10]:


# ensure to pass the list of values here in increasing order of loan amount

def loan_amt_cuts_dict():
    loan_amt_cuts = {}
    loan_amt_cuts['pl'] = [10000, 25000, 50000, 100000, 250000, 500000]
    loan_amt_cuts['hl'] = [500000, 1000000, 3000000, 5000000, 7500000]
    loan_amt_cuts['cl'] = [5000, 10000, 20000, 30000, 50000]
    loan_amt_cuts['bl'] = [50000, 250000, 500000, 1000000, 1500000]
    loan_amt_cuts['cc'] = [15000, 50000, 100000, 300000, 500000]


    return loan_amt_cuts


# In[11]:


def ownership_mapper_dict():
    ownership_mapper = {}
    ownership_mapper[1] = 'Individual'
    ownership_mapper[2] = "Authorised User"
    ownership_mapper[3] = "Guarantor"
    ownership_mapper[4] = "Joint"
    ownership_mapper[5] = "Deceased"

    return ownership_mapper


# In[12]:


def phone_type_map_dict():
    phone_type_map = {0 : 'Not Classified', 1 : 'Mobile', 2 : 'Home', 3 : 'Office'}

    return phone_type_map


# ## Quality Check

# In[13]:


def quality_check_multibureau(acc_df,age_df):
    df = pd.merge(acc_df,age_df,left_on = 'multibureau__c', right_on = 'LOS_APP_ID/UNIQUE_ID')

    # Remove IDs where date_opened_or_disbursed__c column is not populated
    df = df[df['date_opened_or_disbursed__c'].notnull()]
    df_cleaned = df.copy(deep = True)
    initial_len = len(df)

    ## Convert column to date format
    df['createddate'] = pd.to_datetime(df['createddate'], format = '%d-%m-%Y')
    df['date_opened_or_disbursed__c'] = pd.to_datetime(df['date_opened_or_disbursed__c'], format = '%Y-%m-%d')
    df['date_closed__c'] = pd.to_datetime(df['date_closed__c'], format = '%Y-%m-%d')
    df['dateofbirth'] = pd.to_datetime(df['dateofbirth'], format = '%d-%m-%Y')
    # for col in ['dateofbirth']:
    #     df[col] = pd.to_datetime(pd.to_datetime(df[col]).dt.date)

    # 1. Remove Tradelines where Loan disbursement date is before DOB
    mask = df['date_opened_or_disbursed__c'] < df['dateofbirth']
    removed = df.loc[mask]
    print(f"[Check 1] Removed {mask.sum()} rows where disbursement < DOB. Unique IDs:", removed['multibureau__c'].nunique())
    df_cleaned = df_cleaned[~mask]

    # 2. Remove Tradelines where Loan close date is before loan open date
    mask = (df_cleaned['date_closed__c'] < df_cleaned['date_opened_or_disbursed__c'])
    removed = df_cleaned[mask]
    print(f"[Check 2] Removed {mask.sum()} rows with close date < open date. Unique IDs:", removed['multibureau__c'].nunique())
    df_cleaned = df_cleaned[~mask]

    # 3. Remove Id's where Ownership indicator is 5 (Deceased)
    mask = (df_cleaned['ownershipindicator'] == 'Deceased')
    removed = df_cleaned[mask]
    print(f"[Check 3] Removed {removed.shape[0]} rows with ownership = 5 (Deceased). Unique IDs:", removed['multibureau__c'].nunique())
    df_cleaned = df_cleaned[~df_cleaned['multibureau__c'].isin(removed['multibureau__c'].tolist())]


    # 4. Remove Tradelines where Sanctioned Amount is null -> Keep if any DPD exists in payment history (≠ '0' or 'XXX')
    dpd_cols = [col for col in df_cleaned.columns if 'payment_history' in col]

    if dpd_cols:
        mask = df_cleaned['high_credit_or_sanctioned_amount__c'].isna()
        # Added safety check to ensure only non zero null value case is further processed
        if mask.sum()>0:
            df_null_sanctioned = df_cleaned[mask]

            has_dpd = df_null_sanctioned[dpd_cols].astype(str).applymap(str.strip).apply(
                lambda row: ((row != '0') & (row != 'XXX')).any(),
                axis=1)

            # Rows to remove: sanctioned is null AND no DPD present
            mask_to_remove = mask.copy()
            mask_to_remove.loc[mask] = ~has_dpd.values

            removed = df_cleaned[mask_to_remove]
            print(f"[Check 4] Removed {mask_to_remove.sum()} rows with null sanctioned amount and no DPD. Unique IDs:", removed['multibureau__c'].nunique())
            df_cleaned = df_cleaned[~mask_to_remove]
        else:
            print(f"[Check 4] Removed 0 rows with null sanctioned amount and no DPD. Unique IDs:", 0)

    # 5. Excess current balance (over 3%) than sanctioned amount for non-CC/OD and overdue null/not null
    cc_list = ['Credit Card' , 'Fleet Card' , 'Corporate Credit Card', 'Charge Card', 'Secured Credit Card', 'Kisan Credit Card' ]
    od_list = ["Cash Credit" , "Current Account" , "Prime Minister Jaan Dhan Yojana - Overdraft" , "Auto Overdraft" , "Overdraft" , "OD on Savings Account" , "Prime Minister Jana Dhan Yojana - Overdraft" , "Temporary Overdraft"]

    cc_od_list = cc_list + od_list



    # Create % excess column
    df_cleaned['excess_percent'] = (
        (df_cleaned['current_balance__c'] - df_cleaned['high_credit_or_sanctioned_amount__c']) /
        df_cleaned['high_credit_or_sanctioned_amount__c']
    ).round(4)

    # Filter: non-CC/OD, excess > 3%
    mask_excess = (
        df_cleaned['excess_percent'].gt(0.03) &
        df_cleaned['high_credit_or_sanctioned_amount__c'].notnull() &
        ~df_cleaned['acct_type__c'].isin(cc_od_list)
    )

    removed = df_cleaned[mask_excess]
    print(f"[Check 5] Removed {mask_excess.sum()} rows with excess balance > 3% and overdue not null. Unique IDs:", removed['multibureau__c'].nunique())

    df_cleaned = df_cleaned[~mask_excess]

    final_len = len(df_cleaned)
    print(f"\nTotal rows removed: {initial_len - final_len}")

    return df_cleaned


# ## Last N Months Enquiry Features Function

# In[14]:


def enquiries_feats_last_n_months(df2, product_list=['overall', 'unsecured'], months_list=[3, 6],
                                  cal_perc_enq_cnt = True, cal_perc_enq_amt = True, enq_amt_cuts_product_list=['pl', 'hl']):

    """
    Generate product type wise enquiry-based features over the last N months for each multibureau ID
    """

    df = df2.copy(deep=True)

# Difference between enquiry created date and reported date in days
    df['acquisition_days'] = (df['createddate'] - df['date_reported__c']).dt.days

# Multibureau ID level aggregrations
    results_df = df.groupby('multibureau__c').agg(
        Number_of_enquiries=('id', 'count'),
        Number_of_distinct_enquiries=('enquiry_purpose__c', 'nunique'),
        days_since_first_enq=('acquisition_days', 'max'),
        days_since_latest_enq=('acquisition_days', 'min')
    ).reset_index()

# Create cartesian product of input months with input data for flag generation
    df['key'] = 1
    months_df = pd.DataFrame({'months': months_list})
    months_df['key'] = 1
    cross_df = df.merge(months_df, on='key')

    months_to_days = months_to_days_dict(months_list)
    cross_df['days_back'] = cross_df['months'].astype(str).map(months_to_days)

# Flag enquiries falling in "last N months" window
    cross_df['contrib'] = (cross_df['date_reported__c'] >= cross_df['createddate'] - pd.to_timedelta(cross_df['days_back'], unit='D')
                          ).astype(int)

    if 'overall' not in product_list:
        product_list = list(['overall']+product_list)

    product_list = list(set(product_list + enq_amt_cuts_product_list))

# Map enquiry purposes to product types
    product_df = pd.DataFrame([(k, val) for k, vals in product_group_dict()[0].items() for val in vals],
                              columns=['product', 'enquiry_purpose__c']
                             )
    product_df = product_df[product_df['product'].isin(product_list)]

    cross_df = cross_df.merge(product_df, on=['enquiry_purpose__c'], how='left')
    cross_df = cross_df[cross_df['product'].notnull()]

# Count enquiries by product and months
    enq_group = cross_df.groupby(['multibureau__c', 'months', 'product'])['contrib'].sum().reset_index()
    enq_group = enq_group.pivot(index='multibureau__c', columns=['months', 'product'], values='contrib')
    enq_group.columns = [f'last_{m}m_{p}_enq_count' for m, p in enq_group.columns]
    enq_group = enq_group.reset_index()

# Handle missing combinations with 0 count
    expected_cnt_cols = [f'last_{m}m_{p}_enq_count'
                         for m in months_list
                         for p in product_list
                        ]
    for col in expected_cnt_cols:
        if col not in enq_group.columns:
            enq_group[col] = 0
    enq_group = enq_group[['multibureau__c'] + expected_cnt_cols]

    results_df = results_df.merge(enq_group, on='multibureau__c', how='left')

    results_df[expected_cnt_cols] = results_df[expected_cnt_cols].fillna(0).astype(int)

# Compute % share of enquiry counts per product
    if (cal_perc_enq_cnt):
        enq_count_cols = results_df.filter(regex=r'^last_\d+m_(?!overall).*_enq_count$')

        meta_df = enq_count_cols.columns.to_series().str.extract(r'last_(\d+)m_(.+)_enq_count')
        meta_df.columns = ['month', 'product']
        meta_df['base_col'] = enq_count_cols.columns.values
        meta_df['denom_col'] = 'last_' + meta_df['month'] + 'm_overall_enq_count'
        meta_df['perc_col'] = 'last_' + meta_df['month'] + 'm_' + meta_df['product'] + '_enq_perc'

        valid_mask = meta_df['denom_col'].isin(results_df.columns)
        meta_df = meta_df[valid_mask]

        results_df[meta_df['perc_col'].values] = (
            results_df[meta_df['base_col'].values].values /
            results_df[meta_df['denom_col'].values].replace(0, np.nan).values
        ) * 100

# Aggregations on enquiry amount
    amount_agg = cross_df[cross_df['contrib'] == 1].groupby(['multibureau__c', 'months', 'product'])['enquiry_amount__c'].agg(['sum',
                                                                                                                              'min',
                                                                                                                              'max',
                                                                                                                              'mean',
                                                                                                                              'median',
                                                                                                                              ('std', lambda x: x.std(ddof=0))
                                                                                                                          ]).reset_index()



# Add mean_upon_max ratio features
    amount_agg['mean_upon_max'] = amount_agg.apply(
        lambda row: row['mean'] / row['max'] if row['max'] != 0 else np.nan, axis=1
    )


    amount_pivot = amount_agg.pivot(index='multibureau__c', columns=['months', 'product'])
    amount_pivot.columns = [f'last_{m}m_{p}_enq_amt_{stat}' for stat, m, p in amount_pivot.columns]
    amount_pivot = amount_pivot.reset_index()

# Add missing stat columns for combinations
    stats = ['sum', 'min', 'max', 'mean', 'median', 'std', 'mean_upon_max']
    expected_amt_cols = [f'last_{m}m_{p}_enq_amt_{stat}'
                         for m in months_list
                         for p in product_list
                         for stat in stats
                        ]

    for col in expected_amt_cols:
        if col not in amount_pivot.columns:
            amount_pivot[col] = np.nan

    amount_pivot = amount_pivot[['multibureau__c'] + expected_amt_cols]
    results_df = results_df.merge(amount_pivot, on='multibureau__c', how='left')

# Compute % share of enquiry amount
    if (cal_perc_enq_amt):
        enq_amt_sum_cols = results_df.filter(regex=r'^last_\d+m_(?!overall).*_enq_amt_sum$')

        meta_amt_df = enq_amt_sum_cols.columns.to_series().str.extract(r'last_(\d+)m_(.+)_enq_amt_sum')
        meta_amt_df.columns = ['month', 'product']
        meta_amt_df['base_col'] = enq_amt_sum_cols.columns.values
        meta_amt_df['denom_col'] = 'last_' + meta_amt_df['month'] + 'm_overall_enq_amt_sum'
        meta_amt_df['perc_col'] = 'last_' + meta_amt_df['month'] + 'm_' + meta_amt_df['product'] + '_enq_amt_perc'

        valid_amt_mask = meta_amt_df['denom_col'].isin(results_df.columns)
        meta_amt_df = meta_amt_df[valid_amt_mask]

        results_df[meta_amt_df['perc_col'].values] = (
            results_df[meta_amt_df['base_col'].values].values /
            results_df[meta_amt_df['denom_col'].values].replace(0, np.nan).values
        ) * 100


# Bin enquiry amounts into defined brackets and count frequency
    cuts_dict = enq_amt_cuts_dict()

    bin_meta = pd.DataFrame([
        {
            'product': product,
            'bin_edges': [-np.inf] + cuts + [np.inf],
            'bin_labels': (
                [f'le_{cuts[0]}'] +
                [f'{cuts[i]}_to_{cuts[i+1]}' for i in range(len(cuts) - 1)] +
                [f'gt_{cuts[-1]}']
            )
        }
        for product, cuts in cuts_dict.items()
    ])

    cut_df = cross_df[(cross_df['contrib'] == 1) & (cross_df['enquiry_amount__c'].notnull())]
    cut_df = cut_df[cut_df['product'].isin(enq_amt_cuts_product_list)]

# Handle missing cuts columns if no enquiries exist at all for all ids
    if cut_df.empty:
        id_list = df2['multibureau__c'].dropna().unique()
        mp_df = pd.DataFrame(product(months_list, enq_amt_cuts_product_list), columns=['months', 'product'])
        mp_df = mp_df.merge(bin_meta, on='product', how='left')
        mp_df = mp_df.explode('bin_labels', ignore_index=True)
        mp_df['feature'] = ('last_' + mp_df['months'].astype(str) + 'm_' +
                             mp_df['product'] + '_enq_amt_' +
                             mp_df['bin_labels']
                           )
        feature_meta = mp_df['feature'].tolist()
        all_combinations = pd.MultiIndex.from_product([id_list, feature_meta], names=['multibureau__c', 'feature']).to_frame(index=False)
        final_df = all_combinations.assign(value=0).pivot(index='multibureau__c', columns='feature', values='value').fillna(0).reset_index()
        results_df = results_df.merge(final_df, on='multibureau__c', how='left')
        return results_df



    cut_df = cut_df.merge(bin_meta, on='product', how='left')

    cut_df['bin'] = [
        pd.cut([amt], bins=edges, labels=labels, include_lowest=True)[0]
        for amt, edges, labels in zip(
            cut_df['enquiry_amount__c'],
            cut_df['bin_edges'],
            cut_df['bin_labels']
        )
    ]

    cut_df['feature'] = (
        'last_' + cut_df['months'].astype(str) + 'm_' +
        cut_df['product'] + '_enq_amt_' +
        cut_df['bin'].astype(str)
    )

    id_list = df2['multibureau__c'].dropna().unique()

# Generate all feature column names for binned amounts
    mp_df = pd.DataFrame(product(months_list, enq_amt_cuts_product_list), columns=['months', 'product'])
    mp_df = mp_df.merge(bin_meta, on='product', how='left')

# Create one row per bin label for each month-product pair
    mp_df = mp_df.explode('bin_labels', ignore_index=True)

    mp_df['feature'] = ('last_' + mp_df['months'].astype(str) + 'm_' +
                         mp_df['product'] + '_enq_amt_' +
                         mp_df['bin_labels']
                       )

    feature_meta = mp_df['feature'].tolist()

# Ensure all ID-feature combinations exist
    all_combinations = pd.MultiIndex.from_product([id_list, feature_meta], names=['multibureau__c', 'feature']).to_frame(index=False)

    agg_df = (cut_df.groupby(['multibureau__c', 'feature'])
                    .size()
                    .reset_index(name='value')
             )

    final_df = (all_combinations.merge(agg_df, on=['multibureau__c', 'feature'], how='left')
                                # .fillna(0)
                                .pivot(index='multibureau__c', columns='feature', values='value')
                                # .fillna(0)
                                .reset_index()
               )


    results_df = results_df.merge(final_df, on='multibureau__c', how='left')

    return results_df


# ## Before N Years Enquiry Features Function

# In[15]:


def enquiries_feats_before_n_years(df2, product_list=['overall', 'unsecured'], years_list=[1],
                                  cal_perc_enq_cnt = True, cal_perc_enq_amt = True, enq_amt_cuts_product_list=['pl', 'hl']):

    """
    Generate product wise enquiry-based features for before N years for each multibureau ID
    """

    df = df2.copy(deep=True)

# Difference between enquiry created date and reported date in days
    df['acquisition_days'] = (df['createddate'] - df['date_reported__c']).dt.days

    results_df = df[['multibureau__c']].drop_duplicates().reset_index(drop=True)

# Create cartesian product of input years with input data for flag generation
    df['key'] = 1
    years_df = pd.DataFrame({'years': years_list})
    years_df['key'] = 1
    cross_df = df.merge(years_df, on='key')
    cross_df['days_back'] = cross_df['years'] * 365

# Flag enquiries falling in "before N years" window
    cross_df['contrib'] = (cross_df['date_reported__c'] < cross_df['createddate'] - pd.to_timedelta(cross_df['days_back'], unit='D')
                          ).astype(int)

    if 'overall' not in product_list:
        product_list = list(['overall']+product_list)

    product_list = list(set(product_list + enq_amt_cuts_product_list))

# Map enquiry purposes to product types
    product_df = pd.DataFrame([(k, val) for k, vals in product_group_dict()[0].items() for val in vals],
                              columns=['product', 'enquiry_purpose__c']
                             )
    product_df = product_df[product_df['product'].isin(product_list)]

    cross_df = cross_df.merge(product_df, on=['enquiry_purpose__c'], how='left')
    cross_df = cross_df[cross_df['product'].notnull()]

# Count enquiries by product and years
    enq_group = cross_df.groupby(['multibureau__c', 'years', 'product'])['contrib'].sum().reset_index()
    enq_group = enq_group.pivot(index='multibureau__c', columns=['years', 'product'], values='contrib')
    enq_group.columns = [f'before_{y}y_{p}_enq_count' for y, p in enq_group.columns]
    enq_group = enq_group.reset_index()

# Handle missing combinations with 0 count
    expected_cnt_cols = [f'before_{y}y_{p}_enq_count'
                         for y in years_list
                         for p in product_list
                        ]
    for col in expected_cnt_cols:
        if col not in enq_group.columns:
            enq_group[col] = 0
    enq_group = enq_group[['multibureau__c'] + expected_cnt_cols]

    results_df = results_df.merge(enq_group, on='multibureau__c', how='left')

    results_df[expected_cnt_cols] = results_df[expected_cnt_cols].fillna(0).astype(int)

# Compute % share of enquiry counts per product
    if (cal_perc_enq_cnt):
        enq_count_cols = results_df.filter(regex=r'^before_\d+y_(?!overall).*_enq_count$')

        meta_df = enq_count_cols.columns.to_series().str.extract(r'before_(\d+)y_(.+)_enq_count')
        meta_df.columns = ['year', 'product']
        meta_df['base_col'] = enq_count_cols.columns.values
        meta_df['denom_col'] = 'before_' + meta_df['year'] + 'y_overall_enq_count'
        meta_df['perc_col'] = 'before_' + meta_df['year'] + 'y_' + meta_df['product'] + '_enq_perc'

        valid_mask = meta_df['denom_col'].isin(results_df.columns)
        meta_df = meta_df[valid_mask]

        results_df[meta_df['perc_col'].values] = (
            results_df[meta_df['base_col'].values].values /
            results_df[meta_df['denom_col'].values].replace(0, np.nan).values
        ) * 100


# Aggregations on enquiry amount
    amount_agg = cross_df[cross_df['contrib'] == 1].groupby(['multibureau__c', 'years', 'product'])['enquiry_amount__c'].agg(['sum',
                                                                                                                               'min',
                                                                                                                               'max',
                                                                                                                               'mean',
                                                                                                                               'median',
                                                                                                                              #  'std'
                                                                                                                              ('std', lambda x: x.std(ddof=0))
                                                                                                                               ]).reset_index()

# Add mean_upon_max ratio features
    amount_agg['mean_upon_max'] = amount_agg.apply(
        lambda row: row['mean'] / row['max'] if row['max'] != 0 else np.nan, axis=1
    )

    amount_pivot = amount_agg.pivot(index='multibureau__c', columns=['years', 'product'])
    amount_pivot.columns = [f'before_{y}y_{p}_enq_amt_{stat}' for stat, y, p in amount_pivot.columns]
    amount_pivot = amount_pivot.reset_index()

# Add missing stat columns for combinations
    stats = ['sum', 'min', 'max', 'mean', 'median', 'std', 'mean_upon_max']
    expected_amt_cols = [f'before_{y}y_{p}_enq_amt_{stat}'
                         for y in years_list
                         for p in product_list
                         for stat in stats
                        ]

    for col in expected_amt_cols:
        if col not in amount_pivot.columns:
            amount_pivot[col] = np.nan

    amount_pivot = amount_pivot[['multibureau__c'] + expected_amt_cols]
    results_df = results_df.merge(amount_pivot, on='multibureau__c', how='left')


# Compute % share of enquiry amount
    if (cal_perc_enq_amt):
        enq_amt_sum_cols = results_df.filter(regex=r'^before_\d+y_(?!overall).*_enq_amt_sum$')

        meta_amt_df = enq_amt_sum_cols.columns.to_series().str.extract(r'before_(\d+)y_(.+)_enq_amt_sum')
        meta_amt_df.columns = ['year', 'product']
        meta_amt_df['base_col'] = enq_amt_sum_cols.columns.values
        meta_amt_df['denom_col'] = 'before_' + meta_amt_df['year'] + 'y_overall_enq_amt_sum'
        meta_amt_df['perc_col'] = 'before_' + meta_amt_df['year'] + 'y_' + meta_amt_df['product'] + '_enq_amt_perc'

        valid_amt_mask = meta_amt_df['denom_col'].isin(results_df.columns)
        meta_amt_df = meta_amt_df[valid_amt_mask]

        results_df[meta_amt_df['perc_col'].values] = (
            results_df[meta_amt_df['base_col'].values].values /
            results_df[meta_amt_df['denom_col'].values].replace(0, np.nan).values
        ) * 100

# Bin enquiry amounts into defined brackets and count frequency
    cuts_dict = enq_amt_cuts_dict()

    bin_meta = pd.DataFrame([
        {
            'product': product,
            'bin_edges': [-np.inf] + cuts + [np.inf],
            'bin_labels': (
                [f'le_{cuts[0]}'] +
                [f'{cuts[i]}_to_{cuts[i+1]}' for i in range(len(cuts) - 1)] +
                [f'gt_{cuts[-1]}']
            )
        }
        for product, cuts in cuts_dict.items()
    ])

    cut_df = cross_df[(cross_df['contrib'] == 1) & (cross_df['enquiry_amount__c'].notnull())]
    cut_df = cut_df[cut_df['product'].isin(enq_amt_cuts_product_list)]

# Handle missing cuts columns if no enquiries exist at all for all ids
    if cut_df.empty:
        id_list = df2['multibureau__c'].dropna().unique()
        mp_df = pd.DataFrame(product(years_list, enq_amt_cuts_product_list), columns=['years', 'product'])
        mp_df = mp_df.merge(bin_meta, on='product', how='left')
        mp_df = mp_df.explode('bin_labels', ignore_index=True)
        mp_df['feature'] = ('before_' + mp_df['years'].astype(str) + 'y_' +
                             mp_df['product'] + '_enq_amt_' +
                             mp_df['bin_labels']
                           )
        feature_meta = mp_df['feature'].tolist()
        all_combinations = pd.MultiIndex.from_product([id_list, feature_meta], names=['multibureau__c', 'feature']).to_frame(index=False)
        final_df = all_combinations.assign(value=0).pivot(index='multibureau__c', columns='feature', values='value').fillna(0).reset_index()
        results_df = results_df.merge(final_df, on='multibureau__c', how='left')
        return results_df

    cut_df = cut_df.merge(bin_meta, on='product', how='left')

    cut_df['bin'] = [
        pd.cut([amt], bins=edges, labels=labels, include_lowest=True)[0]
        for amt, edges, labels in zip(
            cut_df['enquiry_amount__c'],
            cut_df['bin_edges'],
            cut_df['bin_labels']
        )
    ]

    cut_df['feature'] = (
        'before_' + cut_df['years'].astype(str) + 'y_' +
        cut_df['product'] + '_enq_amt_' +
        cut_df['bin'].astype(str)
    )

    id_list = df2['multibureau__c'].dropna().unique()

# Generate all feature column names for binned amounts
    mp_df = pd.DataFrame(product(years_list, enq_amt_cuts_product_list), columns=['years', 'product'])
    mp_df = mp_df.merge(bin_meta, on='product', how='left')

# Create one row per bin label for each year-product pair
    mp_df = mp_df.explode('bin_labels', ignore_index=True)

    mp_df['feature'] = ('before_' + mp_df['years'].astype(str) + 'y_' +
                         mp_df['product'] + '_enq_amt_' +
                         mp_df['bin_labels']
                       )

    feature_meta = mp_df['feature'].tolist()

# Ensure all ID-feature combinations exist
    all_combinations = pd.MultiIndex.from_product([id_list, feature_meta], names=['multibureau__c', 'feature']).to_frame(index=False)

    agg_df = (cut_df.groupby(['multibureau__c', 'feature'])
                    .size()
                    .reset_index(name='value')
             )

    final_df = (all_combinations.merge(agg_df, on=['multibureau__c', 'feature'], how='left')
                                # .fillna(0)
                                .pivot(index='multibureau__c', columns='feature', values='value')
                                # .fillna(0)
                                .reset_index()
               )


    results_df = results_df.merge(final_df, on='multibureau__c', how='left')

    return results_df


# ## Ever Loans Features Functions

# In[16]:


def overall_tradelines_feats(df2, product_list=['overall', 'unsecured'], cal_perc_acc_cnt=True, cal_perc_acc_amt=True,
                             loan_amt_cuts_product_list=['pl', 'hl']):
    """
    Generates features on overall account level (no timeline/closed filter) for each multibureau ID
    """

    df = df2.copy(deep=True)

    for col in ['createddate', 'date_opened_or_disbursed__c', 'date_closed__c']:
        df[col] = pd.to_datetime(pd.to_datetime(df[col]).dt.date)

# Calculation of months since aacquisition
    df['acquisition_months'] = ((df['createddate'].dt.year - df['date_opened_or_disbursed__c'].dt.year) * 12 +
                                (df['createddate'].dt.month - df['date_opened_or_disbursed__c'].dt.month))



    df.payment_history_start_date__c = pd.to_datetime(df.payment_history_start_date__c)
    df.payment_history_start_date__c = pd.to_datetime(df.payment_history_start_date__c)


# Aggregations on multibureau ID level
    results_df = df.groupby('multibureau__c').agg(
        Number_of_distinct_tradelines=('account_type__c', 'nunique'),
        months_since_first_tradeline_opnd=('acquisition_months', 'max'),
        months_since_latest_tradeline_opnd=('acquisition_months', 'min')
    ).reset_index()



    if 'overall' not in product_list:
        product_list = ['overall'] + product_list

    product_list = list(set(product_list + loan_amt_cuts_product_list))

# Map Account type number to product types
    product_df = pd.DataFrame([(k, val) for k, vals in product_group_dict()[0].items() for val in vals],
                              columns=['product', 'account_type__c'])
    product_df = product_df[product_df['product'].isin(product_list)]

    df_prod = df.merge(product_df, on='account_type__c', how='left')
    df_prod = df_prod[df_prod['product'].notnull()]

# Product-level acquisition stats
    prod_acq_stats = df_prod.groupby(['multibureau__c', 'product'])['acquisition_months'].agg(['min', 'max']).reset_index()
    prod_acq_stats = prod_acq_stats.pivot(index='multibureau__c', columns='product')
    prod_acq_stats.columns = [f'months_since_latest_{prod}_tradeline_opened' if stat == 'min'
                              else f'months_since_first_{prod}_tradeline_opened'
                              for stat, prod in prod_acq_stats.columns]
    prod_acq_stats = prod_acq_stats.reset_index()

    prod_acq_stats = prod_acq_stats.loc[:, ~prod_acq_stats.columns.str.contains('overall')]



    results_df = results_df.merge(prod_acq_stats, on='multibureau__c', how='left')


# Account count per product
    acc_group = df_prod.groupby(['multibureau__c', 'product'])['id'].count().reset_index(name='acc_count')
    acc_group = acc_group.pivot(index='multibureau__c', columns='product', values='acc_count').fillna(0).astype(int)
    acc_group.columns = [f'{p}_acc_count' for p in acc_group.columns]
    acc_group = acc_group.reset_index()

# Handle missing combinations with 0 count
    expected_cnt_cols = [f'{p}_acc_count'
                         for p in product_list]

    for col in expected_cnt_cols:
        if col not in acc_group.columns:
            acc_group[col] = 0

    acc_group = acc_group[['multibureau__c'] + expected_cnt_cols]



    for p in product_list:
        if p != 'overall':
          acc_group[f'{p}_acc_flag'] = (acc_group[f'{p}_acc_count'] > 0).astype(int)



    results_df = results_df.merge(acc_group, on='multibureau__c', how='left')
    results_df[expected_cnt_cols] = results_df[expected_cnt_cols].fillna(0).astype(int)

# Compute % share of tradelines counts per product
    if cal_perc_acc_cnt:
        acc_count_cols = results_df.filter(regex=r'^(?!overall).*_acc_count$')
        meta_df = acc_count_cols.columns.to_series().str.extract(r'(.+)_acc_count')
        meta_df.columns = ['product']
        meta_df['base_col'] = acc_count_cols.columns.values
        meta_df['denom_col'] = 'overall_acc_count'
        meta_df['perc_col'] =  meta_df['product'] + '_acc_perc'

        valid_mask = meta_df['denom_col'].isin(results_df.columns)
        meta_df = meta_df[valid_mask]

        results_df[meta_df['perc_col'].values] = (
            results_df[meta_df['base_col'].values].values /
            results_df[meta_df['denom_col'].values].replace(0, np.nan).values
        ) * 100



# Aggregated statistic features on Loan amount
    amount_agg = df_prod[df_prod['high_credit_or_sanctioned_amount__c'].notnull()].groupby(
        ['multibureau__c', 'product'])['high_credit_or_sanctioned_amount__c'].agg(['sum',
                                                                                  'min',
                                                                                  'max',
                                                                                  'mean',
                                                                                  'median',
                                                                                  ('std', lambda x: x.std(ddof=0))
                                                                                  ]).reset_index()

# Add mean_upon_max ratio features
    amount_agg['mean_upon_max'] = amount_agg.apply(
        lambda row: row['mean'] / row['max'] if row['max'] != 0 else np.nan, axis=1
    )

    amount_pivot = amount_agg.pivot(index='multibureau__c', columns='product')
    amount_pivot.columns = [f'{p}_loan_amt_{stat}' for stat, p in amount_pivot.columns]
    amount_pivot = amount_pivot.reset_index()

    stats = ['sum', 'min', 'max', 'mean', 'median', 'std', 'mean_upon_max']

# Handle missing amount stat columns
    expected_amt_cols = [f'{p}_loan_amt_{stat}'
                         for p in product_list
                         for stat in stats]

    for col in expected_amt_cols:
        if col not in amount_pivot.columns:
            amount_pivot[col] = np.nan


    amount_pivot = amount_pivot[['multibureau__c'] + expected_amt_cols]
    results_df = results_df.merge(amount_pivot, on='multibureau__c', how='left')

# Compute % share of Loan amount
    if cal_perc_acc_amt:
        acc_amt_sum_cols = results_df.filter(regex=r'^(?!overall).*_loan_amt_sum$')
        meta_amt_df = acc_amt_sum_cols.columns.to_series().str.extract(r'(.+)_loan_amt_sum')
        meta_amt_df.columns = ['product']
        meta_amt_df['base_col'] = acc_amt_sum_cols.columns.values
        meta_amt_df['denom_col'] = 'overall_loan_amt_sum'
        meta_amt_df['ratio_col'] = 'perc_' + meta_amt_df['product'] + '_loan_amount'

        valid_amt_mask = meta_amt_df['denom_col'].isin(results_df.columns)
        meta_amt_df = meta_amt_df[valid_amt_mask]

        results_df[meta_amt_df['ratio_col'].values] = (
            results_df[meta_amt_df['base_col'].values].values /
            results_df[meta_amt_df['denom_col'].values].replace(0, np.nan).values
        ) * 100



# Bin Loan amounts into defined brackets and count frequency
    cuts_dict = loan_amt_cuts_dict()

    bin_meta = pd.DataFrame([
        {
            'product': product,
            'bin_edges': [-np.inf] + cuts + [np.inf],
            'bin_labels': (
                [f'le_{cuts[0]}'] +
                [f'{cuts[i]}_to_{cuts[i+1]}' for i in range(len(cuts) - 1)] +
                [f'gt_{cuts[-1]}']
            )
        }
        for product, cuts in cuts_dict.items()
    ])

    cut_df = df_prod[df_prod['product'].isin(loan_amt_cuts_product_list)]
    cut_df = cut_df[cut_df['high_credit_or_sanctioned_amount__c'].notnull()]

# Handle missing columns if no tradelines exist at all for all ids
    if cut_df.empty:
        id_list = df2['multibureau__c'].dropna().unique()
        mp_df = pd.DataFrame({'product': loan_amt_cuts_product_list})
        mp_df = mp_df.merge(bin_meta, on='product', how='left').explode('bin_labels', ignore_index=True)
        mp_df['feature'] = mp_df['product'] + '_loan_amt_' + mp_df['bin_labels']
        feature_meta = mp_df['feature'].tolist()
        all_combinations = pd.MultiIndex.from_product([id_list, feature_meta], names=['multibureau__c', 'feature']).to_frame(index=False)
        final_df = all_combinations.assign(value=0).pivot(index='multibureau__c', columns='feature', values='value').fillna(0).reset_index()
        results_df = results_df.merge(final_df, on='multibureau__c', how='left')
        return results_df



    cut_df = cut_df.merge(bin_meta, on='product', how='left')

    cut_df['bin'] = [
        pd.cut([amt], bins=edges, labels=labels, include_lowest=True)[0]
        for amt, edges, labels in zip(
            cut_df['high_credit_or_sanctioned_amount__c'],
            cut_df['bin_edges'],
            cut_df['bin_labels']
        )
    ]

    cut_df['feature'] = cut_df['product'] + '_loan_amt_' + cut_df['bin'].astype(str)

# Generate zero-combinations for all multibureau IDs and bins
    id_list = df2['multibureau__c'].dropna().unique()
    mp_df = pd.DataFrame({'product': loan_amt_cuts_product_list})
    mp_df = mp_df.merge(bin_meta, on='product', how='left').explode('bin_labels', ignore_index=True)
    mp_df['feature'] = mp_df['product'] + '_loan_amt_' + mp_df['bin_labels']
    feature_meta = mp_df['feature'].tolist()

    all_combinations = pd.MultiIndex.from_product([id_list, feature_meta], names=['multibureau__c', 'feature']).to_frame(index=False)

    agg_df = cut_df.groupby(['multibureau__c', 'feature']).size().reset_index(name='value')

    final_df = (all_combinations.merge(agg_df, on=['multibureau__c', 'feature'], how='left')
                                # .fillna(0)
                                .pivot(index='multibureau__c', columns='feature', values='value')
                                # .fillna(0)
                                .reset_index())

    results_df = results_df.merge(final_df, on='multibureau__c', how='left')

    return results_df


# ## Ever Closed Features

# In[17]:


def overall_clsd_tradelines_feats(df2, product_list=['overall', 'unsecured'], cal_perc_acc_cnt=True, cal_perc_acc_amt=True, loan_amt_cuts_product_list=['pl']):


    df = df2[~df2['date_closed__c'].isna()].copy(deep=True) #Closed tradelines
    df1 = df2.copy(deep=True) # All tradlines

    for col in ['createddate', 'date_opened_or_disbursed__c', 'date_closed__c']:
        df[col] = pd.to_datetime(pd.to_datetime(df[col]).dt.date)


    if 'overall' not in product_list:
        product_list = ['overall'] + product_list

    product_list = list(set(product_list + loan_amt_cuts_product_list))

# Map Account type number to product types

    results_df = pd.DataFrame()
    results_df['multibureau__c'] = list(set(df2.multibureau__c))

    product_df = pd.DataFrame([(k, val) for k, vals in product_group_dict()[0].items() for val in vals],
                              columns=['product', 'account_type__c'])

    product_df = product_df[product_df['product'].isin(product_list)]


    df_prod = df.merge(product_df, on='account_type__c', how='left')
    df_prod = df_prod[df_prod['product'].notnull()]

# Product-level acquisition stats


# Account count per product
    acc_group = df_prod.groupby(['multibureau__c', 'product'])['id'].count().reset_index(name='acc_count')
    acc_group = acc_group.pivot(index='multibureau__c', columns='product', values='acc_count').fillna(0).astype(int)
    acc_group.columns = [f'{p}_clsd_acc_count' for p in acc_group.columns]
    acc_group = acc_group.reset_index()

# Handle missing combinations with 0 count
    expected_cnt_cols = [f'{p}_clsd_acc_count'
                         for p in product_list]

    for col in expected_cnt_cols:
        if col not in acc_group.columns:
            acc_group[col] = 0

    acc_group = acc_group[['multibureau__c'] + expected_cnt_cols]



    results_df = results_df.merge(acc_group, on='multibureau__c', how='left')
    results_df[expected_cnt_cols] = results_df[expected_cnt_cols].fillna(0).astype(int)


# Compute % share of tradelines counts per product
    if cal_perc_acc_cnt:
        acc_count_cols = results_df.filter(regex=r'^(?!overall).*_acc_count$')
        meta_df = acc_count_cols.columns.to_series().str.extract(r'(.+)_acc_count')
        meta_df.columns = ['product']
        meta_df['base_col'] = acc_count_cols.columns.values
        meta_df['denom_col'] = 'overall_acc_count'
        meta_df['perc_col'] =  meta_df['product'] + 'clsd_acc_perc'

        valid_mask = meta_df['denom_col'].isin(results_df.columns)
        meta_df = meta_df[valid_mask]

        results_df[meta_df['perc_col'].values] = (
            results_df[meta_df['base_col'].values].values /
            results_df[meta_df['denom_col'].values].replace(0, np.nan).values
        ) * 100



# Aggregated statistic features on Loan amount
    amount_agg = df_prod[df_prod['high_credit_or_sanctioned_amount__c'].notnull()].groupby(
        ['multibureau__c', 'product'])['high_credit_or_sanctioned_amount__c'].agg(['sum',
                                                                                  'min',
                                                                                  'max',
                                                                                  'mean',
                                                                                  'median',
                                                                                  # 'std'
                                                                                  ('std', lambda x: x.std(ddof=0))
                                                                                  ]).reset_index()

    amount_pivot = amount_agg.pivot(index='multibureau__c', columns='product')
    amount_pivot.columns = [f'{p}_clsd_loan_amt_{stat}' for stat, p in amount_pivot.columns]
    amount_pivot = amount_pivot.reset_index()

    stats = ['sum', 'min', 'max', 'mean', 'median', 'std']

# Handle missing amount stat columns
    expected_amt_cols = [f'{p}_clsd_loan_amt_{stat}'
                         for p in product_list
                         for stat in stats]

    for col in expected_amt_cols:
        if col not in amount_pivot.columns:
            amount_pivot[col] = np.nan


    amount_pivot = amount_pivot[['multibureau__c'] + expected_amt_cols]
    results_df = results_df.merge(amount_pivot, on='multibureau__c', how='left')



# Compute % share of Loan amount
    if cal_perc_acc_amt:
        acc_amt_sum_cols = results_df.filter(regex=r'^(?!overall).*_loan_amt_sum$')
        meta_amt_df = acc_amt_sum_cols.columns.to_series().str.extract(r'(.+)_loan_amt_sum')
        meta_amt_df.columns = ['product']
        meta_amt_df['base_col'] = acc_amt_sum_cols.columns.values
        meta_amt_df['denom_col'] = 'overall_clsd_loan_amt_sum'
        meta_amt_df['ratio_col'] = 'perc_' + meta_amt_df['product'] + '_clsd_loan_amount'

        valid_amt_mask = meta_amt_df['denom_col'].isin(results_df.columns)
        meta_amt_df = meta_amt_df[valid_amt_mask]

        results_df[meta_amt_df['ratio_col'].values] = (
            results_df[meta_amt_df['base_col'].values].values /
            results_df[meta_amt_df['denom_col'].values].replace(0, np.nan).values
        ) * 100



# Bin Loan amounts into defined brackets and count frequency
    cuts_dict = loan_amt_cuts_dict()

    bin_meta = pd.DataFrame([
        {
            'product': product,
            'bin_edges': [-np.inf] + cuts + [np.inf],
            'bin_labels': (
                [f'le_{cuts[0]}'] +
                [f'{cuts[i]}_to_{cuts[i+1]}' for i in range(len(cuts) - 1)] +
                [f'gt_{cuts[-1]}']
            )
        }
        for product, cuts in cuts_dict.items()
    ])

    cut_df = df_prod[df_prod['product'].isin(loan_amt_cuts_product_list)]
    cut_df = cut_df[cut_df['high_credit_or_sanctioned_amount__c'].notnull()]

# Handle missing columns if no tradelines exist at all for all ids
    if cut_df.empty:
        id_list = df2['multibureau__c'].dropna().unique()
        mp_df = pd.DataFrame({'product': loan_amt_cuts_product_list})
        mp_df = mp_df.merge(bin_meta, on='product', how='left').explode('bin_labels', ignore_index=True)
        mp_df['feature'] = mp_df['product'] + '_clsd_loan_amt_' + mp_df['bin_labels']
        feature_meta = mp_df['feature'].tolist()
        all_combinations = pd.MultiIndex.from_product([id_list, feature_meta], names=['multibureau__c', 'feature']).to_frame(index=False)
        final_df = all_combinations.assign(value=0).pivot(index='multibureau__c', columns='feature', values='value').fillna(0).reset_index()
        results_df = results_df.merge(final_df, on='multibureau__c', how='left')
        return results_df



    cut_df = cut_df.merge(bin_meta, on='product', how='left')

    cut_df['bin'] = [
        pd.cut([amt], bins=edges, labels=labels, include_lowest=True)[0]
        for amt, edges, labels in zip(
            cut_df['high_credit_or_sanctioned_amount__c'],
            cut_df['bin_edges'],
            cut_df['bin_labels']
        )
    ]

    cut_df['feature'] = cut_df['product'] + '_clsd_loan_amt_' + cut_df['bin'].astype(str)

# Generate zero-combinations for all multibureau IDs and bins
    id_list = df2['multibureau__c'].dropna().unique()
    mp_df = pd.DataFrame({'product': loan_amt_cuts_product_list})
    mp_df = mp_df.merge(bin_meta, on='product', how='left').explode('bin_labels', ignore_index=True)
    mp_df['feature'] = mp_df['product'] + '_clsd_loan_amt_' + mp_df['bin_labels']
    feature_meta = mp_df['feature'].tolist()

    all_combinations = pd.MultiIndex.from_product([id_list, feature_meta], names=['multibureau__c', 'feature']).to_frame(index=False)

    agg_df = cut_df.groupby(['multibureau__c', 'feature']).size().reset_index(name='value')

    final_df = (all_combinations.merge(agg_df, on=['multibureau__c', 'feature'], how='left')
                                # .fillna(0)
                                .pivot(index='multibureau__c', columns='feature', values='value')
                                # .fillna(0)
                                .reset_index())

    results_df = results_df.merge(final_df, on='multibureau__c', how='left')



    return results_df


# ## Last N Months Closed Loans Features Function

# In[18]:


def closed_tradelines_feats_last_n_months(df2, product_list=['overall', 'unsecured'], months_list=[3,6], cal_perc_acc_cnt=True, cal_perc_acc_amt=True, loan_amt_cuts_product_list=['pl','hl']):

    """
    Generates features for accounts closed over the last N months for each multibureau ID
    """

    df = df2[~df2['date_closed__c'].isna()].copy(deep=True) #Closed tradelines
    df1 = df2.copy(deep=True) # All tradlines

    all_ids_df = df1[['multibureau__c']].drop_duplicates().reset_index(drop=True)

    results_df = df.groupby('multibureau__c').agg(
        **{'Number_of_loans_clsd': ('id', 'count'),
           'Number_of_distinct_loans_clsd': ('account_type__c', 'nunique')
          }
    ).reset_index()

    results_df = all_ids_df.merge(results_df, on='multibureau__c', how='left')

    results_df[['Number_of_loans_clsd', 'Number_of_distinct_loans_clsd']] = \
    results_df[['Number_of_loans_clsd', 'Number_of_distinct_loans_clsd']].fillna(0).astype(int)

# Create cartesian product of input months with input data for flag generation
    df['key'] = 1
    months_df = pd.DataFrame({'months': months_list})
    months_df['key'] = 1
    cross_df = df.merge(months_df, on='key')

    months_to_days = months_to_days_dict(months_list)
    cross_df['days_back'] = cross_df['months'].astype(str).map(months_to_days)

# Flag tradelines falling in "last N months" window
    cross_df['contrib'] = (cross_df['date_closed__c'] >= cross_df['createddate'] - pd.to_timedelta(cross_df['days_back'], unit='D')
                          ).astype(int)

    if 'overall' not in product_list:
        product_list = ['overall'] + product_list


    product_list = list(set(product_list + loan_amt_cuts_product_list))

# Map Account type number to product types
    product_df = pd.DataFrame([(k, val) for k, vals in product_group_dict()[0].items() for val in vals],
                              columns=['product', 'account_type__c'])
    product_df = product_df[product_df['product'].isin(product_list)]

    cross_df = cross_df.merge(product_df, on='account_type__c', how='left')
    cross_df = cross_df[cross_df['product'].notnull()]

# Count of tradelines by product and months
    acc_group = cross_df.groupby(['multibureau__c', 'months', 'product'])['contrib'].sum().reset_index()
    acc_group = acc_group.pivot(index='multibureau__c', columns=['months', 'product'], values='contrib')
    acc_group.columns = [f'last_{m}m_clsd_{p}_acc_count' for m, p in acc_group.columns]
    acc_group = acc_group.reset_index()

# Handle missing combinations with 0 count
    expected_cnt_cols = [f'last_{m}m_clsd_{p}_acc_count' for m in months_list for p in product_list]
    for col in expected_cnt_cols:
        if col not in acc_group.columns:
            acc_group[col] = 0
    acc_group = acc_group[['multibureau__c'] + expected_cnt_cols]

    results_df = results_df.merge(acc_group, on='multibureau__c', how='left')
    results_df[expected_cnt_cols] = results_df[expected_cnt_cols].fillna(0).astype(int)


    df1['interval'] = pd.IntervalIndex.from_arrays(df1['date_opened_or_disbursed__c'].fillna(pd.Timestamp('1980-01-01')),
                                                   df1['date_closed__c'].fillna(pd.Timestamp('2100-01-01')),
                                                   closed='both'
                                                  )

    df1['key'] = 1
    cross_df1 = df1.merge(months_df, on='key')

    cross_df1['days_back'] = cross_df1['months'].astype(str).map(months_to_days)
    cross_df1['date_days_back'] = cross_df1.apply(lambda row: row['createddate'] - pd.to_timedelta(row['days_back'], unit='D'), axis=1)

# Flag tradelines active at any point of time in "last N months"
    cross_df1['present_in_last_n_months'] = cross_df1.apply(lambda row: row['date_days_back'] in row['interval'], axis=1).astype(int)

    cross_df1 = cross_df1.merge(product_df, on='account_type__c', how='left')
    cross_df1 = cross_df1[cross_df1['product'].notnull()]

    acc_group1 = cross_df1.groupby(['multibureau__c', 'months', 'product'])['present_in_last_n_months'].sum().reset_index()
    acc_group1 = acc_group1.pivot(index='multibureau__c', columns=['months', 'product'], values='present_in_last_n_months')
    acc_group1.columns = [f'last_{m}m_rng_{p}_acc_count' for m, p in acc_group1.columns]
    acc_group1 = acc_group1.reset_index()

    expected_rng_cnt_cols = [f'last_{m}m_rng_{p}_acc_count' for m in months_list for p in product_list]
    for col in expected_rng_cnt_cols:
        if col not in acc_group1.columns:
            acc_group1[col] = 0
    acc_group1 = acc_group1[['multibureau__c'] + expected_rng_cnt_cols]
    results_df = results_df.merge(acc_group1, on='multibureau__c', how='left')
    results_df[expected_rng_cnt_cols] = results_df[expected_rng_cnt_cols].fillna(0).astype(int)

# Compute % shares of tradelines counts per product
    if cal_perc_acc_cnt:
        acc_count_cols = results_df.filter(regex=r'^last_\d+m_clsd_(?!overall).*_acc_count$')
        meta_df = acc_count_cols.columns.to_series().str.extract(r'last_(\d+)m_clsd_(.+)_acc_count')
        meta_df.columns = ['month', 'product']
# Compute % shares of Loan counts for closed loans for each product type out of total closed loans
        meta_df['base_col'] = acc_count_cols.columns.values
        meta_df['denom_col'] = 'last_' + meta_df['month'] + 'm_clsd_overall_acc_count'
        meta_df['perc_col'] = 'last_' + meta_df['month'] + 'm_clsd_' + meta_df['product'] + '_upon_clsd_all_acc_perc'

        valid_mask = meta_df['denom_col'].isin(results_df.columns)
        meta_df = meta_df[valid_mask]

        results_df[meta_df['perc_col'].values] = (
            results_df[meta_df['base_col'].values].values /
            results_df[meta_df['denom_col'].values].replace(0, np.nan).values
        ) * 100

        acc_rng_count_cols = results_df.filter(regex=r'^last_\d+m_rng_.*_acc_count$')
        meta_df1 = acc_rng_count_cols.columns.to_series().str.extract(r'last_(\d+)m_rng_(.+)_acc_count')
        meta_df1.columns = ['month', 'product']
# Compute % shares of Loan counts for closed loans out of running loans for each product type
        meta_df1['base_col'] = acc_rng_count_cols.columns.values
        meta_df1['denom_col_same_product'] = 'last_' + meta_df1['month'] + 'm_rng_' + meta_df1['product']+ '_acc_count'
        meta_df1['perc_col_same_product'] = 'last_' + meta_df1['month'] + 'm_clsd_' + meta_df1['product']+ '_upon_rng_' + meta_df1['product']+ '_acc_perc'

        valid_mask1 = meta_df1['denom_col_same_product'].isin(results_df.columns)
        sub_df = meta_df1[valid_mask1]

        results_df[sub_df['perc_col_same_product'].values] = (
            results_df[sub_df['base_col'].values].values /
            results_df[sub_df['denom_col_same_product'].values].replace(0, np.nan).values
        ) * 100

# Compute % shares of Loan counts for closed loans for each product type out of total running loans
        meta_df1['denom_col_all_product'] = 'last_' + meta_df1['month'] + 'm_rng_overall_acc_count'
        meta_df1['perc_col_all_product'] = 'last_' + meta_df1['month'] + 'm_clsd_' + meta_df1['product']+ '_upon_rng_all_acc_perc'

        valid_mask2 = meta_df1['denom_col_all_product'].isin(results_df.columns)
        sub_df1 = meta_df1[valid_mask2]

        results_df[sub_df1['perc_col_all_product'].values] = (
            results_df[sub_df1['base_col'].values].values /
            results_df[sub_df1['denom_col_all_product'].values].replace(0, np.nan).values
        ) * 100

        cols_to_drop = [f'last_{m}m_clsd_overall_upon_rng_all_acc_perc' for m in months_list]
        results_df.drop(columns=cols_to_drop, inplace=True)


    amount_agg = cross_df[cross_df['contrib'] == 1].groupby(['multibureau__c', 'months', 'product'])['high_credit_or_sanctioned_amount__c'].agg(
                                                                                                                                                ['sum',
                                                                                                                                                 'min',
                                                                                                                                                 'max',
                                                                                                                                                 'mean',
                                                                                                                                                 'median',
                                                                                                                                                #  'std'
                                                                                                                                                 ('std', lambda x: x.std(ddof=0))
                                                                                                                                             ]).reset_index()

# Add mean_upon_max ratio features
    amount_agg['mean_upon_max'] = amount_agg.apply(
        lambda row: row['mean'] / row['max'] if row['max'] != 0 else np.nan, axis=1
    )

    amount_pivot = amount_agg.pivot(index='multibureau__c', columns=['months', 'product'])
    amount_pivot.columns = [f'last_{m}m_clsd_{p}_loan_amt_{stat}' for stat, m, p in amount_pivot.columns]
    amount_pivot = amount_pivot.reset_index()

    stats = ['sum', 'min', 'max', 'mean', 'median', 'std', 'mean_upon_max']
    expected_amt_cols = [f'last_{m}m_clsd_{p}_loan_amt_{stat}' for m in months_list for p in product_list for stat in stats]

    for col in expected_amt_cols:
        if col not in amount_pivot.columns:
            amount_pivot[col] = np.nan

    amount_pivot = amount_pivot[['multibureau__c'] + expected_amt_cols]
    results_df = results_df.merge(amount_pivot, on='multibureau__c', how='left')

    amount_agg1 = cross_df1[cross_df1['present_in_last_n_months'] == 1].groupby(['multibureau__c', 'months', 'product'])['high_credit_or_sanctioned_amount__c'].agg(
                                                                                                                                                                    ['sum',
                                                                                                                                                                     'min',
                                                                                                                                                                     'max',
                                                                                                                                                                     'mean',
                                                                                                                                                                     'median',
                                                                                                                                                                    #  'std'
                                                                                                                                                                     ('std', lambda x: x.std(ddof=0))
                                                                                                                                                                     ]).reset_index()

    amount_agg1['mean_upon_max'] = amount_agg1.apply(
        lambda row: row['mean'] / row['max'] if row['max'] != 0 else np.nan, axis=1
    )

    amount_pivot1 = amount_agg1.pivot(index='multibureau__c', columns=['months', 'product'])
    amount_pivot1.columns = [f'last_{m}m_rng_{p}_loan_amt_{stat}' for stat, m, p in amount_pivot1.columns]
    amount_pivot1 = amount_pivot1.reset_index()

    expected_rng_amt_cols = [f'last_{m}m_rng_{p}_loan_amt_{stat}' for m in months_list for p in product_list for stat in stats]

    for col in expected_rng_amt_cols:
        if col not in amount_pivot1.columns:
            amount_pivot1[col] = np.nan

    amount_pivot1 = amount_pivot1[['multibureau__c'] + expected_rng_amt_cols]
    results_df = results_df.merge(amount_pivot1, on='multibureau__c', how='left')

# Compute % shares of Loan amounts
    if cal_perc_acc_amt:
        acc_amt_cols = results_df.filter(regex=r'^last_\d+m_clsd_(?!overall).*_loan_amt_sum$')
        meta_df = acc_amt_cols.columns.to_series().str.extract(r'last_(\d+)m_clsd_(.+)_loan_amt_sum')
        meta_df.columns = ['month', 'product']
# Compute % shares of Loan amounts for closed loans for each product type out of total closed loans
        meta_df['base_col'] = acc_amt_cols.columns.values
        meta_df['denom_col'] = 'last_' + meta_df['month'] + 'm_clsd_overall_loan_amt_sum'
        meta_df['perc_col'] = 'last_' + meta_df['month'] + 'm_clsd_' + meta_df['product'] + '_upon_clsd_all_loan_amt_perc'

        valid_mask = meta_df['denom_col'].isin(results_df.columns)
        meta_df = meta_df[valid_mask]

        results_df[meta_df['perc_col'].values] = (
            results_df[meta_df['base_col'].values].values /
            results_df[meta_df['denom_col'].values].replace(0, np.nan).values
        ) * 100

        acc_rng_amt_cols = results_df.filter(regex=r'^last_\d+m_rng_.*_loan_amt_sum$')
        meta_df1 = acc_rng_amt_cols.columns.to_series().str.extract(r'last_(\d+)m_rng_(.+)_loan_amt_sum')
        meta_df1.columns = ['month', 'product']
# Compute % shares of Loan amounts for closed loans out of running loans for each product type
        meta_df1['base_col'] = acc_rng_amt_cols.columns.values
        meta_df1['denom_col_same_product'] = 'last_' + meta_df1['month'] + 'm_rng_' + meta_df1['product']+ '_loan_amt_sum'
        meta_df1['perc_col_same_product'] = 'last_' + meta_df1['month'] + 'm_clsd_' + meta_df1['product']+ '_upon_rng_' + meta_df1['product']+ '_loan_amt_perc'

        valid_mask1 = meta_df1['denom_col_same_product'].isin(results_df.columns)
        sub_df = meta_df1[valid_mask1]

        results_df[sub_df['perc_col_same_product'].values] = (
            results_df[sub_df['base_col'].values].values /
            results_df[sub_df['denom_col_same_product'].values].replace(0, np.nan).values
        ) * 100

# Compute % shares of Loan amounts for closed loans for each product type out of total running loans
        meta_df1['denom_col_all_product'] = 'last_' + meta_df1['month'] + 'm_rng_overall_loan_amt_sum'
        meta_df1['perc_col_all_product'] = 'last_' + meta_df1['month'] + 'm_clsd_' + meta_df1['product']+ '_upon_rng_all_loan_amt_perc'

        valid_mask2 = meta_df1['denom_col_all_product'].isin(results_df.columns)
        sub_df1 = meta_df1[valid_mask2]

        results_df[sub_df1['perc_col_all_product'].values] = (
            results_df[sub_df1['base_col'].values].values /
            results_df[sub_df1['denom_col_all_product'].values].replace(0, np.nan).values
        ) * 100

        cols_to_drop = [f'last_{m}m_clsd_overall_upon_rng_all_loan_amt_perc' for m in months_list]
        results_df.drop(columns=cols_to_drop, inplace=True)

# Bin Loan amounts into defined brackets and count frequency
    cuts_dict = loan_amt_cuts_dict()

    bin_meta = pd.DataFrame([
        {
            'product': product,
            'bin_edges': [-np.inf] + cuts + [np.inf],
            'bin_labels': (
                [f'le_{cuts[0]}'] +
                [f'{cuts[i]}_to_{cuts[i+1]}' for i in range(len(cuts) - 1)] +
                [f'gt_{cuts[-1]}']
            )
        }
        for product, cuts in cuts_dict.items()
    ])

    cut_df = cross_df[(cross_df['contrib'] == 1) & (cross_df['high_credit_or_sanctioned_amount__c'].notnull())]
    cut_df = cut_df[cut_df['product'].isin(loan_amt_cuts_product_list)]

# Handle missing columns if no tradelines exist at all for all ids
    if cut_df.empty:
        id_list = df2['multibureau__c'].dropna().unique()
        mp_df = pd.DataFrame(product(months_list, loan_amt_cuts_product_list), columns=['months', 'product'])
        mp_df = mp_df.merge(bin_meta, on='product', how='left')
        mp_df = mp_df.explode('bin_labels', ignore_index=True)
        mp_df['feature'] = ('last_' + mp_df['months'].astype(str) + 'm_clsd_' +
                             mp_df['product'] + '_loan_amt_' +
                             mp_df['bin_labels'])
        feature_meta = mp_df['feature'].tolist()
        all_combinations = pd.MultiIndex.from_product([id_list, feature_meta], names=['multibureau__c', 'feature']).to_frame(index=False)
        final_df = all_combinations.assign(value=0).pivot(index='multibureau__c', columns='feature', values='value').fillna(0).reset_index()
        results_df = results_df.merge(final_df, on='multibureau__c', how='left')

        cols_to_drop = results_df.columns[
            results_df.columns.str.contains(r'(^last_\d+m_rng_.*(_acc_count|_loan_amt_))') &
            ~results_df.columns.str.contains(r'_perc$')
        ]
        results_df.drop(columns=cols_to_drop, inplace=True)
        return results_df




    cut_df = cut_df.merge(bin_meta, on='product', how='left')

    cut_df['bin'] = [
        pd.cut([amt], bins=edges, labels=labels, include_lowest=True)[0]
        for amt, edges, labels in zip(
            cut_df['high_credit_or_sanctioned_amount__c'],
            cut_df['bin_edges'],
            cut_df['bin_labels']
        )
    ]

    cut_df['feature'] = (
        'last_' + cut_df['months'].astype(str) + 'm_clsd_' +
        cut_df['product'] + '_loan_amt_' +
        cut_df['bin'].astype(str)
    )

    id_list = df2['multibureau__c'].dropna().unique()

# Generate all feature column names for binned amounts
    mp_df = pd.DataFrame(product(months_list, loan_amt_cuts_product_list), columns=['months', 'product'])

    mp_df = mp_df.merge(bin_meta, on='product', how='left')

# Create one row per bin label for each month-product pair
    mp_df = mp_df.explode('bin_labels', ignore_index=True)

    mp_df['feature'] = ('last_' + mp_df['months'].astype(str) + 'm_clsd_' +
                         mp_df['product'] + '_loan_amt_' +
                         mp_df['bin_labels']
                       )

    feature_meta = mp_df['feature'].tolist()

# Ensure all ID-feature combinations exist
    all_combinations = pd.MultiIndex.from_product([id_list, feature_meta], names=['multibureau__c', 'feature']).to_frame(index=False)


    agg_df = (cut_df.groupby(['multibureau__c', 'feature'])
                    .size()
                    .reset_index(name='value')
             )

    final_df = (all_combinations.merge(agg_df, on=['multibureau__c', 'feature'], how='left')
                                # .fillna(0)
                                .pivot(index='multibureau__c', columns='feature', values='value')
                                # .fillna(0)
                                .reset_index()
               )


    results_df = results_df.merge(final_df, on='multibureau__c', how='left')

# Drop loan counts and amounts columns for running loans
    cols_to_drop = results_df.columns[
        results_df.columns.str.contains(r'(^last_\d+m_rng_.*(_acc_count|_loan_amt_))') &
        ~results_df.columns.str.contains(r'_perc$')
    ]
    results_df.drop(columns=cols_to_drop, inplace=True)


    return results_df


# ## Before N Years Closed Loans Features Function

# In[19]:


def closed_tradelines_feats_before_n_years(df2, product_list=['overall', 'unsecured'], years_list=[1], cal_perc_acc_cnt=True, cal_perc_acc_amt=True, loan_amt_cuts_product_list=['pl','hl']):

    """
    Generates features for accounts closed before N years for each multibureau ID
    """

    df = df2[~df2['date_closed__c'].isna()].copy(deep=True)
    df1 = df2.copy(deep=True)

    results_df = df1[['multibureau__c']].drop_duplicates().reset_index(drop=True)

# Create cartesian product of input years with input data for flag generation
    df['key'] = 1
    years_df = pd.DataFrame({'years': years_list})
    years_df['key'] = 1
    cross_df = df.merge(years_df, on='key')

    cross_df['days_back'] = cross_df['years'] * 365

# Flag tradelines falling in "before N years" window
    cross_df['contrib'] = (cross_df['date_closed__c'] < cross_df['createddate'] - pd.to_timedelta(cross_df['days_back'], unit='D')
                          ).astype(int)

    if 'overall' not in product_list:
        product_list = ['overall'] + product_list


    product_list = list(set(product_list + loan_amt_cuts_product_list))

# Map Account type number to product types
    product_df = pd.DataFrame([(k, val) for k, vals in product_group_dict()[0].items() for val in vals],
                              columns=['product', 'account_type__c'])
    product_df = product_df[product_df['product'].isin(product_list)]

    cross_df = cross_df.merge(product_df, on='account_type__c', how='left')
    cross_df = cross_df[cross_df['product'].notnull()]

# Count of tradelines by product and years
    acc_group = cross_df.groupby(['multibureau__c', 'years', 'product'])['contrib'].sum().reset_index()
    acc_group = acc_group.pivot(index='multibureau__c', columns=['years', 'product'], values='contrib')
    acc_group.columns = [f'before_{y}y_clsd_{p}_acc_count' for y, p in acc_group.columns]
    acc_group = acc_group.reset_index()

# Handle missing combinations with 0 count
    expected_cnt_cols = [f'before_{y}y_clsd_{p}_acc_count' for y in years_list for p in product_list]
    for col in expected_cnt_cols:
        if col not in acc_group.columns:
            acc_group[col] = 0
    acc_group = acc_group[['multibureau__c'] + expected_cnt_cols]

    results_df = results_df.merge(acc_group, on='multibureau__c', how='left')
    results_df[expected_cnt_cols] = results_df[expected_cnt_cols].fillna(0).astype(int)


    df1['interval'] = pd.IntervalIndex.from_arrays(df1['date_opened_or_disbursed__c'].fillna(pd.Timestamp('1980-01-01')),
                                                   df1['date_closed__c'].fillna(pd.Timestamp('2100-01-01')),
                                                   closed='both'
                                                  )

    df1['key'] = 1
    cross_df1 = df1.merge(years_df, on='key')

    cross_df1['days_back'] = cross_df1['years'] * 365
    cross_df1['date_days_back'] = cross_df1.apply(lambda row: row['createddate'] - pd.to_timedelta(row['days_back'], unit='D'), axis=1)

    cross_df1['present_before_n_years'] = cross_df1.apply(lambda row: row['date_days_back'] in row['interval'], axis=1).astype(int)

    cross_df1 = cross_df1.merge(product_df, on='account_type__c', how='left')
    cross_df1 = cross_df1[cross_df1['product'].notnull()]

    acc_group1 = cross_df1.groupby(['multibureau__c', 'years', 'product'])['present_before_n_years'].sum().reset_index()
    acc_group1 = acc_group1.pivot(index='multibureau__c', columns=['years', 'product'], values='present_before_n_years')
    acc_group1.columns = [f'before_{y}y_rng_{p}_acc_count' for y, p in acc_group1.columns]
    acc_group1 = acc_group1.reset_index()

    expected_rng_cnt_cols = [f'before_{y}y_rng_{p}_acc_count' for y in years_list for p in product_list]
    for col in expected_rng_cnt_cols:
        if col not in acc_group1.columns:
            acc_group1[col] = 0
    acc_group1 = acc_group1[['multibureau__c'] + expected_rng_cnt_cols]
    results_df = results_df.merge(acc_group1, on='multibureau__c', how='left')
    results_df[expected_rng_cnt_cols] = results_df[expected_rng_cnt_cols].fillna(0).astype(int)

# Compute % shares of tradelines counts per product
    if cal_perc_acc_cnt:
        acc_count_cols = results_df.filter(regex=r'^before_\d+y_clsd_(?!overall).*_acc_count$')
        meta_df = acc_count_cols.columns.to_series().str.extract(r'before_(\d+)y_clsd_(.+)_acc_count')
        meta_df.columns = ['year', 'product']
# Compute % shares of Loan counts for closed loans for each product type out of total closed loans
        meta_df['base_col'] = acc_count_cols.columns.values
        meta_df['denom_col'] = 'before_' + meta_df['year'] + 'y_clsd_overall_acc_count'
        meta_df['perc_col'] = 'before_' + meta_df['year'] + 'y_clsd_' + meta_df['product'] + '_upon_clsd_all_acc_perc'

        valid_mask = meta_df['denom_col'].isin(results_df.columns)
        meta_df = meta_df[valid_mask]

        results_df[meta_df['perc_col'].values] = (
            results_df[meta_df['base_col'].values].values /
            results_df[meta_df['denom_col'].values].replace(0, np.nan).values
        ) * 100

        acc_rng_count_cols = results_df.filter(regex=r'^before_\d+y_rng_.*_acc_count$')
        meta_df1 = acc_rng_count_cols.columns.to_series().str.extract(r'before_(\d+)y_rng_(.+)_acc_count')
        meta_df1.columns = ['year', 'product']
# Compute % shares of Loan counts for closed loans out of running loans for each product type
        meta_df1['base_col'] = acc_rng_count_cols.columns.values
        meta_df1['denom_col_same_product'] = 'before_' + meta_df1['year'] + 'y_rng_' + meta_df1['product']+ '_acc_count'
        meta_df1['perc_col_same_product'] = 'before_' + meta_df1['year'] + 'y_clsd_' + meta_df1['product']+ '_upon_rng_' + meta_df1['product']+ '_acc_perc'

        valid_mask1 = meta_df1['denom_col_same_product'].isin(results_df.columns)
        sub_df = meta_df1[valid_mask1]

        results_df[sub_df['perc_col_same_product'].values] = (
            results_df[sub_df['base_col'].values].values /
            results_df[sub_df['denom_col_same_product'].values].replace(0, np.nan).values
        ) * 100

# Compute % shares of Loan counts for closed loans for each product type out of total running loans
        meta_df1['denom_col_all_product'] = 'before_' + meta_df1['year'] + 'y_rng_overall_acc_count'
        meta_df1['perc_col_all_product'] = 'before_' + meta_df1['year'] + 'y_clsd_' + meta_df1['product']+ '_upon_rng_all_acc_perc'

        valid_mask2 = meta_df1['denom_col_all_product'].isin(results_df.columns)
        sub_df1 = meta_df1[valid_mask2]

        results_df[sub_df1['perc_col_all_product'].values] = (
            results_df[sub_df1['base_col'].values].values /
            results_df[sub_df1['denom_col_all_product'].values].replace(0, np.nan).values
        ) * 100

        cols_to_drop = [f'before_{y}y_clsd_overall_upon_rng_all_acc_perc' for y in years_list]
        results_df.drop(columns=cols_to_drop, inplace=True)


# Aggregated features on Loan amount
    amount_agg = cross_df[cross_df['contrib'] == 1].groupby(['multibureau__c', 'years', 'product'])['high_credit_or_sanctioned_amount__c'].agg(
                                                                                                                                              ['sum',
                                                                                                                                               'min',
                                                                                                                                               'max',
                                                                                                                                               'mean',
                                                                                                                                               'median',
                                                                                                                                              #  'std'
                                                                                                                                               ('std', lambda x: x.std(ddof=0))
                                                                                                                                               ]).reset_index()

# Add mean_upon_max ratio features
    amount_agg['mean_upon_max'] = amount_agg.apply(
        lambda row: row['mean'] / row['max'] if row['max'] != 0 else np.nan, axis=1
    )

    amount_pivot = amount_agg.pivot(index='multibureau__c', columns=['years', 'product'])
    amount_pivot.columns = [f'before_{y}y_clsd_{p}_loan_amt_{stat}' for stat, y, p in amount_pivot.columns]
    amount_pivot = amount_pivot.reset_index()


    stats = ['sum', 'min', 'max', 'mean', 'median', 'std', 'mean_upon_max']

# Handle missing amount stat columns
    expected_amt_cols = [f'before_{y}y_clsd_{p}_loan_amt_{stat}' for y in years_list for p in product_list for stat in stats]

    for col in expected_amt_cols:
        if col not in amount_pivot.columns:
            amount_pivot[col] = np.nan

    amount_pivot = amount_pivot[['multibureau__c'] + expected_amt_cols]
    results_df = results_df.merge(amount_pivot, on='multibureau__c', how='left')


    amount_agg1 = cross_df1[cross_df1['present_before_n_years'] == 1].groupby(['multibureau__c', 'years', 'product'])['high_credit_or_sanctioned_amount__c'].agg(
                                                                                                                                                                ['sum',
                                                                                                                                                                 'min',
                                                                                                                                                                 'max',
                                                                                                                                                                 'mean',
                                                                                                                                                                 'median',
                                                                                                                                                                #  'std'
                                                                                                                                                                 ('std', lambda x: x.std(ddof=0))
                                                                                                                                                                 ]).reset_index()


    amount_agg1['mean_upon_max'] = amount_agg1.apply(
        lambda row: row['mean'] / row['max'] if row['max'] != 0 else np.nan, axis=1
    )

    amount_pivot1 = amount_agg1.pivot(index='multibureau__c', columns=['years', 'product'])
    amount_pivot1.columns = [f'before_{y}y_rng_{p}_loan_amt_{stat}' for stat, y, p in amount_pivot1.columns]
    amount_pivot1 = amount_pivot1.reset_index()

    expected_rng_amt_cols = [f'before_{y}y_rng_{p}_loan_amt_{stat}' for y in years_list for p in product_list for stat in stats]

    for col in expected_rng_amt_cols:
        if col not in amount_pivot1.columns:
            amount_pivot1[col] = np.nan

    amount_pivot1 = amount_pivot1[['multibureau__c'] + expected_rng_amt_cols]
    results_df = results_df.merge(amount_pivot1, on='multibureau__c', how='left')

# Compute % shares of Loan amounts
    if cal_perc_acc_amt:
        acc_amt_cols = results_df.filter(regex=r'^before_\d+y_clsd_(?!overall).*_loan_amt_sum$')
        meta_df = acc_amt_cols.columns.to_series().str.extract(r'before_(\d+)y_clsd_(.+)_loan_amt_sum')
        meta_df.columns = ['year', 'product']
# Compute % shares of Loan amounts for closed loans for each product type out of total closed loans
        meta_df['base_col'] = acc_amt_cols.columns.values
        meta_df['denom_col'] = 'before_' + meta_df['year'] + 'y_clsd_overall_loan_amt_sum'
        meta_df['perc_col'] = 'before_' + meta_df['year'] + 'y_clsd_' + meta_df['product'] + '_upon_clsd_all_loan_amt_perc'
        valid_mask = meta_df['denom_col'].isin(results_df.columns)
        meta_df = meta_df[valid_mask]

        results_df[meta_df['perc_col'].values] = (
            results_df[meta_df['base_col'].values].values /
            results_df[meta_df['denom_col'].values].replace(0, np.nan).values
        ) * 100

        acc_rng_amt_cols = results_df.filter(regex=r'^before_\d+y_rng_.*_loan_amt_sum$')
        meta_df1 = acc_rng_amt_cols.columns.to_series().str.extract(r'before_(\d+)y_rng_(.+)_loan_amt_sum')
        meta_df1.columns = ['year', 'product']
# Compute % shares of Loan amounts for closed loans out of running loans for each product type
        meta_df1['base_col'] = acc_rng_amt_cols.columns.values
        meta_df1['denom_col_same_product'] = 'before_' + meta_df1['year'] + 'y_rng_' + meta_df1['product']+ '_loan_amt_sum'
        meta_df1['perc_col_same_product'] = 'before_' + meta_df1['year'] + 'y_clsd_' + meta_df1['product']+ '_upon_rng_' + meta_df1['product']+ '_loan_amt_perc'

        valid_mask1 = meta_df1['denom_col_same_product'].isin(results_df.columns)
        sub_df = meta_df1[valid_mask1]

        results_df[sub_df['perc_col_same_product'].values] = (
            results_df[sub_df['base_col'].values].values /
            results_df[sub_df['denom_col_same_product'].values].replace(0, np.nan).values
        ) * 100

# Compute % shares of Loan amounts for closed loans for each product type out of total running loans
        meta_df1['denom_col_all_product'] = 'before_' + meta_df1['year'] + 'y_rng_overall_loan_amt_sum'
        meta_df1['perc_col_all_product'] = 'before_' + meta_df1['year'] + 'y_clsd_' + meta_df1['product']+ '_upon_rng_all_loan_amt_perc'

        valid_mask2 = meta_df1['denom_col_all_product'].isin(results_df.columns)
        sub_df1 = meta_df1[valid_mask2]

        results_df[sub_df1['perc_col_all_product'].values] = (
            results_df[sub_df1['base_col'].values].values /
            results_df[sub_df1['denom_col_all_product'].values].replace(0, np.nan).values
        ) * 100

        cols_to_drop = [f'before_{y}y_clsd_overall_upon_rng_all_loan_amt_perc' for y in years_list]
        results_df.drop(columns=cols_to_drop, inplace=True)

# Bin Loan amounts into defined brackets and count frequency
    cuts_dict = loan_amt_cuts_dict()

    bin_meta = pd.DataFrame([
        {
            'product': product,
            'bin_edges': [-np.inf] + cuts + [np.inf],
            'bin_labels': (
                [f'le_{cuts[0]}'] +
                [f'{cuts[i]}_to_{cuts[i+1]}' for i in range(len(cuts) - 1)] +
                [f'gt_{cuts[-1]}']
            )
        }
        for product, cuts in cuts_dict.items()
    ])

    cut_df = cross_df[(cross_df['contrib'] == 1) & (cross_df['high_credit_or_sanctioned_amount__c'].notnull())]
    cut_df = cut_df[cut_df['product'].isin(loan_amt_cuts_product_list)]

# Handle missing columns if no tradelines exist at all for all ids
    if cut_df.empty:
        id_list = df2['multibureau__c'].dropna().unique()
        mp_df = pd.DataFrame(product(years_list, loan_amt_cuts_product_list), columns=['years', 'product'])
        mp_df = mp_df.merge(bin_meta, on='product', how='left')
        mp_df = mp_df.explode('bin_labels', ignore_index=True)
        mp_df['feature'] = ('before_' + mp_df['years'].astype(str) + 'y_clsd_' +
                             mp_df['product'] + '_loan_amt_' +
                             mp_df['bin_labels']
                           )
        feature_meta = mp_df['feature'].tolist()
        all_combinations = pd.MultiIndex.from_product([id_list, feature_meta], names=['multibureau__c', 'feature']).to_frame(index=False)
        final_df = all_combinations.assign(value=0).pivot(index='multibureau__c', columns='feature', values='value').fillna(0).reset_index()
        results_df = results_df.merge(final_df, on='multibureau__c', how='left')

        cols_to_drop = results_df.columns[
            results_df.columns.str.contains(r'(^before_\d+y_rng_.*(_acc_count|_loan_amt_))') &
            ~results_df.columns.str.contains(r'_perc$')
        ]
        results_df.drop(columns=cols_to_drop, inplace=True)
        return results_df

    cut_df = cut_df.merge(bin_meta, on='product', how='left')

    cut_df['bin'] = [
        pd.cut([amt], bins=edges, labels=labels, include_lowest=True)[0]
        for amt, edges, labels in zip(
            cut_df['high_credit_or_sanctioned_amount__c'],
            cut_df['bin_edges'],
            cut_df['bin_labels']
        )
    ]

    cut_df['feature'] = (
        'before_' + cut_df['years'].astype(str) + 'y_clsd_' +
        cut_df['product'] + '_loan_amt_' +
        cut_df['bin'].astype(str)
    )

    id_list = df2['multibureau__c'].dropna().unique()

# Generate all feature column names for binned amounts
    mp_df = pd.DataFrame(product(years_list, loan_amt_cuts_product_list), columns=['years', 'product'])

    mp_df = mp_df.merge(bin_meta, on='product', how='left')

# Create one row per bin label for each year-product pair
    mp_df = mp_df.explode('bin_labels', ignore_index=True)

    mp_df['feature'] = ('before_' + mp_df['years'].astype(str) + 'y_clsd_' +
                         mp_df['product'] + '_loan_amt_' +
                         mp_df['bin_labels']
                       )

    feature_meta = mp_df['feature'].tolist()

# Ensure all ID-feature combinations exist
    all_combinations = pd.MultiIndex.from_product([id_list, feature_meta], names=['multibureau__c', 'feature']).to_frame(index=False)

    agg_df = (cut_df.groupby(['multibureau__c', 'feature'])
                    .size()
                    .reset_index(name='value')
             )

    final_df = (all_combinations.merge(agg_df, on=['multibureau__c', 'feature'], how='left')
                                # .fillna(0)
                                .pivot(index='multibureau__c', columns='feature', values='value')
                                # .fillna(0)
                                .reset_index()
               )


    results_df = results_df.merge(final_df, on='multibureau__c', how='left')

# Drop loan counts and amounts columns for running loans
    cols_to_drop = results_df.columns[
        results_df.columns.str.contains(r'(^before_\d+y_rng_.*(_acc_count|_loan_amt_))') &
        ~results_df.columns.str.contains(r'_perc$')
    ]
    results_df.drop(columns=cols_to_drop, inplace=True)


    return results_df


# ## Last N Months Opened Loans Features Function

# In[20]:


def opened_tradelines_feats_last_n_months(df2, product_list=['overall', 'unsecured'], months_list=[3, 6],
                                          cal_perc_acc_cnt = True, cal_perc_acc_amt = True, loan_amt_cuts_product_list=['pl', 'hl']):

    """
    Generate features for accounts opened over the last N months for each multibureau ID
    """

    df = df2.copy(deep=True)

    for col in ['createddate', 'date_opened_or_disbursed__c', 'date_closed__c']:
        df[col] = pd.to_datetime(pd.to_datetime(df[col]).dt.date)


    results_df = pd.DataFrame()
    results_df['multibureau__c'] = df['multibureau__c'].unique()



    if 'overall' not in product_list:
        product_list = list(['overall'] + product_list)

    product_list = list(set(product_list + loan_amt_cuts_product_list))


# Map Account type number to product types
    product_df = pd.DataFrame([(k, val) for k, vals in product_group_dict()[0].items() for val in vals],
                              columns=['product', 'account_type__c'])

    product_df = product_df[product_df['product'].isin(product_list)]

    df_prod = df.merge(product_df, on='account_type__c', how='left')
    df_prod = df_prod[df_prod['product'].notnull()]



# Create cartesian product of input months with input data for flag generation
    df['key'] = 1
    months_df = pd.DataFrame({'months': months_list})
    months_df['key'] = 1
    cross_df = df.merge(months_df, on='key')
    months_to_days = months_to_days_dict(months_list)
    cross_df['days_back'] = cross_df['months'].astype(str).map(months_to_days)



# Flag tradelines falling in "last N months" window
    cross_df['contrib'] = (cross_df['date_opened_or_disbursed__c'] >= cross_df['createddate'] - pd.to_timedelta(cross_df['days_back'], unit='D')).astype(int)

    product_df = product_df[product_df['product'].isin(product_list)]
    cross_df = cross_df.merge(product_df, on='account_type__c', how='left')
    cross_df = cross_df[cross_df['product'].notnull()]


# Count of tradelines by product and months
    acc_group = cross_df.groupby(['multibureau__c', 'months', 'product'])['contrib'].sum().reset_index()
    acc_group = acc_group.pivot(index='multibureau__c', columns=['months', 'product'], values='contrib')
    acc_group.columns = [f'last_{m}m_opnd_{p}_acc_count' for m, p in acc_group.columns]
    acc_group = acc_group.reset_index()


# Handle missing combinations with 0 count
    expected_cnt_cols = [f'last_{m}m_opnd_{p}_acc_count'
                         for m in months_list
                         for p in product_list]

    for col in expected_cnt_cols:
        if col not in acc_group.columns:
            acc_group[col] = 0

    acc_group = acc_group[['multibureau__c'] + expected_cnt_cols]
    results_df = results_df.merge(acc_group, on='multibureau__c', how='left')
    results_df[expected_cnt_cols] = results_df[expected_cnt_cols].fillna(0).astype(int)



# Compute % share of tradelines counts per product
    if cal_perc_acc_cnt:
        acc_count_cols = results_df.filter(regex=r'^last_\d+m_opnd_(?!overall).*_acc_count$')
        meta_df = acc_count_cols.columns.to_series().str.extract(r'last_(\d+)m_opnd_(.+)_acc_count')
        meta_df.columns = ['month', 'product']
        meta_df['base_col'] = acc_count_cols.columns.values
        meta_df['denom_col'] = 'last_' + meta_df['month'] + 'm_opnd_overall_acc_count'
        meta_df['perc_col'] = 'last_' + meta_df['month'] + 'm_opnd_' + meta_df['product'] + '_acc_perc'

        valid_mask = meta_df['denom_col'].isin(results_df.columns)
        meta_df = meta_df[valid_mask]

        results_df[meta_df['perc_col'].values] = (
            results_df[meta_df['base_col'].values].values /
            results_df[meta_df['denom_col'].values].replace(0, np.nan).values
        ) * 100



# Aggregated statistic features on Loan amount
    amount_agg = cross_df[cross_df['contrib'] == 1].groupby(['multibureau__c', 'months', 'product'])['high_credit_or_sanctioned_amount__c'].agg(['sum',
                                                                                                                                                 'min',
                                                                                                                                                 'max',
                                                                                                                                                 'mean',
                                                                                                                                                 'median',
                                                                                                                                                 ('std', lambda x: x.std(ddof=0))
                                                                                                                                                 ]).reset_index()

# Add mean_upon_max ratio features
    amount_agg['mean_upon_max'] = amount_agg.apply(
        lambda row: row['mean'] / row['max'] if row['max'] != 0 else np.nan, axis=1
    )

    amount_pivot = amount_agg.pivot(index='multibureau__c', columns=['months', 'product'])
    amount_pivot.columns = [f'last_{m}m_opnd_{p}_loan_amt_{stat}' for stat, m, p in amount_pivot.columns]
    amount_pivot = amount_pivot.reset_index()

    stats = ['sum', 'min', 'max', 'mean', 'median', 'std', 'mean_upon_max']
    expected_amt_cols = [f'last_{m}m_opnd_{p}_loan_amt_{stat}'
                         for m in months_list
                         for p in product_list
                         for stat in stats]

# Handle missing amount stat columns
    for col in expected_amt_cols:
        if col not in amount_pivot.columns:
            amount_pivot[col] = np.nan


    amount_pivot = amount_pivot[['multibureau__c'] + expected_amt_cols]
    results_df = results_df.merge(amount_pivot, on='multibureau__c', how='left')

# Compute % share of Loan amount
    if cal_perc_acc_amt:
        acc_amt_sum_cols = results_df.filter(regex=r'^last_\d+m_opnd_(?!overall).*_loan_amt_sum$')
        meta_amt_df = acc_amt_sum_cols.columns.to_series().str.extract(r'last_(\d+)m_opnd_(.+)_loan_amt_sum')
        meta_amt_df.columns = ['month', 'product']
        meta_amt_df['base_col'] = acc_amt_sum_cols.columns.values
        meta_amt_df['denom_col'] = 'last_' + meta_amt_df['month'] + 'm_opnd_overall_loan_amt_sum'
        meta_amt_df['ratio_col'] = 'perc_' + meta_amt_df['product'] + '_loan_amount_last_' + meta_amt_df['month'] + 'm_opnd'

        valid_amt_mask = meta_amt_df['denom_col'].isin(results_df.columns)
        meta_amt_df = meta_amt_df[valid_amt_mask]
        results_df[meta_amt_df['ratio_col'].values] = (
            results_df[meta_amt_df['base_col'].values].values /
            results_df[meta_amt_df['denom_col'].values].replace(0, np.nan).values
        ) * 100



# Bin Loan amounts into defined brackets and count frequency
    cuts_dict = loan_amt_cuts_dict()

    bin_meta = pd.DataFrame([
        {
            'product': product,
            'bin_edges': [-np.inf] + cuts + [np.inf],
            'bin_labels': (
                [f'le_{cuts[0]}'] +
                [f'{cuts[i]}_to_{cuts[i+1]}' for i in range(len(cuts) - 1)] +
                [f'gt_{cuts[-1]}']
            )
        }
        for product, cuts in cuts_dict.items()
    ])

    cut_df = cross_df[(cross_df['contrib'] == 1) & (cross_df['high_credit_or_sanctioned_amount__c'].notnull())]
    cut_df = cut_df[cut_df['product'].isin(loan_amt_cuts_product_list)]


# Handle missing columns if no tradelines exist at all for all ids
    if cut_df.empty:
        id_list = df2['multibureau__c'].dropna().unique()
        mp_df = pd.DataFrame(product(months_list, loan_amt_cuts_product_list), columns=['months', 'product'])
        mp_df = mp_df.merge(bin_meta, on='product', how='left')
        mp_df = mp_df.explode('bin_labels', ignore_index=True)
        mp_df['feature'] = ('last_' + mp_df['months'].astype(str) + 'm_opnd_' +
                             mp_df['product'] + '_loan_amt_' +
                             mp_df['bin_labels']
                           )
        feature_meta = mp_df['feature'].tolist()
        all_combinations = pd.MultiIndex.from_product([id_list, feature_meta], names=['multibureau__c', 'feature']).to_frame(index=False)
        final_df = all_combinations.assign(value=0).pivot(index='multibureau__c', columns='feature', values='value').fillna(0).reset_index()
        results_df = results_df.merge(final_df, on='multibureau__c', how='left')
        return results_df

    cut_df = cut_df.merge(bin_meta, on='product', how='left')

    cut_df['bin'] = [
        pd.cut([amt], bins=edges, labels=labels, include_lowest=True)[0]
        for amt, edges, labels in zip(
            cut_df['high_credit_or_sanctioned_amount__c'],
            cut_df['bin_edges'],
            cut_df['bin_labels']
        )
    ]

    cut_df['feature'] = (
        'last_' + cut_df['months'].astype(str) + 'm_opnd_' +
        cut_df['product'] + '_loan_amt_' +
        cut_df['bin'].astype(str)
    )

    id_list = df2['multibureau__c'].dropna().unique()



# Generate all feature column names for binned amounts
    mp_df = pd.DataFrame(product(months_list, loan_amt_cuts_product_list), columns=['months', 'product'])

    mp_df = mp_df.merge(bin_meta, on='product', how='left')



 # Create one row per bin label for each month-product pair
    mp_df = mp_df.explode('bin_labels', ignore_index=True)

    mp_df['feature'] = ('last_' + mp_df['months'].astype(str) + 'm_opnd_' +
                         mp_df['product'] + '_loan_amt_' +
                         mp_df['bin_labels']
                       )

    feature_meta = mp_df['feature'].tolist()


# Ensure all ID-feature combinations exist
    all_combinations = pd.MultiIndex.from_product([id_list, feature_meta], names=['multibureau__c', 'feature']).to_frame(index=False)

    agg_df = (cut_df.groupby(['multibureau__c', 'feature'])
                    .size()
                    .reset_index(name='value')
             )

    final_df = (all_combinations.merge(agg_df, on=['multibureau__c', 'feature'], how='left')
                                # .fillna(0)
                                .pivot(index='multibureau__c', columns='feature', values='value')
                                # .fillna(0)
                                .reset_index()
               )


    results_df = results_df.merge(final_df, on='multibureau__c', how='left')

    return results_df


# ## Before N Years Opened Loans Features Function

# In[21]:


def opened_tradelines_feats_before_n_years(df2, product_list=['overall', 'unsecured'], years_list = [1], cal_perc_acc_cnt=True,
                                           cal_perc_acc_amt=True, loan_amt_cuts_product_list=['pl', 'hl']):

    """
    Generate features for accounts opened before N years for each multibureau ID
    """

    df = df2.copy(deep=True)

    for col in ['createddate', 'date_opened_or_disbursed__c', 'date_closed__c']:
        df[col] = pd.to_datetime(pd.to_datetime(df[col]).dt.date)

    results_df = pd.DataFrame()
    results_df['multibureau__c'] = df['multibureau__c'].unique()


# Create cartesian product of input years with input data for flag generation
    df['key'] = 1
    years_df = pd.DataFrame({'years': years_list})
    years_df['key'] = 1
    cross_df = df.merge(years_df, on='key')
    cross_df['cutoff_date'] = cross_df['createddate'] - pd.to_timedelta(cross_df['years'] * 365, unit='D')

# Flag tradelines falling in "before N years" window
    cross_df['contrib'] = (cross_df['date_opened_or_disbursed__c'] < cross_df['cutoff_date']).astype(int)

    if 'overall' not in product_list:
        product_list = list(['overall'] + product_list)
    product_list = list(set(product_list + loan_amt_cuts_product_list))


# Map Account type number to product types
    product_df = pd.DataFrame([(k, val) for k, vals in product_group_dict()[0].items() for val in vals], columns=['product', 'account_type__c'])

    product_df = product_df[product_df['product'].isin(product_list)]
    cross_df = cross_df.merge(product_df, on='account_type__c', how='left')
    cross_df = cross_df[cross_df['product'].notnull()]



# Count of tradelines by product and years
    acc_group = cross_df.groupby(['multibureau__c', 'years', 'product'])['contrib'].sum().reset_index()
    acc_group = acc_group.pivot(index='multibureau__c', columns=['years', 'product'], values='contrib')
    acc_group.columns = [f'before_{y}y_opnd_{p}_acc_count' for y, p in acc_group.columns]
    acc_group = acc_group.reset_index()

    expected_cnt_cols = [f'before_{y}y_opnd_{p}_acc_count' for y in years_list for p in product_list]


# Handle missing combinations with 0 count
    for col in expected_cnt_cols:
        if col not in acc_group.columns:
            acc_group[col] = 0
    acc_group = acc_group[['multibureau__c'] + expected_cnt_cols]

    results_df = results_df.merge(acc_group, on='multibureau__c', how='left')
    results_df[expected_cnt_cols] = results_df[expected_cnt_cols].fillna(0).astype(int)


# Compute % share of tradelines counts per product
    if cal_perc_acc_cnt:
        acc_count_cols = results_df.filter(regex=r'^before_\d+y_opnd_(?!overall).*_acc_count$')
        meta_df = acc_count_cols.columns.to_series().str.extract(r'before_(\d+)y_opnd_(.+)_acc_count')
        meta_df.columns = ['year', 'product']
        meta_df['base_col'] = acc_count_cols.columns.values
        meta_df['denom_col'] = 'before_' + meta_df['year'] + 'y_opnd_overall_acc_count'
        meta_df['perc_col'] = 'before_' + meta_df['year'] + 'y_opnd_' + meta_df['product'] + '_acc_perc'
        valid_mask = meta_df['denom_col'].isin(results_df.columns)
        meta_df = meta_df[valid_mask]
        results_df[meta_df['perc_col'].values] = (
            results_df[meta_df['base_col'].values].values /
            results_df[meta_df['denom_col'].values].replace(0, np.nan).values
        )*100



# Aggregated features on Loan amount
    amount_agg = cross_df[cross_df['contrib'] == 1].groupby(['multibureau__c', 'years', 'product'])['high_credit_or_sanctioned_amount__c'].agg(['sum',
                                                                                                                                                'min',
                                                                                                                                                'max',
                                                                                                                                                'mean',
                                                                                                                                                'median',
                                                                                                                                                ('std', lambda x: x.std(ddof=0))
                                                                                                                                                ]).reset_index()

# Add mean_upon_max ratio features
    amount_agg['mean_upon_max'] = amount_agg.apply(
        lambda row: row['mean'] / row['max'] if row['max'] != 0 else np.nan, axis=1
    )

    amount_pivot = amount_agg.pivot(index='multibureau__c', columns=['years', 'product'])
    amount_pivot.columns = [f'before_{y}y_opnd_{p}_loan_amt_{stat}' for stat, y, p in amount_pivot.columns]
    amount_pivot = amount_pivot.reset_index()

    stats = ['sum', 'min', 'max', 'mean', 'median', 'std', 'mean_upon_max']

# Handle missing amount stat columns
    expected_amt_cols = [f'before_{y}y_opnd_{p}_loan_amt_{stat}' for y in years_list for p in product_list for stat in stats]

    for col in expected_amt_cols:
        if col not in amount_pivot.columns:
            amount_pivot[col] = np.nan

    amount_pivot = amount_pivot[['multibureau__c'] + expected_amt_cols]
    results_df = results_df.merge(amount_pivot, on='multibureau__c', how='left')


# Compute % share of Loan amount Features
    if cal_perc_acc_amt:
        acc_amt_sum_cols = results_df.filter(regex=r'^before_\d+y_opnd_(?!overall).*_loan_amt_sum$')
        meta_amt_df = acc_amt_sum_cols.columns.to_series().str.extract(r'before_(\d+)y_opnd_(.+)_loan_amt_sum')
        meta_amt_df.columns = ['year', 'product']
        meta_amt_df['base_col'] = acc_amt_sum_cols.columns.values
        meta_amt_df['denom_col'] = 'before_' + meta_amt_df['year'] + 'y_opnd_overall_loan_amt_sum'
        meta_amt_df['ratio_col'] = 'perc_' + meta_amt_df['product'] + '_loan_amt_before_' + meta_amt_df['year'] + 'y_opnd'
        valid_amt_mask = meta_amt_df['denom_col'].isin(results_df.columns)
        meta_amt_df = meta_amt_df[valid_amt_mask]
        results_df[meta_amt_df['ratio_col'].values] = (
            results_df[meta_amt_df['base_col'].values].values /
            results_df[meta_amt_df['denom_col'].values].replace(0, np.nan).values
        )*100



# Bin Loan amounts into defined brackets and count frequency
    cuts_dict = loan_amt_cuts_dict()

    bin_meta = pd.DataFrame([
        {
            'product': product,
            'bin_edges': [-np.inf] + cuts + [np.inf],
            'bin_labels': (
                [f'le_{cuts[0]}'] +
                [f'{cuts[i]}_to_{cuts[i+1]}' for i in range(len(cuts) - 1)] +
                [f'gt_{cuts[-1]}']
            )
        }
        for product, cuts in cuts_dict.items()
    ])

    cut_df = cross_df[(cross_df['contrib'] == 1) & (cross_df['high_credit_or_sanctioned_amount__c'].notnull())]
    cut_df = cut_df[cut_df['product'].isin(loan_amt_cuts_product_list)]

# Handle missing columns if no tradelines exist at all for all ids
    if cut_df.empty:
        id_list = df2['multibureau__c'].dropna().unique()
        mp_df = pd.DataFrame(product(years_list, loan_amt_cuts_product_list), columns=['years', 'product'])
        mp_df = mp_df.merge(bin_meta, on='product', how='left')
        mp_df = mp_df.explode('bin_labels', ignore_index=True)
        mp_df['feature'] = ('before_' + mp_df['years'].astype(str) + 'y_opnd_' +
                             mp_df['product'] + '_loan_amt_' +
                             mp_df['bin_labels']
                           )
        feature_meta = mp_df['feature'].tolist()
        all_combinations = pd.MultiIndex.from_product([id_list, feature_meta], names=['multibureau__c', 'feature']).to_frame(index=False)
        final_df = all_combinations.assign(value=0).pivot(index='multibureau__c', columns='feature', values='value').fillna(0).reset_index()
        results_df = results_df.merge(final_df, on='multibureau__c', how='left')
        return results_df



    cut_df = cut_df.merge(bin_meta, on='product', how='left')

    cut_df['bin'] = [
        pd.cut([amt], bins=edges, labels=labels, include_lowest=True)[0]
        for amt, edges, labels in zip(
            cut_df['high_credit_or_sanctioned_amount__c'],
            cut_df['bin_edges'],
            cut_df['bin_labels']
        )
    ]

    cut_df['feature'] = (
        'before_' + cut_df['years'].astype(str) + 'y_' +
        cut_df['product'] + '_loan_amt_' +
        cut_df['bin'].astype(str)
    )

    id_list = df2['multibureau__c'].dropna().unique()


# Generate all feature column names for binned amounts
    mp_df = pd.DataFrame(product(years_list, loan_amt_cuts_product_list), columns=['years', 'product'])
    mp_df = mp_df.merge(bin_meta, on='product', how='left')


 # Create one row per bin label for each month-product pair
    mp_df = mp_df.explode('bin_labels', ignore_index=True)
    mp_df['feature'] = ('before_' + mp_df['years'].astype(str) + 'y_opnd_' +
                         mp_df['product'] + '_loan_amt_' +
                         mp_df['bin_labels']
                       )

    feature_meta = mp_df['feature'].tolist()

# Ensure all ID-feature combinations exist
    all_combinations = pd.MultiIndex.from_product([id_list, feature_meta], names=['multibureau__c', 'feature']).to_frame(index=False)

    agg_df = (cut_df.groupby(['multibureau__c', 'feature'])
                    .size()
                    .reset_index(name='value')
             )

    final_df = (all_combinations.merge(agg_df, on=['multibureau__c', 'feature'], how='left')
                                # .fillna(0)
                                .pivot(index='multibureau__c', columns='feature', values='value')
                                # .fillna(0)
                                .reset_index()
               )


    results_df = results_df.merge(final_df, on='multibureau__c', how='left')

    return results_df


# ## Live Loans Features Function

# In[22]:


def live_tradelines_features(df2, product_list=['overall', 'unsecured'], loan_amt_cuts_product_list=['pl', 'hl'], cal_perc_curr_bal=True,
                             cal_cb_to_sanc_ratio=True, cal_perc_acc_cnt=True, cal_perc_acc_amt=True):

    """
    Generates features for live loans for each multibureau ID
    """

    df = df2.copy(deep=True)


    for col in ['createddate', 'date_opened_or_disbursed__c', 'date_closed__c']:
        df[col] = pd.to_datetime(df[col].astype(str).str[:10])

    results_df = pd.DataFrame()
    results_df['multibureau__c'] = df['multibureau__c'].unique()


    df = df[(df['date_closed__c'].isna()) | (df['date_closed__c'] > df['createddate'])]


    if 'overall' not in product_list:
        product_list = ['overall'] + product_list
    product_list = list(set(product_list + loan_amt_cuts_product_list))

# Map Account type number to product types
    product_df = pd.DataFrame(
        [(k, val) for k, vals in product_group_dict()[0].items() for val in vals],
        columns=['product', 'account_type__c']
    )
    product_df = product_df[product_df['product'].isin(product_list)]

    df_og = df.merge(product_df, on='account_type__c', how='left')
    df_og = df_og[df_og['product'].notnull()]

# Count of tradelines by product
    acc_group = df_og.groupby(['multibureau__c', 'product'])['id'].count().unstack().fillna(0).astype(int).reset_index()
    acc_group.columns = ['multibureau__c'] + [f'live_{col}_acc_count' for col in acc_group.columns[1:]]

    expected_cnt_cols = [f'live_{p}_acc_count'
                         for p in product_list]

# Handle missing combinations with 0 count
    for col in expected_cnt_cols:
        if col not in acc_group.columns:
            acc_group[col] = 0

    results_df = results_df.merge(acc_group, on='multibureau__c', how='left')

# Compute % share of tradelines counts per product
    if cal_perc_acc_cnt:
        acc_count_cols = results_df.filter(regex=r'^live_(?!overall).*_acc_count$')
        meta_df = acc_count_cols.columns.to_series().str.extract(r'live_(.+)_acc_count')
        meta_df.columns = ['product']
        meta_df['base_col'] = acc_count_cols.columns.values
        meta_df['denom_col'] = 'live_overall_acc_count'
        meta_df['perc_col'] = 'live_' + meta_df['product'] + '_acc_perc'

        valid_mask = meta_df['denom_col'].isin(results_df.columns)
        meta_df = meta_df[valid_mask]

        results_df[meta_df['perc_col'].values] = (
            results_df[meta_df['base_col'].values].values /
            results_df[meta_df['denom_col'].values].replace(0, np.nan).values
        ) * 100


# Aggregated features on Loan amount
    amount_agg = df_og.groupby(['multibureau__c', 'product'])['high_credit_or_sanctioned_amount__c'].agg(['sum',
                                                                                                          'min',
                                                                                                          'max',
                                                                                                          'mean',
                                                                                                          'median',
                                                                                                          ('std', lambda x: x.std(ddof=0))
                                                                                                          ]).reset_index()
# Add mean_upon_max ratio features
    amount_agg['mean_upon_max'] = amount_agg.apply(
        lambda row: row['mean'] / row['max'] if row['max'] != 0 else np.nan, axis=1
    )

    amount_pivot = amount_agg.pivot(index='multibureau__c', columns='product')
    amount_pivot.columns = [f'live_{p}_loan_amt_{stat}' for stat, p in amount_pivot.columns]
    amount_pivot = amount_pivot.reset_index()



    stats = ['sum', 'min', 'max', 'mean', 'median', 'std', 'mean_upon_max']

# Handle missing amount stat columns
    expected_amt_cols = [f'live_{p}_loan_amt_{stat}'
                         for p in product_list
                         for stat in stats]

    for col in expected_amt_cols:
        if col not in amount_pivot.columns:
            amount_pivot[col] = np.nan

    amount_pivot = amount_pivot[['multibureau__c'] + expected_amt_cols]
    results_df = results_df.merge(amount_pivot, on='multibureau__c', how='left')

# Compute % share of Loan amount Features
    if cal_perc_acc_amt:
        acc_amt_sum_cols = results_df.filter(regex=r'^live_(?!overall).*_loan_amt_sum$')
        meta_amt_df = acc_amt_sum_cols.columns.to_series().str.extract(r'live_(.+)_loan_amt_sum')
        meta_amt_df.columns = ['product']
        meta_amt_df['base_col'] = acc_amt_sum_cols.columns.values
        meta_amt_df['denom_col'] = 'live_overall_loan_amt_sum'
        meta_amt_df['ratio_col'] = 'perc_live_' + meta_amt_df['product'] + '_loan_amount'

        valid_amt_mask = meta_amt_df['denom_col'].isin(results_df.columns)
        meta_amt_df = meta_amt_df[valid_amt_mask]

        results_df[meta_amt_df['ratio_col'].values] = (
            results_df[meta_amt_df['base_col'].values].values /
            results_df[meta_amt_df['denom_col'].values].replace(0, np.nan).values
        ) * 100




    balance_agg = df_og.groupby(['multibureau__c', 'product'])['current_balance__c'].agg(['sum',
                                                                                          'min',
                                                                                          'max',
                                                                                          'mean',
                                                                                          'median',
                                                                                          ('std', lambda x: x.std(ddof=0))
                                                                                          ]).reset_index()

    balance_agg['mean_upon_max'] = balance_agg.apply(
        lambda row: row['mean'] / row['max'] if row['max'] != 0 else np.nan, axis=1
    )

    balance_pivot = balance_agg.pivot(index='multibureau__c', columns='product')
    balance_pivot.columns = [f'live_{p}_curr_bal_{stat}' for stat, p in balance_pivot.columns]
    balance_pivot = balance_pivot.reset_index()


    stats = ['sum', 'min', 'max', 'mean', 'median', 'std', 'mean_upon_max']

    expected_curr_bal_cols = [f'live_{p}_curr_bal_{stat}'
                         for p in product_list
                         for stat in stats]

    for col in expected_curr_bal_cols:
        if col not in balance_pivot.columns:
            balance_pivot[col] = np.nan

    balance_pivot = balance_pivot[['multibureau__c'] + expected_curr_bal_cols]
    results_df = results_df.merge(balance_pivot, on='multibureau__c', how='left')


# Bin Loan amounts into defined brackets and count frequency
    cuts_dict = loan_amt_cuts_dict()

    bin_meta = pd.DataFrame([
        {
            'product': product,
            'bin_edges': [-np.inf] + cuts + [np.inf],
            'bin_labels': (
                [f'le_{cuts[0]}'] +
                [f'{cuts[i]}_to_{cuts[i+1]}' for i in range(len(cuts) - 1)] +
                [f'gt_{cuts[-1]}']
            )
        }
        for product, cuts in cuts_dict.items()
    ])

    cut_df = df_og[df_og['high_credit_or_sanctioned_amount__c'].notnull()]
    cut_df = cut_df[cut_df['product'].isin(loan_amt_cuts_product_list)]

# Handle missing columns if no tradelines exist at all for all ids
    if cut_df.empty:
        id_list = df2['multibureau__c'].dropna().unique()
        mp_df = pd.DataFrame(product(loan_amt_cuts_product_list), columns=['product'])
        mp_df = mp_df.merge(bin_meta, on='product', how='left')
        mp_df = mp_df.explode('bin_labels', ignore_index=True)
        mp_df['feature'] = ('live_' +
                             mp_df['product'] + '_loan_amt_' +
                             mp_df['bin_labels']
                           )
        feature_meta = mp_df['feature'].tolist()
        all_combinations = pd.MultiIndex.from_product([id_list, feature_meta], names=['multibureau__c', 'feature']).to_frame(index=False)
        final_df = all_combinations.assign(value=0).pivot(index='multibureau__c', columns='feature', values='value').fillna(0).reset_index()
        results_df = results_df.merge(final_df, on='multibureau__c', how='left')
        return results_df

    cut_df = cut_df.merge(bin_meta, on='product', how='left')

    cut_df['bin'] = [
        pd.cut([amt], bins=edges, labels=labels, include_lowest=True)[0]
        for amt, edges, labels in zip(
            cut_df['high_credit_or_sanctioned_amount__c'],
            cut_df['bin_edges'],
            cut_df['bin_labels']
        )
    ]

    cut_df['feature'] = (
        'live_' +
        cut_df['product'] + '_loan_amt_' +
        cut_df['bin'].astype(str)
    )


    id_list = df2['multibureau__c'].dropna().unique()

# Generate all feature column names for binned amounts
    mp_df = pd.DataFrame(product(loan_amt_cuts_product_list), columns=['product'])
    mp_df = mp_df.merge(bin_meta, on='product', how='left')

# Create one row per bin label for each month-product pair
    mp_df = mp_df.explode('bin_labels', ignore_index=True)
    mp_df['feature'] = ('live_' +
                         mp_df['product'] + '_loan_amt_' +
                         mp_df['bin_labels']
                       )

    feature_meta = mp_df['feature'].tolist()

# Ensure all ID-feature combinations exist
    all_combinations = pd.MultiIndex.from_product([id_list, feature_meta], names=['multibureau__c', 'feature']).to_frame(index=False)

    agg_df = (cut_df.groupby(['multibureau__c', 'feature'])
                    .size()
                    .reset_index(name='value')
             )

    final_df = (all_combinations.merge(agg_df, on=['multibureau__c', 'feature'], how='left')
                                # .fillna(0)
                                .pivot(index='multibureau__c', columns='feature', values='value')
                                # .fillna(0)
                                .reset_index()
               )


    results_df = results_df.merge(final_df, on='multibureau__c', how='left')


    if cal_perc_curr_bal:
        curr_bal_sum_cols = results_df.filter(regex=r'^live_(?!overall).*_curr_bal_sum$')
        meta_cb_df = curr_bal_sum_cols.columns.to_series().str.extract(r'live_(.+)_curr_bal_sum')
        meta_cb_df.columns = ['product']
        meta_cb_df['base_col'] = curr_bal_sum_cols.columns.values
        meta_cb_df['denom_col'] = 'live_overall_curr_bal_sum'
        meta_cb_df['ratio_col'] = 'perc_live_' + meta_cb_df['product'] + '_curr_bal'

        valid_cb_mask = meta_cb_df['denom_col'].isin(results_df.columns)
        meta_cb_df = meta_cb_df[valid_cb_mask]

        results_df[meta_cb_df['ratio_col'].values] = (
            results_df[meta_cb_df['base_col'].values].values /
            results_df[meta_cb_df['denom_col'].values].replace(0, np.nan).values
        )*100



    if cal_cb_to_sanc_ratio:
        cb_cols = results_df.filter(regex=r'^live_.*_curr_bal_sum$').columns
        sanc_cols = results_df.filter(regex=r'^live_.*_loan_amt_sum$').columns

        cb_products = cb_cols.str.extract(r'^live_(.*)_curr_bal_sum$')[0]
        sanc_products = sanc_cols.str.extract(r'^live_(.*)_loan_amt_sum$')[0]


        common_products = set(cb_products).intersection(set(sanc_products))
        for p in common_products:
            cb_col = f'live_{p}_curr_bal_sum'
            sanc_col = f'live_{p}_loan_amt_sum'
            ratio_col = f'perc_live_{p}_curr_bal_to_sanc_amt'
            if cb_col in results_df.columns and sanc_col in results_df.columns:
                results_df[ratio_col] = results_df[cb_col] / results_df[sanc_col].replace(0, np.nan) * 100





    loan_df = df_og[
        (df_og['current_balance__c'].notnull()) &
        (df_og['high_credit_or_sanctioned_amount__c'].notnull()) &
        (df_og['high_credit_or_sanctioned_amount__c'] > 0)
    ].copy()

    loan_df['cb_to_sanc_ratio'] = (
        loan_df['current_balance__c'] / loan_df['high_credit_or_sanctioned_amount__c']
    )
    loan_df['ratio_cb_to_sanc_gt_0_9'] = loan_df['cb_to_sanc_ratio'] > 0.9



    productwise_counts = (
        loan_df[loan_df['ratio_cb_to_sanc_gt_0_9']]
        .groupby(['multibureau__c', 'product'])
        .size()
        .unstack(fill_value=0)
        .add_prefix('num_live_')
        .add_suffix('_loans_ratio_cb_to_sanc_gt_0.9')
        .reset_index()
    )

    results_df = results_df.merge(productwise_counts, on='multibureau__c', how='left')

    for col in productwise_counts.columns:
        if col != 'multibureau__c':
            results_df[col] = results_df[col].fillna(0).astype(int)


    return results_df


# ## Ratio of Curr Balance w.r.t Total Sanc Amt

# In[23]:


## input dataframe should have features of overall_tradelines, live_loans_feats

def ratio_curr_bal_by_total_sanc_amt(df2, product_list):

    df = df2.copy()

    results_df = pd.DataFrame()
    results_df['multibureau__c'] = list(set(df.multibureau__c))

    curr_bal = [f'live_{p}_curr_bal_sum' for p in product_list]
    total_sanc_amt = [f'{p}_loan_amt_sum' for p in product_list]

    assert len(curr_bal) == len(total_sanc_amt), \
    f"Mismatch: {len(curr_balclsd_acc_cols)} vs {len(total_sanc_amt)}"

    acc_ratio_num = df[curr_bal]
    acc_ratio_den = df[total_sanc_amt]

    acc_ratio = (acc_ratio_num.values / acc_ratio_den.values) * 100
    ratio_cols = [f'perc_{p}_curr_bal_by_total_{p}_loan_amount' for p in product_list]

    acc_ratios_df = pd.DataFrame(acc_ratio, columns=ratio_cols)
    acc_ratios_df["multibureau__c"] = df["multibureau__c"]

    results_df = results_df.merge(acc_ratios_df, on='multibureau__c', how='left')

    return results_df


# ## Percentage of Last n months with total tradelines

# In[24]:


## input dataframe should have features of overall_tradelines, last_n_months_opened_tradeines


def perc_last_n_months_opened_by_total(df2, product_list, months_list):

    df = df2.copy()

# --- Account Count Ratio ---
    last_n_months_opened_cols = [f'last_{m}m_opnd_{p}_acc_count' for m in months_list for p in product_list]
    total_tardeline_cols = [f'{col.split("_")[-3]}_acc_count' for col in last_n_months_opened_cols]

    assert len(last_n_months_opened_cols) == len(total_tardeline_cols), \
        f"Mismatch: {len(last_n_months_opened_cols)} vs {len(total_tardeline_cols)}"

    acc_ratio_num = df[last_n_months_opened_cols].replace(0, np.nan)
    acc_ratio_den = df[total_tardeline_cols].replace(0, np.nan)

    acc_count_ratio = (acc_ratio_num.values / acc_ratio_den.values) * 100
    ratio_cols = [f'perc_last_{m}m_opnd_{p}_acc_count_by_total_{p}_account' for m in months_list for p in product_list]

    acc_ratios_df = pd.DataFrame(acc_count_ratio, columns=ratio_cols)
    acc_ratios_df["multibureau__c"] = df["multibureau__c"]

# --- Loan Amount Ratio ---
    last_n_months_opened_amt_cols = [f'last_{m}m_opnd_{p}_loan_amt_sum' for m in months_list for p in product_list]
    total_tradeline_amt_cols = [f'{col.split("_")[-4]}_loan_amt_sum' for col in last_n_months_opened_amt_cols]

    assert len(last_n_months_opened_amt_cols) == len(total_tradeline_amt_cols), \
        f"Mismatch: {len(last_n_months_opened_amt_cols)} vs {len(total_tradeline_amt_cols)}"

    amt_ratio_num = df[last_n_months_opened_amt_cols].replace(0, np.nan)
    amt_ratio_den = df[total_tradeline_amt_cols].replace(0, np.nan)

    amt_ratio = (amt_ratio_num.values / amt_ratio_den.values) * 100
    ratio_amt_cols = [
        f'perc_last_{m}m_opnd_{p}_loan_amt_by_total_{p}_loan_amount' for m in months_list for p in product_list
    ]

    amt_ratios_df = pd.DataFrame(amt_ratio, columns=ratio_amt_cols)
    amt_ratios_df["multibureau__c"] = df["multibureau__c"]

# --- Merge both ratio DataFrames ---
    final_ratios_df = pd.merge(acc_ratios_df, amt_ratios_df, on="multibureau__c", how="outer")

    return final_ratios_df


# ## Percentage of Before n years with total tradelines

# In[25]:


## input dataframe should have features of overall_tradelines, before_n_years

def perc_before_n_years_opened_by_total(df2, product_list, years_list):

    df = df2.copy()

# --- Account Count Ratio ---
    before_n_years_opened_cols = [f'before_{y}y_opnd_{p}_acc_count' for y in years_list for p in product_list]
    total_tardeline_cols = [f'{col.split("_")[-3]}_acc_count' for col in before_n_years_opened_cols]

    assert len(before_n_years_opened_cols) == len(total_tardeline_cols), \
        f"Mismatch: {len(before_n_years_opened_cols)} vs {len(total_tardeline_cols)}"

    acc_ratio_num = df[before_n_years_opened_cols].replace(0, np.nan)
    acc_ratio_den = df[total_tardeline_cols].replace(0, np.nan)

    ratio_cols = [f'perc_before_{y}y_opnd_{p}_acc_count_by_total_{p}_account' for y in years_list for p in product_list]
    acc_count_ratio = (acc_ratio_num.values / acc_ratio_den.values) * 100

    acc_ratios_df = pd.DataFrame(acc_count_ratio, columns=ratio_cols)
    acc_ratios_df["multibureau__c"] = df["multibureau__c"]

# --- Loan Amount Ratio ---
    before_n_years_opened_amt_cols = [f'before_{y}y_opnd_{p}_loan_amt_sum' for y in years_list for p in product_list]
    total_tradeline_amt_cols = [f'{col.split("_")[-4]}_loan_amt_sum' for col in before_n_years_opened_amt_cols]

    assert len(before_n_years_opened_amt_cols) == len(total_tradeline_amt_cols), \
        f"Mismatch: {len(before_n_years_opened_amt_cols)} vs {len(total_tradeline_amt_cols)}"

    amt_ratio_num = df[before_n_years_opened_amt_cols].replace(0, np.nan)
    amt_ratio_den = df[total_tradeline_amt_cols].replace(0, np.nan)

    amt_ratio = (amt_ratio_num.values / amt_ratio_den.values) * 100
    ratio_amt_cols = [
        f'perc_before_{y}y_opnd_{p}_loan_amt_by_total_{p}_loan_amount' for y in years_list for p in product_list
    ]

    amt_ratios_df = pd.DataFrame(amt_ratio, columns=ratio_amt_cols)
    amt_ratios_df["multibureau__c"] = df["multibureau__c"]

# --- Merge both ratio DataFrames ---
    final_ratios_df = pd.merge(acc_ratios_df, amt_ratios_df, on="multibureau__c", how="outer")

    return final_ratios_df


# ## Percentage of Live tradelines features w.r.t total tradelines

# In[26]:


## input dataframe should have features of overall_tradelines, live_tradelines

def perc_live_by_total(df2, product_list):

    df = df2.copy()

# --- Account Count Ratio ---
    live_acc_cols = [f'live_{p}_acc_count' for p in product_list]
    total_tardeline_cols = [f'{col.split("_")[-3]}_acc_count' for col in live_acc_cols]

    assert len(live_acc_cols) == len(live_acc_cols), \
        f"Mismatch: {len(total_tardeline_cols)} vs {len(total_tardeline_cols)}"

    acc_ratio_num = df[live_acc_cols].replace(0, np.nan)
    acc_ratio_den = df[total_tardeline_cols].replace(0, np.nan)

    ratio_cols = [f'perc_live_{p}_acc_count_by_total_{p}_account' for p in product_list]
    acc_count_ratio = (acc_ratio_num.values / acc_ratio_den.values) * 100

    acc_ratios_df = pd.DataFrame(acc_count_ratio, columns=ratio_cols)
    acc_ratios_df["multibureau__c"] = df["multibureau__c"]

# --- Loan Amount Ratio ---
    live_amt_cols = [f'live_{p}_loan_amt_sum' for p in product_list]
    total_tradeline_amt_cols = [f'{col.split("_")[-4]}_loan_amt_sum' for col in live_amt_cols]

    assert len(live_amt_cols) == len(total_tradeline_amt_cols), \
        f"Mismatch: {len(live_amt_cols)} vs {len(total_tradeline_amt_cols)}"

    amt_ratio_num = df[live_amt_cols].replace(0, np.nan)
    amt_ratio_den = df[total_tradeline_amt_cols].replace(0, np.nan)

    amt_ratio = (amt_ratio_num.values / amt_ratio_den.values) * 100
    ratio_amt_cols = [
        f'perc_live_{p}_loan_amt_by_total_{p}_loan_amount' for p in product_list
    ]

    amt_ratios_df = pd.DataFrame(amt_ratio, columns=ratio_amt_cols)
    amt_ratios_df["multibureau__c"] = df["multibureau__c"]

# --- Merge both ratio DataFrames ---
    final_ratios_df = pd.merge(acc_ratios_df, amt_ratios_df, on="multibureau__c", how="outer")

    return final_ratios_df


# ## Ratio of Live and Closed Loans

# In[27]:


## input dataframe should have features of overall_closed, live_tradelines


def ratio_live_by_closed(df2, product_list):

    df = df2.copy()

    results_df = pd.DataFrame()
    results_df['multibureau__c'] = list(set(df.multibureau__c))


# --- Account Count Ratio ---
    clsd_acc_cols = [f'{p}_clsd_acc_count' for p in product_list]
    live_acc_cols = [f'live_{p}_acc_count' for p in product_list]

    assert len(clsd_acc_cols) == len(live_acc_cols), \
    f"Mismatch: {len(clsd_acc_cols)} vs {len(live_acc_cols)}"

    acc_ratio_num = df[live_acc_cols].replace(0, np.nan)
    acc_ratio_den = df[clsd_acc_cols].replace(0, np.nan)

    acc_count_ratio = (acc_ratio_num.values / acc_ratio_den.values)
    ratio_cols = [f'ratio_live_{p}_acc_count_by_clsd_{p}_account' for p in product_list]

    acc_ratios_df = pd.DataFrame(acc_count_ratio, columns=ratio_cols)
    acc_ratios_df["multibureau__c"] = df["multibureau__c"]

    results_df = results_df.merge(acc_ratios_df, on='multibureau__c', how='left')



# --- Loan Amount Ratio ---
    live_amt_cols = [f'live_{p}_loan_amt_sum' for p in product_list]
    clsd_amt_cols = [f'{p}_clsd_loan_amt_sum' for p in product_list]

    assert len(live_amt_cols) == len(clsd_amt_cols), \
        f"Mismatch: {len(live_amt_cols)} vs {len(clsd_amt_cols)}"

    amt_ratio_num = df[live_amt_cols]
    amt_ratio_den = df[clsd_amt_cols]

    amt_ratio = (amt_ratio_num.values / amt_ratio_den.values)
    ratio_amt_cols = [
        f'ratio_live_{p}_loan_amt_by_clsd_{p}_loan_amount' for p in product_list
    ]

    amt_ratios_df = pd.DataFrame(amt_ratio, columns=ratio_amt_cols)
    amt_ratios_df["multibureau__c"] = df["multibureau__c"]

    results_df = results_df.merge(amt_ratios_df, on='multibureau__c', how='left')

    return results_df


# ## Overdue Features Function

# In[28]:


def overdue_features(df2, product_list=['pl', 'cl'], overdue_amt_cuts=[1000, 5000]):
    df = df2.copy()
    results_df = pd.DataFrame()
    results_df['multibureau__c'] = df['multibureau__c'].unique()

    if 'overall' not in product_list:
        product_list = ['overall'] + product_list

    # Create product mapping
    product_df = pd.DataFrame(
        [(k, val) for k, vals in product_group_dict()[0].items() for val in vals],
        columns=['product', 'account_type__c']
    )
    product_df = product_df[product_df['product'].isin(product_list)]

    # Create tradeline presence map (from full DF)
    df_full = df2.merge(product_df, on='account_type__c', how='left')
    df_full = df_full[df_full['product'].notnull()]
    tradeline_presence = df_full.groupby(['multibureau__c', 'product']).size().unstack(fill_value=0)

    # Only use rows with overdue > 0 for calculations
    df_overdue = df[df['overdue_amount__c'] > 0]
    cross_df = df_overdue.merge(product_df, on='account_type__c', how='left')
    cross_df = cross_df[cross_df['product'].notnull()]

    ### 1. Overdue account count ###
    amount_count = cross_df.groupby(['multibureau__c', 'product'])['overdue_amount__c'].count().unstack()
    amount_count = amount_count.reindex(columns=tradeline_presence.columns, index=tradeline_presence.index, fill_value=np.nan)

    for col in amount_count.columns:
        for idx in amount_count.index:
            if pd.isna(amount_count.loc[idx, col]) and tradeline_presence.loc[idx, col] > 0:
                amount_count.loc[idx, col] = 0

    amount_count.columns = [f'{p}_overdue_accounts' for p in amount_count.columns]
    amount_count = amount_count.reset_index()
    results_df = results_df.merge(amount_count, on='multibureau__c', how='left')

    ### 2. Sum, Max, Mean of overdue_amount ###
    amount_agg = cross_df.groupby(['multibureau__c', 'product'])['overdue_amount__c'].agg(['sum', 'max', 'mean']).unstack()
    amount_agg = amount_agg.reindex(columns=product_list, level=1)
    amount_agg = amount_agg.reindex(index=tradeline_presence.index)

    # Fill NaNs with 0 where tradeline exists
    for stat in ['sum', 'max', 'mean']:
        for product in product_list:
            col = (stat, product)
            if col in amount_agg.columns and product in tradeline_presence.columns:
                mask = amount_agg[col].isna() & (tradeline_presence[product] > 0)
                amount_agg.loc[mask, col] = 0

    amount_agg.columns = [f'{p}_overdue_amt_{stat}' for stat, p in amount_agg.columns]
    amount_agg = amount_agg.reset_index()
    results_df = results_df.merge(amount_agg, on='multibureau__c', how='left')

    ### 3. Overdue amount cutoff counts ###
    cutoff_df = df_overdue.groupby('multibureau__c')['overdue_amount__c'].agg([
        (f'overdue_count_gt_{overdue_amt_cuts[0]}', lambda x: (x > overdue_amt_cuts[0]).sum()),
        (f'overdue_count_gt_{overdue_amt_cuts[1]}', lambda x: (x > overdue_amt_cuts[1]).sum()),
    ])
    cutoff_df = cutoff_df.reindex(index=tradeline_presence.index)

    # Fill 0 if the customer has ANY tradeline
    for col in cutoff_df.columns:
        for idx in cutoff_df.index:
            if pd.isna(cutoff_df.loc[idx, col]) and tradeline_presence.loc[idx].sum() > 0:
                cutoff_df.loc[idx, col] = 0

    cutoff_df = cutoff_df.reset_index()
    results_df = results_df.merge(cutoff_df, on='multibureau__c', how='left')

    return results_df


# ## CC Utilization Features Function

# In[29]:


def cc_utilization_features(df2):

    """
    Generates credit card utilization features at the multibureau ID level.
    Includes metrics on current and historical utilization and flags for high utilization.
    """

    df = df2.copy(deep=True)

    results_df = pd.DataFrame()
    results_df['multibureau__c'] = df['multibureau__c'].unique()

# Filter to credit card accounts using fixed list and product group dict
    cc_df = df[df['account_type__c'].isin(['Credit Card' , 'Fleet Card' , 'Corporate Credit Card', 'Charge Card', 'Secured Credit Card', 'Kisan Credit Card'])].copy(deep=True)

# Derive credit limit used in utilization calculations
    cc_df['CL_updated'] = np.where(cc_df.credit_limit__c > 0, cc_df.credit_limit__c , cc_df.high_credit_or_sanctioned_amount__c)

# Calculate current utilization as a % of available credit
    cc_df['current_cc_utilization'] = cc_df['current_balance__c'] / cc_df['CL_updated'] * 100

# Calculate peak utilization ever observed
    cc_df['utlization_ever'] = cc_df['high_credit_or_sanctioned_amount__c'] / cc_df['CL_updated'] * 100

# Flag accounts where utilization exceeded 100%
    cc_df['utilization_gt_100'] = np.where(cc_df['utlization_ever']>100 , 1 , 0)


# Aggregate current utilization stats
    utlization_feats = cc_df.groupby('multibureau__c')['current_cc_utilization'].agg(
        ['min', 'max', 'mean', 'median']
    ).reset_index()

    utlization_feats.columns = ['multibureau__c'] + [f'current_cc_utilization_{stat}' for stat in ['min', 'max', 'mean', 'median']]

# Aggregate utilization > 100% flags
    utlization_gt_100_feats = cc_df.groupby('multibureau__c')['utilization_gt_100'].agg(['count', 'sum']).reset_index()

    utlization_gt_100_feats.columns = [
        'multibureau__c',
        'count',
        'num_cc_acc_utlization_gt_100_ever'
    ]

# Calculate percentage of credit card accounts with utilization > 100%
    utlization_gt_100_feats['perc_cc_acc_utilization_gt_100'] = (utlization_gt_100_feats['num_cc_acc_utlization_gt_100_ever'] /
                                                                 utlization_gt_100_feats['count']
                                                                ) * 100

    utlization_gt_100_feats = utlization_gt_100_feats.drop('count', axis=1)

# Merge all features into one dataframe
    results_df = pd.merge(results_df , utlization_feats , on='multibureau__c', how='left')
    results_df = pd.merge(results_df , utlization_gt_100_feats , on='multibureau__c', how='left')

    return results_df


# ## Ownership Features Function

# In[ ]:





# In[30]:


def ownership_features(df2):

    df = df2.copy(deep=True)


    ownership_df = df[['multibureau__c', 'ownership_indicator__c']].copy(deep=True)
    ownership_mapper = ownership_mapper_dict()
    ownership_df['ownership'] = ownership_df['ownership_indicator__c'].map(ownership_mapper)

    # Create a flag for counting
    ownership_df['count'] = 1

    # Pivot to count number of tradelines by ownership type
    ownership_pivot = ownership_df.pivot_table(
        index='multibureau__c',
        columns='ownership',
        values='count',
        aggfunc='sum',
        fill_value=0
    )

    # Calculate total tradelines per multibureau
    ownership_pivot['total_ownership_tradelines'] = ownership_pivot.sum(axis=1)

    # Create percentage columns
    perc_df = ownership_pivot.div(ownership_pivot['total_ownership_tradelines'], axis=0)
    perc_df = perc_df.drop(columns='total_ownership_tradelines')
    perc_df.columns = [f'perc_acc_{col.lower()}' for col in perc_df.columns]

    # Rename count columns
    ownership_pivot.columns = [f'num_acc_{col.lower()}' if col != 'total_ownership_tradelines' else col for col in ownership_pivot.columns]

    # Combine count and percentage columns
    result_df = pd.concat([ownership_pivot.reset_index(), perc_df.reset_index(drop=True)], axis=1)

    result_df = result_df.drop('total_ownership_tradelines', axis=1)

    return result_df


# ## DPD Features Functions

# In[31]:


def payment_history_date_wise(values):

    """
    Generates a dictionary of payment history keyed by date for each tradeline.
    Filters out missing data and aligns payment history fields with monthly dates.
    """

    missing_date = date(1980, 1, 1)


    ph_start = values.get('payment_history_start_date__c')
    ph_end = values.get('date_opened_or_disbursed__c')

    if pd.isna(ph_start) or pd.isna(ph_end):
        return {}

    ph_end = ph_end - pd.DateOffset(months=1)

# Handle missing dates (1980-01-01 is treated as placeholder for missing)
    if (ph_start == missing_date) | (ph_end == missing_date):
        return {}

# Generate monthly date range in reverse order from end to start
    try:
        ph_dates = pd.date_range(ph_end, ph_start, freq='MS', inclusive='both')
    except ValueError:
        return {}
    ph_dates = list(ph_dates)
    ph_dates.reverse()

# List of expected payment history column names (01 to 36 months back)
    cols_names = ['paymt_hst_01', 'paymt_hst_02', 'paymt_hst_03',
       'paymt_hst_04', 'paymt_hst_05', 'paymt_hst_06', 'paymt_hst_07',
       'paymt_hst_08', 'paymt_hst_09', 'paymt_hst_10', 'paymt_hst_11',
       'paymt_hst_12', 'paymt_hst_13', 'paymt_hst_14', 'paymt_hst_15',
       'paymt_hst_16', 'paymt_hst_17', 'paymt_hst_18', 'paymt_hst_19',
       'paymt_hst_20', 'paymt_hst_21', 'paymt_hst_22', 'paymt_hst_23',
       'paymt_hst_24', 'paymt_hst_25', 'paymt_hst_26', 'paymt_hst_27',
       'paymt_hst_28', 'paymt_hst_29', 'paymt_hst_30', 'paymt_hst_31',
       'paymt_hst_32', 'paymt_hst_33', 'paymt_hst_34', 'paymt_hst_35',
       'paymt_hst_36']

    pay_hist_dict = OrderedDict()

# Populate dictionary with available payment history values and corresponding suffix index
    for d, c in zip(ph_dates, cols_names[:len(ph_dates)]):
        pay_hist_dict[d] = [float(values[c]), c[-2:]]

# Remove entries where the first element is NaN
    pay_hist_dict = OrderedDict((k, v) for k, v in pay_hist_dict.items() if v[0] is not None and not pd.isna(v[0]))

    return pay_hist_dict


# In[32]:


def calculate_tradeline_flags(row, months_list=[3, 6, 9, 12], dpd_list=[1, 30, 60, 90], required_list=['count', 'max']):

#     display("calculate_tradeline_flags",months_list )
    """
    Generates tradline level DPD flags
    """

    today = pd.to_datetime(row['createddate'], errors='coerce').tz_localize(None)  # Use the value from the DataFrame row
    history = row['payment_history_dictionary']
    parsed_history = {
        pd.to_datetime(k, errors='coerce'): v for k, v in history.items()
    }

    flags = {}

# For "ever", just take all valid records
    all_valid = [v for v in parsed_history.values() if isinstance(v, list) and len(v) > 0]
    all_dpd = [v[0] for v in all_valid]

    months_to_days = months_to_days_dict(months_list)
#     display("months_to_days", months_to_days)
    for m in months_list:
        days_back = months_to_days[str(m)]
#         display("days_back",days_back)
        date_cutoff = today - pd.to_timedelta(days_back, unit='D')
        filtered = {
            k: v for k, v in parsed_history.items()
            if date_cutoff <= k <= today and isinstance(v, list) and len(v) > 0
        }
        filtered_dpd = [v[0] for v in filtered.values()]

        total_key = f'{m}_mob_total_payment_count'
        flags[total_key] = len(filtered_dpd)

        for d in dpd_list:
            dpd_key = f'{m}_mob_{d}+_dpd_count'
            flags[dpd_key] = sum(1 for val in filtered_dpd if val > d)

        if 'max' in required_list:
            flags[f'{m}_mob_max_dpd'] = max(filtered_dpd) if filtered_dpd else 0

    total_key = 'ever_total_payment_count'
    flags[total_key] = len(all_dpd)

# Ever logic (whole history)
    for d in dpd_list:
        dpd_key = f'ever_{d}+_dpd_count'
        flags[dpd_key] = sum(1 for val in all_dpd if val > d)

    if 'max' in required_list:
        flags['ever_max_dpd'] = max(all_dpd) if all_dpd else 0

    return pd.Series(flags)


# In[33]:


def months_since_last_x_dpd(payment_history, created_date, dpd_list):
    try:
        parsed = {
            pd.to_datetime(k): v for k, v in payment_history.items()
        }
    except Exception:
        return {f'months_since_last_{dpd}+_dpd': None for dpd in dpd_list}

    # Sort months descending (most recent first)
    sorted_dates = sorted(parsed.keys(), reverse=True)

    # Initialize result dict with None
    result = {f'months_since_last_{dpd}+_dpd': None for dpd in dpd_list}

    for dpd_threshold in dpd_list:
        for dt in sorted_dates:
            dpd_value = parsed[dt][0]
            if dpd_value >= dpd_threshold:
                months_diff = (created_date.year - dt.year) * 12 + (created_date.month - dt.month)
                result[f'months_since_last_{dpd_threshold}+_dpd'] = months_diff
                break  # stop once the most recent is found

    return result




# In[34]:


def aggregate_dpd_features(df, dpd_feature_list, customer_id_col='multibureau__c'):
    """
    Aggregates DPD features for each customer:
    - Sum for normal DPD count columns
    - Max for max_dpd columns
    - Min/Mean for months_since_last_*_dpd columns
    """

    sum_cols = [col for col in dpd_feature_list
                if 'max_dpd' not in col and not col.startswith('months_since_last_')]

    max_cols = [col for col in dpd_feature_list if 'max_dpd' in col]

    time_cols = [col for col in dpd_feature_list if col.startswith('months_since_last_')]


#     # Build aggregation dictionary
#     agg_dict = {col: 'sum' for col in sum_cols}
#     agg_dict.update({col: 'max' for col in max_cols})
#     agg_dict.update({col: 'min' for col in min_cols})
#     agg_dict.update({col: 'mean' for col in mean_cols})


    # Build aggregation list for groupby().agg()
    agg_dict = {}

    for col in sum_cols:
        agg_dict[f'{col}'] = (col, 'sum')

    for col in max_cols:
        agg_dict[f'{col}'] = (col, 'max')

    for col in time_cols:
        agg_dict[f'{col}_min'] = (col, 'min')
        agg_dict[f'{col}_mean'] = (col, 'mean')


    # Perform aggregation
#     df_agg = df.groupby(customer_id_col).agg(agg_dict).reset_index()
    df_agg = df.groupby(customer_id_col).agg(**agg_dict).reset_index()


    return df_agg


# In[35]:


# def aggregate_dpd_features(df,dpd_feature_list,customer_id_col='multibureau__c'):
#     """
#     Aggregates DPD features for each customer.
#     - Sum for normal DPD count columns
#     - Max for max_dpd columns
#     """
# # Identify which columns to sum and which to max
#     sum_cols = [col for col in dpd_feature_list if 'max_dpd' not in col]
#     max_cols = [col for col in dpd_feature_list if 'max_dpd' in col]

# # Create aggregation dictionary
#     agg_dict = {col: 'sum' for col in sum_cols}
#     agg_dict.update({col: 'max' for col in max_cols})

# # Perform aggregation
#     df_agg = df.groupby(customer_id_col).agg(agg_dict).reset_index()

#     return df_agg


# In[36]:


def generate_all_product_group_dpd_aggregates(df, dpd_feature_list, product_list=['overall', 'unsecured'], customer_id_col='multibureau__c'):
    """
    For each product group, filter data based on account_type__c and apply DPD aggregation.
    Returns a dictionary with keys as product group names and values as aggregated DataFrames.
    """
    product_dict, _ = product_group_dict()
    result_dict = {}


    for group_name in product_list:
# Filter the DataFrame for relevant account_type__c
        df_group = df[df['account_type__c'].isin(product_dict[group_name])]

        if df_group.empty:
            continue  # skip if no rows match

# Aggregate
        agg_df = aggregate_dpd_features(df_group, dpd_feature_list, customer_id_col)

# Optionally rename columns with group prefix (except ID column)
        agg_df = agg_df.rename(columns={
            col: f"{group_name}_{col}" for col in agg_df.columns if col != customer_id_col
        })

        result_dict[group_name] = agg_df

    return result_dict


# In[37]:


def create_dpd_ratios(df1, product_list=['overall', 'unsecured'], dpd_list=[1, 30, 60, 90], time_buckets = ['3_mob', '6_mob', '9_mob', '12_mob', 'ever']):

    """
    Creates DPD (Days Past Due) ratio features from count-based features.
    """

    df = df1.copy(deep=True)

# Loop through each product type
    for prod in product_list:
# Loop through each time bucket (e.g., last 3, 6, 9, 12 months, or ever)
        for bucket in time_buckets:
# Construct the column name for the total tradeline count
            total_col = f"{prod}_{bucket}_total_payment_count"
# Loop through each DPD threshold
            for d in dpd_list:
# Construct the column name for the DPD count at this threshold
                dpd_col = f"{prod}_{bucket}_{d}+_dpd_count"
# Construct the new column name for the DPD ratio
                ratio_col = f"{prod}_{bucket}_{d}+_dpd_ratio"
# Check if both necessary columns are present in the DataFrame
                if total_col in df.columns and dpd_col in df.columns:
# Calculate the ratio of DPD count to total count
                    df[ratio_col] = df[dpd_col] / df[total_col].replace(0, np.nan)

    return df


# In[38]:


def dpd_main(df, months_list=[3, 6, 9, 12], dpd_list=[1, 30, 60, 90], required_list=['count', 'max'],
             product_list=['overall','unsecured','secured','pl','cc','od','gl','hl','lap','al','twl','cl','el','bl'],
             time_buckets = ['3_mob', '6_mob', '9_mob', '12_mob', 'ever']):

    """
    Generates DPD (delinquency) features for each multibureau ID
    """


    for col in ['createddate', 'date_opened_or_disbursed__c', 'date_closed__c']:
        df[col] = pd.to_datetime(pd.to_datetime(df[col]).dt.date)


    df2 = df.copy(deep=True)
    df2['combined_payment_history'] = (df2['payment_history_1__c'].fillna('') + df2['payment_history_2__c'].fillna(''))
    df2['combined_payment_history'] = df2['combined_payment_history'].apply(lambda x: x + 'N' * (108 - len(x)) if len(x) < 108 else x[:108])

    for i in range(36):
        col_name = f'paymt_hst_{i+1:02d}'
        start = i * 3
        end = start + 3
        df2[col_name] = df2['combined_payment_history'].str[start:end]

    df2.replace('NNN',np.nan,inplace = True)

# replace string in  payment values with int
    payment_month_list = [x for x in df2.columns if 'hst' in x.lower()]

    dpd_code_replace_dict = {'901':'000', '902':'SUB', '903': "DBT",'904':'LSS', '905':'SMA'}
    replace_dict = {"XXX": 0,"SUB": 90, "DBT": 90,"SMA": 90,"STD": 0,"LSS": 90,"   ": np.nan,'""': np.nan,'\\"\\"': np.nan}

    df2.loc[:, payment_month_list] = df2[payment_month_list].astype(str).replace(dpd_code_replace_dict).replace(replace_dict).astype(float)


    df2['payment_history_dictionary'] = df2.apply(payment_history_date_wise, axis = 1)
    df2.reset_index(drop = True,inplace = True)

    df_dpd=df2.apply(lambda row: calculate_tradeline_flags(row,
                                                           months_list,
                                                           dpd_list,
                                                           required_list
                                                          ),axis=1)
    dpd_feature_list = df_dpd.columns.tolist()
    filtered_df_1 = df2.join(df_dpd)

    # Apply row-wise only where 3_mob_30+_dpd_count > 0
    result_df = filtered_df_1.apply(
        lambda row: months_since_last_x_dpd(row['payment_history_dictionary'], row['createddate'],dpd_list),
        axis=1, result_type='expand'
    )

    # Concatenate the new features back into df2
    filtered_df_1 = pd.concat([filtered_df_1, result_df], axis=1)


    additional_dpd_feature_list = result_df.columns.tolist()
    dpd_feature_list += additional_dpd_feature_list

# result_dict will hold one DataFrame per product group
    result_dict = generate_all_product_group_dpd_aggregates(filtered_df_1,
                                                            dpd_feature_list,
                                                            product_list
                                                           )


# Create base DataFrame
    final_df = filtered_df_1[['multibureau__c']].drop_duplicates()



# Merge only available product group aggregates
    for group in result_dict:
        final_df = final_df.merge(result_dict[group], on='multibureau__c', how='left')

# Handle missing columns
    expected_total_cnt_cols = [f'{p}_{t}_total_payment_count'
                                 for p in product_list
                                 for t in time_buckets
                        ]


    expected_dpd_cnt_cols = [f'{p}_{t}_{d}+_dpd_count'
                                 for p in product_list
                                 for t in time_buckets
                                 for d in dpd_list
                        ]


    expected_max_dpd_cols = [f'{p}_{t}_max_dpd'
                                for p in product_list
                                for t in time_buckets
                        ]

    expected_months_since_mean_dpd_cols = [f'{p}_months_since_last_{d}+_dpd_mean'
                                for p in product_list
                                for d in dpd_list
                        ]

    expected_months_since_min_dpd_cols = [f'{p}_months_since_last_{d}+_dpd_min'
                                for p in product_list
                                for d in dpd_list
                        ]
    expected_cols = expected_total_cnt_cols + expected_dpd_cnt_cols + expected_max_dpd_cols + expected_months_since_mean_dpd_cols + expected_months_since_min_dpd_cols

# Handle missing values
    for col in expected_cols:
        if col not in final_df:
            final_df[col] = np.nan

    final_df[expected_total_cnt_cols] = final_df[expected_total_cnt_cols].fillna(0)


    final_dpd_feats = create_dpd_ratios(final_df,
                                        product_list,
                                        dpd_list,
                                        time_buckets
                                       )

    return final_dpd_feats, filtered_df_1#,result_dict,dpd_feature_list#,df2 # final_dpd_feats contains the dpd feats and df2 is just for validation purpose



# ## Written Off Features Function

# In[39]:


def create_writtenoff_feat(acc_df):
# Replace NaNs in amount columns with 0 for summation
    df = acc_df.copy(deep = True)

    amount_cols = ['settlement_amount__c', 'writtenoff_amount_total', 'writtenoff_amount_principal']
    #df['settlement_amount_flag'] = np.where(df['settlement_amount__c'].isnull(),0,1)
    df['writtenoff_amount_total_flag'] = np.where(df['writtenoff_amount_total'].isnull(),0,1)
    #df['writtenoff_amount_principal_flag'] = np.where(df['writtenoff_amount_principal'].isnull(),0,1)

# Group by multibureau__c and compute count and sum
    agg_df = df.groupby('multibureau__c').agg(
        #sum_settlement_amount=('settlement_amount__c', 'sum'),
        sum_writtenoff_total=('writtenoff_amount_total', 'sum'),
        #sum_writtenoff_principal=('writtenoff_amount_principal', 'sum'),
        #count_settlement_amount=('settlement_amount_flag', 'sum'),
        count_writtenoff_total=('writtenoff_amount_total_flag', 'sum'),
        #count_writtenoff_principal=('writtenoff_amount_principal_flag', 'sum')
    ).reset_index()

    return agg_df


# ## Enuiry List and Account List Velocity Features Function

# In[40]:


def account_to_enquiry_ratio_features(enq_feats_last_n_months, opnd_acc_feats_last_n_months, months_list, product_list):

    """
    Generate ratio features for account list upon enquiry list for each multibureau ID
    """

    enq_df = enq_feats_last_n_months.copy(deep=True)
    acc_df = opnd_acc_feats_last_n_months.copy(deep=True)

# Merge enq_df and acc_df on multibureau__c
    merged_df = pd.merge(enq_df, acc_df, on='multibureau__c', how='inner')

# Generate column names
    count_enq_cols = [f'last_{m}m_{p}_enq_count' for m in months_list for p in product_list]
    count_acc_cols = [f'last_{m}m_opnd_{p}_acc_count' for m in months_list for p in product_list]

    amt_enq_cols = [f'last_{m}m_{p}_enq_amt_sum' for m in months_list for p in product_list]
    amt_acc_cols = [f'last_{m}m_opnd_{p}_loan_amt_sum' for m in months_list for p in product_list]

# Count ratios
    enq_counts = merged_df[count_enq_cols].replace(0, np.nan)
    acc_counts = merged_df[count_acc_cols]
    count_ratios = acc_counts.values / enq_counts.values

    count_ratio_cols = [col.replace("_enq_count", "_opnd_acc_cnt_to_enq_cnt_ratio")
                        for col in count_enq_cols]

    count_ratios_df = pd.DataFrame(count_ratios, columns=count_ratio_cols)
    count_ratios_df["multibureau__c"] = merged_df["multibureau__c"]

# Amount ratios
    enq_amts = merged_df[amt_enq_cols].replace(0, np.nan)
    acc_amts = merged_df[amt_acc_cols]
    amt_ratios = acc_amts.values / enq_amts.values

    amt_ratio_cols = [col.replace("_enq_amt_sum", "_opnd_acc_amt_to_enq_amt_ratio")
                      for col in amt_enq_cols]

    amt_ratios_df = pd.DataFrame(amt_ratios, columns=amt_ratio_cols)
    amt_ratios_df["multibureau__c"] = merged_df["multibureau__c"]

# Merge ratio features back to enq_df (or acc_df — your choice)
    final_df = enq_df[["multibureau__c"]].merge(count_ratios_df, on="multibureau__c", how="left") \
                     .merge(amt_ratios_df, on="multibureau__c", how="left")

    return final_df


# ## ID List Features Function

# In[41]:


def get_id_list_features(idlist_df):

    """
    Generate features for ID list for each multibureau ID
    """

    idlist = idlist_df.copy(deep=True)

# Replace specific ID type strings with numeric values for consistency
    idlist['id_type__c'] = idlist['id_type__c'].replace({"EMAIL":51,
                                                         "CkycId":np.nan,
                                                         "SocialId":np.nan,
                                                         "TaxId":np.nan}
                                                       )

# Convert id_type__c to float type for processing
    idlist['id_type__c'] = idlist['id_type__c'].astype(float)

# Calculate number of days left until ID expiry from the 'createddate'
    idlist['duration_left'] = (pd.to_datetime(idlist['expiry_date__c'], errors = 'coerce') -
                               pd.to_datetime(idlist['createddate'], errors = 'coerce')
                              ).dt.days

# Calculate total validity period in days from issue date to expiry date
    idlist['duration_issue'] = (pd.to_datetime(idlist['expiry_date__c'], errors = 'coerce') -
                                pd.to_datetime(idlist['issue_date__c'], errors = 'coerce')
                               ).dt.days

# Flag if the ID is expired as of the createddate
    idlist['expired_flag'] = (pd.to_datetime(idlist['expiry_date__c'], errors = 'coerce') <
                              pd.to_datetime(idlist['createddate'], errors = 'coerce')
                             )

# Flag if the ID is still valid (i.e., live) as of the createddate
    idlist['live_flag'] = (pd.to_datetime(idlist['expiry_date__c'], errors = 'coerce') >
                           pd.to_datetime(idlist['createddate'], errors = 'coerce')
                          )

    idlist['ID'] = idlist['id_type__c']

# Rename feature columns for clarity
    idlist.rename(columns={"expired_flag":"ID_expired",
                      "live_flag":"ID_live",
                      "duration_left":"ID_days_to_expiration",
                      "duration_issue":'ID_validity_period_in_days'},inplace=True)

# Group by 'multibureau__c' and compute aggregate features
    idlist_df = idlist.groupby('multibureau__c').agg({"ID":[("_count",'count')
         ,("_unique_count",pd.Series.nunique )
         ,("_pan_flag",lambda x: np.where(x==1,1,0).sum())
         ,("_passport_flag",lambda x: np.where(x==2,1,0).sum())
         ,("_driving_licencse_flag",lambda x: np.where(x==3,1,0).sum())
         ,("_voterid_flag",lambda x: np.where(x==4,1,0).sum())
         ,("_rationcard_flag",lambda x: np.where(x==5,1,0).sum())
         ,("_uid_flag",lambda x: np.where(x==6,1,0).sum())
         ,("_otherID_7_flag",lambda x: np.where(x==7,1,0).sum())
         ,("_otherID_8_flag",lambda x: np.where(x==8,1,0).sum())]
         ,"ID_expired":[('_Count',sum)]
         ,"ID_live":[('_Count',sum)]
         ,"ID_days_to_expiration": [("max",max)
                           ,("min",min)]
         ,"ID_validity_period_in_days": [("std",np.std)
                           ,("mean",np.mean)
                            , ("min",min)]})  \
                                .reset_index()

# Flatten the multi-index column names
    idlist_df.columns = [''.join(col).strip() if isinstance(col, tuple) else col for col in idlist_df.columns]

    return idlist_df


# ## Address List Features Function

# In[42]:


def get_addresslist_features(addlist_df):

    """
    Generate features for address list for each multibureau ID
    """

    addlist = addlist_df.copy(deep=True)

# Remove rows where date_reported__c is null (as they're not useful for length of stay)
    addlist = addlist[addlist['date_reported__c'].notnull()]

# Sort addresses by reported date in descending order (most recent first)
    addlist = addlist.sort_values(by='date_reported__c',ascending = False)
    addlist = addlist.reset_index(drop=True)

# Convert createddate and date_reported__c columns to datetime format
    addlist['createddate'] = pd.to_datetime(addlist['createddate'], format='%Y-%m-%d')
    addlist['createddate'] = addlist['createddate'].apply(lambda x : x.strftime('%Y-%m-%d'))
    addlist['createddate'] = addlist['createddate'].apply(lambda x : datetime.strptime(x, '%Y-%m-%d'))
    addlist['date_reported__c'] = pd.to_datetime(addlist['date_reported__c'], format='%Y-%m-%d')

# Calculate length of stay in days from date reported to createddate
    addlist['length_of_stay_in_days'] = (addlist['createddate'] - addlist['date_reported__c']).dt.days

# Aggregated address-level features grouped by unique customer/application ID (multibureau__c)
    addr_list_df = addlist.groupby('multibureau__c').agg({
        "address_category__c":[("number_of_permanent_address",lambda x: np.where(x==1,1,0).sum()),
                               ("number_of_residence_address",lambda x: np.where(x==2,1,0).sum()),
                               ("number_of_office_address",lambda x: np.where(x==3,1,0).sum()),
                               ("number_of_not_categorized_address",lambda x: np.where(x==4,1,0).sum())
                              ],
        "residence_code__c":[("number_of_own_address",lambda x: np.where(x==1,1,0).sum()),
                             ("number_of_rent_address",lambda x: np.where(x==2,1,0).sum())
                            ],
        "length_of_stay_in_days":[("max_length_of_stay_in_days",max),
                                  ("min_length_of_stay_in_days",min),
                                  ("mean_length_of_stay_in_days",np.mean)
                                 ],
        "pin_code__c": [('unique_pincode', lambda x: x.nunique())],
        "state_code__c":[("unique_state_codes", lambda x: x.nunique())],
        "bureau_address__c":[("count_of_address",lambda x: x.count())]
    }).reset_index()

# Flatten multi-level columns for readability
    addr_list_df.columns = ['multibureau__c'] + [col[1] for col in addr_list_df.columns[1:]]

# Extract latest address per multibureau__c based on most recent date_reported__c
    latest_addlist_features = addlist.fillna("nan").sort_values(by=['multibureau__c','date_reported__c'],
                                                               ascending=[False,False]
                                                              ).groupby('multibureau__c').first().replace("nan",np.nan)[['bureau_address__c',
                                                                                                                         'state_code__c',
                                                                                                                         'pin_code__c',
                                                                                                                         'address_category__c',
                                                                                                                         'length_of_stay_in_days',
                                                                                                                         'date_reported__c']].reset_index()

# Rename latest address columns to clearly indicate they're the most recent
    latest_address_rename_columns ={"bureau_address__c":"latest_address",
                                "state_code__c":"latest_address_statecode",
                                "pin_code__c":"latest_address_pincode",
                                "address_category__c":"latest_address_category",
                                "length_of_stay_in_days":"latest_address_length_of_stay_in_days",
                                "date_reported__c":"latest_address_date_reported"}
    latest_addlist_features = latest_addlist_features.rename(columns=latest_address_rename_columns)


# Merge aggregated features with latest address features on multibureau__c
    address_features = pd.merge(latest_addlist_features,addr_list_df,on='multibureau__c',how='outer')

    return address_features


# ## Phone List Features Function

# In[43]:


def get_phonelist_features(phonelist_df):

    """
    Generate features for phone list for each multibureau ID
    """
    phonelist = phonelist_df.copy(deep=True)

# Load a dictionary that maps raw phone type codes to readable categories (e.g., 'Mobile', 'Office', etc.)
    phone_type_map = phone_type_map_dict()

# Replace the phone type codes in the dataframe using the mapping
    phonelist['phone_type__c'] = phonelist['phone_type__c'].replace(phone_type_map)

# Group the data by unique entity (e.g., customer ID) and calculate aggregated phone-related features
    phonelist_features = phonelist.groupby("multibureau__c").agg(number_of_unique_phone_numbers = ("telephone_number__c",'nunique'),
                                                                unique_phone_types = ("phone_type__c","nunique"),
                                                                phone_type_mobile = ("phone_type__c",lambda x: np.where(x == 'Mobile',1,0).max()),
                                                                phone_type_office = ("phone_type__c",lambda x: np.where(x == 'Office',1,0).max()),
                                                                phone_type_not_classified = ("phone_type__c",lambda x: np.where(x == 'Not Classified',1,0).max()),
                                                                phone_type_home = ("phone_type__c",lambda x: np.where(x == 'Home',1,0).max())
                                                                ).reset_index()

    return phonelist_features


# ## Employment List Features Function

# In[44]:


def get_employmentlist_features(emp_df):

    """
    Generate features for employment list for each multibureau ID
    """

    df = emp_df.copy(deep=True)
    df['occupationCode'] = df['occupationCode'].astype(float)
    df['income'] = df['income'].astype(float)
    df['createddate'] = pd.to_datetime(df['createddate'])
    df['dateReported'] = pd.to_datetime(df['dateReported'])

# Occupation flags
# 1 - Salaried
# 2 - Self Employed Professional
# 3 - Self Employed
# 4 - Others
    df['is_salaried'] = (df['occupationCode'] == 1).astype(int)
    df['is_self_employed'] = df['occupationCode'].isin([2, 3]).astype(int)
    df['is_others'] = (df['occupationCode'] == 4).astype(int)

# Compute year difference
    df['employment_years'] = ((df['createddate'] - df['dateReported']).dt.days / 365).round(1)

# Tenure flags
    df['reported_within_1_year'] = (df['employment_years'] <= 1).astype(int)
    df['reported_between_1_and_2_years'] = np.where((1<df['employment_years']) & (df['employment_years'] <= 2),1,0)
    df['reported_more_than_2_years'] = (df['employment_years'] > 2).astype(int)

# Monthly income mapping
# Assuming 'M' = monthly, 'A' = annual
    df['monthly_income'] = df.apply(lambda x: x['income'] / 12 if x['monthlyIncomeIndicator'] == 'A'
                                    else x['income'] if x['monthlyIncomeIndicator'] == 'M'
                                    else np.nan, axis=1)

    feature_cols = ['multibureau__c','is_salaried','is_self_employed','is_others','employment_years','monthly_income','reported_within_1_year','reported_between_1_and_2_years','reported_more_than_2_years']

    df.sort_values(by=['multibureau__c','createddate','dateReported'],ascending=[False,False,False],inplace=True)

# Considering the latest reported employment details
    df_dedup = df.drop_duplicates(subset=['multibureau__c'],keep='first').reset_index(drop=True)

    return df_dedup[feature_cols]




# # Enquiry List Data Pull and Feature Creation

# ## Data read



# In[48]:


# NOTE: splitting Inquiry.csv into split_ids/part_<N>.csv now happens ONCE,
# up front, in split_ids.py (so parallel part-workers don't each redo it).
# This script only reads its assigned part_<PART>.csv below.


# In[51]:


df_part_1 = pd.read_csv(os.path.join(SPLIT_IDS_DIR, f'part_{PART}.csv'))
print(f"df_part_1 (part_{PART}) shape:", df_part_1.shape)


# In[53]:


acc = pd.read_csv(os.path.join(SCRUB_DIR, 'Account.csv'), sep="|")
enq = pd.read_csv(os.path.join(SCRUB_DIR, 'IOI.csv'), sep="|")



# In[54]:


acc = acc[acc['LOS-APP-ID'].isin(df_part_1['LOS-APP-ID'].tolist())]
enq = enq[enq['LOS-APP-ID'].isin(df_part_1['LOS-APP-ID'].tolist())]


# In[55]:


print("enq agg result -> ",enq.agg({"count","nunique"}))


# In[56]:


print("acc agg result -> ",acc.agg({"count","nunique"}))


# In[57]:


# pii = pd.read_csv("./scrub_received/pii_files_with_all_columns.csv",usecols=["lead_id","retro date","LOS_APP_ID/UNIQUE_ID","dob"])
# pii = pii.rename(columns={"LOS_APP_ID/UNIQUE_ID":"LOS-APP-ID",'dob' : 'dateofbirth', "retro date":'Retro Date'})
# acc = pd.merge(acc,pii,on="LOS-APP-ID",how="left")
# enq = pd.merge(enq,pii,on="LOS-APP-ID",how="left")


# In[58]:


acc.drop(columns="Unnamed: 42",inplace=True)
enq.drop(columns="Unnamed: 9",inplace=True)




# In[60]:


enquiry_df_read = enq.copy()#[enq["LOS-APP-ID"].isin(acc["LOS-APP-ID"].unique()[:1000])]





# In[62]:


acc_df_read = acc.copy()#[acc["LOS-APP-ID"].isin(acc["LOS-APP-ID"].unique()[:1000])]



# In[65]:


acc_df_read["lead_id"] = acc_df_read["LOS-APP-ID"]
enquiry_df_read["lead_id"] = enquiry_df_read["LOS-APP-ID"]




# In[67]:


#acc_df_read = acc_df_read[acc_df_read['LOS-APP-ID'].isin(acc_df_read['LOS-APP-ID'].sample(10).values)]
#enquiry_df_read = enquiry_df_read[enquiry_df_read['LOS-APP-ID'].isin(acc_df_read['LOS-APP-ID'].sample(10).values)]


# In[68]:


df_pii = pd.read_csv(os.path.join(SCRUB_DIR, 'Inquiry.csv'), sep='|', usecols=["RETRO-PR-DT","LOS-APP-ID","DOB"])
df_pii = df_pii[df_pii['LOS-APP-ID'].isin(df_part_1['LOS-APP-ID'].tolist())]






# In[70]:


df_pii["lead_id"] = df_pii["LOS-APP-ID"]


# In[71]:


df_pii.rename(columns={"RETRO-PR-DT":"retro date","DOB":"dob","LOS-APP-ID":"LOS_APP_ID/UNIQUE_ID"},inplace=True)
# df_pii.sample(9)


# In[72]:


# df_pii.agg(['nunique', 'count'])


# In[73]:


df_pii.rename(columns={'dob' : 'dateofbirth'},inplace=True)



# ## Parsing Scrubs

# In[75]:


# def month_difference(dateprocessed, payment_history_start_date):
#             # Convert each date to the 1st day of the next month
#             ph_next_month_start = payment_history_start_date  #+ pd.offsets.MonthBegin(1)
#             dateprocessed_same_month_start = dateprocessed + pd.offsets.MonthBegin(0)
#             month_difference = ((dateprocessed_same_month_start - ph_next_month_start).dt.days // 30).astype(int)
#             return month_difference

def month_difference(dateprocessed, payment_history_start_date):

    # Calculate difference using year and month components to avoid nanosecond overflow
    month_diff = (dateprocessed.dt.year - payment_history_start_date.dt.year) * 12 + \
                 (dateprocessed.dt.month - payment_history_start_date.dt.month)

    # Return as integer (handle NaNs if necessary)
    return month_diff.fillna(0).astype(int)

bank_type_map = {
    'NBF': 'NBFC',
    'COP': 'CO_OPERATIVE_BANK',
    'MFI': 'NGO_MFI',
    'HF': 'HOUSING_FINANCE_COMPANY',
    'RRB': 'REGIONAL_RURAL_BANK',
    'FRB': 'FOREIGN_BANK',
    'NAB': 'NATIONALIZED_BANK',
    'PRB': 'PRIVATE_BANK',
    'INS': 'INSURANCE_COMPANY',
    'SBI': 'SBI_AND_ASSOCIATES',
    'ARC': 'ASSET_RECONSTRUCTION_COMPANY',
    'CCC': 'CREDIT_CARD_COMPANY',
    'DTC': 'DIRECT_TO_CONSUMER',
    'OFI': 'OTHER_FINANCIAL_INSTITUTIONS',
    'SFC': 'STATE_FINANCIAL_CORPORATION',
    'NHB': 'NATIONAL_HOUSING_BANK',
    'TSR': 'TRUSTS',
    'SFB': 'SMALL_FINANCE_BANK',
    'SHG': 'SELF_HELP_GROUP'
}


# Mapping from emi_2 to adjustment factor (multiplier to get monthly equivalent)
freq_adjustment = {
    'Annually': lambda x: x / 12,
    'Yearly': lambda x: x / 12,
    'Semi annually': lambda x: x / 6,
    'Half yearly': lambda x: x / 6,
    'Quarterly': lambda x: x / 3,
    'Monthly': lambda x: x,
    'Weekly': lambda x: x * 4,
    'BiWeekly': lambda x: x * 2,
    'BiMonthly': lambda x: x / 2,
    'Daily': lambda x: x * 30,
    # Treat all these as monthly (no adjustment)
    'Bullet payment': lambda x: x,
    'On-demand': lambda x: x,
    'Single Payment Loan (bullet': lambda x: x,
    'Other': lambda x: x
}


unknown_freqs = set()

# Function to compute
def compute_monthly_emi(row):
    emi_amount = row['emi_amount_parsed']
    freq = row['emi_2']
    adjust_func = freq_adjustment.get(freq)
    if adjust_func is not None:
        return adjust_func(emi_amount)
    else:
        unknown_freqs.add(freq)
        return emi_amount


def acc_scrub_parsing(df_acc,pii):
    # df_acc = pd.read_csv('../data/acc_list_sample_10000_ids.csv')
    # pii = pd.read_csv("../data/pii_file_with_all_columns_10k_sample.csv")
    df_acc.columns =df_acc.columns.str.strip()
    try:
        rename_dict =  {'CLOSE-DT' : 'date_closed__c',
                             'DISBURSED-DT' : 'date_opened_or_disbursed__c',
                             'DISBURSED-AMT/HIGH CREDIT' : 'high_credit_or_sanctioned_amount__c',
                             'ACCT-TYPE' : 'acct_type__c',
                             'CREDIT-LIMIT/SANC AMT' : 'credit_limit__c',
                             'dateprocessed' : 'createddate',
                             'INSTALLMENT-AMT' : 'emi_amount__c',
                             'TENURE' : 'repayment_tenure__c',
                             'OWNERSHIP-IND' : 'ownershipindicator',
                             'DATE-REPORTED': 'date_reported__c',
                            "LOS-APP-ID":'multibureau__c',
                            'ACCT-TYPE':'acct_type__c',
                            'OWNERSHIP-IND':'ownershipindicator',
                            'LAST-PAYMENT-DATE':'payment_history_start_date__c',
                            'CURRENT-BAL':'current_balance__c',
                            'OVERDUE-AMT':'overdue_amount__c'}
        df_acc.rename(columns=rename_dict, inplace=True)
    except:
        print("column renaming error")

    #print(df_acc.multibureau__c.nunique())
    df_acc = df_acc.dropna(subset=['date_opened_or_disbursed__c'])
    #print(df_acc.multibureau__c.nunique())

    df_acc['id'] = df_acc.groupby('multibureau__c').cumcount() + 1
    for _cols in ['date_reported__c','date_opened_or_disbursed__c','date_closed__c','payment_history_start_date__c']:
        df_acc[_cols] = pd.to_datetime(df_acc[_cols],format="%d-%m-%Y",errors='coerce')
#     meging to get createddate or reported date
    mapper_createddate = pii[['LOS_APP_ID/UNIQUE_ID','retro date']].rename(columns={'LOS_APP_ID/UNIQUE_ID':'multibureau__c'})
    df_acc = df_acc.merge(mapper_createddate,on='multibureau__c',how='left')
    df_acc.rename(columns = {'retro date' : 'createddate'},inplace = True)
    df_acc['createddate'] = pd.to_datetime(df_acc['createddate'], format = '%d-%m-%Y')
#     df_acc = df_acc[~df_acc['acct_type__c'].isin('JLG Group,JLG Individual,Individual,SHG Group,SHG Individual,SHG Group - Govt,SHG Intra-Group,Others'.split(','))]
    #print(df_acc.multibureau__c.nunique())
    df_acc = df_acc[df_acc['date_reported__c'] >= df_acc['date_opened_or_disbursed__c']]
    #print(df_acc.multibureau__c.nunique())
    df_acc = df_acc[df_acc['createddate'] > df_acc['date_opened_or_disbursed__c']]
    #print(df_acc.multibureau__c.nunique())
    df_acc = df_acc[df_acc['REPORTED DATE - HIST'].notnull()]
    #print(df_acc.multibureau__c.nunique())
    df_acc = df_acc[df_acc['DPD - HIST'].notnull()]
    #print(df_acc.multibureau__c.nunique())
    df_acc['date_closed__c']  = np.where(df_acc['date_closed__c']>df_acc['createddate'],pd.NaT,df_acc['date_closed__c'])
    df_acc['date_closed__c'] = pd.to_datetime(df_acc['date_closed__c'])
    df_acc['payment_history_1__c'] = df_acc['DPD - HIST'].str[:54]
    df_acc['payment_history_2__c'] = df_acc['DPD - HIST'].str[54:]
    df_acc['product_2'] = df_acc['acct_type__c'].map(product_group_dict_2()[1])
    df_acc['product_2'].fillna('others', inplace=True)
    df_acc['product_sec_unsec'] = df_acc['acct_type__c'].map(product_group_dict_2()[2])
    df_acc['product_sec_unsec'].fillna('others', inplace=True)
    def clean_amount(value):
        if isinstance(value, str):
            value = value.replace(',', '').replace("'", '')
        return pd.to_numeric(value, errors='coerce')
    df_acc['current_balance__c'] = df_acc['current_balance__c'].apply(clean_amount)
    df_acc['credit_limit__c'] = df_acc['credit_limit__c'].apply(clean_amount)
    df_acc['high_credit_or_sanctioned_amount__c'] = df_acc['high_credit_or_sanctioned_amount__c'].apply(clean_amount)
    df_acc['overdue_amount__c'] = df_acc['overdue_amount__c'].apply(clean_amount)
#     Parsing emi_amount column
    df_acc['emi_amount_list'] = df_acc['emi_amount__c'].apply(lambda x: str(x).strip().split('/'))
    df_split = df_acc['emi_amount_list'].apply(pd.Series)
    df_split.columns = [f'emi_{i+1}' for i in df_split.columns]
    df_acc = pd.merge(df_acc,df_split,left_index = True,right_index = True)
    df_acc['emi_amount_parsed'] = df_acc['emi_1'].apply(lambda x: str(x).replace(',', ''))
    df_acc['emi_amount_parsed'] = df_acc['emi_amount_parsed'].astype(float)
    df_acc['emi_2'] = df_acc['emi_2'].str.strip()
    df_acc['emi_amount_parsed'] = df_acc.apply(compute_monthly_emi, axis=1)

    #print(df_acc.multibureau__c.nunique())

    def clean_amt_list(list_item):
        cleaned_list = []
        for item in list_item:
            if item == '':
                cleaned_list.append('0')
            else:
                cleaned_list.append(str(item))
        cleaned_list = cleaned_list[:-1]
        return cleaned_list
#     creating payment_history_start_date__c using REPORTED DATE - HIST
    df_acc['LAST-PAYMENT-DATE_proxy'] = df_acc['REPORTED DATE - HIST'].apply(
        lambda x: pd.to_datetime(str(x).split(',')[0], errors='coerce').strftime("%d-%m-%Y")
        if pd.to_datetime(str(x).split(',')[0], errors='coerce') is not pd.NaT
        else np.nan
    )
    df_acc['LAST-PAYMENT-DATE_proxy'] = pd.to_datetime(df_acc['LAST-PAYMENT-DATE_proxy'],format="%d-%m-%Y")
    df_acc = df_acc[df_acc['LAST-PAYMENT-DATE_proxy']>= '1950-05-31']


    if df_acc['LAST-PAYMENT-DATE_proxy'].isna().sum() > 0:
        print("Erroneous values in REPORTED DATE - HIST, Please check")
        print(df_acc[df_acc['LAST-PAYMENT-DATE_proxy'].isna()]['REPORTED DATE - HIST'].values)
        print('Shape before filter',df_acc.shape)
        df_acc = df_acc[df_acc['LAST-PAYMENT-DATE_proxy'].notnull()]
        print('Shape after filter',df_acc.shape)
    df_acc['payment_history_start_date__c'] = df_acc['LAST-PAYMENT-DATE_proxy']
    df_acc['payment_history_start_date__c'] = pd.to_datetime(df_acc['payment_history_start_date__c'], format = "%d-%m-%Y")
    df_acc['payment_history_start_date__c'] = df_acc['payment_history_start_date__c'].map(lambda x : x.replace(day=1))

    df_acc['payment_history_end_date__c'] = df_acc['REPORTED DATE - HIST'].apply(lambda x: pd.to_datetime(str(x).split(',')[-2], errors='coerce').strftime("%d-%m-%Y")
        if pd.to_datetime(str(x).split(',')[0], errors='coerce') is not pd.NaT
        else np.nan)

    df_acc['payment_history_end_date__c'] = pd.to_datetime(df_acc['payment_history_end_date__c'], format = "%d-%m-%Y").map(lambda x : x.replace(day=1))


    df_acc['padding'] = month_difference(df_acc['createddate'], df_acc['payment_history_start_date__c']).apply(lambda x: [np.nan] * x)
#     parsing current_balance history column and padding it
    nan=np.nan
    df_acc['cur_bal_list'] = df_acc['CUR BAL - HIST'].apply(lambda x : list(map(float, clean_amt_list(str(x).strip().split(','))))) #$.replace("nan",'np.nan').apply(eval)
    #df_acc['cur_bal_list'] = df_acc['cur_bal_list'].apply(clean_amt_list)
    df_acc['CUR BAL - HIST_mod_padded'] = np.where(df_acc['date_closed__c'].notnull() , df_acc['padding'] +  df_acc['cur_bal_list'] ,df_acc['cur_bal_list'])
#     parsing amt paid history column and padding it
    #df_acc['amt_paid_list'] = df_acc['AMT PAID - HIST'].apply(lambda x : map(float, str(x).strip().split(',')))
    df_acc['amt_paid_list'] = df_acc['AMT PAID - HIST'].apply(lambda x : list(map(float, clean_amt_list(str(x).strip().split(',')))))
    #df_acc['amt_paid_list'] = df_acc['amt_paid_list'].apply(clean_amt_list)
    df_acc['AMT PAID - HIST_mod_padded'] = np.where(df_acc['date_closed__c'].notnull() , df_acc['padding'] +  df_acc['amt_paid_list'] ,df_acc['amt_paid_list'])
#     parsing high credit/sanctioned hist column and padding it
    #df_acc['amt_high_sanc_list'] = df_acc['HIGH CRD - HIST'].apply(lambda x : map(float, str(x).strip().split(',')))
    df_acc['amt_high_sanc_list'] = df_acc['HIGH CRD - HIST'].apply(lambda x : list(map(float, clean_amt_list(str(x).strip().split(',')))))
    #df_acc['amt_high_sanc_list'] = df_acc['amt_high_sanc_list'].apply(clean_amt_list)
    df_acc['HIGH CRD - HIST_mod_padded'] = np.where(df_acc['date_closed__c'].notnull() , df_acc['padding'] +  df_acc['amt_high_sanc_list'] ,df_acc['amt_high_sanc_list'])
#     one hot encoding for CONTRIBUTOR-TYPE column
    df_acc['bank_type'] = df_acc['CONTRIBUTOR-TYPE'].map(bank_type_map)
    bank_type_one_hot = pd.get_dummies(df_acc['bank_type'], prefix='reported_by')
    df_acc = pd.concat([df_acc, bank_type_one_hot], axis=1)
    return  df_acc

def enq_scrub_parsing(df_enq,pii):

    #df_enq['inquiry_type__c'] = df_enq['PURPOSE'].map(mapper)
    df_enq.rename(columns = {'AMOUNT' : 'enquiry_amount__c', 'PURPOSE' : 'enquiry_purpose__c'}, inplace = True)
    df_enq['enquiry_amount__c'] = df_enq['enquiry_amount__c'].astype(str).str.replace(',', '').astype(int)
    df_enq.rename(columns = {'INQUIRY-DATE' : 'date_reported__c'}, inplace = True)
    df_enq['date_reported__c'] = pd.to_datetime(df_enq['date_reported__c'], format = '%d-%m-%Y')
    multibureau_enquiry_list = pd.merge(df_enq, pii[['LOS_APP_ID/UNIQUE_ID', 'retro date',"lead_id"]], left_on = 'LOS-APP-ID', right_on = 'LOS_APP_ID/UNIQUE_ID', how ='inner')
    multibureau_enquiry_list.rename(columns = {'retro date' : 'createddate'},inplace = True)
    #multibureau_enquiry_list = multibureau_enquiry_list[~multibureau_enquiry_list['inquiry_type__c'].isin('JLG Group,JLG Individual,JLG-Individual,JLG-Group,Individual,SHG-Group,SHG Group,SHG-Individual,SHG Individual,SHG Group - Govt,SHG Intra-Group,Others'.split(','))]
    multibureau_enquiry_list['createddate'] = pd.to_datetime(multibureau_enquiry_list['createddate'], format = '%d-%m-%Y')
#     display(multibureau_enquiry_list['createddate'].head())
    #multibureau_enquiry_list = multibureau_enquiry_list[multibureau_enquiry_list['createddate']> multibureau_enquiry_list['date_reported__c']]
    multibureau_enquiry_list.rename(columns = {'LOS-APP-ID' : 'multibureau__c'}, inplace = True)
    multibureau_enquiry_list = multibureau_enquiry_list.reset_index(drop = True).reset_index(names = 'id')
    return multibureau_enquiry_list




# In[79]:


print("enquiry_df_read['LOS-APP-ID'].nunique() : ",enquiry_df_read["LOS-APP-ID"].nunique())


# In[80]:


print("df_pii['LOS_APP_ID/UNIQUE_ID'].nunique() : ",df_pii["LOS_APP_ID/UNIQUE_ID"].nunique())


# In[81]:


print("acc_df_read['LOS-APP-ID'].nunique(): ",acc_df_read["LOS-APP-ID"].nunique())


# In[82]:


from tqdm import tqdm
def split(list_a, chunk_size):
    for i in tqdm(range(0, len(list_a), chunk_size)):
        yield list_a[i:i + chunk_size]


# In[83]:


ids_pii = df_pii["LOS_APP_ID/UNIQUE_ID"].unique().tolist()
print("ids_pii's len :",len(ids_pii))


# In[84]:


# enquiry_df_read.head()


# In[85]:


import gc


# In[86]:


enquiry_df=pd.DataFrame()
acc_df=pd.DataFrame()
for id_subset in split(ids_pii,10000):

    enq_chunk = enquiry_df_read[enquiry_df_read["LOS-APP-ID"].isin(id_subset)]
    pii_chunk = df_pii[df_pii["LOS_APP_ID/UNIQUE_ID"].isin(id_subset)]

    df_temp_enq = enq_scrub_parsing(enq_chunk, pii_chunk)
    enquiry_df = pd.concat([enquiry_df,df_temp_enq])
    del enq_chunk
    del pii_chunk
    gc.collect


# In[87]:


print("enquiry_df.multibureau__c.nunique() : ",enquiry_df.multibureau__c.nunique())




# In[89]:


import traceback

acc_df = pd.DataFrame()
failed_details = []

for id_subset in split(ids_pii, 10000):
    acc_chunk = acc_df_read[acc_df_read["LOS-APP-ID"].isin(id_subset)].copy()
    pii_chunk = df_pii[df_pii["LOS_APP_ID/UNIQUE_ID"].isin(id_subset)].copy()

    try:
        df_temp_acc = acc_scrub_parsing(acc_chunk, pii_chunk)
        acc_df = pd.concat([acc_df, df_temp_acc], ignore_index=True)
    except Exception as e:
        error_message = str(e)
        # Get original row numbers and IDs
        for idx, row in acc_chunk.iterrows():
            failed_details.append({
                'row_index': idx,
                'LOS_APP_ID': row.get("LOS-APP-ID", None),
                'error': error_message
            })
        print(f"❌ Error in chunk of {len(id_subset)} IDs: {error_message}")

    del acc_chunk, pii_chunk
    gc.collect()

df_failed = pd.DataFrame(failed_details)


# In[90]:


print("acc_df.multibureau__c.nunique() :",acc_df.multibureau__c.nunique())


# In[91]:


print("acc_df_read['LOS-APP-ID'].nunique(): ",acc_df_read["LOS-APP-ID"].nunique())


# In[92]:


# acc_df.to_parquet("../data/parsed_acc_list_retro.parquet",index=False)


# In[93]:


# acc_df = pd.read_csv("parsed_acc_list.csv")
# acc_df = pd.read_csv("../data/parsed_acc_list_retro.csv")


# In[94]:


print("acc_df.multibureau__c.nunique() :",acc_df.multibureau__c.nunique())





acc_df = quality_check_multibureau(acc_df, df_pii)


# In[104]:


print("acc_df.multibureau__c.nunique() :",acc_df.multibureau__c.nunique())



# ## Enquiry Features

# In[106]:


# need to pass 'overall' in product_list if we need to calculate perc enq features
start_time = time.time()
enquiry_feats_last_n_months = pd.DataFrame()
enquiry_ids_ls = enquiry_df['multibureau__c'].unique().tolist()
for id_subset in split(enquiry_ids_ls, 10000):
    temp_enquiry_feats_last_n_months = enquiries_feats_last_n_months(enquiry_df[enquiry_df.multibureau__c.isin(id_subset)],
                                                                product_list = ['overall','unsecured','secured','pl','cc','od',
                                                                                'gl','hl','lap','al','twl','cl','el','bl'],
                                                                months_list = [3,6,9,12,15],
                                                                cal_perc_enq_cnt = True,
                                                                cal_perc_enq_amt = True,
                                                                enq_amt_cuts_product_list=['pl','hl','cl','bl','cc'])
    enquiry_feats_last_n_months = pd.concat([enquiry_feats_last_n_months, temp_enquiry_feats_last_n_months], ignore_index=True)
end_time=time.time()
print(end_time-start_time, 's')


# In[107]:


print("enquiry_feats_last_n_months.shape :",enquiry_feats_last_n_months.shape)
# In[109]:


# enquiry_feats_last_n_months[["days_since_latest_enq","last_12m_overall_enq_count"]].describe()


# In[110]:


df_pii.rename(columns={"LOS_APP_ID/UNIQUE_ID":"multibureau__c"},inplace=True)


# In[111]:


enquiry_feats_last_n_months_1 = pd.merge(df_pii[["lead_id","multibureau__c"]],enquiry_feats_last_n_months[["multibureau__c","last_12m_overall_enq_count"]],on = "multibureau__c",how="inner")
# enquiry_feats_last_n_months_1.head()


# In[112]:


print("enquiry_feats_last_n_months.shape :",enquiry_feats_last_n_months.shape)
print("enquiry_feats_last_n_months_1.shape : ",enquiry_feats_last_n_months_1.shape)


# In[113]:


enquiry_feats_last_n_months_1[["lead_id","last_12m_overall_enq_count"]].to_csv("last_12m_overall_enq_count_feature.csv",index=False)


# In[114]:


# enquiry_feats_last_n_months_check[["days_since_latest_enq","last_12m_overall_enq_count"]].describe()



# In[117]:


# need to pass 'overall' in product_list if we need to calculate perc enq features
start_time=time.time()
enquiry_feats_before_1_year = enquiries_feats_before_n_years(enquiry_df,
                                                             product_list = ['overall','unsecured','secured','pl','cc','od',
                                                                             'gl','hl','lap','al','twl','cl','el','bl'],
                                                             years_list = [1],
                                                             cal_perc_enq_cnt = True,
                                                             cal_perc_enq_amt = True,
                                                             enq_amt_cuts_product_list=['pl','hl','cl','bl','cc'])

end_time=time.time()
print(end_time-start_time, 's')


# In[118]:


print("enquiry_feats_before_1_year.shape : ",enquiry_feats_before_1_year.shape)


# In[119]:


# enquiry_feats_before_1_year.head()


# In[120]:


enquiry_feats = pd.merge(enquiry_feats_last_n_months, enquiry_feats_before_1_year, on='multibureau__c')


# In[121]:


print("enquiry_feats.shape : ",enquiry_feats.shape)


# In[122]:


enquiry_feats_last_n_months.to_csv("enquiry_feats_last_n_months.csv", index=False)
enquiry_feats_before_1_year.to_csv("enquiry_feats_before_1_year.csv", index=False)
enquiry_feats.to_csv("enquiry_feats.csv", index=False)


# In[123]:


del enquiry_feats_last_n_months, enquiry_feats_before_1_year


# In[124]:


gc.collect()



# # Account List  Feature

# ## Overall Account Features

# In[125]:


# temp_acc = acc_df.copy()
# temp_acc.drop('product', axis = 1, inplace = True)


# In[126]:


# for id_subset in split(ids_pii, 10000):
#     acc_chunk = acc_df_read[acc_df_read["LOS-APP-ID"].isin(id_subset)].copy()
#     pii_chunk = df_pii[df_pii["LOS_APP_ID/UNIQUE_ID"].isin(id_subset)].copy()

#     try:
#         df_temp_acc = acc_scrub_parsing(acc_chunk, pii_chunk)
#         acc_df = pd.concat([acc_df, df_temp_acc], ignore_index=True)


# In[127]:


start_time = time.time()
acc_df.rename(columns = {'acct_type__c' : 'account_type__c'}, inplace = True)
overall_acc_feats = pd.DataFrame()
acc_ids_ls = acc_df['multibureau__c'].unique().tolist()
for id_subset in split(acc_ids_ls, 10000):

    temp_overall_acc_feats = overall_tradelines_feats(acc_df[acc_df.multibureau__c.isin(id_subset)],
                                                 product_list=['overall','unsecured','secured','pl','cc','od',
                                                               'gl','hl','lap','al','twl','cl','el','bl', 'mf'],
                                                 cal_perc_acc_cnt=True,
                                                 cal_perc_acc_amt=True,
                                                 loan_amt_cuts_product_list=['pl','hl','cl','bl','cc'])

    overall_acc_feats = pd.concat([overall_acc_feats, temp_overall_acc_feats], ignore_index=True)

end_time=time.time()
print(end_time-start_time, 's')


# In[128]:


print("overall_acc_feats.shape ->", overall_acc_feats.shape)


# In[129]:


print("overall_acc_feats.head() ->", overall_acc_feats.head())


# In[130]:


overall_acc_feats.to_csv("overall_acc_feats.csv", index=False)


# In[131]:


del temp_overall_acc_feats


# In[132]:


print("acc_df.multibureau__c.nunique() ->", acc_df.multibureau__c.nunique())


# In[ ]:





# ## Closed Account Features

# In[133]:


start_time = time.time()
closed_acc_feats_last_n_months = pd.DataFrame()
acc_ids_ls = acc_df['multibureau__c'].unique().tolist()
for id_subset in split(acc_ids_ls, 10000):
    temp_closed_acc_feats_last_n_months = closed_tradelines_feats_last_n_months(acc_df[acc_df.multibureau__c.isin(id_subset)],
                                                                            product_list=['overall','unsecured','secured','pl','cc','od',
                                                                                          'gl','hl','lap','al','twl','cl','el','bl', 'mf'],
                                                                            months_list=[3,6,9,12,15],
                                                                            cal_perc_acc_cnt=True,
                                                                            cal_perc_acc_amt=True,
                                                                            loan_amt_cuts_product_list=['pl','hl','cl','bl','cc'])

    closed_acc_feats_last_n_months = pd.concat([closed_acc_feats_last_n_months, temp_closed_acc_feats_last_n_months], ignore_index=True)

end_time=time.time()
print(end_time-start_time, 's')
del temp_closed_acc_feats_last_n_months


# In[134]:


print("closed_acc_feats_last_n_months.shape ->", closed_acc_feats_last_n_months.shape)


# In[135]:


print("closed_acc_feats_last_n_months.head():\n", closed_acc_feats_last_n_months.head())


# In[136]:


print("acc_df['date_closed__c'] sample:\n", acc_df['date_closed__c'].head())


# In[137]:


start_time = time.time()
closed_acc_feats_before_1_year = pd.DataFrame()
acc_ids_ls = acc_df['multibureau__c'].unique().tolist()
for id_subset in split(acc_ids_ls, 10000):
    temp_closed_acc_feats_before_1_year = closed_tradelines_feats_before_n_years(acc_df[acc_df.multibureau__c.isin(id_subset)],
                                                                            product_list=['overall','unsecured','secured','pl','cc','od',
                                                                                          'gl','hl','lap','al','twl','cl','el','bl', 'mf'],
                                                                            years_list=[1],
                                                                            cal_perc_acc_cnt=True,
                                                                            cal_perc_acc_amt=True,
                                                                            loan_amt_cuts_product_list=['pl','hl','cl','bl','cc'])
    closed_acc_feats_before_1_year = pd.concat([closed_acc_feats_before_1_year, temp_closed_acc_feats_before_1_year], ignore_index=True)

end_time=time.time()
print(end_time-start_time, 's')


# In[138]:


print("closed_acc_feats_before_1_year.shape ->", closed_acc_feats_before_1_year.shape)


# In[139]:


print("closed_acc_feats_before_1_year.head() ->", closed_acc_feats_before_1_year.head())


# In[140]:


closed_acc_feats = pd.merge(closed_acc_feats_last_n_months, closed_acc_feats_before_1_year, on='multibureau__c')


# In[141]:


print("closed_acc_feats.shape ->", closed_acc_feats.shape)


# In[142]:


closed_acc_feats_last_n_months.to_csv("closed_acc_feats_last_n_months.csv", index=False)
closed_acc_feats_before_1_year.to_csv("closed_acc_feats_before_1_year.csv", index=False)
closed_acc_feats.to_csv("closed_acc_feats.csv", index=False)


# In[143]:


del closed_acc_feats_last_n_months, closed_acc_feats_before_1_year


# ## Opened Account Features

# In[144]:


start_time = time.time()
opened_acc_feats_last_n_months = pd.DataFrame()
acc_ids_ls = acc_df['multibureau__c'].unique().tolist()
for id_subset in split(acc_ids_ls, 10000):
    temp_opened_acc_feats_last_n_months = opened_tradelines_feats_last_n_months(acc_df[acc_df.multibureau__c.isin(id_subset)],
                                                                           product_list=['overall','unsecured','secured','pl','cc','od',
                                                                                         'gl','hl','lap','al','twl','cl','el','bl', 'mf'],
                                                                           months_list=[3,6,9,12,15],
                                                                           cal_perc_acc_cnt=True,
                                                                           cal_perc_acc_amt=True,
                                                                           loan_amt_cuts_product_list=['pl','hl','cl','bl','cc'])
    opened_acc_feats_last_n_months = pd.concat([opened_acc_feats_last_n_months, temp_opened_acc_feats_last_n_months], ignore_index=True)

end_time=time.time()
print(end_time-start_time, 's')


# In[145]:


print("opened_acc_feats_last_n_months.shape ->", opened_acc_feats_last_n_months.shape)


# In[146]:


print("opened_acc_feats_last_n_months.head() ->", opened_acc_feats_last_n_months.head())


# In[ ]:


start_time = time.time()
opened_acc_feats_before_1_year = pd.DataFrame()
acc_ids_ls = acc_df['multibureau__c'].unique().tolist()
for id_subset in split(acc_ids_ls, 10000):
    temp_opened_acc_feats_before_1_year = opened_tradelines_feats_before_n_years(acc_df[acc_df.multibureau__c.isin(id_subset)],
                                                                            product_list=['overall','unsecured','secured','pl','cc','od',
                                                                                          'gl','hl','lap','al','twl','cl','el','bl', 'mf'],
                                                                            years_list=[1],
                                                                            cal_perc_acc_cnt=True,
                                                                            cal_perc_acc_amt=True,
                                                                            loan_amt_cuts_product_list=['pl','hl','cl','bl','cc'])
    opened_acc_feats_before_1_year = pd.concat([opened_acc_feats_before_1_year, temp_opened_acc_feats_before_1_year], ignore_index=True)

end_time=time.time()
print(end_time-start_time, 's')


# In[ ]:


print("opened_acc_feats_before_1_year.shape ->", opened_acc_feats_before_1_year.shape)


# In[ ]:


print("opened_acc_feats_before_1_year.head() ->", opened_acc_feats_before_1_year.head())


# In[ ]:


opened_acc_feats = pd.merge(opened_acc_feats_last_n_months, opened_acc_feats_before_1_year, on='multibureau__c')


# In[ ]:


print("opened_acc_feats.shape ->", opened_acc_feats.shape)


# In[ ]:


opened_acc_feats_last_n_months.to_csv("opened_acc_feats_last_n_months.csv", index=False)
opened_acc_feats_before_1_year.to_csv("opened_acc_feats_before_1_year.csv", index=False)
opened_acc_feats.to_csv("opened_acc_feats.csv", index=False)


# In[ ]:


del opened_acc_feats_last_n_months, opened_acc_feats_before_1_year


# In[ ]:


gc.collect()


# In[ ]:





# In[ ]:





# In[ ]:





# ## Live Account Features

# In[ ]:


start_time = time.time()
live_acc_feats = pd.DataFrame()
acc_ids_ls = acc_df['multibureau__c'].unique().tolist()
for id_subset in split(acc_ids_ls, 10000):
    temp_live_acc_feats = live_tradelines_features(acc_df[acc_df.multibureau__c.isin(id_subset)],
                                              product_list=['overall','unsecured','secured','pl','cc','od',
                                                            'gl','hl','lap','al','twl','cl','el','bl','mf'],
                                              cal_perc_acc_cnt=True,
                                              cal_perc_acc_amt=True,
                                              loan_amt_cuts_product_list=['pl','hl','cl','bl','cc'],
                                              cal_perc_curr_bal=True,
                                              cal_cb_to_sanc_ratio=True)

    live_acc_feats = pd.concat([live_acc_feats, temp_live_acc_feats], ignore_index=True)
end_time=time.time()
print(end_time-start_time, 's')


# In[ ]:


print("live_acc_feats.shape ->", live_acc_feats.shape)


# In[ ]:


print("live_acc_feats.head() ->", live_acc_feats.head())


# In[ ]:


live_acc_feats.to_csv("live_acc_feats.csv", index=False)


# In[ ]:





# ## Overdue Features

# In[ ]:


# overdue_amt_cuts should have only 2 amount values
start_time = time.time()
overdue_feats = pd.DataFrame()
acc_ids_ls = acc_df['multibureau__c'].unique().tolist()
for id_subset in split(acc_ids_ls, 10000):
    temp_overdue_feats = overdue_features(acc_df[acc_df.multibureau__c.isin(id_subset)],product_list=['overall','unsecured','secured','pl','cc','od',
                                                            'gl','hl','lap','al','twl','cl','el','bl', 'mf'] , overdue_amt_cuts = [1000, 5000])

    overdue_feats = pd.concat([overdue_feats, temp_overdue_feats], ignore_index=True)
end_time=time.time()
print(end_time-start_time, 's')


# In[ ]:


print("overdue_feats.shape ->", overdue_feats.shape)


# In[ ]:


print("overdue_feats.head() ->", overdue_feats.head())


# In[ ]:


overdue_feats.to_csv("overdue_feats.csv", index=False)


# ## CC Utilization Features

# In[ ]:


start_time = time.time()
cc_utilization_feats = cc_utilization_features(acc_df)
end_time=time.time()
print(end_time-start_time, 's')


# In[ ]:


print("cc_utilization_feats.shape ->", cc_utilization_feats.shape)


# In[ ]:


print("cc_utilization_feats.head() ->", cc_utilization_feats.head())


# In[ ]:


cc_utilization_feats.to_csv("cc_utilization_feats.csv", index=False)


# In[ ]:





# ## Ownership Features

# In[ ]:


print("acc_df shape (ownership features):", acc_df.shape)


# In[ ]:


print("acc_df ownershipindicator value_counts:\n", acc_df.ownershipindicator.value_counts(dropna=False))


# In[ ]:


acc_df.rename(columns = {'ownershipindicator' : 'ownership_indicator__c'}, inplace = True)


# In[ ]:


def ownership_mapper_dict():
    ownership_mapper = {}
    ownership_mapper["Individual"] = 1
    ownership_mapper["Authorised User"] = 2
    ownership_mapper["Guarantor"] = 3
    ownership_mapper["Joint"] = 4
    ownership_mapper["Deceased"] = 5

    return ownership_mapper

def ownership_features(df2):

    df = df2.copy(deep=True)


    ownership_df = df[['multibureau__c', 'ownership_indicator__c']].copy(deep=True)
    ownership_mapper = ownership_mapper_dict()
    ownership_df['ownership'] = ownership_df['ownership_indicator__c'].map(ownership_mapper)

    # Create a flag for counting
    ownership_df['count'] = 1

    # Pivot to count number of tradelines by ownership type
    ownership_pivot = ownership_df.pivot_table(
        index='multibureau__c',
        columns='ownership',
        values='count',
        aggfunc='sum',
        fill_value=0
    )

    # Calculate total tradelines per multibureau
    ownership_pivot['total_ownership_tradelines'] = ownership_pivot.sum(axis=1)

    # Create percentage columns
    perc_df = ownership_pivot.div(ownership_pivot['total_ownership_tradelines'], axis=0)
    perc_df = perc_df.drop(columns='total_ownership_tradelines')
    display(perc_df.head())
    perc_df.columns = [f'perc_acc_{col.lower()}' for col in perc_df.columns]

    # Rename count columns
    ownership_pivot.columns = [f'num_acc_{col.lower()}' if col != 'total_ownership_tradelines' else col for col in ownership_pivot.columns]

    # Combine count and percentage columns
    result_df = pd.concat([ownership_pivot.reset_index(), perc_df.reset_index(drop=True)], axis=1)

    result_df = result_df.drop('total_ownership_tradelines', axis=1)

    return result_df


# In[ ]:


import pandas as pd

def ownership_mapper_dict():
    # Modified to return clean strings instead of integers 
    # This prevents the .lower() crash and makes feature names readable
    return {
        "Individual": "individual",
        "Authorised User": "authorised_user",
        "Guarantor": "guarantor",
        "Joint": "joint",
        "Deceased": "deceased"
    }

def ownership_features(df2):
    df = df2.copy(deep=True)
    ownership_df = df[['multibureau__c', 'ownership_indicator__c']].copy(deep=True)

    # 1. Apply the string mapper
    ownership_mapper = ownership_mapper_dict()
    ownership_df['ownership'] = ownership_df['ownership_indicator__c'].map(ownership_mapper)

    # 2. Pivot to count number of tradelines by ownership type
    # Using 'count' as aggfunc avoids the need to create a dummy 'count' column of 1s
    ownership_pivot = ownership_df.pivot_table(
        index='multibureau__c',
        columns='ownership',
        values='ownership_indicator__c', 
        aggfunc='count',
        fill_value=0
    )

    # 3. Calculate total tradelines (doing this before adding it to the DF keeps math clean)
    total_tradelines = ownership_pivot.sum(axis=1)

    # 4. Create percentage columns & fill NaNs with 0 (in case a lead has 0 total)
    perc_df = ownership_pivot.div(total_tradelines, axis=0).fillna(0)

    # 5. Rename columns safely
    ownership_pivot.columns = [f'num_acc_{col}' for col in ownership_pivot.columns]
    perc_df.columns = [f'perc_acc_{col}' for col in perc_df.columns]

    # 6. Combine count and percentage columns
    result_df = pd.concat([ownership_pivot, perc_df], axis=1).reset_index()

    # Optional: Remove the categorical axis name left over from pivoting
    result_df.columns.name = None 

    return result_df


# In[ ]:


start_time = time.time()
ownership_feats = ownership_features(acc_df)
end_time=time.time()
print(end_time-start_time, 's')


# In[ ]:


print("ownership_feats.shape ->", ownership_feats.shape)


# In[ ]:


print("ownership_feats.head() ->", ownership_feats.head())


# In[ ]:


ownership_feats.to_csv("../data/features/ownership_feats.csv", index=False)


# In[ ]:





# ## DPD Features

# In[ ]:


acc_df.reset_index(drop=True,inplace=True)


# In[ ]:


def calculate_tradeline_flags_new(row, months_list=[3, 6, 9, 12], dpd_list=[1, 30, 60, 90], required_list=['count', 'max']):


    """
    Generates tradline level DPD flags
    """

    today = pd.to_datetime(row['createddate'], errors='coerce').tz_localize(None)  # Use the value from the DataFrame row
    history = row['payment_history_dictionary']
    parsed_history = {
        pd.to_datetime(k, errors='coerce'): v for k, v in history.items()
    }

    flags = {}

# For "ever", just take all valid records
    all_valid = [v for v in parsed_history.values() if isinstance(v, list) and len(v) > 0]
    all_dpd = [v[0] for v in all_valid]

    months_to_days = months_to_days_dict(months_list)

    for m in months_list:
        days_back = months_to_days[str(m)]
        date_cutoff = today - pd.to_timedelta(days_back, unit='D')
        filtered = {
            k: v for k, v in parsed_history.items()
            if date_cutoff <= k <= today and isinstance(v, list) and len(v) > 0
        }
        filtered_dpd = [v[0] for v in filtered.values()]

        total_key = f'{m}_mob_total_payment_count'
        flags[total_key] = len(filtered_dpd)

        if len(filtered_dpd) == 0:
            # No payments → set DPD counts to NaN
            for d in dpd_list:
                dpd_key = f'{m}_mob_{d}+_dpd_count'
                flags[dpd_key] = np.nan
            if 'max' in required_list:
                flags[f'{m}_mob_max_dpd'] = np.nan
        else:
            for d in dpd_list:
                dpd_key = f'{m}_mob_{d}+_dpd_count'
                flags[dpd_key] = sum(1 for val in filtered_dpd if val > d)

            if 'max' in required_list:
                flags[f'{m}_mob_max_dpd'] = max(filtered_dpd) if filtered_dpd else np.nan

    total_key = 'ever_total_payment_count'
    flags[total_key] = len(all_dpd)

# Ever logic (whole history)
    for d in dpd_list:
        dpd_key = f'ever_{d}+_dpd_count'
        flags[dpd_key] = sum(1 for val in all_dpd if val > d)

    if 'max' in required_list:
        flags['ever_max_dpd'] = max(all_dpd) if all_dpd else 0

    return pd.Series(flags)


# In[ ]:


def sum_nan(series):
    return series.sum(min_count=1)


# In[ ]:


def aggregate_dpd_features_new(df, dpd_feature_list, customer_id_col='multibureau__c'):
    """
    Aggregates DPD features for each customer:
    - Sum for normal DPD count columns
    - Max for max_dpd columns
    - Min/Mean for months_since_last_*_dpd columns
    """

    sum_cols = [col for col in dpd_feature_list
                if 'max_dpd' not in col and not col.startswith('months_since_last_')]

    max_cols = [col for col in dpd_feature_list if 'max_dpd' in col]

    time_cols = [col for col in dpd_feature_list if col.startswith('months_since_last_')]


#     # Build aggregation dictionary
#     agg_dict = {col: 'sum' for col in sum_cols}
#     agg_dict.update({col: 'max' for col in max_cols})
#     agg_dict.update({col: 'min' for col in min_cols})
#     agg_dict.update({col: 'mean' for col in mean_cols})


    # Build aggregation list for groupby().agg()
    agg_dict = {}

    for col in sum_cols:
        agg_dict[f'{col}'] = (col, sum_nan)

    for col in max_cols:
        agg_dict[f'{col}'] = (col, 'max')

    for col in time_cols:
        agg_dict[f'{col}_min'] = (col, 'min')
        agg_dict[f'{col}_mean'] = (col, 'mean')


    # Perform aggregation
#     df_agg = df.groupby(customer_id_col).agg(agg_dict).reset_index()
    df_agg = df.groupby(customer_id_col).agg(**agg_dict).reset_index()


    return df_agg


# In[ ]:


def generate_all_product_group_dpd_aggregates_new(df, dpd_feature_list, product_list=['overall', 'unsecured'], customer_id_col='multibureau__c'):
    """
    For each product group, filter data based on account_type__c and apply DPD aggregation.
    Returns a dictionary with keys as product group names and values as aggregated DataFrames.
    """
    product_dict, _ = product_group_dict()
    result_dict = {}


    for group_name in product_list:
# Filter the DataFrame for relevant account_type__c
        df_group = df[df['account_type__c'].isin(product_dict[group_name])]

        if df_group.empty:
            continue  # skip if no rows match

# Aggregate
        agg_df = aggregate_dpd_features_new(df_group, dpd_feature_list, customer_id_col)

# Optionally rename columns with group prefix (except ID column)
        agg_df = agg_df.rename(columns={
            col: f"{group_name}_{col}" for col in agg_df.columns if col != customer_id_col
        })

        result_dict[group_name] = agg_df

    return result_dict


# In[ ]:


def dpd_main_new(df, months_list=[3, 6, 9, 12], dpd_list=[1, 30, 60, 90], required_list=['count', 'max'],
             product_list=['overall','unsecured','secured','pl','cc','od','gl','hl','lap','al','twl','cl','el','bl'],
             time_buckets = ['3_mob', '6_mob', '9_mob', '12_mob', 'ever']):

    """
    Generates DPD (delinquency) features for each multibureau ID
    """


    for col in ['createddate', 'date_opened_or_disbursed__c', 'date_closed__c']:
        df[col] = pd.to_datetime(pd.to_datetime(df[col]).dt.date)


    df2 = df.copy(deep=True)
    df2['combined_payment_history'] = (df2['payment_history_1__c'].fillna('') + df2['payment_history_2__c'].fillna(''))
    df2['combined_payment_history'] = df2['combined_payment_history'].apply(lambda x: x + 'N' * (108 - len(x)) if len(x) < 108 else x[:108])

    for i in range(36):
        col_name = f'paymt_hst_{i+1:02d}'
        start = i * 3
        end = start + 3
        df2[col_name] = df2['combined_payment_history'].str[start:end]

    df2.replace('NNN',np.nan,inplace = True)

# replace string in  payment values with int
    payment_month_list = [x for x in df2.columns if 'hst' in x.lower()]

    dpd_code_replace_dict = {'901':'000', '902':'SUB', '903': "DBT",'904':'LSS', '905':'SMA'}
    replace_dict = {"XXX": 0,"SUB": 90, "DBT": 90,"SMA": 90,"STD": 0,"LSS": 90,"   ": np.nan,'""': np.nan,'\\"\\"': np.nan}

    df2.loc[:, payment_month_list] = df2[payment_month_list].astype(str).replace(dpd_code_replace_dict).replace(replace_dict).astype(float)


    df2['payment_history_dictionary'] = df2.apply(payment_history_date_wise, axis = 1)
    df2.reset_index(drop = True,inplace = True)

    df_dpd=df2.apply(lambda row: calculate_tradeline_flags_new(row,
                                                           months_list,
                                                           dpd_list,
                                                           required_list
                                                          ),axis=1)
    dpd_feature_list = df_dpd.columns.tolist()
    filtered_df_1 = df2.join(df_dpd)

    # Apply row-wise only where 3_mob_30+_dpd_count > 0
    result_df = filtered_df_1.apply(
        lambda row: months_since_last_x_dpd(row['payment_history_dictionary'], row['createddate'],dpd_list),
        axis=1, result_type='expand'
    )

    # Concatenate the new features back into df2
    filtered_df_1 = pd.concat([filtered_df_1, result_df], axis=1)


    additional_dpd_feature_list = result_df.columns.tolist()
    dpd_feature_list += additional_dpd_feature_list

# result_dict will hold one DataFrame per product group
    result_dict = generate_all_product_group_dpd_aggregates_new(filtered_df_1,
                                                            dpd_feature_list,
                                                            product_list
                                                           )


# Create base DataFrame
    final_df = filtered_df_1[['multibureau__c']].drop_duplicates()



# Merge only available product group aggregates
    for group in result_dict:
        final_df = final_df.merge(result_dict[group], on='multibureau__c', how='left')

# Handle missing columns
    expected_total_cnt_cols = [f'{p}_{t}_total_payment_count'
                                 for p in product_list
                                 for t in time_buckets
                        ]


    expected_dpd_cnt_cols = [f'{p}_{t}_{d}+_dpd_count'
                                 for p in product_list
                                 for t in time_buckets
                                 for d in dpd_list
                        ]


    expected_max_dpd_cols = [f'{p}_{t}_max_dpd'
                                for p in product_list
                                for t in time_buckets
                        ]

    expected_months_since_mean_dpd_cols = [f'{p}_months_since_last_{d}+_dpd_mean'
                                for p in product_list
                                for d in dpd_list
                        ]

    expected_months_since_min_dpd_cols = [f'{p}_months_since_last_{d}+_dpd_min'
                                for p in product_list
                                for d in dpd_list
                        ]
    expected_cols = expected_total_cnt_cols + expected_dpd_cnt_cols + expected_max_dpd_cols + expected_months_since_mean_dpd_cols + expected_months_since_min_dpd_cols

# Handle missing values
    for col in expected_cols:
        if col not in final_df:
            final_df[col] = np.nan

    final_df[expected_total_cnt_cols] = final_df[expected_total_cnt_cols].fillna(0)


    final_dpd_feats = create_dpd_ratios(final_df,
                                        product_list,
                                        dpd_list,
                                        time_buckets
                                       )

    return final_dpd_feats, filtered_df_1#,result_dict,dpd_feature_list, additional_dpd_feature_list, dpd_feature_list#,df2 # final_dpd_feats contains the dpd feats and df2 is just for validation purpose


# In[ ]:


start_time = time.time()
dpd_feats = pd.DataFrame()
acc_ids_ls = acc_df['multibureau__c'].unique().tolist()
for id_subset in split(acc_ids_ls, 10000):
    temp_dpd_feats, df2 = dpd_main_new(acc_df[acc_df.multibureau__c.isin(id_subset)],
                              months_list=[3, 6, 9, 12, 15], dpd_list=[1, 30, 60, 90], required_list=['count', 'max'],
                              product_list=['overall','unsecured','secured','pl','cc','od','gl','hl','lap','al','twl','cl','el','bl', 'mf'],
                              time_buckets = ['3_mob', '6_mob', '9_mob', '12_mob','15_mob' 'ever'])
    dpd_feats = pd.concat([dpd_feats, temp_dpd_feats], ignore_index=True)

end_time = time.time()
print(end_time-start_time, 's')


# In[ ]:


dpd_feats.shape#1051


# In[ ]:


print("dpd_feats.head() ->", dpd_feats.head())


# In[ ]:


dpd_feats.to_csv("../data/features/dpd_feats.csv", index=False)


# ## Written Off Features

# In[ ]:


acc_df.rename(columns = {'WRITE-OFF-AMT' : 'writtenoff_amount_total'}, inplace = True)


# In[ ]:


start_time = time.time()
written_off_feats = create_writtenoff_feat(acc_df)
end_time = time.time()
print(end_time-start_time, 's')


# In[ ]:


print("written_off_feats.shape ->", written_off_feats.shape)


# In[ ]:


print("written_off_feats.head() ->", written_off_feats.head())


# In[ ]:


written_off_feats.to_csv("../data/features/written_off_feats.csv", index=False)


# # Enquiry List and Account List Velocity Feature Creation

# In[ ]:


enquiry_feats_last_n_months = pd.read_csv("./enquiry_feats_last_n_months.csv")
opened_acc_feats_last_n_months = pd.read_csv("./opened_acc_feats_last_n_months.csv")


# In[ ]:


print("enquiry_feats_last_n_months.shape ->", enquiry_feats_last_n_months.shape)


# In[ ]:


print("opened_acc_feats_last_n_months.shape ->", opened_acc_feats_last_n_months.shape)


# In[ ]:


print("enquiry_feats_last_n_months.head() ->", enquiry_feats_last_n_months.head())


# In[ ]:


print("opened_acc_feats_last_n_months.head() ->", opened_acc_feats_last_n_months.head())


# In[ ]:


start_time = time.time()
acc_to_enq_ratio_feats = account_to_enquiry_ratio_features(enquiry_feats_last_n_months,
                                                           opened_acc_feats_last_n_months,
                                                           months_list=[3, 6, 9, 12,15],
                                                           product_list=['overall','unsecured','secured','pl','cc','od',
                                                                         'gl','hl','lap','al','twl','cl','el','bl']
                                                          )
end_time = time.time()
print(end_time-start_time, 's')


# In[ ]:


print("acc_to_enq_ratio_feats.shape ->", acc_to_enq_ratio_feats.shape)


# In[ ]:


print("acc_to_enq_ratio_feats.head() ->", acc_to_enq_ratio_feats.head())


# In[ ]:


acc_to_enq_ratio_feats.to_csv("../data/features/acc_to_enq_ratio_feats.csv", index=False)


# In[ ]:


del enquiry_feats_last_n_months, opened_acc_feats_last_n_months


# # Merging features

# In[ ]:


from functools import reduce

# List of all DataFrames to merge
dfs_to_merge = [acc_df[['multibureau__c']].drop_duplicates(),
    enquiry_feats,
    overall_acc_feats,
    closed_acc_feats,
    opened_acc_feats,
    live_acc_feats,
    overdue_feats,
    cc_utilization_feats,
    ownership_feats,
    dpd_feats,
    written_off_feats,
    acc_to_enq_ratio_feats
]

# Set 'multibureau__c' as index in all DataFrames
dfs_indexed = [df.set_index('multibureau__c') for df in dfs_to_merge]

# Merge using index-based joins (much faster)
merged_feats_df = reduce(lambda left, right: left.join(right, how='outer'), dfs_indexed)

merged_feats_df.reset_index(inplace=True)


# In[ ]:


print("merged_feats_df.shape ->", merged_feats_df.shape)


# In[ ]:


acc_df[['multibureau__c']].drop_duplicates().shape


# # sequence features

# In[ ]:


def month_difference(dateprocessed, payment_history_start_date):
            # Convert each date to the 1st day of the next month
            ph_next_month_start = payment_history_start_date  #+ pd.offsets.MonthBegin(1)
            dateprocessed_same_month_start = dateprocessed + pd.offsets.MonthBegin(0)
            month_difference = ((dateprocessed_same_month_start - ph_next_month_start).dt.days // 30).astype(int)
            return month_difference

def clean_amount(value):
    if isinstance(value, str):
        value = value.replace(',', '').replace("'", '')
    return pd.to_numeric(value, errors='coerce')

acc_df['high_credit_or_sanctioned_amount__c'] = acc_df['high_credit_or_sanctioned_amount__c'].apply(clean_amount).astype('float')
acc_df['credit_limit__c'] = acc_df['credit_limit__c'].apply(clean_amount).astype('float')

acc_df['utilization_history'] = acc_df.apply(
                                        lambda x: (
                                            [val / x['credit_limit__c'] for val in x['CUR BAL - HIST_mod_padded']]
                                            if x['product_2'] == 'cc' and pd.notnull(x['credit_limit__c']) and x['credit_limit__c'] != 0
                                            else
                                            [val / x['high_credit_or_sanctioned_amount__c'] for val in x['CUR BAL - HIST_mod_padded']]
                                            if pd.notnull(x['high_credit_or_sanctioned_amount__c']) and x['high_credit_or_sanctioned_amount__c'] != 0
                                            else
                                            [val/np.nanmax(x['CUR BAL - HIST_mod_padded']) for val in x['CUR BAL - HIST_mod_padded']]
                                        ),
                                        axis=1
                                    )

acc_df['active'] = np.where(acc_df.date_closed__c.isna(), 1, 0)


# ## Utilization based feats

# In[ ]:


def aggregate_customer_level_features(
                                        df,
                                        key_col='multibureau__c',
                                        product_col='product_2',
                                        value_col='utilization_history',
                                        timelines=[1, 3, 6, 'ever'],
                                        horizontal_aggs=['nanmin', 'nanmax', 'nanmean', 'nanmedian', 'nanstd'],
                                        vertical_aggs=['nanmin', 'nanmax', 'nanmean', 'nanmedian', 'nanstd'],
                                        product_filter=['cc', 'pl', 'bl', 'cl', 'gl', 'hl', 'mf', 'al'],
                                        account_status_filter=['overall']
                                    ):
    """
    Performs horizontal (row-level) and vertical (group-level) aggregations on list-type values grouped by customer.
    Aggregation functions like 'nanmin', 'nanmax' etc. are supported via numpy.
    """
    final_df = df[[key_col]].drop_duplicates().reset_index(drop=True)

    # Mapping for 'active' status
    status_map = {
        'active': 1,
        'inactive': 0,
        'overall': None  # No filtering
    }

    for status in account_status_filter:
        if status not in status_map:
            continue

        status_suffix = f"_{status}" if status != 'overall' else ""
        status_val = status_map[status]

        # Filter by account status
        status_df = df[df['active'] == status_val].copy() if status_val is not None else df.copy()

        for timeline in timelines:
            for product in product_filter:
                product_df = status_df[status_df[product_col] == product].copy()

                # Time-based subsetting
                if timeline == 'ever':
                    subset_col = f'{value_col}_ever'
                    product_df[subset_col] = product_df[value_col].apply(
                        lambda x: [v for v in x] if isinstance(x, list) else [] # need to check
                    )
                    timeline_suffix = 'ever'
                else:
                    subset_col = f'{value_col}_{timeline}m'
                    product_df[subset_col] = product_df[value_col].apply(
                        lambda x: [v for v in x[:timeline]] if isinstance(x, list) else []
                    )
                    timeline_suffix = f'{timeline}m'

                # Horizontal (row-level) aggregation
                for h_agg in horizontal_aggs:
                    stat_col = f'{product}_util_h_{h_agg}_{timeline_suffix}{status_suffix}'
                    agg_func = getattr(np, h_agg, None)
                    if agg_func is not None:
                        product_df[stat_col] = product_df[subset_col].apply(
                            lambda x: agg_func(x) if len(x) > 0 else np.nan
                        )
                    else:
                        product_df[stat_col] = np.nan  # fallback in case of unknown agg

                # Vertical aggregation per customer
                agg_cols = [f'{product}_util_h_{h_agg}_{timeline_suffix}{status_suffix}' for h_agg in horizontal_aggs]
                grouped = product_df[[key_col] + agg_cols].groupby(key_col).agg(
                    {
                        col: [getattr(np, v_agg, np.nan) for v_agg in vertical_aggs]
                        for col in agg_cols
                    }
                )

                # Flatten column names with v_ prefix
                grouped.columns = [
                    f"{col}_v_{v_agg}" for col, v_agg in grouped.columns
                ]
                grouped = grouped.reset_index()

                # Merge with the final result
                final_df = final_df.merge(grouped, on=key_col, how='left')

    return final_df


# In[ ]:


aggregate_customer_level_feats = pd.DataFrame()
acc_ids_ls = acc_df['multibureau__c'].unique().tolist()
for id_subset in split(acc_ids_ls, 10000):
    temp_aggregate_customer_level_feats = aggregate_customer_level_features(
                                            acc_df[acc_df.multibureau__c.isin(id_subset)][['multibureau__c','product_2','utilization_history','active']],
                                            key_col='multibureau__c',
                                            product_col='product_2',
                                            value_col='utilization_history',
                                            timelines=[3, 6, 9, 12,15, 'ever'],
                                            horizontal_aggs=['nanmin', 'nanmax', 'nanmean', 'nanmedian', 'nanstd'],
                                            vertical_aggs=['nanmin', 'nanmax', 'nanmean', 'nanmedian', 'nanstd'],
                                            product_filter=['cc', 'pl', 'bl', 'cl', 'gl', 'hl', 'mf', 'al'],
                                            account_status_filter=['active','overall','inactive']
                                        )
    aggregate_customer_level_feats = pd.concat([aggregate_customer_level_feats, temp_aggregate_customer_level_feats], ignore_index=True)


# In[ ]:


print("aggregate_customer_level_feats.shape ->", aggregate_customer_level_feats.shape)


# In[ ]:


print("aggregate_customer_level_feats['multibureau__c'].nunique() ->", aggregate_customer_level_feats['multibureau__c'].nunique())


# In[ ]:


aggregate_customer_level_feats.to_csv('../data/features/aggregate_customer_level_features_dev.csv', index=False)


# In[ ]:


aggregate_customer_level_feats.shape#7000


# In[ ]:


del aggregate_customer_level_feats


# In[ ]:


aggregate_customer_level_feats_sec_unsec = pd.DataFrame()
acc_ids_ls = acc_df['multibureau__c'].unique().tolist()
for id_subset in split(acc_ids_ls, 10000):
    temp_aggregate_customer_level_feats_sec_unsec = aggregate_customer_level_features(
                                            acc_df[acc_df.multibureau__c.isin(id_subset)][['multibureau__c','product_sec_unsec','utilization_history','active']],
                                            key_col='multibureau__c',
                                            product_col='product_sec_unsec',
                                            value_col='utilization_history',
                                            timelines=[3, 6, 9, 12,15, 'ever'],
                                            horizontal_aggs=['nanmin', 'nanmax', 'nanmean', 'nanmedian', 'nanstd'],
                                            vertical_aggs=['nanmin', 'nanmax', 'nanmean', 'nanmedian', 'nanstd'],
                                            product_filter=['unsecured', 'secured'],
                                            account_status_filter=['active','overall','inactive']
                                        )
    aggregate_customer_level_feats_sec_unsec = pd.concat([aggregate_customer_level_feats_sec_unsec, temp_aggregate_customer_level_feats_sec_unsec], ignore_index=True)


# In[ ]:


print("aggregate_customer_level_feats_sec_unsec.shape ->", aggregate_customer_level_feats_sec_unsec.shape)


# In[ ]:


aggregate_customer_level_feats_sec_unsec.to_csv('../data/features/aggregate_customer_level_feats_sec_unsec_dev.csv', index=False)


# In[ ]:





# In[ ]:


del  aggregate_customer_level_feats_sec_unsec


# In[ ]:





# ### High Credit

# In[ ]:


# acc_df1['HIGH CRD - HIST_mod'] = acc_df1['HIGH CRD - HIST'].astype(str).apply(
#     lambda x: ('[' + ','.join(['0' if v == '' else v for v in x.strip(',').split(',')]) + ']').replace("nan",'np.nan')
# ).apply(eval)


# In[ ]:


# acc_df1[acc_df1['acct_type__c'].isin(['Overdraft'])][['acct_type__c','HIGH CRD - HIST','HIGH CRD - HIST_mod']].sample(10)


# In[ ]:


# acc_df1['HIGH CRD - HIST_mod_padded'] = np.where(acc_df1['date_closed__c'].notnull() , acc_df1['CB_HIST_padding'] +  acc_df1['HIGH CRD - HIST_mod'] ,acc_df1['HIGH CRD - HIST_mod'])


# In[ ]:


def months_since_last_increase(x):
    for i in range(len(x) - 1, 0, -1):
        if x[i] > x[i - 1]:
            return len(x) - i - 1
    return np.nan if len(x) < 2 else len(x) - 1


# In[ ]:


import pandas as pd
import numpy as np

def high_credit_features(
                                    df,
                                    key_col='multibureau__c',
                                    product_col='product',
                                    value_col='HIGH CRD - HIST_mod_padded',
                                    timelines=[1, 3, 6, 9, 12, 'ever'],
                                    product_filter=['cc', 'pl', 'cl', 'od', 'hl', 'sec', 'unsec'],
                                    account_status_filter=['overall'],
                                    vertical_aggs=['nanmean']
                                ):
    final_df = df[[key_col]].drop_duplicates().reset_index(drop=True)

    status_map = {
        'active': 1,
        'inactive': 0,
        'overall': None
    }

    for status in account_status_filter:
        if status not in status_map:
            continue

        status_val = status_map[status]
        status_suffix = f"_{status}" if status != 'overall' else ""

        status_df = df[df['active'] == status_val].copy() if status_val is not None else df.copy()

        for product in product_filter:
            product_df = status_df[status_df[product_col] == product].copy()

            for timeline in timelines:
                if timeline == 'ever':
                    subset_col = f'{value_col}_ever'
                    product_df[subset_col] = product_df[value_col].apply(
                        lambda x: [v for v in x] if isinstance(x, list) else []
                    )
                    suffix = 'ever'
                else:
                    subset_col = f'{value_col}_{timeline}m'
                    product_df[subset_col] = product_df[value_col].apply(
                        lambda x: [v for v in x[:timeline]] if isinstance(x, list) else []
                    )
                    suffix = f'{timeline}m'

                # Feature 1: Percentage increase in High Credit
                product_df[f'{product}_pct_inc_high_cred_{suffix}_{status_suffix}'] = product_df[subset_col].apply(
                    lambda x: ((x[0] - x[-1]) / x[-1]) * 100 if len(x) >= 2 and x[-1] != 0 else np.nan
                )

                # Feature 2: % of instances of increase
                product_df[f'{product}_pct_inst_inc_high_cred_{suffix}_{status_suffix}'] = product_df[subset_col].apply(
                    lambda lst: np.nanmean([0] if len(lst) == 1 else [1 if lst[i] > lst[i+1] else 0 for i in range(len(lst)-1)]) # + [0])
                )

                # Feature 3: Months since last increase
                product_df[f'{product}_months_since_last_inc_{suffix}{status_suffix}'] = product_df[subset_col].apply(
                    lambda lst: ([0] if len(lst) == 1 else [1 if lst[i] > lst[i+1] else 0 for i in range(len(lst)-1)] )
                ).apply(
                    lambda lst: np.nanmin([np.nan] + [i for i, val in enumerate(lst) if val == 1]) + 1
                )

                # Feature 4: % of same values
                # print(product_df[subset_col])
                product_df[f'{product}_pct_same_high_credit_{suffix}{status_suffix}'] = product_df[subset_col].apply(
                    lambda lst: np.nanmean([np.nan] if len(lst) == 1 else [1 if lst[i] == lst[i+1] else 0 for i in range(len(lst)-1)]) # + [0])
                )

                # Feature 5: Ever increased flag
                product_df[f'{product}_ever_increase_flag_{suffix}{status_suffix}'] = product_df[subset_col].apply(
                    lambda lst: np.max([0] if len(lst) == 1 else [1 if lst[i] > lst[i+1] else 0 for i in range(len(lst)-1)] + [0])
                )

            # Select feature columns
            features = [col for col in product_df.columns if any(metric in col for metric in [
                '_pct_inc_high_cred_', '_pct_inst_inc_high_cred_',
                '_months_since_last_inc_', '_pct_same_high_credit_', '_ever_increase_flag_'
            ]) and col.endswith(status_suffix)]

            # Vertical aggregation
            # print(product_df[[key_col] + features])
            grouped = product_df[[key_col] + features].groupby(key_col).agg(
                {col: [getattr(np, agg) for agg in vertical_aggs] for col in features}
            )
            grouped.columns = [f"{col}_v_{agg}" for col, agg in grouped.columns]
            grouped = grouped.reset_index()

            # Merge to final
            # print(product_df['product'].unique())
            final_df = final_df.merge(grouped, on=key_col, how='left')

    return final_df


# In[ ]:


# acc_df1[acc_df1['multibureau__c'] == 'DMI 250618CR6410027593042273'][['active','product']]


# In[ ]:


high_cred_feats = pd.DataFrame()
acc_ids_ls = acc_df['multibureau__c'].unique().tolist()
for id_subset in split(acc_ids_ls, 10000):
    temp_high_cred_feats = high_credit_features(
                                    acc_df[acc_df.multibureau__c.isin(id_subset)][['multibureau__c','product_2','HIGH CRD - HIST_mod_padded','active']],
                                    key_col='multibureau__c',
                                    product_col='product_2',
                                    value_col='HIGH CRD - HIST_mod_padded',
                                    timelines=[ 3, 6, 9, 12, 15, 'ever'],
                                    product_filter=['cc'],
                                    account_status_filter=['active', 'inactive', 'overall'],
                                    vertical_aggs=['nanmin', 'nanmax', 'nanmean', 'nanmedian', 'nanstd'],
                                )
    high_cred_feats = pd.concat([high_cred_feats, temp_high_cred_feats], ignore_index=True)


# In[ ]:


print("high_cred_feats.head() ->", high_cred_feats.head())


# In[ ]:


print("high_cred_feats['multibureau__c'].nunique() ->", high_cred_feats['multibureau__c'].nunique())


# In[ ]:


print("high_cred_feats.shape ->", high_cred_feats.shape)


# In[ ]:


high_cred_feats.to_csv('../data/features/high_cred_feats_dev.csv', index=False)


# In[ ]:


del high_cred_feats


# ### Time based

# In[ ]:


def extract_utilization_trend_features(
                                        df,
                                        key_col='multibureau__c',
                                        product_col='product',
                                        value_col='utilization_history',
                                        timelines=[3, 6, 9, 12, 'ever'],
                                        product_filter=['cc', 'pl', 'bl', 'hl', 'al', ],
                                        account_status_filter=['overall'],
                                        vertical_aggs=['nanmean'],
                                        increasing_corr_thresh=0.5,
                                        decreasing_corr_thresh=-0.5,
                                        zero_usage_thresh=0.01
                                    ):
    final_df = df[[key_col]].drop_duplicates().reset_index(drop=True)

    status_map = {
        'active': 1,
        'inactive': 0,
        'overall': None
    }

    for status in account_status_filter:
        if status not in status_map:
            continue

        status_val = status_map[status]
        status_suffix = f"_{status}" if status != 'overall' else ""

        status_df = df[df['active'] == status_val].copy() if status_val is not None else df.copy()

        for product in product_filter:
            product_df = status_df[status_df[product_col] == product].copy()

            for timeline in timelines:
                if timeline == 'ever':
                    subset_col = f'{value_col}_ever'
                    product_df[subset_col] = product_df[value_col].apply(
                        lambda x: [v for v in x] if isinstance(x, list) else []
                    )
                    suffix = 'ever'
                else:
                    subset_col = f'{value_col}_{timeline}m'
                    product_df[subset_col] = product_df[value_col].apply(
                        lambda x: [v for v in x[:timeline]] if isinstance(x, list) else []
                    )
                    suffix = f'{timeline}m'

                # 1. Correlation-based Trend Features
                def get_corr(x):
                    if len(x) >= 2:
                        idx = np.arange(len(x)-1, -1, -1)
                        return np.corrcoef(idx, x)[0, 1] if not np.isnan(np.corrcoef(idx, x)[0, 1]) else np.nan
                    return np.nan

                product_df[f'{product}_util_corr_{suffix}{status_suffix}'] = product_df[subset_col].apply(get_corr)

                # 2. Increasing Trend Flag
                product_df[f'{product}_increasing_flag_{suffix}{status_suffix}'] = product_df[f'{product}_util_corr_{suffix}{status_suffix}'].apply(
                    lambda x: 1 if x is not None and x > increasing_corr_thresh else 0
                )

                # 3. Decreasing Trend Flag
                product_df[f'{product}_decreasing_flag_{suffix}{status_suffix}'] = product_df[f'{product}_util_corr_{suffix}{status_suffix}'].apply(
                    lambda x: 1 if x is not None and x < decreasing_corr_thresh else 0
                )

                # 4. Time Since Max Usage
                product_df[f'{product}_months_since_max_util_{suffix}{status_suffix}'] = product_df[subset_col].apply(
    lambda x: np.nanargmax(x) + 1 if isinstance(x, list) and len(x) > 0 and not all(pd.isna(x)) else np.nan
            )

                # 5. Zero Usage Duration
                product_df[f'{product}_pct_zero_util_{suffix}{status_suffix}'] = product_df[subset_col].apply(
                    lambda x: np.mean([1 if val < zero_usage_thresh else 0 for val in x]) if len(x) > 0 else np.nan
                )

            # Collect all features for aggregation
            features = [col for col in product_df.columns if any(key in col for key in [
                '_util_corr_', '_increasing_flag_', '_decreasing_flag_', '_months_since_max_util_', '_pct_zero_util_']) and col.endswith(status_suffix)]

            # Vertical Aggregation
            grouped = product_df[[key_col] + features].groupby(key_col).agg(
                {col: [getattr(np, agg) for agg in vertical_aggs] for col in features}
            )
            grouped.columns = [f"{col}_v_{agg}" for col, agg in grouped.columns]
            grouped = grouped.reset_index()

            final_df = final_df.merge(grouped, on=key_col, how='left')

    return final_df


# In[ ]:


extract_utilization_trend_feats = pd.DataFrame()
acc_ids_ls = acc_df['multibureau__c'].unique().tolist()
for id_subset in split(acc_ids_ls, 10000):
    temp_extract_utilization_trend_feats = extract_utilization_trend_features(
                                        acc_df[acc_df.multibureau__c.isin(id_subset)][['multibureau__c','product_2','utilization_history','active']],
                                        key_col='multibureau__c',
                                        product_col='product_2',
                                        value_col='utilization_history',
                                        timelines=[3, 6, 9, 12, 15, 'ever'],
                                        product_filter=['cc'],
                                        account_status_filter=['overall', 'active', 'inactive'],
                                        vertical_aggs=['nanmean'],
                                        increasing_corr_thresh=0.5,
                                        decreasing_corr_thresh=-0.5,
                                        zero_usage_thresh=0.01
                                    )
    extract_utilization_trend_feats = pd.concat([extract_utilization_trend_feats, temp_extract_utilization_trend_feats], ignore_index=True)


# In[ ]:


print("extract_utilization_trend_feats.shape ->", extract_utilization_trend_feats.shape)


# In[ ]:


print("extract_utilization_trend_feats['multibureau__c'].nunique() ->", extract_utilization_trend_feats['multibureau__c'].nunique())


# In[ ]:


extract_utilization_trend_feats.to_csv('../data/features/extract_utilization_trend_feats_dev.csv', index=False)


# In[ ]:


del extract_utilization_trend_feats


# In[ ]:





# ### Score

# In[ ]:


score = pd.read_csv(os.path.join(SCRUB_DIR, 'Score_Trends.csv'), sep='|', index_col=False)
print("score (Score_Trends) shape:", score.shape)


# In[ ]:


print("score.head():\n", score.head())


# In[ ]:


score_filtered = score[score['LOS-APP-ID'].isin(acc_df['multibureau__c'])]
print("score_filtered shape:", score_filtered.shape)


# In[ ]:


value_cols = [f'VALUE_{i}' for i in range(1, 13)]
score_filtered['score_list'] = score_filtered.apply(lambda row: [row[col] for col in value_cols], axis=1)


# In[ ]:


# Score list doubts
# As the dates are at different timeline, how to add padding for this case



# In[ ]:


print("score_filtered shape (after score_list):", score_filtered.shape)


# In[ ]:


print("score_filtered SCORE-TYPE value_counts:\n", score_filtered['SCORE-TYPE'].value_counts())


# In[ ]:


print("df_pii shape:", df_pii.shape)


# In[ ]:


score1 = score_filtered.merge(df_pii[['lead_id','retro date']].rename(columns={"lead_id":"LOS-APP-ID",'retro date':'createddate'}),on='LOS-APP-ID',how='left')
print("score1 shape:", score1.shape)


# In[ ]:


score1 = score1[~score1['DATE_1'].isna()]


# In[ ]:


score1['createddate'] = pd.to_datetime(score1['createddate'])
score1['DATE_1'] = pd.to_datetime(score1['DATE_1'])


# In[ ]:


score1[['createddate','DATE_1']].head(10)


# In[ ]:


score1['score_list_padding'] = month_difference(score1['createddate'], score1['DATE_1']).apply(lambda x: [np.nan] * (x//3))


# In[ ]:


score1['score_list_padded'] =  score1['score_list'].values


# In[ ]:


score1[['CREDT-REPORT-ID','score_list']].head(10)


# In[ ]:


score1.rename(columns={'LOS-APP-ID':'multibureau__c'}, inplace=True)


# In[ ]:


def pct_change_right_to_left(lst):
    if not isinstance(lst, list) or len(lst) < 2:
        return []

    pct_changes = []
    for i in range(len(lst) - 1, 0, -1):
        curr, prev = lst[i - 1], lst[i]
        if prev in [0, np.nan] or pd.isna(prev):
            pct_changes.append(np.nan)
        else:
            try:
                pct_changes.append(((curr - prev) / prev) * 100)
            except:
                pct_changes.append(np.nan)
    return pct_changes[::-1] + [np.nan]

def score_threshold_jump_flag(lst, threshold=300):
    if not isinstance(lst, list) or len(lst) < 2:
        return 0
    for i in range(len(lst) - 1, 0, -1):
        right = lst[i]
        left = lst[i - 1]
        if pd.notna(right) and pd.notna(left):
            if right < threshold and left >= threshold:
                return 1
    return 0

def aggregate_score_features(
                                df,
                                key_col='CREDT-REPORT-ID',
                                value_col='score_list',
                                timelines=[1, 3, 6, 'ever'],
                                horizontal_aggs=['nanmin', 'nanmax', 'nanmean', 'nanmedian', 'nanstd'],
                                ntc_score_cutoff=300
                            ):
    final_df = df[[key_col]].drop_duplicates().reset_index(drop=True)

    for timeline in timelines:
        product_df = df.copy()

        # Time-based subsetting
        if timeline == 'ever':
            subset_col = f'{value_col}_ever'
            product_df[subset_col] = product_df[value_col].apply(
                lambda x: [v for v in x] if isinstance(x, list) else []
            )
            timeline_suffix = 'ever'
        else:
            subset_col = f'{value_col}_{timeline}quaters'
            product_df[subset_col] = product_df[value_col].apply(
                lambda x: [v for v in x[:timeline]] if isinstance(x, list) else []
            )
            timeline_suffix = f'{timeline}quaters'

        # Percentage change from right to left
        pct_col = f'{value_col}_pct_chg_{timeline_suffix}'
        product_df[pct_col] = product_df[subset_col].apply(pct_change_right_to_left)

        # Horizontal aggregation on percentage change
        for h_agg in horizontal_aggs:
            stat_col = f'score_pctchg_h_{h_agg}_{timeline_suffix}'
            agg_func = getattr(np, h_agg, None)
            if agg_func is not None:
                product_df[stat_col] = product_df[pct_col].apply(
                    lambda x: agg_func(x) if len(x) > 0 else np.nan
                )
            else:
                product_df[stat_col] = np.nan

        # Horizontal aggregation on raw scores
        for h_agg in horizontal_aggs:
            stat_col = f'score_h_{h_agg}_{timeline_suffix}'
            agg_func = getattr(np, h_agg, None)
            if agg_func is not None:
                product_df[stat_col] = product_df[subset_col].apply(
                    lambda x: agg_func(x) if len(x) > 0 else np.nan
                )
            else:
                product_df[stat_col] = np.nan

        # # NTC to ETC flag (no horizontal agg)
        # ntc_flag_col = f'ntc_to_etc_flag_{timeline_suffix}'
        # product_df[ntc_flag_col] = product_df[subset_col].apply(
        #     lambda x: score_threshold_jump_flag(x, threshold=ntc_score_cutoff)
        # )

        features = [col for col in product_df.columns if
                    (col.startswith('score_h_') or col.startswith('score_pctchg_h_')) and col.endswith(timeline_suffix)]
        final_df = final_df.merge(product_df[[key_col] + features], on=key_col, how='left')

    return final_df


# In[ ]:


aggregate_score_feats = aggregate_score_features(
                        score1[['multibureau__c','score_list']],
                        key_col='multibureau__c',
                        value_col='score_list',
                        timelines=[2,3,4,5,6,7,8, 'ever'], # in quaters
                        horizontal_aggs=['nanmin', 'nanmax','nanmean','nanstd','nanmedian'],
                        ntc_score_cutoff=300
                    )


# In[ ]:


print("aggregate_score_feats.head() ->", aggregate_score_feats.head())


# In[ ]:


print("aggregate_score_feats.shape ->", aggregate_score_feats.shape)


# In[ ]:


aggregate_score_feats.to_csv('../data/features/aggregate_score_feats_dev.csv', index=False)


# In[ ]:


del aggregate_score_feats


# ### Amount paid feats

# In[ ]:


def aggregate_payment_features(
                                df,
                                key_col='multibureau__c',
                                amt_paid_col='amt_paid_list',
                                emi_col='emi_amount_parsed',
                                high_credit_col='high_credit_or_sanctioned_amount__c',
                                product_col='product',
                                product_filter=['pl', 'cl', 'cc'],
                                timelines=[1, 3, 6, 'ever'],
                                horizontal_aggs=['nanmean', 'nanmax', 'nanmin', 'nanmedian', 'nanstd'],
                                vertical_aggs=['nanmean', 'nanmax', 'nanmin']
                            ):
    final_df = df[[key_col]].drop_duplicates().reset_index(drop=True)

    for product in product_filter:
        product_df = df[df[product_col] == product].copy()

        for timeline in timelines:
            if timeline == 'ever':
                subset_col = f'{amt_paid_col}_ever'
                product_df[subset_col] = product_df[amt_paid_col].apply(
                    lambda x: [v for v in x if isinstance(x, list)] if isinstance(x, list) else []
                )
                suffix = 'ever'
            else:
                subset_col = f'{amt_paid_col}_{timeline}m'
                product_df[subset_col] = product_df[amt_paid_col].apply(
                    lambda x: [v for v in x[:timeline]] if isinstance(x, list) else []
                )
                suffix = f'{timeline}m'

            # Ratio of amt_paid_list / high_credit_or_sanctioned_amount__c
            ratio_col_1 = f'{product}_amt_to_high_credit_ratio_{suffix}'
            product_df[ratio_col_1] = product_df.apply(
                lambda row: [amt / row[high_credit_col] if pd.notna(row[high_credit_col]) and row[high_credit_col] != 0 else np.nan for amt in row[subset_col]]
                if isinstance(row[subset_col], list) else np.nan, axis=1
            )

            # Ratio of amt_paid_list / emi_amount_parsed (only if emi_amount_parsed is NaN or 0)
            ratio_col_2 = f'{product}_amt_to_emi_ratio_{suffix}'
            product_df[ratio_col_2] = product_df.apply(
                lambda row: [amt / row[emi_col] if pd.notna(row[emi_col]) and row[emi_col] != 0 else np.nan for amt in row[subset_col]]
                if isinstance(row[subset_col], list) and pd.notna(row[emi_col]) and row[emi_col] != 0 else np.nan, axis=1
            )

            # Sum of EMI amount in last n months
            emi_sum_col = f'{product}_emi_sum_{suffix}'
            product_df[emi_sum_col] = product_df[subset_col].apply(
                lambda x: np.nansum(x) if isinstance(x, list) else np.nan
            )

            # Horizontal aggregations
            for ratio_col in [ratio_col_1, ratio_col_2]:
                if ratio_col not in product_df.columns:
                    continue
                for h_agg in horizontal_aggs:
                    agg_func = getattr(np, h_agg, None)
                    hcol = f'{ratio_col}_h_{h_agg}'
                    product_df[hcol] = product_df[ratio_col].apply(
                        lambda x: agg_func(x) if isinstance(x, list) and len(x) > 0 else np.nan
                    )

            # Select only generated scalar columns for vertical aggregation
            # feature_cols = [col for col in product_df.columns if '_h_' in col]
            feature_cols = []

            # Add horizontally aggregated columns from both ratio columns
            for base_col in [ratio_col_1, ratio_col_2]:
                for h_agg in horizontal_aggs:
                    col_name = f"{base_col}_h_{h_agg}"
                    if col_name in product_df.columns:
                        feature_cols.append(col_name)

            # Add EMI sum column (which is a scalar, directly eligible for vertical agg)
            if emi_sum_col in product_df.columns:
                feature_cols.append(emi_sum_col)

            # feature_cols.remove("cc_amt_to_emi_ratio_ever")
            grouped = product_df[[key_col] + feature_cols].groupby(key_col).agg(
                {col: [getattr(np, vagg) for vagg in vertical_aggs] for col in feature_cols}
            )
            grouped.columns = [f"{col}_v_{agg}" for col, agg in grouped.columns]
            grouped = grouped.reset_index()
            final_df = final_df.merge(grouped, on=key_col, how='left')

    return final_df



# In[ ]:


aggregate_payment_feats = pd.DataFrame()
acc_ids_ls = acc_df['multibureau__c'].unique().tolist()
for id_subset in split(acc_ids_ls, 10000):
    temp_aggregate_payment_feats = aggregate_payment_features(
                                acc_df[acc_df.multibureau__c.isin(id_subset)][['multibureau__c','amt_paid_list','emi_amount_parsed','product_2','high_credit_or_sanctioned_amount__c']],
                                key_col='multibureau__c',
                                amt_paid_col='amt_paid_list',
                                emi_col='emi_amount_parsed',
                                high_credit_col='high_credit_or_sanctioned_amount__c',
                                product_col='product_2',
                                product_filter=['cc', 'pl', 'bl', 'cl', 'gl', 'hl', 'mf', 'al'],
                                timelines=[3, 6, 9, 12, 15, 'ever'],
                                horizontal_aggs=['nanmean', 'nanmax', 'nanmin', 'nanmedian', 'nanstd'],
                                vertical_aggs=['nanmean', 'nanmax', 'nanmin']
                            )
    aggregate_payment_feats = pd.concat([aggregate_payment_feats, temp_aggregate_payment_feats], ignore_index=True)


# In[ ]:


print("aggregate_payment_feats.shape ->", aggregate_payment_feats.shape)


# In[ ]:


aggregate_payment_feats.to_csv('../data/features/aggregate_payment_feats_dev.csv', index=False)


# In[ ]:


del aggregate_payment_feats


# In[ ]:


aggregate_payment_feats_sec_unsec = pd.DataFrame()
acc_ids_ls = acc_df['multibureau__c'].unique().tolist()
for id_subset in split(acc_ids_ls, 10000):
    temp_aggregate_payment_feats_sec_unsec = aggregate_payment_features(
                                acc_df[acc_df.multibureau__c.isin(id_subset)][['multibureau__c','amt_paid_list','emi_amount_parsed','product_sec_unsec','high_credit_or_sanctioned_amount__c']],
                                key_col='multibureau__c',
                                amt_paid_col='amt_paid_list',
                                emi_col='emi_amount_parsed',
                                high_credit_col='high_credit_or_sanctioned_amount__c',
                                product_col='product_sec_unsec',
                                product_filter= ['unsecured', 'secured'],
                                timelines=[3, 6, 9, 12, 15, 'ever'],
                                horizontal_aggs=['nanmean', 'nanmax', 'nanmin', 'nanmedian', 'nanstd'],
                                vertical_aggs=['nanmean', 'nanmax', 'nanmin']
                            )
    aggregate_payment_feats_sec_unsec = pd.concat([aggregate_payment_feats_sec_unsec, temp_aggregate_payment_feats_sec_unsec], ignore_index=True)


# In[ ]:


print("aggregate_payment_feats_sec_unsec.shape ->", aggregate_payment_feats_sec_unsec.shape)


# In[ ]:


aggregate_payment_feats_sec_unsec.to_csv('../data/features/aggregate_payment_feats_sec_unsec_dev.csv', index=False)


# In[ ]:


del aggregate_payment_feats_sec_unsec


# ### Volatility based features

# In[ ]:


def generate_util_range_and_cv(
    df,
    key_col='multibureau__c',
    product_col='product',
    value_col='utilization_history',
    timelines=[3, 6, 9, 'ever'],
    account_status_filter=['overall']
):
    final_df = df[[key_col]].drop_duplicates().reset_index(drop=True)
    status_map = {'active': 1, 'inactive': 0, 'overall': None}

    for status in account_status_filter:
        if status not in status_map:
            continue

        status_val = status_map[status]
        status_suffix = f"_{status}" if status != 'overall' else ""
        status_df = df[df['active'] == status_val].copy() if status_val is not None else df.copy()

        for timeline in timelines:
            for product in ['cc']:
                product_df = status_df[status_df[product_col] == product].copy()
                if timeline == 'ever':
                    subset_col = f'{value_col}_ever'
                    product_df[subset_col] = product_df[value_col].apply(lambda x: [v for v in x if pd.notnull(v)] if isinstance(x, list) else [])
                    timeline_suffix = 'ever'
                else:
                    subset_col = f'{value_col}_{timeline}m'
                    product_df[subset_col] = product_df[value_col].apply(lambda x: [v for v in x[:timeline] if pd.notnull(v)] if isinstance(x, list) else [])
                    timeline_suffix = f'{timeline}m'

                product_df[f'{product}_util_h_max_{timeline_suffix}{status_suffix}'] = product_df[subset_col].apply(lambda x: np.nanmax(x) if x else np.nan)
                product_df[f'{product}_util_h_min_{timeline_suffix}{status_suffix}'] = product_df[subset_col].apply(lambda x: np.nanmin(x) if x else np.nan)
                product_df[f'{product}_util_h_range_{timeline_suffix}{status_suffix}'] = (
                    product_df[f'{product}_util_h_max_{timeline_suffix}{status_suffix}'] -
                    product_df[f'{product}_util_h_min_{timeline_suffix}{status_suffix}']
                )
                product_df[f'{product}_util_h_cv_{timeline_suffix}{status_suffix}'] = product_df[subset_col].apply(
                    lambda x: np.nanstd(x) / np.nanmean(x) if len(x) > 0 and np.nanmean(x) > 0 else np.nan
                )

                agg_cols = [f'{product}_util_h_range_{timeline_suffix}{status_suffix}',
                            f'{product}_util_h_cv_{timeline_suffix}{status_suffix}']

                grouped = product_df[[key_col] + agg_cols].groupby(key_col).agg({
                    col: [np.nanmax, np.nanmean, np.nanmedian] for col in agg_cols
                })

                grouped.columns = [f"{col}_v_{agg.replace('nan', '')}" for col, agg in grouped.columns]
                grouped = grouped.reset_index()
                final_df = final_df.merge(grouped, on=key_col, how='left')

    return final_df


# In[ ]:


util_range_and_cv_feats = pd.DataFrame()
acc_ids_ls = acc_df['multibureau__c'].unique().tolist()
for id_subset in split(acc_ids_ls, 10000):
    temp_util_range_and_cv_feats = generate_util_range_and_cv(
                                        acc_df[acc_df.multibureau__c.isin(id_subset)][['multibureau__c','product_2','utilization_history','active']],
                                        key_col='multibureau__c',
                                        product_col='product_2',
                                        value_col='utilization_history',
                                        timelines=[3, 6, 9, 12, 15, 'ever'],
                                        account_status_filter=['active','overall','inactive']
                                    )
    util_range_and_cv_feats = pd.concat([util_range_and_cv_feats, temp_util_range_and_cv_feats], ignore_index=True)


# In[ ]:


print("util_range_and_cv_feats.shape ->", util_range_and_cv_feats.shape)


# In[ ]:


util_range_and_cv_feats.to_csv('../data/features/util_range_and_cv_feats_dev.csv', index=False)


# In[ ]:


import pandas as pd
pd.set_option("display.max_columns",None)
df_check = pd.read_csv("../data/features/util_range_and_cv_feats_dev.csv",nrows=100)
df_check


# In[ ]:


del util_range_and_cv_feats


# In[ ]:


def generate_utilization_flags(
    df,
    key_col='multibureau__c',
    product_col='product',
    value_col='utilization_history',
    product_filter=['cc'],
    timelines=[3, 6, 9, 'ever'],
    over_cutoff=0.9,
    under_cutoff=0.1,
    vertical_aggs=['nanmin', 'nanmax', 'nanmean'],
    account_status_filter=['overall']
):

    final_df = df[[key_col]].drop_duplicates().reset_index(drop=True)

    status_map = {
        'active': 1,
        'inactive': 0,
        'overall': None
    }

    for status in account_status_filter:
        if status not in status_map:
            continue

        status_val = status_map[status]
        status_suffix = f'_{status}' if status != 'overall' else ''

        status_df = df[df['active'] == status_val].copy() if status_val is not None else df.copy()

        for product in product_filter:
            for timeline in timelines:
                product_df = status_df[status_df[product_col] == product].copy()

                if timeline == 'ever':
                    subset_col = f'{value_col}_ever'
                    product_df[subset_col] = product_df[value_col].apply(
                        lambda x: [v for v in x] if isinstance(x, list) else []
                    )
                    time_suffix = 'ever'
                else:
                    subset_col = f'{value_col}_{timeline}m'
                    product_df[subset_col] = product_df[value_col].apply(
                        lambda x: [v for v in x[:timeline]] if isinstance(x, list) else []
                    )
                    time_suffix = f'{timeline}m'

                # Over-utilization flag
                over_flag_col = f'{product}_util_over_{int(over_cutoff * 100)}_h_max_{time_suffix}{status_suffix}'
                product_df[over_flag_col] = product_df[subset_col].apply(
                    lambda x: int(np.any(np.array(x) > over_cutoff)) if isinstance(x, list) else np.nan
                )

                # Under-utilization flag
                under_flag_col = f'{product}_util_under_{int(under_cutoff * 100)}_h_max_{time_suffix}{status_suffix}'
                product_df[under_flag_col] = product_df[subset_col].apply(
                    lambda x: int(len(x) > 0 and np.all(np.array(x) < under_cutoff)) if isinstance(x, list) else np.nan
                )

                for flag_col in [over_flag_col, under_flag_col]:
                    grouped = product_df[[key_col, flag_col]].groupby(key_col).agg({
                        flag_col: [getattr(np, vagg, np.nanmean) for vagg in vertical_aggs]
                    })

                    grouped.columns = [f"{flag_col}_v_{vagg.replace('nan', '')}" for vagg in vertical_aggs]
                    grouped = grouped.reset_index()

                    final_df = final_df.merge(grouped, on=key_col, how='left')

    return final_df


# In[ ]:


util_flags_feats = pd.DataFrame()
acc_ids_ls = acc_df['multibureau__c'].unique().tolist()
for id_subset in split(acc_ids_ls, 10000):
    temp_util_flags_feats = generate_utilization_flags(
                                        acc_df[acc_df.multibureau__c.isin(id_subset)][['multibureau__c','product_2','utilization_history','active']],
                                        key_col='multibureau__c',
                                        product_col='product_2',
                                        value_col='utilization_history',
                                        timelines=[3, 6, 9, 12, 15, 'ever'],
                                        over_cutoff=0.9,
                                        under_cutoff=0.1,
                                        vertical_aggs=['nanmin', 'nanmax', 'nanmean', 'nanmedian', 'nanstd'],
                                        product_filter=['cc'],
                                        account_status_filter=['active','overall','inactive']
                                    )
    util_flags_feats = pd.concat([util_flags_feats, temp_util_flags_feats], ignore_index=True)


# In[ ]:


print("util_flags_feats.shape ->", util_flags_feats.shape)


# In[ ]:


util_flags_feats.to_csv('../data/features/util_flags_feats_dev.csv', index=False)


# In[ ]:


del util_flags_feats


# In[ ]:


def generate_zero_utilization_percentage(
    df,
    key_col='multibureau__c',
    product_col='product',
    value_col='utilization_history',
    timelines=[3, 6, 9, 'ever'],
    account_status_filter=['overall'],
    zero_cutoff=0.005
):
    final_df = df[[key_col]].drop_duplicates().reset_index(drop=True)

    status_map = {
        'active': 1,
        'inactive': 0,
        'overall': None
    }

    for status in account_status_filter:
        if status not in status_map:
            continue

        status_val = status_map[status]
        status_suffix = f'_{status}' if status != 'overall' else ''
        status_df = df[df['active'] == status_val].copy() if status_val is not None else df.copy()

        for timeline in timelines:
            product_df = status_df[status_df[product_col] == 'cc'].copy()

            if timeline == 'ever':
                subset_col = f'{value_col}_ever'
                product_df[subset_col] = product_df[value_col].apply(
                    lambda x: [v for v in x if pd.notnull(v)] if isinstance(x, list) else []
                )
                timeline_suffix = 'ever'
            else:
                subset_col = f'{value_col}_{timeline}m'
                product_df[subset_col] = product_df[value_col].apply(
                    lambda x: [v for v in x[:timeline] if pd.notnull(v)] if isinstance(x, list) else []
                )
                timeline_suffix = f'{timeline}m'

            flag_col = f'cc_util_zero_lt_{int(zero_cutoff*1000)}bp_{timeline_suffix}{status_suffix}'
            product_df[flag_col] = product_df[subset_col].apply(
                lambda x: int(len(x) > 0 and np.all(np.array(x) < zero_cutoff)) if isinstance(x, list) else np.nan
            )

            # Compute mean per customer → gives % of CC accounts with zero util
            grouped = product_df[[key_col, flag_col]].groupby(key_col)[flag_col].mean().reset_index()
            grouped.rename(columns={flag_col: f'{flag_col}_v_mean'}, inplace=True)

            final_df = final_df.merge(grouped, on=key_col, how='left')

    return final_df


# In[ ]:


zero_util_perc_feats = pd.DataFrame()
acc_ids_ls = acc_df['multibureau__c'].unique().tolist()
for id_subset in split(acc_ids_ls, 10000):
    temp_zero_util_perc_feats = generate_zero_utilization_percentage(
                                        acc_df[acc_df.multibureau__c.isin(id_subset)][['multibureau__c','product_2','utilization_history','active']],
                                        key_col='multibureau__c',
                                        product_col='product_2',
                                        value_col='utilization_history',
                                        timelines=[3, 6, 9, 12, 15, 'ever'],
                                        account_status_filter=['active','overall','inactive'],
                                        zero_cutoff=0.005
                                    )
    zero_util_perc_feats = pd.concat([zero_util_perc_feats, temp_zero_util_perc_feats], ignore_index=True)


# In[ ]:


print("zero_util_perc_feats.shape ->", zero_util_perc_feats.shape)


# In[ ]:


zero_util_perc_feats.to_csv('../data/features/zero_util_perc_feats_dev.csv', index=False)


# In[ ]:


del zero_util_perc_feats


# ### Contibuted type feats

# In[ ]:


def aggregate_reported_by_features(
    df,
    key_col='multibureau__c',
    product_col='product',
    sanctioned_amt_col='high_credit_or_sanctioned_amount__c',
    product_filter=['pl', 'cl', 'cc'],
    vertical_aggs=['nanmean', 'nanmax', 'nanmin'],
    account_status_filter=['overall']
):
    final_df = df[[key_col]].drop_duplicates().reset_index(drop=True)

    status_map = {
        'active': 1,
        'inactive': 0,
        'overall': None
    }

    for status in account_status_filter:
        if status not in status_map:
            continue

        status_val = status_map[status]
        status_suffix = f"_{status}" if status != 'overall' else ""

        status_df = df[df['active'] == status_val].copy() if status_val is not None else df.copy()

        for product in product_filter:
            product_df = status_df[status_df[product_col] == product].copy()
            reported_cols = [col for col in df.columns if 'reported_by_' in col]

            # Aggregated stats on reported_by_ cols
            grouped = product_df[[key_col] + reported_cols].groupby(key_col).agg(
                {col: [getattr(np, vagg) for vagg in vertical_aggs] for col in reported_cols}
            )
            grouped.columns = [f"{product}_{col}_v_{agg}{status_suffix}" for col, agg in grouped.columns]
            grouped = grouped.reset_index()
            final_df = final_df.merge(grouped, on=key_col, how='left')

            # Percentage of sanctioned amount where reported_by_* == 1
            for col in reported_cols:
                temp_df = product_df[[key_col, col, sanctioned_amt_col]].copy()
                temp_df['sanctioned_if_1'] = temp_df.apply(lambda x: x[sanctioned_amt_col] if x[col] == 1 else 0, axis=1)
                grouped_pct = temp_df.groupby(key_col).agg(
                    total_sanctioned=(sanctioned_amt_col, 'sum'),
                    sanctioned_when_1=('sanctioned_if_1', 'sum')
                )
                grouped_pct[f'{product}_{col}_pct_sanctioned_if_1{status_suffix}'] = grouped_pct['sanctioned_when_1'] / grouped_pct['total_sanctioned']
                grouped_pct = grouped_pct[[f'{product}_{col}_pct_sanctioned_if_1{status_suffix}']].reset_index()
                final_df = final_df.merge(grouped_pct, on=key_col, how='left')

    return final_df


# In[ ]:


contributor_type_feats = pd.DataFrame()
acc_ids_ls = acc_df['multibureau__c'].unique().tolist()
for id_subset in split(acc_ids_ls, 10000):
    temp_contributor_type_feats = aggregate_reported_by_features(
                                acc_df[acc_df.multibureau__c.isin(id_subset)][['multibureau__c'] + [x for x in acc_df.columns if 'reported_by_' in x] +['product_2','high_credit_or_sanctioned_amount__c']+['active']],
                                key_col='multibureau__c',
                                product_col='product_2',
                                product_filter=['cc', 'pl', 'bl', 'cl', 'gl', 'hl', 'mf', 'al'],
                                vertical_aggs=['nanmean', 'nanmax', 'nanmin'],
                                account_status_filter=['active', 'inactive', 'overall']
                            )
    contributor_type_feats = pd.concat([contributor_type_feats, temp_contributor_type_feats], ignore_index=True)


# In[ ]:


print("contributor_type_feats.shape ->", contributor_type_feats.shape)


# In[ ]:


## converting True False to 1 & 0 for nanmax & nanmin features
boolean_object_columns = contributor_type_feats[contributor_type_feats.columns[(~contributor_type_feats.columns.str.contains('pct_sanctioned')) & (~contributor_type_feats.columns.str.contains('nanmean'))]].columns[1:]#.dtypes.value_counts()
contributor_type_feats[boolean_object_columns] = contributor_type_feats[boolean_object_columns].replace(False, 0).values
contributor_type_feats[boolean_object_columns] = contributor_type_feats[boolean_object_columns].replace(True, 1).values


# In[ ]:


contributor_type_feats.to_csv('../data/features/contributor_type_feats_dev.csv', index=False)


# In[ ]:


del contributor_type_feats


# In[ ]:


contributor_type_feats_sec_unsec = pd.DataFrame()
acc_ids_ls = acc_df['multibureau__c'].unique().tolist()
for id_subset in split(acc_ids_ls, 10000):
    temp_contributor_type_feats_sec_unsec = aggregate_reported_by_features(
                                acc_df[acc_df.multibureau__c.isin(id_subset)][['multibureau__c'] + [x for x in acc_df.columns if 'reported_by_' in x] +['product_sec_unsec','high_credit_or_sanctioned_amount__c']+['active']],
                                key_col='multibureau__c',
                                product_col='product_sec_unsec',
                                product_filter=['unsecured', 'secured'],
                                vertical_aggs=['nanmean', 'nanmax', 'nanmin'],
                                account_status_filter=['active', 'inactive', 'overall']
                            )
    contributor_type_feats_sec_unsec = pd.concat([contributor_type_feats_sec_unsec, temp_contributor_type_feats_sec_unsec], ignore_index=True)


# In[ ]:


print("contributor_type_feats_sec_unsec.shape ->", contributor_type_feats_sec_unsec.shape)


# In[ ]:


contributor_type_feats_sec_unsec.to_csv('../data/features/contributor_type_feats_sec_unsec_dev.csv', index=False)


# In[ ]:


del contributor_type_feats_sec_unsec


# ### Loading and merging data

# In[ ]:


aggregate_customer_level_feats = pd.read_csv('../data/features/aggregate_customer_level_features_dev.csv')
aggregate_customer_level_feats_sec_unsec = pd.read_csv('../data/features/aggregate_customer_level_feats_sec_unsec_dev.csv')
high_cred_feats = pd.read_csv('../data/features/high_cred_feats_dev.csv')


# In[ ]:


acc_df[['multibureau__c']].drop_duplicates()


# In[ ]:


merged_feats = pd.merge( acc_df[['multibureau__c']].drop_duplicates(), aggregate_customer_level_feats, how='outer', on='multibureau__c')
print(merged_feats.shape)
del aggregate_customer_level_feats,acc_df
merged_feats = pd.merge( merged_feats, aggregate_customer_level_feats_sec_unsec, how='outer', on='multibureau__c')
print(merged_feats.shape)
del aggregate_customer_level_feats_sec_unsec
merged_feats = pd.merge( merged_feats, high_cred_feats, how='outer', on='multibureau__c')
print(merged_feats.shape)
del high_cred_feats


# In[ ]:


gc.collect()


# In[ ]:


extract_utilization_trend_feats = pd.read_csv('../data/features/extract_utilization_trend_feats_dev.csv')
aggregate_score_feats = pd.read_csv('../data/features/aggregate_score_feats_dev.csv')
aggregate_payment_feats = pd.read_csv('../data/features/aggregate_payment_feats_dev.csv')
print(extract_utilization_trend_feats.shape,aggregate_score_feats.shape,aggregate_payment_feats.shape)


# In[ ]:


merged_feats = pd.merge( merged_feats, extract_utilization_trend_feats, how='outer', on='multibureau__c')
print(merged_feats.shape)
del extract_utilization_trend_feats
merged_feats = pd.merge( merged_feats, aggregate_score_feats, how='outer', on='multibureau__c')
print(merged_feats.shape)
del aggregate_score_feats
merged_feats = pd.merge( merged_feats, aggregate_payment_feats, how='outer', on='multibureau__c')
print(merged_feats.shape)
del aggregate_payment_feats


# In[ ]:


gc.collect()


# In[ ]:


aggregate_payment_feats_sec_unsec = pd.read_csv('../data/features/aggregate_payment_feats_sec_unsec_dev.csv')
util_range_and_cv_feats = pd.read_csv('../data/features/util_range_and_cv_feats_dev.csv')
util_flags_feats = pd.read_csv('../data/features/util_flags_feats_dev.csv')
print(aggregate_payment_feats_sec_unsec.shape,util_range_and_cv_feats.shape,util_flags_feats.shape)


# In[ ]:


merged_feats = pd.merge( merged_feats, aggregate_payment_feats_sec_unsec, how='outer', on='multibureau__c')
print(merged_feats.shape)
del aggregate_payment_feats_sec_unsec
merged_feats = pd.merge( merged_feats, util_range_and_cv_feats, how='outer', on='multibureau__c')
print(merged_feats.shape)
del util_range_and_cv_feats
merged_feats = pd.merge( merged_feats, util_flags_feats, how='outer', on='multibureau__c')
print(merged_feats.shape)
del util_flags_feats


# In[ ]:


gc.collect()


# In[ ]:


zero_util_perc_feats = pd.read_csv('../data/features/zero_util_perc_feats_dev.csv')
contributor_type_feats = pd.read_csv('../data/features/contributor_type_feats_dev.csv')
contributor_type_feats_sec_unsec = pd.read_csv('../data/features/contributor_type_feats_sec_unsec_dev.csv')


# In[ ]:


print(zero_util_perc_feats.shape,contributor_type_feats.shape,contributor_type_feats_sec_unsec.shape)


# In[ ]:


merged_feats = pd.merge( merged_feats, zero_util_perc_feats, how='outer', on='multibureau__c')
print(merged_feats.shape)
merged_feats = pd.merge( merged_feats, contributor_type_feats, how='outer', on='multibureau__c')
print(merged_feats.shape)
merged_feats = pd.merge( merged_feats, contributor_type_feats_sec_unsec, how='outer', on='multibureau__c')
print(merged_feats.shape)


# In[ ]:


# merged_feats = pd.merge( merged_feats, zero_util_perc_feats, how='outer', on='multibureau__c')
# print(merged_feats.shape)
# merged_feats = pd.merge( merged_feats, contributor_type_feats, how='outer', on='multibureau__c')
# print(merged_feats.shape)
# merged_feats = pd.merge( merged_feats, contributor_type_feats_sec_unsec, how='outer', on='multibureau__c')
# print(merged_feats.shape)


# In[ ]:


del zero_util_perc_feats,contributor_type_feats,contributor_type_feats_sec_unsec


# # merging all features

# In[ ]:


import pandas as pd


# In[ ]:


merged_feats.to_parquet("../data/features/merged_feats.parquet", index=False)


# In[ ]:


merged_feats.to_csv("../data/features/merged_feats.csv", index=False)


# In[ ]:


print("merged_feats.shape ->", merged_feats.shape)


# In[ ]:





# In[ ]:


merged_feats = pd.read_parquet("../data/features/merged_feats.parquet")
print("merged_feats.head() ->", merged_feats.head())


# In[ ]:


print("merged_feats.shape ->", merged_feats.shape)


# In[ ]:


print("merged_feats.shape ->", merged_feats.shape)


# In[ ]:


# opened_acc_feats = pd.read_csv("../data/features/opened_acc_feats.csv")
# enquiry_feats = pd.read_csv("../data/features/enquiry_feats.csv")
# dpd_feats = pd.read_csv("../data/features/dpd_feats.csv")
# ownership_feats = pd.read_csv("../data/features/ownership_feats.csv")
# cc_utilization_feats = pd.read_csv("../data/features/cc_utilization_feats.csv")
# overdue_feats = pd.read_csv("../data/features/overdue_feats.csv")
# acc_to_enq_ratio_feats = pd.read_csv("../data/features/acc_to_enq_ratio_feats.csv")
# overall_acc_feats = pd.read_csv("../data/features/overall_acc_feats.csv")
# written_off_feats = pd.read_csv("../data/features/written_off_feats.csv")
# live_acc_feats = pd.read_csv("../data/features/live_acc_feats.csv")
# closed_acc_feats = pd.read_csv("../data/features/closed_acc_feats.csv")


# In[ ]:


opened_acc_feats = pd.read_csv("./opened_acc_feats.csv")
enquiry_feats = pd.read_csv("./enquiry_feats.csv")
dpd_feats = pd.read_csv("../data/features/dpd_feats.csv")
ownership_feats = pd.read_csv("../data/features/ownership_feats.csv")
cc_utilization_feats = pd.read_csv("./cc_utilization_feats.csv")
overdue_feats = pd.read_csv("./overdue_feats.csv")
acc_to_enq_ratio_feats = pd.read_csv("../data/features/acc_to_enq_ratio_feats.csv")
overall_acc_feats = pd.read_csv("./overall_acc_feats.csv")
written_off_feats = pd.read_csv("../data/features/written_off_feats.csv")
live_acc_feats = pd.read_csv("./live_acc_feats.csv")
closed_acc_feats = pd.read_csv("./closed_acc_feats.csv")


# In[ ]:


# enquiry_feats = pd.read_csv("enquiry_feats.csv")
# dpd_feats = pd.read_csv("dpd_feats.csv")
# ownership_feats = pd.read_csv("ownership_feats.csv")
# cc_utilization_feats = pd.read_csv("cc_utilization_feats.csv")
# overdue_feats = pd.read_csv("overdue_feats.csv")
# acc_to_enq_ratio_feats = pd.read_csv("acc_to_enq_ratio_feats.csv")
# overall_acc_feats = pd.read_csv("overall_acc_feats.csv")
# written_off_feats = pd.read_csv("written_off_feats.csv")
# live_acc_feats = pd.read_csv("live_acc_feats.csv")
# closed_acc_feats = pd.read_csv("closed_acc_feats.csv")


# In[ ]:


from functools import reduce

# List of all DataFrames to merge
dfs_to_merge = [merged_feats[['multibureau__c']].drop_duplicates(),
    enquiry_feats,
    overall_acc_feats,
    closed_acc_feats,
    opened_acc_feats,
    live_acc_feats,
    overdue_feats,
    cc_utilization_feats,
    ownership_feats,
    dpd_feats,
    written_off_feats,
    acc_to_enq_ratio_feats
]

# Set 'multibureau__c' as index in all DataFrames
dfs_indexed = [df.set_index('multibureau__c') for df in dfs_to_merge]

# Merge using index-based joins (much faster)
merged_feats_df = reduce(lambda left, right: left.join(right, how='outer'), dfs_indexed)

merged_feats_df.reset_index(inplace=True)


# In[ ]:


print("merged_feats_df.shape ->", merged_feats_df.shape)


# In[ ]:


merged_feats_df.to_parquet("../data/features/merged_feats_df.parquet", index=False)


# In[ ]:


final_feats = pd.merge(merged_feats, merged_feats_df, on = 'multibureau__c', how = 'outer')


# In[ ]:


print("final_feats.shape ->", final_feats.shape)


# In[ ]:


# final_feats.to_csv('Final_features.csv', index=False)


# In[ ]:


final_feats.to_parquet(os.path.join(FINAL_FEATURES_DIR, f"part_{PART}_Final_features.parquet"), index=False)


# In[ ]:


final_feats = pd.read_parquet(os.path.join(FINAL_FEATURES_DIR, f"part_{PART}_Final_features.parquet"))
print("final_feats.head() ->", final_feats.head())


# In[ ]:


print("final_feats.shape ->", final_feats.shape)
# part_1 ==(49992, 13833)


# In[ ]:


final_feats[['multibureau__c','live_secured_acc_count','live_overall_acc_count','live_unsecured_acc_count']]


# In[ ]:


# notebook debug leftover (one-off single-ID inspection, result not used):
# acc[(acc['LOS-APP-ID']=='00QOS00000AZKHX2AP') & (acc['ACCOUNT-STATUS']!='Closed')]


# In[ ]:


# notebook display leftover (bare frame, result not used):
# acc


# In[ ]:





# In[ ]:





# In[ ]:




