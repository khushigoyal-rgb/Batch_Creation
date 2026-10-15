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




####Functions to Consider

##Util Fn
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


# ensure to pass the list of values here in increasing order of enquiry amount

def enq_amt_cuts_dict():
    enq_amt_cuts = {}
    enq_amt_cuts['pl'] = [1, 100, 10000, 15000, 20000, 50000, 100000]
    enq_amt_cuts['hl'] = [500000, 1000000, 3000000, 5000000, 7500000]
    enq_amt_cuts['cl'] = [1, 5000, 10000, 20000, 30000, 50000]
    enq_amt_cuts['bl'] = [1, 50000, 200000, 500000, 1000000]
    enq_amt_cuts['cc'] = [1, 100, 15000, 30000, 50000]

    return enq_amt_cuts

def months_to_days_dict(months_list):
    return {str(m): m * 30 for m in months_list}

# ensure to pass the list of values here in increasing order of loan amount

def loan_amt_cuts_dict():
    loan_amt_cuts = {}
    loan_amt_cuts['pl'] = [10000, 25000, 50000, 100000, 250000, 500000]
    loan_amt_cuts['hl'] = [500000, 1000000, 3000000, 5000000, 7500000]
    loan_amt_cuts['cl'] = [5000, 10000, 20000, 30000, 50000]
    loan_amt_cuts['bl'] = [50000, 250000, 500000, 1000000, 1500000]
    loan_amt_cuts['cc'] = [15000, 50000, 100000, 300000, 500000]


    return loan_amt_cuts

def ownership_mapper_dict():
    ownership_mapper = {}
    ownership_mapper[1] = 'Individual'
    ownership_mapper[2] = "Authorised User"
    ownership_mapper[3] = "Guarantor"
    ownership_mapper[4] = "Joint"
    ownership_mapper[5] = "Deceased"

    return ownership_mapper


def phone_type_map_dict():
    phone_type_map = {0 : 'Not Classified', 1 : 'Mobile', 2 : 'Home', 3 : 'Office'}

    return phone_type_map


##Quality Check
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


## Last N Months Enquiry Features Function
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

## Before N Years Enquiry Features Function    
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


## Ever Loans Features Functions
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





## Ever Closed Features
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





## Last N Months Closed Loans Features Function
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




## Before N Years Closed Loans Features Function
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






## Last N Months Opened Loans Features Function
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





## Before N Years Opened Loans Features Function
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




## Live Loans Features Function
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




## Ratio of Curr Balance w.r.t Total Sanc Amt
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



## Percentage of Last n months with total tradelines
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






## Percentage of Before n years with total tradelines
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







## Percentage of Live tradelines features w.r.t total tradelines
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





## Ratio of Live and Closed Loans
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




## Overdue Features Function
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





## CC Utilization Features Function
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




## Ownership Features Function
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





## DPD Features Functions
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







## Written Off Features Function
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


## Enuiry List and Account List Velocity Features Function
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





## ID List Features Function
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






## Address List Features Function
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






## Phone List Features Function
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



## Employment List Features Function
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



## Enuiry List and Account List Velocity Features Function
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





















































# Enquiry List Data Pull and Feature Creation

    
            