#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
from datetime import datetime


# In[2]:


pd.set_option('display.max_columns',None)


# In[3]:


#!ls ./data/


# In[4]:


df_data_jan26_ids = pd.read_csv('./data/dim_case_data_jan26_ids.csv')


# In[5]:


df_data_jan26_ids.head()


# In[6]:


df_data_jan26_ids.tail(10)


# In[7]:


# import pandas as pd
import json

# # 1. Configuration
# THRESHOLD = 10000
# # Facets to include (Subject is excluded per instructions)
# facets = ['qrc', 'case_origin', 'closure_remarks', 'type', 'sub_type', 'case_category']

# def create_anchor_map(df_input, facet_list, threshold_val):
#     anchor_map = {}

#     # We work on a copy of the columns to avoid changing the original df yet
#     temp_df = df_input[facet_list].copy()

#     print(f"Analyzing {len(temp_df)} records to freeze the mapping...")

#     for col in facet_list:
#         # A. Normalization: Merge Miscellaneous1 into Miscellaneous
#         temp_df[col] = temp_df[col].replace('Miscellaneous1', 'Miscellaneous')

#         # B. Check Unique Count
#         unique_vals = temp_df[col].nunique()

#         if unique_vals > 20:
#             # Rule: > 20 unique? Keep only those with >= 10,000 records
#             counts = temp_df[col].value_counts()
#             retained = counts[counts >= threshold_val].index.tolist()

#             # Ensure 'Miscellaneous' is explicitly in the list
#             if "Miscellaneous" not in retained:
#                 retained.append("Miscellaneous")

#             anchor_map[col] = retained
#             print(f"Facet '{col}': {unique_vals} unique found. Retained {len(retained)} categories (Threshold >= 10k).")
#         else:
#             # Rule: <= 20 unique? Keep all original categories
#             retained = temp_df[col].unique().tolist()
#             anchor_map[col] = retained
#             print(f"Facet '{col}': {unique_vals} unique found. Retained all (Count <= 20).")

#     return anchor_map

# # Execute
# FINAL_CATEGORIES_MAP = create_anchor_map(df_data_jan26_ids, facets, THRESHOLD)

# # 2. Review and Save
# print("\n--- FINAL ANCHOR MAP ---")
# for facet, cats in FINAL_CATEGORIES_MAP.items():
#     print(f"{facet}: {cats}")

# with open("categories.json", "w") as f:
#     json.dump(FINAL_CATEGORIES_MAP, f, indent=4)


# In[8]:


with open('categories.json', 'r') as f:
    FINAL_CATEGORIES_MAP = json.load(f)


# In[ ]:





# In[9]:


import pandas as pd
import numpy as np
import gc

def create_preclosure_features_v4(df_input, snapshot_date_str):
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

    windows = { "7d": 7, "30d": 30, "60d": 60, "90d": 90, "180d": 180 }
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


# In[10]:


master_features_final = create_preclosure_features_v4(df_data_jan26_ids, '2025-12-31')


# In[11]:


master_features_final


# In[ ]:


master_features_final.to_csv('./data/updated_jan26_customer_query_features.csv')


# In[ ]:


#vel_type_noc_related_30d_90d
#vel_case_category_miscellaneous_30d_180d
#ratio_case_age_in_days_60d_180d
#vel_type_pre-closure_related_60d_180d
#vel_closure_remarks_closed_with_failure_30d_180d


# In[ ]:


master_features_final['vel_type_noc_related_30d_90d'].describe(percentiles=[.1,.2,.7,.9,.99])


# In[ ]:


master_features_final['ratio_case_age_in_days_60d_180d'].describe(percentiles=[.1,.2,.7,.9,.99])


# In[ ]:


master_features_final['ratio_case_age_in_days_60d_180d'].nunique()


# In[ ]:


#86155

