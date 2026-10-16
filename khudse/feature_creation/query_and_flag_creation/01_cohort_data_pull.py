import pandas as pd
import numpy as np
import datetime
import boto3
from boto3 import Session
import time
from datetime import timedelta
from dateutil.relativedelta import relativedelta
pd.set_option('display.max_columns',None)

# In[3]:


df = pd.read_csv('../../data/df_complete_base_with_partner_filtered.csv')


# In[4]:


df.shape


# In[5]:


df_f = df[df['year_month']=='2026-01']


# In[6]:


df_f.shape


# In[7]:


df_f['partner_name'].value_counts()


# In[8]:


import psycopg2


# In[9]:


db_params = {
    'dbname': 'dwh_dmi',
    'user': 'hemant.agarwal@dmifinance.in',
    'password': 'HemantAgarwal@123',
    'host': 'dmi-prod.cqmjwdcqo0bv.ap-south-1.redshift.amazonaws.com',
    #'host' : '127.0.0.1',
    'port': '5439'
}

# Establish a connection to Redshift
try:
    conn = psycopg2.connect(**db_params)
except psycopg2.Error as e:
    print("Error: Unable to connect to the database")
    print(e)
    exit(1)


# In[10]:


def split(list_a, chunk_size):
    for i in tqdm(range(0, len(list_a), chunk_size)):
        yield list_a[i:i + chunk_size]


# In[11]:


from tqdm import tqdm


# In[12]:


opp_unique_ids = df_f['opp_id'].to_list()
opp_unique_ids[:3], len(opp_unique_ids)


# In[13]:


opp_df=pd.DataFrame()
for id_subset in split(opp_unique_ids,100000):     
    qry_opp_data = f"""SELECT 
        O.loan_id as opp_id,
        O.loan_number,
        O.loan_type
    FROM dwh_dmi.lms.dim_loan O
    WHERE O.loan_id in  ({','.join([f"'{x}'" for x in id_subset])})
    """
    df_id_temp = pd.read_sql(qry_opp_data, conn)
    opp_df = pd.concat([df_id_temp,opp_df])     


# In[14]:


opp_df.to_csv('./data/opp_id_with_loan_number_jan26_ids.csv',index = False)


# In[15]:


df_jan_opps_2 = pd.merge(df_f,opp_df,on = 'opp_id')


# In[16]:


df_jan_opps_2


# In[17]:


df_jan_opps_2.agg(['count','nunique'])


# In[18]:


unique_loan_number = df_jan_opps_2['loan_number'].to_list()


# In[19]:


len(unique_loan_number)


# In[20]:


cases_df=pd.DataFrame()
for id_subset in split(unique_loan_number,100000):     
    qry_case_data = f"""select *
        from lms.dim_cases dc  
        where dc.loan_number in  ({','.join([f"'{x}'" for x in id_subset])})
    """
    df_id_temp = pd.read_sql(qry_case_data, conn)
    cases_df = pd.concat([cases_df,df_id_temp])     


# In[21]:


cases_df.to_csv('./data/dim_case_data_jan26_ids.csv',index = False)


# In[22]:


cases_df


# In[23]:


cases_df['loan_number'].nunique()


# # part_2

# In[24]:


unique_opp_id = df_jan_opps_2['opp_id'].to_list()


# In[25]:


len(unique_opp_id)


# In[26]:


cases_df_2=pd.DataFrame()
for id_subset in split(unique_loan_number,100000):     
    qry_case_data = f"""select *
        from lms.dim_cases dc  
        where dc.associated_loan in  ({','.join([f"'{x}'" for x in id_subset])})
        and dc.loan_number IS NULL
    """
    df_id_temp = pd.read_sql(qry_case_data, conn)
    cases_df_2 = pd.concat([cases_df_2,df_id_temp])     


# In[27]:


cases_df_2


# In[ ]:




