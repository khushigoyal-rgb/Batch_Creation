import pandas as pd
import numpy as np
from datetime import datetime





df_data_jan26_ids = pd.read_csv('./data/dim_case_data_jan26_ids.csv')


# In[5]:


df_data_jan26_ids.head()


# In[6]:


df_data_jan26_ids.tail(10)


# In[7]:


snapshot_date_str = '2025-12-31'


# In[8]:


snap_date = pd.to_datetime(snapshot_date_str)
max_date = snap_date + pd.Timedelta(days=90)
print(max_date)

df = df_data_jan26_ids.copy()
df['createddate'] = pd.to_datetime(df['createddate'])
df = df[df['createddate'] > snap_date]
df = df[df['createddate'] <= max_date]



# In[9]:


df


# In[10]:


df.columns


# In[11]:


df[['case_category','qrc','type','sub_type','case_status']].drop_duplicates().to_clipboard()


# In[12]:


pd.DataFrame(df['case_category'].value_counts()).head(50)


# In[13]:


df['qrc'].value_counts()


# In[14]:


df['type'].value_counts()
# ['Pre-closure Related','Loan Closure Request']


# In[15]:


df[df['type'].isin(['Pre-closure Related','Loan Closure Request'])]['qrc'].value_counts()


# In[16]:


df[df['type'].isin(['Pre-closure Related','Loan Closure Request'])]['sub_type'].value_counts()


# In[17]:


pd.DataFrame(df['sub_type'].value_counts()).head(50)
['Loan Pre-closure Request','Loan Pre-closure']


# In[18]:


df[df['type'].isin(['Pre-closure Related','Loan Closure Request'])]['associated_loan'].nunique()


# In[19]:


df


# In[20]:


# Define the valid (qrc, type, sub_type, case_status, case_category) combinations
preclosure_combos = [
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

combo_set = set(preclosure_combos)

df['preclosure_flag'] = df[['qrc', 'type', 'sub_type', 'case_status', 'case_category']].apply(
    lambda row: 1 if tuple(row) in combo_set else 0, axis=1
)

df['preclosure_flag'].value_counts()


# In[21]:


df


# In[22]:


df_2 = df[['associated_loan','preclosure_flag']].drop_duplicates().reset_index(drop=True)


# In[23]:


df_2


# In[24]:


df_2[df_2['associated_loan'].notnull()]


# In[25]:


df_2[df_2['associated_loan'].notnull()].to_csv('./data/updated_jan26_customer_query_flag.csv',index = False)


# In[ ]:




