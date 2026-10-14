import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
from tqdm import tqdm
import datetime, timedelta
import time 
import os
from io import StringIO
import warnings
warnings.filterwarnings("ignore")




from tqdm import tqdm
import psycopg2
import boto3
import io





db_params = {
    
    'dbname': 'dwh_dmi',
    'user': 'khushi.goyal@dmifinance.in',
    'password': 'KhushiGoyal@123',
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


start_date = '2025-05-21'
end_date = '2026-03-31'

current_month = datetime.datetime.today().strftime('%b_%Y')
print("Current Month:",current_month)
previous_month = (datetime.datetime.today() - relativedelta(months=1)).strftime('%b_%Y')
print("Previous Month:",previous_month)

new_query = f"""select 
                    ps.loan_id as opportunity__c,
                    o.loan_number,
                    o.loantenor_in_month,
                    o.partner_name,
                    o.created_date,
                    ps.ps_id as id_pay_sch,
                    ps.created_date as createddate_paysch ,
                    ps.emi_due_date as date__c,
                    CAST(ps.pmt_rec_date AS VARCHAR) as payment_received_date__c
                FROM 
                    lms.fact_repayment_schedule as ps
                    join lms.dim_loan o on o.loan_id = ps.loan_id
                    where ps.emi_due_date BETWEEN '{start_date}' AND '{end_date}'
                    AND (ps.ps_id not in (select distinct record_id from lms.dim_purge_records where object_name = 'Payment Schedule'))
                    and o.product_type='Personal Loan'
            """
df_opp = pd.read_sql(new_query, conn)

print("df_opp.dtypes : ",df_opp.dtypes)
df_opp['created_date'] = pd.to_datetime(df_opp['created_date'])
df_opp['date__c'] = pd.to_datetime(df_opp['date__c'])


df_opp_2 = df_opp.drop_duplicates(subset = 'opportunity__c',keep = 'first')
print("df_opp_2.shape: ",df_opp_2.shape)

df_opp_2.to_csv('../Data/opp_ids_between_2025-05-21_till_2026-03-21.csv',index= False)


today=datetime.today()
list_1 = [6,8,9,10,12,13,14,15,16,17,18]
print(today)

df_ids = df_opp_2.copy()
opp_unique_ids = df_ids['opportunity__c'].tolist()
print("len of opp_ids: ",len(opp_unique_ids))

def split(list_a, chunk_size):
    for i in tqdm(range(0, len(list_a), chunk_size)):
        yield list_a[i:i + chunk_size]


opp_df=pd.DataFrame()
for id_subset in split(opp_unique_ids,100000):     
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
    opp_df = pd.concat([df_id_temp,opp_df])     

    
opp_df.to_csv('../Data/opp_data_13aug26.csv',index = False)  


payment_df=pd.DataFrame()
for id_subset in split(opp_unique_ids,100000):     
    qry_payment_data = f"""select 
                    ps.ps_id as id_pay_sch,
                    ps.created_date as createddate_paysch ,
                    ps.loan_id as opportunity__c,
                    ps.net_pmt as net_payment__c,
                    ps.emi_due_date as date__c,
                    ps.net_payment as netpayment__c,
                    CAST(ps.pmt_rec_date AS VARCHAR) as payment_received_date__c
                FROM 
                    dwh_dmi.lms.fact_repayment_schedule as ps
                    where loan_id in ({','.join([f"'{x}'" for x in id_subset])})
                    AND (ps.ps_id not in (select distinct record_id from lms.dim_purge_records where object_name = 'Payment Schedule'))
                """
    df_id_temp = pd.read_sql(qry_payment_data, conn)
    payment_df = pd.concat([df_id_temp,payment_df])     

payment_df.to_csv('../Data/payment_data_13aug26.csv',index = False)



## FLAG CREATION 
df_payments = payment_df.copy()
df=opp_df.copy()

print("unique opp_ids in df_payments :", df_payments['opportunity__c'].nunique())
print("unique opp_ids in df_oppp :", df['opp_id'].nunique())


df = df[df['opp_id'].isin(opp_unique_ids)]
df_payments = df_payments[df_payments['opportunity__c'].isin(opp_unique_ids)]
print("opp_df shape after : ", df.shape)
print("pay_df shape after : ", df_payments.shape)


current_date = today.date() 
previous_month_date = current_date.replace(day=20) - relativedelta(months=1)
previous_month_timestamp = pd.Timestamp(previous_month_date)
current_date_timestamp = pd.Timestamp(current_date)

# fill payment_received_date__c null values and create cols list
today_str=today.strftime('%Y-%m-%d')
# to select ids whoes closedate was before previous_month_date
print('previous_month_date',previous_month_date)
# select all-due dates before current_date 
print('current_date_timestamp',current_date_timestamp)

df[df['stagename'].isin(['Canceled', 'Rejected', 'Disbursement Approved', 'Restructured', 'InitialApproach', 'Credit Approved'])]
df['createddate_opp'] = pd.to_datetime(df['createddate_opp'])
df['closedate'] = pd.to_datetime(df['closedate'])
df = df[~df['stagename'].isin(['Canceled', 'Rejected', 'Disbursement Approved', 'Restructured', 'InitialApproach', 'Credit Approved'])]
print("opp_df shape after stagename filter: ", df.shape)

df['maturity_date__c'] = pd.to_datetime(df['maturity_date__c'],format='%Y-%m-%d')
df['actual_maturity_date__c'] = pd.to_datetime(df['actual_maturity_date__c'],format='%Y-%m-%d')
df['closedate'] = pd.to_datetime(df['closedate'],format='%Y-%m-%d')
df = df[df['closedate'] <= previous_month_timestamp]
print('max_opp_close_date',df['closedate'].max())
print('Maximum opportunity closedate should not be more than of previous month 20th ')


df_opps_payments = pd.merge(df_payments, df, left_on = 'opportunity__c', right_on = 'opp_id', how = 'inner')
df_opps_payments['date__c'] = pd.to_datetime(df_opps_payments['date__c'])
df_opps_payments.sort_values(by = ['opp_id', 'date__c'], inplace = True)
df_opps_payments['closedate'] = pd.to_datetime(df_opps_payments['closedate'])
df_opps_payments = df_opps_payments[df_opps_payments['date__c'] < current_date_timestamp]

print('max_date__c',df_opps_payments['date__c'].max())
print('Maximum due date(date__c) should not more than today date')

df_opps_payments['payment_received_date__c'] = pd.to_datetime(df_opps_payments['payment_received_date__c'])
df_opps_payments.sort_values(by = ['opp_id', 'date__c'], inplace = True)
df_opps_payments.reset_index(drop = True, inplace = True)
df_opps_payments['payment_received_date__c'] = np.where((df_opps_payments['stagename'].isin(['Matured', 'Matured Closed'])) & (df_opps_payments['payment_received_date__c'].isnull()),df_opps_payments.actual_maturity_date__c, df_opps_payments.payment_received_date__c)
df_opps_payments['payment_received_date__c'].fillna(pd.to_datetime(today_str), inplace = True)

##fifo start
ps_data_date__c = df_opps_payments.drop(['payment_received_date__c'], axis = 1)
ps_data_prd = df_opps_payments[['opportunity__c','payment_received_date__c']]
#sorting each dataframe
ps_data_prd.sort_values(by = ['opportunity__c','payment_received_date__c'],inplace=True)
ps_data_date__c.sort_values(['opportunity__c','date__c'],inplace=True)
ps_data_prd.reset_index(drop=True, inplace=True)
ps_data_date__c.reset_index(drop=True, inplace=True)
#Concatenatiang 2 dataframes
df_opps_payments = pd.concat([ps_data_date__c,ps_data_prd],ignore_index=False,axis=1)

df_opps_payments_2 = df_opps_payments[['opp_id', 'date__c',
                                       'payment_received_date__c',
                                       'actual_maturity_date__c','loan_tenor_in_month__c','sanction_amount__c']].copy(deep = True)
df_opps_payments_2.reset_index(drop = True, inplace  = True)
df_opps_payments_final = df_opps_payments_2.copy(deep = True)#pd.concat([df_opps_payments_2_og, df_opps_payments_2_offset])
df_opps_payments_final.sort_values(by = ['opp_id', 'date__c'], inplace  = True)

df_opps_payments_final_2 = df_opps_payments_final.copy(deep = True)
df_opps_payments_final_2['delinquency'] = (df_opps_payments_final_2['payment_received_date__c'] - df_opps_payments_final_2['date__c']).dt.days

df_opps_payments_final_w_date = pd.merge(df_opps_payments_final_2, df_opps_payments[['opp_id', 'closedate']].drop_duplicates(), on = 'opp_id', how = 'inner')
df_opps_payments_final_w_date.sort_values(['opp_id','date__c'], ascending=True,inplace=True)
df_opps_payments_final_w_date['payment_number']=df_opps_payments_final_w_date.groupby(['opp_id'])['date__c'].cumcount()+1
df_opps_payments_final_w_date['closedate'] = pd.to_datetime(df_opps_payments_final_w_date['closedate'])
df_opps_payments_final_w_date['closedate'] = pd.to_datetime(df_opps_payments_final_w_date['closedate'],utc=True)
df_opps_payments_final_w_date.to_csv('../Data/df_opps_payments_final_w_date.csv')