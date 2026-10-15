import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
from tqdm import tqdm
from datetime import datetime, timedelta
import time
import os
from io import StringIO
import warnings
warnings.filterwarnings("ignore")

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
COHORT = "jan_26"
_MON = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}
_ms, _yy = COHORT.split("_")
cohort_month  = datetime(2000 + int(_yy), _MON[_ms[:3].lower()], 1)
COHORT_YEAR_MONTH    = cohort_month.strftime("%Y-%m")

# Cohort month window (1st -> last day of the cohort month) used to seed the
# eligible loan universe: any loan with at least one EMI due in the cohort month.
cohort_month_start = cohort_month.strftime("%Y-%m-%d")
cohort_month_end   = (cohort_month + relativedelta(months=1) - timedelta(days=1)).strftime("%Y-%m-%d")

current_month = datetime.today().strftime('%b_%Y')
print("Current Month:", current_month)
previous_month = (datetime.today() - relativedelta(months=1)).strftime('%b_%Y')
print("Previous Month:", previous_month)

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
                    where ps.emi_due_date BETWEEN '{cohort_month_start}' AND '{cohort_month_end}'
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


today = datetime.today()
list_1 = [6,8,9,10,12,13,14,15,16,17,18]
print(today)

# Date bounds computed up front so they can be pushed down into SQL.
# previous month's 20th -> upper bound for opportunity closedate
previous_month_date = today.date().replace(day=20) - relativedelta(months=1)
previous_month_str = previous_month_date.strftime('%Y-%m-%d')
# today -> upper bound for EMI due dates
today_str = today.strftime('%Y-%m-%d')

# Stages excluded from the opportunity base
EXCLUDED_STAGES = ['Canceled', 'Rejected', 'Disbursement Approved',
                   'Restructured', 'InitialApproach', 'Credit Approved']
_excluded_stages_sql = ','.join([f"'{s}'" for s in EXCLUDED_STAGES])

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
        AND O.stage not in ({_excluded_stages_sql})
        AND O.closed_date <= '{previous_month_str}'
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
                    AND ps.emi_due_date < '{today_str}'
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


# NOTE: stagename exclusion, closedate <= previous month 20th, and
# emi_due_date < today are now applied directly in the SQL queries above.

current_date = today.date()
previous_month_timestamp = pd.Timestamp(previous_month_date)
current_date_timestamp = pd.Timestamp(current_date)

print('previous_month_date', previous_month_date)
print('current_date_timestamp', current_date_timestamp)

# Type conversions still required for the delinquency / FIFO logic below.
df['createddate_opp'] = pd.to_datetime(df['createddate_opp'])
df['maturity_date__c'] = pd.to_datetime(df['maturity_date__c'], format='%Y-%m-%d')
df['actual_maturity_date__c'] = pd.to_datetime(df['actual_maturity_date__c'], format='%Y-%m-%d')
df['closedate'] = pd.to_datetime(df['closedate'], format='%Y-%m-%d')
print('max_opp_close_date', df['closedate'].max())
print('Maximum opportunity closedate should not be more than of previous month 20th ')


df_opps_payments = pd.merge(df_payments, df, left_on = 'opportunity__c', right_on = 'opp_id', how = 'inner')
df_opps_payments['date__c'] = pd.to_datetime(df_opps_payments['date__c'])
df_opps_payments.sort_values(by = ['opp_id', 'date__c'], inplace = True)
df_opps_payments['closedate'] = pd.to_datetime(df_opps_payments['closedate'])

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

df_opps_payments_final_w_date_ = pd.merge(df_opps_payments_final_2, df_opps_payments[['opp_id', 'closedate']].drop_duplicates(), on = 'opp_id', how = 'inner')
df_opps_payments_final_w_date_.sort_values(['opp_id','date__c'], ascending=True,inplace=True)
df_opps_payments_final_w_date_['payment_number']=df_opps_payments_final_w_date_.groupby(['opp_id'])['date__c'].cumcount()+1
df_opps_payments_final_w_date_['closedate'] = pd.to_datetime(df_opps_payments_final_w_date_['closedate'])
df_opps_payments_final_w_date_['closedate'] = pd.to_datetime(df_opps_payments_final_w_date_['closedate'],utc=True)
df_opps_payments_final_w_date_.to_csv('../Data/df_opps_payments_final_w_date.csv')


df_opps_payments_final_w_date = df_opps_payments_final_w_date_.copy()
print("df_opps_payments_final_w_date.dtypes :",df_opps_payments_final_w_date.dtypes)
df_opps_payments_final_w_date['max_dpd_till_now'] = (
    df_opps_payments_final_w_date
    .groupby('opp_id')['delinquency']
    .expanding()
    .max()
    .reset_index(level=0, drop=True)
)
df_opps_payments_final_w_date[df_opps_payments_final_w_date['delinquency']>15]
df_opps_payments_final_w_date['remaining_payments'] = df_opps_payments_final_w_date['loan_tenor_in_month__c']-df_opps_payments_final_w_date['payment_number']
df_final_filt =df_opps_payments_final_w_date[df_opps_payments_final_w_date['remaining_payments']>=2]
df_final_filt['date__c'] = pd.to_datetime(df_final_filt['date__c'],format='%Y-%m-%d')
df_final_filt =df_final_filt[df_final_filt['date__c']>'2024-12-21']
print("df_final_filt.shape",df_final_filt.shape)
df_final_filt['year_month'] = df_final_filt['date__c'].dt.to_period('M')#.dt.strftime('%Y-%m')
df_final_filt[(df_final_filt['max_dpd_till_now']<=15)]
df_final_filt_2 = df_final_filt.copy()





##PRE CLOSED OPPS
def pre_closed_opps():
    df_opps = opp_df.copy()
    df_matured = df_opps[df_opps['stagename'].isin(['Matured Closed'])]#,'Matured'
    df_matured_2 = df_matured[['opp_id','closedate','stagename',
                           'maturity_date__c','actual_maturity_date__c','loan_tenor_in_month__c']]
    print("df_matured_2.dtypes :",df_matured_2.dtypes) 
    df_matured_2['maturity_date__c'] = pd.to_datetime(df_matured_2['maturity_date__c'])
    df_matured_2['actual_maturity_date__c'] = pd.to_datetime(df_matured_2['actual_maturity_date__c'])
    df_matured_2['closedate'] = pd.to_datetime(df_matured_2['closedate'])
    print("df_matured_2.dtypes :",df_matured_2.dtypes) 
    df_matured_2['diff_days'] = df_matured_2['maturity_date__c'] - df_matured_2['actual_maturity_date__c']
    df_matured_2['diff_days'] = pd.to_timedelta(df_matured_2['diff_days']).dt.days.fillna(0).astype(int)
    df_pre_closed = df_matured_2[df_matured_2['diff_days']>60]
    print("df_pre_closed agg : ", df_pre_closed.agg(['count','nunique']))
    df_pre_closed['maturity_year_month'] = df_pre_closed['actual_maturity_date__c'].dt.to_period('M')
    df_final_base = pd.merge(df_final_filt_2,
                         df_pre_closed[['opp_id',
                                        'diff_days',
                                        'maturity_year_month']],left_on= ['opp_id',
                                                                          'year_month'],right_on = ['opp_id',
                                                                                                    'maturity_year_month'],how = 'left')
    df_final_base['actual_maturity_date__c'] = pd.to_datetime(df_final_base['actual_maturity_date__c'])
    # df_final_base[~((df_final_base['date__c']>df_final_base['actual_maturity_date__c']) & 
    #                                (df_final_base['maturity_year_month'].isnull()))]                                                                                                
    # df_final_base[(df_final_base['date__c']>df_final_base['actual_maturity_date__c'])]
    df_complete_base = df_final_base[~((df_final_base['date__c']>df_final_base['actual_maturity_date__c']) & 
                                   (df_final_base['maturity_year_month'].isnull()))]
    df_complete_base['pre_closed_flag'] = np.where(df_complete_base['maturity_year_month'].notnull(),1,0)
    summary_df = df_complete_base.groupby('year_month').agg(
        total_customers=('pre_closed_flag', 'count'),
        pre_closed_count=('pre_closed_flag', 'sum')
    )
    # Calculate the rate
    summary_df['pre_close_rate'] = (summary_df['pre_closed_count'] / summary_df['total_customers']) * 100

    return df_complete_base, summary_df


def partner_wise_distr(df_complete_base):
    list_of_partners = ['GooglePay', 'X-Sell', 'DMID2C', 'Samsung', 'AirtelFinance',
                        'SuperMoney', 'Oppo', 'Finnable', 'Airtel', 'Paisabazaar',
                        'MoneyView', 'Billcut', 'Niro']
    df_ids.rename(columns={'opportunity__c': 'opp_id'}, inplace=True)
    df_complete_base_with_partner = pd.merge(
        df_complete_base, df_ids[['opp_id', 'partner_name']], on='opp_id')
    print("df_complete_base.shape :", df_complete_base.shape)
    df_complete_base_with_partner_filtered = df_complete_base_with_partner[
        df_complete_base_with_partner['partner_name'].isin(list_of_partners)]
    print("df_complete_base_with_partner_filtered.shape :", df_complete_base_with_partner_filtered.shape)
    print("checking partners counts :", df_complete_base_with_partner_filtered['partner_name'].value_counts())
    df_complete_base_with_partner_filtered.to_csv(
        '../Data/df_complete_base_with_partner_filtered.csv', index=False)
    print("df_complete_base_with_partner_filtered['pre_closed_flag'].value_counts() :",
          df_complete_base_with_partner_filtered['pre_closed_flag'].value_counts())

    return df_complete_base_with_partner_filtered


def cohort_final_base_selection(df_complete_base_with_partner_filtered):
    df_cohort = df_complete_base_with_partner_filtered[
        df_complete_base_with_partner_filtered['year_month'] == COHORT_YEAR_MONTH]
    print("df_cohort's shape: ", df_cohort.shape)
    list_opp = df_complete_base_with_partner_filtered[
        df_complete_base_with_partner_filtered['year_month'].isin([COHORT_YEAR_MONTH])]['opp_id'].tolist()
    print("list_opp's len : ", len(list_opp))
    unique_list = list(set(list_opp))
    print("unique_list_opp's len : ", len(unique_list))

    dim_paren_opp = pd.DataFrame()
    for id_subset in split(unique_list, 100000):
        qry_opp_data = f"""select
                            l.loan_id,l.parent_loan_id
                            from  lms.dim_loan l
                            where l.loan_id in ({','.join([f"'{x}'" for x in id_subset])})
                        """
        df_id_temp = pd.read_sql(qry_opp_data, conn)
        dim_paren_opp = pd.concat([dim_paren_opp, df_id_temp])
    print("dim_paren_opp.shape : ", dim_paren_opp.shape)
    dim_paren_opp['final_opp_id'] = np.where(
        dim_paren_opp['parent_loan_id'].isnull(),
        dim_paren_opp['loan_id'],
        dim_paren_opp['parent_loan_id'])
    parent_opp_list = dim_paren_opp['final_opp_id'].tolist()
    print("parent_opp_list len : ", len(parent_opp_list))

    dim_lead = pd.DataFrame()
    for id_subset in split(parent_opp_list, 100000):
        qry_opp_data = f"""select dl.lead_id,
                            l.loan_id,l.parent_loan_id
                            from lms.dim_lead dl
                            right join lms.dim_loan l on l.loan_id = dl.loan_id
                            where l.loan_id in ({','.join([f"'{x}'" for x in id_subset])})
                        """
        df_id_temp = pd.read_sql(qry_opp_data, conn)
        dim_lead = pd.concat([dim_lead, df_id_temp])

    dim_lead.rename(columns={'loan_id': 'final_opp_id'}, inplace=True)
    df_mapping = pd.merge(dim_lead[['lead_id', 'final_opp_id']], dim_paren_opp, on='final_opp_id')
    df_mapping.to_csv('../Data/final_ids_opp_lead_mapping.csv', index=False)

    ##07_flag
    df_cohort_with_lead = pd.merge(df_mapping, df_cohort, left_on='loan_id', right_on='opp_id')
    print("final shape of df_cohort_with_lead : ", df_cohort_with_lead.shape)

    return df_cohort_with_lead


if __name__ == '__main__':
    df_complete_base, summary_df = pre_closed_opps()
    df_complete_base_with_partner_filtered = partner_wise_distr(df_complete_base)
    df_cohort_with_lead = cohort_final_base_selection(df_complete_base_with_partner_filtered)

