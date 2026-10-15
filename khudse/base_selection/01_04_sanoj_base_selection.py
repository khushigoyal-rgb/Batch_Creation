import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
from tqdm import tqdm
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")

import psycopg2


# ---------------------------------------------------------------------------
# Configuration (the knobs you change per run)
# ---------------------------------------------------------------------------
db_params = {
    'dbname': 'dwh_dmi',
    'user': 'khushi.goyal@dmifinance.in',
    'password': 'KhushiGoyal@123',
    'host': 'dmi-prod.cqmjwdcqo0bv.ap-south-1.redshift.amazonaws.com',
    # 'host': '127.0.0.1',
    'port': '5439',
}

COHORT = "jan_26"
_MON = {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12}

# Stages excluded from the opportunity base
EXCLUDED_STAGES = ['Canceled', 'Rejected', 'Disbursement Approved',
                   'Restructured', 'InitialApproach', 'Credit Approved']

CHUNK_SIZE = 100000


def connect_redshift(params):
    """Open a Redshift connection, exit on failure."""
    try:
        return psycopg2.connect(**params)
    except psycopg2.Error as e:
        print("Error: Unable to connect to the database")
        print(e)
        raise SystemExit(1)


def compute_dates(cohort):
    """Derive all date bounds used throughout the pipeline from the cohort label."""
    ms, yy = cohort.split("_")
    cohort_month = datetime(2000 + int(yy), _MON[ms[:3].lower()], 1)
    dates = {
        'cohort_month': cohort_month,
        'cohort_period': pd.Period(cohort_month.strftime("%Y-%m"), freq='M'),
        # cohort month window (1st -> last day): seeds the eligible loan universe
        'cohort_month_start': cohort_month.strftime("%Y-%m-%d"),
        'cohort_month_end': (cohort_month + relativedelta(months=1) - timedelta(days=1)).strftime("%Y-%m-%d"),
    }
    today = datetime.today()
    dates['today'] = today
    dates['today_str'] = today.strftime('%Y-%m-%d')
    # previous month's 20th -> upper bound for opportunity closedate
    previous_month_date = today.date().replace(day=20) - relativedelta(months=1)
    dates['previous_month_date'] = previous_month_date
    dates['previous_month_str'] = previous_month_date.strftime('%Y-%m-%d')
    return dates


def split(list_a, chunk_size):
    """Yield successive chunks of list_a (with a progress bar)."""
    for i in tqdm(range(0, len(list_a), chunk_size)):
        yield list_a[i:i + chunk_size]


def _ids_in_clause(id_subset):
    return ','.join([f"'{x}'" for x in id_subset])


# ---------------------------------------------------------------------------
# Stage 1: eligible loan universe (seed IDs) + partner info
# ---------------------------------------------------------------------------
def fetch_seed_ids(conn, dates):
    """Loans with at least one EMI due in the cohort month (Personal Loan, non-purged)."""
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
                    where ps.emi_due_date BETWEEN '{dates['cohort_month_start']}' AND '{dates['cohort_month_end']}'
                    AND (ps.ps_id not in (select distinct record_id from lms.dim_purge_records where object_name = 'Payment Schedule'))
                    and o.product_type='Personal Loan'
            """
    df_opp = pd.read_sql(new_query, conn)
    print("df_opp.dtypes : ", df_opp.dtypes)

    df_ids = df_opp.drop_duplicates(subset='opportunity__c', keep='first')
    print("df_opp_2.shape: ", df_ids.shape)
    df_ids.to_csv(
        f"../Data/opp_ids_{dates['cohort_month_start']}_to_{dates['cohort_month_end']}.csv",
        index=False)

    opp_unique_ids = df_ids['opportunity__c'].tolist()
    print("len of opp_ids: ", len(opp_unique_ids))
    return df_ids, opp_unique_ids


# ---------------------------------------------------------------------------
# Stage 2: loan attributes + full payment history for the seed IDs
# ---------------------------------------------------------------------------
def fetch_opp_data(conn, opp_unique_ids, dates):
    """Loan-level attributes for the seed IDs (stage/closedate filtered in SQL)."""
    excluded_stages_sql = ','.join([f"'{s}'" for s in EXCLUDED_STAGES])
    chunks = []
    for id_subset in split(opp_unique_ids, CHUNK_SIZE):
        qry = f"""SELECT
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
    WHERE O.loan_id in  ({_ids_in_clause(id_subset)})
        AND O.stage not in ({excluded_stages_sql})
        AND O.closed_date <= '{dates['previous_month_str']}'
    """
        chunks.append(pd.read_sql(qry, conn))

    opp_df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
    opp_df.to_csv('../Data/opp_data_13aug26.csv', index=False)
    return opp_df


def fetch_payment_data(conn, opp_unique_ids, dates):
    """Full repayment schedule (up to today) for the seed IDs."""
    chunks = []
    for id_subset in split(opp_unique_ids, CHUNK_SIZE):
        qry = f"""select
                    ps.ps_id as id_pay_sch,
                    ps.created_date as createddate_paysch ,
                    ps.loan_id as opportunity__c,
                    ps.net_pmt as net_payment__c,
                    ps.emi_due_date as date__c,
                    ps.net_payment as netpayment__c,
                    CAST(ps.pmt_rec_date AS VARCHAR) as payment_received_date__c
                FROM
                    dwh_dmi.lms.fact_repayment_schedule as ps
                    where loan_id in ({_ids_in_clause(id_subset)})
                    AND ps.emi_due_date < '{dates['today_str']}'
                    AND (ps.ps_id not in (select distinct record_id from lms.dim_purge_records where object_name = 'Payment Schedule'))
                """
        chunks.append(pd.read_sql(qry, conn))

    payment_df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
    payment_df.to_csv('../Data/payment_data_13aug26.csv', index=False)
    return payment_df


# ---------------------------------------------------------------------------
# Stage 3: flag creation (FIFO delinquency, remaining payments)
# ---------------------------------------------------------------------------
def build_flags(opp_df, payment_df, dates):
    """FIFO-match payments to due dates, compute delinquency and remaining payments."""
    df_payments = payment_df.copy()
    df = opp_df.copy()

    print("unique opp_ids in df_payments :", df_payments['opportunity__c'].nunique())
    print("unique opp_ids in df_oppp :", df['opp_id'].nunique())

    # NOTE: stagename exclusion, closedate <= previous month 20th, and
    # emi_due_date < today are applied directly in the SQL queries above.
    print('previous_month_date', dates['previous_month_date'])
    print('current_date_timestamp', pd.Timestamp(dates['today'].date()))

    # Type conversions required for the delinquency / FIFO logic.
    df['createddate_opp'] = pd.to_datetime(df['createddate_opp'])
    df['maturity_date__c'] = pd.to_datetime(df['maturity_date__c'], format='%Y-%m-%d')
    df['actual_maturity_date__c'] = pd.to_datetime(df['actual_maturity_date__c'], format='%Y-%m-%d')
    df['closedate'] = pd.to_datetime(df['closedate'], format='%Y-%m-%d')
    print('max_opp_close_date', df['closedate'].max())
    print('Maximum opportunity closedate should not be more than of previous month 20th ')

    df_opps_payments = pd.merge(df_payments, df, left_on='opportunity__c', right_on='opp_id', how='inner')
    df_opps_payments['date__c'] = pd.to_datetime(df_opps_payments['date__c'])
    df_opps_payments.sort_values(by=['opp_id', 'date__c'], inplace=True)
    df_opps_payments['closedate'] = pd.to_datetime(df_opps_payments['closedate'])

    print('max_date__c', df_opps_payments['date__c'].max())
    print('Maximum due date(date__c) should not more than today date')

    df_opps_payments['payment_received_date__c'] = pd.to_datetime(df_opps_payments['payment_received_date__c'])
    df_opps_payments.sort_values(by=['opp_id', 'date__c'], inplace=True)
    df_opps_payments.reset_index(drop=True, inplace=True)
    df_opps_payments['payment_received_date__c'] = np.where(
        (df_opps_payments['stagename'].isin(['Matured', 'Matured Closed'])) &
        (df_opps_payments['payment_received_date__c'].isnull()),
        df_opps_payments.actual_maturity_date__c, df_opps_payments.payment_received_date__c)
    df_opps_payments['payment_received_date__c'].fillna(pd.to_datetime(dates['today_str']), inplace=True)

    ## fifo start
    ps_data_date__c = df_opps_payments.drop(['payment_received_date__c'], axis=1)
    ps_data_prd = df_opps_payments[['opportunity__c', 'payment_received_date__c']]
    # sorting each dataframe
    ps_data_prd.sort_values(by=['opportunity__c', 'payment_received_date__c'], inplace=True)
    ps_data_date__c.sort_values(['opportunity__c', 'date__c'], inplace=True)
    ps_data_prd.reset_index(drop=True, inplace=True)
    ps_data_date__c.reset_index(drop=True, inplace=True)
    # concatenating 2 dataframes
    df_opps_payments = pd.concat([ps_data_date__c, ps_data_prd], ignore_index=False, axis=1)

    df_opps_payments_2 = df_opps_payments[['opp_id', 'date__c',
                                           'payment_received_date__c',
                                           'actual_maturity_date__c',
                                           'loan_tenor_in_month__c',
                                           'sanction_amount__c']].copy(deep=True)
    df_opps_payments_2.reset_index(drop=True, inplace=True)
    df_opps_payments_final = df_opps_payments_2.copy(deep=True)
    df_opps_payments_final.sort_values(by=['opp_id', 'date__c'], inplace=True)

    df_opps_payments_final_2 = df_opps_payments_final.copy(deep=True)
    df_opps_payments_final_2['delinquency'] = (
        df_opps_payments_final_2['payment_received_date__c'] - df_opps_payments_final_2['date__c']).dt.days

    df_w_date = pd.merge(
        df_opps_payments_final_2,
        df_opps_payments[['opp_id', 'closedate']].drop_duplicates(),
        on='opp_id', how='inner')
    df_w_date.sort_values(['opp_id', 'date__c'], ascending=True, inplace=True)
    df_w_date['payment_number'] = df_w_date.groupby(['opp_id'])['date__c'].cumcount() + 1
    df_w_date['closedate'] = pd.to_datetime(df_w_date['closedate'])
    df_w_date['closedate'] = pd.to_datetime(df_w_date['closedate'], utc=True)
    df_w_date.to_csv('../Data/df_opps_payments_final_w_date.csv')

    print("df_opps_payments_final_w_date.dtypes :", df_w_date.dtypes)
    df_w_date['max_dpd_till_now'] = (
        df_w_date
        .groupby('opp_id')['delinquency']
        .expanding()
        .max()
        .reset_index(level=0, drop=True)
    )
    df_w_date['remaining_payments'] = df_w_date['loan_tenor_in_month__c'] - df_w_date['payment_number']
    df_final_filt = df_w_date[df_w_date['remaining_payments'] >= 2]
    df_final_filt['date__c'] = pd.to_datetime(df_final_filt['date__c'], format='%Y-%m-%d')
    df_final_filt = df_final_filt[df_final_filt['date__c'] > '2024-12-21']
    print("df_final_filt.shape", df_final_filt.shape)
    df_final_filt['year_month'] = df_final_filt['date__c'].dt.to_period('M')
    return df_final_filt.copy()


# ---------------------------------------------------------------------------
# Stage 4: pre-closed flag
# ---------------------------------------------------------------------------
def pre_closed_opps(opp_df, df_final_filt_2):
    df_opps = opp_df.copy()
    df_matured = df_opps[df_opps['stagename'].isin(['Matured Closed'])]  # ,'Matured'
    df_matured_2 = df_matured[['opp_id', 'closedate', 'stagename',
                               'maturity_date__c', 'actual_maturity_date__c', 'loan_tenor_in_month__c']]
    print("df_matured_2.dtypes :", df_matured_2.dtypes)
    df_matured_2['maturity_date__c'] = pd.to_datetime(df_matured_2['maturity_date__c'])
    df_matured_2['actual_maturity_date__c'] = pd.to_datetime(df_matured_2['actual_maturity_date__c'])
    df_matured_2['closedate'] = pd.to_datetime(df_matured_2['closedate'])
    print("df_matured_2.dtypes :", df_matured_2.dtypes)
    df_matured_2['diff_days'] = df_matured_2['maturity_date__c'] - df_matured_2['actual_maturity_date__c']
    df_matured_2['diff_days'] = pd.to_timedelta(df_matured_2['diff_days']).dt.days.fillna(0).astype(int)
    df_pre_closed = df_matured_2[df_matured_2['diff_days'] > 60]
    print("df_pre_closed agg : ", df_pre_closed.agg(['count', 'nunique']))
    df_pre_closed['maturity_year_month'] = df_pre_closed['actual_maturity_date__c'].dt.to_period('M')
    df_final_base = pd.merge(
        df_final_filt_2,
        df_pre_closed[['opp_id', 'diff_days', 'maturity_year_month']],
        left_on=['opp_id', 'year_month'],
        right_on=['opp_id', 'maturity_year_month'],
        how='left')
    df_final_base['actual_maturity_date__c'] = pd.to_datetime(df_final_base['actual_maturity_date__c'])
    df_complete_base = df_final_base[~((df_final_base['date__c'] > df_final_base['actual_maturity_date__c']) &
                                       (df_final_base['maturity_year_month'].isnull()))]
    df_complete_base['pre_closed_flag'] = np.where(df_complete_base['maturity_year_month'].notnull(), 1, 0)
    summary_df = df_complete_base.groupby('year_month').agg(
        total_customers=('pre_closed_flag', 'count'),
        pre_closed_count=('pre_closed_flag', 'sum')
    )
    summary_df['pre_close_rate'] = (summary_df['pre_closed_count'] / summary_df['total_customers']) * 100

    return df_complete_base, summary_df


# ---------------------------------------------------------------------------
# Stage 5: partner-wise distribution
# ---------------------------------------------------------------------------
def partner_wise_distr(df_complete_base, df_ids):
    list_of_partners = ['GooglePay', 'X-Sell', 'DMID2C', 'Samsung', 'AirtelFinance',
                        'SuperMoney', 'Oppo', 'Finnable', 'Airtel', 'Paisabazaar',
                        'MoneyView', 'Billcut', 'Niro']
    df_partner = df_ids.rename(columns={'opportunity__c': 'opp_id'})
    df_complete_base_with_partner = pd.merge(
        df_complete_base, df_partner[['opp_id', 'partner_name']], on='opp_id')
    print("df_complete_base.shape :", df_complete_base.shape)
    df_filtered = df_complete_base_with_partner[
        df_complete_base_with_partner['partner_name'].isin(list_of_partners)]
    print("df_complete_base_with_partner_filtered.shape :", df_filtered.shape)
    print("checking partners counts :", df_filtered['partner_name'].value_counts())
    df_filtered.to_csv('../Data/df_complete_base_with_partner_filtered.csv', index=False)
    print("df_complete_base_with_partner_filtered['pre_closed_flag'].value_counts() :",
          df_filtered['pre_closed_flag'].value_counts())

    return df_filtered


# ---------------------------------------------------------------------------
# Stage 6: cohort final base selection (resolve parents + leads)
# ---------------------------------------------------------------------------
def cohort_final_base_selection(conn, df_complete_base_with_partner_filtered, dates):
    cohort_period = dates['cohort_period']
    df_cohort = df_complete_base_with_partner_filtered[
        df_complete_base_with_partner_filtered['year_month'] == cohort_period]
    print("df_cohort's shape: ", df_cohort.shape)
    list_opp = df_complete_base_with_partner_filtered[
        df_complete_base_with_partner_filtered['year_month'].isin([cohort_period])]['opp_id'].tolist()
    print("list_opp's len : ", len(list_opp))
    unique_list = list(set(list_opp))
    print("unique_list_opp's len : ", len(unique_list))

    paren_chunks = []
    for id_subset in split(unique_list, CHUNK_SIZE):
        qry = f"""select
                            l.loan_id,l.parent_loan_id
                            from  lms.dim_loan l
                            where l.loan_id in ({_ids_in_clause(id_subset)})
                        """
        paren_chunks.append(pd.read_sql(qry, conn))
    dim_paren_opp = pd.concat(paren_chunks, ignore_index=True) if paren_chunks else pd.DataFrame()
    print("dim_paren_opp.shape : ", dim_paren_opp.shape)
    dim_paren_opp['final_opp_id'] = np.where(
        dim_paren_opp['parent_loan_id'].isnull(),
        dim_paren_opp['loan_id'],
        dim_paren_opp['parent_loan_id'])
    parent_opp_list = dim_paren_opp['final_opp_id'].tolist()
    print("parent_opp_list len : ", len(parent_opp_list))

    lead_chunks = []
    for id_subset in split(parent_opp_list, CHUNK_SIZE):
        qry = f"""select dl.lead_id,
                            l.loan_id,l.parent_loan_id
                            from lms.dim_lead dl
                            right join lms.dim_loan l on l.loan_id = dl.loan_id
                            where l.loan_id in ({_ids_in_clause(id_subset)})
                        """
        lead_chunks.append(pd.read_sql(qry, conn))
    dim_lead = pd.concat(lead_chunks, ignore_index=True) if lead_chunks else pd.DataFrame()

    dim_lead.rename(columns={'loan_id': 'final_opp_id'}, inplace=True)
    df_mapping = pd.merge(dim_lead[['lead_id', 'final_opp_id']], dim_paren_opp, on='final_opp_id')
    df_mapping.to_csv('../Data/final_ids_opp_lead_mapping.csv', index=False)

    ## 07_flag
    df_cohort_with_lead = pd.merge(df_mapping, df_cohort, left_on='loan_id', right_on='opp_id')
    print("final shape of df_cohort_with_lead : ", df_cohort_with_lead.shape)

    return df_cohort_with_lead


def main():
    dates = compute_dates(COHORT)
    print("Current Month:", dates['today'].strftime('%b_%Y'))
    print("Previous Month:", (dates['today'] - relativedelta(months=1)).strftime('%b_%Y'))
    print(dates['today'])

    conn = connect_redshift(db_params)

    # Stage 1: seed IDs
    df_ids, opp_unique_ids = fetch_seed_ids(conn, dates)

    # Stage 2: pull loan attributes + payment history
    opp_df = fetch_opp_data(conn, opp_unique_ids, dates)
    payment_df = fetch_payment_data(conn, opp_unique_ids, dates)

    # Stage 3: delinquency / remaining-payments flags
    df_final_filt_2 = build_flags(opp_df, payment_df, dates)

    # Stage 4 -> 5 -> 6 (strict order)
    df_complete_base, summary_df = pre_closed_opps(opp_df, df_final_filt_2)
    df_complete_base_with_partner_filtered = partner_wise_distr(df_complete_base, df_ids)
    df_cohort_with_lead = cohort_final_base_selection(
        conn, df_complete_base_with_partner_filtered, dates)

    return df_cohort_with_lead


if __name__ == '__main__':
    main()
