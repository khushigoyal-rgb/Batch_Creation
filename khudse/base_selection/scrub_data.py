"""
Stage 0-D/E/F: pull bureau scrub (Account / IOI / Inquiry / Score_Trends) from Redshift.

Runs AFTER 01_04_sanoj_base_selection.py, which produces:
    Data/final_ids_opp_lead_mapping.csv   (columns: lead_id, final_opp_id, loan_id, parent_loan_id)
    (Data/ lives inside base_selection/; run both scripts from the base_selection/ directory.)

This script:
  1. reads the cohort's final_opp_id list (the "final truth" ids),
  2. looks up each final_opp_id's loan_number from dim_loan,
  3. pulls the 4 scrub tables keyed by loan_number (= los_app_id),
  4. re-keys rows to lead_id and writes pipe-delimited CSVs to ../Output.

final_opp_id is already parent-resolved upstream, so no parent-chasing is done here.
"""

import os
import pandas as pd
import numpy as np
import psycopg2
from tqdm import tqdm


# ---------------------------------------------------------------------------
# Configuration
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
_ms, _yy = COHORT.split("_")

# Input mapping produced by 01_04_sanoj_base_selection.py (Data folder inside base_selection/)
BASE_CSV = "Data/final_ids_opp_lead_mapping.csv"
# Output scrub CSVs go to the top-level Output folder (outside base_selection)
SCRUB_DIR = "../Output"

# scrub_type for this cohort: "Portfolio-Refresh-<Mon>-<yy>"
SCRUB_TYPE = f"Portfolio-Refresh-{_ms[:3].capitalize()}-{_yy}"

# Values that should be treated as NULL when read as strings
_NULL = {"None", "nan", "NaN", "NULL", "null", ""}


# DB (snake_case) column -> scrub column name (exact names incl. leading/trailing spaces the
# feature functions expect). Only the 4 tables the pipeline consumes.
COLUMN_RENAME_MAP = {
    "crif_account": {
        "credt_rpt_id":"CREDT-RPT-ID","los_app_id":"LOS-APP-ID","candidate_id":"CANDIDATE - ID",
        "customer_id_mbr_id":"CUSTOMER ID/MBR ID","branch":"BRANCH","kendra":"KENDRA",
        "self_indicator":"SELF-INDICATOR","match_type":"MATCH-TYPE","acc_num":"ACC-NUM",
        "credit_grantor":"CREDIT-GRANTOR","acct_type":"ACCT-TYPE","contributor_type":"CONTRIBUTOR-TYPE",
        "date_reported":"DATE-REPORTED","ownership_ind":"OWNERSHIP-IND","account_status":"ACCOUNT-STATUS",
        "disbursed_dt":"DISBURSED-DT","close_dt":"CLOSE-DT","last_payment_date":"LAST-PAYMENT-DATE",
        "credit_limit_sanc_amt":"CREDIT-LIMIT/SANC AMT","disbursed_amt_high_credit":" DISBURSED-AMT/HIGH CREDIT",
        "installment_amt":" INSTALLMENT-AMT","current_bal":" CURRENT-BAL","installment_frequency":"INSTALLMENT-FREQUENCY",
        "write_off_date":"WRITE-OFF-DATE","overdue_amt":" OVERDUE-AMT","write_off_amt":" WRITE-OFF-AMT",
        "asset_class":"ASSET_CLASS","account_remarks":" ACCOUNT-REMARKS","linked_accounts":"LINKED-ACCOUNTS",
        "reported_date_hist":"REPORTED DATE - HIST ","dpd_hist":"DPD - HIST","asset_class_hist":"ASSET CLASS - HIST ",
        "high_crd_hist":"HIGH CRD - HIST ","cur_bal_hist":"CUR BAL - HIST ","das_hist":"DAS - HIST ",
        "amt_overdue_hist":"AMT OVERDUE - HIST ","amt_paid_hist":"AMT PAID - HIST ","income":"INCOME ",
        "income_indicator":" INCOME INDICATOR ","tenure":"TENURE ","occupation":" OCCUPATION",
        "institution_category":"INSTITUTION-CATEGORY",
    },
    "crif_ioi": {
        "credt_rpt_id":"CREDT-RPT-ID","los_app_id":"LOS-APP-ID","customer_id_mbr_id":"CUSTOMER ID/MBR ID",
        "credit_grantor":"CREDIT-GRANTOR","inquiry_date":"INQUIRY-DATE","ownership_type":"OWNERSHIP-TYPE",
        "purpose":"PURPOSE","amount":"AMOUNT","remarks":"REMARKS",
    },
    "crif_inquiry": {
        "credt_rpt_id":"CREDT-RPT-ID","los_app_id":"LOS-APP-ID","branch_id":"BRANCH-ID",
        "customer_id_mbr_id":"CUSTOMER ID/MBR ID","applicant_name":"APPLICANT-NAME","aka":"AKA","dob":"DOB",
        "pan":"PAN","passport":"PASSPORT","dl":"DL","rc":"RC","uid":"UID","voters":"VOTERS","other_id":"OTHER-ID",
        "father":"FATHER","spouse":"SPOUSE","mother":"MOTHER","other":"OTHER","phone_1":"PHONE 1","phone_2":"PHONE 2",
        "phone_3":"PHONE 3","email_1":"EMAIL 1","email_2":"EMAIL 2","gender":"GENDER","address_1":"ADDRESS-1",
        "state_1":"STATE-1","address_2":"ADDRESS-2","state_2":"STATE-2","address_3":"ADDRESS-3","state_3":"STATE-3",
        "retro_pr_dt":"RETRO-PR-DT",
    },
    "crif_score_trends": {
        "credt_report_id":"CREDT-REPORT-ID","los_app_id":"LOS-APP-ID","score_type":"SCORE-TYPE",
        **{f"date_{i}":f"DATE_{i}" for i in range(1,13)},
        **{f"value_{i}":f"VALUE_{i}" for i in range(1,13)},
        **{f"description_{i}":f"DESCRIPTION_{i}" for i in range(1,13)},
    },
}

# scrub table -> output scrub filename consumed downstream
SCRUB_TABLES = {"crif_account":"Account.csv","crif_ioi":"IOI.csv",
                "crif_inquiry":"Inquiry.csv","crif_score_trends":"Score_Trends.csv"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def get_redshift_conn(params):
    try:
        return psycopg2.connect(**params)
    except psycopg2.Error as e:
        print("Error: Unable to connect to the database")
        print(e)
        raise SystemExit(1)


def split(list_a, chunk_size):
    for i in tqdm(range(0, len(list_a), chunk_size)):
        yield list_a[i:i + chunk_size]


def _in_clause(id_subset):
    return ",".join([f"'{x}'" for x in id_subset])


# ---------------------------------------------------------------------------
# Stage 0-D/E/F
# ---------------------------------------------------------------------------
def main():
    _targets = [os.path.join(SCRUB_DIR, fn) for fn in SCRUB_TABLES.values()]
    if all(os.path.exists(t) for t in _targets):
        print("Stage 0-D/E/F: scrub CSVs already exist, skipping pull ->", SCRUB_DIR)
        return

    os.makedirs(SCRUB_DIR, exist_ok=True)
    conn = get_redshift_conn(db_params)
    print("SCRUB_TYPE:", SCRUB_TYPE)

    # (1) read cohort final_opp_id list + the lead_id already resolved upstream.
    base = pd.read_csv(BASE_CSV)
    base = base.astype(str)
    for _c in ["final_opp_id", "lead_id"]:
        base[_c] = base[_c].where(~base[_c].isin(_NULL), np.nan)
    base = base.dropna(subset=["final_opp_id"])
    final_opp_ids = base["final_opp_id"].unique().tolist()
    print(f"final_opp_ids from mapping: {len(final_opp_ids):,}")

    # (2) look up loan_number for each final_opp_id from dim_loan.
    chunks = []
    for s in split(final_opp_ids, 100000):
        q = (f"select O.loan_id as final_opp_id, O.loan_number "
             f"from dwh_dmi.lms.dim_loan O where O.loan_id in ({_in_clause(s)})")
        chunks.append(pd.read_sql(q, conn))
    dl = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame(columns=["final_opp_id", "loan_number"])
    dl = dl.astype(str)
    dl["loan_number"] = dl["loan_number"].where(~dl["loan_number"].isin(_NULL), np.nan)

    # (3) build loan_number <-> lead_id mapper (lead_id comes from the upstream mapping).
    mapper = (base[["final_opp_id", "lead_id"]]
              .merge(dl[["final_opp_id", "loan_number"]], on="final_opp_id", how="left")
              [["loan_number", "lead_id"]]
              .dropna(subset=["loan_number", "lead_id"])
              .drop_duplicates())

    # ONE bureau report per LEAD: if a lead resolves from >1 loan_number this cohort, keep one
    # to avoid stacking duplicate tradelines / >1 Inquiry-Score row.
    _multi = int((mapper.groupby("lead_id")["loan_number"].nunique() > 1).sum())
    if _multi:
        print(f"NOTE: {_multi:,} lead(s) resolve from >1 loan_number this cohort -> keeping ONE report per lead (dedup).")
    mapper = mapper.sort_values(["lead_id", "loan_number"]).drop_duplicates("lead_id", keep="last")

    loan_numbers = mapper["loan_number"].dropna().astype(str).unique().tolist()
    print(f"id mapper: {mapper['lead_id'].nunique():,} leads <-> {len(loan_numbers):,} loan_numbers (1 per lead)")
    assert loan_numbers, "No loan_numbers resolved for the cohort — check dim_loan loan_number for final_opp_id."

    # (4) pull each scrub table, rename, re-key to lead_id, write pipe-delimited.
    _lead_by_loan = mapper.set_index("loan_number")["lead_id"]
    for table, out_name in SCRUB_TABLES.items():
        colmap = COLUMN_RENAME_MAP[table]
        db_cols = list(colmap.keys())
        frames = []
        for s in split(loan_numbers, 10000):
            q = (f"select {', '.join(db_cols)} from scrub.{table} "
                 f"where scrub_type = '{SCRUB_TYPE}' and los_app_id in ({_in_clause(s)})")
            frames.append(pd.read_sql(q, conn))
        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=db_cols)
        df = df.rename(columns=colmap)
        # re-key: LOS-APP-ID currently holds loan_number -> map to lead_id
        df["LOS-APP-ID"] = df["LOS-APP-ID"].astype(str).map(_lead_by_loan)
        df = df[df["LOS-APP-ID"].notna()]
        # Inquiry/Score_Trends are 1-row-per-report tables: keep a single report per lead.
        if table in ("crif_inquiry", "crif_score_trends"):
            df = df.drop_duplicates(subset=["LOS-APP-ID"], keep="last")
        out_path = os.path.join(SCRUB_DIR, out_name)
        df.to_csv(out_path, sep="|", index=False)
        print(f"  wrote {out_name}: {df.shape} -> {out_path}")
    conn.close()

    # (5) validation: files exist + lead linkage.
    for t in _targets:
        assert os.path.exists(t), f"missing output scrub file: {t}"
    _inq = pd.read_csv(os.path.join(SCRUB_DIR, "Inquiry.csv"), sep="|", usecols=["LOS-APP-ID", "RETRO-PR-DT"])
    matched = _inq["LOS-APP-ID"].astype(str).str.upper().isin(mapper["lead_id"].astype(str).str.upper()).mean()
    print(f"lead linkage in Inquiry: {matched:.1%} of rows map to cohort leads")
    print("NOTE: bureau vintage guard (Section 7) checks RETRO-PR-DT vs SNAPSHOT_DATE.")
    print("      Quarterly refresh RETRO-PR-DT may be a quarter-end (<= snapshot); relax the guard")
    print("      to '<= SNAPSHOT_DATE' there if you accept nearest-quarter vintage.")


if __name__ == '__main__':
    main()
