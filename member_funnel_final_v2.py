
# FINAL PATCH - COLUMN FIX + ML STABLE

from google.cloud import bigquery
import pandas as pd
import numpy as np

PROJECT_ID = "columbia-ga4"

def load_ml_scores():
    client = bigquery.Client(project=PROJECT_ID)

    sql = '''
    SELECT
      CAST(COALESCE(member_id, '') AS STRING) AS member_id_norm,
      CAST(COALESCE(user_id, '') AS STRING) AS user_id_norm,

      CAST(COALESCE(crm_action_type, '') AS STRING) AS ml_action_type,
      CAST(COALESCE(priority_tier, '') AS STRING) AS ml_priority_tier,

      SAFE_CAST(COALESCE(repurchase_score, repurchase_45d_score, repurchase_30d_score) AS FLOAT64) AS repurchase_30d_score,
      SAFE_CAST(COALESCE(first_purchase_score, first_purchase_45d_score, first_purchase_30d_score) AS FLOAT64) AS first_purchase_30d_score,
      SAFE_CAST(COALESCE(churn_risk_score, churn_90d_score, churn_60d_score) AS FLOAT64) AS churn_60d_score,
      SAFE_CAST(ltv_score AS FLOAT64) AS ltv_score,

      CAST(COALESCE(next_best_category, '') AS STRING) AS next_best_category

    FROM `columbia-ga4.crm_mart.crm_member_totalview_scores`
    '''

    print("[DEBUG] load_ml_scores start")
    df = client.query(sql).to_dataframe()
    print(f"[DEBUG] ML rows loaded={len(df)}")

    return df


def merge_ml_scores(user_df, ml_df):
    if ml_df is None or len(ml_df) == 0:
        print("[ERROR] ML EMPTY")
        return user_df

    user_df["member_id_norm"] = user_df["member_id_norm"].astype(str).str.strip()
    ml_df["member_id_norm"] = ml_df["member_id_norm"].astype(str).str.strip()

    merged = user_df.merge(ml_df, on="member_id_norm", how="left")

    print(f"[DEBUG] MATCHED ROWS = {merged['repurchase_30d_score'].notnull().sum()}")

    return merged
