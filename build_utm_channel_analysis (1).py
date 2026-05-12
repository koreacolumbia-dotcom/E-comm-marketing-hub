#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
UTM / Source-Medium Channel Analysis builder

- Pulls GA4 export data from BigQuery (GA에서 볼 수 있는 퍼널 지표는 GA4 BQ export 기준)
- Uses the uploaded Looker Studio CASE logic as the channel-group mapping baseline
- Builds CSV / JSON / HTML outputs for GitHub Pages hub
- Optionally joins ad cost data if AD_COST_TABLE is provided

Required env on GitHub Actions:
  GOOGLE_APPLICATION_CREDENTIALS=/tmp/...json
  BQ_PROJECT=columbia-ga4
  BQ_DATASET=analytics_358593394
  GA4_PROPERTY_ID=358593394 (optional metadata only)

Optional env:
  AD_COST_TABLE=project.dataset.table
    Expected flexible columns: date/event_dt/day, source, medium, campaign, content, term, cost/spend/ad_cost, clicks, impressions
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

KST = timezone(timedelta(hours=9))
DEFAULT_PROJECT = os.getenv("BQ_PROJECT", "columbia-ga4")
DEFAULT_DATASET = os.getenv("BQ_DATASET", "analytics_358593394")
DEFAULT_TABLE = os.getenv("GA4_EVENTS_TABLE", f"{DEFAULT_PROJECT}.{DEFAULT_DATASET}.events_*")
DEFAULT_PROPERTY_ID = os.getenv("GA4_PROPERTY_ID", "358593394")

# ================================================================
# Channel mapping SQL
# - Mirrors user's Looker Studio CASE logic as closely as possible
# ================================================================
CHANNEL_CASE_SQL = r"""
CASE
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(instagram).*') AND REGEXP_CONTAINS(session_source_medium, r'(?i).*(story).*') THEN '4. Official SNS'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(benz).*') THEN '3. Organic Traffic'

  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(nap).*') AND REGEXP_CONTAINS(session_source_medium, r'(?i).*(da).*') THEN '2. Paid Ad'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(toss).*') THEN '2. Paid Ad'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(blind).*') THEN '2. Paid Ad'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(kakaobs).*') THEN '2. Paid Ad'

  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(inhouse).*') THEN '3. Organic Traffic'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(lms).*') OR REGEXP_CONTAINS(session_campaign, r'(?i).*(lms).*') THEN '5. Owned Channel'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(email|edm).*') THEN '5. Owned Channel'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(kakao_fridnstalk).*') THEN '5. Owned Channel'

  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(mkt|_bd).*') OR REGEXP_CONTAINS(session_campaign, r'(?i).*(mkt|\[bd).*') THEN '1. Awareness'

  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(igshopping).*') THEN '4. Official SNS'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(facebook).*') AND REGEXP_CONTAINS(session_source_medium, r'(?i).*(referral).*') THEN '3. Organic Traffic'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(instagram).*') AND REGEXP_CONTAINS(session_source_medium, r'(?i).*(referral).*') THEN '4. Official SNS'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(meta|facebook|instagram|ig|fb).*') THEN '2. Paid Ad'

  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(google / cpc).*') AND REGEXP_CONTAINS(session_campaign, r'(?i).*(디멘드젠|디멘드잰|디맨드젠|디맨드잰|dg|demandgen).*') THEN '1. Awareness'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(google / cpc).*') AND REGEXP_CONTAINS(session_campaign, r'(?i).*(pmax).*') THEN '2. Paid Ad'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(google / cpc).*') AND REGEXP_CONTAINS(session_campaign, r'(?i).*(유튜브|yt|youtube|instream|vac|vvc).*') THEN '1. Awareness'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(google / cpc).*') AND REGEXP_CONTAINS(session_campaign, r'(?i).*(discovery).*') THEN '1. Awareness'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(google / cpc).*') AND REGEXP_CONTAINS(session_campaign, r'(?i).*(sa|ss|검색).*') THEN '2. Paid Ad'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(google / cpc).*') THEN '2. Paid Ad'

  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(google / organic).*') THEN '3. Organic Traffic'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(google).*') THEN '3. Organic Traffic'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(youtube).*') THEN '3. Organic Traffic'

  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(naver).*') AND REGEXP_CONTAINS(session_source_medium, r'(?i).*(da).*') THEN '2. Paid Ad'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(gfa).*') THEN '2. Paid Ad'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(naverbs).*') THEN '2. Paid Ad'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(naver).*') AND REGEXP_CONTAINS(session_source_medium, r'(?i).*(cpc).*') THEN '2. Paid Ad'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(shopping_ad).*') THEN '2. Paid Ad'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(naver).*') AND REGEXP_CONTAINS(session_source_medium, r'(?i).*(shopping).*') THEN '3. Organic Traffic'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(naver).*') AND REGEXP_CONTAINS(session_source_medium, r'(?i).*(organic).*') THEN '3. Organic Traffic'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(naver).*') THEN '3. Organic Traffic'

  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(daum / organic).*') THEN '3. Organic Traffic'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(daum).*') AND REGEXP_CONTAINS(session_source_medium, r'(?i).*(referral).*') THEN '3. Organic Traffic'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(kakao_ch).*') OR REGEXP_CONTAINS(session_campaign, r'(?i).*(kakao_ch).*') THEN '5. Owned Channel'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(kakao_alimtalk).*') THEN '5. Owned Channel'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(kakao_coupon).*') THEN '5. Owned Channel'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(kakao_chatbot).*') THEN '5. Owned Channel'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(kakao).*') THEN '2. Paid Ad'

  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(\(direct\) / \(none\)).*') THEN '3. Organic Traffic'

  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(signalplay|signal play|signal_play|sg_|signal|manplus).*') THEN '2. Paid Ad'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(buzzvill).*') THEN '2. Paid Ad'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(criteo).*') THEN '2. Paid Ad'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(mobon).*') THEN '2. Paid Ad'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(snow).*') THEN '2. Paid Ad'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(smr).*') THEN '2. Paid Ad'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(tg).*') THEN '2. Paid Ad'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(t_cafe).*') THEN '2. Paid Ad'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(blind).*') THEN '2. Paid Ad'

  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(cpc).*') THEN '2. Paid Ad'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(organic).*') THEN '3. Organic Traffic'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(banner|da).*') THEN '2. Paid Ad'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(referral).*') THEN '3. Organic Traffic'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(shopping).*') THEN '3. Organic Traffic'
  WHEN REGEXP_CONTAINS(session_source_medium, r'(?i).*(social).*') THEN '3. Organic Traffic'

  ELSE '6. etc'
END
"""

MEDIA_FAMILY_SQL = r"""
CASE
  WHEN REGEXP_CONTAINS(LOWER(CONCAT(source, ' / ', medium, ' / ', campaign)), r'meta|facebook|instagram|ig|fb') THEN 'Meta'
  WHEN REGEXP_CONTAINS(LOWER(CONCAT(source, ' / ', medium, ' / ', campaign)), r'google|gdn|demandgen|demand_gen|youtube|yt|디멘드|디맨드') THEN 'Google'
  WHEN REGEXP_CONTAINS(LOWER(CONCAT(source, ' / ', medium, ' / ', campaign)), r'naver|gfa|naverbs|shopping_ad') THEN 'Naver'
  WHEN REGEXP_CONTAINS(LOWER(CONCAT(source, ' / ', medium, ' / ', campaign)), r'kakao|kakaobs|alimtalk|friendtalk|친구톡|알림톡|kakao_ch') THEN 'Kakao'
  WHEN REGEXP_CONTAINS(LOWER(CONCAT(source, ' / ', medium, ' / ', campaign)), r'criteo') THEN 'Criteo'
  WHEN REGEXP_CONTAINS(LOWER(CONCAT(source, ' / ', medium, ' / ', campaign)), r'mobon') THEN 'Mobon'
  WHEN REGEXP_CONTAINS(LOWER(CONCAT(source, ' / ', medium, ' / ', campaign)), r'email|edm') THEN 'EDM'
  WHEN REGEXP_CONTAINS(LOWER(CONCAT(source, ' / ', medium, ' / ', campaign)), r'lms') THEN 'LMS'
  WHEN REGEXP_CONTAINS(LOWER(CONCAT(source, ' / ', medium, ' / ', campaign)), r'toss') THEN 'Toss'
  WHEN REGEXP_CONTAINS(LOWER(CONCAT(source, ' / ', medium, ' / ', campaign)), r'blind') THEN 'Blind'
  ELSE 'Other'
END
"""


def ymd(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def now_kst() -> datetime:
    return datetime.now(KST)


def ensure_parent(path: str | Path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def safe_num(v: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(v):
            return default
        return float(v)
    except Exception:
        return default


def fmt_int(v: Any) -> str:
    return f"{safe_num(v):,.0f}"


def fmt_money(v: Any) -> str:
    return f"₩{safe_num(v):,.0f}"


def fmt_pct(v: Any) -> str:
    return f"{safe_num(v):.1f}%"


def fmt_float(v: Any) -> str:
    if pd.isna(v):
        return "-"
    return f"{safe_num(v):.1f}"


def clean_df_for_json(df: pd.DataFrame) -> List[dict]:
    if df.empty:
        return []
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].dt.strftime("%Y-%m-%d")
    out = out.replace({float("inf"): None, float("-inf"): None})
    out = out.where(pd.notnull(out), None)
    return out.to_dict(orient="records")


def build_ga4_bq_sql(events_table: str, start_date: str, end_date: str) -> str:
    return f"""
DECLARE start_date DATE DEFAULT DATE('{start_date}');
DECLARE end_date DATE DEFAULT DATE('{end_date}');

WITH raw_events AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_dt,
    event_timestamp,
    event_name,
    user_pseudo_id,
    user_id,
    CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING) AS ga_session_id,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
    COALESCE(
      CAST(ecommerce.transaction_id AS STRING),
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'transaction_id')
    ) AS transaction_id,
    SAFE_CAST(ecommerce.purchase_revenue AS FLOAT64) AS purchase_revenue,

    session_traffic_source_last_click.manual_campaign.source AS stslc_source,
    session_traffic_source_last_click.manual_campaign.medium AS stslc_medium,
    session_traffic_source_last_click.manual_campaign.campaign_name AS stslc_campaign,
    session_traffic_source_last_click.manual_campaign.content AS stslc_content,
    session_traffic_source_last_click.manual_campaign.term AS stslc_term,

    collected_traffic_source.manual_source AS collected_source,
    collected_traffic_source.manual_medium AS collected_medium,
    collected_traffic_source.manual_campaign_name AS collected_campaign,
    collected_traffic_source.manual_content AS collected_content,
    collected_traffic_source.manual_term AS collected_term,

    traffic_source.source AS first_user_source,
    traffic_source.medium AS first_user_medium,
    traffic_source.name AS first_user_campaign
  FROM `{events_table}`
  WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', start_date)
                          AND FORMAT_DATE('%Y%m%d', end_date)
),
url_utm AS (
  SELECT
    *,
    NET.URL_DECODE(REGEXP_EXTRACT(page_location, r'[?&]utm_source=([^&#]+)')) AS url_utm_source,
    NET.URL_DECODE(REGEXP_EXTRACT(page_location, r'[?&]utm_medium=([^&#]+)')) AS url_utm_medium,
    NET.URL_DECODE(REGEXP_EXTRACT(page_location, r'[?&]utm_campaign=([^&#]+)')) AS url_utm_campaign,
    NET.URL_DECODE(REGEXP_EXTRACT(page_location, r'[?&]utm_content=([^&#]+)')) AS url_utm_content,
    NET.URL_DECODE(REGEXP_EXTRACT(page_location, r'[?&]utm_term=([^&#]+)')) AS url_utm_term
  FROM raw_events
),
session_events AS (
  SELECT
    event_dt,
    CONCAT(user_pseudo_id, '-', ga_session_id) AS session_key,
    ANY_VALUE(user_pseudo_id) AS user_pseudo_id,
    ANY_VALUE(user_id) AS user_id,

    LOWER(COALESCE(
      NULLIF(ANY_VALUE(stslc_source), ''),
      NULLIF(ANY_VALUE(collected_source), ''),
      NULLIF(ANY_VALUE(url_utm_source), ''),
      NULLIF(ANY_VALUE(first_user_source), ''),
      '(direct)'
    )) AS source,

    LOWER(COALESCE(
      NULLIF(ANY_VALUE(stslc_medium), ''),
      NULLIF(ANY_VALUE(collected_medium), ''),
      NULLIF(ANY_VALUE(url_utm_medium), ''),
      NULLIF(ANY_VALUE(first_user_medium), ''),
      '(none)'
    )) AS medium,

    COALESCE(
      NULLIF(ANY_VALUE(stslc_campaign), ''),
      NULLIF(ANY_VALUE(collected_campaign), ''),
      NULLIF(ANY_VALUE(url_utm_campaign), ''),
      NULLIF(ANY_VALUE(first_user_campaign), ''),
      '(not set)'
    ) AS campaign,

    COALESCE(
      NULLIF(ANY_VALUE(stslc_content), ''),
      NULLIF(ANY_VALUE(collected_content), ''),
      NULLIF(ANY_VALUE(url_utm_content), ''),
      '(not set)'
    ) AS content,

    COALESCE(
      NULLIF(ANY_VALUE(stslc_term), ''),
      NULLIF(ANY_VALUE(collected_term), ''),
      NULLIF(ANY_VALUE(url_utm_term), ''),
      '(not set)'
    ) AS term,

    COUNTIF(event_name = 'page_view') AS pageviews,
    MAX(IF(event_name = 'signup_complete', 1, 0)) AS has_signup,
    MIN(IF(event_name = 'signup_complete', event_timestamp, NULL)) AS signup_ts,
    MAX(IF(event_name = 'purchase', 1, 0)) AS has_purchase,
    MIN(IF(event_name = 'purchase', event_timestamp, NULL)) AS first_purchase_ts,
    COUNTIF(event_name = 'purchase') AS purchase_events,
    COUNT(DISTINCT IF(event_name = 'purchase' AND NULLIF(transaction_id, '') IS NOT NULL, transaction_id, NULL)) AS transactions,
    SUM(IF(event_name = 'purchase', IFNULL(purchase_revenue, 0), 0)) AS revenue
  FROM url_utm
  WHERE user_pseudo_id IS NOT NULL
    AND ga_session_id IS NOT NULL
  GROUP BY event_dt, session_key
),
attributed AS (
  SELECT
    *,
    CONCAT(source, ' / ', medium) AS session_source_medium,
    campaign AS session_campaign
  FROM session_events
),
channel_mapped AS (
  SELECT
    *,
    {CHANNEL_CASE_SQL} AS channel_group,
    {MEDIA_FAMILY_SQL} AS media_family
  FROM attributed
),
period_user_flags AS (
  SELECT
    user_pseudo_id,
    MAX(has_signup) AS period_signup_user,
    MAX(has_purchase) AS period_buyer,
    MIN(signup_ts) AS period_first_signup_ts,
    MIN(first_purchase_ts) AS period_first_purchase_ts
  FROM channel_mapped
  GROUP BY user_pseudo_id
),
final AS (
  SELECT
    c.event_dt,
    c.channel_group,
    c.media_family,
    c.source,
    c.medium,
    c.campaign,
    c.content,
    c.term,

    COUNT(DISTINCT c.session_key) AS sessions,
    COUNT(DISTINCT c.user_pseudo_id) AS users,
    COUNT(DISTINCT IF(c.has_signup = 1, c.user_pseudo_id, NULL)) AS signups,
    ROUND(SAFE_DIVIDE(COUNT(DISTINCT IF(c.has_signup = 1, c.user_pseudo_id, NULL)), COUNT(DISTINCT c.session_key)) * 100, 1) AS signup_cvr,
    ROUND(SAFE_DIVIDE(SUM(IF(c.has_signup = 1, c.pageviews, 0)), COUNT(DISTINCT IF(c.has_signup = 1, c.user_pseudo_id, NULL))), 1) AS avg_signup_user_pv,
    COUNT(DISTINCT IF(c.has_signup = 1 AND f.period_buyer = 1, c.user_pseudo_id, NULL)) AS signup_to_buyers,
    COUNT(DISTINCT IF(c.has_purchase = 1, c.user_pseudo_id, NULL)) AS buyers,
    ROUND(SAFE_DIVIDE(COUNT(DISTINCT IF(c.has_purchase = 1, c.user_pseudo_id, NULL)), COUNT(DISTINCT c.session_key)) * 100, 1) AS buy_cvr,
    SUM(c.purchase_events) AS purchase_events,
    SUM(c.transactions) AS purchase,
    ROUND(SUM(c.revenue), 0) AS revenue,
    ROUND(SAFE_DIVIDE(SUM(c.revenue), COUNT(DISTINCT IF(c.has_purchase = 1, c.user_pseudo_id, NULL))), 0) AS aov_per_buyer,
    ROUND(SAFE_DIVIDE(SUM(c.pageviews), COUNT(DISTINCT c.user_pseudo_id)), 1) AS pv_per_user
  FROM channel_mapped c
  LEFT JOIN period_user_flags f
    ON c.user_pseudo_id = f.user_pseudo_id
  GROUP BY
    c.event_dt, c.channel_group, c.media_family, c.source, c.medium, c.campaign, c.content, c.term
)
SELECT *
FROM final
ORDER BY event_dt DESC, channel_group, media_family, sessions DESC
"""


def run_bigquery(sql: str, project: str) -> pd.DataFrame:
    try:
        from google.cloud import bigquery
    except ImportError as exc:
        raise RuntimeError("google-cloud-bigquery is required when --input-csv is not provided") from exc
    client = bigquery.Client(project=project)
    job = client.query(sql)
    return job.result().to_dataframe(create_bqstorage_client=True)


def normalize_metric_cols(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    numeric_cols = [
        "sessions", "users", "signups", "signup_cvr", "avg_signup_user_pv", "signup_to_buyers",
        "buyers", "buy_cvr", "purchase_events", "purchase", "revenue", "aov_per_buyer", "pv_per_user",
        "cost", "clicks", "impressions", "roas", "cpc", "ctr"
    ]
    for col in numeric_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    if "event_dt" in out.columns:
        out["event_dt"] = pd.to_datetime(out["event_dt"]).dt.date.astype(str)
    # Use purchase_events fallback where transaction_id is missing.
    if "purchase" in out.columns and "purchase_events" in out.columns:
        out["purchase"] = out.apply(
            lambda r: r["purchase"] if safe_num(r.get("purchase")) > 0 else r.get("purchase_events"), axis=1
        )
    return out




# ================================================================
# Numeric alert system + readable full-data HTML report
# ================================================================

DIM_COLS = ["channel_group", "media_family", "source", "medium", "campaign", "content", "term"]
DISPLAY_METRICS = [
    "sessions", "users", "signups", "signup_cvr", "avg_signup_user_pv", "signup_to_buyers",
    "buyers", "buy_cvr", "purchase", "revenue", "aov_per_buyer", "pv_per_user"
]

METRIC_LABELS = {
    "sessions": "Sessions",
    "users": "Users",
    "signups": "Signups",
    "signup_cvr": "Signup CVR",
    "avg_signup_user_pv": "Avg Signup User PV",
    "signup_to_buyers": "Signup → Buyers",
    "buyers": "Buyers",
    "buy_cvr": "Buy CVR",
    "purchase": "Purchase",
    "revenue": "Revenue",
    "aov_per_buyer": "AOV / Buyer",
    "pv_per_user": "PV / User",
}

# DoD alert rules: intentionally strict to avoid noisy 2~3% changes.
ALERT_RULES = {
    "sessions": {"type": "rate", "rate": 10.0, "abs": 300, "min_prev": 300, "direction": "both"},
    "users": {"type": "rate", "rate": 10.0, "abs": 300, "min_prev": 300, "direction": "both"},
    "signups": {"type": "rate", "rate": 15.0, "abs": 20, "min_prev": 20, "direction": "both"},
    "signup_to_buyers": {"type": "rate", "rate": 20.0, "abs": 10, "min_prev": 10, "direction": "both"},
    "buyers": {"type": "rate", "rate": 15.0, "abs": 10, "min_prev": 10, "direction": "both"},
    "purchase": {"type": "rate", "rate": 15.0, "abs": 10, "min_prev": 10, "direction": "both"},
    "revenue": {"type": "rate", "rate": 10.0, "abs": 1_000_000, "min_prev": 1_000_000, "direction": "both"},
    "signup_cvr": {"type": "point", "point": 0.3, "min_sessions": 300, "direction": "both"},
    "buy_cvr": {"type": "point", "point": 0.3, "min_sessions": 300, "direction": "both"},
    "avg_signup_user_pv": {"type": "rate", "rate": 15.0, "abs": 0.5, "min_prev": 1, "min_base_metric": "signups", "min_base": 10, "direction": "both"},
    "aov_per_buyer": {"type": "rate", "rate": 15.0, "abs": 10_000, "min_prev": 10_000, "min_base_metric": "buyers", "min_base": 10, "direction": "both"},
    "pv_per_user": {"type": "rate", "rate": 15.0, "abs": 0.5, "min_prev": 1, "min_base_metric": "users", "min_base": 300, "direction": "both"},
}


def _agg_metrics(g: pd.DataFrame) -> Dict[str, float]:
    sessions = safe_num(g.get("sessions", pd.Series(dtype=float)).sum())
    users = safe_num(g.get("users", pd.Series(dtype=float)).sum())
    signups = safe_num(g.get("signups", pd.Series(dtype=float)).sum())
    signup_to_buyers = safe_num(g.get("signup_to_buyers", pd.Series(dtype=float)).sum())
    buyers = safe_num(g.get("buyers", pd.Series(dtype=float)).sum())
    purchase = safe_num(g.get("purchase", pd.Series(dtype=float)).sum())
    revenue = safe_num(g.get("revenue", pd.Series(dtype=float)).sum())
    pageviews = safe_num((g.get("pv_per_user", pd.Series(dtype=float)).fillna(0) * g.get("users", pd.Series(dtype=float)).fillna(0)).sum()) if "pv_per_user" in g and "users" in g else 0
    signup_pv = safe_num((g.get("avg_signup_user_pv", pd.Series(dtype=float)).fillna(0) * g.get("signups", pd.Series(dtype=float)).fillna(0)).sum()) if "avg_signup_user_pv" in g and "signups" in g else 0
    return {
        "sessions": sessions,
        "users": users,
        "signups": signups,
        "signup_cvr": round(signups / sessions * 100, 1) if sessions else 0,
        "avg_signup_user_pv": round(signup_pv / signups, 1) if signups else 0,
        "signup_to_buyers": signup_to_buyers,
        "buyers": buyers,
        "buy_cvr": round(buyers / sessions * 100, 1) if sessions else 0,
        "purchase": purchase,
        "revenue": revenue,
        "aov_per_buyer": round(revenue / buyers, 0) if buyers else 0,
        "pv_per_user": round(pageviews / users, 1) if users else 0,
    }


def summarize(df: pd.DataFrame, period_start: str, period_end: str) -> Dict[str, Any]:
    if df.empty:
        base = {m: 0 for m in DISPLAY_METRICS}
        base.update({"period": f"{period_start} ~ {period_end}", "rows": 0, "top_channel": "-"})
        return base
    total = _agg_metrics(df)
    channel_rev = df.groupby("channel_group", dropna=False)["revenue"].sum().sort_values(ascending=False) if "channel_group" in df.columns else pd.Series(dtype=float)
    total.update({
        "period": f"{period_start} ~ {period_end}",
        "rows": int(len(df)),
        "top_channel": str(channel_rev.index[0]) if len(channel_rev) else "-",
    })
    return total


def build_channel_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    rows = []
    for channel, g in df.groupby("channel_group", dropna=False):
        item = {"channel_group": channel}
        item.update(_agg_metrics(g))
        rows.append(item)
    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(["revenue", "sessions"], ascending=False)
    return out


def add_period_comparison(df: pd.DataFrame) -> pd.DataFrame:
    """Latest-day row-level comparison against previous day by full dimension."""
    if df.empty or "event_dt" not in df.columns:
        return pd.DataFrame()
    out = df.copy()
    out["event_dt_dt"] = pd.to_datetime(out["event_dt"], errors="coerce")
    max_dt = out["event_dt_dt"].max()
    if pd.isna(max_dt):
        return pd.DataFrame()
    prev_dt = max_dt - pd.Timedelta(days=1)
    dim_cols = [c for c in DIM_COLS if c in out.columns]
    curr = out[out["event_dt_dt"] == max_dt].copy()
    prev_cols = dim_cols + [m for m in DISPLAY_METRICS if m in out.columns]
    prev = out[out["event_dt_dt"] == prev_dt][prev_cols].copy()
    prev = prev.rename(columns={m: f"prev_{m}" for m in DISPLAY_METRICS if m in prev.columns})
    merged = curr.merge(prev, on=dim_cols, how="left")
    for metric in DISPLAY_METRICS:
        if metric in merged.columns and f"prev_{metric}" in merged.columns:
            merged[f"{metric}_dod"] = merged[metric].fillna(0) - merged[f"prev_{metric}"].fillna(0)
            merged[f"{metric}_dod_rate"] = merged.apply(
                lambda r: (safe_num(r[metric]) / safe_num(r[f"prev_{metric}"]) - 1) * 100 if safe_num(r[f"prev_{metric}"]) else None,
                axis=1,
            )
    return merged.drop(columns=["event_dt_dt"], errors="ignore")


def build_compare_scope(df: pd.DataFrame, scope_cols: List[str]) -> pd.DataFrame:
    """Aggregate current/previous day for numeric alert scopes."""
    if df.empty or "event_dt" not in df.columns:
        return pd.DataFrame()
    tmp = df.copy()
    tmp["event_dt_dt"] = pd.to_datetime(tmp["event_dt"], errors="coerce")
    latest_dt = tmp["event_dt_dt"].max()
    if pd.isna(latest_dt):
        return pd.DataFrame()
    prev_dt = latest_dt - pd.Timedelta(days=1)

    rows = []
    for label, day_df in [("curr", tmp[tmp["event_dt_dt"] == latest_dt]), ("prev", tmp[tmp["event_dt_dt"] == prev_dt])]:
        if day_df.empty:
            continue
        for keys, g in day_df.groupby(scope_cols, dropna=False):
            if not isinstance(keys, tuple):
                keys = (keys,)
            item = {col: key for col, key in zip(scope_cols, keys)}
            item["period_type"] = label
            item.update(_agg_metrics(g))
            rows.append(item)
    if not rows:
        return pd.DataFrame()
    agg = pd.DataFrame(rows)
    curr = agg[agg["period_type"] == "curr"].drop(columns=["period_type"])
    prev = agg[agg["period_type"] == "prev"].drop(columns=["period_type"])
    prev = prev.rename(columns={m: f"prev_{m}" for m in DISPLAY_METRICS if m in prev.columns})
    merged = curr.merge(prev, on=scope_cols, how="left")
    for metric in DISPLAY_METRICS:
        if metric in merged.columns and f"prev_{metric}" in merged.columns:
            merged[f"{metric}_delta"] = merged[metric].fillna(0) - merged[f"prev_{metric}"].fillna(0)
            merged[f"{metric}_delta_rate"] = merged.apply(
                lambda r: (safe_num(r[metric]) / safe_num(r[f"prev_{metric}"]) - 1) * 100 if safe_num(r[f"prev_{metric}"]) else None,
                axis=1,
            )
    return merged


def alert_passes(row: pd.Series, metric: str, rule: Dict[str, Any]) -> bool:
    curr = safe_num(row.get(metric))
    prev = safe_num(row.get(f"prev_{metric}"))
    delta = curr - prev
    if prev <= 0:
        return False
    if rule.get("min_base_metric"):
        base_metric = rule["min_base_metric"]
        if safe_num(row.get(base_metric)) < safe_num(rule.get("min_base", 0)) and safe_num(row.get(f"prev_{base_metric}")) < safe_num(rule.get("min_base", 0)):
            return False
    if rule["type"] == "point":
        if max(safe_num(row.get("sessions")), safe_num(row.get("prev_sessions"))) < safe_num(rule.get("min_sessions", 0)):
            return False
        return abs(delta) >= safe_num(rule.get("point", 0))
    rate = (curr / prev - 1) * 100
    return abs(rate) >= safe_num(rule.get("rate", 0)) and abs(delta) >= safe_num(rule.get("abs", 0)) and prev >= safe_num(rule.get("min_prev", 0))


def alert_score(row: pd.Series, metric: str, rule: Dict[str, Any]) -> float:
    curr = safe_num(row.get(metric))
    prev = safe_num(row.get(f"prev_{metric}"))
    if prev <= 0:
        return 0
    if rule["type"] == "point":
        return abs(curr - prev) / max(safe_num(rule.get("point", 0.1)), 0.1)
    rate = abs((curr / prev - 1) * 100)
    abs_delta = abs(curr - prev)
    return (rate / max(safe_num(rule.get("rate", 1)), 1)) + (abs_delta / max(safe_num(rule.get("abs", 1)), 1))


def build_numeric_alerts(df: pd.DataFrame, max_alerts: int = 80) -> pd.DataFrame:
    """Strict DoD numeric alerts. No narrative insights, only measurable alerts."""
    if df.empty:
        return pd.DataFrame()
    scopes = [
        ("channel", ["channel_group"]),
        ("media", ["channel_group", "media_family"]),
        ("source_medium", ["channel_group", "media_family", "source", "medium"]),
        ("campaign", ["channel_group", "media_family", "source", "medium", "campaign"]),
    ]
    alerts = []
    for scope_name, scope_cols in scopes:
        compare = build_compare_scope(df, scope_cols)
        if compare.empty:
            continue
        for _, row in compare.iterrows():
            scope_label = " / ".join(str(row.get(c, "-")) for c in scope_cols)
            for metric, rule in ALERT_RULES.items():
                if metric not in compare.columns or f"prev_{metric}" not in compare.columns:
                    continue
                if not alert_passes(row, metric, rule):
                    continue
                curr = safe_num(row.get(metric))
                prev = safe_num(row.get(f"prev_{metric}"))
                delta = curr - prev
                rate = (curr / prev - 1) * 100 if prev else None
                direction = "UP" if delta > 0 else "DOWN"
                alerts.append({
                    "scope": scope_name,
                    "scope_label": scope_label,
                    "metric": metric,
                    "metric_label": METRIC_LABELS.get(metric, metric),
                    "direction": direction,
                    "current": curr,
                    "previous": prev,
                    "delta": delta,
                    "delta_rate": rate,
                    "score": alert_score(row, metric, rule),
                    "sessions": safe_num(row.get("sessions")),
                    "prev_sessions": safe_num(row.get("prev_sessions")),
                    "revenue": safe_num(row.get("revenue")),
                    "prev_revenue": safe_num(row.get("prev_revenue")),
                    "channel_group": row.get("channel_group", "-"),
                    "media_family": row.get("media_family", "-"),
                    "source": row.get("source", "-"),
                    "medium": row.get("medium", "-"),
                    "campaign": row.get("campaign", "-"),
                })
    alert_df = pd.DataFrame(alerts)
    if alert_df.empty:
        return alert_df
    # Dedupe: same scope + metric can appear in multiple broader labels; keep highest score.
    alert_df = alert_df.sort_values("score", ascending=False)
    alert_df = alert_df.drop_duplicates(["scope", "scope_label", "metric"], keep="first")
    return alert_df.head(max_alerts).reset_index(drop=True)


def build_utm_quality(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    rows = []
    total_sessions = safe_num(df["sessions"].sum())
    for field in ["source", "medium", "campaign", "content", "term"]:
        if field not in df.columns:
            continue
        mask = df[field].fillna("").astype(str).str.lower().isin(["", "(not set)", "(none)"])
        sessions = safe_num(df.loc[mask, "sessions"].sum())
        rows.append({"field": field, "missing_sessions": sessions, "missing_share": round(sessions / total_sessions * 100, 1) if total_sessions else 0})
    return pd.DataFrame(rows).sort_values("missing_share", ascending=False) if rows else pd.DataFrame()


def html_escape(s: Any) -> str:
    text = "" if s is None else str(s)
    return (text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;"))


def fmt_metric_value(metric: str, value: Any) -> str:
    if metric in {"revenue", "aov_per_buyer"}:
        return fmt_money(value)
    if metric in {"signup_cvr", "buy_cvr"}:
        return fmt_pct(value)
    if metric in {"avg_signup_user_pv", "pv_per_user"}:
        return fmt_float(value)
    return fmt_int(value)


def fmt_delta(metric: str, value: Any) -> str:
    sign = "+" if safe_num(value) > 0 else ""
    if metric in {"revenue", "aov_per_buyer"}:
        return f"{sign}{fmt_money(value)}" if safe_num(value) >= 0 else f"-{fmt_money(abs(safe_num(value)))}"
    if metric in {"signup_cvr", "buy_cvr"}:
        return f"{sign}{safe_num(value):.1f}p"
    if metric in {"avg_signup_user_pv", "pv_per_user"}:
        return f"{sign}{safe_num(value):.1f}"
    return f"{sign}{fmt_int(value)}"


def make_alert_cards(alerts: pd.DataFrame) -> str:
    if alerts.empty:
        return """
        <div class="alert-card neutral">
          <div class="alert-top"><span class="alert-chip neutral">CLEAR</span></div>
          <div class="alert-title">전일 대비 유의미한 수치 변동 없음</div>
          <div class="alert-sub">설정 기준을 넘는 증감만 상단에 표시합니다.</div>
        </div>
        """
    cards = []
    for _, r in alerts.head(8).iterrows():
        direction = str(r.get("direction"))
        cls = "up" if direction == "UP" else "down"
        arrow = "▲" if direction == "UP" else "▼"
        metric = str(r.get("metric"))
        rate = r.get("delta_rate")
        rate_txt = f"{arrow} {abs(safe_num(rate)):.1f}%" if rate is not None and not pd.isna(rate) else arrow
        if metric in {"signup_cvr", "buy_cvr"}:
            rate_txt = f"{arrow} {abs(safe_num(r.get('delta'))):.1f}p"
        cards.append(f"""
        <div class="alert-card {cls}">
          <div class="alert-top"><span class="alert-chip {cls}">{html_escape(rate_txt)}</span><span>{html_escape(str(r.get('scope')).upper())}</span></div>
          <div class="alert-title">{html_escape(r.get('metric_label'))}</div>
          <div class="alert-scope">{html_escape(r.get('scope_label'))}</div>
          <div class="alert-values">
            <b>{html_escape(fmt_metric_value(metric, r.get('current')))}</b>
            <span>Prev {html_escape(fmt_metric_value(metric, r.get('previous')))} · Δ {html_escape(fmt_delta(metric, r.get('delta')))}</span>
          </div>
        </div>
        """)
    return "\n".join(cards)


def make_alert_rows(alerts: pd.DataFrame) -> str:
    if alerts.empty:
        return '<tr><td colspan="9" class="empty">전일 대비 유의미한 수치 변동이 없습니다.</td></tr>'
    rows = []
    for _, r in alerts.iterrows():
        metric = str(r.get("metric"))
        direction = str(r.get("direction"))
        cls = "up" if direction == "UP" else "down"
        arrow = "▲" if direction == "UP" else "▼"
        rows.append(f"""
        <tr>
          <td><span class="dir {cls}">{arrow} {html_escape(direction)}</span></td>
          <td>{html_escape(r.get('scope'))}</td>
          <td class="wide">{html_escape(r.get('scope_label'))}</td>
          <td>{html_escape(r.get('metric_label'))}</td>
          <td class="num strong">{html_escape(fmt_metric_value(metric, r.get('current')))}</td>
          <td class="num">{html_escape(fmt_metric_value(metric, r.get('previous')))}</td>
          <td class="num">{html_escape(fmt_delta(metric, r.get('delta')))}</td>
          <td class="num">{safe_num(r.get('delta_rate')):.1f}%</td>
          <td class="num">{safe_num(r.get('score')):.1f}</td>
        </tr>
        """)
    return "\n".join(rows)


def make_quality_rows(qdf: pd.DataFrame) -> str:
    if qdf.empty:
        return '<tr><td colspan="3" class="empty">UTM 품질 데이터가 없습니다.</td></tr>'
    return "\n".join(
        f"<tr><td>{html_escape(r['field'])}</td><td class='num'>{fmt_int(r['missing_sessions'])}</td><td class='num'>{fmt_pct(r['missing_share'])}</td></tr>"
        for _, r in qdf.iterrows()
    )


def make_channel_cards(channel_df: pd.DataFrame) -> str:
    if channel_df.empty:
        return ""
    cards = []
    for _, r in channel_df.head(6).iterrows():
        cards.append(f"""
        <div class="card mini">
          <div class="label">{html_escape(r['channel_group'])}</div>
          <div class="metric">{fmt_money(r['revenue'])}</div>
          <div class="sub">Sessions {fmt_int(r['sessions'])} · Users {fmt_int(r['users'])} · Buy CVR {fmt_pct(r['buy_cvr'])}</div>
        </div>
        """)
    return "\n".join(cards)


def make_table_rows(df: pd.DataFrame, limit: int = 200) -> str:
    if df.empty:
        return '<tr><td colspan="19" class="empty">데이터가 없습니다.</td></tr>'
    show = df.sort_values(["event_dt", "revenue", "sessions"], ascending=[False, False, False]).head(limit)
    rows = []
    for _, r in show.iterrows():
        rows.append(f"""
        <tr>
          <td>{html_escape(r.get('event_dt', '-'))}</td>
          <td><span class="pill">{html_escape(r.get('channel_group', '-'))}</span></td>
          <td>{html_escape(r.get('media_family', '-'))}</td>
          <td>{html_escape(r.get('source', '-'))} / {html_escape(r.get('medium', '-'))}</td>
          <td class="wide">{html_escape(r.get('campaign', '-'))}</td>
          <td class="wide">{html_escape(r.get('content', '-'))}</td>
          <td class="wide">{html_escape(r.get('term', '-'))}</td>
          <td class="num">{fmt_int(r.get('sessions'))}</td>
          <td class="num">{fmt_int(r.get('users'))}</td>
          <td class="num">{fmt_int(r.get('signups'))}</td>
          <td class="num">{fmt_pct(r.get('signup_cvr'))}</td>
          <td class="num">{fmt_float(r.get('avg_signup_user_pv'))}</td>
          <td class="num">{fmt_int(r.get('signup_to_buyers'))}</td>
          <td class="num">{fmt_int(r.get('buyers'))}</td>
          <td class="num">{fmt_pct(r.get('buy_cvr'))}</td>
          <td class="num">{fmt_int(r.get('purchase'))}</td>
          <td class="num strong">{fmt_money(r.get('revenue'))}</td>
          <td class="num">{fmt_money(r.get('aov_per_buyer'))}</td>
          <td class="num">{fmt_float(r.get('pv_per_user'))}</td>
        </tr>
        """)
    return "\n".join(rows)


def render_html(df: pd.DataFrame, alerts: pd.DataFrame, channel_df: pd.DataFrame, quality_df: pd.DataFrame, meta: Dict[str, Any], out_path: str) -> None:
    summary = meta["summary"]
    payload = clean_df_for_json(df)
    alert_payload = clean_df_for_json(alerts)
    channel_payload = clean_df_for_json(channel_df)
    min_date, max_date = _date_bounds_from_df(df)
    html_doc = f"""<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>UTM / Source-Medium Numeric Alerts</title>
<script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;600;800;900&display=swap');
  :root {{ --brand:#002d72; --ink:#0f172a; --muted:#64748b; --line:#e2e8f0; --bg:#f6f8fb; --up:#047857; --down:#be123c; }}
  body {{ margin:0; font-family:'Plus Jakarta Sans', system-ui, sans-serif; background:linear-gradient(180deg,#f8fafc,#eef3f9); color:var(--ink); }}
  .wrap {{ padding:28px; max-width:1880px; margin:0 auto; }}
  .hero {{ border-radius:30px; padding:28px; background:linear-gradient(135deg,rgba(0,45,114,.96),rgba(29,78,216,.78)); color:white; box-shadow:0 24px 60px rgba(0,45,114,.18); }}
  .eyebrow {{ font-size:11px; letter-spacing:.24em; text-transform:uppercase; font-weight:900; opacity:.75; }}
  h1 {{ margin:8px 0 4px; font-size:32px; line-height:1.12; font-weight:900; }}
  .hero p {{ margin:0; opacity:.82; font-weight:700; }}
  .grid {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:16px; margin-top:18px; }}
  .alert-grid {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:14px; margin-top:18px; }}
  .card, .alert-card {{ background:rgba(255,255,255,.78); border:1px solid rgba(255,255,255,.9); backdrop-filter:blur(16px); border-radius:24px; padding:20px; box-shadow:0 18px 40px rgba(15,23,42,.06); }}
  .mini {{ min-height:118px; }}
  .label {{ color:#64748b; font-size:11px; font-weight:900; letter-spacing:.06em; text-transform:uppercase; }}
  .metric {{ font-size:28px; font-weight:900; margin-top:8px; }}
  .sub {{ font-size:12px; color:#64748b; font-weight:800; margin-top:6px; }}
  .section {{ margin-top:18px; }}
  .two {{ display:grid; grid-template-columns:1.2fr .8fr; gap:16px; }}
  .alert-card {{ min-height:150px; }}
  .alert-card.up {{ border-color:#a7f3d0; }}
  .alert-card.down {{ border-color:#fecdd3; }}
  .alert-card.neutral {{ border-color:#e2e8f0; }}
  .alert-top {{ display:flex; justify-content:space-between; gap:10px; align-items:center; color:#94a3b8; font-size:10px; font-weight:900; letter-spacing:.08em; }}
  .alert-chip {{ display:inline-flex; padding:6px 10px; border-radius:999px; font-size:12px; font-weight:900; }}
  .alert-chip.up {{ background:#ecfdf5; color:#047857; }}
  .alert-chip.down {{ background:#fff1f2; color:#be123c; }}
  .alert-chip.neutral {{ background:#f8fafc; color:#64748b; }}
  .alert-title {{ font-size:20px; font-weight:900; margin-top:14px; }}
  .alert-scope {{ color:#64748b; font-size:12px; font-weight:800; margin-top:6px; line-height:1.35; min-height:34px; word-break:break-word; }}
  .alert-values {{ margin-top:12px; display:flex; flex-direction:column; gap:4px; }}
  .alert-values b {{ font-size:22px; color:#002d72; }}
  .alert-values span {{ font-size:12px; color:#64748b; font-weight:800; }}
  .toolbar {{ display:flex; gap:10px; align-items:center; justify-content:space-between; flex-wrap:wrap; margin-bottom:12px; }}
  input, select, button {{ border:1px solid #dbe4ef; border-radius:14px; padding:11px 12px; font-weight:800; background:white; color:#0f172a; outline:none; }}
  button {{ cursor:pointer; background:#002d72; color:#fff; border-color:#002d72; }}
  button.secondary {{ background:#fff; color:#002d72; }}
  .date-controls {{ display:flex; gap:10px; align-items:center; flex-wrap:wrap; margin-top:18px; }}
  .view-tabs {{ display:flex; gap:8px; flex-wrap:wrap; }}
  .view-tabs button {{ padding:9px 12px; font-size:12px; }}
  .view-tabs button.active {{ background:#002d72; color:#fff; }}
  .view-tabs button:not(.active) {{ background:#fff; color:#002d72; }}
  .tablebox {{ max-height:760px; overflow:auto; border-radius:20px; border:1px solid var(--line); background:white; }}
  table {{ width:100%; border-collapse:separate; border-spacing:0; font-size:12px; }}
  th {{ position:sticky; top:0; background:#f8fafc; color:#475569; text-align:left; font-size:10px; letter-spacing:.06em; text-transform:uppercase; padding:12px 10px; border-bottom:1px solid var(--line); z-index:2; white-space:nowrap; }}
  td {{ padding:12px 10px; border-bottom:1px solid #eef2f7; vertical-align:top; font-weight:700; color:#334155; }}
  tr:hover td {{ background:#f8fafc; }}
  .num {{ text-align:right; white-space:nowrap; font-variant-numeric:tabular-nums; }}
  .wide {{ min-width:170px; max-width:340px; word-break:break-word; }}
  .strong {{ color:#002d72; font-weight:900; }}
  .pill {{ display:inline-flex; align-items:center; padding:6px 9px; border-radius:999px; background:#eff6ff; color:#1d4ed8; font-weight:900; white-space:nowrap; }}
  .dir {{ display:inline-flex; padding:6px 9px; border-radius:999px; font-weight:900; white-space:nowrap; }}
  .dir.up {{ background:#ecfdf5; color:#047857; }}
  .dir.down {{ background:#fff1f2; color:#be123c; }}
  .empty {{ text-align:center; color:#94a3b8; padding:42px; }}
  .foot {{ color:#94a3b8; font-size:11px; font-weight:800; margin-top:18px; }}
  canvas {{ width:100%; max-height:320px; }}
  @media(max-width:1280px) {{ .grid, .alert-grid {{ grid-template-columns:repeat(2,minmax(0,1fr)); }} .two {{ grid-template-columns:1fr; }} }}
  @media(max-width:720px) {{ .wrap {{ padding:16px; }} h1 {{ font-size:24px; }} .grid, .alert-grid {{ grid-template-columns:1fr; }} }}
</style>
</head>
<body>
<div class="wrap">
  <section class="hero">
    <div class="eyebrow">GA4 BigQuery · UTM / Source-Medium</div>
    <h1>소스/매체 수치 얼럿</h1>
    <p>데이터 범위 {html_escape(min_date)} ~ {html_escape(max_date)} · 업데이트 {html_escape(meta['updated_at'])} · 전일 대비 기준 초과 변동만 노출</p>
    <div class="date-controls">
      <label>시작 <input type="date" id="startDate" value="{html_escape(min_date)}" min="{html_escape(min_date)}" max="{html_escape(max_date)}"></label>
      <label>종료 <input type="date" id="endDate" value="{html_escape(max_date)}" min="{html_escape(min_date)}" max="{html_escape(max_date)}"></label>
      <button id="applyBtn">조회</button>
      <button class="secondary" id="yesterdayBtn">전일</button>
      <button class="secondary" id="last7Btn">최근 7일</button>
      <button class="secondary" id="last30Btn">최근 30일</button>
      <button class="secondary" id="allBtn">전체</button>
    </div>
  </section>

  <section class="grid">
    <div class="card"><div class="label">Sessions</div><div class="metric">{fmt_int(summary['sessions'])}</div><div class="sub">Users {fmt_int(summary['users'])}</div></div>
    <div class="card"><div class="label">Signup CVR</div><div class="metric">{fmt_pct(summary['signup_cvr'])}</div><div class="sub">Signups {fmt_int(summary['signups'])} · Signup→Buyers {fmt_int(summary['signup_to_buyers'])}</div></div>
    <div class="card"><div class="label">Buy CVR</div><div class="metric">{fmt_pct(summary['buy_cvr'])}</div><div class="sub">Buyers {fmt_int(summary['buyers'])} · Purchase {fmt_int(summary['purchase'])}</div></div>
    <div class="card"><div class="label">Revenue</div><div class="metric">{fmt_money(summary['revenue'])}</div><div class="sub">AOV/Buyer {fmt_money(summary['aov_per_buyer'])} · PV/User {fmt_float(summary['pv_per_user'])}</div></div>
  </section>

  <section class="grid" id="selectedCards"></section>

  <section class="alert-grid">{make_alert_cards(alerts)}</section>

  <section class="grid">{make_channel_cards(channel_df)}</section>

  <section class="section two">
    <div class="card">
      <div class="toolbar"><div><div class="label">Numeric Alert Table</div><div class="sub">전일 대비 엄격 기준을 넘은 수치 변동</div></div></div>
      <div class="tablebox" style="max-height:440px;">
        <table>
          <thead><tr><th>Dir</th><th>Scope</th><th>Label</th><th>Metric</th><th class="num">Current</th><th class="num">Previous</th><th class="num">Delta</th><th class="num">Rate</th><th class="num">Score</th></tr></thead>
          <tbody>{make_alert_rows(alerts)}</tbody>
        </table>
      </div>
    </div>
    <div class="card">
      <div class="toolbar"><div><div class="label">Revenue by Channel</div><div class="sub">채널그룹별 매출</div></div></div>
      <canvas id="channelChart"></canvas>
      <div class="toolbar" style="margin-top:18px;"><div><div class="label">UTM Missing Check</div><div class="sub">누락값 포함 세션 비중</div></div></div>
      <div class="tablebox" style="max-height:230px;">
        <table><thead><tr><th>Field</th><th class="num">Sessions</th><th class="num">Share</th></tr></thead><tbody>{make_quality_rows(quality_df)}</tbody></table>
      </div>
    </div>
  </section>

  <section class="section card">
    <div class="toolbar">
      <div>
        <div class="label">Data View</div>
        <div class="sub">기간별 요약 / 일자별 요약 / 전체 상세 데이터를 전환해서 확인</div>
      </div>
      <div>
        <input id="q" placeholder="검색: source, campaign, content, term..." />
        <select id="channel"><option value="">전체 채널</option></select>
        <select id="metricSort"><option value="revenue">Revenue순</option><option value="sessions">Sessions순</option><option value="buy_cvr">Buy CVR순</option><option value="signup_cvr">Signup CVR순</option></select>
      </div>
      <div class="view-tabs">
        <button class="active" data-view="period">기간별 요약</button>
        <button data-view="daily">일자별 요약</button>
        <button data-view="detail">전체 상세</button>
      </div>
    </div>
    <div class="tablebox" id="periodBox">
      <table id="periodTbl">
        <thead>
          <tr>
            <th>Channel</th><th>Media</th><th>Source/Medium</th><th>Campaign</th>
            <th class="num">Sessions</th><th class="num">Users</th><th class="num">Signups</th><th class="num">Signup CVR</th><th class="num">Avg Signup PV</th><th class="num">Signup→Buyers</th>
            <th class="num">Buyers</th><th class="num">Buy CVR</th><th class="num">Purchase</th><th class="num">Revenue</th><th class="num">AOV/Buyer</th><th class="num">PV/User</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </div>
    <div class="tablebox" id="dailyBox" style="display:none;">
      <table id="dailyTbl">
        <thead>
          <tr>
            <th>Date</th><th>Channel</th>
            <th class="num">Sessions</th><th class="num">Users</th><th class="num">Signups</th><th class="num">Signup CVR</th><th class="num">Avg Signup PV</th><th class="num">Signup→Buyers</th>
            <th class="num">Buyers</th><th class="num">Buy CVR</th><th class="num">Purchase</th><th class="num">Revenue</th><th class="num">AOV/Buyer</th><th class="num">PV/User</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </div>
    <div class="tablebox" id="detailBox" style="display:none;">
      <table id="tbl">
        <thead>
          <tr>
            <th>Date</th><th>Channel</th><th>Media</th><th>Source/Medium</th><th>Campaign</th><th>Content</th><th>Term</th>
            <th class="num">Sessions</th><th class="num">Users</th><th class="num">Signups</th><th class="num">Signup CVR</th><th class="num">Avg Signup PV</th><th class="num">Signup→Buyers</th>
            <th class="num">Buyers</th><th class="num">Buy CVR</th><th class="num">Purchase</th><th class="num">Revenue</th><th class="num">AOV/Buyer</th><th class="num">PV/User</th>
          </tr>
        </thead>
        <tbody>{make_table_rows(df)}</tbody>
      </table>
    </div>
    <div class="foot">Data: GA4 BigQuery Export · Channel rule: uploaded CASE logic · Numeric alert thresholds are intentionally strict to suppress minor 2~3% noise.</div>
  </section>
</div>
<script>
const rows = {json.dumps(payload, ensure_ascii=False)};
const alerts = {json.dumps(alert_payload, ensure_ascii=False)};
const channelRows = {json.dumps(channel_payload, ensure_ascii=False)};
const allMinDate = "{html_escape(min_date)}";
const allMaxDate = "{html_escape(max_date)}";
function money(v) {{ return '₩' + Number(v||0).toLocaleString('ko-KR', {{maximumFractionDigits:0}}); }}
function pct(v) {{ return Number(v||0).toFixed(1) + '%'; }}
function num(v) {{ return Number(v||0).toLocaleString('ko-KR', {{maximumFractionDigits:0}}); }}
function one(v) {{ return v === null || v === undefined || isNaN(Number(v)) ? '-' : Number(v).toFixed(1); }}
function esc(s) {{ return String(s ?? '').replace(/[&<>"']/g, m => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}}[m])); }}
function n(v) {{ const x = Number(v || 0); return isFinite(x) ? x : 0; }}
const channelSelect = document.getElementById('channel');
[...new Set(rows.map(r => r.channel_group).filter(Boolean))].sort().forEach(ch => {{
  const opt = document.createElement('option'); opt.value = ch; opt.textContent = ch; channelSelect.appendChild(opt);
}});
function dateAdd(d, days) {{ const x = new Date(d + 'T00:00:00'); x.setDate(x.getDate()+days); return x.toISOString().slice(0,10); }}
function filteredRows() {{
  const q = (document.getElementById('q').value || '').toLowerCase();
  const ch = channelSelect.value;
  const s = document.getElementById('startDate') ? document.getElementById('startDate').value : allMinDate;
  const e = document.getElementById('endDate') ? document.getElementById('endDate').value : allMaxDate;
  return rows.filter(r => (!s || r.event_dt >= s) && (!e || r.event_dt <= e) && (!ch || r.channel_group === ch) && (!q || JSON.stringify(r).toLowerCase().includes(q)));
}}
function aggMetrics(arr) {{
  const sessions = arr.reduce((a,r)=>a+n(r.sessions),0), users = arr.reduce((a,r)=>a+n(r.users),0), signups = arr.reduce((a,r)=>a+n(r.signups),0);
  const signupToBuyers = arr.reduce((a,r)=>a+n(r.signup_to_buyers),0), buyers = arr.reduce((a,r)=>a+n(r.buyers),0), purchase = arr.reduce((a,r)=>a+n(r.purchase),0), revenue = arr.reduce((a,r)=>a+n(r.revenue),0);
  const pageviews = arr.reduce((a,r)=>a+n(r.pv_per_user)*n(r.users),0);
  const signupPv = arr.reduce((a,r)=>a+n(r.avg_signup_user_pv)*n(r.signups),0);
  return {{sessions, users, signups, signup_cvr:sessions?signups/sessions*100:0, avg_signup_user_pv:signups?signupPv/signups:0, signup_to_buyers:signupToBuyers, buyers, buy_cvr:sessions?buyers/sessions*100:0, purchase, revenue, aov_per_buyer:buyers?revenue/buyers:0, pv_per_user:users?pageviews/users:0}};
}}
function groupBy(arr, keys) {{
  const map = new Map();
  arr.forEach(r => {{ const k = keys.map(x => r[x] ?? '-').join('||'); if (!map.has(k)) map.set(k, {{keys:Object.fromEntries(keys.map(x=>[x,r[x]??'-'])), rows:[]}}); map.get(k).rows.push(r); }});
  return [...map.values()].map(g => ({{...g.keys, ...aggMetrics(g.rows)}}));
}}
function metricCells(r) {{ return `<td class="num">${{num(r.sessions)}}</td><td class="num">${{num(r.users)}}</td><td class="num">${{num(r.signups)}}</td><td class="num">${{pct(r.signup_cvr)}}</td><td class="num">${{one(r.avg_signup_user_pv)}}</td><td class="num">${{num(r.signup_to_buyers)}}</td><td class="num">${{num(r.buyers)}}</td><td class="num">${{pct(r.buy_cvr)}}</td><td class="num">${{num(r.purchase)}}</td><td class="num strong">${{money(r.revenue)}}</td><td class="num">${{money(r.aov_per_buyer)}}</td><td class="num">${{one(r.pv_per_user)}}</td>`; }}
function renderCards(arr) {{
  const s = aggMetrics(arr);
  const el = document.getElementById('selectedCards');
  if (!el) return;
  el.innerHTML = `<div class="card"><div class="label">Selected Sessions</div><div class="metric">${{num(s.sessions)}}</div><div class="sub">Users ${{num(s.users)}} · PV/User ${{one(s.pv_per_user)}}</div></div><div class="card"><div class="label">Selected Signup CVR</div><div class="metric">${{pct(s.signup_cvr)}}</div><div class="sub">Signups ${{num(s.signups)}} · Avg Signup PV ${{one(s.avg_signup_user_pv)}}</div></div><div class="card"><div class="label">Selected Buy CVR</div><div class="metric">${{pct(s.buy_cvr)}}</div><div class="sub">Buyers ${{num(s.buyers)}} · Purchase ${{num(s.purchase)}} · Signup→Buyers ${{num(s.signup_to_buyers)}}</div></div><div class="card"><div class="label">Selected Revenue</div><div class="metric">${{money(s.revenue)}}</div><div class="sub">AOV/Buyer ${{money(s.aov_per_buyer)}}</div></div>`;
}}
function renderPeriod(arr) {{
  const body = document.querySelector('#periodTbl tbody'); if (!body) return;
  const grouped = groupBy(arr, ['channel_group','media_family','source','medium','campaign']).sort((a,b)=>n(b.revenue)-n(a.revenue)||n(b.sessions)-n(a.sessions));
  body.innerHTML = grouped.length ? grouped.map(r => `<tr><td><span class="pill">${{esc(r.channel_group)}}</span></td><td>${{esc(r.media_family)}}</td><td>${{esc(r.source)}} / ${{esc(r.medium)}}</td><td class="wide">${{esc(r.campaign)}}</td>${{metricCells(r)}}</tr>`).join('') : '<tr><td colspan="16" class="empty">조건에 맞는 데이터가 없습니다.</td></tr>';
}}
function renderDaily(arr) {{
  const body = document.querySelector('#dailyTbl tbody'); if (!body) return;
  const grouped = groupBy(arr, ['event_dt','channel_group']).sort((a,b)=>String(b.event_dt).localeCompare(String(a.event_dt))||n(b.revenue)-n(a.revenue));
  body.innerHTML = grouped.length ? grouped.map(r => `<tr><td>${{esc(r.event_dt)}}</td><td><span class="pill">${{esc(r.channel_group)}}</span></td>${{metricCells(r)}}</tr>`).join('') : '<tr><td colspan="14" class="empty">조건에 맞는 데이터가 없습니다.</td></tr>';
}}
function renderTable() {{
  const sortKey = document.getElementById('metricSort').value || 'revenue';
  const filtered = filteredRows().sort((a,b) => (Number(b[sortKey]||0)-Number(a[sortKey]||0)) || (Number(b.sessions||0)-Number(a.sessions||0)));
  renderCards(filtered); renderPeriod(filtered); renderDaily(filtered);
  const body = document.querySelector('#tbl tbody');
  if (!filtered.length) {{ body.innerHTML = '<tr><td colspan="19" class="empty">조건에 맞는 데이터가 없습니다.</td></tr>'; return; }}
  body.innerHTML = filtered.map(r => `
    <tr>
      <td>${{r.event_dt || '-'}}</td><td><span class="pill">${{r.channel_group || '-'}}</span></td><td>${{r.media_family || '-'}}</td>
      <td>${{r.source || '-'}} / ${{r.medium || '-'}}</td><td class="wide">${{r.campaign || '-'}}</td><td class="wide">${{r.content || '-'}}</td><td class="wide">${{r.term || '-'}}</td>
      <td class="num">${{num(r.sessions)}}</td><td class="num">${{num(r.users)}}</td><td class="num">${{num(r.signups)}}</td><td class="num">${{pct(r.signup_cvr)}}</td><td class="num">${{one(r.avg_signup_user_pv)}}</td><td class="num">${{num(r.signup_to_buyers)}}</td>
      <td class="num">${{num(r.buyers)}}</td><td class="num">${{pct(r.buy_cvr)}}</td><td class="num">${{num(r.purchase)}}</td><td class="num strong">${{money(r.revenue)}}</td><td class="num">${{money(r.aov_per_buyer)}}</td><td class="num">${{one(r.pv_per_user)}}</td>
    </tr>`).join('');
  const foot = document.querySelector('.foot'); if (foot) foot.textContent = `Filtered rows ${{filtered.length.toLocaleString('ko-KR')}} / Total rows ${{rows.length.toLocaleString('ko-KR')}} · 모든 요청 지표 표시`;
}}
document.getElementById('q').addEventListener('input', renderTable);
channelSelect.addEventListener('change', renderTable);
document.getElementById('metricSort').addEventListener('change', renderTable);
['startDate','endDate'].forEach(id => {{ const el=document.getElementById(id); if(el) el.addEventListener('change', renderTable); }});
document.getElementById('applyBtn').addEventListener('click', renderTable);
document.getElementById('yesterdayBtn').onclick = () => {{ document.getElementById('startDate').value=allMaxDate; document.getElementById('endDate').value=allMaxDate; renderTable(); }};
document.getElementById('last7Btn').onclick = () => {{ document.getElementById('startDate').value=dateAdd(allMaxDate,-6); document.getElementById('endDate').value=allMaxDate; renderTable(); }};
document.getElementById('last30Btn').onclick = () => {{ document.getElementById('startDate').value=dateAdd(allMaxDate,-29); document.getElementById('endDate').value=allMaxDate; renderTable(); }};
document.getElementById('allBtn').onclick = () => {{ document.getElementById('startDate').value=allMinDate; document.getElementById('endDate').value=allMaxDate; renderTable(); }};
document.querySelectorAll('.view-tabs button').forEach(btn => btn.addEventListener('click', () => {{
  document.querySelectorAll('.view-tabs button').forEach(b=>b.classList.remove('active')); btn.classList.add('active');
  document.getElementById('periodBox').style.display = btn.dataset.view === 'period' ? 'block' : 'none';
  document.getElementById('dailyBox').style.display = btn.dataset.view === 'daily' ? 'block' : 'none';
  document.getElementById('detailBox').style.display = btn.dataset.view === 'detail' ? 'block' : 'none';
}}));
const ctx = document.getElementById('channelChart');
new Chart(ctx, {{
  type: 'bar',
  data: {{ labels: channelRows.map(r => r.channel_group), datasets: [{{ label: 'Revenue', data: channelRows.map(r => Number(r.revenue||0)) }}] }},
  options: {{ responsive:true, plugins: {{ legend: {{ display:false }}, tooltip: {{ callbacks: {{ label: c => money(c.raw) }} }} }}, scales: {{ y: {{ ticks: {{ callback: v => money(v) }} }} }} }}
}});
renderTable();
</script>
</body>
</html>"""
    ensure_parent(out_path)
    Path(out_path).write_text(html_doc, encoding="utf-8")


def write_summary_json(report_key: str, meta: Dict[str, Any], out_dir: str = "reports") -> None:
    path = Path(out_dir) / "summary.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    data: Dict[str, Any] = {}
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8")) or {}
        except Exception:
            data = {}
    data[report_key] = meta
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")



# ================================================================
# Incremental / one-year retention helpers
# ================================================================

def _date_bounds_from_df(df: pd.DataFrame) -> tuple[str, str]:
    if df.empty or "event_dt" not in df.columns:
        d = now_kst().date() - timedelta(days=1)
        return ymd(d), ymd(d)
    dt = pd.to_datetime(df["event_dt"], errors="coerce").dropna()
    if dt.empty:
        d = now_kst().date() - timedelta(days=1)
        return ymd(d), ymd(d)
    return dt.min().strftime("%Y-%m-%d"), dt.max().strftime("%Y-%m-%d")


def load_existing_json(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        return pd.DataFrame()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            return pd.DataFrame()
        return normalize_metric_cols(pd.DataFrame(raw))
    except Exception as exc:
        print(f"[UTM][WARN] existing JSON read failed: {path} | {exc}")
        return pd.DataFrame()


def merge_incremental(existing: pd.DataFrame, new_df: pd.DataFrame, start_date: str, end_date: str, keep_days: int = 365) -> pd.DataFrame:
    """Replace the queried date range in the existing JSON and retain latest keep_days.

    Daily GitHub Actions flow:
    - First run: no JSON exists -> query 365 days.
    - Next runs: JSON exists -> query only end_date, replace that date, then rewrite JSON/CSV/HTML.
    """
    new_df = normalize_metric_cols(new_df)
    if existing.empty:
        merged = new_df.copy()
    else:
        existing = normalize_metric_cols(existing)
        existing_dates = pd.to_datetime(existing.get("event_dt"), errors="coerce")
        sdt = pd.to_datetime(start_date)
        edt = pd.to_datetime(end_date)
        existing = existing.loc[~((existing_dates >= sdt) & (existing_dates <= edt))].copy()
        merged = pd.concat([existing, new_df], ignore_index=True)

    if not merged.empty and "event_dt" in merged.columns:
        merged["event_dt"] = pd.to_datetime(merged["event_dt"], errors="coerce").dt.strftime("%Y-%m-%d")
        max_dt = pd.to_datetime(merged["event_dt"], errors="coerce").max()
        if pd.notna(max_dt) and keep_days > 0:
            cutoff = max_dt - pd.Timedelta(days=keep_days - 1)
            merged = merged[pd.to_datetime(merged["event_dt"], errors="coerce") >= cutoff].copy()
        dedupe_cols = [c for c in ["event_dt"] + DIM_COLS if c in merged.columns]
        merged = merged.drop_duplicates(dedupe_cols, keep="last")
        sort_cols = [c for c in ["event_dt", "channel_group", "media_family", "sessions"] if c in merged.columns]
        if sort_cols:
            ascending = [False if c in ("event_dt", "sessions") else True for c in sort_cols]
            merged = merged.sort_values(sort_cols, ascending=ascending)
    return normalize_metric_cols(merged).reset_index(drop=True)


def parse_args() -> argparse.Namespace:
    yesterday = (now_kst().date() - timedelta(days=1))
    lookback_days = int(os.getenv("LOOKBACK_DAYS", os.getenv("TARGET_DAYS", "365")))
    default_start = yesterday - timedelta(days=max(1, lookback_days) - 1)
    p = argparse.ArgumentParser(description="Build UTM / Source-Medium numeric alert report")
    p.add_argument("--start-date", default=os.getenv("START_DATE", ymd(default_start)), help="YYYY-MM-DD. Full-load start date. Default: yesterday - 364 days")
    p.add_argument("--end-date", default=os.getenv("END_DATE", ymd(yesterday)), help="YYYY-MM-DD. Usually yesterday KST")
    p.add_argument("--lookback-days", type=int, default=lookback_days, help="First-load window and JSON retention days. Default 365")
    p.add_argument("--incremental", action="store_true", default=os.getenv("INCREMENTAL", "1").strip().lower() not in ("0", "false", "no", "n"), help="If output JSON exists, query only --end-date and merge. Default on")
    p.add_argument("--force-full", action="store_true", default=os.getenv("FORCE_FULL", "0").strip().lower() in ("1", "true", "yes", "y"), help="Ignore existing JSON and rebuild full --start-date ~ --end-date")
    p.add_argument("--project", default=DEFAULT_PROJECT)
    p.add_argument("--events-table", default=DEFAULT_TABLE)
    p.add_argument("--input-csv", default="", help="Optional local CSV input for test rendering; skips BigQuery/incremental")
    p.add_argument("--output-html", default="reports/utm_channel/index.html")
    p.add_argument("--output-json", default="reports/utm_channel/data/utm_channel_daily.json")
    p.add_argument("--output-csv", default="reports/utm_channel/data/utm_channel_daily.csv")
    p.add_argument("--alerts-json", default="reports/utm_channel/data/utm_channel_alerts.json")
    p.add_argument("--alerts-csv", default="reports/utm_channel/data/utm_channel_alerts.csv")
    p.add_argument("--meta-json", default="reports/utm_channel/data/meta.json")
    p.add_argument("--report-key", default="utm_channel")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    query_start = args.start_date
    query_end = args.end_date
    build_mode = "full"

    if args.input_csv:
        print(f"[UTM] Reading local sample CSV: {args.input_csv}")
        df = pd.read_csv(args.input_csv)
        build_mode = "local_csv"
    else:
        existing_df = pd.DataFrame()
        if args.incremental and not args.force_full:
            existing_df = load_existing_json(args.output_json)
            if not existing_df.empty:
                query_start = args.end_date
                query_end = args.end_date
                build_mode = "incremental_yesterday"

        print(f"[UTM] Build mode: {build_mode}")
        print(f"[UTM] Query range: {query_start} ~ {query_end}")
        print(f"[UTM] Events table: {args.events_table}")
        sql = build_ga4_bq_sql(args.events_table, query_start, query_end)
        queried_df = run_bigquery(sql, args.project)
        if build_mode == "incremental_yesterday":
            df = merge_incremental(existing_df, queried_df, query_start, query_end, args.lookback_days)
        else:
            df = queried_df

    df = normalize_metric_cols(df)
    effective_start, effective_end = _date_bounds_from_df(df)
    channel_df = build_channel_summary(df)
    alerts = build_numeric_alerts(df)
    quality_df = build_utm_quality(df)
    summary = summarize(df, effective_start, effective_end)

    meta = {
        "report_key": args.report_key,
        "title": "UTM / Source-Medium Numeric Alerts",
        "updated_at": now_kst().strftime("%Y-%m-%d %H:%M KST"),
        "period_start": effective_start,
        "period_end": effective_end,
        "query_start": query_start,
        "query_end": query_end,
        "build_mode": build_mode,
        "lookback_days": args.lookback_days,
        "ga4_property_id": DEFAULT_PROPERTY_ID,
        "source": "GA4 BigQuery Export",
        "events_table": args.events_table,
        "summary": summary,
        "alert_count": int(len(alerts)),
        "alert_rules": ALERT_RULES,
        "outputs": {
            "html": args.output_html,
            "json": args.output_json,
            "csv": args.output_csv,
            "alerts_json": args.alerts_json,
            "alerts_csv": args.alerts_csv,
            "meta": args.meta_json,
        },
    }

    for path in [args.output_csv, args.output_json, args.alerts_json, args.alerts_csv, args.meta_json]:
        ensure_parent(path)
    df.to_csv(args.output_csv, index=False, encoding="utf-8-sig")
    Path(args.output_json).write_text(json.dumps(clean_df_for_json(df), ensure_ascii=False, indent=2), encoding="utf-8")
    alerts.to_csv(args.alerts_csv, index=False, encoding="utf-8-sig")
    Path(args.alerts_json).write_text(json.dumps(clean_df_for_json(alerts), ensure_ascii=False, indent=2), encoding="utf-8")
    Path(args.meta_json).write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    render_html(df, alerts, channel_df, quality_df, meta, args.output_html)
    write_summary_json(args.report_key, meta, out_dir="reports")

    print(f"[UTM] mode={build_mode}, rows={len(df):,}, alerts={len(alerts):,}, sessions={summary['sessions']:,.0f}, revenue={summary['revenue']:,.0f}")
    print(f"[UTM] data range={effective_start} ~ {effective_end} | queried={query_start} ~ {query_end}")
    print(f"[UTM] wrote: {args.output_html}")


if __name__ == "__main__":
    main()
