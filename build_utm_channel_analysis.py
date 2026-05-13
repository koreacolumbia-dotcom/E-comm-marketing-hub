#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
UTM / Source-Medium Channel Analysis builder
Patched: BigQuery NET.URL_DECODE compatibility fix via URL_DECODE_SAFE JS UDF

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

CREATE TEMP FUNCTION URL_DECODE_SAFE(s STRING)
RETURNS STRING
LANGUAGE js AS r'''
  if (s === null || s === undefined) return null;
  try {{
    return decodeURIComponent(String(s).replace(/\+/g, ' '));
  }} catch (e) {{
    return String(s);
  }}
''';

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
    URL_DECODE_SAFE(REGEXP_EXTRACT(page_location, r'[?&]utm_source=([^&#]+)')) AS url_utm_source,
    URL_DECODE_SAFE(REGEXP_EXTRACT(page_location, r'[?&]utm_medium=([^&#]+)')) AS url_utm_medium,
    URL_DECODE_SAFE(REGEXP_EXTRACT(page_location, r'[?&]utm_campaign=([^&#]+)')) AS url_utm_campaign,
    URL_DECODE_SAFE(REGEXP_EXTRACT(page_location, r'[?&]utm_content=([^&#]+)')) AS url_utm_content,
    URL_DECODE_SAFE(REGEXP_EXTRACT(page_location, r'[?&]utm_term=([^&#]+)')) AS url_utm_term
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



CHANNEL_TONE_CLASSES = {
    "google": ("#fff8dd", "#8a5a00", "#f5d98b", "#fbbc04"),
    "meta": ("#eef2ff", "#4338ca", "#c7d2fe", "#6366f1"),
    "naver": ("#ecfdf3", "#047857", "#a7f3d0", "#03c75a"),
    "kakao": ("#fff9db", "#8a5a00", "#fde68a", "#fee500"),
    "criteo": ("#fff1f2", "#be123c", "#fecdd3", "#ef4444"),
    "mobon": ("#ecfeff", "#0f766e", "#99f6e4", "#14b8a6"),
    "edm": ("#eff6ff", "#1d4ed8", "#bfdbfe", "#3b82f6"),
    "lms": ("#eef2ff", "#5b21b6", "#ddd6fe", "#8b5cf6"),
    "awareness": ("#fff7ed", "#c2410c", "#fdba74", "#f97316"),
    "paid": ("#eff6ff", "#1d4ed8", "#bfdbfe", "#2563eb"),
    "organic": ("#f0fdf4", "#166534", "#bbf7d0", "#16a34a"),
    "owned": ("#f0fdfa", "#0f766e", "#99f6e4", "#14b8a6"),
    "sns": ("#fdf2f8", "#be185d", "#fbcfe8", "#ec4899"),
    "other": ("#f8fafc", "#475569", "#cbd5e1", "#94a3b8"),
}


def tone_key(label: Any) -> str:
    s = str(label or "").lower()
    if "google" in s:
        return "google"
    if any(x in s for x in ["meta", "facebook", "instagram", "ig", "fb"]):
        return "meta"
    if "naver" in s:
        return "naver"
    if "kakao" in s:
        return "kakao"
    if "criteo" in s:
        return "criteo"
    if "mobon" in s:
        return "mobon"
    if "edm" in s or "email" in s:
        return "edm"
    if "lms" in s:
        return "lms"
    if "awareness" in s or s.startswith("1."):
        return "awareness"
    if "paid" in s or s.startswith("2."):
        return "paid"
    if "organic" in s or s.startswith("3."):
        return "organic"
    if "official sns" in s or s.startswith("4.") or "sns" in s:
        return "sns"
    if "owned" in s or s.startswith("5."):
        return "owned"
    return "other"


def tone_class(label: Any) -> str:
    return f"tone-{tone_key(label)}"


def tone_style(label: Any) -> str:
    bg, fg, border, dot = CHANNEL_TONE_CLASSES[tone_key(label)]
    return f"--tone-bg:{bg};--tone-fg:{fg};--tone-border:{border};--tone-dot:{dot};"


def make_badge(text: Any, label: Any | None = None, extra: str = "") -> str:
    t = html_escape(text if text not in (None, "") else "-")
    cls = tone_class(label if label is not None else text)
    style = tone_style(label if label is not None else text)
    return f'<span class="badge {cls} {extra}" style="{style}">{t}</span>'


def make_legend_html() -> str:
    items = [
        ("Google", "google"), ("Meta", "meta"), ("Naver", "naver"), ("Kakao", "kakao"),
        ("EDM", "edm"), ("LMS", "lms"), ("Organic", "organic"), ("Paid", "paid"), ("Owned", "owned"), ("SNS", "sns")
    ]
    return "".join([f'<span class="legend-chip {tone_class(key)}" style="{tone_style(key)}">{html_escape(label)}</span>' for label, key in items])


def make_alert_cards(alerts: pd.DataFrame) -> str:
    if alerts.empty:
        return """
        <div class="alert-card neutral">
          <div class="alert-head"><span class="state-chip neutral">알림 없음</span></div>
          <div class="alert-title">전일 대비 유의미한 수치 변동이 없습니다.</div>
          <div class="alert-desc">2~3% 수준의 작은 변동은 노출하지 않습니다.</div>
        </div>
        """
    cards = []
    for _, r in alerts.head(8).iterrows():
        direction = str(r.get("direction"))
        cls = "up" if direction == "UP" else "down"
        arrow = "▲" if direction == "UP" else "▼"
        metric = str(r.get("metric"))
        delta = safe_num(r.get("delta"))
        rate = safe_num(r.get("delta_rate"))
        metric_txt = html_escape(r.get("metric_label"))
        scope_txt = html_escape(r.get("scope_label"))
        current_txt = html_escape(fmt_metric_value(metric, r.get("current")))
        previous_txt = html_escape(fmt_metric_value(metric, r.get("previous")))
        delta_txt = html_escape(fmt_delta(metric, r.get("delta")))
        diff_txt = f"{arrow} {abs(delta):.1f}%p" if metric in {"signup_cvr", "buy_cvr"} else f"{arrow} {abs(rate):.1f}%"
        tone = tone_style(str(r.get("scope_label")) + " " + str(r.get("scope")))
        cards.append(f"""
        <div class="alert-card {cls}" style="{tone}">
          <div class="alert-head">
            <span class="state-chip {cls}">{html_escape(diff_txt)}</span>
            <span class="badge-wrap">{make_badge(r.get('scope'), r.get('scope_label'))}</span>
          </div>
          <div class="alert-title">{metric_txt}</div>
          <div class="alert-desc">{scope_txt}</div>
          <div class="alert-values">
            <div class="value-main">{current_txt}</div>
            <div class="value-sub">전일 {previous_txt} · 증감 {delta_txt}</div>
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
          <td><span class="dir {cls}">{arrow} {html_escape('상승' if direction == 'UP' else '하락')}</span></td>
          <td>{make_badge(r.get('scope'), r.get('scope_label'))}</td>
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
        f"<tr><td>{make_badge(r['field'], r['field'])}</td><td class='num'>{fmt_int(r['missing_sessions'])}</td><td class='num'>{fmt_pct(r['missing_share'])}</td></tr>"
        for _, r in qdf.iterrows()
    )



def make_channel_cards(channel_df: pd.DataFrame) -> str:
    if channel_df.empty:
        return ""
    cards = []
    for _, r in channel_df.head(8).iterrows():
        label = r.get('media_family') if str(r.get('media_family', '')).strip() not in ('', 'Other') else r.get('channel_group')
        cards.append(f"""
        <div class="media-card {tone_class(label)}" style="{tone_style(label)}">
          <div class="media-top">
            {make_badge(r.get('channel_group'), r.get('channel_group'))}
            {make_badge(r.get('media_family'), label, 'media-badge')}
          </div>
          <div class="media-name">{html_escape(r.get('source','-'))} / {html_escape(r.get('medium','-'))}</div>
          <div class="media-metric">{fmt_money(r['revenue'])}</div>
          <div class="media-sub">세션 {fmt_int(r['sessions'])} · 사용자 {fmt_int(r['users'])}</div>
          <div class="media-sub">구매 CVR {fmt_pct(r['buy_cvr'])} · 구매자 {fmt_int(r['buyers'])}</div>
        </div>
        """)
    return "\n".join(cards)



def make_table_rows(df: pd.DataFrame, limit: int = 200) -> str:
    if df.empty:
        return '<tr><td colspan="19" class="empty">데이터가 없습니다.</td></tr>'
    show = df.sort_values(["event_dt", "revenue", "sessions"], ascending=[False, False, False]).head(limit)
    rows = []
    for _, r in show.iterrows():
        media_label = r.get('media_family') if str(r.get('media_family', '')).strip() not in ('', 'Other') else r.get('channel_group')
        rows.append(f"""
        <tr>
          <td>{html_escape(r.get('event_dt', '-'))}</td>
          <td>{make_badge(r.get('channel_group', '-'), r.get('channel_group', '-'))}</td>
          <td>{make_badge(r.get('media_family', '-'), media_label, 'media-badge')}</td>
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
    quality_payload = clean_df_for_json(quality_df)
    min_date, max_date = _date_bounds_from_df(df)
    alert_cards_html = make_alert_cards(alerts)
    legend_html = make_legend_html()
    tone_palette_json = json.dumps({k: {'bg': v[0], 'fg': v[1], 'border': v[2], 'dot': v[3]} for k, v in CHANNEL_TONE_CLASSES.items()}, ensure_ascii=False)

    html_template = '''<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>소스/매체 분석</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
  @import url("https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap");
  :root{
    --bg:#f6f8fc; --card:#ffffff; --line:#e7ebf3; --line-2:#eef2f7;
    --text:#101828; --muted:#667085; --muted-2:#98a2b3; --brand:#2962ff;
    --success:#16a34a; --danger:#ef4444; --shadow:0 10px 30px rgba(16,24,40,.05);
  }
  *{box-sizing:border-box}
  body{margin:0;font-family:"Inter","Pretendard","Apple SD Gothic Neo","Malgun Gothic",sans-serif;background:linear-gradient(180deg,#fafbfe 0%, #f6f8fc 100%);color:var(--text)}
  .app{max-width:1420px;margin:0 auto;padding:28px 24px 40px}
  .header{display:flex;justify-content:space-between;align-items:flex-start;gap:16px;margin-bottom:18px}
  .title{font-size:38px;line-height:1.1;margin:0;font-weight:900;letter-spacing:-.03em}
  .sub{margin:10px 0 0;color:var(--muted);font-size:14px;font-weight:600}
  .head-right{display:flex;align-items:center;gap:12px;color:var(--muted);font-size:13px;font-weight:700}
  .mini-icon{width:38px;height:38px;border-radius:14px;background:#fff;border:1px solid var(--line);display:flex;align-items:center;justify-content:center;box-shadow:var(--shadow)}
  .tool-card,.section-card{background:var(--card);border:1px solid var(--line);border-radius:24px;box-shadow:var(--shadow)}
  .tool-card{padding:16px 18px;margin-bottom:16px}
  .section-card{padding:20px;margin-top:16px}
  .section-head{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:14px}
  .section-title{font-size:20px;font-weight:900;letter-spacing:-.02em;margin:0}
  .toolbar{display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap}
  .toolbar-left,.toolbar-right,.quick-row,.tab-row,.filter-row{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
  .control,.btn{height:40px;border:1px solid var(--line);border-radius:14px;background:#fff;padding:0 14px;font-size:13px;font-weight:700;color:#344054}
  .btn{cursor:pointer;display:inline-flex;align-items:center;justify-content:center;gap:6px}
  .btn.ghost{background:#fff;color:#344054}
  .pill{height:34px;border-radius:12px;padding:0 12px;border:1px solid var(--line);background:#fff;font-size:12px;font-weight:800;color:var(--muted);display:inline-flex;align-items:center;gap:6px;cursor:pointer}
  .pill.active{background:#edf3ff;color:#1d4ed8;border-color:#cfe0ff}
  .grid-kpi{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:14px}
  .kpi{background:linear-gradient(180deg,#ffffff 0%, #fbfcff 100%);border:1px solid var(--line);border-radius:18px;padding:18px 16px;min-height:122px}
  .kpi .label{font-size:12px;color:var(--muted);font-weight:800;margin-bottom:10px}
  .kpi .value{font-size:34px;line-height:1.1;font-weight:900;letter-spacing:-.03em;color:#111827}
  .kpi .meta{margin-top:10px;font-size:12px;color:var(--muted-2);font-weight:700;line-height:1.45}
  .delta{margin-top:10px;display:inline-flex;align-items:center;gap:4px;font-size:12px;font-weight:900;padding:6px 10px;border-radius:999px;background:#e8f8ee;color:#118a3e}
  .alerts-grid{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:14px}
  .alert-card{border:1px solid var(--line);border-radius:18px;padding:16px;background:linear-gradient(180deg,#fff,#fcfdff);min-height:158px}
  .alert-card.up{border-color:#c9f0d7;background:linear-gradient(180deg,#f8fffb,#fff)}
  .alert-card.down{border-color:#ffd2d6;background:linear-gradient(180deg,#fff9fa,#fff)}
  .alert-head{display:flex;justify-content:space-between;align-items:flex-start;gap:8px}
  .state-chip{display:inline-flex;align-items:center;padding:5px 8px;border-radius:999px;font-size:11px;font-weight:900}
  .state-chip.up{background:#e8f8ee;color:#118a3e}.state-chip.down{background:#ffeff0;color:#d92d20}.state-chip.neutral{background:#f2f4f7;color:#667085}
  .alert-title{margin-top:14px;font-size:15px;font-weight:900;color:#101828}
  .alert-desc{margin-top:5px;font-size:12px;color:#98a2b3;font-weight:800}
  .value-main{margin-top:18px;font-size:29px;font-weight:900;color:#1d4ed8;letter-spacing:-.02em}
  .value-sub{margin-top:6px;font-size:12px;color:#667085;font-weight:700}
  .legend-row{display:flex;align-items:center;gap:14px;flex-wrap:wrap;margin-bottom:10px}
  .legend-chip,.badge{display:inline-flex;align-items:center;gap:7px;height:28px;padding:0 10px;border-radius:999px;background:var(--tone-bg);color:var(--tone-fg);border:1px solid var(--tone-border);font-size:12px;font-weight:800;white-space:nowrap}
  .legend-chip::before,.badge::before{content:"";width:8px;height:8px;border-radius:50%;background:var(--tone-dot)}
  .media-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:16px}
  .media-card{border-radius:20px;border:1px solid var(--tone-border);background:linear-gradient(180deg,#fff 0%, var(--tone-bg) 100%);padding:16px}
  .media-top{display:flex;justify-content:space-between;align-items:center;gap:10px}
  .media-brand{display:flex;align-items:center;gap:10px;font-weight:900;color:#1f2937}
  .media-logo{width:28px;height:28px;border-radius:10px;background:#fff;border:1px solid var(--tone-border);display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:900;color:var(--tone-fg)}
  .media-growth{font-size:12px;font-weight:900;color:var(--success)}
  .media-metric-label{margin-top:16px;font-size:12px;color:#98a2b3;font-weight:800}
  .media-metric-value{margin-top:4px;font-size:28px;font-weight:900;letter-spacing:-.02em}
  .media-row{display:flex;justify-content:space-between;gap:12px;margin-top:10px}
  .media-stat .k{font-size:11px;color:#98a2b3;font-weight:800}.media-stat .v{margin-top:4px;font-size:15px;font-weight:900;color:#111827}
  .spark-wrap{margin-top:10px;height:48px}
  .chart-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}
  .chart-card{background:#fff;border:1px solid var(--line);border-radius:20px;padding:18px}
  .chart-head{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:8px}
  .chart-title{font-size:18px;font-weight:900;margin:0}.chart-sub{font-size:12px;color:#98a2b3;font-weight:700}
  .chart-wrap{height:290px}
  .tab-btn{height:34px;padding:0 14px;border-radius:12px;border:1px solid var(--line);background:#fff;font-size:12px;font-weight:800;color:#667085;cursor:pointer}
  .tab-btn.active{background:#edf3ff;color:#1d4ed8;border-color:#cfe0ff}
  .table-card{border:1px solid var(--line);border-radius:20px;overflow:hidden;background:#fff}
  .table-scroll{overflow:auto;max-height:720px}
  table{width:100%;border-collapse:separate;border-spacing:0}
  thead th{position:sticky;top:0;background:#fcfdff;border-bottom:1px solid var(--line);padding:13px 12px;text-align:left;font-size:11px;font-weight:900;color:#667085;white-space:nowrap;z-index:1}
  tbody td{padding:13px 12px;border-bottom:1px solid var(--line-2);font-size:12px;font-weight:700;color:#344054;vertical-align:top}
  tbody tr:hover td{background:#fafcff}
  .rank{width:26px;color:#98a2b3;font-weight:900}
  .num{text-align:right;white-space:nowrap;font-variant-numeric:tabular-nums}
  .wide{min-width:150px;max-width:240px;word-break:break-word;line-height:1.45}
  .strong{font-weight:900;color:#101828}
  .empty{padding:40px;text-align:center;color:#98a2b3;font-weight:800}
  .help{font-size:11px;color:#98a2b3;font-weight:700}
  @media(max-width:1280px){.grid-kpi{grid-template-columns:repeat(3,minmax(0,1fr))}.alerts-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.media-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.chart-grid{grid-template-columns:1fr}}
  @media(max-width:760px){.app{padding:18px 14px 30px}.title{font-size:28px}.grid-kpi,.alerts-grid,.media-grid{grid-template-columns:1fr}.head-right{display:none}}
</style>
</head>
<body>
  <div class="app">
    <div class="header">
      <div>
        <h1 class="title">소스/매체 분석</h1>
        <p class="sub">다양한 소스/매체별 성과를 한눈에 확인하고 효율적인 마케팅 의사결정을 지원합니다.</p>
      </div>
      <div class="head-right">
        <div class="mini-icon">🔔</div>
        <div class="mini-icon">👤</div>
      </div>
    </div>

    <section class="tool-card">
      <div class="toolbar">
        <div class="toolbar-left">
          <input class="control" type="date" id="startDate" value="__MIN_DATE__" min="__MIN_DATE__" max="__MAX_DATE__" />
          <span style="color:#98a2b3;font-weight:800">~</span>
          <input class="control" type="date" id="endDate" value="__MAX_DATE__" min="__MIN_DATE__" max="__MAX_DATE__" />
          <select class="control" id="compareMode"><option>비교: 이전 기간</option><option>비교 안 함</option></select>
          <div class="quick-row">
            <button class="pill" id="d7Btn">7D</button>
            <button class="pill active" id="d30Btn">30D</button>
            <button class="pill" id="d90Btn">90D</button>
            <button class="pill" id="d365Btn">12M</button>
          </div>
        </div>
        <div class="toolbar-right">
          <button class="btn ghost" id="downloadBtn">내보내기</button>
        </div>
      </div>
    </section>

    <section class="section-card">
      <div class="section-head"><h2 class="section-title">핵심 지표</h2></div>
      <div class="grid-kpi" id="kpiGrid"></div>
    </section>

    <section class="section-card">
      <div class="section-head">
        <h2 class="section-title">수치 알림</h2>
        <button class="btn ghost">모두 보기</button>
      </div>
      <div class="alerts-grid">__ALERT_CARDS__</div>
    </section>

    <section class="section-card">
      <div class="section-head">
        <h2 class="section-title">매체별 한눈에 보기</h2>
        <div class="legend-row">__LEGEND_HTML__</div>
      </div>
      <div class="media-grid" id="mediaGrid"></div>
    </section>

    <section class="section-card">
      <div class="chart-grid">
        <div class="chart-card">
          <div class="chart-head">
            <div><h3 class="chart-title">채널별 매출</h3><div class="chart-sub">선택 기간 기준</div></div>
            <div class="tab-row"><button class="tab-btn active" id="chartRevenueBtn">금액</button><button class="tab-btn" id="chartSessionBtn">세션</button></div>
          </div>
          <div class="chart-wrap"><canvas id="revenueChart"></canvas></div>
        </div>
        <div class="chart-card">
          <div class="chart-head">
            <div><h3 class="chart-title">구매 전환율 (BUY CVR)</h3><div class="chart-sub">채널별 비교</div></div>
            <select class="control" id="lineMetricSel"><option value="buy_cvr">구매 전환율</option><option value="signup_cvr">회원가입 전환율</option></select>
          </div>
          <div class="chart-wrap"><canvas id="lineChart"></canvas></div>
        </div>
      </div>
    </section>

    <section class="section-card">
      <div class="section-head"><h2 class="section-title">상세 데이터</h2></div>
      <div class="toolbar" style="margin-bottom:14px">
        <div class="filter-row">
          <select class="control" id="viewMode"><option value="period">기본 표</option><option value="daily">일자별 요약</option><option value="detail">전체 상세</option></select>
          <select class="control" id="channelFilter"><option value="">전체 채널</option></select>
        </div>
        <div class="filter-row">
          <input class="control" type="search" id="searchBox" placeholder="채널/매체 검색" style="width:260px" />
          <button class="btn ghost" id="downloadDataBtn">내보내기</button>
        </div>
      </div>
      <div class="table-card"><div class="table-scroll" id="tableMount"></div></div>
      <div class="help" style="margin-top:10px">기간별 · 일자별 · 전체 상세 보기를 전환할 수 있습니다. 가독성을 위해 채널/매체 색상을 일관되게 적용했습니다.</div>
    </section>
  </div>
<script>
const rows = __ROWS__;
const tonePalette = __TONE_PALETTE__;
const allMinDate = "__MIN_DATE__";
const allMaxDate = "__MAX_DATE__";
let barMetric = "revenue";
let revenueChart = null;
let lineChart = null;

function n(v){const x=Number(v||0);return isFinite(x)?x:0;}
function money(v){return "₩"+Math.round(n(v)).toLocaleString("ko-KR");}
function num(v){return Math.round(n(v)).toLocaleString("ko-KR");}
function one(v){return n(v).toLocaleString("ko-KR",{minimumFractionDigits:1,maximumFractionDigits:1});}
function pct(v){return n(v).toLocaleString("ko-KR",{minimumFractionDigits:1,maximumFractionDigits:1})+"%";}
function esc(s){return String(s ?? "").replace(/[&<>"']/g,m=>({"&":"&amp;","<":"&lt;",">":"&gt;",""":"&quot;","'":"&#039;"}[m]));}
function toneKey(value){const s=String(value||"").toLowerCase(); if(s.includes("google")) return "google"; if(s.includes("meta")||s.includes("facebook")||s.includes("instagram")) return "meta"; if(s.includes("naver")) return "naver"; if(s.includes("kakao")) return "kakao"; if(s.includes("edm")||s.includes("email")) return "edm"; if(s.includes("organic")) return "organic"; if(s.includes("owned")) return "owned"; if(s.includes("sns")) return "sns"; if(s.includes("paid")||s.startsWith("2.")) return "paid"; return "other";}
function tone(value){return tonePalette[toneKey(value)] || tonePalette.other;}
function badge(text,value){const t=tone(value||text); return `<span class="badge" style="--tone-bg:${t.bg};--tone-fg:${t.fg};--tone-border:${t.border};--tone-dot:${t.dot}">${esc(text||"-")}</span>`;}
function dateAdd(d,days){const x=new Date(d+"T00:00:00"); x.setDate(x.getDate()+days); return x.toISOString().slice(0,10);}
function filteredRows(){const q=(document.getElementById("searchBox").value||"").toLowerCase(); const ch=document.getElementById("channelFilter").value; const s=document.getElementById("startDate").value||allMinDate; const e=document.getElementById("endDate").value||allMaxDate; return rows.filter(r=>(!s||r.event_dt>=s)&&(!e||r.event_dt<=e)&&(!ch||r.channel_group===ch)&&(!q||JSON.stringify(r).toLowerCase().includes(q)));}
function aggMetrics(arr){const sessions=arr.reduce((a,r)=>a+n(r.sessions),0), users=arr.reduce((a,r)=>a+n(r.users),0), signups=arr.reduce((a,r)=>a+n(r.signups),0), buyers=arr.reduce((a,r)=>a+n(r.buyers),0), purchase=arr.reduce((a,r)=>a+n(r.purchase),0), revenue=arr.reduce((a,r)=>a+n(r.revenue),0); const pv=arr.reduce((a,r)=>a+n(r.pv_per_user)*n(r.users),0); return {sessions, users, signups, signup_cvr:sessions?signups/sessions*100:0, buyers, buy_cvr:sessions?buyers/sessions*100:0, purchase, revenue, aov_per_buyer:buyers?revenue/buyers:0, pv_per_user:users?pv/users:0};}
function groupBy(arr, keys){const m=new Map(); arr.forEach(r=>{const k=keys.map(x=>r[x]??"-").join("||"); if(!m.has(k)) m.set(k,{keys:Object.fromEntries(keys.map(x=>[x,r[x]??"-"])), rows:[]}); m.get(k).rows.push(r);}); return [...m.values()].map(g=>({...g.keys,...aggMetrics(g.rows)}));}
function preferredMediaName(r){const raw=String(r.media_family||"").trim(); if(raw && raw!=="Other") return raw; const cg=String(r.channel_group||""); if(cg.includes("Organic")) return "Organic"; if(cg.includes("Owned")) return "Owned"; if(cg.includes("Official SNS")) return "SNS"; if(cg.includes("Paid")) return "Paid"; return cg.replace(/^\d+\.\s*/,"") || "Etc";}
function logoText(name){const s=String(name||""); if(s==="Google") return "G"; if(s==="Meta") return "M"; if(s==="Naver") return "N"; if(s==="Kakao") return "K"; if(s==="EDM") return "✉"; if(s==="Organic") return "O"; if(s==="Owned") return "Ow"; if(s==="SNS") return "S"; return s.slice(0,1).toUpperCase();}
function makeSparkSvg(values,color){if(!values.length) values=[0]; const w=100,h=30,pad=2; const max=Math.max(...values,1), min=Math.min(...values,0); const range=Math.max(max-min,1); const pts=values.map((v,i)=>{const x=pad + (w-pad*2)*(values.length===1?0:i/(values.length-1)); const y=h-pad - ((v-min)/range)*(h-pad*2); return [x,y];}); const d=pts.map((p,i)=>(i?"L":"M")+p[0].toFixed(1)+","+p[1].toFixed(1)).join(" "); return `<svg viewBox="0 0 ${w} ${h}" width="100%" height="100%"><path d="${d}" fill="none" stroke="${color}" stroke-width="2.3" stroke-linecap="round" stroke-linejoin="round"/></svg>`;}

function renderKpis(arr){const s=aggMetrics(arr); const cards=[
  {label:"세션", value:num(s.sessions), meta:`Users ${num(s.users)}`, delta:pct(s.buy_cvr)},
  {label:"구매 전환율", value:pct(s.buy_cvr), meta:`Buyers ${num(s.buyers)}`, delta:pct(s.signup_cvr)},
  {label:"구매 전환율 (BUY)", value:pct(s.buy_cvr), meta:`구매건수 ${num(s.purchase)}`, delta:pct(s.buy_cvr)},
  {label:"매출", value:money(s.revenue), meta:`AOV ${money(s.aov_per_buyer)}`, delta:pct(s.signup_cvr)},
  {label:"선택 세션", value:num(s.sessions), meta:`Users ${num(s.users)} / PV ${one(s.pv_per_user)}`, delta:pct(s.buy_cvr)},
  {label:"선택 회원 CVR", value:pct(s.signup_cvr), meta:`Signup ${num(s.signups)}`, delta:pct(s.signup_cvr)}
];
  document.getElementById("kpiGrid").innerHTML = cards.map(c=>`<div class="kpi"><div class="label">${c.label}</div><div class="value">${c.value}</div><div class="meta">${c.meta}</div><div class="delta">▲ ${c.delta}</div></div>`).join("");
}

function renderMediaGrid(arr){const dailyByMedia = groupBy(arr.map(r=>({...r, media_name:preferredMediaName(r)})), ["media_name","event_dt"]); const trendMap = {}; dailyByMedia.forEach(r=>{const k=r.media_name; if(!trendMap[k]) trendMap[k]=[]; trendMap[k].push({date:r.event_dt,revenue:r.revenue});}); Object.keys(trendMap).forEach(k=>trendMap[k].sort((a,b)=>String(a.date).localeCompare(String(b.date)))); const grouped = groupBy(arr.map(r=>({...r, media_name:preferredMediaName(r)})), ["media_name"]).sort((a,b)=>n(b.revenue)-n(a.revenue)); document.getElementById("mediaGrid").innerHTML = grouped.slice(0,8).map(r=>{const name=r.media_name; const t=tone(name); const prev = (trendMap[name]||[]).slice(-14,-7).reduce((a,x)=>a+n(x.revenue),0); const curr = (trendMap[name]||[]).slice(-7).reduce((a,x)=>a+n(x.revenue),0); const change = prev?((curr-prev)/prev*100):0; return `<div class="media-card" style="--tone-bg:${t.bg};--tone-fg:${t.fg};--tone-border:${t.border};--tone-dot:${t.dot}"><div class="media-top"><div class="media-brand"><div class="media-logo">${logoText(name)}</div><div>${esc(name)}</div></div><div class="media-growth">${change>=0?"▲":"▼"} ${Math.abs(change).toFixed(1)}%</div></div><div class="media-metric-label">매출</div><div class="media-metric-value">${money(r.revenue)}</div><div class="media-row"><div class="media-stat"><div class="k">세션</div><div class="v">${num(r.sessions)}</div></div><div class="media-stat"><div class="k">구매 전환율</div><div class="v">${pct(r.buy_cvr)}</div></div></div><div class="spark-wrap">${makeSparkSvg((trendMap[name]||[]).slice(-14).map(x=>n(x.revenue)), t.dot)}</div></div>`;}).join("");}

function buildPeriodTable(arr){const grouped = groupBy(arr.map(r=>({...r, media_name:preferredMediaName(r)})), ["media_name","source","medium","campaign"]).sort((a,b)=>n(b.revenue)-n(a.revenue)||n(b.sessions)-n(a.sessions)); return `<table><thead><tr><th class="rank">#</th><th>채널</th><th>소스 / 매체</th><th>캠페인</th><th class="num">세션</th><th class="num">매출</th><th class="num">구매 전환율</th><th class="num">AOV</th><th class="num">구매수</th><th class="num">회원가입 CVR</th></tr></thead><tbody>${grouped.length?grouped.map((r,i)=>`<tr><td class="rank">${i+1}</td><td>${badge(r.media_name,r.media_name)}</td><td>${esc(r.source)} / ${esc(r.medium)}</td><td class="wide">${esc(r.campaign)}</td><td class="num">${num(r.sessions)}</td><td class="num strong">${money(r.revenue)}</td><td class="num">${pct(r.buy_cvr)}</td><td class="num">${money(r.aov_per_buyer)}</td><td class="num">${num(r.buyers)}</td><td class="num">${pct(r.signup_cvr)}</td></tr>`).join(""):`<tr><td colspan="10" class="empty">조건에 맞는 데이터가 없습니다.</td></tr>`}</tbody></table>`;}
function buildDailyTable(arr){const grouped = groupBy(arr, ["event_dt","channel_group"]).sort((a,b)=>String(b.event_dt).localeCompare(String(a.event_dt))||n(b.revenue)-n(a.revenue)); return `<table><thead><tr><th>일자</th><th>채널</th><th class="num">세션</th><th class="num">사용자</th><th class="num">회원가입</th><th class="num">회원가입 CVR</th><th class="num">구매수</th><th class="num">구매 전환율</th><th class="num">매출</th><th class="num">AOV</th></tr></thead><tbody>${grouped.length?grouped.map(r=>`<tr><td>${esc(r.event_dt)}</td><td>${badge(r.channel_group,r.channel_group)}</td><td class="num">${num(r.sessions)}</td><td class="num">${num(r.users)}</td><td class="num">${num(r.signups)}</td><td class="num">${pct(r.signup_cvr)}</td><td class="num">${num(r.buyers)}</td><td class="num">${pct(r.buy_cvr)}</td><td class="num strong">${money(r.revenue)}</td><td class="num">${money(r.aov_per_buyer)}</td></tr>`).join(""):`<tr><td colspan="10" class="empty">조건에 맞는 데이터가 없습니다.</td></tr>`}</tbody></table>`;}
function buildDetailTable(arr){const sorted = [...arr].sort((a,b)=>n(b.revenue)-n(a.revenue)||n(b.sessions)-n(a.sessions)); return `<table><thead><tr><th class="rank">#</th><th>채널</th><th>소스 / 매체</th><th>캠페인</th><th class="num">세션</th><th class="num">매출</th><th class="num">구매 전환율</th><th class="num">AOV</th><th class="num">구매수</th><th class="num">회원가입 CVR</th><th class="num">사용자</th><th class="num">회원가입</th><th class="num">구매건수</th><th class="num">가입자 평균 PV</th><th class="num">PV/사용자</th></tr></thead><tbody>${sorted.length?sorted.map((r,i)=>`<tr><td class="rank">${i+1}</td><td>${badge(preferredMediaName(r),preferredMediaName(r))}</td><td>${esc(r.source)} / ${esc(r.medium)}</td><td class="wide">${esc(r.campaign)}</td><td class="num">${num(r.sessions)}</td><td class="num strong">${money(r.revenue)}</td><td class="num">${pct(r.buy_cvr)}</td><td class="num">${money(r.aov_per_buyer)}</td><td class="num">${num(r.buyers)}</td><td class="num">${pct(r.signup_cvr)}</td><td class="num">${num(r.users)}</td><td class="num">${num(r.signups)}</td><td class="num">${num(r.purchase)}</td><td class="num">${one(r.avg_signup_user_pv)}</td><td class="num">${one(r.pv_per_user)}</td></tr>`).join(""):`<tr><td colspan="15" class="empty">조건에 맞는 데이터가 없습니다.</td></tr>`}</tbody></table>`;}
function renderTableSection(arr){const mode=document.getElementById("viewMode").value; const mount=document.getElementById("tableMount"); if(mode==="daily") mount.innerHTML=buildDailyTable(arr); else if(mode==="detail") mount.innerHTML=buildDetailTable(arr); else mount.innerHTML=buildPeriodTable(arr);}
function renderCharts(arr){const grouped = groupBy(arr.map(r=>({...r, media_name:preferredMediaName(r)})), ["media_name"]).sort((a,b)=>n(b[barMetric])-n(a[barMetric])); const labels = grouped.map(r=>r.media_name); const barData = grouped.map(r=>n(r[barMetric])); const barColors = labels.map(l=>tone(l).dot); if(revenueChart) revenueChart.destroy(); revenueChart = new Chart(document.getElementById("revenueChart"), {type:"bar", data:{labels, datasets:[{data:barData, backgroundColor:barColors, borderRadius:8, maxBarThickness:36}]}, options:{responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}, tooltip:{callbacks:{label:(ctx)=>barMetric==="revenue"? "매출 "+money(ctx.raw):"세션 "+num(ctx.raw)}}}, scales:{x:{grid:{display:false}, ticks:{font:{size:11,weight:"700"}, color:"#667085"}}, y:{grid:{color:"#eef2f7"}, ticks:{color:"#98a2b3", callback:(v)=>barMetric==="revenue"? money(v): num(v)}}}}}); const metric = document.getElementById("lineMetricSel").value; const lineGrouped = groupBy(arr.map(r=>({...r, media_name:preferredMediaName(r)})), ["media_name"]).sort((a,b)=>n(b[metric])-n(a[metric])); const lineLabels = lineGrouped.map(r=>r.media_name); const lineData = lineGrouped.map(r=>n(r[metric])); if(lineChart) lineChart.destroy(); lineChart = new Chart(document.getElementById("lineChart"), {type:"line", data:{labels:lineLabels, datasets:[{data:lineData, borderColor:"#3b82f6", backgroundColor:"rgba(59,130,246,.1)", pointBackgroundColor:"#3b82f6", pointRadius:4, tension:.35, fill:false}]}, options:{responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}, tooltip:{callbacks:{label:(ctx)=> (metric==="buy_cvr"?"구매 전환율 ":"회원가입 전환율 ")+pct(ctx.raw)}}}, scales:{x:{grid:{display:false}, ticks:{font:{size:11,weight:"700"}, color:"#667085"}}, y:{grid:{color:"#eef2f7"}, ticks:{color:"#98a2b3", callback:(v)=>pct(v)}}}}});}
function renderAll(){const arr=filteredRows(); renderKpis(arr); renderMediaGrid(arr); renderCharts(arr); renderTableSection(arr);}
[...new Set(rows.map(r=>r.channel_group).filter(Boolean))].sort().forEach(ch=>{const o=document.createElement("option"); o.value=ch; o.textContent=ch; document.getElementById("channelFilter").appendChild(o);});
document.getElementById("startDate").value = dateAdd(allMaxDate,-29);
["startDate","endDate","channelFilter","viewMode","lineMetricSel"].forEach(id=>document.getElementById(id).addEventListener("change",renderAll));
document.getElementById("searchBox").addEventListener("input",renderAll);
document.getElementById("d7Btn").onclick=()=>{document.getElementById("startDate").value=dateAdd(allMaxDate,-6);document.getElementById("endDate").value=allMaxDate;document.querySelectorAll(".pill").forEach(x=>x.classList.remove("active"));document.getElementById("d7Btn").classList.add("active");renderAll();};
document.getElementById("d30Btn").onclick=()=>{document.getElementById("startDate").value=dateAdd(allMaxDate,-29);document.getElementById("endDate").value=allMaxDate;document.querySelectorAll(".pill").forEach(x=>x.classList.remove("active"));document.getElementById("d30Btn").classList.add("active");renderAll();};
document.getElementById("d90Btn").onclick=()=>{document.getElementById("startDate").value=dateAdd(allMaxDate,-89);document.getElementById("endDate").value=allMaxDate;document.querySelectorAll(".pill").forEach(x=>x.classList.remove("active"));document.getElementById("d90Btn").classList.add("active");renderAll();};
document.getElementById("d365Btn").onclick=()=>{document.getElementById("startDate").value=allMinDate;document.getElementById("endDate").value=allMaxDate;document.querySelectorAll(".pill").forEach(x=>x.classList.remove("active"));document.getElementById("d365Btn").classList.add("active");renderAll();};
document.getElementById("chartRevenueBtn").onclick=()=>{barMetric="revenue";document.getElementById("chartRevenueBtn").classList.add("active");document.getElementById("chartSessionBtn").classList.remove("active");renderAll();};
document.getElementById("chartSessionBtn").onclick=()=>{barMetric="sessions";document.getElementById("chartSessionBtn").classList.add("active");document.getElementById("chartRevenueBtn").classList.remove("active");renderAll();};
document.getElementById("downloadBtn").onclick=()=>{const blob=new Blob([JSON.stringify(rows,null,2)],{type:"application/json"});const a=document.createElement("a");a.href=URL.createObjectURL(blob);a.download="utm_channel_rows.json";a.click();};
document.getElementById("downloadDataBtn").onclick=()=>{const blob=new Blob([JSON.stringify(filteredRows(),null,2)],{type:"application/json"});const a=document.createElement("a");a.href=URL.createObjectURL(blob);a.download="utm_channel_filtered.json";a.click();};
renderAll();
</script>
</body>
</html>'''

    replacements = {
        '__MIN_DATE__': html_escape(min_date),
        '__MAX_DATE__': html_escape(max_date),
        '__ROWS__': json.dumps(payload, ensure_ascii=False),
        '__TONE_PALETTE__': tone_palette_json,
        '__ALERT_CARDS__': alert_cards_html,
        '__LEGEND_HTML__': legend_html,
    }
    html_doc = html_template
    for k, v in replacements.items():
        html_doc = html_doc.replace(k, v)
    ensure_parent(out_path)
    Path(out_path).write_text(html_doc, encoding='utf-8')

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
