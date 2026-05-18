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

FINAL_PATCH_VERSION = "2026-05-18-ui-data-render-fix"

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
    return decodeURIComponent(String(s).replace(/\\+/g, ' '));
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
    payload = clean_df_for_json(df)
    min_date, max_date = _date_bounds_from_df(df)
    alert_cards_html = make_alert_cards(alerts)
    legend_html = make_legend_html()
    tone_palette_json = json.dumps({k: {'bg': v[0], 'fg': v[1], 'border': v[2], 'dot': v[3]} for k, v in CHANNEL_TONE_CLASSES.items()}, ensure_ascii=False)
    updated_at = html_escape(meta.get("updated_at", now_kst().strftime("%Y-%m-%d %H:%M KST")))

    html_template = r'''<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>소스/매체 분석</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200;400;600;800&family=Noto+Sans+KR:wght@400;500;700;800;900&display=swap');
  :root{
    --report-max:1480px;
    --motion-ease:cubic-bezier(.2,.8,.2,1);
    --bg-top:#f8fafc;
    --bg-bottom:#eef2f7;
    --card:#ffffff;
    --line:rgba(148,163,184,.22);
    --line-strong:rgba(148,163,184,.34);
    --ink:#0f172a;
    --muted:#64748b;
    --muted-2:#94a3b8;
    --brand:#0f172a;
    --blue:#2563eb;
    --blue-soft:#eff6ff;
    --green:#10b981;
    --red:#ef4444;
    --shadow-soft:0 6px 18px rgba(15,23,42,.04);
    --shadow-card:0 12px 28px rgba(15,23,42,.06);
    --shadow-hover:0 18px 40px rgba(15,23,42,.08);
  }
  *{box-sizing:border-box}
  html{scroll-behavior:smooth}
  body{
    margin:0;
    min-height:100vh;
    font-family:'Plus Jakarta Sans','Noto Sans KR','Malgun Gothic','Apple SD Gothic Neo',system-ui,-apple-system,'Segoe UI',Roboto,Arial,sans-serif;
    color:var(--ink);
    background:
      radial-gradient(circle at 12% 2%, rgba(59,130,246,.10), transparent 24%),
      radial-gradient(circle at 85% 8%, rgba(16,185,129,.08), transparent 22%),
      linear-gradient(180deg,var(--bg-top) 0%,var(--bg-bottom) 100%);
    -webkit-font-smoothing:antialiased;
    text-rendering:optimizeLegibility;
  }
  .layout{display:block;width:100%;min-height:100vh}
  .main{width:100%;max-width:var(--report-max);margin:0 auto;padding:30px 28px 46px}

  .topbar{display:flex;justify-content:space-between;align-items:flex-start;gap:18px;margin-bottom:18px;animation:cardRise .55s var(--motion-ease) both}
  .page-title{margin:0;font-size:42px;line-height:1.08;font-weight:950;letter-spacing:-.055em;color:#0b1220}
  .page-sub{margin:10px 0 0;font-size:14px;color:var(--muted);font-weight:700}
  .top-actions{display:flex;align-items:center;gap:10px}
  .icon-btn,.user-pill{height:40px;border:1px solid rgba(148,163,184,.25);background:rgba(255,255,255,.9);box-shadow:var(--shadow-soft);backdrop-filter:blur(14px)}
  .icon-btn{width:40px;border-radius:999px;display:flex;align-items:center;justify-content:center;cursor:pointer;transition:all .22s var(--motion-ease)}
  .user-pill{border-radius:999px;padding:0 14px;display:flex;align-items:center;gap:8px;font-size:13px;font-weight:900;color:#475569}
  .icon-btn:hover,.user-pill:hover{transform:translateY(-1px);box-shadow:var(--shadow-hover)}

  .panel{
    position:relative;
    overflow:hidden;
    background:rgba(255,255,255,.86);
    border:1px solid rgba(148,163,184,.22);
    border-radius:26px;
    box-shadow:var(--shadow-card);
    backdrop-filter:blur(16px);
    animation:cardRise .7s var(--motion-ease) both;
    transform-origin:center bottom;
  }
  .panel::after{content:'';position:absolute;inset:0 auto auto 0;width:100%;height:1px;background:linear-gradient(90deg,rgba(255,255,255,.95),rgba(255,255,255,.16));pointer-events:none}
  .panel:hover{box-shadow:var(--shadow-hover)}
  .filter-panel{padding:18px;margin-bottom:16px}
  .card-section{padding:20px;margin-bottom:16px}
  .filter-row,.table-tools,.section-head{display:flex;justify-content:space-between;gap:12px;align-items:center;flex-wrap:wrap}
  .left-controls,.right-controls,.range-buttons,.section-tools{display:flex;align-items:center;gap:10px;flex-wrap:wrap}

  .control,.btn,.pill,.tab-btn{
    font-family:inherit;
    border:1px solid rgba(148,163,184,.25);
    background:#fff;
    color:#475569;
    font-weight:900;
    box-shadow:var(--shadow-soft);
    transition:all .22s var(--motion-ease);
  }
  .control,.btn{height:42px;border-radius:14px;padding:0 14px;font-size:13px;outline:none}
  .control.date{min-width:136px}
  .control:hover,.btn:hover,.pill:hover,.tab-btn:hover{transform:translateY(-1px);box-shadow:0 12px 28px rgba(15,23,42,.08);border-color:rgba(59,130,246,.22)}
  .control:focus{border-color:rgba(59,130,246,.42);box-shadow:0 0 0 4px rgba(59,130,246,.10)}
  .btn{display:inline-flex;align-items:center;justify-content:center;gap:6px;cursor:pointer}
  .pill,.tab-btn{height:34px;padding:0 13px;border-radius:999px;font-size:12px;cursor:pointer}
  .pill.active,.tab-btn.active{background:#0f172a;color:#fff;border-color:#0f172a;box-shadow:0 14px 32px rgba(15,23,42,.16)}

  .section-title{margin:0;font-size:16px;font-weight:950;letter-spacing:-.025em;color:#0f172a;display:flex;align-items:center;gap:6px}
  .info-badge{width:18px;height:18px;border-radius:50%;background:#f1f5f9;color:#94a3b8;font-size:11px;font-weight:900;display:inline-flex;align-items:center;justify-content:center}

  .kpi-grid{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:14px}
  .kpi-card{
    position:relative;overflow:hidden;
    border:1px solid rgba(148,163,184,.20);
    border-radius:22px;
    padding:18px 17px 16px;
    min-height:136px;
    background:linear-gradient(180deg,rgba(255,255,255,.96) 0%,rgba(248,250,252,.96) 100%);
    box-shadow:var(--shadow-soft);
    transition:transform .24s var(--motion-ease), box-shadow .24s var(--motion-ease), border-color .24s var(--motion-ease);
  }
  .kpi-card:before{content:'';position:absolute;inset:-40% auto auto -20%;width:60%;height:180%;background:linear-gradient(90deg,transparent,rgba(255,255,255,.62),transparent);transform:rotate(14deg);animation:shineSweep 4.2s linear infinite;pointer-events:none}
  .kpi-card:hover{transform:translateY(-6px) scale(1.01);box-shadow:0 22px 44px rgba(15,23,42,.08);border-color:rgba(59,130,246,.22)}
  .kpi-label{font-size:12px;font-weight:900;color:var(--muted);margin-bottom:13px}
  .kpi-value{font-size:22px;font-weight:950;letter-spacing:-.035em;color:#0b1220;animation:numberPop .8s var(--motion-ease) both}
  .kpi-sub{margin-top:9px;font-size:12px;line-height:1.45;color:var(--muted-2);font-weight:750}
  .kpi-delta{margin-top:11px;display:inline-flex;align-items:center;gap:4px;font-size:12px;font-weight:950;padding:5px 9px;border-radius:999px;background:#f8fafc}
  .kpi-delta.up{color:#047857;background:#ecfdf5}.kpi-delta.down{color:#be123c;background:#fff1f2}.kpi-delta.flat{color:#64748b;background:#f1f5f9}

  .alerts-grid{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:14px}
  .alert-card{
    position:relative;overflow:hidden;
    border:1px solid rgba(16,185,129,.22);
    border-radius:22px;
    padding:17px;
    background:linear-gradient(180deg,rgba(255,255,255,.96),rgba(248,250,252,.9));
    box-shadow:var(--shadow-soft);
    transition:all .24s var(--motion-ease);
  }
  .alert-card:hover{transform:translateY(-4px);box-shadow:var(--shadow-hover)}
  .alert-card.up{border-color:rgba(16,185,129,.28)}.alert-card.down{border-color:rgba(239,68,68,.28)}
  .alert-head{display:flex;justify-content:space-between;align-items:flex-start;gap:8px}
  .state-chip{display:inline-flex;align-items:center;height:26px;padding:0 10px;border-radius:999px;font-size:11px;font-weight:950;border:1px solid transparent}
  .state-chip.up{background:#ecfdf5;color:#047857;border-color:#bbf7d0}.state-chip.down{background:#fff1f2;color:#be123c;border-color:#fecdd3}.state-chip.neutral{background:#f1f5f9;color:#64748b;border-color:#e2e8f0}
  .alert-title{margin-top:14px;font-size:15px;font-weight:950;color:#0f172a}
  .alert-desc{margin-top:6px;font-size:12px;color:var(--muted-2);font-weight:850;line-height:1.35;min-height:32px}
  .value-main{margin-top:16px;font-size:30px;font-weight:950;letter-spacing:-.045em;color:#1d4ed8;animation:numberPop .8s var(--motion-ease) both}
  .value-sub{margin-top:6px;font-size:12px;color:#64748b;font-weight:750}

  .legend-row{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
  .legend-chip,.badge{display:inline-flex;align-items:center;gap:7px;height:29px;padding:0 11px;border-radius:999px;background:var(--tone-bg);color:var(--tone-fg);border:1px solid var(--tone-border);font-size:11px;font-weight:950;white-space:nowrap;box-shadow:0 6px 18px rgba(15,23,42,.04), inset 0 1px 0 rgba(255,255,255,.65)}
  .legend-chip::before,.badge::before{content:'';width:8px;height:8px;border-radius:50%;background:var(--tone-dot)}

  .media-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px;margin-top:14px}
  .media-card{
    position:relative;overflow:hidden;
    border:1px solid var(--tone-border);
    border-radius:24px;
    padding:17px;
    background:linear-gradient(180deg,rgba(255,255,255,.98) 0%,var(--tone-bg) 100%);
    box-shadow:var(--shadow-soft);
    transition:all .24s var(--motion-ease);
    animation:cardRise .7s var(--motion-ease) both;
  }
  .media-card:after{content:'';position:absolute;right:-26px;top:-26px;width:110px;height:110px;border-radius:999px;background:var(--tone-dot);opacity:.075;filter:blur(.3px)}
  .media-card:hover{transform:translateY(-4px);box-shadow:var(--shadow-hover)}
  .media-head{display:flex;justify-content:space-between;align-items:flex-start;gap:8px;position:relative;z-index:1}
  .media-brand{display:flex;align-items:center;gap:9px;font-size:14px;font-weight:950;color:#334155}
  .media-logo{width:32px;height:32px;border-radius:12px;display:flex;align-items:center;justify-content:center;background:#fff;border:1px solid var(--tone-border);color:var(--tone-fg);font-size:13px;font-weight:950;box-shadow:0 8px 18px rgba(15,23,42,.05);overflow:hidden}.media-logo svg{width:18px;height:18px;display:block}.media-logo .abbr{font-size:10px;font-weight:950;letter-spacing:-.02em}
  .media-growth{font-size:12px;font-weight:950;color:#047857;padding:5px 8px;border-radius:999px;background:rgba(16,185,129,.09)}
  .media-metric-label{margin-top:14px;font-size:11px;font-weight:900;color:#94a3b8}
  .media-metric-value{margin-top:4px;font-size:17px;font-weight:950;color:#0f172a;letter-spacing:-.03em}
  .media-stats{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:14px}
  .media-stat-label{font-size:11px;font-weight:850;color:#94a3b8}
  .media-stat-value{margin-top:4px;font-size:15px;font-weight:950;color:#111827}
  .spark-wrap{height:42px;margin-top:12px;padding-top:2px}

  .chart-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px}
  .chart-card{padding:20px}
  .chart-head{display:flex;justify-content:space-between;align-items:flex-start;gap:10px;margin-bottom:12px}
  .chart-title{margin:0;font-size:17px;font-weight:950;color:#0f172a;letter-spacing:-.02em}
  .chart-sub{margin-top:3px;font-size:12px;font-weight:750;color:#94a3b8}
  .chart-wrap{height:286px}

  .table-card{border:1px solid rgba(148,163,184,.22);border-radius:22px;background:rgba(255,255,255,.88);overflow:hidden;box-shadow:inset 0 1px 0 rgba(255,255,255,.8)}
  .table-scroll{overflow:auto;max-height:760px}
  table{width:100%;border-collapse:separate;border-spacing:0}
  thead th{position:sticky;top:0;background:rgba(248,250,252,.96);backdrop-filter:blur(8px);border-bottom:1px solid rgba(148,163,184,.24);padding:13px 12px;text-align:left;font-size:11px;font-weight:950;color:#64748b;white-space:nowrap;z-index:1}
  tbody td{padding:12px;border-bottom:1px solid rgba(226,232,240,.72);font-size:12px;font-weight:750;color:#475569;vertical-align:top}
  tbody tr{transition:background .18s var(--motion-ease)}
  tbody tr:hover td{background:#f8fbff}
  tbody tr:nth-child(even) td{background:rgba(248,250,252,.45)}
  .rank{width:30px;color:#94a3b8;font-weight:950}
  .num{text-align:right;white-space:nowrap;font-variant-numeric:tabular-nums}
  .wide{min-width:160px;max-width:260px;word-break:break-word;line-height:1.45}
  .strong{font-weight:950;color:#0f172a}
  .empty{padding:40px;text-align:center;color:#94a3b8;font-weight:850}
  .table-footer{display:flex;justify-content:space-between;align-items:center;gap:12px;padding:12px 4px 0;color:#94a3b8;font-size:12px;font-weight:850}
  .pager{display:flex;align-items:center;gap:8px}
  .pager-btn{width:30px;height:30px;border-radius:10px;border:1px solid rgba(148,163,184,.25);background:#fff;color:#94a3b8;display:flex;align-items:center;justify-content:center;box-shadow:var(--shadow-soft)}
  .pager-current{min-width:30px;height:30px;border-radius:10px;background:#0f172a;color:#fff;display:flex;align-items:center;justify-content:center;padding:0 10px;font-weight:950}
  .rows-select{height:32px;padding:0 8px;border:1px solid rgba(148,163,184,.25);border-radius:10px;background:#fff;color:#64748b;font-size:12px;font-weight:850}


  /* UTM media delta + alert split patch · 2026-05-18 */
  .alerts-wrap{display:grid;grid-template-columns:1fr 1fr;gap:14px;width:100%}
  .alert-lane{border:1px solid rgba(148,163,184,.20);border-radius:22px;background:rgba(255,255,255,.64);padding:14px;min-height:154px}
  .alert-lane-head{display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:12px}
  .alert-lane-title{display:flex;align-items:center;gap:8px;font-size:13px;font-weight:950;color:#0f172a;letter-spacing:-.02em}
  .alert-lane-count{height:24px;min-width:24px;padding:0 8px;border-radius:999px;display:inline-flex;align-items:center;justify-content:center;font-size:11px;font-weight:950;background:#f8fafc;color:#64748b;border:1px solid rgba(148,163,184,.22)}
  .alert-lane.up{border-color:rgba(16,185,129,.24);background:linear-gradient(180deg,rgba(236,253,245,.62),rgba(255,255,255,.74))}
  .alert-lane.down{border-color:rgba(239,68,68,.24);background:linear-gradient(180deg,rgba(255,241,242,.62),rgba(255,255,255,.74))}
  .alert-lane.up .alert-lane-title{color:#047857}.alert-lane.down .alert-lane-title{color:#be123c}
  .alert-list{display:grid;grid-template-columns:1fr;gap:10px}
  .alert-list .alert-card{min-height:0;padding:14px;border-radius:18px}
  .alert-list .alert-title{margin-top:10px}.alert-list .value-main{font-size:22px;margin-top:10px}.alert-list .alert-desc{min-height:auto}
  .media-growth{display:inline-flex;align-items:center;gap:4px;white-space:nowrap}
  .media-growth.up{color:#047857;background:rgba(16,185,129,.11)}
  .media-growth.down{color:#be123c;background:rgba(239,68,68,.10)}
  .media-growth.flat{color:#64748b;background:#f1f5f9}
  .media-delta-sub{margin-top:5px;font-size:11px;font-weight:850;color:#94a3b8}

  @keyframes cardRise{from{opacity:0;transform:translateY(26px) scale(.985)}to{opacity:1;transform:translateY(0) scale(1)}}
  @keyframes numberPop{0%{opacity:.2;transform:translateY(12px) scale(.96)}60%{opacity:1;transform:translateY(-2px) scale(1.02)}100%{opacity:1;transform:translateY(0) scale(1)}}
  @keyframes shineSweep{0%{transform:translateX(-160%) rotate(14deg)}100%{transform:translateX(320%) rotate(14deg)}}

  @media (max-width:1360px){.main{padding:24px 18px 34px}.kpi-grid{grid-template-columns:repeat(3,minmax(0,1fr))}.alerts-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.media-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.chart-grid{grid-template-columns:1fr}}
  @media (max-width:900px){.alerts-wrap{grid-template-columns:1fr}}
  @media (max-width:760px){.main{padding:18px 12px 26px}.page-title{font-size:32px}.topbar{flex-direction:column}.kpi-grid,.alerts-grid,.media-grid{grid-template-columns:1fr}.panel{border-radius:22px}}

  /* ================================================================
     Daily Digest Design Sync Patch · 2026-05-14
     - Font stack follows Daily Digest: Plus Jakarta Sans + Noto Sans KR
     - Softer slate background, card rhythm, rounded table layout
     - Removed heavy/dashboard-like feel while keeping existing JS intact
  ================================================================ */
  :root{--report-max:none;--motion-ease:cubic-bezier(.2,.8,.2,1)}
  body{
    font-family:'Plus Jakarta Sans','Noto Sans KR','Malgun Gothic','Apple SD Gothic Neo',system-ui,-apple-system,'Segoe UI',Roboto,Arial,sans-serif;
    background:linear-gradient(180deg,#f8fafc 0%,#eef2f7 100%);
  }
  .main{max-width:none;padding:24px 20px 42px}
  .topbar{align-items:center;margin-bottom:18px;padding:2px 2px 0}
  .page-title{font-size:30px;letter-spacing:-.045em;font-weight:900;color:#0f172a}
  .page-sub{margin-top:7px;font-size:13px;font-weight:700;color:#64748b}
  .top-actions{gap:8px}.icon-btn,.user-pill{height:38px;background:#fff;border-color:rgba(148,163,184,.25);box-shadow:0 6px 18px rgba(15,23,42,.04)}
  .icon-btn{font-size:14px}.user-pill{font-size:12px;color:#64748b;letter-spacing:-.01em}
  .panel{border-radius:24px;background:rgba(255,255,255,.72);border-color:rgba(226,232,240,.95);box-shadow:0 12px 28px rgba(15,23,42,.055);backdrop-filter:blur(14px)}
  .panel:hover{transform:translateY(-3px);box-shadow:0 18px 40px rgba(15,23,42,.08)}
  .filter-panel{padding:16px 18px}.card-section{padding:20px 18px;margin-bottom:16px}
  .control,.btn{height:40px;border-radius:12px;font-size:12px;font-weight:800;background:#fff;border-color:#e2e8f0;color:#475569;box-shadow:0 6px 18px rgba(15,23,42,.035)}
  .control:hover,.btn:hover,.pill:hover,.tab-btn:hover{border-color:rgba(15,23,42,.18);box-shadow:0 12px 28px rgba(15,23,42,.075)}
  .pill,.tab-btn{height:32px;padding:0 12px;font-size:11px;font-weight:900;background:#fff;border-color:#e2e8f0;color:#64748b;box-shadow:0 6px 18px rgba(15,23,42,.035)}
  .pill.active,.tab-btn.active{background:#0f172a;color:#fff;border-color:#0f172a;box-shadow:0 14px 32px rgba(15,23,42,.16)}
  .section-title{font-size:18px;font-weight:900;letter-spacing:-.02em;text-transform:none;color:#0f172a}.info-badge{display:inline-flex}
  .kpi-grid{grid-template-columns:repeat(6,minmax(150px,1fr));gap:12px}
  .kpi-card{min-height:126px;border-radius:20px;background:linear-gradient(180deg,#fff 0%,#f8fafc 100%);border-color:rgba(226,232,240,.95);box-shadow:0 6px 18px rgba(15,23,42,.04);padding:16px}
  .kpi-card:hover{transform:translateY(-5px) scale(1.008);box-shadow:0 22px 44px rgba(15,23,42,.08)}
  .kpi-label{font-size:11px;font-weight:900;color:#64748b;text-transform:uppercase;letter-spacing:.04em;margin-bottom:10px}.kpi-value{font-size:24px;font-weight:900;color:#0f172a}.kpi-sub{font-size:11px;color:#94a3b8;font-weight:700}.kpi-delta{font-size:11px;font-weight:900}
  .alerts-grid{grid-template-columns:repeat(5,minmax(170px,1fr));gap:12px}.alert-card{border-radius:20px;background:#fff;border-color:#e2e8f0;box-shadow:0 6px 18px rgba(15,23,42,.04)}
  .alert-title{font-size:13px}.alert-desc{font-size:11px}.value-main{font-size:24px;color:#0f172a}.state-chip{height:24px;font-size:10px}
  .media-grid{grid-template-columns:repeat(4,minmax(180px,1fr));gap:12px}.media-card{border-radius:22px;background:linear-gradient(180deg,#fff 0%,var(--tone-bg) 100%);box-shadow:0 6px 18px rgba(15,23,42,.04)}
  .media-logo{box-shadow:inset 0 1px 0 rgba(255,255,255,.6);font-weight:900}.media-metric-value{font-size:23px}.media-stats{background:rgba(255,255,255,.58)}
  .chart-grid{gap:16px}.chart-card{padding:18px}.chart-title{font-size:18px;font-weight:900;letter-spacing:-.02em;text-transform:none;color:#0f172a}.chart-sub{font-size:12px;color:#94a3b8}
  .table-card{margin-top:14px;border-radius:22px;border:1px solid #e2e8f0;background:#fff;box-shadow:0 6px 18px rgba(15,23,42,.04)}
  table{border-collapse:separate;border-spacing:0;width:100%;min-width:1120px;background:#fff}
  thead th{position:sticky;top:0;z-index:1;background:#f8fafc;color:#64748b;font-size:11px;font-weight:900;letter-spacing:.04em;text-transform:uppercase;border-bottom:1px solid #e2e8f0}
  tbody td{padding:12px 10px;border-bottom:1px solid #eef2f7;font-size:12px;font-weight:700;color:#475569;background:#fff}
  tbody tr:nth-child(even) td{background:#fbfdff} tbody tr:hover td{background:#f8fbff}.strong{color:#0f172a;font-weight:900}.rank{color:#94a3b8;font-weight:900}.empty{padding:34px!important;color:#94a3b8;text-align:center;font-weight:800}
  .badge,.legend-chip{height:28px;font-size:10px;font-weight:900;box-shadow:0 6px 18px rgba(15,23,42,.035),inset 0 1px 0 rgba(255,255,255,.7)}
  .table-footer{padding:12px 2px 0;color:#94a3b8;font-size:12px;font-weight:800}.pager-btn,.pager-current,.rows-select{border-color:#e2e8f0;background:#fff;color:#64748b}
  @media (max-width:1360px){.kpi-grid{grid-template-columns:repeat(3,minmax(0,1fr))}.alerts-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.media-grid{grid-template-columns:repeat(2,minmax(0,1fr))}}
  @media (max-width:900px){.alerts-wrap{grid-template-columns:1fr}}
  @media (max-width:760px){.main{padding:18px 12px 28px}.topbar{align-items:flex-start}.page-title{font-size:26px}.kpi-grid,.alerts-grid,.media-grid{grid-template-columns:1fr}.panel{border-radius:20px}.filter-row,.table-tools,.section-head{align-items:flex-start}.right-controls,.left-controls{width:100%}.control[type="search"]{width:100%!important}}

</style>
</head>
<body>
  <div class="layout">
    <main class="main">
      <div class="topbar"><div><h1 class="page-title">소스/매체 분석</h1><p class="page-sub">다양한 소스/매체별 성과를 한눈에 확인하고 효율적인 마케팅 의사결정을 지원합니다.</p></div><div class="top-actions"><button class="icon-btn" type="button" title="알림">🔔</button><div class="user-pill">마케터님 · __UPDATED_AT__</div></div></div>
      <section class="panel filter-panel"><div class="filter-row"><div class="left-controls"><input class="control date" type="date" id="startDate" value="__MIN_DATE__" min="__MIN_DATE__" max="__MAX_DATE__" /><span style="color:#98a2b3;font-weight:900">~</span><input class="control date" type="date" id="endDate" value="__MAX_DATE__" min="__MIN_DATE__" max="__MAX_DATE__" /><select class="control" id="compareMode"><option value="prev">비교: 이전 기간</option><option value="none">비교 안 함</option></select><div class="range-buttons"><button class="pill" id="d7Btn" type="button">7D</button><button class="pill active" id="d30Btn" type="button">30D</button><button class="pill" id="d90Btn" type="button">90D</button><button class="pill" id="d365Btn" type="button">12M</button></div></div><div class="right-controls"><button class="btn" id="filterBtn" type="button">필터</button><button class="btn" id="downloadBtn" type="button">내보내기</button></div></div></section>
      <section class="panel card-section"><div class="section-head"><h2 class="section-title">핵심 지표 <span class="info-badge">i</span></h2></div><div class="kpi-grid" id="kpiGrid"></div></section>
      <section class="panel card-section"><div class="section-head"><h2 class="section-title">수치 알림 <span class="info-badge">i</span></h2><div class="section-tools"><button class="btn" type="button">모두 보기</button></div></div><div class="alerts-grid" id="alertGrid">__ALERT_CARDS__</div></section>
      <section class="panel card-section"><div class="section-head"><h2 class="section-title">매체별 한눈에 보기</h2><div class="legend-row">__LEGEND_HTML__</div></div><div class="media-grid" id="mediaGrid"></div></section>
      <div class="chart-grid"><section class="panel chart-card"><div class="chart-head"><div><h3 class="chart-title">채널별 매출</h3><div class="chart-sub">선택 기간 기준</div></div><div class="section-tools"><button class="tab-btn active" id="chartRevenueBtn" type="button">금액</button><button class="tab-btn" id="chartSessionBtn" type="button">세션</button></div></div><div class="chart-wrap"><canvas id="revenueChart"></canvas></div></section><section class="panel chart-card"><div class="chart-head"><div><h3 class="chart-title">구매 전환율 (BUY CVR)</h3><div class="chart-sub">채널별 비교</div></div><select class="control" id="lineMetricSel"><option value="buy_cvr">구매 전환율</option><option value="signup_cvr">회원가입 전환율</option></select></div><div class="chart-wrap"><canvas id="lineChart"></canvas></div></section></div>
      <section class="panel card-section"><div class="section-head"><h2 class="section-title">상세 데이터</h2></div><div class="table-tools"><div class="left-controls"><select class="control" id="viewMode"><option value="period">기본 지표</option><option value="daily">일자별 요약</option><option value="detail">전체 상세</option></select><select class="control" id="channelFilter"><option value="">전체 채널</option></select></div><div class="right-controls"><input class="control" type="search" id="searchBox" placeholder="채널/매체 검색" style="width:220px" /><button class="btn" id="downloadDataBtn" type="button">내보내기</button></div></div><div class="table-card"><div class="table-scroll" id="tableMount"></div></div><div class="table-footer"><div id="tableCount">전체 0건</div><div class="pager"><button class="pager-btn" type="button">‹‹</button><button class="pager-btn" type="button">‹</button><div class="pager-current">1</div><div>/</div><div id="pageCount">1</div><button class="pager-btn" type="button">›</button><button class="pager-btn" type="button">››</button><select class="rows-select"><option>20 / 페이지</option></select></div></div></section>
    </main>
  </div>
<script>
const rows = __ROWS__; const tonePalette = __TONE_PALETTE__; const allMinDate = "__MIN_DATE__"; const allMaxDate = "__MAX_DATE__"; let barMetric = "revenue"; let revenueChart = null; let lineChart = null;
function n(v){const x=Number(v||0);return isFinite(x)?x:0;} function money(v){return '₩'+Math.round(n(v)).toLocaleString('ko-KR');} function num(v){return Math.round(n(v)).toLocaleString('ko-KR');} function one(v){return n(v).toLocaleString('ko-KR',{minimumFractionDigits:1,maximumFractionDigits:1});} function pct(v){return n(v).toLocaleString('ko-KR',{minimumFractionDigits:1,maximumFractionDigits:1})+'%';} function esc(s){return String(s ?? '').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}[m]));}
function toneKey(value){const s=String(value||'').toLowerCase(); if(s.includes('google')) return 'google'; if(s.includes('meta')||s.includes('facebook')||s.includes('instagram')) return 'meta'; if(s.includes('naver')) return 'naver'; if(s.includes('kakao')) return 'kakao'; if(s.includes('edm')||s.includes('email')) return 'edm'; if(s.includes('lms')) return 'lms'; if(s.includes('organic')) return 'organic'; if(s.includes('owned')) return 'owned'; if(s.includes('sns')) return 'sns'; if(s.includes('paid')||s.startsWith('2.')) return 'paid'; return 'other';} function tone(value){return tonePalette[toneKey(value)] || tonePalette.other;} function badge(text,value){const t=tone(value||text); return `<span class="badge" style="--tone-bg:${t.bg};--tone-fg:${t.fg};--tone-border:${t.border};--tone-dot:${t.dot}">${esc(text||'-')}</span>`;}
function dateAdd(d,days){const x=new Date(d+'T00:00:00'); x.setDate(x.getDate()+days); return x.toISOString().slice(0,10);} function daysBetween(a,b){const x=new Date(a+'T00:00:00'); const y=new Date(b+'T00:00:00'); return Math.round((y-x)/86400000)+1;} function currentDateRange(){return {start:document.getElementById('startDate').value||allMinDate,end:document.getElementById('endDate').value||allMaxDate};} function searchQuery(){return (document.getElementById('searchBox').value||'').toLowerCase();} function channelFilterValue(){return document.getElementById('channelFilter').value||'';} function rowsMatchBase(r,ch,q){return (!ch || r.channel_group===ch) && (!q || JSON.stringify(r).toLowerCase().includes(q));} function filteredRows(){const {start,end}=currentDateRange(); const ch=channelFilterValue(); const q=searchQuery(); return rows.filter(r=>r.event_dt>=start && r.event_dt<=end && rowsMatchBase(r,ch,q));} function previousRows(){if(document.getElementById('compareMode').value==='none') return []; const {start,end}=currentDateRange(); const days=daysBetween(start,end); const prevEnd=dateAdd(start,-1); const prevStart=dateAdd(prevEnd,-(days-1)); const ch=channelFilterValue(); const q=searchQuery(); return rows.filter(r=>r.event_dt>=prevStart && r.event_dt<=prevEnd && rowsMatchBase(r,ch,q));}
function aggMetrics(arr){const sessions=arr.reduce((a,r)=>a+n(r.sessions),0), users=arr.reduce((a,r)=>a+n(r.users),0), signups=arr.reduce((a,r)=>a+n(r.signups),0), buyers=arr.reduce((a,r)=>a+n(r.buyers),0), purchase=arr.reduce((a,r)=>a+n(r.purchase),0), revenue=arr.reduce((a,r)=>a+n(r.revenue),0), pv=arr.reduce((a,r)=>a+n(r.pv_per_user)*n(r.users),0), signupPv=arr.reduce((a,r)=>a+n(r.avg_signup_user_pv)*n(r.signups),0); return {sessions,users,signups,signup_cvr:sessions?signups/sessions*100:0,buyers,buy_cvr:sessions?buyers/sessions*100:0,purchase,revenue,aov_per_buyer:buyers?revenue/buyers:0,pv_per_user:users?pv/users:0,avg_signup_user_pv:signups?signupPv/signups:0};}
function groupBy(arr, keys){const m=new Map(); arr.forEach(r=>{const k=keys.map(x=>r[x]??'-').join('||'); if(!m.has(k)) m.set(k,{keys:Object.fromEntries(keys.map(x=>[x,r[x]??'-'])),rows:[]}); m.get(k).rows.push(r);}); return [...m.values()].map(g=>({...g.keys,...aggMetrics(g.rows)}));}
function preferredMediaName(r){const raw=String(r.media_family||'').trim(); if(raw && raw!=='Other') return raw; const cg=String(r.channel_group||''); if(cg.includes('Organic')) return 'Organic'; if(cg.includes('Owned')) return 'Owned'; if(cg.includes('Official SNS')) return 'SNS'; if(cg.includes('Paid')) return 'Paid'; return cg.replace(/^\d+\.\s*/,'') || 'Etc';}
function logoMarkup(name){const s=String(name||'').toLowerCase().trim(); if(s==='google') return `<svg viewBox="0 0 24 24" aria-label="Google logo" role="img"><path fill="#4285F4" d="M21.8 12.2c0-.7-.1-1.4-.2-2H12v3.8h5.5c-.2 1.3-1 2.4-2.1 3.1v2.6h3.4c2-1.8 3-4.5 3-7.5z"/><path fill="#34A853" d="M12 22c2.7 0 4.9-.9 6.5-2.4l-3.4-2.6c-.9.6-2 .9-3.1.9-2.4 0-4.4-1.6-5.1-3.8H3.4v2.7C5 19.9 8.2 22 12 22z"/><path fill="#FBBC05" d="M6.9 14.1c-.2-.6-.3-1.4-.3-2.1s.1-1.4.3-2.1V7.2H3.4C2.5 8.7 2 10.3 2 12s.5 3.3 1.4 4.8l3.5-2.7z"/><path fill="#EA4335" d="M12 6.1c1.5 0 2.9.5 4 1.5l3-3C17 2.8 14.7 2 12 2 8.2 2 5 4.1 3.4 7.2l3.5 2.7c.7-2.2 2.7-3.8 5.1-3.8z"/></svg>`; if(s==='naver') return `<svg viewBox="0 0 24 24" aria-label="Naver logo" role="img"><rect x="2" y="2" width="20" height="20" rx="4" fill="#03C75A"/><path fill="#fff" d="M7 6h3.2l3.8 6V6H17v12h-3.1l-3.8-6V18H7z"/></svg>`; if(s==='kakao') return `<svg viewBox="0 0 24 24" aria-label="Kakao logo" role="img"><path fill="#FEE500" d="M12 3c5 0 9 3 9 6.9 0 3.4-2.9 6.2-6.8 6.8l-2.9 3.2c-.3.3-.8 0-.7-.5l.7-3C7 16.2 3 13.5 3 9.9 3 6 7 3 12 3z"/><path fill="#191919" d="M9 8.1h1.6v2.4l2.4-2.4H15l-2.9 2.8 3.1 4h-2l-2.2-2.9-.8.8v2.1H9z"/></svg>`; if(s==='meta') return `<svg viewBox="0 0 24 24" aria-label="Meta logo" role="img"><path d="M5.2 15.8c1.2-4.6 2.8-7 4-7 1.4 0 2.2 2.2 2.8 4 .7-1.7 1.5-4 3-4 1.7 0 2.9 2.8 3.9 7" fill="none" stroke="#0866FF" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/></svg>`; if(s==='edm') return `<svg viewBox="0 0 24 24" aria-label="EDM"><rect x="3" y="5" width="18" height="14" rx="3" fill="#3B82F6"/><path d="M6 8l6 4 6-4" fill="none" stroke="#fff" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></svg>`; if(s==='lms') return `<svg viewBox="0 0 24 24" aria-label="LMS"><rect x="3" y="4" width="18" height="16" rx="4" fill="#8B5CF6"/><path d="M7 8h10M7 12h7M7 16h8" stroke="#fff" stroke-width="1.8" stroke-linecap="round"/></svg>`; if(s==='organic') return `<svg viewBox="0 0 24 24" aria-label="Organic"><path d="M18.5 5.5C11 6 7 10 6 18.5c8.3-1 12.3-5 12.5-13z" fill="#22C55E"/><path d="M8 16c2-3 4.5-5.5 8-8" fill="none" stroke="#fff" stroke-width="1.7" stroke-linecap="round"/></svg>`; if(s==='owned') return `<svg viewBox="0 0 24 24" aria-label="Owned"><rect x="4" y="4" width="16" height="16" rx="4" fill="#14B8A6"/><path d="M8 16V8h2.3l1.7 3.2L13.7 8H16v8h-1.8v-4.8l-1.8 3.3h-.9L9.8 11.2V16z" fill="#fff"/></svg>`; if(s==='sns') return `<svg viewBox="0 0 24 24" aria-label="SNS"><rect x="4" y="4" width="16" height="16" rx="5" fill="#F97316"/><circle cx="12" cy="12" r="3.2" fill="none" stroke="#fff" stroke-width="1.7"/><circle cx="16.7" cy="7.5" r="1.1" fill="#fff"/></svg>`; if(s==='paid') return `<svg viewBox="0 0 24 24" aria-label="Paid"><circle cx="12" cy="12" r="9" fill="#2563EB"/><path d="M9 7.5h3.7c2.2 0 3.8 1.3 3.8 3.3s-1.6 3.3-3.8 3.3H11V17H9zM11 12.6h1.5c1.1 0 1.8-.6 1.8-1.5s-.7-1.5-1.8-1.5H11z" fill="#fff"/></svg>`; if(s==='etc' || s==='other') return `<svg viewBox="0 0 24 24" aria-label="Etc"><circle cx="12" cy="12" r="9" fill="#E2E8F0"/><text x="12" y="15" text-anchor="middle" font-size="9" font-weight="900" fill="#475569">E</text></svg>`; return `<span class="abbr">${esc(String(name||'').slice(0,2).toUpperCase())}</span>`;}
const alertRules={sessions:{type:'rate',rate:10,abs:300,min_prev:300},users:{type:'rate',rate:10,abs:300,min_prev:300},signups:{type:'rate',rate:15,abs:20,min_prev:20},signup_to_buyers:{type:'rate',rate:20,abs:10,min_prev:10},buyers:{type:'rate',rate:15,abs:10,min_prev:10},purchase:{type:'rate',rate:15,abs:10,min_prev:10},revenue:{type:'rate',rate:10,abs:1000000,min_prev:1000000},signup_cvr:{type:'point',point:0.3,min_sessions:300},buy_cvr:{type:'point',point:0.3,min_sessions:300},avg_signup_user_pv:{type:'rate',rate:15,abs:0.5,min_prev:1,minBaseMetric:'signups',minBase:10},aov_per_buyer:{type:'rate',rate:15,abs:10000,min_prev:10000,minBaseMetric:'buyers',minBase:10},pv_per_user:{type:'rate',rate:15,abs:0.5,min_prev:1,minBaseMetric:'users',minBase:300}};
const alertMetricLabels={sessions:'세션',users:'사용자',signups:'회원가입',signup_to_buyers:'가입→구매자',buyers:'구매수',purchase:'구매건수',revenue:'매출',signup_cvr:'회원가입 전환율',buy_cvr:'구매 전환율',avg_signup_user_pv:'Avg Signup User PV',aov_per_buyer:'AOV / Buyer',pv_per_user:'PV / User'};
function formatMetricValue(metric, value){if(metric==='revenue'||metric==='aov_per_buyer') return money(value); if(metric==='signup_cvr'||metric==='buy_cvr') return pct(value); if(metric==='avg_signup_user_pv'||metric==='pv_per_user') return one(value); return num(value);} 
function formatMetricDelta(metric, value){if(metric==='revenue'||metric==='aov_per_buyer') return money(value); if(metric==='signup_cvr'||metric==='buy_cvr') return `${value>=0?'+':''}${n(value).toFixed(1)}%p`; if(metric==='avg_signup_user_pv'||metric==='pv_per_user') return `${value>=0?'+':''}${n(value).toFixed(1)}`; return `${value>=0?'+':''}${num(value)}`;}
function alertScoreJs(curr, prev, metric, rule){if(prev<=0) return 0; if(rule.type==='point'){return Math.abs(curr-prev)/Math.max(n(rule.point||0.1),0.1);} const rate=Math.abs((curr/prev-1)*100); const absDelta=Math.abs(curr-prev); return (rate/Math.max(n(rule.rate||1),1)) + (absDelta/Math.max(n(rule.abs||1),1));}
function buildAlerts(curArr, prevArr){if(document.getElementById('compareMode').value==='none') return []; const scopes=[{scope:'channel',keys:['channel_group']},{scope:'media',keys:['channel_group','media_name']},{scope:'source_medium',keys:['channel_group','media_name','source','medium']},{scope:'campaign',keys:['channel_group','media_name','source','medium','campaign']}]; const normalize=(arr)=>arr.map(r=>({...r, media_name:preferredMediaName(r)})); const curNorm=normalize(curArr), prevNorm=normalize(prevArr); const alerts=[]; scopes.forEach(sc=>{const curGrouped=groupBy(curNorm, sc.keys); const prevGrouped=groupBy(prevNorm, sc.keys); const prevMap=new Map(prevGrouped.map(row=>[sc.keys.map(k=>String(row[k]??'-')).join('||'), row])); curGrouped.forEach(row=>{const mapKey=sc.keys.map(k=>String(row[k]??'-')).join('||'); const prev=prevMap.get(mapKey); if(!prev) return; const scopeLabel=sc.keys.map(k=>String(row[k]??'-')).filter(v=>v && v!=='-' && v!=='(not set)').join(' / '); Object.entries(alertRules).forEach(([metric, rule])=>{const curr=n(row[metric]); const prevVal=n(prev[metric]); if(prevVal<=0) return; if(rule.minBaseMetric){const baseNow=n(row[rule.minBaseMetric]); const basePrev=n(prev[rule.minBaseMetric]); if(baseNow<n(rule.minBase||0) && basePrev<n(rule.minBase||0)) return;} if(rule.type==='point'){const maxSessions=Math.max(n(row.sessions), n(prev.sessions)); if(maxSessions<n(rule.min_sessions||0)) return; if(Math.abs(curr-prevVal)<n(rule.point||0)) return;} else {const rate=((curr/prevVal)-1)*100; if(Math.abs(rate)<n(rule.rate||0) || Math.abs(curr-prevVal)<n(rule.abs||0) || prevVal<n(rule.min_prev||0)) return;} const delta=curr-prevVal; const rate=prevVal?((curr/prevVal)-1)*100:0; alerts.push({scope:sc.scope, scope_label:scopeLabel||sc.scope, metric, metric_label:alertMetricLabels[metric]||metric, current:curr, previous:prevVal, delta, delta_rate:rate, direction:delta>=0?'UP':'DOWN', score:alertScoreJs(curr, prevVal, metric, rule)});});});}); const seen=new Set(); return alerts.sort((a,b)=>b.score-a.score).filter(a=>{const k=[a.scope,a.scope_label,a.metric].join('||'); if(seen.has(k)) return false; seen.add(k); return true;}).slice(0,8);} 

function makeSparkSvg(values, color){
  const vals=(values||[]).map(n).filter(v=>isFinite(v));
  const w=160, h=42, pad=4;
  if(!vals.length){return `<svg viewBox="0 0 ${w} ${h}" width="100%" height="42" aria-hidden="true"><path d="M4 30 H156" stroke="#e2e8f0" stroke-width="2" fill="none" stroke-linecap="round"/></svg>`;}
  const min=Math.min(...vals), max=Math.max(...vals);
  const span=(max-min)||1;
  const step=vals.length>1?(w-pad*2)/(vals.length-1):0;
  const pts=vals.map((v,i)=>[pad+i*step, h-pad-((v-min)/span)*(h-pad*2)]);
  const d=pts.map((p,i)=>`${i?'L':'M'}${p[0].toFixed(1)} ${p[1].toFixed(1)}`).join(' ');
  const area=`${d} L${pts[pts.length-1][0].toFixed(1)} ${h-pad} L${pad} ${h-pad} Z`;
  return `<svg viewBox="0 0 ${w} ${h}" width="100%" height="42" aria-hidden="true"><path d="${area}" fill="${esc(color||'#3b82f6')}" opacity=".10"></path><path d="${d}" stroke="${esc(color||'#3b82f6')}" stroke-width="2.2" fill="none" stroke-linecap="round" stroke-linejoin="round"></path></svg>`;
}
function deltaInfo(cur, prev, metric){
  const c=n(cur[metric]); const p=n(prev[metric]); const d=c-p; const rate=p?((c/p)-1)*100:0;
  return {delta:d, rate, cls:d>0?'up':(d<0?'down':'flat'), arrow:d>0?'▲':(d<0?'▼':'·')};
}
function deltaText(metric, info){
  if(!info) return '비교 데이터 없음';
  if(metric==='signup_cvr'||metric==='buy_cvr') return `${info.arrow} ${Math.abs(n(info.delta)).toFixed(1)}%p`;
  if(metric==='revenue'||metric==='aov_per_buyer') return `${info.arrow} ${Math.abs(n(info.rate)).toFixed(1)}%`;
  return `${info.arrow} ${Math.abs(n(info.rate)).toFixed(1)}%`;
}
function renderKpis(arr){
  const mount=document.getElementById('kpiGrid');
  if(!mount) return;
  const cur=aggMetrics(arr);
  const prev=aggMetrics(previousRows());
  const specs=[
    ['sessions','세션',num(cur.sessions),'선택 기간 GA 세션'],
    ['users','사용자',num(cur.users),'선택 기간 사용자'],
    ['revenue','매출',money(cur.revenue),'선택 기간 구매 매출'],
    ['buyers','구매수',num(cur.buyers),'구매 사용자 기준'],
    ['buy_cvr','구매 전환율',pct(cur.buy_cvr),'구매수 / 세션'],
    ['signup_cvr','회원가입 전환율',pct(cur.signup_cvr),'회원가입 / 세션']
  ];
  if(!arr.length){
    mount.innerHTML = specs.map(([metric,label])=>`<div class="kpi-card"><div class="kpi-label">${esc(label)}</div><div class="kpi-value">-</div><div class="kpi-sub">선택 기간에 데이터가 없습니다.</div><div class="kpi-delta flat">· 0.0%</div></div>`).join('');
    return;
  }
  mount.innerHTML = specs.map(([metric,label,value,sub])=>{
    const info=deltaInfo(cur, prev, metric);
    const hasPrev=previousRows().length>0 && n(prev[metric])>0;
    return `<div class="kpi-card"><div class="kpi-label">${esc(label)}</div><div class="kpi-value">${value}</div><div class="kpi-sub">${esc(sub)}</div><div class="kpi-delta ${hasPrev?info.cls:'flat'}">${hasPrev?deltaText(metric,info):'비교 데이터 없음'}</div></div>`;
  }).join('');
}
function renderAlerts(arr){
  const mount=document.getElementById('alertGrid');
  if(!mount) return;
  const alerts=buildAlerts(arr, previousRows());
  const cardHtml=(a)=>{
    const cls=a.direction==='UP'?'up':'down';
    const arrow=a.direction==='UP'?'▲':'▼';
    const diffTxt=(a.metric==='signup_cvr'||a.metric==='buy_cvr')?`${arrow} ${Math.abs(n(a.delta)).toFixed(1)}%p`:`${arrow} ${Math.abs(n(a.delta_rate)).toFixed(1)}%`;
    const t=tone(`${a.scope_label} ${a.scope}`);
    return `<div class="alert-card ${cls}" style="--tone-bg:${t.bg};--tone-fg:${t.fg};--tone-border:${t.border};--tone-dot:${t.dot}">
      <div class="alert-head"><span class="state-chip ${cls}">${diffTxt}</span><span class="badge-wrap">${badge(a.scope,a.scope_label)}</span></div>
      <div class="alert-title">${esc(a.metric_label)}</div>
      <div class="alert-desc">${esc(a.scope_label)}</div>
      <div class="alert-values"><div class="value-main">${formatMetricValue(a.metric,a.current)}</div><div class="value-sub">이전 ${formatMetricValue(a.metric,a.previous)} · 증감 ${formatMetricDelta(a.metric,a.delta)}</div></div>
    </div>`;
  };
  if(!alerts.length){
    mount.innerHTML=`<div class="alert-card neutral"><div class="alert-head"><span class="state-chip neutral">알림 없음</span></div><div class="alert-title">선택 기간 기준 유의미한 수치 변동이 없습니다.</div><div class="alert-desc">비교 기간이 없거나 변동 폭이 작은 경우 알림이 생성되지 않습니다.</div></div>`;
    return;
  }
  const ups=alerts.filter(a=>a.direction==='UP').sort((a,b)=>b.score-a.score).slice(0,4);
  const downs=alerts.filter(a=>a.direction==='DOWN').sort((a,b)=>b.score-a.score).slice(0,4);
  const emptyUp=`<div class="alert-card neutral"><div class="alert-head"><span class="state-chip neutral">급등 없음</span></div><div class="alert-title">유의미한 상승 항목이 없습니다.</div><div class="alert-desc">비교 기간 대비 기준값 이상 상승한 지표만 노출합니다.</div></div>`;
  const emptyDown=`<div class="alert-card neutral"><div class="alert-head"><span class="state-chip neutral">급락 없음</span></div><div class="alert-title">유의미한 하락 항목이 없습니다.</div><div class="alert-desc">비교 기간 대비 기준값 이상 하락한 지표만 노출합니다.</div></div>`;
  mount.innerHTML=`<div class="alerts-wrap">
    <div class="alert-lane up"><div class="alert-lane-head"><div class="alert-lane-title">▲ 급등 TOP</div><div class="alert-lane-count">${ups.length}</div></div><div class="alert-list">${ups.length?ups.map(cardHtml).join(''):emptyUp}</div></div>
    <div class="alert-lane down"><div class="alert-lane-head"><div class="alert-lane-title">▼ 급락 TOP</div><div class="alert-lane-count">${downs.length}</div></div><div class="alert-list">${downs.length?downs.map(cardHtml).join(''):emptyDown}</div></div>
  </div>`;
}
function renderMediaGrid(arr){
  const curNorm=arr.map(r=>({...r, media_name:preferredMediaName(r)}));
  const prevNorm=previousRows().map(r=>({...r, media_name:preferredMediaName(r)}));
  const dailyByMedia = groupBy(curNorm, ['media_name','event_dt']);
  const trendMap = {};
  dailyByMedia.forEach(r=>{const k=r.media_name; if(!trendMap[k]) trendMap[k]=[]; trendMap[k].push({date:r.event_dt,revenue:r.revenue,sessions:r.sessions});});
  Object.keys(trendMap).forEach(k=>trendMap[k].sort((a,b)=>String(a.date).localeCompare(String(b.date))));
  const grouped = groupBy(curNorm, ['media_name']).sort((a,b)=>n(b.revenue)-n(a.revenue));
  const prevMap = new Map(groupBy(prevNorm, ['media_name']).map(r=>[String(r.media_name), r]));
  const mount=document.getElementById('mediaGrid');
  if(!mount) return;
  if(!grouped.length){mount.innerHTML='<div class="empty">조건에 맞는 매체 데이터가 없습니다.</div>'; return;}
  mount.innerHTML = grouped.slice(0,8).map(r=>{
    const name=r.media_name;
    const t=tone(name);
    const prev=prevMap.get(String(name));
    const prevRevenue=prev?n(prev.revenue):0;
    const change=prevRevenue?((n(r.revenue)-prevRevenue)/prevRevenue*100):null;
    const cls=change===null?'flat':(change>0?'up':(change<0?'down':'flat'));
    const arrow=change===null?'–':(change>0?'▲':(change<0?'▼':'–'));
    const changeTxt=change===null?'비교 없음':`${arrow} ${Math.abs(change).toFixed(1)}%`;
    const deltaTxt=change===null?'이전 기간 데이터 없음':`이전 ${money(prevRevenue)} · 증감 ${money(n(r.revenue)-prevRevenue)}`;
    return `<div class="media-card" style="--tone-bg:${t.bg};--tone-fg:${t.fg};--tone-border:${t.border};--tone-dot:${t.dot}">
      <div class="media-head"><div class="media-brand"><div class="media-logo">${logoMarkup(name)}</div><div>${esc(name)}</div></div><div><div class="media-growth ${cls}">${changeTxt}</div><div class="media-delta-sub">${esc(deltaTxt)}</div></div></div>
      <div class="media-metric-label">매출</div><div class="media-metric-value">${money(r.revenue)}</div>
      <div class="media-stats"><div><div class="media-stat-label">세션</div><div class="media-stat-value">${num(r.sessions)}</div></div><div><div class="media-stat-label">구매 전환율</div><div class="media-stat-value">${pct(r.buy_cvr)}</div></div></div>
      <div class="spark-wrap">${makeSparkSvg((trendMap[name]||[]).slice(-14).map(x=>n(x.revenue)), t.dot)}</div>
    </div>`;
  }).join('');
}
function buildPeriodRows(arr){return groupBy(arr.map(r=>({...r, media_name:preferredMediaName(r)})), ['media_name','source','medium','campaign']).sort((a,b)=>n(b.revenue)-n(a.revenue)||n(b.sessions)-n(a.sessions));} function buildPeriodTable(grouped){return `<table><thead><tr><th class="rank">#</th><th>채널 + 매체</th><th>소스 / 매체</th><th>캠페인</th><th class="num">세션</th><th class="num">매출</th><th class="num">구매 전환율</th><th class="num">AOV</th><th class="num">구매수</th><th class="num">회원가입 CVR</th></tr></thead><tbody>${grouped.length?grouped.map((r,i)=>`<tr><td class="rank">${i+1}</td><td>${badge(r.media_name,r.media_name)}</td><td>${esc(r.source)} / ${esc(r.medium)}</td><td class="wide">${esc(r.campaign)}</td><td class="num">${num(r.sessions)}</td><td class="num strong">${money(r.revenue)}</td><td class="num">${pct(r.buy_cvr)}</td><td class="num">${money(r.aov_per_buyer)}</td><td class="num">${num(r.buyers)}</td><td class="num">${pct(r.signup_cvr)}</td></tr>`).join(''):`<tr><td colspan="10" class="empty">조건에 맞는 데이터가 없습니다.</td></tr>`}</tbody></table>`;} function buildDailyRows(arr){return groupBy(arr,['event_dt','channel_group']).sort((a,b)=>String(b.event_dt).localeCompare(String(a.event_dt))||n(b.revenue)-n(a.revenue));} function buildDailyTable(grouped){return `<table><thead><tr><th>일자</th><th>채널</th><th class="num">세션</th><th class="num">사용자</th><th class="num">회원가입</th><th class="num">회원가입 CVR</th><th class="num">구매수</th><th class="num">구매 전환율</th><th class="num">매출</th><th class="num">AOV</th></tr></thead><tbody>${grouped.length?grouped.map(r=>`<tr><td>${esc(r.event_dt)}</td><td>${badge(r.channel_group,r.channel_group)}</td><td class="num">${num(r.sessions)}</td><td class="num">${num(r.users)}</td><td class="num">${num(r.signups)}</td><td class="num">${pct(r.signup_cvr)}</td><td class="num">${num(r.buyers)}</td><td class="num">${pct(r.buy_cvr)}</td><td class="num strong">${money(r.revenue)}</td><td class="num">${money(r.aov_per_buyer)}</td></tr>`).join(''):`<tr><td colspan="10" class="empty">조건에 맞는 데이터가 없습니다.</td></tr>`}</tbody></table>`;} function buildDetailRows(arr){return [...arr].sort((a,b)=>n(b.revenue)-n(a.revenue)||n(b.sessions)-n(a.sessions));} function buildDetailTable(sorted){return `<table><thead><tr><th class="rank">#</th><th>채널 + 매체</th><th>소스 / 매체</th><th>캠페인</th><th class="num">세션</th><th class="num">매출</th><th class="num">구매 전환율</th><th class="num">AOV</th><th class="num">구매수</th><th class="num">회원가입 CVR</th><th class="num">사용자</th><th class="num">회원가입</th><th class="num">구매건수</th><th class="num">가입자 평균 PV</th><th class="num">PV/사용자</th></tr></thead><tbody>${sorted.length?sorted.map((r,i)=>`<tr><td class="rank">${i+1}</td><td>${badge(preferredMediaName(r),preferredMediaName(r))}</td><td>${esc(r.source)} / ${esc(r.medium)}</td><td class="wide">${esc(r.campaign)}</td><td class="num">${num(r.sessions)}</td><td class="num strong">${money(r.revenue)}</td><td class="num">${pct(r.buy_cvr)}</td><td class="num">${money(r.aov_per_buyer)}</td><td class="num">${num(r.buyers)}</td><td class="num">${pct(r.signup_cvr)}</td><td class="num">${num(r.users)}</td><td class="num">${num(r.signups)}</td><td class="num">${num(r.purchase)}</td><td class="num">${one(r.avg_signup_user_pv)}</td><td class="num">${one(r.pv_per_user)}</td></tr>`).join(''):`<tr><td colspan="15" class="empty">조건에 맞는 데이터가 없습니다.</td></tr>`}</tbody></table>`;}
function renderTableSection(arr){const mode=document.getElementById('viewMode').value; const mount=document.getElementById('tableMount'); let count=0; let pages=1; if(mode==='daily'){const grouped=buildDailyRows(arr); count=grouped.length; mount.innerHTML=buildDailyTable(grouped);} else if(mode==='detail'){const sorted=buildDetailRows(arr); count=sorted.length; mount.innerHTML=buildDetailTable(sorted);} else {const grouped=buildPeriodRows(arr); count=grouped.length; mount.innerHTML=buildPeriodTable(grouped);} pages=Math.max(1, Math.ceil(count/20)); document.getElementById('tableCount').textContent=`전체 ${num(count)}건`; document.getElementById('pageCount').textContent=num(pages);}
function renderCharts(arr){if(typeof Chart==='undefined'){const rc=document.getElementById('revenueChart'); const lc=document.getElementById('lineChart'); if(rc&&rc.parentElement) rc.parentElement.innerHTML='<div class="empty">Chart.js 로딩 전입니다. 데이터 표는 정상 렌더링됩니다.</div>'; if(lc&&lc.parentElement) lc.parentElement.innerHTML='<div class="empty">Chart.js 로딩 전입니다. 데이터 표는 정상 렌더링됩니다.</div>'; return;} const grouped = groupBy(arr.map(r=>({...r, media_name:preferredMediaName(r)})), ['media_name']).sort((a,b)=>n(b[barMetric])-n(a[barMetric])); const labels = grouped.map(r=>r.media_name); const barData = grouped.map(r=>n(r[barMetric])); const barColors = labels.map(l=>tone(l).dot); if(revenueChart) revenueChart.destroy(); revenueChart = new Chart(document.getElementById('revenueChart'), {type:'bar', data:{labels, datasets:[{data:barData, backgroundColor:barColors, borderRadius:8, maxBarThickness:34}]}, options:{responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}, tooltip:{callbacks:{label:(ctx)=>barMetric==='revenue'?'매출 '+money(ctx.raw):'세션 '+num(ctx.raw)}}}, scales:{x:{grid:{display:false}, ticks:{font:{size:11,weight:'700'}, color:'#667085'}}, y:{grid:{color:'#eef2f7'}, ticks:{color:'#98a2b3', callback:(v)=>barMetric==='revenue'? money(v): num(v)}}}}}); const metric = document.getElementById('lineMetricSel').value; const lineGrouped = groupBy(arr.map(r=>({...r, media_name:preferredMediaName(r)})), ['media_name']).sort((a,b)=>n(b[metric])-n(a[metric])); const lineLabels = lineGrouped.map(r=>r.media_name); const lineData = lineGrouped.map(r=>n(r[metric])); if(lineChart) lineChart.destroy(); lineChart = new Chart(document.getElementById('lineChart'), {type:'line', data:{labels:lineLabels, datasets:[{data:lineData, borderColor:'#3b82f6', backgroundColor:'rgba(59,130,246,.08)', pointBackgroundColor:'#3b82f6', pointBorderColor:'#fff', pointBorderWidth:2, pointRadius:4, tension:.28, fill:false}]}, options:{responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}, tooltip:{callbacks:{label:(ctx)=> (metric==='buy_cvr'?'구매 전환율 ':'회원가입 전환율 ')+pct(ctx.raw)}}}, scales:{x:{grid:{display:false}, ticks:{font:{size:11,weight:'700'}, color:'#667085'}}, y:{grid:{color:'#eef2f7'}, ticks:{color:'#98a2b3', callback:(v)=>pct(v)}}}}});}
function renderAll(){const arr=filteredRows(); renderKpis(arr); renderAlerts(arr); renderMediaGrid(arr); renderCharts(arr); renderTableSection(arr);} [...new Set(rows.map(r=>r.channel_group).filter(Boolean))].sort().forEach(ch=>{const o=document.createElement('option'); o.value=ch; o.textContent=ch; document.getElementById('channelFilter').appendChild(o);}); document.getElementById('startDate').value = dateAdd(allMaxDate,-29); document.getElementById('endDate').value = allMaxDate; ['startDate','endDate','channelFilter','viewMode','lineMetricSel','compareMode'].forEach(id=>document.getElementById(id).addEventListener('change',renderAll)); document.getElementById('searchBox').addEventListener('input',renderAll); function activateRange(id,startOffset){document.querySelectorAll('.pill').forEach(x=>x.classList.remove('active')); document.getElementById(id).classList.add('active'); document.getElementById('startDate').value = startOffset===null?allMinDate:dateAdd(allMaxDate,startOffset); document.getElementById('endDate').value = allMaxDate; renderAll();} document.getElementById('d7Btn').onclick=()=>activateRange('d7Btn',-6); document.getElementById('d30Btn').onclick=()=>activateRange('d30Btn',-29); document.getElementById('d90Btn').onclick=()=>activateRange('d90Btn',-89); document.getElementById('d365Btn').onclick=()=>activateRange('d365Btn',null); document.getElementById('chartRevenueBtn').onclick=()=>{barMetric='revenue';document.getElementById('chartRevenueBtn').classList.add('active');document.getElementById('chartSessionBtn').classList.remove('active');renderAll();}; document.getElementById('chartSessionBtn').onclick=()=>{barMetric='sessions';document.getElementById('chartSessionBtn').classList.add('active');document.getElementById('chartRevenueBtn').classList.remove('active');renderAll();}; document.getElementById('downloadBtn').onclick=()=>{const blob=new Blob([JSON.stringify(rows,null,2)],{type:'application/json'}); const a=document.createElement('a'); a.href=URL.createObjectURL(blob); a.download='utm_channel_rows.json'; a.click();}; document.getElementById('downloadDataBtn').onclick=()=>{const blob=new Blob([JSON.stringify(filteredRows(),null,2)],{type:'application/json'}); const a=document.createElement('a'); a.href=URL.createObjectURL(blob); a.download='utm_channel_filtered.json'; a.click();}; renderAll();
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
        '__UPDATED_AT__': updated_at,
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
