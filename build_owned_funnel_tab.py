#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from google.cloud import bigquery


DEFAULT_SOURCE = Path(__file__).with_name("owned_funnel_tab.html")
DEFAULT_OUTPUT = Path(__file__).parent / "reports" / "daily_digest" / "owned_funnel_tab.html"
DEFAULT_SOURCE_BASE = "reports/daily_digest/data/funnel"
KST = timedelta(hours=9)
OWNED_MESSAGE_BLOCKS = (
    {"channel": "KAKAO", "title_col": 2, "date_col": 3},
    {"channel": "LMS", "title_col": 11, "date_col": 12},
    {"channel": "EDM", "title_col": 21, "date_col": 22},
)
PLAIN_MMDD_RE = re.compile(r"(?<!\d)(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])(?!\d)")
YYYYMMDD_RE = re.compile(r"(?<!\d)(20\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])(?!\d)")
YYMMDD_RE = re.compile(r"(?<!\d)(\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])(?!\d)")


def clean_text(value: Any) -> str:
    return str(value or "").strip()


def kst_today() -> date:
    return (datetime.utcnow() + KST).date()


def ymd(value: date) -> str:
    return value.strftime("%Y-%m-%d")


def ymd_suffix(value: date) -> str:
    return value.strftime("%Y%m%d")


def parse_ymd(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, obj: Any) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def list_funnel_dates(funnel_dir: Path) -> List[str]:
    dates: List[str] = []
    for file in funnel_dir.glob("funnel_*.json"):
        match = re.match(r"funnel_(\d{4}-\d{2}-\d{2})\.json$", file.name)
        if match:
            dates.append(match.group(1))
    return sorted(set(dates))


def guess_owned_message_source(cli_value: str = "") -> Optional[Path]:
    if cli_value:
        return Path(cli_value).expanduser().resolve()

    candidates = [
        Path("data") / "owned_inputs" / "KAKAO,LMS 2025.xlsx",
        Path.home() / "Downloads" / "KAKAO,LMS 2025.xlsx",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    return None


def parse_owned_message_date(raw_value: Any, default_year: str) -> Optional[str]:
    text = clean_text(raw_value)
    if not text:
        return None

    match = re.search(r"(20\d{2})[^\d]?(0?[1-9]|1[0-2])[^\d]?(0?[1-9]|[12]\d|3[01])", text)
    if match:
        return f"{match.group(1)}-{int(match.group(2)):02d}-{int(match.group(3)):02d}"

    match = re.search(r"(0?[1-9]|1[0-2])\D+(0?[1-9]|[12]\d|3[01])", text)
    if match and re.fullmatch(r"\d{4}", default_year or ""):
        return f"{default_year}-{int(match.group(1)):02d}-{int(match.group(2)):02d}"
    return None


def load_owned_message_log(message_source: Optional[Path]) -> pd.DataFrame:
    columns = ["date", "channel", "message_title"]
    if not message_source or not message_source.exists():
        return pd.DataFrame(columns=columns)

    inferred_year_match = re.search(r"(20\d{2})", message_source.name)
    inferred_year = inferred_year_match.group(1) if inferred_year_match else ""
    raw = pd.read_excel(message_source, sheet_name=0, header=None)
    rows: List[Dict[str, str]] = []

    for block in OWNED_MESSAGE_BLOCKS:
        if raw.shape[1] <= max(block["title_col"], block["date_col"]):
            continue
        channel = block["channel"]
        for idx in raw.index:
            title = clean_text(raw.iat[idx, block["title_col"]])
            date_str = parse_owned_message_date(raw.iat[idx, block["date_col"]], inferred_year)
            if not title or not date_str:
                continue
            rows.append({"date": date_str, "channel": channel, "message_title": title})

    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows, columns=columns).drop_duplicates().sort_values(["date", "channel", "message_title"]).reset_index(drop=True)


def build_owned_message_title_lookup(message_log_df: pd.DataFrame) -> Dict[Tuple[str, str], str]:
    lookup: Dict[Tuple[str, str], str] = {}
    if message_log_df.empty:
        return lookup

    grouped = (
        message_log_df.groupby(["date", "channel"], dropna=False)["message_title"]
        .apply(lambda s: " | ".join(dict.fromkeys(clean_text(v) for v in s if clean_text(v))))
        .reset_index()
    )
    for _, row in grouped.iterrows():
        key = (clean_text(row.get("date", "")), clean_text(row.get("channel", "")))
        if any(key):
            lookup[key] = clean_text(row.get("message_title", ""))
    return lookup


def extract_group_year_mmdd(*values: Any, fallback_date: Optional[str] = None) -> Tuple[str, str]:
    fallback = clean_text(fallback_date)
    fallback_year = fallback[:4] if len(fallback) >= 10 else ""
    fallback_mmdd = fallback[5:7] + fallback[8:10] if len(fallback) >= 10 else ""

    for raw in values:
        text = clean_text(raw)
        if not text:
            continue

        match8 = YYYYMMDD_RE.search(text)
        if match8:
            return match8.group(1), f"{match8.group(2)}{match8.group(3)}"

        match6 = YYMMDD_RE.search(text)
        if match6:
            return f"20{match6.group(1)}", f"{match6.group(2)}{match6.group(3)}"

        match4 = PLAIN_MMDD_RE.search(text)
        if match4 and fallback_year:
            return fallback_year, f"{match4.group(1)}{match4.group(2)}"

    return fallback_year, fallback_mmdd


def build_query(project: str, dataset: str, start_suffix: str, end_suffix: str) -> str:
    table = f"`{project}.{dataset}.events_*`"
    return f"""
DECLARE start_suffix STRING DEFAULT '{start_suffix}';
DECLARE end_suffix STRING DEFAULT '{end_suffix}';

WITH base AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_dt,
    event_timestamp,
    user_pseudo_id,
    COALESCE(
      NULLIF(CAST(user_id AS STRING), ''),
      NULLIF((SELECT value.string_value FROM UNNEST(event_params) WHERE key='user_id'), '')
    ) AS user_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_id') AS ga_session_id,
    collected_traffic_source.manual_source AS cts_source,
    collected_traffic_source.manual_medium AS cts_medium,
    collected_traffic_source.manual_campaign_name AS cts_campaign,
    collected_traffic_source.manual_term AS cts_term,
    traffic_source.source AS ts_source,
    traffic_source.medium AS ts_medium,
    traffic_source.name AS ts_campaign,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key='source') AS ep_source,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key='medium') AS ep_medium,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key='campaign') AS ep_campaign,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key='term') AS ep_term,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key='utm_source') AS ep_utm_source,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key='utm_medium') AS ep_utm_medium,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key='utm_campaign') AS ep_utm_campaign,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key='utm_term') AS ep_utm_term,
    event_name,
    IFNULL(ecommerce.purchase_revenue, 0) AS purchase_revenue,
    CAST(ecommerce.transaction_id AS STRING) AS transaction_id,
    items
  FROM {table}
  WHERE _TABLE_SUFFIX BETWEEN start_suffix AND end_suffix
),
base2 AS (
  SELECT
    event_dt,
    event_timestamp,
    user_pseudo_id,
    user_id,
    ga_session_id,
    CONCAT(user_pseudo_id, '-', CAST(ga_session_id AS STRING)) AS session_key,
    NULLIF(COALESCE(cts_source, ep_utm_source, ep_source, ts_source), '') AS utm_source,
    NULLIF(COALESCE(cts_medium, ep_utm_medium, ep_medium, ts_medium), '') AS utm_medium,
    NULLIF(COALESCE(cts_campaign, ep_utm_campaign, ep_campaign, ts_campaign), '') AS utm_campaign,
    NULLIF(COALESCE(cts_term, ep_utm_term, ep_term), '') AS utm_term,
    event_name,
    purchase_revenue,
    transaction_id,
    items
  FROM base
  WHERE ga_session_id IS NOT NULL
),
session_dim AS (
  SELECT
    MIN(event_dt) AS date,
    user_pseudo_id,
    ARRAY_AGG(user_id IGNORE NULLS ORDER BY event_timestamp LIMIT 1)[SAFE_OFFSET(0)] AS user_id,
    ga_session_id,
    session_key,
    ARRAY_AGG(utm_source IGNORE NULLS ORDER BY event_timestamp LIMIT 1)[SAFE_OFFSET(0)] AS utm_source,
    ARRAY_AGG(utm_medium IGNORE NULLS ORDER BY event_timestamp LIMIT 1)[SAFE_OFFSET(0)] AS utm_medium,
    ARRAY_AGG(utm_campaign IGNORE NULLS ORDER BY event_timestamp LIMIT 1)[SAFE_OFFSET(0)] AS utm_campaign,
    ARRAY_AGG(utm_term IGNORE NULLS ORDER BY event_timestamp LIMIT 1)[SAFE_OFFSET(0)] AS utm_term
  FROM base2
  GROUP BY user_pseudo_id, ga_session_id, session_key
),
session_event_flags AS (
  SELECT
    session_key,
    MAX(IF(event_name = 'view_item', 1, 0)) AS has_pdp_view,
    MAX(IF(event_name = 'add_to_cart', 1, 0)) AS has_add_to_cart,
    MAX(IF(event_name = 'begin_checkout', 1, 0)) AS has_begin_checkout,
    MAX(IF(event_name = 'purchase', 1, 0)) AS has_purchase
  FROM base2
  GROUP BY session_key
),
purchase_evt AS (
  SELECT
    session_key,
    COUNT(DISTINCT NULLIF(transaction_id, '')) AS txns,
    COUNT(*) AS purchase_events,
    SUM(IFNULL(purchase_revenue, 0)) AS revenue_evt
  FROM base2
  WHERE event_name = 'purchase'
  GROUP BY session_key
),
purchase_items AS (
  SELECT
    b.session_key,
    SUM(IFNULL(it.item_revenue, 0)) AS revenue_items
  FROM base2 b
  CROSS JOIN UNNEST(b.items) AS it
  WHERE b.event_name = 'purchase'
  GROUP BY b.session_key
),
session_classified AS (
  SELECT
    s.date,
    s.session_key,
    s.user_pseudo_id,
    COALESCE(NULLIF(s.user_id, ''), s.user_pseudo_id) AS user_key,
    IFNULL(s.utm_source, '') AS utm_source,
    IFNULL(s.utm_medium, '') AS utm_medium,
    IFNULL(s.utm_campaign, '') AS utm_campaign,
    IFNULL(s.utm_term, '') AS utm_term,
    CASE
      WHEN REGEXP_CONTAINS(LOWER(IFNULL(s.utm_medium, '')), r'kakao|kko')
        OR REGEXP_CONTAINS(LOWER(IFNULL(s.utm_campaign, '')), r'kakao|kko')
        OR REGEXP_CONTAINS(LOWER(IFNULL(s.utm_term, '')), r'kakao|kko') THEN 'KAKAO'
      WHEN LOWER(IFNULL(s.utm_medium, '')) = 'lms'
        OR REGEXP_CONTAINS(LOWER(IFNULL(s.utm_medium, '')), r'lms')
        OR REGEXP_CONTAINS(LOWER(IFNULL(s.utm_campaign, '')), r'lms')
        OR REGEXP_CONTAINS(LOWER(IFNULL(s.utm_term, '')), r'lms') THEN 'LMS'
      WHEN REGEXP_CONTAINS(LOWER(IFNULL(s.utm_medium, '')), r'edm|email')
        OR REGEXP_CONTAINS(LOWER(IFNULL(s.utm_campaign, '')), r'edm|email')
        OR REGEXP_CONTAINS(LOWER(IFNULL(s.utm_term, '')), r'edm|email') THEN 'EDM'
      ELSE 'OTHER'
    END AS owned_channel,
    REGEXP_CONTAINS(LOWER(IFNULL(s.utm_medium, '')), r'cpc|ppc|paid|display|banner|affiliate|retarget|programmatic|cpv|cpm|cpp') AS is_paid,
    REGEXP_CONTAINS(LOWER(IFNULL(s.utm_medium, '')), r'organic|seo') AS is_organic,
    IFNULL(f.has_pdp_view, 0) AS pdp_views,
    IFNULL(f.has_add_to_cart, 0) AS add_to_cart,
    IFNULL(f.has_begin_checkout, 0) AS begin_checkout,
    IFNULL(f.has_purchase, 0) AS purchase_sessions,
    CASE WHEN IFNULL(e.txns, 0) > 0 THEN IFNULL(e.txns, 0) ELSE IFNULL(e.purchase_events, 0) END AS purchases,
    CASE WHEN IFNULL(i.revenue_items, 0) > 0 THEN IFNULL(i.revenue_items, 0) ELSE IFNULL(e.revenue_evt, 0) END AS revenue
  FROM session_dim s
  LEFT JOIN session_event_flags f USING (session_key)
  LEFT JOIN purchase_evt e USING (session_key)
  LEFT JOIN purchase_items i USING (session_key)
),
summary_rows AS (
  SELECT
    'SUMMARY' AS row_type,
    CAST(date AS STRING) AS date,
    'SITE' AS detail,
    '' AS utm_campaign,
    '' AS utm_term,
    '' AS user_key,
    '' AS session_key,
    COUNT(DISTINCT session_key) AS sessions,
    COUNT(DISTINCT user_pseudo_id) AS users,
    COUNT(DISTINCT IF(pdp_views > 0, session_key, NULL)) AS pdp_views,
    COUNT(DISTINCT IF(add_to_cart > 0, session_key, NULL)) AS add_to_cart,
    COUNT(DISTINCT IF(begin_checkout > 0, session_key, NULL)) AS begin_checkout,
    COUNT(DISTINCT IF(purchase_sessions > 0, session_key, NULL)) AS purchase_sessions,
    COUNT(DISTINCT IF(pdp_views > 0, user_key, NULL)) AS pdp_users,
    COUNT(DISTINCT IF(add_to_cart > 0, user_key, NULL)) AS add_to_cart_users,
    COUNT(DISTINCT IF(begin_checkout > 0, user_key, NULL)) AS begin_checkout_users,
    COUNT(DISTINCT IF(purchase_sessions > 0, user_key, NULL)) AS purchase_users,
    COUNT(DISTINCT IF(purchases > 0, user_key, NULL)) AS buyer_users,
    SUM(purchases) AS purchases,
    SUM(revenue) AS revenue
  FROM session_classified
  GROUP BY date

  UNION ALL

  SELECT
    'SUMMARY' AS row_type,
    CAST(date AS STRING) AS date,
    'OWNED' AS detail,
    '' AS utm_campaign,
    '' AS utm_term,
    '' AS user_key,
    '' AS session_key,
    COUNT(DISTINCT session_key) AS sessions,
    COUNT(DISTINCT user_pseudo_id) AS users,
    COUNT(DISTINCT IF(pdp_views > 0, session_key, NULL)) AS pdp_views,
    COUNT(DISTINCT IF(add_to_cart > 0, session_key, NULL)) AS add_to_cart,
    COUNT(DISTINCT IF(begin_checkout > 0, session_key, NULL)) AS begin_checkout,
    COUNT(DISTINCT IF(purchase_sessions > 0, session_key, NULL)) AS purchase_sessions,
    COUNT(DISTINCT IF(pdp_views > 0, user_key, NULL)) AS pdp_users,
    COUNT(DISTINCT IF(add_to_cart > 0, user_key, NULL)) AS add_to_cart_users,
    COUNT(DISTINCT IF(begin_checkout > 0, user_key, NULL)) AS begin_checkout_users,
    COUNT(DISTINCT IF(purchase_sessions > 0, user_key, NULL)) AS purchase_users,
    COUNT(DISTINCT IF(purchases > 0, user_key, NULL)) AS buyer_users,
    SUM(purchases) AS purchases,
    SUM(revenue) AS revenue
  FROM session_classified
  WHERE owned_channel IN ('EDM', 'LMS', 'KAKAO')
  GROUP BY date

  UNION ALL

  SELECT
    'SUMMARY' AS row_type,
    CAST(date AS STRING) AS date,
    owned_channel AS detail,
    '' AS utm_campaign,
    '' AS utm_term,
    '' AS user_key,
    '' AS session_key,
    COUNT(DISTINCT session_key) AS sessions,
    COUNT(DISTINCT user_pseudo_id) AS users,
    COUNT(DISTINCT IF(pdp_views > 0, session_key, NULL)) AS pdp_views,
    COUNT(DISTINCT IF(add_to_cart > 0, session_key, NULL)) AS add_to_cart,
    COUNT(DISTINCT IF(begin_checkout > 0, session_key, NULL)) AS begin_checkout,
    COUNT(DISTINCT IF(purchase_sessions > 0, session_key, NULL)) AS purchase_sessions,
    COUNT(DISTINCT IF(pdp_views > 0, user_key, NULL)) AS pdp_users,
    COUNT(DISTINCT IF(add_to_cart > 0, user_key, NULL)) AS add_to_cart_users,
    COUNT(DISTINCT IF(begin_checkout > 0, user_key, NULL)) AS begin_checkout_users,
    COUNT(DISTINCT IF(purchase_sessions > 0, user_key, NULL)) AS purchase_users,
    COUNT(DISTINCT IF(purchases > 0, user_key, NULL)) AS buyer_users,
    SUM(purchases) AS purchases,
    SUM(revenue) AS revenue
  FROM session_classified
  WHERE owned_channel IN ('EDM', 'LMS', 'KAKAO')
  GROUP BY date, detail

  UNION ALL

  SELECT
    'SUMMARY' AS row_type,
    CAST(date AS STRING) AS date,
    'PAID' AS detail,
    '' AS utm_campaign,
    '' AS utm_term,
    '' AS user_key,
    '' AS session_key,
    COUNT(DISTINCT session_key) AS sessions,
    COUNT(DISTINCT user_pseudo_id) AS users,
    COUNT(DISTINCT IF(pdp_views > 0, session_key, NULL)) AS pdp_views,
    COUNT(DISTINCT IF(add_to_cart > 0, session_key, NULL)) AS add_to_cart,
    COUNT(DISTINCT IF(begin_checkout > 0, session_key, NULL)) AS begin_checkout,
    COUNT(DISTINCT IF(purchase_sessions > 0, session_key, NULL)) AS purchase_sessions,
    COUNT(DISTINCT IF(pdp_views > 0, user_key, NULL)) AS pdp_users,
    COUNT(DISTINCT IF(add_to_cart > 0, user_key, NULL)) AS add_to_cart_users,
    COUNT(DISTINCT IF(begin_checkout > 0, user_key, NULL)) AS begin_checkout_users,
    COUNT(DISTINCT IF(purchase_sessions > 0, user_key, NULL)) AS purchase_users,
    COUNT(DISTINCT IF(purchases > 0, user_key, NULL)) AS buyer_users,
    SUM(purchases) AS purchases,
    SUM(revenue) AS revenue
  FROM session_classified
  WHERE owned_channel = 'OTHER' AND is_paid
  GROUP BY date

  UNION ALL

  SELECT
    'SUMMARY' AS row_type,
    CAST(date AS STRING) AS date,
    'ORGANIC' AS detail,
    '' AS utm_campaign,
    '' AS utm_term,
    '' AS user_key,
    '' AS session_key,
    COUNT(DISTINCT session_key) AS sessions,
    COUNT(DISTINCT user_pseudo_id) AS users,
    COUNT(DISTINCT IF(pdp_views > 0, session_key, NULL)) AS pdp_views,
    COUNT(DISTINCT IF(add_to_cart > 0, session_key, NULL)) AS add_to_cart,
    COUNT(DISTINCT IF(begin_checkout > 0, session_key, NULL)) AS begin_checkout,
    COUNT(DISTINCT IF(purchase_sessions > 0, session_key, NULL)) AS purchase_sessions,
    COUNT(DISTINCT IF(pdp_views > 0, user_key, NULL)) AS pdp_users,
    COUNT(DISTINCT IF(add_to_cart > 0, user_key, NULL)) AS add_to_cart_users,
    COUNT(DISTINCT IF(begin_checkout > 0, user_key, NULL)) AS begin_checkout_users,
    COUNT(DISTINCT IF(purchase_sessions > 0, user_key, NULL)) AS purchase_users,
    COUNT(DISTINCT IF(purchases > 0, user_key, NULL)) AS buyer_users,
    SUM(purchases) AS purchases,
    SUM(revenue) AS revenue
  FROM session_classified
  WHERE owned_channel = 'OTHER' AND is_organic
  GROUP BY date
),
owned_session_rows AS (
  SELECT
    'OWNED_SESSION' AS row_type,
    CAST(date AS STRING) AS date,
    owned_channel AS detail,
    utm_campaign,
    utm_term,
    user_key,
    session_key,
    1 AS sessions,
    1 AS users,
    pdp_views,
    add_to_cart,
    begin_checkout,
    purchase_sessions,
    CASE WHEN pdp_views > 0 THEN 1 ELSE 0 END AS pdp_users,
    CASE WHEN add_to_cart > 0 THEN 1 ELSE 0 END AS add_to_cart_users,
    CASE WHEN begin_checkout > 0 THEN 1 ELSE 0 END AS begin_checkout_users,
    CASE WHEN purchase_sessions > 0 THEN 1 ELSE 0 END AS purchase_users,
    CASE WHEN purchases > 0 THEN 1 ELSE 0 END AS buyer_users,
    purchases,
    revenue
  FROM session_classified
  WHERE owned_channel IN ('EDM', 'LMS', 'KAKAO')
)
SELECT * FROM summary_rows
UNION ALL
SELECT * FROM owned_session_rows
ORDER BY date, row_type, detail
"""


def run_bq_query(client: bigquery.Client, query: str) -> pd.DataFrame:
    return client.query(query).result().to_dataframe(create_bqstorage_client=False)


def build_daily_summary_rows(day_df: pd.DataFrame, day: str) -> List[Dict[str, Any]]:
    keys = ["SITE", "PAID", "ORGANIC", "OWNED", "EDM", "LMS", "KAKAO"]
    metric_defaults = {
        "sessions": 0,
        "users": 0,
        "pdp_views": 0,
        "add_to_cart": 0,
        "begin_checkout": 0,
        "purchase_sessions": 0,
        "pdp_users": 0,
        "add_to_cart_users": 0,
        "begin_checkout_users": 0,
        "purchase_users": 0,
        "buyer_users": 0,
        "purchases": 0,
        "revenue": 0,
    }
    rows: List[Dict[str, Any]] = []
    for key in keys:
        match = day_df[day_df["detail"] == key]
        payload = {"date": day, "detail": key, **metric_defaults}
        if not match.empty:
            row = match.iloc[0]
            payload.update(
                {
                    "date": clean_text(row.get("date", "")),
                    "sessions": int(row.get("sessions", 0) or 0),
                    "users": int(row.get("users", 0) or 0),
                    "pdp_views": int(row.get("pdp_views", 0) or 0),
                    "add_to_cart": int(row.get("add_to_cart", 0) or 0),
                    "begin_checkout": int(row.get("begin_checkout", 0) or 0),
                    "purchase_sessions": int(row.get("purchase_sessions", 0) or 0),
                    "pdp_users": int(row.get("pdp_users", 0) or 0),
                    "add_to_cart_users": int(row.get("add_to_cart_users", 0) or 0),
                    "begin_checkout_users": int(row.get("begin_checkout_users", 0) or 0),
                    "purchase_users": int(row.get("purchase_users", 0) or 0),
                    "buyer_users": int(row.get("buyer_users", 0) or 0),
                    "purchases": int(row.get("purchases", 0) or 0),
                    "revenue": float(row.get("revenue", 0) or 0),
                }
            )
        rows.append(payload)
    return rows


def build_owned_groups(day_df: pd.DataFrame, title_lookup: Dict[Tuple[str, str], str]) -> List[Dict[str, Any]]:
    if day_df.empty:
        return []

    working = day_df.copy()
    working["year"] = ""
    working["mmdd"] = ""

    for idx, row in working.iterrows():
        year, mmdd = extract_group_year_mmdd(row.get("utm_campaign", ""), row.get("utm_term", ""), fallback_date=row.get("date", ""))
        working.at[idx, "year"] = year
        working.at[idx, "mmdd"] = mmdd

    grouped_rows: List[Dict[str, Any]] = []
    for (group_date, detail, year, mmdd), group in working.groupby(["date", "detail", "year", "mmdd"], dropna=False):
        if not clean_text(year) or not clean_text(mmdd):
            continue
        group = group.copy()
        user_key_series = group["user_key"].astype(str).str.strip()
        pdp_user_keys = user_key_series[group["pdp_views"].fillna(0) > 0]
        cart_user_keys = user_key_series[group["add_to_cart"].fillna(0) > 0]
        checkout_user_keys = user_key_series[group["begin_checkout"].fillna(0) > 0]
        purchase_user_keys = user_key_series[group["purchase_sessions"].fillna(0) > 0]
        buyer_user_keys = user_key_series[group["purchases"].fillna(0) > 0]

        grouped_rows.append(
            {
                "date": clean_text(group_date),
                "detail": clean_text(detail),
                "year": clean_text(year),
                "mmdd": clean_text(mmdd),
                "message_title": title_lookup.get((clean_text(group_date), clean_text(detail)), ""),
                "sessions": int(group["session_key"].astype(str).nunique()),
                "users": int(user_key_series[user_key_series != ""].nunique()),
                "pdp_views": int(group["pdp_views"].fillna(0).sum()),
                "add_to_cart": int(group["add_to_cart"].fillna(0).sum()),
                "begin_checkout": int(group["begin_checkout"].fillna(0).sum()),
                "purchase_sessions": int(group["purchase_sessions"].fillna(0).sum()),
                "pdp_users": int(pdp_user_keys[pdp_user_keys != ""].nunique()),
                "add_to_cart_users": int(cart_user_keys[cart_user_keys != ""].nunique()),
                "begin_checkout_users": int(checkout_user_keys[checkout_user_keys != ""].nunique()),
                "purchase_users": int(purchase_user_keys[purchase_user_keys != ""].nunique()),
                "buyer_users": int(buyer_user_keys[buyer_user_keys != ""].nunique()),
                "purchases": int(group["purchases"].fillna(0).sum()),
                "revenue": float(group["revenue"].fillna(0).sum()),
            }
        )
    return sorted(grouped_rows, key=lambda row: (row["year"], row["mmdd"], row["detail"]), reverse=True)


def build_day_bundle(day: str, summary_df: pd.DataFrame, owned_sessions_df: pd.DataFrame, title_lookup: Dict[Tuple[str, str], str]) -> Dict[str, Any]:
    return {
        "date": day,
        "summary_rows": build_daily_summary_rows(summary_df[summary_df["date"] == day], day),
        "owned_groups": build_owned_groups(owned_sessions_df[owned_sessions_df["date"] == day], title_lookup),
    }


def daterange(start_d: date, end_d: date) -> List[date]:
    days: List[date] = []
    cursor = start_d
    while cursor <= end_d:
        days.append(cursor)
        cursor += timedelta(days=1)
    return days


def build_range(
    project: str,
    dataset: str,
    start_d: date,
    end_d: date,
    site_dir: Path,
    overwrite: bool = False,
    owned_message_source: Optional[Path] = None,
) -> Path:
    client = bigquery.Client(project=project)
    funnel_dir = site_dir / "data" / "funnel"
    ensure_dir(funnel_dir)

    if overwrite:
        for day in daterange(start_d, end_d):
            path = funnel_dir / f"funnel_{ymd(day)}.json"
            if path.exists():
                path.unlink()

    query = build_query(project, dataset, ymd_suffix(start_d), ymd_suffix(end_d))
    df = run_bq_query(client, query)
    if df.empty:
        write_json(funnel_dir / "available_dates.json", {"available_dates": list_funnel_dates(funnel_dir)})
        return funnel_dir

    for col in ["sessions", "users", "pdp_views", "add_to_cart", "begin_checkout", "purchase_sessions", "pdp_users", "add_to_cart_users", "begin_checkout_users", "purchase_users", "buyer_users", "purchases", "revenue"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    for col in ["date", "detail", "utm_campaign", "utm_term", "user_key", "session_key"]:
        if col in df.columns:
            df[col] = df[col].map(clean_text)

    message_log_df = load_owned_message_log(owned_message_source)
    title_lookup = build_owned_message_title_lookup(message_log_df)

    summary_df = df[df["row_type"] == "SUMMARY"].copy()
    owned_sessions_df = df[df["row_type"] == "OWNED_SESSION"].copy()

    written_days = sorted(set(summary_df["date"].tolist()) | set(owned_sessions_df["date"].tolist()))
    for day in written_days:
        bundle = build_day_bundle(day, summary_df, owned_sessions_df, title_lookup)
        write_json(funnel_dir / f"funnel_{day}.json", bundle)

    write_json(funnel_dir / "available_dates.json", {"available_dates": list_funnel_dates(funnel_dir)})
    return funnel_dir


def resolve_source_path(source_value: str) -> Path:
    candidates: List[Path] = []
    if source_value:
        candidates.append(Path(source_value))
    candidates.extend(
        [
            DEFAULT_SOURCE,
            Path("owned_funnel_tab.html"),
            Path("reports") / "daily_digest" / "owned_funnel_tab.html",
        ]
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    raise FileNotFoundError("owned_funnel_tab.html not found in expected paths.")


def build_html(source: Path, out: Path, data_base: str) -> None:
    html = source.read_text(encoding="utf-8")
    published_html = html.replace(DEFAULT_SOURCE_BASE, data_base.replace("\\", "/"))
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(published_html, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build funnel analysis dataset and published HTML.")
    parser.add_argument("--mode", choices=["html", "data", "all"], default="html", help="Build HTML only, data only, or both.")
    parser.add_argument("--source", default="", help="Source HTML file to publish.")
    parser.add_argument("--out", default=str(DEFAULT_OUTPUT), help="Published HTML output path.")
    parser.add_argument("--data-base", default="data/funnel", help="Relative funnel data path to inject for the published HTML.")
    parser.add_argument("--project", default="columbia-ga4", help="BigQuery project id.")
    parser.add_argument("--dataset", default="analytics_358593394", help="BigQuery dataset id.")
    parser.add_argument("--start", default="", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", default="", help="End date YYYY-MM-DD")
    parser.add_argument("--recent-days", type=int, default=0, help="Incremental window in days, ending yesterday KST.")
    parser.add_argument("--site-dir", default="reports/daily_digest", help="Output directory that will receive data/funnel.")
    parser.add_argument("--owned-message-source", default="", help="Optional workbook path for owned message title lookup.")
    parser.add_argument("--overwrite", action="store_true", help="Rewrite all daily bundle files in the requested range.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    out = Path(args.out).resolve()

    if args.mode in {"data", "all"}:
        if args.recent_days and args.recent_days > 0:
            end_d = kst_today() - timedelta(days=1)
            start_d = end_d - timedelta(days=args.recent_days - 1)
        else:
            if not args.start or not args.end:
                raise SystemExit("Either --recent-days or both --start/--end are required for data mode.")
            start_d = parse_ymd(args.start)
            end_d = parse_ymd(args.end)
        if start_d > end_d:
            raise SystemExit("Start date must be before or equal to end date.")

        funnel_dir = build_range(
            project=args.project,
            dataset=args.dataset,
            start_d=start_d,
            end_d=end_d,
            site_dir=Path(args.site_dir).resolve(),
            overwrite=args.overwrite,
            owned_message_source=guess_owned_message_source(args.owned_message_source),
        )
        print(f"[OK] wrote data: {funnel_dir}")
        print(f"[OK] available_dates count: {len(list_funnel_dates(funnel_dir))}")

    if args.mode in {"html", "all"}:
        source = resolve_source_path(args.source)
        build_html(source, out, args.data_base)
        print(f"[OK] source: {source}")
        print(f"[OK] out: {out}")
        print(f"[OK] data base: {args.data_base}")


if __name__ == "__main__":
    main()
