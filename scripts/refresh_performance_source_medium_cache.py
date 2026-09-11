#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Maintain a repo-local daily source/medium cache for Performance Dashboard.

First run backfills the required 6-month TY + LY windows from GA4 BigQuery.
Subsequent runs refresh only the latest 3 days and the matching LY dates.
"""
import os
from pathlib import Path
import pandas as pd
from google.cloud import bigquery

PROJECT = os.getenv("GCP_PROJECT", "columbia-ga4")
DATASET = os.getenv("GA4_DATASET", "analytics_358593394")
OUT = Path(os.getenv("OUT_DIR", "reports"))
OUT.mkdir(parents=True, exist_ok=True)
CACHE = OUT / "performance_source_medium_daily.csv"
TODAY = pd.Timestamp.today().normalize()
END = pd.Timestamp(os.getenv("REPORT_END_DATE", (TODAY - pd.Timedelta(days=1)).strftime("%Y-%m-%d")))
DATA_START = END - pd.DateOffset(months=6) + pd.Timedelta(days=1)
LY_START = DATA_START - pd.DateOffset(years=1)
LY_END = END - pd.DateOffset(years=1)


def query(a, b):
    a_s, b_s = a.strftime("%Y%m%d"), b.strftime("%Y%m%d")
    return f"""
    WITH base AS (
      SELECT
        PARSE_DATE('%Y%m%d', event_date) AS event_date,
        TIMESTAMP_MICROS(event_timestamp) AS event_ts,
        user_pseudo_id,
        event_name,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_id') AS ga_session_id,
        COALESCE(
          session_traffic_source_last_click.manual_campaign.source,
          collected_traffic_source.manual_source,
          traffic_source.source,
          '(direct)'
        ) AS source,
        COALESCE(
          session_traffic_source_last_click.manual_campaign.medium,
          collected_traffic_source.manual_medium,
          traffic_source.medium,
          '(none)'
        ) AS medium,
        ecommerce.transaction_id AS transaction_id,
        COALESCE(ecommerce.purchase_revenue, 0) AS revenue
      FROM `{PROJECT}.{DATASET}.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '{a_s}' AND '{b_s}'
    ), sessions AS (
      SELECT
        event_date,
        CONCAT(user_pseudo_id, '-', CAST(ga_session_id AS STRING)) AS session_key,
        ARRAY_AGG(source IGNORE NULLS ORDER BY event_ts LIMIT 1)[SAFE_OFFSET(0)] AS source,
        ARRAY_AGG(medium IGNORE NULLS ORDER BY event_ts LIMIT 1)[SAFE_OFFSET(0)] AS medium,
        COUNT(DISTINCT IF(event_name='purchase', transaction_id, NULL)) AS purchases,
        SUM(IF(event_name='purchase', revenue, 0)) AS revenue
      FROM base
      WHERE ga_session_id IS NOT NULL
      GROUP BY 1,2
    )
    SELECT
      event_date, source, medium,
      COUNT(DISTINCT session_key) AS sessions,
      SUM(purchases) AS purchases,
      SUM(revenue) AS revenue
    FROM sessions
    GROUP BY 1,2,3
    """


def fetch(a, b):
    if a > b:
        return pd.DataFrame(columns=["event_date","source","medium","sessions","purchases","revenue"])
    print(f"BigQuery refresh {a.date()} ~ {b.date()}")
    df = bigquery.Client(project=PROJECT).query(query(a, b)).to_dataframe()
    if df.empty:
        return df
    df["event_date"] = pd.to_datetime(df["event_date"])
    df["source"] = df["source"].fillna("(direct)").astype(str)
    df["medium"] = df["medium"].fillna("(none)").astype(str)
    for c in ["sessions","purchases","revenue"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df


def load_cache():
    if not CACHE.exists() or CACHE.stat().st_size == 0:
        return pd.DataFrame(columns=["event_date","source","medium","sessions","purchases","revenue"])
    df = pd.read_csv(CACHE)
    df["event_date"] = pd.to_datetime(df["event_date"])
    return df


def replace_range(cache, fresh, a, b):
    keep = cache[(cache.event_date < a) | (cache.event_date > b)] if not cache.empty else cache
    return pd.concat([keep, fresh], ignore_index=True)


def main():
    cache = load_cache()
    required_start, required_end = LY_START, END
    full_backfill = cache.empty or cache.event_date.min() > LY_START or cache.event_date.max() < (END - pd.Timedelta(days=7))

    if full_backfill:
        print("Cache missing/incomplete: one-time backfill")
        ty = fetch(DATA_START, END)
        ly = fetch(LY_START, LY_END)
        cache = pd.concat([ty, ly], ignore_index=True)
    else:
        refresh_start = max(DATA_START, END - pd.Timedelta(days=2))
        fresh_ty = fetch(refresh_start, END)
        cache = replace_range(cache, fresh_ty, refresh_start, END)

        ly_refresh_start = refresh_start - pd.DateOffset(years=1)
        ly_refresh_end = END - pd.DateOffset(years=1)
        fresh_ly = fetch(ly_refresh_start, ly_refresh_end)
        cache = replace_range(cache, fresh_ly, ly_refresh_start, ly_refresh_end)

    # Keep only what the dashboard can use, plus a small buffer for safe rolling updates.
    floor = required_start - pd.Timedelta(days=7)
    ceil = required_end
    cache = cache[(cache.event_date >= floor) & (cache.event_date <= ceil)].copy()
    cache = cache.sort_values(["event_date","source","medium"]).reset_index(drop=True)
    cache["event_date"] = cache["event_date"].dt.strftime("%Y-%m-%d")
    cache.to_csv(CACHE, index=False, encoding="utf-8")
    print(f"Cache rows: {len(cache):,} -> {CACHE}")


if __name__ == "__main__":
    main()
