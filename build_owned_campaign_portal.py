#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
OWNED Campaign Explorer — Data Builder (JSON only)

- Pulls GA4 BigQuery Export (events_*) and builds daily JSON bundles for the Owned portal.
- Writes ONLY:
    <site_dir>/data/owned/owned_YYYY-MM-DD.json
    <site_dir>/data/owned/available_dates.json
  (Does NOT write/overwrite index.html)

JSON schema (per day):
{
  "date": "YYYY-MM-DD",
  "kpi": [
    {"date","channel","campaign","term","sessions","users","purchases","revenue","items_purchased"}
  ],
  "prod": [
    {"date","channel","campaign","term","item_id","item_name","prod_items","prod_revenue"}
  ]
}

Channel mapping:
- LMS: utm_medium contains 'lms' (e.g., LMS_ECOM, lms, lms_xxx)
- EDM: utm_medium in (edm,email) or contains 'edm'/'email'
- KAKAO: utm_source or utm_medium contains 'kakao'

Notes
- MMDD grouping is handled by index.html on the client:
  it extracts MMDD from campaign first, then term.
  (Supports cases like EDM_0214 + term=Birdley..., and campaign=EDM + term=0226_SKU...)

Env
- BQ_PROJECT: BigQuery project id
- BQ_DATASET: GA4 export dataset (contains events_* tables)
- GOOGLE_SA_JSON_B64: optional; base64-encoded service account JSON. If provided, written to /tmp/ga_sa.json.

Usage
python build_owned_campaign_data_only_FINAL.py --site-dir reports/owned_portal --start 2026-02-20 --end 2026-02-26
"""

import os
import json
import base64
import argparse
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import List

import pandas as pd
from google.cloud import bigquery


KST = timezone(timedelta(hours=9))


def kst_today() -> date:
    return datetime.now(tz=KST).date()


def suffix(d: date) -> str:
    return d.strftime("%Y%m%d")


def ymd(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def maybe_write_sa_from_b64() -> None:
    b64 = (os.getenv("GOOGLE_SA_JSON_B64") or "").strip()
    if not b64:
        return
    p = Path("/tmp/ga_sa.json")
    p.write_bytes(base64.b64decode(b64))
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(p)


def build_sql(project: str, dataset: str, start_suffix: str, end_suffix: str) -> str:
    table = f"`{project}.{dataset}.events_*`"
    return f"""
DECLARE start_suffix STRING DEFAULT '{start_suffix}';
DECLARE end_suffix   STRING DEFAULT '{end_suffix}';

WITH base AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS date,
    event_timestamp,
    user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_id') AS ga_session_id,

    collected_traffic_source.manual_source        AS cts_source,
    collected_traffic_source.manual_medium        AS cts_medium,
    collected_traffic_source.manual_campaign_name AS cts_campaign,
    collected_traffic_source.manual_term          AS cts_term,

    traffic_source.source AS ts_source,
    traffic_source.medium AS ts_medium,
    traffic_source.name   AS ts_campaign,

    (SELECT value.string_value FROM UNNEST(event_params) WHERE key='source')   AS ep_source,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key='medium')   AS ep_medium,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key='campaign') AS ep_campaign,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key='term')     AS ep_term,

    (SELECT value.string_value FROM UNNEST(event_params) WHERE key='utm_source')   AS ep_utm_source,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key='utm_medium')   AS ep_utm_medium,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key='utm_campaign') AS ep_utm_campaign,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key='utm_term')     AS ep_utm_term,

    event_name,
    ecommerce.purchase_revenue AS purchase_revenue,
    items
  FROM {table}
  WHERE _TABLE_SUFFIX BETWEEN start_suffix AND end_suffix
),

base2 AS (
  SELECT
    date,
    event_timestamp,
    user_pseudo_id,
    ga_session_id,
    CONCAT(user_pseudo_id, '-', CAST(ga_session_id AS STRING)) AS session_key,

    -- Priority: collected_traffic_source > event_params(utm_*) > event_params(source/medium/..) > traffic_source
    NULLIF(COALESCE(cts_source,   ep_utm_source,   ep_source,   ts_source),   '') AS utm_source,
    NULLIF(COALESCE(cts_medium,   ep_utm_medium,   ep_medium,   ts_medium),   '') AS utm_medium,
    NULLIF(COALESCE(cts_campaign, ep_utm_campaign, ep_campaign, ts_campaign), '') AS utm_campaign,
    NULLIF(COALESCE(cts_term,     ep_utm_term,     ep_term),                 '') AS utm_term,

    event_name,
    IFNULL(purchase_revenue, 0) AS purchase_revenue,
    items
  FROM base
  WHERE ga_session_id IS NOT NULL
),

session_dim AS (
  -- Session-scope UTM: take the first non-null in timestamp order
  SELECT
    date,
    user_pseudo_id,
    session_key,
    ARRAY_AGG(utm_source   IGNORE NULLS ORDER BY event_timestamp LIMIT 1)[OFFSET(0)] AS utm_source,
    ARRAY_AGG(utm_medium   IGNORE NULLS ORDER BY event_timestamp LIMIT 1)[OFFSET(0)] AS utm_medium,
    ARRAY_AGG(utm_campaign IGNORE NULLS ORDER BY event_timestamp LIMIT 1)[OFFSET(0)] AS utm_campaign,
    ARRAY_AGG(utm_term     IGNORE NULLS ORDER BY event_timestamp LIMIT 1)[OFFSET(0)] AS utm_term
  FROM base2
  GROUP BY 1,2,3
),

sessions_owned AS (
  SELECT
    date,
    user_pseudo_id,
    session_key,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_term,

    CASE
      WHEN (
        LOWER(IFNULL(utm_medium,'')) = 'lms'
        OR LOWER(IFNULL(utm_medium,'')) LIKE 'lms%'
        OR LOWER(IFNULL(utm_medium,'')) LIKE '%lms%'
      ) THEN 'LMS'
      WHEN (
        LOWER(IFNULL(utm_medium,'')) IN ('edm','email')
        OR LOWER(IFNULL(utm_medium,'')) LIKE 'edm%'
        OR LOWER(IFNULL(utm_medium,'')) LIKE '%edm%'
        OR LOWER(IFNULL(utm_medium,'')) LIKE 'email%'
        OR LOWER(IFNULL(utm_medium,'')) LIKE '%email%'
      ) THEN 'EDM'
      WHEN LOWER(IFNULL(utm_source,'')) LIKE '%kakao%' OR LOWER(IFNULL(utm_medium,'')) LIKE '%kakao%' THEN 'KAKAO'
      ELSE 'OTHER'
    END AS channel,

    NULLIF(utm_campaign,'') AS campaign,
    NULLIF(utm_term,'') AS term
  FROM session_dim
  WHERE (
    LOWER(IFNULL(utm_medium,'')) IN ('lms','edm','email')
    OR LOWER(IFNULL(utm_medium,'')) LIKE 'lms%'
    OR LOWER(IFNULL(utm_medium,'')) LIKE '%lms%'
    OR LOWER(IFNULL(utm_medium,'')) LIKE 'edm%'
    OR LOWER(IFNULL(utm_medium,'')) LIKE '%edm%'
    OR LOWER(IFNULL(utm_medium,'')) LIKE 'email%'
    OR LOWER(IFNULL(utm_medium,'')) LIKE '%email%'
    OR LOWER(IFNULL(utm_source,'')) LIKE '%kakao%'
    OR LOWER(IFNULL(utm_medium,'')) LIKE '%kakao%'
  )
),

session_kpi AS (
  SELECT
    date,
    channel,
    campaign,
    term,
    COUNT(DISTINCT session_key) AS sessions,
    COUNT(DISTINCT user_pseudo_id) AS users
  FROM sessions_owned
  WHERE COALESCE(campaign, term) IS NOT NULL
  GROUP BY 1,2,3,4
),

purchase_kpi AS (
  SELECT
    s.date,
    s.channel,
    s.campaign,
    s.term,
    COUNT(1) AS purchases,
    SUM(b.purchase_revenue) AS revenue
  FROM sessions_owned s
  JOIN base2 b
    ON b.session_key = s.session_key
   AND b.date = s.date
   AND b.event_name = 'purchase'
  GROUP BY 1,2,3,4
),

items_kpi AS (
  SELECT
    s.date,
    s.channel,
    s.campaign,
    s.term,
    SUM(IFNULL(it.quantity,0)) AS items_purchased
  FROM sessions_owned s
  JOIN base2 b
    ON b.session_key = s.session_key
   AND b.date = s.date
   AND b.event_name = 'purchase'
  CROSS JOIN UNNEST(b.items) it
  GROUP BY 1,2,3,4
),

prod_rows AS (
  SELECT
    s.date,
    s.channel,
    s.campaign,
    s.term,
    CAST(it.item_id AS STRING) AS item_id,
    ANY_VALUE(it.item_name) AS item_name,
    SUM(IFNULL(it.quantity,0)) AS prod_items,
    SUM(IFNULL(it.item_revenue,0)) AS prod_revenue
  FROM sessions_owned s
  JOIN base2 b
    ON b.session_key = s.session_key
   AND b.date = s.date
   AND b.event_name = 'purchase'
  CROSS JOIN UNNEST(b.items) it
  GROUP BY 1,2,3,4,5
),

kpi_final AS (
  SELECT
    k.date,
    k.channel,
    k.campaign,
    k.term,
    k.sessions,
    k.users,
    IFNULL(p.purchases, 0) AS purchases,
    IFNULL(p.revenue, 0)   AS revenue,
    IFNULL(i.items_purchased, 0) AS items_purchased
  FROM session_kpi k
  LEFT JOIN purchase_kpi p USING (date, channel, campaign, term)
  LEFT JOIN items_kpi i   USING (date, channel, campaign, term)
)

SELECT
  'kpi' AS row_type,
  date,
  channel,
  campaign,
  term,
  sessions,
  users,
  purchases,
  revenue AS kpi_revenue,
  items_purchased,
  NULL AS item_id,
  NULL AS item_name,
  NULL AS prod_items,
  NULL AS prod_revenue
FROM kpi_final

UNION ALL

SELECT
  'prod' AS row_type,
  date,
  channel,
  campaign,
  term,
  NULL AS sessions,
  NULL AS users,
  NULL AS purchases,
  NULL AS kpi_revenue,
  NULL AS items_purchased,
  item_id,
  item_name,
  prod_items,
  prod_revenue
FROM prod_rows
;
"""


def write_available_dates(data_dir: Path, dates: List[str]) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "available_dates.json").write_text(
        json.dumps({"available_dates": sorted(dates)}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--site-dir", required=True, help="site root that contains data/owned/")
    ap.add_argument("--start", default="", help="YYYY-MM-DD (default: yesterday KST)")
    ap.add_argument("--end", default="", help="YYYY-MM-DD (default: yesterday KST)")
    ap.add_argument("--write-empty-days", action="store_true", help="write empty daily json as well")
    args = ap.parse_args()

    maybe_write_sa_from_b64()

    project = (os.getenv("BQ_PROJECT") or "").strip()
    dataset = (os.getenv("BQ_DATASET") or "").strip()
    if not project or not dataset:
        raise SystemExit("[ERROR] Please set env BQ_PROJECT and BQ_DATASET")

    if args.start:
        start_d = parse_date(args.start)
    else:
        start_d = kst_today() - timedelta(days=1)

    if args.end:
        end_d = parse_date(args.end)
    else:
        end_d = kst_today() - timedelta(days=1)

    if end_d < start_d:
        start_d, end_d = end_d, start_d

    sql = build_sql(project, dataset, suffix(start_d), suffix(end_d))
    client = bigquery.Client(project=project)
    df = client.query(sql).result().to_dataframe(create_bqstorage_client=True)

    site_dir = Path(args.site_dir)
    data_dir = site_dir / "data" / "owned"
    data_dir.mkdir(parents=True, exist_ok=True)
    (site_dir / ".nojekyll").write_text("", encoding="utf-8")

    available_set = set()
    existing_path = data_dir / "available_dates.json"
    if existing_path.exists():
        try:
            existing = json.loads(existing_path.read_text(encoding="utf-8"))
            for dd in (existing.get("available_dates") or []):
                if isinstance(dd, str) and dd:
                    available_set.add(dd)
        except Exception:
            pass

    # Prepare days list
    days_to_write: List[date] = []
    cur = start_d
    while cur <= end_d:
        days_to_write.append(cur)
        cur += timedelta(days=1)

    if df.empty:
        # still refresh available_dates.json (keep existing)
        write_available_dates(data_dir, sorted(available_set))
        print("[WARN] Query returned no rows. No daily JSON written.")
        return

    df["date"] = df["date"].astype(str)
    kpi_df = df[df["row_type"] == "kpi"].copy()
    prod_df = df[df["row_type"] == "prod"].copy()

    for col in ["sessions", "users", "purchases", "kpi_revenue", "items_purchased"]:
        if col in kpi_df.columns:
            kpi_df[col] = pd.to_numeric(kpi_df[col], errors="coerce").fillna(0)

    for col in ["prod_items", "prod_revenue"]:
        if col in prod_df.columns:
            prod_df[col] = pd.to_numeric(prod_df[col], errors="coerce").fillna(0)

    for day in days_to_write:
        d = ymd(day)

        k_rows = kpi_df[kpi_df["date"] == d][
            ["date", "channel", "campaign", "term", "sessions", "users", "purchases", "kpi_revenue", "items_purchased"]
        ].to_dict(orient="records")

        p_rows = prod_df[prod_df["date"] == d][
            ["date", "channel", "campaign", "term", "item_id", "item_name", "prod_items", "prod_revenue"]
        ].to_dict(orient="records")

        if (not k_rows) and (not p_rows) and (not args.write_empty_days):
            continue

        kpi_rows = [{
            "date": r.get("date"),
            "channel": r.get("channel"),
            "campaign": r.get("campaign"),
            "term": r.get("term"),
            "sessions": int(r.get("sessions", 0)),
            "users": int(r.get("users", 0)),
            "purchases": int(r.get("purchases", 0)),
            "revenue": float(r.get("kpi_revenue", 0.0)),
            "items_purchased": int(r.get("items_purchased", 0)),
        } for r in k_rows]

        prod_rows = [{
            "date": r.get("date"),
            "channel": r.get("channel"),
            "campaign": r.get("campaign"),
            "term": r.get("term"),
            "item_id": r.get("item_id"),
            "item_name": r.get("item_name"),
            "prod_items": int(r.get("prod_items", 0)),
            "prod_revenue": float(r.get("prod_revenue", 0.0)),
        } for r in p_rows]

        out = {"date": d, "kpi": kpi_rows, "prod": prod_rows}
        (data_dir / f"owned_{d}.json").write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
        available_set.add(d)

    write_available_dates(data_dir, sorted(available_set))
    print(f"[OK] Wrote JSON to: {data_dir}")
    print(f"[OK] Updated: {data_dir / 'available_dates.json'}")


if __name__ == "__main__":
    main()
