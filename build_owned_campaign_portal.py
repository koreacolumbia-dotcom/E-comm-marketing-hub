#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
OWNED Campaign → Data Builder (GA4 BigQuery Export)
- EDM/LMS/KAKAO 세션 성과 + 구매 상품을 campaign/term 레벨로 집계
- 일자별 JSON 번들 생성 (정적 index.html에서 로드)

✅ 날짜 그룹핑 케이스 대응 (index.html에서 처리)
- campaign에 MMDD가 있는 케이스: EDM_0214 / LMS_ECOM_0214 ...
- campaign이 EDM처럼 고정이고 term에 MMDD가 있는 케이스: EDM + 0226_C75...

Outputs
- <site-dir>/data/owned/owned_YYYY-MM-DD.json
- <site-dir>/data/owned/available_dates.json
- <site-dir>/.nojekyll

Auth
- GOOGLE_SA_JSON_B64 (optional) : service account json base64

BQ config priority
1) CLI args: --project / --dataset
2) env: BQ_PROJECT / BQ_DATASET
"""

import os
import json
import base64
import argparse
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import List, Optional

import pandas as pd
from google.cloud import bigquery

# ----------------------------
# Time helpers (KST)
# ----------------------------
KST = timezone(timedelta(hours=9))

def kst_today() -> date:
    return datetime.now(tz=KST).date()

def suffix(d: date) -> str:
    return d.strftime("%Y%m%d")

def ymd(d: date) -> str:
    return d.strftime("%Y-%m-%d")

def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()

# ----------------------------
# Auth helpers (optional b64)
# ----------------------------
def maybe_write_sa_from_b64() -> None:
    b64 = (os.getenv("GOOGLE_SA_JSON_B64") or "").strip()
    if not b64:
        return
    p = Path("/tmp/ga_sa.json")
    p.write_bytes(base64.b64decode(b64))
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(p)

# ----------------------------
# BigQuery SQL
# ----------------------------
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
    ecommerce.purchase_revenue AS ecommerce_purchase_revenue,
    ecommerce.purchase_revenue_in_usd AS ecommerce_purchase_revenue_usd,
    (SELECT COALESCE(value.double_value, CAST(value.int_value AS FLOAT64), CAST(value.float_value AS FLOAT64))
     FROM UNNEST(event_params) WHERE key='value') AS ep_value,
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

    -- priority: collected_traffic_source > event_params(utm_*) > event_params(source/medium/..) > traffic_source
    NULLIF(COALESCE(cts_source,   ep_utm_source,   ep_source,   ts_source),   '') AS utm_source,
    NULLIF(COALESCE(cts_medium,   ep_utm_medium,   ep_medium,   ts_medium),   '') AS utm_medium,
    NULLIF(COALESCE(cts_campaign, ep_utm_campaign, ep_campaign, ts_campaign), '') AS utm_campaign,
    NULLIF(COALESCE(cts_term,     ep_utm_term,     ep_term),                 '') AS utm_term,

    event_name,
    COALESCE(ecommerce_purchase_revenue, ecommerce_purchase_revenue_usd, ep_value, 0) AS purchase_revenue,
    items
  FROM base
  WHERE ga_session_id IS NOT NULL
),

session_dim AS (
  -- per session: take first non-null utm fields by timestamp
  SELECT
    date,
    user_pseudo_id,
    ga_session_id,
    session_key,
    ARRAY_AGG(utm_source   IGNORE NULLS ORDER BY event_timestamp LIMIT 1)[OFFSET(0)] AS utm_source,
    ARRAY_AGG(utm_medium   IGNORE NULLS ORDER BY event_timestamp LIMIT 1)[OFFSET(0)] AS utm_medium,
    ARRAY_AGG(utm_campaign IGNORE NULLS ORDER BY event_timestamp LIMIT 1)[OFFSET(0)] AS utm_campaign,
    ARRAY_AGG(utm_term     IGNORE NULLS ORDER BY event_timestamp LIMIT 1)[OFFSET(0)] AS utm_term
  FROM base2
  GROUP BY 1,2,3,4
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

# ----------------------------
# Helpers
# ----------------------------
def read_existing_dates(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        j = json.loads(path.read_text(encoding="utf-8"))
        return {d for d in (j.get("available_dates") or []) if isinstance(d, str) and d}
    except Exception:
        return set()

def write_available_dates(path: Path, dates: set[str]) -> None:
    path.write_text(json.dumps({"available_dates": sorted(dates)}, ensure_ascii=False, indent=2), encoding="utf-8")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--site-dir", required=True, help="output site dir (e.g. reports/owned_portal)")
    ap.add_argument("--start", default="", help="YYYY-MM-DD (default: yesterday KST)")
    ap.add_argument("--end", default="", help="YYYY-MM-DD (default: yesterday KST)")
    ap.add_argument("--write-empty-days", action="store_true", help="write empty daily json as well")
    ap.add_argument("--overwrite", action="store_true", help="overwrite owned_YYYY-MM-DD.json if exists")
    ap.add_argument("--project", default="", help="BigQuery project id (optional; overrides env BQ_PROJECT)")
    ap.add_argument("--dataset", default="", help="BigQuery dataset (optional; overrides env BQ_DATASET)")
    args = ap.parse_args()

    maybe_write_sa_from_b64()

    project = (args.project or os.getenv("BQ_PROJECT") or "").strip()
    dataset = (args.dataset or os.getenv("BQ_DATASET") or "").strip()
    if not project or not dataset:
        raise SystemExit("[ERROR] Please set --project/--dataset or env BQ_PROJECT/BQ_DATASET")

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

    print(f"[INFO] OWNED backfill: {ymd(start_d)} ~ {ymd(end_d)} ({'overwrite' if args.overwrite else 'skip existing'})")

    sql = build_sql(project, dataset, suffix(start_d), suffix(end_d))
    client = bigquery.Client(project=project)
    df = client.query(sql).result().to_dataframe(create_bqstorage_client=True)

    site_dir = Path(args.site_dir)
    data_dir = site_dir / "data" / "owned"
    data_dir.mkdir(parents=True, exist_ok=True)
    (site_dir / ".nojekyll").write_text("", encoding="utf-8")

    if df.empty:
        print("[WARN] Query returned no rows.")
        # still keep available_dates.json if exists
        av_path = data_dir / "available_dates.json"
        if not av_path.exists():
            write_available_dates(av_path, set())
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

    av_path = data_dir / "available_dates.json"
    available = read_existing_dates(av_path)

    # days to write
    cur = start_d
    while cur <= end_d:
        d = ymd(cur)
        out_path = data_dir / f"owned_{d}.json"

        if out_path.exists() and (not args.overwrite):
            # still make sure available_dates has it
            available.add(d)
            cur += timedelta(days=1)
            continue

        k_rows = kpi_df[kpi_df["date"] == d][
            ["channel", "campaign", "term", "sessions", "users", "purchases", "kpi_revenue", "items_purchased"]
        ].to_dict(orient="records")

        p_rows = prod_df[prod_df["date"] == d][
            ["channel", "campaign", "term", "item_id", "item_name", "prod_items", "prod_revenue"]
        ].to_dict(orient="records")

        if (not k_rows) and (not p_rows) and (not args.write_empty_days):
            cur += timedelta(days=1)
            continue

        kpi_rows = [{
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
            "channel": r.get("channel"),
            "campaign": r.get("campaign"),
            "term": r.get("term"),
            "item_id": r.get("item_id"),
            "item_name": r.get("item_name"),
            "prod_items": int(r.get("prod_items", 0)),
            "prod_revenue": float(r.get("prod_revenue", 0.0)),
        } for r in p_rows]

        out = {"date": d, "kpi": kpi_rows, "prod": prod_rows}
        out_path.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
        available.add(d)

        cur += timedelta(days=1)

    write_available_dates(av_path, available)
    print(f"[OK] Wrote JSON to: {data_dir}")
    print(f"[OK] available_dates: {len(available)}")

if __name__ == "__main__":
    main()
