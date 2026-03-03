#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
OWNED Campaign → Product Explorer (GA4 BigQuery Export)
- EDM/LMS/KAKAO 세션 성과 + 구매 상품을 campaign/term 레벨로 집계
- 일자별 JSON 번들 생성 (GitHub Pages용 정적 사이트 data)

✅ Fixes (FINAL)
1) --recent-days 지원 (GitHub Actions incremental 모드 호환)
2) --project / --dataset CLI 지원 (env 없을 때도 동작)
3) --overwrite 지원: 해당 기간의 daily json + available_dates를 재작성
4) ✅ LMS/KAKAO/EDM channel labeling 안정화
5) ✅ KPI Revenue/Items가 Products 테이블 합계와 100% 일치하도록 수정
   - revenue = SUM(items.item_revenue)
   - items_purchased = SUM(items.quantity)
6) ✅ purchases는 transaction_id distinct 우선(중복 purchase event 방지), 없으면 event count fallback
7) ✅ recent-days의 end는 KST 전일(yesterday) 기준
"""

from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from google.cloud import bigquery


# -----------------------------
# Time helpers (KST)
# -----------------------------
KST = timedelta(hours=9)


def kst_today() -> date:
    return (datetime.utcnow() + KST).date()


def ymd(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def ymd_suffix(d: date) -> str:
    return d.strftime("%Y%m%d")


def parse_ymd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


# -----------------------------
# Channel inference
# -----------------------------
def infer_channel(utm_campaign: Optional[str], utm_term: Optional[str], utm_medium: Optional[str]) -> str:
    """
    EDM/LMS/KAKAO 라벨링:
    - 기본은 campaign/term/medium에서 keyword로 판단
    - 우선순위: medium -> campaign -> term
    """
    c = (utm_campaign or "").lower()
    t = (utm_term or "").lower()
    m = (utm_medium or "").lower()

    # medium-based
    if "kakao" in m or "kko" in m:
        return "KAKAO"
    if m == "lms" or m.startswith("lms") or "lms" in m:
        return "LMS"
    if "edm" in m or "email" in m:
        return "EDM"

    # campaign/term based
    if "kakao" in c or "kko" in c or "kakao" in t or "kko" in t:
        return "KAKAO"
    if c.startswith("lms") or "lms" in c or t.startswith("lms") or "lms" in t:
        return "LMS"
    if c.startswith("edm") or "edm" in c or t.startswith("edm") or "edm" in t or "email" in c or "email" in t:
        return "EDM"

    return "OTHER"


# -----------------------------
# MMDD parsing (for UI group)
# -----------------------------
MMDD_RE = re.compile(r"(?<!\d)(\d{4})(?!\d)")


def extract_mmdd(s: str) -> Optional[str]:
    if not s:
        return None
    m = MMDD_RE.search(s)
    return m.group(1) if m else None


# -----------------------------
# SQL builder
# -----------------------------
def build_query(project: str, dataset: str, start_suffix: str, end_suffix: str) -> str:
    table = f"`{project}.{dataset}.events_*`"
    return f"""
DECLARE start_suffix STRING DEFAULT '{start_suffix}';
DECLARE end_suffix   STRING DEFAULT '{end_suffix}';

WITH base AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_dt,
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
    ga_session_id,
    CONCAT(user_pseudo_id, '-', CAST(ga_session_id AS STRING)) AS session_key,

    -- 우선순위: collected_traffic_source > event_params(utm_*) > event_params(source/medium/..) > traffic_source
    NULLIF(COALESCE(cts_source,   ep_utm_source,   ep_source,   ts_source),   '') AS utm_source,
    NULLIF(COALESCE(cts_medium,   ep_utm_medium,   ep_medium,   ts_medium),   '') AS utm_medium,
    NULLIF(COALESCE(cts_campaign, ep_utm_campaign, ep_campaign, ts_campaign), '') AS utm_campaign,
    NULLIF(COALESCE(cts_term,     ep_utm_term,     ep_term),                 '') AS utm_term,

    event_name,
    purchase_revenue,
    transaction_id,
    items
  FROM base
  WHERE ga_session_id IS NOT NULL
),
session_dim AS (
  -- 세션별 기준일: MIN(event_dt) (세션 시작일)
  -- UTM: timestamp 오름차순 최초 non-null 값을 채택
  SELECT
    MIN(event_dt) AS session_date,
    user_pseudo_id,
    ga_session_id,
    session_key,
    ARRAY_AGG(utm_source   IGNORE NULLS ORDER BY event_timestamp LIMIT 1)[OFFSET(0)] AS utm_source,
    ARRAY_AGG(utm_medium   IGNORE NULLS ORDER BY event_timestamp LIMIT 1)[OFFSET(0)] AS utm_medium,
    ARRAY_AGG(utm_campaign IGNORE NULLS ORDER BY event_timestamp LIMIT 1)[OFFSET(0)] AS utm_campaign,
    ARRAY_AGG(utm_term     IGNORE NULLS ORDER BY event_timestamp LIMIT 1)[OFFSET(0)] AS utm_term
  FROM base2
  GROUP BY 2,3,4
),
sessions_owned AS (
  SELECT
    session_date AS date,
    user_pseudo_id,
    session_key,

    utm_source,
    utm_medium,
    utm_campaign,
    utm_term,

    LOWER(IFNULL(utm_campaign,'')) AS lc_campaign,
    LOWER(IFNULL(utm_term,''))     AS lc_term,
    LOWER(IFNULL(utm_source,''))   AS lc_source,
    LOWER(IFNULL(utm_medium,''))   AS lc_medium
  FROM session_dim
),
owned_labeled AS (
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
        lc_medium = 'lms' OR lc_medium LIKE 'lms%' OR lc_medium LIKE '%lms%'
        OR lc_campaign LIKE 'lms%' OR lc_term LIKE 'lms%'
        OR lc_campaign LIKE '%_lms%' OR lc_term LIKE '%_lms%'
      ) THEN 'LMS'
      WHEN (
        lc_campaign LIKE 'edm%' OR lc_term LIKE 'edm%'
        OR lc_campaign LIKE '%_edm%' OR lc_term LIKE '%_edm%'
        OR lc_medium LIKE '%edm%' OR lc_medium LIKE '%email%'
      ) THEN 'EDM'
      WHEN (
        lc_campaign LIKE 'kakao%' OR lc_term LIKE 'kakao%'
        OR lc_campaign LIKE '%_kakao%' OR lc_term LIKE '%_kakao%'
        OR lc_campaign LIKE 'kko%' OR lc_term LIKE 'kko%'
        OR lc_medium LIKE '%kakao%' OR lc_medium LIKE '%kko%'
      ) THEN 'KAKAO'
      ELSE 'OTHER'
    END AS channel,

    -- send_id: EDM/LMS는 campaign 우선, KAKAO는 term 우선
    CASE
      WHEN (
        lc_campaign LIKE 'kakao%' OR lc_term LIKE 'kakao%'
        OR lc_campaign LIKE '%_kakao%' OR lc_term LIKE '%_kakao%'
        OR lc_campaign LIKE 'kko%' OR lc_term LIKE 'kko%'
        OR lc_medium LIKE '%kakao%' OR lc_medium LIKE '%kko%'
      )
      THEN COALESCE(NULLIF(utm_term,''), NULLIF(utm_campaign,''), '(not_set)')
      ELSE COALESCE(NULLIF(utm_campaign,''), NULLIF(utm_term,''), '(not_set)')
    END AS campaign,

    COALESCE(NULLIF(utm_term,''), '-') AS term
  FROM sessions_owned
  WHERE 1=1
    AND (
      (lc_campaign LIKE 'edm%' OR lc_campaign LIKE 'lms%' OR lc_campaign LIKE 'kakao%' OR lc_campaign LIKE 'kko%')
      OR (lc_term LIKE 'edm%' OR lc_term LIKE 'lms%' OR lc_term LIKE 'kakao%' OR lc_term LIKE 'kko%')
      OR (lc_medium LIKE '%edm%' OR lc_medium LIKE '%lms%' OR lc_medium LIKE '%kakao%' OR lc_medium LIKE '%kko%' OR lc_medium LIKE '%email%')
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
  FROM owned_labeled
  GROUP BY 1,2,3,4
),
purchase_events AS (
  SELECT
    session_key,
    -- event-level revenue (ecommerce.purchase_revenue) and item-level revenue (SUM(items.item_revenue))
    SUM(purchase_revenue) AS revenue_evt,
    SUM((
      SELECT IFNULL(SUM(IFNULL(it.item_revenue,0)),0) FROM UNNEST(items) it
    )) AS revenue_items,

    -- ✅ purchases: prefer distinct transaction_id when available (prevents double-count)
    COUNT(DISTINCT NULLIF(transaction_id, '')) AS txn_cnt,
    COUNTIF(event_name='purchase') AS purchase_events_raw,

    -- item quantity (Products table sums this)
    SUM((
      SELECT IFNULL(SUM(IFNULL(it.quantity,0)),0) FROM UNNEST(items) it
    )) AS items_qty
  FROM base2
  WHERE event_name='purchase'
  GROUP BY 1
),
purchase_kpi AS (
  SELECT
    s.date,
    s.channel,
    s.campaign,
    s.term,
    SUM(IF(p.txn_cnt > 0, p.txn_cnt, IFNULL(p.purchase_events_raw,0))) AS purchases,
    -- ✅ KPI revenue must match Products (SUM(items.item_revenue))
    SUM(IFNULL(p.revenue_items,0)) AS revenue,
    SUM(IFNULL(p.items_qty,0)) AS items_purchased
  FROM owned_labeled s
  LEFT JOIN purchase_events p
    ON p.session_key = s.session_key
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
    SUM(IFNULL(it.quantity,0)) AS items,
    SUM(IFNULL(it.item_revenue,0)) AS revenue
  FROM owned_labeled s
  JOIN base2 b
    ON b.session_key = s.session_key
   AND b.event_name='purchase'
  CROSS JOIN UNNEST(b.items) it
  GROUP BY 1,2,3,4,5
),
final_campaign AS (
  SELECT
    k.date,
    k.channel,
    k.campaign,
    k.term,
    k.sessions,
    k.users,
    IFNULL(p.purchases,0) AS purchases,
    IFNULL(p.revenue,0) AS revenue,
    IFNULL(p.items_purchased,0) AS items_purchased
  FROM session_kpi k
  LEFT JOIN purchase_kpi p
    ON p.date=k.date AND p.channel=k.channel AND p.campaign=k.campaign AND p.term=k.term
)
SELECT
  'CAMPAIGN' AS row_type,
  CAST(date AS STRING) AS date,
  channel,
  campaign,
  term,
  sessions,
  users,
  purchases,
  revenue,
  items_purchased,
  NULL AS item_id,
  NULL AS item_name,
  NULL AS items,
  NULL AS item_revenue
FROM final_campaign

UNION ALL

SELECT
  'PRODUCT' AS row_type,
  CAST(date AS STRING) AS date,
  channel,
  campaign,
  term,
  NULL AS sessions,
  NULL AS users,
  NULL AS purchases,
  NULL AS revenue,
  NULL AS items_purchased,
  item_id,
  item_name,
  items,
  revenue AS item_revenue
FROM prod_rows
;
"""


# -----------------------------
# IO helpers
# -----------------------------
def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def write_json(p: Path, obj: Any) -> None:
    ensure_dir(p.parent)
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def list_owned_dates(owned_dir: Path) -> List[str]:
    # owned_YYYY-MM-DD.json
    dates = []
    for f in owned_dir.glob("owned_*.json"):
        m = re.match(r"owned_(\d{4}-\d{2}-\d{2})\.json$", f.name)
        if m:
            dates.append(m.group(1))
    return sorted(set(dates))


# -----------------------------
# BigQuery runner
# -----------------------------
@dataclass
class BQConfig:
    project: str
    dataset: str


def run_bq_query(client: bigquery.Client, query: str) -> pd.DataFrame:
    job = client.query(query)
    return job.result().to_dataframe(create_bqstorage_client=False)


# -----------------------------
# Build bundles
# -----------------------------
def build_day_bundle(df: pd.DataFrame, day: str) -> Dict[str, Any]:
    # split campaign/product rows
    camp = df[df["row_type"] == "CAMPAIGN"].copy()
    prod = df[df["row_type"] == "PRODUCT"].copy()

    # normalize numeric cols
    for col in ["sessions", "users", "purchases", "revenue", "items_purchased"]:
        if col in camp.columns:
            camp[col] = camp[col].fillna(0).astype(float)

    for col in ["items", "item_revenue"]:
        if col in prod.columns:
            prod[col] = prod[col].fillna(0).astype(float)

    # add group mmdd + year
    def _mmdd_from_campaign_term(c: str, t: str) -> Optional[str]:
        return extract_mmdd(c) or extract_mmdd(t)

    camp["year"] = camp["date"].str.slice(0, 4)
    camp["mmdd"] = camp.apply(lambda r: _mmdd_from_campaign_term(str(r["campaign"]), str(r["term"])), axis=1)

    # JSON payload
    campaigns: List[Dict[str, Any]] = []
    for _, r in camp.iterrows():
        campaigns.append(
            dict(
                date=r["date"],
                year=r["year"],
                mmdd=r["mmdd"],
                channel=r["channel"],
                campaign=r["campaign"],
                term=r["term"],
                sessions=int(r["sessions"]),
                users=int(r["users"]),
                purchases=int(r["purchases"]),
                revenue=float(r["revenue"]),
                items_purchased=int(r["items_purchased"]),
            )
        )

    products: List[Dict[str, Any]] = []
    for _, r in prod.iterrows():
        products.append(
            dict(
                date=r["date"],
                channel=r["channel"],
                campaign=r["campaign"],
                term=r["term"],
                item_id=r["item_id"],
                item_name=r["item_name"],
                items=int(r["items"]),
                revenue=float(r["item_revenue"]),
            )
        )

    return {"date": day, "campaigns": campaigns, "products": products}


def build_range(bq: BQConfig, start_d: date, end_d: date, site_dir: Path, overwrite: bool = False) -> None:
    client = bigquery.Client(project=bq.project)
    owned_dir = site_dir / "data" / "owned"
    ensure_dir(owned_dir)

    # Build one big query over suffix range
    q = build_query(bq.project, bq.dataset, ymd_suffix(start_d), ymd_suffix(end_d))
    df = run_bq_query(client, q)

    # Per-day bundles
    df_day_groups = df.groupby("date", dropna=True)
    wanted_days = []
    for day, g in df_day_groups:
        if not isinstance(day, str):
            day = str(day)
        wanted_days.append(day)
        bundle = build_day_bundle(g, day)
        out = owned_dir / f"owned_{day}.json"
        if overwrite or (not out.exists()):
            write_json(out, bundle)

    # available_dates.json (include all present on disk after writing)
    dates = list_owned_dates(owned_dir)
    write_json(owned_dir / "available_dates.json", dates)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default=os.getenv("BQ_PROJECT", ""), help="BigQuery project id")
    ap.add_argument("--dataset", default=os.getenv("BQ_DATASET", ""), help="BigQuery dataset (GA4 export dataset)")
    ap.add_argument("--start", default="", help="Start date YYYY-MM-DD")
    ap.add_argument("--end", default="", help="End date YYYY-MM-DD")
    ap.add_argument("--recent-days", type=int, default=0, help="Incremental window (days). End is yesterday(KST)")
    ap.add_argument("--site-dir", default="site", help="Output site directory (writes data/owned)")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing daily JSONs in the range")
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    if not args.project or not args.dataset:
        raise SystemExit("[ERROR] --project/--dataset (or env BQ_PROJECT/BQ_DATASET) required")

    site_dir = Path(args.site_dir).resolve()
    ensure_dir(site_dir / "data" / "owned")

    # determine range
    if args.recent_days and args.recent_days > 0:
        # ✅ BigQuery export is typically complete up to *yesterday* (KST)
        end_d = kst_today() - timedelta(days=1)
        start_d = end_d - timedelta(days=args.recent_days - 1)
    else:
        if not args.start or not args.end:
            raise SystemExit("[ERROR] Provide --start/--end OR --recent-days")
        start_d = parse_ymd(args.start)
        end_d = parse_ymd(args.end)

    if end_d < start_d:
        raise SystemExit("[ERROR] end < start")

    bq = BQConfig(project=args.project, dataset=args.dataset)

    print(f"[INFO] Build OWNED bundles: {ymd(start_d)} ~ {ymd(end_d)} | site_dir={site_dir}")
    build_range(bq, start_d, end_d, site_dir, overwrite=args.overwrite)

    owned_dir = site_dir / "data" / "owned"
    print(f"[OK] wrote: {owned_dir}")
    print(f"[OK] available_dates count: {len(list_owned_dates(owned_dir))}")


if __name__ == "__main__":
    main()
