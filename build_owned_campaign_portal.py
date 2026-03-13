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
YYYYMMDD_RE = re.compile(r"(?<!\d)(20\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])(?!\d)")
YYMMDD_RE = re.compile(r"(?<!\d)(\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])(?!\d)")
PLAIN_MMDD_RE = re.compile(r"(?<!\d)(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])(?!\d)")


def extract_mmdd(s: str) -> Optional[str]:
    if not s:
        return None
    m = PLAIN_MMDD_RE.search(str(s))
    return f"{m.group(1)}{m.group(2)}" if m else None


def extract_group_year_mmdd(*values: Any, fallback_date: Optional[str] = None) -> tuple[str, str, bool]:
    fallback_date = str(fallback_date or "")
    fallback_year = fallback_date[:4] if len(fallback_date) >= 4 else ""

    for raw in values:
        s = str(raw or "").strip()
        if not s:
            continue

        m8 = YYYYMMDD_RE.search(s)
        if m8:
            return m8.group(1), f"{m8.group(2)}{m8.group(3)}", True

        m6 = YYMMDD_RE.search(s)
        if m6:
            return f"20{m6.group(1)}", f"{m6.group(2)}{m6.group(3)}", True

        m4 = PLAIN_MMDD_RE.search(s)
        if m4:
            return fallback_year, f"{m4.group(1)}{m4.group(2)}", True

    return "", "", False


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
    IFNULL(ecommerce.total_item_quantity, 0) AS total_item_quantity,
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

    NULLIF(COALESCE(cts_source,   ep_utm_source,   ep_source,   ts_source),   '') AS utm_source,
    NULLIF(COALESCE(cts_medium,   ep_utm_medium,   ep_medium,   ts_medium),   '') AS utm_medium,
    NULLIF(COALESCE(cts_campaign, ep_utm_campaign, ep_campaign, ts_campaign), '') AS utm_campaign,
    NULLIF(COALESCE(cts_term,     ep_utm_term,     ep_term),                 '') AS utm_term,

    event_name,
    purchase_revenue,
    total_item_quantity,
    transaction_id,
    items
  FROM base
  WHERE ga_session_id IS NOT NULL
),
session_dim AS (
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
    COALESCE(NULLIF(utm_campaign,''), '(not_set)') AS campaign,
    COALESCE(NULLIF(utm_term,''), '-') AS term,
    CASE
      WHEN (
        lc_campaign LIKE 'kakao%' OR lc_term LIKE 'kakao%'
        OR lc_campaign LIKE '%_kakao%' OR lc_term LIKE '%_kakao%'
        OR lc_campaign LIKE 'kko%' OR lc_term LIKE 'kko%'
        OR lc_medium LIKE '%kakao%' OR lc_medium LIKE '%kko%'
      )
      THEN COALESCE(NULLIF(utm_term,''), NULLIF(utm_campaign,''), '(not_set)')
      ELSE COALESCE(NULLIF(utm_campaign,''), NULLIF(utm_term,''), '(not_set)')
    END AS send_id
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
    send_id,
    campaign,
    term,
    COUNT(DISTINCT session_key) AS sessions,
    COUNT(DISTINCT user_pseudo_id) AS users
  FROM owned_labeled
  GROUP BY 1,2,3,4,5
),
purchase_events AS (
  SELECT
    session_key,
    SUM(purchase_revenue) AS revenue_evt,
    SUM((SELECT IFNULL(SUM(IFNULL(it.item_revenue,0)),0) FROM UNNEST(items) it)) AS revenue_items,
    COUNT(DISTINCT NULLIF(transaction_id, '')) AS txn_cnt,
    COUNTIF(event_name='purchase') AS purchase_events_raw,
    SUM((SELECT IFNULL(SUM(IFNULL(it.quantity,0)),0) FROM UNNEST(items) it)) AS items_qty,
    SUM(IFNULL(total_item_quantity,0)) AS items_qty_evt
  FROM base2
  WHERE event_name='purchase'
  GROUP BY 1
),
purchase_kpi AS (
  SELECT
    s.date,
    s.channel,
    s.send_id,
    s.campaign,
    s.term,
    SUM(IF(p.txn_cnt > 0, p.txn_cnt, IFNULL(p.purchase_events_raw,0))) AS purchases,
    SUM(CASE WHEN IFNULL(p.revenue_items,0) > 0 THEN p.revenue_items ELSE IFNULL(p.revenue_evt,0) END) AS revenue,
    SUM(CASE WHEN IFNULL(p.items_qty,0) > 0 THEN p.items_qty ELSE IFNULL(p.items_qty_evt,0) END) AS items_purchased
  FROM owned_labeled s
  LEFT JOIN purchase_events p
    ON p.session_key = s.session_key
  GROUP BY 1,2,3,4,5
),
prod_rows AS (
  SELECT
    s.date,
    s.channel,
    s.send_id,
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
  GROUP BY 1,2,3,4,5,6
),
send_rollup AS (
  SELECT
    date,
    channel,
    send_id,
    SUM(sessions) AS send_sessions,
    SUM(users) AS send_users,
    SUM(IFNULL(purchases,0)) AS send_purchases,
    SUM(IFNULL(revenue,0)) AS send_revenue,
    SUM(IFNULL(items_purchased,0)) AS send_items
  FROM (
    SELECT
      k.date,
      k.channel,
      k.send_id,
      k.sessions,
      k.users,
      IFNULL(p.purchases,0) AS purchases,
      IFNULL(p.revenue,0) AS revenue,
      IFNULL(p.items_purchased,0) AS items_purchased
    FROM session_kpi k
    LEFT JOIN purchase_kpi p
      ON p.date=k.date AND p.channel=k.channel AND p.send_id=k.send_id AND p.campaign=k.campaign AND p.term=k.term
  )
  GROUP BY 1,2,3
),
final_campaign AS (
  SELECT
    k.date,
    k.channel,
    k.send_id,
    k.campaign,
    k.term,
    k.sessions,
    k.users,
    IFNULL(p.purchases,0) AS purchases,
    IFNULL(p.revenue,0) AS revenue,
    IFNULL(p.items_purchased,0) AS items_purchased,
    CASE
      WHEN ROW_NUMBER() OVER (PARTITION BY k.date, k.channel, k.send_id ORDER BY k.campaign, k.term) = 1 THEN 1
      ELSE 0
    END AS send_count,
    CASE
      WHEN ROW_NUMBER() OVER (PARTITION BY k.date, k.channel, k.send_id ORDER BY k.campaign, k.term) = 1 THEN IFNULL(sr.send_sessions,0)
      ELSE 0
    END AS avg_leverage
  FROM session_kpi k
  LEFT JOIN purchase_kpi p
    ON p.date=k.date AND p.channel=k.channel AND p.send_id=k.send_id AND p.campaign=k.campaign AND p.term=k.term
  LEFT JOIN send_rollup sr
    ON sr.date=k.date AND sr.channel=k.channel AND sr.send_id=k.send_id
)
SELECT
  'CAMPAIGN' AS row_type,
  CAST(date AS STRING) AS date,
  channel,
  send_id,
  campaign,
  term,
  sessions,
  users,
  purchases,
  revenue,
  items_purchased,
  send_count,
  avg_leverage,
  '' AS message_title,
  '' AS message_body,
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
  send_id,
  campaign,
  term,
  NULL AS sessions,
  NULL AS users,
  NULL AS purchases,
  NULL AS revenue,
  NULL AS items_purchased,
  NULL AS send_count,
  NULL AS avg_leverage,
  '' AS message_title,
  '' AS message_body,
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
    camp = df[df["row_type"] == "CAMPAIGN"].copy()
    prod = df[df["row_type"] == "PRODUCT"].copy()

    for col in ["sessions", "users", "purchases", "revenue", "items_purchased", "send_count", "avg_leverage"]:
        if col in camp.columns:
            camp[col] = camp[col].fillna(0).astype(float)

    for col in ["items", "item_revenue"]:
        if col in prod.columns:
            prod[col] = prod[col].fillna(0).astype(float)

    camp_group = camp.apply(
        lambda r: extract_group_year_mmdd(
            r.get("campaign", ""),
            r.get("send_id", ""),
            r.get("term", ""),
            fallback_date=str(r.get("date", "")),
        ),
        axis=1,
        result_type="expand",
    )
    camp["year"] = camp_group[0]
    camp["mmdd"] = camp_group[1]
    camp["has_group_mmdd"] = camp_group[2].astype(bool)

    prod_group = prod.apply(
        lambda r: extract_group_year_mmdd(
            r.get("campaign", ""),
            r.get("send_id", ""),
            r.get("term", ""),
            fallback_date=str(r.get("date", "")),
        ),
        axis=1,
        result_type="expand",
    )
    prod["year"] = prod_group[0]
    prod["mmdd"] = prod_group[1]
    prod["has_group_mmdd"] = prod_group[2].astype(bool)

    campaigns: List[Dict[str, Any]] = []
    for _, r in camp.iterrows():
        campaigns.append(
            dict(
                date=str(r["date"]),
                year=str(r["year"]),
                mmdd=str(r["mmdd"]),
                has_group_mmdd=bool(r.get("has_group_mmdd", False)),
                channel=str(r["channel"]),
                send_id=str(r.get("send_id", "") or ""),
                campaign=str(r["campaign"]),
                term=str(r["term"]),
                sessions=int(r["sessions"]),
                users=int(r["users"]),
                purchases=int(r["purchases"]),
                revenue=float(r["revenue"]),
                items_purchased=int(r["items_purchased"]),
                send_count=int(r.get("send_count", 0) or 0),
                avg_leverage=float(r.get("avg_leverage", 0) or 0),
                message_title=str(r.get("message_title", "") or ""),
                message_body=str(r.get("message_body", "") or ""),
            )
        )

    products: List[Dict[str, Any]] = []
    for _, r in prod.iterrows():
        products.append(
            dict(
                date=str(r["date"]),
                year=str(r["year"]),
                mmdd=str(r["mmdd"]),
                has_group_mmdd=bool(r.get("has_group_mmdd", False)),
                channel=str(r["channel"]),
                send_id=str(r.get("send_id", "") or ""),
                campaign=str(r["campaign"]),
                term=str(r["term"]),
                item_id=r["item_id"],
                item_name=r["item_name"],
                items=int(r["items"]),
                revenue=float(r["item_revenue"]),
            )
        )

    return {"date": day, "campaigns": campaigns, "products": products}


def build_range(
    bq: BQConfig,
    start_d: date,
    end_d: date,
    site_dir: Path,
    overwrite: bool = False,
    merge_prev_year: bool = False,
) -> None:
    client = bigquery.Client(project=bq.project)
    owned_dir = site_dir / "data" / "owned"
    ensure_dir(owned_dir)

    q = build_query(bq.project, bq.dataset, ymd_suffix(start_d), ymd_suffix(end_d))
    df = run_bq_query(client, q)

    df_day_groups = df.groupby("date", dropna=True)
    for day, g in df_day_groups:
        if not isinstance(day, str):
            day = str(day)
        bundle = build_day_bundle(g, day)
        if merge_prev_year:
            bundle = merge_previous_year_bundle_rows(owned_dir, bundle, day)
        out = owned_dir / f"owned_{day}.json"
        if overwrite or (not out.exists()):
            write_json(out, bundle)

    dates = list_owned_dates(owned_dir)
    write_json(owned_dir / "available_dates.json", {"available_dates": dates})


# -----------------------------
# Previous-year merge helper
# -----------------------------
def _unique_dict_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        try:
            key = json.dumps(row, ensure_ascii=False, sort_keys=True, default=str)
        except Exception:
            key = str(row)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _filter_exact_year_rows(rows: List[Dict[str, Any]], year_str: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        row_year = str((row or {}).get("year") or "").strip()
        row_date = str((row or {}).get("date") or "").strip()
        if row_year == year_str or row_date.startswith(f"{year_str}-"):
            out.append(row)
    return out


def merge_previous_year_bundle_rows(owned_dir: Path, bundle: Dict[str, Any], day: str) -> Dict[str, Any]:
    """
    Current-day OWNED bundle에 전년도 동일 MM-DD bundle의 rows를 합쳐 넣는다.

    Why:
    - build_summary.py의 OWNED YTD YoY는 latest owned_YYYY-MM-DD.json 하나만 읽는다.
    - 따라서 latest bundle 안에 당해년도 rows + 전년도 same-MM-DD rows가 같이 있어야
      LY / YoY 계산이 정상 동작한다.

    Example:
    - writing owned_2026-03-09.json
    - also load owned_2025-03-09.json if exists
    - merge only 2025 rows from that file into current bundle
    """
    try:
        cur_d = parse_ymd(day)
    except Exception:
        return bundle

    prev_day = f"{cur_d.year - 1}-{cur_d.strftime('%m-%d')}"
    prev_path = owned_dir / f"owned_{prev_day}.json"
    if not prev_path.exists():
        return bundle

    try:
        prev_obj = json.loads(prev_path.read_text(encoding="utf-8"))
    except Exception:
        return bundle

    prev_year = str(cur_d.year - 1)
    cur_campaigns = list(bundle.get("campaigns") or [])
    cur_products = list(bundle.get("products") or [])

    prev_campaigns = _filter_exact_year_rows(list(prev_obj.get("campaigns") or []), prev_year)
    prev_products = _filter_exact_year_rows(list(prev_obj.get("products") or []), prev_year)

    bundle["campaigns"] = _unique_dict_rows(cur_campaigns + prev_campaigns)
    bundle["products"] = _unique_dict_rows(cur_products + prev_products)
    bundle["previous_year_merged_from"] = str(prev_path)
    return bundle


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default=os.getenv("BQ_PROJECT", ""), help="BigQuery project id")
    ap.add_argument("--dataset", default=os.getenv("BQ_DATASET", ""), help="BigQuery dataset (GA4 export dataset)")
    ap.add_argument("--start", default="", help="Start date YYYY-MM-DD")
    ap.add_argument("--end", default="", help="End date YYYY-MM-DD")
    ap.add_argument("--recent-days", type=int, default=0, help="Incremental window (days). End is yesterday(KST)")
    ap.add_argument("--site-dir", default="site", help="Output site directory (writes data/owned)")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing daily JSONs in the range")
    ap.add_argument("--merge-prev-year", action="store_true", help="Also merge previous-year same-MM-DD rows into each daily bundle")
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
    build_range(
        bq,
        start_d,
        end_d,
        site_dir,
        overwrite=args.overwrite,
        merge_prev_year=args.merge_prev_year,
    )

    owned_dir = site_dir / "data" / "owned"
    print(f"[OK] wrote: {owned_dir}")
    print(f"[OK] available_dates count: {len(list_owned_dates(owned_dir))}")


if __name__ == "__main__":
    main()
