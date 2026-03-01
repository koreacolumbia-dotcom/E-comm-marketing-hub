#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
OWNED Campaign → Product Explorer (GA4 BigQuery Export)
- EDM/LMS/KAKAO 세션 성과 + 구매 상품을 campaign/term 레벨로 집계
- 일자별 JSON 번들 생성 (GitHub Pages용 정적 사이트 data)

✅ Fixes (v6)
1) --recent-days 지원 (GitHub Actions incremental 모드 호환)
2) --project / --dataset CLI 지원 (env 없을 때도 동작)
3) --overwrite 지원: 해당 기간의 daily json + available_dates를 재작성
4) ✅ LMS/KAKAO/EDM revenue=0 issue fix
   - purchase join에서 date 조건 제거 (session_key로만 결합)
   - session_date = MIN(event_date)로 세션 기준일 고정
   - revenue fallback: ecommerce.purchase_revenue가 0/NULL이면 items.item_revenue 합으로 보강
5) OWNED 채널 판별 강화: medium/source 뿐 아니라 campaign/term 키워드도 보조 신호로 사용

Output
- {site_dir}/data/owned/owned_YYYY-MM-DD.json
- {site_dir}/data/owned/available_dates.json
- {site_dir}/.nojekyll  (Pages용)

Env (optional)
- BQ_PROJECT, BQ_DATASET
- GOOGLE_SA_JSON_B64
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


def env(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or default or "").strip()


def build_sql(project: str, dataset: str, start_suffix: str, end_suffix: str) -> str:
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
        OR lc_campaign LIKE '%lms%' OR lc_term LIKE '%lms%'
      ) THEN 'LMS'

      WHEN (
        lc_medium IN ('edm','email') OR lc_medium LIKE 'edm%' OR lc_medium LIKE '%edm%'
        OR lc_medium LIKE 'email%' OR lc_medium LIKE '%email%'
        OR lc_campaign LIKE '%edm%' OR lc_campaign LIKE '%email%'
        OR lc_campaign LIKE 'edm_%' OR lc_campaign LIKE 'email_%'
        OR lc_term LIKE '%edm%' OR lc_term LIKE '%email%'
      ) THEN 'EDM'

      WHEN (
        lc_source LIKE '%kakao%' OR lc_medium LIKE '%kakao%'
        OR lc_campaign LIKE '%kakao%' OR lc_term LIKE '%kakao%'
      ) THEN 'KAKAO'

      ELSE 'OTHER'
    END AS channel,

    NULLIF(utm_campaign,'') AS campaign,
    NULLIF(utm_term,'')     AS term
  FROM sessions_owned
  WHERE (
    lc_medium IN ('lms','edm','email')
    OR lc_medium LIKE 'lms%' OR lc_medium LIKE '%lms%'
    OR lc_medium LIKE 'edm%' OR lc_medium LIKE '%edm%'
    OR lc_medium LIKE 'email%' OR lc_medium LIKE '%email%'
    OR lc_source LIKE '%kakao%' OR lc_medium LIKE '%kakao%'
    OR lc_campaign LIKE '%edm%' OR lc_campaign LIKE '%email%' OR lc_campaign LIKE '%lms%' OR lc_campaign LIKE '%kakao%'
    OR lc_term LIKE '%edm%' OR lc_term LIKE '%email%' OR lc_term LIKE '%lms%' OR lc_term LIKE '%kakao%'
  )
),
session_kpi AS (
  SELECT
    date, channel, campaign, term,
    COUNT(DISTINCT session_key) AS sessions,
    COUNT(DISTINCT user_pseudo_id) AS users
  FROM owned_labeled
  WHERE COALESCE(campaign, term) IS NOT NULL
  GROUP BY 1,2,3,4
),
purchase_events AS (
  SELECT
    session_key,
    SUM(purchase_revenue) AS revenue_evt,
    SUM((
      SELECT IFNULL(SUM(IFNULL(it.item_revenue,0)),0) FROM UNNEST(items) it
    )) AS revenue_items,
    COUNTIF(event_name='purchase') AS purchase_events,
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
    SUM(IFNULL(p.purchase_events,0)) AS purchases,
    SUM(CASE
          WHEN IFNULL(p.revenue_evt,0) > 0 THEN p.revenue_evt
          ELSE IFNULL(p.revenue_items,0)
        END) AS revenue,
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
    SUM(IFNULL(it.quantity,0)) AS prod_items,
    SUM(IFNULL(it.item_revenue,0)) AS prod_revenue
  FROM owned_labeled s
  JOIN base2 b
    ON b.session_key = s.session_key
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
    IFNULL(p.items_purchased, 0) AS items_purchased
  FROM session_kpi k
  LEFT JOIN purchase_kpi p USING (date, channel, campaign, term)
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


def delete_daily_files(data_dir: Path, dates: List[str]) -> None:
    for d in dates:
        p = data_dir / f"owned_{d}.json"
        if p.exists():
            try:
                p.unlink()
            except Exception:
                pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--site-dir", required=True, help="output site dir (e.g. reports/owned_portal)")
    ap.add_argument("--start", default="", help="YYYY-MM-DD (default: yesterday KST)")
    ap.add_argument("--end", default="", help="YYYY-MM-DD (default: yesterday KST)")
    ap.add_argument("--recent-days", type=int, default=None, help="build recent N days (ending today KST)")
    ap.add_argument("--write-empty-days", action="store_true", help="write empty daily json as well")
    ap.add_argument("--overwrite", action="store_true", help="overwrite daily files + available_dates for the range")
    ap.add_argument("--project", default="", help="BQ project id (or env BQ_PROJECT)")
    ap.add_argument("--dataset", default="", help="BQ dataset id (or env BQ_DATASET)")
    args = ap.parse_args()

    maybe_write_sa_from_b64()

    project = (args.project or env("BQ_PROJECT")).strip()
    dataset = (args.dataset or env("BQ_DATASET")).strip()
    if not project or not dataset:
        raise SystemExit("[ERROR] Please set --project/--dataset or env BQ_PROJECT/BQ_DATASET")

    # resolve date range
    if args.recent_days and args.recent_days > 0:
        end_d = kst_today()
        start_d = end_d - timedelta(days=args.recent_days - 1)
    else:
        start_d = parse_date(args.start) if args.start else (kst_today() - timedelta(days=1))
        end_d = parse_date(args.end) if args.end else (kst_today() - timedelta(days=1))

    if end_d < start_d:
        start_d, end_d = end_d, start_d

    site_dir = Path(args.site_dir)
    data_dir = site_dir / "data" / "owned"
    data_dir.mkdir(parents=True, exist_ok=True)
    (site_dir / ".nojekyll").write_text("", encoding="utf-8")

    # days to write list
    days_to_write: List[date] = []
    cur = start_d
    while cur <= end_d:
        days_to_write.append(cur)
        cur += timedelta(days=1)
    date_strs = [ymd(d) for d in days_to_write]

    if args.overwrite:
        delete_daily_files(data_dir, date_strs)

    sql = build_sql(project, dataset, suffix(start_d), suffix(end_d))
    client = bigquery.Client(project=project)
    df = client.query(sql).result().to_dataframe(create_bqstorage_client=True)

    # existing available dates (merge unless overwrite)
    available_set = set()
    existing_path = data_dir / "available_dates.json"
    if (not args.overwrite) and existing_path.exists():
        try:
            existing = json.loads(existing_path.read_text(encoding="utf-8"))
            for dd in (existing.get("available_dates") or []):
                if isinstance(dd, str) and dd:
                    available_set.add(dd)
        except Exception:
            pass

    if df.empty:
        if args.write_empty_days:
            for d in date_strs:
                out = {"date": d, "kpi": [], "prod": []}
                (data_dir / f"owned_{d}.json").write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
                available_set.add(d)
        write_available_dates(data_dir, sorted(available_set))
        print("[WARN] Query returned no rows.")
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

    for d in date_strs:
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
    print(f"[OK] Wrote owned bundles: {date_strs[0]} ~ {date_strs[-1]}  (overwrite={args.overwrite})")
    print(f"[OK] Site dir: {site_dir}")


if __name__ == "__main__":
    main()
