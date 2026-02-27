#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
OWNED Campaign → Product Explorer (GA4 BigQuery Export)
- EDM/LMS/KAKAO 세션 성과 + 구매 상품을 campaign/term 레벨로 집계
- 일자별 JSON 번들 + index.html 생성 (GitHub Pages용 정적 사이트)

✅ 핵심 UX를 위한 데이터 구조
- KPI row: {date, channel, campaign, term, sessions, users, purchases, revenue, items_purchased}
- PROD row:{date, channel, campaign, term, item_id, item_name, prod_items, prod_revenue}

✅ 날짜 그룹핑 케이스 대응
- campaign에 MMDD가 있는 케이스: EDM_0214 / LMS_ECOM_0214 ...
- campaign이 EDM처럼 고정이고 term에 MMDD가 있는 케이스: EDM + 0226_C75...

Env
- BQ_PROJECT: BigQuery project id
- BQ_DATASET: GA4 export dataset (events_* 있는 dataset)
- GOOGLE_SA_JSON_B64: (optional) service account json base64
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

    -- 우선순위: collected_traffic_source > event_params(utm_*) > event_params(source/medium/..) > traffic_source
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
  -- 세션별로 timestamp 오름차순 최초값을 채택(세션 스코프 UTM 복원)
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
    -- owned만 남기기(EDM/LMS/KAKAO)
    LOWER(IFNULL(utm_medium,'')) IN ('lms','edm','email')
    OR LOWER(IFNULL(utm_medium,'')) LIKE 'lms%'
    OR LOWER(IFNULL(utm_medium,'')) LIKE '%lms%'
    OR LOWER(IFNULL(utm_medium,'')) LIKE 'edm%'
    OR LOWER(IFNULL(utm_medium,'')) LIKE '%edm%'
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

purchase_sessions AS (
  -- purchase 이벤트가 발생한 session_key
  SELECT DISTINCT
    date,
    session_key
  FROM base2
  WHERE event_name = 'purchase'
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
# index.html (portal)
# ----------------------------
def build_index_html() -> str:
    # 아래 index.html은 "campaign/term 기반" 최종 UI
    return INDEX_HTML


def build_hub_html_placeholder() -> str:
    return """<!doctype html><html lang="ko"><meta charset="utf-8"/>
<title>Hub placeholder</title><body style="font-family:sans-serif;padding:24px">
<h2>Hub placeholder</h2>
<p>필요하면 기존 허브 링크로 교체해도 돼.</p>
</body></html>"""


def write_available_dates(data_dir: Path, dates: List[str]) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "available_dates.json").write_text(
        json.dumps({"available_dates": sorted(dates)}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ----------------------------
# Main
# ----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--site-dir", required=True, help="output site dir (e.g. reports/owned_portal)")
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
    job = client.query(sql)
    df = job.result().to_dataframe(create_bqstorage_client=True)

    site_dir = Path(args.site_dir)
    data_dir = site_dir / "data" / "owned"
    data_dir.mkdir(parents=True, exist_ok=True)

    (site_dir / ".nojekyll").write_text("", encoding="utf-8")
    (site_dir / "index.html").write_text(build_index_html(), encoding="utf-8")
    (site_dir / "hub.html").write_text(build_hub_html_placeholder(), encoding="utf-8")

    if df.empty:
        print("[WARN] Query returned no rows.")
        write_available_dates(data_dir, [])
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

    # merge with existing available dates
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

    # days we should write
    days_to_write: List[date] = []
    cur = start_d
    while cur <= end_d:
        days_to_write.append(cur)
        cur += timedelta(days=1)

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

        kpi_rows = []
        for r in k_rows:
            kpi_rows.append({
                "date": r.get("date"),
                "channel": r.get("channel"),
                "campaign": r.get("campaign"),
                "term": r.get("term"),
                "sessions": int(r.get("sessions", 0)),
                "users": int(r.get("users", 0)),
                "purchases": int(r.get("purchases", 0)),
                "revenue": float(r.get("kpi_revenue", 0.0)),
                "items_purchased": int(r.get("items_purchased", 0)),
            })

        prod_rows = []
        for r in p_rows:
            prod_rows.append({
                "date": r.get("date"),
                "channel": r.get("channel"),
                "campaign": r.get("campaign"),
                "term": r.get("term"),
                "item_id": r.get("item_id"),
                "item_name": r.get("item_name"),
                "prod_items": int(r.get("prod_items", 0)),
                "prod_revenue": float(r.get("prod_revenue", 0.0)),
            })

        out = {"date": d, "kpi": kpi_rows, "prod": prod_rows}
        (data_dir / f"owned_{d}.json").write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
        available_set.add(d)

    write_available_dates(data_dir, sorted(available_set))
    print(f"[OK] Wrote site to: {site_dir}")


# ----------------------------
# Embedded index.html (v2)
# ----------------------------
INDEX_HTML = r"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>OWNED Campaign → Product Explorer</title>
  <script src="https://cdn.tailwindcss.com"></script>

  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200;400;600;800&display=swap');
    :root { --brand:#002d72; --bg0:#f6f8fb; --bg1:#eef3f9; }

    html, body { height: 100%; overflow: auto; }
    body{
      background: linear-gradient(180deg, var(--bg0), var(--bg1));
      font-family: 'Plus Jakarta Sans', sans-serif;
      color:#0f172a;
      min-height:100vh;
    }
    .glass{
      background: rgba(255,255,255,0.72);
      backdrop-filter: blur(18px);
      border: 1px solid rgba(255,255,255,0.85);
      border-radius: 26px;
      box-shadow: 0 24px 60px rgba(0,45,114,0.07);
    }
    .chip{
      border: 1px solid rgba(148,163,184,0.35);
      background: rgba(255,255,255,0.78);
      border-radius: 999px;
      padding: 8px 12px;
      font-weight: 900;
      font-size: 12px;
      color: #0f172a;
      transition: all .15s ease;
      user-select:none;
      white-space: nowrap;
    }
    .chip:hover{ transform: translateY(-1px); box-shadow: 0 10px 24px rgba(0,45,114,0.08); }
    .chip.active{
      background: rgba(0,45,114,0.08);
      border-color: rgba(0,45,114,0.28);
      color: var(--brand);
    }
    .btn{
      border-radius: 14px;
      padding: 10px 14px;
      font-weight: 900;
      font-size: 12px;
      border: 1px solid rgba(148,163,184,0.28);
      background: rgba(255,255,255,0.88);
      transition: all .15s ease;
      user-select:none;
      white-space: nowrap;
      display:inline-flex;
      align-items:center;
      gap:8px;
    }
    .btn:hover{ transform: translateY(-1px); box-shadow: 0 10px 24px rgba(0,45,114,0.08); }
    .btn-primary{ background: #002d72; border-color: #002d72; color: white; }
    .muted{ color:#64748b; }
    .small-label{
      font-size: 10px;
      font-weight: 900;
      letter-spacing: .22em;
      text-transform: uppercase;
      color: #94a3b8;
    }
    input[type="date"], input[type="text"], select{
      border: 1px solid rgba(148,163,184,0.28);
      background: rgba(255,255,255,0.90);
      border-radius: 14px;
      padding: 10px 12px;
      font-weight: 900;
      font-size: 12px;
      color: #0f172a;
      outline: none;
      width: 100%;
    }
    input[type="date"]:focus, input[type="text"]:focus, select:focus{
      border-color: rgba(0,45,114,0.40);
      box-shadow: 0 0 0 4px rgba(0,45,114,0.08);
    }
    .notice{
      border: 1px solid rgba(148,163,184,0.25);
      background: rgba(255,255,255,0.86);
      border-radius: 18px;
      padding: 10px 12px;
      font-weight: 800;
      font-size: 12px;
      color:#0f172a;
      display:none;
      align-items:center;
      gap:10px;
      box-shadow: 0 10px 28px rgba(0,0,0,0.06);
    }
    .notice .dot{
      width:10px; height:10px; border-radius:999px;
      background: rgba(2,45,114,0.85);
      flex: 0 0 auto;
    }
    .notice .x{
      margin-left:auto;
      border: 1px solid rgba(148,163,184,0.28);
      background: rgba(255,255,255,0.85);
      border-radius: 12px;
      padding: 6px 10px;
      font-weight: 900;
      cursor:pointer;
    }
    .kpi-card{
      border: 1px solid rgba(148,163,184,0.22);
      background: rgba(255,255,255,0.86);
      border-radius: 18px;
      padding: 12px 14px;
      box-shadow: 0 12px 32px rgba(0,0,0,0.06);
    }
    .kpi-title{ font-size: 11px; font-weight: 900; letter-spacing: .12em; text-transform: uppercase; color:#94a3b8; }
    .kpi-value{ font-size: 20px; font-weight: 900; margin-top: 6px; }
    .kpi-sub{ font-size: 12px; font-weight: 800; color:#64748b; margin-top: 2px; }
    table{
      width:100%;
      border-collapse: separate;
      border-spacing: 0;
      overflow:hidden;
      border-radius: 18px;
      background: rgba(255,255,255,0.82);
      border: 1px solid rgba(148,163,184,0.22);
      box-shadow: 0 12px 34px rgba(0,0,0,0.06);
    }
    thead th{
      text-align:left;
      font-size: 11px;
      font-weight: 900;
      letter-spacing: .12em;
      text-transform: uppercase;
      color:#64748b;
      padding: 12px 14px;
      border-bottom: 1px solid rgba(148,163,184,0.20);
      background: rgba(255,255,255,0.9);
      position: sticky;
      top: 0;
      z-index: 5;
    }
    tbody td{
      padding: 12px 14px;
      border-bottom: 1px solid rgba(148,163,184,0.16);
      font-weight: 800;
      font-size: 13px;
      color:#0f172a;
    }
    tbody tr:hover td{ background: rgba(0,45,114,0.04); }
    .pill{
      display:inline-flex;
      align-items:center;
      gap:6px;
      padding: 6px 10px;
      border-radius: 999px;
      border: 1px solid rgba(148,163,184,0.22);
      background: rgba(255,255,255,0.88);
      font-weight: 900;
      font-size: 12px;
    }
    .mono{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }
    .sec-title{ font-size: 11px; font-weight: 900; letter-spacing: .16em; text-transform: uppercase; color:#94a3b8; }
  </style>
</head>

<body class="p-6 md:p-10">
  <div class="max-w-7xl mx-auto">
    <div class="flex flex-col md:flex-row md:items-end md:justify-between gap-4 mb-6">
      <div>
        <div class="text-4xl font-black tracking-tight">OWNED Campaign Explorer</div>
        <div class="muted font-semibold mt-1">EDM/LMS/KAKAO 캠페인(날짜) → TERM 드릴다운 → 구매 상품</div>
      </div>

      <div class="flex items-center gap-3 flex-wrap justify-end">
        <a class="btn" href="./hub.html" title="(옵션) 기존 리포트 허브">Hub</a>
        <button id="btnReload" class="btn btn-primary" type="button">새로고침</button>
      </div>
    </div>

    <div id="notice" class="notice mb-4">
      <div class="dot"></div>
      <div id="noticeText">-</div>
      <button id="noticeClose" class="x" type="button">닫기</button>
    </div>

    <div class="glass p-5">
      <!-- Controls -->
      <div class="flex flex-wrap items-center gap-2 mb-4">
        <div class="small-label">View</div>
        <button id="chipDaily" class="chip active" type="button">DAILY</button>
        <button id="chipWeek" class="chip" type="button">WEEK</button>
        <button id="chipRange" class="chip" type="button">RANGE</button>

        <div class="ml-2 small-label">Date</div>
        <div id="dailyBox" class="w-[160px]"><input id="datePicker" type="date" /></div>
        <div id="weekHint" class="hidden text-xs font-extrabold text-slate-500">(Mon ~ Sun)</div>

        <div id="rangeBox" class="hidden items-center gap-2">
          <div class="small-label">From</div>
          <div class="w-[160px]"><input id="startPicker" type="date" /></div>
          <div class="small-label">To</div>
          <div class="w-[160px]"><input id="endPicker" type="date" /></div>

          <button id="btnApplyRange" class="btn btn-primary" type="button">적용</button>
          <button id="btn7d" class="btn" type="button">7D</button>
          <button id="btn30d" class="btn" type="button">30D</button>
          <button id="btnMTD" class="btn" type="button">MTD</button>
          <button id="btnWEEK" class="btn" type="button">WEEK</button>
        </div>

        <button id="btnPrev" class="btn" type="button">◀</button>
        <button id="btnNext" class="btn" type="button">▶</button>
        <button id="btnToday" class="btn" type="button">오늘</button>

        <div class="ml-2 small-label">Channel</div>
        <button id="chipAll" class="chip active" type="button">ALL</button>
        <button id="chipEDM" class="chip" type="button">EDM</button>
        <button id="chipLMS" class="chip" type="button">LMS</button>
        <button id="chipKAKAO" class="chip" type="button">KAKAO</button>

        <div class="ml-2 small-label">Search</div>
        <div class="w-[260px]"><input id="q" type="text" placeholder="campaign/term/0226 검색…" /></div>

        <div class="ml-auto flex items-center gap-2">
          <div class="small-label">Top</div>
          <select id="topN" class="w-[120px]">
            <option value="30" selected>30</option>
            <option value="60">60</option>
            <option value="120">120</option>
            <option value="300">300</option>
          </select>
        </div>
      </div>

      <!-- Campaign groups -->
      <div class="mb-4">
        <div class="small-label mb-2">Campaigns (grouped by MMDD)</div>

        <div id="secEDM" class="mb-3">
          <div class="sec-title mb-2">EDM · By Date (MMDD)</div>
          <div id="wrapEDMDate" class="flex flex-wrap gap-2"></div>
        </div>

        <div id="secLMS" class="mb-3">
          <div class="sec-title mb-2">LMS · By Date (MMDD)</div>
          <div id="wrapLMSDate" class="flex flex-wrap gap-2"></div>
        </div>

        <div id="secKAKAO" class="mb-3">
          <div class="sec-title mb-2">KAKAO · By Date (MMDD)</div>
          <div id="wrapKakaoDate" class="flex flex-wrap gap-2"></div>
        </div>

        <div id="secOTHER">
          <div class="sec-title mb-2">Other campaigns</div>
          <div id="wrapOther" class="flex flex-wrap gap-2"></div>
        </div>
      </div>

      <!-- Terms (drilldown list for date group) -->
      <div id="termSection" class="hidden mb-5">
        <div class="flex items-center gap-2 mb-2">
          <div class="small-label">Terms</div>
          <div class="muted font-semibold text-sm">선택한 날짜(MMDD) 그룹의 개별 TERM 목록</div>
        </div>

        <div class="overflow-auto rounded-[18px]">
          <table>
            <thead>
              <tr>
                <th style="min-width:260px">Campaign</th>
                <th style="min-width:320px">Term</th>
                <th style="min-width:120px">Sessions</th>
                <th style="min-width:120px">Users</th>
                <th style="min-width:120px">Purchases</th>
                <th style="min-width:140px">Revenue</th>
                <th style="min-width:120px">Items</th>
              </tr>
            </thead>
            <tbody id="termTb">
              <tr><td colspan="7" class="muted font-semibold">날짜 그룹을 선택하면 TERM 목록이 표시돼.</td></tr>
            </tbody>
          </table>
        </div>

        <div class="muted font-semibold text-xs mt-2">
          TIP: 행 클릭 → 해당 (campaign+term) 단위로 KPI/상품이 드릴다운돼.
        </div>
      </div>

      <!-- KPI -->
      <div class="grid grid-cols-1 md:grid-cols-5 gap-3 mb-4">
        <div class="kpi-card">
          <div class="kpi-title">Sessions</div>
          <div id="kSessions" class="kpi-value">-</div>
          <div class="kpi-sub" id="kSessionsSub">-</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-title">Users</div>
          <div id="kUsers" class="kpi-value">-</div>
          <div class="kpi-sub" id="kUsersSub">-</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-title">Purchases</div>
          <div id="kPurchases" class="kpi-value">-</div>
          <div class="kpi-sub" id="kCvrSub">-</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-title">Revenue</div>
          <div id="kRevenue" class="kpi-value">-</div>
          <div class="kpi-sub" id="kAovSub">-</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-title">Items</div>
          <div id="kItems" class="kpi-value">-</div>
          <div class="kpi-sub" id="kItemsSub">-</div>
        </div>
      </div>

      <!-- Products -->
      <div class="flex items-center gap-2 mb-2">
        <div class="small-label">Products</div>
        <div id="selMeta" class="pill">
          <span class="mono" id="selChannel">-</span> ·
          <span class="mono" id="selCampaign">-</span> ·
          <span class="mono" id="selTerm">-</span>
          <span id="selPeriod" class="muted font-semibold"></span>
        </div>
      </div>

      <div class="overflow-auto rounded-[18px]">
        <table>
          <thead>
            <tr>
              <th style="min-width:220px">Item name</th>
              <th style="min-width:140px">Item id</th>
              <th style="min-width:120px">Items</th>
              <th style="min-width:140px">Revenue</th>
            </tr>
          </thead>
          <tbody id="tb">
            <tr><td colspan="4" class="muted font-semibold">TERM을 선택하면 상품 리스트가 표시돼.</td></tr>
          </tbody>
        </table>
      </div>

      <div class="muted font-semibold text-xs mt-3">
        데이터: GA4 BigQuery Export · JSON: owned_YYYY-MM-DD.json 누적 · WEEK/RANGE는 클라이언트 합산
      </div>
    </div>
  </div>

<script>
(function(){
  const notice = document.getElementById('notice');
  const noticeText = document.getElementById('noticeText');
  const noticeClose = document.getElementById('noticeClose');
  function showNotice(msg){ noticeText.textContent = msg; notice.style.display='flex'; }
  function hideNotice(){ notice.style.display='none'; noticeText.textContent='-'; }
  noticeClose.addEventListener('click', hideNotice);

  const btnReload = document.getElementById('btnReload');

  const chipDaily = document.getElementById('chipDaily');
  const chipWeek  = document.getElementById('chipWeek');
  const chipRange = document.getElementById('chipRange');

  const rangeBox  = document.getElementById('rangeBox');
  const dailyBox  = document.getElementById('dailyBox');
  const weekHint  = document.getElementById('weekHint');

  const datePicker  = document.getElementById('datePicker');
  const startPicker = document.getElementById('startPicker');
  const endPicker   = document.getElementById('endPicker');

  const btnApplyRange = document.getElementById('btnApplyRange');
  const btn7d = document.getElementById('btn7d');
  const btn30d = document.getElementById('btn30d');
  const btnMTD = document.getElementById('btnMTD');
  const btnWEEK = document.getElementById('btnWEEK');

  const btnPrev = document.getElementById('btnPrev');
  const btnNext = document.getElementById('btnNext');
  const btnToday = document.getElementById('btnToday');

  const chipAll = document.getElementById('chipAll');
  const chipEDM = document.getElementById('chipEDM');
  const chipLMS = document.getElementById('chipLMS');
  const chipKAKAO = document.getElementById('chipKAKAO');

  const secEDM = document.getElementById('secEDM');
  const secLMS = document.getElementById('secLMS');
  const secKAKAO = document.getElementById('secKAKAO');
  const secOTHER = document.getElementById('secOTHER');

  const q = document.getElementById('q');
  const topN = document.getElementById('topN');

  const wrapEDMDate = document.getElementById('wrapEDMDate');
  const wrapLMSDate = document.getElementById('wrapLMSDate');
  const wrapKakaoDate = document.getElementById('wrapKakaoDate');
  const wrapOther = document.getElementById('wrapOther');

  const termSection = document.getElementById('termSection');
  const termTb = document.getElementById('termTb');

  const tb = document.getElementById('tb');

  const kSessions = document.getElementById('kSessions');
  const kUsers = document.getElementById('kUsers');
  const kPurchases = document.getElementById('kPurchases');
  const kRevenue = document.getElementById('kRevenue');
  const kItems = document.getElementById('kItems');
  const kSessionsSub = document.getElementById('kSessionsSub');
  const kUsersSub = document.getElementById('kUsersSub');
  const kCvrSub = document.getElementById('kCvrSub');
  const kAovSub = document.getElementById('kAovSub');
  const kItemsSub = document.getElementById('kItemsSub');

  const selChannel = document.getElementById('selChannel');
  const selCampaign  = document.getElementById('selCampaign');
  const selTerm  = document.getElementById('selTerm');
  const selPeriod  = document.getElementById('selPeriod');

  function fmt(n){
    if(n === null || n === undefined) return '-';
    const x = Number(n);
    if(!isFinite(x)) return '-';
    return x.toLocaleString('en-US');
  }
  function fmtMoney(n){
    if(n === null || n === undefined) return '-';
    const x = Number(n);
    if(!isFinite(x)) return '-';
    return x.toLocaleString('en-US', { maximumFractionDigits: 0 });
  }

  function ymd(d){
    const y = d.getFullYear();
    const m = String(d.getMonth()+1).padStart(2,'0');
    const dd = String(d.getDate()).padStart(2,'0');
    return `${y}-${m}-${dd}`;
  }
  function parseYMD(s){
    const [y,m,d] = (s||'').split('-').map(Number);
    return new Date(y,(m||1)-1,d||1);
  }
  function addDays(d,n){ const x=new Date(d); x.setDate(x.getDate()+n); return x; }

  function startOfWeekMon(d){
    const x = new Date(d);
    const day = x.getDay(); // 0=Sun
    const diff = (day === 0) ? -6 : (1 - day);
    x.setDate(x.getDate() + diff);
    return x;
  }
  function endOfWeekSun(d){
    const s = startOfWeekMon(d);
    const e = new Date(s);
    e.setDate(e.getDate() + 6);
    return e;
  }

  // ✅ campaign/term에서 MMDD 추출 (campaign 우선, 없으면 term)
  function mmddFromText(txt){
    const raw = String(txt||'');
    if(!raw) return null;
    const tokens = raw.replace(/[^A-Za-z0-9]+/g,'_').split('_').filter(Boolean);
    for(const t of tokens){
      if(/^[0-9]{4}$/.test(t)){
        const mm = parseInt(t.slice(0,2),10);
        const dd = parseInt(t.slice(2,4),10);
        if(mm>=1 && mm<=12 && dd>=1 && dd<=31) return t;
      }
    }
    const m = raw.match(/^(\d{2})(\d{2})/);
    if(m){
      const mm = +m[1], dd = +m[2];
      if(mm>=1 && mm<=12 && dd>=1 && dd<=31) return `${m[1]}${m[2]}`;
    }
    return null;
  }
  function dateCodeOfRow(r){
    const c = mmddFromText(r.campaign);
    if(c) return c;
    return mmddFromText(r.term);
  }

  function clampToAvailable(dStr){
    if(!AVAILABLE || !AVAILABLE.length) return dStr;
    if(AVAILABLE.includes(dStr)) return dStr;
    for(let i=AVAILABLE.length-1;i>=0;i--){
      if(AVAILABLE[i] <= dStr) return AVAILABLE[i];
    }
    return AVAILABLE[0];
  }

  let VIEW = 'DAILY';     // DAILY | WEEK | RANGE
  let CHANNEL = 'ALL';    // ALL | EDM | LMS | KAKAO

  let RAW = null;         // {kpi:[], prod:[]}  (DAILY면 단일, WEEK/RANGE면 합산)
  let KPI = [];
  let SELECTED = null;    // {type:'single', channel, campaign, term} or {type:'group', channel, date_code}
  let AVAILABLE = null;   // ["YYYY-MM-DD", ...]

  function setViewActive(){
    [chipDaily, chipWeek, chipRange].forEach(el=>el.classList.remove('active'));
    if(VIEW==='DAILY'){
      chipDaily.classList.add('active');
      rangeBox.classList.add('hidden'); rangeBox.classList.remove('flex');
      dailyBox.classList.remove('hidden');
      weekHint.classList.add('hidden');
    }
    if(VIEW==='WEEK'){
      chipWeek.classList.add('active');
      rangeBox.classList.add('hidden'); rangeBox.classList.remove('flex');
      dailyBox.classList.remove('hidden');
      weekHint.classList.remove('hidden');
    }
    if(VIEW==='RANGE'){
      chipRange.classList.add('active');
      rangeBox.classList.remove('hidden'); rangeBox.classList.add('flex');
      dailyBox.classList.add('hidden');
      weekHint.classList.add('hidden');
    }
  }

  function setChipActive(){
    [chipAll, chipEDM, chipLMS, chipKAKAO].forEach(el=>el.classList.remove('active'));
    if(CHANNEL==='ALL') chipAll.classList.add('active');
    if(CHANNEL==='EDM') chipEDM.classList.add('active');
    if(CHANNEL==='LMS') chipLMS.classList.add('active');
    if(CHANNEL==='KAKAO') chipKAKAO.classList.add('active');
  }

  function toggleSections(){
    const showAll = (CHANNEL==='ALL');
    secEDM.style.display = (showAll || CHANNEL==='EDM') ? '' : 'none';
    secLMS.style.display = (showAll || CHANNEL==='LMS') ? '' : 'none';
    secKAKAO.style.display = (showAll || CHANNEL==='KAKAO') ? '' : 'none';
    // 니 요청: EDM 누르면 EDM만 나오게 → OTHER는 ALL에서만 표시
    secOTHER.style.display = (showAll) ? '' : 'none';
  }

  function periodLabel(){
    if(VIEW==='DAILY'){
      return ` · ${datePicker.value || '-'}`;
    }
    if(VIEW==='WEEK'){
      const cur = parseYMD(datePicker.value || ymd(new Date()));
      const s = ymd(startOfWeekMon(cur));
      const e = ymd(endOfWeekSun(cur));
      return ` · WEEK ${s} ~ ${e}`;
    }
    return ` · ${startPicker.value || '-'} ~ ${endPicker.value || '-'}`;
  }

  function filterKPI(){
    if(!RAW){ KPI=[]; return; }
    const qq = (q.value||'').trim().toLowerCase();
    KPI = (RAW.kpi||[]).filter(r=>{
      if(CHANNEL!=='ALL' && r.channel!==CHANNEL) return false;
      if(qq){
        const c = String(r.campaign||'').toLowerCase();
        const t = String(r.term||'').toLowerCase();
        const dc = String(dateCodeOfRow(r)||'');
        if(c.indexOf(qq)===-1 && t.indexOf(qq)===-1 && dc.indexOf(qq)===-1) return false;
      }
      return true;
    });
  }

  function makeChip(label, title, active, dataAttrs){
    const cls = active ? 'chip active' : 'chip';
    const attrs = Object.entries(dataAttrs||{}).map(([k,v])=>`data-${k}="${String(v).replace(/"/g,'&quot;')}"`).join(' ');
    return `<button class="${cls}" ${attrs} title="${title||''}">${label}</button>`;
  }

  function aggregateRow(rows){
    const out = {sessions:0, users:0, purchases:0, revenue:0, items_purchased:0};
    for(const r of rows){
      out.sessions += Number(r.sessions||0);
      out.users += Number(r.users||0);
      out.purchases += Number(r.purchases||0);
      out.revenue += Number(r.revenue||0);
      out.items_purchased += Number(r.items_purchased||0);
    }
    return out;
  }

  function renderCampaignButtons(){
    wrapEDMDate.innerHTML = '';
    wrapLMSDate.innerHTML = '';
    wrapKakaoDate.innerHTML = '';
    wrapOther.innerHTML = '';

    if(!KPI.length){
      const empty = `<div class="muted font-semibold text-sm">결과가 없어. (필터가 너무 좁거나, 해당 기간에 데이터가 없을 수 있어)</div>`;
      wrapEDMDate.innerHTML = empty;
      wrapLMSDate.innerHTML = empty;
      wrapKakaoDate.innerHTML = empty;
      wrapOther.innerHTML = empty;
      return;
    }

    const limit = Math.max(1, parseInt(topN.value||'30',10));

    const byDate = {EDM:new Map(), LMS:new Map(), KAKAO:new Map()};
    const others = [];

    for(const r of KPI){
      const dc = dateCodeOfRow(r);
      if(dc && (r.channel==='EDM' || r.channel==='LMS' || r.channel==='KAKAO')){
        const m = byDate[r.channel];
        if(!m.has(dc)) m.set(dc, []);
        m.get(dc).push(r);
      }else{
        others.push(r);
      }
    }

    function renderDateGroup(channel, wrapEl){
      const m = byDate[channel];
      const arr = Array.from(m.entries()).map(([dc, rows])=>{
        const a = aggregateRow(rows);
        return {dc, channel, rows, agg:a};
      }).sort((a,b)=> (b.agg.sessions||0)-(a.agg.sessions||0)).slice(0, limit);

      if(!arr.length){
        wrapEl.innerHTML = `<div class="muted font-semibold text-sm">-</div>`;
        return;
      }

      wrapEl.innerHTML = arr.map(x=>{
        const label = `${x.dc}`;
        const title = `${x.channel} · ${x.dc} · sessions:${x.agg.sessions} · terms:${x.rows.length}`;
        const active = (SELECTED && SELECTED.type==='group' && SELECTED.channel===x.channel && SELECTED.date_code===x.dc);
        return makeChip(label, title, active, {type:'group', channel:x.channel, dc:x.dc});
      }).join('');

      Array.from(wrapEl.querySelectorAll('button.chip')).forEach(btn=>{
        btn.addEventListener('click', ()=>{
          selectGroup(btn.getAttribute('data-channel'), btn.getAttribute('data-dc'));
        });
      });
    }

    renderDateGroup('EDM', wrapEDMDate);
    renderDateGroup('LMS', wrapLMSDate);
    renderDateGroup('KAKAO', wrapKakaoDate);

    // OTHER는 ALL에서만 표시(니 요구)
    const oRows = others.slice().sort((a,b)=> (b.sessions||0)-(a.sessions||0)).slice(0, limit);
    if(!oRows.length){
      wrapOther.innerHTML = `<div class="muted font-semibold text-sm">-</div>`;
    }else{
      wrapOther.innerHTML = oRows.map(r=>{
        const label = `${r.channel}_${r.campaign||''}_${r.term||''}`;
        const title = `${r.channel} · sessions:${r.sessions}`;
        const active = (SELECTED && SELECTED.type==='single' && SELECTED.channel===r.channel && SELECTED.campaign===r.campaign && SELECTED.term===r.term);
        return makeChip(label, title, active, {
          type:'single',
          channel:r.channel,
          campaign:encodeURIComponent(r.campaign||''),
          term:encodeURIComponent(r.term||'')
        });
      }).join('');

      Array.from(wrapOther.querySelectorAll('button.chip')).forEach(btn=>{
        btn.addEventListener('click', ()=>{
          selectSingle(
            btn.getAttribute('data-channel'),
            decodeURIComponent(btn.getAttribute('data-campaign')||''),
            decodeURIComponent(btn.getAttribute('data-term')||'')
          );
        });
      });
    }
  }

  function aggregateKPIForSelected(){
    selPeriod.textContent = periodLabel();

    if(!SELECTED || !RAW){
      kSessions.textContent='-'; kUsers.textContent='-'; kPurchases.textContent='-'; kRevenue.textContent='-'; kItems.textContent='-';
      kSessionsSub.textContent='-'; kUsersSub.textContent='-'; kCvrSub.textContent='-'; kAovSub.textContent='-'; kItemsSub.textContent='-';
      selChannel.textContent='-'; selCampaign.textContent='-'; selTerm.textContent='-';
      return;
    }

    let rows = [];
    if(SELECTED.type==='single'){
      rows = (RAW.kpi||[]).filter(x=> x.channel===SELECTED.channel && (x.campaign||'')===(SELECTED.campaign||'') && (x.term||'')===(SELECTED.term||''));
      selChannel.textContent = SELECTED.channel;
      selCampaign.textContent = SELECTED.campaign || '-';
      selTerm.textContent = SELECTED.term || '-';
    }else{
      rows = (RAW.kpi||[]).filter(x=> x.channel===SELECTED.channel && String(dateCodeOfRow(x)||'')===SELECTED.date_code);
      selChannel.textContent = SELECTED.channel;
      selCampaign.textContent = `${SELECTED.date_code} (group)`;
      selTerm.textContent = `terms:${rows.length}`;
    }

    if(!rows.length) return;
    const a = aggregateRow(rows);

    kSessions.textContent = fmt(a.sessions);
    kUsers.textContent = fmt(a.users);
    kPurchases.textContent = fmt(a.purchases);
    kRevenue.textContent = fmtMoney(a.revenue);
    kItems.textContent = fmt(a.items_purchased);

    const cvr = a.sessions ? (a.purchases/a.sessions*100) : 0;
    const aov = a.purchases ? (a.revenue/a.purchases) : 0;
    const ips = a.purchases ? (a.items_purchased/a.purchases) : 0;

    kSessionsSub.textContent = `-`;
    kUsersSub.textContent = (VIEW !== 'DAILY') ? 'Users: sum of daily (may overcount)' : '-';
    kCvrSub.textContent = `CVR: ${cvr.toFixed(2)}%`;
    kAovSub.textContent = `AOV: ${fmtMoney(aov)}`;
    kItemsSub.textContent = `Items/Order: ${ips.toFixed(2)}`;
  }

  function renderProducts(){
    if(!SELECTED || !RAW || SELECTED.type!=='single'){
      tb.innerHTML = `<tr><td colspan="4" class="muted font-semibold">TERM을 선택하면 상품 리스트가 표시돼.</td></tr>`;
      return;
    }

    const rows = (RAW.prod||[]).filter(r =>
      r.channel===SELECTED.channel &&
      (r.campaign||'')===(SELECTED.campaign||'') &&
      (r.term||'')===(SELECTED.term||'')
    );

    if(!rows.length){
      tb.innerHTML = `<tr><td colspan="4" class="muted font-semibold">구매 상품 데이터가 없어.</td></tr>`;
      return;
    }

    const m = new Map();
    for(const r of rows){
      const key = String(r.item_id||'');
      if(!m.has(key)){
        m.set(key, {item_id:r.item_id, item_name:r.item_name, prod_items:0, prod_revenue:0});
      }
      const cur = m.get(key);
      cur.prod_items += Number(r.prod_items||0);
      cur.prod_revenue += Number(r.prod_revenue||0);
      if(!cur.item_name && r.item_name) cur.item_name = r.item_name;
    }

    const out = Array.from(m.values()).sort((a,b)=> (b.prod_revenue||0)-(a.prod_revenue||0));

    tb.innerHTML = out.map(r=>`
      <tr>
        <td>${(r.item_name||'-')}</td>
        <td class="mono">${(r.item_id||'-')}</td>
        <td>${fmt(r.prod_items||0)}</td>
        <td>${fmtMoney(r.prod_revenue||0)}</td>
      </tr>
    `).join('');
  }

  function renderTerms(){
    if(!termSection || !termTb) return;

    if(!SELECTED || SELECTED.type!=='group' || !RAW){
      termSection.classList.add('hidden');
      termTb.innerHTML = `<tr><td colspan="7" class="muted font-semibold">날짜 그룹을 선택하면 TERM 목록이 표시돼.</td></tr>`;
      return;
    }

    const rows = (RAW.kpi||[])
      .filter(r => r.channel===SELECTED.channel && String(dateCodeOfRow(r)||'')===SELECTED.date_code)
      .slice()
      .sort((a,b)=> (Number(b.sessions||0) - Number(a.sessions||0)));

    termSection.classList.remove('hidden');

    if(!rows.length){
      termTb.innerHTML = `<tr><td colspan="7" class="muted font-semibold">해당 날짜 그룹에 TERM 데이터가 없어.</td></tr>`;
      return;
    }

    termTb.innerHTML = rows.map(r=>`
      <tr class="cursor-pointer" data-channel="${r.channel}" data-campaign="${encodeURIComponent(r.campaign||'')}" data-term="${encodeURIComponent(r.term||'')}">
        <td class="mono">${(r.campaign||'-')}</td>
        <td class="mono">${(r.term||'-')}</td>
        <td>${fmt(r.sessions||0)}</td>
        <td>${fmt(r.users||0)}</td>
        <td>${fmt(r.purchases||0)}</td>
        <td>${fmtMoney(r.revenue||0)}</td>
        <td>${fmt(r.items_purchased||0)}</td>
      </tr>
    `).join('');

    Array.from(termTb.querySelectorAll('tr[data-channel]')).forEach(tr=>{
      tr.addEventListener('click', ()=>{
        selectSingle(
          tr.getAttribute('data-channel'),
          decodeURIComponent(tr.getAttribute('data-campaign')||''),
          decodeURIComponent(tr.getAttribute('data-term')||'')
        );
      });
    });
  }

  function selectSingle(channel, campaign, term){
    SELECTED = {type:'single', channel, campaign, term};
    renderCampaignButtons();
    aggregateKPIForSelected();
    renderTerms();
    renderProducts();
  }

  function selectGroup(channel, date_code){
    SELECTED = {type:'group', channel, date_code};
    renderCampaignButtons();
    aggregateKPIForSelected();
    renderTerms();
    renderProducts();
  }

  async function fetchJson(url){
    const res = await fetch(url, {cache:'no-store'});
    if(!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  }

  function listDatesBetween(startStr, endStr){
    if(!AVAILABLE || !AVAILABLE.length) return [];
    return AVAILABLE.filter(d => d >= startStr && d <= endStr);
  }

  function sumInto(map, key, obj, fields){
    let cur = map.get(key);
    if(!cur){
      cur = Object.assign({}, obj);
      fields.forEach(f=>{ cur[f] = Number(cur[f]||0); });
      map.set(key, cur);
      return;
    }
    fields.forEach(f=>{
      cur[f] = Number(cur[f]||0) + Number(obj[f]||0);
    });
  }

  function buildAggregated(dailyJsonList){
    const kpiMap = new Map();   // key = channel||campaign||term
    const prodMap = new Map();  // key = channel||campaign||term||item_id

    for(const j of dailyJsonList){
      for(const r of (j.kpi||[])){
        const key = `${r.channel}||${r.campaign||''}||${r.term||''}`;
        sumInto(kpiMap, key, r, ['sessions','users','purchases','revenue','items_purchased']);
      }
      for(const p of (j.prod||[])){
        const key = `${p.channel}||${p.campaign||''}||${p.term||''}||${p.item_id||''}`;
        sumInto(prodMap, key, p, ['prod_items','prod_revenue']);
      }
    }

    return { kpi: Array.from(kpiMap.values()), prod: Array.from(prodMap.values()) };
  }

  async function loadDaily(dStr){
    hideNotice();
    SELECTED = null;
    const url = `data/owned/owned_${dStr}.json?t=${Date.now()}`;

    try{
      RAW = await fetchJson(url);
    }catch(e){
      RAW = null;
      KPI = [];
      renderCampaignButtons();
      aggregateKPIForSelected();
      renderTerms();
      renderProducts();
      showNotice(`데이터가 없어서 표시할 수 없어. (${dStr})`);
      return;
    }

    toggleSections();
    filterKPI();
    renderCampaignButtons();
    aggregateKPIForSelected();
    renderTerms();
    renderProducts();
  }

  async function loadRange(startStr, endStr){
    hideNotice();
    SELECTED = null;

    if(!AVAILABLE || !AVAILABLE.length){
      showNotice('available_dates.json을 읽지 못해서 기간 누적을 할 수 없어.');
      RAW = null;
      KPI = [];
      toggleSections();
      renderCampaignButtons();
      aggregateKPIForSelected();
      renderTerms();
      renderProducts();
      return;
    }

    let s = startStr, e = endStr;
    if(s > e){ const tmp=s; s=e; e=tmp; }

    s = clampToAvailable(s);
    e = clampToAvailable(e);
    startPicker.value = s;
    endPicker.value = e;

    const dates = listDatesBetween(s, e);
    if(!dates.length){
      RAW = null;
      KPI = [];
      showNotice(`선택한 기간에 데이터가 없어. (${s} ~ ${e})`);
      toggleSections();
      renderCampaignButtons();
      aggregateKPIForSelected();
      renderTerms();
      renderProducts();
      return;
    }

    const all = [];
    const chunkSize = 10;
    for(let i=0;i<dates.length;i+=chunkSize){
      const chunk = dates.slice(i, i+chunkSize);
      showNotice(`기간 데이터 로딩중… ${Math.min(i+chunk.length, dates.length)}/${dates.length}`);
      const urls = chunk.map(d => `data/owned/owned_${d}.json?t=${Date.now()}`);
      const res = await Promise.allSettled(urls.map(u => fetchJson(u)));
      res.forEach((r)=>{
        if(r.status==='fulfilled') all.push(r.value);
      });
    }
    hideNotice();

    RAW = buildAggregated(all);
    toggleSections();
    filterKPI();
    renderCampaignButtons();
    aggregateKPIForSelected();
    renderTerms();
    renderProducts();
  }

  async function loadWeekByDate(dStr){
    const cur = parseYMD(dStr);
    const s = ymd(startOfWeekMon(cur));
    const e = ymd(endOfWeekSun(cur));
    await loadRange(s, e);
  }

  async function loadAvailableDates(){
    try{
      const j = await fetchJson(`data/owned/available_dates.json?t=${Date.now()}`);
      AVAILABLE = (j.available_dates||[]).slice().sort();
    }catch(e){
      AVAILABLE = null;
    }
  }

  function setToLastNDays(n){
    const today = new Date();
    const end = ymd(addDays(today, -1));
    const start = ymd(addDays(parseYMD(end), -(n-1)));
    startPicker.value = clampToAvailable(start);
    endPicker.value = clampToAvailable(end);
  }

  function setToMTD(){
    const today = new Date();
    const end = ymd(addDays(today, -1));
    const d = parseYMD(end);
    const start = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-01`;
    startPicker.value = clampToAvailable(start);
    endPicker.value = clampToAvailable(end);
  }

  function setToWeek(){
    const end = parseYMD(endPicker.value || ymd(addDays(new Date(), -1)));
    const mon = startOfWeekMon(end);
    const sun = endOfWeekSun(end);
    startPicker.value = clampToAvailable(ymd(mon));
    endPicker.value = clampToAvailable(ymd(sun));
  }

  // View toggles
  chipDaily.addEventListener('click', ()=>{
    VIEW='DAILY';
    setViewActive();
    const d = clampToAvailable(datePicker.value || (AVAILABLE && AVAILABLE.length ? AVAILABLE[AVAILABLE.length-1] : ymd(addDays(new Date(),-1))));
    datePicker.value = d;
    loadDaily(d);
  });

  chipWeek.addEventListener('click', async ()=>{
    VIEW='WEEK';
    setViewActive();
    const d = clampToAvailable(datePicker.value || (AVAILABLE && AVAILABLE.length ? AVAILABLE[AVAILABLE.length-1] : ymd(addDays(new Date(),-1))));
    datePicker.value = d;
    await loadWeekByDate(d);
  });

  chipRange.addEventListener('click', ()=>{
    VIEW='RANGE';
    setViewActive();
    setToLastNDays(7);
    loadRange(startPicker.value, endPicker.value);
  });

  // Channel chips
  chipAll.addEventListener('click', ()=>{ CHANNEL='ALL'; setChipActive(); toggleSections(); filterKPI(); renderCampaignButtons(); aggregateKPIForSelected(); renderTerms(); renderProducts(); });
  chipEDM.addEventListener('click', ()=>{ CHANNEL='EDM'; setChipActive(); toggleSections(); filterKPI(); renderCampaignButtons(); aggregateKPIForSelected(); renderTerms(); renderProducts(); });
  chipLMS.addEventListener('click', ()=>{ CHANNEL='LMS'; setChipActive(); toggleSections(); filterKPI(); renderCampaignButtons(); aggregateKPIForSelected(); renderTerms(); renderProducts(); });
  chipKAKAO.addEventListener('click', ()=>{ CHANNEL='KAKAO'; setChipActive(); toggleSections(); filterKPI(); renderCampaignButtons(); aggregateKPIForSelected(); renderTerms(); renderProducts(); });

  q.addEventListener('input', ()=>{ filterKPI(); renderCampaignButtons(); aggregateKPIForSelected(); renderTerms(); renderProducts(); });
  topN.addEventListener('change', ()=>{ renderCampaignButtons(); aggregateKPIForSelected(); renderTerms(); renderProducts(); });

  // Range quick buttons
  btnApplyRange.addEventListener('click', ()=> loadRange(startPicker.value, endPicker.value));
  btn7d.addEventListener('click', ()=>{ setToLastNDays(7); loadRange(startPicker.value, endPicker.value); });
  btn30d.addEventListener('click', ()=>{ setToLastNDays(30); loadRange(startPicker.value, endPicker.value); });
  btnMTD.addEventListener('click', ()=>{ setToMTD(); loadRange(startPicker.value, endPicker.value); });
  btnWEEK.addEventListener('click', ()=>{ setToWeek(); loadRange(startPicker.value, endPicker.value); });

  // Nav buttons
  btnPrev.addEventListener('click', async ()=>{
    if(VIEW==='DAILY'){
      const cur = parseYMD(datePicker.value);
      const d = ymd(addDays(cur,-1));
      datePicker.value = clampToAvailable(d);
      loadDaily(datePicker.value);
      return;
    }
    if(VIEW==='WEEK'){
      const cur = parseYMD(datePicker.value);
      const d = ymd(addDays(cur,-7));
      datePicker.value = clampToAvailable(d);
      await loadWeekByDate(datePicker.value);
      return;
    }
    const s = parseYMD(startPicker.value);
    const e = parseYMD(endPicker.value);
    startPicker.value = clampToAvailable(ymd(addDays(s,-7)));
    endPicker.value = clampToAvailable(ymd(addDays(e,-7)));
    loadRange(startPicker.value, endPicker.value);
  });

  btnNext.addEventListener('click', async ()=>{
    if(VIEW==='DAILY'){
      const cur = parseYMD(datePicker.value);
      const d = ymd(addDays(cur,+1));
      datePicker.value = clampToAvailable(d);
      loadDaily(datePicker.value);
      return;
    }
    if(VIEW==='WEEK'){
      const cur = parseYMD(datePicker.value);
      const d = ymd(addDays(cur,+7));
      datePicker.value = clampToAvailable(d);
      await loadWeekByDate(datePicker.value);
      return;
    }
    const s = parseYMD(startPicker.value);
    const e = parseYMD(endPicker.value);
    startPicker.value = clampToAvailable(ymd(addDays(s,+7)));
    endPicker.value = clampToAvailable(ymd(addDays(e,+7)));
    loadRange(startPicker.value, endPicker.value);
  });

  btnToday.addEventListener('click', async ()=>{
    const d = clampToAvailable(ymd(new Date()));
    if(VIEW==='DAILY'){
      datePicker.value = d;
      loadDaily(d);
      return;
    }
    if(VIEW==='WEEK'){
      datePicker.value = d;
      await loadWeekByDate(d);
      return;
    }
    setToLastNDays(7);
    loadRange(startPicker.value, endPicker.value);
  });

  btnReload.addEventListener('click', ()=> location.reload());

  // init
  (async function init(){
    setChipActive();
    toggleSections();
    await loadAvailableDates();

    if(AVAILABLE && AVAILABLE.length){
      const minD = AVAILABLE[0];
      const maxD = AVAILABLE[AVAILABLE.length - 1];
      datePicker.min = minD; datePicker.max = maxD;
      startPicker.min = minD; startPicker.max = maxD;
      endPicker.min = minD; endPicker.max = maxD;
    }

    const d = (AVAILABLE && AVAILABLE.length) ? AVAILABLE[AVAILABLE.length-1] : ymd(addDays(new Date(), -1));
    datePicker.value = d;

    VIEW='DAILY';
    setViewActive();
    await loadDaily(d);
  })();

})();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    main()
