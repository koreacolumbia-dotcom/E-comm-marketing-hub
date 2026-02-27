#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
OWNED Campaign → Product Performance Portal (GA4 BigQuery Export)

✅ What this script does
- Pulls OWNED (EDM/LMS/KAKAO) session performance + purchased products by UTM (session-scoped)
- Generates a static portal (index.html + daily JSON bundles) that can be served via GitHub Pages

✅ Key fixes in this patched version (B안)
1) **Session campaign / manual term 양쪽 모두 매칭**
   - EDM/LMS: send_id = COALESCE(utm_campaign, utm_term)
   - KAKAO:   send_id = COALESCE(utm_term, utm_campaign)
2) **세션 수가 작게 잡히는 문제 완화**
   - session_start 한 이벤트만 보지 않고, 세션 내 전체 이벤트에서
     traffic_source / collected_traffic_source / event_params('source','medium','campaign','term','utm_*')를
     시간순으로 최초 1개를 채택해 session UTM을 복원합니다.
3) 대소문자 변형 없음 (send_id 원본 그대로 사용, channel 판별만 lower 사용)
4) index.html은 사용자가 준 최신(DAILY/RANGE 누적 지원) 버전을 그대로 포함합니다.

Outputs (when --site-dir is e.g. reports/owned_portal)
- <site-dir>/index.html
- <site-dir>/data/owned/owned_YYYY-MM-DD.json
- <site-dir>/data/owned/available_dates.json
- <site-dir>/.nojekyll

Requirements:
  pip install google-cloud-bigquery pandas pyarrow
"""

import os
import json
import base64
import argparse
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import Optional, Dict, Any, List

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
# BigQuery SQL (Session-scoped UTM reconstruction)
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

    collected_traffic_source.manual_source AS cts_source,
    collected_traffic_source.manual_medium AS cts_medium,
    collected_traffic_source.manual_campaign_name AS cts_campaign,
    collected_traffic_source.manual_term AS cts_term,

    traffic_source.source AS ts_source,
    traffic_source.medium AS ts_medium,
    traffic_source.name   AS ts_campaign,

    (SELECT value.string_value FROM UNNEST(event_params) WHERE key='source') AS ep_source,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key='medium') AS ep_medium,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key='campaign') AS ep_campaign,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key='term') AS ep_term,

    (SELECT value.string_value FROM UNNEST(event_params) WHERE key='utm_source') AS ep_utm_source,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key='utm_medium') AS ep_utm_medium,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key='utm_campaign') AS ep_utm_campaign,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key='utm_term') AS ep_utm_term,

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
    CONCAT(user_pseudo_id, '.', CAST(ga_session_id AS STRING)) AS session_key,

    COALESCE(NULLIF(cts_source,''), NULLIF(ts_source,''), NULLIF(ep_source,''), NULLIF(ep_utm_source,'')) AS utm_source,
    COALESCE(NULLIF(cts_medium,''), NULLIF(ts_medium,''), NULLIF(ep_medium,''), NULLIF(ep_utm_medium,'')) AS utm_medium,
    COALESCE(NULLIF(cts_campaign,''), NULLIF(ts_campaign,''), NULLIF(ep_campaign,''), NULLIF(ep_utm_campaign,'')) AS utm_campaign,
    COALESCE(NULLIF(cts_term,''), NULLIF(ep_term,''), NULLIF(ep_utm_term,'')) AS utm_term,

    event_name,
    IFNULL(purchase_revenue, 0) AS purchase_revenue,
    items
  FROM base
  WHERE ga_session_id IS NOT NULL
),

session_dim AS (
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
    ga_session_id,
    session_key,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_term,

    CASE
      WHEN LOWER(IFNULL(utm_medium,'')) = 'lms' THEN 'LMS'
      WHEN LOWER(IFNULL(utm_medium,'')) IN ('edm','email') THEN 'EDM'
      WHEN LOWER(IFNULL(utm_source,'')) LIKE '%kakao%' OR LOWER(IFNULL(utm_medium,'')) LIKE '%kakao%' THEN 'KAKAO'
      ELSE 'OTHER'
    END AS channel,

    CASE
      WHEN LOWER(IFNULL(utm_medium,'')) IN ('lms','edm','email')
        THEN COALESCE(NULLIF(utm_campaign,''), NULLIF(utm_term,''))
      WHEN LOWER(IFNULL(utm_source,'')) LIKE '%kakao%' OR LOWER(IFNULL(utm_medium,'')) LIKE '%kakao%'
        THEN COALESCE(NULLIF(utm_term,''), NULLIF(utm_campaign,''))
      ELSE NULL
    END AS send_id
  FROM session_dim
  WHERE (LOWER(IFNULL(utm_medium,'')) IN ('lms','edm','email')
     OR LOWER(IFNULL(utm_source,'')) LIKE '%kakao%'
     OR LOWER(IFNULL(utm_medium,'')) LIKE '%kakao%')
),

session_kpi AS (
  SELECT
    date,
    channel,
    send_id,
    COUNT(DISTINCT session_key) AS sessions,
    COUNT(DISTINCT user_pseudo_id) AS users
  FROM sessions_owned
  WHERE send_id IS NOT NULL
  GROUP BY 1,2,3
),

purchase_events AS (
  SELECT
    b.date,
    b.session_key,
    IFNULL(b.purchase_revenue, 0) AS purchase_revenue
  FROM base2 b
  WHERE b.event_name = 'purchase'
),

purchase_kpi AS (
  SELECT
    p.date,
    s.channel,
    s.send_id,
    COUNT(1) AS purchases,
    SUM(p.purchase_revenue) AS revenue
  FROM purchase_events p
  JOIN sessions_owned s
    ON s.session_key = p.session_key
  WHERE s.send_id IS NOT NULL
  GROUP BY 1,2,3
),

purchase_items AS (
  SELECT
    b.date,
    b.session_key,
    i.item_id AS item_id,
    i.item_name AS item_name,
    IFNULL(i.quantity, 0) AS qty,
    COALESCE(i.item_revenue, i.price * i.quantity, 0) AS item_rev
  FROM base2 b,
  UNNEST(b.items) i
  WHERE b.event_name = 'purchase'
),

items_kpi AS (
  SELECT
    pi.date,
    s.channel,
    s.send_id,
    SUM(pi.qty) AS items_purchased
  FROM purchase_items pi
  JOIN sessions_owned s
    ON s.session_key = pi.session_key
  WHERE s.send_id IS NOT NULL
  GROUP BY 1,2,3
),

kpi AS (
  SELECT
    sk.date,
    sk.channel,
    sk.send_id,
    sk.sessions,
    sk.users,
    IFNULL(pk.purchases,0) AS purchases,
    IFNULL(pk.revenue,0) AS revenue,
    IFNULL(ik.items_purchased,0) AS items_purchased
  FROM session_kpi sk
  LEFT JOIN purchase_kpi pk
    ON pk.date=sk.date AND pk.channel=sk.channel AND pk.send_id=sk.send_id
  LEFT JOIN items_kpi ik
    ON ik.date=sk.date AND ik.channel=sk.channel AND ik.send_id=sk.send_id
),

prod AS (
  SELECT
    pi.date,
    s.channel,
    s.send_id,
    pi.item_id,
    ANY_VALUE(pi.item_name) AS item_name,
    SUM(pi.qty) AS prod_items,
    SUM(pi.item_rev) AS prod_revenue
  FROM purchase_items pi
  JOIN sessions_owned s
    ON s.session_key = pi.session_key
  WHERE s.send_id IS NOT NULL
  GROUP BY 1,2,3,4
)

SELECT
  'kpi' AS row_type,
  CAST(k.date AS STRING) AS date,
  k.channel,
  k.send_id,
  NULL AS item_id,
  NULL AS item_name,
  NULL AS prod_items,
  NULL AS prod_revenue,
  k.sessions,
  k.users,
  k.purchases,
  k.revenue AS kpi_revenue,
  k.items_purchased
FROM kpi k

UNION ALL

SELECT
  'prod' AS row_type,
  CAST(p.date AS STRING) AS date,
  p.channel,
  p.send_id,
  p.item_id,
  p.item_name,
  p.prod_items AS prod_items,
  p.prod_revenue AS prod_revenue,
  NULL AS sessions,
  NULL AS users,
  NULL AS purchases,
  NULL AS kpi_revenue,
  NULL AS items_purchased
FROM prod p
;
"""


# ----------------------------
# HTML (user provided)
# ----------------------------
def build_index_html() -> str:
    return r'''<!doctype html>
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
        <div class="muted font-semibold mt-1">EDM/LMS/KAKAO 캠페인(발송)별 성과 + 구매 상품까지 확인</div>
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
        <button id="chipRange" class="chip" type="button">RANGE</button>

        <div class="ml-2 small-label">Date</div>
        <div class="w-[160px]"><input id="datePicker" type="date" /></div>

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
        <div class="w-[260px]"><input id="q" type="text" placeholder="캠페인명(send_id) 검색…" /></div>

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

      <!-- Campaign buttons -->
      <div class="mb-4">
        <div class="small-label mb-2">Campaigns (grouped)</div>

        <div class="mb-3">
          <div class="sec-title mb-2">EDM · by date (####)</div>
          <div id="wrapEDMDate" class="flex flex-wrap gap-2"></div>
        </div>

        <div class="mb-3">
          <div class="sec-title mb-2">LMS · by date (####)</div>
          <div id="wrapLMSDate" class="flex flex-wrap gap-2"></div>
        </div>

        <div class="mb-3">
          <div class="sec-title mb-2">KAKAO · by date (####)</div>
          <div id="wrapKakaoDate" class="flex flex-wrap gap-2"></div>
        </div>

        <div>
          <div class="sec-title mb-2">Other campaigns</div>
          <div id="wrapOther" class="flex flex-wrap gap-2"></div>
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
          <span class="mono" id="selChannel">-</span> · <span class="mono" id="selSendId">-</span>
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
            <tr><td colspan="4" class="muted font-semibold">캠페인을 선택하면 상품 리스트가 표시돼.</td></tr>
          </tbody>
        </table>
      </div>

      <div class="muted font-semibold text-xs mt-3">
        데이터: GA4 BigQuery Export · JSON: owned_YYYY-MM-DD.json 누적 · 기간 조회는 클라이언트 합산
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
  const chipRange = document.getElementById('chipRange');
  const rangeBox = document.getElementById('rangeBox');

  const datePicker = document.getElementById('datePicker');
  const startPicker = document.getElementById('startPicker');
  const endPicker = document.getElementById('endPicker');
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
  const q = document.getElementById('q');
  const topN = document.getElementById('topN');

  const wrapEDMDate = document.getElementById('wrapEDMDate');
  const wrapLMSDate = document.getElementById('wrapLMSDate');
  const wrapKakaoDate = document.getElementById('wrapKakaoDate');
  const wrapOther = document.getElementById('wrapOther');

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
  const selSendId = document.getElementById('selSendId');
  const selPeriod = document.getElementById('selPeriod');

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

  function clampToAvailable(dStr){
    if(!AVAILABLE || !AVAILABLE.length) return dStr;
    if(AVAILABLE.includes(dStr)) return dStr;
    for(let i=AVAILABLE.length-1;i>=0;i--){
      if(AVAILABLE[i] <= dStr) return AVAILABLE[i];
    }
    return AVAILABLE[0];
  }

  // send_id에서 날짜코드(####) 추출
  function dateCodeOf(sendId){
    const s = String(sendId||'');
    const m = s.match(/(?:^|_)(\d{4})(?:_|$)/);
    return m ? m[1] : null;
  }

  let VIEW = 'DAILY'; // DAILY | RANGE
  let CHANNEL = 'ALL';

  let RAW = null;
  let KPI = [];
  let SELECTED = null;   // {type:'single'|'group', channel, send_id? , date_code?}
  let AVAILABLE = null;

  function setViewActive(){
    [chipDaily, chipRange].forEach(el=>el.classList.remove('active'));
    if(VIEW==='DAILY'){
      chipDaily.classList.add('active');
      rangeBox.classList.add('hidden'); rangeBox.classList.remove('flex');
      datePicker.parentElement.classList.remove('hidden');
    }
    if(VIEW==='RANGE'){
      chipRange.classList.add('active');
      rangeBox.classList.remove('hidden'); rangeBox.classList.add('flex');
      datePicker.parentElement.classList.add('hidden');
    }
  }

  function setChipActive(){
    [chipAll, chipEDM, chipLMS, chipKAKAO].forEach(el=>el.classList.remove('active'));
    if(CHANNEL==='ALL') chipAll.classList.add('active');
    if(CHANNEL==='EDM') chipEDM.classList.add('active');
    if(CHANNEL==='LMS') chipLMS.classList.add('active');
    if(CHANNEL==='KAKAO') chipKAKAO.classList.add('active');
  }

  function periodLabel(){
    if(VIEW==='DAILY'){
      const d = datePicker.value || '-';
      return ` · ${d}`;
    }
    const s = startPicker.value || '-';
    const e = endPicker.value || '-';
    return ` · ${s} ~ ${e}`;
  }

  function filterKPI(){
    if(!RAW){ KPI=[]; return; }
    const qq = (q.value||'').trim().toLowerCase();
    KPI = (RAW.kpi||[]).filter(r=>{
      if(CHANNEL!=='ALL' && r.channel!==CHANNEL) return false;
      if(qq){
        const sid = String(r.send_id||'').toLowerCase();
        const dc = dateCodeOf(r.send_id);
        if(sid.indexOf(qq)===-1 && String(dc||'').indexOf(qq)===-1) return false;
      }
      return true;
    });
  }

  function isActiveSingle(ch, send){
    return SELECTED && SELECTED.type==='single' && SELECTED.channel===ch && SELECTED.send_id===send;
  }
  function isActiveGroup(ch, dc){
    return SELECTED && SELECTED.type==='group' && SELECTED.channel===ch && SELECTED.date_code===dc;
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
    // clear
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

    // group by channel + datecode
    const byDate = {EDM:new Map(), LMS:new Map(), KAKAO:new Map()};
    const others = [];

    for(const r of KPI){
      const dc = dateCodeOf(r.send_id);
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
        const label = `${x.channel}_${x.dc}`;
        const title = `${x.channel} · ${x.dc} · sessions:${x.agg.sessions} · campaigns:${x.rows.length}`;
        const active = isActiveGroup(x.channel, x.dc);
        return makeChip(label, title, active, {type:'group', channel:x.channel, dc:x.dc});
      }).join('');

      Array.from(wrapEl.querySelectorAll('button.chip')).forEach(btn=>{
        btn.addEventListener('click', ()=>{
          const ch = btn.getAttribute('data-channel');
          const dc = btn.getAttribute('data-dc');
          selectGroup(ch, dc);
        });
      });
    }

    renderDateGroup('EDM', wrapEDMDate);
    renderDateGroup('LMS', wrapLMSDate);
    renderDateGroup('KAKAO', wrapKakaoDate);

    // others: show individual send_id (sorted by sessions) limited
    const oRows = others.slice().sort((a,b)=> (b.sessions||0)-(a.sessions||0)).slice(0, limit);
    if(!oRows.length){
      wrapOther.innerHTML = `<div class="muted font-semibold text-sm">-</div>`;
    }else{
      wrapOther.innerHTML = oRows.map(r=>{
        const label = `${r.channel}_${r.send_id}`;
        const title = `${r.channel} · sessions:${r.sessions}`;
        const active = isActiveSingle(r.channel, r.send_id);
        return makeChip(label, title, active, {type:'single', channel:r.channel, send:encodeURIComponent(r.send_id)});
      }).join('');

      Array.from(wrapOther.querySelectorAll('button.chip')).forEach(btn=>{
        btn.addEventListener('click', ()=>{
          const ch = btn.getAttribute('data-channel');
          const send = decodeURIComponent(btn.getAttribute('data-send')||'');
          selectSingle(ch, send);
        });
      });
    }
  }

  function aggregateKPIForSelected(){
    selPeriod.textContent = periodLabel();

    if(!SELECTED || !RAW){
      kSessions.textContent='-'; kUsers.textContent='-'; kPurchases.textContent='-'; kRevenue.textContent='-'; kItems.textContent='-';
      kSessionsSub.textContent='-'; kUsersSub.textContent='-'; kCvrSub.textContent='-'; kAovSub.textContent='-'; kItemsSub.textContent='-';
      selChannel.textContent='-'; selSendId.textContent='-';
      return;
    }

    let rows = [];
    if(SELECTED.type==='single'){
      rows = (RAW.kpi||[]).filter(x=> x.channel===SELECTED.channel && x.send_id===SELECTED.send_id);
    }else{
      rows = (RAW.kpi||[]).filter(x=> x.channel===SELECTED.channel && dateCodeOf(x.send_id)===SELECTED.date_code);
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
    kUsersSub.textContent = `-`;
    kCvrSub.textContent = `CVR: ${cvr.toFixed(2)}%`;
    kAovSub.textContent = `AOV: ${fmtMoney(aov)}`;
    kItemsSub.textContent = `Items/Order: ${ips.toFixed(2)}`;

    selChannel.textContent = SELECTED.channel;
    if(SELECTED.type==='single'){
      selSendId.textContent = SELECTED.send_id;
    }else{
      selSendId.textContent = `${SELECTED.date_code} (group)`;
    }
  }

  function renderProducts(){
    if(!SELECTED || !RAW){
      tb.innerHTML = `<tr><td colspan="4" class="muted font-semibold">캠페인을 선택하면 상품 리스트가 표시돼.</td></tr>`;
      return;
    }

    let rows = [];
    if(SELECTED.type==='single'){
      rows = (RAW.prod||[]).filter(r=> r.channel===SELECTED.channel && r.send_id===SELECTED.send_id);
    }else{
      rows = (RAW.prod||[]).filter(r=> r.channel===SELECTED.channel && dateCodeOf(r.send_id)===SELECTED.date_code);
    }

    if(!rows.length){
      tb.innerHTML = `<tr><td colspan="4" class="muted font-semibold">구매 상품 데이터가 없어. (purchase가 없거나 items가 비어있을 수 있어)</td></tr>`;
      return;
    }

    // aggregate by item_id
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

  function selectSingle(channel, send_id){
    SELECTED = {type:'single', channel, send_id};
    renderCampaignButtons();
    aggregateKPIForSelected();
    renderProducts();
  }
  function selectGroup(channel, date_code){
    SELECTED = {type:'group', channel, date_code};
    renderCampaignButtons();
    aggregateKPIForSelected();
    renderProducts();
  }

  async function fetchJson(url){
    const res = await fetch(url, {cache:'no-store'});
    if(!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  }

  function listDatesBetween(startStr, endStr){
    if(!AVAILABLE || !AVAILABLE.length) return [];
    const s = startStr;
    const e = endStr;
    return AVAILABLE.filter(d => d >= s && d <= e);
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
    const kpiMap = new Map();
    const prodMap = new Map();

    for(const j of dailyJsonList){
      const kpi = (j.kpi||[]);
      for(const r of kpi){
        const key = `${r.channel}||${r.send_id}`;
        sumInto(kpiMap, key, r, ['sessions','users','purchases','revenue','items_purchased']);
      }
      const prod = (j.prod||[]);
      for(const p of prod){
        const key = `${p.channel}||${p.send_id}||${p.item_id}`;
        sumInto(prodMap, key, p, ['prod_items','prod_revenue']);
      }
    }

    return {
      kpi: Array.from(kpiMap.values()),
      prod: Array.from(prodMap.values())
    };
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
      wrapEDMDate.innerHTML = '';
      wrapLMSDate.innerHTML = '';
      wrapKakaoDate.innerHTML = '';
      wrapOther.innerHTML = '';
      tb.innerHTML = `<tr><td colspan="4" class="muted font-semibold">해당 날짜 데이터 파일이 없어: owned_${dStr}.json</td></tr>`;
      showNotice(`데이터가 없어서 표시할 수 없어. (${dStr})`);
      aggregateKPIForSelected();
      return;
    }

    filterKPI();
    renderCampaignButtons();
    aggregateKPIForSelected();
    renderProducts();
  }

  async function loadRange(startStr, endStr){
    hideNotice();
    SELECTED = null;

    if(!AVAILABLE || !AVAILABLE.length){
      showNotice('available_dates.json을 읽지 못해서 기간 누적을 할 수 없어.');
      RAW = null;
      KPI = [];
      renderCampaignButtons();
      aggregateKPIForSelected();
      renderProducts();
      return;
    }

    let s = startStr;
    let e = endStr;
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
      renderCampaignButtons();
      aggregateKPIForSelected();
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

    filterKPI();
    renderCampaignButtons();
    aggregateKPIForSelected();
    renderProducts();
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
    // 기준: endPicker 기준으로 그 주 월~일
    const end = parseYMD(endPicker.value || ymd(addDays(new Date(), -1)));
    const day = end.getDay(); // 0=Sun..6=Sat
    const diffToMon = (day===0) ? -6 : (1 - day); // to Monday
    const mon = addDays(end, diffToMon);
    const sun = addDays(mon, 6);
    startPicker.value = clampToAvailable(ymd(mon));
    endPicker.value = clampToAvailable(ymd(sun));
  }

  // events: view
  chipDaily.addEventListener('click', ()=>{
    VIEW='DAILY';
    setViewActive();
    const d = clampToAvailable(datePicker.value || (AVAILABLE && AVAILABLE.length ? AVAILABLE[AVAILABLE.length-1] : ymd(addDays(new Date(),-1))));
    datePicker.value = d;
    loadDaily(d);
  });

  chipRange.addEventListener('click', ()=>{
    VIEW='RANGE';
    setViewActive();
    setToLastNDays(7);
    loadRange(startPicker.value, endPicker.value);
  });

  // channel chips
  chipAll.addEventListener('click', ()=>{ CHANNEL='ALL'; setChipActive(); filterKPI(); renderCampaignButtons(); });
  chipEDM.addEventListener('click', ()=>{ CHANNEL='EDM'; setChipActive(); filterKPI(); renderCampaignButtons(); });
  chipLMS.addEventListener('click', ()=>{ CHANNEL='LMS'; setChipActive(); filterKPI(); renderCampaignButtons(); });
  chipKAKAO.addEventListener('click', ()=>{ CHANNEL='KAKAO'; setChipActive(); filterKPI(); renderCampaignButtons(); });

  q.addEventListener('input', ()=>{ filterKPI(); renderCampaignButtons(); });
  topN.addEventListener('change', ()=>{ renderCampaignButtons(); });

  // range quick buttons
  btnApplyRange.addEventListener('click', ()=> loadRange(startPicker.value, endPicker.value));
  btn7d.addEventListener('click', ()=>{ setToLastNDays(7); loadRange(startPicker.value, endPicker.value); });
  btn30d.addEventListener('click', ()=>{ setToLastNDays(30); loadRange(startPicker.value, endPicker.value); });
  btnMTD.addEventListener('click', ()=>{ setToMTD(); loadRange(startPicker.value, endPicker.value); });
  btnWEEK.addEventListener('click', ()=>{ setToWeek(); loadRange(startPicker.value, endPicker.value); });

  // nav
  btnPrev.addEventListener('click', ()=>{
    if(VIEW==='DAILY'){
      const cur = parseYMD(datePicker.value);
      const d = ymd(addDays(cur,-1));
      datePicker.value = clampToAvailable(d);
      loadDaily(datePicker.value);
      return;
    }
    const s = parseYMD(startPicker.value);
    const e = parseYMD(endPicker.value);
    startPicker.value = clampToAvailable(ymd(addDays(s,-7)));
    endPicker.value = clampToAvailable(ymd(addDays(e,-7)));
    loadRange(startPicker.value, endPicker.value);
  });

  btnNext.addEventListener('click', ()=>{
    if(VIEW==='DAILY'){
      const cur = parseYMD(datePicker.value);
      const d = ymd(addDays(cur,+1));
      datePicker.value = clampToAvailable(d);
      loadDaily(datePicker.value);
      return;
    }
    const s = parseYMD(startPicker.value);
    const e = parseYMD(endPicker.value);
    startPicker.value = clampToAvailable(ymd(addDays(s,+7)));
    endPicker.value = clampToAvailable(ymd(addDays(e,+7)));
    loadRange(startPicker.value, endPicker.value);
  });

  btnToday.addEventListener('click', ()=>{
    if(VIEW==='DAILY'){
      const d = clampToAvailable(ymd(new Date()));
      datePicker.value = d;
      loadDaily(d);
      return;
    }
    setToLastNDays(7);
    loadRange(startPicker.value, endPicker.value);
  });

  btnReload.addEventListener('click', ()=> location.reload());

  // init
  (async function init(){
    setChipActive();
    await loadAvailableDates();

    let d;
    if(AVAILABLE && AVAILABLE.length){
      d = AVAILABLE[AVAILABLE.length-1];
    }else{
      d = ymd(addDays(new Date(), -1));
    }
    datePicker.value = d;

    VIEW='DAILY';
    setViewActive();
    await loadDaily(d);
  })();

})();
</script>
</body>
</html>
'''


def build_hub_html_placeholder() -> str:
    return """<!doctype html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Hub</title>
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="p-8">
  <div class="max-w-3xl mx-auto">
    <div class="text-3xl font-black mb-2">Hub</div>
    <div class="text-slate-600 font-semibold mb-6">원래 사용하던 Hub가 있으면 hub.html로 교체해서 쓰면 돼.</div>
    <a class="px-4 py-3 rounded-xl bg-slate-900 text-white font-black inline-block" href="./index.html">OWNED Campaign Explorer로 이동</a>
  </div>
</body>
</html>
"""


# ----------------------------
# JSON writing
# ----------------------------
def write_daily_json(out_dir: Path, d: str, kpi_rows: List[Dict[str, Any]], prod_rows: List[Dict[str, Any]]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = {"date": d, "kpi": kpi_rows, "prod": prod_rows}
    (out_dir / f"owned_{d}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_available_dates(out_dir: Path, dates: List[str]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = {"available_dates": sorted(dates)}
    (out_dir / "available_dates.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


# ----------------------------
# Main
# ----------------------------
def _safe_to_dataframe(job: bigquery.job.QueryJob) -> pd.DataFrame:
    try:
        return job.result().to_dataframe(create_bqstorage_client=True)
    except Exception:
        return job.result().to_dataframe()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default=os.getenv("BQ_PROJECT", "columbia-ga4"))
    ap.add_argument("--dataset", default=os.getenv("BQ_DATASET", "analytics_358593394"))
    ap.add_argument("--start", default="2025-01-01")
    ap.add_argument("--end", default=None, help="End date (YYYY-MM-DD). Default: yesterday(KST).")
    ap.add_argument("--backfill", action="store_true", help="Backfill from --backfill-start to end (default yesterday).")
    ap.add_argument("--backfill-start", default="2025-01-01", help="Backfill start date (YYYY-MM-DD).")
        ap.add_argument("--write-empty-days", dest="write_empty_days", action="store_true", default=True, help="Write empty JSON for days with no data (recommended).")
    ap.add_argument("--recent-days", type=int, default=None, help="If set, fetch only recent N days ending today(KST).")
    ap.add_argument("--site-dir", default="site")
    args = ap.parse_args()

    maybe_write_sa_from_b64()

    site_dir = Path(args.site_dir)
    data_dir = site_dir / "data" / "owned"

    end_d = parse_date(args.end) if args.end else (kst_today() - timedelta(days=1))
    # Range priority: --recent-days (incremental) > --backfill > --start
    if args.recent_days and args.recent_days > 0:
        start_d = end_d - timedelta(days=args.recent_days - 1)
    elif args.backfill:
        start_d = parse_date(args.backfill_start)
    else:
        start_d = parse_date(args.start)

    client = bigquery.Client(project=args.project)
    sql = build_sql(args.project, args.dataset, suffix(start_d), suffix(end_d))
    job = client.query(sql)
    df = _safe_to_dataframe(job)

    # Site skeleton
    site_dir.mkdir(parents=True, exist_ok=True)
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

    available_set: set[str] = set()

    # Merge with existing available_dates.json (so incremental runs keep history)
    existing_path = data_dir / "available_dates.json"
    if existing_path.exists():
        try:
            existing = json.loads(existing_path.read_text(encoding="utf-8"))
            for dd in (existing.get("available_dates") or []):
                if isinstance(dd, str) and dd:
                    available_set.add(dd)
        except Exception:
            pass

    # Build list of days we SHOULD write this run (including empty days if enabled)
    days_to_write: List[date] = []
    cur = start_d
    while cur <= end_d:
        days_to_write.append(cur)
        cur += timedelta(days=1)

    for day in days_to_write:
        d = ymd(day)

        k_rows = kpi_df[kpi_df["date"] == d][
            ["date", "channel", "send_id", "sessions", "users", "purchases", "kpi_revenue", "items_purchased"]
        ].to_dict(orient="records")

        p_rows = prod_df[prod_df["date"] == d][
            ["date", "channel", "send_id", "item_id", "item_name", "prod_items", "prod_revenue"]
        ].to_dict(orient="records")

        if (not k_rows) and (not p_rows) and (not args.write_empty_days):
            # Skip writing empty days if user asked (but still keep previous data files)
            continue

        kpi_rows = []
        for r in k_rows:
            kpi_rows.append({
                "date": r.get("date"),
                "channel": r.get("channel"),
                "send_id": r.get("send_id"),
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
                "send_id": r.get("send_id"),
                "item_id": r.get("item_id"),
                "item_name": r.get("item_name"),
                "prod_items": float(r.get("prod_items", 0.0)),
                "prod_revenue": float(r.get("prod_revenue", 0.0)),
            })

        write_daily_json(data_dir, d, kpi_rows, prod_rows)
        available_set.add(d)

    write_available_dates(data_dir, sorted(available_set))

    print(f"[OK] Wrote site to: {site_dir.resolve()}")
    print(f"[OK] Data files: {data_dir.resolve()} (days={len(available)})")


if __name__ == "__main__":
    main()
