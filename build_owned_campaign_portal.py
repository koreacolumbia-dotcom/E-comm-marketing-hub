#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
OWNED Campaign → Product Performance Portal (GA4 BigQuery Export)

What it does
- Pulls performance + purchased products by send_id (campaign/term/source) from GA4 BigQuery export
- Writes per-day JSON files:
    <site-dir>/data/owned/owned_YYYY-MM-DD.json
    <site-dir>/data/owned/available_dates.json
- (Optional) Writes <site-dir>/index.html (static portal)

Designed for:
- One-time backfill (e.g., 2025-01-01 ~ yesterday)
- Daily incremental (yesterday only), via GitHub Actions

Auth
- Uses GOOGLE_APPLICATION_CREDENTIALS (recommended)
- Or set GOOGLE_SA_JSON_B64 and this script will write /tmp/ga_sa.json

Requirements
  pip install google-cloud-bigquery pandas pyarrow db-dtypes

Usage examples
  # Backfill
  python build_owned_campaign_portal.py --project columbia-ga4 --dataset analytics_358593394 \
    --start 2025-01-01 --end 2026-02-25 --site-dir reports/owned_portal --overwrite --write-index

  # Daily incremental (yesterday only)
  python build_owned_campaign_portal.py --project columbia-ga4 --dataset analytics_358593394 \
    --start 2026-02-25 --end 2026-02-25 --site-dir reports/owned_portal
"""

import os
import re
import json
import base64
import argparse
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import pandas as pd
from google.cloud import bigquery

KST = timezone(timedelta(hours=9))


# ----------------------------
# Time helpers
# ----------------------------
def kst_today() -> date:
    return datetime.now(tz=KST).date()


def kst_yesterday() -> date:
    return kst_today() - timedelta(days=1)


def parse_ymd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def ymd(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def ymd_suffix(d: date) -> str:
    return d.strftime("%Y%m%d")


def daterange(start: date, end: date) -> List[date]:
    out: List[date] = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur += timedelta(days=1)
    return out


# ----------------------------
# Auth helper
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
def build_sql(table_fq: str, start_suffix: str, end_suffix: str) -> str:
    """
    Verified mapping (based on your BQ test runs):
      send_id = COALESCE(event_params['campaign'], event_params['term'], traffic_source.source)

    Revenue / items_purchased:
      Use UNNEST(items) + SUM(item_revenue), SUM(quantity)
      (matches your validated CSV output)

    Notes:
    - We exclude "(direct)/(none)/(not set)"-style send_id so it doesn't pollute the portal.
    """
    return f"""
DECLARE start_suffix STRING DEFAULT '{start_suffix}';
DECLARE end_suffix   STRING DEFAULT '{end_suffix}';

WITH base AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS date,
    event_name,
    user_pseudo_id,
    traffic_source.source AS ts_source,
    traffic_source.medium AS ts_medium,

    (SELECT value.string_value FROM UNNEST(event_params) WHERE key='campaign') AS p_campaign,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key='term')     AS p_term,

    ecommerce.transaction_id AS transaction_id,
    items
  FROM {table_fq}
  WHERE _TABLE_SUFFIX BETWEEN start_suffix AND end_suffix
),

norm AS (
  SELECT
    date,
    event_name,
    user_pseudo_id,

    COALESCE(NULLIF(p_campaign,''), NULLIF(p_term,''), NULLIF(ts_source,'')) AS send_id,
    ts_source,
    ts_medium,
    transaction_id,
    items
  FROM base
),

owned AS (
  SELECT
    date,
    event_name,
    user_pseudo_id,
    send_id,

    CASE
      WHEN UPPER(send_id) LIKE 'KAKAO%' OR UPPER(ts_source) LIKE 'KAKAO%' OR UPPER(ts_medium) LIKE 'KAKAO%' THEN 'KAKAO'
      WHEN UPPER(send_id) LIKE 'LMS%'   OR UPPER(ts_source) LIKE 'LMS%'   OR UPPER(ts_medium) LIKE 'LMS%'   THEN 'LMS'
      ELSE 'EDM'
    END AS channel,

    transaction_id,
    items
  FROM norm
  WHERE
    send_id IS NOT NULL
    AND NOT REGEXP_CONTAINS(send_id, r'^\\(.*\\)$')  -- (direct)/(none)/(not set) 방지
),

kpi_core AS (
  SELECT
    date,
    channel,
    send_id,
    COUNTIF(event_name='session_start') AS sessions,
    COUNT(DISTINCT IF(event_name='session_start', user_pseudo_id, NULL)) AS users,
    COUNT(DISTINCT IF(event_name='purchase', transaction_id, NULL)) AS purchases
  FROM owned
  GROUP BY date, channel, send_id
),

rev AS (
  SELECT
    o.date,
    o.channel,
    o.send_id,
    SUM(IFNULL(it.quantity, 0)) AS items_purchased,
    SUM(IFNULL(it.item_revenue, 0)) AS revenue
  FROM owned o,
  UNNEST(o.items) AS it
  WHERE o.event_name = 'purchase'
  GROUP BY o.date, o.channel, o.send_id
),

kpi AS (
  SELECT
    k.date,
    k.channel,
    k.send_id,
    k.sessions,
    k.users,
    k.purchases,
    IFNULL(r.revenue, 0) AS revenue,
    IFNULL(r.items_purchased, 0) AS items_purchased
  FROM kpi_core k
  LEFT JOIN rev r
    ON k.date = r.date AND k.channel = r.channel AND k.send_id = r.send_id
),

prod AS (
  SELECT
    o.date,
    o.channel,
    o.send_id,
    it.item_id AS item_id,
    it.item_name AS item_name,
    SUM(IFNULL(it.quantity, 0)) AS prod_items,
    SUM(IFNULL(it.item_revenue, 0)) AS prod_revenue
  FROM owned o,
  UNNEST(o.items) AS it
  WHERE o.event_name='purchase'
  GROUP BY o.date, o.channel, o.send_id, item_id, item_name
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
  p.prod_items,
  p.prod_revenue,
  NULL AS sessions,
  NULL AS users,
  NULL AS purchases,
  NULL AS kpi_revenue,
  NULL AS items_purchased
FROM prod p
;
"""


# ----------------------------
# JSON writers
# ----------------------------
def scan_existing_owned_dates(out_dir: Path) -> List[str]:
    if not out_dir.exists():
        return []
    dates: List[str] = []
    for p in out_dir.glob("owned_????-??-??.json"):
        m = re.match(r"owned_(\\d{4}-\\d{2}-\\d{2})\\.json$", p.name)
        if m:
            dates.append(m.group(1))
    return sorted(set(dates))


def write_available_dates(out_dir: Path, dates: List[str]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    p = out_dir / "available_dates.json"
    p.write_text(json.dumps({"available_dates": dates}, ensure_ascii=False, indent=2), encoding="utf-8")


def write_daily_json(out_dir: Path, day: str, kpi_rows: List[Dict[str, Any]], prod_rows: List[Dict[str, Any]], overwrite: bool) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    p = out_dir / f"owned_{day}.json"
    if p.exists() and not overwrite:
        return
    payload = {
        "date": day,
        "kpi": kpi_rows,
        "prod": prod_rows,
    }
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


# ----------------------------
# HTML (optional)
# ----------------------------
def build_index_html_range() -> str:
    """
    Range-based portal UI:
    - Reads data/owned/available_dates.json
    - Lets user select date range (From/To) and aggregates daily JSONs in-browser
    """
    # This is the exact HTML you were using in the earlier range patch (with the "no-data clears UI" fix included).
    return r"""<!doctype html>
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
        <div class="small-label">Date</div>

        <div class="w-[160px]"><input id="startPicker" type="date" /></div>
        <div class="text-slate-400 font-black">~</div>
        <div class="w-[160px]"><input id="endPicker" type="date" /></div>

        <button id="btnApply" class="btn btn-primary" type="button">적용</button>
        <button id="btnPrev" class="btn" type="button">◀</button>
        <button id="btnNext" class="btn" type="button">▶</button>
        <button id="btn7d" class="btn" type="button">최근 7일</button>
        <button id="btn30d" class="btn" type="button">최근 30일</button>
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
        <div class="small-label mb-2">Campaigns (send_id)</div>
        <div id="campaignWrap" class="flex flex-wrap gap-2"></div>
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
          <span class="text-slate-400 font-black">·</span>
          <span class="mono" id="selRange">-</span>
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
        데이터: GA4 BigQuery Export (daily JSON) · send_id: campaign/term/source COALESCE · purchase 기준 상품 집계
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

  const startPicker = document.getElementById('startPicker');
  const endPicker   = document.getElementById('endPicker');
  const btnApply = document.getElementById('btnApply');
  const btnPrev = document.getElementById('btnPrev');
  const btnNext = document.getElementById('btnNext');
  const btn7d = document.getElementById('btn7d');
  const btn30d = document.getElementById('btn30d');
  const btnToday = document.getElementById('btnToday');
  const btnReload = document.getElementById('btnReload');

  const chipAll = document.getElementById('chipAll');
  const chipEDM = document.getElementById('chipEDM');
  const chipLMS = document.getElementById('chipLMS');
  const chipKAKAO = document.getElementById('chipKAKAO');
  const q = document.getElementById('q');
  const topN = document.getElementById('topN');

  const campaignWrap = document.getElementById('campaignWrap');
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
  const selRange = document.getElementById('selRange');

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

  let CHANNEL = 'ALL';

  let RAW = null;      // aggregated range result
  let KPI = [];
  let SELECTED = null; // {channel, send_id}
  let AVAILABLE = null; // [YYYY-MM-DD...]

  function clearAllUI(rangeText){
    RAW = null;
    KPI = [];
    SELECTED = null;

    kSessions.textContent='-'; kUsers.textContent='-'; kPurchases.textContent='-'; kRevenue.textContent='-'; kItems.textContent='-';
    kSessionsSub.textContent = rangeText || '-';
    kUsersSub.textContent='-'; kCvrSub.textContent='-'; kAovSub.textContent='-'; kItemsSub.textContent='-';

    selChannel.textContent='-';
    selSendId.textContent='-';
    selRange.textContent = rangeText || '-';

    campaignWrap.innerHTML = '';
    tb.innerHTML = `<tr><td colspan="4" class="muted font-semibold">해당 기간 데이터가 없어.</td></tr>`;
  }

  function setChipActive(){
    [chipAll, chipEDM, chipLMS, chipKAKAO].forEach(el=>el.classList.remove('active'));
    if(CHANNEL==='ALL') chipAll.classList.add('active');
    if(CHANNEL==='EDM') chipEDM.classList.add('active');
    if(CHANNEL==='LMS') chipLMS.classList.add('active');
    if(CHANNEL==='KAKAO') chipKAKAO.classList.add('active');
  }

  function filterKPI(){
    if(!RAW){ KPI=[]; return; }
    const qq = (q.value||'').trim().toLowerCase();
    KPI = RAW.kpi.filter(r=>{
      if(CHANNEL!=='ALL' && r.channel!==CHANNEL) return false;
      if(qq && String(r.send_id||'').toLowerCase().indexOf(qq)===-1) return false;
      return true;
    });
  }

  function renderCampaignButtons(){
    campaignWrap.innerHTML = '';
    if(!KPI.length){
      campaignWrap.innerHTML = `<div class="muted font-semibold text-sm">캠페인이 없거나(또는 필터가 너무 좁아) 결과가 없어.</div>`;
      return;
    }

    const limit = Math.max(1, parseInt(topN.value||'30',10));
    const rows = KPI.slice().sort((a,b)=> (b.sessions||0)-(a.sessions||0)).slice(0, limit);

    campaignWrap.innerHTML = rows.map(r=>{
      const label = `${r.send_id}`;
      const meta = `${r.channel} · S:${r.sessions}`;
      const active = (SELECTED && SELECTED.channel===r.channel && SELECTED.send_id===r.send_id) ? 'active' : '';
      return `<button class="chip ${active}" data-channel="${r.channel}" data-send="${encodeURIComponent(r.send_id)}" title="${meta}">${label}</button>`;
    }).join('');

    Array.from(campaignWrap.querySelectorAll('button.chip')).forEach(btn=>{
      btn.addEventListener('click', ()=>{
        const ch = btn.getAttribute('data-channel');
        const send = decodeURIComponent(btn.getAttribute('data-send')||'');
        selectCampaign(ch, send);
      });
    });
  }

  function aggregateKPIForSelected(){
    if(!SELECTED || !RAW){
      clearAllUI(selRange.textContent||'-');
      tb.innerHTML = `<tr><td colspan="4" class="muted font-semibold">캠페인을 선택하면 상품 리스트가 표시돼.</td></tr>`;
      return;
    }
    const row = RAW.kpi.find(x=> x.channel===SELECTED.channel && x.send_id===SELECTED.send_id);
    if(!row) return;

    const sessions = row.sessions||0;
    const users = row.users||0;
    const purchases = row.purchases||0;
    const revenue = row.revenue||0;
    const items = row.items_purchased||0;

    kSessions.textContent = fmt(sessions);
    kUsers.textContent = fmt(users);
    kPurchases.textContent = fmt(purchases);
    kRevenue.textContent = fmtMoney(revenue);
    kItems.textContent = fmt(items);

    const cvr = sessions ? (purchases/sessions*100) : 0;
    const aov = purchases ? (revenue/purchases) : 0;
    const ips = purchases ? (items/purchases) : 0;

    const rangeText = `${RAW.range.start} ~ ${RAW.range.end} (${RAW.range.dates.length}d)`;
    kSessionsSub.textContent = rangeText;
    kUsersSub.textContent = `Range sum (not dedup)`;
    kCvrSub.textContent = `CVR: ${cvr.toFixed(2)}%`;
    kAovSub.textContent = `AOV: ${fmtMoney(aov)}`;
    kItemsSub.textContent = `Items/Order: ${ips.toFixed(2)}`;

    selChannel.textContent = SELECTED.channel;
    selSendId.textContent = SELECTED.send_id;
    selRange.textContent = rangeText;
  }

  function renderProducts(){
    if(!SELECTED || !RAW){
      tb.innerHTML = `<tr><td colspan="4" class="muted font-semibold">캠페인을 선택하면 상품 리스트가 표시돼.</td></tr>`;
      return;
    }

    const rows = RAW.prod
      .filter(r=> r.channel===SELECTED.channel && r.send_id===SELECTED.send_id)
      .slice()
      .sort((a,b)=> (b.prod_revenue||0)-(a.prod_revenue||0));

    if(!rows.length){
      tb.innerHTML = `<tr><td colspan="4" class="muted font-semibold">구매 상품 데이터가 없어.</td></tr>`;
      return;
    }

    tb.innerHTML = rows.map(r=>`
      <tr>
        <td>${(r.item_name||'-')}</td>
        <td class="mono">${(r.item_id||'-')}</td>
        <td>${fmt(r.prod_items||0)}</td>
        <td>${fmtMoney(r.prod_revenue||0)}</td>
      </tr>
    `).join('');
  }

  function selectCampaign(channel, send_id){
    SELECTED = {channel, send_id};
    renderCampaignButtons();
    aggregateKPIForSelected();
    renderProducts();
  }

  function getDatesInRange(startYMD, endYMD){
    if(!AVAILABLE || !AVAILABLE.length) return [];
    const s = startYMD, e = endYMD;
    const out = [];
    for(const d of AVAILABLE){
      if(d >= s && d <= e) out.push(d);
    }
    return out;
  }

  async function fetchDay(d){
    const url = `data/owned/owned_${d}.json?t=${Date.now()}`;
    const res = await fetch(url, {cache:'no-store'});
    if(!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  }

  function normalizeNumber(x){
    const n = Number(x);
    return isFinite(n) ? n : 0;
  }

  function aggregateRangePayload(startYMD, endYMD, daysPayload){
    const kMap = new Map();
    for(const day of daysPayload){
      for(const r of (day.kpi||[])){
        const key = `${r.channel}||${r.send_id}`;
        const cur = kMap.get(key) || {
          channel: r.channel,
          send_id: r.send_id,
          sessions: 0,
          users: 0,
          purchases: 0,
          revenue: 0,
          items_purchased: 0,
        };
        cur.sessions += normalizeNumber(r.sessions);
        cur.users += normalizeNumber(r.users);
        cur.purchases += normalizeNumber(r.purchases);
        cur.revenue += normalizeNumber(r.revenue);
        cur.items_purchased += normalizeNumber(r.items_purchased);
        kMap.set(key, cur);
      }
    }

    const pMap = new Map();
    for(const day of daysPayload){
      for(const r of (day.prod||[])){
        const key = `${r.channel}||${r.send_id}||${r.item_id}||${r.item_name}`;
        const cur = pMap.get(key) || {
          channel: r.channel,
          send_id: r.send_id,
          item_id: r.item_id,
          item_name: r.item_name,
          prod_items: 0,
          prod_revenue: 0,
        };
        cur.prod_items += normalizeNumber(r.prod_items);
        cur.prod_revenue += normalizeNumber(r.prod_revenue);
        pMap.set(key, cur);
      }
    }

    return {
      range: { start: startYMD, end: endYMD, dates: daysPayload.map(x=>x.date).filter(Boolean) },
      kpi: Array.from(kMap.values()),
      prod: Array.from(pMap.values()),
    };
  }

  async function loadRange(startYMD, endYMD){
    hideNotice();
    SELECTED = null;
    RAW = null;
    KPI = [];
    campaignWrap.innerHTML = '';
    tb.innerHTML = `<tr><td colspan="4" class="muted font-semibold">데이터 로딩 중…</td></tr>`;

    if(!startYMD || !endYMD){
      showNotice('기간(From/To)을 먼저 선택해줘.');
      clearAllUI('-');
      return;
    }
    if(startYMD > endYMD){
      showNotice('From 날짜가 To보다 뒤야. 날짜를 바꿔줘.');
      clearAllUI(`${startYMD} ~ ${endYMD}`);
      return;
    }

    const days = getDatesInRange(startYMD, endYMD);
    if(!days.length){
      const rangeText = `${startYMD} ~ ${endYMD}`;
      showNotice(`선택한 기간에 데이터 파일이 없어. (${rangeText})`);
      clearAllUI(rangeText);
      return;
    }

    if(days.length > 180){
      showNotice(`기간이 너무 길어(${days.length}일) 브라우저가 느려질 수 있어. 180일 이하로 줄여줘.`);
      clearAllUI(`${startYMD} ~ ${endYMD}`);
      return;
    }

    try{
      const payloads = await Promise.all(days.map(d => fetchDay(d)));
      RAW = aggregateRangePayload(startYMD, endYMD, payloads);
    }catch(e){
      RAW = null;
      showNotice(`로드 중 오류: ${e && e.message ? e.message : e}`);
      clearAllUI(`${startYMD} ~ ${endYMD}`);
      return;
    }

    filterKPI();
    renderCampaignButtons();
    aggregateKPIForSelected();
    renderProducts();
  }

  async function loadAvailableDates(){
    try{
      const res = await fetch(`data/owned/available_dates.json?t=${Date.now()}`, {cache:'no-store'});
      if(!res.ok) throw new Error(`HTTP ${res.status}`);
      const j = await res.json();
      AVAILABLE = (j.available_dates||[]).slice().sort();
    }catch(e){
      AVAILABLE = null;
    }
  }

  function setRangeToLastNDays(n){
    const today = new Date();
    const end = ymd(today);
    const start = ymd(addDays(today, -(n-1)));
    startPicker.value = start;
    endPicker.value = end;
    loadRange(start, end);
  }

  function shiftRange(deltaDays){
    if(!startPicker.value || !endPicker.value) return;
    const s = parseYMD(startPicker.value);
    const e = parseYMD(endPicker.value);
    const ns = ymd(addDays(s, deltaDays));
    const ne = ymd(addDays(e, deltaDays));
    startPicker.value = ns;
    endPicker.value = ne;
    loadRange(ns, ne);
  }

  chipAll.addEventListener('click', ()=>{ CHANNEL='ALL'; setChipActive(); filterKPI(); renderCampaignButtons(); });
  chipEDM.addEventListener('click', ()=>{ CHANNEL='EDM'; setChipActive(); filterKPI(); renderCampaignButtons(); });
  chipLMS.addEventListener('click', ()=>{ CHANNEL='LMS'; setChipActive(); filterKPI(); renderCampaignButtons(); });
  chipKAKAO.addEventListener('click', ()=>{ CHANNEL='KAKAO'; setChipActive(); filterKPI(); renderCampaignButtons(); });

  q.addEventListener('input', ()=>{ filterKPI(); renderCampaignButtons(); });
  topN.addEventListener('change', ()=>{ renderCampaignButtons(); });

  btnApply.addEventListener('click', ()=> loadRange(startPicker.value, endPicker.value));
  btnPrev.addEventListener('click', ()=> shiftRange(-1));
  btnNext.addEventListener('click', ()=> shiftRange(+1));
  btn7d.addEventListener('click', ()=> setRangeToLastNDays(7));
  btn30d.addEventListener('click', ()=> setRangeToLastNDays(30));
  btnToday.addEventListener('click', ()=>{
    const d = ymd(new Date());
    startPicker.value = d;
    endPicker.value = d;
    loadRange(d, d);
  });

  btnReload.addEventListener('click', ()=> location.reload());
  noticeClose.addEventListener('click', hideNotice);

  (async function init(){
    setChipActive();
    await loadAvailableDates();

    let d;
    if(AVAILABLE && AVAILABLE.length){
      d = AVAILABLE[AVAILABLE.length-1];
    }else{
      d = ymd(addDays(new Date(), -1));
    }
    startPicker.value = d;
    endPicker.value = d;
    loadRange(d, d);
  })();

})();
</script>
</body>
</html>
"""


# ----------------------------
# Main build
# ----------------------------
def run_query(client: bigquery.Client, sql: str) -> pd.DataFrame:
    job = client.query(sql)
    return job.result().to_dataframe(create_bqstorage_client=False)


def split_rows(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    kpi = df[df["row_type"] == "kpi"].copy()
    prod = df[df["row_type"] == "prod"].copy()
    return kpi, prod


def build_day_payloads(kpi_df: pd.DataFrame, prod_df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """
    Returns dict keyed by YYYY-MM-DD:
      { "2026-02-25": {"kpi":[...], "prod":[...]} }
    """
    out: Dict[str, Dict[str, Any]] = {}

    if not kpi_df.empty:
        for day, g in kpi_df.groupby("date"):
            rows = []
            for _, r in g.iterrows():
                rows.append({
                    "channel": str(r["channel"]),
                    "send_id": str(r["send_id"]),
                    "sessions": int(r["sessions"] or 0),
                    "users": int(r["users"] or 0),
                    "purchases": int(r["purchases"] or 0),
                    "revenue": float(r["kpi_revenue"] or 0),
                    "items_purchased": float(r["items_purchased"] or 0),
                })
            out.setdefault(day, {})["kpi"] = rows

    if not prod_df.empty:
        for day, g in prod_df.groupby("date"):
            rows = []
            for _, r in g.iterrows():
                rows.append({
                    "channel": str(r["channel"]),
                    "send_id": str(r["send_id"]),
                    "item_id": "" if pd.isna(r["item_id"]) else str(r["item_id"]),
                    "item_name": "" if pd.isna(r["item_name"]) else str(r["item_name"]),
                    "prod_items": float(r["prod_items"] or 0),
                    "prod_revenue": float(r["prod_revenue"] or 0),
                })
            out.setdefault(day, {})["prod"] = rows

    # Ensure keys exist
    for day in list(out.keys()):
        out[day].setdefault("kpi", [])
        out[day].setdefault("prod", [])

    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default="", help="BigQuery project id (for events_* table)")
    ap.add_argument("--dataset", default="", help="BigQuery dataset id (for events_* table)")
    ap.add_argument("--table", default="", help="Full table wildcard (optional), e.g. `proj.ds.events_*`")

    ap.add_argument("--start", default="", help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", default="", help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--recent-days", type=int, default=0, help="If start/end omitted, fetch last N days ending yesterday(KST)")

    ap.add_argument("--site-dir", default="reports/owned_portal", help="Output site dir (contains index.html, data/owned/*)")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing owned_YYYY-MM-DD.json")
    ap.add_argument("--write-index", action="store_true", help="Write/overwrite index.html (range UI)")

    args = ap.parse_args()

    maybe_write_sa_from_b64()

    site_dir = Path(args.site_dir)
    data_dir = site_dir / "data" / "owned"
    data_dir.mkdir(parents=True, exist_ok=True)

    # Resolve table fq
    if args.table.strip():
        table_fq = args.table.strip()
        if not (table_fq.startswith("`") and table_fq.endswith("`")):
            table_fq = f"`{table_fq.strip('`')}`"
    else:
        if not args.project or not args.dataset:
            raise SystemExit("[ERROR] Provide --table or (--project and --dataset).")
        table_fq = f"`{args.project}.{args.dataset}.events_*`"

    # Resolve date range
    if args.start and args.end:
        start_d = parse_ymd(args.start)
        end_d = parse_ymd(args.end)
    elif args.recent_days and args.recent_days > 0:
        end_d = kst_yesterday()
        start_d = end_d - timedelta(days=args.recent_days - 1)
    else:
        # safe default: last 7 days ending yesterday
        end_d = kst_yesterday()
        start_d = end_d - timedelta(days=6)

    if start_d > end_d:
        raise SystemExit("[ERROR] start > end.")

    start_suf = ymd_suffix(start_d)
    end_suf = ymd_suffix(end_d)

    print(f"[INFO] Table: {table_fq}")
    print(f"[INFO] Range: {ymd(start_d)} ~ {ymd(end_d)}  (suffix {start_suf} ~ {end_suf})")
    print(f"[INFO] Out: {data_dir.resolve()}")

    client = bigquery.Client()

    sql = build_sql(table_fq, start_suf, end_suf)
    df = run_query(client, sql)

    if df.empty:
        print("[WARN] Query returned 0 rows. Will still refresh available_dates.json.")
        dates = scan_existing_owned_dates(data_dir)
        write_available_dates(data_dir, dates)
        if args.write_index:
            (site_dir / "index.html").write_text(build_index_html_range(), encoding="utf-8")
        print(f"[OK] available_dates.json refreshed (days={len(dates)})")
        return

    kpi_df, prod_df = split_rows(df)
    day_payloads = build_day_payloads(kpi_df, prod_df)

    # Write per-day JSON
    for day in sorted(day_payloads.keys()):
        write_daily_json(
            out_dir=data_dir,
            day=day,
            kpi_rows=day_payloads[day].get("kpi", []),
            prod_rows=day_payloads[day].get("prod", []),
            overwrite=args.overwrite,
        )

    # Refresh available_dates
    dates = scan_existing_owned_dates(data_dir)
    write_available_dates(data_dir, dates)
    print(f"[OK] Wrote {len(day_payloads)} daily JSON(s). available_dates={len(dates)}")

    # Optional index
    if args.write_index:
        (site_dir / "index.html").write_text(build_index_html_range(), encoding="utf-8")
        # pages friendly
        (site_dir / ".nojekyll").write_text("", encoding="utf-8")
        print(f"[OK] index.html written: {(site_dir / 'index.html').resolve()}")

    # quick preview
    print("[INFO] Sample files:")
    for p in sorted(data_dir.glob("owned_*.json"))[-5:]:
        print(" -", p.name)


if __name__ == "__main__":
    main()
