#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
OWNED Campaign → Product Performance Portal (GA4 BigQuery Export)
- Pulls OWNED (EDM/LMS/KAKAO) performance + purchased products by campaign(send_id)
- Generates static site for GitHub Pages
  - site/index.html (campaign explorer)
  - site/data/owned/*.json (per date)
  - site/data/owned/available_dates.json
  - .github/workflows/owned_pages.yml (deploy to GitHub Pages)
  - site/.nojekyll

Default: 2025-01-01 ~ today(KST)
Supports:
  - backfill: fetch entire range and write daily json files
  - incremental: fetch recent N days only

Requirements:
  pip install google-cloud-bigquery pandas pyarrow

Auth:
  - Use GOOGLE_APPLICATION_CREDENTIALS or ADC in GitHub Actions
  - Or set env GOOGLE_SA_JSON_B64 and it will write to /tmp/ga_sa.json

Usage:
  python build_owned_campaign_portal.py --project columbia-ga4 --dataset analytics_358593394 --start 2025-01-01 --end 2026-02-25
  python build_owned_campaign_portal.py --recent-days 7
"""

import os
import json
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


def ymd(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def suffix(d: date) -> str:
    return d.strftime("%Y%m%d")


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def daterange(start: date, end: date) -> List[date]:
    cur = start
    out = []
    while cur <= end:
        out.append(cur)
        cur += timedelta(days=1)
    return out


# ----------------------------
# Auth helpers (optional b64)
# ----------------------------
def maybe_write_sa_from_b64() -> None:
    b64 = (os.getenv("GOOGLE_SA_JSON_B64") or "").strip()
    if not b64:
        return
    import base64
    p = Path("/tmp/ga_sa.json")
    p.write_bytes(base64.b64decode(b64))
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(p)


# ----------------------------
# BigQuery SQL
# ----------------------------
def build_sql(project: str, dataset: str, start_suffix: str, end_suffix: str) -> str:
    table = f"`{project}.{dataset}.events_*`"
    # Notes:
    # - sessions/users from session_start
    # - purchase facts from purchase
    # - product facts from purchase + UNNEST(items)
    # - send_id: KAKAO -> manual_term, else campaign
    # - channel: KAKAO / LMS / EDM
    # - owned filter: source-based
    return f"""
DECLARE start_suffix STRING DEFAULT '{start_suffix}';
DECLARE end_suffix   STRING DEFAULT '{end_suffix}';

WITH base AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS date,
    event_name,
    user_pseudo_id,

    collected_traffic_source.manual_source AS source,
    collected_traffic_source.manual_medium AS medium,
    collected_traffic_source.manual_campaign_name AS campaign,
    collected_traffic_source.manual_term AS manual_term,

    ecommerce.transaction_id AS transaction_id,
    ecommerce.purchase_revenue AS purchase_revenue,
    ecommerce.total_item_quantity AS total_item_quantity,

    items
  FROM {table}
  WHERE _TABLE_SUFFIX BETWEEN start_suffix AND end_suffix
),

owned AS (
  SELECT
    date,
    event_name,
    user_pseudo_id,
    source, medium, campaign, manual_term,
    transaction_id,
    purchase_revenue,
    total_item_quantity,
    items,

    CASE
      WHEN UPPER(source) LIKE 'KAKAO%' THEN manual_term
      WHEN UPPER(source) LIKE 'LMS%' THEN campaign
      WHEN UPPER(source) IN ('EDM','EMAIL_MKT','DM') THEN campaign
      ELSE campaign
    END AS send_id,

    CASE
      WHEN UPPER(source) LIKE 'KAKAO%' THEN 'KAKAO'
      WHEN UPPER(source) LIKE 'LMS%' THEN 'LMS'
      WHEN UPPER(source) IN ('EDM','EMAIL_MKT','DM') THEN 'EDM'
      ELSE 'OTHER'
    END AS channel
  FROM base
  WHERE
    UPPER(source) LIKE 'KAKAO%'
    OR UPPER(source) LIKE 'LMS%'
    OR UPPER(source) IN ('EDM','EMAIL_MKT','DM')
),

kpi AS (
  SELECT
    date,
    channel,
    send_id,

    COUNTIF(event_name='session_start') AS sessions,
    COUNT(DISTINCT IF(event_name='session_start', user_pseudo_id, NULL)) AS users,

    COUNT(DISTINCT IF(event_name='purchase', transaction_id, NULL)) AS purchases,
    SUM(IF(event_name='purchase', purchase_revenue, 0)) AS revenue,
    SUM(IF(event_name='purchase', total_item_quantity, 0)) AS items_purchased
  FROM owned
  WHERE send_id IS NOT NULL
  GROUP BY date, channel, send_id
),

prod AS (
  SELECT
    o.date,
    o.channel,
    o.send_id,
    it.item_id AS item_id,
    it.item_name AS item_name,
    SUM(IFNULL(it.quantity, 0)) AS items,
    SUM(IFNULL(it.item_revenue, 0)) AS revenue
  FROM owned o,
  UNNEST(o.items) AS it
  WHERE o.event_name='purchase'
    AND o.send_id IS NOT NULL
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
  p.items AS prod_items,
  p.revenue AS prod_revenue,
  NULL AS sessions,
  NULL AS users,
  NULL AS purchases,
  NULL AS kpi_revenue,
  NULL AS items_purchased
FROM prod p
;
"""


# ----------------------------
# HTML (Tailwind + your glass/chip/btn design)
# ----------------------------
def build_index_html() -> str:
    # This is a static SPA:
    # - pick date
    # - channel chips
    # - campaign buttons (send_id)
    # - KPI cards + product table
    # Data source: site/data/owned/owned_YYYY-MM-DD.json
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
    .btn:disabled, .chip:disabled{
      opacity: .55;
      cursor: not-allowed;
      transform:none !important;
      box-shadow:none !important;
    }
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
        <div class="small-label">Date</div>
        <div class="w-[160px]"><input id="datePicker" type="date" /></div>
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
        <div id="selMeta" class="pill"><span class="mono" id="selChannel">-</span> · <span class="mono" id="selSendId">-</span></div>
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
        데이터: GA4 BigQuery Export · send_id 규칙: KAKAO=manual_term, EDM/LMS=campaign · purchase 기준 상품 집계
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

  const datePicker = document.getElementById('datePicker');
  const btnPrev = document.getElementById('btnPrev');
  const btnNext = document.getElementById('btnNext');
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
  let RAW = null;      // {kpi:[], prod:[]}
  let KPI = [];        // filtered KPI rows
  let PROD = [];       // filtered PROD rows
  let SELECTED = null; // {channel, send_id}

  function setChipActive(){
    [chipAll, chipEDM, chipLMS, chipKAKAO].forEach(el=>el.classList.remove('active'));
    if(CHANNEL==='ALL') chipAll.classList.add('active');
    if(CHANNEL==='EDM') chipEDM.classList.add('active');
    if(CHANNEL==='LMS') chipLMS.classList.add('active');
    if(CHANNEL==='KAKAO') chipKAKAO.classList.add('active');
  }

  function filterRows(){
    if(!RAW){ KPI=[]; PROD=[]; return; }
    const qq = (q.value||'').trim().toLowerCase();

    KPI = RAW.kpi.filter(r=>{
      if(CHANNEL!=='ALL' && r.channel!==CHANNEL) return false;
      if(qq && String(r.send_id||'').toLowerCase().indexOf(qq)===-1) return false;
      return true;
    });

    PROD = RAW.prod.filter(r=>{
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
    // Sort by sessions desc
    const rows = KPI.slice().sort((a,b)=> (b.sessions||0)-(a.sessions||0)).slice(0, limit);

    const html = rows.map(r=>{
      const label = `${r.send_id}`;
      const meta = `${r.channel} · S:${r.sessions}`;
      const active = (SELECTED && SELECTED.channel===r.channel && SELECTED.send_id===r.send_id) ? 'active' : '';
      return `<button class="chip ${active}" data-channel="${r.channel}" data-send="${encodeURIComponent(r.send_id)}" title="${meta}">${label}</button>`;
    }).join('');

    campaignWrap.innerHTML = html;

    // bind
    Array.from(campaignWrap.querySelectorAll('button.chip')).forEach(btn=>{
      btn.addEventListener('click', ()=>{
        const ch = btn.getAttribute('data-channel');
        const send = decodeURIComponent(btn.getAttribute('data-send')||'');
        selectCampaign(ch, send);
      });
    });
  }

  function aggregateKPIForSelected(){
    if(!SELECTED){
      kSessions.textContent='-'; kUsers.textContent='-'; kPurchases.textContent='-'; kRevenue.textContent='-'; kItems.textContent='-';
      kSessionsSub.textContent='-'; kUsersSub.textContent='-'; kCvrSub.textContent='-'; kAovSub.textContent='-'; kItemsSub.textContent='-';
      selChannel.textContent='-'; selSendId.textContent='-';
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

    kSessionsSub.textContent = `-`;
    kUsersSub.textContent = `-`;
    kCvrSub.textContent = `CVR: ${cvr.toFixed(2)}%`;
    kAovSub.textContent = `AOV: ${fmtMoney(aov)}`;
    kItemsSub.textContent = `Items/Order: ${ips.toFixed(2)}`;

    selChannel.textContent = SELECTED.channel;
    selSendId.textContent = SELECTED.send_id;
  }

  function renderProducts(){
    if(!SELECTED){
      tb.innerHTML = `<tr><td colspan="4" class="muted font-semibold">캠페인을 선택하면 상품 리스트가 표시돼.</td></tr>`;
      return;
    }

    const rows = RAW.prod
      .filter(r=> r.channel===SELECTED.channel && r.send_id===SELECTED.send_id)
      .slice()
      .sort((a,b)=> (b.prod_revenue||0)-(a.prod_revenue||0));

    if(!rows.length){
      tb.innerHTML = `<tr><td colspan="4" class="muted font-semibold">구매 상품 데이터가 없어. (purchase가 없거나 items가 비어있을 수 있어)</td></tr>`;
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

  async function loadDate(d){
    hideNotice();
    SELECTED = null;

    const url = `data/owned/owned_${d}.json?t=${Date.now()}`;
    try{
      const res = await fetch(url, {cache:'no-store'});
      if(!res.ok) throw new Error(`HTTP ${res.status}`);
      RAW = await res.json();
    }catch(e){
      RAW = null;
      KPI = []; PROD = [];
      campaignWrap.innerHTML = '';
      tb.innerHTML = `<tr><td colspan="4" class="muted font-semibold">해당 날짜 데이터 파일이 없어: owned_${d}.json</td></tr>`;
      showNotice(`데이터가 없어서 표시할 수 없어. (${d})`);
      aggregateKPIForSelected();
      return;
    }

    filterRows();
    renderCampaignButtons();
    aggregateKPIForSelected();
    renderProducts();
  }

  // events
  chipAll.addEventListener('click', ()=>{ CHANNEL='ALL'; setChipActive(); filterRows(); renderCampaignButtons(); });
  chipEDM.addEventListener('click', ()=>{ CHANNEL='EDM'; setChipActive(); filterRows(); renderCampaignButtons(); });
  chipLMS.addEventListener('click', ()=>{ CHANNEL='LMS'; setChipActive(); filterRows(); renderCampaignButtons(); });
  chipKAKAO.addEventListener('click', ()=>{ CHANNEL='KAKAO'; setChipActive(); filterRows(); renderCampaignButtons(); });

  q.addEventListener('input', ()=>{ filterRows(); renderCampaignButtons(); });
  topN.addEventListener('change', ()=>{ renderCampaignButtons(); });

  btnPrev.addEventListener('click', ()=>{ const cur=parseYMD(datePicker.value); const d=ymd(addDays(cur,-1)); datePicker.value=d; loadDate(d); });
  btnNext.addEventListener('click', ()=>{ const cur=parseYMD(datePicker.value); const d=ymd(addDays(cur,+1)); datePicker.value=d; loadDate(d); });
  btnToday.addEventListener('click', ()=>{ const now=new Date(); const d=ymd(now); datePicker.value=d; loadDate(d); });

  btnReload.addEventListener('click', ()=> location.reload());
  noticeClose.addEventListener('click', hideNotice);

  // init date: yesterday (KST-ish; browser local ok for UX)
  (function init(){
    const today = new Date();
    const d = ymd(addDays(today,-1));
    datePicker.value = d;
    loadDate(d);
    setChipActive();
  })();

})();
</script>
</body>
</html>
"""


def build_hub_html_placeholder(user_hub_html: Optional[str] = None) -> str:
    # If you want to include your existing hub html exactly:
    # - pass it as --hub-html-file and we will embed it.
    # Otherwise create a simple link page.
    if user_hub_html:
        return user_hub_html
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
    <div class="text-slate-600 font-semibold mb-6">원래 사용하던 Daily/Weekly Hub HTML을 여기에 붙여넣어서 사용할 수 있어.</div>
    <a class="px-4 py-3 rounded-xl bg-slate-900 text-white font-black inline-block" href="./index.html">OWNED Campaign Explorer로 이동</a>
  </div>
</body>
</html>
"""


# ----------------------------
# Workflow YAML (GitHub Pages)
# ----------------------------
def build_pages_workflow() -> str:
    # Uses actions/configure-pages + upload-pages-artifact + deploy-pages
    # Runs daily and on manual dispatch
    return """name: OWNED Campaign Portal (Build & Deploy)

on:
  workflow_dispatch:
    inputs:
      recent_days:
        description: "Fetch recent N days (leave empty to use default 7)"
        required: false
        default: "7"
  schedule:
    - cron: "20 22 * * *"  # 07:20 KST daily

permissions:
  contents: write
  pages: write
  id-token: write

concurrency:
  group: "pages"
  cancel-in-progress: false

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install deps
        run: |
          pip install -U pip
          pip install google-cloud-bigquery pandas pyarrow

      - name: Build site (fetch + render)
        env:
          GOOGLE_SA_JSON_B64: ${{ secrets.GOOGLE_SA_JSON_B64 }}
        run: |
          RECENT="${{ github.event.inputs.recent_days }}"
          if [ -z "$RECENT" ]; then RECENT="7"; fi
          python build_owned_campaign_portal.py --recent-days "$RECENT"

      - name: Commit data changes
        run: |
          git config user.name "github-actions"
          git config user.email "github-actions@users.noreply.github.com"
          git add site/ .github/workflows/owned_pages.yml build_owned_campaign_portal.py || true
          git commit -m "Update OWNED portal data" || true
          git push || true

      - name: Setup Pages
        uses: actions/configure-pages@v5

      - name: Upload artifact
        uses: actions/upload-pages-artifact@v3
        with:
          path: site

  deploy:
    needs: build
    runs-on: ubuntu-latest
    environment:
      name: github-pages
      url: ${{ steps.deployment.outputs.page_url }}
    steps:
      - name: Deploy to GitHub Pages
        id: deployment
        uses: actions/deploy-pages@v4
"""


# ----------------------------
# JSON writing
# ----------------------------
def write_daily_json(out_dir: Path, d: str, kpi_rows: List[Dict[str, Any]], prod_rows: List[Dict[str, Any]]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "date": d,
        "kpi": kpi_rows,
        "prod": prod_rows,
    }
    (out_dir / f"owned_{d}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_available_dates(out_dir: Path, dates: List[str]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = {"available_dates": sorted(dates)}
    (out_dir / "available_dates.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


# ----------------------------
# Main
# ----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default=os.getenv("BQ_PROJECT", "columbia-ga4"))
    ap.add_argument("--dataset", default=os.getenv("BQ_DATASET", "analytics_358593394"))
    ap.add_argument("--start", default="2025-01-01")
    ap.add_argument("--end", default=None)
    ap.add_argument("--recent-days", type=int, default=None, help="If set, fetch only recent N days ending today(KST).")
    ap.add_argument("--site-dir", default="site")
    ap.add_argument("--hub-html-file", default=None, help="Optional: path to your existing Hub HTML (will be copied to site/hub.html).")
    args = ap.parse_args()

    maybe_write_sa_from_b64()

    site_dir = Path(args.site_dir)
    data_dir = site_dir / "data" / "owned"
    wf_path = Path(".github/workflows/owned_pages.yml")

    # Determine date range
    end_d = parse_date(args.end) if args.end else kst_today()
    if args.recent_days and args.recent_days > 0:
        start_d = end_d - timedelta(days=args.recent_days - 1)
    else:
        start_d = parse_date(args.start)

    start_sfx = suffix(start_d)
    end_sfx = suffix(end_d)

    # BigQuery query
    client = bigquery.Client()
    sql = build_sql(args.project, args.dataset, start_sfx, end_sfx)
    job = client.query(sql)
    df = job.result().to_dataframe(create_bqstorage_client=True)

    if df.empty:
        print("[WARN] Query returned no rows.")
        # Still write site skeleton
        site_dir.mkdir(parents=True, exist_ok=True)
        (site_dir / ".nojekyll").write_text("", encoding="utf-8")
        (site_dir / "index.html").write_text(build_index_html(), encoding="utf-8")
        hub_html = None
        if args.hub_html_file and Path(args.hub_html_file).exists():
            hub_html = Path(args.hub_html_file).read_text(encoding="utf-8")
        (site_dir / "hub.html").write_text(build_hub_html_placeholder(hub_html), encoding="utf-8")
        wf_path.parent.mkdir(parents=True, exist_ok=True)
        wf_path.write_text(build_pages_workflow(), encoding="utf-8")
        return

    # Normalize
    df["date"] = df["date"].astype(str)
    # Split kpi/prod
    kpi_df = df[df["row_type"] == "kpi"].copy()
    prod_df = df[df["row_type"] == "prod"].copy()

    # Clean numeric
    for col in ["sessions", "users", "purchases", "kpi_revenue", "items_purchased", "prod_items", "prod_revenue"]:
        if col in df.columns:
            if col in kpi_df.columns:
                kpi_df[col] = pd.to_numeric(kpi_df[col], errors="coerce").fillna(0)
            if col in prod_df.columns:
                prod_df[col] = pd.to_numeric(prod_df[col], errors="coerce").fillna(0)

    # Write daily jsons
    site_dir.mkdir(parents=True, exist_ok=True)
    (site_dir / ".nojekyll").write_text("", encoding="utf-8")

    available = []

    for d in sorted(set(df["date"].tolist())):
        k_rows = kpi_df[kpi_df["date"] == d][["date","channel","send_id","sessions","users","purchases","kpi_revenue","items_purchased"]].to_dict(orient="records")
        # rename to stable fields
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
                "items_purchased": float(r.get("items_purchased", 0.0)),
            })

        p_rows = prod_df[prod_df["date"] == d][["date","channel","send_id","item_id","item_name","prod_items","prod_revenue"]].to_dict(orient="records")
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
        available.append(d)

    write_available_dates(data_dir, available)

    # Write HTML pages
    (site_dir / "index.html").write_text(build_index_html(), encoding="utf-8")

    hub_html = None
    if args.hub_html_file and Path(args.hub_html_file).exists():
        hub_html = Path(args.hub_html_file).read_text(encoding="utf-8")
    (site_dir / "hub.html").write_text(build_hub_html_placeholder(hub_html), encoding="utf-8")

    # Write workflow
    wf_path.parent.mkdir(parents=True, exist_ok=True)
    wf_path.write_text(build_pages_workflow(), encoding="utf-8")

    print(f"[OK] Wrote site to: {site_dir.resolve()}")
    print(f"[OK] Data files: {data_dir.resolve()} (days={len(available)})")
    print("[OK] Next: push to GitHub, enable Pages (Deploy from Actions).")


if __name__ == "__main__":
    main()
