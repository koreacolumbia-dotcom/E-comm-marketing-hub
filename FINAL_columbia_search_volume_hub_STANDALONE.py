#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Columbia Search Volume Dashboard Builder
- GA4 BigQuery: 검색어 / 검색 이벤트 / 검색 사용자
- SQL Server -> BigQuery 적재 테이블: 실제 구매수량 / ERP 매출
- GitHub Actions에서 실행하는 리포트 생성용

Output
- reports/search_volume/index.html
- reports/search_volume/data/search_volume.json
- reports/search_volume/data/meta.json
"""

from __future__ import annotations

import os
import json
import base64
import datetime as dt
from pathlib import Path
from typing import Any

import pandas as pd
from google.cloud import bigquery


KST = dt.timezone(dt.timedelta(hours=9))


def log(msg: str) -> None:
    print(f"[SEARCH_VOLUME] {msg}", flush=True)


def getenv(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def setup_google_credentials() -> None:
    if getenv("GOOGLE_APPLICATION_CREDENTIALS") and Path(getenv("GOOGLE_APPLICATION_CREDENTIALS")).exists():
        return
    b64 = getenv("GOOGLE_SA_JSON_B64")
    if b64:
        p = Path("gcp_service_account.json")
        p.write_bytes(base64.b64decode(b64))
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(p.resolve())


def kst_today() -> dt.date:
    return dt.datetime.now(KST).date()


def parse_date(value: str, default: dt.date) -> dt.date:
    try:
        return dt.datetime.strptime(value, "%Y-%m-%d").date()
    except Exception:
        return default


def fmt_int(v: Any) -> str:
    try:
        return f"{int(round(float(v))):,}"
    except Exception:
        return "-"


def fmt_krw(v: Any) -> str:
    try:
        return f"₩{int(round(float(v))):,}"
    except Exception:
        return "-"


def fmt_pct(v: Any) -> str:
    try:
        return f"{float(v) * 100:.1f}%"
    except Exception:
        return "-"


def extract_yyyymmdd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")


def run_search_query(client: bigquery.Client, start_date: dt.date, end_date: dt.date, lookback_days: int) -> pd.DataFrame:
    events_table = getenv("GA4_EVENTS_TABLE", "columbia-ga4.analytics_358593394.events_*")
    order_table = getenv("BQ_ORDER_PRODUCT_TABLE", "columbia-ga4.crm_raw.tb_order_product_search_mart")
    search_event = getenv("SEARCH_EVENT_NAME", "view_search_results")
    location = getenv("BQ_LOCATION", "asia-northeast3")

    # GA 검색어 이벤트 이후 N일 내 SQL 주문을 user_id/member_id로 매칭
    # search_term은 event_params.search_term 우선, 없으면 page_location의 q/query/keyword/searchKeyword에서 추출
    sql = f"""
    DECLARE start_date DATE DEFAULT @start_date;
    DECLARE end_date DATE DEFAULT @end_date;
    DECLARE lookback_days INT64 DEFAULT @lookback_days;

    WITH search_events AS (
      SELECT
        DATE(TIMESTAMP_MICROS(event_timestamp), 'Asia/Seoul') AS search_date,
        TIMESTAMP_MICROS(event_timestamp) AS search_ts,
        COALESCE(NULLIF(TRIM(user_id), ''), NULL) AS member_id,
        user_pseudo_id,
        CONCAT(user_pseudo_id, '-', CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_id') AS STRING)) AS session_key,
        LOWER(TRIM(COALESCE(
          (SELECT value.string_value FROM UNNEST(event_params) WHERE key='search_term'),
          (SELECT value.string_value FROM UNNEST(event_params) WHERE key='term'),
          REGEXP_EXTRACT((SELECT value.string_value FROM UNNEST(event_params) WHERE key='page_location'), r'[?&](?:q|query|keyword|searchKeyword|search_word|searchTerm)=([^&#]+)')
        ))) AS search_term_raw
      FROM `{events_table}`
      WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', start_date) AND FORMAT_DATE('%Y%m%d', end_date)
        AND event_name = @search_event
    ),
    clean_search AS (
      SELECT
        search_date,
        search_ts,
        member_id,
        user_pseudo_id,
        session_key,
        NULLIF(
          REGEXP_REPLACE(
            REPLACE(REPLACE(REPLACE(search_term_raw, '+', ' '), '%20', ' '), '%EC%BD%9C%EB%A1%AC%EB%B9%84%EC%95%84', '콜롬비아'),
            r'\\s+',
            ' '
          ),
          ''
        ) AS search_term
      FROM search_events
      WHERE search_term_raw IS NOT NULL
        AND search_term_raw NOT IN ('(not set)', 'not set', 'undefined', 'null')
    ),
    search_agg AS (
      SELECT
        search_date,
        search_term,
        COUNT(*) AS searches,
        COUNT(DISTINCT user_pseudo_id) AS search_users,
        COUNT(DISTINCT session_key) AS search_sessions,
        COUNT(DISTINCT member_id) AS login_search_users
      FROM clean_search
      WHERE search_term IS NOT NULL
      GROUP BY 1, 2
    ),
    order_lines AS (
      SELECT
        DATE(order_datetime, 'Asia/Seoul') AS order_date,
        TIMESTAMP(order_datetime) AS order_ts,
        member_id,
        order_no,
        order_product_no,
        product_code,
        brand_code,
        purchase_qty,
        erp_revenue,
        net_erp_revenue
      FROM `{order_table}`
      WHERE order_date BETWEEN start_date AND DATE_ADD(end_date, INTERVAL lookback_days DAY)
        AND member_id IS NOT NULL
        AND member_id != ''
    ),
    joined AS (
      SELECT
        s.search_date,
        s.search_term,
        o.order_no,
        o.order_product_no,
        o.product_code,
        o.brand_code,
        o.purchase_qty,
        o.erp_revenue,
        o.net_erp_revenue
      FROM clean_search s
      INNER JOIN order_lines o
        ON s.member_id = o.member_id
       AND o.order_ts >= s.search_ts
       AND o.order_ts < TIMESTAMP_ADD(s.search_ts, INTERVAL lookback_days DAY)
      WHERE s.search_term IS NOT NULL
    ),
    order_agg AS (
      SELECT
        search_date,
        search_term,
        COUNT(DISTINCT order_no) AS orders,
        COUNT(DISTINCT product_code) AS purchased_products,
        SUM(CAST(purchase_qty AS INT64)) AS purchase_qty,
        SUM(CAST(erp_revenue AS INT64)) AS erp_revenue,
        SUM(CAST(net_erp_revenue AS INT64)) AS net_erp_revenue
      FROM joined
      GROUP BY 1, 2
    )
    SELECT
      a.search_date,
      a.search_term,
      a.searches,
      a.search_users,
      a.search_sessions,
      a.login_search_users,
      IFNULL(o.orders, 0) AS orders,
      IFNULL(o.purchased_products, 0) AS purchased_products,
      IFNULL(o.purchase_qty, 0) AS purchase_qty,
      IFNULL(o.erp_revenue, 0) AS erp_revenue,
      IFNULL(o.net_erp_revenue, 0) AS net_erp_revenue,
      SAFE_DIVIDE(IFNULL(o.orders, 0), a.search_sessions) AS order_cvr
    FROM search_agg a
    LEFT JOIN order_agg o
      USING(search_date, search_term)
    ORDER BY search_date DESC, searches DESC
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date.isoformat()),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date.isoformat()),
            bigquery.ScalarQueryParameter("lookback_days", "INT64", lookback_days),
            bigquery.ScalarQueryParameter("search_event", "STRING", search_event),
        ]
    )
    log(f"Querying GA4 + SQL mart. {start_date}~{end_date}, lookback={lookback_days}d")
    return client.query(sql, job_config=job_config, location=location).to_dataframe()


def make_payload(df: pd.DataFrame, start_date: dt.date, end_date: dt.date, lookback_days: int) -> dict:
    if df.empty:
        daily = pd.DataFrame(columns=["date", "searches", "purchase_qty", "erp_revenue", "net_erp_revenue", "orders"])
        top = pd.DataFrame(columns=["search_term", "searches", "orders", "purchase_qty", "erp_revenue", "net_erp_revenue", "order_cvr"])
    else:
        df["search_date"] = pd.to_datetime(df["search_date"]).dt.date
        daily = (
            df.groupby("search_date", as_index=False)
              .agg(
                  searches=("searches", "sum"),
                  search_users=("search_users", "sum"),
                  search_sessions=("search_sessions", "sum"),
                  orders=("orders", "sum"),
                  purchase_qty=("purchase_qty", "sum"),
                  erp_revenue=("erp_revenue", "sum"),
                  net_erp_revenue=("net_erp_revenue", "sum"),
              )
              .sort_values("search_date")
        )
        daily["date"] = daily["search_date"].astype(str)

        top = (
            df.groupby("search_term", as_index=False)
              .agg(
                  searches=("searches", "sum"),
                  search_users=("search_users", "sum"),
                  search_sessions=("search_sessions", "sum"),
                  orders=("orders", "sum"),
                  purchased_products=("purchased_products", "sum"),
                  purchase_qty=("purchase_qty", "sum"),
                  erp_revenue=("erp_revenue", "sum"),
                  net_erp_revenue=("net_erp_revenue", "sum"),
              )
        )
        top["order_cvr"] = top.apply(lambda r: (r["orders"] / r["search_sessions"]) if r["search_sessions"] else 0, axis=1)
        top = top.sort_values(["net_erp_revenue", "purchase_qty", "searches"], ascending=False).head(30)

    totals = {
        "searches": int(df["searches"].sum()) if not df.empty else 0,
        "search_users": int(df["search_users"].sum()) if not df.empty else 0,
        "search_sessions": int(df["search_sessions"].sum()) if not df.empty else 0,
        "login_search_users": int(df["login_search_users"].sum()) if not df.empty else 0,
        "orders": int(df["orders"].sum()) if not df.empty else 0,
        "purchase_qty": int(df["purchase_qty"].sum()) if not df.empty else 0,
        "erp_revenue": int(df["erp_revenue"].sum()) if not df.empty else 0,
        "net_erp_revenue": int(df["net_erp_revenue"].sum()) if not df.empty else 0,
    }
    totals["order_cvr"] = (totals["orders"] / totals["search_sessions"]) if totals["search_sessions"] else 0

    return {
        "meta": {
            "title": "Search Volume",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "lookback_days": lookback_days,
            "updated_at_kst": dt.datetime.now(KST).strftime("%Y-%m-%d %H:%M KST"),
            "period_text": f"{start_date.isoformat()} ~ {end_date.isoformat()}",
            "source": "GA4 BigQuery + SQL Server order mart",
        },
        "totals": totals,
        "daily": daily.drop(columns=["search_date"], errors="ignore").to_dict("records"),
        "top_terms": top.to_dict("records"),
    }


SEARCH_VOLUME_HTML_TEMPLATE = r"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Search Volume | Columbia E-COMM</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;600;700;800;900&family=Noto+Sans+KR:wght@300;400;600;700;900&display=swap');
    :root{
      --brand:#002d72;
      --ink:#0f172a;
      --muted:#64748b;
      --line:rgba(148,163,184,.25);
      --motion:cubic-bezier(.2,.8,.2,1);
    }
    *{box-sizing:border-box}
    body{
      margin:0;
      min-height:100vh;
      background:
        radial-gradient(circle at 10% 0%, rgba(37,99,235,.14), transparent 28%),
        radial-gradient(circle at 90% 10%, rgba(14,165,233,.12), transparent 26%),
        linear-gradient(180deg,#f8fafc 0%,#eef2f7 100%);
      color:var(--ink);
      font-family:'Plus Jakarta Sans','Noto Sans KR',system-ui,sans-serif;
    }
    .shell{width:100%;max-width:1480px;margin:0 auto;padding:28px;}
    .glass{
      background:rgba(255,255,255,.72);
      border:1px solid rgba(255,255,255,.85);
      box-shadow:0 20px 50px rgba(15,23,42,.06);
      backdrop-filter:blur(18px);
      border-radius:28px;
    }
    .card{
      background:rgba(255,255,255,.82);
      border:1px solid rgba(226,232,240,.85);
      box-shadow:0 16px 38px rgba(15,23,42,.05);
      border-radius:26px;
      transition:transform .24s var(--motion), box-shadow .24s var(--motion);
      animation:rise .7s var(--motion) both;
    }
    .card:hover{transform:translateY(-4px);box-shadow:0 24px 54px rgba(15,23,42,.08)}
    .kpi{
      position:relative;
      overflow:hidden;
    }
    .kpi:before{
      content:'';
      position:absolute;
      inset:-80% auto auto -35%;
      width:70%;
      height:220%;
      background:linear-gradient(90deg,transparent,rgba(255,255,255,.55),transparent);
      transform:rotate(16deg);
      animation:shine 4.8s linear infinite;
    }
    .pill{
      border:1px solid rgba(148,163,184,.28);
      background:#fff;
      color:#64748b;
      border-radius:999px;
      padding:9px 14px;
      font-size:12px;
      font-weight:900;
      transition:.2s var(--motion);
    }
    .pill.active{background:#0f172a;color:#fff;border-color:#0f172a;box-shadow:0 12px 30px rgba(15,23,42,.16)}
    .search-input{
      width:100%;
      background:#fff;
      border:1px solid rgba(148,163,184,.32);
      border-radius:18px;
      padding:13px 16px;
      outline:none;
      font-weight:800;
    }
    .search-input:focus{border-color:rgba(37,99,235,.42);box-shadow:0 0 0 4px rgba(37,99,235,.08)}
    .table-wrap{overflow:auto;border-radius:22px;border:1px solid rgba(226,232,240,.9)}
    table{width:100%;min-width:980px;border-collapse:separate;border-spacing:0;background:white}
    th{
      position:sticky;top:0;
      background:#f8fafc;
      color:#64748b;
      font-size:11px;
      letter-spacing:.08em;
      text-transform:uppercase;
      padding:14px 16px;
      text-align:right;
      border-bottom:1px solid #e2e8f0;
      font-weight:900;
    }
    th:first-child,td:first-child{text-align:left}
    td{
      padding:14px 16px;
      border-bottom:1px solid #f1f5f9;
      font-size:13px;
      font-weight:800;
      text-align:right;
      white-space:nowrap;
    }
    tr:hover td{background:#f8fafc}
    .rank{
      display:inline-flex;align-items:center;justify-content:center;
      width:28px;height:28px;border-radius:10px;background:#eff6ff;color:#1d4ed8;font-weight:900;margin-right:10px;
    }
    .term{font-weight:950;color:#0f172a}
    .sub{color:#64748b;font-weight:700}
    @keyframes rise{from{opacity:0;transform:translateY(22px) scale(.985)}to{opacity:1;transform:translateY(0) scale(1)}}
    @keyframes shine{0%{transform:translateX(-160%) rotate(16deg)}100%{transform:translateX(360%) rotate(16deg)}}
  </style>
</head>
<body>
  <div class="shell">
    <header class="glass p-6 md:p-8 mb-6">
      <div class="flex flex-col xl:flex-row xl:items-end xl:justify-between gap-6">
        <div>
          <div class="text-[11px] font-black tracking-[.28em] uppercase text-slate-400">CSK E-COMM · SEARCH ANALYTICS</div>
          <h1 class="mt-3 text-3xl md:text-5xl font-black tracking-tight">Search Volume</h1>
          <p class="mt-3 text-sm md:text-base font-bold text-slate-500">
            공식몰 검색어 행동은 GA4, 구매수량/구매금액은 SQL Server 주문상품 데이터 기준으로 연결합니다.
          </p>
          <div class="mt-4 flex flex-wrap gap-2 text-xs font-black text-slate-500">
            <span class="px-3 py-2 rounded-full bg-white border border-slate-200" id="periodText">-</span>
            <span class="px-3 py-2 rounded-full bg-white border border-slate-200" id="updatedText">-</span>
            <span class="px-3 py-2 rounded-full bg-white border border-slate-200" id="sourceText">GA4 + SQL Server</span>
          </div>
        </div>

        <div class="flex flex-col md:flex-row gap-3 md:items-center">
          <input id="termFilter" class="search-input md:w-[280px]" placeholder="검색어 필터" />
          <div class="inline-flex gap-2">
            <button class="pill active" data-view="daily">DAILY</button>
            <button class="pill" data-view="week">WEEK</button>
          </div>
        </div>
      </div>
    </header>

    <section class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4 mb-6">
      <div class="card kpi p-6">
        <div class="text-xs font-black text-slate-400 uppercase tracking-widest">Searches</div>
        <div class="mt-3 text-4xl font-black" id="kpiSearches">-</div>
        <div class="mt-2 text-xs font-bold text-slate-500">GA4 view_search_results</div>
      </div>
      <div class="card kpi p-6">
        <div class="text-xs font-black text-slate-400 uppercase tracking-widest">Orders</div>
        <div class="mt-3 text-4xl font-black" id="kpiOrders">-</div>
        <div class="mt-2 text-xs font-bold text-slate-500">검색 후 7일 내 회원 매칭</div>
      </div>
      <div class="card kpi p-6">
        <div class="text-xs font-black text-slate-400 uppercase tracking-widest">Purchase Qty</div>
        <div class="mt-3 text-4xl font-black" id="kpiQty">-</div>
        <div class="mt-2 text-xs font-bold text-slate-500">SQL Server ProductQuantity</div>
      </div>
      <div class="card kpi p-6">
        <div class="text-xs font-black text-slate-400 uppercase tracking-widest">ERP Revenue</div>
        <div class="mt-3 text-4xl font-black" id="kpiRevenue">-</div>
        <div class="mt-2 text-xs font-bold text-slate-500">SQL Server ErpPrice</div>
      </div>
    </section>

    <section class="grid grid-cols-1 xl:grid-cols-12 gap-5 mb-6">
      <div class="card p-6 xl:col-span-8">
        <div class="flex items-center justify-between gap-4 mb-5">
          <div>
            <div class="text-xs font-black text-slate-400 uppercase tracking-widest">Trend</div>
            <h2 class="text-xl font-black mt-1">구매수량 · 구매금액 추이</h2>
          </div>
          <div class="text-xs font-black text-slate-400">Bar = Qty / Line = Revenue</div>
        </div>
        <canvas id="trendChart" height="112"></canvas>
      </div>

      <div class="card p-6 xl:col-span-4">
        <div class="text-xs font-black text-slate-400 uppercase tracking-widest">Top Search Terms</div>
        <h2 class="text-xl font-black mt-1 mb-5">매출 기여 검색어</h2>
        <div id="topCards" class="space-y-3"></div>
      </div>
    </section>

    <section class="card p-6">
      <div class="flex flex-col md:flex-row md:items-end md:justify-between gap-3 mb-5">
        <div>
          <div class="text-xs font-black text-slate-400 uppercase tracking-widest">Detail</div>
          <h2 class="text-xl font-black mt-1">검색어별 성과 테이블</h2>
        </div>
        <div class="text-xs font-bold text-slate-500">정상매출 필터: SQL product_status_no IN (4, 5)</div>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>검색어</th>
              <th>검색수</th>
              <th>검색 세션</th>
              <th>주문수</th>
              <th>구매수량</th>
              <th>ERP 매출</th>
              <th>순 ERP 매출</th>
              <th>구매 CVR</th>
            </tr>
          </thead>
          <tbody id="termRows"></tbody>
        </table>
      </div>
    </section>
  </div>

<script>
const DATA = __DATA_JSON__;

const fmtInt = (v) => Number(v || 0).toLocaleString('ko-KR');
const fmtKrw = (v) => '₩' + Number(v || 0).toLocaleString('ko-KR');
const fmtPct = (v) => `${(Number(v || 0) * 100).toFixed(1)}%`;

let currentView = 'daily';
let trendChart = null;

function groupWeekly(rows){
  const map = new Map();
  rows.forEach(r => {
    const d = new Date(r.date + 'T00:00:00');
    const day = d.getDay();
    const monday = new Date(d);
    monday.setDate(d.getDate() - ((day + 6) % 7));
    const key = monday.toISOString().slice(0,10);
    if(!map.has(key)) map.set(key, {date:key, searches:0, orders:0, purchase_qty:0, erp_revenue:0, net_erp_revenue:0});
    const x = map.get(key);
    x.searches += Number(r.searches || 0);
    x.orders += Number(r.orders || 0);
    x.purchase_qty += Number(r.purchase_qty || 0);
    x.erp_revenue += Number(r.erp_revenue || 0);
    x.net_erp_revenue += Number(r.net_erp_revenue || 0);
  });
  return Array.from(map.values()).sort((a,b)=>a.date.localeCompare(b.date));
}

function getTrendRows(){
  const rows = DATA.daily || [];
  return currentView === 'week' ? groupWeekly(rows) : rows;
}

function renderHeader(){
  document.getElementById('periodText').textContent = DATA.meta.period_text || '-';
  document.getElementById('updatedText').textContent = DATA.meta.updated_at_kst || '-';
  document.getElementById('sourceText').textContent = DATA.meta.source || 'GA4 + SQL Server';

  const t = DATA.totals || {};
  document.getElementById('kpiSearches').textContent = fmtInt(t.searches);
  document.getElementById('kpiOrders').textContent = fmtInt(t.orders);
  document.getElementById('kpiQty').textContent = fmtInt(t.purchase_qty);
  document.getElementById('kpiRevenue').textContent = fmtKrw(t.erp_revenue);
}

function renderTrend(){
  const rows = getTrendRows();
  const labels = rows.map(r => r.date);
  const qty = rows.map(r => Number(r.purchase_qty || 0));
  const revenue = rows.map(r => Number(r.erp_revenue || 0));

  const ctx = document.getElementById('trendChart');
  if(trendChart) trendChart.destroy();

  trendChart = new Chart(ctx, {
    data: {
      labels,
      datasets: [
        {
          type:'bar',
          label:'구매수량',
          data: qty,
          borderWidth: 0,
          borderRadius: 10,
          yAxisID:'y'
        },
        {
          type:'line',
          label:'구매금액',
          data: revenue,
          tension:.35,
          borderWidth:3,
          pointRadius:3,
          yAxisID:'y1'
        }
      ]
    },
    options: {
      responsive:true,
      interaction:{mode:'index',intersect:false},
      plugins:{
        legend:{labels:{font:{weight:'bold'}}},
        tooltip:{
          callbacks:{
            label:(ctx)=>{
              if(ctx.dataset.label === '구매금액') return `${ctx.dataset.label}: ${fmtKrw(ctx.raw)}`;
              return `${ctx.dataset.label}: ${fmtInt(ctx.raw)}`;
            }
          }
        }
      },
      scales:{
        x:{grid:{display:false},ticks:{font:{weight:'bold'},maxRotation:0,autoSkip:true}},
        y:{beginAtZero:true,grid:{color:'rgba(226,232,240,.85)'},ticks:{callback:v=>fmtInt(v)}},
        y1:{beginAtZero:true,position:'right',grid:{drawOnChartArea:false},ticks:{callback:v=>fmtKrw(v)}}
      }
    }
  });
}

function filteredTerms(){
  const q = (document.getElementById('termFilter').value || '').trim().toLowerCase();
  const rows = DATA.top_terms || [];
  if(!q) return rows;
  return rows.filter(r => String(r.search_term || '').toLowerCase().includes(q));
}

function renderTopCards(){
  const rows = filteredTerms().slice(0,5);
  const el = document.getElementById('topCards');
  if(!rows.length){
    el.innerHTML = `<div class="p-5 rounded-2xl bg-slate-50 text-sm font-bold text-slate-500">검색어 데이터가 없습니다.</div>`;
    return;
  }
  el.innerHTML = rows.map((r, idx)=>`
    <div class="p-4 rounded-2xl bg-white border border-slate-200 shadow-sm">
      <div class="flex items-center justify-between gap-3">
        <div class="min-w-0">
          <div><span class="rank">${idx+1}</span><span class="term">${r.search_term || '-'}</span></div>
          <div class="sub text-xs mt-2">검색 ${fmtInt(r.searches)} · 주문 ${fmtInt(r.orders)} · Qty ${fmtInt(r.purchase_qty)}</div>
        </div>
        <div class="text-right font-black">${fmtKrw(r.erp_revenue)}</div>
      </div>
    </div>
  `).join('');
}

function renderTable(){
  const rows = filteredTerms();
  const tbody = document.getElementById('termRows');
  tbody.innerHTML = rows.map((r, idx)=>`
    <tr>
      <td><span class="rank">${idx+1}</span><span class="term">${r.search_term || '-'}</span></td>
      <td>${fmtInt(r.searches)}</td>
      <td>${fmtInt(r.search_sessions)}</td>
      <td>${fmtInt(r.orders)}</td>
      <td>${fmtInt(r.purchase_qty)}</td>
      <td>${fmtKrw(r.erp_revenue)}</td>
      <td>${fmtKrw(r.net_erp_revenue)}</td>
      <td>${fmtPct(r.order_cvr)}</td>
    </tr>
  `).join('');
}

function renderAll(){
  renderHeader();
  renderTrend();
  renderTopCards();
  renderTable();

  try {
    parent.postMessage({ type: 'dailyDigestResize', height: document.documentElement.scrollHeight }, '*');
  } catch(e) {}
}

document.querySelectorAll('.pill[data-view]').forEach(btn=>{
  btn.addEventListener('click', ()=>{
    document.querySelectorAll('.pill[data-view]').forEach(b=>b.classList.remove('active'));
    btn.classList.add('active');
    currentView = btn.dataset.view;
    renderTrend();
  });
});

document.getElementById('termFilter').addEventListener('input', ()=>{
  renderTopCards();
  renderTable();
});

renderAll();
</script>
</body>
</html>
"""


def render_html(payload: dict) -> str:
    """
    Standalone renderer.
    HTML template is embedded in this Python file, so no separate
    columbia_search_volume_hub_template.html file is required.
    """
    data_json = json.dumps(payload, ensure_ascii=False)
    return SEARCH_VOLUME_HTML_TEMPLATE.replace("__DATA_JSON__", data_json)


def main() -> int:
    setup_google_credentials()

    project = getenv("BQ_PROJECT", "columbia-ga4")
    location = getenv("BQ_LOCATION", "asia-northeast3")
    client = bigquery.Client(project=project or None, location=location or None)

    today = kst_today()
    default_end = today - dt.timedelta(days=1)
    days = int(getenv("SEARCH_VOLUME_DAYS", "30"))
    end_date = parse_date(getenv("SEARCH_VOLUME_END_DATE"), default_end)
    start_date = parse_date(getenv("SEARCH_VOLUME_START_DATE"), end_date - dt.timedelta(days=days - 1))
    lookback_days = int(getenv("SEARCH_TO_ORDER_LOOKBACK_DAYS", "7"))

    out_dir = Path(getenv("SEARCH_VOLUME_OUT_DIR", "reports/search_volume"))
    data_dir = out_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    df = run_search_query(client, start_date, end_date, lookback_days)
    payload = make_payload(df, start_date, end_date, lookback_days)

    (data_dir / "search_volume.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (data_dir / "meta.json").write_text(json.dumps(payload["meta"], ensure_ascii=False, indent=2), encoding="utf-8")
    (out_dir / "index.html").write_text(render_html(payload), encoding="utf-8")

    log(f"Wrote {out_dir / 'index.html'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
