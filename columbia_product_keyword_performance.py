#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Columbia Product Keyword Performance Dashboard Builder
- GA4 BigQuery: 키워드 / 검색 이벤트 / 검색 사용자
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
    print(f"[PRODUCT_KEYWORD] {msg}", flush=True)


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

    # GA4 purchase.transaction_id = SQL order_no 매칭 추가.
    # 기존 user_id/member_id 매칭이 0이어도 transaction_id가 맞으면 주문수/수량/매출이 들어옵니다.
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
    ga_purchase AS (
      SELECT
        TIMESTAMP_MICROS(event_timestamp) AS purchase_ts,
        user_pseudo_id,
        COALESCE(NULLIF(TRIM(user_id), ''), NULL) AS member_id,
        NULLIF(TRIM(COALESCE(
          ecommerce.transaction_id,
          (SELECT value.string_value FROM UNNEST(event_params) WHERE key='transaction_id'),
          (SELECT value.string_value FROM UNNEST(event_params) WHERE key='order_no'),
          (SELECT value.string_value FROM UNNEST(event_params) WHERE key='orderNo')
        )), '') AS transaction_id
      FROM `{events_table}`
      WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', start_date)
                              AND FORMAT_DATE('%Y%m%d', DATE_ADD(end_date, INTERVAL lookback_days DAY))
        AND event_name = 'purchase'
    ),
    order_lines AS (
      SELECT
        order_date,
        TIMESTAMP(order_datetime) AS order_ts,
        CAST(member_id AS STRING) AS member_id,
        CAST(order_no AS STRING) AS order_no,
        CAST(order_product_no AS STRING) AS order_product_no,
        CAST(product_code AS STRING) AS product_code,
        CAST(brand_code AS STRING) AS brand_code,
        CAST(purchase_qty AS INT64) AS purchase_qty,
        CAST(erp_revenue AS INT64) AS erp_revenue,
        CAST(net_erp_revenue AS INT64) AS net_erp_revenue
      FROM `{order_table}`
      WHERE order_date BETWEEN start_date AND DATE_ADD(end_date, INTERVAL lookback_days DAY)
    ),
    joined_by_transaction AS (
      SELECT
        s.search_date, s.search_term,
        o.order_no, o.order_product_no, o.product_code, o.brand_code,
        o.purchase_qty, o.erp_revenue, o.net_erp_revenue,
        'transaction_id' AS match_type
      FROM clean_search s
      INNER JOIN ga_purchase p
        ON s.user_pseudo_id = p.user_pseudo_id
       AND p.purchase_ts >= s.search_ts
       AND p.purchase_ts < TIMESTAMP_ADD(s.search_ts, INTERVAL lookback_days DAY)
       AND p.transaction_id IS NOT NULL
      INNER JOIN order_lines o
        ON p.transaction_id = o.order_no
      WHERE s.search_term IS NOT NULL
    ),
    joined_by_member AS (
      SELECT
        s.search_date, s.search_term,
        o.order_no, o.order_product_no, o.product_code, o.brand_code,
        o.purchase_qty, o.erp_revenue, o.net_erp_revenue,
        'member_id' AS match_type
      FROM clean_search s
      INNER JOIN order_lines o
        ON s.member_id = o.member_id
       AND s.member_id IS NOT NULL
       AND o.order_ts >= s.search_ts
       AND o.order_ts < TIMESTAMP_ADD(s.search_ts, INTERVAL lookback_days DAY)
      WHERE s.search_term IS NOT NULL
    ),
    joined_union AS (
      SELECT * FROM joined_by_transaction
      UNION ALL
      SELECT * FROM joined_by_member
    ),
    joined_dedup AS (
      SELECT * EXCEPT(rn)
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (
            PARTITION BY search_date, search_term, order_product_no
            ORDER BY IF(match_type='transaction_id', 0, 1)
          ) AS rn
        FROM joined_union
      )
      WHERE rn = 1
    ),
    order_agg AS (
      SELECT
        search_date,
        search_term,
        COUNT(DISTINCT order_no) AS orders,
        COUNT(DISTINCT product_code) AS purchased_products,
        SUM(purchase_qty) AS purchase_qty,
        SUM(erp_revenue) AS erp_revenue,
        SUM(net_erp_revenue) AS net_erp_revenue,
        COUNTIF(match_type = 'transaction_id') AS matched_by_transaction_rows,
        COUNTIF(match_type = 'member_id') AS matched_by_member_rows
      FROM joined_dedup
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
      IFNULL(o.matched_by_transaction_rows, 0) AS matched_by_transaction_rows,
      IFNULL(o.matched_by_member_rows, 0) AS matched_by_member_rows,
      SAFE_DIVIDE(IFNULL(o.orders, 0), a.search_sessions) AS order_cvr
    FROM search_agg a
    LEFT JOIN order_agg o USING(search_date, search_term)
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
    log(f"Querying GA4 keyword + SQL order product mart. {start_date}~{end_date}, lookback={lookback_days}d")
    return client.query(sql, job_config=job_config, location=location).to_dataframe()

def make_payload(df: pd.DataFrame, start_date: dt.date, end_date: dt.date, lookback_days: int) -> dict:
    """
    Build dashboard payload.

    raw_daily_by_term is always defined so the HTML keyword filter chart
    works even when the query returns zero rows.
    """
    raw_daily_by_term = []

    if df.empty:
        daily = pd.DataFrame(
            columns=[
                "date",
                "searches",
                "search_users",
                "search_sessions",
                "orders",
                "purchase_qty",
                "erp_revenue",
                "net_erp_revenue",
            ]
        )
        top = pd.DataFrame(
            columns=[
                "search_term",
                "searches",
                "search_users",
                "search_sessions",
                "orders",
                "purchased_products",
                "purchase_qty",
                "erp_revenue",
                "net_erp_revenue",
                "order_cvr",
            ]
        )
    else:
        df = df.copy()
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

        raw_daily_by_term_df = (
            df.groupby(["search_date", "search_term"], as_index=False)
              .agg(
                  searches=("searches", "sum"),
                  orders=("orders", "sum"),
                  purchase_qty=("purchase_qty", "sum"),
                  erp_revenue=("erp_revenue", "sum"),
                  net_erp_revenue=("net_erp_revenue", "sum"),
              )
              .sort_values(["search_date", "search_term"])
        )
        raw_daily_by_term_df["date"] = raw_daily_by_term_df["search_date"].astype(str)
        raw_daily_by_term = raw_daily_by_term_df.drop(columns=["search_date"], errors="ignore").to_dict("records")

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
        top["order_cvr"] = top.apply(
            lambda r: (r["orders"] / r["search_sessions"]) if r["search_sessions"] else 0,
            axis=1,
        )
        top = top.sort_values(["net_erp_revenue", "purchase_qty", "searches"], ascending=False).head(30)

    totals = {
        "searches": int(df["searches"].sum()) if not df.empty else 0,
        "search_users": int(df["search_users"].sum()) if not df.empty else 0,
        "search_sessions": int(df["search_sessions"].sum()) if not df.empty else 0,
        "login_search_users": int(df["login_search_users"].sum()) if not df.empty and "login_search_users" in df.columns else 0,
        "orders": int(df["orders"].sum()) if not df.empty else 0,
        "purchase_qty": int(df["purchase_qty"].sum()) if not df.empty else 0,
        "erp_revenue": int(df["erp_revenue"].sum()) if not df.empty else 0,
        "net_erp_revenue": int(df["net_erp_revenue"].sum()) if not df.empty else 0,
        "matched_by_transaction_rows": int(df["matched_by_transaction_rows"].sum()) if not df.empty and "matched_by_transaction_rows" in df.columns else 0,
        "matched_by_member_rows": int(df["matched_by_member_rows"].sum()) if not df.empty and "matched_by_member_rows" in df.columns else 0,
    }
    totals["order_cvr"] = (totals["orders"] / totals["search_sessions"]) if totals["search_sessions"] else 0

    return {
        "meta": {
            "title": "상품 키워드 성과",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "lookback_days": lookback_days,
            "updated_at_kst": dt.datetime.now(KST).strftime("%Y-%m-%d %H:%M KST"),
            "period_text": f"{start_date.isoformat()} ~ {end_date.isoformat()}",
            "source": "GA4 search terms + SQL Server order product mart",
        },
        "totals": totals,
        "daily": daily.drop(columns=["search_date"], errors="ignore").to_dict("records"),
        "top_terms": top.to_dict("records"),
        "raw_daily_by_term": raw_daily_by_term,
    }


SEARCH_VOLUME_HTML_TEMPLATE = r"""<!doctype html>
<html lang="ko">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>상품 키워드 성과 | Columbia E-COMM</title>
<script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;600;700;800;900&family=Noto+Sans+KR:wght@300;400;600;700;900&display=swap');
*{box-sizing:border-box} body{margin:0;background:linear-gradient(180deg,#f8fafc,#eef2f7);font-family:'Plus Jakarta Sans','Noto Sans KR',system-ui,sans-serif;color:#0f172a}
.shell{max-width:1480px;margin:0 auto;padding:8px 12px 28px}.card{background:rgba(255,255,255,.94);border:1px solid #e2e8f0;box-shadow:0 16px 42px rgba(15,23,42,.05);border-radius:26px}.chart-card{padding:24px;margin-bottom:18px}.topbar{display:flex;align-items:flex-start;justify-content:space-between;gap:18px;margin-bottom:18px}.eyebrow{font-size:12px;font-weight:950;letter-spacing:.16em;text-transform:uppercase;color:#94a3b8}h1{font-size:26px;line-height:1.25;margin:5px 0 0;font-weight:950;letter-spacing:-.03em}.meta{font-size:12px;font-weight:850;color:#64748b;margin-top:8px}.controls{display:flex;align-items:center;gap:10px;flex-wrap:wrap;justify-content:flex-end}.keyword-wrap{display:flex;align-items:center;gap:8px;background:#fff200;border:1px solid rgba(15,23,42,.08);box-shadow:0 10px 28px rgba(15,23,42,.05);border-radius:18px;padding:9px 12px;min-width:300px}.keyword-label{font-size:13px;font-weight:950;color:#111827;white-space:nowrap}#keywordFilterTop{width:100%;border:0;outline:0;background:transparent;font-size:14px;font-weight:900;color:#0f172a}.periods{display:flex;border:1px solid #bfdbfe;border-radius:18px;overflow:hidden;background:#d7eef8}.period-btn{border:0;min-width:92px;padding:13px 18px;background:transparent;font-size:14px;font-weight:950;color:#0f172a;cursor:pointer}.period-btn.active{background:#0f172a;color:#fff}.kpis{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-bottom:16px}.kpi{background:#f8fafc;border:1px solid #e2e8f0;border-radius:18px;padding:16px 18px;min-height:92px}.kpi-label{font-size:11px;font-weight:950;letter-spacing:.12em;text-transform:uppercase;color:#64748b}.kpi-value{font-size:30px;font-weight:950;margin-top:8px;letter-spacing:-.03em}.mixed-panel{background:#fff;border:1px solid #e2e8f0;border-radius:22px;padding:22px;min-height:410px}.panel-head{display:flex;align-items:flex-end;justify-content:space-between;gap:12px;margin-bottom:10px}.panel-title{font-size:18px;font-weight:950}.panel-sub{font-size:12px;font-weight:800;color:#64748b}.table-card{padding:20px 22px}.table-wrap{overflow:auto;border-radius:20px;border:1px solid #e2e8f0}table{width:100%;min-width:960px;border-collapse:separate;border-spacing:0;background:white}th{background:#f8fafc;color:#64748b;font-size:11px;letter-spacing:.08em;text-transform:uppercase;padding:13px 14px;text-align:right;border-bottom:1px solid #e2e8f0;font-weight:950}th:first-child,td:first-child{text-align:left}td{padding:14px;border-bottom:1px solid #f1f5f9;font-size:13px;font-weight:850;text-align:right;white-space:nowrap}tr:hover td{background:#f8fafc}.rank{display:inline-flex;align-items:center;justify-content:center;width:26px;height:26px;border-radius:9px;background:#eff6ff;color:#1d4ed8;font-weight:950;margin-right:10px}.term{font-weight:950;color:#0f172a}.notice{display:none;margin:12px 0 0;padding:12px 14px;border-radius:16px;background:#fff7ed;border:1px solid #fed7aa;color:#9a3412;font-size:12px;font-weight:850}@media(max-width:900px){.topbar{flex-direction:column}.controls{width:100%}.keyword-wrap{min-width:0;width:100%}.periods{width:100%}.period-btn{flex:1}.kpis{grid-template-columns:repeat(2,minmax(0,1fr))}.kpi-value{font-size:24px}}
</style>
</head>
<body>
<div class="shell">
<section class="card chart-card">
<div class="topbar"><div><div class="eyebrow">DETAIL</div><h1>키워드별 구매상품 성과</h1><div class="meta" id="metaText">-</div></div><div class="controls"><label class="keyword-wrap"><span class="keyword-label">검색</span><input id="keywordFilterTop" placeholder="키워드 입력" /></label><div class="periods"><button class="period-btn active" data-view="daily">DAILY</button><button class="period-btn" data-view="week">WEEK</button></div></div></div>
<div class="kpis"><div class="kpi"><div class="kpi-label">키워드 검색수</div><div class="kpi-value" id="kpiSearches">-</div></div><div class="kpi"><div class="kpi-label">주문수</div><div class="kpi-value" id="kpiOrders">-</div></div><div class="kpi"><div class="kpi-label">구매수량</div><div class="kpi-value" id="kpiQty">-</div></div><div class="kpi"><div class="kpi-label">구매금액</div><div class="kpi-value" id="kpiRevenue">-</div></div></div>
<div class="mixed-panel"><div class="panel-head"><div><div class="panel-title">그래프 혼합</div><div class="panel-sub">구매수량 = 막대그래프 · 구매금액 = 꺾은선 그래프</div></div><div class="panel-sub" id="matchText">-</div></div><canvas id="mixedChart" height="108"></canvas><div class="notice" id="zeroNotice">구매 데이터가 0이면 GA4 purchase의 transaction_id와 SQL order_no 매칭 여부를 확인해야 합니다. 이번 패치에는 user_id/member_id 매칭과 transaction_id/order_no 매칭을 모두 포함했습니다.</div></div>
</section>
<section class="card table-card"><div class="topbar" style="margin-bottom:14px;"><div><div class="eyebrow">DETAIL</div><h1 style="font-size:22px;">키워드별 구매상품 성과</h1></div></div><div class="table-wrap"><table><thead><tr><th>키워드</th><th>검색수</th><th>검색 세션</th><th>주문수</th><th>구매수량</th><th>구매금액</th><th>순구매금액</th><th>구매 CVR</th></tr></thead><tbody id="termRows"></tbody></table></div></section>
</div>
<script>
const DATA=__DATA_JSON__; const fmtInt=v=>Number(v||0).toLocaleString('ko-KR'); const fmtKrw=v=>'₩'+Number(v||0).toLocaleString('ko-KR'); const fmtPct=v=>`${(Number(v||0)*100).toFixed(1)}%`; let currentView='daily'; let chart=null;
function groupWeekly(rows){const map=new Map();rows.forEach(r=>{const d=new Date(r.date+'T00:00:00');const day=d.getDay();const monday=new Date(d);monday.setDate(d.getDate()-((day+6)%7));const key=monday.toISOString().slice(0,10);if(!map.has(key))map.set(key,{date:key,searches:0,orders:0,purchase_qty:0,erp_revenue:0,net_erp_revenue:0});const x=map.get(key);x.searches+=Number(r.searches||0);x.orders+=Number(r.orders||0);x.purchase_qty+=Number(r.purchase_qty||0);x.erp_revenue+=Number(r.erp_revenue||0);x.net_erp_revenue+=Number(r.net_erp_revenue||0);});return Array.from(map.values()).sort((a,b)=>a.date.localeCompare(b.date));}
function getQuery(){return(document.getElementById('keywordFilterTop').value||'').trim().toLowerCase();} function filteredTerms(){const q=getQuery();const rows=DATA.top_terms||[];return q?rows.filter(r=>String(r.search_term||'').toLowerCase().includes(q)):rows;} function trendRows(){const q=getQuery();let rows=DATA.daily||[];if(q&&DATA.raw_daily_by_term){rows=DATA.raw_daily_by_term.filter(r=>String(r.search_term||'').toLowerCase().includes(q));}return currentView==='week'?groupWeekly(rows):rows;}
function filteredTotals(){const q=getQuery();if(!q)return DATA.totals||{};const rows=filteredTerms();const t={searches:0,search_sessions:0,orders:0,purchase_qty:0,erp_revenue:0,net_erp_revenue:0};rows.forEach(r=>{t.searches+=Number(r.searches||0);t.search_sessions+=Number(r.search_sessions||0);t.orders+=Number(r.orders||0);t.purchase_qty+=Number(r.purchase_qty||0);t.erp_revenue+=Number(r.erp_revenue||0);t.net_erp_revenue+=Number(r.net_erp_revenue||0);});return t;}
function renderHeader(){const t=filteredTotals();document.getElementById('kpiSearches').textContent=fmtInt(t.searches);document.getElementById('kpiOrders').textContent=fmtInt(t.orders);document.getElementById('kpiQty').textContent=fmtInt(t.purchase_qty);document.getElementById('kpiRevenue').textContent=fmtKrw(t.erp_revenue);document.getElementById('metaText').textContent=`${DATA.meta.period_text||'-'} · ${DATA.meta.updated_at_kst||'-'}`;const all=DATA.totals||{};document.getElementById('matchText').textContent=`transaction rows ${fmtInt(all.matched_by_transaction_rows||0)} · member rows ${fmtInt(all.matched_by_member_rows||0)}`;document.getElementById('zeroNotice').style.display=Number(all.searches||0)>0&&Number(all.orders||0)===0?'block':'none';}
function renderChart(){const rows=trendRows();const labels=rows.map(r=>r.date);const qty=rows.map(r=>Number(r.purchase_qty||0));const revenue=rows.map(r=>Number(r.erp_revenue||0));const ctx=document.getElementById('mixedChart');if(chart)chart.destroy();chart=new Chart(ctx,{data:{labels,datasets:[{type:'bar',label:'구매수량',data:qty,borderWidth:0,borderRadius:8,backgroundColor:'rgba(96,165,250,.55)',yAxisID:'y'},{type:'line',label:'구매금액',data:revenue,tension:.35,borderWidth:3,pointRadius:3,borderColor:'rgba(244,63,94,.9)',backgroundColor:'rgba(244,63,94,.15)',yAxisID:'y1'}]},options:{responsive:true,maintainAspectRatio:true,interaction:{mode:'index',intersect:false},plugins:{legend:{position:'top',labels:{font:{weight:'bold'}}},tooltip:{callbacks:{label:ctx=>ctx.dataset.label==='구매금액'?`${ctx.dataset.label}: ${fmtKrw(ctx.raw)}`:`${ctx.dataset.label}: ${fmtInt(ctx.raw)}`}}},scales:{x:{grid:{display:false},ticks:{font:{weight:'bold'},maxRotation:0,autoSkip:true}},y:{beginAtZero:true,grid:{color:'rgba(226,232,240,.9)'},ticks:{callback:v=>fmtInt(v)}},y1:{beginAtZero:true,position:'right',grid:{drawOnChartArea:false},ticks:{callback:v=>fmtKrw(v)}}}}});}
function renderTable(){const rows=filteredTerms();const tbody=document.getElementById('termRows');tbody.innerHTML=rows.map((r,idx)=>`<tr><td><span class="rank">${idx+1}</span><span class="term">${r.search_term||'-'}</span></td><td>${fmtInt(r.searches)}</td><td>${fmtInt(r.search_sessions)}</td><td>${fmtInt(r.orders)}</td><td>${fmtInt(r.purchase_qty)}</td><td>${fmtKrw(r.erp_revenue)}</td><td>${fmtKrw(r.net_erp_revenue)}</td><td>${fmtPct(r.order_cvr)}</td></tr>`).join('');}
function renderAll(){renderHeader();renderChart();renderTable();try{parent.postMessage({type:'dailyDigestResize',height:document.documentElement.scrollHeight},'*');}catch(e){}} document.querySelectorAll('.period-btn[data-view]').forEach(btn=>{btn.addEventListener('click',()=>{document.querySelectorAll('.period-btn[data-view]').forEach(b=>b.classList.remove('active'));btn.classList.add('active');currentView=btn.dataset.view;renderChart();});}); document.getElementById('keywordFilterTop').addEventListener('input',()=>{renderHeader();renderChart();renderTable();}); renderAll();
</script>
</body>
</html>"""

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
    days = int(getenv("PRODUCT_KEYWORD_DAYS", getenv("SEARCH_VOLUME_DAYS", "30")))
    end_date = parse_date(getenv("PRODUCT_KEYWORD_END_DATE", getenv("SEARCH_VOLUME_END_DATE")), default_end)
    start_date = parse_date(getenv("PRODUCT_KEYWORD_START_DATE", getenv("SEARCH_VOLUME_START_DATE")), end_date - dt.timedelta(days=days - 1))
    lookback_days = int(getenv("SEARCH_TO_ORDER_LOOKBACK_DAYS", "7"))

    out_dir = Path(getenv("PRODUCT_KEYWORD_OUT_DIR", getenv("SEARCH_VOLUME_OUT_DIR", "reports/product_keyword")))
    data_dir = out_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    df = run_search_query(client, start_date, end_date, lookback_days)
    payload = make_payload(df, start_date, end_date, lookback_days)

    (data_dir / "product_keyword.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (data_dir / "meta.json").write_text(json.dumps(payload["meta"], ensure_ascii=False, indent=2), encoding="utf-8")
    (out_dir / "index.html").write_text(render_html(payload), encoding="utf-8")

    log(f"Wrote {out_dir / 'index.html'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
