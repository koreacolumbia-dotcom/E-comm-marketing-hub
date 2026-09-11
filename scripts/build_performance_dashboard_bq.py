#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build Performance Dashboard from GA4 BigQuery only."""
import os, json
from pathlib import Path
import numpy as np
import pandas as pd
from google.cloud import bigquery

OUT = Path(os.getenv("OUT_DIR", "reports"))
OUT.mkdir(parents=True, exist_ok=True)
PROJECT = os.getenv("GCP_PROJECT", "columbia-ga4")
DATASET = os.getenv("GA4_DATASET", "analytics_358593394")
TODAY = pd.Timestamp.today().normalize()
END = pd.Timestamp(os.getenv("REPORT_END_DATE", (TODAY - pd.Timedelta(days=1)).strftime("%Y-%m-%d")))
START = pd.Timestamp(os.getenv("REPORT_START_DATE", (END - pd.Timedelta(days=30)).strftime("%Y-%m-%d")))
LY_START, LY_END = START - pd.DateOffset(years=1), END - pd.DateOffset(years=1)


def query(a, b):
    a, b = a.strftime("%Y%m%d"), b.strftime("%Y%m%d")
    return f"""
    WITH base AS (
      SELECT
        PARSE_DATE('%Y%m%d', event_date) AS event_date,
        TIMESTAMP_MICROS(event_timestamp) AS event_ts,
        user_pseudo_id,
        event_name,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_id') AS ga_session_id,
        COALESCE(
          session_traffic_source_last_click.manual_campaign.source,
          collected_traffic_source.manual_source,
          traffic_source.source,
          '(direct)'
        ) AS source,
        COALESCE(
          session_traffic_source_last_click.manual_campaign.medium,
          collected_traffic_source.manual_medium,
          traffic_source.medium,
          '(none)'
        ) AS medium,
        COALESCE(
          session_traffic_source_last_click.manual_campaign.campaign_name,
          collected_traffic_source.manual_campaign_name,
          '(not set)'
        ) AS campaign,
        ecommerce.transaction_id AS transaction_id,
        COALESCE(ecommerce.purchase_revenue, 0) AS revenue
      FROM `{PROJECT}.{DATASET}.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '{a}' AND '{b}'
    ), sessions AS (
      SELECT
        event_date,
        CONCAT(user_pseudo_id, '-', CAST(ga_session_id AS STRING)) AS session_key,
        ARRAY_AGG(source IGNORE NULLS ORDER BY event_ts LIMIT 1)[SAFE_OFFSET(0)] AS source,
        ARRAY_AGG(medium IGNORE NULLS ORDER BY event_ts LIMIT 1)[SAFE_OFFSET(0)] AS medium,
        ARRAY_AGG(campaign IGNORE NULLS ORDER BY event_ts LIMIT 1)[SAFE_OFFSET(0)] AS campaign,
        COUNT(DISTINCT IF(event_name='purchase', transaction_id, NULL)) AS purchases,
        SUM(IF(event_name='purchase', revenue, 0)) AS revenue
      FROM base
      WHERE ga_session_id IS NOT NULL
      GROUP BY 1,2
    )
    SELECT
      event_date, source, medium, campaign,
      COUNT(DISTINCT session_key) AS sessions,
      SUM(purchases) AS purchases,
      SUM(revenue) AS revenue
    FROM sessions
    GROUP BY 1,2,3,4
    """


def load(a, b):
    client = bigquery.Client(project=PROJECT)
    df = client.query(query(a, b)).to_dataframe()
    df["event_date"] = pd.to_datetime(df["event_date"])
    for c in ["sessions", "purchases", "revenue"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    df["source"] = df["source"].fillna("(direct)")
    df["medium"] = df["medium"].fillna("(none)")
    df["source_medium"] = df["source"] + " / " + df["medium"]
    return df


def summarize(cur, ly):
    dims = ["source_medium", "source", "medium"]
    c = cur.groupby(dims, dropna=False).agg(
        sessions_ty=("sessions", "sum"),
        purchases_ty=("purchases", "sum"),
        revenue_ty=("revenue", "sum")
    ).reset_index()
    l = ly.groupby(dims, dropna=False).agg(
        sessions_ly=("sessions", "sum"),
        purchases_ly=("purchases", "sum"),
        revenue_ly=("revenue", "sum")
    ).reset_index()
    x = c.merge(l, on=dims, how="outer").fillna(0)
    x = x[(x["source"].str.lower() != "(not set)") & (x["medium"].str.lower() != "(not set)")]
    x["cvr_ty"] = np.where(x.sessions_ty > 0, x.purchases_ty / x.sessions_ty, 0)
    x["cvr_ly"] = np.where(x.sessions_ly > 0, x.purchases_ly / x.sessions_ly, 0)
    for m in ["sessions", "purchases", "revenue"]:
        x[f"{m}_yoy"] = np.where(x[f"{m}_ly"] != 0, x[f"{m}_ty"] / x[f"{m}_ly"] - 1, np.nan)
    x["cvr_yoy_pp"] = (x.cvr_ty - x.cvr_ly) * 100
    return x.sort_values("revenue_ty", ascending=False)


def total_metrics(df):
    sessions = float(df.sessions.sum())
    purchases = float(df.purchases.sum())
    revenue = float(df.revenue.sum())
    cvr = purchases / sessions if sessions else 0
    return {"sessions": sessions, "purchases": purchases, "revenue": revenue, "cvr": cvr}


def pct(v):
    return "-" if pd.isna(v) else f"{v*100:+.1f}%"


def krw(v):
    return f"₩{v:,.0f}"


def num(v):
    return f"{v:,.0f}"


def make_trend_payload(cur, ly, top10):
    ty_dates = pd.date_range(START, END, freq="D")
    ly_dates = pd.date_range(LY_START, LY_END, freq="D")
    labels = [d.strftime("%m/%d") for d in ty_dates]
    payload = []
    for _, r in top10.iterrows():
        sm = r.source_medium
        c = cur[cur.source_medium == sm].groupby("event_date")[["sessions", "purchases", "revenue"]].sum()
        l = ly[ly.source_medium == sm].groupby("event_date")[["sessions", "purchases", "revenue"]].sum()
        item = {
            "label": sm,
            "labels": labels,
            "ty": {},
            "ly": {},
            "totals": {
                "revenue_ty": float(r.revenue_ty), "revenue_ly": float(r.revenue_ly), "revenue_yoy": None if pd.isna(r.revenue_yoy) else float(r.revenue_yoy),
                "sessions_ty": float(r.sessions_ty), "sessions_ly": float(r.sessions_ly), "sessions_yoy": None if pd.isna(r.sessions_yoy) else float(r.sessions_yoy),
                "purchases_ty": float(r.purchases_ty), "purchases_ly": float(r.purchases_ly), "purchases_yoy": None if pd.isna(r.purchases_yoy) else float(r.purchases_yoy),
                "cvr_ty": float(r.cvr_ty), "cvr_ly": float(r.cvr_ly), "cvr_yoy_pp": float(r.cvr_yoy_pp)
            }
        }
        for metric in ["revenue", "sessions", "purchases"]:
            item["ty"][metric] = [round(float(c.loc[d, metric]), 2) if d in c.index else 0 for d in ty_dates]
            item["ly"][metric] = [round(float(l.loc[d, metric]), 2) if d in l.index else 0 for d in ly_dates]
        payload.append(item)
    return payload


def render(summary, cur, ly):
    top10 = summary.head(10).copy()
    cur_t, ly_t = total_metrics(cur), total_metrics(ly)
    def yoy(key):
        a, b = cur_t[key], ly_t[key]
        return None if b == 0 else a / b - 1

    trends = make_trend_payload(cur, ly, top10)
    cards = []
    for i, r in enumerate(trends):
        t = r["totals"]
        cards.append(f"""
        <section class='panel source-panel'>
          <div class='head'>
            <div><div class='rank'>#{i+1}</div><h2>{r['label']}</h2></div>
            <div class='mini'>Revenue {krw(t['revenue_ty'])} · YoY {pct(t['revenue_yoy'])}</div>
          </div>
          <div class='stats'>
            <span>Sessions <b>{num(t['sessions_ty'])}</b> <em>{pct(t['sessions_yoy'])}</em></span>
            <span>Purchases <b>{num(t['purchases_ty'])}</b> <em>{pct(t['purchases_yoy'])}</em></span>
            <span>CVR <b>{t['cvr_ty']*100:.2f}%</b> <em>{t['cvr_yoy_pp']:+.2f}pp</em></span>
          </div>
          <div class='chartbox'><canvas id='chart{i}'></canvas></div>
        </section>
        """)

    return f"""<!doctype html>
<html lang='ko'>
<head>
<meta charset='utf-8'>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<title>Performance Dashboard</title>
<script src='https://cdn.jsdelivr.net/npm/chart.js'></script>
<style>
:root{{--bg:#070b14;--panel:#0d1422;--panel2:#111a2b;--line:#243044;--text:#eef4ff;--muted:#91a0b8}}
*{{box-sizing:border-box}} body{{margin:0;background:var(--bg);color:var(--text);font-family:Inter,Arial,'Noto Sans KR',sans-serif}}
.wrap{{max-width:1500px;margin:auto;padding:28px}} h1{{margin:0 0 4px;font-size:28px}} .sub{{color:var(--muted);font-size:13px;margin-bottom:18px}}
.toolbar{{display:flex;justify-content:flex-end;margin-bottom:16px}} select{{background:#0a1120;color:var(--text);border:1px solid var(--line);border-radius:10px;padding:8px 10px}}
.cards{{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:18px}} .card,.panel{{background:linear-gradient(180deg,var(--panel2),var(--panel));border:1px solid var(--line);border-radius:16px;padding:18px}}
.k{{color:var(--muted);font-size:12px}} .v{{font-size:25px;font-weight:800;margin-top:8px}} .yoy{{font-size:12px;margin-top:5px;color:var(--muted)}}
.grid{{display:grid;grid-template-columns:1fr 1fr;gap:16px}} .head{{display:flex;justify-content:space-between;align-items:flex-start;gap:12px;margin-bottom:8px}} .head h2{{font-size:16px;margin:2px 0 0}} .rank{{color:var(--muted);font-size:11px}} .mini{{color:var(--muted);font-size:12px;text-align:right}}
.stats{{display:flex;gap:18px;flex-wrap:wrap;color:var(--muted);font-size:11px;margin-bottom:10px}} .stats b{{color:var(--text);margin-left:4px}} .stats em{{font-style:normal;margin-left:4px;color:var(--muted)}}
.chartbox{{height:260px}} .note{{color:var(--muted);font-size:11px;margin-top:16px}}
@media(max-width:1000px){{.grid{{grid-template-columns:1fr}}.cards{{grid-template-columns:1fr 1fr}}.wrap{{padding:16px}}.chartbox{{height:240px}}}}
</style>
</head>
<body><div class='wrap'>
<h1>Performance Dashboard</h1>
<div class='sub'>GA4 BigQuery only · Revenue 기준 상위 10개 Source / Medium · {START.date()} ~ {END.date()} vs 전년 동기간</div>
<div class='cards'>
<div class='card'><div class='k'>Revenue</div><div class='v'>{krw(cur_t['revenue'])}</div><div class='yoy'>YoY {pct(yoy('revenue'))}</div></div>
<div class='card'><div class='k'>Sessions</div><div class='v'>{num(cur_t['sessions'])}</div><div class='yoy'>YoY {pct(yoy('sessions'))}</div></div>
<div class='card'><div class='k'>Purchases</div><div class='v'>{num(cur_t['purchases'])}</div><div class='yoy'>YoY {pct(yoy('purchases'))}</div></div>
<div class='card'><div class='k'>CVR</div><div class='v'>{cur_t['cvr']*100:.2f}%</div><div class='yoy'>YoY {(cur_t['cvr']-ly_t['cvr'])*100:+.2f}pp</div></div>
</div>
<div class='toolbar'><select id='metric'><option value='revenue'>Revenue trend</option><option value='sessions'>Sessions trend</option><option value='purchases'>Purchases trend</option></select></div>
<div class='grid'>{''.join(cards)}</div>
<div class='note'>(not set) Source 또는 Medium은 Top 10 선정에서 제외. 각 그래프는 현재기간(TY)과 전년 동기간(LY)의 일별 등락을 비교.</div>
</div>
<script>
const trends={json.dumps(trends, ensure_ascii=False)};
let charts=[];
function money(v){{return '₩'+Math.round(v).toLocaleString();}}
function draw(metric){{
  charts.forEach(c=>c.destroy()); charts=[];
  trends.forEach((r,i)=>{{
    const ctx=document.getElementById('chart'+i);
    const ch=new Chart(ctx,{{type:'line',data:{{labels:r.labels,datasets:[
      {{label:'TY',data:r.ty[metric],borderWidth:2,tension:.25,pointRadius:1.5}},
      {{label:'LY',data:r.ly[metric],borderWidth:2,tension:.25,pointRadius:1.5,borderDash:[5,4]}}
    ]}},options:{{responsive:true,maintainAspectRatio:false,interaction:{{mode:'index',intersect:false}},plugins:{{legend:{{labels:{{color:'#c7d2e5',boxWidth:14}}}},tooltip:{{callbacks:{{label:(c)=>metric==='revenue'?c.dataset.label+': '+money(c.raw):c.dataset.label+': '+Math.round(c.raw).toLocaleString()}}}}}},scales:{{x:{{ticks:{{color:'#91a0b8',maxTicksLimit:8}},grid:{{color:'#172235'}}}},y:{{beginAtZero:true,ticks:{{color:'#91a0b8',callback:(v)=>metric==='revenue'?'₩'+Intl.NumberFormat('ko-KR',{{notation:'compact'}}).format(v):Intl.NumberFormat('ko-KR',{{notation:'compact'}}).format(v)}},grid:{{color:'#172235'}}}}}}}}}});
    charts.push(ch);
  }});
}}
draw('revenue'); document.getElementById('metric').addEventListener('change',e=>draw(e.target.value));
</script></body></html>"""


def main():
    print(f"[1/4] BigQuery TY {START.date()} ~ {END.date()}")
    cur = load(START, END)
    print(f"[2/4] BigQuery LY {LY_START.date()} ~ {LY_END.date()}")
    ly = load(LY_START, LY_END)
    print("[3/4] Build 10 source/medium trend charts")
    summary = summarize(cur, ly)
    html = render(summary, cur, ly)
    (OUT / "performance_dashboard.html").write_text(html, encoding="utf-8")
    with pd.ExcelWriter(OUT / "performance_export.xlsx", engine="openpyxl") as w:
        summary.to_excel(w, sheet_name="source_medium", index=False)
        cur.to_excel(w, sheet_name="ty_raw", index=False)
        ly.to_excel(w, sheet_name="ly_raw", index=False)
    print("[4/4] Done")

if __name__ == "__main__":
    main()
