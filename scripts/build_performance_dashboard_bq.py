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
    df["source_medium"] = df["source"].fillna("(direct)") + " / " + df["medium"].fillna("(none)")
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


def render(summary, cur, ly):
    top10 = summary.head(10).copy()
    cur_t, ly_t = total_metrics(cur), total_metrics(ly)
    def yoy(key):
        a, b = cur_t[key], ly_t[key]
        return None if b == 0 else a / b - 1

    chart_rows = []
    for _, r in top10.iterrows():
        chart_rows.append({
            "label": r.source_medium,
            "revenue_ty": round(float(r.revenue_ty), 2),
            "revenue_ly": round(float(r.revenue_ly), 2),
            "sessions_ty": round(float(r.sessions_ty), 2),
            "sessions_ly": round(float(r.sessions_ly), 2),
            "purchases_ty": round(float(r.purchases_ty), 2),
            "purchases_ly": round(float(r.purchases_ly), 2)
        })

    rows = "".join(
        f"<tr><td>{i+1}</td><td class='sm'>{r.source_medium}</td><td>{krw(r.revenue_ty)}</td><td>{pct(r.revenue_yoy)}</td><td>{num(r.sessions_ty)}</td><td>{pct(r.sessions_yoy)}</td><td>{num(r.purchases_ty)}</td><td>{pct(r.purchases_yoy)}</td><td>{r.cvr_ty*100:.2f}%</td><td>{r.cvr_yoy_pp:+.2f}pp</td></tr>"
        for i, (_, r) in enumerate(top10.iterrows())
    )

    return f"""<!doctype html>
<html lang='ko'>
<head>
<meta charset='utf-8'>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<title>Performance Dashboard</title>
<script src='https://cdn.jsdelivr.net/npm/chart.js'></script>
<style>
:root{{--bg:#070b14;--panel:#0d1422;--panel2:#111a2b;--line:#243044;--text:#eef4ff;--muted:#91a0b8;--accent:#4f8cff;--green:#43d39e;--red:#ff6b7a}}
*{{box-sizing:border-box}} body{{margin:0;background:var(--bg);color:var(--text);font-family:Inter,Arial,'Noto Sans KR',sans-serif}}
.wrap{{max-width:1500px;margin:auto;padding:28px}} h1{{margin:0 0 4px;font-size:28px}} .sub{{color:var(--muted);font-size:13px;margin-bottom:22px}}
.cards{{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:18px}} .card,.panel{{background:linear-gradient(180deg,var(--panel2),var(--panel));border:1px solid var(--line);border-radius:16px;padding:18px}}
.k{{color:var(--muted);font-size:12px}} .v{{font-size:25px;font-weight:800;margin-top:8px}} .yoy{{font-size:12px;margin-top:5px;color:var(--muted)}}
.panel{{margin-bottom:18px}} .head{{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:14px}} .head h2{{font-size:17px;margin:0}} select{{background:#0a1120;color:var(--text);border:1px solid var(--line);border-radius:10px;padding:8px 10px}}
.chartbox{{height:520px}} table{{width:100%;border-collapse:collapse;font-size:12px}} th,td{{padding:11px 9px;border-bottom:1px solid var(--line);text-align:right;white-space:nowrap}} th{{color:var(--muted);font-weight:600}} th:nth-child(1),td:nth-child(1),th:nth-child(2),td:nth-child(2){{text-align:left}} td.sm{{max-width:300px;overflow:hidden;text-overflow:ellipsis}}
.note{{color:var(--muted);font-size:11px;margin-top:10px}} @media(max-width:900px){{.cards{{grid-template-columns:1fr 1fr}}.wrap{{padding:16px}}.chartbox{{height:430px}}.tablewrap{{overflow:auto}}}} 
</style>
</head>
<body><div class='wrap'>
<h1>Performance Dashboard</h1>
<div class='sub'>GA4 BigQuery only · {START.date()} ~ {END.date()} vs {LY_START.date()} ~ {LY_END.date()}</div>
<div class='cards'>
<div class='card'><div class='k'>Revenue</div><div class='v'>{krw(cur_t['revenue'])}</div><div class='yoy'>YoY {pct(yoy('revenue'))}</div></div>
<div class='card'><div class='k'>Sessions</div><div class='v'>{num(cur_t['sessions'])}</div><div class='yoy'>YoY {pct(yoy('sessions'))}</div></div>
<div class='card'><div class='k'>Purchases</div><div class='v'>{num(cur_t['purchases'])}</div><div class='yoy'>YoY {pct(yoy('purchases'))}</div></div>
<div class='card'><div class='k'>CVR</div><div class='v'>{cur_t['cvr']*100:.2f}%</div><div class='yoy'>YoY {(cur_t['cvr']-ly_t['cvr'])*100:+.2f}pp</div></div>
</div>
<div class='panel'>
<div class='head'><h2>Top 10 Source / Medium</h2><select id='metric'><option value='revenue'>Revenue</option><option value='sessions'>Sessions</option><option value='purchases'>Purchases</option></select></div>
<div class='chartbox'><canvas id='top10Chart'></canvas></div>
<div class='note'>현재 기간 Revenue 기준 상위 10개 소스/매체. 드롭다운으로 Sessions / Purchases 전환 가능.</div>
</div>
<div class='panel'><div class='head'><h2>Top 10 상세</h2></div><div class='tablewrap'><table><thead><tr><th>#</th><th>Source / Medium</th><th>Revenue</th><th>Rev YoY</th><th>Sessions</th><th>Ses YoY</th><th>Purchases</th><th>Pur YoY</th><th>CVR</th><th>CVR YoY</th></tr></thead><tbody>{rows}</tbody></table></div></div>
</div>
<script>
const rows={json.dumps(chart_rows, ensure_ascii=False)};
const ctx=document.getElementById('top10Chart');
let chart;
function draw(metric){{
  const labelMap={{revenue:'Revenue',sessions:'Sessions',purchases:'Purchases'}};
  if(chart) chart.destroy();
  chart=new Chart(ctx,{{type:'bar',data:{{labels:rows.map(x=>x.label),datasets:[{{label:'TY '+labelMap[metric],data:rows.map(x=>x[metric+'_ty'])}},{{label:'LY '+labelMap[metric],data:rows.map(x=>x[metric+'_ly'])}}]}},options:{{indexAxis:'y',responsive:true,maintainAspectRatio:false,interaction:{{mode:'index',intersect:false}},plugins:{{legend:{{labels:{{color:'#c7d2e5'}}}},tooltip:{{callbacks:{{label:(c)=>metric==='revenue'?c.dataset.label+': ₩'+Math.round(c.raw).toLocaleString():c.dataset.label+': '+Math.round(c.raw).toLocaleString()}}}}}},scales:{{x:{{beginAtZero:true,ticks:{{color:'#91a0b8'}},grid:{{color:'#1a2537'}}}},y:{{ticks:{{color:'#dce6f7',autoSkip:false}},grid:{{display:false}}}}}}}}}});
}}
draw('revenue'); document.getElementById('metric').addEventListener('change',e=>draw(e.target.value));
</script></body></html>"""


def main():
    print(f"[1/4] BigQuery TY {START.date()} ~ {END.date()}")
    cur = load(START, END)
    print(f"[2/4] BigQuery LY {LY_START.date()} ~ {LY_END.date()}")
    ly = load(LY_START, LY_END)
    print("[3/4] Build source/medium summary")
    summary = summarize(cur, ly)
    html = render(summary, cur, ly)
    (OUT / "performance_dashboard.html").write_text(html, encoding="utf-8")
    with pd.ExcelWriter(OUT / "performance_export.xlsx", engine="openpyxl") as w:
        summary.to_excel(w, sheet_name="source_medium", index=False)
        cur.to_excel(w, sheet_name="ty_raw", index=False)
        ly.to_excel(w, sheet_name="ly_raw", index=False)
    print("[4/4] Done")
    print(OUT / "performance_dashboard.html")

if __name__ == "__main__":
    main()
