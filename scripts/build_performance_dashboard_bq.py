#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build Performance Dashboard from GA4 BigQuery only."""
import os, json
from pathlib import Path
import pandas as pd
from google.cloud import bigquery

OUT = Path(os.getenv("OUT_DIR", "reports"))
OUT.mkdir(parents=True, exist_ok=True)
PROJECT = os.getenv("GCP_PROJECT", "columbia-ga4")
DATASET = os.getenv("GA4_DATASET", "analytics_358593394")
TODAY = pd.Timestamp.today().normalize()
END = pd.Timestamp(os.getenv("REPORT_END_DATE", (TODAY - pd.Timedelta(days=1)).strftime("%Y-%m-%d")))
DATA_START = END - pd.DateOffset(months=6) + pd.Timedelta(days=1)
LY_START = DATA_START - pd.DateOffset(years=1)
LY_END = END - pd.DateOffset(years=1)


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
    df["source"] = df["source"].fillna("(direct)").astype(str)
    df["medium"] = df["medium"].fillna("(none)").astype(str)
    df = df[(df["source"].str.lower() != "(not set)") & (df["medium"].str.lower() != "(not set)")]
    df["source_medium"] = df["source"] + " / " + df["medium"]
    return df


def daily_payload(df, period):
    g = df.groupby(["event_date", "source_medium"], as_index=False)[["sessions", "purchases", "revenue"]].sum()
    if period == "ly":
        g["display_date"] = g["event_date"] + pd.DateOffset(years=1)
    else:
        g["display_date"] = g["event_date"]
    rows = []
    for _, r in g.iterrows():
        rows.append({
            "date": r["display_date"].strftime("%Y-%m-%d"),
            "sm": r["source_medium"],
            "sessions": int(round(float(r["sessions"]))),
            "purchases": int(round(float(r["purchases"]))),
            "revenue": round(float(r["revenue"]), 2),
        })
    return rows


def render(cur, ly):
    ty_rows = daily_payload(cur, "ty")
    ly_rows = daily_payload(ly, "ly")
    min_date = DATA_START.strftime("%Y-%m-%d")
    max_date = END.strftime("%Y-%m-%d")
    default_start = max(DATA_START, END - pd.Timedelta(days=30)).strftime("%Y-%m-%d")

    return f"""<!doctype html>
<html lang='ko'>
<head>
<meta charset='utf-8'>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<title>Performance Dashboard</title>
<script src='https://cdn.jsdelivr.net/npm/chart.js'></script>
<style>
:root{{--bg:#070b14;--panel:#0d1422;--panel2:#111a2b;--line:#243044;--text:#eef4ff;--muted:#91a0b8;--accent:#4f8cff}}
*{{box-sizing:border-box}} body{{margin:0;background:var(--bg);color:var(--text);font-family:Inter,Arial,'Noto Sans KR',sans-serif}}
.wrap{{max-width:1500px;margin:auto;padding:28px}} h1{{margin:0 0 4px;font-size:28px}} .sub{{color:var(--muted);font-size:13px;margin-bottom:18px}}
.toolbar{{display:flex;gap:10px;align-items:end;flex-wrap:wrap;background:linear-gradient(180deg,var(--panel2),var(--panel));border:1px solid var(--line);border-radius:16px;padding:14px;margin-bottom:16px}}
.field{{display:flex;flex-direction:column;gap:5px}} .field label{{font-size:11px;color:var(--muted)}} select,input,button{{background:#0a1120;color:var(--text);border:1px solid var(--line);border-radius:9px;padding:8px 10px}}
button{{cursor:pointer}} button.active{{border-color:var(--accent)}} .preset{{display:flex;gap:6px;flex-wrap:wrap}}
.cards{{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:18px}} .card,.panel{{background:linear-gradient(180deg,var(--panel2),var(--panel));border:1px solid var(--line);border-radius:16px;padding:18px}}
.k{{color:var(--muted);font-size:12px}} .v{{font-size:25px;font-weight:800;margin-top:8px}} .yoy{{font-size:12px;margin-top:5px;color:var(--muted)}}
.grid{{display:grid;grid-template-columns:1fr 1fr;gap:16px}} .head{{display:flex;justify-content:space-between;align-items:flex-start;gap:12px;margin-bottom:8px}} .head h2{{font-size:16px;margin:2px 0 0}} .rank{{color:var(--muted);font-size:11px}} .mini{{color:var(--muted);font-size:12px;text-align:right}}
.stats{{display:flex;gap:18px;flex-wrap:wrap;color:var(--muted);font-size:11px;margin-bottom:10px}} .stats b{{color:var(--text);margin-left:4px}} .stats em{{font-style:normal;margin-left:4px;color:var(--muted)}}
.chartbox{{height:260px}} .note{{color:var(--muted);font-size:11px;margin-top:16px}} .empty{{padding:30px;text-align:center;color:var(--muted)}}
@media(max-width:1000px){{.grid{{grid-template-columns:1fr}}.cards{{grid-template-columns:1fr 1fr}}.wrap{{padding:16px}}.chartbox{{height:240px}}}}
</style>
</head>
<body><div class='wrap'>
<h1>Performance Dashboard</h1>
<div class='sub'>GA4 BigQuery only · 최대 최근 6개월 · 선택 기간 Revenue 기준 Top 10 Source / Medium · LY 동기간 비교</div>
<div class='toolbar'>
  <div class='field'><label>빠른 기간</label><div class='preset'><button data-days='31' class='active'>최근 31일</button><button data-months='3'>최근 3개월</button><button data-months='6'>최근 6개월</button></div></div>
  <div class='field'><label>시작일</label><input id='startDate' type='date' min='{min_date}' max='{max_date}' value='{default_start}'></div>
  <div class='field'><label>종료일</label><input id='endDate' type='date' min='{min_date}' max='{max_date}' value='{max_date}'></div>
  <div class='field'><label>지표</label><select id='metric'><option value='revenue'>Revenue</option><option value='sessions'>Sessions</option><option value='purchases'>Purchases</option></select></div>
  <button id='applyBtn'>적용</button>
</div>
<div id='periodText' class='sub'></div>
<div class='cards'>
<div class='card'><div class='k'>Revenue</div><div class='v' id='totalRevenue'>-</div><div class='yoy' id='revYoy'>-</div></div>
<div class='card'><div class='k'>Sessions</div><div class='v' id='totalSessions'>-</div><div class='yoy' id='sesYoy'>-</div></div>
<div class='card'><div class='k'>Purchases</div><div class='v' id='totalPurchases'>-</div><div class='yoy' id='purYoy'>-</div></div>
<div class='card'><div class='k'>CVR</div><div class='v' id='totalCvr'>-</div><div class='yoy' id='cvrYoy'>-</div></div>
</div>
<div id='grid' class='grid'></div>
<div class='note'>(not set) Source 또는 Medium은 제외. Top 10은 선택한 기간의 Revenue 기준으로 매번 다시 선정되며, 각 그래프는 TY와 전년 동일 날짜를 겹쳐서 보여줍니다.</div>
</div>
<script>
const TY={json.dumps(ty_rows, ensure_ascii=False)};
const LY={json.dumps(ly_rows, ensure_ascii=False)};
const MIN_DATE='{min_date}', MAX_DATE='{max_date}';
let charts=[];
const fmtN=v=>Math.round(v||0).toLocaleString();
const fmtK=v=>'₩'+Math.round(v||0).toLocaleString();
const pct=(a,b)=>b?((a/b-1)*100):null;
const pctText=v=>v===null?'YoY -':`YoY ${{v>=0?'+':''}}${{v.toFixed(1)}}%`;
const addDays=(s,n)=>{{const d=new Date(s+'T00:00:00');d.setDate(d.getDate()+n);return d.toISOString().slice(0,10)}};
const addMonths=(s,n)=>{{const d=new Date(s+'T00:00:00');d.setMonth(d.getMonth()+n);return d.toISOString().slice(0,10)}};
function inRange(r,s,e){{return r.date>=s&&r.date<=e}}
function aggregate(rows){{return rows.reduce((a,r)=>{{a.sessions+=r.sessions;a.purchases+=r.purchases;a.revenue+=r.revenue;return a}},{{sessions:0,purchases:0,revenue:0}})}}
function bySm(rows){{const m=new Map();rows.forEach(r=>{{if(!m.has(r.sm))m.set(r.sm,{{sessions:0,purchases:0,revenue:0}});const x=m.get(r.sm);x.sessions+=r.sessions;x.purchases+=r.purchases;x.revenue+=r.revenue}});return m}}
function daily(rows,sm,metric,s,e){{const m=new Map();rows.filter(r=>r.sm===sm&&inRange(r,s,e)).forEach(r=>m.set(r.date,(m.get(r.date)||0)+r[metric]));const out=[];for(let d=s;d<=e;d=addDays(d,1))out.push(m.get(d)||0);return out}}
function labels(s,e){{const out=[];for(let d=s;d<=e;d=addDays(d,1))out.push(d.slice(5).replace('-','/'));return out}}
function render(){{
  const s=document.getElementById('startDate').value,e=document.getElementById('endDate').value,metric=document.getElementById('metric').value;
  if(!s||!e||s>e||s<MIN_DATE||e>MAX_DATE){{alert(`기간은 ${{MIN_DATE}} ~ ${{MAX_DATE}} 사이로 선택해줘.`);return}}
  const ty=TY.filter(r=>inRange(r,s,e)),ly=LY.filter(r=>inRange(r,s,e));
  const t=aggregate(ty),l=aggregate(ly),tcvr=t.sessions?t.purchases/t.sessions:0,lcvr=l.sessions?l.purchases/l.sessions:0;
  totalRevenue.textContent=fmtK(t.revenue); totalSessions.textContent=fmtN(t.sessions); totalPurchases.textContent=fmtN(t.purchases); totalCvr.textContent=(tcvr*100).toFixed(2)+'%';
  revYoy.textContent=pctText(pct(t.revenue,l.revenue)); sesYoy.textContent=pctText(pct(t.sessions,l.sessions)); purYoy.textContent=pctText(pct(t.purchases,l.purchases)); cvrYoy.textContent=`YoY ${{((tcvr-lcvr)*100)>=0?'+':''}}${{((tcvr-lcvr)*100).toFixed(2)}}pp`;
  periodText.textContent=`선택 기간: ${{s}} ~ ${{e}} · 전년 동일기간 비교`;
  const tm=bySm(ty),lm=bySm(ly); const top=[...tm.entries()].sort((a,b)=>b[1].revenue-a[1].revenue).slice(0,10);
  charts.forEach(c=>c.destroy());charts=[];grid.innerHTML='';
  if(!top.length){{grid.innerHTML='<div class="panel empty">선택 기간에 데이터가 없습니다.</div>';return}}
  const labs=labels(s,e);
  top.forEach(([sm,a],i)=>{{const b=lm.get(sm)||{{sessions:0,purchases:0,revenue:0}},cvr=a.sessions?a.purchases/a.sessions:0,cvrLy=b.sessions?b.purchases/b.sessions:0;
    const sec=document.createElement('section');sec.className='panel source-panel';sec.innerHTML=`<div class='head'><div><div class='rank'>#${{i+1}}</div><h2>${{sm}}</h2></div><div class='mini'>Revenue ${{fmtK(a.revenue)}} · ${{pctText(pct(a.revenue,b.revenue))}}</div></div><div class='stats'><span>Sessions <b>${{fmtN(a.sessions)}}</b> <em>${{pctText(pct(a.sessions,b.sessions)).replace('YoY ','')}}</em></span><span>Purchases <b>${{fmtN(a.purchases)}}</b> <em>${{pctText(pct(a.purchases,b.purchases)).replace('YoY ','')}}</em></span><span>CVR <b>${{(cvr*100).toFixed(2)}}%</b> <em>${{((cvr-cvrLy)*100)>=0?'+':''}}${{((cvr-cvrLy)*100).toFixed(2)}}pp</em></span></div><div class='chartbox'><canvas id='chart${{i}}'></canvas></div>`;grid.appendChild(sec);
    const tyD=daily(TY,sm,metric,s,e),lyD=daily(LY,sm,metric,s,e);
    const ch=new Chart(document.getElementById('chart'+i),{{type:'line',data:{{labels:labs,datasets:[{{label:'TY',data:tyD,borderWidth:2,tension:.2,pointRadius:1.2}},{{label:'LY',data:lyD,borderWidth:2,tension:.2,pointRadius:1.2,borderDash:[5,4]}}]}},options:{{responsive:true,maintainAspectRatio:false,interaction:{{mode:'index',intersect:false}},plugins:{{legend:{{labels:{{color:'#c7d2e5',boxWidth:14}}}},tooltip:{{callbacks:{{label:c=>metric==='revenue'?c.dataset.label+': '+fmtK(c.raw):c.dataset.label+': '+fmtN(c.raw)}}}}}},scales:{{x:{{ticks:{{color:'#91a0b8',maxTicksLimit:9}},grid:{{color:'#172235'}}}},y:{{beginAtZero:true,ticks:{{color:'#91a0b8',callback:v=>metric==='revenue'?'₩'+Intl.NumberFormat('ko-KR',{{notation:'compact'}}).format(v):Intl.NumberFormat('ko-KR',{{notation:'compact'}}).format(v)}},grid:{{color:'#172235'}}}}}}}}}});charts.push(ch);
  }});
}}
document.querySelectorAll('.preset button').forEach(btn=>btn.addEventListener('click',()=>{{document.querySelectorAll('.preset button').forEach(x=>x.classList.remove('active'));btn.classList.add('active');const e=MAX_DATE;let s;if(btn.dataset.days)s=addDays(e,-(Number(btn.dataset.days)-1));else s=addDays(addMonths(e,-Number(btn.dataset.months)),1);if(s<MIN_DATE)s=MIN_DATE;startDate.value=s;endDate.value=e;render()}}));
applyBtn.addEventListener('click',()=>{{document.querySelectorAll('.preset button').forEach(x=>x.classList.remove('active'));render()}});metric.addEventListener('change',render);render();
</script></body></html>"""


def main():
    print(f"[1/4] BigQuery TY {DATA_START.date()} ~ {END.date()}")
    cur = load(DATA_START, END)
    print(f"[2/4] BigQuery LY {LY_START.date()} ~ {LY_END.date()}")
    ly = load(LY_START, LY_END)
    print("[3/4] Build interactive 6-month performance dashboard")
    (OUT / "performance_dashboard.html").write_text(render(cur, ly), encoding="utf-8")
    with pd.ExcelWriter(OUT / "performance_export.xlsx", engine="openpyxl") as w:
        cur.to_excel(w, sheet_name="ty_6m_raw", index=False)
        ly.to_excel(w, sheet_name="ly_6m_raw", index=False)
    print("[4/4] Done")


if __name__ == "__main__":
    main()
