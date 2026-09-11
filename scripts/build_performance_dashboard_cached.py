#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build Performance Dashboard using the repo-local daily cache.

The default dashboard ships only the latest 31 days for fast first paint.
The full six-month TY/LY payload is fetched lazily only when an older range is
requested. Chart.js is deferred and the client indexes daily series so Top 10
charts do not repeatedly scan the complete payload.
"""
import gzip
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

import build_performance_dashboard_bq as base

OUT = Path(os.getenv("OUT_DIR", "reports"))
CACHE = OUT / "performance_source_medium_daily.csv"
DATA_JSON = OUT / "performance_data.json"
DATA_GZ = OUT / "performance_data.json.gz"
DATA_RECENT_JSON = OUT / "performance_data_recent.json"
DATA_RECENT_GZ = OUT / "performance_data_recent.json.gz"


def cached_load(a, b):
    if not CACHE.exists() or CACHE.stat().st_size == 0:
        print("Cache unavailable; falling back to raw BigQuery query")
        return base.load(a, b)
    df = pd.read_csv(CACHE)
    df["event_date"] = pd.to_datetime(df["event_date"])
    for c in ["sessions", "purchases", "revenue"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    df["source"] = df["source"].fillna("(direct)").astype(str)
    df["medium"] = df["medium"].fillna("(none)").astype(str)
    df = df[(df["source"].str.lower() != "(not set)") & (df["medium"].str.lower() != "(not set)")]
    df["source_medium"] = df["source"] + " / " + df["medium"]
    mask = (df["event_date"] >= pd.Timestamp(a)) & (df["event_date"] <= pd.Timestamp(b))
    out = df.loc[mask].copy()
    print(f"Cache load {pd.Timestamp(a).date()} ~ {pd.Timestamp(b).date()}: {len(out):,} rows")
    return out


def write_payload(path_json, path_gz, payload):
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    path_json.write_text(text, encoding="utf-8")
    with gzip.open(path_gz, "wt", encoding="utf-8", compresslevel=9) as f:
        f.write(text)
    return len(text.encode("utf-8"))


def fast_render(cur, ly):
    html = original_render(cur, ly)

    ty_start = html.index("const TY=")
    ly_start = html.index("\nconst LY=", ty_start)
    constants_start = html.index("\nconst MIN_DATE=", ly_start)

    ty_json = html[ty_start + len("const TY="):ly_start].rstrip(";")
    ly_json = html[ly_start + len("\nconst LY="):constants_start].rstrip(";")
    payload = {"ty": json.loads(ty_json), "ly": json.loads(ly_json)}

    recent_start = max(base.DATA_START, base.END - pd.Timedelta(days=30)).strftime("%Y-%m-%d")
    recent_payload = {
        "ty": [r for r in payload["ty"] if r["date"] >= recent_start],
        "ly": [r for r in payload["ly"] if r["date"] >= recent_start],
    }
    full_bytes = write_payload(DATA_JSON, DATA_GZ, payload)
    recent_bytes = write_payload(DATA_RECENT_JSON, DATA_RECENT_GZ, recent_payload)

    # Keep initial HTML light and make the iframe's load event independent of
    # Chart.js/CDN latency. The hub can paint immediately while data hydrates.
    html = html[:ty_start] + "let TY=[];\nlet LY=[];" + html[constants_start:]
    html = html.replace(
        "<script src='https://cdn.jsdelivr.net/npm/chart.js'></script>\n",
        "",
        1,
    )
    html = html.replace(
        "<div id='grid' class='grid'></div>",
        "<div id='grid' class='grid'><div class='panel empty' id='dataLoading'>최근 31일 데이터 준비 중...</div></div>",
        1,
    )

    # Rendering ten charts should be immediate rather than animated.
    html = html.replace(
        "responsive:true,maintainAspectRatio:false,interaction:",
        "responsive:true,maintainAspectRatio:false,animation:false,interaction:",
    )
    html = html.replace("tension:.2,pointRadius:1.2", "tension:.2,pointRadius:0")

    init_pos = html.rfind("render();")
    if init_pos < 0:
        raise RuntimeError("Could not find dashboard initial render() call")

    version = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    loader = f"""// Fast-path shim: cache each source/medium's daily values once instead of
// filtering the complete TY/LY array again for every Top 10 chart.
const __dailyIndexCache=new WeakMap();
function __dailyIndex(rows){{
  let bySm=__dailyIndexCache.get(rows);
  if(bySm)return bySm;
  bySm=new Map();
  rows.forEach(r=>{{
    let dates=bySm.get(r.sm);
    if(!dates){{dates=new Map();bySm.set(r.sm,dates);}}
    const prev=dates.get(r.date);
    if(prev){{prev.sessions+=r.sessions;prev.purchases+=r.purchases;prev.revenue+=r.revenue;}}
    else dates.set(r.date,{{sessions:r.sessions,purchases:r.purchases,revenue:r.revenue}});
  }});
  __dailyIndexCache.set(rows,bySm);
  return bySm;
}}
daily=function(rows,sm,metric,s,e){{
  const dates=__dailyIndex(rows).get(sm),out=[];
  for(let d=s;d<=e;d=addDays(d,1))out.push(dates?.get(d)?.[metric]||0);
  return out;
}};

function loadScript(src){{
  return new Promise((resolve,reject)=>{{
    const el=document.createElement('script');
    el.src=src;el.async=true;el.onload=resolve;el.onerror=reject;
    document.head.appendChild(el);
  }});
}}
function loadChartJs(){{
  if(window.Chart)return Promise.resolve();
  return loadScript('https://cdn.jsdelivr.net/npm/chart.js')
    .catch(()=>loadScript('https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js'));
}}
async function fetchPayloadFile(stem){{
  if('DecompressionStream' in window){{
    try{{
      const r=await fetch(stem+'.json.gz?v={version}',{{cache:'force-cache'}});
      if(!r.ok)throw new Error('HTTP '+r.status);
      const stream=r.body.pipeThrough(new DecompressionStream('gzip'));
      return JSON.parse(await new Response(stream).text());
    }}catch(err){{console.warn('gzip payload fallback',stem,err);}}
  }}
  const r=await fetch(stem+'.json?v={version}',{{cache:'force-cache'}});
  if(!r.ok)throw new Error('HTTP '+r.status);
  return r.json();
}}
const RECENT_START='{recent_start}';
window.__performanceDataScope='recent';
let fullPayloadPromise=null;
function applyPayload(d,scope){{TY=d.ty||[];LY=d.ly||[];window.__performanceDataScope=scope;}}
function ensureFullPayload(){{
  if(window.__performanceDataScope==='full')return Promise.resolve();
  if(!fullPayloadPromise){{
    fullPayloadPromise=fetchPayloadFile('performance_data')
      .then(d=>applyPayload(d,'full'))
      .catch(err=>{{fullPayloadPromise=null;throw err;}});
  }}
  return fullPayloadPromise;
}}
function renderFastSummary(){{
  const s=document.getElementById('startDate').value,e=document.getElementById('endDate').value;
  const ty=TY.filter(r=>inRange(r,s,e)),ly=LY.filter(r=>inRange(r,s,e));
  const t=aggregate(ty),l=aggregate(ly),tcvr=t.sessions?t.purchases/t.sessions:0,lcvr=l.sessions?l.purchases/l.sessions:0;
  totalRevenue.textContent=fmtK(t.revenue); totalSessions.textContent=fmtN(t.sessions); totalPurchases.textContent=fmtN(t.purchases); totalCvr.textContent=(tcvr*100).toFixed(2)+'%';
  revYoy.textContent=pctText(pct(t.revenue,l.revenue)); sesYoy.textContent=pctText(pct(t.sessions,l.sessions)); purYoy.textContent=pctText(pct(t.purchases,l.purchases)); cvrYoy.textContent=`YoY ${{((tcvr-lcvr)*100)>=0?'+':''}}${{((tcvr-lcvr)*100).toFixed(2)}}pp`;
  periodText.textContent=`선택 기간: ${{s}} ~ ${{e}} · 전년 동일기간 비교`;
  const tm=bySm(ty),lm=bySm(ly); const top=[...tm.entries()].sort((a,b)=>b[1].revenue-a[1].revenue).slice(0,10);
  grid.innerHTML='';
  if(!top.length){{grid.innerHTML='<div class=\"panel empty\">선택 기간에 데이터가 없습니다.</div>';return;}}
  top.forEach(([sm,a],i)=>{{
    const b=lm.get(sm)||{{sessions:0,purchases:0,revenue:0}},cvr=a.sessions?a.purchases/a.sessions:0,cvrLy=b.sessions?b.purchases/b.sessions:0;
    const sec=document.createElement('section');sec.className='panel source-panel';
    sec.innerHTML=`<div class='head'><div><div class='rank'>#${{i+1}}</div><h2>${{sm}}</h2></div><div class='mini'>Revenue ${{fmtK(a.revenue)}} · ${{pctText(pct(a.revenue,b.revenue))}}</div></div><div class='stats'><span>Sessions <b>${{fmtN(a.sessions)}}</b> <em>${{pctText(pct(a.sessions,b.sessions)).replace('YoY ','')}}</em></span><span>Purchases <b>${{fmtN(a.purchases)}}</b> <em>${{pctText(pct(a.purchases,b.purchases)).replace('YoY ','')}}</em></span><span>CVR <b>${{(cvr*100).toFixed(2)}}%</b> <em>${{((cvr-cvrLy)*100)>=0?'+':''}}${{((cvr-cvrLy)*100).toFixed(2)}}pp</em></span></div><div class='empty' style='padding:18px 8px'>차트 불러오는 중...</div>`;
    grid.appendChild(sec);
  }});
}}
const __renderCore=render;
render=function(){{
  const s=document.getElementById('startDate').value;
  if(window.__performanceDataScope!=='full' && s<RECENT_START){{
    grid.innerHTML='<div class=\"panel empty\">6개월 데이터 불러오는 중...</div>';
    ensureFullPayload().then(()=>{{
      if(window.Chart)render();
      else renderFastSummary();
    }}).catch(err=>{{
      console.error('Full performance payload load failed',err);
      grid.innerHTML='<div class=\"panel empty\">전체 데이터 로딩 실패 · 새로고침 후 다시 시도해주세요.</div>';
    }});
    return;
  }}
  if(!window.Chart){{renderFastSummary();return;}}
  return __renderCore();
}};
async function startPerformanceDashboard(){{
  try{{
    const d=await fetchPayloadFile('performance_data_recent');
    applyPayload(d,'recent');
    renderFastSummary();
  }}catch(err){{
    console.error('Performance data load failed',err);
    grid.innerHTML='<div class=\"panel empty\">데이터 로딩 실패 · 새로고침 후 다시 시도해주세요.</div>';
    return;
  }}
  loadChartJs()
    .then(()=>render())
    .catch(err=>{{
      console.error('Chart.js load failed',err);
      document.querySelectorAll('.source-panel .empty').forEach(el=>el.textContent='차트 라이브러리 로딩 실패 · 숫자 데이터는 정상 조회 가능');
    }});
}}
// Begin data hydration immediately. The dashboard no longer waits for the
// external Chart.js CDN before showing KPI cards and Top 10 source/medium rows.
setTimeout(startPerformanceDashboard,0);"""
    html = html[:init_pos] + loader + html[init_pos + len("render();"):]

    print(
        f"Fast shell: recent {len(recent_payload['ty']):,} TY + {len(recent_payload['ly']):,} LY rows "
        f"({recent_bytes/1024:.1f} KiB JSON) first; full {len(payload['ty']):,} TY + {len(payload['ly']):,} LY rows "
        f"({full_bytes/1024:.1f} KiB JSON) lazy; Chart.js deferred"
    )
    return html


original_render = base.render
base.load = cached_load
base.render = fast_render

if __name__ == "__main__":
    base.main()
