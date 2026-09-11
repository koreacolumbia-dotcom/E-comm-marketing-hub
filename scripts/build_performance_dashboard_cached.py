#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build Performance Dashboard using the repo-local daily cache.

The large 6-month TY/LY payload is stored separately (gzip preferred), and the
iframe shell has no render-blocking Chart.js request. This lets index.html hide
its loading overlay immediately; data and chart code hydrate afterwards.
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


def fast_render(cur, ly):
    html = original_render(cur, ly)

    ty_start = html.index("const TY=")
    ly_start = html.index("\nconst LY=", ty_start)
    constants_start = html.index("\nconst MIN_DATE=", ly_start)

    ty_json = html[ty_start + len("const TY="):ly_start].rstrip(";")
    ly_json = html[ly_start + len("\nconst LY="):constants_start].rstrip(";")
    payload = {"ty": json.loads(ty_json), "ly": json.loads(ly_json)}
    payload_text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    DATA_JSON.write_text(payload_text, encoding="utf-8")
    with gzip.open(DATA_GZ, "wt", encoding="utf-8", compresslevel=9) as f:
        f.write(payload_text)

    # Keep initial HTML light and make the iframe's load event independent of
    # Chart.js/CDN latency. The hub's full-screen loader can disappear at once.
    html = html[:ty_start] + "let TY=[];\nlet LY=[];" + html[constants_start:]
    html = html.replace(
        "<script src='https://cdn.jsdelivr.net/npm/chart.js'></script>\n",
        "",
        1,
    )
    html = html.replace(
        "<div id='grid' class='grid'></div>",
        "<div id='grid' class='grid'><div class='panel empty' id='dataLoading'>차트 데이터 준비 중...</div></div>",
        1,
    )

    # Rendering ten daily charts should be immediate rather than animated.
    html = html.replace(
        "responsive:true,maintainAspectRatio:false,interaction:",
        "responsive:true,maintainAspectRatio:false,animation:false,interaction:",
    )
    html = html.replace("tension:.2,pointRadius:1.2", "tension:.2,pointRadius:0")

    init_pos = html.rfind("render();")
    if init_pos < 0:
        raise RuntimeError("Could not find dashboard initial render() call")

    version = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    loader = f"""function loadScript(src){{
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
async function loadPayload(){{
  if('DecompressionStream' in window){{
    try{{
      const r=await fetch('performance_data.json.gz?v={version}',{{cache:'force-cache'}});
      if(!r.ok)throw new Error('HTTP '+r.status);
      const stream=r.body.pipeThrough(new DecompressionStream('gzip'));
      return JSON.parse(await new Response(stream).text());
    }}catch(err){{console.warn('gzip payload fallback',err);}}
  }}
  const r=await fetch('performance_data.json?v={version}',{{cache:'force-cache'}});
  if(!r.ok)throw new Error('HTTP '+r.status);
  return r.json();
}}
function startPerformanceDashboard(){{
  Promise.all([loadPayload(),loadChartJs()])
    .then(([d])=>{{TY=d.ty||[];LY=d.ly||[];render();}})
    .catch(err=>{{
      console.error('Performance dashboard load failed',err);
      grid.innerHTML='<div class=\"panel empty\">차트 로딩 실패 · 새로고침 후 다시 시도해주세요.</div>';
    }});
}}
// Start only after the iframe itself has fully loaded. This means the parent
// hub no longer waits on Chart.js or six-month data before hiding its loader.
if(document.readyState==='complete')setTimeout(startPerformanceDashboard,0);
else window.addEventListener('load',()=>setTimeout(startPerformanceDashboard,0),{{once:true}});"""
    html = html[:init_pos] + loader + html[init_pos + len("render();"):]

    print(
        f"Fast shell: {len(payload['ty']):,} TY + {len(payload['ly']):,} LY rows "
        f"-> {DATA_JSON.name} / {DATA_GZ.name}; Chart.js deferred"
    )
    return html


original_render = base.render
base.load = cached_load
base.render = fast_render

if __name__ == "__main__":
    base.main()
