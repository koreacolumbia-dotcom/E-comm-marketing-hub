#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build Performance Dashboard using the repo-local daily cache.

The large 6-month TY/LY payload is written to a separate JSON file so the
iframe HTML can finish loading immediately. The browser then fetches the data
asynchronously and renders the charts without blocking the hub loading overlay.
"""
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

import build_performance_dashboard_bq as base

OUT = Path(os.getenv("OUT_DIR", "reports"))
CACHE = OUT / "performance_source_medium_daily.csv"
DATA_JSON = OUT / "performance_data.json"


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
    """Reuse the existing dashboard UI but move the large data arrays out of HTML."""
    html = original_render(cur, ly)

    ty_start = html.index("const TY=")
    ly_start = html.index("\nconst LY=", ty_start)
    constants_start = html.index("\nconst MIN_DATE=", ly_start)

    ty_json = html[ty_start + len("const TY="):ly_start].rstrip(";")
    ly_json = html[ly_start + len("\nconst LY="):constants_start].rstrip(";")
    payload = {
        "ty": json.loads(ty_json),
        "ly": json.loads(ly_json),
    }
    DATA_JSON.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )

    # Keep the initial HTML light. Data arrives after iframe load.
    html = html[:ty_start] + "let TY=[];\nlet LY=[];" + html[constants_start:]
    html = html.replace(
        "<div id='grid' class='grid'></div>",
        "<div id='grid' class='grid'><div class='panel empty' id='dataLoading'>데이터 불러오는 중...</div></div>",
        1,
    )

    # Ten charts x up to six months of daily points do not need entrance animation.
    html = html.replace(
        "responsive:true,maintainAspectRatio:false,interaction:",
        "responsive:true,maintainAspectRatio:false,animation:false,interaction:",
    )
    html = html.replace("tension:.2,pointRadius:1.2", "tension:.2,pointRadius:0")

    # Replace only the final initial render(); event-handler render calls stay intact.
    init_pos = html.rfind("render();")
    if init_pos < 0:
        raise RuntimeError("Could not find dashboard initial render() call")

    version = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    loader = f"""fetch('performance_data.json?v={version}')
  .then(r=>{{if(!r.ok)throw new Error('HTTP '+r.status);return r.json();}})
  .then(d=>{{TY=d.ty||[];LY=d.ly||[];render();}})
  .catch(err=>{{
    console.error('Performance data load failed',err);
    grid.innerHTML='<div class=\"panel empty\">데이터 로딩 실패 · 새로고침 후 다시 시도해주세요.</div>';
  }});"""
    html = html[:init_pos] + loader + html[init_pos + len("render();"):]

    print(f"Split dashboard payload: {len(payload['ty']):,} TY + {len(payload['ly']):,} LY rows -> {DATA_JSON}")
    return html


original_render = base.render
base.load = cached_load
base.render = fast_render

if __name__ == "__main__":
    base.main()
