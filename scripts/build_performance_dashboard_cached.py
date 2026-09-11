#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build Performance Dashboard using the repo-local daily cache."""
import os
from pathlib import Path
import pandas as pd

import build_performance_dashboard_bq as base

OUT = Path(os.getenv("OUT_DIR", "reports"))
CACHE = OUT / "performance_source_medium_daily.csv"


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


base.load = cached_load

if __name__ == "__main__":
    base.main()
