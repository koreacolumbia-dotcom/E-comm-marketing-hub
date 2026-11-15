#!/usr/bin/env python3
"""Fail closed on invalid Daily Digest artifacts before publication or email."""
import argparse
import datetime as dt
import json
import math
import pathlib
import sys

p = argparse.ArgumentParser()
p.add_argument("--date", required=True)
p.add_argument("--root", default="reports/daily_digest")
p.add_argument("--require-pdp", action="store_true")
args = p.parse_args()
root = pathlib.Path(args.root)
day = args.date
errors = []
html = root / "daily" / f"{day}.html"
png = root / "daily" / f"{day}.png"
data = root / "data" / "daily" / f"{day}.json"
for f in (html, png, data):
    if not f.is_file() or f.stat().st_size == 0:
        errors.append(f"missing/empty artifact: {f}")
if not errors:
    obj = json.loads(data.read_text(encoding="utf-8"))
    if obj.get("end_date") != day:
        errors.append(f"wrong report date: {obj.get('end_date')} != {day}")
    built = obj.get("built_at_kst", "")
    if not built:
        errors.append("missing built_at_kst")
    else:
        try:
            built_date = dt.datetime.strptime(built[:19], "%Y-%m-%d %H:%M:%S").date()
            if built_date < dt.date.fromisoformat(day):
                errors.append("report build timestamp predates reporting day")
        except ValueError:
            errors.append(f"invalid built_at_kst: {built}")
    kpis = obj.get("kpis") or {}
    total = float(kpis.get("sessions") or 0)
    channels = obj.get("channel_snapshot") or []
    channel_total = sum(float(row.get("sessions") or 0) for row in channels if str(row.get("bucket")).lower() != "total")
    if total <= 0:
        errors.append("overall sessions is zero")
    elif abs(channel_total - total) / total > 0.01:
        errors.append(f"channel sum {channel_total:,.0f} vs overall {total:,.0f}: exceeds 1% tolerance")
    for row in channels:
        for key in ("sessions", "transactions", "purchaseRevenue"):
            v = float(row.get(key) or 0)
            if not math.isfinite(v) or v < 0:
                errors.append(f"invalid channel metric {row.get('bucket')} {key}: {v}")
    etc = next((x for x in channels if str(x.get("bucket")).lower() == "etc"), None)
    if etc is None:
        errors.append("ETC bucket missing")
    elif total > 0:
        etc_share = float(etc.get("sessions") or 0) / total
        print(f"[QA] ETC share {etc_share:.2%}; unresolved details: {len((obj.get('channel_detail_map') or {}).get('etc') or [])}")
        if etc_share >= 0.15:
            errors.append(f"ETC share {etc_share:.2%} >= 15% anomaly threshold")
    pdp = obj.get("pdp_series") or {}
    rows = pdp.get("rows") or []
    if args.require_pdp:
        if not rows:
            errors.append("PDP data absent")
        elif not pdp.get("x") or pdp["x"][-1] != dt.date.fromisoformat(day).strftime("%m/%d"):
            errors.append("PDP series does not end on report day")
        elif not any(float(r.get("views_d1") or 0) > 0 for r in rows):
            errors.append("PDP last-day views all zero: probable BigQuery export lag")
        elif any(len(r.get("ys") or []) != 7 for r in rows):
            errors.append("PDP trend must contain 7 daily values")
    if png.read_bytes()[:8] != b"\\x89PNG\\r\\n\\x1a\\n":
        errors.append("PNG header invalid")
    if "<html" not in html.read_text(encoding="utf-8").lower():
        errors.append("HTML structure invalid")
if errors:
    for e in errors:
        print(f"::error::{e}", file=sys.stderr)
    sys.exit(1)
print(f"[QA OK] {day}: artifacts, session totals, ETC, PDP validation passed")
