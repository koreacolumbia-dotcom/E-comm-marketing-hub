#!/usr/bin/env python3
from __future__ import annotations

import base64
import datetime as dt
import html
import json
import math
import os
import re
from pathlib import Path
from typing import Any

import pandas as pd
from google.cloud import bigquery

ROOT = Path(__file__).resolve().parents[1]
REPORT_DIR = ROOT / "reports" / "daily_digest" / "daily"
DATA_DIR = ROOT / "reports" / "daily_digest" / "data" / "daily"
TABLE = os.getenv("DAILY_DIGEST_BQ_EVENTS_TABLE", "columbia-ga4.analytics_358593394.events_*").strip()
LOCATION = os.getenv("DAILY_DIGEST_BQ_LOCATION", "asia-northeast3").strip()

CATEGORY_MAP = {
    "OUTER": {
        "Padding/Slim Down": ["Padding/Slim Down"],
        "Mid/Heavy Down": ["Mid/Heavy Down"],
        "Interchange": ["Interchange (3 in 1)"],
        "Rain": ["Rain"],
    },
    "FLEECE": {"Fleece Pullover": ["Fleece pullover"], "Jacket": ["Jacket"]},
    "TOPS": {
        "Fleece Top": ["Fleece top"],
        "Round T-shirt": ["Round T-shirt"],
        "Polo/Zip Up": ["Polo/Zip up"],
    },
    "PANTS": {"Pants": ["Pants"]},
    "FOOTWEAR": {
        "Boots": ["Boots"],
        "Omni-Max": ["Omni-Max"],
        "Hiking": ["Hiking"],
        "Sneakers": ["Sneakers"],
    },
}


def setup_credentials() -> None:
    current = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if current and Path(current).exists():
        return
    encoded = os.getenv("GOOGLE_SA_JSON_B64", "").strip()
    if not encoded:
        raise SystemExit("[ERROR] GOOGLE_SA_JSON_B64 is missing")
    target = Path("/tmp/owned_pdp_yoy_sa.json")
    target.write_bytes(base64.b64decode(encoded))
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(target)


def safe(v: Any) -> float:
    try:
        x = float(v or 0)
        return x if math.isfinite(x) else 0.0
    except Exception:
        return 0.0


def fmt(n: Any) -> str:
    return f"{int(round(safe(n))):,}"


def dual_svg(labels: list[str], cur: list[float], py: list[float], cy: int, pyy: int) -> str:
    width, height = 360, 112
    pl, pr, pt, pb = 40, 10, 29, 23
    n = max(len(labels), 1)
    ymax = max(cur + py + [1.0]) * 1.12
    iw, ih = width - pl - pr, height - pt - pb

    def xy(i: int, value: float) -> tuple[float, float]:
        return pl + iw * i / max(n - 1, 1), pt + ih * (1 - safe(value) / ymax)

    grid, ytext = [], []
    for frac in (0.0, 0.5, 1.0):
        y = pt + ih * (1 - frac)
        grid.append(f"<line x1='{pl}' y1='{y:.1f}' x2='{width-pr}' y2='{y:.1f}' stroke='#e2e8f0'/>")
        ytext.append(f"<text x='{pl-6}' y='{y+3:.1f}' text-anchor='end' font-size='8.5' fill='#64748b'>{int(round(ymax*frac))}</text>")

    def draw(values: list[float], other: list[float], color: str, dash: str, year: int) -> str:
        pts = [xy(i, v) for i, v in enumerate(values)]
        poly = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
        dash_attr = f" stroke-dasharray='{dash}'" if dash else ""
        dots = []
        for i, ((x, y), value) in enumerate(zip(pts, values)):
            comp = safe(other[i])
            yoy = ((safe(value)-comp)/comp*100) if comp else (0 if not value else 100)
            title = html.escape(f"{labels[i]} · {year}: {fmt(value)} · 비교: {fmt(comp)} · YoY {yoy:+.1f}%")
            dots.append(f"<circle cx='{x:.1f}' cy='{y:.1f}' r='3' fill='{color}' stroke='#fff'><title>{title}</title></circle>")
        return f"<polyline fill='none' stroke='{color}' stroke-width='2.8'{dash_attr} points='{poly}'/>" + "".join(dots)

    xtext = []
    for i, label in enumerate(labels):
        x, _ = xy(i, 0)
        xtext.append(f"<text x='{x:.1f}' y='{height-5}' text-anchor='middle' font-size='8.5' fill='#64748b'>{html.escape(label)}</text>")

    cur_total, py_total = sum(cur), sum(py)
    yoy = ((cur_total-py_total)/py_total*100) if py_total else (0 if not cur_total else 100)
    yoy_color = "#059669" if yoy >= 0 else "#dc2626"
    return f"""<svg width='{width}' height='{height}' viewBox='0 0 {width} {height}' xmlns='http://www.w3.org/2000/svg' style='display:block'>
    {''.join(grid)}<line x1='{pl}' y1='{height-pb}' x2='{width-pr}' y2='{height-pb}' stroke='#cbd5e1'/>
    {draw(py,cur,'#94a3b8','6 4',pyy)}{draw(cur,py,'#0092CE','',cy)}{''.join(ytext)}{''.join(xtext)}
    <g transform='translate({pl},4)'><line x1='0' y1='7' x2='18' y2='7' stroke='#0092CE' stroke-width='3'/><text x='23' y='10' font-size='9' font-weight='700' fill='#334155'>{cy}</text>
    <line x1='64' y1='7' x2='82' y2='7' stroke='#94a3b8' stroke-width='3' stroke-dasharray='6 4'/><text x='87' y='10' font-size='9' font-weight='700' fill='#64748b'>{pyy}</text>
    <text x='142' y='10' font-size='9' font-weight='800' fill='{yoy_color}'>7D YoY {yoy:+.1f}%</text></g></svg>"""


def query(end_date: dt.date) -> list[dict[str, Any]]:
    cur_dates = [end_date - dt.timedelta(days=i) for i in range(6, -1, -1)]
    py_dates = [d - dt.timedelta(days=364) for d in cur_dates]
    cur_start, py_start, py_end = cur_dates[0], py_dates[0], py_dates[-1]
    lookup_cur = end_date - dt.timedelta(days=30)
    lookup_py = py_end - dt.timedelta(days=30)
    sql = f"""
    WITH item_lookup AS (
      SELECT items.item_id,
        ARRAY_AGG(STRUCT(
          CASE WHEN items.item_category='SALE' THEN items.item_category2 ELSE items.item_category END AS c1,
          CASE WHEN items.item_category='SALE' THEN items.item_category3 ELSE items.item_category2 END AS c2
        ) ORDER BY event_timestamp DESC LIMIT 1)[SAFE_OFFSET(0)] cat
      FROM `{TABLE}` CROSS JOIN UNNEST(items) items
      WHERE ((_TABLE_SUFFIX BETWEEN '{lookup_cur:%Y%m%d}' AND '{end_date:%Y%m%d}') OR (_TABLE_SUFFIX BETWEEN '{lookup_py:%Y%m%d}' AND '{py_end:%Y%m%d}'))
        AND items.item_id IS NOT NULL
      GROUP BY 1
    ), pdp AS (
      SELECT CASE WHEN PARSE_DATE('%Y%m%d',event_date) BETWEEN DATE '{cur_start}' AND DATE '{end_date}' THEN 'current' ELSE 'previous' END period,
        PARSE_DATE('%Y%m%d',event_date) d, UPPER(IFNULL(l.cat.c1,'')) c1, IFNULL(l.cat.c2,'') c2, COUNT(*) views
      FROM `{TABLE}` e CROSS JOIN UNNEST(e.items) items JOIN item_lookup l ON l.item_id=items.item_id
      WHERE ((_TABLE_SUFFIX BETWEEN '{cur_start:%Y%m%d}' AND '{end_date:%Y%m%d}') OR (_TABLE_SUFFIX BETWEEN '{py_start:%Y%m%d}' AND '{py_end:%Y%m%d}'))
        AND event_name='view_item' AND items.item_id IS NOT NULL
      GROUP BY 1,2,3,4
    ) SELECT * FROM pdp
    """
    frame = bigquery.Client().query(sql, location=LOCATION or None).to_dataframe()
    if frame.empty:
        return []
    frame["d"] = pd.to_datetime(frame["d"]).dt.date
    frame["views"] = pd.to_numeric(frame["views"], errors="coerce").fillna(0)
    labels = [d.strftime("%m/%d") for d in cur_dates]
    rows = []
    for c1, subs in CATEGORY_MAP.items():
        for sub, values in subs.items():
            def extract(period: str, dates: list[dt.date]) -> list[float]:
                out = []
                for d in dates:
                    mask = (frame.period == period) & (frame.d == d) & (frame.c1 == c1)
                    mask &= frame.c2.isin(values) if values else frame.c2.eq(sub)
                    out.append(float(frame.loc[mask, "views"].sum()))
                return out
            cur, py = extract("current", cur_dates), extract("previous", py_dates)
            # Only keep categories that have data in both periods.
            # This removes legacy/current-year-only cards from the visible report.
            if sum(cur) <= 0 or sum(py) <= 0:
                continue
            rows.append({"category": f"{c1} · {sub}", "cur": cur, "py": py, "labels": labels, "svg": dual_svg(labels, cur, py, end_date.year, end_date.year-1)})
    return rows


def patch(end_date: dt.date) -> None:
    report = REPORT_DIR / f"{end_date:%Y-%m-%d}.html"
    data_path = DATA_DIR / f"{end_date:%Y-%m-%d}.json"
    if not report.exists():
        raise SystemExit(f"[ERROR] report not found: {report}")
    rows = query(end_date)
    if not rows:
        raise SystemExit("[ERROR] PDP YoY query returned no comparable rows")
    cards = []
    for row in rows:
        cur, py = row["cur"], row["py"]
        cur_avg, py_avg = sum(cur)/len(cur), sum(py)/len(py)
        yoy = ((cur_avg-py_avg)/py_avg*100) if py_avg else 0
        cls = "text-emerald-600" if yoy >= 0 else "text-rose-600"
        cards.append(f"""<div class='flex flex-col gap-2 rounded-2xl border border-slate-200 bg-white/70 p-3 xl:flex-row xl:items-center xl:justify-between'>
        <div class='min-w-0 flex-1'><div class='truncate text-sm font-extrabold text-slate-900'>{html.escape(row['category'])}</div>
        <div class='text-xs text-slate-500'>{end_date.year} D1 {fmt(cur[-1])} · 7D Avg {fmt(cur_avg)} &nbsp;|&nbsp; {end_date.year-1} D1 {fmt(py[-1])} · 7D Avg {fmt(py_avg)} · <b class='{cls}'>YoY {yoy:+.1f}%</b></div></div>
        <div class='shrink-0 overflow-x-auto'>{row['svg']}</div></div>""")
    replacement = "<div class=\"mt-6 rounded-2xl border border-slate-200 bg-white/70 p-4\" data-pdp-yoy-section=\"true\"><div class=\"flex flex-wrap items-end justify-between gap-2\"><div><div class=\"text-xs font-extrabold tracking-widest text-slate-500 uppercase\">PDP View Trend (Category)</div><div class=\"mt-1 text-xs text-slate-400\">Current 7D vs prior-year comparable 7D · comparable categories only</div></div></div><div class=\"mt-3 space-y-2\">" + "".join(cards) + "</div></div>"
    text = report.read_text(encoding="utf-8")

    # Replace the entire PDP block, including any duplicated legacy single-year cards,
    # up to the next Search section. This is intentionally broader than the old regex.
    pattern = re.compile(
        r'<div class="mt-6 rounded-2xl border border-slate-200 bg-white/70 p-4"(?:\s+data-pdp-yoy-section="true")?>\s*'
        r'<div[^>]*>\s*(?:<div[^>]*>\s*)?'
        r'<div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">PDP View Trend \(Category\)</div>'
        r'.*?(?=\s*<div class="mt-6 grid grid-cols-1 gap-4 lg:grid-cols-2">\s*'
        r'<div class="report-card rounded-2xl border border-slate-200 bg-white/70 p-4">\s*'
        r'<div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Search · New</div>)',
        re.S,
    )
    new_text, count = pattern.subn(replacement + "\n\n    ", text, count=1)
    if count != 1:
        raise SystemExit("[ERROR] Full PDP category section was not found in HTML")
    report.write_text(new_text, encoding="utf-8")

    if data_path.exists():
        data = json.loads(data_path.read_text(encoding="utf-8"))
        data["pdp_series_yoy"] = {
            "current_year": end_date.year,
            "previous_year": end_date.year-1,
            "shift_days": 364,
            "comparable_only": True,
            "rows": [{"itemCategory": r["category"], "ys": r["cur"], "ys_py": r["py"]} for r in rows],
        }
        data_path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"[OK] PDP YoY patched: {report} comparable_rows={len(rows)}")


if __name__ == "__main__":
    setup_credentials()
    raw = os.getenv("YMD", "").strip()
    end = dt.date.fromisoformat(raw) if raw else dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).date()-dt.timedelta(days=1)
    patch(end)
