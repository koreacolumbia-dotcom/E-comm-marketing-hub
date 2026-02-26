#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Columbia Daily Digest — Live GA4 Data (Google Analytics Data API) + BigQuery PDP Trend
+ ✅ Compare UI (A date/period vs B date/period) — **NO extra GA/BQ cost**
+ ✅ File-based Data Cache (bundle JSON) — regenerate HTML without re-query
+ ✅ Expensive part cache (BigQuery PDP Trend) — reuses cached JSON

How it works (cost-min)
- Each report build writes a compact data bundle:
    reports/daily_digest/data/daily/YYYY-MM-DD.json
    reports/daily_digest/data/weekly/END_YYYY-MM-DD.json
- Report HTML includes a “Compare” bar.
  When you compare, the browser fetches these cached JSON files (static),
  then computes diffs client-side. ✅ No GA4 API / BigQuery calls.
- If bundle JSON exists, script can rebuild HTML from bundle without querying.

Env (new)
- DAILY_DIGEST_USE_DATA_CACHE=true|false   (default true)
- DAILY_DIGEST_WRITE_DATA_CACHE=true|false (default true)
- DAILY_DIGEST_CACHE_PDP=true|false        (default true)

Existing:
- DAILY_DIGEST_SKIP_IF_EXISTS=true|false   (default true) : if HTML exists, skip entirely

Run
  setx GOOGLE_APPLICATION_CREDENTIALS "C:\\path\\service_account.json"
  setx GA4_PROPERTY_ID "358593394"
  pip install google-analytics-data pandas python-dateutil google-cloud-bigquery
  python daily_digest_live.py

Outputs
- reports/daily_digest/index.html
- reports/daily_digest/daily/YYYY-MM-DD.html
- reports/daily_digest/weekly/END_YYYY-MM-DD.html
- reports/daily_digest/data/daily/YYYY-MM-DD.json
- reports/daily_digest/data/weekly/END_YYYY-MM-DD.json
"""

from __future__ import annotations

import os
import json
import base64
import datetime as dt
from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple, Any
from zoneinfo import ZoneInfo

import pandas as pd

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.auth import default as google_auth_default
from google.analytics.data_v1beta.types import (
    DateRange, Dimension, Metric, RunReportRequest,
    OrderBy, FilterExpression, Filter, FilterExpressionList
)

# Optional: BigQuery backend for PDP Trend
try:
    from google.cloud import bigquery  # type: ignore
except Exception:
    bigquery = None


# =========================
# Env / Config
# =========================
PROPERTY_ID = os.getenv("GA4_PROPERTY_ID", "").strip()
LOGO_PATH = os.getenv("DAILY_DIGEST_LOGO_PATH", "pngwing.com.png")

OUT_DIR = os.getenv("DAILY_DIGEST_OUT_DIR", os.path.join("reports", "daily_digest")).strip()
DATA_DIR = os.getenv("DAILY_DIGEST_DATA_DIR", os.path.join(OUT_DIR, "data")).strip()
DAYS_TO_BUILD = int(os.getenv("DAILY_DIGEST_BUILD_DAYS", "14"))

IMAGE_XLS_PATH = os.getenv("DAILY_DIGEST_IMAGE_XLS_PATH", "상품코드별 이미지.xlsx").strip()
MISSING_SKU_OUT = os.getenv("DAILY_DIGEST_MISSING_SKU_OUT", "missing_image_skus.csv")
PLACEHOLDER_IMG = os.getenv("DAILY_DIGEST_PLACEHOLDER_IMG", "").strip()

# BigQuery GA4 export wildcard table
BQ_EVENTS_TABLE = os.getenv("DAILY_DIGEST_BQ_EVENTS_TABLE", "columbia-ga4.analytics_358593394.events_*").strip()
BQ_LOCATION = os.getenv("DAILY_DIGEST_BQ_LOCATION", "asia-northeast3").strip()

SIGNUP_EVENT = os.getenv("DAILY_DIGEST_SIGNUP_EVENT", "sign_up")
LOGIN_EVENT = os.getenv("DAILY_DIGEST_LOGIN_EVENT", "login")
SEARCH_EVENT = os.getenv("DAILY_DIGEST_SEARCH_EVENT", "view_search_results")

# Rising basis: qty | views | revenue
RISING_BASIS = os.getenv("DAILY_DIGEST_RISING_BASIS", "qty").strip().lower()
if RISING_BASIS not in ("qty", "views", "revenue"):
    RISING_BASIS = "qty"

# YoY overrides (optional)
YOY_SHIFT_DAYS = os.getenv("DAILY_DIGEST_YOY_SHIFT_DAYS", "").strip()  # e.g. "364" or "365"
YOY_DAILY_DATE_OVERRIDE = os.getenv("DAILY_DIGEST_YOY_DAILY_DATE", "").strip()  # "YYYY-MM-DD"
YOY_WEEKLY_END_OVERRIDE = os.getenv("DAILY_DIGEST_YOY_WEEKLY_END", "").strip()  # "YYYY-MM-DD"

# Cost-saving: if HTML exists for same date, skip all queries and keep HTML as-is
SKIP_IF_EXISTS = os.getenv("DAILY_DIGEST_SKIP_IF_EXISTS", "true").strip().lower() in ("1", "true", "yes", "y")

# ✅ New: hub index write control (keep your own static index.html)
SKIP_HUB_WRITE = os.getenv("DAILY_DIGEST_SKIP_HUB_WRITE", "true").strip().lower() in ("1", "true", "yes", "y")

# ✅ New: data cache (bundle JSON)
USE_DATA_CACHE = os.getenv("DAILY_DIGEST_USE_DATA_CACHE", "true").strip().lower() in ("1", "true", "yes", "y")
WRITE_DATA_CACHE = os.getenv("DAILY_DIGEST_WRITE_DATA_CACHE", "true").strip().lower() in ("1", "true", "yes", "y")
CACHE_PDP = os.getenv("DAILY_DIGEST_CACHE_PDP", "true").strip().lower() in ("1", "true", "yes", "y")

CHANNEL_BUCKETS = {
    "Organic": {"Organic Search"},
    "Paid AD": {"Paid Search", "Paid Social", "Display"},
    "Owned": {"Email", "SMS", "Mobile Push Notifications", "Direct"},
    "Awareness": {"Referral", "Video", "Organic Video", "Affiliates", "Cross-network"},
    "SNS": {"Organic Social"},
}
PAID_SUBGROUPS = ["Paid Search", "Paid Social", "Display"]


# =========================
# Utilities
# =========================
def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def load_logo_base64(path: str) -> str:
    if not path or not os.path.exists(path):
        return ""
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")

def fmt_int(n) -> str:
    try:
        return f"{int(round(float(n))):,}"
    except Exception:
        return "-"

def fmt_currency_krw(n) -> str:
    try:
        return f"₩{int(round(float(n))):,}"
    except Exception:
        return "₩-"

def fmt_pct(p, digits=1) -> str:
    try:
        return f"{p*100:.{digits}f}%"
    except Exception:
        return "-"

def fmt_pp(p, digits=2) -> str:
    try:
        return f"{p*100:.{digits}f}%p"
    except Exception:
        return "-"

def pct_change(curr: float, prev: float) -> float:
    if prev == 0:
        return 0.0 if curr == 0 else 1.0
    return (curr - prev) / prev

def ymd(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")

def parse_yyyymmdd(s: str) -> dt.date:
    return dt.datetime.strptime(s, "%Y%m%d").date()

def parse_yyyy_mm_dd(s: str) -> Optional[dt.date]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return dt.datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def bucket_channel(ch: str) -> str:
    for bucket, members in CHANNEL_BUCKETS.items():
        if ch in members:
            return bucket
    return "Awareness"

def index_series(vals: List[float]) -> List[float]:
    base = vals[0] if vals and vals[0] else 1.0
    return [v / base * 100.0 for v in vals]

def write_missing_image_skus(path: str, skus: list[str]) -> None:
    if not path or not skus:
        return
    try:
        df = pd.DataFrame({"sku": sorted(set([str(s).strip() for s in skus if str(s).strip()]))})
        df.to_csv(path, index=False, encoding="utf-8-sig")
        print(f"[INFO] Wrote missing image SKUs: {path}")
    except Exception as e:
        print(f"[WARN] Could not write missing image SKUs: {type(e).__name__}: {e}")

def attach_image_urls(df: pd.DataFrame, image_map: Dict[str, str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(df, copy=True)
    out = df.copy()
    if "itemId" not in out.columns:
        return out
    out["image_url"] = out["itemId"].astype(str).str.strip().map(lambda x: image_map.get(x, ""))
    return out

def is_not_set(x: str) -> bool:
    s = (x or "").strip().lower()
    return (s == "" or s == "(not set)" or s == "not set")

def pick_yoy_same_weekday(end_date: dt.date) -> dt.date:
    for d in (364, 365, 366):
        cand = end_date - dt.timedelta(days=d)
        if cand.weekday() == end_date.weekday():
            return cand
    return end_date - dt.timedelta(days=364)

def read_json(path: str) -> Optional[dict]:
    try:
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def write_json(path: str, obj: dict) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def parse_date_list_env(name: str) -> List[dt.date]:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return []
    out: List[dt.date] = []
    for token in raw.replace(";", ",").split(","):
        s = token.strip()
        if not s:
            continue
        d = parse_yyyy_mm_dd(s)
        if d:
            out.append(d)
    return out

def clamp_recent_dates(dates: List[dt.date], max_days: int) -> List[dt.date]:
    # 안전장치: 실수로 수백일 넣었을 때 방지(원하면 지워도 됨)
    if not dates:
        return dates
    dates = sorted(set(dates))
    if len(dates) <= max_days:
        return dates
    return dates[-max_days:]

# =========================
# GA4 Filters / Report
# =========================
def ga_filter_eq(field_name: str, value: str) -> FilterExpression:
    return FilterExpression(
        filter=Filter(
            field_name=field_name,
            string_filter=Filter.StringFilter(
                value=value,
                match_type=Filter.StringFilter.MatchType.EXACT
            ),
        )
    )

def ga_filter_in(field_name: str, values: List[str]) -> FilterExpression:
    return FilterExpression(
        filter=Filter(
            field_name=field_name,
            in_list_filter=Filter.InListFilter(values=values),
        )
    )

def ga_filter_and(exprs: List[FilterExpression]) -> FilterExpression:
    return FilterExpression(and_group=FilterExpressionList(expressions=exprs))

def run_report(
    client: BetaAnalyticsDataClient,
    property_id: str,
    start_date: str,
    end_date: str,
    dimensions: List[str],
    metrics: List[str],
    dimension_filter: Optional[FilterExpression] = None,
    order_bys: Optional[List[OrderBy]] = None,
    limit: int = 10000,
) -> pd.DataFrame:
    req = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name=d) for d in dimensions],
        metrics=[Metric(name=m) for m in metrics],
        limit=limit,
    )
    if dimension_filter is not None:
        req.dimension_filter = dimension_filter
    if order_bys:
        req.order_bys = order_bys

    resp = client.run_report(req)
    rows = []
    for r in resp.rows:
        row = {}
        for i, d in enumerate(dimensions):
            row[d] = r.dimension_values[i].value
        for j, m in enumerate(metrics):
            row[m] = r.metric_values[j].value
        rows.append(row)
    return pd.DataFrame(rows)


# =========================
# SVG charts
# =========================
def combined_index_svg(
    xlabels: List[str],
    series: List[List[float]],
    colors: List[str],
    labels: List[str],
    width=820, height=240,
    pad_l=46, pad_r=16, pad_t=18, pad_b=46,
) -> str:
    n = len(xlabels)
    allv = [v for s in series for v in s]
    y_min, y_max = min(allv), max(allv)
    if y_max == y_min:
        y_max += 1
    span = y_max - y_min
    y_min2 = y_min - span * 0.08
    y_max2 = y_max + span * 0.10

    inner_w = width - pad_l - pad_r
    inner_h = height - pad_t - pad_b

    def xy(i, v):
        x = pad_l + inner_w * (i / (n - 1 if n > 1 else 1))
        y_norm = (v - y_min2) / (y_max2 - y_min2)
        y = pad_t + inner_h * (1 - y_norm)
        return x, y

    ticks = 5
    grid, ylabels_svg = [], []
    for t in range(ticks + 1):
        frac = t / ticks
        y = pad_t + inner_h * (1 - frac)
        val = y_min2 + (y_max2 - y_min2) * frac
        grid.append(f"<line x1='{pad_l}' y1='{y:.1f}' x2='{width-pad_r}' y2='{y:.1f}' stroke='#eef2ff' stroke-width='1'/>")
        ylabels_svg.append(f"<text x='{pad_l-8}' y='{y+3:.1f}' text-anchor='end' font-size='10' fill='#6b7280'>{val:.0f}</text>")

    xlabels_svg = []
    for i, lab in enumerate(xlabels):
        x = pad_l + inner_w * (i / (n - 1 if n > 1 else 1))
        xlabels_svg.append(f"<text x='{x:.1f}' y='{height-pad_b+18}' text-anchor='middle' font-size='10' fill='#6b7280'>{lab}</text>")

    axes = f"""
      <line x1='{pad_l}' y1='{pad_t}' x2='{pad_l}' y2='{height-pad_b}' stroke='#c7d2fe' stroke-width='1'/>
      <line x1='{pad_l}' y1='{height-pad_b}' x2='{width-pad_r}' y2='{height-pad_b}' stroke='#c7d2fe' stroke-width='1'/>
    """

    polys, dots = [], []
    for sidx, s in enumerate(series):
        pts = [xy(i, v) for i, v in enumerate(s)]
        poly = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
        color = colors[sidx]
        polys.append(f"<polyline fill='none' stroke='{color}' stroke-width='2.6' points='{poly}'/>")
        dots.append("".join([f"<circle cx='{x:.1f}' cy='{y:.1f}' r='3.0' fill='{color}'/>" for x, y in pts]))

    legend_items = []
    lx, ly = pad_l, 8
    for i, lab in enumerate(labels):
        legend_items.append(
            f"<g transform='translate({lx + i*160},{ly})'>"
            f"<line x1='0' y1='8' x2='18' y2='8' stroke='{colors[i]}' stroke-width='3'/>"
            f"<text x='26' y='11' font-size='11' fill='#334155' style='font-weight:600'>{lab}</text>"
            f"</g>"
        )

    return f"""
    <svg width="100%" viewBox="0 0 {width} {height}" preserveAspectRatio="none" style="display:block;">
      {''.join(grid)}
      {axes}
      {''.join(polys)}
      {''.join(dots)}
      {''.join(ylabels_svg)}
      {''.join(xlabels_svg)}
      <text x='{pad_l}' y='{height-8}' font-size='10' fill='#94a3b8'>Index (D-7 = 100)</text>
      {''.join(legend_items)}
    </svg>
    """

def spark_svg(
    xlabels: List[str],
    ys: List[float],
    width=240, height=70,
    pad_l=36, pad_r=10, pad_t=10, pad_b=22,
    stroke="#0055a5",
) -> str:
    n = len(xlabels)
    y_min, y_max = min(ys), max(ys) if ys else (0.0, 1.0)
    if y_max == y_min:
        y_max += 1
    span = y_max - y_min
    y_min2 = y_min - span * 0.12
    y_max2 = y_max + span * 0.12

    inner_w = width - pad_l - pad_r
    inner_h = height - pad_t - pad_b

    def xy(i, v):
        x = pad_l + inner_w * (i / (n - 1 if n > 1 else 1))
        y_norm = (v - y_min2) / (y_max2 - y_min2)
        y = pad_t + inner_h * (1 - y_norm)
        return x, y

    pts = [xy(i, v) for i, v in enumerate(ys)] if ys else [(pad_l, height-pad_b)]
    poly = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)

    grid = []
    for frac in [0.0, 0.5, 1.0]:
        y = pad_t + inner_h * (1 - frac)
        grid.append(f"<line x1='{pad_l}' y1='{y:.1f}' x2='{width-pad_r}' y2='{y:.1f}' stroke='#eef2fb' stroke-width='1'/>")

    axes = f"""
      <line x1='{pad_l}' y1='{pad_t}' x2='{pad_l}' y2='{height-pad_b}' stroke='#cbd5e1' stroke-width='1'/>
      <line x1='{pad_l}' y1='{height-pad_b}' x2='{width-pad_r}' y2='{height-pad_b}' stroke='#cbd5e1' stroke-width='1'/>
    """

    ylab = [
        (y_max, pad_t + 3),
        (y_min + (y_max - y_min) / 2, pad_t + inner_h / 2 + 3),
        (y_min, height - pad_b + 3),
    ]
    ylabels_svg = "".join(
        [f"<text x='{pad_l-7}' y='{yy:.1f}' text-anchor='end' font-size='9' fill='#6b7280'>{int(round(val))}</text>" for val, yy in ylab]
    )

    idxs = [0, n // 2, n - 1] if n >= 3 else list(range(n))
    xlabels_svg = []
    for i in idxs:
        x = pad_l + inner_w * (i / (n - 1 if n > 1 else 1))
        xlabels_svg.append(f"<text x='{x:.1f}' y='{height-5}' text-anchor='middle' font-size='9' fill='#6b7280'>{xlabels[i]}</text>")

    area = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
    area_poly = f"{pad_l:.1f},{height-pad_b:.1f} {area} {width-pad_r:.1f},{height-pad_b:.1f}"
    dots = "".join([f"<circle cx='{x:.1f}' cy='{y:.1f}' r='2.8' fill='{stroke}'/>" for x, y in pts])

    return f"""
    <svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg" style="display:block;">
      {''.join(grid)}
      {axes}
      <polygon points="{area_poly}" fill="{stroke}" opacity="0.08"></polygon>
      <polyline fill="none" stroke="{stroke}" stroke-width="2.4" points="{poly}"/>
      {dots}
      {ylabels_svg}
      {''.join(xlabels_svg)}
    </svg>
    """


# =========================
# Excel image map
# =========================
def load_image_map_from_excel_urls(xlsx_path: str) -> Dict[str, str]:
    # ✅ FIX: 기존 코드에 잘못 들어간 summary_payload 관련 구문 제거 (SyntaxError 원인)
    if not xlsx_path:
        print("[WARN] Image Excel path is empty.")
        return {}

    if not os.path.exists(xlsx_path):
        alt = os.path.join(os.path.dirname(os.path.abspath(__file__)), xlsx_path)
        if os.path.exists(alt):
            xlsx_path = alt
        else:
            print(f"[WARN] Image Excel not found: {xlsx_path}")
            return {}

    def norm(x) -> str:
        return str(x).strip() if x is not None else ""

    m: Dict[str, str] = {}
    try:
        raw = pd.read_excel(xlsx_path, sheet_name=0, header=None)
        if raw.empty:
            return {}

        header_row = None
        sku_idx = None
        url_idx = None

        for i in range(min(50, raw.shape[0])):
            row = raw.iloc[i].tolist()
            for j, v in enumerate(row):
                if isinstance(v, str):
                    sv = v.strip()
                    if sku_idx is None and ("상품코드" in sv or sv.lower() in ["sku", "itemid", "item_id"]):
                        sku_idx = j
                    if url_idx is None and ("이미지링크" in sv or "image_url" in sv.lower()):
                        url_idx = j
            if sku_idx is not None and url_idx is not None:
                header_row = i
                break

        if header_row is None:
            sku_idx, url_idx = 2, 4
            header_row = 0

        for r in range(header_row + 1, raw.shape[0]):
            sku = norm(raw.iat[r, sku_idx]) if sku_idx < raw.shape[1] else ""
            url = norm(raw.iat[r, url_idx]) if url_idx < raw.shape[1] else ""
            if not sku:
                continue
            if url.lower().startswith("http"):
                m[sku] = url

        if not m:
            print("[WARN] Excel image map parsed 0 rows. Check columns.")
        return m
    except Exception as e:
        print(f"[WARN] Failed to load Excel image map: {type(e).__name__}: {e}")
        return {}


# =========================
# Window logic (daily vs weekly) + YoY
# =========================
@dataclass
class DigestWindow:
    mode: str
    end_date: dt.date
    cur_start: dt.date
    cur_end: dt.date
    prev_start: dt.date
    prev_end: dt.date
    compare_label: str
    yoy_start: dt.date
    yoy_end: dt.date

def build_window(end_date: dt.date, mode: str) -> DigestWindow:
    mode = (mode or "daily").lower().strip()
    if mode not in ("daily", "weekly"):
        mode = "daily"

    if mode == "daily":
        cur_start = end_date
        cur_end = end_date
        prev_end = end_date - dt.timedelta(days=1)
        prev_start = prev_end
        compare_label = "DoD"

        yoy_override = parse_yyyy_mm_dd(YOY_DAILY_DATE_OVERRIDE)
        if yoy_override:
            yoy_start = yoy_override
            yoy_end = yoy_override
        else:
            if YOY_SHIFT_DAYS.isdigit():
                yoy_start = end_date - dt.timedelta(days=int(YOY_SHIFT_DAYS))
            else:
                yoy_start = pick_yoy_same_weekday(end_date)
            yoy_end = yoy_start

    else:
        cur_end = end_date
        cur_start = end_date - dt.timedelta(days=6)

        prev_end = end_date - dt.timedelta(days=7)
        prev_start = prev_end - dt.timedelta(days=6)
        compare_label = "WoW"

        yoy_end_override = parse_yyyy_mm_dd(YOY_WEEKLY_END_OVERRIDE)
        if yoy_end_override:
            yoy_end = yoy_end_override
        else:
            if YOY_SHIFT_DAYS.isdigit():
                yoy_end = end_date - dt.timedelta(days=int(YOY_SHIFT_DAYS))
            else:
                yoy_end = pick_yoy_same_weekday(end_date)
        yoy_start = yoy_end - dt.timedelta(days=6)

    return DigestWindow(
        mode=mode,
        end_date=end_date,
        cur_start=cur_start,
        cur_end=cur_end,
        prev_start=prev_start,
        prev_end=prev_end,
        compare_label=compare_label,
        yoy_start=yoy_start,
        yoy_end=yoy_end,
    )


# =========================
# Data fetchers (3-way)
# =========================
def get_overall_kpis(client: BetaAnalyticsDataClient, w: DigestWindow) -> Dict[str, Dict[str, float]]:
    mets = ["sessions", "transactions", "purchaseRevenue"]

    d_cur = run_report(client, PROPERTY_ID, ymd(w.cur_start), ymd(w.cur_end), [], mets)
    d_prev = run_report(client, PROPERTY_ID, ymd(w.prev_start), ymd(w.prev_end), [], mets)
    d_yoy = run_report(client, PROPERTY_ID, ymd(w.yoy_start), ymd(w.yoy_end), [], mets)

    def row_to_dict(df):
        if df.empty:
            return {"sessions": 0.0, "transactions": 0.0, "purchaseRevenue": 0.0}
        r = df.iloc[0]
        return {m: float(r.get(m, 0) or 0) for m in mets}

    cur = row_to_dict(d_cur)
    prev = row_to_dict(d_prev)
    yoy = row_to_dict(d_yoy)

    cur["cvr"] = (cur["transactions"] / cur["sessions"]) if cur["sessions"] else 0.0
    prev["cvr"] = (prev["transactions"] / prev["sessions"]) if prev["sessions"] else 0.0
    yoy["cvr"] = (yoy["transactions"] / yoy["sessions"]) if yoy["sessions"] else 0.0

    return {"current": cur, "prev": prev, "yoy": yoy}

def get_multi_event_users_3way(client: BetaAnalyticsDataClient, w: DigestWindow, event_names: List[str]) -> Dict[str, float]:
    def get_one(start: dt.date, end: dt.date) -> float:
        total = 0.0
        for ev in event_names:
            filt = ga_filter_eq("eventName", ev)
            df = run_report(client, PROPERTY_ID, ymd(start), ymd(end), [], ["totalUsers"], dimension_filter=filt)
            total += float(df.iloc[0]["totalUsers"]) if (not df.empty and "totalUsers" in df.columns) else 0.0
        return total

    return {
        "current": get_one(w.cur_start, w.cur_end),
        "prev": get_one(w.prev_start, w.prev_end),
        "yoy": get_one(w.yoy_start, w.yoy_end),
    }

def get_channel_snapshot_3way(client: BetaAnalyticsDataClient, w: DigestWindow) -> pd.DataFrame:
    dims = ["sessionDefaultChannelGroup"]
    mets = ["sessions", "transactions", "purchaseRevenue"]

    cur = run_report(client, PROPERTY_ID, ymd(w.cur_start), ymd(w.cur_end), dims, mets)
    prev = run_report(client, PROPERTY_ID, ymd(w.prev_start), ymd(w.prev_end), dims, mets)
    yoy = run_report(client, PROPERTY_ID, ymd(w.yoy_start), ymd(w.yoy_end), dims, mets)

    if cur.empty:  cur = pd.DataFrame(columns=dims + mets)
    if prev.empty: prev = pd.DataFrame(columns=dims + mets)
    if yoy.empty:  yoy = pd.DataFrame(columns=dims + mets)

    for df in (cur, prev, yoy):
        df["bucket"] = df["sessionDefaultChannelGroup"].apply(bucket_channel)
        df[mets] = df[mets].apply(pd.to_numeric, errors="coerce").fillna(0.0)

    cur_agg = cur.groupby("bucket", as_index=False)[mets].sum()
    prev_agg = prev.groupby("bucket", as_index=False)[mets].sum()
    yoy_agg = yoy.groupby("bucket", as_index=False)[mets].sum()

    buckets = ["Organic", "Paid AD", "Owned", "Awareness", "SNS"]
    base = pd.DataFrame({"bucket": buckets})

    out = (
        base.merge(cur_agg, on="bucket", how="left")
            .merge(prev_agg, on="bucket", how="left", suffixes=("", "_prev"))
            .merge(yoy_agg, on="bucket", how="left", suffixes=("", "_yoy"))
            .fillna(0.0)
    )

    out["rev_vs_prev"] = out.apply(lambda r: pct_change(float(r["purchaseRevenue"]), float(r["purchaseRevenue_prev"])), axis=1)
    out["rev_yoy"] = out.apply(lambda r: pct_change(float(r["purchaseRevenue"]), float(r["purchaseRevenue_yoy"])), axis=1)

    tot_cur_rev = float(out["purchaseRevenue"].sum())
    tot_prev_rev = float(out["purchaseRevenue_prev"].sum())
    tot_yoy_rev = float(out["purchaseRevenue_yoy"].sum())
    total_row = {
        "bucket": "Total",
        "sessions": float(out["sessions"].sum()),
        "transactions": float(out["transactions"].sum()),
        "purchaseRevenue": tot_cur_rev,
        "sessions_prev": float(out["sessions_prev"].sum()),
        "transactions_prev": float(out["transactions_prev"].sum()),
        "purchaseRevenue_prev": tot_prev_rev,
        "sessions_yoy": float(out["sessions_yoy"].sum()),
        "transactions_yoy": float(out["transactions_yoy"].sum()),
        "purchaseRevenue_yoy": tot_yoy_rev,
        "rev_vs_prev": pct_change(tot_cur_rev, tot_prev_rev),
        "rev_yoy": pct_change(tot_cur_rev, tot_yoy_rev),
    }

    out = pd.concat([out, pd.DataFrame([total_row])], ignore_index=True)
    return out[[
        "bucket",
        "sessions", "transactions", "purchaseRevenue",
        "rev_vs_prev", "rev_yoy"
    ]]

def get_paid_detail_3way(client: BetaAnalyticsDataClient, w: DigestWindow) -> pd.DataFrame:
    dims = ["sessionDefaultChannelGroup"]
    mets = ["sessions", "purchaseRevenue"]
    filt = ga_filter_in("sessionDefaultChannelGroup", PAID_SUBGROUPS)

    cur = run_report(client, PROPERTY_ID, ymd(w.cur_start), ymd(w.cur_end), dims, mets, dimension_filter=filt)
    prev = run_report(client, PROPERTY_ID, ymd(w.prev_start), ymd(w.prev_end), dims, mets, dimension_filter=filt)
    yoy = run_report(client, PROPERTY_ID, ymd(w.yoy_start), ymd(w.yoy_end), dims, mets, dimension_filter=filt)

    if cur.empty:  cur = pd.DataFrame(columns=dims + mets)
    if prev.empty: prev = pd.DataFrame(columns=dims + mets)
    if yoy.empty:  yoy = pd.DataFrame(columns=dims + mets)

    cur = cur.rename(columns={"sessionDefaultChannelGroup": "sub_channel"})
    prev = prev.rename(columns={"sessionDefaultChannelGroup": "sub_channel"})
    yoy = yoy.rename(columns={"sessionDefaultChannelGroup": "sub_channel"})

    for df in (cur, prev, yoy):
        df[mets] = df[mets].apply(pd.to_numeric, errors="coerce").fillna(0.0)

    out = (
        pd.DataFrame({"sub_channel": PAID_SUBGROUPS})
        .merge(cur, on="sub_channel", how="left")
        .merge(prev, on="sub_channel", how="left", suffixes=("", "_prev"))
        .merge(yoy, on="sub_channel", how="left", suffixes=("", "_yoy"))
        .fillna(0.0)
    )
    out["rev_vs_prev"] = out.apply(lambda r: pct_change(r["purchaseRevenue"], r["purchaseRevenue_prev"]), axis=1)
    out["rev_yoy"] = out.apply(lambda r: pct_change(r["purchaseRevenue"], r["purchaseRevenue_yoy"]), axis=1)

    total_cur_rev = float(out["purchaseRevenue"].sum())
    total_prev_rev = float(out["purchaseRevenue_prev"].sum())
    total_yoy_rev = float(out["purchaseRevenue_yoy"].sum())

    total = pd.DataFrame([{
        "sub_channel": "Total",
        "sessions": float(out["sessions"].sum()),
        "purchaseRevenue": total_cur_rev,
        "rev_vs_prev": pct_change(total_cur_rev, total_prev_rev),
        "rev_yoy": pct_change(total_cur_rev, total_yoy_rev),
    }])

    out2 = out[["sub_channel", "sessions", "purchaseRevenue", "rev_vs_prev", "rev_yoy"]]
    return pd.concat([out2, total], ignore_index=True)

def get_paid_top3(client: BetaAnalyticsDataClient, w: DigestWindow) -> pd.DataFrame:
    if w.mode != "daily":
        return pd.DataFrame(columns=["sessionSourceMedium", "sessions", "purchaseRevenue"])

    dims = ["sessionSourceMedium"]
    mets = ["sessions", "purchaseRevenue"]
    filt = ga_filter_in("sessionDefaultChannelGroup", PAID_SUBGROUPS)
    order = [OrderBy(metric=OrderBy.MetricOrderBy(metric_name="purchaseRevenue"), desc=True)]

    df = run_report(client, PROPERTY_ID, ymd(w.cur_end), ymd(w.cur_end), dims, mets, dimension_filter=filt, order_bys=order, limit=3)
    if df.empty:
        return pd.DataFrame(columns=["sessionSourceMedium", "sessions", "purchaseRevenue"])

    df["sessions"] = pd.to_numeric(df["sessions"], errors="coerce").fillna(0.0)
    df["purchaseRevenue"] = pd.to_numeric(df["purchaseRevenue"], errors="coerce").fillna(0.0)

    total = pd.DataFrame([{
        "sessionSourceMedium": "Total",
        "sessions": float(df["sessions"].sum()),
        "purchaseRevenue": float(df["purchaseRevenue"].sum()),
    }])
    return pd.concat([df, total], ignore_index=True)

def get_kpi_snapshot_table_3way(client: BetaAnalyticsDataClient, w: DigestWindow, overall: Dict[str, Dict[str, float]]) -> pd.DataFrame:
    signup = get_multi_event_users_3way(client, w, ["signup_complete", "signup"])
    cur = overall["current"]; prev = overall["prev"]; yoy = overall["yoy"]

    rows = [
        ("Sessions", cur["sessions"], prev["sessions"], yoy["sessions"], "int"),
        ("CVR", cur["cvr"], prev["cvr"], yoy["cvr"], "pct"),
        ("Revenue", cur["purchaseRevenue"], prev["purchaseRevenue"], yoy["purchaseRevenue"], "krw"),
        ("Orders", cur["transactions"], prev["transactions"], yoy["transactions"], "int"),
        ("Sign-up Users", signup["current"], signup["prev"], signup["yoy"], "int"),
    ]

    out = []
    for metric, c, p, y, kind in rows:
        if kind == "pct":
            delta_prev = (c - p)
            delta_yoy = (c - y)
            value_fmt = f"{c*100:.2f}%"
            prev_fmt = fmt_pp(delta_prev, 2)
            yoy_fmt = fmt_pp(delta_yoy, 2)
        else:
            delta_prev = pct_change(c, p)
            delta_yoy = pct_change(c, y)
            value_fmt = fmt_int(c) if kind == "int" else fmt_currency_krw(c)
            prev_fmt = f"{'+' if delta_prev>=0 else ''}{fmt_pct(delta_prev,1)}"
            yoy_fmt = f"{'+' if delta_yoy>=0 else ''}{fmt_pct(delta_yoy,1)}"

        out.append({
            "metric": metric,
            "value_fmt": value_fmt,
            "delta_prev": float(delta_prev),
            "delta_prev_fmt": prev_fmt,
            "delta_yoy": float(delta_yoy),
            "delta_yoy_fmt": yoy_fmt,
        })
    return pd.DataFrame(out)

def get_trend_view_series(client: BetaAnalyticsDataClient, w: DigestWindow) -> dict:
    end = w.cur_end
    start = end - dt.timedelta(days=6)
    df = run_report(client, PROPERTY_ID, ymd(start), ymd(end), ["date"], ["sessions","transactions","purchaseRevenue"])
    axis_dates = [start + dt.timedelta(days=i) for i in range(7)]
    x = [d.strftime("%m/%d") for d in axis_dates]

    if df.empty:
        return {"x": x, "sessions": [0.0]*7, "revenue": [0.0]*7, "cvr": [0.0]*7}

    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")
    for c in ["sessions","transactions","purchaseRevenue"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    df["cvr"] = df.apply(lambda r: (r["transactions"]/r["sessions"]) if r["sessions"] else 0.0, axis=1)

    tmp = df.set_index(df["date"].dt.date)
    s = [float(tmp.loc[d, "sessions"]) if d in tmp.index else 0.0 for d in axis_dates]
    r = [float(tmp.loc[d, "purchaseRevenue"]) if d in tmp.index else 0.0 for d in axis_dates]
    c = [float(tmp.loc[d, "cvr"]) if d in tmp.index else 0.0 for d in axis_dates]
    return {"x": x, "sessions": s, "revenue": r, "cvr": c}

def trend_svg_from_series(series: dict) -> str:
    x = series.get("x", [])
    s_raw = series.get("sessions", [])
    r_raw = series.get("revenue", [])
    c_raw = series.get("cvr", [])
    if not x or len(x) != 7:
        return combined_index_svg(["--"]*7, [[100]*7,[100]*7,[100]*7], ["#0055a5","#16a34a","#c2410c"], ["Sessions","Revenue","CVR"])
    s = index_series([float(v) for v in s_raw])
    r = index_series([float(v) for v in r_raw])
    c = index_series([float(v) for v in c_raw])
    return combined_index_svg(x, [s, r, c], ["#0055a5","#16a34a","#c2410c"], ["Sessions","Revenue","CVR"])


# =========================
# Best Sellers + trend series per SKU (for cache rebuild)
# =========================
def get_best_sellers_with_trends(client: BetaAnalyticsDataClient, w: DigestWindow, image_map: Dict[str, str]) -> Tuple[pd.DataFrame, dict]:
    start = w.cur_end if w.mode == "daily" else w.cur_start
    end = w.cur_end

    order = [OrderBy(metric=OrderBy.MetricOrderBy(metric_name="itemsPurchased"), desc=True)]
    cand = run_report(
        client, PROPERTY_ID,
        ymd(start), ymd(end),
        ["itemId", "itemName"], ["itemsPurchased"],
        order_bys=order, limit=50
    )
    if cand.empty:
        return pd.DataFrame(columns=["itemId","itemName","qty","trend_svg","image_url"]), {"x": [], "items": []}

    cand["itemsPurchased"] = pd.to_numeric(cand["itemsPurchased"], errors="coerce").fillna(0.0)
    cand["itemId"] = cand["itemId"].astype(str).str.strip()
    cand["itemName"] = cand["itemName"].astype(str).fillna("").map(lambda x: x.strip())
    cand = cand[~cand["itemName"].map(is_not_set)]
    if cand.empty:
        return pd.DataFrame(columns=["itemId","itemName","qty","trend_svg","image_url"]), {"x": [], "items": []}

    cand["image_url"] = cand["itemId"].map(lambda s: image_map.get(str(s).strip(), ""))
    with_img = cand[cand["image_url"].astype(str).str.strip() != ""].copy().sort_values("itemsPurchased", ascending=False)
    no_img = cand[cand["image_url"].astype(str).str.strip() == ""].copy().sort_values("itemsPurchased", ascending=False)
    top = pd.concat([with_img.head(5), no_img.head(max(0, 5 - len(with_img.head(5))))], ignore_index=True).head(5).copy()
    top["qty"] = top["itemsPurchased"]
    skus = [str(s).strip() for s in top["itemId"].tolist() if str(s).strip()]

    axis_dates = [w.cur_end - dt.timedelta(days=6 - i) for i in range(7)]
    xlabels = [d.strftime("%m/%d") for d in axis_dates]

    ts = pd.DataFrame()
    if skus:
        ts = run_report(
            client, PROPERTY_ID,
            ymd(w.cur_end - dt.timedelta(days=6)), ymd(w.cur_end),
            ["date","itemId"], ["itemsPurchased"],
            dimension_filter=ga_filter_in("itemId", skus),
            limit=10000
        )

    series_cache = {"x": xlabels, "items": []}

    if ts.empty:
        top["trend_svg"] = ""
        if PLACEHOLDER_IMG:
            top.loc[top["image_url"].astype(str).str.strip() == "", "image_url"] = PLACEHOLDER_IMG
        for sku in skus:
            series_cache["items"].append({"itemId": sku, "ys": [0.0]*7})
        return top[["itemId","itemName","qty","trend_svg","image_url"]], series_cache

    ts["date"] = ts["date"].apply(parse_yyyymmdd)
    ts["itemsPurchased"] = pd.to_numeric(ts["itemsPurchased"], errors="coerce").fillna(0.0)
    ts["itemId"] = ts["itemId"].astype(str).str.strip()
    ts = ts.sort_values(["itemId","date"])

    svgs = []
    for sku in skus:
        sub = ts[ts["itemId"] == sku].set_index("date")["itemsPurchased"]
        ys = [float(sub.get(d, 0.0)) for d in axis_dates]
        svgs.append(spark_svg(xlabels, ys, width=240, height=70, stroke="#0055a5"))
        series_cache["items"].append({"itemId": sku, "ys": ys})

    top["trend_svg"] = svgs

    if PLACEHOLDER_IMG:
        top.loc[top["image_url"].astype(str).str.strip() == "", "image_url"] = PLACEHOLDER_IMG

    return top[["itemId","itemName","qty","trend_svg","image_url"]], series_cache


# =========================
# Rising Products (basis selectable)
# =========================
def _get_item_views_best_effort(
    client: BetaAnalyticsDataClient,
    start: dt.date,
    end: dt.date,
    skus: List[str],
) -> pd.DataFrame:
    if not skus:
        return pd.DataFrame(columns=["itemId", "views"])

    tries: List[Tuple[str, bool]] = [
        ("itemViewEvents", False),
        ("itemsViewed", False),
        ("eventCount", True),  # with eventName=view_item
    ]
    for metric_name, use_event_filter in tries:
        try:
            v = run_report(
                client, PROPERTY_ID,
                ymd(start), ymd(end),
                ["itemId"], [metric_name],
                dimension_filter=(
                    ga_filter_and([ga_filter_in("itemId", skus), ga_filter_eq("eventName", "view_item")])
                    if use_event_filter else ga_filter_in("itemId", skus)
                ),
                limit=10000,
            )
            if v.empty:
                continue
            v["itemId"] = v["itemId"].astype(str).str.strip()
            v[metric_name] = pd.to_numeric(v[metric_name], errors="coerce").fillna(0.0)
            return v[["itemId", metric_name]].rename(columns={metric_name: "views"})
        except Exception:
            continue

    return pd.DataFrame(columns=["itemId", "views"])

def _get_item_revenue_best_effort(
    client: BetaAnalyticsDataClient,
    start: dt.date,
    end: dt.date,
    limit: int = 10000,
) -> pd.DataFrame:
    for metric_name in ("itemRevenue",):
        try:
            df = run_report(
                client, PROPERTY_ID, ymd(start), ymd(end),
                ["itemId", "itemName"], [metric_name],
                limit=limit
            )
            if df.empty:
                continue
            df["itemId"] = df["itemId"].astype(str).str.strip()
            df["itemName"] = df["itemName"].astype(str).fillna("").map(lambda x: x.strip())
            df[metric_name] = pd.to_numeric(df[metric_name], errors="coerce").fillna(0.0)
            df = df.rename(columns={metric_name: "revenue"})
            return df[["itemId", "itemName", "revenue"]]
        except Exception:
            continue
    return pd.DataFrame(columns=["itemId", "itemName", "revenue"])

def get_rising_products(client: BetaAnalyticsDataClient, w: DigestWindow, top_n: int = 5) -> pd.DataFrame:
    cur_start, cur_end = (w.cur_end, w.cur_end) if w.mode == "daily" else (w.cur_start, w.cur_end)
    prev_start, prev_end = (w.prev_end, w.prev_end) if w.mode == "daily" else (w.prev_start, w.prev_end)

    d1_qty = run_report(client, PROPERTY_ID, ymd(cur_start), ymd(cur_end), ["itemId", "itemName"], ["itemsPurchased"], limit=10000)
    d0_qty = run_report(client, PROPERTY_ID, ymd(prev_start), ymd(prev_end), ["itemId"], ["itemsPurchased"], limit=10000)

    if d1_qty.empty:
        return pd.DataFrame(columns=["itemId", "itemName", "qty", "views", "revenue", "delta", "delta_label"])

    d1_qty["itemsPurchased"] = pd.to_numeric(d1_qty["itemsPurchased"], errors="coerce").fillna(0.0)
    d1_qty["itemId"] = d1_qty["itemId"].astype(str).str.strip()
    d1_qty["itemName"] = d1_qty["itemName"].astype(str).fillna("").map(lambda x: x.strip())
    d1_qty = d1_qty[~d1_qty["itemName"].map(is_not_set)]
    if d1_qty.empty:
        return pd.DataFrame(columns=["itemId", "itemName", "qty", "views", "revenue", "delta", "delta_label"])

    if not d0_qty.empty:
        d0_qty["itemsPurchased"] = pd.to_numeric(d0_qty["itemsPurchased"], errors="coerce").fillna(0.0)
        d0_qty["itemId"] = d0_qty["itemId"].astype(str).str.strip()
    else:
        d0_qty = pd.DataFrame(columns=["itemId", "itemsPurchased"])

    qty_m = d1_qty.merge(d0_qty, on="itemId", how="left", suffixes=("_cur", "_prev")).fillna(0.0)
    qty_m["qty"] = qty_m["itemsPurchased_cur"]
    qty_m["qty_prev"] = qty_m["itemsPurchased_prev"]
    qty_m["qty_delta"] = qty_m["qty"] - qty_m["qty_prev"]

    skus = [str(x).strip() for x in qty_m["itemId"].tolist() if str(x).strip()]

    v_cur = _get_item_views_best_effort(client, cur_start, cur_end, skus)
    v_prev = _get_item_views_best_effort(client, prev_start, prev_end, skus)
    if not v_cur.empty:
        v_cur = v_cur.rename(columns={"views": "views_cur"})
    else:
        v_cur = pd.DataFrame({"itemId": skus, "views_cur": [0.0]*len(skus)})
    if not v_prev.empty:
        v_prev = v_prev.rename(columns={"views": "views_prev"})
    else:
        v_prev = pd.DataFrame({"itemId": skus, "views_prev": [0.0]*len(skus)})

    r_cur = _get_item_revenue_best_effort(client, cur_start, cur_end)
    r_prev = _get_item_revenue_best_effort(client, prev_start, prev_end)
    if not r_cur.empty:
        r_cur = r_cur[["itemId", "revenue"]].rename(columns={"revenue": "revenue_cur"})
    else:
        r_cur = pd.DataFrame({"itemId": skus, "revenue_cur": [0.0]*len(skus)})
    if not r_prev.empty:
        r_prev = r_prev[["itemId", "revenue"]].rename(columns={"revenue": "revenue_prev"})
    else:
        r_prev = pd.DataFrame({"itemId": skus, "revenue_prev": [0.0]*len(skus)})

    m = qty_m.merge(v_cur, on="itemId", how="left").merge(v_prev, on="itemId", how="left").merge(r_cur, on="itemId", how="left").merge(r_prev, on="itemId", how="left")
    for c in ("views_cur", "views_prev", "revenue_cur", "revenue_prev"):
        m[c] = pd.to_numeric(m.get(c), errors="coerce").fillna(0.0)

    m["views"] = m["views_cur"]
    m["revenue"] = m["revenue_cur"]
    m["views_delta"] = m["views_cur"] - m["views_prev"]
    m["revenue_delta"] = m["revenue_cur"] - m["revenue_prev"]

    if RISING_BASIS == "views":
        m["delta"] = m["views_delta"]
        m["delta_label"] = "Views Δ"
    elif RISING_BASIS == "revenue":
        m["delta"] = m["revenue_delta"]
        m["delta_label"] = "Revenue Δ"
    else:
        m["delta"] = m["qty_delta"]
        m["delta_label"] = "Qty Δ"

    m = m.sort_values("delta", ascending=False).head(top_n).copy()
    return m[["itemId", "itemName", "qty", "views", "revenue", "delta", "delta_label"]]


# =========================
# PDP Trend (BigQuery) + file cache
# =========================
PDP_CATEGORY_MAP = {
    "OUTER": {
        "경량패딩/슬림다운": ["Padding/Slim Down"],
        "미드/롱다운": ["Mid/Heavy Down"],
        "인터체인지": ["Interchange (3 in 1)"],
        "방수자켓": ["Rain"],
    },
    "FLEECE": {
        "플리스 풀오버": ["Fleece pullover"],
        "자켓/베스트": ["Jacket"],
    },
    "TOPS": {
        "플리스": ["Fleece top"],
        "라운드티": ["Round T-shirt"],
        "폴로티/집업": ["Polo/Zip up"],
    },
    "PANTS": {"긴바지": ["Pants"]},
    "FOOTWEAR": {
        "윈터부츠": ["Boots"],
        "옴니맥스": ["Omni-Max"],
        "등산화": ["Hiking"],
        "스니커즈": ["Sneakers"],
    },
}

def pdp_cache_path(end_date: dt.date) -> str:
    return os.path.join(OUT_DIR, "cache", "pdp", f"{ymd(end_date)}.json")

def get_category_pdp_view_trend_bq(end_date: dt.date) -> Tuple[pd.DataFrame, dict]:
    axis_dates = [end_date - dt.timedelta(days=i) for i in range(6, -1, -1)]
    xlabels = [d.strftime('%m/%d') for d in axis_dates]

    if CACHE_PDP:
        cached = read_json(pdp_cache_path(end_date))
        if cached and isinstance(cached.get("rows"), list) and cached.get("x") == xlabels:
            df = pd.DataFrame(cached["rows"])
            return df, {"x": cached.get("x", xlabels), "rows": cached["rows"]}

    if bigquery is None or not BQ_EVENTS_TABLE:
        print("[WARN] BigQuery not available; PDP Trend empty.")
        df = pd.DataFrame(columns=["itemCategory", "views_d1", "views_avg7d", "trend_svg"])
        return df, {"x": xlabels, "rows": []}

    try:
        bq = bigquery.Client()

        start_suffix = (end_date - dt.timedelta(days=6)).strftime('%Y%m%d')
        end_suffix = end_date.strftime('%Y%m%d')
        lookup_start = (end_date - dt.timedelta(days=30)).strftime('%Y%m%d')
        lookup_end = end_date.strftime('%Y%m%d')

        diag_sql = f"""
        WITH base AS (
          SELECT event_date, event_name, items.item_id AS item_id
          FROM `{BQ_EVENTS_TABLE}`
          LEFT JOIN UNNEST(items) AS items
          WHERE _TABLE_SUFFIX BETWEEN '{start_suffix}' AND '{end_suffix}'
            AND event_name = 'view_item'
        )
        SELECT
          COUNTIF(event_name='view_item') AS view_item_events,
          COUNTIF(event_name='view_item' AND item_id IS NOT NULL) AS view_item_with_itemid
        FROM base
        """
        diag = bq.query(diag_sql, location=BQ_LOCATION or None).to_dataframe()
        if not diag.empty:
            ve = int(diag.loc[0, "view_item_events"] or 0)
            vi = int(diag.loc[0, "view_item_with_itemid"] or 0)
            print(f"[DIAG] PDP Trend | view_item_events={ve:,} | view_item_with_itemid={vi:,}")
            if ve > 0 and vi == 0:
                print("[WARN] PDP Trend empty: view_item events have NO items[].item_id in BigQuery export.")

        sql = f"""
        WITH item_lookup AS (
          SELECT
            items.item_id AS item_id,
            ANY_VALUE(CASE WHEN items.item_category = 'SALE' THEN items.item_category2 ELSE items.item_category END) AS c1_norm,
            ANY_VALUE(CASE WHEN items.item_category = 'SALE' THEN items.item_category3 ELSE items.item_category2 END) AS c2_norm
          FROM `{BQ_EVENTS_TABLE}`
          CROSS JOIN UNNEST(items) AS items
          WHERE _TABLE_SUFFIX BETWEEN '{lookup_start}' AND '{lookup_end}'
            AND items.item_id IS NOT NULL
            AND (items.item_category IS NOT NULL OR items.item_category2 IS NOT NULL OR items.item_category3 IS NOT NULL)
          GROUP BY 1
        ),
        pdp AS (
          SELECT
            PARSE_DATE('%Y%m%d', event_date) AS d,
            l.c1_norm AS c1,
            l.c2_norm AS c2,
            COUNT(*) AS views
          FROM `{BQ_EVENTS_TABLE}`
          CROSS JOIN UNNEST(items) AS items
          JOIN item_lookup l ON l.item_id = items.item_id
          WHERE _TABLE_SUFFIX BETWEEN '{start_suffix}' AND '{end_suffix}'
            AND event_name = 'view_item'
            AND items.item_id IS NOT NULL
          GROUP BY 1,2,3
        )
        SELECT d, UPPER(IFNULL(c1,'')) AS c1, IFNULL(c2,'') AS c2, views
        FROM pdp
        """
        df = bq.query(sql, location=BQ_LOCATION or None).to_dataframe()
        if df.empty:
            out = pd.DataFrame(columns=["itemCategory", "views_d1", "views_avg7d", "trend_svg"])
            if CACHE_PDP:
                write_json(pdp_cache_path(end_date), {"x": xlabels, "rows": []})
            return out, {"x": xlabels, "rows": []}

        df['d'] = pd.to_datetime(df['d'], errors='coerce').dt.date
        df = df.dropna(subset=['d'])
        df['c1'] = df['c1'].astype(str).str.strip().str.upper()
        df['c2'] = df['c2'].astype(str).str.strip()
        df['views'] = pd.to_numeric(df['views'], errors="coerce").fillna(0.0)

        rows = []
        cache_rows = []
        for c1, subs in PDP_CATEGORY_MAP.items():
            for sub_label, c2_list in subs.items():
                ys = []
                for d0 in axis_dates:
                    m = (df['d'] == d0) & (df['c1'] == c1) & (df['c2'].isin(c2_list))
                    ys.append(float(df.loc[m, 'views'].sum()))

                d1 = ys[-1] if ys else 0.0
                avg7 = (sum(ys) / len(ys)) if ys else 0.0
                label = f"{c1} · {sub_label}"
                rows.append({
                    "itemCategory": label,
                    "views_d1": float(d1),
                    "views_avg7d": float(avg7),
                    "trend_svg": spark_svg(xlabels, ys, width=260, height=70, stroke="#0f766e"),
                })
                cache_rows.append({
                    "itemCategory": label,
                    "views_d1": float(d1),
                    "views_avg7d": float(avg7),
                    "ys": ys
                })

        out_df = pd.DataFrame(rows)
        if CACHE_PDP:
            write_json(pdp_cache_path(end_date), {"x": xlabels, "rows": cache_rows})
        return out_df, {"x": xlabels, "rows": cache_rows}

    except Exception as e:
        print(f"[WARN] PDP Trend BigQuery failed: {type(e).__name__}: {e}")
        out = pd.DataFrame(columns=["itemCategory", "views_d1", "views_avg7d", "trend_svg"])
        return out, {"x": xlabels, "rows": []}


# =========================
# Search Trends (count 포함)
# =========================
def get_search_trends(client: BetaAnalyticsDataClient, end_date: dt.date) -> Dict[str, pd.DataFrame]:
    lookback_start = end_date - dt.timedelta(days=13)
    df = run_report(
        client, PROPERTY_ID,
        ymd(lookback_start), ymd(end_date),
        ["date","searchTerm"], ["eventCount"],
        dimension_filter=ga_filter_eq("eventName", SEARCH_EVENT),
        limit=10000
    )
    if df.empty:
        return {
            "new": pd.DataFrame(columns=["searchTerm","count"]),
            "rising": pd.DataFrame(columns=["searchTerm","pct","count"])
        }

    df["date"] = df["date"].apply(parse_yyyymmdd)
    df["eventCount"] = pd.to_numeric(df["eventCount"], errors="coerce").fillna(0.0)

    y_df = df[df["date"] == end_date].groupby("searchTerm", as_index=False)["eventCount"].sum()
    y_df = y_df.rename(columns={"eventCount":"count"}).sort_values("count", ascending=False)

    prior_start = end_date - dt.timedelta(days=7)
    prior_df = df[(df["date"] >= prior_start) & (df["date"] <= (end_date - dt.timedelta(days=1)))]
    prior_agg = prior_df.groupby("searchTerm", as_index=False)["eventCount"].mean().rename(columns={"eventCount":"prior_avg"})

    merged = y_df.merge(prior_agg, on="searchTerm", how="left").fillna(0.0)

    new_terms = merged[merged["prior_avg"] == 0].head(3)[["searchTerm","count"]].copy()

    rising = merged[merged["prior_avg"] > 0].copy()
    rising["pct"] = (rising["count"] - rising["prior_avg"]) / rising["prior_avg"] * 100.0
    rising = rising.replace([float("inf"), -float("inf")], 0.0)
    rising = rising.sort_values("pct", ascending=False).head(3)[["searchTerm","pct","count"]]
    return {"new": new_terms, "rising": rising}


# =========================
# Bundle JSON (cache)
# =========================
def bundle_path(mode: str, end_date: dt.date) -> str:
    if mode == "weekly":
        return os.path.join(DATA_DIR, "weekly", f"END_{ymd(end_date)}.json")
    return os.path.join(DATA_DIR, "daily", f"{ymd(end_date)}.json")

def to_records(df: pd.DataFrame) -> List[dict]:
    if df is None or df.empty:
        return []
    return df.to_dict(orient="records")

def build_bundle(
    w: DigestWindow,
    overall: Dict[str, Dict[str, float]],
    signup_users: Dict[str, float],
    channel_snapshot: pd.DataFrame,
    paid_detail: pd.DataFrame,
    paid_top3: pd.DataFrame,
    kpi_snapshot: pd.DataFrame,
    trend_series: dict,
    best_sellers_df: pd.DataFrame,
    best_sellers_series: dict,
    rising_df: pd.DataFrame,
    pdp_series: dict,
    search_new: pd.DataFrame,
    search_rising: pd.DataFrame,
) -> dict:
    # ---- summary cache (for Hub range compare) ----
    cur = overall.get("current", {}) or {}
    sessions = float(cur.get("sessions", 0) or 0)
    orders = float(cur.get("transactions", 0) or 0)
    revenue = float(cur.get("purchaseRevenue", 0) or 0)
    cvr = (orders / sessions) if sessions else 0.0

    channels: Dict[str, Dict[str, float]] = {}
    if channel_snapshot is not None and (not channel_snapshot.empty):
        for r in channel_snapshot.itertuples(index=False):
            b = str(getattr(r, "bucket", "") or "")
            if not b:
                continue
            channels[b] = {
                "sessions": float(getattr(r, "sessions", 0) or 0),
                "orders": float(getattr(r, "transactions", 0) or 0),
                "revenue": float(getattr(r, "purchaseRevenue", 0) or 0),
            }

    # ✅ IMPORTANT: Hub/index.html에서 바로 쓰는 최상단 구조
    summary_payload = {
        "mode": w.mode,
        "end_date": ymd(w.end_date),
        "period": {"start": ymd(w.cur_start), "end": ymd(w.cur_end)},
        "compare": {"label": w.compare_label, "prev_start": ymd(w.prev_start), "prev_end": ymd(w.prev_end)},
        "yoy": {"start": ymd(w.yoy_start), "end": ymd(w.yoy_end)},
        "kpis": {
            "sessions": sessions,
            "orders": orders,
            "revenue": revenue,
            "cvr": cvr,
            "signups": float(signup_users.get("current", 0.0) or 0.0),
        },
        "channels": channels,
        "built_at_kst": dt.datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S"),
    }

    # ✅ meta: report compare bar presetPrev()가 prev_end를 참조하므로 포함
    meta = {
        "mode": w.mode,
        "end_date": ymd(w.end_date),
        "cur_start": ymd(w.cur_start),
        "cur_end": ymd(w.cur_end),
        "prev_start": ymd(w.prev_start),
        "prev_end": ymd(w.prev_end),
        "compare_label": w.compare_label,
        "yoy_start": ymd(w.yoy_start),
        "yoy_end": ymd(w.yoy_end),
        "rising_basis": RISING_BASIS,
    }

    # ✅ 최상단(summary_payload) + 상세 섹션들
    return {
        **summary_payload,
        "meta": meta,
        "overall": overall,
        "signup_users": signup_users,
        "channel_snapshot": to_records(channel_snapshot),
        "paid_detail": to_records(paid_detail),
        "paid_top3": to_records(paid_top3),
        "kpi_snapshot": to_records(kpi_snapshot),
        "trend_series": trend_series,
        "best_sellers": to_records(best_sellers_df.drop(columns=["trend_svg"], errors="ignore")),
        "best_sellers_series": best_sellers_series,
        "rising": to_records(rising_df),
        "pdp_series": pdp_series,
        "search_new": to_records(search_new),
        "search_rising": to_records(search_rising),
    }

def rebuild_runtime_objects_from_bundle(bundle: dict, image_map: Dict[str, str]) -> dict:
    m = bundle.get("meta", {})
    mode = (m.get("mode") or "daily").lower()
    end_date = parse_yyyy_mm_dd(m.get("end_date", "")) or dt.date.today()
    w = build_window(end_date=end_date, mode=mode)

    overall = bundle.get("overall", {"current": {}, "prev": {}, "yoy": {}})
    signup_users = bundle.get("signup_users", {"current": 0.0, "prev": 0.0, "yoy": 0.0})

    channel_snapshot = pd.DataFrame(bundle.get("channel_snapshot", []))
    paid_detail = pd.DataFrame(bundle.get("paid_detail", []))
    paid_top3 = pd.DataFrame(bundle.get("paid_top3", []))
    kpi_snapshot = pd.DataFrame(bundle.get("kpi_snapshot", []))

    trend_series = bundle.get("trend_series", {})
    trend_svg = trend_svg_from_series(trend_series)

    bs_base = pd.DataFrame(bundle.get("best_sellers", []))
    bs_series = bundle.get("best_sellers_series", {"x": [], "items": []})
    x = bs_series.get("x", [])
    items_map = {it.get("itemId"): it.get("ys", [0.0]*7) for it in bs_series.get("items", []) if it.get("itemId")}
    trend_svgs = []
    for _, r in bs_base.iterrows():
        sku = str(r.get("itemId", "")).strip()
        ys = items_map.get(sku, [0.0]*7)
        trend_svgs.append(spark_svg(x or ["--"]*7, ys, width=240, height=70, stroke="#0055a5"))
    if not bs_base.empty:
        bs_base["trend_svg"] = trend_svgs
        if PLACEHOLDER_IMG and "image_url" in bs_base.columns:
            bs_base.loc[bs_base["image_url"].astype(str).str.strip() == "", "image_url"] = PLACEHOLDER_IMG

    rising = pd.DataFrame(bundle.get("rising", []))
    rising = attach_image_urls(rising, image_map)
    if PLACEHOLDER_IMG and (not rising.empty) and ("image_url" in rising.columns):
        rising.loc[rising["image_url"].astype(str).str.strip() == "", "image_url"] = PLACEHOLDER_IMG

    pdp_series = bundle.get("pdp_series", {"x": [], "rows": []})
    pdp_rows = []
    for row in pdp_series.get("rows", []):
        ys = row.get("ys", [0.0]*7)
        pdp_rows.append({
            "itemCategory": row.get("itemCategory", ""),
            "views_d1": row.get("views_d1", 0.0),
            "views_avg7d": row.get("views_avg7d", 0.0),
            "trend_svg": spark_svg(pdp_series.get("x", ["--"]*7), ys, width=260, height=70, stroke="#0f766e")
        })
    category_pdp_trend = pd.DataFrame(pdp_rows)

    search_new = pd.DataFrame(bundle.get("search_new", []))
    search_rising = pd.DataFrame(bundle.get("search_rising", []))

    return {
        "w": w,
        "overall": overall,
        "signup_users": signup_users,
        "channel_snapshot": channel_snapshot,
        "paid_detail": paid_detail,
        "paid_top3": paid_top3,
        "kpi_snapshot": kpi_snapshot,
        "trend_svg": trend_svg,
        "best_sellers": bs_base,
        "rising": rising,
        "category_pdp_trend": category_pdp_trend,
        "search_new": search_new,
        "search_rising": search_rising,
    }


# =========================
# UI (Compare bar + modal)
# =========================
def render_page_html(
    logo_b64: str,
    w: DigestWindow,
    overall: Dict[str, Dict[str, float]],
    signup_users: Dict[str, float],
    channel_snapshot: pd.DataFrame,
    paid_detail: pd.DataFrame,
    paid_top3: pd.DataFrame,
    kpi_snapshot: pd.DataFrame,
    trend_svg: str,
    best_sellers: pd.DataFrame,
    rising: pd.DataFrame,
    category_pdp_trend: pd.DataFrame,
    search_new: pd.DataFrame,
    search_rising: pd.DataFrame,
    nav_links: Dict[str, str],
    bundle_rel_path: str,
) -> str:
    import html as _html

    cur = overall["current"]; prev = overall["prev"]; yoy = overall["yoy"]

    s_delta = pct_change(cur["sessions"], prev["sessions"])
    o_delta = pct_change(cur["transactions"], prev["transactions"])
    r_delta = pct_change(cur["purchaseRevenue"], prev["purchaseRevenue"])
    c_pp = cur["cvr"] - prev["cvr"]

    s_yoy = pct_change(cur["sessions"], yoy["sessions"])
    o_yoy = pct_change(cur["transactions"], yoy["transactions"])
    r_yoy = pct_change(cur["purchaseRevenue"], yoy["purchaseRevenue"])
    c_yoy_pp = cur["cvr"] - yoy["cvr"]

    su_cur = float(signup_users.get("current", 0.0) or 0.0)
    su_prev = float(signup_users.get("prev", 0.0) or 0.0)
    su_yoy = float(signup_users.get("yoy", 0.0) or 0.0)
    su_delta = pct_change(su_cur, su_prev)
    su_yoy_delta = pct_change(su_cur, su_yoy)

    def esc(s: Any) -> str:
        return _html.escape(str(s or ""), quote=True)

    def delta_cls(v: float) -> str:
        return "text-blue-600" if v >= 0 else "text-orange-700"

    def top_kpi_card(title: str, value: str, delta_main: str, delta_yoy_s: str, cls_main: str, cls_yoy: str) -> str:
        return f"""
        <div class="rounded-2xl border border-slate-200 bg-white/70 p-4">
          <div class="text-[11px] font-extrabold tracking-widest text-slate-500 uppercase">{esc(title)}</div>
          <div class="mt-1 text-xl font-black text-slate-900">{esc(value)}</div>
          <div class="mt-1 text-[11px] text-slate-500">{w.compare_label} <b class="{cls_main}">{esc(delta_main)}</b> · YoY <b class="{cls_yoy}">{esc(delta_yoy_s)}</b></div>
        </div>
        """

    def product_img(url: str) -> str:
        u = (url or PLACEHOLDER_IMG or "").strip()
        if u:
            return f"<img src='{esc(u)}' class='w-8 h-8 rounded-xl object-cover border border-slate-200'/>"
        return "<div class='w-8 h-8 rounded-xl bg-slate-100 border border-slate-200'></div>"

    def table_row(cols: List[str], bold=False) -> str:
        fw = "font-extrabold" if bold else "font-medium"
        bg = "bg-slate-50" if bold else ""
        tds = "".join([f"<td class='px-3 py-2 border-b border-slate-100 {fw}'>{c}</td>" for c in cols])
        return f"<tr class='{bg}'>{tds}</tr>"

    # --- Channel snapshot table rows
    chan_html = ""
    if channel_snapshot is not None and (not channel_snapshot.empty):
        for r in channel_snapshot.itertuples(index=False):
            chan_html += table_row([
                esc(getattr(r, "bucket", "")),
                f"<div class='text-right'>{fmt_int(getattr(r, 'sessions', 0))}</div>",
                f"<div class='text-right'>{fmt_int(getattr(r, 'transactions', 0))}</div>",
                f"<div class='text-right'>{fmt_currency_krw(getattr(r, 'purchaseRevenue', 0))}</div>",
                f"<div class='text-right {delta_cls(float(getattr(r, 'rev_vs_prev', 0) or 0))}'>{('+' if float(getattr(r,'rev_vs_prev',0) or 0)>=0 else '')}{fmt_pct(float(getattr(r,'rev_vs_prev',0) or 0),1)}</div>",
                f"<div class='text-right {delta_cls(float(getattr(r, 'rev_yoy', 0) or 0))}'>{('+' if float(getattr(r,'rev_yoy',0) or 0)>=0 else '')}{fmt_pct(float(getattr(r,'rev_yoy',0) or 0),1)}</div>",
            ], bold=(str(getattr(r, "bucket", "")) == "Total"))

    # --- Paid detail rows
    paid_html = ""
    if paid_detail is not None and (not paid_detail.empty):
        for r in paid_detail.itertuples(index=False):
            paid_html += table_row([
                esc(getattr(r, "sub_channel", "")),
                f"<div class='text-right'>{fmt_int(getattr(r, 'sessions', 0))}</div>",
                f"<div class='text-right'>{fmt_currency_krw(getattr(r, 'purchaseRevenue', 0))}</div>",
                f"<div class='text-right {delta_cls(float(getattr(r, 'rev_vs_prev', 0) or 0))}'>{('+' if float(getattr(r,'rev_vs_prev',0) or 0)>=0 else '')}{fmt_pct(float(getattr(r,'rev_vs_prev',0) or 0),1)}</div>",
                f"<div class='text-right {delta_cls(float(getattr(r, 'rev_yoy', 0) or 0))}'>{('+' if float(getattr(r,'rev_yoy',0) or 0)>=0 else '')}{fmt_pct(float(getattr(r,'rev_yoy',0) or 0),1)}</div>",
            ], bold=(str(getattr(r, "sub_channel", "")) == "Total"))

    # --- Best sellers cards
    bs_rows = ""
    if best_sellers is not None and (not best_sellers.empty):
        for r in best_sellers.itertuples(index=False):
            bs_rows += f"""
            <div class="flex items-center gap-3 rounded-2xl border border-slate-200 bg-white/70 p-3">
              {product_img(getattr(r, "image_url", ""))}
              <div class="min-w-0 flex-1">
                <div class="truncate text-sm font-extrabold text-slate-900">{esc(getattr(r, "itemName", "") or "")}</div>
                <div class="text-xs text-slate-500">{esc(getattr(r, "itemId", "") or "")} · Qty {fmt_int(getattr(r, "qty", 0))}</div>
              </div>
              <div class="shrink-0">{getattr(r, "trend_svg", "") or ""}</div>
            </div>
            """

    # --- Rising cards
    rising_rows = ""
    if rising is not None and (not rising.empty):
        for r in rising.itertuples(index=False):
            delta = float(getattr(r, "delta", 0) or 0.0)
            cls = "text-blue-600" if delta >= 0 else "text-orange-700"
            rising_rows += f"""
            <div class="flex items-center gap-3 rounded-2xl border border-slate-200 bg-white/70 p-3">
              {product_img(getattr(r, "image_url", ""))}
              <div class="min-w-0 flex-1">
                <div class="truncate text-sm font-extrabold text-slate-900">{esc(getattr(r, "itemName", "") or "")}</div>
                <div class="text-xs text-slate-500">{esc(getattr(r, "itemId", "") or "")} · Qty {fmt_int(getattr(r, "qty", 0))} · Views {fmt_int(getattr(r, "views", 0))}</div>
              </div>
              <div class="text-sm font-black {cls}">{esc(getattr(r, "delta_label", "Δ") or "Δ")} {('+' if delta>=0 else '')}{fmt_int(delta)}</div>
            </div>
            """

    # --- PDP cards
    pdp_rows = ""
    if category_pdp_trend is not None and (not category_pdp_trend.empty):
        for r in category_pdp_trend.itertuples(index=False):
            pdp_rows += f"""
            <div class="flex items-center justify-between gap-3 rounded-2xl border border-slate-200 bg-white/70 p-3">
              <div class="min-w-0 flex-1">
                <div class="truncate text-sm font-extrabold text-slate-900">{esc(getattr(r, "itemCategory", "") or "")}</div>
                <div class="text-xs text-slate-500">D1 {fmt_int(getattr(r, "views_d1", 0))} · 7D Avg {fmt_int(getattr(r, "views_avg7d", 0))}</div>
              </div>
              <div class="shrink-0">{getattr(r, "trend_svg", "") or ""}</div>
            </div>
            """
    else:
        pdp_rows = "<div class='text-sm text-slate-500'>No data</div>"

    # --- Search terms
    new_terms_html = ""
    if search_new is not None and (not search_new.empty):
        for r in search_new.itertuples(index=False):
            new_terms_html += f"<div class='flex justify-between text-sm'><span class='font-extrabold'>{esc(getattr(r,'searchTerm',''))}</span><span class='text-slate-500'>{fmt_int(getattr(r,'count',0))}</span></div>"

    rising_terms_html = ""
    if search_rising is not None and (not search_rising.empty):
        for r in search_rising.itertuples(index=False):
            pct = float(getattr(r, "pct", 0) or 0.0)
            rising_terms_html += f"<div class='flex justify-between text-sm'><span class='font-extrabold'>{esc(getattr(r,'searchTerm',''))}</span><span class='text-slate-500'>{'+' if pct>=0 else ''}{pct:.1f}% · {fmt_int(getattr(r,'count',0))}</span></div>"

    # --- KPI cards
    kpis_cards = "".join([
        top_kpi_card("Sessions", fmt_int(cur["sessions"]),
                     f"{'+' if s_delta>=0 else ''}{fmt_pct(s_delta,1)}",
                     f"{'+' if s_yoy>=0 else ''}{fmt_pct(s_yoy,1)}",
                     delta_cls(s_delta), delta_cls(s_yoy)),
        top_kpi_card("Revenue", fmt_currency_krw(cur["purchaseRevenue"]),
                     f"{'+' if r_delta>=0 else ''}{fmt_pct(r_delta,1)}",
                     f"{'+' if r_yoy>=0 else ''}{fmt_pct(r_yoy,1)}",
                     delta_cls(r_delta), delta_cls(r_yoy)),
        top_kpi_card("Orders", fmt_int(cur["transactions"]),
                     f"{'+' if o_delta>=0 else ''}{fmt_pct(o_delta,1)}",
                     f"{'+' if o_yoy>=0 else ''}{fmt_pct(o_yoy,1)}",
                     delta_cls(o_delta), delta_cls(o_yoy)),
        top_kpi_card("CVR", f"{cur['cvr']*100:.2f}%",
                     f"{'+' if c_pp>=0 else ''}{fmt_pp(c_pp,2)}",
                     f"{'+' if c_yoy_pp>=0 else ''}{fmt_pp(c_yoy_pp,2)}",
                     delta_cls(c_pp), delta_cls(c_yoy_pp)),
        top_kpi_card("Sign-up Users", fmt_int(su_cur),
                     f"{'+' if su_delta>=0 else ''}{fmt_pct(su_delta,1)}",
                     f"{'+' if su_yoy_delta>=0 else ''}{fmt_pct(su_yoy_delta,1)}",
                     delta_cls(su_delta), delta_cls(su_yoy_delta)),
    ])

    default_a = ymd(w.end_date)
    default_b = ymd(w.prev_end)

    compare_js = f"""
"""

    compare_bar_html = ""

return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>CSK E-COMM | Daily Digest</title>
  <script src="https://cdn.tailwindcss.com"></script>

  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200;400;600;800&display=swap');

    body{{ font-family:'Plus Jakarta Sans', system-ui, -apple-system, Segoe UI, Roboto, Arial; }}

    /* ✅ iframe embed=1 로드 시 상단 컨트롤/허브 버튼/모달 숨김 */
    html[data-embed="1"] .embed-hide {{ display: none !important; }}
    html[data-embed="1"] body {{ background: transparent !important; }}
    html[data-embed="1"] .embed-tight {{ padding: 0 !important; }}

    /* ✅ iframe 내부 스크롤 제거 */
    html[data-embed="1"] body {{ overflow: hidden !important; }}
  </style>

  <script>
    (function() {{
      try {{
        const p = new URLSearchParams(location.search);
        if (p.get('embed') === '1') {{
          document.documentElement.setAttribute('data-embed', '1');
        }}
      }} catch (e) {{}}
    }})();
  </script>
"""

</head>
<body class="bg-slate-50 text-slate-900">
  <div class="mx-auto max-w-6xl p-6 embed-tight">
    <div class="flex flex-wrap items-center justify-between gap-3">
      <div class="flex items-center gap-3">
        <div class="text-2xl font-black">Daily Digest</div>
        <div class="rounded-full bg-slate-900 px-3 py-1 text-xs font-extrabold text-white">{w.mode.upper()}</div>
</div>
      <div class="flex items-center gap-2">
        <a href="{esc(nav_links.get(\'hub\',\'#\'))}" class="embed-hide rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm font-extrabold hover:bg-slate-50">Hub</a>
      </div>
    </div>

    {compare_bar_html}

    <div class="mt-6 grid grid-cols-1 gap-3 md:grid-cols-5">
      {kpis_cards}
    </div>

    <div class="mt-6 grid grid-cols-1 gap-4 lg:grid-cols-2">
      <div class="rounded-2xl border border-slate-200 bg-white/70 p-4">
        <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Channel Snapshot</div>
    <div class="mt-6 rounded-2xl border border-slate-200 bg-white/70 p-4">
      <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">7D Trend (Index)</div>
      <div class="mt-3">{trend_svg}</div>
    </div>

        <table class="mt-3 w-full text-sm">
          <thead class="text-xs text-slate-500">
            <tr>
              <th class="px-3 py-2 text-left">Bucket</th>
              <th class="px-3 py-2 text-right">Sessions</th>
              <th class="px-3 py-2 text-right">Orders</th>
              <th class="px-3 py-2 text-right">Revenue</th>
              <th class="px-3 py-2 text-right">{w.compare_label}</th>
              <th class="px-3 py-2 text-right">YoY</th>
            </tr>
          </thead>
          <tbody>{chan_html}</tbody>
        </table>
      </div>

      <div class="rounded-2xl border border-slate-200 bg-white/70 p-4">
        <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Paid Detail</div>
        <table class="mt-3 w-full text-sm">
          <thead class="text-xs text-slate-500">
            <tr>
              <th class="px-3 py-2 text-left">Sub</th>
              <th class="px-3 py-2 text-right">Sessions</th>
              <th class="px-3 py-2 text-right">Revenue</th>
              <th class="px-3 py-2 text-right">{w.compare_label}</th>
              <th class="px-3 py-2 text-right">YoY</th>
            </tr>
          </thead>
          <tbody>{paid_html}</tbody>
        </table>
      </div>
    </div>

    <div class="mt-6 grid grid-cols-1 gap-4 lg:grid-cols-2">
      <div class="rounded-2xl border border-slate-200 bg-white/70 p-4">
        <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Best Sellers (Top 5)</div>
        <div class="mt-3 space-y-2">{bs_rows or "<div class='text-sm text-slate-500'>No data</div>"}</div>
      </div>

      <div class="rounded-2xl border border-slate-200 bg-white/70 p-4">
        <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Rising Products (Top 5)</div>
        <div class="mt-3 space-y-2">{rising_rows or "<div class='text-sm text-slate-500'>No data</div>"}</div>
      </div>
    </div>

    <div class="mt-6 rounded-2xl border border-slate-200 bg-white/70 p-4">
      <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">PDP View Trend (Category)</div>
      <div class="mt-3 space-y-2">{pdp_rows}</div>
    </div>

    <div class="mt-6 grid grid-cols-1 gap-4 lg:grid-cols-2">
      <div class="rounded-2xl border border-slate-200 bg-white/70 p-4">
        <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Search · New</div>
        <div class="mt-3 space-y-2">{new_terms_html or "<div class='text-sm text-slate-500'>No data</div>"}</div>
      </div>
      <div class="rounded-2xl border border-slate-200 bg-white/70 p-4">
        <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Search · Rising</div>
        <div class="mt-3 space-y-2">{rising_terms_html or "<div class='text-sm text-slate-500'>No data</div>"}</div>
      </div>
    </div>

  </div>

  {compare_js}
</body>
</html>
"""

# =========================
# Hub page
# =========================

def render_hub_index(dates: List[dt.date]) -> str:
    import html as _html

    dates = sorted(dates)
    if not dates:
        dates = [dt.datetime.now(ZoneInfo("Asia/Seoul")).date() - dt.timedelta(days=1)]
    latest = dates[-1]

    date_opts = "\n".join([f"<option value='{d.strftime('%Y-%m-%d')}'>{d.strftime('%Y-%m-%d')}</option>" for d in reversed(dates)])

    # Minimal hub UI; does NOT trigger GA/BQ calls. It reads cached bundle JSON in /data/.
    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>CSK E-COMM | Daily Digest Hub</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200;400;600;800&display=swap');
    body{{ font-family:'Plus Jakarta Sans', system-ui, -apple-system, Segoe UI, Roboto, Arial; }}
  </style>
</head>
<body class="bg-slate-50 text-slate-900">
  <div class="mx-auto max-w-6xl p-6">
    <div class="flex flex-wrap items-center justify-between gap-3">
      <div class="flex items-center gap-3">
        <div class="text-2xl font-black">Daily Digest Hub</div>
        <div class="rounded-full bg-slate-900 px-3 py-1 text-xs font-extrabold text-white">STATIC</div>
      </div>
      <div class="text-sm text-slate-500">Data cache 기반 · Compare는 브라우저에서 JSON 합산</div>
    </div>

    <!-- Quick open -->
    <div class="mt-5 grid grid-cols-1 gap-3 md:grid-cols-3">
      <div class="rounded-2xl border border-slate-200 bg-white/70 p-4">
        <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Open report</div>
        <div class="mt-3 flex items-center gap-2">
          <select id="openDate" class="w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm">
            {date_opts}
          </select>
        </div>
        <div class="mt-3 flex gap-2">
          <button id="openDaily" class="flex-1 rounded-xl bg-slate-900 px-4 py-2 text-sm font-extrabold text-white hover:bg-slate-800">Daily</button>
          <button id="openWeekly" class="flex-1 rounded-xl border border-slate-200 bg-white px-4 py-2 text-sm font-extrabold hover:bg-slate-50">Weekly</button>
        </div>
      </div>

      <!-- Range compare -->
      <div class="md:col-span-2 rounded-2xl border border-slate-200 bg-white/70 p-4">
        <div class="flex flex-wrap items-center gap-2">
          <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Range Compare</div>
          <select id="mode" class="ml-auto rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm">
            <option value="daily">Daily</option>
            <option value="weekly">Weekly (END date)</option>
          </select>
        </div>

        <div class="mt-3 grid grid-cols-1 gap-3 lg:grid-cols-2">
          <div class="rounded-2xl border border-slate-200 bg-white p-3">
            <div class="text-xs font-extrabold text-slate-600">A 구간</div>
            <div class="mt-2 flex flex-wrap gap-2">
              <input id="aStart" type="date" class="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm" />
              <input id="aEnd" type="date" class="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm" />
              <button id="aYoY" class="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm font-extrabold hover:bg-slate-50">A→YoY</button>
              <button id="aPrev" class="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm font-extrabold hover:bg-slate-50">A→Prev</button>
            </div>
          </div>

          <div class="rounded-2xl border border-slate-200 bg-white p-3">
            <div class="text-xs font-extrabold text-slate-600">B 구간</div>
            <div class="mt-2 flex flex-wrap gap-2">
              <input id="bStart" type="date" class="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm" />
              <input id="bEnd" type="date" class="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm" />
              <button id="swap" class="rounded-xl bg-slate-900 px-3 py-2 text-sm font-extrabold text-white hover:bg-slate-800">Swap</button>
              <button id="run" class="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm font-extrabold hover:bg-slate-50">Compare</button>
            </div>
          </div>
        </div>

        <div id="rangeErr" class="mt-3 text-sm font-semibold text-orange-700"></div>
      </div>
    </div>

    <!-- Output -->
    <div class="mt-6 grid grid-cols-1 gap-4 lg:grid-cols-2">
      <div class="rounded-2xl border border-slate-200 bg-white/70 p-4">
        <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">KPIs</div>
        <div id="kpiOut" class="mt-3 text-sm text-slate-500">구간을 선택하고 Compare를 누르면 계산됩니다.</div>
      </div>
      <div class="rounded-2xl border border-slate-200 bg-white/70 p-4">
        <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Channel Revenue</div>
        <div id="chOut" class="mt-3 text-sm text-slate-500">—</div>
      </div>
    </div>

    <!-- Recent list -->
    <div class="mt-6 rounded-2xl border border-slate-200 bg-white/70 p-4">
      <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Recent</div>
      <div id="recentList" class="mt-3 grid grid-cols-1 gap-2 md:grid-cols-2"></div>
    </div>
  </div>

<script>
(() => {{
  const dates = {json.dumps([d.strftime("%Y-%m-%d") for d in dates])};
  const latest = "{latest.strftime('%Y-%m-%d')}";

  const $ = (s) => document.querySelector(s);
  const esc = (s) => String(s ?? "").replace(/[&<>"]/g, c => ({{"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}}[c]));
  const fmtInt = (x) => Number(x||0).toLocaleString("en-US");
  const fmtKRW = (x) => "₩" + Math.round(Number(x||0)).toLocaleString("en-US");
  const pctChange = (c,p) => (Number(p||0)===0) ? ((Number(c||0)===0)?0:1) : ((Number(c||0)-Number(p||0))/Number(p||0));
  const fmtPct = (p,d=1) => (Number(p||0)*100).toFixed(d)+"%";
  const fmtPP = (p,d=2) => (Number(p||0)*100).toFixed(d)+"%p";

  function bundlePath(mode, d) {{
    if(mode==="weekly") return "data/weekly/END_"+d+".json";
    return "data/daily/"+d+".json";
  }}

  function ymdToDate(s) {{
    const [y,m,d] = s.split("-").map(x=>parseInt(x,10));
    return new Date(y, m-1, d);
  }}
  function dateToYmd(dt) {{
    const y = dt.getFullYear();
    const m = String(dt.getMonth()+1).padStart(2,"0");
    const d = String(dt.getDate()).padStart(2,"0");
    return `${{y}}-${{m}}-${{d}}`;
  }}

  function rangeDates(start, end) {{
    const out = [];
    let a = ymdToDate(start);
    const b = ymdToDate(end);
    if(a>b) return out;
    for(; a<=b; a.setDate(a.getDate()+1)) out.push(dateToYmd(a));
    return out;
  }}

  async function fetchJSON(path) {{
    const r = await fetch(path, {{cache:"force-cache"}});
    if(!r.ok) throw new Error(path);
    return await r.json();
  }}

  function initDefaults() {{
    $("#openDate").value = latest;
    $("#mode").value = "daily";
    $("#aStart").value = latest;
    $("#aEnd").value = latest;
    // default B: previous day
    const dt = ymdToDate(latest); dt.setDate(dt.getDate()-1);
    const prev = dateToYmd(dt);
    $("#bStart").value = prev;
    $("#bEnd").value = prev;
  }}

  function renderRecent() {{
    const items = dates.slice(-10).reverse();
    $("#recentList").innerHTML = items.map(d => `
      <div class="flex items-center justify-between rounded-2xl border border-slate-200 bg-white p-3">
        <div class="text-sm font-extrabold">${{esc(d)}}</div>
        <div class="flex gap-2">
          <a class="rounded-xl bg-slate-900 px-3 py-2 text-xs font-extrabold text-white hover:bg-slate-800" href="daily/${{esc(d)}}.html">Daily</a>
          <a class="rounded-xl border border-slate-200 bg-white px-3 py-2 text-xs font-extrabold hover:bg-slate-50" href="weekly/END_${{esc(d)}}.html">Weekly</a>
        </div>
      </div>
    `).join("");
  }}

  function sumAgg() {{
    return {{
      sessions:0, orders:0, revenue:0, cvr_num:0, cvr_den:0, signups:0,
      channels: {{}} // bucket -> revenue
    }};
  }}

  function addBundle(agg, b) {{
    const cur = (b.overall && b.overall.current) || {{}};
    const sessions = Number(cur.sessions||0);
    const orders = Number(cur.transactions||0);
    const revenue = Number(cur.purchaseRevenue||0);
    const signups = Number((b.signup_users && b.signup_users.current)||0);

    agg.sessions += sessions;
    agg.orders += orders;
    agg.revenue += revenue;
    agg.signups += signups;
    agg.cvr_num += orders;
    agg.cvr_den += sessions;

    const ch = (b.channels) ? b.channels : null; // hub-friendly
    if(ch && typeof ch === "object") {{
      Object.keys(ch).forEach(k => {{
        const rev = Number((ch[k] && ch[k].revenue)||0);
        agg.channels[k] = (agg.channels[k]||0) + rev;
      }});
    }} else if(Array.isArray(b.channel_snapshot)) {{
      b.channel_snapshot.forEach(x => {{
        const k = x.bucket;
        if(!k) return;
        const rev = Number(x.purchaseRevenue||0);
        agg.channels[k] = (agg.channels[k]||0) + rev;
      }});
    }}
  }}

  function finalize(agg) {{
    const cvr = (agg.cvr_den===0)?0:(agg.cvr_num/agg.cvr_den);
    return {{
      sessions: agg.sessions,
      orders: agg.orders,
      revenue: agg.revenue,
      cvr,
      signups: agg.signups,
      channels: agg.channels
    }};
  }}

  function renderCompare(a, b) {{
    const rows = [
      ["Sessions", fmtInt(a.sessions), fmtInt(b.sessions), pctChange(a.sessions, b.sessions), false],
      ["Orders", fmtInt(a.orders), fmtInt(b.orders), pctChange(a.orders, b.orders), false],
      ["Revenue", fmtKRW(a.revenue), fmtKRW(b.revenue), pctChange(a.revenue, b.revenue), false],
      ["CVR", (a.cvr*100).toFixed(2)+"%", (b.cvr*100).toFixed(2)+"%", (a.cvr - b.cvr), true],
      ["Sign-ups", fmtInt(a.signups), fmtInt(b.signups), pctChange(a.signups, b.signups), false],
    ];
    const kpiTable = `
      <table class="w-full text-sm">
        <thead class="text-xs text-slate-500">
          <tr>
            <th class="px-3 py-2 text-left">Metric</th>
            <th class="px-3 py-2 text-right">A</th>
            <th class="px-3 py-2 text-right">B</th>
            <th class="px-3 py-2 text-right">Diff</th>
          </tr>
        </thead>
        <tbody>
          ${{rows.map(r=>{{
            const diff = r[3];
            const isPP = r[4];
            const cls = diff>=0 ? "text-blue-600" : "text-orange-700";
            const txt = isPP ? ((diff>=0?"+":"")+fmtPP(diff,2)) : ((diff>=0?"+":"")+fmtPct(diff,1));
            return `
              <tr class="border-b border-slate-100">
                <td class="px-3 py-2 font-extrabold">${{esc(r[0])}}</td>
                <td class="px-3 py-2 text-right font-semibold">${{esc(r[1])}}</td>
                <td class="px-3 py-2 text-right font-semibold">${{esc(r[2])}}</td>
                <td class="px-3 py-2 text-right"><span class="${{cls}} font-extrabold">${{esc(txt)}}</span></td>
              </tr>
            `;
          }}).join("")}}
        </tbody>
      </table>
    `;
    $("#kpiOut").innerHTML = kpiTable;

    const buckets = ["Organic","Paid AD","Owned","Awareness","SNS","Total"];
    const chRows = buckets.map(k => {{
      const ar = Number(a.channels[k]||0);
      const br = Number(b.channels[k]||0);
      const diff = pctChange(ar, br);
      const cls = diff>=0 ? "text-blue-600" : "text-orange-700";
      return `
        <tr class="border-b border-slate-100">
          <td class="px-3 py-2 font-extrabold">${{esc(k)}}</td>
          <td class="px-3 py-2 text-right">${{esc(fmtKRW(ar))}}</td>
          <td class="px-3 py-2 text-right">${{esc(fmtKRW(br))}}</td>
          <td class="px-3 py-2 text-right"><span class="${{cls}} font-extrabold">${{esc((diff>=0?"+":"")+fmtPct(diff,1))}}</span></td>
        </tr>
      `;
    }}).join("");

    $("#chOut").innerHTML = `
      <table class="w-full text-sm">
        <thead class="text-xs text-slate-500">
          <tr>
            <th class="px-3 py-2 text-left">Bucket</th>
            <th class="px-3 py-2 text-right">A</th>
            <th class="px-3 py-2 text-right">B</th>
            <th class="px-3 py-2 text-right">Diff</th>
          </tr>
        </thead>
        <tbody>${{chRows}}</tbody>
      </table>
    `;
  }}

  async function runCompare() {{
    $("#rangeErr").textContent = "";
    const mode = $("#mode").value;
    const aS = $("#aStart").value, aE = $("#aEnd").value;
    const bS = $("#bStart").value, bE = $("#bEnd").value;
    if(!aS||!aE||!bS||!bE) {{
      $("#rangeErr").textContent = "A/B 구간 날짜를 모두 선택해줘.";
      return;
    }}
    const aDates = rangeDates(aS, aE);
    const bDates = rangeDates(bS, bE);
    if(aDates.length===0 || bDates.length===0) {{
      $("#rangeErr").textContent = "구간이 잘못됐어. (start <= end)";
      return;
    }}

    const aAgg = sumAgg();
    const bAgg = sumAgg();

    try {{
      await Promise.all(aDates.map(async d => {{
        const b = await fetchJSON(bundlePath(mode, d));
        addBundle(aAgg, b);
      }}));
      await Promise.all(bDates.map(async d => {{
        const b = await fetchJSON(bundlePath(mode, d));
        addBundle(bAgg, b);
      }}));
    }} catch(e) {{
      $("#rangeErr").textContent = "JSON이 없는 날짜가 있어. 먼저 해당 날짜 리포트가 생성돼 있어야 해.";
      console.error(e);
      return;
    }}

    renderCompare(finalize(aAgg), finalize(bAgg));
  }}

  function swapAB() {{
    const as = $("#aStart").value, ae = $("#aEnd").value;
    $("#aStart").value = $("#bStart").value;
    $("#aEnd").value = $("#bEnd").value;
    $("#bStart").value = as;
    $("#bEnd").value = ae;
  }}

  function shiftRange(start, end, days) {{
    let s = ymdToDate(start);
    let e = ymdToDate(end);
    s.setDate(s.getDate()+days);
    e.setDate(e.getDate()+days);
    return [dateToYmd(s), dateToYmd(e)];
  }}

  function presetPrevFromA() {{
    const as = $("#aStart").value, ae = $("#aEnd").value;
    if(!as||!ae) return;
    const aDates = rangeDates(as, ae);
    const len = aDates.length;
    const [bs, be] = shiftRange(as, ae, -len);
    $("#bStart").value = bs;
    $("#bEnd").value = be;
  }}

  function presetYoYFromA() {{
    const as = $("#aStart").value, ae = $("#aEnd").value;
    if(!as||!ae) return;
    const [bs, be] = shiftRange(as, ae, -364);
    $("#bStart").value = bs;
    $("#bEnd").value = be;
  }}

  function init() {{
    initDefaults();
    renderRecent();
    $("#openDaily").addEventListener("click", () => {{
      const d = $("#openDate").value;
      window.location.href = "daily/" + d + ".html";
    }});
    $("#openWeekly").addEventListener("click", () => {{
      const d = $("#openDate").value;
      window.location.href = "weekly/END_" + d + ".html";
    }});
    $("#run").addEventListener("click", runCompare);
    $("#swap").addEventListener("click", swapAB);
    $("#aPrev").addEventListener("click", presetPrevFromA);
    $("#aYoY").addEventListener("click", presetYoYFromA);
  }}
  document.addEventListener("DOMContentLoaded", init);
}})();
</script>

</body>
</html>
"""


# =========================
# Build one report (with bundle cache)
# =========================
def build_one(
    client: BetaAnalyticsDataClient,
    end_date: dt.date,
    mode: str,
    image_map: Dict[str, str],
    logo_b64: str,
) -> Tuple[str, dict]:
    w = build_window(end_date=end_date, mode=mode)

    bpath = bundle_path(w.mode, w.end_date)
    if USE_DATA_CACHE:
        cached = read_json(bpath)
        if cached:
            rt = rebuild_runtime_objects_from_bundle(cached, image_map=image_map)
            bundle_rel = "../data/weekly/END_" + ymd(w.end_date) + ".json" if w.mode == "weekly" else "../data/daily/" + ymd(w.end_date) + ".json"
            html = render_page_html(
                logo_b64=logo_b64,
                w=rt["w"],
                overall=rt["overall"],
                signup_users=rt["signup_users"],
                channel_snapshot=rt["channel_snapshot"],
                paid_detail=rt["paid_detail"],
                paid_top3=rt["paid_top3"],
                kpi_snapshot=rt["kpi_snapshot"],
                trend_svg=rt["trend_svg"],
                best_sellers=rt["best_sellers"],
                rising=rt["rising"],
                category_pdp_trend=rt["category_pdp_trend"],
                search_new=rt["search_new"],
                search_rising=rt["search_rising"],
                nav_links={"hub": "../index.html", "daily_index": "../index.html", "weekly_index": "../index.html"},
                bundle_rel_path=bundle_rel,
            )
            return html, cached

    overall = get_overall_kpis(client, w)
    signup_users = get_multi_event_users_3way(client, w, ["signup_complete", "signup"])
    channel_snapshot = get_channel_snapshot_3way(client, w)
    paid_detail = get_paid_detail_3way(client, w)
    paid_top3 = get_paid_top3(client, w)
    kpi_snapshot = get_kpi_snapshot_table_3way(client, w, overall)

    trend_series = get_trend_view_series(client, w)
    trend_svg = trend_svg_from_series(trend_series)

    best_sellers, best_sellers_series = get_best_sellers_with_trends(client, w, image_map)
    rising = get_rising_products(client, w, top_n=5)
    rising = attach_image_urls(rising, image_map)
    if PLACEHOLDER_IMG and (not rising.empty) and ("image_url" in rising.columns):
        rising.loc[rising["image_url"].astype(str).str.strip() == "", "image_url"] = PLACEHOLDER_IMG

    missing = []
    if not best_sellers.empty and 'itemId' in best_sellers.columns:
        missing += [sku for sku in best_sellers['itemId'].tolist() if str(sku).strip() not in image_map]
    if not rising.empty and 'itemId' in rising.columns:
        missing += [sku for sku in rising['itemId'].tolist() if str(sku).strip() not in image_map]
    if missing:
        write_missing_image_skus(MISSING_SKU_OUT, missing)

    category_pdp_trend, pdp_series = get_category_pdp_view_trend_bq(end_date=end_date)
    search = get_search_trends(client, end_date=end_date)

    bundle = build_bundle(
        w=w,
        overall=overall,
        signup_users=signup_users,
        channel_snapshot=channel_snapshot,
        paid_detail=paid_detail,
        paid_top3=paid_top3,
        kpi_snapshot=kpi_snapshot,
        trend_series=trend_series,
        best_sellers_df=best_sellers,
        best_sellers_series=best_sellers_series,
        rising_df=rising,
        pdp_series=pdp_series,
        search_new=search["new"],
        search_rising=search["rising"],
    )

    if WRITE_DATA_CACHE:
        write_json(bpath, bundle)

    bundle_rel = "../data/weekly/END_" + ymd(w.end_date) + ".json" if w.mode == "weekly" else "../data/daily/" + ymd(w.end_date) + ".json"
    html = render_page_html(
        logo_b64=logo_b64,
        w=w,
        overall=overall,
        signup_users=signup_users,
        channel_snapshot=channel_snapshot,
        paid_detail=paid_detail,
        paid_top3=paid_top3,
        kpi_snapshot=kpi_snapshot,
        trend_svg=trend_svg,
        best_sellers=best_sellers,
        rising=rising,
        category_pdp_trend=category_pdp_trend,
        search_new=search["new"],
        search_rising=search["rising"],
        nav_links={"hub": "../index.html", "daily_index": "../index.html", "weekly_index": "../index.html"},
        bundle_rel_path=bundle_rel,
    )
    return html, bundle


# =========================
# Main
# =========================
def main():
    if not PROPERTY_ID:
        raise SystemExit("ERROR: GA4_PROPERTY_ID is empty. Set env var GA4_PROPERTY_ID and retry.")

    _scopes = [
        'https://www.googleapis.com/auth/analytics.readonly',
        'https://www.googleapis.com/auth/cloud-platform',
    ]
    _creds, _proj = google_auth_default(scopes=_scopes)
    client = BetaAnalyticsDataClient(credentials=_creds)

    if bigquery is None:
        print('[WARN] google-cloud-bigquery not installed; PDP Trend will be empty. Install: pip install google-cloud-bigquery')

    today_kst = dt.datetime.now(ZoneInfo("Asia/Seoul")).date()
    latest_end = today_kst - dt.timedelta(days=1)

    dates = [latest_end - dt.timedelta(days=i) for i in range(max(1, DAYS_TO_BUILD))]
    dates = [d for d in dates if d.year >= 2000]

    logo_b64 = load_logo_base64(LOGO_PATH)
    image_map = load_image_map_from_excel_urls(IMAGE_XLS_PATH)

    ensure_dir(OUT_DIR)
    daily_dir = os.path.join(OUT_DIR, "daily")
    weekly_dir = os.path.join(OUT_DIR, "weekly")
    ensure_dir(daily_dir)
    ensure_dir(weekly_dir)
    ensure_dir(os.path.join(DATA_DIR, "daily"))
    ensure_dir(os.path.join(DATA_DIR, "weekly"))
    ensure_dir(os.path.join(OUT_DIR, "cache", "pdp"))

    yoy_dates: List[dt.date] = []
    for d in dates:
        yoy_dates.append(build_window(end_date=d, mode="daily").yoy_end)
        yoy_dates.append(build_window(end_date=d, mode="weekly").yoy_end)
    all_dates = sorted(set(dates + yoy_dates))

    for d in all_dates:
        out_daily = os.path.join(daily_dir, f"{ymd(d)}.html")
        out_weekly = os.path.join(weekly_dir, f"END_{ymd(d)}.html")

        if SKIP_IF_EXISTS and os.path.exists(out_daily):
            print(f"[SKIP] Exists (Daily): {out_daily}")
        else:
            html_daily, _bundle = build_one(client, end_date=d, mode="daily", image_map=image_map, logo_b64=logo_b64)
            with open(out_daily, "w", encoding="utf-8") as f:
                f.write(html_daily)
            print(f"[OK] Wrote: {out_daily}")

        if SKIP_IF_EXISTS and os.path.exists(out_weekly):
            print(f"[SKIP] Exists (Weekly): {out_weekly}")
        else:
            html_weekly, _bundle = build_one(client, end_date=d, mode="weekly", image_map=image_map, logo_b64=logo_b64)
            with open(out_weekly, "w", encoding="utf-8") as f:
                f.write(html_weekly)
            print(f"[OK] Wrote: {out_weekly}")

    hub_path = os.path.join(OUT_DIR, "index.html")
    force_overwrite = os.getenv("DAILY_DIGEST_FORCE_HUB_OVERWRITE", "false").strip().lower() in ("1", "true", "yes", "y")

    if SKIP_HUB_WRITE:
        print(f"[SKIP] HUB write disabled (keeping existing): {hub_path}")
    elif (not force_overwrite) and os.path.exists(hub_path):
        print(f"[SKIP] HUB exists (no overwrite): {hub_path}")
    else:
        hub = render_hub_index(dates=dates)
        with open(hub_path, "w", encoding="utf-8") as f:
            f.write(hub)
        print(f"[OK] Wrote HUB: {hub_path}")



if __name__ == "__main__":
    main()
