#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Columbia Daily Digest live report builder.

Features
- GA4 Data API based daily / weekly digest
- BigQuery PDP trend enrichment
- Bundle JSON cache for rebuilding HTML without re-query
- Client-side compare UI using cached data only

Key behavior
1. KPI Sessions always matches Channel Snapshot Total Sessions.
2. Channel Snapshot Paid AD always matches Paid Detail Total.
3. Paid Detail Total is computed from the full Paid AD base, not only visible rows.
4. Compare UI works from cached JSON, so no extra GA4 / BigQuery cost is added.
5. Trend cards, search cards, and table layouts are rendered for email-friendly HTML.

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
import re
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

# Hub index write control
SKIP_HUB_WRITE = os.getenv("DAILY_DIGEST_SKIP_HUB_WRITE", "true").strip().lower() in ("1", "true", "yes", "y")

# Data cache (bundle JSON)
USE_DATA_CACHE = os.getenv("DAILY_DIGEST_USE_DATA_CACHE", "true").strip().lower() in ("1", "true", "yes", "y")
WRITE_DATA_CACHE = os.getenv("DAILY_DIGEST_WRITE_DATA_CACHE", "true").strip().lower() in ("1", "true", "yes", "y")
CACHE_PDP = os.getenv("DAILY_DIGEST_CACHE_PDP", "true").strip().lower() in ("1", "true", "yes", "y")

# Paid detail fixed order / labels
PAID_DETAIL_SOURCES = [
    "naver brand search",
    "naver search",
    "criteo",
    "meta",
    "google demand gen",
    "google pmax",
    "google search",
    "google",
    "instagram",
]

TARGET_ROAS_XLS_PATH = os.getenv("DAILY_DIGEST_TARGET_ROAS_XLS_PATH", "target_roas.xlsx").strip()
MEDIA_SPEND_XLS_PATH = os.getenv("DAILY_DIGEST_MEDIA_SPEND_XLS_PATH", os.path.join(DATA_DIR, "paid_media_spend.xlsx")).strip()
MEDIA_SPEND_HISTORY_PATH = os.getenv("DAILY_DIGEST_MEDIA_SPEND_HISTORY_PATH", os.path.join(DATA_DIR, "paid_media_spend_history.csv")).strip()
MEDIA_SPEND_VENDOR_DIR = os.getenv("DAILY_DIGEST_MEDIA_SPEND_VENDOR_DIR", DATA_DIR).strip()


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
        return "-"

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

def normalize_sku_key(value: Any) -> str:
    s = str(value or "").strip()
    if not s:
        return ""
    s = re.sub(r"\.0+$", "", s)
    return s.upper()

def attach_image_urls(df: pd.DataFrame, image_map: Dict[str, str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(df, copy=True)
    out = df.copy()
    if "itemId" not in out.columns:
        return out
    out["image_url"] = out["itemId"].map(lambda x: image_map.get(normalize_sku_key(x), ""))
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
    if not xlsx_path:
        print("[WARN] Image Excel path is empty.")
        return {}

    if not os.path.exists(xlsx_path):
        candidates = [
            os.path.join(os.path.dirname(os.path.abspath(__file__)), xlsx_path),
            os.path.join(DATA_DIR, os.path.basename(xlsx_path)),
        ]
        found = next((p for p in candidates if os.path.exists(p)), "")
        if found:
            xlsx_path = found
        else:
            print(f"[WARN] Image Excel not found: {xlsx_path}")
            return {}

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
            sku = normalize_sku_key(raw.iat[r, sku_idx]) if sku_idx < raw.shape[1] else ""
            url = str(raw.iat[r, url_idx]).strip() if url_idx < raw.shape[1] else ""
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
        prev_end = end_date - dt.timedelta(days=7)
        prev_start = prev_end
        compare_label = "WoW"

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


# =========================
# Looker CASE rules based on sample.rtf source/medium + campaign logic
# =========================
def _rx(p: str):
    return re.compile(p, re.IGNORECASE)

def classify_looker_channel(source_medium: str, campaign: str = "") -> str:
    sm = (source_medium or "").strip()
    cp = (campaign or "").strip()

    # Order matters here to match the Looker CASE evaluation order.
    if _rx(r".*(instagram).*").search(sm) and _rx(r".*(story).*").search(sm):
        return "SNS"
    if _rx(r".*(benz).*").search(sm):
        return "Organic"

    if _rx(r".*(nap).*").search(sm) and _rx(r".*(da).*").search(sm):
        return "Paid AD"
    if _rx(r".*(toss).*").search(sm):
        return "Paid AD"
    if _rx(r".*(blind).*").search(sm):
        return "Paid AD"
    if _rx(r".*(kakaobs).*").search(sm):
        return "Paid AD"

    if _rx(r".*(inhouse).*").search(sm):
        return "Organic"
    if _rx(r".*(lms).*").search(sm) or _rx(r".*(lms).*").search(cp):
        return "Owned"
    if _rx(r".*(email|edm).*").search(sm):
        return "Owned"
    if _rx(r".*(kakao_fridnstalk).*").search(sm):
        return "Owned"

    # Awareness campaign patterns from sample.rtf.
    if _rx(r".*(mkt|_bd|\[bd).*").search(sm) or _rx(r".*(mkt|_bd|\[bd).*").search(cp):
        return "Awareness"

    if _rx(r".*(igshopping).*").search(sm):
        return "SNS"
    if _rx(r".*(facebook).*").search(sm) and _rx(r".*(referral).*").search(sm):
        return "Organic"
    if _rx(r".*(instagram).*").search(sm) and _rx(r".*(referral).*").search(sm):
        return "SNS"
    if _rx(r".*(meta|facebook|instagram|ig|fb).*").search(sm):
        return "Paid AD"

    # google / cpc split by campaign keyword
    if _rx(r".*google\s*/\s*cpc.*").search(sm) and _rx(r".*(dg|demandgen).*").search(cp):
        return "Awareness"
    if _rx(r".*google\s*/\s*cpc.*").search(sm) and _rx(r".*(pmax).*").search(cp):
        return "Paid AD"
    if _rx(r".*google\s*/\s*cpc.*").search(sm) and _rx(r".*(yt|youtube|instream|vac|vvc).*").search(cp):
        return "Awareness"
    if _rx(r".*google\s*/\s*cpc.*").search(sm) and _rx(r".*(discovery).*").search(cp):
        return "Awareness"
    if _rx(r".*google\s*/\s*cpc.*").search(sm) and _rx(r".*(sa|ss|search).*").search(cp):
        return "Paid AD"
    if _rx(r".*google\s*/\s*cpc.*").search(sm):
        return "Paid AD"

    if _rx(r".*google\s*/\s*organic.*").search(sm):
        return "Organic"
    if _rx(r".*(google).*").search(sm):
        return "Organic"

    if _rx(r".*(youtube).*").search(sm):
        return "Organic"

    if _rx(r".*(naver).*").search(sm) and _rx(r".*(da).*").search(sm):
        return "Paid AD"
    if _rx(r".*(gfa).*").search(sm):
        return "Paid AD"
    if _rx(r".*(naverbs).*").search(sm):
        return "Paid AD"
    if _rx(r".*(naver).*").search(sm) and _rx(r".*(cpc).*").search(sm):
        return "Paid AD"
    if _rx(r".*(shopping_ad).*").search(sm):
        return "Paid AD"
    if _rx(r".*(naver).*").search(sm) and _rx(r".*(shopping).*").search(sm):
        return "Organic"
    if _rx(r".*(naver).*").search(sm) and _rx(r".*(organic).*").search(sm):
        return "Organic"
    if _rx(r".*(naver).*").search(sm):
        return "Organic"

    if _rx(r".*daum\s*/\s*organic.*").search(sm):
        return "Organic"
    if _rx(r".*(daum).*").search(sm) and _rx(r".*(referral).*").search(sm):
        return "Organic"

    if _rx(r".*(kakao_ch).*").search(sm) or _rx(r".*(kakao_ch).*").search(cp):
        return "Owned"
    if _rx(r".*(kakao_alimtalk).*").search(sm):
        return "Owned"
    if _rx(r".*(kakao_coupon).*").search(sm):
        return "Owned"
    if _rx(r".*(kakao_chatbot).*").search(sm):
        return "Owned"
    if _rx(r".*(kakao).*").search(sm):
        return "Paid AD"

    if _rx(r".*\(direct\)\s*/\s*\(none\).*").search(sm):
        return "Organic"

    if _rx(r".*(signalplay|signal play|signal_play|sg_|signal|manplus).*").search(sm):
        return "Paid AD"
    if _rx(r".*(buzzvill).*").search(sm):
        return "Paid AD"
    if _rx(r".*(criteo).*").search(sm):
        return "Paid AD"
    if _rx(r".*(mobon).*").search(sm):
        return "Paid AD"
    if _rx(r".*(snow).*").search(sm):
        return "Paid AD"
    if _rx(r".*(smr).*").search(sm):
        return "Paid AD"
    if _rx(r".*(tg).*").search(sm):
        return "Paid AD"
    if _rx(r".*(t_cafe).*").search(sm):
        return "Paid AD"
    if _rx(r".*(blind).*").search(sm):
        return "Paid AD"

    # fallbacks
    if _rx(r".*(cpc).*").search(sm):
        return "Paid AD"
    if _rx(r".*(organic).*").search(sm):
        return "Organic"
    if _rx(r".*(banner|da).*").search(sm):
        return "Paid AD"
    if _rx(r".*(referral).*").search(sm):
        return "Organic"
    if _rx(r".*(shopping).*").search(sm):
        return "Organic"
    if _rx(r".*(social).*").search(sm):
        return "Organic"

    return "Other"


def classify_paid_detail(source_medium: str, campaign: str = "") -> str:
    sm = (source_medium or "").strip().lower()
    cp = (campaign or "").strip().lower()

    def has(p: str, s: str) -> bool:
        return re.search(p, s, re.IGNORECASE) is not None

    if has(r".*naverbs.*", sm):
        return "naver brand search"
    if has(r".*(igshopping|instagram|(^|[^a-z])ig([^a-z]|$)).*", sm):
        return "instagram"
    if has(r".*criteo.*", sm):
        return "criteo"
    if has(r".*(meta|facebook|(^|[^a-z])fb([^a-z]|$)).*", sm):
        return "meta"
    if has(r".*google\s*/\s*cpc.*", sm) or has(r"(^|[^a-z])google([^a-z]|$)", sm):
        if has(r"(^|[^a-z])(dg|demand|demand[\s_-]*gen)([^a-z]|$)", cp):
            return "google demand gen"
        if has(r"(^|[^a-z])pmax([^a-z]|$)", cp):
            return "google pmax"
        if has(r"(^|[^a-z])(sa|ss|search)([^a-z]|$)", cp):
            return "google search"
        return "google"
    if has(r".*naver.*", sm) and has(r".*cpc.*", sm):
        if has(r"(naverbs|brandsearch|brand search)", cp) or has(r"(naverbs|brandsearch|brand search)", sm):
            return "naver brand search"
        if has(r"(naversa|(^|[^a-z])sa([^a-z]|$)|search)", cp) or has(r".*(m\.search\.naver\.com|m\.ad\.search\.naver\.com|m\.search\.naver).*", sm):
            return "naver search"
        return "naver search"

    base = sm.split("/")[0].strip()
    base = re.sub(r"\s+", " ", base)
    return base or "other"


# =========================
# Channel Snapshot based on Looker CASE rules
# =========================
def get_channel_snapshot_3way(
    client: BetaAnalyticsDataClient,
    w: DigestWindow,
    overall: Dict[str, Dict[str, float]],
) -> pd.DataFrame:
    """
    Build Channel Snapshot using the Looker CASE bucket logic.
    The Total row is always forced from the authoritative overall KPI.
    """
    dims = ["sessionSourceMedium", "sessionCampaignName"]
    mets = ["sessions", "transactions", "purchaseRevenue"]

    def fetch(start: dt.date, end: dt.date) -> pd.DataFrame:
        try:
            df = run_report(client, PROPERTY_ID, ymd(start), ymd(end), dims, mets, limit=250000)
            if df.empty:
                return pd.DataFrame(columns=dims + mets)
            df = df.copy()
            df["sessionSourceMedium"] = df["sessionSourceMedium"].astype(str).fillna("")
            df["sessionCampaignName"] = df["sessionCampaignName"].astype(str).fillna("")
            for c in mets:
                df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0.0)
            df["bucket"] = df.apply(lambda r: classify_looker_channel(r["sessionSourceMedium"], r["sessionCampaignName"]), axis=1)
            return df
        except Exception:
            # Fallback when sessionCampaignName is unavailable.
            dims2 = ["sessionSourceMedium"]
            df = run_report(client, PROPERTY_ID, ymd(start), ymd(end), dims2, mets, limit=250000)
            if df.empty:
                return pd.DataFrame(columns=dims2 + mets + ["sessionCampaignName", "bucket"])
            df = df.copy()
            df["sessionSourceMedium"] = df["sessionSourceMedium"].astype(str).fillna("")
            df["sessionCampaignName"] = ""
            for c in mets:
                df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0.0)
            df["bucket"] = df.apply(lambda r: classify_looker_channel(r["sessionSourceMedium"], ""), axis=1)
            return df

    cur = fetch(w.cur_start, w.cur_end)
    prev = fetch(w.prev_start, w.prev_end)
    yoy = fetch(w.yoy_start, w.yoy_end)

    def agg(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=["bucket", f"sessions{suffix}", f"transactions{suffix}", f"revenue{suffix}"])
        g = df.groupby("bucket", as_index=False)[["sessions", "transactions", "purchaseRevenue"]].sum()
        g = g.rename(columns={
            "sessions": f"sessions{suffix}",
            "transactions": f"transactions{suffix}",
            "purchaseRevenue": f"revenue{suffix}",
        })
        return g

    cur_b = agg(cur, "_cur")
    prev_b = agg(prev, "_prev")
    yoy_b = agg(yoy, "_yoy")

    m = cur_b.merge(prev_b, on="bucket", how="outer").merge(yoy_b, on="bucket", how="outer").fillna(0.0)

    oc = overall.get("current", {}) or {}
    op = overall.get("prev", {}) or {}
    oy = overall.get("yoy", {}) or {}

    total_sessions_cur = float(oc.get("sessions", 0) or 0)
    total_sessions_prev = float(op.get("sessions", 0) or 0)
    total_sessions_yoy = float(oy.get("sessions", 0) or 0)
    total_orders_cur = float(oc.get("transactions", 0) or 0)
    total_orders_prev = float(op.get("transactions", 0) or 0)
    total_orders_yoy = float(oy.get("transactions", 0) or 0)
    total_revenue_cur = float(oc.get("purchaseRevenue", 0) or 0)
    total_revenue_prev = float(op.get("purchaseRevenue", 0) or 0)
    total_revenue_yoy = float(oy.get("purchaseRevenue", 0) or 0)

    canonical_buckets = ["Organic", "Paid AD", "Owned", "Awareness", "SNS", "Other"]
    numeric_cols = [
        "sessions_cur", "transactions_cur", "revenue_cur",
        "sessions_prev", "transactions_prev", "revenue_prev",
        "sessions_yoy", "transactions_yoy", "revenue_yoy",
    ]
    if m.empty:
        m = pd.DataFrame({"bucket": canonical_buckets})
    for col in numeric_cols:
        if col not in m.columns:
            m[col] = 0.0
    missing = [b for b in canonical_buckets if b not in set(m["bucket"].tolist())]
    if missing:
        m = pd.concat([m, pd.DataFrame({"bucket": missing})], ignore_index=True, sort=False)
    m[numeric_cols] = m[numeric_cols].fillna(0.0)
    m = m.groupby("bucket", as_index=False)[numeric_cols].sum().set_index("bucket")

    non_other = [b for b in canonical_buckets if b != "Other"]
    m.loc["Other", "sessions_cur"] = max(total_sessions_cur - float(m.loc[non_other, "sessions_cur"].sum()), 0.0)
    m.loc["Other", "sessions_prev"] = max(total_sessions_prev - float(m.loc[non_other, "sessions_prev"].sum()), 0.0)
    m.loc["Other", "sessions_yoy"] = max(total_sessions_yoy - float(m.loc[non_other, "sessions_yoy"].sum()), 0.0)
    m.loc["Other", "transactions_cur"] = max(total_orders_cur - float(m.loc[non_other, "transactions_cur"].sum()), 0.0)
    m.loc["Other", "transactions_prev"] = max(total_orders_prev - float(m.loc[non_other, "transactions_prev"].sum()), 0.0)
    m.loc["Other", "transactions_yoy"] = max(total_orders_yoy - float(m.loc[non_other, "transactions_yoy"].sum()), 0.0)
    m.loc["Other", "revenue_cur"] = max(total_revenue_cur - float(m.loc[non_other, "revenue_cur"].sum()), 0.0)
    m.loc["Other", "revenue_prev"] = max(total_revenue_prev - float(m.loc[non_other, "revenue_prev"].sum()), 0.0)
    m.loc["Other", "revenue_yoy"] = max(total_revenue_yoy - float(m.loc[non_other, "revenue_yoy"].sum()), 0.0)
    m = m.reset_index()

    m["dod"] = m.apply(lambda r: pct_change(float(r["sessions_cur"]), float(r["sessions_prev"])), axis=1)
    m["yoy"] = m.apply(lambda r: pct_change(float(r["sessions_cur"]), float(r["sessions_yoy"])), axis=1)

    out = pd.DataFrame({
        "bucket": m["bucket"],
        "sessions": m["sessions_cur"],
        "transactions": m["transactions_cur"],
        "purchaseRevenue": m["revenue_cur"],
        "rev_dod": m["dod"],
        "rev_yoy": m["yoy"],
    })

    order = {"Organic": 0, "Paid AD": 1, "Owned": 2, "Awareness": 3, "SNS": 4, "Other": 5}
    out["__o"] = out["bucket"].map(order).fillna(99).astype(int)
    out = out.sort_values(["__o", "bucket"]).drop(columns="__o")

    # Total row is forced from overall KPI.
    total = pd.DataFrame([{
        "bucket": "Total",
        "sessions": total_sessions_cur,
        "transactions": total_orders_cur,
        "purchaseRevenue": total_revenue_cur,
        "rev_dod": pct_change(total_sessions_cur, total_sessions_prev),
        "rev_yoy": pct_change(total_sessions_cur, total_sessions_yoy),
    }])

    out = pd.concat([out, total], ignore_index=True)
    return out[["bucket", "sessions", "transactions", "purchaseRevenue", "rev_dod", "rev_yoy"]]


# =========================
# Paid Detail split from the same Paid AD base as Channel Snapshot
# =========================
def get_paid_detail_3way(
    client: BetaAnalyticsDataClient,
    w: DigestWindow,
    paid_ad_totals: Optional[Dict[str, Dict[str, float]]] = None,
) -> pd.DataFrame:
    """
    Keep Paid Detail Total aligned with Channel Snapshot Paid AD.
    DoD / YoY are session based.
    """
    dims = ["sessionSourceMedium", "sessionCampaignName"]
    mets = ["sessions", "purchaseRevenue"]

    def fetch(start: dt.date, end: dt.date) -> pd.DataFrame:
        try:
            df = run_report(client, PROPERTY_ID, ymd(start), ymd(end), dims, mets, limit=250000)
            if df.empty:
                return pd.DataFrame(columns=dims + mets)
            df = df.copy()
            df["sessionSourceMedium"] = df["sessionSourceMedium"].astype(str).fillna("")
            df["sessionCampaignName"] = df["sessionCampaignName"].astype(str).fillna("")
            for c in mets:
                df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0.0)
            df["bucket"] = df.apply(lambda r: classify_looker_channel(r["sessionSourceMedium"], r["sessionCampaignName"]), axis=1)
            df = df[df["bucket"] == "Paid AD"].copy()
            df["sub"] = df.apply(lambda r: classify_paid_detail(r["sessionSourceMedium"], r["sessionCampaignName"]), axis=1)
            return df
        except Exception:
            dims2 = ["sessionSourceMedium"]
            df = run_report(client, PROPERTY_ID, ymd(start), ymd(end), dims2, mets, limit=250000)
            if df.empty:
                return pd.DataFrame(columns=["sessionSourceMedium", "sessionCampaignName"] + mets + ["bucket", "sub"])
            df = df.copy()
            df["sessionSourceMedium"] = df["sessionSourceMedium"].astype(str).fillna("")
            df["sessionCampaignName"] = ""
            for c in mets:
                df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0.0)
            df["bucket"] = df.apply(lambda r: classify_looker_channel(r["sessionSourceMedium"], ""), axis=1)
            df = df[df["bucket"] == "Paid AD"].copy()
            df["sub"] = df.apply(lambda r: classify_paid_detail(r["sessionSourceMedium"], ""), axis=1)
            return df

    cur = fetch(w.cur_start, w.cur_end)
    prev = fetch(w.prev_start, w.prev_end)
    yoy = fetch(w.yoy_start, w.yoy_end)

    def agg(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=["sub", "sessions", "purchaseRevenue"])
        return df.groupby("sub", as_index=False)[["sessions", "purchaseRevenue"]].sum()

    cur_a = agg(cur).rename(columns={"sessions": "sessions_cur", "purchaseRevenue": "rev_cur"})
    prev_a = agg(prev).rename(columns={"sessions": "sessions_prev", "purchaseRevenue": "rev_prev"})
    yoy_a = agg(yoy).rename(columns={"sessions": "sessions_yoy", "purchaseRevenue": "rev_yoy_base"})
    yoy_subs = set(yoy_a["sub"].astype(str).tolist()) if not yoy_a.empty else set()

    merged = cur_a.merge(prev_a, on="sub", how="outer").merge(yoy_a, on="sub", how="outer").fillna(0.0)
    merged["dod"] = merged.apply(lambda r: pct_change(float(r["sessions_cur"]), float(r["sessions_prev"])), axis=1)
    merged["yoy"] = merged.apply(lambda r: pct_change(float(r["sessions_cur"]), float(r["sessions_yoy"])), axis=1)
    merged["has_yoy"] = merged["sub"].astype(str).isin(yoy_subs)

    # Ensure core labels always exist in the visible table.
    core = list(PAID_DETAIL_SOURCES)
    for c in core:
        if c not in set(merged["sub"].tolist()):
            merged = pd.concat([merged, pd.DataFrame([{
                "sub": c, "sessions_cur": 0.0, "rev_cur": 0.0,
                "sessions_prev": 0.0, "rev_prev": 0.0,
                "sessions_yoy": 0.0, "rev_yoy_base": 0.0,
                "dod": 0.0, "yoy": 0.0, "has_yoy": False
            }])], ignore_index=True)

    # Show core rows plus top "others", but compute Total from the full Paid AD base.
    others = merged[~merged["sub"].isin(core)].copy()
    others = others.sort_values(["sessions_cur", "rev_cur"], ascending=[False, False]).head(6)

    ordered = pd.concat([
        merged[merged["sub"].isin(core)].assign(_ord=lambda d: d["sub"].apply(lambda x: core.index(x))).sort_values("_ord"),
        others.assign(_ord=999),
    ], ignore_index=True)

    # Force Total to match Channel Snapshot Paid AD.
    # Use Channel Snapshot current values first, then fallback to merged sums.
    # If prev / yoy snapshot values are unavailable, fallback to merged sums.
    merged_cur_s = float(merged["sessions_cur"].sum()) if not merged.empty else 0.0
    merged_prev_s = float(merged["sessions_prev"].sum()) if not merged.empty else 0.0
    merged_yoy_s = float(merged["sessions_yoy"].sum()) if not merged.empty else 0.0
    merged_cur_r = float(merged["rev_cur"].sum()) if not merged.empty else 0.0

    if paid_ad_totals:
        cur_sessions_val = paid_ad_totals.get("current", {}).get("sessions", merged_cur_s)
        cur_revenue_val = paid_ad_totals.get("current", {}).get("revenue", merged_cur_r)
        prev_sessions_val = paid_ad_totals.get("prev", {}).get("sessions", None)
        yoy_sessions_val = paid_ad_totals.get("yoy", {}).get("sessions", None)

        t_cur_s = merged_cur_s if cur_sessions_val is None else float(cur_sessions_val or 0.0)
        t_cur_r = merged_cur_r if cur_revenue_val is None else float(cur_revenue_val or 0.0)
        t_prev_s = merged_prev_s if prev_sessions_val in (None, 0, 0.0) else float(prev_sessions_val)
        t_yoy_s = merged_yoy_s if yoy_sessions_val in (None, 0, 0.0) else float(yoy_sessions_val)
    else:
        t_cur_s = merged_cur_s
        t_prev_s = merged_prev_s
        t_yoy_s = merged_yoy_s
        t_cur_r = merged_cur_r

    total_row = {
        "sub_channel": "Total",
        "sessions": t_cur_s,
        "purchaseRevenue": t_cur_r,
        "dod": pct_change(t_cur_s, t_prev_s),
        "yoy": pct_change(t_cur_s, t_yoy_s),
        "has_yoy": (t_yoy_s not in (None, 0, 0.0)),
    }

    out = ordered[["sub", "sessions_cur", "rev_cur", "dod", "yoy", "has_yoy"]].copy()
    out = out.rename(columns={
        "sub": "sub_channel",
        "sessions_cur": "sessions",
        "rev_cur": "purchaseRevenue",
    })
    out = pd.concat([out, pd.DataFrame([total_row])], ignore_index=True)
    return out


# =========================
# Paid Top3 (kept)
# =========================
def get_paid_top3(client: BetaAnalyticsDataClient, w: DigestWindow) -> pd.DataFrame:
    if w.mode != "daily":
        return pd.DataFrame(columns=["sessionSourceMedium", "sessions", "purchaseRevenue"])

    dims = ["sessionSourceMedium"]
    mets = ["sessions", "purchaseRevenue"]
    # Keep the legacy fallback that maps Paid AD from sessionDefaultChannelGroup.
    filt = ga_filter_in("sessionDefaultChannelGroup", ["Paid Search", "Paid Social", "Display"])
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


# =========================
# KPI snapshot table (kept)
# =========================
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


# =========================
# Trend series (kept)
# =========================
def get_trend_view_series(client: BetaAnalyticsDataClient, w: DigestWindow) -> dict:
    end = w.cur_end
    start = end - dt.timedelta(days=29)
    df = run_report(client, PROPERTY_ID, ymd(start), ymd(end), ["date"], ["sessions","transactions","purchaseRevenue"])
    axis_dates = [start + dt.timedelta(days=i) for i in range(30)]
    x = [f"{d.strftime('%m/%d')} ({['Mon','Tue','Wed','Thu','Fri','Sat','Sun'][d.weekday()]})" for d in axis_dates]

    if df.empty:
        return {"x": x, "dates": [ymd(d) for d in axis_dates], "sessions": [0.0]*30, "revenue": [0.0]*30, "cvr": [0.0]*30}

    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")
    for c in ["sessions","transactions","purchaseRevenue"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    df["cvr"] = df.apply(lambda r: (r["transactions"]/r["sessions"]) if r["sessions"] else 0.0, axis=1)

    tmp = df.set_index(df["date"].dt.date)
    s = [float(tmp.loc[d, "sessions"]) if d in tmp.index else 0.0 for d in axis_dates]
    r = [float(tmp.loc[d, "purchaseRevenue"]) if d in tmp.index else 0.0 for d in axis_dates]
    c = [float(tmp.loc[d, "cvr"]) if d in tmp.index else 0.0 for d in axis_dates]
    return {"x": x, "dates": [ymd(d) for d in axis_dates], "sessions": s, "revenue": r, "cvr": c}

def combined_index_svg_monthly(
    axis_dates: List[dt.date],
    xlabels: List[str],
    series: List[List[float]],
    colors: List[str],
    labels: List[str],
    footer_label: str = "Index",
    width=980, height=280,
    pad_l=46, pad_r=16, pad_t=18, pad_b=62,
) -> str:
    n = len(xlabels)
    if n == 0:
        return combined_index_svg(["--"], [[100],[100],[100]], colors, labels)
    allv = [v for s in series for v in s] or [0,1]
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

    grid, ylabels_svg = [], []
    for t in range(6):
        frac = t / 5
        y = pad_t + inner_h * (1 - frac)
        val = y_min2 + (y_max2 - y_min2) * frac
        grid.append(f"<line x1='{pad_l}' y1='{y:.1f}' x2='{width-pad_r}' y2='{y:.1f}' stroke='#eef2ff' stroke-width='1'/>")
        ylabels_svg.append(f"<text x='{pad_l-8}' y='{y+3:.1f}' text-anchor='end' font-size='10' fill='#6b7280'>{val:.0f}</text>")

    weekend_marks = []
    xlabels_svg = []
    for i, (d, lab) in enumerate(zip(axis_dates, xlabels)):
        x = pad_l + inner_w * (i / (n - 1 if n > 1 else 1))
        wk = d.weekday()
        color = '#6b7280'
        if wk == 5:
            color = '#2563eb'
        elif wk == 6:
            color = '#dc2626'
        if i < n - 1:
            nx = pad_l + inner_w * ((i+1) / (n - 1 if n > 1 else 1))
            if wk >= 5:
                weekend_marks.append(f"<rect x='{x:.1f}' y='{pad_t}' width='{max(1.0, nx-x):.1f}' height='{inner_h:.1f}' fill='{ '#eff6ff' if wk==5 else '#fef2f2' }' opacity='0.35'/>")
        text = d.strftime('%m/%d')
        dow = ['M','T','W','T','F','S','S'][wk]
        xlabels_svg.append(f"<text x='{x:.1f}' y='{height-pad_b+18}' text-anchor='middle' font-size='9' fill='{color}'>{text}</text>")
        xlabels_svg.append(f"<text x='{x:.1f}' y='{height-pad_b+31}' text-anchor='middle' font-size='9' fill='{color}'>{dow}</text>")

    axes = f"<line x1='{pad_l}' y1='{pad_t}' x2='{pad_l}' y2='{height-pad_b}' stroke='#c7d2fe' stroke-width='1'/><line x1='{pad_l}' y1='{height-pad_b}' x2='{width-pad_r}' y2='{height-pad_b}' stroke='#c7d2fe' stroke-width='1'/>"
    polys, dots = [], []
    for sidx, s in enumerate(series):
        pts = [xy(i, v) for i, v in enumerate(s)]
        poly = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
        color = colors[sidx]
        polys.append(f"<polyline fill='none' stroke='{color}' stroke-width='2.3' points='{poly}'/>")
        dots.append("".join([f"<circle cx='{x:.1f}' cy='{y:.1f}' r='2.3' fill='{color}'/>" for x, y in pts]))
    legend_items = []
    for i, lab in enumerate(labels):
        legend_items.append(f"<g transform='translate({pad_l + i*165},8)'><line x1='0' y1='8' x2='18' y2='8' stroke='{colors[i]}' stroke-width='3'/><text x='26' y='11' font-size='11' fill='#334155' style='font-weight:600'>{lab}</text></g>")
    return f"<svg width='100%' viewBox='0 0 {width} {height}' preserveAspectRatio='none' style='display:block;'>{''.join(weekend_marks)}{''.join(grid)}{axes}{''.join(polys)}{''.join(dots)}{''.join(ylabels_svg)}{''.join(xlabels_svg)}<text x='{pad_l}' y='{height-8}' font-size='10' fill='#94a3b8'>{footer_label}</text>{''.join(legend_items)}</svg>"

def trend_svg_from_series(series: dict, days: int = 30) -> str:
    raw_dates = [parse_yyyy_mm_dd(d) for d in series.get("dates", [])]
    raw_sessions = [float(v) for v in series.get("sessions", [])]
    raw_revenue = [float(v) for v in series.get("revenue", [])]
    raw_cvr = [float(v) for v in series.get("cvr", [])]
    available = min(len(raw_dates), len(raw_sessions), len(raw_revenue), len(raw_cvr))
    days = max(1, int(days or 30))
    if available <= 0 or any(d is None for d in raw_dates[:available]):
        fallback_dates = [dt.date.today() - dt.timedelta(days=days - 1 - i) for i in range(days)]
        base = [[100] * days, [100] * days, [100] * days]
        return combined_index_svg_monthly(
            fallback_dates,
            [d.strftime('%m/%d') for d in fallback_dates],
            base,
            ["#0055a5", "#16a34a", "#c2410c"],
            ["Sessions", "Revenue", "CVR"],
            footer_label=f"Index (D-{days} start = 100)",
        )

    use_days = min(days, available)
    axis_dates = [d for d in raw_dates[available - use_days:available] if d is not None]
    sessions = raw_sessions[available - use_days:available]
    revenue = raw_revenue[available - use_days:available]
    cvr = raw_cvr[available - use_days:available]
    return combined_index_svg_monthly(
        axis_dates,
        [d.strftime('%m/%d') for d in axis_dates],
        [index_series(sessions), index_series(revenue), index_series(cvr)],
        ["#0055a5", "#16a34a", "#c2410c"],
        ["Sessions", "Revenue", "CVR"],
        footer_label=f"Index (D-{use_days} start = 100)",
    )

def build_trend_svg_map(series: dict) -> Dict[str, str]:
    return {
        "7d": trend_svg_from_series(series, days=7),
        "14d": trend_svg_from_series(series, days=14),
        "1m": trend_svg_from_series(series, days=30),
    }


# =========================
# Best Sellers + trends per SKU (kept)
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
        return pd.DataFrame(columns=["itemId","itemName","qty","views","trend_svg","image_url"]), {"x": [], "items": []}

    cand["itemsPurchased"] = pd.to_numeric(cand["itemsPurchased"], errors="coerce").fillna(0.0)
    cand["itemId"] = cand["itemId"].map(normalize_sku_key)
    cand["itemName"] = cand["itemName"].astype(str).fillna("").map(lambda x: x.strip())
    cand = cand[~cand["itemName"].map(is_not_set)]
    if cand.empty:
        return pd.DataFrame(columns=["itemId","itemName","qty","views","trend_svg","image_url"]), {"x": [], "items": []}

    cand["image_url"] = cand["itemId"].map(lambda s: image_map.get(normalize_sku_key(s), ""))
    with_img = cand[cand["image_url"].astype(str).str.strip() != ""].copy().sort_values("itemsPurchased", ascending=False)
    no_img = cand[cand["image_url"].astype(str).str.strip() == ""].copy().sort_values("itemsPurchased", ascending=False)
    top = pd.concat([with_img.head(5), no_img.head(max(0, 5 - len(with_img.head(5))))], ignore_index=True).head(5).copy()
    top["qty"] = top["itemsPurchased"]
    skus = [str(s).strip() for s in top["itemId"].tolist() if str(s).strip()]

    # Fetch views for the Best Sellers cards.
    views_df = _get_item_views_best_effort(client, start, end, skus)
    if views_df.empty:
        top["views"] = 0.0
    else:
        views_df["itemId"] = views_df["itemId"].map(normalize_sku_key)
        views_df["views"] = pd.to_numeric(views_df["views"], errors="coerce").fillna(0.0)
        top = top.merge(views_df[["itemId", "views"]], on="itemId", how="left")
        top["views"] = pd.to_numeric(top["views"], errors="coerce").fillna(0.0)

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
        return top[["itemId","itemName","qty","views","trend_svg","image_url"]], series_cache

    ts["date"] = ts["date"].apply(parse_yyyymmdd)
    ts["itemsPurchased"] = pd.to_numeric(ts["itemsPurchased"], errors="coerce").fillna(0.0)
    ts["itemId"] = ts["itemId"].map(normalize_sku_key)
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

    return top[["itemId","itemName","qty","views","trend_svg","image_url"]], series_cache


# =========================
# Rising Products
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

def get_rising_products(
    client: BetaAnalyticsDataClient,
    w: DigestWindow,
    top_n: int = 5,
    exclude_skus: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Rising products are items with positive quantity growth vs the previous period.
    Items with prev == 0 are excluded, and Best Sellers SKUs are also excluded.
    """
    exclude = set([str(x).strip() for x in (exclude_skus or []) if str(x).strip()])
    cur_start, cur_end = (w.cur_end, w.cur_end) if w.mode == "daily" else (w.cur_start, w.cur_end)
    prev_start, prev_end = (w.prev_end, w.prev_end) if w.mode == "daily" else (w.prev_start, w.prev_end)

    d1_qty = run_report(client, PROPERTY_ID, ymd(cur_start), ymd(cur_end), ["itemId", "itemName"], ["itemsPurchased"], limit=10000)
    d0_qty = run_report(client, PROPERTY_ID, ymd(prev_start), ymd(prev_end), ["itemId"], ["itemsPurchased"], limit=10000)

    if d1_qty.empty:
        return pd.DataFrame(columns=["itemId", "itemName", "qty", "views", "revenue", "delta", "delta_label", "image_url"])

    d1_qty["itemsPurchased"] = pd.to_numeric(d1_qty["itemsPurchased"], errors="coerce").fillna(0.0)
    d1_qty["itemId"] = d1_qty["itemId"].astype(str).str.strip()
    d1_qty["itemName"] = d1_qty["itemName"].astype(str).fillna("").map(lambda x: x.strip())
    d1_qty = d1_qty[~d1_qty["itemName"].map(is_not_set)]
    if d1_qty.empty:
        return pd.DataFrame(columns=["itemId", "itemName", "qty", "views", "revenue", "delta", "delta_label", "image_url"])

    if not d0_qty.empty:
        d0_qty["itemsPurchased"] = pd.to_numeric(d0_qty["itemsPurchased"], errors="coerce").fillna(0.0)
        d0_qty["itemId"] = d0_qty["itemId"].astype(str).str.strip()
    else:
        d0_qty = pd.DataFrame(columns=["itemId", "itemsPurchased"])

    qty_m = d1_qty.merge(d0_qty, on="itemId", how="left", suffixes=("_cur", "_prev")).fillna(0.0)
    qty_m["qty"] = qty_m["itemsPurchased_cur"]
    qty_m["qty_prev"] = qty_m["itemsPurchased_prev"]
    qty_m["qty_delta"] = qty_m["qty"] - qty_m["qty_prev"]

    # Exclude best sellers and keep only prev > 0 with positive delta.
    qty_m = qty_m[~qty_m["itemId"].isin(exclude)].copy()
    qty_m = qty_m[(qty_m["qty_prev"] > 0) & (qty_m["qty_delta"] > 0)].copy()
    if qty_m.empty:
        return pd.DataFrame(columns=["itemId", "itemName", "qty", "views", "revenue", "delta", "delta_label", "image_url"])

    skus = [str(x).strip() for x in qty_m["itemId"].tolist() if str(x).strip()]

    v_cur = _get_item_views_best_effort(client, cur_start, cur_end, skus).rename(columns={"views": "views_cur"}) if skus else pd.DataFrame(columns=["itemId","views_cur"])
    v_prev = _get_item_views_best_effort(client, prev_start, prev_end, skus).rename(columns={"views": "views_prev"}) if skus else pd.DataFrame(columns=["itemId","views_prev"])
    if v_cur.empty:
        v_cur = pd.DataFrame({"itemId": skus, "views_cur": [0.0]*len(skus)})
    if v_prev.empty:
        v_prev = pd.DataFrame({"itemId": skus, "views_prev": [0.0]*len(skus)})

    r_cur = _get_item_revenue_best_effort(client, cur_start, cur_end)
    r_prev = _get_item_revenue_best_effort(client, prev_start, prev_end)
    r_cur = r_cur[["itemId", "revenue"]].rename(columns={"revenue": "revenue_cur"}) if not r_cur.empty else pd.DataFrame({"itemId": skus, "revenue_cur": [0.0]*len(skus)})
    r_prev = r_prev[["itemId", "revenue"]].rename(columns={"revenue": "revenue_prev"}) if not r_prev.empty else pd.DataFrame({"itemId": skus, "revenue_prev": [0.0]*len(skus)})

    m = qty_m.merge(v_cur, on="itemId", how="left").merge(v_prev, on="itemId", how="left").merge(r_cur, on="itemId", how="left").merge(r_prev, on="itemId", how="left")
    for c in ("views_cur", "views_prev", "revenue_cur", "revenue_prev"):
        m[c] = pd.to_numeric(m.get(c), errors="coerce").fillna(0.0)

    m["views"] = m["views_cur"]
    m["revenue"] = m["revenue_cur"]
    m["views_delta"] = m["views_cur"] - m["views_prev"]
    m["revenue_delta"] = m["revenue_cur"] - m["revenue_prev"]

    if RISING_BASIS == "views":
        m["delta"] = m["views_delta"]
        m["delta_label"] = "Views ?"
    elif RISING_BASIS == "revenue":
        m["delta"] = m["revenue_delta"]
        m["delta_label"] = "Revenue ?"
    else:
        m["delta"] = m["qty_delta"]
        m["delta_label"] = "Qty ?"

    m = m.sort_values("delta", ascending=False).head(top_n).copy()
    m["image_url"] = ""  # Filled later by attach_image_urls().
    return m[["itemId", "itemName", "qty", "views", "revenue", "delta", "delta_label", "image_url"]]


# =========================
# PDP Trend (BigQuery) + file cache (kept)
# =========================
PDP_CATEGORY_MAP = {
    "OUTER": {
        "Padding/Slim Down": ["Padding/Slim Down"],
        "Mid/Heavy Down": ["Mid/Heavy Down"],
        "Interchange": ["Interchange (3 in 1)"],
        "Rain": ["Rain"],
    },
    "FLEECE": {
        "Fleece Pullover": ["Fleece pullover"],
        "Jacket": ["Jacket"],
    },
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

def pdp_cache_path(end_date: dt.date) -> str:
    return os.path.join(OUT_DIR, "cache", "pdp", f"{ymd(end_date)}.json")

def get_category_pdp_view_trend_bq(end_date: dt.date) -> Tuple[pd.DataFrame, dict]:
    axis_dates = [end_date - dt.timedelta(days=i) for i in range(6, -1, -1)]
    xlabels = [d.strftime('%m/%d') for d in axis_dates]

    if bigquery is None or not BQ_EVENTS_TABLE:
        return pd.DataFrame(columns=["itemCategory", "views_d1", "views_avg7d", "trend_svg"]), {}

    try:
        bq = bigquery.Client()

        start_suffix = (end_date - dt.timedelta(days=6)).strftime('%Y%m%d')
        end_suffix = end_date.strftime('%Y%m%d')
        lookup_start = (end_date - dt.timedelta(days=30)).strftime('%Y%m%d')
        lookup_end = end_date.strftime('%Y%m%d')

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
            return pd.DataFrame(columns=["itemCategory", "views_d1", "views_avg7d", "trend_svg"]), {}

        df['d'] = pd.to_datetime(df['d'], errors='coerce').dt.date
        df = df.dropna(subset=['d'])
        df['c1'] = df['c1'].astype(str).str.strip().str.upper()
        df['c2'] = df['c2'].astype(str).str.strip()
        df['views'] = pd.to_numeric(df['views'], errors='coerce').fillna(0.0)

        rows = []
        for c1, subs in PDP_CATEGORY_MAP.items():
            for sub_label, c2_list in subs.items():
                ys = []
                for d in axis_dates:
                    m = (df['d'] == d) & (df['c1'] == c1)
                    if c2_list:
                        m = m & (df['c2'].isin(c2_list))
                    else:
                        m = m & (df['c2'] == sub_label)
                    ys.append(float(df.loc[m, 'views'].sum()))

                d1 = ys[-1] if ys else 0.0
                avg7 = (sum(ys) / len(ys)) if ys else 0.0
                rows.append({
                    'itemCategory': f"{c1} · {sub_label}",
                    'views_d1': float(d1),
                    'views_avg7d': float(avg7),
                    'trend_svg': spark_svg(xlabels, ys, width=260, height=70, stroke="#0f766e"),
                })

        pdp_series = {
            "x": xlabels,
            "rows": [
                {
                    "itemCategory": r["itemCategory"],
                    "views_d1": r["views_d1"],
                    "views_avg7d": r["views_avg7d"],
                    "ys": []
                } for r in rows
            ]
        }

        return pd.DataFrame(rows), pdp_series

    except Exception as e:
        print(f"[WARN] PDP Category Trend BigQuery failed: {type(e).__name__}: {e}")
        return pd.DataFrame(columns=["itemCategory", "views_d1", "views_avg7d", "trend_svg"]), {}


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

    new_terms = merged[merged["prior_avg"] == 0].head(10)[["searchTerm","count"]].copy()

    rising = merged[merged["prior_avg"] > 0].copy()
    rising["pct"] = (rising["count"] - rising["prior_avg"]) / rising["prior_avg"] * 100.0
    rising = rising.replace([float("inf"), -float("inf")], 0.0)
    rising = rising.sort_values("pct", ascending=False).head(10)[["searchTerm","pct","count"]]
    return {"new": new_terms, "rising": rising}




def safe_read_excel(path: str) -> pd.DataFrame:
    try:
        if path and os.path.exists(path):
            return pd.read_excel(path)
    except Exception as e:
        print(f"[WARN] Excel read failed: {path} | {type(e).__name__}: {e}")
    return pd.DataFrame()

def safe_read_table(path: str) -> pd.DataFrame:
    try:
        if not path or not os.path.exists(path):
            return pd.DataFrame()
        ext = os.path.splitext(path)[1].lower()
        if ext == ".csv":
            return pd.read_csv(path)
        return pd.read_excel(path)
    except Exception as e:
        print(f"[WARN] Table read failed: {path} | {type(e).__name__}: {e}")
        return pd.DataFrame()

def normalize_media_channel(value: str) -> str:
    s = str(value or "").strip().lower()
    if not s:
        return ""
    if "google" in s or "구글" in s:
        return "google"
    if "meta" in s or "메타" in s or "instagram" in s or "인스타" in s:
        return "meta"
    if "naver" in s or "네이버" in s or "브랜드검색" in s or "쇼검" in s or "쇼핑검색" in s:
        return "naver"
    return s

def classify_glink_spend_sub(media: Any, campaign_type: Any = "", device: Any = "", segment: Any = "") -> str:
    m = str(media or "").strip().lower()
    ct = str(campaign_type or "").strip().lower()
    dv = str(device or "").strip().lower()
    sg = str(segment or "").strip().lower()
    joined = " ".join([m, ct, dv, sg])

    def has(p: str) -> bool:
        return re.search(p, joined, re.IGNORECASE) is not None

    if has(r"criteo"):
        return "criteo"
    if has(r"(instagram|인스타)"):
        return "instagram"
    if has(r"(meta|메타|facebook|페이스북)"):
        return "meta"

    if has(r"(google|구글)"):
        if has(r"(demand|dg|demand[\s_-]*gen)"):
            return "google demand gen"
        if has(r"pmax"):
            return "google pmax"
        if has(r"(^|[^a-z])(sa|ss|search)([^a-z]|$)|검색"):
            return "google search"
        return "google"

    if has(r"(naver|네이버)"):
        if has(r"(brandsearch|brand search|브랜드검색|naverbs)"):
            return "naver brand search"
        if has(r"(naversa|search|검색|쇼핑검색|쇼검)"):
            return "naver search"
        return "naver search"

    return normalize_media_channel(m)

def list_glink_vendor_files(source_dir: str) -> List[str]:
    if not source_dir or not os.path.isdir(source_dir):
        return []
    pattern = re.compile(r"^\(glink\)\s*columbia_.*report_\d{6,8}\.(xlsx|xlsm|xls)$", re.IGNORECASE)
    out: List[str] = []
    for name in os.listdir(source_dir):
        low = name.lower()
        if not os.path.isfile(os.path.join(source_dir, name)):
            continue
        if not low.endswith((".xlsx", ".xlsm", ".xls")):
            continue
        if low.startswith("~$"):
            continue
        if pattern.match(low):
            out.append(os.path.join(source_dir, name))
    return sorted(out)

def find_header_row(df: pd.DataFrame, required_labels: List[str], scan_rows: int = 20) -> Optional[int]:
    max_rows = min(scan_rows, len(df.index))
    required = [str(x).strip() for x in required_labels]
    for idx in range(max_rows):
        vals = [str(v).replace("\n", "").replace("\r", "").strip() for v in df.iloc[idx].tolist()]
        if all(label in vals for label in required):
            return idx
    return None

def extract_glink_spend_history_rows(xlsx_path: str) -> pd.DataFrame:
    try:
        raw = pd.read_excel(xlsx_path, sheet_name="매체RAW", header=None)
    except Exception as e:
        print(f"[WARN] Glink spend parse failed: {xlsx_path} | {type(e).__name__}: {e}")
        return pd.DataFrame(columns=["date", "channel", "sub_channel", "spend", "source_file", "source_mtime"])

    header_idx = find_header_row(raw, ["매체", "날짜최종", "총비용(vat제외)"])
    if header_idx is None:
        print(f"[WARN] Could not find spend header row in: {xlsx_path}")
        return pd.DataFrame(columns=["date", "channel", "sub_channel", "spend", "source_file", "source_mtime"])

    header_vals = []
    for i, v in enumerate(raw.iloc[header_idx].tolist()):
        label = str(v).replace("\n", "").replace("\r", "").strip()
        header_vals.append(label or f"col_{i}")

    df = raw.iloc[header_idx + 1 :].copy()
    df.columns = header_vals
    cols = {str(c).strip(): c for c in df.columns}
    date_col = cols.get("날짜최종") or cols.get("기간")
    media_col = cols.get("매체") or cols.get("채널")
    campaign_type_col = cols.get("캠페인유형") or cols.get("campaign_type")
    device_col = cols.get("기기") or cols.get("device")
    segment_col = cols.get("구분") or cols.get("segment")
    spend_col = cols.get("총비용(vat제외)") or cols.get("광고비(vat제외)") or cols.get("광고비(vat-)") or cols.get("광고비")
    if not date_col or not media_col or not spend_col:
        print(f"[WARN] Missing spend columns in: {xlsx_path}")
        return pd.DataFrame(columns=["date", "channel", "sub_channel", "spend", "source_file", "source_mtime"])

    keep_cols = [date_col, media_col, spend_col]
    if campaign_type_col:
        keep_cols.append(campaign_type_col)
    if device_col:
        keep_cols.append(device_col)
    if segment_col:
        keep_cols.append(segment_col)

    tmp = df[keep_cols].copy()
    rename_map = {
        date_col: "date",
        media_col: "media",
        spend_col: "spend",
    }
    if campaign_type_col:
        rename_map[campaign_type_col] = "campaign_type"
    if device_col:
        rename_map[device_col] = "device"
    if segment_col:
        rename_map[segment_col] = "segment"
    tmp = tmp.rename(columns=rename_map)
    tmp["date"] = pd.to_datetime(tmp["date"], errors="coerce").dt.date
    tmp["spend"] = pd.to_numeric(tmp["spend"].astype(str).str.replace(",", "", regex=False).str.strip(), errors="coerce").fillna(0.0)
    tmp["channel"] = tmp["media"].map(normalize_media_channel)
    tmp["sub_channel"] = tmp.apply(
        lambda r: classify_glink_spend_sub(
            r.get("media", ""),
            r.get("campaign_type", ""),
            r.get("device", ""),
            r.get("segment", ""),
        ),
        axis=1,
    )
    tmp = tmp[tmp["date"].notna()].copy()
    tmp = tmp[tmp["channel"].isin(["google", "naver", "meta"])].copy()
    if tmp.empty:
        return pd.DataFrame(columns=["date", "channel", "sub_channel", "spend", "source_file", "source_mtime"])

    out = tmp.groupby(["date", "channel", "sub_channel"], as_index=False)["spend"].sum()
    out["source_file"] = os.path.basename(xlsx_path)
    out["source_mtime"] = os.path.getmtime(xlsx_path)
    return out[["date", "channel", "sub_channel", "spend", "source_file", "source_mtime"]]

def refresh_glink_spend_history(source_dir: str, out_path: str) -> pd.DataFrame:
    files = list_glink_vendor_files(source_dir)
    if not files:
        return pd.DataFrame(columns=["date", "channel", "sub_channel", "spend"])

    frames: List[pd.DataFrame] = []
    for path in files:
        rows = extract_glink_spend_history_rows(path)
        if not rows.empty:
            frames.append(rows)
    if not frames:
        return pd.DataFrame(columns=["date", "channel", "sub_channel", "spend"])

    merged = pd.concat(frames, ignore_index=True)
    merged = merged.sort_values(["date", "channel", "sub_channel", "source_mtime", "source_file"])
    merged = merged.drop_duplicates(subset=["date", "channel", "sub_channel"], keep="last")
    merged = merged.sort_values(["date", "channel", "sub_channel"]).reset_index(drop=True)

    out = merged[["date", "channel", "sub_channel", "spend"]].copy()
    try:
        ensure_dir(os.path.dirname(out_path))
        out.to_csv(out_path, index=False, encoding="utf-8-sig")
    except Exception as e:
        print(f"[WARN] Could not write spend history: {out_path} | {type(e).__name__}: {e}")
    return out

def load_target_roas_map(xlsx_path: str) -> Dict[str, float]:
    df = safe_read_table(xlsx_path)
    if df.empty:
        return {}
    cols = {str(c).strip().lower(): c for c in df.columns}
    ch_col = cols.get('channel') or cols.get('media') or cols.get('매체') or cols.get('채널')
    tr_col = cols.get('target_roas') or cols.get('target roas') or cols.get('target') or cols.get('목표 roas') or cols.get('목표roas')
    if not ch_col or not tr_col:
        return {}
    out = {}
    for _, r in df.iterrows():
        ch = str(r.get(ch_col, '') or '').strip().lower()
        if not ch:
            continue
        try:
            val = float(r.get(tr_col, 0) or 0)
        except Exception:
            continue
        if val > 10:  # permit 300 for 300%
            val = val / 100.0
        out[ch] = val
    return out

def load_manual_spend_map(xlsx_path: str, start: dt.date, end: dt.date, group_key: str = "channel") -> Dict[str, float]:
    df = refresh_glink_spend_history(MEDIA_SPEND_VENDOR_DIR, MEDIA_SPEND_HISTORY_PATH)
    if df.empty:
        history_df = safe_read_table(MEDIA_SPEND_HISTORY_PATH)
        if not history_df.empty:
            df = history_df
    if df.empty:
        df = safe_read_table(xlsx_path)
    if df.empty:
        return {}
    cols = {str(c).strip().lower(): c for c in df.columns}
    if group_key == "sub_channel":
        ch_col = cols.get('sub_channel') or cols.get('sub') or cols.get('sub channel')
    else:
        ch_col = cols.get('channel') or cols.get('media') or cols.get('매체') or cols.get('채널')
    spend_col = cols.get('spend') or cols.get('budget') or cols.get('cost')
    date_col = cols.get('date') or cols.get('일자')
    year_col = cols.get('year') or cols.get('연도')
    out = {}
    if not ch_col or not spend_col:
        return out
    tmp = df.copy()
    if date_col and date_col in tmp.columns:
        tmp[date_col] = pd.to_datetime(tmp[date_col], errors='coerce').dt.date
        tmp = tmp[(tmp[date_col] >= start) & (tmp[date_col] <= end)]
    elif year_col and year_col in tmp.columns:
        tmp = tmp[pd.to_numeric(tmp[year_col], errors='coerce').fillna(0).astype(int) == start.year]
    for _, r in tmp.iterrows():
        raw_key = str(r.get(ch_col, '') or '').strip().lower()
        ch = raw_key if group_key == "sub_channel" else normalize_media_channel(raw_key)
        if not ch:
            continue
        out[ch] = out.get(ch, 0.0) + float(r.get(spend_col, 0) or 0)
    return out

def map_sub_to_media(sub: str) -> str:
    sub = (sub or '').strip().lower()
    if sub in ('google', 'google demand gen', 'google pmax', 'google search'):
        return 'google'
    if sub in ('meta', 'instagram'):
        return 'meta'
    if sub in ('naver brand search', 'naver search'):
        return 'naver'
    return sub

def fetch_platform_spend_map(start: dt.date, end: dt.date) -> Dict[str, float]:
    manual = load_manual_spend_map(MEDIA_SPEND_XLS_PATH, start, end)
    return {k: float(v or 0) for k, v in manual.items()}

def get_other_detail_3way(client: BetaAnalyticsDataClient, w: DigestWindow) -> pd.DataFrame:
    dims = ["sessionSourceMedium", "sessionCampaignName"]
    mets = ["sessions", "transactions", "purchaseRevenue"]
    def fetch(start: dt.date, end: dt.date) -> pd.DataFrame:
        try:
            df = run_report(client, PROPERTY_ID, ymd(start), ymd(end), dims, mets, limit=250000)
        except Exception:
            df = run_report(client, PROPERTY_ID, ymd(start), ymd(end), ["sessionSourceMedium"], mets, limit=250000)
            df['sessionCampaignName'] = ''
        if df.empty:
            return pd.DataFrame(columns=dims + mets)
        for c in mets:
            df[c] = pd.to_numeric(df.get(c, 0), errors='coerce').fillna(0.0)
        df['bucket'] = df.apply(lambda r: classify_looker_channel(str(r.get('sessionSourceMedium','')), str(r.get('sessionCampaignName',''))), axis=1)
        df = df[df['bucket'] == 'Other'].copy()
        df['sub'] = df['sessionSourceMedium'].astype(str).str.strip().replace('', 'other')
        return df
    cur = fetch(w.cur_start, w.cur_end)
    prev = fetch(w.prev_start, w.prev_end)
    yoy = fetch(w.yoy_start, w.yoy_end)
    def agg(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=['sub', f'sessions{suffix}', f'transactions{suffix}', f'revenue{suffix}'])
        g = df.groupby('sub', as_index=False)[['sessions','transactions','purchaseRevenue']].sum()
        return g.rename(columns={'sessions':f'sessions{suffix}','transactions':f'transactions{suffix}','purchaseRevenue':f'revenue{suffix}'})
    m = agg(cur,'_cur').merge(agg(prev,'_prev'), on='sub', how='outer').merge(agg(yoy,'_yoy'), on='sub', how='outer').fillna(0.0)
    if m.empty:
        return pd.DataFrame(columns=['sub_channel','sessions','orders','purchaseRevenue','dod','yoy'])
    m['dod'] = m.apply(lambda r: pct_change(float(r['sessions_cur']), float(r['sessions_prev'])), axis=1)
    m['yoy'] = m.apply(lambda r: pct_change(float(r['sessions_cur']), float(r['sessions_yoy'])), axis=1)
    out = m.rename(columns={'sub':'sub_channel','sessions_cur':'sessions','transactions_cur':'orders','revenue_cur':'purchaseRevenue'})[['sub_channel','sessions','orders','purchaseRevenue','dod','yoy']]
    out = out.sort_values(['sessions','purchaseRevenue'], ascending=[False,False]).head(12)
    return out

def get_paid_media_comparison_table(client: BetaAnalyticsDataClient, w: DigestWindow, target_roas_map: Dict[str, float]) -> pd.DataFrame:
    dims = ["sessionSourceMedium", "sessionCampaignName"]
    mets = ["sessions", "transactions", "purchaseRevenue"]
    def fetch(start: dt.date, end: dt.date) -> pd.DataFrame:
        try:
            df = run_report(client, PROPERTY_ID, ymd(start), ymd(end), dims, mets, limit=250000)
        except Exception:
            df = run_report(client, PROPERTY_ID, ymd(start), ymd(end), ["sessionSourceMedium"], mets, limit=250000)
            df['sessionCampaignName'] = ''
        if df.empty:
            return pd.DataFrame(columns=dims + mets)
        for c in mets:
            df[c] = pd.to_numeric(df.get(c, 0), errors='coerce').fillna(0.0)
        df['bucket'] = df.apply(lambda r: classify_looker_channel(str(r.get('sessionSourceMedium','')), str(r.get('sessionCampaignName',''))), axis=1)
        df = df[df['bucket'] == 'Paid AD'].copy()
        df['sub'] = df.apply(lambda r: classify_paid_detail(str(r.get('sessionSourceMedium','')), str(r.get('sessionCampaignName',''))), axis=1)
        return df
    cur = fetch(w.cur_start, w.cur_end)
    def agg(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=['sub',f'sessions{suffix}',f'orders{suffix}',f'revenue{suffix}'])
        g = df.groupby('sub', as_index=False)[['sessions','transactions','purchaseRevenue']].sum()
        return g.rename(columns={'sessions':f'sessions{suffix}','transactions':f'orders{suffix}','purchaseRevenue':f'revenue{suffix}'})
    merged = agg(cur,'_cur').fillna(0.0)
    spend_cur = load_manual_spend_map(MEDIA_SPEND_XLS_PATH, w.cur_start, w.cur_end, group_key="sub_channel")
    if spend_cur:
        top_level_spend = {}
        for sub_key, spend in spend_cur.items():
            media_key = map_sub_to_media(sub_key)
            top_level_spend[media_key] = top_level_spend.get(media_key, 0.0) + float(spend or 0.0)
        for media_key, spend in top_level_spend.items():
            if media_key not in spend_cur:
                spend_cur[media_key] = spend

    subs = list(PAID_DETAIL_SOURCES)
    extras = []
    merged_subs = merged["sub"].astype(str).tolist() if not merged.empty else []
    for key in merged_subs + list(spend_cur.keys()):
        k = str(key or "").strip()
        if k and k not in subs and k not in extras:
            extras.append(k)
    subs.extend(extras)

    rows=[]
    for sub in subs:
        row = merged[merged['sub']==sub]
        if row.empty:
            rc = {'sessions_cur':0.0,'orders_cur':0.0,'revenue_cur':0.0}
        else:
            rr = row.iloc[0]
            rc = {k: float(rr.get(k,0) or 0) for k in ['sessions_cur','orders_cur','revenue_cur']}
        cur_spend = float(spend_cur.get(sub,0) or 0)
        cur_roas = (rc['revenue_cur']/cur_spend) if cur_spend else 0.0
        cur_cvr = (rc['orders_cur']/rc['sessions_cur']) if rc['sessions_cur'] else 0.0
        media = map_sub_to_media(sub)
        rows.append({
            'channel': sub,
            'target_roas': float(
                target_roas_map.get(sub, target_roas_map.get(media, target_roas_map.get(media.title().lower(), 0.0))) or 0
            ),
            'budget': cur_spend,
            'roas': cur_roas,
            'cvr': cur_cvr,
        })
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out = out[(out["budget"] != 0) | (out["roas"] != 0) | (out["cvr"] != 0)].copy()
    return out

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
    other_detail: pd.DataFrame,
    paid_media_compare: pd.DataFrame,
) -> dict:
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
        "other_detail": to_records(other_detail),
        "paid_media_compare": to_records(paid_media_compare),
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
    page_trend_svgs = build_trend_svg_map(trend_series)

    bs_base = pd.DataFrame(bundle.get("best_sellers", []))
    bs_series = bundle.get("best_sellers_series", {"x": [], "items": []})
    x = bs_series.get("x", [])
    items_map = {it.get("itemId"): it.get("ys", [0.0]*7) for it in bs_series.get("items", []) if it.get("itemId")}
    bs_trend_svgs = []
    for _, r in bs_base.iterrows():
        sku = str(r.get("itemId", "")).strip()
        ys = items_map.get(sku, [0.0]*7)
        bs_trend_svgs.append(spark_svg(x or ["--"]*7, ys, width=240, height=70, stroke="#0055a5"))
    if not bs_base.empty:
        bs_base["trend_svg"] = bs_trend_svgs
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
    other_detail = pd.DataFrame(bundle.get("other_detail", []))
    paid_media_compare = pd.DataFrame(bundle.get("paid_media_compare", []))

    return {
        "w": w,
        "overall": overall,
        "signup_users": signup_users,
        "channel_snapshot": channel_snapshot,
        "paid_detail": paid_detail,
        "paid_top3": paid_top3,
        "kpi_snapshot": kpi_snapshot,
        "trend_svgs": page_trend_svgs,
        "best_sellers": bs_base,
        "rising": rising,
        "category_pdp_trend": category_pdp_trend,
        "search_new": search_new,
        "search_rising": search_rising,
        "other_detail": other_detail,
        "paid_media_compare": paid_media_compare,
    }


# =========================
# UI renderer
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
    trend_svgs: Dict[str, str],
    best_sellers: pd.DataFrame,
    rising: pd.DataFrame,
    category_pdp_trend: pd.DataFrame,
    search_new: pd.DataFrame,
    search_rising: pd.DataFrame,
    other_detail: pd.DataFrame,
    paid_media_compare: pd.DataFrame,
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

    # Slightly tighter padding to protect the last columns from clipping.
    def table_row(cols: List[str], bold=False, row_class: str = "") -> str:
        fw = "font-extrabold" if bold else "font-medium"
        bg = "bg-slate-50" if bold else ""
        tds = ""
        for i, c in enumerate(cols):
            extra = " pr-4" if i == (len(cols) - 1) else ""
            tds += f"<td class='px-2 py-2 border-b border-slate-100 whitespace-nowrap {fw}{extra}'>{c}</td>"
        return f"<tr class='{bg} {row_class}'>{tds}</tr>"

    # Channel Snapshot rows
    chan_html = ""
    if channel_snapshot is not None and (not channel_snapshot.empty):
        for r in channel_snapshot.itertuples(index=False):
            chan_html += table_row([
                esc(getattr(r, "bucket", "")),
                f"<div class='text-right'>{fmt_int(getattr(r, 'sessions', 0))}</div>",
                f"<div class='text-right'>{fmt_int(getattr(r, 'transactions', 0))}</div>",
                f"<div class='text-right'>{fmt_currency_krw(getattr(r, 'purchaseRevenue', 0))}</div>",
                f"<div class='text-right {delta_cls(float(getattr(r, 'rev_dod', 0) or 0))}'>{('+' if float(getattr(r,'rev_dod',0) or 0)>=0 else '')}{fmt_pct(float(getattr(r,'rev_dod',0) or 0),1)}</div>",
                f"<div class='text-right {delta_cls(float(getattr(r, 'rev_yoy', 0) or 0))}'>{('+' if float(getattr(r,'rev_yoy',0) or 0)>=0 else '')}{fmt_pct(float(getattr(r,'rev_yoy',0) or 0),1)}</div>",
            ], bold=(str(getattr(r, "bucket", "")) == "Total"))

    # Paid detail rows
    paid_html = ""
    paid_total_row = ""
    if paid_detail is not None and (not paid_detail.empty):
        show_n = 8
        max_n = 14
        idx_non_total = 0

        for r in paid_detail.itertuples(index=False):
            sub = str(getattr(r, "sub_channel", "") or "").strip()
            is_total = (sub.lower() == "total")
            is_bold = (sub.lower() == "total") or (sub.lower() == "google")
            has_yoy = bool(getattr(r, "has_yoy", False))

            row_cls = ""
            if (not is_total) and idx_non_total >= show_n:
                row_cls = "paid-extra hidden"
            if not is_total:
                idx_non_total += 1

            yoy_val = float(getattr(r, 'yoy', 0) or 0)
            yoy_html = (
                f"<div class='text-right {delta_cls(yoy_val)}'>{('+' if yoy_val>=0 else '')}{fmt_pct(yoy_val,1)}</div>"
                if (is_total or has_yoy)
                else "<div class='text-right text-slate-400'>-</div>"
            )

            row_html = table_row([
                esc(sub),
                f"<div class='text-right'>{fmt_int(getattr(r, 'sessions', 0))}</div>",
                f"<div class='text-right'>{fmt_currency_krw(getattr(r, 'purchaseRevenue', 0))}</div>",
                f"<div class='text-right {delta_cls(float(getattr(r, 'dod', 0) or 0))}'>{('+' if float(getattr(r,'dod',0) or 0)>=0 else '')}{fmt_pct(float(getattr(r,'dod',0) or 0),1)}</div>",
                yoy_html,
            ], bold=is_bold, row_class=row_cls)

            if is_total:
                paid_total_row += row_html
            else:
                if idx_non_total <= max_n:
                    paid_html += row_html

        paid_html += paid_total_row

    bs_rows = ""
    if best_sellers is not None and (not best_sellers.empty):
        for r in best_sellers.itertuples(index=False):
            bs_rows += f"""
            <div class="flex items-center gap-3 rounded-2xl border border-slate-200 bg-white/70 p-3">
              {product_img(getattr(r, "image_url", ""))}
              <div class="min-w-0 flex-1">
                <div class="truncate text-sm font-extrabold text-slate-900">{esc(getattr(r, "itemName", "") or "")}</div>
                <div class="text-xs text-slate-500">{esc(getattr(r, "itemId", "") or "")} · Qty {fmt_int(getattr(r, "qty", 0))} · Views {fmt_int(getattr(r, "views", 0))}</div>
              </div>
              <div class="shrink-0">{getattr(r, "trend_svg", "") or ""}</div>
            </div>
            """

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
              <div class="text-sm font-black {cls}">{esc(getattr(r, "delta_label", "?") or "?")} {('+' if delta>=0 else '')}{fmt_int(delta)}</div>
            </div>
            """

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

    new_terms_html = ""
    if search_new is not None and (not search_new.empty):
        for r in search_new.itertuples(index=False):
            new_terms_html += f"<div class='flex justify-between text-sm'><span class='font-extrabold'>{esc(getattr(r,'searchTerm',''))}</span><span class='text-slate-500'>{fmt_int(getattr(r,'count',0))}</span></div>"

    rising_terms_html = ""
    if search_rising is not None and (not search_rising.empty):
        for r in search_rising.itertuples(index=False):
            rising_terms_html += f"<div class='flex justify-between text-sm'><span class='font-extrabold'>{esc(getattr(r,'searchTerm',''))}</span><span class='text-slate-500'>{fmt_int(getattr(r,'count',0))}</span></div>"

    paid_media_compare_html = ""
    if paid_media_compare is not None and (not paid_media_compare.empty):
        for r in paid_media_compare.itertuples(index=False):
            paid_media_compare_html += table_row([
                esc(getattr(r, 'channel', '')),
                f"<div class='text-right'>{fmt_pct(float(getattr(r, 'target_roas', 0) or 0),1)}</div>",
                f"<div class='text-right'>{fmt_currency_krw(getattr(r, 'budget', 0))}</div>",
                f"<div class='text-right'>{fmt_pct(float(getattr(r, 'roas', 0) or 0),1)}</div>",
                f"<div class='text-right'>{fmt_pct(float(getattr(r, 'cvr', 0) or 0),2)}</div>",
            ])

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

    trend_tabs = [
        ("7d", "7D", trend_svgs.get("7d", "")),
        ("14d", "14D", trend_svgs.get("14d", "")),
        ("1m", "1M", trend_svgs.get("1m", "")),
    ]
    trend_tabs_html = "".join([
        f"<button type='button' data-trend-tab='{key}' class='rounded-full border px-3 py-1 text-xs font-extrabold transition {'border-slate-900 bg-slate-900 text-white' if i == 0 else 'border-slate-200 bg-white text-slate-500 hover:bg-slate-50'}'>{label}</button>"
        for i, (key, label, _) in enumerate(trend_tabs)
    ])
    trend_panels_html = "".join([
        f"<div data-trend-panel='{key}' class='mt-3{' hidden' if i != 0 else ''}'>{svg}</div>"
        for i, (key, _, svg) in enumerate(trend_tabs)
    ])

    paid_toggle_js = """<script>
(function(){
  const btn = document.getElementById('paidToggle');
  if(btn){
    let on = false;
    function setPaid(onNext){
      on = !!onNext;
      document.querySelectorAll('.paid-extra').forEach(el=>{
        if(on) el.classList.remove('hidden');
        else el.classList.add('hidden');
      });
      btn.textContent = on ? 'Show less' : 'Show more';
    }
    btn.addEventListener('click', ()=> setPaid(!on));
    setPaid(false);
  }

  const trendBtns = Array.from(document.querySelectorAll('[data-trend-tab]'));
  const trendPanels = Array.from(document.querySelectorAll('[data-trend-panel]'));
  function setTrend(target){
    trendBtns.forEach(el=>{
      const active = el.getAttribute('data-trend-tab') === target;
      el.classList.toggle('border-slate-900', active);
      el.classList.toggle('bg-slate-900', active);
      el.classList.toggle('text-white', active);
      el.classList.toggle('border-slate-200', !active);
      el.classList.toggle('bg-white', !active);
      el.classList.toggle('text-slate-500', !active);
    });
    trendPanels.forEach(el=>{
      const active = el.getAttribute('data-trend-panel') === target;
      if(active) el.classList.remove('hidden');
      else el.classList.add('hidden');
    });
  }
  trendBtns.forEach(el=>{
    el.addEventListener('click', ()=> setTrend(el.getAttribute('data-trend-tab')));
  });
  if(trendBtns.length){
    setTrend(trendBtns[0].getAttribute('data-trend-tab'));
  }
})();
</script>"""

    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>CSK E-COMM | Daily Digest</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200;400;600;800&display=swap');
    body{{ font-family:'Plus Jakarta Sans','Noto Sans KR','Malgun Gothic','Apple SD Gothic Neo',system-ui,-apple-system,'Segoe UI',Roboto,Arial; }}
  </style>
</head>
<body class="bg-slate-50 text-slate-900">
  <div class="mx-auto max-w-7xl p-6">
    <div class="flex flex-wrap items-center justify-between gap-3">
      <div class="flex items-center gap-3">
        <div class="text-2xl font-black">Daily Digest</div>
        <div class="rounded-full bg-slate-900 px-3 py-1 text-xs font-extrabold text-white">{w.mode.upper()}</div>
        <div class="text-sm text-slate-500">{ymd(w.cur_start)} ~ {ymd(w.cur_end)} · {w.compare_label} vs {ymd(w.prev_start)} ~ {ymd(w.prev_end)} · YoY {ymd(w.yoy_start)} ~ {ymd(w.yoy_end)}</div>
      </div>
      <div class="flex items-center gap-2">
        <a href="{esc(nav_links.get('hub','#'))}" class="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm font-extrabold hover:bg-slate-50">Hub</a>
      </div>
    </div>

    <div class="mt-6 grid grid-cols-1 gap-3 md:grid-cols-5">
      {kpis_cards}
    </div>

    <div class="mt-6 rounded-2xl border border-slate-200 bg-white/70 p-4">
      <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Channel Snapshot</div>
      <table class="mt-3 w-full table-auto text-sm">
        <thead class="text-xs text-slate-500">
          <tr>
            <th class="px-2 py-2 text-left whitespace-nowrap">Bucket</th>
            <th class="px-2 py-2 text-right whitespace-nowrap">Sessions</th>
            <th class="px-2 py-2 text-right whitespace-nowrap">Orders</th>
            <th class="px-2 py-2 text-right whitespace-nowrap">Revenue</th>
            <th class="px-2 py-2 text-right whitespace-nowrap">{w.compare_label}</th>
            <th class="px-2 py-2 text-right whitespace-nowrap pr-4">YoY</th>
          </tr>
        </thead>
        <tbody>{chan_html}</tbody>
      </table>
    </div>

    <div class="mt-6 rounded-2xl border border-slate-200 bg-white/70 p-4">
      <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Paid Detail</div>
      <div class="mt-3 overflow-x-auto">
        <table class="w-full table-auto text-sm min-w-[920px]">
          <thead class="text-xs text-slate-500">
            <tr>
              <th class="px-2 py-2 text-left whitespace-nowrap">Sub</th>
              <th class="px-2 py-2 text-right whitespace-nowrap">Sessions</th>
              <th class="px-2 py-2 text-right whitespace-nowrap">Revenue</th>
              <th class="px-2 py-2 text-right whitespace-nowrap">{w.compare_label}</th>
              <th class="px-2 py-2 text-right whitespace-nowrap pr-4">YoY</th>
            </tr>
          </thead>
          <tbody>{paid_html}</tbody>
        </table>
      </div>
      <div class="mt-3 flex justify-end">
        <button id="paidToggle" type="button" class="rounded-full border border-slate-200 bg-white px-3 py-1 text-xs font-extrabold text-slate-600 hover:bg-slate-50">Show more</button>
      </div>
    </div>

    <div class="mt-6 rounded-2xl border border-slate-200 bg-white/70 p-4">
      <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Paid Budget / ROAS / CVR</div>
      <div class="mt-3 overflow-x-auto">
        <table class="w-full table-auto text-sm min-w-[760px]">
          <thead class="text-xs text-slate-500">
            <tr>
              <th class="px-2 py-2 text-left whitespace-nowrap">Sub</th>
              <th class="px-2 py-2 text-right whitespace-nowrap">Target ROAS</th>
              <th class="px-2 py-2 text-right whitespace-nowrap">Budget</th>
              <th class="px-2 py-2 text-right whitespace-nowrap">ROAS</th>
              <th class="px-2 py-2 text-right whitespace-nowrap pr-4">CVR</th>
            </tr>
          </thead>
          <tbody>{paid_media_compare_html or "<tr><td colspan='5' class='px-2 py-6 text-center text-slate-400'>No data</td></tr>"}</tbody>
        </table>
      </div>
    </div>

    <div class="mt-6 rounded-2xl border border-slate-200 bg-white/70 p-4">
      <div class="flex flex-wrap items-center justify-between gap-3">
        <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Trend (Index)</div>
        <div class="flex flex-wrap items-center gap-2">{trend_tabs_html}</div>
      </div>
      {trend_panels_html}
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

  {paid_toggle_js}

</body>
</html>
"""


# =========================
# Hub page
# =========================
def render_hub_index(dates: List[dt.date]) -> str:
    dates = sorted(dates)
    if not dates:
        dates = [dt.datetime.now(ZoneInfo("Asia/Seoul")).date() - dt.timedelta(days=1)]
    latest = dates[-1]

    date_opts = "\n".join([f"<option value='{d.strftime('%Y-%m-%d')}'>{d.strftime('%Y-%m-%d')}</option>" for d in reversed(dates)])

    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>CSK E-COMM | Daily Digest Hub</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200;400;600;800&display=swap');
    body{{ font-family:'Plus Jakarta Sans','Noto Sans KR','Malgun Gothic','Apple SD Gothic Neo',system-ui,-apple-system,'Segoe UI',Roboto,Arial; }}
  </style>
</head>
<body class="bg-slate-50 text-slate-900">
  <div class="mx-auto max-w-7xl p-6">
    <div class="flex flex-wrap items-center justify-between gap-3">
      <div class="flex items-center gap-3">
        <div class="text-2xl font-black">Daily Digest Hub</div>
        <div class="rounded-full bg-slate-900 px-3 py-1 text-xs font-extrabold text-white">STATIC</div>
      </div>
      <div class="text-sm text-slate-500">Data cache 기반</div>
    </div>

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

      <div class="md:col-span-2 rounded-2xl border border-slate-200 bg-white/70 p-4">
        <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Recent</div>
        <div id="recentList" class="mt-3 grid grid-cols-1 gap-2 md:grid-cols-2"></div>
      </div>
    </div>
  </div>

<script>
(() => {{
  const dates = {json.dumps([d.strftime("%Y-%m-%d") for d in dates])};
  const latest = "{latest.strftime('%Y-%m-%d')}";

  const $ = (s) => document.querySelector(s);
  const esc = (s) => String(s ?? "").replace(/[&<>"]/g, c => ({{"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}}[c]));

  function initDefaults() {{
    $("#openDate").value = latest;
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
                trend_svgs=rt["trend_svgs"],
                best_sellers=rt["best_sellers"],
                rising=rt["rising"],
                category_pdp_trend=rt["category_pdp_trend"],
                search_new=rt["search_new"],
                search_rising=rt["search_rising"],
                other_detail=rt["other_detail"],
                paid_media_compare=rt["paid_media_compare"],
                nav_links={"hub": "../index.html", "daily_index": "../index.html", "weekly_index": "../index.html"},
                bundle_rel_path=bundle_rel,
            )
            return html, cached

    overall = get_overall_kpis(client, w)
    signup_users = get_multi_event_users_3way(client, w, ["signup_complete", "signup"])

    # Channel Snapshot: Looker CASE + Total forced from KPI.
    channel_snapshot = get_channel_snapshot_3way(client, w, overall=overall)

    # Extract Paid AD totals from Channel Snapshot for Paid Detail alignment.
    paid_ad_totals = {
        "current": {"sessions": None, "revenue": None},
        "prev": {"sessions": None, "revenue": None},
        "yoy": {"sessions": None, "revenue": None},
    }
    try:
        # Only current-period absolute values are needed to force Paid Detail Total.
        row = channel_snapshot[channel_snapshot["bucket"] == "Paid AD"]
        if not row.empty:
            paid_ad_totals["current"]["sessions"] = float(row.iloc[0]["sessions"])
            paid_ad_totals["current"]["revenue"] = float(row.iloc[0]["purchaseRevenue"])
    except Exception:
        pass

    paid_detail = get_paid_detail_3way(client, w, paid_ad_totals=paid_ad_totals)
    paid_top3 = get_paid_top3(client, w)
    other_detail = pd.DataFrame()
    target_roas_map = load_target_roas_map(TARGET_ROAS_XLS_PATH)
    paid_media_compare = get_paid_media_comparison_table(client, w, target_roas_map)
    kpi_snapshot = get_kpi_snapshot_table_3way(client, w, overall)

    trend_series = get_trend_view_series(client, w)
    trend_svgs = build_trend_svg_map(trend_series)

    best_sellers, best_sellers_series = get_best_sellers_with_trends(client, w, image_map)

    # Rising: exclude Best Sellers SKUs and keep prev > 0 with positive delta.
    exclude = []
    if not best_sellers.empty and "itemId" in best_sellers.columns:
        exclude = [str(x).strip() for x in best_sellers["itemId"].tolist() if str(x).strip()]
    rising = get_rising_products(client, w, top_n=5, exclude_skus=exclude)
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
        other_detail=other_detail,
        paid_media_compare=paid_media_compare,
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
        trend_svgs=trend_svgs,
        best_sellers=best_sellers,
        rising=rising,
        category_pdp_trend=category_pdp_trend,
        search_new=search["new"],
        search_rising=search["rising"],
        other_detail=other_detail,
        paid_media_compare=paid_media_compare,
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

        # Always rebuild the latest end date even if SKIP_IF_EXISTS is enabled.
        force_rebuild = (d == latest_end)

        if (not force_rebuild) and SKIP_IF_EXISTS and os.path.exists(out_daily):
            print(f"[SKIP] Exists (Daily): {out_daily}")
        else:
            html_daily, _bundle = build_one(client, end_date=d, mode="daily", image_map=image_map, logo_b64=logo_b64)
            with open(out_daily, "w", encoding="utf-8") as f:
                f.write(html_daily)
            print(f"[OK] Wrote: {out_daily} (force={force_rebuild})")

        if (not force_rebuild) and SKIP_IF_EXISTS and os.path.exists(out_weekly):
            print(f"[SKIP] Exists (Weekly): {out_weekly}")
        else:
            html_weekly, _bundle = build_one(client, end_date=d, mode="weekly", image_map=image_map, logo_b64=logo_b64)
            with open(out_weekly, "w", encoding="utf-8") as f:
                f.write(html_weekly)
            print(f"[OK] Wrote: {out_weekly} (force={force_rebuild})")

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

