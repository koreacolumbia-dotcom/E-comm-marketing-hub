#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Columbia Daily Digest — Live GA4 Data (Google Analytics Data API) + BigQuery PDP Trend
+ ✅ Compare UI (A date/period vs B date/period) — **NO extra GA/BQ cost**
+ ✅ File-based Data Cache (bundle JSON) — regenerate HTML without re-query
+ ✅ Expensive part cache (BigQuery PDP Trend) — reuses cached JSON

✅ PATCH (이번 요청 반영)
1) KPI Sessions == Channel Snapshot Total Sessions **항상 일치**
   - Total row는 무조건 Overall KPI(current/prev/yoy)로 “강제” 세팅 (권위 데이터)
2) Channel Snapshot의 Paid AD == Paid Detail Total (sessions / revenue) **항상 일치**
   - Channel Snapshot을 Looker CASE(샘플.rtf) 기반 source/medium + campaign 로직으로 집계
   - Paid Detail도 동일 베이스에서 Paid AD만 필터 → sub 분해
   - Paid Detail Total은 Paid AD row 값을 “강제” 세팅(세션/매출)
3) Paid Detail Total mismatch 원인 제거
   - 기존처럼 “표에 보여줄 일부(sub top)”만으로 Total 계산하지 않음 (전체 Paid AD 합으로 계산)
4) Hub의 Range A/B “구간비교” UI/기능 **아예 제거**
5) Channel Snapshot / Paid Detail 테이블에서 DoD/YoY 컬럼이 잘리는 문제 완화
   - td padding 축소, 마지막 컬럼 우측 padding 확대, table-auto + whitespace-nowrap 적용
6) Best Sellers vs Rising Products 중복 완화
   - Best Sellers: itemsPurchased 상위
   - Rising: 전기간 대비 qty 증가(Δ) 기준 + **Best Sellers SKU는 제외** + prev>0 조건(“올라오는” 성격)
   - (원하면 prev==0 신규 급등도 포함하도록 옵션화 가능)

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
import re
import urllib.parse
import urllib.request
import urllib.error
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

# Paid detail (custom) — fixed order / labels
PAID_DETAIL_SOURCES = ["naverbs", "criteo", "meta", "google", "naver mo", "instagram"]

TARGET_ROAS_XLS_PATH = os.getenv("DAILY_DIGEST_TARGET_ROAS_XLS_PATH", "target_roas.xlsx").strip()
MEDIA_SPEND_XLS_PATH = os.getenv("DAILY_DIGEST_MEDIA_SPEND_XLS_PATH", "paid_media_spend.xlsx").strip()

META_APP_ID = os.getenv("META_APP_ID", "").strip()
META_AD_ACCOUNT_ID = os.getenv("META_AD_ACCOUNT_ID", "").strip()
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN", "").strip()

GOOGLE_ADS_CUSTOMER_ID = os.getenv("GOOGLE_ADS_CUSTOMER_ID", "").strip().replace("-", "")
GOOGLE_ADS_DEVELOPER_TOKEN = os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN", "").strip()
GOOGLE_ADS_ACCESS_TOKEN = os.getenv("GOOGLE_ADS_ACCESS_TOKEN", "").strip()
GOOGLE_ADS_REFRESH_TOKEN = os.getenv("GOOGLE_ADS_REFRESH_TOKEN", "").strip()
GOOGLE_ADS_CLIENT_ID = os.getenv("GOOGLE_ADS_CLIENT_ID", "").strip()
GOOGLE_ADS_CLIENT_SECRET = os.getenv("GOOGLE_ADS_CLIENT_SECRET", "").strip()
GOOGLE_ADS_LOGIN_CUSTOMER_ID = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "").strip().replace("-", "")

NAVER_AD_CUSTOMER_ID = os.getenv("NAVER_AD_CUSTOMER_ID", "").strip()
NAVER_AD_ACCESS_LICENSE = os.getenv("NAVER_AD_ACCESS_LICENSE", "").strip()
NAVER_AD_SECRET_KEY = os.getenv("NAVER_AD_SECRET_KEY", "").strip()
NAVER_AD_BASE_URL = os.getenv("NAVER_AD_BASE_URL", "https://api.naver.com").strip()


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
# Looker CASE rules (샘플.rtf) — source/medium + campaign
# =========================
def _rx(p: str):
    return re.compile(p, re.IGNORECASE)

def classify_looker_channel(source_medium: str, campaign: str = "") -> str:
    sm = (source_medium or "").strip()
    cp = (campaign or "").strip()

    # Order is 중요한 “CASE 순서” (샘플.rtf)
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

    # 샘플.rtf에서 mkt/_bd 캠페인 조건
    if _rx(r".*(mkt|_bd).*").search(sm) or _rx(r".*(mkt|_bd).*").search(cp):
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
    if _rx(r".*google\s*/\s*cpc.*").search(sm) and _rx(r".*(sa|ss).*").search(cp):
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
        return "naverbs"
    if has(r".*(igshopping|instagram|(^|[^a-z])ig([^a-z]|$)).*", sm):
        return "instagram"
    if has(r".*criteo.*", sm):
        return "criteo"
    if has(r".*(meta|facebook|(^|[^a-z])fb([^a-z]|$)).*", sm):
        return "meta"
    if has(r".*google\s*/\s*cpc.*", sm) or has(r"(^|[^a-z])google([^a-z]|$)", sm):
        return "google"
    if has(r".*(m\.search\.naver\.com|m\.ad\.search\.naver\.com|m\.search\.naver).*", sm):
        return "naver mo"
    if has(r".*naver.*", sm) and has(r".*cpc.*", sm):
        return "naver mo"

    base = sm.split("/")[0].strip()
    base = re.sub(r"\s+", " ", base)
    return base or "other"


# =========================
# Channel Snapshot — Looker CASE 기반 (sessions/revenue 합치기)
# =========================
def get_channel_snapshot_3way(
    client: BetaAnalyticsDataClient,
    w: DigestWindow,
    overall: Dict[str, Dict[str, float]],
) -> pd.DataFrame:
    """
    ✅ FIX 핵심
    - Looker CASE(샘플.rtf) 기준으로 bucket 분류
    - Total row는 Overall KPI를 “강제”로 사용 → KPI Sessions와 100% 동일
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
            # fallback: campaign dimension 미지원 케이스 대비
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

    # ✅ Total row는 KPI(Overall)로 강제
    oc = overall.get("current", {}) or {}
    op = overall.get("prev", {}) or {}
    oy = overall.get("yoy", {}) or {}

    total_sessions_cur = float(oc.get("sessions", 0) or 0)
    total_sessions_prev = float(op.get("sessions", 0) or 0)
    total_sessions_yoy = float(oy.get("sessions", 0) or 0)

    total = pd.DataFrame([{
        "bucket": "Total",
        "sessions": total_sessions_cur,
        "transactions": float(oc.get("transactions", 0) or 0),
        "purchaseRevenue": float(oc.get("purchaseRevenue", 0) or 0),
        "rev_dod": pct_change(total_sessions_cur, total_sessions_prev),
        "rev_yoy": pct_change(total_sessions_cur, total_sessions_yoy),
    }])

    out = pd.concat([out, total], ignore_index=True)
    return out[["bucket", "sessions", "transactions", "purchaseRevenue", "rev_dod", "rev_yoy"]]


# =========================
# Paid Detail — Channel Snapshot과 동일 베이스(Paid AD)에서 sub 분해
# =========================
def get_paid_detail_3way(
    client: BetaAnalyticsDataClient,
    w: DigestWindow,
    paid_ad_totals: Optional[Dict[str, Dict[str, float]]] = None,
) -> pd.DataFrame:
    """
    ✅ FIX
    - Paid Detail Total이 Channel Snapshot Paid AD와 항상 일치하도록
    - DoD/YoY는 Sessions 기준(표 컬럼 의미 일관)
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

    merged = cur_a.merge(prev_a, on="sub", how="outer").merge(yoy_a, on="sub", how="outer").fillna(0.0)
    merged["dod"] = merged.apply(lambda r: pct_change(float(r["sessions_cur"]), float(r["sessions_prev"])), axis=1)
    merged["yoy"] = merged.apply(lambda r: pct_change(float(r["sessions_cur"]), float(r["sessions_yoy"])), axis=1)

    # 표에 반드시 보여줄 core label 보강
    core = list(PAID_DETAIL_SOURCES)
    for c in core:
        if c not in set(merged["sub"].tolist()):
            merged = pd.concat([merged, pd.DataFrame([{
                "sub": c, "sessions_cur": 0.0, "rev_cur": 0.0,
                "sessions_prev": 0.0, "rev_prev": 0.0,
                "sessions_yoy": 0.0, "rev_yoy_base": 0.0,
                "dod": 0.0, "yoy": 0.0
            }])], ignore_index=True)

    # 표에선 core + others(top)만 노출, 하지만 Total은 “전체 Paid AD 합”으로 계산
    others = merged[~merged["sub"].isin(core)].copy()
    others = others.sort_values(["sessions_cur", "rev_cur"], ascending=[False, False]).head(6)

    ordered = pd.concat([
        merged[merged["sub"].isin(core)].assign(_ord=lambda d: d["sub"].apply(lambda x: core.index(x))).sort_values("_ord"),
        others.assign(_ord=999),
    ], ignore_index=True)

    # ✅ Total 강제 (Channel Snapshot Paid AD와 동일)
    # current는 Channel Snapshot Paid AD 값을 우선 사용하고,
    # prev / yoy raw 값이 없으면 merged 합계로 fallback 해서 100% 고정 버그 방지
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
    }

    out = ordered[["sub", "sessions_cur", "rev_cur", "dod", "yoy"]].copy()
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
    # (기존 유지: Paid AD 그룹을 sessionDefaultChannelGroup으로 잡는 부분은 남겨둠)
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
    return f"<svg width='100%' viewBox='0 0 {width} {height}' preserveAspectRatio='none' style='display:block;'>{''.join(weekend_marks)}{''.join(grid)}{axes}{''.join(polys)}{''.join(dots)}{''.join(ylabels_svg)}{''.join(xlabels_svg)}<text x='{pad_l}' y='{height-8}' font-size='10' fill='#94a3b8'>Index (D-30 start = 100)</text>{''.join(legend_items)}</svg>"

def trend_svg_from_series(series: dict) -> str:
    x = series.get("x", [])
    axis_dates = [parse_yyyy_mm_dd(d) for d in series.get("dates", [])]
    if not x or len(x) != 30 or not axis_dates or any(d is None for d in axis_dates):
        fallback_dates = [dt.date.today() - dt.timedelta(days=29-i) for i in range(30)]
        return combined_index_svg_monthly(fallback_dates, [d.strftime('%m/%d') for d in fallback_dates], [[100]*30,[100]*30,[100]*30], ["#0055a5","#16a34a","#c2410c"], ["Sessions","Revenue","CVR"])
    s = index_series([float(v) for v in series.get("sessions", [])])
    r = index_series([float(v) for v in series.get("revenue", [])])
    c = index_series([float(v) for v in series.get("cvr", [])])
    return combined_index_svg_monthly(axis_dates, x, [s, r, c], ["#0055a5","#16a34a","#c2410c"], ["Sessions","Revenue","CVR"])


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
    cand["itemId"] = cand["itemId"].astype(str).str.strip()
    cand["itemName"] = cand["itemName"].astype(str).fillna("").map(lambda x: x.strip())
    cand = cand[~cand["itemName"].map(is_not_set)]
    if cand.empty:
        return pd.DataFrame(columns=["itemId","itemName","qty","views","trend_svg","image_url"]), {"x": [], "items": []}

    cand["image_url"] = cand["itemId"].map(lambda s: image_map.get(str(s).strip(), ""))
    with_img = cand[cand["image_url"].astype(str).str.strip() != ""].copy().sort_values("itemsPurchased", ascending=False)
    no_img = cand[cand["image_url"].astype(str).str.strip() == ""].copy().sort_values("itemsPurchased", ascending=False)
    top = pd.concat([with_img.head(5), no_img.head(max(0, 5 - len(with_img.head(5))))], ignore_index=True).head(5).copy()
    top["qty"] = top["itemsPurchased"]
    skus = [str(s).strip() for s in top["itemId"].tolist() if str(s).strip()]

    # ✅ Best Sellers 카드에 Views 수치 노출
    views_df = _get_item_views_best_effort(client, start, end, skus)
    if views_df.empty:
        top["views"] = 0.0
    else:
        views_df["itemId"] = views_df["itemId"].astype(str).str.strip()
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

    return top[["itemId","itemName","qty","views","trend_svg","image_url"]], series_cache


# =========================
# Rising Products (Best Sellers와 겹침 완화)
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
    ✅ Rising 정의(현재 구현)
    - prev 대비 qty 증가(Δ) 큰 상품
    - prev > 0 (완전 신규 폭증 대신 “올라오는” 성격)
    - Best Sellers SKU는 제외(겹침 완화)
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

    # ✅ exclude best sellers + “올라오는” 조건(prev > 0) + delta>0
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
        m["delta_label"] = "Views Δ"
    elif RISING_BASIS == "revenue":
        m["delta"] = m["revenue_delta"]
        m["delta_label"] = "Revenue Δ"
    else:
        m["delta"] = m["qty_delta"]
        m["delta_label"] = "Qty Δ"

    m = m.sort_values("delta", ascending=False).head(top_n).copy()
    m["image_url"] = ""  # attach_image_urls에서 채움
    return m[["itemId", "itemName", "qty", "views", "revenue", "delta", "delta_label", "image_url"]]


# =========================
# PDP Trend (BigQuery) + file cache (kept)
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

    new_terms = merged[merged["prior_avg"] == 0].head(3)[["searchTerm","count"]].copy()

    rising = merged[merged["prior_avg"] > 0].copy()
    rising["pct"] = (rising["count"] - rising["prior_avg"]) / rising["prior_avg"] * 100.0
    rising = rising.replace([float("inf"), -float("inf")], 0.0)
    rising = rising.sort_values("pct", ascending=False).head(3)[["searchTerm","pct","count"]]
    return {"new": new_terms, "rising": rising}




def safe_read_excel(path: str) -> pd.DataFrame:
    try:
        if path and os.path.exists(path):
            return pd.read_excel(path)
    except Exception as e:
        print(f"[WARN] Excel read failed: {path} | {type(e).__name__}: {e}")
    return pd.DataFrame()

def load_target_roas_map(xlsx_path: str) -> Dict[str, float]:
    df = safe_read_excel(xlsx_path)
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

def refresh_google_ads_access_token() -> str:
    if GOOGLE_ADS_ACCESS_TOKEN:
        return GOOGLE_ADS_ACCESS_TOKEN
    if not (GOOGLE_ADS_REFRESH_TOKEN and GOOGLE_ADS_CLIENT_ID and GOOGLE_ADS_CLIENT_SECRET):
        return ''
    data = urllib.parse.urlencode({
        'client_id': GOOGLE_ADS_CLIENT_ID,
        'client_secret': GOOGLE_ADS_CLIENT_SECRET,
        'refresh_token': GOOGLE_ADS_REFRESH_TOKEN,
        'grant_type': 'refresh_token',
    }).encode('utf-8')
    req = urllib.request.Request('https://oauth2.googleapis.com/token', data=data, headers={'Content-Type':'application/x-www-form-urlencoded'})
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = json.loads(resp.read().decode('utf-8'))
    return str(payload.get('access_token','')).strip()

def http_json(url: str, headers: Optional[Dict[str,str]] = None, data: Optional[bytes] = None, method: Optional[str] = None) -> dict:
    req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
    with urllib.request.urlopen(req, timeout=60) as resp:
        body = resp.read().decode('utf-8')
    try:
        return json.loads(body)
    except Exception:
        return {}

def fetch_meta_spend(start: dt.date, end: dt.date) -> float:
    act_id = META_AD_ACCOUNT_ID or ''
    if act_id and not act_id.startswith('act_'):
        act_id = f'act_{act_id}'
    if not (act_id and META_ACCESS_TOKEN):
        return 0.0
    params = {
        'fields': 'spend',
        'level': 'account',
        'time_range': json.dumps({'since': ymd(start), 'until': ymd(end)}),
        'access_token': META_ACCESS_TOKEN,
    }
    url = f"https://graph.facebook.com/v23.0/{act_id}/insights?" + urllib.parse.urlencode(params)
    try:
        js = http_json(url)
        rows = js.get('data', []) or []
        return float(rows[0].get('spend', 0) or 0) if rows else 0.0
    except Exception as e:
        print(f"[WARN] META spend fetch failed: {type(e).__name__}: {e}")
        return 0.0

def fetch_google_ads_spend(start: dt.date, end: dt.date) -> float:
    if not (GOOGLE_ADS_CUSTOMER_ID and GOOGLE_ADS_DEVELOPER_TOKEN):
        return 0.0
    token = refresh_google_ads_access_token()
    if not token:
        return 0.0
    query = {
        'query': f"SELECT metrics.cost_micros FROM customer WHERE segments.date BETWEEN '{ymd(start)}' AND '{ymd(end)}'"
    }
    headers = {
        'Authorization': f'Bearer {token}',
        'developer-token': GOOGLE_ADS_DEVELOPER_TOKEN,
        'Content-Type': 'application/json',
    }
    if GOOGLE_ADS_LOGIN_CUSTOMER_ID:
        headers['login-customer-id'] = GOOGLE_ADS_LOGIN_CUSTOMER_ID
    url = f"https://googleads.googleapis.com/v19/customers/{GOOGLE_ADS_CUSTOMER_ID}/googleAds:searchStream"
    try:
        js = http_json(url, headers=headers, data=json.dumps(query).encode('utf-8'), method='POST')
        total = 0.0
        if isinstance(js, list):
            for block in js:
                for row in block.get('results', []) or []:
                    total += float((((row.get('metrics') or {}).get('costMicros', 0)) or 0)) / 1_000_000.0
        return total
    except Exception as e:
        print(f"[WARN] GOOGLE spend fetch failed: {type(e).__name__}: {e}")
        return 0.0

def fetch_naver_spend(start: dt.date, end: dt.date) -> float:
    # API schemas vary by contract. Fallback to manual sheet when unavailable.
    return 0.0

def load_manual_spend_map(xlsx_path: str, start: dt.date, end: dt.date) -> Dict[str, float]:
    df = safe_read_excel(xlsx_path)
    if df.empty:
        return {}
    cols = {str(c).strip().lower(): c for c in df.columns}
    ch_col = cols.get('channel') or cols.get('media') or cols.get('매체') or cols.get('채널')
    spend_col = cols.get('spend') or cols.get('budget') or cols.get('광고비') or cols.get('예산')
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
        ch = str(r.get(ch_col, '') or '').strip().lower()
        if not ch:
            continue
        out[ch] = out.get(ch, 0.0) + float(r.get(spend_col, 0) or 0)
    return out

def map_sub_to_media(sub: str) -> str:
    sub = (sub or '').strip().lower()
    if sub in ('google',):
        return 'google'
    if sub in ('meta', 'instagram'):
        return 'meta'
    if sub in ('naverbs', 'naver mo'):
        return 'naver'
    return sub

def fetch_platform_spend_map(start: dt.date, end: dt.date) -> Dict[str, float]:
    manual = load_manual_spend_map(MEDIA_SPEND_XLS_PATH, start, end)
    out = {k: float(v or 0) for k, v in manual.items()}
    out['meta'] = max(out.get('meta', 0.0), fetch_meta_spend(start, end))
    out['google'] = max(out.get('google', 0.0), fetch_google_ads_spend(start, end))
    out['naver'] = max(out.get('naver', 0.0), fetch_naver_spend(start, end))
    return out

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
        df['media'] = df['sub'].map(map_sub_to_media)
        return df
    cur = fetch(w.cur_start, w.cur_end)
    yoy = fetch(w.yoy_start, w.yoy_end)
    def agg(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=['media',f'sessions{suffix}',f'orders{suffix}',f'revenue{suffix}'])
        g = df.groupby('media', as_index=False)[['sessions','transactions','purchaseRevenue']].sum()
        return g.rename(columns={'sessions':f'sessions{suffix}','transactions':f'orders{suffix}','purchaseRevenue':f'revenue{suffix}'})
    merged = agg(cur,'_cur').merge(agg(yoy,'_py'), on='media', how='outer').fillna(0.0)
    spend_cur = fetch_platform_spend_map(w.cur_start, w.cur_end)
    spend_py = fetch_platform_spend_map(w.yoy_start, w.yoy_end)
    medias = ['google','naver','meta']
    rows=[]
    for media in medias:
        row = merged[merged['media']==media]
        if row.empty:
            rc = {'sessions_cur':0.0,'orders_cur':0.0,'revenue_cur':0.0,'sessions_py':0.0,'orders_py':0.0,'revenue_py':0.0}
        else:
            rr = row.iloc[0]
            rc = {k: float(rr.get(k,0) or 0) for k in ['sessions_cur','orders_cur','revenue_cur','sessions_py','orders_py','revenue_py']}
        cur_spend = float(spend_cur.get(media,0) or 0)
        py_spend = float(spend_py.get(media,0) or 0)
        cur_roas = (rc['revenue_cur']/cur_spend) if cur_spend else 0.0
        py_roas = (rc['revenue_py']/py_spend) if py_spend else 0.0
        cur_cvr = (rc['orders_cur']/rc['sessions_cur']) if rc['sessions_cur'] else 0.0
        py_cvr = (rc['orders_py']/rc['sessions_py']) if rc['sessions_py'] else 0.0
        rows.append({
            'channel': media.title(),
            'target_roas': float(target_roas_map.get(media, target_roas_map.get(media.title().lower(), 0.0)) or 0),
            'budget_prev_year': py_spend,
            'budget_current_year': cur_spend,
            'roas_prev_year': py_roas,
            'roas_current_year': cur_roas,
            'cvr_prev_year': py_cvr,
            'cvr_current_year': cur_cvr,
        })
    return pd.DataFrame(rows)

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
        "trend_svg": trend_svg,
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
    trend_svg: str,
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

    # ✅ padding 조정(마지막 컬럼 잘림 완화)
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
        show_n = 6
        max_n = 12
        idx_non_total = 0

        for r in paid_detail.itertuples(index=False):
            sub = str(getattr(r, "sub_channel", "") or "").strip()
            is_total = (sub.lower() == "total")
            is_bold = (sub.lower() == "total") or (sub.lower() == "google")

            row_cls = ""
            if (not is_total) and idx_non_total >= show_n:
                row_cls = "paid-extra hidden"
            if not is_total:
                idx_non_total += 1

            row_html = table_row([
                esc(sub),
                f"<div class='text-right'>{fmt_int(getattr(r, 'sessions', 0))}</div>",
                f"<div class='text-right'>{fmt_currency_krw(getattr(r, 'purchaseRevenue', 0))}</div>",
                f"<div class='text-right {delta_cls(float(getattr(r, 'dod', 0) or 0))}'>{('+' if float(getattr(r,'dod',0) or 0)>=0 else '')}{fmt_pct(float(getattr(r,'dod',0) or 0),1)}</div>",
                f"<div class='text-right {delta_cls(float(getattr(r, 'yoy', 0) or 0))}'>{('+' if float(getattr(r,'yoy',0) or 0)>=0 else '')}{fmt_pct(float(getattr(r,'yoy',0) or 0),1)}</div>",
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
              <div class="text-sm font-black {cls}">{esc(getattr(r, "delta_label", "Δ") or "Δ")} {('+' if delta>=0 else '')}{fmt_int(delta)}</div>
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
            pct = float(getattr(r, "pct", 0) or 0.0)
            rising_terms_html += f"<div class='flex justify-between text-sm'><span class='font-extrabold'>{esc(getattr(r,'searchTerm',''))}</span><span class='text-slate-500'>{'+' if pct>=0 else ''}{pct:.1f}% · {fmt_int(getattr(r,'count',0))}</span></div>"

    other_html = ""
    if other_detail is not None and (not other_detail.empty):
        for r in other_detail.itertuples(index=False):
            other_html += table_row([
                esc(getattr(r, "sub_channel", "")),
                f"<div class='text-right'>{fmt_int(getattr(r, 'sessions', 0))}</div>",
                f"<div class='text-right'>{fmt_int(getattr(r, 'orders', 0))}</div>",
                f"<div class='text-right'>{fmt_currency_krw(getattr(r, 'purchaseRevenue', 0))}</div>",
                f"<div class='text-right {delta_cls(float(getattr(r, 'dod', 0) or 0))}'>{('+' if float(getattr(r,'dod',0) or 0)>=0 else '')}{fmt_pct(float(getattr(r,'dod',0) or 0),1)}</div>",
                f"<div class='text-right {delta_cls(float(getattr(r, 'yoy', 0) or 0))}'>{('+' if float(getattr(r,'yoy',0) or 0)>=0 else '')}{fmt_pct(float(getattr(r,'yoy',0) or 0),1)}</div>",
            ])

    paid_media_compare_html = ""
    if paid_media_compare is not None and (not paid_media_compare.empty):
        for r in paid_media_compare.itertuples(index=False):
            paid_media_compare_html += table_row([
                esc(getattr(r, 'channel', '')),
                f"<div class='text-right'>{fmt_pct(float(getattr(r, 'target_roas', 0) or 0),1)}</div>",
                f"<div class='text-right'>{fmt_currency_krw(getattr(r, 'budget_prev_year', 0))}</div>",
                f"<div class='text-right'>{fmt_currency_krw(getattr(r, 'budget_current_year', 0))}</div>",
                f"<div class='text-right'>{fmt_pct(float(getattr(r, 'roas_prev_year', 0) or 0),1)}</div>",
                f"<div class='text-right'>{fmt_pct(float(getattr(r, 'roas_current_year', 0) or 0),1)}</div>",
                f"<div class='text-right'>{fmt_pct(float(getattr(r, 'cvr_prev_year', 0) or 0),2)}</div>",
                f"<div class='text-right'>{fmt_pct(float(getattr(r, 'cvr_current_year', 0) or 0),2)}</div>",
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

    paid_toggle_js = """<script>
(function(){
  const btn = document.getElementById('paidToggle');
  if(!btn) return;
  let on = false;
  function set(onNext){
    on = !!onNext;
    document.querySelectorAll('.paid-extra').forEach(el=>{
      if(on) el.classList.remove('hidden');
      else el.classList.add('hidden');
    });
    btn.textContent = on ? 'Show less' : 'Show more (12)';
  }
  btn.addEventListener('click', ()=> set(!on));
  set(false);
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
    body{{ font-family:'Plus Jakarta Sans', system-ui, -apple-system, Segoe UI, Roboto, Arial; }}
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

    <div class="mt-6 grid grid-cols-1 gap-4 lg:grid-cols-2">
      <div class="rounded-2xl border border-slate-200 bg-white/70 p-4">
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

      <div class="rounded-2xl border border-slate-200 bg-white/70 p-4">
        <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Other Detail</div>
        <table class="mt-3 w-full table-auto text-sm">
          <thead class="text-xs text-slate-500">
            <tr>
              <th class="px-2 py-2 text-left whitespace-nowrap">Source / Medium</th>
              <th class="px-2 py-2 text-right whitespace-nowrap">Sessions</th>
              <th class="px-2 py-2 text-right whitespace-nowrap">Orders</th>
              <th class="px-2 py-2 text-right whitespace-nowrap">Revenue</th>
              <th class="px-2 py-2 text-right whitespace-nowrap">{w.compare_label}</th>
              <th class="px-2 py-2 text-right whitespace-nowrap pr-4">YoY</th>
            </tr>
          </thead>
          <tbody>{other_html or "<tr><td colspan='6' class='px-2 py-6 text-center text-slate-400'>No data</td></tr>"}</tbody>
        </table>
      </div>
    </div>

    <div class="mt-6 rounded-2xl border border-slate-200 bg-white/70 p-4">
      <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">1Month Trend (Index)</div>
      <div class="mt-3">{trend_svg}</div>
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
    </div>

    <div class="mt-6 rounded-2xl border border-slate-200 bg-white/70 p-4">
      <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Paid Budget / ROAS / CVR</div>
      <div class="mt-3 overflow-x-auto">
        <table class="w-full table-auto text-sm min-w-[1120px]">
          <thead class="text-xs text-slate-500">
            <tr>
              <th class="px-2 py-2 text-left whitespace-nowrap">Channel</th>
              <th class="px-2 py-2 text-right whitespace-nowrap">Target ROAS</th>
              <th class="px-2 py-2 text-right whitespace-nowrap">PY Budget</th>
              <th class="px-2 py-2 text-right whitespace-nowrap">CY Budget</th>
              <th class="px-2 py-2 text-right whitespace-nowrap">PY ROAS</th>
              <th class="px-2 py-2 text-right whitespace-nowrap">CY ROAS</th>
              <th class="px-2 py-2 text-right whitespace-nowrap">PY CVR</th>
              <th class="px-2 py-2 text-right whitespace-nowrap pr-4">CY CVR</th>
            </tr>
          </thead>
          <tbody>{paid_media_compare_html or "<tr><td colspan='8' class='px-2 py-6 text-center text-slate-400'>No data</td></tr>"}</tbody>
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

  {paid_toggle_js}

</body>
</html>
"""


# =========================
# Hub page (구간비교 제거 버전)
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
    body{{ font-family:'Plus Jakarta Sans', system-ui, -apple-system, Segoe UI, Roboto, Arial; }}
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
                trend_svg=rt["trend_svg"],
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

    # ✅ Channel Snapshot: Looker CASE + Total 강제(KPI)
    channel_snapshot = get_channel_snapshot_3way(client, w, overall=overall)

    # ✅ Paid AD totals 추출(채널스냅샷 기반으로 강제용)
    paid_ad_totals = {
        "current": {"sessions": None, "revenue": None},
        "prev": {"sessions": None, "revenue": None},
        "yoy": {"sessions": None, "revenue": None},
    }
    try:
        # Channel snapshot은 current period값만 들어있고(prev/yoy는 %로만) → Paid Detail Total 강제는 current만 필요
        row = channel_snapshot[channel_snapshot["bucket"] == "Paid AD"]
        if not row.empty:
            paid_ad_totals["current"]["sessions"] = float(row.iloc[0]["sessions"])
            paid_ad_totals["current"]["revenue"] = float(row.iloc[0]["purchaseRevenue"])
    except Exception:
        pass

    paid_detail = get_paid_detail_3way(client, w, paid_ad_totals=paid_ad_totals)
    paid_top3 = get_paid_top3(client, w)
    other_detail = get_other_detail_3way(client, w)
    target_roas_map = load_target_roas_map(TARGET_ROAS_XLS_PATH)
    paid_media_compare = get_paid_media_comparison_table(client, w, target_roas_map)
    kpi_snapshot = get_kpi_snapshot_table_3way(client, w, overall)

    trend_series = get_trend_view_series(client, w)
    trend_svg = trend_svg_from_series(trend_series)

    best_sellers, best_sellers_series = get_best_sellers_with_trends(client, w, image_map)

    # ✅ Rising: Best Sellers SKU 제외 + prev>0 + delta>0
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
        trend_svg=trend_svg,
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

        # ✅✅✅ "전일(latest_end)"은 항상 강제 재생성 (SKIP_IF_EXISTS 무시)
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
