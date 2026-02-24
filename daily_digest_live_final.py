#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Columbia Daily Digest — Live GA4 Data (Google Analytics Data API) + BigQuery PDP Trend

✅ Updates in this version (your requests)
1) Weekly에서도 Best Sellers / Rising Products 표시
   - Weekly Best Sellers: 7D 누적 구매수량 TOP5 + 7D spark
   - Weekly Rising: (현재 7D - 직전 7D) 구매수량 Δ TOP5 + (가능하면) 7D views
2) Weekly 선택 시 비교군 = 전주(직전 7D) 대비(WoW)로 고정
3) 전년 동요일/동기간(YoY) 비교도 “한번에” 같이 표시
   - Daily: 전일(DoD) + YoY(전년 동요일, -364d)
   - Weekly: 전주(WoW) + YoY(전년 동기간, -364d)
4) Search Trend에 “검색 수(eventCount)” 표시 (신규/급상승 모두 count 포함)
5) Best Sellers에 (not set) 제거 + “상품이미지/아이템명 있는 것” 우선 노출
   - itemName이 비었거나 "(not set)"인 행 제외
   - 이미지 맵에 존재하는 SKU 우선 (부족하면 placeholder로 보완)
6) 동일 날짜 재실행이면 비용 절감:
   - 해당 날짜/모드의 HTML이 이미 존재하면 GA/BQ 쿼리 자체를 SKIP하고 기존 HTML 유지

Run (Windows PowerShell)
  setx GOOGLE_APPLICATION_CREDENTIALS "C:\\path\\service_account.json"
  setx GA4_PROPERTY_ID "358593394"
  pip install google-analytics-data pandas python-dateutil google-cloud-bigquery
  python daily_digest_live.py

Outputs
- reports/daily_digest/index.html
- reports/daily_digest/daily/YYYY-MM-DD.html
- reports/daily_digest/weekly/END_YYYY-MM-DD.html
"""

from __future__ import annotations

import os
import base64
import datetime as dt
from dataclasses import dataclass
from typing import List, Optional, Dict
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

# Cost-saving: if HTML exists for same date, skip all queries and keep HTML as-is
SKIP_IF_EXISTS = os.getenv("DAILY_DIGEST_SKIP_IF_EXISTS", "true").strip().lower() in ("1", "true", "yes", "y")

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
# SVG charts (keep as-is)
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
        print("[WARN] Image Excel path is empty. Set DAILY_DIGEST_IMAGE_XLS_PATH or keep file next to the script.")
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
            print("[WARN] Excel image map parsed 0 rows. Check SKU column & URL column.")
        return m
    except Exception as e:
        print(f"[WARN] Failed to load Excel image map: {type(e).__name__}: {e}")
        return {}


# =========================
# Window logic (daily vs weekly) + YoY
# =========================
@dataclass
class DigestWindow:
    mode: str                 # "daily" or "weekly"
    end_date: dt.date
    # main compare
    cur_start: dt.date
    cur_end: dt.date
    prev_start: dt.date
    prev_end: dt.date
    compare_label: str        # "DoD" or "WoW"
    # YoY compare (same weekday/period)
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
        # YoY same weekday: -364 days
        yoy_start = end_date - dt.timedelta(days=364)
        yoy_end = yoy_start
    else:
        cur_end = end_date
        cur_start = end_date - dt.timedelta(days=6)
        # Weekly compare = previous 7D (WoW)
        prev_end = end_date - dt.timedelta(days=7)
        prev_start = prev_end - dt.timedelta(days=6)
        compare_label = "WoW"
        # YoY same period: shift -364 days to preserve weekday alignment
        yoy_end = end_date - dt.timedelta(days=364)
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
# Data fetchers (now return current / prev / yoy)
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

    # ✅ FIX: YoY/Prev revenue deltas computed correctly from merged columns
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
    # weekly에서는 비용/해석 이슈로 기존대로 비워둠
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

def get_trend_view_svg(client: BetaAnalyticsDataClient, w: DigestWindow) -> str:
    end = w.cur_end
    start = end - dt.timedelta(days=6)

    df = run_report(client, PROPERTY_ID, ymd(start), ymd(end), ["date"], ["sessions","transactions","purchaseRevenue"])
    if df.empty:
        x = [(start + dt.timedelta(days=i)).strftime("%m/%d") for i in range(7)]
        return combined_index_svg(x, [[100]*7,[100]*7,[100]*7], ["#0055a5","#16a34a","#c2410c"], ["Sessions","Revenue","CVR"])

    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")
    for c in ["sessions","transactions","purchaseRevenue"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    df["cvr"] = df.apply(lambda r: (r["transactions"]/r["sessions"]) if r["sessions"] else 0.0, axis=1)
    x = df["date"].dt.strftime("%m/%d").tolist()
    s = index_series(df["sessions"].tolist())
    r = index_series(df["purchaseRevenue"].tolist())
    c = index_series(df["cvr"].tolist())
    return combined_index_svg(x, [s,r,c], ["#0055a5","#16a34a","#c2410c"], ["Sessions","Revenue","CVR"])


# =========================
# Best Sellers / Rising (Weekly enabled) + (not set) 제거 + 이미지 우선
# =========================
def get_best_sellers_with_trends(client: BetaAnalyticsDataClient, w: DigestWindow, image_map: Dict[str, str]) -> pd.DataFrame:
    """
    Daily: 어제(1D) Best Sellers TOP5 + 7D trend spark
    Weekly: 7D 누적 Best Sellers TOP5 + 7D trend spark

    (not set) 제거 + 이미지/아이템명 있는 것 우선:
      - 후보를 넉넉히(50개) 가져오고
      - itemName empty / (not set) 제거
      - image_map에 존재하는 SKU 우선 선택
    """
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
        return pd.DataFrame(columns=["itemId","itemName","itemsPurchased_yesterday","trend_svg","image_url"])

    cand["itemsPurchased"] = pd.to_numeric(cand["itemsPurchased"], errors="coerce").fillna(0.0)
    cand["itemId"] = cand["itemId"].astype(str).str.strip()
    cand["itemName"] = cand["itemName"].astype(str).fillna("").map(lambda x: x.strip())

    # remove (not set)
    cand = cand[~cand["itemName"].map(is_not_set)]
    if cand.empty:
        return pd.DataFrame(columns=["itemId","itemName","itemsPurchased_yesterday","trend_svg","image_url"])

    cand["image_url"] = cand["itemId"].map(lambda s: image_map.get(str(s).strip(), ""))

    # 이미지 있는 것 우선으로 TOP5, 부족하면 이미지 없는 것까지 채움
    with_img = cand[cand["image_url"].astype(str).str.strip() != ""].copy()
    no_img = cand[cand["image_url"].astype(str).str.strip() == ""].copy()

    with_img = with_img.sort_values("itemsPurchased", ascending=False)
    no_img = no_img.sort_values("itemsPurchased", ascending=False)

    top = pd.concat([with_img.head(5), no_img.head(max(0, 5 - len(with_img.head(5))))], ignore_index=True)
    top = top.head(5).copy()

    top = top.rename(columns={"itemsPurchased":"itemsPurchased_yesterday"})
    skus = [str(s).strip() for s in top["itemId"].tolist() if str(s).strip()]

    # 7D spark는 항상 end 기준 7일로 통일
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

    if ts.empty:
        top["trend_svg"] = ""
        if PLACEHOLDER_IMG:
            top.loc[top["image_url"].astype(str).str.strip() == "", "image_url"] = PLACEHOLDER_IMG
        return top[["itemId","itemName","itemsPurchased_yesterday","trend_svg","image_url"]]

    ts["date"] = ts["date"].apply(parse_yyyymmdd)
    ts["itemsPurchased"] = pd.to_numeric(ts["itemsPurchased"], errors="coerce").fillna(0.0)
    ts["itemId"] = ts["itemId"].astype(str).str.strip()
    ts = ts.sort_values(["itemId","date"])

    svgs = []
    for sku in skus:
        sub = ts[ts["itemId"] == sku].set_index("date")["itemsPurchased"]
        ys = [float(sub.get(d, 0.0)) for d in axis_dates]
        svgs.append(spark_svg(xlabels, ys, width=240, height=70, stroke="#0055a5"))

    top["trend_svg"] = svgs

    if PLACEHOLDER_IMG:
        top.loc[top["image_url"].astype(str).str.strip() == "", "image_url"] = PLACEHOLDER_IMG

    return top[["itemId","itemName","itemsPurchased_yesterday","trend_svg","image_url"]]


def get_rising_products(client: BetaAnalyticsDataClient, w: DigestWindow, top_n: int = 5) -> pd.DataFrame:
    """
    Daily: (어제 qty - 전일 qty) 큰 순 TOPN
    Weekly: (현재 7D qty - 직전 7D qty) 큰 순 TOPN
    Views: daily=어제, weekly=현재 7D (가능 metric best-effort)
    """
    if w.mode == "daily":
        cur_start, cur_end = w.cur_end, w.cur_end
        prev_start, prev_end = w.prev_end, w.prev_end
    else:
        cur_start, cur_end = w.cur_start, w.cur_end
        prev_start, prev_end = w.prev_start, w.prev_end

    d1 = run_report(client, PROPERTY_ID, ymd(cur_start), ymd(cur_end), ["itemId", "itemName"], ["itemsPurchased"], limit=10000)
    d0 = run_report(client, PROPERTY_ID, ymd(prev_start), ymd(prev_end), ["itemId"], ["itemsPurchased"], limit=10000)

    if d1.empty:
        return pd.DataFrame(columns=["itemId", "itemName", "itemViews_yesterday", "delta"])

    d1["itemsPurchased"] = pd.to_numeric(d1["itemsPurchased"], errors="coerce").fillna(0.0)
    d1["itemId"] = d1["itemId"].astype(str).str.strip()
    d1["itemName"] = d1["itemName"].astype(str).fillna("").map(lambda x: x.strip())

    # remove (not set) for nicer list
    d1 = d1[~d1["itemName"].map(is_not_set)]
    if d1.empty:
        return pd.DataFrame(columns=["itemId", "itemName", "itemViews_yesterday", "delta"])

    if not d0.empty:
        d0["itemsPurchased"] = pd.to_numeric(d0["itemsPurchased"], errors="coerce").fillna(0.0)
        d0["itemId"] = d0["itemId"].astype(str).str.strip()
    else:
        d0 = pd.DataFrame(columns=["itemId", "itemsPurchased"])

    m = d1.merge(d0, on="itemId", how="left", suffixes=("_cur", "_prev")).fillna(0.0)
    m["delta"] = m["itemsPurchased_cur"] - m["itemsPurchased_prev"]
    m = m.sort_values("delta", ascending=False).head(top_n)

    # Views best-effort
    skus = [str(x).strip() for x in m["itemId"].tolist() if str(x).strip()]
    views_df = pd.DataFrame(columns=["itemId", "itemViews_yesterday"])

    if skus:
        for metric_name, use_event_filter in [
            ("itemViewEvents", False),
            ("itemsViewed", False),
            ("eventCount", True),
        ]:
            try:
                v = run_report(
                    client, PROPERTY_ID, ymd(cur_start), ymd(cur_end),
                    ["itemId"], [metric_name],
                    dimension_filter=(
                        ga_filter_and([ga_filter_in("itemId", skus), ga_filter_eq("eventName", "view_item")])
                        if use_event_filter else ga_filter_in("itemId", skus)
                    ),
                    limit=10000,
                )
                if not v.empty:
                    v["itemId"] = v["itemId"].astype(str).str.strip()
                    v[metric_name] = pd.to_numeric(v[metric_name], errors="coerce").fillna(0.0)
                    views_df = v[["itemId", metric_name]].rename(columns={metric_name: "itemViews_yesterday"})
                break
            except Exception:
                continue

    m = m.merge(views_df, on="itemId", how="left")
    m["itemViews_yesterday"] = pd.to_numeric(m.get("itemViews_yesterday"), errors="coerce").fillna(0.0)
    return m[["itemId", "itemName", "itemViews_yesterday", "delta"]]


# =========================
# PDP Trend (BigQuery) + Diagnostics
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

def get_category_pdp_view_trend_bq(end_date: dt.date) -> pd.DataFrame:
    axis_dates = [end_date - dt.timedelta(days=i) for i in range(6, -1, -1)]
    xlabels = [d.strftime('%m/%d') for d in axis_dates]

    if bigquery is None or not BQ_EVENTS_TABLE:
        print("[WARN] BigQuery not available; PDP Trend will be empty.")
        return pd.DataFrame(columns=["itemCategory", "views_d1", "views_avg7d", "trend_svg"])

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
            return pd.DataFrame(columns=["itemCategory", "views_d1", "views_avg7d", "trend_svg"])

        df['d'] = pd.to_datetime(df['d'], errors='coerce').dt.date
        df = df.dropna(subset=['d'])
        df['c1'] = df['c1'].astype(str).str.strip().str.upper()
        df['c2'] = df['c2'].astype(str).str.strip()
        df['views'] = pd.to_numeric(df['views'], errors="coerce").fillna(0.0)

        rows = []
        for c1, subs in PDP_CATEGORY_MAP.items():
            for sub_label, c2_list in subs.items():
                ys = []
                for d in axis_dates:
                    m = (df['d'] == d) & (df['c1'] == c1) & (df['c2'].isin(c2_list))
                    ys.append(float(df.loc[m, 'views'].sum()))

                d1 = ys[-1] if ys else 0.0
                avg7 = (sum(ys) / len(ys)) if ys else 0.0
                rows.append({
                    "itemCategory": f"{c1} · {sub_label}",
                    "views_d1": float(d1),
                    "views_avg7d": float(avg7),
                    "trend_svg": spark_svg(xlabels, ys, width=260, height=70, stroke="#0f766e"),
                })

        return pd.DataFrame(rows)
    except Exception as e:
        print(f"[WARN] PDP Trend BigQuery failed: {type(e).__name__}: {e}")
        return pd.DataFrame(columns=["itemCategory", "views_d1", "views_avg7d", "trend_svg"])


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
# UI
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
) -> str:
    cur = overall["current"]; prev = overall["prev"]; yoy = overall["yoy"]

    # primary compare: daily=DoD, weekly=WoW
    s_delta = pct_change(cur["sessions"], prev["sessions"])
    o_delta = pct_change(cur["transactions"], prev["transactions"])
    r_delta = pct_change(cur["purchaseRevenue"], prev["purchaseRevenue"])
    c_pp = cur["cvr"] - prev["cvr"]

    # YoY
    s_yoy = pct_change(cur["sessions"], yoy["sessions"])
    o_yoy = pct_change(cur["transactions"], yoy["transactions"])
    r_yoy = pct_change(cur["purchaseRevenue"], yoy["purchaseRevenue"])
    c_yoy_pp = cur["cvr"] - yoy["cvr"]

    su_cur = float(signup_users.get("current", 0.0) or 0.0)
    su_prev = float(signup_users.get("prev", 0.0) or 0.0)
    su_yoy = float(signup_users.get("yoy", 0.0) or 0.0)
    su_delta = pct_change(su_cur, su_prev)
    su_yoy_delta = pct_change(su_cur, su_yoy)

    def delta_cls(v: float) -> str:
        return "text-blue-600" if v >= 0 else "text-orange-700"

    def top_kpi_card(title: str, value: str, delta_main: str, delta_yoy_s: str, cls_main: str, cls_yoy: str) -> str:
        return f"""
        <div class="rounded-2xl border border-slate-200 bg-white/70 p-4">
          <div class="text-[11px] font-extrabold tracking-widest text-slate-500 uppercase">{title}</div>
          <div class="mt-1 text-xl font-black text-slate-900">{value}</div>
          <div class="mt-1 text-[11px] text-slate-500">{w.compare_label} <b class="{cls_main}">{delta_main}</b> · YoY <b class="{cls_yoy}">{delta_yoy_s}</b></div>
        </div>
        """

    def product_img(url: str) -> str:
        u = (url or PLACEHOLDER_IMG or "").strip()
        if u:
            return f"<img src='{u}' class='w-8 h-8 rounded-xl object-cover border border-slate-200'/>"
        return "<div class='w-8 h-8 rounded-xl bg-slate-100 border border-slate-200'></div>"

    def table_row(cols: List[str], bold=False) -> str:
        fw = "font-extrabold" if bold else "font-medium"
        bg = "bg-slate-50" if bold else ""
        tds = "".join([f"<td class='px-3 py-2 border-b border-slate-100 {fw}'>{c}</td>" for c in cols])
        return f"<tr class='{bg}'>{tds}</tr>"

    # Channel rows
    chan_html = ""
    for r in channel_snapshot.itertuples(index=False):
        chan_html += table_row([
            str(r.bucket),
            f"<div class='text-right'>{fmt_int(r.sessions)}</div>",
            f"<div class='text-right'>{fmt_int(r.transactions)}</div>",
            f"<div class='text-right'>{fmt_currency_krw(r.purchaseRevenue)}</div>",
            f"<div class='text-right {delta_cls(r.rev_vs_prev)}'>{('+' if r.rev_vs_prev>=0 else '')}{fmt_pct(r.rev_vs_prev,1)}</div>",
            f"<div class='text-right {delta_cls(r.rev_yoy)}'>{('+' if r.rev_yoy>=0 else '')}{fmt_pct(r.rev_yoy,1)}</div>",
        ], bold=(r.bucket == "Total"))

    paid_html = ""
    for r in paid_detail.itertuples(index=False):
        paid_html += table_row([
            str(r.sub_channel),
            f"<div class='text-right'>{fmt_int(r.sessions)}</div>",
            f"<div class='text-right'>{fmt_currency_krw(r.purchaseRevenue)}</div>",
            f"<div class='text-right {delta_cls(r.rev_vs_prev)}'>{('+' if r.rev_vs_prev>=0 else '')}{fmt_pct(r.rev_vs_prev,1)}</div>",
            f"<div class='text-right {delta_cls(r.rev_yoy)}'>{('+' if r.rev_yoy>=0 else '')}{fmt_pct(r.rev_yoy,1)}</div>",
        ], bold=(r.sub_channel == "Total"))

    paid_top3_html = ""
    if not paid_top3.empty:
        for r in paid_top3.itertuples(index=False):
            paid_top3_html += table_row([
                str(r.sessionSourceMedium),
                f"<div class='text-right'>{fmt_int(getattr(r,'sessions',0))}</div>",
                f"<div class='text-right'>{fmt_currency_krw(r.purchaseRevenue)}</div>",
            ], bold=(r.sessionSourceMedium == "Total"))

    # KPI Snapshot rows (main + yoy)
    kpi_html = ""
    for r in kpi_snapshot.itertuples(index=False):
        kpi_html += table_row([
            str(r.metric),
            f"<div class='text-right'>{r.value_fmt}</div>",
            f"<div class='text-right {delta_cls(r.delta_prev)}'>{r.delta_prev_fmt}</div>",
            f"<div class='text-right {delta_cls(r.delta_yoy)}'>{r.delta_yoy_fmt}</div>",
        ])

    bs_html = ""
    if not best_sellers.empty:
        for r in best_sellers.itertuples(index=False):
            bs_html += f"""
            <tr>
              <td class="px-3 py-2 border-b border-slate-100">{product_img(getattr(r,'image_url',''))}</td>
              <td class="px-3 py-2 border-b border-slate-100">{getattr(r,'itemName',None) or '—'}</td>
              <td class="px-3 py-2 border-b border-slate-100">{getattr(r,'itemId',None) or '—'}</td>
              <td class="px-3 py-2 border-b border-slate-100 text-right font-semibold">{fmt_int(getattr(r,'itemsPurchased_yesterday',0))}</td>
              <td class="px-3 py-2 border-b border-slate-100">{getattr(r,'trend_svg','')}</td>
            </tr>
            """

    rp_html = ""
    if not rising.empty:
        for r in rising.itertuples(index=False):
            delta = float(getattr(r, "delta", 0) or 0.0)
            rp_html += f"""
            <tr>
              <td class="px-3 py-2 border-b border-slate-100">{product_img(getattr(r,'image_url',''))}</td>
              <td class="px-3 py-2 border-b border-slate-100">{getattr(r,'itemId',None) or '—'}</td>
              <td class="px-3 py-2 border-b border-slate-100">{getattr(r,'itemName',None) or '—'}</td>
              <td class="px-3 py-2 border-b border-slate-100 text-right">{fmt_int(getattr(r,'itemViews_yesterday',0))}</td>
              <td class="px-3 py-2 border-b border-slate-100 text-right font-extrabold {delta_cls(delta)}">
                {'▲' if delta>=0 else '▼'} {fmt_int(abs(delta))}
              </td>
            </tr>
            """

    pdp_html = ""
    if not category_pdp_trend.empty:
        for r in category_pdp_trend.itertuples(index=False):
            pdp_html += f"""
            <tr>
              <td class="px-3 py-2 border-b border-slate-100">{r.itemCategory}</td>
              <td class="px-3 py-2 border-b border-slate-100">
                <div class="text-[11px] text-slate-500 mb-1">
                  D-1 <b class="text-slate-900">{fmt_int(getattr(r,'views_d1',0))}</b> ·
                  7D Avg <b class="text-slate-900">{fmt_int(getattr(r,'views_avg7d',0))}</b>
                </div>
                {getattr(r,'trend_svg','')}
              </td>
            </tr>
            """
    else:
        pdp_html = """
        <tr>
          <td class="px-3 py-4 text-slate-400" colspan="2">
            PDP Trend 데이터가 없습니다. (BigQuery view_item에 items[].item_id가 없으면 비어있게 됩니다)
          </td>
        </tr>
        """

    def kw_rows(df: pd.DataFrame, mode: str) -> str:
        if df.empty:
            return "<tr><td class='px-3 py-2 border-b border-slate-100 text-slate-400'>—</td><td class='px-3 py-2 border-b border-slate-100 text-right text-slate-400'>—</td></tr>"
        out = ""
        for r in df.itertuples(index=False):
            if mode == "new":
                out += f"<tr><td class='px-3 py-2 border-b border-slate-100'>{r.searchTerm}</td><td class='px-3 py-2 border-b border-slate-100 text-right text-slate-500'>{fmt_int(getattr(r,'count',0))}</td></tr>"
            else:
                tag = f"{'+' if r.pct>=0 else ''}{r.pct:.0f}%"
                out += f"<tr><td class='px-3 py-2 border-b border-slate-100'>{r.searchTerm}</td><td class='px-3 py-2 border-b border-slate-100 text-right text-slate-500'>{tag} · {fmt_int(getattr(r,'count',0))}</td></tr>"
        return out

    mode_badge = "Daily" if w.mode == "daily" else "Weekly (7D Cumulative)"
    period_text = f"{ymd(w.cur_start)} ~ {ymd(w.cur_end)}" if w.mode == "weekly" else f"{ymd(w.end_date)}"

    qty_label = "Qty (D-1)" if w.mode == "daily" else "Qty (7D)"
    cmp_label = w.compare_label  # DoD or WoW

    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>CSK E-COMM | Daily Digest</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200;400;600;800&display=swap');
    :root {{ --brand:#002d72; }}
    body{{ background: linear-gradient(180deg, #f6f8fb, #eef3f9); font-family:'Plus Jakarta Sans',sans-serif; color:#0f172a; }}
    .glass-card{{ background: rgba(255,255,255,0.70); backdrop-filter: blur(14px); border: 1px solid rgba(15,23,42,0.06); box-shadow: 0 16px 50px rgba(15,23,42,0.08); }}
    .badge{{ font-size:11px; font-weight:900; padding:6px 10px; border-radius:999px; background: rgba(0,45,114,.08); color: var(--brand); }}
    .badge-soft{{ font-size:11px; font-weight:900; padding:6px 10px; border-radius:999px; background: rgba(15,23,42,.06); color: rgba(15,23,42,.70); }}
  </style>
</head>
<body>
  <div class="px-3 sm:px-6 py-6 max-w-[1200px] mx-auto">

    <div class="mb-5 flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
      <div class="flex items-center gap-3">
        {"<img src='data:image/png;base64," + logo_b64 + "' class='h-8 w-auto'/>" if logo_b64 else ""}
        <div>
          <div class="text-2xl sm:text-3xl font-black tracking-tight">Daily Digest</div>
          <div class="text-sm text-slate-500">eCommerce Performance · {mode_badge} · <b class="text-slate-700">{period_text}</b></div>
        </div>
      </div>

      <div class="flex items-center gap-2">
        <a href="{nav_links.get('hub','index.html')}" class="px-4 py-2 rounded-2xl glass-card font-extrabold text-sm">Hub</a>
        <a href="{nav_links.get('daily_index','index.html')}" class="px-4 py-2 rounded-2xl glass-card font-extrabold text-sm">Daily</a>
        <a href="{nav_links.get('weekly_index','index.html')}" class="px-4 py-2 rounded-2xl glass-card font-extrabold text-sm">Weekly</a>
        <span class="badge-soft">{mode_badge}</span>
      </div>
    </div>

    <div class="glass-card rounded-3xl p-5 mb-6">
      <div class="flex items-center justify-between">
        <div class="text-base font-black text-slate-900">Top KPIs</div>
        <span class="badge">{ymd(w.end_date)} 기준</span>
      </div>

      <div class="mt-4 grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3">
        {top_kpi_card("Sessions", fmt_int(cur["sessions"]),
                     f"{'+' if s_delta>=0 else ''}{fmt_pct(s_delta,1)}",
                     f"{'+' if s_yoy>=0 else ''}{fmt_pct(s_yoy,1)}",
                     delta_cls(s_delta), delta_cls(s_yoy))}
        {top_kpi_card("Orders", fmt_int(cur["transactions"]),
                     f"{'+' if o_delta>=0 else ''}{fmt_pct(o_delta,1)}",
                     f"{'+' if o_yoy>=0 else ''}{fmt_pct(o_yoy,1)}",
                     delta_cls(o_delta), delta_cls(o_yoy))}
        {top_kpi_card("Revenue", fmt_currency_krw(cur["purchaseRevenue"]),
                     f"{'+' if r_delta>=0 else ''}{fmt_pct(r_delta,1)}",
                     f"{'+' if r_yoy>=0 else ''}{fmt_pct(r_yoy,1)}",
                     delta_cls(r_delta), delta_cls(r_yoy))}
        {top_kpi_card("CVR", f"{cur['cvr']*100:.2f}%",
                     fmt_pp(c_pp,2),
                     fmt_pp(c_yoy_pp,2),
                     delta_cls(c_pp), delta_cls(c_yoy_pp))}
        {top_kpi_card("Sign-up Users", fmt_int(su_cur),
                     f"{'+' if su_delta>=0 else ''}{fmt_pct(su_delta,1)}",
                     f"{'+' if su_yoy_delta>=0 else ''}{fmt_pct(su_yoy_delta,1)}",
                     delta_cls(su_delta), delta_cls(su_yoy_delta))}
      </div>

      <div class="mt-3 text-xs text-slate-500">
        비교 기준: <b class="text-slate-700">{cmp_label}</b> ({"전일" if w.mode=="daily" else "전주(직전 7D)"} 대비) ·
        <b class="text-slate-700">YoY</b> (전년 {"동요일" if w.mode=="daily" else "동기간"} 대비)
      </div>
    </div>

    <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">

      <div class="glass-card rounded-3xl p-6">
        <div class="flex items-center justify-between">
          <div class="text-lg font-black">Channel Snapshot</div>
          <span class="badge-soft">Rev {cmp_label} + YoY</span>
        </div>
        <div class="mt-4 overflow-x-auto">
          <table class="w-full text-sm">
            <thead>
              <tr class="bg-slate-50">
                <th class="px-3 py-2 text-left text-xs tracking-widest uppercase text-slate-500">Channel</th>
                <th class="px-3 py-2 text-right text-xs tracking-widest uppercase text-slate-500">Traffic</th>
                <th class="px-3 py-2 text-right text-xs tracking-widest uppercase text-slate-500">Orders</th>
                <th class="px-3 py-2 text-right text-xs tracking-widest uppercase text-slate-500">Revenue</th>
                <th class="px-3 py-2 text-right text-xs tracking-widest uppercase text-slate-500">{cmp_label} (Rev)</th>
                <th class="px-3 py-2 text-right text-xs tracking-widest uppercase text-slate-500">YoY (Rev)</th>
              </tr>
            </thead>
            <tbody>{chan_html}</tbody>
          </table>
        </div>
      </div>

      <div class="glass-card rounded-3xl p-6">
        <div class="flex items-center justify-between">
          <div class="text-lg font-black">Paid AD Detail</div>
          <span class="badge-soft">Rev {cmp_label} + YoY</span>
        </div>

        <div class="mt-4 overflow-x-auto">
          <table class="w-full text-sm">
            <thead>
              <tr class="bg-slate-50">
                <th class="px-3 py-2 text-left text-xs tracking-widest uppercase text-slate-500">Sub-channel</th>
                <th class="px-3 py-2 text-right text-xs tracking-widest uppercase text-slate-500">Traffic</th>
                <th class="px-3 py-2 text-right text-xs tracking-widest uppercase text-slate-500">Revenue</th>
                <th class="px-3 py-2 text-right text-xs tracking-widest uppercase text-slate-500">{cmp_label} (Rev)</th>
                <th class="px-3 py-2 text-right text-xs tracking-widest uppercase text-slate-500">YoY (Rev)</th>
              </tr>
            </thead>
            <tbody>{paid_html}</tbody>
          </table>
        </div>

        {"<div class='mt-5'><div class='text-sm font-black mb-2'>Paid AD Top 3 (Daily only)</div><div class='overflow-x-auto'><table class='w-full text-sm'><thead><tr class='bg-slate-50'><th class='px-3 py-2 text-left text-xs tracking-widest uppercase text-slate-500'>Source / Medium</th><th class='px-3 py-2 text-right text-xs tracking-widest uppercase text-slate-500'>Traffic</th><th class='px-3 py-2 text-right text-xs tracking-widest uppercase text-slate-500'>Revenue</th></tr></thead><tbody>"+paid_top3_html+"</tbody></table></div></div>" if (w.mode=='daily' and paid_top3_html) else ""}
      </div>

      <div class="glass-card rounded-3xl p-6">
        <div class="flex items-center justify-between">
          <div class="text-lg font-black">KPI Snapshot</div>
          <span class="badge-soft">{cmp_label} + YoY</span>
        </div>
        <div class="mt-4 overflow-x-auto">
          <table class="w-full text-sm">
            <thead>
              <tr class="bg-slate-50">
                <th class="px-3 py-2 text-left text-xs tracking-widest uppercase text-slate-500">Metric</th>
                <th class="px-3 py-2 text-right text-xs tracking-widest uppercase text-slate-500">Value</th>
                <th class="px-3 py-2 text-right text-xs tracking-widest uppercase text-slate-500">{cmp_label}</th>
                <th class="px-3 py-2 text-right text-xs tracking-widest uppercase text-slate-500">YoY</th>
              </tr>
            </thead>
            <tbody>{kpi_html}</tbody>
          </table>
        </div>
      </div>

      <div class="glass-card rounded-3xl p-6">
        <div class="flex items-center justify-between">
          <div class="text-lg font-black">Trend View</div>
          <span class="badge-soft">Index (D-7=100)</span>
        </div>
        <div class="mt-4 rounded-2xl border border-slate-200 bg-white/60 p-3">
          {trend_svg}
        </div>
      </div>

      <div class="glass-card rounded-3xl p-6">
        <div class="flex items-center justify-between">
          <div class="text-lg font-black">Best Sellers · TOP 5 ({qty_label})</div>
          <span class="badge-soft">{mode_badge}</span>
        </div>
        <div class="mt-4 overflow-x-auto">
          <table class="w-full text-sm">
            <thead>
              <tr class="bg-slate-50">
                <th class="px-3 py-2 text-left text-xs tracking-widest uppercase text-slate-500">Image</th>
                <th class="px-3 py-2 text-left text-xs tracking-widest uppercase text-slate-500">Item</th>
                <th class="px-3 py-2 text-left text-xs tracking-widest uppercase text-slate-500">SKU</th>
                <th class="px-3 py-2 text-right text-xs tracking-widest uppercase text-slate-500">{qty_label}</th>
                <th class="px-3 py-2 text-left text-xs tracking-widest uppercase text-slate-500">7D Trend</th>
              </tr>
            </thead>
            <tbody>
              {bs_html if bs_html else "<tr><td class='px-3 py-4 text-slate-400' colspan='5'>데이터가 없습니다.</td></tr>"}
            </tbody>
          </table>
        </div>
      </div>

      <div class="glass-card rounded-3xl p-6">
        <div class="flex items-center justify-between">
          <div class="text-lg font-black">Rising Products</div>
          <span class="badge-soft">{mode_badge} · Δ Qty ({cmp_label})</span>
        </div>
        <div class="mt-4 overflow-x-auto">
          <table class="w-full text-sm">
            <thead>
              <tr class="bg-slate-50">
                <th class="px-3 py-2 text-left text-xs tracking-widest uppercase text-slate-500">Image</th>
                <th class="px-3 py-2 text-left text-xs tracking-widest uppercase text-slate-500">SKU</th>
                <th class="px-3 py-2 text-left text-xs tracking-widest uppercase text-slate-500">Item</th>
                <th class="px-3 py-2 text-right text-xs tracking-widest uppercase text-slate-500">Views</th>
                <th class="px-3 py-2 text-right text-xs tracking-widest uppercase text-slate-500">Qty Δ</th>
              </tr>
            </thead>
            <tbody>
              {rp_html if rp_html else "<tr><td class='px-3 py-4 text-slate-400' colspan='5'>데이터가 없습니다.</td></tr>"}
            </tbody>
          </table>
        </div>
      </div>

      <div class="glass-card rounded-3xl p-6">
        <div class="flex items-center justify-between">
          <div class="text-lg font-black">PDP Trend</div>
          <span class="badge-soft">BigQuery · view_item</span>
        </div>
        <div class="mt-4 overflow-x-auto">
          <table class="w-full text-sm">
            <thead>
              <tr class="bg-slate-50">
                <th class="px-3 py-2 text-left text-xs tracking-widest uppercase text-slate-500">Category</th>
                <th class="px-3 py-2 text-left text-xs tracking-widest uppercase text-slate-500">PDP Views 7D Trend</th>
              </tr>
            </thead>
            <tbody>{pdp_html}</tbody>
          </table>
        </div>
      </div>

      <div class="glass-card rounded-3xl p-6 lg:col-span-2">
        <div class="flex items-center justify-between">
          <div class="text-lg font-black">Search Trend</div>
          <span class="badge-soft">{ymd(w.end_date)}</span>
        </div>
        <div class="mt-4 grid grid-cols-1 md:grid-cols-2 gap-4">
          <div class="rounded-2xl border border-slate-200 bg-white/60">
            <div class="px-4 py-3 font-extrabold text-sm border-b border-slate-200">신규 진입 Top 3 (count)</div>
            <table class="w-full text-sm"><tbody>{kw_rows(search_new, "new")}</tbody></table>
          </div>
          <div class="rounded-2xl border border-slate-200 bg-white/60">
            <div class="px-4 py-3 font-extrabold text-sm border-b border-slate-200">급상승 Top 3 (% · count)</div>
            <table class="w-full text-sm"><tbody>{kw_rows(search_rising, "rising")}</tbody></table>
          </div>
        </div>
      </div>

    </div>

    <div class="mt-6 text-xs text-slate-400 text-right">
      Auto-generated · {mode_badge} · End = {ymd(w.end_date)} · Compare={cmp_label} · YoY shift=-364d
    </div>

  </div>
</body>
</html>
"""


# =========================
# Hub pages
# =========================
def render_hub_index(dates: List[dt.date]) -> str:
    date_links = "\n".join([
        f"<a class='px-4 py-2 rounded-2xl border border-slate-200 bg-white/70 font-extrabold' href='daily/{ymd(d)}.html'>{ymd(d)}</a>"
        for d in dates
    ])
    week_links = "\n".join([
        f"<a class='px-4 py-2 rounded-2xl border border-slate-200 bg-white/70 font-extrabold' href='weekly/END_{ymd(d)}.html'>END {ymd(d)}</a>"
        for d in dates
    ])

    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Daily Digest Hub</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200;400;600;800&display=swap');
    body{{ background: linear-gradient(180deg, #f6f8fb, #eef3f9); font-family:'Plus Jakarta Sans',sans-serif; color:#0f172a; }}
    .glass-card{{ background: rgba(255,255,255,0.70); backdrop-filter: blur(14px); border: 1px solid rgba(15,23,42,0.06); box-shadow: 0 16px 50px rgba(15,23,42,0.08); }}
  </style>
</head>
<body>
  <div class="px-3 sm:px-6 py-6 max-w-[1100px] mx-auto">
    <div class="mb-6">
      <div class="text-3xl font-black tracking-tight">Daily Digest Hub</div>
      <div class="text-sm text-slate-500">최근 생성된 리포트로 이동</div>
    </div>

    <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
      <div class="glass-card rounded-3xl p-6">
        <div class="text-lg font-black">Daily Reports</div>
        <div class="mt-4 flex flex-wrap gap-2">{date_links}</div>
      </div>

      <div class="glass-card rounded-3xl p-6">
        <div class="text-lg font-black">Weekly (7D Cumulative)</div>
        <div class="mt-4 text-xs text-slate-500 mb-3">END 날짜 기준으로 7일 누적 (비교: 전주 / YoY)</div>
        <div class="flex flex-wrap gap-2">{week_links}</div>
      </div>
    </div>
  </div>
</body>
</html>
"""


# =========================
# Build one report
# =========================
def build_one(client: BetaAnalyticsDataClient, end_date: dt.date, mode: str, image_map: Dict[str, str], logo_b64: str) -> str:
    w = build_window(end_date=end_date, mode=mode)

    overall = get_overall_kpis(client, w)
    signup_users = get_multi_event_users_3way(client, w, ["signup_complete", "signup"])

    channel_snapshot = get_channel_snapshot_3way(client, w)
    paid_detail = get_paid_detail_3way(client, w)
    paid_top3 = get_paid_top3(client, w)

    kpi_snapshot = get_kpi_snapshot_table_3way(client, w, overall)
    trend_svg = get_trend_view_svg(client, w)

    best_sellers = get_best_sellers_with_trends(client, w, image_map)

    rising = get_rising_products(client, w, top_n=5)
    rising = attach_image_urls(rising, image_map)
    if PLACEHOLDER_IMG and (not rising.empty) and ("image_url" in rising.columns):
        rising.loc[rising["image_url"].astype(str).str.strip() == "", "image_url"] = PLACEHOLDER_IMG

    # Missing image tracking
    missing = []
    if not best_sellers.empty and 'itemId' in best_sellers.columns:
        missing += [sku for sku in best_sellers['itemId'].tolist() if str(sku).strip() not in image_map]
    if not rising.empty and 'itemId' in rising.columns:
        missing += [sku for sku in rising['itemId'].tolist() if str(sku).strip() not in image_map]
    if missing:
        write_missing_image_skus(MISSING_SKU_OUT, missing)

    category_pdp_trend = get_category_pdp_view_trend_bq(end_date=end_date)

    search = get_search_trends(client, end_date=end_date)

    nav_links = {
        "hub": "../index.html",
        "daily_index": "../index.html",
        "weekly_index": "../index.html",
    }

    return render_page_html(
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
        nav_links=nav_links,
    )


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

    for d in dates:
        out_daily = os.path.join(daily_dir, f"{ymd(d)}.html")
        out_weekly = os.path.join(weekly_dir, f"END_{ymd(d)}.html")

        # Cost saving: if exists, skip queries and keep existing HTML
        if SKIP_IF_EXISTS and os.path.exists(out_daily):
            print(f"[SKIP] Exists (Daily): {out_daily}")
        else:
            html_daily = build_one(client, end_date=d, mode="daily", image_map=image_map, logo_b64=logo_b64)
            with open(out_daily, "w", encoding="utf-8") as f:
                f.write(html_daily)
            print(f"[OK] Wrote: {out_daily}")

        if SKIP_IF_EXISTS and os.path.exists(out_weekly):
            print(f"[SKIP] Exists (Weekly): {out_weekly}")
        else:
            html_weekly = build_one(client, end_date=d, mode="weekly", image_map=image_map, logo_b64=logo_b64)
            with open(out_weekly, "w", encoding="utf-8") as f:
                f.write(html_weekly)
            print(f"[OK] Wrote: {out_weekly}")

    # Hub index (❗️index.html 덮어쓰기 방지: 허브가 예전 버전으로 돌아가는 현상 차단)
    # 기본 동작: index.html이 이미 있으면 절대 덮어쓰지 않음
    # 필요할 때만 env로 강제 갱신:
    #   DAILY_DIGEST_FORCE_HUB_OVERWRITE=true

    hub_path = os.path.join(OUT_DIR, "index.html")
    force_overwrite = os.getenv("DAILY_DIGEST_FORCE_HUB_OVERWRITE", "false").strip().lower() in ("1", "true", "yes", "y")

    if (not force_overwrite) and os.path.exists(hub_path):
        print(f"[SKIP] HUB exists (no overwrite): {hub_path}")
    else:
        hub = render_hub_index(dates=dates)
        with open(hub_path, "w", encoding="utf-8") as f:
            f.write(hub)
        print(f"[OK] Wrote HUB: {hub_path}")


if __name__ == "__main__":
    main()
