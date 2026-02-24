# -*- coding: utf-8 -*-
"""
Columbia Daily Digest — Live GA4 Data (Google Analytics Data API)

Run (Windows PowerShell)
  setx GOOGLE_APPLICATION_CREDENTIALS "C:\\path\\service_account.json"
  setx GA4_PROPERTY_ID "358593394"
  pip install google-analytics-data pandas python-dateutil google-cloud-bigquery
  python daily_digest_live.py

Outputs (NEW)
- reports/daily_digest/index.html                       (navigation hub)
- reports/daily_digest/daily/YYYY-MM-DD.html           (daily pages)
- reports/daily_digest/weekly/END_YYYY-MM-DD.html      (weekly cumulative pages)

Notes
- Weekly = 7D cumulative (END date 기준, END-6 ~ END)
  - Previous = 그 직전 7D (END-13 ~ END-7)
- PDP Trend uses BigQuery GA4 export (view_item + UNNEST(items)).
  - If your view_item events do NOT contain items[] (item_id), PDP Trend will be empty (diagnostic logs added).
"""

from __future__ import annotations

import os
import base64
import datetime as dt
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
import re
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

# NEW: output directory
OUT_DIR = os.getenv("DAILY_DIGEST_OUT_DIR", os.path.join("reports", "daily_digest")).strip()
DAYS_TO_BUILD = int(os.getenv("DAILY_DIGEST_BUILD_DAYS", "14"))  # recent N days

# Image mapping Excel (SKU -> image URL)
IMAGE_XLS_PATH = os.getenv("DAILY_DIGEST_IMAGE_XLS_PATH", "상품코드별 이미지.xlsx").strip()
MISSING_SKU_OUT = os.getenv("DAILY_DIGEST_MISSING_SKU_OUT", "missing_image_skus.csv")
PLACEHOLDER_IMG = os.getenv("DAILY_DIGEST_PLACEHOLDER_IMG", "")

# BigQuery GA4 export wildcard table
BQ_EVENTS_TABLE = os.getenv("DAILY_DIGEST_BQ_EVENTS_TABLE", "columbia-ga4.analytics_358593394.events_*").strip()
BQ_LOCATION = os.getenv("DAILY_DIGEST_BQ_LOCATION", "asia-northeast3").strip()

SIGNUP_EVENT = os.getenv("DAILY_DIGEST_SIGNUP_EVENT", "sign_up")
LOGIN_EVENT = os.getenv("DAILY_DIGEST_LOGIN_EVENT", "login")
SEARCH_EVENT = os.getenv("DAILY_DIGEST_SEARCH_EVENT", "view_search_results")

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

        # fallback to fixed C/E
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
# Window logic (NEW: daily vs weekly)
# =========================
@dataclass
class DigestWindow:
    mode: str                 # "daily" or "weekly"
    end_date: dt.date         # report end date (Daily: that day, Weekly: that day = 7D end)
    prev_end_date: dt.date    # previous comparable end date (Daily: end-1, Weekly: end-7)
    cur_start: dt.date
    cur_end: dt.date
    prev_start: dt.date
    prev_end: dt.date

def build_window(end_date: dt.date, mode: str) -> DigestWindow:
    mode = (mode or "daily").lower().strip()
    if mode not in ("daily", "weekly"):
        mode = "daily"

    if mode == "daily":
        cur_start = end_date
        cur_end = end_date
        prev_end = end_date - dt.timedelta(days=1)
        prev_start = prev_end
        prev_end_date = prev_end
    else:
        # weekly cumulative: 7D ending at end_date
        cur_end = end_date
        cur_start = end_date - dt.timedelta(days=6)
        prev_end = end_date - dt.timedelta(days=7)
        prev_start = prev_end - dt.timedelta(days=6)
        prev_end_date = prev_end

    return DigestWindow(
        mode=mode,
        end_date=end_date,
        prev_end_date=prev_end_date,
        cur_start=cur_start,
        cur_end=cur_end,
        prev_start=prev_start,
        prev_end=prev_end
    )


# =========================
# Data fetchers
# =========================
def get_overall_kpis(client: BetaAnalyticsDataClient, w: DigestWindow) -> Dict[str, Dict[str, float]]:
    mets = ["sessions", "transactions", "purchaseRevenue"]

    d1 = run_report(client, PROPERTY_ID, ymd(w.cur_start), ymd(w.cur_end), [], mets)
    d0 = run_report(client, PROPERTY_ID, ymd(w.prev_start), ymd(w.prev_end), [], mets)

    def row_to_dict(df):
        if df.empty:
            return {"sessions": 0.0, "transactions": 0.0, "purchaseRevenue": 0.0}
        r = df.iloc[0]
        return {m: float(r.get(m, 0) or 0) for m in mets}

    cur = row_to_dict(d1)
    prev = row_to_dict(d0)

    cur["cvr"] = (cur["transactions"] / cur["sessions"]) if cur["sessions"] else 0.0
    prev["cvr"] = (prev["transactions"] / prev["sessions"]) if prev["sessions"] else 0.0
    return {"current": cur, "prev": prev}


def get_event_users(client: BetaAnalyticsDataClient, w: DigestWindow, event_name: str) -> Dict[str, float]:
    filt = ga_filter_eq("eventName", event_name)
    d1 = run_report(client, PROPERTY_ID, ymd(w.cur_start), ymd(w.cur_end), [], ["totalUsers"], dimension_filter=filt)
    d0 = run_report(client, PROPERTY_ID, ymd(w.prev_start), ymd(w.prev_end), [], ["totalUsers"], dimension_filter=filt)
    cur = float(d1.iloc[0]["totalUsers"]) if (not d1.empty and "totalUsers" in d1.columns) else 0.0
    prev = float(d0.iloc[0]["totalUsers"]) if (not d0.empty and "totalUsers" in d0.columns) else 0.0
    return {"current": cur, "prev": prev}

def get_multi_event_users(client: BetaAnalyticsDataClient, w: DigestWindow, event_names: List[str]) -> Dict[str, float]:
    cur_total = 0.0
    prev_total = 0.0
    for ev in event_names:
        r = get_event_users(client, w, ev)
        cur_total += r["current"]
        prev_total += r["prev"]
    return {"current": cur_total, "prev": prev_total}


def get_channel_snapshot(client: BetaAnalyticsDataClient, w: DigestWindow) -> pd.DataFrame:
    dims = ["sessionDefaultChannelGroup"]
    mets = ["sessions", "transactions", "purchaseRevenue"]

    cur = run_report(client, PROPERTY_ID, ymd(w.cur_start), ymd(w.cur_end), dims, mets)
    prev = run_report(client, PROPERTY_ID, ymd(w.prev_start), ymd(w.prev_end), dims, mets)

    if cur.empty:
        cur = pd.DataFrame(columns=dims + mets)
    if prev.empty:
        prev = pd.DataFrame(columns=dims + mets)

    cur["bucket"] = cur["sessionDefaultChannelGroup"].apply(bucket_channel)
    prev["bucket"] = prev["sessionDefaultChannelGroup"].apply(bucket_channel)

    cur[mets] = cur[mets].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    prev[mets] = prev[mets].apply(pd.to_numeric, errors="coerce").fillna(0.0)

    cur_agg = cur.groupby("bucket", as_index=False)[mets].sum()
    prev_agg = prev.groupby("bucket", as_index=False)[mets].sum()

    buckets = ["Organic", "Paid AD", "Owned", "Awareness", "SNS"]
    base = pd.DataFrame({"bucket": buckets})
    out = (
        base.merge(cur_agg, on="bucket", how="left")
            .merge(prev_agg, on="bucket", how="left", suffixes=("", "_prev"))
            .fillna(0.0)
    )

    out["rev_dod"] = out.apply(lambda r: pct_change(r["purchaseRevenue"], r["purchaseRevenue_prev"]), axis=1)

    tot_cur_rev = float(out["purchaseRevenue"].sum())
    tot_prev_rev = float(out["purchaseRevenue_prev"].sum())
    total_row = {
        "bucket": "Total",
        "sessions": float(out["sessions"].sum()),
        "transactions": float(out["transactions"].sum()),
        "purchaseRevenue": tot_cur_rev,
        "sessions_prev": float(out["sessions_prev"].sum()),
        "transactions_prev": float(out["transactions_prev"].sum()),
        "purchaseRevenue_prev": tot_prev_rev,
        "rev_dod": pct_change(tot_cur_rev, tot_prev_rev),
    }

    out = pd.concat([out, pd.DataFrame([total_row])], ignore_index=True)
    return out[["bucket", "sessions", "transactions", "purchaseRevenue", "rev_dod"]]


def get_paid_detail(client: BetaAnalyticsDataClient, w: DigestWindow) -> pd.DataFrame:
    dims = ["sessionDefaultChannelGroup"]
    mets = ["sessions", "purchaseRevenue"]
    filt = ga_filter_in("sessionDefaultChannelGroup", PAID_SUBGROUPS)

    cur = run_report(client, PROPERTY_ID, ymd(w.cur_start), ymd(w.cur_end), dims, mets, dimension_filter=filt)
    prev = run_report(client, PROPERTY_ID, ymd(w.prev_start), ymd(w.prev_end), dims, mets, dimension_filter=filt)

    if cur.empty:
        cur = pd.DataFrame(columns=dims + mets)
    if prev.empty:
        prev = pd.DataFrame(columns=dims + mets)

    cur = cur.rename(columns={"sessionDefaultChannelGroup": "sub_channel"})
    prev = prev.rename(columns={"sessionDefaultChannelGroup": "sub_channel"})

    cur[mets] = cur[mets].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    prev[mets] = prev[mets].apply(pd.to_numeric, errors="coerce").fillna(0.0)

    out = (
        pd.DataFrame({"sub_channel": PAID_SUBGROUPS})
        .merge(cur, on="sub_channel", how="left")
        .merge(prev, on="sub_channel", how="left", suffixes=("", "_prev"))
        .fillna(0.0)
    )
    out["rev_dod"] = out.apply(lambda r: pct_change(r["purchaseRevenue"], r["purchaseRevenue_prev"]), axis=1)

    total_cur_rev = float(out["purchaseRevenue"].sum())
    total_prev_rev = float(out["purchaseRevenue_prev"].sum())
    total = pd.DataFrame([{
        "sub_channel": "Total",
        "sessions": float(out["sessions"].sum()),
        "purchaseRevenue": total_cur_rev,
        "rev_dod": pct_change(total_cur_rev, total_prev_rev),
    }])

    out2 = out[["sub_channel", "sessions", "purchaseRevenue", "rev_dod"]]
    return pd.concat([out2, total], ignore_index=True)


def get_paid_top3(client: BetaAnalyticsDataClient, w: DigestWindow) -> pd.DataFrame:
    # weekly에서는 top3 의미가 약해져서(7D aggregation으로 소스/미디엄 분해가 흔히 비용↑),
    # 안정성을 위해 daily에서만 보여주고, weekly에서는 빈 DF로 처리.
    if w.mode != "daily":
        return pd.DataFrame(columns=["sessionSourceMedium", "sessions", "purchaseRevenue"])

    dims = ["sessionSourceMedium"]
    mets = ["sessions", "purchaseRevenue"]
    filt = ga_filter_in("sessionDefaultChannelGroup", PAID_SUBGROUPS)
    order = [OrderBy(metric=OrderBy.MetricOrderBy(metric_name="purchaseRevenue"), desc=True)]

    df = run_report(client, PROPERTY_ID, ymd(w.cur_start), ymd(w.cur_end), dims, mets, dimension_filter=filt, order_bys=order, limit=3)

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


def get_kpi_snapshot_table(client: BetaAnalyticsDataClient, w: DigestWindow, overall: Dict[str, Dict[str, float]]) -> pd.DataFrame:
    signup = get_multi_event_users(client, w, ["signup_complete", "signup"])
    cur = overall["current"]; prev = overall["prev"]

    rows = [
        ("Sessions", cur["sessions"], prev["sessions"], "int"),
        ("CVR", cur["cvr"], prev["cvr"], "pct"),
        ("Revenue", cur["purchaseRevenue"], prev["purchaseRevenue"], "krw"),
        ("Orders", cur["transactions"], prev["transactions"], "int"),
        ("Sign-up Users", signup["current"], signup["prev"], "int"),
    ]

    out = []
    for metric, c, p, kind in rows:
        dod = pct_change(c, p) if kind != "pct" else (c - p)
        if kind == "int":
            value_fmt, dod_fmt = fmt_int(c), fmt_pct(dod, 1)
        elif kind == "krw":
            value_fmt, dod_fmt = fmt_currency_krw(c), fmt_pct(dod, 1)
        else:
            value_fmt, dod_fmt = f"{c*100:.2f}%", fmt_pp(dod, 2)
        out.append({"metric": metric, "value_fmt": value_fmt, "dod": dod, "dod_fmt": dod_fmt})
    return pd.DataFrame(out)


def get_trend_view_svg(client: BetaAnalyticsDataClient, w: DigestWindow) -> str:
    # trend view는 항상 "cur_end 기준 7D"로 동일하게 보이게(weekly/daily 모두 동일) 유지
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


def get_best_sellers_with_trends(client: BetaAnalyticsDataClient, w: DigestWindow, image_map: Dict[str, str]) -> pd.DataFrame:
    # best sellers는 daily에서만 (weekly는 의미 약함)
    if w.mode != "daily":
        return pd.DataFrame(columns=["itemId","itemName","itemsPurchased_yesterday","trend_svg","image_url"])

    order = [OrderBy(metric=OrderBy.MetricOrderBy(metric_name="itemsPurchased"), desc=True)]
    top = run_report(client, PROPERTY_ID, ymd(w.cur_end), ymd(w.cur_end), ["itemId","itemName"], ["itemsPurchased"], order_bys=order, limit=5)
    if top.empty:
        return pd.DataFrame(columns=["itemId","itemName","itemsPurchased_yesterday","trend_svg","image_url"])

    top["itemsPurchased"] = pd.to_numeric(top["itemsPurchased"], errors="coerce").fillna(0.0)
    top = top.rename(columns={"itemsPurchased":"itemsPurchased_yesterday"})
    skus = top["itemId"].tolist()

    axis_dates = [w.cur_end - dt.timedelta(days=6-i) for i in range(7)]
    xlabels = [d.strftime("%m/%d") for d in axis_dates]

    ts = run_report(
        client, PROPERTY_ID,
        ymd(w.cur_end - dt.timedelta(days=6)), ymd(w.cur_end),
        ["date","itemId"], ["itemsPurchased"],
        dimension_filter=ga_filter_in("itemId", [str(s).strip() for s in skus]),
        limit=10000
    )

    if ts.empty:
        top["trend_svg"] = ""
        top["image_url"] = top["itemId"].map(lambda s: image_map.get(str(s).strip(), ""))
        return top[["itemId","itemName","itemsPurchased_yesterday","trend_svg","image_url"]]

    ts["date"] = ts["date"].apply(parse_yyyymmdd)
    ts["itemsPurchased"] = pd.to_numeric(ts["itemsPurchased"], errors="coerce").fillna(0.0)
    ts = ts.sort_values(["itemId","date"])

    svgs = []
    for sku in skus:
        sub = ts[ts["itemId"] == sku].set_index("date")["itemsPurchased"]
        ys = [float(sub.get(d, 0.0)) for d in axis_dates]
        svgs.append(spark_svg(xlabels, ys, width=240, height=70, stroke="#0055a5"))

    top["trend_svg"] = svgs
    top["image_url"] = top["itemId"].map(lambda s: image_map.get(str(s).strip(), ""))
    return top[["itemId","itemName","itemsPurchased_yesterday","trend_svg","image_url"]]


def get_rising_products(client: BetaAnalyticsDataClient, w: DigestWindow, top_n: int = 5) -> pd.DataFrame:
    # Rising은 daily에서만 유지
    if w.mode != "daily":
        return pd.DataFrame(columns=["itemId", "itemName", "itemViews_yesterday", "delta"])

    d1 = run_report(client, PROPERTY_ID, ymd(w.cur_end), ymd(w.cur_end), ["itemId", "itemName"], ["itemsPurchased"], limit=10000)
    d0 = run_report(client, PROPERTY_ID, ymd(w.prev_end_date), ymd(w.prev_end_date), ["itemId"], ["itemsPurchased"], limit=10000)

    if d1.empty:
        return pd.DataFrame(columns=["itemId", "itemName", "itemViews_yesterday", "delta"])

    d1["itemsPurchased"] = pd.to_numeric(d1["itemsPurchased"], errors="coerce").fillna(0.0)
    if not d0.empty:
        d0["itemsPurchased"] = pd.to_numeric(d0["itemsPurchased"], errors="coerce").fillna(0.0)
    else:
        d0 = pd.DataFrame(columns=["itemId", "itemsPurchased"])

    m = d1.merge(d0, on="itemId", how="left", suffixes=("_y", "_d0")).fillna(0.0)
    m["delta"] = m["itemsPurchased_y"] - m["itemsPurchased_d0"]
    m = m.sort_values("delta", ascending=False).head(top_n)

    # Views (best-effort)
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
                    client, PROPERTY_ID, ymd(w.cur_end), ymd(w.cur_end),
                    ["itemId"], [metric_name],
                    dimension_filter=(
                        ga_filter_and([ga_filter_in("itemId", skus), ga_filter_eq("eventName", "view_item")])
                        if use_event_filter else ga_filter_in("itemId", skus)
                    ),
                    limit=10000,
                )
                if not v.empty:
                    v[metric_name] = pd.to_numeric(v[metric_name], errors="coerce").fillna(0.0)
                    views_df = v[["itemId", metric_name]].rename(columns={metric_name: "itemViews_yesterday"})
                break
            except Exception:
                continue

    m = m.merge(views_df, on="itemId", how="left")
    m["itemViews_yesterday"] = pd.to_numeric(m.get("itemViews_yesterday"), errors="coerce").fillna(0.0)
    return m[["itemId", "itemName", "itemViews_yesterday", "delta"]]


# =========================
# PDP Trend (BigQuery) + Diagnostics (NEW)
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
    """
    PDP Trend (7D ending at end_date), based on BigQuery events (view_item).

    Diagnostic added:
    - counts view_item events
    - counts view_item rows where items.item_id is not null
    - if 0 => GA4 export에 items가 안 실리는 케이스 (가장 흔한 원인)
    """
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

        # ---- Diagnostics (cheap)
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
                print("[WARN] PDP Trend is empty because view_item events have NO items[].item_id in BigQuery export.")
                print("       -> GA4 ecommerce item payload 미전송/미수집 가능성이 큼 (view_item에 items 배열이 있어야 매핑 가능)")

        # ---- Main query
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


def get_search_trends(client: BetaAnalyticsDataClient, end_date: dt.date) -> Dict[str, pd.DataFrame]:
    # Search trend is daily-style (end_date 기준)
    lookback_start = end_date - dt.timedelta(days=13)
    df = run_report(
        client, PROPERTY_ID,
        ymd(lookback_start), ymd(end_date),
        ["date","searchTerm"], ["eventCount"],
        dimension_filter=ga_filter_eq("eventName", SEARCH_EVENT),
        limit=10000
    )
    if df.empty:
        return {"new": pd.DataFrame(columns=["searchTerm"]), "rising": pd.DataFrame(columns=["searchTerm","pct"])}

    df["date"] = df["date"].apply(parse_yyyymmdd)
    df["eventCount"] = pd.to_numeric(df["eventCount"], errors="coerce").fillna(0.0)

    y_df = df[df["date"] == end_date].groupby("searchTerm", as_index=False)["eventCount"].sum().sort_values("eventCount", ascending=False)
    prior_start = end_date - dt.timedelta(days=7)
    prior_df = df[(df["date"] >= prior_start) & (df["date"] <= (end_date - dt.timedelta(days=1)))]
    prior_agg = prior_df.groupby("searchTerm", as_index=False)["eventCount"].mean().rename(columns={"eventCount":"prior_avg"})

    merged = y_df.merge(prior_agg, on="searchTerm", how="left").fillna(0.0)
    new_terms = merged[merged["prior_avg"] == 0].head(3)[["searchTerm"]].copy()
    rising = merged[merged["prior_avg"] > 0].copy()
    rising["pct"] = (rising["eventCount"] - rising["prior_avg"]) / merged["prior_avg"] * 100.0
    rising = rising.replace([float("inf"), -float("inf")], 0.0)
    rising = rising.sort_values("pct", ascending=False).head(3)[["searchTerm","pct"]]
    return {"new": new_terms, "rising": rising}


# =========================
# UI (NEW: build_summary look & feel)
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
    nav_links: Dict[str, str],  # {"daily_index": "...", "weekly_index": "...", "hub": "..."}
) -> str:
    cur = overall["current"]; prev = overall["prev"]
    s_dod = pct_change(cur["sessions"], prev["sessions"])
    o_dod = pct_change(cur["transactions"], prev["transactions"])
    r_dod = pct_change(cur["purchaseRevenue"], prev["purchaseRevenue"])
    c_pp = cur["cvr"] - prev["cvr"]

    su_cur = float(signup_users.get("current", 0.0) or 0.0)
    su_prev = float(signup_users.get("prev", 0.0) or 0.0)
    su_dod = pct_change(su_cur, su_prev)

    def delta_cls(v: float) -> str:
        return "text-blue-600" if v >= 0 else "text-orange-700"

    def top_kpi_card(title: str, value: str, delta: str, cls: str) -> str:
        return f"""
        <div class="rounded-2xl border border-slate-200 bg-white/70 p-4">
          <div class="text-[11px] font-extrabold tracking-widest text-slate-500 uppercase">{title}</div>
          <div class="mt-1 text-xl font-black text-slate-900">{value}</div>
          <div class="mt-1 text-[11px] text-slate-500">전일 대비 <b class="{cls}">{delta}</b></div>
        </div>
        """

    def product_img(url: str) -> str:
        u = (url or PLACEHOLDER_IMG or "").strip()
        if u:
            return f"<img src='{u}' class='w-8 h-8 rounded-xl object-cover border border-slate-200'/>"
        return "<div class='w-8 h-8 rounded-xl bg-slate-100 border border-slate-200'></div>"

    # Tables
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
            f"<div class='text-right {delta_cls(r.rev_dod)}'>{('+' if r.rev_dod>=0 else '')}{fmt_pct(r.rev_dod,1)}</div>",
        ], bold=(r.bucket == "Total"))

    paid_html = ""
    for r in paid_detail.itertuples(index=False):
        paid_html += table_row([
            str(r.sub_channel),
            f"<div class='text-right'>{fmt_int(r.sessions)}</div>",
            f"<div class='text-right'>{fmt_currency_krw(r.purchaseRevenue)}</div>",
            f"<div class='text-right {delta_cls(r.rev_dod)}'>{('+' if r.rev_dod>=0 else '')}{fmt_pct(r.rev_dod,1)}</div>",
        ], bold=(r.sub_channel == "Total"))

    paid_top3_html = ""
    if not paid_top3.empty:
        for r in paid_top3.itertuples(index=False):
            paid_top3_html += table_row([
                str(r.sessionSourceMedium),
                f"<div class='text-right'>{fmt_int(getattr(r,'sessions',0))}</div>",
                f"<div class='text-right'>{fmt_currency_krw(r.purchaseRevenue)}</div>",
            ], bold=(r.sessionSourceMedium == "Total"))

    kpi_html = ""
    for r in kpi_snapshot.itertuples(index=False):
        kpi_html += table_row([
            str(r.metric),
            f"<div class='text-right'>{r.value_fmt}</div>",
            f"<div class='text-right {delta_cls(r.dod)}'>{('+' if r.dod>=0 else '')}{r.dod_fmt}</div>",
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
                out += f"<tr><td class='px-3 py-2 border-b border-slate-100'>{r.searchTerm}</td><td class='px-3 py-2 border-b border-slate-100 text-right text-slate-400'>new</td></tr>"
            else:
                tag = f"{'+' if r.pct>=0 else ''}{r.pct:.0f}%"
                out += f"<tr><td class='px-3 py-2 border-b border-slate-100'>{r.searchTerm}</td><td class='px-3 py-2 border-b border-slate-100 text-right text-slate-400'>{tag}</td></tr>"
        return out

    mode_badge = "Daily" if w.mode == "daily" else "Weekly (7D Cumulative)"
    period_text = f"{ymd(w.cur_start)} ~ {ymd(w.cur_end)}" if w.mode == "weekly" else f"{ymd(w.end_date)}"

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

    <!-- Header -->
    <div class="mb-5 flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
      <div class="flex items-center gap-3">
        {"<img src='data:image/png;base64," + logo_b64 + "' class='h-8 w-auto'/>" if logo_b64 else ""}
        <div>
          <div class="text-2xl sm:text-3xl font-black tracking-tight">Daily Digest</div>
          <div class="text-sm text-slate-500">eCommerce Performance · {mode_badge} · <b class="text-slate-700">{period_text}</b></div>
        </div>
      </div>

      <!-- Nav -->
      <div class="flex items-center gap-2">
        <a href="{nav_links.get('hub','index.html')}" class="px-4 py-2 rounded-2xl glass-card font-extrabold text-sm">Hub</a>
        <a href="{nav_links.get('daily_index','index.html')}" class="px-4 py-2 rounded-2xl glass-card font-extrabold text-sm">Daily</a>
        <a href="{nav_links.get('weekly_index','index.html')}" class="px-4 py-2 rounded-2xl glass-card font-extrabold text-sm">Weekly</a>
        <span class="badge-soft">{mode_badge}</span>
      </div>
    </div>

    <!-- KPI strip -->
    <div class="glass-card rounded-3xl p-5 mb-6">
      <div class="flex items-center justify-between">
        <div class="text-base font-black text-slate-900">Top KPIs</div>
        <span class="badge">{ymd(w.end_date)} 기준</span>
      </div>

      <div class="mt-4 grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3">
        {top_kpi_card("Sessions", fmt_int(cur["sessions"]), f"{'+' if s_dod>=0 else ''}{fmt_pct(s_dod,1)}", delta_cls(s_dod))}
        {top_kpi_card("Orders", fmt_int(cur["transactions"]), f"{'+' if o_dod>=0 else ''}{fmt_pct(o_dod,1)}", delta_cls(o_dod))}
        {top_kpi_card("Revenue", fmt_currency_krw(cur["purchaseRevenue"]), f"{'+' if r_dod>=0 else ''}{fmt_pct(r_dod,1)}", delta_cls(r_dod))}
        {top_kpi_card("CVR", f"{cur['cvr']*100:.2f}%", fmt_pp(c_pp,2), delta_cls(c_pp))}
        {top_kpi_card("Sign-up Users", fmt_int(su_cur), f"{'+' if su_dod>=0 else ''}{fmt_pct(su_dod,1)}", delta_cls(su_dod))}
      </div>
    </div>

    <!-- Content cards -->
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">

      <!-- Channel Snapshot -->
      <div class="glass-card rounded-3xl p-6">
        <div class="flex items-center justify-between">
          <div class="text-lg font-black">Channel Snapshot</div>
          <span class="badge-soft">Organic · Paid · Owned · Awareness · SNS</span>
        </div>
        <div class="mt-4 overflow-x-auto">
          <table class="w-full text-sm">
            <thead>
              <tr class="bg-slate-50">
                <th class="px-3 py-2 text-left text-xs tracking-widest uppercase text-slate-500">Channel</th>
                <th class="px-3 py-2 text-right text-xs tracking-widest uppercase text-slate-500">Traffic</th>
                <th class="px-3 py-2 text-right text-xs tracking-widest uppercase text-slate-500">Orders</th>
                <th class="px-3 py-2 text-right text-xs tracking-widest uppercase text-slate-500">Revenue</th>
                <th class="px-3 py-2 text-right text-xs tracking-widest uppercase text-slate-500">DoD (Rev)</th>
              </tr>
            </thead>
            <tbody>{chan_html}</tbody>
          </table>
        </div>
      </div>

      <!-- Paid detail -->
      <div class="glass-card rounded-3xl p-6">
        <div class="flex items-center justify-between">
          <div class="text-lg font-black">Paid AD Detail</div>
          <span class="badge-soft">{'Daily only Top3' if w.mode=='daily' else 'Weekly aggregation'}</span>
        </div>

        <div class="mt-4 overflow-x-auto">
          <table class="w-full text-sm">
            <thead>
              <tr class="bg-slate-50">
                <th class="px-3 py-2 text-left text-xs tracking-widest uppercase text-slate-500">Sub-channel</th>
                <th class="px-3 py-2 text-right text-xs tracking-widest uppercase text-slate-500">Traffic</th>
                <th class="px-3 py-2 text-right text-xs tracking-widest uppercase text-slate-500">Revenue</th>
                <th class="px-3 py-2 text-right text-xs tracking-widest uppercase text-slate-500">DoD (Rev)</th>
              </tr>
            </thead>
            <tbody>{paid_html}</tbody>
          </table>
        </div>

        {"<div class='mt-5'><div class='text-sm font-black mb-2'>Paid AD Top 3</div><div class='overflow-x-auto'><table class='w-full text-sm'><thead><tr class='bg-slate-50'><th class='px-3 py-2 text-left text-xs tracking-widest uppercase text-slate-500'>Source / Medium</th><th class='px-3 py-2 text-right text-xs tracking-widest uppercase text-slate-500'>Traffic</th><th class='px-3 py-2 text-right text-xs tracking-widest uppercase text-slate-500'>Revenue</th></tr></thead><tbody>"+paid_top3_html+"</tbody></table></div></div>" if (w.mode=='daily' and paid_top3_html) else ""}
      </div>

      <!-- KPI Snapshot -->
      <div class="glass-card rounded-3xl p-6">
        <div class="flex items-center justify-between">
          <div class="text-lg font-black">KPI Snapshot</div>
          <span class="badge-soft">{mode_badge}</span>
        </div>
        <div class="mt-4 overflow-x-auto">
          <table class="w-full text-sm">
            <thead>
              <tr class="bg-slate-50">
                <th class="px-3 py-2 text-left text-xs tracking-widest uppercase text-slate-500">Metric</th>
                <th class="px-3 py-2 text-right text-xs tracking-widest uppercase text-slate-500">Value</th>
                <th class="px-3 py-2 text-right text-xs tracking-widest uppercase text-slate-500">Δ</th>
              </tr>
            </thead>
            <tbody>{kpi_html}</tbody>
          </table>
        </div>
      </div>

      <!-- Trend View -->
      <div class="glass-card rounded-3xl p-6">
        <div class="flex items-center justify-between">
          <div class="text-lg font-black">Trend View</div>
          <span class="badge-soft">Index (D-7=100)</span>
        </div>
        <div class="mt-4 rounded-2xl border border-slate-200 bg-white/60 p-3">
          {trend_svg}
        </div>
      </div>

      <!-- Best Sellers -->
      <div class="glass-card rounded-3xl p-6">
        <div class="flex items-center justify-between">
          <div class="text-lg font-black">Best Sellers · TOP 5 (Qty)</div>
          <span class="badge-soft">{'Daily' if w.mode=='daily' else '—'}</span>
        </div>
        <div class="mt-4 overflow-x-auto">
          <table class="w-full text-sm">
            <thead>
              <tr class="bg-slate-50">
                <th class="px-3 py-2 text-left text-xs tracking-widest uppercase text-slate-500">Image</th>
                <th class="px-3 py-2 text-left text-xs tracking-widest uppercase text-slate-500">Item</th>
                <th class="px-3 py-2 text-left text-xs tracking-widest uppercase text-slate-500">SKU</th>
                <th class="px-3 py-2 text-right text-xs tracking-widest uppercase text-slate-500">Qty</th>
                <th class="px-3 py-2 text-left text-xs tracking-widest uppercase text-slate-500">7D Trend</th>
              </tr>
            </thead>
            <tbody>
              {bs_html if (w.mode=='daily' and bs_html) else "<tr><td class='px-3 py-4 text-slate-400' colspan='5'>Weekly에서는 Best Sellers를 생략합니다.</td></tr>"}
            </tbody>
          </table>
        </div>
      </div>

      <!-- Rising -->
      <div class="glass-card rounded-3xl p-6">
        <div class="flex items-center justify-between">
          <div class="text-lg font-black">Rising Products</div>
          <span class="badge-soft">{'Daily' if w.mode=='daily' else '—'}</span>
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
              {rp_html if (w.mode=='daily' and rp_html) else "<tr><td class='px-3 py-4 text-slate-400' colspan='5'>Weekly에서는 Rising Products를 생략합니다.</td></tr>"}
            </tbody>
          </table>
        </div>
      </div>

      <!-- PDP Trend -->
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

      <!-- Search Trend -->
      <div class="glass-card rounded-3xl p-6 lg:col-span-2">
        <div class="flex items-center justify-between">
          <div class="text-lg font-black">Search Trend</div>
          <span class="badge-soft">{ymd(w.end_date)}</span>
        </div>
        <div class="mt-4 grid grid-cols-1 md:grid-cols-2 gap-4">
          <div class="rounded-2xl border border-slate-200 bg-white/60">
            <div class="px-4 py-3 font-extrabold text-sm border-b border-slate-200">신규 진입 Top 3</div>
            <table class="w-full text-sm"><tbody>{kw_rows(search_new, "new")}</tbody></table>
          </div>
          <div class="rounded-2xl border border-slate-200 bg-white/60">
            <div class="px-4 py-3 font-extrabold text-sm border-b border-slate-200">급상승 Top 3</div>
            <table class="w-full text-sm"><tbody>{kw_rows(search_rising, "rising")}</tbody></table>
          </div>
        </div>
      </div>

    </div>

    <div class="mt-6 text-xs text-slate-400 text-right">
      Auto-generated · {mode_badge} · End = {ymd(w.end_date)}
    </div>

  </div>
</body>
</html>
"""


# =========================
# Hub pages (NEW)
# =========================
def render_hub_index(dates: List[dt.date]) -> str:
    # dates: descending (recent first)
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
        <div class="mt-4 text-xs text-slate-500 mb-3">END 날짜 기준으로 7일 누적</div>
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
    signup_users = get_multi_event_users(client, w, ["signup_complete", "signup"])
    channel_snapshot = get_channel_snapshot(client, w)
    paid_detail = get_paid_detail(client, w)
    paid_top3 = get_paid_top3(client, w)
    kpi_snapshot = get_kpi_snapshot_table(client, w, overall)
    trend_svg = get_trend_view_svg(client, w)

    best_sellers = get_best_sellers_with_trends(client, w, image_map)
    rising = get_rising_products(client, w, top_n=5)
    rising = attach_image_urls(rising, image_map)

    # Missing image tracking (daily only)
    missing = []
    if mode == "daily":
        if not best_sellers.empty and 'itemId' in best_sellers.columns:
            missing += [sku for sku in best_sellers['itemId'].tolist() if str(sku).strip() not in image_map]
        if not rising.empty and 'itemId' in rising.columns:
            missing += [sku for sku in rising['itemId'].tolist() if str(sku).strip() not in image_map]
        if missing:
            write_missing_image_skus(MISSING_SKU_OUT, missing)

    # PDP Trend: always 7D ending at end_date
    category_pdp_trend = get_category_pdp_view_trend_bq(end_date=end_date)

    # Search trend: end_date 기준
    search = get_search_trends(client, end_date=end_date)

    nav_links = {
        "hub": "../index.html" if mode == "daily" else "../index.html",
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
# Main (build N days)
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

    # 기준 날짜: KST today-1 을 최신 end date로 (어제 데이터)
    today_kst = dt.datetime.now(ZoneInfo("Asia/Seoul")).date()
    latest_end = today_kst - dt.timedelta(days=1)

    # build 대상 날짜들 (recent first)
    dates = [latest_end - dt.timedelta(days=i) for i in range(max(1, DAYS_TO_BUILD))]
    dates = [d for d in dates if d.year >= 2000]

    # Load assets
    logo_b64 = load_logo_base64(LOGO_PATH)
    image_map = load_image_map_from_excel_urls(IMAGE_XLS_PATH)

    # Output folders
    ensure_dir(OUT_DIR)
    daily_dir = os.path.join(OUT_DIR, "daily")
    weekly_dir = os.path.join(OUT_DIR, "weekly")
    ensure_dir(daily_dir)
    ensure_dir(weekly_dir)

    # Build pages
    for d in dates:
        # Daily
        html_daily = build_one(client, end_date=d, mode="daily", image_map=image_map, logo_b64=logo_b64)
        out_daily = os.path.join(daily_dir, f"{ymd(d)}.html")
        with open(out_daily, "w", encoding="utf-8") as f:
            f.write(html_daily)
        print(f"[OK] Wrote: {out_daily}")

        # Weekly cumulative
        html_weekly = build_one(client, end_date=d, mode="weekly", image_map=image_map, logo_b64=logo_b64)
        out_weekly = os.path.join(weekly_dir, f"END_{ymd(d)}.html")
        with open(out_weekly, "w", encoding="utf-8") as f:
            f.write(html_weekly)
        print(f"[OK] Wrote: {out_weekly}")

    # Hub index
    hub = render_hub_index(dates=dates)
    hub_path = os.path.join(OUT_DIR, "index.html")
    with open(hub_path, "w", encoding="utf-8") as f:
        f.write(hub)
    print(f"[OK] Wrote HUB: {hub_path}")


if __name__ == "__main__":
    main()
