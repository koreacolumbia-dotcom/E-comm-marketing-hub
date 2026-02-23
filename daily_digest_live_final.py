# -*- coding: utf-8 -*-
"""
Columbia Daily Digest — Live GA4 Data (Google Analytics Data API)

Run (Windows PowerShell)
  setx GOOGLE_APPLICATION_CREDENTIALS "C:\\path\\service_account.json"
  setx GA4_PROPERTY_ID "358593394"
  pip install google-analytics-data pandas python-dateutil
  python daily_digest_live.py

Output
- daily_digest_live.html (default)

Notes
- Category CTR = select_item / view_item_list (by itemCategory)
- Trend View uses Index (D-7 = 100) to combine Sessions/Revenue/CVR on one chart.
"""

import os
import base64
import datetime as dt
from dataclasses import dataclass
from typing import List, Optional, Dict
import re


def write_missing_image_skus(path: str, skus: list[str]) -> None:
    """Write missing SKU list to CSV for maintenance."""
    if not path or not skus:
        return
    try:
        df = pd.DataFrame({"sku": sorted(set([str(s).strip() for s in skus if str(s).strip()]))})
        df.to_csv(path, index=False, encoding="utf-8-sig")
        print(f"[INFO] Wrote missing image SKUs: {path}")
    except Exception as e:
        print(f"[WARN] Could not write missing image SKUs: {type(e).__name__}: {e}")

import pandas as pd
import openpyxl


def attach_image_urls(df: pd.DataFrame, image_map: Dict[str, str]) -> pd.DataFrame:
    """Attach `image_url` column using a SKU->URL map.

    Expects a column named `itemId`. Returns a copy.
    """
    if df is None or df.empty:
        # keep schema predictable
        return pd.DataFrame(df, copy=True)
    out = df.copy()
    if "itemId" not in out.columns:
        return out
    out["image_url"] = out["itemId"].astype(str).str.strip().map(lambda x: image_map.get(x, ""))
    return out

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.auth import default as google_auth_default
from google.analytics.data_v1beta.types import (
    DateRange, Dimension, Metric, RunReportRequest,
    OrderBy, FilterExpression, Filter, FilterExpressionList
)

# Optional: BigQuery backend for Category Trend when GA4 Data API dims/metrics are incompatible
try:
    from google.cloud import bigquery  # type: ignore
except Exception:  # pragma: no cover
    bigquery = None

PROPERTY_ID = os.getenv("GA4_PROPERTY_ID", "").strip()
LOGO_PATH = os.getenv("DAILY_DIGEST_LOGO_PATH", "pngwing.com.png")
OUT_HTML = os.getenv("DAILY_DIGEST_OUT_HTML", "daily_digest_live.html")
# Image mapping Excel (SKU -> image URL)
# - If the Excel is in the same folder as this script, you can leave env var empty.
# - Otherwise set: DAILY_DIGEST_IMAGE_XLS_PATH="C:\\...\\상품코드별 이미지.xlsx"
IMAGE_XLS_PATH = os.getenv("DAILY_DIGEST_IMAGE_XLS_PATH", "상품코드별 이미지.xlsx").strip()
MISSING_SKU_OUT = os.getenv("DAILY_DIGEST_MISSING_SKU_OUT", "missing_image_skus.csv")
PLACEHOLDER_IMG = os.getenv("DAILY_DIGEST_PLACEHOLDER_IMG", "")

# If set, Category Trend will use BigQuery events table instead of GA4 Data API (more reliable for item_category fields).
# Example: columbia-ga4.analytics_358593394.events_*
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

def load_logo_base64(path: str) -> str:
    if not path or not os.path.exists(path):
        return ""
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")

def fmt_int(n) -> str:
    try:
        return f"{int(round(float(n))):,}"
    except Exception:
        return "0"

def fmt_currency_krw(n) -> str:
    try:
        return f"₩{int(round(float(n))):,}"
    except Exception:
        return "₩0"

def fmt_pct(p, digits=1) -> str:
    try:
        return f"{p*100:.{digits}f}%"
    except Exception:
        return "0.0%"

def fmt_pp(p, digits=2) -> str:
    try:
        return f"{p*100:.{digits}f}%p"
    except Exception:
        return "0.00%p"

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
    """AND-group multiple filter expressions."""
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
        grid.append(f"<line x1='{pad_l}' y1='{y:.1f}' x2='{width-pad_r}' y2='{y:.1f}' stroke='#eef2ff' stroke-width='1'/>" )
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
        polys.append(f"<polyline fill='none' stroke='{color}' stroke-width='2.6' points='{poly}'/>" )
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



def load_image_map_from_excel_urls(xlsx_path: str) -> Dict[str, str]:
    """Load SKU -> image_url mapping from your Excel.

    Supports:
      A) Header-based columns (상품코드 + 이미지링크 / image_url)
      B) Fixed columns: C=상품코드(SKU), E=이미지링크(URL) even if headers are merged / Unnamed
    """
    if not xlsx_path:
        print("[WARN] Image Excel path is empty. Set DAILY_DIGEST_IMAGE_XLS_PATH or keep '상품코드별 이미지.xlsx' next to the script.")
        return {}

    # Allow relative path (same folder as script)
    if not os.path.exists(xlsx_path):
        alt = os.path.join(os.path.dirname(os.path.abspath(__file__)), xlsx_path)
        if os.path.exists(alt):
            xlsx_path = alt
        else:
            print(f"[WARN] Image Excel not found: {xlsx_path}. Place the file next to the script or set DAILY_DIGEST_IMAGE_XLS_PATH.")
            return {}
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
            print("[WARN] Excel image map parsed 0 rows. Make sure C column has SKU and E column has https image URL.")
        return m
    except Exception as e:
        print(f"[WARN] Failed to load Excel image map: {type(e).__name__}: {e}")
        return {}

def spark_svg(
    xlabels: List[str],
    ys: List[float],
    width=240, height=70,
    pad_l=36, pad_r=10, pad_t=10, pad_b=22,
    stroke="#0055a5",
) -> str:
    n = len(xlabels)
    y_min, y_max = min(ys), max(ys)
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

    pts = [xy(i, v) for i, v in enumerate(ys)]
    poly = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)

    grid = []
    for frac in [0.0, 0.5, 1.0]:
        y = pad_t + inner_h * (1 - frac)
        grid.append(f"<line x1='{pad_l}' y1='{y:.1f}' x2='{width-pad_r}' y2='{y:.1f}' stroke='#eef2fb' stroke-width='1'/>" )

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

@dataclass
class DailyWindow:
    run_date: dt.date
    yesterday: dt.date
    day_before: dt.date
    window_start: dt.date
    window_end: dt.date

def compute_window(run_date: Optional[dt.date] = None) -> DailyWindow:
    if run_date is None:
        run_date = dt.date.today()
    yesterday = run_date - dt.timedelta(days=1)
    day_before = run_date - dt.timedelta(days=2)
    window_end = yesterday
    window_start = window_end - dt.timedelta(days=6)
    return DailyWindow(run_date, yesterday, day_before, window_start, window_end)

def get_overall_kpis(client: BetaAnalyticsDataClient, w: DailyWindow) -> Dict[str, Dict[str, float]]:
    mets = ["sessions", "transactions", "purchaseRevenue"]
    d1 = run_report(client, PROPERTY_ID, ymd(w.yesterday), ymd(w.yesterday), [], mets)
    d0 = run_report(client, PROPERTY_ID, ymd(w.day_before), ymd(w.day_before), [], mets)

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

def get_event_count(client: BetaAnalyticsDataClient, w: DailyWindow, event_name: str) -> Dict[str, float]:
    filt = ga_filter_eq("eventName", event_name)
    d1 = run_report(client, PROPERTY_ID, ymd(w.yesterday), ymd(w.yesterday), [], ["eventCount"], dimension_filter=filt)
    d0 = run_report(client, PROPERTY_ID, ymd(w.day_before), ymd(w.day_before), [], ["eventCount"], dimension_filter=filt)
    cur = float(d1.iloc[0]["eventCount"]) if not d1.empty else 0.0
    prev = float(d0.iloc[0]["eventCount"]) if not d0.empty else 0.0
    return {"current": cur, "prev": prev}


def get_event_users(client: BetaAnalyticsDataClient, w: DailyWindow, event_name: str) -> Dict[str, float]:
    """Return totalUsers for a specific event_name for D-1 vs D-2."""
    filt = ga_filter_eq("eventName", event_name)
    d1 = run_report(client, PROPERTY_ID, ymd(w.yesterday), ymd(w.yesterday), [], ["totalUsers"], dimension_filter=filt)
    d0 = run_report(client, PROPERTY_ID, ymd(w.day_before), ymd(w.day_before), [], ["totalUsers"], dimension_filter=filt)
    cur = float(d1.iloc[0]["totalUsers"]) if (not d1.empty and "totalUsers" in d1.columns) else 0.0
    prev = float(d0.iloc[0]["totalUsers"]) if (not d0.empty and "totalUsers" in d0.columns) else 0.0
    return {"current": cur, "prev": prev}


def get_multi_event_users(client: BetaAnalyticsDataClient, w: DailyWindow, event_names: List[str]) -> Dict[str, float]:
    """Sum totalUsers across multiple event names for D-1 vs D-2."""
    cur_total = 0.0
    prev_total = 0.0
    for ev in event_names:
        r = get_event_users(client, w, ev)
        cur_total += r["current"]
        prev_total += r["prev"]
    return {"current": cur_total, "prev": prev_total}



def get_channel_snapshot(client: BetaAnalyticsDataClient, w: DailyWindow) -> pd.DataFrame:
    """Channel Snapshot for D-1 vs D-2.

    Returns bucketed rows plus a Total row.
    """
    dims = ["sessionDefaultChannelGroup"]
    mets = ["sessions", "transactions", "purchaseRevenue"]

    cur = run_report(client, PROPERTY_ID, ymd(w.yesterday), ymd(w.yesterday), dims, mets)
    prev = run_report(client, PROPERTY_ID, ymd(w.day_before), ymd(w.day_before), dims, mets)

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

    # Total row
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


def get_paid_detail(client: BetaAnalyticsDataClient, w: DailyWindow) -> pd.DataFrame:
    dims = ["sessionDefaultChannelGroup"]
    mets = ["sessions", "purchaseRevenue"]
    filt = ga_filter_in("sessionDefaultChannelGroup", PAID_SUBGROUPS)

    cur = run_report(client, PROPERTY_ID, ymd(w.yesterday), ymd(w.yesterday), dims, mets, dimension_filter=filt)
    prev = run_report(client, PROPERTY_ID, ymd(w.day_before), ymd(w.day_before), dims, mets, dimension_filter=filt)

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
    total = pd.DataFrame([
        {
            "sub_channel": "Total",
            "sessions": float(out["sessions"].sum()),
            "purchaseRevenue": total_cur_rev,
            "rev_dod": pct_change(total_cur_rev, total_prev_rev),
        }
    ])

    out2 = out[["sub_channel", "sessions", "purchaseRevenue", "rev_dod"]]
    return pd.concat([out2, total], ignore_index=True)


def get_paid_top3(client: BetaAnalyticsDataClient, w: DailyWindow) -> pd.DataFrame:
    """Paid AD Top 3 by revenue.

    Includes Traffic(sessions) + Revenue, and appends a Total row.
    """
    dims = ["sessionSourceMedium"]
    mets = ["sessions", "purchaseRevenue"]
    filt = ga_filter_in("sessionDefaultChannelGroup", PAID_SUBGROUPS)
    order = [OrderBy(metric=OrderBy.MetricOrderBy(metric_name="purchaseRevenue"), desc=True)]

    df = run_report(
        client,
        PROPERTY_ID,
        ymd(w.yesterday),
        ymd(w.yesterday),
        dims,
        mets,
        dimension_filter=filt,
        order_bys=order,
        limit=3,
    )

    if df.empty:
        return pd.DataFrame(columns=["sessionSourceMedium", "sessions", "purchaseRevenue"])

    df["sessions"] = pd.to_numeric(df["sessions"], errors="coerce").fillna(0.0)
    df["purchaseRevenue"] = pd.to_numeric(df["purchaseRevenue"], errors="coerce").fillna(0.0)

    total = pd.DataFrame(
        [{
            "sessionSourceMedium": "Total",
            "sessions": float(df["sessions"].sum()),
            "purchaseRevenue": float(df["purchaseRevenue"].sum()),
        }]
    )
    return pd.concat([df, total], ignore_index=True)

def get_kpi_snapshot_table(client: BetaAnalyticsDataClient, w: DailyWindow, overall: Dict[str, Dict[str, float]]) -> pd.DataFrame:
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

def get_trend_view_svg(client: BetaAnalyticsDataClient, w: DailyWindow) -> str:
    df = run_report(client, PROPERTY_ID, ymd(w.window_start), ymd(w.window_end), ["date"], ["sessions","transactions","purchaseRevenue"])
    if df.empty:
        x = [(w.window_start + dt.timedelta(days=i)).strftime("%m/%d") for i in range(7)]
        return combined_index_svg(x, [[100]*7,[100]*7,[100]*7], ["#0055a5","#16a34a","#c2410c"], ["Sessions","Revenue","CVR"])

    # GA4 date comes as YYYYMMDD string; convert to pandas datetime64 for .dt usage
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

def get_best_sellers_with_trends(client: BetaAnalyticsDataClient, w: DailyWindow, image_map: Dict[str, str]) -> pd.DataFrame:
    order = [OrderBy(metric=OrderBy.MetricOrderBy(metric_name="itemsPurchased"), desc=True)]
    top = run_report(client, PROPERTY_ID, ymd(w.yesterday), ymd(w.yesterday), ["itemId","itemName"], ["itemsPurchased"], order_bys=order, limit=5)
    if top.empty:
        return pd.DataFrame(columns=["itemId","itemName","itemsPurchased_yesterday","trend_svg","image_url"])
    top["itemsPurchased"] = pd.to_numeric(top["itemsPurchased"], errors="coerce").fillna(0.0)
    top = top.rename(columns={"itemsPurchased":"itemsPurchased_yesterday"})
    skus = top["itemId"].tolist()

    ts = run_report(client, PROPERTY_ID, ymd(w.window_start), ymd(w.window_end), ["date","itemId"], ["itemsPurchased"], dimension_filter=ga_filter_in("itemId", skus), limit=10000)
    axis_dates = [w.window_start + dt.timedelta(days=i) for i in range(7)]
    xlabels = [d.strftime("%m/%d") for d in axis_dates]

    if ts.empty:
        top["trend_svg"] = ""
        top["image_url"] = top["itemId"].map(lambda s: image_map.get(str(s).strip(), ""))
    # Ensure columns exist even if time series is empty
    if "trend_svg" not in top.columns:
        top["trend_svg"] = ""
    if "image_url" not in top.columns:
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


def get_rising_products(client: BetaAnalyticsDataClient, w: DailyWindow, top_n: int = 5) -> pd.DataFrame:
    """Rising products by purchase quantity delta (D-1 vs D-2), with views added.

    NOTE: GA4 Data API has strict dimension/metric compatibility. Some properties reject
    `itemViews` with item dimensions (common InvalidArgument). To keep the widget stable,
    we:
      1) compute rising by `itemsPurchased` only
      2) fetch views via a *separate* compatible report, with fallbacks
    """

    # 1) Purchases (stable)
    d1 = run_report(
        client,
        PROPERTY_ID,
        ymd(w.yesterday),
        ymd(w.yesterday),
        ["itemId", "itemName"],
        ["itemsPurchased"],
        limit=10000,
    )
    d0 = run_report(
        client,
        PROPERTY_ID,
        ymd(w.day_before),
        ymd(w.day_before),
        ["itemId"],
        ["itemsPurchased"],
        limit=10000,
    )

    if d1.empty:
        return pd.DataFrame(columns=["itemId", "itemName", "itemViews_yesterday", "delta"])

    for c in ["itemsPurchased"]:
        d1[c] = pd.to_numeric(d1[c], errors="coerce").fillna(0.0)
        if not d0.empty:
            d0[c] = pd.to_numeric(d0[c], errors="coerce").fillna(0.0)

    if d0.empty:
        d0 = pd.DataFrame(columns=["itemId", "itemsPurchased"])

    m = d1.merge(d0, on="itemId", how="left", suffixes=("_y", "_d0")).fillna(0.0)
    m["delta"] = m["itemsPurchased_y"] - m["itemsPurchased_d0"]
    m = m.sort_values("delta", ascending=False).head(top_n)

    # 2) Views (best-effort, with fallbacks)
    skus = [str(x).strip() for x in m["itemId"].tolist() if str(x).strip()]
    views_df = pd.DataFrame(columns=["itemId", "itemViews_yesterday"])
    if skus:
        # Try GA4 native item view metrics first; if incompatible, fallback to eventCount(view_item)
        for metric_name, use_event_filter in [
            ("itemViewEvents", False),
            ("itemsViewed", False),
            ("eventCount", True),
        ]:
            try:
                v = run_report(
                    client,
                    PROPERTY_ID,
                    ymd(w.yesterday),
                    ymd(w.yesterday),
                    ["itemId"],
                    [metric_name],
                    dimension_filter=(
                        ga_filter_and([
                            ga_filter_in("itemId", skus),
                            ga_filter_eq("eventName", "view_item"),
                        ])
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


def get_category_ctr_trend(client: BetaAnalyticsDataClient, w: DailyWindow) -> pd.DataFrame:
    """Category Trend (CTR 7D)

    Priority:
    1) BigQuery (recommended): uses events table and UNNEST(items) so item_category/item_category2 are reliably available
    2) GA4 Data API fallback: may return zeros if the property doesn't support requested dims/metrics

    CTR = select_item / view_item_list (item-level rows)
    """

    # Axis (last 7 days including end)
    axis_dates = [w.window_end - dt.timedelta(days=i) for i in range(6, -1, -1)]
    xlabels = [d.strftime("%m/%d") for d in axis_dates]

    # Requested structure (display labels)
    structure = {
        "OUTER": ["경량패딩/슬림다운", "미드/롱다운", "인터체인지", "베스트", "방수자켓", "바람막이"],
        "FLEECE": ["플리스 풀오버", "자켓/베스트", "팬츠", "플리스 악세서리"],
        "TOP": ["플리스", "셔츠", "라운드티", "폴로티/집업", "맨투맨/후드티"],
        "PANTS": ["긴바지", "카고/조거", "원피스/스커트", "점프수트"],
        "SHOES": ["윈터부츠", "옴니맥스", "등산화", "트레일러닝", "스니커즈", "샌들/슬리퍼"],
    }

    # GA4 실제 값 기반 매핑 (BQ 결과 기준)
    ga_outer_alias = {
        "OUTER": "OUTER",
        "FLEECE": "FLEECE",
        "TOP": "TOPS",
        "PANTS": "PANTS",
        "SHOES": "FOOTWEAR",
    }

    category_map = {
        "OUTER": {
            "경량패딩/슬림다운": ["Padding/Slim Down"],
            "미드/롱다운": ["Mid/Heavy Down"],
            "인터체인지": ["Interchange (3 in 1)"],
            "베스트": [],
            "방수자켓": ["Rain"],
            "바람막이": [],
        },
        "FLEECE": {
            "플리스 풀오버": ["Fleece pullover"],
            "자켓/베스트": ["Jacket"],
            "팬츠": [],
            "플리스 악세서리": [],
        },
        "TOP": {
            "플리스": ["Fleece top"],
            "셔츠": [],
            "라운드티": ["Round T-shirt"],
            "폴로티/집업": ["Polo/Zip up"],
            "맨투맨/후드티": [],
        },
        "PANTS": {
            "긴바지": ["Pants"],
            "카고/조거": [],
            "원피스/스커트": [],
            "점프수트": [],
        },
        "SHOES": {
            "윈터부츠": ["Boots"],
            "옴니맥스": ["Omni-Max"],
            "등산화": ["Hiking"],
            "트레일러닝": [],
            "스니커즈": ["Sneakers"],
            "샌들/슬리퍼": [],
        },
    }

    def norm_outer(s: str) -> str:
        return str(s or "").strip().upper()

    def norm_sub(s: str) -> str:
        s = str(s or "").strip()
        s = re.sub(r"\s+", " ", s)
        s = s.replace("／", "/")
        return s

    def norm_key(s: str) -> str:
        return norm_sub(s).lower()

    def spark_svg(xlabels, ys, width=260, height=70, stroke="#2563eb"):
        if not ys:
            ys = [0.0]
        mn, mx = min(ys), max(ys)
        if mx == mn:
            mx = mn + 1.0
        pad = 8
        iw, ih = width - pad * 2, height - pad * 2
        pts = []
        for i, v in enumerate(ys):
            x = pad + (iw * (i / (len(ys) - 1 if len(ys) > 1 else 1)))
            y = pad + ih * (1 - ((v - mn) / (mx - mn)))
            pts.append((x, y))
        path = "M " + " L ".join([f"{x:.1f},{y:.1f}" for x, y in pts])
        dots = "".join([f"<circle cx='{x:.1f}' cy='{y:.1f}' r='2.2' fill='{stroke}'/>" for x, y in pts])
        return (
            f"<svg width='{width}' height='{height}' viewBox='0 0 {width} {height}' xmlns='http://www.w3.org/2000/svg'>"
            f"<rect x='0' y='0' width='{width}' height='{height}' rx='8' fill='white' stroke='#eef2ff'/>"
            f"<path d='{path}' fill='none' stroke='{stroke}' stroke-width='2.2'/>"
            f"{dots}</svg>"
        )

    def build_rows(clicks_views_map: dict) -> pd.DataFrame:
        rows = []
        color_cycle = ["#0055a5", "#16a34a", "#c2410c", "#7c3aed", "#0f766e"]
        color_i = 0

        actual_by_outer = {}
        for (outer, sub, d), (c, v) in clicks_views_map.items():
            actual_by_outer.setdefault(outer, set()).add(sub)

        for outer, subs in structure.items():
            o = norm_outer(ga_outer_alias.get(outer, outer))
            actual_subs = sorted(actual_by_outer.get(o, set()))
            actual_norm = {norm_key(a): a for a in actual_subs}

            for desired_sub in subs:
                mapped = category_map.get(outer, {}).get(desired_sub, [])
                matched = []
                for m in mapped:
                    mk = norm_key(m)
                    if mk and mk in actual_norm:
                        matched.append(actual_norm[mk])

                ys = []
                for d in axis_dates:
                    clicks = views = 0.0
                    if matched:
                        for a in matched:
                            c, v = clicks_views_map.get((o, a, d), (0.0, 0.0))
                            clicks += float(c or 0.0)
                            views += float(v or 0.0)
                    ctr = (clicks / views) * 100.0 if views else 0.0
                    ys.append(ctr)

                stroke = color_cycle[color_i % len(color_cycle)]
                color_i += 1
                label = f"{outer} · {desired_sub}"
                latest = ys[-1] if ys else 0.0
                avg = (sum(ys) / len(ys)) if ys else 0.0
                rows.append({
                    "itemCategory": label,
                    "ctr_latest": float(latest),
                    "ctr_avg7d": float(avg),
                    "trend_svg": spark_svg(xlabels, ys, width=260, height=70, stroke=stroke),
                })

        return pd.DataFrame(rows)

    # --- BigQuery path ---
    if bigquery is not None and BQ_EVENTS_TABLE:
        try:
            bq = bigquery.Client()
            start = (w.window_end - dt.timedelta(days=6)).strftime('%Y%m%d')
            end = w.window_end.strftime('%Y%m%d')
            sql = f"""
            SELECT
              PARSE_DATE('%Y%m%d', event_date) AS d,
              UPPER(IFNULL(items.item_category, '')) AS c1,
              IFNULL(items.item_category2, '') AS c2,
              SUM(CASE WHEN event_name='select_item' THEN 1 ELSE 0 END) AS clicks,
              SUM(CASE WHEN event_name='view_item_list' THEN 1 ELSE 0 END) AS views
            FROM `{BQ_EVENTS_TABLE}`
            CROSS JOIN UNNEST(items) AS items
            WHERE _TABLE_SUFFIX BETWEEN '{start}' AND '{end}'
              AND event_name IN ('view_item_list','select_item')
            GROUP BY 1,2,3
            """
            df = bq.query(sql).to_dataframe()
            clicks_views_map = {}
            for r in df.itertuples(index=False):
                d = getattr(r, 'd')
                c1 = norm_outer(getattr(r, 'c1', ''))
                c2 = norm_sub(getattr(r, 'c2', ''))
                if not d or not c1 or not c2:
                    continue
                clicks_views_map[(c1, c2, d)] = (float(getattr(r,'clicks',0) or 0.0), float(getattr(r,'views',0) or 0.0))
            return build_rows(clicks_views_map)
        except Exception as e:
            print(f"[WARN] Category Trend BigQuery failed, fallback to GA API: {type(e).__name__}: {e}")

    # --- GA4 Data API fallback (may produce zeros) ---
    # We keep the previous implementation by calling a lightweight GA query: click/view events by hierarchy pairs.
    try:
        clicks_views_map = {}
        dim_pairs = [
            ('itemCategory', 'itemCategory2'),
            ('itemCategory2', 'itemCategory3'),
            ('itemCategory3', 'itemCategory4'),
            ('itemCategory', 'itemCategory3'),
        ]

        def prep_df(df: pd.DataFrame, d_outer: str, d_sub: str, click_col: str, view_col: str) -> pd.DataFrame:
            if df.empty:
                return df
            df = df.copy()
            df['date'] = pd.to_datetime(df['date'], format='%Y%m%d', errors='coerce').dt.date
            df = df.dropna(subset=['date'])
            df[click_col] = pd.to_numeric(df[click_col], errors='coerce').fillna(0.0)
            df[view_col] = pd.to_numeric(df[view_col], errors='coerce').fillna(0.0)
            df[d_outer] = df[d_outer].astype(str).map(norm_outer)
            df[d_sub] = df[d_sub].astype(str).map(norm_sub)
            df = df[(df[d_outer] != '') & (df[d_sub] != '')]
            return df

        for d_outer, d_sub in dim_pairs:
            try:
                df = run_report(
                    client,
                    date_ranges=[DateRange(start_date=ymd(w.window_end - dt.timedelta(days=6)), end_date=ymd(w.window_end))],
                    dimensions=[Dimension(name='date'), Dimension(name=d_outer), Dimension(name=d_sub)],
                    metrics=[Metric(name='eventCount')],
                    dimension_filter=ga_filter_in('eventName', ['view_item_list', 'select_item']),
                    limit=100000,
                )
                if df.empty:
                    continue
                df = prep_df(df, d_outer, d_sub, 'eventCount', 'eventCount')
                if df.empty:
                    continue
                # Split by eventName is not available with this dim set, so cannot compute CTR reliably here
                # Return all zeros structure to avoid crash
                break
            except Exception:
                continue
        return build_rows(clicks_views_map)
    except Exception:
        return build_rows({})




# =========================
# Category Trend: PDP Views (view_item) — BigQuery
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
    "PANTS": {
        "긴바지": ["Pants"],
    },
    "FOOTWEAR": {
        "윈터부츠": ["Boots"],
        "옴니맥스": ["Omni-Max"],
        "등산화": ["Hiking"],
        "스니커즈": ["Sneakers"],
    },
}


def get_category_pdp_view_trend_bq(w: DailyWindow) -> pd.DataFrame:
    """Category PDP Trend (7D) based on BigQuery events (view_item).

    Returns DataFrame columns:
      - itemCategory: label (e.g., OUTER · 경량패딩/슬림다운)
      - views_d1: yesterday views
      - views_avg7d: avg over 7D
      - trend_svg: sparkline

    Notes:
      - Uses item_id lookup to attach normalized (c1,c2) for view_item.
      - SALE correction: if item_category='SALE', use (item_category2, item_category3) as (c1,c2).
    """

    axis_dates = [w.window_end - dt.timedelta(days=i) for i in range(6, -1, -1)]
    xlabels = [d.strftime('%m/%d') for d in axis_dates]

    # If BigQuery is not available, return empty
    if bigquery is None or not BQ_EVENTS_TABLE:
        return pd.DataFrame(columns=["itemCategory", "views_d1", "views_avg7d", "trend_svg"]) 

    try:
        bq = bigquery.Client()

        start_suffix = (w.window_end - dt.timedelta(days=6)).strftime('%Y%m%d')
        end_suffix = w.window_end.strftime('%Y%m%d')
        lookup_start = (w.window_end - dt.timedelta(days=30)).strftime('%Y%m%d')
        lookup_end = w.window_end.strftime('%Y%m%d')

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

        return pd.DataFrame(rows)

    except Exception as e:
        print(f"[WARN] PDP Category Trend BigQuery failed: {type(e).__name__}: {e}")
        return pd.DataFrame(columns=["itemCategory", "views_d1", "views_avg7d", "trend_svg"]) 
def get_search_trends(client: BetaAnalyticsDataClient, w: DailyWindow) -> Dict[str, pd.DataFrame]:
    lookback_start = w.window_end - dt.timedelta(days=13)
    df = run_report(client, PROPERTY_ID, ymd(lookback_start), ymd(w.window_end), ["date","searchTerm"], ["eventCount"], dimension_filter=ga_filter_eq("eventName", SEARCH_EVENT), limit=10000)
    if df.empty:
        return {"new": pd.DataFrame(columns=["searchTerm"]), "rising": pd.DataFrame(columns=["searchTerm","pct"])}

    df["date"] = df["date"].apply(parse_yyyymmdd)
    df["eventCount"] = pd.to_numeric(df["eventCount"], errors="coerce").fillna(0.0)

    y_df = df[df["date"] == w.window_end].groupby("searchTerm", as_index=False)["eventCount"].sum().sort_values("eventCount", ascending=False)
    prior_start = w.window_end - dt.timedelta(days=7)
    prior_df = df[(df["date"] >= prior_start) & (df["date"] <= (w.window_end - dt.timedelta(days=1)))]
    prior_agg = prior_df.groupby("searchTerm", as_index=False)["eventCount"].mean().rename(columns={"eventCount":"prior_avg"})

    merged = y_df.merge(prior_agg, on="searchTerm", how="left").fillna(0.0)
    new_terms = merged[merged["prior_avg"] == 0].head(3)[["searchTerm"]].copy()
    rising = merged[merged["prior_avg"] > 0].copy()
    rising["pct"] = (rising["eventCount"] - rising["prior_avg"]) / rising["prior_avg"] * 100.0
    rising = rising.sort_values("pct", ascending=False).head(3)[["searchTerm","pct"]]
    return {"new": new_terms, "rising": rising}

def render_html(logo_b64: str, w: DailyWindow, overall: Dict[str, Dict[str, float]],
                signup_users: Dict[str, float],
                channel_snapshot: pd.DataFrame, paid_detail: pd.DataFrame, paid_top3: pd.DataFrame,
                kpi_snapshot: pd.DataFrame, trend_svg: str, best_sellers: pd.DataFrame,
                rising: pd.DataFrame, category_pdp_trend: pd.DataFrame,
                search_new: pd.DataFrame, search_rising: pd.DataFrame) -> str:

    cur = overall["current"]; prev = overall["prev"]
    s_dod = pct_change(cur["sessions"], prev["sessions"])
    o_dod = pct_change(cur["transactions"], prev["transactions"])
    r_dod = pct_change(cur["purchaseRevenue"], prev["purchaseRevenue"])
    c_pp = cur["cvr"] - prev["cvr"]

    su_cur = float(signup_users.get('current', 0.0) or 0.0)
    su_prev = float(signup_users.get('prev', 0.0) or 0.0)
    su_dod = pct_change(su_cur, su_prev)

    def delta_color(v): return "#1d4ed8" if v >= 0 else "#c2410c"
    def kpi_cell(title, value, delta_text, color):
        return f"""<td width='20%' valign='top' style='background:#ffffff;border-radius:12px;border:1px solid #dfe6f3;box-shadow:0 6px 18px rgba(0,0,0,0.05);padding:10px 12px;'>
          <div style='font-size:11px;color:#667;margin-bottom:2px;'>{title}</div>
          <div style='font-size:18px;font-weight:800;color:#111;'>{value}</div>
          <div style='font-size:11px;margin-top:2px;'>전일 대비 <span style='color:{color};font-weight:800;'>{delta_text}</span></div>
        </td>"""

    chan_rows = "".join([
        f"<tr style='font-weight:{'800' if r.bucket=='Total' else '400'};background:{'#f8fafc' if r.bucket=='Total' else 'transparent'};'><td style='padding:7px 8px;border-bottom:1px solid #f1f5ff;'>{r.bucket}</td>"
        f"<td align='right' style='padding:7px 8px;border-bottom:1px solid #f1f5ff;'>{fmt_int(r.sessions)}</td>"
        f"<td align='right' style='padding:7px 8px;border-bottom:1px solid #f1f5ff;'>{fmt_int(r.transactions)}</td>"
        f"<td align='right' style='padding:7px 8px;border-bottom:1px solid #f1f5ff;'>{fmt_currency_krw(r.purchaseRevenue)}</td>"
        f"<td align='right' style='padding:7px 8px;border-bottom:1px solid #f1f5ff;color:{delta_color(r.rev_dod)};'>{'+' if r.rev_dod>=0 else ''}{fmt_pct(r.rev_dod,1)}</td></tr>"
        for r in channel_snapshot.itertuples(index=False)
    ])

    paid_rows = "".join([
        f"<tr style='font-weight:{'800' if r.sub_channel=='Total' else '400'};background:{'#f8fafc' if r.sub_channel=='Total' else 'transparent'};'><td style='padding:7px 8px;border-bottom:1px solid #f1f5ff;'>{r.sub_channel}</td>"
        f"<td align='right' style='padding:7px 8px;border-bottom:1px solid #f1f5ff;'>{fmt_int(r.sessions)}</td>"
        f"<td align='right' style='padding:7px 8px;border-bottom:1px solid #f1f5ff;'>{fmt_currency_krw(r.purchaseRevenue)}</td>"
        f"<td align='right' style='padding:7px 8px;border-bottom:1px solid #f1f5ff;color:{delta_color(r.rev_dod)};'>{'+' if r.rev_dod>=0 else ''}{fmt_pct(r.rev_dod,1)}</td></tr>"
        for r in paid_detail.itertuples(index=False)
    ])

    

    paid_top3_rows = "".join([
    f"<tr style='font-weight:{'800' if r.sessionSourceMedium=='Total' else '400'};background:{'#f8fafc' if r.sessionSourceMedium=='Total' else 'transparent'};'><td style='padding:6px 8px;border-bottom:1px solid #f1f5ff;'>{r.sessionSourceMedium}</td>"
    f"<td align='right' style='padding:6px 8px;border-bottom:1px solid #f1f5ff;'>{fmt_int(getattr(r,'sessions',0))}</td>"
    f"<td align='right' style='padding:6px 8px;border-bottom:1px solid #f1f5ff;'>{fmt_currency_krw(r.purchaseRevenue)}</td></tr>"
    for r in paid_top3.itertuples(index=False)
    ])

    kpi_rows = "".join([
        f"<tr><td style='padding:7px 8px;border-bottom:1px solid #f1f5ff;'>{r.metric}</td>"
        f"<td align='right' style='padding:7px 8px;border-bottom:1px solid #f1f5ff;'>{r.value_fmt}</td>"
        f"<td align='right' style='padding:7px 8px;border-bottom:1px solid #f1f5ff;color:{delta_color(r.dod)};'>{'+' if r.dod>=0 else ''}{r.dod_fmt}</td></tr>"
        for r in kpi_snapshot.itertuples(index=False)
    ])

    def product_img_cell(url: str) -> str:
        u = (url or PLACEHOLDER_IMG or "").strip()
        if u:
            return (
                f"<img src='{u}' width='28' height='28' "
                "style='display:block;border-radius:8px;object-fit:cover;border:1px solid #dfe6f3;'/>"
            )
        return "<div style='width:28px;height:28px;border-radius:8px;background:#e9eefb;border:1px solid #dfe6f3;'></div>"

    bs_rows_parts = []
    for r in best_sellers.itertuples(index=False):
        bs_rows_parts.append(
            "<tr>"
            f"<td style='padding:7px 8px;border-bottom:1px solid #f1f5ff;'>{product_img_cell(getattr(r, 'image_url', ''))}</td>"
            f"<td style='padding:7px 8px;border-bottom:1px solid #f1f5ff;'>{(getattr(r, 'itemName', None) or '—')}</td>"
            f"<td style='padding:7px 8px;border-bottom:1px solid #f1f5ff;'>{(getattr(r, 'itemId', None) or '—')}</td>"
            f"<td align='right' style='padding:7px 8px;border-bottom:1px solid #f1f5ff;'>{fmt_int(getattr(r, 'itemsPurchased_yesterday', 0))}</td>"
            f"<td style='padding:7px 8px;border-bottom:1px solid #f1f5ff;'>{getattr(r, 'trend_svg', '')}</td>"
            "</tr>"
        )
    bs_rows = "".join(bs_rows_parts)

    

    # Rising Products: include image column as well.
    rp_rows = "".join([
        "<tr>"
        f"<td style='padding:7px 8px;border-bottom:1px solid #f1f5ff;'>{product_img_cell(getattr(r, 'image_url', ''))}</td>"
        f"<td style='padding:7px 8px;border-bottom:1px solid #f1f5ff;'>{(getattr(r, 'itemId', None) or '—')}</td>"
        f"<td style='padding:7px 8px;border-bottom:1px solid #f1f5ff;'>{(getattr(r, 'itemName', None) or '—')}</td>"
        f"<td align='right' style='padding:7px 8px;border-bottom:1px solid #f1f5ff;'>{fmt_int(getattr(r,'itemViews_yesterday',0))}</td>"
        f"<td align='right' style='padding:7px 8px;border-bottom:1px solid #f1f5ff;'><span style='color:{delta_color(getattr(r,'delta',0))};font-weight:800;'>{'▲' if getattr(r,'delta',0)>=0 else '▼'} {fmt_int(abs(getattr(r,'delta',0)))}</span></td>"
        "</tr>"
        for r in rising.itertuples(index=False)
    ])

    

    pdp_rows = "".join([
        "<tr>"
        f"<td style='padding:7px 8px;border-bottom:1px solid #f1f5ff;'>{r.itemCategory}</td>"
        "<td style='padding:7px 8px;border-bottom:1px solid #f1f5ff;'>"
        f"<div style='font-size:10px;color:#667085;margin-bottom:4px;'>D-1 <b style='color:#111827;'>{fmt_int(getattr(r,'views_d1',0))}</b> · 7D Avg <b style='color:#111827;'>{fmt_int(getattr(r,'views_avg7d',0))}</b></div>"
        f"{getattr(r,'trend_svg','')}"
        "</td></tr>"
        for r in category_pdp_trend.itertuples(index=False)
    ])
    def kw_rows(df, mode):
        if df.empty:
            return "<tr><td style='padding:6px 8px;border-bottom:1px solid #f1f5ff;color:#99a;'>—</td><td align='right' style='padding:6px 8px;border-bottom:1px solid #f1f5ff;color:#99a;'>—</td></tr>"
        rows = []
        for r in df.itertuples(index=False):
            if mode == "new":
                rows.append(f"<tr><td style='padding:6px 8px;border-bottom:1px solid #f1f5ff;'>{r.searchTerm}</td><td align='right' style='padding:6px 8px;border-bottom:1px solid #f1f5ff;color:#99a;'>new</td></tr>")
            else:
                tag = f"{'+' if r.pct>=0 else ''}{r.pct:.0f}%"
                rows.append(f"<tr><td style='padding:6px 8px;border-bottom:1px solid #f1f5ff;'>{r.searchTerm}</td><td align='right' style='padding:6px 8px;border-bottom:1px solid #f1f5ff;color:#99a;'>{tag}</td></tr>")
        return "".join(rows)

    date_badge = f"{ymd(w.yesterday)} 기준 (어제 데이터)"

    return f"""<!DOCTYPE html>
<html lang='ko'><head><meta charset='utf-8'><title>Daily Digest</title></head>
<body style='margin:0;padding:0;background:#f5f7fb;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Noto Sans KR",Arial,sans-serif;'>
<table role='presentation' width='100%' cellspacing='0' cellpadding='0' style='background:#f5f7fb;'>
  <tr><td align='center'>
    <table role='presentation' width='900' cellspacing='0' cellpadding='0' style='padding:24px 12px;background:#f5f7fb;'>
      <tr><td>
        <table role='presentation' width='100%' cellspacing='0' cellpadding='0' style='background:#ffffff;border-radius:18px;border:1px solid #e6e9ef;box-shadow:0 6px 18px rgba(0,0,0,0.06);'>
          <tr><td style='height:4px;background:#0055a5;line-height:4px;font-size:0;'>&nbsp;</td></tr>
          <tr><td style='padding:18px 20px 14px 20px;'>
            <table role='presentation' width='100%' cellspacing='0' cellpadding='0'>
              <tr>
                <td valign='middle'>
                  <table role='presentation' cellspacing='0' cellpadding='0'><tr>
                    <td style='padding-right:10px;'>{("<img alt='Columbia' height='28' style='display:block;height:28px;' src='data:image/png;base64,"+logo_b64+"'>") if logo_b64 else ""}</td>
                    <td>
                      <div style='font-size:18px;font-weight:700;color:#0055a5;margin-bottom:2px;'>COLUMBIA SPORTSWEAR KOREA</div>
                      <div style='font-size:13px;color:#555;'>Daily eCommerce Performance Digest</div>
                    </td>
                  </tr></table>
                  <span style='display:inline-block;font-size:11px;padding:4px 10px;border-radius:999px;background:#eaf3ff;color:#0055a5;border:1px solid #d7e7ff;margin-top:10px;'>{date_badge}</span>
                </td>
                <td valign='top' align='right' style='padding-top:2px;'>
                  <div style='font-size:11px;color:#667;line-height:1.5;text-align:right;'>
                    오늘의 핵심<br>
                    <b style='color:#0055a5;'>Revenue {('+' if r_dod>=0 else '')}{fmt_pct(r_dod,1)}</b> ·
                    <b style='color:#c2410c;'>CVR {fmt_pp(c_pp,2)}</b>
                  </div>
                </td>
              </tr>
            </table>
          </td></tr>

          <tr><td style='border-top:1px solid #eef2fb;'></td></tr>

          <tr><td style='padding:14px 18px 18px 18px;'>

            <div style='font-size:13px;font-weight:700;color:#224;margin:6px 0 8px 0;'>Traffic / MKT</div>
            <table width='100%' cellpadding='0' cellspacing='0' style='border-collapse:separate;border-spacing:10px 10px;'><tr>
              {kpi_cell('Sessions', fmt_int(cur['sessions']), f"{('+' if s_dod>=0 else '')}{fmt_pct(s_dod,1)}", delta_color(s_dod))}
              {kpi_cell('Orders', fmt_int(cur['transactions']), f"{('+' if o_dod>=0 else '')}{fmt_pct(o_dod,1)}", delta_color(o_dod))}
              {kpi_cell('Revenue', fmt_currency_krw(cur['purchaseRevenue']), f"{('+' if r_dod>=0 else '')}{fmt_pct(r_dod,1)}", delta_color(r_dod))}
              {kpi_cell('CVR', f"{cur['cvr']*100:.2f}%", fmt_pp(c_pp,2), delta_color(c_pp))}
              {kpi_cell('Sign-up Users', fmt_int(su_cur), f"{('+' if su_dod>=0 else '')}{fmt_pct(su_dod,1)}", delta_color(su_dod))}
            </tr></table>

            <table width='100%' cellpadding='0' cellspacing='0' style='background:#ffffff;border-radius:12px;border:1px solid #dfe6f3;box-shadow:0 6px 18px rgba(0,0,0,0.05);padding:10px 12px;margin-top:6px;'><tr><td>
              <div style='font-size:11px;font-weight:600;color:#224;margin-bottom:2px;'>Channel Snapshot</div>
              <div style='font-size:10px;color:#888;margin-bottom:8px;line-height:1.4;'>Organic / Paid AD / Owned / Awareness / SNS</div>
              <table width='100%' cellpadding='0' cellspacing='0' style='border-collapse:collapse;font-size:11px;'>
                <tr style='background:#f6f8ff;'>
                  <th align='left' style='padding:7px 8px;border-bottom:1px solid #e8eefb;'>Channel</th>
                  <th align='right' style='padding:7px 8px;border-bottom:1px solid #e8eefb;'>Traffic</th>
                  <th align='right' style='padding:7px 8px;border-bottom:1px solid #e8eefb;'>Orders</th>
                  <th align='right' style='padding:7px 8px;border-bottom:1px solid #e8eefb;'>Revenue</th>
                  <th align='right' style='padding:7px 8px;border-bottom:1px solid #e8eefb;'>DoD (Rev)</th>
                </tr>
                {chan_rows}
              </table>
            </td></tr></table>

            <table width='100%' cellpadding='0' cellspacing='0' style='background:#ffffff;border-radius:12px;border:1px solid #dfe6f3;box-shadow:0 6px 18px rgba(0,0,0,0.05);padding:10px 12px;margin-top:10px;'><tr><td>
              <div style='font-size:11px;font-weight:600;color:#224;margin-bottom:2px;'>Paid AD Detail</div>
              <table width='100%' cellpadding='0' cellspacing='0' style='border-collapse:collapse;font-size:11px;'>
                <tr style='background:#f6f8ff;'>
                  <th align='left' style='padding:7px 8px;border-bottom:1px solid #e8eefb;'>Sub-channel</th>
                  <th align='right' style='padding:7px 8px;border-bottom:1px solid #e8eefb;'>Traffic</th>
                  <th align='right' style='padding:7px 8px;border-bottom:1px solid #e8eefb;'>Revenue</th>
                  <th align='right' style='padding:7px 8px;border-bottom:1px solid #e8eefb;'>DoD (Rev)</th>
                </tr>
                {paid_rows}
              </table>
              {("<div style='margin-top:10px;'><div style='font-size:11px;font-weight:700;color:#224;margin-bottom:6px;'>Paid AD Top 3 (Traffic, Revenue)</div>"
                "<table width='100%' cellpadding='0' cellspacing='0' style='border-collapse:collapse;font-size:11px;'>"
                "<tr style='background:#f6f8ff;'><th align='left' style='padding:7px 8px;border-bottom:1px solid #e8eefb;'>Source / Medium</th>"
                "<th align='right' style='padding:7px 8px;border-bottom:1px solid #e8eefb;'>Traffic</th><th align='right' style='padding:7px 8px;border-bottom:1px solid #e8eefb;'>Revenue</th></tr>"
                + paid_top3_rows + "</table></div>") if paid_top3_rows else ""}
            </td></tr></table>

            <div style='font-size:13px;font-weight:700;color:#224;margin:16px 0 8px 0;'>Site Ops</div>
            <table width='100%' cellpadding='0' cellspacing='0' style='background:#ffffff;border-radius:12px;border:1px solid #dfe6f3;box-shadow:0 6px 18px rgba(0,0,0,0.05);padding:10px 12px;'><tr><td>
              <div style='font-size:11px;font-weight:600;color:#224;margin-bottom:2px;'>KPI Snapshot</div>
              <table width='100%' cellpadding='0' cellspacing='0' style='border-collapse:collapse;font-size:11px;'>
                <tr style='background:#f6f8ff;'><th align='left' style='padding:7px 8px;border-bottom:1px solid #e8eefb;'>Metric</th>
                  <th align='right' style='padding:7px 8px;border-bottom:1px solid #e8eefb;'>Value</th>
                  <th align='right' style='padding:7px 8px;border-bottom:1px solid #e8eefb;'>DoD</th></tr>
                {kpi_rows}
              </table>
            </td></tr></table>

            <table width='100%' cellpadding='0' cellspacing='0' style='background:#ffffff;border-radius:12px;border:1px solid #dfe6f3;box-shadow:0 6px 18px rgba(0,0,0,0.05);padding:10px 12px;margin-top:10px;'><tr><td>
              <div style='font-size:11px;font-weight:600;color:#224;margin-bottom:2px;'>Trend View</div>
              <div style='border:1px solid #eef2fb;border-radius:10px;padding:8px 10px;background:#fbfdff;'>{trend_svg}</div>
            </td></tr></table>

            <div style='font-size:13px;font-weight:700;color:#224;margin:16px 0 8px 0;'>Product</div>
            <table width='100%' cellpadding='0' cellspacing='0' style='background:#ffffff;border-radius:12px;border:1px solid #dfe6f3;box-shadow:0 6px 18px rgba(0,0,0,0.05);padding:10px 12px;'><tr><td>
              <div style='font-size:11px;font-weight:600;color:#224;margin-bottom:2px;'>Best Sellers · TOP 5 (Qty)</div>
              <table width='100%' cellpadding='0' cellspacing='0' style='border-collapse:collapse;font-size:11px;'>
                <tr style='background:#f6f8ff;'><th align='left' style='padding:7px 8px;border-bottom:1px solid #e8eefb;'>Image</th>
                  <th align='left' style='padding:7px 8px;border-bottom:1px solid #e8eefb;'>Item (EN)</th>
                  <th align='left' style='padding:7px 8px;border-bottom:1px solid #e8eefb;'>SKU</th>
                  <th align='right' style='padding:7px 8px;border-bottom:1px solid #e8eefb;'>Qty (D-1)</th>
                  <th align='left' style='padding:7px 8px;border-bottom:1px solid #e8eefb;'>7D Trend</th></tr>
                {bs_rows}
              </table>
            </td></tr></table>

            <table width='100%' cellpadding='0' cellspacing='0' style='background:#ffffff;border-radius:12px;border:1px solid #dfe6f3;box-shadow:0 6px 18px rgba(0,0,0,0.05);padding:10px 12px;margin-top:10px;'><tr><td>
              <div style='font-size:11px;font-weight:600;color:#224;margin-bottom:2px;'>Rising Products</div>
              <table width='100%' cellpadding='0' cellspacing='0' style='border-collapse:collapse;font-size:11px;'>
                <tr style='background:#f6f8ff;'><th align='left' style='padding:7px 8px;border-bottom:1px solid #e8eefb;'>Image</th>
                  <th align='left' style='padding:7px 8px;border-bottom:1px solid #e8eefb;'>SKU</th>
                  <th align='left' style='padding:7px 8px;border-bottom:1px solid #e8eefb;'>Item</th>
                  <th align='right' style='padding:7px 8px;border-bottom:1px solid #e8eefb;'>Views (D-1)</th>
                  <th align='right' style='padding:7px 8px;border-bottom:1px solid #e8eefb;'>Qty Δ</th></tr>
                {rp_rows}
              </table>
            </td></tr></table>

            <table width='100%' cellpadding='0' cellspacing='0' style='background:#ffffff;border-radius:12px;border:1px solid #dfe6f3;box-shadow:0 6px 18px rgba(0,0,0,0.05);padding:10px 12px;margin-top:10px;'><tr><td>
              <div style='font-size:11px;font-weight:600;color:#224;margin-bottom:2px;'>PDP Trend</div>
              <table width='100%' cellpadding='0' cellspacing='0' style='border-collapse:collapse;font-size:11px;'>
                <tr style='background:#f6f8ff;'><th align='left' style='padding:7px 8px;border-bottom:1px solid #e8eefb;'>Category</th>
                  <th align='left' style='padding:7px 8px;border-bottom:1px solid #e8eefb;'>PDP Views 7D Trend</th></tr>
                {pdp_rows}
              </table>
            </td></tr></table>


            <table width='100%' cellpadding='0' cellspacing='0' style='background:#ffffff;border-radius:12px;border:1px solid #dfe6f3;box-shadow:0 6px 18px rgba(0,0,0,0.05);padding:10px 12px;margin-top:10px;'><tr><td>
              <div style='font-size:11px;font-weight:600;color:#224;margin-bottom:2px;'>Search Trend</div>
              <table width='100%' cellpadding='0' cellspacing='0' style='border-collapse:collapse;font-size:11px;'>
                <tr><td width='50%' valign='top' style='padding-right:10px;'>
                    <div style='font-size:11px;font-weight:700;color:#224;margin-bottom:6px;'>신규 진입 Top 3</div>
                    <table width='100%' cellpadding='0' cellspacing='0' style='border-collapse:collapse;font-size:11px;'>{kw_rows(search_new,'new')}</table>
                  </td>
                  <td width='50%' valign='top' style='padding-left:10px;'>
                    <div style='font-size:11px;font-weight:700;color:#224;margin-bottom:6px;'>급상승 Top 3</div>
                    <table width='100%' cellpadding='0' cellspacing='0' style='border-collapse:collapse;font-size:11px;'>{kw_rows(search_rising,'rising')}</table>
                  </td></tr>
              </table>
            </td></tr></table>

            <div style='margin-top:16px;font-size:10px;color:#99a;text-align:right;'>Auto-generated · Rolling window ends at {ymd(w.window_end)}</div>
          </td></tr>
        </table>
      </td></tr>
    </table>
  </td></tr>
</table>
</body></html>"""

def main():
    if not PROPERTY_ID:
        raise SystemExit("ERROR: GA4_PROPERTY_ID is empty. Set env var GA4_PROPERTY_ID and retry.")
    # Use ADC but request Analytics Data API scope explicitly; otherwise you may get
    # 'Request had insufficient authentication scopes' when using gcloud end-user creds.
    _scopes = [
        'https://www.googleapis.com/auth/analytics.readonly',
        'https://www.googleapis.com/auth/cloud-platform',
    ]
    _creds, _proj = google_auth_default(scopes=_scopes)
    client = BetaAnalyticsDataClient(credentials=_creds)
    if bigquery is None:
        print('[WARN] google-cloud-bigquery not installed; Category Trend may be 0. Install: pip install google-cloud-bigquery')
    w = compute_window()
    logo_b64 = load_logo_base64(LOGO_PATH)

    overall = get_overall_kpis(client, w)
    signup_users = get_multi_event_users(client, w, ["signup_complete", "signup"])
    channel_snapshot = get_channel_snapshot(client, w)
    paid_detail = get_paid_detail(client, w)
    paid_top3 = get_paid_top3(client, w)
    kpi_snapshot = get_kpi_snapshot_table(client, w, overall)
    trend_svg = get_trend_view_svg(client, w)
    image_map = load_image_map_from_excel_urls(IMAGE_XLS_PATH)
    best_sellers = get_best_sellers_with_trends(client, w, image_map)
    rising = get_rising_products(client, w, top_n=5)
    # Attach images for Rising Products as well.
    rising = attach_image_urls(rising, image_map)

    missing = []
    if not best_sellers.empty and 'itemId' in best_sellers.columns:
        missing += [sku for sku in best_sellers['itemId'].tolist() if str(sku).strip() not in image_map]
    if not rising.empty and 'itemId' in rising.columns:
        missing += [sku for sku in rising['itemId'].tolist() if str(sku).strip() not in image_map]
    if missing:
        write_missing_image_skus(MISSING_SKU_OUT, missing)
    # Category Trend (CTR) removed by request.
    category_pdp_trend = get_category_pdp_view_trend_bq(w)
    search = get_search_trends(client, w)

    html = render_html(logo_b64, w, overall, signup_users, channel_snapshot, paid_detail, paid_top3,
                       kpi_snapshot, trend_svg, best_sellers, rising, category_pdp_trend,
                       search["new"], search["rising"])

    with open(OUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"[OK] Wrote HTML: {OUT_HTML}")
    print(f"     Window: {ymd(w.window_start)} ~ {ymd(w.window_end)} (rolling 7d)")

if __name__ == "__main__":
    main()
