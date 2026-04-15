from __future__ import annotations

FALLBACK_METRIC_HTML = (
    "<span class='metric-slot active text-slate-400' data-metric='sessions'>-</span>"
    "<span class='metric-slot text-slate-400' data-metric='orders'>-</span>"
    "<span class='metric-slot text-slate-400' data-metric='revenue'>-</span>"
)


REPORT_PATCH_CSS = """
  <style>
    :root{--report-max:none;--motion-ease:cubic-bezier(.2,.8,.2,1);}
    .report-body{background:linear-gradient(180deg,#f8fafc 0%,#eef2f7 100%);}
    .report-shell{width:100%;}
    .metric-switch{display:inline-flex;gap:6px;flex-wrap:wrap;}
    .metric-tab{border:1px solid rgba(148,163,184,.25);background:#fff;border-radius:999px;padding:8px 12px;font-size:11px;font-weight:900;color:#64748b;transition:all .22s var(--motion-ease);box-shadow:0 6px 18px rgba(15,23,42,.04)}
    .metric-tab:hover{transform:translateY(-1px);box-shadow:0 12px 28px rgba(15,23,42,.08)}
    .metric-tab.active{background:#0f172a;color:#fff;border-color:#0f172a;box-shadow:0 14px 32px rgba(15,23,42,.16)}
    .metric-label{display:none}.metric-label.active{display:inline}
    .metric-slot{display:none}.metric-slot.active{display:block;animation:metricSwap .42s var(--motion-ease)}
    .metric-inline .metric-slot.active{display:inline}
    .report-card,.bucket-detail-panel,.weather-card{animation:cardRise .7s var(--motion-ease) both;transform-origin:center bottom}
    .report-card:hover{transform:translateY(-4px);box-shadow:0 18px 40px rgba(15,23,42,.08)}
    .kpi-card{position:relative;overflow:hidden;transition:transform .24s var(--motion-ease), box-shadow .24s var(--motion-ease), border-color .24s var(--motion-ease)}
    .kpi-card:before{content:'';position:absolute;inset:-40% auto auto -20%;width:60%;height:180%;background:linear-gradient(90deg,transparent,rgba(255,255,255,.45),transparent);transform:rotate(14deg);animation:shineSweep 4.2s linear infinite;pointer-events:none}
    .kpi-card:hover{transform:translateY(-6px) scale(1.01);box-shadow:0 22px 44px rgba(15,23,42,.08);border-color:rgba(59,130,246,.22)}
    .kpi-value{animation:numberPop .8s var(--motion-ease) both}
    .channel-table-wrap,.paid-table-wrap{overflow-x:auto}
    .channel-table-wrap table,.paid-table-wrap table{min-width:980px}
    @keyframes cardRise{from{opacity:0;transform:translateY(26px) scale(.985)}to{opacity:1;transform:translateY(0) scale(1)}}
    @keyframes metricSwap{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}
    @keyframes numberPop{0%{opacity:.2;transform:translateY(12px) scale(.96)}60%{opacity:1;transform:translateY(-2px) scale(1.02)}100%{opacity:1;transform:translateY(0) scale(1)}}
    @keyframes shineSweep{0%{transform:translateX(-160%) rotate(14deg)}100%{transform:translateX(320%) rotate(14deg)}}
  </style>
"""

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


import os
import json
import base64
import datetime as dt
import re
from pathlib import Path
from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple, Any
from zoneinfo import ZoneInfo
from urllib.parse import urlencode, quote_plus
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
from contextlib import suppress
from html import unescape as html_unescape

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
DAILY_ONLY_MODE = os.getenv("DAILY_DIGEST_DAILY_ONLY_MODE", "true").strip().lower() in ("1", "true", "yes", "y")
BACKFILL_MONTH = os.getenv("DAILY_DIGEST_BACKFILL_MONTH", "2026-04").strip()

IMAGE_XLS_PATH = os.getenv("DAILY_DIGEST_IMAGE_XLS_PATH", "상품코드별 이미지.xlsx").strip()
MISSING_SKU_OUT = os.getenv("DAILY_DIGEST_MISSING_SKU_OUT", "missing_image_skus.csv")
PLACEHOLDER_IMG = os.getenv("DAILY_DIGEST_PLACEHOLDER_IMG", "").strip()

# BigQuery GA4 export wildcard table
BQ_EVENTS_TABLE = os.getenv("DAILY_DIGEST_BQ_EVENTS_TABLE", "columbia-ga4.analytics_358593394.events_*").strip()
BQ_LOCATION = os.getenv("DAILY_DIGEST_BQ_LOCATION", "asia-northeast3").strip()
ADMIN_BQ_PROJECT = os.getenv("DAILY_DIGEST_ADMIN_BQ_PROJECT", os.getenv("BQ_PROJECT", "columbia-ga4")).strip()
ADMIN_BQ_LOCATION = os.getenv("DAILY_DIGEST_ADMIN_BQ_LOCATION", BQ_LOCATION).strip()
ADMIN_BQ_TABLE = os.getenv("DAILY_DIGEST_ADMIN_BQ_TABLE", "crm_mart.member_funnel_admin_daily").strip()

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

POWERLINK_BRANDS = [
    x.strip() for x in os.getenv(
        "DAILY_DIGEST_POWERLINK_BRANDS",
        "노스페이스,뉴발란스,아크테릭스,블랙야크,아이더,K2,디스커버리,밀레,파타고니아,네파",
    ).split(",")
    if x.strip()
]
POWERLINK_TIMEOUT_SEC = int(os.getenv("DAILY_DIGEST_POWERLINK_TIMEOUT_SEC", "12"))
POWERLINK_MAX_TAGS = int(os.getenv("DAILY_DIGEST_POWERLINK_MAX_TAGS", "6"))
POWERLINK_MAX_CARDS = int(os.getenv("DAILY_DIGEST_POWERLINK_MAX_CARDS", "4"))
POWERLINK_CACHE_PATH = os.getenv(
    "DAILY_DIGEST_POWERLINK_CACHE_PATH",
    os.path.join(DATA_DIR, "brand_powerlink_snapshot.json"),
).strip()
POWERLINK_ENABLED = os.getenv("DAILY_DIGEST_POWERLINK_ENABLED", "true").strip().lower() in ("1", "true", "yes", "y")
POWERLINK_USE_PLAYWRIGHT = os.getenv("DAILY_DIGEST_POWERLINK_USE_PLAYWRIGHT", "true").strip().lower() in ("1", "true", "yes", "y")
POWERLINK_SCREENSHOT_DIR = os.getenv(
    "DAILY_DIGEST_POWERLINK_SCREENSHOT_DIR",
    os.path.join(DATA_DIR, "powerlink_captures"),
).strip()
POWERLINK_WAIT_MS = int(os.getenv("DAILY_DIGEST_POWERLINK_WAIT_MS", "2500"))
POWERLINK_VIEWPORT_WIDTH = int(os.getenv("DAILY_DIGEST_POWERLINK_VIEWPORT_WIDTH", "430"))
POWERLINK_VIEWPORT_HEIGHT = int(os.getenv("DAILY_DIGEST_POWERLINK_VIEWPORT_HEIGHT", "2200"))

CHANNEL_BUCKET_ORDER = [
    "Awareness",
    "Paid Ad",
    "Organic Traffic",
    "Official SNS",
    "Owned Channel",
    "etc",
]

TARGET_ROAS_XLS_PATH = os.getenv("DAILY_DIGEST_TARGET_ROAS_XLS_PATH", "target_roas.xlsx").strip()
MEDIA_SPEND_XLS_PATH = os.getenv("DAILY_DIGEST_MEDIA_SPEND_XLS_PATH", os.path.join(DATA_DIR, "paid_media_spend.xlsx")).strip()
MEDIA_SPEND_HISTORY_PATH = os.getenv("DAILY_DIGEST_MEDIA_SPEND_HISTORY_PATH", os.path.join(DATA_DIR, "paid_media_spend_history.csv")).strip()
MEDIA_SPEND_VENDOR_DIR = os.getenv("DAILY_DIGEST_MEDIA_SPEND_VENDOR_DIR", DATA_DIR).strip()

KMA_SERVICE_KEY = os.getenv("DAILY_DIGEST_KMA_SERVICE_KEY", "").strip()
KMA_LOCATION = os.getenv("DAILY_DIGEST_KMA_LOCATION", "Seoul").strip()
KMA_NX = int(os.getenv("DAILY_DIGEST_KMA_NX", "60"))
KMA_NY = int(os.getenv("DAILY_DIGEST_KMA_NY", "127"))
KMA_MID_LAND_REG_ID = os.getenv("DAILY_DIGEST_KMA_MID_LAND_REG_ID", "11B00000").strip()
KMA_MID_TA_REG_ID = os.getenv("DAILY_DIGEST_KMA_MID_TA_REG_ID", "11B10101").strip()


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


def _qualified_admin_bq_table(table_name: str) -> str:
    t = str(table_name or "").strip().strip("`")
    if not t:
        return ""
    if t.count(".") == 2:
        return t
    if t.count(".") == 1 and ADMIN_BQ_PROJECT:
        return f"{ADMIN_BQ_PROJECT}.{t}"
    return t


def _num(value: Any) -> float:
    try:
        v = pd.to_numeric(value, errors="coerce")
        if pd.isna(v):
            return 0.0
        return float(v)
    except Exception:
        return 0.0


def _parse_bq_table_parts(table_name: str) -> tuple[str, str, str]:
    t = str(table_name or "").strip().strip("`")
    parts = [x for x in t.split(".") if x]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2 and ADMIN_BQ_PROJECT:
        return ADMIN_BQ_PROJECT, parts[0], parts[1]
    raise ValueError(f"Invalid BigQuery table name: {table_name}")


def _admin_table_columns(bq: Any, table_name: str) -> set[str]:
    project_id, dataset_id, table_id = _parse_bq_table_parts(table_name)
    sql = f"""
    SELECT LOWER(column_name) AS column_name
    FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = @table_name
    """
    cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("table_name", "STRING", table_id),
        ]
    )
    df = bq.query(sql, job_config=cfg, location=ADMIN_BQ_LOCATION).to_dataframe()
    if df.empty:
        return set()
    return {str(v).strip().lower() for v in df["column_name"].tolist() if str(v).strip()}


def _admin_login_sessions_expr(columns: set[str]) -> tuple[str, str]:
    login_candidates = [
        "login_users",
        "login_user_count",
        "login_user_cnt",
        "login_count",
        "login_cnt",
        "logged_in_users",
        "logged_in_user_count",
        "member_login_users",
        "member_login_count",
        "logins",
        "로그인수",
        "로그인_수",
    ]
    for col in login_candidates:
        if col.lower() in columns:
            return f"SUM(COALESCE({col}, 0))", col
    return "SUM(COALESCE(sessions, 0))", "sessions"


def fetch_admin_period_snapshot(start_date: dt.date, end_date: dt.date) -> dict:
    table_name = _qualified_admin_bq_table(ADMIN_BQ_TABLE)
    empty = {
        "date_start": start_date.isoformat(),
        "date_end": end_date.isoformat(),
        "sessions": 0.0,
        "pv": 0.0,
        "signups": 0.0,
        "orders": 0.0,
        "buyers": 0.0,
        "revenue": 0.0,
        "erp_revenue": 0.0,
        "total_price": 0.0,
        "cancel_amount": 0.0,
        "aov": 0.0,
        "source": "admin_load_failed",
    }
    if bigquery is None or not table_name:
        return empty
    try:
        bq = bigquery.Client(project=ADMIN_BQ_PROJECT or None, location=ADMIN_BQ_LOCATION or None)
        admin_cols = _admin_table_columns(bq, table_name)
        sessions_expr, sessions_source_col = _admin_login_sessions_expr(admin_cols)
        sql = f"""
        SELECT
          {sessions_expr} AS sessions,
          SUM(COALESCE(pv, 0)) AS pv,
          SUM(COALESCE(signups, 0)) AS signups,
          SUM(COALESCE(orders, 0)) AS orders,
          SUM(COALESCE(buyers, 0)) AS buyers,
          SUM(COALESCE(revenue, 0)) AS revenue,
          SUM(COALESCE(total_price, 0)) AS total_price,
          SUM(COALESCE(cancel_amount, 0)) AS cancel_amount
        FROM `{table_name}`
        WHERE report_date BETWEEN @start_date AND @end_date
        """
        cfg = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date.isoformat()),
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date.isoformat()),
            ]
        )
        df = bq.query(sql, job_config=cfg, location=ADMIN_BQ_LOCATION).to_dataframe()
        if df.empty:
            return empty
        row = df.iloc[0].to_dict()
        orders = _num(row.get("orders", 0))
        buyers = _num(row.get("buyers", 0))
        revenue = _num(row.get("revenue", 0))
        total_price = _num(row.get("total_price", 0))
        cancel_amount = _num(row.get("cancel_amount", 0))
        erp_revenue = revenue
        return {
            "date_start": start_date.isoformat(),
            "date_end": end_date.isoformat(),
            "sessions": _num(row.get("sessions", 0)),
            "pv": _num(row.get("pv", 0)),
            "signups": _num(row.get("signups", 0)),
            "orders": orders,
            "buyers": buyers if buyers > 0 else orders,
            "revenue": erp_revenue,
            "erp_revenue": erp_revenue,
            "total_price": total_price,
            "cancel_amount": cancel_amount,
            "aov": (erp_revenue / orders) if orders else 0.0,
            "source": f"admin_bq_daily_erp_login_users:{sessions_source_col}",
        }
    except Exception as e:
        print(f"[WARN] fetch_admin_period_snapshot failed: {type(e).__name__}: {e}")
        return empty


def build_admin_overall(w: DigestWindow) -> Dict[str, Dict[str, float]]:
    return {
        "current": fetch_admin_period_snapshot(w.cur_start, w.cur_end),
        "prev": fetch_admin_period_snapshot(w.prev_start, w.prev_end),
        "yoy": fetch_admin_period_snapshot(w.yoy_start, w.yoy_end),
    }


# =========================
# KMA weather forecast helpers
# =========================
def _kma_json_request(url: str, params: Dict[str, Any], timeout: int = 12) -> dict:
    qs = urlencode({k: v for k, v in params.items() if v is not None})
    full_url = f"{url}?{qs}"
    with urlopen(full_url, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("KMA response is not JSON object")
    return data


def _kma_pick_items(payload: dict) -> List[dict]:
    body = ((payload or {}).get("response") or {}).get("body") or {}
    items = body.get("items") or {}
    if isinstance(items, dict):
        items = items.get("item") or []
    if isinstance(items, list):
        return items
    return []


def _latest_short_forecast_base(now_kst: Optional[dt.datetime] = None) -> Tuple[str, str]:
    now_kst = now_kst or dt.datetime.now(ZoneInfo("Asia/Seoul"))
    candidates = ["2300", "2000", "1700", "1400", "1100", "0800", "0500", "0200"]
    current_hm = now_kst.strftime("%H%M")
    base_date = now_kst.date()
    chosen = None
    for c in candidates:
        if current_hm >= c:
            chosen = c
            break
    if chosen is None:
        base_date = base_date - dt.timedelta(days=1)
        chosen = "2300"
    return base_date.strftime("%Y%m%d"), chosen


def _latest_mid_forecast_tmfc(now_kst: Optional[dt.datetime] = None) -> str:
    now_kst = now_kst or dt.datetime.now(ZoneInfo("Asia/Seoul"))
    base_date = now_kst.date()
    hhmm = now_kst.strftime("%H%M")
    tmfc = "1800" if hhmm >= "1800" else "0600"
    if hhmm < "0600":
        base_date = base_date - dt.timedelta(days=1)
        tmfc = "1800"
    return base_date.strftime("%Y%m%d") + tmfc


def _safe_int(value: Any) -> Optional[int]:
    try:
        return int(round(float(str(value).replace("mm", "").replace("cm", "").strip())))
    except Exception:
        return None


def _safe_float(value: Any) -> Optional[float]:
    try:
        return float(str(value).replace("mm", "").replace("cm", "").strip())
    except Exception:
        return None


def _sky_text(sky: Any, pty: Any) -> str:
    p = str(pty or "").strip()
    if p in ("1", "2", "4"):
        return {"1": "Rain", "2": "Rain/Snow", "4": "Shower"}.get(p, "Rain")
    if p == "3":
        return "Snow"
    s = str(sky or "").strip()
    return {"1": "Sunny", "3": "Cloudy", "4": "Overcast"}.get(s, "-")


def _weather_emoji(label: str) -> str:
    t = (label or "").lower()
    if "snow" in t:
        return "❄️"
    if "rain" in t or "shower" in t:
        return "🌧️"
    if "overcast" in t:
        return "☁️"
    if "cloud" in t:
        return "⛅"
    if "sun" in t or "clear" in t:
        return "☀️"
    return "🌤️"



def _kma_error_message(api_name: str, exc: Exception) -> str:
    if isinstance(exc, HTTPError):
        if exc.code == 403:
            return (
                f"{api_name}: HTTP 403 Forbidden. "
                "Check DAILY_DIGEST_KMA_SERVICE_KEY and make sure KMA API usage is approved "
                "for getVilageFcst, getMidLandFcst, and getMidTa."
            )
        return f"{api_name}: HTTP {exc.code} {exc.reason}"
    if isinstance(exc, URLError):
        return f"{api_name}: URL error: {exc.reason}"
    return f"{api_name}: {type(exc).__name__}: {exc}"


def get_weekly_weather_forecast(end_date: Optional[dt.date] = None) -> dict:
    today = end_date or dt.datetime.now(ZoneInfo("Asia/Seoul")).date()
    fallback_days = [today + dt.timedelta(days=i) for i in range(1, 8)]
    empty = {
        "location": KMA_LOCATION,
        "status": "disabled" if not KMA_SERVICE_KEY else "error",
        "source": "KMA API Hub",
        "days": [
            {
                "date": ymd(d),
                "label": d.strftime("%a"),
                "min_temp": None,
                "max_temp": None,
                "pop": None,
                "weather": "-",
                "weather_emoji": "🌤️",
            }
            for d in fallback_days
        ],
        "generated_at": dt.datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S"),
        "error": (
            "Weather disabled: set DAILY_DIGEST_KMA_SERVICE_KEY to enable KMA forecast."
            if not KMA_SERVICE_KEY
            else ""
        ),
    }
    if not KMA_SERVICE_KEY:
        return empty

    out: Dict[str, Dict[str, Any]] = {
        ymd(d): {
            "date": ymd(d),
            "label": d.strftime("%a"),
            "min_temp": None,
            "max_temp": None,
            "pop": None,
            "weather": "-",
            "weather_emoji": "🌤️",
        }
        for d in fallback_days
    }

    errors: List[str] = []

    try:
        base_date, base_time = _latest_short_forecast_base()
        short_payload = _kma_json_request(
            "https://apihub.kma.go.kr/api/typ02/openApi/VilageFcstInfoService_2.0/getVilageFcst",
            {
                "pageNo": 1,
                "numOfRows": 1000,
                "dataType": "JSON",
                "base_date": base_date,
                "base_time": base_time,
                "nx": KMA_NX,
                "ny": KMA_NY,
                "authKey": KMA_SERVICE_KEY,
            },
        )
        by_day: Dict[str, Dict[str, Any]] = {}
        for item in _kma_pick_items(short_payload):
            d = str(item.get("fcstDate") or "")
            if len(d) != 8:
                continue
            dkey = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
            if dkey not in out:
                continue
            rec = by_day.setdefault(dkey, {"temps": [], "pops": [], "sky": {}, "pty": {}})
            cat = str(item.get("category") or "")
            val = item.get("fcstValue")
            ftime = str(item.get("fcstTime") or "")
            if cat == "TMP":
                fv = _safe_float(val)
                if fv is not None:
                    rec["temps"].append(fv)
            elif cat == "TMN":
                fv = _safe_float(val)
                if fv is not None:
                    out[dkey]["min_temp"] = fv
            elif cat == "TMX":
                fv = _safe_float(val)
                if fv is not None:
                    out[dkey]["max_temp"] = fv
            elif cat == "POP":
                iv = _safe_int(val)
                if iv is not None:
                    rec["pops"].append(iv)
            elif cat == "SKY":
                rec["sky"][ftime] = str(val)
            elif cat == "PTY":
                rec["pty"][ftime] = str(val)
        for dkey, rec in by_day.items():
            if out[dkey]["min_temp"] is None and rec["temps"]:
                out[dkey]["min_temp"] = min(rec["temps"])
            if out[dkey]["max_temp"] is None and rec["temps"]:
                out[dkey]["max_temp"] = max(rec["temps"])
            if rec["pops"]:
                out[dkey]["pop"] = max(rec["pops"])
            target_time = "1200"
            sky = rec["sky"].get(target_time) or next(iter(rec["sky"].values()), "")
            pty = rec["pty"].get(target_time) or next(iter(rec["pty"].values()), "")
            out[dkey]["weather"] = _sky_text(sky, pty)
            out[dkey]["weather_emoji"] = _weather_emoji(out[dkey]["weather"])
    except Exception as e:
        errors.append(_kma_error_message("short_forecast_failed", e))

    try:
        tmfc = _latest_mid_forecast_tmfc()
        land_payload = _kma_json_request(
            "https://apihub.kma.go.kr/api/typ02/openApi/MidFcstInfoService/getMidLandFcst",
            {
                "pageNo": 1,
                "numOfRows": 10,
                "dataType": "JSON",
                "regId": KMA_MID_LAND_REG_ID,
                "tmFc": tmfc,
                "authKey": KMA_SERVICE_KEY,
            },
        )
        ta_payload = _kma_json_request(
            "https://apihub.kma.go.kr/api/typ02/openApi/MidFcstInfoService/getMidTa",
            {
                "pageNo": 1,
                "numOfRows": 10,
                "dataType": "JSON",
                "regId": KMA_MID_TA_REG_ID,
                "tmFc": tmfc,
                "authKey": KMA_SERVICE_KEY,
            },
        )
        land_items = _kma_pick_items(land_payload)
        ta_items = _kma_pick_items(ta_payload)
        land = land_items[0] if land_items else {}
        ta = ta_items[0] if ta_items else {}
        for idx in range(3, 11):
            d = today + dt.timedelta(days=idx)
            dkey = ymd(d)
            if dkey not in out:
                continue
            am = str(land.get(f"wf{idx}Am") or "").strip()
            pm = str(land.get(f"wf{idx}Pm") or "").strip()
            wf = pm or am
            if wf:
                out[dkey]["weather"] = wf
                out[dkey]["weather_emoji"] = _weather_emoji(wf)
            pop_am = _safe_int(land.get(f"rnSt{idx}Am"))
            pop_pm = _safe_int(land.get(f"rnSt{idx}Pm"))
            if pop_am is not None or pop_pm is not None:
                out[dkey]["pop"] = max([v for v in [pop_am, pop_pm] if v is not None])
            mn = _safe_float(ta.get(f"taMin{idx}"))
            mx = _safe_float(ta.get(f"taMax{idx}"))
            if mn is not None:
                out[dkey]["min_temp"] = mn
            if mx is not None:
                out[dkey]["max_temp"] = mx
    except Exception as e:
        errors.append(_kma_error_message("mid_forecast_failed", e))

    days = [out[k] for k in sorted(out.keys())][:7]
    status = "ok" if any(
        (d.get("min_temp") is not None or d.get("max_temp") is not None or d.get("weather") not in ("", "-"))
        for d in days
    ) else "error"

    error_msg = ""
    if status != "ok":
        error_msg = " | ".join(errors) if errors else "weather forecast unavailable"

    return {
        "location": KMA_LOCATION,
        "status": status,
        "source": "KMA API Hub",
        "days": days,
        "generated_at": dt.datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S"),
        "error": error_msg,
    }



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
    ys = [float(v) for v in (ys or [0.0])]
    if not xlabels:
        xlabels = ["--"] * len(ys)
    elif len(xlabels) < len(ys):
        xlabels = list(xlabels) + ["--"] * (len(ys) - len(xlabels))
    elif len(xlabels) > len(ys):
        xlabels = list(xlabels[:len(ys)])

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
# Naver PowerLink / brand search helpers
# =========================
def _powerlink_clean_text(value: Any) -> str:
    s = html_unescape(re.sub(r"<[^>]+>", " ", str(value or "")))
    s = s.replace("\xa0", " ").replace("\u200b", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _powerlink_dedupe_keep_order(values: List[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for v in values:
        raw = str(v or "").strip()
        key = raw.lower()
        if not raw or key in seen:
            continue
        seen.add(key)
        out.append(raw)
    return out


def _powerlink_sanitize_line(line: Any) -> str:
    s = _powerlink_clean_text(line)
    if not s:
        return ""
    noise_patterns = [
        r"https?://\S+",
        r"(?:^|\s)REQUIRE(?:\s|$)",
        r"window\.__[A-Z0-9_]+",
        r'"link"\s*:\s*"[^"]+"',
        r"eventNo=\d+",
        r"utm_[a-z_]+=\S+",
        r"#(?:ct|nx_option_form|mask\S*|clip\S*|[A-Fa-f0-9]{6,}|x3D|ffffff|00000008)\b",
    ]
    for pat in noise_patterns:
        s = re.sub(pat, " ", s, flags=re.I)
    s = re.sub(r"\s+", " ", s).strip(" -·|,/")
    return s.strip()


_KEYWORD_LINE_RX = re.compile(
    r"(신상|신규|NEW|BEST|베스트|랭킹|할인|쿠폰|혜택|세일|자켓|재킷|바람막이|티셔츠|후드|패딩|액티비티|Promotion|UP TO|OFF)",
    re.I,
)


def _powerlink_filter_candidate_lines(lines: List[str]) -> List[str]:
    out: List[str] = []
    for line in lines:
        s = _powerlink_sanitize_line(line)
        if not s:
            continue
        if len(s) < 2:
            continue
        if s.lower().startswith("https://m.search.naver.com"):
            continue
        if re.match(r"^#[A-Za-z0-9_\-]{1,40}$", s) and re.search(r"(?:ct|nx_|mask|clip)", s, re.I):
            continue
        out.append(s)
    return _powerlink_dedupe_keep_order(out)


def _extract_tag_candidates(text_or_lines: Any) -> List[str]:
    values = text_or_lines if isinstance(text_or_lines, list) else re.split(r"[\n|]+", str(text_or_lines or ""))
    tags: List[str] = []
    for raw in values:
        s = _powerlink_sanitize_line(raw)
        if not s:
            continue
        if s.startswith("#"):
            if 2 <= len(s) <= 30 and not re.search(r"(?:ct|nx_|mask|clip|x3D)$", s, re.I):
                tags.append(s.replace("# ", "#"))
            continue
        if len(s) <= 16 and _KEYWORD_LINE_RX.search(s):
            tags.append(s if s.startswith("#") else s)
    return _powerlink_dedupe_keep_order(tags)[:POWERLINK_MAX_TAGS]


def _extract_card_candidates(text_or_lines: Any) -> List[str]:
    values = text_or_lines if isinstance(text_or_lines, list) else re.split(r"[\n|]+", str(text_or_lines or ""))
    cards: List[str] = []
    for raw in values:
        s = _powerlink_sanitize_line(raw)
        if not s:
            continue
        if len(s) > 18:
            continue
        if re.search(r"(?:남성|여성|신규|신상|베스트|랭킹|세일|혜택|자켓|액티비티|선물)", s, re.I):
            cards.append(s)
    return _powerlink_dedupe_keep_order(cards)[:POWERLINK_MAX_CARDS]


def _fallback_extract_powerlink_from_html(raw_html: str, brand: str, url: str, now_kst: str) -> dict:
    text_only = _powerlink_clean_text(raw_html)
    lines = _powerlink_filter_candidate_lines(re.split(r"(?<=[.!?])\s+|\n+", text_only))
    tags = _extract_tag_candidates(lines)
    cards = _extract_card_candidates(lines)
    title_like = [x for x in lines if len(x) <= 40 and not x.startswith("#")]
    item = {
        "brand": brand,
        "query": brand,
        "status": "ok",
        "status_label": "수집 성공",
        "headline": title_like[0] if title_like else "",
        "main_copy": title_like[1] if len(title_like) > 1 else "",
        "sub_copy": title_like[2] if len(title_like) > 2 else "",
        "tags": tags,
        "cards": cards,
        "keyword_highlights": _powerlink_dedupe_keep_order(tags + cards)[:POWERLINK_MAX_TAGS],
        "collected_at": now_kst,
        "source": url,
        "capture_path": "",
        "capture_method": "html_fallback",
    }
    if not (item["headline"] or item["main_copy"] or item["tags"] or item["cards"]):
        item["status"] = "empty"
        item["status_label"] = "문구 미검출"
    return item


_POWERLINK_BLOCK_FINDER_JS = r"""
(brand) => {
  const norm = (s) => (s || '').replace(/\s+/g, ' ').trim();
  const keywordRe = /(신상|신규|NEW|BEST|베스트|랭킹|할인|쿠폰|혜택|세일|자켓|재킷|바람막이|티셔츠|후드|패딩|액티비티|Promotion|UP TO|OFF)/i;
  const isVisible = (el) => {
    const st = window.getComputedStyle(el);
    const r = el.getBoundingClientRect();
    return st && st.display !== 'none' && st.visibility !== 'hidden' && r.width > 220 && r.height > 120;
  };
  const nodes = Array.from(document.querySelectorAll('section, article, div, li'));
  let best = null;
  let bestScore = -1;
  for (const el of nodes) {
    if (!isVisible(el)) continue;
    const r = el.getBoundingClientRect();
    if (r.width < 220 || r.height < 150 || r.height > 900) continue;
    const text = norm(el.innerText || '');
    if (!text || text.length < 20) continue;
    const lineCount = text.split(/
+/).length;
    const imgCount = el.querySelectorAll('img').length;
    const kwHits = (text.match(new RegExp(keywordRe, 'ig')) || []).length;
    const hashHits = (text.match(/#/g) || []).length;
    const containsBrand = brand && text.toLowerCase().includes(String(brand).toLowerCase());
    let score = 0;
    score += Math.min(imgCount, 3) * 5;
    score += Math.min(kwHits, 8) * 3;
    score += Math.min(hashHits, 4) * 2;
    score += Math.min(lineCount, 10);
    if (containsBrand) score += 4;
    if (imgCount >= 2 && kwHits >= 2) score += 8;
    if (text.length > 350) score -= 4;
    if (r.height > 700) score -= 4;
    if (score > bestScore) { bestScore = score; best = el; }
  }
  if (!best) return null;
  const r = best.getBoundingClientRect();
  const lines = Array.from(best.querySelectorAll('*'))
    .map((n) => norm(n.innerText || ''))
    .filter(Boolean)
    .filter((v, i, arr) => arr.indexOf(v) === i);
  const images = Array.from(best.querySelectorAll('img')).map((img) => ({
    src: img.currentSrc || img.src || '',
    alt: norm(img.alt || ''),
  })).filter((x) => x.src).slice(0, 6);
  return {
    lines,
    text: text,
    images,
    rect: { x: Math.max(r.x, 0), y: Math.max(r.y, 0), width: Math.max(r.width, 1), height: Math.max(r.height, 1) },
  };
}
"""


def _extract_powerlink_structured_from_lines(lines: List[str], brand: str, url: str, now_kst: str, capture_path: str = "") -> dict:
    cleaned = _powerlink_filter_candidate_lines(lines)
    tags = _extract_tag_candidates(cleaned)
    cards = _extract_card_candidates(cleaned)
    body_lines = [x for x in cleaned if x not in tags and x not in cards]
    title_candidates = [x for x in body_lines if len(x) <= 44]
    headline = title_candidates[0] if title_candidates else (cleaned[0] if cleaned else "")
    main_copy = title_candidates[1] if len(title_candidates) > 1 else ""
    sub_copy = title_candidates[2] if len(title_candidates) > 2 else ""
    if headline and headline.lower() == brand.lower() and len(title_candidates) > 1:
        headline = title_candidates[1]
        main_copy = title_candidates[2] if len(title_candidates) > 2 else ""
        sub_copy = title_candidates[3] if len(title_candidates) > 3 else ""
    item = {
        "brand": brand,
        "query": brand,
        "status": "ok",
        "status_label": "수집 성공",
        "headline": headline,
        "main_copy": main_copy,
        "sub_copy": sub_copy,
        "tags": tags,
        "cards": cards,
        "keyword_highlights": _powerlink_dedupe_keep_order(tags + cards)[:POWERLINK_MAX_TAGS],
        "collected_at": now_kst,
        "source": url,
        "capture_path": capture_path,
        "capture_method": "playwright_element",
    }
    if not (item["headline"] or item["main_copy"] or item["tags"] or item["cards"]):
        item["status"] = "empty"
        item["status_label"] = "문구 미검출"
    return item


def _fetch_brand_powerlink_snapshot_playwright(brands: List[str], cache_path: str, now_kst: str) -> List[dict]:
    from playwright.sync_api import sync_playwright

    ensure_dir(POWERLINK_SCREENSHOT_DIR)
    out: List[dict] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(
            viewport={"width": POWERLINK_VIEWPORT_WIDTH, "height": POWERLINK_VIEWPORT_HEIGHT},
            user_agent=(
                "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36"
            ),
            is_mobile=True,
            device_scale_factor=2,
            locale="ko-KR",
        )
        for brand in brands:
            url = f"https://m.search.naver.com/search.naver?query={quote_plus(brand)}"
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=max(30000, POWERLINK_TIMEOUT_SEC * 1000))
                with suppress(Exception):
                    page.wait_for_load_state("networkidle", timeout=5000)
                with suppress(Exception):
                    page.wait_for_timeout(POWERLINK_WAIT_MS)
                data = page.evaluate(_POWERLINK_BLOCK_FINDER_JS, brand)
                if not data or not data.get("lines"):
                    raise RuntimeError("powerlink block not found")
                safe_brand = re.sub(r"[^0-9A-Za-z가-힣_-]+", "_", brand)
                shot_path = os.path.join(POWERLINK_SCREENSHOT_DIR, f"{safe_brand}.png")
                page.screenshot(path=shot_path, clip=data["rect"])
                item = _extract_powerlink_structured_from_lines(data.get("lines") or [], brand, url, now_kst, shot_path)
                if not item.get("cards"):
                    img_alts = [x.get("alt", "") for x in (data.get("images") or [])]
                    item["cards"] = _extract_card_candidates(img_alts)
                    item["keyword_highlights"] = _powerlink_dedupe_keep_order((item.get("tags") or []) + (item.get("cards") or []))[:POWERLINK_MAX_TAGS]
                out.append(item)
            except Exception as e:
                fallback = {
                    "brand": brand,
                    "query": brand,
                    "status": "error",
                    "status_label": f"수집 실패: {type(e).__name__}",
                    "headline": "",
                    "main_copy": "",
                    "sub_copy": "",
                    "tags": [],
                    "cards": [],
                    "keyword_highlights": [],
                    "collected_at": now_kst,
                    "source": url,
                    "capture_path": "",
                    "capture_method": "playwright_element",
                }
                try:
                    html = page.content()
                    fb = _fallback_extract_powerlink_from_html(html, brand, url, now_kst)
                    fb["status"] = "ok" if (fb.get("headline") or fb.get("tags") or fb.get("cards")) else fallback["status"]
                    fb["status_label"] = "수집 성공" if fb["status"] == "ok" else fallback["status_label"]
                    fb["capture_method"] = "playwright_html_fallback"
                    out.append(fb)
                except Exception:
                    out.append(fallback)
        browser.close()
    if cache_path:
        try:
            ensure_dir(os.path.dirname(cache_path))
            write_json(cache_path, {"collected_at": now_kst, "brands": out})
        except Exception as e:
            print(f"[WARN] could not write powerlink cache: {type(e).__name__}: {e}")
    return out


def fetch_brand_powerlink_snapshot(brands: Optional[List[str]] = None, cache_path: Optional[str] = None) -> List[dict]:
    brands = [str(x).strip() for x in (brands or POWERLINK_BRANDS) if str(x).strip()]
    cache_path = (cache_path or POWERLINK_CACHE_PATH or "").strip()
    now_kst = dt.datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")

    if not POWERLINK_ENABLED:
        return [{
            "brand": b,
            "query": b,
            "status": "disabled",
            "status_label": "disabled",
            "headline": "",
            "main_copy": "",
            "sub_copy": "",
            "tags": [],
            "cards": [],
            "keyword_highlights": [],
            "collected_at": now_kst,
            "source": "powerlink_disabled",
            "capture_path": "",
            "capture_method": "disabled",
        } for b in brands]

    if POWERLINK_USE_PLAYWRIGHT:
        try:
            return _fetch_brand_powerlink_snapshot_playwright(brands, cache_path, now_kst)
        except Exception as e:
            print(f"[WARN] PowerLink Playwright capture failed, fallback to HTML scraping: {type(e).__name__}: {e}")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cache-Control": "no-cache",
    }
    out: List[dict] = []
    for brand in brands:
        url = f"https://m.search.naver.com/search.naver?query={quote_plus(brand)}"
        try:
            req = Request(url, headers=headers)
            with urlopen(req, timeout=POWERLINK_TIMEOUT_SEC) as resp:
                raw = resp.read().decode("utf-8", errors="ignore")
            out.append(_fallback_extract_powerlink_from_html(raw, brand, url, now_kst))
        except Exception as e:
            out.append({
                "brand": brand,
                "query": brand,
                "status": "error",
                "status_label": f"수집 실패: {type(e).__name__}",
                "headline": "",
                "main_copy": "",
                "sub_copy": "",
                "tags": [],
                "cards": [],
                "keyword_highlights": [],
                "collected_at": now_kst,
                "source": url,
                "capture_path": "",
                "capture_method": "html_fallback",
            })

    if cache_path:
        try:
            ensure_dir(os.path.dirname(cache_path))
            write_json(cache_path, {"collected_at": now_kst, "brands": out})
        except Exception as e:
            print(f"[WARN] could not write powerlink cache: {type(e).__name__}: {e}")
    return out


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


def get_ga_unique_users_3way(client: BetaAnalyticsDataClient, w: DigestWindow) -> Dict[str, float]:
    def get_one(start: dt.date, end: dt.date) -> float:
        df = run_report(client, PROPERTY_ID, ymd(start), ymd(end), [], ["totalUsers"])
        return float(df.iloc[0]["totalUsers"]) if (not df.empty and "totalUsers" in df.columns) else 0.0

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

def map_default_channel_group_to_bucket(default_channel_group: str = "") -> str:
    dcg = (default_channel_group or "").strip().lower()
    if not dcg:
        return ""
    if dcg in {"email", "sms", "mobile push notifications"}:
        return "Owned Channel"
    if dcg in {"organic social"}:
        return "Official SNS"
    if dcg in {"paid search", "paid social", "display", "paid shopping"}:
        return "Paid Ad"
    if dcg in {"video", "organic video", "affiliates", "cross-network", "audio", "paid video"}:
        return "Awareness"
    if dcg in {"direct", "organic search", "organic shopping", "referral", "unassigned"}:
        return "Organic Traffic"
    return ""


def classify_looker_channel(source_medium: str, campaign: str = "", default_channel_group: str = "") -> str:
    """
    Follow the uploaded Looker Studio / RTF CASE logic as the source of truth.
    We intentionally classify by sessionSourceMedium + sessionCampaignName to reduce
    GA default-channel Unassigned volume, while keeping the metric itself as GA sessions.
    """
    sm = (source_medium or "").strip()
    cp = (campaign or "").strip()

    if _rx(r"(?i).*(instagram).*").search(sm) and _rx(r"(?i).*(story).*").search(sm):
        return "Official SNS"
    if _rx(r"(?i).*(benz).*").search(sm):
        return "Organic Traffic"

    if _rx(r"(?i).*(nap).*").search(sm) and _rx(r"(?i).*(da).*").search(sm):
        return "Paid Ad"
    if _rx(r"(?i).*(toss).*").search(sm):
        return "Paid Ad"
    if _rx(r"(?i).*(blind).*").search(sm):
        return "Paid Ad"
    if _rx(r"(?i).*(kakaobs).*").search(sm):
        return "Paid Ad"

    if _rx(r"(?i).*(inhouse).*").search(sm):
        return "Organic Traffic"
    if _rx(r"(?i).*(lms).*").search(sm) or _rx(r"(?i).*(lms).*").search(cp):
        return "Owned Channel"
    if _rx(r"(?i).*(email|edm).*").search(sm):
        return "Owned Channel"
    if _rx(r"(?i).*(kakao_fridnstalk).*").search(sm):
        return "Owned Channel"

    if _rx(r"(?i).*(mkt|_bd).*").search(sm) or _rx(r"(?i).*(mkt|\[bd).*").search(cp):
        return "Awareness"

    if _rx(r"(?i).*(igshopping).*").search(sm):
        return "Official SNS"
    if _rx(r"(?i).*(facebook).*").search(sm) and _rx(r"(?i).*(referral).*").search(sm):
        return "Organic Traffic"
    if _rx(r"(?i).*(instagram).*").search(sm) and _rx(r"(?i).*(referral).*").search(sm):
        return "Official SNS"
    if _rx(r"(?i).*(meta|facebook|instagram|ig|fb).*").search(sm):
        return "Paid Ad"

    if _rx(r"(?i).*(google \/ cpc).*").search(sm) and _rx(r"(?i).*(디멘드젠|디멘드잰|디맨드젠|디맨드잰|dg|demandgen).*").search(cp):
        return "Awareness"
    if _rx(r"(?i).*(google \/ cpc).*").search(sm) and _rx(r"(?i).*(pmax).*").search(cp):
        return "Paid Ad"
    if _rx(r"(?i).*(google \/ cpc).*").search(sm) and _rx(r"(?i).*(유튜브|yt|youtube|instream|vac|vvc).*").search(cp):
        return "Awareness"
    if _rx(r"(?i).*(google \/ cpc).*").search(sm) and _rx(r"(?i).*(discovery).*").search(cp):
        return "Awareness"
    if _rx(r"(?i).*(google \/ cpc).*").search(sm) and _rx(r"(?i).*(sa|ss|검색).*").search(cp):
        return "Paid Ad"
    if _rx(r"(?i).*(google \/ cpc).*").search(sm):
        return "Paid Ad"

    if _rx(r"(?i).*(google \/ organic).*").search(sm):
        return "Organic Traffic"
    if _rx(r"(?i).*(google).*").search(sm):
        return "Organic Traffic"

    if _rx(r"(?i).*(youtube).*").search(sm):
        return "Organic Traffic"

    if _rx(r"(?i).*(naver).*").search(sm) and _rx(r"(?i).*(da).*").search(sm):
        return "Paid Ad"
    if _rx(r"(?i).*(gfa).*").search(sm):
        return "Paid Ad"
    if _rx(r"(?i).*(naverbs).*").search(sm):
        return "Paid Ad"
    if _rx(r"(?i).*(naver).*").search(sm) and _rx(r"(?i).*(cpc).*").search(sm):
        return "Paid Ad"
    if _rx(r"(?i).*(shopping_ad).*").search(sm):
        return "Paid Ad"
    if _rx(r"(?i).*(naver).*").search(sm) and _rx(r"(?i).*(shopping).*").search(sm):
        return "Organic Traffic"
    if _rx(r"(?i).*(naver).*").search(sm) and _rx(r"(?i).*(organic).*").search(sm):
        return "Organic Traffic"
    if _rx(r"(?i).*(naver).*").search(sm):
        return "Organic Traffic"

    if _rx(r"(?i).*(daum \/ organic).*").search(sm):
        return "Organic Traffic"
    if _rx(r"(?i).*(daum).*").search(sm) and _rx(r"(?i).*(referral).*").search(sm):
        return "Organic Traffic"
    if _rx(r"(?i).*(kakao_ch).*").search(sm) or _rx(r"(?i).*(kakao_ch).*").search(cp):
        return "Owned Channel"
    if _rx(r"(?i).*(kakao_alimtalk).*").search(sm):
        return "Owned Channel"
    if _rx(r"(?i).*(kakao_coupon).*").search(sm):
        return "Owned Channel"
    if _rx(r"(?i).*(kakao_chatbot).*").search(sm):
        return "Owned Channel"
    if _rx(r"(?i).*(kakao).*").search(sm):
        return "Paid Ad"

    if _rx(r"(?i).*(\(direct\) / \(none\)).*").search(sm):
        return "Organic Traffic"

    if _rx(r"(?i).*(signalplay|signal play|signal_play|sg_|signal|manplus).*").search(sm):
        return "Paid Ad"
    if _rx(r"(?i).*(buzzvill).*").search(sm):
        return "Paid Ad"
    if _rx(r"(?i).*(criteo).*").search(sm):
        return "Paid Ad"
    if _rx(r"(?i).*(mobon).*").search(sm):
        return "Paid Ad"
    if _rx(r"(?i).*(snow).*").search(sm):
        return "Paid Ad"
    if _rx(r"(?i).*(smr).*").search(sm):
        return "Paid Ad"
    if _rx(r"(?i).*(tg).*").search(sm):
        return "Paid Ad"
    if _rx(r"(?i).*(t_cafe).*").search(sm):
        return "Paid Ad"
    if _rx(r"(?i).*(blind).*").search(sm):
        return "Paid Ad"

    if _rx(r"(?i).*(cpc).*").search(sm):
        return "Paid Ad"
    if _rx(r"(?i).*(organic).*").search(sm):
        return "Organic Traffic"
    if _rx(r"(?i).*(banner|da).*").search(sm):
        return "Paid Ad"
    if _rx(r"(?i).*(referral).*").search(sm):
        return "Organic Traffic"
    if _rx(r"(?i).*(shopping).*").search(sm):
        return "Organic Traffic"
    if _rx(r"(?i).*(social).*").search(sm):
        return "Organic Traffic"

    return "etc"

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
    return base or "(not set)"


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
    Session metric is GA sessions by channel grouping.
    The Total row is forced from the authoritative GA overall sessions KPI.
    """
    dims = ["sessionSourceMedium", "sessionCampaignName", "sessionDefaultChannelGroup"]
    session_metric = "sessions"
    mets = [session_metric, "transactions", "purchaseRevenue"]

    def fetch(start: dt.date, end: dt.date) -> pd.DataFrame:
        try:
            df = run_report(client, PROPERTY_ID, ymd(start), ymd(end), dims, mets, limit=250000)
            if df.empty:
                return pd.DataFrame(columns=dims + mets)
            df = df.copy()
            df["sessionSourceMedium"] = df["sessionSourceMedium"].astype(str).fillna("")
            df["sessionCampaignName"] = df["sessionCampaignName"].astype(str).fillna("")
            df["sessionDefaultChannelGroup"] = df["sessionDefaultChannelGroup"].astype(str).fillna("")
            for c in mets:
                df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0.0)
            df["bucket"] = df.apply(lambda r: classify_looker_channel(r["sessionSourceMedium"], r["sessionCampaignName"], r["sessionDefaultChannelGroup"]), axis=1)
            return df
        except Exception:
            dims2 = ["sessionSourceMedium"]
            df = run_report(client, PROPERTY_ID, ymd(start), ymd(end), dims2, mets, limit=250000)
            if df.empty:
                return pd.DataFrame(columns=dims2 + mets + ["sessionCampaignName", "bucket"])
            df = df.copy()
            df["sessionSourceMedium"] = df["sessionSourceMedium"].astype(str).fillna("")
            df["sessionCampaignName"] = ""
            df["sessionDefaultChannelGroup"] = ""
            for c in mets:
                df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0.0)
            df["bucket"] = df.apply(lambda r: classify_looker_channel(r["sessionSourceMedium"], "", ""), axis=1)
            return df

    cur = fetch(w.cur_start, w.cur_end)
    prev = fetch(w.prev_start, w.prev_end)
    yoy = fetch(w.yoy_start, w.yoy_end)

    def agg(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=["bucket", f"sessions{suffix}", f"transactions{suffix}", f"revenue{suffix}"])
        g = df.groupby("bucket", as_index=False)[[session_metric, "transactions", "purchaseRevenue"]].sum()
        g = g.rename(columns={
            session_metric: f"sessions{suffix}",
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
    os_cur = float(oc.get("sessions", 0) or 0)
    os_prev = float(op.get("sessions", 0) or 0)
    os_yoy = float(oy.get("sessions", 0) or 0)

    canonical_buckets = list(CHANNEL_BUCKET_ORDER)
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
    m = m.reset_index()

    m["session_dod"] = m.apply(lambda r: pct_change(float(r["sessions_cur"]), float(r["sessions_prev"])), axis=1)
    m["session_yoy"] = m.apply(lambda r: pct_change(float(r["sessions_cur"]), float(r["sessions_yoy"])), axis=1)
    m["orders_dod"] = m.apply(lambda r: pct_change(float(r["transactions_cur"]), float(r["transactions_prev"])), axis=1)
    m["orders_yoy"] = m.apply(lambda r: pct_change(float(r["transactions_cur"]), float(r["transactions_yoy"])), axis=1)
    m["revenue_dod"] = m.apply(lambda r: pct_change(float(r["revenue_cur"]), float(r["revenue_prev"])), axis=1)
    m["revenue_yoy"] = m.apply(lambda r: pct_change(float(r["revenue_cur"]), float(r["revenue_yoy"])), axis=1)

    out = pd.DataFrame({
        "bucket": m["bucket"],
        "sessions": m["sessions_cur"],
        "transactions": m["transactions_cur"],
        "purchaseRevenue": m["revenue_cur"],
        "session_dod": m["session_dod"],
        "session_yoy": m["session_yoy"],
        "orders_dod": m["orders_dod"],
        "orders_yoy": m["orders_yoy"],
        "revenue_dod": m["revenue_dod"],
        "revenue_yoy": m["revenue_yoy"],
        "rev_dod": m["session_dod"],
        "rev_yoy": m["session_yoy"],
    })

    order = {b: i for i, b in enumerate(CHANNEL_BUCKET_ORDER)}
    out["__o"] = out["bucket"].map(order).fillna(99).astype(int)
    out = out.sort_values(["__o", "bucket"]).drop(columns="__o")

    total = pd.DataFrame([{
        "bucket": "Total",
        "sessions": os_cur,
        "transactions": float(out["transactions"].sum() if not out.empty else 0),
        "purchaseRevenue": float(out["purchaseRevenue"].sum() if not out.empty else 0),
        "session_dod": pct_change(os_cur, os_prev),
        "session_yoy": pct_change(os_cur, os_yoy),
        "orders_dod": pct_change(float(out["transactions"].sum() if not out.empty else 0), float(m["transactions_prev"].sum() if not m.empty else 0)),
        "orders_yoy": pct_change(float(out["transactions"].sum() if not out.empty else 0), float(m["transactions_yoy"].sum() if not m.empty else 0)),
        "revenue_dod": pct_change(float(out["purchaseRevenue"].sum() if not out.empty else 0), float(m["revenue_prev"].sum() if not m.empty else 0)),
        "revenue_yoy": pct_change(float(out["purchaseRevenue"].sum() if not out.empty else 0), float(m["revenue_yoy"].sum() if not m.empty else 0)),
        "rev_dod": pct_change(os_cur, os_prev),
        "rev_yoy": pct_change(os_cur, os_yoy),
    }])

    out = pd.concat([out, total], ignore_index=True)
    return out[[
        "bucket", "sessions", "transactions", "purchaseRevenue",
        "session_dod", "session_yoy", "orders_dod", "orders_yoy", "revenue_dod", "revenue_yoy",
        "rev_dod", "rev_yoy"
    ]]

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
    Provide switchable Session / Orders / Revenue deltas.
    """
    dims = ["sessionSourceMedium", "sessionCampaignName", "sessionDefaultChannelGroup"]
    session_metric = "sessions"
    mets = [session_metric, "transactions", "purchaseRevenue"]

    def fetch(start: dt.date, end: dt.date) -> pd.DataFrame:
        try:
            df = run_report(client, PROPERTY_ID, ymd(start), ymd(end), dims, mets, limit=250000)
            if df.empty:
                return pd.DataFrame(columns=dims + mets)
            df = df.copy()
            df["sessionSourceMedium"] = df["sessionSourceMedium"].astype(str).fillna("")
            df["sessionCampaignName"] = df["sessionCampaignName"].astype(str).fillna("")
            df["sessionDefaultChannelGroup"] = df["sessionDefaultChannelGroup"].astype(str).fillna("")
            for c in mets:
                df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0.0)
            df["bucket"] = df.apply(lambda r: classify_looker_channel(r["sessionSourceMedium"], r["sessionCampaignName"], r["sessionDefaultChannelGroup"]), axis=1)
            df = df[df["bucket"] == "Paid Ad"].copy()
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
            df["sessionDefaultChannelGroup"] = ""
            for c in mets:
                df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0.0)
            df["bucket"] = df.apply(lambda r: classify_looker_channel(r["sessionSourceMedium"], "", ""), axis=1)
            df = df[df["bucket"] == "Paid Ad"].copy()
            df["sub"] = df.apply(lambda r: classify_paid_detail(r["sessionSourceMedium"], ""), axis=1)
            return df

    cur = fetch(w.cur_start, w.cur_end)
    prev = fetch(w.prev_start, w.prev_end)
    yoy = fetch(w.yoy_start, w.yoy_end)

    def agg(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=["sub", session_metric, "transactions", "purchaseRevenue"])
        return df.groupby("sub", as_index=False)[[session_metric, "transactions", "purchaseRevenue"]].sum()

    cur_a = agg(cur).rename(columns={session_metric: "sessions_cur", "transactions": "orders_cur", "purchaseRevenue": "rev_cur"})
    prev_a = agg(prev).rename(columns={session_metric: "sessions_prev", "transactions": "orders_prev", "purchaseRevenue": "rev_prev"})
    yoy_a = agg(yoy).rename(columns={session_metric: "sessions_yoy", "transactions": "orders_yoy", "purchaseRevenue": "rev_yoy_base"})
    yoy_subs = set(yoy_a["sub"].astype(str).tolist()) if not yoy_a.empty else set()

    merged = cur_a.merge(prev_a, on="sub", how="outer").merge(yoy_a, on="sub", how="outer").fillna(0.0)
    merged["session_dod"] = merged.apply(lambda r: pct_change(float(r["sessions_cur"]), float(r["sessions_prev"])), axis=1)
    merged["session_yoy"] = merged.apply(lambda r: pct_change(float(r["sessions_cur"]), float(r["sessions_yoy"])), axis=1)
    merged["orders_dod"] = merged.apply(lambda r: pct_change(float(r["orders_cur"]), float(r["orders_prev"])), axis=1)
    merged["orders_yoy"] = merged.apply(lambda r: pct_change(float(r["orders_cur"]), float(r["orders_yoy"])), axis=1)
    merged["revenue_dod"] = merged.apply(lambda r: pct_change(float(r["rev_cur"]), float(r["rev_prev"])), axis=1)
    merged["revenue_yoy"] = merged.apply(lambda r: pct_change(float(r["rev_cur"]), float(r["rev_yoy_base"])), axis=1)
    merged["dod"] = merged["session_dod"]
    merged["yoy"] = merged["session_yoy"]
    merged["has_yoy"] = merged["sub"].astype(str).isin(yoy_subs)

    core = list(PAID_DETAIL_SOURCES)
    for c in core:
        if c not in set(merged["sub"].tolist()):
            merged = pd.concat([merged, pd.DataFrame([{
                "sub": c, "sessions_cur": 0.0, "orders_cur": 0.0, "rev_cur": 0.0,
                "sessions_prev": 0.0, "orders_prev": 0.0, "rev_prev": 0.0,
                "sessions_yoy": 0.0, "orders_yoy": 0.0, "rev_yoy_base": 0.0,
                "session_dod": 0.0, "session_yoy": 0.0,
                "orders_dod": 0.0, "orders_yoy": 0.0,
                "revenue_dod": 0.0, "revenue_yoy": 0.0,
                "dod": 0.0, "yoy": 0.0, "has_yoy": False
            }])], ignore_index=True)

    others = merged[~merged["sub"].isin(core)].copy()
    others = others.sort_values(["sessions_cur", "rev_cur"], ascending=[False, False])

    ordered = pd.concat([
        merged[merged["sub"].isin(core)].assign(_ord=lambda d: d["sub"].apply(lambda x: core.index(x))).sort_values("_ord"),
        others.assign(_ord=999),
    ], ignore_index=True)

    merged_cur_s = float(merged["sessions_cur"].sum()) if not merged.empty else 0.0
    merged_prev_s = float(merged["sessions_prev"].sum()) if not merged.empty else 0.0
    merged_yoy_s = float(merged["sessions_yoy"].sum()) if not merged.empty else 0.0
    merged_cur_o = float(merged["orders_cur"].sum()) if not merged.empty else 0.0
    merged_prev_o = float(merged["orders_prev"].sum()) if not merged.empty else 0.0
    merged_yoy_o = float(merged["orders_yoy"].sum()) if not merged.empty else 0.0
    merged_cur_r = float(merged["rev_cur"].sum()) if not merged.empty else 0.0
    merged_prev_r = float(merged["rev_prev"].sum()) if not merged.empty else 0.0
    merged_yoy_r = float(merged["rev_yoy_base"].sum()) if not merged.empty else 0.0

    if paid_ad_totals:
        t_cur_s = float(paid_ad_totals.get("current", {}).get("sessions", merged_cur_s) or 0.0)
        t_cur_r = float(paid_ad_totals.get("current", {}).get("revenue", merged_cur_r) or 0.0)
        t_prev_s = float(paid_ad_totals.get("prev", {}).get("sessions", merged_prev_s) or merged_prev_s)
        t_yoy_s = float(paid_ad_totals.get("yoy", {}).get("sessions", merged_yoy_s) or merged_yoy_s)
    else:
        t_cur_s, t_prev_s, t_yoy_s, t_cur_r = merged_cur_s, merged_prev_s, merged_yoy_s, merged_cur_r

    total_row = {
        "sub_channel": "Total",
        "sessions": t_cur_s,
        "orders": merged_cur_o,
        "purchaseRevenue": t_cur_r,
        "session_dod": pct_change(t_cur_s, t_prev_s),
        "session_yoy": pct_change(t_cur_s, t_yoy_s),
        "orders_dod": pct_change(merged_cur_o, merged_prev_o),
        "orders_yoy": pct_change(merged_cur_o, merged_yoy_o),
        "revenue_dod": pct_change(t_cur_r, merged_prev_r),
        "revenue_yoy": pct_change(t_cur_r, merged_yoy_r),
        "dod": pct_change(t_cur_s, t_prev_s),
        "yoy": pct_change(t_cur_s, t_yoy_s),
        "has_yoy": (t_yoy_s not in (None, 0, 0.0)),
    }

    out = ordered[["sub", "sessions_cur", "orders_cur", "rev_cur", "session_dod", "session_yoy", "orders_dod", "orders_yoy", "revenue_dod", "revenue_yoy", "dod", "yoy", "has_yoy"]].copy()
    out = out.rename(columns={
        "sub": "sub_channel",
        "sessions_cur": "sessions",
        "orders_cur": "orders",
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

    report_patch_css = """
  <style>
    :root{--report-max:none;--motion-ease:cubic-bezier(.2,.8,.2,1);}
    .report-body{background:linear-gradient(180deg,#f8fafc 0%,#eef2f7 100%);}
    .report-shell{width:100%;}
    .metric-switch{display:inline-flex;gap:6px;flex-wrap:wrap;}
    .metric-tab{border:1px solid rgba(148,163,184,.25);background:#fff;border-radius:999px;padding:8px 12px;font-size:11px;font-weight:900;color:#64748b;transition:all .22s var(--motion-ease);box-shadow:0 6px 18px rgba(15,23,42,.04)}
    .metric-tab:hover{transform:translateY(-1px);box-shadow:0 12px 28px rgba(15,23,42,.08)}
    .metric-tab.active{background:#0f172a;color:#fff;border-color:#0f172a;box-shadow:0 14px 32px rgba(15,23,42,.16)}
    .metric-label{display:none}.metric-label.active{display:inline}
    .metric-slot{display:none}.metric-slot.active{display:block;animation:metricSwap .42s var(--motion-ease)}
    .metric-inline .metric-slot.active{display:inline}
    .report-card,.bucket-detail-panel,.weather-card{animation:cardRise .7s var(--motion-ease) both;transform-origin:center bottom}
    .report-card:hover{transform:translateY(-4px);box-shadow:0 18px 40px rgba(15,23,42,.08)}
    .kpi-card{position:relative;overflow:hidden;transition:transform .24s var(--motion-ease), box-shadow .24s var(--motion-ease), border-color .24s var(--motion-ease)}
    .kpi-card:before{content:'';position:absolute;inset:-40% auto auto -20%;width:60%;height:180%;background:linear-gradient(90deg,transparent,rgba(255,255,255,.45),transparent);transform:rotate(14deg);animation:shineSweep 4.2s linear infinite;pointer-events:none}
    .kpi-card:hover{transform:translateY(-6px) scale(1.01);box-shadow:0 22px 44px rgba(15,23,42,.08);border-color:rgba(59,130,246,.22)}
    .kpi-value{animation:numberPop .8s var(--motion-ease) both}
    .channel-table-wrap,.paid-table-wrap{overflow-x:auto}
    .channel-table-wrap table,.paid-table-wrap table{min-width:980px}
    @keyframes cardRise{from{opacity:0;transform:translateY(26px) scale(.985)}to{opacity:1;transform:translateY(0) scale(1)}}
    @keyframes metricSwap{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}
    @keyframes numberPop{0%{opacity:.2;transform:translateY(12px) scale(.96)}60%{opacity:1;transform:translateY(-2px) scale(1.02)}100%{opacity:1;transform:translateY(0) scale(1)}}
    @keyframes shineSweep{0%{transform:translateX(-160%) rotate(14deg)}100%{transform:translateX(320%) rotate(14deg)}}
  </style>
    """

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
                    'ys': list(ys),
                    'trend_svg': spark_svg(xlabels, ys, width=260, height=70, stroke="#0f766e"),
                })

        pdp_series = {
            "x": xlabels,
            "rows": [
                {
                    "itemCategory": r["itemCategory"],
                    "views_d1": r["views_d1"],
                    "views_avg7d": r["views_avg7d"],
                    "ys": r["ys"]
                } for r in rows
            ]
        }

        return pd.DataFrame(rows)[["itemCategory", "views_d1", "views_avg7d", "trend_svg"]], pdp_series

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

def get_channel_detail_map_3way(client: BetaAnalyticsDataClient, w: DigestWindow) -> Dict[str, pd.DataFrame]:
    dims = ["sessionSourceMedium", "sessionCampaignName"]
    session_metric = "sessions"
    mets = [session_metric, "transactions", "purchaseRevenue"]
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
        return df
    cur = fetch(w.cur_start, w.cur_end)
    prev = fetch(w.prev_start, w.prev_end)
    yoy = fetch(w.yoy_start, w.yoy_end)
    def make_detail_key(df: pd.DataFrame, bucket: str) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=dims + mets + ["bucket", "sub"])
        out = df[df["bucket"] == bucket].copy()
        if out.empty:
            return out
        if bucket == "Paid Ad":
            out["sub"] = out.apply(lambda r: classify_paid_detail(str(r.get("sessionSourceMedium", "")), str(r.get("sessionCampaignName", ""))), axis=1)
        else:
            out["sub"] = out["sessionSourceMedium"].astype(str).str.strip().replace("", "(not set)")
        return out
    def agg(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=['sub', f'sessions{suffix}', f'transactions{suffix}', f'revenue{suffix}'])
        g = df.groupby('sub', as_index=False)[[session_metric,'transactions','purchaseRevenue']].sum()
        return g.rename(columns={session_metric:f'sessions{suffix}','transactions':f'transactions{suffix}','purchaseRevenue':f'revenue{suffix}'})
    out_map: Dict[str, pd.DataFrame] = {}
    for bucket in CHANNEL_BUCKET_ORDER:
        merged = agg(make_detail_key(cur, bucket),'_cur').merge(
            agg(make_detail_key(prev, bucket),'_prev'), on='sub', how='outer'
        ).merge(
            agg(make_detail_key(yoy, bucket),'_yoy'), on='sub', how='outer'
        ).fillna(0.0)
        if merged.empty:
            out_map[bucket] = pd.DataFrame(columns=['sub_channel','sessions','orders','purchaseRevenue','session_dod','session_yoy','orders_dod','orders_yoy','revenue_dod','revenue_yoy','dod','yoy'])
            continue
        merged['session_dod'] = merged.apply(lambda r: pct_change(float(r['sessions_cur']), float(r['sessions_prev'])), axis=1)
        merged['session_yoy'] = merged.apply(lambda r: pct_change(float(r['sessions_cur']), float(r['sessions_yoy'])), axis=1)
        merged['orders_dod'] = merged.apply(lambda r: pct_change(float(r['transactions_cur']), float(r['transactions_prev'])), axis=1)
        merged['orders_yoy'] = merged.apply(lambda r: pct_change(float(r['transactions_cur']), float(r['transactions_yoy'])), axis=1)
        merged['revenue_dod'] = merged.apply(lambda r: pct_change(float(r['revenue_cur']), float(r['revenue_prev'])), axis=1)
        merged['revenue_yoy'] = merged.apply(lambda r: pct_change(float(r['revenue_cur']), float(r['revenue_yoy'])), axis=1)
        merged['dod'] = merged['session_dod']
        merged['yoy'] = merged['session_yoy']
        detail = merged.rename(columns={'sub':'sub_channel','sessions_cur':'sessions','transactions_cur':'orders','revenue_cur':'purchaseRevenue'})[['sub_channel','sessions','orders','purchaseRevenue','session_dod','session_yoy','orders_dod','orders_yoy','revenue_dod','revenue_yoy','dod','yoy']]
        out_map[bucket] = detail.sort_values(['sessions','purchaseRevenue'], ascending=[False,False]).head(12).reset_index(drop=True)
    return out_map

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
        df = df[df['bucket'] == 'Paid Ad'].copy()
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
    channel_detail_map: Dict[str, pd.DataFrame],
    paid_media_compare: pd.DataFrame,
    weather_forecast: Optional[dict] = None,
    admin_overall: Optional[Dict[str, Dict[str, float]]] = None,
    brand_powerlink_status: Optional[List[dict]] = None,
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

    admin_overall = admin_overall or {"current": {}, "prev": {}, "yoy": {}}

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
        "admin_kpis": {
            "sessions": float((admin_overall.get("current", {}) or {}).get("sessions", 0) or 0),
            "orders": float((admin_overall.get("current", {}) or {}).get("orders", 0) or 0),
            "buyers": float((admin_overall.get("current", {}) or {}).get("buyers", 0) or 0),
            "revenue": float((admin_overall.get("current", {}) or {}).get("erp_revenue", (admin_overall.get("current", {}) or {}).get("revenue", 0)) or 0),
            "signups": float((admin_overall.get("current", {}) or {}).get("signups", 0) or 0),
            "aov": float((admin_overall.get("current", {}) or {}).get("aov", 0) or 0),
            "source": str((admin_overall.get("current", {}) or {}).get("source", "admin_bq_daily_erp")),
        },
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
        "admin_overall": admin_overall,
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
        "channel_detail_map": {k: to_records(v) for k, v in (channel_detail_map or {}).items()},
        "other_detail": to_records((channel_detail_map or {}).get("Other", pd.DataFrame())),
        "paid_media_compare": to_records(paid_media_compare),
        "weather_forecast": weather_forecast or {},
        "brand_powerlink_status": brand_powerlink_status or [],
    }

def rebuild_runtime_objects_from_bundle(bundle: dict, image_map: Dict[str, str]) -> dict:
    m = bundle.get("meta", {})
    mode = (m.get("mode") or "daily").lower()
    end_date = parse_yyyy_mm_dd(m.get("end_date", "")) or dt.date.today()
    w = build_window(end_date=end_date, mode=mode)

    overall = bundle.get("overall", {"current": {}, "prev": {}, "yoy": {}})
    admin_overall = bundle.get("admin_overall", {"current": {}, "prev": {}, "yoy": {}})
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
    items_map = {it.get("itemId"): (it.get("ys") or [0.0] * max(len(x), 7)) for it in bs_series.get("items", []) if it.get("itemId")}
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
        ys = row.get("ys") or [0.0] * max(len(pdp_series.get("x", [])), 7)
        pdp_rows.append({
            "itemCategory": row.get("itemCategory", ""),
            "views_d1": row.get("views_d1", 0.0),
            "views_avg7d": row.get("views_avg7d", 0.0),
            "trend_svg": spark_svg(pdp_series.get("x", ["--"]*7), ys, width=260, height=70, stroke="#0f766e")
        })
    category_pdp_trend = pd.DataFrame(pdp_rows)

    search_new = pd.DataFrame(bundle.get("search_new", []))
    search_rising = pd.DataFrame(bundle.get("search_rising", []))
    raw_channel_detail_map = bundle.get("channel_detail_map", {})
    channel_detail_map = {
        str(bucket): pd.DataFrame(rows or [])
        for bucket, rows in raw_channel_detail_map.items()
    } if isinstance(raw_channel_detail_map, dict) else {}
    if (not channel_detail_map) and bundle.get("other_detail") is not None:
        channel_detail_map = {"Other": pd.DataFrame(bundle.get("other_detail", []))}
    paid_media_compare = pd.DataFrame(bundle.get("paid_media_compare", []))
    weather_forecast = bundle.get("weather_forecast", {}) or {}
    brand_powerlink_status = bundle.get("brand_powerlink_status", []) or []

    return {
        "w": w,
        "overall": overall,
        "admin_overall": admin_overall,
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
        "channel_detail_map": channel_detail_map,
        "paid_media_compare": paid_media_compare,
        "weather_forecast": weather_forecast,
        "brand_powerlink_status": brand_powerlink_status,
    }


# =========================
# UI renderer
# =========================
def render_page_html(
    logo_b64: str,
    w: DigestWindow,
    overall: Dict[str, Dict[str, float]],
    admin_overall: Dict[str, Dict[str, float]],
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
    channel_detail_map: Dict[str, pd.DataFrame],
    paid_media_compare: pd.DataFrame,
    weather_forecast: Optional[dict],
    brand_powerlink_status: Optional[List[dict]],
    nav_links: Dict[str, str],
    bundle_rel_path: str,
) -> str:
    import html as _html
    import json as _json

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

    adm_cur = admin_overall.get("current", {}) or {}
    adm_prev = admin_overall.get("prev", {}) or {}
    adm_yoy = admin_overall.get("yoy", {}) or {}

    # Keep the ADMIN Sessions card aligned with the GA Sessions KPI as requested.
    adm_sessions_cur = float(cur.get("sessions", 0) or 0)
    adm_sessions_prev = float(prev.get("sessions", 0) or 0)
    adm_sessions_yoy = float(yoy.get("sessions", 0) or 0)
    adm_orders_cur = float(adm_cur.get("orders", 0) or 0)
    adm_orders_prev = float(adm_prev.get("orders", 0) or 0)
    adm_orders_yoy = float(adm_yoy.get("orders", 0) or 0)
    adm_buyers_cur = float(adm_cur.get("buyers", 0) or 0)
    adm_buyers_prev = float(adm_prev.get("buyers", 0) or 0)
    adm_buyers_yoy = float(adm_yoy.get("buyers", 0) or 0)
    adm_revenue_cur = float(adm_cur.get("erp_revenue", adm_cur.get("revenue", 0)) or 0)
    adm_revenue_prev = float(adm_prev.get("erp_revenue", adm_prev.get("revenue", 0)) or 0)
    adm_revenue_yoy = float(adm_yoy.get("erp_revenue", adm_yoy.get("revenue", 0)) or 0)
    adm_signups_cur = float(adm_cur.get("signups", 0) or 0)
    adm_signups_prev = float(adm_prev.get("signups", 0) or 0)
    adm_signups_yoy = float(adm_yoy.get("signups", 0) or 0)

    def esc(s: Any) -> str:
        return _html.escape(str(s or ""), quote=True)

    def delta_cls(v: float) -> str:
        return "text-blue-600" if v >= 0 else "text-orange-700"

    def top_kpi_card(title: str, value: str, delta_main: str, delta_yoy_s: str, cls_main: str, cls_yoy: str, source_label: str = "GA") -> str:
        return f"""
        <div class="report-card kpi-card rounded-2xl border border-slate-200 bg-white/70 p-4">
          <div class="flex items-center justify-between gap-2">
            <div class="text-[11px] font-extrabold tracking-widest text-slate-500 uppercase">{esc(title)}</div>
            <div class="rounded-full border border-slate-200 bg-slate-50 px-2 py-0.5 text-[10px] font-black tracking-wide text-slate-600 uppercase">{esc(source_label)}</div>
          </div>
          <div class="mt-1 text-xl font-black text-slate-900 kpi-value">{esc(value)}</div>
          <div class="mt-1 text-[11px] text-slate-500">{w.compare_label} <b class="{cls_main}">{esc(delta_main)}</b> · YoY <b class="{cls_yoy}">{esc(delta_yoy_s)}</b></div>
        </div>
        """

    def kpi_group_title(label: str, sublabel: str) -> str:
        return f"""
        <div class="mt-6 flex flex-wrap items-end justify-between gap-2">
          <div>
            <div class="text-xs font-extrabold tracking-[0.22em] text-slate-500 uppercase">{esc(label)}</div>
            <div class="mt-1 text-sm font-bold text-slate-500">{esc(sublabel)}</div>
          </div>
        </div>
        """


    def metric_value_slots(prefix: str, sessions_v: Any, orders_v: Any, revenue_v: Any) -> str:
        return (
            f"<span class='{prefix} metric-slot active' data-metric='sessions'>{fmt_int(sessions_v)}</span>"
            f"<span class='{prefix} metric-slot' data-metric='orders'>{fmt_int(orders_v)}</span>"
            f"<span class='{prefix} metric-slot' data-metric='revenue'>{fmt_currency_krw(revenue_v)}</span>"
        )

    def fixed_metric_value_cols(sessions_v: Any, orders_v: Any, revenue_v: Any) -> List[str]:
        return [
            f"<div class='text-right'>{fmt_int(sessions_v)}</div>",
            f"<div class='text-right'>{fmt_int(orders_v)}</div>",
            f"<div class='text-right'>{fmt_currency_krw(revenue_v)}</div>",
        ]

    def metric_delta_slots(prefix: str, session_v: float, order_v: float, revenue_v: float) -> str:
        def one(metric: str, v: float) -> str:
            return f"<span class='{prefix} metric-slot {'active' if metric == 'sessions' else ''} {delta_cls(v)}' data-metric='{metric}'>{'+' if float(v or 0) >= 0 else ''}{fmt_pct(float(v or 0),1)}</span>"
        return one('sessions', session_v) + one('orders', order_v) + one('revenue', revenue_v)

    def metric_tabs_html(section: str) -> str:
        return (
            f"<div class='metric-switch' data-metric-switch='{section}'>"
            f"<button type='button' class='metric-tab active' data-metric-tab='sessions'>Sessions</button>"
            f"<button type='button' class='metric-tab' data-metric-tab='orders'>Orders</button>"
            f"<button type='button' class='metric-tab' data-metric-tab='revenue'>Revenue</button>"
            f"</div>"
        )

    def product_img(url: str) -> str:
        u = (url or PLACEHOLDER_IMG or "").strip()
        if u:
            return f"<img src='{esc(u)}' class='w-8 h-8 rounded-xl object-cover border border-slate-200'/>"
        return "<div class='w-8 h-8 rounded-xl bg-slate-100 border border-slate-200'></div>"

    # Slightly tighter padding to protect the last columns from clipping.
    def table_row(cols: List[str], bold=False, row_class: str = "", row_attrs: str = "") -> str:
        fw = "font-extrabold" if bold else "font-medium"
        bg = "bg-slate-50" if bold else ""
        tds = ""
        for i, c in enumerate(cols):
            extra = " pr-4" if i == (len(cols) - 1) else ""
            tds += f"<td class='px-2 py-2 border-b border-slate-100 whitespace-nowrap {fw}{extra}'>{c}</td>"
        return f"<tr class='{bg} {row_class}' {row_attrs}>{tds}</tr>"

    # Channel Snapshot rows
    chan_html = ""
    detail_buckets = {
        str(bucket)
        for bucket, df in (channel_detail_map or {}).items()
        if isinstance(df, pd.DataFrame)
    }
    if channel_snapshot is not None and (not channel_snapshot.empty):
        for r in channel_snapshot.itertuples(index=False):
            bucket = str(getattr(r, "bucket", "") or "")
            bucket_html = esc(bucket)
            row_class = ""
            row_attrs = ""
            if bucket != "Total" and bucket in detail_buckets:
                bucket_html = f"<div class='flex items-center gap-2'><span>{esc(bucket)}</span><span class='rounded-full bg-slate-900/5 px-2 py-0.5 text-[10px] font-extrabold tracking-wide text-slate-500'>DETAIL</span></div>"
                row_class = "bucket-summary-row cursor-pointer hover:bg-slate-50"
                row_attrs = f"data-bucket='{esc(bucket)}'"
            chan_html += table_row([
                bucket_html,
                *fixed_metric_value_cols(getattr(r, 'sessions', 0), getattr(r, 'transactions', 0), getattr(r, 'purchaseRevenue', 0)),
                f"<div class='text-right metric-inline'>{metric_delta_slots('cs-wow', float(getattr(r, 'session_dod', getattr(r, 'rev_dod', 0)) or 0), float(getattr(r, 'orders_dod', 0) or 0), float(getattr(r, 'revenue_dod', 0) or 0))}</div>",
                f"<div class='text-right metric-inline'>{metric_delta_slots('cs-yoy', float(getattr(r, 'session_yoy', getattr(r, 'rev_yoy', 0)) or 0), float(getattr(r, 'orders_yoy', 0) or 0), float(getattr(r, 'revenue_yoy', 0) or 0))}</div>",
            ], bold=(bucket == "Total"), row_class=row_class, row_attrs=row_attrs)

    def build_bucket_detail_rows(df: pd.DataFrame) -> str:
        rows = ""
        if df is None or df.empty:
            return "<tr><td colspan='6' class='px-2 py-6 text-center text-slate-400'>No data</td></tr>"
        for r in df.itertuples(index=False):
            rows += table_row([
                esc(getattr(r, "sub_channel", "")),
                *fixed_metric_value_cols(getattr(r, 'sessions', 0), getattr(r, 'orders', 0), getattr(r, 'purchaseRevenue', 0)),
                f"<div class='text-right metric-inline'>{metric_delta_slots('bd-wow', float(getattr(r, 'session_dod', getattr(r, 'dod', 0)) or 0), float(getattr(r, 'orders_dod', 0) or 0), float(getattr(r, 'revenue_dod', 0) or 0))}</div>",
                f"<div class='text-right metric-inline'>{metric_delta_slots('bd-yoy', float(getattr(r, 'session_yoy', getattr(r, 'yoy', 0)) or 0), float(getattr(r, 'orders_yoy', 0) or 0), float(getattr(r, 'revenue_yoy', 0) or 0))}</div>",
            ])
        return rows

    channel_detail_payload: Dict[str, Dict[str, str]] = {}
    summary_lookup = {}
    if channel_snapshot is not None and (not channel_snapshot.empty):
        for row in channel_snapshot.itertuples(index=False):
            summary_lookup[str(getattr(row, "bucket", "") or "")] = row
    for bucket in sorted(detail_buckets):
        df = channel_detail_map.get(bucket, pd.DataFrame())
        actual_sessions = float(df["sessions"].sum()) if df is not None and (not df.empty) and ("sessions" in df.columns) else 0.0
        actual_revenue = float(df["purchaseRevenue"].sum()) if df is not None and (not df.empty) and ("purchaseRevenue" in df.columns) else 0.0
        diag_bits = [
            f"<span class='rounded-full bg-slate-900/5 px-2 py-1 font-extrabold text-slate-600'>Visible Detail {fmt_int(actual_sessions)} users / {fmt_currency_krw(actual_revenue)}</span>"
        ]
        summary_row = summary_lookup.get(bucket)
        if summary_row is not None:
            summary_sessions = float(getattr(summary_row, "sessions", 0) or 0)
            summary_revenue = float(getattr(summary_row, "purchaseRevenue", 0) or 0)
            diag_bits.append(
                f"<span class='rounded-full bg-slate-100 px-2 py-1 font-extrabold text-slate-600'>Snapshot Total {fmt_int(summary_sessions)} users / {fmt_currency_krw(summary_revenue)}</span>"
            )
            if bucket == "Other":
                diag_bits.append(
                    f"<span class='rounded-full bg-orange-50 px-2 py-1 font-extrabold text-orange-700'>Residual {fmt_int(max(summary_sessions - actual_sessions, 0.0))} users / {fmt_currency_krw(max(summary_revenue - actual_revenue, 0.0))}</span>"
                )
        channel_detail_payload[bucket] = {
            "title": f"{bucket} Detail",
            "description": f"Breakdown by source / medium for {bucket}.",
            "diag_html": f"<div class='mt-2 flex flex-wrap items-center gap-2 text-xs'>{''.join(diag_bits)}</div>",
            "sessions_rows": build_bucket_detail_rows(df),
            "revenue_rows": build_bucket_detail_rows(
                df.sort_values(["purchaseRevenue", "sessions"], ascending=[False, False]).reset_index(drop=True)
                if df is not None and (not df.empty)
                else pd.DataFrame()
            ),
        }
    channel_detail_payload_json = _json.dumps(channel_detail_payload, ensure_ascii=False).replace("</", "<\\/")
    bucket_detail_panel_html = f"""
    <div id="bucketDetailHost" class="mt-6 hidden">
      <div id="bucketDetailSection" class="bucket-detail-panel rounded-2xl border border-slate-200 bg-white/85 p-4">
        <div class="flex flex-wrap items-center justify-between gap-3">
          <div>
            <div id="bucketDetailTitle" class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Channel Detail</div>
            <div id="bucketDetailDesc" class="mt-1 text-sm text-slate-500">Breakdown by source / medium.</div>
            <div id="bucketDetailDiag"></div>
          </div>
          <button id="bucketDetailClose" type="button" class="rounded-xl border border-slate-200 bg-white px-3 py-2 text-xs font-extrabold text-slate-600 hover:bg-slate-50">Close</button>
        </div>
        <div class="mt-4 flex flex-wrap items-center justify-between gap-3">
          <div class="flex flex-wrap items-center gap-2">
            <button type="button" data-bucket-tab="sessions" class="bucket-tab-btn active rounded-full border border-slate-900 bg-slate-900 px-3 py-1 text-xs font-extrabold text-white">Sort by Sessions</button>
            <button type="button" data-bucket-tab="revenue" class="bucket-tab-btn rounded-full border border-slate-200 bg-white px-3 py-1 text-xs font-extrabold text-slate-500">Sort by Revenue</button>
          </div>
          {metric_tabs_html('bucket-detail')}
        </div>
        <div data-bucket-panel="sessions" class="mt-4 overflow-x-auto">
          <table class="w-full table-auto text-sm min-w-[1120px]">
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
            <tbody id="bucketDetailSessionsBody"></tbody>
          </table>
        </div>
        <div data-bucket-panel="revenue" class="mt-4 hidden overflow-x-auto">
          <table class="w-full table-auto text-sm min-w-[1120px]">
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
            <tbody id="bucketDetailRevenueBody"></tbody>
          </table>
        </div>
      </div>
      <script id="bucketDetailPayload" type="application/json">{channel_detail_payload_json}</script>
    </div>
    """

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
                *fixed_metric_value_cols(getattr(r, 'sessions', 0), getattr(r, 'orders', 0), getattr(r, 'purchaseRevenue', 0)),
                f"<div class='text-right metric-inline'>{metric_delta_slots('pd-wow', float(getattr(r, 'session_dod', getattr(r, 'dod', 0)) or 0), float(getattr(r, 'orders_dod', 0) or 0), float(getattr(r, 'revenue_dod', 0) or 0))}</div>",
                f"<div class='text-right metric-inline'>{metric_delta_slots('pd-yoy', float(getattr(r, 'session_yoy', getattr(r, 'yoy', 0)) or 0), float(getattr(r, 'orders_yoy', 0) or 0), float(getattr(r, 'revenue_yoy', 0) or 0)) if (is_total or has_yoy) else FALLBACK_METRIC_HTML}</div>",
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


    weather = weather_forecast or {}
    weather_days = weather.get("days") or []
    weather_cards = ""
    for day in weather_days[:7]:
        min_t = day.get("min_temp")
        max_t = day.get("max_temp")
        pop = day.get("pop")
        t_label = "-"
        if min_t is not None and max_t is not None:
            t_label = f"{int(round(float(min_t)))}° / {int(round(float(max_t)))}°"
        elif max_t is not None:
            t_label = f"~ {int(round(float(max_t)))}°"
        elif min_t is not None:
            t_label = f"{int(round(float(min_t)))}° ~"
        pop_label = f"POP {int(round(float(pop)))}%" if pop is not None else "POP -"
        weather_cards += f"""
        <div class="rounded-2xl border border-slate-200 bg-white px-3 py-3 text-center shadow-sm">
          <div class="text-[11px] font-extrabold tracking-widest text-slate-500 uppercase">{esc(day.get('label',''))}</div>
          <div class="mt-2 text-2xl leading-none">{esc(day.get('weather_emoji','🌤️'))}</div>
          <div class="mt-2 text-sm font-black text-slate-900">{esc(day.get('weather','-'))}</div>
          <div class="mt-1 text-xs font-bold text-slate-600">{esc(t_label)}</div>
          <div class="mt-1 text-[11px] text-slate-400">{esc(pop_label)}</div>
        </div>
        """
    weather_meta = ""
    if weather.get("status") == "ok":
        weather_meta = f"<div class='text-xs text-slate-400'>Source: {esc(weather.get('source','KMA API Hub'))} · Updated {esc(weather.get('generated_at',''))}</div>"
    elif weather.get("error"):
        weather_meta = f"<div class='text-xs text-amber-600'>{esc(weather.get('error','Weather API unavailable'))}</div>"
    weather_section_html = ""
    if weather_days:
        weather_section_html = f"""
        <div class="mt-6 rounded-[28px] border border-slate-200 bg-gradient-to-br from-sky-50 via-white to-indigo-50 p-5 shadow-sm">
          <div class="flex flex-wrap items-end justify-between gap-3">
            <div>
              <div class="text-xs font-extrabold tracking-[0.22em] text-sky-700 uppercase">Next Week Forecast</div>
              <div class="mt-1 text-xl font-black text-slate-900">{esc(weather.get('location','Seoul'))} · Upcoming 7 Days</div>
              <div class="mt-1 text-sm text-slate-500">Top summary card for weekly weather planning</div>
            </div>
            {weather_meta}
          </div>
          <div class="mt-4 grid grid-cols-2 gap-3 md:grid-cols-4 xl:grid-cols-7">
            {weather_cards}
          </div>
        </div>
        """

    ga_kpis_cards = "".join([
        top_kpi_card("Sessions", fmt_int(cur["sessions"]),
                     f"{'+' if s_delta>=0 else ''}{fmt_pct(s_delta,1)}",
                     f"{'+' if s_yoy>=0 else ''}{fmt_pct(s_yoy,1)}",
                     delta_cls(s_delta), delta_cls(s_yoy), "GA"),
        top_kpi_card("Revenue", fmt_currency_krw(cur["purchaseRevenue"]),
                     f"{'+' if r_delta>=0 else ''}{fmt_pct(r_delta,1)}",
                     f"{'+' if r_yoy>=0 else ''}{fmt_pct(r_yoy,1)}",
                     delta_cls(r_delta), delta_cls(r_yoy), "GA"),
        top_kpi_card("Orders", fmt_int(cur["transactions"]),
                     f"{'+' if o_delta>=0 else ''}{fmt_pct(o_delta,1)}",
                     f"{'+' if o_yoy>=0 else ''}{fmt_pct(o_yoy,1)}",
                     delta_cls(o_delta), delta_cls(o_yoy), "GA"),
        top_kpi_card("CVR", f"{cur['cvr']*100:.2f}%",
                     f"{'+' if c_pp>=0 else ''}{fmt_pp(c_pp,2)}",
                     f"{'+' if c_yoy_pp>=0 else ''}{fmt_pp(c_yoy_pp,2)}",
                     delta_cls(c_pp), delta_cls(c_yoy_pp), "GA"),
        top_kpi_card("Sign-up Users", fmt_int(su_cur),
                     f"{'+' if su_delta>=0 else ''}{fmt_pct(su_delta,1)}",
                     f"{'+' if su_yoy_delta>=0 else ''}{fmt_pct(su_yoy_delta,1)}",
                     delta_cls(su_delta), delta_cls(su_yoy_delta), "GA"),
    ])

    adm_sessions_delta = pct_change(adm_sessions_cur, adm_sessions_prev)
    adm_sessions_yoy_delta = pct_change(adm_sessions_cur, adm_sessions_yoy)
    adm_orders_delta = pct_change(adm_orders_cur, adm_orders_prev)
    adm_orders_yoy_delta = pct_change(adm_orders_cur, adm_orders_yoy)
    adm_buyers_delta = pct_change(adm_buyers_cur, adm_buyers_prev)
    adm_buyers_yoy_delta = pct_change(adm_buyers_cur, adm_buyers_yoy)
    adm_revenue_delta = pct_change(adm_revenue_cur, adm_revenue_prev)
    adm_revenue_yoy_delta = pct_change(adm_revenue_cur, adm_revenue_yoy)
    adm_signups_delta = pct_change(adm_signups_cur, adm_signups_prev)
    adm_signups_yoy_delta = pct_change(adm_signups_cur, adm_signups_yoy)

    admin_kpis_cards = "".join([
        top_kpi_card("Sessions", fmt_int(adm_sessions_cur),
                     f"{'+' if adm_sessions_delta>=0 else ''}{fmt_pct(adm_sessions_delta,1)}",
                     f"{'+' if adm_sessions_yoy_delta>=0 else ''}{fmt_pct(adm_sessions_yoy_delta,1)}",
                     delta_cls(adm_sessions_delta), delta_cls(adm_sessions_yoy_delta), "ADMIN"),
        top_kpi_card("Revenue", fmt_currency_krw(adm_revenue_cur),
                     f"{'+' if adm_revenue_delta>=0 else ''}{fmt_pct(adm_revenue_delta,1)}",
                     f"{'+' if adm_revenue_yoy_delta>=0 else ''}{fmt_pct(adm_revenue_yoy_delta,1)}",
                     delta_cls(adm_revenue_delta), delta_cls(adm_revenue_yoy_delta), "ERP"),
        top_kpi_card("Orders", fmt_int(adm_orders_cur),
                     f"{'+' if adm_orders_delta>=0 else ''}{fmt_pct(adm_orders_delta,1)}",
                     f"{'+' if adm_orders_yoy_delta>=0 else ''}{fmt_pct(adm_orders_yoy_delta,1)}",
                     delta_cls(adm_orders_delta), delta_cls(adm_orders_yoy_delta), "ADMIN"),
        top_kpi_card("Buyers", fmt_int(adm_buyers_cur),
                     f"{'+' if adm_buyers_delta>=0 else ''}{fmt_pct(adm_buyers_delta,1)}",
                     f"{'+' if adm_buyers_yoy_delta>=0 else ''}{fmt_pct(adm_buyers_yoy_delta,1)}",
                     delta_cls(adm_buyers_delta), delta_cls(adm_buyers_yoy_delta), "ADMIN"),
        top_kpi_card("Sign-up Users", fmt_int(adm_signups_cur),
                     f"{'+' if adm_signups_delta>=0 else ''}{fmt_pct(adm_signups_delta,1)}",
                     f"{'+' if adm_signups_yoy_delta>=0 else ''}{fmt_pct(adm_signups_yoy_delta,1)}",
                     delta_cls(adm_signups_delta), delta_cls(adm_signups_yoy_delta), "ADMIN"),
    ])

    kpis_cards_html = (
        kpi_group_title("GA KPI", "GA4 기준")
        + f'<div class="mt-3 grid grid-cols-1 gap-3 md:grid-cols-5">{ga_kpis_cards}</div>'
        + kpi_group_title("ADMIN KPI", "Sessions=GA 기준 · Revenue=ERP 기준 · member_funnel_admin_daily")
        + f'<div class="mt-3 grid grid-cols-1 gap-3 md:grid-cols-5">{admin_kpis_cards}</div>'
    )

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


    metric_switch_js = """<script>
(function(){
  function initMetricSection(section, wowClass, yoyClass){
    const root = document.querySelector(`[data-metric-switch="${section}"]`);
    if(!root) return;
    let active = 'sessions';
    function sync(metric){
      active = metric || 'sessions';
      root.querySelectorAll('[data-metric-tab]').forEach(btn=>{
        btn.classList.toggle('active', btn.getAttribute('data-metric-tab') === active);
      });
      document.querySelectorAll(`.${wowClass}, .${yoyClass}`).forEach(el=>{
        el.classList.toggle('active', el.getAttribute('data-metric') === active);
      });
    }
    root.querySelectorAll('[data-metric-tab]').forEach(btn=>{
      btn.addEventListener('click', ()=> sync(btn.getAttribute('data-metric-tab') || 'sessions'));
    });
    sync(active);
  }
  initMetricSection('channel-snapshot','cs-wow','cs-yoy');
  initMetricSection('bucket-detail','bd-wow','bd-yoy');
  initMetricSection('paid-detail','pd-wow','pd-yoy');
})();
</script>"""
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

  const detailRows = Array.from(document.querySelectorAll('.bucket-summary-row'));
  const bucketDetailHost = document.getElementById('bucketDetailHost');
  const bucketDetailSection = document.getElementById('bucketDetailSection');
  const bucketDetailClose = document.getElementById('bucketDetailClose');
  const bucketDetailTitle = document.getElementById('bucketDetailTitle');
  const bucketDetailDesc = document.getElementById('bucketDetailDesc');
  const bucketDetailDiag = document.getElementById('bucketDetailDiag');
  const bucketDetailSessionsBody = document.getElementById('bucketDetailSessionsBody');
  const bucketDetailRevenueBody = document.getElementById('bucketDetailRevenueBody');
  const bucketDetailPayloadEl = document.getElementById('bucketDetailPayload');
  const bucketDetailPayload = bucketDetailPayloadEl ? JSON.parse(bucketDetailPayloadEl.textContent || '{}') : {};
  const bucketTabBtns = Array.from(document.querySelectorAll('[data-bucket-tab]'));
  const bucketPanels = Array.from(document.querySelectorAll('[data-bucket-panel]'));
  let activeBucket = '';
  function setBucketTab(target){
    bucketTabBtns.forEach(el=>{
      const active = el.getAttribute('data-bucket-tab') === target;
      el.classList.toggle('active', active);
      el.classList.toggle('border-slate-900', active);
      el.classList.toggle('bg-slate-900', active);
      el.classList.toggle('text-white', active);
      el.classList.toggle('border-slate-200', !active);
      el.classList.toggle('bg-white', !active);
      el.classList.toggle('text-slate-500', !active);
    });
    bucketPanels.forEach(el=>{
      const active = el.getAttribute('data-bucket-panel') === target;
      el.classList.toggle('hidden', !active);
    });
  }
  function renderBucketDetail(bucket){
    const payload = bucketDetailPayload[bucket];
    if(!payload) return false;
    if(bucketDetailTitle) bucketDetailTitle.textContent = payload.title || 'Channel Detail';
    if(bucketDetailDesc) bucketDetailDesc.textContent = payload.description || '';
    if(bucketDetailDiag) bucketDetailDiag.innerHTML = payload.diag_html || '';
    if(bucketDetailSessionsBody) bucketDetailSessionsBody.innerHTML = payload.sessions_rows || '';
    if(bucketDetailRevenueBody) bucketDetailRevenueBody.innerHTML = payload.revenue_rows || '';
    return true;
  }
  function markActiveRow(bucket){
    detailRows.forEach(el=>{
      const active = el.getAttribute('data-bucket') === bucket;
      el.classList.toggle('active', active);
    });
  }
  function openBucketDetail(bucket){
    if(!bucketDetailHost || !bucketDetailSection) return;
    if(!renderBucketDetail(bucket)) return;
    activeBucket = bucket;
    markActiveRow(bucket);
    bucketDetailHost.classList.remove('hidden');
    setBucketTab('sessions');
    requestAnimationFrame(()=>{
      bucketDetailSection.classList.add('open');
      setTimeout(()=> bucketDetailSection.scrollIntoView({ behavior:'smooth', block:'nearest' }), 120);
    });
  }
  function closeBucketDetail(){
    if(!bucketDetailHost || !bucketDetailSection) return;
    activeBucket = '';
    markActiveRow('');
    bucketDetailSection.classList.remove('open');
    setTimeout(()=> bucketDetailHost.classList.add('hidden'), 260);
  }
  detailRows.forEach(el=>{
    el.addEventListener('click', ()=>{
      const bucket = el.getAttribute('data-bucket') || '';
      if(!bucket) return;
      if(activeBucket === bucket && bucketDetailHost && !bucketDetailHost.classList.contains('hidden')){
        closeBucketDetail();
      } else {
        openBucketDetail(bucket);
      }
    });
  });
  if(bucketDetailClose) bucketDetailClose.addEventListener('click', closeBucketDetail);
  bucketTabBtns.forEach(el=>{
    el.addEventListener('click', ()=> setBucketTab(el.getAttribute('data-bucket-tab')));
  });
  const defaultBucket = bucketDetailPayload['Paid Ad'] ? 'Paid Ad' : (Object.keys(bucketDetailPayload)[0] || '');
  if(defaultBucket){
    if(bucketDetailHost) bucketDetailHost.classList.remove('hidden');
    openBucketDetail(defaultBucket);
  }
})();
</script>"""


    brand_powerlink_rows = []
    for item in (brand_powerlink_status or []):
        brand = esc(item.get("brand", ""))
        status = esc(item.get("status_label", item.get("status", "")))
        status_cls = "bg-emerald-50 text-emerald-700 border-emerald-200"
        if str(item.get("status", "")) == "error":
            status_cls = "bg-rose-50 text-rose-700 border-rose-200"
        elif str(item.get("status", "")) == "empty":
            status_cls = "bg-amber-50 text-amber-700 border-amber-200"
        highlights = item.get("keyword_highlights", []) or []
        if not highlights:
            highlights = item.get("tags", []) or item.get("cards", []) or []
        chips = "".join([
            f"<span class='inline-flex items-center rounded-full border border-sky-200 bg-sky-50 px-2 py-1 text-[11px] font-bold text-sky-700'>{esc(v)}</span>"
            for v in highlights[:6]
        ]) or "<span class='text-xs text-slate-400'>No keywords</span>"
        copy_lines = [x for x in [item.get("headline", ""), item.get("main_copy", ""), item.get("sub_copy", "")] if str(x).strip()]
        copy_block = "".join([f"<div>{esc(x)}</div>" for x in copy_lines]) if copy_lines else '<span class="text-slate-400">대표 문구 미검출</span>'
        cards = item.get("cards", []) or []
        card_labels = "".join([
            f"<span class='inline-flex items-center rounded-full border border-slate-200 bg-white px-2 py-1 text-[11px] font-semibold text-slate-600'>{esc(v)}</span>"
            for v in cards[:4]
        ])
        capture_path = str(item.get("capture_path", "") or "").strip()
        capture_html = f"<div class='mt-3 overflow-hidden rounded-2xl border border-slate-200 bg-slate-50'><img src='{esc(capture_path)}' alt='{brand} powerlink capture' class='block h-auto w-full'/></div>" if capture_path else ""
        brand_powerlink_rows.append(f"""
        <div class="rounded-2xl border border-slate-200 bg-white/80 p-4 shadow-sm">
          <div class="flex items-start justify-between gap-3">
            <div>
              <div class="text-sm font-black text-slate-900">{brand}</div>
              <div class="mt-1 text-[11px] text-slate-500">{esc(item.get('collected_at', ''))}</div>
            </div>
            <div class="rounded-full border px-2.5 py-1 text-[11px] font-extrabold {status_cls}">{status}</div>
          </div>
          {capture_html}
          <div class="mt-3 space-y-1 text-sm font-semibold text-slate-700">{copy_block}</div>
          <div class="mt-3 flex flex-wrap gap-2">{chips}</div>
          <div class="mt-3 flex flex-wrap gap-2">{card_labels}</div>
        </div>
        """)
    brand_powerlink_html = ""
    if brand_powerlink_rows:
        brand_powerlink_html = f"""
        <div class="report-card mt-6 rounded-2xl border border-slate-200 bg-white/70 p-4">
          <div class="flex flex-wrap items-center justify-between gap-3">
            <div>
              <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">브랜드별 파워링크 현황</div>
              <div class="mt-1 text-sm text-slate-500">주요 문구, 태그, 카테고리 키워드 기준</div>
            </div>
            <div class="rounded-full border border-slate-200 bg-white px-3 py-1 text-[11px] font-black text-slate-600">{len(brand_powerlink_rows)} brands</div>
          </div>
          <div class="mt-4 grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3">{''.join(brand_powerlink_rows)}</div>
        </div>
        """

    html = f"""<!doctype html>
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
  {REPORT_PATCH_CSS}
</head>
<body class="bg-slate-50 text-slate-900 report-body">
  <div class="w-full max-w-none px-5 py-6 xl:px-8 2xl:px-10 report-shell">
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

    {weather_section_html}

    {kpis_cards_html}

    <div class="report-card mt-6 rounded-2xl border border-slate-200 bg-white/70 p-4">
      <div class="flex flex-wrap items-center justify-between gap-3">
        <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Channel Snapshot</div>
        {metric_tabs_html('channel-snapshot')}
      </div>
      <div class="channel-table-wrap mt-3">
      <table class="w-full table-auto text-sm">
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
    </div>
    {bucket_detail_panel_html}

    <div class="report-card mt-6 rounded-2xl border border-slate-200 bg-white/70 p-4">
      <div class="flex flex-wrap items-center justify-between gap-3">
        <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Paid Detail</div>
        {metric_tabs_html('paid-detail')}
      </div>
      <div class="paid-table-wrap mt-3 overflow-x-auto">
        <table class="w-full table-auto text-sm min-w-[1120px]">
          <thead class="text-xs text-slate-500">
            <tr>
              <th class="px-2 py-2 text-left whitespace-nowrap">Sub</th>
              <th class="px-2 py-2 text-right whitespace-nowrap">Sessions</th>
              <th class="px-2 py-2 text-right whitespace-nowrap">Orders</th>
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

    {brand_powerlink_html}

    <div class="report-card mt-6 rounded-2xl border border-slate-200 bg-white/70 p-4">
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

    <div class="report-card mt-6 rounded-2xl border border-slate-200 bg-white/70 p-4">
      <div class="flex flex-wrap items-center justify-between gap-3">
        <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Trend (Index)</div>
        <div class="flex flex-wrap items-center gap-2">{trend_tabs_html}</div>
      </div>
      {trend_panels_html}
    </div>

    <div class="mt-6 grid grid-cols-1 gap-4 lg:grid-cols-2">
      <div class="report-card rounded-2xl border border-slate-200 bg-white/70 p-4">
        <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Best Sellers (Top 5)</div>
        <div class="mt-3 space-y-2">{bs_rows or "<div class='text-sm text-slate-500'>No data</div>"}</div>
      </div>

      <div class="report-card rounded-2xl border border-slate-200 bg-white/70 p-4">
        <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Rising Products (Top 5)</div>
        <div class="mt-3 space-y-2">{rising_rows or "<div class='text-sm text-slate-500'>No data</div>"}</div>
      </div>
    </div>

    <div class="mt-6 rounded-2xl border border-slate-200 bg-white/70 p-4">
      <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">PDP View Trend (Category)</div>
      <div class="mt-3 space-y-2">{pdp_rows}</div>
    </div>

    <div class="mt-6 grid grid-cols-1 gap-4 lg:grid-cols-2">
      <div class="report-card rounded-2xl border border-slate-200 bg-white/70 p-4">
        <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Search · New</div>
        <div class="mt-3 space-y-2">{new_terms_html or "<div class='text-sm text-slate-500'>No data</div>"}</div>
      </div>
      <div class="report-card rounded-2xl border border-slate-200 bg-white/70 p-4">
        <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Search · Rising</div>
        <div class="mt-3 space-y-2">{rising_terms_html or "<div class='text-sm text-slate-500'>No data</div>"}</div>
      </div>
    </div>

  </div>

  {metric_switch_js}

  {paid_toggle_js}

</body>
</html>
"""
    return inject_report_toolbar(html, w)


def inject_report_toolbar(html: str, w: WindowSpec) -> str:
    toolbar_css = """
    .toolbar-card{background:rgba(255,255,255,0.88);border:1px solid rgba(226,232,240,0.95);border-radius:20px;padding:16px;box-shadow:0 10px 30px rgba(15,23,42,0.05)}
    .toolbar-row{display:flex;flex-wrap:wrap;gap:10px;align-items:center}
    .toolbar-label{font-size:11px;font-weight:900;letter-spacing:.18em;text-transform:uppercase;color:#94a3b8}
    .toolbar-chip{border:1px solid rgba(148,163,184,0.28);background:#fff;border-radius:999px;padding:9px 12px;font-size:12px;font-weight:900;color:#0f172a}
    .toolbar-chip.active{background:#0f172a;color:#fff;border-color:#0f172a}
    .toolbar-btn{border:1px solid rgba(148,163,184,0.28);background:#fff;border-radius:14px;padding:10px 12px;font-size:12px;font-weight:900;color:#0f172a}
    .toolbar-btn.primary{background:#002d72;color:#fff;border-color:#002d72}
    .toolbar-input{border:1px solid rgba(148,163,184,0.28);background:#fff;border-radius:14px;padding:10px 12px;font-size:12px;font-weight:900;color:#0f172a;min-width:160px}
    .compare-frame{width:100%;border:0;border-radius:18px;min-height:2600px;background:transparent}
    .compare-grid{display:grid;grid-template-columns:minmax(0,1fr);gap:16px}
    @media (min-width:1280px){.compare-grid.dual{grid-template-columns:minmax(0,1fr) minmax(0,1fr)}}
    .bucket-summary-row{transition:transform .22s ease, background-color .22s ease, box-shadow .22s ease}
    .bucket-summary-row:hover{transform:translateX(4px)}
    .bucket-summary-row.active{background:rgba(15,23,42,.04)}
    .bucket-detail-panel{overflow:hidden;max-height:0;opacity:0;transform:translateY(22px) scale(.985);padding-top:0;padding-bottom:0;transition:max-height .65s cubic-bezier(.2,.8,.2,1),opacity .45s ease,transform .45s ease,padding .45s ease}
    .bucket-detail-panel.open{max-height:2200px;opacity:1;transform:translateY(0) scale(1);padding-top:16px;padding-bottom:16px}
    .bucket-tab-btn{transition:transform .22s ease,box-shadow .22s ease,background .22s ease,color .22s ease,border-color .22s ease}
    .bucket-tab-btn:hover{transform:translateY(-2px);box-shadow:0 12px 24px rgba(15,23,42,0.08)}
    """
    toolbar_html = f"""
    <div id="reportToolbar" class="toolbar-card">
      <div class="toolbar-row">
        <div class="toolbar-label">Mode</div>
        <button id="modeDaily" class="toolbar-chip{' active' if w.mode == 'daily' else ''}" type="button">Daily</button>
        <button id="modeWeekly" class="toolbar-chip{' active' if w.mode == 'weekly' else ''}" type="button">Weekly (7D)</button>
        <div class="toolbar-label">Date</div>
        <input id="aDate" class="toolbar-input" type="date" value="{ymd(w.end_date)}" />
        <button id="btnPrev" class="toolbar-btn" type="button">이전</button>
        <button id="btnNext" class="toolbar-btn" type="button">다음</button>
        <button id="btnYesterday" class="toolbar-btn" type="button">어제</button>
        <button id="btnToday" class="toolbar-btn" type="button">오늘(있으면)</button>
      </div>
      <div class="toolbar-row" style="margin-top:12px;">
        <div class="toolbar-label">Compare</div>
        <button id="btnCompareToggle" class="toolbar-chip" type="button">비교 OFF</button>
        <button id="btnPresetPrev" class="toolbar-btn" type="button">전기준</button>
        <button id="btnPresetYoY" class="toolbar-btn" type="button">YoY</button>
        <input id="bDate" class="toolbar-input" type="date" value="{ymd(w.prev_end)}" />
        <button id="btnCompareGo" class="toolbar-btn primary" type="button">비교하기</button>
      </div>
    </div>
    <div id="compareGrid" class="compare-grid" style="margin-top:16px;">
      <div id="primaryReport">
    """
    compare_tail = """
      </div>
      <div id="compareReportWrap" class="hidden">
        <div class="mb-2 text-xs font-extrabold tracking-widest text-slate-500 uppercase">Compare Report</div>
        <iframe id="compareFrame" class="compare-frame" loading="eager" scrolling="no"></iframe>
      </div>
    </div>
    """
    toolbar_script = f"""
  <script>
  (() => {{
    const EMBED = new URLSearchParams(window.location.search).get("embed") === "1";
    const CURRENT_MODE = {json.dumps(w.mode)};
    const CURRENT_DATE = {json.dumps(ymd(w.end_date))};
    const toolbar = document.getElementById("reportToolbar");
    const compareGrid = document.getElementById("compareGrid");
    const compareWrap = document.getElementById("compareReportWrap");
    const compareFrame = document.getElementById("compareFrame");
    const modeDaily = document.getElementById("modeDaily");
    const modeWeekly = document.getElementById("modeWeekly");
    const aDate = document.getElementById("aDate");
    const bDate = document.getElementById("bDate");
    const btnPrev = document.getElementById("btnPrev");
    const btnNext = document.getElementById("btnNext");
    const btnYesterday = document.getElementById("btnYesterday");
    const btnToday = document.getElementById("btnToday");
    const btnCompareToggle = document.getElementById("btnCompareToggle");
    const btnPresetPrev = document.getElementById("btnPresetPrev");
    const btnPresetYoY = document.getElementById("btnPresetYoY");
    const btnCompareGo = document.getElementById("btnCompareGo");
    let mode = CURRENT_MODE;
    let compareOn = false;

    function fmtYMD(d) {{
      const y = d.getFullYear();
      const m = String(d.getMonth() + 1).padStart(2, "0");
      const dd = String(d.getDate()).padStart(2, "0");
      return `${{y}}-${{m}}-${{dd}}`;
    }}
    function parseYMD(s) {{
      const [y, m, d] = String(s || "").split("-").map(Number);
      return new Date(y, (m || 1) - 1, d || 1);
    }}
    function addDays(d, n) {{
      const x = new Date(d);
      x.setDate(x.getDate() + n);
      return x;
    }}
    function kstNowDate() {{
      const now = new Date();
      const utc = now.getTime() + now.getTimezoneOffset() * 60000;
      return new Date(utc + 9 * 60 * 60000);
    }}
    function stepDays() {{ return mode === "weekly" ? 7 : 1; }}
    function buildReportUrl(nextMode, dateStr, embed) {{
      const base = nextMode === "daily" ? `../daily/${{dateStr}}.html` : `../weekly/END_${{dateStr}}.html`;
      return embed ? `${{base}}?embed=1` : base;
    }}
    function buildCachePath(nextMode, dateStr) {{
      return nextMode === "daily" ? `../data/daily/${{dateStr}}.json` : `../data/weekly/END_${{dateStr}}.json`;
    }}
    async function yoyEndFor(dateStr) {{
      try {{
        const res = await fetch(buildCachePath(mode, dateStr) + `?t=${{Date.now()}}`, {{ cache: "no-store" }});
        if (!res.ok) throw new Error("no cache");
        const j = await res.json();
        const y = j && j.yoy && j.yoy.end ? String(j.yoy.end) : "";
        if (!y) throw new Error("no yoy.end");
        return y;
      }} catch (e) {{
        return fmtYMD(addDays(parseYMD(dateStr), -364));
      }}
    }}
    function resizeCompareFrame() {{
      try {{
        const doc = compareFrame.contentDocument || compareFrame.contentWindow.document;
        if (!doc) return;
        const h = Math.max(doc.body ? doc.body.scrollHeight : 0, doc.documentElement ? doc.documentElement.scrollHeight : 0);
        if (h > 0) compareFrame.style.height = `${{h + 12}}px`;
      }} catch (e) {{}}
    }}
    function setMode(nextMode) {{
      mode = nextMode;
      modeDaily.classList.toggle("active", mode === "daily");
      modeWeekly.classList.toggle("active", mode === "weekly");
    }}
    function setCompare(on) {{
      compareOn = !!on;
      btnCompareToggle.textContent = compareOn ? "비교 ON" : "비교 OFF";
      btnCompareToggle.classList.toggle("active", compareOn);
      compareWrap.classList.toggle("hidden", !compareOn);
      compareGrid.classList.toggle("dual", compareOn);
    }}
    function goCurrent(dateStr) {{
      window.location.href = buildReportUrl(mode, dateStr, false);
    }}
    function openCompare() {{
      const dateStr = String(bDate.value || "").trim();
      if (!dateStr) return;
      setCompare(true);
      compareFrame.src = buildReportUrl(mode, dateStr, true);
    }}

    if (EMBED) {{
      if (toolbar) toolbar.remove();
      return;
    }}

    compareFrame && compareFrame.addEventListener("load", () => {{
      resizeCompareFrame();
      setTimeout(resizeCompareFrame, 300);
      setTimeout(resizeCompareFrame, 900);
    }});

    setMode(CURRENT_MODE);
    aDate.value = CURRENT_DATE;
    bDate.value = {json.dumps(ymd(w.prev_end))};
    setCompare(false);

    modeDaily && modeDaily.addEventListener("click", () => {{
      setMode("daily");
      goCurrent(aDate.value || CURRENT_DATE);
    }});
    modeWeekly && modeWeekly.addEventListener("click", () => {{
      setMode("weekly");
      goCurrent(aDate.value || CURRENT_DATE);
    }});
    btnPrev && btnPrev.addEventListener("click", () => goCurrent(fmtYMD(addDays(parseYMD(aDate.value || CURRENT_DATE), -stepDays()))));
    btnNext && btnNext.addEventListener("click", () => goCurrent(fmtYMD(addDays(parseYMD(aDate.value || CURRENT_DATE), stepDays()))));
    btnYesterday && btnYesterday.addEventListener("click", () => goCurrent(fmtYMD(addDays(kstNowDate(), -1))));
    btnToday && btnToday.addEventListener("click", () => goCurrent(fmtYMD(kstNowDate())));
    aDate && aDate.addEventListener("change", () => {{
      if (aDate.value) goCurrent(aDate.value);
    }});
    btnCompareToggle && btnCompareToggle.addEventListener("click", () => setCompare(!compareOn));
    btnPresetPrev && btnPresetPrev.addEventListener("click", () => {{
      bDate.value = fmtYMD(addDays(parseYMD(aDate.value || CURRENT_DATE), -stepDays()));
      openCompare();
    }});
    btnPresetYoY && btnPresetYoY.addEventListener("click", async () => {{
      bDate.value = await yoyEndFor(aDate.value || CURRENT_DATE);
      openCompare();
    }});
    btnCompareGo && btnCompareGo.addEventListener("click", openCompare);
  }})();
  </script>
    """

    html = html.replace("</style>", toolbar_css + "\n  </style>", 1)
    html = html.replace('<div class="w-full max-w-none px-5 py-6 xl:px-8 2xl:px-10 report-shell">', '<div class="w-full max-w-none px-5 py-6 xl:px-8 2xl:px-10 report-shell">' + toolbar_html, 1)
    html = re.sub(
        r'\s*<div class="flex items-center gap-2">\s*<a href="[^"]*" class="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm font-extrabold hover:bg-slate-50">Hub</a>\s*</div>',
        '',
        html,
        count=1,
    )
    html = html.replace("\n  </div>\n\n  <script>", "\n" + compare_tail + "\n  </div>\n\n  <script>", 1)
    html = html.replace("</body>", toolbar_script + "\n</body>", 1)
    return html


# =========================
# Hub page
# =========================
def render_hub_index(dates: List[dt.date]) -> str:
    return render_hub_index_compare(dates)

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
      <div class="report-card rounded-2xl border border-slate-200 bg-white/70 p-4">
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


def render_hub_index_compare(dates: List[dt.date]) -> str:
    dates = sorted(dates)
    if not dates:
        dates = [dt.datetime.now(ZoneInfo("Asia/Seoul")).date() - dt.timedelta(days=1)]
    latest = dates[-1]
    known_dates = [d.strftime("%Y-%m-%d") for d in dates]

    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Daily Digest Hub</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200;400;600;800&display=swap');
    :root {{ --brand:#002d72; --bg0:#f6f8fb; --bg1:#eef3f9; }}

    html, body {{ height: 100%; overflow: auto; }}
    body {{
      background: linear-gradient(180deg, var(--bg0), var(--bg1));
      font-family: 'Plus Jakarta Sans', 'Noto Sans KR', 'Malgun Gothic', sans-serif;
      color:#0f172a;
      min-height:100vh;
    }}
    .glass {{
      background: rgba(255,255,255,0.72);
      backdrop-filter: blur(18px);
      border: 1px solid rgba(255,255,255,0.85);
      border-radius: 26px;
      box-shadow: 0 24px 60px rgba(0,45,114,0.07);
    }}
    .chip {{
      border: 1px solid rgba(148,163,184,0.35);
      background: rgba(255,255,255,0.78);
      border-radius: 999px;
      padding: 8px 12px;
      font-weight: 900;
      font-size: 12px;
      color: #0f172a;
      transition: all .15s ease;
      user-select:none;
      white-space: nowrap;
    }}
    .chip:hover {{ transform: translateY(-1px); box-shadow: 0 10px 24px rgba(0,45,114,0.08); }}
    .chip.active {{
      background: rgba(0,45,114,0.08);
      border-color: rgba(0,45,114,0.28);
      color: var(--brand);
    }}
    .btn {{
      border-radius: 14px;
      padding: 10px 14px;
      font-weight: 900;
      font-size: 12px;
      border: 1px solid rgba(148,163,184,0.28);
      background: rgba(255,255,255,0.88);
      transition: all .15s ease;
      user-select:none;
      white-space: nowrap;
      display:inline-flex;
      align-items:center;
      gap:8px;
    }}
    .btn:hover {{ transform: translateY(-1px); box-shadow: 0 10px 24px rgba(0,45,114,0.08); }}
    .btn-primary {{ background: #002d72; border-color: #002d72; color: white; }}
    .btn:disabled, .chip:disabled {{
      opacity: .55;
      cursor: not-allowed;
      transform:none !important;
      box-shadow:none !important;
    }}
    .muted {{ color:#64748b; }}
    .small-label {{
      font-size: 10px;
      font-weight: 900;
      letter-spacing: .22em;
      text-transform: uppercase;
      color: #94a3b8;
    }}
    input[type="date"] {{
      border: 1px solid rgba(148,163,184,0.28);
      background: rgba(255,255,255,0.90);
      border-radius: 14px;
      padding: 10px 12px;
      font-weight: 900;
      font-size: 12px;
      color: #0f172a;
      outline: none;
      width: 100%;
    }}
    input[type="date"]:focus {{
      border-color: rgba(0,45,114,0.40);
      box-shadow: 0 0 0 4px rgba(0,45,114,0.08);
    }}
    .viewer-frame {{
      width: 100%;
      border: 0;
      border-radius: 18px;
      background: transparent;
      overflow: hidden;
      display:block;
      height: 3200px;
    }}
    .loading-backdrop {{
      position: fixed;
      inset: 0;
      background: rgba(15, 23, 42, 0.18);
      backdrop-filter: blur(2px);
      display: none;
      align-items: center;
      justify-content: center;
      z-index: 9999;
    }}
    .loading-card {{
      background: rgba(255,255,255,0.92);
      border: 1px solid rgba(148,163,184,0.22);
      border-radius: 18px;
      padding: 14px 16px;
      box-shadow: 0 18px 48px rgba(0,0,0,0.12);
      display:flex;
      align-items:center;
      gap:10px;
      font-weight:900;
      color:#0f172a;
    }}
    .spinner {{
      width: 18px;
      height: 18px;
      border-radius: 999px;
      border: 3px solid rgba(2,45,114,0.18);
      border-top-color: rgba(2,45,114,0.95);
      animation: spin 0.8s linear infinite;
    }}
    @keyframes spin {{ to {{ transform: rotate(360deg); }} }}
    .btn .btn-spin {{
      width: 14px;
      height: 14px;
      border-radius: 999px;
      border: 2px solid rgba(255,255,255,0.45);
      border-top-color: rgba(255,255,255,0.95);
      animation: spin 0.8s linear infinite;
      display:none;
    }}
    .btn.loading .btn-spin {{ display:inline-block; }}
    .toolbar-row {{
      overflow-x:auto;
      -webkit-overflow-scrolling: touch;
      padding-bottom: 2px;
    }}
    .toolbar-row::-webkit-scrollbar {{ height: 8px; }}
    .toolbar-row::-webkit-scrollbar-thumb {{ background: rgba(148,163,184,0.35); border-radius: 999px; }}
  </style>
</head>

<body class="p-6 md:p-10">
  <div class="max-w-7xl mx-auto">
    <div class="mb-6 flex flex-col gap-4 md:flex-row md:items-end md:justify-between">
      <div>
        <div class="text-4xl font-black tracking-tight">Daily Digest Hub</div>
        <div class="mt-1 font-semibold muted">날짜 선택, Compare, YoY 비교를 허브에서 바로 확인할 수 있습니다.</div>
      </div>

      <div class="flex flex-wrap items-center justify-end gap-3">
        <div class="flex items-center gap-2">
          <div class="mr-2 small-label">Mode</div>
          <button id="modeDaily" class="chip active" type="button">Daily</button>
          <button id="modeWeekly" class="chip" type="button">Weekly (7D)</button>
        </div>
        <button id="btnReload" class="btn btn-primary" type="button"><span class="btn-spin"></span>새로고침</button>
      </div>
    </div>

    <div class="glass p-5">
      <div class="toolbar-row mb-3 flex items-center gap-2 whitespace-nowrap">
        <div class="small-label">Date</div>
        <div class="w-[160px]"><input id="aDate" type="date" /></div>
        <button id="btnPrev" class="btn" type="button">이전</button>
        <button id="btnNext" class="btn" type="button">다음</button>
        <button id="btnYesterday" class="btn" type="button">어제</button>
        <button id="btnToday" class="btn" type="button">오늘(있으면)</button>

        <div class="ml-2 small-label">Compare</div>
        <button id="btnCompareToggle" class="chip" type="button">비교 OFF</button>
        <button id="btnPresetPrev" class="btn" type="button">전기준</button>
        <button id="btnPresetYoY" class="btn" type="button">YoY</button>
        <div class="w-[160px]"><input id="bDate" type="date" /></div>
        <button id="btnCompareGo" class="btn btn-primary" type="button"><span class="btn-spin"></span>비교하기</button>
      </div>

      <div id="viewerGrid" class="grid grid-cols-1 gap-4">
        <div>
          <div class="mb-2 text-xs font-semibold muted" id="viewerATitle">A: -</div>
          <iframe id="viewerA" class="viewer-frame" loading="eager" scrolling="no"></iframe>
        </div>

        <div id="viewerBWrap" class="hidden">
          <div class="mb-2 text-xs font-semibold muted" id="viewerBTitle">B: -</div>
          <iframe id="viewerB" class="viewer-frame" loading="eager" scrolling="no"></iframe>
        </div>
      </div>
    </div>
  </div>

  <div id="loading" class="loading-backdrop">
    <div class="loading-card">
      <div class="spinner"></div>
      <div id="loadingText">로딩 중...</div>
    </div>
  </div>

<script>
(() => {{
  const BASE = "";
  const KNOWN_DATES = {json.dumps(known_dates)};
  const LATEST = "{latest.strftime('%Y-%m-%d')}";

  function kstNowDate() {{
    const now = new Date();
    const utc = now.getTime() + now.getTimezoneOffset() * 60000;
    return new Date(utc + 9 * 60 * 60000);
  }}
  function fmtYMD(d) {{
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, "0");
    const dd = String(d.getDate()).padStart(2, "0");
    return `${{y}}-${{m}}-${{dd}}`;
  }}
  function parseYMD(s) {{
    const [y, m, d] = String(s || "").split("-").map(Number);
    return new Date(y, (m || 1) - 1, d || 1);
  }}
  function addDays(d, n) {{
    const x = new Date(d);
    x.setDate(x.getDate() + n);
    return x;
  }}

  const modeDaily = document.getElementById("modeDaily");
  const modeWeekly = document.getElementById("modeWeekly");
  const btnReload = document.getElementById("btnReload");
  const aDate = document.getElementById("aDate");
  const bDate = document.getElementById("bDate");
  const btnPrev = document.getElementById("btnPrev");
  const btnNext = document.getElementById("btnNext");
  const btnYesterday = document.getElementById("btnYesterday");
  const btnToday = document.getElementById("btnToday");
  const btnCompareToggle = document.getElementById("btnCompareToggle");
  const btnPresetPrev = document.getElementById("btnPresetPrev");
  const btnPresetYoY = document.getElementById("btnPresetYoY");
  const btnCompareGo = document.getElementById("btnCompareGo");
  const viewerA = document.getElementById("viewerA");
  const viewerB = document.getElementById("viewerB");
  const viewerBWrap = document.getElementById("viewerBWrap");
  const viewerGrid = document.getElementById("viewerGrid");
  const viewerATitle = document.getElementById("viewerATitle");
  const viewerBTitle = document.getElementById("viewerBTitle");
  const loading = document.getElementById("loading");
  const loadingText = document.getElementById("loadingText");

  let MODE = "daily";
  let COMPARE = false;

  function stepDays() {{ return MODE === "weekly" ? 7 : 1; }}
  function buildReportPath(dateStr) {{
    const base = MODE === "daily" ? `${{BASE}}daily/${{dateStr}}.html` : `${{BASE}}weekly/END_${{dateStr}}.html`;
    return base + "?embed=1";
  }}
  function buildReportBase(dateStr) {{
    return MODE === "daily" ? `${{BASE}}daily/${{dateStr}}.html` : `${{BASE}}weekly/END_${{dateStr}}.html`;
  }}
  function buildCachePath(dateStr) {{
    return MODE === "daily" ? `${{BASE}}data/daily/${{dateStr}}.json` : `${{BASE}}data/weekly/END_${{dateStr}}.json`;
  }}

  const ACTION_BTNS = [btnReload, btnCompareGo];
  const MODE_BTNS = [modeDaily, modeWeekly, btnCompareToggle, btnPresetPrev, btnPresetYoY, btnPrev, btnNext, btnYesterday, btnToday];

  function setBusy(on, msg) {{
    if (on) {{
      loadingText.textContent = msg || "로딩 중...";
      loading.style.display = "flex";
      for (const b of ACTION_BTNS) {{ b.disabled = true; b.classList.add("loading"); }}
      for (const b of MODE_BTNS) {{ b.disabled = true; }}
    }} else {{
      loading.style.display = "none";
      for (const b of ACTION_BTNS) {{ b.disabled = false; b.classList.remove("loading"); }}
      for (const b of MODE_BTNS) {{ b.disabled = false; }}
    }}
  }}

  const LS_KEY = "ddhub_exists_cache_v10";
  const TTL_MS = 6 * 60 * 60 * 1000;
  const RECENT_DAYS_NO_NEG_CACHE = 3;
  const memCache = new Map();

  function loadLS() {{ try {{ return JSON.parse(localStorage.getItem(LS_KEY) || "{{}}") || {{}}; }} catch (e) {{ return {{}}; }} }}
  function saveLS(obj) {{ try {{ localStorage.setItem(LS_KEY, JSON.stringify(obj)); }} catch (e) {{}} }}

  function isRecentDateStr(dateStr) {{
    try {{
      const d = parseYMD(dateStr);
      const now = kstNowDate();
      const diffDays = Math.floor((now - d) / (24 * 60 * 60 * 1000));
      return diffDays >= 0 && diffDays <= RECENT_DAYS_NO_NEG_CACHE;
    }} catch (e) {{
      return false;
    }}
  }}

  function cacheGet(u) {{
    if (memCache.has(u)) return memCache.get(u);
    const db = loadLS();
    const hit = db[u];
    if (hit && (Date.now() - hit.ts) < TTL_MS) {{
      memCache.set(u, hit.ok);
      return hit.ok;
    }}
    return null;
  }}
  function cacheSet(u, ok) {{
    memCache.set(u, ok);
    const db = loadLS();
    db[u] = {{ ok: !!ok, ts: Date.now() }};
    const keys = Object.keys(db);
    if (keys.length > 900) {{
      keys.sort((a, b) => (db[a].ts || 0) - (db[b].ts || 0));
      for (let i = 0; i < keys.length - 700; i += 1) delete db[keys[i]];
    }}
    saveLS(db);
  }}

  async function existsReport(dateStr) {{
    const base = buildReportBase(dateStr);
    const cached = cacheGet(base);
    if (cached !== null) {{
      if (!(cached === false && isRecentDateStr(dateStr))) return cached;
    }}
    try {{
      const res = await fetch(base + `?t=${{Date.now()}}`, {{ method: "HEAD", cache: "no-store" }});
      cacheSet(base, res.ok);
      return res.ok;
    }} catch (e) {{
      cacheSet(base, false);
      return false;
    }}
  }}

  function resizeFrameToContent(frame) {{
    try {{
      const doc = frame.contentDocument || frame.contentWindow.document;
      if (!doc) return;
      let style = doc.getElementById("hub_scrub_css");
      if (!style) {{
        style = doc.createElement("style");
        style.id = "hub_scrub_css";
        style.type = "text/css";
        style.textContent = "html, body { overflow: visible !important; height: auto !important; } body { margin:0 !important; min-height:auto !important; }";
        doc.head.appendChild(style);
      }}
      const h = Math.max(doc.body ? doc.body.scrollHeight : 0, doc.documentElement ? doc.documentElement.scrollHeight : 0);
      if (h && Number.isFinite(h)) frame.style.height = `${{h + 12}}px`;
    }} catch (e) {{}}
  }}

  function onFrameLoad(frame) {{
    resizeFrameToContent(frame);
    let n = 0;
    const t = setInterval(() => {{
      resizeFrameToContent(frame);
      n += 1;
      if (n >= 10) clearInterval(t);
    }}, 250);
  }}

  viewerA.addEventListener("load", () => {{ onFrameLoad(viewerA); setBusy(false); }});
  viewerB.addEventListener("load", () => {{ onFrameLoad(viewerB); setBusy(false); }});

  async function loadA(dateStr) {{
    viewerA.src = buildReportPath(dateStr);
    viewerATitle.textContent = `A: ${{MODE.toUpperCase()}} · ${{dateStr}}`;
  }}
  async function loadB(dateStr) {{
    viewerB.src = buildReportPath(dateStr);
    viewerBTitle.textContent = `B: ${{MODE.toUpperCase()}} · ${{dateStr}}`;
  }}

  async function yoyEndFor(dateStr) {{
    try {{
      const res = await fetch(buildCachePath(dateStr) + `?t=${{Date.now()}}`, {{ cache: "no-store" }});
      if (!res.ok) throw new Error("no cache");
      const j = await res.json();
      const y = j && j.yoy && j.yoy.end ? String(j.yoy.end) : "";
      if (!y) throw new Error("no yoy.end");
      return y;
    }} catch (e) {{
      return fmtYMD(addDays(parseYMD(dateStr), -364));
    }}
  }}

  function updateCompareLayout() {{
    if (COMPARE) {{
      viewerBWrap.classList.remove("hidden");
      viewerGrid.className = "grid grid-cols-1 gap-4 lg:grid-cols-2";
    }} else {{
      viewerBWrap.classList.add("hidden");
      viewerGrid.className = "grid grid-cols-1 gap-4";
    }}
  }}

  function setCompare(on) {{
    COMPARE = !!on;
    btnCompareToggle.classList.toggle("active", COMPARE);
    btnCompareToggle.textContent = COMPARE ? "비교 ON" : "비교 OFF";
    updateCompareLayout();
  }}

  function nearestKnownDate(preferredDateStr) {{
    if (!KNOWN_DATES.length) return preferredDateStr;
    if (KNOWN_DATES.includes(preferredDateStr)) return preferredDateStr;
    for (let i = KNOWN_DATES.length - 1; i >= 0; i -= 1) {{
      if (KNOWN_DATES[i] <= preferredDateStr) return KNOWN_DATES[i];
    }}
    return KNOWN_DATES[KNOWN_DATES.length - 1];
  }}

  async function resolveLatestAvailableDate(preferredDateStr, maxScanDays) {{
    const known = nearestKnownDate(preferredDateStr);
    if (await existsReport(known)) return known;
    let d0 = parseYMD(preferredDateStr);
    for (let i = 0; i <= maxScanDays; i += 1) {{
      const cand = fmtYMD(addDays(d0, -i));
      if (await existsReport(cand)) return cand;
    }}
    return known || preferredDateStr;
  }}

  async function applyA() {{
    const d = String(aDate.value || "").trim();
    if (!d) return;
    setBusy(true, "A 리포트 로딩 중...");
    let target = d;
    if (!(await existsReport(target))) {{
      const step = stepDays();
      const tries = [-step, +step, -1, +1, -2, +2];
      for (const delta of tries) {{
        const cand = fmtYMD(addDays(parseYMD(target), delta));
        if (await existsReport(cand)) {{
          target = cand;
          break;
        }}
      }}
      aDate.value = target;
    }}
    await loadA(target);
  }}

  async function doCompare() {{
    const a = String(aDate.value || "").trim();
    const b = String(bDate.value || "").trim();
    if (!a || !b) return;
    setCompare(true);
    setBusy(true, "A/B 리포트 로딩 중...");
    await applyA();
    let targetB = b;
    if (!(await existsReport(targetB))) {{
      targetB = aDate.value;
      bDate.value = targetB;
    }}
    await loadB(targetB);
  }}

  async function setMode(next) {{
    MODE = next;
    modeDaily.classList.toggle("active", MODE === "daily");
    modeWeekly.classList.toggle("active", MODE === "weekly");
    setBusy(true, "모드 변경 중...");
    const baseY = LATEST || fmtYMD(addDays(kstNowDate(), -1));
    const latest = await resolveLatestAvailableDate(baseY, 30);
    aDate.value = latest;
    bDate.value = fmtYMD(addDays(parseYMD(latest), -stepDays()));
    await applyA();
    if (COMPARE) await doCompare();
  }}

  modeDaily.addEventListener("click", () => setMode("daily"));
  modeWeekly.addEventListener("click", () => setMode("weekly"));
  btnReload.addEventListener("click", () => {{
    setBusy(true, "새로고침 중...");
    try {{
      localStorage.removeItem(LS_KEY);
      memCache.clear();
    }} catch (e) {{}}
    location.reload();
  }});
  btnCompareToggle.addEventListener("click", () => setCompare(!COMPARE));
  btnPresetPrev.addEventListener("click", () => {{
    const a = aDate.value;
    if (!a) return;
    bDate.value = fmtYMD(addDays(parseYMD(a), -stepDays()));
    setCompare(true);
  }});
  btnPresetYoY.addEventListener("click", async () => {{
    const a = aDate.value;
    if (!a) return;
    setBusy(true, "YoY 날짜 계산 중...");
    bDate.value = await yoyEndFor(a);
    setBusy(false);
    setCompare(true);
  }});
  btnCompareGo.addEventListener("click", doCompare);
  btnYesterday.addEventListener("click", async () => {{
    const baseY = LATEST || fmtYMD(addDays(kstNowDate(), -1));
    setBusy(true, "최신 리포트 탐색 중...");
    aDate.value = await resolveLatestAvailableDate(baseY, 30);
    setBusy(false);
    applyA();
  }});
  btnToday.addEventListener("click", async () => {{
    setBusy(true, "오늘 리포트 확인 중...");
    const t = fmtYMD(kstNowDate());
    aDate.value = (await existsReport(t)) ? t : await resolveLatestAvailableDate(LATEST || fmtYMD(addDays(kstNowDate(), -1)), 30);
    setBusy(false);
    applyA();
  }});
  btnPrev.addEventListener("click", () => {{
    const cur = aDate.value;
    if (!cur) return;
    aDate.value = fmtYMD(addDays(parseYMD(cur), -stepDays()));
    applyA();
  }});
  btnNext.addEventListener("click", () => {{
    const cur = aDate.value;
    if (!cur) return;
    aDate.value = fmtYMD(addDays(parseYMD(cur), +stepDays()));
    applyA();
  }});

  (async function init() {{
    setCompare(false);
    const baseY = LATEST || fmtYMD(addDays(kstNowDate(), -1));
    setBusy(true, "최신 리포트 탐색 중...");
    const latest = await resolveLatestAvailableDate(baseY, 30);
    setBusy(false);
    aDate.value = latest;
    bDate.value = fmtYMD(addDays(parseYMD(latest), -1));
    await setMode("daily");
  }})();
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
                admin_overall=rt.get("admin_overall", {"current": {}, "prev": {}, "yoy": {}}),
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
                channel_detail_map=rt["channel_detail_map"],
                paid_media_compare=rt["paid_media_compare"],
                weather_forecast=rt.get("weather_forecast", {}),
                brand_powerlink_status=rt.get("brand_powerlink_status", []),
                nav_links={"hub": "../index.html", "daily_index": "../index.html", "weekly_index": "../index.html"},
                bundle_rel_path=bundle_rel,
            )
            return html, cached

    overall = get_overall_kpis(client, w)
    signup_users = get_multi_event_users_3way(client, w, ["signup_complete", "signup"])
    # Channel Snapshot: GA sessions by channel bucket, with Total forced from overall GA sessions.
    channel_snapshot = get_channel_snapshot_3way(client, w, overall=overall)

    # Extract Paid AD totals from Channel Snapshot for Paid Detail alignment.
    paid_ad_totals = {
        "current": {"sessions": None, "revenue": None},
        "prev": {"sessions": None, "revenue": None},
        "yoy": {"sessions": None, "revenue": None},
    }
    try:
        # Only current-period absolute values are needed to force Paid Detail Total.
        row = channel_snapshot[channel_snapshot["bucket"] == "Paid Ad"]
        if not row.empty:
            paid_ad_totals["current"]["sessions"] = float(row.iloc[0]["sessions"])
            paid_ad_totals["current"]["revenue"] = float(row.iloc[0]["purchaseRevenue"])
    except Exception:
        pass

    paid_detail = get_paid_detail_3way(client, w, paid_ad_totals=paid_ad_totals)
    paid_top3 = get_paid_top3(client, w)
    channel_detail_map = get_channel_detail_map_3way(client, w)
    target_roas_map = load_target_roas_map(TARGET_ROAS_XLS_PATH)
    paid_media_compare = get_paid_media_comparison_table(client, w, target_roas_map)
    kpi_snapshot = get_kpi_snapshot_table_3way(client, w, overall)
    admin_overall = build_admin_overall(w)

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
    weather_forecast = get_weekly_weather_forecast(end_date=end_date)
    brand_powerlink_status = fetch_brand_powerlink_snapshot()

    bundle = build_bundle(
        w=w,
        overall=overall,
        admin_overall=admin_overall,
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
        channel_detail_map=channel_detail_map,
        paid_media_compare=paid_media_compare,
        weather_forecast=weather_forecast,
        brand_powerlink_status=brand_powerlink_status,
    )

    if WRITE_DATA_CACHE:
        write_json(bpath, bundle)

    bundle_rel = "../data/weekly/END_" + ymd(w.end_date) + ".json" if w.mode == "weekly" else "../data/daily/" + ymd(w.end_date) + ".json"
    html = render_page_html(
        logo_b64=logo_b64,
        w=w,
        overall=overall,
        admin_overall=admin_overall,
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
        channel_detail_map=channel_detail_map,
        paid_media_compare=paid_media_compare,
        weather_forecast=weather_forecast,
        brand_powerlink_status=brand_powerlink_status,
        nav_links={"hub": "../index.html", "daily_index": "../index.html", "weekly_index": "../index.html"},
        bundle_rel_path=bundle_rel,
    )
    return html, bundle


# =========================
# Main

def _list_existing_digest_dates(daily_dir: str, weekly_dir: str, data_dir: str, latest_end: dt.date) -> List[dt.date]:
    found: set[dt.date] = set()
    patterns = [
        (Path(daily_dir), r"^(\d{4}-\d{2}-\d{2})\.html$"),
        (Path(weekly_dir), r"^END_(\d{4}-\d{2}-\d{2})\.html$"),
        (Path(data_dir) / "daily", r"^(\d{4}-\d{2}-\d{2})\.json$"),
        (Path(data_dir) / "weekly", r"^END_(\d{4}-\d{2}-\d{2})\.json$"),
    ]
    for base, pattern in patterns:
        if not base.exists():
            continue
        rx = re.compile(pattern)
        for p in base.iterdir():
            m = rx.match(p.name)
            if not m:
                continue
            d = parse_yyyy_mm_dd(m.group(1))
            if d and d <= latest_end:
                found.add(d)
    return sorted(found)


def _build_dates_for_one_time_refresh(latest_end: dt.date, daily_dir: str, weekly_dir: str, data_dir: str) -> List[dt.date]:
    explicit_start = parse_yyyy_mm_dd(os.getenv("DAILY_DIGEST_ONE_TIME_FULL_REFRESH_START", "").strip())
    if explicit_start:
        if explicit_start > latest_end:
            explicit_start = latest_end
        span = max(1, (latest_end - explicit_start).days + 1)
        return [explicit_start + dt.timedelta(days=i) for i in range(span)]

    existing = _list_existing_digest_dates(daily_dir, weekly_dir, data_dir, latest_end)
    if existing:
        return existing

    days = max(1, int(os.getenv("DAILY_DIGEST_BUILD_DAYS", str(DAYS_TO_BUILD))))
    start = latest_end - dt.timedelta(days=days - 1)
    return [start + dt.timedelta(days=i) for i in range(days)]

def _build_missing_daily_dates_for_month(target_month: str, latest_end: dt.date, daily_dir: str, data_dir: str) -> List[dt.date]:
    month_str = str(target_month or "").strip()
    if not re.fullmatch(r"\d{4}-\d{2}", month_str):
        month_str = latest_end.strftime("%Y-%m")

    month_start = dt.datetime.strptime(month_str + "-01", "%Y-%m-%d").date()
    if month_start > latest_end:
        return []

    if month_start.month == 12:
        next_month = dt.date(month_start.year + 1, 1, 1)
    else:
        next_month = dt.date(month_start.year, month_start.month + 1, 1)
    month_end = min(latest_end, next_month - dt.timedelta(days=1))

    missing: List[dt.date] = []
    span = (month_end - month_start).days + 1
    for i in range(max(span, 0)):
        d = month_start + dt.timedelta(days=i)
        out_daily = os.path.join(daily_dir, f"{ymd(d)}.html")
        data_daily = os.path.join(data_dir, "daily", f"{ymd(d)}.json")
        if not (os.path.exists(out_daily) and os.path.exists(data_daily)):
            missing.append(d)
    return missing

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

    logo_b64 = load_logo_base64(LOGO_PATH)
    image_map = load_image_map_from_excel_urls(IMAGE_XLS_PATH)

    ensure_dir(OUT_DIR)
    daily_dir = os.path.join(OUT_DIR, "daily")
    weekly_dir = os.path.join(OUT_DIR, "weekly")
    cache_dir = os.path.join(OUT_DIR, "cache")
    ensure_dir(daily_dir)
    ensure_dir(weekly_dir)
    ensure_dir(os.path.join(DATA_DIR, "daily"))
    ensure_dir(os.path.join(DATA_DIR, "weekly"))
    ensure_dir(os.path.join(OUT_DIR, "cache", "pdp"))
    ensure_dir(cache_dir)

    daily_window = build_window(latest_end, "daily")
    yoy_target = daily_window.yoy_end

    dates = sorted(set([latest_end, yoy_target]))
    print(
        f"[INFO] Daily-only targeted rebuild mode: latest={ymd(latest_end)}, yoy={ymd(yoy_target)} | "
        "existing other daily/weekly outputs are kept as-is"
    )

    all_dates = list(dates)

    for d in all_dates:
        out_daily = os.path.join(daily_dir, f"{ymd(d)}.html")
        force_rebuild = d in {latest_end, yoy_target}

        if (not force_rebuild) and SKIP_IF_EXISTS and os.path.exists(out_daily):
            print(f"[SKIP] Exists (Daily): {out_daily}")
        else:
            html_daily, _bundle = build_one(client, end_date=d, mode="daily", image_map=image_map, logo_b64=logo_b64)
            with open(out_daily, "w", encoding="utf-8") as f:
                f.write(html_daily)
            print(f"[OK] Wrote: {out_daily} (force={force_rebuild})")

    # Daily-only patch: rebuild only latest day and its YoY anchor; keep all existing outputs.

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
