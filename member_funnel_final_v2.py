#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import datetime as dt
import html
import json
import os
import re
from pathlib import Path
from typing import Any

import pandas as pd

try:
    import pyodbc  # type: ignore
except Exception:
    pyodbc = None

try:
    from google.cloud import bigquery  # type: ignore
except Exception:
    bigquery = None

KST = dt.timezone(dt.timedelta(hours=9))
TODAY_KST = dt.datetime.now(KST).date()
YESTERDAY_KST = TODAY_KST - dt.timedelta(days=1)

OUT_DIR = Path(os.getenv("MEMBER_FUNNEL_OUT_DIR", os.path.join("reports", "member_funnel")))
DATA_DIR = Path(os.getenv("MEMBER_FUNNEL_DATA_DIR", str(OUT_DIR / "data")))
DOWNLOAD_DIR = Path(os.getenv("MEMBER_FUNNEL_DOWNLOAD_DIR", str(OUT_DIR / "downloads")))
HUB_SUMMARY_DIR = Path(os.getenv("MEMBER_FUNNEL_HUB_SUMMARY_DIR", "reports"))
PROJECT_ID = os.getenv("MEMBER_FUNNEL_PROJECT_ID", "").strip()
BQ_LOCATION = os.getenv("MEMBER_FUNNEL_BQ_LOCATION", "asia-northeast3").strip()
BASE_TABLE = os.getenv("MEMBER_FUNNEL_BASE_TABLE", "crm_mart.member_funnel_master").strip()
ADMIN_BQ_TABLE = os.getenv("MEMBER_FUNNEL_ADMIN_BQ_TABLE", "crm_mart.member_funnel_admin_daily").strip()
IMAGE_XLS_PATH = os.getenv("MEMBER_FUNNEL_IMAGE_XLS_PATH", os.getenv("IMAGE_XLS_PATH", "")).strip()
IMAGE_BASE_URL = os.getenv("MEMBER_FUNNEL_IMAGE_BASE_URL", "https://www.columbiasportswear.co.kr").strip().rstrip("/")
IMAGE_XLS_CANDIDATES = [
    "/mnt/data/상품코드별_이미지링크완성_최종(1).xlsx",
    "E-comm-marketing-hub/상품코드별 이미지.xlsx",
    "./E-comm-marketing-hub/상품코드별 이미지.xlsx",
    "./상품코드별 이미지.xlsx",
    "../상품코드별 이미지.xlsx",
    "../../상품코드별 이미지.xlsx",
]
SAMPLE_JSON = os.getenv("MEMBER_FUNNEL_SAMPLE_JSON", "").strip()
WRITE_DATA_CACHE = os.getenv("MEMBER_FUNNEL_WRITE_DATA_CACHE", "true").lower() in {"1","true","yes","y"}
UI_MAX_TABLE_ROWS = int(os.getenv("MEMBER_FUNNEL_UI_MAX_TABLE_ROWS", "300"))
UI_MAX_PRODUCT_ROWS = int(os.getenv("MEMBER_FUNNEL_UI_MAX_PRODUCT_ROWS", "200"))
UI_MAX_TARGET_ROWS = int(os.getenv("MEMBER_FUNNEL_UI_MAX_TARGET_ROWS", "1000"))

MSSQL_DRIVER = os.getenv("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server")
MSSQL_HOST = os.getenv("MSSQL_HOST", "")
MSSQL_PORT = os.getenv("MSSQL_PORT", "1433")
MSSQL_DATABASE = os.getenv("MSSQL_DATABASE", "")
MSSQL_USERNAME = os.getenv("MSSQL_USERNAME", "")
MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD", "")
MEMBER_FUNNEL_ADMIN_DAILY_SQL = os.getenv("MEMBER_FUNNEL_ADMIN_DAILY_SQL", "").strip()

DEFAULT_ADMIN_DAILY_SQL = r"""
DECLARE @TARGET_DATE date = '__TARGET_DATE__';

WITH traffic AS (
    SELECT
        SUM(ISNULL(StatisticsPV, 0)) AS pv,
        SUM(ISNULL(StatisticsSessions, 0)) AS sessions
    FROM dbo.TB_Statistics_Google
    WHERE CAST(StatisticsDate AS date) = @TARGET_DATE
),
signups AS (
    SELECT
        COUNT(*) AS signups
    FROM dbo.TB_Member
    WHERE CAST(MemberRegdate AS date) = @TARGET_DATE
),
orders AS (
    SELECT
        COUNT(DISTINCT OrderNo) AS order_count,
        SUM(ISNULL(OrderTotalPay, 0)) AS revenue,
        SUM(ISNULL(OrderTotalPrice, 0)) AS total_price,
        SUM(ISNULL(OrderUseCouponPrice, 0)) AS coupon_used,
        SUM(ISNULL(OrderUsePoint, 0)) AS point_used,
        SUM(ISNULL(OrderCancelPrice, 0)) AS cancel_amount
    FROM dbo.TB_Order
    WHERE CAST(OrderRegdate AS date) = @TARGET_DATE
)
SELECT
    @TARGET_DATE AS report_date,
    t.sessions,
    t.pv,
    s.signups,
    o.order_count AS orders,
    o.order_count AS buyers,
    o.revenue,
    o.total_price,
    o.coupon_used,
    o.point_used,
    o.cancel_amount,
    CASE
        WHEN o.order_count > 0 THEN CAST(o.revenue AS float) / o.order_count
        ELSE 0
    END AS aov
FROM traffic t
CROSS JOIN signups s
CROSS JOIN orders o;
"""

PERIOD_PRESETS = [
    {"key": "1d", "label": "1DAY", "days": 1, "filename": "daily.html", "is_default": False},
    {"key": "7d", "label": "7D", "days": 7, "filename": "7d.html", "is_default": False},
    {"key": "1m", "label": "30D", "days": 30, "filename": "index.html", "is_default": True},
    {"key": "1y", "label": "1YEAR", "days": 365, "filename": "1year.html", "is_default": False},
]
CHANNEL_BUCKET_ORDER = ["Awareness", "Paid Ad", "Organic Traffic", "Official SNS", "Owned Channel", "Direct", "Unknown", "etc"]
SEGMENT_ORDER = ["non_buyer", "cart_abandon", "high_intent", "repeat_buyer", "dormant", "vip"]
SEGMENT_LABELS = {"non_buyer":"Non Buyer","cart_abandon":"Cart Abandon","high_intent":"High Intent","repeat_buyer":"Repeat Buyer","dormant":"Dormant","vip":"VIP"}


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def read_json(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, payload: Any) -> None:
    ensure_dir(path.parent)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _write_summary_json(out_dir: Path, report_key: str, payload: dict) -> None:
    ensure_dir(out_dir)
    path = out_dir / "summary.json"
    data = {}
    if path.exists():
        try:
            data = read_json(path) or {}
        except Exception:
            data = {}
    data[report_key] = payload
    write_json(path, data)


def esc(v: Any) -> str:
    return html.escape("" if v is None else str(v))


def fmt_int(v: Any) -> str:
    try:
        return f"{int(round(float(v or 0))):,}"
    except Exception:
        return "0"


def fmt_money(v: Any) -> str:
    try:
        return f"₩{int(round(float(v or 0))):,}"
    except Exception:
        return "₩0"


def fmt_date(v: Any) -> str:
    if v is None or v == "" or pd.isna(v):
        return ""
    if isinstance(v, (dt.date, dt.datetime, pd.Timestamp)):
        try:
            return pd.to_datetime(v).strftime("%Y-%m-%d")
        except Exception:
            return ""
    s = str(v).strip()
    return s[:10]


def clean_label(v: Any, fallback: str = "") -> str:
    if v is None or pd.isna(v):
        return fallback
    s = str(v).strip()
    if not s or s.lower() in {"unknown","(not set)","not set","null","none","nan","nat","undefined","-"}:
        return fallback
    return s


def canonical_bucket(channel_group: Any, first_source: Any = None, medium: Any = None, campaign: Any = None) -> str:
    raw = str(channel_group or "").strip()
    mapping = {"awareness":"Awareness","paid ad":"Paid Ad","organic traffic":"Organic Traffic","official sns":"Official SNS","owned channel":"Owned Channel","direct":"Direct","unknown":"Unknown","etc":"etc"}
    if raw and raw.lower() in mapping:
        return mapping[raw.lower()]
    src = f"{first_source or ''} {medium or ''} {campaign or ''}".lower()
    if any(x in src for x in ["email","edm","kakao","lms"]): return "Owned Channel"
    if any(x in src for x in ["google","meta","facebook","naver","criteo","display","banner","cpc"]): return "Paid Ad"
    if any(x in src for x in ["instagram","ig","story","social"]): return "Official SNS"
    if any(x in src for x in ["organic","referral","search"]): return "Organic Traffic"
    return raw or "Unknown"


def first_existing(df: pd.DataFrame, candidates: list[str]) -> str:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    return ""


def safe_series(df: pd.DataFrame, candidates: list[str], default: Any = None) -> pd.Series:
    col = first_existing(df, candidates)
    return df[col] if col else pd.Series([default] * len(df), index=df.index)


def to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(0)


def period_date_range(days: int):
    end_date = YESTERDAY_KST
    start_date = end_date - dt.timedelta(days=max(1, days)-1)
    return start_date, end_date



def _sql_ident(v: Any) -> str:
    return str(v or "").strip().lower().replace(" ", "").replace("_", "").replace("/", "").replace("(", "").replace(")", "")

def _pick_value(row: dict[str, Any], *patterns: str) -> float:
    norm = {_sql_ident(k): v for k, v in row.items()}
    for pat in patterns:
        p = _sql_ident(pat)
        for nk, val in norm.items():
            if p and p in nk:
                try:
                    return float(pd.to_numeric(val, errors="coerce"))
                except Exception:
                    continue
    return 0.0


_IMAGE_MAP_CACHE: dict[str, dict[str, str]] | None = None

def derive_category_name(product_name: Any, fallback: str = "") -> str:
    name = str(product_name or "").upper().strip()
    if not name:
        return fallback or "미분류"
    rules = [
        ("PACKABLE BACKPACK", "Equipment"),
        ("BACKPACK", "Equipment"),
        ("BOTTLE HOLDER", "Equipment"),
        ("SIDE BAG", "Equipment"),
        ("BAG", "Equipment"),
        ("SLING", "Equipment"),
        ("BOOT", "Footwear"),
        ("SHOE", "Footwear"),
        ("OUTDRY", "Footwear"),
        ("PEAKFREAK", "Footwear"),
        ("CRESTWOOD", "Footwear"),
        ("KONOS", "Footwear"),
        ("MONTRAIL", "Footwear"),
        ("SHANDAL", "Footwear"),
        ("CLOG", "Footwear"),
        ("JACKET", "Outerwear"),
        ("WINDBREAKER", "Outerwear"),
        ("SHELL", "Outerwear"),
        ("DOWN", "Outerwear"),
        ("PARKA", "Outerwear"),
        ("VEST", "Outerwear"),
        ("FLEECE", "Outerwear"),
        ("PANT", "Bottom"),
        ("TROUSER", "Bottom"),
        ("CARGO", "Bottom"),
        ("SHORT", "Bottom"),
        ("TEE", "Top"),
        ("T-SHIRT", "Top"),
        ("SHIRT", "Top"),
        ("CREW", "Top"),
        ("HOOD", "Top"),
        ("SWEAT", "Top"),
        ("CAP", "Accessory"),
        ("BOONEY", "Accessory"),
        ("BUCKET", "Accessory"),
        ("HAT", "Accessory"),
        ("SOCK", "Accessory"),
        ("NECK GAITER", "Accessory"),
        ("WALLET", "Accessory"),
        ("GLOVE", "Accessory"),
    ]
    for needle, label in rules:
        if needle in name:
            return label
    return fallback or "미분류"

def normalize_image_url(v: Any) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    if s.startswith("http://") or s.startswith("https://") or s.startswith("data:"):
        return s
    if s.startswith("//"):
        return "https:" + s
    if s.startswith("/"):
        return f"{IMAGE_BASE_URL}{s}"
    return f"{IMAGE_BASE_URL}/{s.lstrip('./')}"

def normalize_product_code(v: Any) -> str:
    s = str(v or "").strip().upper()
    if not s or s in {"NAN", "NONE", "NULL"}:
        return ""
    s = re.sub(r"\s+", "", s)
    s = s.replace("-", "").replace("_", "")
    return s

def normalize_product_name(v: Any) -> str:
    s = str(v or "").strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return ""
    s = re.sub(r"\s+", " ", s)
    return s

def _find_col(cols: list[str], candidates: list[str]) -> str:
    low = {str(c).strip().lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in low:
            return str(low[cand.lower()])
    return ""


def resolve_image_xls_path() -> Path | None:
    raw = str(IMAGE_XLS_PATH or "").strip()
    candidates: list[Path] = []
    if raw:
        p = Path(raw)
        candidates.append(p)
        candidates.append((Path.cwd() / raw))
        candidates.append((Path(__file__).resolve().parent / raw))
    base = Path(__file__).resolve().parent
    for rel in IMAGE_XLS_CANDIDATES:
        candidates.append(base / rel)
        candidates.append(base.parent / rel)
        candidates.append(base.parent.parent / rel)
        candidates.append(Path.cwd() / rel)
    seen = set()
    for cand in candidates:
        try:
            cp = cand.resolve()
        except Exception:
            cp = cand
        key = str(cp)
        if key in seen:
            continue
        seen.add(key)
        if cp.exists() and cp.is_file():
            return cp
    return None

def load_image_map() -> dict[str, dict[str, str]]:
    global _IMAGE_MAP_CACHE
    if _IMAGE_MAP_CACHE is not None:
        return _IMAGE_MAP_CACHE
    _IMAGE_MAP_CACHE = {"by_code": {}, "by_name": {}}
    p = resolve_image_xls_path()
    if not p or not p.exists():
        return _IMAGE_MAP_CACHE
    try:
        # 1) 일반 헤더형 엑셀 우선 시도
        xdf = pd.read_excel(p)
        if not xdf.empty:
            code_col = _find_col(list(xdf.columns), ["상품코드", "product_code", "item_id", "code"])
            name_col = _find_col(list(xdf.columns), ["상품명", "product_name", "item_name", "name"])
            img_col = _find_col(list(xdf.columns), ["image_url", "img_url", "thumbnail_url", "thumbnail", "image", "이미지", "대표이미지", "img", "이미지링크"])
            if img_col:
                for _, r in xdf.iterrows():
                    img = str(r.get(img_col, "") or "").strip()
                    if not img:
                        continue
                    if code_col:
                        code = normalize_product_code(r.get(code_col, ""))
                        if code:
                            _IMAGE_MAP_CACHE["by_code"][code] = normalize_image_url(img)
                    if name_col:
                        nm = normalize_product_name(r.get(name_col, ""))
                        if nm:
                            _IMAGE_MAP_CACHE["by_name"][nm] = normalize_image_url(img)

        # 2) 업로드한 파일 전용 포맷 fallback:
        #    C열=상품코드, D열=상품명, E열=이미지링크
        if not _IMAGE_MAP_CACHE["by_code"]:
            raw = pd.read_excel(p, header=None)
            if raw.shape[1] >= 5:
                for i in range(len(raw)):
                    code = normalize_product_code(raw.iloc[i, 2]) if raw.shape[1] > 2 else ""
                    name = normalize_product_name(raw.iloc[i, 3]) if raw.shape[1] > 3 else ""
                    img = str(raw.iloc[i, 4] or "").strip() if raw.shape[1] > 4 else ""
                    if code in {"", "상품코드"} and name in {"", "상품명"}:
                        continue
                    if img in {"", "이미지링크", "nan"}:
                        continue
                    if code:
                        _IMAGE_MAP_CACHE["by_code"][code] = normalize_image_url(img)
                    if name:
                        _IMAGE_MAP_CACHE["by_name"][name] = normalize_image_url(img)
    except Exception:
        return _IMAGE_MAP_CACHE
    return _IMAGE_MAP_CACHE

def get_mssql_connection():
    if pyodbc is None:
        raise RuntimeError("pyodbc is not installed")
    if not all([MSSQL_HOST, MSSQL_DATABASE, MSSQL_USERNAME, MSSQL_PASSWORD]):
        raise RuntimeError("MSSQL env vars are incomplete")
    conn_str = (
        f"DRIVER={{{MSSQL_DRIVER}}};"
        f"SERVER={MSSQL_HOST},{MSSQL_PORT};"
        f"DATABASE={MSSQL_DATABASE};"
        f"UID={MSSQL_USERNAME};"
        f"PWD={MSSQL_PASSWORD};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)

def fetch_admin_daily_snapshot(target_date: dt.date) -> dict | None:
    sql = MEMBER_FUNNEL_ADMIN_DAILY_SQL or DEFAULT_ADMIN_DAILY_SQL
    if not sql:
        return None
    sql = sql.replace("__TARGET_DATE__", target_date.strftime("%Y-%m-%d"))
    conn = None
    try:
        conn = get_mssql_connection()
        df = pd.read_sql(sql, conn)
        if df.empty:
            return None
        row = df.iloc[0].to_dict()
        orders = _pick_value(row, "order_count", "orders", "주문건수", "주문건")
        buyers = _pick_value(row, "buyers", "구매자수", "구매회원")
        if buyers <= 0:
            buyers = orders
        snapshot = {
            "date": target_date.isoformat(),
            "revenue": _pick_value(row, "revenue", "총매출금액", "ERP매출", "매출금액", "sales", "total_pay"),
            "sessions": _pick_value(row, "sessions", "SESSION", "세션"),
            "signups": _pick_value(row, "signups", "가입자수", "가입자"),
            "buyers": buyers,
            "orders": orders,
            "aov": _pick_value(row, "aov", "AOV", "객단가"),
            "pv": _pick_value(row, "pv", "PV"),
            "source": "admin_mssql",
            "raw": {str(k): (None if pd.isna(v) else v) for k, v in row.items()},
        }
        return snapshot
    except Exception:
        return None
    finally:
        if conn is not None:
            conn.close()


def _qualified_bq_table(table_name: str) -> str:
    t = str(table_name or "").strip().strip("`")
    if not t:
        return ""
    if t.count(".") == 2:
        return t
    if t.count(".") == 1 and PROJECT_ID:
        return f"{PROJECT_ID}.{t}"
    return t

def fetch_admin_period_snapshot(start_date: dt.date, end_date: dt.date) -> dict | None:
    table_name = _qualified_bq_table(ADMIN_BQ_TABLE)
    if not table_name:
        return None
    try:
        client = get_bq_client()
        sql = f"""
        SELECT
          SUM(COALESCE(sessions, 0)) AS sessions,
          SUM(COALESCE(pv, 0)) AS pv,
          SUM(COALESCE(signups, 0)) AS signups,
          SUM(COALESCE(orders, 0)) AS orders,
          SUM(COALESCE(buyers, 0)) AS buyers,
          SUM(COALESCE(revenue, 0)) AS revenue,
          SUM(COALESCE(total_price, 0)) AS total_price,
          SUM(COALESCE(coupon_used, 0)) AS coupon_used,
          SUM(COALESCE(point_used, 0)) AS point_used,
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
        df = client.query(sql, job_config=cfg, location=BQ_LOCATION).to_dataframe()
        if df.empty:
            return None
        row = df.iloc[0].to_dict()
        orders = float(pd.to_numeric(row.get("orders", 0), errors="coerce") or 0)
        buyers = float(pd.to_numeric(row.get("buyers", 0), errors="coerce") or 0)
        revenue = float(pd.to_numeric(row.get("revenue", 0), errors="coerce") or 0)
        return {
            "date_start": start_date.isoformat(),
            "date_end": end_date.isoformat(),
            "sessions": float(pd.to_numeric(row.get("sessions", 0), errors="coerce") or 0),
            "pv": float(pd.to_numeric(row.get("pv", 0), errors="coerce") or 0),
            "signups": float(pd.to_numeric(row.get("signups", 0), errors="coerce") or 0),
            "orders": orders,
            "buyers": buyers if buyers > 0 else orders,
            "revenue": revenue,
            "aov": (revenue / orders) if orders else 0.0,
            "source": "admin_bq_daily",
            "raw": {str(k): (None if pd.isna(v) else v) for k, v in row.items()},
        }
    except Exception:
        return None

def get_bq_client():
    if bigquery is None:
        raise RuntimeError("google-cloud-bigquery is not installed")
    return bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)


def fetch_rows_from_bq() -> pd.DataFrame:
    client = get_bq_client()
    return client.query(f"SELECT * FROM `{BASE_TABLE}`", location=BQ_LOCATION).to_dataframe()


def _sample_json_for_period(base_path: str, key: str) -> Path | None:
    if not base_path:
        return None
    p = Path(base_path)
    if p.is_file() and "{period}" not in base_path:
        return p
    if "{period}" in base_path:
        cand = Path(base_path.format(period=key))
        return cand if cand.exists() else None
    return None


def load_rows(period_key: str) -> pd.DataFrame:
    sample = _sample_json_for_period(SAMPLE_JSON, period_key)
    if sample and sample.exists():
        raw = read_json(sample)
        if isinstance(raw, list):
            return pd.DataFrame(raw)
        if isinstance(raw, dict):
            for k in ["rows","data","records","bundle"]:
                if isinstance(raw.get(k), list):
                    return pd.DataFrame(raw[k])
    return fetch_rows_from_bq()


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    out["event_date_norm"] = pd.to_datetime(safe_series(out, ["event_date","date","last_visit_date","signup_date","first_order_date","last_order_date"], None), errors="coerce").dt.date
    out["event_date_norm"] = out["event_date_norm"].fillna(YESTERDAY_KST)
    out["member_id_norm"] = safe_series(out, ["member_id","memberid","member_no","memberno"], "").astype(str).str.strip()
    out["user_id_norm"] = safe_series(out, ["user_id","userid"], "").astype(str).str.strip()
    out["phone_norm"] = safe_series(out, ["phone","mobile_phone","cellphone","member_phone","mobile","phone_number"], "").astype(str).str.strip()
    out["channel_group_norm"] = [canonical_bucket(a,b,None,c) for a,b,c in zip(safe_series(out,["channel_group","channel_group_enhanced"],""), safe_series(out,["first_source","latest_source"],""), safe_series(out,["first_campaign","latest_campaign","session_campaign"],""))]
    out["sessions_norm"] = to_num(safe_series(out,["total_sessions","sessions"],0))
    out["orders_norm"] = to_num(safe_series(out,["order_count","orders"],0))
    out["revenue_norm"] = to_num(safe_series(out,["total_revenue","revenue"],0))
    out["ga_revenue_norm"] = to_num(safe_series(out,["ga_purchase_revenue","purchase_revenue","ga_revenue"],0))
    out["metric_revenue_norm"] = out["ga_revenue_norm"].where(out["ga_revenue_norm"] > 0, out["revenue_norm"])
    out["signup_norm"] = to_num(safe_series(out,["signup_yn"],0))
    out["purchase_norm"] = to_num(safe_series(out,["purchase_yn"],0))
    out["pageviews_norm"] = to_num(safe_series(out,["total_pageviews","pageviews"],0))
    out["product_view_norm"] = to_num(safe_series(out,["product_view_count"],0))
    out["add_to_cart_norm"] = to_num(safe_series(out,["add_to_cart_count"],0))
    out["age_norm"] = to_num(safe_series(out,["age"],None))
    age_band = safe_series(out,["age_band"],"")
    derived = pd.cut(out["age_norm"], bins=[0,19,29,39,49,59,200], labels=["10s","20s","30s","40s","50s","60+"], right=True).astype(object)
    out["age_band_norm"] = age_band.where(age_band.astype(str).str.strip()!="", derived.fillna("미확인")).astype(str)
    gender = safe_series(out,["gender","member_gender_raw"],"").astype(str).str.upper().str.strip().replace({"1":"MALE","2":"FEMALE","M":"MALE","F":"FEMALE","UNKNOWN":""})
    out["gender_norm"] = gender.where(gender!="","미확인")
    out["campaign_display_norm"] = [clean_label(a,"") or clean_label(b,"") or "미분류" for a,b in zip(safe_series(out,["campaign_display","session_campaign","first_campaign","latest_campaign"],""), out["channel_group_norm"])]
    out["purchase_product_name_norm"] = safe_series(out,["purchase_product_name","top_product_name","first_purchase_product_name","last_purchase_product_name","top_purchased_item_name","product_name_display","product_name","item_name","top_product"],"").map(lambda x: normalize_product_name(clean_label(x,"")))
    out["product_code_norm"] = safe_series(out,["top_product_code","first_purchase_product_code","last_purchase_product_code","top_purchased_item_id","item_id","product_code"],"").map(normalize_product_code)
    raw_cat = safe_series(out,["top_category","preferred_category","last_category","top_category_name","top_purchased_item_category","top_viewed_item_category"],"").map(lambda x: clean_label(x,""))
    out["top_category_norm"] = [derive_category_name(nm, fallback=cat if cat not in {"", "미분류"} else "") for nm, cat in zip(out["purchase_product_name_norm"], raw_cat)]
    out["product_image_norm"] = safe_series(out,["product_image","image_url","image","img_url","thumbnail","thumbnail_url"],"").astype(str).str.strip()
    _img_map = load_image_map()
    if _img_map["by_code"] or _img_map["by_name"]:
        out["product_image_norm"] = [
            normalize_image_url(img if str(img or "").strip() else _img_map["by_code"].get(normalize_product_code(code), "") or _img_map["by_name"].get(normalize_product_name(name), ""))
            for img, code, name in zip(out["product_image_norm"], out["product_code_norm"], out["purchase_product_name_norm"])
        ]
    out["product_image_norm"] = out["product_image_norm"].map(normalize_image_url)
    out["recommended_message_norm"] = safe_series(out,["recommended_message"],"GENERAL").astype(str)
    out["last_order_date_norm"] = safe_series(out,["last_order_date"],"").map(fmt_date)
    out["consent_norm"] = ((to_num(safe_series(out,["is_mailing"],0))>0) | (to_num(safe_series(out,["is_sms"],0))>0) | (to_num(safe_series(out,["is_alimtalk"],0))>0)).astype(int)
    for flag in ["non_buyer","cart_abandon","high_intent","repeat_buyer","dormant","vip"]:
        out[f"is_{flag}_norm"] = to_num(safe_series(out,[f"is_{flag}"],0)).astype(int)
    return out


def build_user_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    key = df["user_id_norm"].where(df["user_id_norm"]!="", df["member_id_norm"])
    sort_cols = [c for c in ["event_date_norm","revenue_norm","orders_norm","sessions_norm"] if c in df.columns]
    return df.assign(__k=key).sort_values(sort_cols, ascending=[False]*len(sort_cols)).drop_duplicates("__k", keep="first").drop(columns=["__k"])


def distribution(df: pd.DataFrame, key: str, top_n: int = 6):
    if df.empty or key not in df.columns:
        return []
    s = df[key].astype(str).replace({"":"미분류"})
    vc = s.value_counts(dropna=False)
    total = int(vc.sum())
    return [{"label":str(k),"count":int(v),"share_pct":round(v/total*100,1) if total else 0} for k,v in vc.head(top_n).items()]


def top_label(df: pd.DataFrame, key: str, fallback: str = "미분류"):
    d = distribution(df, key, 1)
    return d[0]["label"] if d else fallback


def avg_age(df: pd.DataFrame):
    s = pd.to_numeric(df.get("age_norm", pd.Series(dtype=float)), errors="coerce").dropna()
    return round(float(s.mean()),1) if not s.empty else 0


def channel_names(df: pd.DataFrame):
    present = set(df.get("channel_group_norm", pd.Series(dtype=str)).astype(str).tolist())
    ordered = [x for x in CHANNEL_BUCKET_ORDER if x in present]
    extras = sorted([x for x in present if x and x not in ordered])
    return ["ALL"] + ordered + extras


def non_buyer_df(user_df: pd.DataFrame):
    return user_df[(user_df["sessions_norm"] > 0) & ((user_df["user_id_norm"] != "") | (user_df["member_id_norm"] != "")) & (user_df["orders_norm"] <= 0) & (user_df["purchase_norm"] <= 0) & (user_df["metric_revenue_norm"] <= 0)].copy()


def buyer_df(user_df: pd.DataFrame):
    return user_df[(user_df["purchase_norm"] > 0) | (user_df["orders_norm"] > 0) | (user_df["revenue_norm"] > 0)].copy()


def dedupe_user_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    out = df.copy()
    out["__dedupe_key"] = out["member_id_norm"].where(out["member_id_norm"] != "", out["user_id_norm"])
    sort_cols = [c for c in ["event_date_norm", "metric_revenue_norm", "orders_norm", "sessions_norm"] if c in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols, ascending=[False] * len(sort_cols))
    out = out.drop_duplicates("__dedupe_key", keep="first").drop(columns=["__dedupe_key"])
    return out


def rows_from_df(df: pd.DataFrame, cols_map: dict[str,str]):
    cols = [c for c in cols_map.keys() if c in df.columns]
    if not cols:
        return []
    out = df[cols].rename(columns={k:v for k,v in cols_map.items() if k in cols}).fillna("")
    return out.to_dict(orient="records")


def build_bundle(df: pd.DataFrame, start_date: dt.date, end_date: dt.date, period_key: str, period_label: str):
    df = normalize_dataframe(df)
    df = df[(df["event_date_norm"] >= start_date) & (df["event_date_norm"] <= end_date)].copy()
    user = build_user_rows(df)
    nb = non_buyer_df(user)
    buy = buyer_df(user)
    members = user[user["member_id_norm"] != ""].copy()
    total_rows = buyer_df(members)
    latest_date = max(df["event_date_norm"].tolist() or [YESTERDAY_KST])
    latest = df[df["event_date_norm"] == latest_date].copy()
    admin_daily = fetch_admin_period_snapshot(start_date, end_date) or (fetch_admin_daily_snapshot(end_date) if period_key == "1d" else None)

    def channel_panels(source: pd.DataFrame, mode: str):
        out = {}
        src_dedup = dedupe_user_rows(source)
        for ch in channel_names(src_dedup):
            sdf = src_dedup if ch == "ALL" else src_dedup[src_dedup["channel_group_norm"] == ch].copy()
            revenue = float(sdf["metric_revenue_norm"].sum()) if "metric_revenue_norm" in sdf.columns else float(sdf["revenue_norm"].sum()) if "revenue_norm" in sdf.columns else 0.0
            orders = float(sdf["orders_norm"].sum()) if "orders_norm" in sdf.columns else 0.0
            products = (
                sdf[sdf["purchase_product_name_norm"] != ""]
                .sort_values(["metric_revenue_norm","orders_norm"], ascending=[False,False])
                .groupby(["purchase_product_name_norm", "top_category_norm", "product_code_norm"], dropna=False)
                .agg(
                    buyers=("member_id_norm","nunique"),
                    revenue=("metric_revenue_norm","sum"),
                    image=("product_image_norm", lambda x: next((str(v) for v in x if str(v or "").strip()), "")),
                )
                .reset_index().sort_values(["revenue","buyers"], ascending=[False,False])
            ) if not sdf.empty else pd.DataFrame()
            prod_cards = [{"product": clean_label(r.get("purchase_product_name_norm"), "미분류"), "category": clean_label(r.get("top_category_norm"), "미분류"), "buyers": int(r.get("buyers",0)), "revenue": float(r.get("revenue",0)), "image": normalize_image_url(r.get("image","") or "")} for _,r in products.head(8).iterrows()]
            if mode == "non_buyer":
                rows = rows_from_df(sdf, {"member_id_norm":"member_id","phone_norm":"phone","user_id_norm":"user_id","channel_group_norm":"channel_group","campaign_display_norm":"campaign","sessions_norm":"sessions","pageviews_norm":"pageviews","consent_norm":"consent"})
                summary = {"members": int(sdf["member_id_norm"].replace("", pd.NA).nunique()), "sessions": int(sdf["sessions_norm"].sum()), "avg_age": avg_age(sdf), "top_channel": top_label(sdf, "channel_group_norm")}
                extra = {"channel_dist": distribution(sdf, "channel_group_norm", 6)}
            elif mode == "buyer":
                rows = rows_from_df(sdf.sort_values(["metric_revenue_norm","orders_norm"], ascending=[False,False]), {"member_id_norm":"member_id","phone_norm":"phone","user_id_norm":"user_id","channel_group_norm":"channel_group","campaign_display_norm":"campaign","purchase_product_name_norm":"purchase_product_name","orders_norm":"orders","metric_revenue_norm":"revenue","consent_norm":"consent"})
                summary = {"buyers": int(max(sdf["member_id_norm"].replace("", pd.NA).nunique(), sdf["user_id_norm"].replace("", pd.NA).nunique())), "revenue": revenue, "avg_age": avg_age(sdf), "top_product": top_label(sdf, "purchase_product_name_norm")}
                extra = {}
            elif mode == "total":
                rows = rows_from_df(sdf.sort_values(["metric_revenue_norm","orders_norm"], ascending=[False,False]), {"member_id_norm":"member_id","phone_norm":"phone","user_id_norm":"user_id","channel_group_norm":"channel_group","campaign_display_norm":"campaign","purchase_product_name_norm":"purchase_product_name","orders_norm":"orders","metric_revenue_norm":"revenue","consent_norm":"consent"})
                summary = {"members": int(sdf["member_id_norm"].replace("", pd.NA).nunique()), "buyers": int(buyer_df(sdf)["member_id_norm"].replace("", pd.NA).nunique()), "revenue": revenue, "aov": revenue/orders if orders else 0.0, "avg_age": avg_age(sdf), "top_product": top_label(sdf, "purchase_product_name_norm")}
                extra = {"category_dist": distribution(buyer_df(sdf), "top_category_norm", 8), "products": prod_cards}
            else:
                table = sdf[sdf["purchase_product_name_norm"] != ""].groupby(["channel_group_norm","purchase_product_name_norm"], dropna=False).agg(buyers=("member_id_norm","nunique"), revenue=("revenue_norm","sum")).reset_index().sort_values(["revenue","buyers"], ascending=[False,False])
                rows = [{"channel": clean_label(r.get("channel_group_norm"),"미분류"), "product": clean_label(r.get("purchase_product_name_norm"),"미분류"), "buyers": int(r.get("buyers",0)), "revenue": float(r.get("revenue",0))} for _,r in table.head(100).iterrows()]
                summary = {"buyers": int(sdf["member_id_norm"].replace("", pd.NA).nunique()), "revenue": revenue, "avg_age": avg_age(sdf)}
                extra = {"category_dist": distribution(sdf, "top_category_norm", 8), "products": prod_cards}
            out[ch] = {"summary": summary, "age_dist": distribution(sdf, "age_band_norm", 6), "gender_dist": distribution(sdf, "gender_norm", 3), "rows": rows, **extra}
        return {"channels": channel_names(source), "panels": out}

    bundle = {
        "generated_at": dt.datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST"),
        "period_key": period_key,
        "period_label": period_label,
        "date_range": {"start": start_date.isoformat(), "end": end_date.isoformat()},
        "latest_summary": [
            f"최근 선택 가능 일자 기준 최신 데이터는 {fmt_date(latest_date)}입니다.",
            f"해당 일자 세션 {fmt_int(latest['sessions_norm'].sum())} / 가입 {fmt_int(latest['signup_norm'].sum())} / 구매자 {fmt_int(latest['purchase_norm'].sum())} 흐름입니다.",
            f"주요 채널은 {top_label(latest, 'channel_group_norm')}, 매출 기여 상위 상품은 {top_label(latest[latest['metric_revenue_norm']>0], 'purchase_product_name_norm')}입니다.",
        ],
        "overview": {"sessions": int(user[(user["sessions_norm"] > 0) & ((user["user_id_norm"] != "") | (user["member_id_norm"] != ""))]["member_id_norm"].replace("", pd.NA).nunique()), "orders": int(df['orders_norm'].sum()), "revenue": float(df['metric_revenue_norm'].sum()), "signups": int(df['signup_norm'].sum()), "buyers": int(buy['member_id_norm'].replace('', pd.NA).nunique()), "members": int(user['member_id_norm'].replace('', pd.NA).nunique()), "non_buyers": int(nb['member_id_norm'].replace('', pd.NA).nunique()), "metric_source": "ga4_crm_mart"},
        "user_view": {"non_buyer": channel_panels(nb, 'non_buyer'), "buyer": channel_panels(buy, 'buyer'), "product": channel_panels(buy, 'product'), "target": {"cards": [{"label": SEGMENT_LABELS[k], "count": int(user[user[f'is_{k}_norm'] == 1]['member_id_norm'].replace('', pd.NA).nunique()), "top_channel": top_label(user[user[f'is_{k}_norm'] == 1], 'channel_group_norm'), "top_product": top_label(user[user[f'is_{k}_norm'] == 1], 'purchase_product_name_norm'), "top_message": top_label(user[user[f'is_{k}_norm'] == 1], 'recommended_message_norm', 'GENERAL')} for k in SEGMENT_ORDER if f'is_{k}_norm' in user.columns and not user[user[f'is_{k}_norm'] == 1].empty], "rows": rows_from_df(user[(user[[c for c in user.columns if c.startswith('is_') and c.endswith('_norm')]].sum(axis=1) > 0)], {"member_id_norm":"member_id","phone_norm":"phone","channel_group_norm":"channel_group","campaign_display_norm":"campaign","purchase_product_name_norm":"preferred_product","recommended_message_norm":"recommended_message","consent_norm":"consent"})}},
        "total_view": {"member_overview": channel_panels(members, 'total')},
    }
    if admin_daily:
        _login_users = int(user[(user["sessions_norm"] > 0) & ((user["user_id_norm"] != "") | (user["member_id_norm"] != ""))]["member_id_norm"].replace("", pd.NA).nunique())
        if _login_users <= 0:
            _login_users = int(df["sessions_norm"].sum())
        bundle["overview"].update({
            "sessions": _login_users,
            "orders": int(round(float(admin_daily.get("orders", 0) or 0))),
            "revenue": float(admin_daily.get("revenue", 0) or 0),
            "signups": int(round(float(admin_daily.get("signups", 0) or 0))),
            "buyers": int(round(float(admin_daily.get("buyers", 0) or admin_daily.get("orders", 0) or 0))),
            "metric_source": str(admin_daily.get("source", "admin_bq_daily")),
        })
        bundle["latest_summary"] = [
            f"최근 선택 가능 일자 기준 최신 데이터는 {fmt_date(end_date)}입니다.",
            f"선택 기간 로그인 유저 {fmt_int(bundle['overview'].get('sessions', 0))} / 가입 {fmt_int(admin_daily.get('signups', 0))} / 주문 {fmt_int(admin_daily.get('orders', 0))} / 매출 {fmt_money(admin_daily.get('revenue', 0))} 입니다.",
            "상단 KPI는 운영 집계(BigQuery Admin Daily 우선, 필요 시 MSSQL fallback) 기준입니다.",
        ]
    else:
        if period_key == "1d":
            bundle["overview"].update({
                "sessions": 0,
                "orders": 0,
                "revenue": 0.0,
                "signups": 0,
                "buyers": 0,
                "metric_source": "admin_load_failed",
            })
            bundle["latest_summary"] = [
                f"최근 선택 가능 일자 기준 최신 데이터는 {fmt_date(end_date)}입니다.",
                "1DAY 운영 KPI를 불러오지 못했습니다.",
                "BigQuery admin daily 테이블 또는 MSSQL 연결 정보를 확인해주세요.",
            ]
    return bundle



def make_light_bundle(bundle: dict) -> dict:
    import copy
    light = copy.deepcopy(bundle)

    def trim_panel_rows(block: dict, max_rows: int):
        if not isinstance(block, dict):
            return
        for panel in (block.get("panels") or {}).values():
            rows = panel.get("rows")
            if isinstance(rows, list) and len(rows) > max_rows:
                panel["rows"] = rows[:max_rows]
                panel["rows_total"] = len(rows)
            elif isinstance(rows, list):
                panel["rows_total"] = len(rows)

    trim_panel_rows(light.get("user_view", {}).get("non_buyer", {}), UI_MAX_TABLE_ROWS)
    trim_panel_rows(light.get("user_view", {}).get("buyer", {}), UI_MAX_TABLE_ROWS)
    trim_panel_rows(light.get("user_view", {}).get("product", {}), UI_MAX_PRODUCT_ROWS)
    trim_panel_rows(light.get("total_view", {}).get("member_overview", {}), UI_MAX_TABLE_ROWS)

    target = light.get("user_view", {}).get("target", {})
    if isinstance(target.get("rows"), list):
        total = len(target["rows"])
        target["rows_total"] = total
        if total > UI_MAX_TARGET_ROWS:
            target["rows"] = target["rows"][:UI_MAX_TARGET_ROWS]

    resolved_img = resolve_image_xls_path()
    light["meta"] = {
        "ui_max_table_rows": UI_MAX_TABLE_ROWS,
        "ui_max_product_rows": UI_MAX_PRODUCT_ROWS,
        "ui_max_target_rows": UI_MAX_TARGET_ROWS,
        "image_xls_path": str(resolved_img) if resolved_img else "",
    }
    return light


def export_excel_files(bundle: dict, period_key: str):
    ensure_dir(DOWNLOAD_DIR)
    links = {}
    nb = pd.DataFrame(bundle['user_view']['non_buyer']['panels']['ALL']['rows'])
    if not nb.empty and 'consent' in nb.columns:
        nb = nb[nb['consent'].astype(int) == 1]
        if not nb.empty:
            p = DOWNLOAD_DIR / f'member_funnel_{period_key}_non_buyer.xlsx'
            nb[[c for c in ['member_id','phone','user_id','channel_group','campaign'] if c in nb.columns]].to_excel(p, index=False)
            links['non_buyer'] = os.path.relpath(p, OUT_DIR).replace('\\','/')
    tgt = pd.DataFrame(bundle['user_view']['target']['rows'])
    if not tgt.empty and 'consent' in tgt.columns:
        tgt = tgt[tgt['consent'].astype(int) == 1]
        if not tgt.empty:
            p = DOWNLOAD_DIR / f'member_funnel_{period_key}_target_segments.xlsx'
            tgt[[c for c in ['member_id','phone','channel_group','campaign','preferred_product','recommended_message'] if c in tgt.columns]].to_excel(p, index=False)
            links['target'] = os.path.relpath(p, OUT_DIR).replace('\\','/')
    bundle['downloads'] = links


def render_page(bundle: dict, preset: dict) -> str:
    period_nav = ''.join(f'<a class="period-chip {"active" if p["key"] == preset["key"] else ""}" href="{esc(p["filename"])}">{esc(p["label"])} </a>' for p in PERIOD_PRESETS)
    latest_summary = ''.join(f'<li>{esc(x)}</li>' for x in bundle.get('latest_summary', []))
    downloads = bundle.get('downloads', {})
    metric_source = str(bundle.get('overview', {}).get('metric_source', 'ga4_crm_mart'))
    revenue_label = 'Revenue' if metric_source in {'admin_bq_daily', 'admin_mssql', 'admin_mssql_failed', 'admin_load_failed'} else 'GA Revenue'
    count_label = 'Buyers' if metric_source in {'admin_bq_daily', 'admin_mssql', 'admin_mssql_failed', 'admin_load_failed'} else 'Buyers'
    html_template = """<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Member Funnel</title>
<style>
:root{--bg:#eef4ff;--bg2:#f8fbff;--surface:#ffffff;--surface-2:#f8fbff;--line:#d9e6fb;--line-strong:#bfd3fb;--navy:#0f255f;--blue:#2451e6;--blue-2:#4d7bff;--ink:#0f172a;--muted:#60708f;--chip:#eaf1ff;--shadow:0 22px 50px rgba(28,53,125,.10);--shadow-soft:0 12px 28px rgba(15,23,42,.06)}
*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;background:#ffffff;color:var(--ink);font-family:'Pretendard','Noto Sans KR','Apple SD Gothic Neo','Malgun Gothic',Arial,sans-serif}.page{max-width:1600px;margin:0 auto;padding:14px 18px 96px}
.hero{position:relative;overflow:hidden;background:linear-gradient(180deg,#020617 0%,#030816 100%);border:1px solid rgba(15,23,42,.08);border-radius:26px;padding:18px 18px 12px;color:#fff;box-shadow:0 10px 22px rgba(2,6,23,.12)}.hero:before{content:'';position:absolute;inset:auto -110px -150px auto;width:340px;height:340px;border-radius:999px;background:radial-gradient(circle,rgba(37,99,235,.22),transparent 62%)}.hero:after{content:'';position:absolute;inset:-120px auto auto -120px;width:220px;height:220px;border-radius:999px;background:radial-gradient(circle,rgba(255,255,255,.05),transparent 68%)}
.hero-grid{position:relative;z-index:1;display:grid;grid-template-columns:minmax(0,1.42fr) minmax(460px,.96fr);gap:52px;align-items:stretch}.eyebrow{display:inline-flex;align-items:center;gap:8px;padding:5px 10px;border:1px solid rgba(255,255,255,.10);border-radius:999px;font-size:9px;font-weight:900;text-transform:uppercase;letter-spacing:.16em;background:rgba(255,255,255,.06);backdrop-filter:blur(8px)}h1{margin:14px 0 8px;font-size:30px;line-height:1.02;letter-spacing:-.05em;font-weight:900}.hero p{max-width:720px;margin:0 0 12px;font-size:11px;font-weight:800;line-height:1.72;opacity:.92}.hero-meta{display:flex;gap:10px;flex-wrap:wrap}.period-chip{display:inline-flex;align-items:center;justify-content:center;min-width:48px;height:28px;padding:0 12px;border-radius:999px;border:1px solid rgba(255,255,255,.14);background:rgba(255,255,255,.04);color:#fff;text-decoration:none;font-size:10px;font-weight:900;backdrop-filter:blur(8px);transition:.18s ease}.period-chip:hover{transform:translateY(-1px);background:rgba(255,255,255,.10)}.period-chip.active{background:#fff;color:#0f172a;border-color:#fff;box-shadow:none}
.hero-kpis{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:34px}.hero-stat,.card,.summary-card{background:rgba(255,255,255,.96);border:1px solid var(--line);border-radius:26px;box-shadow:var(--shadow-soft)}.hero-stat{position:relative;padding:28px 30px;color:var(--ink);min-height:148px;display:flex;flex-direction:column;justify-content:center}.hero-stat:before{display:none}.label,.kicker{font-size:9px;font-weight:900;letter-spacing:.12em;text-transform:uppercase;color:#94a3b8}.value,.kpi{margin-top:6px;font-size:15px;font-weight:900;line-height:1.04;word-break:break-word;letter-spacing:-.03em}
.toolbar{display:flex;justify-content:space-between;align-items:center;gap:28px;margin:36px 0 30px}.tabs,.subtabs{display:flex;gap:10px;flex-wrap:wrap}.tab-btn,.subtab-btn,.table-expand-btn{height:28px;padding:0 12px;border-radius:10px;border:1px solid #e6ebf2;background:#f4f7fb;color:#475569;font-weight:900;cursor:pointer;box-shadow:none;transition:.18s ease;font-size:10px}.tab-btn:hover,.subtab-btn:hover,.table-expand-btn:hover{transform:none;border-color:#cbd5e1}.tab-btn.active,.subtab-btn.active{background:#3b82f6;color:#fff;border-color:#3b82f6;box-shadow:none}
.summary-card{padding:34px 38px;margin-bottom:40px;background:linear-gradient(180deg,rgba(255,255,255,.98) 0%,rgba(247,250,255,.98) 100%)}.summary-card ul{margin:8px 0 0 16px;padding:0}.summary-card li{margin:10px 0;font-weight:800}
.panel{display:none}.panel.active{display:block}.section-head{display:flex;justify-content:space-between;align-items:flex-end;gap:28px;margin:44px 0 30px}.section-title{font-size:12px;font-weight:900;letter-spacing:.18em;text-transform:uppercase;color:#697a98}.section-head h2{margin:6px 0 0;font-size:24px;line-height:1.2;letter-spacing:-.02em}
.download-row{display:flex;gap:12px;flex-wrap:wrap}.download-btn{display:inline-flex;align-items:center;height:40px;padding:0 14px;border-radius:999px;background:linear-gradient(135deg,#102a74 0%,#2043b8 100%);color:#fff;text-decoration:none;font-weight:900;box-shadow:0 12px 24px rgba(31,67,183,.18)}
.grid-4{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:40px}.grid-3{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:40px}.grid-2{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:40px}.card{padding:38px;min-height:188px;background:linear-gradient(180deg,#fff 0%,#fbfdff 100%)}.kpi-sub{margin-top:8px;font-size:12px;font-weight:800;color:var(--muted)}
.chart-row{display:grid;grid-template-columns:92px 1fr 60px;gap:10px;align-items:center;margin:12px 0}.chart-label,.chart-metric{font-size:12px;font-weight:900}.chart-track{height:10px;background:#e6edf9;border-radius:999px;overflow:hidden}.chart-fill{display:block;height:100%;background:linear-gradient(90deg,#2451e6,#92b7ff);border-radius:999px}.nonbuyer-grid{gap:30px!important}
.donut-wrap{display:grid;grid-template-columns:120px 1fr;gap:14px;align-items:center}.donut{width:120px;height:120px;border-radius:50%;box-shadow:inset 0 0 0 10px rgba(255,255,255,.6),0 10px 22px rgba(36,81,230,.10)}.legend-item{display:flex;justify-content:space-between;gap:10px;align-items:center;margin:8px 0;font-size:12px;font-weight:900}.legend-dot{width:10px;height:10px;border-radius:50%;background:#2451e6;display:inline-block;margin-right:8px;box-shadow:0 0 0 4px rgba(36,81,230,.10)}.legend-dot.alt{background:#93c5fd;box-shadow:0 0 0 4px rgba(147,197,253,.18)}
.product-card{display:flex;gap:26px;align-items:center}.thumb{width:64px;height:64px;border-radius:18px;object-fit:cover;border:1px solid var(--line);background:#fff}.thumb-empty{display:flex;align-items:center;justify-content:center;background:#eef3ff;color:#64748b;font-size:11px;font-weight:900}.mini-title{margin-top:4px;font-size:15px;font-weight:900;line-height:1.35}.stack-meta{margin-top:6px;font-size:12px;font-weight:800;color:var(--muted)}
.table-meta,.table-tools{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;font-size:12px;font-weight:800;color:var(--muted)}.table-wrap{overflow:auto;border:1px solid var(--line);border-radius:24px;background:#fff;box-shadow:0 18px 32px rgba(15,23,42,.06)}.data-table{width:100%;min-width:860px;border-collapse:collapse}th,td{padding:9px 10px;border-bottom:1px solid #eef2f7;text-align:left;font-size:10px;font-weight:800;white-space:nowrap}th{background:#f8fbff;color:#64748b;text-transform:uppercase;letter-spacing:.08em;font-size:9px;position:sticky;top:0}td.num,th.num{text-align:right}.is-hidden{display:none}
.channel-panel{animation:fadeInUp .24s ease}.channel-panel[hidden]{display:none!important}@keyframes fadeInUp{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}.hero,.summary-card,.card,.table-wrap{animation:fadeInUp .18s ease}
@media (max-width:1280px){.hero-grid{grid-template-columns:1fr}.hero-kpis{grid-template-columns:repeat(2,minmax(0,1fr))}}@media (max-width:1100px){.grid-4,.grid-3,.grid-2{grid-template-columns:1fr}.page{padding:22px}.hero{padding:32px 24px}.hero-kpis{grid-template-columns:1fr}.toolbar{flex-direction:column;align-items:flex-start}h1{font-size:40px}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font-family:'Noto Sans KR','Apple SD Gothic Neo','Malgun Gothic',Arial,sans-serif}.page{max-width:1680px;margin:0 auto;padding:44px 48px}
.hero{background:radial-gradient(circle at top right,rgba(255,255,255,.10),transparent 22%),linear-gradient(135deg,#17327f 0%,#1f43b7 55%,#3158df 100%);border-radius:38px;padding:46px 48px;color:#fff;box-shadow:0 34px 70px rgba(15,37,95,.20)}
.hero-grid{display:grid;grid-template-columns:minmax(0,1.45fr) minmax(460px,0.95fr);gap:40px;align-items:stretch}.eyebrow{display:inline-flex;padding:8px 12px;border:1px solid rgba(255,255,255,.25);border-radius:999px;font-size:12px;font-weight:800;text-transform:uppercase;letter-spacing:.12em;background:rgba(255,255,255,.08)}
h1{margin:18px 0 10px;font-size:56px;line-height:1}.hero p{margin:0 0 18px;font-size:16px;font-weight:700;line-height:1.6;opacity:.96}
.hero-meta{display:flex;gap:10px;flex-wrap:wrap}.period-chip{display:inline-flex;align-items:center;justify-content:center;height:44px;padding:0 18px;border-radius:999px;border:1px solid rgba(255,255,255,.24);color:#fff;text-decoration:none;font-weight:900}.period-chip.active{background:#fff;color:var(--navy)}
.hero-kpis{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:28px}.hero-stat,.card,.summary-card{background:var(--card);border:1px solid var(--line);border-radius:26px;box-shadow:0 16px 34px rgba(15,23,42,.06)}.hero-stat{padding:24px 28px;color:var(--text);min-height:136px;display:flex;flex-direction:column;justify-content:center}.label,.kicker{font-size:11px;font-weight:900;letter-spacing:.16em;text-transform:uppercase;color:var(--muted)}.value,.kpi{margin-top:10px;font-size:28px;font-weight:1000;line-height:1.1;word-break:break-word}.toolbar{display:flex;justify-content:space-between;align-items:center;gap:26px;margin:42px 0 28px}.tabs,.subtabs{display:flex;gap:10px;flex-wrap:wrap}.tab-btn,.subtab-btn,.table-expand-btn{height:44px;padding:0 18px;border-radius:999px;border:1px solid var(--line);background:#fff;color:var(--navy);font-weight:900;cursor:pointer;box-shadow:0 4px 10px rgba(15,23,42,.03)}.tab-btn.active,.subtab-btn.active{background:var(--navy);color:#fff;border-color:var(--navy)}.summary-card{padding:28px 32px;margin-bottom:34px}.summary-card ul{margin:8px 0 0 16px;padding:0}.summary-card li{margin:8px 0;font-weight:700}
.panel{display:none}.panel.active{display:block}.section-head{display:flex;justify-content:space-between;align-items:flex-end;gap:22px;margin:38px 0 24px}.section-title{font-size:12px;font-weight:900;letter-spacing:.18em;text-transform:uppercase;color:#64748b}.section-head h2{margin:6px 0 0;font-size:24px;line-height:1.2}
.download-row{display:flex;gap:12px;flex-wrap:wrap}.download-btn{display:inline-flex;align-items:center;height:40px;padding:0 14px;border-radius:999px;background:var(--navy);color:#fff;text-decoration:none;font-weight:900}
.grid-4{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:26px}.grid-3{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:26px}.grid-2{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:26px}.card{padding:28px;min-height:152px}.kpi-sub{margin-top:8px;font-size:12px;font-weight:700;color:var(--muted)}
.chart-row{display:grid;grid-template-columns:88px 1fr 58px;gap:10px;align-items:center;margin:12px 0}.chart-label,.chart-metric{font-size:12px;font-weight:800}.chart-track{height:10px;background:#e6edf7;border-radius:999px;overflow:hidden}.chart-fill{display:block;height:100%;background:linear-gradient(90deg,#2b5cff,#84b6ff);border-radius:999px}
.donut-wrap{display:grid;grid-template-columns:120px 1fr;gap:14px;align-items:center}.donut{width:120px;height:120px;border-radius:50%}.legend-item{display:flex;justify-content:space-between;gap:10px;align-items:center;margin:8px 0;font-size:12px;font-weight:800}.legend-dot{width:10px;height:10px;border-radius:50%;background:#2563eb;display:inline-block;margin-right:8px}.legend-dot.alt{background:#93c5fd}
.product-card{display:flex;gap:18px;align-items:center}.thumb{width:44px;height:44px;border-radius:12px;object-fit:cover;border:1px solid var(--line)}.thumb-empty{display:flex;align-items:center;justify-content:center;background:#eef3f8;color:#64748b;font-size:11px;font-weight:900}.mini-title{margin-top:4px;font-size:15px;font-weight:900}.stack-meta{margin-top:6px;font-size:12px;font-weight:800;color:var(--muted)}
.table-meta,.table-tools{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;font-size:12px;font-weight:800;color:var(--muted)}.table-wrap{overflow:auto;border:1px solid var(--line);border-radius:24px;background:#fff;box-shadow:0 16px 30px rgba(15,23,42,.05)}.data-table{width:100%;min-width:860px;border-collapse:collapse}th,td{padding:12px 10px;border-bottom:1px solid #eef3f8;text-align:left;font-size:12px;font-weight:800;white-space:nowrap}th{background:#f8fbff;color:#64748b;text-transform:uppercase;letter-spacing:.08em;font-size:11px}td.num,th.num{text-align:right}.is-hidden{display:none}

</style>
</head>
<body>
<div class="page">
<section class="hero"><div class="hero-grid"><div><div class="eyebrow">External Signal Style · CRM Funnel</div><h1>Member Funnel</h1><p>채널별 행동 데이터와 CRM 액션 대상을 한 화면에서 빠르게 읽고, 바로 추출할 수 있게 재정렬했습니다. USER VIEW는 USER_ID 단위 대표행 기준 액션 분석, TOTAL VIEW는 기존 회원 전체 분석입니다.</p><div class="hero-meta">__PERIOD_NAV__</div></div><div class="hero-kpis"><div class="hero-stat"><div class="label">Login Users</div><div class="value">__SESSIONS__</div></div><div class="hero-stat"><div class="label">__COUNT_LABEL__</div><div class="value">__BUYERS__</div></div><div class="hero-stat"><div class="label">__REVENUE_LABEL__</div><div class="value">__REVENUE__</div></div><div class="hero-stat"><div class="label">Non Buyers</div><div class="value">__NON_BUYERS__</div></div></div></div></section>
<div class="toolbar"><div class="tabs"><button class="tab-btn active" data-main-target="user-view">USER VIEW</button><button class="tab-btn" data-main-target="total-view">TOTAL VIEW</button></div></div>
<div class="summary-card"><div class="section-title">이번 구간 핵심 요약</div><ul>__LATEST_SUMMARY__</ul><div class="kpi-sub" style="margin-top:14px">상단 KPI Source · __METRIC_SOURCE__</div></div>
<section class="panel active" id="user-view"><div class="section-head"><div><div class="section-title">USER VIEW</div><h2>행동 데이터 · CRM 액션 뷰 (USER_ID 대표행 기준)</h2></div><div class="download-row">__DOWNLOADS__</div></div><div id="user-sections"></div></section>
<section class="panel" id="total-view"><div class="section-head"><div><div class="section-title">TOTAL VIEW</div><h2>기존 회원 전체 분석</h2></div></div><div id="total-sections"></div></section>
</div>
<script>
let BUNDLE = null;
const INLINE_BUNDLE = __INLINE_BUNDLE__;
function money(v){const n=Number(v||0); return '₩'+Math.round(n).toLocaleString('ko-KR')}
function num(v){return Math.round(Number(v||0)).toLocaleString('ko-KR')}
function pct(v){return `${Number(v||0).toFixed(1)}%`}
function esc2(s){return String(s ?? '').replace(/[&<>"']/g, m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]))}
function bar(items){ if(!items||!items.length) return '<div class="kpi-sub">데이터가 없습니다.</div>'; const max=Math.max(...items.map(x=>Number(x.count||0)),1); return items.map(x=>`<div class="chart-row"><div class="chart-label">${esc2(x.label)}</div><div class="chart-track"><span class="chart-fill" style="width:${(Number(x.count||0)/max*100).toFixed(1)}%"></span></div><div class="chart-metric">${pct(x.share_pct)}</div></div>`).join('') }
function donut(items){ if(!items||!items.length) return '<div class="kpi-sub">데이터가 없습니다.</div>'; const a=Number(items[0]?.share_pct||0), b=Number(items[1]?.share_pct||Math.max(0,100-a)); const bg=`conic-gradient(#2563eb 0 ${a}%, #93c5fd ${a}% ${a+b}%, #e2e8f0 ${a+b}% 100%)`; const lg=items.slice(0,2).map((x,i)=>`<div class="legend-item"><span><span class="legend-dot ${i?'alt':''}"></span>${esc2(x.label)}</span><strong>${pct(x.share_pct)}</strong></div>`).join(''); return `<div class="donut-wrap"><div class="donut" style="background:${bg}"></div><div>${lg}</div></div>`; }
function table(rows, cols, numeric, id){ const limit=15; const head=cols.map(c=>`<th class="${numeric.includes(c[0])?'num':''}">${esc2(c[1])}</th>`).join(''); const body=(rows||[]).map((r,i)=>`<tr class="${i>=limit?'is-hidden extra-row':''}">${cols.map(c=>{let v=r[c[0]] ?? ''; if(numeric.includes(c[0])) v=(c[0].includes('revenue')||c[0]==='aov')?money(v):num(v); return `<td class="${numeric.includes(c[0])?'num':''}">${esc2(v)}</td>`;}).join('')}</tr>`).join('') || `<tr><td colspan="${cols.length}">데이터가 없습니다.</td></tr>`; const btn=(rows||[]).length>limit?`<button class="table-expand-btn" data-expand="${id}">전체보기</button>`:''; return `<div class="table-meta"><span>전체 ${num((rows||[]).length)}행 중 ${num(Math.min((rows||[]).length,15))}행 기본 표시</span><span>${btn}</span></div><div class="table-wrap"><table class="data-table" id="${id}"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`; }
function focusCards(items){ if(!items||!items.length) return '<div class="kpi-sub">데이터가 없습니다.</div>'; return items.slice(0,4).map(x=>`<div class="card product-card"><div>${x.image?`<img class="thumb" src="${esc2(x.image)}" alt="">`:'<div class="thumb thumb-empty">IMG</div>'}</div><div><div class="kicker">${esc2(x.category||'미분류')}</div><div class="mini-title">${esc2(x.product||'미분류')}</div><div class="stack-meta">Buyers ${num(x.buyers)} / Revenue ${money(x.revenue)}</div></div></div>`).join(''); }
function tabsHtml(group, channels){ return `<div class="subtabs">${channels.map((ch,i)=>`<button class="subtab-btn ${i===0?'active':''}" data-group="${group}" data-target="${ch}">${esc2(ch)}</button>`).join('')}</div>`; }
function groupSlug(s){ return String(s).toLowerCase().replace(/[^a-z0-9가-힣]+/g,'-'); }
function renderNonBuyer(block){ return `<div class="section-head"><div><div class="section-title">NON BUYER</div><h2>가입했지만 아직 사지 않은 사람</h2></div></div>${tabsHtml('nonbuyer', block.channels)}${block.channels.map((ch,i)=>{ const p=block.panels[ch]; return `<div class="channel-panel ${i===0?'active':''}" data-panel-group="nonbuyer" data-panel-id="${ch}" ${i===0?'':'hidden'}><div class="grid-4 nonbuyer-grid"><div class="card"><div class="kicker">Non Buyer Members</div><div class="kpi">${num(p.summary.members)}</div></div><div class="card"><div class="kicker">Non Buyer Sessions</div><div class="kpi">${num(p.summary.sessions)}</div></div><div class="card"><div class="kicker">평균 나이</div><div class="kpi">${p.summary.avg_age||'-'}</div></div><div class="card"><div class="kicker">대표 유입 채널</div><div class="kpi">${esc2(p.summary.top_channel)}</div></div></div><div class="grid-3"><div class="card"><div class="section-title">AGE 비율</div>${bar(p.age_dist)}</div><div class="card"><div class="section-title">GENDER 비율</div>${donut(p.gender_dist)}</div><div class="card"><div class="section-title">대표 유입 채널 비율</div>${bar(p.channel_dist)}</div></div><div class="card">${table(p.rows,[['member_id','Member ID'],['phone','Phone'],['user_id','USER_ID'],['channel_group','Channel'],['campaign','Campaign'],['sessions','Sessions'],['pageviews','PV']],['sessions','pageviews'],`nb-${groupSlug(ch)}`)}</div></div>`; }).join('')}`; }
function renderBuyer(block){ return `<div class="section-head"><div><div class="section-title">BUYER REVENUE</div><h2>누가 매출을 만들었는지</h2><div class="kpi-sub" style="margin-top:8px">채널 디테일은 USER_ID 대표행 기준으로 중복 제거 후 집계합니다.</div></div></div>${tabsHtml('buyer', block.channels)}${block.channels.map((ch,i)=>{ const p=block.panels[ch]; return `<div class="channel-panel ${i===0?'active':''}" data-panel-group="buyer" data-panel-id="${ch}" ${i===0?'':'hidden'}><div class="grid-4"><div class="card"><div class="kicker">Buyers</div><div class="kpi">${num(p.summary.buyers)}</div></div><div class="card"><div class="kicker">Revenue</div><div class="kpi">${money(p.summary.revenue)}</div></div><div class="card"><div class="kicker">평균 나이</div><div class="kpi">${p.summary.avg_age||"-"}</div></div><div class="card"><div class="kicker">대표 구매 상품명</div><div class="kpi">${esc2(p.summary.top_product)}</div></div></div><div class="grid-2"><div class="card"><div class="section-title">AGE 비율</div>${bar(p.age_dist)}</div><div class="card"><div class="section-title">GENDER 비율</div>${donut(p.gender_dist)}</div></div><div class="card">${table(p.rows,[['member_id','Member ID'],['user_id','USER_ID'],['channel_group','Channel'],['campaign','Campaign'],['purchase_product_name','구매 상품명'],['orders','Orders'],['revenue','Revenue']],['orders','revenue'],`buy-${groupSlug(ch)}`)}</div></div>`; }).join('')}`; }
function renderProduct(block){ return `<div class="section-head"><div><div class="section-title">PRODUCT INSIGHT</div><h2>무슨 상품이 고객을 움직였는지</h2></div></div>${tabsHtml('product', block.channels)}${block.channels.map((ch,i)=>{ const p=block.panels[ch]; return `<div class="channel-panel ${i===0?'active':''}" data-panel-group="product" data-panel-id="${ch}" ${i===0?'':'hidden'}><div class="grid-2"><div class="card"><div class="section-title">Category 비율</div>${bar(p.category_dist)}</div><div class="card"><div class="section-title">구매 상품명 Focus</div><div class="grid-2">${focusCards(p.products)}</div></div></div><div class="card">${table(p.rows,[['channel','Channel'],['product','Product'],['buyers','Buyers'],['revenue','Revenue']],['buyers','revenue'],`prod-${groupSlug(ch)}`)}</div></div>`; }).join('')}`; }
function renderTarget(target){ return `<div class="section-head"><div><div class="section-title">지금 바로 액션 가능한 대상자</div><h2>세그먼트별 채널 · 관심상품 · 추천 메시지</h2></div></div><div class="grid-2">${(target.cards||[]).map(x=>`<div class="card"><div class="kicker">${esc2(x.label)}</div><div class="kpi">${num(x.count)}명</div><div class="stack-meta">Top Channel · ${esc2(x.top_channel)}</div><div class="stack-meta">Top Product · ${esc2(x.top_product)}</div><div class="stack-meta">Message · ${esc2(x.top_message)}</div></div>`).join('')}</div>`; }
function renderTotal(block){ return `${tabsHtml('totalmember', block.channels)}${block.channels.map((ch,i)=>{ const p=block.panels[ch]; return `<div class="channel-panel ${i===0?'active':''}" data-panel-group="totalmember" data-panel-id="${ch}" ${i===0?'':'hidden'}><div class="grid-4"><div class="card"><div class="kicker">기존 회원</div><div class="kpi">${num(p.summary.members)}</div></div><div class="card"><div class="kicker">구매 회원</div><div class="kpi">${num(p.summary.buyers)}</div></div><div class="card"><div class="kicker">Revenue</div><div class="kpi">${money(p.summary.revenue)}</div></div><div class="card"><div class="kicker">대표 구매 상품명</div><div class="kpi">${esc2(p.summary.top_product)}</div></div></div><div class="grid-2"><div class="card"><div class="section-title">AGE 비율</div>${bar(p.age_dist)}</div><div class="card"><div class="section-title">GENDER 비율</div>${donut(p.gender_dist)}</div><div class="card"><div class="section-title">Category 비율</div>${bar(p.category_dist)}</div><div class="card"><div class="section-title">구매 상품명 Focus</div><div class="grid-2">${focusCards(p.products)}</div></div></div><div class="card">${table(p.rows,[['member_id','Member ID'],['user_id','USER_ID'],['channel_group','Channel'],['campaign','Campaign'],['purchase_product_name','구매 상품명'],['orders','Orders'],['revenue','Revenue']],['orders','revenue'],`tot-${groupSlug(ch)}`)}</div></div>`; }).join('')}`; }
function bindUi(){
  document.querySelectorAll('[data-main-target]').forEach(btn=>btn.addEventListener('click',()=>{ document.querySelectorAll('[data-main-target]').forEach(b=>b.classList.remove('active')); btn.classList.add('active'); document.querySelectorAll('.panel').forEach(p=>p.classList.remove('active')); document.getElementById(btn.dataset.mainTarget).classList.add('active'); }));
  document.addEventListener('click', (e)=>{ const sub=e.target.closest('.subtab-btn'); if(sub){ const g=sub.dataset.group, t=sub.dataset.target; document.querySelectorAll(`.subtab-btn[data-group="${g}"]`).forEach(b=>b.classList.toggle('active', b===sub)); document.querySelectorAll(`.channel-panel[data-panel-group="${g}"]`).forEach(p=>{ const on=p.dataset.panelId===t; p.hidden=!on; p.classList.toggle('active', on); }); } const ex=e.target.closest('[data-expand]'); if(ex){ const table=document.getElementById(ex.dataset.expand); if(table){ table.querySelectorAll('.extra-row').forEach(r=>r.classList.toggle('is-hidden')); ex.textContent = ex.textContent==='전체보기' ? '접기' : '전체보기'; } } });
}
function init(bundle){
  BUNDLE = bundle;
  document.getElementById('user-sections').innerHTML = renderNonBuyer(BUNDLE.user_view.non_buyer) + renderBuyer(BUNDLE.user_view.buyer) + renderProduct(BUNDLE.user_view.product) + renderTarget(BUNDLE.user_view.target);
  document.getElementById('total-sections').innerHTML = renderTotal(BUNDLE.total_view.member_overview);
}
async function tryFetchBundle(paths){
  for(const bundlePath of paths){
    try {
      const res = await fetch(bundlePath, {cache:'no-store'});
      if(!res.ok) continue;
      return await res.json();
    } catch(err) {}
  }
  throw new Error('all bundle fetch paths failed');
}
(async function(){
  init(INLINE_BUNDLE);
  bindUi();
  try {
    const viewFile = '__VIEW_FILE__';
    const candidates = [
      `./data/${viewFile}`,
      `data/${viewFile}`,
      `./${viewFile}`,
      `../member_funnel/data/${viewFile}`,
      `../data/member_funnel/${viewFile}`,
    ];
    const bundle = await tryFetchBundle(candidates);
    init(bundle);
  } catch(err) {
    console.warn('member_funnel fetch fallback -> inline bundle used', err);
  }
})();
</script></body></html>"""
    download_html = ''
    if downloads.get('non_buyer'):
        download_html += f'<a class="download-btn" href="{esc(downloads["non_buyer"])}">Non Buyer Excel</a>'
    if downloads.get('target'):
        download_html += f'<a class="download-btn" href="{esc(downloads["target"])}">Target Segment Excel</a>'
    return (html_template
        .replace('__PERIOD_NAV__', period_nav)
        .replace('__SESSIONS__', fmt_int(bundle['overview']['sessions']))
        .replace('__BUYERS__', fmt_int(bundle['overview']['buyers']))
        .replace('__REVENUE__', fmt_money(bundle['overview']['revenue']))
        .replace('__REVENUE_LABEL__', revenue_label)
        .replace('__COUNT_LABEL__', count_label)
        .replace('__NON_BUYERS__', fmt_int(bundle['overview'].get('non_buyers', 0)))
        .replace('__LATEST_SUMMARY__', latest_summary)
        .replace('__DOWNLOADS__', download_html)
        .replace('__METRIC_SOURCE__', 'BigQuery Admin Daily' if metric_source == 'admin_bq_daily' else ('MSSQL Admin Daily' if metric_source == 'admin_mssql' else ('ADMIN LOAD FAILED' if metric_source in {'admin_mssql_failed','admin_load_failed'} else 'GA4 + CRM Mart')))
        .replace('__INLINE_BUNDLE__', json.dumps(bundle, ensure_ascii=False))
        .replace('__VIEW_FILE__', f"{preset['key']}_view.json")
    )

def main():
    ensure_dir(OUT_DIR); ensure_dir(DATA_DIR); ensure_dir(DOWNLOAD_DIR)
    default_payload = None
    for preset in PERIOD_PRESETS:
        start_date, end_date = period_date_range(preset['days'])
        df = load_rows(preset['key'])
        bundle = build_bundle(df, start_date, end_date, preset['key'], preset['label'])
        export_excel_files(bundle, preset['key'])
        light_bundle = make_light_bundle(bundle)
        if WRITE_DATA_CACHE:
            write_json(DATA_DIR / f"{preset['key']}_view.json", light_bundle)
        (OUT_DIR / preset['filename']).write_text(render_page(light_bundle, preset), encoding='utf-8')
        if preset['is_default']:
            default_payload = light_bundle
    if default_payload:
        _write_summary_json(HUB_SUMMARY_DIR, 'member_funnel', {'title':'Member Funnel','updated_at':default_payload.get('generated_at'),'range':default_payload.get('date_range'),'sessions':default_payload.get('overview',{}).get('sessions',0),'buyers':default_payload.get('overview',{}).get('buyers',0)})

if __name__ == '__main__':
    main()
