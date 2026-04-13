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
import numpy as np

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


ML_SCORE_TABLE = os.getenv("MEMBER_FUNNEL_ML_SCORE_TABLE", os.getenv("CRM_MEMBER_TOTALVIEW_SCORES_TABLE", os.getenv("CRM_MEMBER_TARGET_SCORE_TABLE", "crm_mart.crm_member_totalview_scores"))).strip()
ML_SCORE_PROJECT = os.getenv("MEMBER_FUNNEL_ML_PROJECT_ID", PROJECT_ID).strip() if 'PROJECT_ID' in globals() else os.getenv("MEMBER_FUNNEL_ML_PROJECT_ID", "").strip()
_ML_SCORES_CACHE: pd.DataFrame | None = None

def _qualified_ml_table(table_name: str) -> str:
    t = str(table_name or "").strip().strip("`")
    if not t:
        return ""
    if t.count(".") == 2:
        return t
    proj = ML_SCORE_PROJECT or PROJECT_ID
    if t.count(".") == 1 and proj:
        return f"{proj}.{t}"
    return t

def _safe_num(v: Any, default: float = 0.0) -> float:
    try:
        return float(pd.to_numeric(v, errors="coerce"))
    except Exception:
        return default


def load_ml_scores() -> pd.DataFrame:
    global _ML_SCORES_CACHE
    if _ML_SCORES_CACHE is not None:
        return _ML_SCORES_CACHE.copy()
    table_name = _qualified_ml_table(ML_SCORE_TABLE)
    empty = pd.DataFrame(columns=[
        "member_id_norm","user_id_norm","ml_action_type","ml_priority_tier",
        "repurchase_30d_score","first_purchase_30d_score","churn_60d_score",
        "ltv_score","next_best_category","predicted_member_stage"
    ])
    if not table_name or bigquery is None or not (PROJECT_ID or ML_SCORE_PROJECT):
        _ML_SCORES_CACHE = empty
        return empty.copy()
    try:
        client = get_bq_client()
        sql = f"SELECT * FROM `{table_name}`"
        raw = client.query(sql, location=BQ_LOCATION).to_dataframe()
        if raw.empty:
            _ML_SCORES_CACHE = empty
            return empty.copy()

        cols = {str(c).strip().lower(): c for c in raw.columns}
        def pick(*names: str) -> str:
            for n in names:
                if n.lower() in cols:
                    return str(cols[n.lower()])
            return ""

        out = pd.DataFrame()
        member_col = pick("member_id", "member_id_norm")
        user_col = pick("user_id", "user_id_norm")
        out["member_id_norm"] = raw[member_col].astype(str).str.strip() if member_col else ""
        out["user_id_norm"] = raw[user_col].astype(str).str.strip() if user_col else ""

        action_col = pick("crm_action_type", "ml_action_type")
        priority_col = pick("priority_tier", "ml_priority_tier")
        rep_col = pick("repurchase_30d_score", "repurchase_45d_score", "repurchase_score")
        first_col = pick("first_purchase_30d_score", "first_purchase_45d_score", "first_purchase_score")
        churn_col = pick("churn_60d_score", "churn_90d_score", "churn_risk_score", "churn_score")
        ltv_col = pick("ltv_score")
        next_cat_col = pick("next_best_category")
        stage_col = pick("predicted_member_stage")

        out["ml_action_type"] = raw[action_col].astype(str).replace({"nan": "", "None": ""}).fillna("") if action_col else ""
        out["ml_priority_tier"] = raw[priority_col].astype(str).replace({"nan": "", "None": ""}).fillna("") if priority_col else ""
        out["repurchase_30d_score"] = pd.to_numeric(raw[rep_col], errors="coerce").fillna(0.0) if rep_col else 0.0
        out["first_purchase_30d_score"] = pd.to_numeric(raw[first_col], errors="coerce").fillna(0.0) if first_col else 0.0
        out["churn_60d_score"] = pd.to_numeric(raw[churn_col], errors="coerce").fillna(0.0) if churn_col else 0.0
        out["ltv_score"] = pd.to_numeric(raw[ltv_col], errors="coerce").fillna(0.0) if ltv_col else 0.0
        out["next_best_category"] = raw[next_cat_col].astype(str).replace({"nan": "", "None": ""}).fillna("") if next_cat_col else ""
        out["predicted_member_stage"] = raw[stage_col].astype(str).replace({"nan": "", "None": ""}).fillna("") if stage_col else ""

        # TOTAL VIEW 전용 score table은 행동기반 first_purchase가 비어도 member stage 기반 fallback이 가능하도록 보강
        if (out["ml_action_type"].astype(str).str.strip() == "").all():
            action = pd.Series(["GENERAL"] * len(out))
            action = np.where(out["churn_60d_score"] >= 0.75, "CHURN_PREVENTION", action)
            action = np.where(out["repurchase_30d_score"] >= 0.75, "RETENTION_REPURCHASE", action)
            action = np.where((out["first_purchase_30d_score"] >= 0.80), "FIRST_PURCHASE_NUDGE", action)
            action = np.where((out["ltv_score"] >= 0.85), "VIP_UPSELL", action)
            action = np.where((pd.Series(action).astype(str) == "GENERAL") & (out["next_best_category"].astype(str).str.strip() != ""), "CATEGORY_CROSSSELL", action)
            out["ml_action_type"] = pd.Series(action).astype(str)

        if (out["ml_priority_tier"].astype(str).str.strip() == "").all():
            priority = np.full(len(out), "P3", dtype=object)
            priority = np.where((out["ltv_score"] >= 0.8) | (out["repurchase_30d_score"] >= 0.8), "P1", priority)
            priority = np.where(((out["churn_60d_score"] >= 0.7) | (out["first_purchase_30d_score"] >= 0.75)) & (pd.Series(priority) != "P1"), "P2", priority)
            out["ml_priority_tier"] = pd.Series(priority).astype(str)

        _ML_SCORES_CACHE = out
        return out.copy()
    except Exception:
        _ML_SCORES_CACHE = empty
        return empty.copy()


def merge_ml_scores(user_df: pd.DataFrame, ml_scores: pd.DataFrame) -> pd.DataFrame:
    if user_df.empty or ml_scores.empty:
        out = user_df.copy()
        for c in ["ml_action_type","ml_priority_tier","repurchase_30d_score","first_purchase_30d_score","churn_60d_score","ltv_score","next_best_category"]:
            if c not in out.columns:
                out[c] = "" if c in {"ml_action_type","ml_priority_tier","next_best_category"} else 0.0
        return out
    out = user_df.copy()
    ml = ml_scores.copy()
    ml["__ml_join_key"] = ml["member_id_norm"].where(ml["member_id_norm"] != "", ml["user_id_norm"])
    ml = ml.sort_values(["ltv_score","repurchase_30d_score","first_purchase_30d_score","churn_60d_score"], ascending=[False,False,False,False]).drop_duplicates("__ml_join_key", keep="first")
    out["__ml_join_key"] = out["member_id_norm"].where(out["member_id_norm"] != "", out["user_id_norm"])
    out = out.merge(
        ml[["__ml_join_key","ml_action_type","ml_priority_tier","repurchase_30d_score","first_purchase_30d_score","churn_60d_score","ltv_score","next_best_category"]],
        on="__ml_join_key",
        how="left"
    ).drop(columns=["__ml_join_key"])
    for c in ["ml_action_type","ml_priority_tier","next_best_category"]:
        out[c] = out.get(c, "").fillna("").astype(str)
    for c in ["repurchase_30d_score","first_purchase_30d_score","churn_60d_score","ltv_score"]:
        out[c] = pd.to_numeric(out.get(c, 0.0), errors="coerce").fillna(0.0)
    return out

def _default_target_payload(user: pd.DataFrame) -> dict:
    seg_flag_cols = [c for c in user.columns if c.startswith('is_') and c.endswith('_norm')]
    return {
        "cards": [
            {
                "label": SEGMENT_LABELS[k],
                "count": int(user[user[f'is_{k}_norm'] == 1]['member_id_norm'].replace('', pd.NA).nunique()),
                "top_channel": top_label(user[user[f'is_{k}_norm'] == 1], 'channel_group_norm'),
                "top_product": top_label(user[user[f'is_{k}_norm'] == 1], 'purchase_product_name_norm'),
                "top_message": top_label(user[user[f'is_{k}_norm'] == 1], 'recommended_message_norm', 'GENERAL')
            }
            for k in SEGMENT_ORDER if f'is_{k}_norm' in user.columns and not user[user[f'is_{k}_norm'] == 1].empty
        ],
        "rows": rows_from_df(
            user[(user[seg_flag_cols].sum(axis=1) > 0)] if seg_flag_cols else user.head(0),
            {
                "member_id_norm":"member_id","phone_norm":"phone","channel_group_norm":"channel_group",
                "campaign_display_norm":"campaign","purchase_product_name_norm":"preferred_product",
                "recommended_message_norm":"recommended_message","consent_norm":"consent"
            }
        )
    }

def build_ml_target_payload(user: pd.DataFrame) -> dict:
    ml_scores = load_ml_scores()
    merged = merge_ml_scores(user, ml_scores)
    if ml_scores.empty or (merged["ml_action_type"].astype(str).str.strip() == "").all():
        return _default_target_payload(user)
    eligible = merged[merged["consent_norm"].astype(int) == 1].copy() if "consent_norm" in merged.columns else merged.copy()
    if eligible.empty:
        eligible = merged.copy()
    eligible["recommended_message_norm"] = eligible["ml_action_type"].where(eligible["ml_action_type"].astype(str).str.strip() != "", eligible["recommended_message_norm"] if "recommended_message_norm" in eligible.columns else "GENERAL")
    action_order = [
        "RETENTION_REPURCHASE","FIRST_PURCHASE_NUDGE","CHURN_PREVENTION","VIP_UPSELL","CATEGORY_CROSSSELL","GENERAL"
    ]
    cards = []
    for action in action_order:
        sdf = eligible[eligible["ml_action_type"] == action].copy()
        if sdf.empty:
            continue
        cards.append({
            "label": action.replace("_", " ").title(),
            "count": int(sdf["member_id_norm"].replace("", pd.NA).nunique()),
            "top_channel": top_label(sdf, "channel_group_norm"),
            "top_product": top_label(sdf, "purchase_product_name_norm"),
            "top_message": action,
        })
    if not cards:
        return _default_target_payload(user)
    sort_cols = [c for c in ["ltv_score","repurchase_30d_score","first_purchase_30d_score","churn_60d_score"] if c in eligible.columns]
    if sort_cols:
        eligible = eligible.sort_values(sort_cols, ascending=[False]*len(sort_cols))
    rows = rows_from_df(
        eligible,
        {
            "member_id_norm":"member_id","phone_norm":"phone","channel_group_norm":"channel_group",
            "campaign_display_norm":"campaign","purchase_product_name_norm":"preferred_product",
            "recommended_message_norm":"recommended_message","consent_norm":"consent"
        }
    )
    return {"cards": cards, "rows": rows}


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
    out["login_date_norm"] = pd.to_datetime(safe_series(out, ["event_date","date","last_visit_date","signup_date"], None), errors="coerce").dt.date
    out["order_date_norm"] = pd.to_datetime(safe_series(out, ["last_order_date","first_order_date","order_date"], None), errors="coerce").dt.date
    out["event_date_norm"] = out["login_date_norm"].fillna(out["order_date_norm"]).fillna(YESTERDAY_KST)
    out["member_id_norm"] = safe_series(out, ["member_id","memberid","member_no","memberno"], "").astype(str).str.strip()
    out["user_id_norm"] = safe_series(out, ["user_id","userid"], "").astype(str).str.strip()
    out["phone_norm"] = safe_series(out, ["phone","mobile_phone","cellphone","member_phone","mobile","phone_number"], "").astype(str).str.strip()
    out["channel_group_norm"] = [canonical_bucket(a,b,None,c) for a,b,c in zip(safe_series(out,["channel_group","channel_group_enhanced"],""), safe_series(out,["first_source","latest_source"],""), safe_series(out,["first_campaign","latest_campaign","session_campaign"],""))]
    out["sessions_norm"] = to_num(safe_series(out,["total_sessions","sessions"],0))
    out["orders_norm"] = to_num(safe_series(out,["order_count","orders"],0))
    out["order_id_norm"] = safe_series(out,["order_id","transaction_id","transactionid","order_no","orderno"],"").astype(str).str.strip()
    out["item_quantity_norm"] = to_num(safe_series(out,["item_quantity","quantity","qty","purchase_quantity"],0))
    out["item_price_norm"] = to_num(safe_series(out,["item_price","price","product_price","purchase_item_price"],0))
    out["item_revenue_norm"] = to_num(safe_series(out,["item_revenue","purchase_item_revenue","product_revenue","items_revenue"],0))
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
    out = df[cols].rename(columns={k:v for k,v in cols_map.items() if k in cols}).copy()
    for c in out.columns:
        try:
            if pd.api.types.is_datetime64_any_dtype(out[c]) or "date" in str(c).lower():
                out[c] = out[c].map(fmt_date)
            else:
                out[c] = out[c].where(~out[c].isna(), "")
        except Exception:
            out[c] = out[c].astype(str).replace({"nan": ""})
    return out.to_dict(orient="records")


def build_bundle(df: pd.DataFrame, start_date: dt.date, end_date: dt.date, period_key: str, period_label: str):
    raw = normalize_dataframe(df)

    df_login = raw[(raw["login_date_norm"] >= start_date) & (raw["login_date_norm"] <= end_date)].copy()
    df_order = raw[(raw["order_date_norm"] >= start_date) & (raw["order_date_norm"] <= end_date)].copy()

    user = build_user_rows(df_login)
    ml_scores = load_ml_scores()
    user = merge_ml_scores(user, ml_scores)
    nb = non_buyer_df(user)

    buy_source = build_user_rows(df_order)
    buy_source = merge_ml_scores(buy_source, ml_scores)
    buy = buyer_df(buy_source)
    buy_product_raw = merge_ml_scores(buyer_df(df_order.copy()), ml_scores)

    total_start_date = dt.date(end_date.year, 1, 1)
    member_activity = raw[
        (
            ((raw["login_date_norm"] >= total_start_date) & (raw["login_date_norm"] <= end_date))
            | ((raw["order_date_norm"] >= total_start_date) & (raw["order_date_norm"] <= end_date))
        )
        & (raw["member_id_norm"] != "")
    ].copy()
    members = dedupe_user_rows(member_activity) if not member_activity.empty else raw.head(0).copy()
    total_product_raw = buyer_df(member_activity.copy()) if not member_activity.empty else member_activity.copy()

    df = df_login if not df_login.empty else raw.copy()
    latest_date = max(df["event_date_norm"].tolist() or [YESTERDAY_KST])
    latest = df[df["event_date_norm"] == latest_date].copy()
    admin_daily = fetch_admin_period_snapshot(start_date, end_date) or (fetch_admin_daily_snapshot(end_date) if period_key == "1d" else None)

    def channel_panels(source: pd.DataFrame, mode: str, product_source_override: pd.DataFrame | None = None):
        out = {}
        src_dedup = dedupe_user_rows(source)
        base_channels = source if mode == "product" else src_dedup
        for ch in channel_names(base_channels):
            sdf = src_dedup if ch == "ALL" else src_dedup[src_dedup["channel_group_norm"] == ch].copy()
            sdf_raw = source if ch == "ALL" else source[source["channel_group_norm"] == ch].copy()
            product_base_raw = (product_source_override if product_source_override is not None else sdf_raw)
            product_base_raw = product_base_raw if ch == "ALL" else product_base_raw[product_base_raw["channel_group_norm"] == ch].copy()
            revenue_base = sdf_raw if mode in {"buyer", "product", "total"} else sdf
            revenue = float(revenue_base["metric_revenue_norm"].sum()) if "metric_revenue_norm" in revenue_base.columns else float(revenue_base["revenue_norm"].sum()) if "revenue_norm" in revenue_base.columns else 0.0
            orders = float(revenue_base["orders_norm"].sum()) if "orders_norm" in revenue_base.columns else 0.0

            products = pd.DataFrame()
            if not product_base_raw.empty:
                pb = product_base_raw[product_base_raw["purchase_product_name_norm"] != ""].copy()
                if not pb.empty:
                    if "item_revenue_norm" not in pb.columns:
                        pb["item_revenue_norm"] = 0.0
                    if "item_price_norm" not in pb.columns:
                        pb["item_price_norm"] = 0.0
                    if "item_quantity_norm" not in pb.columns:
                        pb["item_quantity_norm"] = 0.0
                    if "order_id_norm" not in pb.columns:
                        pb["order_id_norm"] = ""
                    qty = pb["item_quantity_norm"].where(pb["item_quantity_norm"] > 0, 1.0)
                    fallback_item_rev = (pb["item_price_norm"] * qty).where(pb["item_price_norm"] > 0, 0.0)
                    pb["product_revenue_calc"] = pb["item_revenue_norm"].where(pb["item_revenue_norm"] > 0, fallback_item_rev)
                    if float(pb["product_revenue_calc"].sum()) <= 0:
                        pb["product_revenue_calc"] = pb["metric_revenue_norm"]
                    buyer_key = pb["member_id_norm"].where(pb["member_id_norm"] != "", pb["user_id_norm"])
                    pb["buyer_key_norm"] = buyer_key.astype(str)
                    pb["order_key_norm"] = pb["order_id_norm"].astype(str).str.strip()
                    order_has_id = (pb["order_key_norm"] != "").any()
                    grouped = pb.groupby(["purchase_product_name_norm", "top_category_norm", "product_code_norm"], dropna=False)
                    pb["buyer_total_orders"] = pd.to_numeric(pb.get("orders_norm", 0), errors="coerce").fillna(0)
                    pb["buyer_total_revenue"] = pd.to_numeric(pb.get("metric_revenue_norm", pb.get("revenue_norm", 0)), errors="coerce").fillna(0)
                    pb["buyer_age_val"] = pd.to_numeric(pb.get("age_norm", 0), errors="coerce")
                    pb["is_repeat_buyer_flag"] = (pb["buyer_total_orders"] >= 2).astype(int)
                    pb["is_new_buyer_flag"] = (pb["buyer_total_orders"] <= 1).astype(int)
                    pb["gender_clean"] = pb.get("gender_norm", "미확인").astype(str)
                    rows_acc = []
                    for (prod_name, top_cat, prod_code), g in grouped:
                        buyers_n = int(g["buyer_key_norm"].replace("", pd.NA).dropna().nunique())
                        if order_has_id:
                            orders_n = int(g["order_key_norm"].replace("", pd.NA).dropna().nunique())
                        else:
                            orders_n = int(round(float(pd.to_numeric(g["orders_norm"], errors="coerce").fillna(0).sum())))
                        rev_n = float(pd.to_numeric(g["product_revenue_calc"], errors="coerce").fillna(0).sum())
                        img_n = next((str(v) for v in g["product_image_norm"] if str(v or "").strip()), "")
                        age_s = pd.to_numeric(g["buyer_age_val"], errors="coerce").dropna()
                        avg_age_n = float(age_s.mean()) if not age_s.empty else 0.0
                        buyer_flags = g[["buyer_key_norm", "is_repeat_buyer_flag", "is_new_buyer_flag", "channel_group_norm", "gender_clean"]].copy()
                        buyer_flags = buyer_flags[buyer_flags["buyer_key_norm"].astype(str).str.strip() != ""]
                        buyer_flags = buyer_flags.drop_duplicates("buyer_key_norm")
                        rep_pct = float(buyer_flags["is_repeat_buyer_flag"].mean() * 100.0) if not buyer_flags.empty else 0.0
                        new_pct = float(buyer_flags["is_new_buyer_flag"].mean() * 100.0) if not buyer_flags.empty else 0.0
                        top_channel_n = buyer_flags["channel_group_norm"].astype(str).value_counts().index[0] if not buyer_flags.empty else "미분류"
                        top_gender_n = buyer_flags["gender_clean"].astype(str).value_counts().index[0] if not buyer_flags.empty else "미확인"
                        rows_acc.append({
                            "purchase_product_name_norm": prod_name,
                            "top_category_norm": top_cat,
                            "product_code_norm": prod_code,
                            "buyers": buyers_n,
                            "orders": orders_n,
                            "revenue": rev_n,
                            "image": img_n,
                            "avg_age": avg_age_n,
                            "repeat_pct": rep_pct,
                            "new_pct": new_pct,
                            "orders_per_buyer": (float(orders_n) / buyers_n) if buyers_n else 0.0,
                            "aov": (rev_n / orders_n) if orders_n else 0.0,
                            "top_channel": top_channel_n,
                            "top_gender": top_gender_n,
                        })
                    products = pd.DataFrame(rows_acc)
                    if not products.empty:
                        total_prod_revenue = float(products["revenue"].sum())
                        products["rev_share_pct"] = products["revenue"].apply(lambda x: (float(x) / total_prod_revenue * 100.0) if total_prod_revenue else 0.0)
                        products = products.sort_values(["revenue", "buyers", "orders"], ascending=[False, False, False])
            prod_cards = [{"product": clean_label(r.get("purchase_product_name_norm"), "미분류"), "category": clean_label(r.get("top_category_norm"), "미분류"), "buyers": int(r.get("buyers",0)), "orders": int(round(float(r.get("orders",0) or 0))), "revenue": float(r.get("revenue",0)), "image": normalize_image_url(r.get("image","") or ""), "avg_age": float(r.get("avg_age",0) or 0), "repeat_pct": float(r.get("repeat_pct",0) or 0), "new_pct": float(r.get("new_pct",0) or 0), "orders_per_buyer": float(r.get("orders_per_buyer",0) or 0), "top_channel": clean_label(r.get("top_channel"), "미분류"), "rev_share_pct": float(r.get("rev_share_pct",0) or 0)} for _,r in products.head(8).iterrows()]
            if mode == "non_buyer":
                sdf_nb = sdf.copy()
                sdf_nb["interest_product"] = safe_series(sdf_nb, ["top_viewed_item_name","top_purchased_item_name","purchase_product_name_norm"], "").map(lambda x: clean_label(x, "미분류"))
                sdf_nb["product_view_count_norm"] = pd.to_numeric(sdf_nb.get("product_view_count_norm", 0), errors="coerce").fillna(0)
                sdf_nb["add_to_cart_norm"] = pd.to_numeric(sdf_nb.get("add_to_cart_norm", 0), errors="coerce").fillna(0)
                sdf_nb["drop_stage"] = sdf_nb.apply(lambda r: "Cart Abandon" if float(r.get("add_to_cart_norm",0) or 0) > 0 else ("PDP Drop" if float(r.get("product_view_count_norm",0) or 0) > 0 else ("Browse Drop" if float(r.get("pageviews_norm",0) or 0) > 0 else "Low Signal")), axis=1)
                rows = rows_from_df(sdf_nb.sort_values(["add_to_cart_norm","product_view_count_norm","pageviews_norm","sessions_norm"], ascending=[False,False,False,False]), {"member_id_norm":"member_id","phone_norm":"phone","user_id_norm":"user_id","channel_group_norm":"channel_group","campaign_display_norm":"campaign","interest_product":"interest_product","drop_stage":"drop_stage","sessions_norm":"sessions","pageviews_norm":"pageviews","product_view_count_norm":"pdp_views","add_to_cart_norm":"atc","first_purchase_30d_score":"ml_first_purchase","churn_60d_score":"ml_churn","consent_norm":"consent"})
                summary = {"members": int(sdf_nb["member_id_norm"].replace("", pd.NA).nunique()), "sessions": int(sdf_nb["sessions_norm"].sum()), "avg_age": avg_age(sdf_nb), "top_channel": top_label(sdf_nb, "channel_group_norm"), "pdp_users": int((sdf_nb["product_view_count_norm"] > 0).sum()), "cart_users": int((sdf_nb["add_to_cart_norm"] > 0).sum()), "cart_abandon": int((sdf_nb["add_to_cart_norm"] > 0).sum()), "high_intent": int(((sdf_nb["product_view_count_norm"] >= 3) | (sdf_nb["add_to_cart_norm"] > 0)).sum()), "avg_pv": float(pd.to_numeric(sdf_nb["pageviews_norm"], errors="coerce").fillna(0).mean()) if not sdf_nb.empty else 0.0, "avg_sessions": float(pd.to_numeric(sdf_nb["sessions_norm"], errors="coerce").fillna(0).mean()) if not sdf_nb.empty else 0.0}
                top_interest = (sdf_nb[sdf_nb["interest_product"] != "미분류"]["interest_product"].value_counts().head(8))
                top_interest_rows = [{"product": str(k), "buyers": int(v), "revenue": 0, "category": "Interest", "image": ""} for k,v in top_interest.items()]
                extra = {"channel_dist": distribution(sdf_nb, "channel_group_norm", 6), "drop_stage_mix": distribution(sdf_nb, "drop_stage", 6), "top_interest_products": top_interest_rows}
            elif mode == "buyer":
                buyer_count = int(sdf["member_id_norm"].replace("", pd.NA).where(sdf["member_id_norm"] != "", sdf["user_id_norm"]).replace("", pd.NA).nunique()) if not sdf.empty else 0
                repeat_buyers = int((pd.to_numeric(sdf.get("orders_norm", 0), errors="coerce").fillna(0) >= 2).sum())
                vip_buyers = int((pd.to_numeric(sdf.get("ltv_score", 0), errors="coerce").fillna(0) >= 0.8).sum()) if "ltv_score" in sdf.columns else 0
                rows = rows_from_df(sdf.sort_values(["metric_revenue_norm","orders_norm"], ascending=[False,False]), {"member_id_norm":"member_id","phone_norm":"phone","user_id_norm":"user_id","age_norm":"age","channel_group_norm":"channel_group","campaign_display_norm":"campaign","top_category_norm":"top_category","purchase_product_name_norm":"purchase_product_name","orders_norm":"orders","metric_revenue_norm":"revenue","last_order_date_norm":"last_order_date","repurchase_30d_score":"ml_repurchase","churn_60d_score":"ml_churn","ltv_score":"ltv_score","consent_norm":"consent"})
                summary = {"buyers": buyer_count, "revenue": revenue, "avg_age": avg_age(sdf), "top_product": top_label(sdf, "purchase_product_name_norm"), "orders": int(round(orders)), "aov": revenue/orders if orders else 0.0, "repeat_buyers": repeat_buyers, "avg_orders_per_buyer": (orders / buyer_count) if buyer_count else 0.0, "avg_rev_per_buyer": (revenue / buyer_count) if buyer_count else 0.0, "vip_buyers": vip_buyers}
                extra = {"products": prod_cards, "channel_dist": distribution(sdf, "channel_group_norm", 6)}
            elif mode == "total":
                rows = rows_from_df(sdf.sort_values(["metric_revenue_norm","orders_norm"], ascending=[False,False]), {"member_id_norm":"member_id","phone_norm":"phone","user_id_norm":"user_id","channel_group_norm":"channel_group","campaign_display_norm":"campaign","purchase_product_name_norm":"purchase_product_name","orders_norm":"orders","metric_revenue_norm":"revenue","consent_norm":"consent"})
                summary = {"members": int(sdf["member_id_norm"].replace("", pd.NA).nunique()), "buyers": int(buyer_df(sdf)["member_id_norm"].replace("", pd.NA).nunique()), "revenue": revenue, "aov": revenue/orders if orders else 0.0, "avg_age": avg_age(sdf), "top_product": top_label(sdf, "purchase_product_name_norm")}
                extra = {"category_dist": distribution(buyer_df(sdf), "top_category_norm", 8), "products": prod_cards}
            else:
                rows = [{
                    "channel": ch,
                    "product_name": clean_label(r.get("purchase_product_name_norm"), "미분류"),
                    "category": clean_label(r.get("top_category_norm"), "미분류"),
                    "product_code": clean_label(r.get("product_code_norm"), ""),
                    "avg_age": f"{float(r.get('avg_age',0) or 0):.1f}세" if float(r.get("avg_age",0) or 0) > 0 else "미확인",
                    "buyers": int(r.get("buyers", 0)),
                    "orders": int(round(float(r.get("orders", 0) or 0))),
                    "orders_per_buyer": float(r.get("orders_per_buyer", 0) or 0),
                    "repeat_pct": float(r.get("repeat_pct", 0) or 0),
                    "new_pct": float(r.get("new_pct", 0) or 0),
                    "top_channel": clean_label(r.get("top_channel"), "미분류"),
                    "rev_share_pct": float(r.get("rev_share_pct", 0) or 0),
                    "revenue": float(r.get("revenue", 0)),
                    "image": normalize_image_url(r.get("image", "") or ""),
                } for _, r in products.head(100).iterrows()]
                buyer_count = int(sdf["member_id_norm"].replace("", pd.NA).where(sdf["member_id_norm"] != "", sdf["user_id_norm"]).replace("", pd.NA).nunique()) if not sdf.empty else 0
                summary = {"buyers": buyer_count, "revenue": revenue, "avg_age": avg_age(sdf), "top_product": top_label(sdf, "purchase_product_name_norm"), "orders": int(round(orders)), "orders_per_buyer": (orders / buyer_count) if buyer_count else 0.0, "repeat_pct": float((pd.to_numeric(sdf.get("orders_norm", 0), errors="coerce").fillna(0) >= 2).mean() * 100.0) if not sdf.empty else 0.0, "new_pct": float((pd.to_numeric(sdf.get("orders_norm", 0), errors="coerce").fillna(0) <= 1).mean() * 100.0) if not sdf.empty else 0.0, "top_channel": top_label(sdf, "channel_group_norm"), "sku_count": int(len(products.index)), "aov": revenue/orders if orders else 0.0}
                extra = {"category_dist": distribution(sdf, "top_category_norm", 8), "products": prod_cards, "channel_dist": distribution(sdf, "channel_group_norm", 6), "buyer_type_mix": [{"label":"Repeat","count":int((pd.to_numeric(sdf.get('orders_norm',0), errors='coerce').fillna(0) >= 2).sum())},{"label":"New","count":int((pd.to_numeric(sdf.get('orders_norm',0), errors='coerce').fillna(0) <= 1).sum())}]}
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
        "overview": {"sessions": int(user[(user["sessions_norm"] > 0) & ((user["user_id_norm"] != "") | (user["member_id_norm"] != ""))]["member_id_norm"].replace("", pd.NA).nunique()), "orders": int(df['orders_norm'].sum()), "revenue": float(df['metric_revenue_norm'].sum()), "signups": int(df['signup_norm'].sum()), "buyers": int(max(buy['member_id_norm'].replace('', pd.NA).nunique(), buy['user_id_norm'].replace('', pd.NA).nunique())), "members": int(user['member_id_norm'].replace('', pd.NA).nunique()), "non_buyers": int(nb['member_id_norm'].replace('', pd.NA).nunique()), "metric_source": "ga4_crm_mart"},
        "user_view": {"non_buyer": channel_panels(nb, 'non_buyer'), "buyer": channel_panels(buy, 'buyer'), "product": channel_panels(buy, 'product'), "target": build_ml_target_payload(user)},
        "total_view": {"member_overview": channel_panels(members, 'total'), "date_range": {"start": total_start_date.isoformat(), "end": end_date.isoformat()}, "period_label": f"YTD {total_start_date.isoformat()} ~ {end_date.isoformat()}"},
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
            "buyers": int(max(buy["member_id_norm"].replace("", pd.NA).nunique(), buy["user_id_norm"].replace("", pd.NA).nunique())),
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
@import url('https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css');
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;700&display=swap');
:root{--bg:#ffffff;--page:#ffffff;--surface:#ffffff;--surface-soft:#f7f9fc;--surface-muted:#f3f6fb;--line:#e5ebf3;--line-strong:#d6dfeb;--ink:#111827;--muted:#6b7280;--muted-2:#94a3b8;--navy:#020817;--blue:#2563eb;--blue-strong:#1d4ed8;--blue-soft:#dbeafe;--blue-glow:rgba(37,99,235,.18);--hero-glow:rgba(59,130,246,.34);--shadow:0 16px 36px rgba(15,23,42,.07);--shadow-strong:0 28px 70px rgba(2,6,23,.16);--shadow-float:0 20px 48px rgba(15,23,42,.10);--radius-xl:30px;--radius-lg:24px;--radius-md:18px}
*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;background:#fff;color:var(--ink);font-family:'Pretendard','Noto Sans KR','Apple SD Gothic Neo','Malgun Gothic',Arial,sans-serif}.page{max-width:1680px;margin:0 auto;padding:24px 24px 110px;background:#fff}
@keyframes fadeUpSoft{0%{opacity:0;transform:translate3d(0,22px,0) scale(.985)}100%{opacity:1;transform:translate3d(0,0,0) scale(1)}}
@keyframes fadeIn{0%{opacity:0}100%{opacity:1}}
@keyframes heroFloatA{0%,100%{transform:translate3d(0,0,0) scale(1)}50%{transform:translate3d(18px,-12px,0) scale(1.04)}}
@keyframes heroFloatB{0%,100%{transform:translate3d(0,0,0) scale(1)}50%{transform:translate3d(-16px,14px,0) scale(1.06)}}
@keyframes heroShine{0%{transform:translateX(-130%) skewX(-18deg);opacity:0}18%{opacity:.22}40%{transform:translateX(135%) skewX(-18deg);opacity:0}100%{transform:translateX(135%) skewX(-18deg);opacity:0}}
@keyframes softPulse{0%,100%{box-shadow:0 0 0 0 rgba(255,255,255,0),0 12px 24px rgba(2,6,23,.10)}50%{box-shadow:0 0 0 10px rgba(255,255,255,.06),0 18px 36px rgba(2,6,23,.14)}}
@keyframes chipRise{0%,100%{transform:translateY(0)}50%{transform:translateY(-2px)}}
@keyframes barGrow{0%{transform:scaleX(.12);opacity:.35}100%{transform:scaleX(1);opacity:1}}
@keyframes cardGlow{0%,100%{box-shadow:var(--shadow)}50%{box-shadow:var(--shadow-float)}}
@keyframes panelReveal{0%{opacity:0;transform:translateY(16px)}100%{opacity:1;transform:translateY(0)}}
.hero{position:relative;overflow:hidden;background:linear-gradient(180deg,#020617 0%,#020b1f 56%,#05122d 100%);border:1px solid rgba(15,23,42,.08);border-radius:34px;padding:28px;box-shadow:var(--shadow-strong);color:#fff;animation:fadeUpSoft .8s cubic-bezier(.22,1,.36,1)}.hero:before{content:'';position:absolute;inset:-90px auto auto -90px;width:280px;height:280px;border-radius:999px;background:radial-gradient(circle,rgba(255,255,255,.10),transparent 68%);filter:blur(4px);animation:heroFloatA 10s ease-in-out infinite}.hero:after{content:'';position:absolute;inset:auto -120px -150px auto;width:360px;height:360px;border-radius:999px;background:radial-gradient(circle,var(--hero-glow),transparent 62%);filter:blur(10px);animation:heroFloatB 12s ease-in-out infinite}.hero .hero-grid:after{content:'';position:absolute;inset:-20% auto auto -18%;width:36%;height:220%;background:linear-gradient(90deg,transparent,rgba(255,255,255,.05),transparent);transform:translateX(-130%) skewX(-18deg);animation:heroShine 8.2s ease-in-out infinite}
.hero-grid{position:relative;z-index:1;display:grid;grid-template-columns:minmax(0,1.28fr) minmax(540px,.92fr);gap:28px;align-items:stretch}.eyebrow{display:inline-flex;align-items:center;gap:8px;padding:7px 13px;border-radius:999px;border:1px solid rgba(255,255,255,.12);background:rgba(255,255,255,.06);font-size:10px;font-weight:900;letter-spacing:.14em;text-transform:uppercase;backdrop-filter:blur(8px);animation:fadeIn .8s ease}.hero-copy h1{margin:16px 0 12px;font-size:clamp(38px,5vw,58px);line-height:.98;letter-spacing:-.05em;font-weight:900;animation:fadeUpSoft .88s cubic-bezier(.22,1,.36,1)}.hero-copy p{max-width:760px;margin:0 0 18px;font-size:13px;line-height:1.7;font-weight:700;color:rgba(255,255,255,.84);animation:fadeUpSoft .96s cubic-bezier(.22,1,.36,1)}.hero-meta{display:flex;gap:10px;flex-wrap:wrap;animation:fadeUpSoft 1.02s cubic-bezier(.22,1,.36,1)}.period-chip{display:inline-flex;align-items:center;justify-content:center;min-width:52px;height:36px;padding:0 14px;border-radius:999px;border:1px solid rgba(255,255,255,.14);background:rgba(255,255,255,.04);color:#fff;text-decoration:none;font-size:11px;font-weight:900;transition:transform .22s ease,background .22s ease,border-color .22s ease,box-shadow .22s ease;backdrop-filter:blur(8px)}.period-chip:hover{transform:translateY(-2px);background:rgba(255,255,255,.1);box-shadow:0 14px 24px rgba(2,6,23,.18);animation:chipRise .5s ease}.period-chip.active{background:#fff;border-color:#fff;color:#0f172a;box-shadow:0 12px 28px rgba(255,255,255,.18)}
.hero-kpis{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:18px;animation:fadeUpSoft 1.06s cubic-bezier(.22,1,.36,1)}.hero-stat{position:relative;overflow:hidden;background:linear-gradient(180deg,rgba(255,255,255,.9) 0%,rgba(255,255,255,.8) 100%);border:1px solid rgba(255,255,255,.14);border-radius:24px;padding:20px 22px;min-height:118px;display:flex;flex-direction:column;justify-content:space-between;box-shadow:0 12px 24px rgba(2,6,23,.10);backdrop-filter:blur(12px);animation:softPulse 6s ease-in-out infinite}.hero-stat:before{content:'';position:absolute;inset:auto -24px -26px auto;width:90px;height:90px;border-radius:999px;background:radial-gradient(circle,rgba(37,99,235,.14),transparent 66%)}.hero-stat .label{position:relative;font-size:10px;font-weight:900;letter-spacing:.12em;text-transform:uppercase;color:#6b7280}.hero-stat .value{position:relative;font-family:'Space Grotesk','Pretendard',sans-serif;font-size:clamp(28px,2.4vw,40px);line-height:1;letter-spacing:-.05em;font-weight:700;color:#111827;word-break:break-word}
.toolbar{display:flex;justify-content:space-between;align-items:center;gap:20px;margin:22px 0 30px;animation:fadeUpSoft .9s cubic-bezier(.22,1,.36,1)}.tabs,.subtabs{display:flex;gap:12px;flex-wrap:wrap}.tab-btn,.subtab-btn,.table-expand-btn{height:42px;padding:0 16px;border-radius:17px;border:1px solid var(--line);background:#fff;color:#334155;font-size:12px;font-weight:900;cursor:pointer;transition:transform .2s ease,border-color .2s ease,box-shadow .2s ease,background .2s ease,color .2s ease;box-shadow:none}.tab-btn:hover,.subtab-btn:hover,.table-expand-btn:hover{border-color:#cbd5e1;transform:translateY(-2px);box-shadow:0 14px 24px rgba(15,23,42,.08)}.tab-btn.active,.subtab-btn.active{background:#2563eb;color:#fff;border-color:#2563eb;box-shadow:0 12px 24px rgba(37,99,235,.20)}
.summary-card,.card,.table-wrap{background:#fff;border:1px solid var(--line);border-radius:26px;box-shadow:var(--shadow)}.summary-card{padding:28px 30px;margin-bottom:38px;animation:fadeUpSoft .95s cubic-bezier(.22,1,.36,1)}.summary-card ul{margin:10px 0 0 18px;padding:0}.summary-card li{margin:9px 0;font-size:14px;font-weight:700;color:#1f2937}
.panel{display:none}.panel.active{display:block;animation:panelReveal .42s cubic-bezier(.22,1,.36,1)}.section-head{display:flex;justify-content:space-between;align-items:flex-end;gap:18px;margin:42px 0 24px}.section-title{font-size:11px;font-weight:900;letter-spacing:.16em;text-transform:uppercase;color:#94a3b8}.section-head h2{margin:6px 0 0;font-size:30px;line-height:1.12;letter-spacing:-.04em;font-weight:900;color:#0f172a}.download-row{display:flex;gap:12px;flex-wrap:wrap}.download-btn{display:inline-flex;align-items:center;height:42px;padding:0 16px;border-radius:999px;background:#0f172a;color:#fff;text-decoration:none;font-size:12px;font-weight:900;box-shadow:0 12px 24px rgba(15,23,42,.12);transition:transform .22s ease,box-shadow .22s ease}.download-btn:hover{transform:translateY(-2px);box-shadow:0 18px 28px rgba(15,23,42,.16)}
.grid-4,.grid-3,.grid-2{display:grid;gap:28px;margin-bottom:28px}.grid-4{grid-template-columns:repeat(4,minmax(0,1fr))}.grid-3{grid-template-columns:repeat(3,minmax(0,1fr))}.grid-2{grid-template-columns:repeat(2,minmax(0,1fr))}.span-2{grid-column:span 2}.span-4{grid-column:1 / -1}.buyer-insight-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:28px;margin-bottom:28px;align-items:stretch}.product-wide-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:28px}.nonbuyer-grid{gap:28px!important}.card{position:relative;padding:26px;min-height:180px;background:linear-gradient(180deg,#fff 0%,#fbfdff 100%);transition:transform .24s ease,box-shadow .24s ease,border-color .24s ease;animation:fadeUpSoft .7s cubic-bezier(.22,1,.36,1),cardGlow 7s ease-in-out infinite}.card:hover{transform:translateY(-4px);box-shadow:var(--shadow-float);border-color:#d9e5f3}.card:before{content:'';position:absolute;inset:0 0 auto 0;height:1px;background:linear-gradient(90deg,transparent,rgba(37,99,235,.20),transparent);opacity:.9}.card + .card{margin-top:0}.kicker{font-size:10px;font-weight:900;letter-spacing:.12em;text-transform:uppercase;color:#94a3b8}.kpi{margin-top:12px;font-family:'Space Grotesk','Pretendard',sans-serif;font-size:clamp(24px,2.1vw,34px);line-height:1.02;letter-spacing:-.05em;font-weight:700;color:#0f172a}.kpi-sub{margin-top:10px;font-size:12px;line-height:1.6;font-weight:700;color:#64748b}
.chart-row{display:grid;grid-template-columns:100px 1fr 64px;gap:12px;align-items:center;margin:13px 0}.chart-label,.chart-metric{font-size:12px;font-weight:800;color:#334155}.chart-track{height:10px;background:#edf2f7;border-radius:999px;overflow:hidden}.chart-fill{display:block;height:100%;background:linear-gradient(90deg,#2563eb,#60a5fa);border-radius:999px;transform-origin:left center;animation:barGrow .9s cubic-bezier(.22,1,.36,1)}.donut-wrap{display:grid;grid-template-columns:120px 1fr;gap:18px;align-items:center}.donut{width:120px;height:120px;border-radius:50%;box-shadow:inset 0 0 0 10px rgba(255,255,255,.75),0 12px 28px rgba(37,99,235,.10);animation:fadeUpSoft .85s cubic-bezier(.22,1,.36,1)}.legend-item{display:flex;justify-content:space-between;gap:10px;align-items:center;margin:10px 0;font-size:12px;font-weight:800;color:#334155}.legend-dot{width:10px;height:10px;border-radius:50%;background:#2563eb;display:inline-block;margin-right:8px;box-shadow:0 0 0 4px rgba(37,99,235,.12)}.legend-dot.alt{background:#93c5fd;box-shadow:0 0 0 4px rgba(147,197,253,.18)}
.product-card{display:flex;gap:18px;align-items:center}.thumb{width:68px;height:68px;border-radius:18px;object-fit:cover;border:1px solid var(--line);background:#f8fafc;flex:0 0 auto;transition:transform .22s ease,box-shadow .22s ease}.product-card:hover .thumb{transform:scale(1.04);box-shadow:0 14px 22px rgba(15,23,42,.10)}.thumb-empty{display:flex;align-items:center;justify-content:center;background:#eef2f7;color:#64748b;font-size:11px;font-weight:900}.mini-title{margin-top:2px;font-size:16px;line-height:1.4;font-weight:900;color:#111827}.stack-meta{margin-top:6px;font-size:12px;font-weight:700;line-height:1.6;color:#64748b}
.table-meta,.table-tools{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:14px;font-size:12px;font-weight:700;color:#64748b}.table-wrap{overflow:auto;border-radius:26px;animation:fadeUpSoft .85s cubic-bezier(.22,1,.36,1)}.data-table{width:100%;min-width:860px;border-collapse:collapse}th,td{padding:14px 15px;border-bottom:1px solid #eef2f7;text-align:left;font-size:12px;font-weight:700;white-space:nowrap;color:#1f2937}tbody tr{transition:background .18s ease,transform .18s ease}tbody tr:hover{background:#f8fbff}th{background:#f8fafc;color:#64748b;text-transform:uppercase;letter-spacing:.08em;font-size:10px;position:sticky;top:0;z-index:1}td.num,th.num{text-align:right}.is-hidden{display:none}
.channel-panel{animation:panelReveal .34s cubic-bezier(.22,1,.36,1)}.channel-panel[hidden]{display:none!important}
@media (prefers-reduced-motion:reduce){*,*:before,*:after{animation:none!important;transition:none!important;scroll-behavior:auto!important}}
@media (max-width:1380px){.hero-grid{grid-template-columns:1fr}.hero-kpis{grid-template-columns:repeat(2,minmax(0,1fr))}.grid-4{grid-template-columns:repeat(2,minmax(0,1fr))}.grid-3{grid-template-columns:repeat(2,minmax(0,1fr))}.buyer-insight-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.product-wide-grid{grid-template-columns:1fr}.span-2,.span-4{grid-column:auto}}
@media (max-width:960px){.page{padding:16px 14px 72px}.hero{padding:20px}.hero-kpis,.grid-4,.grid-3,.grid-2,.buyer-insight-grid,.product-wide-grid{grid-template-columns:1fr}.toolbar,.section-head{flex-direction:column;align-items:flex-start}.section-head h2{font-size:24px}.hero-copy h1{font-size:36px}.summary-card,.card{padding:20px}.chart-row{grid-template-columns:84px 1fr 56px}.donut-wrap{grid-template-columns:1fr}.donut{margin:0 auto}.span-2,.span-4{grid-column:auto}}
</style>
</head>
<body>
<div class="page">
<section class="hero"><div class="hero-grid"><div class="hero-copy"><div class="eyebrow">Community Signal Style · CRM Funnel</div><h1>Member Funnel Dashboard</h1><p>External Signal 대시보드의 톤을 거의 그대로 가져오되, 회원 행동 데이터와 CRM 액션 타깃을 한 화면에서 더 넓고 선명하게 읽히도록 재구성했습니다. USER VIEW는 USER_ID 대표행 기준 액션 분석, TOTAL VIEW는 기존 회원 전체 분석입니다.</p><div class="hero-meta">__PERIOD_NAV__</div></div><div class="hero-kpis"><div class="hero-stat"><div class="label">Login Users</div><div class="value">__SESSIONS__</div></div><div class="hero-stat"><div class="label">__COUNT_LABEL__</div><div class="value">__BUYERS__</div></div><div class="hero-stat"><div class="label">__REVENUE_LABEL__</div><div class="value">__REVENUE__</div></div><div class="hero-stat"><div class="label">Non Buyers</div><div class="value">__NON_BUYERS__</div></div></div></div></section>
<div class="toolbar"><div class="tabs"><button class="tab-btn active" data-main-target="user-view">USER VIEW</button><button class="tab-btn" data-main-target="total-view">TOTAL VIEW</button></div></div>
<div class="summary-card"><div class="section-title">이번 구간 핵심 요약</div><ul>__LATEST_SUMMARY__</ul><div class="kpi-sub" style="margin-top:14px">상단 KPI Source · __METRIC_SOURCE__</div></div>
<section class="panel active" id="user-view"><div class="section-head"><div><div class="section-title">USER VIEW</div><h2>행동 데이터 · CRM 액션 뷰</h2></div><div class="download-row">__DOWNLOADS__</div></div><div id="user-sections"></div></section>
<section class="panel" id="total-view"><div class="section-head"><div><div class="section-title">TOTAL VIEW</div><h2>기존 회원 전체 분석</h2></div></div><div id="total-sections"></div></section>
</div>
<script>
let BUNDLE = null;
const INLINE_BUNDLE = __INLINE_BUNDLE__;
function money(v){const n=Number(v||0); return '₩'+Math.round(n).toLocaleString('ko-KR')}
function num(v){return Math.round(Number(v||0)).toLocaleString('ko-KR')}
function pct(v){return `${Number(v||0).toFixed(1)}%`}
function esc2(s){return String(s ?? '').replace(/[&<>"']/g, m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]))}
function bar(items){ if(!items||!items.length) return '<div class="kpi-sub">데이터가 없습니다.</div>'; const max=Math.max(...items.map(x=>Number(x.count||0)),1); return items.map(x=>`<div class="chart-row"><div class="chart-label">${esc2(x.label)}</div><div class="chart-track"><span class="chart-fill" style="width:${Math.max(4,(Number(x.count||0)/max)*100)}%"></span></div><div class="chart-metric">${num(x.count||0)}</div></div>`).join(''); }
function donut(items){ if(!items||!items.length) return '<div class="kpi-sub">데이터가 없습니다.</div>'; const total=items.reduce((a,b)=>a+Number(b.count||0),0)||1; const a=Number(items[0]?.count||0), angle=(a/total)*360; return `<div class="donut-wrap"><div class="donut" style="background:conic-gradient(#2563eb 0deg ${angle}deg,#93c5fd ${angle}deg 360deg)"></div><div>${items.slice(0,2).map((x,i)=>`<div class="legend-item"><div><span class="legend-dot ${i===1?'alt':''}"></span>${esc2(x.label)}</div><div>${pct((Number(x.count||0)/total)*100)}</div></div>`).join('')}</div></div>`; }
function groupSlug(s){return String(s||'group').toLowerCase().replace(/[^a-z0-9]+/g,'-')}
function focusCards(items, wide=false){ if(!items||!items.length) return '<div class="kpi-sub">데이터가 없습니다.</div>'; return items.slice(0,4).map(x=>{ const img=x.image_url||x.image||''; const name=x.product_name||x.product||'미분류'; const ageVal=x.avg_age?`${Number(x.avg_age).toFixed(1)}세`:(x.top_age||'미확인'); const extra=[]; if(x.rev_share_pct!=null) extra.push(`Share ${pct(x.rev_share_pct)}`); if(x.orders_per_buyer!=null) extra.push(`O/B ${Number(x.orders_per_buyer||0).toFixed(2)}`); if(x.repeat_pct!=null) extra.push(`Repeat ${pct(x.repeat_pct)}`); if(x.new_pct!=null) extra.push(`New ${pct(x.new_pct)}`); if(x.top_channel) extra.push(`Top ${esc2(x.top_channel)}`); return `<div class="card"><div class="product-card"><div class="thumb ${img?'':'thumb-empty'}">${img?`<img class="thumb" src="${esc2(img)}" alt="">`:'NO IMG'}</div><div><div class="mini-title">${esc2(name)}</div><div class="stack-meta">${esc2(x.category||'미분류')} · Buyers ${num(x.buyers||0)} · Orders ${num(x.orders||0)} · Revenue ${money(x.revenue||0)}</div><div class="stack-meta">주구매 연령 ${esc2(ageVal)}</div>${extra.length?`<div class="stack-meta">${extra.join(' · ')}</div>`:''}</div></div></div>` }).join(''); }
function table(rows, cols, numCols=[], id=''){ if(!rows||!rows.length) return '<div class="kpi-sub">데이터가 없습니다.</div>'; const visible=12; const head=cols.map(([k,l])=>`<th class="${numCols.includes(k)?'num':''}">${esc2(l)}</th>`).join(''); const body=rows.map((r,i)=>`<tr class="${i>=visible?'extra-row is-hidden':''}">${cols.map(([k])=>`<td class="${numCols.includes(k)?'num':''}">${numCols.includes(k)?(k==='revenue'?money(r[k]):num(r[k])):esc2(r[k]??'')}</td>`).join('')}</tr>`).join(''); const tools=rows.length>visible?`<div class="table-tools"><div>Rows ${num(rows.length)}</div><button class="table-expand-btn" data-expand="${id}">전체보기</button></div>`:`<div class="table-tools"><div>Rows ${num(rows.length)}</div></div>`; return `${tools}<div class="table-wrap" id="${id}"><table class="data-table"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`; }
function subTabs(group, items){ return `<div class="subtabs">${items.map((x,i)=>`<button class="subtab-btn ${i===0?'active':''}" data-group="${group}" data-target="${groupSlug(group)}-${groupSlug(x.key)}">${esc2(x.label)}</button>`).join('')}</div>`; }
function safePanelSet(section){ const channels=(section?.channels||[]); const panels=section?.panels||{}; return channels.map(ch=>({channel:ch, ...(panels[ch]||{summary:{},age_dist:[],gender_dist:[],rows:[],category_dist:[],channel_dist:[],products:[]})})); }
function pickAllPanel(section){ return (section?.panels?.ALL) || (safePanelSet(section)[0]) || {summary:{},rows:[],channel_dist:[],category_dist:[],age_dist:[],gender_dist:[],products:[]}; }
function renderNonBuyer(section){ const panel=pickAllPanel(section); const cards=`<div class="grid-4 nonbuyer-grid"><div class="card"><div class="kicker">Users</div><div class="kpi">${num(panel.summary?.members ?? panel.rows?.length ?? 0)}</div><div class="kpi-sub">구매 이력이 없는 로그인 유저</div></div><div class="card"><div class="kicker">Sessions</div><div class="kpi">${num(panel.summary?.sessions ?? 0)}</div><div class="kpi-sub">해당 세그먼트 총 세션</div></div><div class="card"><div class="kicker">PDP Users</div><div class="kpi">${num(panel.summary?.pdp_users ?? 0)}</div><div class="kpi-sub">상품상세 조회 유저</div></div><div class="card"><div class="kicker">Cart Users</div><div class="kpi">${num(panel.summary?.cart_users ?? 0)}</div><div class="kpi-sub">장바구니 진입 유저</div></div><div class="card"><div class="kicker">Cart Abandon</div><div class="kpi">${num(panel.summary?.cart_abandon ?? 0)}</div><div class="kpi-sub">장바구니 이탈 유저</div></div><div class="card"><div class="kicker">High Intent</div><div class="kpi">${num(panel.summary?.high_intent ?? 0)}</div><div class="kpi-sub">PDP/ATC 기준 고의도</div></div><div class="card"><div class="kicker">Avg PV</div><div class="kpi">${Number(panel.summary?.avg_pv ?? 0).toFixed(1)}</div><div class="kpi-sub">유저당 평균 PV</div></div><div class="card"><div class="kicker">Top Channel</div><div class="kpi" style="font-size:22px;line-height:1.2;font-family:'Pretendard','Noto Sans KR',sans-serif">${esc2(panel.summary?.top_channel || '미분류')}</div><div class="kpi-sub">주요 유입 채널</div></div></div>`; return `<section><div class="section-head"><div><div class="section-title">NON BUYER</div><h2>비구매 유저 진단</h2></div></div>${cards}<div class="grid-2"><div class="card"><div class="section-title">CHANNEL MIX</div>${bar(panel.channel_dist)}</div><div class="card"><div class="section-title">AGE MIX</div>${bar(panel.age_dist)}</div><div class="card"><div class="section-title">DROP STAGE MIX</div>${bar(panel.drop_stage_mix)}</div><div class="card"><div class="section-title">TOP INTEREST PRODUCTS</div><div class="grid-2">${focusCards(panel.top_interest_products)}</div></div></div><div class="card">${table(panel.rows,[['member_id','Member ID'],['user_id','USER_ID'],['channel_group','Channel'],['campaign','Campaign'],['interest_product','관심 상품'],['drop_stage','Drop Stage'],['sessions','Sessions'],['pageviews','PV'],['pdp_views','PDP'],['atc','ATC'],['ml_first_purchase','1st Buy Score'],['ml_churn','Churn']],['sessions','pageviews','pdp_views','atc','ml_first_purchase','ml_churn'],'nb-table')}</div></section>`; }
function renderBuyer(section){ const channels=safePanelSet(section); const tabs=subTabs('buyer', channels.map(x=>({key:x.channel,label:x.channel}))); const panels=channels.map((ch,i)=>{ const orders=Number(ch.summary?.orders ?? (ch.rows?.reduce((acc,r)=>acc+Number(r.orders||0),0) || 0)); const aov=Number(ch.summary?.aov ?? (orders ? (Number(ch.summary?.revenue||0)/orders) : 0)); return `<div class="channel-panel ${i===0?'active':''}" data-panel-group="buyer" data-panel-id="buyer-${groupSlug(ch.channel)}" ${i===0?'':'hidden'}><div class="grid-4"><div class="card"><div class="kicker">Buyers</div><div class="kpi">${num(ch.summary?.buyers ?? ch.rows?.length ?? 0)}</div></div><div class="card"><div class="kicker">Revenue</div><div class="kpi">${money(ch.summary?.revenue ?? 0)}</div></div><div class="card"><div class="kicker">Orders</div><div class="kpi">${num(orders)}</div></div><div class="card"><div class="kicker">AOV</div><div class="kpi">${money(aov)}</div></div><div class="card"><div class="kicker">Repeat Buyers</div><div class="kpi">${num(ch.summary?.repeat_buyers ?? 0)}</div></div><div class="card"><div class="kicker">Avg Orders / Buyer</div><div class="kpi">${Number(ch.summary?.avg_orders_per_buyer ?? 0).toFixed(2)}</div></div><div class="card"><div class="kicker">Avg Rev / Buyer</div><div class="kpi">${money(ch.summary?.avg_rev_per_buyer ?? 0)}</div></div><div class="card"><div class="kicker">Top Product</div><div class="kpi" style="font-size:22px;line-height:1.2;font-family:'Pretendard','Noto Sans KR',sans-serif">${esc2(ch.summary?.top_product || '미분류')}</div></div></div><div class="buyer-insight-grid"><div class="card span-2"><div class="section-title">AGE MIX</div>${bar(ch.age_dist)}</div><div class="card"><div class="section-title">GENDER MIX</div>${donut(ch.gender_dist)}</div><div class="card"><div class="section-title">CHANNEL MIX</div>${bar(ch.channel_dist)}</div><div class="card span-4"><div class="section-title">TOP PRODUCTS</div><div class="product-wide-grid">${focusCards(ch.products,true)}</div></div></div><div class="card">${table(ch.rows,[['member_id','Member ID'],['user_id','USER_ID'],['age','Age'],['campaign','Campaign'],['top_category','Top Category'],['purchase_product_name','구매 상품명'],['orders','Orders'],['revenue','Revenue'],['last_order_date','Last Order'],['ml_repurchase','Repurchase'],['ml_churn','Churn'],['ltv_score','LTV']],['age','orders','revenue','ml_repurchase','ml_churn','ltv_score'],`buyer-${groupSlug(ch.channel)}`)}</div></div>` }).join(''); return `<section><div class="section-head"><div><div class="section-title">BUYER</div><h2>채널별 구매 유저</h2></div>${tabs}</div>${panels}</section>`; }
function renderProduct(section){ const channels=safePanelSet(section); const tabs=subTabs('product', channels.map(x=>({key:x.channel,label:x.channel}))); const panels=channels.map((ch,i)=>{ const topCat=(ch.category_dist||[])[0]?.label || '미분류'; const topProduct=ch.summary?.top_product || (ch.products||[])[0]?.product || '미분류'; const rows=(ch.rows||[]).map(r=>({channel_group:ch.channel, product_name:r.product_name||r.product||'', category:r.category||'', product_code:r.product_code||'', avg_age:r.avg_age||'미확인', buyers:r.buyers||0, orders:r.orders||0, orders_per_buyer:r.orders_per_buyer||0, repeat_pct:r.repeat_pct||0, new_pct:r.new_pct||0, top_channel:r.top_channel||'', rev_share_pct:r.rev_share_pct||0, revenue:r.revenue||0})); return `<div class="channel-panel ${i===0?'active':''}" data-panel-group="product" data-panel-id="product-${groupSlug(ch.channel)}" ${i===0?'':'hidden'}><div class="grid-4"><div class="card"><div class="kicker">Buyers</div><div class="kpi">${num(ch.summary?.buyers ?? 0)}</div></div><div class="card"><div class="kicker">Revenue</div><div class="kpi">${money(ch.summary?.revenue ?? 0)}</div></div><div class="card"><div class="kicker">Orders</div><div class="kpi">${num(ch.summary?.orders ?? 0)}</div></div><div class="card"><div class="kicker">AOV</div><div class="kpi">${money(ch.summary?.aov ?? 0)}</div></div><div class="card"><div class="kicker">Top Category</div><div class="kpi" style="font-size:22px;line-height:1.2;font-family:'Pretendard','Noto Sans KR',sans-serif">${esc2(topCat)}</div></div><div class="card"><div class="kicker">Top Product</div><div class="kpi" style="font-size:22px;line-height:1.2;font-family:'Pretendard','Noto Sans KR',sans-serif">${esc2(topProduct)}</div></div><div class="card"><div class="kicker">Repeat Buyer %</div><div class="kpi">${pct(ch.summary?.repeat_pct ?? 0)}</div></div><div class="card"><div class="kicker">New Buyer %</div><div class="kpi">${pct(ch.summary?.new_pct ?? 0)}</div></div><div class="card"><div class="kicker">Orders / Buyer</div><div class="kpi">${Number(ch.summary?.orders_per_buyer ?? 0).toFixed(2)}</div></div><div class="card"><div class="kicker">Top Channel</div><div class="kpi" style="font-size:22px;line-height:1.2;font-family:'Pretendard','Noto Sans KR',sans-serif">${esc2(ch.summary?.top_channel || '미분류')}</div></div><div class="card"><div class="kicker">Avg Age</div><div class="kpi">${Number(ch.summary?.avg_age ?? 0).toFixed(1)}</div></div><div class="card"><div class="kicker">SKU Count</div><div class="kpi">${num(ch.summary?.sku_count ?? 0)}</div></div></div><div class="grid-2"><div class="card"><div class="section-title">CATEGORY SHARE</div>${bar(ch.category_dist)}</div><div class="card"><div class="section-title">CHANNEL REVENUE MIX</div>${bar(ch.channel_dist)}</div><div class="card"><div class="section-title">BUYER TYPE MIX</div>${bar(ch.buyer_type_mix)}</div><div class="card"><div class="section-title">TOP PRODUCTS</div><div class="product-wide-grid">${focusCards(ch.products,true)}</div></div></div><div class="card">${table(rows,[['product_name','Product'],['category','Category'],['product_code','Product Code'],['avg_age','주구매 연령'],['buyers','Buyers'],['orders','Orders'],['orders_per_buyer','Orders/Buyer'],['repeat_pct','Repeat %'],['new_pct','New %'],['top_channel','Top Channel'],['rev_share_pct','Rev Share %'],['revenue','Revenue']],['buyers','orders','orders_per_buyer','repeat_pct','new_pct','rev_share_pct','revenue'],`product-${groupSlug(ch.channel)}`)}</div></div>` }).join(''); return `<section><div class="section-head"><div><div class="section-title">PRODUCT</div><h2>구매 상품 집중 분석</h2></div>${tabs}</div>${panels}</section>`; }
function renderTarget(section){ const cards=(section?.cards||[]).map(seg=>`<div class="card"><div class="kicker">${esc2(seg.label)}</div><div class="kpi">${num(seg.count)}</div><div class="kpi-sub">Top Channel ${esc2(seg.top_channel||'미분류')}</div><div class="kpi-sub" style="margin-top:8px">Top Msg ${esc2(seg.top_message||'GENERAL')}</div></div>`).join(''); return `<section><div class="section-head"><div><div class="section-title">TARGET</div><h2>CRM 액션 대상 세그먼트</h2></div></div><div class="grid-4">${cards || '<div class="kpi-sub">데이터가 없습니다.</div>'}</div><div class="card">${table(section?.rows||[],[['member_id','Member ID'],['phone','Phone'],['channel_group','Channel'],['campaign','Campaign'],['preferred_product','관심 상품'],['recommended_message','메시지']],[],'target-table')}</div></section>`; }
function renderTotal(section){ const channels=safePanelSet(section); const tabs=subTabs('total', channels.map(x=>({key:x.channel,label:x.channel}))); const periodStart=section?.date_range?.start||''; const periodEnd=section?.date_range?.end||''; const periodText=(periodStart&&periodEnd)?`데이터 기간 · ${periodStart} ~ ${periodEnd} (YTD 누적)`:'YTD 누적'; const panels=channels.map((ch,i)=>`<div class="channel-panel ${i===0?'active':''}" data-panel-group="total" data-panel-id="total-${groupSlug(ch.channel)}" ${i===0?'':'hidden'}><div class="grid-4"><div class="card"><div class="kicker">Members</div><div class="kpi">${num(ch.summary?.members ?? 0)}</div></div><div class="card"><div class="kicker">Buyers</div><div class="kpi">${num(ch.summary?.buyers ?? 0)}</div></div><div class="card"><div class="kicker">Revenue</div><div class="kpi">${money(ch.summary?.revenue ?? 0)}</div></div><div class="card"><div class="kicker">대표 구매 상품명</div><div class="kpi" style="font-size:22px;line-height:1.2;font-family:'Pretendard','Noto Sans KR',sans-serif">${esc2(ch.summary?.top_product || '미분류')}</div></div></div><div class="grid-2"><div class="card"><div class="section-title">AGE 비율</div>${bar(ch.age_dist)}</div><div class="card"><div class="section-title">GENDER 비율</div>${donut(ch.gender_dist)}</div><div class="card"><div class="section-title">CATEGORY 비율</div>${bar(ch.category_dist)}</div><div class="card"><div class="section-title">구매 상품명 Focus</div><div class="grid-2">${focusCards(ch.products)}</div></div></div><div class="card">${table(ch.rows,[['member_id','Member ID'],['user_id','USER_ID'],['channel_group','Channel'],['campaign','Campaign'],['purchase_product_name','구매 상품명'],['orders','Orders'],['revenue','Revenue']],['orders','revenue'],`tot-${groupSlug(ch.channel)}`)}</div></div>`).join(''); return `<section><div class="section-head"><div><div class="section-title">TOTAL MEMBER</div><h2>기존 회원 전체 분석</h2><div class="kpi-sub" style="margin-top:10px">${esc2(periodText)}</div></div>${tabs}</div>${panels}</section>`; }
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
      `./reports/member_funnel/data/${viewFile}`,
      `./site/data/member_funnel/${viewFile}`,
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
