#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Member Funnel dashboard builder - External Signal style / CRM Funnel edition

What it does
- Reads member funnel data from BigQuery or a sample JSON bundle
- Builds static HTML pages under reports/member_funnel/
- Provides 2 main tabs:
  1) USER VIEW  : USER_ID matched CRM/actionable audience view
  2) TOTAL VIEW : official mall overall session-based performance view
- Generates Excel exports for:
  * non_buyer members (member_id + phone)
  * target segments (member_id + phone)

Required / expected columns in base table (graceful fallback if missing)
- event_date
- user_id, member_id
- phone / mobile_phone / cellphone / member_phone (any one is fine)
- channel_group, first_source, first_campaign, latest_source, latest_campaign
- total_sessions, total_revenue, order_count, purchase_yn, signup_yn
- top_category, top_product, first_purchase_product, preferred_category, preferred_product
- age, age_band, gender
- is_non_buyer, is_cart_abandon, is_high_intent, is_repeat_buyer, is_dormant, is_vip
- recommended_message

Optional columns used if present
- session_campaign
- last_category
- last_order_date
- last_visit_date
- days_since_signup
- days_since_last_purchase
- add_to_cart_count, product_view_count, total_pageviews
"""

from __future__ import annotations

import json
import html
import math
import os
import re
import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

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
SAMPLE_JSON = os.getenv("MEMBER_FUNNEL_SAMPLE_JSON", "").strip()
WRITE_DATA_CACHE = os.getenv("MEMBER_FUNNEL_WRITE_DATA_CACHE", "true").lower() in {"1", "true", "yes", "y"}

PERIOD_PRESETS = [
    {"key": "1d", "label": "1DAY", "days": 1, "filename": "daily.html", "is_default": False},
    {"key": "7d", "label": "7D", "days": 7, "filename": "7d.html", "is_default": False},
    {"key": "1m", "label": "1MONTH", "days": 30, "filename": "index.html", "is_default": True},
    {"key": "1y", "label": "1YEAR", "days": 365, "filename": "1year.html", "is_default": False},
]

CHANNEL_BUCKET_ORDER = [
    "Awareness",
    "Paid Ad",
    "Organic Traffic",
    "Official SNS",
    "Owned Channel",
    "Direct",
    "Unknown",
    "etc",
]

SEGMENT_ORDER = ["non_buyer", "cart_abandon", "high_intent", "repeat_buyer", "dormant", "vip"]
SEGMENT_LABELS = {
    "non_buyer": "Non Buyer",
    "cart_abandon": "Cart Abandon",
    "high_intent": "High Intent",
    "repeat_buyer": "Repeat Buyer",
    "dormant": "Dormant",
    "vip": "VIP",
}


# ------------------------------------------------------------------
# utilities
# ------------------------------------------------------------------
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


def slugify(text: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9가-힣]+", "-", str(text or "").strip().lower())
    return re.sub(r"-+", "-", text).strip("-") or "all"


def fmt_int(v: Any) -> str:
    try:
        return f"{int(round(float(v or 0))):,}"
    except Exception:
        return "0"


def fmt_pct(v: Any, digits: int = 1) -> str:
    try:
        return f"{float(v or 0):.{digits}f}%"
    except Exception:
        return f"{0:.{digits}f}%"


def fmt_money(v: Any) -> str:
    try:
        return f"₩{int(round(float(v or 0))):,}"
    except Exception:
        return "₩0"


def fmt_date(v: Any) -> str:
    if v is None or v == "":
        return ""
    if isinstance(v, dt.datetime):
        return v.astimezone(KST).strftime("%Y-%m-%d")
    if isinstance(v, dt.date):
        return v.strftime("%Y-%m-%d")
    s = str(v)
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return dt.datetime.strptime(s[:19], fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    return s[:10]


def canonical_bucket(channel_group: Any, first_source: Any = None, medium: Any = None, campaign: Any = None) -> str:
    raw = str(channel_group or "").strip()
    if raw:
        norm = raw.lower()
        mapping = {
            "awareness": "Awareness",
            "paid ad": "Paid Ad",
            "organic traffic": "Organic Traffic",
            "official sns": "Official SNS",
            "owned channel": "Owned Channel",
            "direct": "Direct",
            "unknown": "Unknown",
            "etc": "etc",
        }
        if norm in mapping:
            return mapping[norm]
        return raw

    src = f"{first_source or ''} {medium or ''} {campaign or ''}".lower()
    if any(x in src for x in ["email", "edm", "kakao", "lms"]):
        return "Owned Channel"
    if any(x in src for x in ["google", "meta", "facebook", "naver", "criteo", "display", "banner", "cpc"]):
        return "Paid Ad"
    if any(x in src for x in ["instagram", "ig", "story", "social"]):
        return "Official SNS"
    if any(x in src for x in ["organic", "referral", "search"]):
        return "Organic Traffic"
    return "Unknown"


def first_existing(df: pd.DataFrame, candidates: List[str], default: str = "") -> str:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    return default


def safe_series(df: pd.DataFrame, candidates: List[str], default: Any = None) -> pd.Series:
    col = first_existing(df, candidates)
    if col:
        return df[col]
    return pd.Series([default] * len(df), index=df.index)


def to_numeric_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(0)


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]

    out["event_date_norm"] = pd.to_datetime(safe_series(out, ["event_date", "date"], None), errors="coerce").dt.date
    if out["event_date_norm"].isna().all():
        out["event_date_norm"] = YESTERDAY_KST

    out["member_id_norm"] = safe_series(out, ["member_id", "memberid", "member_no", "memberno"], "").astype(str).str.strip()
    out["user_id_norm"] = safe_series(out, ["user_id", "userid"], "").astype(str).str.strip()
    out["phone_norm"] = safe_series(out, ["mobile_phone", "phone", "cellphone", "member_phone", "mobile"], "").astype(str).str.strip()

    out["channel_group_norm"] = [
        canonical_bucket(cg, fs, None, camp)
        for cg, fs, camp in zip(
            safe_series(out, ["channel_group", "channel_group_enhanced"], ""),
            safe_series(out, ["first_source", "latest_source"], ""),
            safe_series(out, ["first_campaign", "latest_campaign", "session_campaign"], ""),
        )
    ]

    out["sessions_norm"] = to_numeric_series(safe_series(out, ["total_sessions", "sessions"], 0))
    out["orders_norm"] = to_numeric_series(safe_series(out, ["order_count", "orders"], 0))
    out["revenue_norm"] = to_numeric_series(safe_series(out, ["total_revenue", "revenue"], 0))
    out["signup_norm"] = to_numeric_series(safe_series(out, ["signup_yn"], 0))
    out["purchase_norm"] = to_numeric_series(safe_series(out, ["purchase_yn"], 0))
    out["pageviews_norm"] = to_numeric_series(safe_series(out, ["total_pageviews", "pageviews"], 0))
    out["product_view_norm"] = to_numeric_series(safe_series(out, ["product_view_count"], 0))
    out["add_to_cart_norm"] = to_numeric_series(safe_series(out, ["add_to_cart_count"], 0))

    out["age_norm"] = to_numeric_series(safe_series(out, ["age"], None))
    age_band = safe_series(out, ["age_band"], "")
    derived_age_band = pd.cut(
        out["age_norm"],
        bins=[0, 19, 29, 39, 49, 59, 200],
        labels=["10s", "20s", "30s", "40s", "50s", "60+"],
        right=True,
    ).astype(object)
    out["age_band_norm"] = age_band.where(age_band.astype(str).str.strip() != "", derived_age_band.fillna("UNKNOWN")).astype(str)

    gender = safe_series(out, ["gender", "member_gender_raw", "member_gender"], "UNKNOWN").astype(str).str.upper()
    gender = gender.replace({"1": "MALE", "2": "FEMALE", "0": "UNKNOWN", "M": "MALE", "F": "FEMALE"})
    out["gender_norm"] = gender

    out["last_category_norm"] = safe_series(out, ["last_category", "top_category", "preferred_category"], "(not set)").astype(str)
    out["top_category_norm"] = safe_series(out, ["top_category", "preferred_category", "last_category"], "(not set)").astype(str)
    out["top_product_norm"] = safe_series(out, ["top_product", "preferred_product", "first_purchase_product"], "(not set)").astype(str)
    out["campaign_norm"] = safe_series(out, ["session_campaign", "first_campaign", "latest_campaign"], "(not set)").astype(str)
    out["recommended_message_norm"] = safe_series(out, ["recommended_message"], "GENERAL").astype(str)
    out["first_source_norm"] = safe_series(out, ["first_source", "latest_source"], "(not set)").astype(str)
    out["last_order_date_norm"] = safe_series(out, ["last_order_date"], "").map(fmt_date)

    flag_map = {
        "is_non_buyer_norm": ["is_non_buyer"],
        "is_cart_abandon_norm": ["is_cart_abandon"],
        "is_high_intent_norm": ["is_high_intent"],
        "is_repeat_buyer_norm": ["is_repeat_buyer"],
        "is_dormant_norm": ["is_dormant"],
        "is_vip_norm": ["is_vip"],
    }
    for target, candidates in flag_map.items():
        out[target] = to_numeric_series(safe_series(out, candidates, 0)).astype(int)

    return out


# ------------------------------------------------------------------
# data load
# ------------------------------------------------------------------
def _sample_json_for_period(base_path: str, key: str) -> Optional[Path]:
    if not base_path:
        return None
    p = Path(base_path)
    if p.is_file() and "{period}" not in base_path:
        return p
    if "{period}" in base_path:
        cand = Path(base_path.format(period=key))
        return cand if cand.exists() else None
    return None


def get_bq_client() -> "bigquery.Client":
    if bigquery is None:
        raise RuntimeError("google-cloud-bigquery is not installed")
    return bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)


def fetch_rows_from_bq(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    client = get_bq_client()
    sql = f"""
    SELECT *
    FROM `{BASE_TABLE}`
    WHERE DATE(event_date) BETWEEN @start_date AND @end_date
       OR SAFE_CAST(event_date AS DATE) BETWEEN @start_date AND @end_date
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date.isoformat()),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date.isoformat()),
        ]
    )
    return client.query(sql, job_config=job_config, location=BQ_LOCATION).to_dataframe()


def load_bundle_for_period(period_key: str, start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    sample_path = _sample_json_for_period(SAMPLE_JSON, period_key)
    if sample_path and sample_path.exists():
        raw = read_json(sample_path)
        if isinstance(raw, dict):
            for k in ["rows", "data", "bundle", "records"]:
                if k in raw and isinstance(raw[k], list):
                    return pd.DataFrame(raw[k])
        if isinstance(raw, list):
            return pd.DataFrame(raw)
        raise RuntimeError(f"Unsupported sample json format: {sample_path}")
    return fetch_rows_from_bq(start_date, end_date)


# ------------------------------------------------------------------
# aggregations
# ------------------------------------------------------------------
def period_date_range(days: int) -> Tuple[dt.date, dt.date]:
    end_date = YESTERDAY_KST
    start_date = end_date - dt.timedelta(days=max(1, days) - 1)
    return start_date, end_date


def build_distribution(rows: pd.DataFrame, key: str, top_n: int = 5) -> List[dict]:
    if rows.empty or key not in rows.columns:
        return []
    s = rows[key].fillna("UNKNOWN").astype(str)
    vc = s.value_counts(dropna=False)
    total = int(vc.sum())
    result = []
    for label, count in vc.head(top_n).items():
        result.append({"label": str(label), "count": int(count), "share_pct": round((count / total * 100) if total else 0, 1)})
    return result


def build_channel_snapshot(df: pd.DataFrame) -> List[dict]:
    if df.empty:
        return []
    g = (
        df.groupby("channel_group_norm", dropna=False)
        .agg(sessions=("sessions_norm", "sum"), orders=("orders_norm", "sum"), revenue=("revenue_norm", "sum"))
        .reset_index()
    )
    g["bucket_order"] = g["channel_group_norm"].map({v: i for i, v in enumerate(CHANNEL_BUCKET_ORDER)}).fillna(999)
    g = g.sort_values(["bucket_order", "channel_group_norm"]).drop(columns=["bucket_order"])
    return [
        {
            "bucket": r["channel_group_norm"],
            "sessions": int(round(r["sessions"])),
            "orders": int(round(r["orders"])),
            "revenue": float(r["revenue"]),
        }
        for _, r in g.iterrows()
    ]


def build_channel_daily_table(df: pd.DataFrame) -> List[dict]:
    if df.empty:
        return []
    g = (
        df.groupby(["event_date_norm", "channel_group_norm"], dropna=False)
        .agg(sessions=("sessions_norm", "sum"), signups=("signup_norm", "sum"), buyers=("purchase_norm", "sum"), orders=("orders_norm", "sum"), revenue=("revenue_norm", "sum"))
        .reset_index()
        .sort_values(["event_date_norm", "channel_group_norm"], ascending=[False, True])
    )
    out = []
    for _, r in g.iterrows():
        out.append(
            {
                "date": fmt_date(r["event_date_norm"]),
                "bucket": r["channel_group_norm"],
                "sessions": int(round(r["sessions"])),
                "signups": int(round(r["signups"])),
                "buyers": int(round(r["buyers"])),
                "orders": int(round(r["orders"])),
                "revenue": float(r["revenue"]),
            }
        )
    return out


def build_operator_actions(df: pd.DataFrame) -> List[dict]:
    total_sessions = int(df["sessions_norm"].sum())
    total_signups = int(df["signup_norm"].sum())
    total_buyers = int(df["purchase_norm"].sum())

    snapshot = pd.DataFrame(build_channel_snapshot(df))
    paid_share = 0.0
    if not snapshot.empty and total_sessions:
        paid_sessions = float(snapshot.loc[snapshot["bucket"] == "Paid Ad", "sessions"].sum())
        paid_share = paid_sessions / total_sessions * 100

    sign_cvr = total_signups / total_sessions * 100 if total_sessions else 0
    buy_cvr = total_buyers / total_sessions * 100 if total_sessions else 0

    return [
        {
            "title": "Paid 비중 관리",
            "value": fmt_pct(paid_share),
            "text": "Paid Ad 세션 비중이 너무 커지면 채널 믹스 리스크가 커집니다. Organic / Owned 보강 필요 여부를 같이 보세요.",
        },
        {
            "title": "가입 전환 체크",
            "value": fmt_pct(sign_cvr),
            "text": "세션 대비 가입 전환율 기준입니다. 메인 진입 랜딩과 회원가입 메시지/혜택 영역을 우선 확인하면 좋습니다.",
        },
        {
            "title": "구매 전환 체크",
            "value": fmt_pct(buy_cvr),
            "text": "세션 대비 구매 전환율 기준입니다. PDP, 가격/혜택, 재방문 유도 메시지 보강 우선순위를 같이 볼 수 있습니다.",
        },
    ]


def build_user_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    sort_cols = ["event_date_norm", "revenue_norm", "orders_norm", "sessions_norm"]
    sort_cols = [c for c in sort_cols if c in df.columns]
    user_key = df["user_id_norm"].where(df["user_id_norm"] != "", df["member_id_norm"])
    dedup = df.assign(__user_key=user_key).sort_values(sort_cols, ascending=[False] * len(sort_cols)).drop_duplicates("__user_key", keep="first")
    return dedup.drop(columns=["__user_key"])


def _top_label(df: pd.DataFrame, col: str) -> str:
    if df.empty or col not in df.columns:
        return "(not set)"
    vc = df[col].fillna("(not set)").astype(str).value_counts()
    return str(vc.index[0]) if len(vc) else "(not set)"


def build_non_buyer_block(user_df: pd.DataFrame) -> dict:
    nb = user_df[(user_df["is_non_buyer_norm"] == 1) | ((user_df["orders_norm"] <= 0) & (user_df["member_id_norm"] != ""))].copy()
    return {
        "summary": {
            "members": int(nb["member_id_norm"].nunique()),
            "sessions": int(nb["sessions_norm"].sum()),
            "avg_pageviews": round(float(nb["pageviews_norm"].mean()), 1) if not nb.empty else 0.0,
            "top_category": _top_label(nb, "last_category_norm"),
        },
        "age_dist": build_distribution(nb, "age_band_norm"),
        "gender_dist": build_distribution(nb, "gender_norm"),
        "category_dist": build_distribution(nb, "last_category_norm"),
        "rows": nb[[c for c in [
            "member_id_norm", "phone_norm", "user_id_norm", "channel_group_norm", "first_source_norm", "campaign_norm",
            "last_category_norm", "top_product_norm", "sessions_norm", "pageviews_norm", "add_to_cart_norm", "last_order_date_norm"
        ] if c in nb.columns]].rename(columns={
            "member_id_norm": "member_id",
            "phone_norm": "phone",
            "user_id_norm": "user_id",
            "channel_group_norm": "channel_group",
            "first_source_norm": "first_source",
            "campaign_norm": "campaign",
            "last_category_norm": "last_category",
            "top_product_norm": "top_product",
            "sessions_norm": "sessions",
            "pageviews_norm": "pageviews",
            "add_to_cart_norm": "add_to_cart_count",
            "last_order_date_norm": "last_order_date",
        }).fillna("").to_dict(orient="records"),
    }


def build_buyer_block(user_df: pd.DataFrame) -> dict:
    b = user_df[(user_df["purchase_norm"] > 0) | (user_df["orders_norm"] > 0) | (user_df["revenue_norm"] > 0)].copy()
    revenue = float(b["revenue_norm"].sum())
    buyers = int(max(b["member_id_norm"].nunique(), b["user_id_norm"].replace("", pd.NA).nunique()))
    return {
        "summary": {
            "buyers": buyers,
            "revenue": revenue,
            "aov": revenue / float(b["orders_norm"].sum()) if float(b["orders_norm"].sum()) else 0,
            "top_campaign": _top_label(b, "campaign_norm"),
            "top_category": _top_label(b, "top_category_norm"),
            "top_product": _top_label(b, "top_product_norm"),
        },
        "age_dist": build_distribution(b, "age_band_norm"),
        "gender_dist": build_distribution(b, "gender_norm"),
        "rows": b[[c for c in [
            "member_id_norm", "user_id_norm", "phone_norm", "channel_group_norm", "campaign_norm", "top_category_norm", "top_product_norm", "orders_norm", "revenue_norm", "last_order_date_norm"
        ] if c in b.columns]].rename(columns={
            "member_id_norm": "member_id",
            "user_id_norm": "user_id",
            "phone_norm": "phone",
            "channel_group_norm": "channel_group",
            "campaign_norm": "campaign",
            "top_category_norm": "top_category",
            "top_product_norm": "top_product",
            "orders_norm": "orders",
            "revenue_norm": "revenue",
            "last_order_date_norm": "last_order_date",
        }).fillna("").sort_values(["revenue", "orders"], ascending=[False, False]).to_dict(orient="records"),
    }


def build_product_block(df: pd.DataFrame) -> dict:
    category_dist = build_distribution(df[df["top_category_norm"] != "(not set)"], "top_category_norm", top_n=8)
    g = (
        df.groupby(["channel_group_norm", "top_product_norm"], dropna=False)
        .agg(buyers=("purchase_norm", "sum"), revenue=("revenue_norm", "sum"))
        .reset_index()
        .sort_values(["revenue", "buyers"], ascending=[False, False])
    )
    channel_product = [
        {
            "channel": r["channel_group_norm"],
            "product": r["top_product_norm"],
            "buyers": int(round(r["buyers"])),
            "revenue": float(r["revenue"]),
        }
        for _, r in g.head(20).iterrows()
    ]
    top_products = (
        df.groupby(["top_product_norm", "top_category_norm"], dropna=False)
        .agg(buyers=("purchase_norm", "sum"), revenue=("revenue_norm", "sum"))
        .reset_index()
        .sort_values(["revenue", "buyers"], ascending=[False, False])
    )
    products = [
        {
            "product": r["top_product_norm"],
            "category": r["top_category_norm"],
            "buyers": int(round(r["buyers"])),
            "revenue": float(r["revenue"]),
        }
        for _, r in top_products.head(12).iterrows()
    ]
    return {"category_dist": category_dist, "channel_product": channel_product, "products": products}


def build_target_block(user_df: pd.DataFrame) -> dict:
    rows = []
    for seg in SEGMENT_ORDER:
        flag_col = f"is_{seg}_norm"
        if flag_col not in user_df.columns:
            continue
        sdf = user_df[user_df[flag_col] == 1].copy()
        if sdf.empty:
            continue
        rows.extend(
            sdf.assign(segment=seg)[[
                "segment", "member_id_norm", "phone_norm", "channel_group_norm", "first_source_norm", "campaign_norm",
                "top_category_norm", "top_product_norm", "recommended_message_norm", "revenue_norm", "last_order_date_norm"
            ]].rename(columns={
                "member_id_norm": "member_id",
                "phone_norm": "phone",
                "channel_group_norm": "channel_group",
                "first_source_norm": "first_source",
                "campaign_norm": "campaign",
                "top_category_norm": "preferred_category",
                "top_product_norm": "preferred_product",
                "recommended_message_norm": "recommended_message",
                "revenue_norm": "total_revenue",
                "last_order_date_norm": "last_order_date",
            }).fillna("").to_dict(orient="records")
        )

    seg_cards = []
    rdf = pd.DataFrame(rows) if rows else pd.DataFrame()
    for seg in SEGMENT_ORDER:
        sdf = rdf[rdf["segment"] == seg].copy() if not rdf.empty else pd.DataFrame()
        if sdf.empty:
            continue
        seg_cards.append(
            {
                "segment": seg,
                "label": SEGMENT_LABELS.get(seg, seg),
                "count": int(sdf["member_id"].nunique()),
                "top_channel": _top_label(sdf, "channel_group"),
                "top_category": _top_label(sdf, "preferred_category"),
                "top_message": _top_label(sdf, "recommended_message"),
            }
        )
    return {"cards": seg_cards, "rows": rows}


def build_latest_day_summary(df: pd.DataFrame) -> List[str]:
    if df.empty:
        return []
    latest_date = max([d for d in df["event_date_norm"].dropna().tolist()] or [YESTERDAY_KST])
    latest = df[df["event_date_norm"] == latest_date].copy()
    sessions = int(latest["sessions_norm"].sum())
    signups = int(latest["signup_norm"].sum())
    buyers = int(latest["purchase_norm"].sum())
    top_channel = _top_label(latest, "channel_group_norm")
    top_category = _top_label(latest[latest["revenue_norm"] > 0], "top_category_norm")
    return [
        f"최근 선택 가능 일자 기준 최신 데이터는 {fmt_date(latest_date)}입니다.",
        f"해당 일자 세션 {fmt_int(sessions)} / 가입 {fmt_int(signups)} / 구매자 {fmt_int(buyers)} 흐름입니다.",
        f"주요 채널은 {top_channel}, 매출 기여 상위 카테고리는 {top_category}입니다.",
    ]


def build_bundle(df: pd.DataFrame, start_date: dt.date, end_date: dt.date, period_key: str, period_label: str) -> dict:
    df = normalize_dataframe(df)
    df = df[(df["event_date_norm"] >= start_date) & (df["event_date_norm"] <= end_date)].copy()
    user_df = build_user_rows(df)

    total_sessions = int(df["sessions_norm"].sum())
    total_orders = int(df["orders_norm"].sum())
    total_revenue = float(df["revenue_norm"].sum())
    total_signups = int(df["signup_norm"].sum())
    buyers = int(user_df[(user_df["purchase_norm"] > 0) | (user_df["orders_norm"] > 0) | (user_df["revenue_norm"] > 0)]["member_id_norm"].replace("", pd.NA).nunique())

    non_buyer = build_non_buyer_block(user_df)
    buyer = build_buyer_block(user_df)
    product = build_product_block(user_df)
    target = build_target_block(user_df)

    return {
        "generated_at": dt.datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST"),
        "period_key": period_key,
        "period_label": period_label,
        "date_range": {"start": start_date.isoformat(), "end": end_date.isoformat()},
        "latest_summary": build_latest_day_summary(df),
        "overview": {
            "sessions": total_sessions,
            "orders": total_orders,
            "revenue": total_revenue,
            "signups": total_signups,
            "buyers": buyers,
            "members": int(user_df["member_id_norm"].replace("", pd.NA).nunique()),
        },
        "total_view": {
            "channel_snapshot": build_channel_snapshot(df),
            "channel_daily_table": build_channel_daily_table(df),
            "operator_actions": build_operator_actions(df),
        },
        "user_view": {
            "non_buyer": non_buyer,
            "buyer": buyer,
            "product": product,
            "target": target,
        },
    }


# ------------------------------------------------------------------
# export files
# ------------------------------------------------------------------
def export_excel_files(bundle: dict, period_key: str) -> dict:
    ensure_dir(DOWNLOAD_DIR)
    links = {}

    nb_rows = bundle.get("user_view", {}).get("non_buyer", {}).get("rows", [])
    if nb_rows:
        nb_path = DOWNLOAD_DIR / f"member_funnel_{period_key}_non_buyer.xlsx"
        pd.DataFrame(nb_rows)[[c for c in ["member_id", "phone", "user_id", "channel_group", "campaign", "last_category", "top_product"] if c in pd.DataFrame(nb_rows).columns]].to_excel(nb_path, index=False)
        links["non_buyer"] = os.path.relpath(nb_path, OUT_DIR).replace("\\", "/")

    tgt_rows = bundle.get("user_view", {}).get("target", {}).get("rows", [])
    if tgt_rows:
        tgt_path = DOWNLOAD_DIR / f"member_funnel_{period_key}_target_segments.xlsx"
        pd.DataFrame(tgt_rows)[[c for c in ["segment", "member_id", "phone", "channel_group", "campaign", "preferred_category", "preferred_product", "recommended_message"] if c in pd.DataFrame(tgt_rows).columns]].to_excel(tgt_path, index=False)
        links["target"] = os.path.relpath(tgt_path, OUT_DIR).replace("\\", "/")

    bundle["downloads"] = links
    return links


# ------------------------------------------------------------------
# HTML pieces
# ------------------------------------------------------------------
def pills_html(items: List[dict], style: str = "default") -> str:
    if not items:
        return '<div class="empty-note">데이터가 없습니다.</div>'
    rows = []
    max_share = max([float(x.get("share_pct", 0) or 0) for x in items] or [0])
    for item in items:
        pct = float(item.get("share_pct", 0) or 0)
        width = (pct / max_share * 100) if max_share else 0
        rows.append(
            f'''
            <div class="dist-row {style}">
              <div class="dist-label">{esc(item.get("label"))}</div>
              <div class="dist-bar"><span style="width:{width:.1f}%"></span></div>
              <div class="dist-value">{fmt_pct(pct)}</div>
            </div>
            '''
        )
    return "".join(rows)


def table_html(rows: List[dict], columns: List[Tuple[str, str]], numeric: Optional[set] = None) -> str:
    numeric = numeric or set()
    head = "".join(f'<th class="{"num" if key in numeric else ""}">{esc(label)}</th>' for key, label in columns)
    body = []
    if not rows:
        colspan = len(columns)
        body.append(f'<tr><td colspan="{colspan}" class="muted">데이터가 없습니다.</td></tr>')
    else:
        for r in rows:
            tds = []
            for key, _ in columns:
                val = r.get(key, "")
                if key in numeric:
                    if "revenue" in key or key == "aov":
                        val = fmt_money(val)
                    else:
                        val = fmt_int(val)
                tds.append(f'<td class="{"num" if key in numeric else ""}">{esc(val)}</td>')
            body.append(f"<tr>{''.join(tds)}</tr>")
    return f'<div class="table-wrap"><table class="data-table"><thead><tr>{head}</tr></thead><tbody>{"".join(body)}</tbody></table></div>'


def render_page(bundle: dict, preset: dict) -> str:
    overview = bundle["overview"]
    total_view = bundle["total_view"]
    user_view = bundle["user_view"]
    downloads = bundle.get("downloads", {})

    period_nav = "".join(
        f'<a class="period-chip {"active" if p["key"] == preset["key"] else ""}" href="{esc(p["filename"])}">{esc(p["label"])}<\/a>'
        for p in PERIOD_PRESETS
    )

    latest_summary = "".join(f'<li>{esc(x)}</li>' for x in bundle.get("latest_summary", []))
    latest_end = bundle["date_range"]["end"]

    non_buyer = user_view["non_buyer"]
    buyer = user_view["buyer"]
    product = user_view["product"]
    target = user_view["target"]

    channel_snapshot_table = table_html(
        total_view["channel_snapshot"],
        [("bucket", "Bucket"), ("sessions", "Sessions"), ("orders", "Orders"), ("revenue", "Revenue")],
        {"sessions", "orders", "revenue"},
    )
    channel_daily_table = table_html(
        total_view["channel_daily_table"],
        [("date", "Date"), ("bucket", "Bucket"), ("sessions", "Sessions"), ("signups", "Signups"), ("buyers", "Buyers"), ("orders", "Orders"), ("revenue", "Revenue")],
        {"sessions", "signups", "buyers", "orders", "revenue"},
    )
    non_buyer_table = table_html(
        non_buyer["rows"][:120],
        [("member_id", "Member ID"), ("phone", "Phone"), ("user_id", "USER_ID"), ("channel_group", "Channel"), ("campaign", "Campaign"), ("last_category", "Last Category"), ("top_product", "Top Product")],
        set(),
    )
    buyer_table = table_html(
        buyer["rows"][:120],
        [("member_id", "Member ID"), ("user_id", "USER_ID"), ("channel_group", "Channel"), ("campaign", "Campaign"), ("top_category", "Top Category"), ("top_product", "Top Product"), ("orders", "Orders"), ("revenue", "Revenue")],
        {"orders", "revenue"},
    )
    channel_product_table = table_html(
        product["channel_product"],
        [("channel", "Channel"), ("product", "Product"), ("buyers", "Buyers"), ("revenue", "Revenue")],
        {"buyers", "revenue"},
    )

    target_cards = "".join(
        f'''
        <div class="mini-panel">
          <div class="mini-kicker">{esc(card["label"])}</div>
          <div class="mini-main">{fmt_int(card["count"])} 명</div>
          <div class="mini-copy">Top Channel · {esc(card["top_channel"])}</div>
          <div class="mini-copy">Top Category · {esc(card["top_category"])}</div>
          <div class="mini-copy">Message · {esc(card["top_message"])}</div>
        </div>
        '''
        for card in target["cards"][:6]
    ) or '<div class="empty-note">타겟 대상자가 없습니다.</div>'

    product_cards = "".join(
        f'''
        <div class="mini-panel">
          <div class="mini-kicker">{esc(row["category"])}</div>
          <div class="mini-main">{esc(row["product"])}</div>
          <div class="mini-copy">Buyers · {fmt_int(row["buyers"])} / Revenue · {fmt_money(row["revenue"])} </div>
        </div>
        '''
        for row in product["products"][:6]
    ) or '<div class="empty-note">상품 데이터가 없습니다.</div>'

    action_cards = "".join(
        f'''
        <div class="action-card">
          <div class="mini-kicker">{esc(card["title"])}</div>
          <div class="action-value">{esc(card["value"])}</div>
          <div class="mini-copy">{esc(card["text"])}</div>
        </div>
        '''
        for card in total_view["operator_actions"]
    )

    return f'''<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Member Funnel</title>
<style>
:root {{
  --bg:#eef3f8;
  --bg2:#f7fafc;
  --ink:#0f172a;
  --muted:#64748b;
  --line:#dbe4ee;
  --card:#ffffff;
  --navy:#04122d;
  --navy2:#0c1f49;
  --blue:#2563eb;
  --blue2:#4f8cff;
  --green:#059669;
  --shadow:0 16px 40px rgba(15,23,42,.08);
  --radius:24px;
}}
*{{box-sizing:border-box}}
body{{margin:0;font-family:Inter, system-ui, -apple-system, Segoe UI, Noto Sans KR, sans-serif;color:var(--ink);background:linear-gradient(180deg,var(--bg2),var(--bg));}}
a{{color:inherit;text-decoration:none}}
.wrap{{max-width:1540px;margin:0 auto;padding:18px 22px 60px}}
.hero{{background:linear-gradient(135deg,#020817,#071b45 60%,#0f3fb1);color:#fff;border-radius:28px;padding:18px 18px 20px;box-shadow:0 20px 50px rgba(2,8,23,.22)}}
.hero-grid{{display:grid;grid-template-columns:1.35fr .95fr;gap:14px}}
.badge{{display:inline-flex;align-items:center;padding:6px 10px;border-radius:999px;background:rgba(255,255,255,.08);font-size:11px;font-weight:900;letter-spacing:.12em;text-transform:uppercase}}
.hero h1{{margin:14px 0 8px;font-size:48px;line-height:1.02;letter-spacing:-.04em}}
.hero p{{margin:0;color:rgba(255,255,255,.8);font-size:14px;font-weight:700;line-height:1.6;max-width:860px}}
.period-row,.hero-links{{display:flex;gap:8px;flex-wrap:wrap;margin-top:16px}}
.period-chip{{padding:10px 14px;border-radius:999px;background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.12);font-size:12px;font-weight:900}}
.period-chip.active{{background:#fff;color:#071b45}}
.hero-stat-grid{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}}
.hero-stat{{background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.12);border-radius:22px;padding:16px}}
.kicker{{font-size:11px;font-weight:900;letter-spacing:.14em;text-transform:uppercase;color:var(--muted)}}
.hero .kicker{{color:rgba(255,255,255,.72)}}
.hero-value{{font-size:30px;font-weight:950;letter-spacing:-.03em;margin-top:10px}}
.hero-sub{{margin-top:6px;color:rgba(255,255,255,.78);font-size:12px;font-weight:800;line-height:1.5}}
.filters{{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;margin-top:18px;padding:16px 18px;background:rgba(255,255,255,.88);border:1px solid var(--line);border-radius:24px;box-shadow:var(--shadow)}}
.filters-left{{display:flex;gap:8px;flex-wrap:wrap}}
.tab-btn{{padding:10px 16px;border-radius:999px;border:1px solid var(--line);background:#fff;color:#334155;font-size:13px;font-weight:900;cursor:pointer}}
.tab-btn.active{{background:#0f172a;color:#fff;border-color:#0f172a}}
.date-tools{{display:flex;gap:10px;align-items:center;flex-wrap:wrap}}
.date-tools input{{height:42px;border:1px solid var(--line);border-radius:14px;padding:0 12px;background:#fff;color:#334155;font-weight:800}}
.date-tools button{{height:42px;border:0;border-radius:14px;background:#0f172a;color:#fff;padding:0 14px;font-weight:900;cursor:pointer}}
.summary-card{{margin-top:18px;background:#fff;border:1px solid var(--line);border-radius:24px;padding:18px 18px;box-shadow:var(--shadow)}}
.summary-card ul{{margin:10px 0 0 18px;padding:0;color:#334155;font-weight:800;line-height:1.8}}
.panel{{display:none;margin-top:18px}}
.panel.active{{display:block}}
.grid-4{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px}}
.grid-3{{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px}}
.grid-2{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}}
.card{{background:#fff;border:1px solid var(--line);border-radius:24px;padding:18px;box-shadow:var(--shadow)}}
.kpi{{font-size:34px;font-weight:950;letter-spacing:-.04em;margin-top:8px}}
.kpi-sub{{margin-top:8px;color:var(--muted);font-size:12px;font-weight:800;line-height:1.55}}
.section-title{{font-size:13px;font-weight:900;letter-spacing:.16em;text-transform:uppercase;color:#64748b;margin-bottom:12px}}
.section-head{{display:flex;align-items:end;justify-content:space-between;gap:10px;flex-wrap:wrap;margin:28px 0 12px}}
.section-head h2{{margin:0;font-size:34px;letter-spacing:-.03em}}
.soft-note{{font-size:12px;font-weight:800;color:#64748b}}
.dist-list{{display:grid;gap:10px}}
.dist-row{{display:grid;grid-template-columns:120px 1fr 64px;gap:10px;align-items:center}}
.dist-label{{font-size:13px;font-weight:900;color:#1e293b;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
.dist-bar{{height:10px;border-radius:999px;background:#edf2f7;overflow:hidden}}
.dist-bar span{{display:block;height:100%;background:linear-gradient(90deg,#2563eb,#60a5fa);border-radius:999px}}
.dist-value{{text-align:right;font-size:12px;font-weight:900;color:#334155}}
.table-wrap{{overflow:auto}}
.data-table{{width:100%;border-collapse:collapse;min-width:820px}}
th{{text-align:left;padding:12px 10px;font-size:11px;text-transform:uppercase;letter-spacing:.12em;color:#64748b;border-bottom:1px solid var(--line)}}
td{{padding:12px 10px;border-bottom:1px solid #edf2f7;font-size:13px;font-weight:800;color:#1e293b}}
th.num,td.num{{text-align:right}}
.action-card{{background:#fff;border:1px solid var(--line);border-radius:22px;padding:18px;box-shadow:var(--shadow)}}
.action-value{{font-size:28px;font-weight:950;letter-spacing:-.03em;margin-top:8px}}
.mini-grid{{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px}}
.mini-panel{{background:#fff;border:1px solid var(--line);border-radius:22px;padding:16px;box-shadow:var(--shadow)}}
.mini-kicker{{font-size:11px;font-weight:900;letter-spacing:.12em;text-transform:uppercase;color:#64748b}}
.mini-main{{font-size:22px;font-weight:950;letter-spacing:-.03em;margin-top:8px;line-height:1.25}}
.mini-copy{{margin-top:8px;color:#475569;font-size:12px;font-weight:800;line-height:1.55}}
.download-row{{display:flex;gap:10px;flex-wrap:wrap}}
.download-btn{{display:inline-flex;align-items:center;justify-content:center;background:#0f172a;color:#fff;padding:10px 14px;border-radius:14px;font-size:12px;font-weight:900}}
.empty-note{{padding:18px;border-radius:18px;background:#fff;border:1px dashed var(--line);color:#64748b;font-size:13px;font-weight:800}}
.hero-link{{display:inline-flex;align-items:center;justify-content:center;padding:10px 14px;border-radius:14px;background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.12);font-size:12px;font-weight:900}}
@media (max-width:1280px){{.hero-grid,.grid-4,.grid-3,.grid-2,.mini-grid{{grid-template-columns:1fr 1fr}}}}
@media (max-width:860px){{.hero-grid,.grid-4,.grid-3,.grid-2,.mini-grid{{grid-template-columns:1fr}} .hero h1{{font-size:36px}} .dist-row{{grid-template-columns:92px 1fr 54px}} .wrap{{padding:16px}} }}
</style>
</head>
<body>
<div class="wrap">
  <section class="hero">
    <div class="hero-grid">
      <div>
        <span class="badge">EXTERNAL SIGNAL STYLE · CRM FUNNEL</span>
        <h1>Member Funnel</h1>
        <p>External Signal 리포트 톤을 그대로 가져와서, 채널 · 고객 · 상품 · 타겟 액션이 한 화면에서 또렷하게 읽히도록 재정렬했습니다.</p>
        <div class="period-row">{period_nav}</div>
        <div class="hero-links">
          <span class="hero-link">{esc(bundle['period_label'])} · {esc(bundle['date_range']['start'])} ~ {esc(bundle['date_range']['end'])}</span>
          <span class="hero-link">Updated {esc(bundle['generated_at'])}</span>
          <span class="hero-link">Sign-up → Buyer → Product → Target</span>
        </div>
      </div>
      <div class="hero-stat-grid">
        <div class="hero-stat"><div class="kicker">Sessions</div><div class="hero-value">{fmt_int(overview['sessions'])}</div><div class="hero-sub">공식몰 전체 세션 기준</div></div>
        <div class="hero-stat"><div class="kicker">Orders</div><div class="hero-value">{fmt_int(overview['orders'])}</div><div class="hero-sub">선택 구간 주문 수</div></div>
        <div class="hero-stat"><div class="kicker">Revenue</div><div class="hero-value">{fmt_money(overview['revenue'])}</div><div class="hero-sub">공식몰 전체 매출</div></div>
        <div class="hero-stat"><div class="kicker">Matched Members</div><div class="hero-value">{fmt_int(overview['members'])}</div><div class="hero-sub">USER_ID/CRM 매칭 기준</div></div>
      </div>
    </div>
  </section>

  <section class="filters">
    <div class="filters-left">
      <button class="tab-btn active" data-target="user-view">USER VIEW</button>
      <button class="tab-btn" data-target="total-view">TOTAL VIEW</button>
    </div>
    <div class="date-tools">
      <input type="date" id="start-date" value="{esc(bundle['date_range']['start'])}" min="{esc(bundle['date_range']['start'])}" max="{esc(bundle['date_range']['end'])}">
      <input type="date" id="end-date" value="{esc(bundle['date_range']['end'])}" min="{esc(bundle['date_range']['start'])}" max="{esc(bundle['date_range']['end'])}">
      <button type="button" id="apply-date">Apply</button>
    </div>
  </section>

  <section class="summary-card">
    <div class="section-title">이번 구간 핵심 요약</div>
    <div class="soft-note">선택 가능한 일자 범위 안에서 최신 일자 데이터 기준으로 요약합니다. 현재 최신 반영일은 {esc(latest_end)} 입니다.</div>
    <ul>{latest_summary}</ul>
  </section>

  <section class="panel active" id="user-view">
    <div class="section-head"><div><div class="section-title">USER VIEW</div><h2>행동 데이터 · CRM 액션 뷰</h2></div><div class="download-row">{'<a class="download-btn" href="'+esc(downloads.get('non_buyer',''))+'">Non Buyer Excel</a>' if downloads.get('non_buyer') else ''}{'<a class="download-btn" href="'+esc(downloads.get('target',''))+'">Target Segment Excel</a>' if downloads.get('target') else ''}</div></div>

    <div class="grid-4">
      <div class="card"><div class="kicker">Non Buyer Members</div><div class="kpi">{fmt_int(non_buyer['summary']['members'])}</div><div class="kpi-sub">가입했지만 아직 구매하지 않은 Member 기준</div></div>
      <div class="card"><div class="kicker">Non Buyer Sessions</div><div class="kpi">{fmt_int(non_buyer['summary']['sessions'])}</div><div class="kpi-sub">대상자의 공식몰 세션 합계</div></div>
      <div class="card"><div class="kicker">Buyer Revenue</div><div class="kpi">{fmt_money(buyer['summary']['revenue'])}</div><div class="kpi-sub">구매자 누적 매출</div></div>
      <div class="card"><div class="kicker">Top Campaign</div><div class="kpi">{esc(buyer['summary']['top_campaign'])}</div><div class="kpi-sub">구매자 기준 가장 많이 잡힌 캠페인</div></div>
    </div>

    <div class="section-head"><div><div class="section-title">Non Buyer</div><h2>가입했지만 아직 사지 않은 사람</h2></div></div>
    <div class="grid-2">
      <div class="card"><div class="section-title">AGE 비율</div><div class="dist-list">{pills_html(non_buyer['age_dist'])}</div></div>
      <div class="card"><div class="section-title">GENDER 비율</div><div class="dist-list">{pills_html(non_buyer['gender_dist'])}</div></div>
      <div class="card"><div class="section-title">Last Category 비율</div><div class="dist-list">{pills_html(non_buyer['category_dist'])}</div></div>
      <div class="card"><div class="kicker">Top Category</div><div class="kpi">{esc(non_buyer['summary']['top_category'])}</div><div class="kpi-sub">재방문/첫구매 메시지 우선 카테고리</div></div>
    </div>
    <div class="card">{non_buyer_table}</div>

    <div class="section-head"><div><div class="section-title">Buyer Revenue</div><h2>누가 매출을 만들었는지</h2></div></div>
    <div class="grid-4">
      <div class="card"><div class="kicker">Buyers</div><div class="kpi">{fmt_int(buyer['summary']['buyers'])}</div><div class="kpi-sub">구매자 수</div></div>
      <div class="card"><div class="kicker">Revenue</div><div class="kpi">{fmt_money(buyer['summary']['revenue'])}</div><div class="kpi-sub">구매자 매출</div></div>
      <div class="card"><div class="kicker">AOV</div><div class="kpi">{fmt_money(buyer['summary']['aov'])}</div><div class="kpi-sub">주문당 평균 매출</div></div>
      <div class="card"><div class="kicker">Top Product</div><div class="kpi">{esc(buyer['summary']['top_product'])}</div><div class="kpi-sub">구매자 기준 최다 제품</div></div>
    </div>
    <div class="grid-2">
      <div class="card"><div class="section-title">AGE 비율</div><div class="dist-list">{pills_html(buyer['age_dist'])}</div></div>
      <div class="card"><div class="section-title">GENDER 비율</div><div class="dist-list">{pills_html(buyer['gender_dist'])}</div></div>
    </div>
    <div class="card">{buyer_table}</div>

    <div class="section-head"><div><div class="section-title">Product Insight</div><h2>무슨 상품이 고객을 움직였는지</h2></div></div>
    <div class="grid-2">
      <div class="card"><div class="section-title">Category 비율</div><div class="dist-list">{pills_html(product['category_dist'])}</div></div>
      <div class="card"><div class="section-title">Top Product Focus</div><div class="mini-grid">{product_cards}</div></div>
    </div>
    <div class="card">{channel_product_table}</div>

    <div class="section-head"><div><div class="section-title">지금 바로 액션 가능한 대상자</div><h2>세그먼트별 채널 · 관심상품 · 추천 메시지</h2></div></div>
    <div class="mini-grid">{target_cards}</div>
  </section>

  <section class="panel" id="total-view">
    <div class="section-head"><div><div class="section-title">TOTAL VIEW</div><h2>공식몰 전체 데이터</h2></div></div>
    <div class="grid-4">
      <div class="card"><div class="kicker">Sessions</div><div class="kpi">{fmt_int(overview['sessions'])}</div><div class="kpi-sub">선택 구간 공식몰 전체 세션</div></div>
      <div class="card"><div class="kicker">Signups</div><div class="kpi">{fmt_int(overview['signups'])}</div><div class="kpi-sub">공식몰 전체 가입 수</div></div>
      <div class="card"><div class="kicker">Orders</div><div class="kpi">{fmt_int(overview['orders'])}</div><div class="kpi-sub">주문 수</div></div>
      <div class="card"><div class="kicker">Revenue</div><div class="kpi">{fmt_money(overview['revenue'])}</div><div class="kpi-sub">공식몰 전체 매출</div></div>
    </div>

    <div class="section-head"><div><div class="section-title">Channel Snapshot</div><h2>공식몰 채널 성과</h2></div></div>
    <div class="card">{channel_snapshot_table}</div>

    <div class="section-head"><div><div class="section-title">Channel Daily Table</div><h2>채널별 일자 테이블</h2></div></div>
    <div class="card">{channel_daily_table}</div>

    <div class="section-head"><div><div class="section-title">Operator Actions</div><h2>실행 우선순위 카드</h2></div></div>
    <div class="grid-3">{action_cards}</div>
  </section>
</div>
<script>
(function(){{
  const buttons = Array.from(document.querySelectorAll('.tab-btn'));
  const panels = Array.from(document.querySelectorAll('.panel'));
  function activate(id){{
    buttons.forEach(btn => btn.classList.toggle('active', btn.dataset.target === id));
    panels.forEach(panel => panel.classList.toggle('active', panel.id === id));
  }}
  buttons.forEach(btn => btn.addEventListener('click', () => activate(btn.dataset.target)));
  document.getElementById('apply-date').addEventListener('click', function(){{
    const s = document.getElementById('start-date').value;
    const e = document.getElementById('end-date').value;
    if(!s || !e) return;
    const u = new URL(window.location.href);
    u.searchParams.set('start', s);
    u.searchParams.set('end', e);
    window.location.href = u.toString();
  }});
}})();
</script>
</body>
</html>'''


# ------------------------------------------------------------------
# main
# ------------------------------------------------------------------
def main() -> None:
    ensure_dir(OUT_DIR)
    ensure_dir(DATA_DIR)
    ensure_dir(DOWNLOAD_DIR)

    default_payload = None
    for preset in PERIOD_PRESETS:
        start_date, end_date = period_date_range(preset["days"])
        df = load_bundle_for_period(preset["key"], start_date, end_date)
        bundle = build_bundle(df, start_date, end_date, preset["key"], preset["label"])
        export_excel_files(bundle, preset["key"])

        if WRITE_DATA_CACHE:
            write_json(DATA_DIR / f"{preset['key']}_bundle.json", bundle)

        html_text = render_page(bundle, preset)
        out_path = OUT_DIR / preset["filename"]
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(html_text)

        if preset["is_default"]:
            default_payload = bundle

    if default_payload:
        _write_summary_json(HUB_SUMMARY_DIR, "member_funnel", {
            "title": "Member Funnel",
            "updated_at": default_payload.get("generated_at"),
            "range": default_payload.get("date_range"),
            "sessions": default_payload.get("overview", {}).get("sessions", 0),
            "buyers": default_payload.get("overview", {}).get("buyers", 0),
            "revenue": default_payload.get("overview", {}).get("revenue", 0),
        })

    print(f"✅ Member Funnel generated: {OUT_DIR}")


if __name__ == "__main__":
    main()
