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
WRITE_DATA_CACHE = os.getenv("MEMBER_FUNNEL_WRITE_DATA_CACHE", "true").lower() in {"1","true","yes","y"}
UI_MAX_TABLE_ROWS = int(os.getenv("MEMBER_FUNNEL_UI_MAX_TABLE_ROWS", "300"))
UI_MAX_PRODUCT_ROWS = int(os.getenv("MEMBER_FUNNEL_UI_MAX_PRODUCT_ROWS", "200"))
UI_MAX_TARGET_ROWS = int(os.getenv("MEMBER_FUNNEL_UI_MAX_TARGET_ROWS", "1000"))

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
    camp = str(campaign or "").strip().lower()
    src_mix = f"{first_source or ''} {medium or ''} {campaign or ''}".lower()
    if camp == "benz_running_program_2026_coupon":
        return "Organic Traffic"
    if raw and raw.lower() in mapping and raw.lower() != "direct":
        return mapping[raw.lower()]
    if any(x in src_mix for x in ["benz_running_program_2026_coupon", "organic", "referral", "search"]):
        return "Organic Traffic"
    if any(x in src_mix for x in ["email","edm","kakao","lms", "sms"]): return "Owned Channel"
    if any(x in src_mix for x in ["google","meta","facebook","naver","criteo","display","banner","cpc"]): return "Paid Ad"
    if any(x in src_mix for x in ["instagram","ig","story","social"]): return "Official SNS"
    if raw and raw.lower() in mapping:
        return mapping[raw.lower()]
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
    out["purchase_product_name_norm"] = safe_series(out,["purchase_product_name","top_product_name","first_purchase_product_name","last_purchase_product_name","top_purchased_item_name","product_name_display","product_name","item_name","top_product"],"").map(lambda x: clean_label(x,""))
    out["top_category_norm"] = safe_series(out,["top_category","preferred_category","last_category","top_category_name"],"").map(lambda x: clean_label(x,""))
    out["top_category_norm"] = [x if clean_label(x,"") else classify_category(n) for x,n in zip(out["top_category_norm"], out["purchase_product_name_norm"])]
    out["product_image_norm"] = safe_series(out,["product_image","image_url","image","img_url","thumbnail","thumbnail_url"],"").astype(str).str.strip()
    out["recommended_message_norm"] = safe_series(out,["recommended_message"],"GENERAL").astype(str)
    out["last_order_date_norm"] = safe_series(out,["last_order_date"],"").map(fmt_date)
    out["consent_norm"] = ((to_num(safe_series(out,["is_mailing"],0))>0) | (to_num(safe_series(out,["is_sms"],0))>0) | (to_num(safe_series(out,["is_alimtalk"],0))>0)).astype(int)
    for flag in ["non_buyer","cart_abandon","high_intent","repeat_buyer","dormant","vip"]:
        out[f"is_{flag}_norm"] = to_num(safe_series(out,[f"is_{flag}"],0)).astype(int)
    return out




def classify_category(name: Any) -> str:
    s = clean_label(name, "")
    if not s:
        return "미분류"
    u = s.upper()
    rules = [
        ("BACKPACK", ["BACKPACK", "PACKABLE", "BAG", "BPACK", "배낭", "백팩", "가방"]),
        ("SHOES", ["BOOT", "SHOE", "SANDAL", "SNEAKER", "TRAIL", "등산화", "신발"]),
        ("JACKET", ["JACKET", "PARKA", "WINDBREAKER", "SHELL", "자켓", "바람막이"]),
        ("PADDING", ["DOWN", "PADDED", "PADDING", "패딩"]),
        ("FLEECE", ["FLEECE", "플리스"]),
        ("PANTS", ["PANT", "PANTS", "TROUSER", "SHORT", "팬츠", "바지", "쇼츠"]),
        ("TEE", ["TEE", "T-SHIRT", "TSHIRT", "SHORT SLEEVE", "티셔츠"]),
        ("TOP", ["SHIRT", "HOODIE", "SWEAT", "CREW", "VEST", "LONG SLEEVE", "셔츠", "후디", "맨투맨", "조끼"]),
        ("HAT", ["CAP", "HAT", "BEANIE", "모자", "캡"]),
        ("ACCESSORY", ["SOCK", "GLOVE", "BELT", "WALLET", "양말", "장갑", "벨트"]),
    ]
    for label, tokens in rules:
        if any(tok in u for tok in tokens):
            return label
    return "미분류"

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
    return user_df[(user_df["is_non_buyer_norm"] == 1) | ((user_df["orders_norm"] <= 0) & (user_df["member_id_norm"] != ""))].copy()


def buyer_df(user_df: pd.DataFrame):
    return user_df[(user_df["purchase_norm"] > 0) | (user_df["orders_norm"] > 0) | (user_df["revenue_norm"] > 0)].copy()


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

    def channel_panels(source: pd.DataFrame, mode: str):
        out = {}
        for ch in channel_names(source):
            sdf = source if ch == "ALL" else source[source["channel_group_norm"] == ch].copy()
            revenue = float(sdf["revenue_norm"].sum()) if "revenue_norm" in sdf.columns else 0.0
            orders = float(sdf["orders_norm"].sum()) if "orders_norm" in sdf.columns else 0.0
            products = (
                sdf[sdf["purchase_product_name_norm"] != ""]
                .groupby(["purchase_product_name_norm", "top_category_norm", "product_image_norm"], dropna=False)
                .agg(buyers=("member_id_norm","nunique"), revenue=("revenue_norm","sum"))
                .reset_index().sort_values(["revenue","buyers"], ascending=[False,False])
            ) if not sdf.empty else pd.DataFrame()
            prod_cards = [{"product": clean_label(r.get("purchase_product_name_norm"), "미분류"), "category": clean_label(r.get("top_category_norm"), "미분류"), "buyers": int(r.get("buyers",0)), "revenue": float(r.get("revenue",0)), "image": str(r.get("product_image_norm","") or "")} for _,r in products.head(8).iterrows()]
            if mode == "non_buyer":
                rows = rows_from_df(sdf, {"member_id_norm":"member_id","phone_norm":"phone","user_id_norm":"user_id","channel_group_norm":"channel_group","campaign_display_norm":"campaign","sessions_norm":"sessions","pageviews_norm":"pageviews","consent_norm":"consent"})
                summary = {"members": int(sdf["member_id_norm"].replace("", pd.NA).nunique()), "sessions": int(sdf["sessions_norm"].sum()), "avg_age": avg_age(sdf), "top_channel": top_label(sdf, "channel_group_norm")}
                extra = {"channel_dist": distribution(sdf, "channel_group_norm", 6)}
            elif mode == "buyer":
                rows = rows_from_df(sdf.sort_values(["revenue_norm","orders_norm"], ascending=[False,False]), {"member_id_norm":"member_id","phone_norm":"phone","user_id_norm":"user_id","channel_group_norm":"channel_group","campaign_display_norm":"campaign","purchase_product_name_norm":"purchase_product_name","orders_norm":"orders","revenue_norm":"revenue","consent_norm":"consent"})
                summary = {"buyers": int(max(sdf["member_id_norm"].replace("", pd.NA).nunique(), sdf["user_id_norm"].replace("", pd.NA).nunique())), "revenue": revenue, "aov": revenue/orders if orders else 0.0, "avg_age": avg_age(sdf), "top_product": top_label(sdf, "purchase_product_name_norm")}
                extra = {}
            elif mode == "total":
                rows = rows_from_df(sdf.sort_values(["revenue_norm","orders_norm"], ascending=[False,False]), {"member_id_norm":"member_id","phone_norm":"phone","user_id_norm":"user_id","channel_group_norm":"channel_group","campaign_display_norm":"campaign","purchase_product_name_norm":"purchase_product_name","orders_norm":"orders","revenue_norm":"revenue","consent_norm":"consent"})
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
            f"주요 채널은 {top_label(latest, 'channel_group_norm')}, 매출 기여 상위 상품은 {top_label(latest[latest['revenue_norm']>0], 'purchase_product_name_norm')}입니다.",
        ],
        "overview": {"sessions": int(df['sessions_norm'].sum()), "orders": int(df['orders_norm'].sum()), "revenue": float(df['revenue_norm'].sum()), "signups": int(df['signup_norm'].sum()), "buyers": int(buy['member_id_norm'].replace('', pd.NA).nunique()), "members": int(user['member_id_norm'].replace('', pd.NA).nunique())},
        "user_view": {"non_buyer": channel_panels(nb, 'non_buyer'), "buyer": channel_panels(buy, 'buyer'), "product": channel_panels(buy, 'product'), "target": {"cards": [{"label": SEGMENT_LABELS[k], "count": int(user[user[f'is_{k}_norm'] == 1]['member_id_norm'].replace('', pd.NA).nunique()), "top_channel": top_label(user[user[f'is_{k}_norm'] == 1], 'channel_group_norm'), "top_product": top_label(user[user[f'is_{k}_norm'] == 1], 'purchase_product_name_norm'), "top_message": top_label(user[user[f'is_{k}_norm'] == 1], 'recommended_message_norm', 'GENERAL')} for k in SEGMENT_ORDER if f'is_{k}_norm' in user.columns and not user[user[f'is_{k}_norm'] == 1].empty], "rows": rows_from_df(user[(user[[c for c in user.columns if c.startswith('is_') and c.endswith('_norm')]].sum(axis=1) > 0)], {"member_id_norm":"member_id","phone_norm":"phone","channel_group_norm":"channel_group","campaign_display_norm":"campaign","purchase_product_name_norm":"preferred_product","recommended_message_norm":"recommended_message","consent_norm":"consent"})}},
        "total_view": {"member_overview": channel_panels(members, 'total')},
    }
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

    light["meta"] = {
        "ui_max_table_rows": UI_MAX_TABLE_ROWS,
        "ui_max_product_rows": UI_MAX_PRODUCT_ROWS,
        "ui_max_target_rows": UI_MAX_TARGET_ROWS,
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


def render_page(initial_bundle: dict, preset: dict, all_bundles: dict[str, dict]) -> str:
    period_nav = ''.join(f'<button class="period-chip {"active" if p["key"] == preset["key"] else ""}" data-period="{esc(p["key"])}">{esc(p["label"])} </button>' for p in PERIOD_PRESETS)
    downloads = initial_bundle.get('downloads', {})
    all_json = json.dumps(all_bundles, ensure_ascii=False)
    initial_json = json.dumps(initial_bundle, ensure_ascii=False)
    html_template = """<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Member Funnel</title>
<style>
:root{--bg:#edf2f7;--card:#ffffff;--line:#d9e2ef;--navy:#102a72;--navy2:#2346b5;--text:#111827;--muted:#64748b;--shadow:0 18px 44px rgba(15,23,42,.08)}
*{box-sizing:border-box} body{margin:0;background:linear-gradient(180deg,#f6f8fc,#edf2f7);color:var(--text);font-family:"Noto Sans KR","Apple SD Gothic Neo","Malgun Gothic",sans-serif} .page{max-width:1380px;margin:0 auto;padding:22px} .shell{padding:22px;border-radius:28px;background:rgba(255,255,255,.38);backdrop-filter:blur(8px);border:1px solid rgba(255,255,255,.55)}
.hero{background:linear-gradient(135deg,#132f86 0%,#2141ae 58%,#274fd0 100%);border-radius:30px;padding:26px 32px;color:#fff;box-shadow:0 22px 60px rgba(19,47,134,.28);position:relative;overflow:hidden}.hero:after{content:"";position:absolute;right:-70px;top:-50px;width:280px;height:280px;border-radius:50%;background:radial-gradient(circle,rgba(255,255,255,.16),rgba(255,255,255,0));pointer-events:none}.hero-grid{display:grid;grid-template-columns:1.4fr 1fr;gap:24px;align-items:stretch}.eyebrow{display:inline-flex;padding:8px 14px;border:1px solid rgba(255,255,255,.24);border-radius:999px;font-size:12px;font-weight:900;letter-spacing:.16em;text-transform:uppercase;background:rgba(255,255,255,.08)}h1{margin:20px 0 12px;font-size:58px;line-height:1;font-weight:1000;letter-spacing:-.04em}.hero p{margin:0;font-size:15px;line-height:1.65;font-weight:800;max-width:720px}.hero-meta{display:flex;gap:10px;flex-wrap:wrap;margin-top:24px}.period-chip{display:inline-flex;align-items:center;justify-content:center;height:48px;padding:0 20px;border-radius:999px;border:1px solid rgba(255,255,255,.26);background:transparent;color:#fff;text-decoration:none;font-weight:1000;cursor:pointer;transition:.2s transform,.2s background,.2s opacity}.period-chip:hover{transform:translateY(-1px);background:rgba(255,255,255,.09)}.period-chip.active{background:#fff;color:#102a72} .hero-kpis{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}.hero-stat,.card,.summary-card,.section-box{background:var(--card);border:1px solid var(--line);border-radius:22px;box-shadow:var(--shadow)} .hero-stat{padding:18px 20px;color:var(--text);min-height:112px}.label,.kicker,.section-title{font-size:11px;font-weight:1000;letter-spacing:.18em;text-transform:uppercase;color:var(--muted)}.value,.kpi{margin-top:10px;font-size:28px;font-weight:1000;line-height:1.1;letter-spacing:-.03em;word-break:break-word}.toolbar{display:flex;justify-content:space-between;align-items:center;gap:12px;margin:26px 0 18px}.tabs,.subtabs{display:flex;gap:10px;flex-wrap:wrap}.tab-btn,.subtab-btn,.table-expand-btn{height:42px;padding:0 16px;border-radius:999px;border:1px solid var(--line);background:#fff;color:#102a72;font-weight:1000;cursor:pointer;transition:.2s transform,.2s background,.2s color}.tab-btn:hover,.subtab-btn:hover,.table-expand-btn:hover{transform:translateY(-1px)}.tab-btn.active,.subtab-btn.active{background:#102a72;color:#fff;border-color:#102a72}.summary-card{padding:22px 24px;margin-bottom:22px}.summary-card ul{margin:10px 0 0 18px;padding:0}.summary-card li{margin:10px 0;font-weight:800}.panel{display:none;animation:fadeUp .28s ease}.panel.active{display:block}.section-head{display:flex;justify-content:space-between;align-items:flex-end;gap:12px;margin:30px 0 14px}.section-head h2{margin:8px 0 0;font-size:24px;line-height:1.2;letter-spacing:-.03em}.download-row{display:flex;gap:10px;flex-wrap:wrap}.download-btn{display:inline-flex;align-items:center;height:42px;padding:0 16px;border-radius:999px;background:#102a72;color:#fff;text-decoration:none;font-weight:1000;transition:.2s transform}.download-btn:hover{transform:translateY(-1px)} .grid-4{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px}.grid-3{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px}.grid-2{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}.card,.section-box{padding:18px}.kpi-sub{margin-top:8px;font-size:12px;font-weight:800;color:var(--muted)} .chart-row{display:grid;grid-template-columns:110px 1fr 58px;gap:10px;align-items:center;margin:12px 0}.chart-label,.chart-metric{font-size:12px;font-weight:900}.chart-track{height:12px;background:#e7eef7;border-radius:999px;overflow:hidden}.chart-fill{display:block;height:100%;background:linear-gradient(90deg,#2b5cff,#83afff);border-radius:999px}.donut-wrap{display:grid;grid-template-columns:116px 1fr;gap:14px;align-items:center}.donut{width:116px;height:116px;border-radius:50%}.legend-item{display:flex;justify-content:space-between;gap:10px;align-items:center;margin:8px 0;font-size:12px;font-weight:900}.legend-dot{width:10px;height:10px;border-radius:50%;background:#2b5cff;display:inline-block;margin-right:8px}.legend-dot.alt{background:#93c5fd}.product-card{display:flex;gap:12px;align-items:center;padding:10px;border:1px solid #e8eef8;border-radius:18px;background:#fff}.thumb{width:48px;height:48px;border-radius:14px;object-fit:cover;border:1px solid var(--line)}.thumb-empty{display:flex;align-items:center;justify-content:center;background:#eef3f8;color:#64748b;font-size:11px;font-weight:1000}.mini-title{margin-top:2px;font-size:14px;font-weight:1000;line-height:1.3}.stack-meta{margin-top:4px;font-size:12px;font-weight:800;color:var(--muted)} .table-meta,.table-tools{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;font-size:12px;font-weight:900;color:var(--muted)}.table-wrap{overflow:auto;border:1px solid var(--line);border-radius:18px}.data-table{width:100%;min-width:860px;border-collapse:collapse}th,td{padding:12px 10px;border-bottom:1px solid #eef3f8;text-align:left;font-size:12px;font-weight:800;white-space:nowrap}th{background:#f8fbff;color:#64748b;text-transform:uppercase;letter-spacing:.08em;font-size:11px}td.num,th.num{text-align:right}.is-hidden{display:none}.empty{padding:16px;font-size:13px;font-weight:800;color:var(--muted)}.channel-panel{animation:fadeUp .25s ease}.fade-pop{animation:fadePop .25s ease}.pill-note{display:inline-flex;align-items:center;padding:6px 10px;border-radius:999px;background:#eef3ff;color:#2346b5;font-size:11px;font-weight:1000} @keyframes fadeUp{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}} @keyframes fadePop{from{opacity:0;transform:scale(.985)}to{opacity:1;transform:scale(1)}} @media (max-width:1100px){.hero-grid,.grid-4,.grid-3,.grid-2{grid-template-columns:1fr}h1{font-size:42px}.chart-row{grid-template-columns:88px 1fr 58px}}
</style>
</head>
<body>
<div class="page"><div class="shell">
<section class="hero"><div class="hero-grid"><div><div class="eyebrow">External Signal Style · CRM Funnel</div><h1>Member Funnel</h1><p>채널별 행동 데이터와 CRM 액션 대상을 한 화면에서 빠르게 읽고, 바로 추출할 수 있게 재정렬했습니다. USER VIEW는 USER_ID 단위 대표행 기준 액션 분석, TOTAL VIEW는 기존 회원 전체 분석입니다.</p><div class="hero-meta">__PERIOD_NAV__</div></div><div class="hero-kpis"><div class="hero-stat fade-pop"><div class="label">Sessions</div><div class="value" id="hero-sessions"></div></div><div class="hero-stat fade-pop"><div class="label">Buyers</div><div class="value" id="hero-buyers"></div></div><div class="hero-stat fade-pop"><div class="label">Revenue</div><div class="value" id="hero-revenue"></div></div><div class="hero-stat fade-pop"><div class="label">Members</div><div class="value" id="hero-members"></div></div></div></div></section>
<div class="toolbar"><div class="tabs"><button class="tab-btn active" data-main-target="user-view">USER VIEW</button><button class="tab-btn" data-main-target="total-view">TOTAL VIEW</button></div><div class="pill-note" id="period-note"></div></div>
<div class="summary-card fade-pop"><div class="section-title">이번 구간 핵심 요약</div><ul id="latest-summary"></ul><div id="fetch-debug" class="kpi-sub" style="display:none"></div></div>
<section class="panel active" id="user-view"><div class="section-head"><div><div class="section-title">USER VIEW</div><h2>행동 데이터 · CRM 액션 뷰 (USER_ID 대표행 기준)</h2></div><div class="download-row">__DOWNLOADS__</div></div><div id="user-sections"></div></section>
<section class="panel" id="total-view"><div class="section-head"><div><div class="section-title">TOTAL VIEW</div><h2>기존 회원 전체 분석</h2></div></div><div id="total-sections"></div></section>
</div></div>
<script>
const ALL_BUNDLES = __ALL_BUNDLES__;
let currentPeriod = '__INITIAL_PERIOD__';
let currentView = 'user-view';
function money(v){const n=Number(v||0); return '₩'+Math.round(n).toLocaleString('ko-KR')}
function num(v){return Math.round(Number(v||0)).toLocaleString('ko-KR')}
function pct(v){return `${Number(v||0).toFixed(1)}%`}
function esc2(s){return String(s ?? '').replace(/[&<>"']/g, m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]))}
function bar(items){ if(!items||!items.length) return '<div class="empty">데이터가 없습니다.</div>'; const max=Math.max(...items.map(x=>Number(x.count||0)),1); return items.map(x=>`<div class="chart-row"><div class="chart-label">${esc2(x.label)}</div><div class="chart-track"><span class="chart-fill" style="width:${(Number(x.count||0)/max*100).toFixed(1)}%"></span></div><div class="chart-metric">${pct(x.share_pct)}</div></div>`).join('') }
function donut(items){ if(!items||!items.length) return '<div class="empty">데이터가 없습니다.</div>'; const a=Number(items[0]?.share_pct||0), b=Number(items[1]?.share_pct||Math.max(0,100-a)); const bg=`conic-gradient(#2b5cff 0 ${a}%, #93c5fd ${a}% ${a+b}%, #e2e8f0 ${a+b}% 100%)`; const lg=items.slice(0,2).map((x,i)=>`<div class="legend-item"><span><span class="legend-dot ${i?'alt':''}"></span>${esc2(x.label)}</span><strong>${pct(x.share_pct)}</strong></div>`).join(''); return `<div class="donut-wrap"><div class="donut" style="background:${bg}"></div><div>${lg}</div></div>`; }
function table(rows, cols, numeric, id){ const limit=15; const head=cols.map(c=>`<th class="${numeric.includes(c[0])?'num':''}">${esc2(c[1])}</th>`).join(''); const body=(rows||[]).map((r,i)=>`<tr class="${i>=limit?'extra-row is-hidden':''}">${cols.map(c=>{ const val=r[c[0]]; const show=numeric.includes(c[0])?(c[0].includes('revenue')||c[0]==='aov'?money(val):num(val)):esc2(val||''); return `<td class="${numeric.includes(c[0])?'num':''}">${show}</td>`; }).join('')}</tr>`).join('') || `<tr><td colspan="${cols.length}">데이터가 없습니다.</td></tr>`; const more=(rows||[]).length>limit?`<button class="table-expand-btn" data-expand="${id}">전체보기</button>`:''; return `<div class="table-tools"><div class="table-meta">전체 ${num((rows||[]).length)}행 중 ${num(Math.min(limit,(rows||[]).length))}행 기본 표시</div>${more}</div><div class="table-wrap" id="${id}"><table class="data-table"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`; }
function focusCards(items){ if(!items||!items.length) return '<div class="empty">상품 데이터가 없습니다.</div>'; return items.map(x=>`<div class="product-card fade-pop">${x.image?`<img class="thumb" src="${esc2(x.image)}" alt="">`:`<div class="thumb thumb-empty">IMG</div>`}<div><div class="mini-title">${esc2(x.product)}</div><div class="stack-meta">${esc2(x.category||'미분류')}</div><div class="stack-meta">Buyers ${num(x.buyers)} · Revenue ${money(x.revenue)}</div></div></div>`).join(''); }
function tabsHtml(group, channels){ return `<div class="subtabs">${channels.map((ch,i)=>`<button class="subtab-btn ${i===0?'active':''}" data-group="${group}" data-target="${ch}">${esc2(ch)}</button>`).join('')}</div>`; }
function groupSlug(s){ return String(s).toLowerCase().replace(/[^a-z0-9가-힣]+/g,'-'); }
function renderNonBuyer(block){ return `<div class="section-head"><div><div class="section-title">NON BUYER</div><h2>가입했지만 아직 사지 않은 사람</h2></div></div>${tabsHtml('nonbuyer', block.channels)}${block.channels.map((ch,i)=>{ const p=block.panels[ch]; return `<div class="channel-panel ${i===0?'active':''}" data-panel-group="nonbuyer" data-panel-id="${ch}" ${i===0?'':'hidden'}><div class="grid-4"><div class="card"><div class="kicker">Non Buyer Members</div><div class="kpi">${num(p.summary.members)}</div></div><div class="card"><div class="kicker">Non Buyer Sessions</div><div class="kpi">${num(p.summary.sessions)}</div></div><div class="card"><div class="kicker">평균 나이</div><div class="kpi">${p.summary.avg_age||'-'}</div></div><div class="card"><div class="kicker">대표 유입 채널</div><div class="kpi">${esc2(p.summary.top_channel)}</div></div></div><div class="grid-3"><div class="section-box"><div class="section-title">AGE 비율</div>${bar(p.age_dist)}</div><div class="section-box"><div class="section-title">GENDER 비율</div>${donut(p.gender_dist)}</div><div class="section-box"><div class="section-title">유입 채널 비율</div>${bar(p.channel_dist)}</div></div><div class="section-box">${table(p.rows,[['member_id','Member ID'],['phone','Phone'],['user_id','USER_ID'],['channel_group','Channel'],['campaign','Campaign'],['sessions','Sessions'],['pageviews','PV']],['sessions','pageviews'],`nb-${groupSlug(ch)}`)}</div></div>`; }).join('')}`; }
function renderBuyer(block){ return `<div class="section-head"><div><div class="section-title">BUYER REVENUE</div><h2>누가 매출을 만들었는지</h2></div></div>${tabsHtml('buyer', block.channels)}${block.channels.map((ch,i)=>{ const p=block.panels[ch]; return `<div class="channel-panel ${i===0?'active':''}" data-panel-group="buyer" data-panel-id="${ch}" ${i===0?'':'hidden'}><div class="grid-4"><div class="card"><div class="kicker">Buyers</div><div class="kpi">${num(p.summary.buyers)}</div></div><div class="card"><div class="kicker">Revenue</div><div class="kpi">${money(p.summary.revenue)}</div></div><div class="card"><div class="kicker">AOV</div><div class="kpi">${money(p.summary.aov)}</div></div><div class="card"><div class="kicker">구매 상품명</div><div class="kpi">${esc2(p.summary.top_product)}</div></div></div><div class="grid-3"><div class="section-box"><div class="section-title">AGE 비율</div>${bar(p.age_dist)}</div><div class="section-box"><div class="section-title">GENDER 비율</div>${donut(p.gender_dist)}</div><div class="section-box"><div class="section-title">구매 상품명 Focus</div><div class="grid-2">${focusCards(p.products)}</div></div></div><div class="section-box">${table(p.rows,[['member_id','Member ID'],['phone','Phone'],['user_id','USER_ID'],['channel_group','Channel'],['campaign','Campaign'],['purchase_product_name','구매 상품명'],['orders','Orders'],['revenue','Revenue']],['orders','revenue'],`buy-${groupSlug(ch)}`)}</div></div>`; }).join('')}`; }
function renderProduct(block){ return `<div class="section-head"><div><div class="section-title">PRODUCT INSIGHT</div><h2>무슨 상품이 고객을 움직였는지</h2></div></div>${tabsHtml('product', block.channels)}${block.channels.map((ch,i)=>{ const p=block.panels[ch]; return `<div class="channel-panel ${i===0?'active':''}" data-panel-group="product" data-panel-id="${ch}" ${i===0?'':'hidden'}><div class="grid-2"><div class="section-box"><div class="section-title">Category 비율</div>${bar(p.category_dist)}</div><div class="section-box"><div class="section-title">구매 상품명 Focus</div><div class="grid-2">${focusCards(p.products)}</div></div></div><div class="section-box">${table(p.rows,[['channel','Channel'],['product','Product'],['buyers','Buyers'],['revenue','Revenue']],['buyers','revenue'],`prd-${groupSlug(ch)}`)}</div></div>`; }).join('')}`; }
function renderTarget(block){ const cards=(block.cards||[]).map(x=>`<div class="card fade-pop"><div class="kicker">${esc2(x.label)}</div><div class="kpi">${num(x.count)}명</div><div class="stack-meta">Top Channel · ${esc2(x.top_channel)}</div><div class="stack-meta">Top Product · ${esc2(x.top_product)}</div><div class="stack-meta">Message · ${esc2(x.top_message)}</div></div>`).join(''); return `<div class="section-head"><div><div class="section-title">SEGMENT</div><h2>세그먼트별 채널 · 관심상품 · 추천 메시지</h2></div></div><div class="grid-3">${cards||'<div class="empty">세그먼트 데이터가 없습니다.</div>'}</div>`; }
function renderTotal(block){ return `${tabsHtml('totalmember', block.channels)}${block.channels.map((ch,i)=>{ const p=block.panels[ch]; return `<div class="channel-panel ${i===0?'active':''}" data-panel-group="totalmember" data-panel-id="${ch}" ${i===0?'':'hidden'}><div class="grid-4"><div class="card"><div class="kicker">기존 회원</div><div class="kpi">${num(p.summary.members)}</div></div><div class="card"><div class="kicker">구매 회원</div><div class="kpi">${num(p.summary.buyers)}</div></div><div class="card"><div class="kicker">Revenue</div><div class="kpi">${money(p.summary.revenue)}</div></div><div class="card"><div class="kicker">대표 구매 상품명</div><div class="kpi">${esc2(p.summary.top_product)}</div></div></div><div class="grid-2"><div class="section-box"><div class="section-title">AGE 비율</div>${bar(p.age_dist)}</div><div class="section-box"><div class="section-title">GENDER 비율</div>${donut(p.gender_dist)}</div><div class="section-box"><div class="section-title">Category 비율</div>${bar(p.category_dist)}</div><div class="section-box"><div class="section-title">구매 상품명 Focus</div><div class="grid-2">${focusCards(p.products)}</div></div></div><div class="section-box">${table(p.rows,[['member_id','Member ID'],['user_id','USER_ID'],['channel_group','Channel'],['campaign','Campaign'],['purchase_product_name','구매 상품명'],['orders','Orders'],['revenue','Revenue']],['orders','revenue'],`tot-${groupSlug(ch)}`)}</div></div>`; }).join('')}`; }
function renderPeriod(bundle){ document.getElementById('hero-sessions').textContent=num(bundle.overview.sessions); document.getElementById('hero-buyers').textContent=num(bundle.overview.buyers); document.getElementById('hero-revenue').textContent=money(bundle.overview.revenue); document.getElementById('hero-members').textContent=num(bundle.overview.members); document.getElementById('period-note').textContent=`${bundle.period_label} · ${bundle.date_range.start} ~ ${bundle.date_range.end}`; document.getElementById('latest-summary').innerHTML=(bundle.latest_summary||[]).map(x=>`<li>${esc2(x)}</li>`).join(''); document.getElementById('user-sections').innerHTML=renderNonBuyer(bundle.user_view.non_buyer)+renderBuyer(bundle.user_view.buyer)+renderProduct(bundle.user_view.product)+renderTarget(bundle.user_view.target); document.getElementById('total-sections').innerHTML=renderTotal(bundle.total_view.member_overview); bindDynamic(); }
function bindDynamic(){ document.querySelectorAll('[data-expand]').forEach(btn=>{ btn.onclick=()=>{ const table=document.getElementById(btn.dataset.expand); if(table){ table.querySelectorAll('.extra-row').forEach(r=>r.classList.toggle('is-hidden')); btn.textContent = btn.textContent==='전체보기' ? '접기' : '전체보기'; } }; }); document.querySelectorAll('.subtab-btn').forEach(btn=>{ btn.onclick=()=>{ const g=btn.dataset.group, t=btn.dataset.target; document.querySelectorAll(`.subtab-btn[data-group="${g}"]`).forEach(b=>b.classList.toggle('active', b===btn)); document.querySelectorAll(`.channel-panel[data-panel-group="${g}"]`).forEach(p=>{ const on=p.dataset.panelId===t; p.hidden=!on; p.classList.toggle('active', on); }); }; }); }
function bindStatic(){ document.querySelectorAll('[data-main-target]').forEach(btn=>btn.addEventListener('click',()=>{ currentView=btn.dataset.mainTarget; document.querySelectorAll('[data-main-target]').forEach(b=>b.classList.remove('active')); btn.classList.add('active'); document.querySelectorAll('.panel').forEach(p=>p.classList.remove('active')); document.getElementById(currentView).classList.add('active'); })); document.querySelectorAll('[data-period]').forEach(btn=>btn.addEventListener('click',()=>{ currentPeriod=btn.dataset.period; document.querySelectorAll('[data-period]').forEach(b=>b.classList.toggle('active',b===btn)); renderPeriod(ALL_BUNDLES[currentPeriod]); })); }
(function(){ try{ bindStatic(); renderPeriod(ALL_BUNDLES[currentPeriod]); document.getElementById(currentView).classList.add('active'); } catch(err){ document.getElementById('user-sections').innerHTML = `<div class="summary-card"><strong>데이터 렌더 실패</strong><div class="kpi-sub">${esc2(err && err.message ? err.message : err)}</div></div>`; }})();
</script></body></html>
"""
    download_html = ''
    if downloads.get('non_buyer'):
        download_html += f'<a class="download-btn" href="{esc(downloads["non_buyer"])}">Non Buyer Excel</a>'
    if downloads.get('target'):
        download_html += f'<a class="download-btn" href="{esc(downloads["target"])}">Target Segment Excel</a>'
    return (html_template
        .replace('__PERIOD_NAV__', period_nav)
        .replace('__DOWNLOADS__', download_html)
        .replace('__ALL_BUNDLES__', all_json)
        .replace('__INITIAL_PERIOD__', preset['key'])
    )

def main():
    ensure_dir(OUT_DIR); ensure_dir(DATA_DIR); ensure_dir(DOWNLOAD_DIR)
    default_payload = None
    all_bundles = {}
    bundle_by_key = {}
    for preset in PERIOD_PRESETS:
        start_date, end_date = period_date_range(preset['days'])
        df = load_rows(preset['key'])
        bundle = build_bundle(df, start_date, end_date, preset['key'], preset['label'])
        export_excel_files(bundle, preset['key'])
        light_bundle = make_light_bundle(bundle)
        bundle_by_key[preset['key']] = light_bundle
        all_bundles[preset['key']] = light_bundle
        if WRITE_DATA_CACHE:
            write_json(DATA_DIR / f"{preset['key']}_view.json", light_bundle)
        if preset['is_default']:
            default_payload = light_bundle
    for preset in PERIOD_PRESETS:
        html_text = render_page(bundle_by_key[preset['key']], preset, all_bundles)
        (OUT_DIR / preset['filename']).write_text(html_text, encoding='utf-8')
    if default_payload:
        _write_summary_json(HUB_SUMMARY_DIR, 'member_funnel', {'title':'Member Funnel','updated_at':default_payload.get('generated_at'),'range':default_payload.get('date_range'),'sessions':default_payload.get('overview',{}).get('sessions',0),'buyers':default_payload.get('overview',{}).get('buyers',0)})

if __name__ == '__main__':
    main()
