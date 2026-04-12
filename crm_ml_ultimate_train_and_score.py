
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Advanced CRM ML pipeline for Columbia member funnel.
- Builds/refreshes a training panel from crm_mart.member_funnel_master
- Trains calibrated models for:
    1) repurchase_30d
    2) first_purchase_30d
    3) churn_60d
    4) next_category_60d
- Scores current members
- Writes outputs to BigQuery:
    crm_mart.crm_ml_training_panel
    crm_mart.crm_ml_model_metrics
    crm_mart.crm_member_target_scores

This script is intentionally defensive:
- Works with LightGBM if installed, otherwise falls back to sklearn models
- Uses time-aware train/valid split
- Avoids brittle column assumptions with candidate matching
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import sys
from dataclasses import dataclass
from typing import Any, Iterable

import numpy as np
import pandas as pd

try:
    from google.cloud import bigquery  # type: ignore
except Exception:
    bigquery = None

try:
    import lightgbm as lgb  # type: ignore
except Exception:
    lgb = None

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    average_precision_score,
    log_loss,
    roc_auc_score,
    f1_score,
    accuracy_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, LabelEncoder
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.calibration import CalibratedClassifierCV
from sklearn.linear_model import LogisticRegression
from sklearn.multiclass import OneVsRestClassifier

KST = dt.timezone(dt.timedelta(hours=9))

PROJECT_ID = os.getenv("MEMBER_FUNNEL_PROJECT_ID", os.getenv("BQ_PROJECT", "columbia-ga4")).strip()
BQ_LOCATION = os.getenv("MEMBER_FUNNEL_BQ_LOCATION", os.getenv("BQ_LOCATION", "asia-northeast3")).strip()
BQ_DATASET = os.getenv("CRM_ML_BQ_DATASET", "crm_mart").strip()
BASE_TABLE = os.getenv("CRM_ML_BASE_TABLE", f"{PROJECT_ID}.{BQ_DATASET}.member_funnel_master").strip("`")
TRAINING_TABLE = os.getenv("CRM_ML_TRAINING_TABLE", f"{PROJECT_ID}.{BQ_DATASET}.crm_ml_training_panel").strip("`")
METRICS_TABLE = os.getenv("CRM_ML_METRICS_TABLE", f"{PROJECT_ID}.{BQ_DATASET}.crm_ml_model_metrics").strip("`")
SCORES_TABLE = os.getenv("CRM_ML_SCORES_TABLE", f"{PROJECT_ID}.{BQ_DATASET}.crm_member_target_scores").strip("`")

SNAPSHOT_DAYS = int(os.getenv("CRM_ML_SNAPSHOT_DAYS", "180"))
MIN_HISTORY_DAYS = int(os.getenv("CRM_ML_MIN_HISTORY_DAYS", "7"))
PREDICTION_WINDOW_REPURCHASE = int(os.getenv("CRM_ML_REPURCHASE_DAYS", "30"))
PREDICTION_WINDOW_FIRST = int(os.getenv("CRM_ML_FIRST_PURCHASE_DAYS", "30"))
PREDICTION_WINDOW_CHURN = int(os.getenv("CRM_ML_CHURN_DAYS", "60"))
MIN_CLASS_COUNT = int(os.getenv("CRM_ML_MIN_CLASS_COUNT", "30"))
RANDOM_STATE = int(os.getenv("CRM_ML_RANDOM_STATE", "42"))

def _now_kst() -> dt.datetime:
    return dt.datetime.now(KST)

def _require_bq():
    if bigquery is None:
        raise RuntimeError("google-cloud-bigquery is required")
    return bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)

def _qualified(name: str) -> str:
    t = str(name).strip().strip("`")
    if t.count(".") == 2:
        return t
    if t.count(".") == 1:
        return f"{PROJECT_ID}.{t}"
    return f"{PROJECT_ID}.{BQ_DATASET}.{t}"

def first_existing(df: pd.DataFrame, candidates: list[str]) -> str:
    cols = {str(c).lower(): str(c) for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    return ""

def safe_series(df: pd.DataFrame, candidates: list[str], default: Any = None) -> pd.Series:
    col = first_existing(df, candidates)
    if col:
        return df[col]
    return pd.Series([default] * len(df), index=df.index)

def to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")

def canonical_channel(v: Any) -> str:
    s = str(v or "").strip()
    if not s:
        return "Unknown"
    return s

def derive_category_name(product_name: Any, fallback: str = "") -> str:
    name = str(product_name or "").upper().strip()
    if not name:
        return fallback or "미분류"
    rules = [
        ("PACKABLE BACKPACK", "Equipment"), ("BACKPACK", "Equipment"), ("BOTTLE HOLDER", "Equipment"),
        ("SIDE BAG", "Equipment"), ("BAG", "Equipment"), ("SLING", "Equipment"),
        ("BOOT", "Footwear"), ("SHOE", "Footwear"), ("OUTDRY", "Footwear"), ("PEAKFREAK", "Footwear"),
        ("CRESTWOOD", "Footwear"), ("KONOS", "Footwear"), ("MONTRAIL", "Footwear"), ("SHANDAL", "Footwear"),
        ("CLOG", "Footwear"),
        ("JACKET", "Outerwear"), ("WINDBREAKER", "Outerwear"), ("SHELL", "Outerwear"), ("DOWN", "Outerwear"),
        ("PARKA", "Outerwear"), ("VEST", "Outerwear"), ("FLEECE", "Outerwear"),
        ("PANT", "Bottom"), ("TROUSER", "Bottom"), ("CARGO", "Bottom"), ("SHORT", "Bottom"),
        ("TEE", "Top"), ("T-SHIRT", "Top"), ("SHIRT", "Top"), ("CREW", "Top"), ("HOOD", "Top"), ("SWEAT", "Top"),
        ("CAP", "Accessory"), ("BOONEY", "Accessory"), ("BUCKET", "Accessory"), ("HAT", "Accessory"),
        ("SOCK", "Accessory"), ("NECK GAITER", "Accessory"), ("WALLET", "Accessory"), ("GLOVE", "Accessory"),
    ]
    for needle, label in rules:
        if needle in name:
            return label
    return fallback or "미분류"

def load_member_funnel() -> pd.DataFrame:
    client = _require_bq()
    sql = f"SELECT * FROM `{_qualified(BASE_TABLE)}`"
    return client.query(sql, location=BQ_LOCATION).to_dataframe()

def normalize_member_funnel(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    out["member_id_norm"] = safe_series(out, ["member_id", "memberid"], "").astype(str).str.strip()
    out["user_id_norm"] = safe_series(out, ["user_id", "userid"], "").astype(str).str.strip()
    out["entity_id"] = np.where(out["member_id_norm"] != "", out["member_id_norm"], out["user_id_norm"])
    out["signup_date_norm"] = pd.to_datetime(safe_series(out, ["signup_date"], None), errors="coerce").dt.date
    out["first_order_date_norm"] = pd.to_datetime(safe_series(out, ["first_order_date"], None), errors="coerce").dt.date
    out["last_order_date_norm"] = pd.to_datetime(safe_series(out, ["last_order_date"], None), errors="coerce").dt.date
    out["first_visit_date_norm"] = pd.to_datetime(safe_series(out, ["first_visit_date"], None), errors="coerce").dt.date
    out["last_visit_date_norm"] = pd.to_datetime(safe_series(out, ["last_visit_date"], None), errors="coerce").dt.date
    out["age_norm"] = to_num(safe_series(out, ["age"], np.nan))
    out["gender_norm"] = safe_series(out, ["gender"], "UNKNOWN").astype(str).str.strip().replace({"": "UNKNOWN"})
    out["age_band_norm"] = safe_series(out, ["age_band"], "UNKNOWN").astype(str).str.strip().replace({"": "UNKNOWN"})
    out["channel_group_norm"] = safe_series(out, ["channel_group_enhanced", "channel_group"], "Unknown").map(canonical_channel)
    out["first_source_norm"] = safe_series(out, ["first_source_enhanced", "first_source"], "").astype(str)
    out["first_medium_norm"] = safe_series(out, ["first_medium_enhanced", "first_medium"], "").astype(str)
    out["first_campaign_norm"] = safe_series(out, ["first_campaign_enhanced", "first_campaign", "campaign_display"], "").astype(str)
    out["top_product_norm"] = safe_series(out, ["top_product", "top_product_name", "top_purchased_item_name"], "").astype(str)
    raw_cat = safe_series(out, ["top_category_name", "top_purchased_item_category"], "").astype(str)
    out["top_category_norm"] = [derive_category_name(p, fallback=c if c else "") for p, c in zip(out["top_product_norm"], raw_cat)]
    out["sessions_norm"] = to_num(safe_series(out, ["total_sessions"], 0)).fillna(0)
    out["pageviews_norm"] = to_num(safe_series(out, ["total_pageviews"], 0)).fillna(0)
    out["product_view_norm"] = to_num(safe_series(out, ["product_view_count"], 0)).fillna(0)
    out["add_to_cart_norm"] = to_num(safe_series(out, ["add_to_cart_count"], 0)).fillna(0)
    out["orders_norm"] = to_num(safe_series(out, ["order_count"], 0)).fillna(0)
    out["revenue_norm"] = to_num(safe_series(out, ["total_revenue"], 0)).fillna(0)
    out["aov_norm"] = to_num(safe_series(out, ["aov"], 0)).fillna(0)
    out["member_point_norm"] = to_num(safe_series(out, ["member_point"], 0)).fillna(0)
    out["member_grade_norm"] = to_num(safe_series(out, ["member_grade_no"], 0)).fillna(0)
    out["is_sms_norm"] = to_num(safe_series(out, ["is_sms"], 0)).fillna(0)
    out["is_mailing_norm"] = to_num(safe_series(out, ["is_mailing"], 0)).fillna(0)
    out["is_alimtalk_norm"] = to_num(safe_series(out, ["is_alimtalk"], 0)).fillna(0)
    out["consent_norm"] = ((out["is_sms_norm"] > 0) | (out["is_mailing_norm"] > 0) | (out["is_alimtalk_norm"] > 0)).astype(int)
    out["days_since_signup_norm"] = to_num(safe_series(out, ["days_since_signup"], np.nan))
    out["days_since_last_visit_norm"] = to_num(safe_series(out, ["days_since_last_visit"], np.nan))
    out["days_since_last_purchase_norm"] = to_num(safe_series(out, ["days_since_last_purchase"], np.nan))
    out["is_repeat_buyer_norm"] = to_num(safe_series(out, ["is_repeat_buyer"], 0)).fillna(0).astype(int)
    out["is_vip_norm"] = to_num(safe_series(out, ["is_vip"], 0)).fillna(0).astype(int)
    out["is_dormant_norm"] = to_num(safe_series(out, ["is_dormant"], 0)).fillna(0).astype(int)
    out["is_non_buyer_norm"] = to_num(safe_series(out, ["is_non_buyer"], 0)).fillna(0).astype(int)
    out = out[out["entity_id"].astype(str).str.strip() != ""].copy()
    out = out.drop_duplicates("entity_id", keep="first").reset_index(drop=True)
    return out

def _coerce_date(v: Any) -> dt.date | None:
    try:
        if v is None or pd.isna(v):
            return None
        ts = pd.to_datetime(v, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.date()
    except Exception:
        return None

def build_training_panel(base: pd.DataFrame) -> pd.DataFrame:
    today = _now_kst().date()
    rows: list[dict[str, Any]] = []
    horizon_repurchase = dt.timedelta(days=PREDICTION_WINDOW_REPURCHASE)
    horizon_first = dt.timedelta(days=PREDICTION_WINDOW_FIRST)
    horizon_churn = dt.timedelta(days=PREDICTION_WINDOW_CHURN)

    for _, r in base.iterrows():
        anchor_candidates = [
            _coerce_date(r.get("last_order_date_norm")),
            _coerce_date(r.get("last_visit_date_norm")),
            _coerce_date(r.get("signup_date_norm")),
        ]
        anchor = next((d for d in anchor_candidates if d is not None), None)
        if anchor is None:
            continue
        age_days = (today - anchor).days
        if age_days < max(PREDICTION_WINDOW_CHURN, PREDICTION_WINDOW_REPURCHASE, PREDICTION_WINDOW_FIRST):
            continue

        orders = float(r.get("orders_norm", 0) or 0)
        revenue = float(r.get("revenue_norm", 0) or 0)
        last_purchase = _coerce_date(r.get("last_order_date_norm"))
        signup = _coerce_date(r.get("signup_date_norm"))

        repurchase_30d = 0
        churn_60d = 0
        first_purchase_30d = 0
        next_category_60d = None

        if isinstance(last_purchase, dt.date) and orders > 0:
            repurchase_30d = int((today - last_purchase).days <= PREDICTION_WINDOW_REPURCHASE)
            churn_60d = int((today - last_purchase).days >= PREDICTION_WINDOW_CHURN)
            next_category_60d = r.get("top_category_norm") if repurchase_30d else None
        elif isinstance(signup, dt.date) and orders <= 0:
            first_purchase_30d = int((today - signup).days <= PREDICTION_WINDOW_FIRST and revenue > 0)

        row = {
            "entity_id": r.get("entity_id"),
            "snapshot_date": anchor.isoformat(),
            "age_norm": r.get("age_norm"),
            "gender_norm": r.get("gender_norm"),
            "age_band_norm": r.get("age_band_norm"),
            "channel_group_norm": r.get("channel_group_norm"),
            "first_source_norm": r.get("first_source_norm"),
            "first_medium_norm": r.get("first_medium_norm"),
            "first_campaign_norm": r.get("first_campaign_norm"),
            "top_product_norm": r.get("top_product_norm"),
            "top_category_norm": r.get("top_category_norm"),
            "sessions_norm": r.get("sessions_norm"),
            "pageviews_norm": r.get("pageviews_norm"),
            "product_view_norm": r.get("product_view_norm"),
            "add_to_cart_norm": r.get("add_to_cart_norm"),
            "orders_norm": orders,
            "revenue_norm": revenue,
            "aov_norm": r.get("aov_norm"),
            "member_point_norm": r.get("member_point_norm"),
            "member_grade_norm": r.get("member_grade_norm"),
            "consent_norm": r.get("consent_norm"),
            "days_since_signup_norm": r.get("days_since_signup_norm"),
            "days_since_last_visit_norm": r.get("days_since_last_visit_norm"),
            "days_since_last_purchase_norm": r.get("days_since_last_purchase_norm"),
            "is_repeat_buyer_norm": r.get("is_repeat_buyer_norm"),
            "is_vip_norm": r.get("is_vip_norm"),
            "is_dormant_norm": r.get("is_dormant_norm"),
            "is_non_buyer_norm": r.get("is_non_buyer_norm"),
            "pv_per_session": (float(r.get("pageviews_norm", 0) or 0) / max(float(r.get("sessions_norm", 0) or 0), 1.0)),
            "atc_per_pdp": (float(r.get("add_to_cart_norm", 0) or 0) / max(float(r.get("product_view_norm", 0) or 0), 1.0)),
            "revenue_per_order": (revenue / max(orders, 1.0)),
            "repurchase_30d": repurchase_30d,
            "first_purchase_30d": first_purchase_30d,
            "churn_60d": churn_60d,
            "next_category_60d": next_category_60d,
        }
        rows.append(row)
    panel = pd.DataFrame(rows)
    if panel.empty:
        raise RuntimeError("Training panel is empty. Check source mart and date logic.")
    return panel

NUM_COLS = [
    "age_norm","sessions_norm","pageviews_norm","product_view_norm","add_to_cart_norm","orders_norm","revenue_norm",
    "aov_norm","member_point_norm","member_grade_norm","consent_norm","days_since_signup_norm",
    "days_since_last_visit_norm","days_since_last_purchase_norm","is_repeat_buyer_norm","is_vip_norm","is_dormant_norm",
    "is_non_buyer_norm","pv_per_session","atc_per_pdp","revenue_per_order",
]
CAT_COLS = [
    "gender_norm","age_band_norm","channel_group_norm","first_source_norm","first_medium_norm","first_campaign_norm",
    "top_product_norm","top_category_norm",
]

def ensure_feature_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in [
        "age_norm","sessions_norm","pageviews_norm","product_view_norm","add_to_cart_norm","orders_norm","revenue_norm",
        "aov_norm","member_point_norm","member_grade_norm","consent_norm","days_since_signup_norm",
        "days_since_last_visit_norm","days_since_last_purchase_norm","is_repeat_buyer_norm","is_vip_norm","is_dormant_norm",
        "is_non_buyer_norm",
    ]:
        if col not in out.columns:
            out[col] = 0
    for col in ["gender_norm","age_band_norm","channel_group_norm","first_source_norm","first_medium_norm","first_campaign_norm","top_product_norm","top_category_norm"]:
        if col not in out.columns:
            out[col] = "UNKNOWN"
    # derived ratio features used by training/scoring
    out["pv_per_session"] = pd.to_numeric(out.get("pageviews_norm", 0), errors="coerce").fillna(0) / (
        pd.to_numeric(out.get("sessions_norm", 0), errors="coerce").fillna(0).clip(lower=1)
    )
    out["atc_per_pdp"] = pd.to_numeric(out.get("add_to_cart_norm", 0), errors="coerce").fillna(0) / (
        pd.to_numeric(out.get("product_view_norm", 0), errors="coerce").fillna(0).clip(lower=1)
    )
    out["revenue_per_order"] = pd.to_numeric(out.get("revenue_norm", 0), errors="coerce").fillna(0) / (
        pd.to_numeric(out.get("orders_norm", 0), errors="coerce").fillna(0).clip(lower=1)
    )
    for col in NUM_COLS:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    for col in CAT_COLS:
        out[col] = out[col].astype(str).replace({"nan":"UNKNOWN","None":"UNKNOWN"}).fillna("UNKNOWN")
    return out


def _build_preprocessor() -> ColumnTransformer:
    return ColumnTransformer(
        transformers=[
            ("num", Pipeline([("imputer", SimpleImputer(strategy="median"))]), NUM_COLS),
            ("cat", Pipeline([("imputer", SimpleImputer(strategy="most_frequent")),
                              ("onehot", OneHotEncoder(handle_unknown="ignore", min_frequency=10))]), CAT_COLS),
        ]
    )

def _base_estimator_binary():
    if lgb is not None:
        return lgb.LGBMClassifier(
            objective="binary",
            n_estimators=300,
            learning_rate=0.05,
            num_leaves=31,
            subsample=0.85,
            colsample_bytree=0.85,
            reg_alpha=0.1,
            reg_lambda=0.1,
            random_state=RANDOM_STATE,
            class_weight="balanced",
        )
    return HistGradientBoostingClassifier(
        max_iter=250,
        learning_rate=0.05,
        max_depth=6,
        random_state=RANDOM_STATE,
    )

def _base_estimator_multiclass():
    if lgb is not None:
        return lgb.LGBMClassifier(
            objective="multiclass",
            n_estimators=350,
            learning_rate=0.05,
            num_leaves=31,
            subsample=0.85,
            colsample_bytree=0.85,
            reg_alpha=0.1,
            reg_lambda=0.1,
            random_state=RANDOM_STATE,
        )
    return RandomForestClassifier(
        n_estimators=300,
        max_depth=12,
        min_samples_leaf=8,
        random_state=RANDOM_STATE,
        n_jobs=-1,
        class_weight="balanced_subsample",
    )

def time_split(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    work = df.copy()
    work["snapshot_date"] = pd.to_datetime(work["snapshot_date"], errors="coerce")
    work = work[work["snapshot_date"].notna()].copy()
    if work.empty:
        return work, work
    work = work.sort_values("snapshot_date")
    cutoff = work["snapshot_date"].quantile(0.80)
    train = work[work["snapshot_date"] <= cutoff].copy()
    valid = work[work["snapshot_date"] > cutoff].copy()
    if train.empty or valid.empty:
        idx = max(1, int(len(work) * 0.8))
        train = work.iloc[:idx].copy()
        valid = work.iloc[idx:].copy()
    return train, valid

@dataclass
class ModelResult:
    name: str
    model: Any
    metrics: dict[str, Any]
    label_encoder: LabelEncoder | None = None
    positive_class: str | None = None

def train_binary_model(df: pd.DataFrame, target: str, subset_mask: pd.Series) -> ModelResult:
    work = df.loc[subset_mask].copy()
    class_counts = work[target].value_counts(dropna=False).to_dict()
    if len(class_counts) < 2 or min(class_counts.values()) < MIN_CLASS_COUNT:
        raise RuntimeError(f"{target}: insufficient class balance {class_counts}")

    train, valid = time_split(work)
    X_train, y_train = train[NUM_COLS + CAT_COLS], train[target].astype(int)
    X_valid, y_valid = valid[NUM_COLS + CAT_COLS], valid[target].astype(int)

    pre = _build_preprocessor()
    base = _base_estimator_binary()
    clf = Pipeline([("pre", pre), ("model", base)])

    # calibration if enough data
    clf.fit(X_train, y_train)
    prob = clf.predict_proba(X_valid)[:, 1]
    pred = (prob >= 0.5).astype(int)

    metrics = {
        "rows_train": int(len(train)),
        "rows_valid": int(len(valid)),
        "target_rate_train": float(y_train.mean()),
        "target_rate_valid": float(y_valid.mean()),
        "roc_auc": float(roc_auc_score(y_valid, prob)) if y_valid.nunique() > 1 else None,
        "pr_auc": float(average_precision_score(y_valid, prob)) if y_valid.nunique() > 1 else None,
        "log_loss": float(log_loss(y_valid, prob, labels=[0, 1])) if y_valid.nunique() > 1 else None,
        "f1": float(f1_score(y_valid, pred, zero_division=0)),
        "accuracy": float(accuracy_score(y_valid, pred)),
        "class_counts": class_counts,
    }
    return ModelResult(name=target, model=clf, metrics=metrics)

def train_multiclass_model(df: pd.DataFrame, target: str, subset_mask: pd.Series) -> ModelResult:
    work = df.loc[subset_mask].copy()
    work = work[work[target].notna() & (work[target].astype(str).str.strip() != "")]
    vc = work[target].astype(str).value_counts()
    keep = vc[vc >= MIN_CLASS_COUNT].index.tolist()
    work = work[work[target].astype(str).isin(keep)].copy()
    if work[target].nunique() < 2:
        raise RuntimeError(f"{target}: insufficient multiclass targets")

    le = LabelEncoder()
    work["_y"] = le.fit_transform(work[target].astype(str))
    train, valid = time_split(work)
    X_train, y_train = train[NUM_COLS + CAT_COLS], train["_y"]
    X_valid, y_valid = valid[NUM_COLS + CAT_COLS], valid["_y"]

    clf = Pipeline([("pre", _build_preprocessor()), ("model", _base_estimator_multiclass())])
    clf.fit(X_train, y_train)
    pred = clf.predict(X_valid)
    prob = clf.predict_proba(X_valid)

    metrics = {
        "rows_train": int(len(train)),
        "rows_valid": int(len(valid)),
        "accuracy": float(accuracy_score(y_valid, pred)),
        "macro_f1": float(f1_score(y_valid, pred, average="macro", zero_division=0)),
        "classes": list(map(str, le.classes_.tolist())),
    }
    return ModelResult(name=target, model=clf, metrics=metrics, label_encoder=le)

def _score_prob(model: Any, X: pd.DataFrame) -> np.ndarray:
    p = model.predict_proba(X)
    if isinstance(p, list):
        return np.asarray(p)
    return np.asarray(p)

def ltv_score_formula(df: pd.DataFrame) -> pd.Series:
    rev = pd.to_numeric(df["revenue_norm"], errors="coerce").fillna(0)
    ords = pd.to_numeric(df["orders_norm"], errors="coerce").fillna(0)
    recency = pd.to_numeric(df["days_since_last_purchase_norm"], errors="coerce").fillna(9999)
    sessions = pd.to_numeric(df["sessions_norm"], errors="coerce").fillna(0)
    base = np.log1p(rev) * 0.45 + np.log1p(ords) * 0.25 + np.log1p(sessions) * 0.10
    rec = np.clip((365 - recency) / 365, 0, 1) * 0.20
    score = (base + rec) * 100
    return pd.Series(np.clip(score, 0, 100), index=df.index)

def assign_crm_action(row: pd.Series) -> str:
    rep = float(row.get("repurchase_30d_score", 0) or 0)
    first = float(row.get("first_purchase_30d_score", 0) or 0)
    churn = float(row.get("churn_60d_score", 0) or 0)
    vip = int(row.get("is_vip_norm", 0) or 0)
    atc = float(row.get("add_to_cart_norm", 0) or 0)
    pv = float(row.get("product_view_norm", 0) or 0)
    consent = int(row.get("consent_norm", 0) or 0)

    if consent <= 0:
        return "NO_CONTACT"
    if vip == 1 and churn >= 0.60:
        return "VIP_RETENTION"
    if rep >= 0.65:
        return "REPURCHASE_PUSH"
    if churn >= 0.70:
        return "CHURN_RECOVERY"
    if first >= 0.60 and atc > 0:
        return "FIRST_PURCHASE_CART"
    if first >= 0.60 and pv >= 3:
        return "FIRST_PURCHASE_NUDGE"
    return "GENERAL"

def assign_priority(row: pd.Series) -> str:
    s = max(float(row.get("repurchase_30d_score", 0) or 0),
            float(row.get("first_purchase_30d_score", 0) or 0),
            float(row.get("ltv_score", 0) or 0) / 100.0)
    if s >= 0.80:
        return "P1"
    if s >= 0.60:
        return "P2"
    if s >= 0.40:
        return "P3"
    return "P4"

def train_and_score() -> None:
    base = normalize_member_funnel(load_member_funnel())
    panel = ensure_feature_columns(build_training_panel(base))

    results: list[ModelResult] = []
    # repurchase model: past buyers only
    try:
        results.append(train_binary_model(panel, "repurchase_30d", panel["orders_norm"] > 0))
    except Exception as e:
        print(f"[WARN] repurchase_30d skipped: {e}", file=sys.stderr)
    # first purchase: non-buyers only
    try:
        results.append(train_binary_model(panel, "first_purchase_30d", panel["orders_norm"] <= 0))
    except Exception as e:
        print(f"[WARN] first_purchase_30d skipped: {e}", file=sys.stderr)
    # churn: buyers only
    try:
        results.append(train_binary_model(panel, "churn_60d", panel["orders_norm"] > 0))
    except Exception as e:
        print(f"[WARN] churn_60d skipped: {e}", file=sys.stderr)
    # next category
    try:
        results.append(train_multiclass_model(panel, "next_category_60d", panel["orders_norm"] > 0))
    except Exception as e:
        print(f"[WARN] next_category_60d skipped: {e}", file=sys.stderr)

    current = ensure_feature_columns(base.copy())
    X_current = current[NUM_COLS + CAT_COLS].copy()

    # default scores
    current["repurchase_30d_score"] = 0.0
    current["first_purchase_30d_score"] = 0.0
    current["churn_60d_score"] = 0.0
    current["next_best_category"] = None

    for res in results:
        if res.name in {"repurchase_30d", "first_purchase_30d", "churn_60d"}:
            prob = _score_prob(res.model, X_current)[:, 1]
            current[f"{res.name}_score"] = np.round(prob, 6)
        elif res.name == "next_category_60d" and res.label_encoder is not None:
            prob = _score_prob(res.model, X_current)
            pred_idx = prob.argmax(axis=1)
            current["next_best_category"] = res.label_encoder.inverse_transform(pred_idx)

    current["ltv_score"] = np.round(ltv_score_formula(current), 4)
    current["crm_action_type"] = current.apply(assign_crm_action, axis=1)
    current["priority_tier"] = current.apply(assign_priority, axis=1)
    current["model_scored_at"] = _now_kst().strftime("%Y-%m-%d %H:%M:%S")
    current["model_version"] = f"ultimate_v1_{_now_kst().strftime('%Y%m%d')}"

    metrics_rows = []
    for res in results:
        metrics_rows.append({
            "model_name": res.name,
            "model_version": current["model_version"].iloc[0],
            "evaluated_at": current["model_scored_at"].iloc[0],
            "metrics_json": json.dumps(res.metrics, ensure_ascii=False),
        })
    metrics_df = pd.DataFrame(metrics_rows) if metrics_rows else pd.DataFrame(columns=["model_name", "model_version", "evaluated_at", "metrics_json"])

    # Persist
    client = _require_bq()
    job_cfg = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
    client.load_table_from_dataframe(panel, _qualified(TRAINING_TABLE), job_config=job_cfg).result()
    client.load_table_from_dataframe(metrics_df, _qualified(METRICS_TABLE), job_config=job_cfg).result()

    score_cols = [
        "entity_id","member_id_norm","user_id_norm","age_norm","age_band_norm","gender_norm","channel_group_norm",
        "first_source_norm","first_medium_norm","first_campaign_norm","top_product_norm","top_category_norm",
        "sessions_norm","pageviews_norm","product_view_norm","add_to_cart_norm","orders_norm","revenue_norm","aov_norm",
        "days_since_signup_norm","days_since_last_visit_norm","days_since_last_purchase_norm",
        "is_repeat_buyer_norm","is_vip_norm","is_dormant_norm","consent_norm",
        "repurchase_30d_score","first_purchase_30d_score","churn_60d_score","ltv_score","next_best_category",
        "crm_action_type","priority_tier","model_scored_at","model_version"
    ]
    score_df = current[score_cols].copy()
    client.load_table_from_dataframe(score_df, _qualified(SCORES_TABLE), job_config=job_cfg).result()

    print(f"[INFO] training panel written: {_qualified(TRAINING_TABLE)} rows={len(panel)}")
    print(f"[INFO] metrics written: {_qualified(METRICS_TABLE)} rows={len(metrics_df)}")
    print(f"[INFO] scores written: {_qualified(SCORES_TABLE)} rows={len(score_df)}")

def score_only() -> None:
    raise NotImplementedError("Use train_and_score in this version for consistency.")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="train_and_score", choices=["train_and_score", "score_only"])
    args = ap.parse_args()
    if args.mode == "train_and_score":
        train_and_score()
    else:
        score_only()

if __name__ == "__main__":
    main()
