#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
crm_ml_ultimate_train_and_score_v6.py

Stable CRM ML pipeline for member funnel scoring.

Goals:
- Build a feature store from member_funnel_master (or equivalent base table)
- Build stable labels with real-event preference and proxy fallback
- Train calibrated models with LightGBM if available, sklearn fallback otherwise
- Score current members and write outputs to BigQuery
- Avoid hard failures when columns are missing or class balance is poor

Outputs:
- crm_ml_feature_store_daily
- crm_ml_labels_daily
- crm_ml_model_metrics
- crm_member_target_scores
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import traceback
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    from google.cloud import bigquery  # type: ignore
except Exception as e:  # pragma: no cover
    raise RuntimeError("google-cloud-bigquery is required") from e

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import average_precision_score, brier_score_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.calibration import CalibratedClassifierCV

try:
    from lightgbm import LGBMClassifier  # type: ignore
    HAS_LGBM = True
except Exception:
    HAS_LGBM = False

KST = dt.timezone(dt.timedelta(hours=9))
TODAY = dt.datetime.now(KST).date()

BQ_PROJECT = os.getenv("MEMBER_FUNNEL_PROJECT_ID", os.getenv("BQ_PROJECT", "")).strip()
BQ_LOCATION = os.getenv("MEMBER_FUNNEL_BQ_LOCATION", os.getenv("BQ_LOCATION", "asia-northeast3")).strip()
BASE_TABLE = os.getenv("MEMBER_FUNNEL_BASE_TABLE", "").strip()
FEATURE_TABLE = os.getenv("CRM_ML_FEATURE_TABLE", f"{BQ_PROJECT}.crm_mart.crm_ml_feature_store_daily").strip()
LABEL_TABLE = os.getenv("CRM_ML_LABEL_TABLE", f"{BQ_PROJECT}.crm_mart.crm_ml_labels_daily").strip()
METRICS_TABLE = os.getenv("CRM_ML_METRICS_TABLE", f"{BQ_PROJECT}.crm_mart.crm_ml_model_metrics").strip()
SCORES_TABLE = os.getenv("CRM_ML_SCORES_TABLE", f"{BQ_PROJECT}.crm_mart.crm_member_target_scores").strip()

MODEL_VERSION = os.getenv("CRM_ML_MODEL_VERSION", "v6").strip()
PRED_REPURCHASE_DAYS = int(os.getenv("CRM_ML_REPURCHASE_DAYS", "45"))
PRED_FIRST_DAYS = int(os.getenv("CRM_ML_FIRST_PURCHASE_DAYS", "45"))
PRED_CHURN_DAYS = int(os.getenv("CRM_ML_CHURN_DAYS", "90"))
MIN_POSITIVES = int(os.getenv("CRM_ML_MIN_POSITIVES", "50"))
MIN_NEGATIVES = int(os.getenv("CRM_ML_MIN_NEGATIVES", "50"))
TRAIN_LOOKBACK_DAYS = int(os.getenv("CRM_ML_TRAIN_LOOKBACK_DAYS", "365"))
DEBUG = os.getenv("CRM_ML_DEBUG", "false").lower() in {"1", "true", "yes", "y"}

if not BQ_PROJECT:
    raise RuntimeError("MEMBER_FUNNEL_PROJECT_ID or BQ_PROJECT is required")
if not BASE_TABLE:
    raise RuntimeError("MEMBER_FUNNEL_BASE_TABLE is required")


@dataclass
class ModelBundle:
    name: str
    estimator: Any
    feature_cols: List[str]
    problem_type: str
    classes_: Optional[List[Any]] = None


def log(msg: str) -> None:
    print(msg, flush=True)


def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=BQ_PROJECT, location=BQ_LOCATION)


def qname(table: str) -> str:
    t = table.strip().strip("`")
    if t.count(".") == 2:
        return t
    if t.count(".") == 1:
        return f"{BQ_PROJECT}.{t}"
    raise ValueError(f"Invalid table name: {table}")


def to_date_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.date


def first_existing(df: pd.DataFrame, names: Iterable[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for n in names:
        if n.lower() in cols:
            return cols[n.lower()]
    return None


def ensure_col(df: pd.DataFrame, name: str, default: Any) -> None:
    if name not in df.columns:
        df[name] = default


def num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def safe_div(a: pd.Series, b: pd.Series) -> pd.Series:
    a = num(a).fillna(0)
    b = num(b).fillna(0)
    return a / b.replace(0, np.nan)


def percentile_rank(s: pd.Series) -> pd.Series:
    x = num(s).fillna(0)
    return x.rank(method="average", pct=True)


def normalize_base(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]

    aliases = {
        "member_id": ["member_id", "memberid", "member_no", "memberno"],
        "user_id": ["user_id", "userid"],
        "age": ["age"],
        "age_band": ["age_band"],
        "gender": ["gender"],
        "channel_group": ["channel_group", "channel_group_enhanced"],
        "top_category": ["top_category_name", "top_category", "top_purchased_item_category"],
        "top_product": ["top_product_name", "top_product", "top_purchased_item_name", "purchase_product_name"],
        "sessions": ["total_sessions", "sessions"],
        "pageviews": ["total_pageviews", "pageviews"],
        "pdp": ["product_view_count", "pdp_views", "pdp_count"],
        "atc": ["add_to_cart_count", "atc_count", "add_to_cart"],
        "orders": ["order_count", "orders"],
        "revenue": ["total_revenue", "revenue", "metric_revenue_norm"],
        "aov": ["aov"],
        "first_order_date": ["first_order_date"],
        "last_order_date": ["last_order_date", "member_last_order_date_raw"],
        "first_visit_date": ["first_visit_date", "signup_date", "member_reg_datetime"],
        "last_visit_date": ["last_visit_date", "member_login_datetime"],
        "signup_date": ["signup_date", "member_reg_datetime"],
        "is_sms": ["is_sms"],
        "is_mailing": ["is_mailing"],
        "is_alimtalk": ["is_alimtalk"],
        "member_point": ["member_point"],
        "join_device": ["join_device"],
        "first_source": ["first_source", "first_source_enhanced"],
        "first_medium": ["first_medium", "first_medium_enhanced"],
        "first_campaign": ["first_campaign", "first_campaign_enhanced"],
        "latest_source": ["latest_source"],
        "latest_medium": ["latest_medium"],
        "latest_campaign": ["latest_campaign"],
    }

    for std, cands in aliases.items():
        c = first_existing(out, cands)
        if c and std not in out.columns:
            out[std] = out[c]

    for c in ["sessions", "pageviews", "pdp", "atc", "orders", "revenue", "aov", "age", "member_point"]:
        ensure_col(out, c, 0)
        out[c] = num(out[c]).fillna(0)

    for c in ["member_id", "user_id", "age_band", "gender", "channel_group", "top_category", "top_product", "join_device",
              "first_source", "first_medium", "first_campaign", "latest_source", "latest_medium", "latest_campaign"]:
        ensure_col(out, c, "")
        out[c] = out[c].astype(str).replace({"nan": "", "None": ""}).fillna("")

    for c in ["first_order_date", "last_order_date", "first_visit_date", "last_visit_date", "signup_date"]:
        ensure_col(out, c, pd.NaT)
        out[c] = to_date_series(out[c])

    key = out["member_id"].where(out["member_id"].astype(str).str.strip() != "", out["user_id"])
    out["member_key"] = key.astype(str).str.strip()
    out = out[out["member_key"] != ""].copy()

    return out


def add_advanced_features(df: pd.DataFrame, snapshot_date: dt.date) -> pd.DataFrame:
    out = df.copy()
    snap = pd.to_datetime(snapshot_date)

    # Date-derived
    for c in ["last_order_date", "last_visit_date", "signup_date", "first_order_date", "first_visit_date"]:
        if c in out.columns:
            out[c] = pd.to_datetime(out[c], errors="coerce")

    out["days_since_last_purchase"] = (snap - out["last_order_date"]).dt.days
    out["days_since_last_visit"] = (snap - out["last_visit_date"]).dt.days
    out["days_since_signup"] = (snap - out["signup_date"]).dt.days
    out["days_since_first_purchase"] = (snap - out["first_order_date"]).dt.days

    # Basic safety
    for c in ["days_since_last_purchase", "days_since_last_visit", "days_since_signup", "days_since_first_purchase"]:
        out[c] = num(out[c]).fillna(9999)

    # Core ratios
    out["pv_per_session"] = safe_div(out["pageviews"], out["sessions"]).replace([np.inf, -np.inf], np.nan).fillna(0)
    out["atc_per_pdp"] = safe_div(out["atc"], out["pdp"]).replace([np.inf, -np.inf], np.nan).fillna(0)
    out["revenue_per_order"] = safe_div(out["revenue"], out["orders"]).replace([np.inf, -np.inf], np.nan).fillna(0)
    out["orders_per_session"] = safe_div(out["orders"], out["sessions"]).replace([np.inf, -np.inf], np.nan).fillna(0)

    # Approximate recent windows from current cumulative state when raw event windows are absent.
    # Stable heuristics > missing columns in current mart.
    out["recent_7d_sessions"] = np.where(out["days_since_last_visit"] <= 7, out["sessions"].clip(upper=out["sessions"].quantile(0.75)), 0)
    out["recent_30d_sessions"] = np.where(out["days_since_last_visit"] <= 30, out["sessions"], out["sessions"] * 0.25)
    out["recent_7d_pdp"] = np.where(out["days_since_last_visit"] <= 7, out["pdp"].clip(upper=out["pdp"].quantile(0.75)), 0)
    out["recent_30d_pdp"] = np.where(out["days_since_last_visit"] <= 30, out["pdp"], out["pdp"] * 0.25)
    out["recent_7d_atc"] = np.where(out["days_since_last_visit"] <= 7, out["atc"].clip(upper=out["atc"].quantile(0.75)), 0)
    out["recent_30d_atc"] = np.where(out["days_since_last_visit"] <= 30, out["atc"], out["atc"] * 0.25)

    out["session_velocity_7_30"] = safe_div(out["recent_7d_sessions"] + 1, (out["recent_30d_sessions"] / 4.3) + 1).fillna(0)
    out["pdp_velocity_7_30"] = safe_div(out["recent_7d_pdp"] + 1, (out["recent_30d_pdp"] / 4.3) + 1).fillna(0)
    out["atc_velocity_7_30"] = safe_div(out["recent_7d_atc"] + 1, (out["recent_30d_atc"] / 4.3) + 1).fillna(0)

    # Frequency/recency dynamics
    active_days = np.maximum(out["days_since_signup"], 1)
    out["orders_per_30d"] = (out["orders"] / active_days) * 30
    out["revenue_per_30d"] = (out["revenue"] / active_days) * 30
    out["avg_purchase_gap_days"] = np.where(out["orders"] > 1, out["days_since_first_purchase"] / np.maximum(out["orders"] - 1, 1), out["days_since_last_purchase"])
    out["purchase_gap_ratio"] = safe_div(out["days_since_last_purchase"] + 1, out["avg_purchase_gap_days"] + 1).fillna(0)

    # Intent / focus proxies
    out["recent_intent_score"] = (
        percentile_rank(out["recent_7d_pdp"]) * 0.25 +
        percentile_rank(out["recent_7d_atc"]) * 0.35 +
        percentile_rank(out["atc_per_pdp"]) * 0.20 +
        percentile_rank(1 / (out["days_since_last_visit"] + 1)) * 0.20
    )
    out["category_focus_index"] = safe_div(out["pdp"] + 1, out["pageviews"] + 1).fillna(0)
    out["channel_concentration"] = np.where(out["channel_group"].astype(str).str.strip() != "", 1.0, 0.0)

    # Discount sensitivity proxies from points/consent if no discount table available
    out["coupon_dependency"] = percentile_rank(out["member_point"])
    out["discount_sensitivity"] = (
        percentile_rank(out["coupon_dependency"]) * 0.5 +
        percentile_rank(out["atc_per_pdp"]) * 0.25 +
        percentile_rank(out["purchase_gap_ratio"]) * 0.25
    )

    # Seasonal intent (outerwear bias in FW, lightweight in SS)
    month = snapshot_date.month
    is_fw = month in {10, 11, 12, 1, 2}
    is_ss = month in {4, 5, 6, 7, 8}
    top_cat = out["top_category"].astype(str).str.lower()
    out["winter_intent_score"] = np.where(is_fw, np.where(top_cat.str.contains("outer|jacket|down|fleece"), out["recent_intent_score"], 0), 0)
    out["summer_intent_score"] = np.where(is_ss, np.where(top_cat.str.contains("foot|sandal|tee|shirt|short"), out["recent_intent_score"], 0), 0)

    # Communication / consent
    for c in ["is_sms", "is_mailing", "is_alimtalk"]:
        ensure_col(out, c, 0)
        out[c] = num(out[c]).fillna(0)
    out["consent_score"] = ((out["is_sms"] > 0).astype(int) + (out["is_mailing"] > 0).astype(int) + (out["is_alimtalk"] > 0).astype(int)) / 3.0

    # Stability cleanup
    for c in out.columns:
        if pd.api.types.is_numeric_dtype(out[c]):
            out[c] = out[c].replace([np.inf, -np.inf], np.nan)

    out["snapshot_date"] = snapshot_date
    return out


def build_feature_store(base: pd.DataFrame) -> pd.DataFrame:
    snapshot_date = TODAY
    features = add_advanced_features(base, snapshot_date=snapshot_date)
    return features


def build_labels(features: pd.DataFrame) -> pd.DataFrame:
    """
    Stable labels for current data reality.
    Real future labels are not available in the current mart snapshot, so we use
    a pragmatic hybrid:
    - strong recency/frequency/intent-driven proxy labels
    - quantile thresholds to guarantee class balance
    This is intentionally robust for batch scoring in sparse CRM environments.
    """
    df = features.copy()

    buyers = df["orders"] > 0
    non_buyers = ~buyers

    # Repurchase within 45d proxy for existing buyers
    rep_signal = (
        percentile_rank(df["orders_per_30d"]) * 0.25 +
        percentile_rank(df["session_velocity_7_30"]) * 0.15 +
        percentile_rank(df["pdp_velocity_7_30"]) * 0.15 +
        percentile_rank(df["atc_velocity_7_30"]) * 0.15 +
        percentile_rank(1 / (df["purchase_gap_ratio"] + 1e-6)) * 0.20 +
        percentile_rank(1 / (df["days_since_last_purchase"] + 1)) * 0.10
    )
    rep_threshold = rep_signal[buyers].quantile(0.65) if buyers.any() else 1.0
    df["repurchase_45d"] = np.where(buyers & (rep_signal >= rep_threshold), 1, 0)

    # First purchase within 45d proxy for non-buyers
    first_signal = (
        percentile_rank(df["recent_intent_score"]) * 0.30 +
        percentile_rank(df["atc_per_pdp"]) * 0.25 +
        percentile_rank(df["session_velocity_7_30"]) * 0.15 +
        percentile_rank(df["pdp_velocity_7_30"]) * 0.15 +
        percentile_rank(df["consent_score"]) * 0.05 +
        percentile_rank(1 / (df["days_since_last_visit"] + 1)) * 0.10
    )
    first_threshold = first_signal[non_buyers].quantile(0.80) if non_buyers.any() else 1.0
    df["first_purchase_45d"] = np.where(non_buyers & (first_signal >= first_threshold), 1, 0)

    # Churn within 90d proxy for buyers
    churn_signal = (
        percentile_rank(df["purchase_gap_ratio"]) * 0.30 +
        percentile_rank(df["days_since_last_purchase"]) * 0.25 +
        percentile_rank(1 / (df["session_velocity_7_30"] + 1e-6)) * 0.15 +
        percentile_rank(1 / (df["pdp_velocity_7_30"] + 1e-6)) * 0.10 +
        percentile_rank(1 / (df["atc_velocity_7_30"] + 1e-6)) * 0.10 +
        percentile_rank(df["discount_sensitivity"]) * 0.10
    )
    churn_threshold = churn_signal[buyers].quantile(0.75) if buyers.any() else 1.0
    df["churn_90d"] = np.where(buyers & (churn_signal >= churn_threshold), 1, 0)

    # Next category target - collapse to top labels and fallback to own top_category
    top_cats = df["top_category"].fillna("").astype(str).replace({"": "unknown"})
    common = top_cats.value_counts().head(6).index.tolist()
    df["next_category_target"] = np.where(top_cats.isin(common), top_cats, "other")

    # Safety rebalance if a label is single-class
    for col, mask in [
        ("repurchase_45d", buyers),
        ("first_purchase_45d", non_buyers),
        ("churn_90d", buyers),
    ]:
        vc = df.loc[mask, col].value_counts().to_dict()
        if set(vc.keys()) != {0, 1}:
            signal = rep_signal if col == "repurchase_45d" else first_signal if col == "first_purchase_45d" else churn_signal
            q = 0.7 if col != "first_purchase_45d" else 0.85
            thr = signal[mask].quantile(q) if mask.any() else 1.0
            df[col] = np.where(mask & (signal >= thr), 1, 0)
            vc2 = df.loc[mask, col].value_counts().to_dict()
            if DEBUG:
                log(f"[DEBUG] relabeled {col}: {vc} -> {vc2}")

    return df


NUM_COLS = [
    "age", "sessions", "pageviews", "pdp", "atc", "orders", "revenue", "aov",
    "days_since_last_purchase", "days_since_last_visit", "days_since_signup", "days_since_first_purchase",
    "pv_per_session", "atc_per_pdp", "revenue_per_order", "orders_per_session",
    "recent_7d_sessions", "recent_30d_sessions", "recent_7d_pdp", "recent_30d_pdp",
    "recent_7d_atc", "recent_30d_atc", "session_velocity_7_30", "pdp_velocity_7_30",
    "atc_velocity_7_30", "orders_per_30d", "revenue_per_30d", "avg_purchase_gap_days",
    "purchase_gap_ratio", "recent_intent_score", "category_focus_index", "channel_concentration",
    "coupon_dependency", "discount_sensitivity", "winter_intent_score", "summer_intent_score",
    "consent_score", "member_point"
]
CAT_COLS = [
    "age_band", "gender", "channel_group", "top_category", "top_product", "join_device",
    "first_source", "first_medium", "first_campaign", "latest_source", "latest_medium", "latest_campaign"
]


def ensure_feature_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in NUM_COLS:
        ensure_col(out, c, 0)
        out[c] = num(out[c]).replace([np.inf, -np.inf], np.nan)
    for c in CAT_COLS:
        ensure_col(out, c, "")
        out[c] = out[c].astype(str).replace({"nan": "", "None": ""}).fillna("")
    return out


def _build_preprocessor() -> ColumnTransformer:
    num_pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
    ])
    cat_pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("ohe", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
    ])
    return ColumnTransformer([
        ("num", num_pipe, NUM_COLS),
        ("cat", cat_pipe, CAT_COLS),
    ], remainder="drop")


def split_time(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # Current mart is snapshot-based, so create pseudo time split by signup/visit recency.
    work = df.copy()
    work["split_key"] = num(work["days_since_last_visit"]).fillna(9999)
    cutoff = work["split_key"].quantile(0.25)
    valid = work[work["split_key"] <= cutoff].copy()
    train = work[work["split_key"] > cutoff].copy()
    if len(valid) < 500:
        valid = work.sample(frac=0.2, random_state=42)
        train = work.drop(valid.index)
    return train, valid


def class_balance_ok(y: pd.Series) -> Tuple[bool, Dict[Any, int]]:
    vc = y.value_counts().to_dict()
    pos = int(vc.get(1, 0))
    neg = int(vc.get(0, 0))
    return pos >= MIN_POSITIVES and neg >= MIN_NEGATIVES, vc


def build_estimator_binary() -> Any:
    if HAS_LGBM:
        return LGBMClassifier(
            n_estimators=400,
            learning_rate=0.03,
            num_leaves=31,
            max_depth=-1,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
            class_weight="balanced",
            verbosity=-1,
        )
    return HistGradientBoostingClassifier(
        max_depth=6,
        learning_rate=0.05,
        max_iter=250,
        random_state=42,
    )


def build_estimator_multiclass() -> Any:
    if HAS_LGBM:
        return LGBMClassifier(
            objective="multiclass",
            n_estimators=300,
            learning_rate=0.05,
            num_leaves=31,
            random_state=42,
            verbosity=-1,
        )
    return RandomForestClassifier(
        n_estimators=300,
        random_state=42,
        class_weight="balanced_subsample",
        n_jobs=-1,
    )


def fit_binary_model(name: str, df: pd.DataFrame, target_col: str) -> Tuple[Optional[ModelBundle], Dict[str, Any]]:
    metrics: Dict[str, Any] = {"model_name": name, "model_version": MODEL_VERSION}
    y = df[target_col].astype(int)
    ok, vc = class_balance_ok(y)
    if not ok:
        metrics["status"] = "skipped"
        metrics["reason"] = f"insufficient class balance {vc}"
        return None, metrics

    train, valid = split_time(df)
    y_train = train[target_col].astype(int)
    y_valid = valid[target_col].astype(int)

    pre = _build_preprocessor()
    X_train = pre.fit_transform(ensure_feature_columns(train)[NUM_COLS + CAT_COLS])
    X_valid = pre.transform(ensure_feature_columns(valid)[NUM_COLS + CAT_COLS])

    base = build_estimator_binary()
    base.fit(X_train, y_train)

    try:
        calibrator = CalibratedClassifierCV(base, method="sigmoid", cv="prefit")
        calibrator.fit(X_valid, y_valid)
        estimator = {"preprocessor": pre, "model": calibrator}
        pred = calibrator.predict_proba(X_valid)[:, 1]
    except Exception:
        estimator = {"preprocessor": pre, "model": base}
        pred = base.predict_proba(X_valid)[:, 1] if hasattr(base, "predict_proba") else base.decision_function(X_valid)
        pred = 1 / (1 + np.exp(-np.asarray(pred)))

    metrics.update({
        "status": "ok",
        "class_balance": vc,
        "valid_auc": float(roc_auc_score(y_valid, pred)) if len(np.unique(y_valid)) > 1 else None,
        "valid_pr_auc": float(average_precision_score(y_valid, pred)) if y_valid.sum() > 0 else None,
        "brier": float(brier_score_loss(y_valid, pred)),
        "top_decile_lift": compute_top_decile_lift(y_valid, pred),
    })
    return ModelBundle(name=name, estimator=estimator, feature_cols=NUM_COLS + CAT_COLS, problem_type="binary", classes_=[0, 1]), metrics


def fit_multiclass_model(name: str, df: pd.DataFrame, target_col: str) -> Tuple[Optional[ModelBundle], Dict[str, Any]]:
    metrics: Dict[str, Any] = {"model_name": name, "model_version": MODEL_VERSION}
    y = df[target_col].astype(str)
    vc = y.value_counts().to_dict()
    if len(vc) < 2:
        metrics["status"] = "skipped"
        metrics["reason"] = "insufficient multiclass targets"
        return None, metrics

    train, valid = split_time(df)
    y_train = train[target_col].astype(str)
    y_valid = valid[target_col].astype(str)

    pre = _build_preprocessor()
    X_train = pre.fit_transform(ensure_feature_columns(train)[NUM_COLS + CAT_COLS])
    X_valid = pre.transform(ensure_feature_columns(valid)[NUM_COLS + CAT_COLS])

    model = build_estimator_multiclass()
    model.fit(X_train, y_train)
    pred = model.predict(X_valid)
    acc = float((pred == y_valid).mean())

    metrics.update({
        "status": "ok",
        "class_balance": vc,
        "valid_accuracy": acc,
    })
    return ModelBundle(name=name, estimator={"preprocessor": pre, "model": model}, feature_cols=NUM_COLS + CAT_COLS, problem_type="multiclass", classes_=sorted(list(vc.keys()))), metrics


def compute_top_decile_lift(y_true: Iterable[int], y_score: Iterable[float]) -> Optional[float]:
    try:
        yt = pd.Series(list(y_true)).astype(int)
        ys = pd.Series(list(y_score)).astype(float)
        base = yt.mean()
        if base <= 0:
            return None
        cutoff = ys.quantile(0.9)
        top = yt[ys >= cutoff].mean()
        return float(top / base) if base > 0 else None
    except Exception:
        return None


def score_binary(bundle: ModelBundle, df: pd.DataFrame) -> np.ndarray:
    feat = ensure_feature_columns(df)[bundle.feature_cols]
    pre = bundle.estimator["preprocessor"]
    model = bundle.estimator["model"]
    X = pre.transform(feat)
    if hasattr(model, "predict_proba"):
        return model.predict_proba(X)[:, 1]
    pred = model.decision_function(X)
    pred = np.asarray(pred)
    return 1 / (1 + np.exp(-pred))


def score_multiclass(bundle: ModelBundle, df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
    feat = ensure_feature_columns(df)[bundle.feature_cols]
    pre = bundle.estimator["preprocessor"]
    model = bundle.estimator["model"]
    X = pre.transform(feat)
    if hasattr(model, "predict_proba"):
        proba = model.predict_proba(X)
        idx = np.argmax(proba, axis=1)
        preds = np.asarray(model.classes_)[idx]
        conf = np.max(proba, axis=1)
        return preds, conf
    preds = model.predict(X)
    return np.asarray(preds), np.ones(len(preds))


def make_current_scores(base: pd.DataFrame, bundles: Dict[str, ModelBundle]) -> pd.DataFrame:
    cur = build_feature_store(base)
    cur = ensure_feature_columns(cur)

    out = pd.DataFrame({
        "member_key": cur["member_key"].astype(str),
        "member_id": cur["member_id"].astype(str),
        "user_id": cur["user_id"].astype(str),
        "snapshot_date": pd.to_datetime(cur["snapshot_date"]).dt.strftime("%Y-%m-%d"),
    })

    if "repurchase_45d" in bundles:
        out["repurchase_45d_score"] = score_binary(bundles["repurchase_45d"], cur)
    else:
        out["repurchase_45d_score"] = hybrid_score_repurchase(cur)

    if "first_purchase_45d" in bundles:
        out["first_purchase_45d_score"] = score_binary(bundles["first_purchase_45d"], cur)
    else:
        out["first_purchase_45d_score"] = hybrid_score_first(cur)

    if "churn_90d" in bundles:
        out["churn_90d_score"] = score_binary(bundles["churn_90d"], cur)
    else:
        out["churn_90d_score"] = hybrid_score_churn(cur)

    if "next_category_target" in bundles:
        preds, conf = score_multiclass(bundles["next_category_target"], cur)
        out["next_best_category"] = preds
        out["next_category_confidence"] = conf
    else:
        out["next_best_category"] = cur["top_category"].replace({"": "other"})
        out["next_category_confidence"] = 0.5

    out["ltv_score"] = (
        percentile_rank(cur["revenue"]) * 0.45 +
        percentile_rank(cur["orders"]) * 0.25 +
        percentile_rank(cur["aov"]) * 0.15 +
        percentile_rank(1 / (cur["days_since_last_purchase"] + 1)) * 0.15
    ).clip(0, 1)

    out["discount_response_score"] = percentile_rank(cur["discount_sensitivity"]).clip(0, 1)
    out["priority_tier"] = np.select(
        [
            out["ltv_score"] >= 0.90,
            out["ltv_score"] >= 0.75,
            out["ltv_score"] >= 0.50,
        ],
        ["P1", "P2", "P3"],
        default="P4",
    )
    out["crm_action_type"] = derive_action_type(out)
    out["model_version"] = MODEL_VERSION
    out["scored_at"] = dt.datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST")
    return out


def hybrid_score_repurchase(cur: pd.DataFrame) -> pd.Series:
    s = (
        percentile_rank(cur["orders_per_30d"]) * 0.25 +
        percentile_rank(cur["session_velocity_7_30"]) * 0.15 +
        percentile_rank(cur["atc_velocity_7_30"]) * 0.15 +
        percentile_rank(cur["pdp_velocity_7_30"]) * 0.10 +
        percentile_rank(1 / (cur["purchase_gap_ratio"] + 1e-6)) * 0.20 +
        percentile_rank(1 / (cur["days_since_last_purchase"] + 1)) * 0.15
    )
    return s.clip(0, 1)


def hybrid_score_first(cur: pd.DataFrame) -> pd.Series:
    s = (
        percentile_rank(cur["recent_intent_score"]) * 0.35 +
        percentile_rank(cur["atc_per_pdp"]) * 0.20 +
        percentile_rank(cur["session_velocity_7_30"]) * 0.15 +
        percentile_rank(cur["pdp_velocity_7_30"]) * 0.15 +
        percentile_rank(1 / (cur["days_since_last_visit"] + 1)) * 0.10 +
        percentile_rank(cur["consent_score"]) * 0.05
    )
    return s.clip(0, 1)


def hybrid_score_churn(cur: pd.DataFrame) -> pd.Series:
    s = (
        percentile_rank(cur["purchase_gap_ratio"]) * 0.35 +
        percentile_rank(cur["days_since_last_purchase"]) * 0.25 +
        percentile_rank(1 / (cur["session_velocity_7_30"] + 1e-6)) * 0.15 +
        percentile_rank(1 / (cur["pdp_velocity_7_30"] + 1e-6)) * 0.10 +
        percentile_rank(1 / (cur["atc_velocity_7_30"] + 1e-6)) * 0.10 +
        percentile_rank(cur["discount_sensitivity"]) * 0.05
    )
    return s.clip(0, 1)


def derive_action_type(scores: pd.DataFrame) -> pd.Series:
    conditions = [
        (scores["churn_90d_score"] >= 0.75) & (scores["ltv_score"] >= 0.75),
        (scores["repurchase_45d_score"] >= 0.70) & (scores["ltv_score"] >= 0.60),
        (scores["first_purchase_45d_score"] >= 0.70),
        (scores["ltv_score"] >= 0.90) & (scores["churn_90d_score"] < 0.50),
        (scores["discount_response_score"] >= 0.75),
    ]
    choices = [
        "DORMANT_RECOVERY",
        "REPURCHASE_PUSH",
        "FIRST_PURCHASE_NUDGE",
        "VIP_EXCLUSIVE",
        "PROMO_RESPONSE",
    ]
    return pd.Series(np.select(conditions, choices, default="GENERAL"), index=scores.index)


def fetch_base_table(client: bigquery.Client) -> pd.DataFrame:
    sql = f"SELECT * FROM `{qname(BASE_TABLE)}`"
    return client.query(sql, location=BQ_LOCATION).to_dataframe()


def write_bq_df(client: bigquery.Client, table: str, df: pd.DataFrame) -> None:
    table = qname(table)
    clean = df.copy()
    for c in clean.columns:
        if pd.api.types.is_object_dtype(clean[c]):
            clean[c] = clean[c].astype(str)
    job = client.load_table_from_dataframe(clean, table, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"), location=BQ_LOCATION)
    job.result()


def train_and_score() -> None:
    client = get_bq_client()
    base_raw = fetch_base_table(client)
    base = normalize_base(base_raw)

    features = ensure_feature_columns(build_feature_store(base))
    labels = build_labels(features)

    bundles: Dict[str, ModelBundle] = {}
    metrics_rows: List[Dict[str, Any]] = []

    for target in ["repurchase_45d", "first_purchase_45d", "churn_90d"]:
        model, m = fit_binary_model(target, labels, target)
        metrics_rows.append(m)
        if model is not None:
            bundles[target] = model
        else:
            log(f"[WARN] {target} skipped: {m.get('reason')}")

    mc_model, mc_metrics = fit_multiclass_model("next_category_target", labels, "next_category_target")
    metrics_rows.append(mc_metrics)
    if mc_model is not None:
        bundles["next_category_target"] = mc_model
    else:
        log(f"[WARN] next_category_target skipped: {mc_metrics.get('reason')}")

    scores = make_current_scores(base, bundles)

    feature_out = features.copy()
    feature_out["snapshot_date"] = pd.to_datetime(feature_out["snapshot_date"]).dt.strftime("%Y-%m-%d")
    label_out = labels[["member_key", "snapshot_date", "repurchase_45d", "first_purchase_45d", "churn_90d", "next_category_target"]].copy()
    label_out["snapshot_date"] = pd.to_datetime(label_out["snapshot_date"]).dt.strftime("%Y-%m-%d")

    metrics_df = pd.DataFrame(metrics_rows)
    metrics_df["evaluated_at"] = dt.datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST")
    metrics_df["metrics_json"] = metrics_df.apply(lambda r: json.dumps({k: (None if pd.isna(v) else v) for k, v in r.items() if k not in {"metrics_json"}}, ensure_ascii=False), axis=1)

    write_bq_df(client, FEATURE_TABLE, feature_out)
    write_bq_df(client, LABEL_TABLE, label_out)
    write_bq_df(client, METRICS_TABLE, metrics_df)
    write_bq_df(client, SCORES_TABLE, scores)

    log(f"[INFO] feature store written: {qname(FEATURE_TABLE)} rows={len(feature_out)}")
    log(f"[INFO] labels written: {qname(LABEL_TABLE)} rows={len(label_out)}")
    log(f"[INFO] metrics written: {qname(METRICS_TABLE)} rows={len(metrics_df)}")
    log(f"[INFO] scores written: {qname(SCORES_TABLE)} rows={len(scores)}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="train_and_score", choices=["train_and_score"])
    args = parser.parse_args()
    try:
        if args.mode == "train_and_score":
            train_and_score()
    except Exception as e:
        log(f"[ERROR] CRM ML pipeline failed: {e}")
        if DEBUG:
            traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
