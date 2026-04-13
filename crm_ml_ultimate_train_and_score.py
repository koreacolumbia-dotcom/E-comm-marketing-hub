#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TOTAL VIEW 전용 회원 ML 파이프라인
- member_id 기반
- 기존 회원 CRM/주문 이력 중심
- TOTAL VIEW에서 바로 활용 가능한 score table 생성

Outputs (BigQuery):
- crm_mart.crm_member_totalview_feature_store
- crm_mart.crm_member_totalview_labels
- crm_mart.crm_member_totalview_model_metrics
- crm_mart.crm_member_totalview_scores
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from dataclasses import dataclass
from typing import Any, Iterable

import numpy as np
import pandas as pd

try:
    from google.cloud import bigquery  # type: ignore
except Exception:
    bigquery = None

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import roc_auc_score, average_precision_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
from sklearn.calibration import CalibratedClassifierCV
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split

try:
    from lightgbm import LGBMClassifier, LGBMRegressor  # type: ignore
    HAS_LGBM = True
except Exception:
    HAS_LGBM = False


PROJECT_ID = os.getenv("MEMBER_FUNNEL_PROJECT_ID", os.getenv("CRM_ML_PROJECT_ID", "")).strip()
BQ_LOCATION = os.getenv("MEMBER_FUNNEL_BQ_LOCATION", os.getenv("CRM_ML_BQ_LOCATION", "asia-northeast3")).strip()

BASE_TABLE = os.getenv("CRM_ML_BASE_TABLE", os.getenv("MEMBER_FUNNEL_BASE_TABLE", "crm_mart.member_funnel_master")).strip()
DATASET = os.getenv("CRM_ML_BQ_DATASET", "crm_mart").strip()

FEATURE_TABLE = os.getenv("CRM_MEMBER_TOTALVIEW_FEATURE_TABLE", f"{DATASET}.crm_member_totalview_feature_store").strip()
LABEL_TABLE = os.getenv("CRM_MEMBER_TOTALVIEW_LABEL_TABLE", f"{DATASET}.crm_member_totalview_labels").strip()
METRICS_TABLE = os.getenv("CRM_MEMBER_TOTALVIEW_METRICS_TABLE", f"{DATASET}.crm_member_totalview_model_metrics").strip()
SCORES_TABLE = os.getenv("CRM_MEMBER_TOTALVIEW_SCORES_TABLE", f"{DATASET}.crm_member_totalview_scores").strip()

REPURCHASE_DAYS = int(os.getenv("CRM_TOTALVIEW_REPURCHASE_DAYS", "60"))
CHURN_DAYS = int(os.getenv("CRM_TOTALVIEW_CHURN_DAYS", "120"))
LTV_HORIZON_DAYS = int(os.getenv("CRM_TOTALVIEW_LTV_HORIZON_DAYS", "180"))

TOP_CATEGORY_MIN_COUNT = int(os.getenv("CRM_TOTALVIEW_TOP_CATEGORY_MIN_COUNT", "20"))
MIN_TRAIN_ROWS = int(os.getenv("CRM_TOTALVIEW_MIN_TRAIN_ROWS", "300"))
RANDOM_STATE = int(os.getenv("CRM_TOTALVIEW_RANDOM_STATE", "42"))


def qname(name: str) -> str:
    n = str(name or "").strip().strip("`")
    if not n:
        return n
    if n.count(".") == 2:
        return n
    if n.count(".") == 1 and PROJECT_ID:
        return f"{PROJECT_ID}.{n}"
    return n


def get_client():
    if bigquery is None:
        raise RuntimeError("google-cloud-bigquery is not installed")
    return bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)


def to_date(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.date


def num(s: pd.Series | Iterable[Any], default: float = 0.0) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(default)


def safe_div(a: pd.Series, b: pd.Series) -> pd.Series:
    a = num(a)
    b = num(b)
    return a / b.replace(0, np.nan)


def season_flags(today: dt.date) -> dict[str, int]:
    m = today.month
    return {
        "is_winter_season": int(m in [10, 11, 12, 1, 2]),
        "is_summer_season": int(m in [5, 6, 7, 8]),
    }


@dataclass
class ModelBundle:
    name: str
    model: Any | None
    metrics: dict[str, Any]
    target_col: str
    kind: str  # binary, multiclass, regression


NUM_COLS: list[str] = [
    "age",
    "days_since_signup",
    "days_since_last_visit",
    "days_since_last_purchase",
    "total_sessions",
    "total_pageviews",
    "product_view_count",
    "add_to_cart_count",
    "order_count",
    "total_revenue",
    "aov",
    "pv_per_session",
    "atc_per_pdp",
    "revenue_per_order",
    "session_velocity_7_30",
    "pdp_velocity_7_30",
    "atc_velocity_7_30",
    "purchase_gap_ratio",
    "orders_per_30d",
    "revenue_per_30d",
    "recent_intent_score",
    "category_focus_index",
    "channel_concentration",
    "discount_sensitivity",
    "coupon_dependency",
    "winter_intent_score",
    "summer_intent_score",
    "consent_score",
]

CAT_COLS: list[str] = [
    "gender",
    "channel_group",
    "top_category",
    "top_product",
    "age_band",
]


def load_member_base() -> pd.DataFrame:
    client = get_client()
    sql = f"""
    SELECT *
    FROM `{qname(BASE_TABLE)}`
    WHERE COALESCE(CAST(member_id AS STRING), '') != ''
    """
    df = client.query(sql, location=BQ_LOCATION).to_dataframe()
    if df.empty:
        return df

    # normalize columns
    low = {c.lower(): c for c in df.columns}

    def col(*names: str) -> str | None:
        for n in names:
            if n.lower() in low:
                return low[n.lower()]
        return None

    out = pd.DataFrame()
    out["member_id"] = df[col("member_id")] if col("member_id") else ""
    out["user_id"] = df[col("user_id")] if col("user_id") else ""
    out["signup_date"] = to_date(df[col("signup_date", "member_regdate", "member_reg_date")] if col("signup_date", "member_regdate", "member_reg_date") else pd.Series([None] * len(df)))
    out["last_visit_date"] = to_date(df[col("event_date", "last_visit_date", "date")] if col("event_date", "last_visit_date", "date") else pd.Series([None] * len(df)))
    out["last_order_date"] = to_date(df[col("last_order_date", "order_date")] if col("last_order_date", "order_date") else pd.Series([None] * len(df)))

    out["age"] = num(df[col("age")] if col("age") else pd.Series([np.nan] * len(df)), np.nan)
    out["gender"] = (df[col("gender")] if col("gender") else "미확인").astype(str).replace({"M": "MALE", "F": "FEMALE", "1": "MALE", "2": "FEMALE"}).fillna("미확인")
    out["age_band"] = pd.cut(out["age"], bins=[0, 19, 29, 39, 49, 59, 200], labels=["10s", "20s", "30s", "40s", "50s", "60+"]).astype(object).fillna("미확인")

    out["channel_group"] = (df[col("channel_group", "channel_group_enhanced")] if col("channel_group", "channel_group_enhanced") else "Unknown").astype(str).fillna("Unknown")
    out["top_category"] = (df[col("top_category", "preferred_category")] if col("top_category", "preferred_category") else "미분류").astype(str).fillna("미분류")
    out["top_product"] = (df[col("purchase_product_name", "top_product_name", "top_product")] if col("purchase_product_name", "top_product_name", "top_product") else "미분류").astype(str).fillna("미분류")

    out["total_sessions"] = num(df[col("total_sessions", "sessions")] if col("total_sessions", "sessions") else 0)
    out["total_pageviews"] = num(df[col("total_pageviews", "pageviews")] if col("total_pageviews", "pageviews") else 0)
    out["product_view_count"] = num(df[col("product_view_count")] if col("product_view_count") else 0)
    out["add_to_cart_count"] = num(df[col("add_to_cart_count")] if col("add_to_cart_count") else 0)
    out["order_count"] = num(df[col("order_count", "orders")] if col("order_count", "orders") else 0)
    out["total_revenue"] = num(df[col("total_revenue", "revenue")] if col("total_revenue", "revenue") else 0)
    out["coupon_used_total"] = num(df[col("coupon_used", "coupon_used_total")] if col("coupon_used", "coupon_used_total") else 0)
    out["cancel_amount_total"] = num(df[col("cancel_amount")] if col("cancel_amount") else 0)

    # consent
    is_mailing = num(df[col("is_mailing")] if col("is_mailing") else 0)
    is_sms = num(df[col("is_sms")] if col("is_sms") else 0)
    is_alimtalk = num(df[col("is_alimtalk")] if col("is_alimtalk") else 0)
    out["consent_score"] = ((is_mailing > 0).astype(int) + (is_sms > 0).astype(int) + (is_alimtalk > 0).astype(int)) / 3.0

    today = dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).date()
    out["snapshot_date"] = today
    out["days_since_signup"] = (today - out["signup_date"]).apply(lambda x: x.days if pd.notna(x) else np.nan)
    out["days_since_last_visit"] = (today - out["last_visit_date"]).apply(lambda x: x.days if pd.notna(x) else np.nan)
    out["days_since_last_purchase"] = (today - out["last_order_date"]).apply(lambda x: x.days if pd.notna(x) else np.nan)

    # derived
    out["aov"] = safe_div(out["total_revenue"], out["order_count"]).replace([np.inf, -np.inf], np.nan).fillna(0)
    out["pv_per_session"] = safe_div(out["total_pageviews"], out["total_sessions"]).replace([np.inf, -np.inf], np.nan).fillna(0)
    out["atc_per_pdp"] = safe_div(out["add_to_cart_count"], out["product_view_count"]).replace([np.inf, -np.inf], np.nan).fillna(0)
    out["revenue_per_order"] = safe_div(out["total_revenue"], out["order_count"]).replace([np.inf, -np.inf], np.nan).fillna(0)

    # v6-style proxy advanced features from available columns
    # when true 7d/30d rolling cols are absent, use approximate proxies based on intent and recency
    out["session_velocity_7_30"] = ((out["total_sessions"] * (30 / np.maximum(out["days_since_signup"].fillna(30).clip(lower=7), 7))) + 1) / ((out["total_sessions"] / 4.3) + 1)
    out["pdp_velocity_7_30"] = ((out["product_view_count"] * (30 / np.maximum(out["days_since_last_visit"].fillna(30).clip(lower=7), 7))) + 1) / ((out["product_view_count"] / 4.3) + 1)
    out["atc_velocity_7_30"] = ((out["add_to_cart_count"] * (30 / np.maximum(out["days_since_last_visit"].fillna(30).clip(lower=7), 7))) + 1) / ((out["add_to_cart_count"] / 4.3) + 1)

    avg_gap = np.where(out["order_count"] > 1, out["days_since_signup"] / np.maximum(out["order_count"], 1), np.nan)
    out["purchase_gap_ratio"] = ((out["days_since_last_purchase"].fillna(999) + 1) / (pd.Series(avg_gap).fillna(out["days_since_last_purchase"].fillna(999) + 1) + 1)).replace([np.inf, -np.inf], np.nan).fillna(1.0)
    out["orders_per_30d"] = safe_div(out["order_count"] * 30.0, out["days_since_signup"].clip(lower=30)).replace([np.inf, -np.inf], np.nan).fillna(0)
    out["revenue_per_30d"] = safe_div(out["total_revenue"] * 30.0, out["days_since_signup"].clip(lower=30)).replace([np.inf, -np.inf], np.nan).fillna(0)
    out["recent_intent_score"] = (
        out["pv_per_session"] * 0.15
        + out["product_view_count"] * 0.2
        + out["add_to_cart_count"] * 0.45
        + (1 / np.maximum(out["days_since_last_visit"].fillna(365), 1)) * 25.0
    )
    out["category_focus_index"] = safe_div(out["product_view_count"], out["total_pageviews"] + 1).fillna(0)
    out["channel_concentration"] = 1.0  # one dominant channel per member in current mart snapshot
    out["discount_sensitivity"] = safe_div(out["coupon_used_total"], out["total_revenue"] + 1).fillna(0)
    out["coupon_dependency"] = safe_div(out["coupon_used_total"], out["total_revenue"] + 1).fillna(0)

    flags = season_flags(today)
    outerwear_signal = out["top_category"].astype(str).str.contains("Outer|아우터|JACKET|FLEECE", case=False, na=False).astype(int)
    footwear_signal = out["top_category"].astype(str).str.contains("Foot|슈즈|신발|BOOT|SHOE", case=False, na=False).astype(int)
    out["winter_intent_score"] = outerwear_signal * flags["is_winter_season"] * (out["recent_intent_score"] + 1)
    out["summer_intent_score"] = footwear_signal * flags["is_summer_season"] * (out["recent_intent_score"] + 1)

    return out.drop_duplicates("member_id", keep="first")


def top_decile_lift(y_true: pd.Series, y_score: pd.Series) -> float:
    try:
        d = pd.DataFrame({"y": y_true, "p": y_score}).dropna()
        if d.empty or d["y"].nunique() < 2:
            return 0.0
        base = float(d["y"].mean())
        if base <= 0:
            return 0.0
        top = d.sort_values("p", ascending=False).head(max(1, int(len(d) * 0.1)))
        return float(top["y"].mean() / base)
    except Exception:
        return 0.0


def make_labels(fs: pd.DataFrame) -> pd.DataFrame:
    df = fs.copy()

    # member_id 기반 TOTAL VIEW용 안정형 라벨
    # real future labels가 현재 mart 단일 snapshot으로 어렵기 때문에
    # "실제 CRM 운영에 쓸 수 있는 member-state label"로 설계
    # 1) repurchase propensity proxy: 구매자 중 recency/frequency/intent 상위
    buyers = df["order_count"] > 0
    rep_base = pd.Series(False, index=df.index)
    if buyers.any():
        sub = df.loc[buyers].copy()
        score = (
            (sub["order_count"].rank(pct=True) * 0.35)
            + ((1 - sub["days_since_last_purchase"].fillna(999).rank(pct=True)) * 0.35)
            + (sub["recent_intent_score"].rank(pct=True) * 0.20)
            + ((1 - sub["purchase_gap_ratio"].clip(upper=5).rank(pct=True)) * 0.10)
        )
        threshold = float(score.quantile(0.70))
        rep_base.loc[sub.index] = score >= threshold
    df["repurchase_score_label"] = rep_base.astype(int)

    # 2) first purchase propensity proxy: 비구매자 중 intent/consent/recency 상위
    non_buyers = df["order_count"] <= 0
    fp_base = pd.Series(False, index=df.index)
    if non_buyers.any():
        sub = df.loc[non_buyers].copy()
        score = (
            (sub["recent_intent_score"].rank(pct=True) * 0.50)
            + (sub["add_to_cart_count"].rank(pct=True) * 0.20)
            + ((1 - sub["days_since_last_visit"].fillna(365).rank(pct=True)) * 0.20)
            + (sub["consent_score"].rank(pct=True) * 0.10)
        )
        threshold = float(score.quantile(0.80))
        fp_base.loc[sub.index] = score >= threshold
    df["first_purchase_score_label"] = fp_base.astype(int)

    # 3) churn risk: 기존 구매자 중 recency 악화 + intent 낮음
    churn = pd.Series(False, index=df.index)
    if buyers.any():
        sub = df.loc[buyers].copy()
        score = (
            (sub["days_since_last_purchase"].fillna(999).rank(pct=True) * 0.50)
            + (sub["purchase_gap_ratio"].rank(pct=True) * 0.25)
            + ((1 - sub["recent_intent_score"].rank(pct=True)) * 0.15)
            + ((1 - sub["session_velocity_7_30"].rank(pct=True)) * 0.10)
        )
        threshold = float(score.quantile(0.75))
        churn.loc[sub.index] = score >= threshold
    df["churn_risk_label"] = churn.astype(int)

    # 4) ltv regression target proxy
    df["ltv_target"] = (
        df["total_revenue"] * 0.6
        + (df["order_count"] * 50000)
        + (df["aov"] * 0.4)
    )

    # 5) next category target: existing top_category but trimmed to trainable classes
    vc = df["top_category"].fillna("미분류").astype(str).value_counts()
    keep = set(vc[vc >= TOP_CATEGORY_MIN_COUNT].index.tolist())
    df["next_category_target"] = df["top_category"].astype(str).where(df["top_category"].astype(str).isin(keep), "OTHER")

    return df


def build_preprocessor() -> ColumnTransformer:
    num_pipe = Pipeline(steps=[("imputer", SimpleImputer(strategy="median"))])
    cat_pipe = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("ohe", OneHotEncoder(handle_unknown="ignore")),
    ])
    return ColumnTransformer(
        transformers=[
            ("num", num_pipe, NUM_COLS),
            ("cat", cat_pipe, CAT_COLS),
        ]
    )


def make_binary_estimator():
    if HAS_LGBM:
        return LGBMClassifier(
            n_estimators=300,
            learning_rate=0.03,
            num_leaves=31,
            subsample=0.9,
            colsample_bytree=0.9,
            random_state=RANDOM_STATE,
            class_weight="balanced",
        )
    return HistGradientBoostingClassifier(
        learning_rate=0.05,
        max_depth=6,
        max_iter=350,
        random_state=RANDOM_STATE,
    )


def make_reg_estimator():
    if HAS_LGBM:
        return LGBMRegressor(
            n_estimators=350,
            learning_rate=0.03,
            num_leaves=31,
            subsample=0.9,
            colsample_bytree=0.9,
            random_state=RANDOM_STATE,
        )
    return HistGradientBoostingRegressor(
        learning_rate=0.05,
        max_depth=6,
        max_iter=350,
        random_state=RANDOM_STATE,
    )


def fit_binary(df: pd.DataFrame, target_col: str) -> ModelBundle:
    data = df.dropna(subset=[target_col]).copy()
    data = data[data[target_col].isin([0, 1])]
    dist = data[target_col].value_counts().to_dict()
    if len(data) < MIN_TRAIN_ROWS or len(dist) < 2 or min(dist.values()) < 30:
        return ModelBundle(target_col, None, {"status": "skipped", "reason": f"insufficient class balance {dist}"}, target_col, "binary")

    train, valid = train_test_split(
        data, test_size=0.2, random_state=RANDOM_STATE, stratify=data[target_col]
    )
    if train.empty or valid.empty:
        return ModelBundle(target_col, None, {"status": "skipped", "reason": "empty split"}, target_col, "binary")

    pre = build_preprocessor()
    X_train = train[NUM_COLS + CAT_COLS]
    X_valid = valid[NUM_COLS + CAT_COLS]
    y_train = train[target_col].astype(int)
    y_valid = valid[target_col].astype(int)

    estimator = make_binary_estimator()
    model = Pipeline(steps=[("pre", pre), ("model", estimator)])
    model.fit(X_train, y_train)

    # optional calibration
    calibrated = model
    try:
        if len(np.unique(y_valid)) >= 2 and len(valid) >= 100:
            Xt = model.named_steps["pre"].transform(X_train)
            base = make_binary_estimator()
            base.fit(Xt, y_train)
            cal = CalibratedClassifierCV(base, method="sigmoid", cv="prefit")
            cal.fit(model.named_steps["pre"].transform(X_valid), y_valid)
            calibrated = Pipeline(steps=[("pre", model.named_steps["pre"]), ("model", cal)])
    except Exception:
        calibrated = model

    p = calibrated.predict_proba(X_valid)[:, 1]
    metrics = {
        "status": "ok",
        "rows_train": int(len(train)),
        "rows_valid": int(len(valid)),
        "positive_rate_train": float(y_train.mean()),
        "positive_rate_valid": float(y_valid.mean()),
        "roc_auc": float(roc_auc_score(y_valid, p)) if len(np.unique(y_valid)) >= 2 else None,
        "pr_auc": float(average_precision_score(y_valid, p)) if len(np.unique(y_valid)) >= 2 else None,
        "top_decile_lift": float(top_decile_lift(y_valid, p)),
    }
    return ModelBundle(target_col, calibrated, metrics, target_col, "binary")


def fit_multiclass(df: pd.DataFrame, target_col: str) -> ModelBundle:
    data = df.dropna(subset=[target_col]).copy()
    vc = data[target_col].astype(str).value_counts()
    if len(vc) < 2:
        return ModelBundle(target_col, None, {"status": "skipped", "reason": "insufficient multiclass targets"}, target_col, "multiclass")

    # use multinomial logistic on preprocessed matrix for stability
    train, valid = train_test_split(
        data, test_size=0.2, random_state=RANDOM_STATE, stratify=data[target_col]
    )
    if train.empty or valid.empty:
        return ModelBundle(target_col, None, {"status": "skipped", "reason": "empty split"}, target_col, "multiclass")

    pre = build_preprocessor()
    X_train = pre.fit_transform(train[NUM_COLS + CAT_COLS])
    X_valid = pre.transform(valid[NUM_COLS + CAT_COLS])
    y_train = train[target_col].astype(str)
    y_valid = valid[target_col].astype(str)

    clf = LogisticRegression(max_iter=1000, multi_class="auto")
    clf.fit(X_train, y_train)

    class Wrapped:
        def __init__(self, pre, clf):
            self.pre = pre
            self.clf = clf
            self.classes_ = clf.classes_
        def predict(self, X):
            return self.clf.predict(self.pre.transform(X))
        def predict_proba(self, X):
            return self.clf.predict_proba(self.pre.transform(X))

    metrics = {
        "status": "ok",
        "rows_train": int(len(train)),
        "rows_valid": int(len(valid)),
        "n_classes": int(len(clf.classes_)),
    }
    return ModelBundle(target_col, Wrapped(pre, clf), metrics, target_col, "multiclass")


def fit_regression(df: pd.DataFrame, target_col: str) -> ModelBundle:
    data = df.dropna(subset=[target_col]).copy()
    if len(data) < MIN_TRAIN_ROWS:
        return ModelBundle(target_col, None, {"status": "skipped", "reason": "insufficient regression rows"}, target_col, "regression")

    train, valid = train_test_split(data, test_size=0.2, random_state=RANDOM_STATE)
    pre = build_preprocessor()
    reg = make_reg_estimator()
    model = Pipeline(steps=[("pre", pre), ("model", reg)])
    model.fit(train[NUM_COLS + CAT_COLS], train[target_col])

    p = model.predict(valid[NUM_COLS + CAT_COLS])
    mae = float(np.mean(np.abs(valid[target_col] - p))) if len(valid) else None
    metrics = {
        "status": "ok",
        "rows_train": int(len(train)),
        "rows_valid": int(len(valid)),
        "mae": mae,
    }
    return ModelBundle(target_col, model, metrics, target_col, "regression")


def score_current(fs: pd.DataFrame, bundles: list[ModelBundle]) -> pd.DataFrame:
    current = fs.copy()
    for col, default in [
        ("repurchase_score", 0.0),
        ("first_purchase_score", 0.0),
        ("churn_risk_score", 0.0),
        ("ltv_score", 0.0),
        ("next_best_category", ""),
    ]:
        if col not in current.columns:
            current[col] = default

    X = current[NUM_COLS + CAT_COLS].copy()

    for bundle in bundles:
        if bundle.model is None:
            continue
        if bundle.kind == "binary":
            p = bundle.model.predict_proba(X)[:, 1]
            if bundle.target_col == "repurchase_score_label":
                current["repurchase_score"] = p
            elif bundle.target_col == "first_purchase_score_label":
                current["first_purchase_score"] = p
            elif bundle.target_col == "churn_risk_label":
                current["churn_risk_score"] = p
        elif bundle.kind == "regression":
            if bundle.target_col == "ltv_target":
                current["ltv_score"] = bundle.model.predict(X)
        elif bundle.kind == "multiclass":
            if bundle.target_col == "next_category_target":
                current["next_best_category"] = bundle.model.predict(X)

    # scaled LTV
    ltv = num(current["ltv_score"])
    if ltv.max() > ltv.min():
        current["ltv_score"] = (ltv - ltv.min()) / (ltv.max() - ltv.min())
    else:
        current["ltv_score"] = 0.0

    # action type
    action = np.full(len(current), "GENERAL", dtype=object)
    action = np.where(current["churn_risk_score"] >= 0.75, "CHURN_PREVENTION", action)
    action = np.where(current["repurchase_score"] >= 0.75, "RETENTION_REPURCHASE", action)
    action = np.where((current["order_count"] <= 0) & (current["first_purchase_score"] >= 0.80), "FIRST_PURCHASE_NUDGE", action)
    action = np.where((current["ltv_score"] >= 0.85) & (current["order_count"] >= 2), "VIP_UPSELL", action)
    action = np.where((action == "GENERAL") & current["next_best_category"].astype(str).ne(""), "CATEGORY_CROSSSELL", action)
    current["crm_action_type"] = action

    priority = np.full(len(current), "P3", dtype=object)
    priority = np.where((current["ltv_score"] >= 0.8) | (current["repurchase_score"] >= 0.8), "P1", priority)
    priority = np.where(((current["churn_risk_score"] >= 0.7) | (current["first_purchase_score"] >= 0.75)) & (priority != "P1"), "P2", priority)
    current["priority_tier"] = priority

    current["predicted_member_stage"] = np.select(
        [
            current["order_count"] <= 0,
            current["order_count"] == 1,
            current["order_count"] >= 2,
            current["ltv_score"] >= 0.85,
        ],
        ["PROSPECT", "ONE_TIME_BUYER", "REPEAT_BUYER", "VIP"],
        default="GENERAL_MEMBER",
    )

    return current


def upload_dataframe(df: pd.DataFrame, table_name: str) -> None:
    client = get_client()
    full_name = qname(table_name)
    df2 = df.copy()
    for c in df2.columns:
        if str(df2[c].dtype) == "object":
            df2[c] = df2[c].astype(str)
    job = client.load_table_from_dataframe(df2, full_name, location=BQ_LOCATION)
    job.result()


def train_and_score() -> None:
    fs = load_member_base()
    if fs.empty:
        print("[WARN] member base empty -> skip ML")
        return

    labels = make_labels(fs)

    # upload feature / label store
    upload_dataframe(fs, FEATURE_TABLE)
    upload_dataframe(labels[["member_id", "repurchase_score_label", "first_purchase_score_label", "churn_risk_label", "ltv_target", "next_category_target"]], LABEL_TABLE)

    bundles = [
        fit_binary(labels, "repurchase_score_label"),
        fit_binary(labels, "first_purchase_score_label"),
        fit_binary(labels, "churn_risk_label"),
        fit_regression(labels, "ltv_target"),
        fit_multiclass(labels, "next_category_target"),
    ]

    metrics_rows = []
    run_at = dt.datetime.now(dt.timezone.utc).isoformat()
    for b in bundles:
        row = {
            "model_name": b.target_col,
            "model_version": "member_totalview_v1",
            "evaluated_at": run_at,
            "metrics_json": json.dumps(b.metrics, ensure_ascii=False),
        }
        metrics_rows.append(row)
        if b.metrics.get("status") != "ok":
            print(f"[WARN] {b.target_col} skipped: {b.metrics.get('reason')}")
    if metrics_rows:
        upload_dataframe(pd.DataFrame(metrics_rows), METRICS_TABLE)

    scored = score_current(fs, bundles)
    out = scored[[
        "member_id", "user_id", "age", "gender", "age_band",
        "channel_group", "top_category", "top_product",
        "order_count", "total_revenue", "aov",
        "signup_date", "last_visit_date", "last_order_date",
        "days_since_signup", "days_since_last_visit", "days_since_last_purchase",
        "repurchase_score", "first_purchase_score", "churn_risk_score", "ltv_score",
        "next_best_category", "crm_action_type", "priority_tier", "predicted_member_stage",
    ]].copy()
    upload_dataframe(out, SCORES_TABLE)

    print(f"[INFO] feature store written: {qname(FEATURE_TABLE)} rows={len(fs)}")
    print(f"[INFO] labels written: {qname(LABEL_TABLE)} rows={len(labels)}")
    print(f"[INFO] metrics written: {qname(METRICS_TABLE)} rows={len(metrics_rows)}")
    print(f"[INFO] scores written: {qname(SCORES_TABLE)} rows={len(out)}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--mode", default="train_and_score", choices=["train_and_score"])
    return p.parse_args()


def main() -> None:
    _ = parse_args()
    train_and_score()


if __name__ == "__main__":
    main()
