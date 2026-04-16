#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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
from sklearn.metrics import average_precision_score, roc_auc_score
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
TOP_CATEGORY_MIN_COUNT = int(os.getenv("CRM_TOTALVIEW_TOP_CATEGORY_MIN_COUNT", "20"))
MIN_TRAIN_ROWS = int(os.getenv("CRM_TOTALVIEW_MIN_TRAIN_ROWS", "300"))
MIN_CLASS_COUNT = int(os.getenv("CRM_TOTALVIEW_MIN_CLASS_COUNT", "30"))
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

def _to_date_series(s):
    return pd.to_datetime(pd.Series(s), errors="coerce").dt.date

def _num_series(s, default=0.0):
    return pd.to_numeric(pd.Series(s), errors="coerce").fillna(default)

def _safe_div(a, b, default=0.0):
    out = _num_series(a) / _num_series(b).replace(0, np.nan)
    return out.replace([np.inf, -np.inf], np.nan).fillna(default)

def _norm_name(s):
    return str(s or "").strip().lower().replace(" ", "").replace("_", "").replace("-", "").replace("/", "").replace("(", "").replace(")", "")

def _build_colmap(df):
    return {_norm_name(c): c for c in df.columns}

def _find_col(df, *candidates):
    cmap = _build_colmap(df)
    for c in candidates:
        key = _norm_name(c)
        if key in cmap:
            return cmap[key]
    return None

def safe_get_series(df, candidates, default=""):
    c = _find_col(df, *candidates)
    if c is None:
        return pd.Series([default] * len(df), index=df.index)
    return df[c]

def safe_str(df, candidates, default=""):
    s = safe_get_series(df, candidates, default=default)
    try:
        s = s.astype(str)
    except Exception:
        s = pd.Series([default] * len(df), index=df.index)
    return s.replace({"nan": default, "None": default, "NaT": default}).fillna(default)

def safe_num(df, candidates, default=0.0):
    return _num_series(safe_get_series(df, candidates, default=default), default=default)

def safe_date(df, candidates):
    return _to_date_series(safe_get_series(df, candidates, default=None))

def season_flags(today):
    m = today.month
    return {"is_winter_season": int(m in [10,11,12,1,2]), "is_summer_season": int(m in [5,6,7,8])}

@dataclass
class ModelBundle:
    name: str
    model: Any | None
    metrics: dict[str, Any]
    target_col: str
    kind: str

NUM_COLS = ["age","days_since_signup","days_since_last_visit","days_since_last_purchase","total_sessions","total_pageviews","product_view_count","add_to_cart_count","order_count","total_revenue","aov","pv_per_session","atc_per_pdp","revenue_per_order","session_velocity_7_30","pdp_velocity_7_30","atc_velocity_7_30","purchase_gap_ratio","orders_per_30d","revenue_per_30d","recent_intent_score","category_focus_index","channel_concentration","discount_sensitivity","coupon_dependency","winter_intent_score","summer_intent_score","consent_score","distinct_product_count","max_distinct_products_in_order","avg_distinct_products_per_order","multi_product_order_count","multi_product_customer_flag","multi_color_product_count","max_color_count","multi_color_flag","multi_size_product_count","max_size_count","multi_size_flag","repeat_item_product_count","max_repeat_item_orders","repeat_item_flag"]
CAT_COLS = ["gender","channel_group","top_category","top_product","age_band","top_multi_color_product_name","top_multi_size_product_name","top_repeat_product_name"]

def ensure_feature_columns(df):
    out = df.copy()
    for c in NUM_COLS:
        if c not in out.columns:
            out[c] = 0.0
        out[c] = _num_series(out[c], default=0.0)
    for c in CAT_COLS:
        if c not in out.columns:
            out[c] = "미분류"
        out[c] = out[c].astype(str).replace({"nan":"미분류","None":"미분류","NaT":"미분류"}).fillna("미분류")
    return out

def load_member_base():
    client = get_client()
    df = client.query(f"SELECT * FROM `{qname(BASE_TABLE)}`", location=BQ_LOCATION).to_dataframe()
    if df.empty:
        return pd.DataFrame()
    out = pd.DataFrame(index=df.index)
    out["member_id"] = safe_str(df, ["member_id","memberid","member_no","memberno"], default="")
    out = out[out["member_id"].astype(str).str.strip() != ""].copy()
    if out.empty:
        return pd.DataFrame()
    df = df.loc[out.index].copy()
    out["user_id"] = safe_str(df, ["user_id","userid"], default="")
    out["signup_date"] = safe_date(df, ["signup_date","member_regdate","member_reg_date","memberregdate","regdate"])
    out["last_visit_date"] = safe_date(df, ["event_date","last_visit_date","date","login_date"])
    out["last_order_date"] = safe_date(df, ["last_order_date","order_date","lastpurchase_date"])
    out["age"] = safe_num(df, ["age"], default=np.nan)
    gender = safe_str(df, ["gender","member_gender_raw"], default="미확인").str.upper()
    out["gender"] = gender.replace({"M":"MALE","F":"FEMALE","1":"MALE","2":"FEMALE","":"미확인"}).fillna("미확인")
    age_band_raw = safe_str(df, ["age_band"], default="")
    age_band_derived = pd.cut(out["age"], bins=[0,19,29,39,49,59,200], labels=["10s","20s","30s","40s","50s","60+"]).astype(object)
    out["age_band"] = age_band_raw.where(age_band_raw.astype(str).str.strip() != "", age_band_derived.fillna("미확인")).fillna("미확인")
    out["channel_group"] = safe_str(df, ["channel_group","channel_group_enhanced","first_channel_group"], default="Unknown")
    out["top_category"] = safe_str(df, ["top_category","preferred_category","top_category_name","top_purchased_item_category"], default="미분류")
    out["top_product"] = safe_str(df, ["purchase_product_name","top_product_name","top_product","top_purchased_item_name","product_name"], default="미분류")
    out["total_sessions"] = safe_num(df, ["total_sessions","sessions"], default=0.0)
    out["total_pageviews"] = safe_num(df, ["total_pageviews","pageviews"], default=0.0)
    out["product_view_count"] = safe_num(df, ["product_view_count","pdp_views","product_views"], default=0.0)
    out["add_to_cart_count"] = safe_num(df, ["add_to_cart_count","atc","cart_count"], default=0.0)
    out["order_count"] = safe_num(df, ["order_count","orders"], default=0.0)
    out["total_revenue"] = safe_num(df, ["total_revenue","revenue","erp_revenue_total","erp_price_total"], default=0.0)
    out["coupon_used_total"] = safe_num(df, ["coupon_used","coupon_used_total","orderusecouponprice"], default=0.0)
    out["point_used_total"] = safe_num(df, ["point_used","point_used_total","order_product_use_mileage_total","mileage_used_total"], default=0.0)
    is_mailing = safe_num(df, ["is_mailing","mailing_yn"], default=0.0)
    is_sms = safe_num(df, ["is_sms","sms_yn"], default=0.0)
    is_alimtalk = safe_num(df, ["is_alimtalk","alimtalk_yn"], default=0.0)
    out["consent_score"] = ((is_mailing > 0).astype(int) + (is_sms > 0).astype(int) + (is_alimtalk > 0).astype(int)) / 3.0
    today = dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).date()
    out["snapshot_date"] = today
    out["days_since_signup"] = pd.Series([(today - x).days if pd.notna(x) else np.nan for x in out["signup_date"]], index=out.index)
    out["days_since_last_visit"] = pd.Series([(today - x).days if pd.notna(x) else np.nan for x in out["last_visit_date"]], index=out.index)
    out["days_since_last_purchase"] = pd.Series([(today - x).days if pd.notna(x) else np.nan for x in out["last_order_date"]], index=out.index)
    out["aov"] = _safe_div(out["total_revenue"], out["order_count"], default=0.0)
    out["pv_per_session"] = _safe_div(out["total_pageviews"], out["total_sessions"], default=0.0)
    out["atc_per_pdp"] = _safe_div(out["add_to_cart_count"], out["product_view_count"], default=0.0)
    out["revenue_per_order"] = _safe_div(out["total_revenue"], out["order_count"], default=0.0)
    out["session_velocity_7_30"] = ((out["total_sessions"] * (30 / np.maximum(out["days_since_signup"].fillna(30).clip(lower=7), 7))) + 1) / ((out["total_sessions"] / 4.3) + 1)
    out["pdp_velocity_7_30"] = ((out["product_view_count"] * (30 / np.maximum(out["days_since_last_visit"].fillna(30).clip(lower=7), 7))) + 1) / ((out["product_view_count"] / 4.3) + 1)
    out["atc_velocity_7_30"] = ((out["add_to_cart_count"] * (30 / np.maximum(out["days_since_last_visit"].fillna(30).clip(lower=7), 7))) + 1) / ((out["add_to_cart_count"] / 4.3) + 1)
    avg_gap_s = pd.Series(np.where(out["order_count"] > 1, out["days_since_signup"] / np.maximum(out["order_count"], 1), np.nan), index=out.index)
    out["purchase_gap_ratio"] = ((out["days_since_last_purchase"].fillna(999) + 1) / (avg_gap_s.fillna(out["days_since_last_purchase"].fillna(999) + 1) + 1)).replace([np.inf, -np.inf], np.nan).fillna(1.0)
    out["orders_per_30d"] = _safe_div(out["order_count"] * 30.0, out["days_since_signup"].clip(lower=30), default=0.0)
    out["revenue_per_30d"] = _safe_div(out["total_revenue"] * 30.0, out["days_since_signup"].clip(lower=30), default=0.0)
    out["recent_intent_score"] = out["pv_per_session"] * 0.15 + out["product_view_count"] * 0.20 + out["add_to_cart_count"] * 0.45 + (1 / np.maximum(out["days_since_last_visit"].fillna(365), 1)) * 25.0
    out["category_focus_index"] = _safe_div(out["product_view_count"], out["total_pageviews"] + 1, default=0.0)
    out["channel_concentration"] = 1.0
    out["discount_sensitivity"] = _safe_div(out["coupon_used_total"], out["total_revenue"] + 1, default=0.0)
    out["coupon_dependency"] = _safe_div(out["coupon_used_total"], out["total_revenue"] + 1, default=0.0)
    flags = season_flags(today)
    top_cat = out["top_category"].astype(str)
    outerwear_signal = top_cat.str.contains("Outer|아우터|JACKET|FLEECE", case=False, na=False).astype(int)
    footwear_signal = top_cat.str.contains("Foot|슈즈|신발|BOOT|SHOE", case=False, na=False).astype(int)
    out["winter_intent_score"] = outerwear_signal * flags["is_winter_season"] * (out["recent_intent_score"] + 1)
    out["summer_intent_score"] = footwear_signal * flags["is_summer_season"] * (out["recent_intent_score"] + 1)
    out["distinct_product_count"] = safe_num(df, ["distinct_product_count"], default=0.0)
    out["max_distinct_products_in_order"] = safe_num(df, ["max_distinct_products_in_order"], default=0.0)
    out["avg_distinct_products_per_order"] = safe_num(df, ["avg_distinct_products_per_order"], default=0.0)
    out["multi_product_order_count"] = safe_num(df, ["multi_product_order_count"], default=0.0)
    out["multi_product_customer_flag"] = safe_num(df, ["multi_product_customer_flag"], default=0.0)
    out["multi_color_product_count"] = safe_num(df, ["multi_color_product_count"], default=0.0)
    out["max_color_count"] = safe_num(df, ["max_color_count"], default=0.0)
    out["multi_color_flag"] = safe_num(df, ["multi_color_flag"], default=0.0)
    out["multi_size_product_count"] = safe_num(df, ["multi_size_product_count"], default=0.0)
    out["max_size_count"] = safe_num(df, ["max_size_count"], default=0.0)
    out["multi_size_flag"] = safe_num(df, ["multi_size_flag"], default=0.0)
    out["repeat_item_product_count"] = safe_num(df, ["repeat_item_product_count"], default=0.0)
    out["max_repeat_item_orders"] = safe_num(df, ["max_repeat_item_orders"], default=0.0)
    out["repeat_item_flag"] = safe_num(df, ["repeat_item_flag"], default=0.0)
    out["top_multi_color_product_name"] = safe_str(df, ["top_multi_color_product_name"], default="미분류")
    out["top_multi_size_product_name"] = safe_str(df, ["top_multi_size_product_name"], default="미분류")
    out["top_repeat_product_name"] = safe_str(df, ["top_repeat_product_name"], default="미분류")
    return ensure_feature_columns(out).drop_duplicates("member_id", keep="first")

def top_decile_lift(y_true, y_score):
    try:
        d = pd.DataFrame({"y": y_true, "p": y_score}).dropna()
        if d.empty or d["y"].nunique() < 2:
            return 0.0
        base = float(d["y"].mean())
        if base <= 0:
            return 0.0
        top = d.sort_values("p", ascending=False).head(max(1, int(len(d)*0.1)))
        return float(top["y"].mean() / base)
    except Exception:
        return 0.0

def _quantile_binary(score, q):
    score = _num_series(score, default=0.0)
    if score.empty:
        return pd.Series(dtype=int)
    thr = float(score.quantile(q))
    if not np.isfinite(thr):
        thr = float(score.median()) if len(score) else 0.0
    y = (score >= thr).astype(int)
    if y.nunique() < 2 and len(score) >= 2:
        rk = score.rank(method="first", pct=True)
        y = (rk >= q).astype(int)
        if y.nunique() < 2:
            y.iloc[:max(1, len(y)//5)] = 1
            y.iloc[max(1, len(y)//5):] = 0
    return y

def make_labels(fs):
    df = fs.copy()
    buyers = df["order_count"] > 0
    non_buyers = ~buyers
    rep_labels = pd.Series(0, index=df.index, dtype=int)
    if buyers.any():
        sub = df.loc[buyers].copy()
        rep_score = sub["order_count"].rank(pct=True) * 0.28 + (1 - sub["days_since_last_purchase"].fillna(999).rank(pct=True)) * 0.28 + sub["recent_intent_score"].rank(pct=True) * 0.18 + (1 - sub["purchase_gap_ratio"].clip(upper=5).rank(pct=True)) * 0.08 + sub["repeat_item_flag"].rank(pct=True) * 0.08 + sub["multi_color_flag"].rank(pct=True) * 0.05 + sub["multi_size_flag"].rank(pct=True) * 0.05
        rep_labels.loc[sub.index] = _quantile_binary(rep_score, 0.70).astype(int)
    df["repurchase_score_label"] = rep_labels
    fp_labels = pd.Series(0, index=df.index, dtype=int)
    if non_buyers.any():
        sub = df.loc[non_buyers].copy()
        fp_score = sub["recent_intent_score"].rank(pct=True) * 0.42 + sub["add_to_cart_count"].rank(pct=True) * 0.18 + (1 - sub["days_since_last_visit"].fillna(365).rank(pct=True)) * 0.15 + sub["consent_score"].rank(pct=True) * 0.10 + sub["max_distinct_products_in_order"].rank(pct=True) * 0.10 + sub["multi_product_customer_flag"].rank(pct=True) * 0.05
        fp_labels.loc[sub.index] = _quantile_binary(fp_score, 0.80).astype(int)
    df["first_purchase_score_label"] = fp_labels
    churn_labels = pd.Series(0, index=df.index, dtype=int)
    if buyers.any():
        sub = df.loc[buyers].copy()
        churn_score = sub["days_since_last_purchase"].fillna(999).rank(pct=True) * 0.44 + sub["purchase_gap_ratio"].rank(pct=True) * 0.22 + (1 - sub["recent_intent_score"].rank(pct=True)) * 0.14 + (1 - sub["session_velocity_7_30"].rank(pct=True)) * 0.08 + (1 - sub["repeat_item_flag"].rank(pct=True)) * 0.06 + (1 - sub["multi_color_flag"].rank(pct=True)) * 0.06
        churn_labels.loc[sub.index] = _quantile_binary(churn_score, 0.75).astype(int)
    df["churn_risk_label"] = churn_labels
    df["ltv_target"] = df["total_revenue"] * 0.50 + (df["order_count"] * 50000) + (df["aov"] * 0.25) + (df["distinct_product_count"] * 12000) + (df["repeat_item_flag"] * 25000) + (df["multi_color_flag"] * 15000) + (df["multi_size_flag"] * 12000)
    vc = df["top_category"].fillna("미분류").astype(str).value_counts()
    keep = set(vc[vc >= TOP_CATEGORY_MIN_COUNT].index.tolist())
    df["next_category_target"] = df["top_category"].astype(str).where(df["top_category"].astype(str).isin(keep), "OTHER")
    return df

def build_preprocessor():
    num_pipe = Pipeline(steps=[("imputer", SimpleImputer(strategy="median"))])
    cat_pipe = Pipeline(steps=[("imputer", SimpleImputer(strategy="most_frequent")), ("ohe", OneHotEncoder(handle_unknown="ignore"))])
    return ColumnTransformer(transformers=[("num", num_pipe, NUM_COLS), ("cat", cat_pipe, CAT_COLS)])

def make_binary_estimator():
    if HAS_LGBM:
        return LGBMClassifier(n_estimators=250, learning_rate=0.04, num_leaves=31, subsample=0.9, colsample_bytree=0.9, random_state=RANDOM_STATE, class_weight="balanced")
    return HistGradientBoostingClassifier(learning_rate=0.05, max_depth=6, max_iter=300, random_state=RANDOM_STATE)

def make_reg_estimator():
    if HAS_LGBM:
        return LGBMRegressor(n_estimators=300, learning_rate=0.04, num_leaves=31, subsample=0.9, colsample_bytree=0.9, random_state=RANDOM_STATE)
    return HistGradientBoostingRegressor(learning_rate=0.05, max_depth=6, max_iter=300, random_state=RANDOM_STATE)

def fit_binary(df, target_col):
    data = ensure_feature_columns(df.dropna(subset=[target_col]).copy())
    data = data[data[target_col].isin([0,1])]
    dist = data[target_col].value_counts().to_dict()
    if len(data) < MIN_TRAIN_ROWS or len(dist) < 2 or min(dist.values()) < MIN_CLASS_COUNT:
        return ModelBundle(target_col, None, {"status":"skipped","reason":f"insufficient class balance {dist}"}, target_col, "binary")
    try:
        train, valid = train_test_split(data, test_size=0.2, random_state=RANDOM_STATE, stratify=data[target_col])
    except Exception:
        return ModelBundle(target_col, None, {"status":"skipped","reason":"train_test_split failed"}, target_col, "binary")
    if train.empty or valid.empty:
        return ModelBundle(target_col, None, {"status":"skipped","reason":"empty split"}, target_col, "binary")
    X_train = train[NUM_COLS + CAT_COLS].copy()
    X_valid = valid[NUM_COLS + CAT_COLS].copy()
    y_train = train[target_col].astype(int)
    y_valid = valid[target_col].astype(int)
    model = Pipeline(steps=[("pre", build_preprocessor()), ("model", make_binary_estimator())])
    try:
        model.fit(X_train, y_train)
    except Exception as e:
        return ModelBundle(target_col, None, {"status":"skipped","reason":f"fit failed: {e}"}, target_col, "binary")
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
    try:
        p = calibrated.predict_proba(X_valid)[:,1]
    except Exception:
        return ModelBundle(target_col, model, {"status":"ok_no_proba","rows_train":int(len(train)),"rows_valid":int(len(valid))}, target_col, "binary")
    metrics = {"status":"ok","rows_train":int(len(train)),"rows_valid":int(len(valid)),"positive_rate_train":float(y_train.mean()),"positive_rate_valid":float(y_valid.mean()),"roc_auc":float(roc_auc_score(y_valid, p)) if len(np.unique(y_valid)) >= 2 else None,"pr_auc":float(average_precision_score(y_valid, p)) if len(np.unique(y_valid)) >= 2 else None,"top_decile_lift":float(top_decile_lift(y_valid, p))}
    return ModelBundle(target_col, calibrated, metrics, target_col, "binary")

def fit_multiclass(df, target_col):
    data = ensure_feature_columns(df.dropna(subset=[target_col]).copy())
    vc = data[target_col].astype(str).value_counts()
    if len(data) < MIN_TRAIN_ROWS or len(vc) < 2:
        return ModelBundle(target_col, None, {"status":"skipped","reason":"insufficient multiclass targets"}, target_col, "multiclass")
    try:
        train, valid = train_test_split(data, test_size=0.2, random_state=RANDOM_STATE, stratify=data[target_col])
    except Exception:
        return ModelBundle(target_col, None, {"status":"skipped","reason":"train_test_split failed"}, target_col, "multiclass")
    if train.empty or valid.empty:
        return ModelBundle(target_col, None, {"status":"skipped","reason":"empty split"}, target_col, "multiclass")
    pre = build_preprocessor()
    try:
        X_train = pre.fit_transform(train[NUM_COLS + CAT_COLS])
        X_valid = pre.transform(valid[NUM_COLS + CAT_COLS])
    except Exception as e:
        return ModelBundle(target_col, None, {"status":"skipped","reason":f"preprocess failed: {e}"}, target_col, "multiclass")
    y_train = train[target_col].astype(str)
    clf = LogisticRegression(max_iter=1000, multi_class="auto")
    try:
        clf.fit(X_train, y_train)
    except Exception as e:
        return ModelBundle(target_col, None, {"status":"skipped","reason":f"fit failed: {e}"}, target_col, "multiclass")
    class Wrapped:
        def __init__(self, pre, clf):
            self.pre = pre
            self.clf = clf
            self.classes_ = clf.classes_
        def predict(self, X):
            return self.clf.predict(self.pre.transform(X))
        def predict_proba(self, X):
            return self.clf.predict_proba(self.pre.transform(X))
    return ModelBundle(target_col, Wrapped(pre, clf), {"status":"ok","rows_train":int(len(train)),"rows_valid":int(len(valid)),"n_classes":int(len(clf.classes_))}, target_col, "multiclass")

def fit_regression(df, target_col):
    data = ensure_feature_columns(df.dropna(subset=[target_col]).copy())
    if len(data) < MIN_TRAIN_ROWS:
        return ModelBundle(target_col, None, {"status":"skipped","reason":"insufficient regression rows"}, target_col, "regression")
    try:
        train, valid = train_test_split(data, test_size=0.2, random_state=RANDOM_STATE)
    except Exception:
        return ModelBundle(target_col, None, {"status":"skipped","reason":"train_test_split failed"}, target_col, "regression")
    if train.empty or valid.empty:
        return ModelBundle(target_col, None, {"status":"skipped","reason":"empty split"}, target_col, "regression")
    model = Pipeline(steps=[("pre", build_preprocessor()), ("model", make_reg_estimator())])
    try:
        model.fit(train[NUM_COLS + CAT_COLS], train[target_col])
        p = model.predict(valid[NUM_COLS + CAT_COLS])
    except Exception as e:
        return ModelBundle(target_col, None, {"status":"skipped","reason":f"fit failed: {e}"}, target_col, "regression")
    return ModelBundle(target_col, model, {"status":"ok","rows_train":int(len(train)),"rows_valid":int(len(valid)),"mae":float(np.mean(np.abs(valid[target_col] - p))) if len(valid) else None}, target_col, "regression")

def score_current(fs, bundles):
    current = ensure_feature_columns(fs.copy())
    current["repurchase_score"] = 0.0
    current["first_purchase_score"] = 0.0
    current["churn_risk_score"] = 0.0
    current["ltv_score"] = 0.0
    current["next_best_category"] = ""
    X = current[NUM_COLS + CAT_COLS].copy()
    for bundle in bundles:
        if bundle.model is None:
            continue
        try:
            if bundle.kind == "binary":
                p = bundle.model.predict_proba(X)[:,1]
                if bundle.target_col == "repurchase_score_label":
                    current["repurchase_score"] = p
                elif bundle.target_col == "first_purchase_score_label":
                    current["first_purchase_score"] = p
                elif bundle.target_col == "churn_risk_label":
                    current["churn_risk_score"] = p
            elif bundle.kind == "regression" and bundle.target_col == "ltv_target":
                current["ltv_score"] = bundle.model.predict(X)
            elif bundle.kind == "multiclass" and bundle.target_col == "next_category_target":
                current["next_best_category"] = bundle.model.predict(X)
        except Exception:
            continue
    ltv = _num_series(current["ltv_score"], default=0.0)
    if len(ltv) and float(ltv.max()) > float(ltv.min()):
        current["ltv_score"] = (ltv - ltv.min()) / (ltv.max() - ltv.min())
    else:
        current["ltv_score"] = 0.0
    action = np.full(len(current), "GENERAL", dtype=object)
    action = np.where(current["churn_risk_score"] >= 0.75, "CHURN_PREVENTION", action)
    action = np.where(current["repurchase_score"] >= 0.75, "RETENTION_REPURCHASE", action)
    action = np.where((current["order_count"] <= 0) & (current["first_purchase_score"] >= 0.80), "FIRST_PURCHASE_NUDGE", action)
    action = np.where((current["ltv_score"] >= 0.85) & (current["order_count"] >= 2), "VIP_UPSELL", action)
    action = np.where((action == "GENERAL") & (current["repeat_item_flag"] >= 1) & (current["repurchase_score"] >= 0.60), "RETENTION_REPURCHASE", action)
    action = np.where((action == "GENERAL") & ((current["multi_color_flag"] >= 1) | (current["multi_size_flag"] >= 1) | (current["multi_product_customer_flag"] >= 1)) & current["next_best_category"].astype(str).ne(""), "CATEGORY_CROSSSELL", action)
    action = np.where((action == "GENERAL") & (current["order_count"] >= 1) & (current["repurchase_30d_score"] >= 0.65) & (current["churn_60d_score"] < 0.55), "TARGET_BUYER", action)
    current["crm_action_type"] = action
    current["ml_action_type"] = current["crm_action_type"]
    priority = np.full(len(current), "P3", dtype=object)
    priority = np.where((current["ltv_score"] >= 0.8) | (current["repurchase_score"] >= 0.8), "P1", priority)
    priority = np.where(((current["churn_risk_score"] >= 0.7) | (current["first_purchase_score"] >= 0.75)) & (priority != "P1"), "P2", priority)
    current["priority_tier"] = priority
    current["predicted_member_stage"] = np.select([current["ltv_score"] >= 0.85, current["order_count"] >= 2, current["order_count"] == 1, current["order_count"] <= 0], ["VIP","REPEAT_BUYER","ONE_TIME_BUYER","PROSPECT"], default="GENERAL_MEMBER")
    return current

def upload_dataframe(df, table_name):
    if df is None:
        return
    client = get_client()
    full_name = qname(table_name)
    df2 = df.copy()
    for c in df2.columns:
        if str(df2[c].dtype) == "object":
            df2[c] = df2[c].astype(str)
    job_config = None
    try:
        if bigquery is not None:
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(df2, full_name, location=BQ_LOCATION, job_config=job_config)
        job.result()
    except Exception as e:
        print(f"[WARN] upload failed for {full_name}: {e}")

def train_and_score():
    fs = load_member_base()
    if fs.empty:
        print("[WARN] member base empty -> write empty score table and skip ML")
        upload_dataframe(pd.DataFrame(columns=["member_id","user_id","age","gender","age_band","channel_group","top_category","top_product","order_count","total_revenue","aov","distinct_product_count","max_distinct_products_in_order","avg_distinct_products_per_order","multi_product_order_count","multi_product_customer_flag","multi_color_product_count","max_color_count","multi_color_flag","multi_size_product_count","max_size_count","multi_size_flag","repeat_item_product_count","max_repeat_item_orders","repeat_item_flag","top_multi_color_product_name","top_multi_size_product_name","top_repeat_product_name","signup_date","last_visit_date","last_order_date","days_since_signup","days_since_last_visit","days_since_last_purchase","repurchase_score","repurchase_30d_score","first_purchase_score","first_purchase_30d_score","churn_risk_score","churn_60d_score","ltv_score","next_best_category","crm_action_type","ml_action_type","priority_tier","ml_priority_tier","predicted_member_stage"]), SCORES_TABLE)
        return
    labels = make_labels(fs)
    upload_dataframe(fs, FEATURE_TABLE)
    upload_dataframe(labels[["member_id","repurchase_score_label","first_purchase_score_label","churn_risk_label","ltv_target","next_category_target"]].copy(), LABEL_TABLE)
    bundles = [fit_binary(labels, "repurchase_score_label"), fit_binary(labels, "first_purchase_score_label"), fit_binary(labels, "churn_risk_label"), fit_regression(labels, "ltv_target"), fit_multiclass(labels, "next_category_target")]
    metrics_rows = []
    run_at = dt.datetime.now(dt.timezone.utc).isoformat()
    for b in bundles:
        metrics_rows.append({"model_name": b.target_col, "model_version": "member_totalview_v2", "evaluated_at": run_at, "metrics_json": json.dumps(b.metrics, ensure_ascii=False)})
        if b.metrics.get("status") != "ok":
            print(f"[WARN] {b.target_col} skipped: {b.metrics.get('reason')}")
    upload_dataframe(pd.DataFrame(metrics_rows), METRICS_TABLE)
    scored = score_current(fs, bundles)
    out = scored[["member_id","user_id","age","gender","age_band","channel_group","top_category","top_product","order_count","total_revenue","aov","distinct_product_count","max_distinct_products_in_order","avg_distinct_products_per_order","multi_product_order_count","multi_product_customer_flag","multi_color_product_count","max_color_count","multi_color_flag","multi_size_product_count","max_size_count","multi_size_flag","repeat_item_product_count","max_repeat_item_orders","repeat_item_flag","top_multi_color_product_name","top_multi_size_product_name","top_repeat_product_name","signup_date","last_visit_date","last_order_date","days_since_signup","days_since_last_visit","days_since_last_purchase","repurchase_score","first_purchase_score","churn_risk_score","ltv_score","next_best_category","crm_action_type","priority_tier","predicted_member_stage"]].copy()
    if "repurchase_score" not in out.columns:
        out["repurchase_score"] = 0.0
    if "first_purchase_score" not in out.columns:
        out["first_purchase_score"] = 0.0
    if "churn_risk_score" not in out.columns:
        out["churn_risk_score"] = 0.0
    if "ltv_score" not in out.columns:
        out["ltv_score"] = 0.0
    out["repurchase_30d_score"] = pd.to_numeric(out.get("repurchase_30d_score", out["repurchase_score"]), errors="coerce").fillna(pd.to_numeric(out["repurchase_score"], errors="coerce").fillna(0.0))
    out["first_purchase_30d_score"] = pd.to_numeric(out.get("first_purchase_30d_score", out["first_purchase_score"]), errors="coerce").fillna(pd.to_numeric(out["first_purchase_score"], errors="coerce").fillna(0.0))
    out["churn_60d_score"] = pd.to_numeric(out.get("churn_60d_score", out["churn_risk_score"]), errors="coerce").fillna(pd.to_numeric(out["churn_risk_score"], errors="coerce").fillna(0.0))
    out["ml_action_type"] = out.get("ml_action_type", out["crm_action_type"]).fillna(out["crm_action_type"])
    out["ml_priority_tier"] = out.get("ml_priority_tier", out["priority_tier"]).fillna(out["priority_tier"])
    print("[DEBUG] score output columns:", sorted(out.columns.tolist()))
    print("[DEBUG] repurchase_30d_score non-zero rows=", int((pd.to_numeric(out["repurchase_30d_score"], errors="coerce").fillna(0) > 0).sum()))
    upload_dataframe(out, SCORES_TABLE)
    print(f"[INFO] feature store written: {qname(FEATURE_TABLE)} rows={len(fs)}")
    print(f"[INFO] labels written: {qname(LABEL_TABLE)} rows={len(labels)}")
    print(f"[INFO] metrics written: {qname(METRICS_TABLE)} rows={len(metrics_rows)}")
    print(f"[INFO] scores written: {qname(SCORES_TABLE)} rows={len(out)}")

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", default="train_and_score", choices=["train_and_score"])
    return p.parse_args()

def main():
    _ = parse_args()
    try:
        train_and_score()
    except Exception as e:
        print(f"[WARN] CRM MEMBER TOTALVIEW ML pipeline failed but will not hard-crash: {e}")
        try:
            empty = pd.DataFrame(columns=["member_id","user_id","age","gender","age_band","channel_group","top_category","top_product","order_count","total_revenue","aov","signup_date","last_visit_date","last_order_date","days_since_signup","days_since_last_visit","days_since_last_purchase","repurchase_score","repurchase_30d_score","first_purchase_score","first_purchase_30d_score","churn_risk_score","churn_60d_score","ltv_score","next_best_category","crm_action_type","ml_action_type","priority_tier","ml_priority_tier","predicted_member_stage"])
            upload_dataframe(empty, SCORES_TABLE)
        except Exception as inner:
            print(f"[WARN] fallback empty score upload failed: {inner}")

if __name__ == "__main__":
    main()
