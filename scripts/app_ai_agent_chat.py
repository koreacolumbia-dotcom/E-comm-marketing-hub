#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Columbia AI Agent Chat - Streamlit

Role
----
Local conversational BI app for Columbia official mall CRM/order mart.

What it does
------------
- Connects to BigQuery
- Routes Korean natural-language questions to safe aggregate queries
- Never exposes raw member-level data by default
- Optionally calls Gemini / Groq / OpenRouter for natural-language interpretation
- Falls back to rule-based interpretation when no API key exists

Run
---
streamlit run scripts/app_ai_agent_chat.py

Required env
------------
GOOGLE_APPLICATION_CREDENTIALS or GOOGLE_SA_JSON_B64

Optional env
------------
GEMINI_API_KEY
GROQ_API_KEY
OPENROUTER_API_KEY
BQ_PROJECT=columbia-ga4
BQ_RAW_DATASET=crm_raw
BQ_MART_DATASET=crm_mart
BQ_ORDER_PRODUCT_TABLE=tb_order_product_search_mart
BQ_ADMIN_DAILY_TABLE=member_funnel_admin_daily
"""

from __future__ import annotations

import base64
import datetime as dt
import json
import os
import re
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import streamlit as st
from google.cloud import bigquery


KST = dt.timezone(dt.timedelta(hours=9))


# =========================================================
# Environment / credentials
# =========================================================
def getenv(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def setup_google_credentials() -> None:
    cred_path = getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if cred_path and Path(cred_path).exists():
        return

    b64 = getenv("GOOGLE_SA_JSON_B64")
    if b64:
        out = Path(getenv("GOOGLE_SA_JSON_OUT", "gcp_service_account.json"))
        if not out.exists():
            out.write_bytes(base64.b64decode(b64))
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(out.resolve())


def validate_identifier(value: str, label: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_\-]+", value):
        raise ValueError(f"Invalid {label}: {value}")
    return value


def bq_table(project: str, dataset: str, table: str) -> str:
    project = validate_identifier(project, "project")
    dataset = validate_identifier(dataset, "dataset")
    table = validate_identifier(table, "table")
    return f"`{project}.{dataset}.{table}`"


@st.cache_resource(show_spinner=False)
def get_bq_client(project: str, location: str) -> bigquery.Client:
    setup_google_credentials()
    return bigquery.Client(project=project, location=location)


@st.cache_data(ttl=600, show_spinner=False)
def run_bq_query(sql: str, params_json: str, project: str, location: str) -> pd.DataFrame:
    client = get_bq_client(project, location)
    params = json.loads(params_json) if params_json else {}

    query_params = []
    for k, v in params.items():
        if isinstance(v, int):
            query_params.append(bigquery.ScalarQueryParameter(k, "INT64", v))
        elif isinstance(v, float):
            query_params.append(bigquery.ScalarQueryParameter(k, "FLOAT64", v))
        else:
            # Dates are passed as string and casted in SQL
            query_params.append(bigquery.ScalarQueryParameter(k, "STRING", str(v)))

    job_config = bigquery.QueryJobConfig(query_parameters=query_params)
    return client.query(sql, job_config=job_config, location=location).to_dataframe()


# =========================================================
# Formatting
# =========================================================
def krw(v: Any) -> str:
    try:
        return f"{float(v):,.0f}원"
    except Exception:
        return "N/A"


def pct(v: Any) -> str:
    try:
        sign = "+" if float(v) > 0 else ""
        return f"{sign}{float(v):.1f}%"
    except Exception:
        return "N/A"


def pp(v: Any) -> str:
    try:
        sign = "+" if float(v) > 0 else ""
        return f"{sign}{float(v):.1f}p"
    except Exception:
        return "N/A"


def default_target_date() -> dt.date:
    return dt.datetime.now(KST).date() - dt.timedelta(days=1)


# =========================================================
# Query router
# =========================================================
def route_question(q: str) -> str:
    ql = q.lower()

    if any(x in q for x in ["재고", "품절", "사이즈", "저재고"]):
        return "stock"
    if any(x in q for x in ["쿠폰", "혜택", "할인"]):
        return "coupon"
    if any(x in q for x in ["프로모션", "promotion"]):
        return "promotion"
    if any(x in q for x in ["장바구니", "카트", "cart"]):
        return "cart"
    if any(x in q for x in ["카테고리", "카테고리별", "전시", "mdpick", "md pick"]):
        return "category"
    if any(x in q for x in ["상품", "품번", "제품", "빠졌", "하락", "상승", "잘 팔", "베스트"]):
        return "product"
    if any(x in q for x in ["모바일", "pc", "디바이스", "device"]):
        return "device"
    if any(x in q for x in ["포인트", "마일리지", "적립"]):
        return "point"
    return "daily"


def build_queries(project: str, raw_dataset: str, mart_dataset: str, order_table: str, admin_table: str) -> dict[str, str]:
    order = bq_table(project, raw_dataset, order_table)
    admin = bq_table(project, mart_dataset, admin_table)

    return {
        "daily": f"""
WITH base AS (
  SELECT
    DATE(report_date) AS report_date,
    SUM(revenue) AS revenue,
    SUM(orders) AS orders,
    SUM(buyers) AS buyers,
    SUM(quantity) AS quantity,
    SUM(coupon_used) AS coupon_used,
    SUM(point_used) AS point_used,
    SUM(cancel_amount) AS cancel_amount,
    SUM(coupon_orders) AS coupon_orders,
    SUM(promotion_orders) AS promotion_orders,
    SAFE_DIVIDE(SUM(revenue), NULLIF(SUM(orders), 0)) AS aov
  FROM {admin}
  WHERE DATE(report_date) IN (
    DATE(@target_date),
    DATE_SUB(DATE(@target_date), INTERVAL 7 DAY)
  )
  GROUP BY report_date
),
pivoted AS (
  SELECT
    MAX(IF(report_date = DATE(@target_date), revenue, NULL)) AS revenue,
    MAX(IF(report_date = DATE_SUB(DATE(@target_date), INTERVAL 7 DAY), revenue, NULL)) AS revenue_prev,
    MAX(IF(report_date = DATE(@target_date), orders, NULL)) AS orders,
    MAX(IF(report_date = DATE_SUB(DATE(@target_date), INTERVAL 7 DAY), orders, NULL)) AS orders_prev,
    MAX(IF(report_date = DATE(@target_date), buyers, NULL)) AS buyers,
    MAX(IF(report_date = DATE_SUB(DATE(@target_date), INTERVAL 7 DAY), buyers, NULL)) AS buyers_prev,
    MAX(IF(report_date = DATE(@target_date), aov, NULL)) AS aov,
    MAX(IF(report_date = DATE_SUB(DATE(@target_date), INTERVAL 7 DAY), aov, NULL)) AS aov_prev,
    MAX(IF(report_date = DATE(@target_date), coupon_used, NULL)) AS coupon_used,
    MAX(IF(report_date = DATE(@target_date), point_used, NULL)) AS point_used,
    MAX(IF(report_date = DATE(@target_date), cancel_amount, NULL)) AS cancel_amount,
    MAX(IF(report_date = DATE(@target_date), coupon_orders, NULL)) AS coupon_orders,
    MAX(IF(report_date = DATE(@target_date), promotion_orders, NULL)) AS promotion_orders
  FROM base
)
SELECT
  DATE(@target_date) AS target_date,
  revenue,
  revenue_prev,
  SAFE_DIVIDE(revenue - revenue_prev, NULLIF(revenue_prev, 0)) * 100 AS revenue_wow_pct,
  orders,
  orders_prev,
  SAFE_DIVIDE(orders - orders_prev, NULLIF(orders_prev, 0)) * 100 AS orders_wow_pct,
  buyers,
  buyers_prev,
  SAFE_DIVIDE(buyers - buyers_prev, NULLIF(buyers_prev, 0)) * 100 AS buyers_wow_pct,
  aov,
  aov_prev,
  SAFE_DIVIDE(aov - aov_prev, NULLIF(aov_prev, 0)) * 100 AS aov_wow_pct,
  coupon_used,
  point_used,
  cancel_amount,
  coupon_orders,
  promotion_orders
FROM pivoted
""",
        "product": f"""
WITH cur AS (
  SELECT
    product_code,
    ANY_VALUE(product_name_kor) AS product_name_kor,
    ANY_VALUE(product_name) AS product_name,
    ANY_VALUE(category_title_kr) AS category_title_kr,
    SUM(net_erp_revenue) AS revenue,
    SUM(purchase_qty) AS qty,
    COUNT(DISTINCT order_no) AS orders,
    MAX(size_stock_qty) AS current_stock,
    MAX(is_soldout_size) AS is_soldout_size,
    MAX(is_low_stock_size) AS is_low_stock_size
  FROM {order}
  WHERE order_date BETWEEN DATE_SUB(DATE(@target_date), INTERVAL @period_days - 1 DAY) AND DATE(@target_date)
  GROUP BY product_code
),
prev AS (
  SELECT
    product_code,
    SUM(net_erp_revenue) AS revenue_prev,
    SUM(purchase_qty) AS qty_prev,
    COUNT(DISTINCT order_no) AS orders_prev
  FROM {order}
  WHERE order_date BETWEEN DATE_SUB(DATE(@target_date), INTERVAL @period_days + 6 DAY)
                       AND DATE_SUB(DATE(@target_date), INTERVAL 7 DAY)
  GROUP BY product_code
)
SELECT
  cur.product_code,
  COALESCE(cur.product_name_kor, cur.product_name, cur.product_code) AS product_name,
  cur.category_title_kr,
  cur.revenue,
  COALESCE(prev.revenue_prev, 0) AS revenue_prev,
  cur.revenue - COALESCE(prev.revenue_prev, 0) AS revenue_diff,
  SAFE_DIVIDE(cur.revenue - COALESCE(prev.revenue_prev, 0), NULLIF(prev.revenue_prev, 0)) * 100 AS revenue_wow_pct,
  cur.qty,
  COALESCE(prev.qty_prev, 0) AS qty_prev,
  cur.orders,
  COALESCE(prev.orders_prev, 0) AS orders_prev,
  cur.current_stock,
  cur.is_soldout_size,
  cur.is_low_stock_size
FROM cur
LEFT JOIN prev USING(product_code)
ORDER BY revenue_diff ASC
LIMIT 30
""",
        "stock": f"""
SELECT
  product_code,
  COALESCE(ANY_VALUE(product_name_kor), ANY_VALUE(product_name), product_code) AS product_name,
  ANY_VALUE(category_title_kr) AS category_title_kr,
  SUM(net_erp_revenue) AS revenue_30d,
  SUM(purchase_qty) AS qty_30d,
  COUNT(DISTINCT order_no) AS orders_30d,
  MAX(size_stock_qty) AS current_size_stock,
  MAX(stock_limit) AS stock_limit,
  MAX(is_soldout_size) AS is_soldout_size,
  MAX(is_low_stock_size) AS is_low_stock_size,
  MAX(cart_add_count_180d) AS cart_add_count_180d
FROM {order}
WHERE order_date BETWEEN DATE_SUB(DATE(@target_date), INTERVAL 29 DAY) AND DATE(@target_date)
GROUP BY product_code
HAVING current_size_stock <= stock_limit OR is_soldout_size = 1 OR is_low_stock_size = 1
ORDER BY revenue_30d DESC, cart_add_count_180d DESC
LIMIT 30
""",
        "coupon": f"""
SELECT
  order_date,
  COUNT(DISTINCT order_no) AS orders,
  COUNT(DISTINCT IF(is_coupon_order = 1, order_no, NULL)) AS coupon_orders,
  SAFE_DIVIDE(COUNT(DISTINCT IF(is_coupon_order = 1, order_no, NULL)), NULLIF(COUNT(DISTINCT order_no), 0)) * 100 AS coupon_order_rate,
  SUM(net_erp_revenue) AS revenue,
  SUM(order_use_coupon_price) AS coupon_used,
  SUM(order_use_point) AS point_used,
  SUM(product_promotion_sale_price) AS promotion_sale_amount
FROM {order}
WHERE order_date BETWEEN DATE_SUB(DATE(@target_date), INTERVAL @period_days - 1 DAY) AND DATE(@target_date)
GROUP BY order_date
ORDER BY order_date
""",
        "promotion": f"""
SELECT
  product_code,
  COALESCE(ANY_VALUE(product_name_kor), ANY_VALUE(product_name), product_code) AS product_name,
  COUNT(DISTINCT order_no) AS orders,
  SUM(net_erp_revenue) AS revenue,
  SUM(product_promotion_sale_price) AS promotion_sale_amount,
  COUNTIF(is_promotion_line = 1) AS promotion_lines,
  MAX(promotion_master_no) AS promotion_master_no,
  MAX(promotion_group_no) AS promotion_group_no
FROM {order}
WHERE order_date BETWEEN DATE_SUB(DATE(@target_date), INTERVAL @period_days - 1 DAY) AND DATE(@target_date)
GROUP BY product_code
HAVING promotion_lines > 0 OR promotion_sale_amount > 0
ORDER BY revenue DESC
LIMIT 30
""",
        "cart": f"""
SELECT
  product_code,
  COALESCE(ANY_VALUE(product_name_kor), ANY_VALUE(product_name), product_code) AS product_name,
  ANY_VALUE(category_title_kr) AS category_title_kr,
  MAX(cart_add_count_180d) AS cart_add_count_180d,
  MAX(cart_qty_180d) AS cart_qty_180d,
  SUM(net_erp_revenue) AS revenue,
  SUM(purchase_qty) AS qty,
  MAX(size_stock_qty) AS current_stock,
  MAX(is_soldout_size) AS is_soldout_size
FROM {order}
WHERE order_date BETWEEN DATE_SUB(DATE(@target_date), INTERVAL @period_days - 1 DAY) AND DATE(@target_date)
GROUP BY product_code
HAVING cart_add_count_180d > 0
ORDER BY cart_add_count_180d DESC, revenue ASC
LIMIT 30
""",
        "category": f"""
WITH cur AS (
  SELECT
    COALESCE(category_title_kr, relation_category, CAST(category_manager_no AS STRING), '미분류') AS category,
    SUM(net_erp_revenue) AS revenue,
    SUM(purchase_qty) AS qty,
    COUNT(DISTINCT order_no) AS orders,
    COUNT(DISTINCT product_code) AS products
  FROM {order}
  WHERE order_date BETWEEN DATE_SUB(DATE(@target_date), INTERVAL @period_days - 1 DAY) AND DATE(@target_date)
  GROUP BY category
),
prev AS (
  SELECT
    COALESCE(category_title_kr, relation_category, CAST(category_manager_no AS STRING), '미분류') AS category,
    SUM(net_erp_revenue) AS revenue_prev,
    SUM(purchase_qty) AS qty_prev,
    COUNT(DISTINCT order_no) AS orders_prev
  FROM {order}
  WHERE order_date BETWEEN DATE_SUB(DATE(@target_date), INTERVAL @period_days + 6 DAY)
                       AND DATE_SUB(DATE(@target_date), INTERVAL 7 DAY)
  GROUP BY category
)
SELECT
  cur.category,
  cur.revenue,
  COALESCE(prev.revenue_prev, 0) AS revenue_prev,
  cur.revenue - COALESCE(prev.revenue_prev, 0) AS revenue_diff,
  SAFE_DIVIDE(cur.revenue - COALESCE(prev.revenue_prev, 0), NULLIF(prev.revenue_prev, 0)) * 100 AS revenue_wow_pct,
  cur.qty,
  cur.orders,
  cur.products
FROM cur
LEFT JOIN prev USING(category)
ORDER BY revenue_diff ASC
LIMIT 30
""",
        "device": f"""
SELECT
  order_device_type,
  COUNT(DISTINCT order_no) AS orders,
  COUNT(DISTINCT member_id) AS buyers,
  SUM(net_erp_revenue) AS revenue,
  SAFE_DIVIDE(SUM(net_erp_revenue), NULLIF(COUNT(DISTINCT order_no), 0)) AS aov,
  SUM(order_use_coupon_price) AS coupon_used
FROM {order}
WHERE order_date BETWEEN DATE_SUB(DATE(@target_date), INTERVAL @period_days - 1 DAY) AND DATE(@target_date)
GROUP BY order_device_type
ORDER BY revenue DESC
""",
        "point": f"""
SELECT
  order_date,
  COUNT(DISTINCT order_no) AS orders,
  COUNT(DISTINCT IF(is_point_used_order = 1, order_no, NULL)) AS point_orders,
  SAFE_DIVIDE(COUNT(DISTINCT IF(is_point_used_order = 1, order_no, NULL)), NULLIF(COUNT(DISTINCT order_no), 0)) * 100 AS point_order_rate,
  SUM(net_erp_revenue) AS revenue,
  SUM(order_use_point) AS order_use_point,
  SUM(order_product_use_mileage) AS order_product_use_mileage,
  SUM(detail_get_point) AS detail_get_point,
  SUM(detail_use_point) AS detail_use_point
FROM {order}
WHERE order_date BETWEEN DATE_SUB(DATE(@target_date), INTERVAL @period_days - 1 DAY) AND DATE(@target_date)
GROUP BY order_date
ORDER BY order_date
""",
    }


# =========================================================
# Rule-based interpretation + optional LLM
# =========================================================
def dataframe_for_prompt(df: pd.DataFrame, max_rows: int = 20) -> list[dict[str, Any]]:
    safe = df.head(max_rows).copy()
    for c in safe.columns:
        if safe[c].dtype.kind in "M":
            safe[c] = safe[c].astype(str)
    return json.loads(safe.to_json(orient="records", force_ascii=False))


def rule_based_answer(route: str, df: pd.DataFrame, question: str) -> str:
    if df.empty:
        return "조회 결과가 비어 있어. 날짜 범위, ETL 적재 여부, 테이블명을 먼저 확인해야 해."

    if route == "daily":
        r = df.iloc[0]
        return (
            f"기준일 {r.get('target_date')} 매출은 **{krw(r.get('revenue'))}**이고, "
            f"전주 동일 요일 대비 **{pct(r.get('revenue_wow_pct'))}**야. "
            f"주문수는 {pct(r.get('orders_wow_pct'))}, 구매자는 {pct(r.get('buyers_wow_pct'))}, "
            f"객단가는 {pct(r.get('aov_wow_pct'))} 변동했어. "
            f"매출 하락이면 우선 구매자 수와 객단가 중 어느 쪽 영향이 큰지 보고, 그다음 상품/카테고리/재고/쿠폰 영향을 파고들면 돼."
        )

    if route == "product":
        worst = df.iloc[0]
        return (
            f"상품 기준 하락 기여도가 가장 큰 건 **{worst.get('product_code')} / {worst.get('product_name')}**이야. "
            f"최근 기간 매출은 {krw(worst.get('revenue'))}, 전주 비교 증감은 {krw(worst.get('revenue_diff'))} "
            f"({pct(worst.get('revenue_wow_pct'))})로 보여. "
            f"재고가 낮거나 품절 플래그가 있으면 상세페이지 문제가 아니라 판매 가능 재고 이슈일 가능성이 커."
        )

    if route == "stock":
        top = df.iloc[0]
        return (
            f"재고/품절 관점에서는 **{top.get('product_code')} / {top.get('product_name')}**부터 확인하는 게 좋아. "
            f"최근 30일 매출 {krw(top.get('revenue_30d'))}, 현재 사이즈 재고 {top.get('current_size_stock')}개, "
            f"품절 플래그 {top.get('is_soldout_size')}로 잡혀 있어. "
            f"매출 상위인데 재고가 낮으면 전환율 하락의 직접 원인일 수 있어."
        )

    if route == "coupon":
        total_rev = df["revenue"].sum() if "revenue" in df else 0
        total_coupon = df["coupon_used"].sum() if "coupon_used" in df else 0
        avg_rate = df["coupon_order_rate"].mean() if "coupon_order_rate" in df else None
        return (
            f"선택 기간 쿠폰 사용액은 **{krw(total_coupon)}**, 매출은 **{krw(total_rev)}**야. "
            f"평균 쿠폰 주문 비중은 {pct(avg_rate)} 수준으로 보여. "
            f"매출 변동일에는 쿠폰 주문 비중과 쿠폰 사용액이 같이 움직였는지 확인하면 돼."
        )

    if route == "category":
        worst = df.iloc[0]
        return (
            f"카테고리 기준 하락 기여도가 가장 큰 건 **{worst.get('category')}**야. "
            f"매출 증감은 {krw(worst.get('revenue_diff'))} ({pct(worst.get('revenue_wow_pct'))})로 보여. "
            f"이 카테고리 안에서 상품 하락/재고/쿠폰 여부를 추가로 보면 원인이 좁혀져."
        )

    if route == "cart":
        top = df.iloc[0]
        return (
            f"장바구니 시그널은 **{top.get('product_code')} / {top.get('product_name')}**이 가장 눈에 띄어. "
            f"최근 장바구니 수 {top.get('cart_add_count_180d')}건인데 매출은 {krw(top.get('revenue'))}야. "
            f"장바구니는 많은데 구매가 낮으면 가격, 쿠폰, 재고, 배송 혜택을 점검하는 게 좋아."
        )

    if route == "promotion":
        top = df.iloc[0]
        return (
            f"프로모션 라인에서는 **{top.get('product_code')} / {top.get('product_name')}** 매출이 가장 커. "
            f"매출 {krw(top.get('revenue'))}, 프로모션 할인/혜택액 {krw(top.get('promotion_sale_amount'))}로 보여. "
            f"프로모션 매출 의존도가 커졌는지 같이 봐야 해."
        )

    if route == "device":
        return "디바이스별 성과를 조회했어. 모바일/PC별 매출, 주문수, 객단가 차이를 보고 특정 디바이스에서 전환 문제가 있는지 확인하면 돼."

    if route == "point":
        total_use = df["detail_use_point"].sum() if "detail_use_point" in df else 0
        return f"선택 기간 포인트/마일리지 사용 흐름을 조회했어. 상세 사용 포인트 합계는 {krw(total_use)} 수준이야."

    return "조회 결과를 표로 정리했어. 상위/하위 행을 기준으로 원인 후보를 좁히면 돼."


def call_llm(question: str, route: str, df: pd.DataFrame, fallback: str) -> str:
    payload = {
        "question": question,
        "route": route,
        "data_rows": dataframe_for_prompt(df),
        "instruction": (
            "너는 이커머스 마케팅 데이터 분석가다. "
            "제공된 집계 데이터 안에서만 한국어로 원인과 액션을 답해라. "
            "개별 회원 추적이나 개인정보 추정은 하지 마라. "
            "결론, 근거, 액션 순서로 짧고 명확하게 작성해라."
        ),
    }

    providers = [x.strip().lower() for x in getenv("LLM_PROVIDER_ORDER", "gemini,groq,openrouter").split(",") if x.strip()]

    for p in providers:
        try:
            if p == "gemini" and getenv("GEMINI_API_KEY"):
                return call_gemini(payload)
            if p == "groq" and getenv("GROQ_API_KEY"):
                return call_groq(payload)
            if p == "openrouter" and getenv("OPENROUTER_API_KEY"):
                return call_openrouter(payload)
        except Exception as e:
            st.warning(f"{p} API 실패 → fallback 사용: {e}")

    return fallback


def call_gemini(payload: dict[str, Any]) -> str:
    api_key = getenv("GEMINI_API_KEY")
    model = getenv("GEMINI_MODEL", "gemini-2.5-flash-lite")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
    prompt = json.dumps(payload, ensure_ascii=False, indent=2)
    r = requests.post(
        url,
        json={"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"temperature": 0.2}},
        timeout=45,
    )
    r.raise_for_status()
    data = r.json()
    return data["candidates"][0]["content"]["parts"][0]["text"]


def call_groq(payload: dict[str, Any]) -> str:
    api_key = getenv("GROQ_API_KEY")
    model = getenv("GROQ_MODEL", "llama-3.1-8b-instant")
    url = "https://api.groq.com/openai/v1/chat/completions"
    r = requests.post(
        url,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": "너는 이커머스 마케팅 데이터 분석가다."},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False, indent=2)},
            ],
            "temperature": 0.2,
        },
        timeout=45,
    )
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]


def call_openrouter(payload: dict[str, Any]) -> str:
    api_key = getenv("OPENROUTER_API_KEY")
    model = getenv("OPENROUTER_MODEL", "openrouter/free")
    url = "https://openrouter.ai/api/v1/chat/completions"
    r = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": getenv("OPENROUTER_SITE_URL", "https://github.com"),
            "X-Title": getenv("OPENROUTER_APP_NAME", "Columbia AI Agent Chat"),
        },
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": "너는 이커머스 마케팅 데이터 분석가다."},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False, indent=2)},
            ],
            "temperature": 0.2,
        },
        timeout=45,
    )
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]


# =========================================================
# Streamlit UI
# =========================================================
def main() -> None:
    st.set_page_config(
        page_title="Columbia AI Agent Chat",
        page_icon="🧭",
        layout="wide",
    )

    st.markdown(
        """
        <style>
        .block-container { padding-top: 1.6rem; max-width: 1320px; }
        [data-testid="stMetricValue"] { font-size: 1.55rem; }
        .agent-card {
            border: 1px solid rgba(16,24,40,.10);
            border-radius: 22px;
            padding: 18px 20px;
            background: linear-gradient(180deg,#ffffff 0%,#f8fafc 100%);
            box-shadow: 0 18px 45px rgba(16,24,40,.06);
        }
        .muted { color: #667085; font-size: .92rem; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.title("Columbia AI Agent Chat")
    st.caption("BigQuery에 적재된 AI Agent 주문상품 마트를 기반으로 대화형 분석을 합니다.")

    with st.sidebar:
        st.header("설정")
        project = st.text_input("BQ_PROJECT", getenv("BQ_PROJECT", "columbia-ga4"))
        location = st.text_input("BQ_LOCATION", getenv("BQ_LOCATION", "asia-northeast3"))
        raw_dataset = st.text_input("BQ_RAW_DATASET", getenv("BQ_RAW_DATASET", "crm_raw"))
        mart_dataset = st.text_input("BQ_MART_DATASET", getenv("BQ_MART_DATASET", "crm_mart"))
        order_table = st.text_input("주문상품 마트", getenv("BQ_ORDER_PRODUCT_TABLE", "tb_order_product_search_mart"))
        admin_table = st.text_input("일별 KPI 마트", getenv("BQ_ADMIN_DAILY_TABLE", "member_funnel_admin_daily"))

        target_date = st.date_input("기준일", value=default_target_date())
        period_days = st.slider("분석 기간", 1, 30, 7)

        use_llm = st.toggle("AI API 코멘트 사용", value=bool(getenv("GEMINI_API_KEY") or getenv("GROQ_API_KEY") or getenv("OPENROUTER_API_KEY")))
        st.markdown("---")
        st.write("예시 질문")
        st.code("어제 매출 왜 떨어졌어?\n어떤 상품이 제일 빠졌어?\n재고 때문이야?\n쿠폰 영향 있어?\n장바구니 많은데 안 팔리는 상품은?\n카테고리별로 뭐가 문제야?")

    queries = build_queries(project, raw_dataset, mart_dataset, order_table, admin_table)

    # Daily overview
    params = {"target_date": str(target_date), "period_days": int(period_days)}
    try:
        daily_df = run_bq_query(queries["daily"], json.dumps(params), project, location)
    except Exception as e:
        st.error(f"BigQuery 연결/조회 실패: {e}")
        st.stop()

    if not daily_df.empty:
        r = daily_df.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("매출", krw(r.get("revenue")), pct(r.get("revenue_wow_pct")))
        c2.metric("주문수", f"{int(r.get('orders') or 0):,}", pct(r.get("orders_wow_pct")))
        c3.metric("구매자", f"{int(r.get('buyers') or 0):,}", pct(r.get("buyers_wow_pct")))
        c4.metric("객단가", krw(r.get("aov")), pct(r.get("aov_wow_pct")))

    st.markdown("### 질문하기")
    if "messages" not in st.session_state:
        st.session_state.messages = []

    for m in st.session_state.messages:
        with st.chat_message(m["role"]):
            st.markdown(m["content"])
            if "df" in m and isinstance(m["df"], pd.DataFrame):
                st.dataframe(m["df"], use_container_width=True)

    question = st.chat_input("분석 질문을 입력해줘. 예: 어제 매출 왜 떨어졌어?")

    if question:
        st.session_state.messages.append({"role": "user", "content": question})

        route = route_question(question)
        sql = queries.get(route, queries["daily"])

        try:
            df = run_bq_query(sql, json.dumps(params), project, location)
            fallback = rule_based_answer(route, df, question)
            answer = call_llm(question, route, df, fallback) if use_llm else fallback

            content = f"**분석 유형:** `{route}`\n\n{answer}"
            st.session_state.messages.append({"role": "assistant", "content": content, "df": df})

            with st.chat_message("assistant"):
                st.markdown(content)
                st.dataframe(df, use_container_width=True)

        except Exception as e:
            err = f"조회 중 오류가 발생했어: `{type(e).__name__}: {e}`"
            st.session_state.messages.append({"role": "assistant", "content": err})
            with st.chat_message("assistant"):
                st.error(err)

    with st.expander("현재 일별 KPI Raw"):
        st.dataframe(daily_df, use_container_width=True)


if __name__ == "__main__":
    main()
