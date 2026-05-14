#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Columbia Product Keyword Performance Dashboard
- GA4 search keyword + SQL Server order-product mart in BigQuery
- Supports SPC 1/2/3 depth keyword selection
- Supports multiple product-code filtering
- Supports YOY comparison
- Robust to old BigQuery mart schema: optional product master columns are selected only if present
"""

from __future__ import annotations

import os
import json
import base64
import datetime as dt
from pathlib import Path
from typing import Any

import pandas as pd
from google.cloud import bigquery

KST = dt.timezone(dt.timedelta(hours=9))


def log(msg: str) -> None:
    print(f"[PRODUCT_KEYWORD] {msg}", flush=True)


def getenv(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def setup_google_credentials() -> None:
    if getenv("GOOGLE_APPLICATION_CREDENTIALS") and Path(getenv("GOOGLE_APPLICATION_CREDENTIALS")).exists():
        return
    b64 = getenv("GOOGLE_SA_JSON_B64")
    if b64:
        p = Path("gcp_service_account.json")
        p.write_bytes(base64.b64decode(b64))
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(p.resolve())


def kst_today() -> dt.date:
    return dt.datetime.now(KST).date()


def parse_date(value: str, default: dt.date) -> dt.date:
    try:
        return dt.datetime.strptime(value, "%Y-%m-%d").date()
    except Exception:
        return default


def safe_int(v: Any) -> int:
    try:
        return int(round(float(v or 0)))
    except Exception:
        return 0


SPC_GROUPS = {
    "상품그룹": {
        "기획/상태": ["신상품", "베스트", "온라인 단독", "빅 사이즈", "마지막 사이즈"],
    },
    "Activity": {
        "활동": ["하이킹", "캐주얼&트래블", "피싱", "트레일 러닝"],
    },
    "아우터": {
        "아우터": ["방수자켓", "바람막이", "경량패딩/슬림다운", "인터체인지", "베스트"],
    },
    "플리스": {
        "플리스": ["아우터", "상의", "하의", "모자"],
    },
    "상의": {
        "상의": ["키즈", "반팔티", "라운드티", "셔츠", "폴로티/집업", "맨투맨/후드티", "플리스"],
    },
    "하의": {
        "하의": ["긴바지", "카고/조거", "반바지"],
    },
    "신발": {
        "신발": ["옴니맥스", "등산화", "트레일러닝", "스니커즈", "샌들/슬리퍼", "레인 부츠"],
    },
    "가방": {
        "가방": ["백팩", "크로스/토트백", "힙색/슬링백"],
    },
    "모자": {
        "모자": ["볼캡", "버킷/버니"],
    },
    "용품": {
        "용품": ["장갑", "스틱", "양말", "지갑", "기타"],
    },
}


def get_bq_columns(client: bigquery.Client, table_id: str) -> set[str]:
    table = client.get_table(table_id)
    return {field.name for field in table.schema}


def col_expr(columns: set[str], col: str, typ: str, alias: str | None = None) -> str:
    alias = alias or col
    if col in columns:
        return f"CAST({col} AS {typ}) AS {alias}"
    if typ.upper().startswith("INT"):
        return f"CAST(NULL AS {typ}) AS {alias}"
    return f"CAST(NULL AS {typ}) AS {alias}"


def run_query(client: bigquery.Client, start_date: dt.date, end_date: dt.date, lookback_days: int) -> pd.DataFrame:
    events_table = getenv("GA4_EVENTS_TABLE", "columbia-ga4.analytics_358593394.events_*")
    order_table = getenv("BQ_ORDER_PRODUCT_TABLE", "columbia-ga4.crm_raw.tb_order_product_search_mart")
    search_event = getenv("SEARCH_EVENT_NAME", "view_search_results")
    location = getenv("BQ_LOCATION", "asia-northeast3")

    order_cols = get_bq_columns(client, order_table)
    log("Order mart columns: " + ", ".join(sorted(order_cols)))

    product_style_expr = col_expr(order_cols, "product_style", "STRING")
    product_name_kor_expr = col_expr(order_cols, "product_name_kor", "STRING")
    product_name_expr = col_expr(order_cols, "product_name", "STRING")
    relation_category_expr = col_expr(order_cols, "relation_category", "STRING")
    category_manager_no_expr = col_expr(order_cols, "category_manager_no", "INT64")
    mdpick_depth2_expr = col_expr(order_cols, "mdpick_depth2", "INT64")

    sql = f"""
    DECLARE start_date DATE DEFAULT @start_date;
    DECLARE end_date DATE DEFAULT @end_date;
    DECLARE ly_start DATE DEFAULT DATE_SUB(start_date, INTERVAL 1 YEAR);
    DECLARE ly_end DATE DEFAULT DATE_SUB(end_date, INTERVAL 1 YEAR);
    DECLARE lookback_days INT64 DEFAULT @lookback_days;

    WITH periods AS (
      SELECT 'TY' AS period, start_date AS s, end_date AS e
      UNION ALL
      SELECT 'LY' AS period, ly_start AS s, ly_end AS e
    ),
    search_events AS (
      SELECT
        p.period,
        DATE(TIMESTAMP_MICROS(e.event_timestamp), 'Asia/Seoul') AS search_date,
        TIMESTAMP_MICROS(e.event_timestamp) AS search_ts,
        COALESCE(NULLIF(TRIM(e.user_id), ''), NULL) AS member_id,
        e.user_pseudo_id,
        CONCAT(
          e.user_pseudo_id,
          '-',
          CAST((SELECT value.int_value FROM UNNEST(e.event_params) WHERE key='ga_session_id') AS STRING)
        ) AS session_key,
        LOWER(TRIM(COALESCE(
          (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key='search_term'),
          (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key='term'),
          REGEXP_EXTRACT(
            (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key='page_location'),
            r'[?&](?:q|query|keyword|searchKeyword|search_word|searchTerm)=([^&#]+)'
          )
        ))) AS search_term_raw
      FROM `{events_table}` e
      JOIN periods p
        ON DATE(TIMESTAMP_MICROS(e.event_timestamp), 'Asia/Seoul') BETWEEN p.s AND p.e
      WHERE (
            _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', start_date) AND FORMAT_DATE('%Y%m%d', end_date)
         OR _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', ly_start) AND FORMAT_DATE('%Y%m%d', ly_end)
      )
        AND e.event_name = @search_event
    ),
    clean_search AS (
      SELECT
        period,
        search_date,
        search_ts,
        member_id,
        user_pseudo_id,
        session_key,
        NULLIF(
          REGEXP_REPLACE(
            REPLACE(REPLACE(REPLACE(search_term_raw, '+', ' '), '%20', ' '), '%EC%BD%9C%EB%A1%AC%EB%B9%84%EC%95%84', '콜롬비아'),
            r'\\s+',
            ' '
          ),
          ''
        ) AS search_term
      FROM search_events
      WHERE search_term_raw IS NOT NULL
        AND search_term_raw NOT IN ('(not set)', 'not set', 'undefined', 'null')
    ),
    search_agg AS (
      SELECT
        period,
        search_date,
        search_term,
        COUNT(*) AS searches,
        COUNT(DISTINCT user_pseudo_id) AS search_users,
        COUNT(DISTINCT session_key) AS search_sessions,
        COUNT(DISTINCT member_id) AS login_search_users
      FROM clean_search
      WHERE search_term IS NOT NULL
      GROUP BY 1, 2, 3
    ),
    ga_purchase AS (
      SELECT
        p.period,
        TIMESTAMP_MICROS(e.event_timestamp) AS purchase_ts,
        e.user_pseudo_id,
        COALESCE(NULLIF(TRIM(e.user_id), ''), NULL) AS member_id,
        NULLIF(TRIM(COALESCE(
          e.ecommerce.transaction_id,
          (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key='transaction_id'),
          (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key='order_no'),
          (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key='orderNo')
        )), '') AS transaction_id
      FROM `{events_table}` e
      JOIN periods p
        ON DATE(TIMESTAMP_MICROS(e.event_timestamp), 'Asia/Seoul') BETWEEN p.s AND DATE_ADD(p.e, INTERVAL lookback_days DAY)
      WHERE (
            _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', start_date) AND FORMAT_DATE('%Y%m%d', DATE_ADD(end_date, INTERVAL lookback_days DAY))
         OR _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', ly_start) AND FORMAT_DATE('%Y%m%d', DATE_ADD(ly_end, INTERVAL lookback_days DAY))
      )
        AND e.event_name = 'purchase'
    ),
    order_lines AS (
      SELECT
        p.period,
        order_date,
        TIMESTAMP(order_datetime) AS order_ts,
        CAST(member_id AS STRING) AS member_id,
        CAST(order_no AS STRING) AS order_no,
        CAST(order_product_no AS STRING) AS order_product_no,
        UPPER(TRIM(CAST(product_code AS STRING))) AS product_code,
        CAST(brand_code AS STRING) AS brand_code,
        {product_style_expr},
        {product_name_kor_expr},
        {product_name_expr},
        {relation_category_expr},
        {category_manager_no_expr},
        {mdpick_depth2_expr},
        CAST(purchase_qty AS INT64) AS purchase_qty,
        CAST(erp_revenue AS INT64) AS erp_revenue
      FROM `{order_table}` o
      JOIN periods p
        ON o.order_date BETWEEN p.s AND DATE_ADD(p.e, INTERVAL lookback_days DAY)
    ),
    joined_by_transaction AS (
      SELECT
        s.period,
        s.search_date,
        s.search_term,
        o.order_no,
        o.order_product_no,
        o.product_code,
        o.brand_code,
        o.product_style,
        o.product_name_kor,
        o.product_name,
        o.relation_category,
        o.category_manager_no,
        o.mdpick_depth2,
        o.purchase_qty,
        o.erp_revenue,
        'transaction_id' AS match_type
      FROM clean_search s
      INNER JOIN ga_purchase p
        ON s.period = p.period
       AND s.user_pseudo_id = p.user_pseudo_id
       AND p.purchase_ts >= s.search_ts
       AND p.purchase_ts < TIMESTAMP_ADD(s.search_ts, INTERVAL lookback_days DAY)
       AND p.transaction_id IS NOT NULL
      INNER JOIN order_lines o
        ON p.period = o.period
       AND p.transaction_id = o.order_no
      WHERE s.search_term IS NOT NULL
    ),
    joined_by_member AS (
      SELECT
        s.period,
        s.search_date,
        s.search_term,
        o.order_no,
        o.order_product_no,
        o.product_code,
        o.brand_code,
        o.product_style,
        o.product_name_kor,
        o.product_name,
        o.relation_category,
        o.category_manager_no,
        o.mdpick_depth2,
        o.purchase_qty,
        o.erp_revenue,
        'member_id' AS match_type
      FROM clean_search s
      INNER JOIN order_lines o
        ON s.period = o.period
       AND s.member_id = o.member_id
       AND s.member_id IS NOT NULL
       AND o.order_ts >= s.search_ts
       AND o.order_ts < TIMESTAMP_ADD(s.search_ts, INTERVAL lookback_days DAY)
      WHERE s.search_term IS NOT NULL
    ),
    joined_union AS (
      SELECT * FROM joined_by_transaction
      UNION ALL
      SELECT * FROM joined_by_member
    ),
    joined_dedup AS (
      SELECT * EXCEPT(rn)
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (
            PARTITION BY period, search_date, search_term, order_product_no
            ORDER BY IF(match_type='transaction_id', 0, 1)
          ) AS rn
        FROM joined_union
      )
      WHERE rn = 1
    ),
    order_agg_product AS (
      SELECT
        period,
        search_date,
        search_term,
        product_code,
        ANY_VALUE(product_style) AS product_style,
        ANY_VALUE(product_name_kor) AS product_name_kor,
        ANY_VALUE(product_name) AS product_name,
        ANY_VALUE(relation_category) AS relation_category,
        ANY_VALUE(category_manager_no) AS category_manager_no,
        ANY_VALUE(mdpick_depth2) AS mdpick_depth2,
        COUNT(DISTINCT order_no) AS orders,
        COUNT(DISTINCT product_code) AS purchased_products,
        SUM(purchase_qty) AS purchase_qty,
        SUM(erp_revenue) AS erp_revenue,
        COUNTIF(match_type = 'transaction_id') AS matched_by_transaction_rows,
        COUNTIF(match_type = 'member_id') AS matched_by_member_rows
      FROM joined_dedup
      GROUP BY 1, 2, 3, 4
    )
    SELECT
      a.period,
      a.search_date,
      a.search_term,
      o.product_code,
      o.product_style,
      o.product_name_kor,
      o.product_name,
      o.relation_category,
      o.category_manager_no,
      o.mdpick_depth2,
      a.searches,
      a.search_users,
      a.search_sessions,
      a.login_search_users,
      IFNULL(o.orders, 0) AS orders,
      IFNULL(o.purchased_products, 0) AS purchased_products,
      IFNULL(o.purchase_qty, 0) AS purchase_qty,
      IFNULL(o.erp_revenue, 0) AS erp_revenue,
      IFNULL(o.matched_by_transaction_rows, 0) AS matched_by_transaction_rows,
      IFNULL(o.matched_by_member_rows, 0) AS matched_by_member_rows,
      SAFE_DIVIDE(IFNULL(o.orders, 0), a.search_sessions) AS order_cvr
    FROM search_agg a
    LEFT JOIN order_agg_product o
      USING(period, search_date, search_term)
    ORDER BY a.period, a.search_date DESC, a.searches DESC, o.erp_revenue DESC
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date.isoformat()),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date.isoformat()),
            bigquery.ScalarQueryParameter("lookback_days", "INT64", lookback_days),
            bigquery.ScalarQueryParameter("search_event", "STRING", search_event),
        ]
    )
    log(f"Querying product keyword TY/LY. TY={start_date}~{end_date}, lookback={lookback_days}d")
    return client.query(sql, job_config=job_config, location=location).to_dataframe()



def run_direct_product_query(client: bigquery.Client, start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """
    Product-code direct sales query from SQL order-product mart.
    This is used when users type product code/prefix such as C7 or C72YL3596774.
    It does NOT require a GA4 search event match.
    """
    order_table = getenv("BQ_ORDER_PRODUCT_TABLE", "columbia-ga4.crm_raw.tb_order_product_search_mart")
    location = getenv("BQ_LOCATION", "asia-northeast3")
    order_cols = get_bq_columns(client, order_table)

    product_style_expr = col_expr(order_cols, "product_style", "STRING")
    product_name_kor_expr = col_expr(order_cols, "product_name_kor", "STRING")
    product_name_expr = col_expr(order_cols, "product_name", "STRING")
    relation_category_expr = col_expr(order_cols, "relation_category", "STRING")
    category_manager_no_expr = col_expr(order_cols, "category_manager_no", "INT64")
    mdpick_depth2_expr = col_expr(order_cols, "mdpick_depth2", "INT64")

    sql = f"""
    DECLARE start_date DATE DEFAULT @start_date;
    DECLARE end_date DATE DEFAULT @end_date;
    DECLARE ly_start DATE DEFAULT DATE_SUB(start_date, INTERVAL 1 YEAR);
    DECLARE ly_end DATE DEFAULT DATE_SUB(end_date, INTERVAL 1 YEAR);

    WITH periods AS (
      SELECT 'TY' AS period, start_date AS s, end_date AS e
      UNION ALL
      SELECT 'LY' AS period, ly_start AS s, ly_end AS e
    ),
    order_lines AS (
      SELECT
        p.period,
        o.order_date,
        CAST(o.order_no AS STRING) AS order_no,
        CAST(o.order_product_no AS STRING) AS order_product_no,
        UPPER(TRIM(CAST(o.product_code AS STRING))) AS product_code,
        CAST(o.brand_code AS STRING) AS brand_code,
        {product_style_expr},
        {product_name_kor_expr},
        {product_name_expr},
        {relation_category_expr},
        {category_manager_no_expr},
        {mdpick_depth2_expr},
        CAST(o.purchase_qty AS INT64) AS purchase_qty,
        CAST(o.erp_revenue AS INT64) AS erp_revenue
      FROM `{order_table}` o
      JOIN periods p
        ON o.order_date BETWEEN p.s AND p.e
      WHERE UPPER(TRIM(CAST(o.product_code AS STRING))) <> ''
    )
    SELECT
      period,
      order_date AS search_date,
      '(상품코드 직접검색)' AS search_term,
      product_code,
      ANY_VALUE(product_style) AS product_style,
      ANY_VALUE(product_name_kor) AS product_name_kor,
      ANY_VALUE(product_name) AS product_name,
      ANY_VALUE(relation_category) AS relation_category,
      ANY_VALUE(category_manager_no) AS category_manager_no,
      ANY_VALUE(mdpick_depth2) AS mdpick_depth2,
      0 AS searches,
      0 AS search_users,
      0 AS search_sessions,
      0 AS login_search_users,
      COUNT(DISTINCT order_no) AS orders,
      COUNT(DISTINCT product_code) AS purchased_products,
      SUM(purchase_qty) AS purchase_qty,
      SUM(erp_revenue) AS erp_revenue,
      0 AS matched_by_transaction_rows,
      0 AS matched_by_member_rows,
      0 AS order_cvr
    FROM order_lines
    GROUP BY 1, 2, 4
    ORDER BY period, search_date DESC, erp_revenue DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date.isoformat()),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date.isoformat()),
        ]
    )
    log(f"Querying direct SQL product sales. TY={start_date}~{end_date}")
    return client.query(sql, job_config=job_config, location=location).to_dataframe()

def build_payload(df: pd.DataFrame, direct_df: pd.DataFrame, start_date: dt.date, end_date: dt.date, lookback_days: int) -> dict:
    if df.empty:
        df = pd.DataFrame(columns=[
            "period", "search_date", "search_term", "product_code", "product_name_kor", "product_name", "relation_category",
            "searches", "search_users", "search_sessions", "login_search_users", "orders", "purchased_products",
            "purchase_qty", "erp_revenue", "matched_by_transaction_rows", "matched_by_member_rows", "order_cvr"
        ])
    else:
        df = df.copy()
        df["search_date"] = pd.to_datetime(df["search_date"]).dt.date
        df["product_code"] = df["product_code"].fillna("").astype(str).str.upper().str.strip()

    if direct_df is None or direct_df.empty:
        direct_df = pd.DataFrame(columns=[
            "period", "search_date", "search_term", "product_code", "product_name_kor", "product_name", "relation_category",
            "searches", "search_users", "search_sessions", "login_search_users", "orders", "purchased_products",
            "purchase_qty", "erp_revenue", "matched_by_transaction_rows", "matched_by_member_rows", "order_cvr"
        ])
    else:
        direct_df = direct_df.copy()
        direct_df["search_date"] = pd.to_datetime(direct_df["search_date"]).dt.date
        direct_df["product_code"] = direct_df["product_code"].fillna("").astype(str).str.upper().str.strip()

    search_base = df[["period", "search_date", "search_term", "searches", "search_users", "search_sessions", "login_search_users"]].drop_duplicates(["period", "search_date", "search_term"]) if not df.empty else df
    order_rows = df[df["product_code"] != ""].copy() if not df.empty else df

    daily_search = (
        search_base.groupby(["period", "search_date"], as_index=False)
        .agg(searches=("searches", "sum"), search_users=("search_users", "sum"), search_sessions=("search_sessions", "sum"))
        if not search_base.empty else pd.DataFrame(columns=["period", "search_date", "searches", "search_users", "search_sessions"])
    )
    daily_order = (
        order_rows.groupby(["period", "search_date"], as_index=False)
        .agg(orders=("orders", "sum"), purchase_qty=("purchase_qty", "sum"), erp_revenue=("erp_revenue", "sum"))
        if not order_rows.empty else pd.DataFrame(columns=["period", "search_date", "orders", "purchase_qty", "erp_revenue"])
    )
    daily = daily_search.merge(daily_order, on=["period", "search_date"], how="left").fillna(0)
    daily["date"] = daily["search_date"].astype(str)

    daily_term = (
        search_base.groupby(["period", "search_date", "search_term"], as_index=False)
        .agg(searches=("searches", "sum"), search_sessions=("search_sessions", "sum"))
        .merge(
            order_rows.groupby(["period", "search_date", "search_term"], as_index=False).agg(
                orders=("orders", "sum"), purchase_qty=("purchase_qty", "sum"), erp_revenue=("erp_revenue", "sum")
            ) if not order_rows.empty else pd.DataFrame(columns=["period", "search_date", "search_term", "orders", "purchase_qty", "erp_revenue"]),
            on=["period", "search_date", "search_term"], how="left"
        ).fillna(0)
        if not search_base.empty else pd.DataFrame(columns=["period", "search_date", "search_term", "searches", "search_sessions", "orders", "purchase_qty", "erp_revenue"])
    )
    daily_term["date"] = daily_term["search_date"].astype(str)

    daily_product = (
        order_rows.groupby(["period", "search_date", "search_term", "product_code"], as_index=False).agg(
            product_name_kor=("product_name_kor", "first"),
            product_name=("product_name", "first"),
            relation_category=("relation_category", "first"),
            orders=("orders", "sum"),
            purchase_qty=("purchase_qty", "sum"),
            erp_revenue=("erp_revenue", "sum"),
        ) if not order_rows.empty else pd.DataFrame(columns=["period", "search_date", "search_term", "product_code", "product_name_kor", "product_name", "relation_category", "orders", "purchase_qty", "erp_revenue"])
    )
    if not daily_product.empty:
        daily_product["date"] = daily_product["search_date"].astype(str)

    term_search = (
        search_base.groupby(["period", "search_term"], as_index=False)
        .agg(searches=("searches", "sum"), search_users=("search_users", "sum"), search_sessions=("search_sessions", "sum"))
        if not search_base.empty else pd.DataFrame(columns=["period", "search_term", "searches", "search_users", "search_sessions"])
    )
    term_order = (
        order_rows.groupby(["period", "search_term"], as_index=False)
        .agg(orders=("orders", "sum"), purchased_products=("product_code", "nunique"), purchase_qty=("purchase_qty", "sum"), erp_revenue=("erp_revenue", "sum"))
        if not order_rows.empty else pd.DataFrame(columns=["period", "search_term", "orders", "purchased_products", "purchase_qty", "erp_revenue"])
    )
    top_terms = term_search.merge(term_order, on=["period", "search_term"], how="left").fillna(0)
    top_terms["order_cvr"] = top_terms.apply(lambda r: (r["orders"] / r["search_sessions"]) if r["search_sessions"] else 0, axis=1)

    product_rows = (
        order_rows.groupby(["period", "search_term", "product_code"], as_index=False).agg(
            product_style=("product_style", "first"),
            product_name_kor=("product_name_kor", "first"),
            product_name=("product_name", "first"),
            relation_category=("relation_category", "first"),
            category_manager_no=("category_manager_no", "first"),
            mdpick_depth2=("mdpick_depth2", "first"),
            orders=("orders", "sum"),
            purchase_qty=("purchase_qty", "sum"),
            erp_revenue=("erp_revenue", "sum"),
        ).sort_values(["period", "erp_revenue", "purchase_qty"], ascending=[True, False, False]).head(600)
        if not order_rows.empty else pd.DataFrame(columns=["period", "search_term", "product_code", "product_name_kor", "product_name", "relation_category", "orders", "purchase_qty", "erp_revenue"])
    )

    def totals_for(period: str) -> dict:
        sb = search_base[search_base["period"] == period] if not search_base.empty else search_base
        ob = order_rows[order_rows["period"] == period] if not order_rows.empty else order_rows
        return {
            "searches": safe_int(sb["searches"].sum()) if not sb.empty else 0,
            "search_users": safe_int(sb["search_users"].sum()) if not sb.empty else 0,
            "search_sessions": safe_int(sb["search_sessions"].sum()) if not sb.empty else 0,
            "orders": safe_int(ob["orders"].sum()) if not ob.empty else 0,
            "purchase_qty": safe_int(ob["purchase_qty"].sum()) if not ob.empty else 0,
            "erp_revenue": safe_int(ob["erp_revenue"].sum()) if not ob.empty else 0,
            "matched_by_transaction_rows": safe_int(df[df["period"] == period]["matched_by_transaction_rows"].sum()) if not df.empty and "matched_by_transaction_rows" in df else 0,
            "matched_by_member_rows": safe_int(df[df["period"] == period]["matched_by_member_rows"].sum()) if not df.empty and "matched_by_member_rows" in df else 0,
        }

    ty = totals_for("TY")
    ly = totals_for("LY")
    yoy = {
        "orders": (ty["orders"] / ly["orders"] - 1) if ly["orders"] else None,
        "purchase_qty": (ty["purchase_qty"] / ly["purchase_qty"] - 1) if ly["purchase_qty"] else None,
        "erp_revenue": (ty["erp_revenue"] / ly["erp_revenue"] - 1) if ly["erp_revenue"] else None,
        "searches": (ty["searches"] / ly["searches"] - 1) if ly["searches"] else None,
    }

    direct_daily_product = (
        direct_df.groupby(["period", "search_date", "search_term", "product_code"], as_index=False).agg(
            product_name_kor=("product_name_kor", "first"),
            product_name=("product_name", "first"),
            relation_category=("relation_category", "first"),
            product_style=("product_style", "first"),
            orders=("orders", "sum"),
            purchase_qty=("purchase_qty", "sum"),
            erp_revenue=("erp_revenue", "sum"),
        ) if not direct_df.empty else pd.DataFrame(columns=["period", "search_date", "search_term", "product_code", "product_name_kor", "product_name", "relation_category", "product_style", "orders", "purchase_qty", "erp_revenue"])
    )
    if not direct_daily_product.empty:
        direct_daily_product["date"] = direct_daily_product["search_date"].astype(str)

    direct_product_rows = (
        direct_df.groupby(["period", "search_term", "product_code"], as_index=False).agg(
            product_style=("product_style", "first"),
            product_name_kor=("product_name_kor", "first"),
            product_name=("product_name", "first"),
            relation_category=("relation_category", "first"),
            category_manager_no=("category_manager_no", "first"),
            mdpick_depth2=("mdpick_depth2", "first"),
            orders=("orders", "sum"),
            purchase_qty=("purchase_qty", "sum"),
            erp_revenue=("erp_revenue", "sum"),
        ).sort_values(["period", "erp_revenue", "purchase_qty"], ascending=[True, False, False]).head(2000)
        if not direct_df.empty else pd.DataFrame(columns=["period", "search_term", "product_code", "product_name_kor", "product_name", "relation_category", "orders", "purchase_qty", "erp_revenue"])
    )

    return {
        "meta": {
            "title": "상품 키워드 성과",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "ly_start_date": (start_date.replace(year=start_date.year - 1)).isoformat(),
            "ly_end_date": (end_date.replace(year=end_date.year - 1)).isoformat(),
            "lookback_days": lookback_days,
            "updated_at_kst": dt.datetime.now(KST).strftime("%Y-%m-%d %H:%M KST"),
            "period_text": f"{start_date.isoformat()} ~ {end_date.isoformat()}",
            "source": "GA4 search terms + SQL Server order product mart",
        },
        "totals": {"TY": ty, "LY": ly, "YOY": yoy},
        "daily": daily.drop(columns=["search_date"], errors="ignore").to_dict("records"),
        "raw_daily_by_term": daily_term.drop(columns=["search_date"], errors="ignore").to_dict("records"),
        "raw_daily_by_product": daily_product.drop(columns=["search_date"], errors="ignore").to_dict("records"),
        "direct_daily_by_product": direct_daily_product.drop(columns=["search_date"], errors="ignore").to_dict("records"),
        "top_terms": top_terms.to_dict("records"),
        "product_rows": product_rows.to_dict("records"),
        "direct_product_rows": direct_product_rows.to_dict("records"),
        "spc_groups": SPC_GROUPS,
    }


HTML_TEMPLATE = r"""<!doctype html>
<html lang="ko">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>상품 키워드 성과 | Columbia E-COMM</title>
<script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;600;700;800;900&family=Noto+Sans+KR:wght@300;400;600;700;900&display=swap');
*{box-sizing:border-box} body{margin:0;background:linear-gradient(180deg,#f8fafc,#eef2f7);font-family:'Plus Jakarta Sans','Noto Sans KR',system-ui,sans-serif;color:#0f172a}.shell{max-width:1480px;margin:0 auto;padding:8px 12px 28px}.card{background:rgba(255,255,255,.95);border:1px solid #e2e8f0;box-shadow:0 16px 42px rgba(15,23,42,.05);border-radius:26px}.chart-card{padding:24px;margin-bottom:18px}.topbar{display:flex;align-items:flex-start;justify-content:space-between;gap:18px;margin-bottom:18px}.eyebrow{font-size:12px;font-weight:950;letter-spacing:.16em;text-transform:uppercase;color:#94a3b8}h1{font-size:26px;line-height:1.25;margin:5px 0 0;font-weight:950;letter-spacing:-.03em}.meta{font-size:12px;font-weight:850;color:#64748b;margin-top:8px}.controls{display:grid;grid-template-columns:repeat(3,minmax(150px,1fr));gap:10px;align-items:center;width:min(920px,100%)}.control{background:#fff;border:1px solid #e2e8f0;border-radius:16px;padding:8px 10px}.control label{display:block;font-size:10px;font-weight:950;letter-spacing:.12em;color:#64748b;text-transform:uppercase;margin-bottom:5px}.control select,.control input{width:100%;border:0;outline:0;background:transparent;font-size:13px;font-weight:900;color:#0f172a}.keyword-wrap{background:#fff200}.product-input{grid-column:span 2}.periods{display:flex;border:1px solid #bfdbfe;border-radius:16px;overflow:hidden;background:#d7eef8;height:48px}.period-btn{border:0;flex:1;background:transparent;font-size:14px;font-weight:950;color:#0f172a;cursor:pointer}.period-btn.active{background:#0f172a;color:#fff}.kpis{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-bottom:16px}.kpi{background:#f8fafc;border:1px solid #e2e8f0;border-radius:18px;padding:16px 18px;min-height:92px}.kpi-label{font-size:11px;font-weight:950;letter-spacing:.12em;text-transform:uppercase;color:#64748b}.kpi-value{font-size:30px;font-weight:950;margin-top:8px;letter-spacing:-.03em}.yoy{font-size:12px;font-weight:950;margin-top:5px;color:#64748b}.mixed-panel{background:#fff;border:1px solid #e2e8f0;border-radius:22px;padding:22px;min-height:410px}.panel-head{display:flex;align-items:flex-end;justify-content:space-between;gap:12px;margin-bottom:10px}.panel-title{font-size:18px;font-weight:950}.panel-sub{font-size:12px;font-weight:800;color:#64748b}.table-card{padding:20px 22px}.table-wrap{overflow:auto;border-radius:20px;border:1px solid #e2e8f0}table{width:100%;min-width:1280px;border-collapse:separate;border-spacing:0;background:white}th{background:#f8fafc;color:#64748b;font-size:11px;letter-spacing:.08em;text-transform:uppercase;padding:13px 14px;text-align:right;border-bottom:1px solid #e2e8f0;font-weight:950}th:first-child,td:first-child{text-align:left}td{padding:14px;border-bottom:1px solid #f1f5f9;font-size:13px;font-weight:850;text-align:right;white-space:nowrap}tr:hover td{background:#f8fafc}.rank{display:inline-flex;align-items:center;justify-content:center;width:26px;height:26px;border-radius:9px;background:#eff6ff;color:#1d4ed8;font-weight:950;margin-right:10px}.term{font-weight:950;color:#0f172a}.notice{display:none;margin:12px 0 0;padding:12px 14px;border-radius:16px;background:#fff7ed;border:1px solid #fed7aa;color:#9a3412;font-size:12px;font-weight:850}@media(max-width:900px){.topbar{flex-direction:column}.controls{grid-template-columns:1fr}.product-input{grid-column:span 1}.kpis{grid-template-columns:repeat(2,minmax(0,1fr))}.kpi-value{font-size:24px}}
</style>
</head>
<body>
<div class="shell">
<section class="card chart-card">
<div class="topbar">
<div><div class="eyebrow">DETAIL</div><h1>키워드별 구매상품 성과</h1><div class="meta" id="metaText">-</div></div>
<div class="controls">
<div class="control"><label>SPC 1 Depth</label><select id="spcDepth1"></select></div>
<div class="control"><label>SPC 2 Depth</label><select id="spcDepth2"></select></div>
<div class="control"><label>SPC 3 Depth</label><select id="spcDepth3"></select></div>
<div class="control keyword-wrap"><label>검색 키워드</label><input id="keywordFilterTop" placeholder="키워드 입력 / SPC 선택" /></div>
<div class="control product-input"><label>상품코드 복수/앞자리 검색</label><input id="productCodeFilter" placeholder="C7, C72YL3596774, C75BK9168010" /></div>
<div class="periods"><button class="period-btn active" data-view="daily">DAILY</button><button class="period-btn" data-view="week">WEEK</button></div>
</div>
</div>
<div class="kpis">
<div class="kpi"><div class="kpi-label">키워드 검색수</div><div class="kpi-value" id="kpiSearches">-</div><div class="yoy" id="yoySearches">YoY -</div></div>
<div class="kpi"><div class="kpi-label">주문수</div><div class="kpi-value" id="kpiOrders">-</div><div class="yoy" id="yoyOrders">YoY -</div></div>
<div class="kpi"><div class="kpi-label">구매수량</div><div class="kpi-value" id="kpiQty">-</div><div class="yoy" id="yoyQty">YoY -</div></div>
<div class="kpi"><div class="kpi-label">구매금액</div><div class="kpi-value" id="kpiRevenue">-</div><div class="yoy" id="yoyRevenue">YoY -</div></div>
</div>
<div class="mixed-panel"><div class="panel-head"><div><div class="panel-title">그래프 혼합</div><div class="panel-sub">구매수량 = 막대그래프 · 구매금액 = 꺾은선 그래프 · TY/LY 비교</div></div><div class="panel-sub" id="matchText">-</div></div><canvas id="mixedChart" height="108"></canvas><div class="notice" id="zeroNotice">구매 데이터가 0이면 SQL 주문상품 mart의 기간 범위 또는 GA4 transaction_id/order_no 매칭을 확인해야 합니다.</div></div>
</section>
<section class="card table-card"><div class="topbar" style="margin-bottom:14px;"><div><div class="eyebrow">DETAIL</div><h1 style="font-size:22px;">키워드 × 상품코드 성과</h1><div class="meta" id="detailPeriodText">-</div></div></div><div class="table-wrap"><table><thead><tr><th>키워드</th><th>상품코드</th><th>상품명</th><th>카테고리</th><th>TY 주문</th><th>LY 주문</th><th>TY 수량</th><th>LY 수량</th><th>TY 구매금액</th><th>LY 구매금액</th><th>매출 YoY</th></tr></thead><tbody id="termRows"></tbody></table></div></section>
</div>
<script>
const DATA=__DATA_JSON__; const SPC=DATA.spc_groups||{}; const fmtInt=v=>Number(v||0).toLocaleString('ko-KR'); const fmtKrw=v=>'₩'+Number(v||0).toLocaleString('ko-KR'); const fmtPct=v=>v===null||v===undefined?'-':`${(Number(v)*100).toFixed(1)}%`; let currentView='daily'; let chart=null;
function splitTokens(v){return String(v||'').split(/[\\s,;|]+/).map(x=>x.trim()).filter(Boolean);}
function codes(){return splitTokens(document.getElementById('productCodeFilter').value).map(x=>x.toUpperCase());}
function codeMatches(code, token){
  code=String(code||'').toUpperCase();
  token=String(token||'').toUpperCase().replace(/\*/g,'');
  if(!token)return true;
  // 상품코드 검색은 복수 검색 + prefix 검색을 모두 지원합니다.
  // 예: C72YL3596774 = exact / C7 또는 C72 = prefix / C7* = prefix
  if(token.length < 10) return code.startsWith(token);
  return code===token || code.startsWith(token);
}
function q(){return(document.getElementById('keywordFilterTop').value||'').trim().toLowerCase();}
function populateSpc(){const d1=document.getElementById('spcDepth1'),d2=document.getElementById('spcDepth2'),d3=document.getElementById('spcDepth3');d1.innerHTML='<option value="">SPC 선택</option>'+Object.keys(SPC).map(k=>`<option>${k}</option>`).join('');function f2(){const obj=SPC[d1.value]||{};d2.innerHTML='<option value="">2 Depth</option>'+Object.keys(obj).map(k=>`<option>${k}</option>`).join('');d3.innerHTML='<option value="">3 Depth</option>'}function f3(){const arr=(SPC[d1.value]||{})[d2.value]||[];d3.innerHTML='<option value="">3 Depth</option>'+arr.map(k=>`<option>${k}</option>`).join('')}d1.addEventListener('change',()=>{f2();document.getElementById('keywordFilterTop').value=d1.value||'';renderAll()});d2.addEventListener('change',()=>{f3();document.getElementById('keywordFilterTop').value=d2.value||d1.value||'';renderAll()});d3.addEventListener('change',()=>{if(d3.value)document.getElementById('keywordFilterTop').value=d3.value;renderAll()});f2()}
function groupWeekly(rows){
  const map=new Map();
  rows.forEach(r=>{
    const d=new Date(r.date+'T00:00:00');
    const day=d.getDay();
    const monday=new Date(d);
    monday.setDate(d.getDate()-((day+6)%7));
    const week=monday.toISOString().slice(0,10);
    const p=r.period||'TY';
    const isProduct=!!r.product_code;
    const mk=isProduct
      ? [p,week,r.search_term||'',String(r.product_code||'').toUpperCase()].join('|')
      : [p,week].join('|');
    if(!map.has(mk)){
      map.set(mk,{
        period:p,
        date:week,
        search_term:r.search_term,
        product_code:String(r.product_code||'').toUpperCase(),
        product_name_kor:r.product_name_kor,
        product_name:r.product_name,
        relation_category:r.relation_category,
        product_style:r.product_style,
        searches:0,
        orders:0,
        purchase_qty:0,
        erp_revenue:0
      });
    }
    const x=map.get(mk);
    x.searches+=Number(r.searches||0);
    x.orders+=Number(r.orders||0);
    x.purchase_qty+=Number(r.purchase_qty||0);
    x.erp_revenue+=Number(r.erp_revenue||0);
  });
  return Array.from(map.values()).sort((a,b)=>a.date.localeCompare(b.date)||a.period.localeCompare(b.period));
}
function rowText(r){
  return [
    r.search_term,
    r.product_code,
    r.product_name_kor,
    r.product_name,
    r.relation_category,
    r.product_style
  ].map(x=>String(x||'').toLowerCase()).join(' ');
}
function rowMatches(r){
  const qq=q();
  const cc=codes();
  const code=String(r.product_code||'').toUpperCase();
  const keywordOk=!qq || rowText(r).includes(qq);
  const codeOk=!cc.length || cc.some(t=>codeMatches(code,t));
  return keywordOk && codeOk;
}
function filterRows(rows){return (rows||[]).filter(rowMatches)}
function productSourceRows(){
  // 상품코드만 입력한 경우에는 GA4 검색매칭 rows가 아니라 SQL 상품코드 직접 매출 rows를 사용합니다.
  // 키워드가 함께 있으면 검색매칭 rows를 우선 사용하고, 없을 때만 direct rows로 fallback합니다.
  const hasCode=codes().length>0;
  const hasKeyword=!!q();
  if(hasCode && !hasKeyword) return filterRows(DATA.direct_daily_by_product||[]);
  let rows=filterRows(DATA.raw_daily_by_product||[]);
  if(!rows.length && hasCode) rows=filterRows(DATA.direct_daily_by_product||[]);
  return rows;
}
function trendRows(){
  let rows;
  if(q() || codes().length){
    rows=productSourceRows();
    if(!rows.length && q() && !codes().length) rows=filterRows(DATA.raw_daily_by_term||[]);
  } else {
    rows=DATA.daily||[];
  }
  return currentView==='week'?groupWeekly(rows):rows;
}
function totalsFromRows(rows, period){const rr=rows.filter(r=>(r.period||'TY')===period);return rr.reduce((t,r)=>{t.searches+=Number(r.searches||0);t.orders+=Number(r.orders||0);t.purchase_qty+=Number(r.purchase_qty||0);t.erp_revenue+=Number(r.erp_revenue||0);return t},{searches:0,orders:0,purchase_qty:0,erp_revenue:0})}
function latestPeriodRows(rows, period){
  const rr=rows.filter(r=>(r.period||'TY')===period);
  if(!rr.length)return [];
  const latest=rr.map(r=>r.date).sort().slice(-1)[0];
  return rr.filter(r=>r.date===latest);
}
function currentTotals(){
  // DAILY/WEEK 버튼은 차트뿐 아니라 KPI 기준 기간도 바꿉니다.
  // DAILY = 선택 조건 내 가장 최근 1일
  // WEEK  = 선택 조건 내 가장 최근 주차 합계
  const rows=trendRows();
  const tyRows=latestPeriodRows(rows,'TY');
  const lyRows=latestPeriodRows(rows,'LY');
  const TY=totalsFromRows(tyRows,'TY');
  const LY=totalsFromRows(lyRows,'LY');
  return {
    TY, LY,
    YOY:{
      searches:LY.searches?TY.searches/LY.searches-1:null,
      orders:LY.orders?TY.orders/LY.orders-1:null,
      purchase_qty:LY.purchase_qty?TY.purchase_qty/LY.purchase_qty-1:null,
      erp_revenue:LY.erp_revenue?TY.erp_revenue/LY.erp_revenue-1:null
    }
  }
}
function renderHeader(){const t=currentTotals();const ty=t.TY||{};const ly=t.LY||{};const yy=t.YOY||{};document.getElementById('kpiSearches').textContent=fmtInt(ty.searches);document.getElementById('kpiOrders').textContent=fmtInt(ty.orders);document.getElementById('kpiQty').textContent=fmtInt(ty.purchase_qty);document.getElementById('kpiRevenue').textContent=fmtKrw(ty.erp_revenue);document.getElementById('yoySearches').textContent=`LY ${fmtInt(ly.searches)} · YoY ${fmtPct(yy.searches)}`;document.getElementById('yoyOrders').textContent=`LY ${fmtInt(ly.orders)} · YoY ${fmtPct(yy.orders)}`;document.getElementById('yoyQty').textContent=`LY ${fmtInt(ly.purchase_qty)} · YoY ${fmtPct(yy.purchase_qty)}`;document.getElementById('yoyRevenue').textContent=`LY ${fmtKrw(ly.erp_revenue)} · YoY ${fmtPct(yy.erp_revenue)}`;document.getElementById('metaText').textContent=`${currentView==='daily'?'DAILY 최근 1일':'WEEK 최근 주차'} · TY ${DATA.meta.period_text||'-'} · LY ${DATA.meta.ly_start_date||'-'} ~ ${DATA.meta.ly_end_date||'-'} · ${DATA.meta.updated_at_kst||'-'}`;const all=DATA.totals?.TY||{};document.getElementById('matchText').textContent=`transaction rows ${fmtInt(all.matched_by_transaction_rows||0)} · member rows ${fmtInt(all.matched_by_member_rows||0)}`;document.getElementById('zeroNotice').style.display=Number(all.searches||0)>0&&Number(all.orders||0)===0?'block':'none'}
function renderChart(){const rows=trendRows();const labels=[...new Set(rows.map(r=>r.date))].sort();const val=(p,k)=>labels.map(d=>rows.filter(r=>r.date===d&&(r.period||'TY')===p).reduce((s,r)=>s+Number(r[k]||0),0));const ctx=document.getElementById('mixedChart');if(chart)chart.destroy();chart=new Chart(ctx,{data:{labels,datasets:[{type:'bar',label:'TY 구매수량',data:val('TY','purchase_qty'),backgroundColor:'rgba(96,165,250,.6)',borderWidth:0,borderRadius:8,yAxisID:'y'},{type:'bar',label:'LY 구매수량',data:val('LY','purchase_qty'),backgroundColor:'rgba(148,163,184,.35)',borderWidth:0,borderRadius:8,yAxisID:'y'},{type:'line',label:'TY 구매금액',data:val('TY','erp_revenue'),borderColor:'rgba(244,63,94,.9)',backgroundColor:'rgba(244,63,94,.12)',borderWidth:3,pointRadius:3,tension:.35,yAxisID:'y1'},{type:'line',label:'LY 구매금액',data:val('LY','erp_revenue'),borderColor:'rgba(100,116,139,.85)',backgroundColor:'rgba(100,116,139,.1)',borderWidth:2,pointRadius:2,tension:.35,borderDash:[6,4],yAxisID:'y1'}]},options:{responsive:true,interaction:{mode:'index',intersect:false},plugins:{legend:{position:'top',labels:{font:{weight:'bold'}}}},scales:{x:{grid:{display:false},ticks:{font:{weight:'bold'},maxRotation:0,autoSkip:true}},y:{beginAtZero:true,grid:{color:'rgba(226,232,240,.9)'},ticks:{callback:v=>fmtInt(v)}},y1:{beginAtZero:true,position:'right',grid:{drawOnChartArea:false},ticks:{callback:v=>fmtKrw(v)}}}}})}
function scopedProductRows(){
  let base=productSourceRows();
  if(currentView==='week') base=groupWeekly(base);
  const ty=latestRowsByPeriod(base,'TY');
  const ly=latestRowsByPeriod(base,'LY');
  return [...ty,...ly];
}
function mergedTableRows(){
  // Detail table follows the same DAILY/WEEK and filters as KPI.
  const scoped=scopedProductRows();
  const map=new Map();
  scoped.forEach(r=>{
    const key=`${r.search_term}||${String(r.product_code||'').toUpperCase()}`;
    if(!map.has(key)){
      map.set(key,{
        search_term:r.search_term,
        product_code:String(r.product_code||'').toUpperCase(),
        product_name_kor:r.product_name_kor,
        product_name:r.product_name,
        relation_category:r.relation_category,
        TY:{},
        LY:{}
      });
    }
    const target=map.get(key);
    target[r.period||'TY']=r;
    if(!target.product_name_kor) target.product_name_kor=r.product_name_kor;
    if(!target.product_name) target.product_name=r.product_name;
    if(!target.relation_category) target.relation_category=r.relation_category;
  });
  return Array.from(map.values())
    .filter(r=>Number(r.TY?.erp_revenue||0)>0 || Number(r.LY?.erp_revenue||0)>0 || Number(r.TY?.purchase_qty||0)>0 || Number(r.LY?.purchase_qty||0)>0)
    .sort((a,b)=>
      Number(b.TY?.erp_revenue||0)-Number(a.TY?.erp_revenue||0)
      || Number(b.LY?.erp_revenue||0)-Number(a.LY?.erp_revenue||0)
    )
    .slice(0,300);
}
function renderTable(){
  const rows=mergedTableRows();
  const tbody=document.getElementById('termRows');
  const label=currentView==='daily'?'DAILY 최근 1일 기준 · 필터 적용':'WEEK 최근 주차 기준 · 필터 적용';
  const dpt=document.getElementById('detailPeriodText');
  if(dpt)dpt.textContent=label;
  tbody.innerHTML=rows.map((r,idx)=>{
    const ty=r.TY||{},ly=r.LY||{};
    const yoy=Number(ly.erp_revenue||0)?Number(ty.erp_revenue||0)/Number(ly.erp_revenue||0)-1:null;
    return`<tr><td><span class="rank">${idx+1}</span><span class="term">${r.search_term||'-'}</span></td><td>${r.product_code||'-'}</td><td>${r.product_name_kor||r.product_name||'-'}</td><td>${r.relation_category||'-'}</td><td>${fmtInt(ty.orders)}</td><td>${fmtInt(ly.orders)}</td><td>${fmtInt(ty.purchase_qty)}</td><td>${fmtInt(ly.purchase_qty)}</td><td>${fmtKrw(ty.erp_revenue)}</td><td>${fmtKrw(ly.erp_revenue)}</td><td>${fmtPct(yoy)}</td></tr>`
  }).join('');
}
function renderAll(){renderHeader();renderChart();renderTable();try{parent.postMessage({type:'dailyDigestResize',height:document.documentElement.scrollHeight},'*')}catch(e){}}
document.querySelectorAll('.period-btn[data-view]').forEach(btn=>btn.addEventListener('click',()=>{document.querySelectorAll('.period-btn[data-view]').forEach(b=>b.classList.remove('active'));btn.classList.add('active');currentView=btn.dataset.view;renderAll()}));document.getElementById('keywordFilterTop').addEventListener('input',renderAll);document.getElementById('productCodeFilter').addEventListener('input',renderAll);populateSpc();renderAll();
</script></body></html>"""


def render_html(payload: dict) -> str:
    return HTML_TEMPLATE.replace("__DATA_JSON__", json.dumps(payload, ensure_ascii=False))


def main() -> int:
    setup_google_credentials()
    project = getenv("BQ_PROJECT", "columbia-ga4")
    location = getenv("BQ_LOCATION", "asia-northeast3")
    client = bigquery.Client(project=project or None, location=location or None)

    today = kst_today()
    default_end = today - dt.timedelta(days=1)
    days = int(getenv("PRODUCT_KEYWORD_DAYS", getenv("SEARCH_VOLUME_DAYS", "30")))
    end_date = parse_date(getenv("PRODUCT_KEYWORD_END_DATE", getenv("SEARCH_VOLUME_END_DATE")), default_end)
    start_date = parse_date(getenv("PRODUCT_KEYWORD_START_DATE", getenv("SEARCH_VOLUME_START_DATE")), end_date - dt.timedelta(days=days - 1))
    lookback_days = int(getenv("SEARCH_TO_ORDER_LOOKBACK_DAYS", "7"))

    out_dir = Path(getenv("PRODUCT_KEYWORD_OUT_DIR", getenv("SEARCH_VOLUME_OUT_DIR", "reports/product_keyword")))
    data_dir = out_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    df = run_query(client, start_date, end_date, lookback_days)
    direct_df = run_direct_product_query(client, start_date, end_date)
    payload = build_payload(df, direct_df, start_date, end_date, lookback_days)

    (data_dir / "product_keyword.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (data_dir / "meta.json").write_text(json.dumps(payload["meta"], ensure_ascii=False, indent=2), encoding="utf-8")
    (out_dir / "index.html").write_text(render_html(payload), encoding="utf-8")
    log(f"Wrote {out_dir / 'index.html'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
