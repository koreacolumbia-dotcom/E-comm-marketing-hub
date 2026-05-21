#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Columbia Purchase Pattern HTML Report
=====================================

BigQuery 구매 마트에서 전체/선택 기간 구매 데이터를 분석해
GitHub Pages용 정적 HTML, summary.json, CSV, Excel을 생성한다.

V7 핵심 패치
-----------
- 상단 KPI 날짜/기간 선택 UI 및 HTML 내장 데이터 기반 KPI 재계산
- 월별 매출 흐름 전체 월 표시 및 2026 포함 여부 확인 배지
- TOP PRODUCTS 이미지 매칭/크롤링 보강
- MEMBER SEGMENTS 한글 라벨화 및 01_ 같은 정렬 prefix 제거
- 표 중심 UI를 카드/바/히트맵형 시각화로 전환
- 회원등급별 구매/재구매 현황 추가
- 재구매 시 많이 산 카테고리/상품 추가
- 집단별 특징 자동 코멘트 추가
"""
from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
import json
import os
import re
import sys
import time
from html import escape
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import quote_plus, urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    from google.cloud import bigquery  # type: ignore
except Exception:
    bigquery = None

KST = dt.timezone(dt.timedelta(hours=9))
BASE_DIR = Path(__file__).resolve().parent
DEFAULT_OUT_DIR = Path("reports") / "purchase_pattern"
DEFAULT_SOURCE_TABLE = "tb_order_product_search_mart"
DEFAULT_MEMBER_TABLE = "tb_member_staging"
OFFICIAL_MALL_BASE = "https://www.columbiakorea.co.kr"

SCRIPT_VERSION = "PURCHASE_PATTERN_HTML_REPORT_V8_GRADE_COUPON_PERIOD_DYNAMIC"
PATCH_NOTES = [
    "date_range_controls_for_top_kpis",
    "monthly_chart_full_period_with_2026_check",
    "stronger_product_image_matching_and_crawling",
    "korean_labels_and_visual_cards",
    "member_grade_repeat_purchase_analysis",
    "repeat_category_and_product_analysis",
    "group_characteristics_auto_summary",
    "grade_1_4_family_silver_gold_titanium_mapping",
    "coupon_name_analysis",
    "period_filter_updates_all_major_sections",
    "grade_split_repeat_product_and_category_cards",
    "monthly_revenue_collapsible_section",
]


# =========================================================
# V7 PATCH CONFIRMATION
# =========================================================
# This block is intentionally visible in GitHub diff.
# V7 includes:
# - Top KPI date/range controls
# - Full monthly revenue timeline including 2026 check badge
# - Stronger TOP PRODUCTS image matching/crawling
# - Korean visual sections instead of raw-looking tables
# - Member grade repurchase overview
# - Repeat category/product analysis
# - Auto-written group characteristics
# =========================================================

REPORT_PATCH_CSS = """
<style>
  :root{--motion-ease:cubic-bezier(.2,.8,.2,1);--brand:#002d72;}
  body{font-family:'Plus Jakarta Sans','Noto Sans KR','Malgun Gothic','Apple SD Gothic Neo',system-ui,-apple-system,'Segoe UI',Roboto,Arial;}
  .report-body{background:linear-gradient(180deg,#f8fafc 0%,#eef2f7 100%);}
  .report-card,.product-card,.viz-card{animation:cardRise .62s var(--motion-ease) both;transform-origin:center bottom;}
  .report-card:hover,.product-card:hover,.viz-card:hover{transform:translateY(-3px);box-shadow:0 18px 40px rgba(15,23,42,.08);}
  .kpi-card{position:relative;overflow:hidden;transition:transform .24s var(--motion-ease),box-shadow .24s var(--motion-ease),border-color .24s var(--motion-ease)}
  .kpi-card:before{content:'';position:absolute;inset:-40% auto auto -20%;width:60%;height:180%;background:linear-gradient(90deg,transparent,rgba(255,255,255,.45),transparent);transform:rotate(14deg);animation:shineSweep 4.2s linear infinite;pointer-events:none}
  .kpi-card:hover{transform:translateY(-5px) scale(1.01);box-shadow:0 22px 44px rgba(15,23,42,.08);border-color:rgba(59,130,246,.22)}
  .kpi-value{animation:numberPop .8s var(--motion-ease) both}
  .bar-track{height:9px;border-radius:999px;background:#e2e8f0;overflow:hidden}.bar-fill{height:100%;border-radius:999px;background:linear-gradient(90deg,#0f172a,#2563eb);}
  .bar-fill-soft{height:100%;border-radius:999px;background:linear-gradient(90deg,#93c5fd,#1d4ed8);}
  .product-img{aspect-ratio:1/1;object-fit:cover;background:#f1f5f9;}
  .pill{border:1px solid rgba(148,163,184,.28);background:#fff;border-radius:999px;padding:8px 12px;font-size:12px;font-weight:900;color:#475569;transition:.18s ease;}
  .pill:hover{transform:translateY(-1px);box-shadow:0 10px 20px rgba(15,23,42,.06)}
  .pill.active{background:#0f172a;color:#fff;border-color:#0f172a;}
  .heat-cell{border-radius:14px;background:#eff6ff;border:1px solid rgba(59,130,246,.12);}
  .scroll-x{overflow-x:auto;}
  .month-strip{min-width:1120px;}
  .table-wrap{overflow-x:auto}.table-wrap table{min-width:860px}
  details.month-details summary{cursor:pointer;list-style:none}
  details.month-details summary::-webkit-details-marker{display:none}
  details.month-details[open] .chev{transform:rotate(180deg)}
  .fade-swap{animation:fadeSwap .34s var(--motion-ease) both}
  .stagger-card{animation:cardRise .58s var(--motion-ease) both}
  .pulse-dot{animation:pulseDot 1.8s ease-in-out infinite}
  @keyframes fadeSwap{from{opacity:.25;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}
  @keyframes pulseDot{0%,100%{opacity:.35;transform:scale(.86)}50%{opacity:1;transform:scale(1.08)}}
  @keyframes cardRise{from{opacity:0;transform:translateY(22px) scale(.985)}to{opacity:1;transform:translateY(0) scale(1)}}
  @keyframes numberPop{0%{opacity:.2;transform:translateY(10px) scale(.96)}60%{opacity:1;transform:translateY(-2px) scale(1.02)}100%{opacity:1;transform:translateY(0) scale(1)}}
  @keyframes shineSweep{0%{transform:translateX(-160%) rotate(14deg)}100%{transform:translateX(320%) rotate(14deg)}}
</style>
"""


def log(msg: str) -> None:
    now = dt.datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST")
    print(f"[{now}] {msg}", flush=True)


def getenv(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def setup_google_credentials() -> None:
    cred_path = getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if cred_path and Path(cred_path).exists():
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path
        log(f"Using GOOGLE_APPLICATION_CREDENTIALS: {cred_path}")
        return
    default_local_cred = Path(r"C:\Users\122431\Downloads\columbia-ga4-fe5abb360210.json")
    if default_local_cred.exists():
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(default_local_cred)
        log(f"Using default local GOOGLE_APPLICATION_CREDENTIALS: {default_local_cred}")
        return
    b64 = getenv("GOOGLE_SA_JSON_B64")
    if b64:
        out = Path(getenv("GOOGLE_SA_JSON_OUT", "gcp_service_account.json"))
        out.write_bytes(base64.b64decode(b64))
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(out.resolve())
        log(f"Wrote service account json: {out.resolve()}")
        return
    raise RuntimeError("Google credentials not found. Set GOOGLE_APPLICATION_CREDENTIALS or GOOGLE_SA_JSON_B64.")


def bq_client(project: str, location: str) -> bigquery.Client:
    if bigquery is None:
        raise RuntimeError("google-cloud-bigquery is not installed. Run: pip install google-cloud-bigquery")
    return bigquery.Client(project=project, location=location)


def normalize_table_id(table_id: str, project: str, raw_dataset: str) -> str:
    table_id = str(table_id or "").strip().strip("`")
    parts = [p for p in table_id.split(".") if p]
    if len(parts) == 1:
        return f"{project}.{raw_dataset}.{parts[0]}"
    if len(parts) == 2:
        return f"{project}.{parts[0]}.{parts[1]}"
    if len(parts) == 3:
        return ".".join(parts)
    raise ValueError(f"Invalid BigQuery table id: {table_id}")


def parse_bq_table_parts(table_id: str) -> tuple[str, str, str]:
    parts = str(table_id or "").strip("`").split(".")
    if len(parts) != 3:
        raise ValueError(f"Expected project.dataset.table: {table_id}")
    return parts[0], parts[1], parts[2]


def get_table_columns(client: bigquery.Client, table_id: str, location: str) -> set[str]:
    project, dataset, table = parse_bq_table_parts(table_id)
    sql = f"""
    SELECT column_name
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = @table_name
    """
    cfg = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("table_name", "STRING", table)]
    )
    df = client.query(sql, job_config=cfg, location=location).to_dataframe()
    return {str(x).strip() for x in df["column_name"].tolist() if str(x).strip()}


def safe_get_columns(client: bigquery.Client, table_id: str, location: str) -> set[str]:
    try:
        return get_table_columns(client, table_id, location)
    except Exception as e:
        log(f"Optional table unavailable: {table_id} / {type(e).__name__}: {e}")
        return set()


def pick_col(columns: set[str], candidates: Iterable[str], default: str = "") -> str:
    lowered = {c.lower(): c for c in columns}
    for cand in candidates:
        if cand and cand.lower() in lowered:
            return lowered[cand.lower()]
    return default


def safe_str_expr(columns: set[str], col: str, alias: str, default: str = "") -> str:
    if col and col in columns:
        return f"NULLIF(TRIM(CAST({col} AS STRING)), '') AS {alias}"
    return f"CAST('{default}' AS STRING) AS {alias}"


def date_filter_sql(start_date: str, end_date: str) -> str:
    parts = []
    if start_date:
        parts.append("DATE(order_date) >= @start_date")
    if end_date:
        parts.append("DATE(order_date) <= @end_date")
    return "" if not parts else "\n    AND " + "\n    AND ".join(parts)


def query_config(start_date: str, end_date: str) -> bigquery.QueryJobConfig:
    params = []
    if start_date:
        params.append(bigquery.ScalarQueryParameter("start_date", "DATE", start_date))
    if end_date:
        params.append(bigquery.ScalarQueryParameter("end_date", "DATE", end_date))
    return bigquery.QueryJobConfig(query_parameters=params)


def fmt_int(v: Any) -> str:
    try:
        return f"{int(round(float(v))):,}"
    except Exception:
        return "-"


def fmt_krw(v: Any) -> str:
    try:
        return f"₩{int(round(float(v))):,}"
    except Exception:
        return "-"


def fmt_pct(v: Any, digits: int = 1) -> str:
    try:
        return f"{float(v) * 100:.{digits}f}%"
    except Exception:
        return "-"


def df_to_records(df: pd.DataFrame, limit: Optional[int] = None) -> list[dict[str, Any]]:
    if df is None or df.empty:
        return []
    out = df.head(limit).copy() if limit else df.copy()
    return json.loads(out.to_json(orient="records", force_ascii=False, date_format="iso"))


# =========================================================
# BigQuery SQL
# =========================================================

def build_member_join_parts(member_table: str, member_columns: set[str], source_columns: set[str]) -> tuple[str, str, str, str]:
    """Return member join SQL and grade expressions.

    If the order mart already has grade columns, use them.
    Otherwise try LEFT JOIN member table using MemberID/member_id.
    """
    source_grade_no = pick_col(source_columns, ["member_grade_no", "MemberGradeNo", "grade_no", "memberGradeNo"])
    source_grade_name = pick_col(source_columns, ["member_grade_name", "MemberGradeName", "grade_name", "memberGradeName"])
    if source_grade_no or source_grade_name:
        grade_raw_expr = f"CAST({source_grade_no} AS STRING)" if source_grade_no else "''"
        grade_name_expr = f"CAST({source_grade_name} AS STRING)" if source_grade_name else "''"
        return "", grade_raw_expr, grade_name_expr, "source_table"

    member_id_col = pick_col(member_columns, ["MemberID", "member_id", "memberId", "userid", "user_id"])
    member_grade_no = pick_col(member_columns, ["MemberGradeNo", "member_grade_no", "grade_no", "memberGradeNo"])
    member_grade_name = pick_col(member_columns, ["MemberGradeName", "member_grade_name", "GradeName", "grade_name", "memberGrade"])
    if member_table and member_id_col and (member_grade_no or member_grade_name):
        join_sql = f"""
LEFT JOIN `{member_table}` m
  ON TRIM(CAST(m.{member_id_col} AS STRING)) = TRIM(CAST(src.member_id AS STRING))
"""
        grade_raw_expr = f"CAST(m.{member_grade_no} AS STRING)" if member_grade_no else "''"
        grade_name_expr = f"CAST(m.{member_grade_name} AS STRING)" if member_grade_name else "''"
        return join_sql, grade_raw_expr, grade_name_expr, "member_table"

    return "", "''", "''", "unknown"


def build_base_cte(
    source_table: str,
    columns: set[str],
    start_date: str,
    end_date: str,
    member_table: str = "",
    member_columns: Optional[set[str]] = None,
) -> str:
    member_columns = member_columns or set()
    revenue_col = pick_col(columns, ["admin_revenue", "admin_net_product_amount", "net_erp_revenue", "erp_revenue", "revenue"])
    gross_col = pick_col(columns, ["line_gross_admin_revenue", "order_product_price", "gross_revenue", "product_price"])
    qty_col = pick_col(columns, ["purchase_qty", "ProductQuantity", "quantity", "qty"])
    image_col = pick_col(columns, ["image_url", "product_image_url", "thumbnail_url", "product_img_url", "main_image_url", "img_url"])
    product_name_kr = pick_col(columns, ["product_name_kor", "ProductName_Kor", "item_name", "itemName", "product_name"])
    product_name = pick_col(columns, ["product_name", "ProductName", "itemName", "item_name"])
    category_kr = pick_col(columns, ["category_title_kr", "category_name_kr", "category_kr"])
    category = pick_col(columns, ["category_title", "category_code", "category", "category_name"])
    category_depth2 = pick_col(columns, ["category_depth2_kr", "category_depth2", "category2_kr", "category2", "middle_category_kr", "middle_category", "cate2_name", "category_name_depth2"])
    coupon_name_col = pick_col(columns, ["coupon_name", "coupon_title", "coupon_nm", "CouponName", "order_coupon_name", "used_coupon_name", "promotion_coupon_name"])
    order_dt = pick_col(columns, ["order_datetime", "order_reg_datetime", "order_product_datetime", "order_date"])
    product_code = pick_col(columns, ["product_code", "ProductCode", "item_id", "itemId", "sku"])
    order_no = pick_col(columns, ["order_no", "OrderNo"])
    member_id = pick_col(columns, ["member_id", "MemberID", "user_id", "userid"])
    member_age_col = pick_col(columns, ["member_age", "MemberAge", "age"])
    member_age_expr = (
        f"COALESCE(SAFE_CAST(src.{member_age_col} AS INT64), NULL) AS member_age"
        if member_age_col else
        "CAST(NULL AS INT64) AS member_age"
    )

    if not revenue_col:
        raise RuntimeError("No revenue column found. Expected admin_revenue/admin_net_product_amount/net_erp_revenue/erp_revenue/revenue.")
    if not qty_col:
        raise RuntimeError("No quantity column found. Expected purchase_qty/ProductQuantity/quantity/qty.")
    if not order_no or not member_id or not product_code:
        raise RuntimeError("Required columns missing. Need order_no, member_id, product_code equivalents.")

    refund_condition = ""
    if "order_refund_status" in columns:
        refund_condition = "AND COALESCE(SAFE_CAST(order_refund_status AS INT64), 0) = 0"
    net_sales_condition = ""
    if "is_net_sales_line" in columns:
        net_sales_condition = "AND COALESCE(SAFE_CAST(is_net_sales_line AS INT64), 1) = 1"

    join_sql, grade_raw_expr, grade_name_expr, grade_source = build_member_join_parts(member_table, member_columns, columns)

    return f"""
WITH src AS (
  SELECT *
  FROM `{source_table}`
  WHERE order_date IS NOT NULL
    AND CAST({order_no} AS STRING) IS NOT NULL
    AND TRIM(CAST({order_no} AS STRING)) != ''
    AND CAST({member_id} AS STRING) IS NOT NULL
    AND TRIM(CAST({member_id} AS STRING)) != ''
    AND COALESCE(SAFE_CAST({qty_col} AS INT64), 0) > 0
    AND COALESCE(SAFE_CAST({revenue_col} AS INT64), 0) > 0
    {refund_condition}
    {net_sales_condition}
    {date_filter_sql(start_date, end_date)}
),
purchase_lines AS (
  SELECT
    DATE(src.order_date) AS order_date,
    DATETIME(src.{order_dt}) AS order_datetime,
    EXTRACT(YEAR FROM DATE(src.order_date)) AS year,
    FORMAT_DATE('%Y-%m', DATE(src.order_date)) AS month,
    FORMAT_DATE('%G-W%V', DATE(src.order_date)) AS iso_week,
    EXTRACT(DAYOFWEEK FROM DATE(src.order_date)) AS dow_num,
    CASE EXTRACT(DAYOFWEEK FROM DATE(src.order_date))
      WHEN 1 THEN '일' WHEN 2 THEN '월' WHEN 3 THEN '화' WHEN 4 THEN '수'
      WHEN 5 THEN '목' WHEN 6 THEN '금' WHEN 7 THEN '토'
    END AS weekday,
    EXTRACT(HOUR FROM DATETIME(src.{order_dt})) AS order_hour,
    CAST(src.{order_no} AS STRING) AS order_no,
    TRIM(CAST(src.{member_id} AS STRING)) AS member_id,
    UPPER(TRIM(CAST(src.{product_code} AS STRING))) AS product_code,
    {safe_str_expr(columns, product_name_kr, 'product_name_kor')},
    {safe_str_expr(columns, product_name, 'product_name')},
    {safe_str_expr(columns, pick_col(columns, ['product_style', 'ProductStyle']), 'product_style')},
    {safe_str_expr(columns, pick_col(columns, ['product_size', 'ProductSize']), 'product_size')},
    {safe_str_expr(columns, pick_col(columns, ['product_color', 'ProductColor']), 'product_color')},
    {safe_str_expr(columns, pick_col(columns, ['master_product_color', 'ProductColorName']), 'master_product_color')},
    {safe_str_expr(columns, category_kr, 'category_title_kr')},
    {safe_str_expr(columns, category, 'category_title')},
    {safe_str_expr(columns, category_depth2, 'category_depth2')},
    {safe_str_expr(columns, pick_col(columns, ['category_code']), 'category_code')},
    {safe_str_expr(columns, pick_col(columns, ['sex_label', 'gender_label']), 'sex_label')},
    {safe_str_expr(columns, pick_col(columns, ['product_year', 'ProductYear']), 'product_year')},
    {safe_str_expr(columns, pick_col(columns, ['product_season', 'ProductSeason']), 'product_season')},
    {safe_str_expr(columns, pick_col(columns, ['member_gender', 'MemberGender']), 'member_gender')},
    {member_age_expr},
    {safe_str_expr(columns, pick_col(columns, ['order_device_type', 'OrderSaleCategory']), 'order_device_type')},
    {safe_str_expr(columns, coupon_name_col, 'coupon_name')},
    {safe_str_expr(columns, image_col, 'source_image_url')},
    NULLIF(TRIM({grade_raw_expr}), '') AS member_grade_raw,
    CASE
      WHEN TRIM(COALESCE(NULLIF(TRIM({grade_raw_expr}), ''), NULLIF(TRIM({grade_name_expr}), ''), '')) = '1'
        OR REGEXP_CONTAINS(UPPER(COALESCE(NULLIF(TRIM({grade_name_expr}), ''), NULLIF(TRIM({grade_raw_expr}), ''), '')), r'FAMILY|패밀리') THEN 'FAMILY'
      WHEN TRIM(COALESCE(NULLIF(TRIM({grade_raw_expr}), ''), NULLIF(TRIM({grade_name_expr}), ''), '')) = '2'
        OR REGEXP_CONTAINS(UPPER(COALESCE(NULLIF(TRIM({grade_name_expr}), ''), NULLIF(TRIM({grade_raw_expr}), ''), '')), r'SILVER|실버') THEN 'SILVER'
      WHEN TRIM(COALESCE(NULLIF(TRIM({grade_raw_expr}), ''), NULLIF(TRIM({grade_name_expr}), ''), '')) = '3'
        OR REGEXP_CONTAINS(UPPER(COALESCE(NULLIF(TRIM({grade_name_expr}), ''), NULLIF(TRIM({grade_raw_expr}), ''), '')), r'GOLD|골드') THEN 'GOLD'
      WHEN TRIM(COALESCE(NULLIF(TRIM({grade_raw_expr}), ''), NULLIF(TRIM({grade_name_expr}), ''), '')) = '4'
        OR REGEXP_CONTAINS(UPPER(COALESCE(NULLIF(TRIM({grade_name_expr}), ''), NULLIF(TRIM({grade_raw_expr}), ''), '')), r'TITANIUM|티타늄') THEN 'TITANIUM'
      ELSE COALESCE(NULLIF(TRIM({grade_name_expr}), ''), NULLIF(TRIM({grade_raw_expr}), ''), 'UNKNOWN')
    END AS member_grade_label,
    '{grade_source}' AS member_grade_source,
    COALESCE(SAFE_CAST(src.{qty_col} AS INT64), 0) AS purchase_qty,
    COALESCE(SAFE_CAST(src.{revenue_col} AS INT64), 0) AS revenue,
    COALESCE(SAFE_CAST(src.{gross_col if gross_col else revenue_col} AS INT64), 0) AS gross_revenue,
    COALESCE(SAFE_CAST(src.{pick_col(columns, ['order_use_coupon_total', 'order_use_coupon_price'], '0')} AS INT64), 0) AS coupon_amount,
    COALESCE(SAFE_CAST(src.{pick_col(columns, ['order_product_use_mileage', 'order_use_point'], '0')} AS INT64), 0) AS mileage_amount,
    COALESCE(SAFE_CAST(src.{pick_col(columns, ['product_promotion_sale_price', 'promotion_amount'], '0')} AS INT64), 0) AS promotion_amount,
    COALESCE(SAFE_CAST(src.{pick_col(columns, ['is_coupon_order'], '0')} AS INT64), 0) AS is_coupon_order,
    COALESCE(SAFE_CAST(src.{pick_col(columns, ['is_point_used_order'], '0')} AS INT64), 0) AS is_point_used_order,
    COALESCE(SAFE_CAST(src.{pick_col(columns, ['is_promotion_line'], '0')} AS INT64), 0) AS is_promotion_line
  FROM src
  {join_sql}
)
"""


def build_queries(
    source_table: str,
    columns: set[str],
    start_date: str,
    end_date: str,
    top_limit: int,
    member_table: str = "",
    member_columns: Optional[set[str]] = None,
) -> dict[str, str]:
    base = build_base_cte(source_table, columns, start_date, end_date, member_table, member_columns)
    top_limit = int(top_limit)
    return {
        "overview": base + """
, order_level AS (
  SELECT order_no, member_id, MIN(order_date) AS order_date, SUM(revenue) AS order_revenue,
         SUM(purchase_qty) AS order_qty, COUNT(DISTINCT product_code) AS sku_count
  FROM purchase_lines
  GROUP BY order_no, member_id
), member_level AS (
  SELECT member_id, COUNT(DISTINCT order_no) AS frequency_orders, SUM(order_revenue) AS member_revenue
  FROM order_level
  GROUP BY member_id
), line_summary AS (
  SELECT
    MIN(order_date) AS min_order_date,
    MAX(order_date) AS max_order_date,
    COUNT(*) AS line_rows,
    COUNT(DISTINCT order_no) AS orders,
    COUNT(DISTINCT member_id) AS buyers,
    COUNT(DISTINCT product_code) AS products,
    SUM(purchase_qty) AS quantity,
    SUM(revenue) AS revenue,
    SAFE_DIVIDE(SUM(revenue), COUNT(DISTINCT order_no)) AS aov_per_order,
    SAFE_DIVIDE(SUM(revenue), COUNT(DISTINCT member_id)) AS revenue_per_buyer,
    SAFE_DIVIDE(SUM(purchase_qty), COUNT(DISTINCT order_no)) AS qty_per_order,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT order_no)) AS line_per_order
  FROM purchase_lines
), member_summary AS (
  SELECT
    SAFE_DIVIDE(COUNTIF(frequency_orders >= 2), COUNT(*)) AS repeat_buyer_rate,
    SAFE_DIVIDE(SUM(CASE WHEN frequency_orders >= 2 THEN member_revenue ELSE 0 END), SUM(member_revenue)) AS repeat_buyer_revenue_share
  FROM member_level
)
SELECT * FROM line_summary CROSS JOIN member_summary
""",
        "daily": base + """
SELECT order_date, COUNT(DISTINCT order_no) AS orders, COUNT(DISTINCT member_id) AS buyers,
       COUNT(DISTINCT product_code) AS products, SUM(purchase_qty) AS quantity, SUM(revenue) AS revenue,
       SAFE_DIVIDE(SUM(revenue), COUNT(DISTINCT order_no)) AS aov,
       COUNT(DISTINCT CASE WHEN is_coupon_order = 1 THEN order_no END) AS coupon_orders
FROM purchase_lines
GROUP BY order_date
ORDER BY order_date
""",
        "monthly": base + """
SELECT year, month, COUNT(DISTINCT order_no) AS orders, COUNT(DISTINCT member_id) AS buyers,
       COUNT(DISTINCT product_code) AS products, SUM(purchase_qty) AS quantity, SUM(revenue) AS revenue,
       SAFE_DIVIDE(SUM(revenue), COUNT(DISTINCT order_no)) AS aov,
       SAFE_DIVIDE(SUM(purchase_qty), COUNT(DISTINCT order_no)) AS qty_per_order,
       COUNT(DISTINCT CASE WHEN is_coupon_order = 1 THEN order_no END) AS coupon_orders,
       SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN is_coupon_order = 1 THEN order_no END), COUNT(DISTINCT order_no)) AS coupon_order_rate
FROM purchase_lines
GROUP BY year, month
ORDER BY month
""",
        "weekday_hour": base + """
SELECT dow_num, weekday, order_hour, COUNT(DISTINCT order_no) AS orders, COUNT(DISTINCT member_id) AS buyers,
       SUM(purchase_qty) AS quantity, SUM(revenue) AS revenue,
       SAFE_DIVIDE(SUM(revenue), COUNT(DISTINCT order_no)) AS aov
FROM purchase_lines
GROUP BY dow_num, weekday, order_hour
ORDER BY dow_num, order_hour
""",
        "top_products": base + f"""
SELECT product_code, ANY_VALUE(product_name_kor) AS product_name_kor, ANY_VALUE(product_name) AS product_name,
       ANY_VALUE(product_style) AS product_style, ANY_VALUE(sex_label) AS sex_label,
       ANY_VALUE(product_year) AS product_year, ANY_VALUE(product_season) AS product_season,
       ANY_VALUE(source_image_url) AS source_image_url,
       COUNT(DISTINCT order_no) AS orders, COUNT(DISTINCT member_id) AS buyers,
       SUM(purchase_qty) AS quantity, SUM(revenue) AS revenue,
       SAFE_DIVIDE(SUM(revenue), SUM(purchase_qty)) AS unit_revenue,
       SAFE_DIVIDE(SUM(revenue), COUNT(DISTINCT order_no)) AS revenue_per_order
FROM purchase_lines
GROUP BY product_code
ORDER BY revenue DESC
LIMIT {top_limit}
""",
        "top_categories": base + """
SELECT COALESCE(NULLIF(category_depth2,''), NULLIF(category_title_kr,''), NULLIF(category_title,''), NULLIF(category_code,''), 'UNKNOWN') AS category,
       COUNT(DISTINCT order_no) AS orders, COUNT(DISTINCT member_id) AS buyers,
       COUNT(DISTINCT product_code) AS products, SUM(purchase_qty) AS quantity, SUM(revenue) AS revenue,
       SAFE_DIVIDE(SUM(revenue), COUNT(DISTINCT order_no)) AS aov
FROM purchase_lines
GROUP BY category
ORDER BY revenue DESC
LIMIT 300
""",
        "member_segments": base + """
, order_level AS (
  SELECT member_id, order_no, MIN(order_date) AS order_date, SUM(revenue) AS order_revenue
  FROM purchase_lines GROUP BY member_id, order_no
), member_level AS (
  SELECT member_id, COUNT(DISTINCT order_no) AS frequency_orders, SUM(order_revenue) AS monetary_revenue,
         DATE_DIFF(CURRENT_DATE('Asia/Seoul'), MAX(order_date), DAY) AS recency_days
  FROM order_level GROUP BY member_id
), segmented AS (
  SELECT *,
    CASE WHEN frequency_orders = 1 THEN '구매 1회'
         WHEN frequency_orders BETWEEN 2 AND 3 THEN '반복 2~3회'
         WHEN frequency_orders BETWEEN 4 AND 6 THEN '로열 4~6회'
         ELSE 'VIP 7회 이상' END AS frequency_segment,
    CASE WHEN recency_days <= 30 THEN '최근 30일'
         WHEN recency_days <= 90 THEN '최근 90일'
         WHEN recency_days <= 180 THEN '최근 180일'
         ELSE '180일 초과' END AS recency_segment
  FROM member_level
)
SELECT frequency_segment, recency_segment, COUNT(*) AS buyers, SUM(frequency_orders) AS orders,
       SUM(monetary_revenue) AS revenue,
       SAFE_DIVIDE(SUM(monetary_revenue), COUNT(*)) AS revenue_per_buyer,
       SAFE_DIVIDE(SUM(frequency_orders), COUNT(*)) AS orders_per_buyer
FROM segmented
GROUP BY frequency_segment, recency_segment
ORDER BY revenue DESC
""",
        "basket_size": base + """
, order_level AS (
  SELECT order_no, member_id, SUM(revenue) AS order_revenue, SUM(purchase_qty) AS order_qty,
         COUNT(DISTINCT product_code) AS sku_count, COUNT(*) AS line_count
  FROM purchase_lines GROUP BY order_no, member_id
)
SELECT
  CASE WHEN sku_count = 1 THEN '1 SKU' WHEN sku_count = 2 THEN '2 SKU'
       WHEN sku_count = 3 THEN '3 SKU' ELSE '4 SKU 이상' END AS sku_bucket,
  COUNT(DISTINCT order_no) AS orders, COUNT(DISTINCT member_id) AS buyers,
  SUM(order_qty) AS quantity, SUM(order_revenue) AS revenue,
  SAFE_DIVIDE(SUM(order_revenue), COUNT(DISTINCT order_no)) AS aov
FROM order_level
GROUP BY sku_bucket
ORDER BY CASE sku_bucket WHEN '1 SKU' THEN 1 WHEN '2 SKU' THEN 2 WHEN '3 SKU' THEN 3 ELSE 4 END
""",
        "purchase_interval": base + """
, order_level AS (
  SELECT member_id, order_no, MIN(order_date) AS order_date, SUM(revenue) AS order_revenue
  FROM purchase_lines GROUP BY member_id, order_no
), ordered AS (
  SELECT *, LAG(order_date) OVER(PARTITION BY member_id ORDER BY order_date, order_no) AS prev_order_date
  FROM order_level
), intervals AS (
  SELECT member_id, DATE_DIFF(order_date, prev_order_date, DAY) AS days_since_prev_order, order_revenue
  FROM ordered WHERE prev_order_date IS NOT NULL
)
SELECT
  CASE WHEN days_since_prev_order <= 7 THEN '7일 이내'
       WHEN days_since_prev_order <= 30 THEN '8~30일'
       WHEN days_since_prev_order <= 90 THEN '31~90일'
       WHEN days_since_prev_order <= 180 THEN '91~180일'
       ELSE '181일 이상' END AS interval_bucket,
  COUNT(*) AS repeat_orders, COUNT(DISTINCT member_id) AS repeat_buyers,
  AVG(days_since_prev_order) AS avg_days_since_prev_order,
  APPROX_QUANTILES(days_since_prev_order, 100)[OFFSET(50)] AS median_days_since_prev_order,
  SUM(order_revenue) AS revenue
FROM intervals
GROUP BY interval_bucket
ORDER BY MIN(days_since_prev_order)
""",
        "coupon_promo": base + """
SELECT CASE WHEN is_coupon_order = 1 THEN '쿠폰 사용' ELSE '쿠폰 미사용' END AS coupon_flag,
       COALESCE(NULLIF(coupon_name, ''), CASE WHEN is_coupon_order = 1 THEN '쿠폰명 미확인' ELSE '쿠폰 미사용' END) AS coupon_name,
       CASE WHEN is_point_used_order = 1 THEN '포인트 사용' ELSE '포인트 미사용' END AS point_flag,
       CASE WHEN is_promotion_line = 1 THEN '프로모션 상품' ELSE '일반 상품' END AS promotion_flag,
       COUNT(DISTINCT order_no) AS orders, COUNT(DISTINCT member_id) AS buyers,
       SUM(purchase_qty) AS quantity, SUM(revenue) AS revenue,
       SUM(coupon_amount) AS coupon_amount, SUM(mileage_amount) AS mileage_amount,
       SUM(promotion_amount) AS promotion_amount,
       SAFE_DIVIDE(SUM(revenue), COUNT(DISTINCT order_no)) AS aov
FROM purchase_lines
GROUP BY coupon_flag, coupon_name, point_flag, promotion_flag
ORDER BY revenue DESC
""",
        "size_color": base + """
SELECT product_code, ANY_VALUE(product_name_kor) AS product_name_kor,
       COALESCE(product_size, 'UNKNOWN') AS product_size,
       COALESCE(product_color, master_product_color, 'UNKNOWN') AS product_color,
       COUNT(DISTINCT order_no) AS orders, COUNT(DISTINCT member_id) AS buyers,
       SUM(purchase_qty) AS quantity, SUM(revenue) AS revenue
FROM purchase_lines
GROUP BY product_code, product_size, product_color
ORDER BY revenue DESC
LIMIT 1000
""",
        "grade_overview": base + """
, order_level AS (
  SELECT member_grade_label, member_id, order_no, MIN(order_date) AS order_date,
         SUM(revenue) AS order_revenue, SUM(purchase_qty) AS order_qty,
         MAX(is_coupon_order) AS has_coupon, MAX(is_point_used_order) AS has_point, MAX(is_promotion_line) AS has_promo
  FROM purchase_lines
  GROUP BY member_grade_label, member_id, order_no
), member_level AS (
  SELECT member_grade_label, member_id, COUNT(DISTINCT order_no) AS frequency_orders, SUM(order_revenue) AS member_revenue
  FROM order_level
  GROUP BY member_grade_label, member_id
)
SELECT
  o.member_grade_label,
  COUNT(DISTINCT o.member_id) AS buyers,
  COUNT(DISTINCT o.order_no) AS orders,
  SUM(o.order_qty) AS quantity,
  SUM(o.order_revenue) AS revenue,
  SAFE_DIVIDE(SUM(o.order_revenue), COUNT(DISTINCT o.order_no)) AS aov,
  SAFE_DIVIDE(SUM(o.order_revenue), COUNT(DISTINCT o.member_id)) AS revenue_per_buyer,
  COUNT(DISTINCT CASE WHEN m.frequency_orders >= 2 THEN m.member_id END) AS repeat_buyers,
  SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN m.frequency_orders >= 2 THEN m.member_id END), COUNT(DISTINCT o.member_id)) AS repeat_rate,
  SUM(CASE WHEN m.frequency_orders >= 2 THEN o.order_revenue ELSE 0 END) AS repeat_revenue,
  SAFE_DIVIDE(SUM(CASE WHEN m.frequency_orders >= 2 THEN o.order_revenue ELSE 0 END), SUM(o.order_revenue)) AS repeat_revenue_share,
  SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN o.has_coupon = 1 THEN o.order_no END), COUNT(DISTINCT o.order_no)) AS coupon_order_rate,
  SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN o.has_point = 1 THEN o.order_no END), COUNT(DISTINCT o.order_no)) AS point_order_rate,
  SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN o.has_promo = 1 THEN o.order_no END), COUNT(DISTINCT o.order_no)) AS promo_order_rate
FROM order_level o
LEFT JOIN member_level m ON o.member_grade_label = m.member_grade_label AND o.member_id = m.member_id
GROUP BY o.member_grade_label
ORDER BY revenue DESC
""",
        "grade_monthly": base + """
SELECT member_grade_label, month, COUNT(DISTINCT order_no) AS orders, COUNT(DISTINCT member_id) AS buyers,
       SUM(purchase_qty) AS quantity, SUM(revenue) AS revenue,
       SAFE_DIVIDE(SUM(revenue), COUNT(DISTINCT order_no)) AS aov
FROM purchase_lines
GROUP BY member_grade_label, month
ORDER BY member_grade_label, month
""",
        "grade_top_products": base + """
SELECT member_grade_label, product_code, ANY_VALUE(product_name_kor) AS product_name_kor, ANY_VALUE(product_name) AS product_name,
       ANY_VALUE(source_image_url) AS source_image_url,
       COUNT(DISTINCT order_no) AS orders, COUNT(DISTINCT member_id) AS buyers,
       SUM(purchase_qty) AS quantity, SUM(revenue) AS revenue,
       ROW_NUMBER() OVER(PARTITION BY member_grade_label ORDER BY SUM(revenue) DESC) AS rank_in_grade
FROM purchase_lines
GROUP BY member_grade_label, product_code
QUALIFY rank_in_grade <= 10
ORDER BY member_grade_label, rank_in_grade
""",
        "grade_categories": base + """
SELECT member_grade_label, COALESCE(NULLIF(category_depth2,''), NULLIF(category_title_kr,''), NULLIF(category_title,''), NULLIF(category_code,''), 'UNKNOWN') AS category,
       COUNT(DISTINCT order_no) AS orders, COUNT(DISTINCT member_id) AS buyers,
       COUNT(DISTINCT product_code) AS products, SUM(purchase_qty) AS quantity, SUM(revenue) AS revenue,
       ROW_NUMBER() OVER(PARTITION BY member_grade_label ORDER BY SUM(revenue) DESC) AS rank_in_grade
FROM purchase_lines
GROUP BY member_grade_label, category
QUALIFY rank_in_grade <= 10
ORDER BY member_grade_label, rank_in_grade
""",
        "grade_coupon_promo": base + """
SELECT member_grade_label,
       CASE WHEN is_coupon_order = 1 THEN '쿠폰 사용' ELSE '쿠폰 미사용' END AS coupon_flag,
       COALESCE(NULLIF(coupon_name, ''), CASE WHEN is_coupon_order = 1 THEN '쿠폰명 미확인' ELSE '쿠폰 미사용' END) AS coupon_name,
       CASE WHEN is_point_used_order = 1 THEN '포인트 사용' ELSE '포인트 미사용' END AS point_flag,
       CASE WHEN is_promotion_line = 1 THEN '프로모션 상품' ELSE '일반 상품' END AS promotion_flag,
       COUNT(DISTINCT order_no) AS orders, COUNT(DISTINCT member_id) AS buyers,
       SUM(purchase_qty) AS quantity, SUM(revenue) AS revenue,
       SAFE_DIVIDE(SUM(revenue), COUNT(DISTINCT order_no)) AS aov
FROM purchase_lines
GROUP BY member_grade_label, coupon_flag, coupon_name, point_flag, promotion_flag
ORDER BY member_grade_label, revenue DESC
""",
        "grade_repeat_overview": base + """
, order_level AS (
  SELECT member_grade_label, member_id, order_no, MIN(order_date) AS order_date, SUM(revenue) AS order_revenue
  FROM purchase_lines GROUP BY member_grade_label, member_id, order_no
), ordered AS (
  SELECT *, ROW_NUMBER() OVER(PARTITION BY member_id ORDER BY order_date, order_no) AS order_rank,
            LAG(order_date) OVER(PARTITION BY member_id ORDER BY order_date, order_no) AS prev_order_date
  FROM order_level
)
SELECT member_grade_label,
       COUNT(DISTINCT CASE WHEN order_rank >= 2 THEN order_no END) AS repeat_orders,
       COUNT(DISTINCT CASE WHEN order_rank >= 2 THEN member_id END) AS repeat_buyers,
       SUM(CASE WHEN order_rank >= 2 THEN order_revenue ELSE 0 END) AS repeat_revenue,
       AVG(CASE WHEN order_rank >= 2 THEN DATE_DIFF(order_date, prev_order_date, DAY) END) AS avg_repeat_days,
       APPROX_QUANTILES(CASE WHEN order_rank >= 2 THEN DATE_DIFF(order_date, prev_order_date, DAY) END, 100 IGNORE NULLS)[OFFSET(50)] AS median_repeat_days
FROM ordered
GROUP BY member_grade_label
ORDER BY repeat_revenue DESC
""",
        "repeat_categories": base + """
, order_level AS (
  SELECT member_id, order_no, MIN(order_date) AS order_date
  FROM purchase_lines GROUP BY member_id, order_no
), ranked_orders AS (
  SELECT *, ROW_NUMBER() OVER(PARTITION BY member_id ORDER BY order_date, order_no) AS order_rank
  FROM order_level
)
SELECT COALESCE(NULLIF(p.category_depth2,''), NULLIF(p.category_title_kr,''), NULLIF(p.category_title,''), NULLIF(p.category_code,''), 'UNKNOWN') AS category,
       COUNT(DISTINCT p.order_no) AS repeat_orders, COUNT(DISTINCT p.member_id) AS repeat_buyers,
       SUM(p.purchase_qty) AS quantity, SUM(p.revenue) AS revenue
FROM purchase_lines p
JOIN ranked_orders r ON p.member_id = r.member_id AND p.order_no = r.order_no
WHERE r.order_rank >= 2
GROUP BY category
ORDER BY revenue DESC
LIMIT 50
""",
        "repeat_products": base + f"""
, order_level AS (
  SELECT member_id, order_no, MIN(order_date) AS order_date
  FROM purchase_lines GROUP BY member_id, order_no
), ranked_orders AS (
  SELECT *, ROW_NUMBER() OVER(PARTITION BY member_id ORDER BY order_date, order_no) AS order_rank
  FROM order_level
)
SELECT p.product_code, ANY_VALUE(p.product_name_kor) AS product_name_kor, ANY_VALUE(p.product_name) AS product_name,
       ANY_VALUE(p.source_image_url) AS source_image_url,
       COUNT(DISTINCT p.order_no) AS repeat_orders, COUNT(DISTINCT p.member_id) AS repeat_buyers,
       SUM(p.purchase_qty) AS quantity, SUM(p.revenue) AS revenue
FROM purchase_lines p
JOIN ranked_orders r ON p.member_id = r.member_id AND p.order_no = r.order_no
WHERE r.order_rank >= 2
GROUP BY p.product_code
ORDER BY revenue DESC
LIMIT {top_limit}
""",
        "grade_repeat_categories": base + """
, order_level AS (
  SELECT member_id, order_no, MIN(order_date) AS order_date
  FROM purchase_lines GROUP BY member_id, order_no
), ranked_orders AS (
  SELECT *, ROW_NUMBER() OVER(PARTITION BY member_id ORDER BY order_date, order_no) AS order_rank
  FROM order_level
)
SELECT p.member_grade_label, COALESCE(NULLIF(p.category_depth2,''), NULLIF(p.category_title_kr,''), NULLIF(p.category_title,''), NULLIF(p.category_code,''), 'UNKNOWN') AS category,
       COUNT(DISTINCT p.order_no) AS repeat_orders, COUNT(DISTINCT p.member_id) AS repeat_buyers,
       SUM(p.purchase_qty) AS quantity, SUM(p.revenue) AS revenue,
       ROW_NUMBER() OVER(PARTITION BY p.member_grade_label ORDER BY SUM(p.revenue) DESC) AS rank_in_grade
FROM purchase_lines p
JOIN ranked_orders r ON p.member_id = r.member_id AND p.order_no = r.order_no
WHERE r.order_rank >= 2
GROUP BY p.member_grade_label, category
QUALIFY rank_in_grade <= 8
ORDER BY p.member_grade_label, rank_in_grade
""",
        "grade_repeat_products": base + """
, order_level AS (
  SELECT member_id, order_no, MIN(order_date) AS order_date
  FROM purchase_lines GROUP BY member_id, order_no
), ranked_orders AS (
  SELECT *, ROW_NUMBER() OVER(PARTITION BY member_id ORDER BY order_date, order_no) AS order_rank
  FROM order_level
)
SELECT p.member_grade_label, p.product_code, ANY_VALUE(p.product_name_kor) AS product_name_kor, ANY_VALUE(p.product_name) AS product_name,
       ANY_VALUE(p.source_image_url) AS source_image_url,
       COUNT(DISTINCT p.order_no) AS repeat_orders, COUNT(DISTINCT p.member_id) AS repeat_buyers,
       SUM(p.purchase_qty) AS quantity, SUM(p.revenue) AS revenue,
       ROW_NUMBER() OVER(PARTITION BY p.member_grade_label ORDER BY SUM(p.revenue) DESC) AS rank_in_grade
FROM purchase_lines p
JOIN ranked_orders r ON p.member_id = r.member_id AND p.order_no = r.order_no
WHERE r.order_rank >= 2
GROUP BY p.member_grade_label, p.product_code
QUALIFY rank_in_grade <= 8
ORDER BY p.member_grade_label, rank_in_grade
""",
        "ui_daily_categories": base + """
SELECT order_date, COALESCE(NULLIF(category_depth2,''), NULLIF(category_title_kr,''), NULLIF(category_title,''), NULLIF(category_code,''), 'UNKNOWN') AS category,
       COUNT(DISTINCT order_no) AS orders, COUNT(DISTINCT member_id) AS buyers,
       COUNT(DISTINCT product_code) AS products, SUM(purchase_qty) AS quantity, SUM(revenue) AS revenue
FROM purchase_lines
GROUP BY order_date, category
ORDER BY order_date, revenue DESC
""",
        "ui_daily_products": base + """
SELECT order_date, product_code, ANY_VALUE(product_name_kor) AS product_name_kor, ANY_VALUE(product_name) AS product_name,
       ANY_VALUE(source_image_url) AS source_image_url,
       COUNT(DISTINCT order_no) AS orders, COUNT(DISTINCT member_id) AS buyers,
       SUM(purchase_qty) AS quantity, SUM(revenue) AS revenue
FROM purchase_lines
GROUP BY order_date, product_code
ORDER BY order_date, revenue DESC
""",
        "ui_daily_basket": base + """
, order_level AS (
  SELECT order_date, order_no, member_id, SUM(revenue) AS order_revenue, SUM(purchase_qty) AS order_qty,
         COUNT(DISTINCT product_code) AS sku_count
  FROM purchase_lines GROUP BY order_date, order_no, member_id
)
SELECT order_date,
  CASE WHEN sku_count = 1 THEN '1 SKU' WHEN sku_count = 2 THEN '2 SKU'
       WHEN sku_count = 3 THEN '3 SKU' ELSE '4 SKU 이상' END AS sku_bucket,
  COUNT(DISTINCT order_no) AS orders, COUNT(DISTINCT member_id) AS buyers,
  SUM(order_qty) AS quantity, SUM(order_revenue) AS revenue,
  SAFE_DIVIDE(SUM(order_revenue), COUNT(DISTINCT order_no)) AS aov
FROM order_level
GROUP BY order_date, sku_bucket
ORDER BY order_date, sku_bucket
""",
        "ui_daily_coupon": base + """
SELECT order_date,
       CASE WHEN is_coupon_order = 1 THEN '쿠폰 사용' ELSE '쿠폰 미사용' END AS coupon_flag,
       COALESCE(NULLIF(coupon_name, ''), CASE WHEN is_coupon_order = 1 THEN '쿠폰명 미확인' ELSE '쿠폰 미사용' END) AS coupon_name,
       COUNT(DISTINCT order_no) AS orders, COUNT(DISTINCT member_id) AS buyers,
       SUM(purchase_qty) AS quantity, SUM(revenue) AS revenue, SUM(coupon_amount) AS coupon_amount,
       SAFE_DIVIDE(SUM(revenue), COUNT(DISTINCT order_no)) AS aov
FROM purchase_lines
GROUP BY order_date, coupon_flag, coupon_name
ORDER BY order_date, revenue DESC
""",
        "ui_daily_grade": base + """
SELECT order_date, member_grade_label,
       COUNT(DISTINCT order_no) AS orders, COUNT(DISTINCT member_id) AS buyers,
       SUM(purchase_qty) AS quantity, SUM(revenue) AS revenue,
       SAFE_DIVIDE(SUM(revenue), COUNT(DISTINCT order_no)) AS aov
FROM purchase_lines
GROUP BY order_date, member_grade_label
ORDER BY order_date, revenue DESC
""",
        "ui_daily_repeat_categories": base + """
, order_level AS (
  SELECT member_id, order_no, MIN(order_date) AS order_date
  FROM purchase_lines GROUP BY member_id, order_no
), ranked_orders AS (
  SELECT *, ROW_NUMBER() OVER(PARTITION BY member_id ORDER BY order_date, order_no) AS order_rank
  FROM order_level
)
SELECT p.order_date, COALESCE(NULLIF(p.category_depth2,''), NULLIF(p.category_title_kr,''), NULLIF(p.category_title,''), NULLIF(p.category_code,''), 'UNKNOWN') AS category,
       COUNT(DISTINCT p.order_no) AS repeat_orders, COUNT(DISTINCT p.member_id) AS repeat_buyers,
       SUM(p.purchase_qty) AS quantity, SUM(p.revenue) AS revenue
FROM purchase_lines p
JOIN ranked_orders r ON p.member_id = r.member_id AND p.order_no = r.order_no
WHERE r.order_rank >= 2
GROUP BY p.order_date, category
ORDER BY p.order_date, revenue DESC
""",
        "ui_daily_grade_repeat_categories": base + """
, order_level AS (
  SELECT member_id, order_no, MIN(order_date) AS order_date
  FROM purchase_lines GROUP BY member_id, order_no
), ranked_orders AS (
  SELECT *, ROW_NUMBER() OVER(PARTITION BY member_id ORDER BY order_date, order_no) AS order_rank
  FROM order_level
)
SELECT p.order_date, p.member_grade_label, COALESCE(NULLIF(p.category_depth2,''), NULLIF(p.category_title_kr,''), NULLIF(p.category_title,''), NULLIF(p.category_code,''), 'UNKNOWN') AS category,
       COUNT(DISTINCT p.order_no) AS repeat_orders, COUNT(DISTINCT p.member_id) AS repeat_buyers,
       SUM(p.purchase_qty) AS quantity, SUM(p.revenue) AS revenue
FROM purchase_lines p
JOIN ranked_orders r ON p.member_id = r.member_id AND p.order_no = r.order_no
WHERE r.order_rank >= 2
GROUP BY p.order_date, p.member_grade_label, category
ORDER BY p.order_date, p.member_grade_label, revenue DESC
""",
        "ui_daily_grade_repeat_products": base + """
, order_level AS (
  SELECT member_id, order_no, MIN(order_date) AS order_date
  FROM purchase_lines GROUP BY member_id, order_no
), ranked_orders AS (
  SELECT *, ROW_NUMBER() OVER(PARTITION BY member_id ORDER BY order_date, order_no) AS order_rank
  FROM order_level
)
SELECT p.order_date, p.member_grade_label, p.product_code, ANY_VALUE(p.product_name_kor) AS product_name_kor, ANY_VALUE(p.product_name) AS product_name,
       ANY_VALUE(p.source_image_url) AS source_image_url,
       COUNT(DISTINCT p.order_no) AS repeat_orders, COUNT(DISTINCT p.member_id) AS repeat_buyers,
       SUM(p.purchase_qty) AS quantity, SUM(p.revenue) AS revenue
FROM purchase_lines p
JOIN ranked_orders r ON p.member_id = r.member_id AND p.order_no = r.order_no
WHERE r.order_rank >= 2
GROUP BY p.order_date, p.member_grade_label, p.product_code
ORDER BY p.order_date, p.member_grade_label, revenue DESC
""",
    }


def run_queries(client: bigquery.Client, queries: dict[str, str], location: str, start_date: str, end_date: str) -> dict[str, pd.DataFrame]:
    results: dict[str, pd.DataFrame] = {}
    cfg = query_config(start_date, end_date)
    for name, sql in queries.items():
        log(f"Running BigQuery: {name}")
        df = client.query(sql, job_config=cfg, location=location).to_dataframe()
        results[name] = df
        log(f"{name}: rows={len(df):,}, cols={len(df.columns):,}")
    return results


# =========================================================
# Product image helpers
# =========================================================

def normalize_sku(value: Any) -> str:
    s = str(value or "").strip()
    s = re.sub(r"\.0+$", "", s)
    return s.upper()


def normalize_image_url(url: str, base_url: str = OFFICIAL_MALL_BASE) -> str:
    u = str(url or "").strip()
    if not u or u.lower() in {"nan", "none", "null"}:
        return ""
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("/"):
        return urljoin(base_url, u)
    if u.lower().startswith("http"):
        return u
    return ""


def load_image_map_from_excel(path: str) -> dict[str, str]:
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        candidates = [BASE_DIR / path, Path("reports") / "daily_digest" / "data" / Path(path).name, Path("reports") / "daily_digest" / Path(path).name]
        p = next((x for x in candidates if x.exists()), p)
    if not p.exists():
        log(f"Image Excel not found: {path}")
        return {}

    m: dict[str, str] = {}
    try:
        raw = pd.read_excel(p, sheet_name=0, header=None)
        sku_idx = url_idx = None
        header_row = 0
        for i in range(min(80, raw.shape[0])):
            for j, v in enumerate(raw.iloc[i].tolist()):
                sv = str(v or "").strip()
                low = sv.lower()
                if sku_idx is None and ("상품코드" in sv or "품번" in sv or low in {"sku", "itemid", "item_id", "product_code", "productcode"}):
                    sku_idx = j
                if url_idx is None and (("이미지" in sv and ("링크" in sv or "url" in low)) or "image_url" in low or "thumbnail" in low):
                    url_idx = j
            if sku_idx is not None and url_idx is not None:
                header_row = i
                break
        if sku_idx is None:
            sku_idx = 0
        if url_idx is None:
            # Fallback: first column that looks like URL in first 100 rows
            for j in range(raw.shape[1]):
                sample = raw.iloc[: min(100, raw.shape[0]), j].astype(str).str.contains("http", case=False, na=False).sum()
                if sample >= 2:
                    url_idx = j
                    break
        if url_idx is None:
            url_idx = 1
        for r in range(header_row + 1, raw.shape[0]):
            sku = normalize_sku(raw.iat[r, sku_idx]) if sku_idx < raw.shape[1] else ""
            url = normalize_image_url(str(raw.iat[r, url_idx]).strip() if url_idx < raw.shape[1] else "")
            if sku and url:
                m[sku] = url
        log(f"Loaded image map: {len(m):,} rows from {p}")
    except Exception as e:
        log(f"Image Excel parse failed: {type(e).__name__}: {e}")
    return m


def candidate_product_urls(sku: str, name: str = "") -> list[str]:
    sku_q = quote_plus(sku)
    name_q = quote_plus(name) if name else sku_q
    return [
        f"{OFFICIAL_MALL_BASE}/product/{sku}",
        f"{OFFICIAL_MALL_BASE}/Product/{sku}",
        f"{OFFICIAL_MALL_BASE}/goods/goods_view.php?goodsNo={sku_q}",
        f"{OFFICIAL_MALL_BASE}/shop/goods/goods_view.php?goodsNo={sku_q}",
        f"{OFFICIAL_MALL_BASE}/search?keyword={sku_q}",
        f"{OFFICIAL_MALL_BASE}/product/search?keyword={sku_q}",
        f"{OFFICIAL_MALL_BASE}/search?keyword={name_q}",
    ]


def is_productish_image(url: str) -> bool:
    u = normalize_image_url(url).lower()
    if not u.startswith("http"):
        return False
    bad = ["logo", "sprite", "icon", "blank", "loading", "placeholder", "favicon", "noimage"]
    return not any(x in u for x in bad)


def extract_image_from_html(html: str, base_url: str, sku: str = "", name: str = "") -> str:
    text = html or ""
    soup = BeautifulSoup(text, "html.parser")
    sku_low = sku.lower()
    name_low = (name or "").lower()[:20]

    # 1) meta images
    for sel in ["meta[property='og:image']", "meta[name='twitter:image']", "meta[property='og:image:secure_url']"]:
        tag = soup.select_one(sel)
        if tag and tag.get("content"):
            url = normalize_image_url(urljoin(base_url, tag.get("content", "").strip()), base_url)
            if is_productish_image(url):
                return url

    # 2) JSON/script URL extraction, prefer URLs containing SKU
    url_candidates = re.findall(r"https?:\\/\\/[^'\"\\]+|https?://[^'\"\s<>]+|//[^'\"\s<>]+|/[^'\"\s<>]+\.(?:jpg|jpeg|png|webp)(?:\?[^'\"\s<>]*)?", text, flags=re.I)
    cleaned: list[str] = []
    for raw in url_candidates:
        u = raw.replace("\\/", "/")
        u = normalize_image_url(urljoin(base_url, u), base_url)
        if is_productish_image(u):
            cleaned.append(u)
    if sku_low:
        for u in cleaned:
            if sku_low in u.lower():
                return u

    # 3) img tags, prefer src/alt/title containing SKU or product name
    imgs = []
    for img in soup.select("img"):
        meta = " ".join([str(img.get(x, "")) for x in ["alt", "title", "data-product", "data-code"]]).lower()
        for attr in ["data-src", "data-original", "data-lazy", "data-image", "srcset", "src"]:
            val = img.get(attr)
            if not val:
                continue
            val = str(val).split(",")[0].strip().split(" ")[0]
            url = normalize_image_url(urljoin(base_url, val), base_url)
            if is_productish_image(url):
                score = 0
                if sku_low and (sku_low in url.lower() or sku_low in meta):
                    score += 10
                if name_low and (name_low in meta):
                    score += 3
                imgs.append((score, url))
    if imgs:
        imgs.sort(key=lambda x: x[0], reverse=True)
        return imgs[0][1]
    if cleaned:
        return cleaned[0]
    return ""


def crawl_product_image(sku: str, name: str, session: requests.Session, sleep_sec: float = 0.15) -> tuple[str, str]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    for url in candidate_product_urls(sku, name):
        try:
            resp = session.get(url, headers=headers, timeout=12, allow_redirects=True)
            if resp.status_code >= 400 or not resp.text:
                continue
            img = extract_image_from_html(resp.text, resp.url or url, sku=sku, name=name)
            if img:
                time.sleep(max(sleep_sec, 0))
                return img, resp.url or url
        except Exception:
            continue
    return "", ""


def download_image(url: str, out_dir: Path, sku: str) -> str:
    url = normalize_image_url(url)
    if not url:
        return ""
    out_dir.mkdir(parents=True, exist_ok=True)
    ext = Path(url.split("?", 1)[0]).suffix.lower()
    if ext not in {".jpg", ".jpeg", ".png", ".webp"}:
        ext = ".jpg"
    safe_sku = re.sub(r"[^A-Za-z0-9_-]+", "_", sku)[:80] or hashlib.md5(url.encode()).hexdigest()[:12]
    out_path = out_dir / f"{safe_sku}{ext}"
    if out_path.exists() and out_path.stat().st_size > 0:
        return str(out_path)
    try:
        resp = requests.get(url, timeout=18, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        ctype = resp.headers.get("content-type", "").lower()
        if "image" not in ctype and len(resp.content) < 2048:
            return ""
        out_path.write_bytes(resp.content)
        return str(out_path)
    except Exception:
        return ""


def attach_product_images(
    top_products: pd.DataFrame,
    image_xlsx: str,
    out_dir: Path,
    crawl_images: bool,
    download_images: bool,
    max_crawl: int,
    placeholder_img: str,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    if top_products is None or top_products.empty:
        return pd.DataFrame(), []
    df = top_products.copy()
    image_map = load_image_map_from_excel(image_xlsx)
    session = requests.Session()
    log_rows: list[dict[str, Any]] = []
    final_urls: list[str] = []
    sources: list[str] = []
    crawled = 0
    local_img_dir = out_dir / "assets" / "product_images"

    for _, row in df.iterrows():
        sku = normalize_sku(row.get("product_code"))
        name = str(row.get("product_name_kor") or row.get("product_name") or sku)
        source_url = normalize_image_url(str(row.get("source_image_url") or ""))
        image_url = ""
        image_source = ""
        crawled_from = ""

        if sku in image_map:
            image_url = image_map[sku]
            image_source = "excel_map"
        elif source_url:
            image_url = source_url
            image_source = "bigquery_column"
        elif crawl_images and crawled < max_crawl:
            image_url, crawled_from = crawl_product_image(sku, name, session)
            crawled += 1
            image_source = "crawled" if image_url else "not_found"
        else:
            image_source = "skipped"

        local_path = ""
        if download_images and image_url:
            local_path = download_image(image_url, local_img_dir, sku)
            if local_path:
                image_url_for_html = Path(local_path).relative_to(out_dir).as_posix()
                image_source += "+downloaded"
            else:
                image_url_for_html = image_url
        else:
            image_url_for_html = image_url

        if not image_url_for_html and placeholder_img:
            image_url_for_html = placeholder_img
            image_source = "placeholder"

        final_urls.append(image_url_for_html)
        sources.append(image_source)
        log_rows.append({
            "product_code": sku,
            "product_name": name,
            "image_url": image_url,
            "html_image_url": image_url_for_html,
            "image_source": image_source,
            "crawled_from": crawled_from,
            "local_path": local_path,
        })

    df["image_url"] = final_urls
    df["image_source"] = sources
    return df, log_rows


# =========================================================
# Insights and HTML helpers
# =========================================================

def safe_num(v: Any) -> float:
    try:
        if pd.isna(v):
            return 0.0
        return float(v)
    except Exception:
        return 0.0


def insight_summary(results: dict[str, pd.DataFrame]) -> list[str]:
    lines: list[str] = []
    overview = results.get("overview", pd.DataFrame())
    monthly = results.get("monthly", pd.DataFrame())
    top_products = results.get("top_products", pd.DataFrame())
    segments = results.get("member_segments", pd.DataFrame())
    grade = results.get("grade_overview", pd.DataFrame())
    repeat_cat = results.get("repeat_categories", pd.DataFrame())
    repeat_prod = results.get("repeat_products", pd.DataFrame())

    if not overview.empty:
        r = overview.iloc[0]
        lines.append(f"분석 기간은 {r.get('min_order_date')}~{r.get('max_order_date')}이며, 총 매출 {fmt_krw(r.get('revenue'))}, 주문 {fmt_int(r.get('orders'))}건, 구매자 {fmt_int(r.get('buyers'))}명입니다.")
        lines.append(f"객단가는 {fmt_krw(r.get('aov_per_order'))}, 구매자당 매출은 {fmt_krw(r.get('revenue_per_buyer'))}, 재구매자 매출 비중은 {fmt_pct(r.get('repeat_buyer_revenue_share'))}입니다.")
    if not monthly.empty:
        best = monthly.sort_values("revenue", ascending=False).iloc[0]
        has_2026 = bool((monthly["month"].astype(str).str.startswith("2026")).any()) if "month" in monthly.columns else False
        lines.append(f"월별 최고 매출 월은 {best.get('month')}이며 매출 {fmt_krw(best.get('revenue'))}, 주문 {fmt_int(best.get('orders'))}건입니다. 2026년 데이터 포함 여부: {'포함' if has_2026 else '미포함'}.")
    if not top_products.empty:
        p = top_products.sort_values("revenue", ascending=False).iloc[0]
        pname = p.get("product_name_kor") or p.get("product_name") or p.get("product_code")
        lines.append(f"매출 1위 상품은 {pname}({p.get('product_code')})로, 매출 {fmt_krw(p.get('revenue'))}, 판매수량 {fmt_int(p.get('quantity'))}개입니다.")
    if not grade.empty:
        g = grade.sort_values("revenue", ascending=False).iloc[0]
        lines.append(f"등급별로는 {g.get('member_grade_label')} 집단의 매출 비중이 가장 크며, 재구매율은 {fmt_pct(g.get('repeat_rate'))}, 객단가는 {fmt_krw(g.get('aov'))}입니다.")
    if not repeat_cat.empty:
        c = repeat_cat.sort_values("revenue", ascending=False).iloc[0]
        lines.append(f"재구매 시 가장 많이 매출을 만든 카테고리는 {c.get('category')}이며, 재구매 매출 {fmt_krw(c.get('revenue'))}입니다.")
    if not repeat_prod.empty:
        p = repeat_prod.sort_values("revenue", ascending=False).iloc[0]
        pname = p.get("product_name_kor") or p.get("product_name") or p.get("product_code")
        lines.append(f"재구매 상품 TOP은 {pname}({p.get('product_code')})이며, 재구매 매출 {fmt_krw(p.get('revenue'))}입니다.")
    if not segments.empty:
        s = segments.sort_values("revenue", ascending=False).iloc[0]
        lines.append(f"회원 세그먼트는 {s.get('frequency_segment')} × {s.get('recency_segment')} 조합이 가장 큰 매출을 만들고 있습니다.")
    return lines[:7]


def build_group_characteristics(results: dict[str, pd.DataFrame]) -> list[dict[str, str]]:
    grade = results.get("grade_overview", pd.DataFrame())
    grade_cat = results.get("grade_categories", pd.DataFrame())
    grade_prod = results.get("grade_top_products", pd.DataFrame())
    repeat_grade = results.get("grade_repeat_overview", pd.DataFrame())
    out: list[dict[str, str]] = []
    if grade is None or grade.empty:
        return out
    for _, g in grade.sort_values("revenue", ascending=False).head(8).iterrows():
        label = str(g.get("member_grade_label") or "UNKNOWN")
        cats = grade_cat[grade_cat["member_grade_label"].astype(str) == label] if grade_cat is not None and not grade_cat.empty else pd.DataFrame()
        prods = grade_prod[grade_prod["member_grade_label"].astype(str) == label] if grade_prod is not None and not grade_prod.empty else pd.DataFrame()
        rep = repeat_grade[repeat_grade["member_grade_label"].astype(str) == label] if repeat_grade is not None and not repeat_grade.empty else pd.DataFrame()
        top_cat = str(cats.iloc[0].get("category")) if not cats.empty else "-"
        top_prod = str(prods.iloc[0].get("product_name_kor") or prods.iloc[0].get("product_name") or prods.iloc[0].get("product_code")) if not prods.empty else "-"
        repeat_days = fmt_int(rep.iloc[0].get("median_repeat_days")) if not rep.empty else "-"
        out.append({
            "grade": label,
            "title": f"{label} 집단 특징",
            "body": f"매출은 {fmt_krw(g.get('revenue'))}, 주문은 {fmt_int(g.get('orders'))}건, 구매자는 {fmt_int(g.get('buyers'))}명입니다. 객단가는 {fmt_krw(g.get('aov'))}, 구매자당 매출은 {fmt_krw(g.get('revenue_per_buyer'))}이며, 재구매율은 {fmt_pct(g.get('repeat_rate'))}, 재구매 매출 비중은 {fmt_pct(g.get('repeat_revenue_share'))}입니다. 선호 2뎁스 카테고리는 {top_cat}, 대표 상품은 {top_prod}이고, 재구매 중앙 간격은 {repeat_days}일입니다. 쿠폰 주문 비중은 {fmt_pct(g.get('coupon_order_rate'))}, 포인트 사용 주문 비중은 {fmt_pct(g.get('point_order_rate'))}로 확인됩니다.",
            "action": "",
        })
    return out


def metric_card(label: str, value: str, sub: str = "", id_prefix: str = "") -> str:
    vid = f" id='{id_prefix}-value'" if id_prefix else ""
    sid = f" id='{id_prefix}-sub'" if id_prefix else ""
    return f"""
    <div class="kpi-card rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm">
      <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">{escape(label)}</div>
      <div{vid} class="kpi-value mt-3 text-3xl font-black text-slate-950">{escape(value)}</div>
      <div{sid} class="mt-2 text-sm font-semibold text-slate-500">{escape(sub)}</div>
    </div>
    """


def bar_list(df: pd.DataFrame, label_col: str, value_col: str, fmt: str = "krw", limit: Optional[int] = 10, soft: bool = False) -> str:
    if df is None or df.empty or value_col not in df.columns:
        return "<div class='p-6 text-sm font-bold text-slate-400'>데이터 없음</div>"
    d = df.copy()
    if limit:
        d = d.head(limit)
    max_v = float(pd.to_numeric(d[value_col], errors="coerce").fillna(0).max() or 1)
    rows = []
    for _, r in d.iterrows():
        val = safe_num(r.get(value_col))
        pct = min(max(val / max_v * 100, 0), 100)
        txt = fmt_krw(val) if fmt == "krw" else (fmt_pct(val) if fmt == "pct" else fmt_int(val))
        label = str(r.get(label_col, ""))
        rows.append(f"""
        <div class="py-2">
          <div class="mb-1 flex items-center justify-between gap-3">
            <div class="truncate text-sm font-black text-slate-700" title="{escape(label)}">{escape(label)}</div>
            <div class="whitespace-nowrap text-sm font-black text-slate-900">{escape(txt)}</div>
          </div>
          <div class="bar-track"><div class="{'bar-fill-soft' if soft else 'bar-fill'}" style="width:{pct:.1f}%"></div></div>
        </div>
        """)
    return "".join(rows)


def mini_visual_cards(df: pd.DataFrame, label_col: str, value_col: str, sub_cols: list[tuple[str, str, str]], limit: int = 8, fmt: str = "krw") -> str:
    if df is None or df.empty:
        return "<div class='p-6 text-sm font-bold text-slate-400'>데이터 없음</div>"
    cards = []
    max_v = safe_num(pd.to_numeric(df[value_col], errors="coerce").fillna(0).max()) or 1
    for _, r in df.head(limit).iterrows():
        label = str(r.get(label_col, ""))
        val = safe_num(r.get(value_col))
        pct = min(max(val / max_v * 100, 0), 100)
        value_txt = fmt_krw(val) if fmt == "krw" else fmt_int(val)
        subs = []
        for col, lab, typ in sub_cols:
            x = r.get(col, "")
            if typ == "krw": txt = fmt_krw(x)
            elif typ == "pct": txt = fmt_pct(x)
            else: txt = fmt_int(x)
            subs.append(f"<div class='rounded-xl bg-slate-50 p-2'><div class='text-[10px] font-black text-slate-400'>{escape(lab)}</div><div class='text-sm font-black text-slate-800'>{escape(txt)}</div></div>")
        cards.append(f"""
        <div class="viz-card rounded-2xl border border-slate-200 bg-white/80 p-4 shadow-sm">
          <div class="flex items-start justify-between gap-3">
            <div class="min-w-0">
              <div class="truncate text-base font-black text-slate-900" title="{escape(label)}">{escape(label)}</div>
              <div class="mt-1 text-xl font-black text-blue-700">{escape(value_txt)}</div>
            </div>
            <div class="rounded-full bg-slate-900 px-2 py-1 text-[10px] font-black text-white">{pct:.0f}%</div>
          </div>
          <div class="mt-3 bar-track"><div class="bar-fill" style="width:{pct:.1f}%"></div></div>
          <div class="mt-3 grid grid-cols-2 gap-2">{''.join(subs)}</div>
        </div>
        """)
    return "<div class='grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-4'>" + "".join(cards) + "</div>"


def product_cards(df: pd.DataFrame, limit: int = 12, repeat: bool = False) -> str:
    if df is None or df.empty:
        return "<div class='p-6 text-sm font-bold text-slate-400'>상품 데이터 없음</div>"
    cards = []
    for idx, (_, r) in enumerate(df.head(limit).iterrows(), start=1):
        sku = str(r.get("product_code", ""))
        name = str(r.get("product_name_kor") or r.get("product_name") or sku)
        img = str(r.get("image_url") or "")
        img_html = f"<img src='{escape(img)}' alt='{escape(name)}' class='product-img w-full rounded-xl border border-slate-100' loading='lazy' onerror=\"this.outerHTML='<div class=&quot;product-img flex w-full items-center justify-center rounded-xl border border-slate-100 text-xs font-black text-slate-300&quot;>NO IMAGE</div>'\"/>" if img else "<div class='product-img flex w-full items-center justify-center rounded-xl border border-slate-100 text-xs font-black text-slate-300'>NO IMAGE</div>"
        main_label = "재구매 매출" if repeat else "매출"
        qty_label = "재구매 수량" if repeat else "수량"
        revenue_col = "revenue"
        qty_col = "quantity"
        cards.append(f"""
        <div class="product-card rounded-2xl border border-slate-200 bg-white/80 p-3 shadow-sm">
          <div class="relative">
            {img_html}
            <div class="absolute left-2 top-2 rounded-full bg-slate-950 px-2 py-1 text-xs font-black text-white">#{idx}</div>
          </div>
          <div class="mt-3 line-clamp-2 min-h-[40px] text-sm font-black text-slate-900" title="{escape(name)}">{escape(name)}</div>
          <div class="mt-1 truncate text-xs font-extrabold text-slate-400">{escape(sku)}</div>
          <div class="mt-3 grid grid-cols-2 gap-2 text-xs">
            <div class="rounded-xl bg-slate-50 p-2"><div class="text-slate-400 font-black">{main_label}</div><div class="font-black text-slate-900">{fmt_krw(r.get(revenue_col))}</div></div>
            <div class="rounded-xl bg-slate-50 p-2"><div class="text-slate-400 font-black">{qty_label}</div><div class="font-black text-slate-900">{fmt_int(r.get(qty_col))}</div></div>
          </div>
          <div class="mt-2 text-[10px] font-bold text-slate-400">이미지: {escape(str(r.get('image_source') or ''))}</div>
        </div>
        """)
    return "".join(cards)


def heatmap_weekday_hour(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "<div class='p-6 text-sm font-bold text-slate-400'>데이터 없음</div>"
    d = df.copy()
    d["revenue"] = pd.to_numeric(d["revenue"], errors="coerce").fillna(0)
    max_v = float(d["revenue"].max() or 1)
    weekdays = ["월", "화", "수", "목", "금", "토", "일"]
    hours = list(range(0, 24, 3))
    cells = []
    for wd in weekdays:
        row = [f"<div class='text-xs font-black text-slate-500'>{wd}</div>"]
        for h in hours:
            bucket = d[(d["weekday"].astype(str) == wd) & (pd.to_numeric(d["order_hour"], errors="coerce").fillna(-1).between(h, h+2))]
            val = float(bucket["revenue"].sum())
            opacity = 0.08 + min(val / max_v, 1) * 0.72
            row.append(f"<div class='heat-cell px-2 py-3 text-center text-[10px] font-black text-slate-700' style='background:rgba(37,99,235,{opacity:.2f})' title='{wd} {h}~{h+2}시 {fmt_krw(val)}'>{fmt_int(val/10000)}만</div>")
        cells.append("".join(row))
    header = "<div></div>" + "".join(f"<div class='text-center text-[10px] font-black text-slate-400'>{h}시</div>" for h in hours)
    return f"<div class='grid grid-cols-9 gap-2'>{header}{''.join(cells)}</div>"


def grade_cards(grade: pd.DataFrame, chars: list[dict[str, str]]) -> str:
    if grade is None or grade.empty:
        return "<div class='p-6 text-sm font-bold text-slate-400'>등급 데이터 없음</div>"
    char_map = {c["grade"]: c for c in chars}
    cards = []
    for _, r in grade.sort_values("revenue", ascending=False).head(8).iterrows():
        label = str(r.get("member_grade_label") or "UNKNOWN")
        c = char_map.get(label, {})
        cards.append(f"""
        <div class="viz-card rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm">
          <div class="flex items-center justify-between gap-3">
            <div class="text-lg font-black text-slate-950">{escape(label)}</div>
            <div class="rounded-full bg-blue-50 px-3 py-1 text-xs font-black text-blue-700">재구매율 {fmt_pct(r.get('repeat_rate'))}</div>
          </div>
          <div class="mt-4 grid grid-cols-2 gap-2">
            <div class="rounded-xl bg-slate-50 p-3"><div class="text-[10px] font-black text-slate-400">매출</div><div class="text-base font-black text-slate-900">{fmt_krw(r.get('revenue'))}</div></div>
            <div class="rounded-xl bg-slate-50 p-3"><div class="text-[10px] font-black text-slate-400">객단가</div><div class="text-base font-black text-slate-900">{fmt_krw(r.get('aov'))}</div></div>
            <div class="rounded-xl bg-slate-50 p-3"><div class="text-[10px] font-black text-slate-400">구매자</div><div class="text-base font-black text-slate-900">{fmt_int(r.get('buyers'))}</div></div>
            <div class="rounded-xl bg-slate-50 p-3"><div class="text-[10px] font-black text-slate-400">재구매자</div><div class="text-base font-black text-slate-900">{fmt_int(r.get('repeat_buyers'))}</div></div>
          </div>
          <div class="mt-4 rounded-2xl bg-slate-50 p-4 text-sm font-bold leading-6 text-slate-600">{escape(c.get('body', '집단 특징 데이터 생성 대기'))}</div>
        </div>
        """)
    return "<div class='grid grid-cols-1 gap-4 xl:grid-cols-2'>" + "".join(cards) + "</div>"



def sort_grade_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or "member_grade_label" not in df.columns:
        return df
    order = {"FAMILY": 1, "SILVER": 2, "GOLD": 3, "TITANIUM": 4}
    d = df.copy()
    d["_grade_order"] = d["member_grade_label"].astype(str).map(lambda x: order.get(x.upper(), 99))
    return d.sort_values(["_grade_order", "revenue"], ascending=[True, False]).drop(columns=["_grade_order"], errors="ignore")


def grade_grouped_product_cards(df: pd.DataFrame, limit_per_grade: int = 6, repeat: bool = True) -> str:
    if df is None or df.empty:
        return "<div class='p-6 text-sm font-bold text-slate-400'>등급별 상품 데이터 없음</div>"
    d = sort_grade_df(df)
    grades = [g for g in ["FAMILY", "SILVER", "GOLD", "TITANIUM"] if g in set(d["member_grade_label"].astype(str))]
    grades += [g for g in d["member_grade_label"].astype(str).unique().tolist() if g not in grades]
    blocks = []
    for grade_label in grades[:4]:
        sub = d[d["member_grade_label"].astype(str) == grade_label].sort_values("revenue", ascending=False).head(limit_per_grade)
        blocks.append(f"""
        <div class="report-card rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm">
          <div class="mb-4 flex items-center justify-between gap-3">
            <div><div class="text-lg font-black text-slate-950">{escape(grade_label)}</div><div class="text-xs font-extrabold tracking-widest text-slate-400 uppercase">재구매 상품 TOP</div></div>
            <div class="rounded-full bg-slate-900 px-3 py-1 text-xs font-black text-white">TOP {len(sub)}</div>
          </div>
          <div class="grid grid-cols-2 gap-3 xl:grid-cols-3">{product_cards(sub, limit_per_grade, repeat)}</div>
        </div>
        """)
    return "<div class='grid grid-cols-1 gap-5 xl:grid-cols-2'>" + "".join(blocks) + "</div>"


def grade_grouped_category_cards(df: pd.DataFrame, limit_per_grade: int = 8) -> str:
    if df is None or df.empty:
        return "<div class='p-6 text-sm font-bold text-slate-400'>등급별 카테고리 데이터 없음</div>"
    d = sort_grade_df(df)
    grades = [g for g in ["FAMILY", "SILVER", "GOLD", "TITANIUM"] if g in set(d["member_grade_label"].astype(str))]
    grades += [g for g in d["member_grade_label"].astype(str).unique().tolist() if g not in grades]
    blocks = []
    for grade_label in grades[:4]:
        sub = d[d["member_grade_label"].astype(str) == grade_label].sort_values("revenue", ascending=False).head(limit_per_grade)
        blocks.append(f"""
        <div class="viz-card rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm">
          <div class="mb-4 flex items-center justify-between gap-3">
            <div><div class="text-lg font-black text-slate-950">{escape(grade_label)}</div><div class="text-xs font-extrabold tracking-widest text-slate-400 uppercase">재구매 2뎁스 카테고리</div></div>
            <div class="rounded-full bg-blue-50 px-3 py-1 text-xs font-black text-blue-700">TOP {len(sub)}</div>
          </div>
          {bar_list(sub, 'category', 'revenue', 'krw', limit_per_grade, True)}
        </div>
        """)
    return "<div class='grid grid-cols-1 gap-5 xl:grid-cols-2'>" + "".join(blocks) + "</div>"

def characteristics_html(chars: list[dict[str, str]]) -> str:
    if not chars:
        return "<div class='p-6 text-sm font-bold text-slate-400'>집단별 특징 없음</div>"
    rows = []
    for c in chars:
        rows.append(f"""
        <div class="rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm">
          <div class="text-base font-black text-slate-900">{escape(c.get('title',''))}</div>
          <div class="mt-2 text-sm font-bold leading-7 text-slate-600">{escape(c.get('body',''))}</div>
        </div>
        """)
    return "<div class='grid grid-cols-1 gap-4 xl:grid-cols-2'>" + "".join(rows) + "</div>"



def build_interactive_js(min_date: str, max_date: str, source_table: str) -> str:
    """Client-side period filter JS. Updates KPI and major visual sections using embedded daily datasets."""
    template = """
  <script>
    window.PURCHASE_PATTERN_FALLBACK_DATA = __DATA_JSON__;
  </script>
  <script>
    (function(){
      const payload = window.PURCHASE_PATTERN_FALLBACK_DATA || {};
      const normDate = x => String(x||'').slice(0,10);
      const daily = (payload.daily || []).map(x => ({...x, order_date:normDate(x.order_date)}));
      const sourceTable = __SOURCE_TABLE__;
      const fmtInt = n => Number(n||0).toLocaleString('ko-KR', {maximumFractionDigits:0});
      const fmtKrw = n => '₩' + Number(n||0).toLocaleString('ko-KR', {maximumFractionDigits:0});
      const fmtPct = n => ((Number(n||0))*100).toFixed(1) + '%';
      const $ = id => document.getElementById(id);
      const minDate = __MIN_DATE__;
      const maxDate = __MAX_DATE__;
      const gradeOrder = {FAMILY:1, SILVER:2, GOLD:3, TITANIUM:4};
      function setText(id, txt){ const el=$(id); if(el) el.textContent=txt; }
      function daysBefore(dateStr, days){ const d = new Date(dateStr + 'T00:00:00'); d.setDate(d.getDate()-days+1); return d.toISOString().slice(0,10); }
      function inRange(row, start, end){ const d=normDate(row.order_date); return (!start || d >= start) && (!end || d <= end); }
      function rows(name,start,end){ return (payload[name]||[]).map(x => ({...x, order_date:normDate(x.order_date)})).filter(r => inRange(r,start,end)); }
      function groupBy(rows, keys, sums){
        const m = new Map();
        rows.forEach(r => {
          const k = keys.map(x => String(r[x] ?? '')).join('||');
          if(!m.has(k)){ const base={}; keys.forEach(x=>base[x]=r[x]); sums.forEach(x=>base[x]=0); m.set(k,base); }
          const o=m.get(k); sums.forEach(x=>o[x]+=Number(r[x]||0));
        });
        return Array.from(m.values());
      }
      function barList(data, labelCol, valueCol, limit=12){
        if(!data.length) return `<div class='p-6 text-sm font-bold text-slate-400'>선택 기간 데이터 없음</div>`;
        data = data.slice().sort((a,b)=>Number(b[valueCol]||0)-Number(a[valueCol]||0)).slice(0,limit);
        const max = Math.max(...data.map(x=>Number(x[valueCol]||0)),1);
        return data.map(r => {
          const val=Number(r[valueCol]||0), pct=Math.max(0, Math.min(100, val/max*100));
          const label=String(r[labelCol]||'UNKNOWN');
          return `<div class="py-2 stagger-card"><div class="mb-1 flex items-center justify-between gap-3"><div class="truncate text-sm font-black text-slate-700" title="${label}">${label}</div><div class="whitespace-nowrap text-sm font-black text-slate-900">${fmtKrw(val)}</div></div><div class="bar-track"><div class="bar-fill-soft" style="width:${pct.toFixed(1)}%"></div></div></div>`;
        }).join('');
      }
      function miniCards(data,labelCol,valueCol,subs,limit=8,typ='krw'){
        if(!data.length) return `<div class='p-6 text-sm font-bold text-slate-400'>선택 기간 데이터 없음</div>`;
        data=data.slice().sort((a,b)=>Number(b[valueCol]||0)-Number(a[valueCol]||0)).slice(0,limit);
        const max=Math.max(...data.map(x=>Number(x[valueCol]||0)),1);
        return `<div class='grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-4'>` + data.map(r=>{
          const val=Number(r[valueCol]||0), pct=Math.max(0,Math.min(100,val/max*100));
          const main=typ==='int'?fmtInt(val):fmtKrw(val);
          const subHtml=subs.map(([col,lab,t])=>`<div class='rounded-xl bg-slate-50 p-2'><div class='text-[10px] font-black text-slate-400'>${lab}</div><div class='text-sm font-black text-slate-800'>${t==='krw'?fmtKrw(r[col]):fmtInt(r[col])}</div></div>`).join('');
          return `<div class="viz-card rounded-2xl border border-slate-200 bg-white/80 p-4 shadow-sm fade-swap"><div class="flex items-start justify-between gap-3"><div class="min-w-0"><div class="truncate text-base font-black text-slate-900" title="${r[labelCol]||''}">${r[labelCol]||''}</div><div class="mt-1 text-xl font-black text-blue-700">${main}</div></div><div class="rounded-full bg-slate-900 px-2 py-1 text-[10px] font-black text-white">${pct.toFixed(0)}%</div></div><div class="mt-3 bar-track"><div class="bar-fill" style="width:${pct.toFixed(1)}%"></div></div><div class="mt-3 grid grid-cols-2 gap-2">${subHtml}</div></div>`;
        }).join('') + `</div>`;
      }
      function productCards(data, limit=18, repeat=true){
        if(!data.length) return `<div class='p-6 text-sm font-bold text-slate-400'>선택 기간 상품 데이터 없음</div>`;
        data=data.slice().sort((a,b)=>Number(b.revenue||0)-Number(a.revenue||0)).slice(0,limit);
        return data.map((r,i)=>{
          const name=String(r.product_name_kor||r.product_name||r.product_code||'');
          const sku=String(r.product_code||'');
          const img=String(r.image_url||r.source_image_url||'');
          const imgHtml=img?`<img src="${img}" alt="${name}" class="product-img w-full rounded-xl border border-slate-100" loading="lazy" onerror="this.outerHTML='<div class=&quot;product-img flex w-full items-center justify-center rounded-xl border border-slate-100 text-xs font-black text-slate-300&quot;>NO IMAGE</div>'"/>`:`<div class='product-img flex w-full items-center justify-center rounded-xl border border-slate-100 text-xs font-black text-slate-300'>NO IMAGE</div>`;
          return `<div class="product-card rounded-2xl border border-slate-200 bg-white/80 p-3 shadow-sm fade-swap"><div class="relative">${imgHtml}<div class="absolute left-2 top-2 rounded-full bg-slate-950 px-2 py-1 text-xs font-black text-white">#${i+1}</div></div><div class="mt-3 line-clamp-2 min-h-[40px] text-sm font-black text-slate-900" title="${name}">${name}</div><div class="mt-1 truncate text-xs font-extrabold text-slate-400">${sku}</div><div class="mt-3 grid grid-cols-2 gap-2 text-xs"><div class="rounded-xl bg-slate-50 p-2"><div class="text-slate-400 font-black">${repeat?'재구매 매출':'매출'}</div><div class="font-black text-slate-900">${fmtKrw(r.revenue)}</div></div><div class="rounded-xl bg-slate-50 p-2"><div class="text-slate-400 font-black">수량</div><div class="font-black text-slate-900">${fmtInt(r.quantity)}</div></div></div></div>`;
        }).join('');
      }
      function gradeProductBlocks(data){
        if(!data.length) return `<div class='p-6 text-sm font-bold text-slate-400'>선택 기간 등급별 상품 데이터 없음</div>`;
        const grades=[...new Set(data.map(x=>String(x.member_grade_label||'UNKNOWN')))].sort((a,b)=>(gradeOrder[a]||99)-(gradeOrder[b]||99));
        return `<div class='grid grid-cols-1 gap-5 xl:grid-cols-2'>` + grades.slice(0,4).map(g=>{
          const sub=groupBy(data.filter(x=>String(x.member_grade_label||'')===g), ['member_grade_label','product_code','product_name_kor','product_name','image_url','source_image_url'], ['repeat_orders','repeat_buyers','quantity','revenue']).sort((a,b)=>b.revenue-a.revenue).slice(0,6);
          return `<div class="report-card rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm fade-swap"><div class="mb-4 flex items-center justify-between gap-3"><div><div class="text-lg font-black text-slate-950">${g}</div><div class="text-xs font-extrabold tracking-widest text-slate-400 uppercase">재구매 상품 TOP</div></div><div class="rounded-full bg-slate-900 px-3 py-1 text-xs font-black text-white">TOP ${sub.length}</div></div><div class="grid grid-cols-2 gap-3 xl:grid-cols-3">${productCards(sub,6,true)}</div></div>`;
        }).join('') + `</div>`;
      }
      function gradeCategoryBlocks(data){
        if(!data.length) return `<div class='p-6 text-sm font-bold text-slate-400'>선택 기간 등급별 카테고리 데이터 없음</div>`;
        const grades=[...new Set(data.map(x=>String(x.member_grade_label||'UNKNOWN')))].sort((a,b)=>(gradeOrder[a]||99)-(gradeOrder[b]||99));
        return `<div class='grid grid-cols-1 gap-5 xl:grid-cols-2'>` + grades.slice(0,4).map(g=>{
          const sub=groupBy(data.filter(x=>String(x.member_grade_label||'')===g), ['member_grade_label','category'], ['repeat_orders','repeat_buyers','quantity','revenue']).sort((a,b)=>b.revenue-a.revenue).slice(0,8);
          return `<div class="viz-card rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm fade-swap"><div class="mb-4 flex items-center justify-between gap-3"><div><div class="text-lg font-black text-slate-950">${g}</div><div class="text-xs font-extrabold tracking-widest text-slate-400 uppercase">재구매 2뎁스 카테고리</div></div><div class="rounded-full bg-blue-50 px-3 py-1 text-xs font-black text-blue-700">TOP ${sub.length}</div></div>${barList(sub,'category','revenue',8)}</div>`;
        }).join('') + `</div>`;
      }
      function updateHtml(id, html){ const el=$(id); if(el){ el.innerHTML=html; el.classList.remove('fade-swap'); void el.offsetWidth; el.classList.add('fade-swap'); } }
      function applyRange(start, end, label){
        if(!daily.length) return;
        const drows = daily.filter(r => inRange(r,start,end));
        const sum = (k) => drows.reduce((a,r)=>a+Number(r[k]||0),0);
        const orders = sum('orders'), buyers = sum('buyers'), revenue=sum('revenue'), qty=sum('quantity'), products=sum('products');
        const aov = orders ? revenue/orders : 0, qtyPerOrder = orders ? qty/orders : 0;
        setText('kpi-revenue-value', fmtKrw(revenue)); setText('kpi-revenue-sub', label);
        setText('kpi-orders-value', fmtInt(orders)); setText('kpi-orders-sub', '객단가 ' + fmtKrw(aov));
        setText('kpi-buyers-value', fmtInt(buyers)); setText('kpi-buyers-sub', '선택 기간 구매자 합계');
        setText('kpi-quantity-value', fmtInt(qty)); setText('kpi-quantity-sub', '주문당 수량 ' + fmtInt(qtyPerOrder));
        setText('kpi-products-value', fmtInt(products)); setText('kpi-products-sub', '일별 SKU 합산 기준');
        const couponOrders = sum('coupon_orders'); setText('kpi-repeat-value', fmtPct(orders ? couponOrders/orders : 0)); setText('kpi-repeat-sub', '선택 기간 쿠폰 주문 비중');
        setText('active-period-label', label + ' · source: ' + sourceTable);
        const m = groupBy(drows.map(r=>({...r, month:String(r.order_date).slice(0,7)})), ['month'], ['orders','buyers','products','quantity','revenue','coupon_orders']);
        updateHtml('monthly-flow', `<div class="month-strip">${barList(m.sort((a,b)=>String(a.month).localeCompare(String(b.month))),'month','revenue',999)}</div>`);
        updateHtml('category-top', barList(groupBy(rows('ui_daily_categories',start,end), ['category'], ['orders','buyers','products','quantity','revenue']), 'category','revenue',12));
        updateHtml('top-products', productCards(groupBy(rows('ui_daily_products',start,end), ['product_code','product_name_kor','product_name','image_url','source_image_url'], ['orders','buyers','quantity','revenue']),18,false));
        updateHtml('basket-composition', miniCards(groupBy(rows('ui_daily_basket',start,end), ['sku_bucket'], ['orders','buyers','quantity','revenue']), 'sku_bucket','revenue', [['orders','주문','int'],['buyers','구매자','int'],['quantity','수량','int'],['aov','객단가','krw']],4,'krw'));
        updateHtml('coupon-usage', miniCards(groupBy(rows('ui_daily_coupon',start,end), ['coupon_name'], ['orders','buyers','quantity','revenue','coupon_amount']), 'coupon_name','revenue', [['orders','주문','int'],['buyers','구매자','int'],['coupon_amount','쿠폰액','krw'],['quantity','수량','int']],8,'krw'));
        updateHtml('repeat-categories', barList(groupBy(rows('ui_daily_repeat_categories',start,end), ['category'], ['repeat_orders','repeat_buyers','quantity','revenue']), 'category','revenue',12));
        updateHtml('grade-repeat-categories', gradeCategoryBlocks(rows('ui_daily_grade_repeat_categories',start,end)));
        updateHtml('grade-repeat-products', gradeProductBlocks(rows('ui_daily_grade_repeat_products',start,end)));
      }
      function setActive(btn){ document.querySelectorAll('.pill').forEach(b=>b.classList.remove('active')); if(btn) btn.classList.add('active'); }
      document.querySelectorAll('[data-range]').forEach(btn => {
        btn.addEventListener('click', () => {
          const r = btn.getAttribute('data-range'); let s=minDate, e=maxDate, label='전체 기간';
          if(r==='7'){ s=daysBefore(maxDate,7); label='최근 7일'; }
          if(r==='30'){ s=daysBefore(maxDate,30); label='최근 30일'; }
          if(r==='90'){ s=daysBefore(maxDate,90); label='최근 90일'; }
          if(r==='ytd'){ s=maxDate.slice(0,4)+'-01-01'; label=maxDate.slice(0,4)+'년 YTD'; }
          if(r==='2026'){ s='2026-01-01'; e='2026-12-31'; label='2026년'; }
          if($('date-start')) $('date-start').value=s; if($('date-end')) $('date-end').value=e;
          setActive(btn); applyRange(s,e,label);
        });
      });
      const apply = $('apply-date');
      if(apply){ apply.addEventListener('click', () => { setActive(null); const s=$('date-start').value; const e=$('date-end').value; applyRange(s,e, s+' ~ '+e); }); }
    })();
  </script>
"""
    return (template
            .replace('__SOURCE_TABLE__', json.dumps(source_table, ensure_ascii=False))
            .replace('__MIN_DATE__', json.dumps(min_date, ensure_ascii=False))
            .replace('__MAX_DATE__', json.dumps(max_date, ensure_ascii=False)))

def render_html(results: dict[str, pd.DataFrame], out_dir: Path, source_table: str, period_label: str, summary_lines: list[str], group_chars: list[dict[str, str]]) -> str:
    overview_df = results.get("overview", pd.DataFrame())
    overview = overview_df.iloc[0].to_dict() if not overview_df.empty else {}
    monthly = results.get("monthly", pd.DataFrame())
    top_products = results.get("top_products", pd.DataFrame())
    top_categories = results.get("top_categories", pd.DataFrame())
    segments = results.get("member_segments", pd.DataFrame())
    basket = results.get("basket_size", pd.DataFrame())
    intervals = results.get("purchase_interval", pd.DataFrame())
    coupon = results.get("coupon_promo", pd.DataFrame())
    weekday = results.get("weekday_hour", pd.DataFrame())
    size_color = results.get("size_color", pd.DataFrame())
    grade = results.get("grade_overview", pd.DataFrame())
    grade_repeat = results.get("grade_repeat_overview", pd.DataFrame())
    repeat_categories = results.get("repeat_categories", pd.DataFrame())
    repeat_products = results.get("repeat_products", pd.DataFrame())
    grade_repeat_categories = results.get("grade_repeat_categories", pd.DataFrame())
    grade_repeat_products = results.get("grade_repeat_products", pd.DataFrame())

    min_date = str(overview.get("min_order_date") or "")
    max_date = str(overview.get("max_order_date") or "")
    has_2026 = bool((monthly["month"].astype(str).str.startswith("2026")).any()) if monthly is not None and not monthly.empty and "month" in monthly.columns else False
    years = sorted(monthly["year"].dropna().astype(int).unique().tolist()) if monthly is not None and not monthly.empty and "year" in monthly.columns else []
    year_text = ", ".join(map(str, years)) if years else "-"
    interactive_js = build_interactive_js(min_date, max_date, source_table)

    summary_html = "".join(f"<li class='leading-7 text-sm font-bold text-slate-600'>{escape(x)}</li>" for x in summary_lines)
    kpis = "".join([
        metric_card("매출", fmt_krw(overview.get("revenue")), period_label, "kpi-revenue"),
        metric_card("주문", fmt_int(overview.get("orders")), f"객단가 {fmt_krw(overview.get('aov_per_order'))}", "kpi-orders"),
        metric_card("구매자", fmt_int(overview.get("buyers")), f"구매자당 매출 {fmt_krw(overview.get('revenue_per_buyer'))}", "kpi-buyers"),
        metric_card("판매수량", fmt_int(overview.get("quantity")), f"주문당 수량 {fmt_int(overview.get('qty_per_order'))}", "kpi-quantity"),
        metric_card("상품 수", fmt_int(overview.get("products")), "구매 발생 SKU", "kpi-products"),
        metric_card("재구매 매출비중", fmt_pct(overview.get("repeat_buyer_revenue_share")), "재구매자 기준", "kpi-repeat"),
    ])

    size_color_vis = size_color.copy() if size_color is not None and not size_color.empty else size_color
    if size_color_vis is not None and not size_color_vis.empty:
        size_color_vis["option_label"] = size_color_vis.apply(lambda x: f"{x.get('product_size','')} / {x.get('product_color','')}", axis=1)
    grade_repeat_cat_vis = grade_repeat_categories.copy() if grade_repeat_categories is not None and not grade_repeat_categories.empty else grade_repeat_categories
    if grade_repeat_cat_vis is not None and not grade_repeat_cat_vis.empty:
        grade_repeat_cat_vis["label"] = grade_repeat_cat_vis.apply(lambda x: f"{x.get('member_grade_label','')} · {x.get('category','')}", axis=1)
    grade_repeat_prod_vis = grade_repeat_products.copy() if grade_repeat_products is not None and not grade_repeat_products.empty else grade_repeat_products
    if grade_repeat_prod_vis is not None and not grade_repeat_prod_vis.empty:
        grade_repeat_prod_vis["label"] = grade_repeat_prod_vis.apply(lambda x: f"{x.get('member_grade_label','')} · {x.get('product_name_kor') or x.get('product_name') or x.get('product_code')}", axis=1)

    month_badge_cls = "bg-green-50 text-green-700" if has_2026 else "bg-rose-50 text-rose-700"
    month_badge_txt = "2026 데이터 포함" if has_2026 else "2026 데이터 없음"

    html = f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>CSK E-COMM | 구매 패턴 분석</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200;400;600;800&display=swap');</style>
  {REPORT_PATCH_CSS}
</head>
<body class="bg-slate-50 text-slate-900 report-body">
  <div class="w-full max-w-none px-5 py-6 xl:px-8 2xl:px-10">
    <div class="flex flex-wrap items-center justify-between gap-3">
      <div class="flex flex-wrap items-center gap-3">
        <div class="text-2xl font-black">구매 패턴 분석</div>
        <div class="rounded-full bg-slate-900 px-3 py-1 text-xs font-extrabold text-white">PURCHASE DASHBOARD V7</div>
        <div id="active-period-label" class="text-sm font-semibold text-slate-500">{escape(period_label)} · source: {escape(source_table)}</div>
      </div>
      <div class="flex items-center gap-2">
        <a href="data/summary.json" class="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm font-extrabold hover:bg-slate-50">summary.json</a>
        <a href="data/purchase_pattern_analysis.xlsx" class="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm font-extrabold hover:bg-slate-50">Excel</a>
      </div>
    </div>

    <div class="report-card mt-5 rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm">
      <div class="flex flex-wrap items-end justify-between gap-4">
        <div><div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">기간 선택</div><div class="mt-1 text-sm font-bold text-slate-500">상단 KPI는 선택 기간 기준으로 즉시 재계산됩니다.</div></div>
        <div class="flex flex-wrap items-center gap-2">
          <button class="pill active" data-range="all">전체</button><button class="pill" data-range="7">최근 7일</button><button class="pill" data-range="30">최근 30일</button><button class="pill" data-range="90">최근 90일</button><button class="pill" data-range="ytd">올해 YTD</button><button class="pill" data-range="2026">2026년</button>
          <input id="date-start" type="date" min="{escape(min_date)}" max="{escape(max_date)}" value="{escape(min_date)}" class="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm font-black text-slate-700" />
          <input id="date-end" type="date" min="{escape(min_date)}" max="{escape(max_date)}" value="{escape(max_date)}" class="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm font-black text-slate-700" />
          <button id="apply-date" class="rounded-xl bg-slate-900 px-4 py-2 text-sm font-black text-white">적용</button>
        </div>
      </div>
    </div>

    <div class="mt-5 grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-6">{kpis}</div>

    <div class="report-card mt-6 rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm"><div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">핵심 요약</div><ul class="mt-3 list-disc pl-5">{summary_html}</ul></div>

    <details class="report-card month-details mt-6 rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm" open>
      <summary class="mb-0 flex flex-wrap items-center justify-between gap-3">
        <div><div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">월별 매출 흐름</div><div class="text-sm font-bold text-slate-400">전체 월 표시 · 포함 연도: {escape(year_text)} · 클릭해서 접기/펼치기</div></div>
        <div class="flex items-center gap-2"><div class="rounded-full {month_badge_cls} px-3 py-1 text-xs font-black">{month_badge_txt}</div><div class="chev rounded-full border border-slate-200 bg-white px-3 py-1 text-xs font-black text-slate-500 transition">⌄</div></div>
      </summary>
      <div id="monthly-flow" class="scroll-x mt-4 fade-swap"><div class="month-strip">{bar_list(monthly.sort_values('month') if not monthly.empty else monthly, 'month', 'revenue', 'krw', None)}</div></div>
    </details>

    <div class="mt-6 grid grid-cols-1 gap-5 xl:grid-cols-2">
      <div class="report-card rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm"><div class="mb-4"><div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">카테고리 매출 TOP</div><div class="text-sm font-bold text-slate-400">카테고리별 매출 집중도</div></div><div id="category-top">{bar_list(top_categories, 'category', 'revenue', 'krw', 12)}</div></div>
      <div class="report-card rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm"><div class="mb-4"><div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">요일 × 시간대 히트맵</div><div class="text-sm font-bold text-slate-400">칸 숫자는 만원 단위 매출</div></div>{heatmap_weekday_hour(weekday)}</div>
    </div>

    <div class="report-card mt-6 rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm"><div class="flex flex-wrap items-center justify-between gap-3"><div><div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">TOP PRODUCTS</div><div class="text-sm font-bold text-slate-400">엑셀/BQ/공식몰 크롤링 순으로 이미지 매칭 · 실패 내역은 product_image_log.json 확인</div></div><div class="rounded-full border border-slate-200 bg-white px-3 py-1 text-xs font-black text-slate-500">{len(top_products)} products</div></div><div class="mt-4 grid grid-cols-2 gap-4 md:grid-cols-3 xl:grid-cols-6"><div id="top-products" class="contents">{product_cards(top_products, 18)}</div></div></div>

    <div class="report-card mt-6 rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm"><div class="mb-4"><div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">회원 세그먼트</div><div class="text-sm font-bold text-slate-400">구매 빈도와 최근성 조합을 보기 쉽게 정리</div></div>{mini_visual_cards(segments, 'frequency_segment', 'revenue', [('buyers','구매자','int'),('orders','주문','int'),('revenue_per_buyer','인당 매출','krw'),('orders_per_buyer','인당 주문','int')], 8, 'krw')}</div>

    <div class="mt-6 grid grid-cols-1 gap-5 xl:grid-cols-2">
      <div class="report-card rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm"><div class="mb-4"><div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">장바구니/주문 구성</div><div class="text-sm font-bold text-slate-400">SKU 수·수량 기준 주문 구조</div></div><div id="basket-composition">{mini_visual_cards(basket, 'sku_bucket', 'revenue', [('orders','주문','int'),('buyers','구매자','int'),('quantity','수량','int'),('aov','객단가','krw')], 4, 'krw')}</div></div>
      <div class="report-card rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm"><div class="mb-4"><div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">재구매 간격</div><div class="text-sm font-bold text-slate-400">첫 구매 이후 다음 구매까지 걸린 기간</div></div>{mini_visual_cards(intervals, 'interval_bucket', 'repeat_orders', [('repeat_buyers','재구매자','int'),('avg_days_since_prev_order','평균일','int'),('median_days_since_prev_order','중앙일','int'),('revenue','매출','krw')], 8, 'int')}</div>
    </div>

    <div class="mt-6 grid grid-cols-1 gap-5 xl:grid-cols-2">
      <div class="report-card rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm"><div class="mb-4"><div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">쿠폰/포인트/프로모션</div><div class="text-sm font-bold text-slate-400">사용 쿠폰명 기준 매출</div></div><div id="coupon-usage">{mini_visual_cards(coupon, 'coupon_name', 'revenue', [('orders','주문','int'),('buyers','구매자','int'),('coupon_amount','쿠폰액','krw'),('aov','객단가','krw')], 8, 'krw')}</div></div>
      <div class="report-card rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm"><div class="mb-4"><div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">사이즈/컬러 TOP</div><div class="text-sm font-bold text-slate-400">옵션 단위 판매 집중도</div></div>{bar_list(size_color_vis, 'option_label', 'revenue', 'krw', 12, True)}</div>
    </div>

    <div class="report-card mt-6 rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm"><div class="mb-4"><div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">등급별 구매 패턴</div><div class="text-sm font-bold text-slate-400">등급별 매출·객단가·재구매율·주요 특징</div></div><div id="grade-overview-cards">{grade_cards(grade, group_chars)}</div></div>

    <div class="mt-6 grid grid-cols-1 gap-5 xl:grid-cols-2">
      <div class="report-card rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm"><div class="mb-4"><div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">등급별 재구매 현황</div><div class="text-sm font-bold text-slate-400">등급별 재구매 주문·매출·재구매 간격</div></div>{mini_visual_cards(grade_repeat, 'member_grade_label', 'repeat_revenue', [('repeat_orders','재구매 주문','int'),('repeat_buyers','재구매자','int'),('median_repeat_days','중앙일','int'),('avg_repeat_days','평균일','int')], 8, 'krw')}</div>
      <div class="report-card rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm"><div class="mb-4"><div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">집단별 특징</div><div class="text-sm font-bold text-slate-400">등급별 상세 특징</div></div>{characteristics_html(group_chars)}</div>
    </div>

    <div class="mt-6 grid grid-cols-1 gap-5 xl:grid-cols-2">
      <div class="report-card rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm"><div class="mb-4"><div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">재구매 카테고리 TOP</div><div class="text-sm font-bold text-slate-400">두 번째 구매 이후 많이 산 카테고리</div></div><div id="repeat-categories">{bar_list(repeat_categories, 'category', 'revenue', 'krw', 12)}</div></div>
      <div class="report-card rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm"><div class="mb-4"><div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">등급별 재구매 카테고리</div><div class="text-sm font-bold text-slate-400">등급별 재구매 2뎁스 카테고리 TOP</div></div><div id="grade-repeat-categories">{grade_grouped_category_cards(grade_repeat_categories, 8)}</div></div>
    </div>

    <div class="report-card mt-6 rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm"><div class="flex flex-wrap items-center justify-between gap-3"><div><div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">재구매 상품 TOP</div><div class="text-sm font-bold text-slate-400">두 번째 구매 이후 매출이 큰 상품</div></div><div class="rounded-full border border-slate-200 bg-white px-3 py-1 text-xs font-black text-slate-500">repeat products</div></div><div class="mt-4 grid grid-cols-2 gap-4 md:grid-cols-3 xl:grid-cols-6"><div id="repeat-products" class="contents">{product_cards(repeat_products, 18, True)}</div></div></div>

    <div class="report-card mt-6 rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm"><div class="mb-4"><div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">등급별 재구매 상품 TOP</div><div class="text-sm font-bold text-slate-400">등급별 반복구매 상품 매출</div></div><div id="grade-repeat-products">{grade_grouped_product_cards(grade_repeat_products, 6, True)}</div></div>

    <div class="mt-8 pb-8 text-xs font-bold text-slate-400">Generated at {dt.datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S KST')} · {escape(SCRIPT_VERSION)}</div>
  </div>
  {interactive_js}
</body>
</html>
"""
    return html

def build_embedded_payload(results: dict[str, pd.DataFrame], summary: dict[str, Any], group_chars: list[dict[str, str]]) -> dict[str, Any]:
    return {
        "script_version": SCRIPT_VERSION,
        "patch_notes": PATCH_NOTES,
        "summary": summary,
        "group_characteristics": group_chars,
        "overview": df_to_records(results.get("overview", pd.DataFrame()), 1),
        "daily": df_to_records(results.get("daily", pd.DataFrame()), 5000),
        "monthly": df_to_records(results.get("monthly", pd.DataFrame()), 500),
        "top_products": df_to_records(results.get("top_products", pd.DataFrame()), 300),
        "top_categories": df_to_records(results.get("top_categories", pd.DataFrame()), 100),
        "member_segments": df_to_records(results.get("member_segments", pd.DataFrame()), 200),
        "grade_overview": df_to_records(results.get("grade_overview", pd.DataFrame()), 100),
        "grade_monthly": df_to_records(results.get("grade_monthly", pd.DataFrame()), 500),
        "grade_top_products": df_to_records(results.get("grade_top_products", pd.DataFrame()), 500),
        "grade_categories": df_to_records(results.get("grade_categories", pd.DataFrame()), 500),
        "grade_coupon_promo": df_to_records(results.get("grade_coupon_promo", pd.DataFrame()), 200),
        "grade_repeat_overview": df_to_records(results.get("grade_repeat_overview", pd.DataFrame()), 100),
        "repeat_categories": df_to_records(results.get("repeat_categories", pd.DataFrame()), 100),
        "repeat_products": df_to_records(results.get("repeat_products", pd.DataFrame()), 300),
        "grade_repeat_categories": df_to_records(results.get("grade_repeat_categories", pd.DataFrame()), 300),
        "grade_repeat_products": df_to_records(results.get("grade_repeat_products", pd.DataFrame()), 300),
        "ui_daily_categories": df_to_records(results.get("ui_daily_categories", pd.DataFrame()), 10000),
        "ui_daily_products": df_to_records(results.get("ui_daily_products", pd.DataFrame()), 20000),
        "ui_daily_basket": df_to_records(results.get("ui_daily_basket", pd.DataFrame()), 10000),
        "ui_daily_coupon": df_to_records(results.get("ui_daily_coupon", pd.DataFrame()), 10000),
        "ui_daily_grade": df_to_records(results.get("ui_daily_grade", pd.DataFrame()), 10000),
        "ui_daily_repeat_categories": df_to_records(results.get("ui_daily_repeat_categories", pd.DataFrame()), 10000),
        "ui_daily_grade_repeat_categories": df_to_records(results.get("ui_daily_grade_repeat_categories", pd.DataFrame()), 10000),
        "ui_daily_grade_repeat_products": df_to_records(results.get("ui_daily_grade_repeat_products", pd.DataFrame()), 20000),
    }


def finalize_html_payload(html: str, payload: dict[str, Any]) -> str:
    payload_json = json.dumps(payload, ensure_ascii=False, default=str)
    if "__DATA_JSON__" in html:
        html = html.replace("__DATA_JSON__", payload_json.replace("</", "<\\/"))
    data_script = '<script id="purchase-pattern-data" type="application/json">' + payload_json.replace("</", "<\\/") + '</script>'
    if 'id="purchase-pattern-data"' not in html:
        html = html.replace("</body>", data_script + "\n</body>") if "</body>" in html else html + "\n" + data_script
    if "__DATA_JSON__" in html:
        raise RuntimeError("Purchase Pattern HTML still contains __DATA_JSON__ after render replacement.")
    return html


def write_outputs(results: dict[str, pd.DataFrame], out_dir: Path, source_table: str, period_label: str, image_log: list[dict[str, Any]]) -> None:
    data_dir = out_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    for name, df in results.items():
        path = data_dir / f"{name}.csv"
        df.to_csv(path, index=False, encoding="utf-8-sig")
        log(f"Wrote CSV: {path}")

    xlsx_path = data_dir / "purchase_pattern_analysis.xlsx"
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        for name, df in results.items():
            df.to_excel(writer, sheet_name=name[:31], index=False)
    log(f"Wrote Excel: {xlsx_path}")

    summary_lines = insight_summary(results)
    group_chars = build_group_characteristics(results)
    summary = {
        "script_version": SCRIPT_VERSION,
        "patch_notes": PATCH_NOTES,
        "generated_at_kst": dt.datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "source_table": source_table,
        "period_label": period_label,
        "overview": df_to_records(results.get("overview", pd.DataFrame()), 1)[0] if not results.get("overview", pd.DataFrame()).empty else {},
        "summary": summary_lines,
        "group_characteristics": group_chars,
        "top_products": df_to_records(results.get("top_products", pd.DataFrame()), 50),
        "top_categories": df_to_records(results.get("top_categories", pd.DataFrame()), 50),
        "grade_overview": df_to_records(results.get("grade_overview", pd.DataFrame()), 50),
        "repeat_categories": df_to_records(results.get("repeat_categories", pd.DataFrame()), 50),
        "repeat_products": df_to_records(results.get("repeat_products", pd.DataFrame()), 50),
        "image_log": image_log,
        "outputs": {
            "html": "index.html",
            "excel": "data/purchase_pattern_analysis.xlsx",
            "csv_dir": "data/",
        },
    }
    (data_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    (data_dir / "product_image_log.json").write_text(json.dumps(image_log, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    log(f"Wrote summary JSON: {data_dir / 'summary.json'}")

    html = render_html(results, out_dir, source_table, period_label, summary_lines, group_chars)
    embedded_payload = build_embedded_payload(results, summary, group_chars)
    html = finalize_html_payload(html, embedded_payload)

    build_manifest = {
        "script_version": SCRIPT_VERSION,
        "patch_notes": PATCH_NOTES,
        "generated_at_kst": dt.datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "source_table": source_table,
        "period_label": period_label,
        "html_has_data_script": 'id="purchase-pattern-data"' in html,
        "html_has_placeholder": "__DATA_JSON__" in html,
        "new_outputs": [
            "daily.csv",
            "grade_repeat_overview.csv",
            "repeat_categories.csv",
            "repeat_products.csv",
            "grade_repeat_categories.csv",
            "grade_repeat_products.csv",
            "ui_daily_categories.csv",
            "ui_daily_products.csv",
            "ui_daily_basket.csv",
            "ui_daily_coupon.csv",
            "ui_daily_grade_repeat_categories.csv",
            "ui_daily_grade_repeat_products.csv",
        ],
    }
    (data_dir / "build_manifest.json").write_text(json.dumps(build_manifest, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    log(f"Wrote build manifest: {data_dir / 'build_manifest.json'}")

    html_path = out_dir / "index.html"
    html_path.write_text(html, encoding="utf-8")
    log(f"Wrote HTML: {html_path}")


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Purchase pattern static HTML report from BigQuery order mart")
    p.add_argument("--project", default=getenv("BQ_PROJECT", "columbia-ga4"))
    p.add_argument("--location", default=getenv("BQ_LOCATION", "asia-northeast3"))
    p.add_argument("--raw-dataset", default=getenv("BQ_RAW_DATASET", "crm_raw"))
    p.add_argument("--source-table", default=getenv("BQ_ORDER_PRODUCT_TABLE", DEFAULT_SOURCE_TABLE))
    p.add_argument("--member-table", default=getenv("BQ_MEMBER_TABLE", DEFAULT_MEMBER_TABLE))
    p.add_argument("--start-date", default=getenv("PURCHASE_PATTERN_START_DATE", ""), help="Optional YYYY-MM-DD")
    p.add_argument("--end-date", default=getenv("PURCHASE_PATTERN_END_DATE", ""), help="Optional YYYY-MM-DD")
    p.add_argument("--out-dir", default=getenv("PURCHASE_PATTERN_OUT_DIR", str(DEFAULT_OUT_DIR)))
    p.add_argument("--top-limit", type=int, default=int(getenv("PURCHASE_PATTERN_TOP_LIMIT", "500")))
    p.add_argument("--image-xlsx", default=getenv("PURCHASE_PATTERN_IMAGE_XLSX", getenv("DAILY_DIGEST_IMAGE_XLS_PATH", "상품코드별 이미지.xlsx")))
    p.add_argument("--crawl-images", action="store_true", default=getenv("PURCHASE_PATTERN_CRAWL_IMAGES", "false").lower() in {"1", "true", "yes", "y"})
    p.add_argument("--download-images", action="store_true", default=getenv("PURCHASE_PATTERN_DOWNLOAD_IMAGES", "false").lower() in {"1", "true", "yes", "y"})
    p.add_argument("--max-crawl", type=int, default=int(getenv("PURCHASE_PATTERN_MAX_CRAWL", "120")))
    p.add_argument("--placeholder-img", default=getenv("PLACEHOLDER_IMG", getenv("PURCHASE_PATTERN_PLACEHOLDER_IMG", "")))
    p.add_argument("--print-version", action="store_true", help="Print script version and exit")
    return p


def main() -> int:
    args = build_arg_parser().parse_args()
    if args.print_version:
        print(SCRIPT_VERSION)
        return 0
    try:
        log(f"script_version={SCRIPT_VERSION}")
        setup_google_credentials()
        source_table = normalize_table_id(args.source_table, args.project, args.raw_dataset)
        member_table = normalize_table_id(args.member_table, args.project, args.raw_dataset) if args.member_table else ""
        out_dir = Path(args.out_dir)
        period_label = f"{args.start_date or 'ALL'} ~ {args.end_date or 'ALL'}"

        log("=" * 80)
        log("Columbia Purchase Pattern HTML Report")
        log(f"source_table={source_table}")
        log(f"member_table={member_table or '-'}")
        log(f"period={period_label}")
        log(f"out_dir={out_dir}")
        log(f"crawl_images={args.crawl_images}, download_images={args.download_images}, max_crawl={args.max_crawl}")
        log("=" * 80)

        client = bq_client(args.project, args.location)
        columns = get_table_columns(client, source_table, args.location)
        member_columns = safe_get_columns(client, member_table, args.location) if member_table else set()
        log(f"Source columns detected: {len(columns):,}")
        log(f"Member columns detected: {len(member_columns):,}")

        queries = build_queries(
            source_table=source_table,
            columns=columns,
            start_date=args.start_date,
            end_date=args.end_date,
            top_limit=args.top_limit,
            member_table=member_table,
            member_columns=member_columns,
        )
        results = run_queries(client, queries, args.location, args.start_date, args.end_date)

        top_products, image_log = attach_product_images(
            top_products=results.get("top_products", pd.DataFrame()),
            image_xlsx=args.image_xlsx,
            out_dir=out_dir,
            crawl_images=args.crawl_images,
            download_images=args.download_images,
            max_crawl=args.max_crawl,
            placeholder_img=args.placeholder_img,
        )
        results["top_products"] = top_products

        # Reuse the same image URLs for repeat product outputs where possible.
        img_map = {str(r.get("product_code")): r.get("image_url", "") for _, r in top_products.iterrows()} if not top_products.empty else {}
        img_src_map = {str(r.get("product_code")): r.get("image_source", "") for _, r in top_products.iterrows()} if not top_products.empty else {}
        for key in ["repeat_products", "grade_repeat_products", "grade_top_products", "ui_daily_products", "ui_daily_grade_repeat_products"]:
            if key in results and not results[key].empty:
                results[key] = results[key].copy()
                results[key]["image_url"] = results[key]["product_code"].astype(str).map(lambda x: img_map.get(x, ""))
                results[key]["image_source"] = results[key]["product_code"].astype(str).map(lambda x: img_src_map.get(x, ""))

        write_outputs(results, out_dir, source_table, period_label, image_log)
        log("DONE")
        return 0
    except Exception as e:
        log(f"ERROR: {type(e).__name__}: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
