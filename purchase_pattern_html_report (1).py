#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Columbia Purchase Pattern HTML Report
=====================================

목적
----
BigQuery에 적재된 구매 마트에서 전체 기간 구매 데이터만 추출해
구매 패턴 분석용 정적 HTML / JSON / CSV / Excel 파일을 생성한다.

Daily Digest 디자인 스타일과 동일한 방향
----------------------------------------
- Tailwind CDN 기반
- rounded-2xl 카드
- slate 계열 배경 / white translucent card
- KPI 카드, 테이블, 상품 카드, 요약 섹션
- GitHub Pages에서 바로 열 수 있는 reports/purchase_pattern/index.html 생성

상품 이미지 처리 우선순위
------------------------
1) --image-xlsx 엑셀의 상품코드/이미지링크 매핑
2) BigQuery 소스 테이블의 image_url / product_image_url / thumbnail_url 등 이미지 컬럼
3) 공식몰 상품 페이지/검색 페이지 크롤링 후 og:image 또는 img 추출
4) PLACEHOLDER_IMG 또는 빈 이미지

실행 예시
---------
python purchase_pattern_html_report.py
python purchase_pattern_html_report.py --start-date 2026-02-01 --end-date 2026-05-20
python purchase_pattern_html_report.py --crawl-images --download-images
python purchase_pattern_html_report.py --source-table columbia-ga4.crm_raw.tb_order_product_search_mart

필수 환경변수
-------------
GOOGLE_APPLICATION_CREDENTIALS 또는 GOOGLE_SA_JSON_B64
선택: BQ_PROJECT, BQ_LOCATION

추천 GitHub Pages 출력
----------------------
reports/purchase_pattern/index.html
reports/purchase_pattern/data/summary.json
reports/purchase_pattern/data/*.csv
reports/purchase_pattern/assets/product_images/*.jpg
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
from typing import Any, Dict, Iterable, Optional
from urllib.parse import quote_plus, urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery

KST = dt.timezone(dt.timedelta(hours=9))
BASE_DIR = Path(__file__).resolve().parent
DEFAULT_OUT_DIR = Path("reports") / "purchase_pattern"
DEFAULT_SOURCE_TABLE = "tb_order_product_search_mart"
OFFICIAL_MALL_BASE = "https://www.columbiakorea.co.kr"

REPORT_PATCH_CSS = """
<style>
  :root{--motion-ease:cubic-bezier(.2,.8,.2,1);}
  body{font-family:'Plus Jakarta Sans','Noto Sans KR','Malgun Gothic','Apple SD Gothic Neo',system-ui,-apple-system,'Segoe UI',Roboto,Arial;}
  .report-body{background:linear-gradient(180deg,#f8fafc 0%,#eef2f7 100%);}
  .report-card,.product-card{animation:cardRise .7s var(--motion-ease) both;transform-origin:center bottom;}
  .report-card:hover,.product-card:hover{transform:translateY(-4px);box-shadow:0 18px 40px rgba(15,23,42,.08);}
  .kpi-card{position:relative;overflow:hidden;transition:transform .24s var(--motion-ease), box-shadow .24s var(--motion-ease), border-color .24s var(--motion-ease)}
  .kpi-card:before{content:'';position:absolute;inset:-40% auto auto -20%;width:60%;height:180%;background:linear-gradient(90deg,transparent,rgba(255,255,255,.45),transparent);transform:rotate(14deg);animation:shineSweep 4.2s linear infinite;pointer-events:none}
  .kpi-card:hover{transform:translateY(-6px) scale(1.01);box-shadow:0 22px 44px rgba(15,23,42,.08);border-color:rgba(59,130,246,.22)}
  .kpi-value{animation:numberPop .8s var(--motion-ease) both}
  .table-wrap{overflow-x:auto}.table-wrap table{min-width:920px}
  .bar-track{height:8px;border-radius:999px;background:#e2e8f0;overflow:hidden}.bar-fill{height:100%;border-radius:999px;background:#0f172a;}
  .product-img{aspect-ratio:1/1;object-fit:cover;background:#f1f5f9;}
  @keyframes cardRise{from{opacity:0;transform:translateY(26px) scale(.985)}to{opacity:1;transform:translateY(0) scale(1)}}
  @keyframes numberPop{0%{opacity:.2;transform:translateY(12px) scale(.96)}60%{opacity:1;transform:translateY(-2px) scale(1.02)}100%{opacity:1;transform:translateY(0) scale(1)}}
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


def normalize_table_id(table_id: str, project: str, raw_dataset: str) -> str:
    table_id = table_id.strip().strip("`")
    parts = [p for p in table_id.split(".") if p]
    if len(parts) == 1:
        return f"{project}.{raw_dataset}.{parts[0]}"
    if len(parts) == 2:
        return f"{project}.{parts[0]}.{parts[1]}"
    if len(parts) == 3:
        return ".".join(parts)
    raise ValueError(f"Invalid BigQuery table id: {table_id}")


def bq_client(project: str, location: str) -> bigquery.Client:
    return bigquery.Client(project=project, location=location)


def parse_bq_table_parts(table_id: str) -> tuple[str, str, str]:
    parts = table_id.strip("`").split(".")
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


def pick_col(columns: set[str], candidates: Iterable[str], default: str = "") -> str:
    lowered = {c.lower(): c for c in columns}
    for cand in candidates:
        if cand.lower() in lowered:
            return lowered[cand.lower()]
    return default


def safe_str_expr(columns: set[str], col: str, alias: str, default: str = "") -> str:
    if col and col in columns:
        return f"NULLIF(CAST({col} AS STRING), '') AS {alias}"
    return f"CAST('{default}' AS STRING) AS {alias}"


def safe_num_expr(columns: set[str], col: str, alias: str, default: str = "0") -> str:
    if col and col in columns:
        return f"COALESCE(SAFE_CAST({col} AS FLOAT64), {default}) AS {alias}"
    return f"CAST({default} AS FLOAT64) AS {alias}"


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


def build_base_cte(source_table: str, columns: set[str], start_date: str, end_date: str) -> str:
    revenue_col = pick_col(columns, ["admin_revenue", "admin_net_product_amount", "net_erp_revenue", "erp_revenue", "revenue"])
    gross_col = pick_col(columns, ["line_gross_admin_revenue", "order_product_price", "gross_revenue", "product_price"])
    qty_col = pick_col(columns, ["purchase_qty", "ProductQuantity", "quantity", "qty"])
    image_col = pick_col(columns, ["image_url", "product_image_url", "thumbnail_url", "product_img_url", "main_image_url"])
    product_name_kr = pick_col(columns, ["product_name_kor", "ProductName_Kor", "item_name", "itemName", "product_name"])
    product_name = pick_col(columns, ["product_name", "ProductName", "itemName", "item_name"])
    category_kr = pick_col(columns, ["category_title_kr", "category_name_kr", "category_kr"])
    category = pick_col(columns, ["category_title", "category_code", "category"])
    order_dt = pick_col(columns, ["order_datetime", "order_reg_datetime", "order_product_datetime", "order_date"])

    if not revenue_col:
        raise RuntimeError("No revenue column found. Expected admin_revenue/admin_net_product_amount/net_erp_revenue/erp_revenue/revenue.")
    if not qty_col:
        raise RuntimeError("No quantity column found. Expected purchase_qty/ProductQuantity/quantity/qty.")

    refund_condition = ""
    if "order_refund_status" in columns:
        refund_condition = "AND COALESCE(SAFE_CAST(order_refund_status AS INT64), 0) = 0"
    net_sales_condition = ""
    if "is_net_sales_line" in columns:
        net_sales_condition = "AND COALESCE(SAFE_CAST(is_net_sales_line AS INT64), 1) = 1"

    return f"""
WITH purchase_lines AS (
  SELECT
    DATE(order_date) AS order_date,
    DATETIME({order_dt}) AS order_datetime,
    EXTRACT(YEAR FROM DATE(order_date)) AS year,
    FORMAT_DATE('%Y-%m', DATE(order_date)) AS month,
    FORMAT_DATE('%G-W%V', DATE(order_date)) AS iso_week,
    EXTRACT(DAYOFWEEK FROM DATE(order_date)) AS dow_num,
    CASE EXTRACT(DAYOFWEEK FROM DATE(order_date))
      WHEN 1 THEN 'Sun' WHEN 2 THEN 'Mon' WHEN 3 THEN 'Tue' WHEN 4 THEN 'Wed'
      WHEN 5 THEN 'Thu' WHEN 6 THEN 'Fri' WHEN 7 THEN 'Sat'
    END AS weekday,
    EXTRACT(HOUR FROM DATETIME({order_dt})) AS order_hour,
    CAST(order_no AS STRING) AS order_no,
    CAST(member_id AS STRING) AS member_id,
    CAST(product_code AS STRING) AS product_code,
    {safe_str_expr(columns, product_name_kr, 'product_name_kor')},
    {safe_str_expr(columns, product_name, 'product_name')},
    {safe_str_expr(columns, pick_col(columns, ['product_style', 'ProductStyle']), 'product_style')},
    {safe_str_expr(columns, pick_col(columns, ['product_size', 'ProductSize']), 'product_size')},
    {safe_str_expr(columns, pick_col(columns, ['product_color', 'ProductColor']), 'product_color')},
    {safe_str_expr(columns, pick_col(columns, ['master_product_color', 'ProductColorName']), 'master_product_color')},
    {safe_str_expr(columns, category_kr, 'category_title_kr')},
    {safe_str_expr(columns, category, 'category_title')},
    {safe_str_expr(columns, pick_col(columns, ['category_code']), 'category_code')},
    {safe_str_expr(columns, pick_col(columns, ['sex_label', 'gender_label']), 'sex_label')},
    {safe_str_expr(columns, pick_col(columns, ['product_year', 'ProductYear']), 'product_year')},
    {safe_str_expr(columns, pick_col(columns, ['product_season', 'ProductSeason']), 'product_season')},
    {safe_str_expr(columns, pick_col(columns, ['member_gender', 'MemberGender']), 'member_gender')},
    COALESCE(SAFE_CAST({pick_col(columns, ['member_age'], 'NULL')} AS INT64), NULL) AS member_age,
    {safe_str_expr(columns, pick_col(columns, ['order_device_type', 'OrderSaleCategory']), 'order_device_type')},
    {safe_str_expr(columns, image_col, 'source_image_url')},
    COALESCE(SAFE_CAST({qty_col} AS INT64), 0) AS purchase_qty,
    COALESCE(SAFE_CAST({revenue_col} AS INT64), 0) AS revenue,
    COALESCE(SAFE_CAST({gross_col if gross_col else revenue_col} AS INT64), 0) AS gross_revenue,
    COALESCE(SAFE_CAST({pick_col(columns, ['order_use_coupon_total', 'order_use_coupon_price'], '0')} AS INT64), 0) AS coupon_amount,
    COALESCE(SAFE_CAST({pick_col(columns, ['order_product_use_mileage', 'order_use_point'], '0')} AS INT64), 0) AS mileage_amount,
    COALESCE(SAFE_CAST({pick_col(columns, ['product_promotion_sale_price', 'promotion_amount'], '0')} AS INT64), 0) AS promotion_amount,
    COALESCE(SAFE_CAST({pick_col(columns, ['is_coupon_order'], '0')} AS INT64), 0) AS is_coupon_order,
    COALESCE(SAFE_CAST({pick_col(columns, ['is_point_used_order'], '0')} AS INT64), 0) AS is_point_used_order,
    COALESCE(SAFE_CAST({pick_col(columns, ['is_promotion_line'], '0')} AS INT64), 0) AS is_promotion_line
  FROM `{source_table}`
  WHERE order_date IS NOT NULL
    AND CAST(order_no AS STRING) IS NOT NULL
    AND TRIM(CAST(order_no AS STRING)) != ''
    AND CAST(member_id AS STRING) IS NOT NULL
    AND TRIM(CAST(member_id AS STRING)) != ''
    AND COALESCE(SAFE_CAST({qty_col} AS INT64), 0) > 0
    AND COALESCE(SAFE_CAST({revenue_col} AS INT64), 0) > 0
    {refund_condition}
    {net_sales_condition}
    {date_filter_sql(start_date, end_date)}
)
"""


def build_queries(source_table: str, columns: set[str], start_date: str, end_date: str, top_limit: int) -> dict[str, str]:
    base = build_base_cte(source_table, columns, start_date, end_date)
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
SELECT *
FROM line_summary
CROSS JOIN member_summary
""",
        "monthly": base + """
SELECT month, COUNT(DISTINCT order_no) AS orders, COUNT(DISTINCT member_id) AS buyers,
       COUNT(DISTINCT product_code) AS products, SUM(purchase_qty) AS quantity, SUM(revenue) AS revenue,
       SAFE_DIVIDE(SUM(revenue), COUNT(DISTINCT order_no)) AS aov,
       SAFE_DIVIDE(SUM(purchase_qty), COUNT(DISTINCT order_no)) AS qty_per_order,
       COUNT(DISTINCT CASE WHEN is_coupon_order = 1 THEN order_no END) AS coupon_orders,
       SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN is_coupon_order = 1 THEN order_no END), COUNT(DISTINCT order_no)) AS coupon_order_rate
FROM purchase_lines
GROUP BY month
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
LIMIT {int(top_limit)}
""",
        "top_categories": base + """
SELECT COALESCE(category_title_kr, category_title, category_code, 'UNKNOWN') AS category,
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
    CASE WHEN frequency_orders = 1 THEN '01_one_time'
         WHEN frequency_orders BETWEEN 2 AND 3 THEN '02_repeat_2_3'
         WHEN frequency_orders BETWEEN 4 AND 6 THEN '03_loyal_4_6'
         ELSE '04_vip_7_plus' END AS frequency_segment,
    CASE WHEN recency_days <= 30 THEN '01_recent_30d'
         WHEN recency_days <= 90 THEN '02_recent_90d'
         WHEN recency_days <= 180 THEN '03_recent_180d'
         ELSE '04_dormant_180d_plus' END AS recency_segment
  FROM member_level
)
SELECT frequency_segment, recency_segment, COUNT(*) AS buyers, SUM(frequency_orders) AS orders,
       SUM(monetary_revenue) AS revenue,
       SAFE_DIVIDE(SUM(monetary_revenue), COUNT(*)) AS revenue_per_buyer,
       SAFE_DIVIDE(SUM(frequency_orders), COUNT(*)) AS orders_per_buyer
FROM segmented
GROUP BY frequency_segment, recency_segment
ORDER BY frequency_segment, recency_segment
""",
        "basket_size": base + """
, order_level AS (
  SELECT order_no, member_id, SUM(revenue) AS order_revenue, SUM(purchase_qty) AS order_qty,
         COUNT(DISTINCT product_code) AS sku_count, COUNT(*) AS line_count
  FROM purchase_lines GROUP BY order_no, member_id
)
SELECT
  CASE WHEN sku_count = 1 THEN '01_1_sku' WHEN sku_count = 2 THEN '02_2_skus'
       WHEN sku_count = 3 THEN '03_3_skus' ELSE '04_4_plus_skus' END AS sku_bucket,
  CASE WHEN order_qty = 1 THEN '01_1_qty' WHEN order_qty = 2 THEN '02_2_qty'
       WHEN order_qty = 3 THEN '03_3_qty' ELSE '04_4_plus_qty' END AS qty_bucket,
  COUNT(DISTINCT order_no) AS orders, COUNT(DISTINCT member_id) AS buyers,
  SUM(order_qty) AS quantity, SUM(order_revenue) AS revenue,
  SAFE_DIVIDE(SUM(order_revenue), COUNT(DISTINCT order_no)) AS aov
FROM order_level
GROUP BY sku_bucket, qty_bucket
ORDER BY sku_bucket, qty_bucket
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
  CASE WHEN days_since_prev_order <= 7 THEN '01_within_7d'
       WHEN days_since_prev_order <= 30 THEN '02_8_30d'
       WHEN days_since_prev_order <= 90 THEN '03_31_90d'
       WHEN days_since_prev_order <= 180 THEN '04_91_180d'
       ELSE '05_181d_plus' END AS interval_bucket,
  COUNT(*) AS repeat_orders, COUNT(DISTINCT member_id) AS repeat_buyers,
  AVG(days_since_prev_order) AS avg_days_since_prev_order,
  APPROX_QUANTILES(days_since_prev_order, 100)[OFFSET(50)] AS median_days_since_prev_order,
  SUM(order_revenue) AS revenue
FROM intervals
GROUP BY interval_bucket
ORDER BY interval_bucket
""",
        "coupon_promo": base + """
SELECT CASE WHEN is_coupon_order = 1 THEN 'coupon_used' ELSE 'no_coupon' END AS coupon_flag,
       CASE WHEN is_point_used_order = 1 THEN 'point_used' ELSE 'no_point' END AS point_flag,
       CASE WHEN is_promotion_line = 1 THEN 'promotion' ELSE 'no_promotion' END AS promotion_flag,
       COUNT(DISTINCT order_no) AS orders, COUNT(DISTINCT member_id) AS buyers,
       SUM(purchase_qty) AS quantity, SUM(revenue) AS revenue,
       SUM(coupon_amount) AS coupon_amount, SUM(mileage_amount) AS mileage_amount,
       SUM(promotion_amount) AS promotion_amount,
       SAFE_DIVIDE(SUM(revenue), COUNT(DISTINCT order_no)) AS aov
FROM purchase_lines
GROUP BY coupon_flag, point_flag, promotion_flag
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


def normalize_sku(value: Any) -> str:
    s = str(value or "").strip()
    s = re.sub(r"\.0+$", "", s)
    return s.upper()


def load_image_map_from_excel(path: str) -> dict[str, str]:
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        candidates = [BASE_DIR / path, Path("reports") / "daily_digest" / "data" / Path(path).name]
        p = next((x for x in candidates if x.exists()), p)
    if not p.exists():
        log(f"Image Excel not found: {path}")
        return {}

    m: dict[str, str] = {}
    try:
        raw = pd.read_excel(p, sheet_name=0, header=None)
        sku_idx = url_idx = None
        header_row = 0
        for i in range(min(50, raw.shape[0])):
            for j, v in enumerate(raw.iloc[i].tolist()):
                sv = str(v or "").strip()
                low = sv.lower()
                if sku_idx is None and ("상품코드" in sv or low in {"sku", "itemid", "item_id", "product_code"}):
                    sku_idx = j
                if url_idx is None and ("이미지" in sv or "image" in low or "url" in low):
                    url_idx = j
            if sku_idx is not None and url_idx is not None:
                header_row = i
                break
        if sku_idx is None:
            sku_idx = 0
        if url_idx is None:
            url_idx = 1
        for r in range(header_row + 1, raw.shape[0]):
            sku = normalize_sku(raw.iat[r, sku_idx]) if sku_idx < raw.shape[1] else ""
            url = str(raw.iat[r, url_idx]).strip() if url_idx < raw.shape[1] else ""
            if sku and url.lower().startswith("http"):
                m[sku] = url
        log(f"Loaded image map: {len(m):,} rows from {p}")
    except Exception as e:
        log(f"Image Excel parse failed: {type(e).__name__}: {e}")
    return m


def candidate_product_urls(sku: str) -> list[str]:
    sku_q = quote_plus(sku)
    return [
        f"{OFFICIAL_MALL_BASE}/product/{sku}",
        f"{OFFICIAL_MALL_BASE}/Product/{sku}",
        f"{OFFICIAL_MALL_BASE}/goods/goods_view.php?goodsNo={sku_q}",
        f"{OFFICIAL_MALL_BASE}/product/search?keyword={sku_q}",
        f"{OFFICIAL_MALL_BASE}/search?keyword={sku_q}",
    ]


def is_productish_image(url: str) -> bool:
    u = (url or "").lower()
    if not u.startswith("http"):
        return False
    bad = ["logo", "sprite", "icon", "blank", "loading", "placeholder", "favicon"]
    return not any(x in u for x in bad)


def extract_image_from_html(html: str, base_url: str) -> str:
    soup = BeautifulSoup(html or "", "html.parser")
    for sel in [
        "meta[property='og:image']",
        "meta[name='twitter:image']",
        "meta[property='og:image:secure_url']",
    ]:
        tag = soup.select_one(sel)
        if tag and tag.get("content"):
            url = urljoin(base_url, tag.get("content", "").strip())
            if is_productish_image(url):
                return url
    for img in soup.select("img"):
        for attr in ["data-src", "data-original", "data-lazy", "src"]:
            val = img.get(attr)
            if not val:
                continue
            url = urljoin(base_url, str(val).strip())
            if is_productish_image(url):
                return url
    return ""


def crawl_product_image(sku: str, session: requests.Session, sleep_sec: float = 0.2) -> tuple[str, str]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    for url in candidate_product_urls(sku):
        try:
            resp = session.get(url, headers=headers, timeout=10, allow_redirects=True)
            if resp.status_code >= 400 or not resp.text:
                continue
            img = extract_image_from_html(resp.text, resp.url or url)
            if img:
                time.sleep(max(sleep_sec, 0))
                return img, resp.url or url
        except Exception:
            continue
    return "", ""


def download_image(url: str, out_dir: Path, sku: str) -> str:
    if not url or not url.lower().startswith("http"):
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
        resp = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        if len(resp.content) < 1024:
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
        source_url = str(row.get("source_image_url") or "").strip()
        image_url = ""
        image_source = ""
        crawled_from = ""

        if sku in image_map:
            image_url = image_map[sku]
            image_source = "excel_map"
        elif source_url.lower().startswith("http"):
            image_url = source_url
            image_source = "bigquery_column"
        elif crawl_images and crawled < max_crawl:
            image_url, crawled_from = crawl_product_image(sku, session)
            crawled += 1
            image_source = "crawled" if image_url else "not_found"
        else:
            image_source = "skipped"

        local_path = ""
        if download_images and image_url:
            local_path = download_image(image_url, local_img_dir, sku)
            if local_path:
                # HTML에서 reports/purchase_pattern/index.html 기준 상대경로로 사용
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
            "image_url": image_url,
            "html_image_url": image_url_for_html,
            "image_source": image_source,
            "crawled_from": crawled_from,
            "local_path": local_path,
        })

    df["image_url"] = final_urls
    df["image_source"] = sources
    return df, log_rows


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


def insight_summary(results: dict[str, pd.DataFrame]) -> list[str]:
    lines: list[str] = []
    overview = results.get("overview", pd.DataFrame())
    monthly = results.get("monthly", pd.DataFrame())
    top_products = results.get("top_products", pd.DataFrame())
    segments = results.get("member_segments", pd.DataFrame())
    intervals = results.get("purchase_interval", pd.DataFrame())
    coupon = results.get("coupon_promo", pd.DataFrame())

    if not overview.empty:
        r = overview.iloc[0]
        lines.append(f"전체 분석 기간은 {r.get('min_order_date')}~{r.get('max_order_date')}이며, 총 매출 {fmt_krw(r.get('revenue'))}, 주문 {fmt_int(r.get('orders'))}건, 구매자 {fmt_int(r.get('buyers'))}명입니다.")
        lines.append(f"객단가는 {fmt_krw(r.get('aov_per_order'))}, 구매자당 매출은 {fmt_krw(r.get('revenue_per_buyer'))}, 재구매자 매출 비중은 {fmt_pct(r.get('repeat_buyer_revenue_share'))}입니다.")
    if not monthly.empty:
        best = monthly.sort_values("revenue", ascending=False).iloc[0]
        lines.append(f"월별 최고 매출 월은 {best.get('month')}이며 매출 {fmt_krw(best.get('revenue'))}, 주문 {fmt_int(best.get('orders'))}건입니다.")
    if not top_products.empty:
        p = top_products.sort_values("revenue", ascending=False).iloc[0]
        pname = p.get("product_name_kor") or p.get("product_name") or p.get("product_code")
        lines.append(f"매출 1위 상품은 {pname}({p.get('product_code')})로, 매출 {fmt_krw(p.get('revenue'))}, 판매수량 {fmt_int(p.get('quantity'))}개입니다.")
    if not segments.empty:
        best_seg = segments.sort_values("revenue", ascending=False).iloc[0]
        lines.append(f"가장 큰 세그먼트는 {best_seg.get('frequency_segment')} × {best_seg.get('recency_segment')}이며 매출 {fmt_krw(best_seg.get('revenue'))}입니다.")
    if not intervals.empty:
        repeat_sum = intervals["repeat_orders"].sum() if "repeat_orders" in intervals.columns else 0
        if repeat_sum:
            best_i = intervals.sort_values("repeat_orders", ascending=False).iloc[0]
            lines.append(f"재구매 간격은 {best_i.get('interval_bucket')} 구간이 가장 많고, 해당 구간 재구매 주문은 {fmt_int(best_i.get('repeat_orders'))}건입니다.")
    if not coupon.empty:
        c = coupon.sort_values("revenue", ascending=False).iloc[0]
        lines.append(f"쿠폰/포인트/프로모션 조합 중 매출 기여가 큰 조합은 {c.get('coupon_flag')} · {c.get('point_flag')} · {c.get('promotion_flag')}입니다.")
    return lines[:6]


def metric_card(label: str, value: str, sub: str = "") -> str:
    return f"""
    <div class="kpi-card rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm">
      <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">{escape(label)}</div>
      <div class="kpi-value mt-3 text-3xl font-black text-slate-950">{escape(value)}</div>
      <div class="mt-2 text-sm font-semibold text-slate-500">{escape(sub)}</div>
    </div>
    """


def simple_table(df: pd.DataFrame, columns: list[tuple[str, str, str]], limit: int = 12) -> str:
    if df is None or df.empty:
        return "<div class='p-6 text-sm font-bold text-slate-400'>No data</div>"
    d = df.head(limit)
    thead = "".join(f"<th class='whitespace-nowrap px-4 py-3 text-left text-xs font-black uppercase tracking-wider text-slate-500'>{escape(label)}</th>" for _, label, _ in columns)
    rows = []
    for _, r in d.iterrows():
        tds = []
        for col, _, typ in columns:
            val = r.get(col, "")
            if typ == "krw":
                txt = fmt_krw(val)
            elif typ == "int":
                txt = fmt_int(val)
            elif typ == "pct":
                txt = fmt_pct(val)
            else:
                txt = str(val if pd.notna(val) else "")
            align = "text-right" if typ in {"krw", "int", "pct"} else "text-left"
            tds.append(f"<td class='whitespace-nowrap px-4 py-3 text-sm font-bold text-slate-700 {align}'>{escape(txt)}</td>")
        rows.append(f"<tr class='border-t border-slate-100 hover:bg-slate-50'>{''.join(tds)}</tr>")
    return f"""
    <div class="table-wrap">
      <table class="w-full border-collapse">
        <thead><tr>{thead}</tr></thead>
        <tbody>{''.join(rows)}</tbody>
      </table>
    </div>
    """


def bar_list(df: pd.DataFrame, label_col: str, value_col: str, fmt="krw", limit=10) -> str:
    if df is None or df.empty or value_col not in df.columns:
        return "<div class='p-6 text-sm font-bold text-slate-400'>No data</div>"
    d = df.head(limit).copy()
    max_v = float(pd.to_numeric(d[value_col], errors="coerce").fillna(0).max() or 1)
    rows = []
    for _, r in d.iterrows():
        val = float(pd.to_numeric(r.get(value_col), errors="coerce") or 0)
        pct = min(max(val / max_v * 100, 0), 100)
        txt = fmt_krw(val) if fmt == "krw" else fmt_int(val)
        label = str(r.get(label_col, ""))
        rows.append(f"""
        <div class="py-2">
          <div class="mb-1 flex items-center justify-between gap-3">
            <div class="truncate text-sm font-black text-slate-700">{escape(label)}</div>
            <div class="whitespace-nowrap text-sm font-black text-slate-900">{escape(txt)}</div>
          </div>
          <div class="bar-track"><div class="bar-fill" style="width:{pct:.1f}%"></div></div>
        </div>
        """)
    return "".join(rows)


def product_cards(df: pd.DataFrame, limit: int = 12) -> str:
    if df is None or df.empty:
        return "<div class='p-6 text-sm font-bold text-slate-400'>No product data</div>"
    cards = []
    for idx, (_, r) in enumerate(df.head(limit).iterrows(), start=1):
        sku = str(r.get("product_code", ""))
        name = str(r.get("product_name_kor") or r.get("product_name") or sku)
        img = str(r.get("image_url") or "")
        img_html = f"<img src='{escape(img)}' alt='{escape(name)}' class='product-img w-full rounded-xl border border-slate-100' loading='lazy'/>" if img else "<div class='product-img flex w-full items-center justify-center rounded-xl border border-slate-100 text-xs font-black text-slate-300'>NO IMAGE</div>"
        cards.append(f"""
        <div class="product-card rounded-2xl border border-slate-200 bg-white/80 p-3 shadow-sm">
          <div class="relative">
            {img_html}
            <div class="absolute left-2 top-2 rounded-full bg-slate-950 px-2 py-1 text-xs font-black text-white">#{idx}</div>
          </div>
          <div class="mt-3 truncate text-sm font-black text-slate-900" title="{escape(name)}">{escape(name)}</div>
          <div class="mt-1 text-xs font-extrabold text-slate-400">{escape(sku)}</div>
          <div class="mt-3 grid grid-cols-2 gap-2 text-xs">
            <div class="rounded-xl bg-slate-50 p-2"><div class="text-slate-400 font-black">Revenue</div><div class="font-black text-slate-900">{fmt_krw(r.get('revenue'))}</div></div>
            <div class="rounded-xl bg-slate-50 p-2"><div class="text-slate-400 font-black">Qty</div><div class="font-black text-slate-900">{fmt_int(r.get('quantity'))}</div></div>
          </div>
        </div>
        """)
    return "".join(cards)


def render_html(results: dict[str, pd.DataFrame], out_dir: Path, source_table: str, period_label: str, summary_lines: list[str]) -> str:
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

    kpis = "".join([
        metric_card("Revenue", fmt_krw(overview.get("revenue")), f"{period_label}"),
        metric_card("Orders", fmt_int(overview.get("orders")), f"AOV {fmt_krw(overview.get('aov_per_order'))}"),
        metric_card("Buyers", fmt_int(overview.get("buyers")), f"Revenue / buyer {fmt_krw(overview.get('revenue_per_buyer'))}"),
        metric_card("Products", fmt_int(overview.get("products")), f"Qty / order {fmt_int(overview.get('qty_per_order'))}"),
        metric_card("Repeat Revenue Share", fmt_pct(overview.get("repeat_buyer_revenue_share")), "재구매자 매출 비중"),
        metric_card("Lines / Order", fmt_int(overview.get("line_per_order")), "주문당 라인 수"),
    ])

    summary_html = "".join(f"<li class='leading-7 text-sm font-bold text-slate-600'>{escape(x)}</li>" for x in summary_lines)

    html = f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>CSK E-COMM | Purchase Pattern</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200;400;600;800&display=swap');</style>
  {REPORT_PATCH_CSS}
</head>
<body class="bg-slate-50 text-slate-900 report-body">
  <div class="w-full max-w-none px-5 py-6 xl:px-8 2xl:px-10">
    <div class="flex flex-wrap items-center justify-between gap-3">
      <div class="flex flex-wrap items-center gap-3">
        <div class="text-2xl font-black">Purchase Pattern</div>
        <div class="rounded-full bg-slate-900 px-3 py-1 text-xs font-extrabold text-white">ALL PURCHASE DATA</div>
        <div class="text-sm font-semibold text-slate-500">{escape(period_label)} · source: {escape(source_table)}</div>
      </div>
      <div class="flex items-center gap-2">
        <a href="data/summary.json" class="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm font-extrabold hover:bg-slate-50">summary.json</a>
      </div>
    </div>

    <div class="mt-6 grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-6">{kpis}</div>

    <div class="report-card mt-6 rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm">
      <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Executive Summary</div>
      <ul class="mt-3 list-disc pl-5">{summary_html}</ul>
    </div>

    <div class="mt-6 grid grid-cols-1 gap-5 xl:grid-cols-2">
      <div class="report-card rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm">
        <div class="mb-4 flex items-center justify-between"><div><div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Monthly Revenue</div><div class="text-sm text-slate-400">월별 매출 흐름</div></div></div>
        {bar_list(monthly.sort_values('month') if not monthly.empty else monthly, 'month', 'revenue', 'krw', 18)}
      </div>
      <div class="report-card rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm">
        <div class="mb-4"><div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Top Categories</div><div class="text-sm text-slate-400">카테고리별 매출 TOP</div></div>
        {bar_list(top_categories, 'category', 'revenue', 'krw', 12)}
      </div>
    </div>

    <div class="report-card mt-6 rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm">
      <div class="flex flex-wrap items-center justify-between gap-3">
        <div><div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Top Products</div><div class="text-sm text-slate-400">상품 이미지는 엑셀/BQ/공식몰 크롤링 순으로 매칭</div></div>
        <div class="rounded-full border border-slate-200 bg-white px-3 py-1 text-xs font-black text-slate-500">{len(top_products)} products</div>
      </div>
      <div class="mt-4 grid grid-cols-2 gap-4 md:grid-cols-3 xl:grid-cols-6">{product_cards(top_products, 18)}</div>
    </div>

    <div class="mt-6 grid grid-cols-1 gap-5 xl:grid-cols-2">
      <div class="report-card rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm">
        <div class="mb-3 text-xs font-extrabold tracking-widest text-slate-500 uppercase">Member Segments</div>
        {simple_table(segments, [('frequency_segment','Frequency','text'),('recency_segment','Recency','text'),('buyers','Buyers','int'),('orders','Orders','int'),('revenue','Revenue','krw'),('revenue_per_buyer','Rev/Buyer','krw')], 16)}
      </div>
      <div class="report-card rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm">
        <div class="mb-3 text-xs font-extrabold tracking-widest text-slate-500 uppercase">Basket Size</div>
        {simple_table(basket, [('sku_bucket','SKU Bucket','text'),('qty_bucket','Qty Bucket','text'),('orders','Orders','int'),('buyers','Buyers','int'),('quantity','Qty','int'),('revenue','Revenue','krw'),('aov','AOV','krw')], 16)}
      </div>
    </div>

    <div class="mt-6 grid grid-cols-1 gap-5 xl:grid-cols-2">
      <div class="report-card rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm">
        <div class="mb-3 text-xs font-extrabold tracking-widest text-slate-500 uppercase">Purchase Interval</div>
        {simple_table(intervals, [('interval_bucket','Interval','text'),('repeat_orders','Repeat Orders','int'),('repeat_buyers','Repeat Buyers','int'),('avg_days_since_prev_order','Avg Days','int'),('median_days_since_prev_order','Median Days','int'),('revenue','Revenue','krw')], 12)}
      </div>
      <div class="report-card rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm">
        <div class="mb-3 text-xs font-extrabold tracking-widest text-slate-500 uppercase">Coupon / Point / Promo</div>
        {simple_table(coupon, [('coupon_flag','Coupon','text'),('point_flag','Point','text'),('promotion_flag','Promo','text'),('orders','Orders','int'),('revenue','Revenue','krw'),('coupon_amount','Coupon Amt','krw'),('aov','AOV','krw')], 12)}
      </div>
    </div>

    <div class="mt-6 grid grid-cols-1 gap-5 xl:grid-cols-2">
      <div class="report-card rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm">
        <div class="mb-3 text-xs font-extrabold tracking-widest text-slate-500 uppercase">Weekday × Hour</div>
        {simple_table(weekday.sort_values('revenue', ascending=False) if not weekday.empty else weekday, [('weekday','Weekday','text'),('order_hour','Hour','int'),('orders','Orders','int'),('buyers','Buyers','int'),('quantity','Qty','int'),('revenue','Revenue','krw'),('aov','AOV','krw')], 20)}
      </div>
      <div class="report-card rounded-2xl border border-slate-200 bg-white/80 p-5 shadow-sm">
        <div class="mb-3 text-xs font-extrabold tracking-widest text-slate-500 uppercase">Size / Color</div>
        {simple_table(size_color, [('product_code','SKU','text'),('product_name_kor','Product','text'),('product_size','Size','text'),('product_color','Color','text'),('orders','Orders','int'),('quantity','Qty','int'),('revenue','Revenue','krw')], 20)}
      </div>
    </div>

    <div class="mt-8 pb-8 text-xs font-bold text-slate-400">Generated at {dt.datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S KST')}</div>
  </div>
</body>
</html>
"""
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
    summary = {
        "generated_at_kst": dt.datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "source_table": source_table,
        "period_label": period_label,
        "overview": df_to_records(results.get("overview", pd.DataFrame()), 1)[0] if not results.get("overview", pd.DataFrame()).empty else {},
        "summary": summary_lines,
        "top_products": df_to_records(results.get("top_products", pd.DataFrame()), 50),
        "top_categories": df_to_records(results.get("top_categories", pd.DataFrame()), 50),
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

    html = render_html(results, out_dir, source_table, period_label, summary_lines)
    html_path = out_dir / "index.html"
    html_path.write_text(html, encoding="utf-8")
    log(f"Wrote HTML: {html_path}")


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Purchase pattern static HTML report from BigQuery order mart")
    p.add_argument("--project", default=getenv("BQ_PROJECT", "columbia-ga4"))
    p.add_argument("--location", default=getenv("BQ_LOCATION", "asia-northeast3"))
    p.add_argument("--raw-dataset", default=getenv("BQ_RAW_DATASET", "crm_raw"))
    p.add_argument("--source-table", default=getenv("BQ_ORDER_PRODUCT_TABLE", DEFAULT_SOURCE_TABLE))
    p.add_argument("--start-date", default=getenv("PURCHASE_PATTERN_START_DATE", ""), help="Optional YYYY-MM-DD")
    p.add_argument("--end-date", default=getenv("PURCHASE_PATTERN_END_DATE", ""), help="Optional YYYY-MM-DD")
    p.add_argument("--out-dir", default=getenv("PURCHASE_PATTERN_OUT_DIR", str(DEFAULT_OUT_DIR)))
    p.add_argument("--top-limit", type=int, default=int(getenv("PURCHASE_PATTERN_TOP_LIMIT", "500")))
    p.add_argument("--image-xlsx", default=getenv("PURCHASE_PATTERN_IMAGE_XLSX", getenv("DAILY_DIGEST_IMAGE_XLS_PATH", "상품코드별 이미지.xlsx")))
    p.add_argument("--crawl-images", action="store_true", default=getenv("PURCHASE_PATTERN_CRAWL_IMAGES", "false").lower() in {"1", "true", "yes", "y"})
    p.add_argument("--download-images", action="store_true", default=getenv("PURCHASE_PATTERN_DOWNLOAD_IMAGES", "false").lower() in {"1", "true", "yes", "y"})
    p.add_argument("--max-crawl", type=int, default=int(getenv("PURCHASE_PATTERN_MAX_CRAWL", "80")))
    p.add_argument("--placeholder-img", default=getenv("PLACEHOLDER_IMG", getenv("PURCHASE_PATTERN_PLACEHOLDER_IMG", "")))
    return p


def main() -> int:
    args = build_arg_parser().parse_args()
    try:
        setup_google_credentials()
        source_table = normalize_table_id(args.source_table, args.project, args.raw_dataset)
        out_dir = Path(args.out_dir)
        period_label = f"{args.start_date or 'ALL'} ~ {args.end_date or 'ALL'}"

        log("=" * 80)
        log("Columbia Purchase Pattern HTML Report")
        log(f"source_table={source_table}")
        log(f"period={period_label}")
        log(f"out_dir={out_dir}")
        log(f"crawl_images={args.crawl_images}, download_images={args.download_images}, max_crawl={args.max_crawl}")
        log("=" * 80)

        client = bq_client(args.project, args.location)
        columns = get_table_columns(client, source_table, args.location)
        log(f"Source columns detected: {len(columns):,}")

        queries = build_queries(source_table, columns, args.start_date, args.end_date, args.top_limit)
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

        write_outputs(results, out_dir, source_table, period_label, image_log)
        log("DONE")
        return 0
    except Exception as e:
        log(f"ERROR: {type(e).__name__}: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
