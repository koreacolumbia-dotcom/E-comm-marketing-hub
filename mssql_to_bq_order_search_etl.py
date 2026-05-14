#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Columbia CRM / Order Product AI Agent ETL - FINAL

What this file does
-------------------
1) MSSQL -> BigQuery raw/staging sync
   - TB_Member
   - TB_Order
   - TB_Product
   - TB_Statistics_Google
   - enhanced TB_Order + TB_OrderProduct mart

2) Enhanced order-product mart for AI Agent analysis
   - Order / order product
   - Product master
   - Category manager + category product mapping
   - Size stock / sold-out signal
   - Latest order status signal
   - Point / mileage signal
   - Cart signal
   - Coupon / point / promotion helper flags

3) Safety / stability
   - No hardcoded DB credentials
   - Supports GOOGLE_APPLICATION_CREDENTIALS or GOOGLE_SA_JSON_B64
   - Dynamically checks optional MSSQL tables/columns before building SQL
   - Falls back to NULL/0 fields when optional columns do not exist
   - BigQuery load uses autodetect to tolerate evolving columns

Recommended schedule
--------------------
- Local Windows Task Scheduler: 06:10~06:30 KST
- Then run GitHub Actions dashboard/report build after this ETL

Required env
------------
Google:
- GOOGLE_APPLICATION_CREDENTIALS or GOOGLE_SA_JSON_B64

MSSQL:
- MSSQL_SERVER or MSSQL_HOST
- MSSQL_DATABASE
- MSSQL_USERNAME / MSSQL_PASSWORD
  or MSSQL_TRUSTED_CONNECTION=yes

BigQuery:
- BQ_PROJECT=columbia-ga4
- BQ_LOCATION=asia-northeast3
- BQ_RAW_DATASET=crm_raw
- BQ_MART_DATASET=crm_mart

Examples
--------
python columbia_ai_agent_crm_order_etl_final.py --mode order_mart
python columbia_ai_agent_crm_order_etl_final.py --mode crm_raw
python columbia_ai_agent_crm_order_etl_final.py --mode all
python columbia_ai_agent_crm_order_etl_final.py --mode order_mart --days-back 30 --dry-run-sql
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
import re
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
import pyodbc
from google.cloud import bigquery


KST = dt.timezone(dt.timedelta(hours=9))
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)


# =========================================================
# 1) Logging / env helpers
# =========================================================
def log(msg: str) -> None:
    now = dt.datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST")
    print(f"[{now}] {msg}", flush=True)


def getenv(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def getenv_first(names: list[str], default: str = "") -> str:
    for name in names:
        value = getenv(name)
        if value:
            return value
    return default


def parse_statuses(value: str) -> str:
    parts = []
    for x in value.split(","):
        x = x.strip()
        if not x:
            continue
        if not re.fullmatch(r"-?\d+", x):
            raise ValueError(f"Invalid status value: {x}")
        parts.append(x)
    if not parts:
        raise ValueError("include_statuses cannot be empty.")
    return ",".join(parts)


def kst_today_str() -> str:
    return dt.datetime.now(KST).date().isoformat()


# =========================================================
# 2) Credentials / connections
# =========================================================
def setup_google_credentials() -> None:
    """
    GitHub/Local both supported:
    1) GOOGLE_APPLICATION_CREDENTIALS points to existing json file
    2) GOOGLE_SA_JSON_B64 contains base64 service account json
    """
    cred_path = getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if cred_path and Path(cred_path).exists():
        log(f"Using GOOGLE_APPLICATION_CREDENTIALS: {cred_path}")
        return

    b64 = getenv("GOOGLE_SA_JSON_B64")
    if b64:
        out = Path(getenv("GOOGLE_SA_JSON_OUT", "gcp_service_account.json"))
        out.write_bytes(base64.b64decode(b64))
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(out.resolve())
        log(f"Wrote service account json: {out.resolve()}")
        return

    raise RuntimeError(
        "Google credentials not found. Set GOOGLE_APPLICATION_CREDENTIALS or GOOGLE_SA_JSON_B64."
    )


def make_mssql_connection() -> pyodbc.Connection:
    driver = getenv("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server")
    server = getenv_first(["MSSQL_SERVER", "MSSQL_HOST"])
    port = getenv("MSSQL_PORT")
    database = getenv("MSSQL_DATABASE")
    username = getenv_first(["MSSQL_USERNAME", "MSSQL_USER"])
    password = getenv_first(["MSSQL_PASSWORD", "MSSQL_PWD"])
    trusted = getenv("MSSQL_TRUSTED_CONNECTION", "no").lower() in ("1", "true", "yes", "y")

    if not server or not database:
        raise RuntimeError("MSSQL_SERVER/MSSQL_HOST and MSSQL_DATABASE are required.")

    server_part = f"{server},{port}" if port and "," not in server else server

    if trusted:
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server_part};"
            f"DATABASE={database};"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
        )
    else:
        if not username or not password:
            raise RuntimeError(
                "MSSQL_USERNAME/MSSQL_PASSWORD are required unless MSSQL_TRUSTED_CONNECTION=yes."
            )
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server_part};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            "TrustServerCertificate=yes;"
        )

    log(f"Connecting to SQL Server: {server_part} / {database} / driver={driver}")
    return pyodbc.connect(conn_str, timeout=int(getenv("MSSQL_TIMEOUT_SEC", "30")))


def make_bq_client(project: Optional[str] = None, location: Optional[str] = None) -> bigquery.Client:
    project = project or getenv("BQ_PROJECT", "columbia-ga4")
    location = location or getenv("BQ_LOCATION", "asia-northeast3")
    return bigquery.Client(project=project, location=location)


# =========================================================
# 3) MSSQL metadata helpers
# =========================================================
def get_table_columns(conn: pyodbc.Connection, table_name: str, schema: str = "dbo") -> set[str]:
    sql = """
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    """
    df = pd.read_sql(sql, conn, params=[schema, table_name])
    return set(df["COLUMN_NAME"].astype(str).tolist())


def table_exists(conn: pyodbc.Connection, table_name: str, schema: str = "dbo") -> bool:
    sql = """
    SELECT 1 AS exists_yn
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    """
    df = pd.read_sql(sql, conn, params=[schema, table_name])
    return not df.empty


def col(
    table_cols: dict[str, set[str]],
    table: str,
    alias: str,
    column: str,
    sql_type: str,
    output: str,
    default: str = "NULL",
    expr: Optional[str] = None,
) -> str:
    """
    Safe SELECT expression builder.
    If a column is missing, returns CAST(default AS type) AS output.
    """
    if column in table_cols.get(table, set()):
        raw = expr if expr else f"{alias}.{column}"
        return f"CAST({raw} AS {sql_type}) AS {output}"
    return f"CAST({default} AS {sql_type}) AS {output}"


def ncol(
    table_cols: dict[str, set[str]],
    table: str,
    alias: str,
    column: str,
    output: str,
    default: str = "0",
    cast_type: str = "bigint",
) -> str:
    if column in table_cols.get(table, set()):
        return f"CAST(ISNULL({alias}.{column}, 0) AS {cast_type}) AS {output}"
    return f"CAST({default} AS {cast_type}) AS {output}"


# =========================================================
# 4) Raw CRM queries from sync_crm style
# =========================================================
def build_member_query(conn: pyodbc.Connection, sync_start_date: str) -> str:
    cols = get_table_columns(conn, "TB_Member")
    wanted = [
        "MemberNo", "MemberID", "MemberBirthday", "MemberGender",
        "MemberRegdate", "MemberUpdateRegdate", "MemberLogindate",
        "MemberEnable", "MemberDormancy", "MemberDormancyDate",
        "MemberIsSMS", "MemberIsMaillinglist", "MemberIsAlimTalk",
        "MemberIsSMSDate", "MemberIsMaillingDate", "MemberIsAlimTalkDate",
        "MemberJoinDevice", "MemberGradeNo", "MemberGradeRegdate",
        "MemberPoint", "MemberOrderCount", "MemberOrderPrice", "MemberOrderDate",
        "MemberTel1", "MemberTel2", "MemberCompanyTel",
        "NaverConnect", "FacebookConnect", "KakaoConnect", "googleconnect",
        "privacyYear", "privacyYearDate",
    ]
    select_cols = [c for c in wanted if c in cols]
    if not select_cols:
        raise RuntimeError("No usable columns found in dbo.TB_Member.")
    return f"""
    SELECT {", ".join(select_cols)}
    FROM dbo.TB_Member
    WHERE MemberRegdate IS NULL OR CAST(MemberRegdate AS date) >= '{sync_start_date}'
    """


def build_order_header_query(conn: pyodbc.Connection, sync_start_date: str) -> str:
    cols = get_table_columns(conn, "TB_Order")
    wanted = [
        "OrderNo", "MemberID", "OrderRegdate", "CodeOrderStatusNo",
        "OrderTotalPay", "OrderTotalPrice", "OrderUseCouponPrice",
        "OrderUsePoint", "OrderCancelPrice", "OrderSaleCategory",
        "OrderSalePrice", "OrderDeliveryFee", "OrderCancelDate",
    ]
    select_exprs = []
    for c in wanted:
        if c in cols:
            if c == "CodeOrderStatusNo":
                select_exprs.append("CodeOrderStatusNo AS OrderStatusNo")
            else:
                select_exprs.append(c)
    if not select_exprs:
        raise RuntimeError("No usable columns found in dbo.TB_Order.")
    return f"""
    SELECT {", ".join(select_exprs)}
    FROM dbo.TB_Order
    WHERE CAST(OrderRegdate AS date) >= '{sync_start_date}'
    """


def build_product_query(conn: pyodbc.Connection) -> str:
    cols = get_table_columns(conn, "TB_Product")
    wanted = [
        "ProductNo", "ProductCode", "BrandCode", "CodeSexNo", "ProductStyle",
        "ProductName", "ProductName_Kor", "ProductPrice", "ProductPrice_Custom",
        "ProductColor", "ProductRegdate", "ProductUpdate", "ProductYear",
        "ProductSeason", "ProductMetaTag", "ProductClickNo", "CouponTypeNo",
        "ProductKindNo", "ProductGubun", "ProductFit", "ProductTag",
        "ProductStockView",
    ]
    select_cols = [c for c in wanted if c in cols]
    if not select_cols:
        raise RuntimeError("No usable columns found in dbo.TB_Product.")
    return f"SELECT {', '.join(select_cols)} FROM dbo.TB_Product"


def build_traffic_daily_query(sync_start_date: str) -> str:
    return f"""
    SELECT
        CAST(StatisticsDate AS date) AS StatisticsDate,
        SUM(ISNULL(StatisticsPV, 0)) AS StatisticsPV,
        SUM(ISNULL(StatisticsSessions, 0)) AS StatisticsSessions
    FROM dbo.TB_Statistics_Google
    WHERE CAST(StatisticsDate AS date) >= '{sync_start_date}'
    GROUP BY CAST(StatisticsDate AS date)
    ORDER BY CAST(StatisticsDate AS date)
    """


# =========================================================
# 5) Enhanced AI Agent order-product query
# =========================================================
def build_ai_order_product_query(
    conn: pyodbc.Connection,
    days_back: int,
    include_statuses: str,
    cart_days_back: int = 180,
) -> str:
    """
    Dynamic enhanced order product query.
    Optional tables/columns are checked before reference to reduce runtime errors.
    """
    include_statuses = parse_statuses(include_statuses)

    needed_tables = [
        "TB_Order", "TB_OrderProduct", "TB_Product", "TB_CategoryManagerProduct",
        "TB_CategoryManager", "TB_ProductSizeStock", "TB_OrderStatus",
        "TB_GetMemberPoint", "TB_GetMemberPointDetail", "TB_Cart",
    ]
    exists = {t: table_exists(conn, t) for t in needed_tables}
    cols = {t: get_table_columns(conn, t) if exists[t] else set() for t in needed_tables}

    if not exists["TB_Order"] or not exists["TB_OrderProduct"]:
        raise RuntimeError("dbo.TB_Order and dbo.TB_OrderProduct are required.")

    # ---------- optional CTEs ----------
    if exists["TB_OrderStatus"]:
        los_cte = """
latest_order_status AS (
    SELECT
        os.OrderNo,
        os.MemberID,
        os.CodeOrderStatusNo,
        os.OrderStatusNo,
        os.OrderProductNo,
        os.OrderStatusRegdate,
        ROW_NUMBER() OVER (
            PARTITION BY os.OrderNo, ISNULL(os.OrderProductNo, 0)
            ORDER BY os.OrderStatusRegdate DESC
        ) AS rn
    FROM dbo.TB_OrderStatus os
    WHERE os.OrderStatusRegdate >= @start_date
),
"""
        los_join = """
LEFT JOIN latest_order_status los
    ON los.OrderNo = o.OrderNo
   AND (los.OrderProductNo = op.OrderProductNo OR ISNULL(los.OrderProductNo, 0) = 0)
   AND los.rn = 1
"""
        los_select = """
    CAST(los.CodeOrderStatusNo AS int) AS latest_status_code,
    CAST(los.OrderStatusNo AS int) AS latest_order_status_no,
    los.OrderStatusRegdate AS latest_status_datetime,
"""
    else:
        los_cte = ""
        los_join = ""
        los_select = """
    CAST(NULL AS int) AS latest_status_code,
    CAST(NULL AS int) AS latest_order_status_no,
    CAST(NULL AS datetime) AS latest_status_datetime,
"""

    if exists["TB_GetMemberPoint"]:
        point_cte = """
point_by_order AS (
    SELECT
        CAST(MemberID AS varchar(255)) AS MemberID,
        CAST(OrderNo AS varchar(255)) AS OrderNo,
        SUM(ISNULL(GetPoint, 0)) AS order_get_point,
        MAX(CurrentPoint) AS current_point_after_order,
        MAX(PointRegDate) AS last_point_regdate
    FROM dbo.TB_GetMemberPoint
    WHERE PointRegDate >= @start_date
    GROUP BY CAST(MemberID AS varchar(255)), CAST(OrderNo AS varchar(255))
),
"""
        point_join = "LEFT JOIN point_by_order po ON po.OrderNo = o.OrderNo AND po.MemberID = o.MemberID\n"
        point_select = """
    ISNULL(po.order_get_point, 0) AS order_get_point,
    ISNULL(po.current_point_after_order, 0) AS current_point_after_order,
    po.last_point_regdate,
"""
    else:
        point_cte = ""
        point_join = ""
        point_select = """
    CAST(0 AS bigint) AS order_get_point,
    CAST(0 AS bigint) AS current_point_after_order,
    CAST(NULL AS datetime) AS last_point_regdate,
"""

    if exists["TB_GetMemberPointDetail"]:
        point_detail_cte = """
point_detail_by_order AS (
    SELECT
        CAST(MemberID AS varchar(255)) AS MemberID,
        CAST(OrderNo AS varchar(255)) AS OrderNo,
        SUM(ISNULL(MemberGetPoint, 0)) AS detail_get_point,
        SUM(ISNULL(MemberUsePoint, 0)) AS detail_use_point,
        MAX(RegDate) AS last_point_detail_regdate
    FROM dbo.TB_GetMemberPointDetail
    WHERE RegDate >= @start_date
    GROUP BY CAST(MemberID AS varchar(255)), CAST(OrderNo AS varchar(255))
),
"""
        point_detail_join = "LEFT JOIN point_detail_by_order pd ON pd.OrderNo = o.OrderNo AND pd.MemberID = o.MemberID\n"
        point_detail_select = """
    ISNULL(pd.detail_get_point, 0) AS detail_get_point,
    ISNULL(pd.detail_use_point, 0) AS detail_use_point,
    pd.last_point_detail_regdate,
"""
        detail_use_ref = "ISNULL(pd.detail_use_point, 0)"
    else:
        point_detail_cte = ""
        point_detail_join = ""
        point_detail_select = """
    CAST(0 AS bigint) AS detail_get_point,
    CAST(0 AS bigint) AS detail_use_point,
    CAST(NULL AS datetime) AS last_point_detail_regdate,
"""
        detail_use_ref = "0"

    if exists["TB_ProductSizeStock"]:
        stock_name_expr = "MAX(ProductName) AS stock_product_name," if "ProductName" in cols["TB_ProductSizeStock"] else "CAST(NULL AS varchar(300)) AS stock_product_name,"
        stock_limit_expr = "MAX(ISNULL(StockLimit, 0)) AS stock_limit," if "StockLimit" in cols["TB_ProductSizeStock"] else "CAST(0 AS bigint) AS stock_limit,"
        stock_reg_expr = "MAX(RegDate) AS stock_regdate," if "RegDate" in cols["TB_ProductSizeStock"] else "CAST(NULL AS datetime) AS stock_regdate,"
        size_sort_expr = "MAX(SizeSort) AS size_sort" if "SizeSort" in cols["TB_ProductSizeStock"] else "CAST(NULL AS int) AS size_sort"
        stock_qty_col = "ProductStock" if "ProductStock" in cols["TB_ProductSizeStock"] else None
        if stock_qty_col:
            stock_cte = f"""
stock_by_product_size AS (
    SELECT
        ProductNo,
        UPPER(LTRIM(RTRIM(CAST(ProductCode AS varchar(100))))) AS ProductCode,
        LTRIM(RTRIM(CAST(ProductSize AS varchar(50)))) AS ProductSize,
        BrandCode,
        {stock_name_expr}
        SUM(ISNULL({stock_qty_col}, 0)) AS size_stock_qty,
        {stock_limit_expr}
        {stock_reg_expr}
        {size_sort_expr}
    FROM dbo.TB_ProductSizeStock
    GROUP BY
        ProductNo,
        UPPER(LTRIM(RTRIM(CAST(ProductCode AS varchar(100))))),
        LTRIM(RTRIM(CAST(ProductSize AS varchar(50)))),
        BrandCode
),
"""
            stock_join = """
LEFT JOIN stock_by_product_size st
    ON (st.ProductNo = op.ProductNo OR st.ProductCode = UPPER(LTRIM(RTRIM(CAST(op.ProductCode AS varchar(100))))))
   AND ISNULL(st.ProductSize, '') = ISNULL(LTRIM(RTRIM(CAST(op.ProductSize AS varchar(50)))), '')
"""
            stock_select = """
    ISNULL(st.size_stock_qty, 0) AS size_stock_qty,
    ISNULL(st.stock_limit, 0) AS stock_limit,
    CASE WHEN ISNULL(st.size_stock_qty, 0) <= 0 THEN 1 ELSE 0 END AS is_soldout_size,
    CASE WHEN ISNULL(st.size_stock_qty, 0) <= ISNULL(st.stock_limit, 0) THEN 1 ELSE 0 END AS is_low_stock_size,
    st.stock_regdate,
    st.size_sort,
"""
        else:
            stock_cte = ""
            stock_join = ""
            stock_select = """
    CAST(0 AS bigint) AS size_stock_qty,
    CAST(0 AS bigint) AS stock_limit,
    CAST(0 AS int) AS is_soldout_size,
    CAST(0 AS int) AS is_low_stock_size,
    CAST(NULL AS datetime) AS stock_regdate,
    CAST(NULL AS int) AS size_sort,
"""
    else:
        stock_cte = ""
        stock_join = ""
        stock_select = """
    CAST(0 AS bigint) AS size_stock_qty,
    CAST(0 AS bigint) AS stock_limit,
    CAST(0 AS int) AS is_soldout_size,
    CAST(0 AS int) AS is_low_stock_size,
    CAST(NULL AS datetime) AS stock_regdate,
    CAST(NULL AS int) AS size_sort,
"""

    if exists["TB_CategoryManagerProduct"]:
        cm_join = ""
        cm_select = """
        CAST(NULL AS int) AS category_no,
        CAST(NULL AS int) AS category_ref,
        CAST(NULL AS int) AS category_level,
        CAST(NULL AS int) AS category_priority,
        CAST(NULL AS varchar(100)) AS category_code,
        CAST(NULL AS varchar(300)) AS category_title,
        CAST(NULL AS varchar(300)) AS category_title_kr,
        CAST(NULL AS bit) AS category_is_view,
        CAST(NULL AS varchar(300)) AS category_page_title,
        CAST(NULL AS varchar(300)) AS relation_category
"""
        if exists["TB_CategoryManager"]:
            cm_join = "LEFT JOIN dbo.TB_CategoryManager cm ON cmp.CategoryManagerNo = cm.No"
            cm_select = """
        cm.No AS category_no,
        cm.Ref AS category_ref,
        cm.Level AS category_level,
        cm.Priority AS category_priority,
        cm.Code AS category_code,
        cm.Title AS category_title,
        cm.TitleKr AS category_title_kr,
        cm.IsView AS category_is_view,
        cm.PageTitle AS category_page_title,
        cm.RelationCategory AS relation_category
"""
        category_cte = f"""
category_map AS (
    SELECT
        cmp.CategoryManagerNo,
        cmp.ProductNo,
        cmp.BrandCode,
        cmp.ProductStyle,
        UPPER(LTRIM(RTRIM(CAST(cmp.ProductCode AS varchar(100))))) AS ProductCode,
        cmp.ProductPriority,
        cmp.Mdpick,
        cmp.MdpickPriority,
        cmp.MdpickDepth2,
        cmp.MdpickPriorityDepth2,
        cmp.CategoryManagerRegdate,
        cmp.ProductPriority1,
        cmp.ProductPriority2,
        {cm_select}
    FROM dbo.TB_CategoryManagerProduct cmp
    {cm_join}
),
"""
        category_apply = """
OUTER APPLY (
    SELECT TOP 1 cm.*
    FROM category_map cm
    WHERE cm.ProductNo = op.ProductNo
       OR UPPER(LTRIM(RTRIM(CAST(cm.ProductCode AS varchar(100))))) = UPPER(LTRIM(RTRIM(CAST(op.ProductCode AS varchar(100)))))
    ORDER BY
        CASE WHEN cm.ProductNo = op.ProductNo THEN 0 ELSE 1 END,
        cm.category_priority,
        cm.CategoryManagerNo
) cat
"""
        category_select = """
    cat.CategoryManagerNo AS category_manager_no,
    cat.category_no,
    cat.category_ref,
    cat.category_level,
    cat.category_priority,
    cat.category_code,
    cat.category_title,
    cat.category_title_kr,
    cat.category_is_view,
    cat.category_page_title,
    cat.relation_category,
    cat.ProductPriority AS category_product_priority,
    cat.Mdpick AS mdpick,
    cat.MdpickPriority AS mdpick_priority,
    cat.MdpickDepth2 AS mdpick_depth2,
    cat.MdpickPriorityDepth2 AS mdpick_priority_depth2,
    cat.ProductPriority1 AS product_priority1,
    cat.ProductPriority2 AS product_priority2,
"""
    else:
        category_cte = ""
        category_apply = ""
        category_select = """
    CAST(NULL AS int) AS category_manager_no,
    CAST(NULL AS int) AS category_no,
    CAST(NULL AS int) AS category_ref,
    CAST(NULL AS int) AS category_level,
    CAST(NULL AS int) AS category_priority,
    CAST(NULL AS varchar(100)) AS category_code,
    CAST(NULL AS varchar(300)) AS category_title,
    CAST(NULL AS varchar(300)) AS category_title_kr,
    CAST(NULL AS bit) AS category_is_view,
    CAST(NULL AS varchar(300)) AS category_page_title,
    CAST(NULL AS varchar(300)) AS relation_category,
    CAST(NULL AS int) AS category_product_priority,
    CAST(NULL AS int) AS mdpick,
    CAST(NULL AS int) AS mdpick_priority,
    CAST(NULL AS int) AS mdpick_depth2,
    CAST(NULL AS int) AS mdpick_priority_depth2,
    CAST(NULL AS int) AS product_priority1,
    CAST(NULL AS int) AS product_priority2,
"""

    if exists["TB_Cart"] and {"MemberID", "ProductCode"}.issubset(cols["TB_Cart"]):
        cart_qty = "SUM(ISNULL(c.ProductQuantity, 0)) AS cart_qty_180d," if "ProductQuantity" in cols["TB_Cart"] else "CAST(0 AS bigint) AS cart_qty_180d,"
        cart_price = "SUM(ISNULL(c.ProductPrice, 0)) AS cart_product_price_sum_180d," if "ProductPrice" in cols["TB_Cart"] else "CAST(0 AS bigint) AS cart_product_price_sum_180d,"
        cart_coupon = "SUM(ISNULL(c.OrderUseCouponTotalPrice, 0)) AS cart_coupon_total_180d," if "OrderUseCouponTotalPrice" in cols["TB_Cart"] else "CAST(0 AS bigint) AS cart_coupon_total_180d,"
        cart_mileage = "SUM(ISNULL(c.OrderUseMileage, 0)) AS cart_mileage_total_180d," if "OrderUseMileage" in cols["TB_Cart"] else "CAST(0 AS bigint) AS cart_mileage_total_180d,"
        cart_promo = "SUM(ISNULL(c.PromotionSalePrice, 0)) AS cart_promotion_sale_total_180d," if "PromotionSalePrice" in cols["TB_Cart"] else "CAST(0 AS bigint) AS cart_promotion_sale_total_180d,"
        cart_date_col = "CartRegdate" if "CartRegdate" in cols["TB_Cart"] else None
        cart_where = f"WHERE c.{cart_date_col} >= DATEADD(day, -{int(cart_days_back)}, CONVERT(date, GETDATE()))" if cart_date_col else ""
        cart_max_date = f"MAX(c.{cart_date_col}) AS last_cart_regdate" if cart_date_col else "CAST(NULL AS datetime) AS last_cart_regdate"
        cart_cte = f"""
cart_recent AS (
    SELECT
        c.MemberID,
        UPPER(LTRIM(RTRIM(CAST(c.ProductCode AS varchar(100))))) AS ProductCode,
        COUNT(*) AS cart_add_count_180d,
        {cart_qty}
        {cart_price}
        {cart_coupon}
        {cart_mileage}
        {cart_promo}
        {cart_max_date}
    FROM dbo.TB_Cart c
    {cart_where}
    GROUP BY c.MemberID, UPPER(LTRIM(RTRIM(CAST(c.ProductCode AS varchar(100)))))
),
"""
        cart_join = """
LEFT JOIN cart_recent cr
    ON cr.MemberID = o.MemberID
   AND cr.ProductCode = UPPER(LTRIM(RTRIM(CAST(op.ProductCode AS varchar(100)))))
"""
        cart_select = """
    ISNULL(cr.cart_add_count_180d, 0) AS cart_add_count_180d,
    ISNULL(cr.cart_qty_180d, 0) AS cart_qty_180d,
    ISNULL(cr.cart_product_price_sum_180d, 0) AS cart_product_price_sum_180d,
    ISNULL(cr.cart_coupon_total_180d, 0) AS cart_coupon_total_180d,
    ISNULL(cr.cart_mileage_total_180d, 0) AS cart_mileage_total_180d,
    ISNULL(cr.cart_promotion_sale_total_180d, 0) AS cart_promotion_sale_total_180d,
    cr.last_cart_regdate,
"""
    else:
        cart_cte = ""
        cart_join = ""
        cart_select = """
    CAST(0 AS bigint) AS cart_add_count_180d,
    CAST(0 AS bigint) AS cart_qty_180d,
    CAST(0 AS bigint) AS cart_product_price_sum_180d,
    CAST(0 AS bigint) AS cart_coupon_total_180d,
    CAST(0 AS bigint) AS cart_mileage_total_180d,
    CAST(0 AS bigint) AS cart_promotion_sale_total_180d,
    CAST(NULL AS datetime) AS last_cart_regdate,
"""

    ctes = "".join([los_cte, point_cte, point_detail_cte, stock_cte, category_cte, cart_cte]).rstrip().rstrip(",")

    with_clause = f"WITH {ctes}" if ctes.strip() else ""

    # ---------- product columns ----------
    p_select = []
    p_select.append(col(cols, "TB_Product", "p", "ProductStyle", "varchar(100)", "product_style"))
    p_select.append(col(cols, "TB_Product", "p", "ProductName", "varchar(300)", "product_name"))
    p_select.append(col(cols, "TB_Product", "p", "ProductName_Kor", "varchar(300)", "product_name_kor"))
    p_select.append(col(cols, "TB_Product", "p", "CodeSexNo", "int", "code_sex_no", "0"))
    p_select.append("""
    CASE
        WHEN p.CodeSexNo = 1 THEN 'MALE'
        WHEN p.CodeSexNo = 2 THEN 'FEMALE'
        WHEN p.CodeSexNo = 3 THEN 'UNISEX'
        ELSE 'UNKNOWN'
    END AS sex_label""" if "CodeSexNo" in cols["TB_Product"] else "CAST('UNKNOWN' AS varchar(20)) AS sex_label")
    p_select.append(col(cols, "TB_Product", "p", "ProductColor", "varchar(100)", "master_product_color"))
    p_select.append(col(cols, "TB_Product", "p", "ProductMetaTag", "varchar(1000)", "product_meta_tag"))
    p_select.append(ncol(cols, "TB_Product", "p", "ProductPrice", "master_product_price"))
    p_select.append(ncol(cols, "TB_Product", "p", "ProductPrice_Custom", "master_consumer_price"))
    p_select.append(col(cols, "TB_Product", "p", "ProductYear", "varchar(20)", "product_year"))
    p_select.append(col(cols, "TB_Product", "p", "ProductSeason", "varchar(50)", "product_season"))
    p_select.append(col(cols, "TB_Product", "p", "ProductRegdate", "datetime", "product_regdate"))
    p_select.append(col(cols, "TB_Product", "p", "ProductUpdate", "datetime", "product_update_datetime"))
    p_select.append(ncol(cols, "TB_Product", "p", "ProductClickNo", "product_click_no"))
    p_select.append(col(cols, "TB_Product", "p", "CouponTypeNo", "int", "product_coupon_type_no", "0"))
    p_select.append(col(cols, "TB_Product", "p", "ProductKindNo", "int", "product_kind_no", "0"))
    p_select.append(col(cols, "TB_Product", "p", "ProductGubun", "int", "product_gubun", "0"))
    p_select.append(col(cols, "TB_Product", "p", "ProductFit", "int", "product_fit", "0"))
    p_select.append(col(cols, "TB_Product", "p", "ProductTag", "varchar(1000)", "product_tag"))

    # ---------- numeric optional order/product fields ----------
    op_product_price = ncol(cols, "TB_OrderProduct", "op", "ProductPrice", "product_price")
    op_order_product_price = ncol(cols, "TB_OrderProduct", "op", "OrderProductPrice", "order_product_price")
    op_erp = ncol(cols, "TB_OrderProduct", "op", "ErpPrice", "erp_revenue")
    op_erp_cancel = ncol(cols, "TB_OrderProduct", "op", "ErpCancelPrice", "erp_cancel_price")
    net_erp_expr = (
        "CAST(ISNULL(op.ErpPrice, 0) AS bigint) - CAST(ISNULL(op.ErpCancelPrice, 0) AS bigint) AS net_erp_revenue"
        if "ErpPrice" in cols["TB_OrderProduct"] and "ErpCancelPrice" in cols["TB_OrderProduct"]
        else "CAST(0 AS bigint) AS net_erp_revenue"
    )

    # Helper refs for flags
    erp_ref = "ISNULL(op.ErpPrice, 0)" if "ErpPrice" in cols["TB_OrderProduct"] else "0"
    erp_cancel_ref = "ISNULL(op.ErpCancelPrice, 0)" if "ErpCancelPrice" in cols["TB_OrderProduct"] else "0"
    order_coupon_ref = "ISNULL(o.OrderUseCouponPrice, 0)" if "OrderUseCouponPrice" in cols["TB_Order"] else "0"
    order_point_ref = "ISNULL(o.OrderUsePoint, 0)" if "OrderUsePoint" in cols["TB_Order"] else "0"
    op_mileage_ref = "ISNULL(op.OrderProductUseMileage, 0)" if "OrderProductUseMileage" in cols["TB_OrderProduct"] else "0"
    op_promo_sale_ref = "ISNULL(op.PromotionSalePrice, 0)" if "PromotionSalePrice" in cols["TB_OrderProduct"] else "0"
    op_promo_master_ref = "ISNULL(op.PromotionMasterNo, 0)" if "PromotionMasterNo" in cols["TB_OrderProduct"] else "0"
    op_promo_group_ref = "ISNULL(op.PromotionGroupNo, 0)" if "PromotionGroupNo" in cols["TB_OrderProduct"] else "0"

    product_select_sql = ",\n    ".join(p_select)

    query = f"""
DECLARE @days_back INT = {int(days_back)};
DECLARE @start_date DATE = DATEADD(day, -@days_back, CONVERT(date, GETDATE()));

{with_clause}

SELECT
    CONVERT(date, o.OrderRegdate) AS order_date,
    o.OrderRegdate AS order_datetime,
    o.OrderNo AS order_no,
    LTRIM(RTRIM(o.MemberID)) AS member_id,

    CAST(o.CodeOrderStatusNo AS int) AS order_status_no,
    CAST(op.CodeOrderStatusNo AS int) AS product_status_no,

{los_select}
    CAST(op.OrderProductNo AS int) AS order_product_no,
    CAST(op.BrandCode AS varchar(20)) AS brand_code,
    CAST(op.ProductNo AS int) AS product_no,
    UPPER(LTRIM(RTRIM(CAST(op.ProductCode AS varchar(100))))) AS product_code,
    CAST(op.ProductSize AS varchar(50)) AS product_size,
    CAST(op.ProductColor AS varchar(50)) AS product_color,

    {product_select_sql},

{category_select}
{stock_select}
    CAST(ISNULL(op.ProductQuantity, 0) AS bigint) AS purchase_qty,
    {op_product_price},
    {op_order_product_price},
    {op_erp},
    {op_erp_cancel},
    {net_erp_expr},

    {ncol(cols, "TB_Order", "o", "OrderTotalPay", "order_total_pay")},
    {ncol(cols, "TB_Order", "o", "OrderTotalPrice", "order_total_price")},
    {ncol(cols, "TB_Order", "o", "OrderSalePrice", "order_sale_price")},
    {ncol(cols, "TB_Order", "o", "OrderDeliveryFee", "order_delivery_fee")},

    {ncol(cols, "TB_Order", "o", "OrderUseCouponPrice", "order_use_coupon_price")},
    {ncol(cols, "TB_Order", "o", "OrderUsePoint", "order_use_point")},
    {ncol(cols, "TB_Order", "o", "OrderCancelPrice", "order_cancel_price")},

    {ncol(cols, "TB_OrderProduct", "op", "PromotionSalePrice", "product_promotion_sale_price")},
    {ncol(cols, "TB_OrderProduct", "op", "PromotionMasterNo", "promotion_master_no")},
    {ncol(cols, "TB_OrderProduct", "op", "PromotionQuantity", "promotion_quantity")},
    {ncol(cols, "TB_OrderProduct", "op", "PromotionGroupNo", "promotion_group_no")},

    {ncol(cols, "TB_OrderProduct", "op", "ProductGetPointRate", "product_get_point_rate")},
    {ncol(cols, "TB_OrderProduct", "op", "OrderProductUseMileage", "order_product_use_mileage")},

{point_select}
{point_detail_select}
{cart_select}
    {col(cols, "TB_Order", "o", "OrderSaleCategory", "varchar(100)", "order_sale_category")},

    {col(cols, "TB_OrderProduct", "op", "OrderRefundStatus", "int", "order_refund_status", "0")},
    {ncol(cols, "TB_OrderProduct", "op", "OrderRefundPrice", "order_refund_price")},
    {col(cols, "TB_OrderProduct", "op", "OrderProductIsSale", "int", "order_product_is_sale", "0")},
    {col(cols, "TB_OrderProduct", "op", "OrderIsNew", "int", "order_is_new", "0")},
    {col(cols, "TB_OrderProduct", "op", "OrderIsStaff", "int", "order_is_staff", "0")},

    {col(cols, "TB_Order", "o", "OrderCancelDate", "datetime", "order_cancel_datetime")},
    {col(cols, "TB_OrderProduct", "op", "ErpDate", "datetime", "erp_datetime")},
    {col(cols, "TB_OrderProduct", "op", "ErpCancelDate", "datetime", "erp_cancel_datetime")},
    {col(cols, "TB_OrderProduct", "op", "OrderProductRegdate", "datetime", "order_product_datetime")},
    {col(cols, "TB_OrderProduct", "op", "CartInDate", "datetime", "cart_in_datetime")},

    CASE WHEN {erp_ref} - {erp_cancel_ref} > 0 THEN 1 ELSE 0 END AS is_net_sales_line,
    CASE WHEN {order_coupon_ref} > 0 THEN 1 ELSE 0 END AS is_coupon_order,
    CASE WHEN {order_point_ref} > 0 OR {op_mileage_ref} > 0 OR {detail_use_ref} > 0 THEN 1 ELSE 0 END AS is_point_used_order,
    CASE WHEN {op_promo_sale_ref} > 0 OR {op_promo_master_ref} > 0 OR {op_promo_group_ref} > 0 THEN 1 ELSE 0 END AS is_promotion_line,
    CASE
        WHEN o.OrderSaleCategory = 'MO' THEN 'MOBILE'
        WHEN o.OrderSaleCategory = 'PC' THEN 'PC'
        ELSE 'OTHER'
    END AS order_device_type

FROM dbo.TB_Order o
INNER JOIN dbo.TB_OrderProduct op
    ON o.OrderNo = op.OrderNo
LEFT JOIN dbo.TB_Product p
    ON op.ProductNo = p.ProductNo
{category_apply}
{stock_join}
{los_join}
{point_join}
{point_detail_join}
{cart_join}
WHERE o.OrderRegdate >= @start_date
  AND o.MemberID IS NOT NULL
  AND LTRIM(RTRIM(o.MemberID)) <> ''
  AND op.CodeOrderStatusNo IN ({include_statuses})
"""
    return query


# =========================================================
# 6) BigQuery load helpers
# =========================================================
def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    # Dates / datetimes
    date_like_cols = [c for c in df.columns if c.endswith("_date") and not c.endswith("_datetime")]
    datetime_like_cols = [c for c in df.columns if c.endswith("_datetime") or c.endswith("_regdate")]
    explicit_datetime_cols = [
        "order_datetime", "latest_status_datetime", "product_regdate", "product_update_datetime",
        "stock_regdate", "last_point_regdate", "last_point_detail_regdate", "last_cart_regdate",
        "order_cancel_datetime", "erp_datetime", "erp_cancel_datetime", "order_product_datetime",
        "cart_in_datetime",
    ]

    for c in set(datetime_like_cols + explicit_datetime_cols):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    if "order_date" in df.columns:
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce").dt.date

    # Normalize string-like columns
    str_keywords = ["no", "id", "code", "name", "color", "size", "title", "category", "tag", "season", "year", "device", "label"]
    for c in df.columns:
        if any(k in c.lower() for k in str_keywords):
            if df[c].dtype == "object" or str(df[c].dtype).startswith("string"):
                df[c] = df[c].astype("string").fillna("").str.strip()

    # Numeric coercion for known metrics
    numeric_keywords = [
        "qty", "count", "price", "revenue", "amount", "point", "mileage", "stock",
        "priority", "mdpick", "status", "flag", "rate", "quantity", "orders", "buyers",
    ]
    for c in df.columns:
        lc = c.lower()
        if any(k in lc for k in numeric_keywords):
            if c in ["member_id", "order_no", "product_code", "brand_code"]:
                continue
            try:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("int64")
            except Exception:
                pass

    df["etl_loaded_at_kst"] = dt.datetime.now(KST).replace(tzinfo=None)
    return df


def load_to_bigquery(
    df: pd.DataFrame,
    table_id: str,
    project: Optional[str],
    location: str,
    write_disposition: str,
) -> None:
    if df.empty:
        raise RuntimeError(f"Refuse to overwrite {table_id}: dataframe is empty.")

    client = make_bq_client(project=project, location=location)

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition=write_disposition,
    )

    log(f"Loading {len(df):,} rows to BigQuery: {table_id} / write={write_disposition}")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config, location=location)
    job.result()
    table = client.get_table(table_id)
    log(f"BigQuery load complete. table_rows={table.num_rows:,}")


def write_run_summary(path: str, df: pd.DataFrame, table_id: str, extra: Optional[dict] = None) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    summary = {
        "loaded_at_kst": dt.datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "target_table": table_id,
        "rows": int(len(df)),
        "columns": list(df.columns),
    }

    if "order_date" in df.columns and not df.empty:
        summary["min_order_date"] = str(df["order_date"].min())
        summary["max_order_date"] = str(df["order_date"].max())

    for metric in [
        "purchase_qty", "erp_revenue", "erp_cancel_price", "net_erp_revenue",
        "order_total_pay", "order_use_coupon_price", "order_use_point",
        "size_stock_qty", "cart_add_count_180d",
    ]:
        if metric in df.columns and not df.empty:
            summary[f"total_{metric}"] = int(pd.to_numeric(df[metric], errors="coerce").fillna(0).sum())

    if extra:
        summary.update(extra)

    p.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"Wrote summary: {p}")


# =========================================================
# 7) BigQuery mart SQL
# =========================================================
def build_admin_daily_sql(project: str, raw_dataset: str, mart_dataset: str, admin_table: str, source_table: str, traffic_table: str, member_table: str) -> str:
    """
    Daily admin KPI using enhanced order mart.
    """
    return f"""
CREATE OR REPLACE TABLE `{project}.{mart_dataset}.{admin_table}` AS
WITH traffic AS (
  SELECT
    DATE(StatisticsDate) AS report_date,
    SUM(COALESCE(StatisticsSessions, 0)) AS sessions,
    SUM(COALESCE(StatisticsPV, 0)) AS pv
  FROM `{project}.{raw_dataset}.{traffic_table}`
  GROUP BY report_date
),
signups AS (
  SELECT
    DATE(MemberRegdate) AS report_date,
    COUNT(*) AS signups
  FROM `{project}.{raw_dataset}.{member_table}`
  WHERE MemberRegdate IS NOT NULL
  GROUP BY report_date
),
orders AS (
  SELECT
    DATE(order_date) AS report_date,
    COUNT(DISTINCT order_no) AS orders,
    COUNT(DISTINCT CASE WHEN member_id IS NOT NULL AND TRIM(CAST(member_id AS STRING)) != '' THEN member_id END) AS buyers
  FROM `{project}.{raw_dataset}.{source_table}`
  WHERE order_date IS NOT NULL
    AND COALESCE(order_refund_status, 0) = 0
  GROUP BY report_date
),
sales AS (
  SELECT
    DATE(order_date) AS report_date,
    SUM(COALESCE(net_erp_revenue, 0)) AS revenue,
    SUM(COALESCE(purchase_qty, 0)) AS quantity,
    SUM(COALESCE(order_use_coupon_price, 0)) AS coupon_used,
    SUM(COALESCE(order_use_point, 0)) AS point_used,
    SUM(COALESCE(order_cancel_price, 0)) AS cancel_amount,
    SUM(COALESCE(product_promotion_sale_price, 0)) AS promotion_sale_amount,
    COUNT(DISTINCT CASE WHEN is_coupon_order = 1 THEN order_no END) AS coupon_orders,
    COUNT(DISTINCT CASE WHEN is_promotion_line = 1 THEN order_no END) AS promotion_orders
  FROM `{project}.{raw_dataset}.{source_table}`
  GROUP BY report_date
),
dates AS (
  SELECT report_date FROM traffic
  UNION DISTINCT SELECT report_date FROM signups
  UNION DISTINCT SELECT report_date FROM orders
  UNION DISTINCT SELECT report_date FROM sales
)
SELECT
  d.report_date,
  COALESCE(t.sessions, 0) AS sessions,
  COALESCE(t.sessions, 0) AS session,
  COALESCE(t.pv, 0) AS pv,
  COALESCE(su.signups, 0) AS signups,
  COALESCE(o.orders, 0) AS orders,
  COALESCE(o.buyers, 0) AS buyers,
  COALESCE(s.revenue, 0) AS revenue,
  COALESCE(s.revenue, 0) AS total_price,
  COALESCE(s.coupon_used, 0) AS coupon_used,
  COALESCE(s.point_used, 0) AS point_used,
  COALESCE(s.cancel_amount, 0) AS cancel_amount,
  COALESCE(s.promotion_sale_amount, 0) AS promotion_sale_amount,
  COALESCE(s.coupon_orders, 0) AS coupon_orders,
  COALESCE(s.promotion_orders, 0) AS promotion_orders,
  COALESCE(s.quantity, 0) AS quantity,
  SAFE_DIVIDE(COALESCE(o.orders, 0), NULLIF(COALESCE(t.sessions, 0), 0)) AS cvr,
  SAFE_DIVIDE(COALESCE(s.revenue, 0), NULLIF(COALESCE(o.orders, 0), 0)) AS aov,
  SAFE_DIVIDE(COALESCE(s.coupon_orders, 0), NULLIF(COALESCE(o.orders, 0), 0)) AS coupon_order_rate,
  SAFE_DIVIDE(COALESCE(s.promotion_orders, 0), NULLIF(COALESCE(o.orders, 0), 0)) AS promotion_order_rate,
  'mssql_ai_agent_enhanced_order_mart' AS metric_source
FROM dates d
LEFT JOIN traffic t ON d.report_date = t.report_date
LEFT JOIN signups su ON d.report_date = su.report_date
LEFT JOIN orders o ON d.report_date = o.report_date
LEFT JOIN sales s ON d.report_date = s.report_date
"""


def execute_bq_sql(sql: str, project: str, location: str) -> None:
    client = make_bq_client(project=project, location=location)
    job = client.query(sql, location=location)
    job.result()
    log("BigQuery SQL complete.")


# =========================================================
# 8) ETL runners
# =========================================================
def run_query_to_bq(
    conn: pyodbc.Connection,
    query: str,
    table_id: str,
    project: str,
    location: str,
    write_disposition: str,
    summary_path: str,
    label: str,
) -> pd.DataFrame:
    log(f"Running MSSQL query: {label}")
    df = pd.read_sql(query, conn)
    df = normalize_df(df)

    if df.empty:
        raise RuntimeError(f"{label} returned 0 rows. Stop to avoid overwriting BigQuery with empty data.")

    log(f"{label}: extracted rows={len(df):,}, cols={len(df.columns):,}")
    load_to_bigquery(df, table_id=table_id, project=project, location=location, write_disposition=write_disposition)
    write_run_summary(summary_path, df, table_id, extra={"label": label})
    return df


def run_crm_raw(conn: pyodbc.Connection, args: argparse.Namespace) -> None:
    project = args.project
    location = args.location
    raw = args.raw_dataset
    sync_start_date = args.sync_start_date
    write = args.write_disposition

    jobs = []

    if args.sync_member:
        jobs.append((
            "member",
            build_member_query(conn, sync_start_date),
            f"{project}.{raw}.{args.member_table}",
            "logs/member_staging_summary.json",
        ))

    if args.sync_order_header:
        jobs.append((
            "order_header",
            build_order_header_query(conn, sync_start_date),
            f"{project}.{raw}.{args.order_header_table}",
            "logs/order_header_staging_summary.json",
        ))

    if args.sync_product:
        jobs.append((
            "product",
            build_product_query(conn),
            f"{project}.{raw}.{args.product_table}",
            "logs/product_staging_summary.json",
        ))

    if args.sync_traffic and table_exists(conn, "TB_Statistics_Google"):
        jobs.append((
            "traffic_daily",
            build_traffic_daily_query(sync_start_date),
            f"{project}.{raw}.{args.traffic_table}",
            "logs/traffic_daily_staging_summary.json",
        ))

    for label, query, table_id, summary in jobs:
        if args.dry_run_sql:
            log(f"===== DRY RUN SQL: {label} =====")
            print(query)
            continue
        run_query_to_bq(conn, query, table_id, project, location, write, summary, label)


def run_order_mart(conn: pyodbc.Connection, args: argparse.Namespace) -> None:
    query = build_ai_order_product_query(
        conn=conn,
        days_back=args.days_back,
        include_statuses=args.include_product_status,
        cart_days_back=args.cart_days_back,
    )

    if args.dry_run_sql:
        log("===== DRY RUN SQL: enhanced_order_mart =====")
        print(query)
        return

    table_id = f"{args.project}.{args.raw_dataset}.{args.order_product_table}"
    df = run_query_to_bq(
        conn=conn,
        query=query,
        table_id=table_id,
        project=args.project,
        location=args.location,
        write_disposition=args.write_disposition,
        summary_path=args.order_summary_path,
        label="enhanced_order_mart",
    )

    if "net_erp_revenue" in df.columns:
        log(
            "Enhanced order mart summary: rows={:,}, net_erp_revenue={:,}".format(
                len(df),
                int(pd.to_numeric(df["net_erp_revenue"], errors="coerce").fillna(0).sum()),
            )
        )


def run_admin_daily_mart(args: argparse.Namespace) -> None:
    sql = build_admin_daily_sql(
        project=args.project,
        raw_dataset=args.raw_dataset,
        mart_dataset=args.mart_dataset,
        admin_table=args.admin_daily_table,
        source_table=args.order_product_table,
        traffic_table=args.traffic_table,
        member_table=args.member_table,
    )
    if args.dry_run_sql:
        log("===== DRY RUN SQL: admin_daily_mart =====")
        print(sql)
        return
    log(f"Building BigQuery mart: {args.project}.{args.mart_dataset}.{args.admin_daily_table}")
    execute_bq_sql(sql, project=args.project, location=args.location)


# =========================================================
# 9) CLI
# =========================================================
def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Columbia CRM / AI Agent Order ETL final script")

    p.add_argument("--mode", choices=["order_mart", "crm_raw", "admin_daily", "all"], default=getenv("ETL_MODE", "order_mart"))
    p.add_argument("--dry-run-sql", action="store_true")

    p.add_argument("--project", default=getenv("BQ_PROJECT", "columbia-ga4"))
    p.add_argument("--location", default=getenv("BQ_LOCATION", "asia-northeast3"))
    p.add_argument("--raw-dataset", default=getenv("BQ_RAW_DATASET", "crm_raw"))
    p.add_argument("--mart-dataset", default=getenv("BQ_MART_DATASET", "crm_mart"))

    p.add_argument("--sync-start-date", default=getenv("SYNC_START_DATE", "2026-02-01"))
    p.add_argument("--days-back", type=int, default=int(getenv("ORDER_ETL_DAYS_BACK", "760")))
    p.add_argument("--cart-days-back", type=int, default=int(getenv("CART_SIGNAL_DAYS_BACK", "180")))
    p.add_argument("--include-product-status", default=getenv("ORDER_ETL_INCLUDE_PRODUCT_STATUS", "4,5"))
    p.add_argument("--write-disposition", default=getenv("BQ_WRITE_DISPOSITION", "WRITE_TRUNCATE"))

    p.add_argument("--member-table", default=getenv("BQ_MEMBER_STAGING_TABLE", "tb_member_staging"))
    p.add_argument("--order-header-table", default=getenv("BQ_ORDER_HEADER_STAGING_TABLE", "tb_order_staging"))
    p.add_argument("--product-table", default=getenv("BQ_PRODUCT_STAGING_TABLE", "tb_product_staging"))
    p.add_argument("--traffic-table", default=getenv("BQ_TRAFFIC_DAILY_STAGING_TABLE", "tb_statistics_google_staging"))
    p.add_argument("--order-product-table", default=getenv("BQ_ORDER_PRODUCT_TABLE", "tb_order_product_search_mart"))
    p.add_argument("--admin-daily-table", default=getenv("BQ_ADMIN_DAILY_TABLE", "member_funnel_admin_daily"))

    p.add_argument("--order-summary-path", default=getenv("ORDER_ETL_SUMMARY_PATH", "logs/order_product_ai_agent_etl_summary.json"))

    p.add_argument("--sync-member", action=argparse.BooleanOptionalAction, default=getenv("SYNC_MEMBER", "yes").lower() not in ("0", "false", "no"))
    p.add_argument("--sync-order-header", action=argparse.BooleanOptionalAction, default=getenv("SYNC_ORDER_HEADER", "yes").lower() not in ("0", "false", "no"))
    p.add_argument("--sync-product", action=argparse.BooleanOptionalAction, default=getenv("SYNC_PRODUCT", "yes").lower() not in ("0", "false", "no"))
    p.add_argument("--sync-traffic", action=argparse.BooleanOptionalAction, default=getenv("SYNC_TRAFFIC", "yes").lower() not in ("0", "false", "no"))
    p.add_argument("--build-admin-daily", action=argparse.BooleanOptionalAction, default=getenv("BUILD_ADMIN_DAILY", "yes").lower() not in ("0", "false", "no"))

    return p


def main() -> int:
    args = build_arg_parser().parse_args()

    try:
        setup_google_credentials()

        log("=" * 72)
        log("Columbia CRM / AI Agent Order ETL FINAL")
        log(f"mode={args.mode}, project={args.project}, raw={args.raw_dataset}, mart={args.mart_dataset}")
        log(f"today_kst={kst_today_str()}")
        log("=" * 72)

        if args.mode in ("order_mart", "crm_raw", "all"):
            with make_mssql_connection() as conn:
                if args.mode in ("crm_raw", "all"):
                    run_crm_raw(conn, args)

                if args.mode in ("order_mart", "all"):
                    run_order_mart(conn, args)

        if args.mode in ("admin_daily", "all") and args.build_admin_daily:
            run_admin_daily_mart(args)

        log("DONE")
        return 0

    except Exception as e:
        log(f"ERROR: {type(e).__name__}: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
