#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Member Funnel report builder (v2 for crm_mart.member_funnel_master)

Purpose
- Build a static "Member Funnel" report page under reports/member_funnel/
- Reuse the operating pattern from the existing Daily Digest + External Signal reports:
  * JSON bundle cache
  * hub-friendly summary.json export
  * KPI cards / metric tabs / bucket detail
  * tabbed target candidates

Data expectation
- A BigQuery table already exists at MEMBER_FUNNEL_BASE_TABLE with one row per user/member.
- Optional secondary tables can be supplied for product/channel enrichment:
  * MEMBER_FUNNEL_PRODUCT_TABLE
  * MEMBER_FUNNEL_TARGET_TABLE

Recommended base columns in MEMBER_FUNNEL_BASE_TABLE
- user_id, member_id
- event_date
- channel_group, first_source, first_medium, first_campaign, latest_source, latest_campaign
- funnel_stage, signup_yn, purchase_yn, order_count
- total_sessions, total_pageviews, product_view_count, add_to_cart_count
- signup_date, last_visit_date, first_purchase_date, last_order_date
- days_since_signup, days_since_last_purchase
- total_quantity, total_revenue, aov
- top_product, top_category, first_purchase_product
- is_non_buyer, is_cart_abandon, is_high_intent, is_repeat_buyer, is_dormant, is_vip
- recommended_message

Notes
- This script is intentionally designed as a "final v1 renderer":
  it assumes mart tables are already prepared upstream.
- If BigQuery is not available, you can point MEMBER_FUNNEL_SAMPLE_JSON to a prebuilt data bundle.
"""

from __future__ import annotations

import os
import json
import math
import html
import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

try:
    from google.cloud import bigquery  # type: ignore
except Exception:
    bigquery = None


# =========================================================
# Config
# =========================================================
KST = dt.timezone(dt.timedelta(hours=9))

OUT_DIR = Path(os.getenv("MEMBER_FUNNEL_OUT_DIR", os.path.join("reports", "member_funnel")))
DATA_DIR = Path(os.getenv("MEMBER_FUNNEL_DATA_DIR", str(OUT_DIR / "data")))
ASSET_DIR = Path(os.getenv("MEMBER_FUNNEL_ASSET_DIR", str(OUT_DIR / "assets")))
HUB_SUMMARY_DIR = Path(os.getenv("MEMBER_FUNNEL_HUB_SUMMARY_DIR", "reports"))

PROJECT_ID = os.getenv("MEMBER_FUNNEL_PROJECT_ID", "").strip()
BQ_LOCATION = os.getenv("MEMBER_FUNNEL_BQ_LOCATION", "asia-northeast3").strip()

BASE_TABLE = os.getenv("MEMBER_FUNNEL_BASE_TABLE", "crm_mart.member_funnel_master").strip()
PRODUCT_TABLE = os.getenv("MEMBER_FUNNEL_PRODUCT_TABLE", "").strip()
TARGET_TABLE = os.getenv("MEMBER_FUNNEL_TARGET_TABLE", "").strip()  # optional; falls back to BASE_TABLE

TARGET_DAYS = int(os.getenv("MEMBER_FUNNEL_TARGET_DAYS", "30"))
CHANNEL_BUCKET_ORDER = [
    "Awareness",
    "Paid Ad",
    "Organic Traffic",
    "Official SNS",
    "Owned Channel",
    "etc",
]
TARGET_SEGMENT_ORDER = [
    "non_buyer",
    "cart_abandon",
    "high_intent",
    "repeat_buyer",
    "dormant",
    "vip",
]

SAMPLE_JSON = os.getenv("MEMBER_FUNNEL_SAMPLE_JSON", "").strip()
WRITE_DATA_CACHE = os.getenv("MEMBER_FUNNEL_WRITE_DATA_CACHE", "true").strip().lower() in ("1", "true", "yes", "y")


# =========================================================
# CSS / JS
# =========================================================
REPORT_PATCH_CSS = """
<style>
  :root{
    --report-max:none;
    --motion-ease:cubic-bezier(.2,.8,.2,1);
    --bg-a:#f8fafc;
    --bg-b:#eef2f7;
    --line:#e2e8f0;
    --ink:#0f172a;
    --muted:#64748b;
    --card:#ffffffd9;
    --chip:#ffffff;
    --chip-border:rgba(148,163,184,.25);
    --blue:#2563eb;
    --green:#059669;
    --amber:#d97706;
    --rose:#e11d48;
  }
  *{box-sizing:border-box}
  body{
    margin:0;
    color:var(--ink);
    font-family:Inter, "Noto Sans KR", system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    background:linear-gradient(180deg,var(--bg-a) 0%,var(--bg-b) 100%);
  }
  .report-shell{max-width:1600px;margin:0 auto;padding:28px 24px 56px}
  .hero{display:flex;align-items:flex-start;justify-content:space-between;gap:16px;flex-wrap:wrap}
  .hero-title{font-size:34px;font-weight:900;letter-spacing:-.02em}
  .hero-sub{margin-top:8px;color:var(--muted);font-size:14px;font-weight:700}
  .hero-actions{display:flex;gap:8px;flex-wrap:wrap}
  .hero-btn{
    display:inline-flex;align-items:center;justify-content:center;
    border:1px solid var(--line);background:#fff;color:var(--ink);
    border-radius:16px;padding:12px 16px;font-size:13px;font-weight:900;text-decoration:none
  }
  .hero-btn.primary{background:#0f172a;color:#fff;border-color:#0f172a}
  .filter-bar,.report-card,.bucket-detail-panel{
    animation:cardRise .7s var(--motion-ease) both;
    transform-origin:center bottom
  }
  .filter-bar,.report-card,.bucket-detail-panel{
    margin-top:18px;border:1px solid var(--line);background:var(--card);
    border-radius:28px;backdrop-filter:blur(10px)
  }
  .filter-bar{padding:18px}
  .report-card{padding:18px}
  .report-card:hover{transform:translateY(-4px);box-shadow:0 18px 40px rgba(15,23,42,.08)}
  .section-head{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;margin-bottom:14px}
  .section-kicker{font-size:11px;letter-spacing:.14em;text-transform:uppercase;color:#64748b;font-weight:900}
  .section-title{font-size:18px;font-weight:900;letter-spacing:-.02em}
  .section-sub{font-size:12px;color:#64748b;font-weight:700}
  .chip-row,.metric-switch,.segment-tabs{display:inline-flex;gap:8px;flex-wrap:wrap}
  .chip,.metric-tab,.segment-tab{
    border:1px solid var(--chip-border);background:var(--chip);border-radius:999px;
    padding:9px 14px;font-size:12px;font-weight:900;color:#64748b;
    transition:all .22s var(--motion-ease);
    box-shadow:0 6px 18px rgba(15,23,42,.04);cursor:pointer
  }
  .metric-tab:hover,.segment-tab:hover,.chip:hover{transform:translateY(-1px);box-shadow:0 12px 28px rgba(15,23,42,.08)}
  .metric-tab.active,.segment-tab.active,.chip.active{background:#0f172a;color:#fff;border-color:#0f172a;box-shadow:0 14px 32px rgba(15,23,42,.16)}
  .kpi-grid{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:14px}
  .kpi-card{
    position:relative;overflow:hidden;transition:transform .24s var(--motion-ease), box-shadow .24s var(--motion-ease), border-color .24s var(--motion-ease);
    border:1px solid var(--line);background:#fff;border-radius:24px;padding:18px;
  }
  .kpi-card:before{
    content:'';position:absolute;inset:-40% auto auto -20%;width:60%;height:180%;
    background:linear-gradient(90deg,transparent,rgba(255,255,255,.45),transparent);
    transform:rotate(14deg);animation:shineSweep 4.2s linear infinite;pointer-events:none
  }
  .kpi-card:hover{transform:translateY(-6px) scale(1.01);box-shadow:0 22px 44px rgba(15,23,42,.08);border-color:rgba(59,130,246,.22)}
  .kpi-label{font-size:11px;letter-spacing:.14em;text-transform:uppercase;color:#64748b;font-weight:900}
  .kpi-value{margin-top:10px;font-size:28px;font-weight:900;letter-spacing:-.03em;animation:numberPop .8s var(--motion-ease) both}
  .kpi-meta{margin-top:6px;color:#64748b;font-size:12px;font-weight:800}
  .funnel-grid{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:12px}
  .funnel-step{
    border:1px solid var(--line);background:#fff;border-radius:22px;padding:16px;position:relative
  }
  .funnel-step:after{
    content:'→';position:absolute;right:-12px;top:50%;transform:translateY(-50%);
    font-size:20px;font-weight:900;color:#94a3b8
  }
  .funnel-step:last-child:after{display:none}
  .funnel-label{font-size:11px;letter-spacing:.14em;text-transform:uppercase;color:#64748b;font-weight:900}
  .funnel-value{margin-top:8px;font-size:24px;font-weight:900}
  .funnel-rate{margin-top:6px;color:#64748b;font-size:12px;font-weight:800}
  .panel-grid{display:grid;grid-template-columns:1.4fr .9fr;gap:16px}
  .subcard{
    border:1px solid var(--line);background:#fff;border-radius:22px;padding:16px
  }
  table{border-collapse:collapse;width:100%}
  .table-wrap{overflow-x:auto}
  .data-table{min-width:1080px}
  th{
    font-size:11px;letter-spacing:.12em;text-transform:uppercase;color:#64748b;
    font-weight:900;padding:12px 10px;border-bottom:1px solid var(--line);text-align:left;white-space:nowrap
  }
  td{
    padding:12px 10px;border-bottom:1px solid #f1f5f9;font-size:13px;font-weight:700;color:#0f172a;white-space:nowrap
  }
  tr:hover td{background:#f8fafc}
  td.num, th.num{text-align:right}
  .tag{
    display:inline-flex;align-items:center;justify-content:center;border-radius:999px;padding:6px 10px;
    font-size:11px;font-weight:900
  }
  .tag.blue{background:#dbeafe;color:#1d4ed8}
  .tag.green{background:#dcfce7;color:#15803d}
  .tag.amber{background:#fef3c7;color:#b45309}
  .tag.rose{background:#ffe4e6;color:#be123c}
  .tag.slate{background:#e2e8f0;color:#334155}
  .bucket-row{cursor:pointer}
  .muted{color:#64748b}
  .small{font-size:12px}
  .metric-slot{display:none}
  .metric-slot.active{display:inline;animation:metricSwap .42s var(--motion-ease)}
  .segment-panel{display:none}
  .segment-panel.active{display:block}
  .summary-list{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
  .summary-item{border:1px solid var(--line);border-radius:18px;background:#fff;padding:14px}
  .summary-item .label{font-size:11px;color:#64748b;font-weight:900;text-transform:uppercase;letter-spacing:.12em}
  .summary-item .value{margin-top:8px;font-size:18px;font-weight:900}
  @media (max-width: 1280px){
    .kpi-grid{grid-template-columns:repeat(3,minmax(0,1fr))}
    .panel-grid{grid-template-columns:1fr}
  }
  @media (max-width: 900px){
    .kpi-grid{grid-template-columns:repeat(2,minmax(0,1fr))}
    .funnel-grid{grid-template-columns:1fr}
    .summary-list{grid-template-columns:1fr}
  }
  @media (max-width: 640px){
    .report-shell{padding:18px 14px 42px}
    .hero-title{font-size:28px}
    .kpi-grid{grid-template-columns:1fr}
  }
  @keyframes cardRise{from{opacity:0;transform:translateY(26px) scale(.985)}to{opacity:1;transform:translateY(0) scale(1)}}
  @keyframes metricSwap{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}
  @keyframes numberPop{0%{opacity:.2;transform:translateY(12px) scale(.96)}60%{opacity:1;transform:translateY(-2px) scale(1.02)}100%{opacity:1;transform:translateY(0) scale(1)}}
  @keyframes shineSweep{0%{transform:translateX(-160%) rotate(14deg)}100%{transform:translateX(320%) rotate(14deg)}}
</style>
"""

REPORT_PATCH_JS = """
<script>
document.addEventListener('click', function(e){
  const metricBtn = e.target.closest('.metric-tab');
  if(metricBtn){
    const target = metricBtn.dataset.target || '';
    const metric = metricBtn.dataset.metric || '';
    document.querySelectorAll('.metric-tab[data-target="' + target + '"]').forEach(btn => {
      btn.classList.toggle('active', btn === metricBtn);
    });
    document.querySelectorAll('[data-metric-group="' + target + '"] .metric-slot').forEach(el => {
      el.classList.toggle('active', el.dataset.metric === metric);
    });
  }

  const segmentBtn = e.target.closest('.segment-tab');
  if(segmentBtn){
    const target = segmentBtn.dataset.target || '';
    const seg = segmentBtn.dataset.segment || '';
    document.querySelectorAll('.segment-tab[data-target="' + target + '"]').forEach(btn => {
      btn.classList.toggle('active', btn === segmentBtn);
    });
    document.querySelectorAll('.segment-panel[data-target="' + target + '"]').forEach(panel => {
      panel.classList.toggle('active', panel.dataset.segment === seg);
    });
  }

  const bucketRow = e.target.closest('.bucket-row');
  if(bucketRow){
    const bucket = bucketRow.dataset.bucket || '';
    document.querySelectorAll('.bucket-detail').forEach(panel => {
      panel.style.display = panel.dataset.bucket === bucket ? 'block' : 'none';
    });
  }
});
</script>
"""


# =========================================================
# Helpers
# =========================================================
def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

def ymd(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")

def parse_date(value: Any) -> Optional[dt.date]:
    if value is None:
        return None
    if isinstance(value, dt.date):
        return value
    s = str(value).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except Exception:
            continue
    return None

def fmt_int(value: Any) -> str:
    try:
        return f"{int(round(float(value))):,}"
    except Exception:
        return "-"

def fmt_money(value: Any) -> str:
    try:
        return f"₩{int(round(float(value))):,}"
    except Exception:
        return "-"

def fmt_pct(value: Any, digits: int = 1) -> str:
    try:
        return f"{float(value) * 100:.{digits}f}%"
    except Exception:
        return "-"

def safe_div(a: float, b: float) -> float:
    return 0.0 if not b else float(a) / float(b)

def esc(value: Any) -> str:
    return html.escape("" if value is None else str(value))

def pick_tag_class(label: str) -> str:
    low = (label or "").lower()
    if "vip" in low or "repeat" in low:
        return "green"
    if "dormant" in low:
        return "rose"
    if "cart" in low or "high" in low:
        return "amber"
    if "non" in low or "signup" in low:
        return "blue"
    return "slate"

def bucket_sort_key(bucket: str) -> int:
    try:
        return CHANNEL_BUCKET_ORDER.index(bucket)
    except ValueError:
        return len(CHANNEL_BUCKET_ORDER) + 1

def segment_sort_key(seg: str) -> int:
    try:
        return TARGET_SEGMENT_ORDER.index(seg)
    except ValueError:
        return len(TARGET_SEGMENT_ORDER) + 1

def write_json(path: Path, obj: dict) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def read_json(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

def _write_summary_json(out_dir: Path, report_key: str, payload: dict) -> None:
    ensure_dir(out_dir)
    path = out_dir / "summary.json"
    data = {}
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8")) or {}
        except Exception:
            data = {}
    data[report_key] = payload
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# =========================================================
# BigQuery fetch
# =========================================================
def get_bq_client() -> Optional["bigquery.Client"]:
    if bigquery is None:
        return None
    project = PROJECT_ID or None
    return bigquery.Client(project=project, location=BQ_LOCATION)

def run_query(client: "bigquery.Client", sql: str) -> pd.DataFrame:
    job = client.query(sql, location=BQ_LOCATION)
    return job.result().to_dataframe()

def build_base_cte(start_date: dt.date, end_date: dt.date, table: str) -> str:
    return f"""
    WITH base AS (
      SELECT
        *,
        COALESCE(last_visit_date, last_order_date, signup_date) AS event_date,
        CASE WHEN member_id IS NOT NULL THEN 1 ELSE 0 END AS signup_yn,
        CASE WHEN COALESCE(SAFE_CAST(order_count AS INT64), 0) > 0 THEN 1 ELSE 0 END AS purchase_yn,
        COALESCE(NULLIF(top_purchased_item_name, ''), NULLIF(top_product_code, ''), NULLIF(top_viewed_item_name, ''), NULLIF(first_purchase_product_code, ''), '(not set)') AS top_product,
        COALESCE(NULLIF(top_purchased_item_category, ''), NULLIF(top_viewed_item_category, ''), '(not set)') AS top_category,
        COALESCE(first_order_date, signup_date) AS first_purchase_date
      FROM `{table}`
    ),
    scoped AS (
      SELECT *
      FROM base
      WHERE COALESCE(event_date, signup_date) BETWEEN DATE('{ymd(start_date)}') AND DATE('{ymd(end_date)}')
    )
    """

def fetch_bundle_from_bq(start_date: dt.date, end_date: dt.date) -> dict:
    client = get_bq_client()
    if client is None:
        raise RuntimeError("google-cloud-bigquery is not available. Install it or use MEMBER_FUNNEL_SAMPLE_JSON.")

    table = BASE_TABLE
    product_table = PRODUCT_TABLE or BASE_TABLE
    target_table = TARGET_TABLE or BASE_TABLE

    scoped_cte = build_base_cte(start_date, end_date, table)
    product_cte = build_base_cte(start_date, end_date, product_table)
    target_cte = build_base_cte(start_date, end_date, target_table)

    overview_sql = f"""
    {scoped_cte}
    SELECT
      COUNT(DISTINCT COALESCE(NULLIF(CAST(user_id AS STRING), ''), CONCAT('member:', CAST(member_id AS STRING)))) AS users,
      COUNT(DISTINCT CAST(member_id AS STRING)) AS signup_users,
      COUNT(DISTINCT CASE WHEN SAFE_CAST(is_non_buyer AS INT64)=1 THEN CAST(member_id AS STRING) END) AS non_buyer_members,
      COUNT(DISTINCT CASE WHEN COALESCE(SAFE_CAST(order_count AS INT64), 0) > 0 THEN CAST(member_id AS STRING) END) AS buyers,
      COUNT(DISTINCT CASE WHEN COALESCE(SAFE_CAST(order_count AS INT64), 0) >= 2 THEN CAST(member_id AS STRING) END) AS repeat_buyers,
      SUM(COALESCE(SAFE_CAST(total_revenue AS FLOAT64), 0)) AS revenue,
      SAFE_DIVIDE(SUM(COALESCE(SAFE_CAST(total_revenue AS FLOAT64), 0)), NULLIF(SUM(COALESCE(SAFE_CAST(order_count AS INT64), 0)), 0)) AS aov
    FROM scoped
    """

    channel_sql = f"""
    {scoped_cte}
    SELECT
      COALESCE(NULLIF(channel_group, ''), 'etc') AS bucket,
      COUNT(DISTINCT COALESCE(NULLIF(CAST(user_id AS STRING), ''), CONCAT('member:', CAST(member_id AS STRING)))) AS users,
      COUNT(DISTINCT CAST(member_id AS STRING)) AS signups,
      COUNT(DISTINCT CASE WHEN COALESCE(SAFE_CAST(order_count AS INT64), 0) > 0 THEN CAST(member_id AS STRING) END) AS buyers,
      SUM(COALESCE(SAFE_CAST(total_revenue AS FLOAT64), 0)) AS revenue,
      SAFE_DIVIDE(SUM(COALESCE(SAFE_CAST(total_revenue AS FLOAT64), 0)), NULLIF(SUM(COALESCE(SAFE_CAST(order_count AS INT64), 0)), 0)) AS aov
    FROM scoped
    GROUP BY 1
    """

    bucket_detail_sql = f"""
    {scoped_cte}
    SELECT
      COALESCE(NULLIF(channel_group, ''), 'etc') AS bucket,
      COALESCE(NULLIF(first_source, ''), '(not set)') AS source,
      COALESCE(NULLIF(first_medium, ''), '(not set)') AS medium,
      COALESCE(NULLIF(first_campaign, ''), '(not set)') AS campaign,
      COUNT(DISTINCT COALESCE(NULLIF(CAST(user_id AS STRING), ''), CONCAT('member:', CAST(member_id AS STRING)))) AS users,
      COUNT(DISTINCT CAST(member_id AS STRING)) AS signups,
      COUNT(DISTINCT CASE WHEN COALESCE(SAFE_CAST(order_count AS INT64), 0) > 0 THEN CAST(member_id AS STRING) END) AS buyers,
      SUM(COALESCE(SAFE_CAST(total_revenue AS FLOAT64), 0)) AS revenue,
      ANY_VALUE(top_product) AS top_product
    FROM scoped
    GROUP BY 1,2,3,4
    """

    non_buyer_sql = f"""
    {scoped_cte}
    SELECT
      CAST(user_id AS STRING) AS user_id,
      CAST(member_id AS STRING) AS member_id,
      COALESCE(gender, 'UNKNOWN') AS gender,
      COALESCE(age_band, 'UNKNOWN') AS age_band,
      COALESCE(channel_group, 'etc') AS channel_group,
      COALESCE(first_source, '(not set)') AS first_source,
      COALESCE(first_campaign, '(not set)') AS first_campaign,
      CAST(signup_date AS STRING) AS signup_date,
      COALESCE(SAFE_CAST(total_pageviews AS INT64), 0) AS total_pageviews,
      COALESCE(SAFE_CAST(product_view_count AS INT64), 0) AS product_view_count,
      COALESCE(SAFE_CAST(add_to_cart_count AS INT64), 0) AS add_to_cart_count,
      COALESCE(top_category, '(not set)') AS last_viewed_category,
      COALESCE(top_product, '(not set)') AS last_viewed_product,
      COALESCE(recommended_message, 'FIRST_PURCHASE_COUPON') AS recommended_message
    FROM scoped
    WHERE SAFE_CAST(is_non_buyer AS INT64)=1
    ORDER BY product_view_count DESC, add_to_cart_count DESC, total_pageviews DESC
    LIMIT 250
    """

    buyer_sql = f"""
    {scoped_cte}
    SELECT
      CAST(user_id AS STRING) AS user_id,
      CAST(member_id AS STRING) AS member_id,
      COALESCE(gender, 'UNKNOWN') AS gender,
      COALESCE(age_band, 'UNKNOWN') AS age_band,
      COALESCE(channel_group, 'etc') AS channel_group,
      COALESCE(first_source, '(not set)') AS first_source,
      COALESCE(first_campaign, '(not set)') AS first_campaign,
      CAST(first_purchase_date AS STRING) AS first_purchase_date,
      COALESCE(SAFE_CAST(order_count AS INT64), 0) AS order_count,
      COALESCE(SAFE_CAST(total_quantity AS INT64), 0) AS total_quantity,
      COALESCE(SAFE_CAST(total_revenue AS FLOAT64), 0) AS total_revenue,
      COALESCE(SAFE_CAST(aov AS FLOAT64), 0) AS aov,
      COALESCE(top_category, '(not set)') AS top_category,
      COALESCE(top_product, '(not set)') AS top_product
    FROM scoped
    WHERE COALESCE(SAFE_CAST(order_count AS INT64), 0) > 0
    ORDER BY total_revenue DESC, order_count DESC
    LIMIT 250
    """

    product_sql = f"""
    {product_cte}
    SELECT
      COALESCE(NULLIF(top_product, ''), '(not set)') AS product_name,
      COALESCE(NULLIF(top_category, ''), '(not set)') AS category,
      COUNT(DISTINCT CAST(member_id AS STRING)) AS buyers,
      SUM(COALESCE(SAFE_CAST(order_count AS INT64), 0)) AS orders,
      SUM(COALESCE(SAFE_CAST(total_quantity AS INT64), 0)) AS quantity,
      SUM(COALESCE(SAFE_CAST(total_revenue AS FLOAT64), 0)) AS revenue,
      SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN COALESCE(SAFE_CAST(order_count AS INT64), 0)=1 THEN CAST(member_id AS STRING) END), COUNT(DISTINCT CAST(member_id AS STRING))) AS first_purchase_share,
      SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN COALESCE(SAFE_CAST(order_count AS INT64), 0)>=2 THEN CAST(member_id AS STRING) END), COUNT(DISTINCT CAST(member_id AS STRING))) AS repeat_share
    FROM scoped
    WHERE COALESCE(SAFE_CAST(order_count AS INT64), 0) > 0
    GROUP BY 1,2
    QUALIFY ROW_NUMBER() OVER (ORDER BY revenue DESC, buyers DESC) <= 50
    """

    channel_product_sql = f"""
    {product_cte}
    SELECT
      COALESCE(NULLIF(channel_group, ''), 'etc') AS channel_group,
      COALESCE(NULLIF(top_product, ''), '(not set)') AS product_name,
      COUNT(DISTINCT CAST(member_id AS STRING)) AS buyers,
      SUM(COALESCE(SAFE_CAST(total_revenue AS FLOAT64), 0)) AS revenue
    FROM scoped
    WHERE COALESCE(SAFE_CAST(order_count AS INT64), 0) > 0
    GROUP BY 1,2
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel_group ORDER BY revenue DESC, buyers DESC) <= 10
    """

    target_sql = f"""
    {target_cte},
    tagged AS (
      SELECT 'non_buyer' AS segment, * FROM scoped WHERE SAFE_CAST(is_non_buyer AS INT64) = 1
      UNION ALL
      SELECT 'cart_abandon' AS segment, * FROM scoped WHERE SAFE_CAST(is_cart_abandon AS INT64) = 1
      UNION ALL
      SELECT 'high_intent' AS segment, * FROM scoped WHERE SAFE_CAST(is_high_intent AS INT64) = 1
      UNION ALL
      SELECT 'repeat_buyer' AS segment, * FROM scoped WHERE SAFE_CAST(is_repeat_buyer AS INT64) = 1
      UNION ALL
      SELECT 'dormant' AS segment, * FROM scoped WHERE SAFE_CAST(is_dormant AS INT64) = 1
      UNION ALL
      SELECT 'vip' AS segment, * FROM scoped WHERE SAFE_CAST(is_vip AS INT64) = 1
    )
    SELECT
      segment,
      CAST(user_id AS STRING) AS user_id,
      CAST(member_id AS STRING) AS member_id,
      COALESCE(gender, 'UNKNOWN') AS gender,
      COALESCE(age_band, 'UNKNOWN') AS age_band,
      COALESCE(channel_group, 'etc') AS channel_group,
      COALESCE(first_source, '(not set)') AS first_source,
      COALESCE(first_campaign, '(not set)') AS first_campaign,
      COALESCE(top_category, '(not set)') AS preferred_category,
      COALESCE(top_product, '(not set)') AS preferred_product,
      COALESCE(CAST(last_order_date AS STRING), '') AS last_order_date,
      COALESCE(SAFE_CAST(total_revenue AS FLOAT64), 0) AS total_revenue,
      COALESCE(recommended_message, '') AS recommended_message
    FROM tagged
    QUALIFY ROW_NUMBER() OVER (PARTITION BY segment ORDER BY total_revenue DESC, member_id) <= 200
    """

    overview = run_query(client, overview_sql)
    channel = run_query(client, channel_sql)
    bucket_detail = run_query(client, bucket_detail_sql)
    non_buyer = run_query(client, non_buyer_sql)
    buyer = run_query(client, buyer_sql)
    product = run_query(client, product_sql)
    channel_product = run_query(client, channel_product_sql)
    target = run_query(client, target_sql)

    bundle = {
        "generated_at": dt.datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST"),
        "date_range": {"start_date": ymd(start_date), "end_date": ymd(end_date)},
        "overview": overview.fillna("").to_dict(orient="records"),
        "channel_snapshot": channel.fillna("").to_dict(orient="records"),
        "bucket_detail": bucket_detail.fillna("").to_dict(orient="records"),
        "non_buyer": non_buyer.fillna("").to_dict(orient="records"),
        "buyer_revenue": buyer.fillna("").to_dict(orient="records"),
        "product_insight": product.fillna("").to_dict(orient="records"),
        "channel_product": channel_product.fillna("").to_dict(orient="records"),
        "target_candidates": target.fillna("").to_dict(orient="records"),
    }
    return bundle


# =========================================================
# Transform
# =========================================================
def normalize_bundle(bundle: dict) -> dict:
    overview = (bundle.get("overview") or [{}])[0] if (bundle.get("overview") or [{}]) else {}
    channel_rows = pd.DataFrame(bundle.get("channel_snapshot") or [])
    bucket_detail = pd.DataFrame(bundle.get("bucket_detail") or [])
    non_buyer = pd.DataFrame(bundle.get("non_buyer") or [])
    buyer = pd.DataFrame(bundle.get("buyer_revenue") or [])
    product = pd.DataFrame(bundle.get("product_insight") or [])
    channel_product = pd.DataFrame(bundle.get("channel_product") or [])
    target = pd.DataFrame(bundle.get("target_candidates") or [])

    if not channel_rows.empty:
        channel_rows["bucket"] = channel_rows["bucket"].fillna("etc")
        channel_rows["users"] = pd.to_numeric(channel_rows["users"], errors="coerce").fillna(0)
        channel_rows["signups"] = pd.to_numeric(channel_rows["signups"], errors="coerce").fillna(0)
        channel_rows["buyers"] = pd.to_numeric(channel_rows["buyers"], errors="coerce").fillna(0)
        channel_rows["revenue"] = pd.to_numeric(channel_rows["revenue"], errors="coerce").fillna(0)
        channel_rows["aov"] = pd.to_numeric(channel_rows["aov"], errors="coerce").fillna(0)
        channel_rows["cvr"] = channel_rows.apply(lambda r: safe_div(r["buyers"], r["users"]), axis=1)
        channel_rows = channel_rows.sort_values(
            by=["bucket"], key=lambda col: col.map(bucket_sort_key)
        ).reset_index(drop=True)

    funnel = [
        {"key": "users", "label": "Users", "value": int(float(overview.get("users", 0) or 0)), "rate": 1.0},
        {"key": "signup_users", "label": "Sign-up", "value": int(float(overview.get("signup_users", 0) or 0)), "rate": safe_div(float(overview.get("signup_users", 0) or 0), float(overview.get("users", 0) or 0))},
        {"key": "non_buyer_members", "label": "Non Buyer", "value": int(float(overview.get("non_buyer_members", 0) or 0)), "rate": safe_div(float(overview.get("non_buyer_members", 0) or 0), float(overview.get("signup_users", 0) or 0))},
        {"key": "buyers", "label": "Buyer", "value": int(float(overview.get("buyers", 0) or 0)), "rate": safe_div(float(overview.get("buyers", 0) or 0), float(overview.get("signup_users", 0) or 0))},
        {"key": "repeat_buyers", "label": "Repeat", "value": int(float(overview.get("repeat_buyers", 0) or 0)), "rate": safe_div(float(overview.get("repeat_buyers", 0) or 0), float(overview.get("buyers", 0) or 0))},
    ]

    non_buyer_summary = {
        "signup_non_buyer": int(len(non_buyer)),
        "high_pv_non_buyer": int((pd.to_numeric(non_buyer.get("product_view_count"), errors="coerce").fillna(0) >= 3).sum()) if not non_buyer.empty else 0,
        "cart_abandon": int((pd.to_numeric(non_buyer.get("add_to_cart_count"), errors="coerce").fillna(0) >= 1).sum()) if not non_buyer.empty else 0,
        "avg_pv": round(pd.to_numeric(non_buyer.get("total_pageviews"), errors="coerce").fillna(0).mean(), 1) if not non_buyer.empty else 0.0,
    }

    buyer_summary = {
        "buyers": int(float(overview.get("buyers", 0) or 0)),
        "first_buyers": int((pd.to_numeric(buyer.get("order_count"), errors="coerce").fillna(0) == 1).sum()) if not buyer.empty else 0,
        "repeat_buyers": int((pd.to_numeric(buyer.get("order_count"), errors="coerce").fillna(0) >= 2).sum()) if not buyer.empty else 0,
        "revenue": float(overview.get("revenue", 0) or 0),
        "revenue_per_buyer": safe_div(float(overview.get("revenue", 0) or 0), float(overview.get("buyers", 0) or 0)),
        "aov": float(overview.get("aov", 0) or 0),
    }

    target_counts = {}
    if not target.empty:
        for seg, g in target.groupby("segment"):
            target_counts[str(seg)] = int(len(g))

    transformed = {
        "generated_at": bundle.get("generated_at", ""),
        "date_range": bundle.get("date_range", {}),
        "overview": {
            "users": int(float(overview.get("users", 0) or 0)),
            "signup_users": int(float(overview.get("signup_users", 0) or 0)),
            "non_buyer_members": int(float(overview.get("non_buyer_members", 0) or 0)),
            "buyers": int(float(overview.get("buyers", 0) or 0)),
            "repeat_buyers": int(float(overview.get("repeat_buyers", 0) or 0)),
            "revenue": float(overview.get("revenue", 0) or 0),
            "aov": float(overview.get("aov", 0) or 0),
        },
        "funnel": funnel,
        "channel_snapshot": channel_rows.to_dict(orient="records"),
        "bucket_detail": bucket_detail.fillna("").to_dict(orient="records"),
        "non_buyer_summary": non_buyer_summary,
        "non_buyer": non_buyer.fillna("").to_dict(orient="records"),
        "buyer_summary": buyer_summary,
        "buyer_revenue": buyer.fillna("").to_dict(orient="records"),
        "product_insight": product.fillna("").to_dict(orient="records"),
        "channel_product": channel_product.fillna("").to_dict(orient="records"),
        "target_candidates": target.fillna("").to_dict(orient="records"),
        "target_counts": target_counts,
    }
    return transformed


# =========================================================
# HTML render
# =========================================================
def metric_tabs_html(target: str, items: List[tuple[str, str]], active_metric: str) -> str:
    return (
        f'<div class="metric-switch">'
        + "".join(
            f'<button type="button" class="metric-tab {"active" if metric == active_metric else ""}" data-target="{esc(target)}" data-metric="{esc(metric)}">{esc(label)}</button>'
            for metric, label in items
        )
        + "</div>"
    )

def segment_tabs_html(target: str, segments: List[str], active: str) -> str:
    labels = {
        "non_buyer": "Non Buyer",
        "cart_abandon": "Cart Abandon",
        "high_intent": "High Intent",
        "repeat_buyer": "Repeat Buyer",
        "dormant": "Dormant",
        "vip": "VIP",
    }
    return (
        f'<div class="segment-tabs">'
        + "".join(
            f'<button type="button" class="segment-tab {"active" if seg == active else ""}" data-target="{esc(target)}" data-segment="{esc(seg)}">{esc(labels.get(seg, seg))}</button>'
            for seg in segments
        )
        + "</div>"
    )

def render_kpi_cards(data: dict) -> str:
    overview = data["overview"]
    cards = [
        ("Users", fmt_int(overview["users"]), "Tracked people in range"),
        ("Sign-up Users", fmt_int(overview["signup_users"]), "Members who completed sign-up"),
        ("Non Buyer Members", fmt_int(overview["non_buyer_members"]), "Signed up but not purchased"),
        ("Buyers", fmt_int(overview["buyers"]), "Purchased members"),
        ("Revenue", fmt_money(overview["revenue"]), "Total member revenue"),
        ("AOV", fmt_money(overview["aov"]), "Average order value"),
    ]
    return "".join(
        f"""
        <div class="kpi-card">
          <div class="kpi-label">{esc(label)}</div>
          <div class="kpi-value">{esc(value)}</div>
          <div class="kpi-meta">{esc(meta)}</div>
        </div>
        """
        for label, value, meta in cards
    )

def render_funnel_steps(data: dict) -> str:
    steps = []
    for item in data["funnel"]:
        steps.append(
            f"""
            <div class="funnel-step">
              <div class="funnel-label">{esc(item["label"])}</div>
              <div class="funnel-value">{fmt_int(item["value"])}</div>
              <div class="funnel-rate">{fmt_pct(item["rate"])}</div>
            </div>
            """
        )
    return "".join(steps)

def render_channel_snapshot(data: dict) -> str:
    rows = data["channel_snapshot"]
    body = []
    total_users = sum(float(r.get("users", 0) or 0) for r in rows)
    total_buyers = sum(float(r.get("buyers", 0) or 0) for r in rows)
    total_revenue = sum(float(r.get("revenue", 0) or 0) for r in rows)
    total_signups = sum(float(r.get("signups", 0) or 0) for r in rows)

    for r in rows:
        body.append(
            f"""
            <tr class="bucket-row" data-bucket="{esc(r.get('bucket','etc'))}">
              <td>{esc(r.get('bucket','etc'))}</td>
              <td class="num" data-metric-group="channel-snapshot">
                <span class="metric-slot active" data-metric="users">{fmt_int(r.get("users", 0))}</span>
                <span class="metric-slot" data-metric="buyers">{fmt_int(r.get("buyers", 0))}</span>
                <span class="metric-slot" data-metric="revenue">{fmt_money(r.get("revenue", 0))}</span>
              </td>
              <td class="num">{fmt_int(r.get("signups", 0))}</td>
              <td class="num">{fmt_pct(safe_div(float(r.get("buyers", 0) or 0), float(r.get("users", 0) or 0)))}</td>
              <td class="num">{fmt_money(r.get("aov", 0))}</td>
            </tr>
            """
        )

    body.append(
        f"""
        <tr class="bucket-row" data-bucket="__total__">
          <td><strong>Total</strong></td>
          <td class="num" data-metric-group="channel-snapshot">
            <span class="metric-slot active" data-metric="users"><strong>{fmt_int(total_users)}</strong></span>
            <span class="metric-slot" data-metric="buyers"><strong>{fmt_int(total_buyers)}</strong></span>
            <span class="metric-slot" data-metric="revenue"><strong>{fmt_money(total_revenue)}</strong></span>
          </td>
          <td class="num"><strong>{fmt_int(total_signups)}</strong></td>
          <td class="num"><strong>{fmt_pct(safe_div(total_buyers, total_users))}</strong></td>
          <td class="num"><strong>{fmt_money(safe_div(total_revenue, total_buyers))}</strong></td>
        </tr>
        """
    )

    details_map: Dict[str, List[dict]] = {}
    for row in data["bucket_detail"]:
        bucket = str(row.get("bucket") or "etc")
        details_map.setdefault(bucket, []).append(row)

    detail_html = []
    for bucket in CHANNEL_BUCKET_ORDER + ["__total__"]:
        if bucket == "__total__":
            rows_for_bucket = sorted(data["bucket_detail"], key=lambda x: (x.get("bucket", ""), x.get("source", "")))
        else:
            rows_for_bucket = sorted(details_map.get(bucket, []), key=lambda x: (x.get("source", ""), x.get("campaign", "")))
        if not rows_for_bucket:
            continue
        tr_html = "".join(
            f"""
            <tr>
              <td>{esc(r.get("source", "(not set)"))}</td>
              <td>{esc(r.get("medium", "(not set)"))}</td>
              <td>{esc(r.get("campaign", "(not set)"))}</td>
              <td class="num">{fmt_int(r.get("users", 0))}</td>
              <td class="num">{fmt_int(r.get("signups", 0))}</td>
              <td class="num">{fmt_int(r.get("buyers", 0))}</td>
              <td class="num">{fmt_money(r.get("revenue", 0))}</td>
              <td>{esc(r.get("top_product", "(not set)"))}</td>
            </tr>
            """ for r in rows_for_bucket
        )
        detail_html.append(
            f"""
            <div class="bucket-detail bucket-detail-panel report-card" data-bucket="{esc(bucket)}" style="display:{'block' if bucket == CHANNEL_BUCKET_ORDER[0] else 'none'}">
              <div class="section-head">
                <div>
                  <div class="section-kicker">Bucket Detail</div>
                  <div class="section-title">{esc(bucket)}</div>
                  <div class="section-sub">Source / medium / campaign drill-down</div>
                </div>
              </div>
              <div class="table-wrap">
                <table class="data-table">
                  <thead>
                    <tr>
                      <th>Source</th>
                      <th>Medium</th>
                      <th>Campaign</th>
                      <th class="num">Users</th>
                      <th class="num">Sign-ups</th>
                      <th class="num">Buyers</th>
                      <th class="num">Revenue</th>
                      <th>Top Product</th>
                    </tr>
                  </thead>
                  <tbody>{tr_html}</tbody>
                </table>
              </div>
            </div>
            """
        )

    return f"""
    <div class="report-card">
      <div class="section-head">
        <div>
          <div class="section-kicker">Channel Snapshot</div>
          <div class="section-title">유입 버킷별 회원 퍼널</div>
          <div class="section-sub">Daily Digest의 채널 버킷 구조를 그대로 가져온 회원 전환 스냅샷</div>
        </div>
        {metric_tabs_html('channel-snapshot', [('users','Users'), ('buyers','Buyers'), ('revenue','Revenue')], 'users')}
      </div>
      <div class="table-wrap">
        <table class="data-table">
          <thead>
            <tr>
              <th>Bucket</th>
              <th class="num">Metric</th>
              <th class="num">Sign-ups</th>
              <th class="num">CVR</th>
              <th class="num">AOV</th>
            </tr>
          </thead>
          <tbody>{''.join(body)}</tbody>
        </table>
      </div>
    </div>
    {''.join(detail_html)}
    """

def render_non_buyer(data: dict) -> str:
    s = data["non_buyer_summary"]
    rows = data["non_buyer"][:100]
    top_products = (
        pd.DataFrame(rows).groupby("last_viewed_product", dropna=False).size().sort_values(ascending=False).head(5).to_dict()
        if rows else {}
    )
    top_categories = (
        pd.DataFrame(rows).groupby("last_viewed_category", dropna=False).size().sort_values(ascending=False).head(5).to_dict()
        if rows else {}
    )
    table_rows = "".join(
        f"""
        <tr>
          <td>{esc(r.get("user_id",""))}</td>
          <td>{esc(r.get("member_id",""))}</td>
          <td>{esc(r.get("gender","UNKNOWN"))}</td>
          <td>{esc(r.get("age_band","UNKNOWN"))}</td>
          <td>{esc(r.get("gender","UNKNOWN"))}</td>
          <td>{esc(r.get("age_band","UNKNOWN"))}</td>
          <td><span class="tag slate">{esc(r.get("channel_group","etc"))}</span></td>
          <td>{esc(r.get("first_source","(not set)"))}</td>
          <td>{esc(r.get("first_campaign","(not set)"))}</td>
          <td>{esc(r.get("signup_date",""))}</td>
          <td class="num">{fmt_int(r.get("total_pageviews",0))}</td>
          <td class="num">{fmt_int(r.get("product_view_count",0))}</td>
          <td class="num">{fmt_int(r.get("add_to_cart_count",0))}</td>
          <td>{esc(r.get("last_viewed_product","(not set)"))}</td>
          <td><span class="tag {pick_tag_class(str(r.get("recommended_message","")))}">{esc(r.get("recommended_message",""))}</span></td>
        </tr>
        """
        for r in rows
    )
    side_a = "".join(
        f'<div class="summary-item"><div class="label">{esc(k)}</div><div class="value">{fmt_int(v)}</div></div>'
        for k, v in {
            "Signup Non Buyer": s["signup_non_buyer"],
            "High PV Non Buyer": s["high_pv_non_buyer"],
            "Cart Abandon": s["cart_abandon"],
            "Avg PV": s["avg_pv"],
        }.items()
    )
    side_b = "".join(
        f'<div class="summary-item"><div class="label">{esc(k)}</div><div class="value">{fmt_int(v)}</div></div>'
        for k, v in top_products.items()
    ) or '<div class="summary-item"><div class="label">Top Viewed Product</div><div class="value">-</div></div>'
    side_c = "".join(
        f'<div class="summary-item"><div class="label">{esc(k)}</div><div class="value">{fmt_int(v)}</div></div>'
        for k, v in top_categories.items()
    ) or '<div class="summary-item"><div class="label">Top Viewed Category</div><div class="value">-</div></div>'

    return f"""
    <div class="report-card">
      <div class="section-head">
        <div>
          <div class="section-kicker">Non Buyer</div>
          <div class="section-title">가입했지만 아직 안 산 사람</div>
          <div class="section-sub">행동 강도(PV / 상품조회 / 장바구니) 기준으로 바로 리타겟 가능한 목록</div>
        </div>
      </div>
      <div class="panel-grid">
        <div class="subcard">
          <div class="table-wrap">
            <table class="data-table">
              <thead>
                <tr>
                  <th>User ID</th>
                  <th>Member ID</th>
                  <th>Gender</th>
                  <th>Age Band</th>
                  <th>Gender</th>
                  <th>Age Band</th>
                  <th>Channel</th>
                  <th>Source</th>
                  <th>Campaign</th>
                  <th>Signup Date</th>
                  <th class="num">PV</th>
                  <th class="num">Product Views</th>
                  <th class="num">Cart</th>
                  <th>Last Viewed Product</th>
                  <th>Recommended Message</th>
                </tr>
              </thead>
              <tbody>{table_rows}</tbody>
            </table>
          </div>
        </div>
        <div class="subcard">
          <div class="section-kicker">Quick Summary</div>
          <div class="summary-list">{side_a}</div>
          <div class="section-kicker" style="margin-top:16px">Top Viewed Products</div>
          <div class="summary-list">{side_b}</div>
          <div class="section-kicker" style="margin-top:16px">Top Viewed Categories</div>
          <div class="summary-list">{side_c}</div>
        </div>
      </div>
    </div>
    """

def render_buyer_revenue(data: dict) -> str:
    s = data["buyer_summary"]
    rows = data["buyer_revenue"][:100]
    channel_mix = pd.DataFrame(rows).groupby("channel_group", dropna=False)["total_revenue"].sum().sort_values(ascending=False).head(6).to_dict() if rows else {}
    mix_cards = "".join(
        f'<div class="summary-item"><div class="label">{esc(k)}</div><div class="value">{fmt_money(v)}</div></div>'
        for k, v in channel_mix.items()
    ) or '<div class="summary-item"><div class="label">Revenue Mix</div><div class="value">-</div></div>'
    table_rows = "".join(
        f"""
        <tr>
          <td>{esc(r.get("user_id",""))}</td>
          <td>{esc(r.get("member_id",""))}</td>
          <td><span class="tag slate">{esc(r.get("channel_group","etc"))}</span></td>
          <td>{esc(r.get("first_source","(not set)"))}</td>
          <td>{esc(r.get("first_campaign","(not set)"))}</td>
          <td>{esc(r.get("first_purchase_date",""))}</td>
          <td class="num">{fmt_int(r.get("order_count",0))}</td>
          <td class="num">{fmt_int(r.get("total_quantity",0))}</td>
          <td class="num">{fmt_money(r.get("total_revenue",0))}</td>
          <td class="num">{fmt_money(r.get("aov",0))}</td>
          <td>{esc(r.get("top_category","(not set)"))}</td>
          <td>{esc(r.get("top_product","(not set)"))}</td>
        </tr>
        """
        for r in rows
    )
    summary_cards = "".join(
        f'<div class="summary-item"><div class="label">{esc(k)}</div><div class="value">{esc(v)}</div></div>'
        for k, v in {
            "Buyers": fmt_int(s["buyers"]),
            "First Buyers": fmt_int(s["first_buyers"]),
            "Repeat Buyers": fmt_int(s["repeat_buyers"]),
            "Revenue": fmt_money(s["revenue"]),
            "Revenue / Buyer": fmt_money(s["revenue_per_buyer"]),
            "AOV": fmt_money(s["aov"]),
        }.items()
    )

    return f"""
    <div class="report-card">
      <div class="section-head">
        <div>
          <div class="section-kicker">Buyer / Revenue</div>
          <div class="section-title">누가 샀고 얼마를 썼는지</div>
          <div class="section-sub">사람 기준 매출 / 주문 / 대표 상품 확인용</div>
        </div>
      </div>
      <div class="panel-grid">
        <div class="subcard">
          <div class="table-wrap">
            <table class="data-table">
              <thead>
                <tr>
                  <th>User ID</th>
                  <th>Member ID</th>
                  <th>Gender</th>
                  <th>Age Band</th>
                  <th>Channel</th>
                  <th>Source</th>
                  <th>Campaign</th>
                  <th>First Purchase</th>
                  <th class="num">Orders</th>
                  <th class="num">Qty</th>
                  <th class="num">Revenue</th>
                  <th class="num">AOV</th>
                  <th>Top Category</th>
                  <th>Top Product</th>
                </tr>
              </thead>
              <tbody>{table_rows}</tbody>
            </table>
          </div>
        </div>
        <div class="subcard">
          <div class="section-kicker">Quick Summary</div>
          <div class="summary-list">{summary_cards}</div>
          <div class="section-kicker" style="margin-top:16px">Revenue Mix by Channel</div>
          <div class="summary-list">{mix_cards}</div>
        </div>
      </div>
    </div>
    """

def render_product_insight(data: dict) -> str:
    product_rows = data["product_insight"][:50]
    channel_rows = data["channel_product"][:60]

    product_table = "".join(
        f"""
        <tr>
          <td>{esc(r.get("product_name","(not set)"))}</td>
          <td>{esc(r.get("category","(not set)"))}</td>
          <td class="num">{fmt_int(r.get("buyers",0))}</td>
          <td class="num">{fmt_int(r.get("orders",0))}</td>
          <td class="num">{fmt_int(r.get("quantity",0))}</td>
          <td class="num">{fmt_money(r.get("revenue",0))}</td>
          <td class="num">{fmt_pct(r.get("first_purchase_share",0))}</td>
          <td class="num">{fmt_pct(r.get("repeat_share",0))}</td>
        </tr>
        """
        for r in product_rows
    )

    channel_table = "".join(
        f"""
        <tr>
          <td><span class="tag slate">{esc(r.get("channel_group","etc"))}</span></td>
          <td>{esc(r.get("product_name","(not set)"))}</td>
          <td class="num">{fmt_int(r.get("buyers",0))}</td>
          <td class="num">{fmt_money(r.get("revenue",0))}</td>
        </tr>
        """
        for r in channel_rows
    )

    return f"""
    <div class="report-card">
      <div class="section-head">
        <div>
          <div class="section-kicker">Product Insight</div>
          <div class="section-title">어떤 상품이 매출을 만들었는지</div>
          <div class="section-sub">상품 / 카테고리 / 채널별 구매 연결</div>
        </div>
      </div>
      <div class="panel-grid">
        <div class="subcard">
          <div class="section-kicker">Top Products</div>
          <div class="table-wrap">
            <table class="data-table">
              <thead>
                <tr>
                  <th>Product</th>
                  <th>Category</th>
                  <th class="num">Buyers</th>
                  <th class="num">Orders</th>
                  <th class="num">Qty</th>
                  <th class="num">Revenue</th>
                  <th class="num">First Purchase</th>
                  <th class="num">Repeat Share</th>
                </tr>
              </thead>
              <tbody>{product_table}</tbody>
            </table>
          </div>
        </div>
        <div class="subcard">
          <div class="section-kicker">Channel × Product</div>
          <div class="table-wrap">
            <table class="data-table">
              <thead>
                <tr>
                  <th>Channel</th>
                  <th>Product</th>
                  <th class="num">Buyers</th>
                  <th class="num">Revenue</th>
                </tr>
              </thead>
              <tbody>{channel_table}</tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
    """

def render_target_candidates(data: dict) -> str:
    rows = data["target_candidates"]
    if rows:
        df = pd.DataFrame(rows)
    else:
        df = pd.DataFrame(columns=["segment","member_id","user_id","channel_group","first_source","first_campaign","preferred_category","preferred_product","last_order_date","total_revenue","recommended_message"])

    segments = sorted(df["segment"].dropna().astype(str).unique().tolist(), key=segment_sort_key)
    if not segments:
        segments = TARGET_SEGMENT_ORDER[:]

    panels = []
    counts = data.get("target_counts", {})
    summary_items = "".join(
        f'<div class="summary-item"><div class="label">{esc(seg)}</div><div class="value">{fmt_int(counts.get(seg,0))}</div></div>'
        for seg in TARGET_SEGMENT_ORDER
    )

    for idx, seg in enumerate(segments):
        sdf = df[df["segment"].astype(str) == seg].copy()
        table_rows = "".join(
            f"""
            <tr>
              <td>{esc(r.get("segment",""))}</td>
              <td>{esc(r.get("member_id",""))}</td>
              <td>{esc(r.get("user_id",""))}</td>
              <td><span class="tag slate">{esc(r.get("channel_group","etc"))}</span></td>
              <td>{esc(r.get("first_source","(not set)"))}</td>
              <td>{esc(r.get("first_campaign","(not set)"))}</td>
              <td>{esc(r.get("preferred_category","(not set)"))}</td>
              <td>{esc(r.get("preferred_product","(not set)"))}</td>
              <td>{esc(r.get("last_order_date",""))}</td>
              <td class="num">{fmt_money(r.get("total_revenue",0))}</td>
              <td><span class="tag {pick_tag_class(str(r.get("recommended_message","")))}">{esc(r.get("recommended_message",""))}</span></td>
            </tr>
            """ for _, r in sdf.head(120).iterrows()
        )
        panels.append(
            f"""
            <div class="segment-panel {'active' if idx == 0 else ''}" data-target="target-candidates" data-segment="{esc(seg)}">
              <div class="table-wrap">
                <table class="data-table">
                  <thead>
                    <tr>
                      <th>Segment</th>
                      <th>Member ID</th>
                      <th>User ID</th>
                      <th>Channel</th>
                      <th>Source</th>
                      <th>Campaign</th>
                      <th>Preferred Category</th>
                      <th>Preferred Product</th>
                      <th>Last Order</th>
                      <th class="num">Revenue</th>
                      <th>Recommended Message</th>
                    </tr>
                  </thead>
                  <tbody>{table_rows}</tbody>
                </table>
              </div>
            </div>
            """
        )

    return f"""
    <div class="report-card">
      <div class="section-head">
        <div>
          <div class="section-kicker">Target Candidates</div>
          <div class="section-title">지금 메시지 보내야 할 사람들</div>
          <div class="section-sub">Segment별로 바로 export 가능한 대상자 목록</div>
        </div>
        {segment_tabs_html('target-candidates', segments, segments[0] if segments else 'non_buyer')}
      </div>
      <div class="panel-grid">
        <div class="subcard">{''.join(panels)}</div>
        <div class="subcard">
          <div class="section-kicker">Segment Counts</div>
          <div class="summary-list">{summary_items}</div>
          <div class="section-kicker" style="margin-top:16px">Export Guide</div>
          <div class="small muted">
            CSV / LMS / KAKAO / EDM export는 이 테이블을 기준으로 후속 자동화에 연결하면 됩니다.
          </div>
        </div>
      </div>
    </div>
    """

def render_filter_bar(data: dict) -> str:
    dr = data["date_range"]
    chips = [
        ("Period", f'{dr.get("start_date","")} ~ {dr.get("end_date","")}'),
        ("Bucket", "Awareness / Paid Ad / Organic / SNS / Owned / etc"),
        ("Main Views", "Users / Buyers / Revenue / Target"),
        ("Core Goal", "유입 → 회원가입 → 미구매 → 구매 → 상품 → 타겟"),
    ]
    return f"""
    <div class="filter-bar">
      <div class="section-head" style="margin-bottom:0">
        <div>
          <div class="section-kicker">Working Filters</div>
          <div class="section-title">1차 운영 기준</div>
          <div class="section-sub">이 버전은 기간 기준 정적 리포트입니다. 이후 source / campaign / member status 필터를 client-side로 확장하면 됩니다.</div>
        </div>
        <div class="chip-row">
          {''.join(f'<span class="chip active">{esc(k)} · {esc(v)}</span>' for k, v in chips)}
        </div>
      </div>
    </div>
    """

def render_html(data: dict) -> str:
    generated_at = data.get("generated_at", "")
    dr = data.get("date_range", {})
    title = "Member Funnel"
    subtitle = f'{dr.get("start_date","")} ~ {dr.get("end_date","")} · Updated {generated_at}'

    return f"""<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>{esc(title)}</title>
{REPORT_PATCH_CSS}
</head>
<body>
  <div class="report-shell">
    <div class="hero">
      <div>
        <div class="hero-title">{esc(title)}</div>
        <div class="hero-sub">유입 → 회원가입 → 구매 → 상품 → 타겟 흐름을 사람 기준으로 보는 CRM Funnel 리포트</div>
        <div class="hero-sub">{esc(subtitle)}</div>
      </div>
      <div class="hero-actions">
        <a class="hero-btn" href="../index.html">Hub</a>
        <a class="hero-btn" href="./data/target_candidates.json">Target JSON</a>
        <a class="hero-btn primary" href="./data/overview.json">Data Bundle</a>
      </div>
    </div>

    {render_filter_bar(data)}

    <div class="report-card">
      <div class="section-head">
        <div>
          <div class="section-kicker">Overview</div>
          <div class="section-title">핵심 KPI</div>
          <div class="section-sub">운영에 바로 필요한 기본 카드</div>
        </div>
      </div>
      <div class="kpi-grid">{render_kpi_cards(data)}</div>
    </div>

    <div class="report-card">
      <div class="section-head">
        <div>
          <div class="section-kicker">Overview Funnel</div>
          <div class="section-title">사람 기준 퍼널</div>
          <div class="section-sub">Users → Sign-up → Non Buyer → Buyer → Repeat</div>
        </div>
      </div>
      <div class="funnel-grid">{render_funnel_steps(data)}</div>
    </div>

    {render_channel_snapshot(data)}
    {render_non_buyer(data)}
    {render_buyer_revenue(data)}
    {render_product_insight(data)}
    {render_target_candidates(data)}
  </div>
  {REPORT_PATCH_JS}
</body>
</html>
"""


# =========================================================
# Main
# =========================================================
def persist_bundle_files(data: dict) -> None:
    ensure_dir(OUT_DIR)
    ensure_dir(DATA_DIR)
    ensure_dir(ASSET_DIR)

    write_json(DATA_DIR / "overview.json", data)
    write_json(DATA_DIR / "channel_snapshot.json", {"rows": data.get("channel_snapshot", [])})
    write_json(DATA_DIR / "non_buyer.json", {"rows": data.get("non_buyer", [])})
    write_json(DATA_DIR / "buyer_revenue.json", {"rows": data.get("buyer_revenue", [])})
    write_json(DATA_DIR / "product_insight.json", {"rows": data.get("product_insight", []), "channel_rows": data.get("channel_product", [])})
    write_json(DATA_DIR / "target_candidates.json", {"rows": data.get("target_candidates", [])})

    summary_payload = {
        "title": "Member Funnel",
        "updated_at": data.get("generated_at", ""),
        "range": data.get("date_range", {}),
        "users": data.get("overview", {}).get("users", 0),
        "buyers": data.get("overview", {}).get("buyers", 0),
        "revenue": data.get("overview", {}).get("revenue", 0),
        "target_counts": data.get("target_counts", {}),
    }
    _write_summary_json(HUB_SUMMARY_DIR, "member_funnel", summary_payload)

def main() -> None:
    ensure_dir(OUT_DIR)
    ensure_dir(DATA_DIR)

    end_date = dt.datetime.now(KST).date() - dt.timedelta(days=1)
    start_date = end_date - dt.timedelta(days=max(1, TARGET_DAYS) - 1)

    if SAMPLE_JSON:
        bundle = read_json(Path(SAMPLE_JSON))
        if not bundle:
            raise RuntimeError(f"Could not read MEMBER_FUNNEL_SAMPLE_JSON: {SAMPLE_JSON}")
    else:
        bundle = fetch_bundle_from_bq(start_date, end_date)

    data = normalize_bundle(bundle)
    if WRITE_DATA_CACHE:
        persist_bundle_files(data)

    html_str = render_html(data)
    out_path = OUT_DIR / "index.html"
    out_path.write_text(html_str, encoding="utf-8")
    print(f"[OK] Wrote: {out_path}")

if __name__ == "__main__":
    main()
