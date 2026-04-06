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

PERIOD_PRESETS = [
    {"key": "1d", "label": "1D", "days": 1, "filename": "daily.html", "is_default": False, "description": "전일 기준"},
    {"key": "7d", "label": "7D", "days": 7, "filename": "7d.html", "is_default": False, "description": "최근 7일"},
    {"key": "1m", "label": "1M", "days": 30, "filename": "index.html", "is_default": True, "description": "최근 30일"},
    {"key": "1y", "label": "1Y", "days": 365, "filename": "1year.html", "is_default": False, "description": "최근 1년"},
]
CHANNEL_BUCKET_ORDER = [
    "Awareness",
    "Paid Ad",
    "Organic Traffic",
    "Official SNS",
    "Owned Channel",
    "Direct",
    "Unknown",
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
  @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200;400;500;700;800&display=swap');
  :root{
    --brand:#002d72;
    --bg0:#f6f8fb;
    --bg1:#eef3f9;
    --ink:#0f172a;
    --muted:#64748b;
    --line:rgba(15,23,42,.08);
    --glass:rgba(255,255,255,.68);
    --glass-strong:rgba(255,255,255,.82);
    --shadow:0 10px 30px rgba(2,6,23,.08);
    --shadow-strong:0 18px 40px rgba(2,6,23,.12);
    --navy:#0f172a;
    --blue:#2563eb;
    --sky:#0284c7;
    --green:#059669;
    --amber:#d97706;
    --rose:#e11d48;
    --violet:#7c3aed;
    --slate:#475569;
    --radius-xl:28px;
    --radius-lg:22px;
    --radius-md:18px;
    --radius-sm:14px;
    --ease:cubic-bezier(.2,.8,.2,1);
  }
  *{box-sizing:border-box}
  html{scroll-behavior:smooth}
  body{
    margin:0;
    min-height:100vh;
    color:var(--ink);
    font-family:'Plus Jakarta Sans','Noto Sans KR',system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
    background:linear-gradient(180deg,var(--bg0),var(--bg1));
  }
  .report-shell{max-width:1680px;margin:0 auto;padding:0 0 56px}
  .hero,
  .filter-bar,
  .report-card,
  .bucket-detail-panel{
    background:var(--glass);
    border:1px solid var(--line);
    box-shadow:var(--shadow);
    backdrop-filter:blur(10px);
  }
  .hero{
    border-radius:0;
    padding:22px 24px;
    display:grid;
    grid-template-columns:minmax(0,1.35fr) minmax(320px,.9fr);
    gap:18px;
    margin-bottom:18px;
  }
  .hero-main,.hero-side{
    background:transparent;
    border:none;
    box-shadow:none;
    padding:0;
    animation:cardRise .6s var(--ease) both;
  }
  .hero-kicker,.section-kicker{font-size:11px;font-weight:800;letter-spacing:.16em;text-transform:uppercase;color:var(--muted)}
  .hero-title{font-size:34px;font-weight:800;letter-spacing:-.03em;line-height:1.08;color:var(--ink)}
  .hero-sub,.section-sub,.kpi-meta,.mini-meta,.stat-meta,.trend-note,.funnel-rate{font-size:13px;line-height:1.6;color:var(--muted);font-weight:700}
  .hero-meta,.chip-row,.metric-switch,.segment-tabs,.bucket-pills,.channel-trend-tabs,.chart-legend{display:flex;gap:8px;flex-wrap:wrap}
  .hero-chip,.chip,.metric-tab,.segment-tab,.bucket-pill,.link-chip,.hero-btn,.tag{
    display:inline-flex;align-items:center;justify-content:center;gap:6px;
    min-height:38px;padding:0 14px;border-radius:999px;
    border:1px solid rgba(15,23,42,.1);background:#fff;color:var(--slate);
    font-size:12px;font-weight:800;text-decoration:none;
    transition:all .2s var(--ease);
  }
  .hero-chip:hover,.chip:hover,.metric-tab:hover,.segment-tab:hover,.bucket-pill:hover,.link-chip:hover,.hero-btn:hover{transform:translateY(-1px);box-shadow:0 8px 20px rgba(2,6,23,.08)}
  .metric-tab.active,.segment-tab.active,.chip.active,.bucket-pill.active,.hero-btn.primary{background:#0f172a;color:#fff;border-color:#0f172a}
  .hero-btn{min-height:42px;border-radius:14px;padding:0 16px}
  .hero-actions{display:flex;gap:10px;flex-wrap:wrap;margin-top:18px}
  .hero-summary-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}
  .highlight-card,.subcard,.mini-card,.stat-card,.flow-card,.funnel-step,.summary-item,.insight-item,.chart-card,.kpi-card,.persona-card{
    background:var(--glass-strong);
    border:1px solid var(--line);
    box-shadow:none;
  }
  .highlight-card,.subcard,.chart-card,.persona-card,.kpi-card,.flow-card,.funnel-step,.mini-card,.stat-card,.summary-item,.insight-item{border-radius:22px}
  .highlight-card,.persona-card,.kpi-card,.funnel-step,.subcard,.chart-card,.flow-card,.report-card,.bucket-detail-panel,.filter-bar{padding:18px}
  .highlight-label,.mini-label,.stat-label,.kpi-label{font-size:10px;letter-spacing:.14em;text-transform:uppercase;color:var(--muted);font-weight:800}
  .highlight-value{margin-top:8px;font-size:24px;font-weight:800;letter-spacing:-.03em}
  .highlight-meta{margin-top:6px;font-size:12px;color:var(--muted);font-weight:700;line-height:1.55}
  .insight-list{display:grid;gap:10px}
  .insight-item{padding:14px 16px}
  .insight-item .label{font-size:10px;letter-spacing:.14em;text-transform:uppercase;color:var(--muted);font-weight:800}
  .insight-item .value{margin-top:6px;font-size:14px;font-weight:800;line-height:1.6;color:var(--ink)}
  .section-head{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;flex-wrap:wrap;margin-bottom:14px}
  .section-title{font-size:22px;font-weight:800;letter-spacing:-.03em;color:var(--ink)}
  .report-card,.bucket-detail-panel,.filter-bar{margin:18px 24px 0;border-radius:26px;animation:cardRise .6s var(--ease) both}
  .kpi-grid{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:14px}
  .kpi-card{position:relative;overflow:hidden;min-height:136px;transition:transform .22s var(--ease), box-shadow .22s var(--ease), border-color .22s var(--ease)}
  .kpi-card:before{content:'';position:absolute;inset:0 0 auto 0;height:3px;background:linear-gradient(90deg,var(--brand),#3b82f6,#7c3aed)}
  .kpi-card:hover,.persona-card:hover,.flow-card:hover,.subcard:hover,.chart-card:hover,.funnel-step:hover{transform:translateY(-2px);box-shadow:var(--shadow-strong);border-color:rgba(37,99,235,.18)}
  .kpi-value{margin-top:10px;font-size:30px;font-weight:800;letter-spacing:-.04em;line-height:1.05}
  .persona-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px}
  .persona-head{display:flex;align-items:flex-start;justify-content:space-between;gap:10px}
  .persona-title{font-size:18px;font-weight:800;letter-spacing:-.02em}
  .persona-badge{font-size:11px;font-weight:800;letter-spacing:.12em;text-transform:uppercase;color:#fff;background:var(--navy);padding:7px 10px;border-radius:999px}
  .persona-main{margin-top:14px;font-size:14px;font-weight:700;line-height:1.65;color:var(--slate)}
  .persona-main strong{color:var(--ink)}
  .persona-stats{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin-top:14px}
  .mini-value,.stat-value{margin-top:6px;font-size:16px;font-weight:800;letter-spacing:-.02em}
  .funnel-grid{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:12px}
  .funnel-step{position:relative;overflow:hidden}
  .funnel-step:before{content:'';position:absolute;left:18px;top:0;width:44px;height:3px;background:linear-gradient(90deg,var(--brand),#60a5fa);border-radius:999px}
  .funnel-step:after{content:'→';position:absolute;right:-12px;top:50%;transform:translateY(-50%);font-size:22px;font-weight:800;color:#94a3b8}
  .funnel-step:last-child:after{display:none}
  .funnel-label{font-size:11px;letter-spacing:.14em;text-transform:uppercase;color:var(--muted);font-weight:800}
  .funnel-value{margin-top:10px;font-size:26px;font-weight:800;letter-spacing:-.03em}
  .panel-grid{display:grid;grid-template-columns:1.25fr .95fr;gap:16px}
  .three-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:16px}
  .stat-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px}
  .flow-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px}
  .flow-title{font-size:16px;font-weight:800;letter-spacing:-.02em}
  .flow-text,.bullet{margin-top:10px;font-size:13px;font-weight:700;color:var(--muted);line-height:1.65}
  .bullet-list{display:grid;gap:8px;margin-top:12px}
  .bullet{display:flex;gap:8px;align-items:flex-start;color:var(--slate);margin-top:0}
  .bullet:before{content:'•';color:var(--brand);font-weight:800}
  table{border-collapse:collapse;width:100%}
  .table-wrap{overflow:auto;max-width:100%}
  .data-table{min-width:100%;table-layout:auto}
  .compact-table{min-width:820px}
  th{font-size:11px;letter-spacing:.12em;text-transform:uppercase;color:var(--muted);font-weight:800;padding:12px 10px;border-bottom:1px solid #e5edf5;text-align:left;white-space:nowrap}
  td{padding:12px 10px;border-bottom:1px solid #edf2f7;font-size:13px;font-weight:700;color:var(--ink);vertical-align:top}
  tr:hover td{background:#f8fbff}
  td.num,th.num{text-align:right}
  td.center,th.center{text-align:center}
  .tag.blue{background:#dbeafe;color:#1d4ed8}
  .tag.green{background:#dcfce7;color:#15803d}
  .tag.amber{background:#fef3c7;color:#b45309}
  .tag.rose{background:#ffe4e6;color:#be123c}
  .tag.slate{background:#e2e8f0;color:#334155}
  .tag.violet{background:#ede9fe;color:#6d28d9}
  .metric-slot,.segment-panel,.bucket-detail,.channel-trend-panel{display:none}
  .metric-slot.active,.segment-panel.active,.bucket-detail.active,.channel-trend-panel.active{display:block;animation:metricSwap .32s var(--ease)}
  .summary-list{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
  .summary-item{padding:14px}
  .summary-item .label{font-size:11px;color:var(--muted);font-weight:800;text-transform:uppercase;letter-spacing:.12em}
  .summary-item .value{margin-top:8px;font-size:18px;font-weight:800;letter-spacing:-.02em}
  .empty-note{padding:18px;border-radius:18px;background:#fff;border:1px dashed #d4dde7;font-size:13px;font-weight:700;color:var(--muted)}
  .small{font-size:12px}
  .muted{color:var(--muted)}
  .trend-grid{display:grid;grid-template-columns:1.25fr .95fr;gap:16px}
  .chart-head{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;flex-wrap:wrap;margin-bottom:14px}
  .chart-svg{width:100%;height:320px;display:block}
  .chart-grid-line{stroke:#dbe4ee;stroke-width:1}
  .chart-axis-label{font-size:11px;fill:#64748b;font-weight:700}
  .chart-line-users,.chart-line-signups,.chart-line-buyers{fill:none;stroke-width:3.2;stroke-linecap:round;stroke-linejoin:round}
  .chart-line-users{stroke:#2563eb}
  .chart-line-signups{stroke:#7c3aed}
  .chart-line-buyers{stroke:#059669}
  .chart-point.users{fill:#2563eb}
  .chart-point.signups{fill:#7c3aed}
  .chart-point.buyers{fill:#059669}
  .legend-item{display:inline-flex;align-items:center;gap:6px;font-size:12px;font-weight:800;color:var(--slate)}
  .legend-dot{width:10px;height:10px;border-radius:50%;display:inline-block}
  .bar-chart{display:grid;gap:10px}
  .bar-row{display:grid;grid-template-columns:56px 1fr 110px;gap:10px;align-items:center}
  .bar-label,.bar-value{font-size:12px;font-weight:800;color:var(--slate)}
  .bar-track{height:10px;border-radius:999px;background:#eef3f8;overflow:hidden}
  .bar-fill{height:100%;border-radius:999px;background:linear-gradient(90deg,#2563eb,#7c3aed)}
  @media (max-width:1480px){
    .kpi-grid{grid-template-columns:repeat(3,minmax(0,1fr))}
    .persona-grid{grid-template-columns:repeat(2,minmax(0,1fr))}
    .panel-grid,.three-grid,.trend-grid{grid-template-columns:1fr}
    .flow-grid{grid-template-columns:1fr}
  }
  @media (max-width:1120px){
    .hero{grid-template-columns:1fr;border-radius:0;padding:20px 18px}
    .report-card,.bucket-detail-panel,.filter-bar{margin-left:18px;margin-right:18px}
    .funnel-grid{grid-template-columns:repeat(2,minmax(0,1fr))}
  }
  @media (max-width:780px){
    .report-shell{padding-bottom:42px}
    .kpi-grid,.persona-grid,.hero-summary-grid,.summary-list,.stat-grid,.funnel-grid{grid-template-columns:1fr}
    .hero-title{font-size:28px}
    .section-title{font-size:19px}
    .hero-chip,.chip,.metric-tab,.segment-tab,.bucket-pill,.link-chip,.hero-btn,.tag{width:100%}
    .bar-row{grid-template-columns:1fr}
    .funnel-step:after{display:none}
    .report-card,.bucket-detail-panel,.filter-bar{margin-left:14px;margin-right:14px;padding:16px}
  }
  @keyframes metricSwap{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}
  @keyframes cardRise{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:translateY(0)}}
</style>
"""

REPORT_PATCH_JS = """
<script>
document.addEventListener('click', function(e){
  const metricBtn = e.target.closest('.metric-tab');
  if(metricBtn){
    const target = metricBtn.dataset.target || '';
    const metric = metricBtn.dataset.metric || '';
    document.querySelectorAll('.metric-tab[data-target="' + target + '"]').forEach(function(btn){
      btn.classList.toggle('active', btn === metricBtn);
    });
    document.querySelectorAll('[data-metric-group="' + target + '"] .metric-slot').forEach(function(node){
      node.classList.toggle('active', node.dataset.metric === metric);
    });
  }

  const segmentBtn = e.target.closest('.segment-tab');
  if(segmentBtn){
    const target = segmentBtn.dataset.target || '';
    const segment = segmentBtn.dataset.segment || '';
    document.querySelectorAll('.segment-tab[data-target="' + target + '"]').forEach(function(btn){
      btn.classList.toggle('active', btn === segmentBtn);
    });
    document.querySelectorAll('.segment-panel[data-target="' + target + '"]').forEach(function(panel){
      panel.classList.toggle('active', panel.dataset.segment === segment);
    });
  }

  const bucketBtn = e.target.closest('.bucket-pill');
  if(bucketBtn){
    const target = bucketBtn.dataset.target || '';
    const bucket = bucketBtn.dataset.bucket || '';
    document.querySelectorAll('.bucket-pill[data-target="' + target + '"]').forEach(function(btn){
      btn.classList.toggle('active', btn === bucketBtn);
    });
    document.querySelectorAll('.bucket-detail[data-target="' + target + '"]').forEach(function(panel){
      panel.classList.toggle('active', panel.dataset.bucket === bucket);
    });
  }

  const bucketRow = e.target.closest('.bucket-row');
  if(bucketRow && bucketRow.dataset.bucketTarget){
    const target = bucketRow.dataset.bucketTarget;
    const bucket = bucketRow.dataset.bucket || '';
    const pill = document.querySelector('.bucket-pill[data-target="' + target + '"][data-bucket="' + bucket + '"]');
    if(pill){ pill.click(); }
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


    trend_sql = f"""
    {scoped_cte},
    date_spine AS (
      SELECT d AS dt
      FROM UNNEST(GENERATE_DATE_ARRAY(DATE('{ymd(start_date)}'), DATE('{ymd(end_date)}'))) AS d
    ),
    activity AS (
      SELECT
        COALESCE(event_date, signup_date) AS dt,
        COUNT(DISTINCT COALESCE(NULLIF(CAST(user_id AS STRING), ''), CONCAT('member:', CAST(member_id AS STRING)))) AS users
      FROM scoped
      GROUP BY 1
    ),
    signup AS (
      SELECT
        signup_date AS dt,
        COUNT(DISTINCT CAST(member_id AS STRING)) AS signups
      FROM scoped
      WHERE signup_date IS NOT NULL
      GROUP BY 1
    ),
    buyer AS (
      SELECT
        first_purchase_date AS dt,
        COUNT(DISTINCT CASE WHEN COALESCE(SAFE_CAST(order_count AS INT64), 0) > 0 THEN CAST(member_id AS STRING) END) AS buyers,
        SUM(CASE WHEN COALESCE(SAFE_CAST(order_count AS INT64), 0) > 0 THEN COALESCE(SAFE_CAST(total_revenue AS FLOAT64), 0) ELSE 0 END) AS revenue
      FROM scoped
      WHERE first_purchase_date IS NOT NULL
      GROUP BY 1
    )
    SELECT
      CAST(s.dt AS STRING) AS dt,
      COALESCE(a.users, 0) AS users,
      COALESCE(g.signups, 0) AS signups,
      COALESCE(b.buyers, 0) AS buyers,
      COALESCE(b.revenue, 0) AS revenue
    FROM date_spine s
    LEFT JOIN activity a ON s.dt = a.dt
    LEFT JOIN signup g ON s.dt = g.dt
    LEFT JOIN buyer b ON s.dt = b.dt
    ORDER BY s.dt
    """

    overview = run_query(client, overview_sql)
    channel = run_query(client, channel_sql)
    bucket_detail = run_query(client, bucket_detail_sql)
    non_buyer = run_query(client, non_buyer_sql)
    buyer = run_query(client, buyer_sql)
    product = run_query(client, product_sql)
    channel_product = run_query(client, channel_product_sql)
    target = run_query(client, target_sql)
    channel_trend_sql = f"""
    {scoped_cte},
    stacked AS (
      SELECT
        CAST(COALESCE(event_date, signup_date) AS STRING) AS dt,
        COALESCE(channel_group, 'etc') AS bucket,
        COALESCE(first_source, '') AS first_source,
        COALESCE(first_medium, '') AS first_medium,
        COALESCE(first_campaign, '') AS first_campaign,
        COUNT(DISTINCT COALESCE(NULLIF(CAST(user_id AS STRING), ''), CONCAT('member:', CAST(member_id AS STRING)))) AS users,
        0 AS signups,
        0 AS buyers,
        0.0 AS revenue
      FROM scoped
      WHERE COALESCE(event_date, signup_date) IS NOT NULL
      GROUP BY 1,2,3,4,5

      UNION ALL

      SELECT
        CAST(signup_date AS STRING) AS dt,
        COALESCE(channel_group, 'etc') AS bucket,
        COALESCE(first_source, '') AS first_source,
        COALESCE(first_medium, '') AS first_medium,
        COALESCE(first_campaign, '') AS first_campaign,
        0 AS users,
        COUNT(DISTINCT CAST(member_id AS STRING)) AS signups,
        0 AS buyers,
        0.0 AS revenue
      FROM scoped
      WHERE signup_date IS NOT NULL
      GROUP BY 1,2,3,4,5

      UNION ALL

      SELECT
        CAST(first_purchase_date AS STRING) AS dt,
        COALESCE(channel_group, 'etc') AS bucket,
        COALESCE(first_source, '') AS first_source,
        COALESCE(first_medium, '') AS first_medium,
        COALESCE(first_campaign, '') AS first_campaign,
        0 AS users,
        0 AS signups,
        COUNT(DISTINCT CASE WHEN COALESCE(SAFE_CAST(order_count AS INT64), 0) > 0 THEN CAST(member_id AS STRING) END) AS buyers,
        SUM(CASE WHEN COALESCE(SAFE_CAST(order_count AS INT64), 0) > 0 THEN COALESCE(SAFE_CAST(total_revenue AS FLOAT64), 0) ELSE 0 END) AS revenue
      FROM scoped
      WHERE first_purchase_date IS NOT NULL
      GROUP BY 1,2,3,4,5
    )
    SELECT
      dt,
      bucket,
      first_source,
      first_medium,
      first_campaign,
      SUM(users) AS users,
      SUM(signups) AS signups,
      SUM(buyers) AS buyers,
      SUM(revenue) AS revenue
    FROM stacked
    GROUP BY 1,2,3,4,5
    ORDER BY dt, bucket
    """

    trend = run_query(client, trend_sql)
    channel_trend = run_query(client, channel_trend_sql)

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
        "daily_trend": trend.fillna("").to_dict(orient="records"),
        "channel_daily_trend": channel_trend.fillna("").to_dict(orient="records"),
    }
    return bundle


# =========================================================
# Transform
# =========================================================
def _safe_series(df: pd.DataFrame, col: str, default: str = "") -> pd.Series:
    if col in df.columns:
        return df[col]
    return pd.Series([default] * len(df), index=df.index, dtype="object")


def _numeric_series(df: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
    if col in df.columns:
        return pd.to_numeric(df[col], errors="coerce").fillna(default)
    return pd.Series([default] * len(df), index=df.index, dtype="float64")


def _as_dataframe(obj: Any) -> pd.DataFrame:
    if obj is None:
        return pd.DataFrame()
    if isinstance(obj, pd.DataFrame):
        return obj.copy()
    if isinstance(obj, pd.Series):
        return obj.to_frame()
    if isinstance(obj, dict):
        try:
            return pd.DataFrame([obj])
        except Exception:
            return pd.DataFrame()
    if isinstance(obj, (list, tuple)):
        try:
            return pd.DataFrame(list(obj))
        except Exception:
            return pd.DataFrame()
    try:
        return pd.DataFrame(obj)
    except Exception:
        return pd.DataFrame()


def _first_record(obj: Any) -> dict:
    if obj is None:
        return {}
    if isinstance(obj, pd.DataFrame):
        if obj.empty:
            return {}
        return obj.iloc[0].to_dict()
    if isinstance(obj, pd.Series):
        return obj.to_dict()
    if isinstance(obj, dict):
        return dict(obj)
    if isinstance(obj, (list, tuple)):
        if not obj:
            return {}
        first = obj[0]
        if isinstance(first, pd.Series):
            return first.to_dict()
        if isinstance(first, dict):
            return dict(first)
        try:
            return pd.Series(first).to_dict()
        except Exception:
            return {}
    try:
        df = pd.DataFrame(obj)
        if df.empty:
            return {}
        return df.iloc[0].to_dict()
    except Exception:
        return {}


def _coalesce_text_series(primary: pd.Series, fallback: pd.Series, blank_values: Optional[set[str]] = None) -> pd.Series:
    blank_values = {str(x).lower() for x in (blank_values or {"", "nan", "none", "null"})}
    p = primary.fillna("").astype(str).str.strip()
    f = fallback.fillna("").astype(str).str.strip()
    mask = p.str.lower().isin(blank_values)
    return p.where(~mask, f)


def canonical_bucket(bucket: Any, source: Any = "", medium: Any = "", campaign: Any = "") -> str:
    b = str(bucket or "").strip() or "etc"
    s = str(source or "").strip().lower()
    m = str(medium or "").strip().lower()
    c = str(campaign or "").strip().lower()
    pair = f"{s} / {m}".strip()

    if "(direct)" in pair or (s == "direct" and m in ("(none)", "none", "")):
        return "Direct"
    if b in ("Unknown", "unknown"):
        return "Unknown"
    if b == "etc":
        if s in ("", "(not set)", "not set", "unknown") and m in ("", "(not set)", "not set", "unknown"):
            return "Unknown"
        if "direct" in s or "(none)" in m:
            return "Direct"
    if b == "Organic Traffic" and ("(direct)" in pair or " / (none)" in pair):
        return "Direct"
    return b


def _mode_label(series: pd.Series, fallback: str = "-", exclude: Optional[set[str]] = None) -> str:
    exclude = exclude or set()
    vals = (
        series.fillna("")
        .astype(str)
        .str.strip()
    )
    vals = vals[(vals != "") & (~vals.str.lower().isin({x.lower() for x in exclude}))]
    if vals.empty:
        return fallback
    vc = vals.value_counts()
    return str(vc.index[0]) if not vc.empty else fallback


def _topn_labels(series: pd.Series, n: int = 2, exclude: Optional[set[str]] = None) -> str:
    exclude = exclude or set()
    vals = (
        series.fillna("")
        .astype(str)
        .str.strip()
    )
    vals = vals[(vals != "") & (~vals.str.lower().isin({x.lower() for x in exclude}))]
    if vals.empty:
        return "-"
    vc = vals.value_counts().head(n)
    return " · ".join(str(i) for i in vc.index.tolist()) if not vc.empty else "-"


def _gender_mix(df: pd.DataFrame, col: str = "gender") -> str:
    if df.empty:
        return "-"
    s = _safe_series(df, col).astype(str).str.upper().str.strip()
    mapping = {
        "M": "남성", "MALE": "남성", "남": "남성",
        "F": "여성", "FEMALE": "여성", "여": "여성",
        "UNKNOWN": "미상", "": "미상", "NAN": "미상"
    }
    s = s.map(lambda x: mapping.get(x, x if x else "미상"))
    vc = s.value_counts(normalize=True)
    parts = [f"{idx} {round(val*100)}%" for idx, val in vc.head(2).items()]
    return " · ".join(parts) if parts else "-"


def _persona_text(label: str, df: pd.DataFrame, extra: Optional[dict] = None) -> dict:
    extra = extra or {}
    cnt = len(df)
    age = _mode_label(_safe_series(df, "age_band"), fallback="미상", exclude={"UNKNOWN", "미상", "nan"})
    gender = _gender_mix(df)
    channel = _mode_label(_safe_series(df, "bucket"), fallback="미상", exclude={"UNKNOWN", "etc"})
    product = _topn_labels(_safe_series(df, "top_product"), n=2, exclude={"(not set)", "UNKNOWN", "nan"})
    category = _topn_labels(_safe_series(df, "top_category"), n=2, exclude={"(not set)", "UNKNOWN", "nan"})
    source = _topn_labels(_safe_series(df, "first_source"), n=2, exclude={"(not set)", "UNKNOWN", "nan"})
    avg_rev = _numeric_series(df, "total_revenue").mean() if cnt else 0
    avg_orders = _numeric_series(df, "order_count").mean() if cnt else 0
    avg_pv = _numeric_series(df, "total_pageviews").mean() if cnt else 0
    avg_cart = _numeric_series(df, "add_to_cart_count").mean() if cnt else 0
    return {
        "label": label,
        "count": cnt,
        "age": age,
        "gender": gender,
        "channel": channel,
        "product": product,
        "category": category,
        "source": source,
        "avg_revenue": avg_rev,
        "avg_orders": avg_orders,
        "avg_pv": avg_pv,
        "avg_cart": avg_cart,
        **extra,
    }


def _best_channel_insight(channel_df: pd.DataFrame, metric: str) -> str:
    if channel_df.empty or metric not in channel_df.columns:
        return "-"
    sdf = channel_df.copy()
    sdf = sdf.sort_values(metric, ascending=False)
    if sdf.empty:
        return "-"
    row = sdf.iloc[0]
    return str(row.get("bucket", "-"))


def _quality_summary(channel_rows: pd.DataFrame, member_df: pd.DataFrame) -> dict:
    total_users = float(channel_rows["users"].sum()) if not channel_rows.empty and "users" in channel_rows.columns else 0.0
    unknown_users = float(channel_rows.loc[channel_rows["bucket"].isin(["Unknown", "etc"]), "users"].sum()) if not channel_rows.empty else 0.0
    direct_users = float(channel_rows.loc[channel_rows["bucket"] == "Direct", "users"].sum()) if not channel_rows.empty else 0.0
    member_total = len(member_df)
    no_source = 0
    no_campaign = 0
    if not member_df.empty:
        no_source = int((_safe_series(member_df, "first_source").astype(str).str.strip().isin(["", "(not set)", "UNKNOWN", "nan"])) .sum())
        no_campaign = int((_safe_series(member_df, "first_campaign").astype(str).str.strip().isin(["", "(not set)", "UNKNOWN", "nan"])) .sum())
    return {
        "unknown_user_share": safe_div(unknown_users, total_users),
        "direct_user_share": safe_div(direct_users, total_users),
        "unknown_users": int(round(unknown_users)),
        "direct_users": int(round(direct_users)),
        "member_rows": member_total,
        "source_missing_share": safe_div(no_source, member_total),
        "campaign_missing_share": safe_div(no_campaign, member_total),
    }


def _compute_trend_summary(trend_df: pd.DataFrame) -> dict:
    if trend_df.empty:
        return {
            "points": [],
            "recent_rows": [],
            "max_users": 0,
            "max_revenue": 0.0,
            "peak_users_date": "-",
            "peak_users": 0,
            "peak_signups_date": "-",
            "peak_signups": 0,
            "peak_buyers_date": "-",
            "peak_buyers": 0,
            "peak_revenue_date": "-",
            "peak_revenue": 0.0,
        }
    df = trend_df.copy()
    for col in ["users", "signups", "buyers", "revenue"]:
        df[col] = pd.to_numeric(df.get(col), errors="coerce").fillna(0)
    df["dt"] = _safe_series(df, "dt").astype(str)
    points = df.to_dict(orient="records")
    recent_rows = df.sort_values("dt", ascending=False).head(14).to_dict(orient="records")
    def _peak(col: str):
        sdf = df.sort_values([col, "dt"], ascending=[False, False]).head(1)
        if sdf.empty:
            return "-", 0
        row = sdf.iloc[0]
        return str(row.get("dt", "-")), float(row.get(col, 0) or 0)
    pu_date, pu_val = _peak("users")
    ps_date, ps_val = _peak("signups")
    pb_date, pb_val = _peak("buyers")
    pr_date, pr_val = _peak("revenue")
    return {
        "points": points,
        "recent_rows": recent_rows,
        "max_users": int(max(df["users"].max(), df["signups"].max(), df["buyers"].max(), 0)),
        "max_revenue": float(max(df["revenue"].max(), 0)),
        "peak_users_date": pu_date,
        "peak_users": int(round(pu_val)),
        "peak_signups_date": ps_date,
        "peak_signups": int(round(ps_val)),
        "peak_buyers_date": pb_date,
        "peak_buyers": int(round(pb_val)),
        "peak_revenue_date": pr_date,
        "peak_revenue": float(pr_val),
    }


def _build_zero_trend_frame(start_date: str, end_date: str) -> pd.DataFrame:
    try:
        dates = pd.date_range(start=start_date, end=end_date, freq="D")
    except Exception:
        return pd.DataFrame(columns=["dt", "users", "signups", "buyers", "revenue"])
    return pd.DataFrame({
        "dt": dates.strftime("%Y-%m-%d"),
        "users": 0,
        "signups": 0,
        "buyers": 0,
        "revenue": 0.0,
    })


def _compute_channel_trend_summary(trend_df: pd.DataFrame, start_date: str, end_date: str, overall_summary: Optional[dict] = None) -> dict:
    overall_summary = overall_summary or {}
    result = {
        "tabs": [],
        "panels": {},
    }

    base_frame = _build_zero_trend_frame(start_date, end_date)
    overall_points = overall_summary.get("points", []) or []
    all_df = pd.DataFrame(overall_points) if overall_points else base_frame.copy()
    result["tabs"].append({"key": "all", "label": "All"})
    result["panels"]["all"] = {
        "key": "all",
        "label": "All",
        **_compute_trend_summary(all_df),
    }

    if trend_df.empty:
        return result

    df = trend_df.copy()
    for col in ["users", "signups", "buyers", "revenue"]:
        df[col] = pd.to_numeric(df.get(col), errors="coerce").fillna(0)
    df["dt"] = _safe_series(df, "dt").astype(str)
    df["bucket"] = df.apply(
        lambda r: canonical_bucket(r.get("bucket"), r.get("first_source"), r.get("first_medium"), r.get("first_campaign")), axis=1
    )
    grouped = (
        df.groupby(["bucket", "dt"], as_index=False)[["users", "signups", "buyers", "revenue"]]
        .sum()
    )
    bucket_totals = (
        grouped.groupby("bucket", as_index=False)[["users", "signups", "buyers", "revenue"]]
        .sum()
        .sort_values(by="bucket", key=lambda col: col.map(bucket_sort_key))
    )

    for _, brow in bucket_totals.iterrows():
        bucket = str(brow.get("bucket") or "Unknown")
        bdf = grouped[grouped["bucket"].astype(str) == bucket][["dt", "users", "signups", "buyers", "revenue"]].copy()
        if base_frame.empty:
            merged = bdf.sort_values("dt")
        else:
            merged = base_frame[["dt"]].merge(bdf, on="dt", how="left")
            for col in ["users", "signups", "buyers", "revenue"]:
                merged[col] = pd.to_numeric(merged.get(col), errors="coerce").fillna(0)
        summary = _compute_trend_summary(merged)
        summary.update({
            "key": bucket.lower().replace(" ", "_"),
            "label": bucket,
            "total_users": int(round(float(brow.get("users", 0) or 0))),
            "total_signups": int(round(float(brow.get("signups", 0) or 0))),
            "total_buyers": int(round(float(brow.get("buyers", 0) or 0))),
            "total_revenue": float(brow.get("revenue", 0) or 0),
        })
        result["tabs"].append({"key": summary["key"], "label": bucket})
        result["panels"][summary["key"]] = summary
    return result



def normalize_bundle(bundle: dict) -> dict:
    overview = _first_record(bundle.get("overview"))
    channel_rows = _as_dataframe(bundle.get("channel_snapshot"))
    bucket_detail = _as_dataframe(bundle.get("bucket_detail"))
    non_buyer = _as_dataframe(bundle.get("non_buyer"))
    buyer = _as_dataframe(bundle.get("buyer_revenue"))
    product = _as_dataframe(bundle.get("product_insight"))
    channel_product = _as_dataframe(bundle.get("channel_product"))
    target = _as_dataframe(bundle.get("target_candidates"))
    trend = _as_dataframe(bundle.get("daily_trend"))
    channel_trend_raw = _as_dataframe(bundle.get("channel_daily_trend"))

    for df in [bucket_detail, non_buyer, buyer, product, channel_product, target]:
        if not df.empty:
            df.fillna("", inplace=True)

    if not bucket_detail.empty:
        bucket_detail["bucket"] = bucket_detail.apply(
            lambda r: canonical_bucket(r.get("bucket"), r.get("source"), r.get("medium"), r.get("campaign")), axis=1
        )
        for col in ["users", "signups", "buyers", "revenue"]:
            bucket_detail[col] = pd.to_numeric(bucket_detail.get(col), errors="coerce").fillna(0)
        channel_rows = (
            bucket_detail.groupby("bucket", dropna=False, as_index=False)
            .agg({"users": "sum", "signups": "sum", "buyers": "sum", "revenue": "sum"})
        )
    elif not channel_rows.empty:
        channel_rows["bucket"] = channel_rows.apply(
            lambda r: canonical_bucket(r.get("bucket"), r.get("source"), r.get("medium"), r.get("campaign")), axis=1
        )

    if not channel_rows.empty:
        channel_rows["users"] = pd.to_numeric(channel_rows.get("users"), errors="coerce").fillna(0)
        channel_rows["signups"] = pd.to_numeric(channel_rows.get("signups"), errors="coerce").fillna(0)
        channel_rows["buyers"] = pd.to_numeric(channel_rows.get("buyers"), errors="coerce").fillna(0)
        channel_rows["revenue"] = pd.to_numeric(channel_rows.get("revenue"), errors="coerce").fillna(0)
        channel_rows["signup_rate"] = channel_rows.apply(lambda r: safe_div(r["signups"], r["users"]), axis=1)
        channel_rows["buyer_cvr"] = channel_rows.apply(lambda r: safe_div(r["buyers"], r["users"]), axis=1)
        channel_rows["aov"] = channel_rows.apply(lambda r: safe_div(r["revenue"], r["buyers"]), axis=1)
        channel_rows = channel_rows.sort_values(by=["bucket"], key=lambda col: col.map(bucket_sort_key)).reset_index(drop=True)

    # Normalize person-level tables
    member_frames = []
    if not non_buyer.empty:
        nb = non_buyer.copy()
        nb["person_type"] = "non_buyer"
        nb["top_product"] = _coalesce_text_series(
            _safe_series(nb, "last_viewed_product"),
            _safe_series(nb, "preferred_product", default="")
        )
        nb["top_category"] = _coalesce_text_series(
            _safe_series(nb, "last_viewed_category"),
            _safe_series(nb, "preferred_category", default="")
        )
        nb["total_revenue"] = _numeric_series(nb, "total_revenue")
        nb["order_count"] = _numeric_series(nb, "order_count")
        nb["bucket"] = nb.apply(lambda r: canonical_bucket(r.get("channel_group"), r.get("first_source"), "", r.get("first_campaign")), axis=1)
        member_frames.append(nb)
    if not buyer.empty:
        by = buyer.copy()
        by["person_type"] = by.apply(lambda r: "repeat_buyer" if float(r.get("order_count") or 0) >= 2 else "buyer", axis=1)
        by["top_product"] = _safe_series(by, "top_product")
        by["top_category"] = _safe_series(by, "top_category")
        by["bucket"] = by.apply(lambda r: canonical_bucket(r.get("channel_group"), r.get("first_source"), "", r.get("first_campaign")), axis=1)
        member_frames.append(by)

    if member_frames:
        members = pd.concat(member_frames, ignore_index=True, sort=False)
        members["member_key"] = _safe_series(members, "member_id").astype(str).str.strip()
        members.loc[members["member_key"] == "", "member_key"] = _safe_series(members, "user_id").astype(str).str.strip()
        members["sort_revenue"] = _numeric_series(members, "total_revenue")
        members = members.sort_values(["member_key", "sort_revenue"], ascending=[True, False])
        members = members.drop_duplicates(subset=["member_key"], keep="first")
        members = members[members["member_key"].astype(str).str.strip() != ""]
    else:
        members = pd.DataFrame(columns=["member_key"])

    # Attach target segment hints
    segment_bridge = {}
    if not target.empty:
        t = target.copy()
        t["member_key"] = _safe_series(t, "member_id").astype(str).str.strip()
        t.loc[t["member_key"] == "", "member_key"] = _safe_series(t, "user_id").astype(str).str.strip()
        for key, sdf in t.groupby("member_key"):
            if not key:
                continue
            segment_bridge[str(key)] = sorted(sdf["segment"].astype(str).unique().tolist(), key=segment_sort_key)
    if not members.empty:
        members["segments"] = members["member_key"].map(lambda x: segment_bridge.get(str(x), []))
    else:
        members["segments"] = []

    # Channel persona matrix
    matrix_rows = []
    bucket_member_detail = []
    channel_product_map = _as_dataframe(channel_product)
    if not channel_product_map.empty:
        channel_product_map["channel_group"] = channel_product_map["channel_group"].map(lambda x: canonical_bucket(x))
        channel_product_map["buyers"] = pd.to_numeric(channel_product_map.get("buyers"), errors="coerce").fillna(0)
        channel_product_map["revenue"] = pd.to_numeric(channel_product_map.get("revenue"), errors="coerce").fillna(0)

    member_cols_present = not members.empty
    for _, crow in channel_rows.iterrows() if not channel_rows.empty else []:
        bucket = str(crow.get("bucket") or "etc")
        sdf = members[members["bucket"].astype(str) == bucket].copy() if member_cols_present else pd.DataFrame()
        buyers_sdf = sdf[_numeric_series(sdf, "order_count") > 0] if not sdf.empty else pd.DataFrame()
        non_buyer_sdf = sdf[_numeric_series(sdf, "order_count") <= 0] if not sdf.empty else pd.DataFrame()
        if not channel_product_map.empty:
            cp = channel_product_map[channel_product_map["channel_group"].astype(str) == bucket].copy()
            top_prod = cp.sort_values(["revenue", "buyers"], ascending=False).head(2)["product_name"].astype(str).tolist()
            top_product = " · ".join(top_prod) if top_prod else _topn_labels(_safe_series(sdf, "top_product"), n=2, exclude={"(not set)", "UNKNOWN", "nan"})
        else:
            top_product = _topn_labels(_safe_series(sdf, "top_product"), n=2, exclude={"(not set)", "UNKNOWN", "nan"})
        matrix_rows.append({
            "bucket": bucket,
            "users": int(round(float(crow.get("users", 0) or 0))),
            "signups": int(round(float(crow.get("signups", 0) or 0))),
            "signup_rate": float(crow.get("signup_rate", 0) or 0),
            "buyers": int(round(float(crow.get("buyers", 0) or 0))),
            "buyer_cvr": float(crow.get("buyer_cvr", 0) or 0),
            "revenue": float(crow.get("revenue", 0) or 0),
            "aov": float(crow.get("aov", 0) or 0),
            "top_age_band": _mode_label(_safe_series(sdf, "age_band"), fallback="-", exclude={"UNKNOWN", "미상", "nan"}),
            "gender_mix": _gender_mix(sdf),
            "top_product": top_product,
            "top_source": _topn_labels(_safe_series(sdf, "first_source"), n=2, exclude={"(not set)", "UNKNOWN", "nan"}),
            "member_count": int(len(sdf)),
            "member_share": safe_div(len(sdf), max(len(members), 1)),
            "buyer_mix": safe_div(len(buyers_sdf), len(sdf)) if len(sdf) else 0.0,
            "non_buyer_mix": safe_div(len(non_buyer_sdf), len(sdf)) if len(sdf) else 0.0,
        })
        if not sdf.empty:
            source_detail = (
                sdf.assign(total_revenue_num=_numeric_series(sdf, "total_revenue"))
                  .groupby([_safe_series(sdf, "first_source").astype(str), _safe_series(sdf, "first_campaign").astype(str)], dropna=False)
            )
        if not sdf.empty:
            tmp = sdf.copy()
            tmp["first_source"] = _safe_series(tmp, "first_source").astype(str).replace("", "(not set)")
            tmp["first_campaign"] = _safe_series(tmp, "first_campaign").astype(str).replace("", "(not set)")
            tmp["age_band"] = _safe_series(tmp, "age_band").astype(str).replace("", "UNKNOWN")
            tmp["gender"] = _safe_series(tmp, "gender").astype(str).replace("", "UNKNOWN")
            tmp["top_product"] = _safe_series(tmp, "top_product").astype(str).replace("", "(not set)")
            tmp["top_category"] = _safe_series(tmp, "top_category").astype(str).replace("", "(not set)")
            tmp["total_revenue_num"] = _numeric_series(tmp, "total_revenue")
            detail = (
                tmp.groupby(["first_source", "first_campaign"], dropna=False)
                .agg(
                    members=("member_key", "count"),
                    buyers=("order_count", lambda x: int((pd.to_numeric(x, errors="coerce").fillna(0) > 0).sum())),
                    revenue=("total_revenue_num", "sum"),
                    age_band=("age_band", lambda x: _mode_label(pd.Series(x), fallback="-", exclude={"UNKNOWN", "미상", "nan"})),
                    gender=("gender", lambda x: _gender_mix(pd.DataFrame({"gender": list(x)}))),
                    top_product=("top_product", lambda x: _topn_labels(pd.Series(x), n=1, exclude={"(not set)", "UNKNOWN", "nan"})),
                    top_category=("top_category", lambda x: _topn_labels(pd.Series(x), n=1, exclude={"(not set)", "UNKNOWN", "nan"})),
                )
                .reset_index()
                .sort_values(["members", "revenue"], ascending=False)
            )
            for _, dr in detail.head(12).iterrows():
                bucket_member_detail.append({
                    "bucket": bucket,
                    "first_source": dr.get("first_source", "(not set)"),
                    "first_campaign": dr.get("first_campaign", "(not set)"),
                    "members": int(dr.get("members", 0) or 0),
                    "buyers": int(dr.get("buyers", 0) or 0),
                    "revenue": float(dr.get("revenue", 0) or 0),
                    "age_band": dr.get("age_band", "-"),
                    "gender": dr.get("gender", "-"),
                    "top_product": dr.get("top_product", "-"),
                    "top_category": dr.get("top_category", "-"),
                })

    matrix_df = pd.DataFrame(matrix_rows)
    if not matrix_df.empty:
        matrix_df = matrix_df.sort_values(by=["bucket"], key=lambda col: col.map(bucket_sort_key)).reset_index(drop=True)

    # Persona cards
    high_intent_df = pd.DataFrame()
    if not target.empty:
        high_intent_df = target[target["segment"].astype(str).isin(["high_intent", "cart_abandon"])].copy()
        if not high_intent_df.empty:
            high_intent_df["bucket"] = high_intent_df.apply(lambda r: canonical_bucket(r.get("channel_group"), r.get("first_source"), "", r.get("first_campaign")), axis=1)
            high_intent_df["top_product"] = _safe_series(high_intent_df, "preferred_product")
            high_intent_df["top_category"] = _safe_series(high_intent_df, "preferred_category")

    signup_members = members.copy() if not members.empty else pd.DataFrame()
    buyers_only = members[_numeric_series(members, "order_count") > 0].copy() if not members.empty else pd.DataFrame()
    repeat_only = members[_numeric_series(members, "order_count") >= 2].copy() if not members.empty else pd.DataFrame()
    non_buyers_only = members[_numeric_series(members, "order_count") <= 0].copy() if not members.empty else pd.DataFrame()

    personas = [
        _persona_text("Identified Members", signup_members, {"headline": "식별 가능한 회원 기반 프로필", "focus": "가입 이후 행동이 잡히는 사람들"}),
        _persona_text("Buyers", buyers_only, {"headline": "실구매자 대표 프로필", "focus": "첫 구매와 반복 구매 연결"}),
        _persona_text("Repeat Buyers", repeat_only, {"headline": "반복구매자 대표 프로필", "focus": "LTV 확장에 유리한 고객"}),
        _persona_text("High Intent", high_intent_df, {"headline": "장바구니 / 고의도 잠재고객", "focus": "즉시 메시지 발송 우선군"}),
    ]

    # Source / campaign entry points
    entry_points = []
    if not signup_members.empty:
        tmp = signup_members.copy()
        tmp["first_source"] = _safe_series(tmp, "first_source").astype(str).replace("", "(not set)")
        tmp["first_campaign"] = _safe_series(tmp, "first_campaign").astype(str).replace("", "(not set)")
        tmp["bucket"] = _safe_series(tmp, "bucket").astype(str).replace("", "Unknown")
        tmp["revenue_num"] = _numeric_series(tmp, "total_revenue")
        ep = (
            tmp.groupby(["bucket", "first_source", "first_campaign"], dropna=False)
            .agg(
                members=("member_key", "count"),
                buyers=("order_count", lambda x: int((pd.to_numeric(x, errors="coerce").fillna(0) > 0).sum())),
                revenue=("revenue_num", "sum"),
                age_band=("age_band", lambda x: _mode_label(pd.Series(x), fallback="-", exclude={"UNKNOWN", "미상", "nan"})),
                product=("top_product", lambda x: _topn_labels(pd.Series(x), n=1, exclude={"(not set)", "UNKNOWN", "nan"})),
            )
            .reset_index()
            .sort_values(["members", "revenue"], ascending=False)
        )
        entry_points = ep.head(24).to_dict(orient="records")

    # Flow / action summaries
    action_cards = []
    if not matrix_df.empty:
        top_signup = matrix_df.sort_values("signups", ascending=False).head(1)
        top_buyer = matrix_df.sort_values("buyers", ascending=False).head(1)
        top_revenue = matrix_df.sort_values("revenue", ascending=False).head(1)
        if not top_signup.empty:
            r = top_signup.iloc[0]
            action_cards.append({
                "title": f"가입 볼륨 최대 채널: {r['bucket']}",
                "text": f"식별 회원 기준 대표 연령대는 {r.get('top_age_band','-')}이고, 주요 소스는 {r.get('top_source','-')}입니다. 대량 리드 수집 후 후속 온드 메시지 연결이 중요한 구간입니다.",
                "bullets": [
                    f"회원가입 {fmt_int(r.get('signups',0))}",
                    f"가입률 {fmt_pct(r.get('signup_rate',0))}",
                    f"대표 상품 {r.get('top_product','-')}"
                ]
            })
        if not top_buyer.empty:
            r = top_buyer.iloc[0]
            action_cards.append({
                "title": f"구매자 전환 강한 채널: {r['bucket']}",
                "text": f"유입 대비 구매 전환이 상대적으로 강한 채널입니다. 구매자 확장 또는 리마케팅 예산 배분 시 우선 검토할 만합니다.",
                "bullets": [
                    f"구매자 {fmt_int(r.get('buyers',0))}",
                    f"Buyer CVR {fmt_pct(r.get('buyer_cvr',0))}",
                    f"AOV {fmt_money(r.get('aov',0))}"
                ]
            })
        if not top_revenue.empty:
            r = top_revenue.iloc[0]
            action_cards.append({
                "title": f"매출 기여 최대 채널: {r['bucket']}",
                "text": f"매출 기여가 가장 큰 채널입니다. 반복구매 가능성이 높은 카테고리와 연결해 CRM nurture를 설계하는 게 좋습니다.",
                "bullets": [
                    f"매출 {fmt_money(r.get('revenue',0))}",
                    f"대표 연령대 {r.get('top_age_band','-')}",
                    f"대표 상품 {r.get('top_product','-')}"
                ]
            })

    funnel = [
        {"key": "users", "label": "Users", "value": int(float(overview.get("users", 0) or 0)), "rate": 1.0},
        {"key": "signup_users", "label": "Sign-up", "value": int(float(overview.get("signup_users", 0) or 0)), "rate": safe_div(float(overview.get("signup_users", 0) or 0), float(overview.get("users", 0) or 0))},
        {"key": "non_buyer_members", "label": "Non Buyer", "value": int(float(overview.get("non_buyer_members", 0) or 0)), "rate": safe_div(float(overview.get("non_buyer_members", 0) or 0), float(overview.get("signup_users", 0) or 0))},
        {"key": "buyers", "label": "Buyer", "value": int(float(overview.get("buyers", 0) or 0)), "rate": safe_div(float(overview.get("buyers", 0) or 0), float(overview.get("signup_users", 0) or 0))},
        {"key": "repeat_buyers", "label": "Repeat", "value": int(float(overview.get("repeat_buyers", 0) or 0)), "rate": safe_div(float(overview.get("repeat_buyers", 0) or 0), float(overview.get("buyers", 0) or 0))},
    ]

    non_buyer_summary = {
        "signup_non_buyer": int(len(non_buyers_only)),
        "high_pv_non_buyer": int((_numeric_series(non_buyer, "product_view_count") >= 3).sum()) if not non_buyer.empty else 0,
        "cart_abandon": int((_numeric_series(non_buyer, "add_to_cart_count") >= 1).sum()) if not non_buyer.empty else 0,
        "avg_pv": round(_numeric_series(non_buyer, "total_pageviews").mean(), 1) if not non_buyer.empty else 0.0,
    }

    buyer_summary = {
        "buyers": int(float(overview.get("buyers", 0) or 0)),
        "first_buyers": int((_numeric_series(buyer, "order_count") == 1).sum()) if not buyer.empty else 0,
        "repeat_buyers": int((_numeric_series(buyer, "order_count") >= 2).sum()) if not buyer.empty else 0,
        "revenue": float(overview.get("revenue", 0) or 0),
        "revenue_per_buyer": safe_div(float(overview.get("revenue", 0) or 0), float(overview.get("buyers", 0) or 0)),
        "aov": float(overview.get("aov", 0) or 0),
    }

    target_counts = {}
    if not target.empty:
        for seg, g in target.groupby("segment"):
            target_counts[str(seg)] = int(len(g))

    quality = _quality_summary(channel_rows, members)
    trend_summary = _compute_trend_summary(trend)
    dr = bundle.get("date_range", {})
    if not isinstance(dr, dict):
        dr = _first_record(dr)
    channel_trend_summary = _compute_channel_trend_summary(channel_trend_raw, str(dr.get("start_date","")), str(dr.get("end_date","")), trend_summary)
    overview_cards = {
        "signup_rate": safe_div(float(overview.get("signup_users", 0) or 0), float(overview.get("users", 0) or 0)),
        "buyer_cvr": safe_div(float(overview.get("buyers", 0) or 0), float(overview.get("users", 0) or 0)),
        "repeat_rate": safe_div(float(overview.get("repeat_buyers", 0) or 0), float(overview.get("buyers", 0) or 0)),
        "revenue_per_user": safe_div(float(overview.get("revenue", 0) or 0), float(overview.get("users", 0) or 0)),
    }

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
        "overview_cards": overview_cards,
        "funnel": funnel,
        "channel_snapshot": channel_rows.to_dict(orient="records") if not channel_rows.empty else [],
        "channel_matrix": matrix_df.to_dict(orient="records") if not matrix_df.empty else [],
        "bucket_detail": bucket_detail.fillna("").to_dict(orient="records"),
        "bucket_member_detail": bucket_member_detail,
        "non_buyer_summary": non_buyer_summary,
        "non_buyer": non_buyer.fillna("").to_dict(orient="records"),
        "buyer_summary": buyer_summary,
        "buyer_revenue": buyer.fillna("").to_dict(orient="records"),
        "product_insight": product.fillna("").to_dict(orient="records"),
        "channel_product": channel_product.fillna("").to_dict(orient="records"),
        "target_candidates": target.fillna("").to_dict(orient="records"),
        "target_counts": target_counts,
        "personas": personas,
        "entry_points": entry_points,
        "quality": quality,
        "action_cards": action_cards,
        "daily_trend": trend_summary,
        "channel_daily_trend": channel_trend_summary,
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


def bucket_tabs_html(target: str, buckets: List[str], active: str) -> str:
    return (
        f'<div class="bucket-pills">'
        + "".join(
            f'<button type="button" class="bucket-pill {"active" if bucket == active else ""}" data-target="{esc(target)}" data-bucket="{esc(bucket)}">{esc(bucket)}</button>'
            for bucket in buckets
        )
        + "</div>"
    )


def render_kpi_cards(data: dict) -> str:
    overview = data["overview"]
    oc = data.get("overview_cards", {})
    cards = [
        ("Tracked Users", fmt_int(overview["users"]), f"회원 퍼널 기준 전체 추적 유입 · 가입률 {fmt_pct(oc.get('signup_rate', 0))}"),
        ("Sign-up Users", fmt_int(overview["signup_users"]), f"식별 가능한 회원가입 완료자 · Buyer CVR {fmt_pct(oc.get('buyer_cvr', 0))}"),
        ("Non Buyer Members", fmt_int(overview["non_buyer_members"]), "가입 후 아직 구매하지 않은 회원"),
        ("Buyers", fmt_int(overview["buyers"]), f"실구매 회원 · Repeat rate {fmt_pct(oc.get('repeat_rate', 0))}"),
        ("Revenue", fmt_money(overview["revenue"]), f"회원 기준 누적 매출 · Revenue/User {fmt_money(oc.get('revenue_per_user', 0))}"),
        ("AOV", fmt_money(overview["aov"]), "회원 기준 평균 주문 금액"),
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
    return "".join(
        f"""
        <div class="funnel-step">
          <div class="funnel-label">{esc(item['label'])}</div>
          <div class="funnel-value">{fmt_int(item['value'])}</div>
          <div class="funnel-rate">{fmt_pct(item['rate'])}</div>
        </div>
        """ for item in data["funnel"]
    )


def render_persona_cards(data: dict) -> str:
    cards = []
    for p in data.get("personas", []):
        cards.append(
            f"""
            <div class="persona-card">
              <div class="persona-head">
                <div>
                  <div class="section-kicker">Customer Snapshot</div>
                  <div class="persona-title">{esc(p.get('label','-'))}</div>
                </div>
                <span class="persona-badge">{fmt_int(p.get('count',0))}</span>
              </div>
              <div class="persona-main">
                <strong>{esc(p.get('headline','대표 프로필'))}</strong><br/>
                주요 연령대는 <strong>{esc(p.get('age','-'))}</strong>, 성별 믹스는 <strong>{esc(p.get('gender','-'))}</strong>,
                대표 유입 채널은 <strong>{esc(p.get('channel','-'))}</strong> 입니다.
              </div>
              <div class="persona-stats">
                <div class="mini-card">
                  <div class="mini-label">Top Product</div>
                  <div class="mini-value">{esc(p.get('product','-'))}</div>
                  <div class="mini-meta">대표 관심 / 구매 상품</div>
                </div>
                <div class="mini-card">
                  <div class="mini-label">Top Category</div>
                  <div class="mini-value">{esc(p.get('category','-'))}</div>
                  <div class="mini-meta">대표 카테고리</div>
                </div>
                <div class="mini-card">
                  <div class="mini-label">Main Source</div>
                  <div class="mini-value">{esc(p.get('source','-'))}</div>
                  <div class="mini-meta">가입/유입에 많이 등장한 source</div>
                </div>
                <div class="mini-card">
                  <div class="mini-label">Behavior</div>
                  <div class="mini-value">PV {fmt_int(round(float(p.get('avg_pv',0) or 0)))}</div>
                  <div class="mini-meta">평균 주문 {round(float(p.get('avg_orders',0) or 0),1)} · 평균 매출 {fmt_money(p.get('avg_revenue',0))}</div>
                </div>
              </div>
            </div>
            """
        )
    return "".join(cards) if cards else '<div class="empty-note">표시할 고객 프로필 데이터가 없습니다.</div>'


def render_channel_snapshot(data: dict) -> str:
    rows = data.get("channel_matrix", [])
    if not rows:
        return """
        <div class="report-card">
          <div class="section-head">
            <div>
              <div class="section-kicker">Channel Matrix</div>
              <div class="section-title">채널별 가입 / 구매 구조</div>
              <div class="section-sub">표시할 채널 데이터가 없습니다.</div>
            </div>
          </div>
          <div class="empty-note">channel_snapshot 또는 bucket_detail 결과가 비어 있습니다.</div>
        </div>
        """
    body = []
    for r in rows:
        body.append(
            f"""
            <tr class="bucket-row" data-bucket-target="channel-detail" data-bucket="{esc(r.get('bucket','etc'))}">
              <td><span class="tag slate">{esc(r.get('bucket','etc'))}</span></td>
              <td class="num">{fmt_int(r.get('users',0))}</td>
              <td class="num">{fmt_int(r.get('signups',0))}</td>
              <td class="num">{fmt_pct(r.get('signup_rate',0))}</td>
              <td class="num">{fmt_int(r.get('buyers',0))}</td>
              <td class="num">{fmt_pct(r.get('buyer_cvr',0))}</td>
              <td class="num">{fmt_money(r.get('revenue',0))}</td>
              <td class="num">{fmt_money(r.get('aov',0))}</td>
              <td>{esc(r.get('top_age_band','-'))}</td>
              <td>{esc(r.get('gender_mix','-'))}</td>
              <td>{esc(r.get('top_source','-'))}</td>
              <td>{esc(r.get('top_product','-'))}</td>
            </tr>
            """
        )

    buckets = [str(r.get("bucket","etc")) for r in rows]
    details = data.get("bucket_member_detail", [])
    detail_panels = []
    for idx, bucket in enumerate(buckets):
        sdf = [r for r in details if str(r.get("bucket")) == bucket]
        table_rows = "".join(
            f"""
            <tr>
              <td>{esc(r.get('first_source','(not set)'))}</td>
              <td>{esc(r.get('first_campaign','(not set)'))}</td>
              <td class="num">{fmt_int(r.get('members',0))}</td>
              <td class="num">{fmt_int(r.get('buyers',0))}</td>
              <td class="num">{fmt_money(r.get('revenue',0))}</td>
              <td>{esc(r.get('age_band','-'))}</td>
              <td>{esc(r.get('gender','-'))}</td>
              <td>{esc(r.get('top_product','-'))}</td>
              <td>{esc(r.get('top_category','-'))}</td>
            </tr>
            """ for r in sdf
        ) or '<tr><td colspan="9" class="muted">상세 source/campaign 데이터가 없습니다.</td></tr>'
        detail_panels.append(
            f"""
            <div class="bucket-detail {"active" if idx == 0 else ""}" data-target="channel-detail" data-bucket="{esc(bucket)}">
              <div class="section-head">
                <div>
                  <div class="section-kicker">Channel Deep Dive</div>
                  <div class="section-title">{esc(bucket)} 유입자 세부 구조</div>
                  <div class="section-sub">어떤 source / campaign에서 어떤 사람이 가입하고 구매했는지</div>
                </div>
              </div>
              <div class="table-wrap">
                <table class="data-table compact-table">
                  <thead>
                    <tr>
                      <th>Source</th>
                      <th>Campaign</th>
                      <th class="num">Members</th>
                      <th class="num">Buyers</th>
                      <th class="num">Revenue</th>
                      <th>Age Band</th>
                      <th>Gender Mix</th>
                      <th>Top Product</th>
                      <th>Top Category</th>
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
          <div class="section-kicker">Channel Matrix</div>
          <div class="section-title">채널별 유입 · 가입 · 구매 · 사람 특성</div>
          <div class="section-sub">OWNED / PAID / AWARENESS / DIRECT까지 한 화면에서 비교하고, 클릭하면 source/campaign 디테일까지 확인할 수 있습니다.</div>
        </div>
      </div>
      <div class="table-wrap">
        <table class="data-table compact-table">
          <thead>
            <tr>
              <th>Channel</th>
              <th class="num">Users</th>
              <th class="num">Sign-ups</th>
              <th class="num">Signup Rate</th>
              <th class="num">Buyers</th>
              <th class="num">Buyer CVR</th>
              <th class="num">Revenue</th>
              <th class="num">AOV</th>
              <th>Top Age</th>
              <th>Gender Mix</th>
              <th>Top Source</th>
              <th>Top Product</th>
            </tr>
          </thead>
          <tbody>{''.join(body)}</tbody>
        </table>
      </div>
    </div>

    <div class="report-card">
      <div class="section-head">
        <div>
          <div class="section-kicker">Channel Drilldown</div>
          <div class="section-title">채널별 가입 소스 상세</div>
          <div class="section-sub">채널을 바꿔가며 source / campaign / 구매자 구조를 확인</div>
        </div>
        {bucket_tabs_html('channel-detail', buckets, buckets[0] if buckets else '')}
      </div>
      {''.join(detail_panels)}
    </div>
    """


def render_entry_points(data: dict) -> str:
    rows = data.get("entry_points", [])
    tr_html = "".join(
        f"""
        <tr>
          <td><span class="tag slate">{esc(r.get('bucket','-'))}</span></td>
          <td>{esc(r.get('first_source','(not set)'))}</td>
          <td>{esc(r.get('first_campaign','(not set)'))}</td>
          <td class="num">{fmt_int(r.get('members',0))}</td>
          <td class="num">{fmt_int(r.get('buyers',0))}</td>
          <td class="num">{fmt_money(r.get('revenue',0))}</td>
          <td>{esc(r.get('age_band','-'))}</td>
          <td>{esc(r.get('product','-'))}</td>
        </tr>
        """ for r in rows
    ) or '<tr><td colspan="8" class="muted">표시할 엔트리포인트 데이터가 없습니다.</td></tr>'
    actions = data.get("action_cards", [])
    action_html = "".join(
        f"""
        <div class="flow-card">
          <div class="flow-title">{esc(card.get('title','-'))}</div>
          <div class="flow-text">{esc(card.get('text','-'))}</div>
          <div class="bullet-list">{''.join(f'<div class="bullet">{esc(b)}</div>' for b in card.get('bullets', []))}</div>
        </div>
        """ for card in actions
    ) or '<div class="empty-note">아직 생성된 액션 인사이트가 없습니다.</div>'
    return f"""
    <div class="report-card">
      <div class="section-head">
        <div>
          <div class="section-kicker">Signup Attribution</div>
          <div class="section-title">어느 채널에서 어떤 사람이 가입했는지</div>
          <div class="section-sub">가입 기준 source / campaign 진입점과 바로 실행 가능한 액션 요약</div>
        </div>
      </div>
      <div class="panel-grid">
        <div class="subcard">
          <div class="section-kicker">Top Entry Points</div>
          <div class="table-wrap">
            <table class="data-table compact-table">
              <thead>
                <tr>
                  <th>Channel</th>
                  <th>Source</th>
                  <th>Campaign</th>
                  <th class="num">Members</th>
                  <th class="num">Buyers</th>
                  <th class="num">Revenue</th>
                  <th>Top Age</th>
                  <th>Top Product</th>
                </tr>
              </thead>
              <tbody>{tr_html}</tbody>
            </table>
          </div>
        </div>
        <div class="subcard">
          <div class="section-kicker">Operator Actions</div>
          <div class="flow-grid">{action_html}</div>
        </div>
      </div>
    </div>
    """


def render_non_buyer(data: dict) -> str:
    rows = data.get("non_buyer", [])
    summary = data.get("non_buyer_summary", {})
    table_rows = "".join(
        f"""
        <tr>
          <td>{esc(r.get('member_id',''))}</td>
          <td>{esc(r.get('gender','UNKNOWN'))}</td>
          <td>{esc(r.get('age_band','UNKNOWN'))}</td>
          <td><span class="tag slate">{esc(canonical_bucket(r.get('channel_group'), r.get('first_source'), '', r.get('first_campaign')))}</span></td>
          <td>{esc(r.get('first_source','(not set)'))}</td>
          <td>{esc(r.get('first_campaign','(not set)'))}</td>
          <td class="num">{fmt_int(r.get('total_pageviews',0))}</td>
          <td class="num">{fmt_int(r.get('product_view_count',0))}</td>
          <td class="num">{fmt_int(r.get('add_to_cart_count',0))}</td>
          <td>{esc(r.get('last_viewed_category','(not set)'))}</td>
          <td>{esc(r.get('last_viewed_product','(not set)'))}</td>
          <td><span class="tag amber">{esc(r.get('recommended_message','FIRST_PURCHASE_COUPON'))}</span></td>
        </tr>
        """ for r in rows[:160]
    ) or '<tr><td colspan="12" class="muted">비구매 회원 데이터가 없습니다.</td></tr>'
    stats = [
        ("Non Buyer Members", fmt_int(summary.get("signup_non_buyer", 0)), "가입 후 미구매 회원 수"),
        ("High PV", fmt_int(summary.get("high_pv_non_buyer", 0)), "상품 조회 3회 이상"),
        ("Cart Abandon", fmt_int(summary.get("cart_abandon", 0)), "장바구니 흔적 보유"),
        ("Avg PV", str(summary.get("avg_pv", 0)), "평균 페이지뷰"),
    ]
    stat_html = "".join(
        f'<div class="stat-card"><div class="stat-label">{esc(a)}</div><div class="stat-value">{esc(b)}</div><div class="stat-meta">{esc(c)}</div></div>'
        for a,b,c in stats
    )
    return f"""
    <div class="report-card">
      <div class="section-head">
        <div>
          <div class="section-kicker">Non Buyer</div>
          <div class="section-title">가입했지만 아직 사지 않은 사람</div>
          <div class="section-sub">상품 조회, 장바구니, 마지막 관심 상품 기준으로 재접촉 우선순위를 볼 수 있습니다.</div>
        </div>
      </div>
      <div class="stat-grid">{stat_html}</div>
      <div class="subcard" style="margin-top:16px">
        <div class="section-kicker">Priority List</div>
        <div class="table-wrap">
          <table class="data-table compact-table">
            <thead>
              <tr>
                <th>Member ID</th>
                <th>Gender</th>
                <th>Age Band</th>
                <th>Channel</th>
                <th>Source</th>
                <th>Campaign</th>
                <th class="num">PV</th>
                <th class="num">Product Views</th>
                <th class="num">Add to Cart</th>
                <th>Last Category</th>
                <th>Last Product</th>
                <th>Recommended Message</th>
              </tr>
            </thead>
            <tbody>{table_rows}</tbody>
          </table>
        </div>
      </div>
    </div>
    """


def render_buyer_revenue(data: dict) -> str:
    rows = data.get("buyer_revenue", [])
    summary = data.get("buyer_summary", {})
    stats = [
        ("Buyers", fmt_int(summary.get("buyers", 0)), "실구매 회원 수"),
        ("1st Buyers", fmt_int(summary.get("first_buyers", 0)), "1회 구매 회원"),
        ("Repeat Buyers", fmt_int(summary.get("repeat_buyers", 0)), "2회 이상 구매 회원"),
        ("Revenue / Buyer", fmt_money(summary.get("revenue_per_buyer", 0)), "구매자당 평균 매출"),
    ]
    stat_html = "".join(
        f'<div class="stat-card"><div class="stat-label">{esc(a)}</div><div class="stat-value">{esc(b)}</div><div class="stat-meta">{esc(c)}</div></div>'
        for a,b,c in stats
    )
    table_rows = "".join(
        f"""
        <tr>
          <td>{esc(r.get('member_id',''))}</td>
          <td>{esc(r.get('gender','UNKNOWN'))}</td>
          <td>{esc(r.get('age_band','UNKNOWN'))}</td>
          <td><span class="tag slate">{esc(canonical_bucket(r.get('channel_group'), r.get('first_source'), '', r.get('first_campaign')))}</span></td>
          <td>{esc(r.get('first_source','(not set)'))}</td>
          <td>{esc(r.get('first_campaign','(not set)'))}</td>
          <td>{esc(r.get('top_category','(not set)'))}</td>
          <td>{esc(r.get('top_product','(not set)'))}</td>
          <td class="num">{fmt_int(r.get('order_count',0))}</td>
          <td class="num">{fmt_int(r.get('total_quantity',0))}</td>
          <td class="num">{fmt_money(r.get('aov',0))}</td>
          <td class="num">{fmt_money(r.get('total_revenue',0))}</td>
        </tr>
        """ for r in rows[:160]
    ) or '<tr><td colspan="12" class="muted">구매 회원 데이터가 없습니다.</td></tr>'
    return f"""
    <div class="report-card">
      <div class="section-head">
        <div>
          <div class="section-kicker">Buyer Revenue</div>
          <div class="section-title">누가 매출을 만들었는지</div>
          <div class="section-sub">채널, 상품, 주문 수 기준으로 고가치 구매자를 빠르게 확인합니다.</div>
        </div>
      </div>
      <div class="stat-grid">{stat_html}</div>
      <div class="subcard" style="margin-top:16px">
        <div class="section-kicker">Buyer Table</div>
        <div class="table-wrap">
          <table class="data-table compact-table">
            <thead>
              <tr>
                <th>Member ID</th>
                <th>Gender</th>
                <th>Age Band</th>
                <th>Channel</th>
                <th>Source</th>
                <th>Campaign</th>
                <th>Top Category</th>
                <th>Top Product</th>
                <th class="num">Orders</th>
                <th class="num">Qty</th>
                <th class="num">AOV</th>
                <th class="num">Revenue</th>
              </tr>
            </thead>
            <tbody>{table_rows}</tbody>
          </table>
        </div>
      </div>
    </div>
    """


def render_product_insight(data: dict) -> str:
    product_rows = data.get("product_insight", [])
    channel_rows = data.get("channel_product", [])
    product_table = "".join(
        f"""
        <tr>
          <td>{esc(r.get('product_name','(not set)'))}</td>
          <td>{esc(r.get('category','(not set)'))}</td>
          <td class="num">{fmt_int(r.get('buyers',0))}</td>
          <td class="num">{fmt_int(r.get('orders',0))}</td>
          <td class="num">{fmt_int(r.get('quantity',0))}</td>
          <td class="num">{fmt_money(r.get('revenue',0))}</td>
          <td class="num">{fmt_pct(r.get('first_purchase_share',0))}</td>
          <td class="num">{fmt_pct(r.get('repeat_share',0))}</td>
        </tr>
        """ for r in product_rows[:80]
    ) or '<tr><td colspan="8" class="muted">상품 인사이트 데이터가 없습니다.</td></tr>'
    channel_table = "".join(
        f"""
        <tr>
          <td><span class="tag slate">{esc(canonical_bucket(r.get('channel_group')))}</span></td>
          <td>{esc(r.get('product_name','(not set)'))}</td>
          <td class="num">{fmt_int(r.get('buyers',0))}</td>
          <td class="num">{fmt_money(r.get('revenue',0))}</td>
        </tr>
        """ for r in channel_rows[:80]
    ) or '<tr><td colspan="4" class="muted">채널별 상품 데이터가 없습니다.</td></tr>'
    return f"""
    <div class="report-card">
      <div class="section-head">
        <div>
          <div class="section-kicker">Product Insight</div>
          <div class="section-title">무슨 상품이 고객을 움직였는지</div>
          <div class="section-sub">대표 상품, 카테고리, 채널별 매출 연결 구조</div>
        </div>
      </div>
      <div class="panel-grid">
        <div class="subcard">
          <div class="section-kicker">Top Products</div>
          <div class="table-wrap">
            <table class="data-table compact-table">
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
            <table class="data-table compact-table">
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
    rows = data.get("target_candidates", [])
    df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["segment","member_id","user_id","channel_group","first_source","first_campaign","preferred_category","preferred_product","last_order_date","total_revenue","recommended_message"])
    segments = sorted(df["segment"].dropna().astype(str).unique().tolist(), key=segment_sort_key) if not df.empty and "segment" in df.columns else TARGET_SEGMENT_ORDER[:]
    if not segments:
        segments = TARGET_SEGMENT_ORDER[:]
    counts = data.get("target_counts", {})
    summary_items = "".join(
        f'<div class="summary-item"><div class="label">{esc(seg)}</div><div class="value">{fmt_int(counts.get(seg,0))}</div></div>'
        for seg in TARGET_SEGMENT_ORDER
    )
    panels = []
    for idx, seg in enumerate(segments):
        sdf = df[df["segment"].astype(str) == seg].copy() if not df.empty else pd.DataFrame()
        table_rows = "".join(
            f"""
            <tr>
              <td>{esc(r.get('member_id',''))}</td>
              <td>{esc(r.get('user_id',''))}</td>
              <td><span class="tag slate">{esc(canonical_bucket(r.get('channel_group'), r.get('first_source'), '', r.get('first_campaign')))}</span></td>
              <td>{esc(r.get('first_source','(not set)'))}</td>
              <td>{esc(r.get('first_campaign','(not set)'))}</td>
              <td>{esc(r.get('preferred_category','(not set)'))}</td>
              <td>{esc(r.get('preferred_product','(not set)'))}</td>
              <td>{esc(r.get('last_order_date',''))}</td>
              <td class="num">{fmt_money(r.get('total_revenue',0))}</td>
              <td><span class="tag {pick_tag_class(str(r.get('recommended_message','')))}">{esc(r.get('recommended_message',''))}</span></td>
            </tr>
            """ for _, r in sdf.head(160).iterrows()
        ) or '<tr><td colspan="10" class="muted">대상자가 없습니다.</td></tr>'
        panels.append(
            f"""
            <div class="segment-panel {"active" if idx == 0 else ""}" data-target="target-candidates" data-segment="{esc(seg)}">
              <div class="table-wrap">
                <table class="data-table compact-table">
                  <thead>
                    <tr>
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
          <div class="section-title">지금 바로 액션 가능한 대상자</div>
          <div class="section-sub">세그먼트별로 채널 / 관심상품 / 추천 메시지를 같이 보여줍니다.</div>
        </div>
        {segment_tabs_html('target-candidates', segments, segments[0] if segments else 'non_buyer')}
      </div>
      <div class="panel-grid">
        <div class="subcard">{''.join(panels)}</div>
        <div class="subcard">
          <div class="section-kicker">Segment Counts</div>
          <div class="summary-list">{summary_items}</div>
          <div class="section-kicker" style="margin-top:16px">Action Guide</div>
          <div class="bullet-list">
            <div class="bullet">non_buyer / cart_abandon: 첫 구매 쿠폰, 장바구니 리마인드, 재방문 유도</div>
            <div class="bullet">high_intent: 최근 관심 상품 중심 메시지, 24~72시간 후속 리마케팅</div>
            <div class="bullet">repeat_buyer / vip: 신상품 선공개, 카테고리 기반 업셀</div>
            <div class="bullet">dormant: 휴면 복귀 혜택, 대표 구매 카테고리 재노출</div>
          </div>
        </div>
      </div>
    </div>
    """



def build_period_nav(current_key: str = "1m") -> str:
    items = []
    for preset in PERIOD_PRESETS:
        href = "./" + preset["filename"]
        cls = "chip active" if preset["key"] == current_key else "chip"
        items.append(
            f'<a class="{cls}" href="{esc(href)}" style="text-decoration:none">{esc(preset["label"])} · {esc(preset["description"])}</a>'
        )
    return "".join(items)


def period_label_from_data(data: dict) -> str:
    meta = data.get("period_meta", {})
    if not isinstance(meta, dict):
        meta = _first_record(meta)
    label = str(meta.get("label") or "").strip()
    desc = str(meta.get("description") or "").strip()
    if label and desc:
        return f"{label} · {desc}"
    if label:
        return label
    dr = data.get("date_range", {})
    if not isinstance(dr, dict):
        dr = _first_record(dr)
    return f'{dr.get("start_date","")} ~ {dr.get("end_date","")}'.strip(" ~")

def _svg_polyline(points: list[tuple[float, float]]) -> str:
    return " ".join(f"{round(x,1)},{round(y,1)}" for x, y in points)



def render_daily_trend(data: dict) -> str:
    trend = data.get("daily_trend", {})
    if not isinstance(trend, dict):
        trend = {}
    channel_trend = data.get("channel_daily_trend", {})
    if not isinstance(channel_trend, dict):
        channel_trend = {}
    points = trend.get("points", []) or []
    if not points:
        return f"""
        <div class="report-card">
          <div class="section-head">
            <div>
              <div class="section-kicker">Daily Trend</div>
              <div class="section-title">일자별 추이</div>
              <div class="section-sub">표시할 일자별 시계열 데이터가 없습니다.</div>
            </div>
          </div>
          <div class="empty-note">member_funnel_master 기준 날짜별 users / sign-up / first buyer / revenue 데이터가 없습니다.</div>
        </div>
        """

    def _render_trend_chart(panel: dict, title_suffix: str = "") -> str:
        panel_points = panel.get("points", []) or []
        if not panel_points:
            return '<div class="empty-note">해당 채널의 일자별 데이터가 없습니다.</div>'

        plot_w, plot_h = 760.0, 260.0
        left, right, top, bottom = 24.0, 20.0, 18.0, 28.0
        inner_w, inner_h = plot_w - left - right, plot_h - top - bottom
        max_v = max(float(panel.get("max_users", 0) or 0), 1.0)

        def y_of(v: float) -> float:
            return top + inner_h - (float(v) / max_v) * inner_h

        def x_of(i: int, n: int) -> float:
            return left + (inner_w * i / max(n - 1, 1))

        users_pts, signups_pts, buyers_pts = [], [], []
        label_nodes = []
        for i, row in enumerate(panel_points):
            x = x_of(i, len(panel_points))
            users_pts.append((x, y_of(float(row.get("users", 0) or 0))))
            signups_pts.append((x, y_of(float(row.get("signups", 0) or 0))))
            buyers_pts.append((x, y_of(float(row.get("buyers", 0) or 0))))
            if i in {0, len(panel_points)//2, len(panel_points)-1}:
                label_nodes.append(f'<text class="chart-axis-label" x="{round(x,1)}" y="{plot_h-6}" text-anchor="middle">{esc(str(row.get("dt",""))[5:])}</text>')

        grid_nodes = []
        for step in range(5):
            val = max_v * step / 4
            y = y_of(val)
            grid_nodes.append(f'<line class="chart-grid-line" x1="{left}" y1="{round(y,1)}" x2="{plot_w-right}" y2="{round(y,1)}"></line>')
            grid_nodes.append(f'<text class="chart-axis-label" x="0" y="{round(y+4,1)}">{fmt_int(val)}</text>')

        circles = []
        for cls, series in [("users", users_pts), ("signups", signups_pts), ("buyers", buyers_pts)]:
            sample_idx = {0, len(series)//2, len(series)-1}
            for i, (x,y) in enumerate(series):
                if i in sample_idx:
                    circles.append(f'<circle class="chart-point {cls}" cx="{round(x,1)}" cy="{round(y,1)}" r="4"></circle>')

        recent_rows = panel.get("recent_rows", []) or []
        rev_max = max(float(panel.get("max_revenue", 0) or 0), 1.0)
        bar_rows = []
        for row in list(reversed(recent_rows[:10])):
            rev = float(row.get("revenue", 0) or 0)
            width = max(2.5, (rev / rev_max) * 100) if rev_max > 0 else 0
            bar_rows.append(
                f'<div class="bar-row"><div class="bar-label">{esc(str(row.get("dt",""))[5:])}</div><div class="bar-track"><div class="bar-fill" style="width:{width:.1f}%"></div></div><div class="bar-value">{fmt_money(rev)}</div></div>'
            )

        peak_cards = [
            ("Peak Users", fmt_int(panel.get("peak_users", 0)), str(panel.get("peak_users_date", "-"))),
            ("Peak Sign-ups", fmt_int(panel.get("peak_signups", 0)), str(panel.get("peak_signups_date", "-"))),
            ("Peak Buyers", fmt_int(panel.get("peak_buyers", 0)), str(panel.get("peak_buyers_date", "-"))),
            ("Peak Revenue", fmt_money(panel.get("peak_revenue", 0)), str(panel.get("peak_revenue_date", "-"))),
        ]
        peak_html = "".join(
            f'<div class="stat-card"><div class="stat-label">{esc(a)}</div><div class="stat-value">{esc(b)}</div><div class="stat-meta">{esc(c)}</div></div>'
            for a,b,c in peak_cards
        )
        table_rows = "".join(
            f'<tr><td>{esc(r.get("dt",""))}</td><td class="num">{fmt_int(r.get("users",0))}</td><td class="num">{fmt_int(r.get("signups",0))}</td><td class="num">{fmt_int(r.get("buyers",0))}</td><td class="num">{fmt_money(r.get("revenue",0))}</td></tr>'
            for r in recent_rows
        )
        totals_html = ""
        if panel.get("label") != "All":
            totals_html = f'''
            <div class="stat-grid" style="margin-bottom:16px">
              <div class="stat-card"><div class="stat-label">Total Users</div><div class="stat-value">{fmt_int(panel.get("total_users",0))}</div><div class="stat-meta">{esc(panel.get("label","-"))} 누적</div></div>
              <div class="stat-card"><div class="stat-label">Total Sign-ups</div><div class="stat-value">{fmt_int(panel.get("total_signups",0))}</div><div class="stat-meta">가입 볼륨</div></div>
              <div class="stat-card"><div class="stat-label">Total Buyers</div><div class="stat-value">{fmt_int(panel.get("total_buyers",0))}</div><div class="stat-meta">구매 전환</div></div>
              <div class="stat-card"><div class="stat-label">Total Revenue</div><div class="stat-value">{fmt_money(panel.get("total_revenue",0))}</div><div class="stat-meta">누적 매출</div></div>
            </div>
            '''

        return f"""
        {totals_html}
        <div class="trend-grid">
          <div class="chart-card">
            <div class="chart-head">
              <div>
                <div class="section-kicker">Trend Chart</div>
                <div class="section-title" style="font-size:18px">{esc(panel.get("label","All"))}{title_suffix} Users / Sign-ups / Buyers</div>
              </div>
              <div class="chart-legend">
                <span class="legend-item"><span class="legend-dot" style="background:#2563eb"></span>Users</span>
                <span class="legend-item"><span class="legend-dot" style="background:#7c3aed"></span>Sign-ups</span>
                <span class="legend-item"><span class="legend-dot" style="background:#059669"></span>Buyers</span>
              </div>
            </div>
            <svg class="chart-svg" viewBox="0 0 760 320" preserveAspectRatio="none">
              {''.join(grid_nodes)}
              <polyline class="chart-line-users" points="{_svg_polyline(users_pts)}"></polyline>
              <polyline class="chart-line-signups" points="{_svg_polyline(signups_pts)}"></polyline>
              <polyline class="chart-line-buyers" points="{_svg_polyline(buyers_pts)}"></polyline>
              {''.join(circles)}
              {''.join(label_nodes)}
            </svg>
            <div class="trend-note">Revenue는 회원 마트 기준 first_purchase_date에 매핑해 표현했습니다. 주문 레벨 팩트 테이블이 연결되면 구매/매출 추이를 더 정확하게 고도화할 수 있습니다.</div>
          </div>
          <div class="chart-card">
            <div class="chart-head">
              <div>
                <div class="section-kicker">Revenue Pulse</div>
                <div class="section-title" style="font-size:18px">{esc(panel.get("label","All"))}{title_suffix} 최근 10개 일자 Revenue</div>
              </div>
            </div>
            <div class="bar-chart">{''.join(bar_rows) or '<div class="muted">최근 revenue 데이터가 없습니다.</div>'}</div>
            <div class="stat-grid" style="margin-top:16px">{peak_html}</div>
          </div>
        </div>
        <div class="subcard" style="margin-top:16px">
          <div class="section-head">
            <div>
              <div class="section-kicker">Recent Daily Detail</div>
              <div class="section-title" style="font-size:18px">{esc(panel.get("label","All"))}{title_suffix} 최근 14일 상세</div>
            </div>
          </div>
          <div class="table-wrap">
            <table class="data-table compact-table">
              <thead>
                <tr><th>Date</th><th class="num">Users</th><th class="num">Sign-ups</th><th class="num">Buyers</th><th class="num">Revenue</th></tr>
              </thead>
              <tbody>{table_rows}</tbody>
            </table>
          </div>
        </div>
        """

    all_html = _render_trend_chart(trend, "")
    tabs = channel_trend.get("tabs", []) or []
    panels = channel_trend.get("panels", {}) or {}
    channel_section = ""
    if tabs:
        tabs_html = "".join(
            f'<button type="button" class="bucket-pill {"active" if i == 0 else ""}" data-target="channel-trend" data-bucket="{esc(tab.get("key",""))}">{esc(tab.get("label",""))}</button>'
            for i, tab in enumerate(tabs)
        )
        panels_html = "".join(
            f'<div class="channel-trend-panel bucket-detail {"active" if i == 0 else ""}" data-target="channel-trend" data-bucket="{esc(tab.get("key",""))}">{_render_trend_chart(panels.get(tab.get("key",""), {}), " 채널")}</div>'
            for i, tab in enumerate(tabs)
        )
        channel_section = f"""
        <div class="subcard" style="margin-top:18px">
          <div class="section-head">
            <div>
              <div class="section-kicker">Channel Daily Trend</div>
              <div class="section-title" style="font-size:20px">채널별 일자 추이 탭</div>
              <div class="section-sub">전체와 함께 Owned / Paid / Awareness / Direct / Organic 등 채널별 날짜 흐름을 같은 구조로 비교합니다.</div>
            </div>
          </div>
          <div class="channel-trend-tabs">{tabs_html}</div>
          {panels_html}
        </div>
        """

    return f"""
    <div class="report-card">
      <div class="section-head">
        <div>
          <div class="section-kicker">Daily Trend</div>
          <div class="section-title">일자별 유입 · 가입 · 구매 추이</div>
          <div class="section-sub">기간 내 날짜별 흐름을 같이 보고, 피크 발생일과 최근 14일 상세를 바로 확인합니다.</div>
        </div>
      </div>
      {all_html}
      {channel_section}
    </div>
    """



def render_data_quality(data: dict) -> str:
    q = data.get("quality", {})
    stats = [
        ("Unknown Users", fmt_int(q.get("unknown_users", 0)), f"전체 users 대비 {fmt_pct(q.get('unknown_user_share', 0))}"),
        ("Direct Users", fmt_int(q.get("direct_users", 0)), f"전체 users 대비 {fmt_pct(q.get('direct_user_share', 0))}"),
        ("Missing Source", fmt_pct(q.get("source_missing_share", 0)), "식별 회원 기준 first_source 비어 있음"),
        ("Missing Campaign", fmt_pct(q.get("campaign_missing_share", 0)), "식별 회원 기준 first_campaign 비어 있음"),
    ]
    stat_html = "".join(
        f'<div class="stat-card"><div class="stat-label">{esc(a)}</div><div class="stat-value">{esc(b)}</div><div class="stat-meta">{esc(c)}</div></div>'
        for a,b,c in stats
    )
    return f"""
    <div class="report-card">
      <div class="section-head">
        <div>
          <div class="section-kicker">Data Quality Monitor</div>
          <div class="section-title">채널 해석이 불안정해질 수 있는 구간</div>
          <div class="section-sub">Unknown / Direct 비중과 source / campaign 누락 비중을 같이 봅니다.</div>
        </div>
      </div>
      <div class="stat-grid">{stat_html}</div>
    </div>
    """


def render_filter_bar(data: dict, period_nav_html: str = "") -> str:
    dr = data.get("date_range", {})
    period_text = period_label_from_data(data)
    chips = [
        ("Period", period_text or f"{dr.get('start_date','')} ~ {dr.get('end_date','')}"),
        ("Scope", "유입 · 가입 · 구매 · 상품 · 타겟"),
        ("Channel", "Awareness / Paid / Organic / SNS / Owned / Direct / Unknown"),
        ("Read Order", "요약 → 고객 프로필 → 채널 → 액션"),
    ]
    return f"""
    <div class="filter-bar">
      <div class="section-head" style="margin-bottom:0">
        <div>
          <div class="section-kicker">Operating View</div>
          <div class="section-title">실행 중심 레이아웃으로 전면 개편</div>
          <div class="section-sub">External Signal 스타일의 상단 구조를 가져오고, 회원/구매/채널/타겟을 한 흐름으로 재배치했습니다.</div>
        </div>
        <div style="display:flex; flex-direction:column; align-items:flex-end; gap:10px">
          <div class="chip-row">{period_nav_html}</div>
          <div class="chip-row">{''.join(f'<span class="chip active">{esc(a)} · {esc(b)}</span>' for a,b in chips)}</div>
        </div>
      </div>
    </div>
    """

def render_html(data: dict, current_period_key: str = "1m") -> str:
    generated_at = data.get("generated_at", "")
    dr = data.get("date_range", {})
    overview = data.get("overview", {})
    quality = data.get("quality", {})
    title = "Member Funnel"
    subtitle = f"{period_label_from_data(data)} · {dr.get('start_date','')} ~ {dr.get('end_date','')} · Updated {generated_at}"
    period_nav_html = build_period_nav(current_period_key)
    hero_insights = [
        ("Tracked Users", fmt_int(overview.get("users", 0)), f"가입률 {fmt_pct(data.get('overview_cards',{}).get('signup_rate', 0))}"),
        ("Buyers", fmt_int(overview.get("buyers", 0)), f"Buyer CVR {fmt_pct(data.get('overview_cards',{}).get('buyer_cvr', 0))}"),
        ("Revenue", fmt_money(overview.get("revenue", 0)), f"AOV {fmt_money(overview.get('aov', 0))}"),
        ("Unknown Share", fmt_pct(quality.get("unknown_user_share", 0)), f"Unknown users {fmt_int(quality.get('unknown_users', 0))}"),
    ]
    hero_cards = ''.join(
        f'<div class="highlight-card"><div class="highlight-label">{esc(a)}</div><div class="highlight-value">{esc(b)}</div><div class="highlight-meta">{esc(c)}</div></div>'
        for a,b,c in hero_insights
    )
    action_notes = data.get("action_cards", [])[:3]
    insight_html = ''.join(
        f'<div class="insight-item"><div class="label">{esc(card.get("title","-"))}</div><div class="value">{esc(card.get("text","-"))}</div></div>'
        for card in action_notes
    ) or '<div class="insight-item"><div class="label">Insight</div><div class="value">채널/세그먼트 데이터가 쌓이면 자동 요약이 여기에 노출됩니다.</div></div>'
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
      <div class="hero-main">
        <div class="hero-kicker">EXTERNAL SIGNAL STYLE · CRM FUNNEL</div>
        <div class="hero-title">{esc(title)}</div>
        <div class="hero-sub">External Signal 리포트 톤을 그대로 가져와서, 채널·고객·상품·타겟 액션이 한 화면에서 또렷하게 읽히도록 재정렬했습니다.</div>
        <div class="hero-meta">
          <span class="hero-chip">{esc(subtitle)}</span>
          <span class="hero-chip">{esc(period_label_from_data(data))}</span>
          <span class="hero-chip">Sign-up → Buyer → Product → Target</span>
          <span class="hero-chip">Owned / Paid / Awareness / Direct</span>
        </div>
        <div class="hero-actions">
          <a class="hero-btn" href="../index.html">Hub</a>
          <a class="hero-btn" href="./data/target_candidates.json">Target JSON</a>
          <a class="hero-btn primary" href="./data/overview.json">Data Bundle</a>
        </div>
      </div>
      <div class="hero-side">
        <div class="section-head" style="margin-bottom:0">
          <div>
            <div class="section-kicker">Signal Summary</div>
            <div class="section-title">이번 구간 핵심 요약</div>
            <div class="section-sub">채널/고객/액션 관점에서 바로 읽히는 카드</div>
          </div>
        </div>
        <div class="hero-summary-grid">{hero_cards}</div>
        <div class="insight-list">{insight_html}</div>
      </div>
    </div>

    {render_filter_bar(data, period_nav_html)}

    <div class="report-card">
      <div class="section-head">
        <div>
          <div class="section-kicker">Overview</div>
          <div class="section-title">핵심 KPI</div>
          <div class="section-sub">운영 회의 첫 화면에서 바로 보는 숫자</div>
        </div>
      </div>
      <div class="kpi-grid">{render_kpi_cards(data)}</div>
    </div>

    <div class="report-card">
      <div class="section-head">
        <div>
          <div class="section-kicker">Customer Snapshot</div>
          <div class="section-title">고객 프로필 카드</div>
          <div class="section-sub">누가 들어왔고, 누가 샀고, 누가 다시 살 가능성이 높은지</div>
        </div>
      </div>
      <div class="persona-grid">{render_persona_cards(data)}</div>
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

    {render_daily_trend(data)}
    {render_channel_snapshot(data)}
    {render_entry_points(data)}
    {render_non_buyer(data)}
    {render_buyer_revenue(data)}
    {render_product_insight(data)}
    {render_target_candidates(data)}
    {render_data_quality(data)}
  </div>
  {REPORT_PATCH_JS}
</body>
</html>
"""


# =========================================================
# Main
# =========================================================

def persist_bundle_files(data: dict, prefix: str = "") -> None:
    ensure_dir(OUT_DIR)
    ensure_dir(DATA_DIR)
    ensure_dir(ASSET_DIR)

    stem = f"{prefix}_" if prefix else ""
    write_json(DATA_DIR / f"{stem}overview.json", data)
    write_json(DATA_DIR / f"{stem}channel_snapshot.json", {"rows": data.get("channel_snapshot", [])})
    write_json(DATA_DIR / f"{stem}non_buyer.json", {"rows": data.get("non_buyer", [])})
    write_json(DATA_DIR / f"{stem}buyer_revenue.json", {"rows": data.get("buyer_revenue", [])})
    write_json(DATA_DIR / f"{stem}product_insight.json", {"rows": data.get("product_insight", []), "channel_rows": data.get("channel_product", [])})
    write_json(DATA_DIR / f"{stem}target_candidates.json", {"rows": data.get("target_candidates", [])})
    write_json(DATA_DIR / f"{stem}daily_trend.json", {"rows": data.get("daily_trend", {}).get("points", [])})
    write_json(DATA_DIR / f"{stem}channel_daily_trend.json", data.get("channel_daily_trend", {}))

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


def _period_date_range(days: int) -> tuple[dt.date, dt.date]:
    end_date = dt.datetime.now(KST).date() - dt.timedelta(days=1)
    start_date = end_date - dt.timedelta(days=max(1, int(days)) - 1)
    return start_date, end_date


def _sample_json_for_period(base_path: str, key: str) -> Optional[Path]:
    if not base_path:
        return None
    p = Path(base_path)
    if p.is_file():
        if "{period}" in base_path:
            cand = Path(base_path.format(period=key))
            return cand if cand.exists() else None
        return p
    return None


def main() -> None:
    ensure_dir(OUT_DIR)
    ensure_dir(DATA_DIR)

    default_summary_data = None

    for preset in PERIOD_PRESETS:
        start_date, end_date = _period_date_range(preset["days"])
        sample_path = _sample_json_for_period(SAMPLE_JSON, preset["key"])

        if sample_path:
            bundle = read_json(sample_path)
            if not bundle:
                raise RuntimeError(f"Could not read MEMBER_FUNNEL_SAMPLE_JSON for period {preset['key']}: {sample_path}")
        else:
            bundle = fetch_bundle_from_bq(start_date, end_date)

        data = normalize_bundle(bundle)
        data["period_meta"] = {
            "key": preset["key"],
            "label": preset["label"],
            "description": preset["description"],
            "days": preset["days"],
        }

        if WRITE_DATA_CACHE:
            persist_bundle_files(data, prefix=preset["key"])

        html_str = render_html(data, current_period_key=preset["key"])
        out_path = OUT_DIR / preset["filename"]
        out_path.write_text(html_str, encoding="utf-8")
        print(f"[OK] Wrote: {out_path}")

        if preset.get("is_default"):
            default_summary_data = data

    if default_summary_data and WRITE_DATA_CACHE:
        write_json(DATA_DIR / "overview.json", default_summary_data)
        write_json(DATA_DIR / "channel_snapshot.json", {"rows": default_summary_data.get("channel_snapshot", [])})
        write_json(DATA_DIR / "non_buyer.json", {"rows": default_summary_data.get("non_buyer", [])})
        write_json(DATA_DIR / "buyer_revenue.json", {"rows": default_summary_data.get("buyer_revenue", [])})
        write_json(DATA_DIR / "product_insight.json", {"rows": default_summary_data.get("product_insight", []), "channel_rows": default_summary_data.get("channel_product", [])})
        write_json(DATA_DIR / "target_candidates.json", {"rows": default_summary_data.get("target_candidates", [])})
        write_json(DATA_DIR / "daily_trend.json", {"rows": default_summary_data.get("daily_trend", {}).get("points", [])})
        write_json(DATA_DIR / "channel_daily_trend.json", default_summary_data.get("channel_daily_trend", {}))


if __name__ == "__main__":
    main()
