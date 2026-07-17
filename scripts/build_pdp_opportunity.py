#!/usr/bin/env python3
from __future__ import annotations

import base64
import datetime as dt
import html
import json
import math
import os
from pathlib import Path
from typing import Any

from google.cloud import bigquery

KST = dt.timezone(dt.timedelta(hours=9))
ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "reports" / "pdp_opportunity"
DATA = OUT / "data.json"


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def setup_credentials() -> None:
    existing = env("GOOGLE_APPLICATION_CREDENTIALS")
    if existing and Path(existing).exists():
        return
    encoded = env("GOOGLE_SA_JSON_B64")
    if not encoded:
        raise SystemExit("GOOGLE_SA_JSON_B64 is not configured")
    path = ROOT / "gcp_service_account.json"
    path.write_bytes(base64.b64decode(encoded))
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(path)


def safe(v: Any) -> float:
    try:
        x = float(v or 0)
        return x if math.isfinite(x) else 0.0
    except Exception:
        return 0.0


def query(client: bigquery.Client, start: dt.date, end: dt.date) -> list[dict[str, Any]]:
    table = env("GA4_EVENTS_TABLE", "columbia-ga4.analytics_358593394.events_*")
    location = env("BQ_LOCATION", "asia-northeast3")
    sql = f"""
    DECLARE start_date DATE DEFAULT @start_date;
    DECLARE end_date DATE DEFAULT @end_date;

    WITH base AS (
      SELECT
        DATE(TIMESTAMP_MICROS(event_timestamp), 'Asia/Seoul') AS event_date,
        TIMESTAMP_MICROS(event_timestamp) AS event_ts,
        event_name,
        user_pseudo_id,
        CONCAT(
          user_pseudo_id, '-',
          CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_id') AS STRING)
        ) AS session_key,
        device.category AS device_category,
        traffic_source.source AS first_source,
        traffic_source.medium AS first_medium,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key='page_location') AS page_location,
        item.item_id,
        item.item_name,
        item.item_category,
        item.price,
        item.quantity,
        ecommerce.transaction_id,
        ecommerce.purchase_revenue
      FROM `{table}`,
      UNNEST(IF(ARRAY_LENGTH(items)=0,
        [STRUCT(CAST(NULL AS STRING) AS item_id, CAST(NULL AS STRING) AS item_name,
                CAST(NULL AS STRING) AS item_brand, CAST(NULL AS STRING) AS item_variant,
                CAST(NULL AS STRING) AS item_category, CAST(NULL AS STRING) AS item_category2,
                CAST(NULL AS STRING) AS item_category3, CAST(NULL AS STRING) AS item_category4,
                CAST(NULL AS STRING) AS item_category5, CAST(NULL AS NUMERIC) AS price,
                CAST(NULL AS INT64) AS quantity, CAST(NULL AS NUMERIC) AS item_revenue,
                CAST(NULL AS STRING) AS item_list_name, CAST(NULL AS STRING) AS item_list_index,
                CAST(NULL AS STRING) AS promotion_id, CAST(NULL AS STRING) AS promotion_name,
                CAST(NULL AS STRING) AS creative_name, CAST(NULL AS STRING) AS creative_slot,
                CAST([] AS ARRAY<STRUCT<key STRING, value STRUCT<string_value STRING, int_value INT64, float_value FLOAT64, double_value FLOAT64>>>) AS item_params)],
        items)) AS item
      WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', start_date) AND FORMAT_DATE('%Y%m%d', end_date)
        AND event_name IN ('view_item','add_to_cart','begin_checkout','purchase')
    ),
    normalized AS (
      SELECT
        *,
        UPPER(TRIM(COALESCE(NULLIF(item_id,''), REGEXP_EXTRACT(page_location, r'(?i)(?:product|goods|item)[^A-Za-z0-9]*([A-Za-z0-9_-]{{5,}})')))) AS product_code
      FROM base
    ),
    session_product AS (
      SELECT
        session_key,
        product_code,
        ANY_VALUE(NULLIF(item_name,'')) AS product_name,
        ANY_VALUE(NULLIF(item_category,'')) AS category,
        ANY_VALUE(device_category) AS device,
        ANY_VALUE(CONCAT(COALESCE(first_source,'(direct)'), ' / ', COALESCE(first_medium,'(none)'))) AS source_medium,
        MAX(IF(event_name='view_item',1,0)) AS viewed,
        MAX(IF(event_name='add_to_cart',1,0)) AS added,
        MAX(IF(event_name='begin_checkout',1,0)) AS checkout,
        MAX(IF(event_name='purchase',1,0)) AS purchased,
        SUM(IF(event_name='purchase', COALESCE(price,0)*COALESCE(quantity,1), 0)) AS item_revenue,
        MAX(IF(event_name='purchase', COALESCE(purchase_revenue,0), 0)) AS transaction_revenue
      FROM normalized
      WHERE product_code IS NOT NULL
      GROUP BY 1,2
    ),
    product AS (
      SELECT
        product_code,
        ANY_VALUE(product_name HAVING MAX LENGTH(COALESCE(product_name,''))) AS product_name,
        ANY_VALUE(category HAVING MAX LENGTH(COALESCE(category,''))) AS category,
        COUNTIF(viewed=1) AS pdp_sessions,
        COUNTIF(added=1) AS add_to_cart_sessions,
        COUNTIF(checkout=1) AS checkout_sessions,
        COUNTIF(purchased=1) AS purchase_sessions,
        SUM(GREATEST(item_revenue, transaction_revenue)) AS revenue,
        APPROX_TOP_COUNT(device, 1)[SAFE_OFFSET(0)].value AS top_device,
        APPROX_TOP_COUNT(source_medium, 1)[SAFE_OFFSET(0)].value AS top_source_medium
      FROM session_product
      GROUP BY 1
    )
    SELECT * FROM product
    WHERE pdp_sessions >= @min_sessions
    ORDER BY pdp_sessions DESC
    """
    params = [
        bigquery.ScalarQueryParameter("start_date", "DATE", start),
        bigquery.ScalarQueryParameter("end_date", "DATE", end),
        bigquery.ScalarQueryParameter("min_sessions", "INT64", int(env("PDP_MIN_SESSIONS", "10"))),
    ]
    job = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params), location=location)
    return [dict(row.items()) for row in job.result()]


def score_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    max_sessions = max(safe(r.get("pdp_sessions")) for r in rows) or 1
    revenues_per_purchase = [safe(r.get("revenue")) / max(safe(r.get("purchase_sessions")), 1) for r in rows if safe(r.get("revenue")) > 0]
    fallback_aov = sorted(revenues_per_purchase)[len(revenues_per_purchase)//2] if revenues_per_purchase else 100000
    output = []
    for r in rows:
        views = safe(r.get("pdp_sessions")); atc = safe(r.get("add_to_cart_sessions")); checkout = safe(r.get("checkout_sessions")); purchases = safe(r.get("purchase_sessions")); revenue = safe(r.get("revenue"))
        atc_rate = atc / views * 100 if views else 0
        checkout_rate = checkout / views * 100 if views else 0
        cvr = purchases / views * 100 if views else 0
        pdp_abandonment = max(0, 100 - atc_rate)
        checkout_completion = purchases / checkout * 100 if checkout else 0
        traffic = math.log1p(views) / math.log1p(max_sessions)
        friction = min(max((pdp_abandonment - 55) / 45, 0), 1)
        conversion_gap = min(max((2.0 - cvr) / 2.0, 0), 1)
        checkout_gap = min(max((45 - checkout_completion) / 45, 0), 1) if checkout else 1
        opportunity_score = round(100 * (.38*traffic + .27*friction + .23*conversion_gap + .12*checkout_gap), 1)
        target_atc = max(atc_rate, 8.0)
        incremental_atc = max(views * (target_atc-atc_rate)/100, 0)
        downstream = purchases / atc if atc else .12
        expected_orders = incremental_atc * max(downstream, .08)
        aov = revenue / purchases if purchases else fallback_aov
        expected_revenue = round(expected_orders * aov)
        reasons = []
        if views >= 100: reasons.append("트래픽 큼")
        if atc_rate < 5: reasons.append("장바구니 전환 낮음")
        if cvr < 1: reasons.append("구매전환 낮음")
        if checkout and checkout_completion < 35: reasons.append("체크아웃 이탈")
        output.append({
            "product_code": r.get("product_code") or "",
            "product_name": r.get("product_name") or r.get("product_code") or "상품명 미수집",
            "category": r.get("category") or "",
            "pdp_sessions": round(views),
            "add_to_cart_sessions": round(atc),
            "checkout_sessions": round(checkout),
            "purchase_sessions": round(purchases),
            "atc_rate": round(atc_rate,2),
            "checkout_rate": round(checkout_rate,2),
            "cvr": round(cvr,2),
            "pdp_abandonment_rate": round(pdp_abandonment,2),
            "checkout_completion_rate": round(checkout_completion,2),
            "revenue": round(revenue),
            "opportunity_score": opportunity_score,
            "expected_orders": round(expected_orders,1),
            "expected_revenue": expected_revenue,
            "top_device": r.get("top_device") or "",
            "top_source_medium": r.get("top_source_medium") or "",
            "reason": " · ".join(reasons) or "상세 페이지 점검",
        })
    return sorted(output, key=lambda x: (x["opportunity_score"], x["expected_revenue"]), reverse=True)


def render(payload: dict[str, Any]) -> str:
    data = json.dumps(payload, ensure_ascii=False).replace("</", "<\\/")
    return f'''<!doctype html><html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><meta name="robots" content="noindex,nofollow"><title>PDP Opportunity</title><style>
:root{{--bg:#f5f7fb;--card:#fff;--text:#172033;--muted:#6b778c;--line:#e7ebf1;--blue:#0874e8}}*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Noto Sans KR',sans-serif}}main{{max-width:1360px;margin:auto;padding:22px}}h1{{margin:0;font-size:30px}}.sub{{font-size:12px;color:var(--muted)}}.kpis{{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin:16px 0}}.card{{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:16px}}.value{{font-size:25px;font-weight:800}}.row{{display:grid;grid-template-columns:52px minmax(220px,1fr) repeat(5,minmax(90px,.55fr));gap:10px;align-items:center;padding:12px 0;border-bottom:1px solid var(--line)}}.score{{display:grid;place-items:center;width:42px;height:42px;border-radius:12px;background:#eaf4ff;color:var(--blue);font-weight:800}}@media(max-width:700px){{main{{padding:10px}}h1{{font-size:21px}}.kpis{{grid-template-columns:repeat(2,1fr);gap:6px}}.card{{padding:10px;border-radius:12px}}.value{{font-size:18px}}.row{{grid-template-columns:38px 1fr;padding:9px 0;gap:7px}}.score{{width:34px;height:34px;font-size:12px}}.row>*:nth-child(n+3){{display:none}}}}
</style></head><body><main><h1>PDP Opportunity Center</h1><div class="sub">GA4 상품 상세 퍼널 · view_item → add_to_cart → begin_checkout → purchase</div><section class="kpis" id="kpis"></section><section class="card"><div id="rows"></div></section></main><script>const D={data};const f=n=>new Intl.NumberFormat('ko-KR',{{notation:'compact',maximumFractionDigits:1}}).format(n||0);const p=n=>(n||0).toFixed(1)+'%';const R=D.products||[];const total=k=>R.reduce((a,x)=>a+(+x[k]||0),0);document.querySelector('#kpis').innerHTML=[['PDP 세션',f(total('pdp_sessions'))],['구매 세션',f(total('purchase_sessions'))],['통합 CVR',p(total('purchase_sessions')/Math.max(total('pdp_sessions'),1)*100)],['개선 예상 매출',f(R.slice(0,20).reduce((a,x)=>a+(x.expected_revenue||0),0))+'원']].map(x=>'<div class="card"><div class="sub">'+x[0]+'</div><div class="value">'+x[1]+'</div></div>').join('');document.querySelector('#rows').innerHTML=R.slice(0,100).map((x,i)=>'<div class="row"><span class="score">'+Math.round(x.opportunity_score)+'</span><span><b>'+(i+1)+'. '+x.product_name+'</b><div class="sub">'+x.product_code+' · '+x.reason+'</div></span><span>'+f(x.pdp_sessions)+' 세션</span><span>ATC '+p(x.atc_rate)+'</span><span>CVR '+p(x.cvr)+'</span><span>'+f(x.expected_revenue)+'원</span><span>'+x.top_device+'</span></div>').join('')||'<p>조회된 상품 데이터가 없습니다.</p>';</script></body></html>'''


def main() -> int:
    setup_credentials()
    days = int(env("PDP_LOOKBACK_DAYS", "30"))
    end = dt.datetime.now(KST).date() - dt.timedelta(days=1)
    start = end - dt.timedelta(days=days-1)
    client = bigquery.Client(project=env("GCP_PROJECT_ID") or None)
    raw = query(client, start, end)
    products = score_rows(raw)
    payload = {
        "generated_at": dt.datetime.now(KST).isoformat(),
        "data_start": start.isoformat(),
        "data_end": end.isoformat(),
        "definition": {
            "pdp_sessions": "view_item이 발생한 상품별 GA4 세션",
            "pdp_abandonment_rate": "1 - add_to_cart 세션 / view_item 세션",
            "cvr": "purchase 세션 / view_item 세션",
            "expected_revenue": "ATC 8% 목표와 현재 downstream 구매율을 적용한 보수적 개선 추정",
        },
        "products": products,
        "rows": products,
    }
    OUT.mkdir(parents=True, exist_ok=True)
    DATA.write_text(json.dumps(payload, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    (OUT / "index.html").write_text(render(payload), encoding="utf-8")
    (OUT / "meta.json").write_text(json.dumps({"generated_at":payload["generated_at"],"data_start":payload["data_start"],"data_end":payload["data_end"],"status":"fresh" if products else "empty","row_count":len(products)},ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    print(f"[OK] PDP opportunity products={len(products)} period={start}..{end}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
