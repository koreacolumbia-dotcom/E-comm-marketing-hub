#!/usr/bin/env python3
from __future__ import annotations

import base64
import datetime as dt
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
    current = env("GOOGLE_APPLICATION_CREDENTIALS")
    if current and Path(current).exists():
        return
    encoded = env("GOOGLE_SA_JSON_B64")
    if not encoded:
        raise SystemExit("[ERROR] GOOGLE_SA_JSON_B64 is missing")
    target = Path("/tmp/pdp_ga4_service_account.json")
    target.write_bytes(base64.b64decode(encoded))
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(target)


def safe(v: Any) -> float:
    try:
        value = float(v or 0)
        return value if math.isfinite(value) else 0.0
    except Exception:
        return 0.0


def div(a: float, b: float) -> float:
    return a / b if b else 0.0


def percentile(values: list[float], q: float) -> float:
    values = sorted(v for v in values if math.isfinite(v))
    if not values:
        return 0.0
    pos = (len(values) - 1) * q
    lo, hi = math.floor(pos), math.ceil(pos)
    if lo == hi:
        return values[lo]
    return values[lo] + (values[hi] - values[lo]) * (pos - lo)


def query(client: bigquery.Client, start: dt.date, end: dt.date, days: int) -> list[dict[str, Any]]:
    table = env("GA4_EVENTS_TABLE", "columbia-ga4.analytics_358593394.events_*")
    sql = f"""
    DECLARE current_start DATE DEFAULT @start_date;
    DECLARE current_end DATE DEFAULT @end_date;
    DECLARE previous_start DATE DEFAULT DATE_SUB(current_start, INTERVAL @days DAY);
    DECLARE previous_end DATE DEFAULT DATE_SUB(current_end, INTERVAL @days DAY);

    WITH item_events AS (
      SELECT
        CASE
          WHEN PARSE_DATE('%Y%m%d', event_date) BETWEEN current_start AND current_end THEN 'current'
          ELSE 'previous'
        END AS period,
        event_name,
        event_timestamp,
        user_pseudo_id,
        CONCAT(
          user_pseudo_id, '-',
          COALESCE(CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_id') AS STRING), '0')
        ) AS session_id,
        UPPER(TRIM(COALESCE(NULLIF(i.item_id,''), NULLIF(i.item_name,'')))) AS product_code,
        COALESCE(NULLIF(i.item_name,''), NULLIF(i.item_id,''), 'Unknown product') AS product_name,
        COALESCE(NULLIF(i.item_category,''), '미분류') AS category,
        COALESCE(i.price, 0) AS price,
        COALESCE(i.quantity, 1) AS quantity,
        COALESCE(i.item_revenue, i.price * COALESCE(i.quantity, 1), 0) AS item_revenue,
        COALESCE(ecommerce.transaction_id, (SELECT value.string_value FROM UNNEST(event_params) WHERE key='transaction_id')) AS transaction_id,
        device.category AS device_category,
        COALESCE(collected_traffic_source.manual_source, traffic_source.source, '(direct)') AS source,
        COALESCE(collected_traffic_source.manual_medium, traffic_source.medium, '(none)') AS medium
      FROM `{table}` e
      CROSS JOIN UNNEST(e.items) i
      WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', previous_start) AND FORMAT_DATE('%Y%m%d', current_end)
        AND event_name IN ('view_item','add_to_cart','begin_checkout','purchase')
    ),
    product_rollup AS (
      SELECT
        period,
        product_code,
        ANY_VALUE(product_name HAVING MAX event_timestamp) AS product_name,
        ANY_VALUE(category HAVING MAX event_timestamp) AS category,
        APPROX_TOP_COUNT(device_category, 1)[SAFE_OFFSET(0)].value AS top_device,
        APPROX_TOP_COUNT(CONCAT(source, ' / ', medium), 1)[SAFE_OFFSET(0)].value AS top_source_medium,
        AVG(NULLIF(price, 0)) AS avg_price,
        COUNT(DISTINCT IF(event_name='view_item', session_id, NULL)) AS pdp_sessions,
        COUNT(DISTINCT IF(event_name='view_item', user_pseudo_id, NULL)) AS pdp_users,
        COUNT(DISTINCT IF(event_name='add_to_cart', session_id, NULL)) AS add_to_cart_sessions,
        COUNT(DISTINCT IF(event_name='begin_checkout', session_id, NULL)) AS checkout_sessions,
        COUNT(DISTINCT IF(event_name='purchase', session_id, NULL)) AS purchase_sessions,
        COUNT(DISTINCT IF(event_name='purchase', transaction_id, NULL)) AS orders,
        SUM(IF(event_name='purchase', quantity, 0)) AS units,
        SUM(IF(event_name='purchase', item_revenue, 0)) AS revenue
      FROM item_events
      WHERE product_code IS NOT NULL
      GROUP BY period, product_code
    )
    SELECT * FROM product_rollup
    WHERE pdp_sessions >= @min_sessions OR period='previous'
    ORDER BY period, pdp_sessions DESC
    """
    config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("start_date", "DATE", start),
        bigquery.ScalarQueryParameter("end_date", "DATE", end),
        bigquery.ScalarQueryParameter("days", "INT64", days),
        bigquery.ScalarQueryParameter("min_sessions", "INT64", int(env("PDP_MIN_SESSIONS", "10"))),
    ])
    return [dict(row.items()) for row in client.query(sql, job_config=config).result()]


def score_rows(raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, dict[str, Any]]] = {}
    for row in raw:
        code = str(row.get("product_code") or "").strip()
        if code:
            grouped.setdefault(code, {})[str(row.get("period"))] = row

    rates = []
    for periods in grouped.values():
        cur = periods.get("current", {})
        views = safe(cur.get("pdp_sessions"))
        if views >= 20:
            rates.append(div(safe(cur.get("purchase_sessions")), views))
    benchmark = max(percentile(rates, 0.75), 0.015)

    output = []
    for code, periods in grouped.items():
        cur, prev = periods.get("current", {}), periods.get("previous", {})
        views = safe(cur.get("pdp_sessions"))
        if views <= 0:
            continue
        atc = safe(cur.get("add_to_cart_sessions")); checkout = safe(cur.get("checkout_sessions")); purchases = safe(cur.get("purchase_sessions"))
        orders = safe(cur.get("orders")); revenue = safe(cur.get("revenue")); price = safe(cur.get("avg_price"))
        atc_rate = div(atc, views); checkout_rate = div(checkout, views); purchase_rate = div(purchases, views)
        prev_views = safe(prev.get("pdp_sessions")); prev_purchase_rate = div(safe(prev.get("purchase_sessions")), prev_views)
        dropoff = max(0.0, 1 - atc_rate); gap = max(0.0, benchmark - purchase_rate)
        aov = div(revenue, orders) or price or 100000
        expected_orders = views * gap * 0.30
        expected_revenue = expected_orders * aov
        traffic_score = min(math.log1p(views) / 9, 1)
        conversion_score = min(div(gap, benchmark), 1) if benchmark else 0
        value_score = min(math.log1p(max(expected_revenue, revenue)) / 18, 1)
        score = round(100 * (.35 * traffic_score + .25 * dropoff + .25 * conversion_score + .15 * value_score), 1)
        reasons = []
        if views >= 100: reasons.append("트래픽 높음")
        if atc_rate < .08: reasons.append("장바구니 전환 낮음")
        if checkout_rate < .04: reasons.append("체크아웃 진입 낮음")
        if purchase_rate < benchmark: reasons.append("구매전환 개선 여지")
        output.append({
            "product_code": code,
            "product_name": cur.get("product_name") or code,
            "category": cur.get("category") or "미분류",
            "top_device": cur.get("top_device") or "-",
            "top_source_medium": cur.get("top_source_medium") or "-",
            "pdp_sessions": round(views),
            "pdp_users": round(safe(cur.get("pdp_users"))),
            "add_to_cart_sessions": round(atc),
            "checkout_sessions": round(checkout),
            "purchase_sessions": round(purchases),
            "orders": round(orders),
            "units": round(safe(cur.get("units"))),
            "revenue": round(revenue),
            "avg_price": round(price),
            "atc_rate": round(atc_rate * 100, 2),
            "checkout_rate": round(checkout_rate * 100, 2),
            "cvr": round(purchase_rate * 100, 2),
            "purchase_rate": round(purchase_rate * 100, 2),
            "pdp_abandonment_rate": round(dropoff * 100, 2),
            "previous_pdp_sessions": round(prev_views),
            "previous_purchase_rate": round(prev_purchase_rate * 100, 2),
            "purchase_rate_change_pp": round((purchase_rate - prev_purchase_rate) * 100, 2),
            "opportunity_score": score,
            "expected_orders": round(expected_orders, 1),
            "expected_revenue": round(expected_revenue),
            "reason": " · ".join(reasons) or "상세페이지 기본 점검",
        })
    return sorted(output, key=lambda x: (x["opportunity_score"], x["expected_revenue"]), reverse=True)


def render(payload: dict[str, Any]) -> str:
    data = json.dumps(payload, ensure_ascii=False).replace("</", "<\\/")
    return f'''<!doctype html><html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><meta name="robots" content="noindex,nofollow"><title>PDP Opportunity</title><style>:root{{--bg:#f5f7fb;--card:#fff;--text:#172033;--muted:#6b778c;--line:#e7ebf1;--blue:#0874e8}}*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Noto Sans KR',sans-serif}}main{{max-width:1360px;margin:auto;padding:22px}}h1{{margin:0;font-size:28px}}.sub{{font-size:11px;color:var(--muted)}}.kpis{{display:grid;grid-template-columns:repeat(4,1fr);gap:9px;margin:14px 0}}.card{{background:var(--card);border:1px solid var(--line);border-radius:15px;padding:14px}}.value{{font-size:24px;font-weight:800}}.toolbar{{display:flex;gap:7px;margin:10px 0}}input,select{{width:100%;padding:10px;border:1px solid var(--line);border-radius:10px;background:#fff}}.row{{display:grid;grid-template-columns:46px minmax(200px,1fr) repeat(5,minmax(82px,.5fr));gap:9px;align-items:center;padding:10px 0;border-bottom:1px solid var(--line)}}.score{{display:grid;place-items:center;width:40px;height:40px;border-radius:11px;background:#eaf4ff;color:var(--blue);font-weight:800}}@media(max-width:700px){{main{{padding:9px}}h1{{font-size:20px}}.kpis{{grid-template-columns:repeat(2,1fr);gap:5px}}.card{{padding:9px;border-radius:11px}}.value{{font-size:17px}}.toolbar{{position:sticky;top:0;padding:5px 0;background:var(--bg);z-index:2}}.row{{grid-template-columns:35px minmax(0,1fr) 62px;padding:8px 0;gap:6px}}.score{{width:32px;height:32px;font-size:11px}}.row>*:nth-child(n+4){{display:none}}}}</style></head><body><main><h1>PDP Opportunity Center</h1><div class="sub">GA4 상품 퍼널 · view_item → add_to_cart → begin_checkout → purchase · <span id="period"></span></div><section class="kpis" id="kpis"></section><div class="toolbar"><input id="q" placeholder="상품명·상품코드 검색"><select id="cat"><option value="">전체 카테고리</option></select></div><section class="card" id="rows"></section></main><script>const D={data},R=D.products||[];const f=n=>new Intl.NumberFormat('ko-KR',{{notation:'compact',maximumFractionDigits:1}}).format(n||0),p=n=>(+n||0).toFixed(1)+'%';period.textContent=D.data_start+' ~ '+D.data_end;const total=k=>R.reduce((a,x)=>a+(+x[k]||0),0);kpis.innerHTML=[['PDP 세션',f(total('pdp_sessions'))],['PDP→ATC',p(total('add_to_cart_sessions')/Math.max(total('pdp_sessions'),1)*100)],['PDP→구매',p(total('purchase_sessions')/Math.max(total('pdp_sessions'),1)*100)],['개선 예상매출',f(R.slice(0,20).reduce((a,x)=>a+(x.expected_revenue||0),0))+'원']].map(x=>'<div class="card"><div class="sub">'+x[0]+'</div><div class="value">'+x[1]+'</div></div>').join('');[...new Set(R.map(x=>x.category))].sort().forEach(x=>cat.add(new Option(x,x)));function render(){{const t=q.value.toLowerCase(),c=cat.value;rows.innerHTML=R.filter(x=>(!t||(x.product_name+' '+x.product_code).toLowerCase().includes(t))&&(!c||x.category===c)).slice(0,100).map((x,i)=>'<div class="row"><span class="score">'+Math.round(x.opportunity_score)+'</span><span><b>'+(i+1)+'. '+x.product_name+'</b><div class="sub">'+x.product_code+' · '+x.reason+'</div></span><span><b>'+f(x.pdp_sessions)+'</b><div class="sub">세션</div></span><span>ATC '+p(x.atc_rate)+'</span><span>Checkout '+p(x.checkout_rate)+'</span><span>CVR '+p(x.cvr)+'</span><span>'+f(x.expected_revenue)+'원</span></div>').join('')||'<p>조회된 상품 데이터가 없습니다.</p>'}}q.oninput=render;cat.onchange=render;render();</script></body></html>'''


def main() -> int:
    setup_credentials()
    days = int(env("PDP_LOOKBACK_DAYS", "30"))
    end = dt.datetime.now(KST).date() - dt.timedelta(days=1)
    if env("PDP_END_DATE"):
        end = dt.date.fromisoformat(env("PDP_END_DATE"))
    start = end - dt.timedelta(days=days - 1)
    project = env("BQ_PROJECT", "columbia-ga4")
    client = bigquery.Client(project=project, location=env("BQ_LOCATION", "asia-northeast3"))
    products = score_rows(query(client, start, end, days))
    summary = {
        "pdp_sessions": sum(x["pdp_sessions"] for x in products),
        "purchase_sessions": sum(x["purchase_sessions"] for x in products),
        "revenue": sum(x["revenue"] for x in products),
        "expected_revenue": sum(x["expected_revenue"] for x in products[:20]),
        "priority_products": sum(1 for x in products if x["opportunity_score"] >= 70),
    }
    payload = {
        "generated_at": dt.datetime.now(KST).isoformat(),
        "data_start": start.isoformat(),
        "data_end": end.isoformat(),
        "source": env("GA4_EVENTS_TABLE", "columbia-ga4.analytics_358593394.events_*"),
        "definition": {
            "pdp_sessions": "view_item이 발생한 상품별 GA4 세션",
            "pdp_abandonment_rate": "1 - add_to_cart 세션 / view_item 세션이며 GA4 이탈률과는 다른 PDP 퍼널 지표",
            "cvr": "purchase 세션 / view_item 세션",
            "expected_revenue": "상위 25% 구매전환율과 현재값 차이의 30%만 회복하는 보수적 추정",
        },
        "summary": summary,
        "products": products,
        "rows": products,
    }
    OUT.mkdir(parents=True, exist_ok=True)
    DATA.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (OUT / "summary.json").write_text(json.dumps({**summary, "generated_at": payload["generated_at"], "data_end": payload["data_end"]}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (OUT / "index.html").write_text(render(payload), encoding="utf-8")
    (OUT / "meta.json").write_text(json.dumps({"generated_at": payload["generated_at"], "data_start": payload["data_start"], "data_end": payload["data_end"], "status": "fresh" if products else "empty", "row_count": len(products)}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if not products:
        raise SystemExit("[ERROR] PDP query returned no product rows")
    print(f"[OK] PDP opportunity products={len(products)} period={start}..{end}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
