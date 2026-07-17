#!/usr/bin/env python3
from __future__ import annotations

import base64
import datetime as dt
import json
import math
import os
import statistics
from pathlib import Path
from typing import Any

from google.cloud import bigquery

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "reports" / "realtime_alerts"
KST = dt.timezone(dt.timedelta(hours=9))


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def setup_credentials() -> None:
    if env("GOOGLE_APPLICATION_CREDENTIALS") and Path(env("GOOGLE_APPLICATION_CREDENTIALS")).exists():
        return
    encoded = env("GOOGLE_SA_JSON_B64")
    if not encoded:
        raise SystemExit("GOOGLE_SA_JSON_B64 is not configured")
    path = ROOT / "gcp_realtime_alert_sa.json"
    path.write_bytes(base64.b64decode(encoded))
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(path)


def query_rows(client: bigquery.Client) -> list[dict[str, Any]]:
    table = env("GA4_EVENTS_TABLE", "columbia-ga4.analytics_358593394.events_*")
    location = env("BQ_LOCATION", "asia-northeast3")
    sql = f"""
    DECLARE end_ts TIMESTAMP DEFAULT TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), HOUR);
    DECLARE start_ts TIMESTAMP DEFAULT TIMESTAMP_SUB(end_ts, INTERVAL 15 DAY);

    WITH base AS (
      SELECT
        TIMESTAMP_TRUNC(TIMESTAMP_MICROS(event_timestamp), HOUR, 'Asia/Seoul') AS hour_kst,
        event_name,
        user_pseudo_id,
        CONCAT(user_pseudo_id, '-', CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_id') AS STRING)) AS session_key,
        device.category AS device,
        CONCAT(COALESCE(traffic_source.source,'(direct)'), ' / ', COALESCE(traffic_source.medium,'(none)')) AS source_medium,
        ecommerce.transaction_id,
        ecommerce.purchase_revenue,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key='page_location') AS page_location,
        item.item_id,
        item.item_name,
        item.price,
        item.quantity
      FROM `{table}` e
      LEFT JOIN UNNEST(IF(ARRAY_LENGTH(items)=0,
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
        items)) item
      WHERE TIMESTAMP_MICROS(event_timestamp) BETWEEN start_ts AND end_ts
        AND REGEXP_CONTAINS(_TABLE_SUFFIX, r'\\d{{8}}$')
        AND event_name IN ('session_start','page_view','view_item','add_to_cart','begin_checkout','purchase','sign_up','login')
    ),
    overall AS (
      SELECT hour_kst, 'overall' AS dimension_type, 'all' AS dimension_value,
        COUNT(DISTINCT session_key) AS sessions,
        COUNT(DISTINCT user_pseudo_id) AS users,
        COUNTIF(event_name='view_item') AS view_item_events,
        COUNTIF(event_name='add_to_cart') AS add_to_cart_events,
        COUNTIF(event_name='begin_checkout') AS checkout_events,
        COUNT(DISTINCT IF(event_name='purchase', transaction_id, NULL)) AS orders,
        SUM(IF(event_name='purchase', COALESCE(purchase_revenue,0),0)) AS revenue,
        COUNTIF(event_name='sign_up') AS signups,
        COUNT(*) AS event_count,
        COUNTIF(event_name='purchase' AND transaction_id IS NULL) AS purchase_without_id,
        COUNTIF(event_name='purchase' AND COALESCE(purchase_revenue,0)<=0) AS zero_revenue_purchase
      FROM base GROUP BY 1
    ),
    channel AS (
      SELECT hour_kst, 'channel' AS dimension_type, source_medium AS dimension_value,
        COUNT(DISTINCT session_key) AS sessions,
        COUNT(DISTINCT user_pseudo_id) AS users,
        COUNTIF(event_name='view_item') AS view_item_events,
        COUNTIF(event_name='add_to_cart') AS add_to_cart_events,
        COUNTIF(event_name='begin_checkout') AS checkout_events,
        COUNT(DISTINCT IF(event_name='purchase', transaction_id, NULL)) AS orders,
        SUM(IF(event_name='purchase', COALESCE(purchase_revenue,0),0)) AS revenue,
        COUNTIF(event_name='sign_up') AS signups,
        COUNT(*) AS event_count,
        COUNTIF(event_name='purchase' AND transaction_id IS NULL) AS purchase_without_id,
        COUNTIF(event_name='purchase' AND COALESCE(purchase_revenue,0)<=0) AS zero_revenue_purchase
      FROM base GROUP BY 1,3 HAVING sessions >= 10
    ),
    device_dim AS (
      SELECT hour_kst, 'device' AS dimension_type, device AS dimension_value,
        COUNT(DISTINCT session_key) AS sessions,
        COUNT(DISTINCT user_pseudo_id) AS users,
        COUNTIF(event_name='view_item') AS view_item_events,
        COUNTIF(event_name='add_to_cart') AS add_to_cart_events,
        COUNTIF(event_name='begin_checkout') AS checkout_events,
        COUNT(DISTINCT IF(event_name='purchase', transaction_id, NULL)) AS orders,
        SUM(IF(event_name='purchase', COALESCE(purchase_revenue,0),0)) AS revenue,
        COUNTIF(event_name='sign_up') AS signups,
        COUNT(*) AS event_count,
        COUNTIF(event_name='purchase' AND transaction_id IS NULL) AS purchase_without_id,
        COUNTIF(event_name='purchase' AND COALESCE(purchase_revenue,0)<=0) AS zero_revenue_purchase
      FROM base GROUP BY 1,3 HAVING sessions >= 10
    ),
    product_dim AS (
      SELECT hour_kst, 'product' AS dimension_type,
        COALESCE(NULLIF(item_id,''), NULLIF(item_name,''), REGEXP_EXTRACT(page_location, r'(?i)(?:product|goods|item)[^A-Za-z0-9]*([A-Za-z0-9_-]{{5,}})')) AS dimension_value,
        COUNT(DISTINCT session_key) AS sessions,
        COUNT(DISTINCT user_pseudo_id) AS users,
        COUNTIF(event_name='view_item') AS view_item_events,
        COUNTIF(event_name='add_to_cart') AS add_to_cart_events,
        COUNTIF(event_name='begin_checkout') AS checkout_events,
        COUNT(DISTINCT IF(event_name='purchase', transaction_id, NULL)) AS orders,
        SUM(IF(event_name='purchase', COALESCE(price,0)*COALESCE(quantity,1),0)) AS revenue,
        0 AS signups,
        COUNT(*) AS event_count,
        COUNTIF(event_name='purchase' AND transaction_id IS NULL) AS purchase_without_id,
        COUNTIF(event_name='purchase' AND COALESCE(price,0)<=0) AS zero_revenue_purchase
      FROM base
      WHERE event_name IN ('view_item','add_to_cart','begin_checkout','purchase')
      GROUP BY 1,3 HAVING dimension_value IS NOT NULL AND view_item_events >= 5
    )
    SELECT * FROM overall
    UNION ALL SELECT * FROM channel
    UNION ALL SELECT * FROM device_dim
    UNION ALL SELECT * FROM product_dim
    """
    job = client.query(sql, location=location)
    return [dict(r.items()) for r in job.result()]


def f(v: Any) -> float:
    try:
        x = float(v or 0)
        return x if math.isfinite(x) else 0.0
    except Exception:
        return 0.0


def baseline(values: list[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    med = statistics.median(values)
    mad = statistics.median([abs(x-med) for x in values]) if len(values) > 1 else 0.0
    sigma = max(mad * 1.4826, statistics.pstdev(values) if len(values) > 1 else 0.0, 1e-9)
    return med, sigma


def detect(rows: list[dict[str, Any]]) -> dict[str, Any]:
    now = dt.datetime.now(KST)
    completed = now.replace(minute=0, second=0, microsecond=0) - dt.timedelta(hours=1)
    current_key = completed.strftime('%Y-%m-%d %H:00:00+00:00')
    grouped: dict[tuple[str,str], list[dict[str,Any]]] = {}
    for row in rows:
        grouped.setdefault((str(row['dimension_type']), str(row['dimension_value'])), []).append(row)

    alerts: list[dict[str, Any]] = []
    metrics = ['sessions','orders','revenue','view_item_events','add_to_cart_events','checkout_events','signups','event_count']
    for (dtype, dvalue), items in grouped.items():
        items.sort(key=lambda x: str(x['hour_kst']))
        current = next((x for x in items if str(x['hour_kst']).startswith(completed.strftime('%Y-%m-%d %H'))), None)
        if not current:
            continue
        peers = [x for x in items if str(x['hour_kst'])[11:13] == completed.strftime('%H') and str(x['hour_kst'])[:10] != completed.strftime('%Y-%m-%d')]
        if len(peers) < 5:
            continue
        for metric in metrics:
            cur = f(current.get(metric)); hist = [f(x.get(metric)) for x in peers]
            med, sigma = baseline(hist)
            z = (cur-med)/sigma if sigma else 0
            delta = (cur/med-1)*100 if med else None
            min_base = {'sessions':30,'orders':2,'revenue':100000,'event_count':100}.get(metric,5)
            if med < min_base:
                continue
            if abs(z) < 3.0 or (delta is not None and abs(delta) < 25):
                continue
            level = 'critical' if abs(z) >= 5 or (delta is not None and abs(delta) >= 60) else 'warning'
            direction = '급증' if cur > med else '급락'
            alerts.append({
                'id': f'{dtype}:{dvalue}:{metric}:{completed.isoformat()}',
                'level': level,
                'category': dtype,
                'metric': metric,
                'title': f'{dvalue} {metric} {direction}',
                'impact': f'현재 {cur:,.0f} · 기준 {med:,.0f}' + (f' · {delta:+.1f}%' if delta is not None else ''),
                'cause': f'동일 시간대 기준선 대비 z-score {z:.1f}',
                'action': '세부 채널·상품·디바이스 및 태깅 상태 확인',
                'dimension_type': dtype,
                'dimension_value': dvalue,
                'current': cur,
                'baseline': med,
                'delta_pct': delta,
                'z_score': z,
                'detected_at': now.isoformat(),
                'link': '../realtime_alerts/index.html'
            })

        sessions=f(current.get('sessions')); orders=f(current.get('orders')); views=f(current.get('view_item_events')); atc=f(current.get('add_to_cart_events')); checkout=f(current.get('checkout_events'))
        if dtype in {'overall','device','channel'} and sessions >= 30:
            cvr = orders/sessions*100
            peer_cvrs=[f(x.get('orders'))/max(f(x.get('sessions')),1)*100 for x in peers]
            med,sigma=baseline(peer_cvrs)
            if med >= .2 and cvr < med*.55 and (med-cvr) >= .3:
                alerts.append({'id':f'{dtype}:{dvalue}:cvr:{completed.isoformat()}','level':'critical','category':dtype,'metric':'cvr','title':f'{dvalue} 구매전환율 급락','impact':f'현재 {cvr:.2f}% · 기준 {med:.2f}%','cause':'세션은 유지되지만 구매 완료가 비정상적으로 낮음','action':'결제·상품 옵션·쿠폰·모바일 구매 플로우 점검','dimension_type':dtype,'dimension_value':dvalue,'current':cvr,'baseline':med,'delta_pct':(cvr/med-1)*100,'z_score':(cvr-med)/sigma if sigma else 0,'detected_at':now.isoformat(),'link':'../realtime_alerts/index.html'})
        if dtype == 'product' and views >= 10:
            atc_rate=atc/views*100
            if atc_rate < 2 and views >= 30:
                alerts.append({'id':f'product:{dvalue}:atc:{completed.isoformat()}','level':'warning','category':'product','metric':'atc_rate','title':f'{dvalue} PDP 장바구니 전환 저하','impact':f'조회 {views:,.0f} · ATC {atc_rate:.1f}%','cause':'상품 상세 트래픽 대비 장바구니 반응 부족','action':'가격·재고·사이즈·CTA·상품 이미지 점검','dimension_type':'product','dimension_value':dvalue,'current':atc_rate,'baseline':None,'delta_pct':None,'z_score':None,'detected_at':now.isoformat(),'link':'../pdp_opportunity/index.html'})
        if f(current.get('purchase_without_id')) > 0 or f(current.get('zero_revenue_purchase')) > 0:
            alerts.append({'id':f'{dtype}:{dvalue}:data-integrity:{completed.isoformat()}','level':'critical','category':'data_quality','metric':'purchase_integrity','title':f'{dvalue} 구매 이벤트 무결성 이상','impact':f"transaction_id 누락 {f(current.get('purchase_without_id')):,.0f} · 0원 구매 {f(current.get('zero_revenue_purchase')):,.0f}",'cause':'GA4 purchase 태깅 또는 결제 이벤트 이상','action':'GTM·purchase payload·결제 완료 페이지 즉시 확인','dimension_type':dtype,'dimension_value':dvalue,'current':None,'baseline':None,'delta_pct':None,'z_score':None,'detected_at':now.isoformat(),'link':'../realtime_alerts/index.html'})

    rank={'critical':0,'warning':1,'info':2}
    alerts=sorted(alerts,key=lambda x:(rank.get(x['level'],9),-abs(x.get('z_score') or 0),-abs(x.get('delta_pct') or 0)))[:100]
    overall = next((x for x in rows if x['dimension_type']=='overall' and str(x['hour_kst']).startswith(completed.strftime('%Y-%m-%d %H'))), {})
    return {'generated_at':now.isoformat(),'observed_hour':completed.isoformat(),'status':'critical' if any(a['level']=='critical' for a in alerts) else 'warning' if alerts else 'healthy','alert_count':len(alerts),'critical_count':sum(a['level']=='critical' for a in alerts),'warning_count':sum(a['level']=='warning' for a in alerts),'current_overall':overall,'alerts':alerts}


def render(payload: dict[str, Any]) -> str:
    data=json.dumps(payload,ensure_ascii=False).replace('</','<\\/')
    return f'''<!doctype html><html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><meta name="robots" content="noindex,nofollow"><title>Realtime Alert Center</title><style>:root{{--bg:#f4f7fb;--card:#fff;--text:#142033;--muted:#718096;--line:#e7edf5;--red:#dc2626;--amber:#d97706}}*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font-family:system-ui,'Noto Sans KR',sans-serif}}main{{max-width:1200px;margin:auto;padding:16px}}h1{{font-size:25px;margin:0}}.sub{{font-size:11px;color:var(--muted)}}.kpis{{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin:14px 0}}.card{{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:13px}}.value{{font-size:22px;font-weight:800}}.alert{{display:grid;grid-template-columns:64px 1fr;gap:8px;padding:11px 0;border-bottom:1px solid var(--line)}}.level{{font-size:9px;font-weight:800}}.critical{{color:var(--red)}}.warning{{color:var(--amber)}}@media(max-width:700px){{main{{padding:9px}}.kpis{{grid-template-columns:repeat(2,1fr);gap:6px}}.card{{padding:10px;border-radius:11px}}.value{{font-size:18px}}h1{{font-size:21px}}}}</style></head><body><main><h1>Realtime Alert Center</h1><div class="sub" id="meta"></div><section class="kpis" id="kpis"></section><section class="card" id="alerts"></section></main><script>const D={data};const n=x=>new Intl.NumberFormat('ko-KR',{{notation:'compact',maximumFractionDigits:1}}).format(x||0);document.querySelector('#meta').textContent='관측 시간 '+D.observed_hour+' · 생성 '+D.generated_at;document.querySelector('#kpis').innerHTML=[['상태',D.status.toUpperCase()],['전체 알림',D.alert_count],['Critical',D.critical_count],['Warning',D.warning_count]].map(x=>'<div class="card"><div class="sub">'+x[0]+'</div><div class="value">'+x[1]+'</div></div>').join('');document.querySelector('#alerts').innerHTML=D.alerts.map(a=>'<div class="alert"><span class="level '+a.level+'">'+a.level.toUpperCase()+'</span><div><b>'+a.title+'</b><div class="sub">'+a.impact+'<br>'+a.cause+'<br>Action: '+a.action+'</div></div></div>').join('')||'<p>현재 활성 이상 징후가 없습니다.</p>';</script></body></html>'''


def main() -> int:
    setup_credentials()
    client=bigquery.Client(project=env('BQ_PROJECT','columbia-ga4'))
    rows=query_rows(client)
    payload=detect(rows)
    OUT.mkdir(parents=True,exist_ok=True)
    (OUT/'alerts.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    (OUT/'index.html').write_text(render(payload),encoding='utf-8')
    (OUT/'meta.json').write_text(json.dumps({'generated_at':payload['generated_at'],'observed_hour':payload['observed_hour'],'status':payload['status'],'alert_count':payload['alert_count']},ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(f"[REALTIME] status={payload['status']} alerts={payload['alert_count']}")
    return 0

if __name__=='__main__':
    raise SystemExit(main())
