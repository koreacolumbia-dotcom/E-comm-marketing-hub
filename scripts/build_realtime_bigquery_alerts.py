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
    existing = env("GOOGLE_APPLICATION_CREDENTIALS")
    if existing and Path(existing).exists():
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
    DECLARE query_end_ts TIMESTAMP DEFAULT TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR);
    DECLARE observed_start_ts TIMESTAMP DEFAULT TIMESTAMP_SUB(query_end_ts, INTERVAL 1 HOUR);
    DECLARE start_ts TIMESTAMP DEFAULT TIMESTAMP_SUB(observed_start_ts, INTERVAL 15 DAY);

    WITH base AS (
      SELECT
        DATETIME_TRUNC(DATETIME(TIMESTAMP_MICROS(event_timestamp), 'Asia/Seoul'), HOUR) AS hour_kst,
        TIMESTAMP_MICROS(event_timestamp) AS event_ts,
        event_name,
        user_pseudo_id,
        CONCAT(
          user_pseudo_id, '-',
          COALESCE(CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_id') AS STRING), '0')
        ) AS session_key,
        COALESCE(device.category, 'unknown') AS device,
        CONCAT(
          COALESCE(traffic_source.source, '(direct)'), ' / ',
          COALESCE(traffic_source.medium, '(none)')
        ) AS source_medium,
        NULLIF(TRIM(COALESCE(
          ecommerce.transaction_id,
          (SELECT value.string_value FROM UNNEST(event_params) WHERE key='transaction_id'),
          (SELECT value.string_value FROM UNNEST(event_params) WHERE key='order_no')
        )), '') AS transaction_id,
        COALESCE(
          ecommerce.purchase_revenue,
          (SELECT value.double_value FROM UNNEST(event_params) WHERE key='value'),
          CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key='value') AS FLOAT64),
          0
        ) AS purchase_revenue,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key='page_location') AS page_location,
        COALESCE(
          (SELECT ANY_VALUE(NULLIF(item_id,'')) FROM UNNEST(items) WHERE NULLIF(item_id,'') IS NOT NULL),
          REGEXP_EXTRACT(
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key='page_location'),
            r'(?i)(?:product|goods|item)[^A-Za-z0-9]*([A-Za-z0-9_-]{{5,}})'
          )
        ) AS product_code,
        (SELECT ANY_VALUE(NULLIF(item_name,'')) FROM UNNEST(items) WHERE NULLIF(item_name,'') IS NOT NULL) AS product_name
      FROM `{table}`
      WHERE TIMESTAMP_MICROS(event_timestamp) >= start_ts
        AND TIMESTAMP_MICROS(event_timestamp) < query_end_ts
        AND REGEXP_CONTAINS(_TABLE_SUFFIX, r'^(?:intraday_)?\\d{{8}}$')
        AND event_name IN (
          'session_start','page_view','view_item','add_to_cart',
          'begin_checkout','purchase','sign_up','login'
        )
    ),
    dimensions AS (
      SELECT hour_kst, event_ts, event_name, user_pseudo_id, session_key, transaction_id,
             purchase_revenue, 'overall' AS dimension_type, 'all' AS dimension_value
      FROM base
      UNION ALL
      SELECT hour_kst, event_ts, event_name, user_pseudo_id, session_key, transaction_id,
             purchase_revenue, 'channel', source_medium
      FROM base
      UNION ALL
      SELECT hour_kst, event_ts, event_name, user_pseudo_id, session_key, transaction_id,
             purchase_revenue, 'device', device
      FROM base
      UNION ALL
      SELECT hour_kst, event_ts, event_name, user_pseudo_id, session_key, transaction_id,
             purchase_revenue, 'product', COALESCE(product_code, product_name)
      FROM base
      WHERE event_name IN ('view_item','add_to_cart','begin_checkout','purchase')
        AND COALESCE(product_code, product_name) IS NOT NULL
    ),
    agg AS (
      SELECT
        hour_kst,
        dimension_type,
        dimension_value,
        COUNT(DISTINCT session_key) AS sessions,
        COUNT(DISTINCT user_pseudo_id) AS users,
        COUNTIF(event_name='page_view') AS page_view_events,
        COUNTIF(event_name='view_item') AS view_item_events,
        COUNTIF(event_name='add_to_cart') AS add_to_cart_events,
        COUNTIF(event_name='begin_checkout') AS checkout_events,
        COUNTIF(event_name='purchase') AS purchase_events,
        COUNT(DISTINCT IF(event_name='purchase', transaction_id, NULL)) AS orders,
        SUM(IF(event_name='purchase', purchase_revenue, 0)) AS revenue,
        COUNTIF(event_name='sign_up') AS signups,
        COUNT(*) AS event_count,
        COUNTIF(event_name='purchase' AND transaction_id IS NULL) AS purchase_without_id,
        COUNTIF(event_name='purchase' AND purchase_revenue <= 0) AS zero_revenue_purchase,
        COUNTIF(event_name='purchase' AND transaction_id IS NOT NULL) -
          COUNT(DISTINCT IF(event_name='purchase', transaction_id, NULL)) AS duplicate_purchase_events,
        MAX(event_ts) AS latest_event_ts
      FROM dimensions
      GROUP BY 1,2,3
    )
    SELECT *
    FROM agg
    WHERE dimension_type='overall'
       OR (dimension_type IN ('channel','device') AND sessions >= 10)
       OR (dimension_type='product' AND view_item_events >= 5)
    ORDER BY hour_kst, dimension_type, dimension_value
    """
    job_config = bigquery.QueryJobConfig(use_query_cache=True)
    job = client.query(sql, job_config=job_config, location=location)
    return [dict(r.items()) for r in job.result(timeout=600)]


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
    mad = statistics.median([abs(x - med) for x in values]) if len(values) > 1 else 0.0
    pstdev = statistics.pstdev(values) if len(values) > 1 else 0.0
    return med, max(mad * 1.4826, pstdev, 1e-9)


def alert(*, now: dt.datetime, completed: dt.datetime, level: str, category: str,
          metric: str, title: str, impact: str, cause: str, action: str,
          dtype: str, dvalue: str, current: Any = None, base: Any = None,
          delta: Any = None, z: Any = None, link: str = "../realtime_alerts/index.html") -> dict[str, Any]:
    return {
        "id": f"{dtype}:{dvalue}:{metric}:{completed.isoformat()}",
        "level": level, "category": category, "metric": metric,
        "title": title, "impact": impact, "cause": cause, "action": action,
        "dimension_type": dtype, "dimension_value": dvalue,
        "current": current, "baseline": base, "delta_pct": delta,
        "z_score": z, "detected_at": now.isoformat(), "link": link,
    }


def detect(rows: list[dict[str, Any]]) -> dict[str, Any]:
    now = dt.datetime.now(KST)
    completed = now.replace(minute=0, second=0, microsecond=0) - dt.timedelta(hours=1)
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault((str(row["dimension_type"]), str(row["dimension_value"])), []).append(row)

    alerts: list[dict[str, Any]] = []
    metrics = [
        "sessions", "users", "orders", "revenue", "page_view_events",
        "view_item_events", "add_to_cart_events", "checkout_events",
        "signups", "event_count",
    ]

    for (dtype, dvalue), items in grouped.items():
        items.sort(key=lambda x: str(x["hour_kst"]))
        current = next((x for x in items if str(x["hour_kst"]).startswith(completed.strftime("%Y-%m-%d %H"))), None)
        if not current:
            continue
        peers = [
            x for x in items
            if str(x["hour_kst"])[11:13] == completed.strftime("%H")
            and str(x["hour_kst"])[:10] != completed.strftime("%Y-%m-%d")
        ]

        if len(peers) >= 5:
            for metric in metrics:
                cur = f(current.get(metric))
                hist = [f(x.get(metric)) for x in peers]
                med, sigma = baseline(hist)
                z = (cur - med) / sigma if sigma else 0.0
                delta = (cur / med - 1) * 100 if med else None
                min_base = {
                    "sessions": 30, "users": 25, "orders": 2, "revenue": 100000,
                    "page_view_events": 100, "event_count": 150,
                    "view_item_events": 10, "add_to_cart_events": 5,
                    "checkout_events": 3, "signups": 3,
                }.get(metric, 5)
                if med < min_base or abs(z) < 3 or (delta is not None and abs(delta) < 25):
                    continue
                level = "critical" if abs(z) >= 5 or (delta is not None and abs(delta) >= 60) else "warning"
                direction = "급증" if cur > med else "급락"
                alerts.append(alert(
                    now=now, completed=completed, level=level, category=dtype, metric=metric,
                    title=f"{dvalue} {metric} {direction}",
                    impact=f"현재 {cur:,.0f} · 기준 {med:,.0f}" + (f" · {delta:+.1f}%" if delta is not None else ""),
                    cause=f"동일 시간대 15일 기준선 대비 z-score {z:.1f}",
                    action="채널·디바이스·상품·태깅 상태를 세부 리포트에서 확인",
                    dtype=dtype, dvalue=dvalue, current=cur, base=med, delta=delta, z=z,
                ))

        sessions = f(current.get("sessions")); orders = f(current.get("orders"))
        views = f(current.get("view_item_events")); atc = f(current.get("add_to_cart_events"))
        checkout = f(current.get("checkout_events")); purchases = f(current.get("purchase_events"))

        if len(peers) >= 5 and dtype in {"overall", "device", "channel"} and sessions >= 30:
            cvr = orders / sessions * 100
            peer_cvrs = [f(x.get("orders")) / max(f(x.get("sessions")), 1) * 100 for x in peers]
            med, sigma = baseline(peer_cvrs)
            if med >= 0.2 and cvr < med * 0.55 and (med - cvr) >= 0.3:
                alerts.append(alert(
                    now=now, completed=completed, level="critical", category=dtype, metric="cvr",
                    title=f"{dvalue} 구매전환율 급락",
                    impact=f"현재 {cvr:.2f}% · 기준 {med:.2f}%",
                    cause="세션 대비 구매 완료가 동일 시간대 기준보다 현저히 낮음",
                    action="결제·상품 옵션·쿠폰·모바일 구매 플로우 즉시 점검",
                    dtype=dtype, dvalue=dvalue, current=cvr, base=med,
                    delta=(cvr / med - 1) * 100 if med else None,
                    z=(cvr - med) / sigma if sigma else 0,
                ))

        if dtype == "product" and views >= 30:
            atc_rate = atc / views * 100
            checkout_rate = checkout / views * 100
            purchase_rate = purchases / views * 100
            if atc_rate < 2:
                alerts.append(alert(
                    now=now, completed=completed, level="warning", category="product", metric="atc_rate",
                    title=f"{dvalue} PDP 장바구니 전환 저하",
                    impact=f"조회 {views:,.0f} · ATC {atc_rate:.1f}% · 구매 {purchase_rate:.1f}%",
                    cause="상품 상세 트래픽 대비 장바구니 반응 부족",
                    action="가격·재고·사이즈·CTA·이미지·배송 문구 점검",
                    dtype=dtype, dvalue=dvalue, current=atc_rate,
                    link="../pdp_opportunity/index.html",
                ))
            if atc >= 10 and checkout_rate < 1:
                alerts.append(alert(
                    now=now, completed=completed, level="warning", category="product", metric="checkout_rate",
                    title=f"{dvalue} 체크아웃 진입 저하",
                    impact=f"ATC {atc:,.0f} · Checkout {checkout:,.0f}",
                    cause="장바구니 이후 체크아웃 이동이 비정상적으로 낮음",
                    action="장바구니 UI·혜택 적용·재고·옵션 유효성 확인",
                    dtype=dtype, dvalue=dvalue, current=checkout_rate,
                    link="../pdp_opportunity/index.html",
                ))

        missing_id = f(current.get("purchase_without_id"))
        zero_value = f(current.get("zero_revenue_purchase"))
        duplicates = f(current.get("duplicate_purchase_events"))
        if missing_id > 0 or zero_value > 0 or duplicates > 0:
            alerts.append(alert(
                now=now, completed=completed, level="critical", category="data_quality",
                metric="purchase_integrity", title=f"{dvalue} 구매 이벤트 무결성 이상",
                impact=f"ID 누락 {missing_id:,.0f} · 0원 {zero_value:,.0f} · 중복 {duplicates:,.0f}",
                cause="GA4 purchase payload 또는 결제 완료 태깅 이상 가능성",
                action="GTM·transaction_id·value·중복 발화 여부 즉시 확인",
                dtype=dtype, dvalue=dvalue,
            ))

    overall = next((x for x in rows if x["dimension_type"] == "overall" and str(x["hour_kst"]).startswith(completed.strftime("%Y-%m-%d %H"))), {})
    if not overall:
        alerts.append(alert(
            now=now, completed=completed, level="critical", category="data_quality", metric="missing_hour",
            title="최근 완료 시간 데이터 없음",
            impact=f"관측 대상 {completed.strftime('%Y-%m-%d %H:00 KST')} 집계가 비어 있음",
            cause="GA4 Export 적재 지연 또는 이벤트 수집 중단 가능성",
            action="events_intraday 테이블과 GA4 실시간 수집 상태 확인",
            dtype="overall", dvalue="all",
        ))

    latest_ts = overall.get("latest_event_ts") if overall else None
    if latest_ts:
        try:
            latest = latest_ts if isinstance(latest_ts, dt.datetime) else dt.datetime.fromisoformat(str(latest_ts).replace("Z", "+00:00"))
            expected_end = completed.astimezone(dt.timezone.utc) + dt.timedelta(hours=1)
            lag_minutes = max((expected_end - latest.astimezone(dt.timezone.utc)).total_seconds() / 60, 0)
            if lag_minutes > 90:
                alerts.append(alert(
                    now=now, completed=completed, level="critical", category="data_quality", metric="ingestion_lag",
                    title="GA4 BigQuery 적재 지연",
                    impact=f"마지막 이벤트 기준 약 {lag_minutes:.0f}분 지연",
                    cause="intraday export 또는 GA4 수집 지연 가능성",
                    action="BigQuery events_intraday 테이블과 GA4 Export 상태 확인",
                    dtype="overall", dvalue="all", current=lag_minutes,
                ))
        except Exception:
            pass

    rank = {"critical": 0, "warning": 1, "info": 2}
    dedup: dict[str, dict[str, Any]] = {}
    for item in alerts:
        dedup[item["id"]] = item
    alerts = sorted(
        dedup.values(),
        key=lambda x: (rank.get(x["level"], 9), -abs(x.get("z_score") or 0), -abs(x.get("delta_pct") or 0)),
    )[:100]
    return {
        "generated_at": now.isoformat(),
        "observed_hour": completed.isoformat(),
        "status": "critical" if any(a["level"] == "critical" for a in alerts) else "warning" if alerts else "healthy",
        "alert_count": len(alerts),
        "critical_count": sum(a["level"] == "critical" for a in alerts),
        "warning_count": sum(a["level"] == "warning" for a in alerts),
        "current_overall": overall,
        "monitored_dimensions": len(grouped),
        "alerts": alerts,
    }


def render(payload: dict[str, Any]) -> str:
    data = json.dumps(payload, ensure_ascii=False, default=str).replace("</", "<\\/")
    return f'''<!doctype html><html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><meta name="robots" content="noindex,nofollow"><title>Realtime Alert Center</title><style>:root{{--bg:#f4f7fb;--card:#fff;--text:#142033;--muted:#718096;--line:#e7edf5;--red:#dc2626;--amber:#d97706}}*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font-family:system-ui,'Noto Sans KR',sans-serif}}main{{max-width:1200px;margin:auto;padding:16px}}h1{{font-size:25px;margin:0}}.sub{{font-size:11px;color:var(--muted)}}.kpis{{display:grid;grid-template-columns:repeat(5,1fr);gap:8px;margin:14px 0}}.card{{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:13px}}.value{{font-size:22px;font-weight:800}}.alert{{display:grid;grid-template-columns:64px 1fr;gap:8px;padding:11px 0;border-bottom:1px solid var(--line)}}.level{{font-size:9px;font-weight:800}}.critical{{color:var(--red)}}.warning{{color:var(--amber)}}@media(max-width:700px){{main{{padding:9px}}.kpis{{grid-template-columns:repeat(2,1fr);gap:6px}}.card{{padding:10px;border-radius:11px}}.value{{font-size:18px}}h1{{font-size:21px}}}}</style></head><body><main><h1>Realtime Alert Center</h1><div class="sub" id="meta"></div><section class="kpis" id="kpis"></section><section class="card" id="alerts"></section></main><script>const D={data};document.querySelector('#meta').textContent='관측 '+D.observed_hour+' · 생성 '+D.generated_at;document.querySelector('#kpis').innerHTML=[['상태',D.status.toUpperCase()],['전체',D.alert_count],['Critical',D.critical_count],['Warning',D.warning_count],['감시 대상',D.monitored_dimensions]].map(x=>'<div class="card"><div class="sub">'+x[0]+'</div><div class="value">'+x[1]+'</div></div>').join('');document.querySelector('#alerts').innerHTML=D.alerts.map(a=>'<div class="alert"><span class="level '+a.level+'">'+a.level.toUpperCase()+'</span><div><b>'+a.title+'</b><div class="sub">'+a.impact+'<br>'+a.cause+'<br>Action: '+a.action+'</div></div></div>').join('')||'<p>현재 활성 이상 징후가 없습니다.</p>';</script></body></html>'''


def main() -> int:
    setup_credentials()
    client = bigquery.Client(project=env("BQ_PROJECT", "columbia-ga4"))
    rows = query_rows(client)
    payload = detect(rows)
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "alerts.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    (OUT / "index.html").write_text(render(payload), encoding="utf-8")
    (OUT / "meta.json").write_text(json.dumps({
        "generated_at": payload["generated_at"],
        "observed_hour": payload["observed_hour"],
        "status": payload["status"],
        "alert_count": payload["alert_count"],
        "critical_count": payload["critical_count"],
        "warning_count": payload["warning_count"],
        "monitored_dimensions": payload["monitored_dimensions"],
    }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"[REALTIME] status={payload['status']} alerts={payload['alert_count']} dimensions={payload['monitored_dimensions']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
