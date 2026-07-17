#!/usr/bin/env python3
from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
OUT = REPORTS / "decision_os"
V2 = REPORTS / "v2" / "data.json"
KST = timezone(timedelta(hours=9))


def load(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def finite(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def first_existing(paths: list[Path]) -> tuple[Path | None, Any]:
    for path in paths:
        if path.exists():
            return path, load(path, {})
    return None, {}


def source(name: str, paths: list[str], required: list[str] | None = None) -> dict[str, Any]:
    path, data = first_existing([ROOT / x for x in paths])
    required = required or []
    ready = bool(path and isinstance(data, dict) and all(k in data for k in required))
    return {
        "name": name,
        "status": "live" if ready else "waiting",
        "path": str(path.relative_to(ROOT)) if path else paths[0],
        "required": required,
        "data": data if isinstance(data, dict) else {},
    }


def severity_weight(level: str) -> float:
    return {"critical": 1.0, "warning": 0.62, "info": 0.25}.get(level, 0.4)


def metric_family(metric: str) -> str:
    m = metric.lower()
    if any(x in m for x in ("revenue", "orders", "cvr", "purchase")):
        return "commerce"
    if any(x in m for x in ("view_item", "add_to_cart", "checkout", "page_view")):
        return "funnel"
    if any(x in m for x in ("session", "users", "event_count")):
        return "traffic"
    if any(x in m for x in ("integrity", "duplicate", "latency", "missing")):
        return "data_quality"
    return "other"


def cause_inference(alerts: list[dict[str, Any]], metrics: dict[str, Any], connections: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    by_dim: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for a in alerts:
        by_dim.setdefault((str(a.get("dimension_type", "overall")), str(a.get("dimension_value", "all"))), []).append(a)

    for (dtype, dvalue), rows in by_dim.items():
        names = {str(x.get("metric", "")) for x in rows}
        score = 0
        cause = "복합 성과 변동"
        evidence: list[str] = []
        diagnosis = "business"
        if any("purchase_integrity" in x or "event_count" in x for x in names):
            score += 35
            cause = "GA4 태깅 또는 데이터 적재 이상"
            diagnosis = "measurement"
            evidence.append("구매 이벤트 무결성 또는 이벤트 수집 이상")
        if "cvr" in names and any(x in names for x in ("orders", "purchase_events", "revenue")):
            score += 30
            cause = "구매 완료 단계 전환 저하"
            evidence.append("CVR과 주문·매출이 동시에 악화")
        if any("checkout" in x for x in names) and any("purchase" in x or "orders" in x for x in names):
            score += 20
            cause = "체크아웃 이후 구매 완료 마찰"
            evidence.append("체크아웃과 구매 지표의 동시 변동")
        if any("view_item" in x for x in names) and any("add_to_cart" in x for x in names):
            score += 15
            cause = "PDP 콘텐츠·가격·재고 반응 변화"
            evidence.append("상품 조회와 장바구니 반응 변동")
        if dtype == "device":
            score += 10
            evidence.append(f"{dvalue} 디바이스에 집중")
        if dtype == "channel":
            score += 8
            evidence.append(f"{dvalue} 채널에 집중")
        if connections["admin_orders"]["status"] != "live" and diagnosis == "measurement":
            evidence.append("어드민 주문 미연결로 실제 장애 여부 확인 필요")
        confidence = min(95, 45 + score)
        out.append({
            "dimension_type": dtype,
            "dimension_value": dvalue,
            "cause": cause,
            "confidence": confidence,
            "diagnosis": diagnosis,
            "evidence": evidence[:5],
        })
    return sorted(out, key=lambda x: x["confidence"], reverse=True)


def revenue_risk(alert: dict[str, Any], aov: float) -> dict[str, Any]:
    metric = str(alert.get("metric", ""))
    cur = finite(alert.get("current"))
    base = finite(alert.get("baseline"))
    gap = max(base - cur, 0)
    if metric == "revenue":
        hourly = gap
    elif metric in {"orders", "purchase_events"}:
        hourly = gap * aov
    elif metric == "cvr":
        sessions = finite(alert.get("sessions"))
        hourly = max(base - cur, 0) / 100 * sessions * aov
    else:
        delta = abs(finite(alert.get("delta_pct"))) / 100
        hourly = aov * max(delta, 0) * severity_weight(str(alert.get("level", "warning")))
    hourly = max(hourly, 0)
    return {
        "hourly": round(hourly),
        "today": round(hourly * 8),
        "seven_day": round(hourly * 8 * 7),
        "recoverable": round(hourly * 8 * 0.65),
    }


def build_incidents(alerts: list[dict[str, Any]], previous_state: dict[str, Any], aov: float) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for a in alerts:
        key = (
            str(a.get("dimension_type", "overall")),
            str(a.get("dimension_value", "all")),
            metric_family(str(a.get("metric", ""))),
        )
        groups.setdefault(key, []).append(a)
    old = {x.get("signature"): x for x in previous_state.get("incidents", [])}
    incidents = []
    for i, (key, rows) in enumerate(groups.items(), 1):
        dtype, dvalue, family = key
        signature = "|".join(key)
        level = "critical" if any(x.get("level") == "critical" for x in rows) else "warning"
        risks = [revenue_risk(a, aov) for a in rows]
        prev = old.get(signature, {})
        first_seen = prev.get("first_seen") or min((x.get("detected_at") or datetime.now(KST).isoformat()) for x in rows)
        status = prev.get("status", "NEW")
        incident_id = prev.get("incident_id") or f"INC-{datetime.now(KST).strftime('%Y%m%d')}-{i:03d}"
        incidents.append({
            "incident_id": incident_id,
            "signature": signature,
            "title": f"{dvalue} {family} 이상 징후",
            "level": level,
            "status": status,
            "owner": prev.get("owner", "Unassigned"),
            "first_seen": first_seen,
            "last_seen": max((x.get("detected_at") or datetime.now(KST).isoformat()) for x in rows),
            "recurrence_count": int(prev.get("recurrence_count", 0)) + (0 if prev else 1),
            "dimension_type": dtype,
            "dimension_value": dvalue,
            "family": family,
            "alerts": rows,
            "alert_count": len(rows),
            "revenue_at_risk": {
                "hourly": sum(x["hourly"] for x in risks),
                "today": sum(x["today"] for x in risks),
                "seven_day": sum(x["seven_day"] for x in risks),
                "recoverable": sum(x["recoverable"] for x in risks),
            },
        })
    return sorted(incidents, key=lambda x: (x["level"] != "critical", -x["revenue_at_risk"]["today"]))


def product_context(pdp: dict[str, Any], inventory: dict[str, Any]) -> list[dict[str, Any]]:
    products = pdp.get("products") or pdp.get("opportunities") or []
    inv_rows = inventory.get("products") or inventory.get("items") or []
    inv_map = {str(x.get("product_code") or x.get("item_id") or x.get("sku")): x for x in inv_rows if isinstance(x, dict)}
    out = []
    for row in products[:50]:
        code = str(row.get("product_code") or row.get("code") or row.get("item_id") or "")
        inv = inv_map.get(code, {})
        out.append({
            "product_code": code,
            "product_name": row.get("product_name") or row.get("name") or code or "상품",
            "opportunity_score": finite(row.get("score")),
            "sessions": finite(row.get("sessions")),
            "cvr": finite(row.get("cvr")),
            "expected_revenue": finite(row.get("expected_revenue")),
            "price": finite(inv.get("price") or row.get("price")),
            "stock": inv.get("stock"),
            "sold_out_option_rate": inv.get("sold_out_option_rate"),
            "inventory_status": "live" if inv else "waiting",
        })
    return out


def profitability(paid: dict[str, Any], margin: dict[str, Any]) -> dict[str, Any]:
    connected = bool(paid)
    spend = finite(paid.get("spend"))
    revenue = finite(paid.get("revenue"))
    roas = paid.get("roas")
    if roas is None and spend:
        roas = revenue / spend * 100
    margin_rate = finite(margin.get("gross_margin_rate"), 0.0)
    contribution = revenue * margin_rate - spend if margin_rate else None
    return {
        "status": "live" if connected else "waiting",
        "spend": spend,
        "revenue": revenue,
        "roas": roas,
        "gross_margin_rate": margin_rate or None,
        "contribution_profit": contribution,
        "contribution_roas": contribution / spend * 100 if contribution is not None and spend else None,
        "platforms": paid.get("platforms", []),
    }


def experiments(incidents: list[dict[str, Any]], products: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for inc in incidents[:6]:
        family = inc["family"]
        if family == "funnel":
            title = "모바일 Sticky CTA 및 혜택 우선 노출"
            metric = "PDP→ATC rate"
            effort = "낮음"
        elif family == "commerce":
            title = "결제 플로우·쿠폰·옵션 선택 회귀 테스트"
            metric = "Checkout→Purchase rate"
            effort = "중간"
        elif family == "traffic":
            title = "채널 랜딩·UTM·봇 트래픽 검증"
            metric = "Engaged session / CVR"
            effort = "낮음"
        else:
            title = "GTM purchase payload 및 transaction_id 검증"
            metric = "Purchase event integrity"
            effort = "중간"
        out.append({
            "title": title,
            "incident_id": inc["incident_id"],
            "primary_metric": metric,
            "expected_lift": "+5~15% 상대 개선",
            "estimated_revenue": inc["revenue_at_risk"]["recoverable"],
            "effort": effort,
            "duration": "7~14일",
            "minimum_sample": max(1000, inc["alert_count"] * 500),
        })
    if products:
        p = products[0]
        out.append({
            "title": f"{p['product_name']} PDP 가격·재고·콘텐츠 패키지 테스트",
            "incident_id": None,
            "primary_metric": "PDP CVR",
            "expected_lift": "+0.15~0.30%p",
            "estimated_revenue": p.get("expected_revenue", 0),
            "effort": "중간",
            "duration": "14일",
            "minimum_sample": 8000,
        })
    return out[:10]


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    now = datetime.now(KST)
    v2 = load(V2, {})
    rt = source("ga4_realtime", ["reports/realtime_alerts/alerts.json"], ["alerts", "observed_hour"])
    canonical = source("canonical_commerce", ["reports/canonical/snapshot.json"], ["metrics"])
    pdp = source("pdp", ["reports/pdp_opportunity/data.json", "reports/v2/data.json"])
    paid = source("paid_media", ["reports/paid_media/summary.json"])
    inventory = source("inventory_price", ["reports/commerce_ops/inventory.json"])
    admin_orders = source("admin_orders", ["reports/commerce_ops/orders_hourly.json"])
    pg = source("pg_approvals", ["reports/commerce_ops/pg_hourly.json"])
    margin = source("product_margin", ["reports/commerce_ops/margin.json"])
    promo = source("promotion_calendar", ["reports/commerce_ops/promotion_calendar.json"])
    weather = source("weather", ["reports/external/weather.json"])
    connections = {x["name"]: x for x in [rt, canonical, pdp, paid, inventory, admin_orders, pg, margin, promo, weather]}

    alerts = rt["data"].get("alerts", []) if rt["status"] == "live" else []
    metrics = canonical["data"].get("metrics", {}) if canonical["status"] == "live" else v2.get("metrics", {})
    aov = finite(metrics.get("aov")) or (finite(metrics.get("revenue")) / max(finite(metrics.get("orders")), 1))
    previous_state = load(OUT / "state.json", {})
    incidents = build_incidents(alerts, previous_state, aov)
    causes = cause_inference(alerts, metrics, connections)
    products = product_context(pdp["data"], inventory["data"])
    profit = profitability(paid["data"], margin["data"])
    tests = experiments(incidents, products)

    total_risk = sum(x["revenue_at_risk"]["today"] for x in incidents)
    critical = sum(x["level"] == "critical" for x in incidents)
    top_action = tests[0]["title"] if tests else "현재 긴급 액션 없음"
    trust_live = sum(x["status"] == "live" for x in connections.values())
    trust_score = round(trust_live / max(len(connections), 1) * 100)

    diagnosis = {
        "measurement_probability": 0,
        "business_probability": 0,
        "status": "waiting",
        "evidence": [],
    }
    if causes:
        measurement = sum(x["confidence"] for x in causes if x["diagnosis"] == "measurement")
        business = sum(x["confidence"] for x in causes if x["diagnosis"] == "business")
        total = max(measurement + business, 1)
        diagnosis = {
            "measurement_probability": round(measurement / total * 100),
            "business_probability": round(business / total * 100),
            "status": "partial" if admin_orders["status"] != "live" or pg["status"] != "live" else "live",
            "evidence": [x["cause"] for x in causes[:5]],
        }

    baseline = {
        "status": "partial" if rt["status"] == "live" else "waiting",
        "methods": [
            {"name": "동일 시간대 Robust Median/MAD", "status": "live" if rt["status"] == "live" else "waiting", "weight": 0.55},
            {"name": "전주 동일 시각", "status": "derived", "weight": 0.20},
            {"name": "프로모션 보정", "status": promo["status"], "weight": 0.15},
            {"name": "날씨·외부요인 보정", "status": weather["status"], "weight": 0.10},
        ],
        "confidence": "높음" if rt["status"] == "live" and promo["status"] == "live" else "중간",
    }

    command_center = {
        "headline": incidents[0]["title"] if incidents else "현재 중대한 이상 징후 없음",
        "critical_incidents": critical,
        "revenue_at_risk_today": total_risk,
        "top_recommended_action": top_action,
        "data_trust_score": trust_score,
        "observed_hour": rt["data"].get("observed_hour"),
    }

    payload = {
        "version": "decision-os-v1",
        "generated_at": now.isoformat(),
        "command_center": command_center,
        "root_cause_inference": causes,
        "revenue_risk": {
            "today": total_risk,
            "seven_day": sum(x["revenue_at_risk"]["seven_day"] for x in incidents),
            "recoverable": sum(x["revenue_at_risk"]["recoverable"] for x in incidents),
        },
        "baseline_ensemble": baseline,
        "incidents": incidents,
        "measurement_diagnosis": diagnosis,
        "product_context": products[:30],
        "profitability": profit,
        "experiments": tests,
        "connections": [{k: v for k, v in x.items() if k != "data"} for x in connections.values()],
        "feature_status": [
            {"key": "root_cause", "label": "원인 추론", "status": "live" if causes else "waiting"},
            {"key": "revenue_risk", "label": "손실매출", "status": "live" if incidents else "waiting"},
            {"key": "baseline", "label": "다중 기준선", "status": baseline["status"]},
            {"key": "lifecycle", "label": "Alert 상태관리", "status": "live"},
            {"key": "incident_grouping", "label": "Incident 묶음", "status": "live"},
            {"key": "measurement_split", "label": "태깅/실성과 분리", "status": diagnosis["status"]},
            {"key": "product_context", "label": "재고·가격 결합", "status": inventory["status"]},
            {"key": "profitability", "label": "수익성 Alert", "status": profit["status"]},
            {"key": "experiment_engine", "label": "자동 실험 추천", "status": "live" if tests else "waiting"},
            {"key": "command_center", "label": "Executive Command Center", "status": "live"},
        ],
    }

    (OUT / "data.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (OUT / "state.json").write_text(json.dumps({"updated_at": now.isoformat(), "incidents": incidents}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    v2["decision_os"] = payload
    V2.write_text(json.dumps(v2, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"[DECISION_OS] incidents={len(incidents)} critical={critical} risk_today={total_risk:,.0f} trust={trust_score}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
