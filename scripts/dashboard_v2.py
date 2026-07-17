#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import math
import re
from calendar import monthrange
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

KST = timezone(timedelta(hours=9))
ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
OUT = REPORTS / "v2"
CONFIG = ROOT / "config" / "dashboard_v2.json"

METRIC_ALIASES = {
    "revenue": {"revenue", "sales", "purchase_revenue", "total_revenue", "매출", "매출액"},
    "previous_revenue": {"previous_revenue", "prev_revenue", "revenue_prev", "prior_revenue", "전기매출", "전주매출"},
    "sessions": {"sessions", "session", "total_sessions", "세션", "방문"},
    "previous_sessions": {"previous_sessions", "prev_sessions", "sessions_prev", "prior_sessions", "전기세션", "전주세션"},
    "orders": {"orders", "purchases", "transactions", "purchase_count", "주문", "주문수", "구매"},
    "previous_orders": {"previous_orders", "prev_orders", "orders_prev", "prior_orders", "전기주문", "전주주문"},
    "users": {"users", "active_users", "total_users", "사용자", "유저"},
    "cvr": {"cvr", "conversion_rate", "purchase_rate", "전환율", "구매전환율"},
    "aov": {"aov", "average_order_value", "객단가"},
    "spend": {"spend", "cost", "ad_spend", "media_spend", "광고비", "비용"},
    "new_customers": {"new_customers", "new_buyers", "first_buyers", "신규구매자"},
    "signups": {"signups", "sign_up", "registrations", "신규가입", "가입"},
}


def load_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {} if default is None else default


def num(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and math.isfinite(value):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace(",", "")
        percent = "%" in cleaned
        try:
            parsed = float(re.sub(r"[^0-9+\-.]", "", cleaned))
            return parsed / 100 if percent and abs(parsed) > 1 else parsed
        except Exception:
            return None
    return None


def norm_key(key: Any) -> str:
    return re.sub(r"[^0-9a-z가-힣]", "", str(key).lower())


def aliases(name: str) -> set[str]:
    return {norm_key(x) for x in METRIC_ALIASES.get(name, {name})}


def iter_values(obj: Any, trail: tuple[str, ...] = ()) -> Iterable[tuple[tuple[str, ...], str, Any]]:
    if isinstance(obj, dict):
        for key, value in obj.items():
            current = trail + (str(key),)
            yield current, str(key), value
            yield from iter_values(value, current)
    elif isinstance(obj, list):
        for idx, value in enumerate(obj[:5000]):
            yield from iter_values(value, trail + (str(idx),))


def metric_from_obj(obj: Any, name: str) -> tuple[float | None, str | None]:
    wanted = aliases(name)
    candidates: list[tuple[int, float, str]] = []
    for trail, key, value in iter_values(obj):
        if norm_key(key) not in wanted:
            continue
        parsed = num(value)
        if parsed is None:
            continue
        path = ".".join(trail)
        score = 100 - len(trail)
        low = path.lower()
        if any(x in low for x in ("total", "summary", "current", "overall", "kpi")):
            score += 20
        if any(x in low for x in ("row", "items", "products", "channels")):
            score -= 15
        candidates.append((score, parsed, path))
    if not candidates:
        return None, None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1], candidates[0][2]


def json_files() -> list[Path]:
    files = []
    for path in REPORTS.rglob("*.json"):
        if any(part in {"daily", "raw", "archive", "captures"} for part in path.parts):
            continue
        if path.stat().st_size > 15 * 1024 * 1024:
            continue
        files.append(path)
    return sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)


def source_priority(path: Path) -> int:
    text = str(path.relative_to(REPORTS)).lower()
    score = 0
    for token, weight in {
        "summary": 30, "meta": 15, "daily_digest": 22, "hero": 15,
        "utm": 12, "funnel": 12, "purchase": 10, "product": 10,
        "paid": 18, "media": 15, "ads": 15, "v2": -50,
    }.items():
        if token in text:
            score += weight
    return score


def discover_metrics() -> tuple[dict[str, float | None], dict[str, str], list[dict[str, Any]]]:
    found: dict[str, float | None] = {key: None for key in METRIC_ALIASES}
    provenance: dict[str, str] = {}
    status: list[dict[str, Any]] = []
    files = sorted(json_files(), key=lambda p: (source_priority(p), p.stat().st_mtime), reverse=True)
    for path in files:
        data = load_json(path, {})
        detected = 0
        for name in found:
            value, field_path = metric_from_obj(data, name)
            if value is None:
                continue
            detected += 1
            if found[name] is None:
                found[name] = value
                provenance[name] = f"{path.relative_to(ROOT)}::{field_path}"
        if detected:
            age_hours = max((datetime.now().timestamp() - path.stat().st_mtime) / 3600, 0)
            status.append({
                "path": str(path.relative_to(ROOT)),
                "metrics": detected,
                "age_hours": round(age_hours, 1),
                "status": "fresh" if age_hours <= 36 else "stale" if age_hours <= 120 else "old",
            })
    if found["cvr"] is None and found["orders"] is not None and found["sessions"]:
        found["cvr"] = found["orders"] / found["sessions"]
        provenance["cvr"] = "derived:orders/sessions"
    if found["aov"] is None and found["revenue"] is not None and found["orders"]:
        found["aov"] = found["revenue"] / found["orders"]
        provenance["aov"] = "derived:revenue/orders"
    return found, provenance, status[:25]


def find_rows(obj: Any) -> list[dict[str, Any]]:
    candidates: list[list[dict[str, Any]]] = []
    def walk(value: Any) -> None:
        if isinstance(value, list) and value and all(isinstance(x, dict) for x in value[:20]):
            candidates.append(value)
        elif isinstance(value, dict):
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value[:100]:
                walk(child)
    walk(obj)
    return max(candidates, key=len) if candidates else []


def first(row: dict[str, Any], names: set[str]) -> Any:
    wanted = {norm_key(x) for x in names}
    for key, value in row.items():
        if norm_key(key) in wanted:
            return value
    return None


def discover_opportunities() -> tuple[list[dict[str, Any]], str | None]:
    best_rows: list[dict[str, Any]] = []
    best_path: Path | None = None
    best_score = -1
    for path in json_files():
        text = str(path).lower()
        if not any(token in text for token in ("product", "pdp", "keyword", "item", "purchase")):
            continue
        rows = find_rows(load_json(path, {}))
        if not rows:
            continue
        sample_keys = {norm_key(k) for row in rows[:20] for k in row.keys()}
        score = len(rows)
        for names in ("productname", "itemname", "상품명", "productcode", "sku", "sessions", "views", "revenue"):
            if norm_key(names) in sample_keys:
                score += 100
        if score > best_score:
            best_score, best_rows, best_path = score, rows, path
    output: list[dict[str, Any]] = []
    for row in best_rows[:10000]:
        name = first(row, {"product_name", "item_name", "상품명", "name", "product"})
        code = first(row, {"product_code", "item_id", "상품코드", "sku", "item_code"})
        if not name and not code:
            continue
        sessions = num(first(row, {"sessions", "pdp_sessions", "views", "pageviews", "상품조회수", "view_item"})) or 0
        bounce = num(first(row, {"bounce_rate", "exit_rate", "pdp_exit_rate", "이탈률"}))
        cvr = num(first(row, {"cvr", "conversion_rate", "구매전환율"}))
        revenue = num(first(row, {"revenue", "sales", "매출"})) or 0
        orders = num(first(row, {"orders", "purchases", "구매수", "purchase_count"})) or 0
        stock = num(first(row, {"stock", "inventory", "재고"}))
        price = num(first(row, {"price", "selling_price", "판매가"})) or (revenue / orders if orders else 0)
        if bounce is not None and bounce <= 1:
            bounce *= 100
        if cvr is not None and cvr <= 1:
            cvr *= 100
        benchmark_cvr = 2.0
        traffic = min(math.log1p(max(sessions, 0)) / 10, 1)
        friction = min(max((bounce if bounce is not None else 45) / 100, 0), 1)
        gap = min(max((benchmark_cvr - (cvr or 0)) / benchmark_cvr, 0), 1)
        value = min(math.log1p(max(revenue, price * sessions * .01, 0)) / 18, 1)
        availability = .2 if stock == 0 else 1
        score = round(100 * (.34 * traffic + .24 * friction + .27 * gap + .15 * value) * availability, 1)
        expected_orders = round(max(sessions, 0) * max(0, benchmark_cvr - (cvr or 0)) / 100 * .25, 1)
        expected_revenue = round(expected_orders * price, 0)
        reasons = []
        if sessions >= 100:
            reasons.append("트래픽 보유")
        if bounce is not None and bounce >= 60:
            reasons.append("이탈 높음")
        if cvr is not None and cvr < 1.5:
            reasons.append("CVR 낮음")
        if stock == 0:
            reasons.append("재고 없음")
        output.append({
            "name": str(name or code), "code": str(code or ""), "score": score,
            "sessions": sessions, "bounce": bounce, "cvr": cvr, "revenue": revenue,
            "orders": orders, "stock": stock, "expected_orders": expected_orders,
            "expected_revenue": expected_revenue, "reason": " · ".join(reasons) or "상세 점검",
        })
    return sorted(output, key=lambda x: x["score"], reverse=True)[:100], str(best_path.relative_to(ROOT)) if best_path else None


def aggregate_paid_media() -> dict[str, Any]:
    candidates: list[tuple[Path, Any]] = []
    for path in json_files():
        low = str(path).lower()
        if any(x in low for x in ("paid_media", "media_spend", "ad_spend", "paid", "ads")):
            candidates.append((path, load_json(path, {})))
    platforms: list[dict[str, Any]] = []
    total_spend = total_revenue = total_orders = total_new = 0.0
    source_paths: list[str] = []
    for path, data in candidates:
        rows = find_rows(data)
        if not rows and isinstance(data, dict):
            rows = [data]
        detected = False
        for row in rows:
            spend = num(first(row, METRIC_ALIASES["spend"])) or 0
            revenue = num(first(row, METRIC_ALIASES["revenue"])) or 0
            orders = num(first(row, METRIC_ALIASES["orders"])) or 0
            new_customers = num(first(row, METRIC_ALIASES["new_customers"])) or 0
            name = first(row, {"platform", "channel", "source", "media", "매체", "채널"}) or path.parent.name
            if spend <= 0 and revenue <= 0:
                continue
            detected = True
            total_spend += spend; total_revenue += revenue; total_orders += orders; total_new += new_customers
            platforms.append({
                "name": str(name), "spend": spend, "revenue": revenue,
                "roas": revenue / spend * 100 if spend else None,
                "orders": orders, "cac": spend / new_customers if new_customers else None,
            })
        if detected:
            source_paths.append(str(path.relative_to(ROOT)))
    dedup: dict[str, dict[str, Any]] = {}
    for item in platforms:
        key = item["name"].strip().lower()
        if key not in dedup:
            dedup[key] = item
    return {
        "connected": bool(total_spend or total_revenue),
        "spend": total_spend, "revenue": total_revenue,
        "roas": total_revenue / total_spend * 100 if total_spend else None,
        "orders": total_orders, "cac": total_spend / total_new if total_new else None,
        "platforms": sorted(dedup.values(), key=lambda x: x.get("spend") or 0, reverse=True)[:20],
        "sources": source_paths,
    }


def decompose(metrics: dict[str, float | None]) -> dict[str, Any]:
    values = [metrics.get(x) for x in ("revenue", "previous_revenue", "sessions", "previous_sessions", "orders", "previous_orders")]
    if not all(v is not None and v > 0 for v in values):
        return {"ready": False, "message": "전기 매출·세션·주문 집계가 모두 연결되면 자동 분해됩니다."}
    revenue, prev_revenue, sessions, prev_sessions, orders, prev_orders = values
    cvr = orders / sessions; prev_cvr = prev_orders / prev_sessions
    aov = revenue / orders; prev_aov = prev_revenue / prev_orders
    effects = [
        {"name": "세션", "value": (sessions - prev_sessions) * prev_cvr * prev_aov},
        {"name": "CVR", "value": sessions * (cvr - prev_cvr) * prev_aov},
        {"name": "AOV", "value": sessions * cvr * (aov - prev_aov)},
    ]
    return {"ready": True, "current": revenue, "previous": prev_revenue, "change": revenue - prev_revenue, "effects": effects}


def forecast(metrics: dict[str, float | None], config: dict[str, Any]) -> dict[str, Any]:
    now = datetime.now(KST)
    current = metrics.get("revenue")
    if current is None:
        return {"ready": False, "message": "월 누적 매출을 찾지 못했습니다."}
    days = monthrange(now.year, now.month)[1]
    elapsed = max(now.day, 1)
    daily = current / elapsed
    base = daily * days
    target = num(config.get("monthly_revenue_target"))
    target_source = "approved_target" if target else None
    if not target and metrics.get("previous_revenue"):
        target = metrics["previous_revenue"]
        target_source = "previous_period_reference"
    scenarios = {"conservative": base * .92, "base": base, "optimistic": base * 1.08}
    result = {"ready": True, "current": current, "elapsed_days": elapsed, "days": days, "daily_pace": daily, "predicted": base, "scenarios": scenarios, "target": target, "target_source": target_source}
    if target:
        result.update({
            "attainment": current / target * 100,
            "forecast_attainment": base / target * 100,
            "gap": base - target,
            "required_daily": max(target - current, 0) / max(days - elapsed, 1),
        })
    return result


def build_alerts(metrics: dict[str, float | None], status: list[dict[str, Any]], opps: list[dict[str, Any]], ads: dict[str, Any], fc: dict[str, Any]) -> list[dict[str, Any]]:
    alerts: list[dict[str, Any]] = []
    stale = [x for x in status if x["status"] in {"stale", "old"}]
    if stale:
        worst = max(stale, key=lambda x: x["age_hours"])
        alerts.append({"level": "critical" if worst["status"] == "old" else "warning", "category": "data", "title": "데이터 최신성 저하", "impact": f'{len(stale)}개 소스 지연 · 최대 {worst["age_hours"]:.0f}시간', "cause": worst["path"], "action": "원천 적재와 생성 workflow 확인", "link": "../index.html"})
    for current, previous, label, unit in (("revenue", "previous_revenue", "매출", "%"), ("sessions", "previous_sessions", "세션", "%"), ("orders", "previous_orders", "주문", "%")):
        cur, prev = metrics.get(current), metrics.get(previous)
        if cur is not None and prev and prev > 0:
            change = (cur / prev - 1) * 100
            if change <= -15:
                alerts.append({"level": "critical", "category": "performance", "title": f"{label} 급락", "impact": f"전기 대비 {change:.1f}{unit}", "cause": "채널·상품·퍼널 기여도 점검 필요", "action": "원인 분해와 상위 손실 구간 확인", "link": "#decomposition"})
            elif change <= -8:
                alerts.append({"level": "warning", "category": "performance", "title": f"{label} 하락", "impact": f"전기 대비 {change:.1f}{unit}", "cause": "변동 허용범위 초과", "action": "세부 리포트 확인", "link": "../index.html"})
    if opps and opps[0]["score"] >= 65:
        top = opps[0]
        alerts.append({"level": "warning", "category": "pdp", "title": "PDP 개선 기회", "impact": f'{top["name"]} · 예상 추가매출 {top["expected_revenue"]:,.0f}원', "cause": top["reason"], "action": "가격·콘텐츠·CTA·재고 우선 점검", "link": "#opportunity"})
    if not ads.get("connected"):
        alerts.append({"level": "info", "category": "media", "title": "광고비 집계 미연결", "impact": "ROAS·CAC 통합 분석 대기", "cause": "집계 JSON 또는 BigQuery 산출 파일 없음", "action": "paid_media 집계 산출 연결", "link": "#media"})
    elif ads.get("roas") is not None and ads["roas"] < 250:
        alerts.append({"level": "warning", "category": "media", "title": "통합 ROAS 저하", "impact": f'{ads["roas"]:.0f}%', "cause": "비효율 플랫폼 또는 캠페인 존재 가능", "action": "플랫폼별 Spend와 신규 CAC 비교", "link": "#media"})
    if fc.get("target") and fc.get("forecast_attainment", 100) < 90:
        alerts.append({"level": "critical", "category": "forecast", "title": "월 목표 미달 예상", "impact": f'예상 달성률 {fc["forecast_attainment"]:.1f}%', "cause": "현재 일매출 페이스 부족", "action": f'잔여기간 필요 일평균 {fc.get("required_daily", 0):,.0f}원', "link": "#forecast"})
    rank = {"critical": 0, "warning": 1, "info": 2}
    return sorted(alerts, key=lambda x: rank.get(x["level"], 9))[:30]


def decision_actions(alerts: list[dict[str, Any]], opps: list[dict[str, Any]], ads: dict[str, Any], fc: dict[str, Any]) -> list[dict[str, str]]:
    actions: list[dict[str, str]] = []
    for alert in alerts[:4]:
        actions.append({"priority": alert["level"], "title": alert["action"], "reason": alert["title"], "link": alert["link"]})
    if opps:
        actions.append({"priority": "high", "title": f'{opps[0]["name"]} PDP 개선', "reason": f'Opportunity {opps[0]["score"]:.0f}점', "link": "#opportunity"})
    if ads.get("connected") and ads.get("platforms"):
        best = max(ads["platforms"], key=lambda x: x.get("roas") or 0)
        actions.append({"priority": "medium", "title": f'{best["name"]} 효율 구간 확장 검토', "reason": f'ROAS {best.get("roas") or 0:.0f}%', "link": "#media"})
    if fc.get("target") and fc.get("required_daily"):
        actions.append({"priority": "medium", "title": "잔여기간 매출 플랜 수립", "reason": f'필요 일평균 {fc["required_daily"]:,.0f}원', "link": "#forecast"})
    return actions[:8]


def inject_nav() -> None:
    path = ROOT / "index.html"
    if not path.exists():
        return
    text = path.read_text(encoding="utf-8")
    marker = '<div class="nav-item active" data-key="summary"'
    if 'data-key="hub_v2"' not in text and marker in text:
        start = text.find(marker)
        end = text.find("</div>", text.find("</div>", start) + 6) + 6
        item = '\n        <div class="nav-item" data-key="hub_v2" data-target="reports/v2/index.html" data-label="Marketing Hub v2"><i class="fa-solid fa-wand-magic-sparkles"></i><span>Marketing Hub v2</span></div>'
        text = text[:end] + item + text[end:]
    if 'data-key="alert_center"' not in text and 'data-key="hub_v2"' in text:
        needle = '<div class="nav-item" data-key="hub_v2"'
        start = text.find(needle)
        end = text.find("</div>", text.find("</div>", start) + 6) + 6
        item = '\n        <div class="nav-item" data-key="alert_center" data-target="reports/v2/index.html#alerts" data-label="Alert Center"><i class="fa-solid fa-triangle-exclamation"></i><span>Alert Center</span></div>'
        text = text[:end] + item + text[end:]
    path.write_text(text, encoding="utf-8")


def html_page(payload: dict[str, Any]) -> str:
    data = json.dumps(payload, ensure_ascii=False).replace("</", "<\\/")
    return f'''<!doctype html><html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><meta name="robots" content="noindex,nofollow"><title>Marketing Hub v2</title><link rel="manifest" href="manifest.webmanifest"><style>
:root{{--bg:#f4f7fb;--card:#fff;--text:#142033;--muted:#718096;--line:#e7edf5;--blue:#0874e8;--red:#dc2626;--amber:#d97706;--green:#159a63}}*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font-family:Inter,'Noto Sans KR',system-ui,sans-serif}}.v2{{max-width:1440px;margin:auto;padding:22px}}.top{{display:flex;justify-content:space-between;gap:12px;align-items:center}}.top h1{{font-size:30px;margin:4px 0}}.eyebrow{{font-size:10px;letter-spacing:.14em;color:var(--blue);font-weight:800}}.sub{{font-size:11px;color:var(--muted)}}.tools{{display:flex;gap:6px}}button{{border:1px solid var(--line);background:var(--card);border-radius:10px;padding:8px 10px}}.grid{{display:grid;grid-template-columns:repeat(12,1fr);gap:10px;margin-top:12px}}.card{{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:15px;box-shadow:0 5px 18px rgba(22,42,72,.05)}}.s12{{grid-column:span 12}}.s8{{grid-column:span 8}}.s6{{grid-column:span 6}}.s4{{grid-column:span 4}}h2{{font-size:15px;margin:0 0 10px}}.metric{{font-size:26px;font-weight:800;letter-spacing:-.045em}}.kpis{{display:grid;grid-template-columns:repeat(4,1fr);gap:7px}}.kpi{{padding:11px;background:var(--bg);border-radius:10px}}.kpi b{{display:block;font-size:18px}}.brief p{{margin:7px 0;font-size:12px}}.effects,.scenarios,.sources{{display:grid;grid-template-columns:repeat(3,1fr);gap:7px}}.mini{{padding:10px;background:var(--bg);border-radius:10px;font-size:11px}}.alert{{display:grid;grid-template-columns:64px 1fr auto;gap:8px;padding:9px 0;border-bottom:1px solid var(--line);align-items:start}}.level{{font-size:9px;font-weight:800}}.critical{{color:var(--red)}}.warning{{color:var(--amber)}}.info{{color:var(--blue)}}.action{{display:flex;gap:8px;padding:9px 0;border-bottom:1px solid var(--line)}}.dot{{width:8px;height:8px;border-radius:50%;background:var(--blue);margin-top:5px}}.opp{{display:grid;grid-template-columns:38px minmax(160px,1fr) repeat(4,minmax(70px,.45fr));gap:8px;align-items:center;padding:9px 0;border-bottom:1px solid var(--line);color:inherit;text-decoration:none;font-size:11px}}.score{{width:34px;height:34px;border-radius:9px;background:#eaf4ff;color:var(--blue);display:grid;place-items:center;font-weight:800}}.platform{{display:grid;grid-template-columns:1fr repeat(3,90px);gap:8px;padding:8px 0;border-bottom:1px solid var(--line);font-size:11px}}.palette{{position:fixed;inset:0;background:rgba(5,12,24,.48);display:none;place-items:start center;padding-top:10vh;z-index:99}}.palette.open{{display:grid}}.palette-box{{width:min(580px,92vw);background:var(--card);border-radius:14px;padding:10px}}.palette input{{width:100%;padding:11px;border:1px solid var(--line);border-radius:9px}}.palette a{{display:block;padding:10px;color:inherit;text-decoration:none}}
@media(max-width:760px){{.v2{{padding:10px 8px 72px}}.top h1{{font-size:21px}}.eyebrow{{font-size:8px}}.grid{{gap:7px;margin-top:8px}}.card{{padding:11px;border-radius:11px}}.s8,.s6,.s4{{grid-column:span 12}}h2{{font-size:13px;margin-bottom:7px}}.metric{{font-size:20px}}.kpis{{grid-template-columns:repeat(2,1fr);gap:6px}}.kpi{{padding:9px}}.kpi b{{font-size:16px}}.effects,.scenarios,.sources{{grid-template-columns:repeat(3,1fr);gap:5px}}.mini{{padding:8px;font-size:9px}}.alert{{grid-template-columns:50px 1fr;padding:8px 0}}.alert>a{{display:none}}.opp{{grid-template-columns:34px 1fr}}.opp>*:nth-child(n+3){{display:none}}.platform{{grid-template-columns:1fr 70px 70px}}.platform>*:nth-child(4){{display:none}}.brief p{{font-size:11px;margin:5px 0}}}}
</style></head><body><main class="v2"><header class="top"><div><div class="eyebrow">E-COMMERCE DECISION ENGINE</div><h1>Marketing Hub v2</h1><div class="sub" id="updated"></div></div><div class="tools"><button id="cmd">⌘K</button><button id="theme">◐</button></div></header><section class="grid"><article class="card s12"><h2>오늘의 핵심 KPI</h2><div class="kpis" id="kpis"></div></article><article class="card s8"><h2>Executive Brief</h2><div class="brief" id="brief"></div></article><article class="card s4" id="forecast"><h2>Target & Forecast</h2><div id="forecastBody"></div></article><article class="card s6" id="decomposition"><h2>매출 변화 원인 분해</h2><div id="decomp"></div></article><article class="card s6"><h2>Decision Actions</h2><div id="actions"></div></article><article class="card s12" id="alerts"><h2>Alert Center</h2><div id="alertsBody"></div></article><article class="card s6" id="media"><h2>Paid Media 통합</h2><div id="ads"></div></article><article class="card s6"><h2>데이터 연결 상태</h2><div id="sources"></div></article><article class="card s12" id="opportunity"><h2>PDP Opportunity Center</h2><div id="opps"></div></article></section></main><div class="palette" id="palette"><div class="palette-box"><input id="search" placeholder="리포트 또는 기능 검색"><div id="commands"></div></div></div><script>
const D={data};const won=n=>n==null?'-':new Intl.NumberFormat('ko-KR',{{notation:'compact',maximumFractionDigits:1}}).format(n)+'원';const pct=n=>n==null?'-':(Math.abs(n)<=1?n*100:n).toFixed(1)+'%';const nfmt=n=>n==null?'-':new Intl.NumberFormat('ko-KR',{{notation:'compact',maximumFractionDigits:1}}).format(n);document.querySelector('#updated').textContent=D.generated_at+' · '+D.source_count+'개 집계 소스';const M=D.metrics;document.querySelector('#kpis').innerHTML=[['매출',won(M.revenue)],['주문',nfmt(M.orders)],['세션',nfmt(M.sessions)],['CVR',pct(M.cvr)]].map(x=>'<div class="kpi"><span class="sub">'+x[0]+'</span><b>'+x[1]+'</b></div>').join('');let brief=[];if(D.alerts.some(x=>x.level==='critical'))brief.push('Critical 경고가 '+D.alerts.filter(x=>x.level==='critical').length+'건 있습니다.');else brief.push('현재 Critical 경고는 없습니다.');if(D.decomposition.ready){{const e=[...D.decomposition.effects].sort((a,b)=>Math.abs(b.value)-Math.abs(a.value))[0];brief.push('매출 변화 최대 기여 요인은 '+e.name+'이며 영향액은 '+won(e.value)+'입니다.')}}if(D.forecast.ready)brief.push('현재 페이스의 월말 예상 매출은 '+won(D.forecast.predicted)+'입니다.');if(D.opportunities.length)brief.push('PDP 최우선 개선 상품은 '+D.opportunities[0].name+'입니다.');document.querySelector('#brief').innerHTML=brief.map(x=>'<p>• '+x+'</p>').join('');const F=D.forecast;document.querySelector('#forecastBody').innerHTML=F.ready?'<div class="metric">'+won(F.predicted)+'</div><div class="sub">월말 기준 시나리오</div><div class="scenarios">'+Object.entries(F.scenarios).map(x=>'<div class="mini"><b>'+x[0]+'</b><br>'+won(x[1])+'</div>').join('')+'</div>'+(F.target?'<p class="sub">목표 '+won(F.target)+' · 예상 달성률 '+pct(F.forecast_attainment)+'<br>필요 일평균 '+won(F.required_daily)+'</p>':'<p class="sub">승인 목표가 없어 페이스 예측만 표시합니다.</p>'):'<p>'+F.message+'</p>';const C=D.decomposition;document.querySelector('#decomp').innerHTML=C.ready?'<div class="metric">'+won(C.change)+'</div><div class="effects">'+C.effects.map(x=>'<div class="mini"><b>'+x.name+'</b><br>'+won(x.value)+'</div>').join('')+'</div>':'<p class="sub">'+C.message+'</p>';document.querySelector('#actions').innerHTML=D.actions.map(x=>'<a class="action" href="'+x.link+'"><span class="dot"></span><span><b>'+x.title+'</b><div class="sub">'+x.reason+'</div></span></a>').join('')||'<p class="sub">추천 액션이 없습니다.</p>';document.querySelector('#alertsBody').innerHTML=D.alerts.map(x=>'<div class="alert"><span class="level '+x.level+'">'+x.level.toUpperCase()+'</span><div><b>'+x.title+'</b><div class="sub">'+x.impact+' · '+x.cause+'<br>'+x.action+'</div></div><a href="'+x.link+'">열기</a></div>').join('')||'<p class="sub">활성 경고가 없습니다.</p>';const A=D.ads;document.querySelector('#ads').innerHTML=A.connected?'<div class="metric">'+pct(A.roas)+'</div><div class="sub">통합 ROAS · Spend '+won(A.spend)+'</div>'+A.platforms.slice(0,8).map(x=>'<div class="platform"><b>'+x.name+'</b><span>'+won(x.spend)+'</span><span>'+pct(x.roas)+'</span><span>'+won(x.cac)+'</span></div>').join(''):'<p class="sub">광고 집계 JSON이 아직 생성되지 않았습니다. paid_media 또는 media_spend 산출 파일이 생기면 자동 연결됩니다.</p>';document.querySelector('#sources').innerHTML='<div class="sources">'+D.source_status.slice(0,9).map(x=>'<div class="mini"><b>'+x.status.toUpperCase()+'</b><br>'+x.path.split('/').slice(-2).join('/')+'<br>'+x.age_hours+'h</div>').join('')+'</div>';document.querySelector('#opps').innerHTML=D.opportunities.slice(0,30).map((x,i)=>'<a class="opp" href="../product_keyword/index.html"><span class="score">'+Math.round(x.score)+'</span><span><b>'+(i+1)+'. '+x.name+'</b><div class="sub">'+x.reason+'</div></span><span>'+nfmt(x.sessions)+' 세션</span><span>'+pct(x.cvr)+'</span><span>'+won(x.expected_revenue)+'</span><span>'+won(x.revenue)+'</span></a>').join('')||'<p class="sub">상품 단위 집계 JSON을 찾지 못했습니다.</p>';const commands=[['Summary','../index.html'],['상품 성과','../product_keyword/index.html'],['Funnel','../daily_digest/owned_funnel_tab.html'],['소스/매체','../utm_channel/index.html'],['Alert Center','#alerts'],['PDP Opportunity','#opportunity'],['Paid Media','#media']];const pal=document.querySelector('#palette'),render=q=>document.querySelector('#commands').innerHTML=commands.filter(x=>x[0].toLowerCase().includes(q.toLowerCase())).map(x=>'<a href="'+x[1]+'">'+x[0]+'</a>').join('');render('');document.querySelector('#cmd').onclick=()=>{{pal.classList.add('open');document.querySelector('#search').focus()}};document.querySelector('#search').oninput=e=>render(e.target.value);pal.onclick=e=>{{if(e.target===pal)pal.classList.remove('open')}};document.onkeydown=e=>{{if((e.ctrlKey||e.metaKey)&&e.key.toLowerCase()==='k'){{e.preventDefault();document.querySelector('#cmd').click()}}if(e.key==='Escape')pal.classList.remove('open')}};document.querySelector('#theme').onclick=()=>document.documentElement.classList.toggle('dark');
</script></body></html>'''


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    config = load_json(CONFIG, {})
    metrics, provenance, source_status = discover_metrics()
    opportunities, opportunity_source = discover_opportunities()
    ads = aggregate_paid_media()
    fc = forecast(metrics, config)
    decomposition = decompose(metrics)
    alerts = build_alerts(metrics, source_status, opportunities, ads, fc)
    payload = {
        "generated_at": datetime.now(KST).strftime("%Y-%m-%d %H:%M KST"),
        "metrics": metrics,
        "provenance": provenance,
        "source_status": source_status,
        "source_count": len(source_status),
        "opportunity_source": opportunity_source,
        "opportunities": opportunities,
        "ads": ads,
        "forecast": fc,
        "decomposition": decomposition,
        "alerts": alerts,
    }
    payload["actions"] = decision_actions(alerts, opportunities, ads, fc)
    (OUT / "data.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (OUT / "index.html").write_text(html_page(payload), encoding="utf-8")
    (OUT / "manifest.webmanifest").write_text(json.dumps({"name": "CSK Marketing Hub v2", "short_name": "CSK Hub", "start_url": "./", "display": "standalone", "background_color": "#f4f7fb", "theme_color": "#0874e8"}, ensure_ascii=False), encoding="utf-8")
    inject_nav()
    print(f"[OK] generated Marketing Hub v2 from {len(source_status)} aggregate sources")


if __name__ == "__main__":
    main()
