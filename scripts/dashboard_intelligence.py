#!/usr/bin/env python3
"""Generate privacy-safe executive insights and PDP improvement priorities."""
from __future__ import annotations
import json, re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

KST = timezone(timedelta(hours=9))
OUT = Path("reports/dashboard_intelligence.json")


def load(path: Path, default: Any) -> Any:
    try: return json.loads(path.read_text(encoding="utf-8"))
    except Exception: return default


def num(v: Any) -> float | None:
    if isinstance(v, (int, float)): return float(v)
    if isinstance(v, str):
        try: return float(re.sub(r"[^0-9+\-.]", "", v))
        except ValueError: return None
    return None


def deep(obj: Any, names: set[str]) -> Any:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if str(k).lower() in names: return v
        for v in obj.values():
            found = deep(v, names)
            if found is not None: return found
    elif isinstance(obj, list):
        for v in obj:
            found = deep(v, names)
            if found is not None: return found
    return None


def executive(summary: dict[str, Any], meta: dict[str, Any]) -> list[dict[str, str]]:
    metrics = {
        "매출": num(deep(summary, {"revenue_wow", "wow_revenue", "revenue_wow_pct"})),
        "주문": num(deep(summary, {"orders_wow", "wow_orders", "orders_wow_pct"})),
        "세션": num(deep(summary, {"sessions_wow", "wow_sessions", "session_wow_pct"})),
        "전환율": num(deep(summary, {"cvr_wow", "wow_cvr", "cvr_wow_pp"})),
        "신규가입": num(deep(summary, {"signups_wow", "wow_signups", "signup_wow_pct"})),
    }
    items: list[dict[str, str]] = []
    for name, value in metrics.items():
        if value is None: continue
        unit = "%p" if name == "전환율" else "%"
        tone = "positive" if value > 0 else "negative" if value < 0 else "neutral"
        items.append({"title": f"{name} 전주 대비 {value:+.1f}{unit}", "tone": tone,
                      "detail": "증가 흐름을 유지할 기여 요인을 확인하세요." if value > 0 else "채널·상품·퍼널 기여도를 우선 점검하세요." if value < 0 else "전주와 유사한 수준입니다."})
    stale = [k for k, v in (meta.get("reports", {}) or {}).items() if isinstance(v, dict) and v.get("status") in {"stale", "missing"}]
    if stale: items.insert(0, {"title": "데이터 신뢰도 주의", "tone": "negative", "detail": "지연 또는 누락: " + ", ".join(stale[:4])})
    return items[:5] or [{"title": "핵심 KPI 자동 요약 준비 중", "tone": "neutral", "detail": "다음 데이터 갱신 후 비교 인사이트가 표시됩니다."}]


def candidate_rows(obj: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(obj, list):
        if obj and all(isinstance(x, dict) for x in obj): rows.extend(obj)
        for x in obj: rows.extend(candidate_rows(x))
    elif isinstance(obj, dict):
        for v in obj.values(): rows.extend(candidate_rows(v))
    return rows


def pdp_priorities() -> list[dict[str, Any]]:
    files = list(Path("reports").glob("**/*product*.json")) + list(Path("reports").glob("**/*pdp*.json"))
    seen, ranked = set(), []
    for path in files[:30]:
        for row in candidate_rows(load(path, {})):
            code = str(deep(row, {"product_code", "item_id", "sku", "상품코드", "product_id"}) or "").strip()
            name = str(deep(row, {"product_name", "item_name", "상품명", "name"}) or code).strip()
            key = code or name
            if not key or key in seen: continue
            sessions = num(deep(row, {"sessions", "pdp_sessions", "views", "pageviews", "세션", "조회수"})) or 0
            bounce = num(deep(row, {"bounce_rate", "exit_rate", "pdp_exit_rate", "이탈률"})) or 0
            cvr = num(deep(row, {"cvr", "conversion_rate", "purchase_rate", "전환율"})) or 0
            revenue = num(deep(row, {"revenue", "item_revenue", "매출"})) or 0
            stock = num(deep(row, {"stock", "inventory", "재고"}))
            if bounce <= 1: bounce *= 100
            if cvr <= 1: cvr *= 100
            stock_factor = 1 if stock is None or stock > 0 else 0
            score = stock_factor * (min(sessions / 1000, 4) * 25 + min(max(bounce - 45, 0), 45) + min(revenue / 1_000_000, 4) * 8 + max(3 - cvr, 0) * 8)
            if sessions < 20 or score <= 0: continue
            seen.add(key)
            ranked.append({"product_code": code, "product_name": name[:80], "score": round(score, 1), "sessions": int(sessions), "exit_rate": round(bounce, 1), "cvr": round(cvr, 2), "revenue": int(revenue), "reason": "높은 트래픽 대비 이탈·전환 개선 여지가 큼"})
    return sorted(ranked, key=lambda x: x["score"], reverse=True)[:10]


def main() -> int:
    summary = load(Path("reports/summary.json"), {})
    meta = load(Path("reports/meta.json"), {})
    payload = {"generated_at": datetime.now(KST).isoformat(), "executive_brief": executive(summary, meta), "pdp_priorities": pdp_priorities(), "refresh_mode": "BigQuery 기반 업무시간 매시간 자동 갱신"}
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"[OK] wrote {OUT}; products={len(payload['pdp_priorities'])}")
    return 0

if __name__ == "__main__": raise SystemExit(main())
