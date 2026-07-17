#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
CONFIG = ROOT / "config" / "dashboard_data_contracts.json"
V2_DATA = REPORTS / "v2" / "data.json"
V2_HTML = REPORTS / "v2" / "index.html"
OUT = REPORTS / "canonical"
KST = timezone(timedelta(hours=9))


def load(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {} if default is None else default


def dig(obj: Any, dotted: str) -> Any:
    if not dotted:
        return obj
    cur = obj
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def finite(v: Any) -> float | None:
    try:
        x = float(v)
        return x if math.isfinite(x) else None
    except Exception:
        return None


def source_snapshot(name: str, spec: dict[str, Any]) -> dict[str, Any]:
    path = ROOT / spec["path"]
    raw = load(path, None) if path.exists() else None
    root = dig(raw, spec.get("root", "")) if raw is not None else None
    required = spec.get("required", [])
    missing = [key for key in required if not isinstance(root, dict) or key not in root]
    age_hours = None
    if path.exists():
        age_hours = max((datetime.now().timestamp() - path.stat().st_mtime) / 3600, 0)
    return {
        "name": name,
        "path": spec["path"],
        "period_type": spec.get("period_type"),
        "exists": path.exists(),
        "ready": bool(path.exists() and isinstance(root, dict) and not missing),
        "missing": missing,
        "age_hours": round(age_hours, 1) if age_hours is not None else None,
        "root": root if isinstance(root, dict) else {},
        "mapping": spec.get("mapping", {}),
    }


def build_canonical(config: dict[str, Any]) -> dict[str, Any]:
    sources = {name: source_snapshot(name, spec) for name, spec in config["canonical_sources"].items()}
    commerce = sources["commerce_365d"]
    metrics: dict[str, float | None] = {}
    if commerce["ready"]:
        root = commerce["root"]
        for target, source_key in commerce["mapping"].items():
            metrics[target] = finite(root.get(source_key))
    else:
        metrics = {k: None for k in ["sessions", "users", "orders", "revenue", "cvr", "aov", "signups"]}

    period = None
    if commerce["ready"]:
        period = commerce["root"].get("period")

    pdp = sources["pdp_30d"]
    pdp_products = pdp["root"].get("products", []) if pdp["ready"] else []
    paid = sources["paid_media"]
    paid_root = paid["root"] if paid["ready"] else {}

    return {
        "generated_at": datetime.now(KST).isoformat(),
        "contract_version": config.get("version"),
        "period": period,
        "period_type": commerce.get("period_type"),
        "metrics": metrics,
        "sources": {k: {x: v for x, v in s.items() if x not in {"root", "mapping"}} for k, s in sources.items()},
        "pdp": {
            "ready": pdp["ready"],
            "data_start": pdp["root"].get("data_start") if pdp["ready"] else None,
            "data_end": pdp["root"].get("data_end") if pdp["ready"] else None,
            "row_count": len(pdp_products),
            "top_opportunities": pdp_products[:20],
        },
        "paid_media": {
            "ready": paid["ready"],
            "spend": finite(paid_root.get("spend")),
            "revenue": finite(paid_root.get("revenue")),
            "roas": finite(paid_root.get("roas")),
            "platforms": paid_root.get("platforms", []) if isinstance(paid_root.get("platforms"), list) else [],
        },
    }


def validate(snapshot: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    rules = config["quality_rules"]
    m = snapshot["metrics"]
    checks: list[dict[str, Any]] = []

    def add(code: str, ok: bool, message: str, severity: str = "critical") -> None:
        checks.append({"code": code, "ok": ok, "message": message, "severity": severity})

    required = ["sessions", "orders", "revenue"]
    for key in required:
        add(f"metric_{key}", m.get(key) is not None, f"{key} canonical metric {'ready' if m.get(key) is not None else 'missing'}")

    sessions, orders, revenue = m.get("sessions"), m.get("orders"), m.get("revenue")
    if sessions is not None and orders is not None:
        add("orders_vs_sessions", not rules.get("orders_cannot_exceed_sessions") or orders <= sessions, "orders must not exceed sessions")
    cvr = m.get("cvr")
    if cvr is not None:
        cvr_pct = cvr * 100 if abs(cvr) <= 1 else cvr
        add("cvr_range", rules["cvr_min"] <= cvr_pct <= rules["cvr_max_percent"], f"CVR {cvr_pct:.2f}% is within valid range")
    aov = m.get("aov")
    if aov is not None:
        add("aov_range", rules["aov_min"] <= aov <= rules["aov_max"], f"AOV {aov:,.0f} is within valid range")
    if revenue is not None and orders:
        derived = revenue / orders
        if aov is not None:
            delta = abs(derived - aov) / max(abs(aov), 1)
            add("aov_reconciliation", delta <= .15, f"AOV reconciles with revenue/orders (delta {delta:.1%})")

    source_checks = snapshot["sources"]
    for name, source in source_checks.items():
        if source["exists"] and source["age_hours"] is not None:
            add(f"freshness_{name}", source["age_hours"] <= rules["max_source_age_hours"], f"{name} age {source['age_hours']}h", "warning")

    critical_failures = [x for x in checks if not x["ok"] and x["severity"] == "critical"]
    warnings = [x for x in checks if not x["ok"] and x["severity"] == "warning"]
    score = round(100 * sum(1 for x in checks if x["ok"]) / max(len(checks), 1))
    return {
        "status": "blocked" if critical_failures else "warning" if warnings else "healthy",
        "score": score,
        "checks": checks,
        "critical_failures": critical_failures,
        "warnings": warnings,
    }


def feature_matrix(snapshot: dict[str, Any], quality: dict[str, Any], config: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for key, spec in config["feature_registry"].items():
        ready = True
        reasons = []
        for source in spec.get("requires", []):
            src = snapshot["sources"].get(source, {})
            if not src.get("ready"):
                ready = False
                reasons.append(f"{source} missing")
        required_period = spec.get("requires_period_type")
        if required_period and snapshot.get("period_type") != required_period:
            ready = False
            reasons.append(f"requires {required_period}")
        if key in {"decision_engine", "alert_center", "canonical_kpi"} and quality["status"] == "blocked":
            ready = False
            reasons.append("quality gate blocked")
        out.append({"key": key, "label": spec["label"], "status": "live" if ready else "waiting", "reason": " · ".join(reasons) if reasons else "production ready"})
    return out


def apply_to_v2(snapshot: dict[str, Any], quality: dict[str, Any], features: list[dict[str, Any]]) -> None:
    if not V2_DATA.exists() or not V2_HTML.exists():
        raise SystemExit("V2 output missing")
    v2 = load(V2_DATA, {})
    v2["metrics"] = snapshot["metrics"]
    v2["canonical"] = snapshot
    v2["data_quality"] = quality
    v2["feature_matrix"] = features
    v2["period"] = snapshot.get("period")
    v2["period_type"] = snapshot.get("period_type")
    v2["provenance"] = {k: "reports/summary.json::utm_channel.summary" for k, val in snapshot["metrics"].items() if val is not None}
    v2["source_count"] = sum(1 for x in snapshot["sources"].values() if x["ready"])

    if snapshot["period_type"] != "mtd":
        v2["forecast"] = {"ready": False, "message": "월 누적(MTD) canonical source가 연결되기 전에는 Forecast를 계산하지 않습니다."}
        v2["decomposition"] = {"ready": False, "message": "동일 기간의 비교 기준 세트가 연결되기 전에는 원인 분해를 계산하지 않습니다."}

    alerts = [x for x in v2.get("alerts", []) if x.get("category") not in {"data_quality", "forecast"}]
    if quality["status"] != "healthy":
        alerts.insert(0, {
            "level": "critical" if quality["status"] == "blocked" else "warning",
            "category": "data_quality",
            "title": "데이터 품질 게이트",
            "impact": f"신뢰도 {quality['score']}점",
            "cause": quality["critical_failures"][0]["message"] if quality["critical_failures"] else quality["warnings"][0]["message"],
            "action": "Canonical source와 기간 정합성 확인",
            "link": "#data-quality",
        })
    v2["alerts"] = alerts[:30]

    V2_DATA.write_text(json.dumps(v2, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    text = V2_HTML.read_text(encoding="utf-8")
    embedded = json.dumps(v2, ensure_ascii=False).replace("</", "<\\/")
    text, count = re.subn(r"const D=.*?;const won=", "const D=" + embedded + ";const won=", text, count=1, flags=re.S)
    if count != 1:
        raise SystemExit("Could not update V2 embedded data")
    badge = '<article class="card s12" id="data-quality"><h2>Data Trust & Feature Readiness</h2><div id="enterpriseStatus"></div></article>'
    if 'id="enterpriseStatus"' not in text:
        text = text.replace('<article class="card s12"><h2>오늘의 핵심 KPI</h2>', badge + '<article class="card s12"><h2>오늘의 핵심 KPI</h2>', 1)
        text = text.replace("document.querySelector('#kpis').innerHTML=", "document.querySelector('#enterpriseStatus').innerHTML='<div class=\"kpis\"><div class=\"kpi\"><span class=\"sub\">Data Trust</span><b>'+D.data_quality.score+'점</b></div><div class=\"kpi\"><span class=\"sub\">Quality Gate</span><b>'+D.data_quality.status.toUpperCase()+'</b></div><div class=\"kpi\"><span class=\"sub\">Live Features</span><b>'+D.feature_matrix.filter(x=>x.status===\"live\").length+'/'+D.feature_matrix.length+'</b></div><div class=\"kpi\"><span class=\"sub\">Canonical Period</span><b style=\"font-size:13px\">'+(D.period||'-')+'</b></div></div><div class=\"sources\" style=\"margin-top:7px\">'+D.feature_matrix.map(x=>'<div class=\"mini\"><b>'+x.label+'</b><br>'+x.status.toUpperCase()+'<br>'+x.reason+'</div>').join('')+'</div>';document.querySelector('#kpis').innerHTML=", 1)
    V2_HTML.write_text(text, encoding="utf-8")


def main() -> int:
    config = load(CONFIG, {})
    if not config:
        raise SystemExit("data contract config missing")
    snapshot = build_canonical(config)
    quality = validate(snapshot, config)
    features = feature_matrix(snapshot, quality, config)
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "snapshot.json").write_text(json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (OUT / "quality.json").write_text(json.dumps(quality, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (OUT / "features.json").write_text(json.dumps(features, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    apply_to_v2(snapshot, quality, features)
    print(f"[ENTERPRISE] quality={quality['status']} score={quality['score']} live={sum(x['status']=='live' for x in features)}/{len(features)}")
    if quality["status"] == "blocked":
        raise SystemExit("[BLOCKED] canonical data quality gate failed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
