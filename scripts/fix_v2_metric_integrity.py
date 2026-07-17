#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SUMMARY = ROOT / "reports" / "summary.json"
V2_DATA = ROOT / "reports" / "v2" / "data.json"
V2_HTML = ROOT / "reports" / "v2" / "index.html"


def load(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def number(value):
    try:
        return float(value)
    except Exception:
        return None


def main() -> int:
    if not SUMMARY.exists() or not V2_DATA.exists() or not V2_HTML.exists():
        raise SystemExit("[ERROR] summary or V2 outputs are missing")

    summary = load(SUMMARY)
    v2 = load(V2_DATA)
    utm = summary.get("utm_channel") or {}
    source = utm.get("summary") or {}

    required = ("sessions", "users", "purchase", "revenue")
    if not all(number(source.get(key)) is not None for key in required):
        raise SystemExit("[ERROR] coherent UTM summary KPI bundle is incomplete")

    sessions = number(source.get("sessions")) or 0
    orders = number(source.get("purchase")) or 0
    revenue = number(source.get("revenue")) or 0
    users = number(source.get("users")) or 0
    signups = number(source.get("signups"))
    cvr = orders / sessions if sessions else None
    aov = revenue / orders if orders else None
    period = source.get("period") or f'{utm.get("period_start", "-")} ~ {utm.get("period_end", "-")}'

    v2["metrics"] = {
        "revenue": revenue,
        "previous_revenue": None,
        "sessions": sessions,
        "previous_sessions": None,
        "orders": orders,
        "previous_orders": None,
        "users": users,
        "cvr": cvr,
        "aov": aov,
        "spend": None,
        "new_customers": number(source.get("signup_to_buyers")),
        "signups": signups,
    }
    v2["metric_context"] = {
        "source": "reports/summary.json::utm_channel.summary",
        "period": period,
        "period_start": utm.get("period_start"),
        "period_end": utm.get("period_end"),
        "scope": "rolling_365_days" if int(utm.get("lookback_days") or 0) >= 300 else "report_period",
        "definition": "GA4 BigQuery Export의 동일 기간 집계 KPI 묶음",
    }
    v2["provenance"] = {
        key: f"reports/summary.json::utm_channel.summary.{field}"
        for key, field in {
            "revenue": "revenue", "sessions": "sessions", "orders": "purchase",
            "users": "users", "signups": "signups", "new_customers": "signup_to_buyers",
        }.items()
    }
    v2["provenance"]["cvr"] = "derived:utm_channel.summary.purchase/sessions"
    v2["provenance"]["aov"] = "derived:utm_channel.summary.revenue/purchase"

    # Forecast and decomposition must not run on rolling-year or mixed-period data.
    v2["forecast"] = {
        "ready": False,
        "message": "현재 핵심 KPI는 최근 365일 누적 기준입니다. 월 누적(MTD) 전용 데이터가 연결되기 전에는 월말 예측을 표시하지 않습니다.",
    }
    v2["decomposition"] = {
        "ready": False,
        "message": "동일 기간의 전기 비교 KPI 묶음이 없어 원인 분해를 표시하지 않습니다.",
    }

    # Remove invalid performance/forecast alerts created from mixed metrics.
    keep = []
    for alert in v2.get("alerts") or []:
        if alert.get("category") not in {"performance", "forecast"}:
            keep.append(alert)
    v2["alerts"] = keep
    v2["actions"] = [a for a in (v2.get("actions") or []) if "매출" not in str(a) and "페이스" not in str(a)]

    V2_DATA.write_text(json.dumps(v2, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    html = V2_HTML.read_text(encoding="utf-8")
    encoded = json.dumps(v2, ensure_ascii=False).replace("</", "<\\/")
    html, count = re.subn(r"const D=\{.*?\};const won=", "const D=" + encoded + ";const won=", html, count=1, flags=re.S)
    if count != 1:
        raise SystemExit("[ERROR] could not replace embedded V2 payload")

    # Show the KPI period directly under the generated timestamp.
    html = html.replace(
        "document.querySelector('#updated').textContent=D.generated_at+' · '+D.source_count+'개 집계 소스';",
        "document.querySelector('#updated').textContent=D.generated_at+' · 기준 '+(D.metric_context?.period||'-')+' · '+D.source_count+'개 집계 소스';",
    )
    V2_HTML.write_text(html, encoding="utf-8")

    print(f"[OK] V2 KPI integrity enforced: period={period}, revenue={revenue:.0f}, orders={orders:.0f}, sessions={sessions:.0f}, cvr={(cvr or 0)*100:.2f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
