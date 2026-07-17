#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from pathlib import Path

from fix_v2_metric_integrity import main as fix_metric_integrity

ROOT = Path(__file__).resolve().parents[1]
PDP = ROOT / "reports" / "pdp_opportunity" / "data.json"
V2_DATA = ROOT / "reports" / "v2" / "data.json"
V2_HTML = ROOT / "reports" / "v2" / "index.html"
INDEX = ROOT / "index.html"


def main() -> int:
    if not PDP.exists():
        raise SystemExit("[ERROR] PDP data.json missing")
    if not V2_DATA.exists() or not V2_HTML.exists():
        raise SystemExit("[ERROR] V2 output missing")

    # Never attach PDP data to a V2 payload whose KPI values were mixed across
    # unrelated report periods. This rewrites the KPI bundle first.
    fix_metric_integrity()

    pdp = json.loads(PDP.read_text(encoding="utf-8"))
    products = pdp.get("products") or pdp.get("rows") or []
    if not products:
        raise SystemExit("[ERROR] PDP data has no products")

    opportunities = []
    for row in products[:100]:
        opportunities.append({
            "name": row.get("product_name") or row.get("product_code") or "상품명 미수집",
            "code": row.get("product_code") or "",
            "score": row.get("opportunity_score") or 0,
            "sessions": row.get("pdp_sessions") or 0,
            "bounce": row.get("pdp_abandonment_rate"),
            "cvr": row.get("cvr") if row.get("cvr") is not None else row.get("purchase_rate"),
            "revenue": row.get("revenue") or 0,
            "orders": row.get("orders") or row.get("purchase_sessions") or 0,
            "stock": row.get("stock"),
            "expected_orders": row.get("expected_orders") or 0,
            "expected_revenue": row.get("expected_revenue") or 0,
            "reason": row.get("reason") or "PDP 상세 점검",
            "atc_rate": row.get("atc_rate"),
            "checkout_rate": row.get("checkout_rate"),
            "top_device": row.get("top_device"),
            "top_source_medium": row.get("top_source_medium"),
        })

    v2 = json.loads(V2_DATA.read_text(encoding="utf-8"))
    v2["opportunity_source"] = "reports/pdp_opportunity/data.json"
    v2["opportunities"] = opportunities
    v2["pdp_summary"] = pdp.get("summary") or {}
    v2["pdp_data_end"] = pdp.get("data_end")

    alerts = [x for x in (v2.get("alerts") or []) if x.get("category") != "pdp"]
    if opportunities and opportunities[0]["score"] >= 65:
        top = opportunities[0]
        alerts.append({
            "level": "warning",
            "category": "pdp",
            "title": "PDP 개선 기회",
            "impact": f'{top["name"]} · 예상 추가매출 {top["expected_revenue"]:,.0f}원',
            "cause": top["reason"],
            "action": "PDP Opportunity에서 콘텐츠·가격·CTA·옵션 구성을 점검",
            "link": "../pdp_opportunity/index.html",
        })
    rank = {"critical": 0, "warning": 1, "info": 2}
    v2["alerts"] = sorted(alerts, key=lambda x: rank.get(x.get("level"), 9))[:30]

    actions = [x for x in (v2.get("actions") or []) if "PDP" not in str(x.get("title", ""))]
    if opportunities:
        top = opportunities[0]
        actions.insert(0, {
            "priority": "high",
            "title": f'{top["name"]} PDP 개선',
            "reason": f'Opportunity {top["score"]:.0f}점 · 예상 {top["expected_revenue"]:,.0f}원',
            "link": "../pdp_opportunity/index.html",
        })
    v2["actions"] = actions[:8]
    V2_DATA.write_text(json.dumps(v2, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    text = V2_HTML.read_text(encoding="utf-8")
    embedded = json.dumps(v2, ensure_ascii=False).replace("</", "<\\/")
    text, count = re.subn(r"const D=.*?;const won=", "const D=" + embedded + ";const won=", text, count=1, flags=re.S)
    if count != 1:
        raise SystemExit("[ERROR] Could not replace embedded V2 payload")
    text = text.replace('href="../product_keyword/index.html"', 'href="../pdp_opportunity/index.html"')
    text = text.replace("['PDP Opportunity','#opportunity']", "['PDP Opportunity','../pdp_opportunity/index.html']")
    V2_HTML.write_text(text, encoding="utf-8")

    if INDEX.exists():
        root = INDEX.read_text(encoding="utf-8")
        if 'data-key="pdp_opportunity"' not in root:
            needle = '<div class="nav-item" data-key="alert_center"'
            start = root.find(needle)
            if start < 0:
                needle = '<div class="nav-item" data-key="hub_v2"'
                start = root.find(needle)
            if start >= 0:
                end = root.find("</div>", root.find("</div>", start) + 6) + 6
                item = '\n        <div class="nav-item" data-key="pdp_opportunity" data-target="reports/pdp_opportunity/index.html" data-label="PDP Opportunity"><i class="fa-solid fa-bag-shopping"></i><span>PDP Opportunity</span></div>'
                root = root[:end] + item + root[end:]
                INDEX.write_text(root, encoding="utf-8")

    print(f"[OK] Connected {len(opportunities)} PDP opportunities to coherent V2 KPI bundle")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
