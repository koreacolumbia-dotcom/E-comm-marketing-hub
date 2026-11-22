#!/usr/bin/env python3
"""Idempotently theme every non-daily CSK menu report, including direct URLs.

Existing metric definitions, data, markup and functionality remain untouched.
Run after report generation and BEFORE staging commits. Empty/unavailable generated
HTML is never replaced with example content: the next successful generator run
is responsible for producing the document.
"""
from __future__ import annotations

import argparse
import os
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERSION = "20261122-2"

# Matches assets/csk-unified-hub.js route keys, except daily and daily_intelligence.
TARGETS = {
    "decision_health": "reports/decision_health/index.html",
    "summary": "reports/index.html",
    "performance": "reports/performance_dashboard.html",
    "pdp_priority": "reports/pdp_opportunity/index.html",
    "product_keyword": "reports/product_keyword/index.html",
    "utm_channel": "reports/utm_channel/index.html",
    "funnel": "reports/daily_digest/owned_funnel_tab.html",
    "purchase_pattern": "reports/purchase_pattern/index.html",
    "member": "reports/member_funnel/index.html",
    "owned": "reports/owned_portal/index.html",
    "edm_intelligence": "edm-intelligence.html",
    "crm_intelligence": "reports/crm_intelligence/index.html",
    "sale_calendar": "reports/competitor_sale_calendar/index.html",
    "competitor_content": "reports/competitor_content/index.html",
    "competitor": "reports/competitor_intel/dashboard.html",
    "price": "reports/naver_lowest_price.html",
    "hero": "reports/hero_main.html",
    "ops3": "reports/marketing_operations/index.html",
    "ai_agent": "reports/ai_agent/index.html",
    "voc": "reports/external_signal.html",
    "crema": "reports/voc_crema/index.html",
}
EXCLUDED = {
    "reports/daily_digest/index.html",
    "reports/daily_intelligence/index.html",
}
HEAD_END = re.compile(r"</head\s*>", re.I)
BODY_OPEN = re.compile(r"<body(?:\s|>)", re.I)
ASSET_LINK = "csk-saas-unified.css"
ASSET_SCRIPT = "csk-saas-standalone.js"


def patch_report(path: Path, key: str, *, check: bool = False) -> str:
    relative_path = path.relative_to(ROOT).as_posix()
    if relative_path in EXCLUDED or relative_path.startswith("reports/daily_digest/daily/"):
        raise ValueError(f"Refusing to modify an excluded daily report: {relative_path}")
    if not path.is_file() or path.stat().st_size == 0:
        return "not_generated"
    text = path.read_text(encoding="utf-8", errors="replace")
    match = HEAD_END.search(text)
    if not match or not BODY_OPEN.search(text):
        return "not_html"
    asset_base = Path(os.path.relpath(ROOT / "assets", path.parent)).as_posix().rstrip("/") + "/"
    head_bits: list[str] = []
    if ASSET_LINK not in text:
        head_bits.append(
            f'<link id="csk-saas-shared-theme" rel="stylesheet" '
            f'href="{asset_base}{ASSET_LINK}?v={VERSION}">'
        )
    if ASSET_SCRIPT not in text:
        head_bits.append(
            f'<script defer data-csk-route="{key}" '
            f'src="{asset_base}{ASSET_SCRIPT}?v={VERSION}"></script>'
        )
    if not head_bits:
        return "already_present"
    if check:
        return "missing_theme"
    updated = text[:match.start()] + "\n" + "\n".join(head_bits) + "\n" + text[match.start():]
    path.write_text(updated, encoding="utf-8")
    return "updated"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="Fail when a generated target lacks the theme")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()
    errors: list[str] = []
    totals: dict[str, int] = {}
    for key, relative in TARGETS.items():
        result = patch_report(ROOT / relative, key, check=args.check)
        totals[result] = totals.get(result, 0) + 1
        if result == "missing_theme":
            errors.append(relative)
        if not args.quiet:
            print(f"[THEME] {key:22} {result:16} {relative}")
    print("[THEME] total", len(TARGETS), "outcomes:", totals, "daily excluded:", len(EXCLUDED))
    if errors:
        print("[THEME] not applied to generated pages:", ", ".join(errors))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
