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
VERSION = "20261122-layout-v2"

# Matches assets/csk-unified-hub.js route keys, except the owned daily report.
TARGETS = {
    "decision_health": "reports/decision_health/index.html",
    "daily_intelligence": "reports/daily_intelligence/index.html",
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
}
HEAD_END = re.compile(r"</head\s*>", re.I)
BODY_OPEN = re.compile(r"<body(?:\s|>)", re.I)
ASSET_LINK = "csk-saas-unified.css"
ASSET_SCRIPT = "csk-saas-standalone.js"

# Oversized embedded GA4 HTML is too large for GitHub's normal blob upload.
# Keep its original Git object by renaming it and expose a small themed entry.
MAX_GITHUB_BLOB = 90 * 1024 * 1024

def wrap_large_utm(path: Path, *, check: bool = False) -> str:
    """Use an unchanged same-origin original through a themed responsive wrapper.

    Moving an already-tracked file preserves its blob SHA and avoids attempting
    to upload its >100 MiB contents as a newly generated HTML blob.
    """
    if check:
        return "missing_theme"
    legacy = path.with_name("legacy.html")
    if legacy.exists():
        legacy.unlink()
    path.replace(legacy)
    base = Path(os.path.relpath(ROOT / "assets", path.parent)).as_posix().rstrip("/") + "/"
    html = """<!doctype html>
<html lang="ko"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>CSK E-COMM | 소스·매체</title>
<link id="csk-saas-shared-theme" rel="stylesheet" href="__BASE__csk-saas-unified.css?v=__VERSION__">
<script defer data-csk-route="utm_channel" src="__BASE__csk-saas-standalone.js?v=__VERSION__"></script>
<style>html,body{margin:0;background:#f8f9fc}
.csk-utm-shell{max-width:1550px;margin:0 auto;padding:16px 12px 30px}
.csk-utm-head{display:flex;gap:12px;align-items:center;justify-content:space-between;
background:#16192c;color:#fff;padding:15px 20px;border-radius:18px 18px 0 0;
font:700 14px system-ui,sans-serif}.csk-utm-head small{color:#c9c9eb;font-size:11px}
#csk-utm-original{display:block;width:100%;min-height:950px;border:0;background:#f8f9fc}
@media(max-width:640px){.csk-utm-shell{padding:0}.csk-utm-head{border-radius:0}}</style>
</head><body>
<main class="csk-utm-shell">
<div class="csk-utm-head"><span>소스·매체</span><small>원본 성과 데이터</small></div>
<iframe id="csk-utm-original" title="소스·매체 성과 원본" src="./legacy.html" loading="eager"></iframe>
</main>
<script>
(function(){
"use strict";
const f=document.getElementById("csk-utm-original");
const theme=new URL("__BASE__csk-saas-unified.css?v=__VERSION__",location.href).href;
f.addEventListener("load",function(){
 try{
  const d=f.contentDocument;if(!d||!d.body||d.location.origin!==location.origin)return;
  d.body.classList.add("csk-saas-report");
  d.body.dataset.cskRoute="utm_channel";
  if(!d.getElementById("csk-saas-shared-theme")){
   const link=d.createElement("link");link.id="csk-saas-shared-theme";
   link.rel="stylesheet";link.href=theme;d.head.appendChild(link);
  }
  const resize=function(){
   const h=Math.max(950,Math.ceil(d.documentElement.scrollHeight||d.body.scrollHeight)+20);
   if(Math.abs((parseInt(f.style.height,10)||0)-h)>20)f.style.height=Math.min(h,100000)+"px";
  };
  resize();
  if(typeof ResizeObserver!=="undefined")new ResizeObserver(resize).observe(d.body);
 }catch(err){console.warn("Unable to style embedded GA4 report",err)}
});
})();
</script></body></html>"""
    html = html.replace("__BASE__", base).replace("__VERSION__", VERSION)
    path.write_text(html + "\n", encoding="utf-8")
    return "wrapped_large_original"


def patch_report(path: Path, key: str, *, check: bool = False) -> str:
    relative_path = path.relative_to(ROOT).as_posix()
    if relative_path in EXCLUDED or relative_path.startswith("reports/daily_digest/daily/"):
        raise ValueError(f"Refusing to modify an excluded daily report: {relative_path}")
    if not path.is_file() or path.stat().st_size == 0:
        return "not_generated"
    if path.stat().st_size > MAX_GITHUB_BLOB:
        if key != "utm_channel":
            raise ValueError(f"Unexpected >90MiB report HTML: {path}")
        return wrap_large_utm(path, check=check)
    text = path.read_text(encoding="utf-8", errors="replace")
    # The executive Summary must not prepend two redundant diagnostic boards.
    # Their original data/diagnostics remain available in dedicated menu pages.
    if key == "summary":
        for marker in ("DASHBOARD-EXPERIENCE", "DASHBOARD-OPS"):
            text = re.sub(
                rf"<!-- {marker}-START -->.*?<!-- {marker}-END -->",
                "", text, flags=re.S,
            )
    # Rotate hashed CSS/JS references on generated report pages, including
    # documents with an existing theme from the previous build.
    text = re.sub(r'(csk-saas-unified\.css\?v=)[A-Za-z0-9_.-]+',
                  lambda m: m.group(1) + VERSION, text)
    text = re.sub(r'(csk-saas-standalone\.js\?v=)[A-Za-z0-9_.-]+',
                  lambda m: m.group(1) + VERSION, text)
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
    original = path.read_text(encoding="utf-8", errors="replace")
    if check and original != text:
        return "missing_theme"
    if not head_bits:
        if original != text and not check:
            path.write_text(text, encoding="utf-8")
            return "updated"
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
