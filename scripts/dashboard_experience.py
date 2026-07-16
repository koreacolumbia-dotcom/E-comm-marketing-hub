#!/usr/bin/env python3
"""Inject executive brief and PDP priorities without replacing hub navigation."""
from __future__ import annotations
import html, json, re
from pathlib import Path
from typing import Any

# The root index owns the full desktop/mobile navigation. Only the Summary report
# receives the intelligence block. Both paths are cleaned first to remove legacy
# duplicate injections from earlier builds.
CLEAN_TARGETS = [Path("index.html"), Path("reports/index.html")]
INJECT_TARGETS = [Path("reports/index.html")]
DATA = Path("reports/dashboard_intelligence.json")
START = "<!-- DASHBOARD-EXPERIENCE-START -->"
END = "<!-- DASHBOARD-EXPERIENCE-END -->"


def load(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def money(v: Any) -> str:
    try:
        return f"₩{int(float(v)):,}"
    except Exception:
        return "-"


def render(data: dict[str, Any]) -> str:
    briefs = []
    for item in data.get("executive_brief", []):
        tone = html.escape(str(item.get("tone", "neutral")))
        briefs.append(
            f'<article class="dx-brief {tone}"><strong>{html.escape(str(item.get("title", "")))}</strong>'
            f'<span>{html.escape(str(item.get("detail", "")))}</span></article>'
        )

    products = []
    for i, item in enumerate(data.get("pdp_priorities", []), 1):
        products.append(
            f'''<article class="dx-product"><span class="dx-rank">{i}</span><div><strong>{html.escape(str(item.get("product_name") or item.get("product_code") or "상품"))}</strong><small>{html.escape(str(item.get("product_code") or ""))}</small><p>{html.escape(str(item.get("reason", "")))}</p></div><div class="dx-score"><b>{item.get("score", "-")}</b><small>기회점수</small></div><dl><div><dt>세션</dt><dd>{int(item.get("sessions", 0)):,}</dd></div><div><dt>이탈</dt><dd>{item.get("exit_rate", "-")}%</dd></div><div><dt>CVR</dt><dd>{item.get("cvr", "-")}%</dd></div><div><dt>매출</dt><dd>{money(item.get("revenue"))}</dd></div></dl></article>'''
        )
    if not products:
        products.append('<div class="dx-empty">상품 단위 JSON 컬럼 매핑 후 PDP 우선순위가 자동 표시됩니다.</div>')

    refresh = html.escape(str(data.get("refresh_mode", "자동 갱신")))
    return f'''{START}
<style>
html{{scroll-behavior:smooth}}body{{overflow-x:hidden}}#dashboard-experience{{max-width:1500px;margin:0 auto 24px;font-family:inherit}}.dx-shell{{border:1px solid #dbe4ee;background:#fff;border-radius:22px;padding:22px;box-shadow:0 10px 30px rgba(15,23,42,.06)}}.dx-head{{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap}}.dx-head h2{{margin:0;font-size:25px}}.dx-refresh{{border:1px solid #bfdbfe;background:#eff6ff;color:#1d4ed8;border-radius:999px;padding:8px 12px;font-size:12px;font-weight:800}}.dx-grid{{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1.4fr);gap:18px;margin-top:18px}}.dx-panel{{min-width:0;background:#f8fafc;border:1px solid #e2e8f0;border-radius:18px;padding:16px}}.dx-panel h3{{margin:0 0 12px;font-size:15px}}.dx-brief{{display:grid;gap:4px;background:#fff;border:1px solid #e2e8f0;border-left:4px solid #64748b;border-radius:15px;padding:12px;margin-top:9px}}.dx-brief.positive{{border-left-color:#059669}}.dx-brief.negative{{border-left-color:#dc2626}}.dx-brief span,.dx-product small,.dx-product p{{font-size:11px;color:#64748b;line-height:1.45}}.dx-product{{display:grid;grid-template-columns:30px minmax(140px,1fr) 64px;gap:10px;align-items:start;background:#fff;border:1px solid #e2e8f0;border-radius:16px;padding:12px;margin-top:9px}}.dx-rank{{display:grid;place-items:center;width:26px;height:26px;border-radius:9px;background:#0f172a;color:#fff;font-size:11px;font-weight:900}}.dx-product strong,.dx-product small{{display:block;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}.dx-product p{{margin:5px 0 0}}.dx-score{{text-align:right}}.dx-score b,.dx-score small{{display:block}}.dx-score b{{font-size:20px}}.dx-product dl{{grid-column:2/4;display:grid;grid-template-columns:repeat(4,1fr);gap:6px;margin:4px 0 0}}.dx-product dl div{{background:#f8fafc;border-radius:10px;padding:7px}}.dx-product dt{{font-size:9px;color:#64748b}}.dx-product dd{{margin:2px 0 0;font-size:11px;font-weight:800}}.dx-empty{{padding:24px;text-align:center;color:#64748b;font-size:12px}}
@media(max-width:760px){{#dashboard-experience,#dashboard-ops{{margin:0 10px 16px!important}}.dx-shell,.ops-shell{{padding:14px!important;border-radius:18px!important}}.dx-head h2,.ops-head h2{{font-size:20px!important}}.dx-grid{{grid-template-columns:1fr;gap:12px}}.dx-panel{{padding:12px;border-radius:16px}}.dx-product{{grid-template-columns:28px minmax(0,1fr) 56px;padding:10px}}.dx-product dl{{grid-template-columns:repeat(2,1fr)}}.ops-grid{{display:flex!important;overflow-x:auto;scroll-snap-type:x mandatory;padding-bottom:6px;gap:9px!important}}.ops-health-card{{min-width:145px;scroll-snap-align:start}}.ops-action{{padding:12px!important}}iframe{{max-width:100%!important}}table{{font-size:11px}}}}
</style>
<section id="dashboard-experience" class="dx-shell"><div class="dx-head"><div><small>DECISION INTELLIGENCE</small><h2>Executive Brief & PDP Priority</h2></div><span class="dx-refresh">↻ {refresh}</span></div><div class="dx-grid"><section class="dx-panel"><h3>경영 요약</h3>{''.join(briefs)}</section><section class="dx-panel" id="pdp-priority"><h3>PDP 개선 우선순위</h3>{''.join(products)}</section></div></section>
{END}'''


def clean(path: Path) -> None:
    if not path.exists():
        return
    text = path.read_text(encoding="utf-8", errors="ignore")
    cleaned = re.sub(re.escape(START) + r".*?" + re.escape(END), "", text, flags=re.S)
    if cleaned != text:
        path.write_text(cleaned, encoding="utf-8")
        print(f"[OK] removed legacy duplicate experience: {path}")


def inject(path: Path, block: str) -> None:
    if not path.exists():
        return
    text = path.read_text(encoding="utf-8", errors="ignore")
    body = text.lower().find("<body")
    if body >= 0:
        pos = text.find(">", body) + 1
        text = text[:pos] + "\n" + block + "\n" + text[pos:]
    else:
        text = block + "\n" + text
    if 'name="viewport"' not in text.lower():
        text = text.replace("</head>", '<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">\n</head>', 1)
    path.write_text(text, encoding="utf-8")
    print(f"[OK] enhanced experience: {path}")


def main() -> int:
    for target in CLEAN_TARGETS:
        clean(target)
    block = render(load(DATA, {}))
    for target in INJECT_TARGETS:
        inject(target, block)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
