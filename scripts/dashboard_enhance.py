#!/usr/bin/env python3
"""Enhance the generated Summary dashboard with health and action-center UI."""
from __future__ import annotations

import html
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

KST = timezone(timedelta(hours=9))
SUMMARY = Path("reports/index.html")
META = Path("reports/meta.json")
SUMMARY_JSON = Path("reports/summary.json")
START = "<!-- DASHBOARD-OPS-START -->"
END = "<!-- DASHBOARD-OPS-END -->"


def load(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def number(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = re.sub(r"[^0-9+-.]", "", value)
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def deep_find(obj: Any, names: set[str]) -> Any:
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key.lower() in names:
                return value
        for value in obj.values():
            found = deep_find(value, names)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for value in obj:
            found = deep_find(value, names)
            if found is not None:
                return found
    return None


def status_label(status: str) -> tuple[str, str]:
    return {
        "fresh": ("정상", "#059669"),
        "stale": ("지연", "#dc2626"),
        "partial": ("부분", "#d97706"),
        "missing": ("누락", "#64748b"),
    }.get(status, (status or "확인 필요", "#64748b"))


def build_actions(meta: dict[str, Any], summary: dict[str, Any]) -> list[dict[str, str]]:
    actions: list[dict[str, str]] = []
    reports = meta.get("reports", {}) if isinstance(meta, dict) else {}
    for key, item in reports.items():
        if not isinstance(item, dict):
            continue
        status = item.get("status")
        if status in {"stale", "missing"}:
            age = item.get("age_days")
            detail = f"데이터 기준일 {item.get('data_end') or '확인 불가'}"
            if age is not None:
                detail += f", {age}일 지연"
            actions.append({"priority": "P0", "title": f"{key} 데이터 파이프라인 점검", "detail": detail, "link": "#data-health"})

    revenue_wow = number(deep_find(summary, {"revenue_wow", "wow_revenue", "revenue_wow_pct"}))
    orders_wow = number(deep_find(summary, {"orders_wow", "wow_orders", "orders_wow_pct"}))
    cvr_wow = number(deep_find(summary, {"cvr_wow", "wow_cvr", "cvr_wow_pp"}))
    signups_wow = number(deep_find(summary, {"signups_wow", "wow_signups", "signup_wow_pct"}))

    if revenue_wow is not None and revenue_wow <= -10:
        actions.append({"priority": "P0", "title": "매출 급락 원인 분해", "detail": f"전주 대비 매출 {revenue_wow:.1f}% — 세션·CVR·AOV 기여도를 확인하세요.", "link": "#weeklyKpi"})
    if orders_wow is not None and orders_wow <= -10:
        actions.append({"priority": "P1", "title": "주문 감소 상품·채널 확인", "detail": f"전주 대비 주문 {orders_wow:.1f}% — 상품 및 소스/매체 리포트 확인이 필요합니다.", "link": "../index.html?tab=product_keyword"})
    if cvr_wow is not None and cvr_wow <= -0.2:
        actions.append({"priority": "P1", "title": "전환율 하락 PDP 점검", "detail": f"CVR 전주 대비 {cvr_wow:.2f}%p — PDP·장바구니·체크아웃 퍼널을 확인하세요.", "link": "../index.html?tab=funnel"})
    if signups_wow is not None and signups_wow <= -15:
        actions.append({"priority": "P1", "title": "신규가입 유입 점검", "detail": f"신규가입 전주 대비 {signups_wow:.1f}% — 광고 랜딩과 가입 퍼널을 확인하세요.", "link": "../index.html?tab=member"})

    if not actions:
        actions.append({"priority": "OK", "title": "즉시 조치가 필요한 이상 없음", "detail": "핵심 데이터와 KPI가 설정된 경고 기준 안에 있습니다.", "link": "#data-health"})
    return actions[:6]


def render(meta: dict[str, Any], actions: list[dict[str, str]]) -> str:
    reports = meta.get("reports", {}) if isinstance(meta, dict) else {}
    cards = []
    for key, item in reports.items():
        if not isinstance(item, dict):
            continue
        label, color = status_label(str(item.get("status", "missing")))
        end = html.escape(str(item.get("data_end") or "확인 불가"))
        age = item.get("age_days")
        age_text = "-" if age is None else f"{age}일"
        cards.append(f'<article class="ops-health-card" style="--ops-color:{color}"><div class="ops-health-top"><strong>{html.escape(key)}</strong><span>{label}</span></div><div class="ops-health-date">기준일 {end}</div><div class="ops-health-age">지연 {age_text}</div></article>')

    action_html = []
    for action in actions:
        cls = "ok" if action["priority"] == "OK" else "warn"
        action_html.append(f'<a class="ops-action {cls}" href="{html.escape(action["link"])}"><span class="ops-priority">{html.escape(action["priority"])}</span><span><strong>{html.escape(action["title"])}</strong><small>{html.escape(action["detail"])}</small></span></a>')

    overall = str(meta.get("overall_status", "partial"))
    overall_label, overall_color = status_label(overall)
    built = html.escape(str(meta.get("build", datetime.now(KST).strftime("%Y-%m-%d %H:%M KST"))))
    return f'''{START}
<style>
#dashboard-ops{{font-family:inherit;margin:0 auto 28px;max-width:1500px}}.ops-shell{{background:linear-gradient(145deg,rgba(255,255,255,.94),rgba(248,250,252,.82));border:1px solid rgba(148,163,184,.22);border-radius:30px;padding:22px;box-shadow:0 18px 48px rgba(15,23,42,.08)}}
.ops-head{{display:flex;gap:16px;align-items:flex-end;justify-content:space-between;flex-wrap:wrap}}.ops-kicker{{font-size:11px;font-weight:900;letter-spacing:.18em;color:#64748b}}.ops-head h2{{margin:5px 0 0;font-size:26px}}.ops-overall{{border-radius:999px;padding:9px 13px;background:{overall_color}15;color:{overall_color};font-weight:900;border:1px solid {overall_color}35}}.ops-updated{{font-size:11px;color:#64748b;margin-top:5px}}
.ops-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px;margin-top:18px}}.ops-health-card{{background:#fff;border:1px solid #e2e8f0;border-left:4px solid var(--ops-color);border-radius:19px;padding:14px;box-shadow:0 8px 22px rgba(15,23,42,.04)}}.ops-health-top{{display:flex;justify-content:space-between;gap:8px}}.ops-health-top span{{font-size:11px;color:var(--ops-color);font-weight:900}}.ops-health-date,.ops-health-age{{font-size:11px;color:#64748b;margin-top:8px}}
.ops-actions{{display:grid;gap:10px;margin-top:16px}}.ops-action{{display:flex;gap:12px;align-items:flex-start;text-decoration:none;color:#0f172a;background:#fff;border:1px solid #e2e8f0;border-radius:18px;padding:14px;transition:.2s}}.ops-action:hover{{transform:translateY(-2px);box-shadow:0 12px 28px rgba(15,23,42,.08)}}.ops-priority{{min-width:34px;font-size:10px;font-weight:900;padding:5px 7px;border-radius:999px;background:#fee2e2;color:#b91c1c;text-align:center}}.ops-action.ok .ops-priority{{background:#d1fae5;color:#047857}}.ops-action strong,.ops-action small{{display:block}}.ops-action small{{color:#64748b;margin-top:4px;line-height:1.45}}.ops-section-title{{font-size:14px;margin:22px 0 0;font-weight:900}}@media(max-width:640px){{.ops-shell{{padding:16px;border-radius:24px}}.ops-head h2{{font-size:22px}}}}
</style>
<section id="dashboard-ops" class="ops-shell"><div class="ops-head"><div><div class="ops-kicker">DATA OPERATIONS</div><h2>오늘의 운영 현황</h2><div class="ops-updated">최종 점검 {built}</div></div><div class="ops-overall">전체 상태 · {overall_label}</div></div><h3 id="data-health" class="ops-section-title">데이터 상태판</h3><div class="ops-grid">{''.join(cards)}</div><h3 class="ops-section-title">오늘의 액션 센터</h3><div class="ops-actions">{''.join(action_html)}</div></section>
{END}'''


def main() -> int:
    if not SUMMARY.exists():
        raise SystemExit("reports/index.html missing")
    meta = load(META, {})
    summary = load(SUMMARY_JSON, {})
    block = render(meta, build_actions(meta, summary))
    text = SUMMARY.read_text(encoding="utf-8")
    text = re.sub(re.escape(START) + r".*?" + re.escape(END), "", text, flags=re.S)
    pos = text.lower().find("<body")
    if pos >= 0:
        pos = text.find(">", pos) + 1
        text = text[:pos] + "\n" + block + "\n" + text[pos:]
    else:
        text = block + "\n" + text
    SUMMARY.write_text(text, encoding="utf-8")
    print(f"[OK] enhanced {SUMMARY}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
