#!/usr/bin/env python3
from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "reports" / "v2" / "data.json"
HTML = ROOT / "reports" / "v2" / "index.html"
DECISION_HTML = ROOT / "reports" / "decision_os" / "index.html"


def obj(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def rows(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def esc(value: Any) -> str:
    return html.escape(str("-" if value is None or value == "" else value), quote=True)


def number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def nf(value: Any) -> str:
    n = number(value)
    if n is None:
        return "-"
    absolute = abs(n)
    if absolute >= 100_000_000:
        return f"{n / 100_000_000:.1f}억"
    if absolute >= 10_000:
        return f"{n / 10_000:.1f}만"
    if n.is_integer():
        return f"{int(n):,}"
    return f"{n:,.1f}"


def won(value: Any) -> str:
    text = nf(value)
    return text if text == "-" else f"{text}원"


def pct(value: Any) -> str:
    n = number(value)
    if n is None:
        return "-"
    if abs(n) <= 1:
        n *= 100
    return f"{n:.1f}%"


def empty(message: str) -> str:
    return f'<div class="empty">{esc(message)}</div>'


def kpi_grid(items: list[tuple[str, str]]) -> str:
    return '<div class="kpis">' + "".join(
        f'<div class="kpi"><span class="sub">{esc(label)}</span><b>{value}</b></div>'
        for label, value in items
    ) + "</div>"


def render_page(data: dict[str, Any]) -> str:
    decision = obj(data.get("decision_os"))
    command = obj(decision.get("command_center"))
    metrics = obj(data.get("today_kpi")).get("metrics") or data.get("metrics") or {}
    metrics = obj(metrics)
    realtime = obj(data.get("realtime_alerts"))

    command_html = (
        '<div class="command-grid">'
        f'<div class="command-box wide"><div class="sub">NOW</div><strong>{esc(command.get("headline") or "현재 Decision OS 데이터 연결 대기")}</strong>'
        f'<div class="sub">관측 {esc(command.get("observed_hour") or realtime.get("observed_hour") or "-")}</div></div>'
        f'<div class="command-box"><div class="sub">Critical Incident</div><strong>{nf(command.get("critical_incidents"))}</strong></div>'
        f'<div class="command-box"><div class="sub">오늘 위험매출</div><strong>{won(command.get("revenue_at_risk_today"))}</strong></div>'
        f'<div class="command-box"><div class="sub">Data Trust</div><strong>{nf(command.get("data_trust_score"))}점</strong></div>'
        f'<div class="command-box full"><div class="sub">TOP RECOMMENDED ACTION</div><strong>{esc(command.get("top_recommended_action") or "현재 추천 액션 없음")}</strong></div>'
        "</div>"
    )

    kpis_html = kpi_grid([
        ("매출", won(metrics.get("revenue"))),
        ("주문", nf(metrics.get("orders"))),
        ("세션", nf(metrics.get("sessions"))),
        ("CVR", pct(metrics.get("cvr"))),
        ("사용자", nf(metrics.get("users"))),
        ("AOV", won(metrics.get("aov"))),
        ("가입", nf(metrics.get("signups"))),
        ("Alert", nf(len(rows(data.get("alerts"))))),
    ])
    latest = obj(data.get("today_kpi")).get("latest_event_ts")
    kpis_html += f'<div class="sub foot">GA4 BigQuery 오늘 누적 · 마지막 이벤트 {esc(latest or "연결 대기")}</div>'

    incidents = rows(decision.get("incidents"))
    incidents_html = "".join(
        '<div class="row incident">'
        f'<div><span class="level {esc(x.get("level"))}">{esc(str(x.get("level") or "info").upper())}</span><br><span class="pill">{esc(x.get("status"))}</span></div>'
        f'<div><b>{esc(x.get("incident_id"))} · {esc(x.get("title"))}</b><div class="sub">{esc(x.get("dimension_type"))} / {esc(x.get("dimension_value"))} · Alert {nf(x.get("alert_count"))}건<br>최초 {esc(x.get("first_seen"))} · 최근 {esc(x.get("last_seen"))} · 담당 {esc(x.get("owner"))}</div></div>'
        f'<div class="risk"><b>{won(obj(x.get("revenue_at_risk")).get("today"))}</b><div class="sub">오늘 위험매출</div></div></div>'
        for x in incidents
    ) or empty("현재 활성 Incident가 없습니다.")

    risk = obj(decision.get("revenue_risk"))
    risk_html = (
        f'<div class="metric">{won(risk.get("today"))}</div><div class="sub">오늘 예상 손실</div>'
        f'<div class="row"><b>7일 지속</b><span class="right">{won(risk.get("seven_day"))}</span></div>'
        f'<div class="row"><b>회복 가능</b><span class="right">{won(risk.get("recoverable"))}</span></div>'
    )

    causes = rows(decision.get("root_cause_inference"))
    causes_html = "".join(
        f'<div class="row"><b>{esc(x.get("dimension_value"))} · {esc(x.get("cause"))}</b><div class="sub">신뢰도 {nf(x.get("confidence"))}% · {esc(x.get("diagnosis"))}<br>{" · ".join(esc(v) for v in rows(x.get("evidence")))}</div></div>'
        for x in causes[:10]
    ) or empty("원인 추론 대상 Alert가 없습니다.")

    diagnosis = obj(decision.get("measurement_diagnosis"))
    diagnosis_html = kpi_grid([
        ("측정 오류 가능성", f'{nf(diagnosis.get("measurement_probability"))}%'),
        ("실제 성과 문제", f'{nf(diagnosis.get("business_probability"))}%'),
    ]) + f'<div class="row"><span class="pill {esc(diagnosis.get("status"))}">{esc(str(diagnosis.get("status") or "waiting").upper())}</span><div class="sub">{"<br>".join(esc(v) for v in rows(diagnosis.get("evidence"))) or "어드민 주문·PG 데이터 연결 대기"}</div></div>'

    baseline = obj(decision.get("baseline_ensemble"))
    methods = rows(baseline.get("methods"))
    baseline_html = f'<p><span class="pill {esc(baseline.get("status"))}">{esc(str(baseline.get("status") or "waiting").upper())}</span> 기준선 신뢰도 <b>{esc(baseline.get("confidence"))}</b></p>'
    baseline_html += "".join(
        f'<div class="row"><b>{esc(x.get("name"))}</b><span class="right">{esc(x.get("status"))} · {pct(x.get("weight"))}</span></div>' for x in methods
    ) or empty("기준선 데이터 연결 대기")

    realtime_html = kpi_grid([
        ("상태", esc(str(realtime.get("status") or "waiting").upper())),
        ("알림", nf(realtime.get("alert_count"))),
        ("Critical", nf(realtime.get("critical_count"))),
        ("관측", esc(realtime.get("observed_hour"))),
    ]) if realtime else empty("실시간 감시 데이터가 없습니다.")

    experiments = rows(decision.get("experiments"))
    experiments_html = '<div class="tiles">' + "".join(
        f'<div class="mini"><span class="pill">{esc(x.get("effort"))}</span><b>{esc(x.get("title"))}</b><div class="sub">Primary {esc(x.get("primary_metric"))}<br>기대 {esc(x.get("expected_lift"))} · {esc(x.get("duration"))}<br>필요 표본 {nf(x.get("minimum_sample"))} · 예상 {won(x.get("estimated_revenue"))}</div></div>'
        for x in experiments
    ) + "</div>"
    if not experiments:
        experiments_html = empty("추천 실험이 없습니다.")

    products = rows(decision.get("product_context")) or rows(data.get("opportunities"))
    products_html = '<div class="tiles">' + "".join(
        f'<div class="mini"><b>{esc(x.get("product_name") or x.get("name") or x.get("code"))}</b><div class="sub">{esc(x.get("product_code") or x.get("code"))}<br>기회점수 {nf(x.get("opportunity_score") or x.get("score"))} · 세션 {nf(x.get("pdp_sessions") or x.get("sessions"))} · CVR {pct(x.get("cvr"))}<br>PDP 이탈 {pct(x.get("pdp_abandonment_rate"))} · 예상 {won(x.get("expected_revenue"))}</div></div>'
        for x in products[:20]
    ) + "</div>"
    if not products:
        products_html = empty("PDP 데이터 연결 대기 · reports/pdp_opportunity/data.json을 확인하세요.")

    profit = obj(decision.get("profitability"))
    profit_html = f'<p><span class="pill {esc(profit.get("status"))}">{esc(str(profit.get("status") or "waiting").upper())}</span></p>' + kpi_grid([
        ("광고비", won(profit.get("spend"))),
        ("광고매출", won(profit.get("revenue"))),
        ("ROAS", pct(profit.get("roas"))),
        ("기여 ROAS", pct(profit.get("contribution_roas"))),
    ]) + '<p class="sub">마진 데이터가 연결되면 기여이익 기준으로 자동 전환됩니다.</p>'

    features = rows(decision.get("feature_status"))
    features_html = '<div class="tiles">' + "".join(
        f'<div class="mini"><b>{esc(x.get("label"))}</b><span class="right {esc(x.get("status"))}">{esc(str(x.get("status") or "waiting").upper())}</span><div class="sub">{esc(x.get("key"))}</div></div>' for x in features
    ) + "</div>" if features else empty("Decision OS 기능 상태 연결 대기")

    connections = rows(decision.get("connections"))
    connections_html = '<div class="tiles">' + "".join(
        f'<div class="mini"><b>{esc(x.get("name"))}</b><span class="right {esc(x.get("status"))}">{esc(str(x.get("status") or "waiting").upper())}</span><div class="sub">{esc(x.get("path"))}<br>필수: {", ".join(esc(v) for v in rows(x.get("required"))) or "없음"}</div></div>' for x in connections
    ) + "</div>" if connections else empty("데이터 연결 상태를 불러오지 못했습니다.")

    alerts = rows(data.get("alerts"))
    alerts_html = "".join(
        f'<div class="row"><span class="level {esc(x.get("level"))}">{esc(str(x.get("level") or "info").upper())}</span> · <b>{esc(x.get("title"))}</b><div class="sub">{esc(x.get("impact"))}<br>{esc(x.get("cause"))}<br>Action: {esc(x.get("action"))}</div></div>' for x in alerts[:50]
    ) or empty("활성 Alert가 없습니다.")

    cards = [
        ("Executive Command Center", command_html, "s12 command"),
        ("오늘의 핵심 KPI", kpis_html, "s12"),
        ("Incident Center · 중복 Alert 통합", incidents_html, "s8"),
        ("Revenue at Risk", risk_html, "s4"),
        ("Root Cause Inference", causes_html, "s6"),
        ("Measurement vs Business", diagnosis_html, "s6"),
        ("Multi-baseline Ensemble", baseline_html, "s6"),
        ("Realtime BigQuery Monitoring", realtime_html, "s6"),
        ("Decision Actions & Experiment Engine", experiments_html, "s12"),
        ("Product Context · 가격/재고/PDP", products_html, "s6"),
        ("Profitability Intelligence", profit_html, "s6"),
        ("Feature Readiness · 10개 고도화", features_html, "s12"),
        ("Data Connection Registry", connections_html, "s12"),
        ("Alert Center", alerts_html, "s12"),
    ]
    body = "".join(f'<article class="card {cls}"><h2>{esc(title)}</h2>{content}</article>' for title, content, cls in cards)

    generated = data.get("generated_at") or decision.get("generated_at") or "-"
    return f'''<!doctype html><html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><meta name="robots" content="noindex,nofollow"><title>Marketing Hub v2</title><style>
:root{{--bg:#f3f6fb;--card:#fff;--text:#111827;--muted:#64748b;--line:#e2e8f0;--blue:#0874e8;--red:#dc2626;--amber:#d97706;--green:#059669}}*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font-family:Inter,'Noto Sans KR',system-ui,sans-serif}}main{{max-width:1440px;margin:auto;padding:18px}}.hero{{margin-bottom:12px}}.hero h1{{margin:3px 0;font-size:29px}}.eyebrow{{font-size:10px;letter-spacing:.16em;color:var(--blue);font-weight:900}}.sub{{font-size:11px;color:var(--muted);line-height:1.5}}.foot{{margin-top:9px}}.grid{{display:grid;grid-template-columns:repeat(12,1fr);gap:10px}}.card{{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:15px;box-shadow:0 6px 22px rgba(15,23,42,.045);min-width:0}}.s12{{grid-column:span 12}}.s8{{grid-column:span 8}}.s6{{grid-column:span 6}}.s4{{grid-column:span 4}}h2{{font-size:15px;margin:0 0 10px}}.kpis{{display:grid;grid-template-columns:repeat(4,1fr);gap:7px}}.kpi,.mini{{background:var(--bg);border-radius:11px;padding:11px;min-width:0}}.kpi b{{display:block;font-size:18px;overflow-wrap:anywhere}}.command{{background:linear-gradient(135deg,#111827,#1e3a5f);color:#fff}}.command .sub{{color:#cbd5e1}}.command-grid{{display:grid;grid-template-columns:1.5fr repeat(3,1fr);gap:8px}}.command-box{{padding:13px;border-radius:12px;background:rgba(255,255,255,.08)}}.command-box strong{{display:block;font-size:18px;margin-top:4px}}.full{{grid-column:1/-1}}.row{{padding:10px 0;border-bottom:1px solid var(--line)}}.row:last-child{{border-bottom:0}}.level{{font-size:9px;font-weight:900}}.critical{{color:var(--red)}}.warning{{color:var(--amber)}}.live,.healthy{{color:var(--green)}}.waiting{{color:var(--muted)}}.pill{{display:inline-flex;padding:4px 7px;border-radius:999px;background:#eef2f7;font-size:9px;font-weight:800;margin-right:4px}}.incident{{display:grid;grid-template-columns:72px 1fr 130px;gap:9px;align-items:start}}.risk,.right{{float:right;text-align:right}}.tiles{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:7px}}.empty{{padding:18px;text-align:center;color:var(--muted);font-size:12px}}.metric{{font-size:24px;font-weight:900}}
@media(max-width:760px){{main{{padding:10px 8px 78px}}.hero h1{{font-size:21px}}.s8,.s6,.s4{{grid-column:span 12}}.card{{padding:12px;border-radius:13px}}.kpis{{grid-template-columns:repeat(2,1fr)}}.command-grid{{grid-template-columns:1fr 1fr}}.command-grid .wide{{grid-column:span 2}}.incident{{grid-template-columns:56px 1fr}}.incident .risk{{grid-column:2;float:none;text-align:left}}.tiles{{grid-template-columns:1fr}}.kpi b{{font-size:16px}}h2{{font-size:14px}}}}
</style></head><body><main><header class="hero"><div class="eyebrow">E-COMMERCE DECISION OPERATING SYSTEM</div><h1>Marketing Hub v2</h1><div class="sub">{esc(generated)} · 서버사이드 정적 렌더링</div></header><section class="grid">{body}</section></main></body></html>'''


def main() -> None:
    data = json.loads(DATA.read_text(encoding="utf-8"))
    page = render_page(data)
    HTML.parent.mkdir(parents=True, exist_ok=True)
    HTML.write_text(page, encoding="utf-8")
    DECISION_HTML.parent.mkdir(parents=True, exist_ok=True)
    DECISION_HTML.write_text(page, encoding="utf-8")
    print("[OK] V2 server-side static renderer completed")


if __name__ == "__main__":
    main()
