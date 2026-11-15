#!/usr/bin/env python3
"""Source-backed CSK marketing intelligence with explicit data-quality gates.

Reads only aggregate Daily Digest JSON. Does not query customer-level records,
modify campaigns, or send email. A bad source remains bad: never impute zeros.
"""
from __future__ import annotations

import argparse
import datetime as dt
import html
import json
import math
from pathlib import Path
from typing import Any

KST = dt.timezone(dt.timedelta(hours=9))
BASE = Path("reports/daily_digest/data/daily")
OUTPUT = Path("reports/marketing_intelligence")


def n(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def pct(current: Any, base: Any) -> float | None:
    a, b = n(current), n(base)
    return (a / b - 1.0) * 100 if a is not None and b is not None and b > 0 else None


def fmt(value: Any, *, money: bool = False) -> str:
    v = n(value)
    return ("확인 불가" if v is None else
            (f"{v:,.0f}원" if money else f"{v:,.0f}"))


def fmt_pct(value: Any) -> str:
    v = n(value)
    return "비교 불가" if v is None else f"{v:+.1f}%"


def source(day: dt.date, root: Path = BASE) -> dict[str, Any] | None:
    path = root / (day.isoformat() + ".json")
    if not path.exists():
        return None
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else None
    except (OSError, ValueError):
        return None


def check(obj: dict[str, Any] | None, day: dt.date) -> dict[str, Any]:
    issues: list[dict[str, str]] = []
    def issue(code: str, level: str, message: str):
        issues.append({"code": code, "level": level, "message": message})
    if obj is None:
        issue("MISSING_SOURCE", "block", "기준일 Daily Digest JSON이 없거나 읽을 수 없습니다.")
        return {"overall": False, "channel": False, "pdp": False, "paid": False,
                "issues": issues, "etc_share": None, "channel_gap": None,
                "status": "BLOCKED"}
    valid_date = obj.get("end_date") == day.isoformat()
    if not valid_date:
        issue("WRONG_DATE", "block", "JSON의 기준일이 요청한 날짜와 다릅니다.")
    stamp = str(obj.get("built_at_kst") or "")
    try:
        stamp_date = dt.datetime.strptime(stamp[:19], "%Y-%m-%d %H:%M:%S").date()
        if stamp_date < day:
            raise ValueError("timestamp precedes report date")
    except ValueError:
        issue("BAD_TIMESTAMP", "block", "생성 시각이 없거나 기준일보다 오래되었습니다.")
    k = obj.get("kpis") or {}
    required = ("sessions", "orders", "revenue", "cvr", "signups")
    valid_kpis = all(n(k.get(x)) is not None and n(k[x]) >= 0 for x in required)
    if not valid_kpis or (n(k.get("sessions")) or 0) <= 0:
        issue("BAD_KPI", "block", "핵심 KPI가 누락되거나 유효하지 않습니다.")
    overall_ok = valid_date and valid_kpis and bool(stamp) and not any(
        x["code"] == "BAD_TIMESTAMP" for x in issues
    )
    rows = obj.get("channel_snapshot") or []
    if not isinstance(rows, list):
        rows = []
    total = n(k.get("sessions")) or 0
    child_rows = [r for r in rows if isinstance(r, dict)
                  and str(r.get("bucket", "")).lower() != "total"]
    channel_total = sum(n(r.get("sessions")) or 0 for r in child_rows)
    channel_gap = (channel_total - total) / total if total > 0 else None
    etc_rows = [r for r in child_rows if str(r.get("bucket", "")).lower() == "etc"]
    etc_share = ((n(etc_rows[0].get("sessions")) or 0) / total
                 if etc_rows and total > 0 else None)
    channel_ok = overall_ok and bool(child_rows) and etc_share is not None
    if not child_rows:
        issue("NO_CHANNELS", "block", "채널 스냅샷이 없습니다.")
    if channel_gap is not None and abs(channel_gap) > 0.01:
        issue("CHANNEL_MISMATCH", "block",
              f"채널 합계 {channel_total:,.0f} vs 전체 {total:,.0f} "
              f"(차이 {channel_gap:+.1%}) — 채널별 판단 중단")
        channel_ok = False
    if etc_share is None:
        issue("MISSING_ETC", "block", "ETC 분류 또는 총세션을 검증할 수 없습니다.")
        channel_ok = False
    elif etc_share >= 0.05:
        issue("UNASSIGNED_TRAFFIC", "warning" if etc_share < 0.15 else "block",
              f"ETC {etc_share:.1%} — 미귀속 세션 상세 및 GA4 (not set)/(other) 점검 필요")
        channel_ok = False
    if any(n(r.get("sessions")) is None or (n(r.get("sessions")) or 0) < 0
           for r in child_rows):
        issue("BAD_CHANNEL", "block", "채널 세션 값에 결측·음수가 있습니다.")
        channel_ok = False
    pdp = obj.get("pdp_series") or {}
    pdp_rows = pdp.get("rows") or []
    last_axis = (day.strftime("%m/%d"))
    pdp_ok = bool(pdp_rows) and bool(pdp.get("x")) and pdp["x"][-1] == last_axis \
        and any((n(r.get("views_d1")) or 0) > 0 for r in pdp_rows)
    if not pdp_ok:
        issue("PDP_INCOMPLETE", "block",
              "PDP 최신일 수집 누락 또는 카테고리 매핑 오류 — 0을 실제 조회수로 해석 금지")
    elif any(len(r.get("ys") or []) != 7 for r in pdp_rows):
        issue("PDP_TREND", "block", "PDP 7일 추이 데이터 길이가 올바르지 않습니다.")
        pdp_ok = False
    paid = next((r for r in child_rows if r.get("bucket") == "Paid Ad"), None)
    paid_rows = obj.get("paid_detail") or []
    paid_total = next((r for r in paid_rows if r.get("sub_channel") == "Total"), None)
    paid_ok = channel_ok and paid is not None and paid_total is not None
    if paid_ok:
        a, b = n(paid.get("sessions")) or 0, n(paid_total.get("sessions")) or 0
        if abs(a-b) > max(1.0, a * 0.01):
            issue("PAID_DETAIL_MISMATCH", "block",
                  "Paid Ad와 Paid Detail 세션 합계가 일치하지 않습니다.")
            paid_ok = False
    if not paid_ok:
        issue("PAID_NOT_VERIFIED", "info",
              "Paid 상세 성과와 매체별 ROAS 판단을 보류합니다.")
    admin = obj.get("admin_kpis") or {}
    if not any((n(admin.get(k)) or 0) > 0 for k in ("orders", "revenue", "buyers")):
        issue("ADMIN_UNAVAILABLE", "info",
              "어드민 실적이 0으로 내려왔습니다. 실제 매출 0으로 단정하지 않습니다.")
    status = ("BLOCKED" if not overall_ok else
              "PARTIAL" if not (channel_ok and pdp_ok and paid_ok) else "READY")
    return {"overall": overall_ok, "channel": channel_ok, "pdp": pdp_ok,
            "paid": paid_ok, "issues": issues, "etc_share": etc_share,
            "channel_gap": channel_gap, "status": status}


def action(code: str, title: str, observation: str, hypothesis: str,
           validation: str, owner: str, priority: str = "P1") -> dict[str, str]:
    return {"code": code, "title": title, "observation": observation,
            "hypothesis": hypothesis, "validation": validation,
            "owner": owner, "priority": priority}


def analyze(obj: dict[str, Any] | None, qa: dict[str, Any],
            day: dt.date) -> dict[str, Any]:
    actions: list[dict[str, str]] = []
    metrics: list[dict[str, Any]] = []
    if obj is None:
        return {"date": day.isoformat(), "status": qa["status"],
                "quality": qa, "metrics": [], "actions": [],
                "note": "원본 도착 후 재집계해야 합니다."}
    k = obj.get("kpis") or {}
    overall = obj.get("overall") or {}
    previous = overall.get("prev") or {}
    yoy = overall.get("yoy") or {}
    comparison_fields = {
        "sessions": ("sessions", "sessions", "세션"),
        "orders": ("transactions", "orders", "주문"),
        "revenue": ("purchaseRevenue", "revenue", "매출"),
        "cvr": ("cvr", "cvr", "구매 CVR"),
    }
    if qa["overall"]:
        for key, (src, kpi, label) in comparison_fields.items():
            current = n(k.get(kpi))
            metrics.append({"name": label, "key": key, "current": current,
                            "wow_pct": pct(current, previous.get(src)),
                            "yoy_pct": pct(current, yoy.get(src))})
        sess = n(k.get("sessions")) or 0
        orders = n(k.get("orders")) or 0
        prev_sess = n(previous.get("sessions"))
        prev_ord = n(previous.get("transactions"))
        prev_cvr = n(previous.get("cvr"))
        prev_revenue = n(previous.get("purchaseRevenue"))
        revenue = n(k.get("revenue")) or 0
        if prev_sess and prev_ord and prev_cvr:
            session_change = pct(sess, prev_sess)
            order_change = pct(orders, prev_ord)
            cvr_change = pct(n(k.get("cvr")), prev_cvr)
            if (session_change is not None and session_change >= -0.10 * 100
                    and order_change is not None and order_change <= -20
                    and cvr_change is not None and cvr_change <= -20):
                actions.append(action(
                    "CONVERSION", "유입 대비 구매 전환 급락",
                    f"전주 동요일 대비 세션 {fmt_pct(session_change)}, 주문 {fmt_pct(order_change)}, "
                    f"CVR {fmt_pct(cvr_change)}",
                    "랜딩·상품 구성·가격·재고·결제 경험 중 하나 이상이 영향을 줬을 가능성",
                    "채널별 신규/재방문 구매율, PDP→Cart→Checkout→Purchase, 상품별 재고·가격을 비교",
                    "이커머스/분석 담당"))
        if prev_revenue and prev_ord:
            order_change = pct(orders, prev_ord)
            revenue_change = pct(revenue, prev_revenue)
            current_aov = revenue / orders if orders > 0 else None
            prev_aov = prev_revenue / prev_ord if prev_ord > 0 else None
            aov_change = pct(current_aov, prev_aov)
            if aov_change is not None and aov_change <= -15:
                actions.append(action(
                    "AOV", "주문당 매출 감소",
                    f"객단가 전주 동요일 대비 {fmt_pct(aov_change)} "
                    f"(주문 {fmt_pct(order_change)}, 매출 {fmt_pct(revenue_change)})",
                    "저가 상품 판매 비중 또는 할인 구성 변화 가능성",
                    "주문단위 상품수·가격대·신상품/이월 비중 및 할인 적용률 확인",
                    "이커머스/MD", "P2"))
    if not qa["channel"]:
        actions.insert(0, action(
            "CHANNEL_QA", "채널별 성과 분석 보류",
            "채널 합계 또는 ETC 귀속 상태가 기준을 충족하지 않음",
            "GA4 집계 시점 지연, session 차원 중복, 귀속 분류 미확정 가능성",
            "동일 날짜 GA4 전체 sessions vs sessionDefaultChannelGroup 합계 비교, "
            "(not set)/(other) 상세, 조회 API 샘플링·쿼리 차원 확인",
            "데이터/GA4 담당", "P0"))
    if not qa["pdp"]:
        actions.insert(0, action(
            "PDP_QA", "PDP 일별 조회 데이터 복구",
            "최신일 카테고리 조회수가 누락 또는 0으로 기록됨",
            "GA4 BigQuery events_* 일별 export 지연, intraday 적재 또는 상품 카테고리 매핑 문제",
            "events_*와 events_intraday_*의 최근 3일 view_item 날짜별 건수 및 item_category 원본 대조",
            "데이터/BigQuery 담당", "P0"))
    if qa["channel"]:
        paid = next((r for r in obj.get("channel_snapshot", [])
                     if r.get("bucket") == "Paid Ad"), None)
        if paid and (n(paid.get("session_dod")) is not None):
            paid_sess = (n(paid.get("session_dod")) or 0) * 100
            paid_orders = (n(paid.get("orders_dod")) or 0) * 100
            if paid_sess > 10 and paid_orders < -20:
                actions.append(action(
                    "PAID_QUALITY", "Paid 유입 증가 대비 주문 감소",
                    f"Paid 세션 {fmt_pct(paid_sess)}, 주문 {fmt_pct(paid_orders)}",
                    "캠페인 타깃·소재·랜딩 미스매치 가능성",
                    "PMax/Meta/Criteo 등 캠페인 단위 CTR·세션·CVR와 유입 상품을 대조",
                    "퍼포먼스 대행사"))
    actions = sorted(actions, key=lambda x: {"P0": 0, "P1": 1, "P2": 2}.get(x["priority"], 3))
    return {"date": day.isoformat(), "status": qa["status"], "quality": qa,
            "metrics": metrics, "actions": actions[:7],
            "note": ("데이터 미검증 영역의 원인 설명은 가설이며 광고 예산 조정은 자동 수행하지 않습니다. "
                     "매체 ROAS는 신뢰 가능한 비용 데이터 부재 시 계산하지 않습니다.")}


def markdown(report: dict[str, Any]) -> str:
    qa = report["quality"]
    lines = [f"# CSK Marketing Intelligence — {report['date']}",
             f"검증 상태: {report['status']}", "",
             "## KPI (전주 동요일 / 전년 동요일)"]
    for row in report["metrics"]:
        v = row["current"]
        value = fmt(v, money=row["key"] == "revenue")
        if row["key"] == "cvr" and v is not None:
            value = f"{v * 100:.2f}%"
        lines.append(f"- {row['name']}: {value} | WoW {fmt_pct(row['wow_pct'])} | YoY {fmt_pct(row['yoy_pct'])}")
    lines.extend(["", "## 데이터 품질"])
    for x in qa["issues"]:
        lines.append(f"- [{x['level'].upper()}] {x['message']}")
    lines.extend(["", "## 원인 가설 및 업무 액션"])
    for x in report["actions"]:
        lines.extend([f"### {x['priority']} {x['title']}",
                      f"- 관측: {x['observation']}",
                      f"- 가설: {x['hypothesis']}",
                      f"- 검증: {x['validation']}",
                      f"- 담당: {x['owner']}", ""])
    lines.extend(["", report["note"], "",
                  "원본: reports/daily_digest/data/daily/" + report["date"] + ".json"])
    return "\n".join(lines) + "\n"


def webpage(report: dict[str, Any]) -> str:
    e = html.escape
    items = []
    for row in report["metrics"]:
        current = row["current"]
        value = f"{current * 100:.2f}%" if row["key"] == "cvr" and current is not None else fmt(
            current, money=row["key"] == "revenue")
        items.append("<article class='metric'><small>" + e(row["name"]) +
                     "</small><strong>" + e(value) + "</strong><span>WoW " +
                     e(fmt_pct(row["wow_pct"])) + " · YoY " +
                     e(fmt_pct(row["yoy_pct"])) + "</span></article>")
    qa_items = "".join("<li><b>" + e(x["level"].upper()) +
                       "</b> " + e(x["message"]) + "</li>"
                       for x in report["quality"]["issues"])
    action_items = "".join(
        "<article class='action'><small>" + e(x["priority"]) + " · " +
        e(x["owner"]) + "</small><h3>" + e(x["title"]) +
        "</h3><p><b>관측</b> " + e(x["observation"]) +
        "</p><p><b>가설</b> " + e(x["hypothesis"]) +
        "</p><p><b>검증 및 액션</b> " + e(x["validation"]) + "</p></article>"
        for x in report["actions"])
    return """<!doctype html><html lang='ko'><head><meta charset='utf-8'>
<meta name='viewport' content='width=device-width, initial-scale=1'><title>CSK Marketing Intelligence</title>
<style>body{margin:0;background:#f5f7fb;color:#17243b;font:15px/1.65 system-ui,-apple-system,sans-serif}
main{max-width:1080px;margin:auto;padding:36px 24px 72px}h1{font-size:30px;letter-spacing:-.8px}
h2{font-size:19px;margin:32px 0 14px}.muted,small{color:#53657e}
.status{display:inline-block;border-radius:30px;background:#e7effe;color:#15427b;padding:5px 13px;font-weight:700}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:14px}
.metric,.action,.quality{background:white;border:1px solid #e5eaf2;border-radius:16px;padding:20px}
.metric small,.metric strong,.metric span{display:block}.metric strong{font-size:26px;margin:8px 0}
.metric span{font-size:13px;color:#5c6980}.quality{border-left:4px solid #e2a347}
.quality li{margin:8px 0}.action{margin-bottom:12px}.action h3{margin:7px 0}
.action p{margin:7px 0}.foot{font-size:13px;color:#66758b;margin-top:24px}
</style></head><body><main><div class='muted'>COLUMBIA E-COMM · INTERNAL ANALYTICS</div>
<h1>Marketing Intelligence</h1><p>""" + e(report["date"]) + """ · <span class='status'>""" + e(report["status"]) + """</span></p>
<h2>핵심 지표</h2><section class='grid'>""" + "".join(items) + """</section>
<h2>데이터 검증</h2><section class='quality'><ul>""" + (qa_items or "<li>현재 점검 항목에서 경고가 없습니다.</li>") + """</ul></section>
<h2>개선 우선순위</h2>""" + (action_items or "<p>현재 검증 가능한 지표에서 명확한 조치 항목이 없습니다.</p>") + """
<p class='foot'>""" + e(report["note"]) + """ 원천 자료가 부족할 때 가설을 사실로 확정하지 않습니다.</p>
</main></body></html>"""


def build(day: dt.date, root: Path = BASE, out: Path = OUTPUT) -> dict[str, Any]:
    obj = source(day, root)
    qa = check(obj, day)
    report = analyze(obj, qa, day)
    daily = out / "daily"
    daily.mkdir(parents=True, exist_ok=True)
    json_text = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    md_text = markdown(report)
    html_text = webpage(report)
    for path, content in (
        (daily / (day.isoformat() + ".json"), json_text),
        (daily / (day.isoformat() + ".md"), md_text),
        (daily / (day.isoformat() + ".html"), html_text),
        (out / "latest.json", json_text),
        (out / "latest.md", md_text),
        (out / "index.html", html_text),
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    print(f"[MARKETING INTELLIGENCE] {day}: {report['status']}; "
          f"{len(qa['issues'])} QA issues; {len(report['actions'])} actions")
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=(dt.datetime.now(KST).date() -
                        dt.timedelta(days=1)).isoformat())
    parser.add_argument("--source-root", default=str(BASE))
    parser.add_argument("--out-root", default=str(OUTPUT))
    args = parser.parse_args()
    build(dt.date.fromisoformat(args.date),
          Path(args.source_root), Path(args.out_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
