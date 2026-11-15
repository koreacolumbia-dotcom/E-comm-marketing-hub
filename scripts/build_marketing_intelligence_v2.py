#!/usr/bin/env python3
"""CSK Marketing Intelligence 2.0: quality-gated, aggregate-only action system.

Public repo safety:
* Never load customer-level CRM or email/phone/user identifiers.
* All metrics come from pre-existing aggregate reports. Missing is not zero.
* Never send messages, edit campaigns, or close tasks without explicit evidence.
"""
from __future__ import annotations

import argparse
import datetime as dt
import html
import json
import math
import re
from pathlib import Path
from typing import Any

try:
    from scripts.build_marketing_intelligence import analyze, check, source, n, pct
except ModuleNotFoundError:
    from build_marketing_intelligence import analyze, check, source, n, pct

KST = dt.timezone(dt.timedelta(hours=9))
ROOT = Path("reports/marketing_intelligence_v2")
PDP = Path("reports/pdp_opportunity/data.json")
META = Path("reports/pdp_opportunity/meta.json")
OVERRIDES = Path("config/marketing_intelligence_actions.json")
CRM_AGGREGATE = Path("reports/private_aggregate_crm/segments.json")
ALLOWED_STATES = {"OPEN", "IN_PROGRESS", "DONE", "BLOCKED"}
SENSITIVE_KEYS = {"email", "phone", "mobile", "userid", "user_id", "member_id",
                  "customer_id", "address", "birthdate", "name", "user_pseudo_id"}


def load_json(path: Path) -> dict[str, Any] | None:
    try:
        result = json.loads(path.read_text(encoding="utf-8"))
        return result if isinstance(result, dict) else None
    except (OSError, ValueError, UnicodeError):
        return None


def date_of(value: Any) -> dt.date | None:
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def safe_code(value: Any) -> str:
    return re.sub(r"[^A-Z0-9_-]", "", str(value or "").upper())[:40]


def fmt(x: Any) -> str:
    v = n(x)
    return "미확인" if v is None else f"{v:,.0f}"


def v_pct(x: Any) -> str:
    v = n(x)
    return "미확인" if v is None else f"{v:+.1f}%"


def quality_product(path: Path, meta_path: Path, report_day: dt.date,
                    now_day: dt.date) -> dict[str, Any]:
    raw, meta = load_json(path), load_json(meta_path)
    status = {"status": "waiting", "source": str(path), "data_start": None,
              "data_end": None, "notes": [], "candidates": [], "row_count": 0}
    if raw is None or meta is None:
        status["notes"].append("PDP 30일 데이터 또는 메타정보 미연결")
        return status
    end = date_of(raw.get("data_end"))
    meta_end = date_of(meta.get("data_end"))
    status["data_start"] = raw.get("data_start")
    status["data_end"] = raw.get("data_end")
    if end is None or end != meta_end or end > now_day or (report_day - end).days > 3:
        status["notes"].append("PDP 기간·메타정보 또는 신선도 불일치 — 상품 판단 보류")
        return status
    rows = raw.get("products")
    if not isinstance(rows, list) or not rows or len(rows) != len(raw.get("rows", rows)):
        status["notes"].append("PDP 상품 데이터 원본 구조 불일치")
        return status
    status["row_count"] = len(rows)
    if end != report_day:
        status["notes"].append(
            f"PDP 상품 분석 종료일 {end}은 Daily Digest 기준일 {report_day}과 다름. "
            "별도 30일 관측으로만 사용하며 당일 매출 감소의 원인으로 단정하지 않음"
        )
    candidates = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        sku = safe_code(item.get("product_code"))
        sessions = n(item.get("pdp_sessions"))
        carts = n(item.get("add_to_cart_sessions"))
        purchases = n(item.get("purchase_sessions"))
        if not sku or sessions is None or carts is None or purchases is None:
            continue
        if sessions < 100 or not (0 <= carts <= sessions and 0 <= purchases <= sessions):
            continue
        atc_rate = 100 * carts / sessions
        purchase_rate = 100 * purchases / sessions
        if atc_rate >= 8 and purchase_rate >= 1.5:
            continue
        name = str(item.get("product_name") or sku)[:90]
        category = str(item.get("category") or "미분류")[:45]
        score = n(item.get("opportunity_score"))
        if score is None or not (0 <= score <= 100):
            score = 0
        candidates.append({
            "code": sku, "name": name, "category": category,
            "sessions": int(sessions), "cart_sessions": int(carts),
            "purchase_sessions": int(purchases), "atc_rate": round(atc_rate, 2),
            "purchase_rate": round(purchase_rate, 2), "priority_score": round(score, 1),
            "source_period": f"{raw.get('data_start')} ~ {raw.get('data_end')}",
            "reason": "PDP→Cart 전환 낮음" if atc_rate < 8 else "PDP→구매 전환 낮음",
            "validation": "동기간 재고·사이즈 품절·가격·프로모션과 유입 매체를 확인해야 원인 판단 가능"
        })
    candidates.sort(key=lambda x: (-x["priority_score"], -x["sessions"], x["code"]))
    status.update(status="ready", candidates=candidates[:8])
    status["notes"].append(
        "상품 퍼널 지표는 30일 GA4 상품별 세션 기준. 재고/광고 원인 및 매출 하락 기여도를 직접 증명하지 않음")
    return status


def agency(obj: dict[str, Any] | None, qa: dict[str, Any]) -> dict[str, Any]:
    result = {"status": "blocked", "requests": [], "flagged_media": [],
              "spend_status": "미연결: 광고비·ROAS 자동 판정 보류"}
    base_requests = [
        {"platform": "PMax", "question": "최근 7일 캠페인별 비용·전환가치·주문·상품군·검색어/브랜드 유입 비중을 확인하고, Demand Gen/GDN과 별도 집계되는지 점검해 주세요.",
         "scope": "집계 방식 및 트래픽 품질 검증 요청", "is_finding": False},
        {"platform": "Criteo", "question": "리타겟팅 대상·빈도·클릭 후 구매·뷰스루 기여와 GA4 세션/주문 차이를 구분해 공유해 주세요.",
         "scope": "귀속·중복 전환 검증 요청", "is_finding": False},
        {"platform": "Meta", "question": "신규회원 확보 캠페인의 가입 이후 장바구니·구매 전환을 캠페인/타깃별로 구분해 점검해 주세요.",
         "scope": "가입 캠페인 후속 전환 검증 요청", "is_finding": False},
    ]
    result["requests"] = base_requests
    if obj is None or not qa.get("paid"):
        result["requests"].insert(0, {
            "platform": "GA4 공통", "question": "ETC/(not set)/(other) 비중과 Paid Ad/전체 세션 합계부터 복구해 주세요. 검증 전에는 매체별 ROAS로 예산 조정하지 않겠습니다.",
            "scope": "데이터 검증 실패", "is_finding": True
        })
        return result
    result["status"] = "traffic_only"
    for row in obj.get("paid_detail") or []:
        if not isinstance(row, dict) or row.get("sub_channel") == "Total":
            continue
        sessions, orders = n(row.get("sessions")), n(row.get("orders"))
        session_delta, order_delta = n(row.get("session_dod")), n(row.get("orders_dod"))
        if (sessions is not None and sessions >= 100 and orders is not None
                and session_delta is not None and order_delta is not None
                and session_delta > .1 and order_delta < -.2):
            result["flagged_media"].append({
                "media": str(row.get("sub_channel") or "unknown")[:45],
                "sessions": int(sessions), "orders": int(orders),
                "session_change_pct": round(session_delta * 100, 1),
                "order_change_pct": round(order_delta * 100, 1),
                "note": "세션 증가·주문 감소 동시 관측, 캠페인/유입 품질 검증 필요"
            })
    return result


CRM_DEFINITIONS = [
    {"segment": "가입 후 30일 미구매", "rule": "가입일이 30일 이상 경과했으며 유효 주문 0건",
     "channel": "가입 웰컴/상품 탐색 메시지", "guardrail": "발송 동의·최근 발송 빈도 확인"},
    {"segment": "첫 구매 후 60일 재구매 없음", "rule": "유효 구매 정확히 1회, 구매 이후 60일 이상",
     "channel": "연관 상품·재구매 리마인드", "guardrail": "반품/취소·기존 캠페인 제외 조건 확인"},
    {"segment": "최근 관심 상품 반복 조회·미구매", "rule": "최근 30일 동일 상품 PDP 반복 조회 및 장바구니, 해당 상품 구매 없음",
     "channel": "상품 혜택/재고 알림 후보", "guardrail": "GA4와 회원 ID 결합 적법성·동의 확인"},
]


def crm_readiness(path: Path, day: dt.date) -> dict[str, Any]:
    # The public dashboard intentionally publishes no member-level data nor cohort counts.
    data = load_json(path)
    result = {"status": "not_connected", "definitions": CRM_DEFINITIONS,
              "note": "고객 단위 데이터와 세그먼트 인원은 공개 저장소로 출력하지 않음"}
    if data is None:
        return result
    rows = data.get("segments")
    as_of = date_of(data.get("as_of"))
    if not isinstance(rows, list) or not rows or as_of is None or (day - as_of).days > 7:
        result["status"] = "stale_or_invalid"
        return result
    for row in rows:
        if not isinstance(row, dict) or any(k.lower() in SENSITIVE_KEYS for k in row):
            result["status"] = "rejected_sensitive_schema"
            return result
        if n(row.get("member_count")) is None or n(row.get("member_count")) < 30:
            result["status"] = "suppressed_small_cohort"
            return result
    result["status"] = "aggregate_available_private"
    result["note"] = "합산 그룹 적재 확인. 인원 및 개별 식별자는 공개 보고서에 출력하지 않음"
    return result


def new_action(code: str, priority: str, domain: str, title: str,
               observation: str, hypothesis: str, validation: str,
               owner: str) -> dict[str, Any]:
    return {"id": code, "priority": priority, "domain": domain,
            "title": title, "observation": observation,
            "hypothesis": hypothesis, "validation": validation, "owner": owner}


def actions_from_sources(v1: dict[str, Any], product: dict[str, Any],
                         media: dict[str, Any], crm: dict[str, Any]) -> list[dict[str, Any]]:
    result = []
    for row in v1.get("actions", []):
        if not isinstance(row, dict) or not row.get("code"):
            continue
        key = str(row["code"])
        result.append(new_action(key, row.get("priority", "P2"), "DATA" if key.endswith("_QA") else "COMMERCE",
                                 row.get("title", key), row.get("observation", ""),
                                 row.get("hypothesis", ""), row.get("validation", ""),
                                 row.get("owner", "이커머스")))
    for item in product["candidates"][:4]:
        result.append(new_action(
            "PRODUCT_" + item["code"], "P1", "PRODUCT",
            f"{item['name']} 상세페이지 개선 검토",
            f"지난 30일 PDP 세션 {item['sessions']:,}, 장바구니 전환 {item['atc_rate']:.1f}%, "
            f"구매 전환 {item['purchase_rate']:.1f}% (기준 {item['source_period']})",
            "상세페이지 구성·재고·판매가·유입 캠페인 영향 가능성 — 인과관계 미검증",
            item["validation"], "이커머스/MD"))
    if media["status"] == "blocked":
        result.append(new_action(
            "AGENCY_ATTRIBUTION", "P0", "PAID", "대행사 채널 귀속 데이터 우선 점검",
            "Paid 분석에 필요한 GA4 채널 귀속 합계·ETC 품질 게이트 미통과",
            "집계 차원 및 GA4 귀속 지연 가능성",
            "GA4 세션 정의·Paid/Organic 중복·PMax vs GDN 구분을 재검증",
            "데이터/퍼포먼스 대행사"))
    if crm["status"] != "aggregate_available_private":
        result.append(new_action(
            "CRM_SEGMENT_SOURCE", "P2", "CRM", "CRM 세그먼트 집계 연동",
            f"상태: {crm['status']}, 신뢰 가능한 타깃 그룹 수치 없음",
            "구매·회원 데이터의 개인정보 보호형 집계 미연결",
            "비공개 환경에서 회원별 구매 이력을 집계하고 30인 이상 그룹 통계만 접근 제한 저장. "
            "캠페인 발송은 승인 이후 별도 실행",
            "CRM/데이터 담당"))
    dedup = {}
    for x in result:
        dedup[x["id"]] = x
    priority = {"P0": 0, "P1": 1, "P2": 2}
    return sorted(dedup.values(), key=lambda x: (priority.get(x["priority"], 3), x["id"]))


def make_queue(current: list[dict[str, Any]], previous: dict[str, Any] | None,
               overrides: dict[str, Any] | None, day: dt.date) -> list[dict[str, Any]]:
    old = {x.get("id"): x for x in (previous or {}).get("actions", [])
           if isinstance(x, dict) and x.get("id")}
    settings = (overrides or {}).get("overrides") or {}
    settings = settings if isinstance(settings, dict) else {}
    seen = set()
    queue = []
    for x in current:
        item = dict(x)
        prior = old.get(item["id"]) or {}
        patch = settings.get(item["id"]) or {}
        if not isinstance(patch, dict):
            patch = {}
        wanted = patch.get("status", prior.get("status", "OPEN"))
        item["status"] = wanted if wanted in ALLOWED_STATES else "OPEN"
        item["first_seen"] = prior.get("first_seen", day.isoformat())
        item["last_seen"] = day.isoformat()
        item["observed_today"] = True
        item["observations"] = int(prior.get("observations", 0)) + (
            0 if prior.get("last_seen") == day.isoformat() else 1)
        item["note"] = str(patch.get("note") or prior.get("note") or "")[:250]
        if isinstance(patch.get("owner"), str) and patch["owner"].strip():
            item["owner"] = patch["owner"].strip()[:40]
        queue.append(item)
        seen.add(item["id"])
    for key, prior in old.items():
        if key not in seen and prior.get("status") != "DONE":
            item = dict(prior)
            item["observed_today"] = False
            item["note"] = item.get("note", "") or "최근 집계에서 재관측되지 않음. 미발견은 문제 해결의 증거가 아닙니다."
            patch = settings.get(key) or {}
            if isinstance(patch, dict) and patch.get("status") in ALLOWED_STATES:
                item["status"] = patch["status"]
            queue.append(item)
    priority = {"P0": 0, "P1": 1, "P2": 2}
    return sorted(queue, key=lambda x: (x.get("status") == "DONE",
                                        not x.get("observed_today", True),
                                        priority.get(x.get("priority"), 3),
                                        x.get("id", "")))[:100]


def compile_report(day: dt.date, now_day: dt.date,
                   daily_root: Path, pdp_path: Path, meta_path: Path,
                   crm_path: Path, prior_state: dict[str, Any] | None,
                   overrides: dict[str, Any] | None) -> dict[str, Any]:
    obj = source(day, daily_root)
    qa = check(obj, day)
    v1 = analyze(obj, qa, day)
    products = quality_product(pdp_path, meta_path, day, now_day)
    media = agency(obj, qa)
    crm = crm_readiness(crm_path, day)
    todos = actions_from_sources(v1, products, media, crm)
    queue = make_queue(todos, prior_state, overrides, day)
    return {
        "version": "2.0", "report_date": day.isoformat(),
        "generated_at_kst": dt.datetime.now(KST).isoformat(timespec="seconds"),
        "status": v1["status"], "quality": qa,
        "metrics": v1.get("metrics", []),
        "product": products, "agency": media, "crm": crm,
        "actions": queue,
        "action_stats": {
            "open": sum(x["status"] == "OPEN" for x in queue),
            "in_progress": sum(x["status"] == "IN_PROGRESS" for x in queue),
            "done": sum(x["status"] == "DONE" for x in queue),
            "blocked": sum(x["status"] == "BLOCKED" for x in queue),
        },
        "governance": {
            "source_systems": [str(daily_root / (day.isoformat() + ".json")),
                               str(pdp_path), str(meta_path)],
            "consent_required": "대외 이메일 전송·광고비 조정·CRM 발송은 자동화하지 않음",
            "inference_policy": "인과 설명은 가설, 기간이 다른 상품 데이터는 당일 증감 원인에 사용하지 않음",
            "privacy": "고객 식별자·개별 구매 이력·CRM 세그먼트 인원 공개 금지"
        }
    }


def render_markdown(r: dict[str, Any]) -> str:
    lines = [f"# CSK Marketing Intelligence 2.0 — {r['report_date']}", "",
             f"데이터 상태: **{r['status']}**", "", "## 주요 지표"]
    for x in r["metrics"]:
        lines.append(f"- {x['name']}: {fmt(x['current'])} | 전주 {v_pct(x['wow_pct'])} | 전년 {v_pct(x['yoy_pct'])}")
    lines.extend(["", "## QA"])
    for x in r["quality"]["issues"]:
        lines.append(f"- {x['code']} ({x['level']}): {x['message']}")
    lines.extend(["", f"## PDP 상품 분석 ({r['product']['status']})"])
    for x in r["product"]["notes"]:
        lines.append(f"- {x}")
    for x in r["product"]["candidates"]:
        lines.append(f"- {x['code']} {x['name']}: {x['sessions']:,} 세션 · ATC {x['atc_rate']:.1f}% · 구매 {x['purchase_rate']:.1f}%")
    lines.extend(["", f"## 광고 대행사 점검 ({r['agency']['status']})"])
    for x in r["agency"]["requests"]:
        lines.append(f"- {x['platform']}: {x['question']}")
    lines.extend(["", f"## CRM ({r['crm']['status']})"])
    for x in r["crm"]["definitions"]:
        lines.append(f"- {x['segment']}: {x['rule']}")
    lines.extend(["", "## 미해결 액션"])
    for x in r["actions"]:
        lines.append(f"- [{x['status']}] {x['priority']} · {x['domain']} · {x['title']} "
                     f"(최근 관측 {x['last_seen']}; 담당 {x['owner']})")
    lines.extend(["", "원천과 제약: " + r["governance"]["inference_policy"]])
    return "\n".join(lines) + "\n"


def render_html(r: dict[str, Any]) -> str:
    e = lambda s: html.escape(str(s), quote=True)
    badge = lambda x: "<span class='badge'>" + e(x) + "</span>"
    kpi = "".join("<article class='kpi'><div class='meta'>" + e(x["name"]) + "</div><strong>" +
                  (f"{(x['current'] or 0)*100:.2f}%" if x["key"] == "cvr" and x["current"] is not None else fmt(x["current"])) +
                  "</strong><div class='delta'>WoW " + e(v_pct(x["wow_pct"])) + " · YoY " +
                  e(v_pct(x["yoy_pct"])) + "</div></article>" for x in r["metrics"])
    issues = "".join("<li><span class='sev " + e(x["level"]) + "'>" +
                     e(x["level"]) + "</span>" + e(x["message"]) + "</li>"
                     for x in r["quality"]["issues"])
    prods = "".join("<tr><td><b>" + e(p["name"]) + "</b><div class='muted'>" + e(p["code"]) +
                    "</div></td><td>" + e(fmt(p["sessions"])) + "</td><td>" +
                    e(f"{p['atc_rate']:.1f}%") + "</td><td>" +
                    e(f"{p['purchase_rate']:.1f}%") + "</td><td>" +
                    e(p["reason"]) + "</td></tr>" for p in r["product"]["candidates"])
    agency = "".join("<article class='task'><div class='meta'>" + badge(x["platform"]) +
                     badge(x["scope"]) + "</div><p>" + e(x["question"]) + "</p></article>"
                     for x in r["agency"]["requests"])
    definitions = "".join("<article class='task'><h3>" + e(x["segment"]) + "</h3><p>" +
                          e(x["rule"]) + "</p><p class='muted'>권장: " +
                          e(x["channel"]) + " · " + e(x["guardrail"]) +
                          "</p></article>" for x in r["crm"]["definitions"])
    tasks = "".join("<article class='task action' data-domain='" + e(x["domain"]) +
                    "' data-status='" + e(x["status"]) + "'><div class='meta'>" +
                    badge(x["priority"]) + badge(x["status"]) + badge(x["domain"]) +
                    " · " + e(x["owner"]) + "</div><h3>" + e(x["title"]) +
                    "</h3><p>" + e(x["observation"]) +
                    "</p><details><summary>가설 · 검증 방법</summary><p>" + e(x["hypothesis"]) +
                    "</p><p>" + e(x["validation"]) + "</p></details><div class='muted small'>" +
                    e(x["id"]) + " · 최초 " + e(x["first_seen"]) + " · 최근 " +
                    e(x["last_seen"]) + (" · 오늘 관측" if x["observed_today"] else " · 과거 미해결") +
                    "</div></article>" for x in r["actions"])
    product_notes = " ".join(r["product"]["notes"])
    return ("""<!doctype html><html lang='ko'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>
<meta name='robots' content='noindex,nofollow'><title>CSK Marketing Intelligence 2.0</title><style>
:root{--bg:#f5f7fb;--ink:#16233a;--muted:#60718b;--line:#e4e9f3;--brand:#1458a6}
*{box-sizing:border-box}body{margin:0;background:var(--bg);font:14px/1.65 system-ui,-apple-system,'Noto Sans KR',sans-serif;color:var(--ink)}
main{max-width:1260px;margin:auto;padding:34px 22px 74px}.top{display:flex;justify-content:space-between;gap:18px;align-items:center;flex-wrap:wrap}
h1{font-size:30px;letter-spacing:-.9px;margin:5px 0 0}h2{font-size:20px;margin:0 0 16px}h3{font-size:15px;margin:8px 0}
.meta{font-size:11px;color:var(--muted);letter-spacing:.02em}.muted{color:var(--muted)}.small{font-size:11px}
.hero{background:#172b47;color:#f4f7fc;border-radius:20px;padding:24px;margin:22px 0 24px;box-shadow:0 6px 20px #23375a13}
.hero .muted{color:#d4def0}.status{padding:7px 12px;background:#e6eefc;color:#1a4c85;border-radius:99px;font-weight:750}
.kpis{display:grid;grid-template-columns:repeat(4,1fr);gap:11px}.kpi,.panel{background:white;border:1px solid var(--line);border-radius:16px;padding:18px}
.kpi strong{display:block;font-size:25px;margin-top:5px}.delta{font-size:12px;color:#64748b}
.panels{display:grid;grid-template-columns:1fr 1fr;gap:18px;margin:18px 0}.section{margin:20px 0}
.badge{display:inline-block;padding:3px 9px;margin-right:5px;border-radius:99px;background:#edf3fb;color:#295b90;font-weight:750;white-space:nowrap}
ul{padding-left:20px}li{padding:5px 0}.sev{font-weight:800;color:#bd4c19;margin-right:7px;text-transform:uppercase}
table{border-collapse:collapse;width:100%;font-size:12px}td,th{border-bottom:1px solid var(--line);padding:11px 7px;text-align:left;vertical-align:top}
th{color:#60718b;font-weight:700}.task{border:1px solid var(--line);border-radius:12px;padding:14px;margin:9px 0;background:#fff}
.task p{margin:6px 0}details{margin:8px 0;background:#f7f9fd;border-radius:8px;padding:8px 11px}
summary{cursor:pointer;font-weight:650;color:#29568b}.toolbar{display:flex;gap:9px;flex-wrap:wrap;margin-bottom:12px}
select,input{border:1px solid var(--line);border-radius:9px;background:white;padding:10px;min-height:39px}
input{flex:1;min-width:200px}.grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:9px}
footer{border-top:1px solid var(--line);margin-top:26px;padding-top:15px;font-size:12px;color:#687793}
@media(max-width:780px){main{padding:18px 12px 40px}h1{font-size:23px}.kpis{grid-template-columns:repeat(2,1fr)}.kpi strong{font-size:19px}.panels,.grid{grid-template-columns:1fr}.hero{padding:17px}}
</style></head><body><main><header class='top'><div><div class='meta'>COLUMBIA · E-COMM OPERATIONS</div>
<h1>Marketing Intelligence <span style='color:#477fce'>2.0</span></h1>
<div class='muted'>검증 기반 성과 분석 · 상품 개선 · 대행사 점검 · CRM · 액션 트래킹</div></div>
<span class='status'>""" + e(r["status"]) + """ · """ + e(r["report_date"]) + """</span></header>
<section class='hero'><div class='meta' style='color:#d1e2ff'>EXECUTION BOARD</div>
<div style='font-size:21px;font-weight:800;margin:6px 0'>결정 가능한 데이터만 실행 과제로 전환합니다</div>
<div class='muted'>미검증 채널·PDP·CRM 데이터는 추측 대신 확인 과제로 처리하며, 완료 상태는 자동으로 만들지 않습니다.</div>
</section><section class='kpis'>""" + kpi + """</section>
<section class='panels'><div class='panel'><h2>데이터 품질</h2><ul>""" + (issues or "<li>경고 없음</li>") +
"""</ul></div><div class='panel'><h2>액션 현황</h2><p>OPEN <b>""" + str(r["action_stats"]["open"]) +
"""</b> · 진행 중 <b>""" + str(r["action_stats"]["in_progress"]) +
"""</b> · DONE <b>""" + str(r["action_stats"]["done"]) +
"""</b> · BLOCKED <b>""" + str(r["action_stats"]["blocked"]) +
"""</b></p><p class='muted'>기록된 액션은 원본에서 사라져도 자동 완료되지 않습니다. config에서 담당자와 상태를 수동 승인해야 합니다.</p></div></section>
<section class='panel section'><h2>상품 PDP 개선 후보</h2><p class='muted'>분석 기간 """ +
e(r["product"]["data_start"] or "미확인") + " ~ " + e(r["product"]["data_end"] or "미확인") +
" · 상태 " + e(r["product"]["status"]) + " · " + e(product_notes) +
"""</p><div style='overflow:auto'><table><thead><tr><th>상품</th><th>PDP 세션</th><th>Cart 전환</th><th>구매 전환</th><th>관측</th></tr></thead><tbody>""" +
(prods or "<tr><td colspan='5'>사용 가능한 상품 분석 데이터가 없습니다.</td></tr>") +
"""</tbody></table></div></section>
<section class='panels'><div class='panel'><h2>광고 대행사 점검</h2><div class='muted'>""" +
e(r["agency"]["status"]) + " · " + e(r["agency"]["spend_status"]) + """</div>""" + agency +
"""</div><div class='panel'><h2>CRM 고객 세그먼트</h2><div class='muted'>""" + e(r["crm"]["status"]) +
""" · 고객 식별자와 그룹 인원 비공개</div>""" + definitions + """</div></section>
<section class='panel section'><h2>개선 액션 관리</h2><div class='toolbar'>
<input id='search' aria-label='액션 검색' placeholder='문제·상품·담당 부서 검색'>
<select id='domain' aria-label='영역'><option value=''>모든 영역</option>
<option value='DATA'>데이터</option><option value='COMMERCE'>매출</option>
<option value='PRODUCT'>상품</option><option value='PAID'>광고</option><option value='CRM'>CRM</option></select>
<select id='state' aria-label='상태'><option value=''>모든 상태</option><option value='OPEN'>OPEN</option>
<option value='IN_PROGRESS'>진행 중</option><option value='BLOCKED'>BLOCKED</option><option value='DONE'>DONE</option></select></div>
<div class='grid' id='action-grid'>""" + tasks + """</div>
<p id='empty' class='muted' hidden>조건에 맞는 액션이 없습니다.</p></section>
<footer>기준일 """ + e(r["report_date"]) + """ · 집계 파일 기반 보고서 · 실제 캠페인 변경/발송 없음 · """
+e(r["governance"]["privacy"])+"""<p>상품별 퍼널은 기간·귀속 기준이 Daily Digest와 다를 수 있습니다. 증감은 인과관계를 뜻하지 않습니다.</p></footer>
</main><script>
const $=id=>document.getElementById(id);const rows=Array.from(document.querySelectorAll('.action'));
function apply(){const q=$('search').value.trim().toLowerCase(),d=$('domain').value,s=$('state').value;
let count=0;for(const row of rows){const shown=(!d||row.dataset.domain===d)&&(!s||row.dataset.status===s)&&(!q||row.textContent.toLowerCase().includes(q));row.hidden=!shown;if(shown)count++;}
$('empty').hidden=count!==0}
['search','domain','state'].forEach(id=>$(id).addEventListener(id==='search'?'input':'change',apply));
</script></body></html>""")


def write_report(report: dict[str, Any], out: Path = ROOT) -> None:
    out.mkdir(parents=True, exist_ok=True)
    serial = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    files = {
        out / "latest.json": serial, out / "latest.md": render_markdown(report),
        out / "index.html": render_html(report),
        out / "state.json": json.dumps({
            "version": "2.0", "as_of": report["report_date"],
            "actions": report["actions"]}, ensure_ascii=False, indent=2) + "\n"
    }
    for path, data in files.items():
        path.write_text(data, encoding="utf-8")
    print(f"[INTELLIGENCE 2.0] {report['report_date']}: {report['status']} "
          f"product={report['product']['status']} media={report['agency']['status']} "
          f"crm={report['crm']['status']} actions={len(report['actions'])}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=(dt.datetime.now(KST).date() -
                                       dt.timedelta(days=1)).isoformat())
    ap.add_argument("--out", default=str(ROOT))
    ap.add_argument("--daily-root", default="reports/daily_digest/data/daily")
    ap.add_argument("--pdp", default=str(PDP))
    ap.add_argument("--pdp-meta", default=str(META))
    ap.add_argument("--crm", default=str(CRM_AGGREGATE))
    ap.add_argument("--overrides", default=str(OVERRIDES))
    args = ap.parse_args()
    day = dt.date.fromisoformat(args.date)
    output = Path(args.out)
    now = dt.datetime.now(KST).date()
    report = compile_report(
        day, now, Path(args.daily_root), Path(args.pdp), Path(args.pdp_meta),
        Path(args.crm), load_json(output / "state.json"), load_json(Path(args.overrides))
    )
    write_report(report, output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
