#!/usr/bin/env python3
"""CSK Marketing Operations: calendar context, product triage and decision briefs.

Public-repo safety:
- source provenance and dates are mandatory; missing == unknown, never zero
- planned is not confirmed, attributed CRM revenue is not incremental revenue
- incomplete stock/paid/CRM gates prevent sales or ROAS recommendations
- changes to ad budgets, emails, MSSQL, BigQuery and campaign delivery: none
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
    from scripts.csk_marketing_extensions import all_signals
except ModuleNotFoundError:
    from csk_marketing_extensions import all_signals

ROOT = Path("reports/marketing_operations")
V2 = Path("reports/marketing_intelligence_v2/latest.json")
DAILY = Path("reports/daily_digest/data/daily")
CAL2025 = Path("reports/daily_digest/data/marketing_calendar_2025.json")
OWNED2025 = Path("reports/daily_digest/data/owned_message_2025.json")
CAL2026 = Path("config/marketing_calendar_2026.json")
PRIVATE_INCREMENTAL = Path("private_data/csk_signals/incrementality_aggregate.json")
KST = dt.timezone(dt.timedelta(hours=9))


def read_json(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except (ValueError, OSError, UnicodeError):
        return None


def parse_day(value: Any) -> dt.date | None:
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except (ValueError, TypeError):
        return None


def finite(v: Any) -> float | None:
    if isinstance(v, bool):
        return None
    try:
        result = float(v)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def slim(value: Any, max_chars: int = 130) -> str:
    text = re.sub(r"[\r\n\x00-\x1f]+", " ", str(value or "")).strip()
    text = re.sub(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", "[EMAIL REDACTED]", text)
    text = re.sub(r"(?<![0-9])01[016789][- ]?[0-9]{3,4}[- ]?[0-9]{4}(?![0-9])",
                  "[PHONE REDACTED]", text)
    return text[:max_chars]


def date_span(first: dt.date, last: dt.date, date: dt.date, end: dt.date | None) -> bool:
    return date <= last and (end or date) >= first


def calendar_context(report_day: dt.date, current_daily: dict[str, Any] | None,
                     cal25: Path = CAL2025, owned25: Path = OWNED2025,
                     cal26: Path = CAL2026) -> dict[str, Any]:
    """Historical weekday-aligned window from Daily Digest's actual YoY anchor."""
    result: dict[str, Any] = {
        "status": "reference_missing", "current_status": "not_connected",
        "yoy_window": None, "current_window": None, "current": [],
        "last_year_confirmed": [], "last_year_planned": [],
        "interpretation": "일정/발송과 매출의 시간적 일치는 인과 효과나 순증분 매출의 증거가 아님"
    }
    week_start = report_day - dt.timedelta(days=6)
    result["current_window"] = f"{week_start} ~ {report_day}"
    current_source = read_json(cal26)
    if current_source is not None and isinstance(current_source.get("entries"), list):
        found = []
        for entry in current_source["entries"]:
            if not isinstance(entry, dict):
                continue
            start = parse_day(entry.get("date"))
            end = parse_day(entry.get("end_date"))
            if (start and date_span(week_start, report_day, start, end)
                    and entry.get("status") in ("confirmed", "planned")
                    and (entry.get("status") != "confirmed" or
                         str(entry.get("source") or "").strip())):
                found.append({
                    "date": start.isoformat(), "status": entry["status"],
                    "kind": slim(entry.get("kind"), 24),
                    "title": slim(entry.get("title")),
                    "source": slim(entry.get("source"), 110),
                    "year": 2026
                })
        result["current"] = sorted(found, key=lambda x: x["date"])
        if result["current"]:
            result["current_status"] = "records_present"
        # An empty 2026 plan does not indicate zero campaigns.
    meta = (current_daily or {}).get("meta") or {}
    anchor = parse_day(meta.get("yoy_start"))
    if (current_daily or {}).get("end_date") != report_day.isoformat() or anchor is None:
        return result
    past_start = anchor - dt.timedelta(days=6)
    result["yoy_window"] = f"{past_start} ~ {anchor}"
    result["status"] = "available"
    events = []
    for origin, path in (("marketing_calendar_2025", cal25),
                         ("owned_message_2025", owned25)):
        source = read_json(path)
        if source is None or not isinstance(source.get("entries"), list):
            continue
        for entry in source["entries"]:
            if not isinstance(entry, dict):
                continue
            start = parse_day(entry.get("date"))
            end = parse_day(entry.get("end_date"))
            if start is None or not date_span(past_start, anchor, start, end):
                continue
            status = entry.get("status")
            if status not in ("confirmed", "planned"):
                continue
            if origin == "owned_message_2025" and status == "confirmed" and (
                    finite(entry.get("sent_count")) or 0) <= 0:
                # A planned record without confirmed delivery is not a sent message.
                status = "planned"
            # Do not add CRM message_revenue or ROAS from overlapping sources:
            # attribution may differ and events can duplicate.
            events.append({
                "date": start.isoformat(), "end_date": end.isoformat() if end else None,
                "kind": slim(entry.get("kind") or entry.get("channel") or "event", 30),
                "channel": slim(entry.get("channel"), 16) if entry.get("channel") else None,
                "title": slim(entry.get("title")),
                "detail": slim(entry.get("detail"), 180) if entry.get("detail") else None,
                "source": slim(entry.get("source") or source.get("source_file") or origin, 110),
                "origin": origin, "status": status, "year": 2025,
                "sent_count": round(finite(entry.get("sent_count"))) if (
                    origin == "owned_message_2025" and status == "confirmed" and
                    (finite(entry.get("sent_count")) or 0) > 0) else None,
                "sent_confirmed": (finite(entry.get("sent_count")) or 0) > 0
                if origin == "owned_message_2025" else None
            })
    dedup = {(x["origin"], x["date"], x["title"]): x for x in events}
    ordered = sorted(dedup.values(), key=lambda x: (x["date"], x["title"], x["origin"]))
    result["last_year_confirmed"] = [x for x in ordered if x["status"] == "confirmed"][:35]
    result["last_year_planned"] = [x for x in ordered if x["status"] == "planned"][:25]
    result["reference_total"] = len(ordered)
    if not read_json(cal25) and not read_json(owned25):
        result["status"] = "reference_missing"
    return result


def product_triage(v2: dict[str, Any]) -> dict[str, Any]:
    product = v2.get("product") or {}
    if product.get("status") != "ready":
        return {"status": "blocked", "items": [],
                "reason": "상품별 PDP 데이터 신선도/기간 검증 미통과"}
    warehouse = ((v2.get("integrations") or {}).get("warehouse") or {})
    source_ok = warehouse.get("order_mart") == "ready"
    inventory = warehouse.get("inventory")
    items = []
    for row in product.get("candidates", [])[:15]:
        if not isinstance(row, dict):
            continue
        sku = str(row.get("code") or "")
        sessions, atc, purchases = [finite(row.get(k)) for k in
                                    ("sessions", "atc_rate", "purchase_sessions")]
        if not re.fullmatch(r"[A-Z0-9_-]{5,40}", sku) or sessions is None or atc is None or purchases is None:
            continue
        if row.get("measurement_watch") or (sessions >= 500 and purchases == 0):
            stage = "MEASUREMENT_QA"
            reason = "구매 이벤트와 어드민 상품코드 일치 여부 검증 필요"
            rank = 0
        elif sessions >= 100 and atc < 8:
            stage = "CRO_HYPOTHESIS"
            reason = "PDP→Cart 이탈 후보. 유입 매체·재고·상세페이지 요인 검증 전"
            rank = 1
        else:
            stage = "OBSERVE"
            reason = "단독 근거로는 광고/프로모션 증액 우선순위를 정할 수 없음"
            rank = 2
        items.append({
            "code": sku, "name": slim(row.get("name"), 95),
            "sessions": int(sessions), "cart_rate_pct": round(atc, 2),
            "purchase_sessions": int(purchases), "state": stage,
            "reason": reason, "verification": slim(row.get("validation"), 220),
            "period": slim(row.get("source_period"), 50),
            "stock_evidence": "unknown" if inventory != "ready" else "aggregate_only",
            "order_reconciliation": "ready" if source_ok else "blocked",
            "margin_evidence": "not_connected",
            "revenue_opportunity_krw": None,
            "rank": rank
        })
    items.sort(key=lambda x: (x["rank"], -x["sessions"], x["code"]))
    for x in items:
        x.pop("rank", None)
    return {
        "status": "diagnostics_only" if not source_ok else "source_ready_not_reconciled",
        "items": items[:10],
        "reason": "재고·마진·상품별 어드민 주문의 완전한 비교가 없어 잠재 매출액이나 광고비 증액은 산정하지 않음"
    }


def weekly_yoy(day: dt.date, current_week: dict[str, Any],
               root: Path = DAILY) -> dict[str, Any]:
    """Weekday-aligned last-year metrics, exact same BigQuery semantics as TY.

    Never mix historical GA4 API sessions with current BigQuery session keys.
    Only use per-day matched YoY records from the current-date daily cache,
    whose YoY anchor is exactly 364 days ago. No partial-week extrapolation.
    """
    ly_end = day - dt.timedelta(days=364)
    result: dict[str, Any] = {
        "status": "unavailable",
        "period": f"{ly_end-dt.timedelta(days=6)} ~ {ly_end}",
        "alignment": "same_weekday_364_days",
        "source": None,
        "reference": {"status": "incomplete", "total": None},
        "yoy_pct": {},
        "daily_comparison": [],
        "missing_dates": [],
        "note": "전년 동요일 7일의 동일 GA4 BigQuery 지표 검증 후에만 YoY를 표시합니다."
    }
    if not current_week.get("total"):
        return result
    totals = {"sessions": 0., "orders": 0., "revenue": 0.}
    seen_sources: set[str] = set()
    daily_rows: list[dict[str, Any]] = []
    for i in range(6, -1, -1):
        date = day - dt.timedelta(days=i)
        reference_date = date - dt.timedelta(days=364)
        j = read_json(root / f"{date.isoformat()}.json")
        overall = (j or {}).get("overall") or {}
        ty = overall.get("current") or {}
        ly = overall.get("yoy") or {}
        kpis = (j or {}).get("kpis") or {}
        yoy_anchor = (j or {}).get("yoy") or {}
        source = ty.get("source")
        current_check = {
            "sessions": finite(ty.get("sessions")),
            "orders": finite(ty.get("transactions")),
            "revenue": finite(ty.get("purchaseRevenue"))
        }
        previous = {
            "sessions": finite(ly.get("sessions")),
            "orders": finite(ly.get("transactions")),
            "revenue": finite(ly.get("purchaseRevenue"))
        }
        if (j is None or j.get("end_date") != date.isoformat()
            or yoy_anchor.get("start") != reference_date.isoformat()
            or yoy_anchor.get("end") != reference_date.isoformat()
            or source != "ga4_bigquery_session_key_v1" or ly.get("source") != source
            or any(value is None for value in current_check.values())
            or any(value is None for value in previous.values())
            or any(previous[k] < 0 for k in previous)
            or previous["sessions"] <= 0 or previous["orders"] > previous["sessions"]
            or any(finite(kpis.get(k)) is None for k in totals)
            or any(abs(current_check[k]-float(kpis[k])) > (1 if k == "revenue" else 0) for k in totals)):
            result["missing_dates"].append(reference_date.isoformat())
            continue
        seen_sources.add(source)
        daily_rows.append({
            "date": date.isoformat(), "ly_date": reference_date.isoformat(),
            "current": {key: round(val, 2) for key, val in current_check.items()},
            "last_year": {key: round(val, 2) for key, val in previous.items()},
            "revenue_yoy_pct": (
                round((current_check["revenue"]/previous["revenue"] - 1)*100, 2)
                if previous["revenue"] > 0 else None)
        })
        for key in totals:
            totals[key] += previous[key]
    if result["missing_dates"] or seen_sources != {"ga4_bigquery_session_key_v1"}:
        return result
    ref = {key: round(val, 2) for key, val in totals.items()}
    ref["cvr"] = totals["orders"] / totals["sessions"]
    cur = current_week["total"]
    metrics = {"sessions": cur["sessions"], "orders": cur["orders"],
               "revenue": cur["revenue"], "cvr": cur["cvr"],
               "aov": cur["revenue"] / cur["orders"] if cur["orders"] > 0 else None}
    ref["aov"] = ref["revenue"] / ref["orders"] if ref["orders"] > 0 else None
    yoy_pct = {}
    for key, value in metrics.items():
        base = ref.get(key)
        yoy_pct[key] = round((value/base-1)*100, 2) if value is not None and base is not None and base > 0 else None
    result.update(
        status="ready", source="ga4_bigquery_session_key_v1",
        reference={"status": "complete", "total": ref},
        yoy_pct=yoy_pct, daily_comparison=daily_rows,
        note="전년 동일 요일 7일, 364일 오프셋. 동일 BigQuery 세션 정의·상품매출 합계가 아닌 구매매출 기준. 인과 관계는 해석하지 않습니다."
    )
    return result


def weekly_kpis(day: dt.date, root: Path = DAILY) -> dict[str, Any]:
    """Full 7d windows only. Prevent incomplete-day zero filling."""
    def period(last: dt.date) -> dict[str, Any]:
        totals = {"sessions": 0., "orders": 0., "revenue": 0.}
        missing = []
        for i in range(6, -1, -1):
            date = last - dt.timedelta(days=i)
            j = read_json(root / f"{date.isoformat()}.json")
            k = (j or {}).get("kpis") or {}
            if (j is None or j.get("end_date") != date.isoformat()
                    or j.get("mode") not in ("daily", None)):
                missing.append(date.isoformat())
                continue
            vals = {key: finite(k.get(key)) for key in totals}
            if (any(val is None for val in vals.values())
                    or vals["sessions"] <= 0 or vals["orders"] < 0
                    or vals["revenue"] < 0 or vals["orders"] > vals["sessions"]):
                missing.append(date.isoformat())
                continue
            for key, value in vals.items():
                totals[key] += value
        return {"status": "complete" if not missing else "incomplete",
                "missing_dates": missing,
                "total": ({**{key: round(val, 2) for key, val in totals.items()},
                           "cvr": totals["orders"]/totals["sessions"]}
                          if not missing else None)}
    this = period(day)
    last = period(day - dt.timedelta(days=7))
    yoy = weekly_yoy(day, this, root)
    changes = {}
    if this["total"] and last["total"]:
        for key in ("sessions", "orders", "revenue", "cvr"):
            a, b = this["total"][key], last["total"][key]
            changes[key] = round((a / b - 1)*100, 2) if b > 0 else None
    return {
        "period": f"{day-dt.timedelta(days=6)} ~ {day}",
        "comparison_period": f"{day-dt.timedelta(days=13)} ~ {day-dt.timedelta(days=7)}",
        "status": "ready" if this["total"] and last["total"] else "incomplete",
        "current": this, "previous": last, "wow_pct": changes,
        "yoy": yoy,
        "caveat": "Daily KPI 합산 기준. 채널/CRM/PDP 품질 게이트와 별개이며 광고·CRM 기여도 아님"
    }


def incrementality(day: dt.date, path: Path = PRIVATE_INCREMENTAL) -> dict[str, Any]:
    """Aggregate randomized experiment diagnostics; minimum groups 100, never PII."""
    data = read_json(path)
    out = {"status": "awaiting_private_aggregates", "results": [],
           "note": "A/B 홀드아웃 원천 미연결. 순증분 매출·유의성 수치 생성하지 않음"}
    if data is None or not isinstance(data.get("experiments"), list):
        return out
    for e in data["experiments"][:20]:
        if not isinstance(e, dict) or parse_day(e.get("end_date")) is None:
            continue
        if parse_day(e["end_date"]) > day:
            continue
        a, b = e.get("treatment"), e.get("control")
        if not isinstance(a, dict) or not isinstance(b, dict):
            continue
        ta, tb = finite(a.get("eligible")), finite(b.get("eligible"))
        oa, ob = finite(a.get("orders")), finite(b.get("orders"))
        ra, rb = finite(a.get("revenue")), finite(b.get("revenue"))
        if (None in (ta,tb,oa,ob,ra,rb) or ta < 100 or tb < 100 or oa < 0
                or ob < 0 or oa > ta or ob > tb or ra < 0 or rb < 0):
            continue
        conv_t, conv_c = oa/ta, ob/tb
        diff = conv_t-conv_c
        se = math.sqrt(conv_t*(1-conv_t)/ta + conv_c*(1-conv_c)/tb)
        verified = (e.get("randomization_verified") is True
                    and e.get("assignment_audit_passed") is True)
        out["results"].append({
            "id": slim(e.get("id"), 60), "title": slim(e.get("title"), 120),
            "status": "randomization_verified_aggregate" if verified else "descriptive_only",
            "cvr_difference_pp": round(diff*100, 3),
            "cvr_95pct_approx_interval_pp": [round((diff - 1.96*se)*100, 3),
                                             round((diff + 1.96*se)*100, 3)],
            "revenue_per_eligible_difference_krw": round(ra/ta-rb/tb, 2),
            "statistical_caution": ("정규근사 구간; 표본/실험 간섭·추적·반품 기준 재검증 필요"
                                    if min(oa,ob,ta-oa,tb-ob) >= 10 else
                                    "소표본 사건수: 정규근사 신뢰구간 부정확 가능"),
            "is_causal_proof": False
        })
    out["status"] = "descriptive_readout" if out["results"] else "awaiting_valid_private_aggregates"
    out["note"] = "비공개 합계만 사용. 리프트는 추정이며 무작위 배정·추적 완전성 확인 전 인과 확정 금지"
    return out


def recovery_plan(v2: dict[str, Any]) -> list[dict[str, Any]]:
    integrations = v2.get("integrations") or {}
    paid = integrations.get("paid_cost") or {}
    warehouse = integrations.get("warehouse") or {}
    quality = v2.get("quality") or {}
    plans: list[dict[str, Any]] = []
    if paid.get("status") != "fresh_unreconciled":
        plans.append({
            "code": "PAID_COST_FRESHNESS", "priority": "P0", "owner": "매체 담당/대행사",
            "observed": "광고비 상태 "+slim(paid.get("status"), 35)+
                        ", 최종 비용일 "+slim(paid.get("latest_date") or "미확인", 16),
            "next_step": "광고 매체별 원천 export/API 인증·적재 스케줄 복구, 일별 합계·통화/세금/캠페인 코드 대조",
            "unsafe_operation": "광고비 파일을 임의 보간하거나 이 값으로 ROAS/예산 변경 금지"
        })
    if warehouse.get("order_mart") != "ready" or warehouse.get("crm") != "ready":
        plans.append({
            "code": "WAREHOUSE_ETL_FRESHNESS", "priority": "P0", "owner": "데이터 엔지니어",
            "observed": "주문 마트 "+slim(warehouse.get("order_mart"), 25)+
                        ", CRM "+slim(warehouse.get("crm"), 25),
            "next_step": "원천/대상 수정시각·ETL 로그·삭제/덮어쓰기 모드 검토 후 증분 동기화 및 어드민 주문건수 대조",
            "unsafe_operation": "원본 백업·행수/기간/ID 검증 없는 WRITE_TRUNCATE 자동 실행 금지"
        })
    if warehouse.get("inventory") not in ("ready", "ready_current_only"):
        plans.append({
            "code": "MSSQL_STOCK_ACCESS", "priority": "P0", "owner": "인프라/DBA",
            "observed": "MSSQL 재고 접근 상태 "+slim(warehouse.get("inventory"), 30),
            "next_step": "GitHub Actions 러너에서 허용 IP/VPN/DB 포트 1433과 읽기 전용 연결 진단",
            "unsafe_operation": "방화벽 임의 개방·관리자 자격 증명 공개·DB 원본 변경 금지"
        })
    if quality.get("channel") is False:
        plans.append({
            "code": "CHANNEL_QA", "priority": "P0", "owner": "GA4/BigQuery 담당",
            "observed": "채널 합산과 전체 세션 불일치, ETC 귀속 오류",
            "next_step": "동일 세션 키/기간/귀속 규칙으로 재집계 후 ETC·Paid·Organic 중복 제거 확인",
            "unsafe_operation": "채널 데이터 정상화 전 매체별 원인·성과 단정 금지"
        })
    if quality.get("pdp") is False:
        plans.append({
            "code": "PDP_QA", "priority": "P0", "owner": "GA4/BigQuery 담당",
            "observed": "PDP 최신일 수집/상품 이벤트 집계 검증 실패",
            "next_step": "view_item와 purchase.item_id/상품 SKU 매핑, 최신일 지연과 중복 이벤트 확인",
            "unsafe_operation": "조회수 0을 실제 관심도 0으로 해석 금지"
        })
    return plans


def build(report_day: dt.date, v2: dict[str, Any], daily_root: Path = DAILY,
          cal25: Path = CAL2025, owned25: Path = OWNED2025,
          cal26: Path = CAL2026, incremental: Path = PRIVATE_INCREMENTAL) -> dict[str, Any]:
    if v2.get("report_date") != report_day.isoformat():
        raise ValueError("Stale or mismatched v2 report; do not publish a cross-day join")
    daily = read_json(daily_root / f"{report_day.isoformat()}.json")
    if daily is None or daily.get("end_date") != report_day.isoformat():
        raise ValueError("Missing authoritative Daily Digest for report date")
    calendar = calendar_context(report_day, daily, cal25, owned25, cal26)
    weekly = weekly_kpis(report_day, daily_root)
    triage = product_triage(v2)
    recovery = recovery_plan(v2)
    experiments = incrementality(report_day, incremental)
    expansions = all_signals(report_day)
    open_items = [x for x in v2.get("actions", []) if isinstance(x, dict)
                  and x.get("status") not in ("DONE",)]
    summary = {
        "headline": "원천 복구 우선" if recovery else "검증된 지표 기반 실행 관리",
        "business_status": v2.get("status"),
        "priority_blockers": [x["code"] for x in recovery][:5],
        "current_7d_status": weekly["status"],
        "actions_open": len(open_items),
        "data_readiness": {
            "paid_cost": (v2.get("integrations") or {}).get("paid_cost", {}).get("status"),
            "order_mart": (v2.get("integrations") or {}).get("warehouse", {}).get("order_mart"),
            "crm": (v2.get("integrations") or {}).get("warehouse", {}).get("crm"),
            "inventory": (v2.get("integrations") or {}).get("warehouse", {}).get("inventory")
        },
        "decision": ("복구 및 원천 정합성 확인이 최우선. Paid 예산 재배분이나 "
                     "상품 예상 매출액은 데이터 검증 전 실행하지 않음."
                     if recovery else
                     "상품/CRM 개선안을 실험으로 등록하고 검증된 관측 효과를 모니터링.")
    }
    return {
        "version": "3.0", "report_date": report_day.isoformat(),
        "generated_at_kst": dt.datetime.now(KST).isoformat(timespec="seconds"),
        "status": v2.get("status"), "executive": summary, "weekly": weekly,
        "calendar": calendar, "products": triage, "incrementality": experiments,
        "extensions": expansions,
        "recovery": recovery,
        "actions": [{
            "id": slim(x.get("id"), 70),
            "title": slim(x.get("title"), 130),
            "owner": slim(x.get("owner"), 65),
            "priority": slim(x.get("priority"), 8),
            "status": slim(x.get("status"), 30)
        } for x in open_items[:60]],
        "governance": {
            "source_date_aligned": True,
            "no_automatic_mutations": "no ETL, ad budget, CRM send or campaign delivery",
            "privacy": "customer IDs and member-level rows never published",
            "warnings": ["2025 confirmed activity is not evidence of incremental sales",
                         "no current 2026 calendar records does not mean zero actual activity",
                         "current stock is not historical inventory",
                         "campaign control-group incremental results require protected source"]
        }
    }


def fmt_krw(v: Any) -> str:
    x = finite(v)
    return "미확인" if x is None else f"₩{x:,.0f}"


def fmt_pct(v: Any) -> str:
    x = finite(v)
    return "미확인" if x is None else f"{x:+,.1f}%"


def render_md(r: dict[str, Any]) -> str:
    o = [f"# CSK Marketing Operations 3.0 — {r['report_date']}", "",
         f"**보고 상태: {r['status']}** · {r['executive']['headline']}", "",
         "## Executive Summary", r["executive"]["decision"], "",
         "### 원천 신뢰도"]
    for name, value in r["executive"]["data_readiness"].items():
        o.append(f"- {name}: {value or '미확인'}")
    w = r["weekly"]
    o.extend(["", "## 7일 KPI (전주 완결 기간 비교)", f"상태: **{w['status']}**",
              f"기간: {w['period']} / 비교: {w['comparison_period']}"])
    if w["status"] == "ready":
        c = w["current"]["total"]
        o.extend([f"- 세션 {c['sessions']:,.0f} · WoW {fmt_pct(w['wow_pct'].get('sessions'))}",
                  f"- 주문 {c['orders']:,.0f} · WoW {fmt_pct(w['wow_pct'].get('orders'))}",
                  f"- 매출 {fmt_krw(c['revenue'])} · WoW {fmt_pct(w['wow_pct'].get('revenue'))}",
                  f"- 구매 CVR {c['cvr']*100:.2f}% · 상대 WoW {fmt_pct(w['wow_pct'].get('cvr'))}"])
    else:
        o.append("- 7일 중 일별 KPI 누락/이상 존재. 기간 합계를 만들지 않음")
        o.append("- 현재 기간 누락일: "+", ".join(w["current"]["missing_dates"]))
        o.append("- 비교 기간 누락일: "+", ".join(w["previous"]["missing_dates"]))
    c = r["calendar"]
    o.extend(["", "## 마케팅 캘린더 (기간과 출처 확인)", f"올해 {c['current_window']}: {c['current_status']}",
              f"전년 동요일 대응 기간 {c['yoy_window'] or '미확인'}: {c['status']}",
              "### 전년 확인 활동"])
    for x in c["last_year_confirmed"][:12]:
        o.append(f"- {x['date']} [{x['kind']}] {x['title']} · {x['source']}")
    if not c["last_year_confirmed"]:
        o.append("- 확인 가능한 전년 일정 없음(활동 부재의 증거 아님)")
    if c["last_year_planned"]:
        o.append("### 전년 계획만 확인된 활동 (실행 미확인)")
        for x in c["last_year_planned"][:8]:
            o.append(f"- {x['date']} {x['title']}")
    o.extend(["", "## 상품/PDP 개선 검토"])
    for x in r["products"]["items"][:8]:
        o.append(f"- [{x['state']}] {x['name']} ({x['code']}) · PDP {x['sessions']:,} "
                 f"· Cart {x['cart_rate_pct']:.2f}% · {x['reason']}")
    extras = r.get("extensions") or {}
    competitors = extras.get("competitors") or {}
    o.extend(["", "## 경쟁사 가격 참고 (동일 SKU 비교 불가)",
              f"수집 상태: {competitors.get('status', '미연결')}",
              competitors.get("note", "")])
    for brand in competitors.get("brands", [])[:10]:
        o.append(f"- {brand['brand']}: 수집상품 {brand['products']:,}, "
                 f"표기 평균가 {fmt_krw(brand['avg_price_krw'])}, "
                 f"할인 상품 비율 {brand['listed_sale_share_pct']}% "
                 f"(수집일 {brand['checkpoint_date']}, {brand['status']})")
    for key, title in (("launches", "신상품 7·14·30일"), ("creatives", "광고 소재 피로도"),
                       ("creator_crew", "크리에이터 크루 릴스/피드")):
        x = extras.get(key) or {}
        o.extend(["", "## " + title, "상태: " + x.get("status", "미연결"), x.get("note", "")])
        if key == "launches":
            for item in x.get("items", [])[:10]:
                o.append("- "+item["sku"]+" (출시 "+item["launch_date"]+"): "+
                         ", ".join(t+": "+w["status"] for t,w in item["windows"].items()))
        elif key == "creatives":
            for item in x.get("flags", [])[:10]:
                o.append(f"- {item['platform']}/{item['creative_id']}: CTR 상대 변화 "
                         f"{item['ctr_change_pct']}% (원인 추정, 검증 필요)")
        else:
            for item in x.get("groups", [])[:10]:
                o.append(f"- {item['mission']} · {item['format']}: 조회 {item['views']:,}, "
                         f"참여/조회 {item['engagements_per_view_pct']}% (실제 제출 집계)")
            if x.get("estimated_groups"):
                o.append(f"- 별도 추정 그룹 {x['estimated_groups']}개는 실제 성과 합계에서 제외")
    o.extend(["", "## 데이터 복구·담당 부서 액션"])
    for x in r["recovery"]:
        o.append(f"- **{x['code']}** [{x['owner']}]: {x['observed']}. {x['next_step']}")
    exp = r["incrementality"]
    o.extend(["", "## CRM/캠페인 증분효과", f"상태: {exp['status']}", exp["note"]])
    for x in exp["results"][:8]:
        o.append(f"- {x['title']}: 구매 CVR 차이 {x['cvr_difference_pp']:+.2f}%p "
                 f"(정규근사 구간 {x['cvr_95pct_approx_interval_pp']}); 순증분 인과확정 아님")
    o.extend(["", "## 데이터 제약"])
    for msg in r["governance"]["warnings"]:
        o.append("- "+msg)
    return "\n".join(o)+"\n"


def render_html(r: dict[str, Any]) -> str:
    e = lambda s: html.escape(str(s), quote=True)
    w, c = r["weekly"], r["calendar"]
    kpi = w["current"].get("total")
    cards = [("세션", f"{kpi['sessions']:,.0f}" if kpi else "미검증", fmt_pct(w["wow_pct"].get("sessions"))),
             ("주문", f"{kpi['orders']:,.0f}" if kpi else "미검증", fmt_pct(w["wow_pct"].get("orders"))),
             ("매출", fmt_krw(kpi["revenue"]) if kpi else "미검증", fmt_pct(w["wow_pct"].get("revenue"))),
             ("구매 CVR", f"{kpi['cvr']*100:.2f}%" if kpi else "미검증", fmt_pct(w["wow_pct"].get("cvr")))]
    kpi_html = "".join("<article class='kpi'><div class='small'>"+e(name)+"</div><strong>"+
                       e(value)+"</strong><div class='small'>전주 "+e(delta)+"</div></article>"
                       for name, value, delta in cards)
    def activity(entries):
        return "".join("<div class='item'><div class='small'>"+e(x["date"])+" · "+e(x["kind"])+
                       " · "+e(x.get("status", ""))+"</div><b>"+e(x["title"])+
                       "</b><div class='small'>"+e(x["source"])+"</div></div>"
                       for x in entries)
    product_html = "".join("<tr><td><b>"+e(x["name"])+"</b><div class='small'>"+e(x["code"])+
                           "</div></td><td>"+e(f"{x['sessions']:,}")+"</td><td>"+
                           e(f"{x['cart_rate_pct']:.1f}%")+"</td><td><span class='flag'>"+
                           e(x["state"])+"</span></td><td>"+e(x["reason"])+"</td></tr>"
                           for x in r["products"]["items"])
    recovery_html = "".join("<div class='item'><div class='small'>"+
                            e(x["code"])+" · "+e(x["owner"])+"</div><b>"+e(x["observed"])+
                            "</b><p>"+e(x["next_step"])+"</p></div>"
                            for x in r["recovery"])
    issues_html = "".join("<div class='item issue' data-title='"+e(x["title"]+" "+x["owner"])+
                          "'><div class='small'>"+e(x["priority"])+" · "+e(x["status"])+
                          " · "+e(x["owner"])+"</div><b>"+e(x["title"])+
                          "</b><div class='small'>"+e(x["id"])+"</div></div>"
                          for x in r["actions"])
    exp = r["incrementality"]
    exp_html = "".join("<div class='item'><b>"+e(x["title"])+"</b><p>"+
                       e("CVR 차이 "+str(x["cvr_difference_pp"])+"%p")+
                       " · 순증분 인과확정 아님</p></div>" for x in exp["results"]) or (
                       "<p class='small'>비공개 실험 집계 미연결. 수치를 추정하지 않습니다.</p>")
    extras = r.get("extensions") or {}
    competitive = extras.get("competitors") or {}
    comp_table = "".join("<tr><td><b>"+e(x["brand"])+"</b></td><td>"+
                         e(f"{x['products']:,}")+"</td><td>"+
                         e(fmt_krw(x["avg_price_krw"]))+"</td><td>"+
                         e(f"{x['listed_sale_share_pct']:.1f}%")+"</td><td>"+
                         e(x["checkpoint_date"])+"</td></tr>"
                         for x in competitive.get("brands", [])[:10])
    extension_html = (
        "<section class='panel'><h2>경쟁사 가격·프로모션 참고</h2><p class='small'>"+
        e(competitive.get("status","미연결")) + " · " +
        e(competitive.get("note","")) +
        "</p><div class='scroll'><table><thead><tr><th>브랜드</th><th>수집 상품</th>"+
        "<th>상품 평균가</th><th>세일 품목 비율</th><th>수집일</th></tr></thead><tbody>"+
        (comp_table or "<tr><td colspan='5'>검증된 경쟁사 소스 미연결</td></tr>")+
        "</tbody></table></div></section><section class='cols'>"+
        "".join("<div class='panel'><h2>"+e(title)+"</h2><p><b>"+
                e((extras.get(key) or {}).get("status","미연결"))+
                "</b></p><p class='small'>"+e((extras.get(key) or {}).get("note",""))+
                "</p></div>" for key,title in
                (("launches","신상품 런칭 7·14·30일"),
                 ("creatives","광고 소재 피로도"),
                 ("creator_crew","크리에이터 크루 성과")))+
        "</section>"
    )
    return ("""<!doctype html><html lang='ko'><head><meta charset='utf-8'>
<meta name='viewport' content='width=device-width,initial-scale=1'>
<meta name='robots' content='noindex,nofollow'>
<title>CSK Marketing Operations 3.0</title><style>
*{box-sizing:border-box}:root{--bg:#f5f7fb;--card:#fff;--ink:#1c2a42;--mut:#72809a;--bd:#e1e7f0;--navy:#1c3254}
body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.55 system-ui,-apple-system,'Noto Sans KR',sans-serif}
main{max-width:1200px;padding:34px 20px 70px;margin:auto}
h1{font-size:28px;margin:5px 0 8px;letter-spacing:-.6px}h2{font-size:18px;margin:0 0 13px}
h3{font-size:15px;margin:0 0 8px}.small{font-size:12px;color:var(--mut)}
header{display:flex;justify-content:space-between;gap:15px;flex-wrap:wrap;align-items:center}
.pill{background:#e5edfa;color:#265385;border-radius:50px;padding:8px 14px;font-weight:700}
.hero{background:var(--navy);color:#f6f8fc;margin:22px 0;border-radius:19px;padding:24px}
.hero p{color:#d1dfef;margin:8px 0 0}.hero b{font-size:20px}
.kpis{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}.kpi,.panel{background:var(--card);border:1px solid var(--bd);border-radius:14px;padding:17px}
.kpi strong{font-size:22px;display:block;margin:8px 0}.stack{display:grid;gap:16px;margin:19px 0}
.cols{display:grid;grid-template-columns:1fr 1fr;gap:16px}
.item{border-top:1px solid var(--bd);padding:12px 0}.item:first-of-type{border-top:0}
.item p{margin:7px 0;color:var(--mut)}.flag{padding:3px 8px;border-radius:8px;background:#e9eef8;color:#244e82;white-space:nowrap}
table{border-collapse:collapse;width:100%;font-size:12px}td,th{padding:11px 8px;border-bottom:1px solid var(--bd);text-align:left;vertical-align:top}
th{color:var(--mut)}.scroll{overflow-x:auto}.panel p{margin-top:6px}.warn{color:#c26c24}
input{border:1px solid var(--bd);border-radius:9px;padding:10px;max-width:100%;min-width:230px}
footer{font-size:12px;color:var(--mut);margin-top:26px}
@media(max-width:760px){main{padding:16px 12px 45px}.kpis{grid-template-columns:1fr 1fr}.cols{grid-template-columns:1fr}h1{font-size:23px}.hero{padding:18px}.kpi strong{font-size:18px}}
</style>
<link id='csk-saas-shared-theme' rel='stylesheet' href='../../assets/csk-saas-unified.css?v=20261122-layout-v2'>
<script defer data-csk-route="ops3" src='../../assets/csk-saas-standalone.js?v=20261122-layout-v2'></script>
</head><body><main><header><div><div class='small'>CSK · E-COMMERCE DECISION SUPPORT</div>
<h1>Marketing Operations 3.0</h1><div class='small'>Calendar · Product · Recovery · Experiments · Action Board</div>
</div><span class='pill'>""" + e(r["status"]) + " · "+ e(r["report_date"]) + """</span></header>
<section class='hero'><div class='small' style='color:#b3cde8'>EXECUTIVE DECISION</div>
<b>""" + e(r["executive"]["headline"]) + "</b><p>"+e(r["executive"]["decision"])+
"""</p></section><section class='kpis'>""" + kpi_html + """</section>
<section class='stack'><section class='cols'><div class='panel'><h2>원천 복구 우선순위</h2>""" +
(recovery_html or "<p>현재 등록된 원천 복구 차단 항목 없음</p>")+
"""</div><div class='panel'><h2>전년 마케팅 캘린더</h2><p class='small'>전년 동요일 기준 """+
e(c["yoy_window"] or "미연결")+""" · 올해 기록: """+e(c["current_status"])+
"""</p><h3>확인된 전년 활동</h3>""" +
(activity(c["last_year_confirmed"][:8]) or "<p class='small'>근거 있는 전년 일정 없음</p>")+
("""<h3>계획만 확인(실행 미확인)</h3>""" + activity(c["last_year_planned"][:4])
 if c["last_year_planned"] else "") +
"""</div></section><section class='panel'><h2>상품별 우선 점검</h2><p class='small'>""" +
e(r["products"]["reason"])+"""</p><div class='scroll'><table><thead><tr>
<th>상품명</th><th>PDP</th><th>Cart 비율</th><th>단계</th><th>확인할 내용</th>
</tr></thead><tbody>""" + product_html + """</tbody></table></div></section>
<section class='cols'><div class='panel'><h2>액션 담당 현황</h2>
<input id='search' placeholder='문제·상품·담당 조직 검색' aria-label='액션 검색'>
<div id='issues'>""" + issues_html + """</div></div><div class='panel'>
<h2>개선 효과 / 홀드아웃</h2><p class='small'>""" + e(exp["status"])+""" · """ + e(exp["note"])+
"""</p>""" + exp_html + """</div></section>""" + extension_html + """</section>
<footer>기준일 """+e(r["report_date"])+""" · 고객 개인정보 비공개 · 원천 자료 부재=미확인 · 활동 이력은 인과효과가 아닙니다
<p>외부 매체 비용, CRM 발송, 상품 할인 및 ETL 쓰기 자동화는 수행하지 않습니다.</p></footer>
<script>const search=document.getElementById('search');search.addEventListener('input',()=>{const q=search.value.toLowerCase();
for(const x of document.querySelectorAll('.issue'))x.hidden=!x.dataset.title.toLowerCase().includes(q)})</script>
</main></body></html>""")


def write_report(r: dict[str, Any], root: Path = ROOT) -> None:
    root.mkdir(parents=True, exist_ok=True)
    values = {
        root / "latest.json": json.dumps(r, ensure_ascii=False, indent=2)+"\n",
        root / "latest.md": render_md(r),
        root / "index.html": render_html(r)
    }
    for path, value in values.items():
        path.write_text(value, encoding="utf-8")
    print(f"[CSK Marketing Operations] {r['report_date']} {r['status']} "
          f"recoveries={len(r['recovery'])} products={len(r['products']['items'])} "
          f"activities_ly={r['calendar'].get('reference_total',0)} "
          f"weekly={r['weekly']['status']} experiments={r['incrementality']['status']}")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--date", default=None)
    p.add_argument("--v2", default=str(V2))
    p.add_argument("--out", default=str(ROOT))
    args = p.parse_args()
    v2 = read_json(Path(args.v2))
    if v2 is None:
        raise SystemExit("V2 source missing; refusing to publish")
    day = parse_day(args.date) if args.date else parse_day(v2.get("report_date"))
    if day is None:
        raise SystemExit("No valid report date")
    report = build(day, v2)
    write_report(report, Path(args.out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
