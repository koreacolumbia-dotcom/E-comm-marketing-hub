#!/usr/bin/env python3
"""Evidence-led CSK diagnosis: metric decomposition, hypotheses and crawl gaps.

Only reads public, aggregated reports. No customer records, causal scores,
automated marketing spend changes or invented unobserved measurements.
"""
from __future__ import annotations

import datetime as dt
import json
import math
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
INPUTS = {
    "operations": "reports/marketing_operations/latest.json",
    "intelligence": "reports/daily_intelligence/latest.json",
    "content": "reports/competitor_content/strategy-summary.json",
    "quality": "reports/competitor_intelligence/latest-quality.json",
    "calendar": "reports/competitor_sale_calendar/latest.json",
}
TARGET = ROOT / "reports/decision_health/diagnoses.json"


def read(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError, UnicodeError):
        return {}


def num(value: Any) -> float | None:
    if type(value) not in (int, float):
        return None
    return float(value) if math.isfinite(value) and value >= 0 else None


def pct(a: float | None, b: float | None) -> float | None:
    return round((a / b - 1) * 100, 2) if a is not None and b is not None and b > 0 else None


def day(value: Any) -> dt.date | None:
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except (ValueError, TypeError):
        return None


def describe_error(error: str) -> tuple[str, str]:
    e = error.casefold()
    if "robots" in e or "disallowed" in e or "policy_denied" in e:
        return "robots_policy", "robots 정책 확인 후 허용된 공식 공개 경로만 사용"
    if "429" in e or "rate limit" in e:
        return "rate_limited", "해당 출처 요청 중단·재시도 간격 확대"
    if "403" in e or "access denied" in e or "captcha" in e:
        return "access_restricted", "접근 제한 존중, 대체 공식 공개 자료 확인"
    if "certificate" in e or "ssl" in e:
        return "certificate_error", "TLS·도메인 설정 점검, 신뢰 가능한 공식 호스트만 확인"
    if "timeout" in e or "timed out" in e:
        return "timeout", "허용 경로의 타임아웃·재방문 우선순위 점검"
    if "execution context" in e or "navigation" in e:
        return "navigation_race", "동적 페이지 이동 완료 후 DOM 추출 재시도"
    return "other", "오류 로그 검토 및 공식 경로 검증"


def crawl_gaps(content: dict, quality: dict, limit: int = 8) -> list[dict]:
    q_by_brand = {str(b.get("brand")): b for b in quality.get("brands", []) if isinstance(b, dict)}
    rows = []
    for b in content.get("brands", []):
        if not isinstance(b, dict):
            continue
        brand = str(b.get("brand", "")).strip()
        if not brand:
            continue
        sample = int(num(b.get("retained_unique_count")) or 0)
        plan = b.get("coverage_plan") or {}
        target = int(num(plan.get("sample_target")) or 30)
        pending = int(num(b.get("pending_urls")) or 0)
        coverage = b.get("url_coverage") or {}
        observed_pct = num(coverage.get("observed_pct"))
        known = int(num(coverage.get("known_urls")) or 0)
        qrow = q_by_brand.get(brand, {})
        error_rows = qrow.get("errors") or b.get("errors") or []
        types = {}
        for err in error_rows:
            if not isinstance(err, dict):
                continue
            kind, _ = describe_error(str(err.get("error") or ""))
            types[kind] = types.get(kind, 0) + 1
        blockers = [x for x in ("robots_policy", "rate_limited", "access_restricted", "certificate_error") if types.get(x)]
        gap = max(0, target - sample)
        if not gap and not pending and (observed_pct is None or observed_pct >= 80) and not types:
            continue
        # This is work-list priority, NOT actual market size or publication volume.
        score = (min(gap, 200) * 2 + min(pending, 200) +
                 (40 if observed_pct is not None and observed_pct < 30 else 0) +
                 (20 if sample == 0 else 0))
        if brand in {"TNF", "K2", "DISCOVERY", "EIDER", "PATAGONIA", "MONTBELL", "MILLET"}:
            score += 12
        rows.append({
            "brand": brand, "priority_score": score,
            "observed_30d": sample, "sample_target": target, "gap": gap,
            "pending_known_urls": pending, "known_urls": known,
            "known_url_coverage_pct": observed_pct,
            "pages_checked_this_run": int(num(b.get("pages_checked")) or 0),
            "run_status": b.get("collection_status") or "unknown",
            "error_types": types, "policy_or_access_blockers": blockers,
            "next_step": (
                "정책/접근 제한: 추가 요청을 강행하지 않고 공식 대체 공개 경로와 수동 증거를 검토"
                if blockers else
                "미관측 공식 URL부터 순차 점검하고 기존 관측 자료 중복 제거"
            ),
        })
    return sorted(rows, key=lambda x: (-x["priority_score"], x["brand"]))[:limit]


def build(operations: dict, intelligence: dict, content: dict,
          quality: dict, calendar: dict) -> dict:
    report_day = day(intelligence.get("report_date") or operations.get("report_date"))
    weekly = operations.get("weekly") or {}
    current = (weekly.get("current") or {}).get("total") or {}
    previous = (weekly.get("previous") or {}).get("total") or {}
    cr, pr = num(current.get("revenue")), num(previous.get("revenue"))
    co, po = num(current.get("orders")), num(previous.get("orders"))
    cs, ps = num(current.get("sessions")), num(previous.get("sessions"))
    ca = cr/co if cr is not None and co and co > 0 else None
    pa = pr/po if pr is not None and po and po > 0 else None
    financial_ready = bool(
        weekly.get("status") == "ready" and all(v is not None for v in (cr, pr, co, po, cs, ps, ca, pa))
        and day(operations.get("report_date")) == report_day
    )
    finance = {
        "status": "observed" if financial_ready else "source_required",
        "period": weekly.get("period"), "compare_period": weekly.get("comparison_period"),
        "revenue": cr, "revenue_prior": pr, "revenue_pct": pct(cr, pr),
        "orders": co, "orders_prior": po, "orders_pct": pct(co, po),
        "sessions": cs, "sessions_prior": ps, "sessions_pct": pct(cs, ps),
        "aov": round(ca, 2) if ca is not None else None,
        "aov_prior": round(pa, 2) if pa is not None else None,
        "aov_pct": pct(ca, pa),
        "order_effect_krw": round((co-po)*pa, 2) if financial_ready else None,
        "aov_effect_krw": round(co*(ca-pa), 2) if financial_ready else None,
        "revenue_delta_krw": round(cr-pr, 2) if financial_ready else None,
        "source": INPUTS["operations"],
        "note": "R = 주문수 × 객단가의 정확한 산술 분해. 인과관계나 가격·할인 원인 증명이 아님.",
    }
    if financial_ready and abs(finance["order_effect_krw"]+finance["aov_effect_krw"]-finance["revenue_delta_krw"]) > 1:
        raise ValueError("Revenue bridge does not reconcile")
    readiness = (operations.get("executive") or {}).get("data_readiness") or {}
    focus = intelligence.get("product_focus") or {}
    cards = [c for c in focus.get("cards", []) if isinstance(c, dict)]
    pdp_up = [c for c in cards if (num(c.get("pdp_change_pct")) is not None
                                and c.get("pdp_change_pct") >= 20
                                and num(c.get("pdp_delta")) is not None)]
    sales_matched = [c for c in cards if num(c.get("sales_day")) is not None]
    weather_days = num(focus.get("weather_days"))
    linked_events = []
    for e in calendar.get("events", []):
        if not isinstance(e, dict):
            continue
        start, end = day(e.get("start_date")), day(e.get("end_date"))
        if not report_day or start is None or end is None:
            continue  # observed-only banners do NOT have an inferred event window
        if start <= report_day and end >= report_day - dt.timedelta(days=6):
            linked_events.append({
                "brand": e.get("brand") or e.get("brand_name") or "미확인",
                "title": e.get("title") or "공식 행사",
                "start": start.isoformat(), "end": end.isoformat(),
                "url": e.get("source_url") or e.get("url"),
                "date_status": e.get("date_status") or e.get("kind") or "unknown",
            })
    linked_events = linked_events[:12]
    problems = []
    if financial_ready and finance["revenue_pct"] < 0 and finance["orders_pct"] > 0 and finance["aov_pct"] < 0:
        problems.append({
            "id": "AOV_COMPOSITION", "priority": "P0", "status": "observed_decomposition",
            "title": "주문이 증가했는데 매출이 감소함",
            "finding": f"주문 {finance['orders_pct']:+.1f}%, 매출 {finance['revenue_pct']:+.1f}%, 객단가 {finance['aov_pct']:+.1f}%",
            "cause_component": "객단가 하락이 매출 감소를 구성한 사실은 산술적으로 확인됨",
            "cause_hypotheses": ["저가 상품 구매 비중 증가", "할인율 또는 쿠폰 적용 확대", "주문당 구매수량·제품 믹스 변화"],
            "hypothesis_status": "unverified", "causal_confirmed": False,
            "how_to_verify": "동일 기간 SKU×주문 단위 순매출·판매수량·정가·실결제가·쿠폰 적용률을 비교하고 가격 효과와 믹스 효과를 분리",
            "blocked_by": ["주문/CRM 원천" if readiness.get("order_mart") != "ready" else None],
            "sources": [INPUTS["operations"]],
        })
    if cards and len(sales_matched) < len(cards):
        names = [c.get("name") for c in pdp_up[:3] if c.get("name")]
        problems.append({
            "id": "PDP_SALES_JOIN", "priority": "P0", "status": "source_required",
            "title": "PDP 급등과 실제 판매 전환이 연결되지 않음",
            "finding": f"상품 카드 {len(cards)}개 중 판매량 확인 {len(sales_matched)}개 · PDP 상승 관측 {len(pdp_up)}개",
            "cause_component": "현재 데이터는 상품 관심의 증가만 관측하며 판매 전환 변화는 판단할 수 없음",
            "cause_hypotheses": ["측정 이벤트/상품명 연결 누락", "Top-N 표본으로 인한 판매 데이터 누락", "전환율 변화 또는 재고 제약"],
            "hypothesis_status": "unverified", "causal_confirmed": False,
            "how_to_verify": "GA4 item_id ↔ 관리자 스타일/컬러 매핑 후 view_item→add_to_cart→purchase 14일 전체 모집단 집계",
            "blocked_by": ["상품별 완전한 판매 집계"],
            "sample_products": names, "sources": [INPUTS["intelligence"]],
        })
    if weather_days is not None and weather_days < 14:
        problems.append({
            "id": "WEATHER_COVERAGE", "priority": "P1", "status": "source_required",
            "title": "날씨 영향 분석의 실측 기온이 누락됨",
            "finding": f"서울 관측 기온 {int(weather_days)} / 14일 확보",
            "cause_component": "실측 원천 부재로 온도 급변 시점과 PDP·판매 변화 교차 검증 불가",
            "cause_hypotheses": ["기상청 인증키 미설정", "ASOS 요청/파싱 실패", "관측일 기준·캐시 갱신 오류"],
            "hypothesis_status": "unverified", "causal_confirmed": False,
            "how_to_verify": "기상청 108 관측소의 요청 응답 코드·유효 일수·최종 적재일을 점검하고 관측치만 사용",
            "blocked_by": ["14일 실측 기온"], "sources": [INPUTS["intelligence"]],
        })
    stale = {k:v for k,v in readiness.items() if v not in ("ready", "available", "fresh")}
    if stale:
        problems.append({
            "id": "SOURCE_FRESHNESS", "priority": "P0", "status": "source_required",
            "title": "광고비·주문·CRM·재고 원천의 최신성이 부족함",
            "finding": " / ".join(f"{k}: {v}" for k,v in stale.items()),
            "cause_component": "데이터 갱신·접근이 제한되어 매출 영향의 근거 검증이 막힘",
            "cause_hypotheses": ["ETL 적재 지연", "API·계정 연동 장애", "실재고 조회 권한 문제"],
            "hypothesis_status": "unverified", "causal_confirmed": False,
            "how_to_verify": "각 원천의 마지막 적재 시각·건수·유효 스키마를 직접 조회하고 익일 동일 조건 재검증",
            "blocked_by": list(stale), "sources": [INPUTS["operations"]],
        })
    content_quality = content.get("quality") or {}
    observed_market = int(num(content_quality.get("market_count")) or 0)
    observed_owned = int(num(content_quality.get("owned_count")) or 0)
    if observed_market > 0 and observed_owned == 0:
        problems.append({
            "id": "OWNED_CONTENT_BASELINE", "priority": "P0", "status": "source_required",
            "title": "컬럼비아와 경쟁사의 콘텐츠 강·약점 비교 기준이 부족함",
            "finding": f"현재 분석 표본: 경쟁사 {observed_market}건 / 자사 0건",
            "cause_component": "자사 콘텐츠의 직접 관측 표본이 없으므로 경쟁사 대비 열세를 계산할 수 없음",
            "cause_hypotheses": ["공식몰 콘텐츠 경로 수집 누락", "자사 홈페이지 배너/기획전 연결 미흡", "수집 회차 차이"],
            "hypothesis_status": "unverified", "causal_confirmed": False,
            "how_to_verify": "컬럼비아 공식몰·공식 SNS의 동일 기간/동일 분류 기준 콘텐츠와 원본 URL을 수집한 뒤 표본 편향을 보정",
            "blocked_by": ["자사 콘텐츠 표본"], "sources": [INPUTS["content"]],
        })
    gaps = crawl_gaps(content, quality, limit=12)
    if gaps:
        problems.append({
            "id": "COMPETITOR_CRAWL", "priority": "P1", "status": "coverage_limited",
            "title": "일부 경쟁사 콘텐츠 수집 표본이 부족함",
            "finding": f"경쟁 콘텐츠 비교 {len(content.get('brands',[]))}개 브랜드 중 점검 우선 {len(gaps)}개",
            "cause_component": "수집 표본/알려진 공개 URL 접근 범위가 작아 브랜드 간 콘텐츠 비교 신뢰도가 제한됨",
            "cause_hypotheses": ["미방문 공개 URL 적체", "robots·요청 제한", "동적 네비게이션 추출 실패"],
            "hypothesis_status": "unverified", "causal_confirmed": False,
            "how_to_verify": "브랜드별 관측 고유 수·알려진 URL 커버리지·정책 오류를 분리해 안전하게 수집한 다음 동일 표본 정의로 비교",
            "blocked_by": [g["brand"] for g in gaps if g["policy_or_access_blockers"]],
            "sources": [INPUTS["content"], INPUTS["quality"]],
        })
    for problem in problems:
        problem["blocked_by"] = [v for v in problem.get("blocked_by", []) if v]
    return {
        "schema": 1, "report_date": report_day.isoformat() if report_day else None,
        "period": weekly.get("period"), "status": "partial" if problems else "no_diagnosis",
        "method": "관측 사실, 산술 분해, 검증 전 가설 구분. 단순 동시 발생을 원인으로 선언하지 않음.",
        "finance": finance,
        "problems": sorted(problems,key=lambda p: ({"P0":0,"P1":1,"P2":2}.get(p["priority"],3),p["id"])),
        "crawl": {
            "checked_at": content.get("source_checked_at") or quality.get("checked_at_kst"),
            "brand_count": len(content.get("brands", [])),
            "covered_30d": sum(1 for b in content.get("brands", []) if (num(b.get("retained_unique_count")) or 0) > 0),
            "top_gaps": gaps,
            "quality_scope": "최근 30일 관측 고유 콘텐츠 / 알려진 URL. 브랜드 전체 발행량이 아님.",
        },
        "market": {
            "official_event_overlap": linked_events,
            "interpretation": "날짜가 검증된 경쟁사 행사의 동시 발생만 표시. 매출 영향 증거가 아님.",
        },
        "source_paths": INPUTS,
    }


def main() -> None:
    inputs = {k: read(ROOT / p) for k,p in INPUTS.items()}
    data = build(inputs["operations"],inputs["intelligence"],inputs["content"],
                 inputs["quality"],inputs["calendar"])
    if not data["report_date"]:
        raise SystemExit("[ERROR] no consistent report date")
    TARGET.parent.mkdir(parents=True, exist_ok=True)
    TARGET.write_text(json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False)+"\n",encoding="utf-8")
    print("[DIAGNOSIS]",data["report_date"],"problems",len(data["problems"]),
          "crawl_gaps",len(data["crawl"]["top_gaps"]))


if __name__ == "__main__":
    main()
