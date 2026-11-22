"""Regression tests for sourced diagnosis and safe competitor crawl priorities."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from build_problem_diagnosis import build, crawl_gaps, describe_error

ROOT = Path(__file__).resolve().parents[1]
DECISION_HTML = (ROOT / "reports/decision_health/index.html").read_text(encoding="utf-8")
TIMELINE_JS = (ROOT / "reports/decision_health/timeline.js").read_text(encoding="utf-8")
DIAGNOSIS_JS = (ROOT / "reports/decision_health/diagnosis.js").read_text(encoding="utf-8")

OPS = {
    "report_date": "2026-11-20",
    "weekly": {
        "status": "ready",
        "period": "2026-11-14 ~ 2026-11-20",
        "current": {"total": {"revenue": 900, "orders": 12, "sessions": 100}},
        "previous": {"total": {"revenue": 1000, "orders": 10, "sessions": 90}},
    },
    "executive": {"data_readiness": {"paid_cost": "stale", "inventory": "connection_error"}},
}
DI = {
    "report_date": "2026-11-20",
    "product_focus": {"weather_days": 0, "cards": [
        {"name": "테스트 다운", "pdp_change_pct": 30, "pdp_delta": 300, "sales_day": None}
    ]}
}
CONTENT = {
    "source_checked_at": "2026-11-21",
    "brands": [
        {"brand": "TNF", "retained_unique_count": 7, "pending_urls": 41,
         "collection_status": "not_scheduled",
         "url_coverage": {"known_urls": 60, "observed_pct": 10},
         "coverage_plan": {"sample_target": 200}},
        {"brand": "K2", "retained_unique_count": 2, "pending_urls": 12,
         "url_coverage": {"known_urls": 20, "observed_pct": 5},
         "coverage_plan": {"sample_target": 100}},
    ],
}
QUALITY = {"brands": [{"brand": "TNF", "errors": [
    {"url": "https://example.test", "error": "robots disallowed path"}
]}]}
CAL = {"events": [{"brand": "NEPA", "title": "할인",
                   "kind": "verified_period", "date_status": "official_period",
                   "start_date": "2026-11-16", "end_date": "2026-11-22",
                   "source_url": "https://example.com/sale"},
                  {"brand": "K2", "title": "관측 배너", "start_date": None,
                   "end_date": None}]}


class ProblemDiagnosisTests(unittest.TestCase):
    def test_revenue_bridge_is_exact_arithmetic_not_causality(self):
        r = build(OPS, DI, CONTENT, QUALITY, CAL)
        f = r["finance"]
        self.assertEqual(f["order_effect_krw"], 200.0)
        self.assertEqual(f["aov_effect_krw"], -300.0)
        self.assertEqual(f["revenue_delta_krw"], -100.0)
        self.assertAlmostEqual(f["order_effect_krw"]+f["aov_effect_krw"], f["revenue_delta_krw"])
        aov = next(p for p in r["problems"] if p["id"] == "AOV_COMPOSITION")
        self.assertEqual(aov["status"], "observed_decomposition")
        self.assertFalse(aov["causal_confirmed"])
        self.assertEqual(aov["hypothesis_status"], "unverified")

    def test_missing_metrics_stay_missing(self):
        ops = {"report_date": "2026-11-20", "weekly": {"status": "partial",
               "current": {"total": {"revenue": 0, "orders": None}},
               "previous": {"total": {}}}}
        r = build(ops, DI, CONTENT, QUALITY, CAL)
        self.assertEqual(r["finance"]["status"], "source_required")
        self.assertIsNone(r["finance"]["aov_pct"])
        self.assertFalse(any(x["id"] == "AOV_COMPOSITION" for x in r["problems"]))

    def test_pdp_without_sales_is_not_rebranded_as_zero_sales(self):
        r = build(OPS, DI, CONTENT, QUALITY, CAL)
        row = next(p for p in r["problems"] if p["id"] == "PDP_SALES_JOIN")
        self.assertEqual(row["status"], "source_required")
        self.assertIn("테스트 다운", row["sample_products"])
        self.assertFalse(row["causal_confirmed"])

    def test_event_without_verified_dates_is_excluded_from_overlap(self):
        r = build(OPS, DI, CONTENT, QUALITY, CAL)
        self.assertEqual(len(r["market"]["official_event_overlap"]), 1)
        self.assertEqual(r["market"]["official_event_overlap"][0]["brand"], "NEPA")

    def test_policy_denied_does_not_trigger_bypass(self):
        gaps = crawl_gaps(CONTENT, QUALITY)
        tnf = next(v for v in gaps if v["brand"] == "TNF")
        self.assertIn("robots_policy", tnf["policy_or_access_blockers"])
        self.assertIn("추가 요청을 강행하지 않고", tnf["next_step"])

    def test_missing_owned_sample_is_not_market_underperformance(self):
        content = dict(CONTENT)
        content["quality"] = {"market_count": 49, "owned_count": 0}
        result = build(OPS, DI, content, QUALITY, CAL)
        item = next(p for p in result["problems"] if p["id"] == "OWNED_CONTENT_BASELINE")
        self.assertEqual(item["status"], "source_required")
        self.assertIn("강약점 비교가 성립하지 않음", item["cause_component"])
        self.assertFalse(item["causal_confirmed"])

    def test_priority_cards_on_existing_home(self):
        self.assertIn('id="priority-products"', DECISION_HTML)
        self.assertIn('id="priority-cards"', DECISION_HTML)
        self.assertIn('id="priority-detail"', DECISION_HTML)
        self.assertIn('원인 후보 ', DECISION_HTML)
        self.assertIn('원인 후보 TOP 3', DECISION_HTML)
        self.assertIn('구매 / PDP 1천회', DECISION_HTML)
        self.assertIn('전환율 아님', DECISION_HTML)
        self.assertIn('ga4_evidence', DECISION_HTML)
        self.assertIn('id="decision-additional"', DECISION_HTML)
        self.assertIn('scrollIntoView', DECISION_HTML)
        self.assertIn('p.sales_source==="ga4_purchase_items"', DECISION_HTML)
        self.assertIn('p.sales_previous_7d>0', DECISION_HTML)
        self.assertIn('전체 상품 원인 확정 아님', DECISION_HTML)
        self.assertNotIn('window.open(', DECISION_HTML)

    def test_cause_cards_prevent_fabricated_causality(self):
        self.assertIn('p.sales_source==="ga4_purchase_items"', DECISION_HTML)
        self.assertIn('p.sales_match_method', DECISION_HTML)
        self.assertIn('if(g.status!=="ga4_measured"', DECISION_HTML)
        self.assertIn('g.report_date!==focus?.date', DECISION_HTML)
        self.assertIn('manager', DECISION_HTML.replace('관리자', 'manager'))
        self.assertIn('원인 미확정', DECISION_HTML)
        self.assertIn('운영 매출 보고와 기간·정의가 달라', DECISION_HTML)
        self.assertNotIn('window.open(', DECISION_HTML)

    def test_measured_product_cause_uses_same_date_and_units(self):
        self.assertIn('measuredDetails(p,focus,ga4)', DECISION_HTML)
        self.assertIn('조회당 구매비', DECISION_HTML)
        self.assertIn('ga4.period?.current_end!==focus.date', DECISION_HTML)
        self.assertIn('v.previous_units!==p.sales_previous_7d', DECISION_HTML)
        self.assertIn('평균 실현매출', DECISION_HTML)
        self.assertIn('윈터세일 시작일(11/20)', DECISION_HTML)
        self.assertIn('수량 산술분해·인과효과 아님', DECISION_HTML)

    def test_cause_cards_do_not_require_manual_inventory_tracking(self):
        start=DECISION_HTML.index('/* Evidence-first cause shortlist.')
        end=DECISION_HTML.index('</script>', start)
        cause_html=DECISION_HTML[start:end]
        self.assertNotIn('옵션재고', cause_html)
        self.assertNotIn('옵션별 재고', cause_html)
        self.assertIn('장바구니율 · 유입매체 · 평균 실현매출', cause_html)
        self.assertIn('③ 평균 실현매출·상품구성', cause_html)
        self.assertNotIn('["재고",null,block.inventory', DECISION_HTML)
        self.assertIn('main #decision-additional #raw-evidence > section.grid', DIAGNOSIS_JS)
        self.assertIn('const withoutStock=', DIAGNOSIS_JS)
        self.assertIn('filter(x=>!/(재고|품절|inventory|stock)/i.test', DECISION_HTML)
        self.assertIn("p.id==='SOURCE_FRESHNESS'?'광고비·주문·CRM 원천 최신성 확인'", DIAGNOSIS_JS)

    def test_compact_verification_hides_text_heavy_technical_details(self):
        self.assertIn('id="evidence-short"', DECISION_HTML)
        self.assertIn('id="raw-evidence"', DECISION_HTML)
        self.assertIn('src="./evidence-cards.js?v=', DECISION_HTML)
        self.assertIn('검증된 숫자 3개만 보기', DECISION_HTML)
        self.assertIn('CSKDetailEvidence?.render(window.__cskCausePayload)', DECISION_HTML)

    def test_verified_funnel_refines_ranked_causes(self):
        self.assertIn('sessionEvidence=p=>', DECISION_HTML)
        self.assertIn('window.__cskSessionFunnel', DECISION_HTML)
        self.assertIn('장바구니 이후 구매완료율 약화', DECISION_HTML)
        self.assertIn('조회 감소 + 장바구니 진입 약화', DECISION_HTML)
        self.assertIn('stageDiagnosis(p)?.proof||detail.breakdown', DECISION_HTML)
        self.assertIn('f.report_date===day', DECISION_HTML)

    def test_numbered_proofs_and_no_verbose_raw_diagnostics(self):
        self.assertIn('metric(x.data1,1)+metric(x.data2,2)', DECISION_HTML)
        self.assertIn('근거 3 · ', DECISION_HTML)
        self.assertIn('장바구니→구매완료', DECISION_HTML)
        self.assertIn('조회→장바구니', DECISION_HTML)
        self.assertNotIn('근거 ③', DECISION_HTML)
        self.assertIn('id="raw-evidence" hidden', DECISION_HTML)
        self.assertNotIn('원천 로그·기술 진단 더 보기', DECISION_HTML)
        self.assertIn('href="./product_funnel_evidence.json"', DECISION_HTML)
        self.assertIn('href="./ga4_purchase_diagnostics.json"', DECISION_HTML)

    def test_weather_strip_is_present_and_source_scoped(self):
        self.assertIn('id="priority-weather"', DECISION_HTML)
        self.assertIn('NOAA 서울 대체 관측', DECISION_HTML)
        self.assertIn('기상청 서울 ASOS 관측', DECISION_HTML)
        self.assertIn('a.length===7&&b.length===7', DECISION_HTML)
        self.assertIn('매출 인과 미확정', DECISION_HTML)

    def test_decision_product_priority_and_evidence(self):
        self.assertIn('d>=15&&s<=-15', DECISION_HTML)
        self.assertIn('GA4 구매 매칭', DECISION_HTML)
        self.assertIn('p.sales_admin_reconciled===true', DECISION_HTML)
        self.assertIn('상품명 기준 GA4 매칭 표본', DECISION_HTML)

    def test_timeline_distinguishes_pdp_from_ga4_sales(self):
        self.assertIn('s.metric==="sales"', TIMELINE_JS)
        self.assertIn('s.metric==="pdp"', TIMELINE_JS)
        self.assertIn('GA4 매칭 판매수량', TIMELINE_JS)
        self.assertIn('csk-track-bar.sale', TIMELINE_JS)
        self.assertNotIn('(t.products||[]).slice(0,4).forEach', TIMELINE_JS)

    def test_observed_product_divergence_is_labeled_correctly(self):
        self.assertIn("p.status==='observed_product_divergence'?'관측된 PDP·구매 방향 차이'", DIAGNOSIS_JS)

    def test_ga4_period_propagates_only_for_matching_date(self):
        measured = {
            "report_date": "2026-11-20", "status": "ga4_measured",
            "period": {
                "previous_start": "2026-11-06", "previous_end": "2026-11-12",
                "current_start": "2026-11-13", "current_end": "2026-11-19"
            }
        }
        matched = build(OPS, DI, CONTENT, QUALITY, CAL, ga4_purchase=measured)
        self.assertEqual(matched["ga4_evidence"]["period"], measured["period"])
        stale = dict(measured, report_date="2026-11-19")
        unmatched = build(OPS, DI, CONTENT, QUALITY, CAL, ga4_purchase=stale)
        self.assertIsNone(unmatched["ga4_evidence"]["period"])

    def test_ga4_revenue_label_is_source_scoped_and_period_guarded(self):
        self.assertIn("GA4 구매 이벤트 매출 변동 구성", DIAGNOSIS_JS)
        self.assertIn("period.current_end!==opsPeriodEnd", DIAGNOSIS_JS)
        self.assertIn("할인·상품믹스의 원인은 미검증", DIAGNOSIS_JS)
        self.assertNotIn("주간 전체 매출 구성:", DIAGNOSIS_JS)

    def test_error_classification(self):
        self.assertEqual(describe_error("HTTP 429 stop requests")[0], "rate_limited")
        self.assertEqual(describe_error("Page.evaluate: Execution context was destroyed")[0], "navigation_race")
        self.assertEqual(describe_error("robots disallowed path")[0], "robots_policy")


if __name__ == "__main__":
    unittest.main()
