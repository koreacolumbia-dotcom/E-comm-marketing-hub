"""Regression tests for sourced diagnosis and safe competitor crawl priorities."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from build_problem_diagnosis import build, crawl_gaps, describe_error

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

    def test_error_classification(self):
        self.assertEqual(describe_error("HTTP 429 stop requests")[0], "rate_limited")
        self.assertEqual(describe_error("Page.evaluate: Execution context was destroyed")[0], "navigation_race")
        self.assertEqual(describe_error("robots disallowed path")[0], "robots_policy")


if __name__ == "__main__":
    unittest.main()
