import datetime as dt
import unittest
from scripts.build_marketing_intelligence import check, analyze

DAY = dt.date(2026, 11, 14)

def sample(sessions=100, channel_sessions=100, etc=0, pdp_views=50):
    return {
        "end_date": DAY.isoformat(), "built_at_kst": "2026-11-15 09:00:00",
        "kpis": {"sessions": sessions, "orders": 10, "revenue": 100000,
                 "cvr": 0.1, "signups": 3},
        "overall": {"prev": {"sessions": 90, "transactions": 15,
                             "purchaseRevenue": 120000, "cvr": 15 / 90},
                    "yoy": {"sessions": 80, "transactions": 9,
                            "purchaseRevenue": 90000, "cvr": 9 / 80}},
        "channel_snapshot": [
            {"bucket": "Paid Ad", "sessions": channel_sessions - etc,
             "transactions": 10, "purchaseRevenue": 100000},
            {"bucket": "etc", "sessions": etc, "transactions": 0, "purchaseRevenue": 0}],
        "paid_detail": [{"sub_channel": "Total", "sessions": channel_sessions - etc}],
        "pdp_series": {"x": ["11/14"], "rows": [
            {"itemCategory": "OUTER", "views_d1": pdp_views, "ys": [10]*6 + [pdp_views]}]},
        "admin_kpis": {"orders": 10}
    }

class MarketingIntelligenceTests(unittest.TestCase):
    def test_good_aggregate(self):
        obj = sample()
        qa = check(obj, DAY)
        self.assertEqual(qa["status"], "READY")
        self.assertTrue(qa["channel"])
        self.assertTrue(qa["pdp"])

    def test_channel_mismatch_blocks_channel_interpretation(self):
        obj = sample(channel_sessions=130)
        qa = check(obj, DAY)
        self.assertFalse(qa["channel"])
        self.assertEqual(qa["status"], "PARTIAL")
        self.assertTrue(any(x["code"] == "CHANNEL_MISMATCH" for x in qa["issues"]))
        report = analyze(obj, qa, DAY)
        self.assertIn("CHANNEL_QA", [x["code"] for x in report["actions"]])

    def test_etc_five_percent_must_be_qualified(self):
        qa = check(sample(etc=10), DAY)
        self.assertFalse(qa["channel"])
        self.assertIn("UNASSIGNED_TRAFFIC", [x["code"] for x in qa["issues"]])

    def test_pdp_zero_is_missing_not_performance(self):
        qa = check(sample(pdp_views=0), DAY)
        self.assertFalse(qa["pdp"])
        self.assertIn("PDP_INCOMPLETE", [x["code"] for x in qa["issues"]])

    def test_no_source_fails_closed(self):
        qa = check(None, DAY)
        self.assertEqual(qa["status"], "BLOCKED")
        self.assertFalse(qa["overall"])

if __name__ == "__main__":
    unittest.main()
