import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path

from scripts.build_marketing_intelligence_v2 import (
    actions_from_sources, agency, compile_report, crm_readiness, make_queue,
    quality_product, render_html, render_markdown, write_report,
)

DAY = dt.date(2026, 11, 14)


def good_daily():
    return {
        "end_date": "2026-11-14", "built_at_kst": "2026-11-15 09:00:00",
        "kpis": {"sessions": 1000, "orders": 20, "revenue": 2_000_000, "cvr": .02, "signups": 4},
        "overall": {"prev": {"sessions": 1000, "transactions": 30,
                             "purchaseRevenue": 3_200_000, "cvr": .03},
                    "yoy": {"sessions": 920, "transactions": 23,
                            "purchaseRevenue": 2_100_000, "cvr": .025}},
        "channel_snapshot": [
            {"bucket": "Paid Ad", "sessions": 500, "transactions": 10, "purchaseRevenue": 1000000},
            {"bucket": "Owned Channel", "sessions": 450, "transactions": 10, "purchaseRevenue": 1000000},
            {"bucket": "etc", "sessions": 50, "transactions": 0, "purchaseRevenue": 0},
        ],
        "paid_detail": [{"sub_channel": "meta", "sessions": 500, "orders": 10,
                         "session_dod": .2, "orders_dod": -.4},
                        {"sub_channel": "Total", "sessions": 500}],
        "pdp_series": {"x": ["11/14"], "rows": [{"views_d1": 50, "ys": [10] * 6 + [50]}]},
        "admin_kpis": {"orders": 20},
    }


class Intelligence2Tests(unittest.TestCase):
    def product_files(self, path, end="2026-11-14"):
        product = {"product_code": "C75XE1234010", "product_name": "Rock Jacket",
                   "category": "OUTER", "pdp_sessions": 1000,
                   "add_to_cart_sessions": 20, "purchase_sessions": 4,
                   "opportunity_score": 82}
        p, m = path / "pdp.json", path / "meta.json"
        p.write_text(json.dumps({"data_start": "2026-10-16", "data_end": end,
                                 "products": [product], "rows": [product]}), encoding="utf-8")
        m.write_text(json.dumps({"data_end": end}), encoding="utf-8")
        return p, m

    def test_product_candidates_are_observed_not_causal(self):
        with tempfile.TemporaryDirectory() as temp:
            p, m = self.product_files(Path(temp))
            result = quality_product(p, m, DAY, DAY)
            self.assertEqual(result["status"], "ready")
            self.assertEqual(result["candidates"][0]["atc_rate"], 2.)
            self.assertIn("증명하지 않음", result["notes"][-1])

    def test_product_stale_or_wrong_meta_is_suppressed(self):
        with tempfile.TemporaryDirectory() as temp:
            p, m = self.product_files(Path(temp), "2026-11-01")
            result = quality_product(p, m, DAY, DAY)
            self.assertEqual(result["status"], "waiting")
            self.assertFalse(result["candidates"])

    def test_high_views_zero_purchase_prioritizes_event_matching(self):
        with tempfile.TemporaryDirectory() as temp:
            folder = Path(temp)
            p, m = self.product_files(folder)
            data = json.loads(p.read_text())
            data["products"][0]["purchase_sessions"] = 0
            data["rows"][0]["purchase_sessions"] = 0
            p.write_text(json.dumps(data), encoding="utf-8")
            product = quality_product(p, m, DAY, DAY)
            self.assertTrue(product["candidates"][0]["measurement_watch"])
            self.assertIn("매칭", product["candidates"][0]["reason"])
            v1 = {"actions": []}
            media = {"status": "traffic_only"}
            crm = {"status": "aggregate_available_private"}
            actions = actions_from_sources(v1, product, media, crm)
            self.assertEqual(actions[0]["id"], "PRODUCT_QA_C75XE1234010")
            self.assertEqual(actions[0]["priority"], "P0")

    def test_product_no_future_data(self):
        with tempfile.TemporaryDirectory() as temp:
            p, m = self.product_files(Path(temp), "2026-11-17")
            result = quality_product(p, m, DAY, DAY)
            self.assertEqual(result["status"], "waiting")

    def test_agency_blocks_source_quality_and_never_invents_roas(self):
        data = good_daily()
        bad_qa = {"paid": False}
        result = agency(data, bad_qa)
        self.assertEqual(result["status"], "blocked")
        self.assertIn("ROAS", result["spend_status"])
        self.assertFalse(result["requests"][1]["is_finding"])

    def test_agency_trusted_traffic_only(self):
        result = agency(good_daily(), {"paid": True})
        self.assertEqual(result["status"], "traffic_only")
        self.assertEqual(result["flagged_media"][0]["media"], "meta")

    def test_crm_no_member_data_and_small_cohorts_blocked(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "crm.json"
            self.assertEqual(crm_readiness(path, DAY)["status"], "not_connected")
            path.write_text(json.dumps({"as_of": DAY.isoformat(),
                                        "segments": [{"segment": "A", "member_count": 1}]}))
            self.assertEqual(crm_readiness(path, DAY)["status"], "suppressed_small_cohort")
            path.write_text(json.dumps({"as_of": DAY.isoformat(),
                                        "segments": [{"user_id": "abc", "member_count": 100}]}))
            self.assertEqual(crm_readiness(path, DAY)["status"], "rejected_sensitive_schema")

    def test_action_states_sticky_not_auto_resolved(self):
        current = [{"id": "PDP_QA", "priority": "P0", "domain": "DATA", "title": "문제",
                    "observation": "0", "hypothesis": "지연", "validation": "재확인", "owner": "데이터"}]
        day2 = DAY + dt.timedelta(days=1)
        q1 = make_queue(current, None, None, DAY)
        self.assertEqual(q1[0]["status"], "OPEN")
        q2 = make_queue([], {"actions": q1}, None, day2)
        self.assertEqual(q2[0]["status"], "OPEN")
        self.assertFalse(q2[0]["observed_today"])
        override = {"overrides": {"PDP_QA": {"status": "IN_PROGRESS", "owner": "분석팀"}}}
        q3 = make_queue(current, {"actions": q1}, override, day2)
        self.assertEqual(q3[0]["status"], "IN_PROGRESS")
        self.assertEqual(q3[0]["observations"], 2)
        q4 = make_queue(current, {"actions": q3}, override, day2)
        self.assertEqual(q4[0]["observations"], 2)

    def test_end_to_end_missing_data_fail_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)
            r = compile_report(DAY, DAY, path, path / "absent.json", path / "meta.json",
                               path / "crm.json", None, None)
            self.assertEqual(r["status"], "BLOCKED")
            self.assertEqual(r["product"]["status"], "waiting")
            self.assertTrue(r["actions"])
            write_report(r, path / "output")
            self.assertTrue((path / "output" / "state.json").exists())
            self.assertIn("BLOCKED", render_markdown(r))

    def test_end_to_end_quality_block_and_html_escapes(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)
            d = path / "2026-11-14.json"
            raw = good_daily()
            raw["channel_snapshot"][0]["sessions"] = 700
            d.write_text(json.dumps(raw), encoding="utf-8")
            p, m = self.product_files(path)
            bad = json.loads(p.read_text())
            bad["products"][0]["product_name"] = "<script>alert('x')</script>"
            bad["rows"][0]["product_name"] = "<script>alert('x')</script>"
            p.write_text(json.dumps(bad), encoding="utf-8")
            r = compile_report(DAY, DAY, path, p, m, path / "crm.json", None, None)
            self.assertFalse(r["quality"]["channel"])
            self.assertEqual(r["agency"]["status"], "blocked")
            self.assertIn("PRODUCT_C75XE1234010", [x["id"] for x in r["actions"]])
            page = render_html(r)
            self.assertIn("&lt;script&gt;", page)
            self.assertNotIn("<script>alert", page)


if __name__ == "__main__":
    unittest.main()
