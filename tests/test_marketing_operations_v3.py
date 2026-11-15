import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path

from scripts.build_marketing_operations_v3 import (
    calendar_context, product_triage, weekly_kpis, incrementality,
    recovery_plan, build, render_html, render_md, write_report
)
from scripts.sync_csk_ops_issues import candidates, body, marker, sync

DAY = dt.date(2026, 11, 14)


def daily(day, orders=20, revenue=2_000_000):
    return {"mode": "daily", "end_date": day.isoformat(),
            "meta": {"yoy_start": "2025-11-15"},
            "kpis": {"sessions": 2000, "orders": orders, "revenue": revenue}}


def make_v2():
    return {
        "report_date": DAY.isoformat(), "status": "PARTIAL",
        "quality": {"channel": False, "pdp": False},
        "product": {"status": "ready", "candidates": [{
            "code": "C74YM3674342", "name": "Jacket <script>",
            "sessions": 13569, "atc_rate": 0.1,
            "purchase_sessions": 0, "measurement_watch": True,
            "source_period": "2026-10-17 ~ 2026-11-15",
            "validation": "check SKU"
        }]},
        "integrations": {
            "paid_cost": {"status": "stale", "latest_date": "2026-04-06"},
            "warehouse": {"order_mart": "stale", "crm": "stale",
                          "inventory": "connection_error"}},
        "actions": [{"id": "CHANNEL_QA", "priority": "P0",
                     "title": "채널 검증", "status": "OPEN", "owner": "데이터팀"}]
    }


class CalendarTests(unittest.TestCase):
    def test_yoy_uses_daily_digest_anchor_not_same_calendar_date(self):
        with tempfile.TemporaryDirectory() as temp:
            folder = Path(temp)
            c25, owned, c26 = [folder / n for n in ("c25.json", "owned.json", "c26.json")]
            c25.write_text(json.dumps({"entries": [
                {"date":"2025-11-13", "kind":"event", "title":"Winter Down",
                 "status":"confirmed", "source":"W46"},
                {"date":"2025-11-12", "kind":"event", "title":"Campaign planned",
                 "status":"planned", "source":"W46"},
                {"date":"2025-11-18", "kind":"event", "title":"Next week",
                 "status":"confirmed", "source":"W47"}]}))
            owned.write_text(json.dumps({"entries":[
                {"date":"2025-11-14","channel":"LMS","title":"Owned send",
                 "status":"confirmed","sent_count":500,"source":"2025 XLS"},
                {"date":"2025-11-14","channel":"KAKAO","title":"Unconfirmed plan",
                 "status":"planned","sent_count":None,"source":"2025 XLS"}]}))
            c26.write_text(json.dumps({"entries":[]}))
            c = calendar_context(DAY, daily(DAY), c25, owned, c26)
            self.assertEqual(c["yoy_window"], "2025-11-09 ~ 2025-11-15")
            self.assertEqual(c["current_status"], "not_connected")
            self.assertEqual(len(c["last_year_confirmed"]), 2)
            self.assertEqual(len(c["last_year_planned"]), 2)
            self.assertNotIn("Next week", [x["title"] for x in c["last_year_confirmed"]])

    def test_missing_yoy_daily_has_no_reference(self):
        with tempfile.TemporaryDirectory() as temp:
            f = Path(temp)
            c = calendar_context(DAY, {"end_date": "2026-11-13", "meta": {
                "yoy_start": "2025-11-14"}}, f/"x", f/"y", f/"z")
            self.assertEqual(c["status"], "reference_missing")
            self.assertIsNone(c["yoy_window"])

    def test_empty_current_is_unknown_not_zero(self):
        with tempfile.TemporaryDirectory() as temp:
            folder = Path(temp)
            f = folder/"2026.json"
            f.write_text('{"entries":[]}')
            r = calendar_context(DAY, daily(DAY), folder/"25", folder/"owned", f)
            self.assertEqual(r["current"], [])
            self.assertEqual(r["current_status"], "not_connected")


class ProductTests(unittest.TestCase):
    def test_zero_purchase_high_views_goes_to_measurement_qa(self):
        result = product_triage(make_v2())
        self.assertEqual(result["status"], "diagnostics_only")
        self.assertEqual(result["items"][0]["state"], "MEASUREMENT_QA")
        self.assertIsNone(result["items"][0]["revenue_opportunity_krw"])
        self.assertEqual(result["items"][0]["stock_evidence"], "unknown")

    def test_product_without_trusted_source_blocks_recommendation(self):
        v2 = make_v2()
        v2["product"]["status"] = "waiting"
        self.assertEqual(product_triage(v2)["status"], "blocked")

    def test_recovery_builds_evidence_actions(self):
        result = recovery_plan(make_v2())
        codes = {x["code"] for x in result}
        self.assertEqual(codes, {"PAID_COST_FRESHNESS", "WAREHOUSE_ETL_FRESHNESS",
                                 "MSSQL_STOCK_ACCESS", "CHANNEL_QA", "PDP_QA"})


class WeeklyTests(unittest.TestCase):
    def test_incomplete_week_never_fills_zero(self):
        with tempfile.TemporaryDirectory() as temp:
            r = weekly_kpis(DAY, Path(temp))
            self.assertEqual(r["status"], "incomplete")
            self.assertIsNone(r["current"]["total"])
            self.assertEqual(len(r["current"]["missing_dates"]), 7)

    def test_both_full_weeks_are_comparable(self):
        with tempfile.TemporaryDirectory() as temp:
            folder = Path(temp)
            for offset in range(13, -1, -1):
                d = DAY - dt.timedelta(days=offset)
                current = offset < 7
                (folder/f"{d}.json").write_text(json.dumps(
                    daily(d, orders=20 if current else 10,
                          revenue=2_000_000 if current else 1_000_000)))
            r = weekly_kpis(DAY, folder)
            self.assertEqual(r["status"], "ready")
            self.assertEqual(r["wow_pct"]["orders"], 100)
            self.assertEqual(r["current"]["total"]["sessions"], 14_000)


class IncrementalityTests(unittest.TestCase):
    def test_private_missing_does_not_invent_results(self):
        with tempfile.TemporaryDirectory() as temp:
            r = incrementality(DAY, Path(temp)/"missing.json")
            self.assertEqual(r["status"], "awaiting_private_aggregates")
            self.assertEqual(r["results"], [])

    def test_randomized_diagnostic_is_not_causal_proof(self):
        with tempfile.TemporaryDirectory() as temp:
            f = Path(temp)/"data.json"
            f.write_text(json.dumps({"experiments":[{
                "id": "CRM1", "title": "Welcome campaign",
                "end_date": DAY.isoformat(),
                "randomization_verified": True,
                "assignment_audit_passed": True,
                "treatment":{"eligible":1000,"orders":130,"revenue":26000000},
                "control":{"eligible":1000,"orders":100,"revenue":20000000}
            }]}))
            r = incrementality(DAY, f)
            self.assertEqual(r["status"], "descriptive_readout")
            row = r["results"][0]
            self.assertEqual(row["cvr_difference_pp"], 3.0)
            self.assertFalse(row["is_causal_proof"])
            self.assertEqual(row["revenue_per_eligible_difference_krw"], 6000.0)

    def test_private_small_cohort_is_suppressed(self):
        with tempfile.TemporaryDirectory() as temp:
            f = Path(temp)/"data.json"
            f.write_text(json.dumps({"experiments":[{
                "id": "tiny", "end_date":DAY.isoformat(),
                "treatment":{"eligible":10,"orders":3,"revenue":5000},
                "control":{"eligible":11,"orders":2,"revenue":4500}
            }]}))
            r = incrementality(DAY, f)
            self.assertEqual(r["status"], "awaiting_valid_private_aggregates")
            self.assertEqual(r["results"], [])


class EndToEndTests(unittest.TestCase):
    def test_build_and_render_with_incomplete_sources(self):
        with tempfile.TemporaryDirectory() as temp:
            folder = Path(temp)
            (folder/f"{DAY}.json").write_text(json.dumps(daily(DAY)))
            v2 = make_v2()
            r = build(DAY, v2, folder, folder/"25", folder/"owned",
                      folder/"2026", folder/"private")
            self.assertEqual(r["status"], "PARTIAL")
            self.assertEqual(len(r["recovery"]), 5)
            self.assertEqual(r["weekly"]["status"], "incomplete")
            self.assertIn("원천 복구", render_md(r))
            page = render_html(r)
            self.assertIn("&lt;script&gt;", page)
            self.assertNotIn("Jacket <script>", page)
            write_report(r, folder/"result")
            self.assertTrue((folder/"result"/"index.html").exists())

    def test_stale_v2_date_aborts(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(ValueError):
                build(DAY, {"report_date": "2026-11-13"}, Path(temp))


class IssueTests(unittest.TestCase):
    def test_idempotent_never_reopens_human_closed_issue(self):
        class FakeAPI:
            def __init__(self):
                self.items = []
                self.created = []
            def list_issues(self):
                return self.items
            def create_issue(self, title, content):
                x = {"number": 10+len(self.created), "title":title,
                     "body":content, "state":"open", "html_url":"https://github.com/example"}
                self.created.append(x)
                self.items.append(x)
                return x
        report = {"recovery": recovery_plan(make_v2()),
                  "products": product_triage(make_v2())}
        api=FakeAPI()
        first=sync(report, api, enabled=True)
        self.assertGreater(len(first["created"]), 3)
        again=sync(report, api, enabled=True)
        self.assertEqual(again["created"], [])
        self.assertEqual(len(api.items), len(first["created"]))
        api.items[0]["state"]="closed"
        final=sync(report, api, enabled=True)
        self.assertIn(api.items[0]["body"].split("CSKOPS-ID: ")[1].split(" -->")[0],
                      final["skipped_closed"])
        self.assertEqual(final["created"], [])

    def test_issue_marker_rejects_unsafe_input(self):
        with self.assertRaises(ValueError):
            marker("A<script>")
        report={"recovery":[{"code":"UNKNOWN","owner":"A","next_step":"B"}],
                "products":{"items":[]}}
        self.assertEqual(candidates(report), [])
        self.assertEqual(sync(report, enabled=False)["status"], "dry_run")


if __name__=="__main__":
    unittest.main()
