import csv
import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from scripts.csk_intelligence_bridges import (
    build, experiment_results, paid_spend_health, warehouse_health
)
from scripts.build_secure_csk_signals import freshness, probe, safe_id, write_probe

DAY = dt.date(2026, 11, 14)


class SpendTests(unittest.TestCase):
    def write_spend(self, path, lines):
        path.write_text("date,channel,sub_channel,spend\n" + lines, encoding="utf-8")

    def test_old_cost_is_not_eligible_for_roas(self):
        with tempfile.TemporaryDirectory() as t:
            p = Path(t) / "cost.csv"
            self.write_spend(p, "2026-04-06,meta,meta,100.0\n")
            x = paid_spend_health(DAY, p)
            self.assertEqual(x["status"], "stale")
            self.assertIsNone(x["current_spend"])
            self.assertFalse(x["roas_eligible"])

    def test_fresh_cost_is_still_unreconciled(self):
        with tempfile.TemporaryDirectory() as t:
            p = Path(t) / "cost.csv"
            self.write_spend(p, "2026-11-14,meta,meta,100.0\n")
            x = paid_spend_health(DAY, p)
            self.assertEqual(x["status"], "fresh_unreconciled")
            self.assertFalse(x["roas_eligible"])
            self.assertEqual(x["current_spend"], 100.0)

    def test_duplicate_and_future_cost_rows_are_blocked(self):
        with tempfile.TemporaryDirectory() as t:
            p = Path(t) / "cost.csv"
            self.write_spend(p, "2026-11-14,meta,meta,100\n2026-11-14,meta,meta,500\n")
            self.assertEqual(paid_spend_health(DAY, p)["status"], "duplicate_rows")
            self.write_spend(p, "2026-11-16,meta,meta,100\n")
            self.assertEqual(paid_spend_health(DAY, p)["status"], "future_rows")


class WarehouseTests(unittest.TestCase):
    def test_stock_is_incomplete_no_causal_claims(self):
        with tempfile.TemporaryDirectory() as t:
            p = Path(t) / "signals.json"
            p.write_text(json.dumps({"date": DAY.isoformat(),
                "order_mart": {"status": "ready"}, "crm": {"status": "ready"},
                "inventory": {"status": "incomplete"}}))
            out = warehouse_health(DAY, p)
            self.assertEqual(out["status"], "ready")
            self.assertEqual(out["inventory"], "incomplete")
            self.assertFalse(out.get("stock_source_valid", False))

    def test_warehouse_date_prevents_cross_day_use(self):
        with tempfile.TemporaryDirectory() as t:
            p = Path(t) / "signals.json"
            p.write_text(json.dumps({"date": "2026-04-06",
                "order_mart": {"status": "ready"}, "crm": {"status": "ready"}}))
            self.assertEqual(warehouse_health(DAY, p)["status"], "stale_probe")

    def test_invalid_bigquery_identifiers_blocked(self):
        with self.assertRaises(ValueError):
            safe_id("foo.bar;DROP TABLE x")
        with self.assertRaises(ValueError):
            safe_id("some/dataset")

    def test_explicit_no_access_is_not_success(self):
        class Client:
            def get_table(self, name):
                raise PermissionError("Not permitted")
        response, private = probe(DAY, Client(), "columbia-ga4",
                                  "crm_mart", "crm_raw", "order_product_mart")
        self.assertEqual(response["order_mart"]["status"], "query_error")
        self.assertEqual(response["crm"]["status"], "query_error")
        self.assertIsNone(private)

    def test_probe_no_private_member_counts_in_public(self):
        with tempfile.TemporaryDirectory() as t:
            p, crm_path = Path(t)/"status.json", Path(t)/"private"/"crm.json"
            report = {"date": DAY.isoformat(),
                      "order_mart": {"status": "ready"}, "crm": {"status": "ready"},
                      "inventory": {"status": "incomplete"}}
            private = {"as_of": DAY.isoformat(),
                       "segments": [{"segment": "A", "member_count": 123}]}
            write_probe(report, private, p, crm_path)
            self.assertNotIn("123", p.read_text())
            self.assertIn("123", crm_path.read_text())


class ExperimentTests(unittest.TestCase):
    def generate(self, root, start, pre_orders=10, post_orders=20):
        for i in range(-7, 7):
            d = start + dt.timedelta(days=i)
            order = pre_orders if i < 0 else post_orders
            data = {"end_date": d.isoformat(), "mode": "daily",
                    "kpis": {"sessions": 1000, "orders": order,
                             "revenue": order * 100000}}
            (root / f"{d.isoformat()}.json").write_text(json.dumps(data))

    def test_full_week_required_and_observational_only(self):
        with tempfile.TemporaryDirectory() as t:
            folder = Path(t)
            cfg = folder / "experiments.json"
            cfg.write_text(json.dumps({"experiments": [
                {"id": "CRM_ACTION", "title": "CRM 전환 개선", "start_date": "2026-11-08",
                 "window_days": 7}]}))
            self.generate(folder, dt.date(2026, 11, 8))
            r = experiment_results(DAY, cfg, folder)
            self.assertEqual(r["items"][0]["status"], "observational_only")
            self.assertEqual(r["items"][0]["observed_improvement"]["revenue_pct"], 100.0)
            self.assertFalse(r["items"][0]["causal_claim"])

    def test_missing_day_invalidates_experiment(self):
        with tempfile.TemporaryDirectory() as t:
            folder = Path(t)
            cfg = folder / "experiments.json"
            cfg.write_text(json.dumps({"experiments": [
                {"id": "X", "title": "X", "start_date": "2026-11-08",
                 "window_days": 7}]}))
            self.generate(folder, dt.date(2026, 11, 8))
            (folder / "2026-11-05.json").unlink()
            r = experiment_results(DAY, cfg, folder)
            self.assertEqual(r["items"][0]["status"], "insufficient_or_invalid_daily_data")
            self.assertIsNone(r["items"][0]["observed_improvement"])

    def test_not_matured_is_not_success(self):
        with tempfile.TemporaryDirectory() as t:
            folder = Path(t)
            cfg = folder / "experiments.json"
            cfg.write_text(json.dumps({"experiments": [
                {"id": "X", "start_date": "2026-11-13", "window_days": 7}]}))
            r = experiment_results(DAY, cfg, folder)
            self.assertEqual(r["items"][0]["status"], "awaiting_full_window")
            self.assertFalse(r["items"][0]["causal_claim"])


if __name__ == "__main__":
    unittest.main()
