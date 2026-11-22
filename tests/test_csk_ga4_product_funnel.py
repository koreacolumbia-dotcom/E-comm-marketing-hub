import importlib.util
import unittest
from datetime import date
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location("product_funnel",ROOT/"scripts/csk_ga4_product_funnel.py")
mod=importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


class FunnelAnalysis(unittest.TestCase):
    def test_only_verified_target_skus(self):
        focus={"date":"2026-11-21","cards":[{"name":"Test Down","sales_7d":2,"sales_previous_7d":3}]}
        purchases={"report_date":"2026-11-21","status":"ga4_measured","product_sales":[{"name":"Test Down","current_units":2,"previous_units":3,"codes":["C74AA001","in valid sku"]}]}
        self.assertEqual(mod.targets(focus,purchases),{"Test Down":["C74AA001"]})
        self.assertEqual(mod.targets(focus,{**purchases,"report_date":"2026-11-20"}),{})

    def test_partition_cap_and_true_session_order(self):
        q=mod.query_for(date(2026,11,21),{"Test Down":["C74AA001"]})
        self.assertIn("_TABLE_SUFFIX BETWEEN '20261108' AND '20261121'",q)
        self.assertIn("event_name IN ('view_item','add_to_cart','purchase')",q)
        self.assertIn("INNER JOIN targets AS t ON i.item_id=t.item_id",q)
        self.assertIn("cart_ts>=view_ts",q)
        self.assertIn("buy_ts>=cart_ts",q)
        self.assertIn("session_key",q)
        self.assertNotIn("SELECT *",q)
        self.assertEqual(mod.CAP,2147483648)

    def test_aggregate_is_reconciled_and_contains_no_user_identifiers(self):
        data=[{"period":"previous","style":"Test Down","first_source":"google",
               "view_sessions":100,"cart_after_view_sessions":10,"purchase_after_cart_sessions":5,"purchase_units":3},
              {"period":"current","style":"Test Down","first_source":"google",
               "view_sessions":60,"cart_after_view_sessions":3,"purchase_after_cart_sessions":2,"purchase_units":2}]
        result=mod.summarize(data,{"Test Down":["C74AA001"]},
            {"cards":[{"name":"Test Down","sales_7d":2,"sales_previous_7d":3}]})[0]
        self.assertEqual(result["view_to_cart_delta_pp"],-5)
        self.assertEqual(result["current"]["view_to_cart_pct"],5)
        self.assertEqual(result["previous"]["view_to_cart_pct"],10)
        self.assertTrue(result["coverage_reliable"])
        self.assertNotIn("user_pseudo_id",str(result))
        self.assertNotIn("ga_session_id",str(result))


if __name__=="__main__":
    unittest.main()
