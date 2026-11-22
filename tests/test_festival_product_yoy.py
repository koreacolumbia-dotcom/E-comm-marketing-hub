"""Regression tests for bounded event-day style sales comparison without BigQuery."""
from __future__ import annotations

import unittest
from scripts.build_festival_product_yoy import (
    DEFAULT, compare, parse_year, period_from_ops, safe_name, sql_for, style_key
)

class ProductYoYTests(unittest.TestCase):
    def test_sql_scans_only_two_purchase_partitions_per_period(self):
        sql=sql_for("2026-11-20","2026-11-21")
        self.assertIn("FROM \`columbia-ga4.analytics_358593394.events_*\`",sql)
        self.assertIn("_TABLE_SUFFIX BETWEEN '20261120' AND '20261121'",sql)
        self.assertIn("event_name='purchase'",sql)
        self.assertIn("txn IS NULL OR rn=1",sql)
        self.assertIn("SUM(order_units) AS units",sql)
        self.assertNotIn("\\\`",sql)
        with self.assertRaises(ValueError):
            sql_for("2026-11-20' OR TRUE","2026-11-21")
    def test_d3_requires_same_weekday_verified_source(self):
        self.assertEqual(period_from_ops({"report_date":"2026-11-21"}),DEFAULT)
        ops={"report_date":"2026-11-22","weekly":{"yoy":{
            "status":"ready","daily_comparison":[
                {"date":"2026-11-22","ly_date":"2025-11-23",
                 "current":{"orders":1},"last_year":{"orders":2}}]}}}
        self.assertEqual(period_from_ops(ops)["2026"][1],"2026-11-22")
        ops["weekly"]["yoy"]["status"]="not_ready"
        self.assertEqual(period_from_ops(ops)["2026"][1],"2026-11-21")
    def test_revenue_bridge_is_on_item_revenue_only(self):
        previous=parse_year([
            {"kind":"order","order_count":2,"purchase_revenue":250},
            {"kind":"item","name":"River Point Down","sku":"C001","category":"OUTER",
             "units":2,"item_revenue":210,"missing_item_revenue":0},
            {"kind":"item","name":"Timber Fleece","sku":"C002","category":"FLEECE",
             "units":1,"item_revenue":45,"missing_item_revenue":0}])
        current=parse_year([
            {"kind":"order","order_count":3,"purchase_revenue":410},
            {"kind":"item","name":"River Point Down","sku":"C001","category":"OUTER",
             "units":1,"item_revenue":105,"missing_item_revenue":0},
            {"kind":"item","name":"New Down","sku":"C003","category":"OUTER",
             "units":2,"item_revenue":320,"missing_item_revenue":0}])
        result=compare(DEFAULT,{"2025":previous,"2026":current})
        self.assertEqual(result["item_revenue_delta"],170)
        self.assertEqual(sum(p["delta_revenue"] for p in result["products"]),170)
        self.assertEqual(result["years"]["2025"]["purchase_revenue"],250)
        self.assertEqual(result["years"]["2025"]["item_revenue"],255)
        self.assertEqual(result["years"]["2026"]["purchase_revenue"],410)
        self.assertEqual(result["years"]["2026"]["item_revenue"],425)
        self.assertEqual(result["products"][0]["name"],"New Down")
        self.assertTrue(result["quality"]["item_revenue_diff_reconciled"])
        self.assertFalse(result["causal"])
    def test_style_dedup_and_privacy(self):
        self.assertEqual(style_key(" A   B’C "),"a b'c")
        self.assertEqual(safe_name("test@example.com"),"(restricted)")
        self.assertEqual(safe_name("https://example.com/product"),"(restricted)")

if __name__=="__main__":
    unittest.main()
