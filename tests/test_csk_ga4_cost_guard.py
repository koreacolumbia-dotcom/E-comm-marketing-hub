"""Offline tests: exact GA4 unit-price/mix bridge and safe 1-GiB query boundary."""
import datetime as dt
import sys
import unittest
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"scripts"))
from csk_ga4_cost_guard import CAP, analyze, sqls, split_aov

END=dt.date(2026,11,20)
ORDERS=[
    {"period":"previous","orders":10,"missing_tx":0,"duplicates":1,"missing_revenue":0,"revenue":1500},
    {"period":"current","orders":10,"missing_tx":0,"duplicates":0,"missing_revenue":0,"revenue":2000},
]
ITEMS=[
    {"period":"previous","day":"20261110","item_id":"A","name":"Alpha Down","category":"OUTER",
     "units":10,"revenue":1500,"missing_revenue":0,"bad_quantity":0,"missing_discount":0,"reported_discount":10},
    {"period":"current","day":"20261118","item_id":"A","name":"Alpha Down","category":"OUTER",
     "units":20,"revenue":2000,"missing_revenue":0,"bad_quantity":0,"missing_discount":0,"reported_discount":20},
]
ADMIN={"report_date":"2026-11-20",
       "weekly":{"previous":{"total":{"orders":10,"revenue":1500}},
                 "current":{"total":{"orders":10,"revenue":2000}}}}
FOCUS={"product_focus":{"cards":[{"name":"Alpha Down"}]}}


class GA4CappedDiagnosticsTests(unittest.TestCase):
    def test_two_partition_pruned_queries(self):
        q=sqls(END)
        self.assertEqual(len(q),2)
        for s in q:
            self.assertIn("_TABLE_SUFFIX BETWEEN '20261107' AND '20261120'",s)
            self.assertIn("event_name='purchase'",s)
            self.assertNotIn("user_pseudo_id",s)
            self.assertNotIn("email",s)
            self.assertNotIn("SELECT *",s)
        self.assertEqual(CAP,1073741824)

    def test_exact_aov_split(self):
        current={"orders":10,"units":20,"item_revenue":2000}
        previous={"orders":10,"units":10,"item_revenue":1500}
        b=split_aov(current,previous)
        self.assertEqual(b["items_per_order_effect"],125)
        self.assertEqual(b["realized_item_revenue_effect"],-75)
        self.assertEqual(b["item_aov_delta"],50)

    def test_aggregate_and_admin_reconciliation(self):
        r=analyze(ORDERS,ITEMS,END,ADMIN,FOCUS)
        self.assertEqual(r["status"],"ga4_measured")
        self.assertTrue(r["admin_reconciliation"]["comparable"])
        self.assertEqual(r["category_bridge"]["category_mix_effect_per_item"],0)
        self.assertEqual(r["category_bridge"]["within_category_effect_per_item"],-50)
        self.assertEqual(r["product_sales"][0]["current_units"],20)
        self.assertEqual(r["product_sales"][0]["daily_units"][-3],20)
        self.assertEqual(len(r["product_sales"][0]["daily_units"]),14)

    def test_missing_items_cannot_be_verified(self):
        incomplete=[dict(ITEMS[0]),dict(ITEMS[1])]
        incomplete[1]["missing_revenue"]=1
        result=analyze(ORDERS,incomplete,END,ADMIN,FOCUS)
        self.assertEqual(result["status"],"partial_quality")
        self.assertIsNone(result["item_aov_bridge"])

    def test_missing_transactions_block_unqualified_result(self):
        o=[dict(x) for x in ORDERS]
        o[1]["missing_tx"]=2
        result=analyze(o,ITEMS,END,ADMIN,FOCUS)
        self.assertEqual(result["status"],"partial_quality")
        self.assertIsNone(result["item_aov_bridge"])


if __name__=="__main__":
    unittest.main()
