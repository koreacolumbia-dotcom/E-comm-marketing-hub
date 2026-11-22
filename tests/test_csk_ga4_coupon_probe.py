"""Coupon presence is measured separately from discount amount."""
import datetime as dt
import sys
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"scripts"))
from csk_ga4_coupon_probe import CAP, statement, infer

class CouponTests(unittest.TestCase):
    def test_bounded_partition_and_no_private_output_columns(self):
        sql=statement(dt.date(2026,11,20))
        self.assertIn("_TABLE_SUFFIX BETWEEN '20261107' AND '20261120'",sql)
        self.assertIn("i.coupon",sql)
        self.assertIn("purchase",sql)
        self.assertNotIn("user_pseudo_id",sql)
        self.assertEqual(CAP,512*1024*1024)

    def test_coupon_presence_does_not_claim_discount_krw(self):
        rows=[dict(period="previous",orders_with_items=10,orders_with_item_coupon=3,
                   observed_item_rows=15,item_coupon_rows=4,coupon_units=5,units=20,
                   coupon_item_revenue=400,item_revenue=1500,missing_values=0),
              dict(period="current",orders_with_items=12,orders_with_item_coupon=6,
                   observed_item_rows=20,item_coupon_rows=8,coupon_units=9,units=19,
                   coupon_item_revenue=600,item_revenue=1800,missing_values=0)]
        ref={"totals":{"previous":{"orders":10},"current":{"orders":12}}}
        x=infer(rows,ref,dt.date(2026,11,20))
        self.assertEqual(x["status"],"measured_coupon_presence")
        self.assertEqual(x["coupon_presence_delta_pp"],20)
        self.assertNotIn("discount_amount",x)

    def test_denominator_mismatch_never_passes(self):
        rows=[dict(period="previous",orders_with_items=5,orders_with_item_coupon=3,
                   observed_item_rows=5,item_coupon_rows=3,coupon_units=3,units=5,
                   coupon_item_revenue=400,item_revenue=500,missing_values=0)]
        ref={"totals":{"previous":{"orders":10},"current":{"orders":12}}}
        self.assertEqual(infer(rows,ref,dt.date(2026,11,20))["status"],"partial_quality")

if __name__=="__main__":unittest.main()
