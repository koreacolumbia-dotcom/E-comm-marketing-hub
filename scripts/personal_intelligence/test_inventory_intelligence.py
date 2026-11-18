import unittest
from datetime import datetime
from zoneinfo import ZoneInfo
from inventory_intelligence import build_inventory

class InventoryTests(unittest.TestCase):
    def fixture(self):
        return {"end_date":"2026-11-17","product_scopes":{
            "current_name":[{"itemName":"M’s Jacket","views":200}],
            "previous_name":[{"itemName":"M`s Jacket","views":100}],
            "best_name":[],
            "best_sku":[{"itemId":"SKU123","itemName":"M's Jacket","qty":3,"views":100}]},
            "best_sellers_series":{"x":["11/11","11/12","11/13","11/14","11/15","11/16","11/17"],
                                    "items":[{"itemId":"SKU123","ys":[1,2,3,4,1,0,3]}]}}
    def test_unknowns_and_sales_speed(self):
        m=build_inventory(self.fixture(),{"report_date":"2026-11-17","inventory":"connection_error"})
        self.assertEqual(m["stock_source_status"],"connection_error")
        self.assertEqual(m["name_candidates"][0]["pdp_change_pct"],100)
        self.assertIsNone(m["name_candidates"][0]["top_seller_qty"])
        self.assertEqual(m["sku_candidates"][0]["sales_7d"],14)
        self.assertIsNone(m["sku_candidates"][0]["coverage_days"])
        self.assertIsNone(m["lost_sales"])
    def test_verified_current_not_historical(self):
        now=datetime(2026,11,18,10,tzinfo=ZoneInfo("Asia/Seoul"))
        stock={"source":"MSSQL dbo.TB_ProductSizeStock","status":"ready","as_of_kst":now.isoformat(),
               "rows":[{"sku":"SKU123","size_count":4,"positive_size_count":1,"sellable_units":10}]}
        m=build_inventory(self.fixture(),stock=stock,now=now)
        r=m["sku_candidates"][0]
        self.assertEqual(r["stock_status"],"limited_size_availability")
        self.assertIsNone(r["coverage_days"])
        stock["rows"][0]["quantity_scope"]="verified_online_sellable"
        self.assertEqual(build_inventory(self.fixture(),stock=stock,now=now)["sku_candidates"][0]["coverage_days"],5)
    def test_wrong_date_and_total(self):
        d=self.fixture()
        d["best_sellers_series"]["items"][0]["ys"][-1]=99
        self.assertIsNone(build_inventory(d)["sku_candidates"][0]["sales_7d"])
        self.assertEqual(build_inventory(d,{"report_date":"2026-11-16","inventory":"ready"})["stock_source_status"],"not_verified")
if __name__=="__main__":unittest.main()
