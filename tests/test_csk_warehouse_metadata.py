"""Zero-query warehouse metadata contract checks using fake table descriptors."""
import datetime as dt
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"scripts"))
from csk_warehouse_metadata import build_metadata,source_metadata,extract_fields

def field(name,kind="STRING",children=()):
    return SimpleNamespace(name=name,field_type=kind,fields=children)

class FakeBQ:
    def get_table(self,table):
        if "analytics_" in table:
            return SimpleNamespace(schema=[
                field("event_name"),field("event_date"),field("ecommerce","RECORD",(field("purchase_revenue"),)),
                field("items","RECORD",(
                    field("item_id"),field("item_name"),field("item_revenue"),
                    field("quantity","INTEGER"),field("item_category"),field("price","FLOAT")))
            ],modified=dt.datetime(2026,11,21,tzinfo=dt.timezone.utc),num_rows=111)
        if "crm_raw" in table:
            return SimpleNamespace(schema=[
                field("order_no"),field("order_use_coupon_price"),
                field("order_product_price"),field("product_promotion_sale_price")
            ],modified=dt.datetime(2026,10,30,tzinfo=dt.timezone.utc),num_rows=44)
        raise PermissionError("missing source")

class SchemaMetadataTests(unittest.TestCase):
    def test_nested_fields_and_no_synthetic_discount(self):
        doc=build_metadata(FakeBQ(),dt.date(2026,11,20))
        self.assertEqual(doc["query_jobs_executed"],0)
        self.assertEqual(doc["bytes_scanned"],0)
        self.assertFalse(doc["ga4_events"]["item_field_presence"]["discount"])
        self.assertTrue(doc["ga4_events"]["item_field_presence"]["item_revenue"])
        self.assertTrue(doc["order_product_mart"]["required_fields"]["order_use_coupon_price"])
        self.assertFalse(doc["order_product_mart"]["required_fields"]["purchase_qty"])
        self.assertFalse(doc["admin_mart"]["accessible"])
        self.assertEqual(doc["admin_mart"]["error_type"],"PermissionError")

    def test_nested_names_only(self):
        actual=extract_fields([field("items","RECORD",(field("name"),))])
        self.assertEqual(actual["items"][1],{"name":("STRING",{})})

if __name__=="__main__":
    unittest.main()
