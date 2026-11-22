"""Regression tests: new/existing customer classification is only first observed purchase."""
import datetime as dt
import unittest
from scripts.build_festival_customer_yoy import build_sql,make_payload,windows,CANDIDATES

PERIOD={"2025":["2025-11-21","2025-11-22"],"2026":["2026-11-20","2026-11-21"]}

class FestivalCustomerCohortTest(unittest.TestCase):
    def test_select_same_period_as_product_data(self):
        self.assertEqual(windows({"status":"ga4_measured","period":PERIOD}),PERIOD)
        self.assertEqual(windows({})["2026"],PERIOD["2026"])
    def test_sql_dedup_order_history_and_no_customer_identifiers_selected(self):
        t,cols,kind=CANDIDATES[0]
        sql=build_sql(t,cols,PERIOD,kind)
        self.assertIn("member_first AS",sql)
        self.assertIn("MAX(SAFE_CAST(order_total_pay AS FLOAT64))",sql)
        self.assertIn("COUNTIF(SAFE_CAST(is_net_sales_line AS INT64)=1)>0",sql)
        self.assertIn("MIN(dt) AS first_date",sql)
        self.assertIn("COUNT(DISTINCT e.mid) AS buyers",sql)
        self.assertIn("DATE '2025-11-21'",sql)
        self.assertIn("DATE '2026-11-20'",sql)
        self.assertNotIn("SELECT e.mid",sql)
    def test_partition_totals_and_revenue_deltas_reconcile(self):
        rows=[
           dict(year="2025",customer_type="new",buyers=3,orders=4,revenue=2200,
                min_history=dt.date(2024,5,1),max_history=dt.date(2026,11,22),historical_orders=1000),
           dict(year="2025",customer_type="existing",buyers=4,orders=6,revenue=5000,
                min_history=dt.date(2024,5,1),max_history=dt.date(2026,11,22),historical_orders=1000),
           dict(year="2026",customer_type="new",buyers=5,orders=7,revenue=6000,
                min_history=dt.date(2024,5,1),max_history=dt.date(2026,11,22),historical_orders=1000),
           dict(year="2026",customer_type="existing",buyers=9,orders=10,revenue=9200,
                min_history=dt.date(2024,5,1),max_history=dt.date(2026,11,22),historical_orders=1000)]
        report=make_payload(rows,PERIOD,"crm_mart.tb_order_product_search_mart",100000)
        self.assertEqual(report["status"],"measured_observed_first_purchase")
        self.assertEqual(report["deltas"]["new"]["revenue"],3800)
        self.assertEqual(report["deltas"]["existing"]["revenue"],4200)
        self.assertEqual(report["total_revenue_delta"],8000)
        self.assertEqual(report["years"]["2025"]["total"]["orders"],10)
        self.assertEqual(report["years"]["2026"]["total"]["orders"],17)
        self.assertEqual(report["years"]["2026"]["existing"]["revenue_share_pct"],60.53)
        self.assertFalse(report["causal"])
        self.assertFalse(report["quality"]["customer_identifiers_published"])
    def test_no_fake_lifetime_new_if_history_short(self):
        rows=[dict(year=y,customer_type="new",buyers=4,orders=5,revenue=1000,
            min_history=dt.date(2025,10,1),max_history=dt.date(2026,11,22),
            historical_orders=100) for y in ("2025","2026")]
        report=make_payload(rows,PERIOD,"crm_mart.tb_order_product_search_mart",10000)
        self.assertEqual(report["status"],"insufficient_history")
        self.assertNotIn("years",report)
    def test_stale_source_not_published(self):
        rows=[dict(year=y,customer_type="new",buyers=4,orders=5,revenue=1000,
            min_history=dt.date(2024,5,1),max_history=dt.date(2026,10,1),
            historical_orders=100) for y in ("2025","2026")]
        report=make_payload(rows,PERIOD,"crm_mart.tb_order_product_search_mart",10000)
        self.assertEqual(report["status"],"stale_source")
    def test_unknown_buyers_not_assigned_to_existing(self):
        rows=[]
        for y in ("2025","2026"):
            for cohort,n in (("new",1),("existing",2),("unknown",5)):
                rows.append(dict(year=y,customer_type=cohort,buyers=n if cohort!="unknown" else 0,
                    orders=n,revenue=n*100,min_history=dt.date(2024,5,1),
                    max_history=dt.date(2026,11,22),historical_orders=100))
        report=make_payload(rows,PERIOD,"crm_raw.tb_order_staging",20000)
        self.assertEqual(report["status"],"low_member_coverage")
        self.assertNotIn("years",report)

if __name__=="__main__":unittest.main()
