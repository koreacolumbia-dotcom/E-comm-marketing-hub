"""GA4 Data API buyer cohort 2025/2026 historical comparison contracts."""
import unittest
from scripts.build_festival_ga4_buyer_yoy import METRICS,make_report,periods_from_product

PERIOD={"2025":["2025-11-21","2025-11-22"],"2026":["2026-11-20","2026-11-21"]}

def resp(total,first,orders,revenue):
    return {"metricHeaders":[{"name":n} for n in METRICS],
            "rows":[{"metricValues":[{"value":str(x)} for x in (total,first,orders,revenue)]}]}

class BuyerHistoricalTests(unittest.TestCase):
    def test_exact_first_time_and_repeat_2025_vs_2026(self):
        d=make_report({"2025":resp(400,260,427,68716525),
                       "2026":resp(565,471,605,94452556)},PERIOD,
            {"status":"ga4_measured","period":PERIOD,
             "years":{"2025":{"orders":427},"2026":{"orders":605}}})
        self.assertEqual(d["status"],"ga4_measured_yoy")
        self.assertEqual(d["years"]["2025"]["repeat_purchasers_derived"],140)
        self.assertEqual(d["years"]["2026"]["repeat_purchasers_derived"],94)
        self.assertEqual(d["deltas"]["first_time_purchasers"],211)
        self.assertEqual(d["deltas"]["repeat_purchasers_derived"],-46)
        self.assertFalse(d["contains_personal_data"])
        self.assertTrue(d["quality"]["event_orders_reconciled"])
        self.assertNotIn("customer_revenue",str(d))
    def test_wrong_order_reconciliation_is_blocked(self):
        with self.assertRaisesRegex(ValueError,"count differs"):
            make_report({"2025":resp(400,260,428,68716525),
                         "2026":resp(565,471,605,94452556)},PERIOD,
                 {"status":"ga4_measured","period":PERIOD,
                  "years":{"2025":{"orders":427},"2026":{"orders":605}}})
    def test_first_cannot_exceed_total_or_orders(self):
        for values in [(400,401,427,100),(400,260,390,100),(-2,1,427,100)]:
            with self.assertRaises(ValueError):
                make_report({"2025":resp(*values),"2026":resp(565,471,605,200)},PERIOD)
    def test_source_period_alignment(self):
        d=periods_from_product({"status":"ga4_measured","period":PERIOD})
        self.assertEqual(d,PERIOD)
        self.assertEqual(periods_from_product({}),PERIOD)
    def test_bad_headers_rejected(self):
        a=resp(400,260,427,500);a["metricHeaders"][1]["name"]="newUsers"
        with self.assertRaises(ValueError):
            make_report({"2025":a,"2026":resp(565,471,605,200)},PERIOD)

if __name__=="__main__":unittest.main()
