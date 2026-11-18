import unittest
from multi_touch import summarize,weights
class MultiTouchTests(unittest.TestCase):
    def test_weights(self):
        for n in range(1,10):
            for m in ("first_touch","last_touch","linear","position_based"):
                self.assertAlmostEqual(sum(weights(n,m)),1)
        self.assertEqual(weights(2,"position_based"),[.5,.5])
        self.assertEqual(weights(3,"position_based"),[.4,.2,.4])
    def test_credit_and_unknown(self):
        r=summarize([{"path":["Organic Traffic","Paid Ad","Owned Channel"],"orders":10,"revenue":100000},
                     {"path":["Awareness","Paid Ad"],"orders":2,"revenue":20000}],
                     15,150000,"2026-11-17","2026-11-17")
        self.assertEqual(r["observed_orders"],12)
        self.assertEqual(r["unattributed_orders"],3)
        self.assertEqual(r["unattributed_revenue"],30000)
        self.assertEqual(len(r["top_paths"]),1)
        self.assertEqual(r["suppressed_path_orders"],2)
        for k,v in r["model_totals"].items():
            self.assertAlmostEqual(v["revenue"],120000)
            self.assertAlmostEqual(v["orders"],12)
        self.assertEqual(r["assisted_orders_by_channel"]["Organic Traffic"],10)
    def test_impossible_total(self):
        with self.assertRaises(ValueError):
            summarize([{"path":["Paid Ad"],"orders":2,"revenue":10}],1,10,"2026-11-17","2026-11-17")
if __name__=="__main__": unittest.main()
