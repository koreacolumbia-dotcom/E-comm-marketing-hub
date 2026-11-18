import unittest
from datetime import date,timedelta
from budget_optimizer import optimize,MEDIA,PROTECTED

class BudgetTests(unittest.TestCase):
    def fixture(self):
        docs=[];cost=[];controls={}
        for i in range(7):
            day=(date(2026,11,17)-timedelta(days=i)).isoformat()
            rows=[{"sub_channel":k,"sessions":200,"orders":3,"purchaseRevenue":100000*(j+1)} for j,k in enumerate(MEDIA)]
            docs.append({"end_date":day,"meta":{"cur_start":day,"cur_end":day},"paid_detail":rows,
                         "channel_snapshot":[{"bucket":"Paid Ad","sessions":sum(r["sessions"] for r in rows),
                            "transactions":sum(r["orders"] for r in rows),"purchaseRevenue":sum(r["purchaseRevenue"] for r in rows)}]})
            cost.extend({"date":day,"media":k,"spend":100000} for k in MEDIA)
            controls[day]=600000
        return docs,{"status":"verified","currency":"KRW","scope":"paid_detail_media_reconciled",
                     "provenance":"synthetic fixture","rows":cost,"daily_totals":controls}
    def test_budget_neutral_protected(self):
        d,cost=self.fixture()
        m=optimize(d[0],d[1:],cost)
        self.assertEqual(m["status"],"available")
        p=m["scenario"]
        self.assertIsNotNone(p)
        self.assertEqual(sum(x["change"] for x in p["allocations"]),0)
        self.assertTrue(all(abs(x["change"])<=x["baseline_spend"]*.1 for x in p["allocations"]))
        self.assertTrue(all(x["change"]==0 for x in p["allocations"] if x["media"] in PROTECTED))
        self.assertFalse(m["auto_apply"])
        self.assertIsNone(p["forecast_revenue"])
    def test_missing_spend_keeps_actual_traffic(self):
        d,cost=self.fixture()
        m=optimize(d[0],d[1:],{"status":"stale","latest_date":"2026-04-06"})
        self.assertIsNone(m["scenario"])
        self.assertEqual(m["cost_status"],"stale")
        self.assertEqual(len(m["media"]),len(MEDIA))
        self.assertTrue(all(x["roas_pct"] is None for x in m["media"]))
    def test_missing_cost_and_duplicate(self):
        d,cost=self.fixture()
        cost["rows"].pop()
        self.assertIsNone(optimize(d[0],d[1:],cost)["scenario"])
        d,cost=self.fixture()
        cost["rows"].append(cost["rows"][0])
        self.assertEqual(optimize(d[0],d[1:],cost)["status"],"validation_required")
    def test_mismatched_control(self):
        d,cost=self.fixture()
        cost["daily_totals"][d[0]["end_date"]]=100
        self.assertEqual(optimize(d[0],d[1:],cost)["status"],"validation_required")
    def test_parent_reconciliation(self):
        d,cost=self.fixture()
        d[0]["channel_snapshot"][0]["purchaseRevenue"]+=1
        self.assertFalse(optimize(d[0],d[1:],cost)["media"])

if __name__=="__main__": unittest.main()
