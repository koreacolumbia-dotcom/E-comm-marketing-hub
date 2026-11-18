import unittest
from datetime import date,timedelta
from advanced_intelligence import integrated_calendar,impact_matrix,predict,strategy

def doc(end,days=30,stable=True):
    dates=[(end-timedelta(days=i)).isoformat() for i in range(days-1,-1,-1)]
    vals=[1000000+date.fromisoformat(x).weekday()*100000 for x in dates] if stable else [0]*days
    return {"mode":"daily","end_date":dates[-1],"meta":{"cur_end":dates[-1]},
      "overall":{"current":{"sessions":200,"transactions":10,"purchaseRevenue":vals[-1],"ga_api_reference":{"purchaseRevenue":vals[-1]}},"yoy":{"sessions":300,"transactions":15,"purchaseRevenue":2000000}},
      "trend_series":{"dates":dates,"revenue":vals,"sessions":[200]*days,"cvr":[.05]*days}}

class AdvancedTests(unittest.TestCase):
    def test_calendar_and_overlap(self):
        d=doc(date(2026,11,17))
        c={"events":[{"brand":"tnf","title":"A","date_status":"observed_only","observed_on":["2026-11-17"]},
                     {"brand":"tnf","title":"B","date_status":"official_period","source_url":"https://example.com/p","start_date":"2026-11-16","end_date":"2026-11-19"}]}
        owned={"loaded_date":d["end_date"],"message_log":[{"date":"2026-11-17","channel":"LMS","send_count":1,"message_body":"do not export"}]}
        m=integrated_calendar(d,c,owned=owned)
        self.assertEqual(len(m["days"]),37)
        self.assertEqual(m["official_event_count"],1)
        self.assertEqual(m["days"][-8]["date"],d["end_date"])
        self.assertIsNone(m["days"][-1]["revenue"])
        self.assertNotIn("do not export",str(m))
        x=impact_matrix(d,m)
        self.assertTrue(x["reconciled"])
        self.assertAlmostEqual(sum(r["accounting_delta"] for r in x["drivers"]),x["revenue_delta"],places=1)
        self.assertIsNone(x["overlaps"][0]["incremental_revenue"])
    def test_forecast_gates(self):
        today=date(2026,11,17);d=doc(today)
        self.assertEqual(predict(d)["status"],"source_required")
        m=predict(d,[doc(today-timedelta(days=28)),doc(today-timedelta(days=56))])
        self.assertEqual(m["status"],"available")
        self.assertEqual(len(m["forecast"]),7)
        self.assertEqual(m["holdout_wape_pct"],0)
        self.assertTrue(all(r["lower"]<=r["revenue"]<=r["upper"] for r in m["forecast"]))
        self.assertEqual(predict(doc(today,90,False))["forecast"],[])
    def test_strategy_is_advisory(self):
        d=doc(date(2026,11,17))
        m=strategy(d,impact_matrix(d),predict(d))
        self.assertFalse(m["auto_execute"])
        self.assertIsNone(m["expected_incremental_revenue"])
        self.assertTrue(any(r["decision_type"]=="data_recovery" for r in m["actions"]))

if __name__=="__main__":unittest.main()
