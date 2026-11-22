import unittest
from scripts.build_festival_report_snapshot import build,event_days,weekly_channels,media_event,metrics

def day(date,ly,now,prev):
    return {"date":date,"ly_date":ly,
            "current":{"sessions":1000,"orders":10,"revenue":now},
            "last_year":{"sessions":900,"orders":9,"revenue":prev}}

class FestivalSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.ops={"report_date":"2026-11-22",
                  "weekly":{"yoy":{"status":"ready","daily_comparison":[
                       day("2026-11-20","2025-11-21",1200,1000),
                       day("2026-11-21","2025-11-22",1400,1100),
                       day("2026-11-22","2025-11-23",1600,1300)]}}}
        self.source={"status":"ga4_measured","report_date":"2026-11-21",
                     "period":{"previous":{"start":"2026-11-08","end":"2026-11-14"},
                               "current":{"start":"2026-11-15","end":"2026-11-21"}},
                     "rows":[{"source_medium":"meta / banner",
                              "previous":{"sessions":100,"transactions":4,"revenue":400},
                              "current":{"sessions":150,"transactions":5,"revenue":800}}]}
        self.intel={"integrations":{"paid_cost":{"status":"stale","latest_date":"2026-04-06",
                                                "roas_eligible":False}}}
    def test_event_d3_same_weekday_only(self):
        result=event_days(self.ops,{})
        self.assertEqual(result["days"],3)
        self.assertEqual(result["totals"]["current"]["revenue"],4200)
        self.assertEqual(result["totals"]["previous"]["revenue"],3400)
        self.assertEqual(result["daily"][2]["previous_date"],"2025-11-23")
        self.assertFalse(result["causal"])
    def test_no_partial_d3_and_preserve_historic_verified_event(self):
        ops=dict(self.ops,report_date="2026-11-21")
        out=event_days(ops,{})
        self.assertEqual(out["days"],2)
        self.assertEqual(out["totals"]["current"]["revenue"],2600)
        self.assertEqual(event_days({"report_date":"2026-11-25","weekly":{"yoy":{}}},
                                    {"event":out})["days"],2)
    def test_paid_cost_and_weekly_period_are_explicit(self):
        result=build(self.ops,self.source,{},self.intel)
        self.assertEqual(result["weekly_media"]["rows"][0]["current"]["revenue"],800)
        self.assertFalse(result["weekly_media"]["date_aligned_with_daily_kpi"])
        self.assertFalse(result["paid_cost"]["roas_eligible"])
        self.assertEqual(result["event_media"]["status"],"not_ready")
    def test_measured_media_from_existing_utm_scan(self):
        utm={"status":"ga4_measured","period":{"2025":["2025-11-21","2025-11-22"],
                                              "2026":["2026-11-20","2026-11-21"]},
             "media_rollup":{"2025":{"groups":[]},"2026":{"groups":[]}}}
        result=media_event(utm,{})
        self.assertEqual(result["status"],"ga4_measured")
        self.assertFalse(result["causal"])
    def test_never_fabricate_roas_from_freshness_only(self):
        result=build(self.ops,self.source,{},self.intel)
        self.assertNotIn("roas",result["weekly_media"])
        self.assertTrue(result["quality"]["reliable_event_days"]==3)
        self.assertFalse(result["quality"]["roas_eligible"])
        self.assertEqual(metrics({"sessions":0,"orders":0,"revenue":0})["cvr_pct"],None)

if __name__=="__main__":
    unittest.main()
