import unittest
from scripts.build_festival_report_snapshot import build,event_days,weekly_channels,media_event,metrics,diagnose_drivers,shapley_event

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
    def test_funnel_driver_breakdown_reconciles_without_causal_claims(self):
        event=event_days(self.ops,{})
        d=shapley_event(event["totals"])
        self.assertEqual(d["status"],"reconciled")
        parts=d["terms"]
        self.assertAlmostEqual(sum(v["amount_krw"] for v in parts.values()),800,delta=2)
        self.assertIn("not causal",d["note"])
    def test_media_ranked_drivers_only_same_event_days(self):
        event=event_days(self.ops,{})
        groups1=[
            {"id":"naver_brand_search","label":"네이버 브랜드검색 CPC",
             "revenue":2000,"orders":15,"sessions":1500},
            {"id":"referral","label":"레퍼럴","revenue":1400,"orders":12,"sessions":1200}]
        groups2=[
            {"id":"naver_brand_search","label":"네이버 브랜드검색 CPC",
             "revenue":3300,"orders":21,"sessions":1800},
            {"id":"referral","label":"레퍼럴","revenue":900,"orders":9,"sessions":1200}]
        media={"status":"ga4_measured",
            "period":{"2025":["2025-11-21","2025-11-23"],
                      "2026":["2026-11-20","2026-11-22"]},
            "years":{"2025":{"totals":{"revenue":3400,"orders":27,"sessions":2700},
                             "groups":groups1},
                     "2026":{"totals":{"revenue":4200,"orders":30,"sessions":3000},
                             "groups":groups2}}}
        # Daily test fixture order/session totals differ from the mocked media.
        # Use same totals to test positive channel reconciliation explicitly.
        event["totals"]["previous"]["sessions"]=2700
        event["totals"]["previous"]["orders"]=27
        event["totals"]["current"]["sessions"]=3000
        event["totals"]["current"]["orders"]=30
        result=diagnose_drivers(event,media,{"roas_eligible":False})
        self.assertEqual(result["status"],"reconciled")
        self.assertEqual(result["channel_total_change_krw"],800)
        self.assertTrue(result["event_media_aligned"])
        self.assertEqual(result["channel_gains"][0]["id"],"naver_brand_search")
        self.assertEqual(result["channel_losses"][0]["id"],"referral")
        self.assertFalse(result["causal"])
        media["period"]["2026"]=["2026-11-20","2026-11-21"]
        result=diagnose_drivers(event,media,{"roas_eligible":False})
        self.assertFalse(result["event_media_aligned"])
        self.assertEqual(result["status"],"funnel_only")
        self.assertEqual(result["media_unavailable_reason"],"행사 채널 원본 기간과 누적 KPI 집계 기간 불일치")
    def test_media_totals_mismatch_never_publish_unsupported_channel_drivers(self):
        event=event_days(self.ops,{})
        media={"status":"ga4_measured",
               "period":{"2025":["2025-11-21","2025-11-23"],
                         "2026":["2026-11-20","2026-11-22"]},
               "years":{"2025":{"totals":{"revenue":3400,"orders":27,"sessions":2700},
                                "groups":[]},
                        "2026":{"totals":{"revenue":4201,"orders":30,"sessions":3000},
                                "groups":[]}}}
        result=diagnose_drivers(event,media,{})
        self.assertEqual(result["status"],"funnel_only")
        self.assertFalse(result["event_media_aligned"])
        self.assertNotIn("channel_bridge",result)

    def test_never_fabricate_roas_from_freshness_only(self):
        result=build(self.ops,self.source,{},self.intel)
        self.assertNotIn("roas",result["weekly_media"])
        self.assertTrue(result["quality"]["reliable_event_days"]==3)
        self.assertFalse(result["quality"]["roas_eligible"])
        self.assertEqual(metrics({"sessions":0,"orders":0,"revenue":0})["cvr_pct"],None)

if __name__=="__main__":
    unittest.main()
