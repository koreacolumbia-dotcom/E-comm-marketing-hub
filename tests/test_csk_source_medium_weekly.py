import importlib.util
import unittest
import tempfile
import json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location("sm",ROOT/"scripts/csk_source_medium_weekly.py")
m=importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
OPS={"report_date":"2026-11-20","weekly":{"status":"ready","period":"2026-11-14 ~ 2026-11-20",
    "comparison_period":"2026-11-07 ~ 2026-11-13",
    "current":{"status":"complete"},"previous":{"status":"complete"}}}


class SourceMediumReportTests(unittest.TestCase):
    def test_exact_operational_weeks(self):
        win=m.window(OPS)
        self.assertEqual(win["previous"]["start"],"2026-11-07")
        self.assertEqual(win["current"]["end"],"2026-11-20")
        self.assertIsNone(m.window({**OPS,"report_date":"2026-11-21"}))
        self.assertIsNone(m.window({**OPS,"weekly":{**OPS["weekly"],"status":"partial"}}))

    def test_same_period_metrics(self):
        old=[{"sessionSourceMedium":"google / organic","sessions":100,"transactions":2,
              "purchaseRevenue":100000,"expected_sessions":150},
             {"sessionSourceMedium":"(direct) / (none)","sessions":50,"transactions":1,
              "purchaseRevenue":50000,"expected_sessions":150}]
        now=[{"sessionSourceMedium":"google / organic","sessions":120,"transactions":6,
              "purchaseRevenue":400000,"expected_sessions":160},
             {"sessionSourceMedium":"(direct) / (none)","sessions":40,"transactions":1,
              "purchaseRevenue":55000,"expected_sessions":160}]
        j=m.summarize(now,old,m.window(OPS),{"per_query_cap":m.PER_QUERY_CAP})
        self.assertEqual(j["status"],"ga4_measured")
        self.assertEqual(j["totals"]["current"]["sessions"],160)
        self.assertEqual(j["totals"]["previous"]["revenue"],150000)
        google=next(row for row in j["rows"] if row["source_medium"]=="google / organic")
        self.assertEqual(google["current"]["cvr_pct"],5)
        self.assertEqual(google["sessions_change_pct"],20)
        self.assertEqual(google["cvr_change_pp"],3)
        self.assertEqual(google["revenue_delta"],300000)

    def test_no_user_identifiers_or_invalid_values(self):
        self.assertEqual(m.source_label("person@example.com / email"),"(restricted) / (restricted)")
        self.assertEqual(m.source_label("https://x.com?a=b / referral"),"(restricted) / (restricted)")
        self.assertEqual(m.source_label("naver / cpc"),"naver / cpc")
        with self.assertRaises(ValueError):
            m.summarize([{"sessionSourceMedium":"google / cpc","sessions":2,
                "transactions":1,"purchaseRevenue":100,"expected_sessions":3}],
                [{"sessionSourceMedium":"google / cpc","sessions":1,
                  "transactions":0,"purchaseRevenue":0,"expected_sessions":1}],m.window(OPS),{})

    def test_guard_and_coverage(self):
        self.assertEqual(m.PER_QUERY_CAP,3*1024**3+8*1024**2)
        self.assertEqual(m.TOTAL_DRY_CAP,6*1024**3)
        self.assertLess(m.PER_QUERY_CAP, m.TOTAL_DRY_CAP)

    def test_zero_query_cache_reuse_reconciles_without_inventing_source(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t)
            dates=m.window(OPS)
            for period in ("previous","current"):
                start=m.dt.date.fromisoformat(dates[period]["start"])
                for k in range(7):
                    day=(start+m.dt.timedelta(days=k)).isoformat()
                    row={"mode":"daily","end_date":day,
                         "traffic_metric_source":"ga4_bigquery_session_key_v1",
                         "kpis":{"sessions":100,"orders":3,"revenue":1500},
                         "channels":{"Paid Ad":{"sessions":60,"orders":2,"revenue":1000},
                                     "Organic Traffic":{"sessions":40,"orders":1,"revenue":500}},
                         "channel_detail_map":{
                            "Paid Ad":[{"sub_channel":"Paid Search · 브랜드",
                                        "sessions":60,"orders":2,"purchaseRevenue":1000}],
                            "Organic Traffic":[{"sub_channel":"Search · 오가닉",
                                                "sessions":38,"orders":1,"purchaseRevenue":500}]}}
                    (root/(day+".json")).write_text(json.dumps(row))
            out=m.cached_subchannel_fallback(dates,root)
            self.assertEqual(out["status"],"ga4_subchannel_aggregated")
            self.assertEqual(out["totals"]["current"]["sessions"],700)
            self.assertTrue(any("미배분" in row["source_medium"] for row in out["rows"]))
            self.assertFalse(out["quality"]["source_medium_exact"])
            self.assertEqual(out["cost"]["billed_bytes"],0)
            self.assertEqual(len(out["quality"]["unallocated_rows"]),14)
            (root/("2026-11-20.json")).unlink()
            blocked=m.cached_subchannel_fallback(dates,root)
            self.assertEqual(blocked["status"],"fallback_incomplete")
            self.assertEqual(blocked["rows"],[])


if __name__=="__main__":
    unittest.main()
