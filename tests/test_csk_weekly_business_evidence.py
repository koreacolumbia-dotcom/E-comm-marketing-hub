import importlib.util
import unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location("business",ROOT/"scripts/csk_weekly_business_evidence.py")
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


def dataset():
    from datetime import date,timedelta
    prev=date(2026,11,7)
    days={}
    for i in range(14):
        day=(prev+timedelta(days=i)).isoformat()
        ch={"Paid Ad":{"sessions":10,"orders":1,"revenue":100},
            "Organic Traffic":{"sessions":10,"orders":1,"revenue":200},
            "etc":{"sessions":10,"orders":1 if i<7 else 0,"revenue":100 if i<7 else 0}}
        total={"sessions":30,"orders":3 if i<7 else 2,"revenue":400 if i<7 else 300}
        days[day]={"mode":"daily","end_date":day,"kpis":total,"channels":{**ch,"Total":total}}
    ops={"report_date":"2026-11-20","weekly":{
        "status":"ready","period":"2026-11-14 ~ 2026-11-20",
        "comparison_period":"2026-11-07 ~ 2026-11-13",
        "current":{"status":"complete","total":{"sessions":210,"orders":14,"revenue":2100}},
        "previous":{"status":"complete","total":{"sessions":210,"orders":21,"revenue":2800}}}}
    return ops,days


class EvidenceTest(unittest.TestCase):
    def test_exact_calendar_window_reconciles_and_marks_etc(self):
        ops,days=dataset()
        x=m.build(ops,days.get)
        self.assertEqual(x["status"],"reconciled_with_taxonomy_warning")
        self.assertEqual(x["net_revenue_delta"],-700)
        self.assertEqual(sum(row["delta_revenue"] for row in x["channels"]),-700)
        self.assertEqual(x["quality"]["daily_count"],14)
        self.assertTrue(x["quality"]["revenue_reconciled"])
        self.assertEqual(x["quality"]["taxonomy_warnings"][0]["channel"],"etc")

    def test_missing_date_does_not_create_fake_zero_revenue(self):
        ops,days=dataset();days.pop("2026-11-12")
        x=m.build(ops,days.get)
        self.assertEqual(x["status"],"source_incomplete")
        self.assertIn("2026-11-12",x["quality"]["missing_dates"])
        self.assertFalse(x["channels"])

    def test_daily_channel_sessions_are_not_used_for_cvr_when_nonadditive(self):
        ops,days=dataset()
        days["2026-11-10"]["channels"]["Paid Ad"]["sessions"]=19
        x=m.build(ops,days.get)
        self.assertFalse(x["quality"]["channel_session_comparable"])
        self.assertIn("2026-11-10",x["quality"]["daily_channel_session_mismatch_dates"])
        self.assertEqual(x["status"],"reconciled_with_taxonomy_warning")

    def test_nonmatching_total_or_window_blocks_ranking(self):
        ops,days=dataset()
        ops["weekly"]["current"]["total"]["revenue"]=2099
        self.assertEqual(m.build(ops,days.get)["status"],"weekly_total_mismatch")
        ops["weekly"]["current"]["total"]["revenue"]=2100
        ops["weekly"]["comparison_period"]="2026-11-08 ~ 2026-11-14"
        self.assertEqual(m.build(ops,days.get)["status"],"source_incomplete")

    def test_fails_without_cross_channel_revenue_or_order_reconciliation(self):
        ops,days=dataset()
        days["2026-11-20"]["channels"]["Paid Ad"]["revenue"]=99
        self.assertEqual(m.build(ops,days.get)["status"],"source_incomplete")


if __name__=="__main__":unittest.main()
