import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path

from scripts.csk_marketing_extensions import (
    competitor_report, launch_report, creative_fatigue, creator_crew
)
from scripts.build_marketing_operations_v3 import build, render_html

DAY = dt.date(2026, 11, 14)


class CompetitorTests(unittest.TestCase):
    def make_source(self, base, updated="2026-11-14 22:00:00"):
        (base/"brand_summary.csv").write_text(
            "brand,total_products,active_products,avg_price,sale_share_pct\n"
            "COLUMBIA,100,90,120000,20.0\n"
            "ARCTERYX,50,50,220000,5.0\n"
        )
        for brand,slug,count in [("COLUMBIA","columbia",100),
                                  ("ARCTERYX","arcteryx",50)]:
            (base/f"checkpoint_{slug}_status.json").write_text(
                json.dumps({"brand":brand,"saved_products":count,
                            "completed":True,"updated_at":updated}))

    def test_reference_fresh_not_price_competitive_claim(self):
        with tempfile.TemporaryDirectory() as folder:
            base=Path(folder)
            self.make_source(base)
            result=competitor_report(DAY,base)
            self.assertEqual(result["status"],"reference_fresh")
            self.assertEqual(len(result["brands"]),2)
            self.assertIn("동일 SKU",result["note"])

    def test_stale_checkpoints_not_current(self):
        with tempfile.TemporaryDirectory() as folder:
            base=Path(folder)
            self.make_source(base,"2026-04-06 10:00:00")
            self.assertEqual(competitor_report(DAY,base)["status"],
                             "partial_reference")

    def test_unverified_scrape_count_blocks_recommendation(self):
        with tempfile.TemporaryDirectory() as folder:
            base=Path(folder)
            self.make_source(base)
            p=base/"checkpoint_columbia_status.json"
            j=json.loads(p.read_text())
            j["saved_products"]=99
            p.write_text(json.dumps(j))
            x=competitor_report(DAY,base)
            self.assertEqual(x["status"],"partial_reference")
            self.assertEqual(len(x["brands"]),1)


class LaunchTests(unittest.TestCase):
    def test_no_launches_are_unverified_not_zero_sales(self):
        with tempfile.TemporaryDirectory() as folder:
            base=Path(folder)
            reg=base/"registry.json"
            reg.write_text('{"launches":[]}')
            self.assertEqual(launch_report(DAY,reg)["status"],"launch_registry_missing")

    def test_launch_window_requires_all_days(self):
        with tempfile.TemporaryDirectory() as folder:
            base=Path(folder)
            reg,metrics=base/"reg.json",base/"private.json"
            reg.write_text(json.dumps({"launches":[{"sku":"C74YM3674342",
                                 "name":"Launch","launch_date":"2026-11-08"}]}))
            rows=[]
            for i in range(7):
                date=dt.date(2026,11,8)+dt.timedelta(days=i)
                rows.append({"date":date.isoformat(),"sku":"C74YM3674342",
                             "pdp_sessions":100,"orders":2,"revenue":200000})
            metrics.write_text(json.dumps({"rows":rows}))
            x=launch_report(DAY,reg,metrics)
            item=x["items"][0]
            self.assertEqual(item["windows"]["d7"]["status"],"complete")
            self.assertEqual(item["windows"]["d7"]["pdp_sessions"],700)
            self.assertEqual(item["windows"]["d14"]["status"],"awaiting_window")
            rows.pop()
            metrics.write_text(json.dumps({"rows":rows}))
            self.assertEqual(launch_report(DAY,reg,metrics)["items"][0]["windows"]["d7"]["status"],
                             "missing_daily_data")


class CreativeTests(unittest.TestCase):
    def test_ctr_drop_is_hypothesis_not_confirmed_fatigue(self):
        with tempfile.TemporaryDirectory() as folder:
            p=Path(folder)/"creatives.json"
            rows=[]
            for lag in range(14):
                date=DAY-dt.timedelta(days=lag)
                rows.append({"date":date.isoformat(), "platform":"meta",
                             "creative_id":"CREATIVE_X",
                             "impressions":500,
                             "clicks":5 if lag<=6 else 15})
            p.write_text(json.dumps({"rows":rows}))
            r=creative_fatigue(DAY,p)
            self.assertEqual(r["status"],"analyzed")
            self.assertEqual(len(r["flags"]),1)
            self.assertLess(r["flags"][0]["ctr_change_pct"],-25)
            self.assertEqual(r["flags"][0]["status"],"creative_fatigue_hypothesis")

    def test_missing_creatives_are_not_claimed_healthy(self):
        with tempfile.TemporaryDirectory() as folder:
            r=creative_fatigue(DAY,Path(folder)/"missing.json")
            self.assertEqual(r["status"],"not_connected")


class CreatorTests(unittest.TestCase):
    def test_actual_and_estimated_remain_separated(self):
        with tempfile.TemporaryDirectory() as folder:
            file=Path(folder)/"crew.json"
            file.write_text(json.dumps({"rows":[
                {"mission":"2차","format":"REEL","creator_count":8,"posts":8,
                 "views":100000,"engagements":4000,"cash_cost_krw":2000000,
                 "estimated":False},
                {"mission":"2차","format":"FEED","creator_count":5,"posts":5,
                 "views":60000,"engagements":1000,"estimated":True}
            ]}))
            report=creator_crew(DAY,file)
            self.assertEqual(report["status"],"aggregate_available")
            self.assertEqual(len(report["groups"]),1)
            self.assertEqual(report["estimated_groups"],1)
            self.assertEqual(report["groups"][0]["cash_only_cpv_krw"],20.0)
            self.assertTrue(report["groups"][0]["actual_data"])

    def test_small_member_groups_are_not_published(self):
        with tempfile.TemporaryDirectory() as folder:
            file=Path(folder)/"crew.json"
            file.write_text(json.dumps({"rows":[{
                "mission":"3차", "format":"FEED","creator_count":1,
                "posts":1,"views":5000,"engagements":200
            }]}))
            self.assertEqual(creator_crew(DAY,file)["groups"],[])


if __name__=="__main__":
    unittest.main()
