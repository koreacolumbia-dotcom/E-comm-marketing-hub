import csv
import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path
from scripts.build_competitor_sale_calendar import (
    read_snapshots, observed_events, build, benefit, extract_period,
    safe_url, read_registry, date, SALE_PATTERN,
)

ASOF = dt.date(2026,11,15)
HEADER = [
    "date","brand_key","brand_name","rank","title","href","href_clean",
    "plan_start","plan_end","landing_title","landing_summary",
    "landing_text_status","is_fallback"
]


def write_csv(root, day, rows, clock="053810"):
    name="hero_main_banners_"+day.strftime("%Y%m%d")+"_"+clock+".csv"
    with (root/name).open("w",encoding="utf-8-sig",newline="") as f:
        w=csv.DictWriter(f,fieldnames=HEADER)
        w.writeheader()
        for row in rows:
            w.writerow({"date":day.isoformat(),**row})
    return name


class CaptureTests(unittest.TestCase):
    def sample(self):
        return {
            "brand_key":"tnf","brand_name":"The North Face","rank":"1",
            "title":"메인 배너","landing_title":"노스페이스 블랙프라이데이 2026 최대 30% 할인",
            "landing_summary":"11월 12일~12월 3일 공식몰 블랙프라이데이 최대 30% 할인",
            "href_clean":"https://www.thenorthfacekorea.co.kr/event/view/eventCategoryNF/93344",
            "plan_start":"2025-09-21","plan_end":"2028-09-20",
            "landing_text_status":"ok","is_fallback":"0"
        }

    def test_landing_period_beats_unreliable_plan_dates(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            write_csv(root,ASOF,[self.sample()])
            rows,paths=read_snapshots(root,ASOF)
            events=observed_events(rows)
            self.assertEqual(len(paths),1)
            self.assertEqual(len(events),1)
            self.assertEqual(events[0]["kind"],"verified_period")
            self.assertEqual(events[0]["start_date"],"2026-11-12")
            self.assertEqual(events[0]["end_date"],"2026-12-03")
            self.assertNotEqual(events[0]["start_date"],"2025-09-21")

    def test_sale_banner_without_period_observed_only(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            day=dt.date(2026,11,14)
            item={"brand_key":"blackyak","title":"2026 BLACK FESTA 놓칠 수 없는 특가",
                  "href_clean":"https://rF0pCk9RNUKFcOSlGwiXrw.adtouch.adbrix.io/api/v1/click/track"}
            write_csv(root,day,[item])
            write_csv(root,ASOF,[item])
            events=observed_events(read_snapshots(root,ASOF)[0])
            self.assertEqual(len(events),1)
            self.assertEqual(events[0]["kind"],"observed_banner")
            self.assertEqual(events[0]["observed_on"],["2026-11-14","2026-11-15"])
            self.assertIsNone(events[0]["start_date"])
            self.assertIsNone(events[0]["end_date"])
            self.assertIsNone(events[0]["source_url"])

    def test_non_sale_banners_not_misclassified(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            write_csv(root,ASOF,[{
                "brand_key":"salomon","title":"WINTER SHOES COLLECTION",
                "landing_title":"WINTER SHOES COLLECTION",
                "href_clean":"https://salomon.co.kr/collections/winter"}])
            self.assertEqual(read_snapshots(root,ASOF)[0],[])

    def test_landing_fallback_does_not_generate_period(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            item=self.sample()
            item["is_fallback"]="1"
            write_csv(root,ASOF,[item])
            e=observed_events(read_snapshots(root,ASOF)[0])[0]
            self.assertEqual(e["kind"],"observed_banner")

    def test_latest_snapshot_only_one_per_day(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            first=self.sample()
            write_csv(root,ASOF,[first],clock="050001")
            second={"brand_key":"blackyak","title":"2026 BLACK FESTA 특가",
                    "href_clean":"https://www.byn.kr/blackyak"}
            write_csv(root,ASOF,[second],clock="060001")
            rows,files=read_snapshots(root,ASOF)
            self.assertEqual(len(rows),1)
            self.assertEqual(len(files),1)
            self.assertEqual(rows[0]["brand"],"blackyak")

    def test_ad_redirects_rejected_official_url_allowed(self):
        self.assertIsNone(safe_url("http://www.byn.kr/",brand="blackyak"))
        self.assertIsNone(safe_url("https://track.example.org/redirect",brand="tnf"))
        self.assertIsNotNone(safe_url("https://www.thenorthfacekorea.co.kr/event/view/x",brand="tnf"))


class RegistryTests(unittest.TestCase):
    def test_official_ends_and_open_ends(self):
        with tempfile.TemporaryDirectory() as folder:
            reg=Path(folder)/"registry.json"
            reg.write_text(json.dumps({"events":[
                {"id":"snowpeak-down-2026", "brand":"snowpeak",
                 "title":"다운 컬렉션 최대 9만원 즉시 할인",
                 "start_date":"2026-10-22","end_date":None,
                 "source_url":"https://www.snowpeakstore.co.kr/board/list.php?bdId=event",
                 "verification":"official_event_page"},
                {"id":"tnf-sale-2026","brand":"tnf",
                 "title":"노스페이스 블랙프라이데이 최대 30% 할인",
                 "start_date":"2026-11-12","end_date":"2026-12-04",
                 "source_url":"https://www.ssfshop.com/special/125470/view",
                 "verification":"official_retailer_page"},
                {"id":"fabricated","brand":"salomon","title":"블랙프라이데이",
                 "start_date":"2026-11-12","end_date":"2026-11-20",
                 "source_url":"https://unverified.example.org/offer",
                 "verification":"official_event_page"}
            ]}))
            events=read_registry(reg,ASOF)
            self.assertEqual(len(events),2)
            self.assertEqual({x["kind"] for x in events},{"verified_period","verified_start"})
            self.assertIsNone(next(x for x in events if x["brand"]=="snowpeak")["end_date"])

    def test_invalid_future_end_suppressed(self):
        with tempfile.TemporaryDirectory() as folder:
            reg=Path(folder)/"events.json"
            reg.write_text(json.dumps({"events":[
                {"id":"unreal-sale-2026","brand":"tnf",
                 "title":"초특가 세일","start_date":"2026-10-15","end_date":"2999-01-01",
                 "source_url":"https://www.thenorthfacekorea.co.kr/",
                 "verification":"official_event_page"}]}))
            self.assertEqual(read_registry(reg,ASOF),[])


class EndToEnd(unittest.TestCase):
    def test_build_observation_is_not_assumed_continuous(self):
        with tempfile.TemporaryDirectory() as folder:
            base=Path(folder)
            write_csv(base,dt.date(2026,11,13),[{
                "brand_key":"blackyak","title":"K-SALE FESTA 15%할인",
                "href_clean":"https://www.byn.kr/blackyak"}])
            write_csv(base,ASOF,[{
                "brand_key":"blackyak","title":"K-SALE FESTA 15%할인",
                "href_clean":"https://www.byn.kr/blackyak"}])
            result=build(ASOF,base,base/"missing.json")
            self.assertEqual(result["status"],"fresh")
            self.assertEqual(result["counts"]["events"],1)
            item=result["events"][0]
            self.assertEqual(item["kind"],"observed_banner")
            self.assertEqual(item["observed_on"],["2026-11-13","2026-11-15"])
            self.assertNotIn("2026-11-14",item["observed_on"])
            self.assertIsNone(item["end_date"])


if __name__=="__main__":
    unittest.main()
