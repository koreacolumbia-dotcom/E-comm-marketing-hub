"""Guard common GA4 query reuse, real official PDP images and 7-day calendar layout."""
from __future__ import annotations

import datetime as dt
import importlib.util
import json
import re
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]

def load_module(name,relative):
    spec=importlib.util.spec_from_file_location(name,ROOT/relative)
    module=importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module

BQ=load_module("csk_shared_ga4", "scripts/csk_ga4_cost_guard.py")
PRODUCT=load_module("csk_official_images","scripts/personal_intelligence/product_signal_hub.py")

class SharedBigQueryAndImageTests(unittest.TestCase):
    def test_same_period_ga4_aggregate_is_reused(self):
        original=json.loads((ROOT/"reports/decision_health/weekly_ga4_aligned.json").read_text(encoding="utf-8"))
        end=dt.date.fromisoformat(original["report_date"])
        self.assertTrue(BQ.matching_shared_evidence(original,end))
        self.assertFalse(BQ.matching_shared_evidence(original,end-dt.timedelta(days=1)))
        copy=dict(original);copy["status"]="blocked_budget"
        self.assertFalse(BQ.matching_shared_evidence(copy,end))
        copy=dict(original);copy["basket_distribution"]=None
        self.assertFalse(BQ.matching_shared_evidence(copy,end))
        copy=dict(original);copy["period"]=dict(original["period"],previous_start="2026-01-01")
        self.assertFalse(BQ.matching_shared_evidence(copy,end))

    def test_existing_pdp_image_is_a_real_official_store_asset(self):
        daily_dir=ROOT/"reports/daily_digest/data/daily"
        files=sorted(daily_dir.glob("20??-??-??.json"))
        self.assertTrue(files)
        daily=json.loads(files[-1].read_text(encoding="utf-8"))
        sample=(daily["product_scopes"]["current_name"])[0]
        image=PRODUCT.official_product_image(sample)
        self.assertIsNotNone(image)
        self.assertIn("styleship.com/assets/columbia/data/",image.lower())
        self.assertIsNone(PRODUCT.official_product_image({"image_url":"https://example.com/fake.jpg"}))
        self.assertIsNone(PRODUCT.official_product_image({"image_url":"javascript:alert(1)"}))
        self.assertIsNone(PRODUCT.official_product_image({"image_url":None}))

    def test_manifest_one_verified_color_per_style(self):
        doc=json.loads((ROOT/"reports/decision_health/official_product_images.json").read_text(encoding="utf-8"))
        self.assertEqual(len(doc["products"]),len({p["name"] for p in doc["products"]}))
        self.assertGreaterEqual(len(doc["products"]),4)
        for record in doc["products"]:
            with self.subTest(name=record["name"]):
                self.assertIsNotNone(PRODUCT.official_product_image(record))
                self.assertTrue(record["product_url"].startswith("https://www.columbiakorea.co.kr/"))
                self.assertTrue(record["sku"])

    def test_summary_cleaned_and_daily_excluded(self):
        summary=(ROOT/"reports/index.html").read_text(encoding="utf-8")
        self.assertNotIn("<!-- DASHBOARD-EXPERIENCE-START -->",summary)
        self.assertNotIn("<!-- DASHBOARD-OPS-START -->",summary)
        self.assertIn('id="dailyKpi"',summary)
        self.assertIn('id="weeklyKpi"',summary)
        daily=(ROOT/"reports/daily_intelligence/index.html").read_text(encoding="utf-8")
        self.assertNotIn("csk-saas-unified.css",daily)

    def test_calendar_has_one_seven_column_grid_and_mobile_override(self):
        page=(ROOT/"reports/competitor_sale_calendar/index.html").read_text(encoding="utf-8")
        self.assertIn('<span>일</span><span>월</span><span>화</span><span>수</span><span>목</span><span>금</span><span>토</span>',page)
        css=(ROOT/"assets/csk-saas-unified.css").read_text(encoding="utf-8")
        self.assertIn('data-csk-route="sale_calendar"',css)
        self.assertIn("grid-template-columns:repeat(7,minmax(0,1fr))!important",css)
        self.assertIn(".calendar-scroll .grid",css)
        self.assertIn("min-width:0!important",css)

if __name__=="__main__":
    unittest.main()
