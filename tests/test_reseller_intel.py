"""Smoke tests for new authorized reseller intelligence pipeline."""
import csv
import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from scripts.build_reseller_intel import (
    KST, LISTING_FIELDS, SELLER_FIELDS, build,
)


def write_csv(path, columns, rows):
    with path.open("w", encoding="utf-8", newline="") as stream:
        w = csv.DictWriter(stream, fieldnames=columns)
        w.writeheader()
        w.writerows(rows)


class ResellerIntelTest(unittest.TestCase):
    def test_empty_inputs_do_not_claim_live_data(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            write_csv(base / "listings.csv", LISTING_FIELDS, [])
            write_csv(base / "sellers.csv", SELLER_FIELDS, [])
            result = build(base / "listings.csv", base / "sellers.csv", base / "out", 3)
            self.assertEqual(result["status"], "REQUIRES_CURRENT_SOURCE")
            self.assertEqual(result["total_sellers"], 0)
            self.assertTrue((base / "out" / "index.html").is_file())
            self.assertIn("과거 최저가", (base / "out" / "index.html").read_text())

    def test_seller_product_and_other_brands_from_confirmed_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            today = datetime.now(KST).isoformat(timespec="seconds")
            latest = [
                {
                    "observed_at": today, "seller_name": "TESTSTORE", "seller_id": "t01",
                    "channel": "네이버 스마트스토어", "store_url": "https://example.com/store",
                    "brand": "컬럼비아", "product_code": "C99TEST0001",
                    "product_name": "테스트 자켓", "category": "재킷", "price_krw": "99,000",
                    "product_url": "https://example.com/columbia", "source_url": "https://example.com/columbia",
                    "source_type": "unit_test", "verification_status": "verified_public",
                },
                {
                    "observed_at": today, "seller_name": "TESTSTORE", "seller_id": "t01",
                    "channel": "네이버 스마트스토어", "store_url": "https://example.com/store",
                    "brand": "노스페이스", "product_code": "NP99",
                    "product_name": "테스트 플리스", "category": "플리스", "price_krw": "80,000",
                    "product_url": "https://example.com/other", "source_url": "https://example.com/other",
                    "source_type": "unit_test", "verification_status": "verified_public",
                },
            ]
            seller = {
                "seller_name": "TESTSTORE", "seller_id": "t01", "channel": "네이버 스마트스토어",
                "business_name": "TEST BUSINESS", "business_address": "공개된 사업자 주소(테스트)",
                "business_info_url": "https://example.com/legal",
                "return_address": "공개된 반품지(테스트)",
                "return_info_url": "https://example.com/returns",
                "verification_status": "verified_public",
            }
            write_csv(base / "listings.csv", LISTING_FIELDS, latest)
            write_csv(base / "sellers.csv", SELLER_FIELDS, [seller])
            result = build(base / "listings.csv", base / "sellers.csv", base / "out", 3)
            self.assertEqual(result["status"], "CURRENT_SOURCE_IMPORTED")
            self.assertEqual(result["current_columbia_listings"], 1)
            with (base / "out" / "seller_master.csv").open(encoding="utf-8-sig") as fp:
                sellers = list(csv.DictReader(fp))
            self.assertEqual(len(sellers), 1)
            self.assertEqual(sellers[0]["columbia_focus_codes"], "C99TEST0001(1)")
            self.assertEqual(sellers[0]["other_brands_observed"], "노스페이스")
            self.assertEqual(sellers[0]["business_address"], "공개된 사업자 주소(테스트)")

    def test_stale_data_keeps_history_without_current_claim(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            outdated = {
                "observed_at": (datetime.now(KST) - timedelta(days=10)).isoformat(timespec="seconds"),
                "seller_name": "TESTSTORE", "brand": "컬럼비아",
                "product_code": "C99TEST0001", "price_krw": "99000",
                "product_url": "https://example.com/item",
            }
            write_csv(base / "listings.csv", LISTING_FIELDS, [outdated])
            write_csv(base / "sellers.csv", SELLER_FIELDS, [])
            result = build(base / "listings.csv", base / "sellers.csv", base / "out", 3)
            self.assertEqual(result["status"], "REQUIRES_CURRENT_SOURCE")
            self.assertEqual(result["total_observed_listings"], 1)
            self.assertEqual(result["current_listings"], 0)


if __name__ == "__main__":
    unittest.main()
