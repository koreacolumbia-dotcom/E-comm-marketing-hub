import unittest
from official_price import extract,allowed_url
from price_impact import price_impacts
from product_card import enrich_cards

class PriceTests(unittest.TestCase):
    def test_exact_sku(self):
        html='<script type="application/ld+json">{"@type":"Product","sku":"ABC","offers":{"price":89000,"priceCurrency":"KRW"}}</script>'
        u="https://www.columbiakorea.co.kr/product"
        self.assertEqual(extract(html,"ABC",u,"2026-11-18")["sale_price"],89000)
        self.assertFalse(extract(html,"OTHER",u,"2026-11-18")["verified"])
        self.assertIsNone(extract(html,"ABC",u,"2026-11-18")["regular_price"])
    def test_hosts(self):
        self.assertTrue(allowed_url("https://www.columbiakorea.co.kr/product"))
        self.assertFalse(allowed_url("https://www.columbiakorea.co.kr.example.org/"))
    def test_missing_windows(self):
        rows=[{"date":"2026-11-17","sale_price":100,"verified":True,"source_url":"x"},{"date":"2026-11-18","sale_price":80,"verified":True,"source_url":"x"}]
        result=price_impacts(rows,{})[0]
        self.assertAlmostEqual(result["price_change_pct"],-20)
        self.assertIsNone(result["effects"]["sales_units"]["pct"])
    def test_missing_evidence(self):
        card=enrich_cards([{"sku":"ABC"}],{},[])[0]
        self.assertEqual(card["factors"]["inventory"]["status"],"source_required")
        self.assertEqual(card["diagnosis"]["causal_status"],"unverified")
if __name__=="__main__": unittest.main()
