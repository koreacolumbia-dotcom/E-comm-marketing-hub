import unittest
from report_model import competitive_discount_matrix

def card(name, url, regular, sale, rate, evidence="paired prices"):
    return dict(name=name,url=url,regular_price=regular,sale_price=sale,discount_pct=rate,
                evidence=evidence,status="candidate_same_card")

class DiscountMatrixTest(unittest.TestCase):
    def test_missing_is_not_zero(self):
        result=competitive_discount_matrix(None)
        self.assertEqual(len(result["brand_rows"]),11)
        self.assertEqual(result["verified_cells"],[])
        self.assertTrue(all(r["sample_discount_median_pct"] is None for r in result["brand_rows"]))

    def test_variant_dedup_and_arithmetic(self):
        a=card("M'S LIGHT PACK DOWN JACKET","https://example.com/a",349000,279200,20)
        b=card("M'S LIGHT PACK DOWN JACKET","https://example.com/b",349000,279200,20)
        c=card("M'S REMASTER DOWN JACKET","https://example.com/c",529000,476100,10,"회원전용")
        bad=card("Wrong","https://example.com/d",500000,400000,70)
        result=competitive_discount_matrix({"price_candidates":[{"brand":"TNF","paired_discount_cards":[a,a,b,c,bad]}]})
        r=next(r for r in result["brand_rows"] if r["brand"]=="TNF")
        self.assertEqual((r["paired_cards"],r["style_count"],r["rejected"]),(3,2,1))
        self.assertEqual(r["sample_discount_median_pct"],15)
        self.assertEqual(result["styles"][1]["variants"][0]["condition"],"member_only")
        self.assertFalse(result["owned_baseline_available"])

    def test_explicit_color_and_single_price(self):
        cards=[card("시티캠퍼 여성 후드 헤비 다운 자켓 "+color,"https://example.com/"+str(i),439000,379000,14)
               for i,color in enumerate(("Light Beige","Ivory"))]
        result=competitive_discount_matrix({"price_candidates":[
            {"brand":"SNOW PEAK","paired_discount_cards":cards},
            {"brand":"NEPA","structured_product_prices":[{"name":"경량 다운","url":"https://example.com/p","price":319000,"currency":"KRW"}]}]})
        self.assertEqual(result["competitor_style_count"],1)
        self.assertEqual(result["styles"][0]["discount_pct_median"],13.67)
        nepa=next(r for r in result["brand_rows"] if r["brand"]=="NEPA")
        self.assertIsNone(nepa["sample_discount_median_pct"])
        self.assertEqual(nepa["status"],"price_only")

if __name__ == "__main__":
    unittest.main()
