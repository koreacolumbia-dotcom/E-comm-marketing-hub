import unittest
from adapters import normalize_cards
class PriceTests(unittest.TestCase):
    def card(self,text):
        return {"name":"테스트 다운","text":text,"url":"https://example.com/product/1","image":""}
    def test_normal(self):
        r=normalize_cards([self.card("테스트 다운 439,000원 379,000원 14%")])
        self.assertEqual(r[0]["sale_price"],379000)
    def test_reversed(self):
        r=normalize_cards([self.card("테스트 다운 379,000원 439,000원 14%")])
        self.assertEqual(r[0]["regular_price"],439000)
    def test_wrong_discount(self):
        self.assertEqual(normalize_cards([self.card("테스트 다운 439,000원 379,000원 40%")]),[])
    def test_ambiguous(self):
        self.assertEqual(normalize_cards([self.card("테스트 다운 439,000원 379,000원 359,000원 14%")]),[])
    def test_duplicates(self):
        r=self.card("테스트 다운 439,000원 379,000원 14%")
        self.assertEqual(len(normalize_cards([r,r])),1)
    def test_semantic_prices_without_rate(self):
        r=self.card("테스트 다운 439,000 379,000")
        r.update(regular_text="439,000",sale_text="379,000")
        self.assertEqual(normalize_cards([r])[0]["discount_pct"],13.67)
    def test_explicit_labels_without_rate(self):
        r=self.card("테스트 다운 정상가 439,000원 판매가 379,000원")
        self.assertEqual(normalize_cards([r])[0]["sale_price"],379000)
    def test_payback_not_discount(self):
        r=self.card("10%페이백 테스트 다운 30%343,000원 490,000원")
        self.assertEqual(normalize_cards([r])[0]["discount_pct"],30)
    def test_unlabelled_pair_without_rate(self):
        self.assertEqual(normalize_cards([self.card("테스트 다운 439,000원 379,000원")]),[])
    def test_coupon_price_rejected(self):
        self.assertEqual(normalize_cards([self.card("테스트 다운 439,000원 쿠폰 적용가 379,000원 14%")]),[])
if __name__=="__main__": unittest.main()
