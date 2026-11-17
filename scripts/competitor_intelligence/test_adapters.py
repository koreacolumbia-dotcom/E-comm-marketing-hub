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
if __name__=="__main__": unittest.main()
