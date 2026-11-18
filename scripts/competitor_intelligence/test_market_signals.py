import unittest
from market_signals import extract_market_signals, classify, public_topic

URL="https://www.example.com/membership"

class MarketSignalTests(unittest.TestCase):
    def test_distinguish_signup_and_first_purchase(self):
        rows=extract_market_signals("신규 가입 시 10% 쿠폰\n첫 구매 감사 10,000M 마일리지",[],URL)
        self.assertTrue(any(r["category"]=="신규 가입" and "10%" in r["numbers"] for r in rows))
        self.assertTrue(any(r["category"]=="첫 구매" for r in rows))

    def test_split_heading(self):
        rows=extract_market_signals("신규회원 혜택\n20,000원 할인쿠폰",[],URL)
        self.assertTrue(any(r["category"]=="신규 가입" and "20,000원" in r["numbers"] for r in rows))

    def test_generic_shipping_not_incident(self):
        self.assertIsNone(classify("배송 지연이 발생할 수 있는 경우 문의하시기 바랍니다",URL,URL))

    def test_explicit_notice(self):
        r=classify("11월 배송 지연 안내", "https://www.example.com/notice/10","https://www.example.com/notice")
        self.assertEqual(r["category"],"운영 이슈")

    def test_reject_private_pages_and_external_links(self):
        self.assertFalse(public_topic("https://www.example.com/mypage/member","회원 혜택"))
        rows=extract_market_signals("",[{"url":"https://other.example.com/benefit","label":"신규 가입 시 10% 쿠폰"}],URL)
        self.assertEqual(rows,[])

    def test_generic_membership_nav_not_reward(self):
        self.assertIsNone(classify("신규 회원가입 안내 및 이용약관",URL,URL))

if __name__=="__main__":
    unittest.main()
