import json,tempfile,unittest
from pathlib import Path
from social_listening import build_social,url
class SocialTests(unittest.TestCase):
    def test_aggregate_only_and_window(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);p=root/"reports/external_signal_cache";p.mkdir(parents=True)
            row={"title":"컬럼비아 바람막이 추천","content":"가격 문의","created_at":"2026-11-17T10:00:00+09:00","url":"https://gall.dcinside.com/board/view/?id=climbing&no=1&page=3","author":"not exported"}
            (p/"dcinside.json").write_text(json.dumps([row,{**row,"url":row["url"].replace("page=3","page=4")},{**row,"created_at":"2026-11-18T10:00:00+09:00","url":row["url"].replace("no=1","no=2")}]),encoding="utf-8")
            q=root/"reports/crema_raw/daily";q.mkdir(parents=True)
            r={"source":"Official","id":"1","created_at":"2026-11-17","rating":2,"text":"사이즈 작아요","height":"sensitive field","product_name":"Jacket"}
            (q/"2026-11-17.json").write_text(json.dumps({"reviews":[r,r]}),encoding="utf-8")
            m=build_social("2026-11-17",root)
            self.assertEqual(m["observed_community_mentions"],1)
            self.assertEqual(m["voc"]["reviews"],1)
            self.assertEqual(m["voc"]["average_rating"],2)
            self.assertEqual(m["voc"]["product_voc"],[])
            self.assertNotIn("sensitive field",json.dumps(m))
            self.assertIsNone(m["share_of_voice"])
            self.assertIsNone(m["wow"])
    def test_missing_not_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=build_social("2026-11-17",Path(tmp))
            self.assertIsNone(m["observed_community_mentions"])
            self.assertIsNone(m["voc"]["reviews"])
    def test_reject_untrusted_url(self):
        self.assertIsNone(url("dcinside","https://dcinside.com.evil.example/1"))
if __name__=="__main__":unittest.main()
