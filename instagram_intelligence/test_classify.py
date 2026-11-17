import unittest
import datetime as dt
from zoneinfo import ZoneInfo
from classify import classify

class ClassifyTests(unittest.TestCase):
    def test_unknown_date_not_zero(self):
        result=classify({'shortcode':'A','evidence':[{'title':'컬럼비아 후기'}]})
        self.assertIsNone(result['within_48h'])
        self.assertEqual(result['verification'],'candidate_only')
    def test_official_excluded_category(self):
        result=classify({'shortcode':'B','evidence':[{'account':'@columbia_kr','title':'광고'}]})
        self.assertEqual(result['category'],'official')
    def test_recent_negative(self):
        now=dt.datetime(2026,11,18,2,tzinfo=ZoneInfo('Asia/Seoul'))
        result=classify({'shortcode':'C','evidence':[{'published_at':'2026-11-17T15:00:00+09:00','title':'품질 불량'}]},now=now)
        self.assertTrue(result['within_48h'])
        self.assertEqual(result['sentiment'],'negative')
    def test_unverified_timezone(self):
        result=classify({'shortcode':'D','evidence':[{'published_at':'2026-11-17T15:00:00'}]})
        self.assertIsNone(result['within_48h'])
if __name__=='__main__': unittest.main()
