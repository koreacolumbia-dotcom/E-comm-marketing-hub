"""Unit tests for public Instagram candidate ingestion. Run: python -m unittest discover -s instagram_intelligence -p 'test_*.py'"""
import unittest
from collector import collect

class CandidateTests(unittest.TestCase):
    def test_deduplicate_by_shortcode_across_sources(self):
        posts={}
        collect([{"url":"https://www.instagram.com/p/Ab_123/","title":"First"}],"@columbia_kr","brave",posts)
        collect([{"url":"https://instagram.com/reel/Ab_123/?utm_source=test","description":"Second"}],"#컬럼비아","local_voc",posts)
        self.assertEqual(len(posts),1)
        self.assertEqual(set(posts["Ab_123"]["sources"]),{"brave","local_voc"})
        self.assertEqual(len(posts["Ab_123"]["evidence"]),2)
    def test_ignore_non_post_urls(self):
        posts={}
        collect([{"url":"https://www.instagram.com/columbia_kr/"},{"url":"https://example.com/p/Ab123/"}],"test","brave",posts)
        self.assertFalse(posts)
    def test_preserve_source_evidence(self):
        posts={}
        collect([{"url":"https://instagram.com/reel/Xy-9/","published_at":"2026-11-17T09:00:00+09:00","account":"tester"}],"test","brave",posts)
        self.assertEqual(posts["Xy-9"]["evidence"][0]["account"],"tester")
if __name__=="__main__":
    unittest.main()
