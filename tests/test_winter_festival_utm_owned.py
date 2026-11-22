import unittest
from scripts.build_winter_festival_utm_owned import sql_with_utm_term,summarize

class WinterUtmTests(unittest.TestCase):
    def test_canonical_session_source_remains_unchanged(self):
        base="""WITH keyed AS (SELECT * FROM event_data),
attribution AS (SELECT * FROM keyed),
session_keys AS (SELECT session_key FROM keyed),
session_metrics AS (
SELECT
    IFNULL(a.default_group,'') AS sessionDefaultChannelGroup,
    IF(s.session_day BETWEEN DATE('2026-11-20') AND DATE('2026-11-21'),1,0) AS sessions
  FROM session_keys s LEFT JOIN attribution a USING(session_key)
  UNION ALL
  SELECT '(not set) / (not set)', '(not set)', '', 0, transactions, purchaseRevenue
)
SELECT sessionSourceMedium, sessionCampaignName, sessionDefaultChannelGroup,
SUM(sessions) AS sessions, SUM(transactions) AS transactions, SUM(purchaseRevenue) AS purchaseRevenue
FROM session_metrics
GROUP BY 1,2,3
"""
        result=sql_with_utm_term(base)
        self.assertIn("utm_term_by_session",result)
        self.assertIn("WHERE utm_term IS NOT NULL",result)
        self.assertIn("t.utm_term",result)
        self.assertIn("sessionManualTerm",result)
        self.assertIn("GROUP BY 1,2,3,4",result)
        self.assertIn("LEFT JOIN attribution a USING(session_key)",result)
        self.assertIn("utm_term=([^&#]+)",result)
        self.assertNotIn("QUALIFY ROW_NUMBER() OVER (\n    PARTITION BY session_key ORDER BY IF(event_name='session_start',0,1),event_timestamp)=1",result)

    def test_source_hints_separate_from_confirmed_utm_term(self):
        data={
            "2025":[
                {"sessionSourceMedium":"KAKAO_CH / KAKAO_CH","sessionCampaignName":"(not set)",
                 "sessionManualTerm":"(not set)","sessions":20,"transactions":2,"purchaseRevenue":120000},
                {"sessionSourceMedium":"other / referral","sessionCampaignName":"Winter Festival 2025",
                 "sessionManualTerm":"owned","sessions":10,"transactions":1,"purchaseRevenue":50000},
                {"sessionSourceMedium":"private / internal","sessionCampaignName":"Winter Festival",
                 "sessionManualTerm":"user@example.com","sessions":10,"transactions":2,"purchaseRevenue":10000},
                {"sessionSourceMedium":"naver / cpc","sessionCampaignName":"winterfestival",
                 "sessionManualTerm":"paid","sessions":300,"transactions":5,"purchaseRevenue":900000},
                {"sessionSourceMedium":"LMS_ECOM / LMS_ECOM","sessionCampaignName":"LMS_1121_WinterFestival_teasing",
                 "sessionManualTerm":"LMS_1121_WinterFestival_teasingalice123",
                 "sessions":10,"transactions":1,"purchaseRevenue":65000}
            ],
            "2026":[
                {"sessionSourceMedium":"kakao / message","sessionCampaignName":"owned_winter",
                 "sessionManualTerm":"sms","sessions":12,"transactions":3,"purchaseRevenue":300000},
                {"sessionSourceMedium":"lms / sms","sessionCampaignName":"2026 regular",
                 "sessionManualTerm":"fall","sessions":2,"transactions":1,"purchaseRevenue":20000}
            ]}
        out=summarize(data)
        self.assertEqual(out["2025"]["grouped_totals"]["utm_campaign_or_term"]["revenue"],115000)
        self.assertTrue(any(r["utm_term"]=="(individualized suffix masked)" for r in out["2025"]["rows"]))
        self.assertFalse(any(r["source_medium"]=="naver / cpc" for r in out["2025"]["rows"]))
        self.assertEqual(out["2025"]["grouped_totals"]["media_context_only"]["revenue"],120000)
        self.assertEqual(out["2026"]["grouped_totals"]["utm_campaign_or_term"]["orders"],3)
        self.assertEqual(out["2026"]["suppressed_small_groups"]["count"],1)
        self.assertFalse(any("example.com" in str(v) for v in out.values()))
        self.assertEqual(len(out["2026"]["rows"]),1)

if __name__=="__main__":
    unittest.main()
