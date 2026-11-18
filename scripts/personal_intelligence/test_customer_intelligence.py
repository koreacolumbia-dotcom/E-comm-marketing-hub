import unittest
from report_model import customer_intelligence

class CustomerIntelligenceTests(unittest.TestCase):
    def sample(self):
        return {"end_date":"2026-11-17","overall":{"current":{"transactions":92}},
                "signup_users":{"current":156,"prev":251,"yoy":102},
                "channel_snapshot":[
                    {"bucket":"Paid Ad","new_buyers":37,"existing_buyers":22,"transactions":60,"sessions":9000},
                    {"bucket":"Organic Traffic","new_buyers":18,"existing_buyers":2,"transactions":21,"sessions":2000},
                    {"bucket":"Owned Channel","new_buyers":5,"existing_buyers":5,"transactions":11,"sessions":800},
                    {"bucket":"Total","new_buyers":60,"existing_buyers":29,"transactions":92}]}
    def test_observed_counts_and_signup_separation(self):
        x=customer_intelligence(self.sample())
        self.assertEqual(x["observed_buyers"],89)
        self.assertEqual(x["new_mix_pct"],67.42)
        self.assertEqual(x["signups_yoy_pct"],52.94)
        self.assertEqual(x["top_new_channels"][0]["channel"],"Paid Ad")
        self.assertIsNone(x["signup_to_purchase_cvr"])
        self.assertIsNone(x["repeat_purchase_rate"])
        self.assertIsNone(x["buyer_comparisons"]["yoy"])
    def test_channel_overlap_not_summed(self):
        d=self.sample()
        d["channel_snapshot"][0]["new_buyers"]=38
        d["channel_snapshot"][0]["transactions"]=61
        self.assertEqual(customer_intelligence(d)["new_buyers"],60)
    def test_missing_total_never_reconstructed(self):
        d=self.sample()
        d["channel_snapshot"].pop()
        self.assertIsNone(customer_intelligence(d)["observed_buyers"])
    def test_impossible_totals_rejected(self):
        d=self.sample()
        d["channel_snapshot"][-1]["new_buyers"]=100
        x=customer_intelligence(d)
        self.assertEqual(x["buyer_status"],"validation_required")
        self.assertIsNone(x["observed_buyers"])
    def test_signup_only(self):
        x=customer_intelligence({"signup_users":{"current":0,"yoy":10}})
        self.assertEqual(x["signups_yoy_pct"],-100)
        self.assertIsNone(x["new_buyers"])
    def test_definition_and_period_gate(self):
        d=self.sample()
        d["meta"]={"yoy_start":"2025-11-18","yoy_end":"2025-11-18"}
        d["buyer_comparisons"]={"yoy":{"definition":"erp_lifetime","new_buyers":12,"existing_buyers":10,
                                       "period_start":"2025-11-18","period_end":"2025-11-18"}}
        self.assertIsNone(customer_intelligence(d)["buyer_comparisons"]["yoy"])
        d["buyer_comparisons"]["yoy"]["definition"]="ga4_observed_purchase_since_2024"
        self.assertEqual(customer_intelligence(d)["buyer_comparisons"]["yoy"]["new_yoy_or_wow_pct"],400)

if __name__=="__main__":
    unittest.main()
