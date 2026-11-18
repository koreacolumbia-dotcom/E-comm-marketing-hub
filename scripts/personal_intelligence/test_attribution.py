import unittest
from attribution import marketing_attribution, prior_value

class AttributionTests(unittest.TestCase):
    def sample(self):
        names=["Paid Ad","Organic Traffic","Owned Channel","Awareness","Official SNS","etc"]
        current=[(100,5,500),(50,2,200),(20,1,100),(30,0,0),(0,0,0),(0,0,0)]
        prior=[(100,6,600),(100,5,500),(10,1,100),(30,0,0),(0,0,0),(0,0,0)]
        rows=[]
        for name,(s,o,r),(ps,po,pr) in zip(names,current,prior):
            rows.append(dict(bucket=name,sessions=s,transactions=o,purchaseRevenue=r,
                             sessions_yoy_base=ps,orders_yoy_base=po,revenue_yoy_base=pr))
        return {"channel_snapshot":rows,"overall":{"current":dict(sessions=200,transactions=8,purchaseRevenue=800),
                                                   "yoy":dict(sessions=240,transactions=12,purchaseRevenue=1200)}}
    def test_reconciled_channel_and_factor_effects(self):
        m=marketing_attribution(self.sample())
        self.assertTrue(m["comparisons"]["yoy"]["exact_channel_reconciliation"])
        self.assertEqual(m["comparisons"]["yoy"]["total_revenue_delta"],-400)
        self.assertEqual(m["comparisons"]["yoy"]["unallocated_delta"],0)
        self.assertEqual(m["comparisons"]["yoy"]["negative_drivers"][0]["channel"],"Organic Traffic")
        self.assertAlmostEqual(sum(m["channels"][0]["comparisons"]["yoy"]["driver_effects"].values()),-100)
        self.assertIsNone(m["causal_incrementality"])
    def test_legacy_zero_and_sentinel_remain_unknown(self):
        self.assertEqual(prior_value({"revenue_yoy":1},"revenue",100,"yoy"),(None,"missing"))
        self.assertEqual(prior_value({"revenue_yoy":-1},"revenue",0,"yoy"),(None,"missing"))
        self.assertEqual(prior_value({"revenue_yoy_base":0},"revenue",100,"yoy"),(0.0,"reported_base"))
    def test_missing_and_duplicate_channels(self):
        d=self.sample()
        d["channel_snapshot"].pop()
        x=marketing_attribution(d)
        self.assertFalse(x["channel_set_valid"])
        self.assertIsNone(x["channels"][0]["revenue_share_pct"])
        d=self.sample()
        d["channel_snapshot"].append(d["channel_snapshot"][0])
        self.assertFalse(marketing_attribution(d)["channel_set_valid"])
    def test_total_mismatch_blocks_share(self):
        d=self.sample()
        d["overall"]["current"]["purchaseRevenue"]=999
        x=marketing_attribution(d)
        self.assertEqual(x["reconciliation"]["revenue"]["status"],"mismatch")
        self.assertIsNone(x["channels"][0]["revenue_share_pct"])
        self.assertEqual(x["comparisons"]["yoy"]["negative_drivers"],[])
if __name__=="__main__":
    unittest.main()
