import unittest
from report_model import build_model,SECTIONS

class ModelTests(unittest.TestCase):
    def test_all_categories_and_missing_sources(self):
        r=lambda s,o,v:dict(sessions=s,transactions=o,purchaseRevenue=v)
        d={"overall":{"current":r(100,10,1000),"yoy":r(200,20,2400)},"pdp_series":{"rows":[{"itemCategory":"Shoes","ys":[8,4],"ys_yoy":[8,8],"has_yoy":True},{"itemCategory":"Pants","ys":[4,8],"ys_yoy":[4,4],"has_yoy":True}]},"weather_forecast":{"days":[{"min_temp":-3,"max_temp":5}]}}
        m=build_model(d)["sections"]
        self.assertEqual(tuple(m),SECTIONS)
        self.assertEqual(len(m["Product Intelligence"]["data"]["categories"]),2)
        self.assertEqual(m["Product Early Warning"]["data"]["category_declines"][0]["itemCategory"],"Shoes")
        self.assertAlmostEqual(sum(m["Root Cause Analysis"]["data"].values()),-1400)
        self.assertEqual(m["Weather Intelligence"]["data"]["forecast"]["days"][0]["min_temp"],-3)
        self.assertIsNone(m["Predictive Intelligence"]["data"]["forecast"])
        self.assertFalse(m["Competitive Discount Matrix"]["data"]["unknown_is_zero"])

if __name__=="__main__":
    unittest.main()
