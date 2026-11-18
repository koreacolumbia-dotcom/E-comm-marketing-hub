import unittest
from datetime import date,timedelta
from product_timeline import build_timeline,points,days

class TimelineTests(unittest.TestCase):
    def svg(self,dates,values,unit,fill="#0055a5"):
        return "<svg>"+''.join(f'<circle fill="{fill}"><title>{dt[5:].replace("-","/")}: {v:,} {unit}</title></circle>' for dt,v in zip(dates,values))+"</svg>"
    def test_exact_point_labels_and_same_weekday(self):
        end=date(2026,11,17);cur=days(end,7);prev=days(end-timedelta(days=7),7)
        a=[100,110,100,120,140,180,200];b=[80]*7
        svg=self.svg(cur,a,"PDP 조회수")[:-6]+self.svg(prev,b,"PDP 조회수","#64748b")[5:]
        report={"end_date":end.isoformat(),"meta":{"cur_end":end.isoformat()},"product_scopes":{"current_name":[{"itemName":"Down Jacket","views":sum(a),"trend_svg":svg}],"previous_name":[{"itemName":"Down Jacket","views":sum(b)}]}}
        calendar={"events":[{"brand":"tnf","title":"Down promotion","date_status":"official_period","start_date":"2026-11-16","end_date":"2026-11-19","source_url":"https://example.com/a"},{"brand":"tnf","title":"Banner","date_status":"observed_only","observed_on":["2026-11-14","2026-11-16"]}]}
        m=build_timeline(report,[],calendar,{},{"cards":[{"name":"Down Jacket"}]})
        self.assertEqual(m["dates"],prev+cur)
        self.assertEqual(m["products"][0]["values"],b+a)
        self.assertEqual(m["products"][0]["delta"],[None]*7+[v-80 for v in a])
        self.assertEqual(m["brands"][0]["events"][0]["dates"],["2026-11-16","2026-11-17"])
        self.assertEqual(m["brands"][0]["events"][1]["dates"],["2026-11-14","2026-11-16"])
        self.assertEqual(m["weather"],[None]*14)
        self.assertIsNone(points({"trend_svg":svg},prev,"views"))
    def test_reject_mismatched_totals(self):
        end=date(2026,11,17);cur=days(end,7)
        r={"end_date":end.isoformat(),"meta":{"cur_end":end.isoformat()},"product_scopes":{"current_name":[{"itemName":"Jacket","views":1,"trend_svg":self.svg(cur,[10]*7,"PDP 조회수")}]}}
        self.assertEqual(build_timeline(r,[],{}, {},{"cards":[{"name":"Jacket"}]})["products"],[])

if __name__=="__main__":unittest.main()
