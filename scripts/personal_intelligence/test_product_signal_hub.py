import unittest
from datetime import date,timedelta
from product_signal_hub import build_signals, assess_weather_association
from observed_weather import parse
class ProductTests(unittest.TestCase):
    def fixture(self):
        return {"end_date":"2026-11-17","product_scopes":{"current_name":[{"itemName":"M’s Down Jacket","views":400},{"itemName":"New Backpack","views":500}],"previous_name":[{"itemName":"M`s Down Jacket","views":800}],"best_name":[]}}
    def test_overlap_and_no_zero(self):
        d=self.fixture()
        events={"events":[{"brand":"tnf","brand_name":"TNF","title":"다운 할인","date_status":"official_period","start_date":"2026-11-15","end_date":"2026-11-20","source_url":"https://example.com/event"},{"brand":"other","title":"백팩 할인","date_status":"official_period","start_date":"2026-11-15","end_date":"2026-11-20","source_url":"https://example.com/a"},{"brand":"other","title":"종료","date_status":"official_period","start_date":"2026-11-01","end_date":"2026-11-05","source_url":"https://example.com/b"}]}
        w={}
        end=date(2026,11,17)
        for i in range(14):w[(end-timedelta(days=i)).isoformat()]={"mean_temp":2 if i<7 else 10,"station":"108","kind":"observed","verified":True}
        out=build_signals(d,events=events,weather=w)
        self.assertEqual(out["temperature_change_c"],-8)
        self.assertEqual(len(out["cards"]),1)
        r=out["cards"][0]
        self.assertEqual(r["signals"],["pdp_down"])
        self.assertEqual(r["pdp_change_pct"],-50)
        self.assertIsNone(r["sales_day"])
        self.assertEqual(len(r["competitor_events"]),1)
        self.assertEqual(r["competitor_events"][0]["overlap_days"],3)
        self.assertFalse(r["causal"])
    def test_noaa_official_daily_observation_is_accepted_but_not_forecast(self):
        from product_signal_hub import valid_temp
        src={"mean_temp":3.4,"station":"47108099999","kind":"observed","verified":True,"source":"NOAA GSOD Seoul WMO 47108"}
        self.assertEqual(valid_temp(src),3.4)
        self.assertIsNone(valid_temp({**src,"source":"unverified"}))
        self.assertIsNone(valid_temp({**src,"kind":"forecast"}))

    def test_weather_no_forecast(self):
        d=self.fixture()
        self.assertEqual(build_signals(d,weather={"2026-11-17":{"mean_temp":1,"station":"108","kind":"forecast","verified":True}})["weather_days"],0)
    def test_weather_signal_requires_complete_ga4_observations(self):
        temperatures=[5.2,7.2,6.3,9.2,10.8,12.5,11,12.1,12.8,12.6,5.7,1.3,-0.8,-1.5]
        rock=[1,1,1,2,0,0,0,1,0,0,0,0,5,4]
        strong=assess_weather_association(temperatures,rock,"ga4_purchase_items")
        self.assertEqual(strong["status"],"signal")
        self.assertEqual(strong["paired_days"],14)
        self.assertLessEqual(strong["correlation"],-.6)
        self.assertFalse(strong["causal"])
        self.assertEqual(strong["cold_days"],3)
        self.assertEqual(strong["cold_avg_units"],3)
        self.assertEqual(assess_weather_association(temperatures,rock,None)["status"],"insufficient")
        self.assertEqual(assess_weather_association(temperatures,[None]*14,"ga4_purchase_items")["status"],"insufficient")

    def test_weather_signal_weak_or_insufficient_when_evidence_disagrees(self):
        temperatures=[5.2,7.2,6.3,9.2,10.8,12.5,11,12.1,12.8,12.6,5.7,1.3,-0.8,-1.5]
        river=[4,9,2,8,3,5,3,3,3,3,2,1,7,1]
        adventure=[0,0,0,0,0,1,0,0,0,1,0,1,0,0]
        self.assertEqual(assess_weather_association(temperatures,river,"ga4_purchase_items")["status"],"weak")
        self.assertEqual(assess_weather_association(temperatures,adventure,"ga4_purchase_items")["status"],"insufficient")
        self.assertEqual(assess_weather_association([None]*14,river,"ga4_purchase_items")["status"],"insufficient")

    def test_parse_station_date_and_invalid(self):
        raw="# TM STN TA_AVG TA_MAX TA_MIN RN_DAY\n20261117 108 2.2 8 -1 0\n20261116 109 3 8 -1 0\n20261116 108 -99 8 -1 0\n"
        x=parse(raw,date(2026,11,16),date(2026,11,17))
        self.assertEqual(list(x),["2026-11-17"])
        self.assertEqual(x["2026-11-17"]["mean_temp"],2.2)
if __name__=="__main__":unittest.main()
