import unittest
from datetime import date
from unittest.mock import patch
from observed_weather import parse,parse_noaa,fetch,NOAA_SEOUL
from product_signal_hub import valid_temp


class WeatherFallbackTests(unittest.TestCase):
    def test_noaa_station_units_and_quality(self):
        csv="STATION,DATE,TEMP,TEMP_ATTRIBUTES,MIN,MAX\n"+NOAA_SEOUL+",2026-11-21,41.0,12,32.0,50.0\n"
        data=parse_noaa(csv,date(2026,11,21),date(2026,11,21))
        self.assertIn("2026-11-21",data)
        self.assertEqual(data["2026-11-21"]["mean_temp"],5)
        self.assertEqual(valid_temp(data["2026-11-21"]),5)
        self.assertEqual(data["2026-11-21"]["source"],"NOAA GSOD Seoul WMO 47108")

    def test_non_seoul_and_low_sample_and_missing_are_rejected(self):
        csv="STATION,DATE,TEMP,TEMP_ATTRIBUTES\n47112099999,2026-11-21,45.0,11\n"+NOAA_SEOUL+",2026-11-21,40.0,2\n"
        self.assertEqual(parse_noaa(csv,date(2026,11,21),date(2026,11,21)),{})

    def test_official_kma_observation_not_overwritten(self):
        kma={"2026-11-21":{"mean_temp":3.2,"verified":True,"kind":"observed","station":"108","source":"KMA ASOS"}}
        noaa={"2026-11-21":{"mean_temp":8.4,"verified":True,"kind":"observed","station":NOAA_SEOUL,"source":"NOAA GSOD Seoul WMO 47108"}}
        with patch("observed_weather._request",return_value=kma),patch("observed_weather.fetch_noaa",return_value=noaa),patch.dict("os.environ",{"DAILY_DIGEST_KMA_SERVICE_KEY":"TEST"}):
            self.assertEqual(fetch(date(2026,11,21),date(2026,11,21))["2026-11-21"]["mean_temp"],3.2)

    def test_kma_missing_uses_real_noaa_only(self):
        noaa={"2026-11-21":{"mean_temp":5,"verified":True,"kind":"observed","station":NOAA_SEOUL,"source":"NOAA GSOD Seoul WMO 47108"}}
        with patch("observed_weather._request",return_value={}),patch("observed_weather.fetch_noaa",return_value=noaa),patch.dict("os.environ",{"DAILY_DIGEST_KMA_SERVICE_KEY":"TEST"}):
            self.assertEqual(fetch(date(2026,11,21),date(2026,11,21))["2026-11-21"]["source"],"NOAA GSOD Seoul WMO 47108")

    def test_forecast_and_unverified_are_not_treated_as_observations(self):
        data={"mean_temp":5,"verified":True,"kind":"observed","station":NOAA_SEOUL,"source":"NOAA GSOD Seoul WMO 47108"}
        self.assertIsNone(valid_temp({**data,"verified":False}))
        self.assertIsNone(valid_temp({**data,"kind":"forecast"}))
        self.assertIsNone(valid_temp({**data,"station":"wrong"}))


if __name__=="__main__":
    unittest.main()
