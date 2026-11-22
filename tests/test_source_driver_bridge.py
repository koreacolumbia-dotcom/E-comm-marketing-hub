import datetime as dt
import math
import unittest
from scripts.build_source_driver_bridge import bridge, build, source_row

OPS={"report_date":"2026-11-21","weekly":{"status":"ready",
 "period":"2026-11-15 ~ 2026-11-21","comparison_period":"2026-11-08 ~ 2026-11-14",
 "previous":{"total":{"sessions":150,"orders":10,"revenue":1000}},
 "current":{"total":{"sessions":180,"orders":20,"revenue":2260}}}}
SRC={"report_date":"2026-11-21","status":"ga4_measured",
 "period":{"previous":{"start":"2026-11-08","end":"2026-11-14"},
           "current":{"start":"2026-11-15","end":"2026-11-21"}},
 "totals":{"previous":{"sessions":150,"transactions":10,"revenue":1000},
           "current":{"sessions":180,"transactions":20,"revenue":2260}},
 "rows":[
  {"source_medium":"google / cpc",
   "previous":{"sessions":100,"transactions":10,"revenue":1000},
   "current":{"sessions":120,"transactions":18,"revenue":2160}},
  {"source_medium":"new / email",
   "previous":{"sessions":50,"transactions":0,"revenue":0},
   "current":{"sessions":60,"transactions":2,"revenue":100}}]}

class SourceDriverTests(unittest.TestCase):
 def test_shapley_reconciles_and_is_symmetric(self):
    prev=(100,0.1,100);cur=(120,0.15,120)
    forward=bridge(prev,cur)
    reverse=bridge(cur,prev)
    self.assertAlmostEqual(sum(forward.values()),2160-1000)
    for key in forward:
      self.assertAlmostEqual(forward[key],-reverse[key])

 def test_mixed_exact_and_undecomposable_reconcile(self):
    out=build(OPS,SRC)
    self.assertEqual(out["status"],"measured_descriptive")
    self.assertEqual(out["row_count"],2)
    self.assertEqual(out["headline"]["revenue_delta"],1260)
    self.assertEqual(out["driver_totals"]["not_decomposable"],100)
    self.assertAlmostEqual(sum(out["driver_totals"].values()),1260,places=2)
    self.assertTrue(out["quality"]["no_additional_bigquery_jobs"])
    self.assertFalse(out["causal"])
    self.assertEqual(out["top_gains"][0]["source_medium"],"google / cpc")
    self.assertIn("퍼널",out["top_gains"][0]["next_validation"])
    self.assertIn("양 기간",next(x for x in out["rows"] if x["source_medium"]=="new / email")["next_validation"])
    self.assertIsNone(next(x for x in out["rows"] if x["source_medium"]=="new / email")["components"])

 def test_not_ready_or_stale_period_must_show_unavailable(self):
    out=build(OPS,{**SRC,"report_date":"2026-11-20"})
    self.assertEqual(out["status"],"unavailable")
    self.assertEqual(out["rows"],[])
    self.assertEqual(out["quality"]["blocker"],"source_period_or_status_mismatch")

 def test_duplicated_or_inconsistent_aggregate_is_blocked(self):
    repeated={**SRC,"rows":SRC["rows"]+[SRC["rows"][0]]}
    self.assertEqual(build(OPS,repeated)["quality"]["blocker"],"duplicate_source_medium")
    corrupt={**SRC,"totals":{**SRC["totals"],
             "current":{"sessions":180,"transactions":20,"revenue":2400}}}
    self.assertEqual(build(OPS,corrupt)["quality"]["blocker"],"source_row_total_disagreement")

 def test_no_fake_conversion_from_missing_sessions(self):
    with self.assertRaises(ValueError):
      source_row({"source_medium":"other / unknown",
       "previous":{"sessions":0,"transactions":1,"revenue":10},
       "current":{"sessions":10,"transactions":1,"revenue":100}})

 def test_low_volume_is_flagged_not_certified(self):
    out=build(OPS,SRC)
    self.assertEqual(next(x for x in out["rows"] if x["source_medium"]=="google / cpc")["quality"],"reviewable_descriptive")
    self.assertEqual(next(x for x in out["rows"] if x["source_medium"]=="new / email")["quality"],"small_session_base")

if __name__=="__main__":
 unittest.main()
