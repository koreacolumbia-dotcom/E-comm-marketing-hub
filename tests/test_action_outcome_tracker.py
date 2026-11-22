import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path
from scripts.build_action_outcome_tracker import build, window_7d

class ActionOutcomesTests(unittest.TestCase):
    def setUp(self):
        self.report_day=dt.date(2026,11,21)
        self.ops={"report_date":self.report_day.isoformat(),"actions":[
          {"id":"A1","title":"CRM 시나리오 수정","status":"OPEN","priority":"P0","owner":"CRM"},
          {"id":"A2","title":"상품 구조 수정","status":"OPEN","priority":"P1","owner":"ECOMM"}]}

    def make_days(self,root,start,end):
        day=start
        while day<=end:
            j={"mode":"daily","end_date":day.isoformat(),
               "kpis":{"sessions":100,"orders":10,"revenue":1000000}}
            (root/(day.isoformat()+".json")).write_text(json.dumps(j))
            day+=dt.timedelta(days=1)

    def test_no_completed_action_means_no_invented_effect(self):
        with tempfile.TemporaryDirectory() as tmp:
            data=build(self.ops,{"entries":[]},Path(tmp))
            self.assertEqual(data["total"],2)
            self.assertEqual(data["implemented"],0)
            self.assertEqual(data["measured"],0)
            self.assertTrue(all(r["comparison"] is None for r in data["actions"]))

    def test_completed_action_7_day_windows_stay_descriptive(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            self.make_days(root,dt.date(2026,11,1),dt.date(2026,11,21))
            date=dt.date(2026,11,12)
            reg={"entries":[{"action_id":"A1","status":"implemented",
                   "implemented_on":date.isoformat(),
                   "evidence_url":"https://github.com/example/change/1",
                   "primary_metric":"cvr"}]}
            report=build(self.ops,reg,root)
            a=next(a for a in report["actions"] if a["action_id"]=="A1")
            self.assertEqual(a["measurement_status"],"descriptive_only")
            self.assertEqual(a["comparison"]["change_pct"]["orders"],0)
            self.assertFalse(a["causal"])
            self.assertIsNone(next(a for a in report["actions"] if a["action_id"]=="A2")["comparison"])

    def test_maturity_and_evidence_gate(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            self.make_days(root,dt.date(2026,11,1),dt.date(2026,11,21))
            reg={"entries":[{"action_id":"A1","status":"implemented",
                 "implemented_on":"2026-11-19",
                 "evidence_url":"https://github.com/example/change/1"}]}
            a=build(self.ops,reg,root)["actions"][0]
            self.assertEqual(a["measurement_status"],"awaiting_complete_windows")
            reg["entries"][0]["evidence_url"]="Not verified"
            with self.assertRaises(ValueError):
                build(self.ops,reg,root)

if __name__=="__main__":
    unittest.main()
