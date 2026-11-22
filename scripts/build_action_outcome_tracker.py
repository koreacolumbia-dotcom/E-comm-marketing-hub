#!/usr/bin/env python3
"""CSK verified action outcome tracking; source-backed before/after, not causal lift."""
from __future__ import annotations
import datetime as dt
import json
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
OPERATIONS=ROOT/"reports/marketing_operations/latest.json"
REGISTRY=ROOT/"config/marketing_action_log.json"
DAILY=ROOT/"reports/daily_digest/data/daily"
OUT=ROOT/"reports/decision_health/action_outcomes.json"
METRICS=("sessions","orders","revenue","cvr","aov")

def load(path):
    try:
        value=json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value,dict) else {}
    except (ValueError,OSError,UnicodeError):
        return {}

def window_7d(start, daily=DAILY):
    totals={"sessions":0.0,"orders":0.0,"revenue":0.0}
    missing=[]
    for i in range(7):
        day=start+dt.timedelta(days=i)
        report=load(daily/(day.isoformat()+".json"))
        if (report.get("mode") not in ("daily",None) or
            report.get("end_date")!=day.isoformat() or
            not isinstance(report.get("kpis"),dict)):
            missing.append(day.isoformat());continue
        record=report["kpis"]
        if (any(type(record.get(k)) not in (int,float) or record[k]<0 for k in totals)
            or record["sessions"]<=0 or record["orders"]>record["sessions"]):
            missing.append(day.isoformat());continue
        for k in totals:totals[k]+=record[k]
    if missing:return None,missing
    totals["cvr"]=totals["orders"]/totals["sessions"]
    totals["aov"]=totals["revenue"]/totals["orders"] if totals["orders"] else None
    return {k:round(v,4) if v is not None else None for k,v in totals.items()},[]

def build(operations,registry,daily=DAILY):
    report_date=dt.date.fromisoformat(operations["report_date"])
    log=registry.get("entries") or []
    seen=set()
    completed={}
    for item in log:
        if not isinstance(item,dict):raise ValueError("Invalid action entry")
        key=item.get("action_id")
        if not isinstance(key,str) or not key or key in seen:raise ValueError("Action ID must be unique")
        seen.add(key)
        if item.get("status")!="implemented":continue
        completed_at=dt.date.fromisoformat(item["implemented_on"])
        source=item.get("evidence_url")
        if not isinstance(source,str) or not source.startswith("https://") or len(source)>500:
            raise ValueError("Implemented actions require an HTTPS evidence URL")
        if completed_at>report_date:raise ValueError("Implementation cannot be in the future")
        completed[key]={"date":completed_at,"evidence_url":source,
                        "metric":item.get("primary_metric") or "revenue"}
    rows=[]
    for action in operations.get("actions") or []:
        aid=action.get("id")
        if not aid:continue
        result={"action_id":aid,"title":action.get("title"),"priority":action.get("priority"),
                "owner":action.get("owner"),"source_status":action.get("status"),
                "outcome_status":"not_implemented","measurement_status":"not_started",
                "implemented_on":None,"evidence_url":None,
                "comparison":None,"causal":False,
                "next_step":"실행 완료일과 근거 링크가 등록된 뒤 성과 검증을 시작"}
        if aid in completed:
            info=completed[aid]
            date=info["date"]
            result.update(outcome_status="implemented",implemented_on=date.isoformat(),
                          evidence_url=info["evidence_url"],primary_metric=info["metric"])
            # Exclude implementation day from both 7-day windows, avoiding partial days.
            prior,prev_missing=window_7d(date-dt.timedelta(days=7),daily)
            after,after_missing=window_7d(date+dt.timedelta(days=1),daily) if (
                date+dt.timedelta(days=7)<=report_date) else (None,["post_window_not_mature"])
            if prev_missing or after_missing:
                result.update(measurement_status="awaiting_complete_windows",
                              missing_dates=prev_missing+after_missing,
                              next_step="실행 후 7개 완결 일자와 실행 전 기준일 확보 후 자동 비교")
            elif prior and after:
                deltas={k:round((after[k]/prior[k]-1)*100,2)
                        if prior[k] is not None and prior[k]>0 and after[k] is not None else None
                        for k in METRICS}
                result.update(measurement_status="descriptive_only",
                              comparison={"before":prior,"after":after,"change_pct":deltas,
                                          "before_period":f"{date-dt.timedelta(days=7)} ~ {date-dt.timedelta(days=1)}",
                                          "after_period":f"{date+dt.timedelta(days=1)} ~ {date+dt.timedelta(days=7)}"},
                              next_step="유사 시즌·캠페인·광고비 대조 또는 홀드아웃 검증 필요. 증분 효과 미확정")
        rows.append(result)
    rows.sort(key=lambda row:({"P0":0,"P1":1,"P2":2}.get(row.get("priority"),3),
                               row.get("action_id") or ""))
    return {"schema":1,"report_date":report_date.isoformat(),"status":"ready",
            "source":"reports/marketing_operations/latest.json + config/marketing_action_log.json + complete Daily Digest",
            "method":"실행 증거 기록 후 익일~7일 vs 실행 전 7일 단순 기간 비교; 실행 당일 제외",
            "causal":False,"measured":sum(x["measurement_status"]=="descriptive_only" for x in rows),
            "implemented":sum(x["outcome_status"]=="implemented" for x in rows),
            "total":len(rows),"actions":rows,
            "caveat":"조치 실행 기록이 없으면 효과는 미측정. 단순 전후 지표는 인과적 순증분이 아님."}

def main():
    ops,registry=load(OPERATIONS),load(REGISTRY)
    if not ops.get("report_date"):raise SystemExit("Operations report missing")
    result=build(ops,registry)
    OUT.parent.mkdir(parents=True,exist_ok=True)
    OUT.write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    print("[CSK ACTION OUTCOMES]",result["report_date"],
          "tracked",result["total"],"implemented",result["implemented"],
          "measured",result["measured"])

if __name__=="__main__":
    main()
