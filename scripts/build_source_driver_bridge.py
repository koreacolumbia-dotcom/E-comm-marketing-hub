#!/usr/bin/env python3
"""Source/medium metric bridge from already measured 7-day GA4 aggregates.

No additional BigQuery jobs. Explains accounting movements, never causal lift.
"""
from __future__ import annotations
import datetime as dt
import itertools
import json
import math
from pathlib import Path
from typing import Any

ROOT=Path(__file__).resolve().parents[1]
INPUT=ROOT/"reports/decision_health/source_medium_weekly.json"
OPS=ROOT/"reports/marketing_operations/latest.json"
OUT=ROOT/"reports/decision_health/source_driver_bridge.json"
FACTORS=("sessions","cvr","aov")
PERMUTATIONS=tuple(itertools.permutations(range(3)))
METRICS=("sessions","transactions","revenue")
MIN_SESSIONS=100
MIN_ORDERS=5

def load(path:Path)->dict:
    try:
        result=json.loads(path.read_text(encoding="utf-8"))
        return result if isinstance(result,dict) else {}
    except (OSError,UnicodeError,ValueError):
        return {}

def nonnegative(value: Any)->float:
    if type(value) not in (int,float) or not math.isfinite(value) or value<0:
        raise ValueError("Source aggregates must be finite nonnegative")
    return float(value)

def m(factors):
    return factors[0]*factors[1]*factors[2]

def bridge(a,b):
    """Average the marginal effect across all six possible replacement orders."""
    if any(x<0 or not math.isfinite(x) for x in a+b):
        raise ValueError("Invalid decomposition factors")
    result=[0.0,0.0,0.0]
    for sequence in PERMUTATIONS:
        state=list(a)
        for index in sequence:
            was=m(state)
            state[index]=b[index]
            result[index]+=(m(state)-was)/len(PERMUTATIONS)
    expected=m(b)-m(a)
    if abs(sum(result)-expected)>max(0.02,abs(expected)*1e-9):
        raise ValueError("Bridge does not reconcile")
    return dict(zip(FACTORS,result))

def same_week(ops,raw):
    w=ops.get("weekly") or {}
    if (raw.get("status")!="ga4_measured" or w.get("status")!="ready" or
        raw.get("report_date")!=ops.get("report_date")):
        return False
    try:
        ca,cb=[x.strip() for x in w["period"].split("~")]
        pa,pb=[x.strip() for x in w["comparison_period"].split("~")]
    except (KeyError,TypeError,AttributeError,ValueError):
        return False
    p=raw.get("period") or {}
    return (p.get("current")=={"start":ca,"end":cb} and
            p.get("previous")=={"start":pa,"end":pb})

def verdict(row):
    """Enough volume for descriptive drilldown; not a statistical causal score."""
    current,previous=row["current"],row["previous"]
    if min(current["sessions"],previous["sessions"])<MIN_SESSIONS:
        return "small_session_base"
    if min(current["transactions"],previous["transactions"])<MIN_ORDERS:
        return "small_order_base"
    return "reviewable_descriptive"

def source_row(raw):
    name=raw.get("source_medium")
    if not isinstance(name,str) or not name.strip() or len(name)>180:
        raise ValueError("Invalid public source/medium label")
    a,b=[],[]
    for period,target in (("previous",a),("current",b)):
        payload=raw.get(period) or {}
        target.extend(nonnegative(payload.get(field)) for field in METRICS)
    prev={"sessions":a[0],"transactions":a[1],"revenue":a[2]}
    cur={"sessions":b[0],"transactions":b[1],"revenue":b[2]}
    if ((a[0]==0 and a[1]>0) or (b[0]==0 and b[1]>0) or
        (a[1]==0 and a[2]>0) or (b[1]==0 and b[2]>0)):
        raise ValueError("Orphan attributed purchases/revenue")
    delta=b[2]-a[2]
    valid=a[0]>0 and b[0]>0 and a[1]>0 and b[1]>0
    components=None
    if valid:
        p=(a[0],a[1]/a[0],a[2]/a[1])
        c=(b[0],b[1]/b[0],b[2]/b[1])
        comp=bridge(p,c)
        components={k:round(comp[k],2) for k in FACTORS}
        # Preserve exact channel revenue after independent cent rounding.
        cent_residual=round(delta-sum(components.values()),2)
        leading=max(FACTORS,key=lambda key:abs(comp[key]))
        components[leading]=round(components[leading]+cent_residual,2)
        if abs(sum(components.values())-delta)>0.011:
            raise ValueError("Unreconciled row decomposition")
    primary=max(FACTORS,key=lambda key:abs(components[key])) if components else None
    rules={
        "sessions":"집행/노출·소스 귀속·랜딩 유입 변화부터 대조",
        "cvr":"유입 키워드·랜딩별 장바구니→결제 퍼널과 프로모션 조건 대조",
        "aov":"주문당 SKU 수·상품 믹스·실결제 할인 변화를 동일 소스로 비교"
    }
    sample_status=verdict({"previous":prev,"current":cur})
    next_validation=("양 기간 각각 세션 100건·구매 5건 이상 확보 후 재판단"
                     if sample_status!="reviewable_descriptive" else
                     rules.get(primary,"전환수·귀속 설정과 구매 이벤트 확인"))
    result={"source_medium":name,"previous":prev,"current":cur,
            "revenue_delta":round(delta,2),
            "sessions_delta":round(b[0]-a[0]),
            "transactions_delta":round(b[1]-a[1]),
            "cvr_change_pp":round(100*(b[1]/b[0]-a[1]/a[0]),3) if a[0] and b[0] else None,
            "aov_change_pct":round(100*(b[2]/b[1]/(a[2]/a[1])-1),2)
                if a[1] and b[1] and a[2]>0 else None,
            "components":components,
            "primary_component":primary,
            "quality":sample_status,
            "next_validation":next_validation,
            "method":"shapley_6_permutations" if valid else "observed_delta_only",
            "causal":False}
    return result

def build(ops,src):
    result={"schema":1,"report_date":ops.get("report_date"),"status":"unavailable",
            "source":"reports/decision_health/source_medium_weekly.json",
            "period":src.get("period"),"rows":[],
            "headline":{"prior_revenue":None,"current_revenue":None,"revenue_delta":None},
            "driver_totals":None,"top_gains":[],"top_losses":[],
            "quality":{},"causal":False,
            "method":"세션×CVR×객단가 Shapley 6순서 산술 분해. 신규/소멸 판매 경로는 매출차만 표시.",
            "caveat":"매출 산술 분해는 매체·행사·가격의 인과 효과가 아닙니다. 광고비와 증분 ROAS 미포함."}
    if not same_week(ops,src):
        result["quality"]["blocker"]="source_period_or_status_mismatch"
        return result
    data=src.get("rows")
    if not isinstance(data,list) or not data:
        result["quality"]["blocker"]="empty_source"
        return result
    names=set()
    rows=[]
    for item in data:
        r=source_row(item)
        if r["source_medium"] in names:
            result["quality"]["blocker"]="duplicate_source_medium"
            return result
        names.add(r["source_medium"])
        rows.append(r)
    totals={p:{k:sum(r[p][k] for r in rows) for k in METRICS} for p in ("previous","current")}
    src_totals=src.get("totals") or {}
    weekly=ops["weekly"]
    checks={}
    for period in ("previous","current"):
        source=(src_totals.get(period) or {})
        operation=(weekly.get(period) or {}).get("total") or {}
        checks[period]={}
        for field,opskey in (("sessions","sessions"),("transactions","orders"),("revenue","revenue")):
            expected=nonnegative(source.get(field))
            observed=totals[period][field]
            baseline=nonnegative(operation.get(opskey))
            if abs(expected-observed)>max(1,0.00001*max(1,expected)):
                result["quality"]["blocker"]="source_row_total_disagreement"
                return result
            if (field in ("revenue","transactions") and abs(baseline-observed)>max(1,baseline*0.000001)) or (
                field=="sessions" and abs(baseline-observed)>max(1,baseline*0.0005)):
                result["quality"]["blocker"]="operations_reconciliation_failed"
                return result
            checks[period][field]={"rows":round(observed,2),"ops":round(baseline,2),
                                   "difference":round(observed-baseline,2)}
    deltas={k:sum(r["components"][k] for r in rows if r["components"] is not None) for k in FACTORS}
    unseparated=sum(r["revenue_delta"] for r in rows if r["components"] is None)
    revenue_delta=totals["current"]["revenue"]-totals["previous"]["revenue"]
    if abs(sum(deltas.values())+unseparated-revenue_delta)>max(0.5,len(rows)*0.03):
        result["quality"]["blocker"]="bridge_grand_total_mismatch"
        return result
    rows.sort(key=lambda r:(-abs(r["revenue_delta"]),r["source_medium"]))
    result.update(
        status="measured_descriptive",
        rows=rows,
        row_count=len(rows),
        headline={"prior_revenue":round(totals["previous"]["revenue"],2),
                  "current_revenue":round(totals["current"]["revenue"],2),
                  "revenue_delta":round(revenue_delta,2)},
        driver_totals={**{k:round(v,2) for k,v in deltas.items()},
                       "not_decomposable":round(unseparated,2)},
        top_gains=sorted((r for r in rows if r["revenue_delta"]>0),
                         key=lambda r:-r["revenue_delta"])[:5],
        top_losses=sorted((r for r in rows if r["revenue_delta"]<0),
                          key=lambda r:r["revenue_delta"])[:5],
        quality={"source_level":"exact_ga4_session_source_medium",
                 "weekly_reconciliation":checks,
                 "small_sample_rows":sum(r["quality"]!="reviewable_descriptive" for r in rows),
                 "undivided_rows":sum(r["components"] is None for r in rows),
                 "no_additional_bigquery_jobs":True}
    )
    return result

def main():
    out=build(load(OPS),load(INPUT))
    OUT.parent.mkdir(parents=True,exist_ok=True)
    OUT.write_text(json.dumps(out,ensure_ascii=False,indent=2,allow_nan=False)+"\n",encoding="utf-8")
    print("[SOURCE DRIVERS]",out["status"],out.get("row_count",0),
          out.get("quality",{}).get("blocker",""))

if __name__=="__main__":
    main()
