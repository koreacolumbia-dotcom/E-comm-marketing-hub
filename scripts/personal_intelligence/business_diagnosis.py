"""Evidence-first daily business diagnosis; never claims causality from correlations."""
from math import isfinite
from engine import revenue_bridge

def _row(raw):
    return {k: float(raw[v]) for k,v in (("sessions","sessions"),("orders","transactions"),("revenue","purchaseRevenue"))}

def build_diagnosis(report, model=None):
    """Returns ranked observed revenue contributions, hypotheses and next checks.
    Optional source domains remain explicitly missing until verified."""
    missing=[]
    overall=report.get("overall",{})
    cy,ly=overall.get("current"),overall.get("yoy")
    effects=None
    if cy and ly:
        try:
            a,b=_row(cy),_row(ly)
            if any(not isfinite(v) or v < 0 for row in (a,b) for v in row.values()):
                raise ValueError("invalid metrics")
            effects=revenue_bridge(a,b)
        except (ValueError,TypeError,KeyError,ZeroDivisionError):
            missing.append("valid positive sessions/orders for exact bridge")
    else:
        missing.append("current and prior overall metrics")
    candidates=[]
    labels={"traffic":"Session volume","conversion":"Orders per session","aov":"Revenue per order"}
    if effects:
        for key,value in sorted(effects.items(),key=lambda pair:pair[1]):
            candidates.append({
                "dimension":key,"label":labels[key],"observed_revenue_contribution":round(value,2),
                "evidence_type":"exact multiplicative Shapley decomposition",
                "causal":False,
                "hypothesis":"Change may reflect traffic mix, merchandising or measurement effects",
                "validation":"Compare source/medium, campaigns, PDP funnel, inventory and pricing against matched prior dates",
                "priority":"P1" if value<0 and value==min(effects.values()) else ("P2" if value<0 else "P3")
            })
    sources={
        "source_medium_campaign":"channel_detail_map",
        "pdp_funnel":"pdp_funnel",
        "crm":"crm",
        "price_discount":"price_history",
        "inventory":"inventory",
        "weather_observed":"weather_observed",
        "competitor_verified":"competitor_verified",
    }
    for name,key in sources.items():
        if not report.get(key):
            missing.append(name)
    positive=[x for x in candidates if x["observed_revenue_contribution"]>0]
    return {
        "title":"AI BUSINESS DIAGNOSIS",
        "date":report.get("end_date"),
        "status":"partial" if missing else "available",
        "revenue_change":round(_row(cy)["revenue"]-_row(ly)["revenue"],2) if effects else None,
        "ranked_contributions":candidates,
        "positive_signals":positive,
        "actions":[{"priority":x["priority"],"check":x["validation"],"dimension":x["dimension"]} for x in candidates],
        "missing_evidence":missing,
        "limitations":"Accounting decomposition, not incremental causal attribution; hypotheses require independent validation.",
    }

def _self_test():
    d={"end_date":"2026-11-14","overall":{"current":{"sessions":200,"transactions":10,"purchaseRevenue":1000},"yoy":{"sessions":100,"transactions":10,"purchaseRevenue":800}}}
    result=build_diagnosis(d)
    assert result["title"]=="AI BUSINESS DIAGNOSIS"
    assert abs(sum(x["observed_revenue_contribution"] for x in result["ranked_contributions"])-200)<0.02
    assert result["status"]=="partial" and result["missing_evidence"]
    assert all(not x["causal"] for x in result["ranked_contributions"])
    d["overall"]["current"]["transactions"]=0
    assert build_diagnosis(d)["revenue_change"] is None
    print("4/4 diagnosis self-tests passed")

if __name__=="__main__": _self_test()
