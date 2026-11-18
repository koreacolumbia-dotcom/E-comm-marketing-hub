"""Session-attributed channel performance; never an incrementality estimate."""
from math import isfinite
from engine import revenue_bridge

METRICS=("sessions","orders","revenue")
REQUIRED=("Awareness","Paid Ad","Organic Traffic","Owned Channel","Official SNS","etc")
def number(x):
    return float(x) if isinstance(x,(int,float)) and not isinstance(x,bool) and isfinite(x) and x>=0 else None
def ratio(a,b,m=1):
    return a/b*m if a is not None and b is not None and b>0 else None
def growth(a,b):
    return (a/b-1)*100 if a is not None and b is not None and b>0 else None
def metrics(r):
    return dict(sessions=number(r.get("sessions")),orders=number(r.get("transactions",r.get("orders"))),revenue=number(r.get("purchaseRevenue")))
def prior_value(row,metric,current,period):
    key=metric+"_"+period+"_base"
    if key in row:
        return number(row[key]),"reported_base" if number(row[key]) is not None else "missing"
    rate_key={"sessions":"session","orders":"orders","revenue":"revenue"}[metric]+"_"+("yoy" if period=="yoy" else "dod")
    rate=row.get(rate_key)
    # Legacy pct_change uses +100% for some zero-base cases: never invert that sentinel.
    if current is not None and current>0 and isinstance(rate,(int,float)) and not isinstance(rate,bool) and isfinite(rate) and rate>-1 and rate!=1:
        return current/(1+rate),"estimated_from_growth"
    return None,"missing"
def reconcile(rows,totals):
    checks={}
    for k in METRICS:
        vals=[r[k] for r in rows]
        base=totals[k]
        if base is None or not rows or any(v is None for v in vals):
            checks[k]={"status":"missing","delta":None}
        else:
            diff=sum(vals)-base
            checks[k]={"status":"ok" if abs(diff)<=0.01 else "mismatch","delta":round(diff,4)}
    return checks
def make_rows(raw,name_key,overall,periods,required=()):
    rows=[]
    names=set()
    duplicate=False
    for x in raw or []:
        if not isinstance(x,dict): continue
        name=str(x.get(name_key,"")).strip()
        if not name or name.lower()=="total": continue
        if name.casefold() in names:
            duplicate=True;continue
        names.add(name.casefold())
        cur=metrics(x)
        a={"channel":name,**cur,"cvr_pct":ratio(cur["orders"],cur["sessions"],100),
           "aov":ratio(cur["revenue"],cur["orders"]),"revenue_per_session":ratio(cur["revenue"],cur["sessions"]),
           "comparisons":{}}
        for period in periods:
            old,provenance={},{}
            for k in METRICS:
                old[k],provenance[k]=prior_value(x,k,cur[k],period)
            dr=cur["revenue"]-old["revenue"] if cur["revenue"] is not None and old["revenue"] is not None else None
            effects=None
            try:
                effects=revenue_bridge(cur,old)
            except (ValueError,KeyError,TypeError,ZeroDivisionError):
                pass
            old_cvr=ratio(old["orders"],old["sessions"],100)
            a["comparisons"][period]={"prior":old,"provenance":provenance,"revenue_delta":dr,
                "revenue_growth_pct":growth(cur["revenue"],old["revenue"]),
                "session_growth_pct":growth(cur["sessions"],old["sessions"]),
                "order_growth_pct":growth(cur["orders"],old["orders"]),
                "cvr_delta_pp":a["cvr_pct"]-old_cvr if a["cvr_pct"] is not None and old_cvr is not None else None,
                "aov_growth_pct":growth(a["aov"],ratio(old["revenue"],old["orders"])),
                "driver_effects":effects}
        rows.append(a)
    checks=reconcile(rows,overall)
    missing=[x for x in required if x.casefold() not in names]
    valid=not duplicate and not missing
    for a in rows:
        for k in METRICS:
            a[k+"_share_pct"]=ratio(a[k],overall[k],100) if valid and checks[k]["status"]=="ok" else None
    if not valid:
        for a in rows:
            for p in periods: a["comparisons"][p]["driver_effects"]=None
    return rows,checks,valid,missing
def marketing_attribution(d):
    overall=d.get("overall") or {}
    current=metrics(overall.get("current") or {})
    priors={"yoy":metrics(overall.get("yoy") or {}),"wow":metrics(overall.get("prev") or {})}
    rows,checks,valid,missing=make_rows(d.get("channel_snapshot"),"bucket",current,("yoy","wow"),REQUIRED)
    summaries={}
    for p,old in priors.items():
        delta=current["revenue"]-old["revenue"] if current["revenue"] is not None and old["revenue"] is not None else None
        known=[r for r in rows if r["comparisons"][p]["revenue_delta"] is not None]
        subtotal=sum(r["comparisons"][p]["revenue_delta"] for r in known)
        usable=valid and checks["revenue"]["status"]=="ok" and delta is not None
        for r in rows:
            c=r["comparisons"][p]
            c["share_of_net_delta_pct"]=100*c["revenue_delta"]/delta if usable and delta and c["revenue_delta"] is not None else None
        summaries[p]={"total_revenue_delta":delta,"known_delta":subtotal if usable else None,
            "unallocated_delta":delta-subtotal if usable else None,
            "exact_channel_reconciliation":bool(usable and len(known)==len(rows) and abs(delta-subtotal)<=0.01 and all(r["comparisons"][p]["provenance"]["revenue"]=="reported_base" for r in rows)),
            "negative_drivers":sorted([r for r in known if r["comparisons"][p]["revenue_delta"]<0],key=lambda r:r["comparisons"][p]["revenue_delta"])[:3] if usable else [],
            "positive_drivers":sorted([r for r in known if r["comparisons"][p]["revenue_delta"]>0],key=lambda r:-r["comparisons"][p]["revenue_delta"])[:3] if usable else []}
    paid=next((r for r in rows if r["channel"]=="Paid Ad"),None)
    paid_rows,paid_checks,paid_valid,_=make_rows(d.get("paid_detail"),"sub_channel",paid or dict.fromkeys(METRICS),("yoy","wow"))
    # Unrecognised paid labels stay visible for QA; do not automatically reclassify.
    canonical={"naver brand search","naver search","criteo","meta","google gdn","google search","google","instagram","google pmax","kakao"}
    paid_review=[r for r in paid_rows if r["channel"].lower() not in canonical and (r["sessions"] or r["orders"] or r["revenue"])]
    meta=d.get("meta") or {}
    return {"date":d.get("end_date"),"source":d.get("traffic_metric_source"),"periods":{k:meta.get(k) for k in ("cur_start","cur_end","prev_start","prev_end","yoy_start","yoy_end")},
        "current":current,"channels":rows,"reconciliation":checks,"channel_set_valid":valid,"missing_channels":missing,
        "comparisons":summaries,"paid_channels":paid_rows,"paid_reconciliation":paid_checks,"paid_channel_set_valid":paid_valid,"paid_classification_review":paid_review,
        "spend_status":"source_required","causal_incrementality":None,"multi_touch_attribution":None,
        "note":"Session-attributed revenue, not causal contribution. Prior bases use raw values when supplied, otherwise estimates from positive current amounts and non-ambiguous growth rates. Zero/ambiguous bases remain unknown. Driver effects require positive sessions and orders in both periods. Channel and factor decompositions are separate. WoW is the source previous-period comparison, not DoD. No ROAS without spend evidence."}
