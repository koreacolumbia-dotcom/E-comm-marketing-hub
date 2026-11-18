"""Budget review and bounded, non-causal reallocation scenario. No campaign writes."""
from datetime import date,timedelta
from math import isfinite

MEDIA=("naver brand search","naver search","meta","criteo","google search","google pmax")
PROTECTED={"naver brand search","meta"} # Brand demand and mixed-awareness media need manual review.
MIN_ORDERS=10
MAX_MOVE=.10

def number(x):
    return float(x) if isinstance(x,(int,float)) and not isinstance(x,bool) and isfinite(x) and x>=0 else None
def ratio(a,b,m=1):
    return a/b*m if a is not None and b is not None and b>0 else None
def paid_rows(d):
    rows={}
    for x in d.get("paid_detail") or []:
        key=str(x.get("sub_channel","")).strip().lower()
        if not key or key=="total": continue
        if key in rows: raise ValueError("Duplicate paid media")
        vals={k:number(x.get(v)) for k,v in (("sessions","sessions"),("orders","orders"),("revenue","purchaseRevenue"))}
        if any(v is None for v in vals.values()): raise ValueError("Invalid paid media values")
        rows[key]=vals
    all_paid=next((r for r in d.get("channel_snapshot",[]) if r.get("bucket")=="Paid Ad"),None)
    if all_paid is None: raise ValueError("Missing paid control")
    for k,field in (("sessions","sessions"),("orders","transactions"),("revenue","purchaseRevenue")):
        target=number(all_paid.get(field))
        if target is None or abs(sum(v[k] for v in rows.values())-target)>.01:
            raise ValueError("Paid totals do not reconcile")
    return rows

def optimize(d,history=None,costs=None):
    """Historical records must be date-aligned; verified costs must cover exact model scope."""
    out={"status":"source_required","date":d.get("end_date"),"scope":list(MEDIA),"media":[],
         "scenario":None,"marginal_roas":None,"auto_apply":False,"issues":[],"cost_status":"missing",
         "assumptions":{"window_days":7,"min_orders_per_media":MIN_ORDERS,"max_move_pct":100*MAX_MOVE,
                        "protected_media":sorted(PROTECTED),"currency":"KRW"},
         "note":"GA4 session-attributed revenue; not causal ROAS or marginal return. Recommendations are optional bounded test scenarios, not platform updates."}
    try:
        end=date.fromisoformat(d["end_date"])
        today=paid_rows(d)
    except (KeyError,TypeError,ValueError):
        out["issues"].append("Invalid current-date paid detail or parent totals");return out
    out["media"]=[{"media":k,**today[k],"cvr_pct":ratio(today[k]["orders"],today[k]["sessions"],100),
                  "aov":ratio(today[k]["revenue"],today[k]["orders"]),"revenue_per_session":ratio(today[k]["revenue"],today[k]["sessions"]),
                  "spend":None,"roas_pct":None,"cpa":None,"recommendation":"review_only"} for k in MEDIA if k in today]
    days=[(end-timedelta(days=i)).isoformat() for i in range(6,-1,-1)]
    out["period_start"],out["period_end"]=days[0],days[-1]
    docs={d["end_date"]:d}
    for doc in history or []:
        if isinstance(doc,dict) and doc.get("end_date") in days:
            docs.setdefault(doc["end_date"],doc)
    costs=costs if isinstance(costs,dict) else {}
    out["latest_cost_date"]=costs.get("latest_date")
    if costs.get("status")!="verified" or costs.get("currency")!="KRW" or not costs.get("provenance") or costs.get("scope")!="paid_detail_media_reconciled":
        out["cost_status"]=costs.get("status","missing")
        out["issues"].append("Verified platform spend, media mapping and scope required");return out
    out["cost_status"]="verified_input"
    if any(day not in docs for day in days):
        out["issues"].append("Incomplete seven-day GA4 history");return out
    try:
        byday={}
        for day in days:
            doc=docs[day]
            meta=doc.get("meta") or {}
            if meta.get("cur_start")!=day or meta.get("cur_end")!=day: raise ValueError("Unmatched source day")
            byday[day]=paid_rows(doc)
        # Cost rows are daily and reconciled to the same GA4 Paid detail scope.
        cost_map={}
        for row in costs.get("rows",[]):
            day=str(row.get("date",""))
            if day not in days: continue
            key=str(row.get("media","")).strip().lower()
            if key not in MEDIA: raise ValueError("Unmapped spend")
            val=number(row.get("spend"))
            if val is None or (day,key) in cost_map: raise ValueError("Invalid or duplicate spend")
            cost_map[day,key]=val
        if set(cost_map)!={(day,k) for day in days for k in MEDIA}: raise ValueError("Missing media/day spend")
        for day in days:
            if any(k not in byday[day] for k in MEDIA): raise ValueError("Missing GA4 media")
            other=sum(v["revenue"] for k,v in byday[day].items() if k not in MEDIA)
            total=sum(v["revenue"] for v in byday[day].values())
            if total and other/total>.01: raise ValueError("Material unmapped revenue")
        # Compare independent verified cost controls to the rows themselves.
        controls=costs.get("daily_totals") or {}
        if any(number(controls.get(day)) is None or abs(sum(cost_map[day,k] for k in MEDIA)-controls[day])>.01 for day in days):
            raise ValueError("Spend control total mismatch")
    except (KeyError,TypeError,ValueError):
        out["status"]="validation_required";out["issues"].append("Date, mapping, completeness or spend reconciliation failed");return out
    results=[]
    for k in MEDIA:
        vals={m:sum(byday[day][k][m] for day in days) for m in ("sessions","orders","revenue")}
        spend=sum(cost_map[day,k] for day in days)
        results.append({"media":k,**vals,"spend":spend,"roas_pct":ratio(vals["revenue"],spend,100),
                        "cpa":ratio(spend,vals["orders"]),"cvr_pct":ratio(vals["orders"],vals["sessions"],100),
                        "aov":ratio(vals["revenue"],vals["orders"]),"revenue_per_session":ratio(vals["revenue"],vals["sessions"]),
                        "recommendation":"protected" if k in PROTECTED else "hold"})
    out["media"]=results
    out["cost_status"]="reconciled"
    out["status"]="available"
    out["total_spend"]=sum(r["spend"] for r in results)
    out["total_revenue"]=sum(r["revenue"] for r in results)
    out["total_roas_pct"]=ratio(out["total_revenue"],out["total_spend"],100)
    eligible=[r for r in results if r["media"] not in PROTECTED and r["orders"]>=MIN_ORDERS and r["spend"]>0]
    eligible.sort(key=lambda r:r["roas_pct"])
    if len(eligible)<2 or eligible[-1]["roas_pct"]<=eligible[0]["roas_pct"]*1.2 or out["total_spend"]<=0:
        out["issues"].append("No sufficiently differentiated, adequately sampled test pair");return out
    low,high=eligible[0],eligible[-1]
    move=int(min(low["spend"]*MAX_MOVE,high["spend"]*MAX_MOVE,out["total_spend"]*.05))
    if move<1: return out
    low["recommendation"]="test_reduce";high["recommendation"]="test_increase"
    proposed=[{"media":r["media"],"baseline_spend":r["spend"],"change":move if r is high else -move if r is low else 0,
               "proposed_spend":r["spend"]+(move if r is high else -move if r is low else 0)} for r in results]
    assert abs(sum(x["proposed_spend"] for x in proposed)-out["total_spend"])<.01
    out["scenario"]={"type":"budget_neutral_test","move_krw":move,"from":low["media"],"to":high["media"],"allocations":proposed,
                     "forecast_revenue":None,"incremental_revenue":None,"review_days":7,
                     "note":"Historical average ROAS ranking is not marginal ROAS; hold total spend, review after seven days, require human approval."}
    return out
