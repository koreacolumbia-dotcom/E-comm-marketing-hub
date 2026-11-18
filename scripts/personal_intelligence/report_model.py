"""Independent personal report model; no extra warehouse queries."""
from math import isfinite
from statistics import mean
from engine import revenue_bridge
from product_card import enrich_cards
from business_diagnosis import build_diagnosis

SECTIONS=("AI BUSINESS DIAGNOSIS","Executive Summary","Root Cause Analysis","Weather Intelligence","Product Intelligence","Product Early Warning","Competitive Intelligence","Competitive Discount Matrix","Customer Intelligence","Marketing Attribution","Budget Optimizer","Social Listening","Inventory Intelligence","Integrated Calendar","Business Impact Matrix","Predictive Intelligence","Executive AI Strategy")

def change(a,b):
    return (a/b-1)*100 if b else None


def executive_summary(d):
    """GA4 session-based metrics; admin zero placeholders are not used."""
    overall=d.get("overall",{})
    def vals(key):
        x=overall.get(key) or {}
        v={"sessions":x.get("sessions"),"orders":x.get("transactions"),"revenue":x.get("purchaseRevenue")}
        def ratio(n,den):
            return v[n]/v[den] if v[n] is not None and v[den] else None
        v["cvr"]=ratio("orders","sessions")
        v["aov"]=ratio("revenue","orders")
        v["signups"]=(d.get("signup_users") or {}).get(key)
        return v
    cy,ly,pw=map(vals,("current","yoy","prev"))
    def diff(a,b,k):
        if a is None or b is None: return None
        return (a-b)*100 if k=="cvr" else change(a,b)
    cards=[{"metric":k,"current":v,"prior":ly[k],"prior_week":pw[k],
        "yoy":diff(v,ly[k],k),"wow":diff(v,pw[k],k),
        "delta_unit":"pp" if k=="cvr" else "percent"} for k,v in cy.items()]
    return {"current":cy,"prior":ly,"prior_week":pw,"signups":cy["signups"],"cards":cards,
        "period":d.get("period"),"compare":d.get("compare"),"yoy_period":d.get("yoy"),
        "source":overall.get("current",{}).get("source"),"built_at_kst":d.get("built_at_kst"),
        "comparison":"weekday aligned per source; not calendar-date YoY"}


def weather_business(d,events=None):
    from datetime import date,timedelta
    end=date.fromisoformat(d["end_date"])
    axis=[(end+timedelta(days=i)).isoformat() for i in range(-29,8)]
    prior=[(date.fromisoformat(x)-timedelta(days=364)).isoformat() for x in axis]
    observed=d.get("weather_observed") or {}
    if not isinstance(observed,dict): observed={}
    def temp(day):
        x=observed.get(day) or {}
        v=x.get("mean_temp")
        return v if x.get("verified") is True and x.get("kind")=="observed" and str(x.get("station"))=="108" and isinstance(v,(int,float)) and isfinite(v) else None
    def aligned(dates,values):
        lookup=dict(zip(dates,values or []))
        return [lookup.get(x) for x in axis]
    trend=d.get("trend_series") or {}
    result={"dates":axis,"prior_dates":prior,"actual_through":end.isoformat(),
        "temperature_current":[temp(x) for x in axis],
        "temperature_prior":[temp(x) for x in prior],
        "forecast":d.get("weather_forecast"),"events":events or [],
        "overall":{k:aligned(trend.get("dates",[]),trend.get(k)) for k in ("sessions","revenue","cvr")},
        "overall_source":"GA4 trend series (not BQ headline KPI)","categories":[]}
    pdp=d.get("pdp_series") or {}
    def resolve(labels,last):
        n=len(labels or [])
        ds=[(last-timedelta(days=n-1-i)).isoformat() for i in range(n)]
        return ds if all(date.fromisoformat(x).strftime("%m/%d")==l for x,l in zip(ds,labels or [])) else []
    current_dates=resolve(pdp.get("x"),end)
    prior_dates=resolve(pdp.get("yoy_x"),end-timedelta(days=364))
    for row in pdp.get("rows",[]):
        a,b=row.get("ys") or [],row.get("ys_yoy") or []
        prior_lookup=dict(zip(prior_dates,b)) if row.get("has_yoy") else {}
        cy=aligned(current_dates,a)
        ly=[prior_lookup.get(x) for x in prior]
        pairs=[(u,v) for u,v in zip(cy,ly) if u is not None and v is not None]
        result["categories"].append({"name":row.get("itemCategory"),"pdp_current":cy,"pdp_prior":ly,
            "matched_yoy_pct":change(sum(u for u,v in pairs),sum(v for u,v in pairs)) if pairs else None,
            "matched_days":len(pairs),"sales_current":None,"sales_prior":None})
    result["missing"]=["category daily sales","prior overall trend"]
    if any(v is None for v in result["temperature_current"][:30]+result["temperature_prior"][:30]):result["missing"].append("complete verified historical ASOS 108 observations")
    result["interpretation"]="Date-aligned observations; no causal weather or promotion attribution. Missing is null, not zero."
    return result


def root_cause(d, current, prior):
    from math import isclose
    try: effects=revenue_bridge(current,prior)
    except (ValueError,KeyError,TypeError,ZeroDivisionError): effects=None
    delta=current["revenue"]-prior["revenue"]
    rows=[]
    for x in d.get("channel_snapshot",[]):
        if x.get("bucket")=="Total":continue
        a,b=x.get("purchaseRevenue"),x.get("revenue_yoy")
        if not isinstance(a,(int,float)) or not isfinite(a):continue
        old=a/(1+b) if a>0 and isinstance(b,(int,float)) and isfinite(b) and b>-1 else None
        rows.append({"channel":x.get("bucket"),"current":a,"prior_estimate":old,
            "delta_estimate":a-old if old is not None else None,
            "revenue_yoy_pct":b*100 if isinstance(b,(int,float)) else None})
    known=sum(x["delta_estimate"] for x in rows if x["delta_estimate"] is not None)
    rows.sort(key=lambda x:(x["delta_estimate"] is None,x["delta_estimate"] or 0))
    return {"current":current,"prior":prior,"delta":delta,"effects":effects,
        "reconciled":effects is not None and isclose(sum(effects.values()),delta,abs_tol=.01),
        "channels":rows,"channel_unallocated_delta":delta-known,
        "channel_current_reconciled":isclose(sum(x["current"] for x in rows),current["revenue"],abs_tol=.01),
        "channel_note":"Prior estimated from reported revenue_yoy. Zero or unavailable bases remain unknown; residual is not assigned to a channel.",
        "causal":False,"separate_decompositions":True}


def product_intelligence(d):
    pdp=d.get("pdp_series") or {}
    dates=pdp.get("x") or []; prior_dates=pdp.get("yoy_x") or []
    rows=[]
    for r in pdp.get("rows",[]):
        a,b=r.get("ys") or [],r.get("ys_yoy") or []
        valid=lambda seq,n: len(seq)==n and n>0 and all(isinstance(v,(int,float)) and isfinite(v) and v>=0 for v in seq)
        current_ok=valid(a,len(dates)) and r.get("views_d1")==a[-1]
        prior_ok=r.get("has_yoy") and valid(b,len(prior_dates)) and len(a)==len(b)
        quality="check_zero_series" if current_ok and not any(a) and prior_ok and any(b) else ("reported" if current_ok else "invalid")
        comparable=current_ok and prior_ok and quality!="check_zero_series"
        rows.append({"name":r.get("itemCategory"),"current":a if current_ok else None,"prior":b if prior_ok else None,
            "daily_views":a[-1] if current_ok else None,"weekly_views":sum(a) if current_ok else None,
            "day_yoy_pct":change(a[-1],b[-1]) if comparable else None,
            "week_yoy_pct":change(sum(a),sum(b)) if comparable else None,
            "week_delta":sum(a)-sum(b) if comparable else None,
            "quality":quality,"sources_current":r.get("top_sources") if r.get("source_status")=="ok" else None,
            "sources_prior":r.get("top_sources_yoy") if r.get("source_status_yoy")=="ok" else None,
            "source_status":r.get("source_status"),"source_status_prior":r.get("source_status_yoy"),
            "sales_units":None,"sales_status":"category sales source required"})
    ranked=[x for x in rows if x["week_delta"] is not None]
    return {"dates":dates,"prior_dates":prior_dates,"categories":rows,
        "growth":sorted([x for x in ranked if x["week_delta"]>0],key=lambda x:-x["week_delta"])[:3],
        "declines":sorted([x for x in ranked if x["week_delta"]<0],key=lambda x:x["week_delta"])[:3],
        "sample_best_sellers":d.get("best_sellers") or [],"best_seller_scope":"reported top SKU sample, not full style totals",
        "missing":["category sales totals","reconciled category sources where unavailable"]}


def product_intelligence(d):
    """Category demand, matched daily comparisons, and reported SKU sales; no invented category sales."""
    from statistics import mean
    pdp=d.get("pdp_series") or {}
    rows=[]
    for r in pdp.get("rows",[]):
        a,b=r.get("ys") or [],r.get("ys_yoy") or []
        if not a or any(not isinstance(v,(int,float)) or not isfinite(v) or v<0 for v in a):continue
        matched=r.get("has_yoy") and len(a)==len(b) and all(isinstance(v,(int,float)) and isfinite(v) and v>=0 for v in b)
        valid=r.get("views_d1")==a[-1]
        rows.append({**r,"day_yoy_pct":change(a[-1],b[-1]) if matched and valid else None,
            "vs_prior_days_pct":change(a[-1],mean(a[:-1])) if len(a)>1 and valid else None,
            "week_yoy_pct":change(sum(a),sum(b)) if matched and valid else None,
            "day_delta":a[-1]-b[-1] if matched and valid else None,
            "week_delta":sum(a)-sum(b) if matched and valid else None,
            "trend_status":"review" if not valid or not any(a) else "available",
            "sales_units":None,"source_detail_verified":r.get("source_status") in ("ok","reconciled")})
    eligible=[x for x in rows if x["day_delta"] is not None and x["trend_status"]=="available"]
    sku_rows=[{k:r.get(k) for k in ("itemId","itemName","qty","views","revenue","image_url")} for r in d.get("best_sellers",[])]
    return {"dates":pdp.get("x"),"prior_dates":pdp.get("yoy_x"),"categories":rows,
        "growth":sorted([x for x in eligible if x["day_delta"]>0],key=lambda x:-x["day_delta"])[:5],
        "declines":sorted([x for x in eligible if x["day_delta"]<0],key=lambda x:x["day_delta"])[:5],
        "sku_sales_sample":sku_rows,"sku_sales_series":d.get("best_sellers_series"),
        "category_sales_status":"source_required","scope":"PDP category counts may overlap; SKU top-N is not full catalog coverage"}

def build_model(d,competitor=None,events=None,product_evidence=None):
    model={name:{"status":"source_required","data":None} for name in SECTIONS}
    def put(name,data,status="available"):
        model[name]={"status":status,"data":data}
    put("AI BUSINESS DIAGNOSIS", build_diagnosis(d), "partial")
    overall=d["overall"]
    def row(x): return {"sessions":x["sessions"],"orders":x["transactions"],"revenue":x["purchaseRevenue"]}
    cy,ly=row(overall["current"]),row(overall["yoy"])
    put("Executive Summary",executive_summary(d))
    put("Root Cause Analysis",root_cause(d,cy,ly),"partial")
    weather=d.get("weather_forecast",{})
    put("Weather Intelligence",weather_business(d,events),"partial")
    pdp=d.get("pdp_series",{})
    categories=[]
    for r in pdp.get("rows",[]):
        a,b=r.get("ys",[]),r.get("ys_yoy",[])
        if not a: continue
        previous=a[:-1]
        categories.append({**r,"day_yoy_pct":change(a[-1],b[-1]) if b and r.get("has_yoy") else None,"vs_prior_days_pct":change(a[-1],mean(previous)) if previous else None})
    if categories: put("Product Intelligence",product_intelligence(d),"partial")
    valid=[r for r in categories if r["day_yoy_pct"] is not None]
    sales=d.get("best_sellers_series",{})
    candidates=[]
    products={r["itemId"]:r for r in d.get("best_sellers",[])}
    names={k:r["itemName"] for k,r in products.items()}
    for r in sales.get("items",[]):
        a,b=r.get("ys"),r.get("ys_yoy")
        if not a: continue
        candidates.append({"name":names.get(r["itemId"],r["itemId"]),"sku":r["itemId"],"image_url":products.get(r["itemId"],{}).get("image_url"),"daily_pdp":products.get(r["itemId"],{}).get("views"),"pdp_series":None,"sales_dates":sales.get("x"),"prior_dates":sales.get("yoy_x"),"daily_change_pct":change(a[-1],a[-2]) if len(a)>1 else None,"series":a,"prior_series":b,"scope":r.get("comparison_scope"),"yoy_pct":change(sum(a),sum(b)) if b else None})
    enrich_cards(candidates,weather,events,product_evidence)
    put("Product Early Warning",{"category_declines":sorted(valid,key=lambda r:r["day_yoy_pct"])[:5],"category_growth":sorted(valid,key=lambda r:-r["day_yoy_pct"])[:5],"sample_sku_sales":candidates,"style_aggregation_available":False},"partial")
    if competitor: put("Competitive Intelligence",competitor,"partial")
    # Never infer zero discounts or absence of promotions from missing evidence.
    put("Competitive Discount Matrix",{"brands":10,"verified_cells":[],"unknown_is_zero":False},"source_required")
    channels=d.get("channel_snapshot",[])
    if channels: put("Marketing Attribution",{"reported_channel_rows":channels,"causal_incrementality":None},"partial")
    if d.get("glink_paid_snapshot"): put("Budget Optimizer",{"reported_paid_snapshot":d["glink_paid_snapshot"],"marginal_roas":None},"partial")
    put("Integrated Calendar",{"events":events or [],"pdp":pdp,"sample_product_sales":sales,"weather_forecast":weather,"historical_temperature":None,"category_sales":None},"partial")
    put("Business Impact Matrix",{"observed_dimensions":["traffic","conversion","aov","category PDP"],"causal_effects":None},"partial")
    put("Predictive Intelligence",{"forecast":None,"reason":"Backtest and prediction interval validation required"},"validation_required")
    put("Executive AI Strategy",{"actions":["Investigate largest negative revenue bridge contribution","Review category declines across every product category","Verify size/color availability before attributing lost sales"]},"partial")
    return {"date":d.get("end_date"),"sections":model}
