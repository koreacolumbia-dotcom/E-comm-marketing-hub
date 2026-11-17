"""Independent personal report model; no extra warehouse queries."""
from math import isfinite
from statistics import mean
from engine import revenue_bridge

SECTIONS=("Executive Summary","Root Cause Analysis","Weather Intelligence","Product Intelligence","Product Early Warning","Competitive Intelligence","Competitive Discount Matrix","Customer Intelligence","Marketing Attribution","Budget Optimizer","Social Listening","Inventory Intelligence","Integrated Calendar","Business Impact Matrix","Predictive Intelligence","Executive AI Strategy")

def change(a,b):
    return (a/b-1)*100 if b else None

def build_model(d,competitor=None,events=None):
    model={name:{"status":"source_required","data":None} for name in SECTIONS}
    def put(name,data,status="available"):
        model[name]={"status":status,"data":data}
    overall=d["overall"]
    def row(x): return {"sessions":x["sessions"],"orders":x["transactions"],"revenue":x["purchaseRevenue"]}
    cy,ly=row(overall["current"]),row(overall["yoy"])
    put("Executive Summary",{"current":cy,"prior":ly,"signups":d.get("kpis",{}).get("signups")})
    put("Root Cause Analysis",revenue_bridge(cy,ly))
    weather=d.get("weather_forecast",{})
    if weather.get("days"): put("Weather Intelligence",{"forecast":weather,"historical_temperature":None},"partial")
    pdp=d.get("pdp_series",{})
    categories=[]
    for r in pdp.get("rows",[]):
        a,b=r.get("ys",[]),r.get("ys_yoy",[])
        if not a: continue
        previous=a[:-1]
        categories.append({**r,"day_yoy_pct":change(a[-1],b[-1]) if b and r.get("has_yoy") else None,"vs_prior_days_pct":change(a[-1],mean(previous)) if previous else None})
    if categories: put("Product Intelligence",{"dates":pdp.get("x"),"prior_dates":pdp.get("yoy_x"),"categories":categories})
    valid=[r for r in categories if r["day_yoy_pct"] is not None]
    sales=d.get("best_sellers_series",{})
    candidates=[]
    names={r["itemId"]:r["itemName"] for r in d.get("best_sellers",[])}
    for r in sales.get("items",[]):
        a,b=r.get("ys"),r.get("ys_yoy")
        if not a: continue
        candidates.append({"name":names.get(r["itemId"],r["itemId"]),"sku":r["itemId"],"series":a,"prior_series":b,"scope":r.get("comparison_scope"),"yoy_pct":change(sum(a),sum(b)) if b else None})
    put("Product Early Warning",{"category_declines":sorted(valid,key=lambda r:r["day_yoy_pct"])[:5],"category_growth":sorted(valid,key=lambda r:-r["day_yoy_pct"])[:5],"sample_sku_sales":candidates,"style_aggregation_available":False},"partial")
    if competitor: put("Competitive Intelligence",competitor,"partial")
    # Never infer zero discounts or absence of promotions from missing evidence.
    put("Competitive Discount Matrix",{"brands":10,"verified_cells":[],"unknown_is_zero":False},"source_required")
    channels=d.get("channel_snapshot",[])
    if channels: put("Marketing Attribution",{"reported_channel_rows":channels,"causal_incrementality":None},"partial")
    if d.get("glink_paid_snapshot"): put("Budget Optimizer",{"reported_paid_snapshot":d["glink_paid_snapshot"],"marginal_roas":None},"partial")
    if events: put("Integrated Calendar",events,"partial")
    put("Business Impact Matrix",{"observed_dimensions":["traffic","conversion","aov","category PDP"],"causal_effects":None},"partial")
    put("Predictive Intelligence",{"forecast":None,"reason":"Backtest and prediction interval validation required"},"validation_required")
    put("Executive AI Strategy",{"actions":["Investigate largest negative revenue bridge contribution","Review category declines across every product category","Verify size/color availability before attributing lost sales"]},"partial")
    return {"date":d.get("end_date"),"sections":model}
