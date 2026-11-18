"""Evidence-first daily business diagnosis; never claims causality from correlations."""
from math import isfinite
from engine import revenue_bridge

def _row(raw):
    return {k: float(raw[v]) for k,v in (("sessions","sessions"),("orders","transactions"),("revenue","purchaseRevenue"))}


def _observations(report):
    """Rank comparable observations without inventing prior values or funnel data."""
    channels=[]
    for group, rows in (report.get("channel_detail_map") or {}).items():
        for row in rows or []:
            if not isinstance(row,dict): continue
            current, change=row.get("purchaseRevenue"),row.get("revenue_yoy")
            if not isinstance(current,(int,float)) or not isinstance(change,(int,float)): continue
            if not isfinite(current) or not isfinite(change) or current<=0 or change<=-1: continue
            prior=current/(1+change)
            channels.append(dict(group=group,label=row.get("sub_channel"),current=current,
                prior_estimate=round(prior,2),delta_estimate=round(current-prior,2),
                yoy_pct=round(change*100,2),evidence="prior reconstructed from reported YoY; not raw attribution"))
    channels.sort(key=lambda x:x["delta_estimate"])
    pdp=[]
    for row in (report.get("pdp_series") or {}).get("rows",[]):
        a,b=row.get("ys",[]),row.get("ys_yoy",[])
        if not row.get("has_yoy") or not a or len(a)!=len(b): continue
        if any(not isinstance(v,(int,float)) or not isfinite(v) or v<0 for v in a+b): continue
        if row.get("views_d1")!=a[-1]: continue
        pdp.append(dict(label=row.get("itemCategory"),current=a[-1],prior=b[-1],
            delta=a[-1]-b[-1],yoy_pct=round((a[-1]/b[-1]-1)*100,2) if b[-1]>0 else None,
            week_delta=sum(a)-sum(b),source_status=row.get("source_status"),
            validation="Verify taxonomy and source reconciliation" if a[-1]==0 or row.get("source_status")!="ok" else "Compare PDP-to-cart and purchase funnel"))
    pdp.sort(key=lambda x:x["delta"])
    actions=[]
    if channels and channels[0]["delta_estimate"]<0:
        x=channels[0];actions.append(dict(priority="P1",check="Investigate "+str(x["label"])+" revenue decline: traffic, CVR, AOV and campaign mix",dimension="channel"))
    if pdp and pdp[0]["delta"]<0:
        x=pdp[0];actions.append(dict(priority="P2",check=str(x["label"])+": "+x["validation"],dimension="pdp"))
    return dict(channel_observations=channels,pdp_observations=pdp,
        positive_channel_signals=sorted([x for x in channels if x["delta_estimate"]>0],key=lambda x:-x["delta_estimate"])[:3],
        positive_pdp_signals=sorted([x for x in pdp if x["delta"]>0],key=lambda x:-x["delta"])[:3],
        targeted_actions=actions)


def product_cross_analysis(report):
    """Use complete style/day extracts only for dated comparisons; top-N is a candidate feed."""
    from style_anomaly import rank_styles
    from price_impact import price_impacts
    from statistics import median
    rows=report.get("style_daily_rows") or []
    weather=report.get("weather_observed") or {}
    prices=report.get("price_history") or {}
    result={"status":"partial","cards":[],"candidates":[],"missing":[]}
    if not isinstance(weather,dict): weather={}
    if isinstance(prices,list):
        grouped={}
        for x in prices:
            if isinstance(x,dict) and x.get("sku"): grouped.setdefault(x["sku"],[]).append(x)
        prices=grouped
    if not isinstance(prices,dict): prices={}
    if not weather: result["missing"].append("verified observed daily mean temperature")
    if not prices: result["missing"].append("verified dated official SKU prices")
    if not rows or report.get("style_daily_complete") is not True:
        result["missing"].append("complete style/day sales and PDP history")
        for x in sorted(report.get("rising") or [],key=lambda x:abs(x.get("delta") or 0),reverse=True)[:6]:
            result["candidates"].append({k:x.get(k) for k in ("itemId","itemName","qty","views","delta","delta_label")})
        return result
    ranked=rank_styles(rows,report["end_date"])
    def temperature(d):
        x=weather.get(d)
        if not isinstance(x,dict) or x.get("verified") is not True or x.get("station") not in (108,"108") or x.get("kind")!="observed": return None
        v=x.get("mean_temp")
        return v if isinstance(v,(int,float)) and isfinite(v) else None
    for card in ranked["cards"]:
        name=card["style_key"]; dates=ranked["dates"]; prior=ranked["prior_dates"]
        sku_ids={x["item_id"] for x in rows if " ".join(x["style_name"].split()).casefold()==name}
        p={}; impacts={}
        metrics={d:{"sales_units":q,"pdp_views":v} for d,q,v in zip(dates,card["series"]["qty"]["current"],card["series"]["pdp"]["current"])}
        for sku in sorted(sku_ids):
            obs=[x for x in prices.get(sku,[]) if x.get("verified") is True and x.get("source_url")]
            p[sku]={x["date"]:{"normal_price":x.get("normal_price",x.get("regular_price")),"sale_price":x.get("sale_price"),"coupon_price":x.get("coupon_price"),"source_url":x.get("source_url")} for x in obs}
            # Style metrics are not SKU metrics: only single-SKU styles get a price-impact window.
            if len(sku_ids)==1: impacts[sku]=price_impacts(obs,metrics)
        card.update(dates=dates,prior_dates=prior,temperature_current=[temperature(d) for d in dates],
            temperature_prior=[temperature(d) for d in prior],prices_by_sku=p,price_impacts=impacts)
        for metric in ("qty","pdp"):
            a=card["series"][metric]["current"]; b=a[-8:-1]
            card[metric+"_delta_vs_median"]=a[-1]-median(b) if a[-1] is not None and all(v is not None for v in b) else None
        card["interpretation"]="Aligned observations only; check advertising, CRM and price/weather overlap before attributing causes."
        result["cards"].append(card)
    if not result["missing"] and all(all(v is not None for v in c["temperature_current"]+c["temperature_prior"]) and all(all(d in p for d in c["dates"]) for p in c["prices_by_sku"].values()) for c in result["cards"]):
        result["status"]="available"
    return result

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
                "validation":"Compare source/medium, campaigns, PDP funnel, weather and pricing against matched prior dates",
                "priority":"P1" if value<0 and value==min(effects.values()) else ("P2" if value<0 else "P3")
            })
    sources={
        "source_medium_campaign":"channel_detail_map",
        "pdp_funnel":"pdp_funnel",
        "crm":"crm",
        "price_discount":"price_history",
        "weather_observed":"weather_observed",
        "competitor_verified":"competitor_verified",
    }
    for name,key in sources.items():
        if not report.get(key):
            missing.append(name)
    positive=[x for x in candidates if x["observed_revenue_contribution"]>0]
    observations=_observations(report)
    return {
        "title":"AI BUSINESS DIAGNOSIS",
        "date":report.get("end_date"),
        "status":"partial" if missing else "available",
        "revenue_change":round(_row(cy)["revenue"]-_row(ly)["revenue"],2) if effects else None,
        "ranked_contributions":candidates,
        "positive_signals":positive,
        "actions":[{"priority":x["priority"],"check":x["validation"],"dimension":x["dimension"]} for x in candidates],
        "missing_evidence":missing,
        **observations,
        "product_cross_analysis":product_cross_analysis(report),
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
