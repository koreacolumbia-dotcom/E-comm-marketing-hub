"""Sections 15–18: aligned calendar, accounting impact, validated baseline and action queue.
All inputs are aggregate cached reports. No warehouse jobs or external account mutations.
"""
from datetime import date,timedelta
from statistics import mean,median
from math import isfinite,sqrt,ceil
from collections import defaultdict
from engine import revenue_bridge

def day(x):
    try:return date.fromisoformat(str(x))
    except (ValueError,TypeError):return None
def num(x):
    return float(x) if isinstance(x,(int,float)) and not isinstance(x,bool) and isfinite(x) and x>=0 else None
def signed(x):
    return float(x) if isinstance(x,(int,float)) and not isinstance(x,bool) and isfinite(x) else None
def change(a,b):return 100*(a/b-1) if a is not None and b is not None and b>0 else None
def range_days(start,end):return [(start+timedelta(days=i)).isoformat() for i in range((end-start).days+1)]
def trend_values(d,metric):
    t=d.get("trend_series") or {}
    dates=t.get("dates") or []; vals=t.get(metric) or []
    end=day(d.get("end_date"))
    if not end or len(dates)!=len(vals) or not dates or dates[-1]!=end.isoformat() or dates!=range_days(end-timedelta(days=len(dates)-1),end):return {}
    return {k:num(v) for k,v in zip(dates,vals)}

def integrated_calendar(d,competitor=None,internal=None,owned=None):
    end=day(d["end_date"]); dates=range_days(end-timedelta(days=29),end+timedelta(days=7))
    active=defaultdict(list);seen=set();registered=[]
    def add(label,title,a,b,status,link=None):
        start,stop=day(a),day(b)
        if not start or not stop or stop<start or not title:return
        if stop<day(dates[0]) or start>day(dates[-1]):return
        k=(label,title,a,b,status)
        if k in seen:return
        seen.add(k)
        event={"lane":label,"title":title,"start":a,"end":b,"status":status,"url":link}
        registered.append(event)
        for x in range_days(max(start,day(dates[0])),min(stop,day(dates[-1]))):active[x].append(event)
    c=competitor or {}
    for e in c.get("events") or []:
        label=str(e.get("brand_name") or e.get("brand") or "Competitor")
        if e.get("date_status") in ("official_period","first_party_landing_text") and e.get("source_url"):
            add(label,e.get("title"),e.get("start_date"),e.get("end_date"),"official_period",e.get("source_url"))
        elif e.get("date_status")=="observed_only":
            for dt in e.get("observed_on") or []:add(label,e.get("title"),dt,dt,"observed_only",e.get("source_url"))
    internal=internal or {}
    for e in internal.get("entries") or []:
        if e.get("status") in ("confirmed","planned","verified"):
            add(e.get("row") or "Owned",e.get("title"),e.get("start") or e.get("date"),e.get("end") or e.get("date"),e["status"])
    # On a matching daily owned cache, count only explicit logged sends, not customer or message content.
    owned=owned or {}; logs=defaultdict(float)
    owned_valid=owned.get("loaded_date")==end.isoformat()
    if owned_valid:
        for r in owned.get("message_log") or []:
            dt=r.get("date");channel=r.get("channel");n=num(r.get("send_count"))
            if dt in dates and day(dt)<=end and channel in ("LMS","KAKAO","EDM") and n is not None and n>0:logs[dt,channel]+=n
        for (dt,channel),n in logs.items():add(channel,f"{channel} · {n:g} logged sends",dt,dt,"logged_send")
    w=d.get("weather_forecast") or {}
    forecast={}
    if w.get("status")=="ok" and w.get("source")=="KMA API Hub":
        for x in w.get("days") or []:
            dt=day(x.get("date"));lo=signed(x.get("min_temp"));hi=signed(x.get("max_temp"));pop=num(x.get("pop"))
            if dt and dt>end and lo is not None and hi is not None and lo<=hi:
                forecast[dt.isoformat()]={"min":lo,"max":hi,"pop":pop if pop is not None and pop<=100 else None,"kind":"forecast"}
    observed=d.get("weather_observed") or {};observed=observed if isinstance(observed,dict) else {}
    revenue=trend_values(d,"revenue");sessions=trend_values(d,"sessions");cvr=trend_values(d,"cvr")
    rows=[]
    for dt in dates:
        temp=observed.get(dt) or {}; temp=temp if isinstance(temp,dict) else {}
        lo,hi=signed(temp.get("min_temp")),signed(temp.get("max_temp"))
        actual={"kind":"observed","min":lo,"max":hi} if day(dt)<=end and temp.get("verified") is True and temp.get("kind")=="observed" and str(temp.get("station"))=="108" and lo is not None and hi is not None and lo<=hi else None
        ev=active[dt]
        rows.append({"date":dt,"period":"actual" if day(dt)<=end else "future","revenue":revenue.get(dt),"sessions":sessions.get(dt),
                     "cvr_pct":100*cvr[dt] if cvr.get(dt) is not None else None,"weather":actual or forecast.get(dt),
                     "events":ev})
    return {"status":"partial","date":end.isoformat(),"days":rows,"events":registered,"calendar_as_of":c.get("as_of"),
            "owned_log_status":"matched_cache" if owned_valid else "source_required","observed_weather_days":sum(bool(r["weather"] and r["weather"]["kind"]=="observed") for r in rows),
            "official_event_count":sum(e["status"]=="official_period" for e in registered),
            "note":"30-day GA4 trend through report date + 7 forecast days. Competitor calendar as-of may be later than the report date. Observed banners are not verified event periods; absent logs are unknown, not zero. Only logged sends, no message/customer details."}

def impact_matrix(d,calendar=None):
    o=d.get("overall") or {};cur=o.get("current") or {};prior=o.get("yoy") or {}
    rows=[];effects=None;delta=None
    def metric(r):return {"sessions":r["sessions"],"orders":r["transactions"],"revenue":r["purchaseRevenue"]}
    try:
        effects=revenue_bridge(metric(cur),metric(prior));delta=cur["purchaseRevenue"]-prior["purchaseRevenue"]
        if abs(sum(effects.values())-delta)>.01:raise ValueError()
    except (ValueError,KeyError,TypeError,ZeroDivisionError):effects=None;delta=None
    if effects:
        for k,v in sorted(effects.items(),key=lambda x:x[1]):rows.append({"factor":k,"accounting_delta":round(v,2),"direction":"negative" if v<0 else "positive" if v>0 else "flat","evidence":"exact_accounting_bridge","causal":False})
    a=(calendar or {}).get("days") or []
    # Descriptive comparison, not a randomized experiment and no inferred event-free days.
    groups=defaultdict(list)
    for r in a:
        v=num(r.get("revenue"))
        if r.get("period")!="actual" or v is None:continue
        for e in r.get("events") or []:
            if e.get("status")=="logged_send":groups[e["lane"]].append((r["date"],v))
    overlaps=[]
    for channel,arr in groups.items():
        uniq=dict(arr)
        overlaps.append({"signal":channel,"matched_days":len(uniq),"mean_revenue_on_logged_days":mean(uniq.values()) if uniq else None,"incremental_revenue":None,"evidence":"descriptive_overlap"})
    return {"status":"partial" if effects else "source_required","date":d.get("end_date"),"revenue_delta":delta,"drivers":rows,
            "reconciled":bool(effects),"overlaps":overlaps,"unmeasured":["historical observed weather effects","price and inventory effects","causal media/CRM lift","competitor causal effect"],
            "note":"Shapley accounting bridge partitions observed YoY revenue change into traffic, CVR and AOV. Logged-event overlap is descriptive, not attribution. Do not add channel or calendar effects to the accounting bridge."}

def collect_series(d,history=()):
    # Use dated GA4 trend snapshots; newer snapshot supersedes older overlap. No mixed headline session definition.
    end=day(d["end_date"]); allvals={}
    for doc in sorted([x for x in [*history,d] if day(x.get("end_date")) and day(x["end_date"])<=end],key=lambda x:x["end_date"]):
        if doc.get("mode")!="daily" or (doc.get("meta") or {}).get("cur_end")!=doc["end_date"]:continue
        rev=trend_values(doc,"revenue")
        ref=((doc.get("overall") or {}).get("current") or {}).get("ga_api_reference") or {}
        target=num(ref.get("purchaseRevenue"))
        last=rev.get(doc["end_date"])
        if target is None or last is None or abs(target-last)>1:continue
        allvals.update(rev)
    dates=[];values=[]
    for i in range(90):
        k=(end-timedelta(days=i)).isoformat()
        if allvals.get(k) is None:break
        dates.append(k);values.append(allvals[k])
    return dates[::-1],values[::-1]
def block_forecast(y,method):
    if method=="seasonal_naive":return list(y[-7:])
    if method=="moving_average":return [mean(y[-7:])]*7
    if method=="weekday_median":return [median(y[-7*i+j] for i in (1,2,3,4)) for j in range(7)]
    raise ValueError(method)
def wape(pairs):
    den=sum(y for _,y in pairs)
    return sum(abs(p-y) for p,y in pairs)/den if den>0 else None
def quantile_abs(pairs):
    a=sorted(abs(p-y) for p,y in pairs);return a[min(len(a)-1,ceil(.8*len(a))-1)]
def predict(d,history=()):
    dates,y=collect_series(d,history)
    out={"status":"source_required","date":d.get("end_date"),"history_days":len(y),"history_dates":dates[-30:],"history_revenue":y[-30:],
         "forecast":[],"model":None,"holdout_wape_pct":None,"baseline_wape_pct":None,"interval_coverage_pct":None,
         "note":"Seven-day GA4 attributable revenue baseline, no campaign/weather uplift or causal forecast. Requires at least 56 contiguous reconciled daily trend days; tuning and final holdout are separate."}
    if len(y)<56:return out
    methods=("seasonal_naive","moving_average","weekday_median");split=len(y)-14;origins=range(28,split-6,7)
    tuning={m:[(p,v) for i in origins for p,v in zip(block_forecast(y[:i],m),y[i:i+7])] for m in methods}
    if any(len(tuning[m])<14 or wape(tuning[m]) is None for m in methods):return out
    chosen=min(methods,key=lambda m:wape(tuning[m]))
    hold={m:[(p,v) for i in (split,split+7) for p,v in zip(block_forecast(y[:i],m),y[i:i+7])] for m in methods}
    test=wape(hold[chosen]);base=wape(hold["seasonal_naive"])
    if test is None or base is None:return out
    err=quantile_abs(tuning[chosen]);coverage=mean(abs(p-v)<=err for p,v in hold[chosen])*100
    out.update(status="validation_required",model=chosen,holdout_wape_pct=round(test*100,2),baseline_wape_pct=round(base*100,2),
               interval_coverage_pct=round(coverage,1),tuning_days=len(tuning[chosen]),holdout_days=14)
    if test>.35 or test>base+1e-9 or coverage<70:
        out["note"]+=" Holdout gate failed (WAPE ≤35%, no worse than seasonal naive, empirical band coverage ≥70%); forecast withheld."
        return out
    end=day(d["end_date"]);pred=block_forecast(y,chosen)
    out.update(status="available",forecast=[{"date":(end+timedelta(days=i+1)).isoformat(),"revenue":round(p),"lower":round(max(0,p-err)),"upper":round(p+err)} for i,p in enumerate(pred)],
        note=out["note"]+" Band uses tuning absolute error 80th percentile; not a guaranteed probability or an independent prediction interval. Only 14 holdout days.")
    return out

def strategy(d,impact,forecast,inventory=None,budget=None,social=None):
    tasks=[]
    def add(p,team,action,evidence,kpi,kind="investigate"):tasks.append({"priority":p,"owner_role":team,"action":action,"evidence":evidence,"success_metric":kpi,"decision_type":kind})
    for r in (impact or {}).get("drivers",[]):
        if r["accounting_delta"]>=0:continue
        k=r["factor"]
        action={"traffic":"Review source/medium traffic losses and campaign delivery","conversion":"Audit PDP-to-cart, checkout and channel mix","aov":"Review product price mix and basket composition"}[k]
        add("P1" if not tasks else "P2","E-commerce / Media",action,f"YoY accounting contribution: KRW {r['accounting_delta']:,.0f}",{"traffic":"Qualified sessions","conversion":"CVR and funnel rates","aov":"AOV and units/order"}[k])
    if (inventory or {}).get("stock_source_status")!="current_sample_verified":add("P1","Merchandising / IT","Validate online sellable size stock before diagnosing shortage","Stock feed not verified","Matched SKU coverage","data_recovery")
    if (budget or {}).get("cost_status")!="reconciled":add("P1","Media / Agency","Refresh date-aligned spend and verify media mapping","Paid cost not reconciled","Daily spend reconciliation","data_recovery")
    if (forecast or {}).get("status")=="available":add("P2","E-commerce","Review seven-day revenue baseline against approved plan","Holdout gate passed","Daily forecast error","monitor")
    else:add("P2","Analytics","Extend or validate historical forecast series","Forecast gate not passed","History coverage / holdout WAPE","data_recovery")
    if (social or {}).get("observed") is not None:add("P3","Brand / CX","Review logged consumer issues and product VOC","Observed sample, not population","Issue resolution / verified reviews")
    tasks.sort(key=lambda x:x["priority"])
    return {"status":"partial","date":d.get("end_date"),"actions":tasks[:8],"auto_execute":False,"expected_incremental_revenue":None,
            "note":"Evidence-driven rule-based daily decision support, not generative causal recommendations. Suggested role owners; human approval required; no ads, budget or pricing changes."}
