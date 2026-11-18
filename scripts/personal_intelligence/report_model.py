"""Independent personal report model; no extra warehouse queries."""
from math import isfinite
from statistics import mean
from engine import revenue_bridge
from product_card import enrich_cards
from business_diagnosis import build_diagnosis
from style_anomaly import rank_styles

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


def competitive_intelligence(feed, calendar=None, as_of=None):
    """Reuse crawler and verified calendar feeds without treating missing as no promotions."""
    from datetime import date
    from urllib.parse import urlparse
    feed=feed or {}
    calendar=calendar or {}
    day=as_of or calendar.get("as_of")
    aliases={"TNF":"tnf","K2":"k2","DISCOVERY":"discovery","BLACKYAK":"blackyak","NEPA":"nepa","KOLON SPORT":"kolonsport","KOLONSPORT":"kolonsport","EIDER":"eider","NATIONAL GEOGRAPHIC":"national_geographic","SNOW PEAK":"snowpeak","MILLET":"millet"}
    brands=[]
    for b in feed.get("brands",[]):
        key=aliases.get(str(b.get("brand","")).upper(),str(b.get("brand","")).lower().replace(" ",""))
        events=[e for e in calendar.get("events",[]) if e.get("brand")==key]
        active=[e for e in events if day and e.get("date_status") in ("official_period","first_party_landing_text") and e.get("start_date") and e.get("end_date") and e["start_date"]<=day<=e["end_date"]]
        observed=[e for e in events if e.get("date_status")=="observed_only" and day in (e.get("observed_on") or [])]
        brands.append({"brand":b.get("brand"),"status":b.get("status"),"pages_checked":b.get("pages_checked"),
            "promotion_evidence_pages":b.get("promotion_evidence_pages"),"active_events":active,
            "observed_banners":observed,"errors":b.get("errors",[]),
            "promotion_absence_verified":False})
    return {"checked_at":feed.get("checked_at_kst"),"as_of":day,"coverage":feed.get("coverage"),
        "job_status":feed.get("job_status"),"verification_status":feed.get("verification_status"),
        "brands":brands,"calendar_available":bool(calendar),"source_run":feed.get("run_id"),
        "note":"Official periods and observed banners are distinct; retailer and official mall events retain their channel."}

def competitive_discount_matrix(feed):
    """Same-card arithmetic checked samples; not a catalog-wide discount rate."""
    import re
    from statistics import median
    from urllib.parse import urlparse

    feed = feed or {}
    brands = ("COLUMBIA", "TNF", "K2", "DISCOVERY", "BLACKYAK", "NEPA",
              "KOLON SPORT", "EIDER", "NATIONAL GEOGRAPHIC", "SNOW PEAK", "MILLET")
    sources = {str(b.get("brand", "")).upper(): b for b in feed.get("price_candidates", [])
               if isinstance(b, dict)}
    # Owned is collected separately; do not infer Columbia prices from competitor samples.
    owned = feed.get("owned")
    if isinstance(owned, dict) and "COLUMBIA" not in sources:
        sources["COLUMBIA"] = {
            "paired_discount_cards": [c for p in owned.get("evidence", [])
                                      for c in p.get("product_cards", [])],
            "structured_product_prices": [],
        }
    def category(name):
        s = name.upper()
        if re.search(r"BACKPACK|백팩|배낭|RUCKSACK", s): return "Backpack"
        if re.search(r"HEAVY DOWN|헤비 다운|헤비다운|롱 다운|LONG DOWN", s): return "Heavy Down"
        if re.search(r"LIGHT PACK DOWN|LIGHT DOWN|경량 다운|경량다운|초경량", s): return "Light Down"
        if re.search(r"DOWN|다운", s): return "Down (unspecified)"
        if re.search(r"FLEECE|플리스|후리스", s): return "Fleece"
        if re.search(r"SHOES|BOOTS|슈즈|등산화|트레킹화", s): return "Footwear"
        return "Other"
    def valid_url(x):
        return isinstance(x, str) and urlparse(x).scheme in ("https", "http") and bool(urlparse(x).netloc)
    def style_name(brand, name):
        # Only strip explicit known color suffixes for sources where the title includes color.
        if brand == "SNOW PEAK":
            name = re.sub(r"\s+(?:Light Beige|Ivory|Black|Dark Gray|Dark Grey|Khaki|Navy|Beige|Brown|White)$", "", name, flags=re.I)
        return " ".join(name.split())
    rows, styles, cells = [], [], []
    for brand in brands:
        source = sources.get(brand, {})
        by_style, seen = {}, set()
        rejected = 0
        for c in source.get("paired_discount_cards", []):
            if not isinstance(c, dict): rejected += 1; continue
            try:
                name = str(c["name"]).strip()
                url = c["url"]
                regular, sale, advertised = (float(c[k]) for k in ("regular_price", "sale_price", "discount_pct"))
                if not all(isfinite(n) for n in (regular, sale, advertised)) or not (regular >= sale > 0):
                    raise ValueError("invalid amounts")
                rate = (1-sale/regular)*100
                if not (0 <= advertised <= 100 and abs(rate-advertised) <= 1 and name and valid_url(url)
                        and c.get("status") == "candidate_same_card" and c.get("evidence")):
                    raise ValueError("unpaired evidence")
                item_key = (url.split("#")[0].split("?")[0], name, regular, sale)
                if item_key in seen: continue
                seen.add(item_key)
                label = style_name(brand, name)
                # Keep variant price differences, rather than masking them with one arbitrary price.
                key = label.casefold()
                group = by_style.setdefault(key, {"brand":brand, "name":label, "category":category(label),
                                                 "variants":[], "status":"arithmetic_checked_candidate"})
                if re.search(r"회원전용|회원 전용|회원가|회원 한정", str(c.get("evidence",""))):
                    condition = "member_only"
                else:
                    condition = "unspecified"
                group["variants"].append({"name":name, "url":url, "regular_price":regular, "sale_price":sale,
                                          "discount_pct":round(rate,2), "advertised_pct":advertised,
                                          "condition":condition, "evidence":c.get("evidence")})
            except (KeyError, TypeError, ValueError, OverflowError):
                rejected += 1
        groups = list(by_style.values())
        for g in groups:
            g["variant_count"] = len(g["variants"])
            for k in ("regular_price", "sale_price", "discount_pct"):
                values = [v[k] for v in g["variants"]]
                g[k+"_min"], g[k+"_max"] = min(values), max(values)
                g[k+"_median"] = round(median(values),2)
            g["url"] = g["variants"][0]["url"]
        prices = []
        for p in source.get("structured_product_prices", []):
            try:
                price = float(p["price"])
                if p.get("currency") == "KRW" and isfinite(price) and price > 0 and p.get("name") and valid_url(p.get("url")):
                    prices.append({"name":p["name"], "category":category(p["name"]), "price":price, "url":p["url"],
                                   "discount_pct":None, "status":"sale_price_only"})
            except (KeyError, ValueError, TypeError, OverflowError):
                continue
        style_rates = [g["discount_pct_median"] for g in groups]
        rows.append({"brand":brand, "paired_cards":len(seen), "style_count":len(groups), "price_only_count":len(prices),
                     "sample_discount_median_pct":round(median(style_rates),2) if style_rates else None,
                     "sample_discount_max_pct":max(g["discount_pct_max"] for g in groups) if groups else None,
                     "price_only":prices, "rejected":rejected, "status":"candidate" if groups else "price_only" if prices else "source_required"})
        styles.extend(groups)
        for cat in sorted({g["category"] for g in groups}):
            sample = [g for g in groups if g["category"] == cat]
            cells.append({"brand":brand, "category":cat, "style_count":len(sample),
                          "discount_median_pct":round(median(g["discount_pct_median"] for g in sample),2),
                          "sale_price_min":min(g["sale_price_min"] for g in sample),
                          "sale_price_max":max(g["sale_price_max"] for g in sample), "status":"candidate"})
    competitors = [r for r in rows if r["brand"] != "COLUMBIA"]
    return {"checked_at":feed.get("checked_at_kst"), "source_run":feed.get("run_id"),
            "brands":len(competitors), "brand_rows":rows, "styles":styles, "candidate_cells":cells,
            "verified_cells":[], "unknown_is_zero":False,
            "competitor_brands_with_pairs":sum(r["style_count"]>0 for r in competitors),
            "competitor_paired_cards":sum(r["paired_cards"] for r in competitors),
            "competitor_style_count":sum(r["style_count"] for r in competitors),
            "owned_baseline_available":bool(rows[0]["style_count"]),
            "note":"Same-card candidate prices; arithmetic checked only, not independent verification. Style-weighted sample statistics, not catalog averages. Single price and promotional claims are not discount rates."}

def customer_intelligence(d):
    """Aggregate only. GA4 observed buyers are not ERP lifetime members."""
    def count(v):
        return int(v) if isinstance(v,(int,float)) and not isinstance(v,bool) and isfinite(v) and v>=0 and int(v)==v else None
    def pct(n,den):
        return round(100*n/den,2) if n is not None and den is not None and den>0 else None
    def delta(n,b):
        return round(100*(n/b-1),2) if n is not None and b is not None and b>0 else None
    def pair(r):
        n,e=count(r.get("new_buyers")),count(r.get("existing_buyers"))
        return n,e,n+e if n is not None and e is not None else None
    source_rows=d.get("channel_snapshot") or []
    source_rows=[r for r in source_rows if isinstance(r,dict)]
    totals=[r for r in source_rows if str(r.get("bucket","")).strip().lower()=="total"]
    t=totals[0] if len(totals)==1 else {}
    new,existing,buyers=pair(t)
    orders=count((d.get("overall",{}).get("current") or {}).get("transactions"))
    # One observed purchase can have at most one buyer; fail closed for inconsistent aggregates.
    invalid=buyers is not None and orders is not None and buyers>orders
    if invalid: new=existing=buyers=None
    channels=[]
    seen=set()
    duplicate=False
    for r in source_rows:
        name=str(r.get("bucket","")).strip()
        if not name or name.lower()=="total": continue
        if name.casefold() in seen: duplicate=True; continue
        seen.add(name.casefold())
        n,e,b=pair(r)
        tx=count(r.get("transactions"))
        if b is not None and ((tx is not None and b>tx) or (new is not None and n>new) or (existing is not None and e>existing)):
            n=e=b=None
        s=count(r.get("sessions"))
        channels.append({"channel":name,"new_buyers":n,"existing_buyers":e,"observed_buyers":b,
                         "new_mix_pct":pct(n,b),"existing_mix_pct":pct(e,b),
                         "sessions":s,"orders":tx,
                         "orders_per_session_pct":pct(tx,s),
                         "status":"available" if b is not None and not invalid else "source_required"})
    if invalid or duplicate:
        for r in channels:
            for k in ("new_buyers","existing_buyers","observed_buyers","new_mix_pct","existing_mix_pct"): r[k]=None
            r["status"]="validation_required"
    if duplicate: new=existing=buyers=None
    signup=d.get("signup_users") or {}
    sign,prior,week=(count(signup.get(k)) for k in ("current","yoy","prev"))
    meta=d.get("meta") or {}
    # Historical comparisons require a matching cohort definition and an explicit aggregate source.
    definition="ga4_observed_purchase_since_2024"
    comparisons={}
    for label,periodkeys in (("yoy",("yoy_start","yoy_end")),("wow",("prev_start","prev_end"))):
        x=(d.get("buyer_comparisons") or {}).get(label) or {}
        if (x.get("definition")==definition and meta.get(periodkeys[0]) and meta.get(periodkeys[1])
                and x.get("period_start")==meta.get(periodkeys[0]) and x.get("period_end")==meta.get(periodkeys[1])):
            a,b,c=pair(x)
            comparisons[label]={"new_buyers":a,"existing_buyers":b,"observed_buyers":c,
                                "new_yoy_or_wow_pct":delta(new,a),"existing_yoy_or_wow_pct":delta(existing,b)}
        else: comparisons[label]=None
    available=[r for r in channels if r["status"]=="available"]
    new_top=sorted([r for r in available if r["new_buyers"]>0],key=lambda r:-r["new_buyers"])[:3]
    existing_top=sorted([r for r in available if r["existing_buyers"]>0],key=lambda r:-r["existing_buyers"])[:3]
    return {"date":d.get("end_date"),"period_start":meta.get("cur_start"),"period_end":meta.get("cur_end"),
            "definition":definition,"source":"existing Daily Digest channel snapshot; GA4 historical identity proxy",
            "signups":sign,"signups_yoy":prior,"signups_prior_week":week,
            "signups_yoy_pct":delta(sign,prior),"signups_wow_pct":delta(sign,week),
            "new_buyers":new,"existing_buyers":existing,"observed_buyers":buyers,
            "new_mix_pct":pct(new,buyers),"existing_mix_pct":pct(existing,buyers),
            "orders":orders,"channels":channels,"top_new_channels":new_top,"top_existing_channels":existing_top,
            "buyer_comparisons":comparisons,"buyer_status":"available" if buyers is not None else "validation_required" if invalid or duplicate else "source_required",
            "cohort_revenue":None,"signup_to_purchase_cvr":None,"repeat_purchase_rate":None,
            "note":"Period-level distinct buyer totals use the Total row, not the channel sum; a buyer may appear in several channels. GA4 user_id/pseudo identity and history since 2024 are not ERP lifetime members. New buyer means no earlier observed purchase before the period. Same-day signups and new buyers are not the same cohort. Do not derive signup conversion, repeat rate or segment revenue from these counts."}

def build_model(d,competitor=None,events=None,product_evidence=None,competitor_calendar=None):
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
    valid=[r for r in product_intelligence(d)["categories"] if r["day_yoy_pct"] is not None and r["trend_status"]=="available"]
    sales=d.get("best_sellers_series",{})
    candidates=[]
    products={r["itemId"]:r for r in d.get("best_sellers",[])}
    names={k:r["itemName"] for k,r in products.items()}
    for r in sales.get("items",[]):
        a,b=r.get("ys"),r.get("ys_yoy")
        if not a: continue
        candidates.append({"name":names.get(r["itemId"],r["itemId"]),"sku":r["itemId"],"image_url":products.get(r["itemId"],{}).get("image_url"),"daily_pdp":products.get(r["itemId"],{}).get("views"),"pdp_series":None,"sales_dates":sales.get("x"),"prior_dates":sales.get("yoy_x"),"daily_change_pct":change(a[-1],a[-2]) if len(a)>1 else None,"series":a,"prior_series":b,"scope":r.get("comparison_scope"),"yoy_pct":change(sum(a),sum(b)) if b else None})
    enrich_cards(candidates,weather,events,product_evidence)
    from style_anomaly import rank_styles
    from statistics import median
    evidence=product_evidence or {}
    style_alerts=None
    style_status="complete daily style source required"
    rows=evidence.get("style_daily_rows") or d.get("style_daily_rows")
    if rows and (evidence.get("style_daily_complete") is True if evidence.get("style_daily_rows") else d.get("style_daily_complete") is True):
        try:
            style_alerts=rank_styles(rows,d["end_date"],limit=3)
            style_status="available"
        except (ValueError,TypeError,KeyError) as exc:
            style_status="validation failed: "+str(exc)
    sample=[]
    for r in candidates:
        a=r.get("series") or []
        if len(a)<7 or any(not isinstance(v,(int,float)) or not isfinite(v) or v<0 for v in a):continue
        baseline=median(a[:-1])
        delta=a[-1]-baseline
        if baseline>0 and abs(delta)>=3 and abs(delta/baseline)>=.3:
            sample.append({**r,"baseline":baseline,"delta_units":delta,"direction":"rise" if delta>0 else "fall","alert_scope":"selected bestseller SKU only"})
    sample.sort(key=lambda r:-abs(r["delta_units"]))
    put("Product Early Warning",{"style_alerts":style_alerts,"style_status":style_status,
        "category_declines":sorted(valid,key=lambda r:r["day_yoy_pct"])[:5],
        "category_growth":sorted(valid,key=lambda r:-r["day_yoy_pct"])[:5],
        "sample_sku_sales":candidates,"sample_sku_alerts":sample[:6],
        "style_aggregation_available":style_alerts is not None,
        "sample_threshold":{"absolute_units":3,"relative_pct":30},
        "note":"Sample alerts are not full catalog rankings; missing is not zero."},"available" if style_alerts is not None else "partial")
    if competitor: put("Competitive Intelligence",competitive_intelligence(competitor,competitor_calendar),"partial")
    # Never infer zero discounts or absence of promotions from missing evidence.
    discount_matrix=competitive_discount_matrix(competitor)
    put("Competitive Discount Matrix",discount_matrix,"partial" if discount_matrix["styles"] else "source_required")
    customer=customer_intelligence(d)
    put("Customer Intelligence",customer,"partial" if customer["signups"] is not None or customer["observed_buyers"] is not None else "source_required")
    channels=d.get("channel_snapshot",[])
    if channels: put("Marketing Attribution",{"reported_channel_rows":channels,"causal_incrementality":None},"partial")
    if d.get("glink_paid_snapshot"): put("Budget Optimizer",{"reported_paid_snapshot":d["glink_paid_snapshot"],"marginal_roas":None},"partial")
    put("Integrated Calendar",{"events":events or [],"pdp":pdp,"sample_product_sales":sales,"weather_forecast":weather,"historical_temperature":None,"category_sales":None},"partial")
    put("Business Impact Matrix",{"observed_dimensions":["traffic","conversion","aov","category PDP"],"causal_effects":None},"partial")
    put("Predictive Intelligence",{"forecast":None,"reason":"Backtest and prediction interval validation required"},"validation_required")
    put("Executive AI Strategy",{"actions":["Investigate largest negative revenue bridge contribution","Review category declines across every product category","Verify size/color availability before attributing lost sales"]},"partial")
    return {"date":d.get("end_date"),"sections":model}
