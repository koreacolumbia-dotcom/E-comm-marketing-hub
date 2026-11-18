"""Inventory triage using published aggregate demand samples and separate private stock evidence."""
from datetime import date,datetime,timedelta
from math import isfinite
from statistics import mean
from zoneinfo import ZoneInfo
import re

def numeric(x):
    return x if isinstance(x,(int,float)) and not isinstance(x,bool) and isfinite(x) and x>=0 else None
def key(x):
    return " ".join(str(x or "").replace("’","'").replace("`","'").split()).casefold()
def indexed(rows,field):
    out={};dups=set()
    for r in rows or []:
        if not isinstance(r,dict):continue
        k=key(r.get(field))
        if not k:continue
        if k in out:dups.add(k)
        out[k]=r
    return {k:v for k,v in out.items() if k not in dups}
def build_inventory(d,warehouse=None,stock=None,now=None):
    end=date.fromisoformat(d["end_date"])
    scopes=d.get("product_scopes") or {}
    cn=indexed(scopes.get("current_name"),"itemName")
    pn=indexed(scopes.get("previous_name"),"itemName")
    bn=indexed(scopes.get("best_name"),"itemName")
    rows=[]
    for k,r in cn.items():
        views=numeric(r.get("views"))
        if views is None:continue
        prior=numeric(pn.get(k,{}).get("views"))
        qty=numeric(bn.get(k,{}).get("qty"))
        growth=(views/prior-1)*100 if prior else None
        rows.append({"name":r["itemName"],"reported_pdp":views,"prior_sample_pdp":prior,
                     "pdp_change_pct":growth,"top_seller_qty":qty,
                     "demand_priority":"review_rising" if growth is not None and growth>=30 and views-prior>=100 else "review_high_interest",
                     "scope":"name-ranked PDP sample; quantity only if present in independent bestseller sample",
                     "stock_status":"not_verified"})
    rows.sort(key=lambda r:(r["demand_priority"]!="review_rising",-r["reported_pdp"]))
    best=indexed(scopes.get("best_sku") or d.get("best_sellers"),"itemId")
    series=indexed((d.get("best_sellers_series") or {}).get("items"),"itemId")
    labels=(d.get("best_sellers_series") or {}).get("x") or []
    expected=[(end-timedelta(days=i)).strftime("%m/%d") for i in range(6,-1,-1)]
    sku=[]
    for k,r in best.items():
        code=str(r["itemId"]).strip().upper()
        if not re.fullmatch(r"[A-Z0-9_-]{3,40}",code):continue
        qty=numeric(r.get("qty"));views=numeric(r.get("views"))
        a=series.get(k,{}).get("ys") or []
        valid=labels==expected and len(a)==7 and all(numeric(x) is not None for x in a) and qty is not None and a[-1]==qty
        sku.append({"sku":code,"name":r.get("itemName"),"qty":qty,"pdp":views,
                    "sales_7d":sum(a) if valid else None,"daily_sales_avg_7d":mean(a) if valid else None,
                    "sales_series":a if valid else None,"stock_status":"not_verified","coverage_days":None})
    sku.sort(key=lambda r:-(r["sales_7d"] if r["sales_7d"] is not None else r["qty"] or 0))
    warehouse=warehouse or {}
    readiness=warehouse.get("inventory") if warehouse.get("report_date")==end.isoformat() else "not_verified"
    status="source_required" if not rows and not sku else "partial"
    checked=None
    # A current, verified, exact-SKU source is never interpreted as historical availability.
    if stock is not None:
        try:
            if stock.get("source")!="MSSQL dbo.TB_ProductSizeStock" or stock.get("status")!="ready":raise ValueError()
            now=now or datetime.now(ZoneInfo("Asia/Seoul"))
            stamp=datetime.fromisoformat(stock["as_of_kst"])
            if stamp.tzinfo is None or not timedelta(0)<=now-stamp<=timedelta(hours=24):raise ValueError()
            st=indexed(stock["rows"],"sku")
            checked=stamp.isoformat()
            for r in sku:
                e=st.get(key(r["sku"]),{})
                total=numeric(e.get("size_count"));positive=numeric(e.get("positive_size_count"))
                if total is None or positive is None or total<=0 or positive>total or int(total)!=total or int(positive)!=positive:continue
                r["stock_status"]="no_positive_sizes" if positive==0 else "limited_size_availability" if positive/total<=.3 else "some_sizes_available"
                r["available_size_pct"]=round(positive/total*100,1)
                units=numeric(e.get("sellable_units"))
                if units is not None and e.get("quantity_scope")=="verified_online_sellable" and r["daily_sales_avg_7d"]:
                    r["coverage_days"]=round(units/r["daily_sales_avg_7d"],1)
            readiness="current_sample_verified" if any(r["stock_status"]!="not_verified" for r in sku) else "no_matching_rows"
        except (ValueError,KeyError,TypeError):
            readiness="validation_required"
    return {"status":status,"date":end.isoformat(),"stock_source_status":readiness,"stock_as_of":checked,
            "name_candidates":rows[:10],"sku_candidates":sku[:10],
            "observed_stock_count":sum(r["stock_status"]!="not_verified" for r in sku) if checked else None,
            "lost_sales":None,"stockout_rate":None,"note":"Demand proxy from ranked samples, not full catalog. The absence of a name in bestselling rows does not establish zero sales. PDP values are reported sample counts; verify measurement scope before using a conversion ratio. Current stock is not historical stock. No lost-sales estimate or confirmed shortage without verified stock."}
