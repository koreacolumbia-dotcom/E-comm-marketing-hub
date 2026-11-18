"""Single dashboard for 01–18. Only minimum aggregates from already-published inputs.
Private computed journeys and verified cost evidence never enter the public snapshot.
"""
import argparse,json
from pathlib import Path
from datetime import date,datetime,timedelta
from zoneinfo import ZoneInfo
from math import isfinite
from report_model import (executive_summary,root_cause,weather_business,product_intelligence,
    competitive_intelligence,competitive_discount_matrix,customer_intelligence,build_model)
from business_diagnosis import build_diagnosis
from attribution import marketing_attribution
from run_budget import load_budget
from social_listening import build_social
from inventory_intelligence import build_inventory
from advanced_intelligence import integrated_calendar,impact_matrix,predict,strategy

ROOT=Path(__file__).resolve().parents[2]
def read(p):
    try:
        d=json.loads(p.read_text(encoding="utf-8"))
        return d if isinstance(d,dict) else {}
    except (OSError,ValueError):return {}
def pick(r,keys): return {k:r.get(k) for k in keys.split()}
def num(x):return x if isinstance(x,(int,float)) and not isinstance(x,bool) and isfinite(x) else None
def latest(root,day):
    for p in sorted((root/"reports/daily_digest/data/daily").glob("*.json"),reverse=True):
        if p.stem>day:continue
        try: date.fromisoformat(p.stem)
        except ValueError:continue
        d=read(p)
        if d.get("end_date")==p.stem and all(k in d.get("overall",{}) for k in ("current","yoy")):return d
    raise ValueError("No valid dated daily source")
def build(root=ROOT,day=None):
    target=day or (datetime.now(ZoneInfo("Asia/Seoul")).date()-timedelta(days=1)).isoformat()
    d=latest(root,target)
    # Clear optional private enrichment even if it later appears in the daily cache.
    for field in ("multi_touch_attribution","budget_cost_evidence","budget_daily_history"):
        d.pop(field,None)
    rep=root/"reports"
    feed=read(rep/"competitor_intelligence/latest-report-feed.json")
    calendar=read(rep/"competitor_sale_calendar/latest.json")
    content=read(rep/"competitor_content/latest.json")
    results=[]
    def add(n,name,fn):
        try:
            v=fn()
            results.append({"number":n,"title":name,"status":v.pop("status","partial"),**v})
        except Exception as exc:
            results.append({"number":n,"title":name,"status":"validation_required","note":"Section validation failed ("+type(exc).__name__+")"})
    def summary():
        m=executive_summary(d)
        return {"status":"available","cards":m["cards"],"note":m["comparison"]}
    def diagnosis():
        m=build_diagnosis(d)
        return {"status":m["status"],"revenue_change":m["revenue_change"],
          "drivers":[pick(x,"label observed_revenue_contribution priority") for x in m["ranked_contributions"]],
          "actions":[pick(x,"priority check") for x in m["targeted_actions"]],
          "note":m["limitations"]}
    def cause():
        x=executive_summary(d);m=root_cause(d,x["current"],x["prior"])
        return {"delta":m["delta"],"effects":m["effects"],"reconciled":m["reconciled"],
          "channels":[pick(x,"channel current prior_estimate delta_estimate revenue_yoy_pct") for x in m["channels"]],
          "note":m["channel_note"]}
    def weather():
        m=weather_business(d)
        days=(m.get("forecast") or {}).get("days") or []
        return {"forecast":[pick(x,"date min_temp max_temp pop precipitation_probability") for x in days if isinstance(x,dict)][:7],
          "dates":m["dates"],"revenue":m["overall"]["revenue"],"sessions":m["overall"]["sessions"],
          "temperature":m["temperature_current"],"missing":m["missing"],"note":m["interpretation"]}
    def product():
        m=product_intelligence(d)
        return {"dates":m["dates"],"prior_dates":m["prior_dates"],
                "categories":[pick(x,"itemCategory views_d1 day_yoy_pct week_yoy_pct ys ys_yoy has_yoy trend_status") for x in m["categories"]],
                "best_sellers":[pick(x,"itemName itemId qty views revenue") for x in m["sku_sales_sample"][:10]],"note":m["scope"]}
    def early():
        m=build_model(d,competitor=None)["sections"]["Product Early Warning"]["data"]
        return {"style_status":m["style_status"],
          "rising":[pick(x,"name sku daily_change_pct baseline delta_units direction series prior_series") for x in m["sample_sku_alerts"]],
          "declines":[pick(x,"itemCategory views_d1 day_yoy_pct") for x in m["category_declines"]],
          "growth":[pick(x,"itemCategory views_d1 day_yoy_pct") for x in m["category_growth"]],
          "note":m["note"]}
    def comp():
        m=competitive_intelligence(feed,calendar)
        return {"checked_at":m["checked_at"],"calendar_date":calendar.get("as_of"),"coverage":m["coverage"],
          "brands":[{"brand":x["brand"],"status":x["status"],"pages_checked":x["pages_checked"],
                     "active_count":len(x["active_events"]),"observed_count":len(x["observed_banners"])} for x in m["brands"]],
          "events": [pick(x,"brand_name title start_date end_date date_status source_url url") for x in calendar.get("events",[])[:40]],"note":m["note"]}
    def editorial():
        return {"checked_at":content.get("checked_at_kst"),
          "brands":[pick(x,"brand status candidate_count discovered_30d published_30d unknown_publication") for x in content.get("brands",[])],
          "note":"Discovered items are not confirmed publication dates. Open original content monitoring for TPO and details."}
    def matrix():
        m=competitive_discount_matrix(feed)
        return {"checked_at":m["checked_at"],"brand_count":m["brands"],"with_pairs":m["competitor_brands_with_pairs"],
          "styles_count":m["competitor_style_count"],"brands":[pick(x,"brand paired_cards style_count price_only_count sample_discount_median_pct sample_discount_max_pct status") for x in m["brand_rows"]],
          "styles":[pick(x,"brand name category regular_price_median sale_price_median discount_pct_median url") for x in m["styles"][:150]],"note":m["note"]}
    def customers():
        m=customer_intelligence(d)
        return {**pick(m,"signups signups_yoy_pct signups_wow_pct new_buyers existing_buyers observed_buyers new_mix_pct existing_mix_pct buyer_status note"),
          "channels":[pick(x,"channel new_buyers existing_buyers new_mix_pct status") for x in m["channels"]]}
    def attribution():
        m=marketing_attribution(d)
        return {"channels":[{**pick(x,"channel sessions orders revenue revenue_share_pct cvr_pct aov"),
          "revenue_yoy_pct":x["comparisons"]["yoy"]["revenue_growth_pct"],"revenue_delta":x["comparisons"]["yoy"]["revenue_delta"]} for x in m["channels"]],
          "paid":[{**pick(x,"channel sessions orders revenue cvr_pct"),"revenue_yoy_pct":x["comparisons"]["yoy"]["revenue_growth_pct"]} for x in m["paid_channels"]],
          "delta":m["comparisons"]["yoy"]["total_revenue_delta"],"unallocated":m["comparisons"]["yoy"]["unallocated_delta"],
          "multi_touch_status":"private_local_only","note":m["note"]}
    def budget():
        m=load_budget(d,daily_root=rep/"daily_digest/data/daily")
        return {**pick(m,"status cost_status latest_cost_date note"),"media":[pick(x,"media sessions orders revenue cvr_pct spend roas_pct recommendation") for x in m["media"]]}
    def social():
        m=build_social(d["end_date"],root)
        return {"observed":m["observed_community_mentions"],"sources":m["community_source_status"],"topics":m["community_topics"],"daily":m["community_daily"],
          "voc":pick(m["voc"],"reviews average_rating low_rating_count rating_distribution files_present_days topics product_voc"),
          "note":m["note"]}
    def inventory():
        warehouse=(read(rep/"marketing_intelligence_v2/latest.json").get("integrations") or {}).get("warehouse")
        return build_inventory(d,warehouse=warehouse)
    def calendar_section():
        own=read(rep/"owned_portal/data/owned"/("owned_"+d["end_date"]+".json"))
        return integrated_calendar(d,calendar,read(root/"config/marketing_calendar_2026.json"),own)
    def impact_section():
        return impact_matrix(d,calendar_section())
    def prediction_section():
        history=[]
        end=date.fromisoformat(d["end_date"])
        for offset in (28,56,84):
            doc=read(rep/"daily_digest/data/daily"/((end-timedelta(days=offset)).isoformat()+".json"))
            if doc:history.append(doc)
        return predict(d,history)
    def strategy_section():
        return strategy(d,impact_section(),prediction_section(),inventory(),budget(),social())
    for n,name,fn in [(1,"AI BUSINESS DIAGNOSIS",diagnosis),(2,"EXECUTIVE SUMMARY",summary),(3,"ROOT CAUSE ANALYSIS",cause),
    (4,"WEATHER INTELLIGENCE",weather),(5,"PRODUCT INTELLIGENCE",product),(6,"PRODUCT EARLY WARNING",early),
    (7,"COMPETITIVE INTELLIGENCE",comp),(8,"COMPETITIVE CONTENT",editorial),(9,"COMPETITIVE DISCOUNT MATRIX",matrix),
    (10,"CUSTOMER INTELLIGENCE",customers),(11,"MARKETING ATTRIBUTION",attribution),(12,"BUDGET OPTIMIZER",budget),(13,"SOCIAL LISTENING",social),(14,"INVENTORY INTELLIGENCE",inventory),(15,"INTEGRATED CALENDAR",calendar_section),(16,"BUSINESS IMPACT MATRIX",impact_section),(17,"PREDICTIVE INTELLIGENCE",prediction_section),(18,"EXECUTIVE AI STRATEGY",strategy_section)]: add(n,name,fn)
    return {"schema":1,"report_date":d["end_date"],"target_date":target,"source_scope":"existing public aggregate caches only; private journey files not included","sections":results}
def main():
    p=argparse.ArgumentParser();p.add_argument("--out",required=True);a=p.parse_args()
    target=Path(a.out);target.parent.mkdir(parents=True,exist_ok=True)
    m=build();assert [s["number"] for s in m["sections"]]==list(range(1,19))
    assert all(k not in json.dumps(m) for k in ('"path_json"','"user_pseudo_id"','"member_id"','"models"'))
    target.write_text(json.dumps(m,ensure_ascii=False,allow_nan=False,separators=(",",":")),encoding="utf-8")
    print("[DASHBOARD] 18 sections generated; dates and section statuses only:",m["report_date"],[(s["number"],s["status"]) for s in m["sections"]])
if __name__=="__main__":main()
