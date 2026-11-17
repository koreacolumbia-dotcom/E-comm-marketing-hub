"""Price change analysis for official-store observations; no causal claims."""
from datetime import date,timedelta
from statistics import mean
def price_impacts(observations, daily_metrics, window=7):
    rows=sorted((r for r in observations if r.get("sale_price") is not None and r.get("source_url") and r.get("verified")),key=lambda r:r["date"])
    output=[]
    for prev,cur in zip(rows,rows[1:]):
        a,b=prev["sale_price"],cur["sale_price"]
        if a<=0 or b<0 or a==b:continue
        day=date.fromisoformat(cur["date"])
        contiguous=(day-date.fromisoformat(prev["date"])).days==1
        def collect(metric,offsets):
            values=[daily_metrics.get((day+timedelta(days=i)).isoformat(),{}).get(metric) for i in offsets]
            return mean(values) if all(v is not None for v in values) else None
        effects={}
        for metric in ("sales_units","pdp_views","purchase_cvr","revenue"):
            pre=collect(metric,range(-window,0));post=collect(metric,range(0,window))
            effects[metric]={"before_daily_mean":pre,"after_daily_mean":post,"pct":(post/pre-1)*100 if pre and post is not None else None}
        output.append({"observed_date":cur["date"],"previous_observation":prev["date"],"exact_day_resolution":contiguous,"before_price":a,"after_price":b,"price_change_pct":(b/a-1)*100,"effects":effects,"source_url":cur["source_url"],"interpretation":"observational; control for weather, ads, CRM, inventory and competing promotions"})
    return output
