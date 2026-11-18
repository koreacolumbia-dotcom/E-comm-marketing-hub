"""Observed pre-purchase session paths, rule-based credit, aggregate output only."""
from collections import defaultdict
from math import isfinite

MODELS=("first_touch","last_touch","linear","position_based")
CHANNELS=("Paid Ad","Organic Traffic","Owned Channel","Awareness","Official SNS","etc")

def weights(n, model):
    if n<1: raise ValueError("Empty path")
    if n==1: return [1.0]
    if model=="first_touch": return [1.0]+[0.0]*(n-1)
    if model=="last_touch": return [0.0]*(n-1)+[1.0]
    if model=="linear": return [1.0/n]*n
    if model=="position_based":
        return [0.5,0.5] if n==2 else [0.4]+[0.2/(n-2)]*(n-2)+[0.4]
    raise ValueError("Unknown model")

def summarize(rows, expected_orders, expected_revenue, start, end, lookback=30, min_path_orders=5):
    """Input is aggregate path, order count, revenue; no identifiers in output."""
    if not 1<=lookback<=90: raise ValueError("Invalid lookback")
    if min_path_orders<5: raise ValueError("Minimum path disclosure threshold is five")
    if not isinstance(expected_orders,int) or expected_orders<0 or not isfinite(expected_revenue) or expected_revenue<0:
        raise ValueError("Invalid overall totals")
    credit={m:defaultdict(lambda:{"orders":0.0,"revenue":0.0}) for m in MODELS}
    assisted=defaultdict(int)
    groups=defaultdict(lambda:{"orders":0,"revenue":0.0})
    observed_orders=multi_orders=0
    observed_revenue=0.0
    for row in rows:
        path=tuple(row["path"])
        n,r=row["orders"],float(row["revenue"])
        if not path or any(c not in CHANNELS for c in path) or not isinstance(n,int) or n<=0 or not isfinite(r) or r<0:
            raise ValueError("Invalid aggregate path")
        observed_orders+=n;observed_revenue+=r
        multi_orders+=n if len(path)>1 else 0
        for channel in set(path[:-1])-{path[-1]}: assisted[channel]+=n
        for m in MODELS:
            for c,w in zip(path,weights(len(path),m)):
                credit[m][c]["orders"]+=n*w
                credit[m][c]["revenue"]+=r*w
        group=groups[path];group["orders"]+=n;group["revenue"]+=r
    model_totals={}
    for m in MODELS:
        model_totals[m]={"orders":sum(v["orders"] for v in credit[m].values()),"revenue":sum(v["revenue"] for v in credit[m].values())}
        if abs(model_totals[m]["orders"]-observed_orders)>.00001 or abs(model_totals[m]["revenue"]-observed_revenue)>.01:
            raise ValueError("Credit did not reconcile")
    if observed_orders>expected_orders or observed_revenue>expected_revenue+.01:
        raise ValueError("Observed paths exceed source totals")
    top=[{"path":list(p),**v} for p,v in groups.items() if v["orders"]>=min_path_orders]
    top.sort(key=lambda x:-x["orders"])
    return {"status":"available" if observed_orders else "source_required","period_start":start,"period_end":end,
        "lookback_days":lookback,"identity":"same_ga4_user_pseudo_id_only","source":"ga4_bigquery_observed_session_paths",
        "orders_total":expected_orders,"revenue_total":expected_revenue,"observed_orders":observed_orders,"observed_revenue":observed_revenue,
        "order_coverage_pct":100*observed_orders/expected_orders if expected_orders else None,
        "revenue_coverage_pct":100*observed_revenue/expected_revenue if expected_revenue else None,
        "unattributed_orders":expected_orders-observed_orders,"unattributed_revenue":expected_revenue-observed_revenue,
        "multi_session_orders":multi_orders,"models":{m:[{"channel":c,**credit[m][c]} for c in CHANNELS] for m in MODELS},
        "model_totals":model_totals,"assisted_orders_by_channel":dict(assisted),"top_paths":top[:10],
        "suppressed_path_orders":sum(v["orders"] for v in groups.values() if v["orders"]<min_path_orders),
        "causal_incrementality":None,"note":"Observed same-browser sessions only. No ad impressions, cross-device stitching, data-driven attribution or causal lift. Assisted channels may overlap. Rule-based weights are assumptions."}
