#!/usr/bin/env python3
"""Festival customer repeat-purchase cohorts from observed first order history.

NEW = first observed eligible purchase falls within the event window;
EXISTING = earlier eligible purchase in the available order history;
UNKNOWN = no usable member ID. These are NOT GA4 new/returning users,
and the observed-history first purchase need not be a lifetime first order.
Only aggregates are written to the public report JSON.
"""
from __future__ import annotations

import base64
import datetime as dt
import json
import os
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/"reports/decision_health/winter_festival_customer_yoy_2026.json"
PRODUCT=ROOT/"reports/decision_health/winter_festival_products_yoy_2026.json"
PROJECT="columbia-ga4"
REGION="asia-northeast3"
HISTORY_START="2024-01-01"
CAP=3*1024**3
CANDIDATES=[
  ("crm_mart.tb_order_product_search_mart",("order_no","order_datetime","member_id","order_total_pay"),"mart"),
  ("crm_raw.tb_order_staging",("OrderNo","OrderRegdate","MemberID","OrderTotalPay"),"raw"),
]

def read(path):
    try:return json.loads(path.read_text(encoding="utf-8"))
    except (OSError,ValueError):return {}

def windows(doc):
    p=doc.get("period") or {}
    if doc.get("status")=="ga4_measured" and p.get("2025") and p.get("2026"):
        return p
    return {"2025":["2025-11-21","2025-11-22"],"2026":["2026-11-20","2026-11-21"]}

def build_sql(table,fields,periods,kind):
    ident,date,member,amount=fields
    ly0,ly1=periods["2025"]
    now0,now1=periods["2026"]
    for p in (ly0,ly1,now0,now1):
        dt.date.fromisoformat(p)
    valid="COUNTIF(SAFE_CAST(is_net_sales_line AS INT64)=1)>0" if kind=="mart" else (
        "COUNTIF(SAFE_CAST(OrderStatusNo AS INT64) IN (4,5))>0")
    # one row per order, even though the mart is at order-item granularity.
    # no raw MemberID or order_no ever selected in the published result.
    return f"""
WITH source_orders AS (
 SELECT CAST({ident} AS STRING) oid,
   NULLIF(LOWER(TRIM(CAST({member} AS STRING))),'') mid,
   DATE({date}) AS dt,
   MAX(SAFE_CAST({amount} AS FLOAT64)) AS pay,
   {valid} AS accepted
 FROM {chr(96)}{PROJECT}.{table}{chr(96)}
 WHERE DATE({date}) BETWEEN DATE '{HISTORY_START}' AND DATE '{now1}'
 GROUP BY 1,2,3
),
orders AS (
 SELECT * FROM source_orders WHERE accepted AND pay>=0 AND dt IS NOT NULL
),
member_first AS (
 SELECT mid,MIN(dt) AS first_date FROM orders WHERE mid IS NOT NULL GROUP BY 1
),
period_orders AS (
 SELECT IF(o.dt BETWEEN DATE '{ly0}' AND DATE '{ly1}','2025','2026') year,
   CASE WHEN o.mid IS NULL THEN 'unknown'
     WHEN f.first_date >= IF(o.dt BETWEEN DATE '{ly0}' AND DATE '{ly1}',DATE '{ly0}',DATE '{now0}')
       THEN 'new'
     ELSE 'existing' END AS customer_type,
   o.mid,o.oid,o.pay
 FROM orders o LEFT JOIN member_first f USING(mid)
 WHERE o.dt BETWEEN DATE '{ly0}' AND DATE '{ly1}'
    OR o.dt BETWEEN DATE '{now0}' AND DATE '{now1}'
),
history AS (
 SELECT MIN(dt) min_history,MAX(dt) max_history,
   COUNT(*) historical_orders FROM orders
)
SELECT e.year,e.customer_type,COUNT(DISTINCT e.mid) AS buyers,
 COUNT(*) AS orders,ROUND(SUM(e.pay)) AS revenue,
 h.min_history,h.max_history,h.historical_orders
FROM period_orders e CROSS JOIN history h
GROUP BY 1,2,6,7,8
"""

def make_payload(rows,periods,source,estimate):
    by_year={y:{k:{"buyers":0,"orders":0,"revenue":0} for k in ("new","existing","unknown")} for y in ("2025","2026")}
    history=None
    for obj in rows:
        r=dict(obj);year=str(r.get("year"));kind=str(r.get("customer_type"))
        if year not in by_year or kind not in by_year[year]:
            raise ValueError("Unexpected cohort label")
        vals={key:int(r.get(key) or 0) for key in ("buyers","orders","revenue")}
        if any(v<0 for v in vals.values()):raise ValueError("Invalid negative counts")
        by_year[year][kind]=vals
        if history is None:
            history={"min_order_date":str(r["min_history"]),"max_order_date":str(r["max_history"]),
                     "historical_orders":int(r["historical_orders"])}
    if history is None:
        raise ValueError("No event cohort data")
    min_expected=dt.date.fromisoformat(periods["2025"][0])-dt.timedelta(days=365)
    min_actual=dt.date.fromisoformat(history["min_order_date"])
    # If the source starts close to the first event, do not call repeat buyers established.
    if min_actual>min_expected:
        return {"schema":1,"status":"insufficient_history","period":periods,
                "coverage":{"history_start":history["min_order_date"],"expected_latest_start":str(min_expected)},
                "source":source,"causal":False}
    if dt.date.fromisoformat(history["max_order_date"])<dt.date.fromisoformat(periods["2026"][1]):
        return {"schema":1,"status":"stale_source","period":periods,
                "coverage":history,"source":source,"causal":False}
    for year in ("2025","2026"):
        if sum(by_year[year][k]["orders"] for k in ("new","existing","unknown"))==0:
            return {"schema":1,"status":"missing_period","period":periods,"source":source,"causal":False}
        for kind in ("new","existing","unknown"):
            p=by_year[year][kind]
            p["aov"]=round(p["revenue"]/p["orders"]) if p["orders"] else None
            p["orders_per_buyer"]=round(p["orders"]/p["buyers"],3) if p["buyers"] else None
        total_revenue=sum(by_year[year][k]["revenue"] for k in ("new","existing","unknown"))
        total_orders=sum(by_year[year][k]["orders"] for k in ("new","existing","unknown"))
        by_year[year]["total"]={"revenue":total_revenue,"orders":total_orders,
                                "buyers":sum(by_year[year][k]["buyers"] for k in ("new","existing"))}
        for kind in ("new","existing","unknown"):
            p=by_year[year][kind]
            p["revenue_share_pct"]=round(p["revenue"]/total_revenue*100,2) if total_revenue else 0
            p["orders_share_pct"]=round(p["orders"]/total_orders*100,2) if total_orders else 0
    if any(by_year[y]["unknown"]["orders"] > by_year[y]["total"]["orders"]*0.1 for y in by_year):
        return {"schema":1,"status":"low_member_coverage","source":source,"period":periods,
                "cohort_totals":{y:by_year[y]["total"] for y in by_year},"causal":False}
    deltas={kind:{key:by_year["2026"][kind][key]-by_year["2025"][kind][key]
                  for key in ("buyers","orders","revenue")} for kind in ("new","existing","unknown")}
    return {"schema":1,"status":"measured_observed_first_purchase",
            "period":periods,"report_date":periods["2026"][1],
            "source":source,"cohort_definition":"First eligible purchase in observed historical order source, NOT proven lifetime first order / new registration.",
            "history_coverage":history,
            "years":by_year,"deltas":deltas,
            "total_revenue_delta":by_year["2026"]["total"]["revenue"]-by_year["2025"]["total"]["revenue"],
            "quality":{"order_revenue_partition_reconciles":True,
                       "unknown_segment_visible":True,
                       "ga4_comparison_not_additive":True,
                       "historical_start_required":str(min_expected),
                       "customer_identifiers_published":False},
            "query":{"dry_run_bytes":estimate,"maximum_bytes_billed":CAP},
            "causal":False}

def save(doc):
    OUT.parent.mkdir(parents=True,exist_ok=True)
    OUT.write_text(json.dumps(doc,ensure_ascii=False,indent=2,default=str)+"\n",encoding="utf-8")

def main():
    periods=windows(read(PRODUCT))
    base={"schema":1,"status":"not_ready","period":periods,"causal":False}
    old=read(OUT)
    if old.get("status")=="measured_observed_first_purchase" and old.get("period")==periods:
        print("FESTIVAL CUSTOMER existing verified period retained")
        return
    def preserve(status,info=None):
        if old.get("status")=="measured_observed_first_purchase":
            old["refresh"]={"status":status,"requested_period":periods}
            save(old)
        else:save({**base,"status":status,"details":info})
    secret=os.getenv("GOOGLE_SA_JSON_B64")
    if not secret:
        preserve("blocked_credentials");return
    try:
        from google.cloud import bigquery
        from google.oauth2 import service_account
        cred=service_account.Credentials.from_service_account_info(json.loads(base64.b64decode(secret,validate=True)))
        client=bigquery.Client(project=PROJECT,credentials=cred,location=REGION)
        errors=[]
        for table,fields,kind in CANDIDATES:
            try:
                metadata=client.get_table(f"{PROJECT}.{table}")
                cols={x.name for x in metadata.schema}
                if not set(fields).issubset(cols):continue
                if kind=="mart" and "is_net_sales_line" not in cols:continue
                if kind=="raw" and "OrderStatusNo" not in cols:continue
                sql=build_sql(table,fields,periods,kind)
                dry=client.query(sql,location=REGION,
                    job_config=bigquery.QueryJobConfig(dry_run=True,use_query_cache=False))
                estimate=int(dry.total_bytes_processed or 0)
                if estimate>CAP or estimate<=0:
                    errors.append({"source":table,"status":"blocked_budget","dry_bytes":estimate})
                    continue
                jobs=client.query(sql,location=REGION,job_config=bigquery.QueryJobConfig(
                    maximum_bytes_billed=CAP,use_query_cache=True))
                payload=make_payload(jobs.result(timeout=160),periods,table,estimate)
                if payload["status"]=="measured_observed_first_purchase":
                    save(payload)
                    print("FESTIVAL CUSTOMER measured",table,
                          "2025",payload["years"]["2025"]["total"]["orders"],
                          "2026",payload["years"]["2026"]["total"]["orders"])
                    return
                errors.append({"source":table,"status":payload["status"]})
            except Exception as exc:
                errors.append({"source":table,"status":type(exc).__name__})
        preserve("unavailable_source",{"attempts":errors})
    except Exception as exc:
        preserve("query_error",{"error_type":type(exc).__name__})

if __name__=="__main__":
    main()
