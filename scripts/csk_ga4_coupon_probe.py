#!/usr/bin/env python3
"""Low-cost GA4 item coupon presence check; never infer monetary discount.

Reads only GA4 item coupon field and daily purchase events over 14 partitions.
Dry-run required, maximum 512 MiB billed, aggregate output only.
"""
import base64
import datetime as dt
import json
import os
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/"reports/decision_health/ga4_coupon_usage.json"
PURCHASE=ROOT/"reports/decision_health/ga4_purchase_diagnostics.json"
METADATA=ROOT/"reports/decision_health/source_metadata.json"
CAP=512*1024*1024


def load(path):
    try:return json.loads(path.read_text(encoding="utf-8"))
    except (OSError,ValueError):return {}


def statement(end):
    first=end-dt.timedelta(days=13)
    split=end-dt.timedelta(days=6)
    t=chr(96)+"columbia-ga4.analytics_358593394.events_*"+chr(96)
    return f"""
    WITH purchase AS (
        SELECT event_date, event_timestamp,
        NULLIF(TRIM(ecommerce.transaction_id),'') AS transaction_id,
        items,
        ROW_NUMBER() OVER(
          PARTITION BY event_date,NULLIF(TRIM(ecommerce.transaction_id),'')
          ORDER BY event_timestamp
        ) AS n
        FROM {t}
        WHERE _TABLE_SUFFIX BETWEEN '{first:%Y%m%d}' AND '{end:%Y%m%d}'
          AND event_name='purchase'
    ),
    line AS (
        SELECT IF(PARSE_DATE('%Y%m%d',p.event_date)>=DATE '{split}',
                  'current','previous') AS period,
          CONCAT(p.event_date,':',p.transaction_id) AS order_key,
          NULLIF(TRIM(i.coupon),'') AS coupon,
          SAFE_CAST(i.quantity AS FLOAT64) AS units,
          SAFE_CAST(i.item_revenue AS FLOAT64) AS item_revenue
        FROM purchase p CROSS JOIN UNNEST(p.items) AS i
        WHERE p.n=1 AND p.transaction_id IS NOT NULL
    )
    SELECT period,
      COUNT(DISTINCT order_key) AS orders_with_items,
      COUNT(DISTINCT IF(coupon IS NOT NULL,order_key,NULL)) AS orders_with_item_coupon,
      COUNT(*) AS observed_item_rows,
      COUNTIF(coupon IS NOT NULL) AS item_coupon_rows,
      SUM(IF(coupon IS NOT NULL,units,0)) AS coupon_units,
      SUM(units) AS units,
      SUM(IF(coupon IS NOT NULL,item_revenue,0)) AS coupon_item_revenue,
      SUM(item_revenue) AS item_revenue,
      COUNTIF(units IS NULL OR item_revenue IS NULL) AS missing_values
    FROM line GROUP BY 1
    """


def infer(rows,reference,end):
    periods={}
    for row in rows:
        period=row.get("period")
        if period not in ("current","previous"):continue
        current={key:int(row[key] or 0) if key not in ("coupon_units","units","coupon_item_revenue","item_revenue")
                 else float(row[key] or 0)
                 for key in ("orders_with_items","orders_with_item_coupon","observed_item_rows",
                             "item_coupon_rows","coupon_units","units","coupon_item_revenue",
                             "item_revenue","missing_values")}
        if current["orders_with_item_coupon"]>current["orders_with_items"]:
            raise ValueError("Coupon distinct orders exceed observed orders")
        current["order_coupon_presence_pct"]=round(
            100*current["orders_with_item_coupon"]/current["orders_with_items"],2
        ) if current["orders_with_items"] else None
        periods[period]=current
    total=(reference.get("totals") or {})
    aligned=all(p in periods and p in total and
                periods[p]["orders_with_items"]==total[p]["orders"]
                for p in ("current","previous"))
    cur,prev=periods.get("current",{}),periods.get("previous",{})
    change=(round(cur["order_coupon_presence_pct"]-prev["order_coupon_presence_pct"],2)
         if cur.get("order_coupon_presence_pct") is not None and
            prev.get("order_coupon_presence_pct") is not None else None)
    return {"schema":1,"report_date":end.isoformat(),
      "status":"measured_coupon_presence" if aligned and all(
          not periods[p]["missing_values"] for p in ("current","previous")
      ) else "partial_quality",
      "periods":periods,"coupon_presence_delta_pp":change,
      "ga4_purchase_totals_aligned":aligned,
      "source":"GA4 items.coupon presence; not coupon discount amount or proven causal impact",
      "quality_caveat":"An item may have coupon code without an applied monetary discount. Counts are not discount savings."
    }


def save(doc):
    OUT.parent.mkdir(parents=True,exist_ok=True)
    prior=load(OUT)
    if doc.get("status","").startswith("blocked") and prior.get("status")=="measured_coupon_presence":
        return
    OUT.write_text(json.dumps(doc,ensure_ascii=False,indent=2,allow_nan=False)+"\n",encoding="utf-8")


def main():
    ref=load(PURCHASE)
    if ref.get("status")!="ga4_measured":return
    end=dt.date.fromisoformat(ref["report_date"])
    prior=load(OUT)
    if prior.get("report_date")==end.isoformat() and prior.get("status")=="measured_coupon_presence" and os.getenv("CSK_COUPON_FORCE")!="1":
        print("[COUPON] Reusing measured result; no query")
        return
    status={"schema":1,"status":"blocked_metadata","report_date":end.isoformat(),
            "cost":{"maximum_bytes_billed":CAP,"dry_run_required":True}}
    metadata=load(METADATA)
    valid=metadata.get("ga4_events") or {}
    if not valid.get("item_field_presence",{}).get("coupon"):
        save(status);return
    key=os.getenv("GOOGLE_SA_JSON_B64","")
    if not key:
        save({**status,"status":"blocked_credentials"});return
    from google.cloud import bigquery
    from google.oauth2 import service_account
    info=json.loads(base64.b64decode(key,validate=True))
    client=bigquery.Client(project=info["project_id"],
      credentials=service_account.Credentials.from_service_account_info(info),
      location="asia-northeast3")
    sql=statement(end)
    dry=client.query(sql,location="asia-northeast3",
            job_config=bigquery.QueryJobConfig(dry_run=True,use_query_cache=False))
    estimate=dry.total_bytes_processed
    if estimate is None or estimate<=0:
        save({**status,"status":"blocked_estimate"});return
    if estimate>CAP:
        save({**status,"status":"blocked_budget","estimated_bytes":int(estimate)});return
    job=client.query(sql,location="asia-northeast3",
      job_config=bigquery.QueryJobConfig(maximum_bytes_billed=CAP,use_query_cache=True))
    data=infer([dict(x) for x in job.result(timeout=110)],ref,end)
    data["cost"]={"estimated_bytes":int(estimate),
       "billed_bytes":int(job.total_bytes_billed or 0),
       "maximum_bytes_billed":CAP,"dry_run_required":True}
    save(data)
    print("[COUPON] status",data["status"],"coupon rate delta pp",
          data["coupon_presence_delta_pp"],"billed",data["cost"]["billed_bytes"])


if __name__=="__main__":
    try:main()
    except Exception as exc:
        print("[COUPON] guarded query failed",type(exc).__name__)
        raise
