#!/usr/bin/env python3
"""GA4 14-day purchase evidence. Strict combined 1-GiB query budget; no PII exports."""
import base64
import datetime as dt
import json
import math
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/("reports/decision_health/weekly_ga4_aligned.json" if os.getenv("CSK_GA4_ALIGNED_WEEKLY")=="1" else "reports/decision_health/ga4_purchase_diagnostics.json")
TABLE="columbia-ga4.analytics_358593394.events_*"
LOCATION="asia-northeast3"
CAP=1024**3

def num(x):
    return float(x) if type(x) in (int,float) and math.isfinite(x) else None

def read(path):
    try:return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError,ValueError):return {}

def report_date():
    manual=os.getenv("CSK_GA4_END_DATE","")
    if os.getenv("CSK_GA4_ALIGNED_WEEKLY")=="1":
        ops=read(ROOT/"reports/marketing_operations/latest.json")
        w=ops.get("weekly") or {}
        period=w.get("period") or ""
        prior=w.get("comparison_period") or ""
        try:
            a,b=[dt.date.fromisoformat(x.strip()) for x in period.split("~")]
            c,d=[dt.date.fromisoformat(x.strip()) for x in prior.split("~")]
        except (ValueError,TypeError,AttributeError):
            raise ValueError("Weekly operations period unavailable")
        if not (w.get("status")=="ready" and w.get("current",{}).get("status")=="complete"
                and w.get("previous",{}).get("status")=="complete"
                and (b-a).days==6 and (d-c).days==6 and (a-d).days==1
                and ops.get("report_date")==b.isoformat()):
            raise ValueError("Operational comparison window is invalid")
        manual=b.isoformat()
    if not manual:
        paths=sorted((ROOT/"reports/daily_digest/data/daily").glob("20??-??-??.json"))
        if not paths:raise ValueError("No available Daily Digest date")
        manual=paths[-1].stem
    date=dt.date.fromisoformat(manual)
    now=dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).date()
    if not dt.date(2024,1,1)<=date<=now:raise ValueError("Unsafe query date")
    return date

def sqls(date):
    start=date-dt.timedelta(days=13)
    split=date-dt.timedelta(days=6)
    table=chr(96)+TABLE+chr(96)
    predicate=f"_TABLE_SUFFIX BETWEEN '{start:%Y%m%d}' AND '{date:%Y%m%d}' AND event_name='purchase'"
    cte=f"""
    WITH p AS (
      SELECT event_date,event_timestamp,
        NULLIF(TRIM(ecommerce.transaction_id),'') AS txn,
        ecommerce.purchase_revenue AS purchase_revenue,items,
        ROW_NUMBER() OVER (PARTITION BY event_date,
          NULLIF(TRIM(ecommerce.transaction_id),'') ORDER BY event_timestamp) AS rn
      FROM {table} WHERE {predicate}
    )
    """
    period=f"IF(PARSE_DATE('%Y%m%d',event_date)>=DATE '{split}','current','previous')"
    orders=cte+f"""
    SELECT {period} AS period,COUNTIF(txn IS NULL OR rn=1) AS orders,
      COUNTIF(txn IS NULL) AS missing_tx,
      COUNTIF(txn IS NOT NULL AND rn>1) AS duplicates,
      COUNTIF((txn IS NULL OR rn=1) AND purchase_revenue IS NULL) AS missing_revenue,
      SUM(IF(txn IS NULL OR rn=1,SAFE_CAST(purchase_revenue AS FLOAT64),NULL)) AS revenue
    FROM p GROUP BY 1
    """
    products=cte+f"""
    SELECT {period} AS period,event_date AS day,
      COALESCE(NULLIF(TRIM(i.item_id),''),CONCAT('name:',TRIM(i.item_name))) AS item_id,
      ANY_VALUE(i.item_name) AS name,ANY_VALUE(i.item_category) AS category,
      SUM(SAFE_CAST(i.quantity AS FLOAT64)) AS units,
      SUM(SAFE_CAST(i.item_revenue AS FLOAT64)) AS revenue,
      COUNTIF(i.item_revenue IS NULL) AS missing_revenue,
      COUNTIF(i.quantity IS NULL OR i.quantity<0) AS bad_quantity,
      COUNT(*) AS missing_discount,
      CAST(NULL AS FLOAT64) AS reported_discount
    FROM p CROSS JOIN UNNEST(p.items) AS i
    WHERE (txn IS NULL OR rn=1) AND (i.item_id IS NOT NULL OR i.item_name IS NOT NULL)
    GROUP BY 1,2,3
    """
    return [orders,products]

def split_aov(current,previous):
    if any(t["orders"]<=0 or t["units"]<=0 or t["item_revenue"]<=0 for t in (current,previous)):return None
    q1=current["units"]/current["orders"];q0=previous["units"]/previous["orders"]
    p1=current["item_revenue"]/current["units"];p0=previous["item_revenue"]/previous["units"]
    q_effect=(q1-q0)*(p1+p0)/2
    p_effect=(p1-p0)*(q1+q0)/2
    delta=current["item_revenue"]/current["orders"]-previous["item_revenue"]/previous["orders"]
    if abs(q_effect+p_effect-delta)>0.02:raise AssertionError("Item AOV bridge unreconciled")
    return dict(items_per_order_previous=round(q0,3),items_per_order_current=round(q1,3),
                realized_item_price_previous=round(p0,2),realized_item_price_current=round(p1,2),
                item_aov_previous=round(previous["item_revenue"]/previous["orders"],2),
                item_aov_current=round(current["item_revenue"]/current["orders"],2),
                item_aov_delta=round(delta,2),
                items_per_order_effect=round(q_effect,2),
                realized_item_revenue_effect=round(p_effect,2),
                interpretation="Arithmetic only: realized item revenue includes discount and SKU mix.")

def split_categories(categories,total):
    cur,pre=total["current"],total["previous"]
    if not cur["units"] or not pre["units"]:return None
    mix=within=0.
    rows=[]
    for k,v in categories.items():
        c,p=v["current"],v["previous"]
        s1=c["units"]/cur["units"];s0=p["units"]/pre["units"]
        price1=c["revenue"]/c["units"] if c["units"]>0 else None
        price0=p["revenue"]/p["units"] if p["units"]>0 else None
        if price0 is None and price1 is None:continue
        if price0 is None:price0=price1
        if price1 is None:price1=price0
        m=(s1-s0)*(price0+price1)/2
        w=(price1-price0)*(s1+s0)/2
        mix+=m;within+=w
        rows.append(dict(category=k,share_previous_pct=round(s0*100,2),
            share_current_pct=round(s1*100,2),mix_effect_per_item=round(m,2),
            within_category_effect_per_item=round(w,2)))
    delta=cur["item_revenue"]/cur["units"]-pre["item_revenue"]/pre["units"]
    if abs(mix+within-delta)>0.02:raise AssertionError("Category decomposition unreconciled")
    return dict(category_mix_effect_per_item=round(mix,2),
        within_category_effect_per_item=round(within,2),
        realized_item_price_delta=round(delta,2),
        rows=sorted(rows,key=lambda x:-abs(x["mix_effect_per_item"]))[:20],
        caveat="New/retired categories attributed to mix; within-category price contains discount and mix.")

def revenue_bridge(cur,pre):
    """Exact GA4 purchase revenue delta = item units + realized unit + item/purchase gap.

    Item-to-order discrepancy is not classified as coupon without a supported
    transaction-level coupon, refund and price reconciliation source.
    """
    if any(x is None or x<=0 for x in (cur["units"],pre["units"],
         cur["item_revenue"],pre["item_revenue"],
         cur["purchase_revenue"],pre["purchase_revenue"])):
        return None
    p1=cur["item_revenue"]/cur["units"];p0=pre["item_revenue"]/pre["units"]
    qty_effect=(cur["units"]-pre["units"])*(p1+p0)/2
    price_effect=(p1-p0)*(cur["units"]+pre["units"])/2
    gap_now=cur["item_revenue"]-cur["purchase_revenue"]
    gap_old=pre["item_revenue"]-pre["purchase_revenue"]
    gap_effect=-(gap_now-gap_old)
    delta=cur["purchase_revenue"]-pre["purchase_revenue"]
    if abs(qty_effect+price_effect+gap_effect-delta)>0.02:
        raise AssertionError("Purchase revenue bridge did not reconcile")
    return dict(
        qty_effect_krw=round(qty_effect,2),
        realized_unit_revenue_effect_krw=round(price_effect,2),
        item_to_purchase_gap_effect_krw=round(gap_effect,2),
        prior_item_to_purchase_gap_krw=round(gap_old,2),
        current_item_to_purchase_gap_krw=round(gap_now,2),
        purchase_revenue_delta_krw=round(delta,2),
        prior_total_units=int(pre["units"]),current_total_units=int(cur["units"]),
        note="Accounting identity, not proof of coupon/discount/refund causality."
    )


def analyze(orders,items,end,admin,focus):
    start=end-dt.timedelta(days=13)
    dates=[(start+dt.timedelta(days=i)).strftime("%Y%m%d") for i in range(14)]
    total={p:dict(orders=0,missing_tx=0,duplicates=0,missing_revenue=0,
                  purchase_revenue=None,units=0.,item_revenue=0.,missing_item_revenue=0,
                  bad_quantity=0,discount_null=0,reported_discount=0.)
           for p in ("previous","current")}
    for row in orders:
        period=row.get("period")
        if period not in total:continue
        t=total[period]
        for f in ("orders","missing_tx","duplicates","missing_revenue"):t[f]=int(row.get(f) or 0)
        t["purchase_revenue"]=num(row.get("revenue"))
    categories={}
    by_name={}
    for row in items:
        period=row.get("period");day=str(row.get("day"))
        if period not in total or day not in dates:continue
        t=total[period]
        units=num(row.get("units"));revenue=num(row.get("revenue"))
        t["missing_item_revenue"]+=int(row.get("missing_revenue") or 0)
        t["bad_quantity"]+=int(row.get("bad_quantity") or 0)
        t["discount_null"]+=int(row.get("missing_discount") or 0)
        if units is not None and units>=0:t["units"]+=units
        if revenue is not None:t["item_revenue"]+=revenue
        disc=num(row.get("reported_discount"))
        if disc is not None and disc>=0:t["reported_discount"]+=disc
        category=str(row.get("category") or "미분류")[:70]
        c=categories.setdefault(category,{p:dict(units=0.,revenue=0.) for p in total})[period]
        if units is not None and units>=0:c["units"]+=units
        if revenue is not None:c["revenue"]+=revenue
        name=str(row.get("name") or "").strip()[:140]
        if not name:continue
        key=re.sub(r"\s+"," ",name.replace("’","'")).casefold()
        v=by_name.setdefault(key,dict(name=name,codes=set(),
                      dates={d:0. for d in dates},
                      parts={p:dict(units=0.,revenue=0.) for p in total}))
        v["codes"].add(str(row.get("item_id") or "")[:55])
        if units is not None and units>=0:
            v["dates"][day]+=units;v["parts"][period]["units"]+=units
        if revenue is not None:v["parts"][period]["revenue"]+=revenue
    clean=all(t["orders"]>0 and t["units"]>0 and t["item_revenue"]>0
              and not t["missing_tx"] and not t["bad_quantity"]
              and not t["missing_item_revenue"] for t in total.values())
    names=[str(c.get("name") or "").replace("’","'").casefold()
           for c in (focus.get("product_focus") or {}).get("cards") or []]
    ranked=sorted(by_name.values(),key=lambda v:(
        v["name"].replace("’","'").casefold() not in names,
        -v["parts"]["current"]["revenue"]))
    products=[]
    for x in ranked[:100]:
        a=x["parts"]["current"];b=x["parts"]["previous"]
        products.append(dict(name=x["name"],codes=sorted(x["codes"])[:10],
           current_units=int(a["units"]),previous_units=int(b["units"]),
           current_revenue=round(a["revenue"],2),previous_revenue=round(b["revenue"],2),
           daily_units=[int(x["dates"][d]) for d in dates]))
    base=(admin.get("weekly") or {})
    comparison={}
    for p,label in (("current","current"),("previous","previous")):
        a=(base.get(label) or {}).get("total") or {}
        ga=total[p]
        gap=lambda v,ref:round(abs(v/ref-1)*100,2) if v is not None and ref and ref>0 else None
        comparison[p]=dict(order_gap_pct=gap(ga["orders"],num(a.get("orders"))),
            revenue_gap_pct=gap(ga["purchase_revenue"],num(a.get("revenue"))))
    reconciled=admin.get("report_date")==end.isoformat() and all(
        comparison[p]["order_gap_pct"] is not None and comparison[p]["order_gap_pct"]<=3
        and comparison[p]["revenue_gap_pct"] is not None and comparison[p]["revenue_gap_pct"]<=5
        for p in comparison)
    return dict(schema=1,status="ga4_measured" if clean else "partial_quality",
        report_date=end.isoformat(),
        period=dict(previous_start=start.isoformat(),
                    previous_end=(end-dt.timedelta(days=7)).isoformat(),
                    current_start=(end-dt.timedelta(days=6)).isoformat(),
                    current_end=end.isoformat()),
        item_aov_bridge=split_aov(total["current"],total["previous"]) if clean else None,
        sales_revenue_bridge=revenue_bridge(total["current"],total["previous"]) if clean else None,
        category_bridge=split_categories(categories,total) if clean else None,
        product_sales=products,totals=total,
        admin_reconciliation=dict(comparable=reconciled,periods=comparison,
             caveat="GA4 event and administrative net revenue/order definitions can differ."),
        quality=dict(missing_transaction_ids=sum(v["missing_tx"] for v in total.values()),
                     missing_item_revenue_rows=sum(v["missing_item_revenue"] for v in total.values()),
                     null_discount_rows=sum(v["discount_null"] for v in total.values())),
        source_scope="GA4 daily purchase items; aggregated by item name, deduplicated on valid transaction ID. No customer data or causal conclusions.")

def save(doc):
    OUT.parent.mkdir(parents=True,exist_ok=True)
    if doc.get("status","").startswith("blocked") or doc.get("status")=="query_error":
        prior=read(OUT)
        if prior.get("status")=="ga4_measured":return
    OUT.write_text(json.dumps(doc,ensure_ascii=False,indent=2,allow_nan=False)+"\n",encoding="utf-8")

def run():
    end=report_date()
    previous=read(OUT)
    if previous.get("report_date")==end.isoformat() and previous.get("status") in ("ga4_measured","partial_quality") and os.getenv("CSK_GA4_FORCE","")!="1":
        if previous.get("status")=="ga4_measured" and not previous.get("sales_revenue_bridge"):
            totals=previous.get("totals") or {}
            if totals.get("current") and totals.get("previous"):
                previous["sales_revenue_bridge"]=revenue_bridge(totals["current"],totals["previous"])
                previous.setdefault("admin_reconciliation",{})["caveat"]=(
                    "Agreement to existing Daily KPI (GA4 source). Independent MSSQL/ERP revenue still not verified.")
                save(previous)
                print("[GA4] Rebuilt purchase revenue factors from cached aggregates, no BigQuery query")
        print("[GA4] Existing dated evidence reused; zero query cost",end)
        return
    status=dict(schema=1,report_date=end.isoformat(),status="blocked_credentials",
                cost=dict(maximum_bytes_billed_total=CAP,dry_run_required=True))
    token=os.getenv("GOOGLE_SA_JSON_B64","").strip()
    if not token:save(status);return
    from google.cloud import bigquery
    from google.oauth2 import service_account
    info=json.loads(base64.b64decode(token,validate=True))
    client=bigquery.Client(project=info["project_id"],
                           credentials=service_account.Credentials.from_service_account_info(info),
                           location=LOCATION)
    queries=sqls(end);estimates=[]
    for sql in queries:
        job=client.query(sql,location=LOCATION,
                         job_config=bigquery.QueryJobConfig(dry_run=True,use_query_cache=False))
        size=job.total_bytes_processed
        if size is None or size<=0:save({**status,"status":"blocked_estimate"});return
        estimates.append(int(size))
    if sum(estimates)>CAP:
        save({**status,"status":"blocked_budget","estimated_bytes":sum(estimates)});return
    remaining=CAP;billed=0;rows=[]
    for sql in queries:
        if remaining<=0:raise RuntimeError("Combined billing ceiling exhausted")
        job=client.query(sql,location=LOCATION,
                         job_config=bigquery.QueryJobConfig(maximum_bytes_billed=remaining,
                                                            use_query_cache=True))
        rows.append([dict(row) for row in job.result(timeout=130)])
        charged=int(job.total_bytes_billed or 0)
        billed+=charged;remaining-=charged
        if remaining<0:raise AssertionError("Unexpected spend")
    data=analyze(rows[0],rows[1],end,
                 read(ROOT/"reports/marketing_operations/latest.json"),
                 read(ROOT/"reports/daily_intelligence/latest.json"))
    data["cost"]=dict(dry_run_bytes=sum(estimates),billed_bytes=billed,
                      maximum_bytes_billed_total=CAP,dry_run_required=True)
    save(data)
    print("[GA4] status",data["status"],"date",end,"dry_run_bytes",sum(estimates),
          "billed_bytes",billed,"admin_reconciled",data["admin_reconciliation"]["comparable"])

if __name__=="__main__":
    try:run()
    except Exception as exc:
        # Log BigQuery's first error line only; never print credentials or raw rows.
        detail=str(exc).splitlines()[0][:240]
        detail=re.sub(r"(?i)(authkey|token|password|secret)\s*=\s*[^\s&]+",r"\1=[REDACTED]",detail)
        print("[GA4] stopped",type(exc).__name__,detail)
        try:save(dict(schema=1,status="query_error",report_date=report_date().isoformat(),
                      error_type=type(exc).__name__,maximum_bytes_billed_total=CAP))
        except Exception:pass
        sys.exit(1)
