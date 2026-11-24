#!/usr/bin/env python3
"""Bounded GA4 purchase-item year-over-year Winter Festival comparison.

No raw users, transaction IDs, emails, ad costs or customer information persisted.
Two exact event windows, 2025 and 2026 (same event elapsed day/weekday).
The output is item-level GA4 revenue: NOT official order-level revenue or causal lift.
"""
from __future__ import annotations

import base64
import datetime as dt
import json
import math
import os
import re
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "reports/decision_health/winter_festival_products_yoy_2026.json"
OPS = ROOT / "reports/marketing_operations/latest.json"
TABLE = "columbia-ga4.analytics_358593394.events_*"
REGION = "asia-northeast3"
PER_YEAR_CAP = 3 * 1024**3
TOTAL_DRY_CAP = 6 * 1024**3
DEFAULT = {"2025": ["2025-11-21", "2025-11-22"],
           "2026": ["2026-11-20", "2026-11-21"]}
SAFE_LABEL = re.compile(r"@|https?://|(?:\+?\d[\d\s().-]{9,}\d)", re.I)

def read(path: Path) -> dict:
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
        return doc if isinstance(doc, dict) else {}
    except (OSError, UnicodeError, ValueError):
        return {}

def period_from_ops(ops: dict) -> dict:
    yoy = ((ops.get("weekly") or {}).get("yoy") or {})
    days = yoy.get("daily_comparison") or []
    is_d3 = (
        str(ops.get("report_date") or "") >= "2026-11-22"
        and yoy.get("status") == "ready"
        and any(isinstance(d, dict) and d.get("date") == "2026-11-22"
                and d.get("ly_date") == "2025-11-23"
                and isinstance(d.get("current"), dict)
                and isinstance(d.get("last_year"), dict) for d in days)
    )
    return {"2025": ["2025-11-21", "2025-11-23"],
            "2026": ["2026-11-20", "2026-11-22"]} if is_d3 else DEFAULT

def safe_name(value, max_length=95) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).strip()
    text = re.sub(r"\s+", " ", text)
    if not text or len(text) > max_length or SAFE_LABEL.search(text):
        return "(restricted)"
    return text

def style_key(value: str) -> str:
    return safe_name(value).replace("’", "'").replace("‘", "'").replace("\x60", "'").casefold()

def sql_for(start: str, end: str) -> str:
    for value in (start, end):
        dt.date.fromisoformat(value)
        if not re.fullmatch(r"20\d{2}-\d{2}-\d{2}", value):
            raise ValueError("Date not a verified ISO date")
    return f"""
    WITH purchases AS (
      SELECT event_date,event_timestamp,
        NULLIF(TRIM(ecommerce.transaction_id),'') AS txn,
        SAFE_CAST(ecommerce.purchase_revenue AS FLOAT64) AS order_revenue,
        (SELECT SUM(SAFE_CAST(q.quantity AS FLOAT64)) FROM UNNEST(items) q) AS order_units,
        items,
        ROW_NUMBER() OVER (
          PARTITION BY event_date,NULLIF(TRIM(ecommerce.transaction_id),'')
          ORDER BY event_timestamp
        ) AS rn
      FROM {chr(96)}{TABLE}{chr(96)}
      WHERE _TABLE_SUFFIX BETWEEN '{start.replace("-","")}' AND '{end.replace("-","")}'
        AND event_name='purchase'
    ), deduped AS (
      SELECT * FROM purchases WHERE txn IS NULL OR rn=1
    )
    SELECT 'order' AS kind,CAST(NULL AS STRING) AS sku,
      CAST(NULL AS STRING) AS name,CAST(NULL AS STRING) AS category,
      SUM(order_units) AS units,
      CAST(NULL AS FLOAT64) AS item_revenue, 0 AS missing_item_revenue,
      COUNT(*) AS order_count,SUM(order_revenue) AS purchase_revenue
    FROM deduped
    UNION ALL
    SELECT 'item' AS kind,
      COALESCE(NULLIF(TRIM(i.item_id),''),CONCAT('name:',TRIM(i.item_name))) AS sku,
      TRIM(i.item_name) AS name,TRIM(i.item_category) AS category,
      SUM(SAFE_CAST(i.quantity AS FLOAT64)) AS units,
      SUM(SAFE_CAST(i.item_revenue AS FLOAT64)) AS item_revenue,
      COUNTIF(i.item_revenue IS NULL) AS missing_item_revenue,
      0 AS order_count,CAST(NULL AS FLOAT64) AS purchase_revenue
    FROM deduped CROSS JOIN UNNEST(items) i
    WHERE NULLIF(TRIM(i.item_name),'') IS NOT NULL
      AND i.quantity IS NOT NULL AND i.quantity>=0
    GROUP BY 1,2,3,4
    """

def parse_year(records) -> dict:
    product = {}
    orders = 0
    purchase_revenue = 0.
    total_units = 0.
    missing_item_revenue = 0
    for entry in records:
        row = dict(entry)
        if row.get("kind") == "order":
            orders += int(row.get("order_count") or 0)
            purchase_revenue += float(row.get("purchase_revenue") or 0)
            continue
        if row.get("kind") != "item":
            continue
        name = safe_name(row.get("name"))
        if name == "(restricted)":
            continue
        units = float(row.get("units") or 0)
        revenue = float(row.get("item_revenue") or 0)
        if not all(math.isfinite(v) and v >= 0 for v in (units, revenue)):
            continue
        key = style_key(name)
        current = product.setdefault(key, {
            "name":name,"category":safe_name(row.get("category"),50),
            "skus":set(),"units":0.,"item_revenue":0.})
        current["units"] += units
        current["item_revenue"] += revenue
        sku = safe_name(row.get("sku"),55)
        if sku != "(restricted)" and len(current["skus"]) < 15:
            current["skus"].add(sku)
        total_units += units
        missing_item_revenue += int(row.get("missing_item_revenue") or 0)
    for value in product.values():
        value["item_revenue"] = round(value["item_revenue"])
        value["units"] = round(value["units"],2)
        value["skus"] = sorted(value["skus"])
    return {"orders":orders,"purchase_revenue":round(purchase_revenue),
            "item_revenue":round(sum(x["item_revenue"] for x in product.values())),
            "units":round(total_units,2),"missing_item_revenue":missing_item_revenue,
            "products":product}

def compare(period: dict, years: dict) -> dict:
    previous,current = years["2025"],years["2026"]
    names = sorted(set(previous["products"])|set(current["products"]))
    rows=[]
    for key in names:
        a=previous["products"].get(key) or {}
        b=current["products"].get(key) or {}
        n=b or a
        rev0=int(a.get("item_revenue") or 0)
        rev1=int(b.get("item_revenue") or 0)
        qty0=float(a.get("units") or 0)
        qty1=float(b.get("units") or 0)
        rows.append({"name":n.get("name"),"category":n.get("category"),
                     "skus":list(dict.fromkeys((b.get("skus") or [])+(a.get("skus") or [])))[:12],
                     "previous":{"revenue":rev0,"units":qty0},
                     "current":{"revenue":rev1,"units":qty1},
                     "delta_revenue":rev1-rev0,"delta_units":round(qty1-qty0,2)})
    rows.sort(key=lambda x:(-abs(x["delta_revenue"]),x["name"]))
    for row in rows:
        sold_25 = row["previous"]["units"] > 0
        sold_26 = row["current"]["units"] > 0
        row["presence"] = ("both" if sold_25 and sold_26 else
                           "only_2025" if sold_25 else
                           "only_2026" if sold_26 else "no_units")
        # Compare normalized item_name, NOT the SKU. Different colors grouped.
        # Only in 2026 does not imply a true product launch.
        for year in ("previous","current"):
            units = row[year]["units"]
            row[year]["realized_item_revenue"] = (
                round(row[year]["revenue"] / units, 2) if units > 0 else None)
    presence_counts = {
        key: sum(row["presence"] == key for row in rows)
        for key in ("both","only_2025","only_2026","no_units")
    }
    totals={}
    for year in ("2025","2026"):
        value=years[year]
        totals[year]={key:value[key] for key in
            ("orders","purchase_revenue","item_revenue","units","missing_item_revenue")}
        totals[year]["items_per_order"]=(round(value["units"]/value["orders"],3)
                                          if value["orders"]>0 else None)
        totals[year]["revenue_per_item"]=(round(value["item_revenue"]/value["units"],2)
                                          if value["units"]>0 else None)
    item_diff=totals["2026"]["item_revenue"]-totals["2025"]["item_revenue"]
    # Product difference MUST reconcile with item sums (not checkout revenue).
    if sum(x["delta_revenue"] for x in rows)!=item_diff:
        raise ValueError("Item-level contribution bridge does not reconcile")
    quality = {"missing_item_revenue_2025":totals["2025"]["missing_item_revenue"],
               "missing_item_revenue_2026":totals["2026"]["missing_item_revenue"],
               "item_revenue_diff_reconciled":True,
               "item_vs_purchase_revenue_gap":{
                 year:totals[year]["item_revenue"]-totals[year]["purchase_revenue"]
                 for year in ("2025","2026")},
               "source":"GA4 purchase event items, duplicated transaction_id deduplicated within day",
               "no_raw_ids_or_users":True}
    def top_names(items, sort_key):
        return [row["name"] for row in sorted(items,key=sort_key)[:5]]
    common=[row for row in rows if row["presence"]=="both"]
    rankings={
      "top_2025":top_names(rows,lambda x:(-x["previous"]["revenue"],x["name"])),
      "top_2026":top_names(rows,lambda x:(-x["current"]["revenue"],x["name"])),
      "impact_up":top_names([x for x in rows if x["delta_revenue"]>0],
                             lambda x:(-x["delta_revenue"],x["name"])),
      "impact_down":top_names([x for x in rows if x["delta_revenue"]<0],
                               lambda x:(x["delta_revenue"],x["name"])),
      "common_growth":top_names([x for x in common if x["delta_revenue"]>0],lambda x:(-x["delta_revenue"],x["name"])),
      "common_decline":top_names([x for x in common if x["delta_revenue"]<0],lambda x:(x["delta_revenue"],x["name"])),
    }
    quality["full_product_ranking_verified"] = True
    quality["style_match_definition"] = "Unicode NFKC + normalized apostrophes, whitespace and casefold on item_name; SKU/color variants grouped by normalized name"
    return {"schema":2,"status":"ga4_measured","report_date":period["2026"][1],
            "period":period,"comparison":"same_event_elapsed_day_same_weekday",
            "years":totals,"products":rows,
            "product_count":len(rows),"presence_counts":presence_counts,
            "rankings":rankings,
            "item_revenue_delta":item_diff,
            "quality":quality,"causal":False,
            "caveat":"Item-level GA4 revenue can differ from official purchase revenue (tax, coupons, shipping, attribution, gaps). Never sum product revenue delta with channel/funnel bridges. New products are not necessarily promotional incremental lift."}

def save(doc: dict):
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(doc,ensure_ascii=False,indent=2,allow_nan=False)+"\n",
                   encoding="utf-8")


def validated_festival_daily_window():
    """Use only contiguous, matched GA4 daily source periods through yesterday KST."""
    origin=dt.date(2026,11,20)
    end=dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).date()-dt.timedelta(days=1)
    last=None
    for i in range(31):
        day=origin+dt.timedelta(days=i)
        if day>end:break
        file=ROOT/"reports/daily_digest/data/daily"/(day.isoformat()+".json")
        try:doc=json.loads(file.read_text(encoding="utf-8"))
        except (OSError,ValueError):break
        prev_day=day-dt.timedelta(days=364)
        anchor=doc.get("yoy") or {}
        observed=doc.get("overall") or {}
        cur=observed.get("current") or {}
        prev=observed.get("yoy") or {}
        if (doc.get("end_date")!=day.isoformat()
            or anchor.get("start")!=prev_day.isoformat()
            or anchor.get("end")!=prev_day.isoformat()
            or cur.get("source")!="ga4_bigquery_session_key_v1"
            or prev.get("source")!="ga4_bigquery_session_key_v1"
            or not isinstance(cur.get("sessions"),(int,float))
            or not isinstance(prev.get("sessions"),(int,float))
            or cur["sessions"]<=0 or prev["sessions"]<=0):break
        last=day
    if last is None or last < origin+dt.timedelta(days=1):return None
    return {"2025":["2025-11-21",(last-dt.timedelta(days=364)).isoformat()],
            "2026":["2026-11-20",last.isoformat()]}

def main():
    ops = read(OPS)
    period = validated_festival_daily_window() or period_from_ops(ops)
    old = read(OUT)
    if old.get("schema")==2 and old.get("status")=="ga4_measured" and old.get("period")==period:
        print("[FESTIVAL PRODUCT] same measured window, no new BQ job")
        return
    base={"schema":2,"period":period,"report_date":period["2026"][1],
          "status":"not_ready","causal":False}
    def preserve_or_write(status, details=None):
        if old.get("status")=="ga4_measured":
            old["refresh"]={"status":status,
                            "requested_period":period,
                            "note":"Retained last verified event window"}
            save(old)
        else:
            save({**base,"status":status,"details":details})
    if not os.getenv("GOOGLE_SA_JSON_B64"):
        preserve_or_write("blocked_credentials")
        return
    try:
        from google.cloud import bigquery
        from google.oauth2 import service_account
        info=json.loads(base64.b64decode(os.environ["GOOGLE_SA_JSON_B64"],validate=True))
        credentials=service_account.Credentials.from_service_account_info(info)
        client=bigquery.Client(project=info["project_id"],credentials=credentials,
                               location=REGION)
        queries,estimates={},{}
        for year in ("2025","2026"):
            queries[year]=sql_for(*period[year])
            dry=client.query(queries[year],location=REGION,
                 job_config=bigquery.QueryJobConfig(dry_run=True,use_query_cache=False))
            estimates[year]=int(dry.total_bytes_processed or 0)
        if (sum(estimates.values())>TOTAL_DRY_CAP or
                any(v<=0 or v>PER_YEAR_CAP for v in estimates.values())):
            preserve_or_write("blocked_budget",{"estimates":estimates})
            return
        data={}
        for year in ("2025","2026"):
            job=client.query(queries[year],location=REGION,
               job_config=bigquery.QueryJobConfig(maximum_bytes_billed=PER_YEAR_CAP,
                                                   use_query_cache=True))
            data[year]=parse_year(job.result(timeout=180))
        if any(data[y]["orders"]<=0 or data[y]["units"]<=0 for y in data):
            preserve_or_write("insufficient_data")
            return
        result=compare(period,data)
        result["cost"]={"dry_run_bytes":estimates,
                        "per_year_cap":PER_YEAR_CAP,"total_dry_cap":TOTAL_DRY_CAP}
        save(result)
        print("[FESTIVAL PRODUCT] measured",period,
              "items",result["product_count"],"units",
              data["2025"]["units"],data["2026"]["units"])
    except Exception as exc:
        preserve_or_write("query_error",{"error_type":type(exc).__name__})
        print("[FESTIVAL PRODUCT] query_error",type(exc).__name__)

if __name__=="__main__":
    main()
