#!/usr/bin/env python3
"""Read-only BigQuery table metadata. Never scan event/order records or export PII."""
import base64
import datetime as dt
import json
import os
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/"reports/decision_health/source_metadata.json"
PROJECT="columbia-ga4"
ORDER_TABLE="columbia-ga4.crm_raw.tb_order_product_search_mart"
ADMIN_TABLE="columbia-ga4.crm_mart.member_funnel_admin_daily"


def extract_fields(schema):
    """Names/types only; no values, IDs, or examples."""
    return {f.name:(f.field_type,extract_fields(f.fields) if f.fields else {}) for f in schema}


def source_metadata(client,table,required):
    """Metadata calls use tables.get, not BigQuery data processing jobs."""
    try:
        t=client.get_table(table)
    except Exception as exc:
        return {"table":table,"accessible":False,"error_type":type(exc).__name__,
                "required_fields":{x:None for x in required},
                "last_modified":None,"row_count":None}
    fields=extract_fields(t.schema)
    edited=t.modified
    return {
      "table":table,"accessible":True,
      "required_fields":{x:x in fields for x in required},
      "last_modified":edited.isoformat() if edited else None,
      "row_count":t.num_rows if isinstance(t.num_rows,int) else None,
      "source":"BigQuery tables.get (metadata only, no billed data scan)"
    }


def build_metadata(client,date):
    ga4="columbia-ga4.analytics_358593394.events_"+date.strftime("%Y%m%d")
    day=source_metadata(client,ga4,["event_date","event_name","items","ecommerce"])
    if day["accessible"]:
        table=client.get_table(ga4)
        fields=extract_fields(table.schema)
        items=fields.get("items",("",{}))[1]
        day["item_field_presence"]={f:f in items for f in
            ("item_id","item_name","item_revenue","quantity","price","discount","coupon","item_category")}
        day["item_discount_supported"]=day["item_field_presence"]["discount"]
    warehouse=source_metadata(client,ORDER_TABLE,[
        "order_no","order_product_no","order_product_datetime","order_use_coupon_price",
        "product_promotion_sale_price","order_product_price","purchase_qty",
        "order_product_use_mileage"])
    admin=source_metadata(client,ADMIN_TABLE,["date","orders","revenue","coupon_used"])
    return {"schema":1,"report_date":date.isoformat(),
      "status":"metadata_inspected","ga4_events":day,
      "order_product_mart":warehouse,"admin_mart":admin,
      "source_scope":"schema/freshness only, no order rows or customer identifiers",
      "bytes_scanned":0,"query_jobs_executed":0}


def main():
    now=dt.datetime.now(dt.timezone(dt.timedelta(hours=9)))
    previous={}
    if OUT.exists():
        try:previous=json.loads(OUT.read_text(encoding="utf-8"))
        except ValueError:pass
    evidence=ROOT/"reports/decision_health/ga4_purchase_diagnostics.json"
    date=None
    try:date=dt.date.fromisoformat(json.loads(evidence.read_text(encoding="utf-8"))["report_date"])
    except (OSError,KeyError,ValueError):date=now.date()-dt.timedelta(days=2)
    encoded=os.getenv("GOOGLE_SA_JSON_B64","").strip()
    if not encoded:
        result={"schema":1,"report_date":date.isoformat(),"status":"credential_missing",
                "bytes_scanned":0,"query_jobs_executed":0}
    else:
        from google.cloud import bigquery
        from google.oauth2 import service_account
        key=json.loads(base64.b64decode(encoded,validate=True))
        client=bigquery.Client(project=key["project_id"],
          credentials=service_account.Credentials.from_service_account_info(key),
          location="asia-northeast3")
        result=build_metadata(client,date)
    OUT.parent.mkdir(parents=True,exist_ok=True)
    if result["status"]=="credential_missing" and previous.get("status")=="metadata_inspected":
        print("[WAREHOUSE] last successful metadata retained")
    else:
        OUT.write_text(json.dumps(result,ensure_ascii=False,indent=2,allow_nan=False)+"\n",encoding="utf-8")
    print("[WAREHOUSE] status",result["status"],"billable query jobs",0)


if __name__=="__main__":
    main()
