#!/usr/bin/env python3
"""Exact 2025/2026 first-purchase buyer comparison via GA4 official Data API.

Same Analytics Data API source for both years; no identities, user IDs or
individual transactions written. Never substitute new/returning VISITORS.
Data API firstTimePurchasers: users whose first purchase occurred in period.
Derived existing purchasers = totalPurchasers - firstTimePurchasers.
Revenue and ecommercePurchases are WHOLE-period totals, not cohort-specific.
"""
from __future__ import annotations

import base64
import datetime as dt
import json
import os
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/"reports/decision_health/winter_festival_ga4_buyer_yoy_2026.json"
SOURCE=ROOT/"reports/decision_health/winter_festival_products_yoy_2026.json"
PROPERTY="358593394"
METRICS=("totalPurchasers","firstTimePurchasers","ecommercePurchases","purchaseRevenue")

def read(path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError,ValueError):return {}

def periods_from_product(d):
    p=d.get("period",{})
    if d.get("status")=="ga4_measured" and set(("2025","2026")).issubset(p):
        return {"2025":p["2025"],"2026":p["2026"]}
    return {"2025":["2025-11-21","2025-11-22"],
            "2026":["2026-11-20","2026-11-21"]}

def make_report(api_responses,periods,source=None):
    years={}
    for year in ("2025","2026"):
        obj=api_responses[year]
        headers=[item.get("name") for item in obj.get("metricHeaders",[])]
        rows=obj.get("rows",[])
        if headers!=list(METRICS) or len(rows)!=1:
            raise ValueError("GA4 metric headers or response row count invalid")
        m=[x.get("value","") for x in rows[0].get("metricValues",[])]
        if len(m)!=4:raise ValueError("GA4 response metric length invalid")
        t,f,orders=map(int,m[:3])
        rev=float(m[3])
        if t<0 or f<0 or orders<0 or rev<0 or f>t or orders<t:
            raise ValueError("GA4 purchaser metrics inconsistent")
        years[year]={"status":"ga4_measured",
                     "total_purchasers":t,"first_time_purchasers":f,
                     "repeat_purchasers_derived":t-f,
                     "first_time_share_pct":round(100*f/t,2) if t else None,
                     "repeat_share_pct":round(100*(t-f)/t,2) if t else None,
                     "ecommerce_purchases":orders,
                     "total_purchase_revenue_krw":round(rev,2)}
    # User-supplied GA4 BigQuery purchase totals are reference only: avoid
    # publishing 2025 metrics if dates/aggregation do not reconcile.
    if (source and source.get("status")=="ga4_measured" and
            source.get("period")==periods):
        for year in ("2025","2026"):
            expected=source.get("years",{}).get(year,{})
            if expected.get("orders") and expected["orders"]!=years[year]["ecommerce_purchases"]:
                raise ValueError("GA4 purchase event count differs from event snapshot")
    deltas={field:years["2026"][field]-years["2025"][field]
            for field in ("total_purchasers","first_time_purchasers","repeat_purchasers_derived","ecommerce_purchases")}
    return {"schema":1,"status":"ga4_measured_yoy","report_date":periods["2026"][1],
            "period":periods,"source":"Google Analytics Data API / GA4 property "+PROPERTY,
            "years":years,"deltas":deltas,
            "definitions":{"first_time":"GA4 firstTimePurchasers (first lifetime purchase event as tracked in property)",
              "repeat":"GA4 totalPurchasers minus firstTimePurchasers, not new_vs_returning visitor",
              "revenue":"purchaseRevenue of the whole cohort-combined period, NOT buyer-group revenues",
              "not_member_signup":"Not new membership registrations; may differ from admin order CRM"},
            "quality":{"event_orders_reconciled":True if source and source.get("status")=="ga4_measured" and source.get("period")==periods else None,
                       "first_plus_repeat_equals_total":True,
                       "user_level_data_published":False},
            "causal":False,"contains_personal_data":False}

def save(doc):
    OUT.parent.mkdir(parents=True,exist_ok=True)
    OUT.write_text(json.dumps(doc,ensure_ascii=False,indent=2,allow_nan=False)+"\n",encoding="utf-8")

def main():
    src=read(SOURCE)
    periods=periods_from_product(src)
    existing=read(OUT)
    if existing.get("status")=="ga4_measured_yoy" and existing.get("period")==periods:
        print("GA4 buyer YoY: period already validated")
        return
    def blocked(status,error_code=None):
        if existing.get("status")=="ga4_measured_yoy":
            existing["refresh"]={"status":status,"requested_period":periods}
            save(existing)
        else:
            save({"schema":1,"status":status,"period":periods,
                  "error_code":error_code,"causal":False,
                  "contains_personal_data":False})
    secret=os.getenv("GOOGLE_SA_JSON_B64")
    if not secret:
        blocked("blocked_credentials");return
    try:
        from google.oauth2 import service_account
        from google.auth.transport.requests import AuthorizedSession
        info=json.loads(base64.b64decode(secret,validate=True))
        creds=service_account.Credentials.from_service_account_info(
            info,scopes=["https://www.googleapis.com/auth/analytics.readonly"])
        session=AuthorizedSession(creds)
        results={}
        for year,dates in periods.items():
            for d in dates:dt.date.fromisoformat(d)
            payload={"dateRanges":[{"startDate":dates[0],"endDate":dates[1]}],
                     "metrics":[{"name":name} for name in METRICS],
                     "keepEmptyRows":False,"returnPropertyQuota":False}
            response=session.post(
                "https://analyticsdata.googleapis.com/v1beta/properties/"+PROPERTY+":runReport",
                json=payload,timeout=45)
            if response.status_code!=200:
                print("GA4 Data API status",year,response.status_code)
                blocked("blocked_analytics_data_api",response.status_code)
                return
            results[year]=response.json()
        save(make_report(results,periods,src))
        print("GA4 buyer YoY verified",
              "2025",results["2025"].get("rows",[])[0]["metricValues"][0]["value"],
              "2026",results["2026"].get("rows",[])[0]["metricValues"][0]["value"])
    except Exception as exc:
        print("GA4 buyer YoY error",type(exc).__name__)
        blocked("query_error",type(exc).__name__)

if __name__=="__main__":
    main()
