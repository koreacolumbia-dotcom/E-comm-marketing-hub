#!/usr/bin/env python3
"""Winter Festival report-only evidence bundle. Standard library; NO SQL/PII/API calls.

Reads already-published aggregate GA4 sources. Never presents GA4 attributed
revenue as ad-platform ROAS or incremental campaign effects.
"""
from __future__ import annotations
import datetime as dt
import json
import math
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
BASE=ROOT/"reports"
OUT=BASE/"decision_health/winter_festival_integrated_latest.json"
OPS=BASE/"marketing_operations/latest.json"
SOURCES=BASE/"decision_health/source_medium_weekly.json"
UTM=BASE/"decision_health/winter_festival_utm_owned_2026.json"
INTEL=BASE/"marketing_intelligence_v2/latest.json"
DAY_KEYS=[("2026-11-20","2025-11-21"),("2026-11-21","2025-11-22"),
          ("2026-11-22","2025-11-23")]
HIGHLIGHT={
    "meta / banner":("meta_banner","Meta / banner","paid"),
    "META_BD / META_BD":("meta_bd","META_BD / META_BD","paid"),
    "Facebook / banner":("facebook_banner","Facebook / banner","paid"),
    "naverbs / cpc":("naver_brand","네이버 브랜드검색","paid"),
    "naver / cpc":("naver_cpc","네이버 일반검색","paid"),
    "criteo / banner":("criteo","Criteo / banner","paid"),
    "kakao / da":("kakao_da","카카오 DA","paid"),
    "KAKAO_CH / KAKAO_CH":("kakao_owned","카카오 채널","crm"),
    "LMS_ECOM / LMS_ECOM":("lms_owned","LMS","crm"),
    "EDM / EDM":("edm_owned","EDM","crm"),
    "google / organic":("google_organic","Google 자연검색","organic"),
    "naver / organic":("naver_organic","Naver 자연검색","organic"),
}
def read(path):
    try:
        d=json.loads(path.read_text(encoding="utf-8"))
        return d if isinstance(d,dict) else {}
    except (OSError,ValueError,UnicodeError):
        return {}

def valid(v):
    return type(v) in (int,float) and math.isfinite(v) and v>=0

def metrics(v):
    if not isinstance(v,dict):return None
    sessions=v.get("sessions")
    orders=v.get("transactions",v.get("orders"))
    revenue=v.get("revenue")
    if not all(valid(x) for x in (sessions,orders,revenue)):return None
    s,o,r=int(sessions),int(orders),round(revenue)
    return {"sessions":s,"orders":o,"revenue":r,
            "cvr_pct":round(100*o/s,3) if s else None}

def event_days(ops,old):
    year=((ops.get("weekly") or {}).get("yoy") or {})
    rows=year.get("daily_comparison") or []
    index={(r.get("date"),r.get("ly_date")):r for r in rows
           if isinstance(r,dict)}
    matched=[]
    for current,previous in DAY_KEYS:
        row=index.get((current,previous),{})
        now=metrics(row.get("current"))
        ly=metrics(row.get("last_year"))
        if now is None or ly is None or str(ops.get("report_date",""))<current:
            break
        matched.append({"day":len(matched)+1,"current_date":current,"previous_date":previous,
                        "current":now,"previous":ly})
    if len(matched)>=2 and year.get("status")=="ready":
        sums={}
        for period in ("current","previous"):
            raw={k:sum(day[period][k] for day in matched)
                 for k in ("sessions","orders","revenue")}
            sums[period]=metrics(raw)
        return {"status":"verified_ga4","days":len(matched),"daily":matched,
                "totals":sums,"comparison":"same_event_day_same_weekday_364_offset",
                "source_report_date":ops.get("report_date"),"causal":False}
    historic=old.get("event") or {}
    if historic.get("status")=="verified_ga4" and historic.get("days",0)>=2:
        return dict(historic, retention_reason="Last verified event-day comparison retained after rolling weekly window changed.")
    return {"status":"not_ready","days":0,"daily":[],"totals":{},"causal":False}

def weekly_channels(source,ops):
    period=source.get("period") or {}
    rows=source.get("rows") or []
    out=[]
    for row in rows:
        name=row.get("source_medium")
        if name not in HIGHLIGHT:continue
        identity,label,kind=HIGHLIGHT[name]
        previous=metrics(row.get("previous"))
        current=metrics(row.get("current"))
        if previous is None or current is None:continue
        out.append({"id":identity,"source_medium":name,"label":label,"kind":kind,
                    "previous":previous,"current":current,
                    "revenue_delta":current["revenue"]-previous["revenue"]})
    out.sort(key=lambda x:(x["kind"]!="paid",-x["current"]["revenue"],x["label"]))
    is_fresh=(source.get("report_date")==ops.get("report_date"))
    is_measured=(source.get("status")=="ga4_measured")
    return {"status":"ga4_measured" if is_measured else "unavailable",
            "report_date":source.get("report_date"),
            "period":period,"rows":out,
            "date_aligned_with_daily_kpi":is_fresh,
            "source_type":"ga4_session_source_medium",
            "comparison":"current_7d_vs_previous_7d_NOT_year_over_year",
            "caveat":"GA4 session-attributed metrics. This 7-day period is not a festival-only D1-D3 cohort."}

def media_event(utm,old):
    media=utm.get("media_rollup")
    period=utm.get("period")
    data={}
    if (utm.get("status")=="ga4_measured"
            and isinstance(media,dict) and all(y in media for y in ("2025","2026"))
            and isinstance(period,dict)):
        return {"status":"ga4_measured","period":period,"years":media,
                "source_report_date":utm.get("report_date"),
                "causal":False,"scope":"All session source/medium grouped by paid, owned, organic, referral and direct; NOT Meta Ads spend or incremental lift.",
                "cost":utm.get("cost")}
    prior=old.get("event_media") or {}
    if prior.get("status")=="ga4_measured":
        return dict(prior, retention_reason="Previous verified media view retained until bounded GA4 query succeeds.")
    return {"status":"not_ready","period":utm.get("period"),"years":{},
            "source_report_date":utm.get("report_date"),"causal":False}

def owned_comparison(utm):
    data=utm.get("data") or {}
    if utm.get("status")!="ga4_measured":return {"status":"not_ready"}
    out={}
    for year in ("2025","2026"):
        record=(data.get(year) or {}).get("grouped_totals") or {}
        out[year]=record
    return {"status":"ga4_measured","period":utm.get("period"),"grouped_totals":out,
            "tag_naming_not_equivalent":True,"causal":False}

def build(ops,source,utm,intel,previous=None):
    previous=previous or {}
    integration=((intel.get("integrations") or {}).get("paid_cost") or {})
    paid={"status":integration.get("status","unavailable"),
          "latest_date":integration.get("latest_date"),
          "roas_eligible":integration.get("roas_eligible") is True and
            integration.get("latest_date")==ops.get("report_date"),
          "source":integration.get("source"),
          "note":"No verified daily Meta/Google/Naver ad spend for event dates. Do not infer ROAS or CPA."}
    e=event_days(ops,previous)
    w=weekly_channels(source,ops)
    u=media_event(utm,previous)
    now=dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).isoformat(timespec="seconds")
    return {"schema":2,"generated_at_kst":now,"report_date":ops.get("report_date"),
            "event":e,"weekly_media":w,"event_media":u,
            "owned_utm":owned_comparison(utm),"paid_cost":paid,
            "updated_from":["reports/marketing_operations/latest.json",
                            "reports/decision_health/source_medium_weekly.json",
                            "reports/decision_health/winter_festival_utm_owned_2026.json",
                            "reports/marketing_intelligence_v2/latest.json"],
            "quality":{"reliable_event_days":e.get("days",0),
                       "event_media_measured":u.get("status")=="ga4_measured",
                       "weekly_media_date_aligned":w["date_aligned_with_daily_kpi"],
                       "roas_eligible":paid["roas_eligible"],
                       "caution":"GA4 attribution is descriptive, not causal. Media costs are separately verified. No ad-platform campaign spend is implied."}}

def main():
    current=build(read(OPS),read(SOURCES),read(UTM),read(INTEL),read(OUT))
    OUT.parent.mkdir(parents=True,exist_ok=True)
    serialized=json.dumps(current,ensure_ascii=False,allow_nan=False,indent=2)+"\n"
    # Avoid unnecessary commits if only the clock changed and source data did not.
    prior=read(OUT)
    prior.pop("generated_at_kst",None)
    comparable=dict(current)
    comparable.pop("generated_at_kst",None)
    if prior==comparable:
        print("[FESTIVAL] unchanged",current["report_date"])
        return
    OUT.write_text(serialized,encoding="utf-8")
    print("[FESTIVAL] event D",current["event"]["days"],
          "media",current["event_media"]["status"],
          "weekly",current["weekly_media"]["report_date"],
          "roas_eligible",current["paid_cost"]["roas_eligible"])

if __name__=="__main__":
    main()
