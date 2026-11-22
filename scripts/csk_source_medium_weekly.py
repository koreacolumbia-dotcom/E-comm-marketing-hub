#!/usr/bin/env python3
"""GA4 export session source/medium: exact operational comparison weeks, no PII.

Reuses Daily Digest canonical session attribution and a BigQuery dry-run cost gate.
A source/medium conversion rate is transaction count / observed GA4 session keys,
NOT the administrator-wide conversion rate or experimental media lift.
"""
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
OUT=ROOT/"reports/decision_health/source_medium_weekly.json"
OPS=ROOT/"reports/marketing_operations/latest.json"
TABLE="columbia-ga4.analytics_358593394.events_*"
LOCATION="asia-northeast3"
PER_QUERY_CAP=3*1024**3+8*1024**2  # +8 MiB headroom; combined 6-GiB hard cap unchanged
TOTAL_DRY_CAP=6*1024**3
METRICS=("sessions","transactions","revenue")


def read(path):
    try:
        value=json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value,dict) else {}
    except (OSError,UnicodeError,ValueError):
        return {}


def window(ops):
    w=ops.get("weekly") or {}
    if w.get("status")!="ready" or any((w.get(x) or {}).get("status")!="complete" for x in ("current","previous")):
        return None
    try:
        a,b=[dt.date.fromisoformat(x.strip()) for x in w["period"].split("~")]
        c,d=[dt.date.fromisoformat(x.strip()) for x in w["comparison_period"].split("~")]
    except (ValueError,KeyError,AttributeError,TypeError):
        return None
    if (b-a).days!=6 or (d-c).days!=6 or (a-d).days!=1 or ops.get("report_date")!=b.isoformat():
        return None
    return {"previous":{"start":c.isoformat(),"end":d.isoformat()},
            "current":{"start":a.isoformat(),"end":b.isoformat()}}


def source_label(x):
    """Keep the original session source/medium, but never publish email/phone/URLs."""
    x=str(x or "(not set) / (not set)").strip()
    if len(x)>180 or re.search(r"@|(?:\+?\d[\d\s().-]{9,}\d)|https?://|[?&](?:token|email|uid)=",x,re.I):
        return "(restricted) / (restricted)"
    return x or "(not set) / (not set)"


def n(x):
    if type(x) not in (int,float) or not math.isfinite(x) or x<0:
        raise ValueError("Invalid GA4 aggregate")
    return float(x)


def summarize(current,previous,dates,scope):
    store=defaultdict(lambda:{"previous":dict.fromkeys(METRICS,0.0),
                              "current":dict.fromkeys(METRICS,0.0)})
    totals={}
    for period,raw in (("previous",previous),("current",current)):
        if not raw:
            raise ValueError("Empty source/medium report")
        total={x:0.0 for x in METRICS}
        expected={int(n(r["expected_sessions"])) for r in raw}
        if len(expected)!=1 or next(iter(expected))<=0:
            raise ValueError("Cannot reconcile GA4 session keys")
        for row in raw:
            key=source_label(row.get("sessionSourceMedium"))
            values={"sessions":n(row.get("sessions")),"transactions":n(row.get("transactions")),
                    "revenue":n(row.get("purchaseRevenue"))}
            for k,v in values.items():
                store[key][period][k]+=v
                total[k]+=v
        if round(total["sessions"])!=next(iter(expected)):
            raise ValueError("GA4 session-key count mismatch")
        totals[period]={k:round(v,2) for k,v in total.items()}
        totals[period]["cvr_pct"]=round(100*total["transactions"]/total["sessions"],3)
    rows=[]
    for key,records in store.items():
        old,now=records["previous"],records["current"]
        session_pct=(now["sessions"]/old["sessions"]-1)*100 if old["sessions"] else None
        cvr0=100*old["transactions"]/old["sessions"] if old["sessions"] else None
        cvr1=100*now["transactions"]/now["sessions"] if now["sessions"] else None
        rows.append(dict(source_medium=key,
            previous={**{k:round(v,2) for k,v in old.items()},"cvr_pct":round(cvr0,3) if cvr0 is not None else None},
            current={**{k:round(v,2) for k,v in now.items()},"cvr_pct":round(cvr1,3) if cvr1 is not None else None},
            sessions_change_pct=round(session_pct,2) if session_pct is not None else None,
            session_delta=round(now["sessions"]-old["sessions"]),
            transactions_delta=round(now["transactions"]-old["transactions"]),
            cvr_change_pp=round(cvr1-cvr0,3) if cvr0 is not None and cvr1 is not None else None,
            revenue_delta=round(now["revenue"]-old["revenue"],2)))
    rows.sort(key=lambda x:(-x["current"]["sessions"],-x["current"]["revenue"],x["source_medium"]))
    for period in ("previous","current"):
        for k in METRICS:
            if abs(sum(x[period][k] for x in rows)-totals[period][k])>max(1.0,len(rows)*0.011):
                raise ValueError("Source/medium totals do not reconcile")
    return dict(schema=1,status="ga4_measured",report_date=dates["current"]["end"],
        period=dates,definition="GA4 export session-key source/medium (same attribution logic as Daily Digest); "
          "transactions / sessions for CVR, purchase event revenue (not admin net sales).",
        source="daily_digest_session_source.py: build_sql/select_partitions",
        totals=totals,rows=rows,
        row_count=len(rows),cost=scope,
        caveat="Source/medium performance is observational, not incremental causal lift; "
               "GA4 export sessions and revenue may differ from GA4 API and administrative totals.")



def cached_subchannel_fallback(dates,root=None):
    """Zero-query fallback from already published Daily Digest subchannel totals.

    Explicitly NOT raw GA4 session source/medium. Keep channel bucket in key,
    and put gaps in UNALLOCATED rather than assigning them to a plausible source.
    """
    daily=root or (ROOT/"reports/daily_digest/data/daily")
    records=defaultdict(lambda:{"previous":dict.fromkeys(METRICS,0.0),
                                "current":dict.fromkeys(METRICS,0.0)})
    period_totals={}
    missing=[]; unstamped=[]; residuals=[]
    for period in ("previous","current"):
        total={key:0.0 for key in METRICS}
        start=dt.date.fromisoformat(dates[period]["start"])
        for offset in range(7):
            date=(start+dt.timedelta(days=offset)).isoformat()
            day=read(daily/f"{date}.json")
            channel=day.get("channels") or {}
            breakdown=day.get("channel_detail_map") or {}
            kpi=day.get("kpis") or {}
            if (day.get("mode") not in ("daily",None) or day.get("end_date")!=date
                or not isinstance(breakdown,dict) or not breakdown or
                not isinstance(channel,dict) or
                not all(type(kpi.get(k)) in (int,float) for k in ("sessions","orders","revenue"))):
                missing.append(date);continue
            if day.get("traffic_metric_source")!="ga4_bigquery_session_key_v1":
                unstamped.append(date)
            perday={"sessions":0.0,"transactions":0.0,"revenue":0.0}
            for bucket,value in channel.items():
                if bucket=="Total":continue
                items=breakdown.get(bucket) or []
                for key,origin in (("sessions","sessions"),("transactions","orders"),("revenue","revenue")):
                    if type(value.get(origin)) not in (int,float) or value[origin]<0:
                        missing.append(date);break
                else:
                    expected={"sessions":float(value["sessions"]),
                              "transactions":float(value["orders"]),
                              "revenue":float(value["revenue"])}
                    allocated={key:0.0 for key in METRICS}
                    for item in items:
                        if not isinstance(item,dict):missing.append(date);break
                        try:
                            values={"sessions":n(item.get("sessions")),
                                    "transactions":n(item.get("orders")),
                                    "revenue":n(item.get("purchaseRevenue"))}
                        except ValueError:
                            missing.append(date);break
                        name=source_label(item.get("sub_channel") or "미분류")
                        key=bucket+" · "+name
                        for metric in METRICS:
                            allocated[metric]+=values[metric]
                            records[key][period][metric]+=values[metric]
                    gap={metric:round(expected[metric]-allocated[metric],2) for metric in METRICS}
                    if any(x< -1.01 for x in gap.values()):
                        missing.append(date);continue
                    if any(x>0.01 for x in gap.values()):
                        # Missing subcategory attribution remains unknown.
                        key=bucket+" · 미배분(원본 세부채널 없음)"
                        for metric in METRICS:
                            if gap[metric]>0:
                                records[key][period][metric]+=gap[metric]
                        residuals.append({"date":date,"bucket":bucket,
                                          **{m:gap[m] for m in METRICS}})
                    for metric in METRICS:perday[metric]+=expected[metric]
            if any(abs(perday[k]-float(kpi[mapkey]))>1.01 for k,mapkey in
                   (("sessions","sessions"),("transactions","orders"),("revenue","revenue"))):
                missing.append(date)
            for key in METRICS: total[key]+=perday[key]
        period_totals[period]=total
    if missing:
        return {"schema":1,"report_date":dates["current"]["end"],"period":dates,
                "status":"fallback_incomplete","rows":[],"missing_dates":sorted(set(missing))}
    rows=[]
    for key,v in records.items():
        before,after=v["previous"],v["current"]
        cvr0=100*before["transactions"]/before["sessions"] if before["sessions"] else None
        cvr1=100*after["transactions"]/after["sessions"] if after["sessions"] else None
        rows.append({
            "source_medium":key,
            "previous":{"sessions":round(before["sessions"],2),"transactions":round(before["transactions"],2),
                        "revenue":round(before["revenue"],2),"cvr_pct":round(cvr0,3) if cvr0 is not None else None},
            "current":{"sessions":round(after["sessions"],2),"transactions":round(after["transactions"],2),
                       "revenue":round(after["revenue"],2),"cvr_pct":round(cvr1,3) if cvr1 is not None else None},
            "sessions_change_pct":round(100*(after["sessions"]/before["sessions"]-1),2) if before["sessions"] else None,
            "session_delta":round(after["sessions"]-before["sessions"]),
            "transactions_delta":round(after["transactions"]-before["transactions"]),
            "cvr_change_pp":round(cvr1-cvr0,3) if cvr0 is not None and cvr1 is not None else None,
            "revenue_delta":round(after["revenue"]-before["revenue"],2)
        })
    rows.sort(key=lambda row:(-row["current"]["sessions"],row["source_medium"]))
    return {"schema":1,"status":"ga4_subchannel_aggregated",
            "report_date":dates["current"]["end"],"period":dates,
            "definition":"Daily Digest 채널 버킷×세부채널 합산. 원본 GA4 source/medium과 동등하지 않음.",
            "source":"reports/daily_digest/data/daily/YYYY-MM-DD.json:channel_detail_map",
            "totals":{p:{**{k:round(v,2) for k,v in vals.items()},
                          "cvr_pct":round(100*vals["transactions"]/vals["sessions"],3)}
                      for p,vals in period_totals.items()},
            "rows":rows,"row_count":len(rows),
            "cost":{"billed_bytes":0,"reused_aggregate":True,
                    "per_query_cap":PER_QUERY_CAP,"combined_cap":TOTAL_DRY_CAP},
            "quality":{"unstamped_traffic_source_days":unstamped,
                       "unallocated_rows":residuals,
                       "source_medium_exact":False,
                       "caveat":"과거 일부 일자의 채널 측정 방식 표기가 없고 세부채널 누락분이 존재. 소스/매체 원본 정확도 미확인."},
            "caveat":"새 BigQuery 조회 0회. 실제 세부채널 분류이며 광고비·증분 기여도·원본 source/medium이 아닙니다."}



def audit_weekly_totals(report,operations):
    """Keep measurable reconciliation gaps visible, never silently force equality."""
    weekly=operations.get("weekly") or {}
    checks={}
    strict=True
    within=True
    for period,label in (("current","current"),("previous","previous")):
        observed=(report.get("totals") or {}).get(period) or {}
        expected=((weekly.get(label) or {}).get("total") or {})
        gaps={}
        for metric,kpi in (("sessions","sessions"),("transactions","orders"),("revenue","revenue")):
            a,b=observed.get(metric),expected.get(kpi)
            if type(a) not in (int,float) or type(b) not in (int,float) or b<=0:
                gaps[metric]={"status":"unavailable"}
                strict=False;within=False
                continue
            diff=round(float(a)-float(b),2)
            pct=round(100*diff/float(b),4)
            gaps[metric]={"observed":a,"kpi":b,"difference":diff,"difference_pct":pct}
            if abs(diff)>(1 if metric=="revenue" else 0):strict=False
            if abs(pct)>0.05:within=False
        checks[period]=gaps
    report["quality"]={**(report.get("quality") or {}),
                       "weekly_reconciliation":checks,
                       "strict_total_match":strict,
                       "within_005_pct":within,
                       "source_level":"exact_ga4_session_source_medium" if report.get("status")=="ga4_measured"
                                      else "daily_channel_subcategory_not_raw_source_medium",
                       "note":"세션 키 산출과 Daily 주간 KPI 간 소수 집계 차이는 별도 기록. 주문·매출은 비교 가능 여부 확인."}
    return report


def save(x):
    OUT.parent.mkdir(parents=True,exist_ok=True)
    OUT.write_text(json.dumps(x,ensure_ascii=False,indent=2,allow_nan=False)+"\n",encoding="utf-8")


def main():
    ops=read(OPS)
    dates=window(ops)
    status={"schema":1,"report_date":ops.get("report_date"),"period":dates,
            "status":"source_not_ready","rows":[],"cost":{"per_query_cap":PER_QUERY_CAP,"combined_cap":TOTAL_DRY_CAP,
                                                           "dry_run_required":True}}
    if not dates:
        save(status)
        return
    cached=read(OUT)
    if (cached.get("status")=="ga4_measured" and cached.get("period")==dates and cached.get("schema")==1
            and os.getenv("CSK_SOURCE_MEDIUM_FORCE")!="1"):
        if not (cached.get("quality") or {}).get("weekly_reconciliation"):
            save(audit_weekly_totals(cached,ops))
        print("[SOURCE MEDIUM] Same 7-day windows already measured; no query")
        return
    secret=os.getenv("GOOGLE_SA_JSON_B64","").strip()
    if not secret:
        fallback=cached_subchannel_fallback(dates)
        save({**fallback,"cost":{**status["cost"],**fallback.get("cost",{})},"raw_source_status":"blocked_credentials"}
             if fallback.get("status")=="ga4_subchannel_aggregated"
             else {**status,"status":"blocked_credentials"})
        return
    try:
        from google.cloud import bigquery
        from google.oauth2 import service_account
        sys.path.insert(0,str(ROOT))
        from daily_digest_session_source import select_partitions,build_sql
        info=json.loads(base64.b64decode(secret,validate=True))
        credentials=service_account.Credentials.from_service_account_info(info)
        client=bigquery.Client(project=info["project_id"],credentials=credentials,location=LOCATION)
        prepared={}
        estimates={}
        for period in ("previous","current"):
            r=dates[period]
            a,b=dt.date.fromisoformat(r["start"]),dt.date.fromisoformat(r["end"])
            parts=select_partitions(client,TABLE,a,b)
            sql=build_sql(parts,r["start"],r["end"])
            job=client.query(sql,location=LOCATION,
                 job_config=bigquery.QueryJobConfig(dry_run=True,use_query_cache=False))
            estimate=int(job.total_bytes_processed or 0)
            if estimate<=0:
                save({**status,"status":"blocked_estimate"})
                return
            estimates[period]=estimate
            prepared[period]=sql
        cost={**status["cost"],"dry_run_bytes":estimates,"dry_run_total_bytes":sum(estimates.values())}
        if any(v>PER_QUERY_CAP for v in estimates.values()) or sum(estimates.values())>TOTAL_DRY_CAP:
            fallback=cached_subchannel_fallback(dates)
            save({**fallback,"cost":{**cost,**fallback.get("cost",{})},"raw_source_status":"blocked_budget"}
                 if fallback.get("status")=="ga4_subchannel_aggregated"
                 else {**status,"status":"blocked_budget","cost":cost})
            print("[SOURCE MEDIUM] Exceeds strict dry-run cap; raw query not executed",cost["dry_run_bytes"])
            return
        results={}
        billing={}
        for period in ("previous","current"):
            job=client.query(prepared[period],location=LOCATION,
                job_config=bigquery.QueryJobConfig(maximum_bytes_billed=PER_QUERY_CAP,
                                                    use_query_cache=True))
            results[period]=[dict(row) for row in job.result(timeout=200)]
            billing[period]=int(job.total_bytes_billed or 0)
        out=summarize(results["current"],results["previous"],dates,
                       {**cost,"billed_bytes":billing})
        save(audit_weekly_totals(out,ops))
        print("[SOURCE MEDIUM] ga4_measured",len(out["rows"]),"source/medium combinations",
              "sessions",out["totals"]["current"]["sessions"],"billed_bytes",billing)
    except Exception as exc:
        # No raw SQL, event properties or service-account secrets in the public file.
        fallback=cached_subchannel_fallback(dates)
        save({**fallback,"cost":{**status["cost"],**fallback.get("cost",{})},"raw_source_status":"query_error",
              "raw_error_type":type(exc).__name__}
             if fallback.get("status")=="ga4_subchannel_aggregated"
             else {**status,"status":"query_error","error_type":type(exc).__name__})
        print("[SOURCE MEDIUM] Unavailable:",type(exc).__name__)


if __name__=="__main__":
    main()
