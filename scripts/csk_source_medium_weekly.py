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
PER_QUERY_CAP=2*1024**3
TOTAL_DRY_CAP=4*1024**3
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
        print("[SOURCE MEDIUM] Same 7-day windows already measured; no query")
        return
    secret=os.getenv("GOOGLE_SA_JSON_B64","").strip()
    if not secret:
        save({**status,"status":"blocked_credentials"})
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
            save({**status,"status":"blocked_budget","cost":cost})
            print("[SOURCE MEDIUM] Exceeds strict dry-run cap; zero queries executed",cost["dry_run_bytes"])
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
        save(out)
        print("[SOURCE MEDIUM] ga4_measured",len(out["rows"]),"source/medium combinations",
              "sessions",out["totals"]["current"]["sessions"],"billed_bytes",billing)
    except Exception as exc:
        # No raw SQL, event properties or service-account secrets in the public file.
        save({**status,"status":"query_error","error_type":type(exc).__name__})
        print("[SOURCE MEDIUM] Unavailable:",type(exc).__name__)


if __name__=="__main__":
    main()
