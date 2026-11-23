#!/usr/bin/env python3
"""Read-only GA4 historical membership-acquisition campaign signup YoY.
Only campaign-level aggregates; no raw identity/transaction/session details.
"""
from __future__ import annotations
import base64, datetime as dt, json, os, re
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/"reports/decision_health/winter_festival_signup_campaign_yoy_2026.json"
PROPERTY="358593394"
PERIODS={"2025":("2025-11-21","2025-11-23"),"2026":("2026-11-20","2026-11-22")}
# Deliberately strict: discounts/welcome/remarketing are NOT membership recruitment.
STRICT=re.compile(r"회원\\s*가입|회원\\s*유치|회원획득|신규\\s*회원\\s*모집|sign[ _-]?up|registr(?:ation|ation)|member[ _-]?acq|membership[ _-]?(?:join|sign|registration)",re.I)
UNSAFE=re.compile(r"@|https?://|[?&](?:uid|user_id|email|phone|token)=|(?:\\+?\\d[\\d\\s().-]{9,}\\d)",re.I)

def clean(s,limit=160):
    s=str(s or "(not set)").strip()
    return s if s and len(s)<=limit and not UNSAFE.search(s) else "(restricted)"

def write(data):
    OUT.parent.mkdir(parents=True,exist_ok=True)
    OUT.write_text(json.dumps(data,ensure_ascii=False,indent=2,allow_nan=False)+"\\n",encoding="utf-8")

def call(session,year,mode):
    start,end=PERIODS[year]
    dims=["date","sessionSourceMedium","sessionManualCampaignName"]
    if mode=="signup":
        dims+=["eventName"]
        mets=["eventCount"]
        flt={"filter":{"fieldName":"eventName","stringFilter":{"matchType":"EXACT","value":"signup_complete"}}}
    else:
        mets=["sessions","ecommercePurchases","purchaseRevenue"]
        flt=None
    payload={"dateRanges":[{"startDate":start,"endDate":end}],
             "dimensions":[{"name":x} for x in dims],
             "metrics":[{"name":x} for x in mets],"limit":"10000","keepEmptyRows":False}
    if flt: payload["dimensionFilter"]=flt
    resp=session.post("https://analyticsdata.googleapis.com/v1beta/properties/"+PROPERTY+":runReport",json=payload,timeout=85)
    if resp.status_code!=200:
        raise RuntimeError("HTTP_"+str(resp.status_code)+"_"+mode+"_"+year)
    doc=resp.json(); rows=doc.get("rows",[])
    if int(doc.get("rowCount",len(rows)))>len(rows):
        raise RuntimeError("result_truncated_"+mode+"_"+year)
    out=[]
    for r in rows:
        a=[str(x.get("value","")) for x in r.get("dimensionValues",[])]
        m=[str(x.get("value","0")) for x in r.get("metricValues",[])]
        if len(a)!=len(dims) or len(m)!=len(mets):
            raise ValueError("unexpected GA4 dimension/metric count")
        # Only tagged membership recruitment campaigns. No user-level rows.
        if not STRICT.search(a[2]):continue
        if clean(a[1])=="(restricted)" or clean(a[2])=="(restricted)":continue
        if mode=="signup":
            out.append({"date":a[0],"source_medium":clean(a[1]),
                       "campaign":clean(a[2]),"signup_complete":int(m[0])})
        else:
            out.append({"date":a[0],"source_medium":clean(a[1]),
                       "campaign":clean(a[2]),"sessions":int(m[0]),
                       "orders":int(m[1]),"revenue":round(float(m[2]))})
    return out

def summarize(raw,days):
    out={}
    for y in ("2025","2026"):
        end=(dt.date.fromisoformat(PERIODS[y][0])+dt.timedelta(days=days-1)).strftime("%Y%m%d")
        src={}
        for row in raw[y]["signup"]:
            if row["date"]>end:continue
            key=(row["source_medium"],row["campaign"])
            d=src.setdefault(key,{"source_medium":key[0],"campaign":key[1],
                                  "signups":0,"sessions":0,"orders":0,"revenue":0})
            d["signups"]+=row["signup_complete"]
        for row in raw[y]["traffic"]:
            if row["date"]>end:continue
            key=(row["source_medium"],row["campaign"])
            d=src.setdefault(key,{"source_medium":key[0],"campaign":key[1],
                                  "signups":0,"sessions":0,"orders":0,"revenue":0})
            for k in ("sessions","orders","revenue"):d[k]+=row[k]
        rows=sorted(src.values(),key=lambda r:(-r["signups"],-r["sessions"],r["campaign"]))
        totals={k:sum(row[k] for row in rows) for k in ("signups","sessions","orders","revenue")}
        out[y]={"rows":rows,"totals":totals,"number_of_distinct_campaign_source_pairs":len(rows)}
    by_source={}
    for y in ("2025","2026"):
        for row in out[y]["rows"]:
            key=row["source_medium"]
            d=by_source.setdefault(key,{"source_medium":key,
                                         "2025":{"signups":0,"sessions":0,"orders":0,"revenue":0},
                                         "2026":{"signups":0,"sessions":0,"orders":0,"revenue":0}})
            for k in ("signups","sessions","orders","revenue"):d[y][k]+=row[k]
    return {"period":{"2025":[PERIODS["2025"][0],(dt.date.fromisoformat(PERIODS["2025"][0])+dt.timedelta(days=days-1)).isoformat()],
                      "2026":[PERIODS["2026"][0],(dt.date.fromisoformat(PERIODS["2026"][0])+dt.timedelta(days=days-1)).isoformat()]},
            "years":out,
            "source_yoy":sorted(by_source.values(),key=lambda x:-(x["2026"]["signups"]+x["2025"]["signups"]))}

def main():
    old={}
    try:old=json.loads(OUT.read_text(encoding="utf-8"))
    except (OSError,ValueError):pass
    b64=os.getenv("GOOGLE_SA_JSON_B64","")
    if not b64:
        write({"schema":1,"status":"blocked_credentials"});return
    try:
        from google.oauth2 import service_account
        from google.auth.transport.requests import AuthorizedSession
        info=json.loads(base64.b64decode(b64,validate=True))
        creds=service_account.Credentials.from_service_account_info(info,scopes=["https://www.googleapis.com/auth/analytics.readonly"])
        session=AuthorizedSession(creds)
        raw={}
        for y in ("2025","2026"):
            raw[y]={"signup":call(session,y,"signup"),"traffic":call(session,y,"traffic")}
        report={n:summarize(raw,d) for n,d in (("D1-D2",2),("D1-D3",3))}
        out={"schema":1,"status":"ga4_measured_yoy","generated_utc":dt.datetime.now(dt.timezone.utc).isoformat(),
             "ga4_property":PROPERTY,"source":"Google Analytics Data API",
             "scope":"Strict explicit membership-acquisition campaign names only, matching '회원가입/회원유치/signup/registration/member acquisition'",
             "definition":{"signup":"signup_complete eventCount by sessionManualCampaignName and sessionSourceMedium",
                           "campaign":"GA4 sessionManualCampaignName (utm_campaign), NOT ad platform objective",
                           "event_purchase":"ALL visitors' ecommercePurchases attributed to the same campaign; NOT a signup-to-purchase cohort",
                           "cvr":"signup_complete eventCount / sessions; event rate rather than unique joined members",
                           "yoy":"Corresponding Friday-to-Sunday event day windows"},
             "periods":report,"contains_personal_data":False,"causal":False}
        write(out)
        print("GA4 SIGNUP CAMPAIGN YOY",
              {p:{y:report[p]["years"][y]["totals"]["signups"] for y in ("2025","2026")} for p in report})
    except Exception as e:
        print("GA4 SIGNUP CAMPAIGN ERROR",type(e).__name__,str(e)[:110])
        if old.get("status")=="ga4_measured_yoy":
            old["refresh"]={"status":"query_error","code":type(e).__name__}
            write(old)
        else:
            write({"schema":1,"status":"query_error","code":type(e).__name__,"contains_personal_data":False})

if __name__=="__main__":
    main()
