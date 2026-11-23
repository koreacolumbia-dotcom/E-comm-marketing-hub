#!/usr/bin/env python3
"""Sanitized GA4 historical Winter Festival signup source/medium YoY.
No user identifiers, no member data, no secrets, and no raw event rows persisted.
"""
from __future__ import annotations
import base64, datetime as dt, json, os, re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "reports/decision_health/winter_festival_signup_source_medium_yoy_2026.json"
PROPERTY = "358593394"
PERIODS = {
  "2025": ["2025-11-21", "2025-11-23"],
  "2026": ["2026-11-20", "2026-11-22"],
}
EVENTS = ("signup_complete", "sign_up", "sign_up_complete")
UNSAFE = re.compile(r"@|https?://|[?&](?:uid|user_id|email|phone|token)=|\\+?[0-9][0-9().\\s-]{9,}[0-9]", re.I)

def clean(value):
    s = str(value or "(not set)").strip()
    if not s or len(s) > 105 or UNSAFE.search(s):
        return "(restricted)"
    return s

def save(payload):
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")

def run_report(session, year, kind):
    start, end = PERIODS[year]
    dimensions = [{"name":"date"}, {"name":"sessionSourceMedium"}]
    if kind == "signup":
        dimensions.append({"name":"eventName"})
        metrics = [{"name":"eventCount"}]
        filt = {"filter":{"fieldName":"eventName","inListFilter":{"values":list(EVENTS)}}}
    else:
        metrics = [{"name":x} for x in ("sessions","ecommercePurchases","purchaseRevenue")]
        filt = None
    body = {"dateRanges":[{"startDate":start,"endDate":end}],
            "dimensions":dimensions,"metrics":metrics,"limit":"10000",
            "keepEmptyRows":False,"returnPropertyQuota":False}
    if filt:
        body["dimensionFilter"] = filt
    url = "https://analyticsdata.googleapis.com/v1beta/properties/" + PROPERTY + ":runReport"
    response = session.post(url, json=body, timeout=90)
    if response.status_code != 200:
        raise RuntimeError("GA4 HTTP " + str(response.status_code) + " kind=" + kind)
    data = response.json()
    rows = data.get("rows", [])
    if data.get("rowCount", len(rows)) > len(rows):
        raise RuntimeError("GA4 truncated rows; refusing incomplete totals")
    output = []
    for r in rows:
        keys = [v.get("value","") for v in r.get("dimensionValues",[])]
        vals = [v.get("value","0") for v in r.get("metricValues",[])]
        if len(keys) != len(dimensions) or len(vals) != len(metrics):
            raise RuntimeError("Unexpected GA4 response shape")
        sm = clean(keys[1])
        if sm == "(restricted)":
            sm = "(restricted source)"
        if kind == "signup":
            output.append((keys[0],sm,keys[2],int(vals[0])))
        else:
            output.append((keys[0],sm,int(vals[0]),int(vals[1]),round(float(vals[2]))))
    return output

def summarize(raw):
    result = {}
    for label, days in (("D1-D2", 2), ("D1-D3", 3)):
        rows = {}
        for year in ("2025","2026"):
            window_end = (dt.date.fromisoformat(PERIODS[year][0]) + dt.timedelta(days=days-1)).strftime('%Y%m%d')
            tmp = {}
            for d, src, event, count in raw[year]["signup"]:
                if d > window_end: continue
                rec = tmp.setdefault(src,{"signup_complete":0,"sign_up":0,"sign_up_complete":0,
                                          "sessions":0,"orders":0,"revenue":0})
                if event in EVENTS:
                    rec[event] += count
            for d,src,sessions,orders,revenue in raw[year]["traffic"]:
                if d > window_end: continue
                rec=tmp.setdefault(src,{"signup_complete":0,"sign_up":0,"sign_up_complete":0,
                                        "sessions":0,"orders":0,"revenue":0})
                rec["sessions"] += sessions
                rec["orders"] += orders
                rec["revenue"] += revenue
            rows[year] = tmp
        all_sources = set(rows["2025"]) | set(rows["2026"])
        report_rows=[]
        for src in all_sources:
            prior=rows["2025"].get(src,{})
            curr=rows["2026"].get(src,{})
            source_obj={"source_medium":src}
            for y, r in (("2025",prior),("2026",curr)):
                source_obj[y]={k:int(r.get(k,0)) for k in
                    ("signup_complete","sign_up","sign_up_complete","sessions","orders","revenue")}
            report_rows.append(source_obj)
        report_rows.sort(key=lambda z:(
          -(z["2026"]["signup_complete"]+z["2025"]["signup_complete"]),
          -(z["2026"]["sign_up"]+z["2025"]["sign_up"]),z["source_medium"]))
        totals={y:{k:sum(r[y][k] for r in report_rows)
                for k in ("signup_complete","sign_up","sign_up_complete","sessions","orders","revenue")}
                for y in ("2025","2026")}
        result[label] = {
            "period":{"2025":[PERIODS["2025"][0],(dt.date.fromisoformat(PERIODS['2025'][0])+dt.timedelta(days=days-1)).isoformat()],
                      "2026":[PERIODS["2026"][0],(dt.date.fromisoformat(PERIODS['2026'][0])+dt.timedelta(days=days-1)).isoformat()]},
            "rows":report_rows,"totals":totals,
            "matched_signup_complete_in_both_years":totals["2025"]["signup_complete"] > 0 and totals["2026"]["signup_complete"] > 0
        }
    return result

def main():
    existing={}
    try: existing=json.loads(OUT.read_text(encoding="utf-8"))
    except (FileNotFoundError,ValueError): pass
    secret=os.getenv("GOOGLE_SA_JSON_B64")
    if not secret:
        save({"schema":1,"status":"blocked_credentials","note":"GA4 service account secret missing"});return
    try:
        from google.oauth2 import service_account
        from google.auth.transport.requests import AuthorizedSession
        info=json.loads(base64.b64decode(secret,validate=True))
        creds=service_account.Credentials.from_service_account_info(
            info,scopes=["https://www.googleapis.com/auth/analytics.readonly"])
        session=AuthorizedSession(creds)
        raw={y:{"signup":run_report(session,y,"signup"),
                "traffic":run_report(session,y,"traffic")} for y in ("2025","2026")}
        data=summarize(raw)
        save({"schema":1,"status":"ga4_measured_yoy",
              "generated_utc":dt.datetime.now(dt.timezone.utc).isoformat(),
              "source":"GA4 Data API property "+PROPERTY,
              "definition":{"signup":"eventCount filtered to exact eventName=signup_complete (NOT GA4 newUsers)",
                            "alternative_events":"sign_up and sign_up_complete shown separately; do NOT add automatically",
                            "purchase":"same source/medium's ALL purchasers, not necessarily newly registered members",
                            "attribution":"GA4 sessionSourceMedium for signup events and separate traffic metrics",
                            "same_weekday":"2025 11/21 Fri vs 2026 11/20 Fri; matched Fri-Sun 3-day comparison"},
              "periods":data,"causal":False,"contains_personal_data":False})
        print("SIGNUP YOY MEASURED", {p:{y:data[p]["totals"][y]["signup_complete"]
                                      for y in ("2025","2026")} for p in data})
    except Exception as exc:
        safe=type(exc).__name__
        print("SIGNUP YOY ERROR",safe)
        if existing.get("status")=="ga4_measured_yoy":
            existing["refresh"]={"status":"query_error","type":safe}
            save(existing)
        else:
            save({"schema":1,"status":"query_error","type":safe,
                  "causal":False,"contains_personal_data":False})

if __name__=="__main__":
    main()
