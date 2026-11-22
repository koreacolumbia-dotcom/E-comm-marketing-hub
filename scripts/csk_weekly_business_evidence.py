#!/usr/bin/env python3
"""Period-validated 7-day business/channel evidence from already-published daily aggregates.

No warehouse spend, customer identifiers, or claims of causal channel effects.
Reject incomplete dates, mismatched order/revenue sums and impossible category joins.
"""
import datetime as dt
import json
import math
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
OPS=ROOT/"reports/marketing_operations/latest.json"
DAILY=ROOT/"reports/daily_digest/data/daily"
OUT=ROOT/"reports/decision_health/weekly_business_evidence.json"
FIELDS=("sessions","orders","revenue")


def read(path):
    try:
        value=json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value,dict) else {}
    except (OSError,UnicodeError,ValueError):
        return {}


def period(text):
    try:
        a,b=[dt.date.fromisoformat(v.strip()) for v in text.split("~")]
    except (ValueError,AttributeError,TypeError):
        return None
    if (b-a).days!=6:
        return None
    return [(a+dt.timedelta(days=i)).isoformat() for i in range(7)]


def number(obj,k):
    value=obj.get(k) if isinstance(obj,dict) else None
    return float(value) if type(value) in (int,float) and math.isfinite(value) and value>=0 else None


def build(ops,read_day):
    weekly=ops.get("weekly") or {}
    current=period(weekly.get("period"))
    previous=period(weekly.get("comparison_period"))
    stub=dict(schema=1,status="source_incomplete",report_date=ops.get("report_date"),
              current_period=weekly.get("period"),previous_period=weekly.get("comparison_period"),
              channels=[],quality=dict(missing_dates=[],daily_channel_session_mismatch_dates=[]),
              note="Same-window channel revenue attribution; not channel causation or incremental lift.")
    if (weekly.get("status")!="ready" or not current or not previous or
        previous[-1]!=(dt.date.fromisoformat(current[0])-dt.timedelta(days=1)).isoformat() or
        ops.get("report_date")!=current[-1] or
        any((weekly.get(k) or {}).get("status")!="complete" for k in ("current","previous"))):
        return stub
    periods={"previous":previous,"current":current}
    totals={k:dict.fromkeys(FIELDS,0.) for k in periods}
    channels={k:{} for k in periods}
    session_mismatch=[]
    failures=[]
    for window,dates in periods.items():
        for day in dates:
            data=read_day(day) or {}
            if data.get("end_date")!=day or data.get("mode")!="daily":
                failures.append(day)
                continue
            by=data.get("channels") or {}
            total=by.get("Total") or {}
            kpis=data.get("kpis") or {}
            if any(number(total,k) is None or number(kpis,k) is None or
                   abs(number(total,k)-number(kpis,k))>(0.1 if k=="revenue" else 0.01)
                   for k in FIELDS):
                failures.append(day)
                continue
            explicit={name:row for name,row in by.items() if name!="Total"}
            if any(any(number(row,k) is None for k in FIELDS) for row in explicit.values()):
                failures.append(day)
                continue
            sums={k:sum(number(row,k) for row in explicit.values()) for k in FIELDS}
            if abs(sums["revenue"]-number(total,"revenue"))>0.1 or abs(sums["orders"]-number(total,"orders"))>0.01:
                failures.append(day)
                continue
            if abs(sums["sessions"]-number(total,"sessions"))>0.01:
                session_mismatch.append(day)
            for k in FIELDS:totals[window][k]+=number(total,k)
            for name,row in explicit.items():
                current_channel=channels[window].setdefault(name,dict.fromkeys(FIELDS,0.))
                for k in FIELDS:current_channel[k]+=number(row,k)
    quality=dict(missing_dates=sorted(set(failures)),daily_channel_session_mismatch_dates=session_mismatch,
                 channel_session_comparable=not session_mismatch,
                 scope="14 complete daily GA4 aggregates, channel revenue and orders only if reconciled")
    stub["quality"]=quality
    if failures:
        return stub
    for window in periods:
        expected=((weekly.get(window) or {}).get("total") or {})
        if any(number(expected,k) is None or abs(number(expected,k)-totals[window][k])>(0.1 if k=="revenue" else 0.01)
               for k in FIELDS):
            return {**stub,"status":"weekly_total_mismatch",
                    "quality":{**quality,"failed_window":window}}
    names=sorted(set(channels["previous"])|set(channels["current"]))
    rows=[]
    for name in names:
        p=channels["previous"].get(name) or dict.fromkeys(FIELDS,0.)
        c=channels["current"].get(name) or dict.fromkeys(FIELDS,0.)
        delta=c["revenue"]-p["revenue"]
        pct=(c["revenue"]/p["revenue"]-1)*100 if p["revenue"]>0 else None
        rows.append(dict(channel=name,previous_revenue=round(p["revenue"],2),
                         current_revenue=round(c["revenue"],2),delta_revenue=round(delta,2),
                         change_pct=round(pct,2) if pct is not None else None,
                         previous_orders=int(round(p["orders"])),current_orders=int(round(c["orders"]))))
    rows.sort(key=lambda x:(x["delta_revenue"],x["channel"]))
    net=totals["current"]["revenue"]-totals["previous"]["revenue"]
    if abs(sum(x["delta_revenue"] for x in rows)-net)>1.0:
        return {**stub,"status":"channel_unreconciled","quality":quality}
    # Abrupt disappearance of a material unclassified channel is a data-quality warning,
    # not proof that buyers or the related revenue disappeared.
    suspicious=[dict(channel=x["channel"],previous_revenue=x["previous_revenue"],
                share_of_previous_total_pct=round(100*x["previous_revenue"]/totals["previous"]["revenue"],1),
                issue="material_unclassified_channel_zero") for x in rows
                if x["channel"].lower() in ("etc","other","unassigned","(not set)") and
                   x["previous_revenue"]>=totals["previous"]["revenue"]*0.05 and x["current_revenue"]==0]
    return {**stub,"status":"reconciled_with_taxonomy_warning" if suspicious else "reconciled",
            "channels":rows,"net_revenue_delta":round(net,2),
            "sum_channel_delta":round(sum(x["delta_revenue"] for x in rows),2),
            "quality":{**quality,"taxonomy_warnings":suspicious,
                       "daily_count":14,
                       "revenue_reconciled":True,"orders_reconciled":True,
                       "session_reconciled_at_channel_grain":not session_mismatch},
            "note":"Channel attributed revenue shifts on the same 14 calendar dates as operations weekly; "
                   "unclassified-channel changes may reflect tagging/taxonomy changes. "
                   "Channel-level session CVR is suppressed if a daily session total fails reconciliation. "
                   "This is not proof of incremental media performance."}


def main():
    report=build(read(OPS),lambda day:read(DAILY/(day+".json")))
    OUT.parent.mkdir(parents=True,exist_ok=True)
    OUT.write_text(json.dumps(report,ensure_ascii=False,indent=2,allow_nan=False)+"\n",encoding="utf-8")
    print("[WEEKLY BUSINESS] status",report["status"],"channels",len(report["channels"]),
          "bad_session_dates",len(report["quality"].get("daily_channel_session_mismatch_dates",[])))


if __name__=="__main__":
    main()
