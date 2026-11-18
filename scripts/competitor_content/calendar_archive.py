"""Compact 180-day calendar archive; dates of observation and publication stay separate."""
import json
from pathlib import Path
from datetime import datetime,timedelta
ROOT=Path("reports/competitor_content")
def compact(x):
    return {k:x.get(k) for k in ("id","brand","title","summary","url","source_url","source_scope","placement","type","type_tags","tpo","selling_points","thumbnail","first_seen_at","last_seen_at","published_at","publication_status","publication_evidence","change","fingerprint","products","influencer","cta","images","page_metrics","evidence_scope","visual_palette")}
def build(payload,earlier=None):
    checked=payload["checked_at_kst"]; cutoff=(datetime.fromisoformat(checked)-timedelta(days=180)).date().isoformat()
    earlier=earlier or {};items={x["id"]:x for x in earlier.get("items",[]) if x.get("id")}
    for x in [*(payload.get("history") or []),*(payload.get("items") or [])]:
        if not x.get("id"):continue
        old=items.get(x["id"],{});new=compact(x)
        if old.get("first_seen_at") and (not new.get("first_seen_at") or old["first_seen_at"]<new["first_seen_at"]):new["first_seen_at"]=old["first_seen_at"]
        new["changed_at"]=old.get("changed_at")
        if x.get("change")=="changed" and x.get("last_seen_at")==checked:new["changed_at"]=checked
        if not new["changed_at"] and x.get("change")=="changed" and x.get("last_seen_at"):new["changed_at"]=x["last_seen_at"]
        items[x["id"]]=new
    items={k:v for k,v in items.items() if max((v.get("last_seen_at") or "")[:10],(v.get("first_seen_at") or "")[:10],v.get("published_at") or "")>=cutoff}
    runs={r["checked_at_kst"]:r for r in earlier.get("runs",[]) if r.get("checked_at_kst","")[:10]>=cutoff}
    runs[checked]={"checked_at_kst":checked,"target":payload.get("coverage",{}).get("target"),"accessible":payload.get("coverage",{}).get("accessible"),"brands":[{"brand":b["brand"],"pages":b.get("pages_checked",0),"status":b.get("status")} for b in payload.get("brands",[])]}
    snapshots={r["date"]:r for r in earlier.get("banner_snapshots",[]) if r.get("date","")>=cutoff}
    today=checked[:10]
    current={"date":today,"checked_at_kst":checked,"brands":[]}
    old_brands={b["brand"]:b for b in snapshots.get(today,{}).get("brands",[])}
    for b in payload.get("brands",[]):
        rows=[compact(x) for x in payload.get("items",[]) if x.get("brand")==b["brand"] and x.get("placement")=="main_visual"]
        if rows: current["brands"].append({"brand":b["brand"],"pages_checked":b.get("pages_checked",0),"status":b.get("status"),"items":rows})
        elif b["brand"] in old_brands: current["brands"].append(old_brands[b["brand"]])
    snapshots[today]=current
    return {"schema_version":2,"checked_at_kst":checked,"window_days":180,"items":list(items.values()),"runs":list(runs.values()),"banner_snapshots":list(snapshots.values())}
def main():
    p=ROOT/"latest.json";dest=ROOT/"calendar.json";payload=json.loads(p.read_text())
    earlier=json.loads(dest.read_text()) if dest.exists() else {}
    dest.write_text(json.dumps(build(payload,earlier),ensure_ascii=False,separators=(",",":"))+"\n")
if __name__=="__main__":main()
