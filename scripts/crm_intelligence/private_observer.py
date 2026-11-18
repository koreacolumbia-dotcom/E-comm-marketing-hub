#!/usr/bin/env python3
"""Opt-in local CRM journey collection. Never upload account or message data."""
import argparse,csv,datetime as dt,json,re,sys
from pathlib import Path
from urllib.parse import urlsplit
ROOT=Path(__file__).resolve().parents[2]
BRANDS={"TNF":"thenorthfacekorea.co.kr","KOLON SPORT":"kolonmall.com","DISCOVERY":"discovery-expedition.com","SALOMON":"salomon.co.kr","BLACKYAK":"byn.kr","K2":"k-village.co.kr","EIDER":"k-village.co.kr","NEPA":"nplus.co.kr","ARC'TERYX":"arcteryx.co.kr","SNOW PEAK":"snowpeakstore.co.kr","MILLET":"millet.co.kr","NATIONAL GEOGRAPHIC":"nstationmall.com","COLUMBIA":"columbiakorea.co.kr"}
ACTIONS=["signup_complete","baseline","browse","wishlist","cart","coupon_nonuse","inactivity","checkout_view_manual"]
CHANNELS=["email","sms","kakao","app_push","onsite"]
HOURS=[1,3,24,72,168,336,720,1440,2160]
def now():return dt.datetime.now(dt.timezone.utc).isoformat()
def date(x):
    v=dt.datetime.fromisoformat(x.replace("Z","+00:00"))
    if v.tzinfo is None:raise ValueError("Timezone required")
    return v
def private_path(path):
    p=Path(path).expanduser().resolve()
    if p==ROOT or ROOT in p.parents:raise ValueError("Private directory MUST be outside the git repository.")
    p.mkdir(parents=True,exist_ok=True);return p
def append(folder,name,record):
    with (folder/name).open("a",encoding="utf-8") as f:f.write(json.dumps(record,ensure_ascii=False)+"\n")
def records(folder,name):
    p=folder/name
    return [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines() if l.strip()] if p.exists() else []
def safe(brand,url):
    p=urlsplit(url);host=(p.hostname or "").lower().removeprefix("www.")
    return p.scheme=="https" and host==BRANDS[brand] and not p.username and not p.password and not re.search(r"/(?:logout|delete|withdraw|order|checkout|payment|admin|api)(?:/|$)",p.path,re.I)
def cohort(x):
    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,40}",x):raise ValueError("Use a non-identifying cohort code (e.g. cart_a)")
    return x
def add_event(args,p):
    append(p,"actions.jsonl",{"brand":args.brand,"cohort":cohort(args.cohort),"action":args.action,"at":args.at or now()})
def observe(args,p):
    if not args.authorized:raise ValueError("Confirm you own the account and site permits the planned use (--authorized).")
    if not args.url or not safe(args.brand,args.url):raise ValueError("Use an official HTTPS entry URL; no order/payment/admin endpoints.")
    from playwright.sync_api import sync_playwright
    profile=p/"profiles"/args.brand.replace(" ","_").replace("'","")/cohort(args.cohort)
    with sync_playwright() as w:
        ctx=w.chromium.launch_persistent_context(str(profile),headless=False)
        try:
            page=ctx.pages[0] if ctx.pages else ctx.new_page()
            page.goto(args.url,wait_until="domcontentloaded",timeout=30000)
            print("Use your own account. Complete authentication manually. No purchase/order submission. Confirm the observed action in terminal.")
            if args.action=="browse" and args.automate_browse:
                # Single supplied product URL, no scripted clicks, no forms, no bypass.
                print("One public page navigation performed; verify product page before marking action.")
            confirmed=input("Type DONE after the actual intended action, or CANCEL: ").strip()
            if confirmed=="DONE":add_event(args,p)
        finally:ctx.close()
def ingest(args,p):
    # Caller exports only their own received messages to a private CSV.
    # Required: brand,cohort,channel,received_at,subject. No addresses or message bodies.
    with Path(args.csv).expanduser().open(encoding="utf-8-sig",newline="") as f:
        for row in csv.DictReader(f):
            if row["brand"] not in BRANDS or row["channel"] not in CHANNELS:continue
            date(row["received_at"]);append(p,"messages.jsonl",{"brand":row["brand"],"cohort":cohort(row["cohort"]),"channel":row["channel"],"at":row["received_at"],"subject":row["subject"][:300]})
def correlate(events,messages):
    rows=[]
    for m in messages:
        t=date(m["at"]);matched=[e for e in events if e["brand"]==m["brand"] and e["cohort"]==m["cohort"] and 0<=(t-date(e["at"])).total_seconds()<=2160*3600]
        matched.sort(key=lambda e:date(e["at"]),reverse=True)
        baseline=[x for x in messages if x["brand"]==m["brand"] and x["cohort"]!=m["cohort"] and x["subject"].strip().casefold()==m["subject"].strip().casefold() and abs((date(x["at"])-t).total_seconds())<=3600*24 and any(e["brand"]==x["brand"] and e["cohort"]==x["cohort"] and e["action"]=="baseline" for e in events)]
        rows.append({"message":m,"candidate_actions":[{"action":e["action"],"hours":round((t-date(e["at"])).total_seconds()/3600,2)} for e in matched],"status":"general_promotion_candidate" if baseline else "association_only" if matched else "unattributed","control_match":bool(baseline)})
    return rows
def report(p):
    events=records(p,"actions.jsonl");messages=records(p,"messages.jsonl")
    pth=p/"private-correlation.json";pth.write_text(json.dumps(correlate(events,messages),ensure_ascii=False,indent=2),encoding="utf-8")
    print("Private correlation saved:",pth)
    for e in events:
        due=[h for h in HOURS if (dt.datetime.now(dt.timezone.utc)-date(e["at"])).total_seconds()<h*3600]
        print(e["brand"],e["cohort"],e["action"],"next observation hours:",due[0] if due else "complete")
def test():
    e=[{"brand":"TNF","cohort":"cart_a","action":"cart","at":"2026-11-19T00:00:00+09:00"}]
    m=[{"brand":"TNF","cohort":"cart_a","channel":"email","at":"2026-11-19T03:00:00+09:00","subject":"test"}]
    assert correlate(e,m)[0]["candidate_actions"][0]["hours"]==3
    assert not safe("TNF","https://www.thenorthfacekorea.co.kr/payment")
    assert not safe("TNF","https://example.com/")
    print("Private observer self-test passed")
def main():
    a=argparse.ArgumentParser();a.add_argument("--private-dir",default=str(Path.home()/".csk-crm-private"));s=a.add_subparsers(dest="cmd",required=True)
    for name in ["observe","event"]:
        q=s.add_parser(name);q.add_argument("--brand",choices=BRANDS,required=True);q.add_argument("--cohort",required=True);q.add_argument("--action",choices=ACTIONS,required=True);q.add_argument("--at")
        if name=="observe":q.add_argument("--url",required=True);q.add_argument("--authorized",action="store_true");q.add_argument("--automate-browse",action="store_true")
    q=s.add_parser("ingest");q.add_argument("--csv",required=True);s.add_parser("report");s.add_parser("self-test")
    x=a.parse_args()
    if x.cmd=="self-test":test();return
    p=private_path(x.private_dir)
    if x.cmd=="event":
        if x.at:date(x.at)
        add_event(x,p)
    elif x.cmd=="observe":observe(x,p)
    elif x.cmd=="ingest":ingest(x,p)
    elif x.cmd=="report":report(p)
if __name__=="__main__":main()
