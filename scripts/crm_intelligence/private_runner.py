#!/usr/bin/env python3
"""Private, consented CRM journey runner; no checkout, payment or signup automation."""
import argparse,datetime as dt,json,re
from pathlib import Path
from urllib.parse import urlsplit
from private_observer import BRANDS,private_path,append,records,date,now,cohort,safe
PHASES=("baseline","browse","wishlist","cart","inactivity")
WINDOW=7*24*3600
LABELS={"wishlist":re.compile(r"^(찜하기|관심\s*상품\s*(?:추가|등록)|위시리스트\s*추가|add to wishlist)$",re.I),
"cart":re.compile(r"^(장바구니(?:에)?\s*(?:담기|추가)|장바구니|add to (?:cart|bag))$",re.I)}
def plan(history, brand, c, t):
    relevant=[x for x in history if x["brand"]==brand and x["cohort"]==c and x["action"] in PHASES]
    if not relevant:return "baseline"
    latest=max(relevant,key=lambda e:date(e["at"]))
    if (t-date(latest["at"])).total_seconds()<WINDOW:return None
    i=PHASES.index(latest["action"])
    return PHASES[i+1] if i+1<len(PHASES) else None
def profile(p,b,c):return p/"profiles"/b.replace(" ","_").replace("'","")/cohort(c)
def one_visible(loc):
    return loc.count()==1 and loc.is_visible()
def check_url(b,u):
    if not safe(b,u) or re.search(r"(?:/|[?&])(?:payment|checkout|order|logout|withdraw|admin|api|token|auth|delete)(?:/|[?&=]|$)",u,re.I):
        raise ValueError("Out of approved scope")
def blocked(page):
    return bool(re.search(r"captcha|access denied|접근이 제한|자동입력 방지|비정상적인 접근",page.title(),re.I))
def single_click(page,b,action,cfg):
    # Must be explicitly configured on a real product page and verified in a pilot.
    name=cfg.get(action+"_label","")
    if not LABELS[action].fullmatch(name):raise ValueError("Missing approved action label")
    target=page.get_by_role("button",name=name,exact=True)
    if not one_visible(target) or not target.is_enabled():raise ValueError("Button absent or ambiguous")
    # Explicit post-action evidence; no automatic retries after an attempted mutation.
    proof=cfg.get(action+"_proof","")
    if not proof:raise ValueError("Missing post-action proof text")
    locator=page.get_by_text(proof,exact=True)
    if one_visible(locator):raise ValueError("Proof already visible; stop to prevent duplicate action")
    check_url(b,page.url)
    target.click(timeout=5000,no_wait_after=True)
    locator.wait_for(state="visible",timeout=8000)
    if not one_visible(locator):raise ValueError("Action not independently verified")
def execute(p,b,c,cfg,force_setup=False):
    if not cfg.get("authorized"):return "not_authorized"
    if cfg.get("cohort","main")!=c:return "cohort_mismatch"
    u=cfg.get("entry_url" if force_setup else "product_url","")
    if not u:return "missing_url"
    check_url(b,u)
    from playwright.sync_api import sync_playwright
    with sync_playwright() as w:
        ctx=w.chromium.launch_persistent_context(str(profile(p,b,c)),headless=not force_setup)
        try:
            page=ctx.pages[0] if ctx.pages else ctx.new_page()
            if not force_setup:
                # No automatic top-level navigation outside approved brand scope.
                def route(r):
                    if r.request.is_navigation_request() and r.request.frame==page.main_frame:
                        try:check_url(b,r.request.url)
                        except Exception:r.abort();return
                    r.continue_()
                ctx.route("**/*",route)
            resp=page.goto(u,wait_until="domcontentloaded",timeout=30000)
            if force_setup:
                input("Complete your own login and verification, then press Enter. Do not enter passwords here. ")
                return "profile_saved"
            if resp is None or resp.status>=400 or blocked(page):return "access_pause"
            check_url(b,page.url)
            marker=cfg.get("login_proof","")
            if not marker or not one_visible(page.get_by_text(marker,exact=True)):return "login_required_or_unverified"
            product=cfg.get("product_proof","")
            if not product or not one_visible(page.get_by_text(product,exact=True)):return "product_unverified"
            action=plan(records(p,"actions.jsonl"),b,c,dt.datetime.now(dt.timezone.utc))
            if action=="browse":
                page.wait_for_timeout(5000)
            elif action in ("wishlist","cart"):
                if not cfg.get(action+"_approved"):return "pilot_approval_required"
                # record attempt before click; uncertain outcome is never retried automatically.
                attempts=records(p,"attempts.jsonl")
                if any(x["brand"]==b and x["cohort"]==c and x["action"]==action for x in attempts):return "previous_attempt_review"
                append(p,"attempts.jsonl",{"brand":b,"cohort":c,"action":action,"at":now()})
                single_click(page,b,action,cfg)
            else:return "no_browser_action_due"
            append(p,"actions.jsonl",{"brand":b,"cohort":c,"action":action,"at":now(),"evidence":"verified_page_or_ui_confirmation"})
            return "observed_"+action
        finally:ctx.close()
def step(p,b,cfg,setup=False):
    if b not in BRANDS or not cfg.get("authorized"):return "authorization_required"
    c=cohort(cfg.get("cohort","main"))
    if setup:return execute(p,b,c,cfg,True)
    a=plan(records(p,"actions.jsonl"),b,c,dt.datetime.now(dt.timezone.utc))
    if a is None:return "observation_window"
    if a in ("baseline","inactivity"):
        append(p,"actions.jsonl",{"brand":b,"cohort":c,"action":a,"at":now(),"evidence":"observation_window_started"})
        return a+"_started"
    return execute(p,b,c,cfg)
def test():
    t=date("2026-11-19T00:00:00+09:00")
    assert plan([],"TNF","main",t)=="baseline"
    e=[{"brand":"TNF","cohort":"main","action":"baseline","at":t.isoformat()}]
    assert plan(e,"TNF","main",t+dt.timedelta(days=6)) is None
    assert plan(e,"TNF","main",t+dt.timedelta(days=7))=="browse"
    assert not LABELS["cart"].fullmatch("바로 구매")
    print("runner planner tests passed")
def main():
    a=argparse.ArgumentParser();a.add_argument("--private-dir",default=str(Path.home()/".csk-crm-private"));a.add_argument("cmd",choices=["init","setup","run","self-test"]);a.add_argument("--brand",choices=list(BRANDS));x=a.parse_args()
    if x.cmd=="self-test":test();return
    p=private_path(x.private_dir);conf=p/"runner-config.json"
    if x.cmd=="init":
        if not conf.exists():conf.write_text(json.dumps({b:{"authorized":False,"cohort":"main","entry_url":"https://"+domain+"/","product_url":"","login_proof":"","product_proof":"","wishlist_approved":False,"wishlist_label":"","wishlist_proof":"","cart_approved":False,"cart_label":"","cart_proof":""} for b,domain in BRANDS.items()},ensure_ascii=False,indent=2))
        print(conf);return
    config=json.loads(conf.read_text())
    if x.cmd=="setup" and not x.brand:raise ValueError("--brand required")
    for b,cfg in config.items():
        if x.brand and x.brand!=b:continue
        try:status=step(p,b,cfg,x.cmd=="setup")
        except Exception as exc:status="paused_"+type(exc).__name__
        append(p,"runner-status.jsonl",{"brand":b,"status":status,"at":now()})
        print(b,status)
if __name__=="__main__":main()
