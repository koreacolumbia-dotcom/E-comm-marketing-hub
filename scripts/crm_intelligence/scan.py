#!/usr/bin/env python3
"""Read-only public CRM evidence scanner. No logins, forms, purchases or bypass."""
import asyncio, datetime as dt, json, re, sys
from pathlib import Path
from urllib.parse import urlsplit, urljoin, urlunsplit
import aiohttp
from bs4 import BeautifulSoup
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"competitor_intelligence"))
from reliability import PolicyCache, OriginGate
ROOTS={"TNF":"https://www.thenorthfacekorea.co.kr/","KOLON SPORT":"https://www.kolonmall.com/Brands/KOLONSPORT","DISCOVERY":"https://www.discovery-expedition.com/","SALOMON":"https://salomon.co.kr/","BLACKYAK":"https://www.byn.kr/blackyak","K2":"https://www.k-village.co.kr/k2","EIDER":"https://www.k-village.co.kr/eider","NEPA":"https://www.nplus.co.kr/main/main.asp","ARC'TERYX":"https://arcteryx.co.kr/","SNOW PEAK":"https://www.snowpeakstore.co.kr/","MILLET":"https://www.millet.co.kr/front/main","NATIONAL GEOGRAPHIC":"https://www.nstationmall.com/natgeo","COLUMBIA":"https://www.columbiakorea.co.kr/main/main.asp"}
STAGES=[
("welcome","가입·웰컴",r"sign_up|signup|registration|회원가입|신규\s*회원|웰컴\s*쿠폰|가입\s*쿠폰"),
("first_order","첫 구매",r"first.?purchase|first.?order|첫\s*구매|첫\s*주문"),
("browse","상품 탐색",r"view_item\b|viewProduct|product_view|view_content|ViewContent"),
("wishlist","찜·재입고",r"add_to_wishlist|back.?in.?stock|재입고\s*알림|관심상품|찜하기"),
("cart","장바구니 이탈",r"abandoned.?cart|cart.?abandon|장바구니\s*이탈|add_to_cart|AddToCart"),
("checkout","결제 이탈",r"abandoned.?checkout|checkout.?abandon|결제\s*이탈|begin_checkout|InitiateCheckout"),
("coupon","쿠폰 리마인드",r"coupon.?expir|쿠폰\s*(?:만료|소멸|유효기간|사용기한)|쿠폰\s*다운로드"),
("review","리뷰·구매 후",r"review.?request|post.?purchase|리뷰\s*(?:작성|적립)|구매\s*후|상품평\s*작성"),
("recommend","추천·교차 판매",r"cross.?sell|upsell|personalized.?recommend|개인화\s*(?:추천|상품)|추천\s*상품"),
("repeat","재구매",r"repeat.?purchase|repurchase|재구매"),
("winback","장기 미구매",r"win.?back|inactive.?customer|장기\s*미구매|휴면\s*회원"),
("loyalty","등급·생일·포인트",r"birthday|loyalty|생일\s*(?:쿠폰|혜택)|마일리지\s*(?:소멸|적립)|등급\s*혜택|포인트\s*소멸"),
("community","커뮤니티·앱",r"app.?push|앱\s*푸시|앱\s*설치|챌린지\s*참여|등산\s*인증")]
VENDORS={"Groobee":r"groobee|그루비","Datarize":r"datarize|데이터라이즈","IGAWorks":r"igaworks|adbrix|ad-brix|아이지에이웍스","CREMA":r"crema\.|crema_|크리마","Airbridge":r"airbridge|에어브릿지","Braze":r"braze|appboy","Klaviyo":r"klaviyo","Shopify":r"shopify","ChannelTalk":r"channel\.io|channelio|채널톡","GA4/GTM":r"googletagmanager|gtag\(|google-analytics","Meta Pixel":r"fbq\(|connect\.facebook\.net","Kakao":r"kakao(?:pixel|\.min\.js)|카카오\s*알림톡","Salesforce":r"exacttarget|salesforce|mc\.exct\.net","Criteo":r"criteo","Clarity":r"clarity\.ms|clarity\("}
EVENTS=r"view_item|add_to_cart|begin_checkout|add_to_wishlist|sign_up|view_content|AddToCart|InitiateCheckout|purchase"
LINKS=re.compile(r"privacy|policy|terms|membership|benefit|coupon|reward|loyalty|faq|notice|product(?:/|_detail)|goods/(?:view|detail)|회원혜택|멤버십|개인정보|쿠폰|마일리지|상품\s*상세",re.I)
DENY=re.compile(r"logout|delete|remove|unsubscribe|withdraw|checkout|/order|/cart|/login|/auth|admin|token|callback|api/|javascript:|mailto:",re.I)
UA="CSKMarketResearchBot"
MAX=2_000_000
OUT=Path("reports/crm_intelligence/scenario-data.json")
def clean(u):
    p=urlsplit(u)
    return urlunsplit((p.scheme,p.netloc,p.path or "/","",""))
def host(u): return (urlsplit(u).hostname or "").removeprefix("www.")
def same(root,u): 
    p=urlsplit(u)
    return p.scheme=="https" and host(u)==host(root) and not p.username and not p.password and not DENY.search(p.path)
def hits(text,patterns):
    return [key for key,pattern in patterns.items() if re.search(pattern,text,re.I)]
def classify(source,kind,text):
    rows=[]
    for k,label,p in STAGES:
        m=re.search(p,text,re.I)
        if m: rows.append({"scenario":k,"kind":kind,"source":clean(source),"signal":m.group(0)[:65],"status":"public_clue" if kind=="visible_text" else "technical_signal"})
    return rows
async def scan(brand,root,session,gate):
    policy=PolicyCache(session,UA,lambda u: same(root,u) or clean(u).endswith("/robots.txt") and host(u)==host(root))
    record={"brand":brand,"url":root,"pages":[],"scripts":[],"evidence":[],"vendors":[],"failures":[],"status":"not_observed"}
    async def get(url):
        if not same(root,url): return None,"out_of_scope"
        if not await policy.permitted(url): return None,policy.reason(url)
        async with gate.slot(url,policy.delay(url)) as go:
            if not go: return None,"origin_paused"
            try:
                async with session.get(url,allow_redirects=False) as resp:
                    if resp.status in (401,403,429):
                        gate.stop(url,str(resp.status))
                    if resp.status!=200:return None,"HTTP "+str(resp.status)
                    data=await resp.content.read(MAX+1)
                    if len(data)>MAX:return None,"size_limit"
                    return data.decode(resp.charset or "utf-8",errors="replace"),None
            except Exception as exc:return None,type(exc).__name__
    todo=[root];seen=set();scripts=set();vendors={}
    def add_vendors(s,u,kind):
        for v in hits(s,VENDORS):
            vendors.setdefault(v,{"name":v,"source":clean(u),"kind":kind})
    while todo and len(seen)<8:
        u=todo.pop(0)
        if u in seen:continue
        seen.add(u)
        raw,err=await get(u)
        if err:
            record["failures"].append({"source":clean(u),"reason":err});continue
        soup=BeautifulSoup(raw,"html.parser")
        visible=soup.get_text(" ",strip=True)
        if re.search(r"captcha|access denied|checking your browser|접근이 제한",visible[:400],re.I):
            record["failures"].append({"source":clean(u),"reason":"challenge_or_denied"});gate.stop(u,"challenge");continue
        record["pages"].append(clean(u))
        record["evidence"]+=classify(u,"visible_text",visible)
        add_vendors(visible,u,"public_text")
        for node in soup.find_all("script"):
            src=node.get("src")
            if src:
                s=urljoin(u,src)
                add_vendors(s,u,"script_reference")
                if same(root,s) and not urlsplit(s).query:scripts.add(s)
            else:
                inline=node.get_text()
                record["evidence"]+=classify(u,"inline_script",inline)
                add_vendors(inline,u,"inline_script")
        for a in soup.select("a[href]"):
            dest=urljoin(u,a.get("href"))
            if same(root,dest) and not urlsplit(dest).query and LINKS.search(dest+" "+a.get_text(" ",strip=True)) and dest not in seen and dest not in todo and len(todo)<70:todo.append(dest)
    for s in sorted(scripts)[:12]:
        raw,err=await get(s)
        if err:
            record["failures"].append({"source":clean(s),"reason":err});continue
        record["scripts"].append(clean(s))
        record["evidence"]+=classify(s,"external_script",raw)
        add_vendors(raw,s,"external_script")
    record["vendors"]=list(vendors.values())
    unique={(e["scenario"],e["kind"],e["source"],e["signal"]):e for e in record["evidence"]}
    record["evidence"]=list(unique.values())[:140]
    record["status"]="partial" if record["pages"] and record["failures"] else "scanned" if record["pages"] else "blocked_or_unavailable"
    return record
async def main():
    gate=OriginGate()
    async with aiohttp.ClientSession(headers={"User-Agent":UA},timeout=aiohttp.ClientTimeout(total=20)) as session:
        sem=asyncio.Semaphore(4)
        async def task(b,u):
            async with sem:
                try:return await asyncio.wait_for(scan(b,u,session,gate),timeout=250)
                except Exception as e:return {"brand":b,"url":u,"status":"error","pages":[],"scripts":[],"vendors":[],"evidence":[],"failures":[{"source":u,"reason":type(e).__name__}]}
        rows=await asyncio.gather(*(task(b,u) for b,u in ROOTS.items()))
    data={"schema":1,"generated_at":dt.datetime.now(dt.timezone.utc).isoformat(),"scope":"KR public HTML and first-party script; no runtime send confirmation","stages":[{"id":k,"name":n} for k,n,_ in STAGES],"brands":rows}
    OUT.parent.mkdir(parents=True,exist_ok=True)
    tmp=OUT.with_suffix(".tmp");tmp.write_text(json.dumps(data,ensure_ascii=False,indent=2));tmp.replace(OUT)
    print(json.dumps({"brands":len(rows),"pages":sum(len(b["pages"]) for b in rows),"scripts":sum(len(b["scripts"]) for b in rows),"failures":sum(len(b["failures"]) for b in rows)},ensure_ascii=False))
def test():
    assert not same("https://example.com/","https://example.com/checkout")
    assert not same("https://example.com/","https://other.example.com/")
    assert clean("https://example.com/a?token=secret")=="https://example.com/a"
    a=classify("https://example.com/","inline_script","add_to_cart")
    assert a[0]["scenario"]=="cart" and a[0]["status"]=="technical_signal"
    assert hits("https://static.datarize.com/a.js",VENDORS)==["Datarize"]
    print("CRM evidence tests passed")
if __name__=="__main__":
    if "--self-test" in sys.argv:test()
    else:asyncio.run(main())
