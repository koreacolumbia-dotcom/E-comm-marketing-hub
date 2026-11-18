import asyncio, json, re, hashlib, os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urljoin,urlparse
from adapters import PROFILES,extract_cards
from market_signals import PRIVATE, public_topic, extract_market_signals
from reliability import PolicyCache, OriginGate, navigate, checkpoint
from urllib.robotparser import RobotFileParser
import aiohttp
from playwright.async_api import async_playwright
BRANDS={"COLUMBIA":"https://www.columbiakorea.co.kr/main/main.asp","TNF":"https://www.thenorthfacekorea.co.kr/","K2":"https://www.k2.co.kr/","DISCOVERY":"https://www.discovery-expedition.com/","BLACKYAK":"https://www.byn.kr/blackyak","NEPA":"https://www.nplus.co.kr/main/main.asp","KOLON SPORT":"https://www.kolonmall.com/Brands/KOLONSPORT","EIDER":"https://www.eider.co.kr/","NATIONAL GEOGRAPHIC":"https://www.naturestore.co.kr/","SNOW PEAK":"https://www.snowpeakstore.co.kr/","MILLET":"https://www.millet.co.kr/","SALOMON":"https://salomon.co.kr/"}
UA="CSKMarketResearchBot"
FAST=os.getenv('CRAWLER_MODE','fast')=='fast'
PAGE_LIMIT=16 if FAST else 36
BRAND_SECONDS=210 if FAST else 480
EVENT=re.compile(r'promo|collection|newevent|event|promotion|exhibition|/event/|/product/list|_exh|exh_list|special|企画|企画|기획전|이벤트|프로모션|세일|페스타|블랙프라이데이|혜택',re.I)
DEAL=re.compile(r'할인|쿠폰|사은품|적립|특가|black friday|sale|%\s*off',re.I)
PRODUCT=re.compile(r'goods_view|product/detail|productdetail|/product-detail/|/products?/|/goods/|goodsNo=|productNo=|/shop/view',re.I)
DATE=re.compile(r'20\d{2}[.\-/ ]\s*\d{1,2}[.\-/ ]\s*\d{1,2}|\d{1,2}[./]\d{1,2}\s*[~–-]\s*\d{1,2}[./]\d{1,2}')
SEEDS={'BLACKYAK':['https://www.byn.kr/_container/exh_list.php?hta_brand=blackyak&st=1','https://m.byn.kr/blackyak/_exh/'],'NEPA':['https://www.nplus.co.kr/promotion/list.asp','https://www.nplus.co.kr/product/list.asp','https://www.nplus.co.kr/promotion/view.asp?EventNo=856'],'KOLON SPORT':['https://www.kolonmall.com/Special/259472']}
MARKET_SEEDS={
    "SALOMON":["https://salomon.co.kr/pages/membership","https://salomon.co.kr/pages/faq","https://salomon.co.kr/pages/notice-1","https://salomon.co.kr/pages/event-page-v2","https://salomon.co.kr/collections/launch-calendar","https://salomon.co.kr/collections/raffle-event"],
    "COLUMBIA":["https://www.columbiakorea.co.kr/event/261101/new_members.asp","https://www.columbiakorea.co.kr/inside/benefit.asp","https://www.columbiakorea.co.kr/about/notice.asp","https://www.columbiakorea.co.kr/event/list.asp"],
    "TNF":["https://image.thenorthfacekorea.co.kr/cscenter","https://image.thenorthfacekorea.co.kr/cscenter/faq"],
    "K2":["https://www.k-village.co.kr/customer/grade"], "EIDER":["https://www.k-village.co.kr/customer/grade"],
    "DISCOVERY":["https://www.discovery-expedition.com/display/promotions/event/641","https://www.discovery-expedition.com/register"],
    "BLACKYAK":["https://m.byn.kr/board/?db=basic_2"],
    "NEPA":["https://www.nplus.co.kr/customer/membership.asp","https://www.nplus.co.kr/promotion/view.asp?EventNo=241"],
    "KOLON SPORT":["https://www.kolonsport.com/OLOMembership","https://www.kolonsport.com/Event","https://www.kolonmall.com/Brands/4894/Event/ThisMonth"],
    "NATIONAL GEOGRAPHIC":["https://www.nstationmall.com/event/planning/356","https://www.nstationmall.com/cs-center/grade-benefit-info"],
    "SNOW PEAK":["https://www.snowpeakstore.co.kr/member/join_agreement.php","https://m.snowpeakstore.co.kr/service/guide.php"],
    "MILLET":["https://www.millet.co.kr/front/product/29587"],
}
def host(u): return (urlparse(u).hostname or '').removeprefix('www.').removeprefix('m.')
async def scan(browser,session,name,root,gate):
    result={'brand':name,'root':root,'status':'unverified','pages':[],'errors':[]}
    allowed=host(root)
    profile=PROFILES[name]
    market_seeds=MARKET_SEEDS.get(name,[])
    seeds=list(dict.fromkeys(market_seeds+SEEDS.get(name,[])+profile['seeds']))
    market_hosts={host(u) for u in market_seeds}
    def same(u): return host(u)==allowed or host(u) in market_hosts or (urlparse(u).hostname or '') in profile.get('hosts',[])
    def brand_route(u):
        p=urlparse(u)
        if PRIVATE.search(p.path): return False
        if u in market_seeds: return True
        if name=="SALOMON":
            return p.path=="/" or p.path.startswith(("/collections/","/products/","/pages/","/blogs/")) or p.path=="/shop"
        if name=="COLUMBIA":
            # Anonymous, public shopping pages only; never account, cart, order or admin routes.
            return p.path.lower().startswith(("/main/","/event/","/product/list","/product/view","/promotion/","/exhibition/"))
        if host(u) == "k-village.co.kr" and name in ("K2","EIDER"):
            # Multi-brand storefront: only named brand routes and curated shared events.
            prefix = "/k2" if name == "K2" else "/eider"
            return p.path.lower().rstrip("/") == prefix or p.path.lower().startswith(prefix+"/") or u in seeds
        if host(u) == "nstationmall.com" and name == "NATIONAL GEOGRAPHIC":
            # Only verified National Geographic entrypoints; avoid unrelated group brands.
            return u in seeds
        if name=='KOLON SPORT':
            return p.path.lower().startswith('/brands/kolonsport') or u in seeds
        if name=='BLACKYAK':
            return p.path.lower().startswith('/blackyak') or u in seeds
        return True
    context=None
    deadline=asyncio.get_running_loop().time()+BRAND_SECONDS
    try:
        policy=PolicyCache(session,UA,same)
        context=await browser.new_context(user_agent=UA,locale='ko-KR')
        async def route_handler(route):
            u=route.request.url
            if route.request.resource_type=='document' and (not same(u) or not brand_route(u) or gate.key(u) in gate.stopped or not await policy.permitted(u)): await route.abort()
            else: await route.continue_()
        await context.route('**/*',route_handler)
        page=await context.new_page()
        queue=list(dict.fromkeys([root]+seeds));seen=set();blocked_origins=set()
        def origin(u):
            p=urlparse(u);return (p.scheme,p.netloc)
        def priority(u):
            # Favor product/list evidence after the first promotion or root page.
            p=urlparse(u);s=p.path+'?'+p.query
            listing=bool(re.search(r'category|product/list|goods_list|big_section|/list/|collection|search|/best|/new',s,re.I))
            product=bool(PRODUCT.search(u)) and not listing
            # Balance product and event evidence before auxiliary membership/FAQ discovery.
            event=bool(re.search(r'promo|event|exhibition|special|_exh',s,re.I))
            if not result['pages']: return (0 if u==root else 1 if event or product or listing else 2,0)
            has_price=any(p.get('product_cards') or p.get('structured_data') for p in result['pages'])
            has_event=any(p.get('date_mentions') and EVENT.search(p.get('url','')+' '+p.get('title','')) for p in result['pages'])
            if (product or listing) and not has_price: rank=0
            elif event and not has_event: rank=1
            elif product or listing or event: rank=2
            elif public_topic(u): rank=3
            else: rank=4
            return (rank,0 if u in seeds else 1)
        queue=sorted(queue,key=priority)
        while queue and len(seen)<PAGE_LIMIT and asyncio.get_running_loop().time()<deadline:
            u=queue.pop(0)
            if u in seen or origin(u) in blocked_origins or gate.key(u) in gate.stopped or not same(u) or not brand_route(u): continue
            seen.add(u)
            if not await policy.permitted(u):
                result['errors'].append({'url':u,'error':policy.reason(u)}); continue
            try:
                async with gate.slot(u,policy.delay(u)) as permitted:
                    if not permitted:
                        result['errors'].append({'url':u,'error':'shared origin suspended: '+gate.stopped.get(gate.key(u),'')})
                        continue
                    response=await navigate(page,u)
                    if response and response.status in (403,429):
                        reason='HTTP '+str(response.status)+'; stop requests to origin for this run'
                        gate.stop(u,reason)
                        if response.url.startswith('https://'): gate.stop(response.url,reason)
                        blocked_origins.add(origin(u))
                        blocked_origins.add(origin(page.url))
                        raise RuntimeError(reason)
                if not response or response.status>=400 or not same(page.url) or not brand_route(page.url): raise RuntimeError('navigation failed HTTP '+str(response.status if response else 'none'))
                await page.wait_for_timeout(1000)
                await page.evaluate('window.scrollTo(0, Math.min(document.body?.scrollHeight ?? document.documentElement?.scrollHeight ?? 0, 1400))')
                await page.wait_for_timeout(700)
                if re.search(r'category|product/list|goods_list|big_section|/list/|collection|search',page.url,re.I):
                    # Allow ordinary lazy-loaded product cards to appear through bounded scrolling.
                    for _ in range(2 if FAST else 4):
                        await page.evaluate('window.scrollBy(0, Math.max(window.innerHeight, 600))')
                        await page.wait_for_timeout(500)
                body=await page.locator('body').inner_text(timeout=8000)
                if re.search(r'access denied|captcha|접근이 제한',body[:1000],re.I):
                    gate.stop(page.url,'explicit access restriction')
                    raise RuntimeError('explicit access restriction; origin suspended')
                if len(body.strip())<100: raise RuntimeError('empty document')
                structured=await page.locator('script[type="application/ld+json"]').all_text_contents()
                images=await page.locator('img').evaluate_all("els=>els.slice(0,80).map(e=>({src:e.currentSrc||e.src,alt:e.alt}))")
                links=await page.locator('a[href], img[alt]').evaluate_all("""els=>els.map(e=>({url:e.href||e.closest('a')?.href||'',label:[e.innerText,e.alt,e.title,e.querySelector('img')?.alt].filter(Boolean).join(' ')}))""")
                signals=extract_market_signals(body,[x for x in links if same(x['url']) and brand_route(x['url'])],page.url)
                cards=await extract_cards(page,profile)
                if host(page.url)=='k-village.co.kr' and name in ('K2','EIDER'):
                    # Shared mall results must explicitly identify the target brand.
                    cards=[r for r in cards if re.match(r'^'+name+r'\s+',r['name'],re.I) or re.match(r'^'+name+r'\s+',r.get('evidence',''),re.I)]
                    structured=[]
                if host(page.url)=='nstationmall.com' and urlparse(page.url).path.rstrip('/')=='/event/planning':
                    cards=[];structured=[]  # Multi-brand event index only.
                hero_assets=[]
                if name=="COLUMBIA":
                    # Use visible banner images with public event destinations only.
                    hero_assets=await page.locator("a[href] img").evaluate_all("""els=>els.map(e=>{const a=e.closest('a');const r=e.getBoundingClientRect();return {image:e.currentSrc||e.src,url:a?.href||'',title:(e.alt||a?.title||a?.innerText||'').trim().slice(0,160),width:e.naturalWidth,height:e.naturalHeight,visible:r.width>=180&&r.height>=70}}).filter(x=>x.visible&&x.width>=400&&x.height>=120&&/(?:\\/event\\/|\\/promotion\\/|\\/exhibition\\/)/i.test(x.url)).slice(0,24)""")
                    hero_assets=[h for h in hero_assets if same(h["url"]) and h["url"].startswith("https://") and h["image"].startswith("https://")]
                    for asset in hero_assets[:8]:
                        if not asset.get("title"):
                            asset["title"]="공식 이벤트 배너"
                        # Preserve rendered public assets locally rather than claiming that a URL is an archived image.
                        try:
                            if not same(asset["url"]) or not brand_route(asset["url"]): continue
                            key=hashlib.sha256((asset["image"]+"|"+asset["url"]).encode()).hexdigest()[:20]
                            target=Path("reports/competitor_intelligence/owned/assets")/(key+".png")
                            target.parent.mkdir(parents=True,exist_ok=True)
                            if not target.exists():
                                indexed=page.locator("a[href] img")
                                for ix in range(min(await indexed.count(),120)):
                                    el=indexed.nth(ix)
                                    src=await el.evaluate("(e)=>e.currentSrc||e.src",timeout=1200)
                                    if src==asset["image"] and await el.is_visible():
                                        await el.screenshot(path=str(target),timeout=3500,animations="disabled")
                                        break
                            if target.exists() and target.stat().st_size>0: asset["local_image"]=target.name
                        except Exception:
                            pass
                snippets=[x.strip()[:350] for x in body.splitlines() if DEAL.search(x) or DATE.search(x)][:60]
                banners=[x for x in links if EVENT.search(x['label']) or DEAL.search(x['label'])][:25]
                result['pages'].append({'url':page.url,'title':await page.title(),'product_cards':cards,'structured_data':structured[:15],'images':images,'hero_assets':hero_assets,'market_signals':signals,'evidence':snippets,'banner_labels':banners,'date_mentions':DATE.findall(body)[:30],'percent_mentions':re.findall(r'\d{1,3}\s*%',body)[:40],'price_mentions':re.findall(r'[\d,]+\s*원',body)[:40],'fingerprint':hashlib.sha256(json.dumps([snippets,banners],ensure_ascii=False).encode()).hexdigest()})
                checkpoint(name,result)
                candidates=[x['url'].split('#')[0] for x in links if x['url'].startswith('https://') and same(x['url']) and brand_route(x['url']) and (EVENT.search(x['url']+' '+x['label']) or PRODUCT.search(x['url']) or re.search(r'category|big_section|/list/|goods_list',x['url'],re.I) or public_topic(x['url'],x['label']))]
                queue=list(dict.fromkeys(queue+[x for x in candidates if x not in seen]))
                queue=sorted(queue,key=priority)[:120]
            except Exception as e: result['errors'].append({'url':u,'error':str(e)[:300]})
            await asyncio.sleep(2)
        result['status']='partial' if result['pages'] else 'unverified'
        result['promotion_evidence_pages']=sum(bool(p['evidence']) and bool(EVENT.search(p['url']+' '+p['title'])) for p in result['pages'])
    except Exception as e: result['errors'].append({'url':root,'error':str(e)[:300]})
    finally:
        if context:
            try: await context.close()
            except Exception: pass
    return result
async def main():
    import shutil
    shutil.rmtree('competitor-output/checkpoints',ignore_errors=True)
    now=datetime.now(timezone(timedelta(hours=9)))
    async with aiohttp.ClientSession(headers={'User-Agent':UA},timeout=aiohttp.ClientTimeout(total=20)) as session:
        async with async_playwright() as p:
            browser=await p.chromium.launch(headless=True, **({'executable_path':os.environ['CRAWLER_CHROME']} if os.environ.get('CRAWLER_CHROME') else {}))
            limit=asyncio.Semaphore(5 if FAST else 3)
            gate=OriginGate()
            async def bounded(name,root):
                async with limit:
                    try: return await asyncio.wait_for(scan(browser,session,name,root,gate),timeout=BRAND_SECONDS+40)
                    except Exception as e:
                        cp=Path('competitor-output/checkpoints')/(name+'.json')
                        row=json.loads(cp.read_text()) if cp.exists() else {'brand':name,'root':root,'status':'unverified','pages':[],'errors':[]}
                        row['errors'].append({'error':str(e) or type(e).__name__});return row
            rows=[]
            tasks=[asyncio.create_task(bounded(name,root)) for name,root in BRANDS.items()]
            for task in asyncio.as_completed(tasks):
                row=await task
                rows.append(row)
                checkpoint=Path('competitor-output');checkpoint.mkdir(exist_ok=True)
                (checkpoint/'snapshot.json').write_text(json.dumps({'checked_at_kst':now.isoformat(),'brands':rows,'incomplete':len(rows)!=len(BRANDS)},ensure_ascii=False,indent=2))
                print(json.dumps({'brand':row['brand'],'pages':len(row['pages']),'errors':row['errors']},ensure_ascii=False),flush=True)
            rows.sort(key=lambda r:list(BRANDS).index(r['brand']))
            await browser.close()
    out=Path('competitor-output');out.mkdir(exist_ok=True)
    payload={'checked_at_kst':now.isoformat(),'scope':'official-domain candidates only; verify ownership and promotion details before reporting','brands':rows}
    (out/'snapshot.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2))
    print(json.dumps({r['brand']:{'pages':len(r['pages']),'status':r['status']} for r in rows},ensure_ascii=False))
    if not any(r['pages'] for r in rows): raise SystemExit(1)
if __name__=='__main__': asyncio.run(main())
