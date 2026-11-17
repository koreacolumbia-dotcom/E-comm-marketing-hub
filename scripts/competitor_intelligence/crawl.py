import asyncio, json, re, hashlib, os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urljoin,urlparse
from urllib.robotparser import RobotFileParser
import aiohttp
from playwright.async_api import async_playwright
BRANDS={"TNF":"https://www.thenorthfacekorea.co.kr/","K2":"https://www.k2.co.kr/","DISCOVERY":"https://www.discovery-expedition.com/","BLACKYAK":"https://www.byn.kr/blackyak","NEPA":"https://www.nplus.co.kr/main/main.asp","KOLON SPORT":"https://www.kolonmall.com/Brands/KOLONSPORT","EIDER":"https://www.eider.co.kr/","NATIONAL GEOGRAPHIC":"https://www.naturestore.co.kr/","SNOW PEAK":"https://www.snowpeakstore.co.kr/","MILLET":"https://www.millet.co.kr/"}
UA="CSKMarketResearchBot"
EVENT=re.compile(r'event|promotion|exhibition|_exh|exh_list|special|企画|企画|기획전|이벤트|프로모션|세일|페스타|블랙프라이데이|혜택',re.I)
DEAL=re.compile(r'할인|쿠폰|사은품|적립|특가|black friday|sale|%\s*off',re.I)
PRODUCT=re.compile(r'goods_view|product/detail|productdetail|/product/|/goods/|goodsNo=|productNo=',re.I)
DATE=re.compile(r'20\d{2}[.\-/ ]\s*\d{1,2}[.\-/ ]\s*\d{1,2}|\d{1,2}[./]\d{1,2}\s*[~–-]\s*\d{1,2}[./]\d{1,2}')
SEEDS={'BLACKYAK':['https://www.byn.kr/_container/exh_list.php?hta_brand=blackyak&st=1','https://m.byn.kr/blackyak/_exh/'],'NEPA':['https://www.nplus.co.kr/promotion/list.asp','https://www.nplus.co.kr/product/list.asp','https://www.nplus.co.kr/promotion/view.asp?EventNo=856'],'KOLON SPORT':['https://www.kolonmall.com/Special/259472']}
def host(u): return (urlparse(u).hostname or '').removeprefix('www.').removeprefix('m.')
async def scan(browser,session,name,root):
    result={'brand':name,'root':root,'status':'unverified','pages':[],'errors':[]}
    allowed=host(root)
    context=None
    deadline=asyncio.get_running_loop().time()+360
    try:
        async with session.get(urljoin(root,'/robots.txt'),allow_redirects=False) as r:
            if r.status==404: rules=''
            elif r.status==200: rules=await r.text()
            else: raise RuntimeError('robots HTTP '+str(r.status))
        rp=RobotFileParser();rp.parse(rules.splitlines())
        policies={urlparse(root).netloc:rp}
        for seed in SEEDS.get(name,[]):
            netloc=urlparse(seed).netloc
            if netloc in policies: continue
            try:
                async with session.get(urljoin(seed,'/robots.txt'),allow_redirects=False) as rr:
                    if rr.status not in (200,404): continue
                    policy=RobotFileParser();policy.parse((await rr.text() if rr.status==200 else '').splitlines());policies[netloc]=policy
            except Exception: pass
        def permitted(u):
            policy=policies.get(urlparse(u).netloc)
            return bool(policy and policy.can_fetch(UA,u))
        context=await browser.new_context(user_agent=UA,locale='ko-KR')
        async def route_handler(route):
            u=route.request.url
            if route.request.resource_type=='document' and (host(u)!=allowed or not permitted(u)): await route.abort()
            else: await route.continue_()
        await context.route('**/*',route_handler)
        page=await context.new_page()
        queue=SEEDS.get(name,[])+[root];seen=set()
        while queue and len(seen)<18 and asyncio.get_running_loop().time()<deadline:
            u=queue.pop(0)
            if u in seen or host(u)!=allowed: continue
            if not permitted(u):
                result['errors'].append({'url':u,'error':'robots policy unavailable or disallowed'}); continue
            seen.add(u)
            try:
                response=await page.goto(u,wait_until='domcontentloaded',timeout=15000)
                if not response or response.status>=400 or host(page.url)!=allowed: raise RuntimeError('navigation failed')
                await page.wait_for_timeout(1800)
                body=await page.locator('body').inner_text(timeout=8000)
                if len(body.strip())<100 or re.search(r'access denied|captcha|접근이 제한',body[:1000],re.I): raise RuntimeError('empty or blocked document')
                structured=await page.locator('script[type="application/ld+json"]').all_text_contents()
                images=await page.locator('img').evaluate_all("els=>els.slice(0,80).map(e=>({src:e.currentSrc||e.src,alt:e.alt}))")
                links=await page.locator('a[href], img[alt]').evaluate_all("""els=>els.map(e=>({url:e.href||e.closest('a')?.href||'',label:[e.innerText,e.alt,e.title,e.querySelector('img')?.alt].filter(Boolean).join(' ')}))""")
                snippets=[x.strip()[:350] for x in body.splitlines() if DEAL.search(x) or DATE.search(x)][:60]
                banners=[x for x in links if EVENT.search(x['label']) or DEAL.search(x['label'])][:25]
                result['pages'].append({'url':page.url,'title':await page.title(),'structured_data':structured[:15],'images':images,'evidence':snippets,'banner_labels':banners,'date_mentions':DATE.findall(body)[:30],'percent_mentions':re.findall(r'\d{1,3}\s*%',body)[:40],'price_mentions':re.findall(r'[\d,]+\s*원',body)[:40],'fingerprint':hashlib.sha256(json.dumps([snippets,banners],ensure_ascii=False).encode()).hexdigest()})
                candidates=[x['url'].split('#')[0] for x in links if x['url'].startswith('https://') and host(x['url'])==allowed and (EVENT.search(x['url']+' '+x['label']) or PRODUCT.search(x['url']))]
                queue=list(dict.fromkeys(queue+[x for x in candidates if x not in seen]))
                queue=sorted(queue,key=lambda x: (not bool(EVENT.search(x)),not bool(PRODUCT.search(x))))[:40]
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
    now=datetime.now(timezone(timedelta(hours=9)))
    async with aiohttp.ClientSession(headers={'User-Agent':UA},timeout=aiohttp.ClientTimeout(total=20)) as session:
        async with async_playwright() as p:
            browser=await p.chromium.launch(headless=True, **({'executable_path':os.environ['CRAWLER_CHROME']} if os.environ.get('CRAWLER_CHROME') else {}))
            limit=asyncio.Semaphore(3)
            async def bounded(name,root):
                async with limit:
                    try: return await asyncio.wait_for(scan(browser,session,name,root),timeout=420)
                    except Exception as e: return {'brand':name,'root':root,'status':'unverified','pages':[],'errors':[{'error':str(e) or type(e).__name__}]}
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
