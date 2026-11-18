"""Collect visible brand-logo artwork, with provenance, from public official storefronts.
Do not bypass robots or capture generic shared-mall marks. Preserve verified earlier assets."""
import asyncio, json, os, re, sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
import aiohttp
from playwright.async_api import async_playwright
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"competitor_content"))
from crawl import ROOTS, SEEDS, ALTERNATE, official_host
from reliability import PolicyCache
OUT=Path("reports/competitor_content/logos")
UA="CSKMarketResearchBot"
ALIASES={
 "TNF":["thenorthface","northface","노스페이스"],"KOLON SPORT":["kolonsport","코오롱스포츠"],
 "ARC'TERYX":["arcteryx","아크테릭스"],"NATIONAL GEOGRAPHIC":["nationalgeographic","natgeo","내셔널지오그래픽"],
 "NEW BALANCE":["newbalance","nbkorea","뉴발란스"],"BLACK DIAMOND":["blackdiamond","블랙다이아몬드"],
 "FJALLRAVEN":["fjallraven","fjällräven","피엘라벤"],"SNOW PEAK":["snowpeak","스노우피크"],
 "DISCOVERY":["discovery","디스커버리"],"SALOMON":["salomon","살로몬"],"BLACKYAK":["blackyak","블랙야크"],
 "K2":["k2","케이투"],"EIDER":["eider","아이더"],"NEPA":["nepa","네파"],"MILLET":["millet","밀레"],
 "MONTBELL":["montbell","몽벨"],"MAMMUT":["mammut","마무트"],"MERRELL":["merrell","머렐"],
 "GREGORY":["gregory","그레고리"],"SNOWLINE":["snowline","스노우라인"],"PATAGONIA":["patagonia","파타고니아"]
}
SHARED={"KOLON SPORT","BLACKYAK","K2","EIDER","NATIONAL GEOGRAPHIC","NEPA"}
SELECTOR='header img,header svg,[role=banner] img,[role=banner] svg,[class*="logo" i] img,[class*="logo" i] svg,img[class*="logo" i],svg[class*="logo" i],img[alt*="logo" i],img[alt*="로고"],img[src*="logo" i],[class*="logo" i],h1 a'
def norm(s):return re.sub(r"[^a-z0-9가-힣ä]","",s.casefold())
def slug(s):return re.sub(r"[^a-z0-9]+","-",s.casefold()).strip("-")
async def collect(browser,session,brand,root):
    permitted=lambda u:urlparse(u).scheme=="https" and official_host(brand,u)
    policy=PolicyCache(session,UA,permitted)
    urls=[root]+[u for u in SEEDS.get(brand,[]) if urlparse(u).hostname!=urlparse(root).hostname][:1]
    ctx=await browser.new_context(user_agent=UA,viewport={"width":1440,"height":850},device_scale_factor=2,locale="ko-KR",service_workers="block")
    async def route(rt):
        q=rt.request
        if q.method!="GET" or q.resource_type in ("media","font","websocket"):await rt.abort()
        elif q.resource_type=="document" and (not permitted(q.url) or not await policy.permitted(q.url)):await rt.abort()
        else:await rt.continue_()
    await ctx.route("**/*",route)
    page=await ctx.new_page()
    try:
        for url in urls:
            if not await policy.permitted(url):continue
            try:
                res=await page.goto(url,wait_until="domcontentloaded",timeout=16000)
                if not res or res.status==403 or res.status==429:return None
                if res.status>=400 or not permitted(page.url):continue
                await page.wait_for_timeout(1000)
                if re.search(r"access denied|captcha|접근이 제한", (await page.locator("body").inner_text(timeout=5000))[:500],re.I):continue
                loc=page.locator(SELECTOR)
                ranked=[]
                for i in range(min(await loc.count(),100)):
                    el=loc.nth(i)
                    if not await el.is_visible():continue
                    meta=await el.evaluate("""e=>{const r=e.getBoundingClientRect(),p=e.closest('a'),tag=e.tagName.toLowerCase(),cls=(e.getAttribute('class')||'')+' '+(p?.getAttribute('class')||'')+' '+(e.parentElement?.getAttribute('class')||'');return {w:r.width,h:r.height,y:r.y,x:r.x,tag,cls,alt:e.getAttribute('alt')||'',src:e.currentSrc||e.getAttribute('src')||'',aria:(e.getAttribute('aria-label')||'')+' '+(p?.getAttribute('aria-label')||''),href:p?.getAttribute('href')||'',text:(e.textContent||'').trim().slice(0,90),bg:getComputedStyle(e).backgroundImage,header:!!e.closest('header,[role=banner]'),loaded:tag!=='img'||(e.complete&&e.naturalWidth>0)} }""")
                    if not meta["loaded"] or not (22<=meta["w"]<=400 and 13<=meta["h"]<=160 and 0<=meta["y"]<320 and meta["w"]/meta["h"]>=.65):continue
                    if meta["tag"] not in ("svg","img") and meta["bg"]=="none" and not meta["text"]:continue
                    desc=" ".join(str(meta[k]) for k in ("alt","src","aria","href","text","cls","bg"))
                    named=any(norm(a) in norm(desc) for a in ALIASES.get(brand,[brand]) if len(norm(a))>=2)
                    logo=bool(re.search(r"logo|로고|logotype|wordmark",desc,re.I))
                    if not (logo or named) or re.search(r"social|facebook|instagram|kakao|youtube|payment|search|cart|partner|footer",desc,re.I):continue
                    if brand in SHARED and not named:continue
                    if not meta["header"] and not (logo and meta["y"]<160):continue
                    if meta["tag"] not in ("svg","img") and meta["bg"]=="none":continue
                    score=(60 if named else 0)+(30 if logo else 0)+(20 if meta["header"] else 0)+(10 if meta["tag"] in ("img","svg") else 0)-meta["y"]/15
                    ranked.append((score,i,meta))
                for _,i,m in sorted(ranked,key=lambda x:x[0],reverse=True)[:3]:
                    try:
                        raw=await loc.nth(i).screenshot(timeout=5000,animations="disabled",omit_background=True,scale="device")
                        if len(raw)<150 or len(raw)>800000:continue
                        name=slug(brand)+".png";target=OUT/name;target.write_bytes(raw)
                        return {"file":"logos/"+name,"source":page.url,"asset":m["src"] if m["src"].startswith("https://") and "?" not in m["src"] else None,"method":"visible_official_logo","observed_at":datetime.now(timezone.utc).isoformat()}
                    except Exception:continue
            except Exception:continue
    finally:await ctx.close()
    return None
async def main():
    OUT.mkdir(parents=True,exist_ok=True)
    dest=OUT/"manifest.json"
    prior=json.loads(dest.read_text()).get("brands",{}) if dest.exists() else {}
    prior={k:v for k,v in prior.items() if isinstance(v,dict) and (OUT/Path(v.get("file","")).name).is_file()}
    updated=dict(prior);fail=[]
    async with aiohttp.ClientSession(headers={"User-Agent":UA},timeout=aiohttp.ClientTimeout(total=12)) as session:
        async with async_playwright() as p:
            browser=await p.chromium.launch(headless=True,**({"executable_path":os.environ["CRAWLER_CHROME"]} if os.environ.get("CRAWLER_CHROME") else {}))
            sem=asyncio.Semaphore(3)
            async def task(b,u):
                async with sem:
                    try:return b,await asyncio.wait_for(collect(browser,session,b,u),timeout=65)
                    except Exception:return b,None
            for b,result in await asyncio.gather(*(task(b,u) for b,u in ROOTS.items() if b!="COLUMBIA")):
                if result:updated[b]=result
                else:fail.append(b)
            await browser.close()
    payload={"schema_version":1,"brands":updated,"unverified":sorted(set(fail)-set(updated)),"note":"Public visible brand logos only; failures retain earlier verified logo or text fallback."}
    dest.write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n")
    print(json.dumps({"verified":len(updated),"unverified":payload["unverified"]},ensure_ascii=False))
if __name__=="__main__":asyncio.run(main())
