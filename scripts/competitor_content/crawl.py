"""Independent public editorial crawler. Never treats discovery as publication."""
import asyncio, hashlib, json, os, re, sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
import aiohttp
from playwright.async_api import async_playwright
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/"competitor_intelligence"))
from reliability import PolicyCache
from evidence import editorial_title, valid_title, product_evidence, person_evidence, image_candidates, PRODUCT_PATH
from publication import publication

ROOTS={
"KOLON SPORT":"https://www.kolonmall.com/Brands/KOLONSPORT",
"TNF":"https://www.thenorthfacekorea.co.kr/",
"ARC'TERYX":"https://arcteryx.co.kr/",
"SNOW PEAK":"https://www.snowpeakstore.co.kr/",
"DISCOVERY":"https://www.discovery-expedition.com/",
"PATAGONIA":"https://www.patagonia.co.kr/",
"SALOMON":"https://salomon.co.kr/",
"BLACKYAK":"https://www.byn.kr/blackyak",
"K2":"https://www.k2.co.kr/",
"EIDER":"https://www.eider.co.kr/",
"NEW BALANCE":"https://www.nbkorea.com/",
"NEPA":"https://www.nplus.co.kr/main/main.asp",
"NATIONAL GEOGRAPHIC":"https://www.naturestore.co.kr/",
"MILLET":"https://www.millet.co.kr/",
"COLUMBIA":"https://www.columbiakorea.co.kr/main/main.asp",
"MONTBELL":"https://www.montbell.co.kr/",
"MAMMUT":"https://www.mammut.kr/",
"FJALLRAVEN":"https://www.fjallraven.co.kr/app/",
"BLACK DIAMOND":"https://www.blackdiamondequipment.co.kr/",
"MERRELL":"https://www.merrellkorea.co.kr/",
"GREGORY":"https://www.gregorypacks.co.kr/home/",
"SNOWLINE":"https://www.snowlinemall.com/"}
SEEDS={"ARC'TERYX":["https://arcteryx.co.kr/explore/whoweare","https://arcteryx.co.kr/lp"],
"SALOMON":["https://salomon.co.kr/pages/77years-of-salomon","https://salomon.co.kr/blogs/salomon-blog"],
"KOLON SPORT":["https://www.kolonmall.com/Brands/KOLONSPORT/Special","https://www.kolonmall.com/Brands/KOLONSPORT/Special/259601","https://www.kolonmall.com/Brands/KOLONSPORT/Special/259682","https://www.kolonmall.com/Brands/kolonsport/Special/254611"],
"TNF":["https://www.thenorthfacekorea.co.kr/brand/brandStory","https://www.thenorthfacekorea.co.kr/brand/technology","https://www.thenorthfacekorea.co.kr/event/view/eventCategoryNF/93320","https://www.thenorthfacekorea.co.kr/event/view/eventCategoryNF/93338","https://www.thenorthfacekorea.co.kr/event/view/eventCategoryNF/93329","https://www.thenorthfacekorea.co.kr/event/view/eventCategoryNF/93337","https://www.thenorthfacekorea.co.kr/event/view/eventCategoryNF/93314","https://www.thenorthfacekorea.co.kr/event/view/eventCategoryNF/93313","https://www.thenorthfacekorea.co.kr/event/view/eventCategoryNF/93341","https://www.thenorthfacekorea.co.kr/event/view/eventCategoryNF/93339","https://www.thenorthfacekorea.co.kr/event/view/eventCategoryNF/93332","https://www.thenorthfacekorea.co.kr/event/view/eventCategoryNF/93168"],
"DISCOVERY":["https://www.discovery-expedition.com/display/promotions/collection/1556","https://www.discovery-expedition.com/display/promotions/collection/705"],
"K2":["https://www.k-village.co.kr/k2","https://www.k-village.co.kr/k2/is","https://www.k-village.co.kr/k2/campaign"],
"EIDER":["https://www.k-village.co.kr/eider"],
"NATIONAL GEOGRAPHIC":["https://www.nstationmall.com/natgeo","https://www.nstationmall.com/event/planning/1155","https://www.nstationmall.com/event/planning/1157","https://nstationmall.com/event/planning/1124"],
"PATAGONIA":["https://www.patagonia.co.kr/stories/main","https://www.patagonia.co.kr/stories/all","https://www.patagonia.co.kr/quality/main","https://www.patagonia.co.kr/stories/filmDetail/260","https://www.patagonia.co.kr/stories/filmDetail/272"],
"SNOW PEAK":["https://www.snowpeak.co.kr/event-page/2026/lookBook2/apparel.html","https://www.snowpeak.co.kr/event-page/2026/lookBook/apparel.html"],
"MILLET":["https://www.millet.co.kr/front/main","https://www.millet.co.kr/front/etc/brand_story","https://www.millet.co.kr/front/promo/pstories/196","https://www.millet.co.kr/front/promo/pstories/154","https://www.millet.co.kr/front/promo/pstories/161"]}
ALTERNATE={"K2":"k-village.co.kr","EIDER":"k-village.co.kr","NATIONAL GEOGRAPHIC":"nstationmall.com","SNOW PEAK":"snowpeak.co.kr"}
NATGEO_CURATED_PATHS={"/event/planning/1155","/event/planning/1157","/event/planning/1124"}
PAGE_LIMIT=int(os.getenv("CONTENT_PAGE_LIMIT","48"))
PRIORITY_PAGE_LIMIT=int(os.getenv("CONTENT_PRIORITY_PAGE_LIMIT","64"))
BRAND_SECONDS=int(os.getenv("CONTENT_BRAND_SECONDS","420"))
PRIORITY_BRANDS={"TNF","NEW BALANCE","KOLON SPORT","COLUMBIA","SALOMON","K2","EIDER","DISCOVERY","NATIONAL GEOGRAPHIC","ARC'TERYX"}
UA="CSKMarketResearchBot"
OUT=Path("reports/competitor_content")
DISCOVERY_FILE=OUT/"public-discovery.json"
KEY=re.compile(r"story|stories|pstories|journal|magazine|editorial|lookbook|campaign|explore|discover|collection|special|exhibition|technology|heritage|ambassador|athlete|community|responsibility|/event/view/|/event/planning/|/plan/|/lp(?:/|$)|/pages/|스토리|매거진|룩북|화보|캠페인|컬렉션|기술력|헤리티지|앰배서더|크리에이터|커뮤니티|기획전",re.I)
DENY=re.compile(r"(?:^|[/_.?&=])(?:login|logout|register|account|auth|cart|checkout|order|my|mypage|member|customer|support|faq|privacy|terms|admin|search|review|location|stores?|notice|raffle)(?:[/_.?&=]|$)",re.I)
PDP=re.compile(r"/(?:products?/(?:view|detail)(?:/|$)|products/[^/]+/?$|goods/view|goods_view)|[?&](?:goodsNo|productNo)=",re.I)
SALE=re.compile(r"black\s*friday|블랙프라이데이|쿠폰|할인|특가|세일|%\s*off|payback|페이백",re.I)
CTA=re.compile(r"더\s*보기|자세히|바로\s*가기|제품\s*보기|컬렉션\s*보기|shop\s*now|learn\s*more|read\s*more|explore",re.I)
TYPES={"LOOKBOOK":r"lookbook|룩북|화보|스타일링","PRODUCT_STORY":r"technology|고어텍스|방수|보온|기술력|테크니컬|innovation|insulation|단열|기능성","AMBASSADOR":r"ambassador|athlete|앰배서더|애슬릿|크리에이터","COMMUNITY":r"community|클럽|커뮤니티|워크숍|클리닉","BRAND_STORY":r"heritage|history|스토리|헤리티지|환경|지속가능|history|innovation","CAMPAIGN":r"campaign|캠페인|collection|컬렉션|capsule|캡슐"}
TPO={"하이킹":r"hiking|trekking|등산|하이킹|트레킹","캠핑":r"camping|캠핑","러닝":r"running|trail run|러닝|마라톤","설상":r"ski|snowboard|스키|스노보드","데일리":r"daily|urban|city|도심|일상|데일리","여행":r"travel|여행","낚시":r"fishing|angler|피싱|낚시","클라이밍":r"climb|boulder|클라이밍|암벽|등반","백패킹":r"backpacking|백패킹","사이클링":r"cycling|사이클|자전거","워터스포츠":r"water sport|kayak|surfing|수상스포츠|카약|서핑","트레일러닝":r"trail.?running|트레일.?러닝"}
USP={"보온":r"warmth|insulat|보온|체온","방수":r"waterproof|방수|고어텍스|gore.tex","경량":r"lightweight|경량|가벼운","내구성":r"durab|내구성","통기성":r"breathab|통기|투습","지속가능성":r"recycl|재활용|지속가능"}
def canonical(u):
    p=urlsplit(u)
    q=[(k,v) for k,v in parse_qsl(p.query,keep_blank_values=True) if not re.match(r"utm_|^(?:gclid|fbclid|ref|tracking)$",k,re.I)]
    return urlunsplit((p.scheme.lower(),p.netloc.lower(),p.path or "/",urlencode(sorted(q)),""))
def host(u): return (urlsplit(u).hostname or "").removeprefix("www.").removeprefix("m.")
def official_host(brand,u):
    return host(u)==host(ROOTS[brand]) or bool(ALTERNATE.get(brand) and host(u)==ALTERNATE[brand])
def allowed_link(brand,u):
    p=urlsplit(u)
    return p.scheme=="https" and not p.username and not p.password and official_host(brand,u) and not DENY.search(p.path+"?"+p.query)
def scoped(brand,u):
    p=urlsplit(u); path=p.path.lower()
    if re.search(r"/product/(?:detail|list|search)\.html|/product/[a-z0-9][^/]+/\d+/?$|/product/[A-Z0-9]{5,}/?$",p.path,re.I): return False
    if not allowed_link(brand,u): return False
    if re.search(r"/(?:sign-in|sign-up|image_zoom[^/]*|personal-information-collection-statement[^/]*|service/company)(?:/|\.html|$)",path,re.I): return False
    if re.search(r"(?:^|&)(?:product_no|goodsNo|productNo|styleCode)=",p.query,re.I): return False
    if host(u)==ALTERNATE.get(brand):
        if brand=="SNOW PEAK": return (path.startswith("/event-page/") and ("/lookbook" in path or "/takibidai/" in path) and path.endswith(".html")) or path.rstrip("/") in ("/events","/events/detail")
        if brand=="NATIONAL GEOGRAPHIC" and path.rstrip("/") in NATGEO_CURATED_PATHS: return True
        prefix={"K2":"/k2","EIDER":"/eider","NATIONAL GEOGRAPHIC":"/natgeo"}[brand]
        return path.rstrip("/")==prefix or path.startswith(prefix+"/") and not PDP.search(u)
    if PRODUCT_PATH.search(u): return False
    if brand=="COLUMBIA": return path.startswith(("/main/","/event/","/promotion/","/exhibition/","/about/","/technology/","/pc/technology/"))
    if brand=="KOLON SPORT": return path.startswith("/brands/kolonsport")
    if brand=="BLACKYAK": return path.startswith("/blackyak") or (path=="/_container/exh_list.php" and dict(parse_qsl(p.query)).get("hta_brand")=="blackyak")
    return not PDP.search(u)
def tags(patterns, s): return [k for k,v in patterns.items() if re.search(v,s,re.I)]
def published(raw):
    # Only explicit article date metadata; not event periods or site copyright.
    for value in raw or []:
        try:
            d=datetime.fromisoformat(value.replace("Z","+00:00"))
            if 2000<=d.year<=datetime.now().year and d.date()<=datetime.now().date(): return d.date().isoformat()
        except (ValueError,TypeError,AttributeError): pass
    return None
def item(brand, source, block, checked, old, placement):
    title=editorial_title(block.get("title"),block.get("description") or block.get("summary"),block.get("og_title"))
    body=" ".join((block.get("summary") or "").split())[:500]
    if not valid_title(title) or SALE.search(title) or re.fullmatch(r"(?:home|main|shop|공식몰|공식 온라인스토어|메인|더보기|new arrivals|best sellers)",title,re.I): return None
    # Preserve official product CTA destinations as evidence without crawling PDPs.
    urls=[canonical(u) for u in block.get("links",[]) if allowed_link(brand,u)]
    dest=(urls[0] if urls else canonical(source)) if placement=="main_visual" else canonical(source)
    identity=hashlib.sha256((brand+"|"+(canonical(source)+"|"+title.casefold() if placement=="main_visual" else canonical(source))).encode()).hexdigest()[:24]
    txt=title+" "+body
    rich_text=(title+" "+(block.get("body_evidence") or body))[:2500]
    products=product_evidence(block.get("link_details"),lambda u: allowed_link(brand,u) and (host(u)!=ALTERNATE.get(brand) or scoped(brand,u)))
    people=person_evidence(rich_text)
    assets=image_candidates(block)
    ty=tags(TYPES,txt)
    fp=hashlib.sha256(json.dumps([title,body,block.get("image"),urls,products,people,assets],ensure_ascii=False,sort_keys=True).encode()).hexdigest()
    prior=old.get(identity,{})
    date_info=publication(block,checked) if placement!="main_visual" else {"date":None,"status":"unknown","evidence":[]}
    scope="official_brand_editorial" if brand=="SNOW PEAK" and host(source)=="snowpeak.co.kr" else "official_mall"
    return dict(page_metrics=block.get("page_metrics") or None,evidence_scope=block.get("evidence_scope"),source_scope=scope,publication_status=date_info["status"],publication_evidence=date_info["evidence"],id=identity,brand=brand,title=title,url=dest,source_url=source,placement=placement,type=ty[0] if ty else ("MAIN_VISUAL" if placement=="main_visual" else "EDITORIAL_CANDIDATE"),type_tags=ty,tpo=tags(TPO,txt),selling_points=tags(USP,txt),classification_status="text_inferred",summary=body,thumbnail=assets[0]["url"] if assets else None,images=assets,image_status="rendered_candidates" if assets else "unverified",cta=[x[:80] for x in block.get("cta",[]) if CTA.search(x)][:5],cta_urls=list(dict.fromkeys(urls))[:8],influencer=people or None,products=products or None,published_at=date_info["date"],first_seen_at=prior.get("first_seen_at") or checked,last_seen_at=checked,collected_at=checked,fingerprint=fp,change="newly_observed" if not prior else "unchanged" if prior.get("fingerprint")==fp else "changed",status="candidate")
EXTRACT=r"""() => {
const clean=s=>(s||'').replace(/\s+/g,' ').trim();
const visible=e=>{const r=e.getBoundingClientRect();return r.width>10&&r.height>10&&getComputedStyle(e).visibility!=='hidden'};
const imageDetails=e=>Array.from(e.querySelectorAll('img')).filter(x=>visible(x)&&x.naturalWidth>=400&&x.naturalHeight>=120&&x.getBoundingClientRect().width>=220&&x.getBoundingClientRect().height>=80).map(x=>({url:x.currentSrc||x.src,alt:x.alt,width:x.naturalWidth,height:x.naturalHeight,method:'rendered_image'})).slice(0,12);
const images=e=>imageDetails(e).map(x=>x.url);
const linkDetails=e=>Array.from(e.querySelectorAll('a[href]')).filter(visible).map(a=>({url:a.href,label:clean(a.innerText||a.getAttribute('aria-label')||a.title||a.querySelector('img')?.alt),product_name:clean(a.querySelector('[class*="productName"],[class*="product_name"],[class*="goods_name"],[class*="item_name"],[class*="prd_name"]')?.innerText||'')})).slice(0,120);
const main=document.querySelector('main,[role=main]')||document.body;
const links=Array.from(document.querySelectorAll('a[href]')).filter(visible).map(a=>({url:a.href,label:clean(a.innerText||a.title||a.querySelector('img')?.alt)})).slice(0,500);
const dateEvidence=[];
const dateAdd=(raw,method)=>{if(raw)dateEvidence.push({raw:clean(raw),method});};
for(const e of document.querySelectorAll('meta[property="article:published_time"]'))dateAdd(e.content,'article_meta');
const articleRoot=main.querySelector('article,[itemtype*="Article"]');
const editorialRoot=articleRoot||main;
for(const e of (articleRoot||main).querySelectorAll('meta[itemprop="datePublished"],time[itemprop="datePublished"]'))dateAdd(e.content||e.dateTime||e.innerText,'datePublished_itemprop');
for(const el of document.querySelectorAll('script[type="application/ld+json"]')){try{const walk=x=>{if(Array.isArray(x))x.forEach(walk);else if(x&&typeof x==='object'){if(/Article|BlogPosting|NewsArticle/i.test(String(x['@type']))&&x.datePublished){const raw=x.url||x.mainEntityOfPage?.['@id']||x.mainEntityOfPage?.url;let same=false;try{same=typeof raw==='string'&&new URL(raw,location.href).origin===location.origin&&new URL(raw,location.href).pathname===location.pathname&&new URL(raw,location.href).search===location.search;}catch{}if(same||(!raw&&articleRoot))dateAdd(x.datePublished,'article_jsonld');}if(x['@graph'])walk(x['@graph']);}};walk(JSON.parse(el.textContent));}catch(e){}}
const label=/^(?:게시일|등록일|발행일|작성일|공개일|published(?:\s+on)?|posted(?:\s+on)?)\s*[:：]?\s*(?:20\d{2}|\d{2})/i;
for(const line of (articleRoot||main).innerText.split(/\n/).slice(0,80)){if(label.test(clean(line)))dateAdd(line,'explicit_publication_label');}
if(articleRoot){for(const e of articleRoot.querySelectorAll('header time, [class*="article-date"] time, [class*="published"] time')){if(!/modified|updated/i.test(e.className||''))dateAdd(e.dateTime||e.innerText,'article_header_time');}}
const heads=Array.from(main.querySelectorAll('h1,h2,h3')).filter(e=>visible(e)&&!e.closest('header,footer,nav,[role=navigation]')).slice(0,35);
const productPath=u=>/\/product\/(?:productDetail\.action|view\.asp|detail\.html|[A-Z0-9_-]{5,}(?:[/?#]|$))|\/products\/(?:view\/\d+|[^/?#]+$)|\/goods\/(?:view|detail)|[?&](?:goodsNo|productNo|styleCode|product_no)=/i.test(u)&&!/category|catalog|\/list|activity\.asp/i.test(u);
const audit=e=>{const txt=clean(e.innerText),ls=linkDetails(e),productLinks=[...new Set(ls.filter(l=>productPath(l.url)).map(l=>l.url))],cta=Array.from(e.querySelectorAll('a,button')).filter(v=>visible(v)&&!v.closest('header,footer,nav,[role=navigation]')).map(v=>clean(v.innerText||v.getAttribute('aria-label'))).filter(v=>/구매|쇼핑|상품\s*보기|제품\s*보기|shop|buy|read more|learn more|read article|자세히|신청|참가/i.test(v)&&v.length<70).slice(0,35);const sections=[];for(const n of e.querySelectorAll('h1,h2,h3,p,img,video,iframe,a[href],button')){if(!visible(n)||n.closest('header,footer,nav,[role=navigation]'))continue;const tag=n.tagName,label=clean(n.innerText||n.alt);let k=/^H[123]$/.test(tag)?'HEADING':tag==='IMG'&&n.naturalWidth>=400?'IMAGE':tag==='VIDEO'||tag==='IFRAME'&&/youtube|vimeo/i.test(n.src||'')?'VIDEO':tag==='A'&&productPath(n.href)?'PRODUCT':tag==='P'&&label.length>=40?'COPY':/구매|shop now|buy now|read article|제품 보기|참가 신청/i.test(label)&&label.length<70?'CTA':null;if(k&&sections.at(-1)!==k)sections.push(k);if(sections.length>=16)break}return {text_chars:txt.length,headings:e.querySelectorAll('h1,h2,h3').length,paragraphs:Array.from(e.querySelectorAll('p')).filter(v=>visible(v)&&clean(v.innerText).length>=40).length,images:imageDetails(e).length,videos:Array.from(e.querySelectorAll('video,iframe')).filter(v=>visible(v)&&(v.tagName==='VIDEO'||/youtube|vimeo/i.test(v.src||''))).length,product_links:productLinks.length,price_mentions:(txt.match(/\d{1,3}(?:,\d{3})+\s*원/g)||[]).length,cta_count:cta.length,cta_labels:cta,sequence:sections,source:'rendered_dom'}};
const rootTitle=()=>clean(articleRoot?.querySelector('h1,h2')?.innerText||document.querySelector('meta[property="og:title"]')?.content||heads.find(h=>! /자주 묻는 질문|개인정보|stay connected|고객 서비스/i.test(h.innerText))?.innerText||document.title);

const block=h=>{let e=h.parentElement;for(let i=0;i<3&&e.parentElement&&e!==main;i++){if(images(e).length&&e.querySelector('a[href]'))break;const next=e.parentElement;if(next===main||next===document.body||next.querySelectorAll('h1,h2,h3').length>2||clean(next.innerText).length>1800)break;e=next;}const evidence=audit(e);return {title:clean(h.innerText),summary:clean(e.innerText).slice(0,500),image:images(e)[0]||null,image_details:imageDetails(e),body_evidence:clean(e.innerText).slice(0,2500),link_details:linkDetails(e),links:Array.from(e.querySelectorAll('a[href]')).map(a=>a.href).slice(0,8),cta:evidence.cta_labels,page_metrics:evidence,evidence_scope:'local_block'}};
const blocks=heads.map(block);
for(const a of main.querySelectorAll('a[href] img')){if(!visible(a)||a.getBoundingClientRect().width<400||a.getBoundingClientRect().height<120||!clean(a.alt))continue; const p=a.closest('a');blocks.push({title:clean(a.alt),summary:clean(p.innerText),image:a.currentSrc||a.src,image_details:[{url:a.currentSrc||a.src,alt:a.alt,width:a.naturalWidth,height:a.naturalHeight,method:'rendered_image'}],link_details:linkDetails(p),links:[p.href],cta:audit(p).cta_labels,page_metrics:audit(p),evidence_scope:'local_block'});if(blocks.length>=50)break;}
const og=k=>document.querySelector('meta[property="'+k+'"]')?.content;
return {title:clean(heads[0]?.innerText||og('og:title')||document.title),body:clean(main.innerText).slice(0,10000),blocks,links,article:{title:rootTitle(),summary:clean(og('og:description')||editorialRoot.innerText).slice(0,500),description:clean(og('og:description')),og_title:clean(og('og:title')),body_evidence:clean(editorialRoot.innerText).slice(0,2500),image:og('og:image')||images(editorialRoot)[0]||null,image_details:imageDetails(editorialRoot),link_details:linkDetails(editorialRoot),links:linkDetails(editorialRoot).map(x=>x.url).slice(0,30),cta:linkDetails(editorialRoot).filter(x=>x.label).map(x=>x.label).slice(0,30),date_evidence:dateEvidence,page_metrics:audit(editorialRoot),evidence_scope:articleRoot?'editorial_root':'page_main'}};
}"""
async def scan(browser,session,brand,checked,old):
    root=ROOTS[brand]; page_limit=PRIORITY_PAGE_LIMIT if brand in PRIORITY_BRANDS else PAGE_LIMIT; row={"brand":brand,"root":root,"pages_checked":0,"items":[],"errors":[]}
    same=lambda u: scoped(brand,u)
    # robots retrieval is origin-level; allow only the official host.
    policy=PolicyCache(session,UA,lambda u:urlsplit(u).scheme=="https" and official_host(brand,u))
    context=await browser.new_context(user_agent=UA,locale="ko-KR",service_workers="block")
    blocked=set()
    def origin(u): return urlsplit(u).netloc
    async def route(r):
        u=r.request.url
        if r.request.resource_type=="document" and (not same(u) or origin(u) in blocked or not await policy.permitted(u)): await r.abort()
        elif r.request.method!="GET" or r.request.resource_type in ("media","font","websocket"): await r.abort()
        else: await r.continue_()
    await context.route("**/*",route)
    page=await context.new_page()
    queue=[root]+SEEDS.get(brand,[])
    try:
        discovered=json.loads(DISCOVERY_FILE.read_text()).get("entries",[])
        for entry in discovered:
            if entry.get("brand")!=brand or entry.get("kind") not in ("detail","list","candidate"): continue
            url=entry.get("url","")
            # Only ordinary public official destinations. Respect robots on all new origins.
            if isinstance(url,str) and scoped(brand,url) and url not in queue: queue.append(url)
    except (OSError,ValueError,TypeError): pass
    previous=sorted((x for x in old.values() if x.get("brand")==brand),key=lambda x:x.get("last_seen_at",""),reverse=True)
    for x in previous:
        v=x.get("url") if x.get("placement")=="main_visual" else x.get("source_url")
        if isinstance(v,str) and same(v) and KEY.search(v) and v not in queue: queue.append(v)
        if len(queue)>=page_limit: break
    seen=set();ids=set()
    row["discovered_urls"]=0;row["pages_attempted"]=0;row["pending_urls"]=0;row["budget_limit"]=page_limit
    row["homepage_observed"]=False
    deadline=asyncio.get_running_loop().time()+BRAND_SECONDS
    try:
        while queue and len(seen)<page_limit and asyncio.get_running_loop().time()<deadline:
            u=canonical(queue.pop(0))
            if u in seen or not same(u) or origin(u) in blocked: continue
            seen.add(u)
            row["pages_attempted"]+=1
            if not await policy.permitted(u):
                reason=policy.reason(u)
                row["errors"].append({"url":u,"error":reason})
                if re.search(r"HTTP (?:403|429)|CertificateError",str(reason)): blocked.add(origin(u))
                continue
            try:
                resp=await page.goto(u,wait_until="domcontentloaded",timeout=18000)
                if resp and resp.status in (403,429):
                    blocked.update([origin(u),origin(page.url)]);raise RuntimeError("HTTP "+str(resp.status)+"; origin stopped")
                if not resp or resp.status>=400 or not same(page.url): raise RuntimeError("navigation failed or out of scope")
                await page.wait_for_timeout(1200)
                for _ in range(8 if brand in PRIORITY_BRANDS else 5):
                    end=await page.evaluate("()=>{const prior=window.scrollY;window.scrollBy(0,Math.max(window.innerHeight,650));return window.scrollY===prior}")
                    await page.wait_for_timeout(450)
                    if end: break
                await page.evaluate("window.scrollTo(0,0)")
                data=await page.evaluate(EXTRACT)
                if len(data["body"])<100 or re.search(r"access denied|captcha|접근이 제한",data["body"][:600],re.I): raise RuntimeError("blocked or empty document")
                source=canonical(page.url)
                if brand=="NATIONAL GEOGRAPHIC" and urlsplit(source).path.rstrip("/") in NATGEO_CURATED_PATHS and not re.search(r"national\s*geographic|내셔널지오그래픽",data["body"]+" "+data["title"],re.I):
                    raise RuntimeError("shared storefront brand attribution missing")
                row["pages_checked"]+=1
                path=urlsplit(source).path.lower().rstrip("/")
                home=u==canonical(root) or (host(source)==ALTERNATE.get(brand) and path in ("/k2","/eider","/natgeo"))
                if home: row["homepage_observed"]=True
                is_index=any(entry.get("brand")==brand and entry.get("kind")=="list" and canonical(entry.get("url",""))==source for entry in discovered) if 'discovered' in locals() else False
                listing=is_index or bool(re.search(r"/(?:list|category|search)(?:/|$)|/special$|/blogs/[^/]+$|/collections/?$|/stories/(?:main|all|filmDetail)$|/campaign$",path,re.I))
                blocks=data["blocks"] if home else ([] if listing else [data["article"]])
                for b in blocks:
                    evidence=(b.get("title") or "")+" "+(b.get("summary") or "")
                    if home and not b.get("image") and not KEY.search(evidence): continue
                    if home and re.search(r"\d[\d,]*\s*원|₩\s*\d",evidence) and not (KEY.search(b.get("title","")) or (b.get("image_details") and len(b.get("summary",""))>30)): continue
                    if home and b.get("links") and all(PRODUCT_PATH.search(v) for v in b["links"]) and not b.get("image"): continue
                    if not home and not (KEY.search(source+" "+evidence) or tags(TYPES,evidence)): continue
                    x=item(brand,source,b,checked,old,"main_visual" if home else "editorial_page")
                    if x and x["id"] not in ids:
                        row["items"].append(x);ids.add(x["id"])
                        if home and x.get("url") and x["url"]!=source and same(x["url"]) and x["url"] not in seen: queue.append(x["url"])
                        if x["images"] and sum(bool(i.get("image_evidence_file")) for i in row["items"])<2:
                            # Archive only an actually rendered, public image as a bounded screenshot.
                            try:
                                src=x["images"][0]["url"]
                                handle=await page.evaluate_handle("(url)=>Array.from(document.images).find(i=>(i.currentSrc||i.src)===url)||null",src)
                                el=handle.as_element()
                                if el and await el.is_visible():
                                    target=Path("content-evidence")/(x["id"]+"-"+x["fingerprint"][:10]+".png")
                                    target.parent.mkdir(exist_ok=True)
                                    await el.screenshot(path=str(target),timeout=3500,animations="disabled")
                                    x["image_evidence_file"]=target.name
                                    try:
                                        from PIL import Image
                                        with Image.open(target) as im:
                                            sample=im.convert("RGB");sample.thumbnail((64,64))
                                            colors=sample.quantize(colors=5).convert("RGB").getcolors(4096) or []
                                            x["visual_palette"]=["#%02x%02x%02x"%rgb for _,rgb in sorted(colors,reverse=True)[:5]]
                                    except Exception: pass
                                await handle.dispose()
                            except Exception: pass
                for link in data["links"]:
                    v=canonical(link["url"])
                    if same(v) and v not in seen and (KEY.search(v+" "+link["label"]) or (listing and re.search(r"event|promo|project|_exh|planning|/article/|campaignCode|/filmDetail/|story|journal|/special/",v,re.I))) and not SALE.search(link["label"]): queue.append(v)
                queue=list(dict.fromkeys(queue))
                # Explicit editorial routes outrank generic category and promotion hubs.
                queue.sort(key=lambda v: 0 if re.search(r"/(?:blogs/[^/]+/|special/\d+|story|stories|journal|magazine|lookbook|campaign|explore|collection/\d+)",urlsplit(v).path,re.I) else 1)
                row["discovered_urls"]=max(row["discovered_urls"],len(seen)+len(queue))
                queue=queue[:240]
            except Exception as e: row["errors"].append({"url":u,"error":str(e)[:300]})
            await asyncio.sleep(2)
    finally:
        await context.close()
    row["pending_urls"]=len(queue)
    row["budget_exhausted"]=bool(queue and (len(seen)>=page_limit or asyncio.get_running_loop().time()>=deadline))
    row["status"]="candidate" if row["items"] else "no_candidate" if row["pages_checked"] else "not_collected"
    return row
def aggregate(rows,prior,checked,prior_brands=None):
    prior_brands={b['brand']:b for b in (prior_brands or [])}
    current=[x for b in rows for x in b["items"]]
    archive={x["id"]:x for x in prior}
    archive.update({x["id"]:x for x in current})
    cutoff=(datetime.fromisoformat(checked)-timedelta(days=30)).date().isoformat()
    today=datetime.fromisoformat(checked).date().isoformat()
    history=[x for x in archive.values() if x.get("last_seen_at","")[:10]>=cutoff]
    summary=[]
    for b in rows:
        hist=[x for x in history if x["brand"]==b["brand"]]
        known=[x for x in hist if x.get("published_at")]
        summary.append({k:v for k,v in b.items() if k!="items"}|{"last_success_at":checked if b["pages_checked"] else prior_brands.get(b["brand"],{}).get("last_success_at") or max((x.get("last_seen_at") or "" for x in hist),default="") or None,"candidate_count":len(b["items"]),"newly_observed":sum(x["change"]=="newly_observed" for x in b["items"]),"changed":sum(x["change"]=="changed" for x in b["items"]),"discovered_30d":sum(cutoff<=x["first_seen_at"][:10]<=today for x in hist),"published_30d":sum(cutoff<=x["published_at"]<=today for x in known) if known else None,"unknown_publication":len(hist)-len(known)})
    return {"checked_at_kst":checked,"scope":"public official sites; editorial candidates, not verified publications","brands":summary,"items":current,"history":history,"coverage":{"target":len(rows),"accessible":sum(b["pages_checked"]>0 for b in rows),"with_candidates":sum(bool(b["items"]) for b in rows),"items":len(current),"with_rendered_images":sum(bool(x.get("images")) for x in current),"with_product_evidence":sum(bool(x.get("products")) for x in current),"with_person_evidence":sum(bool(x.get("influencer")) for x in current),"with_confirmed_publication_date":sum(bool(x.get("published_at")) for x in current),"with_conflicting_publication_dates":sum(x.get("publication_status")=="conflicting" for x in current),"official_mall_brands":len({x["brand"] for x in current if x.get("source_scope")=="official_mall"}),"official_brand_editorial_brands":len({x["brand"] for x in current if x.get("source_scope")=="official_brand_editorial"}),"archived_image_count":sum(bool(x.get("image_evidence_file")) for x in current)}}
def self_test():
    assert canonical("https://arcteryx.co.kr/lp?utm_source=a&x=2#h")=="https://arcteryx.co.kr/lp?x=2"
    assert not scoped("BLACKYAK","https://www.byn.kr/other")
    assert not scoped("SALOMON","https://salomon.co.kr/account/login")
    assert not scoped("ARC'TERYX","https://arcteryx.co.kr/products/view/685326")
    assert not scoped("SALOMON","https://salomon.co.kr/products/shoe")
    assert not scoped("SALOMON","https://example.com/pages/story")
    assert published(["2020-11-01T12:00:00+09:00"])=="2020-11-01"
    now="2026-11-18T21:00:00+09:00"
    b={"title":"Winter insulation","summary":"보온 기술","dates":["2026-11-01"],"links":[]}
    a=item("SALOMON",ROOTS["SALOMON"],b,now,{},"main_visual")
    assert a["published_at"] is None and "보온" in a["selling_points"]
    assert item("SALOMON",ROOTS["SALOMON"],b,now,{a["id"]:a},"main_visual")["change"]=="unchanged"
    assert item("SALOMON",ROOTS["SALOMON"],{"title":"블랙프라이데이 할인"},now,{},"main_visual") is None
    z=aggregate([{"brand":"SALOMON","items":[a],"pages_checked":1}],[],now)
    assert z["brands"][0]["published_30d"] is None
    assert scoped("K2","https://www.k-village.co.kr/k2")
    assert not scoped("K2","https://www.k-village.co.kr/eider")
    assert scoped("SNOW PEAK","https://www.snowpeak.co.kr/event-page/2026/lookBook2/apparel.html")
    assert not scoped("SNOW PEAK","https://www.snowpeak.co.kr/")
    assert scoped("NATIONAL GEOGRAPHIC","https://www.nstationmall.com/event/planning/1157")
    assert not scoped("NATIONAL GEOGRAPHIC","https://www.nstationmall.com/event/planning/9999")
    assert scoped("TNF","https://www.thenorthfacekorea.co.kr/event/view/eventCategoryNF/93320")
    assert not scoped("TNF","https://www.thenorthfacekorea.co.kr/product/NN2PQ58B")
    assert not scoped("MAMMUT","https://www.mammut.kr/product/detail.html?product_no=12")
    print("content self-tests passed")
async def main():
    checked=datetime.now(timezone(timedelta(hours=9))).isoformat()
    OUT.mkdir(parents=True,exist_ok=True)
    dest=OUT/"latest.json"
    previous=json.loads(dest.read_text()) if dest.exists() else {}
    prior=previous.get("history",[])
    old={x["id"]:x for x in prior}
    async with aiohttp.ClientSession(headers={"User-Agent":UA},timeout=aiohttp.ClientTimeout(total=15)) as session:
        async with async_playwright() as p:
            browser=await p.chromium.launch(headless=True,**({"executable_path":os.environ["CRAWLER_CHROME"]} if os.environ.get("CRAWLER_CHROME") else {}))
            sem=asyncio.Semaphore(3)
            async def task(brand):
                async with sem:
                    try: return await asyncio.wait_for(scan(browser,session,brand,checked,old),timeout=BRAND_SECONDS+30)
                    except Exception as e: return {"brand":brand,"root":ROOTS[brand],"pages_checked":0,"items":[],"status":"not_collected","errors":[{"error":str(e) or type(e).__name__}]}
            rows=await asyncio.gather(*(task(b) for b in ROOTS))
            await browser.close()
    payload=aggregate(rows,prior,checked,previous.get('brands',[]))
    if not payload["coverage"]["accessible"]:
        (OUT/"failed-run.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2))
        raise SystemExit("No accessible brands; prior feed preserved")
    dest.write_text(json.dumps(payload,ensure_ascii=False,indent=2))
    print(json.dumps(payload["coverage"],ensure_ascii=False))
if __name__=="__main__":
    if "--self-test" in sys.argv: self_test()
    else: asyncio.run(main())
