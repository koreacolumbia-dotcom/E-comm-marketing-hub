"""Independent public editorial crawler. Never treats discovery as publication."""
import asyncio, gzip, hashlib, io, json, os, re, sys
import xml.etree.ElementTree as ET
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
"HOKA":"https://linktr.ee/hoka_korea",
"DECATHLON":"https://www.decathlon.co.kr/",
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
SEEDS={"BLACKYAK":["https://www.byn.kr/story/blackyak/","https://www.byn.kr/story/blackyak/?cate=BRAND_CAMPAIGN&gb=list","https://www.byn.kr/story/blackyak/?gb=detail&no=1894","https://www.byn.kr/blackyak/_event/23ss_by_campaign.php","https://www.byn.kr/_container/exh_list.php?hta_brand=blackyak&st=1"],
"ARC'TERYX":["https://arcteryx.co.kr/explore/whoweare","https://arcteryx.co.kr/lp"],
"SALOMON":["https://salomon.co.kr/pages/77years-of-salomon","https://salomon.co.kr/blogs/salomon-blog"],
"KOLON SPORT":["https://www.kolonmall.com/Brands/KOLONSPORT/Special","https://www.kolonmall.com/Brands/KOLONSPORT/Special/259601","https://www.kolonmall.com/Brands/KOLONSPORT/Special/259682","https://www.kolonmall.com/Brands/kolonsport/Special/254611"],
"TNF":["https://www.thenorthfacekorea.co.kr/lookbook","https://www.thenorthfacekorea.co.kr/brand/vision","https://www.thenorthfacekorea.co.kr/brand/brandStory","https://www.thenorthfacekorea.co.kr/brand/technology","https://www.thenorthfacekorea.co.kr/event/view/eventCategoryNF/93320","https://www.thenorthfacekorea.co.kr/event/view/eventCategoryNF/93338","https://www.thenorthfacekorea.co.kr/event/view/eventCategoryNF/93329","https://www.thenorthfacekorea.co.kr/event/view/eventCategoryNF/93337","https://www.thenorthfacekorea.co.kr/event/view/eventCategoryNF/93314","https://www.thenorthfacekorea.co.kr/event/view/eventCategoryNF/93313","https://www.thenorthfacekorea.co.kr/event/view/eventCategoryNF/93341","https://www.thenorthfacekorea.co.kr/event/view/eventCategoryNF/93339","https://www.thenorthfacekorea.co.kr/event/view/eventCategoryNF/93332","https://www.thenorthfacekorea.co.kr/event/view/eventCategoryNF/93168"],
"DISCOVERY":["https://www.discovery-expedition.com/display/promotions","https://www.discovery-expedition.com/display/promotions/collection/1556","https://www.discovery-expedition.com/display/promotions/collection/705","https://www.discovery-expedition.com/display/promotions/collection/1649","https://www.discovery-expedition.com/display/promotions/collection/1619","https://www.discovery-expedition.com/display/promotions/collection/1596","https://www.discovery-expedition.com/display/promotions/collection/1760"],
"K2":["https://www.k-village.co.kr/k2","https://www.k-village.co.kr/k2/is","https://www.k-village.co.kr/k2/line","https://www.k-village.co.kr/k2/campaign","https://www.k-village.co.kr/k2/film","https://www.k-village.co.kr/k2/lookbook"],
"EIDER":["https://www.k-village.co.kr/eider","https://www.k-village.co.kr/eider/film","https://www.k-village.co.kr/eider/lookbook","https://www.k-village.co.kr/eider/is","https://www.k-village.co.kr/eider/line","https://www.k-village.co.kr/eider/line/XENIUM"],
"NATIONAL GEOGRAPHIC":["https://www.nstationmall.com/natgeo","https://www.nstationmall.com/event/planning/1155","https://www.nstationmall.com/event/planning/1157","https://nstationmall.com/event/planning/1124"],
"NEPA":["https://www.nplus.co.kr/promotion/list.asp","https://www.nplus.co.kr/brand/nepa/story.asp","https://www.nplus.co.kr/brand/nepa/csr.asp"],
"MAMMUT":["https://mammut.kr/stories.html","https://mammut.kr/board/promotion/list.html?board_no=8","https://mammut.kr/technologies.html","https://mammut.kr/responsibility.html"],
"MERRELL":["https://merrellkorea.co.kr/archive.html","https://merrellkorea.co.kr/history.html","https://merrellkorea.co.kr/history2.html"],
"SNOWLINE":["https://m.snowlinemall.com/pages/event/event-list.html"],
"COLUMBIA":["https://www.columbiakorea.co.kr/event/list.asp","https://www.columbiakorea.co.kr/event/261112/ohidown.asp","https://www.columbiakorea.co.kr/event/261109/winter_acc.asp","https://www.columbiakorea.co.kr/event/261029/efw.asp","https://www.columbiakorea.co.kr/montrail/montrail.asp"],
"HOKA":["https://brand.naver.com/hoka"],
"PATAGONIA":["https://www.patagonia.co.kr/stories/main","https://www.patagonia.co.kr/stories/all_films","https://www.patagonia.co.kr/stories/articleDetail/276","https://www.patagonia.co.kr/stories/articleDetail/255","https://www.patagonia.co.kr/stories/articleDetail/151","https://www.patagonia.co.kr/stories/filmDetail/275","https://www.patagonia.co.kr/stories/filmDetail/265","https://www.patagonia.co.kr/stories/filmDetail/264","https://www.patagonia.co.kr/stories/all","https://www.patagonia.co.kr/quality/main","https://www.patagonia.co.kr/stories/filmDetail/260","https://www.patagonia.co.kr/stories/filmDetail/273","https://www.patagonia.co.kr/stories/filmDetail/274","https://www.patagonia.co.kr/stories/filmDetail/272","https://www.patagonia.co.kr/stories/all/003001001000000","https://www.patagonia.co.kr/innovation-unusual/main","https://www.patagonia.co.kr/50/main","https://www.patagonia.co.kr/sports/community/003007001000000"],
"SNOW PEAK":["https://www.snowpeak.co.kr/event-page/2026/lookBook2/apparel.html","https://www.snowpeak.co.kr/event-page/2026/lookBook/apparel.html"],
"MONTBELL":["https://www.montbell.co.kr/shop/event.php","https://www.montbell.co.kr/shop/event_latest.php","https://www.montbell.co.kr/shop/event.php?ev_id=1792636687"],
"MILLET":["https://www.millet.co.kr/front/main","https://www.millet.co.kr/front/etc/brand_story","https://www.millet.co.kr/front/promo/pstories/196","https://www.millet.co.kr/front/promo/pstories/154","https://www.millet.co.kr/front/promo/pstories/161"]}
ALTERNATE={"HOKA":"brand.naver.com","K2":"k-village.co.kr","EIDER":"k-village.co.kr","NATIONAL GEOGRAPHIC":"nstationmall.com","SNOW PEAK":"snowpeak.co.kr"}
NATGEO_CURATED_PATHS={"/event/planning/1155","/event/planning/1157","/event/planning/1124"}
PAGE_LIMIT=int(os.getenv("CONTENT_PAGE_LIMIT","120"))
PRIORITY_PAGE_LIMIT=int(os.getenv("CONTENT_PRIORITY_PAGE_LIMIT","180"))
BRAND_SECONDS=int(os.getenv("CONTENT_BRAND_SECONDS","1200"))
SCROLL_LIMIT=int(os.getenv("CONTENT_SCROLL_LIMIT","80"))
SCROLL_STABLE_LIMIT=4
FRONTIER_LIMIT=5000
LEDGER_LIMIT=5000
DETAIL_REVISIT_HOURS=24
SITEMAP_REFRESH_HOURS=24
VISUAL_EVIDENCE_LIMIT=int(os.getenv("CONTENT_VISUAL_EVIDENCE_LIMIT","2"))
LEDGER_WINDOW_DAYS=30
SITEMAP_LIMIT=5
SITEMAP_BYTES=4*1024*1024
TARGET_SAMPLE=int(os.getenv("CONTENT_TARGET_SAMPLE","50"))
CATCHUP_PAGE_LIMIT=int(os.getenv("CONTENT_CATCHUP_PAGE_LIMIT","250"))
CATCHUP_SECONDS=int(os.getenv("CONTENT_CATCHUP_SECONDS","1500"))
MAINTENANCE_PAGE_LIMIT=int(os.getenv("CONTENT_MAINTENANCE_PAGE_LIMIT","40"))
MAINTENANCE_SECONDS=int(os.getenv("CONTENT_MAINTENANCE_SECONDS","300"))
CRAWL_SLICE_SECONDS=max(60,int(os.getenv("CONTENT_CRAWL_SLICE_SECONDS","165")))
PRIORITY_BRANDS={"TNF","HOKA","DECATHLON","BLACKYAK","NEPA","PATAGONIA","MILLET","NEW BALANCE","KOLON SPORT","COLUMBIA","SALOMON","K2","EIDER","DISCOVERY","NATIONAL GEOGRAPHIC","ARC'TERYX"}
UA="CSKMarketResearchBot"
OUT=Path("reports/competitor_content")
DISCOVERY_FILE=OUT/"public-discovery.json"
FRONTIER_FILE=OUT/"crawl-frontier.json"
KEY=re.compile(r"story|stories|pstories|journal|magazine|editorial|lookbook|campaign|explore|discover|collection|special|exhibition|technology|heritage|ambassador|athlete|community|responsibility|/event/view/|/event/planning/|/plan/|/lp(?:/|$)|/pages/|스토리|매거진|룩북|화보|캠페인|컬렉션|기술력|헤리티지|앰배서더|크리에이터|커뮤니티|기획전",re.I)
DENY=re.compile(r"(?:^|[/_.?&=])(?:login|logout|register|account|auth|cart|checkout|order|my|mypage|member|customer|support|faq|privacy|terms|admin|search|review|location|stores?|notice|raffle)(?:[/_.?&=]|$)",re.I)
PDP=re.compile(r"/(?:products?/(?:view|detail)(?:/|$)|products/[^/]+/?$|goods/view|goods_view)|[?&](?:goodsNo|productNo)=",re.I)
SALE=re.compile(r"black\s*friday|블랙프라이데이|쿠폰|할인|특가|세일|%\s*off|payback|페이백",re.I)
CTA=re.compile(r"더\s*보기|자세히|바로\s*가기|제품\s*보기|컬렉션\s*보기|shop\s*now|learn\s*more|read\s*more|explore",re.I)
TYPES={"LOOKBOOK":r"lookbook|룩북|화보|스타일링","PRODUCT_STORY":r"technology|고어텍스|방수|보온|기술력|테크니컬|innovation|insulation|단열|기능성","AMBASSADOR":r"ambassador|athlete|앰배서더|애슬릿|크리에이터","COMMUNITY":r"community|클럽|커뮤니티|워크숍|클리닉","BRAND_STORY":r"heritage|history|스토리|헤리티지|환경|지속가능|history|innovation","CAMPAIGN":r"campaign|캠페인|collection|컬렉션|capsule|캡슐"}
TPO={"하이킹":r"hiking|trekking|등산|하이킹|트레킹","캠핑":r"camping|캠핑","러닝":r"running|trail run|러닝|마라톤","설상":r"ski|snowboard|스키|스노보드","데일리":r"daily|urban|city|도심|일상|데일리","여행":r"travel|여행","낚시":r"fishing|angler|피싱|낚시","클라이밍":r"climb|boulder|클라이밍|암벽|등반","백패킹":r"backpacking|백패킹","사이클링":r"cycling|사이클|자전거","워터스포츠":r"water sport|kayak|surfing|수상스포츠|카약|서핑","트레일러닝":r"trail.?running|트레일.?러닝"}
USP={"보온":r"warmth|insulat|보온|체온","방수":r"waterproof|방수|고어텍스|gore.tex","경량":r"lightweight|경량|가벼운","내구성":r"durab|내구성","통기성":r"breathab|통기|투습","지속가능성":r"recycl|재활용|지속가능"}
# Remove analytics-only query parameters without dropping campaign/content identifiers.
# Keep functional query keys (e.g. gb, no, cate) for distinct editorial pages.
TRACKING_QUERY=re.compile(r"^(?:utm_[a-z0-9_]+|gclid|dclid|gbraid|wbraid|fbclid|msclkid|igshid|mc_cid|mc_eid|_ga|_gl|ref|tracking)$",re.I)
def canonical(u):
    """Normalize editorial URLs without collapsing distinct campaign identifiers.

    Reject malformed authorities before the URL can enter the frontier ledger.
    """
    p = urlsplit(u)
    hostname = p.hostname
    port = p.port  # ValueError on malformed ports; fail closed.
    if p.scheme.lower() != "https" or not hostname or port not in (None, 443):
        raise ValueError("canonical requires a public HTTPS URL on the standard port")
    if p.username is not None or p.password is not None:
        raise ValueError("canonical requires an unauthenticated URL")
    q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
         if not TRACKING_QUERY.fullmatch(k)]
    authority = hostname.lower()
    if ":" in authority:
        authority = "[" + authority + "]"
    return urlunsplit(("https", authority, p.path or "/", urlencode(sorted(q)), ""))
def host(u): return (urlsplit(u).hostname or "").removeprefix("www.").removeprefix("m.")
def official_host(brand,u):
    return host(u)==host(ROOTS[brand]) or bool(ALTERNATE.get(brand) and host(u)==ALTERNATE[brand])
def allowed_link(brand,u):
    # Malformed authorities (including invalid ports/IPv6) must fail closed.
    # This is a public-source crawler, never a general-purpose URL fetcher.
    try:
        p=urlsplit(u)
        hostname=p.hostname
        port=p.port
    except (ValueError, TypeError):
        return False
    return bool(p.scheme=="https" and hostname and port in (None,443)
                and not p.username and not p.password and not p.fragment
                and official_host(brand,u)
                and not DENY.search(p.path+"?"+p.query))
def scoped(brand,u):
    p=urlsplit(u); path=p.path.lower()
    if re.search(r"/product/(?:detail|list|search)\.html|/product/[a-z0-9][^/]+/\d+/?$|/product/[A-Z0-9]{5,}/?$",p.path,re.I): return False
    if not allowed_link(brand,u): return False
    if re.search(r"/(?:sign-in|sign-up|image_zoom[^/]*|personal-information-collection-statement[^/]*|service/company)(?:/|\.html|$)",path,re.I): return False
    if re.search(r"(?:^|&)(?:product_no|goodsNo|productNo|styleCode)=",p.query,re.I): return False
    if brand=="HOKA":
        return (host(u)=="linktr.ee" and path.rstrip("/")=="/hoka_korea") or (host(u)=="brand.naver.com" and path.rstrip("/")=="/hoka")
    if brand=="DECATHLON": return path=="/" or (path.startswith("/s/") and not re.search(r"membership|shipping|payment|return|refund|help|contact|store",path,re.I))
    if host(u)==ALTERNATE.get(brand):
        if brand=="SNOW PEAK": return (path.startswith("/event-page/") and ("/lookbook" in path or "/takibidai/" in path) and path.endswith(".html")) or path.rstrip("/") in ("/events","/events/detail")
        if brand=="NATIONAL GEOGRAPHIC" and path.rstrip("/") in NATGEO_CURATED_PATHS: return True
        prefix={"K2":"/k2","EIDER":"/eider","NATIONAL GEOGRAPHIC":"/natgeo"}[brand]
        return path.rstrip("/")==prefix or path.startswith(prefix+"/") and not PDP.search(u)
    if PRODUCT_PATH.search(u): return False
    if brand=="COLUMBIA": return path.startswith(("/main/","/event/","/promotion/","/exhibition/","/about/","/technology/","/pc/technology/","/montrail/"))
    if brand=="KOLON SPORT": return path.startswith("/brands/kolonsport")
    if brand=="BLACKYAK":
        return (path.rstrip("/") in ("/blackyak","/story/blackyak") or path.startswith(("/blackyak/","/story/blackyak/")) or (path=="/_container/exh_list.php" and dict(parse_qsl(p.query)).get("hta_brand")=="blackyak")) and not PDP.search(u)
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
    if not valid_title(title) or (SALE.search(title) and not (brand=="PATAGONIA" and placement=="editorial_page" and re.search(r"/stories/(?:articleDetail|filmDetail)/",source,re.I) and re.search(r"don.t buy this jacket|이 재킷을 사지 마세요",title,re.I))) or re.fullmatch(r"(?:home|main|shop|공식몰|공식 온라인스토어|메인|더보기|new arrivals|best sellers)",title,re.I): return None
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
    scope="official_brand_editorial" if (brand=="SNOW PEAK" and host(source)=="snowpeak.co.kr") or (brand=="HOKA" and host(source)=="linktr.ee") else "official_mall"
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
const genericTitle=s=>!s||/^(?:event|events|promotion|promotions|all promotions|campaign|collection|story|stories|기획전|이벤트|컬렉션|공식몰|공식 온라인스토어|COLUMBIA|MILLET|PATAGONIA|DISCOVERY|디스커버리 익스페디션)$/i.test(clean(s));
const titleCandidates=[articleRoot?.querySelector('h1,h2')?.innerText,document.querySelector('meta[property="og:title"]')?.content,document.title,...heads.map(h=>h.innerText)].map(s=>clean(s).replace(/\s*[|]\s*(?:컬럼비아.*|COLUMBIA|MILLET|PATAGONIA|DISCOVERY)\s*$/i,'').replace(/^디스커버리 익스페디션\s*-\s*/,''));
const rootTitle=()=>titleCandidates.find(s=>!genericTitle(s)&&!/자주 묻는 질문|개인정보|stay connected|고객 서비스|로그인|회원가입/i.test(s))||clean(document.title);

const block=h=>{let e=h.parentElement;for(let i=0;i<3&&e.parentElement&&e!==main;i++){if(images(e).length&&e.querySelector('a[href]'))break;const next=e.parentElement;if(next===main||next===document.body||next.querySelectorAll('h1,h2,h3').length>2||clean(next.innerText).length>1800)break;e=next;}const evidence=audit(e);return {title:clean(h.innerText),summary:clean(e.innerText).slice(0,500),image:images(e)[0]||null,image_details:imageDetails(e),body_evidence:clean(e.innerText).slice(0,2500),link_details:linkDetails(e),links:Array.from(e.querySelectorAll('a[href]')).map(a=>a.href).slice(0,8),cta:evidence.cta_labels,page_metrics:evidence,evidence_scope:'local_block'}};
const blocks=heads.map(block);
for(const a of main.querySelectorAll('a[href] img')){if(!visible(a)||a.getBoundingClientRect().width<400||a.getBoundingClientRect().height<120||!clean(a.alt))continue; const p=a.closest('a');blocks.push({title:clean(a.alt),summary:clean(p.innerText),image:a.currentSrc||a.src,image_details:[{url:a.currentSrc||a.src,alt:a.alt,width:a.naturalWidth,height:a.naturalHeight,method:'rendered_image'}],link_details:linkDetails(p),links:[p.href],cta:audit(p).cta_labels,page_metrics:audit(p),evidence_scope:'local_block'});if(blocks.length>=50)break;}
const og=k=>document.querySelector('meta[property="'+k+'"]')?.content;
return {title:clean(heads[0]?.innerText||og('og:title')||document.title),body:clean(main.innerText).slice(0,10000),blocks,links,article:{title:rootTitle(),summary:clean(og('og:description')||editorialRoot.innerText).slice(0,500),description:clean(og('og:description')),og_title:clean(og('og:title')),body_evidence:clean(editorialRoot.innerText).slice(0,2500),image:og('og:image')||images(editorialRoot)[0]||null,image_details:imageDetails(editorialRoot),link_details:linkDetails(editorialRoot),links:linkDetails(editorialRoot).map(x=>x.url).slice(0,30),cta:linkDetails(editorialRoot).filter(x=>x.label).map(x=>x.label).slice(0,30),date_evidence:dateEvidence,page_metrics:audit(editorialRoot),evidence_scope:articleRoot?'editorial_root':'page_main'}};
}"""
def unique_urls(urls,accept):
    out={}
    for u in urls:
        if not isinstance(u,str): continue
        try:
            v=canonical(u)
            if accept(v): out.setdefault(v,None)
        except (ValueError,TypeError): continue
    return list(out)

SCROLL_STATE=r"""() => {
 const root=document.scrollingElement||document.documentElement;
 const links=[];
 for(const a of document.querySelectorAll('a[href],[data-href],[data-url]')){
   const r=a.getBoundingClientRect();if(r.width<=0||r.height<=0||getComputedStyle(a).visibility==='hidden')continue;
   const raw=[a.getAttribute('href'),a.getAttribute('data-href'),a.getAttribute('data-url')].find(v=>v&&/^(?:https:\/\/|\/[^/]|\.\.?\/|\?)/i.test(v))||'';
   if(!/^(?:https:\/\/|\/[^/]|\.\.?\/|\?)/i.test(raw))continue;
   try{const url=new URL(raw,location.href);if(url.protocol!=='https:'||url.origin!==location.origin)continue;
     links.push({url:url.href,label:(a.innerText||a.getAttribute('aria-label')||a.title||a.querySelector('img')?.alt||'').replace(/\s+/g,' ').trim().slice(0,180),pagination:a.rel==='next'||!!a.closest('[class*="pagination" i],[class*="paging" i],[aria-label*="pagination" i]')});
   }catch{}
 }
 const imgs=Array.from(document.images).filter(i=>i.complete&&i.naturalWidth>=400).length;
 return {height:root.scrollHeight,top:root.scrollTop,bottom:root.scrollTop+root.clientHeight>=root.scrollHeight-12,imgs,links};
}"""
async def explore_scroll(page, deadline, listing=False, archive_capture=None):
    # Record links during scrolling, including links later removed by lazy/virtualized DOM.
    observed={};stable=0;prev=None;steps=0;more_clicks=0;stop="scroll_budget"
    until=min(deadline,asyncio.get_running_loop().time()+(100 if listing else 45))
    while steps<SCROLL_LIMIT and asyncio.get_running_loop().time()<until:
        state=await page.evaluate(SCROLL_STATE)
        new=0
        for link in state["links"]:
            u=link["url"]
            if u not in observed: observed[u]=link;new+=1
            elif link.get("pagination"): observed[u]["pagination"]=True
        if archive_capture and (steps%4==0 or state["bottom"] or new):
            try: archive_capture(await page.locator("body").inner_text(timeout=2000))
            except Exception: pass
        signature=(state["height"],state["imgs"],len(observed))
        stable=stable+1 if state["bottom"] and signature==prev and new==0 else 0
        prev=signature
        if stable>=SCROLL_STABLE_LIMIT:
            # Only a normal, visible public list expansion control. No forced/private actions.
            if listing and more_clicks<24:
                btn=page.get_by_role("button",name=re.compile(r"^(?:콘텐츠\s*더\s*보기|기획전\s*더\s*보기|더\s*보기|더\s*불러오기|더\s*가져오기|load more|show more|view more)$",re.I)).first
                try:
                    if await btn.count() and await btn.is_visible() and await btn.is_enabled():
                        await btn.click(timeout=2000);more_clicks+=1;stable=0
                        await page.wait_for_timeout(900);continue
                except Exception: pass
            stop="stable_bottom";break
        await page.evaluate("()=>window.scrollBy(0,Math.max(Math.floor(window.innerHeight*.85),450))")
        steps+=1
        await page.wait_for_timeout(700)
    else:
        if asyncio.get_running_loop().time()>=until: stop="time_budget"
    await page.evaluate("()=>window.scrollTo(0,0)")
    return list(observed.values()),{"steps":steps,"more_clicks":more_clicks,"stop":stop,"links":len(observed)}
def content_link(url,label,listing=False,pagination=False):
    if listing and pagination: return True
    return bool(KEY.search(url+" "+label) or (listing and re.search(r"event|promo|project|_exh|planning|/article/|campaignCode|/filmDetail/|story|journal|/special/",url,re.I)))

def brand_listing(brand,u):
    p=urlsplit(u); path=p.path.lower().rstrip("/"); q=dict(parse_qsl(p.query))
    if brand=="BLACKYAK":
        return (path=="/story/blackyak" and q.get("gb","list")!="detail") or (path=="/_container/exh_list.php" and q.get("hta_brand")=="blackyak") or (path=="/blackyak/_exh" and not q.get("no"))
    if brand in ("K2","EIDER"): return path in ("/"+brand.lower()+"/film","/"+brand.lower()+"/lookbook","/"+brand.lower()+"/campaign","/"+brand.lower()+"/line")
    if brand=="MONTBELL": return (path=="/shop/event.php" and not q.get("ev_id")) or path=="/shop/event_latest.php"
    if brand=="COLUMBIA": return path=="/event/list.asp"
    if brand=="DISCOVERY": return path=="/display/promotions"
    if brand=="MILLET": return path in ("/front/promo/pstories","/front/promo/event")
    if brand=="MAMMUT": return path in ("/stories.html","/technologies.html") or (path=="/board/promotion/list.html" and q.get("board_no")=="8")
    if brand=="MERRELL": return path=="/archive.html"
    if brand=="NEPA": return path=="/promotion/list.asp"
    if brand=="SNOWLINE": return path=="/pages/event/event-list.html"
    if brand=="TNF": return path in ("/lookbook","/event","/events")
    if brand=="PATAGONIA": return path in ("/stories/main","/stories/all","/stories/all_films","/stories/filmdetail","/stories/articledetail") or bool(re.fullmatch(r"/stories/(?:category|sports|all|all_films)(?:/[^/]+)?",path))
    return False

def film_archive_entries(brand,source,raw,checked):
    if brand not in ("K2","EIDER") or urlsplit(source).path.lower().rstrip("/")!="/"+brand.lower()+"/film": return []
    lines=[" ".join(v.split()) for v in raw.splitlines() if v.strip()]
    try: begin=next(i for i,v in enumerate(lines) if v.upper()=="FILM")
    except StopIteration: return []
    out={}
    for i in range(begin+2,len(lines)):
        season=re.fullmatch(r"(20\d{2})\s*(SS|FW)",lines[i],re.I)
        if re.match(r"^(?:K\.VILLAGE|고객센터|COPYRIGHT)",lines[i],re.I): break
        if not season: continue
        title=lines[i-1]
        if not valid_title(title) or re.fullmatch(r"FILM|LOOKBOOK|20\d{2}\s*(SS|FW)",title,re.I) or len(title)>100: continue
        season=season[1]+" "+season[2].upper()
        key=(title.casefold(),season)
        out.setdefault(key,{"brand":brand,"kind":"list_entry","title":title+" · "+season,"url":source,"season":season,"content_type":"FILM","source":"rendered_official_archive","listing_status":"제목·시즌 직접 관측","last_seen_at":checked,"published_at":None,"live_verified":False,"summary":"공식 목록에서 직접 관측; 개별 영상 및 발행일 미확인"})
    return list(out.values())

def staff_archive_entries(source,raw,checked):
    # A styling card is not a distinct brand campaign. Do not retain staff measurements or store identifiers.
    if urlsplit(source).path.rstrip("/").lower()!="/lookbook": return []
    if "STAFF GALLERY" not in raw.upper(): return []
    body=re.split(r"STAFF GALLERY",raw,flags=re.I)[-1]
    body=re.split(r"\babout us\b|고객지원실|개인정보처리방침",body,flags=re.I)[0]
    lines=[" ".join(v.split()) for v in body.splitlines() if v.strip()]
    out={}
    for i,line in enumerate(lines):
        if not re.match(r"^키\s*[:：]",line): continue
        end=next((j for j in range(i+1,min(i+8,len(lines))) if re.match(r"^착용\s*사이즈\s*[:：]",lines[j])),None)
        if not end or end<=i+1: continue
        title=lines[end-1]
        if not valid_title(title) or len(title)>120: continue
        if re.search(r"비노출|이염확인|테스트용|삭제 예정",title): continue
        color=re.search(r"색상\s*[:：]\s*([A-Za-z0-9_ -]+?)(?=\s*매장명\s*[:：]|$)",line)
        c=color[1].strip() if color else ""
        # Keep only a product/style + color key; not a claim of unique posts or unique people.
        k=hashlib.sha256((title.casefold()+"|"+c.casefold()).encode()).hexdigest()[:20]
        out.setdefault(k,{"brand":"TNF","kind":"list_entry","title":title+(" · "+c if c else "")+" · STAFF STYLE","url":source,"archive_id":k,"content_type":"STAFF_STYLE","source":"rendered_official_archive","listing_status":"제품·색상 직접 관측","last_seen_at":checked,"published_at":None,"live_verified":False,"summary":"공식 착장 목록 관측; 개별 게시물 URL·발행일 미확인"})
    return list(out.values())

def editorial_route(brand,u):
    p=urlsplit(u); path=p.path.lower(); q=dict(parse_qsl(p.query))
    if brand_listing(brand,u): return False
    if brand=="BLACKYAK" and (path.rstrip("/") in ("/blackyak/_exh","/story/blackyak") and q.get("no")): return True
    if brand=="NEW BALANCE" and re.search(r"/(?:nbmadehistory|nbrunningguide)\.action$",path): return True
    if brand=="MAMMUT" and (re.fullmatch(r"/technologie_\d+\.html|/responsibility\.html",path) or re.match(r"/project/[^/]+/\d+/?$",path) or (path=="/product/project.html" and q.get("cate_no")) or (path=="/board/promotion/read.html" and q.get("board_no")=="8")): return True
    if brand=="MERRELL" and path in ("/history.html","/history2.html"): return True
    if brand=="NEPA" and (path in ("/brand/nepa/story.asp","/brand/nepa/csr.asp") or (path=="/promotion/view.asp" and q.get("EventNo"))): return True
    if brand=="COLUMBIA" and (re.fullmatch(r"/event/\d{6}(?:_\d+)?/[^/]+\.asp",path) or (path=="/event/view.asp" and q.get("en")) or path=="/montrail/montrail.asp"): return True
    if brand in ("K2","EIDER") and (path=="/"+brand.lower()+"/is" or re.fullmatch(r"/"+brand.lower()+r"/(?:line|lookbook)/[^/]+/?",path)): return True
    if brand=="MONTBELL" and ((path=="/shop/event.php" and bool(q.get("ev_id"))) or (path in ("/shop/event_view.php","/shop/event_detail.php") and bool(q.get("ev_id")))): return True
    if brand=="MILLET" and re.fullmatch(r"/front/promo/(?:pstories|event)/\d+",path): return True
    if brand=="PATAGONIA" and (path.startswith("/our_footprint/") or path.rstrip("/") in ("/innovation-unusual","/innovation-unusual/main","/company-history","/50/main")): return True
    return bool(re.search(r"/(?:stories/(?:articledetail|filmdetail|view)/[^/]+|event/view/[^/]+/[^/]+|display/promotions/collection/[^/]+|blogs/[^/]+/[^/]+|special/\d+|journal/[^/]+|magazine/[^/]+|lookbook/[^/]+|campaign/[^/]+|pages/[^/]+|collections/[^/]+|s/[^/]+)",path,re.I))

def sitemap_entries(raw):
    if raw.startswith(b"\x1f\x8b"):
        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as f: raw=f.read(SITEMAP_BYTES+1)
    if len(raw)>SITEMAP_BYTES or b"<!DOCTYPE" in raw.upper() or b"<!ENTITY" in raw.upper(): return [],[]
    root=ET.fromstring(raw)
    kind=root.tag.rsplit("}",1)[-1]
    if kind not in ("sitemapindex","urlset"): return [],[]
    urls=[]
    for child in root:
        for loc in child:
            if loc.tag.rsplit("}",1)[-1]=="loc" and loc.text: urls.append(loc.text.strip())
    return (urls,[]) if kind=="sitemapindex" else ([],urls)

async def sitemap_discovery(session,policy,brand,source,deadline,blocked,checkpoint=None,checked=None):
    # Public robots-declared maps only, on an already successfully observed origin.
    # Never use a sitemap to circumvent denied page navigation.
    p=urlsplit(source); origin=p.scheme+"://"+p.netloc
    rp=policy.cache.get(origin)
    declared=list(rp.site_maps() or []) if rp else []
    checkpoint=checkpoint or {}
    checked=checked or datetime.now(timezone(timedelta(hours=9))).isoformat()
    age=None
    try: age=datetime.fromisoformat(checked)-datetime.fromisoformat(checkpoint.get("started_at",""))
    except (ValueError,TypeError): pass
    running=age is not None and timedelta(0)<=age<timedelta(days=7) and bool(checkpoint.get("pending"))
    fresh=age is not None and timedelta(0)<=age<timedelta(hours=SITEMAP_REFRESH_HOURS) and checkpoint.get("complete")
    pending=list(checkpoint.get("pending",[])) if running else [] if fresh else declared
    done=set(checkpoint.get("done",[])) if running or fresh else set()
    started_at=checkpoint.get("started_at") if running or fresh else checked
    seen=set(); found=[]; errors=[]
    def same_origin(u):
        v=urlsplit(u)
        return v.scheme=="https" and v.netloc==p.netloc and not v.username and not v.password
    while pending and len(seen)<SITEMAP_LIMIT and asyncio.get_running_loop().time()+5<deadline and p.netloc not in blocked:
        u=pending.pop(0)
        if not same_origin(u) or u in seen or u in done: continue
        seen.add(u)
        if not await policy.permitted(u):
            done.add(u);continue
        await asyncio.sleep(policy.delay(u))
        try:
            async with session.get(u,allow_redirects=False) as response:
                if response.status in (403,429):
                    blocked.add(p.netloc);errors.append({"url":u,"error":"sitemap HTTP "+str(response.status)+"; origin stopped"});pending.insert(0,u);break
                if response.status!=200:
                    if response.status in (408,500,502,503,504): pending.append(u)
                    else: done.add(u)
                    continue
                buf=bytearray()
                async for chunk in response.content.iter_chunked(65536):
                    buf.extend(chunk)
                    if len(buf)>SITEMAP_BYTES: break
                if len(buf)>SITEMAP_BYTES:
                    errors.append({"url":u,"error":"sitemap exceeds safe byte limit"});done.add(u);continue
                raw=bytes(buf)
            maps,urls=sitemap_entries(raw)
            done.add(u)
            pending.extend(v for v in maps if same_origin(v) and v not in seen and v not in done)
            found.extend(v for v in urls if same_origin(v) and scoped(brand,v) and (editorial_route(brand,v) or brand_listing(brand,v)))
        except (OSError,ValueError,ET.ParseError,asyncio.TimeoutError,aiohttp.ClientError) as e:
            errors.append({"url":u,"error":"sitemap "+type(e).__name__})
            if isinstance(e,(asyncio.TimeoutError,aiohttp.ClientError)): pending.append(u)
            else: done.add(u)
    pending=unique_urls([v for v in pending if v not in done],same_origin)[:FRONTIER_LIMIT]
    state={"pending":pending,"done":sorted(done)[:FRONTIER_LIMIT],"started_at":started_at,"complete":not pending}
    return unique_urls(found,lambda v:scoped(brand,v)),len(seen),errors,bool(pending),state

def recent_success(entry,checked):
    if entry.get("state")!="observed" or not entry.get("at"): return False
    try:
        delta=datetime.fromisoformat(checked)-datetime.fromisoformat(entry["at"])
        return timedelta(0)<=delta<timedelta(hours=DETAIL_REVISIT_HOURS)
    except (TypeError,ValueError): return False

def retry_cooldown(entry,checked):
    """Respect policy blocks and avoid hammering failed official URLs.

    This is not a bypass: blocked destinations stay blocked until revalidation.
    """
    hours={"policy_denied":24,"access_blocked":12,"not_found":168,"timeout":2,"error":1}
    state=entry.get("state");allowed_hours=hours.get(state)
    if not allowed_hours or not entry.get("at"): return False
    try:
        age=datetime.fromisoformat(checked)-datetime.fromisoformat(entry["at"])
        return timedelta(0)<=age<timedelta(hours=allowed_hours)
    except (TypeError,ValueError): return False


def coverage_audit(ledger,known,checked):
    # Denominator is known in-scope URLs, never a claim of full-site recall.
    cutoff=(datetime.fromisoformat(checked)-timedelta(days=LEDGER_WINDOW_DAYS)).date().isoformat()
    states={}
    for u in known:
        entry=ledger.get(u,{})
        state=entry.get("state") if str(entry.get("at",""))[:10]>=cutoff else None
        states[state or "unobserved"]=states.get(state or "unobserved",0)+1
    n=len(known);observed=states.get("observed",0)+states.get("partial_observed",0)
    return {"scope":"known_scoped_urls_30d","known_urls":n,"observed_urls":observed,
            "unobserved_urls":n-observed,"observed_pct":round(100*observed/n,1) if n else None,
            "states":states,"window_days":LEDGER_WINDOW_DAYS}

def visual_signature(im):
    from PIL import Image
    # Compare only rendered assets. Near matches are review leads, not automatic duplicate merges.
    rgb=im.convert("RGB")
    p=rgb.resize((9,8),Image.Resampling.LANCZOS).convert("L")
    values=list(p.getdata())
    bits=sum(int(values[y*9+x]>values[y*9+x+1])<<(y*8+x) for y in range(8) for x in range(8))
    return {"dhash":f"{bits:016x}","aspect":round(rgb.width/max(1,rgb.height),3),
            "pixel_sha256":hashlib.sha256((str(rgb.size)+":").encode()+rgb.tobytes()).hexdigest(),
            "method":"rendered_pixels"}

def take_next_url(queue,brand,scanned,prior_urls,ledger):
    # Reserve recurring slots for hub/pagination exploration; otherwise detail pages can starve the next archive page.
    if scanned>0 and scanned%5==0:
        for i,v in enumerate(queue):
            if brand_listing(brand,v) and v not in prior_urls and not ledger.get(v): return queue.pop(i)
        for i,v in enumerate(queue):
            if brand_listing(brand,v): return queue.pop(i)
    return queue.pop(0)

def coverage_plan(brand,observed_count,pending_count=0,columbia_count=0):
    # Bounded slices preserve time for every brand. Unfinished public URLs
    # remain in the frontier for scheduled deep-crawl continuation.
    sample_target=max(TARGET_SAMPLE,columbia_count,200 if brand=="TNF" else 100 if brand in {"BLACKYAK","PATAGONIA","DISCOVERY","K2","EIDER","MILLET","NEPA"} else 0)
    catchup=brand!="COLUMBIA" and observed_count<columbia_count
    focus=brand=="TNF" and observed_count<sample_target
    undercovered=observed_count<sample_target
    deep=undercovered or pending_count>0
    full_seconds=max(CATCHUP_SECONDS,BRAND_SECONDS) if catchup or focus else BRAND_SECONDS if deep else MAINTENANCE_SECONDS
    return {"sample_target":sample_target,"prior_sample":observed_count,
            "columbia_benchmark":columbia_count,
            "gap_to_columbia":max(0,columbia_count-observed_count),
            "below_columbia":catchup,"focus_brand":focus,"undercovered":undercovered,
            "page_limit":max(CATCHUP_PAGE_LIMIT,PRIORITY_PAGE_LIMIT) if catchup or focus else PRIORITY_PAGE_LIMIT if deep else MAINTENANCE_PAGE_LIMIT,
            "seconds":min(CRAWL_SLICE_SECONDS,full_seconds),
            "requested_deep_seconds":full_seconds,
            "crawl_mode":"incremental_deep_crawl"}

async def scan(browser,session,brand,checked,old,frontier=None,plan=None):
    plan=plan or coverage_plan(brand,0)
    root=ROOTS[brand]; page_limit=plan["page_limit"]; row={"brand":brand,"root":root,"pages_checked":0,"items":[],"errors":[]}
    same=lambda u: scoped(brand,u)
    # robots retrieval is origin-level; allow only the official host.
    policy=PolicyCache(session,UA,lambda u:allowed_link(brand,u))
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
    # Reserve most of the next run for unfinished discovery, rather than repeating the same first pages.
    seeds=unique_urls(queue,same)
    backlog=unique_urls((frontier or {}).get("pending",[]),same)
    # Spend the limited slice on unvisited public editorial URLs before revisiting
    # already-observed details. Keep the original order within each tier and
    # preserve the every-fifth-page listing discovery reservation below.
    prior_ledger=(frontier or {}).get("url_ledger",{})
    backlog.sort(key=lambda u: (0 if u not in prior_ledger else
                                1 if prior_ledger[u].get("state") in ("error","timeout","not_found") else
                                2 if prior_ledger[u].get("state") in ("observed","partial_observed") else 3))
    # One homepage and one main hub per slice; use the rest to clear prior frontier.
    queue=unique_urls(seeds[:2]+backlog+seeds[2:],same)
    ledger_cutoff=(datetime.fromisoformat(checked)-timedelta(days=LEDGER_WINDOW_DAYS)).date().isoformat()
    ledger={u:v for u,v in (frontier or {}).get("url_ledger",{}).items() if same(u) and isinstance(v,dict) and str(v.get("at",""))[:10]>=ledger_cutoff}
    known=set(queue)|set(ledger)
    refresh=set(seeds[:2])
    row["recent_skip_count"]=0
    def note(u,state):
        ledger[u]={"state":state,"at":checked};known.add(u)
    def skip_recent(u):
        entry=ledger.get(u,{})
        if retry_cooldown(entry,checked): return True
        return u not in refresh and editorial_route(brand,u) and recent_success(entry,checked)
    previous=sorted((x for x in old.values() if x.get("brand")==brand),key=lambda x:x.get("last_seen_at",""),reverse=True)
    prior_urls={canonical(v) for x in previous for v in (x.get("url"),x.get("source_url")) if isinstance(v,str) and same(v)}
    for x in previous:
        v=x.get("url") if x.get("placement")=="main_visual" else x.get("source_url")
        if isinstance(v,str) and same(v) and KEY.search(v) and v not in queue: queue.append(v)
        if len(queue)>=FRONTIER_LIMIT: break
    known.update(queue)
    seen=set();ids=set()
    row["discovered_urls"]=0;row["pages_attempted"]=0;row["pending_urls"]=0;row["budget_limit"]=page_limit
    row["coverage_plan"]=plan
    row["homepage_observed"]=False
    row["archive_entries"]=[]
    row["official_social_links"]=[]
    row["scroll_steps"]=0;row["load_more_clicks"]=0;row["scroll_limited_pages"]=0
    row["pagination_links"]=0;row["frontier_start"]=len(backlog);row["frontier_truncated"]=0
    row["sitemap_urls"]=0;row["sitemap_files_checked"]=0;row["sitemap_partial"]=False
    sitemap_origins=set()
    sitemap_state=dict((frontier or {}).get("sitemap_state",{}))
    deferred=[]
    deadline=asyncio.get_running_loop().time()+plan["seconds"]
    try:
        while queue and len(seen)<page_limit and asyncio.get_running_loop().time()<deadline:
            u=canonical(take_next_url(queue,brand,len(seen),prior_urls,ledger))
            if u in seen or not same(u): continue
            if skip_recent(u):
                row["recent_skip_count"]+=1;continue
            if origin(u) in blocked:
                deferred.append(u);continue
            seen.add(u)
            row["pages_attempted"]+=1
            if not await policy.permitted(u):
                reason=policy.reason(u)
                note(u,"policy_denied")
                row["errors"].append({"url":u,"error":reason})
                if re.search(r"HTTP (?:403|429)|CertificateError",str(reason)):
                    blocked.add(origin(u));deferred.append(u)
                continue
            try:
                resp=await page.goto(u,wait_until="domcontentloaded",timeout=18000)
                if resp and resp.status in (403,429):
                    blocked.update([origin(u),origin(page.url)]);raise RuntimeError("HTTP "+str(resp.status)+"; origin stopped")
                if not resp or resp.status>=400 or not same(page.url): raise RuntimeError("navigation HTTP "+str(resp.status) if resp and resp.status>=400 else "navigation failed or out of scope")
                await page.wait_for_timeout(1200)
                path_hint=urlsplit(page.url).path.lower()
                list_hint=u==canonical(root) or brand_listing(brand,page.url) or bool(re.search(r"list|archive|/special/?$|/campaign/?$|/stories/(?:main|all)|/blogs/[^/]+/?$|/events/?$|/promotion/?$",path_hint)) or any(e.get("kind")=="list" and e.get("brand")==brand and canonical(e.get("url",""))==u for e in locals().get("discovered",[]))
                archive_buffer={}
                archive_source=canonical(page.url)
                def capture_archive(raw):
                    entries=staff_archive_entries(archive_source,raw,checked) if brand=="TNF" else film_archive_entries(brand,archive_source,raw,checked)
                    for x in entries: archive_buffer[archive_key(x)]=x
                capture_enabled=(brand=="TNF" and path_hint.rstrip("/")=="/lookbook") or (brand in ("K2","EIDER") and path_hint.rstrip("/")=="/"+brand.lower()+"/film")
                scroll_links,scroll=await explore_scroll(page,deadline,list_hint,capture_archive if capture_enabled else None)
                row["scroll_steps"]+=scroll["steps"];row["load_more_clicks"]+=scroll["more_clicks"]
                row["scroll_limited_pages"]+=int(scroll["stop"]!="stable_bottom")
                data=await page.evaluate(EXTRACT)
                if re.search(r"access denied|captcha|접근이 제한",data["body"][:600],re.I):
                    blocked.add(origin(page.url));raise RuntimeError("access challenge; origin stopped")
                if len(data["body"])<100: raise RuntimeError("empty document")
                source=canonical(page.url)
                if brand=="NATIONAL GEOGRAPHIC" and urlsplit(source).path.rstrip("/") in NATGEO_CURATED_PATHS and not re.search(r"national\s*geographic|내셔널지오그래픽",data["body"]+" "+data["title"],re.I):
                    raise RuntimeError("shared storefront brand attribution missing")
                row["pages_checked"]+=1
                page_state="observed" if scroll["stop"]=="stable_bottom" else "partial_observed"
                note(u,page_state)
                note(source,page_state)
                if plan["undercovered"] and origin(source) not in sitemap_origins:
                    sitemap_origins.add(origin(source))
                    sm,checked_maps,errors,partial,sm_state=await sitemap_discovery(session,policy,brand,source,deadline,blocked,sitemap_state.get(origin(source)),checked)
                    sitemap_state[origin(source)]=sm_state
                    row["sitemap_urls"]+=len(sm);row["sitemap_files_checked"]+=checked_maps;row["errors"].extend(errors);row["sitemap_partial"]|=partial
                    known.update(sm)
                    queue.extend(v for v in sm if v not in seen and not skip_recent(v))
                path=urlsplit(source).path.lower().rstrip("/")
                home=u==canonical(root) or (host(source)==ALTERNATE.get(brand) and path in ("/k2","/eider","/natgeo"))
                if home:
                    row["homepage_observed"]=True
                    # Only official-site visible footer/social links, not model tags in copy.
                    try:
                        social=await page.evaluate("""()=>Array.from(document.querySelectorAll('footer a[href],[class*=footer] a[href],a[aria-label*=Instagram],a[title*=Instagram]')).map(a=>a.href)""")
                        for href in social:
                            u2=urlsplit(href)
                            h2=(u2.hostname or "").lower()
                            handle=u2.path.strip("/")
                            if h2 not in ("instagram.com","www.instagram.com") or not re.fullmatch(r"[A-Za-z0-9_.]{1,30}",handle) or handle.lower() in ("p","reel","explore","accounts","stories","about"): continue
                            if not any(a["handle"].lower()==handle.lower() for a in row["official_social_links"]):
                                row["official_social_links"].append({"handle":handle,"url":"https://www.instagram.com/"+handle+"/","source":source,"verification":"official_site_footer_candidate"})
                    except Exception: pass
                is_index=any(entry.get("brand")==brand and entry.get("kind")=="list" and canonical(entry.get("url",""))==source for entry in discovered) if 'discovered' in locals() else False
                listing=is_index or brand_listing(brand,source) or bool(re.search(r"/(?:list|category|search)(?:/|$)|/special$|/blogs/[^/]+$|/collections/?$|/stories/(?:main|all|filmDetail)$|/campaign$",path,re.I))
                if capture_enabled:
                    try: capture_archive(await page.locator("body").inner_text(timeout=3000))
                    except Exception: pass
                    row["archive_entries"].extend(archive_buffer.values())
                blocks=data["blocks"] if home else ([] if listing else [data["article"]])
                for b in blocks:
                    evidence=(b.get("title") or "")+" "+(b.get("summary") or "")
                    if home and not b.get("image") and not KEY.search(evidence): continue
                    if home and re.search(r"\d[\d,]*\s*원|₩\s*\d",evidence) and not (KEY.search(b.get("title","")) or (b.get("image_details") and len(b.get("summary",""))>30)): continue
                    if home and b.get("links") and all(PRODUCT_PATH.search(v) for v in b["links"]) and not b.get("image"): continue
                    if not home and not (editorial_route(brand,source) or KEY.search(source+" "+evidence) or tags(TYPES,evidence) or (brand=="BLACKYAK" and re.search(r"/blackyak/_(?:exh|event)/",urlsplit(source).path,re.I))): continue
                    x=item(brand,source,b,checked,old,"main_visual" if home else "editorial_page")
                    if x and x["id"] not in ids:
                        row["items"].append(x);ids.add(x["id"])
                        if home and x.get("url") and x["url"]!=source and same(x["url"]) and x["url"] not in seen:
                            known.add(x["url"])
                            if not skip_recent(x["url"]): queue.append(x["url"])
                        if x["images"] and sum(bool(i.get("visual_palette")) for i in row["items"])<VISUAL_EVIDENCE_LIMIT and asyncio.get_running_loop().time()+5<deadline:
                            # Sample pixels from an already rendered image, no additional image fetch.
                            # Keep two evidence screenshots per brand; store derived palette only for the rest.
                            handle=None
                            try:
                                from io import BytesIO
                                from PIL import Image
                                src=x["images"][0]["url"]
                                handle=await page.evaluate_handle("(url)=>Array.from(document.images).find(i=>(i.currentSrc||i.src)===url)||null",src)
                                el=handle.as_element()
                                if el and await el.is_visible():
                                    image_bytes=await el.screenshot(timeout=2500,animations="disabled")
                                    with Image.open(BytesIO(image_bytes)) as im:
                                        x["visual_signature"]=visual_signature(im)
                                        sample=im.convert("RGB");sample.thumbnail((64,64))
                                        colors=sample.quantize(colors=5).convert("RGB").getcolors(4096) or []
                                        x["visual_palette"]=["#%02x%02x%02x"%rgb for _,rgb in sorted(colors,reverse=True)[:5]]
                                        x["visual_palette_source"]="rendered_image_pixels"
                                    if sum(bool(i.get("image_evidence_file")) for i in row["items"])<2:
                                        target=Path("content-evidence")/(x["id"]+"-"+x["fingerprint"][:10]+".png")
                                        target.parent.mkdir(exist_ok=True)
                                        target.write_bytes(image_bytes);x["image_evidence_file"]=target.name
                            except Exception: pass
                            finally:
                                if handle: await handle.dispose()
                found=[]
                for link in [*scroll_links,*data["links"]]:
                    v=canonical(link["url"])
                    if not same(v) or v in seen: continue
                    if SALE.search(link["label"]) and not brand_listing(brand,v) and not (brand=="PATAGONIA" and re.search(r"/stories/(?:articleDetail|filmDetail)/",v,re.I)): continue
                    if editorial_route(brand,v) or brand_listing(brand,v) or content_link(v,link["label"],listing or list_hint,link.get("pagination",False)):
                        if link.get("pagination"): row["pagination_links"]+=1
                        known.add(v)
                        if not skip_recent(v): found.append(v)
                queue.extend(found)
                queue=list(dict.fromkeys(queue))
                # Explicit editorial routes outrank generic category and promotion hubs.
                queue.sort(key=lambda v: (0 if editorial_route(brand,v) and v not in prior_urls else 1 if brand_listing(brand,v) else 2 if v not in prior_urls else 3,ledger.get(v,{}).get("at","")))
                row["discovered_urls"]=max(row["discovered_urls"],len(seen)+len(queue))
                if len(queue)>FRONTIER_LIMIT:
                    row["frontier_truncated"]+=len(queue)-FRONTIER_LIMIT
                    queue=queue[:FRONTIER_LIMIT]
            except Exception as e:
                reason=str(e)[:300]
                note(u,"access_blocked" if origin(u) in blocked else "not_found" if "HTTP 404" in reason else "timeout" if "timeout" in reason.lower() else "error")
                row["errors"].append({"url":u,"error":reason})
                if origin(u) in blocked or "timeout" in reason.lower(): deferred.append(u)
            await asyncio.sleep(policy.delay(u))
    finally:
        await context.close()
    pending=unique_urls([*queue,*deferred],same)
    row["pending_urls"]=len(pending)
    row["_pending"]=pending[:FRONTIER_LIMIT]
    row["_sitemap_state"]=sitemap_state
    cutoff=(datetime.fromisoformat(checked)-timedelta(days=LEDGER_WINDOW_DAYS)).date().isoformat()
    ledger={u:v for u,v in ledger.items() if str(v.get("at",""))[:10]>=cutoff}
    known.update(pending)
    row["url_coverage"]=coverage_audit(ledger,known,checked)
    # Retain discovered-but-unvisited addresses as well as checked URLs across runs.
    for u in pending:
        if u not in ledger or ledger[u].get("state")=="unobserved": ledger[u]={"state":"unobserved","at":checked}
    row["_url_ledger"]=dict(sorted(ledger.items(),key=lambda kv:kv[1].get("at",""),reverse=True)[:LEDGER_LIMIT])
    row["url_coverage"]["ledger_truncated"]=max(0,len(ledger)-LEDGER_LIMIT)
    row["budget_exhausted"]=bool(queue and (len(seen)>=page_limit or asyncio.get_running_loop().time()>=deadline))
    row["discovery_complete"]=not pending and not row["errors"] and not row["scroll_limited_pages"] and not row["frontier_truncated"] and not row["sitemap_partial"]
    row["status"]="candidate" if row["items"] else "no_candidate" if row["pages_checked"] else "not_collected"
    return row
def archive_key(x):
    return (x.get("brand"),x.get("content_type"),x.get("archive_id") or str(x.get("title","")).casefold().strip())

def retain_archive_entries(previous,current,cutoff):
    seen={}
    for x in [*(previous or []),*(current or [])]:
        if (x.get("last_seen_at") or "")[:10]<cutoff: continue
        k=archive_key(x)
        old=seen.get(k)
        if old and (old.get("last_seen_at") or "")>(x.get("last_seen_at") or ""): continue
        seen[k]=dict(x)
    return list(seen.values())

def aggregate(rows,prior,checked,prior_brands=None):
    prior_brands={b['brand']:b for b in (prior_brands or [])}
    current=[x for b in rows for x in b["items"]]
    archive={x["id"]:x for x in prior}
    for x in current:
        old=archive.get(x["id"],{})
        if old.get("fingerprint")==x.get("fingerprint"):
            for field in ("visual_signature","visual_palette","visual_palette_source"):
                if not x.get(field) and old.get(field): x[field]=old[field]
        archive[x["id"]]=x
    cutoff=(datetime.fromisoformat(checked)-timedelta(days=30)).date().isoformat()
    today=datetime.fromisoformat(checked).date().isoformat()
    history=[x for x in archive.values() if x.get("last_seen_at","")[:10]>=cutoff]
    summary=[]
    for b in rows:
        hist=[x for x in history if x["brand"]==b["brand"]]
        known=[x for x in hist if x.get("published_at")]
        b["archive_entries_current"]=len({archive_key(x) for x in b.get("archive_entries",[])})
        b["archive_entries"]=retain_archive_entries(prior_brands.get(b["brand"],{}).get("archive_entries",[]),b.get("archive_entries",[]),cutoff)
        b["archive_entries_retained"]=len(b["archive_entries"])
        summary.append({k:v for k,v in b.items() if k not in ("items","_pending","_url_ledger","_sitemap_state")}|{"last_success_at":checked if b["pages_checked"] else prior_brands.get(b["brand"],{}).get("last_success_at") or max((x.get("last_seen_at") or "" for x in hist),default="") or None,"candidate_count":len(b["items"]),"newly_observed":sum(x["change"]=="newly_observed" for x in b["items"]),"changed":sum(x["change"]=="changed" for x in b["items"]),"discovered_30d":sum(cutoff<=x["first_seen_at"][:10]<=today for x in hist),"published_30d":sum(cutoff<=x["published_at"]<=today for x in known) if known else None,"unknown_publication":len(hist)-len(known)})
    return {"checked_at_kst":checked,"scope":"public official sites; editorial candidates, not verified publications","brands":summary,"items":current,"history":history,"coverage":{"target":len(rows),"accessible":sum(b["pages_checked"]>0 for b in rows),"with_candidates":sum(bool(b["items"]) for b in rows),"items":len(current),"with_rendered_images":sum(bool(x.get("images")) for x in current),"with_product_evidence":sum(bool(x.get("products")) for x in current),"with_person_evidence":sum(bool(x.get("influencer")) for x in current),"with_confirmed_publication_date":sum(bool(x.get("published_at")) for x in current),"with_conflicting_publication_dates":sum(x.get("publication_status")=="conflicting" for x in current),"official_mall_brands":len({x["brand"] for x in current if x.get("source_scope")=="official_mall"}),"official_brand_editorial_brands":len({x["brand"] for x in current if x.get("source_scope")=="official_brand_editorial"}),"archived_image_count":sum(bool(x.get("image_evidence_file")) for x in current)}}
def self_test():
    assert canonical("https://www.byn.kr/story/blackyak/?gb=detail&no=1894&utm_medium=cpc&fbclid=abc")=="https://www.byn.kr/story/blackyak/?gb=detail&no=1894"
    assert canonical("https://www.byn.kr/story/blackyak/?no=1894&gb=detail")=="https://www.byn.kr/story/blackyak/?gb=detail&no=1894"
    assert canonical("https://www.byn.kr/story/blackyak/?gb=detail&no=1895")!=canonical("https://www.byn.kr/story/blackyak/?gb=detail&no=1894")
    assert allowed_link("TNF","https://www.thenorthfacekorea.co.kr/robots.txt")
    assert not allowed_link("TNF","https://www.thenorthfacekorea.co.kr:8443/")
    assert not allowed_link("TNF","https://www.thenorthfacekorea.co.kr/story#private")
    assert not allowed_link("TNF","https://www.thenorthfacekorea.co.kr:invalid/")
    assert not allowed_link("TNF","https://[::1/")
    assert not allowed_link("TNF","https://www.thenorthfacekorea.co.kr.evil.example/")
    assert canonical("https://arcteryx.co.kr/lp?utm_source=a&x=2#h")=="https://arcteryx.co.kr/lp?x=2"
    assert canonical("https://www.byn.kr:443/story?no=1894&utm_source=x")=="https://www.byn.kr/story?no=1894"
    for bad in ("http://www.byn.kr/story", "https://www.byn.kr:8443/story",
                "https://www.byn.kr:invalid/story", "https://user:pass@www.byn.kr/story"):
        try:
            canonical(bad)
        except ValueError:
            pass
        else:
            raise AssertionError("unsafe URL canonicalized: " + bad)
    assert scoped("HOKA","https://linktr.ee/hoka_korea")
    assert scoped("HOKA","https://brand.naver.com/hoka")
    assert not scoped("HOKA","https://brand.naver.com/other")
    assert not scoped("HOKA","https://linktr.ee/other")
    assert scoped("DECATHLON","https://www.decathlon.co.kr/s/running-campaign")
    assert not scoped("DECATHLON","https://www.decathlon.co.kr/p/test.html")
    assert not scoped("DECATHLON","https://www.decathlon.co.kr/c/test.html")
    assert brand_listing("PATAGONIA","https://www.patagonia.co.kr/stories/all_films")
    assert not brand_listing("PATAGONIA","https://www.patagonia.co.kr/stories/filmDetail/275")
    assert not scoped("NEW BALANCE","https://devapm-m.nbkorea.com/event/test.action")
    assert not scoped("BLACKYAK","https://www.byn.kr/other")
    assert scoped("BLACKYAK","https://www.byn.kr/story/blackyak/?gb=detail&no=1894")
    assert not scoped("BLACKYAK","https://www.byn.kr/story/blackyakkids/")
    assert not scoped("BLACKYAK","https://www.byn.kr/blackyakkids/_exh/?no=123")
    assert scoped("BLACKYAK","https://www.byn.kr/_container/exh_list.php?hta_brand=blackyak&st=1")
    assert brand_listing("BLACKYAK","https://www.byn.kr/story/blackyak/?cate=BRAND_CAMPAIGN&gb=list")
    assert not brand_listing("BLACKYAK","https://www.byn.kr/story/blackyak/?gb=detail&no=1894")
    assert brand_listing("BLACKYAK","https://www.byn.kr/_container/exh_list.php?hta_brand=blackyak&st=1")
    assert not brand_listing("BLACKYAK","https://www.byn.kr/blackyak/_exh/?no=1627")
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
    assert content_link("https://example.org/events?page=2","2",True,True)
    assert not content_link("https://example.org/about","2",False,False)
    assert unique_urls(["https://salomon.co.kr/pages/x?utm_source=a","https://salomon.co.kr/pages/x"],lambda u:scoped("SALOMON",u))==["https://salomon.co.kr/pages/x"]
    assert coverage_plan("TNF",0)["undercovered"]
    assert coverage_plan("SALOMON",TARGET_SAMPLE)["page_limit"]==MAINTENANCE_PAGE_LIMIT
    assert coverage_plan("SALOMON",TARGET_SAMPLE,10)["page_limit"]==PRIORITY_PAGE_LIMIT
    assert coverage_plan("PATAGONIA",0,0,13)["below_columbia"]
    assert coverage_plan("PATAGONIA",0,0,13)["gap_to_columbia"]==13
    assert coverage_plan("PATAGONIA",0,0,13)["page_limit"]>=PRIORITY_PAGE_LIMIT
    assert not coverage_plan("K2",13,0,13)["below_columbia"]
    assert not coverage_plan("COLUMBIA",0,0,13)["below_columbia"]
    assert coverage_plan("K2",TARGET_SAMPLE,0,TARGET_SAMPLE+1)["undercovered"]
    assert editorial_route("PATAGONIA","https://www.patagonia.co.kr/stories/filmDetail/275")
    assert editorial_route("PATAGONIA","https://www.patagonia.co.kr/stories/view/255")
    assert editorial_route("PATAGONIA","https://www.patagonia.co.kr/innovation-unusual/")
    assert editorial_route("BLACKYAK","https://www.byn.kr/story/blackyak/?gb=detail&no=1894")
    assert not editorial_route("PATAGONIA","https://www.patagonia.co.kr/stories/all_films")
    assert sitemap_entries(b'<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"><url><loc>https://example.org/story/1</loc></url></urlset>')==([],["https://example.org/story/1"])
    assert sitemap_entries(b'<!DOCTYPE urlset><urlset/>')==([],[])
    films=film_archive_entries("K2","https://www.k-village.co.kr/k2/film","NAV\nFILM\nALPHA\n2025 FW\nALPHA\n2025 FW\nBETA\n2026SS\nK.VILLAGE\nGAMMA\n2025 SS",now)
    assert len(films)==2 and films[1]["season"]=="2026 SS"
    assert brand_listing("EIDER","https://www.k-village.co.kr/eider/lookbook")
    assert brand_listing("EIDER","https://www.k-village.co.kr/eider/line")
    assert editorial_route("EIDER","https://www.k-village.co.kr/eider/line/XENIUM")
    assert editorial_route("K2","https://www.k-village.co.kr/k2/is")
    assert brand_listing("MONTBELL","https://www.montbell.co.kr/shop/event.php")
    assert brand_listing("MONTBELL","https://www.montbell.co.kr/shop/event.php?page=2")
    assert brand_listing("MONTBELL","https://www.montbell.co.kr/shop/event_latest.php")
    assert editorial_route("MONTBELL","https://www.montbell.co.kr/shop/event.php?ev_id=1792636687")
    assert not editorial_route("MONTBELL","https://www.montbell.co.kr/shop/event.php")
    assert shard_selection(list(range(12)),"2026-11-19T13:05:00+09:00",4)[0]==[2,6,10]
    assert shard_selection(list(range(12)),"2026-11-19T13:35:00+09:00",4)[0]==[3,7,11]
    assert shard_selection(list(range(12)),"2026-11-19T14:05:00+09:00",4)[0]==[0,4,8]
    assert shard_selection(list(range(12)),"2026-11-19T14:35:00+09:00",4)[0]==[1,5,9]
    styles=staff_archive_entries("https://www.thenorthfacekorea.co.kr/lookbook","STAFF GALLERY\n키 : 170cm 사이즈 : 100L 색상 : BLACK 매장명 : Example\nTEST JACKET\n착용 사이즈 : 100L\n10\n키 : 175cm 사이즈 : 100L 색상 : BLACK 매장명 : Example\nTEST JACKET\n착용 사이즈 : 100L\nabout us",now)
    assert len(styles)==1 and styles[0]["content_type"]=="STAFF_STYLE"
    assert "Example" not in str(styles) and "170cm" not in str(styles)
    assert editorial_route("MAMMUT","https://mammut.kr/technologie_02.html")
    assert editorial_route("NEPA","https://www.nplus.co.kr/promotion/view.asp?EventNo=747")
    assert brand_listing("MAMMUT","https://mammut.kr/board/promotion/list.html?board_no=8")
    assert brand_listing("COLUMBIA","https://www.columbiakorea.co.kr/event/list.asp")
    assert editorial_route("COLUMBIA","https://www.columbiakorea.co.kr/event/261112/ohidown.asp")
    assert editorial_route("COLUMBIA","https://www.columbiakorea.co.kr/event/view.asp?ct=&en=1088")
    assert scoped("COLUMBIA","https://www.columbiakorea.co.kr/montrail/montrail.asp")
    assert editorial_route("MILLET","https://www.millet.co.kr/front/promo/event/344")
    assert brand_listing("PATAGONIA","https://www.patagonia.co.kr/stories/all/003001001000000")
    q=["https://www.thenorthfacekorea.co.kr/event/view/eventCategoryNF/93320","https://www.thenorthfacekorea.co.kr/lookbook?page=3"]
    assert take_next_url(q,"TNF",5,set(),{}).endswith("page=3") and len(q)==1
    assert coverage_plan("TNF",17,0,13)["focus_brand"]
    assert coverage_plan("TNF",17,0,13)["sample_target"]==200
    assert coverage_plan("TNF",17,0,13)["seconds"]<=CRAWL_SLICE_SECONDS
    test_now="2026-11-22T08:30:00+09:00"
    assert retry_cooldown({"state":"policy_denied","at":test_now},test_now)
    assert not retry_cooldown({"state":"observed","at":test_now},test_now)
    assert not retry_cooldown({"state":"not_found","at":"2026-10-01T00:00:00+09:00"},test_now)
    example={b:coverage_plan(b,0,10,25) for b in ROOTS}
    frontier_example={b:{"pending":["https://example.test"]*10} for b in ROOTS}
    extra=choose_extra_catchup(list(ROOTS)[:12],list(ROOTS)[:3],example,frontier_example,{b:0 for b in ROOTS},test_now,1)
    assert len(extra)==1 and extra[0] not in list(ROOTS)[:3]
    blocked_root=canonical(ROOTS[extra[0]])
    frontier_example[extra[0]]["url_ledger"]={blocked_root:{"state":"policy_denied","at":test_now}}
    assert extra[0] not in choose_extra_catchup(list(ROOTS)[:12],list(ROOTS)[:3],example,frontier_example,{b:0 for b in ROOTS},test_now,2)
    assert coverage_plan("PATAGONIA",0,0,13)["requested_deep_seconds"]>=BRAND_SECONDS
    assert all(coverage_plan(b,0)["sample_target"]==100 for b in ("BLACKYAK","PATAGONIA","DISCOVERY","K2","EIDER","MILLET","NEPA"))
    assert len(retain_archive_entries(styles,styles,"2026-11-01"))==1
    assert len(retain_archive_entries(styles,[],"2026-11-19"))==0
    assert recent_success({"state":"observed","at":now},"2026-11-19T02:00:00+09:00")
    assert not recent_success({"state":"observed","at":now},"2026-11-20T02:00:00+09:00")
    assert not recent_success({"state":"policy_denied","at":now},now)
    assert not recent_success({"state":"partial_observed","at":now},now)
    audit=coverage_audit({"x":{"state":"observed","at":now},"y":{"state":"policy_denied","at":now}},{"x","y","z"},now)
    assert audit["observed_urls"]==1 and audit["unobserved_urls"]==2 and audit["observed_pct"]==33.3
    from PIL import Image
    uniform=Image.new("RGB",(16,16),(10,20,30))
    assert visual_signature(uniform)["dhash"]=="0000000000000000"
    assert visual_signature(uniform)["pixel_sha256"]!=visual_signature(Image.new("RGB",(16,16),(20,30,40)))["pixel_sha256"]
    print("content self-tests passed")
def shard_selection(brand_order,checked,shards):
    """Short official-brand sets are revisited in a two-hour rotation."""
    shards=max(1,int(shards))
    now=datetime.fromisoformat(checked)
    slot=(now.hour*2+now.minute//30)%shards
    return brand_order[slot::shards],slot

def choose_extra_catchup(brand_order,selected,plans,frontier,counts,checked,maximum=1):
    """Rotate one extra undercovered, policy-allowed official brand per run.

    Base four-shard coverage remains unchanged. Never retry a blocked root or
    ignore an access restriction simply to increase counts.
    """
    candidates=[]
    now=datetime.fromisoformat(checked)
    for brand in brand_order:
        if brand in selected or not plans[brand].get("undercovered"):continue
        pending=len(frontier.get(brand,{}).get("pending",[]))
        if pending<8:continue
        ledger=frontier.get(brand,{}).get("url_ledger",{})
        root_entry=ledger.get(canonical(ROOTS[brand]),{})
        if retry_cooldown(root_entry,checked) and root_entry.get("state") in ("policy_denied","access_blocked"):
            continue
        gap=max(0,plans[brand]["sample_target"]-counts.get(brand,0))
        candidates.append((brand,gap,pending))
    if not candidates or maximum<=0:return []
    candidates.sort(key=lambda x:(-x[1],-x[2],x[0]))
    # Rotate across the highest-priority backlog rather than repeatedly
    # hammering the same undercovered site every 30 minutes.
    pool=candidates[:min(8,len(candidates))]
    slot=(now.hour*2+now.minute//30)%len(pool)
    return [pool[(slot+i)%len(pool)][0] for i in range(min(maximum,len(pool)))]

async def main():
    checked=datetime.now(timezone(timedelta(hours=9))).isoformat()
    OUT.mkdir(parents=True,exist_ok=True)
    dest=OUT/"latest.json"
    previous=json.loads(dest.read_text()) if dest.exists() else {}
    prior=previous.get("history",[])
    old={x["id"]:x for x in prior}
    try: frontier=json.loads(FRONTIER_FILE.read_text()).get("brands",{})
    except (OSError,ValueError): frontier={}
    # Prioritize the deduplicated previous strategy sample if available; otherwise use recent direct observations.
    try:
        audit=json.loads((OUT/"strategy-summary.json").read_text())
        audit_brands=audit.get("brands",[])
        retained_basis=bool(audit_brands) and all(b.get("retained_unique_count") is not None for b in audit_brands)
        counts={b["brand"]:int(b.get("retained_unique_count" if retained_basis else "unique_count") or 0) for b in audit_brands}
    except (OSError,ValueError,TypeError,KeyError): counts={};retained_basis=False
    for brand in ROOTS:
        counts.setdefault(brand,len({x["id"] for x in prior if x.get("brand")==brand}))
    columbia_count=counts.get("COLUMBIA",0)
    plans={b:coverage_plan(b,counts[b],len(frontier.get(b,{}).get("pending",[])),columbia_count) for b in ROOTS}
    for plan in plans.values(): plan["sample_basis"]="retained_30d_observations" if retained_basis else "previous_snapshot_observations"
    order=sorted(ROOTS,key=lambda b:(counts[b]>=30,not plans[b]["focus_brand"],not plans[b]["below_columbia"],not plans[b]["undercovered"],counts[b],b not in PRIORITY_BRANDS,b))
    # Keep shard membership stable across runs; prioritize only inside each fixed shard.
    fixed_order=list(ROOTS)
    selected,shard_slot=shard_selection(fixed_order,checked,os.getenv("CONTENT_CRAWL_SHARDS","1"))
    priority_rank={brand:index for index,brand in enumerate(order)}
    selected.sort(key=lambda brand:priority_rank[brand])
    extra=choose_extra_catchup(
        fixed_order,selected,plans,frontier,counts,checked,
        maximum=max(0,min(2,int(os.getenv("CONTENT_EXTRA_CATCHUP_BRANDS","1")))))
    selected.extend(extra)
    # A recent 30-day Columbia archive does not imply the current crawl observed
    # Columbia. Recover an actual same-run owned sample before comparing tone.
    current_owned=int((audit.get("quality") or {}).get("owned_count") or 0) if "audit" in locals() else 0
    if current_owned==0 and "COLUMBIA" not in selected:
        selected.append("COLUMBIA")
    origin_locks={}
    def group(brand): return ALTERNATE.get(brand) or host(ROOTS[brand])
    async with aiohttp.ClientSession(headers={"User-Agent":UA},timeout=aiohttp.ClientTimeout(total=15)) as session:
        async with async_playwright() as p:
            browser=await p.chromium.launch(headless=True,**({"executable_path":os.environ["CRAWLER_CHROME"]} if os.environ.get("CRAWLER_CHROME") else {}))
            sem=asyncio.Semaphore(4)
            async def task(brand):
                try:
                    async with origin_locks.setdefault(group(brand),asyncio.Lock()):
                        async with sem:
                            result=await asyncio.wait_for(scan(browser,session,brand,checked,old,frontier.get(brand),plans[brand]),timeout=plans[brand]["seconds"]+60)
                except Exception as e:
                    result={"brand":brand,"root":ROOTS[brand],"pages_checked":0,"items":[],"status":"not_collected","coverage_plan":plans[brand],"errors":[{"error":str(e) or type(e).__name__}]}
                print(json.dumps({"brand":brand,"pages":result.get("pages_checked",0),"candidates":len(result.get("items",[])),
                    "pending":result.get("pending_urls"),"errors":len(result.get("errors",[])),
                    "mode":plans[brand]["crawl_mode"],"slice_seconds":plans[brand]["seconds"]},ensure_ascii=False),flush=True)
                return result
            print(json.dumps({"crawl_start":checked,"target_brands":len(order),
                "shard":shard_slot,"shards":max(1,int(os.getenv("CONTENT_CRAWL_SHARDS","1"))),
                "selected_brands":selected,"extra_catchup":extra,
                "slice_seconds":CRAWL_SLICE_SECONDS},ensure_ascii=False),flush=True)
            scanned_rows=await asyncio.gather(*(task(b) for b in selected))
            await browser.close()
    scanned={b["brand"]:b for b in scanned_rows}
    old_brands={b.get("brand"):b for b in previous.get("brands",[])}
    rows=[]
    for brand in ROOTS:
        if brand in scanned:
            rows.append(scanned[brand]);continue
        prior_brand=old_brands.get(brand,{})
        rows.append({"brand":brand,"root":ROOTS[brand],"pages_checked":0,"items":[],
            "status":"not_scheduled","coverage_plan":plans[brand],"errors":[],
            "archive_entries":[],"pending_urls":len(frontier.get(brand,{}).get("pending",[])),
            "url_coverage":prior_brand.get("url_coverage",{}),
            "shard_note":"이번 회차 미선택 · 최근 30일 관측은 별도 유지"})
    for b in rows:
        if "_pending" in b: frontier[b["brand"]]={"pending":b["_pending"],"url_ledger":b.get("_url_ledger",{}),"sitemap_state":b.get("_sitemap_state",{}),"checked_at_kst":checked}
    temp=FRONTIER_FILE.with_suffix(".tmp")
    temp.write_text(json.dumps({"schema_version":1,"brands":frontier},ensure_ascii=False,separators=(",",":")))
    temp.replace(FRONTIER_FILE)
    payload=aggregate(rows,prior,checked,previous.get('brands',[]))
    if not payload["coverage"]["accessible"]:
        (OUT/"failed-run.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2))
        raise SystemExit("No accessible brands; prior feed preserved")
    dest.write_text(json.dumps(payload,ensure_ascii=False,indent=2))
    print(json.dumps(payload["coverage"],ensure_ascii=False))
if __name__=="__main__":
    if "--self-test" in sys.argv: self_test()
    else: asyncio.run(main())
