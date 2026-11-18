// Exclusive primary intent; editorial format is a separate, multi-label dimension.
// URL names (event/collection/special) alone are not evidence of a sale.
export const PURPOSES=[["COMMERCIAL","상품 기획전"],["PROMOTION","할인·혜택 프로모션"],["EDITORIAL","브랜드 콘텐츠"],["HYBRID","콘텐츠형 기획전"],["COMMUNITY","커뮤니티·체험"],["UNCLASSIFIED","판단 보류"]];
export function classifyPurpose(x){
 const title=String(x.title||""),summary=String(x.summary||""),cta=(x.cta||[]).join(" / "),url=String(x.url||x.source_url||""),article=x.placement==="editorial_page",m=x.page_metrics||{},ev=[];
 const add=(n,r,t)=>{const z=String(t||"").match(r);if(z){ev.push(n+": "+z[0].slice(0,100));return true}return false};
 const global=/로그인.{0,45}회원가입|멤버십 혜택\s*>\s*얼리 윈터 프로모션|매장찾기.{0,80}공지사항/i.test(summary);
 const local=x.evidence_scope==="local_block"||x.evidence_scope==="editorial_root"||(!global&&title.length>3&&summary.toLowerCase().startsWith(title.toLowerCase()));
 const body=(article&&!global||local)?summary:"",text=title+" "+body;
 const promo=add("명시 혜택",/블랙\s*프라이데이|black\s*friday|쿠폰|페이백|구매\s*혜택|구매\s*금액대별|\d+\s*%\s*(?:off|할인|우대)|특가|특별한\s*혜택/i,title+" "+cta+" "+(local?body:""));
 const community=add("활동·참여",/워크숍|워크샵|클리닉|클래스|참가자\s*모집|참가\s*신청|크루\s*(?:모집|활동)|community\s*(?:event|program)|workshop|clinic|팝업\s*스토어|pop.up\s*store/i,text+" "+cta);
 const story=add("스토리·정보",/인터뷰|브랜드\s*(?:스토리|철학|역사)|헤리티지|개발\s*(?:스토리|과정)|제작\s*과정|비하인드|필드\s*테스트|착용\s*(?:후기|테스트)|직접\s*(?:입어본|체험)|가이드|매거진|저널|탐험기|산행기|지속\s*가능|interview|heritage|founding\s*stor|brand\s*story|our\s*story|about\s*us|sustainab|field\s*test|journal|gear\s*guide|since\s*(?:19|20)\d{2}/i,text);
 const storyRoute=add("스토리 전용 경로",/\/(?:editorial[-/]|blogs\/[^/]+\/|stories\/(?:[a-z-]+\/)?\d+|brand\/brandStory|brand\/nepa\/story|about\/history|page\/(?:our_heritage|who_we_are|forged_on_mt_hunter)|history2\.html)|\/(?:NBMADEHISTORY|NBRunningGuide)\.action|lookBook[^/]*\/[^/]+\.html/i,url);
 const tech=add("기능 설명",/보온|체열|발열|발수|방수|경량|통기|투습|내구|기능성|기술력|테크놀로지|인터체인지|옴니히트|omni.?heat|프리마로프트|polartec|고어텍스|gore.?tex|BOA[®™]?\s*핏|핏\s*시스템|소재\s*기술|설계|insulat|waterproof|lightweight|technology|디테일/i,text);
 const collection=add("제품 큐레이션",/컬렉션|collection|라인업|셀렉션|아이템\s*모음|상품\s*기획전|상품\s*모아보기|캡슐|capsule|리미티드\s*에디션|limited\s*edition|신상품|시즌\s*오픈|season\s*open|stylebook|룩북|lookbook/i,text);
 const product=add("상품·착장 설명",/다운\s*자켓|다운자켓|구스다운|패딩|플리스|재킷|자켓|아우터|방한화|백팩|코트|텐트|풀오버|레이어|슈즈|착장|스타일링|\b(?:down|jacket|outerwear|backpack|fleece|shoes|gloves)\b/i,text);
 const direct=add("상품·상품모음 연결",/\/product\/(?:view\.asp|productDetail\.action|[a-z0-9_-]{5,})(?:[/?#]|$)|\/products\/(?:view\/\d+|category\/\d+)|\/goods\/catalog[?]|\/goods\/category\/|\/collections\/[^/?#]+|\/product\/activity\.asp|\/lifestyle\/lifestyle-type\//i,url);
 const buy=add("구매 CTA",/상품\s*보기|제품\s*보기|구매하기|쇼핑하기|shop\s*now|shop\s*(?:men|women|the\s*collection)|buy\s*now|add\s*to\s*cart/i,cta+" "+(local?body:""));
 const read=add("읽기 CTA",/read\s*article|read\s*more|learn\s*more|스토리\s*보기|자세히\s*읽기|view\s*lookbook/i,cta+" "+(local?body:""));
 const products=Math.max((article||local?(x.products||[]).filter(z=>z?.name&&z?.url).length:0),Number(m.product_links||0));
 const prices=Number(m.price_mentions||0)||((local?body:"").match(/\d{1,3}(?:,\d{3})+\s*원/g)||[]).length;
 if(products)ev.push("관측 상품 연결 "+products+"개");if(prices)ev.push("관측 가격 "+prices+"개");
 const campaign=/\/(?:Brands\/KOLONSPORT\/Special\/\d+|event\/view\/eventCategoryNF\/\d+|event\/\d{6}(?:_\d+)?\/[^/?#]+\.asp|promotion\/view\.asp|etc\/collection\.action|app\/planning2\/views\/|display\/promotions\/collection\/\d+|20\d{2}[a-z-]+\/)/i.test(url);
 const commerce=direct||buy||products>0||prices>=2, narrative=story||storyRoute||read||tech;
 let code="UNCLASSIFIED",confidence="insufficient";
 if(promo){code="PROMOTION";confidence="evidence_based"}else if(community){code="COMMUNITY";confidence="evidence_based"}else if(narrative&&commerce){code="HYBRID";confidence="evidence_based"}else if(story||storyRoute||read){code="EDITORIAL";confidence="evidence_based"}else if(commerce){code="COMMERCIAL";confidence="evidence_based"}else if(campaign&&(tech||product||collection)){code=tech?"HYBRID":"COMMERCIAL";confidence="inferred";ev.push("기획 상세 경로 + 제품 문구; 실제 판매 구조 재확인") }else if(tech||product&&local&&body.length>title.length+12){code=tech?"EDITORIAL":"COMMERCIAL";confidence="inferred";ev.push("제품 문구 기반 추정")}else if(/상품\s*기획전|쇼핑\s*기획전|상품\s*모아보기/i.test(title)){code="COMMERCIAL";confidence="evidence_based";ev.push("제목에 상품 기획전 명시")}
 else if(collection&&(article||local)){code="COMMERCIAL";confidence="inferred";ev.push("컬렉션 문구 기반 추정; 구매 구조 미확인")}
 else if(/\/etc\/collection\.action\?[^#]*collectionIdx=\d+|\/Brands\/KOLONSPORT\/Special\/\d+|\/event\/view\/eventCategoryNF\/\d+|\/(?:outdoor|lifestyle)\/[^?]*collection\/[^/?]+/i.test(url)&&title.length>=5&&(x.thumbnail||body.length>=12)){code="COMMERCIAL";confidence="inferred";ev.push("확인된 브랜드별 큐레이션 상세 경로 및 소재; 실제 목적 검토 필요")}
 else if(/\b(?:LOOKBOOK|STYLEBOOK|EDITORIAL|STORY|JOURNAL|EXPERIENCE|EXPEDITION|FILM)\b|룩북|스토리|착장|정은채와 함께|\d{4}.*(?:역사|이어온)/i.test(title)){code="EDITORIAL";confidence="inferred";ev.push("콘텐츠 제목 형식 기반 추정")}
 else if(/\b(?:DOWN|PUFFER|JACKET|OUTER|FLEECE|PARKA|WARMTH|BACKPACK|TENT|WINTER|SHOES)\b|따뜻함|온기|다운|재킷|자켓|아우터|플리스|백팩|방한|소재|가벼움/i.test(title)&&title.length>=5&&(x.thumbnail||local)){code="COMMERCIAL";confidence="inferred";ev.push("개별 제품·시즌 소개 제목; 판매 구조 미확인")}
 if(code==="UNCLASSIFIED")ev.push("개별 콘텐츠 근거 부족");
 return {code,confidence,evidence:[...new Set(ev)],commercial_evidence:commerce,promotion_evidence:promo};
}

// Offline content strategy analysis; no network.
export function analyze(feed){
 const norm=s=>String(s??"").normalize("NFKC").replace(/\s+/g," ").trim().toLowerCase();
 const canon=s=>{const m=String(s??"").match(/^https:\/\/([^/?#]+)([^?#]*)(?:\?([^#]*))?/i);if(!m)return "";const p=(m[2]||"/").replace(/\/$/,"");const q=(m[3]||"").split("&").filter(x=>x&&!/^(utm_[^=]*|gclid|fbclid|ref|tracking|sort|inStock)=/i.test(x)).sort().join("&");return "https://"+m[1].toLowerCase()+p+(q?"?"+q:"")};
 const detail=/\/(?:special\/\d+|event\/view\/[^/]+\/\d+|event\/planning\/\d+|exhibition\/[^/]+|display\/promotions\/collection\/\d+|blogs\/[^/]+\/[^/]+|stories\/(?:filmDetail|detail)\/\d+|promo\/pstories\/\d+|campaign\/[^/]+|lookbook[^/]*\/[^/]+)(?:\/|[?]|$)|[?&](?:no|eventNo)=\d+/i;
 const noise=/^(?:\d+|(?:(?:slider|banner|img|image)[ _-]*\d+|\d+[ _-]*(?:slider|banner|img|image))|weekly best items|best sellers|new arrivals|ranking|home|main|official online store|전체 상품|상품 목록|추천 상품|관련 상품|베스트 상품)$/i;
 const index=/\/(?:list|category|search|ranking)(?:\/|$)|\/special\/?$|\/blogs\/[^/]+\/?$|\/stories\/(?:main|all)\/?$/i;
 const rules=[
 ["TPO","TPO·스타일링",/하이킹|트레킹|등산|캠핑|러닝|여행|도심|일상|데일리|스타일링|hiking|trekking|camping|running|urban|daily|styling/i,"도심·트레일·여행 상황별 착장과 제품 CTA 연결","콘텐츠→PDP 클릭률"],
 ["TECH","제품 기능·기술",/경량|방수|보온|투습|통기|내구|기술력|고어텍스|프리마로프트|기능성|체열|옴니히트|인터체인지|omni.?heat|BOA.{0,4}핏|핏 시스템|waterproof|lightweight|insulation|technology|breathab|gore.?tex/i,"27SS 제품 스펙을 실제 사용 테스트와 짧은 비주얼로 증명","PDP→Cart"],
 ["PEOPLE","인물·크리에이터",/앰배서더|애슬릿|크리에이터|인플루언서|ambassador|athlete|creator|[가-힣]{2,4}(?:와|과) 함께한/i,"크리에이터 크루의 실제 착용 스토리를 상품 CTA와 연결","콘텐츠→PDP 클릭률"],
 ["HERITAGE","브랜드 스토리",/헤리티지|브랜드 스토리|브랜드의 역사|heritage|brand story|history|지속가능|환경|worn wear/i,"검증 가능한 브랜드 역사와 기술을 27SS 상품에 연결","콘텐츠 참여율"],
 ["COMMUNITY","커뮤니티·체험",/커뮤니티|워크숍|클리닉|community|workshop|clinic|crew|club/i,"트레킹·러닝 활동 후기를 제품 연계형 콘텐츠로 발행","신규회원 전환율"],
 ["SHOPPABLE","상품 연결형 콘텐츠",/컬렉션|셀렉션|라인업|collection|selection|shop now|lookbook|룩북|화보/i,"룩북·스토리 본문에 관련 제품 카드와 CTA 배치","콘텐츠→PDP 클릭률"]
 ];
 function clean(items){
  const keep=[],rejected=[],merged=[];
  for(const orig of items||[]){
   const x={...orig},src=canon(x.source_url),dest=canon(x.url);let title=norm(x.title),body=norm(x.summary);
   if(/로그인.*회원가입|^개인정보 처리 방침|^고객 서비스$|^베스트 구매 후기$|^image-img$|^our stores$|^stay in the loop$|^you may also like$|^shop all$|^메인 비주얼$/i.test(title)||/personal-information-collection-statement|\/service\/company\//i.test(src)){rejected.push({id:x.id,brand:x.brand,title:x.title,reason:"navigation_or_generic"});continue}
   if((title===norm(x.brand)||/^(?:new balance|블랙야크|내셔널지오그래픽|자주 묻는 질문|stay connected|collection)$/i.test(title))&&x.placement==="editorial_page"&&body.length>=12&&!/로그인|회원가입|멤버십 혜택\s*>/.test(body)){x.title=String(x.summary).slice(0,105);title=norm(x.title)}
   if(!src||!title||noise.test(title)||/안목을 넓히는 새로운 시선, 코오롱몰$/.test(title)||(x.placement!=="main_visual"&&index.test(src.split("?")[0]))){rejected.push({id:x.id,brand:x.brand,title:x.title,reason:"generic_or_index"});continue}
   if(x.placement==="main_visual"&&dest&&index.test(dest.split("?")[0])&&/^(?:packs|jackets|footwear|men|women|kids|accessories|가방|신발|아우터|남성|여성)$/i.test(title)){rejected.push({id:x.id,brand:x.brand,title:x.title,reason:"category_navigation"});continue}
   if(title==="이런 기획전도 놓치지 마세요"){if(!body){rejected.push({id:x.id,brand:x.brand,reason:"generic_title"});continue}x.title=x.summary.slice(0,160)}
   const effective=x.placement==="main_visual"?dest:src,img=canon(x.thumbnail);
   const realDetail=effective&&(/\/event\/\d{6}(?:_\d+)?\/[^/?]+\.asp|[?&]collectionIdx=\d+|\/event\/view\/eventCategoryNF\/\d+|\/app\/planning2\/views\/\d+\/\d+/i.test(effective)||detail.test(effective));
   const keys=[realDetail?"detail:"+effective:"",img?"image_title:"+img+"|"+norm(x.title):"","source_title:"+src+"|"+norm(x.title)].filter(Boolean);
   const m=keep.find(a=>a.brand===x.brand&&a.keys.some(k=>keys.includes(k)));
   if(!m){keep.push({...x,keys,placements:[x.placement].filter(Boolean),source_urls:[src],member_ids:[x.id].filter(Boolean)});continue}
   const old={...m},prefer=x.placement!=="main_visual"&&m.placement==="main_visual";
   if(prefer)Object.assign(m,x);
   m.keys=[...new Set([...old.keys,...keys])];m.placements=[...new Set([...old.placements,x.placement].filter(Boolean))];m.source_urls=[...new Set([...old.source_urls,src])];m.member_ids=[...new Set([...old.member_ids,x.id].filter(Boolean))];m.first_seen_at=[old.first_seen_at,x.first_seen_at].filter(Boolean).sort()[0]??null;m.last_seen_at=[old.last_seen_at,x.last_seen_at].filter(Boolean).sort().at(-1)??null;m.changed_at=[old.changed_at,x.changed_at].filter(Boolean).sort().at(-1)??null;const dates=[...new Set([old.published_at,x.published_at].filter(Boolean))];if(old.publication_status==="conflicting"||x.publication_status==="conflicting"||dates.length>1){m.published_at=null;m.publication_status="conflicting"}else if(dates.length===1){m.published_at=dates[0];m.publication_status=old.publication_status==="explicit"||x.publication_status==="explicit"?"explicit":m.publication_status}
   if(!m.page_metrics&&old.page_metrics)m.page_metrics=old.page_metrics;for(const fld of ["tpo","selling_points","type_tags"])m[fld]=[...new Set([...(old[fld]||[]),...(x[fld]||[])])];
   for(const fld of ["products","influencer","images","publication_evidence"]){const a=[...(Array.isArray(old[fld])?old[fld]:[]),...(Array.isArray(x[fld])?x[fld]:[])];m[fld]=a.length?[...new Map(a.map(v=>[JSON.stringify(v),v])).values()]:null}
   merged.push({brand:x.brand,member_id:x.id,representative_id:m.id,reason:keys.find(k=>old.keys.includes(k))?.split(":")[0]||"matched"});
  }
  return {items:keep.map(({keys,...x})=>x),rejected,merged}
 }
 const c=clean(feed.items),h=clean(feed.history||feed.items);
 const ref=(x,r)=>({brand:x.brand,title:x.title,url:x.placement==="main_visual"?(x.url||x.source_url):x.source_url,thumbnail:x.thumbnail||null,evidence:r});
 const match=(x,r)=>r[2].test([x.title,x.summary,...(x.type_tags||[]),...(x.tpo||[]),...(x.selling_points||[])].join(" "))||(r[0]==="PEOPLE"&&!!x.influencer?.length)||(r[0]==="SHOPPABLE"&&!!x.products?.length);
 const describe=x=>({...x,strategy_signals:rules.filter(r=>match(x,r)).map(r=>r[0]),purpose:classifyPurpose(x)});
 const items=c.items.map(describe);
 const historyItems=h.items.map(describe);
 const brands=(feed.brands||[]).map(b=>{const xs=items.filter(x=>x.brand===b.brand),n=xs.length,retained=historyItems.filter(x=>x.brand===b.brand).length;const signals=Object.fromEntries(rules.map(r=>[r[0],n?xs.filter(x=>x.strategy_signals.includes(r[0])).length:null]));return {brand:b.brand,pages_checked:b.pages_checked,raw_count:b.candidate_count,unique_count:n,retained_unique_count:retained,retained_scope:"recent_30d_observations",pending_urls:b.pending_urls??null,sitemap_urls:b.sitemap_urls??null,coverage_plan:b.coverage_plan??null,collection_status:b.status,signals,dominant_signals:rules.filter(r=>signals[r[0]]>0).sort((a,b)=>signals[b[0]]-signals[a[0]]).slice(0,2).map(r=>r[1]),sample_status:n<3?"thin_sample":"observed_sample",examples:xs.slice(0,2).map(x=>ref(x,"observed"))}});
 const market=items.filter(x=>x.brand!=="COLUMBIA"),owned=items.filter(x=>x.brand==="COLUMBIA"),insights=[];
 for(const r of rules){const ms=market.filter(x=>x.strategy_signals.includes(r[0])),os=owned.filter(x=>x.strategy_signals.includes(r[0])),bs=[...new Set(ms.map(x=>x.brand))];if(!ms.length)continue;const gap=market.length&&owned.length?Math.round((ms.length/market.length-os.length/owned.length)*1000)/10:null;insights.push({signal:r[0],label:r[1],action:r[3],kpi:r[4],competitor_count:ms.length,competitor_total:market.length,competitor_brands:bs.length,owned_count:os.length,owned_total:owned.length,gap_pp:gap,priority:bs.length>=2&&owned.length>=3&&gap>0?"review":"reference",references:bs.slice(0,3).map(b=>{const x=ms.find(y=>y.brand===b);return ref(x,[x.title,x.summary].join(" ").match(r[2])?.[0]||"분류 태그 또는 연결 근거")})})}
 insights.sort((a,b)=>(b.priority==="review")-(a.priority==="review")||b.competitor_brands-a.competitor_brands||b.competitor_count-a.competitor_count);
 return {source_checked_at:feed.checked_at_kst,schema_version:1,notes:"Observed content candidates, not publication or performance. Multi-label inferred signals. 27SS ideas are hypotheses; uncollected is not zero.",quality:{raw_count:(feed.items||[]).length,unique_count:items.length,retained_unique_count:historyItems.length,rejected_count:c.rejected.length,merged_count:c.merged.length,market_count:market.length,owned_count:owned.length},signals:rules.map(r=>({id:r[0],label:r[1]})),purpose_confidence:Object.fromEntries(["evidence_based","inferred","insufficient"].map(k=>[k,items.filter(x=>x.purpose.confidence===k).length])),purpose_counts:Object.fromEntries(PURPOSES.map(([code])=>[code,items.filter(x=>x.purpose.code===code).length])),purpose_by_brand:Object.fromEntries(brands.map(b=>[b.brand,Object.fromEntries(PURPOSES.map(([code])=>[code,items.filter(x=>x.brand===b.brand&&x.purpose.code===code).length]))])),brands,insights,items,history:historyItems,dedup_audit:{rejected:c.rejected,merged:c.merged}}
}
