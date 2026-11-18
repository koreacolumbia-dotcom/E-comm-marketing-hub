// Exclusive primary intent; editorial format is a separate, multi-label dimension.
// URL names (event/collection/special) alone are not evidence of a sale.
export const PURPOSES=[["COMMERCIAL","상품 기획전"],["PROMOTION","할인·혜택 프로모션"],["EDITORIAL","브랜드 콘텐츠"],["HYBRID","콘텐츠형 기획전"],["COMMUNITY","커뮤니티·체험"],["UNCLASSIFIED","판단 보류"]];
export function classifyPurpose(x){
 const title=String(x.title||""),summary=String(x.summary||""),cta=Array.isArray(x.cta)?x.cta.join(" / "):"",placement=x.placement||"",article=placement==="editorial_page";
 const body=article?summary:""; // Main visual summaries may include adjacent banners and global menus.
 const reasons=[];
 const add=(name,re,str)=>{const m=String(str||"").match(re);if(m){reasons.push(name+": "+m[0].slice(0,90));return true}return false};
 const promo=add("혜택 문구",/블랙\s*프라이데이|black\s*friday|쿠폰|할인|특가|세일|페이백|적립금|구매\s*혜택|구매\s*금액대별|(?:최대\s*)?\d+\s*%\s*(?:off|할인|우대)|\d+\s*%\s*OFF|\bclearance\b/i,title+" "+cta)||add("혜택 설명",/구매\s*(?:시|금액대별).{0,28}(?:할인|증정|쿠폰|사은품|우대)|(?:쿠폰|할인)\s*(?:증정|혜택|적용|제공)|\d+\s*%\s*(?:OFF|할인|우대)/i,body);
 const community=add("활동·참여",/워크숍|워크샵|클리닉|클래스|참가자\s*모집|참가\s*신청|커뮤니티\s*(?:활동|프로그램|이벤트)|크루\s*(?:모집|활동)|하이킹\s*프로그램|workshop|clinic|community\s*(?:event|program)|join\s+us/i,title+" "+body+" "+cta);
 const editorial=add("스토리·정보",/인터뷰|브랜드\s*(?:스토리|철학|역사)|헤리티지|제작\s*(?:과정|비하인드)|개발\s*스토리|필드\s*테스트|착용\s*(?:후기|테스트)|가이드|매거진|저널|탐험기|산행기|지속\s*가능|원주민|interview|behind.the.scenes|heritage|brand\s*story|our\s*story|sustainab|field\s*test|gear\s*guide|journal|about\s*us|about\s*the\s*founder|community\s*impact/i,title+" "+body);
 const formats=new Set([x.type,...(x.type_tags||[])]);
 const narrative=editorial||(article&&(formats.has("BRAND_STORY")||formats.has("PRODUCT_STORY")||formats.has("LOOKBOOK"))&&body.length>=100&&!/^(?:what'?s new|new arrivals|weekly best)/i.test(title));
 if(narrative&&!editorial)reasons.push("기사형 본문 및 스토리 형식");
 const directProduct=placement==="main_visual"&&(/\/product\/(?:view\.asp|[a-z0-9_-]{5,})(?:[/?#]|$)|\/products\/(?:view\/\d+|[^/?#]+$)|\/goods\/(?:view|detail)/i.test(String(x.url||"")))&&!/(?:category|list|search)/i.test(String(x.url||""));
 const buy=add("구매 CTA",/상품\s*보기|제품\s*보기|남성\s*제품\s*보기|여성\s*제품\s*보기|구매하기|쇼핑하기|shop\s*now|shop\s*(?:men|women|the\s*collection)|buy\s*now/i,cta);
 const collection=add("판매 큐레이션 문구",/상품\s*기획전|쇼핑\s*기획전|상품\s*모아보기|상품\s*라인업|제품\s*라인업|컬렉션\s*구매|상품\s*셀렉션|shop\s*the\s*collection/i,title+" "+cta);
 const count=Array.isArray(x.products)?x.products.filter(p=>p&&p.url&&p.name&&!/^\d+(?:\.\d+)?\s*\(/.test(p.name)).length:0;
 const productEvidence=article&&count>0;
 if(productEvidence)reasons.push("명시 상품 링크 "+count+"개");
 if(directProduct)reasons.push("상품 상세 페이지로 직접 연결");
 const price=article&&/\d{1,3}(?:,\d{3})+\s*원/.test(body);
 if(price&&productEvidence)reasons.push("가격 노출");
 const commerce=directProduct||buy||collection||productEvidence;
 let code="UNCLASSIFIED";
 if(promo)code="PROMOTION";
 else if(community)code="COMMUNITY";
 else if(narrative&&commerce)code="HYBRID";
 else if(narrative)code="EDITORIAL";
 else if(commerce)code="COMMERCIAL";
 if(code==="UNCLASSIFIED")reasons.push("구매·스토리 판단 근거 부족");
 return {code,evidence:[...new Set(reasons)],confidence:code==="UNCLASSIFIED"?"insufficient":"rule_inferred",commercial_evidence:commerce,promotion_evidence:promo};
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
 ["TECH","제품 기능·기술",/경량|방수|보온|투습|통기|내구|기술력|고어텍스|프리마로프트|기능성|waterproof|lightweight|insulation|technology|breathab|gore.?tex/i,"27SS 제품 스펙을 실제 사용 테스트와 짧은 비주얼로 증명","PDP→Cart"],
 ["PEOPLE","인물·크리에이터",/앰배서더|애슬릿|크리에이터|인플루언서|ambassador|athlete|creator|[가-힣]{2,4}(?:와|과) 함께한/i,"크리에이터 크루의 실제 착용 스토리를 상품 CTA와 연결","콘텐츠→PDP 클릭률"],
 ["HERITAGE","브랜드 스토리",/헤리티지|브랜드 스토리|브랜드의 역사|heritage|brand story|history|지속가능|환경|worn wear/i,"검증 가능한 브랜드 역사와 기술을 27SS 상품에 연결","콘텐츠 참여율"],
 ["COMMUNITY","커뮤니티·체험",/커뮤니티|워크숍|클리닉|community|workshop|clinic|crew|club/i,"트레킹·러닝 활동 후기를 제품 연계형 콘텐츠로 발행","신규회원 전환율"],
 ["SHOPPABLE","상품 연결형 콘텐츠",/컬렉션|셀렉션|라인업|collection|selection|shop now|lookbook|룩북|화보/i,"룩북·스토리 본문에 관련 제품 카드와 CTA 배치","콘텐츠→PDP 클릭률"]
 ];
 function clean(items){
  const keep=[],rejected=[],merged=[];
  for(const orig of items||[]){
   const x={...orig},src=canon(x.source_url),dest=canon(x.url),title=norm(x.title),body=norm(x.summary);
   if(!src||!title||noise.test(title)||/안목을 넓히는 새로운 시선, 코오롱몰$/.test(title)||(x.placement!=="main_visual"&&index.test(src.split("?")[0]))){rejected.push({id:x.id,brand:x.brand,title:x.title,reason:"generic_or_index"});continue}
   if(x.placement==="main_visual"&&dest&&index.test(dest.split("?")[0])&&/^(?:packs|jackets|footwear|men|women|kids|accessories|가방|신발|아우터|남성|여성)$/i.test(title)){rejected.push({id:x.id,brand:x.brand,title:x.title,reason:"category_navigation"});continue}
   if(title==="이런 기획전도 놓치지 마세요"){if(!body){rejected.push({id:x.id,brand:x.brand,reason:"generic_title"});continue}x.title=x.summary.slice(0,160)}
   const effective=x.placement==="main_visual"?dest:src,img=canon(x.thumbnail);
   const keys=[effective&&detail.test(effective)?"detail:"+effective:"",img?"image_title:"+img+"|"+norm(x.title):"","source_title:"+src+"|"+norm(x.title)].filter(Boolean);
   const m=keep.find(a=>a.brand===x.brand&&a.keys.some(k=>keys.includes(k)));
   if(!m){keep.push({...x,keys,placements:[x.placement].filter(Boolean),source_urls:[src],member_ids:[x.id].filter(Boolean)});continue}
   const old={...m},prefer=x.placement!=="main_visual"&&m.placement==="main_visual";
   if(prefer)Object.assign(m,x);
   m.keys=[...new Set([...old.keys,...keys])];m.placements=[...new Set([...old.placements,x.placement].filter(Boolean))];m.source_urls=[...new Set([...old.source_urls,src])];m.member_ids=[...new Set([...old.member_ids,x.id].filter(Boolean))];m.first_seen_at=[old.first_seen_at,x.first_seen_at].filter(Boolean).sort()[0]??null;m.last_seen_at=[old.last_seen_at,x.last_seen_at].filter(Boolean).sort().at(-1)??null;m.changed_at=[old.changed_at,x.changed_at].filter(Boolean).sort().at(-1)??null;const dates=[...new Set([old.published_at,x.published_at].filter(Boolean))];if(old.publication_status==="conflicting"||x.publication_status==="conflicting"||dates.length>1){m.published_at=null;m.publication_status="conflicting"}else if(dates.length===1){m.published_at=dates[0];m.publication_status=old.publication_status==="explicit"||x.publication_status==="explicit"?"explicit":m.publication_status}
   for(const fld of ["tpo","selling_points","type_tags"])m[fld]=[...new Set([...(old[fld]||[]),...(x[fld]||[])])];
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
 const brands=(feed.brands||[]).map(b=>{const xs=items.filter(x=>x.brand===b.brand),n=xs.length;const signals=Object.fromEntries(rules.map(r=>[r[0],n?xs.filter(x=>x.strategy_signals.includes(r[0])).length:null]));return {brand:b.brand,pages_checked:b.pages_checked,raw_count:b.candidate_count,unique_count:n,collection_status:b.status,signals,dominant_signals:rules.filter(r=>signals[r[0]]>0).sort((a,b)=>signals[b[0]]-signals[a[0]]).slice(0,2).map(r=>r[1]),sample_status:n<3?"thin_sample":"observed_sample",examples:xs.slice(0,2).map(x=>ref(x,"observed"))}});
 const market=items.filter(x=>x.brand!=="COLUMBIA"),owned=items.filter(x=>x.brand==="COLUMBIA"),insights=[];
 for(const r of rules){const ms=market.filter(x=>x.strategy_signals.includes(r[0])),os=owned.filter(x=>x.strategy_signals.includes(r[0])),bs=[...new Set(ms.map(x=>x.brand))];if(!ms.length)continue;const gap=market.length&&owned.length?Math.round((ms.length/market.length-os.length/owned.length)*1000)/10:null;insights.push({signal:r[0],label:r[1],action:r[3],kpi:r[4],competitor_count:ms.length,competitor_total:market.length,competitor_brands:bs.length,owned_count:os.length,owned_total:owned.length,gap_pp:gap,priority:bs.length>=2&&owned.length>=3&&gap>0?"review":"reference",references:bs.slice(0,3).map(b=>{const x=ms.find(y=>y.brand===b);return ref(x,[x.title,x.summary].join(" ").match(r[2])?.[0]||"분류 태그 또는 연결 근거")})})}
 insights.sort((a,b)=>(b.priority==="review")-(a.priority==="review")||b.competitor_brands-a.competitor_brands||b.competitor_count-a.competitor_count);
 return {source_checked_at:feed.checked_at_kst,schema_version:1,notes:"Observed content candidates, not publication or performance. Multi-label inferred signals. 27SS ideas are hypotheses; uncollected is not zero.",quality:{raw_count:(feed.items||[]).length,unique_count:items.length,rejected_count:c.rejected.length,merged_count:c.merged.length,market_count:market.length,owned_count:owned.length},signals:rules.map(r=>({id:r[0],label:r[1]})),purpose_counts:Object.fromEntries(PURPOSES.map(([code])=>[code,items.filter(x=>x.purpose.code===code).length])),purpose_by_brand:Object.fromEntries(brands.map(b=>[b.brand,Object.fromEntries(PURPOSES.map(([code])=>[code,items.filter(x=>x.brand===b.brand&&x.purpose.code===code).length]))])),brands,insights,items,history:h.items.map(describe),dedup_audit:{rejected:c.rejected,merged:c.merged}}
}
