// Offline content strategy analysis; no network.
export function analyze(feed){
 const norm=s=>String(s??"").normalize("NFKC").replace(/\s+/g," ").trim().toLowerCase();
 const canon=s=>{const m=String(s??"").match(/^https:\/\/([^/?#]+)([^?#]*)(?:\?([^#]*))?/i);if(!m)return "";const p=(m[2]||"/").replace(/\/$/,"");const q=(m[3]||"").split("&").filter(x=>x&&!/^(utm_[^=]*|gclid|fbclid|ref|tracking|sort|inStock)=/i.test(x)).sort().join("&");return "https://"+m[1].toLowerCase()+p+(q?"?"+q:"")};
 const detail=/\/(?:special\/\d+|event\/view\/[^/]+\/\d+|event\/planning\/\d+|exhibition\/[^/]+|display\/promotions\/collection\/\d+|blogs\/[^/]+\/[^/]+|stories\/(?:filmDetail|detail)\/\d+|promo\/pstories\/\d+|campaign\/[^/]+|lookbook[^/]*\/[^/]+)(?:\/|[?]|$)|[?&](?:no|eventNo)=\d+/i;
 const noise=/^(?:\d+|(?:slider|banner|img|image)[ _-]*\d+|weekly best items|best sellers|new arrivals|ranking|home|main|official online store|전체 상품|상품 목록|추천 상품|관련 상품|베스트 상품)$/i;
 const index=/\/(?:list|category|search|ranking)(?:\/|$)|\/special\/?$|\/blogs\/[^/]+\/?$|\/stories\/(?:main|all)\/?$/i;
 const rules=[
 ["TPO","TPO·스타일링",/하이킹|트레킹|등산|캠핑|러닝|여행|도심|일상|데일리|스타일링|hiking|trekking|camping|running|urban|daily|styling/i,"도심·트레일·여행 상황별 착장과 제품 CTA 연결","콘텐츠→PDP 클릭률"],
 ["TECH","제품 기능·기술",/경량|방수|보온|투습|통기|내구|기술력|고어텍스|프리마로프트|기능성|waterproof|lightweight|insulation|technology|breathab|gore.?tex/i,"27SS 제품 스펙을 실제 사용 테스트와 짧은 비주얼로 증명","PDP→Cart"],
 ["PEOPLE","인물·크리에이터",/앰배서더|애슬릿|크리에이터|인플루언서|ambassador|athlete|creator|정은채와 함께한/i,"크리에이터 크루의 실제 착용 스토리를 상품 CTA와 연결","콘텐츠→PDP 클릭률"],
 ["HERITAGE","브랜드 스토리",/헤리티지|브랜드 스토리|브랜드의 역사|heritage|brand story|history|지속가능|환경|worn wear/i,"검증 가능한 브랜드 역사와 기술을 27SS 상품에 연결","콘텐츠 참여율"],
 ["COMMUNITY","커뮤니티·체험",/커뮤니티|워크숍|클리닉|community|workshop|clinic|crew|club/i,"트레킹·러닝 활동 후기를 제품 연계형 콘텐츠로 발행","신규회원 전환율"],
 ["SHOPPABLE","상품 연결형 콘텐츠",/컬렉션|셀렉션|라인업|collection|selection|shop now|lookbook|룩북|화보/i,"룩북·스토리 본문에 관련 제품 카드와 CTA 배치","콘텐츠→PDP 클릭률"]
 ];
 function clean(items){
  const keep=[],rejected=[],merged=[];
  for(const orig of items||[]){
   const x={...orig},src=canon(x.source_url),dest=canon(x.url),title=norm(x.title),body=norm(x.summary);
   if(!src||!title||noise.test(title)||/안목을 넓히는 새로운 시선, 코오롱몰$/.test(title)||(x.placement!=="main_visual"&&index.test(src.split("?")[0]))){rejected.push({id:x.id,brand:x.brand,title:x.title,reason:"generic_or_index"});continue}
   if(title==="이런 기획전도 놓치지 마세요"){if(!body){rejected.push({id:x.id,brand:x.brand,reason:"generic_title"});continue}x.title=x.summary.slice(0,160)}
   const effective=x.placement==="main_visual"?dest:src,img=canon(x.thumbnail);
   const keys=[effective&&detail.test(effective)?"detail:"+effective:"",img?"image_title:"+img+"|"+norm(x.title):"","source_title:"+src+"|"+norm(x.title)].filter(Boolean);
   const m=keep.find(a=>a.brand===x.brand&&a.keys.some(k=>keys.includes(k)));
   if(!m){keep.push({...x,keys,placements:[x.placement].filter(Boolean),source_urls:[src],member_ids:[x.id].filter(Boolean)});continue}
   const old={...m},prefer=x.placement!=="main_visual"&&m.placement==="main_visual";
   if(prefer)Object.assign(m,x);
   m.keys=[...new Set([...old.keys,...keys])];m.placements=[...new Set([...old.placements,x.placement].filter(Boolean))];m.source_urls=[...new Set([...old.source_urls,src])];m.member_ids=[...new Set([...old.member_ids,x.id].filter(Boolean))];m.first_seen_at=[old.first_seen_at,x.first_seen_at].filter(Boolean).sort()[0]??null;
   for(const fld of ["tpo","selling_points","type_tags"])m[fld]=[...new Set([...(old[fld]||[]),...(x[fld]||[])])];
   for(const fld of ["products","influencer","images","publication_evidence"]){const a=[...(Array.isArray(old[fld])?old[fld]:[]),...(Array.isArray(x[fld])?x[fld]:[])];m[fld]=a.length?[...new Map(a.map(v=>[JSON.stringify(v),v])).values()]:null}
   merged.push({brand:x.brand,member_id:x.id,representative_id:m.id,reason:keys.find(k=>old.keys.includes(k))?.split(":")[0]||"matched"});
  }
  return {items:keep.map(({keys,...x})=>x),rejected,merged}
 }
 const c=clean(feed.items),h=clean(feed.history||feed.items);
 const ref=(x,r)=>({brand:x.brand,title:x.title,url:canon(x.placement==="main_visual"?x.url:x.source_url)||canon(x.source_url),thumbnail:x.thumbnail||null,evidence:r});
 const match=(x,r)=>r[2].test([x.title,x.summary,...(x.type_tags||[]),...(x.tpo||[]),...(x.selling_points||[])].join(" "))||(r[0]==="PEOPLE"&&!!x.influencer?.length)||(r[0]==="SHOPPABLE"&&!!x.products?.length);
 const items=c.items.map(x=>({...x,strategy_signals:rules.filter(r=>match(x,r)).map(r=>r[0])}));
 const brands=(feed.brands||[]).map(b=>{const xs=items.filter(x=>x.brand===b.brand),n=xs.length;const signals=Object.fromEntries(rules.map(r=>[r[0],n?xs.filter(x=>x.strategy_signals.includes(r[0])).length:null]));return {brand:b.brand,pages_checked:b.pages_checked,raw_count:b.candidate_count,unique_count:n,collection_status:b.status,signals,dominant_signals:rules.filter(r=>signals[r[0]]>0).sort((a,b)=>signals[b[0]]-signals[a[0]]).slice(0,2).map(r=>r[1]),sample_status:n<3?"thin_sample":"observed_sample",examples:xs.slice(0,2).map(x=>ref(x,"observed"))}});
 const market=items.filter(x=>x.brand!=="COLUMBIA"),owned=items.filter(x=>x.brand==="COLUMBIA"),insights=[];
 for(const r of rules){const ms=market.filter(x=>x.strategy_signals.includes(r[0])),os=owned.filter(x=>x.strategy_signals.includes(r[0])),bs=[...new Set(ms.map(x=>x.brand))];if(!ms.length)continue;const gap=market.length&&owned.length?Math.round((ms.length/market.length-os.length/owned.length)*1000)/10:null;insights.push({signal:r[0],label:r[1],action:r[3],kpi:r[4],competitor_count:ms.length,competitor_total:market.length,competitor_brands:bs.length,owned_count:os.length,owned_total:owned.length,gap_pp:gap,priority:bs.length>=2&&owned.length>=3&&gap>0?"review":"reference",references:bs.slice(0,3).map(b=>{const x=ms.find(y=>y.brand===b);return ref(x,[x.title,x.summary].join(" ").match(r[2])?.[0]||r[1])})})}
 insights.sort((a,b)=>(b.priority==="review")-(a.priority==="review")||b.competitor_brands-a.competitor_brands||b.competitor_count-a.competitor_count);
 return {source_checked_at:feed.checked_at_kst,schema_version:1,notes:"Observed content candidates, not publication or performance. Multi-label inferred signals. 27SS ideas are hypotheses; uncollected is not zero.",quality:{raw_count:(feed.items||[]).length,unique_count:items.length,rejected_count:c.rejected.length,merged_count:c.merged.length,market_count:market.length,owned_count:owned.length},signals:rules.map(r=>({id:r[0],label:r[1]})),brands,insights,items,history:h.items,dedup_audit:{rejected:c.rejected,merged:c.merged}}
}
