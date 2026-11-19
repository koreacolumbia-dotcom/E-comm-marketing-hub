// Observed-content mix review. No claim about actual publication frequency, sales, or brand superiority.
// Each qualified peer contributes one equally weighted share regardless of its crawl volume.
export const GAP_TOPICS = [
  {id:'SHOPPABLE',label:'상품 연결형 콘텐츠',note:'텍스트·구조 분류상 상품 연계 후보',action:'룩북과 캠페인 본문에 관련 상품 카드·PDP CTA를 연결하고 실제 클릭률을 검증'},
  {id:'TPO',label:'TPO·스타일링',note:'착장·활용 상황 키워드 기준',action:'도심·트레일·여행 등 상황별 실착 콘텐츠를 상품과 함께 노출'},
  {id:'COMMUNITY',label:'커뮤니티·체험',note:'활동·참여 문구 기준',action:'크리에이터 크루·산행·러닝 체험 후기를 공식몰 콘텐츠로 연결'},
  {id:'PEOPLE',label:'인물·크리에이터',note:'인물·협업 언급 기준',action:'실착 인물·크리에이터 콘텐츠에서 상품 탐색을 연결'},
  {id:'HERITAGE',label:'브랜드 스토리',note:'브랜드 역사·지속가능성 문구 기준',action:'브랜드 히스토리와 제품 기술의 관계를 콘텐츠로 설명'},
  {id:'TECH',label:'제품 기능·기술',note:'기능·소재 문구 기준',action:'기술 설명을 실사용 테스트와 연결하여 이해도·PDP 전환을 검증'}
];
const pct=(num,den)=>den?100*num/den:0;
const round1=n=>Math.round(n*10)/10;
const match=(item,signal)=>(item.strategy_signals||[]).includes(signal);
const sourceUrl=x=>x.placement==='main_visual'?(x.url||x.source_url):(x.source_url||x.url);

/**
 * @param {object} opts - analyzed, deduplicated 30-day observation history.
 * minOwned/minPeer prevent declaring absence on thin crawl samples.
 */
export function compareOwnedContent({items=[],brands=[],sourceCheckedAt='',minOwned=20,minPeer=12,minPeerBrands=4}={}){
  const byBrand=new Map();
  for(const item of items){
    if(!item?.brand)continue;
    if(!byBrand.has(item.brand))byBrand.set(item.brand,[]);
    byBrand.get(item.brand).push(item);
  }
  const owned=byBrand.get('COLUMBIA')||[];
  const peers=[...byBrand].filter(([brand,xs])=>brand!=='COLUMBIA'&&xs.length>=minPeer).map(([brand,xs])=>({brand,items:xs}));
  const excluded=[...new Set([...(brands||[]).map(b=>b.brand),...byBrand.keys()])]
    .filter(b=>b&&b!=='COLUMBIA'&&!peers.some(p=>p.brand===b));
  const gap=(owned.length<minOwned||peers.length<minPeerBrands);
  const rows=GAP_TOPICS.map(t=>{
    const ownMatched=owned.filter(x=>match(x,t.id));
    const ownPct=round1(pct(ownMatched.length,owned.length));
    const shares=peers.map(p=>({...p,matched:p.items.filter(x=>match(x,t.id))}))
      .map(p=>({...p,share:pct(p.matched.length,p.items.length)}));
    const peerPct=shares.length?round1(shares.reduce((a,p)=>a+p.share,0)/shares.length):null;
    const delta=peerPct==null?null:round1(peerPct-ownPct);
    const supporting=shares.filter(x=>x.matched.length>0);
    const refs=supporting.sort((a,b)=>b.share-a.share).slice(0,2).flatMap(p=>{
      const x=p.matched.find(z=>sourceUrl(z))||p.matched[0];
      return x?[{brand:p.brand,title:x.title||'공식 콘텐츠',url:sourceUrl(x)||null,id:x.id}]:[];
    });
    const status=gap?'insufficient':delta>=8&&peerPct>=12&&supporting.length>=3?'review':delta<=-8&&ownPct>=12&&ownMatched.length>=3?'strength':delta>=3&&peerPct>=8?'watch':'no_gap';
    return {id:t.id,label:t.label,note:t.note,action:t.action,ownedCount:ownMatched.length,
      ownedTotal:owned.length,ownedPct:ownPct,peerPct,delta,peerBrands:peers.length,
      supportingBrands:supporting.length,peerTotal:peers.reduce((sum,p)=>sum+p.items.length,0),
      status,references:refs,ownedReferences:ownMatched.filter(x=>sourceUrl(x)).slice(0,2).map(x=>({brand:'COLUMBIA',title:x.title||'컬럼비아 공식 콘텐츠',url:sourceUrl(x),id:x.id}))};
  });
  const review=rows.filter(x=>x.status==='review').sort((a,b)=>b.delta-a.delta);
  const watch=rows.filter(x=>x.status==='watch').sort((a,b)=>b.delta-a.delta);
  const strengths=rows.filter(x=>x.status==='strength').sort((a,b)=>a.delta-b.delta);
  return {status:gap?'insufficient':'ready',ownedTotal:owned.length,peerBrands:peers.length,
    peerObserved:peers.reduce((a,p)=>a+p.items.length,0),excluded,
    minOwned,minPeer,minPeerBrands,sourceCheckedAt,review,watch,strengths,rows,
    methodology:'최근 30일 직접 관측 후 중복 제거 · 브랜드별 콘텐츠 구성 비중의 동일 가중 평균 · 텍스트 기반 다중 분류'};
}
