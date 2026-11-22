/* CSK business-level diagnosis: exact 7-day accounting bridge, no causal claims. */
(()=>{
"use strict";
const root=document.getElementById("macro-health");if(!root)return;
const finite=x=>typeof x==="number"&&Number.isFinite(x);
const fmt=x=>finite(x)?Math.round(x).toLocaleString("ko-KR"):"—";
const pct=(a,b)=>finite(a)&&finite(b)&&b>0?(a/b-1)*100:null;
const signed=x=>finite(x)?(x>0?"+":"")+x.toFixed(1)+"%":"—";
const won=x=>finite(x)?(x>=0?"+":"−")+Math.round(Math.abs(x)/10000).toLocaleString("ko-KR")+"만 원":"—";
const esc=s=>String(s??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const multi=v=>v[0]*v[1]*v[2];
const valid=x=>x&&["sessions","orders","revenue"].every(k=>finite(x[k])&&x[k]>0)&&x.orders<=x.sessions;
function bridge(current,previous){
 if(!valid(current)||!valid(previous))return null;
 const start=[previous.sessions,previous.orders/previous.sessions,previous.revenue/previous.orders];
 const finish=[current.sessions,current.orders/current.sessions,current.revenue/current.orders];
 const factors=[0,0,0],perm=[[0,1,2],[0,2,1],[1,0,2],[1,2,0],[2,0,1],[2,1,0]];
 for(const order of perm){
   const x=start.slice();
   for(const i of order){const before=multi(x);x[i]=finish[i];factors[i]+=(multi(x)-before)/6;}
 }
 const delta=current.revenue-previous.revenue,accounted=factors.reduce((a,b)=>a+b,0);
 if(Math.abs(accounted-delta)>Math.max(0.01,Math.abs(delta)*0.0000001))return null;
 return {traffic:factors[0],conversion:factors[1],aov:factors[2],delta,
   current:{...current,cvr:finish[1],aov:finish[2]},
   previous:{...previous,cvr:start[1],aov:start[2]}};
}
const card=(i,driver)=>{
 const role=driver.impact<0?"감소 요인":"증가·상쇄 요인";
 const cls=driver.impact<0?"loss":"gain";
 return '<article class="macro-driver '+cls+'">'+
  '<div class="macro-driver-top"><strong>요인 '+i+' · '+esc(driver.title)+'</strong><span>'+role+'</span></div>'+
  '<div class="macro-impact">'+esc(won(driver.impact))+'</div>'+
  '<div class="macro-numbers">'+esc(driver.metric)+'</div>'+
  '<p class="macro-explain">'+esc(driver.insight)+'</p>'+
  (driver.breakdown||"")+
  '<div class="macro-next"><b>확인된 변화</b> '+esc(driver.proof||"동일 기간 집계")+'</div>'+
  '<div class="macro-next macro-action"><b>추천 실행</b> '+esc(driver.next)+'</div></article>';
};
function render(operations,intelligence,weeklyEvidence,alignedBasket){
 const w=operations?.weekly||{},a=w.current?.total,b=w.previous?.total;
 const safe=w.status==="ready"&&w.current?.status==="complete"&&w.previous?.status==="complete"
   &&typeof w.period==="string"&&typeof w.comparison_period==="string"&&
   w.period.endsWith(operations.report_date||"")&&!!operations.report_date;
 const data=safe?bridge(a,b):null;
 if(!data){
  root.innerHTML='<div class="macro-heading"><h2>사업 전체 원인 분석</h2></div>'+
   '<div class="macro-unavailable">동일 기간의 완전한 7일 매출·주문·세션이 없어 거시 기여액을 계산하지 않았습니다. <a href="../marketing_operations/index.html">운영 원천 확인</a></div>';
  return;
 }
 const cur=data.current,prev=data.previous,change=pct(cur.revenue,prev.revenue);
 const dates=x=>typeof x==="string"?x.split(" ~ "):[];
 const currentDates=dates(w.period),priorDates=dates(w.comparison_period);
 const samePeriodBasket=alignedBasket?.status==="ga4_measured"&&alignedBasket?.report_date===operations.report_date&&
   alignedBasket.period?.current_start===currentDates[0]&&alignedBasket.period?.current_end===currentDates[1]&&
   alignedBasket.period?.previous_start===priorDates[0]&&alignedBasket.period?.previous_end===priorDates[1]&&
   alignedBasket.item_aov_bridge&&finite(alignedBasket.item_aov_bridge.items_per_order_previous)&&
   finite(alignedBasket.item_aov_bridge.items_per_order_current);
 const basketReconciled=samePeriodBasket&&alignedBasket.admin_reconciliation?.comparable===true;
 const basket=samePeriodBasket?alignedBasket.item_aov_bridge:null;
 const qEffect=basketReconciled&&finite(basket.items_per_order_effect)?basket.items_per_order_effect:null;
 const pEffect=basketReconciled&&finite(basket.realized_item_revenue_effect)?basket.realized_item_revenue_effect:null;
 const gapEffect=qEffect!==null&&pEffect!==null?
   (cur.aov-prev.aov)-(qEffect+pEffect):null;
 const reconciledBasketComponents=gapEffect!==null&&Math.abs(gapEffect)<Math.abs(cur.aov-prev.aov)*0.5+10000;
 const basketBreakdown=basketReconciled&&reconciledBasketComponents?
   '<div class="macro-basket-bridge"><div><span>주문당 상품수 변화</span><b>'+(qEffect>0?"+":"")+fmt(qEffect)+'원/건</b></div>'+
   '<div><span>상품당 실현매출 변화</span><b>'+(pEffect>0?"+":"")+fmt(pEffect)+'원/건</b></div>'+
   '<div><span>상품행·구매매출 차이</span><b>'+(gapEffect>0?"+":"")+fmt(gapEffect)+'원/건</b></div></div>':
   '';
 const basketNote=basketReconciled?
   "같은 7일 구간 GA4 구매구성 확인 · 주문당 상품수 "+basket.items_per_order_previous.toFixed(3)+
   "→"+basket.items_per_order_current.toFixed(3)+"개 / 상품당 실현매출 "+
   fmt(basket.realized_item_price_previous)+"→"+fmt(basket.realized_item_price_current)+"원":
   samePeriodBasket?"GA4 구매구성 기간 일치 · 운영 KPI와 정합성 미통과(상품수·가격 기여 미확정)":
   "같은 기간의 상품수·가격 구성이 아직 검증되지 않았습니다.";
 const distribution=basketReconciled?alignedBasket.basket_distribution:null;
 const basketMix=(distribution&&finite(distribution.previous?.single_item_pct)&&
    finite(distribution.current?.single_item_pct)&&
    finite(distribution.previous?.classified_share_pct)&&finite(distribution.current?.classified_share_pct)&&
    distribution.previous.classified_share_pct>=95&&distribution.current.classified_share_pct>=95&&
    distribution.previous.total_orders===prev.orders&&distribution.current.total_orders===cur.orders)?
    distribution:null;
 const mixMarkup=basketMix?
   '<div class="macro-basket-insight"><b>1개 구매 주문 '+basketMix.previous.single_item_pct.toFixed(1)+'% → '+
   basketMix.current.single_item_pct.toFixed(1)+'%</b><span>'+
   (basketMix.single_item_change_pp>0?'+':'')+basketMix.single_item_change_pp.toFixed(1)+'%p</span></div>'+
   (finite(basketMix.previous.multi_item_avg_units)&&finite(basketMix.current.multi_item_avg_units)?
    '<div class="macro-basket-insight"><b>다품목 주문 평균 수량 '+
    basketMix.previous.multi_item_avg_units.toFixed(2)+' → '+
    basketMix.current.multi_item_avg_units.toFixed(2)+'개</b><span>'+
    (basketMix.multi_item_avg_units_change>0?'+':'')+
    basketMix.multi_item_avg_units_change.toFixed(2)+'개</span></div>':'')+
   (finite(basketMix.previous.three_plus_pct)&&finite(basketMix.current.three_plus_pct)?
    '<div class="macro-basket-insight"><b>3개 이상 구매 주문 비중 '+
    basketMix.previous.three_plus_pct.toFixed(1)+'% → '+
    basketMix.current.three_plus_pct.toFixed(1)+'%</b><span>'+
    (basketMix.three_plus_change_pp>0?'+':'')+
    basketMix.three_plus_change_pp.toFixed(1)+'%p</span></div>':''):'';
 const categories=basketReconciled&&Array.isArray(alignedBasket.category_bridge?.rows)?
   alignedBasket.category_bridge.rows:[];
 const cat=n=>categories.find(x=>x.category===n&&finite(x.share_previous_pct)&&finite(x.share_current_pct));
 const outer=cat('OUTER'),sale=cat('SALE');
 const catMarkup=outer||sale?
   '<div class="macro-category-mini"><b>구매 상품군 비중</b>'+
   (outer?'<span>OUTER '+outer.share_previous_pct.toFixed(1)+'% → '+outer.share_current_pct.toFixed(1)+'%</span>':'')+
   (sale?'<span>SALE '+sale.share_previous_pct.toFixed(1)+'% → '+sale.share_current_pct.toFixed(1)+'%</span>':'')+
   '</div>':'';
 const cvBefore=prev.cvr*100,cvAfter=cur.cvr*100;
 const bucket=[
  {id:"aov",title:"객단가·구매 구성",impact:data.aov,
   metric:"객단가 "+fmt(prev.aov)+" → "+fmt(cur.aov)+"원 ("+signed(pct(cur.aov,prev.aov))+")",
   insight:data.aov<0?"주문은 늘었지만 주문 1건당 매출이 낮아졌습니다.":"주문당 매출이 전체 실적에 기여했습니다.",
   proof:basketNote,breakdown:basketBreakdown+mixMarkup+catMarkup,
   confidence:basketReconciled?"동일 기간 GA4 구매 이벤트·운영 KPI 대사 통과 · 독립 ERP 검증 전":
     "운영 객단가 산술 확인 · 구매구성 원인은 검증 제한",
   next:basketMix&&basketMix.multi_item_avg_units_change<0?
     "다품목 주문 수량 확대: 연관상품·세트 구매 실험":
     basketReconciled?"OUTER와 SALE 상품군을 활용한 연관구매 실험":
     "상품 조합별 교차구매 제안 실험"},
  {id:"conversion",title:"구매전환 효율",impact:data.conversion,
   metric:"CVR "+cvBefore.toFixed(3)+"% → "+cvAfter.toFixed(3)+"% ("+
     (cvAfter-cvBefore>0?"+":"")+(cvAfter-cvBefore).toFixed(3)+"%p)",
   insight:data.conversion<0?"방문 대비 주문 효율이 다소 약화됐습니다.":"방문 대비 주문 효율이 개선됐습니다.",
   proof:"방문→구매 "+cvBefore.toFixed(3)+"% → "+cvAfter.toFixed(3)+"%",
   confidence:"전체 전환 산술 확인 · 구간별 구매이탈 원인은 미확정",
   next:"방문 대비 구매 단계가 약한 상품부터 장바구니 흐름 개선"},
  {id:"traffic",title:"유입 규모",impact:data.traffic,
   metric:"세션 "+fmt(prev.sessions)+" → "+fmt(cur.sessions)+" ("+signed(pct(cur.sessions,prev.sessions))+")",
   insight:data.traffic>=0?"유입 증가는 매출 감소를 일부 상쇄했습니다.":"유입 감소가 전체 매출을 압박했습니다.",
   proof:"세션 "+fmt(prev.sessions)+" → "+fmt(cur.sessions),
   confidence:"전체 세션 변화 확인 · 유입 인과효과는 미확정",
   next:"증가한 유입을 추가 구매수량으로 연결"}
 ];
 bucket.sort((x,y)=>(x.impact>=0)-(y.impact>=0)||Math.abs(y.impact)-Math.abs(x.impact));
 const sec=Array.isArray(intelligence?.sections)?intelligence.sections:[];
 const day=intelligence?.report_date||null,opsDay=operations.report_date;
 const weeklyChecked=weeklyEvidence&&
   ["reconciled","reconciled_with_taxonomy_warning"].includes(weeklyEvidence.status)&&
   weeklyEvidence.report_date===opsDay&&weeklyEvidence.current_period===w.period&&
   weeklyEvidence.previous_period===w.comparison_period&&
   weeklyEvidence.quality?.revenue_reconciled===true&&
   finite(weeklyEvidence.net_revenue_delta)&&Math.abs(weeklyEvidence.net_revenue_delta-data.delta)<=1;
 const chan=weeklyChecked?weeklyEvidence.channels||[]:[];
 const lookup=name=>chan.find(x=>x.channel===name);
 const numDelta=name=>lookup(name)?won(lookup(name).delta_revenue):"—";
 const channelMarkup=weeklyChecked?
   '<div class="macro-channel-main"><strong>유입 채널별 매출 변화</strong><small>전주 대비</small></div>'+
   '<div class="macro-channel-values">'+["Organic Traffic","Paid Ad","Owned Channel","etc"].map(n=>
     '<div><small>'+esc(n==="Organic Traffic"?"오가닉":n==="Paid Ad"?"Paid Ad":
       n==="Owned Channel"?"Owned/CRM":"기타")+'</small><b>'+esc(numDelta(n))+'</b></div>').join("")+
   '</div>':'<p class="macro-data-note">채널별 매출 변화는 집계 중입니다.</p>';
 const weather=sec.find(x=>x.number===4);
 const weatherMap=new Map((weather?.dates||[]).map((d,i)=>[d,weather?.temperature?.[i]]));
 const dayRange=(a,b)=>{
   if(!a||!b)return null;
   const rows=[];let d=new Date(a+"T00:00:00Z"),stop=new Date(b+"T00:00:00Z");
   if(!Number.isFinite(d.getTime())||!Number.isFinite(stop.getTime()))return null;
   for(let i=0;i<7&&d<=stop;i++,d.setUTCDate(d.getUTCDate()+1)){
     const key=d.toISOString().slice(0,10),v=weatherMap.get(key);
     if(!finite(v))return null;rows.push(v);
   }
   return rows.length===7&&d>stop?rows:null;
 };
 const wa=dayRange(currentDates[0],currentDates[1]),wb=dayRange(priorDates[0],priorDates[1]);
 const weatherDiff=wa&&wb?wa.reduce((a,b)=>a+b,0)/7-wb.reduce((a,b)=>a+b,0)/7:null;
 const context=[
  ...(outer?[{name:"OUTER 구매 비중",headline:outer.share_previous_pct.toFixed(1)+"% → "+outer.share_current_pct.toFixed(1)+"%",note:"주문 구성"}]:[]),
  ...(sale?[{name:"SALE 카테고리",headline:sale.share_previous_pct.toFixed(1)+"% → "+sale.share_current_pct.toFixed(1)+"%",note:"상품 분류상 SALE"}]:[]),
  ...(finite(weatherDiff)?[{name:"서울 기온",headline:"전주 대비 "+(weatherDiff>0?"+":"")+weatherDiff.toFixed(1)+"°C",note:"동일 기간 관측 · 판매 원인 단정 안 함"}]:[])
 ];
 const contextMarkup=context.map(x=>'<div class="macro-context-item"><b>'+esc(x.name)+'</b>'+
   '<strong>'+esc(x.headline)+'</strong><small>'+esc(x.note)+'</small></div>').join("");
 const diDayNote="";
 root.innerHTML='<div class="macro-heading"><div><small>BUSINESS FIRST · OVERALL SALES DIAGNOSIS</small>'+
  '<h2>이번 주 매출의 핵심 원인</h2><p>매출 영향 → 구매 행동 → 실행 실험</p></div>'+
  '<small class="macro-period">'+esc(w.period)+' vs '+esc(w.comparison_period)+diDayNote+'</small></div>'+
  '<div class="macro-headline"><div><span>최근 7일 매출 변화</span><strong class="'+(change<0?"loss":"gain")+'">'+signed(change)+'</strong>'+
  '<small>'+won(data.delta)+' · '+fmt(prev.revenue)+' → '+fmt(cur.revenue)+'원</small></div>'+
  '<div class="macro-conclusion">'+(data.aov<0&&data.aov<=data.conversion&&data.aov<=data.traffic?
    '<b>우선과제 · 주문당 구매수량 회복</b><p>유입보다 구매수량·상품 조합을 개선할 구간입니다.</p>':
    '<b>주요 지표를 비교합니다.</b><p>각 요인의 산술 기여와 방향을 확인하세요.</p>')+'</div></div>'+
  '<div class="macro-driver-list">'+bucket.map((x,i)=>card(i+1,x)).join("")+'</div>'+
  '<section class="macro-channel-check"><h3>채널별 매출 흐름</h3>'+channelMarkup+'</section>'+
  (context.length?'<div class="macro-context-title"><b>구매 상품군과 날씨</b></div>'+
   '<div class="macro-context-grid">'+contextMarkup+'</div>':'')+
  '<p class="macro-disclaimer">매출 기여액은 실제 관측 KPI의 산술 분해이며 실험에서 측정한 증분 효과는 아닙니다.</p>';
}
window.CSKMacroBoard={bridge,render};
if(window.__cskMacroPayload)render(window.__cskMacroPayload.operations,window.__cskMacroPayload.intelligence,window.__cskMacroPayload.weeklyEvidence,window.__cskMacroPayload.alignedBasket);
})();