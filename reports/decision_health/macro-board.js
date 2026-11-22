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
  '<div class="macro-next"><b>근거</b> '+esc(driver.proof||"동일 기간 운영 KPI의 산술 분해")+'</div>'+
  '<div class="macro-next"><b>검증 상태</b> '+esc(driver.confidence||"산술 확인 · 인과 미확정")+'</div>'+
  '<div class="macro-next"><b>우선 조치</b> '+esc(driver.next)+'</div></article>';
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
   '<div class="macro-basket-bridge"><div><span>주문당 상품수 변화</span><b>'+won(qEffect).replace("만 원","만원")+
   '</b><small>'+fmt(qEffect)+'원/건</small></div>'+
   '<div><span>상품당 실현매출 변화</span><b>'+fmt(pEffect)+'원/건</b></div>'+
   '<div><span>상품행·구매매출 차이</span><b>'+(gapEffect>0?"+":"")+fmt(gapEffect)+'원/건</b></div></div>':
   '';
 const basketNote=basketReconciled?
   "같은 7일 구간 GA4 구매구성 확인 · 주문당 상품수 "+basket.items_per_order_previous.toFixed(3)+
   "→"+basket.items_per_order_current.toFixed(3)+"개 / 상품당 실현매출 "+
   fmt(basket.realized_item_price_previous)+"→"+fmt(basket.realized_item_price_current)+"원":
   samePeriodBasket?"GA4 구매구성 기간 일치 · 운영 KPI와 정합성 미통과(상품수·가격 기여 미확정)":
   "같은 기간의 상품수·가격 구성이 아직 검증되지 않았습니다.";
 const cvBefore=prev.cvr*100,cvAfter=cur.cvr*100;
 const bucket=[
  {id:"aov",title:"객단가·구매 구성",impact:data.aov,
   metric:"객단가 "+fmt(prev.aov)+" → "+fmt(cur.aov)+"원 ("+signed(pct(cur.aov,prev.aov))+")",
   insight:data.aov<0?"주문은 늘었지만 주문 1건당 매출이 낮아졌습니다.":"주문당 매출이 전체 실적에 기여했습니다.",
   proof:basketNote,breakdown:basketBreakdown,
   confidence:basketReconciled?"동일 기간 GA4 구매 이벤트·운영 KPI 대사 통과 · 독립 ERP 검증 전":
     "운영 객단가 산술 확인 · 구매구성 원인은 검증 제한",
   next:basketReconciled?"1개 구매 주문 비중 · 고객/상품군별 구매수량 분포 조사":"동기간 GA4 구매구성·운영 지표 대사 우선"},
  {id:"conversion",title:"구매전환 효율",impact:data.conversion,
   metric:"CVR "+cvBefore.toFixed(3)+"% → "+cvAfter.toFixed(3)+"% ("+
     (cvAfter-cvBefore>0?"+":"")+(cvAfter-cvBefore).toFixed(3)+"%p)",
   insight:data.conversion<0?"방문 대비 주문 효율이 다소 약화됐습니다.":"방문 대비 주문 효율이 개선됐습니다.",
   proof:"전환율은 전체 세션·주문으로 계산. 기존 상품 퍼널의 비교 날짜는 다릅니다.",
   confidence:"전체 전환 산술 확인 · 구간별 구매이탈 원인은 미확정",
   next:"같은 기간 채널·세션 퍼널로 이탈 구간 검증"},
  {id:"traffic",title:"유입 규모",impact:data.traffic,
   metric:"세션 "+fmt(prev.sessions)+" → "+fmt(cur.sessions)+" ("+signed(pct(cur.sessions,prev.sessions))+")",
   insight:data.traffic>=0?"유입 증가는 매출 감소를 일부 상쇄했습니다.":"유입 감소가 전체 매출을 압박했습니다.",
   proof:"일별 전체 세션 합계(채널별 세션과 별개)",
   confidence:"전체 세션 변화 확인 · 유입 인과효과는 미확정",
   next:"채널 유입·고객구성 검증(채널 세션 품질 확인 후)"}
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
 const warnings=weeklyChecked?(weeklyEvidence.quality?.taxonomy_warnings||[]):[];
 const sessionMismatch=weeklyChecked?(weeklyEvidence.quality?.daily_channel_session_mismatch_dates||[]):[];
 const channelMarkup=weeklyChecked?
   '<div class="macro-channel-main"><strong>동일 기간 채널 매출 변화</strong><small>채널별 매출·주문은 주간 합계 검증 · 직접적인 증분 기여가 아님</small></div>'+
   (warnings.length?'<p class="macro-data-warning"><b>채널 분류 확인 1순위</b> ETC 등 미분류 채널 '+
       warnings.map(x=>esc(x.channel)+" "+won(-x.previous_revenue)+" (직전 매출의 "+x.share_of_previous_total_pct+"%)").join(" · ")+
       ' · 실제 수요 감소인지 분류 변경인지 미확정</p>':'')+
   '<div class="macro-channel-values">'+["etc","Organic Traffic","Paid Ad","Owned Channel"].map(n=>
     '<div><small>'+esc(n)+'</small><b>'+esc(numDelta(n))+'</b></div>').join("")+'</div>'+
   (sessionMismatch.length?
     '<p class="macro-data-note">주의: 14일 중 '+sessionMismatch.length+'일은 채널별 세션 합계가 전체 세션과 불일치. 채널별 CVR 분석을 보류합니다.</p>':
     '<p class="macro-data-note">채널별 세션 합계 정합성 확인 완료. 채널 매출의 인과 기여는 별도 검증이 필요합니다.</p>'):
   '<p class="macro-data-note">동일 기간 채널별 매출 대사 결과를 아직 확보하지 못해 원인 순위를 추가하지 않았습니다.</p>';
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
  {name:"채널 분류",headline:weeklyChecked?warnings.length?"미분류 ETC 변화 확인 필요":"동기간 채널 매출 대사 완료":"동기간 채널 근거 미확보",
    note:weeklyChecked?"채널 세션 정합성 "+(sessionMismatch.length?"제한("+sessionMismatch.length+"일 불일치)":"확인"):"주간 매출 영향으로 단정하지 않음"},
  {name:"신규·기존 고객",headline:"동일 주간 신규/기존 구매 구분 미확보",
    note:day&&day!==opsDay?"일별 고객 지표 "+day+" · 주간과 기간 불일치":"주문 이력·고객군 동일 기간 대사 필요"},
  {name:"서울 기온",headline:finite(weatherDiff)?"전주 대비 "+(weatherDiff>0?"+":"")+weatherDiff.toFixed(1)+"°C":"주간 관측 정합성 확인 전",
    note:finite(weatherDiff)?"운영 주간과 날짜 일치 · 기상청 관측 · 매출 인과 미확정":"기온 관측일과 운영 기간 일치 여부 확인 필요"}
 ];
 const contextMarkup=context.map(x=>'<div class="macro-context-item"><b>'+esc(x.name)+'</b>'+
   '<strong>'+esc(x.headline)+'</strong><small>'+esc(x.note)+'</small></div>').join("");
 const diDayNote=day&&day!==opsDay?' · '+esc(day)+' 단일일 분석과 기준 기간 다름':"";
 root.innerHTML='<div class="macro-heading"><div><small>BUSINESS FIRST · OVERALL SALES DIAGNOSIS</small>'+
  '<h2>매출을 움직인 큰 요인</h2><p>상품보다 먼저 전체 매출 구조를 진단합니다.</p></div>'+
  '<small class="macro-period">'+esc(w.period)+' vs '+esc(w.comparison_period)+diDayNote+'</small></div>'+
  '<div class="macro-headline"><div><span>최근 7일 매출 변화</span><strong class="'+(change<0?"loss":"gain")+'">'+signed(change)+'</strong>'+
  '<small>'+won(data.delta)+' · '+fmt(prev.revenue)+' → '+fmt(cur.revenue)+'원</small></div>'+
  '<div class="macro-conclusion">'+(data.aov<0&&data.aov<=data.conversion&&data.aov<=data.traffic?
    '<b>핵심 문제: 객단가·구매 구성</b><p>유입 확대에도 주문당 매출 하락이 더 컸습니다.</p>':
    '<b>주요 지표를 비교합니다.</b><p>각 요인의 산술 기여와 방향을 확인하세요.</p>')+'</div></div>'+
  '<div class="macro-driver-list">'+bucket.map((x,i)=>card(i+1,x)).join("")+'</div>'+
  '<section class="macro-channel-check"><h3>채널별 변화 · 데이터 품질 확인</h3>'+channelMarkup+
  '<small class="macro-channel-source">근거: <a href="./weekly_business_evidence.json">14일 동기간 집계 원본</a> · 이 값은 위 Shapley 요인에 다시 더하지 않습니다.</small></section>'+
  '<div class="macro-context-title"><b>고객·환경 보조 신호</b><small>관측 시점과 집계 정의를 검증한 정보만 사용</small></div>'+
  '<div class="macro-context-grid">'+contextMarkup+'</div>'+
  '<p class="macro-disclaimer">근거: <a href="../marketing_operations/latest.json">7일 운영 집계</a>의 세션 × 주문전환율 × 객단가 Shapley 산술 분해. '+
  '3개 요인의 합계는 매출 증감과 일치하며 광고·할인·날씨의 인과 효과를 의미하지 않습니다. '+esc(w.caveat||"")+'</p>';
}
window.CSKMacroBoard={bridge,render};
if(window.__cskMacroPayload)render(window.__cskMacroPayload.operations,window.__cskMacroPayload.intelligence,window.__cskMacroPayload.weeklyEvidence,window.__cskMacroPayload.alignedBasket);
})();