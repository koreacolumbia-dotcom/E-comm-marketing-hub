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
  '<div class="macro-next"><b>다음 검증</b> '+esc(driver.next)+'</div></article>';
};
function render(operations,intelligence){
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
 const cvBefore=prev.cvr*100,cvAfter=cur.cvr*100;
 const bucket=[
  {id:"aov",title:"객단가·구매 구성",impact:data.aov,
   metric:"객단가 "+fmt(prev.aov)+" → "+fmt(cur.aov)+"원 ("+signed(pct(cur.aov,prev.aov))+")",
   insight:data.aov<0?"주문은 늘었지만 주문 1건당 매출이 낮아졌습니다.":"주문당 매출이 전체 실적에 기여했습니다.",
   next:"주문당 상품수 · 상품군/가격 구성 · 할인"},
  {id:"conversion",title:"구매전환 효율",impact:data.conversion,
   metric:"CVR "+cvBefore.toFixed(3)+"% → "+cvAfter.toFixed(3)+"% ("+
     (cvAfter-cvBefore>0?"+":"")+(cvAfter-cvBefore).toFixed(3)+"%p)",
   insight:data.conversion<0?"방문 대비 주문 효율이 다소 약화됐습니다.":"방문 대비 주문 효율이 개선됐습니다.",
   next:"채널별 세션→주문 · 장바구니→구매"},
  {id:"traffic",title:"유입 규모",impact:data.traffic,
   metric:"세션 "+fmt(prev.sessions)+" → "+fmt(cur.sessions)+" ("+signed(pct(cur.sessions,prev.sessions))+")",
   insight:data.traffic>=0?"유입 증가는 매출 감소를 일부 상쇄했습니다.":"유입 감소가 전체 매출을 압박했습니다.",
   next:"채널별 유입량 · 방문 품질 · 신규/기존 구성"}
 ];
 bucket.sort((x,y)=>(x.impact>=0)-(y.impact>=0)||Math.abs(y.impact)-Math.abs(x.impact));
 const sec=Array.isArray(intelligence?.sections)?intelligence.sections:[];
 const customer=sec.find(x=>x.number===10),channel=sec.find(x=>x.number===11);
 const day=intelligence?.report_date||null,opsDay=operations.report_date;
 const channelValid=day&&channel?.status==="partial"&&Array.isArray(channel.channels);
 const paid=channelValid?channel.channels.find(x=>x.channel==="Paid Ad"):null;
 const owned=channelValid?channel.channels.find(x=>x.channel==="Owned Channel"):null;
 const customerValid=day&&customer?.buyer_status==="available"&&finite(customer.new_mix_pct)&&finite(customer.observed_buyers);
 const temps=intelligence?.product_focus?.temperature;
 const weatherValid=Array.isArray(temps)&&temps.length===14&&temps.every(finite);
 const currentTemp=weatherValid?temps.slice(7).reduce((a,b)=>a+b,0)/7:null;
 const priorTemp=weatherValid?temps.slice(0,7).reduce((a,b)=>a+b,0)/7:null;
 const weatherSource=intelligence?.product_focus?.weather_source||"출처 미확보";
 const context=[
  {name:"채널·CRM",headline:paid&&owned?"Paid "+signed(paid.revenue_yoy_pct)+" · Owned "+signed(owned.revenue_yoy_pct):"같은 기준의 채널 변화 미확보",
   note:(day||"기준일 미확보")+" 단일일 전년비 · 주간 기여액에 더하지 않음",
   url:"../daily_intelligence/index.html"},
  {name:"신규·기존 고객",headline:customerValid?"신규 구매자 "+customer.new_mix_pct.toFixed(1)+"%":"비교 가능한 구매자 정보 미확보",
   note:customerValid?fmt(customer.new_buyers)+"명 신규 / "+fmt(customer.existing_buyers)+"명 기존 · GA4 관측 기준":"GA4 고객 구분 관측 필요",
   url:"../daily_intelligence/index.html"},
  {name:"외부 환경",headline:weatherValid?"서울 평균기온 "+(currentTemp-priorTemp>0?"+":"")+(currentTemp-priorTemp).toFixed(1)+"°C":"서울 관측 기온 미확보",
   note:weatherValid?"최근 7일 vs 직전 7일 · "+weatherSource+" · 인과 미확정":"기온이 빠져도 매출 원인으로 추정하지 않음",
   url:"../daily_intelligence/index.html"}
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
  '<div class="macro-context-title"><b>상위 원인의 추가 확인 축</b><small>채널·고객·기온은 기간 및 지표 정의가 달라 기여액과 합산하지 않음</small></div>'+
  '<div class="macro-context-grid">'+contextMarkup+'</div>'+
  '<p class="macro-disclaimer">근거: <a href="../marketing_operations/latest.json">7일 운영 집계</a>의 세션 × 주문전환율 × 객단가 Shapley 산술 분해. '+
  '3개 요인의 합계는 매출 증감과 일치하며 광고·할인·날씨의 인과 효과를 의미하지 않습니다. '+esc(w.caveat||"")+'</p>';
}
window.CSKMacroBoard={bridge,render};
if(window.__cskMacroPayload)render(window.__cskMacroPayload.operations,window.__cskMacroPayload.intelligence);
})();