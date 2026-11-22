/* Evidence-only contextual strip. No inferred causality or unsourced schedules. */
(()=>{
"use strict";
const root=document.getElementById("market-context-cards");if(!root)return;
const esc=v=>String(v??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const finite=v=>typeof v==="number"&&Number.isFinite(v);
const sign=v=>finite(v)?(v>0?"+":"")+v.toFixed(1)+"%":"—";
const signedInt=v=>finite(v)?(v>0?"+":"")+Math.round(v).toLocaleString("ko-KR"):"—";
const link=v=>typeof v==="string"&&/^https:\/\/[^<>"\s]+$/.test(v)?v:null;
const pct=(v)=>finite(v)?v.toFixed(1):"—";
function render(intelligence,operations,diagnoses){
 const reportDate=operations?.report_date||intelligence?.report_date||"";
 const sec=Array.isArray(intelligence?.sections)?intelligence.sections:[];
 const weather=sec.find(x=>x.number===4)||{},forecast=Array.isArray(weather.forecast)?weather.forecast:[];
 const next=forecast.find(x=>x.date>reportDate&&finite(x.min_temp)&&finite(x.max_temp));
 const dateIndex=Array.isArray(weather.dates)?weather.dates.indexOf(reportDate):-1;
 const observed=dateIndex>=0?weather.temperature?.[dateIndex]:null;
 const weatherValue=next?Math.round(next.min_temp)+"° / "+Math.round(next.max_temp)+"°":finite(observed)?pct(observed)+"°C":"관측 미확보";
 const weatherDate=next?next.date+" 예보":reportDate+" 서울 관측";
 const weatherBody=next?"서울 최저·최고 기온 · 실적 영향은 별도 검증":"기상청 관측 데이터, 매출과 동시 발생만 확인";

 const cards=Array.isArray(intelligence?.product_focus?.cards)?intelligence.product_focus.cards:[];
 const sales=cards.filter(x=>finite(x.sales_7d)&&finite(x.sales_previous_7d)&&finite(x.pdp_change_pct))
   .sort((a,b)=>Math.abs(b.sales_7d-b.sales_previous_7d)-Math.abs(a.sales_7d-a.sales_previous_7d))
   .slice(0,2);
 const productBody=sales.length?sales.map(x=>
   '<div class="ctx-product"><b title="'+esc(x.name)+'">'+esc(x.name)+'</b>'+
   '<span>PDP '+sign(x.pdp_change_pct)+' · GA4 판매 '+signedInt(x.sales_previous_7d)+' → '+signedInt(x.sales_7d)+'개</span></div>').join(""):
   '<p class="ctx-empty">동일 상품명 PDP·판매 비교 대상이 없습니다.</p>';

 const schedules=Array.isArray(operations?.calendar?.current)?operations.calendar.current:[];
 const own=schedules.filter(x=>x&&x.title).slice(0,2);
 const ownBody=own.length?own.map(x=>'<div class="ctx-product"><b>'+esc(x.title)+'</b>'+
   '<span>'+esc(x.date||x.start_date||reportDate)+' · '+esc(x.status||"일정 표본")+'</span></div>').join(""):
   '<p class="ctx-empty">공식 일정 데이터 미연동 · 실제 행사 없음으로 해석하지 않습니다.</p>';

 const events=Array.isArray(diagnoses?.market?.official_event_overlap)?diagnoses.market.official_event_overlap:[];
 const seen=new Set(),active=events.filter(x=>x&&x.start&&x.end&&x.start<=reportDate&&x.end>=reportDate)
   .sort((a,b)=>String(b.start).localeCompare(String(a.start)))
   .filter(x=>{const key=String(x.brand||"").toLowerCase();if(seen.has(key))return false;seen.add(key);return true}).slice(0,3);
 const eventBody=active.length?active.map(x=>{
   const url=link(x.url),item='<b>'+esc(x.brand)+'</b><span>'+esc(x.title)+'</span>';
   return url?'<a class="ctx-event" href="'+esc(url)+'" target="_blank" rel="noopener noreferrer">'+item+'</a>':
     '<div class="ctx-event">'+item+'</div>';
 }).join(""):'<p class="ctx-empty">기간이 검증된 경쟁사 행사 표본이 없습니다.</p>';
 root.innerHTML=
  '<article class="ctx-card"><small>SEOUL · WEATHER</small><strong>'+esc(weatherValue)+'</strong>'+
  '<span class="ctx-meta">'+esc(weatherDate)+'</span><p>'+esc(weatherBody)+'</p></article>'+
  '<article class="ctx-card"><small>PDP · SALES</small><strong>주요 상품 변화</strong>'+
  productBody+'<p>상품명 일치 GA4 표본 · 관리자 주문 대사 전</p></article>'+
  '<article class="ctx-card"><small>COLUMBIA · CALENDAR</small><strong>공식몰 일정</strong>'+
  ownBody+'<a class="ctx-source" href="../marketing_operations/index.html">운영 캘린더 확인</a></article>'+
  '<article class="ctx-card"><small>MARKET · PROMOTIONS</small><strong>경쟁사 행사 '+active.length+'건</strong>'+
  eventBody+'<p>공식 기간 검증 · 컬럼비아 매출 영향 미확정</p></article>';
 const dateEl=document.getElementById("market-context-date");
 if(dateEl)dateEl.textContent=reportDate?reportDate+" 기준":"관측 기준일 미확보";
}
window.CSKMarketContext={render};
if(window.__cskMarketPayload)render(window.__cskMarketPayload.intelligence,window.__cskMarketPayload.operations,window.__cskMarketPayload.diagnoses);
})();