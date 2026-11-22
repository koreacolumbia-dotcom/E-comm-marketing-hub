/* Winter Festival only: accessible progressive enhancement and source-governed Meta/Paid analysis.
   Data is sourced from the aggregate daily snapshot, never Meta Ads API or raw customer data. */
(function(){
"use strict";
var root=document.getElementById("fm-integrated");
if(!root)return;
document.body.classList.add("festival-motion");
var $=function(s){return root.querySelector(s)};
var app={data:null,view:"weekly",filter:"all",metric:"revenue",drawer:null,lastFocus:null};
var NF=new Intl.NumberFormat("ko-KR");
var note={
revenue:"GA4 세션 귀속 매출입니다. 광고 플랫폼의 전환가치 또는 캠페인 순증 매출이 아닙니다.",
orders:"GA4 세션과 연결된 구매 거래수입니다. 광고 관리자 전환 지표와 일치하지 않을 수 있습니다.",
sessions:"GA4 세션 수입니다. 광고 플랫폼 클릭·도달·노출이 아닙니다.",
cvr:"GA4 구매 거래수 ÷ GA4 세션 수입니다. 신규가입 CVR이나 리드 CVR이 아닙니다."};
function esc(s){return String(s===null||s===undefined?"":s).replace(/[&<>"']/g,function(c){return {"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]})}
function num(s){var v=Number(s);return Number.isFinite(v)&&v>=0?v:0}
function fmt(v,k){
 if(v===null||v===undefined)return "미확보";
 if(k==="cvr")return Number(v).toFixed(2)+"%";
 if(k==="revenue"){var n=num(v);return n>=100000000?(n/100000000).toFixed(2)+"억원":n>=10000?(n/10000).toFixed(1)+"만원":NF.format(Math.round(n))+"원";}
 return NF.format(Math.round(num(v)))+(k==="orders"?"건":"회");
}
function percent(a,b,k){if(k==="cvr"){if(a===null||b===null)return {value:"미확보",down:false};var pp=b-a;return {value:(pp>0?"+":"")+pp.toFixed(2)+"%p",down:pp<0};}
 if(a===0){return {value:b>0?"새로 관측":"0%",down:false};}
 var val=(b/a-1)*100;return {value:(val>0?"+":"")+val.toFixed(1)+"%",down:val<0};
}
function oneMetric(x,k){if(!x)return null;if(k==="cvr"){return x.cvr_pct===null?null:num(x.cvr_pct);}return typeof x[k]==="number"?x[k]:null;}
function twoBars(a,b,k){var max=Math.max(a===null?0:a,b===null?0:b,1),one=+(100*(a===null?0:a)/max).toFixed(1),two=+(100*(b===null?0:b)/max).toFixed(1);
return '<div class="fm-bars"><div class="fm-row"><span>2025/전주</span><div class="fm-line"><div class="fm-fill" style="width:'+one+'%"></div></div><b>'+esc(fmt(a,k))+'</b></div>'+
'<div class="fm-row"><span>2026/이번주</span><div class="fm-line"><div class="fm-fill current" style="width:'+two+'%"></div></div><b>'+esc(fmt(b,k))+'</b></div></div>';}
var EVENT_KINDS={
meta_conversion_tagged:"meta",meta_bd_tagged:"meta",instagram_paid_tagged:"meta",naver_brand_search:"paid",naver_search_other:"paid",
google_paid:"paid",criteo:"paid",other_display:"paid",owned_crm_tagged:"crm",
organic_search:"organic",referral:"organic",direct:"direct",other:"other"};
function getRows(){
 var d=app.data;if(!d)return [];
 if(app.view==="event"){
  var result=d.event_media||{},old=((result.years||{})["2025"]||{}).groups||[],now=((result.years||{})["2026"]||{}).groups||[];
  var map=new Map();
  old.forEach(function(x){map.set(x.id,{id:x.id,label:x.label||x.id,kind:EVENT_KINDS[x.id]||"other",previous:x,current:{sessions:0,orders:0,revenue:0,cvr_pct:null}})});
  now.forEach(function(x){
   var e=map.get(x.id)||{id:x.id,label:x.label||x.id,kind:EVENT_KINDS[x.id]||"other",previous:{sessions:0,orders:0,revenue:0,cvr_pct:null}};
   e.current=x;map.set(x.id,e);
  });
  return Array.from(map.values());
 }
 return ((d.weekly_media||{}).rows||[]).map(function(r){return Object.assign({},r,{kind:r.kind==="paid"&&String(r.id).startsWith("meta")?"meta":r.kind==="paid"&&r.id==="facebook_banner"?"meta":r.kind})});
}
function hasData(){
 var d=app.data;return d && (d.weekly_media||{}).status==="ga4_measured" || d&&(d.event_media||{}).status==="ga4_measured";
}
function selectedRows(){
 return getRows().filter(function(r){
  if(app.filter==="all")return true;
  if(app.filter==="meta")return r.kind==="meta";
  if(app.filter==="paid")return r.kind==="paid"||r.kind==="meta";
  return r.kind===app.filter;
 }).sort(function(a,b){
  return num(oneMetric(b.current,app.metric))-num(oneMetric(a.current,app.metric));
 });
}
function unavailable(){
 return '<div class="fm-state warn">요청한 분류의 검증된 세션·구매 데이터가 없습니다. 0건으로 해석하지 않습니다.</div>';
}
function renderLiveAggregate(){
 var rootEl=$("#fm-live-aggregate"),d=app.data||{},e=d.event||{};
 if(!rootEl)return;
 if(e.status!=="verified_ga4"||!e.totals?.current||!e.totals?.previous){
  rootEl.innerHTML='<span>행사 누적 GA4 확정 데이터를 확인하는 중입니다. 기존 D1~D2 검증표는 유지합니다.</span>';
  return;
 }
 var a=e.totals.previous,b=e.totals.current,rev=percent(a.revenue,b.revenue,"revenue");
 rootEl.innerHTML='<div class="fm-live-label">현재 확정된 누적 구간 · D1~D'+esc(e.days)+
    ' <span>'+esc(e.daily?.[0]?.current_date||"")+" ~ "+esc(e.daily?.at(-1)?.current_date||"")+'</span></div>'+
   '<div class="fm-live-line"><strong>'+esc(fmt(b.revenue,"revenue"))+'</strong>'+
     '<span class="fm-delta '+(rev.down?"down":"")+'">'+esc(rev.value)+' YoY</span></div>'+
   '<p>전년 동일 경과일 '+esc(fmt(a.revenue,"revenue"))+' · '+esc(fmt(b.orders,"orders"))+
     ' / '+esc(fmt(b.sessions,"sessions"))+' / CVR '+esc(fmt(b.cvr_pct,"cvr"))+
     '. 아래 기존 D1–D2 표는 최초 검증 기준을 보존합니다.</p>';
}

function renderPaidTotal(){
 var slot=$("#fm-paid-total");if(!slot)return;
 var source=(app.data||{}).event_media||{},years=source.years||{},cur=years["2026"],prev=years["2025"];
 if(source.status!=="ga4_measured"||!cur||!prev){slot.innerHTML="<b>Paid 태그 기준 매출</b> · 동일 행사일 전년 매체 집계 대기. 최근 7일 수치와 섞지 않습니다.";return;}
 var selected=new Set(["naver_brand_search","naver_search_other","google_paid","meta_conversion_tagged",
                       "instagram_paid_tagged","meta_bd_tagged","criteo","other_display"]);
 function totals(a){return (a.groups||[]).reduce(function(t,r){
  if(selected.has(r.id)){t.revenue+=num(r.revenue);t.orders+=num(r.orders);t.sessions+=num(r.sessions)}return t;
 },{revenue:0,orders:0,sessions:0})}
 var a=totals(prev),b=totals(cur),q=percent(a.revenue,b.revenue,"revenue");
 slot.innerHTML='<div class="fm-paid-summary"><div><span>식별된 Paid 소스/매체 · 행사 D1~D'+
  (source.period?.["2026"]?.[1]==="2026-11-22"?"3":"2")+
  '</span><strong>'+esc(fmt(b.revenue,"revenue"))+'</strong>'+
  '<small>2025 '+esc(fmt(a.revenue,"revenue"))+' → 2026 · '+esc(fmt(b.orders,"orders"))+' 구매</small></div>'+
  '<em class="'+(q.down?"down":"")+'">'+esc(q.value)+'</em></div>'+
  '<p class="fm-note">GA4 식별 가능한 Paid 소스만 합산. 기타/미분류는 제외하며 Meta 광고관리자 비용·전환가치·ROAS와는 별개입니다.</p>';
}

function renderReasons(){
 var d=app.data||{},area=$("#fm-reasons");if(!area)return;
 var em=d.event_media||{},ys=em.years||{},a=((ys["2025"]||{}).groups)||[],b=((ys["2026"]||{}).groups)||[];
 var period=em.period||{},matched=em.status==="ga4_measured"&&a.length&&b.length;
 if(!matched){
  area.innerHTML='<article class="fm-driver"><span>근거 1 · Meta</span><strong>유입 목적별 분리</strong><p>Meta / banner와 META_BD를 구분하여 GA4 구매전환을 확인합니다.</p></article>'+
  '<article class="fm-driver"><span>근거 2 · Paid</span><strong>행사 단독 집계 대기</strong><p>현재 확보한 7일 자료는 전주 비교만 가능합니다. 행사일 채널 분석과 분리합니다.</p></article>'+
  '<article class="fm-driver"><span>근거 3 · 비용</span><strong>ROAS 계산 보류</strong><p>광고비 원본이 행사 날짜와 일치하지 않아 효율을 확정할 수 없습니다.</p></article>';return;
 }
 var prev=new Map(a.map(function(x){return [x.id,x]})),cur=new Map(b.map(function(x){return [x.id,x]}));
 var joined=new Set(a.concat(b).map(function(x){return x.id}));
 var all=Array.from(joined).map(function(id){
  var x=prev.get(id)||{revenue:0},y=cur.get(id)||{revenue:0},label=(y.label||x.label||id);
  return {id:id,label:label,diff:y.revenue-x.revenue,prior:x.revenue,current:y.revenue};
 });
 var top=all.filter(function(x){return x.id!=="other"}).sort(function(x,y){return y.diff-x.diff})[0];
 var mb=all.find(function(x){return x.id==="meta_conversion_tagged"})||{diff:0};
 var bd=all.find(function(x){return x.id==="meta_bd_tagged"})||{diff:0};
 var unmapped=all.find(function(x){return x.id==="other"})||{current:0,prior:0};
 var paidCost=d.paid_cost||{};
 function delta(v){return (v>0?"+":v<0?"−":"")+fmt(Math.abs(v),"revenue")}
 var evt=((d.event||{}).totals)||{};
 var periodLabel="본행사 "+((period["2026"]||[]).join(" ~ "));
 var html='<article class="fm-driver"><span>근거 1 · 성장 상위 채널</span><strong>'+esc(top?top.label:"채널 미확보")+'</strong>'+
 '<div class="fm-driver-value">'+esc(top?delta(top.diff):"미확보")+'</div>'+
 '<p>'+esc(periodLabel)+'의 GA4 소스/매체 귀속 변화액입니다. 직접적인 캠페인 순증 효과는 아닙니다.</p></article>'+
 '<article class="fm-driver"><span>근거 2 · Meta 내부 차이</span><strong>Meta 배너와 META_BD 분리</strong>'+
 '<div class="fm-driver-value">'+esc(delta(mb.diff))+' <small>배너</small></div>'+
 '<p>META_BD 귀속 매출 변화 '+esc(delta(bd.diff))+'. 태그·목적별로 구분해야 매출과 유입을 혼동하지 않습니다.</p></article>'+
 '<article class="fm-driver"><span>근거 3 · 우선 점검</span><strong>소스 미분류와 광고비</strong>'+
 '<div class="fm-driver-value">'+esc(fmt(unmapped.current,"revenue"))+' <small>기타 귀속</small></div>'+
 '<p>기타 전년 '+esc(fmt(unmapped.prior,"revenue"))+' / 최신 광고비 '+esc(paidCost.latest_date||"미확보")+'까지 확인. 매체 태깅 및 ROAS 재검증 필요.</p></article>';
 area.innerHTML=html;
}

function render(){
 var d=app.data;
 if(!d){$("#fm-items").innerHTML='<div class="fm-state warn">데이터 집계 원본을 불러오지 못했습니다. 기존 검증된 행사 성과 차트는 그대로 유지됩니다.</div>';return;}
 var period=app.view==="event"?(d.event_media||{}).period:(d.weekly_media||{}).period;
 var labels=app.view==="event"
 ? "같은 행사 D1~D"+(((d.event_media||{}).period||{})["2026"]?.[1]==="2026-11-22"?"3":"2")+" · 2025 vs 2026"
 : "7일 기준 · 전주 대비 · 행사 단독 기여 아님";
 $("#fm-compare-note").textContent=labels;
 var windowEl=$("#fm-period");
 if(period&&app.view==="weekly"&&period.current&&period.previous){
  windowEl.textContent=period.current.start+" ~ "+period.current.end+" / 비교 "+period.previous.start+" ~ "+period.previous.end;
 }else if(period&&app.view==="event"){
  windowEl.textContent="2026 "+((period["2026"]||[]).join("~"))+" · 2025 "+((period["2025"]||[]).join("~"));
 }else windowEl.textContent="확정 기간 없음";
 renderLiveAggregate();
 renderPaidTotal();
 renderReasons();
 var rows=selectedRows();$("#fm-count").textContent=rows.length+"개 유입 그룹";
 $("#fm-items").innerHTML=rows.length?rows.map(function(r,i){
  var k=app.metric,a=oneMetric(r.previous,k),b=oneMetric(r.current,k),p=percent(a===null?0:a,b===null?0:b,k),isEmpty=a===null&&b===null;
  var kpis=app.metric==="revenue"?"세션 "+fmt(oneMetric(r.current,"sessions"),"sessions")+" · 구매 "+fmt(oneMetric(r.current,"orders"),"orders"):"매출 "+fmt(oneMetric(r.current,"revenue"),"revenue")+" · CVR "+fmt(oneMetric(r.current,"cvr"),"cvr");
  var prevLbl=app.view==="event"?"2025":"전주",nowLbl=app.view==="event"?"2026":"이번주";
  var max=Math.max(a===null?0:a,b===null?0:b,1);
  return '<article class="fm-medium" style="animation-delay:'+Math.min(i*35,245)+'ms"><div class="fm-medium-head">'+
   '<div><div class="fm-medium-title">'+esc(r.label)+'</div><small>'+esc(r.source_medium||r.id)+'</small></div>'+
   '<span class="fm-delta '+(p.down?"down":"")+'">'+esc(isEmpty?"미확보":p.value)+'</span></div>'+
   '<div class="fm-figure">'+esc(fmt(b,k))+'</div>'+
   '<div class="fm-bars"><div class="fm-row"><span>'+prevLbl+'</span><div class="fm-line"><div class="fm-fill" style="width:'+((a||0)/max*100).toFixed(1)+'%"></div></div><b>'+esc(fmt(a,k))+'</b></div>'+
   '<div class="fm-row"><span>'+nowLbl+'</span><div class="fm-line"><div class="fm-fill current" style="width:'+((b||0)/max*100).toFixed(1)+'%"></div></div><b>'+esc(fmt(b,k))+'</b></div></div>'+
   '<p class="fm-under">'+esc(kpis)+'</p><button type="button" class="fm-action" data-fm-detail="'+esc(r.id)+'" aria-label="'+esc(r.label)+' 상세 근거 열기">근거 상세</button></article>';
 }).join(""):unavailable();
 $("#fm-items").querySelectorAll("[data-fm-detail]").forEach(function(btn){
  btn.addEventListener("click",function(){openDrawer(btn.getAttribute("data-fm-detail"))});
 });
 var paid=d.paid_cost||{},week=d.weekly_media||{},event=d.event||{},evMedia=d.event_media||{};
 var quality=[];
 quality.push("행사 전체 GA4: "+(event.status==="verified_ga4"?"D1~D"+event.days+" 동일 요일 실측":"확정 비교 대기"));
 quality.push("행사 채널별 GA4: "+(evMedia.status==="ga4_measured"?"별도 실측":"집계 대기 · 7일 보조 자료 표시"));
 quality.push("Meta/유료 매체 광고비: "+(paid.roas_eligible?"검증됨":"미확보 / ROAS 보류")+(paid.latest_date?" · 마지막 "+paid.latest_date:""));
 quality.push("전주 채널자료: "+(week.date_aligned_with_daily_kpi?"일자 일치":"집계 기준일 별도 표기"));
 $("#fm-health").innerHTML='<b>원본 신선도</b> · '+quality.map(esc).join("　|　");
 $("#fm-tip").textContent=note[app.metric];
 $("#fm-source").textContent="업데이트 원본 "+(d.report_date||"미확인")+" · 보고서 생성 "+(d.generated_at_kst||"시간 미확인");
 var canEvent=evMedia.status==="ga4_measured"&&evMedia.years&&evMedia.years["2025"]&&evMedia.years["2026"];
 $("#fm-event-mode").disabled=!canEvent;
 $("#fm-event-mode").title=canEvent?"2025/2026 행사 동일 경과일 비교":"행사 채널별 GA4 검증 집계 대기";
 if(!canEvent && app.view==="event"){app.view="weekly";return render();}
 root.querySelectorAll("[data-fm-view]").forEach(function(b){b.setAttribute("aria-pressed",String(app.view===b.getAttribute("data-fm-view")))});
 root.querySelectorAll("[data-fm-filter]").forEach(function(b){b.setAttribute("aria-selected",String(app.filter===b.getAttribute("data-fm-filter")))});
}
function metaCampaigns(row){
 var d=app.data||{},m=d.event_media||{},years=m.years||{};
 var list=[];
 ["2025","2026"].forEach(function(y){
  (((years[y]||{}).meta_campaign_tags)||[]).filter(function(c){return c.group_id===row.id}).slice(0,8).forEach(function(c){
   list.push('<div class="fm-state"><strong>'+esc(y+" · "+c.campaign)+'</strong><div>'+esc(fmt(c.sessions,"sessions"))+' · '+esc(fmt(c.orders,"orders"))+' · '+esc(fmt(c.revenue,"revenue"))+'</div></div>');
  });
 });
 return list.length?'<h3>해당 계열의 행사 UTM 태그</h3>'+list.join(""):
 '<div class="fm-state">Meta Ads 광고세트·소재 성과 원본은 미연결. GA4 세션/구매 외 Meta 관리자 성과를 추정하지 않습니다.</div>';
}
function openDrawer(id){
 var row=selectedRows().find(function(x){return x.id===id});if(!row)return;
 var title=esc(row.label),d=app.data||{},paid=d.paid_cost||{};
 var detail=document.getElementById("fm-drawer");
 if(!detail)return;
 var v=detail.querySelector("#fm-sheet-content");
 var kind=row.kind==="meta"?"Meta":"채널";
 v.innerHTML='<div class="fm-overline">'+kind+' · GA4 ATTRIBUTION</div><h2>'+title+'</h2>'+
 '<p>확인된 유입 태그: <b>'+esc(row.source_medium||row.id)+'</b></p>'+
 '<div class="fm-state"><strong>2025 또는 전주</strong> · '+fmt(row.previous.revenue,"revenue")+' / '+fmt(row.previous.orders,"orders")+' / '+fmt(row.previous.sessions,"sessions")+'</div>'+
 '<div class="fm-state"><strong>2026 또는 이번 주</strong> · '+fmt(row.current.revenue,"revenue")+' / '+fmt(row.current.orders,"orders")+' / '+fmt(row.current.sessions,"sessions")+'</div>'+
 '<p>'+esc(note[app.metric])+'</p>'+
 (row.kind==="meta"?metaCampaigns(row):"")+
 ((app.view==="event"&&Array.isArray(row.current.top_sources)&&row.current.top_sources.length)?
  '<h3>세부 소스/매체 · 실측 주요 항목</h3>'+row.current.top_sources.slice(0,6).map(function(s){
    return '<div class="fm-state"><strong>'+esc(s.source_medium)+'</strong> · '+fmt(s.revenue,"revenue")+
      ' / '+fmt(s.orders,"orders")+' / '+fmt(s.sessions,"sessions")+'</div>';
  }).join(""):"")+
 '<div class="fm-state warn"><b>인과 효과·광고 플랫폼 실적과 구분</b><p>광고비 기준일: '+esc(paid.latest_date||"미확보")+'. 최신 광고비, 광고관리자 전환가치, ROAS와 CPA는 검증된 원본이 없으면 표시하지 않습니다.</p></div>';
 app.lastFocus=document.activeElement;detail.classList.add("open");detail.setAttribute("aria-hidden","false");
 document.body.style.overflow="hidden";detail.querySelector(".fm-close").focus();
}
function closeDrawer(){
 var detail=document.getElementById("fm-drawer");if(!detail)return;
 detail.classList.remove("open");detail.setAttribute("aria-hidden","true");document.body.style.overflow="";
 if(app.lastFocus&&app.lastFocus.focus)app.lastFocus.focus();
}
function toast(message){
 var box=document.createElement("div");box.className="fm-toast";box.setAttribute("role","status");box.textContent=message;
 document.body.appendChild(box);setTimeout(function(){box.remove()},3400);
}
function fallback(ops,weekly,intel,utm){
 var wd=weekly||{},i=intel||{},p=((i.integrations||{}).paid_cost)||{};
 var targets={
 "meta / banner":["meta_banner","Meta / banner","paid"],
 "META_BD / META_BD":["meta_bd","META_BD / META_BD","paid"],
 "Facebook / banner":["facebook_banner","Facebook / banner","paid"],
 "naverbs / cpc":["naver_brand","네이버 브랜드검색","paid"],
 "naver / cpc":["naver_cpc","네이버 일반검색","paid"],
 "criteo / banner":["criteo","Criteo","paid"],
 "kakao / da":["kakao_da","카카오 DA","paid"],
 "KAKAO_CH / KAKAO_CH":["kakao_owned","카카오 채널","crm"],
 "LMS_ECOM / LMS_ECOM":["lms_owned","LMS","crm"],
 "EDM / EDM":["edm_owned","EDM","crm"],
 "google / organic":["google_organic","Google 자연검색","organic"],
 "naver / organic":["naver_organic","Naver 자연검색","organic"]
 };
 var rows=((wd.rows)||[]).filter(function(x){return targets[x.source_medium]}).map(function(x){
 var meta=targets[x.source_medium];
 function met(y){var a=x[y]||{},s=num(a.sessions),o=num(a.transactions),r=num(a.revenue);
 return {sessions:s,orders:o,revenue:r,cvr_pct:s?o/s*100:null}}
 return {id:meta[0],label:meta[1],source_medium:x.source_medium,kind:meta[2],
 previous:met("previous"),current:met("current")};
 });
 return {report_date:ops?.report_date||wd.report_date,generated_at_kst:"대체 원천으로 실시간 조회",
 event:{status:"not_ready",days:0},
 event_media:{status:"not_ready",years:{}},
 weekly_media:{status:wd.status,report_date:wd.report_date,period:wd.period,rows:rows,
 date_aligned_with_daily_kpi:wd.report_date===ops?.report_date},
 paid_cost:{status:p.status,latest_date:p.latest_date,roas_eligible:false},
 owned_utm:{status:utm?.status||"not_ready"}};
}
async function getJSON(path){
 var response=await fetch(path+(path.indexOf("?")<0?"?":"&")+"r="+Date.now(),{cache:"no-store"});
 if(!response.ok)throw Error("HTTP "+response.status);
 return response.json();
}
async function load(interactive){
 $("#fm-health").textContent="원본 확인 중 · 최신 데이터와 기간 일치 여부를 확인합니다.";
 try{
  var snapshot=await getJSON("./winter_festival_integrated_latest.json");
  if(snapshot.schema!==2)throw Error("schema mismatch");
  app.data=snapshot;
 }catch(e){
  try{
   var all=await Promise.all([
    getJSON("./source_medium_weekly.json"),
    getJSON("../marketing_operations/latest.json"),
    getJSON("../marketing_intelligence_v2/latest.json"),
    getJSON("./winter_festival_utm_owned_2026.json")]);
   app.data=fallback(all[1],all[0],all[2],all[3]);
  }catch(e2){app.data=null;}
 }
 render();
 if(interactive)toast(app.data?"검증된 원본 기준으로 화면을 갱신했습니다.":"새 데이터를 가져오지 못했습니다.");
}
root.querySelectorAll("[data-fm-view]").forEach(function(el){el.addEventListener("click",function(){app.view=el.getAttribute("data-fm-view");render()})});
root.querySelectorAll("[data-fm-filter]").forEach(function(el){el.addEventListener("click",function(){app.filter=el.getAttribute("data-fm-filter");render()})});
$("#fm-metric").addEventListener("change",function(e){app.metric=e.target.value;render()});
$("#fm-refresh").addEventListener("click",function(){load(true)});
$("#fm-method").addEventListener("click",function(){openMethod()});
var drawer=document.getElementById("fm-drawer");
if(drawer){
 drawer.querySelectorAll("[data-fm-close]").forEach(function(el){el.addEventListener("click",closeDrawer)});
 document.addEventListener("keydown",function(e){
  if(e.key==="Escape"&&drawer.classList.contains("open"))closeDrawer();
  if(e.key!=="Tab"||!drawer.classList.contains("open"))return;
  var focusables=Array.from(drawer.querySelectorAll("button,[href],select,[tabindex]:not([tabindex='-1'])"));
  if(!focusables.length)return;var first=focusables[0],last=focusables[focusables.length-1];
  if(e.shiftKey&&document.activeElement===first){e.preventDefault();last.focus();}
  else if(!e.shiftKey&&document.activeElement===last){e.preventDefault();first.focus();}
 });
}
function openMethod(){
 var d=document.getElementById("fm-drawer");if(!d)return;
 app.lastFocus=document.activeElement;
 d.querySelector("#fm-sheet-content").innerHTML='<div class="fm-overline">EVIDENCE / METHODOLOGY</div><h2>성과 해석 기준</h2>'+
 '<p><b>01 · 본행사 비교</b> 2026년 11월 20~21일과 2025년 11월 21~22일을 동일 경과일과 요일로 맞춥니다. 2026년 11월 22일 데이터가 검증되면 D3를 추가합니다.</p>'+
 '<p><b>02 · 채널별 행사 실측</b> BigQuery 세션-소스/매체를 UTM 분석과 동일한 단일 비용 제한 쿼리에서 함께 산출합니다. 채널별 전년 비교는 같은 행사 경과일 자료를 확보했을 때만 표시합니다.</p>'+
 '<p><b>03 · 보조 비교</b> 최근 7일과 직전 7일의 GA4 소스/매체 차이입니다. 행사 단독 효과나 전년 변화율이 아닙니다.</p>'+
 '<p><b>04 · Meta</b> meta / banner와 META_BD 등의 유입을 따로 표시합니다. GA4 소스/매체와 UTM 캠페인은 Meta Ads 관리자 보고서가 아닙니다.</p>'+
 '<p><b>05 · ROAS/CPA</b> 최신 기간과 일치하는 광고비가 검증되지 않는 한 계산하지 않습니다. CRM 매출은 사이트 매출에 더하지 않습니다.</p>';
 d.classList.add("open");d.setAttribute("aria-hidden","false");document.body.style.overflow="hidden";d.querySelector(".fm-close").focus();
}
// Animate only visible cards and suppress all motion for accessibility preferences.
var reduce=window.matchMedia&&window.matchMedia("(prefers-reduced-motion: reduce)").matches;
if(!reduce && "IntersectionObserver" in window){
 var observer=new IntersectionObserver(function(entries){entries.forEach(function(x){
  if(x.isIntersecting){x.target.classList.add("fm-in");observer.unobserve(x.target)}
 })},{rootMargin:"0px 0px -25px 0px",threshold:.05});
 Array.from(document.querySelectorAll("main > section.panel")).forEach(function(el){
  if(el.id!=="festival-d3-live"){el.classList.add("fm-reveal");observer.observe(el)}
 });
}
load(false);
})();
