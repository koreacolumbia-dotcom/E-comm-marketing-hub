/* Winter Festival only: accessible progressive enhancement and source-governed Meta/Paid analysis.
   Data is sourced from the aggregate daily snapshot, never Meta Ads API or raw customer data. */
(function(){
"use strict";
var root=document.getElementById("fm-integrated");
if(!root)return;
document.body.classList.add("festival-motion");
var $=function(s){return root.querySelector(s)};
var app={data:null,view:"event",filter:"all",metric:"revenue",drawer:null,lastFocus:null};
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

/* Causal humility: factors are arithmetic, channels are GA4 attribution. These
   two explanations are alternative views of the SAME revenue, not additive. */
function fmSigned(n){
 var v=Number(n);
 if(!Number.isFinite(v))return "미확보";
 return (v<0?"−":v>0?"+":"")+fmt(Math.abs(v),"revenue");
}
function fmReasonCard(card,index,good){
 var value=Number(card.headline),title=card.title||"분석 항목",evidence=card.evidence||"동일 경과일 근거 미확보";
 return '<article class="fm-cause" data-fm-reason="'+esc(card.id)+'">'+
 '<div class="fm-cause-kicker">'+String(index+1).padStart(2,"0")+' · '+(card.axis==="funnel"?"구매 구조":"채널 변화")+' · 실측</div>'+
 '<div class="fm-cause-title">'+esc(title)+'</div>'+
 '<div class="fm-cause-amount">'+esc(fmSigned(value))+'</div>'+
 '<div class="fm-cause-sub">'+esc(card.subtitle||"동기간 GA4 귀속 매출")+'</div>'+
 '<div class="fm-cause-evidence">'+esc(evidence.split(";").slice(0,2).join(" · "))+'</div>'+
 '<details><summary>근거·검증 보기</summary><div class="fm-cause-explain">'+
 '<p><b>해석 (인과 미확정)</b> · '+esc(card.interpretation||"추가 검증 필요")+'</p>'+
 '<p><b class="fm-verify-label">다음 검증</b> · '+esc(card.verify_next||"원본 추가 확인")+'</p>'+
 '</div></details></article>';
}
function renderDiagnosis(){
 var section=document.getElementById("fm-diagnosis");
 if(!section)return;
 var d=app.data||{},diag=d.diagnosis||{},event=d.event||{};
 var status=section.querySelector("#fm-diagnosis-period");
 if(diag.status!=="funnel_only"&&diag.status!=="reconciled"){
  status.textContent="기준일 실측 갱신 대기";
  section.querySelector("#fm-diagnosis-thesis").innerHTML="<strong>검증 상태</strong><p>원인 판단용 최신 집계가 아직 준비되지 않았습니다. 아래 고정 D1–D2 참고 수치는 이전 검증 기준입니다.</p>";
  return;
 }
 status.textContent="행사 D1~D"+esc(diag.event_days)+" · "+esc((diag.compared_dates||{}).current?.join(" ~ ")||"");
 var funnel=diag.funnel||{},term=(funnel.terms||{}).cvr||{};
 var revenue=Number(diag.net_revenue_change_krw),netPct=Number(diag.net_revenue_yoy_pct);
 var thesis=section.querySelector("#fm-diagnosis-thesis");
 var line=Number.isFinite(term.amount_krw)?(
    "매출 "+fmSigned(revenue)+" ("+(netPct>=0?"+":"")+netPct.toFixed(1)+"% YoY) · "+
    "CVR 산술기여 "+fmSigned(term.amount_krw)+" · 인과 효과 미확정"
 ):"행사 KPI는 확인됐지만 세션·전환율·객단가 분해는 아직 검증 중입니다.";
 thesis.innerHTML='<strong>핵심 결론 · 확정 행사 D1~D'+esc(diag.event_days)+'</strong><p>'+esc(line)+'</p>';
 var goods=Array.isArray(diag.strengths)?diag.strengths:[],bads=Array.isArray(diag.weaknesses)?diag.weaknesses:[];
 var aligned=diag.event_media_aligned===true;
 var sectionNote='<article class="fm-cause"><div class="fm-cause-kicker">추가 근거 대기</div>'+
   '<p>현재 행사 D1~D'+esc(diag.event_days)+' 기준의 매체별 전년 실측 자료가 동일 기간으로 검증되지 않았습니다. 이전 기간 값을 더해 결론 내리지 않습니다.</p></article>';
 section.querySelector("#fm-success-cards").innerHTML=goods.length?goods.slice(0,3).map(function(c,i){return fmReasonCard(c,i,true)}).join(""):sectionNote;
 section.querySelector("#fm-weak-cards").innerHTML=bads.length?bads.slice(0,3).map(function(c,i){return fmReasonCard(c,i,false)}).join(""):sectionNote;
 var basket=diag.basket_watch,aw=section.querySelector("#fm-aov-warning");
 if(basket){aw.innerHTML='<strong>추가 점검 · 객단가/상품 구성</strong>'+
   '<p>객단가 변화의 매출 산술 배분 '+esc(fmSigned(basket.headline))+' · '+esc(basket.evidence)+
   ' · 할인·상품 믹스가 실제 원인인지 미검증</p>';}
 var traffic=diag.traffic_watch,tw=section.querySelector("#fm-awareness-warning");
 if(traffic){tw.innerHTML='<strong>추가 점검 · META_BD 별도 평가</strong>'+
   '<p>'+esc(fmt(traffic.sessions,"sessions"))+' 중 '+esc(fmt(traffic.orders,"orders"))+
   ' · CVR '+esc(fmt(traffic.cvr_pct,"cvr"))+
   '. 인지도 목적은 노출·도달·CPM으로 따로 평가해야 합니다.</p>';}
 else {tw.innerHTML='<strong>추가 점검 · Meta 인지도 유입</strong><p>인지 목적 캠페인은 구매 실적만으로 성과를 판정하지 않습니다.</p>';}
 var place=section.querySelector("#fm-driver-bridge");
 if(!aligned||!Array.isArray(diag.channel_bridge)){
  place.innerHTML='<div class="fm-state warn">'+esc(diag.media_unavailable_reason||"매체별 동일 행사일 비교 대기")+
    ' · 채널별 변화가 해당 기간과 일치할 때만 증감 그래프를 표시합니다.</div>';
  section.querySelector("#fm-diagnosis-foot").textContent="세션·CVR·객단가의 산술 분해만 확정됐습니다. 매체별 원인 판단은 동일 날짜 원본 검증 전까지 보류합니다.";
  return;
 }
 var rows=diag.channel_bridge.filter(function(x){return Number.isFinite(Number(x.delta_krw))})
     .sort(function(x,y){return Math.abs(y.delta_krw)-Math.abs(x.delta_krw)});
 var ceiling=Math.max(1,...rows.map(function(x){return Math.abs(Number(x.delta_krw))}));
 var bars=rows.filter(function(x){return x.delta_krw!==0}).map(function(x){
  var delta=Number(x.delta_krw),pct=Math.max(0,(Math.abs(delta)/ceiling)*100);
  return '<div class="fm-bridge-row '+(delta<0?"down":"")+'">'+
    '<span class="fm-bridge-label" title="'+esc(x.label)+'">'+esc(x.label)+'</span>'+
    '<div class="fm-bridge-track" role="img" aria-label="'+esc(x.label)+" "+esc(fmSigned(delta))+'"><i style="width:'+pct.toFixed(1)+'%"></i></div>'+
    '<b class="fm-bridge-amount">'+esc(fmSigned(delta))+'</b></div>';
 }).join("");
 var net=Number(diag.channel_total_change_krw);
 var warning=Number(diag.unclassified?.current||0);
 place.innerHTML=bars+'<div class="fm-bridge-total"><span>전체 채널 증감 합계 (대사 완료)</span>'+
 '<strong>'+esc(fmSigned(net))+'</strong></div>'+
 (warning>0?'<p class="fm-bridge-warning">기타·미분류 귀속 매출 '+esc(fmt(warning,"revenue"))+
 '은 별도 표기합니다. 실제 Paid/CRM 성과에 임의 배분하지 않습니다.</p>':"");
 section.querySelector("#fm-diagnosis-foot").textContent=
  "채널 합계 "+fmSigned(net)+" = 총매출 증감 "+fmSigned(revenue)+
  " · 퍼널·채널 영향은 중복 계산 금지 · 매체 ROAS 미검증";
}


/* Product metrics: verified 7D vs preceding 7D ONLY. Never label product data YoY. */

function renderProductBridge(first,second,description,labels){
 var panel=document.getElementById("fm-product-item-bridge");if(!panel)return;
 var x=Number(first),y=Number(second);
 if(!Number.isFinite(x)||!Number.isFinite(y)){panel.hidden=true;return;}
 panel.hidden=false;
 var maximum=Math.max(1,Math.abs(x),Math.abs(y));
 function row(label,n){
  return '<div class="fm-item-effect '+(n<0?"negative":"")+'"><span>'+esc(label)+'</span>'+
    '<b>'+esc(fmSigned(n))+'</b><div class="fm-product-bar"><i class="'+(n<0?"down":"")+
    '" style="width:'+(Math.abs(n)/maximum*100).toFixed(1)+'%"></i></div></div>';
 }
 panel.innerHTML='<div class="fm-item-bridge-head"><strong>'+esc(description)+'</strong>'+
   '<small>서로 다른 축의 산술 분해 · 인과 미확정</small></div><div class="fm-item-bridge-grid">'+
   row(labels?.[0]||"수량 변화",x)+row(labels?.[1]||"실현매출 변화",y)+'</div>';
}

function renderProductSignals(data,images){
 var section=document.getElementById("fm-product-cause");if(!section)return;
 function get(id){return document.getElementById(id)}
 if(!data||data.status!=="ga4_measured"||!data.item_aov_bridge||
    !data.period||!Array.isArray(data.product_sales)){
  get("fm-product-verdict").innerHTML="<strong>상품 원인 분석 대기</strong><small>7일 품목 데이터 검증 전. 판매 부진이나 가격 영향을 단정하지 않습니다.</small>";
  get("fm-product-kpis").innerHTML="";
  get("fm-product-winners").textContent="데이터 확인 필요";
  get("fm-product-losers").textContent="데이터 확인 필요";return;
 }
 var a=data.item_aov_bridge, p=data.period,categories=(data.category_bridge||{}).rows||[];
 var pct=function(x,y){return Number(x)>0?(100*(Number(y)/Number(x)-1)):null};
 var pp=function(k){var r=categories.find(function(x){return x.category===k});return r||null};
 var sale=pp("SALE"),outer=pp("OUTER");
 function share(r){return r?Number(r.share_previous_pct).toFixed(1)+" → "+Number(r.share_current_pct).toFixed(1)+"%":"미확보"}
 function delta(r){if(!r)return "미확보";var n=r.share_current_pct-r.share_previous_pct;return (n>0?"+":"")+n.toFixed(1)+"%p"}
 function signed(v){return (v<0?"−":v>0?"+":"")+fmt(Math.abs(Number(v)),"revenue")}
 var quantityDiff=pct(a.items_per_order_previous,a.items_per_order_current);
 var priceDiff=pct(a.realized_item_price_previous,a.realized_item_price_current);
 get("fm-product-verdict").innerHTML='<strong>주문당 수량 '+(quantityDiff===null?"—":quantityDiff.toFixed(1)+"%")+
   ' · 상품 실현매출 '+(priceDiff===null?"—":(priceDiff>=0?"+":"")+priceDiff.toFixed(1)+"%")+
   '</strong><small>WoW 실측. 상품구성 변화가 객단가에 영향을 미쳤을 가능성. 작년 행사 원인으로 확정하지 않음</small>';
 var kpis=[
  ["주문당 수량",Number(a.items_per_order_previous).toFixed(2)+" → "+Number(a.items_per_order_current).toFixed(2),
    quantityDiff===null?"미확보":quantityDiff.toFixed(1)+"% WoW"],
  ["상품 실현매출/개",Number(a.realized_item_price_current).toLocaleString("ko-KR")+"원",
    priceDiff===null?"미확보":(priceDiff>=0?"+":"")+priceDiff.toFixed(1)+"% WoW"],
  ["SALE 분류 비중",share(sale),delta(sale)],
  ["OUTER 분류 비중",share(outer),delta(outer)]
 ];
 get("fm-product-kpis").innerHTML=kpis.map(function(k){
  return '<div class="fm-mini-metric"><small>'+esc(k[0])+'</small><b>'+esc(k[1])+'</b><span>'+esc(k[2])+'</span></div>';
 }).join("");
 renderProductBridge(a.items_per_order_effect,a.realized_item_revenue_effect,"주문당 아이템 매출 차이 (전주 비교)",["주문당 상품수 변화","상품당 실현매출 변화"]);
 var mixes=categories.filter(function(x){
  return x && Number.isFinite(x.share_previous_pct) && Number.isFinite(x.share_current_pct);
 }).sort(function(a,b){return b.share_current_pct-a.share_current_pct}).slice(0,6);
 var chart=document.getElementById("fm-category-bars");
 if(chart){
  chart.innerHTML=mixes.length?mixes.map(function(row){
   var max=100,prev=Math.max(0,Math.min(max,Number(row.share_previous_pct))),
       curr=Math.max(0,Math.min(max,Number(row.share_current_pct))),
       delta=curr-prev;
   return '<div class="fm-category-row"><b>'+esc(row.category)+'</b>'+
      '<div class="fm-category-pair">'+
      '<div class="fm-category-track"><i class="prior" style="width:'+prev+'%"></i></div>'+
      '<div class="fm-category-track"><i class="now" style="width:'+curr+'%"></i></div>'+
      '</div><strong class="'+(delta<0?"down":"")+'">'+(delta>0?"+":"")+delta.toFixed(1)+'%p</strong></div>';
  }).join(""):'<span class="fm-note">상품군 분류 데이터 확인 중</span>';
 }
 var lookup=new Map(((images||{}).products||[]).filter(function(x){
  return /^https:\/\/assetscdn\d+\.styleship\.com\/assets\/columbia\/data\/productimages\/a\/[124]\/[a-z0-9_-]+\.jpg$/i.test(x.image_url||"");
 }).map(function(x){return [String(x.name).toLowerCase().replace(/[‘’\x60]/g,"'").replace(/\s+/g," ").trim(),x]}));
 var items=data.product_sales.filter(function(x){
  return x&&typeof x.name==="string"&&Number.isFinite(Number(x.current_revenue))&&Number.isFinite(Number(x.previous_revenue));
 }).map(function(x){
  return {name:x.name,delta:x.current_revenue-x.previous_revenue,
          prevUnits:x.previous_units,units:x.current_units,
          photo:lookup.get(x.name.toLowerCase().replace(/[‘’\x60]/g,"'").replace(/\s+/g," ").trim())};
 });
 var rises=items.filter(function(x){return x.delta>0}).sort(function(x,y){return y.delta-x.delta}).slice(0,4);
 var falls=items.filter(function(x){return x.delta<0}).sort(function(x,y){return x.delta-y.delta}).slice(0,4);
 var max=Math.max(1,...rises.concat(falls).map(function(x){return Math.abs(x.delta)}));
 function paint(rows){
  return rows.length?rows.map(function(x){
   var progress=Math.max(0,(Math.abs(x.delta)/max*100)).toFixed(1);
   var image=x.photo?'<img class="fm-product-photo" alt="" loading="lazy" src="'+esc(x.photo.image_url)+'">':
                '<span class="fm-product-photo fm-no-photo" aria-hidden="true">▣</span>';
   return '<div class="fm-product-item">'+image+'<div class="fm-product-info"><span class="fm-product-name" title="'+esc(x.name)+'">'+esc(x.name)+'</span>'+
     '<div class="fm-product-bar"><i class="'+(x.delta<0?"down":"")+'" style="width:'+progress+'%"></i></div>'+
     '<small>판매 '+esc(String(x.prevUnits??"—"))+' → '+esc(String(x.units??"—"))+'개</small></div>'+
     '<strong class="fm-product-change '+(x.delta<0?"down":"")+'">'+esc(signed(x.delta))+'</strong></div>';
  }).join(""):'<div class="fm-state">비교 가능한 상품 없음</div>';
 }
 get("fm-product-winners").innerHTML=paint(rises);
 get("fm-product-losers").innerHTML=paint(falls);
 get("fm-product-scope").textContent=p.current_start+"~"+p.current_end+" vs "+p.previous_start+"~"+p.previous_end+
  " · 전주 비교 / GA4 스타일 집계 "+items.length+"개 중 증감 상위 4개 표시. 상품 원인과 할인·재고 영향은 미확정.";
}

var productState={event:null,weekly:null,images:null,mode:"event",rankMode:"impact"};

/* Full 2025/2026 event-day style rankings. Same-name means normalized GA4 item_name
   appears with SOLD units in both periods; SKU/year/color can differ.
   Exact name matching never implies equivalent stock or campaign causality. */
function renderEventRankings(source,images){
 var panel=document.getElementById("fm-product-cause");
 if(!panel)return;
 var winners=panel.querySelector("#fm-product-winners"),losers=panel.querySelector("#fm-product-losers");
 var list=panel.querySelector(".fm-product-columns");
 var sections=list?.querySelectorAll(".fm-product-movers")||[];
 var headings=list?.querySelectorAll("h3")||[];
 var tabs=panel.querySelectorAll("[data-fm-product-rank]");
 var rows=Array.isArray(source?.products)?source.products:[];
 var counts=source?.presence_counts||{};
 var full=source?.schema>=2&&source?.quality?.full_product_ranking_verified===true
   &&rows.length===source?.product_count;
 var rankMode=productState.rankMode||"impact";
 var count=panel.querySelector("#fm-product-match-count");
 var hint=panel.querySelector("#fm-product-rank-scope");
 if(count)count.textContent=full?
    "동일 이름 "+String(counts.both||0)+"개 · 2025만 "+String(counts.only_2025||0)+
    "개 · 2026만 "+String(counts.only_2026||0)+"개":"전체 상품 순위 검증 대기";
 if(hint)hint.textContent=full?"전체 "+String(rows.length)+"개 스타일 기준 · 상품명 매칭":
    "기존 120개 부분집계 · 전체 TOP5 재검증 대기";
 if(!full){
  tabs.forEach(function(t){t.disabled=t.getAttribute("data-fm-product-rank")!=="impact";t.title=t.disabled?"전체 스타일 검증 후 활성화":"";});
  return;
 }
 tabs.forEach(function(t){
   t.disabled=false;t.title="";
   t.setAttribute("aria-selected",String(t.getAttribute("data-fm-product-rank")===rankMode));
 });
 var sorted=rows.slice(),up=[],down=[],single=false;
 if(rankMode==="common"){
   var common=sorted.filter(function(x){return x.presence==="both"});
   up=common.filter(function(x){return x.delta_revenue>0})
     .sort(function(a,b){return b.delta_revenue-a.delta_revenue||a.name.localeCompare(b.name)}).slice(0,5);
   down=common.filter(function(x){return x.delta_revenue<0})
     .sort(function(a,b){return a.delta_revenue-b.delta_revenue||a.name.localeCompare(b.name)}).slice(0,5);
 }else if(rankMode==="2025"){
   up=sorted.filter(function(x){return (x.previous?.units||0)>0})
     .sort(function(a,b){return b.previous.revenue-a.previous.revenue||a.name.localeCompare(b.name)}).slice(0,5);
   single=true;
 }else if(rankMode==="2026"){
   up=sorted.filter(function(x){return (x.current?.units||0)>0})
     .sort(function(a,b){return b.current.revenue-a.current.revenue||a.name.localeCompare(b.name)}).slice(0,5);
   single=true;
 }else{
   up=sorted.filter(function(x){return x.delta_revenue>0})
     .sort(function(a,b){return b.delta_revenue-a.delta_revenue||a.name.localeCompare(b.name)}).slice(0,5);
   down=sorted.filter(function(x){return x.delta_revenue<0})
     .sort(function(a,b){return a.delta_revenue-b.delta_revenue||a.name.localeCompare(b.name)}).slice(0,5);
 }
 var headers=rankMode==="common"?["양년 판매 · 성장 TOP 5","양년 판매 · 하락 TOP 5"]:
             rankMode==="2025"?["2025 매출 TOP 5",""]:
             rankMode==="2026"?["2026 매출 TOP 5",""]:
             ["매출 증가 영향 TOP 5","매출 감소 영향 TOP 5"];
 headings.forEach(function(h,i){
  h.textContent=headers[i]||"";
  if(i===0&&single){var label=document.createElement("small");label.textContent=
     "25·26년 동일 기간 매출·판매수량 비교";h.appendChild(label);}
 });
 list.classList.toggle("fm-ranking-single",single);
 if(sections[1])sections[1].hidden=single;
 var all=up.concat(down);
 var ceiling=Math.max(1,...all.map(function(x){return Math.abs(Number(x.delta_revenue||0))}));
 var maxYear=Math.max(1,...all.map(function(x){return Math.max(x.previous?.revenue||0,x.current?.revenue||0)}));
 var normalize=function(n){return String(n||"").toLowerCase().replace(/[‘’\x60]/g,"'")
   .replace(/\s+/g," ").trim()};
 var imageLookup=new Map(((images||{}).products||[]).filter(function(x){
  return /^https:\/\/assetscdn\d+\.styleship\.com\/assets\/columbia\/data\/productimages\/a\/[124]\/[a-z0-9_-]+\.jpg$/i.test(x.image_url||"") &&
     /^https:\/\/www\.columbiakorea\.co\.kr\//i.test(x.product_url||"");
 }).map(function(x){return [normalize(x.name),x]}));
 function makeRow(item,i){
  var prior=item.previous||{},current=item.current||{};
  var pNum=Number(prior.revenue)||0,cNum=Number(current.revenue)||0;
  var difference=Number(item.delta_revenue)||0;
  var x=imageLookup.get(normalize(item.name));
  var photo=x?'<img class="fm-product-photo" alt="" loading="lazy" src="'+esc(x.image_url)+'">':
          '<span class="fm-product-photo fm-no-photo" aria-hidden="true">▣</span>';
  var presence=item.presence==="both"?"동일 이름·양년 판매":
    item.presence==="only_2025"?"2025 행사만 관측":
    item.presence==="only_2026"?"2026 행사만 관측":"판매수량 확인 필요";
  var label=single?(rankMode==="2025"?pNum:cNum):difference;
  var mainValue=single?fmt(label,"revenue"):fmSigned(label);
  var compareMeta=single?"증감 "+fmSigned(difference):"";
  var max=maxYear;
  function yearRow(tag,rev,units,kind){
   return '<div class="fm-rank-year"><span>'+tag+'</span>'+
      '<div class="fm-rank-track"><i class="'+kind+'" style="width:'+
        (Math.max(0,rev)/max*100).toFixed(1)+'%"></i></div>'+
      '<b>'+esc(fmt(rev,"revenue"))+'</b><small>'+esc(String(units))+'개</small></div>';
  }
  return '<div class="fm-product-item fm-ranked-item">'+
    '<div class="fm-ranked-photo">'+photo+'<span class="fm-rank-number">'+String(i+1).padStart(2,"0")+'</span></div>'+
    '<div class="fm-product-info"><span class="fm-product-name" title="'+esc(item.name)+'">'+esc(item.name)+'</span>'+
    '<div class="fm-rank-badge">'+esc(presence)+'</div>'+
    '<div class="fm-rank-comparison" role="img" aria-label="'+esc(item.name)+
      ' 2025 '+esc(fmt(pNum,"revenue"))+' '+esc(String(prior.units??0))+
      '개, 2026 '+esc(fmt(cNum,"revenue"))+' '+esc(String(current.units??0))+'개">'+
      yearRow("25",pNum,prior.units??0,"prior")+yearRow("26",cNum,current.units??0,"current")+'</div>'+
    '</div><div class="fm-ranked-value"><strong class="fm-product-change '+(label<0?"down":"")+'">'+
    esc(mainValue)+'</strong>'+(compareMeta?'<small>'+esc(compareMeta)+'</small>':"")+
    '</div></div>';
 }
 function buildList(items){
  return items.length?items.map(makeRow).join(""):
    '<div class="fm-state">해당 조건의 상품이 없습니다</div>';
 }
 winners.innerHTML=buildList(up);
 if(!single)losers.innerHTML=buildList(down);
 var scope=panel.querySelector("#fm-product-scope");
 if(scope)scope.textContent="동일 이름=띄어쓰기·대소문자·아포스트로피 정규화한 item_name. "+
   "SKU/컬러를 통합하며 판매 여부는 행사 기간 판매수량>0 기준. "+
   "한 해만 관측된 상품은 신규 출시나 단종으로 단정하지 않습니다.";
}

function renderEventProduct(source,images){
 var section=document.getElementById("fm-product-cause");
 if(!section||!source||source.status!=="ga4_measured")return false;
 var years=source.years||{},a=years["2025"],b=years["2026"],items=source.products||[];
 if(!a||!b||!Array.isArray(items)||!Number.isFinite(a.units)||!Number.isFinite(b.units)||a.units<=0)return false;
 var get=function(id){return document.getElementById(id)};
 var pct=function(x,y){return x>0?(100*(y/x-1)):null};
 var signed=function(v){return fmSigned(v)};
 var unitsChange=pct(a.units,b.units),unitPriceChange=pct(a.revenue_per_item,b.revenue_per_item);
 var itemsPerOrderChange=pct(a.items_per_order,b.items_per_order);
 var avgPrev=a.orders?a.purchase_revenue/a.orders:0,avgNow=b.orders?b.purchase_revenue/b.orders:0;
 var aovChange=pct(avgPrev,avgNow);
 get("fm-product-verdict").innerHTML='<strong>상품 수량 '+(unitsChange>=0?"+":"")+unitsChange.toFixed(1)+'% · 개당 매출 '+
   (unitPriceChange>=0?"+":"")+unitPriceChange.toFixed(1)+'%</strong>'+
   '<small>같은 행사 경과일 · 품목 판매 기준. 상품 가격·할인·구성 중 어느 요인의 영향인지는 별도 확인 필요</small>';
 var kpis=[
  ["상품 판매수량",a.units.toLocaleString("ko-KR")+" → "+b.units.toLocaleString("ko-KR")+"개",
   (unitsChange>=0?"+":"")+unitsChange.toFixed(1)+"% YoY"],
  ["상품당 실현매출",Math.round(a.revenue_per_item).toLocaleString("ko-KR")+" → "+Math.round(b.revenue_per_item).toLocaleString("ko-KR")+"원",
   (unitPriceChange>=0?"+":"")+unitPriceChange.toFixed(1)+"% YoY"],
  ["주문당 상품수",a.items_per_order.toFixed(2)+" → "+b.items_per_order.toFixed(2)+"개",
   (itemsPerOrderChange>=0?"+":"")+itemsPerOrderChange.toFixed(1)+"% YoY"],
  ["주문 객단가",Math.round(avgPrev).toLocaleString("ko-KR")+" → "+Math.round(avgNow).toLocaleString("ko-KR")+"원",
   (aovChange>=0?"+":"")+aovChange.toFixed(1)+"% YoY"]
 ];
 get("fm-product-kpis").innerHTML=kpis.map(function(x){
  return '<div class="fm-mini-metric"><small>'+esc(x[0])+'</small><b>'+esc(x[1])+'</b><span>'+esc(x[2])+'</span></div>';
 }).join("");
 var p0=a.item_revenue/a.units,p1=b.item_revenue/b.units;
 var unitImpact=(b.units-a.units)*(p0+p1)/2;
 var priceImpact=(p1-p0)*(a.units+b.units)/2;
 if(Math.abs(unitImpact+priceImpact-(b.item_revenue-a.item_revenue))<=1){
  renderProductBridge(unitImpact,priceImpact,"전년 동일 행사일 아이템 매출 변화",["판매수량 변화","상품당 실현매출 변화"]);
 }
 var chart=get("fm-product-category");if(chart)chart.hidden=true;
 var matchName=function(n){return String(n||"").toLowerCase().replace(/[‘’\x60]/g,"'").replace(/\s+/g," ").trim()};
 var imagesByName=new Map(((images||{}).products||[]).filter(function(x){
  return /^https:\/\/assetscdn\d+\.styleship\.com\/assets\/columbia\/data\/productimages\/a\/[124]\/[a-z0-9_-]+\.jpg$/i.test(x.image_url||"");
 }).map(function(x){return [matchName(x.name),x]}));
 renderEventRankings(source,images);
 var compareLabel="2025 "+(source.period?.["2025"]||[]).join("~")+" / 2026 "+(source.period?.["2026"]||[]).join("~");
 var label=get("fm-product-window-note");if(label)label.textContent=compareLabel;
 get("fm-product-scope").textContent="동일 이름 상품은 정규화된 상품명 기준으로 SKU·컬러를 통합. 2025/2026 본행사 판매수량을 기준으로 양년·단년 판매를 구분. TOP 5는 검증된 전체 "+String(source.product_count||items.length)+"개 스타일에서 계산. GA4 품목매출은 공식몰 주문매출과 합산 금지.";
 return true;
}
function drawProductWindow(){
 var eventReady=productState.event?.status==="ga4_measured" &&
  Array.isArray(productState.event.products)&&productState.event.years?.["2025"]&&productState.event.years?.["2026"];
 if(productState.mode==="event"&&!eventReady)productState.mode="weekly";
 var showEvent=productState.mode==="event"&&eventReady;
 var rankings=document.querySelector(".fm-ranking-tabs");
 var rankHead=document.querySelector(".fm-ranking-head");
 if(rankings)rankings.hidden=!showEvent;
 if(rankHead)rankHead.hidden=!showEvent;
 var grid=document.querySelector("#fm-product-cause .fm-product-columns");
 if(!showEvent&&grid)grid.classList.remove("fm-ranking-single");
 if(!showEvent&&grid){var movers=grid.querySelectorAll(".fm-product-movers");if(movers[1])movers[1].hidden=false;}
 if(showEvent)renderEventProduct(productState.event,productState.images);
 else renderProductSignals(productState.weekly,productState.images);
 var section=document.getElementById("fm-product-cause");
 if(section){
  var category=section.querySelector("#fm-product-category");if(category)category.hidden=!!showEvent;
  if(!showEvent){
   var head=section.querySelectorAll(".fm-product-movers h3");
   if(head[0])head[0].innerHTML="최근 7일 상승 TOP 4 <small>전주 대비</small>";
   if(head[1])head[1].innerHTML="최근 7일 하락 TOP 4 <small>전주 대비</small>";
  }
  var chip=section.querySelector(".fm-product-top .fm-chip");
  if(chip)chip.textContent=showEvent?"동일 행사일 · YoY":"행사 포함 최근 7일 · WoW";
  var label=section.querySelector("#fm-product-window-note");
  if(label&&!showEvent&&productState.weekly?.period){
   var p=productState.weekly.period;
   label.textContent=p.current_start+"~"+p.current_end+" vs "+p.previous_start+"~"+p.previous_end;
  }
 }
 document.querySelectorAll("[data-fm-product-rank]").forEach(function(button){
 button.addEventListener("click",function(){
  if(button.disabled||productState.mode!=="event")return;
  productState.rankMode=button.getAttribute("data-fm-product-rank");
  renderEventRankings(productState.event,productState.images);
 });
});
document.querySelectorAll("[data-fm-product-window]").forEach(function(button){
  var mode=button.getAttribute("data-fm-product-window");
  button.setAttribute("aria-pressed",String((mode==="event")===showEvent));
  button.disabled=mode==="event"&&!eventReady;
  button.title=mode==="event"&&!eventReady?"행사일 상품 원천 확인 중":"";
 });
}

async function loadProductSignals(){
 var data=await Promise.all([
   getJSON("./winter_festival_products_yoy_2026.json").catch(function(){return null}),
   getJSON("./ga4_purchase_diagnostics.json").catch(function(){return null}),
   getJSON("./official_product_images.json").catch(function(){return {products:[]}})
 ]);
 productState.event=data[0];productState.weekly=data[1];productState.images=data[2];
 productState.mode=productState.event?.status==="ga4_measured"?"event":"weekly";
 drawProductWindow();
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
 renderDiagnosis();
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
 document.querySelectorAll("[data-fm-product-window]").forEach(function(button){
 button.addEventListener("click",function(){
  productState.mode=button.getAttribute("data-fm-product-window")==="event"?"event":"weekly";
  drawProductWindow();
 });
});
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
$("#fm-refresh").addEventListener("click",function(){load(true);loadProductSignals()});
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
loadProductSignals();
})();
