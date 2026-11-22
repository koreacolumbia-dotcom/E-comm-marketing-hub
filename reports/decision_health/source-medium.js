/* Source / Medium conversion analysis on the same two operational weeks. */
(()=>{
"use strict";
const root=document.getElementById("source-medium-health");if(!root)return;
const finite=x=>typeof x==="number"&&Number.isFinite(x);
const fmt=x=>finite(x)?Math.round(x).toLocaleString("ko-KR"):"—";
const won=x=>finite(x)?Math.round(x).toLocaleString("ko-KR")+"원":"—";
const signed=x=>finite(x)?(x>0?"+":"")+x.toFixed(1)+"%":"—";
const pp=x=>finite(x)?(x>0?"+":"")+x.toFixed(2)+"%p":"—";
const escapeHTML=s=>String(s??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const difference=(a,b)=>finite(a)&&finite(b)&&b>0?(a/b-1)*100:null;
const colors=(x)=>finite(x)?x>0?"up":x<0?"down":"flat":"flat";
const group=(raw)=>{
 const text=String(raw||"").toLowerCase(),medium=text.includes(" / ")?text.split(" / ").pop().trim():"";
 if(/(^|[^a-z])(cpc|ppc|cpm|paid|display|banner|retarget|pmax)([^a-z]|$)|shopping_ad|gfa/.test(medium))return "paid";
 if(/email|edm|lms|sms|push|friendstalk|alimtalk|kakao_ch|crm|owned/.test(medium))return "owned";
 if(/organic|referral|direct|none|social/.test(medium))return "organic";
 return "other";
};
const safe=s=>escapeHTML(s);
let payload=null,filter="all",sort="sessions",search="",showAll=false;
function card(label,value,detail,kind=""){
 return '<div class="sm-kpi"><span>'+safe(label)+'</span>'+
  '<strong>'+safe(value)+'</strong><small class="'+(kind||"")+'">'+safe(detail)+'</small></div>';
}
const numChange=(a,b)=>signed(difference(a,b));
const cv=(r)=>finite(r?.cvr_pct)?r.cvr_pct:null;
function viewRow(r,i){
 const a=r.current||{},b=r.previous||{},s=String(r.source_medium||"");
 const cvdiff=r.cvr_change_pp,revDelta=r.revenue_delta,sdiff=r.sessions_change_pct;
 const tdiff=r.transactions_delta;
 const share=a.sessions/(payload?.totals?.current?.sessions||1)*100;
 return '<article class="sm-entry" role="row">'+
  '<div class="sm-entry-name" role="cell"><span class="sm-entry-rank">'+String(i+1).padStart(2,"0")+'</span>'+
  '<div><strong title="'+safe(s)+'">'+safe(s)+'</strong>'+
  '<small>세션 점유율 '+(finite(share)?share.toFixed(1):"—")+'%</small>'+
  '<span class="sm-share-track" aria-hidden="true"><span style="width:'+Math.max(0,Math.min(100,finite(share)?share:0)).toFixed(1)+'%"></span></span></div></div>'+
  '<div class="sm-entry-metric" role="cell" data-label="세션"><strong>'+fmt(a.sessions)+'</strong>'+
  '<small class="'+colors(sdiff)+'">'+signed(sdiff)+' · '+fmt(b.sessions)+' 이전</small></div>'+
  '<div class="sm-entry-metric" role="cell" data-label="구매"><strong>'+fmt(a.transactions)+'</strong>'+
  '<small class="'+colors(tdiff)+'">'+(tdiff>0?"+":"")+fmt(tdiff)+'건</small></div>'+
  '<div class="sm-entry-metric" role="cell" data-label="구매전환율"><strong>'+(
      finite(cv(a))?cv(a).toFixed(2)+"%":"—")+'</strong>'+
  '<small class="'+colors(cvdiff)+'">'+pp(cvdiff)+'</small></div>'+
  '<div class="sm-entry-metric" role="cell" data-label="구매매출"><strong>'+won(a.revenue)+'</strong>'+
  '<small class="'+colors(revDelta)+'">'+(finite(revDelta)?(revDelta>0?"+":"")+fmt(revDelta)+"원":"—")+'</small></div></article>';
}
function currentRows(){
 if(payload?.status!=="ga4_measured"||!Array.isArray(payload.rows))return [];
 const filtered=payload.rows.filter(x=>{
  if(filter!=="all"&&group(x.source_medium)!==filter)return false;
  return !search||String(x.source_medium||"").toLowerCase().includes(search);
 });
 const sorters={
  sessions:(a,b)=>b.current.sessions-a.current.sessions,
  revenue:(a,b)=>b.current.revenue-a.current.revenue,
  conversion:(a,b)=>(b.current.cvr_pct??-1)-(a.current.cvr_pct??-1),
  delta:(a,b)=>a.revenue_delta-b.revenue_delta
 };
 filtered.sort((a,b)=>(sorters[sort]||sorters.sessions)(a,b)||String(a.source_medium).localeCompare(String(b.source_medium)));
 return filtered;
}
function updateRows(){
 const area=root.querySelector?.(".sm-results");
 if(!area)return;
 const data=currentRows(),limit=showAll?data.length:12;
 area.innerHTML=data.length?data.slice(0,limit).map(viewRow).join(""):'<div class="sm-empty">선택한 조건에 해당하는 소스/매체가 없습니다.</div>';
 const counter=root.querySelector?.(".sm-count");if(counter)counter.textContent=data.length+"개 소스/매체"+(data.length>limit?" 중 "+limit+"개 표시":"");
 const more=root.querySelector?.(".sm-more");if(more){more.hidden=data.length<=12;more.textContent=showAll?"접기":"전체 "+data.length+"개 보기";}
 root.querySelectorAll?.("[data-sm-filter]").forEach(btn=>{
  const active=btn.dataset.smFilter===filter;
  btn.setAttribute("aria-pressed",String(active));
  btn.classList.toggle("active",active);
 });
}
function render(data){
 payload=data;
 if(data?.status!=="ga4_measured"||!data?.totals?.current||!data?.totals?.previous||!Array.isArray(data?.rows)){
  root.innerHTML='<div class="sm-section-head"><div><small>ACQUISITION ANALYTICS</small><h2>유입 · 전환 소스/매체</h2>'+
   '<p>세션, 구매전환율, 매출을 같은 기간으로 비교합니다.</p></div></div>'+
   '<div class="sm-pending">'+
   (data?.status==="blocked_budget"?"GA4 소스/매체 집계가 비용 상한을 초과해 이번 실행을 건너뛰었습니다.":
   data?.status==="query_error"?"소스/매체 집계 연결을 확인하고 있습니다.":"소스/매체별 세션·전환 데이터를 집계하고 있습니다.")+
   '</div>';
  return;
 }
 const a=data.totals.current,b=data.totals.previous;
 const dates=data.period||{},current=dates.current||{},previous=dates.previous||{};
 const cvrChange=finite(a.cvr_pct)&&finite(b.cvr_pct)?a.cvr_pct-b.cvr_pct:null;
 const loss=data.rows.filter(r=>r.previous.sessions>=100&&r.current.sessions>=100&&
    finite(r.cvr_change_pp)&&r.cvr_change_pp<=-.3&&r.current.sessions>r.previous.sessions)
    .sort((x,y)=>x.cvr_change_pp-y.cvr_change_pp)[0];
 const message=loss?
  '<div class="sm-insight"><span>확인할 변화</span><strong>'+safe(loss.source_medium)+
  '</strong><small>세션 '+signed(difference(loss.current.sessions,loss.previous.sessions))+
  ' · CVR '+pp(loss.cvr_change_pp)+'</small></div>':"";
 root.innerHTML='<div class="sm-section-head"><div><small>ACQUISITION ANALYTICS · SOURCE / MEDIUM</small>'+
   '<h2>유입부터 구매전환까지</h2><p>소스/매체별 세션 · 주문 · CVR · 매출을 연결해 확인합니다.</p></div>'+
   '<small class="sm-period">'+safe(current.start)+" ~ "+safe(current.end)+"<span>vs "+safe(previous.start)+" ~ "+safe(previous.end)+'</span></small></div>'+
   '<div class="sm-kpis">'+
   card("GA4 세션",fmt(a.sessions),numChange(a.sessions,b.sessions),colors(difference(a.sessions,b.sessions)))+
   card("구매 건수",fmt(a.transactions),numChange(a.transactions,b.transactions),colors(difference(a.transactions,b.transactions)))+
   card("구매전환율",finite(a.cvr_pct)?a.cvr_pct.toFixed(2)+"%":"—",pp(cvrChange),colors(cvrChange))+
   card("구매매출",won(a.revenue),numChange(a.revenue,b.revenue),colors(difference(a.revenue,b.revenue)))+
   '</div>'+message+
   '<div class="sm-toolbar"><div class="sm-filters" role="group" aria-label="소스/매체 그룹 선택">'+
     [['all','전체'],['paid','유료'],['organic','오가닉·직접'],['owned','CRM'],['other','기타']].map(([v,label])=>
     '<button type="button" data-sm-filter="'+v+'" aria-pressed="'+String(v===filter)+'">'+safe(label)+'</button>').join("")+
     '</div><div class="sm-controls"><label class="sm-searchbox"><span>검색</span>'+
     '<input type="search" class="sm-search" placeholder="예: naver / cpc" aria-label="소스/매체 검색" value="'+safe(search)+'"></label>'+
     '<label class="sm-sortbox"><span>정렬</span><select class="sm-sort" aria-label="소스/매체 정렬">'+
     [['sessions','세션순'],['revenue','매출순'],['conversion','전환율순'],['delta','매출 하락순']].map(([v,label])=>
      '<option value="'+v+'"'+(v===sort?" selected":"")+'>'+label+'</option>').join("")+'</select></label></div></div>'+
   '<div class="sm-table"><div class="sm-table-head" aria-hidden="true">'+
   '<span>SOURCE / MEDIUM</span><span>세션 / 전주</span><span>구매</span><span>CVR / 증감</span><span>매출 / 증감</span></div>'+
   '<div class="sm-results" role="rowgroup"></div></div>'+
   '<div class="sm-bottom"><span class="sm-count" aria-live="polite"></span>'+
   '<button class="sm-more" type="button">전체 보기</button></div>'+
   '<p class="sm-caption">GA4 세션 귀속 기준 · CVR = 구매 건수 ÷ 세션 수. 구매 이벤트 매출이며 전체 운영 매출의 증분 기여액은 아닙니다.</p>';
 updateRows();
}
root.addEventListener?.("click",e=>{
 const b=e.target.closest?.("[data-sm-filter]");
 if(b){filter=b.dataset.smFilter;showAll=false;updateRows();return;}
 if(e.target.closest?.(".sm-more")){showAll=!showAll;updateRows();}
});
root.addEventListener?.("input",e=>{
 if(e.target.matches?.(".sm-search")){search=e.target.value.trim().toLowerCase();showAll=false;updateRows();}
});
root.addEventListener?.("change",e=>{
 if(e.target.matches?.(".sm-sort")){sort=e.target.value;showAll=false;updateRows();}
});
window.CSKSourceMedium={render,currentRows,group};
if(window.__cskSmPayload)render(window.__cskSmPayload);
})();