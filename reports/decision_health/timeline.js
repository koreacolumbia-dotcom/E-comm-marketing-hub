/* Date-aligned evidence from Daily Intelligence; null is not zero. */
(()=>{
"use strict";
const root=document.querySelector("section.grid");
if(!root)return;
const esc=v=>String(v??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const container=document.createElement("article");
container.className="card";
container.innerHTML='<h2>03 · 상품·날씨·경쟁사 일자별 타임라인</h2><p class="muted">동일 날짜의 변화와 행사를 함께 관측합니다. 동시 발생은 인과관계의 증거가 아닙니다.</p><div id="csk-aligned" class="muted">관측값을 확인 중입니다.</div>';
root.insertBefore(container,document.getElementById("products")?.closest("article")||null);
const style=document.createElement("style");
style.textContent=".csk-track-wrap{overflow-x:auto;max-width:100%;margin-top:10px}.csk-track{min-width:830px;border:1px solid #e1e8ef;border-radius:9px}.csk-track-row{display:grid;grid-template-columns:205px repeat(14,minmax(30px,1fr));align-items:center;min-height:44px;border-bottom:1px solid #e8edf3}.csk-track-row:last-child{border-bottom:0}.csk-track-row:first-child{background:#ecf3f9;font-weight:700}.csk-track-row>div{font-size:11px;padding:4px;text-align:center}.csk-track-row>div:first-child{text-align:left;padding:5px 8px;line-height:1.3}.csk-track-bar{display:block;background:#427fb4;width:13px;min-height:2px;border-radius:2px;margin:auto}.csk-track-event{display:block;background:#be955d;min-height:14px;margin:auto;border-radius:2px}.csk-track-event.official{background:#7295b8}.csk-track-guide{font-size:11px;color:#64748b;margin-top:8px}";
style.textContent+=".csk-track-row.csk-sale-row{background:#f7fafc}.csk-track-bar.sale{background:#24775f}@media(max-width:760px){.csk-track-row>div:first-child{position:sticky;left:0;background:#fff;z-index:1}.csk-track-row.csk-sale-row>div:first-child{background:#f7fafc}}";
document.head.appendChild(style);
const cell=(v,title)=>'<div'+(title?' title="'+esc(title)+'"':'')+'>'+v+'</div>';
fetch("../daily_intelligence/latest.json?v="+Date.now(),{cache:"no-store"}).then(x=>{if(!x.ok)throw Error("Not found");return x.json()}).then(x=>{
const t=x.product_focus?.timeline;
if(!Array.isArray(t?.dates)||t.dates.length!==14){document.getElementById("csk-aligned").textContent="14일 실측 원천이 아직 준비되지 않았습니다.";return;}
const days=t.dates,rows=[];
rows.push('<div class="csk-track-row">'+cell("지표 / 일자")+days.map(v=>cell(esc(v.slice(5)))).join("")+'</div>');
rows.push('<div class="csk-track-row">'+cell("실측 기온(°C)")+days.map((d,i)=>cell(t.weather?.[i]==null?"—":esc(Number(t.weather[i]).toFixed(1)),t.weather?.[i]==null?"기온 미확보":d+" 기온")).join("")+'</div>');
const series=t.products||[];
const sales=new Map(series.filter(s=>s.metric==="sales"&&s.source==="ga4_purchase_items"&&s.coverage==="complete_14days").map(s=>[s.key||s.name,s]));
const cards=x.product_focus?.cards||[];
const priority=name=>{const c=cards.find(a=>a.name===name);if(!c)return 3;
 const sc=typeof c.sales_7d==="number"&&c.sales_previous_7d>0?(c.sales_7d/c.sales_previous_7d-1)*100:null;
 if(c.pdp_change_pct>=15&&sc!==null&&sc<=-15)return 0;
 if(c.pdp_change_pct<=-15&&sc!==null&&sc<=-15)return 1;
 return 2;};
const selected=series.filter(s=>s.metric==="pdp"&&sales.has(s.key||s.name)).sort((a,b)=>priority(a.name)-priority(b.name)).slice(0,4);
function metricRow(p,type){
 const vals=(p.values||[]).filter(v=>typeof v==="number"&&Number.isFinite(v)),max=Math.max(1,...vals);
 const label=type==="sales"?"GA4 매칭 판매수량":"PDP 조회수";
 const extra=type==="sales"?" · 상품명 매칭 14일 관측":"";
 return '<div class="csk-track-row'+(type==="sales"?" csk-sale-row":"")+'">'+cell('<strong>'+esc(p.name)+'</strong><div class="muted">'+label+extra+'</div>')+days.map((d,i)=>{
   const v=p.values?.[i];if(typeof v!=="number"||!Number.isFinite(v))return cell("—",label+" 미확보");
   const h=Math.max(2,Math.min(33,Math.round(33*v/max)));
   const tip=d+" "+label+" "+v.toLocaleString("ko-KR")+(type==="sales"?" (GA4 매칭 항목, 관리자 미대사)":"");
   return cell('<span class="csk-track-bar'+(type==="sales"?" sale":"")+'" style="height:'+h+'px" aria-label="'+esc(tip)+'"></span>',tip);
 }).join("")+'</div>';
}
selected.forEach(p=>{
 rows.push(metricRow(p,"pdp"));
 rows.push(metricRow(sales.get(p.key||p.name),"sales"));
});
if(!selected.length)rows.push('<div class="csk-track-guide">동일 상품명으로 연결되는 PDP·GA4 판매 일자별 표본이 없습니다.</div>');
(t.brands||[]).filter(b=>b.events?.length).slice(0,6).forEach(b=>{
 rows.push('<div class="csk-track-row">'+cell(esc(b.brand)+'<div class="muted">행사 기간</div>')+days.map(d=>{const events=b.events.filter(e=>e.dates?.includes(d));if(!events.length)return cell("");
 const verified=events.some(e=>["official_period","first_party_landing_text"].includes(e.status));
 return cell('<span class="csk-track-event'+(verified?" official":"")+'"></span>',events.map(e=>e.title).join("; "));}).join("")+'</div>');
});
document.getElementById("csk-aligned").innerHTML='<div class="csk-track-wrap"><div class="csk-track">'+rows.join("")+'</div></div><div class="csk-track-guide">파랑: PDP 조회 · 초록: GA4 상품명 매칭 구매수량(관리자 미대사) · 청회색: 기간 확인 행사 · 갈색: 관측 행사 · —: 관측 미확보. 모바일에서는 좌우로 스크롤하세요. 동시 발생은 인과 증거가 아닙니다.</div>';
}).catch(()=>{document.getElementById("csk-aligned").textContent="원천을 불러오지 못했습니다. 기존 일일 통합 분석에서 수집 상태를 확인하세요.";});
})();