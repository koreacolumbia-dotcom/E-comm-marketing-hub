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
document.head.appendChild(style);
const cell=(v,title)=>'<div'+(title?' title="'+esc(title)+'"':'')+'>'+v+'</div>';
fetch("../daily_intelligence/latest.json?v="+Date.now(),{cache:"no-store"}).then(x=>{if(!x.ok)throw Error("Not found");return x.json()}).then(x=>{
const t=x.product_focus?.timeline;
if(!Array.isArray(t?.dates)||t.dates.length!==14){document.getElementById("csk-aligned").textContent="14일 실측 원천이 아직 준비되지 않았습니다.";return;}
const days=t.dates,rows=[];
rows.push('<div class="csk-track-row">'+cell("지표 / 일자")+days.map(v=>cell(esc(v.slice(5)))).join("")+'</div>');
rows.push('<div class="csk-track-row">'+cell("실측 기온(°C)")+days.map((d,i)=>cell(t.weather?.[i]==null?"—":esc(Number(t.weather[i]).toFixed(1)),t.weather?.[i]==null?"기온 미확보":d+" 기온")).join("")+'</div>');
(t.products||[]).slice(0,4).forEach(p=>{
 const values=(p.values||[]).filter(v=>typeof v==="number"&&Number.isFinite(v)), max=Math.max(1,...values);
 rows.push('<div class="csk-track-row">'+cell('<strong>'+esc(p.name)+'</strong><div class="muted">PDP 조회수</div>')+days.map((d,i)=>{const v=p.values?.[i];if(typeof v!=="number")return cell("—","PDP 미확보");
 const h=Math.max(2,Math.min(33,Math.round(33*v/max)));return cell('<span class="csk-track-bar" style="height:'+h+'px" aria-label="'+esc(d+" 조회 "+v)+'"></span>',d+" PDP "+v.toLocaleString("ko-KR"))}).join("")+'</div>');
});
(t.brands||[]).filter(b=>b.events?.length).slice(0,6).forEach(b=>{
 rows.push('<div class="csk-track-row">'+cell(esc(b.brand)+'<div class="muted">행사 기간</div>')+days.map(d=>{const events=b.events.filter(e=>e.dates?.includes(d));if(!events.length)return cell("");
 const verified=events.some(e=>["official_period","first_party_landing_text"].includes(e.status));
 return cell('<span class="csk-track-event'+(verified?" official":"")+'"></span>',events.map(e=>e.title).join("; "));}).join("")+'</div>');
});
document.getElementById("csk-aligned").innerHTML='<div class="csk-track-wrap"><div class="csk-track">'+rows.join("")+'</div></div><div class="csk-track-guide">파랑: PDP 조회 · 청회색: 기간 확인 행사 · 갈색: 관측 행사 · —: 관측 미확보. 막대는 상품별 상대 높이이며 브랜드 간 절댓값 비교용이 아닙니다.</div>';
}).catch(()=>{document.getElementById("csk-aligned").textContent="원천을 불러오지 못했습니다. 기존 일일 통합 분석에서 수집 상태를 확인하세요.";});
})();