/* Agency Glink paid reference, 2026-11-23. Separate from BigQuery GA4 revenue. */
(() => {
  "use strict";
  const DATA_URL = "reports/agency_paid_glink_20261123.json";
  const style = document.createElement("style");
  style.textContent = `
#csk-agency-paid{font-family:Inter,'Noto Sans KR',sans-serif;color:#13253b;margin-bottom:16px}
#csk-agency-paid *{box-sizing:border-box}
#csk-agency-paid .ap-shell{background:#fff;border:1px solid #dce5ee;border-radius:18px;padding:20px;box-shadow:0 8px 25px rgba(17,45,73,.045)}
#csk-agency-paid .ap-head{display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:12px;margin-bottom:17px}
#csk-agency-paid .ap-eyebrow{font-size:10px;letter-spacing:.12em;color:#5b7190;font-weight:800}
#csk-agency-paid h2{margin:5px 0 4px;font-weight:800;font-size:21px;color:#0b2e56}
#csk-agency-paid .ap-note{font-size:11px;color:#64748b;line-height:1.5}
#csk-agency-paid .ap-kpis{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px}
#csk-agency-paid .ap-kpi{border:1px solid #e2eaf2;background:#f7faff;border-radius:13px;padding:14px;min-width:0}
#csk-agency-paid .ap-lab{font-size:11px;color:#526780;font-weight:650}
#csk-agency-paid .ap-big{font-size:22px;color:#102c50;font-weight:850;margin-top:7px;overflow-wrap:anywhere;letter-spacing:-.03em}
#csk-agency-paid .ap-sub{color:#73839a;font-size:10px;margin-top:6px}
#csk-agency-paid .ap-mid{display:grid;grid-template-columns:1fr 1.2fr;gap:13px;margin-top:13px}
#csk-agency-paid .ap-panel{background:#fff;border:1px solid #e1e9f1;border-radius:13px;padding:15px;min-width:0}
#csk-agency-paid h3{font-size:13px;font-weight:800;color:#17375b;margin:0 0 12px}
#csk-agency-paid .ap-line{display:flex;justify-content:space-between;gap:12px;border-top:1px solid #eef2f7;padding:10px 0;font-size:11px;color:#526780;align-items:center}
#csk-agency-paid .ap-line b{color:#12355b;font-size:12px}
#csk-agency-paid .ap-select{display:flex;flex-wrap:wrap;gap:6px}
#csk-agency-paid .ap-select button{border:1px solid #dbe4ee;background:#fff;color:#556b84;border-radius:20px;padding:7px 11px;font-size:11px;font-weight:800;cursor:pointer}
#csk-agency-paid .ap-select button[aria-pressed=true]{background:#0b3b73;color:white;border-color:#0b3b73}
#csk-agency-paid .ap-channel{display:grid;grid-template-columns:132px 1fr 86px 80px;gap:8px;align-items:center;font-size:11px;padding:7px 0;border-top:1px solid #f0f3f7}
#csk-agency-paid .ap-channel b{text-align:right;color:#1b3a60;font-size:11px}
#csk-agency-paid .ap-track{height:8px;border-radius:8px;background:#ebf1f7;overflow:hidden}
#csk-agency-paid .ap-bar{height:100%;background:linear-gradient(90deg,#2a69ad,#69a8d9);border-radius:8px}
#csk-agency-paid .ap-insights{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:9px;margin-top:13px}
#csk-agency-paid .ap-insight{background:#f6f9fc;border:1px solid #e2eaf2;border-radius:13px;padding:12px}
#csk-agency-paid .ap-insight strong{display:block;font-size:11px;color:#173a63;margin-bottom:6px}
#csk-agency-paid .ap-insight p{margin:0;font-size:11px;line-height:1.55;color:#526780}
#csk-agency-paid .ap-warning{background:#fff8ec;border:1px solid #f5dfb2;padding:9px 12px;border-radius:10px;font-size:11px;color:#866118;margin-top:12px;line-height:1.5}
@media(max-width:900px){#csk-agency-paid .ap-kpis{grid-template-columns:repeat(2,minmax(0,1fr))}#csk-agency-paid .ap-mid{grid-template-columns:1fr}#csk-agency-paid .ap-insights{grid-template-columns:1fr}}
@media(max-width:500px){#csk-agency-paid .ap-shell{padding:12px}#csk-agency-paid .ap-big{font-size:18px}#csk-agency-paid .ap-channel{grid-template-columns:100px 1fr 68px;gap:5px}#csk-agency-paid .ap-channel .ap-roas{display:none}}
`;
  document.head.appendChild(style);
  const fmt = (x) => '₩' + Math.round(Number(x)||0).toLocaleString('ko-KR');
  const money = (x) => (Number(x)/1000000).toFixed(1) + '백만';
  const roas = (rev,spend) => Number(spend) > 0 ? Math.round(rev/spend*100).toLocaleString('ko-KR')+'%' : '—';
  const lift = (a,b) => b ? ((a/b-1)*100>=0?'+':'')+((a/b-1)*100).toFixed(1)+'%' : '비교불가';
  const sel = (scope) => scope==='festival'?'행사 11/20–22':scope==='pre'?'사전 11/18–19':'11월 누적 11/1–22';
  let snapshot, current='festival';
  const root=document.createElement('section'); root.id='csk-agency-paid'; root.setAttribute('aria-label','Glink 대행사 Paid 데이터');
  const container=document.querySelector('main') || document.body;
  container.insertBefore(root,container.firstChild);
  function sync(){root.style.display=(['performance','winter_festival'].includes(location.hash.replace('#','')))?'block':'none';}
  function card(label,value,detail){return `<div class="ap-kpi"><div class="ap-lab">${label}</div><div class="ap-big">${value}</div><div class="ap-sub">${detail}</div></div>`;}
  function render(scope){
    if(!snapshot)return;current=scope;
    const v=snapshot.summary[scope==='festival'?'festival_20_22':scope==='pre'?'pre_18_19':'nov_mtd'];
    const pre=snapshot.summary.pre_18_19,promo=snapshot.summary.festival_20_22;
    const daySpend=lift(promo.spend/3,pre.spend/2),dayGa=lift(promo.ga_revenue/3,pre.ga_revenue/2); const ly=snapshot.yoy_festival?.ly_2025,ty=snapshot.yoy_festival?.ty_2026;
    const brand=snapshot.channels['Naver Brand Search'][scope==='mtd'?'mtd':scope], meta=snapshot.channels['Meta'][scope==='mtd'?'mtd':scope];
    const share=v.ga_revenue>0?(brand[3]/v.ga_revenue*100).toFixed(1)+'%':'—';
    const rows=Object.entries(snapshot.channels).map(([name,periods])=>({name,v:periods[scope==='mtd'?'mtd':scope]})).sort((a,b)=>b.v[3]-a.v[3]);
    const max=Math.max(1,...rows.map(x=>x.v[3]));
    const html=rows.map(x=>`<div class="ap-channel"><span>${x.name}</span><span class="ap-track"><span class="ap-bar" style="display:block;width:${Math.max(0,x.v[3]/max*100).toFixed(1)}%"></span></span><b>${money(x.v[3])}</b><b class="ap-roas">${roas(x.v[3],x.v[1])}</b></div>`).join('');
    root.innerHTML=`<div class="ap-shell"><div class="ap-head"><div><div class="ap-eyebrow">AGENCY EVIDENCE · GLINK · 2026.11.23</div><h2>Paid 원인 분석 · ${sel(scope)}</h2><div class="ap-note">2026.11.22까지 대행사 매체RAW · 광고비 VAT 제외 · 카카오 MSG 제외 · 별도 귀속 기준</div></div><div class="ap-select" role="group" aria-label="집계 기간"><button data-scope="festival" aria-pressed="${scope==='festival'}">윈터페스티벌</button><button data-scope="pre" aria-pressed="${scope==='pre'}">사전알림</button><button data-scope="mtd" aria-pressed="${scope==='mtd'}">11월 누적</button></div></div><div class="ap-kpis">${card('광고비',fmt(v.spend),'VAT 제외 · MSG 제외')}${card('리포트 GA 귀속매출',fmt(v.ga_revenue),'GA4 전체 매출과 합산 금지')}${card('GA 귀속 ROAS',roas(v.ga_revenue,v.spend),'GA 귀속매출 ÷ 광고비')}${card('매체 ROAS',roas(v.media_revenue,v.spend),'매체 전환매출 ÷ 광고비')}</div><div class="ap-mid"><div class="ap-panel"><h3>사전알림 vs 행사 · 일평균 비교</h3><div class="ap-line"><span>광고비 (11/20–22 vs 11/18–19)</span><b>${daySpend}</b></div><div class="ap-line"><span>GA 귀속매출 (일평균)</span><b>${dayGa}</b></div><div class="ap-line"><span>GA 귀속 ROAS</span><b>${roas(pre.ga_revenue,pre.spend)} → ${roas(promo.ga_revenue,promo.spend)}</b></div><div class="ap-line"><span>네이버 브랜드검색 GA 귀속 점유율</span><b>${share}</b></div><div class="ap-line"><span>윈터페스티벌 YoY · 대행사 광고비</span><b>${ly&&ty?lift(ty.spend,ly.spend):"—"}</b></div><div class="ap-line"><span>윈터페스티벌 YoY · GA 귀속매출</span><b>${ly&&ty?lift(ty.ga_revenue,ly.ga_revenue):"—"}</b></div><div class="ap-note" style="margin-top:10px">서로 다른 2일·3일 비교는 일평균. YoY는 2025.11/21–23 vs 2026.11/20–22 행사일 기준, 집행 조건 동일성 미검증.</div></div><div class="ap-panel"><h3>매체별 GA 귀속매출 · ${sel(scope)}</h3><div class="ap-channel" style="border-top:0;font-weight:800;color:#64748b"><span>매체</span><span>GA 매출 규모</span><b>매출</b><b class="ap-roas">GA ROAS</b></div>${html}<div class="ap-note" style="margin-top:9px">GA 귀속값 0은 미귀속/미측정 가능. 매체 귀속 성과와 구별해서 판단.</div></div></div><div class="ap-insights"><div class="ap-insight"><strong>원인 1 · 브랜드 수요 집중</strong><p>선택 기간 네이버 브랜드검색 GA 귀속매출 ${fmt(brand[3])}, Paid 귀속매출의 ${share}. 브랜드 검색이 행사 실적의 주요 관측 지표.</p></div><div class="ap-insight"><strong>원인 2 · 유입 이후 매출 증가</strong><p>행사 3일의 일평균 광고비 ${daySpend}, 리포트 GA 귀속매출 ${dayGa}. 수익 변화가 집행비 증감만으로 설명되지는 않음.</p></div><div class="ap-insight"><strong>원인 3 · 매체 귀속 격차</strong><p>Meta 선택 기간 매체 귀속매출 ${fmt(meta[2])}, GA 귀속매출 ${fmt(meta[3])}. 구매 기여도 평가에는 GA4·상품·신규/기존 구매를 추가 대조해야 함.</p></div></div><div class="ap-warning">검증 필요: 11/12 Naver GFA 클릭 91,054 / 비용 약 424원으로 원본 값 이상 가능성이 있어 클릭 기준 판단은 보류. 이 영역은 대행사 리포트 근거이며 광고 인과 효과나 BigQuery 매출 합계가 아님.</div></div>`;
    root.querySelectorAll('[data-scope]').forEach(b=>b.addEventListener('click',()=>render(b.dataset.scope)));
  }
  sync();window.addEventListener('hashchange',sync);
  document.addEventListener('click',(event)=>{if(event.target.closest('[data-key]'))setTimeout(sync,0)});
  fetch(DATA_URL+'?v=20261123',{cache:'no-cache'}).then(r=>{if(!r.ok)throw Error('HTTP '+r.status);return r.json()}).then(data=>{snapshot=data;render(current);sync()}).catch(err=>{console.warn('Glink Paid evidence unavailable',err);root.textContent='Glink 대행사 자료 로드 실패';});
})();
