/* Three terse evidence panels. Full source logs stay available but collapsed. */
(() => {
  "use strict";
  const root=document.getElementById("evidence-short");
  if(!root)return;
  const finite=x=>typeof x==="number"&&Number.isFinite(x);
  const fmt=x=>finite(x)?Math.round(x).toLocaleString("ko-KR"):"—";
  const pct=x=>finite(x)?(x>0?"+":"")+x.toFixed(1)+"%":"—";
  const pp=x=>finite(x)?(x>0?"+":"")+x.toFixed(2)+"%p":"—";
  const el=(tag,cls,txt)=>{
    const n=document.createElement(tag);if(cls)n.className=cls;
    if(txt!==undefined&&txt!==null)n.textContent=String(txt);
    return n;
  };
  const saleDelta=p=>p?.sales_previous_7d>0?(p.sales_7d/p.sales_previous_7d-1)*100:null;
  const css=el("style");
  css.textContent=[
    "#evidence-short{padding:12px 0 4px}",
    ".ev-intro{display:flex;justify-content:space-between;gap:8px;flex-wrap:wrap;margin:0 0 10px}",
    ".ev-intro strong{font-size:14px}.ev-intro small{font-size:10px;color:#667b8c}",
    ".ev-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:9px}",
    ".ev-one{background:#fff;border:1px solid #dfe7ef;border-radius:9px;padding:12px;min-width:0;display:flex;flex-direction:column;gap:7px}",
    ".ev-head{font-size:10px;color:#496a84;display:flex;justify-content:space-between;gap:8px}",
    ".ev-head b{font-weight:800}.ev-head small{color:#83919c}",
    ".ev-title{font-size:15px;line-height:1.35;font-weight:800;overflow-wrap:anywhere}",
    ".ev-name{font-size:11px;color:#708192;line-height:1.3;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}",
    ".ev-proof{display:grid;grid-template-columns:1fr 1fr;gap:6px}",
    ".ev-num{background:#f6f8fa;border-radius:6px;padding:7px 8px;min-width:0}",
    ".ev-num span{display:block;font-size:10px;color:#65788b}",
    ".ev-num strong{display:block;font-size:17px;font-variant-numeric:tabular-nums;letter-spacing:-.03em}",
    ".ev-one .ev-funnel{border-top:1px solid #e6edf3;padding:8px 0 0;font-size:12px;line-height:1.4;color:#2e526d}",
    ".ev-one .ev-funnel b{font-weight:750}",
    ".ev-one .ev-explain{font-size:11px;line-height:1.5;color:#526a7e;margin:0}",
    ".ev-one .ev-source{font-size:10px;color:#7f8d99;margin-top:auto}",
    ".ev-one .ev-source a{color:#386887;text-decoration:underline}",
    "#raw-evidence{margin:12px 0 0}",
    "#raw-evidence>summary{padding:10px 13px;border:1px solid #dfe7ef;background:#f8fafc;border-radius:8px;cursor:pointer;color:#506b7f;font-size:12px}",
    "#raw-evidence>section{padding-top:12px}",
    "@media(max-width:960px){.ev-grid{grid-template-columns:1fr}.ev-one{gap:6px}.ev-num strong{font-size:18px}}"
  ].join("");
  document.head.appendChild(css);
  let payload=window.__cskCausePayload||null;
  let observed=null;
  let errorStatus="쿼리 결과 대기";
  const simple=(label,value)=>{
    const d=el("div","ev-num");
    d.append(el("span",null,label),el("strong",null,value));return d;
  };
  const sourceLink=(href,label)=>{
    const a=el("a",null,label);a.href=href;return a;
  };
  const productPanel=(p,rank,title,matched,valid)=>{
    const card=el("article","ev-one");
    const head=el("div","ev-head");head.append(el("b",null,"원인 "+rank+" · "+(valid?"추가 쿼리 관측":"가설 검증 중")),el("small",null,"GA4"));
    const proof=el("div","ev-proof");
    proof.append(simple("PDP 조회",pct(p.pdp_change_pct)),simple("구매수량",pct(saleDelta(p))));
    card.append(head,el("strong","ev-title",title),el("div","ev-name",p.name),proof);
    const funnel=el("div","ev-funnel");
    if(valid&&matched&&matched.coverage_reliable===true&&finite(matched.previous?.view_to_cart_pct)&&finite(matched.current?.view_to_cart_pct)){
      const prev=matched.previous.view_to_cart_pct,cur=matched.current.view_to_cart_pct;
      funnel.append(el("b",null,"동일 세션 PDP→장바구니 "+prev.toFixed(2)+"% → "+cur.toFixed(2)+"% ("+pp(matched.view_to_cart_delta_pp)+")"));
      const follow=el("p","ev-explain",cur<prev?
        "관측: 조회 후 장바구니 진입률이 약화됐어. 구매 원인은 아니며 유입 경로·상품 메시지는 별도 검증 대상.":
        "관측: 장바구니 진입률 하락은 확인되지 않았어. 결제 단계·유입 구성을 추가 확인.");
      card.append(funnel,follow);
      const src=matched.first_touch_sources||[];
      if(src.length&&src[0].source!=="(not set)"){
        card.append(el("p","ev-explain","최초 유입 출처 상위: "+src.slice(0,2).map(s=>s.source+" "+s.previous_view_sessions+"→"+s.current_view_sessions).join(" / ")+" (현재 캠페인 귀속 아님)"));
      }
    }else{
      funnel.append(el("b",null,"장바구니·구매 단계: "+errorStatus));
      card.append(funnel,el("p","ev-explain","현재 확인된 것은 조회와 구매 증감뿐이야. 퍼널 구간 원인은 쿼리 집계 확인 전까지 미확정."));
    }
    const src=el("div","ev-source");src.append(sourceLink("./ga4_purchase_diagnostics.json","구매 근거"));
    if(valid)src.append(el("span",null," · "),sourceLink("./product_funnel_evidence.json","세션 퍼널 근거"));
    card.append(src);
    return card;
  };
  const basketPanel=(g,rank)=>{
    const b=g?.item_aov_bridge||{},card=el("article","ev-one");
    const observed=b&&finite(b.items_per_order_previous)&&finite(b.items_per_order_current)&&finite(b.realized_item_price_previous)&&finite(b.realized_item_price_current);
    const head=el("div","ev-head");head.append(el("b",null,"원인 "+rank+" · "+(observed?"산술 기여도 실측":"원천 확인 중")));
    card.append(head,el("strong","ev-title","주문당 구매수량 감소"),el("div","ev-name","전체 GA4 구매상품"));
    const nums=el("div","ev-proof");
    if(observed){
      const pieces=(b.items_per_order_current/b.items_per_order_previous-1)*100;
      const price=(b.realized_item_price_current/b.realized_item_price_previous-1)*100;
      nums.append(simple("주문당 상품수",pct(pieces)),simple("상품당 실현매출",pct(price)));
      card.append(nums,el("div","ev-funnel","수량효과 "+fmt(b.items_per_order_effect)+"원 · 실현단가효과 +"+fmt(b.realized_item_revenue_effect)+"원"));
      card.append(el("p","ev-explain","객단가 변동의 산술 분해는 확인. 다품목 주문 감소 이유는 아직 미검증."));
    }else{
      card.append(el("div","ev-funnel","같은 기간 GA4 상품 구성을 확인할 수 없어 판단 보류"));
    }
    const src=el("div","ev-source");src.append(sourceLink("./ga4_purchase_diagnostics.json","구매·품목 근거"));card.append(src);
    return card;
  };
  function render(){
    if(!payload)return;
    root.replaceChildren();
    const focus=payload.focus||{},diag=payload.diagnoses||{},cards=Array.isArray(focus.cards)?focus.cards:[];
    const top=el("div","ev-intro");
    top.append(el("strong",null,"원인별 실제 검증 수치"),el("small",null,
      (focus.date||"기준일 미확보")+" · 동일 기간 비교 · 원인 확정과 구분"));
    root.append(top);
    const rows=el("div","ev-grid");
    const declining=cards.filter(p=>finite(saleDelta(p))&&saleDelta(p)<=-15);
    const first=declining.find(p=>p.pdp_change_pct>=15);
    const second=declining.find(p=>p.pdp_change_pct<=-15);
    const funnelValid=observed?.status==="ga4_measured"&&observed.report_date===focus.date;
    const byName=n=>funnelValid?observed.products?.find(x=>x.name===n):null;
    if(first)rows.append(productPanel(first,1,"조회↑ · 구매↓",byName(first.name),funnelValid));
    else rows.append(el("article","ev-one","원인 1 · 같은 기간에 비교 가능한 상품 원천 부족"));
    if(second)rows.append(productPanel(second,2,"조회↓ · 구매↓",byName(second.name),funnelValid));
    else rows.append(el("article","ev-one","원인 2 · 같은 기간에 비교 가능한 상품 원천 부족"));
    rows.append(basketPanel(diag.ga4_evidence,3));
    root.append(rows);
  }
  window.CSKDetailEvidence={render:next=>{payload=next;render();}};
  if(payload)render();
  fetch("./product_funnel_evidence.json?v="+Date.now(),{cache:"no-store"})
    .then(r=>{if(!r.ok)throw Error("HTTP");return r.json();})
    .then(j=>{
      observed=j;
      errorStatus=j.status==="blocked_budget"?"쿼리 비용 상한 초과":j.status==="query_error"?"쿼리 실행 오류":
        j.status==="source_not_aligned"?"비교 기간 불일치":j.status==="blocked_credentials"?"GA4 인증 미연결":
        j.status==="no_matching_events"?"일치하는 상품 세션 없음":"동일 세션 자료 미확보";
      render();
    }).catch(()=>{errorStatus="쿼리 결과 생성 전";render();});
})();