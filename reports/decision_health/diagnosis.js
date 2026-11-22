/* CSK integrated decision hypotheses: observed facts != causal attribution. */
(() => {
  'use strict';
  const anchor = document.querySelector('main > section.grid');
  if (!anchor || document.getElementById('csk-root-diagnosis')) return;
  const css = document.createElement('style');
  css.textContent = `
    #csk-root-diagnosis{margin:0 0 20px;padding:0;border:0;background:transparent;color:#14283d}
    #csk-root-diagnosis h2{font-size:20px;margin:0 0 6px}
    #csk-root-diagnosis .sub{margin:0 0 12px;font-size:12px;color:#64748b}
    #csk-root-diagnosis .headline{background:#eaf2f9;border-left:3px solid #28557e;border-radius:6px;padding:11px 12px;font-size:14px;margin-bottom:12px}
    #csk-root-diagnosis .diag-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}
    #csk-root-diagnosis .diag-card{background:white;border:1px solid #e2e8f0;border-radius:11px;padding:14px;min-width:0}
    #csk-root-diagnosis .diag-card h3{font-size:15px;line-height:1.4;margin:7px 0}
    #csk-root-diagnosis .diag-card p{font-size:12px;margin:6px 0}
    #csk-root-diagnosis .diagnosis-label{font-size:11px;letter-spacing:.01em;color:#64748b}
    #csk-root-diagnosis .observed{font-size:13px;font-weight:700;color:#244b6b}
    #csk-root-diagnosis .hypothesis{background:#f7f9fb;padding:9px 10px;border-radius:7px}
    #csk-root-diagnosis .blocked{color:#915b1e;font-size:12px}
    #csk-root-diagnosis .evidence-list{display:flex;gap:8px;flex-wrap:wrap;margin-top:9px}
    #csk-root-diagnosis .evidence-list a{font-size:11px;color:#145789;text-decoration:underline;text-underline-offset:2px}
    #csk-root-diagnosis .diag-status{font-size:10px;border-radius:5px;padding:3px 6px;background:#f1f5f9;font-weight:650}
    #csk-root-diagnosis .diag-status.known{background:#e7f2ef;color:#176a50}
    #csk-root-diagnosis .crawl-table{width:100%;border-collapse:collapse;font-size:12px}
    #csk-root-diagnosis .crawl-table th,#csk-root-diagnosis .crawl-table td{padding:8px;text-align:left;border-bottom:1px solid #e2e8f0;vertical-align:top}
    #csk-root-diagnosis .crawl-table th{color:#60768b;font-weight:600}
    #csk-root-diagnosis .crawl-scroll{overflow-x:auto;background:white;border-radius:10px;border:1px solid #e2e8f0}
    #csk-root-diagnosis .crawl-table{min-width:540px}
    #csk-root-diagnosis .hint{font-size:11px;color:#64748b;margin-top:8px}
    #csk-root-diagnosis .error-state{padding:12px;color:#854d0e}
    @media(max-width:760px){#csk-root-diagnosis{margin:0 0 12px}#csk-root-diagnosis h2{font-size:17px}
      #csk-root-diagnosis .diag-grid{grid-template-columns:1fr;gap:9px}
      #csk-root-diagnosis .diag-card{padding:12px;border-left:0;border-right:0;border-radius:0}
      #csk-root-diagnosis .headline{border-radius:0}
      #csk-root-diagnosis .crawl-scroll{border-left:0;border-right:0;border-radius:0}
    }
  `;
  document.head.appendChild(css);
  const root = document.createElement('section');
  root.id = 'csk-root-diagnosis';
  root.setAttribute('aria-label','문제 원인 진단 및 근거');
  const heading = document.createElement('h2');
  heading.textContent = '문제 → 원인 후보 → 검증할 근거';
  const sub = document.createElement('p');
  sub.className='sub';
  sub.textContent='관측된 숫자와 미검증 가설을 구분합니다. 이벤트가 같은 날짜에 발생해도 매출의 원인으로 단정하지 않습니다.';
  const status=document.createElement('div');status.textContent='근거 기반 진단을 불러오는 중입니다.';
  root.append(heading,sub,status);
  anchor.parentNode.insertBefore(root,anchor);
  const el=(tag,text,cls)=>{
    const node=document.createElement(tag);
    if(cls)node.className=cls;
    if(text!==undefined&&text!==null)node.textContent=String(text);
    return node;
  };
  const fmt=v=>typeof v==='number'&&Number.isFinite(v)?Math.round(v).toLocaleString('ko-KR'):'미확보';
  const pct=v=>typeof v==='number'&&Number.isFinite(v)?(v>0?'+':'')+v.toFixed(1)+'%':'미확보';
  const sources={
    'reports/marketing_operations/latest.json':['운영·매출 원본','../marketing_operations/index.html'],
    'reports/daily_intelligence/latest.json':['18개 분석','../daily_intelligence/index.html'],
    'reports/competitor_content/strategy-summary.json':['경쟁 콘텐츠','../competitor_content/index.html'],
    'reports/competitor_intelligence/latest-quality.json':['경쟁사 수집 품질','../competitor_intel/dashboard.html'],
    'reports/competitor_sale_calendar/latest.json':['경쟁사 행사','../competitor_sale_calendar/index.html']
  };
  const addLink=(parent,label,url)=>{
    if(!url||!/^\.\.\/[a-zA-Z0-9_./-]+$/.test(url))return;
    const link=el('a',label);
    link.href=url;
    parent.append(link);
  };
  fetch('./diagnoses.json?v='+Date.now(),{cache:'no-store'})
    .then(response=>{if(!response.ok)throw Error('HTTP '+response.status);return response.json();})
    .then(data=>{
      if(!Array.isArray(data.problems)||!data.report_date)throw Error('schema');
      status.remove();
      const fin=data.finance||{};
      const main=el('div',null,'headline');
      if(fin.status==='observed'&&typeof fin.aov_pct==='number'){
        main.textContent=`분석 기준 ${data.report_date} · 주문 ${pct(fin.orders_pct)} / 매출 ${pct(fin.revenue_pct)} / 객단가 ${pct(fin.aov_pct)}. 주문수 효과 ${fmt(fin.order_effect_krw)}원, 객단가 효과 ${fmt(fin.aov_effect_krw)}원 (매출 변화 ${fmt(fin.revenue_delta_krw)}원). 가격·상품 구성의 원인은 아직 검증하지 않았습니다.`;
      }else main.textContent=`기준일 ${data.report_date} · 원천 집계가 부족해 매출 분해를 확정하지 않았습니다.`;
      root.append(main);
      const grid=el('div',null,'diag-grid');
      for(const p of data.problems){
        const card=el('article',null,'diag-card');
        const tag=el('span',p.priority+' · '+(p.status==='observed_decomposition'?'산술 확인':p.status==='coverage_limited'?'수집 범위 제한':'검증 자료 필요'),'diag-status'+(p.status==='observed_decomposition'?' known':''));
        card.append(tag,el('h3',p.title),el('p',p.finding,'observed'));
        const fact=el('p',null);
        fact.append(el('strong','확인된 사실: '),document.createTextNode(p.cause_component||'미확보'));
        card.append(fact);
        const hypo=el('div',null,'hypothesis');
        hypo.append(el('span','원인 후보 · 아직 미확정','diagnosis-label'));
        const list=el('p',(p.cause_hypotheses||[]).join(' / '));
        hypo.append(list);
        card.append(hypo);
        const test=el('p',null);
        test.append(el('strong','다음 검증: '),document.createTextNode(p.how_to_verify||'원천 확인'));
        card.append(test);
        if(p.blocked_by?.length)card.append(el('p','현재 장애 요인: '+p.blocked_by.join(', '),'blocked'));
        const evidence=el('div',null,'evidence-list');
        for(const source of p.sources||[]){
          if(sources[source])addLink(evidence,sources[source][0],sources[source][1]);
        }
        card.append(evidence);
        grid.append(card);
      }
      root.append(grid);
      const crawl=data.crawl||{},gaps=crawl.top_gaps||[];
      if(gaps.length){
        const title=el('h2','경쟁사 크롤링 복구 우선순위');
        title.style.marginTop='20px';
        root.append(title);
        const small=el('p',`최근 30일 수집 고유 콘텐츠 · ${crawl.covered_30d??'—'} / ${crawl.brand_count??'—'}개 브랜드에서 관측 사례 확보. 발행량 전체가 아니라 수집된 표본입니다.`,'sub');
        root.append(small);
        const scroll=el('div',null,'crawl-scroll'),table=el('table',null,'crawl-table');
        const h=el('thead');const hr=el('tr');
        ['브랜드','30일 표본','알려진 URL 관측','미방문 URL','수집 제한·우선 조치'].forEach(v=>hr.append(el('th',v)));
        h.append(hr);
        const body=el('tbody');
        gaps.slice(0,10).forEach(g=>{
          const row=el('tr');
          const types=Object.entries(g.error_types||{}).filter(([_,n])=>n>0).map(([k,n])=>k+' '+n).join(', ');
          [g.brand,fmt(g.observed_30d)+' / '+fmt(g.sample_target),
            g.known_url_coverage_pct==null?'미확보':pct(g.known_url_coverage_pct),
            fmt(g.pending_known_urls),
            (types?types+' · ':'')+(g.policy_or_access_blockers?.length?'접근 정책 확인':'공식 공개 URL 재방문')
          ].forEach(v=>row.append(el('td',v)));
          body.append(row);
        });
        table.append(h,body);scroll.append(table);root.append(scroll);
        root.append(el('p','robots/403/429 제한을 우회하지 않고 허용된 공식 공개 자료만 수집합니다. 관측 표본 부족을 경쟁사 실제 콘텐츠 부재로 해석하지 않습니다.','hint'));
        const extra=el('div',null,'evidence-list');
        addLink(extra,'경쟁사 전체 콘텐츠','../competitor_content/index.html');
        addLink(extra,'경쟁사 세일 캘린더','../competitor_sale_calendar/index.html');
        root.append(extra);
      }
      if(data.market?.official_event_overlap?.length){
        const evidence=el('p','동일 기간 경쟁사 공식 행사 '+data.market.official_event_overlap.length+'건이 확인됐지만 매출·PDP 변동의 원인으로 검증되지는 않았습니다.','hint');
        root.append(evidence);
      }
    })
    .catch(()=>{
      status.replaceWith(el('div','통합 진단 데이터가 아직 생성되지 않았습니다. 아래 기존 의사결정 및 원천 상태를 참고해 주세요.','error-state'));
    });
})();
