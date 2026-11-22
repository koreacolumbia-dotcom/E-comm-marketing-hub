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
    'reports/decision_health/ga4_purchase_diagnostics.json':['실제 GA4 상품 집계','../decision_health/ga4_purchase_diagnostics.json'],
    'reports/decision_health/ga4_coupon_usage.json':['GA4 쿠폰 사용 여부','../decision_health/ga4_coupon_usage.json'],
    'reports/decision_health/source_metadata.json':['원천 메타데이터','../decision_health/source_metadata.json'],
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
      const wh=data.warehouse_source_health||{};
      if(wh.inspection_status==='metadata_inspected'&&
         (typeof wh.order_mart_lag_days==='number'||typeof wh.admin_mart_lag_days==='number')){
        const delayed=el('div',null,'headline');
        delayed.textContent=
          '독립 관리자 검증 장애 확인: 주문상품마트 '+(wh.order_mart_last_modified||'날짜 미확인')+
          ' (집계 기준 '+(wh.order_mart_lag_days??'?')+'일 지연), '+
          '관리자 일별마트 '+(wh.admin_mart_last_modified||'날짜 미확인')+
          ' (집계 기준 '+(wh.admin_mart_lag_days??'?')+'일 지연). '+
          '쿠폰/프로모션 컬럼은 있으나 최신 집계가 없어 별도 적재 복구가 필요합니다.';
        root.append(delayed);
      }
      const g=data.ga4_evidence||{};
      if(g.status==='ga4_measured'&&g.item_aov_bridge){
        const b=g.item_aov_bridge,part=g.category_bridge||{};
        const observed=el('article',null,'diag-card');
        observed.style.marginBottom='12px';
        observed.append(el('h3','GA4 구매 원인 분해 · 실제 상품 구매 이벤트'));
        const line=el('p',
          '주문당 구매수량 효과 '+fmt(b.items_per_order_effect)+'원 / '+
          '상품 실현단가 효과 '+fmt(b.realized_item_revenue_effect)+'원 / '+
          'GA4 상품기준 객단가 변화 '+fmt(b.item_aov_delta)+'원','observed');
        observed.append(line);
        observed.append(el('p','상품군 구성 효과(판매 상품 1개당) '+
          fmt(part.category_mix_effect_per_item)+'원 · 동일 상품군 내 실현단가 효과 '+
          fmt(part.within_category_effect_per_item)+'원'));
        const sameKpi=g.admin_reconciliation?.comparable===true;
        const independent=g.independent_admin_verified===true;
        observed.append(el('p',independent?
          '기존 Daily KPI와 독립 관리자 주문 원천까지 확인된 집계입니다. 할인·세금·반품의 세부 귀속은 별도 확인이 필요합니다.':
          sameKpi?'GA4 구매 이벤트와 기존 Daily KPI 합계는 일치하지만, 독립 관리자 주문마트는 최신성이 부족해 별도 검증이 필요합니다.':
          '기존 Daily KPI와 원천 수치가 일치하지 않아 데이터 정합성을 우선 검증해야 합니다.',
          independent?'hint':'blocked'));
        const revenue=g.sales_revenue_bridge||{};
        if(typeof revenue.purchase_revenue_delta_krw==='number'){
          const line=el('p',null,'observed');
          line.append(el('strong','주간 전체 매출 구성: '),
            document.createTextNode(
              '판매수량 효과 '+fmt(revenue.qty_effect_krw)+'원 / '+
              '상품 실현단가 효과 '+fmt(revenue.realized_unit_revenue_effect_krw)+'원 / '+
              '상품매출−구매매출 차액 변화 '+fmt(revenue.item_to_purchase_gap_effect_krw)+'원 = '+
              fmt(revenue.purchase_revenue_delta_krw)+'원'
            ));
          observed.append(line);
          observed.append(el('p','마지막 차액은 원인 미분류 항목입니다. 할인액으로 단정하지 않으며, 쿠폰·배송·환불·집계 정의를 원천에서 확인해야 합니다.','blocked'));
        }
        const cost=g.cost||{};
        observed.append(el('p','BigQuery 처리량: '+fmt(cost.billed_bytes||0)+' bytes 청구 / 실행 상한 1GiB, 사전 dry-run 통과','hint'));
        if(part.rows?.length){
          const t=el('div',null,'crawl-scroll');const table=el('table',null,'crawl-table');
          const head=el('thead'),hr=el('tr');
          ['상품군','이전 비중','현재 비중','구성 효과 (원/개)'].forEach(v=>hr.append(el('th',v)));
          head.append(hr);const body=el('tbody');
          for(const x of part.rows.slice(0,5)){
            const tr=el('tr');
            [x.category,pct(x.share_previous_pct),pct(x.share_current_pct),
             fmt(x.mix_effect_per_item)].forEach(v=>tr.append(el('td',v)));
            body.append(tr);
          }
          table.append(head,body);t.append(table);observed.append(t);
        }
        root.append(observed);
      }else if(['blocked_budget','blocked_credentials','blocked_estimate','query_error','partial_quality'].includes(g.status)){
        const source=el('div','GA4 상품 원인 검증: '+g.status+
          ' · 비용 상한 또는 원천 품질 조건을 만족하지 않아 가격/믹스 원인 확정 보류','error-state');
        root.append(source);
      }
      const couponEvidence=data.ga4_coupon_evidence||{};
      if(couponEvidence.status==='measured_coupon_presence'&&
         typeof couponEvidence.delta_pp==='number'){
        const current=couponEvidence.periods?.current||{};
        const previous=couponEvidence.periods?.previous||{};
        const couponPanel=el('article',null,'diag-card');
        couponPanel.style.marginBottom='12px';
        couponPanel.append(el('h3','상품 쿠폰코드 사용 여부 · 실측'));
        couponPanel.append(el('p',
          '직전 7일 '+pct(previous.order_coupon_presence_pct)+' → 최근 7일 '+
          pct(current.order_coupon_presence_pct)+' ('+
          (couponEvidence.delta_pp>0?'+':'')+couponEvidence.delta_pp.toFixed(1)+'%p)','observed'));
        couponPanel.append(el('p','GA4 items.coupon 값의 존재 여부만 관측했습니다. 실제 쿠폰 할인액이나 매출 영향은 관리자 주문마트 복구 전까지 확정할 수 없습니다.','hint'));
        root.append(couponPanel);
      }
      const grid=el('div',null,'diag-grid');
      for(const p of data.problems){
        const card=el('article',null,'diag-card');
        const observedStatus=['observed_decomposition','observed_ga4_components','observed_product_divergence','source_error_confirmed'].includes(p.status);
        const label=p.status==='observed_decomposition'?'산술 분해 확인':
          p.status==='observed_ga4_components'?'실측 상품 집계':
          p.status==='observed_product_divergence'?'관측된 PDP·구매 방향 차이':
          p.status==='source_error_confirmed'?'원천 오류 확인':
          p.status==='coverage_limited'?'수집 범위 제한':'검증 자료 필요';
        const tag=el('span',p.priority+' · '+label,'diag-status'+(observedStatus?' known':''));
        card.append(tag,el('h3',p.title),el('p',p.finding,'observed'));
        const fact=el('p',null);
        fact.append(el('strong','확인된 사실: '),document.createTextNode(p.cause_component||'미확보'));
        card.append(fact);
        if(p.cause_hypotheses?.length){
          const hypo=el('div',null,'hypothesis');
          hypo.append(el('span','원인 후보 · 아직 미확정','diagnosis-label'));
          const list=el('p',p.cause_hypotheses.join(' / '));
          hypo.append(list);
          card.append(hypo);
        }
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
        const events=data.market.official_event_overlap;
        const panel=el('details',null,'diag-card');
        panel.style.marginTop='12px';
        const toggle=el('summary','같은 기간에 확인된 경쟁사 행사 '+events.length+'건 · 매출 영향 미검증');
        toggle.style.cursor='pointer';
        toggle.style.fontWeight='700';
        panel.append(toggle);
        panel.append(el('p','기간 확인이 가능한 공식 행사만 포함합니다. 할인 내용의 관측과 컬럼비아 매출 변화는 별도 검증 대상입니다.','hint'));
        events.slice(0,8).forEach(event=>{
          const row=el('div',null,'row');
          row.append(el('strong',(event.brand||'경쟁사')+' · '+(event.title||'행사')));
          row.append(el('p',(event.start||'?')+' ~ '+(event.end||'?')+' · 근거 '+(event.date_status||'확인 필요'),'hint'));
          if(typeof event.url==='string'&&/^https:\/\/[a-z0-9.-]+\//i.test(event.url)){
            const link=el('a','공식 행사 원본');
            link.href=event.url;link.target='_blank';link.rel='noopener noreferrer';
            row.append(link);
          }
          panel.append(row);
        });
        root.append(panel);
      }
    })
    .catch(async()=>{
      // First deployment must work even before the scheduled diagnosis JSON exists.
      // Read live, aggregated source data; no customer-level data or synthetic causes.
      try {
        const paths=['../marketing_operations/latest.json','../daily_intelligence/latest.json','../competitor_content/strategy-summary.json'];
        const [ops,di,content]=await Promise.all(paths.map(async u=>{
          const res=await fetch(u+'?v='+Date.now(),{cache:'no-store'});
          if(!res.ok)throw Error('missing source '+u);
          return res.json();
        }));
        const w=ops.weekly||{},c=w.current?.total||{},p=w.previous?.total||{};
        const n=x=>typeof x==='number'&&Number.isFinite(x)?x:null;
        const ca=n(c.revenue)&&n(c.orders)>0?c.revenue/c.orders:null;
        const pa=n(p.revenue)&&n(p.orders)>0?p.revenue/p.orders:null;
        const change=(a,b)=>a!==null&&b!==null&&b>0?(a/b-1)*100:null;
        status.textContent='최신 통합 진단 스냅샷 생성 전 · 원본에서 직접 계산한 임시 요약';
        if(w.status==='ready'&&ca!==null&&pa!==null){
          const headline=el('p','관측: 매출 '+pct(change(c.revenue,p.revenue))+
            ' · 주문 '+pct(change(c.orders,p.orders))+' · 객단가 '+pct(change(ca,pa))+
            '. 매출을 주문수×객단가로 분해한 결과이며, 할인·가격·상품 믹스의 원인은 확인되지 않았습니다.','headline');
          root.append(headline);
        }
        const focus=di.product_focus||{},cards=focus.cards||[];
        if(cards.length){
          const card=el('article',null,'diag-card');
          card.append(el('h3','상품 PDP와 구매 간 검증 필요'),
            el('p','PDP 분석 카드 '+cards.length+'개 · 판매 확인 '+
              cards.filter(x=>n(x.sales_day)!==null).length+'개 · 실측 기온 '+
              (n(focus.weather_days)??'미확보')+'/14일','observed'),
            el('p','확인할 항목: 상품명·SKU 매핑, 전체 구매 이벤트, 기상청 서울 관측소 실측치. 결측은 판매량 0을 뜻하지 않습니다.'));
          root.append(card);
        }
        const brands=content.brands||[];
        const shortage=brands.filter(b=>(n(b.retained_unique_count)||0)<30);
        if(brands.length){
          const card=el('article',null,'diag-card');
          card.append(el('h3','경쟁사 콘텐츠 표본 부족'),
            el('p','수집 대상 '+brands.length+'개 브랜드 · 최근 30일 관측 표본 30건 미만 '+shortage.length+'개 브랜드','observed'),
            el('p','표본 30건 미만은 수집 범위 부족을 의미할 수 있으며 브랜드의 실제 발행량을 의미하지 않습니다.'));
          const list=el('p',shortage.slice(0,8).map(x=>x.brand+' '+(x.retained_unique_count??0)+'건').join(' · '));
          card.append(list);
          root.append(card);
        }
      } catch (_) {
        status.replaceWith(el('div','통합 진단 원본 연결을 확인하지 못했습니다. 기존 의사결정 지표를 확인하세요.','error-state'));
      }
    });
})();
