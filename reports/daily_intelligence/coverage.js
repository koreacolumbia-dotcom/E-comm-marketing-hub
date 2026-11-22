/* CSK observed-source coverage: no synthetic weather or sales metrics. */
(() => {
  'use strict';
  const host = document.querySelector('main > header');
  if (!host || document.getElementById('csk-source-coverage')) return;

  const style = document.createElement('style');
  style.textContent = `
    #csk-source-coverage{margin:12px 0 16px;padding:14px 2px 12px;border-top:1px solid #dce5ee;border-bottom:1px solid #dce5ee;color:#23405a}
    #csk-source-coverage .cover-head{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:6px;margin-bottom:10px}
    #csk-source-coverage .cover-head strong{font-size:14px}
    #csk-source-coverage .cover-head small{font-size:11px;color:#687d90}
    #csk-source-coverage .cover-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:7px}
    #csk-source-coverage .cover-stat{padding:9px 10px;background:#f4f8fb;border-radius:8px;min-width:0}
    #csk-source-coverage .cover-stat span{display:block;color:#637b90;font-size:10px}
    #csk-source-coverage .cover-stat strong{display:block;font-size:16px;margin-top:3px;font-variant-numeric:tabular-nums}
    #csk-source-coverage .cover-actions{margin:10px 0 0;font-size:12px;line-height:1.5}
    #csk-source-coverage a{color:#12568d;text-decoration:underline;text-underline-offset:2px}
    @media(max-width:740px){#csk-source-coverage{padding:10px 0;margin:9px 0 12px}
      #csk-source-coverage .cover-grid{grid-template-columns:repeat(2,minmax(0,1fr))}
      #csk-source-coverage .cover-stat{padding:9px}#csk-source-coverage .cover-stat strong{font-size:16px}
      #csk-source-coverage .cover-head strong{font-size:13px}}
  `;
  document.head.appendChild(style);

  const root = document.createElement('section');
  root.id = 'csk-source-coverage';
  root.setAttribute('aria-label', '통합 분석 데이터 확보 상태');
  const top = document.createElement('div');
  top.className = 'cover-head';
  const title = document.createElement('strong');
  title.textContent = '데이터 확보 현황 · 분석 신뢰도';
  const date = document.createElement('small');
  date.textContent = '원천 상태 확인 중';
  top.append(title, date);
  const grid = document.createElement('div');
  grid.className = 'cover-grid';
  const bottom = document.createElement('p');
  bottom.className = 'cover-actions';
  bottom.textContent = '데이터 상태를 확인하는 중입니다.';
  root.append(top, grid, bottom);
  host.insertAdjacentElement('afterend', root);

  const tile = (caption, value) => {
    const node = document.createElement('div');
    node.className = 'cover-stat';
    const c = document.createElement('span');
    c.textContent = caption;
    const v = document.createElement('strong');
    v.textContent = value;
    node.append(c, v);
    return node;
  };

  fetch('./latest.json?v=' + Date.now(), {cache:'no-store'})
    .then(res => {
      if (!res.ok) throw new Error('HTTP ' + res.status);
      return res.json();
    })
    .then(d => {
      const sections = Array.isArray(d.sections) ? d.sections : [];
      const count = status => sections.filter(x => x.status === status).length;
      const focus = d.product_focus || {};
      const cards = Array.isArray(focus.cards) ? focus.cards : [];
      const salesCovered = cards.filter(c => Number.isFinite(c.sales_day) && c.sales_day >= 0).length;
      const ga4Source=focus.ga4_purchase_status||'not_connected';
      const weatherDays = Number.isFinite(focus.weather_days) ? focus.weather_days : null;
      const pdpNames = Number.isFinite(focus.matched_pdp_names) ? focus.matched_pdp_names : null;
      const sourceRequired = count('source_required') + count('validation_required');
      const dateString = d.report_date || focus.date || '기준일 미확인';
      date.textContent = '집계 기준 ' + dateString + (
        d.target_date && d.target_date !== d.report_date ? ' · 갱신 지연' : ''
      );
      grid.replaceChildren(
        tile('18개 분석 · 집계 완료', count('available') + ' / ' + sections.length),
        tile('부분 집계 / 원본 필요', count('partial') + ' / ' + sourceRequired),
        tile('실측 기온 확보', weatherDays === null ? '확인 필요' : weatherDays + ' / 14일'),
        tile('상품 카드 판매량', cards.length ? salesCovered + ' / ' + cards.length : '확인 필요')
      );
      const missing = [];
      if (cards.length && salesCovered < cards.length) missing.push('상품 판매량 원천 연결 (GA4 '+ga4Source+')');
      const weatherError=(d.weather_source_health||{}).current;
      if(weatherError==='access_denied_403')missing.push('기상청 API 403 접근권한 복구');
      else if(weatherError==='rate_limited_429')missing.push('기상청 API 429 요청제한 확인');
      else if(weatherError==='credential_missing')missing.push('기상청 API 키 연결');
      else if(weatherDays !== null && weatherDays < 14)missing.push('서울 기상 실측치 연결');
      if (sourceRequired) missing.push('원본 미연결 분석 ' + sourceRequired + '건 검증');
      bottom.replaceChildren();
      const lead = document.createElement('span');
      const covered = pdpNames !== null ? '상품명 PDP 비교 ' + pdpNames + '개 확보. ' : '';
      lead.textContent = covered + (missing.length ? '복구 우선순위: ' + missing.join(' · ') + '. ' :
        '현재 확인된 원천 상태를 반영했습니다. ');
      bottom.append(lead);
      if ((cards.length && salesCovered < cards.length) || (weatherDays !== null && weatherDays < 14)) {
        const caution = document.createElement('strong');
        caution.textContent = '판매·날씨 영향은 원천 확보 전까지 인과관계로 판단하지 않습니다. ';
        bottom.append(caution);
      }
      const link = document.createElement('a');
      link.href = '../decision_health/index.html';
      link.textContent = '검증 현황 상세';
      bottom.append(link);
    })
    .catch(() => {
      date.textContent = '집계 원본 연결 실패';
      grid.replaceChildren(tile('데이터', '확인 불가'));
      bottom.textContent = '원본 파일이 연결되지 않아 데이터 확보율을 표시할 수 없습니다.';
    });
})();
