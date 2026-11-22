const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
const target={innerHTML:''};
const sandbox={window:{},document:{getElementById:id=>id==='macro-health'?target:null}};
vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../reports/decision_health/macro-board.js'),'utf8'),sandbox);
const {bridge,render}=sandbox.window.CSKMacroBoard;
const prev={sessions:119936,orders:790,revenue:137770660.02},current={sessions:134393,orders:876,revenue:134038093};
const r=bridge(current,prev);
assert.ok(r);
assert.ok(Math.abs(r.delta-(current.revenue-prev.revenue))<.001);
assert.ok(Math.abs(r.traffic+r.conversion+r.aov-r.delta)<.001);
assert.ok(Math.abs(r.aov+17814508.575939246)<.01);
assert.ok(Math.abs(r.conversion+1427321.141986775)<.01);
assert.ok(Math.abs(r.traffic-15509262.697926015)<.01);
assert.equal(bridge({sessions:0,orders:1,revenue:100},prev),null);
assert.equal(bridge(current,{...prev,orders:0}),null);
const base={report_date:'2026-11-20',weekly:{status:'ready',period:'2026-11-14 ~ 2026-11-20',
 comparison_period:'2026-11-07 ~ 2026-11-13',
 current:{status:'complete',total:current},previous:{status:'complete',total:prev},caveat:'test-only'}};
const di={report_date:'2026-11-21',sections:[{number:10,buyer_status:'available',new_mix_pct:70.35,observed_buyers:226,new_buyers:159,existing_buyers:67},
{number:11,status:'partial',channels:[{channel:'Paid Ad',revenue_yoy_pct:68.336},{channel:'Owned Channel',revenue_yoy_pct:-26.07}]}],
product_focus:{temperature:[5,7,6,9,10,12,11,12,12,12,5,1,-1,-1],weather_source:'KMA ASOS'}};
render(base,di);
assert.match(target.innerHTML,/핵심 문제: 객단가·구매 구성/);
assert.match(target.innerHTML,/요인 1 · 객단가·구매 구성/);
assert.match(target.innerHTML,/요인 2 · 구매전환 효율/);
assert.match(target.innerHTML,/요인 3 · 유입 규모/);
assert.match(target.innerHTML,/Paid/);
assert.match(target.innerHTML,/동일 기간 채널별 매출 대사 결과를 아직 확보/);
assert.match(target.innerHTML,/인과 미확정/);
render({...base,weekly:{...base.weekly,current:{status:'partial',total:current}}},di);
assert.match(target.innerHTML,/기여액을 계산하지 않았습니다/);
const weekly={status:'reconciled_with_taxonomy_warning',report_date:'2026-11-20',
 current_period:'2026-11-14 ~ 2026-11-20',previous_period:'2026-11-07 ~ 2026-11-13',
 net_revenue_delta:r.delta,quality:{revenue_reconciled:true,
 taxonomy_warnings:[{channel:'etc',previous_revenue:10279060,share_of_previous_total_pct:7.5}],
 daily_channel_session_mismatch_dates:['2026-11-07','2026-11-08']},
 channels:[{channel:'etc',delta_revenue:-10279060},{channel:'Organic Traffic',delta_revenue:-6689423},
 {channel:'Paid Ad',delta_revenue:7555026},{channel:'Owned Channel',delta_revenue:5332890}]};
render(base,di,weekly,{status:'ga4_measured',report_date:'2026-11-21'});
assert.match(target.innerHTML,/ETC 등 미분류 채널/);
assert.match(target.innerHTML,/1,028만 원/);
assert.match(target.innerHTML,/2일은 채널별 세션 합계/);
assert.match(target.innerHTML,/같은 기간의 상품수·가격 구성이 아직 검증되지 않았습니다/);
assert.doesNotMatch(target.innerHTML,/1.514→1.338개/);
const aligned={status:'ga4_measured',report_date:'2026-11-20',
 period:{previous_start:'2026-11-07',previous_end:'2026-11-13',current_start:'2026-11-14',current_end:'2026-11-20'},
 item_aov_bridge:{items_per_order_previous:1.5,items_per_order_current:1.3,
 realized_item_price_previous:100000,realized_item_price_current:110000},
 admin_reconciliation:{comparable:false}};
render(base,di,weekly,aligned);
assert.match(target.innerHTML,/운영 KPI와 정합성 미통과/);
assert.doesNotMatch(target.innerHTML,/1.500→1.300개/);
aligned.admin_reconciliation.comparable=true;
render(base,di,weekly,aligned);
assert.match(target.innerHTML,/1.500→1.300개/);
assert.match(target.innerHTML,/상품 구성 확인\(동기간·대사 통과\)/);
render(base,di,{...weekly,net_revenue_delta:1000000},aligned);
assert.match(target.innerHTML,/동일 기간 채널별 매출 대사 결과를 아직 확보/);
console.log('Business evidence: same-window channel anomaly, quality gate, buyer suppression, weather alignment and AOV reconciliation');