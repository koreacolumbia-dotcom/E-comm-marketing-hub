const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
const target={innerHTML:''};
const sandbox={window:{},document:{getElementById:id=>id==='macro-health'?target:null}};
vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../reports/decision_health/macro-board.js'),'utf8'),sandbox);
const {bridge,render}=sandbox.window.CSKMacroBoard;
const prev={sessions:119936,orders:790,revenue:137770660.02};
const current={sessions:134393,orders:876,revenue:134038093};
const r=bridge(current,prev);
assert.ok(r);
assert.ok(Math.abs(r.delta-(current.revenue-prev.revenue))<.001);
assert.ok(Math.abs(r.traffic+r.conversion+r.aov-r.delta)<.001);
assert.ok(Math.abs(r.aov+17814508.575939246)<.01);
assert.ok(Math.abs(r.conversion+1427321.141986775)<.01);
assert.ok(Math.abs(r.traffic-15509262.697926015)<.01);
assert.equal(bridge({sessions:0,orders:1,revenue:100},prev),null);
assert.equal(bridge(current,{...prev,orders:0}),null);
const ops={report_date:'2026-11-20',weekly:{status:'ready',
 period:'2026-11-14 ~ 2026-11-20',comparison_period:'2026-11-07 ~ 2026-11-13',
 current:{status:'complete',total:current},previous:{status:'complete',total:prev}}};
const di={report_date:'2026-11-21',sections:[],product_focus:{}};
render(ops,di);
assert.match(target.innerHTML,/이번 주 매출의 핵심 원인/);
assert.match(target.innerHTML,/요인 1 · 객단가·구매 구성/);
assert.match(target.innerHTML,/요인 2 · 구매전환 효율/);
assert.match(target.innerHTML,/요인 3 · 유입 규모/);
assert.doesNotMatch(target.innerHTML,/데이터 품질 확인|검증 상태|채널 세션 정합성/);
render({...ops,weekly:{...ops.weekly,current:{status:'partial',total:current}}},di);
assert.match(target.innerHTML,/기여액을 계산하지 않았습니다/);
const weekly={status:'reconciled_with_taxonomy_warning',report_date:'2026-11-20',
 current_period:'2026-11-14 ~ 2026-11-20',
 previous_period:'2026-11-07 ~ 2026-11-13',net_revenue_delta:r.delta,
 quality:{revenue_reconciled:true},
 channels:[{channel:'etc',delta_revenue:-10279060},
 {channel:'Organic Traffic',delta_revenue:-6689423},
 {channel:'Paid Ad',delta_revenue:7555026},
 {channel:'Owned Channel',delta_revenue:5332890}]};
const aligned={status:'ga4_measured',report_date:'2026-11-20',
 period:{previous_start:'2026-11-07',previous_end:'2026-11-13',
 current_start:'2026-11-14',current_end:'2026-11-20'},
 item_aov_bridge:{items_per_order_previous:1.504,items_per_order_current:1.331,
 realized_item_price_previous:129543.77,realized_item_price_current:130214.51,
 items_per_order_effect:-22436.26,realized_item_revenue_effect:950.72},
 admin_reconciliation:{comparable:true},
 basket_distribution:{previous:{total_orders:790,classified_share_pct:97.85,single_item_pct:79.95,
 multi_item_avg_units:3.677,three_plus_pct:8.15,single_item_revenue:78401960,
 multi_item_revenue:55697160},
 current:{total_orders:876,classified_share_pct:98.74,single_item_pct:78.84,
 multi_item_avg_units:2.645,three_plus_pct:6.71,single_item_revenue:89967804,
 multi_item_revenue:42039829},
 single_item_change_pp:-1.11,three_plus_change_pp:-1.44,multi_item_avg_units_change:-1.032},
 category_bridge:{rows:[
 {category:'OUTER',share_previous_pct:9.51,share_current_pct:6.09},
 {category:'SALE',share_previous_pct:44.02,share_current_pct:47.51}]}};
render(ops,di,weekly,aligned);
assert.match(target.innerHTML,/주문당 상품수 변화/);
assert.match(target.innerHTML,/-22,436원\/건/);
assert.match(target.innerHTML,/1개 구매 주문 80.0% → 78.8%/);
assert.match(target.innerHTML,/다품목 주문 평균 수량 3.68 → 2.65개/);
assert.match(target.innerHTML,/3개 이상 구매 주문 비중 8.2% → 6.7%/);
assert.match(target.innerHTML,/다품목 주문 매출 −1,366만 원/);
assert.match(target.innerHTML,/단품 주문 \+1,157만 원/);
assert.match(target.innerHTML,/OUTER 9.5% → 6.1%/);
assert.match(target.innerHTML,/SALE 44.0% → 47.5%/);
assert.match(target.innerHTML,/추천 실행/);
assert.match(target.innerHTML,/채널별 매출 흐름/);
assert.doesNotMatch(target.innerHTML,/데이터 품질 확인|검증 상태|채널 세션 정합성|원천 로그/);
render(ops,di,weekly,{...aligned,report_date:'2026-11-21'});
assert.doesNotMatch(target.innerHTML,/1개 구매 주문 80.0% → 78.8%/);
render(ops,di,weekly,{...aligned,admin_reconciliation:{comparable:false}});
assert.doesNotMatch(target.innerHTML,/다품목 주문 평균 수량/);
console.log('Business diagnosis: same-week purchase basket depth, mix, revenue, channels and actions; no technical QA UI');
