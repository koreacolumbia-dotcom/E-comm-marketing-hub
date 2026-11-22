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
assert.match(target.innerHTML,/단일일 전년비 · 주간 기여액에 더하지 않음/);
assert.match(target.innerHTML,/인과 미확정/);
render({...base,weekly:{...base.weekly,current:{status:'partial',total:current}}},di);
assert.match(target.innerHTML,/기여액을 계산하지 않았습니다/);
console.log('Macro board: exact 3-factor weekly arithmetic, safe missing data and separate channel/date signals');