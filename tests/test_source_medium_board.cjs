const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
const root={innerHTML:'',addEventListener:()=>{},querySelector:()=>null,querySelectorAll:()=>[]};
const ctx={window:{},document:{getElementById:()=>root}};
vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../reports/decision_health/source-medium.js'),'utf8'),ctx);
const board=ctx.window.CSKSourceMedium;
assert.equal(board.group('naver / cpc'),'paid');
assert.equal(board.group('google / organic'),'organic');
assert.equal(board.group('kakao / lms'),'owned');
assert.equal(board.group('unknown / unknown'),'other');
board.render({schema:1,status:'blocked_budget'});
assert.match(root.innerHTML,/비용 상한/);
const sample={schema:1,status:'ga4_measured',period:{
  previous:{start:'2026-11-07',end:'2026-11-13'},
  current:{start:'2026-11-14',end:'2026-11-20'}},
  totals:{previous:{sessions:300,transactions:9,cvr_pct:3,revenue:500000},
          current:{sessions:420,transactions:14,cvr_pct:3.333,revenue:670000}},
  rows:[
    {source_medium:'google / organic',previous:{sessions:100,transactions:2,cvr_pct:2,revenue:100000},
     current:{sessions:120,transactions:6,cvr_pct:5,revenue:400000},
     sessions_change_pct:20,cvr_change_pp:3,transactions_delta:4,revenue_delta:300000},
    {source_medium:'naver / cpc',previous:{sessions:200,transactions:7,cvr_pct:3.5,revenue:400000},
     current:{sessions:300,transactions:8,cvr_pct:2.667,revenue:270000},
     sessions_change_pct:50,cvr_change_pp:-.833,transactions_delta:1,revenue_delta:-130000}
]};
board.render(sample);
assert.match(root.innerHTML,/유입부터 구매전환까지/);
assert.match(root.innerHTML,/GA4 세션/);
assert.match(root.innerHTML,/구매전환율/);
assert.match(root.innerHTML,/소스\/매체 검색/);
assert.match(root.innerHTML,/source|SOURCE/);
assert.match(root.innerHTML,/매출 하락순/);
assert.match(root.innerHTML,/2026-11-14 ~ 2026-11-20/);
assert.match(root.innerHTML,/naver \/ cpc/);
assert.match(root.innerHTML,/구매 건수 ÷ 세션 수/);
assert.equal(board.currentRows().length,2);
board.render({...sample,rows:[{...sample.rows[0],source_medium:'<script>alert(1)</script>'}]});
assert.doesNotMatch(root.innerHTML,/<script>alert\(1\)<\/script>/);
assert.match(root.innerHTML,/&lt;script&gt;/);
console.log('Source/medium board: weekly GA4 KPI, responsive rows, filters, safe escaping, blocked-state and definitions.');
