import assert from 'node:assert/strict';import {creativeFormat,uxEvidence,monthly,bannerRows,purposeGap} from '../../reports/competitor_content/advanced.mjs';
assert.equal(creativeFormat({images:[{width:1800,height:800}]}),"LANDSCAPE");assert.equal(creativeFormat({thumbnail:"x"}),"UNKNOWN");assert.equal(uxEvidence({}),null);
const x={id:"a",brand:"TNF",title:"old",thumbnail:"1",first_seen_at:"2026-11-18",tpo:["하이킹"],purpose:{code:"EDITORIAL"}};
assert.equal(monthly([x],[],"TNF")[0].tpo["하이킹"].length,1);
const rows=bannerRows([{date:"2026-11-17",brands:[{brand:"TNF",pages_checked:1,items:[x]}]},{date:"2026-11-18",brands:[{brand:"TNF",pages_checked:0,items:[]}]},{date:"2026-11-19",brands:[{brand:"TNF",pages_checked:1,items:[{...x,thumbnail:"2"}]}]}],"TNF");assert.equal(rows[0].rows[0].banner_change,"CREATIVE_CHANGED");
assert.equal(purposeGap([x],[["EDITORIAL","콘텐츠"]])[0].gap,null);console.log("Advanced content tests passed");
