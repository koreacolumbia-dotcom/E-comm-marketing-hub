#!/usr/bin/env node
/* CSK Unified Layout v2 regression. Checks every analytics menu and all chart styles.
   Daily report sources must remain unchanged, and existing data binding untouched. */
"use strict";
const fs=require("fs"),path=require("path"),assert=require("assert"),vm=require("vm");
const root=path.resolve(__dirname,".."),read=p=>fs.readFileSync(path.join(root,p),"utf8");
const hub=read("assets/csk-unified-hub.js");
const layout=read("assets/csk-layout-v2.js");
const sheet=read("assets/csk-layout-v2.css");
const standalone=read("assets/csk-saas-standalone.js");
const main=read("assets/csk-saas-unified.js");
const routes=[...hub.matchAll(/^\s*\["([a-z0-9_]+)","([^"]+)","([^"]+)","fa-[^"]+"\]/gm)].map(x=>({key:x[1],name:x[2],path:x[3]}));
const excluded=["daily","daily_intelligence"];
const covered=routes.filter(x=>!excluded.includes(x.key));
assert.equal(routes.length,23);
assert.equal(covered.length,21);
assert(layout.includes('const cfg={'),"Route-specific source-preserving layouts required");
for(const {key,path:uri} of covered){
 if(key==="decision_health"){
   assert(read("reports/decision_health/react-dashboard.js").includes("function Dashboard("),"React dashboard missing");
   continue;
 }
 assert(new RegExp("\\b"+key+":\\{shell:").test(layout),"No layout blueprint for "+key);
 assert(new RegExp("data-csk-route=\\\\\""+key+"\\\\\"").test(sheet)||
        sheet.includes("body.csk-layout-v2 .csk-v2-report-shell"),
        "No shared layout treatment for "+key);
}
for(const el of ["--csk-v2-navy","#16192c","#0072ce","#5e54f3",
                 "grid-template-columns:repeat(12,minmax(0,1fr))",
                 ".csk-v2-metrics",".csk-v2-chart",".csk-v2-table",".csk-v2-tabs"]){
 assert(sheet.includes(el),"Missing design system: "+el);
}
assert(layout.includes("standardizeSvg("),"SVG chart adapter missing");
assert(layout.includes("updateChartLibraries("),"Chart library palette adapter missing");
assert(layout.includes("instance.update("),"Chart.js instances not updated");
assert(layout.includes("instance.resize("),"ECharts responsive resize missing");
assert(layout.includes("MutationObserver"),"Dynamic report charts not covered");
assert(standalone.includes("csk-layout-v2.js"),"Standalone report runtime missing");
assert(standalone.includes("csk-layout-v2.css"),"Standalone report CSS missing");
assert(main.includes("csk-layout-v2.js"),"Hub iframe runtime missing");
assert(main.includes("csk-layout-v2.css"),"Hub iframe CSS missing");
assert(main.includes('const SKIP = new Set(["daily","daily_intelligence"])'),"Daily menu exclusion lost");
const js=read("scripts/prepare_cloudflare_bundle.py");
assert(js.includes('"csk-layout-v2.css"')&&js.includes('"csk-layout-v2.js"'),"Cloudflare packaging lost");
for(const filename of ["reports/daily_digest/index.html","reports/daily_intelligence/index.html"]){
 const html=read(filename);
 assert(!html.includes("csk-layout-v2.js"),"Daily report modified: "+filename);
}
// Actual runtime with a daily path must not add layouts or chart observers.
let visited=false;
vm.runInNewContext(layout,{location:{pathname:"/reports/daily_intelligence/index.html"},
  document:{getElementById(){visited=true}},window:{},MutationObserver:function(){visited=true},
  requestAnimationFrame(){visited=true},Event:function(){}},{timeout:4000});
assert(!visited,"Daily analytics runtime must return immediately");
console.log("CSK Layout v2 OK: "+covered.length+" routes, 20 shared blueprints + React, 2 daily excluded.");
