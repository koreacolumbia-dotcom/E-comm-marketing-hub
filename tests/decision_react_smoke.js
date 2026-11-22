#!/usr/bin/env node
/* Regression smoke test: React executive screen must use real source JSON.
   Fake React builds a pure component tree; no CDN, DOM, or browser required. */
"use strict";
const fs=require("fs"),vm=require("vm"),path=require("path"),assert=require("assert");
const root=path.resolve(__dirname,"..");
const file=path.join(root,"reports/decision_health/react-dashboard.js");
const code=fs.readFileSync(file,"utf8");
const ops=JSON.parse(fs.readFileSync(path.join(root,"reports/marketing_operations/latest.json"),"utf8"));
const di=JSON.parse(fs.readFileSync(path.join(root,"reports/daily_intelligence/latest.json"),"utf8"));
const source=JSON.parse(fs.readFileSync(path.join(root,"reports/decision_health/source_medium_weekly.json"),"utf8"));
const sourceDrivers=JSON.parse(fs.readFileSync(path.join(root,"reports/decision_health/source_driver_bridge.json"),"utf8"));
const dx=JSON.parse(fs.readFileSync(path.join(root,"reports/decision_health/diagnoses.json"),"utf8"));
const deepTexts=(node,out=[])=>{
 if(node==null || node===false)return out;
 if(typeof node==="string"||typeof node==="number"){out.push(String(node));return out;}
 if(Array.isArray(node)){node.forEach(x=>deepTexts(x,out));return out;}
 if(node.props && node.props.placeholder)out.push(String(node.props.placeholder));
 if(node.props && node.props["aria-label"])out.push(String(node.props["aria-label"]));
 if(node.children)node.children.forEach(x=>deepTexts(x,out));
 return out;
};
function assertContent(t,strings){
 strings.forEach(s=>assert(t.includes(s),"Expected source-derived UI text: "+s));
}
async function render(tab,filters){
 let tree=null,index=0;
 const values=[tab,null,"all","loss","all","",false,"",null,null,null,false,false];
 const fallback=filters||{};
 Object.entries(fallback).forEach(([k,v])=>{values[Number(k)]=v;});
 const React={
   createElement(type,props,...children){
     let p=Object.assign({},props||{},{children});
     return typeof type==="function"?type(p):{type,props:p,children};
   },
   useState(defaultValue){
     const pos=index++;
     return [values[pos]===undefined?defaultValue:values[pos],()=>{}];
   },
   useMemo:fn=>fn(),
   useEffect:()=>{}
 };
 const window={React,ReactDOM:{createRoot:()=>({render(t){tree=t;}})}};
 const document={getElementById:id=>id==="csk-react-root"?{}:null,
   body:{classList:{add(){},remove(){},toggle(){}}}};
 const fetch=url=>{
   const p=path.resolve(path.dirname(file),url.split("?")[0]);
   assert(p.startsWith(path.resolve(root,"reports")+path.sep),"Unauthorized path "+p);
   return Promise.resolve({ok:fs.existsSync(p),status:fs.existsSync(p)?200:404,
     json:()=>Promise.resolve(JSON.parse(fs.readFileSync(p,"utf8")))});
 };
 vm.runInNewContext(code,{window,document,fetch,console},{timeout:5000});
 for(let i=0;i<8;i++)await Promise.resolve();
 await new Promise(resolve=>setImmediate(resolve));
 assert(tree && tree.type==="div","React root not rendered: "+tab);
 return deepTexts(tree).join(" ");
}
async function main(){
 const overview=await render("overview");
 assertContent(overview,["이커머스 성과 인텔리전스",ops.report_date,"매출","주문","세션","객단가",
   ops.weekly.period,"매출 변화의 핵심 원인","산술 확인 · 인과 미확정","변화 요인 1"]);
 assert(overview.includes(Math.round(ops.weekly.current.total.orders).toLocaleString("ko-KR")+"건"),"Observed order count absent");
 assert(overview.includes(Math.round(ops.weekly.current.total.revenue/ops.weekly.current.total.orders).toLocaleString("ko-KR")+"원"),"AOV must preserve won units");
 assert(!overview.includes("INV-1003")&&!overview.includes("$ 24,850"),"Sample invoice numbers must never appear");
 if(sourceDrivers.status==="measured_descriptive" && sourceDrivers.report_date===ops.report_date){
   assert(sourceDrivers.causal===false,"Source driver must never claim causal lift");
   assert(sourceDrivers.quality.no_additional_bigquery_jobs===true,"Source driver must reuse measured reports");
   assert(Math.abs(Object.values(sourceDrivers.driver_totals).reduce((a,b)=>a+b,0)-
       sourceDrivers.headline.revenue_delta)<1,"Source driver must sum to observed revenue delta");
   assertContent(overview,["어떤 유입에서 매출이 변했을까?","CHANNEL MOVEMENT",
       "검토 표본 "+(sourceDrivers.row_count-sourceDrivers.quality.small_sample_rows)]);
   const leaders=sourceDrivers.top_gains.slice(0,3);
   assert(leaders.some(x=>overview.includes(x.source_medium)),"Measured top source not surfaced");
 }
 const thisYear=ops.calendar||{},actual=thisYear.impact_diagnostics||{};
 if(Array.isArray(thisYear.current) && thisYear.current.length) {
   assertContent(overview,["올해 행사 · 예상 여파","2026 CAMPAIGN CONTEXT",
     "2025 vs 2026 윈터페스티벌 임시 비교 리포트 열기"]);
   const sample=thisYear.current.find(x=>x.impact_hypothesis);
   if(sample)assert(overview.includes(sample.title) && overview.includes(sample.impact_hypothesis),
     "Current-year source event and forecast effect must be visible");
   if(actual.status==="descriptive")
     assertContent(overview,["본행사 전 "+actual.pre_days+"일 · 매출 YoY",
       "본행사 시작 후 "+actual.active_days+"일 · 매출 YoY",
       "동일 요일 비교, 추정 기여액 아님"]);
 }
 const yr=ops.weekly && ops.weekly.yoy||{},lastYear=ops.calendar||{};
 if(yr.status==="ready" && lastYear.yoy_window===yr.period){
   assertContent(overview,["작년 행사 · 매출 비교","YOY CONTEXT","동일 요일 기준"]);
   const confirmed=lastYear.last_year_confirmed||[];
   assert(confirmed.some(x=>overview.includes(x.title)),"At least one verified last-year event must appear");
   if(yr.daily_comparison && yr.daily_comparison.length===7){
     assert(overview.includes("당일 매출 YoY"),"Event-aligned revenue context absent");
   }
   const plans=lastYear.last_year_planned||[];
   if(plans.length)assert(overview.includes("계획 기록 "+plans.length+"건"),"Unverified sending plans must be distinguished");
 }
 const outcomes=JSON.parse(fs.readFileSync(path.join(root,"reports/decision_health/action_outcomes.json"),"utf8"));
 if(outcomes.report_date===ops.report_date){
   assertContent(overview,["조치 후 실제로 개선됐는가?","실행 "+outcomes.implemented,"측정 "+outcomes.measured]);
   if(outcomes.implemented===0)assert(overview.includes("실행 완료 근거 0건"),
     "Never invent completed interventions");
 }
 const acquisition=await render("acquisition");
 assertContent(acquisition,["유입부터 구매전환까지","소스/매체 검색"]);
 if(source.rows.length){
   assert(acquisition.includes("GA4 세션 귀속"),"Observed source attribution label missing");
   const top=[...source.rows].sort((a,b)=>a.revenue_delta-b.revenue_delta).slice(0,12);
   assert(top.some(x=>acquisition.includes(x.source_medium)),"No observed source present");
 } else {
   assert(acquisition.includes("소스/매체 데이터 미확보"),
     "Blocked or unavailable BigQuery must not fabricate source-medium data");
 }
 if(sourceDrivers.status==="measured_descriptive" && sourceDrivers.report_date===ops.report_date){
   const leading=sourceDrivers.top_gains.find(x=>x.components);
   if(leading){
     const selected=await render("acquisition",{8:leading.source_medium});
     assertContent(selected,["이 소스의 매출 변화 분해","이 소스의 다음 검증"]);
     if(leading.next_validation)assert(selected.includes(leading.next_validation),
       "Source-level next action must be grounded in the measured leading factor");
   }
 }
 const current=source.period&&source.period.current||{},wk=ops.weekly||{};
 if(current.end!==ops.report_date || current.start!==(wk.period||"").split(" ~ ")[0])
   assert(acquisition.includes("기간 불일치"),"Out-of-period source must be labeled");
 const products=await render("products");
 assertContent(products,["상품별 PDP·구매 흐름","상품명 검색","GA4 매칭"]);
 const focus=di.product_focus&&di.product_focus.cards||[];
 if(focus.some(x=>x.sales_source==="ga4_purchase_items"&&x.sales_match_method==="normalized_exact_item_name"))assert(products.includes("상품명 기준 분석"),"Observed product missing");
 const market=await render("market");
 assertContent(market,["경쟁사 공식 행사","공식 기간","경쟁사 비교 · 검증 상태"]);
 const festivalReport=fs.readFileSync(path.join(root,
   "reports/decision_health/winter_festival_comparison_2026.html"),"utf8");
 assertContent(festivalReport,["본행사 D1–D2","동일 경과일","2025 WINTER FESTIVAL",
   "2026 WINTER FESTIVAL","매출 증가","Shapley","2026.11.21"]);
 const reactHtml=fs.readFileSync(path.join(root,"reports/decision_health/index.html"),"utf8");
 assert(reactHtml.includes("react-dashboard.js?v=")&&reactHtml.includes("react-dashboard.css?v="));
 assert(reactHtml.includes('id="macro-health"'),"Legacy evidence view must remain");
 console.log("CSK React smoke passed: actual KPI, tabs, period integrity, no invoice fixtures.");
}
main().catch(e=>{console.error(e);process.exitCode=1;});
