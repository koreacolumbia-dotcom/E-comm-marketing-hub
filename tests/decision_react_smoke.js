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
   ops.weekly.period,"매출 변화의 핵심 원인","산술분해 · 인과 확정 아님"]);
 assert(overview.includes(Math.round(ops.weekly.current.total.orders).toLocaleString("ko-KR")+"건"),"Observed order count absent");
 assert(overview.includes(Math.round(ops.weekly.current.total.revenue/ops.weekly.current.total.orders).toLocaleString("ko-KR")+"원"),"AOV must preserve won units");
 assert(!overview.includes("INV-1003")&&!overview.includes("$ 24,850"),"Sample invoice numbers must never appear");
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
 const current=source.period&&source.period.current||{},wk=ops.weekly||{};
 if(current.end!==ops.report_date || current.start!==(wk.period||"").split(" ~ ")[0])
   assert(acquisition.includes("기간 불일치"),"Out-of-period source must be labeled");
 const products=await render("products");
 assertContent(products,["상품별 PDP·구매 흐름","상품명 검색","GA4 매칭"]);
 const focus=di.product_focus&&di.product_focus.cards||[];
 if(focus.some(x=>x.sales_source==="ga4_purchase_items"&&x.sales_match_method==="normalized_exact_item_name"))assert(products.includes("상품명 기준 분석"),"Observed product missing");
 const market=await render("market");
 assertContent(market,["경쟁사 공식 행사","공식 기간"]);
 const reactHtml=fs.readFileSync(path.join(root,"reports/decision_health/index.html"),"utf8");
 assert(reactHtml.includes("react-dashboard.js?v=")&&reactHtml.includes("react-dashboard.css?v="));
 assert(reactHtml.includes('id="macro-health"'),"Legacy evidence view must remain");
 console.log("CSK React smoke passed: actual KPI, tabs, period integrity, no invoice fixtures.");
}
main().catch(e=>{console.error(e);process.exitCode=1;});
