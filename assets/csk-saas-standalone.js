/* CSK SaaS Design System — standalone report entry.
   The shared theme belongs to this report even when opened directly, without
   the parent dashboard iframe. Never style the two daily report routes. */
(function(){
"use strict";
const entry=document.currentScript;
if(!entry||!document.body)return;
const key=String(entry.dataset.cskRoute||"");
const forbidden=new Set(["daily"]);
const path=window.location.pathname;
if(!key||forbidden.has(key)||
   /\/reports\/daily_digest\/(?:index\.html|daily\/[^/]+\.html)$/.test(path))return;
const cssId="csk-saas-shared-theme";
const expectedCss=new URL("./csk-saas-unified.css?v=20261122-2",entry.src).href;
let stylesheet=document.getElementById(cssId);
if(!stylesheet){
  stylesheet=document.createElement("link");
  stylesheet.id=cssId;stylesheet.rel="stylesheet";stylesheet.href=expectedCss;
  document.head.appendChild(stylesheet);
}
document.body.classList.add("csk-saas-report");
document.body.classList.toggle("csk-saas-existing",key==="decision_health");
document.body.dataset.cskRoute=key;
document.documentElement.dataset.cskTheme="standalone";
// v2 owns the layout and chart surfaces, while the original report owns data.
const layoutCssId="csk-layout-v2-css",layoutJsId="csk-layout-v2-script";
if(!document.getElementById(layoutCssId)){
  const link=document.createElement("link");
  link.id=layoutCssId;link.rel="stylesheet";
  link.href=new URL("./csk-layout-v2.css?v=20261122-1",entry.src).href;
  document.head.appendChild(link);
}
if(!document.getElementById(layoutJsId)){
  const script=document.createElement("script");
  script.id=layoutJsId;script.defer=true;
  script.src=new URL("./csk-layout-v2.js?v=20261122-1",entry.src).href;
  document.body.appendChild(script);
}
})();