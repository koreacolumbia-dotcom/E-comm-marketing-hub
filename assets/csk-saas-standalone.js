/* CSK SaaS Design System — standalone report entry.
   The shared theme belongs to this report even when opened directly, without
   the parent dashboard iframe. Never style the two daily report routes. */
(function(){
"use strict";
const entry=document.currentScript;
if(!entry||!document.body)return;
const key=String(entry.dataset.cskRoute||"");
const forbidden=new Set(["daily","daily_intelligence"]);
const path=window.location.pathname;
if(!key||forbidden.has(key)||/\/reports\/daily_intelligence\/index\.html$/.test(path)||
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
})();