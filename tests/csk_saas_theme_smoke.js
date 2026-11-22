#!/usr/bin/env node
/* Source-route smoke test for CSK shared SaaS theme.
   All 23 menu routes are evaluated; owned daily and daily intelligence remain untouched. */
"use strict";
const assert=require("node:assert/strict"),fs=require("node:fs"),path=require("node:path"),vm=require("node:vm");
const ROOT=path.resolve(__dirname,"..");
const HUB=fs.readFileSync(path.join(ROOT,"assets/csk-unified-hub.js"),"utf8");
const THEME=fs.readFileSync(path.join(ROOT,"assets/csk-saas-unified.js"),"utf8");
const SHELL=fs.readFileSync(path.join(ROOT,"index.html"),"utf8");
const CSS=fs.readFileSync(path.join(ROOT,"assets/csk-saas-unified.css"),"utf8");
const re=/^\s*\["([a-z0-9_]+)","([^"]+)","([^"]+)","fa-[^"]+"\]/gm;
const routes=Array.from(HUB.matchAll(re),m=>[m[1],m[2],m[3],"fa"]);
const skipped=new Set(["daily","daily_intelligence"]);
assert.equal(routes.length,23,"All original report menu items must remain");
assert.equal(routes.filter(r=>!skipped.has(r[0])).length,21);
assert(SHELL.includes('href="assets/csk-saas-unified.css'),"Shared CSS not linked");
assert(SHELL.includes('src="assets/csk-saas-unified.js'),"Shared controller not linked");
assert(SHELL.includes('src="assets/csk-unified-hub.js'),"Original routing lost");
assert(CSS.includes("data-csk-route"),"Route-specific adapters missing");
assert(CSS.includes("performanceInline"),"Performance inline styling missing");

function cl() {
 const values=new Set();
 return {add(x){values.add(x)},contains(x){return values.has(x)},toggle(x,on){
  if(on)values.add(x);else values.delete(x);return on;
 }};
}
const origin="https://example.test";
const base=origin+"/E-comm-marketing-hub/";
const listeners={};
let currentDoc=null;
const frame={addEventListener(name,cb){listeners[name]=cb;},get contentDocument(){return currentDoc;}};
const body={classList:cl()};
const document={
 baseURI:base,body,
 getElementById(id){if(id==="frame")return frame;if(id==="hub-menu-root")return {};return null;}
};
const window={
 location:{origin},
 CSKUnifiedMenus:{groups:[["all","All","all","fa",routes]]},
 addEventListener(){}
};
vm.runInNewContext(THEME,{window,document,URL,Object,Set,Map},{timeout:4000});
assert.equal(typeof listeners.load,"function","Frame load listener missing");
assert(body.classList.contains("csk-saas-hub"),"Hub theme not applied");
let styled=0,unchanged=0;
function makeDoc(url,readyState="complete"){
 const links=[];
 const doc={
  location:new URL(url,base),readyState,
  body:{classList:cl(),dataset:{}},
  documentElement:{},
  head:{appendChild(node){links.push(node)}},
  getElementById(id){return links.find(x=>x.id===id)||null;},
  createElement(tag){assert.equal(tag,"link");return {tag};}
 };
 return {doc,links};
}
for(const [key,label,target] of routes){
 const obj=makeDoc(target);currentDoc=obj.doc;listeners.load();
 if(skipped.has(key)){
  assert.equal(obj.links.length,0,"Daily report must not be injected: "+key);
  assert(!obj.doc.body.classList.contains("csk-saas-report"),"Daily report body mutated: "+key);
  unchanged++;
 }else{
  assert.equal(obj.links.length,1,"One design link per route: "+key);
  assert(obj.links[0].href.startsWith(base+"assets/csk-saas-unified.css"),"Root-relative theme URL incorrect: "+key);
  assert(obj.doc.body.classList.contains("csk-saas-report"),"Report class missing: "+key);
  assert.equal(obj.doc.body.dataset.cskRoute,key);
  assert.equal(obj.doc.body.classList.contains("csk-saas-existing"),key==="decision_health");
  listeners.load();
  assert.equal(obj.links.length,1,"Duplicate CSS after iframe load: "+key);
  styled++;
 }
}
for(const p of ["reports/daily_digest/daily/20261121.html","reports/daily_intelligence/index.html"]){
 const x=makeDoc(p);currentDoc=x.doc;listeners.load();
 assert.equal(x.links.length,0,"Daily output must remain unstyled: "+p);
}
const ext=makeDoc("https://elsewhere.test/untrusted");currentDoc=ext.doc;listeners.load();
assert.equal(ext.links.length,0,"Cross-origin CSS injection prohibited");
assert.equal(styled,21);assert.equal(unchanged,2);
console.log("CSK SaaS theme passed: "+styled+" themed, "+unchanged+" daily excluded, no duplicate or foreign injection.");
