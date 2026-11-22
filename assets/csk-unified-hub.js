/* CSK Unified Hub: the one source of truth for desktop, mobile, and report routing. */
(function () {
"use strict";
const GROUPS=[
 ["overview","통합 홈","홈","fa-house",[
  ["decision_health","의사결정 센터","reports/decision_health/index.html","fa-shield-halved"],
  ["summary","전체 Summary","reports/index.html","fa-chart-simple"],
  ["daily","Owned 데일리","reports/daily_digest/index.html","fa-calendar-day"],
  ["daily_intelligence","18개 통합 분석","reports/daily_intelligence/index.html","fa-chart-line"]]],
 ["commerce","매출·상품","성과","fa-chart-column",[
  ["performance","실적 대시보드","reports/performance_dashboard.html?v=20261121070922","fa-chart-pie"],
  ["pdp_priority","PDP 이탈 우선순위","reports/pdp_opportunity/index.html","fa-arrow-trend-down"],
  ["product_keyword","상품·키워드 성과","reports/product_keyword/index.html","fa-magnifying-glass-chart"],
  ["utm_channel","소스·매체","reports/utm_channel/index.html","fa-chart-line"],
  ["funnel","구매 퍼널","reports/daily_digest/owned_funnel_tab.html","fa-filter"],
  ["purchase_pattern","구매 패턴","reports/purchase_pattern/index.html","fa-bag-shopping"]]],
 ["customer","고객·CRM","고객","fa-users",[
  ["member","회원·코호트","reports/member_funnel/index.html","fa-users"],
  ["owned","Owned 메시지","reports/owned_portal/index.html","fa-envelope"],
  ["edm_intelligence","경쟁사 EDM","edm-intelligence.html?v=20261118","fa-envelope-open-text"],
  ["crm_intelligence","경쟁사 CRM","reports/crm_intelligence/index.html?v=20261119","fa-diagram-project"]]],
 ["competitors","경쟁사·콘텐츠","경쟁사","fa-binoculars",[
  ["sale_calendar","경쟁사 세일 캘린더","reports/competitor_sale_calendar/index.html?v=20261118-editorial","fa-calendar-days"],
  ["competitor_content","경쟁사 콘텐츠","reports/competitor_content/index.html?v=20261118-purpose-v6","fa-book-open"],
  ["competitor","경쟁사 상품 비교","reports/competitor_intel/dashboard.html","fa-chart-column"],
  ["price","네이버 최저가","reports/naver_lowest_price.html","fa-won-sign"],
  ["hero","기획전·배너","reports/hero_main.html","fa-image"]]],
 ["operations","운영·AI·VOC","운영","fa-list-check",[
  ["ops3","마케팅 운영·액션","reports/marketing_operations/index.html","fa-list-check"],
  ["ai_agent","AI Agent","reports/ai_agent/index.html","fa-robot"],
  ["voc","커뮤니티 VOC","reports/external_signal.html","fa-tower-broadcast"],
  ["crema","공식몰 리뷰 VOC","reports/voc_crema/index.html","fa-comments"]]]
];
const lookup=new Map();
GROUPS.forEach(g=>g[4].forEach(p=>{if(lookup.has(p[0]))throw Error("Duplicate route: "+p[0]);lookup.set(p[0],g)}));
const root=document.getElementById("hub-menu-root"),mobile=document.getElementById("hub-mobile-root");
if(!root||!mobile)throw Error("Unified menu mount missing");
const css=document.createElement("style");
css.textContent=[
 ".csk-menu-search{width:100%;padding:10px;border:1px solid #d8e0eb;border-radius:10px;background:white;font-size:12px}",
 ".csk-group{margin:6px 0 10px}.csk-group-head{display:flex;width:100%;gap:9px;align-items:center;padding:12px;background:transparent;border:0;border-radius:10px;color:#425872;font-size:13px;font-weight:800;cursor:pointer}",
 ".csk-group-head:hover,.csk-group.open .csk-group-head{background:#eaf1f8;color:#002d72}.csk-group-head .fa-chevron-down{margin-left:auto;font-size:11px}",
 ".csk-group-items[hidden],.csk-group[hidden]{display:none!important}.csk-group .nav-item{margin:2px 0 2px 14px;padding:8px 9px;font-size:12px;border-radius:9px;animation:none;min-height:39px;font-weight:700}",
 ".csk-group .nav-item.active{transform:none;box-shadow:none;background:#e9f2fb}.csk-group .nav-item:hover{transform:none;box-shadow:none}",
 ".csk-mobile-inner{display:grid;grid-template-columns:repeat(6,minmax(0,1fr))}.csk-mobile-inner .mo-tab,.csk-more{border:0;background:transparent;padding:8px 2px;display:flex;flex-direction:column;align-items:center;gap:2px;font-size:10px;color:#64748b;cursor:pointer}",
 ".csk-mobile-inner i{font-size:17px}.csk-mobile-inner .mo-tab.active{background:#e9f2fb;border-radius:9px;color:#002d72}",
 "#csk-drawer-scrim{position:fixed;inset:0;background:rgba(20,35,50,.45);z-index:94;display:none}#csk-drawer-scrim.open{display:block}",
 "#csk-drawer{position:fixed;z-index:95;top:0;right:0;bottom:0;width:min(380px,92vw);background:#fff;transform:translateX(100%);transition:transform .2s;overflow:auto;padding:18px;box-shadow:-15px 0 45px #0002}",
 "#csk-drawer.open{transform:translateX(0)}.csk-drawer-title{display:flex;justify-content:space-between;font-weight:800;font-size:18px;margin-bottom:14px}",
 ".csk-drawer-link{display:block;width:100%;text-align:left;padding:11px;background:none;border:0;border-radius:8px;cursor:pointer;color:#345}.csk-drawer-link.active{background:#e9f2fb;font-weight:800}",
 ".csk-drawer-label{font-size:12px;font-weight:800;color:#5c7187;margin:14px 0 4px}",
 ".csk-drawer-link[hidden],.csk-drawer-label[hidden]{display:none!important}",
 "#hub-mobile-root{z-index:70;background:#fffffff2;border-top:1px solid #e1e8f1;backdrop-filter:blur(14px)}@media(min-width:1024px){#hub-mobile-root{display:none!important}}",
 "@media(max-width:1023px){body.csk-unified-hub #hub-mobile-root.mo-tabbar{display:block!important;position:fixed!important;bottom:0!important;left:0!important;right:0!important}body.csk-unified-hub>main{padding-bottom:90px!important}}"
].join("");
document.head.appendChild(css);
const search=document.createElement("input");search.id="hub-nav-search";search.type="search";search.className="csk-menu-search";search.placeholder="메뉴 검색 (Ctrl+K)";search.setAttribute("aria-label","분석 메뉴 검색");
root.appendChild(search);
const holders=new Map();
GROUPS.forEach((g,i)=>{
 const section=document.createElement("section");section.className="nav-section csk-group"+(i===0?" open":"");section.dataset.group=g[0];
 const head=document.createElement("button");head.type="button";head.className="csk-group-head";head.setAttribute("aria-expanded",String(i===0));
 const ic=document.createElement("i");ic.className="fa-solid "+g[3];const name=document.createElement("span");name.textContent=g[1];const caret=document.createElement("i");caret.className="fa-solid fa-chevron-down";head.append(ic,name,caret);
 const items=document.createElement("div");items.className="csk-group-items";items.hidden=i!==0;
 g[4].forEach(p=>{
  const el=document.createElement("div");el.className="nav-item"+(p[0]==="decision_health"?" active":"");el.dataset.key=p[0];el.dataset.target=p[2];el.dataset.label=p[1];el.setAttribute("role","button");el.tabIndex=0;
  const icon=document.createElement("i");icon.className="fa-solid "+p[3];const label=document.createElement("span");label.textContent=p[1];el.append(icon,label);items.appendChild(el);
 });
 section.append(head,items);root.appendChild(section);holders.set(g[0],{section,head,items});
 head.onclick=()=>{const open=items.hidden;holders.forEach(h=>{h.items.hidden=true;h.section.classList.remove("open");h.head.setAttribute("aria-expanded","false")});items.hidden=!open;section.classList.toggle("open",open);head.setAttribute("aria-expanded",String(open))};
});
const bottom=document.createElement("div");bottom.className="csk-mobile-inner";mobile.appendChild(bottom);
GROUPS.forEach(g=>{
 const p=g[4][0],el=document.createElement("button");el.type="button";el.className="mo-tab";el.dataset.key=p[0];el.dataset.target=p[2];el.dataset.label=p[1];el.setAttribute("aria-label",g[1]);
 const ic=document.createElement("i");ic.className="fa-solid "+g[3];const text=document.createElement("span");text.textContent=g[2];el.append(ic,text);bottom.appendChild(el);
});
const more=document.createElement("button");more.type="button";more.className="csk-more";more.innerHTML='<i class="fa-solid fa-bars"></i><span>전체</span>';bottom.appendChild(more);
const scrim=document.createElement("div");scrim.id="csk-drawer-scrim";document.body.appendChild(scrim);
const drawer=document.createElement("aside");drawer.id="csk-drawer";drawer.innerHTML='<div class="csk-drawer-title"><span>전체 분석 메뉴</span><button type="button" id="csk-drawer-close">닫기</button></div><input class="csk-menu-search" type="search" id="csk-drawer-search" placeholder="상세 분석 검색" aria-label="상세 분석 검색"><div id="csk-drawer-list"></div>';document.body.appendChild(drawer);
const list=document.getElementById("csk-drawer-list");
GROUPS.forEach(g=>{
 const header=document.createElement("div");header.className="csk-drawer-label";header.textContent=g[1];list.appendChild(header);
 g[4].forEach(p=>{const btn=document.createElement("button");btn.type="button";btn.className="csk-drawer-link";btn.dataset.key=p[0];btn.dataset.label=p[1];btn.textContent=p[1];btn.onclick=()=>{document.querySelector('.nav-item[data-key="'+p[0]+'"]')?.click();closeDrawer()};list.appendChild(btn)});
});
function closeDrawer(){drawer.classList.remove("open");scrim.classList.remove("open")}
function openDrawer(){drawer.classList.add("open");scrim.classList.add("open");document.getElementById("csk-drawer-search").focus()}
more.onclick=openDrawer;scrim.onclick=closeDrawer;document.getElementById("csk-drawer-close").onclick=closeDrawer;
function filter(target,drawerMode){
 const term=target.value.trim().toLowerCase();
 if(drawerMode){
  list.querySelectorAll(".csk-drawer-link").forEach(x=>x.hidden=!!term&&!x.dataset.label.toLowerCase().includes(term));
  list.querySelectorAll(".csk-drawer-label").forEach(head=>{let item=head.nextElementSibling,seen=false;while(item&&!item.classList.contains("csk-drawer-label")){if(!item.hidden)seen=true;item=item.nextElementSibling}head.hidden=!seen});
 }else holders.forEach((h,id)=>{const any=GROUPS.find(g=>g[0]===id)[4].some(p=>p[1].toLowerCase().includes(term));h.section.hidden=!!term&&!any;h.items.querySelectorAll(".nav-item").forEach(x=>x.hidden=!!term&&!x.dataset.label.toLowerCase().includes(term));if(term&&any){h.items.hidden=false;h.section.classList.add("open");h.head.setAttribute("aria-expanded","true")}});
}
search.addEventListener("input",()=>filter(search,false));
const dsearch=document.getElementById("csk-drawer-search");dsearch.addEventListener("input",()=>filter(dsearch,true));
document.addEventListener("keydown",e=>{
 if((e.ctrlKey||e.metaKey)&&e.key.toLowerCase()==="k"){e.preventDefault();if(innerWidth<1024)openDrawer();else search.focus()}
 if(e.key==="Escape")closeDrawer();
 if((e.key==="Enter"||e.key===" ")&&e.target?.classList?.contains("nav-item")){e.preventDefault();e.target.click()}
});
function activate(key){const group=lookup.get(key);if(!group)return;
 holders.forEach((h,id)=>{const on=id===group[0];h.items.hidden=!on;h.section.classList.toggle("open",on);h.head.setAttribute("aria-expanded",String(on))});
 bottom.querySelectorAll(".mo-tab").forEach(x=>x.classList.toggle("active",lookup.get(x.dataset.key)?.[0]===group[0]));
 list.querySelectorAll(".csk-drawer-link").forEach(x=>x.classList.toggle("active",x.dataset.key===key));
 closeDrawer();
}
window.CSKUnifiedMenus={activate,groups:GROUPS,closeDrawer};
document.body.classList.add("csk-unified-hub");document.body.dataset.unifiedHub="true";
})();

(function () {
    const frame = document.getElementById("frame");
    const pcItems = Array.from(document.querySelectorAll(".nav-item[data-target]"));
    const moTabs  = Array.from(document.querySelectorAll(".mo-tab[data-target]"));
    const loading = document.getElementById("hub-loading");

    const allNav = [...pcItems, ...moTabs];
    if (!frame || allNav.length === 0) return;

    pcItems.forEach((el, idx) => {
      el.style.animationDelay = `${Math.min(idx * 60, 420)}ms`;
    });

    const META_PATH_BY_KEY = {
      summary: "reports/meta.json",
      daily:   "reports/meta.json",
      owned:   "reports/owned_portal/data/meta.json",
      performance: "reports/meta.json",
      pdp_priority: "reports/pdp_opportunity/meta.json",
      utm_channel: "reports/utm_channel/data/meta.json",
      product_keyword: "reports/product_keyword/data/meta.json",
      purchase_pattern: "reports/purchase_pattern/data/summary.json",
      price:   "reports/meta.json",
      hero:    "reports/meta.json",
      competitor: "reports/meta.json",
      competitor_content: "reports/competitor_content/latest.json",
      voc:     "reports/meta.json",
      crema:   "reports/voc_crema/data/meta.json",

      funnel:  "reports/meta.json",
      member:  "reports/member_funnel/data/meta.json",
      ai_agent: "reports/ai_agent/data/meta.json",
    };

    const showLoading = (on) => {
      if (!loading) return;
      loading.style.display = on ? "flex" : "none";
    };

    function toAbsHref(relativePath) {
      try { return new URL(relativePath, window.location.href).href; }
      catch (e) { return relativePath; }
    }

    function setActiveByTarget(target) {
      allNav.forEach(el => el.classList.remove("active"));
      const matches = allNav.filter(el => el.getAttribute("data-target") === target);
      matches.forEach(el => el.classList.add("active"));
    }

    function setActiveByKey(key) {
      allNav.forEach(el => el.classList.remove("active"));
      const matches = allNav.filter(el => (el.getAttribute("data-key") || "") === key);
      matches.forEach(el => el.classList.add("active"));
    }

    function getKeyByTarget(target) {
      const el = allNav.find(x => x.getAttribute("data-target") === target);
      return el ? (el.getAttribute("data-key") || "") : "";
    }

    function pad2(n){ return String(n).padStart(2, "0"); }
    function fmtKST(d){
      const utc = d.getTime() + (d.getTimezoneOffset() * 60000);
      const kst = new Date(utc + (9 * 60 * 60000));
      return `${kst.getFullYear()}.${pad2(kst.getMonth()+1)}.${pad2(kst.getDate())} ${pad2(kst.getHours())}:${pad2(kst.getMinutes())} KST`;
    }
    function fmtD(d){
      return `${d.getFullYear()}.${pad2(d.getMonth()+1)}.${pad2(d.getDate())}`;
    }
    function fmtYMD(d){
      return `${d.getFullYear()}-${pad2(d.getMonth()+1)}-${pad2(d.getDate())}`;
    }

    function kstNowDate(){
      const now = new Date();
      const utc = now.getTime() + (now.getTimezoneOffset() * 60000);
      return new Date(utc + (9 * 60 * 60000));
    }

    async function headExists(relativePath) {
      try {
        const res = await fetch(toAbsHref(relativePath) + `?t=${Date.now()}`, { method: "HEAD", cache: "no-store" });
        return res.ok;
      } catch (e) {
        return false;
      }
    }

    async function resolveLatestDailyDigestTarget(maxScanDays = 30) {
      const base = kstNowDate();
      for (let offset = 1; offset <= maxScanDays; offset += 1) {
        const cand = new Date(base);
        cand.setDate(cand.getDate() - offset);
        const target = `reports/daily_digest/daily/${fmtYMD(cand)}.html`;
        if (await headExists(target)) return target;
      }
      return "reports/daily_digest/index.html";
    }

    async function setSidebarMetaByKey(key) {
      const elUpdated = document.getElementById("sb-updated-at");
      const elPeriod  = document.getElementById("sb-period");
      if (!elUpdated || !elPeriod) return;

      const metaPath = META_PATH_BY_KEY[key] || "reports/meta.json";
      try {
        const res = await fetch(metaPath + `?t=${Date.now()}`, { cache: "no-store" });
        if (!res.ok) throw new Error("meta.json not found");
        const meta = await res.json();
        elUpdated.textContent = meta.updated_at_kst || meta.generated_at_kst || meta.generated_at || (key === "competitor_content" ? meta.checked_at_kst : null) || "-";
        elPeriod.textContent  = meta.period_text || meta.period_label || meta.date_range || (key === "competitor_content" ? "최근 수집 / 30일 관측" : "-");
      } catch (e) {
        // Never substitute the current time for a missing source update timestamp.
        elUpdated.textContent = "메타데이터 미확보";
        elPeriod.textContent = "기준 기간 확인 필요";
      }
    }

    function resizeIframeToContent() {
      // The mobile edge-to-edge controller is the sole iframe height owner on narrow screens.
      if (window.matchMedia && window.matchMedia("(max-width: 1023px)").matches) return;
      try {
        const doc = frame.contentDocument || frame.contentWindow.document;
        if (!doc) return;

        doc.documentElement.style.overflow = "hidden";
        doc.body.style.overflow = "hidden";
        doc.body.style.margin = "0";

        const contentPage = window.location.hash === "#competitor_content" && doc.getElementById("contentPage");
        const h = contentPage ? Math.ceil(contentPage.getBoundingClientRect().height) : Math.max(
          doc.body.scrollHeight,
          doc.documentElement.scrollHeight,
          doc.body.offsetHeight,
          doc.documentElement.offsetHeight
        );

        frame.style.height = (h + 12) + "px";
      } catch (e) {
        console.warn("iframe resize blocked", e);
      }
    }


    function activatePerformanceInline(host) {
      if (!host || host.dataset.ready === "1") return;
      const template = document.getElementById("performanceTemplate");
      if (!template) return;
      host.appendChild(template.content.cloneNode(true));
      host.dataset.ready = "1";

      const scripts = Array.from(host.querySelectorAll("script"));
      scripts.forEach(oldScript => {
        if ((oldScript.type || "").toLowerCase() === "application/json") return;
        const script = document.createElement("script");
        script.textContent = (oldScript.textContent || "")
          .replaceAll("fetch(stem+'.json.gz", "fetch('reports/'+stem+'.json.gz")
          .replaceAll("fetch(stem+'.json?v=", "fetch('reports/'+stem+'.json?v=");
        oldScript.replaceWith(script);
      });
      setTimeout(() => setupPerformanceExtras(host), 0);
    }

    function setupPerformanceExtras(host) {
      if (!host || host.dataset.extras === "1") return;
      host.dataset.extras = "1";
      const q = s => host.querySelector(s);
      let initialData = null, fullPromise = null, currentRows = [];
      const addDays = (s,n) => { const d=new Date(s+"T00:00:00"); d.setDate(d.getDate()+n); return d.toISOString().slice(0,10); };
      const addMonths = (s,n) => { const d=new Date(s+"T00:00:00"); d.setMonth(d.getMonth()+n); return d.toISOString().slice(0,10); };
      const dates = (s,e) => { const a=[]; for(let d=s;d<=e;d=addDays(d,1))a.push(d); return a; };
      const num = v => Math.round(Number(v)||0).toLocaleString();
      const won = v => "₩"+num(v); const ratio=v=>(Number(v)||0).toFixed(2);
      const pct = (a,b) => b ? (a/b-1)*100 : null;
      const pctText = v => v==null ? "YoY -" : "YoY "+(v>=0?"+":"")+v.toFixed(1)+"%";
      const esc = s => String(s).replace(/[&<>"']/g,ch=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[ch]));
      const loadJson = async stem => {
        if("DecompressionStream" in window){
          try{const r=await fetch("reports/"+stem+".json.gz?v=20260915-pvly",{cache:"no-store"});if(r.ok){const stream=r.body.pipeThrough(new DecompressionStream("gzip"));return JSON.parse(await new Response(stream).text());}}catch(e){}
        }
        const r=await fetch("reports/"+stem+".json?v=20260915-pvly",{cache:"no-store"});if(!r.ok)throw new Error("data load failed");return r.json();
      };
      const loadFull = () => fullPromise || (fullPromise=loadJson("performance_data"));
      const path = (arr,max) => arr.map((v,i)=>{const x=58+(arr.length<=1?0:i/(arr.length-1)*686),y=16+190-Math.max(0,Number(v)||0)/max*190;return (i?"L ":"M ")+x.toFixed(1)+" "+y.toFixed(1);}).join("");
      const chartHtml = (a,b,s,e,labels) => {
        const max=Math.max(1,...a,...b);
        const grid=[0,.25,.5,.75,1].map(fr=>{const y=206-190*fr;return '<line class="trend-grid" x1="58" y1="'+y+'" x2="744" y2="'+y+'"/><text class="trend-axis" x="51" y="'+(y+3)+'" text-anchor="end">'+Math.round(max*fr)+(labels.percent?"%":"")+'</text>';}).join("");
        return '<div class="legend"><span><i class="dot"></i>'+labels.a+'</span><span><i class="dot ly"></i>'+labels.b+'</span></div><div class="drag-hint">드래그/호버</div><svg class="trend-svg" viewBox="0 0 760 238" preserveAspectRatio="none">'+grid+'<path class="trend-ty" d="'+path(a,max)+'"/><path class="trend-ly" d="'+path(b,max)+'"/><text class="trend-date" x="58" y="230">'+s.slice(5)+'</text><text class="trend-date" x="744" y="230" text-anchor="end">'+e.slice(5)+'</text></svg><div class="hover-line"></div><div class="chart-tip"></div>';
      };
      const bindTip = (box,a,b,ds,labels,formatter) => {
        const tip=box.querySelector(".chart-tip"),line=box.querySelector(".hover-line");
        const show=ev=>{const r=box.getBoundingClientRect(),x=Math.max(0,Math.min(r.width,ev.clientX-r.left)),i=Math.max(0,Math.min(ds.length-1,Math.round(x/r.width*(ds.length-1))));line.style.display="block";line.style.left=x+"px";tip.style.display="block";tip.style.left=Math.max(6,Math.min(r.width-175,x+10))+"px";tip.style.top="34px";tip.innerHTML='<div class="d">'+ds[i]+'</div><div class="ty">'+labels.a+" "+formatter(a[i])+'</div><div class="ly">'+labels.b+" "+formatter(b[i])+'</div>';};
        box.addEventListener("pointermove",show,{passive:true});box.addEventListener("pointerdown",show);box.addEventListener("pointerleave",()=>{line.style.display="none";tip.style.display="none";});
      };
      const aggregate=rows=>rows.reduce((a,r)=>{for(const k of ["sessions","users","page_views","purchases","revenue"])a[k]+=Number(r[k])||0;return a;},{sessions:0,users:0,page_views:0,purchases:0,revenue:0});
      const topFromFull=(data,s,e,metric)=>{const ty=(data.ty||[]).filter(r=>r.date>=s&&r.date<=e),ly=(data.ly||[]).filter(r=>r.date>=s&&r.date<=e),tm=new Map(),lm=new Map(),blank=()=>({sessions:0,users:0,page_views:0,purchases:0,revenue:0}),add=(m,r)=>{const x=m.get(r.sm)||blank();for(const k of ["sessions","users","page_views","purchases","revenue"])x[k]+=Number(r[k])||0;m.set(r.sm,x)};ty.forEach(r=>add(tm,r));ly.forEach(r=>add(lm,r));const ds=dates(s,e),vals=(rows,sm)=>{const m=new Map();rows.filter(r=>r.sm===sm).forEach(r=>{const x=m.get(r.date)||blank();for(const k of ["sessions","users","page_views","purchases","revenue"])x[k]+=Number(r[k])||0;m.set(r.date,x)});return ds.map(d=>{const x=m.get(d)||blank();return metric==="pv_per_user"?(x.users?x.page_views/x.users:0):Number(x[metric])||0})};return {totals:{ty:aggregate(ty),ly:aggregate(ly)},ads:(data.ads||[]).filter(r=>r.date>=s&&r.date<=e),items:[...tm.entries()].sort((x,y)=>y[1].revenue-x[1].revenue).slice(0,20).map(([sm,x])=>({sm,ty:x,ly:lm.get(sm)||blank(),a:vals(ty,sm),b:vals(ly,sm)})),ds}};
      const topFromInitial=(data,s,e,metric)=>{const i0=data.dates.indexOf(s),i1=data.dates.indexOf(e),ds=data.dates.slice(i0,i1+1),sum=v=>v.reduce((x,y)=>x+(Number(y)||0),0),tot=ser=>({revenue:sum(ser.revenue.slice(i0,i1+1)),sessions:sum(ser.sessions.slice(i0,i1+1)),users:sum(ser.users.slice(i0,i1+1)),page_views:sum(ser.page_views.slice(i0,i1+1)),purchases:sum(ser.purchases.slice(i0,i1+1))}),series=(x,k)=>metric==="pv_per_user"?x.series[k].page_views.slice(i0,i1+1).map((v,j)=>{const u=Number(x.series[k].users[i0+j])||0;return u?(Number(v)||0)/u:0}):x.series[k][metric].slice(i0,i1+1),items=data.top.map(x=>({sm:x.sm,ty:tot(x.series.ty),ly:tot(x.series.ly),a:series(x,"ty"),b:series(x,"ly")})).sort((x,y)=>y.ty.revenue-x.ty.revenue).slice(0,20),total=k=>items.reduce((z,x)=>{for(const n of ["revenue","sessions","users","page_views","purchases"])z[n]+=Number(x[k][n])||0;return z},{revenue:0,sessions:0,users:0,page_views:0,purchases:0});return {totals:{ty:total("ty"),ly:total("ly")},ads:(data.ads||[]).filter(r=>r.date>=s&&r.date<=e),items,ds}};
      const renderPaid=ads=>{const calc=rows=>rows.reduce((z,r)=>{z.spend+=Number(r.spend)||0;z.ga+=Number(r.report_ga_revenue)||0;z.media+=Number(r.platform_revenue)||0;return z},{spend:0,ga:0,media:0}),sum=calc(ads.filter(r=>(r.period||"ty")==="ty")),groups=new Map();q("#totalSpend").textContent=won(sum.spend);q("#gaRoas").textContent=sum.spend?Math.round(sum.ga/sum.spend*100)+"%":"-";q("#mediaRoas").textContent=sum.spend?Math.round(sum.media/sum.spend*100)+"%":"-";ads.forEach(r=>{const x=groups.get(r.channel)||{ty:[],ly:[]};x[r.period==="ly"?"ly":"ty"].push(r);groups.set(r.channel,x)});q("#paidGrid").innerHTML=[...groups.entries()].map(([n,x])=>[n,calc(x.ty),calc(x.ly)]).sort((x,y)=>y[1].spend-x[1].spend).map(([n,x,y])=>'<div class="paid-card"><h3>'+esc(n)+'</h3><div class="paid-row"><span>Ad Spend TY / LY</span><b>'+won(x.spend)+' / '+won(y.spend)+'</b></div><div class="paid-row"><span>GA ROAS TY / LY</span><b>'+(x.spend?Math.round(x.ga/x.spend*100):0)+'% / '+(y.spend?Math.round(y.ga/y.spend*100):0)+'%</b></div><div class="paid-row"><span>Media ROAS TY / LY</span><b>'+(x.spend?Math.round(x.media/x.spend*100):0)+'% / '+(y.spend?Math.round(y.media/y.spend*100):0)+'%</b></div></div>').join("")};
      const renderRoas=(ads,ds)=>{const build=p=>{const m=new Map();ads.filter(r=>(r.period||"ty")===p).forEach(r=>{const x=m.get(r.date)||{spend:0,ga:0};x.spend+=Number(r.spend)||0;x.ga+=Number(r.report_ga_revenue)||0;m.set(r.date,x)});return ds.map(d=>{const x=m.get(d);return x&&x.spend?x.ga/x.spend*100:0})},ty=build("ty"),ly=build("ly"),box=q("#paidRoasChart");box.innerHTML=chartHtml(ty,ly,ds[0],ds.at(-1),{a:"GA ROAS TY",b:"GA ROAS LY",percent:true});bindTip(box,ty,ly,ds,{a:"GA ROAS TY",b:"GA ROAS LY"},v=>Math.round(v||0)+"%")};
      const channelFor = sm => {
        const x=String(sm||"").toLowerCase();
        if(x.includes("naverbs"))return "Naver Brand Search";
        if(x.includes("adwords")||x.includes("google / cpc")||x.includes("googleads"))return "Google Ads";
        if(x.includes("meta")||x.includes("facebook")||x.includes("instagram"))return "Meta";
        if(x.includes("criteo"))return "Criteo";
        if(x.includes("kakaomoment")||x.includes("kakao / banner"))return "Kakao Moment";
        if(x.includes("naver_gfa")||x.includes("gfa"))return "Naver GFA";
        if((x.includes("naver")||x.includes("shopping"))&&x.includes("cpc"))return "Naver Search & Shopping";
        return null;
      };
      const logoFor = sm => {
        const x=String(sm||"").toLowerCase();let cls="other",txt="•";
        if(x.includes("naver")){cls="naver";txt="N"}else if(x.includes("google")||x.includes("adwords")){cls="google";txt="G"}else if(x.includes("meta")||x.includes("facebook")||x.includes("instagram")){cls="meta";txt="∞"}else if(x.includes("kakao")){cls="kakao";txt="K"}else if(x.includes("criteo")){cls="criteo";txt="C"}else if(x.includes("lms")||x.includes("edm")){cls="message";txt="M"}else if(x.includes("organic")){cls="organic";txt="O"}else if(x.includes("direct")){cls="direct";txt="D"}
        return '<span class="sm-logo '+cls+'" aria-hidden="true">'+txt+'</span>';
      };
      const roasSeries=(ads,ds,channel)=>{const build=p=>{const m=new Map();(ads||[]).filter(r=>r.channel===channel&&(r.period||"ty")===p).forEach(r=>{const x=m.get(r.date)||{spend:0,ga:0};x.spend+=Number(r.spend)||0;x.ga+=Number(r.report_ga_revenue)||0;m.set(r.date,x)});return ds.map(d=>{const x=m.get(d);return x&&x.spend?x.ga/x.spend*100:0})};return {ty:build("ty"),ly:build("ly")}};
      const render = async () => {
        const s=q("#startDate").value,e=q("#endDate").value,metric=q("#metric").value;
        const useInitial=initialData&&s>=initialData.start&&e<=initialData.end;
        if(!useInitial)q("#periodText").textContent="6개월 그래프 준비 중...";
        const baseMetric=(metric==="roas"||metric==="pv_per_user")?"revenue":metric;
        const data=useInitial?topFromInitial(initialData,s,e,baseMetric):topFromFull(await loadFull(),s,e,baseMetric);
        if(metric==="roas"){data.items.forEach(x=>{x.adChannel=channelFor(x.sm);if(x.adChannel){const rs=roasSeries(data.ads,data.ds,x.adChannel);x.a=rs.ty;x.b=rs.ly;}});}
        currentRows=data.items;
        const t=data.totals.ty,l=data.totals.ly,tc=t.sessions?t.purchases/t.sessions:0,lc=l.sessions?l.purchases/l.sessions:0;
        q("#totalRevenue").textContent=won(t.revenue);q("#totalSessions").textContent=num(t.sessions);q("#totalPurchases").textContent=num(t.purchases);q("#totalPvPerUser").textContent=ratio(t.users?t.page_views/t.users:0);q("#pvPerUserYoy").textContent=pctText(pct(t.users?t.page_views/t.users:0,l.users?l.page_views/l.users:0));q("#totalCvr").textContent=(tc*100).toFixed(2)+"%";
        q("#revYoy").textContent=pctText(pct(t.revenue,l.revenue));q("#sesYoy").textContent=pctText(pct(t.sessions,l.sessions));q("#purYoy").textContent=pctText(pct(t.purchases,l.purchases));q("#cvrYoy").textContent="YoY "+((tc-lc)*100>=0?"+":"")+((tc-lc)*100).toFixed(2)+"pp";
        q("#periodText").textContent="선택 기간: "+s+" ~ "+e+" · "+(metric==="roas"?"ROAS":metric==="pv_per_user"?"PV/인":metric[0].toUpperCase()+metric.slice(1))+" · Top 20";
        renderPaid(data.ads);renderRoas(data.ads,data.ds);
        const grid=q("#grid"),pager=q("#pager"),size=10,pages=Math.ceil(data.items.length/size);
        const draw=page=>{grid.innerHTML="";data.items.slice(page*size,(page+1)*size).forEach((x,j)=>{const rank=page*size+j+1,cvr=x.ty.sessions?x.ty.purchases/x.ty.sessions:0,sec=document.createElement("section");sec.className="panel source-panel";const isRoas=metric==="roas",chart=isRoas&&!x.adChannel?'<div class="no-roas">광고비 매핑 없음</div>':'<div class="chartbox">'+chartHtml(x.a,x.b,s,e,isRoas?{a:"GA ROAS TY",b:"GA ROAS LY",percent:true}:{a:"TY",b:"LY",percent:false})+'</div>';sec.innerHTML='<div class="head"><div><div class="rank">#'+rank+'</div><h2 class="sm-title">'+logoFor(x.sm)+'<span>'+esc(x.sm)+'</span></h2></div><div class="mini">Revenue '+won(x.ty.revenue)+' · '+pctText(pct(x.ty.revenue,x.ly.revenue))+(isRoas&&x.adChannel?'<br>'+esc(x.adChannel):'')+'</div></div><div class="stats"><span>Sessions <b>'+num(x.ty.sessions)+'</b></span><span>Users <b>'+num(x.ty.users)+'</b></span><span>PV <b>'+num(x.ty.page_views)+'</b></span><span>PV/인 <b>'+ratio(x.ty.users?x.ty.page_views/x.ty.users:0)+'</b></span><span>Purchases <b>'+num(x.ty.purchases)+'</b></span><span>CVR <b>'+(cvr*100).toFixed(2)+'%</b></span></div>'+chart;grid.appendChild(sec);if(!isRoas||x.adChannel){const box=sec.querySelector(".chartbox");bindTip(box,x.a,x.b,data.ds,isRoas?{a:"GA ROAS",b:"Media ROAS"}:{a:"TY",b:"LY"},v=>isRoas?Math.round(v||0)+"%":metric==="revenue"?won(v):metric==="pv_per_user"?ratio(v):num(v));}});pager.innerHTML="";for(let p=0;p<pages;p++){const b=document.createElement("button");b.textContent=p+1;if(p===page)b.className="active";b.onclick=()=>draw(p);pager.appendChild(b);}};
        draw(0);
      };
      const exportExcel = async () => {
        const s=q("#startDate").value,e=q("#endDate").value,metric=q("#metric").value;
        const table='<table><tr><th>Rank</th><th>Source / Medium</th><th>Revenue</th><th>Sessions</th><th>Purchases</th><th>CVR</th><th>Selected Metric</th></tr>'+currentRows.map((x,i)=>'<tr><td>'+(i+1)+'</td><td>'+esc(x.sm)+'</td><td>'+x.ty.revenue+'</td><td>'+x.ty.sessions+'</td><td>'+x.ty.purchases+'</td><td>'+(x.ty.sessions?x.ty.purchases/x.ty.sessions:0)+'</td><td>'+metric+'</td></tr>').join("")+'</table>';
        const content="\ufeff<html><head><meta charset='utf-8'></head><body><h2>Performance "+s+" ~ "+e+"</h2>"+table+"</body></html>",a=document.createElement("a");a.href="data:application/vnd.ms-excel;charset=utf-8,"+encodeURIComponent(content);a.download="performance_"+s+"_"+e+".xls";document.body.appendChild(a);a.click();a.remove();
      };
      const start = async () => {
        initialData=await loadJson("performance_initial");
        const latestEnd=initialData.end, latestStart=addDays(addMonths(latestEnd,-3),1);
        q("#endDate").value=latestEnd;q("#endDate").max=latestEnd;
        q("#startDate").value=latestStart;q("#startDate").max=latestEnd;
        q("#exportExcelBtn").onclick=exportExcel;
        q("#applyBtn").onclick=render;
        q("#metric").onchange=()=>{q("#graphMetric").value=q("#metric").value;render();};q("#graphMetric").onchange=()=>{q("#metric").value=q("#graphMetric").value;render();};
        host.querySelectorAll(".preset button").forEach(b=>b.onclick=()=>{host.querySelectorAll(".preset button").forEach(x=>x.classList.remove("active"));b.classList.add("active");const e=initialData.end;let s=b.dataset.days?addDays(e,-30):addDays(addMonths(e,-Number(b.dataset.months)),1);q("#startDate").value=s;q("#endDate").value=e;render();});
        render();
      };
      start().catch(e=>{q("#periodText").textContent="데이터를 표시할 수 없습니다.";console.error(e);});
    }

    async function navigate(target) {
      if (!target) return;
      const key = getKeyByTarget(target);
      let resolvedTarget = target;

      if (key === "daily" && target === "reports/daily_digest/index.html") {
        resolvedTarget = await resolveLatestDailyDigestTarget();
      }

      const performanceInline = document.getElementById("performanceInline");
      const performanceTemplate = document.getElementById("performanceTemplate");
      if (key === "performance") {
        showLoading(false);
        frame.style.display = "none";
        if (performanceInline && performanceTemplate) {
          activatePerformanceInline(performanceInline);
          performanceInline.style.display = "block";
        }
        setActiveByKey(key);
        window.location.hash = key;
        window.CSKUnifiedMenus?.activate(key);
        setSidebarMetaByKey(key);
        return;
      }
      if (performanceInline) performanceInline.style.display = "none";
      frame.style.display = "block";
      showLoading(true);

      const onLoadOnce = () => {
        showLoading(false);
        frame.removeEventListener("load", onLoadOnce);

        if (key === "performance") {
          frame.style.height = "1180px";
        } else {
          resizeIframeToContent();
          setTimeout(resizeIframeToContent, 250);
          setTimeout(resizeIframeToContent, 900);
        }
      };
      frame.addEventListener("load", onLoadOnce);

      frame.src = toAbsHref(resolvedTarget);
      if (key) setActiveByKey(key);
      else setActiveByTarget(target);

      if (key) { window.location.hash = key; window.CSKUnifiedMenus?.activate(key); }
      setSidebarMetaByKey(key);
    }

    allNav.forEach(el => {
      el.addEventListener("click", () => navigate(el.getAttribute("data-target")));
      if(el.classList.contains("content-intelligence-nav")) el.addEventListener("keydown", ev=>{if(ev.key==="Enter"||ev.key===" "){ev.preventDefault();navigate(el.getAttribute("data-target"));}});
    });

    function initialNavigate() {
      const hashKey = (window.location.hash || "").replace("#", "").trim();
      if (hashKey) {
        const el = allNav.find(x => x.getAttribute("data-key") === hashKey);
        if (el) { navigate(el.getAttribute("data-target")); return; }
      }
      navigate(frame.getAttribute("src"));
    }
    initialNavigate();

    window.addEventListener("message", (event) => {
      try {
        const data = event && event.data ? event.data : null;
        // Accept height hints only from the active, same-origin report.
        if (!data || event.source !== frame.contentWindow || event.origin !== window.location.origin) return;
        if (data.type === "contentMonitorResize") {
          if (event.origin !== window.location.origin || event.source !== frame.contentWindow || window.location.hash !== "#competitor_content") return;
          const nextHeight=Number(data.height);
          if(Number.isFinite(nextHeight)&&nextHeight>200) frame.style.height=(Math.ceil(nextHeight)+12)+"px";
          return;
        }
        if (!data || data.type !== "dailyDigestResize") return;
        const nextHeight = Number(data.height || 0);
        const currentKey = (window.location.hash || "").replace("#", "");
        if (currentKey !== "performance" && !(window.matchMedia && window.matchMedia("(max-width: 1023px)").matches) && Number.isFinite(nextHeight) && nextHeight > 200) {
          frame.style.height = (nextHeight + 16) + "px";
          setTimeout(resizeIframeToContent, 80);
        }
      } catch (e) {
        console.warn("iframe message resize blocked", e);
      }
    });

    window.addEventListener("resize", () => {
      resizeIframeToContent();
      setTimeout(resizeIframeToContent, 120);
    });
  })();
