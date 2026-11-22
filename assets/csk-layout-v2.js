/* CSK Unified Layout v2 — shared structural redesign for all non-daily reports.
 * Keep each report's data ownership and original DOM/listeners.
 * Only change presentation, region geometry, chart palette and report navigation.
 */
(function () {
 "use strict";
 const NAME="csk-layout-v2";
 const daily=/\/reports\/daily_digest\/(?:index\.html|daily\/[^/]+\.html)$/i;
 if(daily.test(location.pathname))return;
 const ALLOWED=new Set(["decision_health","daily_intelligence","summary","performance","pdp_priority","product_keyword",
   "utm_channel","funnel","purchase_pattern","member","owned","edm_intelligence",
   "crm_intelligence","sale_calendar","competitor_content","competitor","price","hero",
   "ops3","ai_agent","voc","crema"]);
 const cfg={
  daily_intelligence:{shell:"main",kpi:".tiles",chart:".trend,.spark,.barrow",header:"header",full:"#product-focus,#sections"},
  summary:{shell:"body > div:first-of-type",kpi:".summary-kpi-grid",chart:".summary-chart,.trend-svg,.chart-container,canvas,svg[role='img']",header:"header.hero-shell",full:".summary-section"},
  performance:{shell:".hubmain > .wrap",kpi:".cards",chart:".chartbox,.trend-svg,.source-panel,.trend-grid",header:"h1",full:".toolbar,.source-panel"},
  pdp_priority:{shell:"main.shell",kpi:"#kpis",chart:"#trendGrid,.trend-grid,.trend-panel,canvas",header:"header.hero",full:".trend-panel"},
  product_keyword:{shell:"main,.wrap,.container",kpi:".kpis,.metrics,.stats",chart:".chart,.chartbox,canvas",header:"header,h1"},
  utm_channel:{shell:"main,.wrap,.container",kpi:".kpis,.metrics,.stats",chart:".chart,.chartbox,canvas",header:"header,h1"},
  funnel:{shell:".dashboard-shell",kpi:".kpis,.metrics,.stats,.summary-grid",chart:".funnel,.chart,.chart-wrap,svg[role='img']",header:".glass:first-child",full:".glass:first-child"},
  purchase_pattern:{shell:"main,.wrap,.container",kpi:".kpis,.metrics,.stats",chart:".chart,.chartbox,canvas",header:"header,h1"},
  member:{shell:"main,.wrap,.container",kpi:".kpis,.metrics,.stats",chart:".chart,.chartbox,canvas",header:"header,h1"},
  owned:{shell:"body > div:first-of-type",kpi:".kpi-grid,.kpi-wrap,.kpis",chart:".chart,.chart-card,.chartbox,.trend-svg,svg[role='img']",header:"body > div > div:first-child",full:".report-fullwidth"},
  edm_intelligence:{shell:"body > main",kpi:".kpis",chart:".chart,#counts,#trend,#mix",header:"main > header",full:"#gallery,#tracking"},
  crm_intelligence:{shell:"body > main",kpi:".metrics",chart:".journey,.chart,.bar-chart",header:"main > header",full:".panel.grid2"},
  sale_calendar:{shell:"body > main",kpi:".stats",chart:"#calendar,.calendar-scroll,.intel-chart,.timeline,.intel-table-wrap",header:"main > header",full:".calendar-panel,.intel-view,.coverage-panel"},
  competitor_content:{shell:"main#contentPage",kpi:"#snapshot,#kpis",chart:".chart,.timeline,.bar-chart,.gap-columns",header:"main > header",full:"#gapRadar,#archive,#advancedPanel,.deep-section"},
  competitor:{shell:"main,.wrap,.container",kpi:".kpis,.metrics,.stats",chart:".chart,.chartbox,canvas",header:"header,h1"},
  price:{shell:"main,.wrap,.container",kpi:".kpis,.metrics,.stats",chart:".chart,.chartbox,canvas",header:"header,h1"},
  hero:{shell:"body > main",kpi:".kpis,.metrics,.stats",chart:".heatmap,.chart,.chartbox,.keyword-cloud",header:"main > header",full:".heatmap,.keyword-cloud"},
  ops3:{shell:"body > main",kpi:".kpis",chart:".chart,.trend,.stack",header:"main > header",full:".stack,.hero"},
  ai_agent:{shell:".wrap",kpi:".kpis",chart:".chart,.bar,.table-card",header:".hero",full:".section.grid.two"},
  voc:{shell:"body > div:first-of-type",kpi:".grid.xl\\:grid-cols-4,.summary-grid",chart:".chart,.chartbox,.trend-graph,svg[role='img']",header:"section:first-child",full:".tab-panel,.detail-panel"},
  crema:{shell:"main > div:first-child",kpi:".kpis,.grid,.summary-grid",chart:".chart,.chartbox,.sparkline,svg[role='img']",header:".layout-header",full:".glass-card"}
 };
 const palette=["#0072ce","#5e54f3","#4e9cce","#8794e8","#38a59c","#af88df","#a4bdd6"];
 const categories={summary:"통합 현황",performance:"매출 · 유입",pdp_priority:"상품 · 퍼널",
   product_keyword:"상품 · 검색",utm_channel:"유입 · 매체",funnel:"구매 퍼널",purchase_pattern:"구매 분석",
   member:"회원 · 코호트",owned:"CRM · OWNED",edm_intelligence:"경쟁사 CRM",crm_intelligence:"고객 여정",
   sale_calendar:"경쟁사 프로모션",competitor_content:"경쟁사 콘텐츠",competitor:"경쟁사 상품",
   price:"가격 비교",hero:"배너 · 기획전",ops3:"마케팅 운영",ai_agent:"AI 운영",voc:"커뮤니티",crema:"리뷰 VOC"};
 let initialized=false, pending=false, observer=null;
 function visible(el){
   if(!el || el.hidden || el.closest("[hidden]"))return false;
   return true;
 }
 function shellFor(conf){
   try{return document.querySelector(conf.shell)}catch(e){return null}
 }
 function topLevel(el,root){
   if(!el||!root||!root.contains(el))return null;
   for(let x=el;x&&x!==root;x=x.parentElement){if(x.parentElement===root)return x}
   return null;
 }
 function mark(root,selector,className,max=150){
   if(!selector)return 0;
   let count=0,all;
   try{all=root.querySelectorAll(selector)}catch(e){return 0}
   for(const el of all){
     if(count>=max)break;
     if(!el.classList.contains(className))el.classList.add(className);
     count++;
   }
   return count;
 }
 function subzones(root,selector,name){
   if(!selector)return;
   let els=[];try{els=[...root.querySelectorAll(selector)]}catch(e){return}
   for(const el of els){
     if(el===root)continue;
     const top=topLevel(el,root);
     if(top)top.classList.add(name);
   }
 }
 function headings(root){
   const head=root.querySelector("h1");
   if(!head)return;
   head.classList.add("csk-v2-title");
   const direct=topLevel(head,root);
   if(direct)direct.classList.add("csk-v2-heading");
   if(!root.querySelector(".csk-v2-eyebrow")){
     const kicker=document.createElement("span");
     kicker.className="csk-v2-eyebrow";
     kicker.textContent="CSK E-COMMERCE / "+(categories[document.body.dataset.cskRoute]||"ANALYTICS");
     head.parentNode.insertBefore(kicker,head);
   }
 }
 function annotateCharts(root,conf){
   mark(root,conf.chart,"csk-v2-chart");
   mark(root,"canvas, svg[role='img'], svg[data-chart], [class*='chart-'], .chart svg, .trend-svg","csk-v2-plot",130);
   mark(root,"table","csk-v2-table",130);
   mark(root,".tabbar,.tabs,.section-nav,.intel-tabs,.view-tabs,.brandbar,.csk-filter-pills","csk-v2-tabs",45);
   mark(root,".toolbar,.controls,.filters,.filter-bar,.control-bar","csk-v2-toolbar",45);
   mark(root,".panel,.card,.glass-card,.metric-card,.kpi-card,.stat,.kpi,.glass","csk-v2-surface",220);
   mark(root,".panel-dark,.dark-workspace,.decision-workspace","csk-v2-dark-panel",25);
 }
 function gridLayout(root,key,conf){
   // Do not rewrite DOM or trigger data reload. Annotating the source's actual
   // sections preserves all chart data bindings, event handlers and drilldowns.
   root.classList.add("csk-v2-report-shell");
   headings(root);
   subzones(root,conf.kpi,"csk-v2-kpi-section");
   subzones(root,conf.full,"csk-v2-wide-section");
   subzones(root,conf.chart,"csk-v2-chart-section");
   mark(root,conf.kpi,"csk-v2-metrics");
   annotateCharts(root,conf);
   for(const item of [...root.children]){
      if(!item.classList || item.matches("script,style"))continue;
      item.classList.add("csk-v2-root-child");
      if(item.matches("header,.hero,.hero-shell,.layout-header")||item.querySelector("h1")){
         item.classList.add("csk-v2-heading");
      }
   }
   root.dataset.cskLayout="v2";
   document.body.dataset.cskLayoutVersion="2";
 }
 function standardizeSvg(root){
   // Style only recognized chart line strokes; never touch green/red warning
   // states, categorical legend semantics, icons, inline photos or arrows.
   const map={"#0055a5":"#0072ce","#0066cc":"#0072ce",
       "#2563eb":"#0072ce","#3b82f6":"#0072ce",
       "#4f46e5":"#5e54f3","#6366f1":"#5e54f3",
       "#8b5cf6":"#5e54f3","#93c5fd":"#87b5df"};
   const diagrams=root.querySelectorAll(".csk-v2-chart svg,.csk-v2-plot[role='img']");
   let n=0;
   for(const svg of diagrams){
     if(n++>100)break;
     if(svg.dataset.cskPaletteV2==="done")continue;
     const nodes=svg.querySelectorAll("polyline,path,line,circle,rect");
     for(let i=0;i<Math.min(nodes.length,750);i++){
       const el=nodes[i];
       for(const attr of ["stroke","fill"]){
         const raw=el.getAttribute(attr);
         if(!raw)continue;
         const mapped=map[raw.toLowerCase()];
         if(mapped)el.setAttribute(attr,mapped);
       }
     }
     svg.dataset.cskPaletteV2="done";
   }
 }
 function updateChartLibraries(root){
   const chart=window.Chart;
   if(chart){
     try{
       if(chart.defaults) {
         chart.defaults.color="#6b7489";
         chart.defaults.font={...(chart.defaults.font||{}),family:"Pretendard, Noto Sans KR, system-ui, sans-serif",size:12};
         if(chart.defaults.plugins&&chart.defaults.plugins.legend&&chart.defaults.plugins.legend.labels)
           chart.defaults.plugins.legend.labels.color="#65708a";
       }
       if(typeof chart.getChart==="function"){
         let n=0;
         for(const canvas of root.querySelectorAll("canvas")){
           if(n++>80)break;
           const instance=chart.getChart(canvas);
           if(!instance||!instance.data)continue;
           const options=instance.options||{};
           if(!options.plugins)options.plugins={};
           if(options.plugins.legend)options.plugins.legend.labels={
             ...(options.plugins.legend.labels||{}),color:"#65708a",usePointStyle:true
           };
           const datasets=instance.data.datasets||[];
           datasets.forEach((ds,i)=>{
             if(ds.cskPreserveColor||ds.label&&/하락|급락|위험|증가|감소|경고|error|success/i.test(ds.label))return;
             const color=palette[i%palette.length];
             if(!Array.isArray(ds.borderColor)&&typeof ds.borderColor==="string")ds.borderColor=color;
             if(!Array.isArray(ds.backgroundColor)&&typeof ds.backgroundColor==="string")ds.backgroundColor=color+"33";
           });
           instance.update("none");
         }
       }
     }catch(e){/* original data chart remains functional */}
   }
   const ec=window.echarts;
   if(ec&&typeof ec.getInstanceByDom==="function"){
     let n=0;
     for(const chartEl of root.querySelectorAll("[_echarts_instance_],.echarts")){
       if(n++>30)break;
       try{
         const instance=ec.getInstanceByDom(chartEl);
         if(!instance||instance.__cskV2)continue;
         instance.setOption({color:palette,textStyle:{fontFamily:"Pretendard, sans-serif",color:"#65708a"}});
         instance.__cskV2=true;
         instance.resize();
       }catch(e){}
     }
   }
 }
 function layout(){
   const key=document.body.dataset.cskRoute||document.currentScript&&document.currentScript.dataset.cskRoute;
   if(!ALLOWED.has(key)||daily.test(location.pathname))return;
   // Do not redraw source reports with absent/empty markup.
   const conf=cfg[key];
   if(!conf)return;
   const root=shellFor(conf);
   if(!root)return;
   if(!initialized){
      initialized=true;
      document.body.classList.add(NAME);
      gridLayout(root,key,conf);
      updateChartLibraries(root);
      standardizeSvg(root);
      // Recompute source-authored chart sizes once after the new grid is applied.
      requestAnimationFrame(function(){
        try{window.dispatchEvent(new Event("resize"))}catch(e){}
        updateChartLibraries(root);
      });
      // Dynamic cards/filters are generated from their own files. Relabel
      // structure when data changes without replacing DOM or subscribing to
      // metric state. All report-specific handlers remain live.
      observer=new MutationObserver(function(changes){
        if(!changes.some(m=>m.addedNodes.length||m.removedNodes.length))return;
        if(pending)return;
        pending=true;
        requestAnimationFrame(function(){
          pending=false;
          annotateCharts(root,conf);
          standardizeSvg(root);
        });
      });
      observer.observe(root,{subtree:true,childList:true});
      window.addEventListener("resize",function(){
        try{window.dispatchEvent(new Event("csk:layout-resized"))}catch(e){}
      },{passive:true});
   }
 }
 if(document.readyState==="loading")document.addEventListener("DOMContentLoaded",layout,{once:true});
 else layout();
 window.addEventListener("load",layout,{once:true});
})();
