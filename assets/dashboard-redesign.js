(()=>{
  const q=(s,r=document)=>r.querySelector(s),qa=(s,r=document)=>Array.from(r.querySelectorAll(s));
  const icon=el=>q('i',el)?.className||'fa-solid fa-circle';
  const label=el=>el.dataset.label||q('span',el)?.textContent?.trim()||el.textContent.trim();
  const target=el=>el.dataset.target||'';

  const normalizeReportPage=()=>{
    document.documentElement.classList.add('csk-redesign');
    qa('table').forEach(t=>{if(t.parentElement?.classList.contains('csk-mobile-table'))return;const w=document.createElement('div');w.className='csk-mobile-table';t.parentNode?.insertBefore(w,t);w.appendChild(t)});
    qa('img').forEach(i=>{i.loading='lazy';i.decoding='async';i.style.maxWidth='100%';i.style.height='auto'});
    qa('a[target="_blank"]').forEach(a=>a.rel='noopener noreferrer');
    qa('iframe').forEach(f=>{f.style.maxWidth='100%';f.setAttribute('loading','lazy')});
    qa('pre,code').forEach(el=>el.classList.add('csk-code-scroll'));
    qa('select,input,button').forEach(el=>el.classList.add('csk-touch-target'));
    qa('div,section,main').forEach(el=>{if(el.scrollWidth>document.documentElement.clientWidth+20)el.classList.add('csk-responsive-layout')});
    const top=document.createElement('button');top.type='button';top.className='csk-backtop';top.setAttribute('aria-label','맨 위로');top.innerHTML='↑';top.onclick=()=>window.scrollTo({top:0,behavior:'smooth'});document.body.appendChild(top);const syncTop=()=>top.classList.toggle('show',window.scrollY>480);addEventListener('scroll',syncTop,{passive:true});syncTop();
  };

  const ready=()=>{
    normalizeReportPage();
    const isHub=!!q('#frame')&&qa('.nav-item[data-target]').length>0;
    document.body.classList.add(isHub?'csk-hub-page':'csk-report-page');
    const flatten=document.createElement('style');flatten.textContent=`
      body.csk-hub-page>main>section{padding:0!important;margin:0!important;background:transparent!important;border:0!important;box-shadow:none!important;border-radius:0!important;backdrop-filter:none!important}
      body.csk-hub-page #frame{display:block!important;width:100%!important;border:0!important;border-radius:0!important;background:transparent!important;box-shadow:none!important}
      body.csk-report-page>#dashboard-experience.dx-shell,#dashboard-experience.dx-shell,body.csk-report-page>#dashboard-ops.ops-shell,#dashboard-ops.ops-shell{max-width:1500px!important;margin:0 auto 16px!important;padding:0!important;background:transparent!important;border:0!important;border-radius:0!important;box-shadow:none!important;backdrop-filter:none!important}
      body.csk-report-page>main.glass-card,body.csk-report-page>.glass-card:first-child{background:transparent!important;border:0!important;box-shadow:none!important;backdrop-filter:none!important}
      .csk-frame-status{display:none;position:absolute;inset:0;z-index:20;align-items:center;justify-content:center;flex-direction:column;gap:8px;background:#f5f7fb;color:#64748b;text-align:center;padding:24px}.csk-frame-status.show{display:flex}.csk-frame-status strong{color:#172033}.csk-frame-spinner{width:30px;height:30px;border:3px solid #dbe4ee;border-top-color:#0874e8;border-radius:50%;animation:cskspin .8s linear infinite}.csk-frame-status.error .csk-frame-spinner{display:none}@keyframes cskspin{to{transform:rotate(360deg)}}
      @media(max-width:1023px){body.csk-hub-page>main,body.csk-hub-page>main>section{padding:0!important;margin:0!important}body.csk-report-page{padding-left:0!important;padding-right:0!important}body.csk-report-page>#dashboard-experience.dx-shell,#dashboard-experience.dx-shell,body.csk-report-page>#dashboard-ops.ops-shell,#dashboard-ops.ops-shell{margin:0 0 12px!important;padding:0 10px!important}.csk-responsive-layout{max-width:100%!important;min-width:0!important;overflow-x:auto!important}.csk-touch-target{min-height:42px!important}.csk-code-scroll{display:block;max-width:100%;overflow-x:auto}.csk-report-page [style*="grid-template-columns"]{grid-template-columns:minmax(0,1fr)!important}.csk-report-page [class*="grid-cols-"]{grid-template-columns:repeat(1,minmax(0,1fr))!important}.csk-report-page .report-shell,.csk-report-page .dashboard,.csk-report-page .container,.csk-report-page main{width:100%!important;max-width:100%!important}.csk-report-page .sticky{position:static!important}}
    `;document.head.appendChild(flatten);
    // Responsive styling only: all menu and routing behavior is in csk-unified-hub.js.
  };
  document.readyState==='loading'?document.addEventListener('DOMContentLoaded',ready):ready();
})();
