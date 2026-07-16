(()=>{
  const q=(s,r=document)=>r.querySelector(s), qa=(s,r=document)=>Array.from(r.querySelectorAll(s));
  const icon=(el)=>q('i',el)?.className||'fa-solid fa-circle';
  const label=(el)=>el.dataset.label||q('span',el)?.textContent?.trim()||el.textContent.trim();
  const target=(el)=>el.dataset.target||'';
  const ready=()=>{
    document.documentElement.classList.add('csk-redesign');
    const isHub=!!q('#frame')&&qa('.nav-item[data-target]').length>0;
    document.body.classList.add(isHub?'csk-hub-page':'csk-report-page');
    const flatten=document.createElement('style');
    flatten.textContent=`
      body.csk-hub-page>main>section{padding:0!important;margin:0!important;background:transparent!important;border:0!important;box-shadow:none!important;border-radius:0!important;backdrop-filter:none!important}
      body.csk-hub-page #frame{display:block!important;width:100%!important;border:0!important;border-radius:0!important;background:transparent!important;box-shadow:none!important}
      body.csk-report-page>#dashboard-experience.dx-shell,#dashboard-experience.dx-shell{max-width:1500px!important;margin:0 auto 16px!important;padding:0!important;background:transparent!important;border:0!important;border-radius:0!important;box-shadow:none!important;backdrop-filter:none!important}
      body.csk-report-page>#dashboard-ops.ops-shell,#dashboard-ops.ops-shell{max-width:1500px!important;margin:0 auto 16px!important;padding:0!important;background:transparent!important;border:0!important;border-radius:0!important;box-shadow:none!important;backdrop-filter:none!important}
      body.csk-report-page>main.glass-card,body.csk-report-page>.glass-card:first-child{background:transparent!important;border:0!important;box-shadow:none!important;backdrop-filter:none!important}
      @media(max-width:1023px){
        body.csk-hub-page>main{padding:0!important;margin:0!important}
        body.csk-hub-page>main>section{padding:0!important;margin:0!important}
        body.csk-report-page{padding-left:0!important;padding-right:0!important}
        body.csk-report-page>#dashboard-experience.dx-shell,#dashboard-experience.dx-shell,body.csk-report-page>#dashboard-ops.ops-shell,#dashboard-ops.ops-shell{margin:0 0 12px!important;padding:0 12px!important}
        #dashboard-experience .dx-head,#dashboard-ops .ops-head{border-radius:16px!important}
        #dashboard-experience .dx-panel{border-radius:16px!important}
      }
    `;
    document.head.appendChild(flatten);
    qa('table').forEach(t=>{if(t.parentElement?.classList.contains('csk-mobile-table'))return;const w=document.createElement('div');w.className='csk-mobile-table';t.parentNode.insertBefore(w,t);w.appendChild(t)});
    qa('img').forEach(i=>{i.loading='lazy';i.decoding='async'});qa('a[target="_blank"]').forEach(a=>a.rel='noopener noreferrer');
    const top=document.createElement('button');top.type='button';top.className='csk-backtop';top.setAttribute('aria-label','맨 위로');top.innerHTML='↑';top.onclick=()=>window.scrollTo({top:0,behavior:'smooth'});document.body.appendChild(top);const syncTop=()=>top.classList.toggle('show',window.scrollY>480);addEventListener('scroll',syncTop,{passive:true});syncTop();
    if(!isHub)return;
    const frame=q('#frame'), pcItems=qa('.nav-item[data-target]'), oldMobile=qa('.mo-tabbar');oldMobile.forEach(x=>x.style.display='none');
    const groups=[];qa('.sidebar .nav-section').forEach(sec=>{const title=q('.nav-section-title',sec)?.textContent.trim()||'Menu';const items=qa('.nav-item[data-target]',sec);if(items.length)groups.push({title,items})});
    const navigate=(el)=>{const path=target(el);if(!path)return;frame.src=path;pcItems.forEach(x=>x.classList.toggle('active',target(x)===path));qa('.csk-drawer-item,.csk-mobile-nav button').forEach(x=>x.classList.toggle('active',x.dataset.target===path));const name=label(el);const mobileTitle=q('.csk-mobile-title strong');if(mobileTitle)mobileTitle.textContent=name;localStorage.setItem('csk-last-target',path);closeDrawer()};
    pcItems.forEach(el=>el.addEventListener('click',()=>navigate(el)));
    const brandLogo=q('.sidebar img')?.getAttribute('src')||'Columbia_logo.png';
    const header=document.createElement('header');header.className='csk-mobile-header';header.innerHTML=`<div class="csk-mobile-brand"><img src="${brandLogo}" alt="Columbia"><div class="csk-mobile-title"><strong>Summary</strong><span>E-COMM MARKETING HUB</span></div></div><div class="csk-mobile-actions"><button class="csk-icon-btn csk-search-btn" aria-label="메뉴 검색"><i class="fa-solid fa-magnifying-glass"></i></button><button class="csk-icon-btn csk-theme-btn" aria-label="다크모드"><i class="fa-solid fa-moon"></i></button><button class="csk-icon-btn csk-menu-btn" aria-label="전체 메뉴"><i class="fa-solid fa-bars"></i></button></div>`;document.body.appendChild(header);
    const scrim=document.createElement('div');scrim.className='csk-scrim';document.body.appendChild(scrim);
    const drawer=document.createElement('aside');drawer.className='csk-drawer';drawer.innerHTML=`<div class="csk-drawer-head"><div class="csk-drawer-brand"><img src="${brandLogo}" alt="Columbia"><div><strong>CSK E-COMM</strong><span>ALL REPORTS</span></div></div><button class="csk-icon-btn csk-close-btn"><i class="fa-solid fa-xmark"></i></button></div><div class="csk-drawer-search"><input type="search" placeholder="리포트 검색"></div><div class="csk-drawer-list"></div>`;document.body.appendChild(drawer);
    const list=q('.csk-drawer-list',drawer);groups.forEach(g=>{const h=document.createElement('div');h.className='csk-drawer-section';h.textContent=g.title;list.appendChild(h);g.items.forEach(el=>{const b=document.createElement('button');b.className='csk-drawer-item';b.dataset.target=target(el);b.dataset.label=label(el);b.innerHTML=`<i class="${icon(el)}"></i><span>${label(el)}</span>`;b.onclick=()=>navigate(el);list.appendChild(b)})});
    const openDrawer=()=>{drawer.classList.add('open');scrim.classList.add('open')},closeDrawer=()=>{drawer.classList.remove('open');scrim.classList.remove('open')};q('.csk-menu-btn',header).onclick=openDrawer;q('.csk-close-btn',drawer).onclick=closeDrawer;scrim.onclick=closeDrawer;
    q('.csk-drawer-search input',drawer).addEventListener('input',e=>{const term=e.target.value.toLowerCase();qa('.csk-drawer-item',drawer).forEach(x=>x.hidden=!x.dataset.label.toLowerCase().includes(term));qa('.csk-drawer-section',drawer).forEach(h=>{let n=h.nextElementSibling,visible=false;while(n&&!n.classList.contains('csk-drawer-section')){if(!n.hidden)visible=true;n=n.nextElementSibling}h.hidden=!visible})});
    const bottom=document.createElement('nav');bottom.className='csk-mobile-nav';const picks=[['summary','홈','fa-house'],['daily','데일리','fa-calendar-day'],['product_keyword','상품','fa-chart-column'],['hub_v2','V2','fa-wand-magic-sparkles']];picks.forEach(([key,name,ic])=>{const source=pcItems.find(x=>x.dataset.key===key);if(!source)return;const b=document.createElement('button');b.dataset.target=target(source);b.innerHTML=`<i class="fa-solid ${ic}"></i><span>${name}</span>`;b.onclick=()=>navigate(source);bottom.appendChild(b)});const more=document.createElement('button');more.innerHTML='<i class="fa-solid fa-bars"></i><span>전체</span>';more.onclick=openDrawer;bottom.appendChild(more);document.body.appendChild(bottom);
    const command=document.createElement('div');command.className='csk-command';command.innerHTML='<div class="csk-command-box"><input type="search" placeholder="리포트 이름을 입력하세요"><div class="csk-command-results"></div></div>';document.body.appendChild(command);const results=q('.csk-command-results',command),cmdInput=q('input',command);const renderResults=(term='')=>{results.innerHTML='';pcItems.filter(x=>label(x).toLowerCase().includes(term.toLowerCase())).forEach(el=>{const b=document.createElement('button');b.className='csk-command-item';b.innerHTML=`<i class="${icon(el)}"></i><span>${label(el)}</span>`;b.onclick=()=>{navigate(el);command.classList.remove('open')};results.appendChild(b)})};const openCommand=()=>{renderResults();command.classList.add('open');setTimeout(()=>cmdInput.focus(),30)};q('.csk-search-btn',header).onclick=openCommand;cmdInput.oninput=e=>renderResults(e.target.value);command.onclick=e=>{if(e.target===command)command.classList.remove('open')};addEventListener('keydown',e=>{if((e.ctrlKey||e.metaKey)&&e.key.toLowerCase()==='k'){e.preventDefault();openCommand()}if(e.key==='Escape'){command.classList.remove('open');closeDrawer()}});
    const setTheme=dark=>{document.documentElement.classList.toggle('csk-dark',dark);localStorage.setItem('csk-theme',dark?'dark':'light');q('.csk-theme-btn i',header).className=`fa-solid ${dark?'fa-sun':'fa-moon'}`};setTheme(localStorage.getItem('csk-theme')==='dark');q('.csk-theme-btn',header).onclick=()=>setTheme(!document.documentElement.classList.contains('csk-dark'));
    const initial=localStorage.getItem('csk-last-target');if(initial){const el=pcItems.find(x=>target(x)===initial);if(el)navigate(el)}else{const active=pcItems.find(x=>x.classList.contains('active'))||pcItems[0];if(active){q('.csk-mobile-title strong').textContent=label(active);qa('[data-target]').forEach(x=>x.classList.toggle('active',x.dataset.target===target(active)))}}
    frame.addEventListener('load',()=>{const active=pcItems.find(x=>target(x)===frame.getAttribute('src'));if(active)q('.csk-mobile-title strong').textContent=label(active)});
  };
  document.readyState==='loading'?document.addEventListener('DOMContentLoaded',ready):ready();
})();