/* CSK SaaS Design System — shared across hub menus, except daily reports.
   This layer only injects CSS. All report HTML, datasets, charts and controls remain owned
   by their original reports. The original daily pages are deliberately excluded. */
(function () {
  "use strict";
  const frame = document.getElementById("frame");
  if (!frame || !document.getElementById("hub-menu-root")) return;
  const assetHref = new URL("assets/csk-saas-unified.css?v=20261122-3",document.baseURI).href;
  const SKIP = new Set(["daily","winter_festival"]);
  const routeByPath = new Map();
  const groups = window.CSKUnifiedMenus && window.CSKUnifiedMenus.groups || [];
  groups.forEach(g => (g[4] || []).forEach(route => {
    try { routeByPath.set(new URL(route[2],document.baseURI).pathname,route[0]); } catch (_) {}
  }));
  const dailyPath = pathname =>
    /\/reports\/daily_digest\/(?:index\.html|daily\/[^/]+\.html)$/.test(pathname) ||
    false;
  const cssId = "csk-saas-shared-theme";
  const layoutCssHref=new URL("assets/csk-layout-v2.css?v=20261122-weatherfocus-v1",document.baseURI).href;
  const layoutJsHref=new URL("assets/csk-layout-v2.js?v=20261122-18a",document.baseURI).href;
  function ensureLayout(doc) {
    if(!doc.getElementById("csk-layout-v2-css")){
      const css=doc.createElement("link");
      css.id="csk-layout-v2-css";css.rel="stylesheet";css.href=layoutCssHref;
      doc.head.appendChild(css);
    }
    if(!doc.getElementById("csk-layout-v2-script")){
      const js=doc.createElement("script");
      js.id="csk-layout-v2-script";js.src=layoutJsHref;js.defer=true;
      doc.body.appendChild(js);
    }
  }

  function routeFor(doc) {
    const pathname = doc.location && doc.location.pathname || "";
    if (dailyPath(pathname)) return "daily";
    const fromPath = routeByPath.get(pathname);
    if (fromPath) return fromPath;
    // The member/campaign dashboards can have query parameters, so resolve by
    // pathname only. A fragment may reference the NEXT page before its load.
    return null;
  }

  function decorate() {
    let doc;
    try {
      doc = frame.contentDocument;
      if (!doc || !doc.documentElement || !doc.body || doc.location.origin !== window.location.origin) return;
    } catch (_) { return; } // no cross-origin styling or DOM access
    const route = routeFor(doc);
    if (!route || SKIP.has(route)) return;
    if (doc.body.classList.contains("csk-saas-report") && doc.getElementById(cssId)) { ensureLayout(doc); return; }
    doc.body.classList.add("csk-saas-report");
    doc.body.dataset.cskRoute = route;
    // This page already uses the exact same palette and layout; don't undo its
    // dedicated React styling with broad legacy selectors.
    doc.body.classList.toggle("csk-saas-existing",route==="decision_health");
    if (!doc.getElementById(cssId)) {
      const link = doc.createElement("link");
      link.id=cssId;
      link.rel="stylesheet";
      link.href=assetHref;
      doc.head.appendChild(link);
    }
    ensureLayout(doc);
  }
  document.body.classList.add("csk-saas-hub");
  // Hub controller controls the URL. Every load is isolated, preventing styles
  // bleeding from one report to the next.
  frame.addEventListener("load",decorate);
  if (frame.contentDocument && frame.contentDocument.readyState==="complete") decorate();
  window.addEventListener("pageshow",decorate);
  window.CSKSaaSTheme = Object.freeze({
    excluded:["daily"],
    version:"20261122-18a"
  });
})();
