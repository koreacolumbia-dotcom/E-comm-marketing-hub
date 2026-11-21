/* Resize same-origin CSK reports to their content on mobile; no nested report scrollbar. */
(function () {
  'use strict';
  var frame = document.getElementById('frame');
  if (!frame || !window.matchMedia) return;
  var mobile = window.matchMedia('(max-width: 1023px)');
  var observer = null;
  var attachedDoc = null;
  var imageListener = null;
  var childResizeListener = null;
  var timers = [];
  var cssUrl = new URL('assets/mobile-edge-to-edge.css?v=20261122-1', document.baseURI).href;

  function cancelObservers() {
    if (observer) observer.disconnect();
    observer = null;
    if (attachedDoc && imageListener) attachedDoc.removeEventListener('load', imageListener, true);
    if (attachedDoc && childResizeListener) {
      try { attachedDoc.defaultView.removeEventListener('resize', childResizeListener); } catch (_) {}
    }
    timers.forEach(clearTimeout);
    timers = [];
    attachedDoc = null;
    imageListener = null;
    childResizeListener = null;
  }

  function wrapTables(doc) {
    doc.querySelectorAll('table').forEach(function (table) {
      if (table.closest('.csk-mobile-scroll-table, .csk-mobile-table, .table-responsive, .table-wrap, .table-scroll')) return;
      var wrapper = doc.createElement('div');
      wrapper.className = 'csk-mobile-scroll-table';
      table.parentNode.insertBefore(wrapper, table);
      wrapper.appendChild(table);
    });
  }

  function fitFrame(doc) {
    if (!mobile.matches || attachedDoc !== doc || doc !== frame.contentDocument || !doc.body) return;
    var body = doc.body;
    var html = doc.documentElement;
    var measured = Math.max(
      body.scrollHeight || 0,
      body.offsetHeight || 0,
      html.scrollHeight || 0,
      html.offsetHeight || 0
    );
    var height = Math.max(320, Math.ceil(measured));
    var current = parseFloat(frame.style.height) || 0;
    if (Math.abs(current - height) > 8) frame.style.height = height + 'px';
  }

  function scheduleFit(doc, ms) {
    timers.push(setTimeout(function () { fitFrame(doc); }, ms));
  }

  function install() {
    cancelObservers();
    if (!mobile.matches || frame.style.display === 'none') return;
    var doc;
    try { doc = frame.contentDocument; } catch (_) { return; }
    if (!doc || !doc.body || !doc.head) return;
    attachedDoc = doc;
    if (!doc.querySelector('meta[name="viewport"]')) {
      var viewport = doc.createElement('meta');
      viewport.name = 'viewport';
      viewport.content = 'width=device-width, initial-scale=1';
      doc.head.appendChild(viewport);
    }
    doc.documentElement.classList.add('csk-embedded-mobile');
    doc.body.classList.add('csk-embedded-mobile');

    if (!doc.getElementById('csk-mobile-edge-to-edge')) {
      var css = doc.createElement('link');
      css.id = 'csk-mobile-edge-to-edge';
      css.rel = 'stylesheet';
      css.href = cssUrl;
      css.addEventListener('load', function () { fitFrame(doc); }, { once: true });
      doc.head.appendChild(css);
    }
    wrapTables(doc);
    imageListener = function (event) {
      if (event.target && /^(IMG|IFRAME)$/.test(event.target.tagName || '')) fitFrame(doc);
    };
    doc.addEventListener('load', imageListener, true);
    childResizeListener = function () { fitFrame(doc); };
    doc.defaultView.addEventListener('resize', childResizeListener);
    if ('ResizeObserver' in window) {
      observer = new ResizeObserver(function () { fitFrame(doc); });
      observer.observe(doc.body);
      observer.observe(doc.documentElement);
    }
    // Content dashboards render asynchronously after the iframe load event.
    scheduleFit(doc, 80);
    scheduleFit(doc, 350);
    scheduleFit(doc, 1200);
    scheduleFit(doc, 3500);
  }

  frame.addEventListener('load', function () {
    install();
    if (mobile.matches) window.scrollTo({ top: 0, behavior: 'auto' });
  });
  function onViewportChange() {
    if (mobile.matches) install();
    else cancelObservers();
  }
  if (mobile.addEventListener) mobile.addEventListener('change', onViewportChange);
  else if (mobile.addListener) mobile.addListener(onViewportChange);
  window.addEventListener('orientationchange', function () { scheduleFit(attachedDoc, 250); });
  window.addEventListener('pageshow', function () { if (mobile.matches) install(); });
  if (mobile.matches && frame.contentDocument && frame.contentDocument.readyState === 'complete') install();
})();
