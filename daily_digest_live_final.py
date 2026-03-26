#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Patch daily_digest_live_final.py so render_hub_index() auto-loads latest report
while keeping Compare / YoY controls.
"""

from __future__ import annotations

import argparse
import pathlib
import re
import sys

REPLACEMENT = r'''
def render_hub_index(dates: List[dt.date]) -> str:
    dates = sorted(dates)
    if not dates:
        dates = [dt.datetime.now(ZoneInfo("Asia/Seoul")).date() - dt.timedelta(days=1)]

    latest = dates[-1].strftime("%Y-%m-%d")
    dates_json = json.dumps([d.strftime("%Y-%m-%d") for d in dates], ensure_ascii=False)

    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Daily Digest Hub</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200;400;600;800&display=swap');
    :root {{ --brand:#002d72; --bg0:#f6f8fb; --bg1:#eef3f9; }}

    html, body {{ height: 100%; overflow: auto; }}
    body{{
      background: linear-gradient(180deg, var(--bg0), var(--bg1));
      font-family: 'Plus Jakarta Sans', 'Noto Sans KR', sans-serif;
      color:#0f172a;
      min-height:100vh;
    }}
    .glass{{
      background: rgba(255,255,255,0.72);
      backdrop-filter: blur(18px);
      border: 1px solid rgba(255,255,255,0.85);
      border-radius: 26px;
      box-shadow: 0 24px 60px rgba(0,45,114,0.07);
    }}
    .chip{{
      border: 1px solid rgba(148,163,184,0.35);
      background: rgba(255,255,255,0.78);
      border-radius: 999px;
      padding: 8px 12px;
      font-weight: 900;
      font-size: 12px;
      color: #0f172a;
      transition: all .15s ease;
      user-select:none;
      white-space: nowrap;
    }}
    .chip:hover{{ transform: translateY(-1px); box-shadow: 0 10px 24px rgba(0,45,114,0.08); }}
    .chip.active{{
      background: rgba(0,45,114,0.08);
      border-color: rgba(0,45,114,0.28);
      color: var(--brand);
    }}
    .btn{{
      border-radius: 14px;
      padding: 10px 14px;
      font-weight: 900;
      font-size: 12px;
      border: 1px solid rgba(148,163,184,0.28);
      background: rgba(255,255,255,0.88);
      transition: all .15s ease;
      user-select:none;
      white-space: nowrap;
      display:inline-flex;
      align-items:center;
      gap:8px;
    }}
    .btn:hover{{ transform: translateY(-1px); box-shadow: 0 10px 24px rgba(0,45,114,0.08); }}
    .btn-primary{{ background: #002d72; border-color: #002d72; color: white; }}
    .btn:disabled, .chip:disabled{{
      opacity: .55;
      cursor: not-allowed;
      transform:none !important;
      box-shadow:none !important;
    }}
    .muted{{ color:#64748b; }}
    .small-label{{
      font-size: 10px;
      font-weight: 900;
      letter-spacing: .22em;
      text-transform: uppercase;
      color: #94a3b8;
    }}
    input[type="date"]{{
      border: 1px solid rgba(148,163,184,0.28);
      background: rgba(255,255,255,0.90);
      border-radius: 14px;
      padding: 10px 12px;
      font-weight: 900;
      font-size: 12px;
      color: #0f172a;
      outline: none;
      width: 100%;
    }}
    input[type="date"]:focus{{
      border-color: rgba(0,45,114,0.40);
      box-shadow: 0 0 0 4px rgba(0,45,114,0.08);
    }}
    .viewer-frame{{
      width: 100%;
      border: 0;
      border-radius: 18px;
      background: transparent;
      overflow: hidden;
      display:block;
      height: 3200px;
    }}
    .loading-backdrop{{
      position: fixed;
      inset: 0;
      background: rgba(15, 23, 42, 0.18);
      backdrop-filter: blur(2px);
      display: none;
      align-items: center;
      justify-content: center;
      z-index: 9999;
    }}
    .loading-card{{
      background: rgba(255,255,255,0.92);
      border: 1px solid rgba(148,163,184,0.22);
      border-radius: 18px;
      padding: 14px 16px;
      box-shadow: 0 18px 48px rgba(0,0,0,0.12);
      display:flex;
      align-items:center;
      gap:10px;
      font-weight:900;
      color:#0f172a;
    }}
    .spinner{{
      width: 18px;
      height: 18px;
      border-radius: 999px;
      border: 3px solid rgba(2,45,114,0.18);
      border-top-color: rgba(2,45,114,0.95);
      animation: spin 0.8s linear infinite;
    }}
    @keyframes spin {{ to {{ transform: rotate(360deg); }} }}

    .btn .btn-spin{{
      width: 14px;
      height: 14px;
      border-radius: 999px;
      border: 2px solid rgba(255,255,255,0.45);
      border-top-color: rgba(255,255,255,0.95);
      animation: spin 0.8s linear infinite;
      display:none;
    }}
    .btn.loading .btn-spin{{ display:inline-block; }}
    .toolbar-row{{
      overflow-x:auto;
      -webkit-overflow-scrolling: touch;
      padding-bottom: 2px;
    }}
    .toolbar-row::-webkit-scrollbar{{ height: 8px; }}
    .toolbar-row::-webkit-scrollbar-thumb{{ background: rgba(148,163,184,0.35); border-radius: 999px; }}
  </style>
</head>

<body class="p-6 md:p-10">
  <div class="max-w-7xl mx-auto">
    <div class="flex flex-col md:flex-row md:items-end md:justify-between gap-4 mb-6">
      <div>
        <div class="text-4xl font-black tracking-tight">Daily Digest Hub</div>
        <div class="muted font-semibold mt-1">기본: 최신 리포트 자동 탐색(기본 어제 KST) · 비교 ON이면 A/B 각각 리포트 표시</div>
      </div>

      <div class="flex items-center gap-3 flex-wrap justify-end">
        <div class="flex items-center gap-2">
          <div class="small-label mr-2">Mode</div>
          <button id="modeDaily" class="chip active" type="button">Daily</button>
          <button id="modeWeekly" class="chip" type="button">Weekly (7D)</button>
        </div>
        <button id="btnReload" class="btn btn-primary" type="button"><span class="btn-spin"></span>새로고침</button>
      </div>
    </div>

    <div class="glass p-5">
      <div class="toolbar-row flex items-center gap-2 mb-3 whitespace-nowrap">
        <div class="small-label">Date</div>
        <div class="w-[160px]"><input id="aDate" type="date" /></div>
        <button id="btnPrev" class="btn" type="button">◀</button>
        <button id="btnNext" class="btn" type="button">▶</button>
        <button id="btnYesterday" class="btn" type="button">어제</button>
        <button id="btnToday" class="btn" type="button">오늘(있으면)</button>

        <div class="ml-2 small-label">Compare</div>
        <button id="btnCompareToggle" class="chip" type="button">비교 OFF</button>
        <button id="btnPresetPrev" class="btn" type="button">전기준</button>
        <button id="btnPresetYoY" class="btn" type="button">YoY</button>
        <div class="w-[160px]"><input id="bDate" type="date" /></div>
        <button id="btnCompareGo" class="btn btn-primary" type="button"><span class="btn-spin"></span>비교하기</button>
      </div>

      <div id="viewerGrid" class="grid grid-cols-1 gap-4">
        <div>
          <div id="viewerATitle" class="text-sm font-black text-slate-700 mb-2">A</div>
          <iframe id="viewerA" class="viewer-frame" loading="eager"></iframe>
        </div>
        <div id="viewerBWrap" class="hidden">
          <div id="viewerBTitle" class="text-sm font-black text-slate-700 mb-2">B</div>
          <iframe id="viewerB" class="viewer-frame" loading="eager"></iframe>
        </div>
      </div>
    </div>
  </div>

  <div id="loading" class="loading-backdrop">
    <div class="loading-card">
      <div class="spinner"></div>
      <div id="loadingText">로딩 중…</div>
    </div>
  </div>

<script>
(() => {{
  const AVAILABLE_DATES = {dates_json};
  const DEFAULT_DATE = {json.dumps(latest, ensure_ascii=False)};
  const BASE = './';

  function fmtYMD(d){{
    const y = d.getFullYear();
    const m = String(d.getMonth()+1).padStart(2,'0');
    const day = String(d.getDate()).padStart(2,'0');
    return `${{y}}-${{m}}-${{day}}`;
  }}
  function parseYMD(s){{
    const [y,m,d] = (s||'').split('-').map(Number);
    return new Date(y, (m||1)-1, d||1);
  }}
  function addDays(d, n){{
    const x = new Date(d);
    x.setDate(x.getDate()+n);
    return x;
  }}
  function isRecentDateStr(dateStr){{
    const d = parseYMD(dateStr);
    const today = new Date();
    const diff = Math.floor((today - d) / 86400000);
    return diff <= 3;
  }}

  const modeDaily = document.getElementById('modeDaily');
  const modeWeekly = document.getElementById('modeWeekly');
  const btnReload = document.getElementById('btnReload');
  const aDate = document.getElementById('aDate');
  const bDate = document.getElementById('bDate');
  const btnPrev = document.getElementById('btnPrev');
  const btnNext = document.getElementById('btnNext');
  const btnYesterday = document.getElementById('btnYesterday');
  const btnToday = document.getElementById('btnToday');
  const btnCompareToggle = document.getElementById('btnCompareToggle');
  const btnPresetPrev = document.getElementById('btnPresetPrev');
  const btnPresetYoY = document.getElementById('btnPresetYoY');
  const btnCompareGo = document.getElementById('btnCompareGo');
  const viewerA = document.getElementById('viewerA');
  const viewerB = document.getElementById('viewerB');
  const viewerBWrap = document.getElementById('viewerBWrap');
  const viewerGrid = document.getElementById('viewerGrid');
  const viewerATitle = document.getElementById('viewerATitle');
  const viewerBTitle = document.getElementById('viewerBTitle');
  const loading = document.getElementById('loading');
  const loadingText = document.getElementById('loadingText');

  let MODE = 'daily';
  let COMPARE = false;

  function stepDays(){{ return (MODE === 'weekly') ? 7 : 1; }}
  function buildReportPath(dateStr){{
    const base = (MODE === 'daily')
      ? `${{BASE}}daily/${{dateStr}}.html`
      : `${{BASE}}weekly/END_${{dateStr}}.html`;
    return base + `?embed=1`;
  }}
  function buildReportBase(dateStr){{
    return (MODE === 'daily')
      ? `${{BASE}}daily/${{dateStr}}.html`
      : `${{BASE}}weekly/END_${{dateStr}}.html`;
  }}
  function buildCachePath(dateStr){{
    return (MODE === 'daily')
      ? `${{BASE}}data/daily/${{dateStr}}.json`
      : `${{BASE}}data/weekly/END_${{dateStr}}.json`;
  }}

  const ACTION_BTNS = [btnReload, btnCompareGo];
  const MODE_BTNS = [modeDaily, modeWeekly, btnCompareToggle, btnPresetPrev, btnPresetYoY, btnPrev, btnNext, btnYesterday, btnToday];

  function setBusy(on, msg){{
    if(on){{
      loadingText.textContent = msg || '로딩 중…';
      loading.style.display = 'flex';
      for(const b of ACTION_BTNS){{ b.disabled = true; b.classList.add('loading'); }}
      for(const b of MODE_BTNS){{ b.disabled = true; }}
    }}else{{
      loading.style.display = 'none';
      for(const b of ACTION_BTNS){{ b.disabled = false; b.classList.remove('loading'); }}
      for(const b of MODE_BTNS){{ b.disabled = false; }}
    }}
  }}

  const LS_KEY = 'ddhub_exists_cache_v10';
  function cacheGet(key){{
    try{{
      const raw = localStorage.getItem(LS_KEY);
      const box = raw ? JSON.parse(raw) : {{}};
      const item = box[key];
      if(!item) return null;
      if(Date.now() > item.exp) return null;
      return item.ok;
    }}catch(e){{ return null; }}
  }}
  function cacheSet(key, ok){{
    try{{
      const raw = localStorage.getItem(LS_KEY);
      const box = raw ? JSON.parse(raw) : {{}};
      box[key] = {{ ok: !!ok, exp: Date.now() + 10 * 60 * 1000 }};
      localStorage.setItem(LS_KEY, JSON.stringify(box));
    }}catch(e){{}}
  }}

  async function existsReport(dateStr){{
    const base = buildReportBase(dateStr);
    const cached = cacheGet(base);
    if(cached !== null){{
      if(cached === false && isRecentDateStr(dateStr)){{
      }}else{{
        return cached;
      }}
    }}
    try{{
      const res = await fetch(base + `?t=${{Date.now()}}`, {{ method:'HEAD', cache:'no-store' }});
      cacheSet(base, res.ok);
      return res.ok;
    }}catch(e){{
      cacheSet(base, false);
      return false;
    }}
  }}

  function resizeFrameToContent(frame){{
    try{{
      const doc = frame.contentDocument || frame.contentWindow.document;
      if(!doc) return;
      let style = doc.getElementById('hub_scrub_css');
      if(!style){{
        style = doc.createElement('style');
        style.id = 'hub_scrub_css';
        style.type = 'text/css';
        style.textContent = `
          html, body {{ overflow: visible !important; height: auto !important; }}
          body {{ margin:0 !important; min-height: auto !important; }}
        `;
        doc.head.appendChild(style);
      }}
      const h = Math.max(
        doc.body ? doc.body.scrollHeight : 0,
        doc.documentElement ? doc.documentElement.scrollHeight : 0
      );
      if(h && isFinite(h)){{ frame.style.height = (h + 12) + 'px'; }}
    }}catch(e){{}}
  }}
  function onFrameLoad(frame){{
    resizeFrameToContent(frame);
    let n = 0;
    const t = setInterval(() => {{
      resizeFrameToContent(frame);
      n += 1;
      if(n >= 10) clearInterval(t);
    }}, 250);
  }}
  viewerA.addEventListener('load', () => {{ onFrameLoad(viewerA); setBusy(false); }});
  viewerB.addEventListener('load', () => {{ onFrameLoad(viewerB); setBusy(false); }});

  async function loadA(dateStr){{
    viewerA.src = buildReportPath(dateStr);
    viewerATitle.textContent = `A: ${{MODE.toUpperCase()}} · ${{dateStr}}`;
  }}
  async function loadB(dateStr){{
    viewerB.src = buildReportPath(dateStr);
    viewerBTitle.textContent = `B: ${{MODE.toUpperCase()}} · ${{dateStr}}`;
  }}

  async function yoyEndFor(dateStr){{
    try{{
      const res = await fetch(buildCachePath(dateStr) + `?t=${{Date.now()}}`, {{ cache:'no-store' }});
      if(!res.ok) throw new Error('no cache');
      const j = await res.json();
      const y = j && j.yoy && j.yoy.end ? String(j.yoy.end) : '';
      if(!y) throw new Error('no yoy.end');
      return y;
    }}catch(e){{
      return fmtYMD(addDays(parseYMD(dateStr), -364));
    }}
  }}

  function updateCompareLayout(){{
    if(COMPARE){{
      viewerBWrap.classList.remove('hidden');
      viewerGrid.className = 'grid grid-cols-1 lg:grid-cols-2 gap-4';
    }}else{{
      viewerBWrap.classList.add('hidden');
      viewerGrid.className = 'grid grid-cols-1 gap-4';
    }}
  }}
  function setCompare(on){{
    COMPARE = !!on;
    btnCompareToggle.classList.toggle('active', COMPARE);
    btnCompareToggle.textContent = COMPARE ? '비교 ON' : '비교 OFF';
    updateCompareLayout();
  }}

  async function resolveLatestAvailableDate(preferredDateStr, maxScanDays){{
    let d0 = parseYMD(preferredDateStr);
    for(let i=0;i<=maxScanDays;i++){{
      const cand = fmtYMD(addDays(d0, -i));
      if(await existsReport(cand)) return cand;
    }}
    return preferredDateStr;
  }}

  async function applyA(){{
    const d = (aDate.value||'').trim();
    if(!d) return;
    setBusy(true, 'A 리포트 로딩…');
    let target = d;
    if(!await existsReport(target)){{
      target = await resolveLatestAvailableDate(target, 14);
      aDate.value = target;
    }}
    await loadA(target);
  }}
  async function applyB(){{
    if(!COMPARE) return;
    const d = (bDate.value||'').trim();
    if(!d) return;
    setBusy(true, 'B 리포트 로딩…');
    let target = d;
    if(!await existsReport(target)){{
      target = await resolveLatestAvailableDate(target, 370);
      bDate.value = target;
    }}
    await loadB(target);
  }}
  async function applyBoth(){{
    await applyA();
    if(COMPARE) await applyB();
  }}

  async function setMode(mode){{
    MODE = (mode === 'weekly') ? 'weekly' : 'daily';
    modeDaily.classList.toggle('active', MODE === 'daily');
    modeWeekly.classList.toggle('active', MODE === 'weekly');
    await applyBoth();
  }}

  btnReload.addEventListener('click', () => applyBoth());
  modeDaily.addEventListener('click', () => setMode('daily'));
  modeWeekly.addEventListener('click', () => setMode('weekly'));
  btnCompareToggle.addEventListener('click', async () => {{
    setCompare(!COMPARE);
    if(COMPARE){{
      if(!(bDate.value||'').trim()){{ bDate.value = await yoyEndFor(aDate.value || DEFAULT_DATE); }}
      await applyB();
    }}
  }});
  btnPresetPrev.addEventListener('click', async () => {{
    const step = stepDays();
    bDate.value = fmtYMD(addDays(parseYMD(aDate.value || DEFAULT_DATE), -step));
    if(!COMPARE) setCompare(true);
    await applyB();
  }});
  btnPresetYoY.addEventListener('click', async () => {{
    bDate.value = await yoyEndFor(aDate.value || DEFAULT_DATE);
    if(!COMPARE) setCompare(true);
    await applyB();
  }});
  btnCompareGo.addEventListener('click', async () => {{
    if(!COMPARE) setCompare(true);
    await applyB();
  }});
  btnPrev.addEventListener('click', async () => {{
    aDate.value = fmtYMD(addDays(parseYMD(aDate.value || DEFAULT_DATE), -stepDays()));
    await applyA();
  }});
  btnNext.addEventListener('click', async () => {{
    aDate.value = fmtYMD(addDays(parseYMD(aDate.value || DEFAULT_DATE), stepDays()));
    await applyA();
  }});
  btnYesterday.addEventListener('click', async () => {{
    aDate.value = DEFAULT_DATE;
    await applyA();
  }});
  btnToday.addEventListener('click', async () => {{
    aDate.value = fmtYMD(new Date());
    await applyA();
  }});
  aDate.addEventListener('change', () => applyA());
  bDate.addEventListener('change', async () => {{ if(COMPARE) await applyB(); }});

  async function init(){{
    const initial = await resolveLatestAvailableDate(DEFAULT_DATE, 14);
    aDate.value = initial;
    bDate.value = await yoyEndFor(initial);
    setCompare(false);
    await setMode('daily');
  }}
  init();
}})();
</script>
</body>
</html>
"""
'''


def patch_text(src: str) -> str:
    pattern = re.compile(
        r"def render_hub_index\(dates: List\[dt\.date\]\) -> str:\n.*?(?=\n# =========================\n# Main|\ndef parse_args\()",
        re.DOTALL,
    )
    if not pattern.search(src):
        raise RuntimeError("render_hub_index() block not found")
    return pattern.sub(REPLACEMENT.rstrip() + "\n\n", src, count=1)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("target", help="Path to daily_digest_live_final.py")
    ap.add_argument("--output", help="Optional output path. Default: overwrite target")
    args = ap.parse_args()

    target = pathlib.Path(args.target)
    if not target.exists():
      print(f"[ERROR] File not found: {target}", file=sys.stderr)
      return 1

    src = target.read_text(encoding="utf-8")
    patched = patch_text(src)

    out = pathlib.Path(args.output) if args.output else target
    out.write_text(patched, encoding="utf-8")
    print(f"[OK] patched: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
