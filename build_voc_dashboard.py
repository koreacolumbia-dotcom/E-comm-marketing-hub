#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_voc_dashboard.py  (FINAL | full)

✅ What it does
- Reads source JSON produced by crema_voc:
    site/data/reviews.json
    site/data/meta.json
- Writes dashboard package to:
    reports/voc_crema/
      index.html
      data/meta.json
      data/reviews.json

✅ Fixes
- Ships a known-good index.html (no JS syntax breaks)
- Default Daily Feed date = yesterday(KST) (handled in HTML)
- Rewrites local asset paths so that:
    "assets/..." -> "../../site/assets/..."
  because this dashboard lives under /reports/voc_crema/
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
from typing import Any, Dict, List

KST = timezone(timedelta(hours=9))


# ----------------------------
# HTML template (FULL)
# ----------------------------
INDEX_HTML = r"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>VOC Dashboard | Official + Naver Reviews</title>

  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">

  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200;400;600;800&display=swap');
    :root { --brand:#002d72; --bg0:#f6f8fb; --bg1:#eef3f9; }

    html, body { height: 100%; }
    body{
      background: linear-gradient(180deg, var(--bg0), var(--bg1));
      font-family: 'Plus Jakarta Sans', sans-serif;
      color:#0f172a; min-height:100vh;
    }

    .glass-card{
      background: rgba(255,255,255,0.55);
      backdrop-filter: blur(20px);
      border: 1px solid rgba(255,255,255,0.75);
      border-radius: 30px;
      box-shadow: 0 20px 50px rgba(0,45,114,0.05);
    }

    .topbar{
      background: rgba(255,255,255,0.70);
      backdrop-filter: blur(15px);
      border-bottom: 1px solid rgba(255,255,255,0.80);
    }

    .summary-card{
      border-radius: 26px; background: rgba(255,255,255,0.55);
      border: 1px solid rgba(255,255,255,0.75); backdrop-filter: blur(18px);
      box-shadow: 0 20px 50px rgba(0,45,114,0.05);
      padding: 18px 20px;
    }

    .small-label{
      font-size: 10px; letter-spacing: 0.3em; text-transform: uppercase; font-weight: 900;
    }

    .input-glass{
      background: rgba(255,255,255,0.65); border: 1px solid rgba(255,255,255,0.80);
      border-radius: 18px; padding: 12px 14px; outline: none; font-weight: 800; color:#0f172a;
    }
    .input-glass:focus{
      box-shadow: 0 0 0 4px rgba(0,45,114,0.10);
      border-color: rgba(0,45,114,0.25);
    }

    .chip{
      border-radius: 9999px; padding: 10px 14px; font-weight: 900; font-size: 12px;
      border: 1px solid rgba(255,255,255,0.85); background: rgba(255,255,255,0.60);
      color:#334155; cursor:pointer; user-select:none;
    }
    .chip.active{
      background: rgba(0,45,114,0.95); color:#fff; border-color: rgba(0,45,114,1);
      box-shadow: 0 10px 30px rgba(0,45,114,0.15);
    }

    .tab-btn{
      padding: 10px 14px; border-radius: 18px; font-weight: 900; font-size: 12px;
      border: 1px solid rgba(255,255,255,0.85); background: rgba(255,255,255,0.60);
      color:#475569; transition: all .15s ease;
    }
    .tab-btn:hover{ background: rgba(255,255,255,0.90); }
    .tab-btn.active{
      background: rgba(0,45,114,0.95); color:#fff; border-color: rgba(0,45,114,1);
      box-shadow: 0 10px 30px rgba(0,45,114,0.15);
    }

    .overlay{
      position: fixed; inset:0; background: rgba(255,255,255,0.65);
      backdrop-filter: blur(10px);
      display:none; align-items:center; justify-content:center; z-index:9999;
    }
    .overlay.show{ display:flex; }
    .spinner{
      width:56px;height:56px;border-radius:9999px; border:6px solid rgba(0,0,0,0.08);
      border-top-color: rgba(0,45,114,0.95); animation: spin .9s linear infinite;
    }
    @keyframes spin { to { transform: rotate(360deg);} }
    @keyframes floaty { 0%,100% { transform: translate(-50%,-50%) scale(1) } 50% { transform: translate(-50%,-55%) scale(1) } }

    .tbl{
      width:100%; border-collapse: separate; border-spacing: 0; overflow:hidden; border-radius: 22px;
      border: 1px solid rgba(255,255,255,0.85); background: rgba(255,255,255,0.55);
    }
    .tbl th{
      font-size: 11px; letter-spacing: .22em; text-transform: uppercase; font-weight: 900;
      color:#475569; background: rgba(255,255,255,0.75); padding: 14px 14px;
      position: sticky; top: 0; z-index: 1;
    }
    .tbl td{
      padding: 14px 14px; border-top: 1px solid rgba(255,255,255,0.75); font-weight: 800;
      color:#0f172a; font-size: 13px; vertical-align: top;
    }
    .tbl .muted{ color:#64748b; font-weight:800; font-size:12px; }

    .review-card{
      border-radius: 26px; background: rgba(255,255,255,0.55);
      border: 1px solid rgba(255,255,255,0.80); backdrop-filter: blur(18px);
      box-shadow: 0 16px 42px rgba(0,45,114,0.04); padding: 18px 18px;
    }

    .badge{
      display:inline-flex; align-items:center; gap:6px; padding: 6px 10px; border-radius: 9999px;
      font-size: 11px; font-weight: 900; border: 1px solid rgba(255,255,255,0.85);
      background: rgba(255,255,255,0.65); color:#334155;
    }
    .badge.neg{ background: rgba(239,68,68,0.10); color:#b91c1c; border-color: rgba(239,68,68,0.18); }
    .badge.pos{ background: rgba(16,185,129,0.10); color:#047857; border-color: rgba(16,185,129,0.18); }
    .badge.size{ background: rgba(59,130,246,0.10); color:#1d4ed8; border-color: rgba(59,130,246,0.18); }

    .img-box{
      width:72px; height:72px; border-radius:18px; overflow:hidden; background: rgba(255,255,255,0.70);
      border:1px solid rgba(255,255,255,0.85);
    }
    .img-box img{ width:100%; height:100%; object-fit:cover; display:block; }

    .line-clamp-1{ overflow:hidden; display:-webkit-box; -webkit-line-clamp:1; -webkit-box-orient:vertical; }
    .line-clamp-2{ overflow:hidden; display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; }
    .line-clamp-3{ overflow:hidden; display:-webkit-box; -webkit-line-clamp:3; -webkit-box-orient:vertical; }

    .kw-chip{ cursor:pointer; }
    .kw-chip:hover{ transform: translateY(-1px); }

    .review-grid{ display:grid; grid-template-columns: 1fr; gap: 14px; }
    @media (min-width: 768px){ .review-grid{ grid-template-columns: 1fr 1fr; } }
    @media (min-width: 1280px){ .review-grid{ grid-template-columns: 1fr 1fr 1fr; } }

    /* embed */
    body.embedded .topbar, body.embedded .layout-header { display:none !important; }
    body.embedded main{ padding: 24px !important; }
  </style>
</head>

<body>
  <!-- overlay -->
  <div id="overlay" class="overlay">
    <div class="flex flex-col items-center gap-3">
      <div class="spinner"></div>
      <div id="overlayText" class="text-sm font-black text-slate-700">Loading...</div>
    </div>
  </div>

  <!-- top bar -->
  <header class="topbar sticky top-0 z-50">
    <div class="mx-auto w-full max-w-[1320px] px-4 md:px-8 py-4">
      <div class="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-4">
        <div class="flex items-center gap-3">
          <div class="w-10 h-10 rounded-2xl bg-white/60 border border-white/80 flex items-center justify-center">
            <i class="fa-solid fa-chart-line text-slate-700"></i>
          </div>
          <div>
            <div class="text-sm font-black text-slate-900">VOC Dashboard</div>
            <div class="text-xs font-bold text-slate-500">Official + Naver</div>
          </div>
        </div>

        <div class="grid grid-cols-1 gap-3 w-full lg:w-auto">
          <div class="summary-card">
            <div class="small-label text-blue-600 mb-2">RUN</div>
            <div class="text-sm font-black text-slate-900" id="runDateSide">-</div>
            <div class="text-xs font-bold text-slate-500 mt-1" id="periodTextSide">-</div>
          </div>
        </div>
      </div>

      <div class="mt-3 text-[11px] font-bold text-slate-500 leading-relaxed">
        * 데이터: ./data/meta.json, ./data/reviews.json &nbsp;·&nbsp; * 빌드: crema_voc v6
      </div>
    </div>
  </header>

  <main class="p-6 md:p-10 w-full">
    <div class="mx-auto w-full max-w-[1280px]">
      <div class="max-w-[1320px] mx-auto">

        <div class="layout-header">
          <header class="flex flex-col md:flex-row md:items-center justify-between mb-6 gap-6">
            <div>
              <h1 class="text-4xl md:text-5xl font-black tracking-tight text-slate-900 mb-3">
                Official몰 & Naver 리뷰 VOC 대시보드
              </h1>
              <div class="text-sm font-bold text-slate-500" id="headerMeta">-</div>
            </div>

            <div class="flex items-center gap-2 flex-wrap">
              <button class="tab-btn active" data-tab="combined" onclick="switchSourceTab('combined')">
                <i class="fa-solid fa-layer-group mr-2"></i>Combined
              </button>
              <button class="tab-btn" data-tab="official" onclick="switchSourceTab('official')">
                <i class="fa-solid fa-store mr-2"></i>Official
              </button>
              <button class="tab-btn" data-tab="naver" onclick="switchSourceTab('naver')">
                <i class="fa-brands fa-naver mr-2"></i>Naver
              </button>
            </div>
          </header>

          <!-- 0 ML Signals -->
          <section class="mb-10">
            <div class="glass-card p-8">
              <div class="flex items-end justify-between gap-6 flex-wrap mb-6">
                <div>
                  <div class="small-label text-blue-600 mb-2">0. ML Signals</div>
                  <div class="text-2xl font-black text-slate-900">어제 리뷰 체크 + 키워드 마인드맵</div>
                  <div class="text-sm font-bold text-slate-500 mt-2">기본: 어제(KST) · 범위: 최근 3개월</div>
                </div>
                <div class="flex gap-2 flex-wrap">
                  <button class="chip active" id="chip-daily" onclick="toggleChip('daily')">Daily</button>
                  <button class="chip" id="chip-low" onclick="toggleChip('low')">Low</button>
                  <button class="chip" id="mindShuffle" onclick="shuffleMindmap()">Shuffle</button>
                </div>
              </div>

              <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
                <div class="summary-card">
                  <div class="small-label text-blue-600 mb-2">YESTERDAY</div>
                  <div class="text-3xl font-black"><span id="yCount">-</span> reviews</div>
                  <div class="text-xs font-bold text-slate-500 mt-2">어제 업로드된 리뷰 수 (현재 탭/필터 기준)</div>
                  <div class="text-xs font-bold text-slate-500 mt-3" id="yLowHint">-</div>
                </div>

                <div class="summary-card lg:col-span-2">
                  <div class="small-label text-blue-600 mb-2">KEYWORD MINDMAP</div>
                  <div class="text-xs font-bold text-slate-500">버블 클릭 → 검색 필터</div>
                  <div id="mindmapCanvas" class="relative mt-4 rounded-3xl border border-white/80 bg-white/45 overflow-hidden" style="height: 260px;"></div>
                </div>
              </div>

              <div class="summary-card mt-4">
                <div class="small-label text-slate-700 mb-2">TOPICS</div>
                <div id="topicRow" class="flex flex-wrap gap-2"></div>
                <div class="text-xs font-bold text-slate-500 mt-3">TF-IDF + NMF 토픽(최근 3개월) — 클릭하면 해당 키워드로 필터</div>
              </div>
            </div>
          </section>

          <!-- 1 Summary -->
          <section class="mb-10">
            <div class="glass-card p-8">
              <div class="flex items-end justify-between gap-6 flex-wrap mb-6">
                <div>
                  <div class="small-label text-blue-600 mb-2">1. Summary</div>
                  <div class="text-2xl font-black text-slate-900">키워드 하이라이트</div>
                </div>
                <div class="text-xs font-black text-slate-500">
                  * 최근 3개월 기반(토큰/토픽) · 클릭 → 해당 키워드로 필터
                </div>
              </div>

              <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
                <div class="summary-card">
                  <div class="small-label text-slate-700 mb-2">Keywords Top 5</div>

                  <div class="text-xs font-black text-slate-500 mt-1">NEG</div>
                  <div id="topNeg" class="mt-2 flex flex-wrap gap-2"></div>

                  <div class="text-xs font-black text-slate-500 mt-4">POS</div>
                  <div id="topPos" class="mt-2 flex flex-wrap gap-2"></div>

                  <div class="text-xs font-bold text-slate-500 mt-3">클릭 → Daily Feed에서 해당 리뷰로 스크롤</div>
                </div>

                <div class="summary-card">
                  <div class="small-label text-blue-600 mb-2">Workflow</div>
                  <div class="text-sm font-extrabold text-slate-800 leading-relaxed">
                    1) <span class="font-black">어제 리뷰</span>부터 확인<br/>
                    2) Low/개선요청 태그로 리스크 리뷰 먼저 보기<br/>
                    3) 제품 선택 → 리뷰 길이/저평점 정렬로 원인 텍스트 빠르게 파악
                  </div>
                  <div class="text-xs font-bold text-slate-500 mt-3">
                    * 상세 Drill-down은 Daily Feed에서
                  </div>
                </div>
              </div>
            </div>
          </section>

          <!-- 2 Ranking -->
          <section class="mb-10">
            <div class="glass-card p-8">
              <div class="flex items-end justify-between gap-6 flex-wrap mb-6">
                <div>
                  <div class="small-label text-blue-600 mb-2">2. 개선 우선순위 제품 랭킹</div>
                  <div class="text-xs font-bold text-slate-500 mt-1">Top5 기본 노출(접기/펼치기)</div>
                </div>
                <div class="flex gap-2 flex-wrap">
                  <button class="chip active" id="rank-size" onclick="switchRankMode('size')">2-1) 사이즈 이슈율</button>
                  <button class="chip" id="rank-low" onclick="switchRankMode('low')">2-2) 저평점 비중</button>
                  <button class="chip" id="rank-both" onclick="switchRankMode('both')">2-3) 교집합</button>
                </div>
              </div>

              <div class="flex items-center justify-end mb-3">
                <button class="chip" id="rankToggleBtn" onclick="toggleRankingExpand()">Top5만 보기</button>
              </div>

              <div class="overflow-auto">
                <table class="tbl min-w-[980px]">
                  <thead>
                    <tr>
                      <th class="text-left">제품명</th>
                      <th class="text-left">리뷰 수</th>
                      <th class="text-left">사이즈 이슈율</th>
                      <th class="text-left">저평점 비중</th>
                      <th class="text-left">주요 키워드</th>
                    </tr>
                  </thead>
                  <tbody id="rankingBody"></tbody>
                </table>
              </div>
            </div>
          </section>

          <!-- Daily feed -->
          <section class="mb-10">
            <div class="glass-card p-8">
              <div class="flex items-end justify-between gap-6 flex-wrap mb-6">
                <div>
                  <div class="small-label text-blue-600 mb-2">Daily Feed</div>
                  <div class="text-2xl font-black text-slate-900">그날 올라온 리뷰 (업로드 순)</div>
                  <div class="text-sm font-bold text-slate-500 mt-2">최근 7일 범위 내에서 날짜 선택 가능</div>
                </div>
              </div>

              <div class="grid grid-cols-1 lg:grid-cols-5 gap-3 mb-6">
                <input id="daySelect" type="date" class="input-glass" onchange="renderAll()" />
                <select id="productSelect" class="input-glass" onchange="renderAll()">
                  <option value="">제품 선택 (전체)</option>
                </select>
                <select id="sizeSelect" class="input-glass" onchange="renderAll()">
                  <option value="">옵션 사이즈 (전체)</option>
                </select>
                <select id="sortSelect" class="input-glass" onchange="renderAll()">
                  <option value="upload">정렬: 업로드 순 (기본)</option>
                  <option value="latest">최신순</option>
                  <option value="long">리뷰 길이 긴 순</option>
                  <option value="low">저평점순</option>
                </select>
                <input id="qInput" class="input-glass" placeholder="텍스트 검색(옵션)" oninput="renderAll()" />
              </div>

              <div id="dailyFeed" class="review-grid"></div>

              <div class="hidden review-card text-center" id="noResults">
                <div class="text-lg font-black text-slate-800">검색 결과가 없습니다.</div>
              </div>
            </div>
          </section>

          <!-- 4 Product Mindmap -->
          <section class="mb-10" id="mindmapSection">
            <div class="glass-card p-8">
              <div class="flex items-end justify-between gap-6 flex-wrap mb-6">
                <div>
                  <div class="small-label text-blue-600 mb-2">4. Product Mindmap</div>
                  <div class="text-2xl font-black text-slate-900">제품별 대표 문장 (POS/NEG)</div>
                  <div class="text-xs font-bold text-slate-500 mt-2">기본: 최근 3개월 누적 · 제품별 대표 문장(클릭→Daily Feed로 이동)</div>
                </div>
                <div class="flex items-center gap-2 flex-wrap">
                  <button class="chip" onclick="toggleMindmap()">접기/펼치기</button>
                </div>
              </div>

              <div id="productMindmap" class="grid grid-cols-1 lg:grid-cols-3 gap-4"></div>
            </div>
          </section>

          <footer class="text-xs font-bold text-slate-500 pb-8">
            * 데이터 소스: ./data/reviews.json (렌더 시 필터).<br/>
            * ML: TF-IDF + NMF 토픽, 키워드 공기(공출현) 기반 마인드맵(가벼운 그래프).<br/>
            * Product Mindmap: 제품별 POS/NEG 대표 문장(최근 3개월).<br/>
          </footer>

        </div>
      </div>
    </div>
  </main>

  <script>
    // ----------------------------
    // Global state
    // ----------------------------
    let META = null;
    let REVIEWS = [];
    const uiState = {
      sourceTab: "combined",
      rankMode: "size",
      rankExpanded: false,
      chips: { daily: true, low: false },
      pendingScrollId: null,
      mindmapCollapsed: false
    };

    // ----------------------------
    // Utils
    // ----------------------------
    function esc(s){
      return String(s ?? "")
        .replaceAll("&","&amp;")
        .replaceAll("<","&lt;")
        .replaceAll(">","&gt;")
        .replaceAll('"',"&quot;");
    }
    function asArr(x){ return Array.isArray(x) ? x : []; }

    function showOverlay(msg){
      const o = document.getElementById("overlay");
      const t = document.getElementById("overlayText");
      if (t) t.textContent = msg || "Loading...";
      if (o) o.classList.add("show");
    }
    function hideOverlay(){
      const o = document.getElementById("overlay");
      if (o) o.classList.remove("show");
    }
    function runWithOverlay(msg, fn){
      try{
        showOverlay(msg);
        const r = fn();
        if (r && typeof r.then === "function"){
          return r.finally(()=>hideOverlay());
        }
        hideOverlay();
        return r;
      }catch(e){
        console.error(e);
        hideOverlay();
      }
    }

    function kstNow(){
      const now = new Date();
      const utc = now.getTime() + (now.getTimezoneOffset() * 60000);
      return new Date(utc + (9 * 60 * 60000));
    }
    function kstDateStr(offsetDays){
      const d = kstNow();
      d.setDate(d.getDate() + (offsetDays || 0));
      const y = d.getFullYear();
      const m = String(d.getMonth()+1).padStart(2,"0");
      const dd = String(d.getDate()).padStart(2,"0");
      return `${y}-${m}-${dd}`;
    }
    function fmtDT(s){
      if (!s) return "-";
      const t = String(s).replace("T"," ").replace("+09:00","").replace("Z","");
      return t.slice(0,16);
    }

    // ----------------------------
    // Embed detection
    // ----------------------------
    (function(){
      const params = new URLSearchParams(location.search);
      if (params.get("embed") === "1"){
        document.body.classList.add("embedded");
      }
    })();

    // ----------------------------
    // UI actions
    // ----------------------------
    function switchSourceTab(tab){
      uiState.sourceTab = tab;
      document.querySelectorAll(".tab-btn").forEach(b=>{
        b.classList.toggle("active", b.getAttribute("data-tab") === tab);
      });
      renderAll();
    }

    function switchRankMode(mode){
      runWithOverlay("Switching ranking...", () => {
        uiState.rankMode = mode;
        document.getElementById("rank-size")?.classList.toggle("active", mode==="size");
        document.getElementById("rank-low")?.classList.toggle("active", mode==="low");
        document.getElementById("rank-both")?.classList.toggle("active", mode==="both");
        renderAll();
      });
    }

    function toggleRankingExpand(){
      uiState.rankExpanded = !uiState.rankExpanded;
      const btn = document.getElementById("rankToggleBtn");
      if (btn){
        btn.textContent = uiState.rankExpanded ? "접기" : "Top5만 보기";
        btn.classList.toggle("active", uiState.rankExpanded);
      }
      renderAll();
    }

    function toggleChip(name){
      uiState.chips[name] = !uiState.chips[name];
      const el = document.getElementById(`chip-${name}`);
      if (el) el.classList.toggle("active", uiState.chips[name]);
      renderAll();
    }

    function shuffleMindmap(){
      const box = document.getElementById("mindmapCanvas");
      if (!box) return;
      const nodes = Array.from(box.querySelectorAll("[data-kw]"));
      for (const n of nodes){
        n.style.left = (5 + Math.random()*90) + "%";
        n.style.top  = (10 + Math.random()*75) + "%";
      }
    }

    function setSearchAndRender(q){
      const el = document.getElementById("qInput");
      if (el) el.value = q || "";
      renderAll();
    }

    function applyKeywordAndScroll(keyword, rid){
      const q = document.getElementById("qInput");
      if (q) q.value = keyword || "";
      uiState.pendingScrollId = rid ? String(rid) : null;
      renderAll();
    }

    function tryScrollToReview(){
      if (!uiState.pendingScrollId) return;
      const el = document.getElementById(`review-${uiState.pendingScrollId}`);
      if (el){
        el.scrollIntoView({behavior:"smooth", block:"center"});
        el.classList.add("ring-4","ring-blue-200");
        setTimeout(()=>el.classList.remove("ring-4","ring-blue-200"), 1200);
      }
      uiState.pendingScrollId = null;
    }

    function jumpToReviewFromEl(el){
      if (!el) return;
      const payload = {
        id: el.dataset.id || "",
        source: el.dataset.source || "",
        created_at: el.dataset.created_at || "",
        product_code: el.dataset.product_code || "",
        option_size: el.dataset.option_size || ""
      };

      if (payload.source === "Official") uiState.sourceTab = "official";
      else if (payload.source === "Naver") uiState.sourceTab = "naver";
      else uiState.sourceTab = "combined";

      document.querySelectorAll(".tab-btn").forEach(b=>{
        b.classList.toggle("active", b.getAttribute("data-tab") === uiState.sourceTab);
      });

      const dayEl = document.getElementById("daySelect");
      if (uiState.chips.daily && dayEl && payload.created_at){
        dayEl.value = String(payload.created_at).slice(0,10);
      }

      const prodEl = document.getElementById("productSelect");
      if (prodEl) prodEl.value = payload.product_code || "";

      const sizeEl = document.getElementById("sizeSelect");
      if (sizeEl) sizeEl.value = payload.option_size || "";

      uiState.pendingScrollId = payload.id ? String(payload.id) : null;
      renderAll();
    }

    function toggleMindmap(){
      uiState.mindmapCollapsed = !uiState.mindmapCollapsed;
      const grid = document.getElementById("productMindmap");
      if (grid) grid.style.display = uiState.mindmapCollapsed ? "none" : "";
    }

    // ----------------------------
    // Filters & metrics
    // ----------------------------
    function getFilteredReviews(){
      let rows = REVIEWS.slice();

      if (uiState.sourceTab === "official") rows = rows.filter(r => r.source === "Official");
      if (uiState.sourceTab === "naver")    rows = rows.filter(r => r.source === "Naver");

      const day = (document.getElementById("daySelect")?.value || "").trim();
      if (uiState.chips.daily && day){
        rows = rows.filter(r => String(r.created_at || "").slice(0,10) === day);
      }

      const prod = (document.getElementById("productSelect")?.value || "").trim();
      if (prod) rows = rows.filter(r => String(r.product_code || "") === prod);

      const sz = (document.getElementById("sizeSelect")?.value || "").trim();
      if (sz) rows = rows.filter(r => String(r.option_size || "") === sz);

      if (uiState.chips.low){
        rows = rows.filter(r => (Number(r.rating||0) <= 2) || asArr(r.tags).includes("low"));
      }

      const q = (document.getElementById("qInput")?.value || "").trim().toLowerCase();
      if (q){
        rows = rows.filter(r =>
          String(r.text||"").toLowerCase().includes(q) ||
          String(r.product_name||"").toLowerCase().includes(q)
        );
      }

      const sort = document.getElementById("sortSelect")?.value || "upload";
      if (sort === "latest") rows.sort((a,b)=> new Date(b.created_at||0) - new Date(a.created_at||0));
      else if (sort === "long") rows.sort((a,b)=> (String(b.text||"").length - String(a.text||"").length));
      else if (sort === "low") rows.sort((a,b)=> (Number(a.rating||0) - Number(b.rating||0)) || (new Date(b.created_at||0) - new Date(a.created_at||0)));
      // upload(default): keep order as in json
      return rows;
    }

    function calcMetrics(reviews){
      const total = Math.max(1, reviews.length);
      const sizeMention = reviews.filter(r => asArr(r.tags).includes("size")).length;

      const byProd = new Map();
      for (const r of reviews){
        const code = r.product_code || "-";
        if (!byProd.has(code)){
          byProd.set(code, {
            product_code: code,
            product_name: r.product_name || code,
            product_url: r.product_url || "",
            local_product_image: r.local_product_image || "",
            reviews: 0,
            sizeIssue: 0,
            low: 0,
          });
        }
        const g = byProd.get(code);
        g.reviews += 1;
        if (asArr(r.tags).includes("size")) g.sizeIssue += 1;
        if ((Number(r.rating||0) <= 2) || asArr(r.tags).includes("low")) g.low += 1;
      }

      const rows = Array.from(byProd.values()).map(g => {
        const sizeRate = Math.round((g.sizeIssue / Math.max(1,g.reviews))*100);
        const lowRate  = Math.round((g.low / Math.max(1,g.reviews))*100);
        return { ...g, sizeRate, lowRate, kwds: "-" };
      });

      const rankSize = rows.slice().sort((a,b)=> (b.sizeRate - a.sizeRate) || (b.reviews - a.reviews));
      const rankLow  = rows.slice().sort((a,b)=> (b.lowRate - a.lowRate) || (b.reviews - a.reviews));
      const rankBoth = rows.slice().sort((a,b)=> ((b.sizeRate+b.lowRate) - (a.sizeRate+a.lowRate)) || (b.reviews - a.reviews));

      return { total, rankSize, rankLow, rankBoth };
    }

    // ----------------------------
    // Render blocks
    // ----------------------------
    function renderHeader(){
      const runDate = META?.updated_at || "-";
      const periodText = META?.period_text || "-";

      const sideRun = document.getElementById("runDateSide");
      const sidePeriod = document.getElementById("periodTextSide");
      const headerMeta = document.getElementById("headerMeta");

      if (sideRun) sideRun.textContent = String(runDate).slice(0,10).replaceAll("-",".");
      if (sidePeriod) sidePeriod.textContent = periodText;
      if (headerMeta) headerMeta.textContent = `${runDate} · ${periodText}`;
    }

    function renderSummary(){
      const neg = META?.neg_top5 || [];
      const pos = META?.pos_top5 || [];

      const elNeg = document.getElementById("topNeg");
      if (elNeg){
        elNeg.innerHTML = neg.map(([k,c,rid]) => `
          <span class="badge neg kw-chip" onclick="applyKeywordAndScroll('${esc(k)}', '${esc(rid||"")}')">
            #${esc(k)} <span class="opacity-70">${esc(c)}</span>
          </span>
        `).join("");
      }

      const elPos = document.getElementById("topPos");
      if (elPos){
        elPos.innerHTML = pos.map(([k,c,rid]) => `
          <span class="badge pos kw-chip" onclick="applyKeywordAndScroll('${esc(k)}', '${esc(rid||"")}')">
            #${esc(k)} <span class="opacity-70">${esc(c)}</span>
          </span>
        `).join("");
      }
    }

    function renderRanking(metrics){
      let rows = [];
      if (uiState.rankMode === "size") rows = metrics.rankSize;
      else if (uiState.rankMode === "low") rows = metrics.rankLow;
      else rows = metrics.rankBoth;

      const maxRows = uiState.rankExpanded ? Math.min(rows.length, 50) : Math.min(rows.length, 5);
      const tbody = document.getElementById("rankingBody");
      if (!tbody) return;

      tbody.innerHTML = rows.slice(0, maxRows).map(r => `
        <tr>
          <td>
            <div class="flex items-center gap-3">
              <div class="img-box">
                ${r.local_product_image
                  ? `<img src="${esc(r.local_product_image)}" alt="">`
                  : `<div class="w-full h-full flex items-center justify-center text-[10px] text-slate-400">NO IMAGE</div>`
                }
              </div>
              <div class="min-w-0">
                <div class="font-black text-slate-900 line-clamp-1">${esc(r.product_name)}</div>
                <div class="muted">code: ${esc(r.product_code)}</div>
                ${r.product_url ? `<a href="${esc(r.product_url)}" target="_blank" class="text-xs font-black text-blue-600 hover:underline">상품 페이지</a>` : ``}
              </div>
            </div>
          </td>
          <td class="muted">${esc(r.reviews)}</td>
          <td><span class="badge size">${esc(r.sizeRate)}%</span></td>
          <td><span class="badge neg">${esc(r.lowRate)}%</span></td>
          <td class="muted">${esc(r.kwds || "-")}</td>
        </tr>
      `).join("");
    }

    function renderProductSelect(){
      const sel = document.getElementById("productSelect");
      if (!sel) return;

      const current = sel.value;
      const map = new Map();
      for (const r of REVIEWS){
        const code = r.product_code || "";
        if (!code) continue;
        if (!map.has(code)) map.set(code, r.product_name || code);
      }

      sel.innerHTML = [`<option value="">제품 선택 (전체)</option>`].concat(
        Array.from(map.entries())
          .sort((a,b)=> String(a[1]).localeCompare(String(b[1])))
          .map(([code,name]) => `<option value="${esc(code)}">${esc(name)} (${esc(code)})</option>`)
      ).join("");
      sel.value = current;
    }

    function renderSizeSelect(){
      const sel = document.getElementById("sizeSelect");
      if (!sel) return;

      const current = sel.value;
      const set = new Set();
      for (const r of REVIEWS){
        const sz = (r.option_size || "").trim();
        if (sz) set.add(sz);
      }

      const opts = Array.from(set).sort((a,b)=> String(a).localeCompare(String(b)));
      sel.innerHTML = [`<option value="">옵션 사이즈 (전체)</option>`]
        .concat(opts.map(sz => `<option value="${esc(sz)}">${esc(sz)}</option>`))
        .join("");

      sel.value = current;
    }

    function renderDailyFeed(reviews){
      const container = document.getElementById("dailyFeed");
      const no = document.getElementById("noResults");
      if (!container || !no) return;

      const rows = reviews.slice(0, 30);
      if (!rows.length){
        container.innerHTML = "";
        no.classList.remove("hidden");
        return;
      }

      no.classList.add("hidden");
      container.innerHTML = rows.map(reviewCardHTML).join("");
    }

    function reviewCardHTML(r){
      const t = asArr(r.tags);
      const tags = [];

      if (t.includes("pos")) tags.push(`<span class="badge pos"><i class="fa-solid fa-face-smile"></i> #긍정</span>`);
      if (t.includes("size")) tags.push(`<span class="badge size"><i class="fa-solid fa-ruler"></i> #size_issue</span>`);
      if ((Number(r.rating||0) <= 2) || t.includes("low")) tags.push(`<span class="badge neg"><i class="fa-solid fa-triangle-exclamation"></i> #low_rating</span>`);
      if (t.includes("req")) tags.push(`<span class="badge neg"><i class="fa-solid fa-wrench"></i> #개선요청</span>`);
      if (r.text_image_path) tags.push(`<span class="badge"><i class="fa-solid fa-image"></i> #100자+이미지</span>`);

      const prodImg = r.local_product_image
        ? `<img src="${esc(r.local_product_image)}" alt="">`
        : `<div class="w-full h-full flex items-center justify-center text-[10px] text-slate-400">NO IMAGE</div>`;

      const reviewThumb = r.local_review_thumb
        ? `<img src="${esc(r.local_review_thumb)}" class="w-full max-h-56 object-contain rounded-lg bg-slate-50" />`
        : ``;

      const textImg = r.text_image_path
        ? `<img src="${esc(r.text_image_path)}" class="w-full max-h-72 object-contain rounded-2xl bg-slate-50 border border-white/80" />`
        : ``;

      return `
        <div class="review-card hover:shadow-lg transition cursor-pointer"
             id="review-${esc(r.id)}"
             data-id="${esc(r.id)}"
             data-source="${esc(r.source||"")}"
             data-created_at="${esc(r.created_at||"")}"
             data-product_code="${esc(r.product_code||"")}"
             data-option_size="${esc(r.option_size||"")}"
             onclick="jumpToReviewFromEl(this)">

          <div class="flex items-start justify-between gap-3">
            <div class="flex items-center gap-3 min-w-0">
              <div class="img-box">${prodImg}</div>
              <div class="min-w-0">
                <div class="font-black text-slate-900 line-clamp-1">${esc(r.product_name || r.product_code)}</div>
                <div class="text-xs font-bold text-slate-500 mt-1">
                  code: ${esc(r.product_code)} · source: ${esc(r.source || "-")} · id: ${esc(r.id)}
                </div>
              </div>
            </div>
            <div class="text-right">
              <div class="text-xs font-black text-slate-700">★ ${esc(r.rating)}</div>
              <div class="text-[11px] font-bold text-slate-500 mt-1">${esc(fmtDT(r.created_at))}</div>
            </div>
          </div>

          <div class="mt-3 flex flex-wrap gap-2">
            ${(r.option_size ? `<span class="badge">옵션: ${esc(r.option_size)}</span>` : ``)}
            ${tags.join("")}
          </div>

          <div class="mt-3 text-sm font-extrabold text-slate-800 leading-relaxed whitespace-pre-wrap break-words line-clamp-3">
            ${esc(r.text || "")}
          </div>

          ${reviewThumb ? `<div class="mt-3">${reviewThumb}</div>` : ``}
          ${textImg ? `<div class="mt-4"><div class="small-label text-blue-600 mb-2">100+ TEXT IMAGE</div>${textImg}</div>` : ``}
        </div>
      `;
    }

    function renderMLSignals(allFiltered){
      const yday = kstDateStr(-1);
      const yRows = allFiltered.filter(r => String(r.created_at||"").slice(0,10) === yday);
      const yCnt = yRows.length;

      const yCountEl = document.getElementById("yCount");
      if (yCountEl) yCountEl.textContent = yCnt;

      const lowCnt = yRows.filter(r => (Number(r.rating||0) <= 2) || asArr(r.tags).includes("low")).length;
      const hint = document.getElementById("yLowHint");
      if (hint){
        hint.textContent = (yCnt ? `Low/리스크 추정: ${lowCnt}건` : "어제 데이터가 없습니다.");
      }

      const canvas = document.getElementById("mindmapCanvas");
      if (canvas){
        const graph = META?.kw_graph_3m || {nodes:[], links:[]};
        const nodes = Array.isArray(graph.nodes) ? graph.nodes : [];
        if (!nodes.length){
          canvas.innerHTML = `<div class="absolute inset-0 flex items-center justify-center text-sm font-black text-slate-500">키워드 시그널이 없습니다.</div>`;
        } else {
          canvas.innerHTML = nodes.slice(0,28).map((n, i) => {
            const pol = (n.pol||"neutral");
            const cls = pol==="neg" ? "badge neg" : (pol==="pos" ? "badge pos" : "badge");
            const left = (8 + Math.random()*84);
            const top  = (12 + Math.random()*70);
            const scale = Math.min(1.45, 0.92 + (Number(n.w||1) / 16));
            const delay = (i % 10) * 60;

            return `
              <div class="${cls} kw-chip absolute"
                   data-kw="${esc(n.id)}"
                   style="left:${left}%; top:${top}%; transform: translate(-50%,-50%) scale(${scale}); transition: transform .18s ease; animation: floaty 6s ease-in-out ${delay}ms infinite;"
                   onclick="setSearchAndRender('${esc(n.id)}')">
                #${esc(n.label)} <span class="opacity-70">${esc(n.w)}</span>
              </div>
            `;
          }).join("");
        }
      }

      const tr = document.getElementById("topicRow");
      if (tr){
        const topics = META?.ml_topics_3m || [];
        if (!Array.isArray(topics) || !topics.length){
          tr.innerHTML = `<span class="text-xs font-bold text-slate-400">토픽 데이터 없음</span>`;
        } else {
          tr.innerHTML = topics.slice(0,8).map((t, idx) => {
            const words = Array.isArray(t?.words) ? t.words : [];
            const title = words.slice(0,4).join(", ");
            const key = words[0] || "";
            return `<span class="badge kw-chip" title="${esc(title)}" onclick="setSearchAndRender('${esc(key)}')"><i class="fa-solid fa-circle-nodes"></i> Topic ${idx+1}: ${esc(words[0]||"-")}</span>`;
          }).join("");
        }
      }
    }

    function renderProductMindmap(){
      const root = document.getElementById("productMindmap");
      if (!root) return;

      const mm = META?.product_mindmap_3m || [];
      if (!Array.isArray(mm) || !mm.length){
        root.innerHTML = `<div class="text-sm font-black text-slate-500">product_mindmap_3m 데이터가 없습니다.</div>`;
        return;
      }

      const sentenceBlock = (mode, s) => {
        const color = (mode==="pos") ? "text-emerald-700" : "text-red-700";
        const onclick = (s?.id!=null) ? `onclick="applyKeywordAndScroll('', '${esc(s.id)}')"` : "";
        return `
          <div class="mt-2 p-3 rounded-2xl bg-white/55 border border-white/80 cursor-pointer" ${onclick}>
            <div class="text-xs font-black ${color}">
              ${mode.toUpperCase()} · ★ ${esc(s.rating ?? "-")} · ${esc(fmtDT(s.created_at))}
            </div>
            <div class="text-sm font-extrabold text-slate-800 leading-relaxed mt-2">
              ${esc(s.text || "-")}
            </div>
          </div>
        `;
      };

      root.innerHTML = mm.slice(0,24).map(p => {
        const img = p.local_product_image
          ? `<img src="${esc(p.local_product_image)}" alt="" class="w-14 h-14 rounded-2xl object-cover border border-white/80 bg-white/60" />`
          : `<div class="w-14 h-14 rounded-2xl flex items-center justify-center text-[10px] text-slate-400 border border-white/80 bg-white/60">NO IMAGE</div>`;

        const pos = (p.pos_sentences || []).slice(0,2);
        const neg = (p.neg_sentences || []).slice(0,2);

        return `
          <div class="summary-card">
            <div class="flex items-start gap-3">
              ${img}
              <div class="min-w-0">
                <div class="font-black text-slate-900 line-clamp-2">${esc(p.product_name || p.product_code)}</div>
                <div class="text-[11px] font-black text-slate-500 mt-1">code: ${esc(p.product_code)} · 3m reviews: ${esc(p.reviews_3m)}</div>
              </div>
            </div>

            <div class="mt-4">
              ${pos.length ? pos.map(s => sentenceBlock("pos", s)).join("") : `<div class="text-xs font-bold text-slate-400 mt-2">POS 문장 없음</div>`}
            </div>

            <div class="mt-4">
              ${neg.length ? neg.map(s => sentenceBlock("neg", s)).join("") : `<div class="text-xs font-bold text-slate-400 mt-2">NEG 문장 없음</div>`}
            </div>
          </div>
        `;
      }).join("");
    }

    function renderAll(){
      renderHeader();
      renderProductSelect();
      renderSizeSelect();

      const filtered = getFilteredReviews();
      const metrics = calcMetrics(filtered);

      renderMLSignals(filtered);
      renderSummary();
      renderRanking(metrics);
      renderProductMindmap();
      renderDailyFeed(filtered);

      tryScrollToReview();
    }

    // ----------------------------
    // Boot
    // ----------------------------
    async function boot(){
      await runWithOverlay("Loading VOC data...", async () => {
        const [meta, reviews] = await Promise.all([
          fetch("./data/meta.json", {cache:"no-store"}).then(r => r.json()),
          fetch("./data/reviews.json", {cache:"no-store"}).then(r => r.json()),
        ]);

        META = meta || {};
        REVIEWS = (reviews && reviews.reviews) ? reviews.reviews : [];

        // ✅ daySelect default = yesterday(KST) if in range, else period_end
        const dayInput = document.getElementById("daySelect");
        if (dayInput && META?.period_start && META?.period_end){
          dayInput.min = META.period_start;
          dayInput.max = META.period_end;

          const y = kstDateStr(-1);
          const inRange = (y >= META.period_start && y <= META.period_end);
          dayInput.value = inRange ? y : META.period_end;
        }

        renderAll();
      });
    }

    boot();
  </script>
</body>
</html>
"""


def kst_today() -> date:
    return datetime.now(KST).date()


def read_json(p: Path) -> Any:
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(p: Path, obj: Any) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def rewrite_asset_path(val: str, prefix: str) -> str:
    """
    JSON contains relative asset path like:
      assets/products/xxx.jpg

    Dashboard lives at:
      reports/voc_crema/index.html

    Correct relative path to site/assets is:
      ../../site/assets/products/xxx.jpg

    Convert:
      assets/... -> {prefix}/assets/...
    """
    if not isinstance(val, str):
        return val
    if val.startswith("assets/"):
        return f"{prefix}/{val}"
    return val


def normalize_reviews_for_reports(reviews: List[Dict[str, Any]], asset_prefix: str) -> List[Dict[str, Any]]:
    keys = ["local_product_image", "local_review_thumb", "text_image_path"]
    out: List[Dict[str, Any]] = []
    for r in reviews:
        rr = dict(r)
        for k in keys:
            if k in rr and isinstance(rr[k], str):
                rr[k] = rewrite_asset_path(rr[k], asset_prefix)
        out.append(rr)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src-dir", default="site/data", help="source dir containing meta.json & reviews.json")
    ap.add_argument("--out-dir", default="reports/voc_crema", help="output dashboard directory")
    ap.add_argument("--asset-prefix", default="../../site", help='prefix for resolving "assets/..." from out-dir')
    args = ap.parse_args()

    src_dir = Path(args.src_dir)
    out_dir = Path(args.out_dir)
    out_data = out_dir / "data"

    meta_path = src_dir / "meta.json"
    reviews_path = src_dir / "reviews.json"

    if not meta_path.exists():
        raise FileNotFoundError(f"missing: {meta_path}")
    if not reviews_path.exists():
        raise FileNotFoundError(f"missing: {reviews_path}")

    meta = read_json(meta_path)
    reviews_obj = read_json(reviews_path)

    reviews = reviews_obj.get("reviews", [])
    if not isinstance(reviews, list):
        reviews = []

    # ✅ fix local asset paths for dashboard location
    reviews_fixed = normalize_reviews_for_reports(reviews, args.asset_prefix)

    # meta: ensure minimum fields exist (so UI never shows '-' permanently)
    meta_out: Dict[str, Any] = dict(meta) if isinstance(meta, dict) else {}
    meta_out.setdefault("version", "voc-dashboard-final")
    meta_out.setdefault("updated_at", datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"))

    if not meta_out.get("period_end"):
        meta_out["period_end"] = str(kst_today())
    if not meta_out.get("period_start"):
        meta_out["period_start"] = str(kst_today() - timedelta(days=6))
    if not meta_out.get("period_text"):
        meta_out["period_text"] = f"최근 7일 ({meta_out['period_start']} ~ {meta_out['period_end']})"

    # write outputs
    write_json(out_data / "meta.json", meta_out)
    write_json(out_data / "reviews.json", {"reviews": reviews_fixed})

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "index.html").write_text(INDEX_HTML, encoding="utf-8")

    print("[OK] VOC dashboard built:")
    print(f" - {out_dir / 'index.html'}")
    print(f" - {out_data / 'meta.json'}")
    print(f" - {out_data / 'reviews.json'}")


if __name__ == "__main__":
    main()
