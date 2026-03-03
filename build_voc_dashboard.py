#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_voc_dashboard.py  (VOC Dashboard Builder | stable)

What this does
- Input: reviews JSON (from crema_voc pipeline)
- Output: static site under ./site
    site/index.html
    site/template.html
    site/data/meta.json
    site/data/reviews.json
    (assets are expected to exist if collector produced them; we do not download images here)

Key fixes vs broken template
- No duplicated functions / no broken JS blocks
- Default daySelect = yesterday (KST)
- Path normalize for local asset fields (strip leading "site/")
- Keyword cleaning: remove gratitude/noise tokens
- Keyword highlight blacklist: remove "사이즈/신발/..." from top highlights if you don't want them
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional
from collections import Counter, defaultdict


KST = timezone(timedelta(hours=9))

# ---------------------------
# Config: keyword cleaning
# ---------------------------

# 너가 말한 “감사/맘에 않고/감사들어요…” 류 + 의미없는 감탄/조사성 토큰 제거
NOISE_PATTERNS = [
    r"감사", r"고맙", r"감사합니다", r"감사해요", r"고마워요",
    r"맘에", r"마음에", r"들어요", r"듭니다", r"안고", r"있어",
    r"좋아요", r"좋네요", r"괜찮아요",
    r"배송", r"빠르", r"포장", r"만족",  # 원하면 여기 빼도 됨
]

# 키워드 하이라이트에서 빼고 싶은 단어들 (너 요청 반영)
HIGHLIGHT_BLACKLIST = {
    "사이즈", "신발", "운동화", "발볼", "발", "정사이즈", "한치수", "크게", "작게",
}

# 아주 흔한 조사/어미/잡토큰
KOREAN_STOP = {
    "그리고","근데","그런데","진짜","너무","완전","약간","진심",
    "이거","저거","그거","요거","요게","정말","그냥","되게",
    "해서","인데","으로","까지","부터","보다","같이","같은",
    "있어요","있음","없어요","없음","합니다","해요","했어요","됐어요",
    "입니다","네요","거예요","거같","같아요",
}

# ---------------------------
# HTML Template (clean)
# ---------------------------

TEMPLATE_HTML = r"""<!DOCTYPE html>
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

    .review-grid{ display:grid; grid-template-columns: 1fr; gap: 14px; }
    @media (min-width: 768px){ .review-grid{ grid-template-columns: 1fr 1fr; } }
    @media (min-width: 1280px){ .review-grid{ grid-template-columns: 1fr 1fr 1fr; } }

    /* old-school bubble mindmap look */
    .bubble{
      position:absolute;
      transform: translate(-50%,-50%);
      border-radius:9999px;
      padding: 10px 14px;
      font-weight: 900;
      font-size: 12px;
      border: 1px solid rgba(255,255,255,0.90);
      background: rgba(255,255,255,0.72);
      box-shadow: 0 14px 30px rgba(0,45,114,0.08);
      cursor:pointer;
      user-select:none;
      white-space: nowrap;
    }
    .bubble.neg{ background: rgba(239,68,68,0.10); color:#b91c1c; border-color: rgba(239,68,68,0.18); }
    .bubble.pos{ background: rgba(16,185,129,0.10); color:#047857; border-color: rgba(16,185,129,0.18); }

    body.embedded .topbar, body.embedded .layout-header { display:none !important; }
    body.embedded main{ padding: 24px !important; }
  </style>
</head>

<body>
  <div id="overlay" class="overlay">
    <div class="flex flex-col items-center gap-3">
      <div class="spinner"></div>
      <div id="overlayText" class="text-sm font-black text-slate-700">Loading...</div>
    </div>
  </div>

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
        * 데이터: data/meta.json, data/reviews.json &nbsp;·&nbsp; * 빌드: crema_voc v6
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
        </div>

        <!-- 0 ML Mindmap + Yesterday Focus -->
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
              <div class="text-xs font-bold text-slate-500 mt-3">최근 3개월 토픽(클릭하면 해당 키워드로 필터)</div>
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
                * 클릭 → 해당 키워드로 검색
              </div>
            </div>

            <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
              <div class="summary-card">
                <div class="small-label text-slate-700 mb-2">Keywords Top 5</div>

                <div class="text-xs font-black text-slate-500 mt-1">NEG</div>
                <div id="topNeg" class="mt-2 flex flex-wrap gap-2"></div>

                <div class="text-xs font-black text-slate-500 mt-4">POS</div>
                <div id="topPos" class="mt-2 flex flex-wrap gap-2"></div>
              </div>

              <div class="summary-card">
                <div class="small-label text-blue-600 mb-2">Workflow</div>
                <div class="text-sm font-extrabold text-slate-800 leading-relaxed">
                  1) <span class="font-black">어제 리뷰</span>부터 확인<br/>
                  2) Low/개선요청 태그로 리스크 리뷰 먼저 보기<br/>
                  3) 제품 선택 → 저평점/리뷰 길이로 원인 텍스트 빠르게 파악
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

        <!-- 5 Daily feed -->
        <section class="mb-10">
          <div class="glass-card p-8">
            <div class="flex items-end justify-between gap-6 flex-wrap mb-6">
              <div>
                <div class="small-label text-blue-600 mb-2">Daily Feed</div>
                <div class="text-2xl font-black text-slate-900">그날 올라온 리뷰 (업로드 순)</div>
                <div class="text-sm font-bold text-slate-500 mt-2">기본 날짜: 어제(KST) · 최근 7일 범위 내 선택</div>
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

        <footer class="text-xs font-bold text-slate-500 pb-8">
          * 데이터 소스: data/reviews.json / data/meta.json<br/>
          * 기본 동작: 어제(KST) 기준 Daily Feed 표시
        </footer>
      </div>
    </div>
  </main>

  <script>
    let META = null;
    let REVIEWS = [];
    const uiState = {
      sourceTab: "combined",
      rankMode: "size",
      rankExpanded: false,
      chips: { daily: true, low: false },
      pendingScrollId: null
    };

    function esc(s){
      return String(s ?? "")
        .replaceAll("&","&amp;")
        .replaceAll("<","&lt;")
        .replaceAll(">","&gt;")
        .replaceAll('"',"&quot;");
    }
    function asArr(x){ return Array.isArray(x) ? x : []; }
    function fmtDT(s){
      if (!s) return "-";
      const t = String(s).replace("T"," ").replace("+09:00","").replace("Z","");
      return t.slice(0,16);
    }
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

    (function(){
      const params = new URLSearchParams(location.search);
      if (params.get("embed") === "1"){
        document.body.classList.add("embedded");
      }
    })();

    function pad2(n){ return String(n).padStart(2,"0"); }
    function kstDateStr(offsetDays=0){
      const now = new Date();
      const utc = now.getTime() + (now.getTimezoneOffset() * 60000);
      const kst = new Date(utc + (9 * 60 * 60000));
      kst.setDate(kst.getDate() + offsetDays);
      return `${kst.getFullYear()}-${pad2(kst.getMonth()+1)}-${pad2(kst.getDate())}`;
    }

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

    function setSearchAndRender(q){
      const el = document.getElementById("qInput");
      if (el) el.value = q || "";
      renderAll();
    }

    function getFilteredReviews(){
      let rows = REVIEWS.slice();

      if (uiState.sourceTab === "official") rows = rows.filter(r => r.source === "Official");
      if (uiState.sourceTab === "naver") rows = rows.filter(r => r.source === "Naver");

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

      return {
        total,
        sizeMentionRate: Math.round((sizeMention/total)*100),
        rankSize, rankLow, rankBoth,
      };
    }

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
        elNeg.innerHTML = neg.map(([k,c]) => `
          <span class="badge neg" onclick="setSearchAndRender('${esc(k)}')">#${esc(k)} <span class="opacity-70">${esc(c)}</span></span>
        `).join("");
      }

      const elPos = document.getElementById("topPos");
      if (elPos){
        elPos.innerHTML = pos.map(([k,c]) => `
          <span class="badge pos" onclick="setSearchAndRender('${esc(k)}')">#${esc(k)} <span class="opacity-70">${esc(c)}</span></span>
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
        <div class="review-card hover:shadow-lg transition"
             id="review-${esc(r.id)}">

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

    function renderDailyFeed(reviews){
      const container = document.getElementById("dailyFeed");
      const no = document.getElementById("noResults");
      if (!container || !no) return;

      const rows = reviews.slice(0, 60);
      if (!rows.length){
        container.innerHTML = "";
        no.classList.remove("hidden");
        return;
      }

      no.classList.add("hidden");
      container.innerHTML = rows.map(reviewCardHTML).join("");
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

      const options = [`<option value="">제품 선택 (전체)</option>`].concat(
        Array.from(map.entries())
          .sort((a,b)=> String(a[1]).localeCompare(String(b[1])))
          .map(([code,name]) => `<option value="${esc(code)}">${esc(name)} (${esc(code)})</option>`)
      ).join("");

      sel.innerHTML = options;
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
          canvas.innerHTML = nodes.map((n, i) => {
            const pol = (n.pol||"neutral");
            const cls = pol==="neg" ? "bubble neg" : (pol==="pos" ? "bubble pos" : "bubble");
            const left = (8 + Math.random()*84);
            const top  = (12 + Math.random()*70);
            const scale = Math.min(1.6, 0.85 + (Number(n.w||1) / Math.max(10, nodes[0].w||10)));
            return `
              <div class="${cls}"
                   data-kw="${esc(n.id)}"
                   style="left:${left}%; top:${top}%; transform: translate(-50%,-50%) scale(${scale});"
                   onclick="setSearchAndRender('${esc(n.id)}')">
                ${esc(n.label)} <span class="opacity-60">(${esc(n.w)})</span>
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
            const key = words[0] || "";
            const title = words.slice(0,4).join(", ");
            return `<span class="badge" title="${esc(title)}" onclick="setSearchAndRender('${esc(key)}')"><i class="fa-solid fa-circle-nodes"></i> Topic ${idx+1}: ${esc(key||"-")}</span>`;
          }).join("");
        }
      }
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
      renderDailyFeed(filtered);
    }

    async function boot(){
      runWithOverlay("Loading VOC data...", async () => {
        const [meta, reviews] = await Promise.all([
          fetch("./data/meta.json", {cache:"no-store"}).then(r => r.json()),
          fetch("./data/reviews.json", {cache:"no-store"}).then(r => r.json()),
        ]);

        META = meta;
        REVIEWS = (reviews && reviews.reviews) ? reviews.reviews : [];

        const dayInput = document.getElementById("daySelect");
        if (dayInput){
          // min/max
          if (META?.period_start) dayInput.min = META.period_start;
          if (META?.period_end) dayInput.max = META.period_end;

          // ✅ default = yesterday(KST)
          if (!dayInput.value){
            dayInput.value = kstDateStr(-1);
          }
        }

        renderAll();
      });
    }

    boot();
  </script>
</body>
</html>
"""

# ---------------------------
# Helpers
# ---------------------------

def kst_now() -> datetime:
    return datetime.now(tz=KST)

def kst_date_str(d: datetime) -> str:
    return d.astimezone(KST).strftime("%Y-%m-%d")

def parse_dt(s: str) -> Optional[datetime]:
    if not s:
        return None
    # examples: "2026-03-02T21:00:39+09:00" or "2026-03-02 21:00:39"
    t = str(s).strip()
    try:
        if "T" in t:
            return datetime.fromisoformat(t.replace("Z", "+00:00")).astimezone(KST)
    except Exception:
        pass
    try:
        return datetime.strptime(t[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST)
    except Exception:
        return None

def safe_mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def load_reviews(path: Path) -> List[Dict[str, Any]]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(obj, dict) and "reviews" in obj and isinstance(obj["reviews"], list):
        return obj["reviews"]
    if isinstance(obj, dict) and "reviews" in obj and isinstance(obj["reviews"], dict) and "reviews" in obj["reviews"]:
        return obj["reviews"]["reviews"] or []
    if isinstance(obj, list):
        return obj
    return []

def normalize_local_asset_path(v: str) -> str:
    if not v:
        return v
    s = str(v).strip()
    # remove leading ./ and site/
    s = re.sub(r"^(\./)+", "", s)
    s = re.sub(r"^site/", "", s)
    # keep assets/... relative
    return s

def clean_text_for_tokens(s: str) -> str:
    if not s:
        return ""
    t = str(s)
    t = re.sub(r"[^\w가-힣\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def build_noise_regex() -> re.Pattern:
    pat = "(" + "|".join(NOISE_PATTERNS) + ")"
    return re.compile(pat)

NOISE_RE = build_noise_regex()

def tokenize_korean(text: str) -> List[str]:
    t = clean_text_for_tokens(text)
    if not t:
        return []
    # very light tokenizer: split by spaces, keep 2+ length
    raw = [w.strip() for w in t.split(" ") if w.strip()]
    out: List[str] = []
    for w in raw:
        if len(w) < 2:
            continue
        if w in KOREAN_STOP:
            continue
        if NOISE_RE.search(w):
            continue
        out.append(w)
    return out

def pick_top_keywords(reviews: List[Dict[str, Any]], topk: int = 5) -> Tuple[List[Tuple[str,int]], List[Tuple[str,int]]]:
    """
    Returns (neg_top, pos_top) using ratings + tags
    """
    neg_tokens = Counter()
    pos_tokens = Counter()

    for r in reviews:
        text = (r.get("text") or "")
        toks = tokenize_korean(text)

        rating = r.get("rating", None)
        try:
            rating_f = float(rating)
        except Exception:
            rating_f = None

        tags = set(r.get("tags") or [])
        is_low = (rating_f is not None and rating_f <= 2) or ("low" in tags) or ("req" in tags)
        is_pos = (rating_f is not None and rating_f >= 4) or ("pos" in tags)

        for w in toks:
            if w in HIGHLIGHT_BLACKLIST:
                continue
            if is_low:
                neg_tokens[w] += 1
            if is_pos:
                pos_tokens[w] += 1

    neg_top = [(k,v) for k,v in neg_tokens.most_common(50) if k not in HIGHLIGHT_BLACKLIST][:topk]
    pos_top = [(k,v) for k,v in pos_tokens.most_common(50) if k not in HIGHLIGHT_BLACKLIST][:topk]
    return neg_top, pos_top

def build_kw_graph(reviews_3m: List[Dict[str, Any]], topn: int = 22) -> Dict[str, Any]:
    """
    Lightweight "bubble graph": keyword frequency + polarity
    """
    cnt = Counter()
    pol = defaultdict(int)  # + for pos, - for neg

    for r in reviews_3m:
        toks = tokenize_korean(r.get("text") or "")
        rating = r.get("rating", None)
        try:
            rating_f = float(rating)
        except Exception:
            rating_f = None
        tags = set(r.get("tags") or [])
        is_low = (rating_f is not None and rating_f <= 2) or ("low" in tags) or ("req" in tags)
        is_pos = (rating_f is not None and rating_f >= 4) or ("pos" in tags)

        for w in toks:
            if w in HIGHLIGHT_BLACKLIST:
                continue
            cnt[w] += 1
            if is_pos:
                pol[w] += 1
            if is_low:
                pol[w] -= 1

    nodes = []
    for k,v in cnt.most_common(200):
        if k in HIGHLIGHT_BLACKLIST:
            continue
        if v < 3:
            continue
        nodes.append((k,v,pol[k]))
        if len(nodes) >= topn:
            break

    out_nodes = []
    for k,v,p in nodes:
        if p > 1:
            cls = "pos"
        elif p < -1:
            cls = "neg"
        else:
            cls = "neutral"
        out_nodes.append({"id": k, "label": k, "w": int(v), "pol": cls})

    return {"nodes": out_nodes, "links": []}

def build_topics_stub(reviews_3m: List[Dict[str, Any]], k: int = 8) -> List[Dict[str, Any]]:
    """
    No heavy ML here (pipeline already installs sklearn/sentence-transformers,
    but we keep builder fast & stable). We provide 'topic-like' groups by frequency.
    """
    cnt = Counter()
    for r in reviews_3m:
        for w in tokenize_korean(r.get("text") or ""):
            if w in HIGHLIGHT_BLACKLIST:
                continue
            cnt[w] += 1

    words = [w for w,_ in cnt.most_common(60)]
    topics = []
    step = max(5, len(words)//max(1,k))
    for i in range(k):
        chunk = words[i*step:(i+1)*step]
        if not chunk:
            break
        topics.append({"topic": i+1, "words": chunk[:8]})
    return topics

# ---------------------------
# Build
# ---------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Input reviews JSON (expects {'reviews':[...]}).")
    ap.add_argument("--target-days", type=int, default=7, help="Window days for dashboard (default 7).")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        raise SystemExit(f"[ERROR] input not found: {in_path}")

    all_reviews = load_reviews(in_path)
    if args.debug:
        print(f"[DEBUG] loaded reviews: {len(all_reviews)} from {in_path}")

    # normalize key casing: some sources use "Source" not "source"
    norm_reviews: List[Dict[str, Any]] = []
    parse_fail = 0

    for r in all_reviews:
        rr = dict(r)

        if "source" not in rr and "Source" in rr:
            rr["source"] = rr.get("Source")
        if "created_at" not in rr and "createdAt" in rr:
            rr["created_at"] = rr.get("createdAt")

        # normalize local asset fields
        for k in ["local_product_image", "local_review_thumb", "text_image_path"]:
            if rr.get(k):
                rr[k] = normalize_local_asset_path(rr[k])

        dt = parse_dt(rr.get("created_at",""))
        if dt is None:
            parse_fail += 1
            rr["_created_at_dt"] = None
        else:
            rr["_created_at_dt"] = dt

        norm_reviews.append(rr)

    # determine window
    now = kst_now()
    end = now.date() - timedelta(days=1)  # "yesterday"
    start = end - timedelta(days=max(1,args.target_days)-1)

    # filter last N days for main dashboard
    window_reviews: List[Dict[str, Any]] = []
    for r in norm_reviews:
        dt = r.get("_created_at_dt")
        if not isinstance(dt, datetime):
            continue
        d = dt.astimezone(KST).date()
        if start <= d <= end:
            window_reviews.append(r)

    # also build 3m window for mindmap/topics
    start_3m = (now.date() - timedelta(days=92))
    reviews_3m: List[Dict[str, Any]] = []
    for r in norm_reviews:
        dt = r.get("_created_at_dt")
        if not isinstance(dt, datetime):
            continue
        d = dt.astimezone(KST).date()
        if start_3m <= d <= end:
            reviews_3m.append(r)

    # sort by created_at ascending for "upload order"
    window_reviews.sort(key=lambda x: (x.get("_created_at_dt") or datetime(1970,1,1,tzinfo=KST)))

    neg_top, pos_top = pick_top_keywords(reviews_3m, topk=5)
    kw_graph = build_kw_graph(reviews_3m, topn=22)
    topics = build_topics_stub(reviews_3m, k=8)

    meta: Dict[str, Any] = {
        "version": "v6.2-stable-builder",
        "updated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at_kst": now.strftime("%Y.%m.%d (%a) %H:%M KST"),
        "period_text": f"최근 {args.target_days}일 ({start.strftime('%Y-%m-%d')} ~ {end.strftime('%Y-%m-%d')})",
        "period_start": start.strftime("%Y-%m-%d"),
        "period_end": end.strftime("%Y-%m-%d"),
        "total_reviews": len(window_reviews),
        "window_created_at_parse_fail": parse_fail,
        "neg_top5": [[k,int(v)] for k,v in neg_top],
        "pos_top5": [[k,int(v)] for k,v in pos_top],
        "kw_graph_3m": kw_graph,
        "ml_topics_3m": topics,
        "healthcheck": {
            "health_status": "ok" if len(window_reviews) > 0 else "warn",
            "input_total_reviews": len(norm_reviews),
            "window_total_reviews": len(window_reviews),
            "created_at_parse_fail": parse_fail,
        },
    }

    out_site = Path("site")
    out_data = out_site / "data"
    safe_mkdir(out_data)

    # write html
    (out_site / "template.html").write_text(TEMPLATE_HTML, encoding="utf-8")
    (out_site / "index.html").write_text(TEMPLATE_HTML, encoding="utf-8")

    # write data
    (out_data / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    (out_data / "reviews.json").write_text(
        json.dumps({"reviews": strip_internal_fields(window_reviews)}, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    # ensure .nojekyll (GitHub Pages asset serving)
    (out_site / ".nojekyll").write_text("", encoding="utf-8")

    print("[OK] site built:")
    print(" - site/index.html")
    print(" - site/data/meta.json")
    print(" - site/data/reviews.json")
    print(f"[INFO] window: {start} ~ {end}, reviews={len(window_reviews)}")

def strip_internal_fields(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        rr = dict(r)
        rr.pop("_created_at_dt", None)
        out.append(rr)
    return out

if __name__ == "__main__":
    main()
