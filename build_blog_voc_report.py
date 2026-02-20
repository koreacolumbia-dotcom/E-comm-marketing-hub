#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
[BUILD NAVER BLOG COLUMBIA MENTION REPORT | v2.0]
요구사항 반영:
- 긍/부정 분리/키워드 분리 없음
- "컬럼비아/콜롬비아/Columbia" 언급된 글이면 전부 수집 (본문 기준)
- 기간: 기본 60일(2개월) 넉넉히
- 리포트에서 기간(시작/끝) 선택 가능 (프론트에서 필터)

기술:
- Playwright로 네이버 블로그 검색 결과 수집 → 포스트 본문 추출(iframe/SE에디터 대응)
- 광고성/협찬/체험단 문구 감지 후 is_sponsored 마킹
- 출력:
  - reports/voc_blog/data/posts.json
  - reports/voc_blog/data/meta.json
  - reports/voc_blog/index.html (glass-card + Tailwind)
"""

import argparse
import json
import os
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import quote

from dateutil import parser as dtparser
from playwright.sync_api import sync_playwright

KST = timezone(timedelta(hours=9))

# -----------------------------
# Heuristics / Lexicons
# -----------------------------

# "컬럼비아 언급" 필터(본문 기준)
BRAND_MARKERS = [
    "콜롬비아",
    "컬럼비아",
    "Columbia",
    "COLUMBIA",
]

# 광고/협찬 감지
SPONSORED_MARKERS = [
    "광고", "협찬", "체험단", "소정의 원고료", "파트너스", "제공받아", "지원받아",
    "무상 제공", "유료 광고", "포스팅 비용", "원고료", "업체로부터",
]

# 불용어 (간단 키워드맵 용)
STOPWORDS = set("""
그리고 그러나 하지만 그래서 또한 정말 너무 진짜 약간 그냥 조금 매우 완전
제품 사용 구매 후기 리뷰 느낌 생각 정도 경우 부분 이번 오늘 어제 요즘
제가 저는 나는 우리는 여러분 때 좀 것 수 있 있다 없다 해요 했어요 입니다
ㅋㅋ ㅎㅎ ㅠㅠ ㅜㅜ
사이즈 색상 컬러 배송 가격 구매 추천 만족 불만
""".split())

WORD_RE = re.compile(r"[가-힣A-Za-z0-9]{2,}")


# -----------------------------
# Data models
# -----------------------------

@dataclass
class BlogPost:
    query: str
    title: str
    url: str
    published_at: str  # ISO (KST)
    collected_at: str  # ISO (KST)
    is_sponsored: bool
    sponsored_markers: List[str]
    has_columbia: bool
    brand_markers_found: List[str]
    raw_text_len: int
    excerpt: str
    text_preview: str  # 너무 길면 앞부분만(리포트/디버그용)


# -----------------------------
# Utilities
# -----------------------------

def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def now_kst() -> datetime:
    return datetime.now(tz=KST)

def iso(dt: datetime) -> str:
    return dt.astimezone(KST).isoformat()

def fmt_kst(dt: datetime) -> str:
    return dt.astimezone(KST).strftime("%Y.%m.%d %H:%M KST")

def safe_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def detect_sponsored(text: str) -> Tuple[bool, List[str]]:
    found = []
    t = text or ""
    for m in SPONSORED_MARKERS:
        if m in t:
            found.append(m)
    return (len(found) > 0), sorted(list(set(found)))

def detect_brand(text: str) -> Tuple[bool, List[str]]:
    found = []
    t = text or ""
    for m in BRAND_MARKERS:
        if m in t:
            found.append(m)
    found = sorted(list(set(found)))
    return (len(found) > 0), found

def keyword_map_from_texts(texts: List[str], topn: int = 40) -> List[Tuple[str, int]]:
    freq: Dict[str, int] = {}
    for t in texts:
        for w in WORD_RE.findall(t or ""):
            lw = w.lower()
            if lw in STOPWORDS:
                continue
            if len(lw) < 2:
                continue
            freq[lw] = freq.get(lw, 0) + 1
    items = sorted(freq.items(), key=lambda x: x[1], reverse=True)
    return items[:topn]

def parse_naver_date(date_text: str, fallback: datetime) -> datetime:
    """
    네이버 블로그 검색 결과 날짜 텍스트:
    - "2026.02.19."
    - "3일 전", "2시간 전", "1주 전", "1개월 전" 등
    """
    t = safe_text(date_text)
    if not t:
        return fallback

    m = re.search(r"(\d{4})\.(\d{2})\.(\d{2})", t)
    if m:
        y, mo, d = map(int, m.groups())
        return datetime(y, mo, d, 0, 0, tzinfo=KST)

    now = fallback.astimezone(KST)
    m = re.search(r"(\d+)\s*분\s*전", t)
    if m:
        return now - timedelta(minutes=int(m.group(1)))
    m = re.search(r"(\d+)\s*시간\s*전", t)
    if m:
        return now - timedelta(hours=int(m.group(1)))
    m = re.search(r"(\d+)\s*일\s*전", t)
    if m:
        return now - timedelta(days=int(m.group(1)))
    m = re.search(r"(\d+)\s*주\s*전", t)
    if m:
        return now - timedelta(weeks=int(m.group(1)))
    m = re.search(r"(\d+)\s*개월\s*전", t)
    if m:
        return now - timedelta(days=int(m.group(1)) * 30)

    return fallback


# -----------------------------
# Playwright extraction
# -----------------------------

def naver_blog_search_url(query: str, start: int = 1) -> str:
    q = quote(query)
    return f"https://search.naver.com/search.naver?where=blog&sm=tab_pge&query={q}&start={start}"

def normalize_blog_url(url: str) -> str:
    return (url or "").strip()

def get_mainframe_post_url(page) -> Optional[str]:
    try:
        frame_el = page.query_selector("iframe#mainFrame")
        if not frame_el:
            return None
        src = frame_el.get_attribute("src")
        if not src:
            return None
        if src.startswith("/"):
            return "https://blog.naver.com" + src
        return src
    except Exception:
        return None

def extract_post_text_from_dom(page) -> str:
    """
    에디터 유형별 본문 셀렉터 후보를 순회하고 가장 길게 잡히는 텍스트를 본문으로 채택
    """
    candidates = [
        ".se-main-container",
        "#postViewArea",
        "article",
    ]
    text_chunks: List[str] = []
    for sel in candidates:
        try:
            el = page.query_selector(sel)
            if el:
                t = el.inner_text(timeout=2000)
                t = safe_text(t)
                if len(t) >= 80:
                    text_chunks.append(t)
        except Exception:
            pass

    if text_chunks:
        text_chunks.sort(key=len, reverse=True)
        return text_chunks[0]

    try:
        bt = page.inner_text("body", timeout=2000)
        return safe_text(bt)
    except Exception:
        return ""

def extract_title(page) -> str:
    title = ""
    for sel in ["meta[property='og:title']", "meta[name='twitter:title']"]:
        try:
            el = page.query_selector(sel)
            if el:
                c = el.get_attribute("content")
                if c and c.strip():
                    title = safe_text(c)
                    break
        except Exception:
            pass
    if not title:
        try:
            title = safe_text(page.title())
        except Exception:
            title = ""
    return title

def scrape_one_post(context, url: str, timeout_ms: int = 25000) -> Tuple[str, str, str]:
    page = context.new_page()
    page.set_default_timeout(timeout_ms)
    final_url = url
    title = ""
    text = ""

    try:
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_timeout(700)

        mf = get_mainframe_post_url(page)
        if mf:
            page.goto(mf, wait_until="domcontentloaded")
            page.wait_for_timeout(700)
            final_url = page.url

        title = extract_title(page)
        text = extract_post_text_from_dom(page)
        return final_url or url, title, text

    finally:
        try:
            page.close()
        except Exception:
            pass

def scrape_search_results(play, queries: List[str], cutoff: datetime, per_query: int, headless: bool) -> List[Dict[str, Any]]:
    """
    네이버 검색 '블로그' 탭 결과에서 후보 URL 수집.
    """
    collected: List[Dict[str, Any]] = []

    browser = play.chromium.launch(headless=headless)
    context = browser.new_context(
        locale="ko-KR",
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"),
        viewport={"width": 1280, "height": 900},
    )

    try:
        for q in queries:
            got = 0
            start = 1
            safety_pages = 0

            while got < per_query and safety_pages < 12:
                safety_pages += 1
                url = naver_blog_search_url(q, start=start)
                page = context.new_page()
                page.set_default_timeout(20000)

                try:
                    page.goto(url, wait_until="domcontentloaded")
                    page.wait_for_timeout(600)

                    cards = page.query_selector_all("a.title_link")

                    for a in cards:
                        if got >= per_query:
                            break

                        href = normalize_blog_url(a.get_attribute("href") or "")
                        title = safe_text(a.inner_text() or "")

                        if not href.startswith("http"):
                            continue
                        if "blog.naver.com" not in href and "m.blog.naver.com" not in href and "post.naver.com" not in href:
                            continue

                        # 카드 주변에서 날짜 텍스트 추정
                        date_text = ""
                        try:
                            parent = a.evaluate_handle("node => node.closest('div, li, article')")
                            parent_el = parent.as_element() if parent else None
                            if parent_el:
                                for sel in ["span.sub_time", "span.time", "span.date", "span.sub"]:
                                    el = parent_el.query_selector(sel)
                                    if el:
                                        tt = safe_text(el.inner_text() or "")
                                        if tt:
                                            date_text = tt
                                            break
                        except Exception:
                            pass

                        pub_dt = parse_naver_date(date_text, now_kst())
                        if pub_dt < cutoff:
                            continue

                        collected.append({
                            "query": q,
                            "title": title,
                            "url": href,
                            "published_at": iso(pub_dt),
                        })
                        got += 1

                    start += 10
                finally:
                    try:
                        page.close()
                    except Exception:
                        pass

    finally:
        context.close()
        browser.close()

    # URL 중복 제거
    seen = set()
    uniq = []
    for x in collected:
        u = x["url"]
        if u in seen:
            continue
        seen.add(u)
        uniq.append(x)
    return uniq


# -----------------------------
# HTML report generator (기간 선택 가능)
# -----------------------------

def build_report_html(meta: Dict[str, Any], posts: List[Dict[str, Any]]) -> str:
    posts_json = json.dumps(posts, ensure_ascii=False)
    meta_json = json.dumps(meta, ensure_ascii=False)

    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Blog Mentions | CSK E-COMM</title>

  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">

  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200;400;600;800&display=swap');
    :root {{ --brand:#002d72; --bg0:#f6f8fb; --bg1:#eef3f9; }}
    body{{
      background: linear-gradient(180deg, var(--bg0), var(--bg1));
      font-family: 'Plus Jakarta Sans', sans-serif;
      color:#0f172a;
      min-height:100vh;
    }}
    .glass-card{{
      background: rgba(255,255,255,0.60);
      backdrop-filter: blur(18px);
      border: 1px solid rgba(255,255,255,0.75);
      border-radius: 28px;
      box-shadow: 0 20px 50px rgba(0,45,114,0.06);
    }}
    .feed-card {{
      border-radius: 20px;
      background: rgba(255,255,255,0.78);
      border: 1px solid rgba(255,255,255,0.85);
      box-shadow: 0 10px 25px rgba(0,45,114,0.05);
    }}
    .muted {{ color:#64748b; }}
    .badge {{
      display:inline-flex; align-items:center;
      padding: 3px 10px; border-radius:999px;
      font-size: 11px; font-weight: 900;
      background: rgba(15,23,42,0.06);
      color:#0f172a;
    }}
    .badge-ad {{
      background: rgba(234,179,8,0.14);
      color: rgb(161,98,7);
    }}
    .link {{
      color: var(--brand);
      font-weight: 900;
    }}
    .link:hover {{ text-decoration: underline; }}
    .kbd {{
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      font-size: 12px;
      padding: 2px 8px;
      border-radius: 10px;
      background: rgba(15,23,42,0.06);
      color: #334155;
      font-weight: 900;
    }}
    input[type="date"] {{
      background: rgba(255,255,255,0.75);
      border: 1px solid rgba(255,255,255,0.9);
      border-radius: 16px;
      padding: 10px 12px;
      font-weight: 900;
      color:#0f172a;
    }}
    .btn {{
      padding: 10px 14px;
      border-radius: 16px;
      font-weight: 900;
      background: rgba(255,255,255,0.75);
      border: 1px solid rgba(255,255,255,0.9);
      transition: all .15s ease;
    }}
    .btn:hover {{ background: #fff; }}
  </style>
</head>

<body class="p-4 md:p-8">
  <div class="max-w-7xl mx-auto">

    <!-- Header -->
    <div class="glass-card p-6 md:p-8">
      <div class="flex flex-col md:flex-row md:items-end md:justify-between gap-4">
        <div>
          <div class="text-[11px] font-black tracking-[0.3em] text-slate-400 uppercase">CSK E-COMM</div>
          <h1 class="text-3xl md:text-4xl font-black tracking-tight mt-2">
            네이버 블로그 언급 모니터링 (Columbia)
          </h1>
          <p class="muted font-semibold mt-2">
            최근 2개월치(기본) 후보를 수집한 뒤, <span class="font-black">본문에 콜롬비아/Columbia가 실제로 언급된 글만</span> 남깁니다.
          </p>
        </div>

        <div class="flex flex-wrap gap-3">
          <div class="glass-card px-4 py-3 rounded-2xl">
            <div class="text-[10px] font-black tracking-widest text-slate-500 uppercase">Update</div>
            <div id="updatedAt" class="text-[12px] font-black text-slate-700 mt-1">-</div>
          </div>
          <div class="glass-card px-4 py-3 rounded-2xl">
            <div class="text-[10px] font-black tracking-widest text-slate-500 uppercase">Collected Period</div>
            <div id="periodText" class="text-[12px] font-black text-slate-700 mt-1">-</div>
          </div>
        </div>
      </div>

      <!-- Controls -->
      <div class="mt-6 feed-card p-4">
        <div class="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-3">
          <div class="flex flex-wrap items-center gap-3">
            <div class="text-sm font-black">기간 선택</div>
            <input id="dateStart" type="date" />
            <div class="muted font-black">~</div>
            <input id="dateEnd" type="date" />
            <button id="btnApply" class="btn">적용</button>
            <button id="btnReset" class="btn">최근 60일</button>
          </div>

          <div class="flex flex-wrap items-center gap-2">
            <button id="btnHideSponsored" class="btn">
              협찬 숨기기
            </button>
            <span class="kbd">COL-MENTION</span>
          </div>
        </div>

        <div class="mt-3 text-xs font-semibold muted">
          * 이 화면의 기간 필터는 <span class="font-black">이미 수집된 60일 데이터 안에서</span>만 동작합니다.
        </div>
      </div>
    </div>

    <!-- Main grid -->
    <div class="grid grid-cols-1 lg:grid-cols-12 gap-6 mt-6">

      <!-- Left: Summary -->
      <aside class="lg:col-span-4 space-y-6">
        <div class="glass-card p-6">
          <div class="flex items-center justify-between">
            <div class="text-lg font-black">요약</div>
            <div class="kbd">BLOG</div>
          </div>

          <div class="mt-4 grid grid-cols-2 gap-3">
            <div class="feed-card p-4">
              <div class="text-[10px] font-black tracking-widest text-slate-500 uppercase">Posts</div>
              <div id="cntPosts" class="text-2xl font-black mt-1">-</div>
            </div>
            <div class="feed-card p-4">
              <div class="text-[10px] font-black tracking-widest text-slate-500 uppercase">Sponsored</div>
              <div id="cntSponsored" class="text-2xl font-black mt-1">-</div>
            </div>
          </div>

          <div class="mt-5">
            <div class="text-sm font-black">Top Words (본문 기반)</div>
            <div class="muted text-xs font-semibold mt-1">
              키워드맵은 긍/부정 분류가 아닌, <span class="font-black">언급 글의 빈출 단어</span>입니다.
            </div>
            <div class="mt-3 flex flex-wrap gap-2" id="wordChips"></div>
          </div>
        </div>

        <div class="glass-card p-6">
          <div class="text-lg font-black">가이드</div>
          <ul class="mt-3 space-y-2 text-sm font-semibold muted">
            <li>• 카드 클릭 → 원문 새 탭 + 아래 상세로 스크롤</li>
            <li>• “협찬 숨기기”로 내돈내산 성향 글만 보정 가능</li>
            <li>• 기간은 프론트 필터(빠름). 수집 기간 자체를 늘리려면 파이썬 실행 시 days를 늘려</li>
          </ul>
        </div>
      </aside>

      <!-- Right: Feed + Detail -->
      <section class="lg:col-span-8 space-y-6">

        <div class="glass-card p-6">
          <div class="flex items-end justify-between gap-3">
            <div>
              <div class="text-xl font-black">Feed</div>
              <div class="muted text-sm font-semibold mt-1">Columbia 언급 블로그 글 리스트</div>
            </div>
            <div class="text-sm font-black muted">
              정렬: <span class="kbd">최근 → 과거</span>
            </div>
          </div>

          <div class="mt-5 grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4" id="feedGrid"></div>
        </div>

        <div class="glass-card p-6">
          <div class="text-xl font-black">상세</div>
          <div class="muted text-sm font-semibold mt-1">본문 미리보기(앞부분) + 메타 확인</div>
          <div class="mt-5 space-y-4" id="detailList"></div>
        </div>

      </section>
    </div>
  </div>

<script>
  const META = {meta_json};
  const POSTS = {posts_json};

  const $ = (id) => document.getElementById(id);

  function escapeHtml(s) {{
    return String(s||"")
      .replaceAll("&","&amp;")
      .replaceAll("<","&lt;")
      .replaceAll(">","&gt;")
      .replaceAll('"',"&quot;")
      .replaceAll("'","&#039;");
  }}

  function toDateOnly(iso) {{
    try {{
      const d = new Date(iso);
      const y = d.getFullYear();
      const m = String(d.getMonth()+1).padStart(2,"0");
      const da = String(d.getDate()).padStart(2,"0");
      return `${{y}}-${{m}}-${{da}}`;
    }} catch (e) {{
      return "";
    }}
  }}

  function fmtDate(iso) {{
    const d = toDateOnly(iso);
    if (!d) return "-";
    return d.replaceAll("-", ".");
  }}

  function parseDateInput(v) {{
    // v: YYYY-MM-DD
    if (!v) return null;
    const t = new Date(v + "T00:00:00");
    if (isNaN(t.getTime())) return null;
    return t;
  }}

  function inRange(postIso, start, end) {{
    const d = new Date(postIso);
    if (isNaN(d.getTime())) return false;
    if (start && d < start) return false;
    if (end) {{
      // end inclusive
      const end2 = new Date(end.getTime());
      end2.setHours(23,59,59,999);
      if (d > end2) return false;
    }}
    return true;
  }}

  let state = {{
    hideSponsored: false,
    start: null,
    end: null,
    word: null,
  }};

  function filteredPosts() {{
    return POSTS.filter(p => {{
      if (state.hideSponsored && p.is_sponsored) return false;
      if (!inRange(p.published_at, state.start, state.end)) return false;

      if (!state.word) return true;
      const w = state.word.toLowerCase();
      const t = (p.text_preview || "").toLowerCase();
      const title = (p.title || "").toLowerCase();
      return t.includes(w) || title.includes(w);
    }});
  }}

  function renderHeader() {{
    $("updatedAt").textContent = META.updated_at_kst || "-";
    $("periodText").textContent = META.period_text || "-";
  }}

  function renderCounts() {{
    const list = filteredPosts();
    let sponsored = 0;
    list.forEach(p => {{ if (p.is_sponsored) sponsored += 1; }});
    $("cntPosts").textContent = list.length;
    $("cntSponsored").textContent = sponsored;
  }}

  function renderWords() {{
    const box = $("wordChips");
    box.innerHTML = "";
    const words = (META.top_words || []).slice(0, 18);
    words.forEach(([w,c]) => {{
      const el = document.createElement("div");
      el.className = "badge cursor-pointer hover:opacity-80";
      el.innerHTML = `${{escapeHtml(w)}} · ${{c}}`;
      el.addEventListener("click", () => {{
        state.word = w;
        renderAll();
      }});
      box.appendChild(el);
    }});

    // clear word chip
    const clear = document.createElement("div");
    clear.className = "badge cursor-pointer hover:opacity-80";
    clear.innerHTML = "단어 필터 해제";
    clear.addEventListener("click", () => {{
      state.word = null;
      renderAll();
    }});
    box.appendChild(clear);
  }}

  function renderFeed() {{
    const grid = $("feedGrid");
    grid.innerHTML = "";
    const list = filteredPosts()
      .slice()
      .sort((a,b) => (b.published_at||"").localeCompare(a.published_at||""));

    if (list.length === 0) {{
      grid.innerHTML = `<div class="muted font-semibold">조건에 맞는 글이 없습니다.</div>`;
      return;
    }}

    list.forEach((p, idx) => {{
      const card = document.createElement("div");
      card.className = "feed-card p-4 cursor-pointer hover:translate-y-[-1px] transition";

      const badges = [];
      if (p.is_sponsored) badges.push(`<span class="badge badge-ad">협찬</span>`);
      const markers = (p.brand_markers_found || []).slice(0,3).join(", ");
      if (markers) badges.push(`<span class="badge">언급: ${{escapeHtml(markers)}}</span>`);

      card.innerHTML = `
        <div class="flex items-start justify-between gap-2">
          <div class="font-black text-base leading-snug">
            ${{escapeHtml(p.title || "(제목 없음)")}}
          </div>
          <div class="text-[11px] font-black muted whitespace-nowrap">
            ${{fmtDate(p.published_at)}}
          </div>
        </div>

        <div class="mt-2 flex flex-wrap gap-2">${{badges.join("")}}</div>

        <div class="mt-3 text-sm font-semibold muted line-clamp-4">
          ${{escapeHtml(p.excerpt || "")}}
        </div>

        <div class="mt-3 text-[12px] font-black">
          <span class="muted">query:</span> ${{escapeHtml(p.query)}}
        </div>
      `;

      card.addEventListener("click", () => {{
        window.open(p.url, "_blank", "noopener,noreferrer");
        const target = document.getElementById("detail-" + idx);
        if (target) target.scrollIntoView({{ behavior:"smooth", block:"start" }});
      }});

      grid.appendChild(card);
    }});
  }}

  function renderDetails() {{
    const box = $("detailList");
    box.innerHTML = "";

    const list = filteredPosts()
      .slice()
      .sort((a,b) => (b.published_at||"").localeCompare(a.published_at||""));

    list.forEach((p, idx) => {{
      const wrap = document.createElement("div");
      wrap.id = "detail-" + idx;
      wrap.className = "feed-card p-5";

      const sponsoredLine = p.is_sponsored
        ? `<div class="mt-2 text-xs font-black badge badge-ad">협찬/광고 감지: ${{escapeHtml((p.sponsored_markers||[]).join(", "))}}</div>`
        : ``;

      wrap.innerHTML = `
        <div class="flex flex-col md:flex-row md:items-start md:justify-between gap-3">
          <div class="flex-1">
            <div class="text-lg font-black leading-snug">
              ${{escapeHtml(p.title || "(제목 없음)")}}
            </div>
            <div class="mt-2 text-sm font-semibold muted">
              발행: <span class="font-black">${{fmtDate(p.published_at)}}</span>
              <span class="mx-2">·</span>
              query: <span class="font-black">${{escapeHtml(p.query)}}</span>
              <span class="mx-2">·</span>
              언급: <span class="font-black">${{escapeHtml((p.brand_markers_found||[]).join(", ")) || "-"}}</span>
            </div>
            ${{sponsoredLine}}
          </div>

          <a class="link text-sm" href="${{escapeHtml(p.url)}}" target="_blank" rel="noopener noreferrer">
            원문 열기 <i class="fa-solid fa-arrow-up-right-from-square"></i>
          </a>
        </div>

        <div class="mt-4">
          <div class="text-sm font-black">본문 미리보기</div>
          <div class="mt-2 text-sm font-semibold muted whitespace-pre-line">
            ${{escapeHtml(p.text_preview || "")}}
          </div>
        </div>
      `;

      box.appendChild(wrap);
    }});
  }}

  function initDateControls() {{
    // 기본: 최근 60일
    const all = POSTS.slice().sort((a,b)=> (b.published_at||"").localeCompare(a.published_at||""));
    const newest = all[0]?.published_at || null;
    const newestDate = newest ? new Date(newest) : new Date();

    const end = new Date(newestDate.getTime());
    const start = new Date(newestDate.getTime());
    start.setDate(start.getDate() - 59);

    $("dateStart").value = toDateOnly(start.toISOString());
    $("dateEnd").value = toDateOnly(end.toISOString());

    state.start = parseDateInput($("dateStart").value);
    state.end = parseDateInput($("dateEnd").value);

    $("btnApply").addEventListener("click", () => {{
      state.start = parseDateInput($("dateStart").value);
      state.end = parseDateInput($("dateEnd").value);
      renderAll();
    }});

    $("btnReset").addEventListener("click", () => {{
      $("dateStart").value = toDateOnly(start.toISOString());
      $("dateEnd").value = toDateOnly(end.toISOString());
      state.start = parseDateInput($("dateStart").value);
      state.end = parseDateInput($("dateEnd").value);
      renderAll();
    }});

    $("btnHideSponsored").addEventListener("click", () => {{
      state.hideSponsored = !state.hideSponsored;
      $("btnHideSponsored").textContent = state.hideSponsored ? "협찬 보이기" : "협찬 숨기기";
      renderAll();
    }});
  }}

  function renderAll() {{
    renderHeader();
    renderCounts();
    renderWords();
    renderFeed();
    renderDetails();
  }}

  renderHeader();
  initDateControls();
  renderAll();
</script>
</body>
</html>
"""


# -----------------------------
# Main pipeline
# -----------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--queries", nargs="+", required=True, help="네이버 블로그 검색어 리스트")
    ap.add_argument("--days", type=int, default=60, help="최근 N일(기본 60일)")
    ap.add_argument("--per-query", type=int, default=40, help="검색어당 후보 글 수(필터링 고려해 넉넉히)")
    ap.add_argument("--headless", action="store_true", help="headless 모드")
    ap.add_argument("--out-dir", default=os.path.join("reports", "voc_blog"), help="출력 디렉토리")
    ap.add_argument("--require-brand", action="store_true", default=True,
                    help="본문에 콜롬비아/Columbia 언급이 반드시 있어야 포함(기본 True)")
    args = ap.parse_args()

    out_dir = args.out_dir
    data_dir = os.path.join(out_dir, "data")
    ensure_dir(data_dir)

    collected_at = now_kst()
    cutoff = collected_at - timedelta(days=args.days - 1)

    with sync_playwright() as play:
        # 1) Search candidates
        candidates = scrape_search_results(
            play=play,
            queries=args.queries,
            cutoff=cutoff,
            per_query=args.per_query,
            headless=args.headless
        )

        # 2) Scrape posts
        browser = play.chromium.launch(headless=args.headless)
        context = browser.new_context(
            locale="ko-KR",
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"),
            viewport={"width": 1280, "height": 900},
        )

        posts: List[BlogPost] = []
        try:
            for c in candidates:
                q = c["query"]
                base_title = c.get("title") or ""
                url = c["url"]
                pub_iso = c.get("published_at") or iso(collected_at)

                try:
                    pub_dt = dtparser.isoparse(pub_iso).astimezone(KST)
                except Exception:
                    pub_dt = collected_at

                if pub_dt < cutoff:
                    continue

                final_url, page_title, text = scrape_one_post(context, url=url)
                text = text or ""
                page_title = safe_text(page_title) if page_title else ""
                title = page_title or base_title or "(제목 없음)"

                has_brand, brand_found = detect_brand(title + " " + text)
                if args.require_brand and not has_brand:
                    continue

                is_sp, sp_markers = detect_sponsored(title + " " + text)

                # excerpt/preview
                cleaned = safe_text(text)
                excerpt = cleaned[:160] if cleaned else "(본문 추출 실패)"
                preview = cleaned[:1200] if cleaned else "(본문 추출 실패)"

                posts.append(BlogPost(
                    query=q,
                    title=title,
                    url=final_url or url,
                    published_at=iso(pub_dt),
                    collected_at=iso(collected_at),
                    is_sponsored=is_sp,
                    sponsored_markers=sp_markers,
                    has_columbia=has_brand,
                    brand_markers_found=brand_found,
                    raw_text_len=len(text),
                    excerpt=excerpt,
                    text_preview=preview,
                ))

        finally:
            context.close()
            browser.close()

    # 3) Meta + word map (본문 기반, 분류 아님)
    texts_for_words = [p.text_preview for p in posts if p.text_preview]
    top_words = keyword_map_from_texts(texts_for_words, topn=40)

    cnt_posts = len(posts)
    cnt_sponsored = sum(1 for p in posts if p.is_sponsored)

    meta = {
        "updated_at_kst": fmt_kst(collected_at),
        "period_text": f"최근 {args.days}일 ({cutoff.strftime('%Y.%m.%d')} ~ {collected_at.strftime('%Y.%m.%d')})",
        "days": args.days,
        "queries": args.queries,
        "counts": {
            "posts": cnt_posts,
            "sponsored_posts": cnt_sponsored,
        },
        "top_words": top_words,
        "brand_markers": BRAND_MARKERS,
        "note": "이 리포트는 긍/부정 분류가 아니라, Columbia 언급 블로그 포스트 수집/필터링 결과입니다.",
    }

    posts_dicts = [asdict(p) for p in posts]

    # 4) Write JSON
    with open(os.path.join(data_dir, "posts.json"), "w", encoding="utf-8") as f:
        json.dump({"posts": posts_dicts}, f, ensure_ascii=False, indent=2)

    with open(os.path.join(data_dir, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    # 5) Write HTML
    html = build_report_html(meta=meta, posts=posts_dicts)
    with open(os.path.join(out_dir, "index.html"), "w", encoding="utf-8") as f:
        f.write(html)

    print("[OK] Blog mention report generated:")
    print(f" - {os.path.join(out_dir, 'index.html')}")
    print(f" - {os.path.join(data_dir, 'posts.json')}")
    print(f" - {os.path.join(data_dir, 'meta.json')}")

if __name__ == "__main__":
    main()
