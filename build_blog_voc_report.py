#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
[FINAL] NAVER BLOG VOC (Columbia mentions) | v3.0

목표
- 네이버 블로그에서 "컬럼비아/콜롬비아/Columbia" 언급 글을 60일(기본)치 수집
- 긍/부정 분류 없음: 언급 기반으로 모두 수집
- 리포트에서 기간(시작/끝) 선택 필터 제공
- GitHub Actions headless 환경에서 0건 방지 위해:
  - 후보 수집은 Naver OpenAPI(blog search) 사용 (필수: NAVER_CLIENT_ID/SECRET)
  - 본문/제목/OG 추출은 Playwright 사용

출력
- <out_dir>/index.html
- <out_dir>/data/posts.json
- <out_dir>/data/meta.json

실행 예시
python build_blog_voc_report.py \
  --queries "콜롬비아" "컬럼비아" "Columbia" "콜롬비아 패딩" "콜롬비아 자켓" \
  --days 60 --per-query 100 --out-dir reports/voc_blog --headless
"""

import argparse
import json
import os
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urlencode

import requests
from dateutil import parser as dtparser
from playwright.sync_api import sync_playwright


KST = timezone(timedelta(hours=9))

# ============== Settings ==============

BRAND_MARKERS = ["콜롬비아", "컬럼비아", "Columbia", "COLUMBIA"]

SPONSORED_MARKERS = [
    "광고", "협찬", "체험단", "소정의 원고료", "파트너스", "제공받아", "지원받아",
    "무상 제공", "유료 광고", "포스팅 비용", "원고료", "업체로부터",
]

STOPWORDS = set("""
그리고 그러나 하지만 그래서 또한 정말 너무 진짜 약간 그냥 조금 매우 완전
제품 사용 구매 후기 리뷰 느낌 생각 정도 경우 부분 이번 오늘 어제 요즘
제가 저는 나는 우리는 여러분 때 좀 것 수 있 있다 없다 해요 했어요 입니다
ㅋㅋ ㅎㅎ ㅠㅠ ㅜㅜ
사이즈 색상 컬러 배송 가격 구매 추천 만족 불만
""".split())

WORD_RE = re.compile(r"[가-힣A-Za-z0-9]{2,}")

# ====================================


@dataclass
class BlogPost:
    query: str
    title: str
    url: str
    published_at: str  # ISO KST
    collected_at: str  # ISO KST
    is_sponsored: bool
    sponsored_markers: List[str]
    has_columbia: bool
    brand_markers_found: List[str]
    raw_text_len: int
    excerpt: str
    text_preview: str
    source: str  # "naver_api+playwright"


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def now_kst() -> datetime:
    return datetime.now(tz=KST)


def iso(dt: datetime) -> str:
    return dt.astimezone(KST).isoformat()


def safe_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def fmt_kst(dt: datetime) -> str:
    return dt.astimezone(KST).strftime("%Y.%m.%d %H:%M KST")


def strip_html(s: str) -> str:
    return safe_text(re.sub(r"<[^>]+>", "", s or ""))


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


# ==========================
# Naver OpenAPI candidates
# ==========================

def fetch_candidates_via_naver_api(
    queries: List[str],
    per_query: int,
    sort: str = "date",
) -> List[Dict[str, Any]]:
    """
    Naver Blog Search OpenAPI:
    - https://openapi.naver.com/v1/search/blog.json
    Note: API에 정확한 published date 필드가 없을 수 있어 후보 단계에서 느슨하게 잡고,
          실제 페이지 scrape 후 published_at을 확정/필터링한다.
    """
    cid = os.getenv("NAVER_CLIENT_ID", "").strip()
    csec = os.getenv("NAVER_CLIENT_SECRET", "").strip()
    if not cid or not csec:
        raise RuntimeError("Missing NAVER_CLIENT_ID / NAVER_CLIENT_SECRET env")

    headers = {
        "X-Naver-Client-Id": cid,
        "X-Naver-Client-Secret": csec,
    }

    collected: List[Dict[str, Any]] = []

    for q in queries:
        got = 0
        start = 1
        # start 최대 1000
        while got < per_query and start <= 1000:
            display = min(100, per_query - got)
            params = {
                "query": q,
                "display": display,
                "start": start,
                "sort": sort,
            }
            url = "https://openapi.naver.com/v1/search/blog.json?" + urlencode(params)
            r = requests.get(url, headers=headers, timeout=25)
            r.raise_for_status()
            data = r.json()

            items = data.get("items", []) or []
            if not items:
                break

            for it in items:
                link = (it.get("link") or "").strip()
                title = strip_html(it.get("title") or "")
                desc = strip_html(it.get("description") or "")

                if not link.startswith("http"):
                    continue

                collected.append({
                    "query": q,
                    "title": title or "(제목 없음)",
                    "url": link,
                    "snippet": desc,
                })
                got += 1
                if got >= per_query:
                    break

            start += display

    # URL dedup
    seen = set()
    uniq = []
    for x in collected:
        u = x["url"]
        if u in seen:
            continue
        seen.add(u)
        uniq.append(x)
    return uniq


# ==========================
# Playwright extractors
# ==========================

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


def extract_title(page) -> str:
    for sel in ["meta[property='og:title']", "meta[name='twitter:title']"]:
        try:
            el = page.query_selector(sel)
            if el:
                c = el.get_attribute("content")
                if c and c.strip():
                    return safe_text(c)
        except Exception:
            pass
    try:
        return safe_text(page.title())
    except Exception:
        return ""


def extract_published_from_dom(page) -> Optional[datetime]:
    """
    네이버 블로그 발행일은 템플릿마다 달라서 후보 셀렉터 여러개 시도.
    실패하면 None.
    """
    selectors = [
        "span.se_publishDate",
        "p.date",
        "span.date",
        "span#se_publishDate",
        "time",
        "meta[property='article:published_time']",
    ]
    for sel in selectors:
        try:
            el = page.query_selector(sel)
            if not el:
                continue
            if sel.startswith("meta"):
                v = el.get_attribute("content") or ""
            else:
                v = el.inner_text() or ""
            v = safe_text(v)
            if not v:
                continue
            # dateutil로 최대한 파싱
            try:
                dt = dtparser.parse(v)
                if not dt.tzinfo:
                    dt = dt.replace(tzinfo=KST)
                return dt.astimezone(KST)
            except Exception:
                continue
        except Exception:
            continue
    return None


def extract_post_text_from_dom(page) -> str:
    candidates = [
        ".se-main-container",
        "#postViewArea",
        "article",
        "div#contentArea",
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
        return safe_text(page.inner_text("body", timeout=2000))
    except Exception:
        return ""


def scrape_one_post(context, url: str, timeout_ms: int = 25000) -> Tuple[str, str, str, Optional[datetime]]:
    page = context.new_page()
    page.set_default_timeout(timeout_ms)
    final_url = url
    title = ""
    text = ""
    pub_dt = None

    try:
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_timeout(700)

        mf = get_mainframe_post_url(page)
        if mf:
            page.goto(mf, wait_until="domcontentloaded")
            page.wait_for_timeout(700)
            final_url = page.url

        title = extract_title(page)
        pub_dt = extract_published_from_dom(page)
        text = extract_post_text_from_dom(page)
        return final_url or url, title, text, pub_dt

    finally:
        try:
            page.close()
        except Exception:
            pass


# ==========================
# HTML report (same UI)
# ==========================

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

    <div class="glass-card p-6 md:p-8">
      <div class="flex flex-col md:flex-row md:items-end md:justify-between gap-4">
        <div>
          <div class="text-[11px] font-black tracking-[0.3em] text-slate-400 uppercase">CSK E-COMM</div>
          <h1 class="text-3xl md:text-4xl font-black tracking-tight mt-2">
            네이버 블로그 언급 모니터링 (Columbia)
          </h1>
          <p class="muted font-semibold mt-2">
            최근 2개월치(기본) 후보를 수집한 뒤, <span class="font-black">본문/제목에 콜롬비아/Columbia가 실제로 언급된 글만</span> 남깁니다.
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
            <button id="btnHideSponsored" class="btn">협찬 숨기기</button>
            <span class="kbd">COL-MENTION</span>
          </div>
        </div>

        <div class="mt-3 text-xs font-semibold muted">
          * 이 화면의 기간 필터는 <span class="font-black">이미 수집된 {meta.get("days", 60)}일 데이터 안에서</span>만 동작합니다.
          <span class="ml-2">| candidates: <span class="font-black">{meta.get("debug",{}).get("candidates_total","-")}</span></span>
          <span class="ml-2">| saved: <span class="font-black">{meta.get("counts",{}).get("posts","-")}</span></span>
        </div>
      </div>
    </div>

    <div class="grid grid-cols-1 lg:grid-cols-12 gap-6 mt-6">
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
            <li>• 수집 커버리지 개선: 쿼리/ per-query를 늘려서 후보를 넓히면 됨</li>
          </ul>
        </div>
      </aside>

      <section class="lg:col-span-8 space-y-6">
        <div class="glass-card p-6">
          <div class="flex items-end justify-between gap-3">
            <div>
              <div class="text-xl font-black">Feed</div>
              <div class="muted text-sm font-semibold mt-1">Columbia 언급 블로그 글 리스트</div>
            </div>
            <div class="text-sm font-black muted">정렬: <span class="kbd">최근 → 과거</span></div>
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


# ==========================
# main
# ==========================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--queries", nargs="+", required=True, help="블로그 검색어 리스트")
    ap.add_argument("--days", type=int, default=60, help="최근 N일(기본 60)")
    ap.add_argument("--per-query", type=int, default=100, help="쿼리당 후보 수(기본 100)")
    ap.add_argument("--headless", action="store_true", help="headless 모드")
    ap.add_argument("--out-dir", default=os.path.join("reports", "voc_blog"), help="출력 디렉토리")
    ap.add_argument("--require-brand", action="store_true", default=True, help="브랜드 언급 필수(기본 True)")
    args = ap.parse_args()

    out_dir = args.out_dir
    data_dir = os.path.join(out_dir, "data")
    ensure_dir(data_dir)

    collected_at = now_kst()
    cutoff = collected_at - timedelta(days=args.days - 1)

    # 1) candidates via OpenAPI
    candidates = fetch_candidates_via_naver_api(args.queries, per_query=args.per_query, sort="date")
    print(f"[INFO] candidates_total={len(candidates)} (per_query={args.per_query}) cutoff={cutoff.isoformat()}")

    # 2) scrape content via Playwright
    posts: List[BlogPost] = []
    debug = {
        "candidates_total": len(candidates),
        "scraped_ok": 0,
        "scraped_fail": 0,
        "brand_kept": 0,
        "cutoff_filtered": 0,
        "empty_text": 0,
    }

    with sync_playwright() as play:
        browser = play.chromium.launch(headless=args.headless)
        context = browser.new_context(
            locale="ko-KR",
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"),
            viewport={"width": 1280, "height": 900},
        )
        try:
            for c in candidates:
                q = c["query"]
                url = c["url"]
                base_title = c.get("title") or "(제목 없음)"
                snippet = c.get("snippet") or ""

                try:
                    final_url, page_title, text, pub_dt = scrape_one_post(context, url=url)
                    debug["scraped_ok"] += 1
                except Exception:
                    debug["scraped_fail"] += 1
                    continue

                title = safe_text(page_title) if page_title else safe_text(base_title)
                text = text or ""
                if len(safe_text(text)) == 0:
                    debug["empty_text"] += 1

                # 발행일 필터: pub_dt 추출 실패하면 "수집일"로 대체(너무 엄격하면 0됨)
                if pub_dt is None:
                    pub_dt = collected_at
                else:
                    if pub_dt < cutoff:
                        debug["cutoff_filtered"] += 1
                        continue

                # 브랜드 언급 판단: 본문이 비면(추출 실패) title+snippet에서라도 통과시켜 0 방지
                combined_for_brand = f"{title} {text} {snippet}"
                has_brand, brand_found = detect_brand(combined_for_brand)

                if args.require_brand and not has_brand:
                    continue

                debug["brand_kept"] += 1

                is_sp, sp_markers = detect_sponsored(f"{title} {text} {snippet}")

                cleaned = safe_text(text) if text else ""
                excerpt = cleaned[:160] if cleaned else safe_text(snippet)[:160] if snippet else "(본문 추출 실패)"
                preview = cleaned[:1200] if cleaned else safe_text(snippet)[:1200] if snippet else "(본문 추출 실패)"

                posts.append(BlogPost(
                    query=q,
                    title=title or "(제목 없음)",
                    url=final_url or url,
                    published_at=iso(pub_dt),
                    collected_at=iso(collected_at),
                    is_sponsored=is_sp,
                    sponsored_markers=sp_markers,
                    has_columbia=has_brand,
                    brand_markers_found=brand_found,
                    raw_text_len=len(text or ""),
                    excerpt=excerpt,
                    text_preview=preview,
                    source="naver_api+playwright",
                ))

        finally:
            context.close()
            browser.close()

    print(f"[INFO] posts_saved={len(posts)} debug={debug}")

    # 3) meta
    top_words = keyword_map_from_texts([p.text_preview for p in posts], topn=40)

    meta = {
        "updated_at_kst": fmt_kst(collected_at),
        "period_text": f"최근 {args.days}일 ({cutoff.strftime('%Y.%m.%d')} ~ {collected_at.strftime('%Y.%m.%d')})",
        "days": args.days,
        "queries": args.queries,
        "counts": {
            "posts": len(posts),
            "sponsored_posts": sum(1 for p in posts if p.is_sponsored),
        },
        "top_words": top_words,
        "brand_markers": BRAND_MARKERS,
        "note": "긍/부정 분류가 아닌 Columbia 언급 블로그 포스트 수집/필터링 결과입니다.",
        "debug": debug,
    }

    posts_dicts = [asdict(p) for p in posts]

    # 4) write outputs
    with open(os.path.join(data_dir, "posts.json"), "w", encoding="utf-8") as f:
        json.dump({"posts": posts_dicts}, f, ensure_ascii=False, indent=2)

    with open(os.path.join(data_dir, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    html = build_report_html(meta=meta, posts=posts_dicts)
    with open(os.path.join(out_dir, "index.html"), "w", encoding="utf-8") as f:
        f.write(html)

    print("[OK] Blog report generated:")
    print(f" - {os.path.join(out_dir, 'index.html')}")
    print(f" - {os.path.join(data_dir, 'posts.json')}")
    print(f" - {os.path.join(data_dir, 'meta.json')}")


if __name__ == "__main__":
    main()
