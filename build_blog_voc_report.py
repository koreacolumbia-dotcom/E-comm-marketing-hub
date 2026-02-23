#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
[FINAL - HOTFIX] NAVER BLOG VOC (Columbia brand mentions) | v3.1

✅ 응급 개선 포인트
1) '콜롬비아' = 국가/커피/환율/COP 등 노이즈를 컨텍스트 기반으로 강하게 제거
2) 브랜드 확정 신호(컬럼비아/Columbia/기술키워드/SKU/아웃도어 컨텍스트)만 통과
3) 출력 JSON에 alias 키 추가(publishedAt/collectedAt/items) -> 프론트 0 표시 방어

출력
- <out_dir>/index.html
- <out_dir>/data/posts.json
- <out_dir>/data/meta.json

필수 env (GitHub Actions에서 step env로 주입 필요)
- NAVER_CLIENT_ID
- NAVER_CLIENT_SECRET
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

# =======================
# Brand rules (핵심)
# =======================

# 브랜드 확정 신호(이게 나오면 컨텍스트 없이도 통과)
BRAND_STRONG = [
    "컬럼비아", "Columbia", "COLUMBIA",
    "Columbia Sportswear", "sportswear",
    "콜롬비아코리아", "columbiakorea", "columbiakorea.co.kr",
    # Columbia 고유/대표 테크 키워드 (가능한 한 넓게)
    "옴니히트", "omni-heat", "omni heat", "omniheat",
    "옴니위크", "omni-wick", "omni wick", "omniwick",
    "아웃드라이", "outdry",
    "터보다운", "turbo down", "turbodown",
    "인터체인지", "interchange",
    "타이타늄", "titanium",
]

# '콜롬비아'가 브랜드인지 판단할 때 필요한 패션/아웃도어 컨텍스트
OUTDOOR_CONTEXT = [
    "자켓", "재킷", "패딩", "다운", "후리스", "플리스", "바람막이", "윈드브레이커",
    "등산", "트레킹", "하이킹", "캠핑", "아웃도어", "방수", "방풍", "발수", "보온", "보냉",
    "베스트", "조끼", "팬츠", "바지", "레깅스", "티셔츠", "셔츠",
    "모자", "캡", "비니", "장갑", "가방", "백팩",
    "신발", "부츠", "트레킹화", "등산화",
    "사이즈", "핏", "착용", "착샷", "코디",
    "매장", "구매", "구입", "세일", "할인",
]

# 강력 제외 컨텍스트 (이거 나오면 '콜롬비아'는 거의 100% 국가/원두/환율/영화)
NEGATIVE_CONTEXT = [
    # 국가/경제/환율
    "페소", "COP", "환율", "관세", "대사관", "보고타", "남미", "국경", "마약", "카르텔",
    "과세환율", "수출", "수입", "무역", "통관",
    # 커피/원두
    "원두", "커피", "핸드드립", "드립", "에스프레소", "로스팅", "게이샤", "바리스타",
    # 콘텐츠/영화/사건
    "줄거리", "결말", "영화", "드라마", "넷플릭스", "해킹", "로그인", "접속",
]

# SKU 패턴 (콜럼비아 품번 유사)
SKU_PATTERNS = [
    re.compile(r"\bC[0-9A-Z]{6,}\b"),   # 예: C66YM9726MUL
    re.compile(r"\b[0-9A-Z]{2,}-[0-9A-Z]{2,}\b"),  # 범용(필요시)
]

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


# =======================
# Helpers
# =======================

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


# =======================
# Brand decision (핵심 로직)
# =======================

def has_any(patterns: List[str], text: str) -> List[str]:
    found = []
    t = text or ""
    for p in patterns:
        if p and p in t:
            found.append(p)
    return sorted(list(set(found)))

def has_sku(text: str) -> bool:
    t = text or ""
    for rgx in SKU_PATTERNS:
        if rgx.search(t):
            return True
    return False

def contains_any_keywords(text: str, kws: List[str]) -> bool:
    t = text or ""
    return any(k in t for k in kws)

def is_brand_columbia(title: str, body: str, snippet: str) -> Tuple[bool, List[str], str]:
    """
    Returns:
      (is_brand, markers_found, reason)
    """
    title = title or ""
    body = body or ""
    snippet = snippet or ""

    combined = f"{title}\n{snippet}\n{body}"
    combined_l = combined.lower()

    # 0) 강력 제외 컨텍스트가 있으면 '콜롬비아'는 국가/원두/환율일 확률이 너무 높음
    # 단, 브랜드 확정 신호가 동시에 있으면 브랜드로 본다(우선순위: strong > negative).
    strong_found = has_any(BRAND_STRONG, combined) + has_any([s.lower() for s in BRAND_STRONG], combined_l)
    strong_found = sorted(list(set([x for x in strong_found if x])))

    if not strong_found:
        if contains_any_keywords(combined, NEGATIVE_CONTEXT) or contains_any_keywords(combined_l, [x.lower() for x in NEGATIVE_CONTEXT]):
            return False, [], "negative_context"

    # 1) 브랜드 확정 신호 -> 즉시 통과
    if strong_found:
        return True, strong_found[:6], "strong_marker"

    # 2) 여기까지 왔다는 건 "콜롬비아"만으로 걸린 케이스일 가능성 큼
    #    -> 아웃도어 컨텍스트 또는 SKU가 있어야 브랜드로 인정
    #    -> '콜롬비아' 텍스트가 아예 없으면 의미 없음
    if ("콜롬비아" not in combined) and ("columbia" not in combined_l):
        return False, [], "no_brand_token"

    if has_sku(combined):
        return True, ["SKU"], "sku_match"

    if contains_any_keywords(combined, OUTDOOR_CONTEXT) or contains_any_keywords(combined_l, [x.lower() for x in OUTDOOR_CONTEXT]):
        return True, ["context"], "outdoor_context"

    return False, [], "no_context"


# ==========================
# Naver OpenAPI candidates
# ==========================

def fetch_candidates_via_naver_api(queries: List[str], per_query: int, sort: str = "date") -> List[Dict[str, Any]]:
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
    selectors = [
        "meta[property='article:published_time']",
        "span.se_publishDate",
        "span#se_publishDate",
        "p.date",
        "span.date",
        "time",
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
        "div#post-area",
    ]
    text_chunks: List[str] = []
    for sel in candidates:
        try:
            el = page.query_selector(sel)
            if el:
                t = el.inner_text(timeout=2500)
                t = safe_text(t)
                if len(t) >= 80:
                    text_chunks.append(t)
        except Exception:
            pass

    if text_chunks:
        text_chunks.sort(key=len, reverse=True)
        return text_chunks[0]

    try:
        return safe_text(page.inner_text("body", timeout=2500))
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
        page.wait_for_timeout(650)

        mf = get_mainframe_post_url(page)
        if mf:
            page.goto(mf, wait_until="domcontentloaded")
            page.wait_for_timeout(650)
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
# Data model
# ==========================

@dataclass
class BlogPost:
    query: str
    title: str
    url: str
    published_at: str
    collected_at: str
    is_sponsored: bool
    sponsored_markers: List[str]

    # brand
    is_columbia_brand: bool
    brand_markers_found: List[str]
    brand_reason: str

    raw_text_len: int
    excerpt: str
    text_preview: str

    source: str  # "naver_api+playwright"


# ==========================
# HTML report (embedded JSON)
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
    .badge-brand {{
      background: rgba(37,99,235,0.10);
      color: rgb(30,64,175);
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
            네이버 블로그 언급 모니터링 (Columbia - Brand)
          </h1>
          <p class="muted font-semibold mt-2">
            ‘콜롬비아(국가/원두)’ 노이즈를 제거하고 <span class="font-black">의류/아웃도어 브랜드 Columbia</span>만 남깁니다.
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
            <span class="kbd">COL-BRAND</span>
          </div>
        </div>

        <div class="mt-3 text-xs font-semibold muted">
          * 기간 필터는 수집된 {meta.get("days", 60)}일 데이터 안에서 동작합니다.
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
              브랜드 글의 빈출 단어(긍/부정 분류 아님)
            </div>
            <div class="mt-3 flex flex-wrap gap-2" id="wordChips"></div>
          </div>
        </div>

        <div class="glass-card p-6">
          <div class="text-lg font-black">가이드</div>
          <ul class="mt-3 space-y-2 text-sm font-semibold muted">
            <li>• 카드 클릭 → 원문 새 탭 + 상세로 스크롤</li>
            <li>• “협찬 숨기기”로 내돈내산 성향만 보정 가능</li>
            <li>• 노이즈가 남으면 NEGATIVE_CONTEXT/OUTDOOR_CONTEXT를 조정</li>
          </ul>
        </div>
      </aside>

      <section class="lg:col-span-8 space-y-6">
        <div class="glass-card p-6">
          <div class="flex items-end justify-between gap-3">
            <div>
              <div class="text-xl font-black">Feed</div>
              <div class="muted text-sm font-semibold mt-1">Columbia(의류/아웃도어) 언급 블로그 글</div>
            </div>
            <div class="text-sm font-black muted">정렬: <span class="kbd">최근 → 과거</span></div>
          </div>

          <div class="mt-5 grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4" id="feedGrid"></div>
        </div>

        <div class="glass-card p-6">
          <div class="text-xl font-black">상세</div>
          <div class="muted text-sm font-semibold mt-1">본문 미리보기 + 판별 근거</div>
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
      badges.push(`<span class="badge badge-brand">Brand OK</span>`);
      if (p.is_sponsored) badges.push(`<span class="badge badge-ad">협찬</span>`);
      const markers = (p.brand_markers_found || []).slice(0,3).join(", ");
      if (markers) badges.push(`<span class="badge">근거: ${{escapeHtml(markers)}}</span>`);

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
              판별: <span class="font-black">${{escapeHtml(p.brand_reason || "-")}}</span>
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
    ap.add_argument("--per-query", type=int, default=60, help="쿼리당 후보 수(기본 60)")
    ap.add_argument("--headless", action="store_true", help="headless 모드")
    ap.add_argument("--out-dir", default=os.path.join("reports", "voc_blog"), help="출력 디렉토리")
    ap.add_argument("--max-scrape", type=int, default=220, help="Playwright로 실제 페이지 열어볼 최대 후보 수(속도/안정용)")
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
        "scrape_target": min(len(candidates), args.max_scrape),
        "scraped_ok": 0,
        "scraped_fail": 0,
        "cutoff_filtered": 0,
        "empty_text": 0,
        "brand_kept": 0,
        "brand_reject_negative_context": 0,
        "brand_reject_no_context": 0,
        "brand_reject_no_token": 0,
    }

    # 속도/안정: 최신부터 우선(이미 sort=date라 거의 최신이지만 안전)
    candidates = candidates[: args.max_scrape]

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

                # pub_dt 방어: None이거나 비정상이면 수집일로 대체
                if pub_dt is None:
                    pub_dt = collected_at
                else:
                    if pub_dt < (collected_at - timedelta(days=args.days + 7)) or pub_dt > (collected_at + timedelta(days=3)):
                        pub_dt = collected_at
                    if pub_dt < cutoff:
                        debug["cutoff_filtered"] += 1
                        continue

                # 브랜드 판별(응급 핵심)
                ok, markers, reason = is_brand_columbia(title=title, body=text, snippet=snippet)
                if not ok:
                    if reason == "negative_context":
                        debug["brand_reject_negative_context"] += 1
                    elif reason == "no_context":
                        debug["brand_reject_no_context"] += 1
                    else:
                        debug["brand_reject_no_token"] += 1
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
                    is_columbia_brand=True,
                    brand_markers_found=markers,
                    brand_reason=reason,
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
        "per_query": args.per_query,
        "max_scrape": args.max_scrape,
        "queries": args.queries,
        "counts": {
            "posts": len(posts),
            "sponsored_posts": sum(1 for p in posts if p.is_sponsored),
        },
        "top_words": top_words,
        "note": "콜롬비아(국가/원두) 노이즈를 제거하고 Columbia(의류/아웃도어 브랜드)만 남긴 결과입니다.",
        "debug": debug,
    }

    # 4) write outputs (+ alias keys)
    posts_dicts: List[Dict[str, Any]] = []
    for p in posts:
        d = asdict(p)
        # alias keys (프론트 0 방어)
        d["publishedAt"] = d.get("published_at")
        d["collectedAt"] = d.get("collected_at")
        posts_dicts.append(d)

    # root alias: items
    posts_root = {"posts": posts_dicts, "items": posts_dicts}

    with open(os.path.join(data_dir, "posts.json"), "w", encoding="utf-8") as f:
        json.dump(posts_root, f, ensure_ascii=False, indent=2)

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
