#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
[NAVER BLOG VOC] Columbia brand monitoring | v4.0 FINAL (Upgrades 1~4)
- FIXED: Python f-string + JS template literal `${...}` escaping

✅ 1) VOC scoring + highlight sentences
✅ 2) URL cache + early stop
✅ 3) Meta enrichment (blog_name/author/og:image + best-effort engagement)
✅ 4) Dashboard summary cards (New today, VOC top, issue types top, 7d vs 60d)

Outputs:
- <out_dir>/index.html
- <out_dir>/data/posts.json
- <out_dir>/data/meta.json
- <out_dir>/data/seen_urls.json

Requires env:
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
# Brand rules
# =======================

BRAND_STRONG = [
    "컬럼비아", "Columbia", "COLUMBIA",
    "Columbia Sportswear", "sportswear",
    "콜롬비아코리아", "columbiakorea", "columbiakorea.co.kr",
    "옴니히트", "omni-heat", "omni heat", "omniheat",
    "옴니위크", "omni-wick", "omni wick", "omniwick",
    "아웃드라이", "outdry",
    "터보다운", "turbo down", "turbodown",
    "인터체인지", "interchange",
    "타이타늄", "titanium",
]

OUTDOOR_CONTEXT = [
    "자켓", "재킷", "패딩", "다운", "후리스", "플리스", "바람막이", "윈드브레이커",
    "등산", "트레킹", "하이킹", "캠핑", "아웃도어", "방수", "방풍", "발수", "보온", "보냉",
    "베스트", "조끼", "팬츠", "바지", "레깅스", "티셔츠", "셔츠",
    "모자", "캡", "비니", "장갑", "가방", "백팩",
    "신발", "부츠", "트레킹화", "등산화",
    "사이즈", "핏", "착용", "착샷", "코디",
    "매장", "구매", "구입", "세일", "할인",
]

NEGATIVE_CONTEXT = [
    "페소", "COP", "환율", "관세", "대사관", "보고타", "남미", "국경", "과세환율", "통관", "무역",
    "원두", "커피", "핸드드립", "드립", "에스프레소", "로스팅", "게이샤", "바리스타",
    "줄거리", "결말", "영화", "드라마", "넷플릭스", "해킹", "로그인", "접속",
]

SKU_PATTERNS = [
    re.compile(r"\bC[0-9A-Z]{6,}\b"),
    re.compile(r"\bYM[0-9A-Z]{4,}\b"),
]

SPONSORED_MARKERS = [
    "광고", "협찬", "체험단", "소정의 원고료", "파트너스", "제공받아", "지원받아",
    "무상 제공", "유료 광고", "포스팅 비용", "원고료", "업체로부터",
]

# =======================
# VOC rules
# =======================

VOC_TAG_RULES = {
    "SIZE_FIT": [
        "사이즈", "작게", "크게", "정사이즈", "핏", "기장", "어깨", "소매", "허리", "엉덩이",
        "슬림", "오버", "루즈", "타이트", "여유", "끼", "불편",
    ],
    "QUALITY_DEFECT": [
        "불량", "하자", "박음질", "실밥", "찢", "뜯", "올풀", "마감", "지퍼", "고장",
        "누수", "방수", "이염", "변색", "냄새", "털빠짐", "보풀", "약해", "내구",
    ],
    "DELIVERY": [
        "배송", "지연", "늦", "파손", "누락", "오배송", "포장", "택배", "도착",
    ],
    "PRICE_PROMO": [
        "가격", "비싸", "가성비", "세일", "할인", "쿠폰", "프로모션", "적립", "포인트",
        "최저가", "정가", "환불가", "가격차",
    ],
    "EXCHANGE_RETURN_AS": [
        "교환", "반품", "환불", "AS", "A/S", "수선", "고객센터", "센터", "접수",
        "처리", "불친절", "응대",
    ],
    "WARMTH_WATERPROOF": [
        "따뜻", "보온", "한겨울", "추", "한파", "방풍", "방수", "발수", "젖", "비", "눈",
        "습기", "통풍", "땀",
    ],
}

NEG_SENTIMENT = [
    "별로", "아쉽", "불만", "실망", "후회", "문제", "최악", "짜증", "안되", "안 돼", "안됨",
    "못", "힘들", "불편",
]

WORD_RE = re.compile(r"[가-힣A-Za-z0-9]{2,}")
STOPWORDS = set("""
그리고 그러나 하지만 그래서 또한 정말 너무 진짜 약간 그냥 조금 매우 완전
제품 사용 구매 후기 리뷰 느낌 생각 정도 경우 부분 이번 오늘 어제 요즘
제가 저는 나는 우리는 여러분 때 좀 것 수 있 있다 없다 해요 했어요 입니다
ㅋㅋ ㅎㅎ ㅠㅠ ㅜㅜ
""".split())

SENT_SPLIT_RE = re.compile(r"(?<=[\.\!\?\n\r])\s+|(?<=다\.)\s+|(?<=요\.)\s+|(?<=니다\.)\s+|(?<=함\.)\s+")


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def now_kst() -> datetime:
    return datetime.now(tz=KST)

def iso(dt: datetime) -> str:
    return dt.astimezone(KST).isoformat()

def safe_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def strip_html(s: str) -> str:
    return safe_text(re.sub(r"<[^>]+>", "", s or ""))

def fmt_kst(dt: datetime) -> str:
    return dt.astimezone(KST).strftime("%Y.%m.%d %H:%M KST")

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
    title = title or ""
    body = body or ""
    snippet = snippet or ""
    combined = f"{title}\n{snippet}\n{body}"
    combined_l = combined.lower()

    strong_found = has_any(BRAND_STRONG, combined) + has_any([s.lower() for s in BRAND_STRONG], combined_l)
    strong_found = sorted(list(set([x for x in strong_found if x])))

    if not strong_found:
        if contains_any_keywords(combined, NEGATIVE_CONTEXT) or contains_any_keywords(combined_l, [x.lower() for x in NEGATIVE_CONTEXT]):
            return False, [], "negative_context"

    if strong_found:
        return True, strong_found[:6], "strong_marker"

    if ("콜롬비아" not in combined) and ("columbia" not in combined_l):
        return False, [], "no_brand_token"

    if has_sku(combined):
        return True, ["SKU"], "sku_match"

    if contains_any_keywords(combined, OUTDOOR_CONTEXT) or contains_any_keywords(combined_l, [x.lower() for x in OUTDOOR_CONTEXT]):
        return True, ["context"], "outdoor_context"

    return False, [], "no_context"

def split_sentences(text: str) -> List[str]:
    t = (text or "").strip()
    if not t:
        return []
    parts = [p.strip() for p in SENT_SPLIT_RE.split(t) if p and p.strip()]
    out = []
    for p in parts:
        out.extend([x.strip() for x in re.split(r"[\n\r]+", p) if x.strip()])
    return [x for x in out if len(x) >= 8]

def voc_tags_for_text(text: str) -> List[str]:
    t = text or ""
    tags = []
    for tag, kws in VOC_TAG_RULES.items():
        if any(k in t for k in kws):
            tags.append(tag)
    return tags

def voc_score_for_text(title: str, text: str, snippet: str) -> Tuple[int, List[str], List[Dict[str, Any]]]:
    combined = f"{title}\n{snippet}\n{text}"
    tags = voc_tags_for_text(combined)

    score = 0
    tag_weights = {
        "QUALITY_DEFECT": 22,
        "EXCHANGE_RETURN_AS": 18,
        "DELIVERY": 14,
        "SIZE_FIT": 14,
        "WARMTH_WATERPROOF": 10,
        "PRICE_PROMO": 8,
    }
    for t in tags:
        score += tag_weights.get(t, 6)

    if any(k in combined for k in NEG_SENTIMENT):
        score += 10

    hard = ["불량", "하자", "교환", "반품", "환불", "AS", "A/S", "오배송", "누락", "지연", "지퍼", "이염", "누수"]
    hard_hits = sum(1 for k in hard if k in combined)
    score += min(18, hard_hits * 4)
    score = max(0, min(120, score))

    highlights = []
    sents = split_sentences(text or snippet or "")
    for s in sents:
        stags = voc_tags_for_text(s)
        ss = 0
        for tt in stags:
            ss += tag_weights.get(tt, 6)
        if any(k in s for k in NEG_SENTIMENT):
            ss += 6
        hh = sum(1 for k in hard if k in s)
        ss += min(12, hh * 3)
        if ss >= 10:
            highlights.append({"sent": safe_text(s)[:240], "score": int(ss), "tags": stags})

    highlights.sort(key=lambda x: x["score"], reverse=True)
    highlights = highlights[:6]
    return int(score), tags, highlights


def fetch_candidates_via_naver_api(queries: List[str], per_query: int, sort: str = "date") -> List[Dict[str, Any]]:
    cid = os.getenv("NAVER_CLIENT_ID", "").strip()
    csec = os.getenv("NAVER_CLIENT_SECRET", "").strip()
    if not cid or not csec:
        raise RuntimeError("Missing NAVER_CLIENT_ID / NAVER_CLIENT_SECRET env")

    headers = {"X-Naver-Client-Id": cid, "X-Naver-Client-Secret": csec}

    collected: List[Dict[str, Any]] = []
    for q in queries:
        got = 0
        start = 1
        while got < per_query and start <= 1000:
            display = min(100, per_query - got)
            params = {"query": q, "display": display, "start": start, "sort": sort}
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

                collected.append({"query": q, "title": title or "(제목 없음)", "url": link, "snippet": desc})
                got += 1
                if got >= per_query:
                    break
            start += display

    seen = set()
    uniq = []
    for x in collected:
        u = x["url"]
        if u in seen:
            continue
        seen.add(u)
        uniq.append(x)
    return uniq


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

def extract_meta_content(page, selector: str) -> str:
    try:
        el = page.query_selector(selector)
        if el:
            c = el.get_attribute("content") or ""
            return safe_text(c)
    except Exception:
        pass
    return ""

def extract_title(page) -> str:
    for sel in ["meta[property='og:title']", "meta[name='twitter:title']"]:
        t = extract_meta_content(page, sel)
        if t:
            return t
    try:
        return safe_text(page.title())
    except Exception:
        return ""

def extract_og_image(page) -> str:
    return extract_meta_content(page, "meta[property='og:image']")

def extract_site_name(page) -> str:
    v = extract_meta_content(page, "meta[property='og:site_name']")
    return v or ""

def extract_author(page) -> str:
    for sel in ["meta[name='author']", "meta[property='article:author']"]:
        v = extract_meta_content(page, sel)
        if v:
            return v
    try:
        for sel in ["a.nick", "span.nick", "div.blog_name a", "div.blog_name"]:
            el = page.query_selector(sel)
            if el:
                v = safe_text(el.inner_text() or "")
                if v and len(v) <= 40:
                    return v
    except Exception:
        pass
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
            v = el.get_attribute("content") if sel.startswith("meta") else el.inner_text()
            v = safe_text(v or "")
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

def extract_engagement_best_effort(page) -> Tuple[Optional[int], Optional[int]]:
    like_cnt = None
    cmt_cnt = None
    try:
        body = page.inner_text("body", timeout=1200).replace(",", "")
        m = re.search(r"(공감|좋아요)\s*([0-9]{1,6})", body)
        if m:
            like_cnt = int(m.group(2))
        m2 = re.search(r"(댓글)\s*([0-9]{1,6})", body)
        if m2:
            cmt_cnt = int(m2.group(2))
    except Exception:
        pass
    return like_cnt, cmt_cnt

def extract_post_text_from_dom(page) -> str:
    candidates = [".se-main-container", "#postViewArea", "article", "div#contentArea", "div#post-area"]
    text_chunks: List[str] = []
    for sel in candidates:
        try:
            el = page.query_selector(sel)
            if el:
                t = safe_text(el.inner_text(timeout=2500))
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

def scrape_one_post(context, url: str, timeout_ms: int = 25000) -> Dict[str, Any]:
    page = context.new_page()
    page.set_default_timeout(timeout_ms)

    out = {
        "final_url": url,
        "title": "",
        "text": "",
        "pub_dt": None,
        "og_image": "",
        "site_name": "",
        "author": "",
        "like_count": None,
        "comment_count": None,
    }

    try:
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_timeout(650)

        mf = get_mainframe_post_url(page)
        if mf:
            page.goto(mf, wait_until="domcontentloaded")
            page.wait_for_timeout(650)

        out["final_url"] = page.url or url
        out["title"] = extract_title(page)
        out["pub_dt"] = extract_published_from_dom(page)
        out["og_image"] = extract_og_image(page)
        out["site_name"] = extract_site_name(page)
        out["author"] = extract_author(page)
        out["text"] = extract_post_text_from_dom(page)
        like_cnt, cmt_cnt = extract_engagement_best_effort(page)
        out["like_count"] = like_cnt
        out["comment_count"] = cmt_cnt
        return out
    finally:
        try:
            page.close()
        except Exception:
            pass


def load_seen_urls(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {"version": 1, "seen": {}}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if "seen" not in data:
            return {"version": 1, "seen": {}}
        return data
    except Exception:
        return {"version": 1, "seen": {}}

def save_seen_urls(path: str, data: Dict[str, Any]) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def mark_seen(seen_db: Dict[str, Any], url: str, collected_at_iso: str) -> None:
    seen = seen_db.setdefault("seen", {})
    seen[url] = {"last_seen": collected_at_iso}

def is_seen(seen_db: Dict[str, Any], url: str) -> bool:
    return url in (seen_db.get("seen", {}) or {})


@dataclass
class BlogPost:
    query: str
    title: str
    url: str
    published_at: str
    collected_at: str

    is_sponsored: bool
    sponsored_markers: List[str]

    is_columbia_brand: bool
    brand_markers_found: List[str]
    brand_reason: str

    blog_site_name: str
    author: str
    og_image: str
    like_count: Optional[int]
    comment_count: Optional[int]

    raw_text_len: int
    excerpt: str
    text_preview: str

    voc_score: int
    voc_tags: List[str]
    voc_highlights: List[Dict[str, Any]]

    source: str


def build_report_html(meta: Dict[str, Any], posts: List[Dict[str, Any]]) -> str:
    posts_json = json.dumps(posts, ensure_ascii=False)
    meta_json = json.dumps(meta, ensure_ascii=False)

    # ⚠️ IMPORTANT:
    # Python f-string 안에서 JS 템플릿 `${...}`를 쓰려면
    # `{` `}`를 모두 `{{` `}}`로 이스케이프해야 함.
    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Blog VOC | CSK E-COMM</title>
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
    .badge-ad {{ background: rgba(234,179,8,0.14); color: rgb(161,98,7); }}
    .badge-brand {{ background: rgba(37,99,235,0.10); color: rgb(30,64,175); }}
    .badge-voc {{ background: rgba(239,68,68,0.10); color: rgb(153,27,27); }}
    .link {{ color: var(--brand); font-weight: 900; }}
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
    .lineclamp4 {{
      display: -webkit-box;
      -webkit-line-clamp: 4;
      -webkit-box-orient: vertical;
      overflow: hidden;
    }}
  </style>
</head>

<body class="p-4 md:p-8">
  <div class="max-w-7xl mx-auto">
    <div class="glass-card p-6 md:p-8">
      <div class="flex flex-col md:flex-row md:items-end md:justify-between gap-4">
        <div>
          <div class="text-[11px] font-black tracking-[0.3em] text-slate-400 uppercase">CSK E-COMM</div>
          <h1 class="text-3xl md:text-4xl font-black tracking-tight mt-2">
            네이버 블로그 VOC 모니터링 (Columbia - Brand)
          </h1>
          <p class="muted font-semibold mt-2">
            브랜드(의류/아웃도어) 글만 남기고, <span class="font-black">VOC 신호를 자동 추출</span>합니다.
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
            <div class="text-sm font-black">기간</div>
            <input id="dateStart" type="date" />
            <div class="muted font-black">~</div>
            <input id="dateEnd" type="date" />
            <button id="btnApply" class="btn">적용</button>
            <button id="btnReset" class="btn">최근 60일</button>

            <div class="ml-2 flex items-center gap-2">
              <span class="text-sm font-black muted">VOC≥</span>
              <input id="vocMin" type="number" min="0" max="120" value="20"
                class="w-20 px-3 py-2 rounded-2xl font-black bg-white/70 border border-white/90" />
              <button id="btnVocOnly" class="btn">VOC만 보기</button>
            </div>
          </div>

          <div class="flex flex-wrap items-center gap-2">
            <button id="btnHideSponsored" class="btn">협찬 숨기기</button>
            <span class="kbd">COL-BLOG-VOC</span>
          </div>
        </div>

        <div class="mt-3 text-xs font-semibold muted">
          candidates: <span class="font-black">{meta.get("debug",{}).get("candidates_total","-")}</span>
          <span class="mx-2">|</span>
          scraped_ok: <span class="font-black">{meta.get("debug",{}).get("scraped_ok","-")}</span>
          <span class="mx-2">|</span>
          kept_brand: <span class="font-black">{meta.get("debug",{}).get("brand_kept","-")}</span>
          <span class="mx-2">|</span>
          saved: <span class="font-black">{meta.get("counts",{}).get("posts","-")}</span>
        </div>
      </div>
    </div>

    <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4 mt-6" id="summaryCards"></div>

    <div class="grid grid-cols-1 lg:grid-cols-12 gap-6 mt-6">
      <aside class="lg:col-span-4 space-y-6">
        <div class="glass-card p-6">
          <div class="flex items-center justify-between">
            <div class="text-lg font-black">Top VOC</div>
            <div class="kbd">VOC</div>
          </div>
          <div class="mt-4 space-y-3" id="topVocList"></div>
        </div>

        <div class="glass-card p-6">
          <div class="flex items-center justify-between">
            <div class="text-lg font-black">이슈 타입 Top</div>
            <div class="kbd">TAGS</div>
          </div>
          <div class="mt-4 flex flex-wrap gap-2" id="tagChips"></div>
        </div>

        <div class="glass-card p-6">
          <div class="text-lg font-black">Top Words</div>
          <div class="muted text-xs font-semibold mt-1">본문 기반 빈출 단어</div>
          <div class="mt-3 flex flex-wrap gap-2" id="wordChips"></div>
        </div>
      </aside>

      <section class="lg:col-span-8 space-y-6">
        <div class="glass-card p-6">
          <div class="flex items-end justify-between gap-3">
            <div>
              <div class="text-xl font-black">Feed</div>
              <div class="muted text-sm font-semibold mt-1">브랜드 글(정렬: 최근 → 과거)</div>
            </div>
            <div class="text-sm font-black muted">정렬: <span class="kbd">최근</span></div>
          </div>
          <div class="mt-5 grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4" id="feedGrid"></div>
        </div>

        <div class="glass-card p-6">
          <div class="text-xl font-black">상세</div>
          <div class="muted text-sm font-semibold mt-1">VOC 하이라이트 + 메타</div>
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
    vocOnly: false,
    start: null,
    end: null,
    word: null,
    tag: null,
  }};

  function vocMin() {{
    const v = parseInt(($("vocMin")?.value || "20"), 10);
    if (isNaN(v)) return 20;
    return Math.max(0, Math.min(120, v));
  }}

  function filteredPosts() {{
    const minV = vocMin();
    return POSTS.filter(p => {{
      if (state.hideSponsored && p.is_sponsored) return false;
      if (!inRange(p.published_at, state.start, state.end)) return false;

      if (state.vocOnly && (p.voc_score || 0) < minV) return false;

      if (state.tag) {{
        const tags = p.voc_tags || [];
        if (!tags.includes(state.tag)) return false;
      }}

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

  function renderSummaryCards() {{
    const box = $("summaryCards");
    box.innerHTML = "";

    const listAll = filteredPosts();
    const today = new Date();
    const todayKey = `${{today.getFullYear()}}-${{String(today.getMonth()+1).padStart(2,"0")}}-${{String(today.getDate()).padStart(2,"0")}}`;

    let newToday = 0;
    let sponsored = 0;
    let vocOver = 0;

    listAll.forEach(p => {{
      if (toDateOnly(p.collected_at) === todayKey) newToday += 1;
      if (p.is_sponsored) sponsored += 1;
      if ((p.voc_score||0) >= vocMin()) vocOver += 1;
    }});

    const end = state.end ? new Date(state.end.getTime()) : new Date();
    const start7 = new Date(end.getTime()); start7.setDate(start7.getDate()-6);
    const start60 = new Date(end.getTime()); start60.setDate(start60.getDate()-59);

    function ratio(rangeStart) {{
      const subset = POSTS.filter(p => inRange(p.published_at, rangeStart, end));
      if (subset.length === 0) return 0;
      const n = subset.filter(p => (p.voc_score||0) >= vocMin()).length;
      return Math.round((n / subset.length) * 1000) / 10;
    }}

    const r7 = ratio(start7);
    const r60 = ratio(start60);

    const cards = [
      {{ title: "Posts", value: String(listAll.length), sub: "필터 기준" }},
      {{ title: "New Today", value: String(newToday), sub: "수집일 기준" }},
      {{ title: "VOC (≥" + vocMin() + ")", value: String(vocOver), sub: "VOC 감지" }},
      {{ title: "VOC Ratio", value: r7 + "%", sub: "7D vs 60D: " + r60 + "%" }},
    ];

    cards.forEach(c => {{
      const el = document.createElement("div");
      el.className = "glass-card p-5";
      el.innerHTML = `
        <div class="text-[10px] font-black tracking-widest text-slate-500 uppercase">${{escapeHtml(c.title)}}</div>
        <div class="text-3xl font-black mt-2">${{escapeHtml(c.value)}}</div>
        <div class="muted text-sm font-semibold mt-1">${{escapeHtml(c.sub)}}</div>
      `;
      box.appendChild(el);
    }});
  }}

  function renderTopVoc() {{
    const box = $("topVocList");
    box.innerHTML = "";
    const list = filteredPosts().slice().sort((a,b)=> (b.voc_score||0)-(a.voc_score||0)).slice(0, 8);

    if (list.length === 0) {{
      box.innerHTML = `<div class="muted font-semibold">VOC 항목이 없습니다.</div>`;
      return;
    }}

    list.forEach(p => {{
      const hl = (p.voc_highlights || [])[0]?.sent || p.excerpt || "";
      const el = document.createElement("div");
      el.className = "feed-card p-4";
      el.innerHTML = `
        <div class="flex items-start justify-between gap-2">
          <div class="font-black text-sm leading-snug">${{escapeHtml(p.title||"(제목 없음)")}}</div>
          <div class="badge badge-voc">VOC ${{p.voc_score||0}}</div>
        </div>
        <div class="mt-2 text-xs font-semibold muted lineclamp4">${{escapeHtml(hl)}}</div>
        <div class="mt-2 text-xs font-black muted">${{escapeHtml((p.voc_tags||[]).slice(0,3).join(", "))}}</div>
      `;
      el.addEventListener("click", ()=> {{
        window.open(p.url, "_blank", "noopener,noreferrer");
      }});
      box.appendChild(el);
    }});
  }}

  function renderTagChips() {{
    const box = $("tagChips");
    box.innerHTML = "";

    const freq = {{}};
    POSTS.forEach(p => {{
      (p.voc_tags||[]).forEach(t => {{
        freq[t] = (freq[t]||0) + 1;
      }});
    }});
    const items = Object.entries(freq).sort((a,b)=> b[1]-a[1]).slice(0, 14);

    const clear = document.createElement("div");
    clear.className = "badge cursor-pointer hover:opacity-80";
    clear.textContent = "태그 해제";
    clear.addEventListener("click", ()=> {{ state.tag = null; renderAll(); }});
    box.appendChild(clear);

    items.forEach(([t,c]) => {{
      const el = document.createElement("div");
      el.className = "badge cursor-pointer hover:opacity-80";
      el.innerHTML = `${{escapeHtml(t)}} · ${{c}}`;
      el.addEventListener("click", ()=> {{
        state.tag = t;
        renderAll();
      }});
      box.appendChild(el);
    }});
  }}

  function renderWords() {{
    const box = $("wordChips");
    box.innerHTML = "";
    const words = (META.top_words || []).slice(0, 16);
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
    clear.textContent = "단어 해제";
    clear.addEventListener("click", ()=> {{ state.word = null; renderAll(); }});
    box.appendChild(clear);
  }}

  function renderFeed() {{
    const grid = $("feedGrid");
    grid.innerHTML = "";
    const list = filteredPosts().slice().sort((a,b)=> (b.published_at||"").localeCompare(a.published_at||""));

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
      const vs = p.voc_score || 0;
      if (vs >= vocMin()) badges.push(`<span class="badge badge-voc">VOC ${{vs}}</span>`);
      const tags = (p.voc_tags || []).slice(0,2).join(", ");
      if (tags) badges.push(`<span class="badge">tags: ${{escapeHtml(tags)}}</span>`);

      const metaLine = [];
      if (p.author) metaLine.push(p.author);
      if (p.blog_site_name) metaLine.push(p.blog_site_name);
      const metaText = metaLine.join(" · ");

      card.innerHTML = `
        <div class="flex items-start justify-between gap-2">
          <div class="font-black text-base leading-snug">${{escapeHtml(p.title||"(제목 없음)")}}</div>
          <div class="text-[11px] font-black muted whitespace-nowrap">${{fmtDate(p.published_at)}}</div>
        </div>

        <div class="mt-2 flex flex-wrap gap-2">${{badges.join("")}}</div>

        <div class="mt-3 text-sm font-semibold muted lineclamp4">
          ${{escapeHtml(p.excerpt || "")}}
        </div>

        <div class="mt-3 text-[12px] font-black muted">
          ${{escapeHtml(metaText)}}
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
    const list = filteredPosts().slice().sort((a,b)=> (b.published_at||"").localeCompare(a.published_at||""));

    list.forEach((p, idx) => {{
      const wrap = document.createElement("div");
      wrap.id = "detail-" + idx;
      wrap.className = "feed-card p-5";

      const sponsoredLine = p.is_sponsored
        ? `<div class="mt-2 text-xs font-black badge badge-ad">협찬/광고: ${{escapeHtml((p.sponsored_markers||[]).join(", "))}}</div>`
        : ``;

      // ✅ FIXED: outer ${...} -> ${{...}}
      const vocLine = `<div class="mt-2 flex flex-wrap gap-2">
        <span class="badge badge-voc">VOC ${{p.voc_score||0}}</span>
        ${{(p.voc_tags||[]).slice(0,6).map(t=> `<span class="badge">${{escapeHtml(t)}}</span>`).join("")}}
      </div>`;

      const hl = (p.voc_highlights || []).map(h => {{
        return `<div class="mt-2 text-sm font-semibold muted">• ${{escapeHtml(h.sent)}} <span class="badge ml-2">+${{h.score}}</span></div>`;
      }}).join("");

      const img = p.og_image ? `<img src="${{escapeHtml(p.og_image)}}" class="w-full rounded-2xl border border-white/70 mt-4" loading="lazy" />` : "";

      const engage = [];
      if (p.like_count !== null && p.like_count !== undefined) engage.push("좋아요 " + p.like_count);
      if (p.comment_count !== null && p.comment_count !== undefined) engage.push("댓글 " + p.comment_count);

      // ✅ FIXED: outer ${...} -> ${{...}}
      const engageHtml = ${{(engage.length ? `<span class="mx-2">·</span><span class="font-black">${{escapeHtml(engage.join(" / "))}}</span>` : "")}};

      wrap.innerHTML = `
        <div class="flex flex-col md:flex-row md:items-start md:justify-between gap-3">
          <div class="flex-1">
            <div class="text-lg font-black leading-snug">${{escapeHtml(p.title||"(제목 없음)")}}</div>
            <div class="mt-2 text-sm font-semibold muted">
              발행: <span class="font-black">${{fmtDate(p.published_at)}}</span>
              <span class="mx-2">·</span>
              작성자: <span class="font-black">${{escapeHtml(p.author||"-")}}</span>
              <span class="mx-2">·</span>
              메타: <span class="font-black">${{escapeHtml(p.blog_site_name||"-")}}</span>
              ${{engageHtml}}
            </div>
            ${{sponsoredLine}}
            ${{vocLine}}
          </div>

          <a class="link text-sm" href="${{escapeHtml(p.url)}}" target="_blank" rel="noopener noreferrer">
            원문 열기 <i class="fa-solid fa-arrow-up-right-from-square"></i>
          </a>
        </div>

        <div class="mt-4">
          <div class="text-sm font-black">VOC 하이라이트</div>
          <div class="mt-2">
            ${{hl || `<div class="muted font-semibold">하이라이트가 없습니다. (VOC 신호 약함)</div>`}}
          </div>
        </div>

        <div class="mt-4">
          <div class="text-sm font-black">본문 미리보기</div>
          <div class="mt-2 text-sm font-semibold muted whitespace-pre-line">
            ${{escapeHtml(p.text_preview||"")}}
          </div>
        </div>

        ${{img}}
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

    $("btnVocOnly").addEventListener("click", () => {{
      state.vocOnly = !state.vocOnly;
      $("btnVocOnly").textContent = state.vocOnly ? "전체 보기" : "VOC만 보기";
      renderAll();
    }});

    $("vocMin").addEventListener("change", renderAll);
  }}

  function renderAll() {{
    renderHeader();
    renderSummaryCards();
    renderTopVoc();
    renderTagChips();
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--queries", nargs="+", required=True)
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--per-query", type=int, default=60)
    ap.add_argument("--headless", action="store_true")
    ap.add_argument("--out-dir", default=os.path.join("reports", "voc_blog"))
    ap.add_argument("--max-scrape", type=int, default=220)
    ap.add_argument("--brand-target", type=int, default=140)
    ap.add_argument("--voc-target", type=int, default=60)
    ap.add_argument("--voc-min", type=int, default=20)
    ap.add_argument("--force-rescrape-seen", action="store_true")
    args = ap.parse_args()

    out_dir = args.out_dir
    data_dir = os.path.join(out_dir, "data")
    ensure_dir(data_dir)

    collected_at = now_kst()
    cutoff = collected_at - timedelta(days=args.days - 1)

    seen_path = os.path.join(data_dir, "seen_urls.json")
    seen_db = load_seen_urls(seen_path)

    candidates = fetch_candidates_via_naver_api(args.queries, per_query=args.per_query, sort="date")
    print(f"[INFO] candidates_total={len(candidates)} (per_query={args.per_query}) cutoff={cutoff.isoformat()}")

    candidates = candidates[: args.max_scrape]

    debug = {
        "candidates_total": len(candidates),
        "scrape_target": len(candidates),
        "scraped_ok": 0,
        "scraped_fail": 0,
        "skipped_seen": 0,
        "cutoff_filtered": 0,
        "empty_text": 0,
        "brand_kept": 0,
        "brand_reject_negative_context": 0,
        "brand_reject_no_context": 0,
        "brand_reject_no_token": 0,
        "early_stop_brand": 0,
        "early_stop_voc": 0,
    }

    posts: List[BlogPost] = []
    brand_kept = 0
    voc_kept = 0

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

                if (not args.force_rescrape_seen) and is_seen(seen_db, url):
                    debug["skipped_seen"] += 1
                    continue

                try:
                    s = scrape_one_post(context, url=url)
                    debug["scraped_ok"] += 1
                except Exception:
                    debug["scraped_fail"] += 1
                    continue

                final_url = s.get("final_url") or url
                title = safe_text(s.get("title") or base_title)
                text = s.get("text") or ""
                pub_dt = s.get("pub_dt") or collected_at

                if len(safe_text(text)) == 0:
                    debug["empty_text"] += 1

                if pub_dt < cutoff:
                    debug["cutoff_filtered"] += 1
                    mark_seen(seen_db, url, iso(collected_at))
                    continue

                ok, markers, reason = is_brand_columbia(title=title, body=text, snippet=snippet)
                if not ok:
                    if reason == "negative_context":
                        debug["brand_reject_negative_context"] += 1
                    elif reason == "no_context":
                        debug["brand_reject_no_context"] += 1
                    else:
                        debug["brand_reject_no_token"] += 1
                    mark_seen(seen_db, url, iso(collected_at))
                    continue

                debug["brand_kept"] += 1
                brand_kept += 1

                is_sp, sp_markers = detect_sponsored(f"{title} {text} {snippet}")

                cleaned = safe_text(text) if text else ""
                excerpt = cleaned[:180] if cleaned else safe_text(snippet)[:180] if snippet else "(본문 추출 실패)"
                preview = cleaned[:1400] if cleaned else safe_text(snippet)[:1400] if snippet else "(본문 추출 실패)"

                voc_score, voc_tags, voc_highlights = voc_score_for_text(title=title, text=cleaned, snippet=snippet)
                if voc_score >= args.voc_min:
                    voc_kept += 1

                p = BlogPost(
                    query=q,
                    title=title or "(제목 없음)",
                    url=final_url,
                    published_at=iso(pub_dt),
                    collected_at=iso(collected_at),

                    is_sponsored=is_sp,
                    sponsored_markers=sp_markers,

                    is_columbia_brand=True,
                    brand_markers_found=markers,
                    brand_reason=reason,

                    blog_site_name=safe_text(s.get("site_name") or ""),
                    author=safe_text(s.get("author") or ""),
                    og_image=safe_text(s.get("og_image") or ""),
                    like_count=s.get("like_count"),
                    comment_count=s.get("comment_count"),

                    raw_text_len=len(text or ""),
                    excerpt=excerpt,
                    text_preview=preview,

                    voc_score=voc_score,
                    voc_tags=voc_tags,
                    voc_highlights=voc_highlights,

                    source="naver_api+playwright",
                )

                posts.append(p)
                mark_seen(seen_db, url, iso(collected_at))

                if brand_kept >= args.brand_target:
                    debug["early_stop_brand"] = 1
                    if voc_kept >= args.voc_target:
                        debug["early_stop_voc"] = 1
                        break
        finally:
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass

    save_seen_urls(seen_path, seen_db)

    posts.sort(key=lambda x: x.published_at, reverse=True)

    top_words = keyword_map_from_texts([p.text_preview for p in posts], topn=40)

    tag_freq: Dict[str, int] = {}
    for p in posts:
        for t in (p.voc_tags or []):
            tag_freq[t] = tag_freq.get(t, 0) + 1
    tag_top = sorted(tag_freq.items(), key=lambda x: x[1], reverse=True)[:15]

    top_voc = sorted(posts, key=lambda x: x.voc_score, reverse=True)[:12]

    meta = {
        "updated_at_kst": fmt_kst(collected_at),
        "period_text": f"최근 {args.days}일 ({cutoff.strftime('%Y.%m.%d')} ~ {collected_at.strftime('%Y.%m.%d')})",
        "days": args.days,
        "per_query": args.per_query,
        "max_scrape": args.max_scrape,
        "brand_target": args.brand_target,
        "voc_target": args.voc_target,
        "voc_min": args.voc_min,
        "queries": args.queries,
        "counts": {
            "posts": len(posts),
            "sponsored_posts": sum(1 for p in posts if p.is_sponsored),
            "voc_posts": sum(1 for p in posts if p.voc_score >= args.voc_min),
        },
        "top_words": top_words,
        "tag_top": tag_top,
        "top_voc": [
            {"title": p.title, "url": p.url, "voc_score": p.voc_score, "voc_tags": p.voc_tags[:5], "published_at": p.published_at}
            for p in top_voc
        ],
        "debug": debug,
    }

    posts_dicts: List[Dict[str, Any]] = []
    for p in posts:
        d = asdict(p)
        d["publishedAt"] = d.get("published_at")
        d["collectedAt"] = d.get("collected_at")
        posts_dicts.append(d)

    posts_root = {"posts": posts_dicts, "items": posts_dicts}

    with open(os.path.join(data_dir, "posts.json"), "w", encoding="utf-8") as f:
        json.dump(posts_root, f, ensure_ascii=False, indent=2)

    with open(os.path.join(data_dir, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    html = build_report_html(meta=meta, posts=posts_dicts)
    with open(os.path.join(out_dir, "index.html"), "w", encoding="utf-8") as f:
        f.write(html)

    print(f"[OK] posts_saved={len(posts)} voc_posts={meta['counts']['voc_posts']} debug={debug}")
    print(f"[OK] {os.path.join(out_dir, 'index.html')}")


if __name__ == "__main__":
    main()
