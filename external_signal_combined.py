#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin
from dataclasses import dataclass
from typing import List, Dict, Tuple
import urllib3

# ================================================================
# Summary/meta export (Hub first-screen consumption)
# - Writes/updates reports/summary.json (or alongside output html)
# ================================================================
import json
from datetime import datetime, timezone, timedelta

_KST = timezone(timedelta(hours=9))

def _safe_mkdir(p: str):
    os.makedirs(p, exist_ok=True)

def _write_summary_json(out_dir: str, report_key: str, payload: dict):
    """Merge-update a single summary.json file in out_dir."""
    _safe_mkdir(out_dir)
    path = os.path.join(out_dir, "summary.json")
    data = {}
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
    except Exception:
        data = {}
    data[report_key] = payload
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def _now_kst_str():
    return datetime.now(_KST).strftime("%Y-%m-%d %H:%M KST")

# =================================================================
# 1. 크롤링 엔진
# =================================================================
KST = timezone(timedelta(hours=9))
GALLERY_ID = "climbing"
BASE_URL = "https://gall.dcinside.com"
MAX_PAGES = 100
TARGET_DAYS = 7  # 최근 N일(오늘 포함) 데이터 대상

# ✅ 분석 대상 브랜드 (기존 10 + 추가 15 = 총 25)
# - 네가 말했던 15개를 여기 넣으면 됨. (지금은 “아웃도어 TOP” 기준으로 기본값 채워둠)
BRAND_LIST = [
    # 기존
    "컬럼비아", "노스페이스", "파타고니아", "아크테릭스", "블랙야크",
    "K2", "캠프라인", "살로몬", "호카", "마무트",

    # 추가 15 (기본값 - 필요하면 네가 말한 브랜드로 교체/정리해줘)
    "스노우피크", "내셔널지오그래픽", "디스커버리", "코오롱스포츠", "몬벨",
    "네파", "아이더", "노스케이프", "밀레", "라푸마",
    "헬리한센", "오스프리", "그레고리", "데상트", "나이키"
]

# ✅ 브랜드명 정규화(별칭/영문/약칭) 지원이 필요하면 여기에 추가
# 예: "The North Face" / "TNF" -> "노스페이스"
BRAND_ALIASES: Dict[str, List[str]] = {
    "노스페이스": ["노페", "TNF", "The North Face", "NORTHFACE", "NORTH FACE"],
    "아크테릭스": ["아크", "Arc'teryx", "ARCTERYX", "아크테릭"],
    "파타고니아": ["파타", "Patagonia", "PATAGONIA"],
    "살로몬": ["살로몬", "Salomon", "SALOMON"],
    "스노우피크": ["Snow Peak", "SNOWPEAK", "Snowpeak"],
    "내셔널지오그래픽": ["National Geographic", "NATIONALGEOGRAPHIC", "NATGEO", "NatGeo", "내지"],
    "코오롱스포츠": ["Kolon Sport", "KOLONSPORT", "Kolonsport", "코오롱"],
    "몽벨": ["몽벨", "Montbell", "MONTBELL"],
    "디스커버리": ["Discovery", "DISCOVERY"],
    "컬럼비아": ["Columbia", "COLUMBIA", "콜롬비아"],
    "블랙야크": ["Black Yak", "BLACKYAK", "블야"],
    "네파": ["NEPA"],
    "아이더": ["EIDER"],
    "데상트": ["Descente", "DESCENTE"],
}

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
})

@dataclass
class Post:
    title: str
    url: str
    content: str
    comments: str
    created_at: datetime

def crawl_dc_engine(days: int):
    start_date = (datetime.now(KST) - timedelta(days=days)).date()
    posts: List[Post] = []
    stop_signal = False

    print(f"🚀 [M-OS SYSTEM] DCInside '{GALLERY_ID}' 갤러리 분석 시작 (최근 {days}일)")

    for page in range(1, MAX_PAGES + 1):
        if stop_signal:
            break

        url = f"{BASE_URL}/board/lists/?id={GALLERY_ID}&page={page}"
        try:
            resp = SESSION.get(url, timeout=10)
        except Exception:
            break

        if resp.status_code != 200:
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        rows = soup.select("tr.ub-content")

        for row in rows:
            num_el = row.select_one("td.gall_num")
            if not num_el:
                continue

            num = num_el.get_text(strip=True)
            if not num.isdigit():
                continue  # 공지사항 제외

            a_tag = row.select_one("td.gall_tit a")
            if not a_tag:
                continue

            link = urljoin(BASE_URL, a_tag.get("href"))

            try:
                d_resp = SESSION.get(link, timeout=10)
                if d_resp.status_code != 200:
                    continue

                d_soup = BeautifulSoup(d_resp.text, "html.parser")

                date_el = d_soup.select_one(".gall_date")
                if not date_el:
                    continue

                dt = datetime.strptime(date_el.get_text(strip=True), "%Y.%m.%d %H:%M:%S").replace(tzinfo=KST)

                # 날짜 제한 체크
                if dt.date() < start_date:
                    stop_signal = True
                    break

                content_el = d_soup.select_one(".write_div")
                content = content_el.get_text("\n", strip=True) if content_el else ""

                comments = "\n".join([c.get_text(strip=True) for c in d_soup.select(".comment_list .usertxt")])

                posts.append(Post(
                    title=a_tag.get_text(strip=True),
                    url=link,
                    content=content,
                    comments=comments,
                    created_at=dt
                ))
            except Exception:
                continue

        print(f"   - {page}페이지 완료 (누적 수집: {len(posts)})")

    return posts

# =================================================================
# 2. 텍스트 분석 유틸
# =================================================================
def normalize_text(s: str) -> str:
    return (s or "").strip()

def split_sentences(text: str) -> List[str]:
    if not text:
        return []
    # 문장 분리: 줄바꿈/마침/물음/느낌 등 기준
    parts = re.split(r"[.!?\n]", text)
    out = []
    for p in parts:
        p = p.strip()
        if len(p) >= 4:
            out.append(p)
    return out

def build_brand_patterns() -> Dict[str, re.Pattern]:
    """
    브랜드별 매칭 패턴(정규식) 생성:
    - 기본 브랜드명 + 별칭들을 모두 OR로 묶고, 대소문자 무시
    """
    patterns = {}
    for b in BRAND_LIST:
        aliases = BRAND_ALIASES.get(b, [])
        tokens = [re.escape(b)] + [re.escape(a) for a in aliases]
        # 단어 경계는 한글/영문 혼합이라 완벽하지 않아서, 대신 "포함" 매칭을 정규식으로 구현
        pat = re.compile(r"(" + "|".join(tokens) + r")", re.IGNORECASE)
        patterns[b] = pat
    return patterns

def contains_brand(title: str, brand: str, patterns: Dict[str, re.Pattern]) -> bool:
    return bool(patterns[brand].search(title or ""))

def sentence_has_brand(sentence: str, brand: str, patterns: Dict[str, re.Pattern]) -> bool:
    return bool(patterns[brand].search(sentence or ""))

# =================================================================
# 3. 데이터 분석
#   - Hot Keywords 제거
#   - 언급량 summary 생성
#   - 제목에 브랜드가 있으면 댓글에서 브랜드 언급도 추가 수집(강제)
# =================================================================
def process_data(posts: List[Post]):
    patterns = build_brand_patterns()

    brand_map: Dict[str, List[dict]] = {b: [] for b in BRAND_LIST}

    summary = {
        b: {
            "posts_count": 0,
            "title_hits": 0,
            "comment_mentions": 0,   # ✅ "댓글 언급 문장 수(전체)"
            "total_mentions": 0,
        } for b in BRAND_LIST
    }

    for p in posts:
        title = normalize_text(p.title)
        content = normalize_text(p.content)
        comments = normalize_text(p.comments)

        title_sents = [title] if title else []
        content_sents = split_sentences(content)
        comment_sents = split_sentences(comments)

        post_has_brand = {b: False for b in BRAND_LIST}
        title_has_brand = {b: False for b in BRAND_LIST}

        for b in BRAND_LIST:
            # 1) Title hit (포스트 단위)
            if contains_brand(title, b, patterns):
                title_has_brand[b] = True
                summary[b]["title_hits"] += 1
                post_has_brand[b] = True

            # 2) 제목 mentions
            for s in title_sents:
                if sentence_has_brand(s, b, patterns) and len(s) > 3:
                    brand_map[b].append({
                        "text": s,
                        "url": p.url,
                        "title": title,
                        "source": "title"
                    })
                    summary[b]["total_mentions"] += 1
                    post_has_brand[b] = True

            # 3) 본문 mentions
            for s in content_sents:
                if sentence_has_brand(s, b, patterns) and len(s) > 5:
                    brand_map[b].append({
                        "text": s,
                        "url": p.url,
                        "title": title,
                        "source": "content"
                    })
                    summary[b]["total_mentions"] += 1
                    post_has_brand[b] = True

            # 4) ✅ 댓글 mentions (전체)
            for s in comment_sents:
                if sentence_has_brand(s, b, patterns) and len(s) > 5:
                    brand_map[b].append({
                        "text": s,
                        "url": p.url,
                        "title": title,
                        "source": "comment"
                    })
                    summary[b]["comment_mentions"] += 1
                    summary[b]["total_mentions"] += 1
                    post_has_brand[b] = True

            # 5) (선택) 제목에 브랜드가 있으면 댓글 문장 추가 수집(부스트 표기만)
            # - 카운트는 이미 comment에서 함
            if title_has_brand[b]:
                for s in comment_sents:
                    if sentence_has_brand(s, b, patterns) and len(s) > 5:
                        brand_map[b].append({
                            "text": s,
                            "url": p.url,
                            "title": title,
                            "source": "comment(boosted_by_title)"
                        })

        # 포스트 수 집계(브랜드별 1회)
        for b in BRAND_LIST:
            if post_has_brand[b]:
                summary[b]["posts_count"] += 1

    # ✅ dedupe
    for b in BRAND_LIST:
        seen = set()
        uniq = []
        for item in brand_map[b]:
            key = (item.get("url", ""), item.get("text", ""), item.get("source", ""))
            if key in seen:
                continue
            seen.add(key)
            uniq.append(item)
        brand_map[b] = uniq

    summary_df = pd.DataFrame([
        {
            "brand": b,
            "posts_count": summary[b]["posts_count"],
            "title_hits": summary[b]["title_hits"],
            "comment_mentions": summary[b]["comment_mentions"],
            "total_mentions": summary[b]["total_mentions"],
        }
        for b in BRAND_LIST
    ])

    # ✅ Posts=0 AND TitleHits=0 제거
    summary_df = summary_df[~((summary_df["posts_count"] == 0) & (summary_df["title_hits"] == 0))].copy()

    # ✅ 컬럼비아 맨 위 고정
    summary_df["__pin_columbia"] = summary_df["brand"].apply(lambda x: 0 if x == "컬럼비아" else 1)
    summary_df = summary_df.sort_values(
        ["__pin_columbia", "total_mentions", "posts_count"],
        ascending=[True, False, False]
    ).drop(columns=["__pin_columbia"])

    return brand_map, summary_df

# =================================================================
# 4. HTML 생성 (reports/external_signal.html 고정)
#   - Hot Keywords 제거 -> Summary로 대체
# =================================================================
def export_portal(brand_map, summary_df: pd.DataFrame, out_path="reports/external_signal.html"):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    active_brands = [b for b in BRAND_LIST if len(brand_map.get(b, [])) > 0]

    # ✅ Summary HTML (Top5 노출 + 나머지 접기/펼치기)
    if summary_df is None or summary_df.empty:
        summary_html = """
        <div class="text-slate-500 font-bold">요약 데이터가 없습니다.</div>
        """
    else:
        df = summary_df.copy()
        top_n = 5
        top_df = df.head(top_n)
        rest_df = df.iloc[top_n:]

        def row_html(r):
            return f"""
            <tr class="border-b border-slate-100">
              <td class="py-3 pr-4 font-black text-slate-800">{r['brand']}</td>
              <td class="py-3 pr-4 text-slate-600 font-bold">{int(r['posts_count'])}</td>
              <td class="py-3 pr-4 text-slate-600 font-bold">{int(r['title_hits'])}</td>
              <td class="py-3 pr-4 text-slate-600 font-bold">{int(r['comment_mentions'])}</td>
              <td class="py-3 pr-4 text-blue-700 font-black">{int(r['total_mentions'])}</td>
            </tr>
            """

        top_rows = "".join(row_html(r) for _, r in top_df.iterrows())
        rest_rows = "".join(row_html(r) for _, r in rest_df.iterrows())

        toggle_html = ""
        if len(rest_df) > 0:
            toggle_html = f"""
            <div class="mt-5 flex items-center justify-between">
              <div class="text-xs text-slate-400 font-bold">
                Top {top_n} 노출 · 나머지 {len(rest_df)}개는 접혀있음
              </div>
              <button
                id="summaryToggleBtn"
                onclick="toggleSummaryRest()"
                class="px-4 py-2 rounded-xl bg-white/70 border border-white text-slate-700 font-black text-xs hover:bg-white transition"
              >
                나머지 펼치기 <i class="fa-solid fa-chevron-down ml-2"></i>
              </button>
            </div>
            <div id="summaryRestWrap" class="mt-4" style="display:none;">
              <div class="overflow-x-auto rounded-2xl border border-white bg-white/35">
                <table class="w-full text-sm">
                  <tbody>
                    {rest_rows}
                  </tbody>
                </table>
              </div>
            </div>
            """

        summary_html = f"""
        <div class="overflow-x-auto rounded-2xl border border-white bg-white/35">
          <table class="w-full text-sm">
            <thead>
              <tr class="text-left text-[11px] uppercase tracking-widest text-slate-400 border-b border-slate-100">
                <th class="py-3 pr-4">Brand</th>
                <th class="py-3 pr-4">Posts</th>
                <th class="py-3 pr-4">Title Hits</th>
                <th class="py-3 pr-4">Comment Mentions</th>
                <th class="py-3 pr-4">Total Mentions</th>
              </tr>
            </thead>
            <tbody>
              {top_rows}
            </tbody>
          </table>
        </div>
        {toggle_html}
        """

    tab_menu_html = ""
    content_area_html = ""

    if not active_brands:
        tab_menu_html = """
        <div class="px-6 py-4 rounded-2xl bg-white/60 border border-white text-slate-500 font-bold">
          최근 기간 내 브랜드 언급 데이터가 없습니다.
        </div>
        """
        content_area_html = """
        <div class="glass-card p-10">
          <div class="text-slate-800 font-black text-xl mb-2">데이터 없음</div>
          <div class="text-slate-500 font-medium">
            최근 수집 기간(TARGET_DAYS) 동안 해당 브랜드 키워드가 포함된 문장이 발견되지 않았습니다.<br/>
            갤러리/기간/브랜드 리스트를 조정해보세요.
          </div>
        </div>
        """
    else:
        for i, brand in enumerate(active_brands):
            active_class = "bg-[#002d72] text-white shadow-lg" if i == 0 else "bg-white/50 text-slate-500 hover:bg-white"

            tab_menu_html += f"""
            <button onclick="switchTab('{brand}')" id="tab-{brand}" class="tab-btn px-6 py-3 rounded-2xl font-black transition-all text-sm {active_class}">
                {brand} <span class="ml-1 opacity-60 text-xs">{len(brand_map[brand])}</span>
            </button>"""

            display_style = "grid" if i == 0 else "none"
            sentence_cards = ""

            for item in brand_map[brand]:
                title_short = (item.get('title') or "")[:28]
                if len(item.get('title') or "") > 28:
                    title_short += "..."

                src = item.get("source", "")
                if src == "comment(boosted_by_title)":
                    src_badge = '<span class="ml-2 px-2 py-1 rounded-full bg-blue-50 text-blue-700 text-[10px] font-black">TITLE→COMMENT</span>'
                elif src == "comment":
                    src_badge = '<span class="ml-2 px-2 py-1 rounded-full bg-emerald-50 text-emerald-700 text-[10px] font-black">COMMENT</span>'
                elif src == "content":
                    src_badge = '<span class="ml-2 px-2 py-1 rounded-full bg-slate-50 text-slate-600 text-[10px] font-black">CONTENT</span>'
                else:
                    src_badge = '<span class="ml-2 px-2 py-1 rounded-full bg-slate-50 text-slate-500 text-[10px] font-black">TITLE</span>'

                sentence_cards += f"""
                <div class="glass-card p-6 border-white/80 hover:scale-[1.01] transition-transform">
                    <div class="flex items-center justify-between mb-3">
                      <div class="text-[10px] font-black uppercase tracking-widest text-slate-400">SOURCE {src_badge}</div>
                    </div>

                    <p class="text-slate-700 font-medium leading-relaxed mb-5 italic">" {item.get('text','')} "</p>

                    <div class="flex items-center justify-between pt-4 border-t border-slate-100">
                        <span class="text-[10px] font-bold text-slate-400 uppercase tracking-widest">글제목: {title_short}</span>
                        <a href="{item.get('url','')}" target="_blank" class="px-4 py-2 bg-[#002d72] text-white text-[10px] font-black rounded-xl hover:bg-blue-600 transition-colors flex items-center gap-2">
                            원문 링크 열기 <i class="fa-solid fa-arrow-up-right"></i>
                        </a>
                    </div>
                </div>"""

            content_area_html += f"""
            <div id="content-{brand}" class="tab-content grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6" style="display: {display_style};">
                {sentence_cards}
            </div>"""

    now_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M")

    full_html = f"""
<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <title>Columbia M-OS Pro | External Signal (VOC)</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200;400;600;800&display=swap');
    :root {{ --brand: #002d72; --bg0: #f6f8fb; --bg1: #eef3f9; }}
    body {{ background: linear-gradient(180deg, var(--bg0), var(--bg1)); font-family: 'Plus Jakarta Sans', sans-serif; color: #0f172a; min-height: 100vh; }}
    .glass-card {{ background: rgba(255,255,255,0.55); backdrop-filter: blur(20px); border: 1px solid rgba(255,255,255,0.7); border-radius: 30px; box-shadow: 0 20px 50px rgba(0,45,114,0.05); }}
    .sidebar {{ background: rgba(255,255,255,0.7); backdrop-filter: blur(15px); border-right: 1px solid rgba(255,255,255,0.8); }}

    body.embedded aside {{ display:none !important; }}
    body.embedded header {{ display:none !important; }}
    body.embedded .sidebar {{ display:none !important; }}
    body.embedded main {{ padding: 24px !important; }}
    body.embedded .sticky {{ position: static !important; }}
  </style>
</head>
<body class="flex">
  <aside class="w-72 h-screen sticky top-0 sidebar hidden lg:flex flex-col p-8">
    <div class="flex items-center gap-4 mb-16 px-2">
      <div class="w-12 h-12 bg-[#002d72] rounded-2xl flex items-center justify-center text-white shadow-xl shadow-blue-900/20">
        <i class="fa-solid fa-mountain-sun text-xl"></i>
      </div>
      <div>
        <div class="text-xl font-black tracking-tighter italic">M-OS <span class="text-blue-600 font-extrabold">PRO</span></div>
        <div class="text-[9px] font-black uppercase tracking-[0.3em] text-slate-400">Marketing Portal</div>
      </div>
    </div>
    <nav class="space-y-4">
      <div class="p-4 rounded-2xl bg-white shadow-sm text-[#002d72] font-black flex items-center gap-4">
        <i class="fa-solid fa-tower-broadcast"></i> <span>Live VOC 분석</span>
      </div>
      <div class="p-4 rounded-2xl text-slate-400 font-bold flex items-center gap-4 hover:bg-white/50 transition-all cursor-not-allowed">
        <i class="fa-solid fa-chart-line"></i> <span>시장 지수</span>
      </div>
    </nav>
  </aside>

  <main class="flex-1 p-8 md:p-16">
    <header class="flex flex-col md:flex-row md:items-center justify-between mb-10 gap-6">
      <div>
        <h1 class="text-5xl font-black tracking-tight text-slate-900 mb-4">VOC Real-time Analysis</h1>
  <div class=\"text-sm text-slate-500 mt-2\">__PERIOD_LABEL__</div>
        <p class="text-slate-500 text-lg font-medium italic">디시인사이드 등산 갤러리 브랜드 언급 데이터</p>
        <p class="text-slate-400 text-xs mt-2 font-bold">기간: 최근 {TARGET_DAYS}일 · 최대 {MAX_PAGES}페이지 스캔</p>
      </div>
      <div class="glass-card px-6 py-4 flex items-center gap-4">
        <div class="flex h-3 w-3 relative">
          <span class="animate-ping absolute h-full w-full rounded-full bg-blue-400 opacity-75"></span>
          <span class="relative inline-flex rounded-full h-3 w-3 bg-blue-600"></span>
        </div>
        <span class="text-sm font-black text-slate-800 tracking-widest uppercase">{now_str}</span>
      </div>
    </header>

    <section class="glass-card p-10 mb-12">
      <h3 class="text-[10px] font-black uppercase tracking-[0.3em] text-blue-600 mb-6 flex items-center gap-2">
        <i class="fa-solid fa-chart-simple"></i> Mention Summary
      </h3>
      <div class="text-xs text-slate-500 font-bold mb-6">
        Posts = 해당 브랜드가 1회 이상 언급된 글 수 · Title Hits = 제목 언급 글 수 · Comment Mentions = 댓글 언급 문장 수 · Total Mentions = 전체 문장 언급 수
      </div>
      {summary_html}
    </section>

    <section>
      <div class="flex flex-wrap gap-2 mb-8">
        {tab_menu_html}
      </div>

      <div class="min-h-[500px]">
        {content_area_html}
      </div>
    </section>
  </main>

  <script>
    function switchTab(brand) {{
      document.querySelectorAll('.tab-content').forEach(el => el.style.display = 'none');
      const target = document.getElementById('content-' + brand);
      if (target) target.style.display = 'grid';

      document.querySelectorAll('.tab-btn').forEach(btn => {{
        btn.classList.remove('bg-[#002d72]', 'text-white', 'shadow-lg');
        btn.classList.add('bg-white/50', 'text-slate-500');
      }});
      const activeBtn = document.getElementById('tab-' + brand);
      if (activeBtn) {{
        activeBtn.classList.add('bg-[#002d72]', 'text-white', 'shadow-lg');
        activeBtn.classList.remove('bg-white/50', 'text-slate-500');
      }}
    }}

    function toggleSummaryRest() {{
      const wrap = document.getElementById('summaryRestWrap');
      const btn = document.getElementById('summaryToggleBtn');
      if (!wrap || !btn) return;

      const opened = wrap.style.display !== 'none';
      if (opened) {{
        wrap.style.display = 'none';
        btn.innerHTML = '나머지 펼치기 <i class="fa-solid fa-chevron-down ml-2"></i>';
      }} else {{
        wrap.style.display = 'block';
        btn.innerHTML = '접기 <i class="fa-solid fa-chevron-up ml-2"></i>';
      }}
    }}
  </script>

  <script>
    (function () {{
      try {{
        if (window.self !== window.top) document.body.classList.add("embedded");
      }} catch (e) {{
        document.body.classList.add("embedded");
      }}
    }})();
  </script>
</body>
</html>
"""

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(full_html)

    print(f"✅ [성공] External Signal 리포트 생성 완료: {out_path}")

# =================================================================
# main
# =================================================================
if __name__ == "__main__":
    raw_data = crawl_dc_engine(days=TARGET_DAYS)
    if raw_data:
        brand_map, summary_df = process_data(raw_data)
        export_portal(brand_map, summary_df)
    else:
        export_portal({b: [] for b in BRAND_LIST}, pd.DataFrame())
        print("⚠️ 수집 데이터 0건 (빈 리포트 생성)")
