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
TARGET_DAYS = int(os.getenv('TARGET_DAYS', '7'))  # 최근 N일(오늘 포함) 데이터 대상
DEBUG = os.getenv('DEBUG', '0').strip() in ('1','true','TRUE','yes','Y')

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
            resp = SESSION.get(url, timeout=10, verify=False)
            if DEBUG:
                print(f"[DEBUG] GET {url} -> {resp.status_code}")
        except Exception as e:
            if DEBUG:
                print(f"[DEBUG] request failed: {e}")
            break

        if resp.status_code != 200:
            if DEBUG:
                print(f"[DEBUG] non-200 status, stop at page {page}")
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
def export_portal(brand_map, summary_df: pd.DataFrame, raw_posts: List[Post] | None = None, out_path: str = "reports/external_signal.html"):
    """Build external signal portal (HTML) + write summary.json for Hub."""

    raw_posts = raw_posts or []

    # -----------------------------
    # Weekly (last 7 days) cumulative + daily trend
    # -----------------------------
    patterns = build_brand_patterns()

    def _post_day_kst(p: Post) -> str:
        try:
            dt = p.created_at
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=KST)
            return dt.astimezone(KST).strftime("%Y-%m-%d")
        except Exception:
            return "unknown"

    # Per-day totals (mentions, posts)
    daily = {}
    for p in raw_posts:
        day = _post_day_kst(p)
        if day not in daily:
            daily[day] = {"posts": 0, "mentions": 0}
        daily[day]["posts"] += 1

        title = normalize_text(p.title)
        comments = normalize_text(p.comments)

        comment_sents = split_sentences(comments)

        for b in BRAND_LIST:
            title_hit = 1 if contains_brand(title, b, patterns) else 0
            if title_hit:
                daily[day]["mentions"] += 1  # title mention counts as 1

            # comment sentence mentions (same counting concept as summary: sentence-level)
            cm = 0
            for s in comment_sents:
                if sentence_has_brand(s, b, patterns):
                    cm += 1
            # if title has brand, also include boosted mentions from title+content snippet (already handled elsewhere);
            # here we keep it simple and count comment sentence mentions only.
            daily[day]["mentions"] += cm

    # Sort recent days desc
    daily_rows = []
    for d in sorted(daily.keys(), reverse=True):
        daily_rows.append({"date": d, "posts": daily[d]["posts"], "mentions": daily[d]["mentions"]})
    daily_df = pd.DataFrame(daily_rows)

    # Week cumulative (last 7 unique days available)
    week_dates = list(daily_df["date"].head(7)) if not daily_df.empty else []
    week_posts = int(daily_df.head(7)["posts"].sum()) if not daily_df.empty else 0
    week_mentions = int(daily_df.head(7)["mentions"].sum()) if not daily_df.empty else 0

    # Optional previous week (if we crawled >= 14 days)
    prev_week_posts = int(daily_df.iloc[7:14]["posts"].sum()) if len(daily_df) >= 14 else None
    prev_week_mentions = int(daily_df.iloc[7:14]["mentions"].sum()) if len(daily_df) >= 14 else None

    def _pct_change(cur: int, prev: int) -> str:
        if prev == 0:
            return "—"
        return f"{((cur - prev) / prev) * 100:+.1f}%"

    wow_posts = _pct_change(week_posts, prev_week_posts) if prev_week_posts is not None else None
    wow_mentions = _pct_change(week_mentions, prev_week_mentions) if prev_week_mentions is not None else None

    # -----------------------------
    # Hub summary.json export
    # -----------------------------
    try:
        active_brands = [b for b in BRAND_LIST if len(brand_map.get(b, [])) > 0]
        total_mentions = int(summary_df["total_mentions"].sum()) if summary_df is not None and (not summary_df.empty) and "total_mentions" in summary_df.columns else 0

        payload = {
            "updated_at": _now_kst_str(),
            "gallery_id": GALLERY_ID,
            "target_days": int(TARGET_DAYS),
            "max_pages": int(MAX_PAGES),
            "posts_collected": int(len(raw_posts)),
            "brands_active": int(len(active_brands)),
            "total_mentions": total_mentions,
            "week_posts": week_posts,
            "week_mentions": week_mentions,
            "top5": (summary_df.head(5).to_dict(orient="records") if summary_df is not None and not summary_df.empty else []),
        }
        if len(raw_posts) == 0:
            payload["warning"] = "no_posts_collected"
        if summary_df is None or summary_df.empty:
            payload["warning"] = (payload.get("warning", "") + "|no_brand_mentions").strip("|")

        _write_summary_json(os.path.dirname(out_path), "external_signal", payload)
    except Exception as e:
        print(f"[WARN] summary.json export failed: {e}")

    # -----------------------------
    # Build HTML
    # -----------------------------
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
            <tr class="border-b border-slate-200">
              <td class="py-2 pr-4 font-bold">{r['brand']}</td>
              <td class="py-2 pr-4 text-right tabular-nums">{int(r['posts_count'])}</td>
              <td class="py-2 pr-4 text-right tabular-nums">{int(r['title_hits'])}</td>
              <td class="py-2 pr-4 text-right tabular-nums">{int(r['comment_mentions'])}</td>
              <td class="py-2 text-right tabular-nums font-extrabold">{int(r['total_mentions'])}</td>
            </tr>
            """


        top_rows = "".join([row_html(r) for _, r in top_df.iterrows()])
        rest_rows = "".join([row_html(r) for _, r in rest_df.iterrows()]) if len(rest_df) else ""

        summary_html = f"""
        <div class="text-slate-700 font-extrabold mb-2">브랜드 언급 요약 (최근 {int(TARGET_DAYS)}일)</div>
        <div class="overflow-x-auto">
          <table class="w-full text-sm">
            <thead>
              <tr class="text-slate-500 border-b border-slate-200">
                <th class="py-2 text-left">Brand</th>
                <th class="py-2 text-right">Posts</th>
                <th class="py-2 text-right">Title hits</th>
                <th class="py-2 text-right">Comment mentions</th>
                <th class="py-2 text-right">Total</th>
              </tr>
            </thead>
            <tbody>
              {top_rows}
            </tbody>
          </table>
        </div>

        {'' if not rest_rows else f"""
        <button class='mt-3 text-xs font-bold text-blue-700 hover:underline' onclick="toggleMore()">+ 더보기</button>
        <div id='moreBox' class='mt-2 hidden overflow-x-auto'>
          <table class='w-full text-sm'>
            <tbody>
              {rest_rows}
            </tbody>
          </table>
        </div>
        """}
        """


    # ✅ Weekly cumulative + daily trend HTML
    if daily_df is None or daily_df.empty:
        weekly_html = """
        <div class="text-slate-500 font-bold">최근 일주일 추이 데이터가 없습니다.</div>
        """
    else:
        # Build bars relative to max mentions
        max_m = int(daily_df.head(7)["mentions"].max()) if not daily_df.head(7).empty else 1
        if max_m <= 0:
            max_m = 1

        trend_rows = []
        for _, r in daily_df.head(7).iterrows():
            w = int((int(r["mentions"]) / max_m) * 100)
            trend_rows.append(f"""
            <div class="flex items-center gap-3 py-1">
              <div class="w-24 text-xs text-slate-600 tabular-nums">{r['date']}</div>
              <div class="flex-1">
                <div class="h-2 rounded-full bg-slate-200 overflow-hidden">
                  <div class="h-2 bg-blue-600" style="width:{w}%"></div>
                </div>
              </div>
              <div class="w-20 text-right text-xs tabular-nums text-slate-700 font-bold">{int(r['mentions'])}</div>
              <div class="w-16 text-right text-xs tabular-nums text-slate-500">{int(r['posts'])}p</div>
            </div>
            """)

        wow_block = ""
        if wow_posts is not None and wow_mentions is not None:
            wow_block = f"""
            <div class="mt-2 text-xs text-slate-600">
              WoW (이전 7일 대비): Posts {wow_posts} · Mentions {wow_mentions}
            </div>
            """

        weekly_html = f"""
        <div class="text-slate-700 font-extrabold mb-2">최근 7일 누적</div>
        <div class="grid grid-cols-2 md:grid-cols-4 gap-2">
          <div class="p-3 rounded-xl bg-white border border-slate-200">
            <div class="text-xs text-slate-500 font-bold">Posts</div>
            <div class="text-xl font-extrabold tabular-nums">{week_posts}</div>
          </div>
          <div class="p-3 rounded-xl bg-white border border-slate-200">
            <div class="text-xs text-slate-500 font-bold">Mentions</div>
            <div class="text-xl font-extrabold tabular-nums">{week_mentions}</div>
          </div>
          <div class="p-3 rounded-xl bg-white border border-slate-200 col-span-2">
            <div class="text-xs text-slate-500 font-bold">Coverage</div>
            <div class="text-sm text-slate-700 font-bold">{(min(7, len(daily_df)))} days · Gallery: {GALLERY_ID}</div>
            {wow_block}
          </div>
        </div>

        <div class="mt-4">
          <div class="text-slate-700 font-extrabold mb-2">일자별 멘션 추이 (최근 7일)</div>
          <div class="p-3 rounded-2xl bg-white border border-slate-200">
            {''.join(trend_rows)}
            <div class="mt-2 text-[11px] text-slate-500">* Mentions는 제목 1회 + 댓글 문장 단위 언급을 합산한 근사치입니다.</div>
          </div>
        </div>
        """


    # ✅ Brand cards
    def brand_card_html(brand: str, items: List[dict]) -> str:
        if not items:
            return ""
        rows = []
        for it in items[:40]:  # protect size
            text = (it.get("text","") or "").strip()
            url = it.get("url","")
            title = (it.get("title","") or "").strip()
            src = it.get("source","")
            rows.append(f"""
              <div class="p-3 rounded-2xl bg-white border border-slate-200 hover:border-blue-300 transition">
                <div class="text-xs text-slate-500 font-bold mb-1">{src}</div>
                <a class="text-sm font-extrabold text-blue-700 hover:underline" href="{url}" target="_blank" rel="noopener noreferrer">{title[:120] if title else url}</a>
                <div class="mt-2 text-sm text-slate-700 leading-relaxed">{text}</div>
              </div>
            """)
        return f"""
        <section class="mt-6">
          <div class="flex items-baseline justify-between">
            <h3 class="text-lg font-extrabold text-slate-800">{brand}</h3>
            <div class="text-xs text-slate-500 font-bold tabular-nums">{len(items)} mentions</div>
          </div>
          <div class="mt-3 grid grid-cols-1 md:grid-cols-2 gap-3">
            {''.join(rows)}
          </div>
        </section>
        """


    brand_sections = []
    for b in BRAND_LIST:
        if len(brand_map.get(b, [])) > 0:
            brand_sections.append(brand_card_html(b, brand_map[b]))

    if not brand_sections:
        brand_sections_html = "<div class='mt-6 text-slate-500 font-bold'>브랜드 언급이 없습니다.</div>"
    else:
        brand_sections_html = "\n".join(brand_sections)

    updated = _now_kst_str()
    full_html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>External Signal | DCInside {GALLERY_ID}</title>

  <script src="https://cdn.tailwindcss.com"></script>

  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200;400;600;800&display=swap');
    :root {{ --brand:#002d72; --bg0:#f6f8fb; --bg1:#eef3f9; }}
    html, body {{ height: 100%; overflow: auto; }}
    body{{
      background: linear-gradient(180deg, var(--bg0), var(--bg1));
      font-family: 'Plus Jakarta Sans', sans-serif;
      color:#0f172a;
      min-height:100vh;
    }}
    .glass {{
      background: rgba(255,255,255,.65);
      border: 1px solid rgba(15,23,42,.08);
      box-shadow: 0 10px 30px rgba(2,6,23,.08);
      backdrop-filter: blur(10px);
    }}
    .embedded body {{ background: transparent !important; }}
  </style>
</head>

<body class="p-5 md:p-8">
  <div class="max-w-6xl mx-auto">
    <div class="glass rounded-3xl p-5 md:p-7">
      <div class="flex flex-col md:flex-row md:items-end md:justify-between gap-3">
        <div>
          <div class="text-xs text-slate-500 font-extrabold tracking-wide">EXTERNAL SIGNAL</div>
          <h1 class="text-2xl md:text-3xl font-extrabold text-slate-900">DCInside · {GALLERY_ID} (최근 {int(TARGET_DAYS)}일)</h1>
          <div class="mt-1 text-xs text-slate-500 font-bold">Updated: {updated} · Posts collected: {len(raw_posts):,} · Active brands: {len(active_brands):,}</div>
        </div>
        <div class="text-xs text-slate-600 font-bold">
          * 워크플로에서 이 step이 실패해도 넘어가도록 되어 있으면(continue-on-error), 리포트가 갱신 안 될 수 있어요.
        </div>
      </div>

      <div class="mt-6 grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div class="glass rounded-3xl p-5">
          {summary_html}
        </div>
        <div class="glass rounded-3xl p-5">
          {weekly_html}
        </div>
      </div>

      {brand_sections_html}
    </div>
  </div>

  <script>
    function toggleMore() {{
      var el = document.getElementById('moreBox');
      if (!el) return;
      el.classList.toggle('hidden');
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
        export_portal(brand_map, summary_df, raw_posts=raw_data)
    else:
        export_portal({b: [] for b in BRAND_LIST}, pd.DataFrame(), raw_posts=[])
        print("⚠️ 수집 데이터 0건 (빈 리포트 생성)")
