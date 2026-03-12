#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import json
import html
import urllib3
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional

# ================================================================
# Summary/meta export (Hub first-screen consumption)
# ================================================================
_KST = timezone(timedelta(hours=9))


def _safe_mkdir(p: str):
    os.makedirs(p, exist_ok=True)


def _write_summary_json(out_dir: str, report_key: str, payload: dict):
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
# 1. 공통 설정
# =================================================================
KST = timezone(timedelta(hours=9))
GALLERY_ID = "climbing"
BASE_URL = "https://gall.dcinside.com"
MAX_PAGES = int(os.getenv("MAX_PAGES", "100"))
TARGET_DAYS = int(os.getenv("TARGET_DAYS", "7"))
DEBUG = os.getenv("DEBUG", "0").strip().lower() in ("1", "true", "yes", "y")
NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID", "").strip()
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET", "").strip()
NAVER_DISPLAY = max(10, min(int(os.getenv("NAVER_DISPLAY", "100")), 100))
NAVER_CONTEXT_TERMS = [
    "등산", "산행", "아웃도어", "백패킹", "등산화", "바람막이", "플리스", "패딩", "배낭"
]

BRAND_LIST = [
    "컬럼비아", "노스페이스", "파타고니아", "아크테릭스", "블랙야크",
    "K2", "캠프라인", "살로몬", "호카", "마무트",
    "스노우피크", "내셔널지오그래픽", "디스커버리", "코오롱스포츠", "몬벨",
    "네파", "아이더", "노스케이프", "밀레", "라푸마",
    "헬리한센", "오스프리", "그레고리", "데상트", "나이키",
]

# 짧고 모호한 별칭(예: 파타, 아크, 내지, 코오롱)은 오탐이 잦아서 제외
SAFE_BRAND_ALIASES: Dict[str, List[str]] = {
    "노스페이스": ["The North Face", "NORTH FACE", "NORTHFACE"],
    "아크테릭스": ["Arc'teryx", "ARCTERYX"],
    "파타고니아": ["Patagonia", "PATAGONIA"],
    "살로몬": ["Salomon", "SALOMON"],
    "스노우피크": ["Snow Peak", "SNOW PEAK", "SNOWPEAK", "Snowpeak"],
    "내셔널지오그래픽": ["National Geographic", "NATIONAL GEOGRAPHIC", "NATIONALGEOGRAPHIC", "NatGeo", "NATGEO"],
    "코오롱스포츠": ["Kolon Sport", "KOLON SPORT", "KOLONSPORT"],
    "몬벨": ["몽벨", "Montbell", "MONTBELL"],
    "디스커버리": ["Discovery", "DISCOVERY"],
    "컬럼비아": ["Columbia", "COLUMBIA", "콜롬비아"],
    "블랙야크": ["Black Yak", "BLACK YAK", "BLACKYAK"],
    "네파": ["NEPA"],
    "아이더": ["EIDER"],
    "데상트": ["Descente", "DESCENTE"],
    "나이키": ["Nike", "NIKE"],
    "호카": ["HOKA", "Hoka"],
    "마무트": ["Mammut", "MAMMUT"],
    "캠프라인": ["CampLine", "CAMPLINE"],
    "오스프리": ["Osprey", "OSPREY"],
    "그레고리": ["Gregory", "GREGORY"],
    "헬리한센": ["Helly Hansen", "HELLY HANSEN", "HELLYHANSEN"],
    "라푸마": ["Lafuma", "LAFUMA"],
}

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
)


@dataclass
class Post:
    title: str
    url: str
    content: str
    comments: str
    created_at: datetime
    platform: str = "dcinside"
    source: str = ""
    query: str = ""


# =================================================================
# 2. 크롤링 엔진
# =================================================================
def crawl_dc_engine(days: int) -> List[Post]:
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
                continue

            a_tag = row.select_one("td.gall_tit a")
            if not a_tag:
                continue

            link = urljoin(BASE_URL, a_tag.get("href"))

            try:
                d_resp = SESSION.get(link, timeout=10, verify=False)
                if d_resp.status_code != 200:
                    continue

                d_soup = BeautifulSoup(d_resp.text, "html.parser")
                date_el = d_soup.select_one(".gall_date")
                if not date_el:
                    continue

                dt = datetime.strptime(
                    date_el.get_text(strip=True), "%Y.%m.%d %H:%M:%S"
                ).replace(tzinfo=KST)

                if dt.date() < start_date:
                    stop_signal = True
                    break

                content_el = d_soup.select_one(".write_div")
                content = content_el.get_text("\n", strip=True) if content_el else ""
                comments = "\n".join(
                    [c.get_text(strip=True) for c in d_soup.select(".comment_list .usertxt")]
                )

                posts.append(
                    Post(
                        title=a_tag.get_text(strip=True),
                        url=link,
                        content=content,
                        comments=comments,
                        created_at=dt,
                        platform="dcinside",
                        source="dcinside",
                    )
                )
            except Exception as e:
                if DEBUG:
                    print(f"[DEBUG] detail fetch failed: {link} | {e}")
                continue

        print(f"   - {page}페이지 완료 (누적 수집: {len(posts)})")

    return posts


def _clean_naver_html_text(s: str) -> str:
    s = html.unescape(s or "")
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _parse_naver_postdate(postdate: str) -> Optional[datetime]:
    postdate = (postdate or "").strip()
    for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y.%m.%d"):
        try:
            return datetime.strptime(postdate, fmt).replace(tzinfo=KST)
        except Exception:
            continue
    return None


def build_naver_queries() -> List[Tuple[str, str]]:
    queries: List[Tuple[str, str]] = []
    for brand in BRAND_LIST:
        queries.append((brand, brand))
        queries.append((brand, f"{brand} 등산"))
    return queries


def crawl_naver_cafe_engine(days: int) -> Tuple[List[Post], Optional[str]]:
    if not NAVER_CLIENT_ID or not NAVER_CLIENT_SECRET:
        msg = "NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 미설정"
        print(f"[WARN] {msg}")
        return [], msg

    headers = {
        "X-Naver-Client-Id": NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
    }

    start_date = (datetime.now(KST) - timedelta(days=days)).date()
    seen = set()
    posts: List[Post] = []

    print(f"🚀 [M-OS SYSTEM] NAVER Cafe Search API 분석 시작 (최근 {days}일)")

    for brand, query in build_naver_queries():
        params = {
            "query": query,
            "display": NAVER_DISPLAY,
            "start": 1,
            "sort": "date",
        }
        url = "https://openapi.naver.com/v1/search/cafearticle.json"
        try:
            resp = SESSION.get(url, headers=headers, params=params, timeout=15)
            if DEBUG:
                print(f"[DEBUG] NAVER {query} -> {resp.status_code}")
            if resp.status_code != 200:
                continue
            items = (resp.json() or {}).get("items", [])
        except Exception as e:
            if DEBUG:
                print(f"[DEBUG] NAVER request failed: {query} | {e}")
            continue

        brand_patterns = build_brand_patterns({brand: [brand] + SAFE_BRAND_ALIASES.get(brand, [])})

        for item in items:
            dt = _parse_naver_postdate(item.get("postdate", ""))
            if not dt or dt.date() < start_date:
                continue

            title = _clean_naver_html_text(item.get("title", ""))
            content = _clean_naver_html_text(item.get("description", ""))
            link = (item.get("link", "") or "").strip()
            combined = f"{title}\n{content}"

            if not contains_brand(combined, brand, brand_patterns):
                continue
            if query == brand and not any(ctx in combined.lower() for ctx in NAVER_CONTEXT_TERMS):
                continue

            key = (brand, link, title, dt.strftime("%Y-%m-%d"))
            if key in seen:
                continue
            seen.add(key)

            posts.append(
                Post(
                    title=title,
                    url=link,
                    content=content,
                    comments="",
                    created_at=dt,
                    platform="naver_cafe",
                    source="naver_cafe",
                    query=query,
                )
            )

        print(f"   - query='{query}' 완료 (누적 수집: {len(posts)})")

    return posts, None


# =================================================================
# 3. 텍스트 분석 유틸
# =================================================================
def normalize_text(s: str) -> str:
    return (s or "").strip()


def split_sentences(text: str) -> List[str]:
    if not text:
        return []
    parts = re.split(r"[.!?\n]", text)
    return [p.strip() for p in parts if len(p.strip()) >= 4]


def _token_to_pattern(token: str) -> str:
    token = token.strip()
    if not token:
        return ""

    if token.upper() == "K2":
        return r"(?<![A-Za-z0-9])K2(?![A-Za-z0-9])"

    # 영문/숫자 위주 토큰은 단어 경계를 사용
    if re.fullmatch(r"[A-Za-z0-9' +&.-]+", token):
        escaped = re.escape(token)
        return rf"(?<![A-Za-z0-9]){escaped}(?![A-Za-z0-9])"

    # 한글/혼합 토큰은 정확 문자열 사용
    return re.escape(token)


def build_brand_patterns(brand_alias_map: Optional[Dict[str, List[str]]] = None) -> Dict[str, re.Pattern]:
    if brand_alias_map is None:
        brand_alias_map = {b: [b] + SAFE_BRAND_ALIASES.get(b, []) for b in BRAND_LIST}

    patterns: Dict[str, re.Pattern] = {}
    for brand, tokens in brand_alias_map.items():
        cleaned = []
        for tok in tokens:
            pat = _token_to_pattern(tok)
            if pat:
                cleaned.append(pat)
        if not cleaned:
            cleaned.append(re.escape(brand))
        patterns[brand] = re.compile(r"(?:%s)" % "|".join(cleaned), re.IGNORECASE)
    return patterns


def contains_brand(text: str, brand: str, patterns: Dict[str, re.Pattern]) -> bool:
    return bool(patterns[brand].search(text or ""))


def sentence_has_brand(sentence: str, brand: str, patterns: Dict[str, re.Pattern]) -> bool:
    return bool(patterns[brand].search(sentence or ""))


# =================================================================
# 4. 데이터 분석
# =================================================================
def process_data(posts: List[Post]):
    patterns = build_brand_patterns()
    brand_map: Dict[str, List[dict]] = {b: [] for b in BRAND_LIST}

    for p in posts:
        title = normalize_text(p.title)
        content = normalize_text(p.content)
        comments = normalize_text(p.comments)

        source_blocks = [
            ("title", [title] if title else []),
            ("content", split_sentences(content)),
            ("comment", split_sentences(comments)),
        ]

        for brand in BRAND_LIST:
            for source_kind, sentences in source_blocks:
                for sentence in sentences:
                    if source_kind == "title" and len(sentence) <= 1:
                        continue
                    if source_kind != "title" and len(sentence) <= 5:
                        continue
                    if not sentence_has_brand(sentence, brand, patterns):
                        continue

                    brand_map[brand].append(
                        {
                            "text": sentence,
                            "url": p.url,
                            "title": title,
                            "source": source_kind,
                            "platform": p.platform,
                            "query": p.query,
                            "date": p.created_at.strftime("%Y-%m-%d"),
                        }
                    )

    # mention dedupe
    for brand in BRAND_LIST:
        seen = set()
        uniq = []
        for item in brand_map[brand]:
            key = (
                item.get("platform", ""),
                item.get("url", ""),
                item.get("source", ""),
                re.sub(r"\s+", " ", item.get("text", "").strip()),
            )
            if key in seen:
                continue
            seen.add(key)
            uniq.append(item)
        brand_map[brand] = uniq

    # summary를 deduped brand_map 기준으로 다시 계산
    rows = []
    for brand in BRAND_LIST:
        items = brand_map[brand]
        posts_count = len({(it["platform"], it["url"]) for it in items})
        title_hits = len([it for it in items if it["source"] == "title"])
        comment_mentions = len([it for it in items if it["source"] == "comment"])
        total_mentions = len(items)
        rows.append(
            {
                "brand": brand,
                "posts_count": posts_count,
                "title_hits": title_hits,
                "comment_mentions": comment_mentions,
                "total_mentions": total_mentions,
            }
        )

    summary_df = pd.DataFrame(rows)
    summary_df = summary_df[
        ~(
            (summary_df["posts_count"] == 0)
            & (summary_df["title_hits"] == 0)
            & (summary_df["comment_mentions"] == 0)
            & (summary_df["total_mentions"] == 0)
        )
    ].copy()

    if not summary_df.empty:
        summary_df["__pin_columbia"] = summary_df["brand"].apply(lambda x: 0 if x == "컬럼비아" else 1)
        summary_df = (
            summary_df.sort_values(
                ["__pin_columbia", "total_mentions", "posts_count", "title_hits"],
                ascending=[True, False, False, False],
            )
            .drop(columns=["__pin_columbia"])
            .reset_index(drop=True)
        )

    return brand_map, summary_df


# =================================================================
# 5. HTML 컴포넌트
# =================================================================
def summarize_source(raw_posts: List[Post], brand_map: Dict[str, List[dict]], summary_df: pd.DataFrame, source_name: str):
    platform_key = "dcinside" if source_name.lower().startswith("dc") else "naver_cafe"

    # 일자별 post 집계는 원본 posts 기준
    post_daily: Dict[str, set] = {}
    for p in raw_posts:
        dt = p.created_at
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KST)
        day = dt.astimezone(KST).strftime("%Y-%m-%d")
        post_daily.setdefault(day, set()).add(p.url)

    # 일자별 mention 집계는 deduped brand_map 기준
    mention_daily: Dict[str, int] = {}
    for brand in BRAND_LIST:
        for item in brand_map.get(brand, []):
            if item.get("platform") != platform_key:
                continue
            day = item.get("date", "")
            if not day:
                continue
            mention_daily[day] = mention_daily.get(day, 0) + 1

    all_days = sorted(set(post_daily.keys()) | set(mention_daily.keys()), reverse=True)
    daily_rows = []
    for day in all_days:
        daily_rows.append(
            {
                "date": day,
                "posts": len(post_daily.get(day, set())),
                "mentions": mention_daily.get(day, 0),
            }
        )

    daily_df = pd.DataFrame(daily_rows)
    week_posts = int(daily_df.head(7)["posts"].sum()) if not daily_df.empty else 0
    week_mentions = int(daily_df.head(7)["mentions"].sum()) if not daily_df.empty else 0
    active_brands = [
        b for b in BRAND_LIST
        if any(item.get("platform") == platform_key for item in brand_map.get(b, []))
    ]
    total_mentions = int(summary_df["total_mentions"].sum()) if summary_df is not None and not summary_df.empty else 0

    return {
        "source_name": source_name,
        "daily_df": daily_df,
        "week_posts": week_posts,
        "week_mentions": week_mentions,
        "active_brands": active_brands,
        "total_mentions": total_mentions,
    }


def _summary_table_html(summary_df: pd.DataFrame) -> str:
    if summary_df is None or summary_df.empty:
        return '<div class="text-slate-500 font-bold">요약 데이터가 없습니다.</div>'

    top_df = summary_df.head(5)
    rest_df = summary_df.iloc[5:]

    def row_html(r):
        return f'''
        <tr class="border-b border-slate-200">
          <td class="py-2 pr-4 font-bold">{html.escape(str(r["brand"]))}</td>
          <td class="py-2 pr-4 text-right tabular-nums">{int(r["posts_count"])}</td>
          <td class="py-2 pr-4 text-right tabular-nums">{int(r["title_hits"])}</td>
          <td class="py-2 pr-4 text-right tabular-nums">{int(r["comment_mentions"])}</td>
          <td class="py-2 text-right tabular-nums font-extrabold">{int(r["total_mentions"])}</td>
        </tr>
        '''

    rest_rows = "".join([row_html(r) for _, r in rest_df.iterrows()])
    rest_block = ""
    if rest_rows:
        rest_block = '''
        <button class="mt-3 text-xs font-bold text-blue-700 hover:underline" onclick="toggleMore(this)">+ 더보기</button>
        <div class="mt-2 hidden overflow-x-auto more-box">
          <table class="w-full text-sm"><tbody>''' + rest_rows + '''</tbody></table>
        </div>
        '''

    return f'''
    <div class="text-slate-700 font-extrabold mb-2">브랜드 언급 요약 (최근 {int(TARGET_DAYS)}일)</div>
    <div class="overflow-x-auto">
      <table class="w-full text-sm">
        <thead>
          <tr class="text-slate-500 border-b border-slate-200">
            <th class="py-2 text-left">Brand</th>
            <th class="py-2 text-right">Posts</th>
            <th class="py-2 text-right">Title hits</th>
            <th class="py-2 text-right">Comment mentions</th>
            <th class="py-2 text-right">Total mentions</th>
          </tr>
        </thead>
        <tbody>{''.join([row_html(r) for _, r in top_df.iterrows()])}</tbody>
      </table>
    </div>
    <div class="mt-2 text-[11px] text-slate-500">* Total mentions = dedupe 후 title/content/comment 문장 언급 수</div>
    {rest_block}
    '''


def _weekly_html(meta: dict, source_label: str) -> str:
    daily_df = meta["daily_df"]
    if daily_df is None or daily_df.empty:
        return '<div class="text-slate-500 font-bold">최근 일주일 추이 데이터가 없습니다.</div>'

    max_m = int(daily_df.head(7)["mentions"].max()) if not daily_df.head(7).empty else 1
    if max_m <= 0:
        max_m = 1

    trend_rows = []
    for _, r in daily_df.head(7).iterrows():
        w = int((int(r["mentions"]) / max_m) * 100)
        trend_rows.append(
            f'''
            <div class="flex items-center gap-3 py-1">
              <div class="w-24 text-xs text-slate-600 tabular-nums">{r['date']}</div>
              <div class="flex-1"><div class="h-2 rounded-full bg-slate-200 overflow-hidden"><div class="h-2 bg-blue-600" style="width:{w}%"></div></div></div>
              <div class="w-20 text-right text-xs tabular-nums text-slate-700 font-bold">{int(r['mentions'])}</div>
              <div class="w-16 text-right text-xs tabular-nums text-slate-500">{int(r['posts'])}p</div>
            </div>
            '''
        )

    return f'''
    <div class="text-slate-700 font-extrabold mb-2">최근 7일 누적</div>
    <div class="grid grid-cols-2 md:grid-cols-4 gap-2">
      <div class="p-3 rounded-xl bg-white border border-slate-200">
        <div class="text-xs text-slate-500 font-bold">Posts</div>
        <div class="text-xl font-extrabold tabular-nums">{meta['week_posts']}</div>
      </div>
      <div class="p-3 rounded-xl bg-white border border-slate-200">
        <div class="text-xs text-slate-500 font-bold">Mentions</div>
        <div class="text-xl font-extrabold tabular-nums">{meta['week_mentions']}</div>
      </div>
      <div class="p-3 rounded-xl bg-white border border-slate-200 col-span-2">
        <div class="text-xs text-slate-500 font-bold">Coverage</div>
        <div class="text-sm text-slate-700 font-bold">{min(7, len(daily_df))} days · Source: {html.escape(source_label)}</div>
      </div>
    </div>
    <div class="mt-4">
      <div class="text-slate-700 font-extrabold mb-2">일자별 멘션 추이 (최근 7일)</div>
      <div class="p-3 rounded-2xl bg-white border border-slate-200">
        {''.join(trend_rows)}
        <div class="mt-2 text-[11px] text-slate-500">* Mentions는 dedupe 후 title/content/comment 문장 단위 브랜드 언급 합산입니다.</div>
      </div>
    </div>
    '''


def _brand_sections_html(brand_map: Dict[str, List[dict]], platform: str) -> str:
    sections = []
    for brand in BRAND_LIST:
        items = [x for x in brand_map.get(brand, []) if x.get("platform") == platform]
        if not items:
            continue

        # 동일 URL 카드 중복 방지: URL당 최대 2개 문장만 노출
        grouped: Dict[str, List[dict]] = {}
        for item in items:
            grouped.setdefault(item.get("url") or f"__no_url__::{len(grouped)}", []).append(item)

        cards = []
        displayed = 0
        for url, group in grouped.items():
            if displayed >= 40:
                break
            group = sorted(group, key=lambda x: {"title": 0, "content": 1, "comment": 2}.get(x.get("source", ""), 9))
            chosen = group[:2]
            title = html.escape((chosen[0].get("title") or chosen[0].get("url") or "")[:120])
            date = html.escape(chosen[0].get("date") or "")
            query = html.escape(chosen[0].get("query") or "")
            query_badge = f'<span class="px-2 py-1 rounded-full bg-slate-100">query: {query}</span>' if query else ''
            source_badges = " ".join(
                f'<span class="px-2 py-1 rounded-full bg-slate-100">{html.escape(it.get("source") or "")}</span>'
                for it in chosen
            )
            lines = "".join(
                f'<div class="mt-2 text-sm text-slate-700 leading-relaxed">• {html.escape((it.get("text") or "").strip())}</div>'
                for it in chosen
            )
            cards.append(
                f'''
                <div class="p-3 rounded-2xl bg-white border border-slate-200 hover:border-blue-300 transition">
                  <div class="flex flex-wrap gap-2 text-[11px] text-slate-500 font-bold mb-1">
                    {source_badges}
                    <span class="px-2 py-1 rounded-full bg-slate-100">{date}</span>
                    {query_badge}
                  </div>
                  <a class="text-sm font-extrabold text-blue-700 hover:underline" href="{html.escape(url)}" target="_blank" rel="noopener noreferrer">{title}</a>
                  {lines}
                </div>
                '''
            )
            displayed += 1

        sections.append(
            f'''
            <section class="mt-6">
              <div class="flex items-baseline justify-between">
                <h3 class="text-lg font-extrabold text-slate-800">{html.escape(brand)}</h3>
                <div class="text-xs text-slate-500 font-bold tabular-nums">{len(items)} mentions</div>
              </div>
              <div class="mt-3 grid grid-cols-1 md:grid-cols-2 gap-3">{''.join(cards)}</div>
            </section>
            '''
        )
    if not sections:
        return "<div class='mt-6 text-slate-500 font-bold'>브랜드 언급이 없습니다.</div>"
    return "\n".join(sections)


def _source_panel_html(panel_id: str, title: str, subtitle: str, summary_html: str, weekly_html: str, sections_html: str, warning: str = "") -> str:
    warning_html = ""
    if warning:
        warning_html = f'''
        <div class="mt-4 p-3 rounded-2xl bg-amber-50 border border-amber-200 text-amber-800 text-sm font-bold">{html.escape(warning)}</div>
        '''
    return f'''
    <section id="{panel_id}" class="tab-panel hidden">
      <div class="mt-6 flex flex-col md:flex-row md:items-end md:justify-between gap-3">
        <div>
          <div class="text-xs text-slate-500 font-extrabold tracking-wide">EXTERNAL SIGNAL</div>
          <h2 class="text-2xl md:text-3xl font-extrabold text-slate-900">{html.escape(title)}</h2>
          <div class="mt-1 text-xs text-slate-500 font-bold">{html.escape(subtitle)}</div>
        </div>
      </div>
      {warning_html}
      <div class="mt-6 grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div class="glass rounded-3xl p-5">{summary_html}</div>
        <div class="glass rounded-3xl p-5">{weekly_html}</div>
      </div>
      {sections_html}
    </section>
    '''


# =================================================================
# 6. HTML 생성
# =================================================================
def export_portal(
    dc_posts: List[Post],
    dc_brand_map: Dict[str, List[dict]],
    dc_summary_df: pd.DataFrame,
    naver_posts: List[Post],
    naver_brand_map: Dict[str, List[dict]],
    naver_summary_df: pd.DataFrame,
    naver_warning: Optional[str] = None,
    out_path: str = "reports/external_signal.html",
):
    updated = _now_kst_str()

    dc_meta = summarize_source(dc_posts, dc_brand_map, dc_summary_df, "DCInside")
    naver_meta = summarize_source(naver_posts, naver_brand_map, naver_summary_df, "NAVER Cafe")

    try:
        payload = {
            "updated_at": updated,
            "target_days": int(TARGET_DAYS),
            "dcinside": {
                "posts_collected": int(len(dc_posts)),
                "brands_active": int(len(dc_meta["active_brands"])),
                "total_mentions": int(dc_meta["total_mentions"]),
                "week_posts": int(dc_meta["week_posts"]),
                "week_mentions": int(dc_meta["week_mentions"]),
                "top5": dc_summary_df.head(5).to_dict(orient="records") if not dc_summary_df.empty else [],
            },
            "naver_cafe": {
                "posts_collected": int(len(naver_posts)),
                "brands_active": int(len(naver_meta["active_brands"])),
                "total_mentions": int(naver_meta["total_mentions"]),
                "week_posts": int(naver_meta["week_posts"]),
                "week_mentions": int(naver_meta["week_mentions"]),
                "top5": naver_summary_df.head(5).to_dict(orient="records") if not naver_summary_df.empty else [],
                "warning": naver_warning or "",
            },
        }
        _write_summary_json(os.path.dirname(out_path), "external_signal", payload)
    except Exception as e:
        print(f"[WARN] summary.json export failed: {e}")

    dc_panel = _source_panel_html(
        panel_id="panel-dcinside",
        title=f"DCInside · {GALLERY_ID} (최근 {int(TARGET_DAYS)}일)",
        subtitle=f"Updated: {updated} · Posts collected: {len(dc_posts):,} · Active brands: {len(dc_meta['active_brands']):,}",
        summary_html=_summary_table_html(dc_summary_df),
        weekly_html=_weekly_html(dc_meta, "DCInside"),
        sections_html=_brand_sections_html(dc_brand_map, "dcinside"),
    )

    naver_subtitle = f"Updated: {updated} · Posts collected: {len(naver_posts):,} · Active brands: {len(naver_meta['active_brands']):,}"
    if NAVER_CLIENT_ID and NAVER_CLIENT_SECRET:
        naver_subtitle += " · Query mode: brand / brand+등산"

    naver_panel = _source_panel_html(
        panel_id="panel-naver",
        title=f"네이버 카페 · 브랜드 언급 (최근 {int(TARGET_DAYS)}일)",
        subtitle=naver_subtitle,
        summary_html=_summary_table_html(naver_summary_df),
        weekly_html=_weekly_html(naver_meta, "NAVER Cafe Search API"),
        sections_html=_brand_sections_html(naver_brand_map, "naver_cafe"),
        warning=naver_warning or "",
    )

    full_html = f'''<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>External Signal | DCInside + NAVER Cafe</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200;400;600;800&display=swap');
    :root {{ --brand:#002d72; --bg0:#f6f8fb; --bg1:#eef3f9; }}
    html, body {{ height: 100%; overflow: auto; }}
    body {{
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
    .tab-btn.active {{ background:#0f172a; color:#fff; border-color:#0f172a; }}
    .embedded body {{ background: transparent !important; }}
  </style>
</head>
<body class="p-5 md:p-8">
  <div class="max-w-6xl mx-auto">
    <div class="glass rounded-3xl p-5 md:p-7">
      <div class="flex flex-col md:flex-row md:items-end md:justify-between gap-3">
        <div>
          <div class="text-xs text-slate-500 font-extrabold tracking-wide">EXTERNAL SIGNAL HUB</div>
          <h1 class="text-2xl md:text-3xl font-extrabold text-slate-900">DCInside + 네이버 카페 브랜드 언급 모니터링</h1>
          <div class="mt-1 text-xs text-slate-500 font-bold">Updated: {updated} · Target window: 최근 {int(TARGET_DAYS)}일</div>
        </div>
        <div class="text-xs text-slate-600 font-bold">브랜드 수: {len(BRAND_LIST)} · 탭별로 소스 분리 확인</div>
      </div>

      <div class="mt-6 flex flex-wrap gap-2">
        <button class="tab-btn active px-4 py-2 rounded-2xl border border-slate-300 bg-white text-sm font-extrabold" data-target="panel-dcinside">DCInside</button>
        <button class="tab-btn px-4 py-2 rounded-2xl border border-slate-300 bg-white text-sm font-extrabold" data-target="panel-naver">네이버 카페</button>
      </div>

      {dc_panel}
      {naver_panel}
    </div>
  </div>

  <script>
    function toggleMore(btn) {{
      const box = btn.parentElement.querySelector('.more-box');
      if (!box) return;
      box.classList.toggle('hidden');
      btn.textContent = box.classList.contains('hidden') ? '+ 더보기' : '- 접기';
    }}

    (function () {{
      const buttons = Array.from(document.querySelectorAll('.tab-btn'));
      const panels = Array.from(document.querySelectorAll('.tab-panel'));
      function activate(targetId) {{
        buttons.forEach(btn => btn.classList.toggle('active', btn.dataset.target === targetId));
        panels.forEach(panel => panel.classList.toggle('hidden', panel.id !== targetId));
      }}
      buttons.forEach(btn => btn.addEventListener('click', () => activate(btn.dataset.target)));
      activate('panel-dcinside');
      try {{
        if (window.self !== window.top) document.body.classList.add('embedded');
      }} catch (e) {{
        document.body.classList.add('embedded');
      }}
    }})();
  </script>
</body>
</html>
'''

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(full_html)

    print(f"✅ [성공] External Signal 리포트 생성 완료: {out_path}")


# =================================================================
# main
# =================================================================
if __name__ == "__main__":
    dc_posts = crawl_dc_engine(days=TARGET_DAYS)
    dc_brand_map, dc_summary_df = process_data(dc_posts) if dc_posts else ({b: [] for b in BRAND_LIST}, pd.DataFrame())

    naver_posts, naver_warning = crawl_naver_cafe_engine(days=TARGET_DAYS)
    naver_brand_map, naver_summary_df = process_data(naver_posts) if naver_posts else ({b: [] for b in BRAND_LIST}, pd.DataFrame())

    export_portal(
        dc_posts=dc_posts,
        dc_brand_map=dc_brand_map,
        dc_summary_df=dc_summary_df,
        naver_posts=naver_posts,
        naver_brand_map=naver_brand_map,
        naver_summary_df=naver_summary_df,
        naver_warning=naver_warning,
    )

    if not dc_posts:
        print("⚠️ DCInside 수집 데이터 0건")
    if not naver_posts:
        print("⚠️ NAVER Cafe 수집 데이터 0건")
