#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import json
import html
import urllib3
import requests
import time
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlparse, parse_qs, urlencode
from dataclasses import dataclass
from typing import List, Dict, Tuple

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
TARGET_DAYS = int(os.getenv("TARGET_DAYS", "30"))
DEBUG = os.getenv("DEBUG", "0").strip().lower() in ("1", "true", "yes", "y")
NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID", "").strip()
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET", "").strip()
NAVER_DISPLAY = max(10, min(int(os.getenv("NAVER_DISPLAY", "100")), 100))
NAVER_CONTEXT_TERMS = [
    "등산", "산행", "아웃도어", "백패킹", "트레킹", "하이킹", "캠핑",
    "등산화", "트레일러닝", "바람막이", "플리스", "패딩", "자켓", "배낭",
    "고어텍스", "고어 텍스", "등산복", "방수", "보온성", "후기", "착용"
]
NAVER_QUERY_SUFFIXES = ["", " 등산", " 등산화", " 바람막이", " 플리스", " 자켓"]
NAVER_PAGES = max(1, int(os.getenv("NAVER_PAGES", "4")))
NAVER_ALLOWED_CAFE_URLS = [
    "https://cafe.naver.com/windstopper",
    "https://cafe.naver.com/hikingf",
    "https://cafe.naver.com/windstopper/353778",
    "https://cafe.naver.com/firstmountain",
    "https://cafe.naver.com/onefineday7080",
    "https://cafe.naver.com/awrara",
    "https://cafe.naver.com/dieselmania",
    "https://cafe.naver.com/casuallydressed",
    "https://cafe.naver.com/nyblog",
    "https://cafe.naver.com/fitthesize",
    "https://cafe.naver.com/tnvdla",
]
NAVER_CAFE_DISPLAY_NAMES = {
    "windstopper": "고윈클럽",
    "hikingf": "하이킹F",
    "firstmountain": "퍼스트마운틴",
    "onefineday7080": "원파인데이7080",
    "awrara": "아우라라",
    "dieselmania": "디젤매니아",
    "casuallydressed": "고아캐드",
    "nyblog": "딜공",
    "fitthesize": "핏더사이즈",
    "tnvdla": "티엔브이디엘에이",
}
NAVER_BLOCKED_MENU_URLS = [
    "https://cafe.naver.com/f-e/cafes/31116705/menus/9?viewType=L",
]
NAVER_BLOCKED_CAFE_MENU_KEYS = {
    ("31116705", "9"),
}
NAVER_BLOCKED_ARTICLE_URLS = [
    "https://cafe.naver.com/nyblog/2140287",
]
NAVER_BLOCKED_CAFE_ARTICLE_KEYS = {
    ("nyblog", "2140287"),
}
AMBIGUOUS_BRANDS = {"K2", "디스커버리", "데상트", "나이키", "내셔널지오그래픽"}

BRAND_CONTEXT_TERMS: Dict[str, List[str]] = {
    "K2": ["등산", "산행", "아웃도어", "등산화", "등산복", "트레킹", "하이킹", "방수", "고어텍스"],
    "디스커버리": ["등산", "산행", "아웃도어", "패딩", "플리스", "바람막이", "자켓", "트레킹"],
    "데상트": ["등산", "산행", "아웃도어", "러닝", "트레일", "바람막이", "자켓", "하이킹"],
    "나이키": ["등산", "산행", "아웃도어", "트레일", "하이킹", "트레킹", "러닝화", "등산화"],
    "내셔널지오그래픽": ["등산", "산행", "아웃도어", "패딩", "플리스", "바람막이", "자켓", "트레킹"],
}

NAVER_DETAIL_CACHE: Dict[str, tuple[str, datetime | None, str, str, str]] = {}
NAVER_REJECT_LOGS: List[dict] = []
NAVER_LAST_RUN_META: Dict[str, object] = {}

BRAND_LIST = [
    "컬럼비아", "노스페이스", "파타고니아", "아크테릭스", "블랙야크",
    "K2", "캠프라인", "살로몬", "호카", "마무트",
    "스노우피크", "내셔널지오그래픽", "디스커버리", "코오롱스포츠", "몬벨",
    "네파", "아이더", "노스케이프", "밀레", "라푸마",
    "헬리한센", "오스프리", "그레고리", "데상트", "나이키",
]

BRAND_ALIASES: Dict[str, List[str]] = {
    "노스페이스": ["TNF", "The North Face", "NORTHFACE", "NORTH FACE"],
    "아크테릭스": ["Arc'teryx", "ARCTERYX", "아크테릭스"],
    "파타고니아": ["Patagonia", "PATAGONIA"],
    "살로몬": ["Salomon", "SALOMON"],
    "스노우피크": ["Snow Peak", "SNOWPEAK", "Snowpeak"],
    "내셔널지오그래픽": ["National Geographic", "NATIONALGEOGRAPHIC", "NatGeo"],
    "코오롱스포츠": ["Kolon Sport", "KOLONSPORT", "Kolonsport"],
    "몬벨": ["몽벨", "Montbell", "MONTBELL"],
    "디스커버리": ["Discovery", "DISCOVERY"],
    "컬럼비아": ["Columbia", "COLUMBIA", "콜롬비아"],
    "블랙야크": ["Black Yak", "BLACKYAK"],
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
    cafe_key: str = ""
    cafe_name: str = ""


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

                cafe_key, cafe_name = _resolve_naver_cafe_fields(link, cafeurl, cafename, query)
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


def _parse_naver_postdate(postdate: str) -> datetime | None:
    postdate = (postdate or "").strip()
    for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y.%m.%d"):
        try:
            return datetime.strptime(postdate, fmt).replace(tzinfo=KST)
        except Exception:
            continue
    return None



def _get_brand_context_terms(brand: str) -> List[str]:
    return BRAND_CONTEXT_TERMS.get(brand, NAVER_CONTEXT_TERMS)


def _combined_has_context(text: str, brand: str) -> bool:
    low = (text or "").lower()
    return any(ctx.lower() in low for ctx in _get_brand_context_terms(brand))


def _is_within_days(dt: datetime | None, days: int) -> bool:
    if dt is None:
        return False
    cutoff = datetime.now(KST) - timedelta(days=max(1, int(days)))
    return dt >= cutoff


def _get_with_retry(url: str, *, headers=None, params=None, timeout=20, retries=3):
    last_exc = None
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, headers=headers, params=params, timeout=timeout)
            if resp.status_code not in (429, 500, 502, 503, 504):
                return resp
            time.sleep(1.0 * (attempt + 1))
        except Exception as e:
            last_exc = e
            time.sleep(1.0 * (attempt + 1))
    if last_exc:
        raise last_exc
    raise RuntimeError(f"request failed: {url}")


def _extract_naver_article_detail_cached(link: str, cafeurl: str = "", cafename: str = "") -> tuple[str, datetime | None, str, str, str]:
    key = _canonicalize_link(link).rstrip("/")
    if key and key in NAVER_DETAIL_CACHE:
        return NAVER_DETAIL_CACHE[key]
    result = _extract_naver_article_detail(link, cafeurl, cafename)
    if key:
        NAVER_DETAIL_CACHE[key] = result
    return result


def _naver_dedupe_key(brand: str, link: str, title: str) -> tuple:
    meta = _extract_naver_article_meta(link)
    cafe_id = meta.get("cafe_id") or ""
    article_id = meta.get("article_id") or ""
    if cafe_id and article_id:
        return (brand, cafe_id, article_id)
    return (brand, _canonicalize_link(link).rstrip("/"), re.sub(r"\s+", " ", title or "").strip())


def _naver_log_reject(item: dict, brand: str, query: str, reason: str, title: str, content: str, link: str, cafename: str, cafeurl: str, dt: datetime | None = None):
    NAVER_REJECT_LOGS.append(
        {
            "brand": brand,
            "query": query,
            "reason": reason,
            "title": title,
            "content_preview": (content or "")[:250],
            "link": link,
            "cafename": cafename,
            "cafeurl": cafeurl,
            "postdate": (dt.astimezone(KST).strftime("%Y-%m-%d %H:%M") if isinstance(dt, datetime) else ""),
        }
    )


def build_naver_queries() -> List[Tuple[str, str]]:
    queries: List[Tuple[str, str]] = []
    seen = set()
    for brand in BRAND_LIST:
        query = str(brand).strip()
        key = (brand, query)
        if not query or key in seen:
            continue
        seen.add(key)
        queries.append(key)
    return queries


def _brand_token_match(text: str, brand: str) -> bool:
    text = text or ""
    if not text:
        return False
    if brand == "K2":
        return bool(re.search(r"(?<![A-Za-z0-9])K2(?![A-Za-z0-9])", text, re.IGNORECASE))
    tokens = [brand] + BRAND_ALIASES.get(brand, [])
    pattern = r"(?:" + "|".join(re.escape(t) for t in tokens if t) + r")"
    return bool(re.search(pattern, text, re.IGNORECASE))


def _canonicalize_link(link: str) -> str:
    link = (link or "").strip()
    if not link:
        return ""
    return link.replace("http://", "https://")


def _extract_naver_cafe_id(url: str) -> str:
    url = _canonicalize_link(url)
    if not url:
        return ""
    try:
        parsed = urlparse(url)
        parts = [p for p in parsed.path.split("/") if p]
        if parsed.netloc.endswith("cafe.naver.com") and parts:
            if parts[0] == "f-e":
                m = re.search(r"/cafes/(\d+)", parsed.path)
                return m.group(1) if m else ""
            return parts[0].lower()
        return ""
    except Exception:
        return ""


def _extract_naver_article_meta(url: str) -> dict:
    url = _canonicalize_link(url)
    meta = {"cafe_id": "", "article_id": "", "menu_id": ""}
    if not url:
        return meta
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        path = parsed.path or ""

        if "clubid" in qs:
            meta["cafe_id"] = (qs.get("clubid") or [""])[0]
        if "articleid" in qs:
            meta["article_id"] = (qs.get("articleid") or [""])[0]
        if "menuid" in qs:
            meta["menu_id"] = (qs.get("menuid") or [""])[0]

        m = re.search(r"/cafes/(\d+)/articles/(\d+)", path)
        if m:
            meta["cafe_id"] = meta["cafe_id"] or m.group(1)
            meta["article_id"] = meta["article_id"] or m.group(2)

        m = re.search(r"/cafes/(\d+)/menus/(\d+)", path)
        if m:
            meta["cafe_id"] = meta["cafe_id"] or m.group(1)
            meta["menu_id"] = meta["menu_id"] or m.group(2)

        if parsed.netloc.endswith("cafe.naver.com") and not meta["cafe_id"]:
            parts = [p for p in path.split("/") if p]
            if parts and parts[0] != "f-e":
                meta["cafe_id"] = parts[0].lower()
                if len(parts) >= 2 and parts[1].isdigit():
                    meta["article_id"] = parts[1]
    except Exception:
        return meta
    return meta


def _is_blocked_naver_menu(link: str, cafeurl: str, html_text: str = "") -> bool:
    candidates = [_canonicalize_link(link).rstrip("/"), _canonicalize_link(cafeurl).rstrip("/")]
    blocked_urls = {_canonicalize_link(u).rstrip("/") for u in NAVER_BLOCKED_MENU_URLS}
    blocked_article_urls = {_canonicalize_link(u).rstrip("/") for u in NAVER_BLOCKED_ARTICLE_URLS}
    for cand in candidates:
        if cand in blocked_urls or cand in blocked_article_urls:
            return True
        meta = _extract_naver_article_meta(cand)
        if (meta.get("cafe_id") or "", meta.get("menu_id") or "") in NAVER_BLOCKED_CAFE_MENU_KEYS:
            return True
        if (meta.get("cafe_id") or "", meta.get("article_id") or "") in NAVER_BLOCKED_CAFE_ARTICLE_KEYS:
            return True

    if html_text:
        cafe_match = re.search(r'cafes/(\d+)', html_text)
        menu_match = re.search(r"(?:menus/|menuid[^0-9]{0,10})(\d+)", html_text)
        if cafe_match and menu_match:
            if (cafe_match.group(1), menu_match.group(1)) in NAVER_BLOCKED_CAFE_MENU_KEYS:
                return True
    return False


def _naver_mobile_article_url(cafe_id: str, article_id: str) -> str:
    return f"https://m.cafe.naver.com/ca-fe/web/cafes/{cafe_id}/articles/{article_id}"


def _extract_naver_article_detail(link: str, cafeurl: str = "", cafename: str = "") -> tuple[str, datetime | None, str, str, str]:
    link = _canonicalize_link(link)
    fallback_cafe = cafename or ""
    meta = _extract_naver_article_meta(link)
    article_url = link
    html_candidates = []
    final_url = link

    if meta.get("cafe_id") and meta.get("article_id"):
        article_url = _naver_mobile_article_url(meta["cafe_id"], meta["article_id"])

    fetch_urls = [u for u in [article_url, link] if u]
    seen_fetch = set()
    for fetch_url in fetch_urls:
        if fetch_url in seen_fetch:
            continue
        seen_fetch.add(fetch_url)
        try:
            resp = _get_with_retry(fetch_url, timeout=20)
            if resp.status_code != 200:
                continue
            final_url = _canonicalize_link(resp.url or fetch_url)
            html_candidates.append(resp.text or "")

            soup = BeautifulSoup(resp.text, "html.parser")
            iframe = soup.select_one("iframe#cafe_main")
            if iframe and iframe.get("src"):
                iframe_url = urljoin(final_url, iframe.get("src"))
                iresp = _get_with_retry(iframe_url, timeout=20)
                if iresp.status_code == 200:
                    final_url = _canonicalize_link(iresp.url or iframe_url)
                    html_candidates.append(iresp.text or "")
        except Exception:
            continue

    best_text = ""
    best_dt = None
    best_title = ""
    best_cafe = fallback_cafe

    title_selectors = [
        ".tit-box .title_text",
        ".ArticleTitle .title_text",
        "h3.title_text",
        "meta[property='og:title']",
        "title",
    ]
    content_selectors = [
        ".se-main-container",
        ".ContentRenderer",
        ".article_viewer",
        "#tbody",
        ".postArticle",
        ".article_container",
        ".ArticleContentBox .content",
        ".ArticleContentBox",
        "#postContent",
    ]
    cafe_selectors = [
        ".link_cafe", ".cafe_name", ".CafeViewer .cafe_name", "meta[property='og:article:author']"
    ]
    date_selectors = [
        ".article_info .date", ".ArticleContentBox .date", ".date", "span.date", "time"
    ]

    for raw_html in html_candidates:
        if not raw_html:
            continue
        if _is_blocked_naver_menu(final_url, cafeurl, raw_html):
            return "", None, final_url, best_cafe, ""

        soup = BeautifulSoup(raw_html, "html.parser")
        text_candidates = []
        for sel in content_selectors:
            for node in soup.select(sel):
                txt = node.get_text("\n", strip=True)
                if txt:
                    text_candidates.append(txt)
        if text_candidates:
            candidate = max(text_candidates, key=len)
            if len(candidate) > len(best_text):
                best_text = candidate

        for sel in title_selectors:
            node = soup.select_one(sel)
            if not node:
                continue
            if node.name == "meta":
                cand = (node.get("content") or "").strip()
            else:
                cand = node.get_text(" ", strip=True)
            if cand:
                best_title = cand
                break

        for sel in cafe_selectors:
            node = soup.select_one(sel)
            if not node:
                continue
            if node.name == "meta":
                cand = (node.get("content") or "").strip()
            else:
                cand = node.get_text(" ", strip=True)
            if cand:
                best_cafe = cand
                break

        for sel in date_selectors:
            node = soup.select_one(sel)
            if not node:
                continue
            raw = node.get("datetime") if node.name == "time" else node.get_text(" ", strip=True)
            raw = (raw or "").strip()
            m = re.search(r"(20\d{2})[.\-/](\d{1,2})[.\-/](\d{1,2})(?:[^0-9]+(\d{1,2}):(\d{2}))?", raw)
            if m:
                y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
                hh = int(m.group(4) or 0)
                mm = int(m.group(5) or 0)
                best_dt = datetime(y, mo, d, hh, mm, tzinfo=KST)
                break
        if not best_dt:
            m = re.search(r'"(?:addDate|writeDate|articleWriteDate|currentArticleDate)"\s*[:=]\s*"?(20\d{2}[.\-/]\d{1,2}[.\-/]\d{1,2}(?:\s+\d{1,2}:\d{2})?)', raw_html)
            if m:
                raw = m.group(1)
                m2 = re.search(r"(20\d{2})[.\-/](\d{1,2})[.\-/](\d{1,2})(?:\s+(\d{1,2}):(\d{2}))?", raw)
                if m2:
                    y, mo, d = int(m2.group(1)), int(m2.group(2)), int(m2.group(3))
                    hh = int(m2.group(4) or 0)
                    mm = int(m2.group(5) or 0)
                    best_dt = datetime(y, mo, d, hh, mm, tzinfo=KST)

    return best_text.strip(), best_dt, final_url, best_cafe, best_title.strip()


_NAVER_ALLOWED_CAFE_IDS = {
    _extract_naver_cafe_id(u)
    for u in NAVER_ALLOWED_CAFE_URLS
    if _extract_naver_cafe_id(u)
}
_NAVER_ALLOWED_ARTICLE_URLS = {
    _canonicalize_link(u).rstrip("/")
    for u in NAVER_ALLOWED_CAFE_URLS
    if len([p for p in urlparse(_canonicalize_link(u)).path.split("/") if p]) >= 2
}


def _is_allowed_naver_cafe(link: str, cafeurl: str) -> bool:
    link = _canonicalize_link(link).rstrip("/")
    cafeurl = _canonicalize_link(cafeurl).rstrip("/")

    if link in _NAVER_ALLOWED_ARTICLE_URLS:
        return True

    cafe_id = _extract_naver_cafe_id(cafeurl) or _extract_naver_cafe_id(link)
    return bool(cafe_id and cafe_id in _NAVER_ALLOWED_CAFE_IDS)


def _resolve_naver_cafe_fields(link: str, cafeurl: str, cafename: str, query: str) -> tuple[str, str]:
    meta = _extract_naver_article_meta(link)
    cafe_key = (
        _extract_naver_cafe_id(cafeurl)
        or meta.get("cafe_id")
        or _extract_naver_cafe_id(link)
        or ""
    ).strip()
    cafe_name = (cafename or NAVER_CAFE_DISPLAY_NAMES.get(cafe_key, "") or cafe_key or query or "").strip()
    return cafe_key, cafe_name


def _naver_item_keep(item: dict, brand: str, query: str) -> tuple[bool, str, datetime, str, str, str, str, str]:
    title = _clean_naver_html_text(item.get("title", ""))
    content = _clean_naver_html_text(item.get("description", ""))
    link = _canonicalize_link(item.get("link", ""))
    cafename = _clean_naver_html_text(item.get("cafename", ""))
    cafeurl = _canonicalize_link(item.get("cafeurl", ""))
    combined = f"{title} {content} {cafename}".strip()

    if not combined:
        return False, "empty", datetime.now(KST), title, content, link, cafename, cafeurl

    if _is_blocked_naver_menu(link, cafeurl):
        return False, "blocked_menu", datetime.now(KST), title, content, link, cafename, cafeurl

    if not _is_allowed_naver_cafe(link, cafeurl):
        return False, "cafe_not_allowed", datetime.now(KST), title, content, link, cafename, cafeurl

    full_content, actual_dt, final_link, actual_cafename, actual_title = _extract_naver_article_detail(link, cafeurl, cafename)
    if final_link:
        link = final_link
    if actual_cafename:
        cafename = _clean_naver_html_text(actual_cafename)
    if actual_title:
        title = _clean_naver_html_text(actual_title)
    if full_content:
        content = _clean_naver_html_text(full_content)

    combined = f"{title} {content} {cafename}".strip()
    if not combined:
        return False, "empty", datetime.now(KST), title, content, link, cafename, cafeurl

    if _is_blocked_naver_menu(link, cafeurl, content):
        return False, "blocked_menu", datetime.now(KST), title, content, link, cafename, cafeurl

    if not _brand_token_match(combined, brand):
        return False, "brand_miss", datetime.now(KST), title, content, link, cafename, cafeurl

    has_context = any(ctx.lower() in combined.lower() for ctx in NAVER_CONTEXT_TERMS)
    if brand in AMBIGUOUS_BRANDS and not has_context:
        return False, "ambiguous_no_context", datetime.now(KST), title, content, link, cafename, cafeurl

    stamped_dt = actual_dt or _parse_naver_postdate(item.get("postdate", "")) or datetime.now(KST)
    return True, "ok", stamped_dt, title, content, link, cafename, cafeurl


def crawl_naver_cafe_engine(days: int) -> Tuple[List[Post], str | None]:
    if not NAVER_CLIENT_ID or not NAVER_CLIENT_SECRET:
        msg = "NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 미설정"
        print(f"[WARN] {msg}")
        return [], msg

    headers = {
        "X-Naver-Client-Id": NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
    }

    seen = set()
    posts: List[Post] = []
    raw_total = 0
    kept_total = 0
    reason_totals = {"blocked_menu": 0, "cafe_not_allowed": 0, "brand_miss": 0, "ambiguous_no_context": 0, "empty": 0, "dup": 0}

    print(f"🚀 [M-OS SYSTEM] NAVER Cafe Search API 분석 시작 (지정 카페 whitelist 필터 적용 · 브랜드명 단일 query만 사용 · 검색결과 기준 · 실제 게시일 우선 사용 · query set window={days}d)")

    for brand, query in build_naver_queries():
        query_raw = 0
        query_kept = 0
        query_drop = {"blocked_menu": 0, "cafe_not_allowed": 0, "brand_miss": 0, "ambiguous_no_context": 0, "empty": 0, "dup": 0}

        for page_no in range(NAVER_PAGES):
            start = 1 + (page_no * NAVER_DISPLAY)
            params = {
                "query": query,
                "display": NAVER_DISPLAY,
                "start": start,
                "sort": "date",
            }
            url = "https://openapi.naver.com/v1/search/cafearticle.json"
            try:
                resp = SESSION.get(url, headers=headers, params=params, timeout=20)
                status = resp.status_code
                data = resp.json() if resp.status_code == 200 else {}
                items = (data or {}).get("items", [])
            except Exception as e:
                if DEBUG:
                    print(f"[DEBUG] NAVER request failed: {query} | start={start} | {e}")
                break

            raw_count = len(items)
            raw_total += raw_count
            query_raw += raw_count
            if raw_count == 0:
                if DEBUG:
                    print(f"[DEBUG] NAVER query={query} page={page_no+1} raw=0 -> break")
                break

            page_kept = 0
            page_drop = {"blocked_menu": 0, "cafe_not_allowed": 0, "brand_miss": 0, "ambiguous_no_context": 0, "empty": 0, "dup": 0}
            for item in items:
                keep, reason, dt, title, content, link, cafename, cafeurl = _naver_item_keep(item, brand, query)
                if not keep:
                    page_drop[reason] = page_drop.get(reason, 0) + 1
                    query_drop[reason] = query_drop.get(reason, 0) + 1
                    reason_totals[reason] = reason_totals.get(reason, 0) + 1
                    continue

                key = (brand, link or title, title, content)
                if key in seen:
                    page_drop["dup"] += 1
                    query_drop["dup"] += 1
                    reason_totals["dup"] += 1
                    continue
                seen.add(key)

                stamped_content = content
                if cafename:
                    stamped_content = f"[카페:{cafename}] {stamped_content}".strip()

                posts.append(
                    Post(
                        title=title,
                        url=link,
                        content=stamped_content,
                        comments="",
                        created_at=dt,
                        platform="naver_cafe",
                        source="naver_cafe",
                        query=cafename or query,
                        cafe_key=cafe_key,
                        cafe_name=cafe_name,
                    )
                )
                page_kept += 1
                query_kept += 1
                kept_total += 1

            print(
                f"   - query='{query}' page={page_no+1} status={status} raw={raw_count} kept={page_kept} total={len(posts)} "
                f"drop_menu={page_drop.get('blocked_menu',0)} drop_cafe={page_drop.get('cafe_not_allowed',0)} drop_brand={page_drop.get('brand_miss',0)} drop_ctx={page_drop.get('ambiguous_no_context',0)} "
                f"drop_empty={page_drop.get('empty',0)} drop_dup={page_drop.get('dup',0)}"
            )

        print(
            f"   ↳ query='{query}' 완료 raw={query_raw} kept={query_kept} 누적={len(posts)} "
            f"(blocked_menu={query_drop.get('blocked_menu',0)}, cafe_not_allowed={query_drop.get('cafe_not_allowed',0)}, brand_miss={query_drop.get('brand_miss',0)}, ambiguous_no_context={query_drop.get('ambiguous_no_context',0)}, dup={query_drop.get('dup',0)})"
        )

    if kept_total == 0:
        msg = (
            f"NAVER Cafe 결과 0건 (raw={raw_total}, kept={kept_total}) · "
            f"blocked_menu={reason_totals.get('blocked_menu',0)}, cafe_not_allowed={reason_totals.get('cafe_not_allowed',0)}, brand_miss={reason_totals.get('brand_miss',0)}, ambiguous_no_context={reason_totals.get('ambiguous_no_context',0)}, dup={reason_totals.get('dup',0)}"
        )
        return posts, msg

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


def build_brand_patterns() -> Dict[str, re.Pattern]:
    patterns = {}
    for b in BRAND_LIST:
        aliases = BRAND_ALIASES.get(b, [])
        tokens = [re.escape(b)] + [re.escape(a) for a in aliases]
        patterns[b] = re.compile(r"(" + "|".join(tokens) + r")", re.IGNORECASE)
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
    summary = {
        b: {"posts_count": 0, "title_hits": 0, "comment_mentions": 0, "total_mentions": 0}
        for b in BRAND_LIST
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
            if contains_brand(title, b, patterns):
                title_has_brand[b] = True
                summary[b]["title_hits"] += 1
                post_has_brand[b] = True

            for s in title_sents:
                if sentence_has_brand(s, b, patterns) and len(s) > 3:
                    brand_map[b].append(
                        {
                            "text": s,
                            "url": p.url,
                            "title": title,
                            "source": "title",
                            "platform": p.platform,
                            "query": p.query,
                            "cafe_key": p.cafe_key,
                            "cafe_name": p.cafe_name,
                            "date": p.created_at.strftime("%Y-%m-%d"),
                        }
                    )
                    summary[b]["total_mentions"] += 1
                    post_has_brand[b] = True

            for s in content_sents:
                if sentence_has_brand(s, b, patterns) and len(s) > 5:
                    brand_map[b].append(
                        {
                            "text": s,
                            "url": p.url,
                            "title": title,
                            "source": "content",
                            "platform": p.platform,
                            "query": p.query,
                            "cafe_key": p.cafe_key,
                            "cafe_name": p.cafe_name,
                            "date": p.created_at.strftime("%Y-%m-%d"),
                        }
                    )
                    summary[b]["total_mentions"] += 1
                    post_has_brand[b] = True

            for s in comment_sents:
                if sentence_has_brand(s, b, patterns) and len(s) > 5:
                    brand_map[b].append(
                        {
                            "text": s,
                            "url": p.url,
                            "title": title,
                            "source": "comment",
                            "platform": p.platform,
                            "query": p.query,
                            "cafe_key": p.cafe_key,
                            "cafe_name": p.cafe_name,
                            "date": p.created_at.strftime("%Y-%m-%d"),
                        }
                    )
                    summary[b]["comment_mentions"] += 1
                    summary[b]["total_mentions"] += 1
                    post_has_brand[b] = True

            if title_has_brand[b]:
                for s in comment_sents:
                    if sentence_has_brand(s, b, patterns) and len(s) > 5:
                        brand_map[b].append(
                            {
                                "text": s,
                                "url": p.url,
                                "title": title,
                                "source": "comment(boosted_by_title)",
                                "platform": p.platform,
                                "query": p.query,
                                "cafe_key": p.cafe_key,
                                "cafe_name": p.cafe_name,
                                "date": p.created_at.strftime("%Y-%m-%d"),
                            }
                        )

        for b in BRAND_LIST:
            if post_has_brand[b]:
                summary[b]["posts_count"] += 1

    for b in BRAND_LIST:
        seen = set()
        uniq = []
        for item in brand_map[b]:
            key = (
                item.get("url", ""),
                item.get("text", ""),
                item.get("source", ""),
                item.get("platform", ""),
            )
            if key in seen:
                continue
            seen.add(key)
            uniq.append(item)
        brand_map[b] = uniq
        summary[b]["total_mentions"] = len(uniq)
        summary[b]["comment_mentions"] = sum(1 for x in uniq if str(x.get("source", "")).startswith("comment"))

    summary_df = pd.DataFrame(
        [
            {
                "brand": b,
                "posts_count": summary[b]["posts_count"],
                "title_hits": summary[b]["title_hits"],
                "comment_mentions": summary[b]["comment_mentions"],
                "total_mentions": summary[b]["total_mentions"],
            }
            for b in BRAND_LIST
        ]
    )

    summary_df = summary_df[~((summary_df["posts_count"] == 0) & (summary_df["title_hits"] == 0))].copy()
    if not summary_df.empty:
        summary_df["__pin_columbia"] = summary_df["brand"].apply(lambda x: 0 if x == "컬럼비아" else 1)
        summary_df = (
            summary_df.sort_values(
                ["__pin_columbia", "total_mentions", "posts_count"],
                ascending=[True, False, False],
            )
            .drop(columns=["__pin_columbia"])
        )
    return brand_map, summary_df


# =================================================================
# 5. HTML 컴포넌트
# =================================================================
def summarize_source(raw_posts: List[Post], brand_map: Dict[str, List[dict]], summary_df: pd.DataFrame, source_name: str):
    def _post_day_kst(p: Post) -> str:
        dt = p.created_at
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KST)
        return dt.astimezone(KST).strftime("%Y-%m-%d")

    daily: Dict[str, Dict[str, int]] = {}
    for p in raw_posts:
        day = _post_day_kst(p)
        daily.setdefault(day, {"posts": 0, "mentions": 0})
        daily[day]["posts"] += 1

    for b in BRAND_LIST:
        for item in brand_map.get(b, []):
            day = item.get("date") or ""
            if not day:
                continue
            daily.setdefault(day, {"posts": 0, "mentions": 0})
            daily[day]["mentions"] += 1

    daily_rows = [
        {"date": d, "posts": daily[d]["posts"], "mentions": daily[d]["mentions"]}
        for d in sorted(daily.keys(), reverse=True)
    ]
    daily_df = pd.DataFrame(daily_rows)
    week_posts = int(daily_df.head(7)["posts"].sum()) if not daily_df.empty else 0
    week_mentions = int(daily_df.head(7)["mentions"].sum()) if not daily_df.empty else 0
    active_brands = [b for b in BRAND_LIST if len(brand_map.get(b, [])) > 0]
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
            <th class="py-2 text-right">Total</th>
          </tr>
        </thead>
        <tbody>{''.join([row_html(r) for _, r in top_df.iterrows()])}</tbody>
      </table>
    </div>
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
        <div class="mt-2 text-[11px] text-slate-500">* Mentions는 제목/본문/댓글 문장 단위 브랜드 언급 합산입니다.</div>
      </div>
    </div>
    '''


def _naver_allowed_cafe_catalog() -> List[dict]:
    cafes: List[dict] = []
    seen = set()
    for url in NAVER_ALLOWED_CAFE_URLS:
        cafe_key = _extract_naver_cafe_id(url)
        if not cafe_key or cafe_key in seen:
            continue
        seen.add(cafe_key)
        cafes.append(
            {
                "key": cafe_key,
                "label": NAVER_CAFE_DISPLAY_NAMES.get(cafe_key, cafe_key),
                "url": f"https://cafe.naver.com/{cafe_key}",
            }
        )
    return cafes


def _naver_active_cafe_catalog(brand_map: Dict[str, List[dict]]) -> List[dict]:
    active_keys = set()
    for items in brand_map.values():
        for item in items:
            if item.get("platform") != "naver_cafe":
                continue
            cafe_key = str(item.get("cafe_key") or "").strip()
            if cafe_key:
                active_keys.add(cafe_key)

    if not active_keys:
        return []

    ordered = []
    seen = set()
    for cafe in _naver_allowed_cafe_catalog():
        if cafe["key"] in active_keys:
            ordered.append(cafe)
            seen.add(cafe["key"])

    for cafe_key in sorted(active_keys):
        if cafe_key in seen:
            continue
        ordered.append(
            {
                "key": cafe_key,
                "label": NAVER_CAFE_DISPLAY_NAMES.get(cafe_key, cafe_key),
                "url": f"https://cafe.naver.com/{cafe_key}",
            }
        )
    return ordered


def _naver_scope_html() -> str:
    cafes = _naver_allowed_cafe_catalog()
    if not cafes:
        return ""
    chips = []
    for cafe in cafes:
        chips.append(
            f'<a class="px-2 py-1 rounded-full bg-white border border-slate-200 text-slate-600 hover:border-blue-300 hover:text-blue-700 transition" '
            f'href="{html.escape(cafe["url"])}" target="_blank" rel="noopener noreferrer">{html.escape(cafe["label"])}</a>'
        )
    return (
        '<div class="mt-5 p-4 rounded-2xl bg-slate-50 border border-slate-200">'
        '<div class="text-[11px] font-extrabold tracking-wide text-slate-500">NAVER CAFE SCOPE</div>'
        f'<div class="mt-1 text-sm font-bold text-slate-700">현재 크롤링 카페 {len(cafes)}곳</div>'
        '<div class="mt-3 flex flex-wrap gap-2 text-xs">'
        + "".join(chips)
        + '</div></div>'
    )


def _naver_cafe_filter_html(brand_map: Dict[str, List[dict]]) -> str:
    cafes = _naver_active_cafe_catalog(brand_map)
    if not cafes:
        return ""
    buttons = [
        '<button class="cafe-filter-btn active px-3 py-2 rounded-2xl border border-slate-300 bg-white text-sm font-extrabold" '
        'data-platform="naver_cafe" data-cafe="all">전체</button>'
    ]
    for cafe in cafes:
        buttons.append(
            f'<button class="cafe-filter-btn px-3 py-2 rounded-2xl border border-slate-300 bg-white text-sm font-extrabold" '
            f'data-platform="naver_cafe" data-cafe="{html.escape(cafe["key"])}">{html.escape(cafe["label"])}</button>'
        )
    return (
        '<div class="mt-4 p-4 rounded-2xl bg-white/70 border border-slate-200">'
        '<div class="text-[11px] font-extrabold tracking-wide text-slate-500">OPTIONAL CAFE FILTER</div>'
        '<div class="mt-1 text-sm font-bold text-slate-700">메인은 브랜드 기준으로 두고, 필요할 때만 카페별로 좁혀보기</div>'
        '<div class="mt-3 flex flex-wrap gap-2">'
        + "".join(buttons)
        + '</div></div>'
    )


def _brand_sections_html(brand_map: Dict[str, List[dict]], platform: str) -> str:
    sections = []
    for brand in BRAND_LIST:
        items = [x for x in brand_map.get(brand, []) if x.get("platform") == platform]
        if not items:
            continue
        cards = []
        for it in items[:40]:
            title = html.escape((it.get("title") or it.get("url") or "")[:120])
            text = html.escape((it.get("text") or "").strip())
            url = html.escape(it.get("url") or "")
            src = html.escape(it.get("source") or "")
            date = html.escape(it.get("date") or "")
            query = html.escape(it.get("query") or "")
            cafe_key = html.escape((it.get("cafe_key") or "").strip() or "unknown")
            cafe_name = html.escape((it.get("cafe_name") or "").strip())
            cafe_badge = cafe_name or query or cafe_key
            query_badge = f'<span class="px-2 py-1 rounded-full bg-slate-100">query: {query}</span>' if query and query != cafe_badge else ''
            cards.append(
                f'''
                <div class="mention-card p-3 rounded-2xl bg-white border border-slate-200 hover:border-blue-300 transition" data-platform="{html.escape(platform)}" data-cafe="{cafe_key}">
                  <div class="flex flex-wrap gap-2 text-[11px] text-slate-500 font-bold mb-1">
                    <span class="px-2 py-1 rounded-full bg-slate-100">{src}</span>
                    <span class="px-2 py-1 rounded-full bg-slate-100">{date}</span>
                    <span class="px-2 py-1 rounded-full bg-slate-100">cafe: {cafe_badge}</span>
                    {query_badge}
                  </div>
                  <a class="text-sm font-extrabold text-blue-700 hover:underline" href="{url}" target="_blank" rel="noopener noreferrer">{title}</a>
                  <div class="mt-2 text-sm text-slate-700 leading-relaxed">{text}</div>
                </div>
                '''
            )
        sections.append(
            f'''
            <section class="mt-6 brand-section" data-platform="{html.escape(platform)}">
              <div class="flex items-baseline justify-between">
                <h3 class="text-lg font-extrabold text-slate-800">{html.escape(brand)}</h3>
                <div class="text-xs text-slate-500 font-bold tabular-nums" data-role="mention-count">{len(items)} mentions</div>
              </div>
              <div class="mt-3 grid grid-cols-1 md:grid-cols-2 gap-3">{''.join(cards)}</div>
            </section>
            '''
        )
    if not sections:
        return "<div class='mt-6 text-slate-500 font-bold'>브랜드 언급이 없습니다.</div>"
    return "\n".join(sections) + f'\n<div class="cafe-filter-empty hidden mt-6 text-slate-500 font-bold" data-platform="{html.escape(platform)}">선택한 카페에 해당하는 브랜드 언급이 없습니다.</div>'


def _source_panel_html(panel_id: str, title: str, subtitle: str, summary_html: str, weekly_html: str, sections_html: str, warning: str = "", extra_html: str = "") -> str:
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
      {extra_html}
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
    naver_warning: str | None = None,
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
        mode_label = str((NAVER_LAST_RUN_META or {}).get("query_mode", "brand only"))
        raw_total = int((NAVER_LAST_RUN_META or {}).get("raw_total", 0))
        kept_total = int((NAVER_LAST_RUN_META or {}).get("kept_total", len(naver_posts)))
        detail_cache_size = int((NAVER_LAST_RUN_META or {}).get("detail_cache_size", 0))
        naver_subtitle += f" · Query mode: {mode_label} · Allowed cafes: {len(_NAVER_ALLOWED_CAFE_IDS)} · Raw: {raw_total:,} → Kept: {kept_total:,} · Detail cache: {detail_cache_size:,}"

    naver_reason_totals = (NAVER_LAST_RUN_META or {}).get("reason_totals", {}) or {}
    if naver_reason_totals:
        reason_line = (
            f"drop preview={int(naver_reason_totals.get('brand_miss_preview',0)):,}, "
            f"brand={int(naver_reason_totals.get('brand_miss',0)):,}, "
            f"context={int(naver_reason_totals.get('ambiguous_no_context',0)):,}, "
            f"old={int(naver_reason_totals.get('out_of_range',0)):,}, "
            f"dup={int(naver_reason_totals.get('dup',0)):,}"
        )
        naver_warning = f"{naver_warning} | {reason_line}" if naver_warning else reason_line

    naver_panel = _source_panel_html(
        panel_id="panel-naver",
        title="네이버 카페 · 브랜드 언급 / 필터링 결과",
        subtitle=naver_subtitle,
        summary_html=_summary_table_html(naver_summary_df),
        weekly_html=_weekly_html(naver_meta, "NAVER Cafe Search API (수집시각 기준)"),
        sections_html=_brand_sections_html(naver_brand_map, "naver_cafe"),
        warning=naver_warning or "",
        extra_html=_naver_scope_html() + _naver_cafe_filter_html(naver_brand_map),
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
    .tab-btn.active, .cafe-filter-btn.active {{ background:#0f172a; color:#fff; border-color:#0f172a; }}
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
          <div class="mt-1 text-xs text-slate-500 font-bold">Updated: {updated} · DCInside: 최근 {int(TARGET_DAYS)}일 / NAVER Cafe: Search API 결과</div>
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
      const cafeButtons = Array.from(document.querySelectorAll('.cafe-filter-btn'));
      function activate(targetId) {{
        buttons.forEach(btn => btn.classList.toggle('active', btn.dataset.target === targetId));
        panels.forEach(panel => panel.classList.toggle('hidden', panel.id !== targetId));
      }}
      function applyCafeFilter(platform, cafeKey) {{
        const scopedButtons = cafeButtons.filter(btn => btn.dataset.platform === platform);
        const sections = Array.from(document.querySelectorAll(`.brand-section[data-platform="${{platform}}"]`));
        const emptyBox = document.querySelector(`.cafe-filter-empty[data-platform="${{platform}}"]`);
        scopedButtons.forEach(btn => btn.classList.toggle('active', btn.dataset.cafe === cafeKey));
        let visibleSections = 0;
        sections.forEach(section => {{
          const cards = Array.from(section.querySelectorAll(`.mention-card[data-platform="${{platform}}"]`));
          let visibleCount = 0;
          cards.forEach(card => {{
            const show = cafeKey === 'all' || card.dataset.cafe === cafeKey;
            card.classList.toggle('hidden', !show);
            if (show) visibleCount += 1;
          }});
          section.classList.toggle('hidden', visibleCount === 0);
          if (visibleCount > 0) visibleSections += 1;
          const countNode = section.querySelector('[data-role="mention-count"]');
          if (countNode) countNode.textContent = `${{visibleCount}} mentions`;
        }});
        if (emptyBox) emptyBox.classList.toggle('hidden', visibleSections > 0);
      }}
      buttons.forEach(btn => btn.addEventListener('click', () => activate(btn.dataset.target)));
      cafeButtons.forEach(btn => btn.addEventListener('click', () => applyCafeFilter(btn.dataset.platform, btn.dataset.cafe)));
      activate('panel-dcinside');
      applyCafeFilter('naver_cafe', 'all');
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
    with open(out_path, 'w', encoding='utf-8') as f:
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

# ================= PATCH: full-width + weakly-supervised ML sentiment + brand filters =================
from collections import Counter

POSITIVE_HINTS = {
    "좋", "만족", "추천", "예쁘", "이쁘", "편하", "편안", "가볍", "튼튼", "따뜻", "따듯", "훌륭", "최고",
    "잘샀", "재구매", "실용", "탄탄", "마음에", "괜찮", "우수", "짱", "고급", "핏 좋", "기대 이상"
}
NEGATIVE_HINTS = {
    "별로", "아쉽", "실망", "무겁", "불편", "비싸", "두껍", "얇", "구림", "최악", "문제", "하자", "환불",
    "교환", "불량", "답답", "미끄", "후회", "냄새", "찢어", "약하", "애매"
}

def _sentiment_seed_label(text: str) -> int | None:
    s = (text or "").lower()
    pos = sum(1 for kw in POSITIVE_HINTS if kw in s)
    neg = sum(1 for kw in NEGATIVE_HINTS if kw in s)
    if pos >= 1 and neg == 0:
        return 1
    if neg >= 1 and pos == 0:
        return 0
    return None


def build_sentiment_model(posts: List[Post]):
    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.linear_model import LogisticRegression
    except Exception:
        return None, None
    train_texts, train_labels = [], []
    for p in posts:
        candidates = ([p.title] if p.title else []) + split_sentences(p.content) + split_sentences(p.comments)
        for sent in candidates:
            label = _sentiment_seed_label(sent)
            if label is None:
                continue
            train_texts.append(sent)
            train_labels.append(label)
    if len(train_texts) < 10 or len(set(train_labels)) < 2:
        return None, None
    vec = TfidfVectorizer(ngram_range=(1, 2), min_df=1, max_features=8000, sublinear_tf=True)
    X = vec.fit_transform(train_texts)
    clf = LogisticRegression(max_iter=1000, class_weight='balanced', random_state=42)
    clf.fit(X, train_labels)
    return vec, clf


def predict_sentiment_label(text: str, vec, clf) -> tuple[str, float]:
    seeded = _sentiment_seed_label(text)
    if vec is not None and clf is not None and text:
        try:
            proba = clf.predict_proba(vec.transform([text]))[0]
            neg_p = float(proba[0]); pos_p = float(proba[1])
            if max(pos_p, neg_p) >= 0.60:
                return ("positive", pos_p) if pos_p >= neg_p else ("negative", neg_p)
        except Exception:
            pass
    if seeded == 1:
        return "positive", 0.58
    if seeded == 0:
        return "negative", 0.58
    return "neutral", 0.0


def process_data(posts: List[Post]):
    patterns = build_brand_patterns()
    vec, clf = build_sentiment_model(posts)
    brand_map: Dict[str, List[dict]] = {b: [] for b in BRAND_LIST}
    summary = {b: {"posts_count": 0, "title_hits": 0, "comment_mentions": 0, "total_mentions": 0} for b in BRAND_LIST}

    for p in posts:
        title = normalize_text(p.title)
        content = normalize_text(p.content)
        comments = normalize_text(p.comments)
        title_sents = [title] if title else []
        content_sents = split_sentences(content)
        comment_sents = split_sentences(comments)
        post_has_brand = {b: False for b in BRAND_LIST}
        title_has_brand = {b: False for b in BRAND_LIST}

        def add_item(brand, sentence, source):
            sentiment, score = predict_sentiment_label(sentence, vec, clf)
            brand_map[brand].append({
                "text": sentence,
                "url": p.url,
                "title": title,
                "source": source,
                "platform": p.platform,
                "query": p.query,
                "date": p.created_at.strftime("%Y-%m-%d"),
                "sentiment": sentiment,
                "sentiment_score": round(score, 3),
            })

        for b in BRAND_LIST:
            if contains_brand(title, b, patterns):
                title_has_brand[b] = True
                summary[b]["title_hits"] += 1
                post_has_brand[b] = True
            for s in title_sents:
                if sentence_has_brand(s, b, patterns) and len(s) > 3:
                    add_item(b, s, "title")
                    summary[b]["total_mentions"] += 1
                    post_has_brand[b] = True
            for s in content_sents:
                if sentence_has_brand(s, b, patterns) and len(s) > 5:
                    add_item(b, s, "content")
                    summary[b]["total_mentions"] += 1
                    post_has_brand[b] = True
            for s in comment_sents:
                if sentence_has_brand(s, b, patterns) and len(s) > 5:
                    add_item(b, s, "comment")
                    summary[b]["comment_mentions"] += 1
                    summary[b]["total_mentions"] += 1
                    post_has_brand[b] = True
            if title_has_brand[b]:
                for s in comment_sents:
                    if sentence_has_brand(s, b, patterns) and len(s) > 5:
                        add_item(b, s, "comment(boosted_by_title)")
        for b in BRAND_LIST:
            if post_has_brand[b]:
                summary[b]["posts_count"] += 1

    for b in BRAND_LIST:
        seen = set(); uniq = []
        for item in brand_map[b]:
            key = (item.get("url", ""), item.get("text", ""), item.get("source", ""), item.get("platform", ""))
            if key in seen:
                continue
            seen.add(key); uniq.append(item)
        brand_map[b] = uniq
        summary[b]["total_mentions"] = len(uniq)
        summary[b]["comment_mentions"] = sum(1 for x in uniq if str(x.get("source", "")).startswith("comment"))

    summary_df = pd.DataFrame([
        {"brand": b, "posts_count": summary[b]["posts_count"], "title_hits": summary[b]["title_hits"], "comment_mentions": summary[b]["comment_mentions"], "total_mentions": summary[b]["total_mentions"]}
        for b in BRAND_LIST
    ])
    summary_df = summary_df[~((summary_df["posts_count"] == 0) & (summary_df["title_hits"] == 0))].copy()
    if not summary_df.empty:
        summary_df["__pin_columbia"] = summary_df["brand"].apply(lambda x: 0 if x == "컬럼비아" else 1)
        summary_df = summary_df.sort_values(["__pin_columbia", "total_mentions", "posts_count"], ascending=[True, False, False]).drop(columns=["__pin_columbia"])
    return brand_map, summary_df


def _brand_sections_html(brand_map: Dict[str, List[dict]], platform: str) -> str:
    sections = []
    for brand in BRAND_LIST:
        items = [x for x in brand_map.get(brand, []) if x.get("platform") == platform]
        if not items:
            continue
        counts = Counter((it.get("sentiment") or "neutral") for it in items)
        cards = []
        for it in items[:40]:
            title = html.escape((it.get("title") or it.get("url") or "")[:120])
            body_text = html.escape((it.get("text") or "").strip())
            url = html.escape(it.get("url") or "")
            src = html.escape(it.get("source") or "")
            date = html.escape(it.get("date") or "")
            query = html.escape(it.get("query") or "")
            sentiment = (it.get("sentiment") or "neutral").strip()
            score = float(it.get("sentiment_score") or 0)
            senti_class = {"positive": "bg-emerald-50 text-emerald-700 border-emerald-200", "negative": "bg-rose-50 text-rose-700 border-rose-200"}.get(sentiment, "bg-slate-100 text-slate-600 border-slate-200")
            senti_label = {"positive": "긍정", "negative": "부정"}.get(sentiment, "중립")
            query_badge = f'<span class="px-2 py-1 rounded-full bg-slate-100">cafe: {query}</span>' if query else ''
            score_text = f" {score:.2f}" if score else ""
            cards.append(f'''
                <div class="mention-card p-3 rounded-2xl bg-white border border-slate-200 hover:border-blue-300 transition" data-brand="{html.escape(brand)}" data-platform="{html.escape(platform)}">
                  <div class="flex flex-wrap gap-2 text-[11px] text-slate-500 font-bold mb-1">
                    <span class="px-2 py-1 rounded-full bg-slate-100">{src}</span>
                    <span class="px-2 py-1 rounded-full bg-slate-100">{date}</span>
                    {query_badge}
                    <span class="px-2 py-1 rounded-full border {senti_class}">{senti_label}{score_text}</span>
                  </div>
                  <a class="text-sm font-extrabold text-blue-700 hover:underline" href="{url}" target="_blank" rel="noopener noreferrer">{title}</a>
                  <div class="mt-2 text-sm text-slate-700 leading-relaxed whitespace-pre-wrap break-words">{body_text}</div>
                </div>
            ''')
        sections.append(f'''
            <section class="mt-6 brand-section" data-brand="{html.escape(brand)}" data-platform="{html.escape(platform)}">
              <div class="flex items-baseline justify-between gap-3 flex-wrap">
                <h3 class="text-lg font-extrabold text-slate-800">{html.escape(brand)}</h3>
                <div class="flex flex-wrap items-center gap-2 text-xs font-bold tabular-nums">
                  <span class="px-2 py-1 rounded-full bg-slate-100 text-slate-600">{len(items)} mentions</span>
                  <span class="px-2 py-1 rounded-full bg-emerald-50 text-emerald-700">긍정 {counts.get('positive',0)}</span>
                  <span class="px-2 py-1 rounded-full bg-rose-50 text-rose-700">부정 {counts.get('negative',0)}</span>
                  <span class="px-2 py-1 rounded-full bg-slate-100 text-slate-600">중립 {counts.get('neutral',0)}</span>
                </div>
              </div>
              <div class="mt-3 grid grid-cols-1 md:grid-cols-2 gap-3">{''.join(cards)}</div>
            </section>
        ''')
    if not sections:
        return "<div class='mt-6 text-slate-500 font-bold'>브랜드 언급이 없습니다.</div>"
    return "\n".join(sections)


def _brand_filter_controls_html(brand_map: Dict[str, List[dict]], platform: str) -> str:
    active_brands = [b for b in BRAND_LIST if any(x.get("platform") == platform for x in brand_map.get(b, []))]
    if not active_brands:
        return ""
    buttons = [f'<button class="brand-filter-btn active px-3 py-2 rounded-2xl border border-slate-300 bg-white text-sm font-extrabold" data-platform="{html.escape(platform)}" data-brand="all">전체</button>']
    for brand in active_brands:
        buttons.append(f'<button class="brand-filter-btn px-3 py-2 rounded-2xl border border-slate-300 bg-white text-sm font-extrabold" data-platform="{html.escape(platform)}" data-brand="{html.escape(brand)}">{html.escape(brand)}</button>')
    return f'<div class="mt-6 flex flex-wrap gap-2">{"".join(buttons)}</div>'


def _source_panel_html(panel_id: str, title: str, subtitle: str, summary_html: str, weekly_html: str, sections_html: str, filter_html: str = "", warning: str = "") -> str:
    warning_html = ""
    if warning:
        warning_html = f'<div class="mt-4 p-3 rounded-2xl bg-amber-50 border border-amber-200 text-amber-800 text-sm font-bold">{html.escape(warning)}</div>'
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
      {filter_html}
      <div class="mt-6 grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div class="glass rounded-3xl p-5">{summary_html}</div>
        <div class="glass rounded-3xl p-5">{weekly_html}</div>
      </div>
      {sections_html}
    </section>
    '''


def export_portal(dc_posts: List[Post], dc_brand_map: Dict[str, List[dict]], dc_summary_df: pd.DataFrame, naver_posts: List[Post], naver_brand_map: Dict[str, List[dict]], naver_summary_df: pd.DataFrame, naver_warning: str | None = None, out_path: str = "reports/external_signal.html"):
    updated = _now_kst_str()
    dc_meta = summarize_source(dc_posts, dc_brand_map, dc_summary_df, "DCInside")
    naver_meta = summarize_source(naver_posts, naver_brand_map, naver_summary_df, "NAVER Cafe")
    try:
        payload = {
            "updated_at": updated,
            "target_days": int(TARGET_DAYS),
            "dcinside": {"posts_collected": int(len(dc_posts)), "brands_active": int(len(dc_meta["active_brands"])), "total_mentions": int(dc_meta["total_mentions"]), "week_posts": int(dc_meta["week_posts"]), "week_mentions": int(dc_meta["week_mentions"]), "top5": dc_summary_df.head(5).to_dict(orient="records") if not dc_summary_df.empty else []},
            "naver_cafe": {"posts_collected": int(len(naver_posts)), "brands_active": int(len(naver_meta["active_brands"])), "total_mentions": int(naver_meta["total_mentions"]), "week_posts": int(naver_meta["week_posts"]), "week_mentions": int(naver_meta["week_mentions"]), "top5": naver_summary_df.head(5).to_dict(orient="records") if not naver_summary_df.empty else [], "warning": naver_warning or ""},
        }
        _write_summary_json(os.path.dirname(out_path), "external_signal", payload)
    except Exception as e:
        print(f"[WARN] summary.json export failed: {e}")

    dc_panel = _source_panel_html(
        panel_id="panel-dcinside",
        title=f"DCInside · {GALLERY_ID} (최근 {int(TARGET_DAYS)}일)",
        subtitle=f"Updated: {updated} · Posts collected: {len(dc_posts):,} · Active brands: {len(dc_meta['active_brands']):,}",
        summary_html=_summary_table_html(dc_summary_df),
        weekly_html=_weekly_html(dc_meta, "DCInside (게시글 작성일 기준)"),
        sections_html=_brand_sections_html(dc_brand_map, "dcinside"),
        filter_html=_brand_filter_controls_html(dc_brand_map, "dcinside"),
    )

    naver_subtitle = f"Updated: {updated} · Posts collected: {len(naver_posts):,} · Active brands: {len(naver_meta['active_brands']):,}"
    if NAVER_CLIENT_ID and NAVER_CLIENT_SECRET:
        mode_label = str((NAVER_LAST_RUN_META or {}).get("query_mode", "brand only"))
        raw_total = int((NAVER_LAST_RUN_META or {}).get("raw_total", 0))
        kept_total = int((NAVER_LAST_RUN_META or {}).get("kept_total", len(naver_posts)))
        detail_cache_size = int((NAVER_LAST_RUN_META or {}).get("detail_cache_size", 0))
        naver_subtitle += f" · Query mode: {mode_label} · Allowed cafes: {len(_NAVER_ALLOWED_CAFE_IDS)} · Raw: {raw_total:,} → Kept: {kept_total:,} · Detail cache: {detail_cache_size:,}"

    naver_panel = _source_panel_html(
        panel_id="panel-naver",
        title="네이버 카페 · 브랜드 언급 / 필터링 결과",
        subtitle=naver_subtitle,
        summary_html=_summary_table_html(naver_summary_df),
        weekly_html=_weekly_html(naver_meta, "NAVER Cafe (게시글 작성일 기준)"),
        sections_html=_brand_sections_html(naver_brand_map, "naver_cafe"),
        filter_html=_brand_filter_controls_html(naver_brand_map, "naver_cafe"),
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
    html, body {{ height: 100%; overflow: auto; margin:0; }}
    body {{
      background: linear-gradient(180deg, var(--bg0), var(--bg1));
      font-family: 'Plus Jakarta Sans', sans-serif;
      color:#0f172a;
      min-height:100vh;
      margin:0;
    }}
    .glass {{
      background: rgba(255,255,255,.65);
      border: 1px solid rgba(15,23,42,.08);
      box-shadow: 0 10px 30px rgba(2,6,23,.08);
      backdrop-filter: blur(10px);
    }}
    .tab-btn.active, .brand-filter-btn.active {{ background:#0f172a; color:#fff; border-color:#0f172a; }}
    .embedded body {{ background: transparent !important; }}
  </style>
</head>
<body class="w-full">
  <div class="w-full px-0">
    <div class="glass rounded-none p-5 md:p-7 w-full">
      <div class="flex flex-col md:flex-row md:items-end md:justify-between gap-3">
        <div>
          <div class="text-xs text-slate-500 font-extrabold tracking-wide">EXTERNAL SIGNAL HUB</div>
          <h1 class="text-2xl md:text-3xl font-extrabold text-slate-900">DCInside + 네이버 카페 브랜드 언급 모니터링</h1>
          <div class="mt-1 text-xs text-slate-500 font-bold">Updated: {updated} · DCInside: 최근 {int(TARGET_DAYS)}일 / NAVER Cafe: Search API + 게시글 상세 본문/작성일</div>
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
      const brandButtons = Array.from(document.querySelectorAll('.brand-filter-btn'));
      function activate(targetId) {{
        buttons.forEach(btn => btn.classList.toggle('active', btn.dataset.target === targetId));
        panels.forEach(panel => panel.classList.toggle('hidden', panel.id !== targetId));
      }}
      function applyBrandFilter(platform, brand) {{
        const sections = Array.from(document.querySelectorAll(`.brand-section[data-platform="${{platform}}"]`));
        const cards = Array.from(document.querySelectorAll(`.mention-card[data-platform="${{platform}}"]`));
        brandButtons.filter(btn => btn.dataset.platform === platform).forEach(btn => btn.classList.toggle('active', btn.dataset.brand === brand));
        sections.forEach(sec => sec.classList.toggle('hidden', !(brand === 'all' || sec.dataset.brand === brand)));
        cards.forEach(card => card.classList.toggle('hidden', !(brand === 'all' || card.dataset.brand === brand)));
      }}
      buttons.forEach(btn => btn.addEventListener('click', () => activate(btn.dataset.target)));
      brandButtons.forEach(btn => btn.addEventListener('click', () => applyBrandFilter(btn.dataset.platform, btn.dataset.brand)));
      activate('panel-dcinside');
      ['dcinside','naver_cafe'].forEach(platform => applyBrandFilter(platform, 'all'));
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
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(full_html)
    print(f"✅ [성공] External Signal 리포트 생성 완료: {out_path}")
