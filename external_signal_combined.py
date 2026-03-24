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
EOMISAE_PAGES = max(1, int(os.getenv("EOMISAE_PAGES", "2")))
PPOMPPU_PAGES = max(1, int(os.getenv("PPOMPPU_PAGES", "1")))
EOMISAE_BOARD_SPECS = [
    ("fe", "어미새 자유게시판", "https://eomisae.co.kr/fe"),
    ("fh", "어미새 패션게시판", "https://eomisae.co.kr/fh"),
]
PPOMPPU_CLIMB_URL = "https://www.ppomppu.co.kr/zboard/zboard.php?id=climb"
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
    "컬럼비아", "노스페이스", "디스커버리", "내셔널지오그래픽", "코오롱스포츠", "스노우피크",
    "파타고니아", "K2", "블랙야크",
    "네파", "아이더", "밀레", "호카", "아크테릭스", "살로몬",
]

BRAND_ALIASES: Dict[str, List[str]] = {
    "노스페이스": ["TNF", "The North Face", "NORTHFACE", "NORTH FACE"],
    "아크테릭스": ["Arc'teryx", "ARCTERYX", "아크테릭스"],
    "파타고니아": ["Patagonia", "PATAGONIA"],
    "살로몬": ["Salomon", "SALOMON"],
    "스노우피크": ["Snow Peak", "SNOWPEAK", "Snowpeak"],
    "내셔널지오그래픽": ["National Geographic", "NATIONALGEOGRAPHIC", "NatGeo"],
    "코오롱스포츠": ["Kolon Sport", "KOLONSPORT", "Kolonsport"],
    "디스커버리": ["Discovery", "DISCOVERY"],
    "컬럼비아": ["Columbia", "COLUMBIA", "콜롬비아"],
    "블랙야크": ["Black Yak", "BLACKYAK"],
    "네파": ["NEPA"],
    "아이더": ["EIDER"],
    "호카": ["HOKA", "Hoka"],
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


CACHE_RETENTION_DAYS = max(TARGET_DAYS, int(os.getenv("EXTERNAL_SIGNAL_CACHE_RETENTION_DAYS", "30")))
CACHE_DIR = os.path.join("reports", "external_signal_cache")


def _cache_cutoff(days: int) -> datetime:
    return datetime.now(KST) - timedelta(days=max(1, int(days)))


def _cache_path(source: str) -> str:
    return os.path.join(CACHE_DIR, f"{source}.json")


def _post_cache_key(post: Post) -> tuple:
    return (
        (post.platform or "").strip(),
        (post.url or "").strip(),
        re.sub(r"\s+", " ", (post.title or "").strip()),
        post.created_at.strftime("%Y-%m-%d"),
    )


def _post_to_cache_dict(post: Post) -> dict:
    return {
        "title": post.title,
        "url": post.url,
        "content": post.content,
        "comments": post.comments,
        "created_at": post.created_at.astimezone(KST).isoformat(),
        "platform": post.platform,
        "source": post.source,
        "query": post.query,
        "cafe_key": post.cafe_key,
        "cafe_name": post.cafe_name,
    }


def _post_from_cache_dict(item: dict) -> Post | None:
    try:
        raw_dt = str(item.get("created_at") or "").strip()
        if not raw_dt:
            return None
        dt = datetime.fromisoformat(raw_dt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KST)
        else:
            dt = dt.astimezone(KST)
        return Post(
            title=str(item.get("title") or ""),
            url=str(item.get("url") or ""),
            content=str(item.get("content") or ""),
            comments=str(item.get("comments") or ""),
            created_at=dt,
            platform=str(item.get("platform") or ""),
            source=str(item.get("source") or ""),
            query=str(item.get("query") or ""),
            cafe_key=str(item.get("cafe_key") or ""),
            cafe_name=str(item.get("cafe_name") or ""),
        )
    except Exception:
        return None


def _load_cached_source_posts(source: str, days: int) -> List[Post]:
    path = _cache_path(source)
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f) or []
    except Exception:
        return []

    cutoff = _cache_cutoff(max(days, CACHE_RETENTION_DAYS))
    posts: List[Post] = []
    seen = set()
    for item in raw:
        post = _post_from_cache_dict(item)
        if post is None or post.created_at < cutoff:
            continue
        key = _post_cache_key(post)
        if key in seen:
            continue
        seen.add(key)
        posts.append(post)
    posts.sort(key=lambda p: p.created_at, reverse=True)
    return [p for p in posts if p.created_at >= _cache_cutoff(days)]


def _save_cached_source_posts(source: str, posts: List[Post]):
    _safe_mkdir(CACHE_DIR)
    cutoff = _cache_cutoff(CACHE_RETENTION_DAYS)
    deduped: Dict[tuple, Post] = {}
    for post in posts:
        if post.created_at < cutoff:
            continue
        deduped[_post_cache_key(post)] = post
    ordered = sorted(deduped.values(), key=lambda p: p.created_at, reverse=True)
    with open(_cache_path(source), "w", encoding="utf-8") as f:
        json.dump([_post_to_cache_dict(p) for p in ordered], f, ensure_ascii=False, indent=2)


# =================================================================
# 2. 크롤링 엔진
# =================================================================
def crawl_dc_engine(days: int) -> List[Post]:
    start_date = (datetime.now(KST) - timedelta(days=days)).date()
    posts: List[Post] = list(_load_cached_source_posts("dcinside", days))
    seen_urls = {p.url for p in posts if p.url}
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
            if link in seen_urls:
                continue

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
                seen_urls.add(link)
            except Exception as e:
                if DEBUG:
                    print(f"[DEBUG] detail fetch failed: {link} | {e}")
                continue

        print(f"   - {page}페이지 완료 (누적 수집: {len(posts)})")

    _save_cached_source_posts("dcinside", posts)
    return sorted(posts, key=lambda p: p.created_at, reverse=True)


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


def _infer_kst_year(month: int, day: int) -> int:
    now = datetime.now(KST)
    year = now.year
    try:
        candidate = datetime(year, month, day, tzinfo=KST)
        if candidate > now + timedelta(days=2):
            year -= 1
    except Exception:
        return year
    return year


def _parse_source_datetime(text: str) -> datetime | None:
    raw = (text or "").strip()
    if not raw:
        return None

    iso_match = re.search(r"(20\d{2}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:[+-]\d{2}:\d{2})?)", raw)
    if iso_match:
        try:
            dt = datetime.fromisoformat(iso_match.group(1))
            return dt.astimezone(KST) if dt.tzinfo else dt.replace(tzinfo=KST)
        except Exception:
            pass

    normalized = raw.replace("/", "-").replace(".", "-").replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(normalized, fmt).replace(tzinfo=KST)
        except Exception:
            continue

    md_match = re.search(r"(?<!\d)(\d{1,2})[-/.](\d{1,2})(?!\d)", raw)
    if md_match:
        month = int(md_match.group(1))
        day = int(md_match.group(2))
        try:
            return datetime(_infer_kst_year(month, day), month, day, tzinfo=KST)
        except Exception:
            return None
    return None


def _extract_longest_text(soup: BeautifulSoup, selectors: List[str]) -> str:
    candidates: List[str] = []
    for sel in selectors:
        for node in soup.select(sel):
            txt = node.get_text("\n", strip=True)
            if txt:
                candidates.append(txt)
    return max(candidates, key=len) if candidates else ""


def _extract_first_text(soup: BeautifulSoup, selectors: List[str]) -> str:
    for sel in selectors:
        node = soup.select_one(sel)
        if not node:
            continue
        if node.name == "meta":
            txt = (node.get("content") or "").strip()
        else:
            txt = node.get_text(" ", strip=True)
        if txt:
            return txt
    return ""


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


def _naver_post_cache_key(link: str, title: str = "") -> tuple:
    meta = _extract_naver_article_meta(link)
    cafe_id = meta.get("cafe_id") or ""
    article_id = meta.get("article_id") or ""
    if cafe_id and article_id:
        return (cafe_id, article_id)
    return (_canonicalize_link(link).rstrip("/"), re.sub(r"\s+", " ", title or "").strip())


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
    posts: List[Post] = list(_load_cached_source_posts("naver_cafe", days))
    if not NAVER_CLIENT_ID or not NAVER_CLIENT_SECRET:
        msg = "NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 미설정"
        print(f"[WARN] {msg}")
        return sorted(posts, key=lambda p: p.created_at, reverse=True), msg

    headers = {
        "X-Naver-Client-Id": NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
    }

    seen = {_naver_post_cache_key(p.url, p.title) for p in posts}
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
                item_link = _canonicalize_link(item.get("link", ""))
                item_title = _clean_naver_html_text(item.get("title", ""))
                if _naver_post_cache_key(item_link, item_title) in seen:
                    page_drop["dup"] += 1
                    query_drop["dup"] += 1
                    reason_totals["dup"] += 1
                    continue

                keep, reason, dt, title, content, link, cafename, cafeurl = _naver_item_keep(item, brand, query)
                if not keep:
                    page_drop[reason] = page_drop.get(reason, 0) + 1
                    query_drop[reason] = query_drop.get(reason, 0) + 1
                    reason_totals[reason] = reason_totals.get(reason, 0) + 1
                    continue

                key = _naver_post_cache_key(link, title)
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
                        cafe_key=_resolve_naver_cafe_fields(link, cafeurl, cafename, query)[0],
                        cafe_name=_resolve_naver_cafe_fields(link, cafeurl, cafename, query)[1],
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

    if kept_total == 0 and not posts:
        msg = (
            f"NAVER Cafe 결과 0건 (raw={raw_total}, kept={kept_total}) · "
            f"blocked_menu={reason_totals.get('blocked_menu',0)}, cafe_not_allowed={reason_totals.get('cafe_not_allowed',0)}, brand_miss={reason_totals.get('brand_miss',0)}, ambiguous_no_context={reason_totals.get('ambiguous_no_context',0)}, dup={reason_totals.get('dup',0)}"
        )
        _save_cached_source_posts("naver_cafe", posts)
        return posts, msg

    _save_cached_source_posts("naver_cafe", posts)
    return sorted(posts, key=lambda p: p.created_at, reverse=True), None


# =================================================================
# 3. 텍스트 분석 유틸
# =================================================================
def _extract_eomisae_detail(link: str, referer: str) -> tuple[str, datetime | None, str]:
    try:
        resp = _get_with_retry(
            link,
            headers={"Referer": referer, "User-Agent": SESSION.headers.get("User-Agent", "Mozilla/5.0")},
            timeout=20,
        )
        if resp.status_code != 200:
            return "", None, ""
    except Exception:
        return "", None, ""

    soup = BeautifulSoup(resp.text, "html.parser")
    title = _extract_first_text(
        soup,
        ["meta[property='og:title']", "h1", "h2", ".np_18px_span", ".document-title"],
    )
    content = _extract_longest_text(
        soup,
        [".xe_content", ".rhymix_content", ".document-content", ".rd_body", ".board_read .xe_content"],
    )

    dt = None
    meta_time = soup.select_one("meta[property='article:published_time']")
    if meta_time and meta_time.get("content"):
        dt = _parse_source_datetime(meta_time.get("content") or "")
    if dt is None:
        dt = _parse_source_datetime(resp.text)

    return _clean_naver_html_text(content), dt, _clean_naver_html_text(title)


def crawl_eomisae_engine(days: int) -> Tuple[List[Post], str | None]:
    posts: List[Post] = list(_load_cached_source_posts("eomisae", days))
    seen_links = {p.url for p in posts if p.url}
    start_date = (datetime.now(KST) - timedelta(days=max(1, int(days)))).date()
    errors: List[str] = []

    print(f"[M-OS SYSTEM] Eomisae 분석 시작 (boards={len(EOMISAE_BOARD_SPECS)}, pages={EOMISAE_PAGES}, window={days}d)")

    for _, board_name, board_url in EOMISAE_BOARD_SPECS:
        for page in range(1, EOMISAE_PAGES + 1):
            page_url = board_url if page == 1 else f"{board_url}?page={page}"
            try:
                resp = _get_with_retry(page_url, timeout=20)
                if resp.status_code != 200:
                    errors.append(f"{board_name} page {page}: HTTP {resp.status_code}")
                    break
            except Exception as e:
                errors.append(f"{board_name} page {page}: {e}")
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            page_kept = 0
            for row in soup.select("tr"):
                row_classes = row.get("class") or []
                if "notice" in row_classes:
                    continue

                title_cell = row.select_one("td.title")
                if not title_cell:
                    continue

                link_tag = None
                for cand in title_cell.select("a[href]"):
                    href = cand.get("href") or ""
                    if "#C_" in href or "adlink_" in href:
                        continue
                    if href.startswith("/fe/") or href.startswith("/fh/") or re.fullmatch(r"/\d+", href):
                        link_tag = cand
                        break
                if link_tag is None:
                    continue

                link = urljoin(board_url, html.unescape(link_tag.get("href") or ""))
                title = _clean_naver_html_text(link_tag.get_text(" ", strip=True))
                if not link or not title or link in seen_links:
                    continue

                tds = row.select("td")
                list_dt = _parse_source_datetime(tds[3].get_text(" ", strip=True)) if len(tds) >= 4 else None
                content, detail_dt, detail_title = _extract_eomisae_detail(link, board_url)
                stamped_dt = detail_dt or list_dt
                if stamped_dt is None or stamped_dt.date() < start_date:
                    continue

                seen_links.add(link)
                posts.append(
                    Post(
                        title=detail_title or title,
                        url=link,
                        content=content,
                        comments="",
                        created_at=stamped_dt,
                        platform="eomisae",
                        source="eomisae",
                        query=board_name,
                    )
                )
                page_kept += 1

            print(f"   - board='{board_name}' page={page} kept={page_kept} total={len(posts)}")

    warning = None
    if not posts:
        warning = "Eomisae 결과 0건"
        if errors:
            warning = f"{warning} | {'; '.join(errors[:3])}"
    elif errors:
        warning = "; ".join(errors[:3])
    _save_cached_source_posts("eomisae", posts)
    return sorted(posts, key=lambda p: p.created_at, reverse=True), warning


def _extract_ppomppu_detail(link: str) -> tuple[str, datetime | None, str]:
    try:
        resp = _get_with_retry(
            link,
            headers={"Referer": PPOMPPU_CLIMB_URL, "User-Agent": SESSION.headers.get("User-Agent", "Mozilla/5.0")},
            timeout=25,
        )
        if resp.status_code != 200:
            return "", None, ""
    except Exception:
        return "", None, ""

    soup = BeautifulSoup(resp.text, "html.parser")
    title = _extract_first_text(
        soup,
        ["meta[property='og:title']", ".view_title2", ".board_title", "font.view_title2", "title"],
    )
    content = _extract_longest_text(
        soup,
        ["#realArticleContents", ".board-contents", "td.board-contents", "#bbs_contents", ".han"],
    )
    dt = _parse_source_datetime(resp.text)
    return _clean_naver_html_text(content), dt, _clean_naver_html_text(title)


def crawl_ppomppu_engine(days: int) -> Tuple[List[Post], str | None]:
    posts: List[Post] = list(_load_cached_source_posts("ppomppu", days))
    seen_links = {p.url for p in posts if p.url}
    start_date = (datetime.now(KST) - timedelta(days=max(1, int(days)))).date()
    errors: List[str] = []

    print(f"[M-OS SYSTEM] Ppomppu climb 분석 시작 (pages={PPOMPPU_PAGES}, window={days}d)")

    for page in range(1, PPOMPPU_PAGES + 1):
        page_url = PPOMPPU_CLIMB_URL if page == 1 else f"{PPOMPPU_CLIMB_URL}&page={page}"
        try:
            resp = _get_with_retry(
                page_url,
                headers={"Referer": PPOMPPU_CLIMB_URL, "User-Agent": SESSION.headers.get("User-Agent", "Mozilla/5.0")},
                timeout=25,
            )
            if resp.status_code != 200:
                errors.append(f"page {page}: HTTP {resp.status_code}")
                break
        except Exception as e:
            errors.append(f"page {page}: {e}")
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        page_kept = 0
        for link_tag in soup.select("a[href*='view.php?id=climb&no=']"):
            href = html.unescape(link_tag.get("href") or "")
            title = _clean_naver_html_text(link_tag.get_text(" ", strip=True))
            if not href or not title:
                continue

            link = urljoin(page_url, href)
            if link in seen_links:
                continue

            row = link_tag.find_parent("tr")
            row_text = row.get_text(" ", strip=True) if row else title
            if "공지" in row_text[:20]:
                continue

            list_dt = _parse_source_datetime(row_text)
            content, detail_dt, detail_title = _extract_ppomppu_detail(link)
            stamped_dt = detail_dt or list_dt
            if stamped_dt is None or stamped_dt.date() < start_date:
                continue

            seen_links.add(link)
            posts.append(
                Post(
                    title=detail_title or title,
                    url=link,
                    content=content,
                    comments="",
                    created_at=stamped_dt,
                    platform="ppomppu",
                    source="ppomppu",
                    query="뽐뿌 등산포럼",
                )
            )
            page_kept += 1

        print(f"   - page={page} kept={page_kept} total={len(posts)}")

    warning = None
    if not posts:
        warning = "Ppomppu 결과 0건"
        if errors:
            warning = f"{warning} | {'; '.join(errors[:3])}"
    elif errors:
        warning = "; ".join(errors[:3])
    _save_cached_source_posts("ppomppu", posts)
    return sorted(posts, key=lambda p: p.created_at, reverse=True), warning


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
    eomisae_posts: List[Post],
    eomisae_brand_map: Dict[str, List[dict]],
    eomisae_summary_df: pd.DataFrame,
    ppomppu_posts: List[Post],
    ppomppu_brand_map: Dict[str, List[dict]],
    ppomppu_summary_df: pd.DataFrame,
    naver_warning: str | None = None,
    eomisae_warning: str | None = None,
    ppomppu_warning: str | None = None,
    out_path: str = "reports/external_signal.html",
):
    updated = _now_kst_str()

    dc_meta = summarize_source(dc_posts, dc_brand_map, dc_summary_df, "DCInside")
    naver_meta = summarize_source(naver_posts, naver_brand_map, naver_summary_df, "NAVER Cafe")
    eomisae_meta = summarize_source(eomisae_posts, eomisae_brand_map, eomisae_summary_df, "Eomisae")
    ppomppu_meta = summarize_source(ppomppu_posts, ppomppu_brand_map, ppomppu_summary_df, "Ppomppu")

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
            "eomisae": {
                "posts_collected": int(len(eomisae_posts)),
                "brands_active": int(len(eomisae_meta["active_brands"])),
                "total_mentions": int(eomisae_meta["total_mentions"]),
                "week_posts": int(eomisae_meta["week_posts"]),
                "week_mentions": int(eomisae_meta["week_mentions"]),
                "top5": eomisae_summary_df.head(5).to_dict(orient="records") if not eomisae_summary_df.empty else [],
                "warning": eomisae_warning or "",
            },
            "ppomppu": {
                "posts_collected": int(len(ppomppu_posts)),
                "brands_active": int(len(ppomppu_meta["active_brands"])),
                "total_mentions": int(ppomppu_meta["total_mentions"]),
                "week_posts": int(ppomppu_meta["week_posts"]),
                "week_mentions": int(ppomppu_meta["week_mentions"]),
                "top5": ppomppu_summary_df.head(5).to_dict(orient="records") if not ppomppu_summary_df.empty else [],
                "warning": ppomppu_warning or "",
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

    eomisae_panel = _source_panel_html(
        panel_id="panel-eomisae",
        title="어미새 · 자유게시판 + 패션게시판",
        subtitle=f"Updated: {updated} · Posts collected: {len(eomisae_posts):,} · Active brands: {len(eomisae_meta['active_brands']):,}",
        summary_html=_summary_table_html(eomisae_summary_df),
        weekly_html=_weekly_html(eomisae_meta, "Eomisae"),
        sections_html=_brand_sections_html(eomisae_brand_map, "eomisae"),
        warning=eomisae_warning or "",
    )

    ppomppu_panel = _source_panel_html(
        panel_id="panel-ppomppu",
        title="뽐뿌 · 등산포럼",
        subtitle=f"Updated: {updated} · Posts collected: {len(ppomppu_posts):,} · Active brands: {len(ppomppu_meta['active_brands']):,}",
        summary_html=_summary_table_html(ppomppu_summary_df),
        weekly_html=_weekly_html(ppomppu_meta, "Ppomppu climb"),
        sections_html=_brand_sections_html(ppomppu_brand_map, "ppomppu"),
        warning=ppomppu_warning or "",
    )

    full_html = f'''<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>External Signal | DCInside + NAVER Cafe + Eomisae + Ppomppu</title>
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
<body class="p-3 md:p-3">
  <div class="w-full">
    <div class="glass rounded-[32px] p-5 md:p-7 w-full">
      <div class="flex flex-col md:flex-row md:items-end md:justify-between gap-3">
        <div>
          <div class="text-xs text-slate-500 font-extrabold tracking-wide">EXTERNAL SIGNAL HUB</div>
          <h1 class="text-2xl md:text-3xl font-extrabold text-slate-900">DCInside + NAVER Cafe + Eomisae + Ppomppu</h1>
          <div class="mt-1 text-xs text-slate-500 font-bold">Updated: {updated} · 최근 {int(TARGET_DAYS)}일 기준 소스별 브랜드 언급 모니터링</div>
        </div>
        <div class="text-xs text-slate-600 font-bold">브랜드 수: {len(BRAND_LIST)} · 탭별로 소스 분리 확인</div>
      </div>

      <div class="mt-6 flex flex-wrap gap-2">
        <button class="tab-btn active px-4 py-2 rounded-2xl border border-slate-300 bg-white text-sm font-extrabold" data-target="panel-dcinside">DCInside</button>
        <button class="tab-btn px-4 py-2 rounded-2xl border border-slate-300 bg-white text-sm font-extrabold" data-target="panel-naver">네이버 카페</button>
        <button class="tab-btn px-4 py-2 rounded-2xl border border-slate-300 bg-white text-sm font-extrabold" data-target="panel-eomisae">어미새</button>
        <button class="tab-btn px-4 py-2 rounded-2xl border border-slate-300 bg-white text-sm font-extrabold" data-target="panel-ppomppu">뽐뿌</button>
      </div>

      {dc_panel}
      {naver_panel}
      {eomisae_panel}
      {ppomppu_panel}
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


# ================= UX/UI upgrade patch: premium tabs + animated interactions =================
def _ui_compact_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def _ui_excerpt(text: str, limit: int = 220) -> str:
    compact = _ui_compact_text(text)
    if len(compact) <= limit:
        return compact
    return compact[: max(0, limit - 1)].rstrip() + "…"


def _platform_brand_counts(brand_map: Dict[str, List[dict]], platform: str) -> List[tuple[str, int]]:
    counts: List[tuple[str, int]] = []
    for brand in BRAND_LIST:
        count = sum(1 for item in brand_map.get(brand, []) if item.get("platform") == platform)
        if count:
            counts.append((brand, count))
    counts.sort(key=lambda item: (-item[1], item[0]))
    return counts


def _panel_theme(theme_key: str) -> dict:
    themes = {
        "dcinside": {"eyebrow": "TREND BOARD", "tagline": "실시간 반응이 빠른 커뮤니티 흐름"},
        "naver_cafe": {"eyebrow": "TRUSTED COMMUNITY", "tagline": "카페별 맥락을 함께 읽는 탐색형 탭"},
        "eomisae": {"eyebrow": "DEAL & BUZZ", "tagline": "구매 맥락과 체감 반응이 섞이는 탭"},
        "ppomppu": {"eyebrow": "VALUE SIGNAL", "tagline": "가성비 문맥과 반응 온도를 보는 탭"},
    }
    theme = dict(themes.get(theme_key, themes["dcinside"]))
    theme["key"] = theme_key
    return theme


def _panel_lead_brand(summary_df: pd.DataFrame) -> tuple[str, int]:
    if summary_df is None or summary_df.empty:
        return ("데이터 수집 대기", 0)
    row = summary_df.iloc[0]
    return (str(row["brand"]), int(row["total_mentions"]))


def _panel_peak_day(meta: dict) -> tuple[str, int]:
    daily_df = meta.get("daily_df")
    if daily_df is None or daily_df.empty:
        return ("-", 0)
    recent = daily_df.head(7).copy()
    if recent.empty:
        return ("-", 0)
    recent["mentions"] = recent["mentions"].astype(int)
    peak_row = recent.loc[recent["mentions"].idxmax()]
    return (str(peak_row["date"]), int(peak_row["mentions"]))


def _panel_insight_line(summary_df: pd.DataFrame, meta: dict) -> str:
    total_mentions = int(meta.get("total_mentions", 0))
    lead_brand, lead_mentions = _panel_lead_brand(summary_df)
    peak_date, peak_mentions = _panel_peak_day(meta)
    if total_mentions <= 0 or lead_mentions <= 0:
        return "새 데이터가 쌓이면 상위 브랜드와 급등 구간을 이 영역에서 바로 읽을 수 있습니다."
    share = (lead_mentions / total_mentions * 100) if total_mentions else 0
    return (
        f"선두 브랜드는 {lead_brand}이며 전체 언급의 {share:.1f}%를 차지합니다. "
        f"최근 7일 피크는 {peak_date} · {peak_mentions:,} mentions입니다."
    )


def _panel_kpi_html(posts_count: int, meta: dict, summary_df: pd.DataFrame) -> str:
    lead_brand, lead_mentions = _panel_lead_brand(summary_df)
    peak_date, peak_mentions = _panel_peak_day(meta)
    total_mentions = int(meta.get("total_mentions", 0))
    lead_share = (lead_mentions / total_mentions * 100) if total_mentions else 0
    cards = [
        ("수집 게시글", posts_count, f"최근 {int(TARGET_DAYS)}일 기준", "", 0),
        ("활성 브랜드", len(meta.get("active_brands", [])), "실제 언급이 발생한 브랜드", "", 0),
        ("총 언급량", total_mentions, "제목·본문·댓글 포함", "", 0),
        ("선두 점유율", lead_share, f"{lead_brand} · 피크 {peak_date} / {peak_mentions:,}회", "%", 1),
    ]
    html_cards = []
    for idx, (label, value, description, suffix, decimals) in enumerate(cards):
        html_cards.append(
            f'''
            <div class="metric-card rounded-3xl border border-white/70 bg-white/80 p-4 shadow-sm" data-animate style="--index:{idx}">
              <div class="text-[11px] font-extrabold tracking-wide text-slate-500">{html.escape(label)}</div>
              <div class="mt-2 font-['Space_Grotesk'] text-3xl font-bold tracking-[-0.05em] text-slate-900 tabular-nums" data-counter="{float(value):.1f}" data-decimals="{int(decimals)}" data-suffix="{html.escape(suffix)}">0</div>
              <div class="mt-1 text-xs font-bold leading-5 text-slate-500">{html.escape(description)}</div>
            </div>
            '''
        )
    return "".join(html_cards)


def _summary_table_html(summary_df: pd.DataFrame, platform: str) -> str:
    if summary_df is None or summary_df.empty:
        return '<div class="rounded-3xl border border-dashed border-slate-300 bg-white/70 p-8 text-center text-sm font-bold text-slate-500">요약 데이터가 아직 없습니다.</div>'

    total_mentions = int(summary_df["total_mentions"].sum()) if "total_mentions" in summary_df else 0
    top_df = summary_df.head(5)
    rest_df = summary_df.iloc[5:]

    def row_html(rank: int, row) -> str:
        brand = str(row["brand"])
        posts_count = int(row["posts_count"])
        title_hits = int(row["title_hits"])
        comment_mentions = int(row["comment_mentions"])
        mention_total = int(row["total_mentions"])
        share = (mention_total / total_mentions * 100) if total_mentions else 0
        return f'''
        <tr class="border-b border-slate-200/80 last:border-b-0 hover:bg-slate-50/70 transition-colors">
          <td class="py-3 pr-4">
            <button type="button" class="brand-jump group flex items-center gap-3 text-left" data-platform="{html.escape(platform)}" data-brand="{html.escape(brand)}">
              <span class="inline-flex h-8 min-w-8 items-center justify-center rounded-full bg-slate-900 text-xs font-extrabold text-white">#{rank}</span>
              <span class="font-extrabold text-slate-800 group-hover:text-sky-700 transition-colors">{html.escape(brand)}</span>
            </button>
          </td>
          <td class="py-3 pr-4 text-right tabular-nums text-slate-700">{posts_count:,}</td>
          <td class="py-3 pr-4 text-right tabular-nums text-slate-700">{title_hits:,}</td>
          <td class="py-3 pr-4 text-right tabular-nums text-slate-700">{comment_mentions:,}</td>
          <td class="py-3 pr-4 text-right tabular-nums font-extrabold text-slate-900">{mention_total:,}</td>
          <td class="py-3 text-right tabular-nums text-slate-500">{share:.1f}%</td>
        </tr>
        '''

    top_rows = "".join(row_html(rank, row) for rank, (_, row) in enumerate(top_df.iterrows(), start=1))
    rest_rows = "".join(
        row_html(rank, row) for rank, (_, row) in enumerate(rest_df.iterrows(), start=len(top_df) + 1)
    )
    rest_block = ""
    if rest_rows:
        rest_block = (
            '<button type="button" class="ghost-link mt-4 text-xs font-extrabold text-sky-700" onclick="toggleMore(this)">+ 나머지 브랜드 펼치기</button>'
            '<div class="more-box mt-3 hidden overflow-x-auto"><table class="w-full min-w-[720px] text-sm"><tbody>'
            + rest_rows
            + "</tbody></table></div>"
        )

    return f'''
    <div class="flex items-start justify-between gap-3 flex-wrap">
      <div>
        <div class="text-lg font-black tracking-[-0.03em] text-slate-900">브랜드 랭킹</div>
        <div class="mt-1 text-xs font-bold text-slate-500">브랜드명을 누르면 아래 상세 카드로 바로 이동합니다.</div>
      </div>
      <div class="rounded-full bg-slate-100 px-3 py-1 text-xs font-black text-slate-600">최근 {int(TARGET_DAYS)}일</div>
    </div>
    <div class="mt-4 overflow-x-auto">
      <table class="w-full min-w-[720px] text-sm">
        <thead>
          <tr class="border-b border-slate-200 text-slate-500">
            <th class="py-3 text-left">Brand</th>
            <th class="py-3 text-right">Posts</th>
            <th class="py-3 text-right">Title</th>
            <th class="py-3 text-right">Comments</th>
            <th class="py-3 text-right">Mentions</th>
            <th class="py-3 text-right">Share</th>
          </tr>
        </thead>
        <tbody>{top_rows}</tbody>
      </table>
    </div>
    {rest_block}
    '''


def _weekly_html(meta: dict, source_label: str) -> str:
    daily_df = meta.get("daily_df")
    if daily_df is None or daily_df.empty:
        return '<div class="rounded-3xl border border-dashed border-slate-300 bg-white/70 p-8 text-center text-sm font-bold text-slate-500">최근 7일 추이 데이터가 아직 없습니다.</div>'

    recent = daily_df.head(7).copy()
    if recent.empty:
        return '<div class="rounded-3xl border border-dashed border-slate-300 bg-white/70 p-8 text-center text-sm font-bold text-slate-500">최근 7일 추이 데이터가 아직 없습니다.</div>'

    recent["mentions"] = recent["mentions"].astype(int)
    recent["posts"] = recent["posts"].astype(int)
    max_mentions = max(1, int(recent["mentions"].max()))
    avg_mentions = int(round(recent["mentions"].mean())) if not recent.empty else 0
    peak_row = recent.loc[recent["mentions"].idxmax()]

    trend_rows = []
    for idx, (_, row) in enumerate(recent.iterrows()):
        scale = max(0.05, float(row["mentions"]) / max_mentions)
        trend_rows.append(
            f'''
            <div class="trend-row flex items-center gap-3" data-animate style="--index:{idx + 2}">
              <div class="w-24 shrink-0 text-xs font-bold tracking-wide text-slate-500 tabular-nums">{html.escape(str(row["date"]))}</div>
              <div class="flex-1 rounded-full bg-slate-200/80">
                <div class="trend-fill h-2 rounded-full bg-sky-500" style="--scale:{scale:.4f}"></div>
              </div>
              <div class="w-20 shrink-0 text-right text-sm font-black tabular-nums text-slate-900">{int(row["mentions"]):,}</div>
              <div class="w-16 shrink-0 text-right text-xs font-bold tabular-nums text-slate-500">{int(row["posts"]):,}p</div>
            </div>
            '''
        )

    return f'''
    <div class="flex items-start justify-between gap-3 flex-wrap">
      <div>
        <div class="text-lg font-black tracking-[-0.03em] text-slate-900">최근 7일 추이</div>
        <div class="mt-1 text-xs font-bold text-slate-500">{html.escape(source_label)} 기준 일별 mentions 흐름</div>
      </div>
      <div class="rounded-full bg-slate-100 px-3 py-1 text-xs font-black text-slate-600">7D</div>
    </div>
    <div class="mt-4 grid grid-cols-1 sm:grid-cols-3 gap-3">
      <div class="rounded-2xl border border-slate-200 bg-white p-4">
        <div class="text-xs font-black text-slate-500">최근 7일 게시글</div>
        <div class="mt-2 font-['Space_Grotesk'] text-3xl font-bold tracking-[-0.05em] text-slate-900 tabular-nums">{int(meta.get("week_posts", 0)):,}</div>
      </div>
      <div class="rounded-2xl border border-slate-200 bg-white p-4">
        <div class="text-xs font-black text-slate-500">최근 7일 언급량</div>
        <div class="mt-2 font-['Space_Grotesk'] text-3xl font-bold tracking-[-0.05em] text-slate-900 tabular-nums">{int(meta.get("week_mentions", 0)):,}</div>
      </div>
      <div class="rounded-2xl border border-slate-200 bg-white p-4">
        <div class="text-xs font-black text-slate-500">일평균 mentions</div>
        <div class="mt-2 font-['Space_Grotesk'] text-3xl font-bold tracking-[-0.05em] text-slate-900 tabular-nums">{avg_mentions:,}</div>
      </div>
    </div>
    <div class="mt-4 rounded-3xl border border-slate-200 bg-white p-4">
      <div class="flex items-center justify-between gap-3 flex-wrap">
        <div class="text-sm font-black text-slate-700">피크: {html.escape(str(peak_row["date"]))} · {int(peak_row["mentions"]):,} mentions</div>
        <div class="text-xs font-bold text-slate-500">* 최근 7일 최대치 기준 정규화</div>
      </div>
      <div class="mt-4 space-y-3">{''.join(trend_rows)}</div>
    </div>
    '''


def _naver_scope_html() -> str:
    cafes = _naver_allowed_cafe_catalog()
    if not cafes:
        return ""
    chips = []
    for cafe in cafes:
        chips.append(
            f'<a class="scope-chip" href="{html.escape(cafe["url"])}" target="_blank" rel="noopener noreferrer">{html.escape(cafe["label"])}</a>'
        )
    return (
        '<div class="rounded-3xl border border-slate-200 bg-white/80 p-4">'
        '<div class="text-[11px] font-black tracking-[0.18em] text-slate-500">NAVER CAFE SCOPE</div>'
        f'<div class="mt-2 text-sm font-black text-slate-700">허용된 카페 {len(cafes)}곳</div>'
        '<div class="mt-3 flex flex-wrap gap-2 text-xs">'
        + "".join(chips)
        + "</div></div>"
    )


def _naver_cafe_filter_html(brand_map: Dict[str, List[dict]]) -> str:
    cafes = _naver_active_cafe_catalog(brand_map)
    if not cafes:
        return ""
    buttons = [
        '<button type="button" class="filter-chip cafe-filter-btn active" data-platform="naver_cafe" data-cafe="all">전체 카페</button>'
    ]
    for cafe in cafes:
        buttons.append(
            f'<button type="button" class="filter-chip cafe-filter-btn" data-platform="naver_cafe" data-cafe="{html.escape(cafe["key"])}">{html.escape(cafe["label"])}</button>'
        )
    return (
        '<div class="rounded-3xl border border-slate-200 bg-white/80 p-4">'
        '<div class="text-[11px] font-black tracking-[0.18em] text-slate-500">CAFE SEGMENT</div>'
        '<div class="mt-2 text-sm font-black text-slate-700">카페 단위 세그먼트 필터</div>'
        '<div class="mt-3 flex flex-wrap gap-2">'
        + "".join(buttons)
        + "</div></div>"
    )


def _filter_toolbar_html(
    brand_map: Dict[str, List[dict]],
    platform: str,
    search_placeholder: str,
    extra_html: str = "",
) -> str:
    brand_counts = _platform_brand_counts(brand_map, platform)
    brand_buttons = [
        f'<button type="button" class="filter-chip brand-filter-btn active" data-platform="{html.escape(platform)}" data-brand="all">전체 브랜드</button>'
    ]
    for brand, count in brand_counts:
        brand_buttons.append(
            f'<button type="button" class="filter-chip brand-filter-btn" data-platform="{html.escape(platform)}" data-brand="{html.escape(brand)}">{html.escape(brand)} <span>{count}</span></button>'
        )

    extra_block = f'<div class="mt-3 grid grid-cols-1 xl:grid-cols-2 gap-3">{extra_html}</div>' if extra_html else ""
    return f'''
    <div class="mt-6 rounded-[30px] border border-white/70 bg-white/70 p-4 shadow-sm backdrop-blur-xl">
      <div class="grid grid-cols-1 xl:grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)] gap-4">
        <div class="rounded-3xl border border-slate-200 bg-white/80 p-4">
          <div class="text-[11px] font-black tracking-[0.18em] text-slate-500">SEARCH WITHIN TAB</div>
          <div class="mt-2 text-sm font-black text-slate-700">제목, 본문, 출처 텍스트까지 함께 검색합니다.</div>
          <div class="relative mt-3">
            <input type="text" class="panel-search-input h-12 w-full rounded-2xl border border-slate-200 bg-white px-4 pr-24 text-sm font-bold text-slate-900 outline-none" data-platform="{html.escape(platform)}" placeholder="{html.escape(search_placeholder)}" aria-label="{html.escape(search_placeholder)}" />
            <button type="button" class="search-clear-btn absolute right-2 top-2 rounded-xl bg-slate-200 px-3 py-2 text-xs font-black text-slate-700" data-platform="{html.escape(platform)}">지우기</button>
          </div>
          <div class="mt-3 text-xs font-black text-slate-500" data-role="result-count" data-platform="{html.escape(platform)}">전체 결과를 표시 중입니다.</div>
        </div>
        <div class="rounded-3xl border border-slate-200 bg-white/80 p-4">
          <div class="text-[11px] font-black tracking-[0.18em] text-slate-500">BRAND FOCUS</div>
          <div class="mt-2 text-sm font-black text-slate-700">브랜드 버튼으로 상세 카드 범위를 빠르게 좁힙니다.</div>
          <div class="mt-3 flex flex-wrap gap-2">{''.join(brand_buttons)}</div>
        </div>
      </div>
      {extra_block}
    </div>
    '''


def _brand_sections_html(brand_map: Dict[str, List[dict]], platform: str) -> str:
    sections = []
    for section_idx, (brand, _) in enumerate(_platform_brand_counts(brand_map, platform)):
        items = [x for x in brand_map.get(brand, []) if x.get("platform") == platform]
        if not items:
            continue
        cards = []
        for card_idx, it in enumerate(items[:40]):
            raw_title = (it.get("title") or it.get("url") or "")[:120]
            raw_text = _ui_compact_text(it.get("text") or "")
            text = html.escape(raw_text or "본문 미리보기를 준비 중입니다.")
            url = html.escape(it.get("url") or "")
            src = html.escape(it.get("source") or "")
            date = html.escape(it.get("date") or "")
            query = (it.get("query") or "").strip()
            cafe_key_raw = (it.get("cafe_key") or "").strip() or "unknown"
            cafe_name_raw = (it.get("cafe_name") or "").strip()
            cafe_badge = cafe_name_raw or query or cafe_key_raw
            query_badge = (
                f'<span class="meta-pill bg-sky-50 text-sky-700">query: {html.escape(query)}</span>'
                if query and query != cafe_badge
                else ""
            )
            search_blob = html.escape(
                _ui_excerpt(
                    " ".join(
                        [
                            brand,
                            raw_title,
                            raw_text,
                            it.get("source") or "",
                            cafe_badge,
                            query,
                        ]
                    ).lower(),
                    600,
                )
            )
            toggle = ""
            if len(raw_text) > 220:
                toggle = '<button type="button" class="ghost-link mt-3 text-xs font-extrabold text-sky-700" onclick="toggleCard(this)">본문 더보기</button>'
            cards.append(
                f'''
                <article
                  class="mention-card rounded-3xl border border-slate-200 bg-white p-4 shadow-sm transition hover:-translate-y-1 hover:shadow-lg"
                  data-platform="{html.escape(platform)}"
                  data-brand="{html.escape(brand)}"
                  data-cafe="{html.escape(cafe_key_raw)}"
                  data-search="{search_blob}"
                  data-animate
                  style="--index:{card_idx % 12 + 2}"
                >
                  <div class="flex flex-wrap gap-2 text-[11px] font-black text-slate-500">
                    <span class="meta-pill rounded-full bg-slate-100 px-3 py-1">{src}</span>
                    <span class="meta-pill rounded-full bg-slate-100 px-3 py-1">{date}</span>
                    <span class="meta-pill rounded-full bg-slate-100 px-3 py-1">context: {html.escape(cafe_badge)}</span>
                    {query_badge}
                  </div>
                  <div class="mt-4 flex items-start justify-between gap-4">
                    <div class="min-w-0">
                      <a class="mention-title block text-base font-black leading-6 tracking-[-0.02em] text-slate-900 transition hover:text-sky-700" href="{url}" target="_blank" rel="noopener noreferrer">{html.escape(raw_title)}</a>
                      <div class="mention-copy snippet-collapsed mt-3 whitespace-pre-wrap break-words text-sm font-semibold leading-7 text-slate-700">{text}</div>
                      {toggle}
                    </div>
                    <a class="mention-cta shrink-0 rounded-full bg-sky-50 px-3 py-2 text-xs font-black text-sky-700" href="{url}" target="_blank" rel="noopener noreferrer">원문 보기</a>
                  </div>
                </article>
                '''
            )
        sections.append(
            f'''
            <section class="brand-section mt-7" data-platform="{html.escape(platform)}" data-brand="{html.escape(brand)}" data-animate style="--index:{section_idx}">
              <div class="rounded-[30px] border border-white/70 bg-white/75 p-5 shadow-sm backdrop-blur-xl">
                <div class="flex items-start justify-between gap-4 flex-wrap">
                  <div>
                    <div class="text-[11px] font-black tracking-[0.18em] text-slate-500">BRAND DETAIL</div>
                    <h3 class="mt-2 text-2xl font-black tracking-[-0.04em] text-slate-900">{html.escape(brand)}</h3>
                  </div>
                  <div class="section-stats rounded-full bg-slate-100 px-4 py-2 text-xs font-black text-slate-600" data-role="mention-count">{len(items):,}건 노출</div>
                </div>
                <div class="mt-4 grid grid-cols-1 xl:grid-cols-2 gap-4">{''.join(cards)}</div>
              </div>
            </section>
            '''
        )
    if not sections:
        return "<div class='mt-6 rounded-3xl border border-dashed border-slate-300 bg-white/70 p-8 text-center text-sm font-bold text-slate-500'>브랜드 언급이 아직 없습니다.</div>"
    return "\n".join(sections) + f'\n<div class="filter-empty mt-6 hidden rounded-3xl border border-dashed border-slate-300 bg-white/70 p-8 text-center text-sm font-bold text-slate-500" data-platform="{html.escape(platform)}">현재 필터 조건에 맞는 결과가 없습니다.</div>'


def _source_panel_html(
    panel_id: str,
    platform: str,
    title: str,
    subtitle: str,
    insight: str,
    kpi_html: str,
    summary_html: str,
    weekly_html: str,
    sections_html: str,
    warning: str = "",
    toolbar_html: str = "",
) -> str:
    theme = _panel_theme(platform)
    warning_html = ""
    if warning:
        warning_html = f'<div class="mt-5 rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm font-black text-amber-800 shadow-sm">{html.escape(warning)}</div>'
    return f'''
    <section id="{panel_id}" class="tab-panel hidden mt-6" data-platform="{html.escape(platform)}" data-theme="{html.escape(theme["key"])}">
      <div class="panel-hero overflow-hidden rounded-[34px] border border-white/70 bg-white/60 p-6 shadow-sm backdrop-blur-xl">
        <div class="hero-orb hero-orb-primary"></div>
        <div class="hero-orb hero-orb-secondary"></div>
        <div class="relative z-10 flex flex-wrap items-start justify-between gap-4">
          <div class="max-w-4xl">
            <div class="inline-flex rounded-full bg-white/70 px-4 py-2 text-[11px] font-black tracking-[0.18em] text-slate-600">{html.escape(theme["eyebrow"])}</div>
            <h2 class="mt-4 text-3xl font-black tracking-[-0.05em] text-slate-900 md:text-5xl">{html.escape(title)}</h2>
            <div class="mt-3 text-xs font-black leading-6 text-slate-500 md:text-sm">{html.escape(subtitle)}</div>
            <p class="mt-4 max-w-4xl text-sm font-bold leading-7 text-slate-700 md:text-base">{html.escape(insight)}</p>
          </div>
          <div class="rounded-2xl bg-white/70 px-4 py-3 text-xs font-black leading-5 text-slate-600 shadow-sm">{html.escape(theme["tagline"])}</div>
        </div>
        <div class="relative z-10 mt-6 grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-3">{kpi_html}</div>
      </div>
      {warning_html}
      {toolbar_html}
      <div class="mt-6 grid grid-cols-1 xl:grid-cols-[1.05fr_.95fr] gap-4">
        <div class="rounded-[30px] border border-white/70 bg-white/75 p-5 shadow-sm backdrop-blur-xl">{summary_html}</div>
        <div class="rounded-[30px] border border-white/70 bg-white/75 p-5 shadow-sm backdrop-blur-xl">{weekly_html}</div>
      </div>
      {sections_html}
    </section>
    '''


def export_portal(
    dc_posts: List[Post],
    dc_brand_map: Dict[str, List[dict]],
    dc_summary_df: pd.DataFrame,
    naver_posts: List[Post],
    naver_brand_map: Dict[str, List[dict]],
    naver_summary_df: pd.DataFrame,
    eomisae_posts: List[Post],
    eomisae_brand_map: Dict[str, List[dict]],
    eomisae_summary_df: pd.DataFrame,
    ppomppu_posts: List[Post],
    ppomppu_brand_map: Dict[str, List[dict]],
    ppomppu_summary_df: pd.DataFrame,
    naver_warning: str | None = None,
    eomisae_warning: str | None = None,
    ppomppu_warning: str | None = None,
    out_path: str = "reports/external_signal.html",
):
    updated = _now_kst_str()

    dc_meta = summarize_source(dc_posts, dc_brand_map, dc_summary_df, "DCInside")
    naver_meta = summarize_source(naver_posts, naver_brand_map, naver_summary_df, "NAVER Cafe")
    eomisae_meta = summarize_source(eomisae_posts, eomisae_brand_map, eomisae_summary_df, "Eomisae")
    ppomppu_meta = summarize_source(ppomppu_posts, ppomppu_brand_map, ppomppu_summary_df, "Ppomppu")

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
            "eomisae": {
                "posts_collected": int(len(eomisae_posts)),
                "brands_active": int(len(eomisae_meta["active_brands"])),
                "total_mentions": int(eomisae_meta["total_mentions"]),
                "week_posts": int(eomisae_meta["week_posts"]),
                "week_mentions": int(eomisae_meta["week_mentions"]),
                "top5": eomisae_summary_df.head(5).to_dict(orient="records") if not eomisae_summary_df.empty else [],
                "warning": eomisae_warning or "",
            },
            "ppomppu": {
                "posts_collected": int(len(ppomppu_posts)),
                "brands_active": int(len(ppomppu_meta["active_brands"])),
                "total_mentions": int(ppomppu_meta["total_mentions"]),
                "week_posts": int(ppomppu_meta["week_posts"]),
                "week_mentions": int(ppomppu_meta["week_mentions"]),
                "top5": ppomppu_summary_df.head(5).to_dict(orient="records") if not ppomppu_summary_df.empty else [],
                "warning": ppomppu_warning or "",
            },
        }
        _write_summary_json(os.path.dirname(out_path), "external_signal", payload)
    except Exception as e:
        print(f"[WARN] summary.json export failed: {e}")

    naver_subtitle = f"Updated: {updated} · Posts {len(naver_posts):,} · Active brands {len(naver_meta['active_brands']):,}"
    if NAVER_CLIENT_ID and NAVER_CLIENT_SECRET:
        mode_label = str((NAVER_LAST_RUN_META or {}).get("query_mode", "brand only"))
        raw_total = int((NAVER_LAST_RUN_META or {}).get("raw_total", 0))
        kept_total = int((NAVER_LAST_RUN_META or {}).get("kept_total", len(naver_posts)))
        detail_cache_size = int((NAVER_LAST_RUN_META or {}).get("detail_cache_size", 0))
        naver_subtitle += (
            f" · Query {mode_label} · Allowed cafes {len(_NAVER_ALLOWED_CAFE_IDS)}"
            f" · Raw {raw_total:,} → Kept {kept_total:,} · Cache {detail_cache_size:,}"
        )

    naver_reason_totals = (NAVER_LAST_RUN_META or {}).get("reason_totals", {}) or {}
    if naver_reason_totals:
        reason_line = (
            f"drop preview={int(naver_reason_totals.get('brand_miss_preview', 0)):,}, "
            f"brand={int(naver_reason_totals.get('brand_miss', 0)):,}, "
            f"context={int(naver_reason_totals.get('ambiguous_no_context', 0)):,}, "
            f"old={int(naver_reason_totals.get('out_of_range', 0)):,}, "
            f"dup={int(naver_reason_totals.get('dup', 0)):,}"
        )
        naver_warning = f"{naver_warning} | {reason_line}" if naver_warning else reason_line

    dc_panel = _source_panel_html(
        panel_id="panel-dcinside",
        platform="dcinside",
        title=f"DCInside · {GALLERY_ID} 갤러리",
        subtitle=f"Updated: {updated} · Posts {len(dc_posts):,} · Active brands {len(dc_meta['active_brands']):,}",
        insight=_panel_insight_line(dc_summary_df, dc_meta),
        kpi_html=_panel_kpi_html(len(dc_posts), dc_meta, dc_summary_df),
        summary_html=_summary_table_html(dc_summary_df, "dcinside"),
        weekly_html=_weekly_html(dc_meta, "DCInside"),
        sections_html=_brand_sections_html(dc_brand_map, "dcinside"),
        toolbar_html=_filter_toolbar_html(dc_brand_map, "dcinside", "DCInside 안에서 브랜드·키워드를 검색해보세요"),
    )
    naver_panel = _source_panel_html(
        panel_id="panel-naver",
        platform="naver_cafe",
        title="NAVER Cafe · 브랜드 언급 탐색",
        subtitle=naver_subtitle,
        insight=_panel_insight_line(naver_summary_df, naver_meta),
        kpi_html=_panel_kpi_html(len(naver_posts), naver_meta, naver_summary_df),
        summary_html=_summary_table_html(naver_summary_df, "naver_cafe"),
        weekly_html=_weekly_html(naver_meta, "NAVER Cafe"),
        sections_html=_brand_sections_html(naver_brand_map, "naver_cafe"),
        warning=naver_warning or "",
        toolbar_html=_filter_toolbar_html(
            naver_brand_map,
            "naver_cafe",
            "NAVER Cafe 안에서 브랜드·카페·문맥 키워드를 검색해보세요",
            _naver_scope_html() + _naver_cafe_filter_html(naver_brand_map),
        ),
    )
    eomisae_panel = _source_panel_html(
        panel_id="panel-eomisae",
        platform="eomisae",
        title="Eomisae · 자유게시판 + 패션게시판",
        subtitle=f"Updated: {updated} · Posts {len(eomisae_posts):,} · Active brands {len(eomisae_meta['active_brands']):,}",
        insight=_panel_insight_line(eomisae_summary_df, eomisae_meta),
        kpi_html=_panel_kpi_html(len(eomisae_posts), eomisae_meta, eomisae_summary_df),
        summary_html=_summary_table_html(eomisae_summary_df, "eomisae"),
        weekly_html=_weekly_html(eomisae_meta, "Eomisae"),
        sections_html=_brand_sections_html(eomisae_brand_map, "eomisae"),
        warning=eomisae_warning or "",
        toolbar_html=_filter_toolbar_html(eomisae_brand_map, "eomisae", "어미새 안에서 브랜드·상황 키워드를 검색해보세요"),
    )
    ppomppu_panel = _source_panel_html(
        panel_id="panel-ppomppu",
        platform="ppomppu",
        title="Ppomppu · 등산포럼",
        subtitle=f"Updated: {updated} · Posts {len(ppomppu_posts):,} · Active brands {len(ppomppu_meta['active_brands']):,}",
        insight=_panel_insight_line(ppomppu_summary_df, ppomppu_meta),
        kpi_html=_panel_kpi_html(len(ppomppu_posts), ppomppu_meta, ppomppu_summary_df),
        summary_html=_summary_table_html(ppomppu_summary_df, "ppomppu"),
        weekly_html=_weekly_html(ppomppu_meta, "Ppomppu climb"),
        sections_html=_brand_sections_html(ppomppu_brand_map, "ppomppu"),
        warning=ppomppu_warning or "",
        toolbar_html=_filter_toolbar_html(ppomppu_brand_map, "ppomppu", "뽐뿌 안에서 브랜드·가성비 키워드를 검색해보세요"),
    )

    source_cards = [
        ("panel-dcinside", "DCInside", len(dc_posts), int(dc_meta["total_mentions"]), len(dc_meta["active_brands"]), "dcinside"),
        ("panel-naver", "NAVER Cafe", len(naver_posts), int(naver_meta["total_mentions"]), len(naver_meta["active_brands"]), "naver_cafe"),
        ("panel-eomisae", "Eomisae", len(eomisae_posts), int(eomisae_meta["total_mentions"]), len(eomisae_meta["active_brands"]), "eomisae"),
        ("panel-ppomppu", "Ppomppu", len(ppomppu_posts), int(ppomppu_meta["total_mentions"]), len(ppomppu_meta["active_brands"]), "ppomppu"),
    ]
    all_active_brands = set()
    for meta in (dc_meta, naver_meta, eomisae_meta, ppomppu_meta):
        all_active_brands.update(meta.get("active_brands", []))
    total_posts = len(dc_posts) + len(naver_posts) + len(eomisae_posts) + len(ppomppu_posts)
    total_mentions = int(dc_meta["total_mentions"]) + int(naver_meta["total_mentions"]) + int(eomisae_meta["total_mentions"]) + int(ppomppu_meta["total_mentions"])
    lead_cards = []
    for target, label, posts_count, mentions_count, active_brands_count, theme_key in source_cards:
        lead_cards.append(
            f'''
            <button type="button" class="source-switch rounded-[26px] border border-white/70 bg-white/85 p-4 text-left shadow-sm transition hover:-translate-y-1 hover:shadow-lg" data-target="{target}" data-theme="{theme_key}">
              <div class="flex items-center justify-between gap-3">
                <div class="text-sm font-black text-slate-900">{html.escape(label)}</div>
                <div class="rounded-full bg-slate-100 px-3 py-1 text-[11px] font-black text-slate-600">{active_brands_count} brands</div>
              </div>
              <div class="mt-4 font-['Space_Grotesk'] text-3xl font-bold tracking-[-0.05em] text-slate-900 tabular-nums">{mentions_count:,}</div>
              <div class="mt-1 text-xs font-black text-slate-500">Posts {posts_count:,}</div>
            </button>
            '''
        )

    full_html = f'''<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>External Signal | Premium Signal Hub</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    @import url('https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css');
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;700&display=swap');
    body {{ font-family:'Pretendard','Noto Sans KR',sans-serif; background:
      radial-gradient(circle at 12% 18%, rgba(59,130,246,.20), transparent 28%),
      radial-gradient(circle at 84% 14%, rgba(34,197,94,.18), transparent 24%),
      radial-gradient(circle at 72% 74%, rgba(249,115,22,.16), transparent 24%),
      linear-gradient(180deg,#f8fbff 0%,#eef4fb 52%,#e8eff8 100%); background-size:120% 120%; animation:ambientShift 18s ease-in-out infinite alternate; }}
    body.embedded {{ background:transparent !important; }}
    .tab-panel[data-theme="dcinside"] {{ --accent:37 99 235; }}
    .tab-panel[data-theme="naver_cafe"] {{ --accent:22 163 74; }}
    .tab-panel[data-theme="eomisae"] {{ --accent:217 119 6; }}
    .tab-panel[data-theme="ppomppu"] {{ --accent:225 29 72; }}
    .panel-hero {{ position:relative; background:linear-gradient(135deg, rgba(var(--accent),.16), rgba(255,255,255,.96) 38%, rgba(var(--accent),.08)); }}
    .hero-orb {{ position:absolute; border-radius:999px; filter:blur(8px); pointer-events:none; mix-blend-mode:screen; }}
    .hero-orb-primary {{ width:220px; height:220px; right:-42px; top:-42px; background:radial-gradient(circle, rgba(var(--accent),.28), transparent 70%); animation:floatBlob 12s ease-in-out infinite; }}
    .hero-orb-secondary {{ width:180px; height:180px; left:-32px; bottom:-46px; background:radial-gradient(circle, rgba(var(--accent),.18), transparent 70%); animation:floatBlobReverse 14s ease-in-out infinite; }}
    .tab-panel {{ opacity:0; transform:translateY(18px); pointer-events:none; }}
    .tab-panel.is-active {{ opacity:1; transform:translateY(0); pointer-events:auto; animation:panelIn .65s cubic-bezier(.2,.7,.2,1) both; }}
    .tab-btn.active, .source-switch.active {{ color:#fff; background:linear-gradient(135deg, rgba(var(--theme),1), rgba(var(--theme),.82)); box-shadow:0 18px 34px rgba(var(--theme),.24); }}
    .tab-btn[data-theme="dcinside"], .source-switch[data-theme="dcinside"] {{ --theme:37,99,235; }}
    .tab-btn[data-theme="naver_cafe"], .source-switch[data-theme="naver_cafe"] {{ --theme:22,163,74; }}
    .tab-btn[data-theme="eomisae"], .source-switch[data-theme="eomisae"] {{ --theme:217,119,6; }}
    .tab-btn[data-theme="ppomppu"], .source-switch[data-theme="ppomppu"] {{ --theme:225,29,72; }}
    .source-switch.active .text-slate-900, .source-switch.active .text-slate-500, .source-switch.active .text-slate-600 {{ color:#fff !important; }}
    .source-switch.active .bg-slate-100 {{ background:rgba(255,255,255,.16) !important; }}
    .filter-chip.active {{ color:#fff; background:linear-gradient(135deg, rgba(var(--accent),1), rgba(var(--accent),.84)); box-shadow:0 16px 28px rgba(var(--accent),.22); }}
    .panel-search-input:focus {{ border-color:rgba(var(--accent),.45); box-shadow:0 0 0 4px rgba(var(--accent),.10); }}
    .trend-fill {{ transform-origin:left center; transform:scaleX(0); }}
    .tab-panel.is-active .trend-fill {{ animation:growBar .9s cubic-bezier(.2,.7,.2,1) forwards; animation-delay:calc(var(--index,0) * 55ms); }}
    [data-animate] {{ opacity:0; transform:translateY(18px) scale(.985); }}
    .tab-panel.is-active [data-animate] {{ animation:revealUp .68s cubic-bezier(.2,.7,.2,1) forwards; animation-delay:calc(var(--index,0) * 55ms); }}
    .ghost-link {{ background:none; border:none; cursor:pointer; }}
    @keyframes ambientShift {{ 0% {{ background-position:0% 0%; }} 100% {{ background-position:100% 100%; }} }}
    @keyframes panelIn {{ 0% {{ opacity:0; transform:translateY(20px) scale(.99); }} 100% {{ opacity:1; transform:translateY(0) scale(1); }} }}
    @keyframes revealUp {{ 0% {{ opacity:0; transform:translateY(18px) scale(.985); }} 100% {{ opacity:1; transform:translateY(0) scale(1); }} }}
    @keyframes growBar {{ from {{ transform:scaleX(0); }} to {{ transform:scaleX(var(--scale)); }} }}
    @keyframes floatBlob {{ 0%,100% {{ transform:translate3d(0,0,0) scale(1); }} 50% {{ transform:translate3d(-18px,18px,0) scale(1.05); }} }}
    @keyframes floatBlobReverse {{ 0%,100% {{ transform:translate3d(0,0,0) scale(1); }} 50% {{ transform:translate3d(18px,-18px,0) scale(1.08); }} }}
    @media (max-width: 900px) {{ .trend-row {{ display:grid; grid-template-columns:76px minmax(0,1fr) 64px 42px; align-items:center; }} }}
    @media (prefers-reduced-motion: reduce) {{ *,*::before,*::after {{ animation:none !important; transition:none !important; }} .tab-panel {{ opacity:1; transform:none; pointer-events:auto; }} [data-animate] {{ opacity:1; transform:none; }} .trend-fill {{ transform:scaleX(var(--scale)); }} }}
  </style>
</head>
<body class="min-h-screen text-slate-900">
  <div class="mx-auto my-3 w-[min(1600px,calc(100vw-24px))] rounded-[36px] border border-white/70 bg-white/65 p-4 shadow-[0_24px_70px_rgba(15,23,42,0.10)] backdrop-blur-2xl md:p-6">
    <section class="overflow-hidden rounded-[32px] bg-slate-950 px-6 py-6 text-white shadow-xl md:px-8">
      <div class="relative z-10">
        <div class="inline-flex rounded-full bg-white/10 px-4 py-2 text-[11px] font-black tracking-[0.18em]">EXTERNAL SIGNAL PREMIUM HUB</div>
        <h1 class="mt-5 text-4xl font-black tracking-[-0.05em] md:text-6xl">Community Signal Dashboard</h1>
        <p class="mt-4 max-w-4xl text-sm font-semibold leading-7 text-white/80 md:text-base">
          브랜드 언급량을 소스별로 빠르게 비교하고, 탭 전환마다 분위기와 정보 밀도가 달라지도록 UX를 재구성했습니다.
          상단에서 전체 흐름을 보고, 각 탭에서는 검색·브랜드 포커스·카페 세그먼트까지 바로 탐색할 수 있습니다.
        </p>
        <div class="mt-6 grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-4">
          <div class="rounded-3xl border border-white/10 bg-white/10 p-4 backdrop-blur"><div class="text-xs font-black text-white/60">총 수집 게시글</div><div class="mt-2 font-['Space_Grotesk'] text-3xl font-bold tracking-[-0.05em] tabular-nums">{total_posts:,}</div><div class="mt-1 text-xs font-bold text-white/60">4개 커뮤니티 합산</div></div>
          <div class="rounded-3xl border border-white/10 bg-white/10 p-4 backdrop-blur"><div class="text-xs font-black text-white/60">총 mentions</div><div class="mt-2 font-['Space_Grotesk'] text-3xl font-bold tracking-[-0.05em] tabular-nums">{total_mentions:,}</div><div class="mt-1 text-xs font-bold text-white/60">제목·본문·댓글 포함</div></div>
          <div class="rounded-3xl border border-white/10 bg-white/10 p-4 backdrop-blur"><div class="text-xs font-black text-white/60">활성 브랜드</div><div class="mt-2 font-['Space_Grotesk'] text-3xl font-bold tracking-[-0.05em] tabular-nums">{len(all_active_brands):,}</div><div class="mt-1 text-xs font-bold text-white/60">실제 언급 발생 브랜드</div></div>
          <div class="rounded-3xl border border-white/10 bg-white/10 p-4 backdrop-blur"><div class="text-xs font-black text-white/60">Updated</div><div class="mt-2 text-xl font-black">{html.escape(updated)}</div><div class="mt-1 text-xs font-bold text-white/60">최근 {int(TARGET_DAYS)}일 기준</div></div>
        </div>
        <div class="mt-6 grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-4">{''.join(lead_cards)}</div>
      </div>
    </section>

    <div class="sticky top-3 z-30 mt-5 flex flex-wrap gap-2 rounded-[24px] border border-white/70 bg-white/80 p-3 shadow-sm backdrop-blur-xl">
      <button type="button" class="tab-btn active rounded-2xl bg-slate-100 px-4 py-3 text-sm font-black text-slate-700 transition hover:-translate-y-0.5" data-target="panel-dcinside" data-theme="dcinside">DCInside</button>
      <button type="button" class="tab-btn rounded-2xl bg-slate-100 px-4 py-3 text-sm font-black text-slate-700 transition hover:-translate-y-0.5" data-target="panel-naver" data-theme="naver_cafe">NAVER Cafe</button>
      <button type="button" class="tab-btn rounded-2xl bg-slate-100 px-4 py-3 text-sm font-black text-slate-700 transition hover:-translate-y-0.5" data-target="panel-eomisae" data-theme="eomisae">Eomisae</button>
      <button type="button" class="tab-btn rounded-2xl bg-slate-100 px-4 py-3 text-sm font-black text-slate-700 transition hover:-translate-y-0.5" data-target="panel-ppomppu" data-theme="ppomppu">Ppomppu</button>
    </div>

    {dc_panel}
    {naver_panel}
    {eomisae_panel}
    {ppomppu_panel}
  </div>

  <script>
    function toggleMore(btn) {{
      const box = btn.parentElement.querySelector('.more-box');
      if (!box) return;
      box.classList.toggle('hidden');
      btn.textContent = box.classList.contains('hidden') ? '+ 나머지 브랜드 펼치기' : '- 브랜드 목록 접기';
    }}

    function toggleCard(btn) {{
      const copy = btn.parentElement.querySelector('.mention-copy');
      if (!copy) return;
      copy.classList.toggle('snippet-collapsed');
      btn.textContent = copy.classList.contains('snippet-collapsed') ? '본문 더보기' : '본문 접기';
    }}

    (function () {{
      const tabButtons = Array.from(document.querySelectorAll('.tab-btn'));
      const sourceSwitches = Array.from(document.querySelectorAll('.source-switch'));
      const panels = Array.from(document.querySelectorAll('.tab-panel'));
      const brandButtons = Array.from(document.querySelectorAll('.brand-filter-btn'));
      const cafeButtons = Array.from(document.querySelectorAll('.cafe-filter-btn'));
      const searchInputs = Array.from(document.querySelectorAll('.panel-search-input'));
      const clearButtons = Array.from(document.querySelectorAll('.search-clear-btn'));
      const panelState = Object.fromEntries(panels.map((panel) => [panel.dataset.platform, {{ brand: 'all', cafe: 'all', query: '' }}]));

      function escapeSelector(value) {{
        if (window.CSS && typeof window.CSS.escape === 'function') return window.CSS.escape(value);
        return String(value).replace(/"/g, '\\\\"');
      }}

      function formatCount(value) {{
        return Number(value || 0).toLocaleString('ko-KR');
      }}

      function setActiveButtons(buttons, predicate) {{
        buttons.forEach((button) => button.classList.toggle('active', predicate(button)));
      }}

      function animateCounter(el) {{
        const target = parseFloat(el.dataset.counter || '0');
        if (!Number.isFinite(target)) return;
        const decimals = parseInt(el.dataset.decimals || (Number.isInteger(target) ? '0' : '1'), 10);
        const suffix = el.dataset.suffix || '';
        const formatter = new Intl.NumberFormat('ko-KR', {{ minimumFractionDigits: decimals, maximumFractionDigits: decimals }});
        if (el._raf) cancelAnimationFrame(el._raf);
        const start = performance.now();
        const duration = 850;
        const step = (now) => {{
          const progress = Math.min(1, (now - start) / duration);
          const eased = 1 - Math.pow(1 - progress, 3);
          el.textContent = formatter.format(target * eased) + suffix;
          if (progress < 1) {{
            el._raf = requestAnimationFrame(step);
          }} else {{
            el.textContent = formatter.format(target) + suffix;
          }}
        }};
        el.textContent = formatter.format(0) + suffix;
        el._raf = requestAnimationFrame(step);
      }}

      function updateResultCount(panel, count) {{
        const node = panel.querySelector(`[data-role="result-count"][data-platform="${{panel.dataset.platform}}"]`);
        if (!node) return;
        node.textContent = count > 0 ? `현재 조건에서 ${{formatCount(count)}}건을 표시 중입니다.` : '현재 조건에 맞는 결과가 없습니다.';
      }}

      function applyFilters(platform) {{
        const panel = document.querySelector(`.tab-panel[data-platform="${{platform}}"]`);
        if (!panel) return;
        const state = panelState[platform] || {{ brand: 'all', cafe: 'all', query: '' }};
        const query = (state.query || '').trim().toLowerCase();
        const sections = Array.from(panel.querySelectorAll('.brand-section'));
        const emptyNode = panel.querySelector('.filter-empty');
        let totalVisibleCards = 0;
        let visibleSections = 0;

        sections.forEach((section) => {{
          const cards = Array.from(section.querySelectorAll('.mention-card'));
          let visibleCount = 0;
          cards.forEach((card) => {{
            const brandOk = state.brand === 'all' || card.dataset.brand === state.brand;
            const cafeOk = state.cafe === 'all' || card.dataset.cafe === state.cafe;
            const text = (card.dataset.search || '').toLowerCase();
            const queryOk = !query || text.includes(query);
            const show = brandOk && cafeOk && queryOk;
            card.classList.toggle('hidden', !show);
            if (show) {{
              visibleCount += 1;
              totalVisibleCards += 1;
            }}
          }});
          const showSection = (state.brand === 'all' || section.dataset.brand === state.brand) && visibleCount > 0;
          section.classList.toggle('hidden', !showSection);
          if (showSection) visibleSections += 1;
          const countNode = section.querySelector('[data-role="mention-count"]');
          if (countNode) countNode.textContent = `${{formatCount(visibleCount)}}건 노출`;
        }});

        if (emptyNode) emptyNode.classList.toggle('hidden', visibleSections > 0);
        setActiveButtons(brandButtons.filter((btn) => btn.dataset.platform === platform), (button) => button.dataset.brand === state.brand);
        setActiveButtons(cafeButtons.filter((btn) => btn.dataset.platform === platform), (button) => button.dataset.cafe === state.cafe);
        updateResultCount(panel, totalVisibleCards);
      }}

      function activate(targetId) {{
        tabButtons.forEach((button) => button.classList.toggle('active', button.dataset.target === targetId));
        sourceSwitches.forEach((button) => button.classList.toggle('active', button.dataset.target === targetId));
        panels.forEach((panel) => {{
          const isTarget = panel.id === targetId;
          panel.classList.toggle('hidden', !isTarget);
          panel.classList.toggle('is-active', isTarget);
          if (isTarget) {{
            panel.querySelectorAll('[data-counter]').forEach(animateCounter);
            applyFilters(panel.dataset.platform);
          }}
        }});
      }}

      tabButtons.forEach((button) => button.addEventListener('click', () => activate(button.dataset.target)));
      sourceSwitches.forEach((button) => button.addEventListener('click', () => activate(button.dataset.target)));
      brandButtons.forEach((button) => button.addEventListener('click', () => {{ panelState[button.dataset.platform].brand = button.dataset.brand; applyFilters(button.dataset.platform); }}));
      cafeButtons.forEach((button) => button.addEventListener('click', () => {{ panelState[button.dataset.platform].cafe = button.dataset.cafe; applyFilters(button.dataset.platform); }}));
      searchInputs.forEach((input) => input.addEventListener('input', () => {{ panelState[input.dataset.platform].query = input.value || ''; applyFilters(input.dataset.platform); }}));
      clearButtons.forEach((button) => button.addEventListener('click', () => {{
        const platform = button.dataset.platform;
        const input = document.querySelector(`.panel-search-input[data-platform="${{platform}}"]`);
        if (input) input.value = '';
        panelState[platform].query = '';
        applyFilters(platform);
      }}));
      document.querySelectorAll('.brand-jump').forEach((button) => button.addEventListener('click', () => {{
        const platform = button.dataset.platform;
        const brand = button.dataset.brand;
        const panel = document.querySelector(`.tab-panel[data-platform="${{platform}}"]`);
        if (!panel) return;
        panelState[platform].brand = brand;
        activate(panel.id);
        applyFilters(platform);
        const target = panel.querySelector(`.brand-section[data-brand="${{escapeSelector(brand)}}"]`);
        if (target) target.scrollIntoView({{ behavior: 'smooth', block: 'start' }});
      }}));

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

    eomisae_posts, eomisae_warning = crawl_eomisae_engine(days=TARGET_DAYS)
    eomisae_brand_map, eomisae_summary_df = process_data(eomisae_posts) if eomisae_posts else ({b: [] for b in BRAND_LIST}, pd.DataFrame())

    ppomppu_posts, ppomppu_warning = crawl_ppomppu_engine(days=TARGET_DAYS)
    ppomppu_brand_map, ppomppu_summary_df = process_data(ppomppu_posts) if ppomppu_posts else ({b: [] for b in BRAND_LIST}, pd.DataFrame())

    export_portal(
        dc_posts=dc_posts,
        dc_brand_map=dc_brand_map,
        dc_summary_df=dc_summary_df,
        naver_posts=naver_posts,
        naver_brand_map=naver_brand_map,
        naver_summary_df=naver_summary_df,
        eomisae_posts=eomisae_posts,
        eomisae_brand_map=eomisae_brand_map,
        eomisae_summary_df=eomisae_summary_df,
        ppomppu_posts=ppomppu_posts,
        ppomppu_brand_map=ppomppu_brand_map,
        ppomppu_summary_df=ppomppu_summary_df,
        naver_warning=naver_warning,
        eomisae_warning=eomisae_warning,
        ppomppu_warning=ppomppu_warning,
    )

    if not dc_posts:
        print("⚠️ DCInside 수집 데이터 0건")
    if not naver_posts:
        print("⚠️ NAVER Cafe 수집 데이터 0건")

    if not eomisae_posts:
        print("[WARN] Eomisae 수집 데이터 0건")
    if not ppomppu_posts:
        print("[WARN] Ppomppu 수집 데이터 0건")

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
