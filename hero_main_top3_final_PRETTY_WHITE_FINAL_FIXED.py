#!/usr/bin/env python3
# -*- coding: utf-8 -*-

print("HERO SCRIPT FINAL RUNNING", flush=True)

"""
hero_main_top15_FINAL_STABLE_PATCHED_P2PLUS.py

✅ P0~P2 + 추가 개선(운영/정확도/안정성) 반영

[핵심 변경]
1) summary.json 집계 버그 수정 + 다운로드 실패/캐시/차단 분리
2) IMG_URL_CACHE 캐시 키 정규화(normalize_img_url)로 중복 다운로드 방지
3) brand_ok 기준 강화(배너 1개 이상 수집 성공이어야 OK)
4) hero_slider 정확도 개선:
   - "컨테이너 우선" (상단 큰 컨테이너 1~2개 선정 → 내부에서 slide 추출)
   - 부족 시 기존 broad slide 스캔 → 그래도 부족 시 generic fallback
5) 캠페인 날짜 추출 안정화/비용 절감:
   - 기본: RANK 1만 date fetch
   - href_clean 기반 캐시 파일(cache/campaign_dates.json)로 재방문 최소화
6) P2: 운영/디버깅 강화
   - 브랜드별 수집/실패/차단/다운로드실패 등 stats를 summary.json에 저장
   - 실패 브랜드/원인/예외 stack을 reports/snapshots/errors_YYYY-MM-DD.log에 기록
   - 전회 대비 변경 감지(diff) 생성: reports/hero_changes_YYYY-MM-DD.json
   - HTML 상단에 "오늘 변경된 배너" 카드 + 변경 리스트 렌더
7) 브랜드 링크 가끔 안됨 대응:
   - goto 실패 시 프로토콜/슬래시 보정 + ALT_URLS 순회 fallback
   - 최종 성공 URL을 기록(brand_resolved_url)

환경변수:
- OUT_DIR, HEADLESS, NAV_TIMEOUT_MS, WAIT_AFTER_GOTO_MS, WAIT_AFTER_CLICK_MS
- MAX_IMG_WIDTH, JPG_QUALITY
- FETCH_CAMPAIGN_DATES (default 1), DATE_FETCH_TIMEOUT_MS
- DATE_FETCH_RANK1_ONLY (default 1)  # ✅ rank=1만 날짜추출
- CAMPAIGN_DATE_CACHE_TTL_DAYS (default 14)  # ✅ 캐시 TTL
- HTML_USE_ABSOLUTE_FILE_URL
"""

import os, re, csv, hashlib, urllib.parse, sys, time, json, traceback, html, smtplib
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple, Dict, Any
from email.message import EmailMessage
import requests
from pathlib import Path
from contextlib import contextmanager

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError, Error as PWError

_KST = timezone(timedelta(hours=9))

# ================================================================
# Summary/meta export (Hub first-screen consumption)
# ================================================================
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

# Pillow (image resize + meta)
try:
    from PIL import Image, ImageOps
    from io import BytesIO
    PIL_OK = True
except Exception:
    PIL_OK = False

try:
    import pytesseract
    OCR_OK = True
except Exception:
    OCR_OK = False


# =====================================================
# Progress (console)
# =====================================================
class Progress:
    def __init__(self, total: int):
        self.total = max(int(total), 1)
        self.done = 0
        self.ok = 0
        self.fail = 0
        self.start_ts = time.time()
        self.curr_brand = ""
        self.curr_stage = ""
        self.img_saved = 0
        self.img_failed = 0

    def _fmt_eta(self):
        elapsed = time.time() - self.start_ts
        if self.done <= 0:
            return "ETA --:--"
        rate = elapsed / self.done
        remain = rate * (self.total - self.done)
        mm = int(remain // 60)
        ss = int(remain % 60)
        return f"ETA {mm:02d}:{ss:02d}"

    def _render(self):
        elapsed = time.time() - self.start_ts
        mm = int(elapsed // 60)
        ss = int(elapsed % 60)
        pct = int((self.done / self.total) * 100)

        bar_w = 28
        fill = int(bar_w * self.done / self.total)
        bar = "█" * fill + "░" * (bar_w - fill)

        msg = (
            f"[{bar}] {pct:3d}%  "
            f"{self.done}/{self.total}  "
            f"OK:{self.ok} FAIL:{self.fail}  "
            f"IMG:{self.img_saved} (fail:{self.img_failed})  "
            f"{self._fmt_eta()}  "
            f"({mm:02d}:{ss:02d})  "
            f"{self.curr_brand} {self.curr_stage}"
        )
        sys.stdout.write("\r" + msg[:220].ljust(220))
        sys.stdout.flush()

    def set_stage(self, brand: str = "", stage: str = ""):
        if brand:
            self.curr_brand = brand
        if stage:
            self.curr_stage = stage
        self._render()

    def step_done(self, ok: bool):
        self.done += 1
        if ok:
            self.ok += 1
        else:
            self.fail += 1
        self.curr_stage = "done"
        self._render()

    def add_img(self, ok: bool):
        if ok:
            self.img_saved += 1
        else:
            self.img_failed += 1
        self._render()

    def newline(self):
        sys.stdout.write("\n")
        sys.stdout.flush()


PROG: Optional[Progress] = None


@contextmanager
def stage(brand_name: str, stage_name: str):
    global PROG
    if PROG:
        PROG.set_stage(brand=brand_name, stage=stage_name)
    t0 = time.time()
    try:
        yield
    finally:
        dt = time.time() - t0
        if PROG:
            PROG.set_stage(brand=brand_name, stage=f"{stage_name} ({dt:.1f}s)")


# =====================================================
# ENV / CONFIG
# =====================================================
OUT_DIR = os.environ.get("OUT_DIR", "reports")
ASSET_DIR = os.path.join(OUT_DIR, "assets")
SNAP_DIR = os.path.join(OUT_DIR, "snapshots")
CACHE_DIR = os.path.join(OUT_DIR, "cache")

HEADLESS = os.environ.get("HEADLESS", "1") != "0"
NAV_TIMEOUT_MS = int(os.environ.get("NAV_TIMEOUT_MS", "60000"))
WAIT_AFTER_CLICK_MS = int(os.environ.get("WAIT_AFTER_CLICK_MS", "900"))
WAIT_AFTER_GOTO_MS = int(os.environ.get("WAIT_AFTER_GOTO_MS", "1800"))

# image resize
MAX_IMG_WIDTH = int(os.environ.get("MAX_IMG_WIDTH", "1100"))  # px
JPG_QUALITY = int(os.environ.get("JPG_QUALITY", "85"))

# campaign date fetch
FETCH_CAMPAIGN_DATES = os.environ.get("FETCH_CAMPAIGN_DATES", "1") != "0"
DATE_FETCH_TIMEOUT_MS = int(os.environ.get("DATE_FETCH_TIMEOUT_MS", "12000"))
DATE_FETCH_RANK1_ONLY = os.environ.get("DATE_FETCH_RANK1_ONLY", "1") != "0"
CAMPAIGN_DATE_CACHE_TTL_DAYS = int(os.environ.get("CAMPAIGN_DATE_CACHE_TTL_DAYS", "14"))
FETCH_CAMPAIGN_META = os.environ.get("FETCH_CAMPAIGN_META", "1") != "0"
CAMPAIGN_META_RANK_LIMIT = max(1, int(os.environ.get("CAMPAIGN_META_RANK_LIMIT", "1")))
CAMPAIGN_META_CACHE_TTL_DAYS = max(1, int(os.environ.get("CAMPAIGN_META_CACHE_TTL_DAYS", "7")))
ENABLE_OCR = os.environ.get("ENABLE_OCR", "0") == "1"

USER_AGENT = os.environ.get(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
)

# HTML local image path mode
if "HTML_USE_ABSOLUTE_FILE_URL" in os.environ:
    HTML_USE_ABSOLUTE_FILE_URL = os.environ.get("HTML_USE_ABSOLUTE_FILE_URL", "0") != "0"
else:
    HTML_USE_ABSOLUTE_FILE_URL = (os.environ.get("GITHUB_ACTIONS", "").lower() not in {"true", "1", "yes"})

DEFAULT_MAX_ITEMS = max(1, int(os.environ.get("MAX_BANNERS_PER_BRAND", "999")))
RECENT_CHANGE_DAYS = max(1, int(os.environ.get("RECENT_CHANGE_DAYS", "7")))
SECTION_SCAN_Y_MAX = max(1800, int(os.environ.get("SECTION_SCAN_Y_MAX", "6000")))
DEEP_SCAN_NODE_LIMIT = max(400, int(os.environ.get("DEEP_SCAN_NODE_LIMIT", "2200")))



# =====================================================
# EXCLUDE (style codes)
# =====================================================
EXCLUDE_STYLE_CODES = {
    "C71YLT371297",
    "C71YLT371193",
}

# =====================================================
# Brands (15)
# =====================================================

BRANDS = [
    ("tnf", "The North Face", "https://www.thenorthfacekorea.co.kr/", "tnf_slick", DEFAULT_MAX_ITEMS),
    ("patagonia", "Patagonia", "https://www.patagonia.co.kr/", "patagonia_static_hero", DEFAULT_MAX_ITEMS),
    ("arcteryx", "Arc'teryx", "https://www.arcteryx.co.kr/", "hero_sections", DEFAULT_MAX_ITEMS),
    ("salomon", "Salomon", "https://salomon.co.kr/", "hero_sections", DEFAULT_MAX_ITEMS),
    ("snowpeak", "Snow Peak", "https://www.snowpeakstore.co.kr/", "hero_sections", DEFAULT_MAX_ITEMS),

    ("blackyak", "Black Yak", "https://www.byn.kr/blackyak", "blackyak_swiper", DEFAULT_MAX_ITEMS),
    ("discovery", "Discovery Expedition", "https://www.discovery-expedition.com/", "discovery_swiper", DEFAULT_MAX_ITEMS),
    ("nepa", "NEPA", "https://www.nplus.co.kr/main/main.asp", "nepa_static", DEFAULT_MAX_ITEMS),

    ("natgeo", "National Geographic", "https://www.natgeokorea.com/", "hero_sections", DEFAULT_MAX_ITEMS),

    ("kolonsport", "Kolon Sport", "https://www.kolonsport.com/", "hero_sections", DEFAULT_MAX_ITEMS),

    ("k2", "K2", "https://www.k-village.co.kr/K2", "hero_sections", DEFAULT_MAX_ITEMS),
    ("montbell", "Montbell", "https://www.montbell.co.kr/", "hero_sections", DEFAULT_MAX_ITEMS),
    ("eider", "Eider", "https://www.eider.co.kr/", "hero_sections", DEFAULT_MAX_ITEMS),

    ("millet", "Millet", "https://www.millet.co.kr/", "hero_sections", DEFAULT_MAX_ITEMS),
]

BRAND_SEGMENTS: Dict[str, Dict[str, str]] = {
    "tnf": {"style": "라이프스타일", "origin": "글로벌"},
    "patagonia": {"style": "라이프스타일", "origin": "글로벌"},
    "arcteryx": {"style": "전문 산행", "origin": "글로벌"},
    "salomon": {"style": "전문 산행", "origin": "글로벌"},
    "snowpeak": {"style": "라이프스타일", "origin": "글로벌"},
    "blackyak": {"style": "전문 산행", "origin": "로컬"},
    "discovery": {"style": "라이프스타일", "origin": "로컬"},
    "nepa": {"style": "전문 산행", "origin": "로컬"},
    "natgeo": {"style": "라이프스타일", "origin": "글로벌"},
    "kolonsport": {"style": "전문 산행", "origin": "로컬"},
    "k2": {"style": "전문 산행", "origin": "로컬"},
    "montbell": {"style": "전문 산행", "origin": "글로벌"},
    "eider": {"style": "전문 산행", "origin": "글로벌"},
    "millet": {"style": "전문 산행", "origin": "글로벌"},
}

KEYWORD_STOPWORDS = {
    "the", "and", "for", "with", "from", "your", "into", "main", "visual", "section",
    "campaign", "special", "more", "best", "new", "spring", "summer", "fall", "winter",
    "brand", "official", "look", "edition", "outdoor", "image", "hero", "mainvisual",
    "기획전", "메인", "배너", "컬렉션", "이벤트", "브랜드", "공식", "더", "및", "신상",
    "출시", "추천", "위크", "시즌", "라인", "캠페인", "section",
}

DASHBOARD_KEYWORD_EXCLUDES = {
    "COLLECTION",
    "MILLET",
    "ITEM",
    "NATIONAL",
    "GEOGRAPHIC",
    "ARRIVALS",
    "온라인스토어",
    "아웃도어",
    "아크테릭스코리아",
    "모디세이",
    "에어로",
    "살로몬",
    "스노우피크",
    "어패럴",
    "루트",
    "지구",
    "경이로운",
    "행성",
    "담은",
    "화이트라벨",
    "루벤",
    "박보검",
    "몬테라",
}

VISUAL_TAG_RULES = [
    ("우천", [r"rain", r"storm", r"waterproof", r"우천", r"장마", r"방수"]),
    ("하이킹", [r"hike", r"hiking", r"trail", r"trek", r"mountain", r"등산", r"산행"]),
    ("러닝", [r"run", r"running", r"trailrun", r"러닝"]),
    ("캠핑", [r"camp", r"camping", r"캠핑"]),
    ("라이프스타일", [r"lifestyle", r"casual", r"city", r"urban", r"daily", r"일상"]),
    ("여성", [r"women", r"woman", r"women's", r"여성"]),
    ("남성", [r"men", r"man's", r"mens", r"남성"]),
    ("키즈", [r"kids", r"kid", r"junior", r"school", r"키즈", r"주니어"]),
    ("백팩", [r"backpack", r"bag", r"pack", r"배낭", r"백팩"]),
    ("풋웨어", [r"shoe", r"shoes", r"footwear", r"sneaker", r"샌들", r"슈즈"]),
    ("재킷", [r"jacket", r"shell", r"wind", r"다운", r"자켓", r"재킷"]),
    ("봄시즌", [r"spring", r"봄"]),
    ("여름시즌", [r"summer", r"여름"]),
]

ALERT_EVENT_THRESHOLD = max(1, int(os.environ.get("ALERT_EVENT_THRESHOLD", "2")))
ALERT_CHANGED_THRESHOLD = max(1, int(os.environ.get("ALERT_CHANGED_THRESHOLD", "2")))
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "").strip()
ALERT_EMAIL_TO = os.environ.get("ALERT_EMAIL_TO", "").strip()
ALERT_EMAIL_FROM = os.environ.get("ALERT_EMAIL_FROM", "").strip()
ALERT_SMTP_HOST = os.environ.get("ALERT_SMTP_HOST", "").strip()
ALERT_SMTP_PORT = int(os.environ.get("ALERT_SMTP_PORT", "587"))
ALERT_SMTP_USER = os.environ.get("ALERT_SMTP_USER", "").strip()
ALERT_SMTP_PASSWORD = os.environ.get("ALERT_SMTP_PASSWORD", "").strip()

# ✅ URL이 간혹 안되는 브랜드 대비(필요 시 너가 계속 추가/수정)

ALT_URLS: Dict[str, List[str]] = {
    "tnf": [
        "https://www.thenorthfacekorea.co.kr/",
        "https://www.thenorthfacekorea.co.kr/main",
        "https://www.thenorthfacekorea.co.kr/main/",
    ],
    "arcteryx": [
        "https://www.arcteryx.co.kr/",
        "https://www.arcteryx.co.kr/main",
        "https://www.arcteryx.co.kr/main/",
    ],
    "k2": [
        "https://www.k-village.co.kr/K2",
        "https://www.k-village.co.kr/k2",
        "https://www.k-village.co.kr/K2/",
        "https://www.k-village.co.kr/k2/",
        "https://www.k-village.co.kr/k2/is",
    ],
    "natgeo": [
        "https://www.natgeokorea.com/",
        "https://www.natgeokorea.com/main",
    ],
}

BRAND_NAME_MAP: Dict[str, str] = {bk: bn for bk, bn, *_ in BRANDS}
BRAND_ORDER = [bk for bk, *_ in BRANDS]
DEEP_SCAN_BRANDS = {"tnf", "arcteryx", "k2", "salomon", "snowpeak", "natgeo", "kolonsport", "montbell", "eider", "millet"}


# =====================================================
# Data model
# =====================================================
@dataclass
class Banner:
    date: str
    brand_key: str
    brand_name: str
    rank: int
    title: str
    href: str
    img_url: str
    img_local: str

    href_clean: str = ""
    plan_start: str = ""
    plan_end: str = ""
    img_w: int = 0
    img_h: int = 0
    img_bytes: int = 0
    img_status: str = ""   # ok / cached / download_fail / blocked / blocked_html / http_403 / http_429 / no_url / unknown
    img_signature: str = ""
    brand_resolved_url: str = ""

    extract_source: str = ""
    confidence_score: float = 0.0
    is_fallback: bool = False
    is_primary_hero: bool = False

    landing_title: str = ""
    landing_subtitle: str = ""
    landing_summary: str = ""
    landing_keywords: str = ""
    landing_category: str = ""
    landing_text_status: str = ""

    # 운영/디버깅
    brand_resolved_url: str = ""


# =====================================================
# Global caches / meta
# =====================================================
IMG_URL_CACHE: Dict[str, str] = {}       # normalize_img_url(img_url) -> local filename
IMG_META: Dict[str, Tuple[int, int, int]] = {}  # local filename -> (w,h,bytes)
CAMPAIGN_CACHE: Dict[str, Any] = {}
CAMPAIGN_META_CACHE: Dict[str, Any] = {}


# =====================================================
# Util
# =====================================================
def kst_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=9)

def sha1(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()[:10]

def norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def safe_filename(name: str, ext: str) -> str:
    name = re.sub(r"[^0-9a-zA-Z가-힣._\-]+", "_", (name or "")).strip("_")
    if not ext.startswith("."):
        ext = "." + ext
    return (name[:110] or "file") + ext

def abs_url(base: str, url: str) -> str:
    if not url:
        return ""
    url = url.strip()
    if url.startswith("//"):
        return "https:" + url
    return urllib.parse.urljoin(base, url)

def guess_ext(url: str) -> str:
    u = (url or "").lower()
    for ext in [".jpg", ".jpeg", ".png", ".webp"]:
        if ext in u:
            return ext
    return ".jpg"

def get_bg_image(style: str) -> str:
    if not style:
        return ""
    m = re.search(r"background-image:\s*url\(['\"]?([^'\"\)]+)", style)
    return m.group(1) if m else ""

def pick_from_srcset(srcset: str) -> str:
    if not srcset:
        return ""
    first = srcset.split(",")[0].strip()
    return first.split(" ")[0].strip()

def _extract_url_from_css(css: str) -> str:
    try:
        if not css or "url(" not in css:
            return ""
        s = css.split("url(", 1)[1]
        s = s.split(")", 1)[0].strip()
        if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
            s = s[1:-1]
        return s.strip()
    except Exception:
        return ""

def normalize_img_url(u: str) -> str:
    """중복 제거/캐시 키: img_url의 쿼리/프래그먼트 제거 + 공백 트림"""
    u = (u or "").strip()
    if not u:
        return ""
    try:
        sp = urllib.parse.urlsplit(u)
        sp2 = sp._replace(query="", fragment="")
        return urllib.parse.urlunsplit(sp2)
    except Exception:
        return u

def is_style_code(s: str) -> bool:
    s = (s or "").strip()
    if not s:
        return False
    if s in EXCLUDE_STYLE_CODES:
        return True
    for code in EXCLUDE_STYLE_CODES:
        if code in s:
            return True
    return False

def is_junk_title(t: str) -> bool:
    tl = (t or "").strip().lower()
    if not tl:
        return True
    if is_style_code(t):
        return True

    junk_tokens = [
        "phpthumb", "src=/uploads", "w=1200", "q=80", "f=webp",
        ".jpg", ".jpeg", ".png", ".webp", "data:image",
        "main_mc", "kakaotalk_", "img_", "banner_", "thumb",
        "no image",
    ]
    if any(tok in tl for tok in junk_tokens):
        return True
    if re.fullmatch(r"[a-f0-9_\-]{18,}", tl):
        return True
    return False

def clean_campaign_title(t: str) -> str:
    t = norm_ws(t)
    for code in EXCLUDE_STYLE_CODES:
        t = t.replace(code, "").strip()
    t = t.strip('"').strip("'").strip()
    t = re.sub(r'^\s*["\']?|["\']?\s*$', '', t)
    # UI 텍스트 제거(추가 개선)
    bad_ui = ["로그인", "회원가입", "고객센터", "장바구니", "검색", "브랜드스토리", "매장", "스토어", "전체보기"]
    if any(x in t for x in bad_ui) and len(t) < 25:
        return "메인 배너"
    return t[:90] if t else "메인 배너"

def choose_title(*cands: str) -> str:
    c = [norm_ws(x) for x in cands if norm_ws(x)]
    c = [x for x in c if not is_style_code(x)]
    c = [x for x in c if x.lower() not in {"next", "prev", "이전", "다음", "닫기"} and len(x) > 1]
    c2 = [x for x in c if not is_junk_title(x)]
    if c2:
        c2.sort(key=lambda x: (len(x), x), reverse=True)
        return clean_campaign_title(c2[0])
    if c:
        c.sort(key=lambda x: (len(x), x), reverse=True)
        return clean_campaign_title(c[0])
    return "메인 배너"

def normalize_href(href: str) -> str:
    """dedupe용: utm/fbclid/NaPm 등 제거한 canonical href"""
    href = (href or "").strip()
    if not href:
        return ""
    try:
        sp = urllib.parse.urlsplit(href)
        qs = urllib.parse.parse_qsl(sp.query, keep_blank_values=True)
        drop_prefixes = ("utm_",)
        drop_keys = {"fbclid", "gclid", "wbraid", "gbraid", "NaPm", "nacn", "sms_click", "igshid"}
        kept = []
        for k, v in qs:
            kl = (k or "")
            if any(kl.startswith(p) for p in drop_prefixes):
                continue
            if kl in drop_keys:
                continue
            kept.append((k, v))
        new_q = urllib.parse.urlencode(kept, doseq=True)
        sp2 = sp._replace(query=new_q, fragment="")
        return urllib.parse.urlunsplit(sp2)
    except Exception:
        return href

def to_file_url(path: str) -> str:
    try:
        p = Path(path).resolve()
        return p.as_uri()
    except Exception:
        ap = os.path.abspath(path).replace("\\", "/")
        if not ap.startswith("/"):
            return "file:///" + ap
        return "file://" + ap


# =====================================================
# Image download (requests + playwright fallback) + diagnostics
# =====================================================
def _headers_img(referer: str = "") -> Dict[str, str]:
    h = {
        "User-Agent": USER_AGENT,
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    if referer:
        h["Referer"] = referer
        try:
            sp = urllib.parse.urlsplit(referer)
            h["Origin"] = f"{sp.scheme}://{sp.netloc}"
        except Exception:
            pass
    return h

def download_bytes_requests(url: str, referer: str = "") -> Tuple[Optional[bytes], str]:
    """
    returns (content, status)
    status: ok / http_403 / http_429 / http_{code} / blocked_html / empty / exception
    """
    try:
        r = requests.get(url, headers=_headers_img(referer), timeout=25, allow_redirects=True)
        code = int(getattr(r, "status_code", 0) or 0)
        ct = (r.headers.get("content-type") or "").lower()
        if code in (401, 403):
            return None, "http_403"
        if code in (429,):
            return None, "http_429"
        if code != 200:
            return None, f"http_{code}"
        if not r.content:
            return None, "empty"
        # 차단 페이지/봇체크가 html로 내려오는 케이스
        if "text/html" in ct or r.content[:200].lstrip().startswith(b"<!doctype html") or b"<html" in r.content[:400].lower():
            return None, "blocked_html"
        return r.content, "ok"
    except Exception:
        return None, "exception"

def download_bytes_pw(context, url: str, referer: str = "") -> Tuple[Optional[bytes], str]:
    """403/봇/쿠키 필요한 이미지에 대한 fallback"""
    try:
        resp = context.request.get(url, headers=_headers_img(referer), timeout=25000)
        if not resp:
            return None, "pw_noresp"
        if not resp.ok:
            # playwright는 status 접근 가능
            try:
                sc = int(resp.status)
                if sc in (401, 403):
                    return None, "http_403"
                if sc in (429,):
                    return None, "http_429"
                return None, f"http_{sc}"
            except Exception:
                return None, "pw_notok"
        b = resp.body()
        if not b:
            return None, "empty"
        # html 차단
        if b[:200].lstrip().startswith(b"<!doctype html") or b"<html" in b[:400].lower():
            return None, "blocked_html"
        return b, "ok"
    except Exception:
        return None, "pw_exception"

def _record_img_meta(local_path: str, fname: str):
    try:
        size_b = os.path.getsize(local_path)
    except Exception:
        size_b = 0

    w = h = 0
    if PIL_OK:
        try:
            im = Image.open(local_path)
            w, h = im.size
        except Exception:
            w = h = 0
    IMG_META[fname] = (int(w or 0), int(h or 0), int(size_b or 0))


def _image_signature(img_local: str, img_url: str = "") -> str:
    if img_local:
        path = os.path.join(ASSET_DIR, img_local)
        try:
            with open(path, "rb") as f:
                head = f.read(65536)
            return hashlib.sha1(head).hexdigest()[:16]
        except Exception:
            pass
    norm_img = normalize_img_url(img_url or "")
    return sha1(norm_img) if norm_img else ""


def build_banner(date_s: str, brand_key: str, brand_name: str, rank: int, title: str,
                 href: str, img_url: str, img_local: str, raw_img_status: str,
                 extract_source: str = "", confidence_score: float = 0.0,
                 is_fallback: bool = False, is_primary_hero: bool = False) -> Banner:
    banner = Banner(date_s, brand_key, brand_name, rank, title, href, img_url, img_local)
    banner.href_clean = normalize_href(href)
    banner.img_status = raw_img_status or ("ok" if img_local else "")
    banner.img_signature = _image_signature(img_local, img_url)
    banner.extract_source = extract_source or ""
    banner.confidence_score = round(float(confidence_score or 0.0), 2)
    banner.is_fallback = bool(is_fallback)
    banner.is_primary_hero = bool(is_primary_hero)
    if img_local and img_local in IMG_META:
        w, h, sz = IMG_META[img_local]
        banner.img_w, banner.img_h, banner.img_bytes = w, h, sz
    return banner

def save_and_resize_image(context, img_url: str, brand_key: str, rank: int, referer: str = "") -> Tuple[str, str]:
    """
    returns: (local_filename, status)
      status: ok / cached / download_fail / blocked_html / http_403 / http_429 / http_xxx / no_url / exception
    """
    global PROG, IMG_URL_CACHE

    if not img_url or not img_url.strip():
        if PROG: PROG.add_img(False)
        return "", "no_url"

    img_url = img_url.strip()
    norm_key = normalize_img_url(img_url)
    if norm_key in IMG_URL_CACHE and IMG_URL_CACHE[norm_key]:
        return IMG_URL_CACHE[norm_key], "cached"

    os.makedirs(ASSET_DIR, exist_ok=True)
    out_ext = ".jpg" if PIL_OK else guess_ext(img_url)
    fname = safe_filename(f"{brand_key}_{rank}_{sha1(norm_key)}", out_ext)
    out_path = os.path.join(ASSET_DIR, fname)

    # 1) requests
    content, st = download_bytes_requests(img_url, referer=referer)

    # 2) playwright fallback
    if content is None and context is not None:
        content2, st2 = download_bytes_pw(context, img_url, referer=referer)
        if content2 is not None:
            content, st = content2, st2
        else:
            # 더 구체적인 상태를 보존(가능한 한)
            st = st2 if st2 not in {"pw_noresp", "pw_exception", "pw_notok"} else st

    if content is None:
        if PROG: PROG.add_img(False)
        # blocked 계열 분리
        if st in {"http_403", "http_429", "blocked_html"}:
            return "", st
        return "", "download_fail"

    # save (+ resize)
    if not PIL_OK:
        try:
            with open(out_path, "wb") as f:
                f.write(content)
            _record_img_meta(out_path, fname)
            IMG_URL_CACHE[norm_key] = fname
            if PROG: PROG.add_img(True)
            return fname, "ok"
        except Exception:
            if PROG: PROG.add_img(False)
            return "", "exception"

    try:
        im = Image.open(BytesIO(content))
        if im.mode in ("RGBA", "P"):
            im = im.convert("RGB")
        w, h = im.size
        if w > MAX_IMG_WIDTH:
            new_w = MAX_IMG_WIDTH
            new_h = int(h * (new_w / float(w)))
            im = im.resize((new_w, new_h), Image.LANCZOS)
        im.save(out_path, format="JPEG", quality=JPG_QUALITY, optimize=True)

        _record_img_meta(out_path, fname)
        IMG_URL_CACHE[norm_key] = fname
        if PROG: PROG.add_img(True)
        return fname, "ok"
    except Exception:
        # fallback raw write
        try:
            with open(out_path, "wb") as f:
                f.write(content)
            _record_img_meta(out_path, fname)
            IMG_URL_CACHE[norm_key] = fname
            if PROG: PROG.add_img(True)
            return fname, "ok"
        except Exception:
            if PROG: PROG.add_img(False)
            return "", "exception"


# =====================================================
# Playwright helpers
# =====================================================
def close_common_popups(page) -> None:
    sels = [
        "button:has-text('닫기')", "button:has-text('Close')",
        "button:has-text('확인')", "button:has-text('동의')",
        "button:has-text('오늘 하루 보지 않기')",
        "button[aria-label*='close' i]", "button[aria-label*='닫기']",
        ".modal .close", ".popup .close", ".layer .close", ".btn-close",
        "[role=dialog] button",
    ]
    for _ in range(2):
        for s in sels:
            try:
                loc = page.locator(s).first
                if loc.count() and loc.is_visible():
                    loc.click(timeout=1200, force=True)
                    page.wait_for_timeout(250)
            except Exception:
                pass

def wait_first_visible(page, selectors, timeout_ms: int = 12000):
    for sel in selectors:
        try:
            page.wait_for_selector(sel, timeout=timeout_ms, state="visible")
            return sel
        except Exception:
            continue
    return None

def is_closed_error(e: Exception) -> bool:
    msg = str(e).lower()
    return "has been closed" in msg or "target page" in msg or "browser has been closed" in msg

def launch(pw):
    browser = pw.chromium.launch(headless=HEADLESS)
    context = browser.new_context(
        user_agent=USER_AGENT,
        viewport={"width": 1440, "height": 900},
        locale="ko-KR",
    )
    return browser, context


def append_error_log(path: str, brand_key: str, stage: str, message: str,
                     brand_name: str = "", error_type: str = "", attempt: int = 0,
                     extra: Optional[Dict[str, Any]] = None) -> None:
    payload = {
        "ts": datetime.now(_KST).isoformat(),
        "brand_key": brand_key or "",
        "brand_name": brand_name or BRAND_NAME_MAP.get(brand_key, ""),
        "stage": stage or "",
        "error_type": error_type or "error",
        "message": norm_ws(message or ""),
        "attempt": int(attempt or 0),
    }
    if extra:
        payload["extra"] = extra
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        pass


def classify_img_status(status: str) -> str:
    st = (status or "").strip().lower()
    if st == "ok":
        return "img_ok"
    if st == "cached":
        return "img_cached"
    if st == "no_url":
        return "img_no_url"
    if st in {"http_403", "http_429", "blocked_html"}:
        return "img_blocked"
    if st in {"download_fail", "exception", "pw_exception", "pw_noresp", "pw_notok"}:
        return "img_transport_fail"
    if st.startswith("http_5"):
        return "img_transport_fail"
    if st.startswith("http_"):
        return "img_other_fail"
    return "img_other_fail" if st else ""


# =====================================================
# Campaign date extraction + caching
# =====================================================
DATE_PATTERNS = [
    re.compile(r"(\d{4}[./-]\d{1,2}[./-]\d{1,2})\s*(?:~|∼|–|-|—)\s*(\d{4}[./-]\d{1,2}[./-]\d{1,2})"),
    re.compile(r"(\d{4})[./-](\d{1,2})[./-](\d{1,2})\s*(?:~|∼|–|-|—)\s*(\d{1,2})[./-](\d{1,2})"),
    re.compile(r"(\d{4}[./-]\d{1,2}[./-]\d{1,2}).{0,12}?(?:부터|~|∼|–|-|—).{0,12}?(\d{4}[./-]\d{1,2}[./-]\d{1,2}).{0,8}?(?:까지)?"),
]

def _norm_date(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("/", "-").replace(".", "-")
    m = re.match(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if not m:
        return s
    y, mo, d = m.group(1), int(m.group(2)), int(m.group(3))
    return f"{y}-{mo:02d}-{d:02d}"

def extract_date_range_from_text(text: str) -> Tuple[str, str]:
    t = re.sub(r"\s+", " ", text or "")
    for pat in DATE_PATTERNS:
        m = pat.search(t)
        if not m:
            continue
        if pat.pattern.startswith(r"(\d{4})"):
            y = m.group(1)
            mo1, d1 = int(m.group(2)), int(m.group(3))
            mo2, d2 = int(m.group(4)), int(m.group(5))
            s = f"{y}-{mo1:02d}-{d1:02d}"
            e = f"{y}-{mo2:02d}-{d2:02d}"
            return _norm_date(s), _norm_date(e)
        s = _norm_date(m.group(1))
        e = _norm_date(m.group(2))
        return s, e
    return "", ""

def _campaign_cache_path() -> str:
    _safe_mkdir(CACHE_DIR)
    return os.path.join(CACHE_DIR, "campaign_dates.json")

def load_campaign_cache() -> Dict[str, Any]:
    path = _campaign_cache_path()
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f) or {}
    except Exception:
        pass
    return {}

def save_campaign_cache(cache: Dict[str, Any]) -> None:
    path = _campaign_cache_path()
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def _cache_fresh(ts_iso: str) -> bool:
    try:
        t = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - t) <= timedelta(days=CAMPAIGN_DATE_CACHE_TTL_DAYS)
    except Exception:
        return False

def fetch_campaign_dates(context, href: str) -> Tuple[str, str]:
    if not href or not FETCH_CAMPAIGN_DATES:
        return "", ""

    # cache hit
    cache = CAMPAIGN_CACHE
    key = normalize_href(href)
    if key and key in cache:
        item = cache.get(key) or {}
        if item.get("start") or item.get("end"):
            if _cache_fresh(item.get("checked_at", "")):
                return item.get("start", "") or "", item.get("end", "") or ""

    p = None
    s = e = ""
    try:
        p = context.new_page()
        p.goto(href, timeout=DATE_FETCH_TIMEOUT_MS, wait_until="domcontentloaded")
        try:
            p.wait_for_load_state("networkidle", timeout=2500)
        except Exception:
            pass
        try:
            body_text = p.locator("body").inner_text(timeout=2000)
        except Exception:
            body_text = ""
        s, e = extract_date_range_from_text(body_text)
        if not s:
            try:
                html = p.content()
            except Exception:
                html = ""
            s2, e2 = extract_date_range_from_text(html)
            if s2:
                s, e = s2, e2
    except Exception:
        s, e = "", ""
    finally:
        # write cache
        try:
            if key:
                cache[key] = {
                    "start": s or "",
                    "end": e or "",
                    "checked_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                }
        except Exception:
            pass
        try:
            if p:
                p.close()
        except Exception:
            pass
    return s, e


LANDING_TITLE_SELECTORS = [
    "main h1", "h1", "[class*='hero'] h1", "[class*='hero'] h2",
    "[class*='headline']", "[class*='title']", "[class*='tit']",
    ".event-title", ".promo-title", ".hero-title",
]
LANDING_SUBTITLE_SELECTORS = [
    "main h2", "[class*='subtitle']", "[class*='subtit']", "[class*='desc']",
    "[class*='copy']", "[class*='summary']", "[class*='lead']", "[class*='hero'] p",
]
LANDING_SUMMARY_SELECTORS = [
    "main p", "article p", "[class*='description']", "[class*='desc']",
    "[class*='copy']", "[class*='summary']", "[class*='lead']",
]
LANDING_CATEGORY_RULES = [
    ("하이킹", [r"hik", r"trail", r"trek", r"mountain", r"등산", r"산행"]),
    ("러닝", [r"run", r"runner", r"trail run", r"러닝", r"러너"]),
    ("캠핑", [r"camp", r"tent", r"sleeping", r"캠핑", r"텐트"]),
    ("라이프스타일", [r"lifestyle", r"casual", r"daily", r"urban", r"city", r"라이프스타일", r"일상"]),
    ("키즈", [r"kids", r"kid", r"junior", r"school", r"키즈", r"주니어"]),
    ("여성", [r"women", r"woman", r"female", r"여성", r"우먼"]),
    ("우천/방수", [r"rain", r"storm", r"waterproof", r"gore-tex", r"방수", r"우천", r"레인"]),
    ("풋웨어", [r"shoe", r"shoes", r"footwear", r"sneaker", r"boot", r"신발", r"슈즈"]),
    ("백팩", [r"backpack", r"rucksack", r"bag", r"pack", r"백팩", r"가방"]),
    ("재킷", [r"jacket", r"shell", r"wind", r"down", r"자켓", r"재킷"]),
]
BLOCKED_TEXT_PATTERNS = ["access denied", "forbidden", "captcha", "cloudflare", "bot verification", "서비스 이용에 불편"]


def _campaign_meta_cache_path() -> str:
    _safe_mkdir(CACHE_DIR)
    return os.path.join(CACHE_DIR, "campaign_page_meta.json")


def load_campaign_meta_cache() -> Dict[str, Any]:
    path = _campaign_meta_cache_path()
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f) or {}
    except Exception:
        pass
    return {}


def save_campaign_meta_cache(cache: Dict[str, Any]) -> None:
    path = _campaign_meta_cache_path()
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _cache_fresh_ttl(ts_iso: str, ttl_days: int) -> bool:
    try:
        t = datetime.fromisoformat((ts_iso or "").replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - t) <= timedelta(days=max(int(ttl_days or 1), 1))
    except Exception:
        return False


def _clean_text_list(values: List[str], max_items: int = 8, max_len: int = 220) -> List[str]:
    out: List[str] = []
    seen = set()
    for raw in values or []:
        txt = norm_ws(raw)
        txt = re.sub(r"\s+[|/>\-]+\s+", " ", txt)
        txt = txt.strip(" -|/>'\"")
        if len(txt) < 2:
            continue
        key = txt.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(txt[:max_len])
        if len(out) >= max_items:
            break
    return out


def _collect_locator_texts(page, selectors: List[str], limit: int = 6, max_len: int = 220) -> List[str]:
    texts: List[str] = []
    for sel in selectors:
        try:
            loc = page.locator(sel)
            cnt = min(loc.count(), limit)
        except Exception:
            cnt = 0
        for i in range(cnt):
            try:
                txt = loc.nth(i).inner_text(timeout=1200)
            except Exception:
                txt = ""
            texts.extend(_clean_text_list([txt], max_items=1, max_len=max_len))
            if len(texts) >= limit:
                return _clean_text_list(texts, max_items=limit, max_len=max_len)
    return _clean_text_list(texts, max_items=limit, max_len=max_len)


def _head_meta(page) -> Dict[str, str]:
    try:
        return page.evaluate(
            """() => {
                const read = (sel) => document.querySelector(sel)?.getAttribute('content') || '';
                return {
                    page_title: document.title || '',
                    og_title: read('meta[property=\"og:title\"]'),
                    twitter_title: read('meta[name=\"twitter:title\"]'),
                    description: read('meta[name=\"description\"]') || read('meta[property=\"og:description\"]') || read('meta[name=\"twitter:description\"]'),
                };
            }"""
        ) or {}
    except Exception:
        return {}


def _landing_keywords_from_text(*parts: str, limit: int = 6) -> List[str]:
    counter: Counter = Counter()
    text = " ".join([norm_ws(x) for x in parts if norm_ws(x)])
    english_tokens = re.findall(r"[A-Za-z][A-Za-z0-9'\-/&]{2,}", text)
    korean_tokens = re.findall(r"[가-힣]{2,}", text)
    for raw in english_tokens + korean_tokens:
        token = raw.strip("-_/ ").upper() if re.search(r"[A-Za-z]", raw) else raw
        if len(token) < 2:
            continue
        if token.lower() in KEYWORD_STOPWORDS or token in KEYWORD_STOPWORDS:
            continue
        counter[token] += 1
    return [k for k, _ in counter.most_common(limit)]


def _infer_landing_category(title: str, subtitle: str, summary: str, keywords: List[str]) -> str:
    hay = " ".join([title, subtitle, summary, " ".join(keywords)]).lower()
    for category, patterns in LANDING_CATEGORY_RULES:
        for pat in patterns:
            if re.search(pat, hay, re.I):
                return category
    return ""


def _build_landing_summary(title: str, subtitle: str, summary: str, keywords: List[str]) -> str:
    if summary:
        return summary[:220]
    if title and subtitle and subtitle.lower() not in title.lower():
        return f"{title} - {subtitle}"[:220]
    if title and keywords:
        return f"{title} | 키워드: {', '.join(keywords[:4])}"[:220]
    if title:
        return title[:220]
    return "기획전 설명 추출 실패"


def _is_blocked_body(*parts: str) -> bool:
    body = " ".join([norm_ws(x) for x in parts if norm_ws(x)]).lower()
    return any(token in body for token in BLOCKED_TEXT_PATTERNS)


def _campaign_meta_defaults(status: str = "") -> Dict[str, Any]:
    return {
        "plan_start": "",
        "plan_end": "",
        "landing_title": "",
        "landing_subtitle": "",
        "landing_summary": "",
        "landing_keywords": [],
        "landing_category": "",
        "landing_text_status": status,
    }


def fetch_campaign_page_meta(context, href: str) -> Dict[str, Any]:
    if not href or not FETCH_CAMPAIGN_META:
        return _campaign_meta_defaults("skipped")

    key = normalize_href(href)
    cache = CAMPAIGN_META_CACHE
    if key and key in cache:
        cached = cache.get(key) or {}
        if _cache_fresh_ttl(cached.get("checked_at", ""), CAMPAIGN_META_CACHE_TTL_DAYS):
            out = _campaign_meta_defaults(cached.get("landing_text_status", ""))
            out.update(cached)
            out["landing_keywords"] = list(cached.get("landing_keywords") or [])
            return out

    page = None
    out = _campaign_meta_defaults()
    checked_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    try:
        page = context.new_page()
        page.goto(href, timeout=DATE_FETCH_TIMEOUT_MS, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=2500)
        except Exception:
            pass
        page.wait_for_timeout(300)

        head = _head_meta(page)
        try:
            body_text = page.locator("body").inner_text(timeout=1500)
        except Exception:
            body_text = ""
        try:
            main_text = page.locator("main").first.inner_text(timeout=1200)
        except Exception:
            main_text = ""
        try:
            html_text = page.content()
        except Exception:
            html_text = ""

        title_candidates = _clean_text_list([
            head.get("og_title", ""),
            head.get("twitter_title", ""),
            head.get("page_title", ""),
        ] + _collect_locator_texts(page, LANDING_TITLE_SELECTORS, limit=6, max_len=120), max_items=8, max_len=120)
        subtitle_candidates = _clean_text_list(_collect_locator_texts(page, LANDING_SUBTITLE_SELECTORS, limit=8, max_len=160), max_items=8, max_len=160)
        summary_candidates = _clean_text_list([
            head.get("description", ""),
        ] + _collect_locator_texts(page, LANDING_SUMMARY_SELECTORS, limit=10, max_len=220), max_items=10, max_len=220)

        title = choose_title(*title_candidates)
        if title == "메인 배너":
            title = ""
        subtitle = ""
        for cand in subtitle_candidates:
            if cand and cand.lower() != title.lower():
                subtitle = cand
                break
        summary_text = ""
        for cand in summary_candidates:
            if not cand:
                continue
            if title and cand.lower() == title.lower():
                continue
            if subtitle and cand.lower() == subtitle.lower():
                continue
            summary_text = cand
            break

        s, e = extract_date_range_from_text(" ".join([body_text, main_text, html_text]))
        keywords = _landing_keywords_from_text(title, subtitle, summary_text, body_text[:300])
        category = _infer_landing_category(title, subtitle, summary_text, keywords)
        blocked = _is_blocked_body(head.get("page_title", ""), body_text, html_text[:800])

        out.update({
            "plan_start": s,
            "plan_end": e,
            "landing_title": title,
            "landing_subtitle": subtitle,
            "landing_summary": _build_landing_summary(title, subtitle, summary_text, keywords),
            "landing_keywords": keywords,
            "landing_category": category,
            "landing_text_status": "blocked" if blocked else ("ok" if any([title, subtitle, summary_text, keywords, s, e]) else "no_text"),
        })
    except PWTimeoutError:
        out["landing_text_status"] = "timeout"
    except Exception:
        out["landing_text_status"] = "exception"
    finally:
        try:
            if key:
                cache[key] = dict(out, checked_at=checked_at)
        except Exception:
            pass
        try:
            if page:
                page.close()
        except Exception:
            pass
    return out


# =====================================================
# Extractors helpers
# =====================================================
def get_any_alt_text(el) -> str:
    try:
        img = el.locator("img").first
        if img.count():
            return norm_ws(img.get_attribute("alt") or "")
    except Exception:
        pass
    return ""

def get_any_img_url(el, base_url: str) -> str:
    # 1) inline style background-image on self
    try:
        bg = get_bg_image(el.get_attribute("style") or "")
        if bg:
            return abs_url(base_url, bg)
    except Exception:
        pass

    # 1.5) poster / data-poster
    try:
        poster = (el.get_attribute("poster") or el.get_attribute("data-poster") or el.get_attribute("data-poster-url") or "").strip()
        if poster:
            return abs_url(base_url, poster)
        pnode = el.locator("[poster], [data-poster], [data-poster-url], video[poster]").first
        if pnode.count():
            poster2 = (pnode.get_attribute("poster") or pnode.get_attribute("data-poster") or pnode.get_attribute("data-poster-url") or "").strip()
            if poster2:
                return abs_url(base_url, poster2)
    except Exception:
        pass

    # 2) common data attributes on self
    try:
        for attr in ["data-bg", "data-background", "data-image", "data-img", "data-src", "data-original"]:
            v = (el.get_attribute(attr) or "").strip()
            if v and not v.startswith("data:"):
                return abs_url(base_url, v)
    except Exception:
        pass

    # 3) computed style background-image on self
    try:
        bg_css = el.evaluate("e => window.getComputedStyle(e).backgroundImage")
        u = _extract_url_from_css(bg_css)
        if u:
            return abs_url(base_url, u)
    except Exception:
        pass

    # 4) descendant with inline background-image
    try:
        bg_el = el.locator("[style*='background-image']").first
        if bg_el.count():
            bg = get_bg_image(bg_el.get_attribute("style") or "")
            if bg:
                return abs_url(base_url, bg)
        try:
            bg_css = bg_el.evaluate("e => window.getComputedStyle(e).backgroundImage")
            u = _extract_url_from_css(bg_css)
            if u:
                return abs_url(base_url, u)
        except Exception:
            pass
    except Exception:
        pass

    # 5) <picture><source srcset>
    try:
        source = el.locator("source[srcset]").first
        if source.count():
            srcset = source.get_attribute("srcset") or ""
            u = pick_from_srcset(srcset)
            if u:
                return abs_url(base_url, u)
    except Exception:
        pass

    # 6) <img> lazy attrs / srcset
    try:
        img = el.locator("img").first
        if img.count():
            for attr in ["src", "data-src", "data-lazy", "data-original", "data-img", "data-image"]:
                v = (img.get_attribute(attr) or "").strip()
                if v and not v.startswith("data:"):
                    return abs_url(base_url, v)
            srcset = img.get_attribute("srcset") or ""
            u = pick_from_srcset(srcset)
            if u:
                return abs_url(base_url, u)
    except Exception:
        pass

    # 7) scan descendant imgs/sources
    try:
        imgs = el.locator("img, source[srcset]")
        cnt = min(imgs.count(), 10)
        for i in range(cnt):
            node = imgs.nth(i)
            tag = ""
            try:
                tag = (node.evaluate("e => e.tagName") or "").lower()
            except Exception:
                tag = ""
            if tag == "source":
                u = pick_from_srcset(node.get_attribute("srcset") or "")
                if u:
                    return abs_url(base_url, u)
            else:
                for attr in ["src", "data-src", "data-lazy", "data-original", "srcset"]:
                    v = (node.get_attribute(attr) or "").strip()
                    if not v or v.startswith("data:"):
                        continue
                    if attr == "srcset":
                        u = pick_from_srcset(v)
                        if u:
                            return abs_url(base_url, u)
                    else:
                        return abs_url(base_url, v)
    except Exception:
        pass

    # 8) scan descendants for computed background-image
    try:
        bg_nodes = el.locator("div, span, a, section, figure")
        cnt = min(bg_nodes.count(), 80)
        for i in range(cnt):
            n = bg_nodes.nth(i)
            try:
                bg_css = n.evaluate("e => window.getComputedStyle(e).backgroundImage")
                u = _extract_url_from_css(bg_css)
                if u:
                    return abs_url(base_url, u)
            except Exception:
                continue
    except Exception:
        pass

    # 9) pseudo-element background-image
    try:
        nodes = el.locator("div, span, a, section, figure")
        cnt = min(nodes.count(), 80)
        for i in range(cnt):
            n = nodes.nth(i)
            try:
                bg_b = n.evaluate("e => getComputedStyle(e, '::before').backgroundImage")
                u = _extract_url_from_css(bg_b)
                if u:
                    return abs_url(base_url, u)
                bg_a = n.evaluate("e => getComputedStyle(e, '::after').backgroundImage")
                u = _extract_url_from_css(bg_a)
                if u:
                    return abs_url(base_url, u)
            except Exception:
                continue
    except Exception:
        pass

    return ""


# =====================================================
# Dedupe + enrich (dates)
# =====================================================
def dedupe_brand_rows(rows: List[Banner]) -> List[Banner]:
    """브랜드 내 중복 제거: href_clean + img_url(정규화)"""
    if not rows:
        return rows
    rows_sorted = sorted(
        rows,
        key=lambda x: (
            int(x.rank or 9999),
            0 if x.is_primary_hero else 1,
            0 if not x.is_fallback else 1,
            -float(x.confidence_score or 0.0),
        ),
    )

    seen_href = set()
    seen_img = set()
    out = []
    for b in rows_sorted:
        hc = b.href_clean or normalize_href(b.href)
        iu = normalize_img_url(b.img_url or "")

        if hc:
            if hc in seen_href:
                continue
            seen_href.add(hc)

        if iu:
            if iu in seen_img:
                continue
            seen_img.add(iu)

        out.append(b)

    for i, b in enumerate(out, start=1):
        b.rank = i
    return out

def enrich_campaign_meta_for_rows(context, rows: List[Banner]) -> None:
    if not FETCH_CAMPAIGN_META:
        return
    for b in rows:
        if int(b.rank or 0) > CAMPAIGN_META_RANK_LIMIT:
            b.landing_text_status = b.landing_text_status or "skipped"
            continue
        if not b.href_clean and b.href:
            b.href_clean = normalize_href(b.href)
        h = b.href_clean or b.href
        if not h:
            b.landing_text_status = b.landing_text_status or "no_href"
            continue
        meta = fetch_campaign_page_meta(context, h)
        b.plan_start = meta.get("plan_start", "") or ""
        b.plan_end = meta.get("plan_end", "") or ""
        b.landing_title = meta.get("landing_title", "") or ""
        b.landing_subtitle = meta.get("landing_subtitle", "") or ""
        b.landing_summary = meta.get("landing_summary", "") or ""
        b.landing_keywords = ", ".join(meta.get("landing_keywords") or [])
        b.landing_category = meta.get("landing_category", "") or ""
        b.landing_text_status = meta.get("landing_text_status", "") or ""


def enrich_dates_for_rows(context, rows: List[Banner]) -> None:
    enrich_campaign_meta_for_rows(context, rows)


# =====================================================
# Extractors
# =====================================================
def tnf_slick(context, page, base_url: str, brand_key: str, brand_name: str, date_s: str, max_items: int):
    out: List[Banner] = []
    close_common_popups(page)
    container_sel = "[data-module-main-slick-slider]"
    try:
        page.wait_for_selector(container_sel, timeout=12000)
    except Exception:
        container_sel = ".st_component-slider.slick-slider, .st_component-slider"

    items = []
    try:
        items = page.evaluate(
            r"""
            (sel) => {
                const root = document.querySelector(sel) || document;
                const slides = Array.from(root.querySelectorAll('.slide-item.slick-slide[data-slick-index]'))
                    .filter(el => !el.classList.contains('slick-cloned') && (el.getAttribute('data-slick-index') || '0') !== '-1');
                const norm = (s) => (s||'').replace(/\s+/g,' ').trim();
                const pickImg = (el) => {
                    const source = el.querySelector('picture source[srcset]');
                    if (source) {
                        const ss = source.getAttribute('srcset') || '';
                        const first = ss.split(',')[0].trim().split(' ')[0];
                        if (first) return first;
                    }
                    const img = el.querySelector('picture img') || el.querySelector('img');
                    if (img) return img.getAttribute('src')
                        || img.getAttribute('data-src')
                        || img.getAttribute('data-original')
                        || img.getAttribute('srcset')
                        || '';
                    const bgEl = el.querySelector('[style*="background-image"]');
                    if (bgEl) return (bgEl.getAttribute('style')||'').match(/url\(([^)]+)\)/i)?.[1]?.replace(/["']/g,'') || '';
                    return '';
                };
                return slides.map(el => {
                    const a = el.querySelector('a[href]');
                    const href = a ? a.getAttribute('href') : '';
                    const img = pickImg(el);
                    const alt = (el.querySelector('img[alt]')?.getAttribute('alt')) || '';
                    const txt = norm(el.querySelector('.img-title-wrap, .slider-contents, h1, h2, h3, p, strong')?.innerText || '');
                    return {href, img, alt, txt};
                });
            }
            """,
            container_sel,
        )
    except Exception:
        items = []

    uniq, seen = [], set()
    for it in items or []:
        href = abs_url(base_url, (it.get("href") or "").strip()) if it else ""
        img_url = abs_url(base_url, (it.get("img") or "").strip()) if it else ""
        key = (normalize_href(href), normalize_img_url(img_url))
        if not href and not img_url:
            continue
        if key in seen:
            continue
        seen.add(key)
        uniq.append(it)

    rank = 1
    for it in uniq:
        if rank > max_items:
            break
        href = abs_url(base_url, (it.get("href") or "").strip())
        img_url = abs_url(base_url, (it.get("img") or "").strip())
        alt = norm_ws(it.get("alt") or "")
        txt = norm_ws(it.get("txt") or "")

        title = choose_title(txt, alt)
        img_local, st = save_and_resize_image(context, img_url, brand_key, rank, referer=base_url) if img_url else ("", "no_url")

        b = build_banner(date_s, brand_key, brand_name, rank, title, href, img_url, img_local, st,
                         extract_source="tnf_slick", confidence_score=0.96,
                         is_fallback=False, is_primary_hero=(rank == 1))
        out.append(b)
        rank += 1
    return out

def discovery_swiper(context, page, base_url: str, brand_key: str, brand_name: str, date_s: str, max_items: int):
    out: List[Banner] = []
    close_common_popups(page)
    root_sel = ".click_banner_main"
    try:
        page.wait_for_selector(root_sel, timeout=18000)
        page.wait_for_timeout(800)
    except Exception:
        return out

    root = page.locator(root_sel).first
    if not root.count():
        return out

    slides = root.locator("div.swiper-slide")
    n = slides.count()
    candidates = []
    for i in range(min(n, 30)):
        sl = slides.nth(i)
        try:
            href = ""
            a = sl.locator("a[href]").first
            if a.count():
                href = abs_url(base_url, a.get_attribute("href") or "")
            img_url = get_any_img_url(sl, base_url)

            title_txt = ""
            try:
                title_txt = norm_ws(sl.locator(".click_banner_main_name").first.inner_text() or "")
            except Exception:
                try:
                    title_txt = norm_ws(sl.inner_text() or "")
                except Exception:
                    title_txt = ""

            idx = 9999
            try:
                v = sl.get_attribute("data-swiper-slide-index") or ""
                if v.strip().isdigit():
                    idx = int(v.strip())
            except Exception:
                pass

            if not img_url:
                continue

            key = sha1("\n".join([normalize_href(href), normalize_img_url(img_url)]))
            candidates.append((idx, key, title_txt, href, img_url))
        except Exception:
            continue

    candidates.sort(key=lambda x: x[0])

    seen, rank = set(), 1
    for idx, key, title_txt, href, img_url in candidates:
        if rank > max_items:
            break
        if key in seen:
            continue
        seen.add(key)

        title = choose_title(title_txt, get_any_alt_text(page.locator("body")))
        img_local, st = save_and_resize_image(context, img_url, brand_key, rank, referer=base_url)

        b = build_banner(date_s, brand_key, brand_name, rank, title, href, img_url, img_local, st,
                         extract_source="discovery_swiper", confidence_score=0.93,
                         is_fallback=False, is_primary_hero=(rank == 1))
        out.append(b)
        rank += 1
    return out

def blackyak_swiper(context, page, base_url: str, brand_key: str, brand_name: str, date_s: str, max_items: int):
    out: List[Banner] = []
    close_common_popups(page)
    root_sel = "#main_banner_section"
    try:
        page.wait_for_selector(root_sel, timeout=15000)
    except Exception:
        root_sel = "body"

    items = []
    try:
        items = page.evaluate(
            r"""
            (sel) => {
                const root = document.querySelector(sel) || document;
                const anchors = Array.from(root.querySelectorAll('.MAIN-VISUAL-SWIPER .swiper-slide a.item, .MAIN-VISUAL-SWIPER .swiper-slide a'))
                    .filter(a => a && a.querySelector('img'));
                const norm = (s) => (s||'').replace(/\s+/g,' ').trim();
                const pickImg = (a) => {
                    const img = a.querySelector('img');
                    if (!img) return '';
                    return img.getAttribute('src')
                        || img.getAttribute('data-src')
                        || img.getAttribute('data-original')
                        || '';
                };
                const pickTitle = (a) => {
                    const t2 = a.querySelector('.TEXT-2')?.innerText || '';
                    const t3 = a.querySelector('.TEXT-3')?.innerText || '';
                    return norm((t2 + ' ' + t3).trim());
                };
                return anchors.map(a => ({
                    href: a.getAttribute('href') || '',
                    img: pickImg(a),
                    alt: a.querySelector('img')?.getAttribute('alt') || '',
                    txt: pickTitle(a)
                }));
            }
            """,
            root_sel,
        )
    except Exception:
        items = []

    uniq, seen = [], set()
    for it in items or []:
        href = abs_url(base_url, (it.get("href") or "").strip())
        img = (it.get("img") or "").strip()
        if img.startswith("//"):
            img = "https:" + img
        img_url = abs_url(base_url, img)
        key = (normalize_href(href), normalize_img_url(img_url))
        if not img_url:
            continue
        if key in seen:
            continue
        seen.add(key)
        it["_href_abs"] = href
        it["_img_abs"] = img_url
        uniq.append(it)

    rank = 1
    for it in uniq:
        if rank > max_items:
            break
        href = it.get("_href_abs") or abs_url(base_url, it.get("href") or "")
        img_url = it.get("_img_abs") or abs_url(base_url, it.get("img") or "")
        alt = norm_ws(it.get("alt") or "")
        txt = norm_ws(it.get("txt") or "")

        title = choose_title(txt, alt)
        img_local, st = save_and_resize_image(context, img_url, brand_key, rank, referer=base_url)

        b = build_banner(date_s, brand_key, brand_name, rank, title, href, img_url, img_local, st,
                         extract_source="blackyak_swiper", confidence_score=0.93,
                         is_fallback=False, is_primary_hero=(rank == 1))
        out.append(b)
        rank += 1
    return out

def nepa_static(context, page, base_url: str, brand_key: str, brand_name: str, date_s: str, max_items: int):
    out: List[Banner] = []
    root = page
    try:
        for fr in page.frames:
            u = (fr.url or "").lower()
            if "nplus" in u or "nplus.co.kr" in u or "nepa" in u:
                try:
                    if fr.locator("#pcContents, .promo-banner01, .promo-banner").count():
                        root = fr
                        break
                except Exception:
                    pass
    except Exception:
        pass

    wait_first_visible(page, ["#pcContents", "div.promo-banner01.promo-banner", "div.promo-banner01", "iframe"], 15000)

    def extract_from_banner(idx: int, rank: int):
        cls = f"#pcContents .promo-banner{idx:02d}.promo-banner, #pcContents .promo-banner{idx}.promo-banner, .promo-banner{idx:02d}.promo-banner, .promo-banner{idx}.promo-banner"
        box = root.locator(cls).first
        if not box.count():
            return None

        img_url = get_any_img_url(box, base_url)
        href = ""
        try:
            a = box.locator("a[href]").first
            if a.count():
                href = abs_url(base_url, a.get_attribute("href") or "")
        except Exception:
            pass

        title_txt = ""
        try:
            title_txt = norm_ws(box.inner_text() or "")
        except Exception:
            pass

        title = choose_title(title_txt, get_any_alt_text(box))
        if not img_url:
            return None

        img_local, st = save_and_resize_image(context, img_url, brand_key, rank, referer=base_url)

        return build_banner(date_s, brand_key, brand_name, rank, title, href, img_url, img_local, st,
                            extract_source="nepa_static", confidence_score=0.92,
                            is_fallback=False, is_primary_hero=(rank == 1))

    rank = 1
    for idx in range(1, 35):
        if rank > max_items:
            break
        b = extract_from_banner(idx, rank)
        if b:
            out.append(b)
            rank += 1
        if idx >= 6 and len(out) >= max_items:
            break
    return out

def patagonia_static_hero(context, page, base_url: str, brand_key: str, brand_name: str, date_s: str):
    wait_first_visible(page, ["header", "main", "section"], 12000)
    vw = page.viewport_size["width"] if page.viewport_size else 1440
    best_area = 0
    best_img = ""
    best_title = ""
    best_href = ""

    candidates = page.locator("section, div")
    cnt = min(candidates.count(), 260)
    for i in range(cnt):
        el = candidates.nth(i)
        try:
            if not el.is_visible():
                continue
            bb = el.bounding_box()
            if not bb:
                continue
            if bb["y"] < -80 or bb["y"] > 520:
                continue
            if bb["width"] < vw * 0.75 or bb["height"] < 320:
                continue
            img_url = get_any_img_url(el, base_url)
            if not img_url:
                continue
            area = bb["width"] * bb["height"]
            if area > best_area:
                best_area = area
                try:
                    best_title = norm_ws(el.locator("h1,h2,h3,strong").first.inner_text() or "")
                except Exception:
                    best_title = ""
                if not best_title:
                    best_title = get_any_alt_text(el)
                try:
                    a = el.locator("a[href]").first
                    best_href = abs_url(base_url, a.get_attribute("href") or "") if a.count() else ""
                except Exception:
                    best_href = ""
                best_img = img_url
        except Exception:
            continue

    if not best_img:
        return []

    title = choose_title(best_title, urllib.parse.unquote((best_img or "").split("/")[-1]))
    img_local, st = save_and_resize_image(context, best_img, brand_key, 1, referer=base_url)

    b = build_banner(date_s, brand_key, brand_name, 1, title, best_href, best_img, img_local, st,
                     extract_source="patagonia_static_hero", confidence_score=0.94,
                     is_fallback=False, is_primary_hero=True)
    return [b]


# =====================================================
# generic_top_banners
# =====================================================


def deep_section_banners(context, page, base_url: str, brand_key: str, brand_name: str, date_s: str,
                         max_items: int, y_max: int = SECTION_SCAN_Y_MAX) -> List[Banner]:
    """
    섹션/서브배너용 broad extractor.
    - 상단 hero 뿐 아니라 메인 내 section/banner/card를 넓게 스캔
    - Arc'teryx 서브배너, K2 기획전 카드, TNF 누락 보완용
    """
    out: List[Banner] = []
    close_common_popups(page)

    try:
        for y in (0, 600, 1400, 2400, min(y_max, 3600)):
            page.evaluate(f"window.scrollTo(0, {y});")
            page.wait_for_timeout(220)
        page.evaluate("window.scrollTo(0, 0);")
        page.wait_for_timeout(250)
    except Exception:
        pass

    js = r"""
    (yMax, nodeLimit) => {
      const norm = (s) => (s||'').replace(/\s+/g,' ').trim();
      const pickSrc = (root) => {
        const source = root.querySelector('source[srcset]');
        if (source) {
          const ss = source.getAttribute('srcset') || '';
          const first = ss.split(',')[0].trim().split(' ')[0];
          if (first) return first;
        }
        const img = root.querySelector('img');
        if (img) {
          return img.getAttribute('src')
              || img.getAttribute('data-src')
              || img.getAttribute('data-lazy')
              || img.getAttribute('data-original')
              || img.getAttribute('data-image')
              || (img.getAttribute('srcset') ? (img.getAttribute('srcset').split(',')[0].trim().split(' ')[0]) : '')
              || '';
        }
        const bg = root.querySelector('[style*="background-image"]') || root;
        const style = (bg.getAttribute && bg.getAttribute('style')) || '';
        const m = style.match(/url\(([^)]+)\)/i);
        if (m && m[1]) return m[1].replace(/["']/g,'');
        return '';
      };

      const nodes = Array.from(document.querySelectorAll('section, article, li, a, div, figure'));
      const out = [];
      for (const n of nodes.slice(0, nodeLimit)) {
        const r = n.getBoundingClientRect();
        if (!r || r.width < 220 || r.height < 90) continue;
        if (r.y < -120 || r.y > yMax) continue;

        const img = pickSrc(n);
        if (!img) continue;

        const a = n.matches('a[href]') ? n : n.querySelector('a[href]');
        const href = a ? (a.getAttribute('href') || '') : '';
        const alt = n.querySelector('img[alt]')?.getAttribute('alt') || '';
        const txt = norm(
          n.querySelector('h1,h2,h3,strong,p,.title,.tit,.txt,.copy,.desc,.name,.subject')?.innerText
          || (a ? a.innerText : '')
          || n.innerText
          || ''
        ).slice(0, 160);

        let section = '';
        const sec = n.closest('section, article, [id], [class]');
        if (sec) {
          section = norm((sec.getAttribute('id') || '') + ' ' + (sec.getAttribute('class') || '')).slice(0, 120);
        }

        out.push({
          y: r.y,
          area: r.width * r.height,
          href,
          img,
          alt,
          txt,
          section,
        });
      }
      out.sort((a,b) => a.y - b.y || b.area - a.area);
      return out;
    }
    """

    try:
        raw = page.evaluate(js, y_max, DEEP_SCAN_NODE_LIMIT)
    except Exception:
        raw = []

    seen = set()
    rank = 1
    for it in raw or []:
        if rank > max_items:
            break
        href = abs_url(base_url, (it.get('href') or '').strip())
        img_url = abs_url(base_url, (it.get('img') or '').strip())
        if not img_url:
            continue
        fp = (normalize_href(href), normalize_img_url(img_url))
        if fp in seen:
            continue
        seen.add(fp)

        section = norm_ws(it.get('section') or '')
        txt = norm_ws(it.get('txt') or '')
        alt = norm_ws(it.get('alt') or '')
        title = choose_title(txt, alt)
        if section and section.lower() not in title.lower() and len(section) <= 40:
            title = clean_campaign_title(f"{title} | {section}")

        img_local, st = save_and_resize_image(context, img_url, brand_key, rank, referer=base_url)
        b = build_banner(date_s, brand_key, brand_name, rank, title, href, img_url, img_local, st,
                         extract_source="deep_section", confidence_score=0.68,
                         is_fallback=True, is_primary_hero=False)
        out.append(b)
        rank += 1

    return out


def hero_sections(context, page, base_url: str, brand_key: str, brand_name: str, date_s: str, max_items: int):
    """
    섹션별/서브배너까지 최대한 넓게 수집.
    - hero_slider + deep section scan 병합
    - 브랜드별 기획전/중간 배너/서브배너 보강
    """
    merged: List[Banner] = []
    try:
        merged.extend(hero_slider(context, page, base_url, brand_key, brand_name, date_s, max_items))
    except Exception:
        pass
    try:
        merged.extend(deep_section_banners(context, page, base_url, brand_key, brand_name, date_s, max_items, y_max=SECTION_SCAN_Y_MAX))
    except Exception:
        pass
    merged = dedupe_brand_rows(merged)
    return merged[:max_items]

def generic_top_banners(context, page_or_frame, base_url: str, brand_key: str, brand_name: str, date_s: str,
                        max_items: int, y_max: int = SECTION_SCAN_Y_MAX):
    out: List[Banner] = []
    try:
        vw = page_or_frame.viewport_size["width"] if getattr(page_or_frame, "viewport_size", None) else 1440
    except Exception:
        vw = 1440

    # scroll nudges
    try:
        page_or_frame.evaluate("window.scrollTo(0, 150);")
        page_or_frame.wait_for_timeout(250)
        page_or_frame.evaluate("window.scrollTo(0, 700);")
        page_or_frame.wait_for_timeout(300)
        page_or_frame.evaluate("window.scrollTo(0, 0);")
        page_or_frame.wait_for_timeout(250)
    except Exception:
        pass

    try:
        candidates = page_or_frame.locator("a, section, article, li, div")
        cnt = min(candidates.count(), DEEP_SCAN_NODE_LIMIT)
    except Exception:
        return out

    seen = set()
    rank = 1
    for i in range(cnt):
        if rank > max_items:
            break
        el = candidates.nth(i)
        try:
            if not el.is_visible():
                continue
            bb = el.bounding_box()
            if not bb:
                continue
            if bb["y"] < -120 or bb["y"] > y_max:
                continue
            if bb["width"] < max(vw * 0.28, 220) or bb["height"] < 90:
                continue

            img_url = get_any_img_url(el, base_url)
            img_url = abs_url(base_url, img_url) if img_url else ""
            if not img_url:
                continue

            href = ""
            try:
                is_a = el.evaluate("e => e.tagName.toLowerCase()") == "a"
                a = el if is_a else el.locator("a[href]").first
                if a and a.count():
                    href = abs_url(base_url, a.get_attribute("href") or "")
            except Exception:
                pass

            fp = sha1("\n".join([normalize_href(href), normalize_img_url(img_url)]))
            if fp in seen:
                continue
            seen.add(fp)

            title_txt = ""
            try:
                # 텍스트 오염 줄이기: 헤딩 우선
                title_txt = norm_ws(el.locator("h1,h2,h3,strong").first.inner_text() or "")
            except Exception:
                try:
                    title_txt = norm_ws(el.locator("p").first.inner_text() or "")
                except Exception:
                    title_txt = ""

            title = choose_title(title_txt, get_any_alt_text(el))
            img_local, st = save_and_resize_image(context, img_url, brand_key, rank, referer=base_url)

            b = build_banner(date_s, brand_key, brand_name, rank, title, href, img_url, img_local, st,
                             extract_source="generic_fallback", confidence_score=0.55,
                             is_fallback=True, is_primary_hero=False)
            out.append(b)
            rank += 1
        except Exception:
            continue

    return out


# =====================================================
# hero_slider extractor (컨테이너 우선 + broad fallback)
# =====================================================
def _hero_slider_from_container(context, page, base_url: str, brand_key: str, brand_name: str, date_s: str, max_items: int) -> List[Banner]:
    """
    컨테이너 우선:
    - 상단(y<700)에서 큰 컨테이너 1~2개 선택
    - 해당 컨테이너 내부에서 swiper/slick/carousel slide만 추출
    """
    out: List[Banner] = []
    try:
        containers = page.evaluate(
            r"""
            () => {
              const sel = ['main','section','div'];
              const nodes = [];
              sel.forEach(s => document.querySelectorAll(s).forEach(n => nodes.push(n)));
              const items = [];
              for (const n of nodes) {
                const r = n.getBoundingClientRect();
                if (!r || r.width < 700 || r.height < 280) continue;
                if (r.y < -120 || r.y > 700) continue;
                const area = r.width * r.height;
                // hero 단서: 내부에 swiper/slick/carousel 클래스가 있으면 가중치
                const hasSlider = !!(n.querySelector('.swiper, .swiper-container, .swiper-wrapper, .slick-slider, .slick-track, .carousel, .carousel-inner, [data-swiper-slide-index], [data-slick-index], .swiper-slide, .slick-slide'));
                items.push({
                  y: r.y, w: r.width, h: r.height, area,
                  bonus: hasSlider ? 1 : 0,
                  path: (() => {
                    // 간단한 css path(디버깅용)
                    let p = n.tagName.toLowerCase();
                    if (n.id) p += '#' + n.id;
                    const cls = (n.className || '').toString().trim().split(/\s+/).filter(Boolean).slice(0,3);
                    if (cls.length) p += '.' + cls.join('.');
                    return p;
                  })()
                });
              }
              items.sort((a,b) => (b.bonus - a.bonus) || (b.area - a.area) || (a.y - b.y));
              return items.slice(0, 2);
            }
            """
        )
    except Exception:
        containers = []

    if not containers:
        return out

    # 컨테이너 선택 후, 해당 컨테이너 내부 slide 추출
    for c in containers:
        # path는 디버깅용이고, 실제 선택은 bbox 기반으로 JS에서 직접 해당 영역 안 요소를 찾는 방식
        try:
            candidates = page.evaluate(
                r"""
                (yMax) => {
                  const norm = (s) => (s||'').replace(/\s+/g,' ').trim();
                  const pickSrc = (root) => {
                    const source = root.querySelector('source[srcset]');
                    if (source) {
                      const ss = source.getAttribute('srcset') || '';
                      const first = ss.split(',')[0].trim().split(' ')[0];
                      if (first) return first;
                    }
                    const img = root.querySelector('img');
                    if (img) {
                      return img.getAttribute('src')
                          || img.getAttribute('data-src')
                          || img.getAttribute('data-lazy')
                          || img.getAttribute('data-original')
                          || (img.getAttribute('srcset') ? (img.getAttribute('srcset').split(',')[0].trim().split(' ')[0]) : '')
                          || '';
                    }
                    const bg = root.querySelector('[style*="background-image"]');
                    if (bg) {
                      const m = (bg.getAttribute('style')||'').match(/url\(([^)]+)\)/i);
                      if (m && m[1]) return m[1].replace(/["']/g,'');
                    }
                    return '';
                  };

                  // 상단 컨테이너 후보를 다시 계산해서 top2를 골라 내부에서만 추출
                  const nodes = Array.from(document.querySelectorAll('main, section, div'));
                  const cons = nodes.map(n => {
                    const r = n.getBoundingClientRect();
                    if (!r || r.width < 700 || r.height < 280) return null;
                    if (r.y < -120 || r.y > yMax) return null;
                    const area = r.width * r.height;
                    const hasSlider = !!(n.querySelector('.swiper, .swiper-container, .swiper-wrapper, .slick-slider, .slick-track, .carousel, .carousel-inner, [data-swiper-slide-index], [data-slick-index], .swiper-slide, .slick-slide'));
                    return {n, y:r.y, area, bonus: hasSlider ? 1:0};
                  }).filter(Boolean);

                  cons.sort((a,b) => (b.bonus-a.bonus) || (b.area-a.area) || (a.y-b.y));
                  const target = cons.slice(0, 2).map(x => x.n);

                  const slideSelectors = [
                    '.swiper-slide',
                    '.slick-slide',
                    '[data-swiper-slide-index]',
                    '[data-slick-index]',
                    '.carousel-item',
                  ];

                  const out = [];
                  for (const root of target) {
                    const slides = [];
                    slideSelectors.forEach(sel => {
                      root.querySelectorAll(sel).forEach(n => slides.push(n));
                    });

                    const withBox = slides.map(n => {
                      const r = n.getBoundingClientRect();
                      return {n, y:r.y, w:r.width, h:r.height};
                    }).filter(o => o.w > 420 && o.h > 160 && o.y < 1200);

                    withBox.sort((a,b) => a.y - b.y);

                    for (const o of withBox.slice(0, 80)) {
                      const n = o.n;
                      if (n.classList.contains('swiper-slide-duplicate') || n.classList.contains('slick-cloned')) continue;

                      const a = n.querySelector('a[href]');
                      const href = a ? a.getAttribute('href') : '';

                      const img = pickSrc(n);
                      const alt = n.querySelector('img')?.getAttribute('alt') || '';
                      const txt = norm(
                        n.querySelector('h1,h2,h3,strong,p,.title,.tit,.txt,.copy')?.innerText
                        || (a ? a.innerText : '')
                        || ''
                      );

                      let idx = 9999;
                      const d1 = n.getAttribute('data-swiper-slide-index');
                      const d2 = n.getAttribute('data-slick-index');
                      if (d1 && String(d1).trim().match(/^\d+$/)) idx = parseInt(d1.trim(), 10);
                      else if (d2 && String(d2).trim().match(/^\d+$/)) idx = parseInt(d2.trim(), 10);

                      out.push({idx, href, img, alt, txt});
                    }
                  }
                  return out;
                }
                """,
                700
            )
        except Exception:
            candidates = []

        cleaned = []
        for it in candidates or []:
            href = abs_url(base_url, (it.get("href") or "").strip())
            img_url = abs_url(base_url, (it.get("img") or "").strip())
            if not img_url:
                continue
            cleaned.append((
                int(it.get("idx") if str(it.get("idx","")).isdigit() else 9999),
                href,
                img_url,
                norm_ws(it.get("alt") or ""),
                norm_ws(it.get("txt") or "")
            ))

        cleaned.sort(key=lambda x: x[0])

        seen = set()
        rank = 1
        for idx, href, img_url, alt, txt in cleaned:
            if rank > max_items:
                break
            key = (normalize_href(href), normalize_img_url(img_url))
            if key in seen:
                continue
            seen.add(key)

            title = choose_title(txt, alt)
            img_local, st = save_and_resize_image(context, img_url, brand_key, rank, referer=base_url)

            b = build_banner(date_s, brand_key, brand_name, rank, title, href, img_url, img_local, st,
                             extract_source="hero_slider_container", confidence_score=0.88,
                             is_fallback=False, is_primary_hero=(rank == 1))
            out.append(b)
            rank += 1

        if out:
            break

    return out[:max_items]

def _hero_slider_broad(context, page, base_url: str, brand_key: str, brand_name: str, date_s: str, max_items: int) -> List[Banner]:
    """
    기존 broad slide 스캔(컨테이너 방식이 실패했을 때만 사용)
    """
    out: List[Banner] = []
    close_common_popups(page)

    try:
        page.evaluate("window.scrollTo(0, 200);")
        page.wait_for_timeout(250)
        page.evaluate("window.scrollTo(0, 700);")
        page.wait_for_timeout(300)
        page.evaluate("window.scrollTo(0, 0);")
        page.wait_for_timeout(250)
    except Exception:
        pass

    candidates = []
    try:
        candidates = page.evaluate(
            r"""
            () => {
              const norm = (s) => (s||'').replace(/\s+/g,' ').trim();
              const pickSrc = (root) => {
                const source = root.querySelector('source[srcset]');
                if (source) {
                  const ss = source.getAttribute('srcset') || '';
                  const first = ss.split(',')[0].trim().split(' ')[0];
                  if (first) return first;
                }
                const img = root.querySelector('img');
                if (img) {
                  return img.getAttribute('src')
                      || img.getAttribute('data-src')
                      || img.getAttribute('data-lazy')
                      || img.getAttribute('data-original')
                      || (img.getAttribute('srcset') ? (img.getAttribute('srcset').split(',')[0].trim().split(' ')[0]) : '')
                      || '';
                }
                const bg = root.querySelector('[style*="background-image"]');
                if (bg) {
                  const m = (bg.getAttribute('style')||'').match(/url\(([^)]+)\)/i);
                  if (m && m[1]) return m[1].replace(/["']/g,'');
                }
                return '';
              };

              const slideSelectors = [
                '.swiper-slide',
                '.slick-slide',
                '[data-swiper-slide-index]',
                '[data-slick-index]',
                '.carousel-item',
              ];

              const nodes = [];
              slideSelectors.forEach(sel => {
                document.querySelectorAll(sel).forEach(n => nodes.push(n));
              });

              const withBox = nodes.map(n => {
                const r = n.getBoundingClientRect();
                return {n, y: r.y, w: r.width, h: r.height};
              }).filter(o => o.w > 400 && o.h > 160 && o.y < 1200);

              withBox.sort((a,b) => a.y - b.y);

              const out = [];
              for (const o of withBox.slice(0, 120)) {
                const n = o.n;
                if (n.classList.contains('swiper-slide-duplicate') || n.classList.contains('slick-cloned')) continue;

                const a = n.querySelector('a[href]');
                const href = a ? a.getAttribute('href') : '';

                const img = pickSrc(n);
                const alt = n.querySelector('img')?.getAttribute('alt') || '';
                const txt = norm(
                  n.querySelector('h1,h2,h3,strong,p,.title,.tit,.txt,.copy')?.innerText
                  || (a ? a.innerText : '')
                  || ''
                );

                let idx = 9999;
                const d1 = n.getAttribute('data-swiper-slide-index');
                const d2 = n.getAttribute('data-slick-index');
                if (d1 && String(d1).trim().match(/^\d+$/)) idx = parseInt(d1.trim(), 10);
                else if (d2 && String(d2).trim().match(/^\d+$/)) idx = parseInt(d2.trim(), 10);

                out.push({idx, href, img, alt, txt});
              }
              return out;
            }
            """
        )
    except Exception:
        candidates = []

    cleaned = []
    for it in candidates or []:
        href = abs_url(base_url, (it.get("href") or "").strip())
        img_url = abs_url(base_url, (it.get("img") or "").strip())
        if not img_url:
            continue
        cleaned.append((
            int(it.get("idx") if str(it.get("idx","")).isdigit() else 9999),
            href,
            img_url,
            norm_ws(it.get("alt") or ""),
            norm_ws(it.get("txt") or "")
        ))

    cleaned.sort(key=lambda x: x[0])

    seen = set()
    rank = 1
    for idx, href, img_url, alt, txt in cleaned:
        if rank > max_items:
            break
        key = (normalize_href(href), normalize_img_url(img_url))
        if key in seen:
            continue
        seen.add(key)

        title = choose_title(txt, alt)
        img_local, st = save_and_resize_image(context, img_url, brand_key, rank, referer=base_url)

        b = build_banner(date_s, brand_key, brand_name, rank, title, href, img_url, img_local, st,
                         extract_source="hero_slider_broad", confidence_score=0.72,
                         is_fallback=True, is_primary_hero=(rank == 1))
        out.append(b)
        rank += 1

    return out

def hero_slider(context, page, base_url: str, brand_key: str, brand_name: str, date_s: str, max_items: int):
    """
    1) 컨테이너 우선(정확도↑, 오탐↓)
    2) broad slide 스캔
    3) generic fallback
    """
    close_common_popups(page)

    out = _hero_slider_from_container(context, page, base_url, brand_key, brand_name, date_s, max_items)
    out = dedupe_brand_rows(out)[:max_items]
    if len(out) >= max_items:
        return out[:max_items]

    more = _hero_slider_broad(context, page, base_url, brand_key, brand_name, date_s, max_items)
    merged = dedupe_brand_rows(out + more)[:max_items]
    if len(merged) >= max_items:
        return merged[:max_items]

    remain = max_items - len(merged)
    fb = generic_top_banners(context, page, base_url, brand_key, brand_name, date_s, remain, y_max=1600)
    merged2 = dedupe_brand_rows(merged + fb)[:max_items]
    return merged2


# =====================================================
# Output: CSV/HTML + Changes(diff)
# =====================================================
def write_csv(path: str, rows: List[Banner]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow([
            "date","brand_key","brand_name","rank",
            "title","href","href_clean",
            "plan_start","plan_end",
            "img_url","img_local","img_status",
            "img_w","img_h","img_bytes",
            "img_signature","brand_resolved_url",
            "extract_source","confidence_score","is_fallback","is_primary_hero",
            "landing_title","landing_subtitle","landing_summary",
            "landing_keywords","landing_category","landing_text_status",
        ])
        for b in rows:
            w.writerow([
                b.date,b.brand_key,b.brand_name,b.rank,
                b.title,b.href,b.href_clean,
                b.plan_start,b.plan_end,
                b.img_url,b.img_local,b.img_status,
                b.img_w,b.img_h,b.img_bytes,
                b.img_signature,b.brand_resolved_url,
                b.extract_source,b.confidence_score,int(bool(b.is_fallback)),int(bool(b.is_primary_hero)),
                b.landing_title,b.landing_subtitle,b.landing_summary,
                b.landing_keywords,b.landing_category,b.landing_text_status,
            ])

def _read_csv_rows(path: str) -> List[Dict[str, str]]:
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            rd = csv.DictReader(f)
            return [r for r in rd]
    except Exception:
        return []

def _latest_prev_snapshot(date_s: str) -> str:
    """
    snapshots/hero_main_banners_YYYY-MM-DD.csv 중에서 date_s 이전의 가장 최신 파일 찾기
    """
    try:
        if not os.path.isdir(SNAP_DIR):
            return ""
        files = []
        for fn in os.listdir(SNAP_DIR):
            if fn.startswith("hero_main_banners_") and fn.endswith(".csv"):
                files.append(fn)
        files.sort()
        cur = f"hero_main_banners_{date_s}.csv"
        prev = ""
        for fn in files:
            if fn < cur:
                prev = fn
            else:
                break
        return os.path.join(SNAP_DIR, prev) if prev else ""
    except Exception:
        return ""

def build_changes(prev_csv: str, curr_csv: str) -> Dict[str, Any]:
    """
    diff 기준:
    - 키: brand_key + rank
    - 변경: title/href_clean/img_url/img_local/img_status/plan_start/plan_end
    - 추가/삭제도 계산
    """
    prev = _read_csv_rows(prev_csv)
    curr = _read_csv_rows(curr_csv)

    def key(r): return f"{r.get('brand_key','')}|{r.get('rank','')}"
    prev_map = {key(r): r for r in prev}
    curr_map = {key(r): r for r in curr}

    added = []
    removed = []
    changed = []

    fields = ["title","href_clean","img_url","img_local","img_status","plan_start","plan_end"]

    for k, r in curr_map.items():
        if k not in prev_map:
            added.append(r)
            continue
        p = prev_map[k]
        diffs = {}
        for f in fields:
            if (p.get(f,"") or "") != (r.get(f,"") or ""):
                diffs[f] = {"prev": p.get(f,"") or "", "curr": r.get(f,"") or ""}
        if diffs:
            changed.append({"key": k, "brand_key": r.get("brand_key",""), "rank": r.get("rank",""),
                            "diffs": diffs, "curr": r})

    for k, r in prev_map.items():
        if k not in curr_map:
            removed.append(r)

    # 브랜드별 집계
    by_brand = {}
    for r in added:
        by_brand.setdefault(r.get("brand_key",""), {"added":0,"removed":0,"changed":0})
        by_brand[r.get("brand_key","")]["added"] += 1
    for r in removed:
        by_brand.setdefault(r.get("brand_key",""), {"added":0,"removed":0,"changed":0})
        by_brand[r.get("brand_key","")]["removed"] += 1
    for x in changed:
        bk = x.get("brand_key","")
        by_brand.setdefault(bk, {"added":0,"removed":0,"changed":0})
        by_brand[bk]["changed"] += 1

    return {
        "prev_csv": os.path.basename(prev_csv) if prev_csv else "",
        "curr_csv": os.path.basename(curr_csv) if curr_csv else "",
        "summary": {
            "added": len(added),
            "removed": len(removed),
            "changed": len(changed),
            "brands_touched": len([k for k,v in by_brand.items() if sum(v.values())>0]),
        },
        "by_brand": by_brand,
        "added": added[:200],
        "removed": removed[:200],
        "changed": changed[:200],
    }


def _banner_identity_candidates(row: Dict[str, str]) -> List[str]:
    keys: List[str] = []
    href = normalize_href(row.get("href_clean", "") or row.get("href", ""))
    title = norm_ws((row.get("landing_title", "") or row.get("title", "") or "")).lower()
    subtitle = norm_ws(row.get("landing_subtitle", "") or "").lower()
    img_sig = (row.get("img_signature", "") or "").strip().lower()
    img_url = normalize_img_url(row.get("img_url", "") or "").lower()

    if href:
        keys.append(f"href:{href}")
    if img_sig:
        keys.append(f"imgsig:{img_sig}")
    if href and title:
        keys.append(f"href_title:{sha1(href + '|' + title)}")
    if title and img_sig:
        keys.append(f"title_sig:{sha1(title + '|' + img_sig)}")
    if title and img_url:
        keys.append(f"title_img:{sha1(title + '|' + img_url)}")
    if title:
        keys.append(f"title:{sha1(title)}")
    if subtitle:
        keys.append(f"subtitle:{sha1(subtitle)}")
    if img_url:
        keys.append(f"img:{img_url}")
    return keys


def _change_subtypes(prev_row: Dict[str, str], curr_row: Dict[str, str], diffs: Dict[str, Dict[str, str]]) -> List[str]:
    subtypes: List[str] = []
    if (prev_row.get("rank", "") or "") != (curr_row.get("rank", "") or ""):
        subtypes.append("rank_shift")
    if any(k in diffs for k in {"img_url", "img_local", "img_signature"}):
        subtypes.append("image_changed")
    if "href_clean" in diffs:
        subtypes.append("link_changed")
    if any(k in diffs for k in {"landing_title", "landing_subtitle", "landing_summary", "landing_keywords", "landing_category"}):
        subtypes.append("landing_text_changed")
    if "title" in diffs:
        subtypes.append("title_changed")
    if any(k in diffs for k in {"plan_start", "plan_end"}):
        subtypes.append("date_changed")
    if any(k in diffs for k in {"extract_source", "confidence_score", "is_fallback", "is_primary_hero"}):
        subtypes.append("extractor_changed")
    if not subtypes and diffs:
        subtypes.append("content_changed")
    return subtypes


def _primary_subtype(subtypes: List[str]) -> str:
    priority = [
        "image_changed", "link_changed", "landing_text_changed", "title_changed",
        "date_changed", "rank_shift", "extractor_changed", "content_changed",
    ]
    for item in priority:
        if item in subtypes:
            return item
    return subtypes[0] if subtypes else ""


def build_changes(prev_csv: str, curr_csv: str) -> Dict[str, Any]:
    prev = _read_csv_rows(prev_csv)
    curr = _read_csv_rows(curr_csv)

    prev_by_brand: Dict[str, List[Dict[str, str]]] = {}
    curr_by_brand: Dict[str, List[Dict[str, str]]] = {}
    for row in prev:
        prev_by_brand.setdefault(row.get("brand_key", ""), []).append(row)
    for row in curr:
        curr_by_brand.setdefault(row.get("brand_key", ""), []).append(row)

    added: List[Dict[str, str]] = []
    removed: List[Dict[str, str]] = []
    changed: List[Dict[str, Any]] = []
    by_brand: Dict[str, Dict[str, Any]] = {}
    subtype_counts: Counter = Counter()
    compare_fields = [
        "rank", "title", "href_clean", "img_url", "img_local", "img_status", "img_signature",
        "plan_start", "plan_end", "extract_source", "confidence_score", "is_fallback", "is_primary_hero",
        "landing_title", "landing_subtitle", "landing_summary", "landing_keywords", "landing_category", "landing_text_status",
    ]

    for bk in sorted(set(prev_by_brand.keys()) | set(curr_by_brand.keys())):
        prev_rows = sorted(prev_by_brand.get(bk, []), key=lambda r: int(r.get("rank", "9999") or 9999))
        curr_rows = sorted(curr_by_brand.get(bk, []), key=lambda r: int(r.get("rank", "9999") or 9999))

        curr_key_map: Dict[str, List[int]] = {}
        for idx, row in enumerate(curr_rows):
            for key in _banner_identity_candidates(row):
                curr_key_map.setdefault(key, []).append(idx)

        used_curr = set()
        matches: List[Tuple[Dict[str, str], Dict[str, str]]] = []
        for prev_row in prev_rows:
            match_idx = None
            for key in _banner_identity_candidates(prev_row):
                for idx in curr_key_map.get(key, []):
                    if idx not in used_curr:
                        match_idx = idx
                        break
                if match_idx is not None:
                    break
            if match_idx is None:
                removed.append(prev_row)
            else:
                used_curr.add(match_idx)
                matches.append((prev_row, curr_rows[match_idx]))

        for idx, row in enumerate(curr_rows):
            if idx not in used_curr:
                added.append(row)

        for prev_row, curr_row in matches:
            diffs = {}
            for field in compare_fields:
                if (prev_row.get(field, "") or "") != (curr_row.get(field, "") or ""):
                    diffs[field] = {"prev": prev_row.get(field, "") or "", "curr": curr_row.get(field, "") or ""}
            if not diffs:
                continue
            subtypes = _change_subtypes(prev_row, curr_row, diffs)
            subtype = _primary_subtype(subtypes)
            changed.append({
                "identity": _banner_identity_candidates(curr_row)[0] if _banner_identity_candidates(curr_row) else f"{bk}|{curr_row.get('rank', '')}",
                "brand_key": bk,
                "rank": curr_row.get("rank", ""),
                "prev_rank": prev_row.get("rank", ""),
                "subtype": subtype,
                "subtypes": subtypes,
                "diffs": diffs,
                "prev": prev_row,
                "curr": curr_row,
            })
            subtype_counts[subtype] += 1
            by_brand.setdefault(bk, {"added": 0, "removed": 0, "changed": 0, "subtypes": {}})
            by_brand[bk]["changed"] += 1
            for item in subtypes:
                by_brand[bk]["subtypes"][item] = int(by_brand[bk]["subtypes"].get(item, 0) or 0) + 1

    for row in added:
        bk = row.get("brand_key", "")
        by_brand.setdefault(bk, {"added": 0, "removed": 0, "changed": 0, "subtypes": {}})
        by_brand[bk]["added"] += 1
    for row in removed:
        bk = row.get("brand_key", "")
        by_brand.setdefault(bk, {"added": 0, "removed": 0, "changed": 0, "subtypes": {}})
        by_brand[bk]["removed"] += 1

    return {
        "prev_csv": os.path.basename(prev_csv) if prev_csv else "",
        "curr_csv": os.path.basename(curr_csv) if curr_csv else "",
        "summary": {
            "added": len(added),
            "removed": len(removed),
            "changed": len(changed),
            "brands_touched": len([k for k, v in by_brand.items() if (v.get("added", 0) + v.get("removed", 0) + v.get("changed", 0)) > 0]),
            "subtypes": dict(subtype_counts),
        },
        "by_brand": by_brand,
        "added": added[:200],
        "removed": removed[:200],
        "changed": changed[:200],
    }


def _snapshot_date_from_path(path: str) -> Optional[datetime]:
    try:
        m = re.search(r"hero_main_banners_(\d{4}-\d{2}-\d{2})\.csv$", os.path.basename(path or ""))
        if not m:
            return None
        return datetime.strptime(m.group(1), "%Y-%m-%d")
    except Exception:
        return None


def _recent_snapshot_files(date_s: str, days: int = 7) -> List[str]:
    try:
        end_dt = datetime.strptime(date_s, "%Y-%m-%d")
    except Exception:
        return []
    start_dt = end_dt - timedelta(days=max(days - 1, 0))
    out = []
    if not os.path.isdir(SNAP_DIR):
        return out
    for fn in sorted(os.listdir(SNAP_DIR)):
        if not fn.startswith("hero_main_banners_") or not fn.endswith(".csv"):
            continue
        dt = _snapshot_date_from_path(fn)
        if not dt:
            continue
        if start_dt <= dt <= end_dt:
            out.append(os.path.join(SNAP_DIR, fn))
    return sorted(out)


def _snapshot_files_in_range(start_dt: datetime, end_dt: datetime, include_prev_baseline: bool = False) -> List[str]:
    if not os.path.isdir(SNAP_DIR):
        return []
    out: List[str] = []
    prev_baseline = ""
    for fn in sorted(os.listdir(SNAP_DIR)):
        if not fn.startswith("hero_main_banners_") or not fn.endswith(".csv"):
            continue
        path = os.path.join(SNAP_DIR, fn)
        dt = _snapshot_date_from_path(path)
        if not dt:
            continue
        if dt < start_dt:
            prev_baseline = path
        elif start_dt <= dt <= end_dt:
            out.append(path)
    if include_prev_baseline and prev_baseline and out:
        out = [prev_baseline] + out
    return out


def _calc_delta_pct(curr: int, prev: int) -> float:
    curr = int(curr or 0)
    prev = int(prev or 0)
    if prev <= 0:
        return 100.0 if curr > 0 else 0.0
    return round(((curr - prev) / prev) * 100.0, 1)


def _aggregate_change_window(files: List[str]) -> Dict[str, Any]:
    by_brand: Dict[str, Dict[str, Any]] = {}
    total_added = total_removed = total_changed = total_events = 0
    pairs: List[Dict[str, Any]] = []
    timeline_days: List[str] = []

    def ensure_brand(bk: str, brand_name: str, next_csv: str) -> Dict[str, Any]:
        return by_brand.setdefault(bk, {
            "brand_key": bk,
            "brand_name": brand_name or bk,
            "added": 0,
            "removed": 0,
            "changed": 0,
            "events": 0,
            "last_title": "",
            "last_rank": "",
            "last_seen_csv": os.path.basename(next_csv) if next_csv else "",
            "recent_titles": [],
            "daily": {},
            "subtypes": {},
        })

    def touch(info: Dict[str, Any], day_key: str, kind: str, title: str, rank: Any, next_csv: str, subtypes: Optional[List[str]] = None):
        daily = info["daily"].setdefault(day_key, {"added": 0, "removed": 0, "changed": 0, "events": 0, "subtypes": {}})
        daily[kind] += 1
        daily["events"] += 1
        info[kind] += 1
        info["events"] += 1
        if title:
            info["last_title"] = title
            if title not in info["recent_titles"]:
                info["recent_titles"].append(title)
                info["recent_titles"] = info["recent_titles"][-6:]
        if rank not in {"", None}:
            info["last_rank"] = rank
        info["last_seen_csv"] = os.path.basename(next_csv) if next_csv else info.get("last_seen_csv", "")
        for subtype in subtypes or []:
            daily["subtypes"][subtype] = int(daily["subtypes"].get(subtype, 0) or 0) + 1
            info["subtypes"][subtype] = int(info["subtypes"].get(subtype, 0) or 0) + 1

    for prev_csv, next_csv in zip(files, files[1:]):
        diff = build_changes(prev_csv, next_csv)
        sm = diff.get("summary") or {}
        total_added += int(sm.get("added", 0) or 0)
        total_removed += int(sm.get("removed", 0) or 0)
        total_changed += int(sm.get("changed", 0) or 0)
        total_events += int(sm.get("added", 0) or 0) + int(sm.get("removed", 0) or 0) + int(sm.get("changed", 0) or 0)
        day_dt = _snapshot_date_from_path(next_csv)
        day_key = day_dt.strftime("%Y-%m-%d") if day_dt else os.path.basename(next_csv)
        timeline_days.append(day_key)
        pairs.append({
            "date": day_key,
            "prev_csv": os.path.basename(prev_csv),
            "curr_csv": os.path.basename(next_csv),
            "summary": sm,
        })

        for r in diff.get("added", []) or []:
            bk = r.get("brand_key", "")
            if not bk:
                continue
            touch(ensure_brand(bk, r.get("brand_name", bk), next_csv), day_key, "added", r.get("title", "") or "", r.get("rank", ""), next_csv)

        for r in diff.get("removed", []) or []:
            bk = r.get("brand_key", "")
            if not bk:
                continue
            touch(ensure_brand(bk, r.get("brand_name", bk), next_csv), day_key, "removed", r.get("title", "") or "", r.get("rank", ""), next_csv)

        for x in diff.get("changed", []) or []:
            curr = x.get("curr") or {}
            bk = curr.get("brand_key", "")
            if not bk:
                continue
            touch(
                ensure_brand(bk, curr.get("brand_name", bk), next_csv),
                day_key,
                "changed",
                curr.get("landing_title", "") or curr.get("title", "") or "",
                curr.get("rank", ""),
                next_csv,
                x.get("subtypes") or ([x.get("subtype")] if x.get("subtype") else []),
            )

    brands = sorted(
        by_brand.values(),
        key=lambda x: (-int(x.get("events", 0) or 0), -int(x.get("changed", 0) or 0), x.get("brand_name", "")),
    )
    for info in brands:
        info["max_daily_events"] = max([int(v.get("events", 0) or 0) for v in (info.get("daily") or {}).values()] or [0])

    return {
        "summary": {
            "brands_touched": len(brands),
            "events": total_events,
            "added": total_added,
            "removed": total_removed,
            "changed": total_changed,
            "subtypes": dict(Counter({k: sum(int((b.get('subtypes') or {}).get(k, 0) or 0) for b in brands) for k in set().union(*[set((b.get('subtypes') or {}).keys()) for b in brands])})) if brands else {},
        },
        "brands": brands[:100],
        "pairs": pairs[: max(len(pairs), 1)],
        "timeline_days": sorted(set(timeline_days)),
    }


def build_recent_changes(curr_csv: str, date_s: str, days: int = RECENT_CHANGE_DAYS) -> Dict[str, Any]:
    """
    최근 N일 내 snapshot 변화 이력 요약.
    - 브랜드 중복은 하나로 묶음
    - 연속 스냅샷 pair 를 훑어서 최근 7일 내 변경 이력 집계
    """
    try:
        end_dt = datetime.strptime(date_s, "%Y-%m-%d")
    except Exception:
        end_dt = kst_now()
    start_dt = end_dt - timedelta(days=max(days - 1, 0))
    prev_end_dt = start_dt - timedelta(days=1)
    prev_start_dt = prev_end_dt - timedelta(days=max(days - 1, 0))

    files = _snapshot_files_in_range(start_dt, end_dt, include_prev_baseline=True)
    if curr_csv and os.path.exists(curr_csv) and curr_csv not in files:
        files.append(curr_csv)
        files = sorted(set(files))

    current = _aggregate_change_window(files)
    previous = _aggregate_change_window(_snapshot_files_in_range(prev_start_dt, prev_end_dt, include_prev_baseline=True))
    prev_brand_map = {x.get("brand_key", ""): x for x in previous.get("brands", []) or []}

    for info in current.get("brands", []) or []:
        prev_info = prev_brand_map.get(info.get("brand_key", ""), {})
        prev_events = int(prev_info.get("events", 0) or 0)
        prev_changed = int(prev_info.get("changed", 0) or 0)
        info["prev_events"] = prev_events
        info["prev_changed"] = prev_changed
        info["event_delta_pct"] = _calc_delta_pct(int(info.get("events", 0) or 0), prev_events)
        info["changed_delta_pct"] = _calc_delta_pct(int(info.get("changed", 0) or 0), prev_changed)

    current_summary = current.get("summary") or {}
    prev_summary = previous.get("summary") or {}
    return {
        "window_days": int(days),
        "curr_csv": os.path.basename(curr_csv) if curr_csv else "",
        "summary": {
            "brands_touched": int(current_summary.get("brands_touched", 0) or 0),
            "events": int(current_summary.get("events", 0) or 0),
            "added": int(current_summary.get("added", 0) or 0),
            "removed": int(current_summary.get("removed", 0) or 0),
            "changed": int(current_summary.get("changed", 0) or 0),
            "prev_events": int(prev_summary.get("events", 0) or 0),
            "prev_changed": int(prev_summary.get("changed", 0) or 0),
            "event_delta_pct": _calc_delta_pct(int(current_summary.get("events", 0) or 0), int(prev_summary.get("events", 0) or 0)),
            "changed_delta_pct": _calc_delta_pct(int(current_summary.get("changed", 0) or 0), int(prev_summary.get("changed", 0) or 0)),
        },
        "brands": current.get("brands", [])[:100],
        "pairs": current.get("pairs", [])[: max(len(current.get("pairs", []) or []), 1)],
        "timeline_days": current.get("timeline_days", []),
        "previous_window": {
            "start_date": prev_start_dt.strftime("%Y-%m-%d"),
            "end_date": prev_end_dt.strftime("%Y-%m-%d"),
            "summary": prev_summary,
        },
    }


def build_alerts(changes: Optional[Dict[str, Any]], event_threshold: int = ALERT_EVENT_THRESHOLD, changed_threshold: int = ALERT_CHANGED_THRESHOLD) -> Dict[str, Any]:
    alerts: List[Dict[str, Any]] = []
    if not isinstance(changes, dict):
        return {
            "count": 0,
            "items": [],
            "thresholds": {"events": int(event_threshold), "changed": int(changed_threshold)},
        }

    for info in changes.get("brands", []) or []:
        brand_name = info.get("brand_name") or info.get("brand_key") or "-"
        for day_key, daily in (info.get("daily") or {}).items():
            events = int(daily.get("events", 0) or 0)
            changed = int(daily.get("changed", 0) or 0)
            if events < event_threshold and changed < changed_threshold:
                continue
            alerts.append({
                "brand_key": info.get("brand_key", ""),
                "brand_name": brand_name,
                "date": day_key,
                "events": events,
                "changed": changed,
                "added": int(daily.get("added", 0) or 0),
                "removed": int(daily.get("removed", 0) or 0),
                "last_title": info.get("last_title", ""),
            })

    alerts = sorted(alerts, key=lambda x: (-x.get("changed", 0), -x.get("events", 0), x.get("brand_name", ""), x.get("date", "")))
    return {
        "count": len(alerts),
        "items": alerts[:50],
        "thresholds": {"events": int(event_threshold), "changed": int(changed_threshold)},
    }


def _alert_message_lines(alerts_payload: Dict[str, Any]) -> List[str]:
    lines = ["[Hero Banner Alert] 이례적 배너 변동 감지"]
    for item in alerts_payload.get("items", [])[:10]:
        lines.append(
            f"- {item.get('date','-')} | {item.get('brand_name','-')} | events {item.get('events',0)} / changed {item.get('changed',0)} / added {item.get('added',0)} / removed {item.get('removed',0)}"
        )
    return lines


def send_slack_alerts(alerts_payload: Dict[str, Any]) -> str:
    if not SLACK_WEBHOOK_URL or not alerts_payload.get("items"):
        return "skipped"
    try:
        text = "\n".join(_alert_message_lines(alerts_payload))
        resp = requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=8)
        return f"sent:{resp.status_code}" if resp.ok else f"failed:{resp.status_code}"
    except Exception as e:
        return f"error:{e}"


def send_email_alerts(alerts_payload: Dict[str, Any]) -> str:
    if not all([ALERT_EMAIL_TO, ALERT_EMAIL_FROM, ALERT_SMTP_HOST]) or not alerts_payload.get("items"):
        return "skipped"
    try:
        msg = EmailMessage()
        msg["Subject"] = "[Hero Banner Alert] 이례적 배너 변동 감지"
        msg["From"] = ALERT_EMAIL_FROM
        msg["To"] = ALERT_EMAIL_TO
        msg.set_content("\n".join(_alert_message_lines(alerts_payload)))
        with smtplib.SMTP(ALERT_SMTP_HOST, ALERT_SMTP_PORT, timeout=10) as smtp:
            smtp.starttls()
            if ALERT_SMTP_USER:
                smtp.login(ALERT_SMTP_USER, ALERT_SMTP_PASSWORD)
            smtp.send_message(msg)
        return "sent"
    except Exception as e:
        return f"error:{e}"


def analyze_title_keywords(rows: List[Banner], top_n: int = 12) -> List[Dict[str, Any]]:
    counter: Counter = Counter()
    for b in rows:
        text = norm_ws(b.title or "")
        if not text:
            continue
        english_tokens = re.findall(r"[A-Za-z][A-Za-z0-9'\-/&]{2,}", text)
        korean_tokens = re.findall(r"[가-힣]{2,}", text)
        for raw in english_tokens + korean_tokens:
            token = raw.strip("-_/ ").upper() if re.search(r"[A-Za-z]", raw) else raw
            if len(token) < 2:
                continue
            if token.lower() in KEYWORD_STOPWORDS or token in KEYWORD_STOPWORDS:
                continue
            counter[token] += 1
    return [{"keyword": k, "count": v} for k, v in counter.most_common(top_n)]


def _banner_asset_path(banner: Banner) -> str:
    if not banner.img_local:
        return ""
    return os.path.join(ASSET_DIR, banner.img_local)


def _extract_ocr_text(image_path: str) -> str:
    if not (OCR_OK and PIL_OK) or not image_path or not os.path.exists(image_path):
        return ""
    try:
        img = Image.open(image_path)
        img = ImageOps.autocontrast(img.convert("L"))
        txt = pytesseract.image_to_string(img, lang="kor+eng", timeout=3)
        return norm_ws(txt)[:160]
    except Exception:
        return ""


def analyze_visual_identity(banner: Optional[Banner]) -> Dict[str, Any]:
    if not banner:
        return {"tags": [], "ocr_text": "", "summary": "대표 비주얼이 아직 없습니다."}
    source_parts = [banner.title or "", banner.href_clean or "", banner.href or "", banner.img_url or ""]
    ocr_text = _extract_ocr_text(_banner_asset_path(banner))
    if ocr_text:
        source_parts.append(ocr_text)
    source_text = " ".join(source_parts).lower()
    tags: List[str] = []
    for label, patterns in VISUAL_TAG_RULES:
        for pat in patterns:
            if re.search(pat, source_text, re.I):
                tags.append(label)
                break
    deduped_tags: List[str] = []
    for tag in tags:
        if tag not in deduped_tags:
            deduped_tags.append(tag)
    summary = ", ".join(deduped_tags[:4]) if deduped_tags else "태그 자동 분류 데이터 부족"
    return {"tags": deduped_tags[:6], "ocr_text": ocr_text, "summary": summary}


def summarize_brand_insight(brand_key: str, items: List[Banner], changes_map: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    brand_change = changes_map.get(brand_key, {}) if isinstance(changes_map, dict) else {}
    top_item = items[0] if items else None
    visual = analyze_visual_identity(top_item)
    keywords = analyze_title_keywords(items, top_n=6)
    seg = BRAND_SEGMENTS.get(brand_key, {"style": "기타", "origin": "기타"})
    return {
        "segment_style": seg.get("style", "기타"),
        "segment_origin": seg.get("origin", "기타"),
        "events": int(brand_change.get("events", 0) or 0),
        "changed": int(brand_change.get("changed", 0) or 0),
        "added": int(brand_change.get("added", 0) or 0),
        "removed": int(brand_change.get("removed", 0) or 0),
        "event_delta_pct": float(brand_change.get("event_delta_pct", 0.0) or 0.0),
        "changed_delta_pct": float(brand_change.get("changed_delta_pct", 0.0) or 0.0),
        "keywords": keywords,
        "visual": visual,
        "recent_titles": (brand_change.get("recent_titles") or [])[:4],
        "last_rank": brand_change.get("last_rank", ""),
        "last_title": brand_change.get("last_title", ""),
    }


def _h(v: Any) -> str:
    return html.escape(str(v or ""))


def _html_pct_chip(pct: float) -> str:
    pct = float(pct or 0.0)
    if pct > 0.1:
        arrow, cls = "▲", "text-rose-700 bg-rose-50 border-rose-200"
    elif pct < -0.1:
        arrow, cls = "▼", "text-sky-700 bg-sky-50 border-sky-200"
    else:
        arrow, cls = "■", "text-slate-600 bg-slate-50 border-slate-200"
    return f'<span class="inline-flex items-center gap-1 rounded-full border px-2.5 py-1 text-[11px] font-semibold {cls}">{arrow} {abs(pct):.1f}% vs 전주</span>'


def _html_img_src(banner: Banner) -> str:
    if banner.img_local:
        local_path = os.path.join(ASSET_DIR, banner.img_local)
        return to_file_url(local_path) if HTML_USE_ABSOLUTE_FILE_URL else f"assets/{banner.img_local}"
    return banner.img_url or ""


def _html_date_txt(banner: Banner) -> str:
    if banner.plan_start and banner.plan_end:
        return f"{banner.plan_start} ~ {banner.plan_end}"
    if banner.plan_start:
        return banner.plan_start
    return "기간 정보 없음"


def _html_meta_txt(banner: Banner) -> str:
    if banner.img_w and banner.img_h:
        txt = f"{banner.img_w} x {banner.img_h}"
        if banner.img_bytes:
            txt += f" · {int(banner.img_bytes / 1024):,}KB"
        return txt
    if banner.img_status and banner.img_status != "ok":
        return banner.img_status
    return "메타 정보 없음"


def _render_secondary_card(banner: Banner) -> str:
    href = _h(banner.href_clean or banner.href or "#")
    img_url_btn = _h(banner.img_url or _html_img_src(banner) or "#")
    img_src = _h(_html_img_src(banner))
    title = _h(banner.title or "-")
    return f"""
    <article class="soft-panel overflow-hidden flex flex-col">
      <div class="relative aspect-[16/9] bg-slate-100">
        <img src="{img_src}" class="w-full h-full object-cover"
             onerror="this.onerror=null; this.src='https://placehold.co/640x360?text=No+Image';">
        <span class="rank-chip">RANK {int(banner.rank or 0)}</span>
      </div>
      <div class="p-5 flex flex-col gap-3 flex-1">
        <div>
          <div class="text-[11px] font-semibold text-slate-500">{_h(_html_date_txt(banner))}</div>
          <h4 class="mt-2 text-sm font-bold text-slate-900 clamp-2">"{title}"</h4>
          <p class="mt-2 text-[11px] text-slate-500">{_h(_html_meta_txt(banner))}</p>
        </div>
        <div class="mt-auto flex gap-2">
          <a href="{href}" target="_blank" class="btn-primary flex-1 text-center text-[11px]">기획전 보기</a>
          <a href="{img_url_btn}" target="_blank" class="btn-secondary text-[11px]">원본 이미지</a>
        </div>
      </div>
    </article>
    """


def analyze_title_keywords(rows: List[Banner], top_n: int = 12) -> List[Dict[str, Any]]:
    counter: Counter = Counter()
    for b in rows:
        text = " ".join([
            norm_ws(b.landing_title or ""),
            norm_ws(b.landing_subtitle or ""),
            norm_ws(b.landing_summary or ""),
            norm_ws(b.landing_keywords or ""),
            norm_ws(b.title or ""),
        ]).strip()
        if not text:
            continue
        english_tokens = re.findall(r"[A-Za-z][A-Za-z0-9'\-/&]{2,}", text)
        korean_tokens = re.findall(r"[가-힣]{2,}", text)
        for raw in english_tokens + korean_tokens:
            token = raw.strip("-_/ ").upper() if re.search(r"[A-Za-z]", raw) else raw
            if len(token) < 2:
                continue
            if token.lower() in KEYWORD_STOPWORDS or token in KEYWORD_STOPWORDS:
                continue
            counter[token] += 1
    return [{"keyword": k, "count": v} for k, v in counter.most_common(top_n)]


def _normalize_dashboard_keyword(token: Any) -> str:
    txt = norm_ws(str(token or ""))
    if not txt:
        return ""
    return txt.upper() if re.search(r"[A-Za-z]", txt) else txt


def filter_dashboard_keywords(items: List[Dict[str, Any]], top_n: int = 14) -> List[Dict[str, Any]]:
    filtered: List[Dict[str, Any]] = []
    for item in items or []:
        keyword = _normalize_dashboard_keyword(item.get("keyword", ""))
        if not keyword or keyword in DASHBOARD_KEYWORD_EXCLUDES:
            continue
        filtered.append({
            "keyword": keyword,
            "count": int(item.get("count", 0) or 0),
        })
        if len(filtered) >= top_n:
            break
    return filtered


def _extract_ocr_text(image_path: str) -> str:
    if not (ENABLE_OCR and OCR_OK and PIL_OK) or not image_path or not os.path.exists(image_path):
        return ""
    try:
        img = Image.open(image_path)
        img = ImageOps.autocontrast(img.convert("L"))
        txt = pytesseract.image_to_string(img, lang="kor+eng", timeout=3)
        return norm_ws(txt)[:160]
    except Exception:
        return ""


def analyze_visual_identity(banner: Optional[Banner]) -> Dict[str, Any]:
    if not banner:
        return {"tags": [], "ocr_text": "", "summary": "대표 비주얼이 아직 없습니다."}
    source_parts = [
        banner.landing_title or "",
        banner.landing_subtitle or "",
        banner.landing_summary or "",
        banner.landing_keywords or "",
        banner.title or "",
        banner.href_clean or "",
        banner.href or "",
        banner.img_url or "",
    ]
    ocr_text = _extract_ocr_text(_banner_asset_path(banner))
    if ocr_text:
        source_parts.append(ocr_text)
    source_text = " ".join(source_parts).lower()
    tags: List[str] = []
    for label, patterns in VISUAL_TAG_RULES:
        for pat in patterns:
            if re.search(pat, source_text, re.I):
                tags.append(label)
                break
    deduped_tags: List[str] = []
    for tag in tags:
        if tag not in deduped_tags:
            deduped_tags.append(tag)
    summary = ", ".join(deduped_tags[:4]) if deduped_tags else "태그 자동 분류 데이터 부족"
    return {"tags": deduped_tags[:6], "ocr_text": ocr_text, "summary": summary}


def summarize_brand_insight(brand_key: str, items: List[Banner], changes_map: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    brand_change = changes_map.get(brand_key, {}) if isinstance(changes_map, dict) else {}
    top_item = items[0] if items else None
    visual = analyze_visual_identity(top_item)
    keywords = analyze_title_keywords(items, top_n=6)
    seg = BRAND_SEGMENTS.get(brand_key, {"style": "기타", "origin": "기타"})
    return {
        "segment_style": seg.get("style", "기타"),
        "segment_origin": seg.get("origin", "기타"),
        "events": int(brand_change.get("events", 0) or 0),
        "changed": int(brand_change.get("changed", 0) or 0),
        "added": int(brand_change.get("added", 0) or 0),
        "removed": int(brand_change.get("removed", 0) or 0),
        "event_delta_pct": float(brand_change.get("event_delta_pct", 0.0) or 0.0),
        "changed_delta_pct": float(brand_change.get("changed_delta_pct", 0.0) or 0.0),
        "keywords": keywords,
        "visual": visual,
        "recent_titles": (brand_change.get("recent_titles") or [])[:4],
        "last_rank": brand_change.get("last_rank", ""),
        "last_title": brand_change.get("last_title", ""),
        "subtypes": brand_change.get("subtypes", {}) or {},
    }


def _display_title(banner: Banner) -> str:
    return banner.landing_title or banner.landing_subtitle or banner.title or "-"


def _display_subtitle(banner: Banner) -> str:
    current = norm_ws(_display_title(banner))
    for value in [banner.title, banner.landing_subtitle]:
        txt = norm_ws(value or "")
        if txt and txt != current:
            return txt
    return ""


def _display_summary(banner: Banner) -> str:
    if banner.landing_summary:
        return banner.landing_summary
    subtitle = _display_subtitle(banner)
    if subtitle:
        return subtitle
    return banner.title or "설명 없음"


def _keyword_chip_list(banner: Banner) -> List[str]:
    chips = []
    for raw in (banner.landing_keywords or "").split(","):
        txt = norm_ws(raw)
        if txt and txt not in chips:
            chips.append(txt)
    return chips[:6]


def _html_date_txt(banner: Banner) -> str:
    if banner.plan_start and banner.plan_end:
        return f"{banner.plan_start} ~ {banner.plan_end}"
    if banner.plan_start:
        return banner.plan_start
    return "기간 정보 없음"


def _html_meta_txt(banner: Banner) -> str:
    if banner.img_w and banner.img_h:
        txt = f"{banner.img_w} x {banner.img_h}"
        if banner.img_bytes:
            txt += f" · {int(banner.img_bytes / 1024):,}KB"
        return txt
    if banner.img_status and banner.img_status != "ok":
        return banner.img_status
    return "메타 정보 없음"


def _banner_status_meta(banner: Banner) -> str:
    bits = []
    if banner.extract_source:
        bits.append(f"src {banner.extract_source}")
    if banner.confidence_score:
        bits.append(f"conf {banner.confidence_score:.2f}")
    if banner.landing_text_status:
        bits.append(f"meta {banner.landing_text_status}")
    if banner.landing_category:
        bits.append(banner.landing_category)
    return " | ".join(bits)


def _render_secondary_card(banner: Banner) -> str:
    href = _h(banner.href_clean or banner.href or "#")
    img_url_btn = _h(banner.img_url or _html_img_src(banner) or "#")
    img_src = _h(_html_img_src(banner))
    title = _h(_display_title(banner))
    subtitle = _display_subtitle(banner)
    summary = _display_summary(banner)
    chips = "".join([f'<span class="mini-stat">{_h(x)}</span>' for x in _keyword_chip_list(banner)]) or f'<span class="mini-stat">{_h(banner.landing_category or banner.extract_source or "meta 없음")}</span>'
    return f"""
    <article class="soft-panel overflow-hidden flex flex-col">
      <div class="relative aspect-[16/9] bg-slate-100">
        <img src="{img_src}" class="w-full h-full object-cover"
             onerror="this.onerror=null; this.src='https://placehold.co/640x360?text=No+Image';">
        <span class="rank-chip">RANK {int(banner.rank or 0)}</span>
      </div>
      <div class="p-5 flex flex-col gap-3 flex-1">
        <div>
          <div class="text-[11px] font-semibold text-slate-500">{_h(_html_date_txt(banner))}</div>
          <h4 class="mt-2 text-sm font-bold text-slate-900 clamp-2">"{title}"</h4>
          <p class="mt-1 text-[12px] text-slate-500 clamp-2">{_h(subtitle or summary)}</p>
          <div class="mt-3 flex flex-wrap gap-2">{chips}</div>
          <p class="mt-2 text-[11px] text-slate-500">{_h(_banner_status_meta(banner) or _html_meta_txt(banner))}</p>
        </div>
        <div class="mt-auto flex gap-2">
          <a href="{href}" target="_blank" class="btn-primary flex-1 text-center text-[11px]">기획전 보기</a>
          <a href="{img_url_btn}" target="_blank" class="btn-secondary text-[11px]">원본 이미지</a>
        </div>
      </div>
    </article>
    """


def _render_heatmap_section(changes: Dict[str, Any]) -> str:
    brands = (changes.get("brands") or [])[:10]
    timeline_days = changes.get("timeline_days") or [x.get("date", "") for x in (changes.get("pairs") or []) if x.get("date")]
    window_days = int(changes.get("window_days", RECENT_CHANGE_DAYS) or RECENT_CHANGE_DAYS)
    if not brands or not timeline_days:
        return """
        <section class="glass-card p-6 lg:p-7">
          <h2 class="section-title">배너 교체 빈도 히트맵</h2>
          <p class="section-sub mt-2">히트맵을 만들기 위한 스냅샷 이력이 아직 충분하지 않습니다.</p>
        </section>
        """

    max_daily = max(
        [int((((brand.get("daily") or {}).get(day, {}) or {}).get("events", 0) or 0)) for brand in brands for day in timeline_days] or [1]
    )
    palette = [
        ("#f8fafc", "#94a3b8"),
        ("#dbeafe", "#1d4ed8"),
        ("#93c5fd", "#1e40af"),
        ("#3b82f6", "#ffffff"),
        ("#1d4ed8", "#ffffff"),
    ]
    head = "".join([f'<div class="heatmap-head-cell">{_h(day[5:])}</div>' for day in timeline_days])
    rows = []
    for brand in brands:
        cells = [f'<div class="heatmap-brand">{_h(brand.get("brand_name", brand.get("brand_key", "-")))}</div>']
        for day in timeline_days:
            val = int((((brand.get("daily") or {}).get(day, {}) or {}).get("events", 0) or 0))
            idx = 0 if val <= 0 else min(4, max(1, round((val / max_daily) * 4)))
            bg, fg = palette[idx]
            cells.append(f'<div class="heatmap-cell" style="background:{bg}; color:{fg};">{val if val > 0 else ""}</div>')
        rows.append('<div class="heatmap-row">' + "".join(cells) + "</div>")
    return f"""
    <section class="glass-card p-6 lg:p-7">
      <div class="flex items-center justify-between gap-3 mb-4">
        <div>
          <h2 class="section-title">배너 교체 빈도 히트맵</h2>
          <p class="section-sub">브랜드별 최근 {window_days}일 변동량을 날짜 단위로 비교합니다.</p>
        </div>
        <div class="text-[11px] text-slate-500">정렬 기준: 최근 이벤트 수</div>
      </div>
      <div class="heatmap-wrap">
        <div class="heatmap-row heatmap-head">
          <div class="heatmap-brand head-label">브랜드</div>
          {head}
        </div>
        {''.join(rows)}
      </div>
      <div class="mt-4 flex flex-wrap gap-2 text-[11px] text-slate-500">
        <span class="legend-chip">0</span>
        <span class="legend-chip legend-mid">1~2</span>
        <span class="legend-chip legend-strong">3+</span>
      </div>
    </section>
    """


def _dashboard_css(day_count: int) -> str:
    heat_cols = max(int(day_count or 0), 1)
    heat_width = 168 + heat_cols * 66
    heat_width_mobile = 132 + heat_cols * 56
    return f"""
    :root {{
      --brand: #123f8c;
      --brand-deep: #0b2b66;
      --bg-top: #f6f8fc;
      --bg-bottom: #e9eff8;
      --line: rgba(148, 163, 184, 0.22);
      --panel: rgba(255, 255, 255, 0.82);
      --soft: rgba(255, 255, 255, 0.72);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background:
        radial-gradient(circle at top left, rgba(18,63,140,0.12), transparent 28%),
        linear-gradient(180deg, var(--bg-top), var(--bg-bottom));
      color: #0f172a;
      font-family: 'Segoe UI', 'Apple SD Gothic Neo', 'Noto Sans KR', sans-serif;
      min-height: 100vh;
    }}
    .glass-card {{ background: var(--panel); border: 1px solid rgba(255,255,255,0.88); border-radius: 30px; box-shadow: 0 24px 60px rgba(15, 23, 42, 0.07); backdrop-filter: blur(14px); }}
    .soft-panel {{ background: var(--soft); border: 1px solid var(--line); border-radius: 24px; box-shadow: 0 10px 28px rgba(148, 163, 184, 0.10); }}
    .metric-card {{ background: rgba(255,255,255,0.76); border: 1px solid rgba(255,255,255,0.9); border-radius: 24px; padding: 20px 22px; box-shadow: 0 12px 32px rgba(18, 63, 140, 0.08); }}
    .metric-label {{ font-size: 12px; font-weight: 700; letter-spacing: 0.08em; text-transform: uppercase; color: #64748b; }}
    .metric-value {{ margin-top: 10px; font-size: 38px; font-weight: 900; letter-spacing: -0.05em; color: #0f172a; }}
    .metric-sub {{ margin-top: 10px; font-size: 12px; color: #475569; }}
    .section-title {{ font-size: 24px; font-weight: 900; letter-spacing: -0.03em; color: #0f172a; }}
    .section-sub {{ margin-top: 6px; font-size: 14px; color: #64748b; }}
    .keyword-chip {{ display: inline-flex; align-items: center; gap: 8px; margin: 0 10px 10px 0; padding: 10px 14px; border-radius: 999px; background: rgba(255,255,255,0.95); border: 1px solid rgba(191, 219, 254, 0.95); color: #123f8c; font-weight: 800; box-shadow: 0 8px 24px rgba(59, 130, 246, 0.12); }}
    .keyword-chip em {{ font-size: 0.78em; font-style: normal; opacity: 0.68; }}
    .heatmap-wrap {{ overflow-x: auto; padding-bottom: 4px; }}
    .heatmap-row {{ display: grid; grid-template-columns: 168px repeat({heat_cols}, minmax(58px, 1fr)); gap: 8px; min-width: {heat_width}px; margin-bottom: 8px; }}
    .heatmap-head {{ margin-bottom: 12px; }}
    .heatmap-head-cell, .heatmap-brand, .heatmap-cell {{ border-radius: 16px; min-height: 54px; display: flex; align-items: center; justify-content: center; padding: 10px; font-size: 12px; font-weight: 700; border: 1px solid rgba(148, 163, 184, 0.12); }}
    .heatmap-brand {{ justify-content: flex-start; padding-left: 16px; background: rgba(255,255,255,0.92); color: #0f172a; }}
    .heatmap-head-cell {{ background: rgba(15,23,42,0.04); color: #475569; }}
    .heatmap-cell {{ box-shadow: inset 0 0 0 1px rgba(255,255,255,0.28); }}
    .head-label {{ font-weight: 900; }}
    .legend-chip {{ display:inline-flex; align-items:center; justify-content:center; min-width:52px; padding: 8px 12px; border-radius: 999px; background:#f8fafc; color:#64748b; font-weight:700; border:1px solid rgba(148,163,184,0.22); }}
    .legend-mid {{ background:#dbeafe; color:#1d4ed8; }}
    .legend-strong {{ background:#1d4ed8; color:#fff; }}
    .filter-btn, .tab-btn {{ display: inline-flex; align-items: center; gap: 8px; padding: 12px 18px; border-radius: 999px; border: 1px solid rgba(148,163,184,0.18); background: rgba(255,255,255,0.78); color: #475569; font-size: 13px; font-weight: 800; transition: all 0.18s ease; }}
    .filter-btn:hover, .tab-btn:hover {{ background: #fff; color: #0f172a; transform: translateY(-1px); }}
    .filter-btn.active, .tab-btn.active {{ background: var(--brand-deep); color: #fff; box-shadow: 0 12px 28px rgba(11,43,102,0.18); }}
    .tab-meta {{ display:inline-flex; align-items:center; justify-content:center; min-width: 24px; height:24px; padding: 0 8px; border-radius:999px; background: rgba(255,255,255,0.18); font-size: 11px; }}
    .rank-chip {{ position:absolute; left:16px; top:16px; display:inline-flex; align-items:center; justify-content:center; padding: 7px 12px; border-radius: 999px; font-size: 11px; font-weight: 900; letter-spacing: 0.04em; background: rgba(15,23,42,0.55); color:#fff; backdrop-filter: blur(8px); }}
    .btn-primary, .btn-secondary {{ display:inline-flex; align-items:center; justify-content:center; padding: 12px 16px; border-radius: 14px; font-weight: 800; text-decoration:none; }}
    .btn-primary {{ background: linear-gradient(135deg, var(--brand-deep), var(--brand)); color:#fff; }}
    .btn-secondary {{ background: #eef2f7; color:#334155; }}
    .mini-card {{ display:flex; flex-direction:column; gap:6px; border-radius:18px; background:#f8fafc; border:1px solid rgba(226,232,240,0.95); padding:14px 15px; }}
    .mini-card span {{ font-size:11px; font-weight:700; color:#64748b; text-transform:uppercase; letter-spacing:0.07em; }}
    .mini-card strong {{ font-size:14px; font-weight:800; color:#0f172a; }}
    .mini-stat {{ display:inline-flex; align-items:center; padding:7px 11px; border-radius:999px; background:#f8fafc; border:1px solid rgba(148,163,184,0.18); font-size:11px; font-weight:800; color:#334155; }}
    .insight-chip {{ display:inline-flex; align-items:center; gap:6px; padding:8px 11px; border-radius:999px; background:#f8fafc; color:#334155; border:1px solid rgba(148,163,184,0.18); font-size:12px; font-weight:800; }}
    .insight-chip.accent {{ background:#eff6ff; color:#1d4ed8; border-color:#bfdbfe; }}
    .insight-chip em {{ font-style:normal; opacity:0.66; font-size:11px; }}
    .panel-label {{ font-size:12px; font-weight:900; letter-spacing:0.12em; text-transform:uppercase; color:#64748b; }}
    .insight-list-item {{ list-style:none; margin:0; padding:12px 14px; border-radius:18px; background:#f8fafc; border:1px solid rgba(226,232,240,0.95); font-size:13px; color:#334155; }}
    .clamp-2 {{ overflow: hidden; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; }}
    .clamp-3 {{ overflow: hidden; display: -webkit-box; -webkit-line-clamp: 3; -webkit-box-orient: vertical; }}
    body.embedded aside {{ display:none !important; }}
    body.embedded .sidebar {{ display:none !important; }}
    body.embedded main {{ padding: 24px !important; }}
    @media (max-width: 1024px) {{
      .heatmap-row {{ grid-template-columns: 132px repeat({heat_cols}, minmax(48px, 1fr)); min-width: {heat_width_mobile}px; }}
    }}
    """

def write_json(path: str, obj: Any):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def write_html_legacy(path: str, rows: List[Banner], changes: Optional[Dict[str, Any]] = None):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    by_brand: Dict[str, List[Banner]] = {}
    for b in rows:
        by_brand.setdefault(b.brand_key, []).append(b)

    order = [bk for bk, _, _, _, _ in BRANDS]
    active_brand_keys = [bk for bk in order if bk in by_brand] or [bk for bk, *_ in BRANDS]
    now_str = kst_now().strftime('%Y-%m-%d %H:%M')
    period_label = f"조회 기간: 실행 시점 스냅샷(KST) · 전회 실행 대비(변경 감지) · {now_str} KST"

    # changes summary
    ch_summary = (changes or {}).get("summary") or {}
    ch_badge = ""
    ch_list_html = ""
    if changes and ch_summary:
        ch_badge = f"""
        <div class="glass-card px-6 py-4 flex items-center gap-4">
          <div class="flex h-3 w-3 relative"><span class="animate-ping absolute h-full w-full rounded-full bg-emerald-400 opacity-75"></span><span class="relative inline-flex rounded-full h-3 w-3 bg-emerald-600"></span></div>
          <div class="text-sm font-black text-slate-800 tracking-tight">
            최근 {int((changes or {}).get('window_days', RECENT_CHANGE_DAYS))}일 변경 브랜드: <span class="text-emerald-700">{int(ch_summary.get('brands_touched',0))}</span> · 이벤트 <span class="text-slate-700">{int(ch_summary.get('events',0))}</span> · 추가 <span class="text-slate-700">{int(ch_summary.get('added',0))}</span> · 삭제 <span class="text-slate-700">{int(ch_summary.get('removed',0))}</span>
          </div>
        </div>
        """

    # 최근 7일 변화 브랜드 리스트(브랜드 중복 1개로 묶음)
    safe_changes = changes if isinstance(changes, dict) else {}
    changed_items = (safe_changes.get("brands") or [])[:18]
    if changed_items:
        lis = ""
        for x in changed_items:
            bk = x.get("brand_key", "")
            bn = next((name for k, name, *_ in BRANDS if k == bk), x.get("brand_name", bk))
            rk = x.get("last_rank", "")
            ttl = (x.get("last_title", "") or "-").replace('"', "'")
            lis += f"""
        <div class="p-4 rounded-2xl bg-white/50 border border-white/70">
          <div class="text-xs font-black text-slate-800">{bn}</div>
          <div class="text-sm font-bold text-slate-900 line-clamp-1 mt-1">"{ttl}"</div>
          <div class="text-[11px] text-slate-500 mt-1">최근 {int((safe_changes or {}).get('window_days', RECENT_CHANGE_DAYS))}일 · events {int(x.get('events',0))} · changed {int(x.get('changed',0))} · added {int(x.get('added',0))} · removed {int(x.get('removed',0))} · last rank {rk or '-'}</div>
        </div>
            """
        ch_list_html = f"""
    <section class="mb-10">
      <div class="glass-card p-6">
        <div class="text-sm font-black text-slate-900 mb-3">최근 {int((safe_changes or {}).get('window_days', RECENT_CHANGE_DAYS))}일 내 변경된 배너 브랜드 (Top {len(changed_items)})</div>
        <div class="grid grid-cols-1 md:grid-cols-2 gap-3">{lis}</div>
      </div>
    </section>
        """

    tab_menu_html = ""
    content_area_html = ""

    for i, bk in enumerate(active_brand_keys):
        items = sorted(by_brand.get(bk, []), key=lambda x: x.rank)
        brand_name = next((bn for k, bn, *rest in BRANDS if k == bk), bk)
        active_class = "bg-[#002d72] text-white shadow-lg" if i == 0 else "bg-white/50 text-slate-500 hover:bg-white"

        tab_menu_html += f"""
<button onclick="switchTab('{bk}')" id="tab-{bk}" class="tab-btn px-6 py-3 rounded-2xl font-black transition-all text-sm {active_class}">
  {brand_name} <span class="ml-1 opacity-60 text-xs">{len(items)}</span>
</button>"""

        display_style = "grid" if i == 0 else "none"
        cards_html = ""

        if not items:
            cards_html = """
<div class="glass-card p-8 text-slate-500">
  <div class="text-sm font-bold mb-2">데이터가 아직 없어요</div>
  <div class="text-xs">이번 실행에서 히어로 배너를 수집하지 못했습니다. (일시적 구조 변경/팝업/봇체크 가능)</div>
</div>
"""
        else:
            for it in items:
                img_src = ""
                if it.img_local:
                    local_path = os.path.join(ASSET_DIR, it.img_local)
                    img_src = to_file_url(local_path) if HTML_USE_ABSOLUTE_FILE_URL else f"assets/{it.img_local}"
                if not img_src:
                    img_src = it.img_url or ""

                href = it.href_clean or it.href or "#"
                img_url_btn = it.img_url or img_src or "#"

                date_txt = ""
                if it.plan_start and it.plan_end:
                    date_txt = f"{it.plan_start} ~ {it.plan_end}"
                elif it.plan_start:
                    date_txt = f"{it.plan_start}"
                else:
                    date_txt = ""

                meta_txt = ""
                if it.img_w and it.img_h:
                    meta_txt = f"{it.img_w}×{it.img_h}"
                    if it.img_bytes:
                        meta_txt += f" · {int(it.img_bytes/1024):,}KB"
                elif it.img_status and it.img_status != "ok":
                    meta_txt = f"{it.img_status}"

                cards_html += f"""
<div class="glass-card overflow-hidden hover:scale-[1.02] transition-transform flex flex-col">
  <div class="relative aspect-[16/9] bg-slate-100">
    <img src="{img_src}" class="w-full h-full object-cover"
         onerror="this.onerror=null; this.src='https://placehold.co/600x400?text=No+Image';">
    <span class="absolute top-4 left-4 px-3 py-1 bg-black/60 text-white text-[10px] font-bold rounded-full backdrop-blur-md">
      RANK {it.rank}
    </span>
  </div>
  <div class="p-6 flex flex-col flex-1">
    <h4 class="text-slate-800 font-bold text-sm mb-2 line-clamp-2 min-h-[40px]">"{it.title}"</h4>

    <div class="text-xs text-slate-500 mb-4">
      <div>{date_txt}</div>
      <div class="opacity-70">{meta_txt}</div>
    </div>

    <div class="flex gap-2 mt-auto">
      <a href="{href}" target="_blank" class="flex-1 px-4 py-2 bg-[#002d72] text-white text-[10px] font-black rounded-xl text-center hover:bg-blue-600 transition-colors">
        기획전 바로가기
      </a>
      <a href="{img_url_btn}" target="_blank" class="px-4 py-2 bg-slate-100 text-slate-500 text-[10px] font-black rounded-xl text-center hover:bg-slate-200 transition-colors">
        원본이미지
      </a>
    </div>
  </div>
</div>"""

        content_area_html += f"""
<div id="content-{bk}" class="tab-content grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6" style="display: {display_style};">
  {cards_html}
</div>"""

    full_html = f"""
<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <title>M-OS PRO | Competitor Hero Analysis</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
  <style>
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
      <div class="p-4 rounded-2xl text-slate-400 font-bold flex items-center gap-4 hover:bg-white/50 transition-all cursor-pointer">
        <i class="fa-solid fa-tower-broadcast"></i> <span>Live VOC 분석</span>
      </div>
      <div class="p-4 rounded-2xl bg-white shadow-sm text-[#002d72] font-black flex items-center gap-4 cursor-pointer">
        <i class="fa-solid fa-chart-line"></i> <span>경쟁사 기획전</span>
      </div>
    </nav>
  </aside>

  <main class="flex-1 p-8 md:p-16">
    <header class="flex flex-col md:flex-row md:items-center justify-between mb-10 gap-6">
      <div>
        <h1 class="text-5xl font-black tracking-tight text-slate-900 mb-4">Hero Banner Analysis</h1>
        <p class="text-slate-500 text-lg font-medium italic">주요 아웃도어 경쟁사 메인 히어로 배너 모니터링</p>
        <p class="text-slate-500 text-sm mt-3">{period_label}</p>
        <p class="text-slate-400 text-xs mt-3">로컬이미지 경로 모드: {"ABS(file://)" if HTML_USE_ABSOLUTE_FILE_URL else "REL(assets/)"} · 날짜추출: {"ON" if FETCH_CAMPAIGN_DATES else "OFF"} (rank1_only={"ON" if DATE_FETCH_RANK1_ONLY else "OFF"})</p>
      </div>
      <div class="flex flex-col gap-3 items-start md:items-end">
        {ch_badge}
        <div class="glass-card px-6 py-4 flex items-center gap-4">
          <div class="flex h-3 w-3 relative"><span class="animate-ping absolute h-full w-full rounded-full bg-blue-400 opacity-75"></span><span class="relative inline-flex rounded-full h-3 w-3 bg-blue-600"></span></div>
          <span class="text-sm font-black text-slate-800 tracking-widest uppercase">{now_str}</span>
        </div>
      </div>
    </header>

    {ch_list_html}

    <section>
      <div class="flex flex-wrap gap-2 mb-8">
        {tab_menu_html}
      </div>
      <div class="min-h-[600px]">
        {content_area_html}
      </div>
    </section>
  </main>

  <script>
    function switchTab(brandKey) {{
      document.querySelectorAll('.tab-content').forEach(el => el.style.display = 'none');
      const t = document.getElementById('content-' + brandKey);
      if (t) t.style.display = 'grid';

      document.querySelectorAll('.tab-btn').forEach(btn => {{
        btn.classList.remove('bg-[#002d72]', 'text-white', 'shadow-lg');
        btn.classList.add('bg-white/50', 'text-slate-500');
      }});

      const activeBtn = document.getElementById('tab-' + brandKey);
      if (activeBtn) {{
        activeBtn.classList.add('bg-[#002d72]', 'text-white', 'shadow-lg');
        activeBtn.classList.remove('bg-white/50', 'text-slate-500');
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
    with open(path, "w", encoding="utf-8") as f:
        f.write(full_html)


def build_collection_health(rows: List[Banner]) -> Dict[str, Any]:
    total = max(len(rows), 1)
    fallback_count = sum(1 for b in rows if b.is_fallback)
    low_confidence_count = sum(1 for b in rows if float(b.confidence_score or 0.0) < 0.7)
    meta_attempted = [b for b in rows if (b.landing_text_status or "") not in {"", "skipped", "no_href"}]
    meta_ok = sum(1 for b in meta_attempted if b.landing_text_status == "ok")
    meta_fail = sum(1 for b in meta_attempted if b.landing_text_status != "ok")
    brand_zero_banner_list = [BRAND_NAME_MAP.get(bk, bk) for bk in BRAND_ORDER if not any(r.brand_key == bk for r in rows)]
    health = {
        "empty_brand_count": len(brand_zero_banner_list),
        "brand_zero_banner_list": brand_zero_banner_list,
        "fallback_count": fallback_count,
        "fallback_ratio": round(fallback_count / total, 3),
        "low_confidence_count": low_confidence_count,
        "low_confidence_ratio": round(low_confidence_count / total, 3),
        "landing_meta_ok": meta_ok,
        "landing_meta_fail": meta_fail,
        "landing_meta_fail_ratio": round(meta_fail / max(len(meta_attempted), 1), 3) if meta_attempted else 0.0,
    }
    alerts = []
    if health["empty_brand_count"] > 0:
        alerts.append(f"배너 0개 브랜드 {health['empty_brand_count']}개")
    if health["fallback_ratio"] > 0.6:
        alerts.append(f"fallback 비율 높음 ({health['fallback_ratio']:.0%})")
    if health["low_confidence_ratio"] > 0.5:
        alerts.append(f"low confidence 비율 높음 ({health['low_confidence_ratio']:.0%})")
    if health["landing_meta_fail_ratio"] > 0.5:
        alerts.append(f"landing meta 실패 비율 높음 ({health['landing_meta_fail_ratio']:.0%})")
    health["alerts"] = alerts
    return health


def write_dashboard_html_legacy(path: str, rows: List[Banner], changes: Optional[Dict[str, Any]] = None):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    by_brand: Dict[str, List[Banner]] = {}
    for b in rows:
        by_brand.setdefault(b.brand_key, []).append(b)

    active_brand_keys = [bk for bk in BRAND_ORDER if bk in by_brand] or BRAND_ORDER
    safe_changes = changes if isinstance(changes, dict) else {}
    summary = safe_changes.get("summary") or {}
    changes_map = {x.get("brand_key", ""): x for x in (safe_changes.get("brands") or [])}
    keywords = filter_dashboard_keywords(analyze_title_keywords(rows, top_n=40), top_n=14)
    brand_insights = {
        bk: summarize_brand_insight(bk, sorted(by_brand.get(bk, []), key=lambda x: x.rank), changes_map)
        for bk in active_brand_keys
    }
    health = build_collection_health(rows)

    overview_cards_html = f"""
    <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
      <div class="metric-card"><div class="metric-label">Tracked Brands</div><div class="metric-value">{len(active_brand_keys)}</div><div class="metric-sub">이번 실행 기준</div></div>
      <div class="metric-card"><div class="metric-label">Hero Banners</div><div class="metric-value">{len(rows)}</div><div class="metric-sub">배너 / 카드 / 히어로 후보</div></div>
      <div class="metric-card"><div class="metric-label">Change Events</div><div class="metric-value">{int(summary.get('events', 0) or 0)}</div><div class="metric-sub">{_html_pct_chip(float(summary.get('event_delta_pct', 0.0) or 0.0))}</div></div>
      <div class="metric-card"><div class="metric-label">Landing Meta</div><div class="metric-value">{int(health.get('landing_meta_ok', 0) or 0)}</div><div class="metric-sub">ok {int(health.get('landing_meta_ok', 0) or 0)} / fail {int(health.get('landing_meta_fail', 0) or 0)}</div></div>
    </div>
    """

    if keywords:
        max_count = max(int(x.get("count", 0) or 0) for x in keywords) or 1
        keyword_cloud_html = "".join([
            f'<span class="keyword-chip" style="font-size:{0.86 + (float(x.get("count", 0) or 0) / max_count) * 0.65:.2f}rem">{_h(x.get("keyword", ""))} <em>{int(x.get("count", 0) or 0)}</em></span>'
            for x in keywords
        ])
        theme_summary = ", ".join([x.get("keyword", "") for x in keywords[:4]])
    else:
        keyword_cloud_html = '<div class="text-sm text-slate-500">키워드 집계 데이터가 아직 없습니다.</div>'
        theme_summary = "키워드 부족"

    ops_warning_html = "".join([
        f'<div class="rounded-2xl border border-amber-200 bg-amber-50/90 px-4 py-3 text-sm text-amber-800 font-semibold">{_h(msg)}</div>'
        for msg in health.get("alerts", [])
    ]) or '<div class="rounded-2xl border border-emerald-100 bg-emerald-50/90 px-4 py-3 text-sm text-emerald-700 font-semibold">구조 이상 징후는 아직 감지되지 않았습니다.</div>'
    if health.get("brand_zero_banner_list"):
        ops_warning_html += f'<div class="rounded-2xl border border-slate-200 bg-white/80 px-4 py-3 text-sm text-slate-600">배너 0개 브랜드: {_h(", ".join(health.get("brand_zero_banner_list", [])[:8]))}</div>'

    subtype_summary_html = "".join([
        f'<span class="insight-chip">{_h(k)} <em>{int(v or 0)}</em></span>'
        for k, v in sorted((summary.get("subtypes") or {}).items(), key=lambda x: (-int(x[1] or 0), x[0]))[:6]
    ]) or '<span class="text-sm text-slate-400">변화 subtype 집계 없음</span>'

    alert_list_html = "".join([
        f"""
        <div class="rounded-2xl border border-rose-100 bg-rose-50/70 px-4 py-3">
          <div class="text-sm font-bold text-slate-900">{_h(item.get('brand_name', '-'))}</div>
          <div class="text-[11px] text-slate-500">{_h(item.get('date', '-'))} · events {int(item.get('events', 0) or 0)} / changed {int(item.get('changed', 0) or 0)}</div>
        </div>
        """
        for item in alerts_payload.get("items", [])[:4]
    ]) or '<div class="rounded-2xl border border-emerald-100 bg-emerald-50/80 px-4 py-4 text-sm text-emerald-700 font-semibold">기준 이상 변화 알림은 아직 없습니다.</div>'

    tab_menu_html = ""
    content_area_html = ""
    for i, bk in enumerate(active_brand_keys):
        items = sorted(by_brand.get(bk, []), key=lambda x: x.rank)
        brand_name = BRAND_NAME_MAP.get(bk, bk)
        seg = BRAND_SEGMENTS.get(bk, {"style": "기타", "origin": "기타"})
        insight = brand_insights.get(bk, {})
        tab_menu_html += f'<button onclick="switchTab(\'{_h(bk)}\')" id="tab-{_h(bk)}" class="{"tab-btn active" if i == 0 else "tab-btn"}"><span>{_h(brand_name)}</span><span class="tab-meta">{len(items)}</span></button>'
        if not items:
            content_area_html += f'<section id="content-{_h(bk)}" class="tab-content" style="display:{ "block" if i == 0 else "none" }"><div class="glass-card p-8 text-slate-500"><div class="text-base font-bold mb-2">배너 데이터를 가져오지 못했습니다.</div></div></section>'
            continue

        top_item = items[0]
        visual = insight.get("visual", {}) or {}
        keyword_badges = "".join([f'<span class="insight-chip">{_h(x.get("keyword", ""))} <em>{int(x.get("count", 0) or 0)}</em></span>' for x in insight.get("keywords", [])[:5]]) or '<span class="text-sm text-slate-400">키워드 부족</span>'
        subtype_badges = "".join([f'<span class="insight-chip accent">{_h(k)} <em>{int(v or 0)}</em></span>' for k, v in sorted((insight.get("subtypes") or {}).items(), key=lambda x: (-int(x[1] or 0), x[0]))[:4]]) or '<span class="text-sm text-slate-400">subtype 없음</span>'
        landing_badges = "".join([f'<span class="insight-chip">{_h(x)}</span>' for x in _keyword_chip_list(top_item)]) or '<span class="text-sm text-slate-400">landing keyword 없음</span>'
        recent_titles_html = "".join([f'<li class="insight-list-item">"{_h(t)}"</li>' for t in insight.get("recent_titles", [])[:4]]) or '<li class="insight-list-item">최근 변화 제목 없음</li>'
        other_cards = "".join([_render_secondary_card(it) for it in items[1:7]]) or '<div class="soft-panel p-6 text-sm text-slate-500">추가 배너가 없어 메인 카드만 표시합니다.</div>'
        brand_alerts = "".join([
            f'<div class="rounded-2xl border border-rose-100 bg-rose-50/80 px-3 py-3 text-sm text-rose-700 font-semibold">{_h(a.get("date", "-"))} · {int(a.get("events", 0) or 0)}건 변화</div>'
            for a in alerts_payload.get("items", []) if a.get("brand_key") == bk
        ]) or '<div class="rounded-2xl border border-slate-200 bg-slate-50/90 px-3 py-3 text-sm text-slate-500">해당 브랜드는 기준 이상 알림이 없습니다.</div>'
        content_area_html += f"""
        <section id="content-{_h(bk)}" class="tab-content" style="display:{'block' if i == 0 else 'none'};">
          <div class="grid grid-cols-1 xl:grid-cols-3 gap-6">
            <div class="xl:col-span-2 flex flex-col gap-6">
              <article class="glass-card overflow-hidden">
                <div class="grid grid-cols-1 lg:grid-cols-[minmax(0,1.1fr)_minmax(340px,0.9fr)]">
                  <div class="relative min-h-[320px] bg-slate-100">
                    <img src="{_h(_html_img_src(top_item))}" class="w-full h-full object-cover" onerror="this.onerror=null; this.src='https://placehold.co/1100x620?text=No+Image';">
                    <div class="absolute inset-0 bg-gradient-to-t from-slate-950/55 via-transparent to-transparent"></div>
                    <div class="absolute left-5 top-5 flex flex-wrap gap-2">
                      <span class="inline-flex items-center justify-center rounded-full bg-[rgba(11,43,102,0.9)] px-3 py-2 text-[11px] font-black text-white">RANK {int(top_item.rank or 0)}</span>
                      <span class="inline-flex items-center justify-center rounded-full bg-white/15 px-3 py-2 text-[11px] font-black text-white">{_h(seg.get('style', '기타'))}</span>
                      <span class="inline-flex items-center justify-center rounded-full bg-white/15 px-3 py-2 text-[11px] font-black text-white">{_h(seg.get('origin', '기타'))}</span>
                    </div>
                  </div>
                  <div class="p-6 lg:p-7 flex flex-col gap-5">
                    <div>
                      <div class="text-xs font-bold uppercase tracking-[0.16em] text-slate-400">Landing-first Hero Card</div>
                      <h3 class="mt-2 text-2xl font-black tracking-tight text-slate-900">{_h(brand_name)}</h3>
                      <p class="mt-3 text-lg font-bold text-slate-900 clamp-3">"{_h(_display_title(top_item))}"</p>
                      <p class="mt-2 text-sm text-slate-600 clamp-3">{_h(_display_subtitle(top_item) or _display_summary(top_item))}</p>
                      <p class="mt-3 text-sm text-slate-500 clamp-3">{_h(_display_summary(top_item))}</p>
                      <div class="mt-4 flex flex-wrap gap-2">{landing_badges}</div>
                    </div>
                    <div class="grid grid-cols-2 gap-3">
                      <div class="mini-card"><span>기간</span><strong>{_h(_html_date_txt(top_item))}</strong></div>
                      <div class="mini-card"><span>이미지</span><strong>{_h(_html_meta_txt(top_item))}</strong></div>
                      <div class="mini-card"><span>source</span><strong>{_h(top_item.extract_source or '-')}</strong></div>
                      <div class="mini-card"><span>confidence</span><strong>{float(top_item.confidence_score or 0.0):.2f}</strong></div>
                    </div>
                    <div class="grid grid-cols-2 gap-3">
                      <div class="mini-card"><span>landing status</span><strong>{_h(top_item.landing_text_status or '-')}</strong></div>
                      <div class="mini-card"><span>category</span><strong>{_h(top_item.landing_category or '-')}</strong></div>
                    </div>
                    <div class="flex gap-2 mt-auto">
                      <a href="{_h(top_item.href_clean or top_item.href or '#')}" target="_blank" class="btn-primary flex-1 text-center">기획전 보기</a>
                      <a href="{_h(top_item.img_url or _html_img_src(top_item) or '#')}" target="_blank" class="btn-secondary">원본 이미지</a>
                    </div>
                  </div>
                </div>
              </article>
              <div class="grid grid-cols-1 md:grid-cols-2 gap-5">{other_cards}</div>
            </div>
            <aside class="flex flex-col gap-5">
              <section class="glass-card p-6">
                <div class="text-xs font-bold uppercase tracking-[0.16em] text-slate-400">브랜드 인사이트</div>
                <h4 class="mt-2 text-xl font-black text-slate-900">{_h(brand_name)} 분석 요약</h4>
                <p class="mt-3 text-sm text-slate-600">대표 landing summary: {_h(_display_summary(top_item))}</p>
                <div class="mt-5 grid grid-cols-2 gap-3">
                  <div class="mini-card"><span>events</span><strong>{int(insight.get('events', 0) or 0)}</strong></div>
                  <div class="mini-card"><span>changed</span><strong>{int(insight.get('changed', 0) or 0)}</strong></div>
                  <div class="mini-card"><span>added</span><strong>{int(insight.get('added', 0) or 0)}</strong></div>
                  <div class="mini-card"><span>removed</span><strong>{int(insight.get('removed', 0) or 0)}</strong></div>
                </div>
                <div class="mt-5"><div class="panel-label">Keyword Cluster</div><div class="mt-2 flex flex-wrap gap-2">{keyword_badges}</div></div>
                <div class="mt-5"><div class="panel-label">Change Subtypes</div><div class="mt-2 flex flex-wrap gap-2">{subtype_badges}</div></div>
                <div class="mt-5"><div class="panel-label">Visual / OCR</div><p class="mt-2 text-sm text-slate-700">{_h(visual.get('summary', '태그 자동 분류 데이터 부족'))}</p><div class="mt-2 flex flex-wrap gap-2">{"".join([f'<span class="insight-chip accent">{_h(tag)}</span>' for tag in visual.get('tags', [])[:6]]) or '<span class="text-sm text-slate-400">태그 없음</span>'}</div></div>
                <div class="mt-5"><div class="panel-label">OCR 텍스트</div><div class="mt-2 rounded-2xl bg-slate-50 px-4 py-3 text-sm text-slate-600">{_h(visual.get('ocr_text', '') or ('OCR 비활성화' if not ENABLE_OCR else '추출 텍스트 없음'))}</div></div>
              </section>
              <section class="glass-card p-6"><div class="panel-label">최근 변화 제목</div><ul class="mt-3 space-y-2">{recent_titles_html}</ul></section>
              <section class="glass-card p-6"><div class="panel-label">알림</div><div class="mt-3 space-y-3">{brand_alerts}</div></section>
            </aside>
          </div>
        </section>
        """

    full_html = f"""
<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>M-OS PRO | Competitor Hero Analysis</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
  <style>{_dashboard_css(len(safe_changes.get("timeline_days") or []))}</style>
</head>
<body class="flex">
  <aside class="w-72 h-screen sticky top-0 sidebar hidden xl:flex flex-col p-8">
    <div class="flex items-center gap-4 mb-16 px-2"><div class="w-12 h-12 bg-[var(--brand-deep)] rounded-2xl flex items-center justify-center text-white shadow-xl"><i class="fa-solid fa-mountain-sun text-xl"></i></div><div><div class="text-xl font-black tracking-tighter italic">M-OS <span class="text-blue-600 font-extrabold">PRO</span></div><div class="text-[9px] font-black uppercase tracking-[0.3em] text-slate-400">Competitive Watch</div></div></div>
    <nav class="space-y-4"><div class="p-4 rounded-2xl text-slate-400 font-bold flex items-center gap-4"><i class="fa-solid fa-tower-broadcast"></i> <span>Live VOC</span></div><div class="p-4 rounded-2xl bg-white shadow-sm text-[var(--brand-deep)] font-black flex items-center gap-4"><i class="fa-solid fa-chart-line"></i> <span>Hero Banner Monitor</span></div></nav>
  </aside>
  <main class="flex-1 px-5 py-8 md:px-8 xl:px-12 xl:py-10">
    <header class="mb-8"><div class="glass-card p-7 lg:p-8"><div class="flex flex-col xl:flex-row xl:items-end xl:justify-between gap-6"><div class="max-w-3xl"><div class="inline-flex items-center gap-2 rounded-full bg-blue-50 px-3 py-1 text-[11px] font-black tracking-[0.18em] text-blue-700 uppercase">Hero Banner Monitor</div><h1 class="mt-4 text-4xl lg:text-5xl font-black tracking-tight text-slate-950">배너 의미와 구조 이상 징후를 함께 보는 경쟁사 대시보드</h1><p class="mt-4 text-base lg:text-lg text-slate-600">landing title, summary, keyword, category, extractor confidence, 구조 이상 징후를 한 화면에서 보도록 개편했습니다.</p><p class="mt-4 text-sm text-slate-500">최근 {int(safe_changes.get('window_days', RECENT_CHANGE_DAYS) or RECENT_CHANGE_DAYS)}일 기준 변화 모니터링 · {_h(kst_now().strftime('%Y-%m-%d %H:%M'))} KST</p><p class="mt-2 text-xs text-slate-400">이미지 경로: {"ABS(file://)" if HTML_USE_ABSOLUTE_FILE_URL else "REL(assets/)"} · campaign meta: {"ON" if FETCH_CAMPAIGN_META else "OFF"} (rank_limit={int(CAMPAIGN_META_RANK_LIMIT)}) · OCR: {"ON" if ENABLE_OCR else "OFF"}</p></div><div class="soft-panel px-5 py-4 min-w-[240px]"><div class="text-[11px] font-black uppercase tracking-[0.18em] text-slate-400">이번 주 시장 테마</div><div class="mt-2 text-2xl font-black tracking-tight text-slate-900">{_h(theme_summary)}</div><div class="mt-4 text-sm text-slate-500">랜딩 메타와 change subtype 기준으로 정리했습니다.</div></div></div><div class="mt-7">{overview_cards_html}</div></div></header>
    <section class="grid grid-cols-1 2xl:grid-cols-[minmax(0,1.45fr)_minmax(340px,0.95fr)] gap-6 mb-6">{_render_heatmap_section(safe_changes)}<div class="flex flex-col gap-6"><section class="glass-card p-6 lg:p-7"><h2 class="section-title">이번 주 키워드 클라우드</h2><p class="section-sub">landing title / summary 기준으로 반복 메시지를 집계했습니다.</p><div class="mt-5">{keyword_cloud_html}</div></section><section class="glass-card p-6 lg:p-7"><h2 class="section-title">운영 이상 징후</h2><p class="section-sub">empty brand, fallback ratio, low confidence, landing meta fail 비율을 같이 봅니다.</p><div class="mt-4 space-y-3">{ops_warning_html}</div></section></div></section>
    <section class="mb-6"><div class="glass-card p-6 lg:p-7"><div class="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-4"><div><h2 class="section-title">변화 해석</h2><p class="section-sub">rank shift, image changed, landing text changed 등을 구분해서 봅니다.</p></div><div class="text-[11px] text-slate-500">subtype 기준 상위 항목</div></div><div class="mt-5 flex flex-wrap gap-2">{subtype_summary_html}</div><div class="mt-5 space-y-3">{alert_list_html}</div></div></section>
    <section><div class="glass-card p-6 lg:p-7"><div class="flex flex-col gap-4"><div class="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-3"><div><h2 class="section-title">브랜드 상세 비교</h2><p class="section-sub">landing-first 카드로 실제 기획전 성격과 변화 의미를 바로 파악할 수 있게 구성했습니다.</p></div></div><div class="flex flex-wrap gap-2 pt-1">{tab_menu_html}</div></div></div><div class="mt-6 min-h-[720px] space-y-6">{content_area_html}</div></section>
  </main>
  <script>
    function switchTab(brandKey) {{
      document.querySelectorAll('.tab-content').forEach(el => el.style.display = 'none');
      const target = document.getElementById('content-' + brandKey);
      if (target) target.style.display = 'block';
      document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
      const activeBtn = document.getElementById('tab-' + brandKey);
      if (activeBtn) activeBtn.classList.add('active');
    }}
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
    with open(path, "w", encoding="utf-8") as f:
        f.write(full_html)


def write_dashboard_html(path: str, rows: List[Banner], changes: Optional[Dict[str, Any]] = None):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    by_brand: Dict[str, List[Banner]] = {}
    for b in rows:
        by_brand.setdefault(b.brand_key, []).append(b)

    order = [bk for bk, _, _, _, _ in BRANDS]
    brand_name_map = {bk: bn for bk, bn, *_ in BRANDS}
    active_brand_keys = [bk for bk in order if bk in by_brand] or [bk for bk, *_ in BRANDS]
    safe_changes = changes if isinstance(changes, dict) else {}
    summary = safe_changes.get("summary") or {}
    changes_map = {x.get("brand_key", ""): x for x in (safe_changes.get("brands") or [])}
    keywords = filter_dashboard_keywords(analyze_title_keywords(rows, top_n=40), top_n=14)
    brand_insights = {
        bk: summarize_brand_insight(bk, sorted(by_brand.get(bk, []), key=lambda x: x.rank), changes_map)
        for bk in active_brand_keys
    }

    overview_cards_html = f"""
    <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
      <div class="metric-card"><div class="metric-label">Tracked Brands</div><div class="metric-value">{len(active_brand_keys)}</div><div class="metric-sub">Current brand coverage</div></div>
      <div class="metric-card"><div class="metric-label">Hero Banners</div><div class="metric-value">{len(rows)}</div><div class="metric-sub">Main campaign cards</div></div>
      <div class="metric-card"><div class="metric-label">Change Events</div><div class="metric-value">{int(summary.get('events', 0) or 0)}</div><div class="metric-sub">{_html_pct_chip(float(summary.get('event_delta_pct', 0.0) or 0.0))}</div></div>
      <div class="metric-card"><div class="metric-label">Actual Changes</div><div class="metric-value">{int(summary.get('changed', 0) or 0)}</div><div class="metric-sub">{_html_pct_chip(float(summary.get('changed_delta_pct', 0.0) or 0.0))}</div></div>
    </div>
    """

    if keywords:
        max_count = max(int(x.get("count", 0) or 0) for x in keywords) or 1
        keyword_cloud_html = "".join([
            f'<span class="keyword-chip" style="font-size:{0.86 + (float(x.get("count", 0) or 0) / max_count) * 0.65:.2f}rem">{_h(x.get("keyword", ""))} <em>{int(x.get("count", 0) or 0)}</em></span>'
            for x in keywords
        ])
        theme_summary = ", ".join([x.get("keyword", "") for x in keywords[:4]])
    else:
        keyword_cloud_html = '<div class="text-sm text-slate-500">이번 주 반복 키워드를 아직 뽑지 못했습니다.</div>'
        theme_summary = '주요 테마 추출 데이터 부족'

    filter_buttons_html = "".join([
        '<button class="{cls}" data-filter="{key}" onclick="setBrandFilter(\'{key}\')">{label}</button>'.format(
            cls=("filter-btn active" if key == "all" else "filter-btn"), key=_h(key), label=_h(label)
        )
        for key, label in [("all", "전체"), ("라이프스타일", "라이프스타일"), ("전문 산행", "전문 산행"), ("글로벌", "글로벌"), ("로컬", "로컬")]
    ])

    tab_menu_html = ""
    content_area_html = ""
    for i, bk in enumerate(active_brand_keys):
        items = sorted(by_brand.get(bk, []), key=lambda x: x.rank)
        brand_name = brand_name_map.get(bk, bk)
        seg = BRAND_SEGMENTS.get(bk, {"style": "ETC", "origin": "ETC"})
        insight = brand_insights.get(bk, {})
        tab_menu_html += '<button onclick="switchTab(\'{bk}\')" id="tab-{bk}" class="{cls}" data-style="{style}" data-origin="{origin}"><span>{name}</span><span class="tab-meta">{meta}</span></button>'.format(
            bk=_h(bk),
            cls=("tab-btn active" if i == 0 else "tab-btn"),
            style=_h(seg.get("style", "ETC")),
            origin=_h(seg.get("origin", "ETC")),
            name=_h(brand_name),
            meta=int(insight.get("events", 0) or len(items)),
        )
        if not items:
            content_area_html += f'<section id="content-{_h(bk)}" class="tab-content" style="display:{"block" if i == 0 else "none"}"><div class="glass-card p-8 text-slate-500"><div class="text-base font-bold mb-2">No banner data collected yet.</div></div></section>'
            continue

        top_item = items[0]
        visual = insight.get("visual", {}) or {}
        visual_badges = "".join([f'<span class="insight-chip accent">{_h(tag)}</span>' for tag in visual.get("tags", [])[:6]]) or '<span class="text-sm text-slate-400">No tags</span>'
        keyword_badges = "".join([f'<span class="insight-chip">{_h(x.get("keyword", ""))} <em>{int(x.get("count", 0) or 0)}</em></span>' for x in insight.get("keywords", [])[:5]]) or '<span class="text-sm text-slate-400">No keywords</span>'
        recent_titles_html = "".join([f'<li class="insight-list-item">"{_h(t)}"</li>' for t in insight.get("recent_titles", [])[:4]]) or '<li class="insight-list-item">No recent title changes</li>'
        other_cards = "".join([_render_secondary_card(it) for it in items[1:7]]) or '<div class="soft-panel p-6 text-sm text-slate-500">No additional banner cards.</div>'
        content_area_html += f"""
        <section id="content-{_h(bk)}" class="tab-content" style="display:{'block' if i == 0 else 'none'};">
          <div class="grid grid-cols-1 xl:grid-cols-3 gap-6">
            <div class="xl:col-span-2 flex flex-col gap-6">
              <article class="glass-card overflow-hidden">
                <div class="grid grid-cols-1 lg:grid-cols-[minmax(0,1.15fr)_minmax(320px,0.85fr)]">
                  <div class="relative min-h-[320px] bg-slate-100">
                    <img src="{_h(_html_img_src(top_item))}" class="w-full h-full object-cover" onerror="this.onerror=null; this.src='https://placehold.co/1100x620?text=No+Image';">
                    <div class="absolute inset-0 bg-gradient-to-t from-slate-950/50 via-transparent to-transparent"></div>
                    <div class="absolute left-5 top-5 flex flex-wrap gap-2">
                      <span class="inline-flex items-center justify-center rounded-full bg-[rgba(11,43,102,0.9)] px-3 py-2 text-[11px] font-black text-white">RANK {int(top_item.rank or 0)}</span>
                      <span class="inline-flex items-center justify-center rounded-full bg-white/15 px-3 py-2 text-[11px] font-black text-white">{_h(seg.get('style', 'ETC'))}</span>
                      <span class="inline-flex items-center justify-center rounded-full bg-white/15 px-3 py-2 text-[11px] font-black text-white">{_h(seg.get('origin', 'ETC'))}</span>
                    </div>
                  </div>
                  <div class="p-6 lg:p-7 flex flex-col gap-5">
                    <div>
                      <div class="text-xs font-bold uppercase tracking-[0.16em] text-slate-400">Brand Detail</div>
                      <h3 class="mt-2 text-2xl font-black tracking-tight text-slate-900">{_h(brand_name)}</h3>
                      <p class="mt-3 text-lg font-bold text-slate-900 clamp-3">"{_h(top_item.title or '-')}"</p>
                      <div class="mt-4 flex flex-wrap gap-2">{_html_pct_chip(float(insight.get('event_delta_pct', 0.0) or 0.0))}{_html_pct_chip(float(insight.get('changed_delta_pct', 0.0) or 0.0))}</div>
                    </div>
                    <div class="grid grid-cols-2 gap-3">
                      <div class="mini-card"><span>Events</span><strong>{int(insight.get('events', 0) or 0)}</strong></div>
                      <div class="mini-card"><span>Actual Changes</span><strong>{int(insight.get('changed', 0) or 0)}</strong></div>
                      <div class="mini-card"><span>Campaign Dates</span><strong>{_h(_html_date_txt(top_item))}</strong></div>
                      <div class="mini-card"><span>Image Meta</span><strong>{_h(_html_meta_txt(top_item))}</strong></div>
                    </div>
                    <div class="flex flex-wrap gap-2">{visual_badges}</div>
                    <div class="flex gap-2 mt-auto">
                      <a href="{_h(top_item.href_clean or top_item.href or '#')}" target="_blank" class="btn-primary flex-1 text-center">Open Landing</a>
                      <a href="{_h(top_item.img_url or _html_img_src(top_item) or '#')}" target="_blank" class="btn-secondary">Open Image</a>
                    </div>
                  </div>
                </div>
              </article>
              <div class="grid grid-cols-1 md:grid-cols-2 gap-5">{other_cards}</div>
            </div>
            <aside class="flex flex-col gap-5">
              <section class="glass-card p-6">
                <div class="text-xs font-bold uppercase tracking-[0.16em] text-slate-400">Brand Insight</div>
                <h4 class="mt-2 text-xl font-black text-slate-900">{_h(brand_name)} Summary</h4>
                <p class="mt-3 text-sm text-slate-600">Weekly theme: {_h(', '.join([x.get('keyword', '') for x in insight.get('keywords', [])[:3]]) or 'No keyword summary')}</p>
                <div class="mt-5 grid grid-cols-2 gap-3">
                  <div class="mini-card"><span>Added</span><strong>{int(insight.get('added', 0) or 0)}</strong></div>
                  <div class="mini-card"><span>Removed</span><strong>{int(insight.get('removed', 0) or 0)}</strong></div>
                  <div class="mini-card"><span>Segment</span><strong>{_h(insight.get('segment_style', 'ETC'))}</strong></div>
                  <div class="mini-card"><span>Origin</span><strong>{_h(insight.get('segment_origin', 'ETC'))}</strong></div>
                </div>
                <div class="mt-5"><div class="panel-label">Keyword Cluster</div><div class="mt-2 flex flex-wrap gap-2">{keyword_badges}</div></div>
                <div class="mt-5"><div class="panel-label">Visual / OCR</div><p class="mt-2 text-sm text-slate-700">{_h(visual.get('summary', 'No visual summary'))}</p><div class="mt-2 flex flex-wrap gap-2">{visual_badges}</div></div>
                <div class="mt-5"><div class="panel-label">OCR Text</div><div class="mt-2 rounded-2xl bg-slate-50 px-4 py-3 text-sm text-slate-600">{_h(visual.get('ocr_text', '') or 'OCR disabled or no text')}</div></div>
              </section>
              <section class="glass-card p-6"><div class="panel-label">Recent Title Changes</div><ul class="mt-3 space-y-2">{recent_titles_html}</ul></section>
            </aside>
          </div>
        </section>
        """

    full_html = f"""
<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>M-OS PRO | Competitor Hero Analysis</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
  <style>{_dashboard_css(len(safe_changes.get("timeline_days") or []))}</style>
</head>
<body class="flex">
  <aside class="w-72 h-screen sticky top-0 sidebar hidden xl:flex flex-col p-8">
    <div class="flex items-center gap-4 mb-16 px-2"><div class="w-12 h-12 bg-[var(--brand-deep)] rounded-2xl flex items-center justify-center text-white shadow-xl"><i class="fa-solid fa-mountain-sun text-xl"></i></div><div><div class="text-xl font-black tracking-tighter italic">M-OS <span class="text-blue-600 font-extrabold">PRO</span></div><div class="text-[9px] font-black uppercase tracking-[0.3em] text-slate-400">Competitive Watch</div></div></div>
    <nav class="space-y-4"><div class="p-4 rounded-2xl text-slate-400 font-bold flex items-center gap-4"><i class="fa-solid fa-tower-broadcast"></i> <span>Live VOC</span></div><div class="p-4 rounded-2xl bg-white shadow-sm text-[var(--brand-deep)] font-black flex items-center gap-4"><i class="fa-solid fa-chart-line"></i> <span>경쟁사 기획전 분석</span></div></nav>
  </aside>
  <main class="flex-1 px-5 py-8 md:px-8 xl:px-12 xl:py-10">
    <header class="mb-8"><div class="glass-card p-7 lg:p-8"><div class="flex flex-col xl:flex-row xl:items-end xl:justify-between gap-6"><div class="max-w-3xl"><div class="inline-flex items-center gap-2 rounded-full bg-blue-50 px-3 py-1 text-[11px] font-black tracking-[0.18em] text-blue-700 uppercase">Hero Banner Monitor</div><h1 class="mt-4 text-4xl lg:text-5xl font-black tracking-tight text-slate-950">경쟁사 기획전 대시보드</h1><p class="mt-4 text-base lg:text-lg text-slate-600">반복 키워드와 대표 배너를 중심으로 경쟁사 기획전 메시지와 구성을 빠르게 읽을 수 있게 정리했습니다.</p><p class="mt-4 text-sm text-slate-500">최근 {int(safe_changes.get('window_days', RECENT_CHANGE_DAYS) or RECENT_CHANGE_DAYS)}일 기준 경쟁사 메인 히어로 배너 변화 모니터링 · {_h(kst_now().strftime('%Y-%m-%d %H:%M'))} KST</p><p class="mt-2 text-xs text-slate-400">이미지 경로 모드: {"ABS(file://)" if HTML_USE_ABSOLUTE_FILE_URL else "REL(assets/)"} · 날짜 추출: {"ON" if FETCH_CAMPAIGN_DATES else "OFF"} (rank1_only={"ON" if DATE_FETCH_RANK1_ONLY else "OFF"})</p></div><div class="soft-panel px-5 py-4 min-w-[240px]"><div class="text-[11px] font-black uppercase tracking-[0.18em] text-slate-400">이번 주 시장 테마</div><div class="mt-2 text-2xl font-black tracking-tight text-slate-900">{_h(theme_summary)}</div><div class="mt-4 text-sm text-slate-500">선택 브랜드 상세는 아래 탭에서 확인하세요.</div></div></div><div class="mt-7">{overview_cards_html}</div></div></header>
    <section class="grid grid-cols-1 2xl:grid-cols-[minmax(0,1.55fr)_minmax(340px,0.95fr)] gap-6 mb-6">{_render_heatmap_section(safe_changes)}<div class="flex flex-col gap-6"><section class="glass-card p-6 lg:p-7"><h2 class="section-title">이번 주 키워드 클라우드</h2><p class="section-sub">기획전 제목 반복어를 모아 시장의 주요 메시지를 빠르게 파악합니다.</p><div class="mt-5">{keyword_cloud_html}</div></section></div></section>
    <section><div class="glass-card p-6 lg:p-7"><div class="flex flex-col gap-4"><div class="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-3"><div><h2 class="section-title">브랜드 상세 비교</h2><p class="section-sub">필터로 세그먼트를 좁히고 아래에서 브랜드를 선택하면 메인 비주얼과 인사이트 요약을 바로 비교할 수 있습니다.</p></div><div class="flex flex-wrap gap-2">{filter_buttons_html}</div></div><div class="flex flex-wrap gap-2 pt-1">{tab_menu_html}</div></div></div><div class="mt-6 min-h-[720px] space-y-6">{content_area_html}</div></section>
  </main>
  <script>
    function switchTab(brandKey) {{
      document.querySelectorAll('.tab-content').forEach(el => el.style.display = 'none');
      const target = document.getElementById('content-' + brandKey);
      if (target) target.style.display = 'block';
      document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
      const activeBtn = document.getElementById('tab-' + brandKey);
      if (activeBtn) activeBtn.classList.add('active');
    }}
    function setBrandFilter(filterKey) {{
      document.querySelectorAll('.filter-btn').forEach(btn => btn.classList.remove('active'));
      const activeFilter = document.querySelector('.filter-btn[data-filter="' + filterKey + '"]');
      if (activeFilter) activeFilter.classList.add('active');
      let firstVisible = null;
      document.querySelectorAll('.tab-btn').forEach(btn => {{
        const matches = filterKey === 'all' || btn.dataset.style === filterKey || btn.dataset.origin === filterKey;
        btn.style.display = matches ? 'inline-flex' : 'none';
        if (matches && !firstVisible) firstVisible = btn;
      }});
      const currentActive = document.querySelector('.tab-btn.active');
      if (!currentActive || currentActive.style.display === 'none') {{
        if (firstVisible) switchTab(firstVisible.id.replace('tab-', ''));
      }}
    }}
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

    with open(path, "w", encoding="utf-8") as f:
        f.write(full_html)


# =====================================================
# Navigation hardening (브랜드 링크 가끔 안됨 대응)
# =====================================================
def _url_variants(url: str) -> List[str]:
    u = (url or "").strip()
    if not u:
        return []
    out = [u]
    # trailing slash
    if not u.endswith("/"):
        out.append(u + "/")
    # protocol swap
    if u.startswith("http://"):
        out.append("https://" + u[len("http://"):])
    elif u.startswith("https://"):
        out.append("http://" + u[len("https://"):])
    # ensure https:// if missing
    if not u.startswith("http"):
        out.append("https://" + u)
    # dedupe
    uniq = []
    seen = set()
    for x in out:
        if x not in seen:
            uniq.append(x)
            seen.add(x)
    return uniq

def robust_goto(page, brand_key: str, url: str, timeout_ms: int) -> Tuple[bool, str, str]:
    """
    return (ok, resolved_url, err)
    - 기본 url 변형 + ALT_URLS 순회
    """
    candidates = []
    candidates += _url_variants(url)
    for alt in ALT_URLS.get(brand_key, []) or []:
        candidates += _url_variants(alt)

    # 최종 dedupe
    seen = set()
    cand2 = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            cand2.append(c)

    last_err = ""
    for u in cand2[:8]:
        try:
            page.goto(u, timeout=timeout_ms, wait_until="domcontentloaded")
            try:
                page.wait_for_load_state("networkidle", timeout=8000)
            except Exception:
                pass
            page.wait_for_timeout(WAIT_AFTER_GOTO_MS)
            return True, (page.url or u), ""
        except Exception as e:
            last_err = str(e)
            continue
    return False, "", last_err


# =====================================================
# Brand dispatcher
# =====================================================
def crawl_brand(context, page, bk, bn, url, mode, date_s, mx, errlog_path: str, brand_stats: Dict[str, Any]):
    print(f"\n[*] Analyzing: {bn} ({url})", flush=True)

    resolved_url = ""

    with stage(bn, "goto"):
        ok, resolved_url, err = robust_goto(page, bk, url, NAV_TIMEOUT_MS)
        if not ok:
            append_error_log(errlog_path, bk, "goto", err, brand_name=bn, error_type="goto_fail", extra={"url": url})
            brand_stats[bk]["goto_fail"] += 1
            return [], resolved_url

    with stage(bn, f"extract ({mode})"):
        close_common_popups(page)
        try:
            if mode == "tnf_slick":
                rows = tnf_slick(context, page, resolved_url or url, bk, bn, date_s, mx)
            elif mode == "nepa_static":
                rows = nepa_static(context, page, resolved_url or url, bk, bn, date_s, mx)
            elif mode == "patagonia_static_hero":
                rows = patagonia_static_hero(context, page, resolved_url or url, bk, bn, date_s)
            elif mode == "blackyak_swiper":
                rows = blackyak_swiper(context, page, resolved_url or url, bk, bn, date_s, mx)
            elif mode == "discovery_swiper":
                rows = discovery_swiper(context, page, resolved_url or url, bk, bn, date_s, mx)
            elif mode == "hero_slider":
                rows = hero_slider(context, page, resolved_url or url, bk, bn, date_s, mx)
            elif mode == "hero_sections":
                rows = hero_sections(context, page, resolved_url or url, bk, bn, date_s, mx)
            else:
                rows = generic_top_banners(context, page, resolved_url or url, bk, bn, date_s, mx)
        except Exception as e:
            append_error_log(errlog_path, bk, "extract", str(e), brand_name=bn, error_type="extract_fail", extra={"mode": mode, "traceback": traceback.format_exc()})
            brand_stats[bk]["extract_fail"] += 1
            rows = []


    # TNF / Arc'teryx / K2 / 기타 섹션형 브랜드는 deep scan 결과도 병합해서 누락 방지
    if bk in DEEP_SCAN_BRANDS:
        try:
            extra_rows = deep_section_banners(context, page, resolved_url or url, bk, bn, date_s, mx, y_max=SECTION_SCAN_Y_MAX)
            rows = dedupe_brand_rows(rows + extra_rows)
        except Exception:
            pass

    # resolved_url 기록
    for r in rows:
        r.brand_resolved_url = resolved_url or url

    return rows, resolved_url


# =====================================================
# Main
# =====================================================
def _init_brand_stats() -> Dict[str, Any]:
    st = {}
    for bk, bn, *_ in BRANDS:
        st[bk] = {
            "brand_name": bn,
            "banners": 0,
            "img_ok": 0,
            "img_cached": 0,
            "img_no_url": 0,
            "img_blocked": 0,
            "img_transport_fail": 0,
            "img_other_fail": 0,
            "fallback_count": 0,
            "low_confidence_count": 0,
            "landing_meta_ok": 0,
            "landing_meta_fail": 0,
            "goto_fail": 0,
            "extract_fail": 0,
            "empty_result": 0,
            "resolved_url": "",
        }
    return st

def _accum_brand_stats(brand_stats: Dict[str, Any], bk: str, rows: List[Banner], resolved_url: str):
    s = brand_stats.get(bk) or {}
    s["resolved_url"] = resolved_url or s.get("resolved_url","")
    s["banners"] += len(rows)
    for b in rows:
        bucket = classify_img_status(b.img_status or "")
        if bucket:
            s[bucket] = int(s.get(bucket, 0) or 0) + 1
        if b.is_fallback:
            s["fallback_count"] += 1
        if float(b.confidence_score or 0.0) < 0.7:
            s["low_confidence_count"] += 1
        if (b.landing_text_status or "") in {"", "skipped", "no_href"}:
            pass
        elif b.landing_text_status == "ok":
            s["landing_meta_ok"] += 1
        else:
            s["landing_meta_fail"] += 1
    brand_stats[bk] = s

def main():
    global PROG, CAMPAIGN_CACHE, CAMPAIGN_META_CACHE

    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(ASSET_DIR, exist_ok=True)
    os.makedirs(SNAP_DIR, exist_ok=True)
    os.makedirs(CACHE_DIR, exist_ok=True)

    now = kst_now()
    date_s = now.strftime("%Y-%m-%d")
    ts = now.strftime("%Y%m%d_%H%M%S")

    today_snap = os.path.join(SNAP_DIR, f"hero_main_banners_{date_s}.csv")
    report_csv = os.path.join(OUT_DIR, f"hero_main_banners_{ts}.csv")
    report_html = os.path.join(OUT_DIR, "hero_main.html")
    errlog_path = os.path.join(SNAP_DIR, f"errors_{date_s}.jsonl")

    # load campaign cache
    CAMPAIGN_CACHE = load_campaign_cache()
    CAMPAIGN_META_CACHE = load_campaign_meta_cache()

    rows: List[Banner] = []
    brand_stats = _init_brand_stats()

    PROG = Progress(total=len(BRANDS))
    PROG.set_stage(stage="start")
    PROG.newline()

    with sync_playwright() as pw:
        browser, context = launch(pw)

        for bk, bn, url, mode, mx in BRANDS:
            page = None
            attempt = 0
            brand_rows: List[Banner] = []
            resolved_url = ""

            while attempt < 2:
                attempt += 1
                try:
                    if PROG:
                        PROG.set_stage(brand=bn, stage=f"open_page (try {attempt}/2)")
                    page = context.new_page()

                    got, resolved_url = crawl_brand(context, page, bk, bn, url, mode, date_s, mx, errlog_path, brand_stats)
                    brand_rows.extend(got)

                    try:
                        page.close()
                    except Exception:
                        pass

                    # 예외 없이 왔다면 retry는 종료
                    break

                except (PWTimeoutError, PWError, Exception) as e:
                    append_error_log(errlog_path, bk, "brand_run", str(e), brand_name=bn, error_type="brand_run_error", attempt=attempt, extra={"traceback": traceback.format_exc()})

                    try:
                        if page:
                            page.close()
                    except Exception:
                        pass
                    if is_closed_error(e):
                        try:
                            browser.close()
                        except Exception:
                            pass
                        browser, context = launch(pw)
                    if attempt < 2:
                        print(" - Relaunching browser and retrying once...", flush=True)
                        continue
                    break

            # ✅ 브랜드 단위 dedupe + rank 재정렬
            brand_rows = dedupe_brand_rows(brand_rows)

            # ✅ 날짜 enrich (rank1 only + cache)
            with stage(bn, "fetch_campaign_meta"):
                enrich_campaign_meta_for_rows(context, brand_rows)

            # ✅ resolved url 기록(혹시 빈 리스트라도 stats엔 남김)
            brand_stats[bk]["resolved_url"] = resolved_url or brand_stats[bk].get("resolved_url","")

            # ✅ brand_ok 기준 강화: 배너 1개 이상이어야 OK
            brand_ok = (len(brand_rows) > 0)
            if not brand_ok:
                brand_stats[bk]["empty_result"] += 1

            # stats accumulate
            _accum_brand_stats(brand_stats, bk, brand_rows, resolved_url)

            rows.extend(brand_rows)

            if PROG:
                PROG.step_done(ok=brand_ok)
                PROG.newline()

        # save campaign cache
        save_campaign_cache(CAMPAIGN_CACHE)
        save_campaign_meta_cache(CAMPAIGN_META_CACHE)

        # write outputs
        with stage("OUTPUT", "write_csv/html/diff"):
            write_csv(today_snap, rows)
            write_csv(report_csv, rows)


            prev_csv = _latest_prev_snapshot(date_s)
            daily_changes = {}
            if prev_csv and os.path.exists(prev_csv):
                daily_changes = build_changes(prev_csv, today_snap)
                ch_path = os.path.join(OUT_DIR, f"hero_changes_{date_s}.json")
                write_json(ch_path, daily_changes)
            else:
                daily_changes = {"summary": {"added": 0, "removed": 0, "changed": 0}, "prev_csv": "", "curr_csv": os.path.basename(today_snap)}

            recent_changes = build_recent_changes(today_snap, date_s, days=RECENT_CHANGE_DAYS)
            recent_path = os.path.join(OUT_DIR, f"hero_changes_recent_{RECENT_CHANGE_DAYS}d_{date_s}.json")
            write_json(recent_path, recent_changes)
            alerts_payload = build_alerts(recent_changes)
            alert_path = os.path.join(OUT_DIR, f"hero_alerts_{date_s}.json")
            write_json(alert_path, alerts_payload)
            slack_status = send_slack_alerts(alerts_payload)
            email_status = send_email_alerts(alerts_payload)

            write_dashboard_html(report_html, rows, changes=recent_changes)

            # summary.json for Hub
            out_dir = os.path.dirname(report_html) or "."
            collection_health = build_collection_health(rows)
            img_ok = int(sum(1 for b in rows if classify_img_status(b.img_status) == "img_ok"))
            img_cached = int(sum(1 for b in rows if classify_img_status(b.img_status) == "img_cached"))
            img_no_url = int(sum(1 for b in rows if classify_img_status(b.img_status) == "img_no_url"))
            img_blocked = int(sum(1 for b in rows if classify_img_status(b.img_status) == "img_blocked"))
            img_transport_fail = int(sum(1 for b in rows if classify_img_status(b.img_status) == "img_transport_fail"))
            img_other_fail = int(sum(1 for b in rows if classify_img_status(b.img_status) == "img_other_fail"))
            fallback_count = int(sum(1 for b in rows if b.is_fallback))
            low_confidence_count = int(sum(1 for b in rows if float(b.confidence_score or 0.0) < 0.7))
            landing_meta_ok = int(collection_health.get("landing_meta_ok", 0) or 0)
            landing_meta_fail = int(collection_health.get("landing_meta_fail", 0) or 0)

            _write_summary_json(out_dir, "hero_main", {
                "generated_at": _now_kst_str(),
                "period": f"조회 기간: 실행 시점 스냅샷(KST) · 전회 실행 대비(변경 감지) · {kst_now().strftime('%Y-%m-%d %H:%M')} KST",
                "html": os.path.basename(report_html),
                "csv": os.path.basename(report_csv),
                "snapshot_csv": os.path.basename(today_snap),
                "brands": int(len(set([b.brand_key for b in rows]))),
                "banners": int(len(rows)),
                "img_ok": img_ok,
                "img_cached": img_cached,
                "img_no_url": img_no_url,
                "img_blocked": img_blocked,
                "img_transport_fail": img_transport_fail,
                "img_other_fail": img_other_fail,
                "fallback_count": fallback_count,
                "low_confidence_count": low_confidence_count,
                "landing_meta_ok": landing_meta_ok,
                "landing_meta_fail": landing_meta_fail,
                "changes_daily": (daily_changes.get("summary") if daily_changes else {}),
                "changes_recent": (recent_changes.get("summary") if recent_changes else {}),
                "alerts": alerts_payload,
                "collection_health": collection_health,
                "notification_status": {
                    "slack": slack_status,
                    "email": email_status,
                },
                "brand_stats": brand_stats,
                "config": {
                    "HTML_USE_ABSOLUTE_FILE_URL": bool(HTML_USE_ABSOLUTE_FILE_URL),
                    "FETCH_CAMPAIGN_DATES": bool(FETCH_CAMPAIGN_DATES),
                    "DATE_FETCH_RANK1_ONLY": bool(DATE_FETCH_RANK1_ONLY),
                    "CAMPAIGN_DATE_CACHE_TTL_DAYS": int(CAMPAIGN_DATE_CACHE_TTL_DAYS),
                    "FETCH_CAMPAIGN_META": bool(FETCH_CAMPAIGN_META),
                    "CAMPAIGN_META_RANK_LIMIT": int(CAMPAIGN_META_RANK_LIMIT),
                    "CAMPAIGN_META_CACHE_TTL_DAYS": int(CAMPAIGN_META_CACHE_TTL_DAYS),
                    "ENABLE_OCR": bool(ENABLE_OCR),
                }
            })

        print(f"\n[SNAP CSV] {today_snap}", flush=True)
        print(f"[CSV] {report_csv}", flush=True)
        print(f"[HTML] {report_html}", flush=True)
        print(f"[ASSET_DIR] {os.path.abspath(ASSET_DIR)}", flush=True)
        print(f"[ERROR_LOG] {errlog_path}", flush=True)
        print(f"[HTML_USE_ABSOLUTE_FILE_URL] {HTML_USE_ABSOLUTE_FILE_URL}", flush=True)
        print(f"[FETCH_CAMPAIGN_META] {FETCH_CAMPAIGN_META} (rank_limit={CAMPAIGN_META_RANK_LIMIT})", flush=True)
        print(f"[ALERT_NOTIFY] slack={slack_status} email={email_status}", flush=True)

        try:
            browser.close()
        except Exception:
            pass

    if not os.path.exists(report_html):
        print(f"[FATAL] HTML not created: {report_html}", flush=True)
        sys.exit(1)

    print("✅ hero_main.html generated successfully", flush=True)
    sys.exit(0)


# global campaign cache
CAMPAIGN_CACHE: Dict[str, Any] = {}
CAMPAIGN_META_CACHE: Dict[str, Any] = {}

if __name__ == "__main__":
    main()
