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

import os, re, csv, hashlib, urllib.parse, sys, time, json, traceback
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple, Dict, Any
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
    from PIL import Image
    from io import BytesIO
    PIL_OK = True
except Exception:
    PIL_OK = False


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
    ("kolonmall", "Kolon Mall", "https://www.kolonmall.com/", "hero_sections", DEFAULT_MAX_ITEMS),

    ("k2", "K2", "https://www.k-village.co.kr/K2", "hero_sections", DEFAULT_MAX_ITEMS),
    ("montbell", "Montbell", "https://www.montbell.co.kr/", "hero_sections", DEFAULT_MAX_ITEMS),
    ("eider", "Eider", "https://www.eider.co.kr/", "hero_sections", DEFAULT_MAX_ITEMS),

    ("millet", "Millet", "https://www.millet.co.kr/", "hero_sections", DEFAULT_MAX_ITEMS),
]

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

    # 운영/디버깅
    brand_resolved_url: str = ""


# =====================================================
# Global caches / meta
# =====================================================
IMG_URL_CACHE: Dict[str, str] = {}       # normalize_img_url(img_url) -> local filename
IMG_META: Dict[str, Tuple[int, int, int]] = {}  # local filename -> (w,h,bytes)


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
    rows_sorted = sorted(rows, key=lambda x: x.rank)

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

def enrich_dates_for_rows(context, rows: List[Banner]) -> None:
    if not FETCH_CAMPAIGN_DATES:
        return
    for b in rows:
        if DATE_FETCH_RANK1_ONLY and b.rank != 1:
            continue
        if not b.href_clean and b.href:
            b.href_clean = normalize_href(b.href)
        h = b.href_clean or b.href
        if not h:
            continue
        s, e = fetch_campaign_dates(context, h)
        b.plan_start = s
        b.plan_end = e


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

        b = Banner(date_s, brand_key, brand_name, rank, title, href, img_url, img_local)
        b.href_clean = normalize_href(href)
        b.img_status = "ok" if img_local else st

        if img_local and img_local in IMG_META:
            w, h, sz = IMG_META[img_local]
            b.img_w, b.img_h, b.img_bytes = w, h, sz

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

        b = Banner(date_s, brand_key, brand_name, rank, title, href, img_url, img_local)
        b.href_clean = normalize_href(href)
        b.img_status = "ok" if img_local else st
        if img_local and img_local in IMG_META:
            w, h, sz = IMG_META[img_local]
            b.img_w, b.img_h, b.img_bytes = w, h, sz
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

        b = Banner(date_s, brand_key, brand_name, rank, title, href, img_url, img_local)
        b.href_clean = normalize_href(href)
        b.img_status = "ok" if img_local else st
        if img_local and img_local in IMG_META:
            w, h, sz = IMG_META[img_local]
            b.img_w, b.img_h, b.img_bytes = w, h, sz
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

        b = Banner(date_s, brand_key, brand_name, rank, title, href, img_url, img_local)
        b.href_clean = normalize_href(href)
        b.img_status = "ok" if img_local else st
        if img_local and img_local in IMG_META:
            w, h, sz = IMG_META[img_local]
            b.img_w, b.img_h, b.img_bytes = w, h, sz
        return b

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

    b = Banner(date_s, brand_key, brand_name, 1, title, best_href, best_img, img_local)
    b.href_clean = normalize_href(best_href)
    b.img_status = "ok" if img_local else st
    if img_local and img_local in IMG_META:
        w, h, sz = IMG_META[img_local]
        b.img_w, b.img_h, b.img_bytes = w, h, sz
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
        b = Banner(date_s, brand_key, brand_name, rank, title, href, img_url, img_local)
        b.href_clean = normalize_href(href)
        b.img_status = 'ok' if img_local else st
        if img_local and img_local in IMG_META:
            w, h, sz = IMG_META[img_local]
            b.img_w, b.img_h, b.img_bytes = w, h, sz
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

            b = Banner(date_s, brand_key, brand_name, rank, title, href, img_url, img_local)
            b.href_clean = normalize_href(href)
            b.img_status = "ok" if img_local else st

            if img_local and img_local in IMG_META:
                w, h, sz = IMG_META[img_local]
                b.img_w, b.img_h, b.img_bytes = w, h, sz

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

            b = Banner(date_s, brand_key, brand_name, rank, title, href, img_url, img_local)
            b.href_clean = normalize_href(href)
            b.img_status = "ok" if img_local else st
            if img_local and img_local in IMG_META:
                w, h, sz = IMG_META[img_local]
                b.img_w, b.img_h, b.img_bytes = w, h, sz
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

        b = Banner(date_s, brand_key, brand_name, rank, title, href, img_url, img_local)
        b.href_clean = normalize_href(href)
        b.img_status = "ok" if img_local else st
        if img_local and img_local in IMG_META:
            w, h, sz = IMG_META[img_local]
            b.img_w, b.img_h, b.img_bytes = w, h, sz
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
            "brand_resolved_url",
        ])
        for b in rows:
            w.writerow([
                b.date,b.brand_key,b.brand_name,b.rank,
                b.title,b.href,b.href_clean,
                b.plan_start,b.plan_end,
                b.img_url,b.img_local,b.img_status,
                b.img_w,b.img_h,b.img_bytes,
                b.brand_resolved_url,
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


def build_recent_changes(curr_csv: str, date_s: str, days: int = RECENT_CHANGE_DAYS) -> Dict[str, Any]:
    """
    최근 N일 내 snapshot 변화 이력 요약.
    - 브랜드 중복은 하나로 묶음
    - 연속 스냅샷 pair 를 훑어서 최근 7일 내 변경 이력 집계
    """
    files = _recent_snapshot_files(date_s, days=days)
    if curr_csv and os.path.exists(curr_csv) and curr_csv not in files:
        files.append(curr_csv)
        files = sorted(set(files))

    by_brand: Dict[str, Dict[str, Any]] = {}
    total_added = total_removed = total_changed = total_events = 0
    pairs = []

    for prev_csv, next_csv in zip(files, files[1:]):
        diff = build_changes(prev_csv, next_csv)
        sm = diff.get("summary") or {}
        total_added += int(sm.get("added", 0) or 0)
        total_removed += int(sm.get("removed", 0) or 0)
        total_changed += int(sm.get("changed", 0) or 0)
        total_events += int(sm.get("added", 0) or 0) + int(sm.get("removed", 0) or 0) + int(sm.get("changed", 0) or 0)
        pairs.append({
            "prev_csv": os.path.basename(prev_csv),
            "curr_csv": os.path.basename(next_csv),
            "summary": sm,
        })

        for r in diff.get("added", []) or []:
            bk = r.get("brand_key", "")
            if not bk:
                continue
            info = by_brand.setdefault(bk, {
                "brand_key": bk,
                "brand_name": r.get("brand_name", bk),
                "added": 0,
                "removed": 0,
                "changed": 0,
                "events": 0,
                "last_title": "",
                "last_rank": "",
                "last_seen_csv": os.path.basename(next_csv),
            })
            info["added"] += 1
            info["events"] += 1
            info["last_title"] = r.get("title", "") or info.get("last_title", "")
            info["last_rank"] = r.get("rank", "") or info.get("last_rank", "")
            info["last_seen_csv"] = os.path.basename(next_csv)

        for r in diff.get("removed", []) or []:
            bk = r.get("brand_key", "")
            if not bk:
                continue
            info = by_brand.setdefault(bk, {
                "brand_key": bk,
                "brand_name": r.get("brand_name", bk),
                "added": 0,
                "removed": 0,
                "changed": 0,
                "events": 0,
                "last_title": "",
                "last_rank": "",
                "last_seen_csv": os.path.basename(next_csv),
            })
            info["removed"] += 1
            info["events"] += 1
            if not info.get("last_title"):
                info["last_title"] = r.get("title", "")
            if not info.get("last_rank"):
                info["last_rank"] = r.get("rank", "")
            info["last_seen_csv"] = os.path.basename(next_csv)

        for x in diff.get("changed", []) or []:
            curr = x.get("curr") or {}
            bk = curr.get("brand_key", "")
            if not bk:
                continue
            info = by_brand.setdefault(bk, {
                "brand_key": bk,
                "brand_name": curr.get("brand_name", bk),
                "added": 0,
                "removed": 0,
                "changed": 0,
                "events": 0,
                "last_title": "",
                "last_rank": "",
                "last_seen_csv": os.path.basename(next_csv),
            })
            info["changed"] += 1
            info["events"] += 1
            info["last_title"] = curr.get("title", "") or info.get("last_title", "")
            info["last_rank"] = curr.get("rank", "") or info.get("last_rank", "")
            info["last_seen_csv"] = os.path.basename(next_csv)

    brands = sorted(by_brand.values(), key=lambda x: (-int(x.get("events", 0) or 0), x.get("brand_name", "")))
    return {
        "window_days": int(days),
        "curr_csv": os.path.basename(curr_csv) if curr_csv else "",
        "summary": {
            "brands_touched": len(brands),
            "events": total_events,
            "added": total_added,
            "removed": total_removed,
            "changed": total_changed,
        },
        "brands": brands[:100],
        "pairs": pairs[: max(len(pairs), 1)],
    }

def write_json(path: str, obj: Any):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def write_html(path: str, rows: List[Banner], changes: Optional[Dict[str, Any]] = None):
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
            # error log
            with open(errlog_path, "a", encoding="utf-8") as ef:
                ef.write(f"[GOTO_FAIL] {bk} {bn} url={url} err={err}\n")
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
            with open(errlog_path, "a", encoding="utf-8") as ef:
                ef.write(f"[EXTRACT_FAIL] {bk} {bn} mode={mode} err={e}\n")
                ef.write(traceback.format_exc() + "\n")
            brand_stats[bk]["extract_fail"] += 1
            rows = []


    # TNF / Arc'teryx / K2 / 기타 섹션형 브랜드는 deep scan 결과도 병합해서 누락 방지
    if bk in {"tnf", "arcteryx", "k2", "salomon", "snowpeak", "natgeo", "kolonsport", "kolonmall", "montbell", "eider", "millet"}:
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
            "img_fail": 0,
            "img_403": 0,
            "img_429": 0,
            "img_blocked_html": 0,
            "no_url": 0,
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
        st = (b.img_status or "").strip()
        if st == "ok":
            s["img_ok"] += 1
        elif st == "cached":
            s["img_cached"] += 1
        elif st == "no_url":
            s["no_url"] += 1
        elif st == "http_403":
            s["img_403"] += 1
            s["img_fail"] += 1
        elif st == "http_429":
            s["img_429"] += 1
            s["img_fail"] += 1
        elif st == "blocked_html":
            s["img_blocked_html"] += 1
            s["img_fail"] += 1
        elif st:
            s["img_fail"] += 1
    brand_stats[bk] = s

def main():
    global PROG, CAMPAIGN_CACHE

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
    errlog_path = os.path.join(SNAP_DIR, f"errors_{date_s}.log")

    # load campaign cache
    CAMPAIGN_CACHE = load_campaign_cache()

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
                    # error log
                    with open(errlog_path, "a", encoding="utf-8") as ef:
                        ef.write(f"[BRAND_RUN_ERROR] {bk} {bn} try={attempt} err={e}\n")
                        ef.write(traceback.format_exc() + "\n")

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
            with stage(bn, "fetch_dates"):
                enrich_dates_for_rows(context, brand_rows)

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

            write_html(report_html, rows, changes=recent_changes)

            # summary.json for Hub
            out_dir = os.path.dirname(report_html) or "."
            no_image_true = int(sum(1 for b in rows if (not b.img_url and not b.img_local)))
            img_download_fail = int(sum(1 for b in rows if (b.img_status in {"download_fail","exception"} or (b.img_status or "").startswith("http_"))))
            img_blocked = int(sum(1 for b in rows if b.img_status in {"http_403","http_429","blocked_html"}))
            img_cached = int(sum(1 for b in rows if b.img_status == "cached"))

            _write_summary_json(out_dir, "hero_main", {
                "generated_at": _now_kst_str(),
                "period": f"조회 기간: 실행 시점 스냅샷(KST) · 전회 실행 대비(변경 감지) · {kst_now().strftime('%Y-%m-%d %H:%M')} KST",
                "html": os.path.basename(report_html),
                "csv": os.path.basename(report_csv),
                "snapshot_csv": os.path.basename(today_snap),
                "brands": int(len(set([b.brand_key for b in rows]))),
                "banners": int(len(rows)),
                "no_image": no_image_true,
                "img_download_fail": img_download_fail,
                "img_blocked": img_blocked,
                "img_cached": img_cached,
                "changes_daily": (daily_changes.get("summary") if daily_changes else {}),
                "changes_recent": (recent_changes.get("summary") if recent_changes else {}),
                "brand_stats": brand_stats,
                "config": {
                    "HTML_USE_ABSOLUTE_FILE_URL": bool(HTML_USE_ABSOLUTE_FILE_URL),
                    "FETCH_CAMPAIGN_DATES": bool(FETCH_CAMPAIGN_DATES),
                    "DATE_FETCH_RANK1_ONLY": bool(DATE_FETCH_RANK1_ONLY),
                    "CAMPAIGN_DATE_CACHE_TTL_DAYS": int(CAMPAIGN_DATE_CACHE_TTL_DAYS),
                }
            })

        print(f"\n[SNAP CSV] {today_snap}", flush=True)
        print(f"[CSV] {report_csv}", flush=True)
        print(f"[HTML] {report_html}", flush=True)
        print(f"[ASSET_DIR] {os.path.abspath(ASSET_DIR)}", flush=True)
        print(f"[ERROR_LOG] {errlog_path}", flush=True)
        print(f"[HTML_USE_ABSOLUTE_FILE_URL] {HTML_USE_ABSOLUTE_FILE_URL}", flush=True)
        print(f"[FETCH_CAMPAIGN_DATES] {FETCH_CAMPAIGN_DATES} (rank1_only={DATE_FETCH_RANK1_ONLY})", flush=True)

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

if __name__ == "__main__":
    main()
