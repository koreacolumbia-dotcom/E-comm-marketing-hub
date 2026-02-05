#!/usr/bin/env python3
# -*- coding: utf-8 -*-

print("HERO SCRIPT NEW VERSION RUNNING", flush=True)

"""
hero_main_top3_final_PRETTY_WHITE_FINAL_FIXED.py (+ PROGRESS + DEDUPE + DATES + IMG_META)

추가된 개선:
- (중복 다운로드 방지) img_url 단위 다운로드 캐시(IMG_URL_CACHE) 적용
- (중복 배너 제거) 브랜드 내에서 동일 캠페인(정규화 href) / 동일 이미지(img_url) 중복 제거
- (이미지 메타) 저장된 로컬 이미지의 width/height/bytes를 수집해서 CSV/HTML에 반영
- (기획전 이름 정리) phpThumb/파일명/쿼리 기반 제목은 배제하고 텍스트 우선 + 정리 규칙 추가
- (기획전 시작일/기간) href 페이지에서 날짜 패턴 탐색(옵션: FETCH_CAMPAIGN_DATES=1 기본)
- (No Image 원인 기록) img_status 컬럼으로 추적 가능
"""

import os, re, csv, hashlib, urllib.parse, sys, time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Tuple, Dict
import requests
from pathlib import Path
from contextlib import contextmanager

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError, Error as PWError

# Pillow (image resize + meta)
try:
    from PIL import Image
    from io import BytesIO
    PIL_OK = True
except Exception:
    PIL_OK = False


# -----------------------------------------------------
# Progress (console)
# -----------------------------------------------------
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


# -----------------------------------------------------
# ENV / CONFIG
# -----------------------------------------------------
OUT_DIR = os.environ.get("OUT_DIR", "reports")
ASSET_DIR = os.path.join(OUT_DIR, "assets")
SNAP_DIR = os.path.join(OUT_DIR, "snapshots")

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


# -----------------------------------------------------
# Brands
# -----------------------------------------------------
BRANDS = [
    ("tnf", "The North Face", "https://www.thenorthfacekorea.co.kr/", "tnf_slick", 3),
    ("nepa", "NEPA", "https://www.nplus.co.kr/main/main.asp?NaPm=ct%3Dmk68nx7b%7Cci%3Dcheckout%7Ctr%3Dds%7Ctrx%3Dnull%7Chk%3D2eb6245a50cfbdfae4c4e3e806691658fa257fa9", "nepa_static", 3),
    ("patagonia", "Patagonia", "https://www.patagonia.co.kr/", "patagonia_static_hero", 1),
    ("blackyak", "Black Yak", "https://www.byn.kr/blackyak?utm_source=naver&utm_medium=BSA&utm_campaign=BY_EC_250828_hyperpulse_PERF_NV_BSA&utm_content=PC_BY_EC_naver_BSA_250828_hyperpulse_homelink&utm_term=%EB%B8%94%EB%9E%99%EC%95%BC%ED%81%AC&NaPm=ct%3Dmhwxwfpl%7Cci%3DERbd1ca7ea%2Dc04a%2D11f0%2D935c%2Df6a058b83a4c%7Ctr%3Dbrnd%7Chk%3D07dc9aedc63b17fba956801b4aa26232c93036a5%7Cnacn%3DBOWtB0gPQcOt", "blackyak_swiper", 3),
    ("discovery", "Discovery", "https://www.discovery-expedition.com/?gf=A", "discovery_swiper", 3),

    ("arcteryx", "Arc'teryx", "https://www.arcteryx.co.kr/", "generic", 3),
    ("salomon", "Salomon", "https://salomon.co.kr/", "generic", 3),
    ("snowpeak", "Snow Peak", "https://www.snowpeakstore.co.kr/", "generic", 3),
    ("natgeo", "National Geographic", "https://www.natgeokorea.com/", "generic", 3),
    ("kolonsport", "Kolon Sport", "https://www.kolonsport.com/", "generic", 3),
    ("k2", "K2", "https://www.k2.co.kr/", "generic", 3),
    ("montbell", "Montbell", "https://www.montbell.co.kr/", "generic", 3),
    ("eider", "Eider", "https://www.eider.co.kr/", "generic", 3),
]


# -----------------------------------------------------
# Data model
# -----------------------------------------------------
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

    # added
    href_clean: str = ""
    plan_start: str = ""
    plan_end: str = ""
    img_w: int = 0
    img_h: int = 0
    img_bytes: int = 0
    img_status: str = ""   # ok / download_fail / no_url / unknown


# -----------------------------------------------------
# Global caches / meta
# -----------------------------------------------------
IMG_URL_CACHE: Dict[str, str] = {}       # img_url -> local filename
IMG_META: Dict[str, Tuple[int, int, int]] = {}  # local filename -> (w,h,bytes)


# -----------------------------------------------------
# Util
# -----------------------------------------------------
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

def is_junk_title(t: str) -> bool:
    tl = (t or "").strip().lower()
    if not tl:
        return True
    # 파일명/쿼리 기반 or 의미 없는 제목들
    junk_tokens = [
        "phpthumb", "src=/uploads", "w=1200", "q=80", "f=webp",
        ".jpg", ".jpeg", ".png", ".webp", "data:image",
        "main_mc", "kakaotalk_", "img_", "banner_", "thumb",
    ]
    if any(tok in tl for tok in junk_tokens):
        # 단, 정상 문장인데 확장자만 포함된 케이스는 제외하고 싶지만
        # 여기선 보수적으로 junk 처리
        return True
    # 해시처럼 생긴 값만 있는 경우
    if re.fullmatch(r"[a-f0-9_\-]{18,}", tl):
        return True
    return False

def clean_campaign_title(t: str) -> str:
    t = norm_ws(t)
    t = t.strip('"').strip("'").strip()
    # 앞뒤에 파일명 느낌 제거
    t = re.sub(r'^\s*["\']?|["\']?\s*$', '', t)
    # 너무 길면 컷
    return t[:90]

def choose_title(*cands: str) -> str:
    c = [norm_ws(x) for x in cands if norm_ws(x)]
    c = [x for x in c if x.lower() not in {"next", "prev", "이전", "다음", "닫기"} and len(x) > 1]
    # junk 후보 제거
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
        cnt = min(imgs.count(), 8)
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
        cnt = min(bg_nodes.count(), 60)
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
        cnt = min(nodes.count(), 60)
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


# -----------------------------------------------------
# (옵션) 로컬 파일을 HTML에서 깨지지 않게 file:// URL로 변환
# -----------------------------------------------------
def to_file_url(path: str) -> str:
    try:
        p = Path(path).resolve()
        return p.as_uri()
    except Exception:
        ap = os.path.abspath(path).replace("\\", "/")
        if not ap.startswith("/"):
            return "file:///" + ap
        return "file://" + ap


# -----------------------------------------------------
# Image download + resize (+ cache + meta)
# -----------------------------------------------------
def download_bytes(url: str, referer: str = "") -> Optional[bytes]:
    try:
        headers = {"User-Agent": USER_AGENT}
        if referer:
            headers["Referer"] = referer
        r = requests.get(url, headers=headers, timeout=25)
        if r.status_code != 200 or not r.content:
            return None
        return r.content
    except Exception:
        return None

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

def save_and_resize_image(img_url: str, brand_key: str, rank: int, referer: str = "") -> Tuple[str, str]:
    """
    returns: (local_filename, status)
      status: ok / download_fail / no_url / cached
    """
    global PROG, IMG_URL_CACHE

    if not img_url:
        if PROG: PROG.add_img(False)
        return "", "no_url"

    # cache hit
    if img_url in IMG_URL_CACHE and IMG_URL_CACHE[img_url]:
        return IMG_URL_CACHE[img_url], "cached"

    os.makedirs(ASSET_DIR, exist_ok=True)
    out_ext = ".jpg" if PIL_OK else guess_ext(img_url)
    fname = safe_filename(f"{brand_key}_{rank}_{sha1(img_url)}", out_ext)
    out_path = os.path.join(ASSET_DIR, fname)

    content = download_bytes(img_url, referer=referer)
    if content is None:
        if PROG: PROG.add_img(False)
        return "", "download_fail"

    # save (+ resize)
    if not PIL_OK:
        try:
            with open(out_path, "wb") as f:
                f.write(content)
            _record_img_meta(out_path, fname)
            IMG_URL_CACHE[img_url] = fname
            if PROG: PROG.add_img(True)
            return fname, "ok"
        except Exception:
            if PROG: PROG.add_img(False)
            return "", "download_fail"

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
        IMG_URL_CACHE[img_url] = fname
        if PROG: PROG.add_img(True)
        return fname, "ok"
    except Exception:
        try:
            with open(out_path, "wb") as f:
                f.write(content)
            _record_img_meta(out_path, fname)
            IMG_URL_CACHE[img_url] = fname
            if PROG: PROG.add_img(True)
            return fname, "ok"
        except Exception:
            if PROG: PROG.add_img(False)
            return "", "download_fail"


# -----------------------------------------------------
# Playwright helpers
# -----------------------------------------------------
def close_common_popups(page) -> None:
    sels = [
        "button:has-text('닫기')", "button:has-text('Close')",
        "button:has-text('확인')", "button:has-text('동의')",
        "button:has-text('오늘 하루 보지 않기')",
        "button[aria-label*='close' i]", "button[aria-label*='닫기']",
        ".modal .close", ".popup .close", ".layer .close", ".btn-close",
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


# -----------------------------------------------------
# Campaign date extraction
# -----------------------------------------------------
DATE_PATTERNS = [
    # 2026.02.01 ~ 2026.02.10
    re.compile(r"(\d{4}[./-]\d{1,2}[./-]\d{1,2})\s*(?:~|∼|–|-|—)\s*(\d{4}[./-]\d{1,2}[./-]\d{1,2})"),
    # 2026.02.01 ~ 02.10 (end year omitted)
    re.compile(r"(\d{4})[./-](\d{1,2})[./-](\d{1,2})\s*(?:~|∼|–|-|—)\s*(\d{1,2})[./-](\d{1,2})"),
    # 2026-02-01부터 2026-02-10까지
    re.compile(r"(\d{4}[./-]\d{1,2}[./-]\d{1,2}).{0,12}?(?:부터|~|∼|–|-|—).{0,12}?(\d{4}[./-]\d{1,2}[./-]\d{1,2}).{0,8}?(?:까지)?"),
]

def _norm_date(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("/", "-").replace(".", "-")
    # 2026-2-5 -> 2026-02-05
    m = re.match(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if not m:
        return s
    y, mo, d = m.group(1), int(m.group(2)), int(m.group(3))
    return f"{y}-{mo:02d}-{d:02d}"

def extract_date_range_from_text(text: str) -> Tuple[str, str]:
    t = text or ""
    # whitespace normalize
    t = re.sub(r"\s+", " ", t)

    for pat in DATE_PATTERNS:
        m = pat.search(t)
        if not m:
            continue
        if pat.pattern.startswith(r"(\d{4})"):
            # pattern2: y mo d ~ mo d
            y = m.group(1)
            mo1, d1 = int(m.group(2)), int(m.group(3))
            mo2, d2 = int(m.group(4)), int(m.group(5))
            s = f"{y}-{mo1:02d}-{d1:02d}"
            e = f"{y}-{mo2:02d}-{d2:02d}"
            return _norm_date(s), _norm_date(e)

        s = _norm_date(m.group(1))
        e = _norm_date(m.group(2))
        return s, e

    # 못 찾으면 빈값
    return "", ""

def fetch_campaign_dates(context, href: str) -> Tuple[str, str]:
    """href 페이지에서 기간(시작/종료) 탐색"""
    if not href:
        return "", ""
    if not FETCH_CAMPAIGN_DATES:
        return "", ""

    p = None
    try:
        p = context.new_page()
        p.goto(href, timeout=DATE_FETCH_TIMEOUT_MS, wait_until="domcontentloaded")
        try:
            p.wait_for_load_state("networkidle", timeout=3000)
        except Exception:
            pass

        # 너무 무겁게 전체 DOM을 다 긁지 말고, body 텍스트 우선
        try:
            body_text = p.locator("body").inner_text(timeout=2000)
        except Exception:
            body_text = ""

        s, e = extract_date_range_from_text(body_text)

        # fallback: meta/og/ld+json
        if not s:
            try:
                html = p.content()
            except Exception:
                html = ""
            s2, e2 = extract_date_range_from_text(html)
            if s2:
                s, e = s2, e2

        return s, e
    except Exception:
        return "", ""
    finally:
        try:
            if p:
                p.close()
        except Exception:
            pass


# -----------------------------------------------------
# Extractors
# -----------------------------------------------------
def tnf_slick(page, base_url: str, brand_key: str, brand_name: str, date_s: str, max_items: int):
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
        key = (normalize_href(href), img_url)
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
        img_local, st = save_and_resize_image(img_url, brand_key, rank, referer=base_url) if img_url else ("", "no_url")

        b = Banner(date_s, brand_key, brand_name, rank, title, href, img_url, img_local)
        b.href_clean = normalize_href(href)
        b.img_status = "ok" if img_local else st

        if img_local and img_local in IMG_META:
            w, h, sz = IMG_META[img_local]
            b.img_w, b.img_h, b.img_bytes = w, h, sz

        out.append(b)
        rank += 1
    return out

def discovery_swiper(page, base_url: str, brand_key: str, brand_name: str, date_s: str, max_items: int):
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
    for i in range(min(n, 24)):
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

            if not href and not img_url:
                continue
            key = sha1("\n".join([normalize_href(href), img_url]))
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
        img_local, st = save_and_resize_image(img_url, brand_key, rank, referer=base_url) if img_url else ("", "no_url")

        b = Banner(date_s, brand_key, brand_name, rank, title, href, img_url, img_local)
        b.href_clean = normalize_href(href)
        b.img_status = "ok" if img_local else st

        if img_local and img_local in IMG_META:
            w, h, sz = IMG_META[img_local]
            b.img_w, b.img_h, b.img_bytes = w, h, sz

        out.append(b)
        print(f" #{rank}: {title}", flush=True)
        rank += 1

    return out

def blackyak_swiper(page, base_url: str, brand_key: str, brand_name: str, date_s: str, max_items: int):
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
        key = (normalize_href(href), img_url)
        if not href and not img_url:
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
        img_local, st = save_and_resize_image(img_url, brand_key, rank, referer=base_url) if img_url else ("", "no_url")

        b = Banner(date_s, brand_key, brand_name, rank, title, href, img_url, img_local)
        b.href_clean = normalize_href(href)
        b.img_status = "ok" if img_local else st

        if img_local and img_local in IMG_META:
            w, h, sz = IMG_META[img_local]
            b.img_w, b.img_h, b.img_bytes = w, h, sz

        out.append(b)
        rank += 1
    return out

def nepa_static(page, base_url: str, brand_key: str, brand_name: str, date_s: str, max_items: int):
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
        if not img_url and not href:
            return None

        img_local, st = save_and_resize_image(img_url, brand_key, rank, referer=base_url) if img_url else ("", "no_url")

        b = Banner(date_s, brand_key, brand_name, rank, title, href, img_url, img_local)
        b.href_clean = normalize_href(href)
        b.img_status = "ok" if img_local else st

        if img_local and img_local in IMG_META:
            w, h, sz = IMG_META[img_local]
            b.img_w, b.img_h, b.img_bytes = w, h, sz
        return b

    rank = 1
    for idx in range(1, 30):
        if rank > max_items:
            break
        b = extract_from_banner(idx, rank)
        if b:
            out.append(b)
            print(f" #{rank}: {b.title}", flush=True)
            rank += 1
        if idx >= 5 and len(out) >= max_items:
            break

    if not out:
        print(" - NEPA 배너를 못 찾았어요 (팝업/로봇체크/구조변경 가능). HEADLESS=0로 확인 추천", flush=True)
    return out

def patagonia_static_hero(page, base_url: str, brand_key: str, brand_name: str, date_s: str):
    wait_first_visible(page, ["header", "main", "section"], 12000)
    vw = page.viewport_size["width"] if page.viewport_size else 1440
    best_area = 0
    best_img = ""
    best_title = ""
    best_href = ""

    candidates = page.locator("section, div")
    cnt = min(candidates.count(), 220)
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

    if not best_img and not best_href:
        print(" - 상단 히어로 탐지 실패", flush=True)
        return []

    title = choose_title(best_title, urllib.parse.unquote((best_img or "").split("/")[-1]))
    img_local, st = save_and_resize_image(best_img, brand_key, 1, referer=base_url) if best_img else ("", "no_url")

    b = Banner(date_s, brand_key, brand_name, 1, title, best_href, best_img, img_local)
    b.href_clean = normalize_href(best_href)
    b.img_status = "ok" if img_local else st

    if img_local and img_local in IMG_META:
        w, h, sz = IMG_META[img_local]
        b.img_w, b.img_h, b.img_bytes = w, h, sz

    print(f" #1: {title}", flush=True)
    return [b]

def generic_top_banners(page_or_frame, base_url: str, brand_key: str, brand_name: str, date_s: str,
                        max_items: int, y_max: int = 1400):
    out: List[Banner] = []
    try:
        vw = page_or_frame.viewport_size["width"] if getattr(page_or_frame, "viewport_size", None) else 1440
    except Exception:
        vw = 1440

    # scroll nudges
    try:
        page_or_frame.evaluate("window.scrollTo(0, 150);")
        page_or_frame.wait_for_timeout(250)
        page_or_frame.evaluate("window.scrollTo(0, 600);")
        page_or_frame.wait_for_timeout(300)
        page_or_frame.evaluate("window.scrollTo(0, 0);")
        page_or_frame.wait_for_timeout(250)
    except Exception:
        pass

    try:
        candidates = page_or_frame.locator("a, section, div")
        cnt = min(candidates.count(), 420)
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
            if bb["width"] < vw * 0.55 or bb["height"] < 180:
                continue

            img_url = get_any_img_url(el, base_url)
            href = ""
            try:
                is_a = el.evaluate("e => e.tagName.toLowerCase()") == "a"
                a = el if is_a else el.locator("a[href]").first
                if a and a.count():
                    href = abs_url(base_url, a.get_attribute("href") or "")
            except Exception:
                pass

            if not img_url and not href:
                continue

            # local dedupe key (candidate-level)
            fp = sha1("\n".join([normalize_href(href), img_url or ""]))
            if fp in seen:
                continue
            seen.add(fp)

            title_txt = ""
            try:
                title_txt = norm_ws(el.locator("h1,h2,h3,strong,p").first.inner_text() or "")
            except Exception:
                pass
            title = choose_title(title_txt, get_any_alt_text(el))

            img_local, st = save_and_resize_image(img_url, brand_key, rank, referer=base_url) if img_url else ("", "no_url")

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


# -----------------------------------------------------
# Dedupe + enrich (dates)
# -----------------------------------------------------
def dedupe_brand_rows(rows: List[Banner]) -> List[Banner]:
    """브랜드 내 중복 제거: (href_clean 우선) / img_url 보조"""
    if not rows:
        return rows
    rows_sorted = sorted(rows, key=lambda x: x.rank)

    seen_href = set()
    seen_img = set()
    out = []
    for b in rows_sorted:
        hc = b.href_clean or normalize_href(b.href)
        iu = (b.img_url or "").strip()

        # href가 있으면 href 기준으로 강하게 dedupe
        if hc:
            if hc in seen_href:
                continue
            seen_href.add(hc)

        # href가 없거나 빈 경우엔 img_url로라도 dedupe
        if not hc and iu:
            if iu in seen_img:
                continue
            seen_img.add(iu)

        out.append(b)

    # rank 재정렬(1..N)
    for i, b in enumerate(out, start=1):
        b.rank = i
    return out

def enrich_dates_for_rows(context, rows: List[Banner]) -> None:
    if not FETCH_CAMPAIGN_DATES:
        return
    for b in rows:
        if not b.href_clean and b.href:
            b.href_clean = normalize_href(b.href)
        # 날짜는 href_clean 우선
        h = b.href_clean or b.href
        if not h:
            continue
        s, e = fetch_campaign_dates(context, h)
        b.plan_start = s
        b.plan_end = e


# -----------------------------------------------------
# Output
# -----------------------------------------------------
def write_csv(path: str, rows: List[Banner]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow([
            "date","brand_key","brand_name","rank",
            "title","href","href_clean",
            "plan_start","plan_end",
            "img_url","img_local","img_status",
            "img_w","img_h","img_bytes"
        ])
        for b in rows:
            w.writerow([
                b.date,b.brand_key,b.brand_name,b.rank,
                b.title,b.href,b.href_clean,
                b.plan_start,b.plan_end,
                b.img_url,b.img_local,b.img_status,
                b.img_w,b.img_h,b.img_bytes
            ])

def write_html(path: str, rows: List[Banner]):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    by_brand: Dict[str, List[Banner]] = {}
    for b in rows:
        by_brand.setdefault(b.brand_key, []).append(b)

    order = [bk for bk, _, _, _, _ in BRANDS]
    active_brand_keys = [bk for bk in order if bk in by_brand] or [bk for bk, *_ in BRANDS]
    now_str = kst_now().strftime('%Y-%m-%d %H:%M')

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
  <div class="text-xs">해당 브랜드의 히어로 배너를 이번 실행에서 수집하지 못했습니다. (일시적 구조 변경/팝업/봇체크 가능)</div>
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
    <header class="flex flex-col md:flex-row md:items-center justify-between mb-16 gap-6">
      <div>
        <h1 class="text-5xl font-black tracking-tight text-slate-900 mb-4">Hero Banner Analysis</h1>
        <p class="text-slate-500 text-lg font-medium italic">주요 아웃도어 브랜드 메인 히어로 배너 실시간 모니터링</p>
        <p class="text-slate-400 text-xs mt-3">로컬이미지 경로 모드: {"ABS(file://)" if HTML_USE_ABSOLUTE_FILE_URL else "REL(assets/)"} · 날짜추출: {"ON" if FETCH_CAMPAIGN_DATES else "OFF"} </p>
      </div>
      <div class="glass-card px-6 py-4 flex items-center gap-4">
        <div class="flex h-3 w-3 relative"><span class="animate-ping absolute h-full w-full rounded-full bg-blue-400 opacity-75"></span><span class="relative inline-flex rounded-full h-3 w-3 bg-blue-600"></span></div>
        <span class="text-sm font-black text-slate-800 tracking-widest uppercase">{now_str}</span>
      </div>
    </header>

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


# -----------------------------------------------------
# Brand dispatcher
# -----------------------------------------------------
def crawl_brand(page, bk, bn, url, mode, date_s, mx):
    print(f"\n[*] Analyzing: {bn} ({url})", flush=True)

    with stage(bn, "goto"):
        try:
            page.goto(url, timeout=NAV_TIMEOUT_MS, wait_until="domcontentloaded")
            try:
                page.wait_for_load_state("networkidle", timeout=8000)
            except Exception:
                pass
            page.wait_for_timeout(WAIT_AFTER_GOTO_MS)
        except Exception as e:
            print(f" - Goto failed: {e}", flush=True)
            return []

    with stage(bn, f"extract ({mode})"):
        if mode == "tnf_slick":
            return tnf_slick(page, url, bk, bn, date_s, mx)
        elif mode == "nepa_static":
            return nepa_static(page, url, bk, bn, date_s, mx)
        elif mode == "patagonia_static_hero":
            return patagonia_static_hero(page, url, bk, bn, date_s)
        elif mode == "blackyak_swiper":
            return blackyak_swiper(page, url, bk, bn, date_s, mx)
        elif mode == "discovery_swiper":
            return discovery_swiper(page, url, bk, bn, date_s, mx)
        elif mode == "generic":
            return generic_top_banners(page, url, bk, bn, date_s, mx)
        else:
            return generic_top_banners(page, url, bk, bn, date_s, mx)


def main():
    global PROG

    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(ASSET_DIR, exist_ok=True)
    os.makedirs(SNAP_DIR, exist_ok=True)

    now = kst_now()
    date_s = now.strftime("%Y-%m-%d")
    ts = now.strftime("%Y%m%d_%H%M%S")

    today_snap = os.path.join(SNAP_DIR, f"hero_main_banners_{date_s}.csv")
    report_csv = os.path.join(OUT_DIR, f"hero_main_banners_{ts}.csv")
    report_html = os.path.join(OUT_DIR, "hero_main.html")

    rows: List[Banner] = []

    # Progress init
    PROG = Progress(total=len(BRANDS))
    PROG.set_stage(stage="start")
    PROG.newline()

    with sync_playwright() as pw:
        browser, context = launch(pw)

        for bk, bn, url, mode, mx in BRANDS:
            page = None
            attempt = 0
            brand_ok = False
            brand_rows: List[Banner] = []

            while attempt < 2:
                attempt += 1
                try:
                    if PROG:
                        PROG.set_stage(brand=bn, stage=f"open_page (try {attempt}/2)")
                    page = context.new_page()

                    got = crawl_brand(page, bk, bn, url, mode, date_s, mx)
                    brand_rows.extend(got)

                    brand_ok = True
                    try:
                        page.close()
                    except Exception:
                        pass
                    break

                except (PWTimeoutError, PWError, Exception) as e:
                    print(f"\n - Error: {e}", flush=True)
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

            # ✅ 브랜드 단위 dedupe (중복 제거 후 rows에 합치기)
            brand_rows = dedupe_brand_rows(brand_rows)

            # ✅ 날짜 enrich (top3면 비용 감당 가능)
            with stage(bn, "fetch_dates"):
                enrich_dates_for_rows(context, brand_rows)

            rows.extend(brand_rows)

            if PROG:
                PROG.step_done(ok=brand_ok)
                PROG.newline()

        with stage("OUTPUT", "write_csv/html"):
            write_csv(today_snap, rows)
            write_csv(report_csv, rows)
            write_html(report_html, rows)

        print(f"\n[CSV] {report_csv}", flush=True)
        print(f"[HTML] {report_html}", flush=True)
        print(f"[ASSET_DIR] {os.path.abspath(ASSET_DIR)}", flush=True)
        print(f"[HTML_USE_ABSOLUTE_FILE_URL] {HTML_USE_ABSOLUTE_FILE_URL}", flush=True)
        print(f"[FETCH_CAMPAIGN_DATES] {FETCH_CAMPAIGN_DATES}", flush=True)

        try:
            browser.close()
        except Exception:
            pass

    if not os.path.exists(report_html):
        print(f"[FATAL] HTML not created: {report_html}", flush=True)
        sys.exit(1)

    print("✅ hero_main.html generated successfully", flush=True)
    sys.exit(0)


if __name__ == "__main__":
    main()
