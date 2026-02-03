#!/usr/bin/env python3
# -*- coding: utf-8 -*-

print("🔥 HERO SCRIPT NEW VERSION RUNNING 🔥", flush=True
      
"""
hero_main_top3_final_PRETTY_WHITE_FINAL_FIXED.py

Fixes:
- (핵심) HTML에서 로컬 이미지 경로를 file:// 절대경로로 사용 → HTML만 따로 옮겨 열어도 이미지 안 깨짐
- Hotlink/Referer 이슈 대응: 이미지 다운로드 시 Referer를 "페이지 URL"로 설정
- goto 후 networkidle wait으로 lazy-load 안정화
- generic_top_banners 스캔 범위 소폭 확장 + 스크롤 너지로 렌더 유도

[추가 Fix - GitHub Actions 실패 원인 해결]
- 구버전 잔재(hero_main_report_*.html 검사/exit 1)를 완전 제거
- reports/hero_main.html 생성 성공이면 무조건 success
- rows가 비어도(일시적 크롤링 실패) HTML/CSV는 생성하고 exit 0
"""

import os, re, csv, hashlib, urllib.parse, sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional
import requests
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError, Error as PWError

# Pillow (image resize)
try:
    from PIL import Image
    from io import BytesIO
    PIL_OK = True
except Exception:
    PIL_OK = False


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

USER_AGENT = os.environ.get(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
)

# (옵션) HTML에 file:// 절대경로로 로컬이미지를 박을지 여부
# - 로컬에서 HTML 단독 이동/열기 목적이면 1 추천
# - GitHub Pages(https) 배포면 file:// 로드가 브라우저 정책상 불가 → 반드시 0
HTML_USE_ABSOLUTE_FILE_URL = os.environ.get("HTML_USE_ABSOLUTE_FILE_URL", "1") != "0"


# -----------------------------------------------------
# Brands
# -----------------------------------------------------
BRANDS = [
    ("tnf", "The North Face", "https://www.thenorthfacekorea.co.kr/", "tnf_slick", 3),
    ("nepa", "NEPA", "https://www.nplus.co.kr/main/main.asp?NaPm=ct%3Dmk68nx7b%7Cci%3Dcheckout%7Ctr%3Dds%7Ctrx%3Dnull%7Chk%3D2eb6245a50cfbdfae4c4e3e806691658fa257fa9", "nepa_static", 3),
    ("patagonia", "Patagonia", "https://www.patagonia.co.kr/", "patagonia_static_hero", 1),
    ("blackyak", "Black Yak", "https://www.byn.kr/blackyak?utm_source=naver&utm_medium=BSA&utm_campaign=BY_EC_250828_hyperpulse_PERF_NV_BSA&utm_content=PC_BY_EC_naver_BSA_250828_hyperpulse_homelink&utm_term=%EB%B8%94%EB%9E%99%EC%95%BC%ED%81%AC&NaPm=ct%3Dmhwxwfpl%7Cci%3DERbd1ca7ea%2Dc04a%2D11f0%2D935c%2Df6a058b83a4c%7Ctr%3Dbrnd%7Chk%3D07dc9aedc63b17fba956801b4aa26232c93036a5%7Cnacn%3DBOWtB0gPQcOt", "blackyak_swiper", 3),
    ("discovery", "Discovery", "https://www.discovery-expedition.com/?gf=A", "discovery_swiper", 3),
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

def get_any_alt_text(el) -> str:
    try:
        img = el.locator("img").first
        if img.count():
            return norm_ws(img.get_attribute("alt") or "")
    except Exception:
        pass
    return ""

def choose_title(*cands: str) -> str:
    c = [norm_ws(x) for x in cands if norm_ws(x)]
    c = [x for x in c if x.lower() not in {"next", "prev", "이전", "다음", "닫기"} and len(x) > 1]
    if not c:
        return "메인 배너"
    c.sort(key=lambda x: (len(x), x), reverse=True)
    return c[0][:90]


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
# Image download + resize
# -----------------------------------------------------
def download_bytes(url: str, referer: str = "") -> Optional[bytes]:
    try:
        headers = {"User-Agent": USER_AGENT}
        if referer:
            headers["Referer"] = referer  # 중요: 이미지 URL이 아니라 '페이지 URL'
        r = requests.get(url, headers=headers, timeout=25)
        if r.status_code != 200 or not r.content:
            return None
        return r.content
    except Exception:
        return None

def save_and_resize_image(img_url: str, brand_key: str, rank: int, referer: str = "") -> str:
    if not img_url:
        return ""
    os.makedirs(ASSET_DIR, exist_ok=True)
    out_ext = ".jpg" if PIL_OK else guess_ext(img_url)
    fname = safe_filename(f"{brand_key}_{rank}_{sha1(img_url)}", out_ext)
    out_path = os.path.join(ASSET_DIR, fname)

    content = download_bytes(img_url, referer=referer)
    if content is None:
        return ""

    if not PIL_OK:
        with open(out_path, "wb") as f:
            f.write(content)
        return fname

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
        return fname
    except Exception:
        try:
            with open(out_path, "wb") as f:
                f.write(content)
            return fname
        except Exception:
            return ""


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
        key = (href, img_url)
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
        title = choose_title(alt, txt, "", urllib.parse.unquote((img_url or "").split("/")[-1]))
        img_local = save_and_resize_image(img_url, brand_key, rank, referer=base_url) if img_url else ""
        out.append(Banner(date_s, brand_key, brand_name, rank, title, href, img_url, img_local))
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
            key = sha1("\n".join([href, img_url]))
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
        title = choose_title(title_txt, urllib.parse.unquote((img_url or "").split("/")[-1]))
        img_local = save_and_resize_image(img_url, brand_key, rank, referer=base_url) if img_url else ""
        out.append(Banner(date_s, brand_key, brand_name, rank, title, href, img_url, img_local))
        print(f" #{rank}: {title}")
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
        key = (href, img_url)
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
        title = choose_title(alt, txt, "", urllib.parse.unquote((img_url or "").split("/")[-1]))
        img_local = save_and_resize_image(img_url, brand_key, rank, referer=base_url) if img_url else ""
        out.append(Banner(date_s, brand_key, brand_name, rank, title, href, img_url, img_local))
        rank += 1
    return out

def nepa_static(page, base_url: str, brand_key: str, brand_name: str, date_s: str, max_items: int):
    out = []
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
        title = choose_title(title_txt, urllib.parse.unquote((img_url or "").split("/")[-1]))
        if not img_url and not href:
            return None
        img_local = save_and_resize_image(img_url, brand_key, rank, referer=base_url) if img_url else ""
        return Banner(date_s, brand_key, brand_name, rank, title, href, img_url, img_local)

    rank = 1
    for idx in range(1, 30):
        if rank > max_items:
            break
        b = extract_from_banner(idx, rank)
        if b:
            out.append(b)
            print(f" #{rank}: {b.title}")
            rank += 1
        if idx >= 5 and len(out) >= max_items:
            break

    if not out:
        # fallback: top area scan
        try:
            vw = page.viewport_size["width"] if page.viewport_size else 1440
            candidates = root.locator("section, div, a")
            cnt = min(candidates.count(), 260)
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
                    if bb["y"] < -80 or bb["y"] > 700:
                        continue
                    if bb["width"] < vw * 0.60 or bb["height"] < 220:
                        continue
                    img_url = get_any_img_url(el, base_url)
                    href = ""
                    try:
                        a = el.locator("a[href]").first
                        if a.count():
                            href = abs_url(base_url, a.get_attribute("href") or "")
                    except Exception:
                        pass
                    if not img_url and not href:
                        continue
                    fp = sha1("\n".join([img_url or "", href or ""]))
                    if fp in seen:
                        continue
                    seen.add(fp)
                    title_txt = ""
                    try:
                        title_txt = norm_ws(el.locator("h1,h2,h3,strong,p").first.inner_text() or "")
                    except Exception:
                        pass
                    title = choose_title(title_txt, urllib.parse.unquote((img_url or "").split("/")[-1]))
                    img_local = save_and_resize_image(img_url, brand_key, rank, referer=base_url) if img_url else ""
                    out.append(Banner(date_s, brand_key, brand_name, rank, title, href, img_url, img_local))
                    print(f" #{rank}: {title}")
                    rank += 1
                except Exception:
                    continue
        except Exception:
            pass

    if not out:
        print(" - NEPA 배너를 못 찾았어요 (팝업/로봇체크/구조변경 가능). HEADLESS=0로 확인 추천")
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
        print(" - 상단 히어로 탐지 실패")
        return []

    title = choose_title(best_title, urllib.parse.unquote((best_img or "").split("/")[-1]))
    img_local = save_and_resize_image(best_img, brand_key, 1, referer=base_url) if best_img else ""
    print(f" #1: {title}")
    return [Banner(date_s, brand_key, brand_name, 1, title, best_href, best_img, img_local)]

def generic_top_banners(page_or_frame, base_url: str, brand_key: str, brand_name: str, date_s: str,
                        max_items: int, y_max: int = 1400):
    out = []
    try:
        vw = page_or_frame.viewport_size["width"] if getattr(page_or_frame, "viewport_size", None) else 1440
    except Exception:
        vw = 1440

    # scroll nudges to force lazy render
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
            fp = sha1("\n".join([img_url or "", href or ""]))
            if fp in seen:
                continue
            seen.add(fp)
            title_txt = ""
            try:
                title_txt = norm_ws(el.locator("h1,h2,h3,strong,p").first.inner_text() or "")
            except Exception:
                pass
            title = choose_title(title_txt, urllib.parse.unquote((img_url or "").split("/")[-1]))
            img_local = save_and_resize_image(img_url, brand_key, rank, referer=base_url) if img_url else ""
            out.append(Banner(date_s, brand_key, brand_name, rank, title, href, img_url, img_local))
            rank += 1
        except Exception:
            continue
    return out


# -----------------------------------------------------
# Output
# -----------------------------------------------------
def write_csv(path: str, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["date","brand_key","brand_name","rank","title","href","img_url","img_local"])
        for b in rows:
            w.writerow([b.date,b.brand_key,b.brand_name,b.rank,b.title,b.href,b.img_url,b.img_local])

def write_html(path: str, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    by_brand = {}
    for b in rows:
        by_brand.setdefault(b.brand_key, []).append(b)

    order = [bk for bk, _, _, _, _ in BRANDS]
    active_brand_keys = [bk for bk in order if bk in by_brand] or [bk for bk, *_ in BRANDS]  # rows 비어도 탭은 유지
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
                    local_path = os.path.join(OUT_DIR, it.img_local)
                    img_src = to_file_url(local_path) if HTML_USE_ABSOLUTE_FILE_URL else it.img_local
                if not img_src:
                    img_src = it.img_url or ""

                href = it.href or "#"
                img_url_btn = it.img_url or img_src or "#"

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
    <h4 class="text-slate-800 font-bold text-sm mb-4 line-clamp-2 min-h-[40px]">"{it.title}"</h4>
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
        <p class="text-slate-400 text-xs mt-3">로컬이미지 경로 모드: {"ABS(file://)" if HTML_USE_ABSOLUTE_FILE_URL else "REL(assets/)"} </p>
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
</body>
</html>
"""
    with open(path, "w", encoding="utf-8") as f:
        f.write(full_html)


# -----------------------------------------------------
# Brand dispatcher
# -----------------------------------------------------
def crawl_brand(page, bk, bn, url, mode, date_s, mx):
    print(f"[*] Analyzing: {bn} ({url})")
    try:
        page.goto(url, timeout=NAV_TIMEOUT_MS, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=8000)
        except Exception:
            pass
        page.wait_for_timeout(WAIT_AFTER_GOTO_MS)
    except Exception as e:
        print(f" - Goto failed: {e}")
        return []

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
    else:
        return generic_top_banners(page, url, bk, bn, date_s, mx)


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(ASSET_DIR, exist_ok=True)
    os.makedirs(SNAP_DIR, exist_ok=True)

    now = kst_now()
    date_s = now.strftime("%Y-%m-%d")
    ts = now.strftime("%Y%m%d_%H%M%S")

    today_snap = os.path.join(SNAP_DIR, f"hero_main_banners_{date_s}.csv")
    report_csv = os.path.join(OUT_DIR, f"hero_main_banners_{ts}.csv")

    # ✅ 최종 산출물: 항상 reports/hero_main.html (OUT_DIR 기준)
    report_html = os.path.join(OUT_DIR, "hero_main.html")

    rows: List[Banner] = []

    with sync_playwright() as pw:
        browser, context = launch(pw)
        for bk, bn, url, mode, mx in BRANDS:
            page = None
            attempt = 0
            while attempt < 2:
                attempt += 1
                try:
                    page = context.new_page()
                    rows.extend(crawl_brand(page, bk, bn, url, mode, date_s, mx))
                    try:
                        page.close()
                    except Exception:
                        pass
                    break
                except (PWTimeoutError, PWError, Exception) as e:
                    print(f" - Error: {e}")
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
                        print(" - Relaunching browser and retrying once...")
                        continue
                    break

        # ✅ rows 비어도 결과 파일은 생성
        write_csv(today_snap, rows)
        write_csv(report_csv, rows)
        write_html(report_html, rows)

        print(f"[CSV] {report_csv}")
        print(f"[HTML] {report_html}")
        print(f"[ASSET_DIR] {os.path.abspath(ASSET_DIR)}")
        print(f"[HTML_USE_ABSOLUTE_FILE_URL] {HTML_USE_ABSOLUTE_FILE_URL}")

        try:
            browser.close()
        except Exception:
            pass

    # ✅ 성공 조건: reports/hero_main.html만 존재하면 OK
    if not os.path.exists(report_html):
        print(f"[FATAL] HTML not created: {report_html}")
        sys.exit(1)

    print("✅ hero_main.html generated successfully")
    sys.exit(0)


if __name__ == "__main__":
    main()
