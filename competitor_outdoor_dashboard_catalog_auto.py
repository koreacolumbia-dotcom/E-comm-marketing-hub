
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Competitor Outdoor Product Intelligence Dashboard (final patched v5)
- Single-file crawler + analyzer + dashboard builder
- Auto-discovery friendly: works without manual document.querySelector debugging
- Brand-aware URL filtering + detail-link fallback + generic selector fallback
- Outputs:
  reports/competitor_intel/raw_products.csv
  reports/competitor_intel/analyzed_products.csv
  reports/competitor_intel/brand_summary.csv
  reports/competitor_intel/keyword_discovery.csv
  reports/competitor_intel/dashboard_data.json
  reports/competitor_intel/dashboard.html

Notes
-----
1) This version is designed to reduce per-brand manual selector work.
2) It prioritizes:
   seed URLs -> auto-discover category/product links -> collect product detail URLs ->
   crawl detail pages -> analyze -> render dashboard.
3) If a site blocks automation heavily, you may still need 1-2 brand-specific tweaks.
"""

from __future__ import annotations


# === FORCE EXCEL PATH (USER REPO) ===
EXCEL_CATEGORY_PATH = "카테고리_정리.xlsx"


import os
import re
import json
import math
import time
import html
import traceback
from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import Dict, List, Optional, Iterable, Set, Tuple
from urllib.parse import urljoin, urlparse, urlunparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed


def json_safe(obj):
    try:
        import numpy as np
        numpy_types = (np.integer, np.floating, np.bool_)
    except Exception:
        numpy_types = tuple()

    if isinstance(obj, dict):
        return {str(k): json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [json_safe(v) for v in obj]
    if isinstance(obj, tuple):
        return [json_safe(v) for v in obj]
    if numpy_types and isinstance(obj, numpy_types):
        return obj.item()
    if hasattr(obj, "item") and callable(getattr(obj, "item")):
        try:
            return obj.item()
        except Exception:
            pass
    try:
        if pd.isna(obj):
            return None
    except Exception:
        pass
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj


from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException,
    WebDriverException,
)
from webdriver_manager.chrome import ChromeDriverManager


# ============================================================
# 0. CONFIG
# ============================================================
OUT_DIR = os.path.join("reports", "competitor_intel")
HEADLESS = os.getenv("HEADLESS", "1").strip().lower() not in {"0", "false", "no"}
MAX_PRODUCTS_PER_BRAND = int(os.getenv("MAX_PRODUCTS_PER_BRAND", "400"))
MAX_DISCOVERED_LISTING_URLS = int(os.getenv("MAX_DISCOVERED_LISTING_URLS", "120"))
SCROLL_PAUSE_SEC = float(os.getenv("SCROLL_PAUSE_SEC", "1.0"))
DEFAULT_WAIT_SEC = int(os.getenv("DEFAULT_WAIT_SEC", "18"))
SCREENSHOT_ON_ERROR = os.getenv("SCREENSHOT_ON_ERROR", "1").strip().lower() in {"1", "true", "yes"}
TODAY_STR = datetime.now().strftime("%Y-%m-%d %H:%M")
REQUESTS_TIMEOUT = int(os.getenv("REQUESTS_TIMEOUT", "20"))
DETAIL_WORKERS = int(os.getenv("DETAIL_WORKERS", "6"))
REQUEST_HEADERS = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36","Accept-Language":"ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"}


@dataclass
class BrandConfig:
    brand: str
    seed_urls: List[str]
    domain: str
    brand_terms: List[str] = field(default_factory=list)
    product_url_keywords: List[str] = field(default_factory=list)
    listing_url_keywords: List[str] = field(default_factory=list)
    deny_url_keywords: List[str] = field(default_factory=list)
    force_allow_url_keywords: List[str] = field(default_factory=list)
    category_hints: List[str] = field(default_factory=list)
    max_products: int = MAX_PRODUCTS_PER_BRAND
    detail_mode: str = "hybrid"
    detail_name_selectors: List[str] = field(default_factory=list)
    detail_price_selectors: List[str] = field(default_factory=list)
    detail_original_price_selectors: List[str] = field(default_factory=list)
    detail_desc_selectors: List[str] = field(default_factory=list)
    detail_image_selectors: List[str] = field(default_factory=list)



BRAND_CONFIGS: List[BrandConfig] = [
    BrandConfig(
        brand="COLUMBIA",
        seed_urls=["https://www.columbiakorea.co.kr/"],
        domain="www.columbiakorea.co.kr",
        brand_terms=["컬럼비아", "콜롬비아", "columbia"],
        product_url_keywords=["/product/detail", "product/detail", "product/view", "gdno=", "product_no=", "/shop/goods", "/product/"],
        listing_url_keywords=["/product/list", "gdv=", "cno=", "/category", "/display"],
        deny_url_keywords=["benefit", "download", "event", "notice", "inside", "faq", "login", "join", "magazine"],
        category_hints=["자켓", "팬츠", "슈즈", "신발", "티셔츠", "플리스", "패딩"],
        detail_mode="hybrid",
    ),
    BrandConfig(
        brand="DISCOVERY",
        seed_urls=["https://www.discovery-expedition.com/"],
        domain="www.discovery-expedition.com",
        brand_terms=["discovery", "디스커버리"],
        product_url_keywords=["product-detail", "/product/", "/goods/", "sku", "style"],
        listing_url_keywords=["/display/", "/category/", "/collection/", "/shop/"],
        deny_url_keywords=["login", "join", "benefit", "guide", "notice", "store-locator", "about", "magazine", "style-pick", "discoverer-picks", "brand/style-pick"],
        force_allow_url_keywords=["product-detail"],
        detail_mode="selenium_only",
        detail_name_selectors=["[class*=product] [class*=name]", "[class*=detail] [class*=name]", "[class*=title]", "h1", "h2"],
        detail_price_selectors=["[class*=sale][class*=price]", "[class*=current][class*=price]", "[class*=price] strong", "[class*=price]"],
        detail_original_price_selectors=["[class*=origin][class*=price]", "[class*=consumer]", "[class*=normal][class*=price]", "del"],
        detail_desc_selectors=["[class*=product][class*=desc]", "[class*=detail][class*=desc]", "[class*=detail][class*=info]", "[class*=summary]"],
        detail_image_selectors=["[class*=detail] img", "[class*=product] img", ".swiper-slide img", "img"],
    ),
    # Disabled for now
    # BrandConfig(... THE_NORTH_FACE ...),
    # BrandConfig(... K2 ...),
    # BrandConfig(... KOLON_SPORT ...),
    # BrandConfig(... EIDER ...),
    # BrandConfig(... NEPA ...),
    # BrandConfig(... BLACKYAK ...),
    # BrandConfig(... MILLET ...),
    # BrandConfig(... SNOWPEAK_APPAREL ...),
    # BrandConfig(... PATAGONIA ...),
    # BrandConfig(... SALOMON ...),
]

GENERIC_PRODUCT_URL_KEYWORDS = [
    "product-detail", "/product/", "/products/", "/goods/", "/p/", "goodsNo=", "productNo=", "sku=", "style="
]
GENERIC_LISTING_URL_KEYWORDS = [
    "/category", "/display", "/collection", "/collections", "/listing", "/shop", "/plp", "/list", "cno=", "gdv="
]
GENERIC_DENY_URL_KEYWORDS = [
    "login", "join", "signup", "benefit", "download", "event", "notice", "faq", "inside", "brand-story",
    "policy", "terms", "privacy", "store-locator", "magazine", "journal", "about", "community", "customer"
]

GENERIC_NAME_SELECTORS = [
    "[class*='product'][class*='name']",
    "[class*='product'] [class*='name']",
    "[class*='goods'][class*='name']",
    "[class*='goods'] [class*='name']",
    "[class*='item'][class*='name']",
    "[class*='item'] [class*='name']",
    "[class*='detail'] [class*='name']",
    "[class*='title']",
    "[class*='tit']",
    "meta[property='og:title']",
    "h1",
    "h2",
]
GENERIC_PRICE_SELECTORS = [
    "[class*='sale'][class*='price']",
    "[class*='current'][class*='price']",
    "[class*='product'] [class*='price']",
    "[class*='goods'] [class*='price']",
    "[class*='price']",
]
GENERIC_ORIGINAL_PRICE_SELECTORS = [
    "[class*='consumer']",
    "[class*='normal'][class*='price']",
    "[class*='origin'][class*='price']",
    "[class*='list'][class*='price']",
    "del",
]
GENERIC_DESC_SELECTORS = [
    "[class*='description']",
    "[class*='desc']",
    "[class*='summary']",
    "[class*='detail'] [class*='info']",
    "[class*='detail'] [class*='text']",
    "[class*='info']",
    "meta[name='description']",
]
GENERIC_IMAGE_SELECTORS = [
    "meta[property='og:image']",
    "[class*='product'] img",
    "[class*='goods'] img",
    "[class*='detail'] img",
    ".swiper-slide img",
    "img"
]
GENERIC_SOLDOUT_SELECTORS = [
    "[class*='soldout']",
    "[class*='sold-out']",
    "[class*='sold'][class*='out']",
]
GENERIC_GENDER_SELECTORS = [
    "[class*='gender']",
]
GENERIC_SEASON_SELECTORS = [
    "[class*='season']",
]

GENERIC_BREADCRUMB_SELECTORS = [
    "nav[aria-label*=breadcrumb i] a",
    "nav[aria-label*=breadcrumb i] li",
    "[class*=breadcrumb] a",
    "[class*=breadcrumb] li",
    "[class*=breadCrumb] a",
    "[class*=breadCrumb] li",
    "[class*=crumb] a",
    "[class*=crumb] li",
    "ol.breadcrumb li",
    "ul.breadcrumb li",
    ".breadcrumb-item",
    "[data-breadcrumb] a",
    "[data-breadcrumb] li",
]

NOISE_WORDS = {
    "the", "and", "with", "for", "from", "outdoor", "sports", "wear", "new", "best",
    "남성", "여성", "공용", "신상", "기본", "시즌", "정품", "단독", "라인", "기능성",
    "자켓", "팬츠", "셔츠", "신발", "모자", "가방", "상품", "제품", "스타일",
}

BRAND_TERM_MAP: Dict[str, str] = {
    "DRYVENT": "방수",
    "FUTURELIGHT": "방수",
    "OMNI-TECH": "방수",
    "OMNI TECH": "방수",
    "WINDWALL": "방풍",
    "WINDSTOPPER": "방풍",
    "GORE-TEX": "고어텍스",
    "GORE TEX": "고어텍스",
    "GORE-TEX PRO": "고어텍스_PRO",
    "GORE TEX PRO": "고어텍스_PRO",
    "POLARTEC": "폴라텍",
    "CORDURA": "코듀라",
    "DOWN": "다운",
    "덕다운": "다운",
    "구스다운": "다운",
    "2L": "2L",
    "2.5L": "2.5L",
    "3L": "3L",
    "방수": "방수",
    "발수": "방수",
    "방풍": "방풍",
    "투습": "투습",
    "경량": "경량",
    "스트레치": "스트레치",
    "신축": "스트레치",
    "보온": "보온",
    "인슐레이션": "인슐레이션",
    "플리스": "보온",
    "심실링": "방수",
    "쉘": "구조",
}
ATTRIBUTE_PRIORITY: Dict[str, int] = {
    "고어텍스_PRO": 100,
    "고어텍스": 95,
    "폴라텍": 90,
    "코듀라": 85,
    "3L": 80,
    "2.5L": 75,
    "2L": 70,
    "다운": 65,
    "방수": 60,
    "보온": 50,
    "방풍": 40,
    "투습": 35,
    "스트레치": 30,
    "경량": 25,
    "인슐레이션": 20,
    "구조": 10,
}
CONFLICT_RULES: List[Tuple[str, str, str]] = [
    ("고어텍스", "방수", "고어텍스"),
    ("고어텍스_PRO", "방수", "고어텍스_PRO"),
    ("3L", "2L", "3L"),
    ("3L", "2.5L", "3L"),
    ("2.5L", "2L", "2.5L"),
    ("방수", "방풍", "방수"),
    ("다운", "인슐레이션", "다운"),
]


CATEGORY_RULES: Dict[str, List[str]] = {
    "자켓": ["jacket", "jk", "자켓", "재킷", "아노락", "바람막이", "windbreaker", "shell", "parka", "파카"],
    "팬츠": ["pants", "pant", "trouser", "팬츠", "바지", "조거", "슬랙스", "쇼츠", "반바지", "cargo", "카고", "legging", "레깅스"],
    "플리스": ["fleece", "플리스", "boa", "보아"],
    "다운": ["down", "덕다운", "구스다운", "패딩", "puffer"],
    "베스트": ["vest", "베스트"],
    "후디": ["hoodie", "hood", "후디", "후드", "후드티", "sweatshirt", "맨투맨"],
    "티셔츠": ["tee", "t-shirt", "t shirt", "티셔츠", "반팔", "긴팔", "sleeve", "half zip", "half-zip", "집업티", "zip tee", "zip-tee"],
    "셔츠": ["shirt", "셔츠"],
    "슈즈": ["shoe", "shoes", "boot", "boots", "sneaker", "trail", "등산화", "신발", "부츠", "sandals", "sandal", "샌들", "슬라이드", "slide", "슬리퍼", "atr", "outdry"],
    "장갑": ["glove", "gloves", "장갑", "mitt"],
    "백": ["bag", "bags", "backpack", "pack", "배낭", "백팩", "가방", "body bag", "bodybag", "바디백", "sling", "슬링백", "슬링", "shoulder case", "숄더케이스", "케이스"],
    "ACC": ["acc", "accessory", "accessories", "모자", "cap", "hat", "부니", "bunny hat", "버킷햇"],
}

# ============================================================
# 1. MODELS
# ============================================================
@dataclass
class ProductRaw:
    brand: str
    source_url: str
    product_url: str
    name: str
    description: str
    price_text: str
    original_price_text: str
    image_url: str
    sold_out_text: str
    gender_text: str
    season_text: str
    source_category: str = ""
    source_category_url: str = ""
    breadcrumb_text: str = ""
    crawled_at: str = ""


@dataclass
class ProductAnalyzed:
    brand: str
    source_url: str
    product_url: str
    name: str
    description: str
    image_url: str
    current_price: Optional[int]
    original_price: Optional[int]
    discount_rate: Optional[float]
    sold_out: bool
    gender: str
    season: str
    item_category: str
    raw_keywords: List[str] = field(default_factory=list)
    standard_attributes: List[str] = field(default_factory=list)
    dominant_attribute: str = "기타"
    grade: str = "Entry"
    shell_type: str = "Unknown"
    price_band: str = "기타"
    positioning_y: str = "Mass"
    positioning_x: str = "Lifestyle"
    attribute_coverage_flag: str = "OK"
    source_category: str = ""
    source_category_url: str = ""
    breadcrumb_text: str = ""
    crawled_at: str = ""


# ============================================================
# 2. UTILS
# ============================================================
def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def safe_text(value: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def compact_text(value: Optional[str]) -> str:
    return safe_text(value).replace("\xa0", " ")


CATEGORY_MASTER_XLS_CANDIDATES = [
    os.path.join("/mnt/data", "카테고리_정리(1).xlsx"),
    os.path.join(os.getcwd(), "카테고리_정리(1).xlsx"),
]

def _normalize_brand_name(brand: str) -> str:
    b = safe_text(brand).lower()
    if b in {"컬럼비아", "columbia"}:
        return "COLUMBIA"
    if b in {"디스커버리", "discovery"}:
        return "DISCOVERY"
    return safe_text(brand).upper()

def _normalize_category_leaf(v: str) -> str:
    t = safe_text(v)
    if not t:
        return ""
    low = t.lower()
    if any(x in low for x in ["트레일러닝", "등산화", "스니커즈", "샌들", "슬리퍼", "부츠", "레인 부츠", "신발", "슈즈"]):
        return "슈즈"
    if any(x in low for x in ["백팩", "크로스", "토트", "힙색", "슬링", "가방", "백"]):
        return "백"
    if any(x in low for x in ["볼캡", "버킷", "부니", "비니", "모자", "양말", "장갑", "스틱", "지갑", "용품", "acc", "기타"]):
        return "ACC"
    if "플리스" in low:
        return "플리스"
    if any(x in low for x in ["베스트"]):
        return "베스트"
    if any(x in low for x in ["방수자켓", "바람막이", "경량패딩", "인터체인지", "자켓", "아우터", "아노락", "경량자켓"]):
        return "자켓"
    if any(x in low for x in ["라운드티", "반팔티", "긴팔티", "폴로티", "집업", "티셔츠", "상의", "셔츠", "맨투맨", "후드티", "트레이닝 상의", "래쉬가드 상의"]):
        if "셔츠" in low and not any(x in low for x in ["티", "후드", "맨투맨", "폴로", "집업"]):
            return "셔츠"
        if "후드" in low or "맨투맨" in low:
            return "후디"
        return "티셔츠"
    if any(x in low for x in ["카고", "조거", "긴바지", "반바지", "롱팬츠", "숏팬츠", "팬츠", "하의", "바지", "트레이닝 하의", "래쉬가드 하의"]):
        return "팬츠"
    return t

def _build_category_master():
    df = None
    for p in CATEGORY_MASTER_XLS_CANDIDATES:
        try:
            if os.path.exists(p):
                df = pd.read_excel(p, header=2)
                break
        except Exception:
            continue
    if df is None or df.empty:
        return {"url_map": {}, "token_map": {}}
    url_map = {}
    token_map = {}
    for _, row in df.iterrows():
        brand = _normalize_brand_name(row.get("브랜드", ""))
        url = canonicalize_url(str(row.get("URL", "")).strip())
        c2 = safe_text(row.get("분류2", ""))
        c3 = safe_text(row.get("분류3", ""))
        path = safe_text(row.get("카테고리경로", ""))
        final_item = _normalize_category_leaf(c3 or c2 or path)
        if url and final_item:
            url_map[(brand, url)] = {
                "item_category": final_item,
                "gender": ("키즈" if "키즈" in " ".join([c2, c3, path]) else ""),
                "path": path,
                "c2": c2,
                "c3": c3,
            }
        for token in [c3, c2, path]:
            token = safe_text(token).lower()
            if token and final_item:
                token_map[(brand, token)] = final_item
    return {"url_map": url_map, "token_map": token_map}

CATEGORY_MASTER = _build_category_master()

def _apply_category_master_seed_urls():
    brand_urls = {}
    for (brand, url), meta in CATEGORY_MASTER.get("url_map", {}).items():
        if not url:
            continue
        brand_urls.setdefault(brand, []).append(url)
    for cfg in BRAND_CONFIGS:
        normalized = _normalize_brand_name(cfg.brand)
        urls = [canonicalize_url(u) for u in brand_urls.get(normalized, []) if canonicalize_url(u)]
        if urls:
            cfg.seed_urls = unique_preserve_order(urls)

_apply_category_master_seed_urls()



# ===== URL CATEGORY HARD MAPPING (v8) =====
URL_CATEGORY_HARD_MAP = {
    # COLUMBIA
    "cno=251": "티셔츠",
    "cno=201": "자켓",
    "cno=204": "자켓",
    "cno=219": "자켓",
    "cno=213": "자켓",
    "cno=225": "베스트",
    "cno=371": "자켓",
    "cno=301": "팬츠",
    "cno=302": "팬츠",
    "cno=401": "슈즈",
    "cno=501": "백",

    # DISCOVERY
    "DXMB03": "슈즈",
    "DXMB02B01B17": "자켓",
    "DXMB02B01B10": "자켓",
    "DXMB02B01B12": "자켓",
    "DXMB02B01B15": "자켓",
    "DXMB02B02": "팬츠",
    "DXMB02B03": "티셔츠",
    "DXMB05": "ACC",
}

def get_hard_category_from_url(url: str = "", brand: str = "") -> str:
    u = safe_text(url)
    if not u:
        return ""
    low = u.lower()
    for key, val in URL_CATEGORY_HARD_MAP.items():
        if key.lower() in low:
            return val
    # Columbia explicit cno fallback patterns
    if "columbiakorea.co.kr" in low:
        m = re.search(r"[?&]cno=(\d+)", low)
        if m:
            cno = m.group(1)
            return URL_CATEGORY_HARD_MAP.get(f"cno={cno}", "")
    # Discovery display code fallback
    if "discovery-expedition.com" in low:
        m = re.search(r"/display/([A-Z0-9]+)", u, flags=re.I)
        if m:
            code = m.group(1).upper()
            for key, val in URL_CATEGORY_HARD_MAP.items():
                if key.upper() in code:
                    return val
    return ""



def resolve_master_category(brand: str, source_category_url: str = "", source_category: str = "", breadcrumb_text: str = "", name: str = "", description: str = "") -> str:
    b = _normalize_brand_name(brand)

    # 1) Hard URL mapping always wins
    hard = get_hard_category_from_url(source_category_url, b)
    if hard:
        return hard

    # 2) Exact master URL mapping
    key = canonicalize_url(source_category_url)
    row = CATEGORY_MASTER["url_map"].get((b, key))
    if row:
        return row.get("item_category", "") or ""

    # 3) Partial URL/token matching from master map
    for (bb, url_key), meta in CATEGORY_MASTER["url_map"].items():
        if bb == b and url_key and url_key in key:
            return meta.get("item_category", "") or ""
    for t in [source_category_url, source_category, breadcrumb_text]:
        low = safe_text(t).lower()
        if not low:
            continue
        for (bb, token), item in CATEGORY_MASTER["token_map"].items():
            if bb == b and token and token in low:
                return item

    # 4) Conservative product-name fallback only for obvious cases
    merged = safe_text(f"{name} {description}").lower()
    if b == "COLUMBIA":
        if any(x in merged for x in ["konos", "peakfreak", "crestwood", "escape thrive", "omni-max", "outdry", "atr", "shoe", "sneaker", "boot", "clog"]):
            return "슈즈"
        if any(x in merged for x in ["tee", "t-shirt", "short sleeve", "long sleeve", "graphic tee", "crew", "jersey", "polo"]):
            return "티셔츠"
        if any(x in merged for x in ["pant", "pants", "cargo pant", "jogger", "shorts"]):
            return "팬츠"
        if any(x in merged for x in ["jacket", "windbreaker", "anorak", "parka", "shell"]):
            return "자켓"
    return ""


def resolve_master_gender(brand: str, source_category_url: str = "", source_category: str = "", breadcrumb_text: str = "", raw_gender: str = "", name: str = "", description: str = "") -> str:
    b = _normalize_brand_name(brand)
    row = CATEGORY_MASTER["url_map"].get((b, canonicalize_url(source_category_url)))
    if row and row.get("gender"):
        return row["gender"]
    joined = " ".join([safe_text(source_category), safe_text(breadcrumb_text), safe_text(raw_gender), safe_text(name), safe_text(description)]).lower()
    if any(x in joined for x in ["키즈", "kids", "junior", "youth", "boy", "girl"]):
        return "키즈"
    if any(x in joined for x in ["여성", "women", "woman", "womens"]):
        return "여성"
    if any(x in joined for x in ["남성", "men", "man", "mens"]):
        return "남성"
    if any(x in joined for x in ["공용", "unisex"]):
        return "공용"
    return ""



def parse_price_to_int(text: str) -> Optional[int]:
    if not text:
        return None

    raw = str(text).replace("\xa0", " ")
    candidates = re.findall(r'\d{1,3}(?:,\d{3})+', raw)
    if not candidates:
        candidates = re.findall(r'\d{4,7}', raw)

    values = []
    for c in candidates:
        try:
            v = int(re.sub(r'[^0-9]', '', c))
        except Exception:
            continue
        if 1000 <= v <= 5000000:
            values.append(v)

    if values:
        return min(values)

    cleaned = re.sub(r'[^0-9]', '', raw)
    if not cleaned:
        return None
    try:
        v = int(cleaned)
    except ValueError:
        return None
    if 1000 <= v <= 5000000:
        return v
    return None

def calc_discount_rate(current_price: Optional[int], original_price: Optional[int]) -> Optional[float]:
    if current_price is None or original_price is None or original_price <= 0:
        return None
    if current_price > original_price:
        return 0.0
    return round((1 - (current_price / original_price)) * 100, 1)


def canonicalize_url(url: str) -> str:
    if not url:
        return ""
    try:
        parsed = urlparse(url.strip())
        parsed = parsed._replace(fragment="")
        return urlunparse(parsed)
    except Exception:
        return url.strip()


def same_domain(url: str, domain: str) -> bool:
    try:
        return domain.lower() in urlparse(url).netloc.lower()
    except Exception:
        return False


def unique_preserve_order(items: Iterable[str]) -> List[str]:
    seen = set()
    result = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def first_nonempty(*values: str) -> str:
    for v in values:
        if safe_text(v):
            return safe_text(v)
    return ""


def slugify(text: str) -> str:
    text = safe_text(text).lower()
    text = re.sub(r"[^a-z0-9가-힣]+", "-", text)
    return text.strip("-") or "na"


def get_text_from_element(el) -> str:
    try:
        return compact_text(el.text)
    except Exception:
        return ""


def get_attr_from_element(el, attr: str) -> str:
    try:
        return compact_text(el.get_attribute(attr))
    except Exception:
        return ""


def best_image_from_element(el) -> str:
    candidates = []
    try:
        imgs = []
        try:
            imgs = el.find_elements(By.CSS_SELECTOR, "img")
        except Exception:
            imgs = []
        for img in imgs[:12]:
            try:
                displayed = img.is_displayed()
            except Exception:
                displayed = True
            try:
                alt = compact_text(img.get_attribute("alt"))
            except Exception:
                alt = ""
            try:
                cls = compact_text(img.get_attribute("class"))
            except Exception:
                cls = ""
            try:
                w = int(float(img.get_attribute("naturalWidth") or img.get_attribute("width") or 0))
            except Exception:
                w = 0
            try:
                h = int(float(img.get_attribute("naturalHeight") or img.get_attribute("height") or 0))
            except Exception:
                h = 0
            urls = []
            for attr in ["src", "data-src", "data-lazy-src", "data-original", "currentSrc", "content"]:
                try:
                    val = compact_text(img.get_attribute(attr))
                except Exception:
                    val = ""
                if val and val.startswith("http"):
                    urls.append(val)
            try:
                srcset = compact_text(img.get_attribute("srcset"))
            except Exception:
                srcset = ""
            if srcset:
                first = srcset.split(",")[0].strip().split(" ")[0].strip()
                if first.startswith("http"):
                    urls.append(first)
            for u in urls:
                candidates.append({"url": u, "alt": alt, "class": cls, "displayed": displayed, "w": w, "h": h})
    except Exception:
        pass
    if not candidates:
        return ""

    def _score(item):
        low = item["url"].lower()
        alt = (item.get("alt") or "").lower()
        cls = (item.get("class") or "").lower()
        score = 0
        if item.get("displayed"):
            score += 6
        if item.get("w", 0) >= 180 and item.get("h", 0) >= 180:
            score += 5
        if "static-resource.discovery-expedition.com" in low:
            score += 16
        if "/thnail/" in low:
            score += 14
        if any(x in low for x in ["/goods/", "/thumbnail/", "thumb", "product"]):
            score += 6
        if any(x in cls for x in ["thumb", "thumbnail", "goods", "product"]):
            score += 4
        if any(x in alt for x in ["디스커버리", "컬럼비아", "expedition", "columbia"]):
            score += 3
        if any(x in low for x in ["icon", "logo", "arrow", "sprite", "blank", "placeholder", "noimage", "loading", "1x1", "spacer"]):
            score -= 20
        if any(x in low for x in ["wash", "care", "label", "laundry", "guide", "caution", "notice", "symbol"]):
            score -= 50
        if any(x in alt for x in ["손세탁", "염소", "표백", "세탁", "라벨", "wash", "care", "label", "염소표백", "약30", "중성"]):
            score -= 60
        return (-score, len(item["url"]))

    best = sorted(candidates, key=_score)[0]
    return best["url"]



def is_bad_product_image_url(url: str) -> bool:
    low = compact_text(url).lower()
    if not low:
        return True
    bad_tokens = [
        "wash", "care", "label", "laundry", "placeholder", "blank", "noimage", "loading",
        "염소", "표백", "손세탁", "세탁", "케어"
    ]
    return any(t in low for t in bad_tokens)

def best_listing_image_from_anchor(a, brand: str = "") -> str:
    candidates = []
    try:
        imgs = a.find_elements(By.CSS_SELECTOR, "img")
    except Exception:
        imgs = []
    for img in imgs[:12]:
        try:
            displayed = img.is_displayed()
        except Exception:
            displayed = True
        alt = compact_text(img.get_attribute("alt") or "")
        cls = compact_text(img.get_attribute("class") or "")
        urls = []
        for attr in ["src", "data-src", "data-lazy-src", "data-original", "currentSrc"]:
            try:
                val = compact_text(img.get_attribute(attr))
            except Exception:
                val = ""
            if val and val.startswith("http"):
                urls.append(val)
        try:
            srcset = compact_text(img.get_attribute("srcset"))
        except Exception:
            srcset = ""
        if srcset:
            for part in srcset.split(","):
                u = part.strip().split(" ")[0].strip()
                if u.startswith("http"):
                    urls.append(u)
        for u in urls:
            low = u.lower()
            score = 0
            if displayed:
                score += 30
            if brand == "DISCOVERY":
                if "static-resource.discovery-expedition.com" in low:
                    score += 80
                if "/thnail/" in low or "thnail" in low or "thumbnail" in low:
                    score += 120
            if brand == "COLUMBIA":
                if "productimages" in low or "styleship" in low:
                    score += 100
            if is_bad_product_image_url(low):
                score -= 300
            if alt and any(t in alt for t in ["세탁", "손세탁", "표백", "케어", "라벨"]):
                score -= 300
            if cls and any(t in cls.lower() for t in ["thumb", "thumbnail", "goods", "product"]):
                score += 20
            candidates.append((score, u))
    if not candidates:
        return ""
    candidates.sort(key=lambda x: (-x[0], x[1]))
    return candidates[0][1]

def extract_listing_name_from_anchor(a) -> str:
    sels = [
        "[class*='name']", "[class*='title']", "strong", "p", "span"
    ]
    vals = []
    for css in sels:
        try:
            for el in a.find_elements(By.CSS_SELECTOR, css)[:10]:
                txt = compact_text(el.text)
                if txt and len(txt) <= 120:
                    vals.append(txt)
        except Exception:
            pass
    vals = [v for v in vals if not re.search(r'원$|\d{1,3}(?:,\d{3})', v)]
    return max(vals, key=len) if vals else ""

def extract_listing_price_from_anchor(a) -> str:
    vals = []
    sels = [".product-price", ".price", "[class*='price']", "strong", "p", "span"]
    for css in sels:
        try:
            for el in a.find_elements(By.CSS_SELECTOR, css)[:12]:
                txt = compact_text(el.text)
                if txt and re.search(r'\d{1,3}(?:,\d{3})|\d{4,7}', txt):
                    vals.append(txt)
        except Exception:
            pass
    return choose_price_text(vals) if vals else ""


def should_use_listing_image(brand: str, detail_image_url: str, listing_image_url: str) -> bool:
    listing = compact_text(listing_image_url)
    if not listing or is_bad_product_image_url(listing):
        return False
    detail = compact_text(detail_image_url)
    if not detail or is_bad_product_image_url(detail):
        return True
    d = detail.lower()
    l = listing.lower()
    if brand == "DISCOVERY":
        if "static-resource.discovery-expedition.com" in l and "static-resource.discovery-expedition.com" not in d:
            return True
        if "/thnail/" in l and "/thnail/" not in d:
            return True
    if brand == "COLUMBIA":
        if "productimages" in l and "productimages" not in d:
            return True
    return False



def try_find_text(driver_or_el, selectors: List[str]) -> str:
    for css in selectors:
        try:
            if css.startswith("meta["):
                elem = driver_or_el.find_element(By.CSS_SELECTOR, css)
                content = compact_text(elem.get_attribute("content"))
                if content:
                    return content
            else:
                elems = driver_or_el.find_elements(By.CSS_SELECTOR, css)
                for elem in elems:
                    txt = get_text_from_element(elem)
                    if txt and len(txt) <= 300:
                        return txt
        except Exception:
            continue
    return ""



def try_find_long_text(driver_or_el, selectors: List[str]) -> str:
    candidates: List[str] = []
    for css in selectors:
        try:
            if css.startswith("meta["):
                elem = driver_or_el.find_element(By.CSS_SELECTOR, css)
                content = compact_text(elem.get_attribute("content"))
                if content:
                    candidates.append(content)
            else:
                elems = driver_or_el.find_elements(By.CSS_SELECTOR, css)
                for elem in elems[:20]:
                    txt = get_text_from_element(elem)
                    if txt:
                        candidates.append(txt)
        except Exception:
            continue
    if not candidates:
        return ""
    candidates = sorted(set(candidates), key=lambda x: len(x), reverse=True)
    return candidates[0][:4000]


def try_find_all_texts(driver_or_el, selectors: List[str], limit: int = 30) -> List[str]:
    out = []
    for css in selectors:
        try:
            if css.startswith("meta["):
                elem = driver_or_el.find_element(By.CSS_SELECTOR, css)
                content = compact_text(elem.get_attribute("content"))
                if content:
                    out.append(content)
            else:
                elems = driver_or_el.find_elements(By.CSS_SELECTOR, css)
                for elem in elems[:limit]:
                    txt = get_text_from_element(elem)
                    if txt:
                        out.append(txt)
        except Exception:
            continue
    return out


def try_find_attr(driver_or_el, selectors: List[str], attr: str) -> str:
    for css in selectors:
        try:
            elems = driver_or_el.find_elements(By.CSS_SELECTOR, css)
            for elem in elems:
                val = get_attr_from_element(elem, attr)
                if val:
                    return val
        except Exception:
            continue
    return ""




def _clean_breadcrumb_parts(parts: List[str]) -> List[str]:
    cleaned = []
    seen = set()
    for raw in parts:
        t = compact_text(raw)
        if not t:
            continue
        low = t.lower()
        if low in {"home", "홈", "전체", "all", ">", "/"}:
            continue
        if len(t) > 80:
            continue
        if low in seen:
            continue
        seen.add(low)
        cleaned.append(t)
    return cleaned


def normalize_breadcrumb_text(text: str) -> str:
    raw = compact_text(text)
    if not raw:
        return ""
    parts = re.split(r"\s*(?:>|/|›|»|→|::|\\|\||＞)\s*", raw)
    parts = _clean_breadcrumb_parts(parts)
    if not parts and raw:
        parts = _clean_breadcrumb_parts([raw])
    return " > ".join(parts[:8])


def extract_breadcrumb_from_jsonld(html_text: str) -> str:
    if not html_text:
        return ""
    try:
        soup = BeautifulSoup(html_text, "html.parser")
        scripts = soup.find_all("script", attrs={"type": re.compile("ld\+json", re.I)})
        for script in scripts:
            raw = script.string or script.get_text(" ", strip=True)
            if not raw:
                continue
            try:
                parsed = json.loads(raw)
            except Exception:
                continue
            queue = parsed if isinstance(parsed, list) else [parsed]
            while queue:
                node = queue.pop(0)
                if isinstance(node, list):
                    queue.extend(node)
                    continue
                if not isinstance(node, dict):
                    continue
                node_type = node.get("@type")
                node_type = " ".join(node_type) if isinstance(node_type, list) else str(node_type or "")
                if "BreadcrumbList" in node_type:
                    items = node.get("itemListElement") or []
                    names = []
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        name = item.get("name")
                        if not name and isinstance(item.get("item"), dict):
                            name = item.get("item", {}).get("name")
                        if name:
                            names.append(str(name))
                    breadcrumb = normalize_breadcrumb_text(" > ".join(names))
                    if breadcrumb:
                        return breadcrumb
                for v in node.values():
                    if isinstance(v, (dict, list)):
                        queue.append(v)
    except Exception:
        return ""
    return ""


def extract_breadcrumb_soup(soup: BeautifulSoup) -> str:
    if soup is None:
        return ""
    try:
        nav = soup.select_one('nav[aria-label*=breadcrumb i]')
        if nav:
            parts = [x.get_text(" ", strip=True) for x in nav.select("a, li, span")[:20]]
            breadcrumb = normalize_breadcrumb_text(" > ".join(parts))
            if breadcrumb:
                return breadcrumb
    except Exception:
        pass
    for css in GENERIC_BREADCRUMB_SELECTORS:
        try:
            nodes = soup.select(css)
            if not nodes:
                continue
            parts = [n.get_text(" ", strip=True) for n in nodes[:20]]
            breadcrumb = normalize_breadcrumb_text(" > ".join(parts))
            if breadcrumb:
                return breadcrumb
        except Exception:
            continue
    return extract_breadcrumb_from_jsonld(str(soup))


def extract_breadcrumb_driver(driver) -> str:
    try:
        for css in GENERIC_BREADCRUMB_SELECTORS:
            elems = driver.find_elements(By.CSS_SELECTOR, css)
            if not elems:
                continue
            parts = [get_text_from_element(el) for el in elems[:20]]
            breadcrumb = normalize_breadcrumb_text(" > ".join(parts))
            if breadcrumb:
                return breadcrumb
    except Exception:
        pass
    try:
        html_text = driver.page_source
        if html_text:
            return extract_breadcrumb_soup(BeautifulSoup(html_text, "html.parser"))
    except Exception:
        pass
    return ""


def is_bad_discovery_desc(text: str) -> bool:
    t = compact_text(text).lower()
    if not t:
        return False
    bad_patterns = [
        "반품/교환 신청기간", "상품 수령일로부터", "마이페이지", "취소/교환/반품",
        "상세 주문내역", "반품 버튼", "교환 버튼", "상품 수령", "교환/반품",
    ]
    hits = sum(1 for p in bad_patterns if p.lower() in t)
    return hits >= 2 or (len(t) < 220 and hits >= 1)




def clean_product_text(text: str) -> str:
    t = compact_text(text)
    if not t:
        return ""
    t = re.sub(r'#광고\b', '', t, flags=re.I)
    t = re.sub(r'\b(sold\s*out|coming\s*soon)\b', '', t, flags=re.I)
    t = re.sub(r'\s+', ' ', t).strip()
    return t


def strip_discovery_prefix(name: str) -> str:
    t = clean_product_text(name)
    patterns = [
        r'^디스커버리\s*익스페디션\s*[|｜:/-]\s*',
        r'^DISCOVERY\s*EXPEDITION\s*[|｜:/-]\s*',
        r'^디스커버리\s*익스페디션\s+',
        r'^DISCOVERY\s*EXPEDITION\s+',
    ]
    for pat in patterns:
        t = re.sub(pat, '', t, flags=re.I)
    return compact_text(t)


def choose_price_text(values: List[str]) -> str:
    cleaned = [compact_text(v) for v in values if compact_text(v)]
    if not cleaned:
        return ""
    preferred = []
    for v in cleaned:
        if "%" in v and ("원" not in v and "," not in v):
            continue
        if re.search(r'\d{1,3}(?:,\d{3})+', v) or "원" in v:
            score = 0
            if "원" in v:
                score += 3
            if v.strip().endswith("원"):
                score += 2
            if len(v) <= 15:
                score += 2
            if re.fullmatch(r'[0-9,\s원]+', v):
                score += 3
            preferred.append((score, v))
    if preferred:
        preferred.sort(key=lambda x: (-x[0], len(x[1])))
        return preferred[0][1]
    return cleaned[0]

def extract_discount_rate_from_text(text: str) -> Optional[float]:
    if not text:
        return None
    m = re.search(r'(\d{1,2})\s*%', str(text))
    if not m:
        return None
    try:
        v = int(m.group(1))
        if 1 <= v <= 95:
            return float(v)
    except Exception:
        return None
    return None

def choose_first_good_text(values: List[str], brand: str = "") -> str:
    cleaned = [compact_text(v) for v in values if compact_text(v)]
    if not cleaned:
        return ""
    if brand == "DISCOVERY":
        good = [v for v in cleaned if not is_bad_discovery_desc(v)]
        if good:
            return sorted(good, key=len, reverse=True)[0]
    return sorted(cleaned, key=len, reverse=True)[0]

def text_contains_brand(text: str, brand_terms: List[str]) -> bool:
    low = (text or "").lower()
    return any(term.lower() in low for term in brand_terms if term)



def _requests_get(url: str):
    try:
        return requests.get(url, headers=REQUEST_HEADERS, timeout=REQUESTS_TIMEOUT)
    except Exception:
        return None


def bs_first_text(soup: BeautifulSoup, selectors: List[str]) -> str:
    for css in selectors:
        try:
            if css.startswith("meta["):
                el = soup.select_one(css)
                if el:
                    val = compact_text(el.get("content") or "")
                    if val:
                        return val
            else:
                for el in soup.select(css)[:20]:
                    txt = compact_text(el.get_text(" ", strip=True))
                    if txt and len(txt) <= 300:
                        return txt
        except Exception:
            continue
    return ""


def bs_long_text(soup: BeautifulSoup, selectors: List[str]) -> str:
    candidates: List[str] = []
    for css in selectors:
        try:
            if css.startswith("meta["):
                el = soup.select_one(css)
                if el:
                    val = compact_text(el.get("content") or "")
                    if val:
                        candidates.append(val)
            else:
                for el in soup.select(css)[:30]:
                    txt = compact_text(el.get_text(" ", strip=True))
                    if txt:
                        candidates.append(txt)
        except Exception:
            continue
    if not candidates:
        return ""
    return sorted(set(candidates), key=lambda x: len(x), reverse=True)[0][:4000]


def bs_first_attr(soup: BeautifulSoup, selectors: List[str], attr: str) -> str:
    for css in selectors:
        try:
            if css.startswith("meta["):
                el = soup.select_one(css)
                if el:
                    val = compact_text(el.get(attr) or el.get("content") or "")
                    if val:
                        return val
            else:
                for el in soup.select(css)[:20]:
                    val = compact_text(el.get(attr) or "")
                    if val:
                        return val
        except Exception:
            continue
    return ""



def _extract_price_ints_from_texts(values: List[str]) -> List[int]:
    out: List[int] = []
    for value in values or []:
        out.extend(extract_price_candidates(value))
    return sorted(set(v for v in out if 1000 <= v <= 5000000))


def _discovery_is_partial_soldout_text(text: str) -> bool:
    t = compact_text(text)
    return "일시품절" in t and "전체품절" not in t


def extract_discovery_payload_price(page_source: str) -> Optional[int]:
    if not page_source:
        return None
    patterns = [
        r'"salePrice"\s*:\s*"?(\d{4,7})"?',
        r'"price"\s*:\s*"?(\d{4,7})"?',
        r'"consumerPrice"\s*:\s*"?(\d{4,7})"?',
        r'"originPrice"\s*:\s*"?(\d{4,7})"?',
    ]
    vals: List[int] = []
    for pat in patterns:
        for m in re.findall(pat, page_source):
            try:
                v = int(str(m))
            except Exception:
                continue
            if 1000 <= v <= 5000000:
                vals.append(v)
    return min(vals) if vals else None


def extract_discovery_price_bundle(driver) -> Tuple[str, str, str]:
    current_vals: List[int] = []
    original_vals: List[int] = []
    sold_out_text = ""

    try:
        detail_current_nodes = driver.find_elements(By.CSS_SELECTOR, "p.css-9luaii")
        detail_original_nodes = driver.find_elements(By.CSS_SELECTOR, "p.css-1orl9yu")
        current_vals.extend(_extract_price_ints_from_texts([get_text_from_element(n) for n in detail_current_nodes]))
        original_vals.extend(_extract_price_ints_from_texts([get_text_from_element(n) for n in detail_original_nodes]))
    except Exception:
        pass

    try:
        card_current_nodes = driver.find_elements(By.CSS_SELECTOR, ".product-price")
        card_original_nodes = driver.find_elements(By.CSS_SELECTOR, ".product-origin-price")
        current_vals.extend(_extract_price_ints_from_texts([get_text_from_element(n) for n in card_current_nodes]))
        original_vals.extend(_extract_price_ints_from_texts([get_text_from_element(n) for n in card_original_nodes]))
    except Exception:
        pass

    try:
        mui_nodes = driver.find_elements(By.CSS_SELECTOR, "p.MuiTypography-root, p[class*='MuiTypography'], span.MuiTypography-root, span[class*='MuiTypography']")
        vals = _extract_price_ints_from_texts([get_text_from_element(n) for n in mui_nodes])
        if vals:
            if not current_vals:
                current_vals.append(min(vals))
            if len(vals) >= 2 and not original_vals:
                original_vals.append(max(vals))
    except Exception:
        pass

    try:
        payload_price = extract_discovery_payload_price(driver.page_source or "")
        if payload_price is not None and not current_vals:
            current_vals.append(payload_price)
    except Exception:
        pass

    current_vals = sorted(set(v for v in current_vals if 1000 <= v <= 5000000))
    original_vals = sorted(set(v for v in original_vals if 1000 <= v <= 5000000))

    current_price = min(current_vals) if current_vals else None
    original_price = max(original_vals) if original_vals else None

    if current_price is None and original_price is not None:
        current_price = original_price
    if current_price is not None and original_price is not None and original_price <= current_price:
        bigger = [v for v in original_vals if v > current_price]
        original_price = max(bigger) if bigger else None

    soldout_keywords = ["전체품절", "품절입니다", "SOLD OUT", "OUT OF STOCK"]
    try:
        soldout_nodes = driver.find_elements(By.XPATH, "//*[contains(text(),'품절') or contains(text(),'SOLD OUT') or contains(text(),'OUT OF STOCK')]")
        sold_texts = [compact_text(get_text_from_element(n)) for n in soldout_nodes]
        sold_texts = [t for t in sold_texts if t and not _discovery_is_partial_soldout_text(t)]
        sold_texts = [t for t in sold_texts if any(k.lower() in t.lower() for k in soldout_keywords)]
        if sold_texts:
            sold_out_text = sold_texts[0]
    except Exception:
        sold_out_text = ""

    return (
        str(current_price) if current_price is not None else "",
        str(original_price) if original_price is not None else "",
        compact_text(sold_out_text),
    )

# ============================================================
# 3. ANALYZER
# ============================================================



def infer_gender(name: str, description: str, raw_gender: str, brand: str = "", source_category: str = "", source_category_url: str = "", breadcrumb_text: str = "") -> str:
    resolved = resolve_master_gender(brand, source_category_url, source_category, breadcrumb_text, raw_gender, name, description)
    if resolved:
        return resolved
    blob = f"{name} {description} {raw_gender}".lower()
    if any(x in blob for x in ["키즈", "kids", "kid", "junior", "juniors", "youth", "boy", "boys", "girl", "girls", "toddler", "infant", "children"]):
        return "키즈"
    if any(x in blob for x in ["여성", "우먼", "women", "womens", "woman", "(w)", " w "]):
        return "여성"
    if any(x in blob for x in ["남성", "맨즈", "men", "mens", "man", "(m)", " m "]):
        return "남성"
    if any(x in blob for x in ["공용", "유니섹스", "unisex"]):
        return "공용"
    return "공용"


def infer_season(name: str, description: str, raw_season: str) -> str:
    blob = f"{name} {description} {raw_season}".lower()
    if any(x in blob for x in ["spring", "봄"]):
        return "봄"
    if any(x in blob for x in ["summer", "여름", "썸머"]):
        return "여름"
    if any(x in blob for x in ["fall", "autumn", "가을"]):
        return "가을"
    if any(x in blob for x in ["winter", "겨울"]):
        return "겨울"
    return safe_text(raw_season) or "미분류"






def _contains_any(text: str, keywords: List[str]) -> bool:
    t = safe_text(text).lower()
    return any(k.lower() in t for k in keywords if k)


def _item_keyword_map() -> Dict[str, List[str]]:
    return {
        "자켓": ["자켓", "재킷", "jacket", "windbreaker", "바람막이", "shell", "anorak", "parka", "파카", "breaker", "track top", "트랙탑", "우븐자켓", "방풍자켓"],
        "팬츠": ["팬츠", "바지", "pants", "pant", "trouser", "slacks", "슬랙스", "cargo", "카고", "jogger", "조거", "shorts", "short", "쇼츠", "반바지", "skort", "스커트", "5 pocket", "5-pocket", "pocket pant", "치노", "chino", "legging", "레깅스"],
        "플리스": ["플리스", "fleece", "boa", "보아"],
        "다운": ["다운", "패딩", "down", "duck down", "goose down", "덕다운", "구스다운", "puffer", "insulated jacket", "인슐레이티드"],
        "베스트": ["베스트", "vest"],
        "후디": ["후디", "후드", "hoodie", "hood", "sweatshirt", "맨투맨", "sweat"],
        "티셔츠": ["티셔츠", "tee", "tees", "t-shirt", "t shirt", "반팔", "긴팔", "jersey", "half zip", "half-zip", "집업티", "zip tee", "zip-tee", "short sleeve", "long sleeve", "sleeve", "crewneck", "크루넥", "라운드티", "폴로티", "polo shirt", "polo tee"],
        "셔츠": ["셔츠", "shirt", "overshirt"],
        "슈즈": ["슈즈", "신발", "shoe", "shoes", "boot", "boots", "sneaker", "sneakers", "trail shoe", "trail running", "등산화", "부츠", "sandal", "sandals", "샌들", "slide", "slides", "슬리퍼", "outdry", "konos", "peakfreak", "crestwood", "thrive revive", "clog", "omni-max", "omni max"],
        "장갑": ["장갑", "glove", "gloves", "mitt", "mittens"],
        "백": ["백", "가방", "bag", "bags", "backpack", "pack", "백팩", "배낭", "duffel", "더플", "tote", "토트", "sling", "슬링", "body bag", "bodybag", "크로스백", "숄더백", "파우치"],
        "ACC": ["acc", "accessory", "accessories", "모자", "cap", "hat", "beanie", "비니", "bucket", "버킷", "visor", "양말", "sock", "socks", "belt", "벨트", "머플러", "워머", "넥게이터", "헤어밴드"],
    }


def extract_discovery_listing_context(driver, listing_url: str) -> str:
    parts: List[str] = []
    seen = set()

    def _push(value: str):
        v = compact_text(value)
        if not v:
            return
        low = v.lower()
        if low in seen:
            return
        seen.add(low)
        parts.append(v)

    try:
        current_url = canonicalize_url(getattr(driver, "current_url", "") or listing_url)
        for src in [current_url, listing_url]:
            m = re.search(r'/display/([A-Z0-9]+)', src or '', flags=re.I)
            if m:
                _push(m.group(1).upper())
    except Exception:
        pass

    try:
        selected = driver.find_elements(By.CSS_SELECTOR, '#selectedCategory a, #selectedCategory span, #selectedCategory p')
        for el in selected[:12]:
            _push(get_text_from_element(el))
    except Exception:
        pass

    try:
        selected_menu = driver.find_elements(By.CSS_SELECTOR, '#category a.is-selected p, #category a.is-selected, .is-selected p, .is-selected')
        for el in selected_menu[:12]:
            _push(get_text_from_element(el))
    except Exception:
        pass

    try:
        title = compact_text(getattr(driver, 'title', '') or '')
        if title:
            _push(title)
    except Exception:
        pass

    try:
        html_text = driver.page_source or ''
        if html_text:
            for token in re.findall(r'/display/([A-Z0-9]+)', html_text, flags=re.I)[:12]:
                _push(token.upper())
    except Exception:
        pass

    return ' '.join(parts)


def extract_columbia_listing_context(driver, listing_url: str) -> str:
    parts: List[str] = []
    seen = set()

    def _push(value: str):
        v = compact_text(value)
        if not v:
            return
        low = v.lower()
        if low in seen:
            return
        seen.add(low)
        parts.append(v)

    try:
        current_url = canonicalize_url(getattr(driver, "current_url", "") or listing_url)
        for src in [current_url, listing_url]:
            _push(src)
    except Exception:
        pass

    selectors = [
        "nav a", ".breadcrumb a", ".breadcrumb li", "h1", "h2",
        ".title", ".tit", ".location a", ".location li",
        ".category a.on", ".category .on", ".cate a.on", ".cate .on",
        "a[href*='product/list']"
    ]
    for css in selectors:
        try:
            elems = driver.find_elements(By.CSS_SELECTOR, css)
            for el in elems[:30]:
                txt = get_text_from_element(el)
                href = get_attr_from_element(el, "href")
                if txt:
                    _push(txt)
                if href and ("cno=" in href or "gdv=" in href):
                    _push(href)
        except Exception:
            continue

    try:
        title = compact_text(getattr(driver, "title", "") or "")
        if title:
            _push(title)
    except Exception:
        pass

    return " ".join(parts)


def choose_driver_image_url(driver_or_el, selectors: List[str]) -> str:
    def _from_elem(elem):
        for attr in ["src", "data-src", "data-original", "currentSrc", "content"]:
            try:
                val = compact_text(elem.get_attribute(attr))
            except Exception:
                val = ""
            if val and not val.startswith("data:") and any(ext in val.lower() for ext in [".jpg", ".jpeg", ".png", ".webp"]):
                return val
        try:
            srcset = compact_text(elem.get_attribute("srcset"))
        except Exception:
            srcset = ""
        if srcset:
            first = srcset.split(",")[0].strip().split(" ")[0].strip()
            if first and not first.startswith("data:"):
                return first
        return ""

    for css in selectors:
        try:
            elems = driver_or_el.find_elements(By.CSS_SELECTOR, css)
            for elem in elems:
                val = _from_elem(elem)
                if val:
                    return val
        except Exception:
            continue
    return ""


def _discovery_category_tokens(text: str) -> List[str]:
    t = safe_text(text)
    if not t:
        return []
    low = t.lower()
    hits: List[str] = []
    explicit = [
        ('슈즈', ['dxmb03', '신발', '슈즈', 'shoe', 'shoes', 'boots', 'boot', 'sandal', 'sandals']),
        ('자켓', ['b17', '자켓', '재킷', 'jacket']),
        ('자켓', ['b15', '경량자켓', '라이트 자켓', 'light jacket']),
        ('자켓', ['b10', '바람막이', 'windbreaker']),
        ('자켓', ['b12', '아노락', 'anorak']),
        ('팬츠', ['팬츠', '바지', 'pants', 'pant', 'shorts', 'short', 'skort']),
        ('티셔츠', ['티셔츠', '반팔티', '긴팔티', '반팔', '긴팔', 'tee', 't-shirt', 'jersey', 'top', 'tops']),
        ('후디', ['후드티', '후디', '후드', 'hoodie', 'sweatshirt', '맨투맨']),
        ('플리스', ['플리스', 'fleece', 'boa', '보아']),
        ('다운', ['다운', '패딩', 'down', 'puffer']),
        ('베스트', ['베스트', 'vest']),
        ('백', ['가방', '백', 'backpack', 'bag', 'bags', 'sling', 'tote']),
        ('ACC', ['용품', '모자', 'cap', 'hat', 'accessory', 'accessories', 'acc']),
    ]
    for item, kws in explicit:
        if any(kw in low for kw in kws):
            hits.append(item)
    return hits


def normalize_source_category(text: str) -> str:
    t = safe_text(text)
    if not t:
        return ""
    low = t.lower()

    discovery_hits = _discovery_category_tokens(t)
    if discovery_hits:
        priority = ["슈즈", "자켓", "팬츠", "티셔츠", "후디", "플리스", "다운", "베스트", "백", "ACC"]
        for item in priority:
            if item in discovery_hits:
                return item

    explicit_pairs = [
        ("슈즈", ["신발", "슈즈", "스니커즈", "트레일러닝", "등산화", "샌들", "슬리퍼", "레인 부츠", "omni-max", "omni max", "boot", "shoe"]),
        ("백", ["가방", "백팩", "크로스", "토트백", "힙색", "슬링백", "숄더", "백"]),
        ("장갑", ["장갑"]),
        ("ACC", ["모자", "용품", "스틱", "양말", "지갑", "acc", "accessory"]),
        ("베스트", ["베스트", "vest"]),
        ("다운", ["다운", "패딩", "슬림다운"]),
        ("플리스", ["플리스", "fleece", "boa", "보아"]),
        ("후디", ["맨투맨", "후드티", "후디"]),
        ("티셔츠", ["티셔츠", "라운드티", "폴로티", "반팔", "긴팔", "상의", "쿨링"]),
        ("팬츠", ["하의", "긴바지", "카고/조거", "반바지", "팬츠", "바지"]),
        ("자켓", ["아우터", "방수자켓", "바람막이", "경량패딩", "인터체인지", "자켓", "재킷", "아노락"]),
    ]
    for item, kws in explicit_pairs:
        if any(kw.lower() in low for kw in kws):
            return item

    scores: Dict[str, int] = {}
    for item, keys in _item_keyword_map().items():
        score = 0
        for k in keys:
            kk = k.lower()
            if kk in low:
                score += 1
                if re.search(rf'(^|[^a-z가-힣]){re.escape(kk)}([^a-z가-힣]|$)', low):
                    score += 1
        if score:
            scores[item] = score
    if not scores:
        return ""
    return sorted(scores.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]


def infer_item_category(name: str, description: str, source_category: str = "") -> str:
    name_text = safe_text(name)
    desc_text = safe_text(description)
    source_text = safe_text(source_category)
    name_blob = name_text.lower()
    desc_blob = desc_text.lower()
    src_blob = source_text.lower()
    full_blob = " ".join(x for x in [name_blob, desc_blob, src_blob] if x)

    src_item = normalize_source_category(source_text)
    if src_item:
        return src_item

    tail_checks = [
        ("슈즈", [r'\b(konos|peakfreak|crestwood|clog|sneaker|sneakers|shoe|shoes|boot|boots|sandals?|slides?)\b', r'\btrail\s+atr\b']),
        ("티셔츠", [r'\bshort\s+sleeve\b', r'\blong\s+sleeve\b', r'\btee\b', r'\bt-shirt\b', r'\bjersey\b', r'\bcrewneck\b', r'\bround\s*tee\b']),
        ("후디", [r'\bhoodie\b', r'\bhood\b', r'\bsweatshirt\b']),
        ("플리스", [r'\bfleece\b', r'\bboa\b']),
        ("베스트", [r'\bvest\b']),
        ("셔츠", [r'\bshirt\b']),
        ("팬츠", [r'\bcargo\s+pant\b', r'\bcargo\s+pants\b', r'\bjogger\b', r'\bpant\b', r'\bpants\b', r'\bshorts\b', r'\bshort\b', r'\bskort\b', r'\bchino\b']),
        ("자켓", [r'\bwindbreaker\b', r'\banorak\b', r'\bjacket\b', r'\bparka\b', r'\bshell\b']),
    ]
    for item, patterns in tail_checks:
        if any(re.search(p, name_blob) for p in patterns):
            return item

    scores: Dict[str, int] = {}
    for item, keys in _item_keyword_map().items():
        score = 0
        for k in keys:
            kk = k.lower()
            if not kk:
                continue
            if kk in name_blob:
                score += 6
                if re.search(rf'(^|[^a-z가-힣]){re.escape(kk)}([^a-z가-힣]|$)', name_blob):
                    score += 2
            if kk in desc_blob:
                score += 2
        if score:
            scores[item] = score

    if scores:
        return sorted(scores.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]

    if any(x in full_blob for x in ["set", "세트", "kit", "키트", "워머", "양말", "sock", "socks", "belt", "벨트", "cap", "hat", "모자"]):
        return "ACC"
    if full_blob:
        return normalize_source_category(full_blob) or "ACC"
    return "ACC"

def extract_raw_keywords(name: str, description: str) -> List[str]:
    text = f"{name} {description}"
    hits: List[str] = []
    for raw_term in BRAND_TERM_MAP.keys():
        if re.search(re.escape(raw_term), text, flags=re.IGNORECASE):
            hits.append(raw_term)
    candidates = re.findall(r"\b[A-Z][A-Z0-9\-\.\+]{2,}\b|\b\d(?:\.\d)?L\b", text)
    existing = {x.lower() for x in hits}
    for c in candidates:
        if c.lower() not in existing and c.lower() not in NOISE_WORDS:
            hits.append(c)
    return unique_preserve_order(hits)


def map_standard_attributes(raw_keywords: List[str], name: str, description: str) -> List[str]:
    mapped = []
    blob = f"{name} {description}"
    for raw in raw_keywords:
        std = BRAND_TERM_MAP.get(raw.upper()) or BRAND_TERM_MAP.get(raw)
        if std:
            mapped.append(std)
    for raw, std in BRAND_TERM_MAP.items():
        if re.search(re.escape(raw), blob, flags=re.IGNORECASE):
            mapped.append(std)
    return unique_preserve_order(mapped)


def resolve_conflicts(attributes: List[str]) -> List[str]:
    attrs = set(attributes)
    for left, right, winner in CONFLICT_RULES:
        if left in attrs and right in attrs:
            if winner == left:
                attrs.discard(right)
            elif winner == right:
                attrs.discard(left)
    return sorted(attrs, key=lambda x: (-ATTRIBUTE_PRIORITY.get(x, 0), x))


def select_dominant_attribute(attributes: List[str]) -> str:
    if not attributes:
        return "기타"
    return max(attributes, key=lambda x: ATTRIBUTE_PRIORITY.get(x, 0))


def classify_grade(attributes: List[str], dominant_attribute: str) -> str:
    attrs = set(attributes)
    if dominant_attribute == "고어텍스_PRO" or "3L" in attrs:
        return "High"
    if dominant_attribute == "고어텍스" or "2.5L" in attrs or "다운" in attrs:
        return "Mid"
    return "Entry"


def classify_shell_type(attributes: List[str], name: str, description: str) -> str:
    attrs = set(attributes)
    blob = f"{name} {description}".lower()
    hard_score = 0
    soft_score = 0
    if "방수" in attrs or "고어텍스" in attrs or "고어텍스_PRO" in attrs:
        hard_score += 3
    if "3L" in attrs or "2.5L" in attrs or "2L" in attrs:
        hard_score += 2
    if any(x in blob for x in ["심실링", "seamsealed", "seam-sealed", "shell"]):
        hard_score += 2
    if "방풍" in attrs:
        soft_score += 2
    if "스트레치" in attrs:
        soft_score += 2
    if any(x in blob for x in ["softshell", "soft shell", "활동성"]):
        soft_score += 2
    if hard_score > soft_score and hard_score >= 2:
        return "Hard Shell"
    if soft_score > hard_score and soft_score >= 2:
        return "Soft Shell"
    return "Unknown"


def classify_price_band(price: Optional[int]) -> str:
    if price is None:
        return "기타"
    if price < 100000:
        return "0-9.9만"
    if price < 200000:
        return "10-19.9만"
    if price < 300000:
        return "20-29.9만"
    if price < 500000:
        return "30-49.9만"
    return "50만+"


def classify_positioning_y(price: Optional[int]) -> str:
    if price is None:
        return "Mass"
    if price < 150000:
        return "Mass"
    if price < 350000:
        return "Premium"
    return "Luxury"


def classify_positioning_x(attrs: List[str], dominant_attribute: str, shell_type: str) -> str:
    score = 0
    if dominant_attribute in {"고어텍스_PRO", "고어텍스", "폴라텍", "코듀라"}:
        score += 3
    if any(x in attrs for x in ["3L", "2.5L", "2L", "방수", "다운"]):
        score += 2
    if shell_type == "Hard Shell":
        score += 2
    if score >= 6:
        return "Extreme"
    if score >= 3:
        return "Performance"
    return "Lifestyle"





def extract_price_candidates(text: str) -> List[int]:
    if not text:
        return []
    raw = str(text).replace("\xa0", " ")
    candidates = re.findall(r'\d{1,3}(?:,\d{3})+|\d{4,7}', raw)
    vals = []
    for c in candidates:
        try:
            v = int(re.sub(r'[^0-9]', '', c))
        except Exception:
            continue
        if 1000 <= v <= 5000000:
            vals.append(v)
    return sorted(set(vals))


def select_current_original_price(price_text: str, original_text: str) -> Tuple[Optional[int], Optional[int]]:
    cand_current = extract_price_candidates(price_text)
    cand_original = extract_price_candidates(original_text)

    current_price = min(cand_current) if cand_current else None
    original_price = max(cand_original) if cand_original else None

    if current_price is None and original_price is not None:
        current_price = original_price
    if original_price is None and current_price is not None:
        original_price = current_price
    if current_price is not None and original_price is not None and original_price < current_price:
        original_price = current_price

    return current_price, original_price

def analyze_product(raw: ProductRaw) -> ProductAnalyzed:
    raw.name = clean_product_text(raw.name)
    raw.description = clean_product_text(raw.description)
    current_price = parse_price_to_int(raw.price_text)
    original_price = parse_price_to_int(raw.original_price_text)
    if current_price is None and original_price is not None:
        current_price = original_price
    if original_price is None and current_price is not None:
        original_price = current_price

    raw_keywords = extract_raw_keywords(raw.name, raw.description)
    std_attrs = map_standard_attributes(raw_keywords, raw.name, raw.description)
    resolved_attrs = resolve_conflicts(std_attrs)
    dominant = select_dominant_attribute(resolved_attrs)
    master_item = resolve_master_category(raw.brand, getattr(raw, "source_category_url", ""), getattr(raw, "source_category", ""), getattr(raw, "breadcrumb_text", ""), raw.name, raw.description)
    inferred_item = master_item or infer_item_category(raw.name, raw.description, " ".join([getattr(raw, 'breadcrumb_text', ''), getattr(raw, 'source_category', ''), getattr(raw, 'source_category_url', ''), getattr(raw, 'source_url', ''), getattr(raw, 'product_url', '')]))
    grade = classify_grade(resolved_attrs, dominant)
    shell_type = classify_shell_type(resolved_attrs, raw.name, raw.description)
    price_band = classify_price_band(current_price)
    pos_y = classify_positioning_y(current_price)
    pos_x = classify_positioning_x(resolved_attrs, dominant, shell_type)

    return ProductAnalyzed(
        brand=("DISCOVERY" if raw.brand == "DISCOVERY" else raw.brand),
        source_url=raw.source_url,
        product_url=raw.product_url,
        name=raw.name,
        description=raw.description,
        image_url=raw.image_url,
        current_price=current_price,
        original_price=original_price,
        discount_rate=(calc_discount_rate(current_price, original_price) if calc_discount_rate(current_price, original_price) is not None else extract_discount_rate_from_text(f"{raw.price_text} {raw.original_price_text} {raw.name} {raw.description}")),
        sold_out=bool(safe_text(raw.sold_out_text)),
        gender=infer_gender(raw.name, raw.description, raw.gender_text, raw.brand, getattr(raw, "source_category", ""), getattr(raw, "source_category_url", ""), getattr(raw, "breadcrumb_text", "")),
        season=infer_season(raw.name, raw.description, raw.season_text),
        item_category=resolve_item_category_value(
            raw.name,
            raw.description,
            getattr(raw, 'item_category', ''),
            getattr(raw, 'source_category', ''),
            getattr(raw, 'source_category_url', ''),
            getattr(raw, 'source_url', ''),
            getattr(raw, 'product_url', ''),
            getattr(raw, 'breadcrumb_text', ''),
        ),
        raw_keywords=raw_keywords,
        standard_attributes=resolved_attrs,
        dominant_attribute=dominant,
        grade=grade,
        shell_type=shell_type,
        price_band=price_band,
        positioning_y=pos_y,
        positioning_x=pos_x,
        attribute_coverage_flag="OK",
        source_category=raw.source_category,
        source_category_url=raw.source_category_url,
        breadcrumb_text=getattr(raw, "breadcrumb_text", ""),
        crawled_at=raw.crawled_at,
    )


# ============================================================
# 4. KEYWORD DISCOVERY
# ============================================================
def discover_keywords(products: List[ProductAnalyzed]) -> pd.DataFrame:
    rows = []
    brand_keyword_counter: Dict[str, Dict[str, int]] = {}
    global_counter: Dict[str, int] = {}

    for p in products:
        raws = unique_preserve_order([x.upper() for x in p.raw_keywords if safe_text(x)])
        brand_keyword_counter.setdefault(p.brand, {})
        for kw in raws:
            if kw.lower() in NOISE_WORDS or len(kw) < 2:
                continue
            brand_keyword_counter[p.brand][kw] = brand_keyword_counter[p.brand].get(kw, 0) + 1
            global_counter[kw] = global_counter.get(kw, 0) + 1

    for brand, counter in brand_keyword_counter.items():
        total_brand_keywords = sum(counter.values()) or 1
        sorted_items = sorted(counter.items(), key=lambda x: (-x[1], x[0]))
        for kw, cnt in sorted_items:
            global_cnt = global_counter[kw]
            brand_share = round(cnt / total_brand_keywords * 100, 2)
            concentration = round(cnt / max(global_cnt, 1), 3)
            keyword_type = "브랜드 기술" if concentration >= 0.6 and global_cnt >= 2 else "트렌드 기술"
            rows.append({
                "brand": brand,
                "keyword": kw,
                "count": cnt,
                "global_count": global_cnt,
                "brand_share_pct": brand_share,
                "brand_concentration": concentration,
                "keyword_type": keyword_type,
            })

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=[
            "brand", "keyword", "count", "global_count", "brand_share_pct", "brand_concentration", "keyword_type"
        ])
    return df.sort_values(["brand", "count", "brand_concentration"], ascending=[True, False, False]).reset_index(drop=True)


# ============================================================
# 5. CRAWLER
# ============================================================
class AutoCompetitorCrawler:
    def __init__(self, headless: bool = True):
        self.headless = headless
        self.driver = self._build_driver()
        self.wait = WebDriverWait(self.driver, DEFAULT_WAIT_SEC)

    def _build_driver(self):
        options = Options()
        if self.headless:
            options.add_argument("--headless=new")
        options.add_argument("--window-size=1600,2400")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--lang=ko-KR")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(60)
        try:
            driver.execute_cdp_cmd("Network.enable", {})
            driver.execute_cdp_cmd("Network.setBlockedURLs", {
                "urls": [
                    "*.woff", "*.woff2", "*.ttf", "*.otf",
                    "*google-analytics*", "*googletagmanager*", "*doubleclick*",
                    "*facebook*", "*analytics*", "*tracker*"
                ]
            })
        except Exception:
            pass
        return driver

    def close(self):
        try:
            self.driver.quit()
        except Exception:
            pass

    def _safe_get(self, url: str) -> bool:
        try:
            self.driver.get(url)
            time.sleep(0.6)
            return True
        except Exception:
            return False

    def _scroll_to_end(self):
        last_height = 0
        stable_count = 0
        for _ in range(25):
            try:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(SCROLL_PAUSE_SEC)
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    stable_count += 1
                else:
                    stable_count = 0
                    last_height = new_height
                if stable_count >= 3:
                    break
            except Exception:
                break

    def _extract_all_links(self) -> List[str]:
        hrefs = []
        try:
            anchors = self.driver.find_elements(By.CSS_SELECTOR, "a[href]")
        except Exception:
            return []
        for a in anchors:
            href = get_attr_from_element(a, "href")
            if href and href.startswith("http"):
                hrefs.append(canonicalize_url(href))
        return unique_preserve_order(hrefs)

    def _is_deny_url(self, url: str, cfg: BrandConfig) -> bool:
        low = url.lower()
        deny = set(GENERIC_DENY_URL_KEYWORDS + cfg.deny_url_keywords)
        return any(x.lower() in low for x in deny)

    def _is_product_url(self, url: str, cfg: BrandConfig) -> bool:
        low = url.lower()
        force = cfg.force_allow_url_keywords or []
        if any(x.lower() in low for x in force):
            return True
        keys = unique_preserve_order(cfg.product_url_keywords + GENERIC_PRODUCT_URL_KEYWORDS)
        return any(x.lower() in low for x in keys)

    def _is_listing_url(self, url: str, cfg: BrandConfig) -> bool:
        low = url.lower()
        keys = unique_preserve_order(cfg.listing_url_keywords + GENERIC_LISTING_URL_KEYWORDS)
        return any(x.lower() in low for x in keys)

    def _passes_brand_filter(self, url: str, cfg: BrandConfig, text_blob: str = "") -> bool:
        if not cfg.brand_terms:
            return True
        combined = f"{url} {text_blob}".lower()
        return any(term.lower() in combined for term in cfg.brand_terms if term)

    def discover_listing_urls(self, cfg: BrandConfig) -> List[str]:
        explicit = [canonicalize_url(u) for u in (cfg.seed_urls or []) if canonicalize_url(u)]
        explicit = [u for u in explicit if same_domain(u, cfg.domain)]
        if explicit and not (len(explicit) == 1 and explicit[0].rstrip('/') in {f"https://{cfg.domain}", f"https://{cfg.domain}/"}):
            print(f"  - using explicit PLP seeds: {len(explicit)}")
            return unique_preserve_order(explicit)[:MAX_DISCOVERED_LISTING_URLS]

        discovered: List[str] = []
        seen: Set[str] = set()
        queue: List[Tuple[str, int]] = [(u, 0) for u in cfg.seed_urls]
        max_depth = 2 if cfg.brand == "COLUMBIA" else 1

        while queue and len(discovered) < MAX_DISCOVERED_LISTING_URLS:
            current_url, depth = queue.pop(0)
            if current_url in seen:
                continue
            seen.add(current_url)
            if depth == 0:
                print(f"  - seed: {current_url}")
            discovered.append(current_url)

            if not self._safe_get(current_url):
                continue
            self._scroll_to_end()
            links = self._extract_all_links()

            for link in links:
                if len(discovered) + len(queue) >= MAX_DISCOVERED_LISTING_URLS:
                    break
                if not same_domain(link, cfg.domain):
                    continue
                if self._is_deny_url(link, cfg):
                    continue
                if self._is_product_url(link, cfg):
                    continue
                if not self._is_listing_url(link, cfg):
                    continue
                if cfg.brand == "DISCOVERY" and any(x in link.lower() for x in ["style-pick", "discoverer-picks", "/brand/style-pick"]):
                    continue
                if depth + 1 > max_depth:
                    continue
                if link not in seen:
                    queue.append((link, depth + 1))

        return unique_preserve_order(discovered)[:MAX_DISCOVERED_LISTING_URLS]

    def collect_product_urls_from_listing(self, listing_url: str, cfg: BrandConfig) -> List[dict]:
        product_urls: List[dict] = []
        if not self._safe_get(listing_url):
            return product_urls

        source_context = listing_url
        try:
            self.wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
        except Exception:
            pass
        try:
            time.sleep(1.6)
        except Exception:
            pass

        if cfg.brand == "DISCOVERY":
            source_context = extract_discovery_listing_context(self.driver, listing_url) or listing_url
        elif cfg.brand == "COLUMBIA":
            source_context = extract_columbia_listing_context(self.driver, listing_url) or listing_url

        master_item = resolve_master_category(cfg.brand, listing_url, source_context, source_context)

        self._scroll_to_end()

        candidate_selectors = [
            "a[href*='product-detail']",
            "a[href*='/product/detail']",
            "a[href*='/product/view']",
            "a[href*='product_no=']",
            "a[href*='gdno=']",
            "a[href*='/shop/goods']",
        ]
        if cfg.brand == "DISCOVERY":
            candidate_selectors = [
                ".MuiGrid-root a[href*='product-detail']",
                "a[href*='product-detail']",
                "a[href*='/product-detail/']",
                "a[href*='DXSH']",
                "a[href*='DX']",
            ] + candidate_selectors
        elif cfg.brand == "COLUMBIA":
            candidate_selectors = [
                "a[href*='/product/view']",
                "a[href*='gdno=']",
                "a[href*='product_no=']",
                "a[href*='/shop/goods']",
            ] + candidate_selectors

        seen_urls = set()
        for css in candidate_selectors:
            try:
                anchors = self.driver.find_elements(By.CSS_SELECTOR, css)
            except Exception:
                continue
            for node in anchors:
                a = node
                try:
                    if node.tag_name.lower() == 'img':
                        a = node.find_element(By.XPATH, './ancestor::a[1]')
                except Exception:
                    a = node
                try:
                    if not a.is_displayed():
                        continue
                except Exception:
                    pass
                href = canonicalize_url(get_attr_from_element(a, "href"))
                if not href or not same_domain(href, cfg.domain):
                    continue
                if self._is_deny_url(href, cfg) or not self._is_product_url(href, cfg) or not self._passes_brand_filter(href, cfg):
                    continue
                if href in seen_urls:
                    continue
                seen_urls.add(href)
                product_urls.append({
                    "url": href,
                    "source_category": (master_item or source_context),
                    "source_category_url": listing_url,
                    "image_url": best_image_from_element(a),
                })

        if not product_urls:
            links = self._extract_all_links()
            for link in links:
                if not same_domain(link, cfg.domain):
                    continue
                if self._is_deny_url(link, cfg):
                    continue
                if self._is_product_url(link, cfg) and self._passes_brand_filter(link, cfg):
                    if link in seen_urls:
                        continue
                    seen_urls.add(link)
                    product_urls.append({"url": link, "source_category": source_context, "source_category_url": listing_url, "image_url": ""})

        return product_urls


    def crawl_product_detail_requests(self, product_url: str, source_url: str, cfg: BrandConfig) -> Optional[ProductRaw]:
        if cfg.detail_mode == "selenium_only":
            return None
        resp = _requests_get(product_url)
        if resp is None or not getattr(resp, "ok", False) or not getattr(resp, "text", ""):
            return None
        try:
            soup = BeautifulSoup(resp.text, "html.parser")
            title = bs_first_text(soup, GENERIC_NAME_SELECTORS) or compact_text(soup.title.get_text(" ", strip=True) if soup.title else "")
            price_text = bs_first_text(soup, GENERIC_PRICE_SELECTORS)
            original_price_text = bs_first_text(soup, GENERIC_ORIGINAL_PRICE_SELECTORS)
            desc = bs_long_text(soup, GENERIC_DESC_SELECTORS)
            image_url = bs_first_attr(soup, GENERIC_IMAGE_SELECTORS, "src") or bs_first_attr(soup, GENERIC_IMAGE_SELECTORS, "content")
            sold_out_text = bs_first_text(soup, GENERIC_SOLDOUT_SELECTORS)
            gender_text = bs_first_text(soup, GENERIC_GENDER_SELECTORS)
            season_text = bs_first_text(soup, GENERIC_SEASON_SELECTORS)
            breadcrumb_text = extract_breadcrumb_soup(soup) or normalize_breadcrumb_text(source_url)

            if not title and not price_text and not desc:
                return None

            sanity_blob = f"{title} {desc}"
            if cfg.brand_terms and cfg.domain in {"www.k-village.co.kr"}:
                if not text_contains_brand(sanity_blob, cfg.brand_terms):
                    return None

            cp, op = select_current_original_price(price_text, original_price_text)
            final_title = title
            if cfg.brand == "COLUMBIA":
                try:
                    final_title = normalize_columbia_name(title, resp.text)
                except Exception:
                    final_title = title

            return ProductRaw(
                brand=cfg.brand,
                source_url=source_url,
                product_url=product_url,
                name=compact_text(final_title),
                description=compact_text(desc),
                price_text=str(cp) if cp is not None else compact_text(price_text),
                original_price_text=str(op) if op is not None else compact_text(original_price_text),
                image_url=compact_text(image_url),
                sold_out_text=compact_text(sold_out_text),
                gender_text=compact_text(gender_text),
                season_text=compact_text(season_text),
                breadcrumb_text=compact_text(breadcrumb_text),
                crawled_at=TODAY_STR,
            )
        except Exception:
            return None


    def crawl_product_detail(self, product_url: str, source_url: str, cfg: BrandConfig) -> Optional[ProductRaw]:
        # Discovery: selenium-first to avoid capturing policy text from server-rendered fallback HTML
        if cfg.detail_mode == "selenium_only":
            if not self._safe_get(product_url):
                return None
            try:
                self.wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
            except Exception:
                pass
            try:
                WebDriverWait(self.driver, 6).until(
                    lambda d: len(d.find_elements(By.CSS_SELECTOR, "p.css-9luaii, p.css-1orl9yu, .product-price, .product-origin-price, p.MuiTypography-root, p[class*='MuiTypography']")) > 0
                )
            except Exception:
                pass
            try:
                time.sleep(3.0)
            except Exception:
                pass

            name_selectors = cfg.detail_name_selectors or GENERIC_NAME_SELECTORS
            desc_selectors = cfg.detail_desc_selectors or GENERIC_DESC_SELECTORS
            image_selectors = cfg.detail_image_selectors or GENERIC_IMAGE_SELECTORS

            title = strip_discovery_prefix(clean_product_text(try_find_text(self.driver, name_selectors)))
            if not title:
                try:
                    title = strip_discovery_prefix(clean_product_text(self.driver.title))
                except Exception:
                    title = ""

            price_text, original_price_text, sold_out_text = extract_discovery_price_bundle(self.driver)
            desc_candidates = try_find_all_texts(self.driver, desc_selectors, limit=40)
            desc = clean_product_text(choose_first_good_text(desc_candidates, cfg.brand))
            if is_bad_discovery_desc(desc):
                desc = ""
            image_url = choose_driver_image_url(self.driver, image_selectors) or try_find_attr(self.driver, image_selectors, "src") or try_find_attr(self.driver, image_selectors, "content")
            breadcrumb_text = extract_breadcrumb_driver(self.driver) or normalize_breadcrumb_text(source_url)
            gender_text = try_find_text(self.driver, GENERIC_GENDER_SELECTORS)
            season_text = try_find_text(self.driver, GENERIC_SEASON_SELECTORS)

            if not title and not price_text and not desc and not sold_out_text:
                return None

            cp, op = select_current_original_price(price_text, original_price_text)
            if cp is None:
                try:
                    payload_price = extract_discovery_payload_price(self.driver.page_source or "")
                except Exception:
                    payload_price = None
                if payload_price is not None:
                    cp = payload_price

            final_title = title
            if cfg.brand == "COLUMBIA":
                try:
                    final_title = normalize_columbia_name(title, self.driver.page_source)
                except Exception:
                    final_title = title

            return ProductRaw(
                brand=cfg.brand,
                source_url=source_url,
                product_url=product_url,
                name=compact_text(final_title),
                description=compact_text(desc),
                price_text=str(cp) if cp is not None else compact_text(price_text),
                original_price_text=str(op) if op is not None else compact_text(original_price_text),
                image_url=compact_text(image_url),
                sold_out_text=compact_text(sold_out_text),
                gender_text=compact_text(gender_text),
                season_text=compact_text(season_text),
                breadcrumb_text=compact_text(breadcrumb_text),
                crawled_at=TODAY_STR,
            )
        # requests-first for speed
        resp = _requests_get(product_url)
        if resp is not None and getattr(resp, "ok", False) and resp.text:
            try:
                soup = BeautifulSoup(resp.text, "html.parser")
                title = bs_first_text(soup, GENERIC_NAME_SELECTORS) or compact_text(soup.title.get_text(" ", strip=True) if soup.title else "")
                price_text = bs_first_text(soup, GENERIC_PRICE_SELECTORS)
                original_price_text = bs_first_text(soup, GENERIC_ORIGINAL_PRICE_SELECTORS)
                desc = bs_long_text(soup, GENERIC_DESC_SELECTORS)
                image_url = bs_first_attr(soup, GENERIC_IMAGE_SELECTORS, "src") or bs_first_attr(soup, GENERIC_IMAGE_SELECTORS, "content")
                sold_out_text = bs_first_text(soup, GENERIC_SOLDOUT_SELECTORS)
                gender_text = bs_first_text(soup, GENERIC_GENDER_SELECTORS)
                season_text = bs_first_text(soup, GENERIC_SEASON_SELECTORS)
                breadcrumb_text = extract_breadcrumb_soup(soup) or normalize_breadcrumb_text(source_url)

                sanity_blob = f"{title} {desc}"
                if title or price_text or desc:
                    if cfg.brand_terms and cfg.domain in {"www.k-village.co.kr"}:
                        if not text_contains_brand(sanity_blob, cfg.brand_terms):
                            return None
                    cp, op = select_current_original_price(price_text, original_price_text)
                    return ProductRaw(
                        brand=cfg.brand,
                        source_url=source_url,
                        product_url=product_url,
                        name=compact_text(title),
                        description=compact_text(desc),
                        price_text=str(cp) if cp is not None else compact_text(price_text),
                        original_price_text=str(op) if op is not None else compact_text(original_price_text),
                        image_url=compact_text(image_url),
                        sold_out_text=compact_text(sold_out_text),
                        gender_text=compact_text(gender_text),
                        season_text=compact_text(season_text),
                        breadcrumb_text=compact_text(breadcrumb_text),
                        crawled_at=TODAY_STR,
                    )
            except Exception:
                pass

        if not self._safe_get(product_url):
            return None

        try:
            time.sleep(0.5)
        except Exception:
            pass

        title = try_find_text(self.driver, GENERIC_NAME_SELECTORS)
        if not title:
            try:
                title = self.driver.title
            except Exception:
                title = ""

        price_text = try_find_text(self.driver, GENERIC_PRICE_SELECTORS)
        original_price_text = try_find_text(self.driver, GENERIC_ORIGINAL_PRICE_SELECTORS)
        desc = try_find_long_text(self.driver, GENERIC_DESC_SELECTORS)
        image_url = try_find_attr(self.driver, GENERIC_IMAGE_SELECTORS, "src") or try_find_attr(self.driver, GENERIC_IMAGE_SELECTORS, "content")
        breadcrumb_text = extract_breadcrumb_driver(self.driver) or normalize_breadcrumb_text(source_url)
        sold_out_text = try_find_text(self.driver, GENERIC_SOLDOUT_SELECTORS)
        gender_text = try_find_text(self.driver, GENERIC_GENDER_SELECTORS)
        season_text = try_find_text(self.driver, GENERIC_SEASON_SELECTORS)

        if not title and not price_text and not desc:
            return None

        sanity_blob = f"{title} {desc}"
        if cfg.brand_terms and cfg.domain in {"www.k-village.co.kr"}:
            if not text_contains_brand(sanity_blob, cfg.brand_terms):
                return None

        cp, op = select_current_original_price(price_text, original_price_text)
        return ProductRaw(
            brand=cfg.brand,
            source_url=source_url,
            product_url=product_url,
            name=compact_text(title),
            description=compact_text(desc),
            price_text=str(cp) if cp is not None else compact_text(price_text),
            original_price_text=str(op) if op is not None else compact_text(original_price_text),
            image_url=compact_text(image_url),
            sold_out_text=compact_text(sold_out_text),
            gender_text=compact_text(gender_text),
            season_text=compact_text(season_text),
            breadcrumb_text=compact_text(breadcrumb_text),
            crawled_at=TODAY_STR,
        )

    def crawl_brand(self, cfg: BrandConfig) -> List[ProductRaw]:
        print(f"\n[CRAWL START] {cfg.brand} :: seeds={len(cfg.seed_urls)}")
        listings = self.discover_listing_urls(cfg)
        product_urls: List[dict] = []

        for listing in listings:
            print(f"  - listing: {listing}")
            try:
                urls = self.collect_product_urls_from_listing(listing, cfg)
                product_urls.extend(urls)
            except Exception as e:
                print(f"    [WARN] listing failed: {e}")

        if len(product_urls) > cfg.max_products:
            product_urls = product_urls[:cfg.max_products]

        print(f"  - discovered product urls: {len(product_urls)}")

        raw_products: List[ProductRaw] = []
        unresolved_urls: List[str] = []

        # Fast path: requests-first in parallel
        with ThreadPoolExecutor(max_workers=max(1, DETAIL_WORKERS)) as ex:
            futures = {ex.submit(self.crawl_product_detail_requests, meta['url'], meta.get('source_category_url',''), cfg): meta for meta in product_urls}
            done_count = 0
            for fut in as_completed(futures):
                done_count += 1
                meta = futures[fut]
                url = meta['url']
                try:
                    item = fut.result()
                    if item and item.name:
                        item.source_category = meta.get('source_category','')
                        item.source_category_url = meta.get('source_category_url','')
                        if should_use_listing_image(cfg.brand, item.image_url, meta.get('image_url','')):
                            item.image_url = meta.get('image_url','')
                        if not item.name and meta.get('listing_name'):
                            item.name = meta.get('listing_name','')
                        if not item.price_text and meta.get('listing_price_text'):
                            item.price_text = meta.get('listing_price_text','')
                        raw_products.append(item)
                    else:
                        unresolved_urls.append(url)
                except Exception:
                    unresolved_urls.append(url)
                if done_count % 20 == 0:
                    print(f"    [OK] {cfg.brand} requests pass {done_count}/{len(product_urls)} | valid {len(raw_products)}")

        # Accurate fallback: selenium only for unresolved URLs
        unresolved_meta = [m for m in product_urls if m['url'] in unresolved_urls]
        for idx, meta in enumerate(unresolved_meta, start=1):
            product_url = meta['url']
            try:
                item = self.crawl_product_detail(product_url, meta.get('source_category_url',''), cfg)
                if item and item.name:
                    item.source_category = meta.get('source_category','')
                    item.source_category_url = meta.get('source_category_url','')
                    if should_use_listing_image(cfg.brand, item.image_url, meta.get('image_url','')):
                        item.image_url = meta.get('image_url','')
                    if not item.name and meta.get('listing_name'):
                        item.name = meta.get('listing_name','')
                    if not item.price_text and meta.get('listing_price_text'):
                        item.price_text = meta.get('listing_price_text','')
                    raw_products.append(item)
            except Exception as e:
                print(f"    [WARN] selenium fallback failed: {product_url} | {e}")
            if idx % 20 == 0:
                print(f"    [OK] {cfg.brand} selenium fallback {idx}/{len(unresolved_urls)} | valid {len(raw_products)}")

        # dedupe by product_url
        dedup = {}
        for item in raw_products:
            existing = dedup.get(item.product_url)
            if existing is None:
                dedup[item.product_url] = item
                continue
            if (not existing.image_url) and item.image_url:
                existing.image_url = item.image_url
            if (not existing.breadcrumb_text) and item.breadcrumb_text:
                existing.breadcrumb_text = item.breadcrumb_text
            if (not existing.source_category) and item.source_category:
                existing.source_category = item.source_category
            if (not existing.source_category_url) and item.source_category_url:
                existing.source_category_url = item.source_category_url
            if (not existing.name) and item.name:
                existing.name = item.name
            if (not existing.description) and item.description:
                existing.description = item.description
        raw_products = list(dedup.values())

        print(f"[CRAWL DONE] {cfg.brand} -> {len(raw_products)} products")
        return raw_products


# ============================================================
# 6. ANALYTICS
# ============================================================
def products_to_dataframe(products: List[ProductAnalyzed]) -> pd.DataFrame:
    rows = []
    for p in products:
        row = asdict(p)
        row["raw_keywords"] = ", ".join(p.raw_keywords)
        row["standard_attributes"] = ", ".join(p.standard_attributes)
        rows.append(row)
    return pd.DataFrame(rows)


def build_attribute_coverage_flags(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    brand_other_ratio = (
        out.assign(is_other=out["dominant_attribute"].fillna("기타").eq("기타"))
        .groupby("brand")["is_other"]
        .mean()
        .fillna(0)
    )
    flag_map = {}
    for brand, ratio in brand_other_ratio.items():
        if ratio > 0.25:
            flag_map[brand] = "Critical"
        elif ratio > 0.15:
            flag_map[brand] = "Warning"
        else:
            flag_map[brand] = "OK"
    out["attribute_coverage_flag"] = out["brand"].map(flag_map).fillna("OK")
    return out


def build_brand_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[
            "brand", "total_products", "active_products", "avg_price", "min_price", "max_price",
            "sale_products", "sale_share_pct", "sold_out_products", "waterproof_products",
            "goretex_products", "down_products", "jackets", "pants", "shoes"
        ])
    rows = []
    for brand, g in df.groupby("brand"):
        total = len(g)
        active = len(g[~g["sold_out"].fillna(False)])
        avg_price = round(g["current_price"].dropna().mean(), 0) if g["current_price"].notna().any() else None
        min_price = g["current_price"].dropna().min() if g["current_price"].notna().any() else None
        max_price = g["current_price"].dropna().max() if g["current_price"].notna().any() else None
        sale_products = int(g["discount_rate"].fillna(0).gt(0).sum())
        sold_out_products = int(g["sold_out"].fillna(False).sum())
        attrs_str = g["standard_attributes"].fillna("")
        rows.append({
            "brand": brand,
            "total_products": total,
            "active_products": active,
            "avg_price": avg_price,
            "min_price": min_price,
            "max_price": max_price,
            "sale_products": sale_products,
            "sale_share_pct": round(sale_products / total * 100, 1) if total else 0,
            "sold_out_products": sold_out_products,
            "waterproof_products": int(attrs_str.str.contains("방수", regex=False).sum()),
            "goretex_products": int(attrs_str.str.contains("고어텍스", regex=False).sum()),
            "down_products": int(attrs_str.str.contains("다운", regex=False).sum()),
            "jackets": int((g["item_category"] == "자켓").sum()),
            "pants": int((g["item_category"] == "팬츠").sum()),
            "shoes": int((g["item_category"] == "슈즈").sum()),
        })
    return pd.DataFrame(rows).sort_values(["total_products", "avg_price"], ascending=[False, False]).reset_index(drop=True)


def _position_x_to_num(brand: str, df: pd.DataFrame) -> float:
    mapping = {"Lifestyle": 1, "Performance": 2, "Extreme": 3}
    subset = df[df["brand"] == brand]
    if subset.empty:
        return 1
    vals = [mapping.get(v, 1) for v in subset["positioning_x"].fillna("Lifestyle")]
    return round(sum(vals) / len(vals), 2)


def _position_y_to_num(avg_price: Optional[float]) -> float:
    if avg_price is None or (isinstance(avg_price, float) and math.isnan(avg_price)):
        return 1
    if avg_price < 150000:
        return 1
    if avg_price < 350000:
        return 2
    return 3







def resolve_item_category_value(name: str, description: str, item_category: str = "", source_category: str = "", source_category_url: str = "", source_url: str = "", product_url: str = "", breadcrumb_text: str = "") -> str:
    # 1) URL 강제 매핑이 최우선
    hard = get_hard_category_from_url(source_category_url or source_url or product_url)
    if hard:
        return hard

    original_item = safe_text(item_category) or "기타"
    inferred_item = infer_item_category(name or "", description or "", " ".join([breadcrumb_text or "", source_category or "", source_category_url or "", source_url or "", product_url or ""]))
    source_candidates = [
        normalize_source_category(breadcrumb_text or ""),
        normalize_source_category(source_category or ""),
        normalize_source_category(source_category_url or ""),
        normalize_source_category(source_url or ""),
        normalize_source_category(product_url or ""),
    ]
    source_item = next((x for x in source_candidates if x), "")

    for candidate in [source_item, original_item, inferred_item]:
        candidate = safe_text(candidate)
        if candidate and candidate != "기타":
            return candidate

    return "ACC"


def apply_item_reclassification(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()

    out = df.copy()
    original_items = []
    resolved = []
    item_rules = []
    crawler_items = []
    recat_source = []

    for _, r in out.iterrows():
        original_item = safe_text(r.get("item_category", "") or "") or "기타"
        item_rule = infer_item_category(
            str(r.get("name", "") or ""),
            str(r.get("description", "") or ""),
            " ".join([
                str(r.get("breadcrumb_text", "") or ""),
                str(r.get("source_category", "") or ""),
                str(r.get("source_category_url", "") or ""),
                str(r.get("source_url", "") or ""),
                str(r.get("product_url", "") or "")
            ]),
        )
        crawler_item = next((x for x in [
            normalize_source_category(str(r.get("breadcrumb_text", "") or "")),
            normalize_source_category(str(r.get("source_category", "") or "")),
            normalize_source_category(str(r.get("source_category_url", "") or "")),
            normalize_source_category(str(r.get("source_url", "") or "")),
            normalize_source_category(str(r.get("product_url", "") or "")),
        ] if x), "")
        final_item = resolve_item_category_value(
            str(r.get("name", "") or ""),
            str(r.get("description", "") or ""),
            original_item,
            str(r.get("source_category", "") or ""),
            str(r.get("source_category_url", "") or ""),
            str(r.get("source_url", "") or ""),
            str(r.get("product_url", "") or ""),
        )

        original_items.append(original_item)
        item_rules.append(item_rule)
        crawler_items.append(crawler_item or "")
        resolved.append(final_item)

        if final_item != original_item:
            if item_rule and item_rule != "기타":
                recat_source.append("ITEM RULE")
            elif crawler_item:
                recat_source.append("CRAWLED CATEGORY")
            else:
                recat_source.append("FALLBACK")
        else:
            recat_source.append("ORIGINAL")

    out["item_category_original"] = original_items
    out["item_rule"] = item_rules
    out["crawler_item"] = crawler_items
    out["item_category"] = resolved
    out["item_category_resolved"] = resolved
    out["item_recat_source"] = recat_source
    return out


def build_other_debug(df: pd.DataFrame) -> List[dict]:
    if df.empty:
        return []
    rows = []
    for _, r in df.iterrows():
        original_item = safe_text(r.get("item_category_original", "") or r.get("item_category", "") or "") or "기타"
        inferred_item = safe_text(r.get("item_rule", "") or "") or infer_item_category(
            str(r.get("name", "") or ""),
            str(r.get("description", "") or ""),
            " ".join([
                str(r.get("breadcrumb_text", "") or ""),
                str(r.get("source_category", "") or ""),
                str(r.get("source_category_url", "") or ""),
                str(r.get("source_url", "") or ""),
                str(r.get("product_url", "") or "")
            ]),
        )
        source_item = safe_text(r.get("crawler_item", "") or "") or next((x for x in [
            normalize_source_category(str(r.get("breadcrumb_text", "") or "")),
            normalize_source_category(str(r.get("source_category", "") or "")),
            normalize_source_category(str(r.get("source_category_url", "") or "")),
            normalize_source_category(str(r.get("source_url", "") or "")),
            normalize_source_category(str(r.get("product_url", "") or "")),
        ] if x), "")
        final_item = safe_text(r.get("item_category", "") or "") or resolve_item_category_value(
            str(r.get("name", "") or ""),
            str(r.get("description", "") or ""),
            original_item,
            str(r.get("source_category", "") or ""),
            str(r.get("source_category_url", "") or ""),
            str(r.get("source_url", "") or ""),
            str(r.get("product_url", "") or ""),
        )
        dominant = str(r.get("dominant_attribute", "") or "")

        reasons = []
        if inferred_item and inferred_item != "기타":
            reasons.append(f"item rule → {inferred_item}")
        if source_item:
            reasons.append(f"crawled category → {source_item}")
        if not reasons:
            reasons.append("name/description/category signal weak")

        if final_item != "기타":
            continue

        rows.append({
            "brand": str(r.get("brand", "") or ""),
            "gender": str(r.get("gender", "") or "공용"),
            "name": str(r.get("name", "") or ""),
            "original_item": original_item,
            "item_rule": inferred_item or "-",
            "crawler_item": source_item or "-",
            "item": final_item,
            "dominant_attribute": dominant or "-",
            "reason": " / ".join(reasons),
        })

    return rows[:400]


def build_other_debug(df: pd.DataFrame) -> List[dict]:
    if df.empty:
        return []
    rows = []
    for _, r in df.iterrows():
        original_item = safe_text(r.get("item_category_original", "") or r.get("item_category", "") or "") or "기타"
        inferred_item = safe_text(r.get("item_rule", "") or "") or infer_item_category(
            str(r.get("name", "") or ""),
            str(r.get("description", "") or ""),
            " ".join([
                str(r.get("breadcrumb_text", "") or ""),
                str(r.get("source_category", "") or ""),
                str(r.get("source_category_url", "") or ""),
                str(r.get("source_url", "") or ""),
                str(r.get("product_url", "") or ""),
            ]),
        )
        breadcrumb_item = normalize_source_category(str(r.get("breadcrumb_text", "") or ""))
        source_item = safe_text(r.get("crawler_item", "") or "") or next((x for x in [
            breadcrumb_item,
            normalize_source_category(str(r.get("source_category", "") or "")),
            normalize_source_category(str(r.get("source_category_url", "") or "")),
            normalize_source_category(str(r.get("source_url", "") or "")),
            normalize_source_category(str(r.get("product_url", "") or "")),
        ] if x), "")
        final_item = safe_text(r.get("item_category", "") or "") or resolve_item_category_value(
            str(r.get("name", "") or ""),
            str(r.get("description", "") or ""),
            original_item,
            str(r.get("source_category", "") or ""),
            str(r.get("source_category_url", "") or ""),
            str(r.get("source_url", "") or ""),
            str(r.get("product_url", "") or ""),
            str(r.get("breadcrumb_text", "") or ""),
        )
        dominant = str(r.get("dominant_attribute", "") or "")
        reasons = []
        if final_item == "기타":
            reasons.append("item unresolved")
        if dominant == "기타":
            reasons.append("attribute unresolved")
        if breadcrumb_item:
            reasons.append(f"breadcrumb → {breadcrumb_item}")
        if inferred_item and inferred_item != "기타":
            reasons.append(f"item rule → {inferred_item}")
        if source_item and source_item != breadcrumb_item:
            reasons.append(f"crawled category → {source_item}")
        if not reasons:
            continue
        gender = str(r.get("gender", "") or "공용")
        name = str(r.get("name", "") or "")
        if "공용" in name:
            gender = "공용"
        rows.append({
            "brand": r.get("brand", ""),
            "gender": gender or "공용",
            "name": name,
            "original_item": original_item,
            "breadcrumb_text": str(r.get("breadcrumb_text", "") or ""),
            "source_category": str(r.get("source_category", "") or ""),
            "source_category_url": str(r.get("source_category_url", "") or ""),
            "breadcrumb_text": str(r.get("breadcrumb_text", "") or ""),
            "item_rule": inferred_item or "-",
            "breadcrumb_item": breadcrumb_item or "-",
            "crawler_item": source_item or "기타",
            "item": final_item,
            "dominant_attribute": dominant or "-",
            "reason": " / ".join(unique_preserve_order(reasons)),
        })
    return rows



def build_price_band_gender_table(df: pd.DataFrame) -> List[dict]:
    if df.empty:
        return []
    bands = ["0-9.9만", "10-19.9만", "20-29.9만", "30-49.9만", "50만+"]
    rows = []
    grouped = (
        df.groupby(["brand", "gender", "price_band"])
        .size()
        .reset_index(name="style_count")
    )
    for (brand, gender), g in grouped.groupby(["brand", "gender"]):
        row = {"brand": brand, "gender": gender}
        total = 0
        for band in bands:
            cnt = int(g.loc[g["price_band"] == band, "style_count"].sum())
            row[band] = cnt
            total += cnt
        row["total_styles"] = total
        rows.append(row)
    return rows


def build_attribute_gender_table(df: pd.DataFrame) -> List[dict]:
    if df.empty:
        return []
    rows = []
    for (brand, gender), g in df.groupby(["brand", "gender"]):
        attr_series = g["standard_attributes"].fillna("")
        item_series = g["item_category"].fillna("")
        rows.append({
            "brand": brand,
            "gender": gender,
            "2L": int(attr_series.str.contains("2L", regex=False).sum()),
            "2.5L": int(attr_series.str.contains("2.5L", regex=False).sum()),
            "3L": int(attr_series.str.contains("3L", regex=False).sum()),
            "방수": int(attr_series.str.contains("방수", regex=False).sum()),
            "방풍": int(attr_series.str.contains("방풍", regex=False).sum()),
            "고어텍스": int(attr_series.str.contains("고어텍스", regex=False).sum()),
            "다운": int(attr_series.str.contains("다운", regex=False).sum()),
            "집티": int((item_series == "집티").sum()),
        })
    return rows




def build_item_style_table(df: pd.DataFrame) -> List[dict]:
    if df.empty:
        return []
    grouped = (
        df.groupby(["brand", "gender", "item_category"])
        .agg(
            style_count=("product_url", "nunique"),
            avg_price=("current_price", "mean"),
            min_price=("current_price", "min"),
            max_price=("current_price", "max"),
        )
        .reset_index()
    )

    band_counts = (
        df.groupby(["brand", "gender", "item_category", "price_band"])
        .size()
        .reset_index(name="cnt")
    )
    for band in ["0-9.9만", "10-19.9만", "20-29.9만", "30-49.9만", "50만+"]:
        pivot = band_counts[band_counts["price_band"] == band][["brand", "gender", "item_category", "cnt"]].rename(columns={"cnt": band})
        grouped = grouped.merge(pivot, on=["brand", "gender", "item_category"], how="left")
        grouped[band] = grouped[band].fillna(0).astype(int)

    grouped["avg_price"] = grouped["avg_price"].fillna(0).round(0).astype(int)
    grouped["min_price"] = grouped["min_price"].fillna(0).astype(int)
    grouped["max_price"] = grouped["max_price"].fillna(0).astype(int)
    grouped = grouped.sort_values(["brand", "gender", "style_count", "avg_price"], ascending=[True, True, False, False])
    return grouped.to_dict("records")



def build_dashboard_payload(df: pd.DataFrame, brand_summary: pd.DataFrame, kw_df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "generated_at": TODAY_STR,
            "kpis": {"brands": 0, "products": 0, "sale_products": 0, "avg_price": 0, "others_ratio": 0, "reclassified_count": 0},
            "brand_summary": [],
            "products": [],
            "charts": {},
            "keywords": [],
            "price_band_gender_table": [],
            "attribute_gender_table": [],
            "item_style_table": [],
            "other_debug": [],
        }

    df = apply_item_reclassification(df)
    brand_summary = build_brand_summary(df)

    charts = {
        "brandProductCounts": {
            "labels": brand_summary["brand"].tolist(),
            "values": brand_summary["total_products"].fillna(0).astype(int).tolist(),
        },
        "brandAvgPrice": {
            "labels": brand_summary["brand"].tolist(),
            "values": [int(x) if pd.notna(x) else 0 for x in brand_summary["avg_price"]],
        },
        "priceBand": {
            "labels": list(df.loc[df["price_band"].fillna("기타") != "기타", "price_band"].value_counts().index),
            "values": list(df.loc[df["price_band"].fillna("기타") != "기타", "price_band"].value_counts().values),
        },
        "itemCategory": {
            "labels": list(df.loc[df["item_category"].fillna("기타") != "기타", "item_category"].value_counts().head(12).index),
            "values": list(df.loc[df["item_category"].fillna("기타") != "기타", "item_category"].value_counts().head(12).values),
        },
        "dominantAttribute": {
            "labels": list(df.loc[df["dominant_attribute"].fillna("기타") != "기타", "dominant_attribute"].value_counts().head(10).index),
            "values": list(df.loc[df["dominant_attribute"].fillna("기타") != "기타", "dominant_attribute"].value_counts().head(10).values),
        },
        "grade": {
            "labels": list(df["grade"].fillna("Entry").value_counts().index),
            "values": list(df["grade"].fillna("Entry").value_counts().values),
        },
        "shellType": {
            "labels": list(df["shell_type"].fillna("Unknown").value_counts().index),
            "values": list(df["shell_type"].fillna("Unknown").value_counts().values),
        },
        "genderSplit": {
            "labels": [("남" if x=="남성" else "녀" if x=="여성" else "공용") for x in list(df["gender"].fillna("공용").value_counts().index)],
            "values": list(df["gender"].fillna("공용").value_counts().values),
        },
        "positioning": [
            {
                "brand": row["brand"],
                "x": _position_x_to_num(row["brand"], df),
                "y": _position_y_to_num(row["avg_price"]),
                "avg_price": int(row["avg_price"]) if pd.notna(row["avg_price"]) else 0,
                "size": int(row["total_products"]),
            }
            for _, row in brand_summary.iterrows()
        ],
    }

    keywords = []
    if not kw_df.empty:
        for brand, g in kw_df.groupby("brand"):
            keywords.append({"brand": brand, "items": g.head(12).to_dict("records")})

    others_ratio = round(float((df["item_category"].fillna("기타") == "기타").mean()) * 100, 1)

    return {
        "generated_at": TODAY_STR,
        "kpis": {
            "brands": int(df["brand"].nunique()),
            "products": int(len(df)),
            "sale_products": int(df["discount_rate"].fillna(0).gt(0).sum()),
            "avg_price": int(df["current_price"].dropna().mean()) if df["current_price"].notna().any() else 0,
            "others_ratio": others_ratio,
            "reclassified_count": int((df["item_recat_source"].fillna("ORIGINAL") != "ORIGINAL").sum()) if "item_recat_source" in df.columns else 0,
        },
        "brand_summary": brand_summary.to_dict("records"),
        "products": df.sort_values(["brand", "gender", "current_price"], ascending=[True, True, False]).fillna("").to_dict("records"),
        "charts": charts,
        "keywords": keywords,
        "price_band_gender_table": build_price_band_gender_table(df),
        "attribute_gender_table": build_attribute_gender_table(df),
        "item_style_table": build_item_style_table(df),
        "other_debug": build_other_debug(df),
    }


# ============================================================
# 7. DASHBOARD HTML
# ============================================================





def render_dashboard(payload: dict) -> str:
    data_json = json.dumps(json_safe(payload), ensure_ascii=False)
    generated_at = html.escape(payload.get("generated_at", ""))

    template = r"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Competitor Outdoor Dashboard</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    @import url('https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css');
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;700&display=swap');
    html, body { height:100%; overflow:auto; margin:0; }
    body {
      font-family:'Pretendard','Noto Sans KR',sans-serif;
      color:#0f172a;
      min-height:100vh;
      margin:0;
      background:
        radial-gradient(circle at 12% 18%, rgba(59,130,246,.20), transparent 28%),
        radial-gradient(circle at 84% 14%, rgba(34,197,94,.14), transparent 24%),
        radial-gradient(circle at 72% 74%, rgba(249,115,22,.12), transparent 24%),
        linear-gradient(180deg,#f8fbff 0%,#eef4fb 52%,#e8eff8 100%);
      background-size:120% 120%;
      animation:ambientShift 18s ease-in-out infinite alternate;
    }
    .glass {
      background: rgba(255,255,255,.72);
      border: 1px solid rgba(255,255,255,.72);
      box-shadow: 0 18px 42px rgba(15,23,42,.08);
      backdrop-filter: blur(18px);
    }
    .glass-strong {
      background: linear-gradient(135deg, rgba(255,255,255,.95), rgba(255,255,255,.78));
      border: 1px solid rgba(148,163,184,.18);
      box-shadow: 0 18px 40px rgba(15,23,42,.10);
      backdrop-filter: blur(18px);
    }
    .panel-hero { position:relative; background:linear-gradient(135deg, rgba(37,99,235,.16), rgba(255,255,255,.96) 38%, rgba(37,99,235,.08)); }
    .hero-orb { position:absolute; border-radius:999px; filter:blur(8px); pointer-events:none; mix-blend-mode:screen; }
    .hero-orb-primary { width:220px; height:220px; right:-42px; top:-42px; background:radial-gradient(circle, rgba(37,99,235,.28), transparent 70%); animation:floatBlob 12s ease-in-out infinite; }
    .hero-orb-secondary { width:180px; height:180px; left:-32px; bottom:-46px; background:radial-gradient(circle, rgba(37,99,235,.18), transparent 70%); animation:floatBlobReverse 14s ease-in-out infinite; }
    .filter-chip { border-radius:999px; padding:10px 16px; font-size:12px; font-weight:900; background:rgba(255,255,255,.92); color:#475569; border:1px solid rgba(148,163,184,.25); transition:all .18s ease; }
    .filter-chip.active,.filter-chip:hover{ color:#fff; background:linear-gradient(135deg, #2563eb, #0f172a); border-color:#17305f; box-shadow:0 16px 28px rgba(15,23,42,.18); transform:translateY(-1px); }
    .product-grid { display:grid; grid-template-columns:repeat(1,minmax(0,1fr)); gap:16px; }
    @media (min-width: 768px){ .product-grid { grid-template-columns:repeat(2,minmax(0,1fr)); } }
    @media (min-width: 1280px){ .product-grid { grid-template-columns:repeat(3,minmax(0,1fr)); } }
    @media (min-width: 1536px){ .product-grid { grid-template-columns:repeat(4,minmax(0,1fr)); } }
    .product-card { position:relative; overflow:hidden; border-radius:28px; border:1px solid rgba(255,255,255,.72); background:rgba(255,255,255,.72); box-shadow:0 18px 42px rgba(15,23,42,.08); backdrop-filter:blur(18px); }
    .product-card::before { content:""; position:absolute; inset:0; background:linear-gradient(135deg, rgba(37,99,235,.08), transparent 38%, rgba(15,23,42,.03)); pointer-events:none; }
    .product-thumb { aspect-ratio:1 / 1; border-radius:24px; overflow:hidden; background:#f1f5f9; border:1px solid rgba(148,163,184,.16); }
    .product-price-row { display:flex; align-items:flex-end; gap:8px; flex-wrap:wrap; }
    .price-current { font-family:'Space Grotesk','Pretendard',sans-serif; font-size:30px; line-height:1; font-weight:700; letter-spacing:-0.05em; color:#0f172a; }
    .price-original { font-size:14px; font-weight:800; color:#94a3b8; text-decoration:line-through; }
    .price-discount { display:inline-flex; align-items:center; border-radius:999px; padding:4px 8px; background:rgba(37,99,235,.10); color:#1d4ed8; font-size:11px; font-weight:900; }
    .hero-shell{position:relative;overflow:hidden;background:linear-gradient(135deg, rgba(37,99,235,.12), rgba(255,255,255,.96) 38%, rgba(37,99,235,.06));}
    .hero-shell:before{content:"";position:absolute;right:-42px;top:-42px;width:220px;height:220px;border-radius:999px;background:radial-gradient(circle, rgba(37,99,235,.24), transparent 70%);filter:blur(8px);animation:floatBlob 12s ease-in-out infinite;pointer-events:none}
    .hero-shell:after{content:"";position:absolute;left:-32px;bottom:-46px;width:180px;height:180px;border-radius:999px;background:radial-gradient(circle, rgba(14,165,233,.14), transparent 70%);filter:blur(8px);animation:floatBlobReverse 14s ease-in-out infinite;pointer-events:none}
    .metric-card { min-height: 150px; }
    .tab-btn{border-radius:999px;padding:10px 16px;font-size:12px;font-weight:900;background:rgba(255,255,255,.92);color:#475569;border:1px solid rgba(148,163,184,.25);transition:all .18s ease}
    .tab-btn.active,.tab-btn:hover{color:#fff;background:linear-gradient(135deg, #2563eb, #0f172a);border-color:#17305f;box-shadow:0 16px 28px rgba(15,23,42,.18);transform:translateY(-1px)}
    .hero-tab{display:inline-flex;align-items:center;gap:8px;border-radius:999px;padding:10px 16px;font-size:12px;font-weight:900;background:rgba(255,255,255,.88);color:#0f172a;border:1px solid rgba(148,163,184,.18);box-shadow:0 10px 18px rgba(15,23,42,.06)}
    .tag{display:inline-flex;align-items:center;border-radius:999px;padding:4px 10px;font-size:11px;font-weight:800}
    .chart-box{height:320px;position:relative}
    .chart-box canvas{width:100%!important;height:100%!important}
    .table-wrap::-webkit-scrollbar{height:8px;width:8px}.table-wrap::-webkit-scrollbar-thumb{background:#cbd5e1;border-radius:999px}
    .table-head th{position:sticky;top:0;background:rgba(248,250,252,.94);backdrop-filter:blur(8px);z-index:1;}
    .section-lead{letter-spacing:.18em;text-transform:uppercase}
    @keyframes ambientShift { 0% { background-position:0% 0%; } 100% { background-position:100% 100%; } }
    @keyframes floatBlob { 0%,100% { transform:translate3d(0,0,0) scale(1); } 50% { transform:translate3d(-18px,18px,0) scale(1.05); } }
    @keyframes floatBlobReverse { 0%,100% { transform:translate3d(0,0,0) scale(1); } 50% { transform:translate3d(18px,-18px,0) scale(1.08); } }

    .premium-hero{position:relative;overflow:hidden;border-radius:32px;background:#020617;color:#fff;box-shadow:0 22px 48px rgba(2,6,23,.28)}
    .premium-hero::before{content:"";position:absolute;inset:-20% -10% auto auto;width:420px;height:420px;border-radius:999px;background:radial-gradient(circle, rgba(37,99,235,.26), transparent 68%);filter:blur(10px);animation:floatBlob 12s ease-in-out infinite}
    .premium-hero::after{content:"";position:absolute;left:-120px;bottom:-120px;width:340px;height:340px;border-radius:999px;background:radial-gradient(circle, rgba(14,165,233,.18), transparent 68%);filter:blur(10px);animation:floatBlobReverse 14s ease-in-out infinite}
    .premium-kpi{border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.08);backdrop-filter:blur(10px)}
    .surface-card{animation:panelIn .72s cubic-bezier(.2,.7,.2,1) both}
    .sticky-switch{position:sticky;top:12px;z-index:40}
    [data-animate]{opacity:0;transform:translateY(18px) scale(.985)}
    .motion-ready [data-animate]{animation:revealUp .68s cubic-bezier(.2,.7,.2,1) forwards;animation-delay:calc(var(--index,0) * 55ms)}
    .product-card{transition:transform .18s ease, box-shadow .18s ease, border-color .18s ease}
    .product-card:hover{transform:translateY(-4px);box-shadow:0 26px 48px rgba(15,23,42,.12);border-color:rgba(37,99,235,.18)}
    .metric-card{transition:transform .18s ease, box-shadow .18s ease}
    .metric-card:hover{transform:translateY(-3px);box-shadow:0 22px 44px rgba(15,23,42,.12)}
    .trend-fill{transform-origin:left center;transform:scaleX(0)}
    .motion-ready .trend-fill{animation:growBar .9s cubic-bezier(.2,.7,.2,1) forwards;animation-delay:calc(var(--index,0) * 55ms)}
    @keyframes panelIn { 0% { opacity:0; transform:translateY(20px) scale(.99);} 100% { opacity:1; transform:translateY(0) scale(1);} }
    @keyframes revealUp { 0% { opacity:0; transform:translateY(18px) scale(.985);} 100% { opacity:1; transform:translateY(0) scale(1);} }
    @keyframes growBar { from { transform:scaleX(0);} to { transform:scaleX(var(--scale,1));} }
    @media (prefers-reduced-motion: reduce) { *,*::before,*::after{animation:none !important;transition:none !important;} [data-animate]{opacity:1;transform:none;} .trend-fill{transform:scaleX(var(--scale,1));} }

  </style>
</head>
<body class="min-h-screen text-slate-900">
  <div class="mx-auto my-3 w-[min(1600px,calc(100vw-24px))] rounded-[36px] border border-white/70 bg-white/65 p-4 shadow-[0_24px_70px_rgba(15,23,42,0.10)] backdrop-blur-2xl md:p-6">
    <section class="premium-hero px-6 py-6 md:px-8 surface-card">
      <div class="relative z-10">
        <div class="inline-flex rounded-full bg-white/10 px-4 py-2 text-[11px] font-black tracking-[0.18em]">EXTERNAL SIGNAL PREMIUM HUB</div>
        <h1 class="mt-5 text-4xl font-black tracking-[-0.05em] md:text-6xl">Competitor Product Dashboard</h1>
        <p class="mt-4 max-w-4xl text-sm font-semibold leading-7 text-white/80 md:text-base">
          Community Signal Dashboard의 다크 히어로, 깊은 그라데이션, 모션 리듬을 그대로 가져오고,
          상품 카드/차트/테이블 전체를 같은 UX 톤으로 재구성했습니다. 상품명 품목어·크롤러 카테고리·브레드크럼을 함께 반영해
          최종 item을 결정합니다.
        </p>
        <div class="mt-6 grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-4">
          <div class="premium-kpi rounded-3xl p-4"><div class="text-xs font-black text-white/60">브랜드 수</div><div class="mt-2 font-['Space_Grotesk'] text-3xl font-bold tracking-[-0.05em]" id="hero-brand-count">-</div><div class="mt-1 text-xs font-bold text-white/60">크롤링 완료 브랜드</div></div>
          <div class="premium-kpi rounded-3xl p-4"><div class="text-xs font-black text-white/60">총 상품 수</div><div class="mt-2 font-['Space_Grotesk'] text-3xl font-bold tracking-[-0.05em]" id="hero-product-count">-</div><div class="mt-1 text-xs font-bold text-white/60">품절 제외 활성 SKU</div></div>
          <div class="premium-kpi rounded-3xl p-4"><div class="text-xs font-black text-white/60">세일 상품 수</div><div class="mt-2 font-['Space_Grotesk'] text-3xl font-bold tracking-[-0.05em]" id="hero-sale-count">-</div><div class="mt-1 text-xs font-bold text-white/60">현재 할인 상품</div></div>
          <div class="premium-kpi rounded-3xl p-4"><div class="text-xs font-black text-white/60">Updated</div><div class="mt-2 text-xl font-black">__GENERATED_AT__</div><div class="mt-1 text-xs font-bold text-white/60">최근 생성 기준</div></div>
        </div>
        <div class="mt-6 grid grid-cols-2 gap-3 xl:grid-cols-6" id="kpi-grid"></div>
      </div>
    </section>

    <div class="px-5 py-6 md:px-7 space-y-6 motion-ready">
      <section class="grid grid-cols-1 gap-4 xl:grid-cols-3">
        <div class="glass rounded-3xl p-5"><div class="text-[11px] font-extrabold section-lead text-slate-500">SKU</div><div class="mt-2 text-2xl font-black">브랜드별 SKU 수</div><div class="chart-box mt-4"><canvas id="brandCountChart"></canvas></div></div>
        <div class="glass rounded-3xl p-5"><div class="text-[11px] font-extrabold section-lead text-slate-500">PRICE</div><div class="mt-2 text-2xl font-black">브랜드별 평균가</div><div class="chart-box mt-4"><canvas id="avgPriceChart"></canvas></div></div>
        <div class="glass rounded-3xl p-5"><div class="text-[11px] font-extrabold section-lead text-slate-500">GENDER</div><div class="mt-2 text-2xl font-black">남녀 비중</div><div class="chart-box mt-4"><canvas id="genderSplitChart"></canvas></div></div>
      </section>

      <section class="grid grid-cols-1 gap-4 xl:grid-cols-3">
        <div class="glass rounded-3xl p-5">
          <div class="text-[11px] font-extrabold section-lead text-slate-500">PRICE BAND</div>
          <div class="mt-2 text-2xl font-black">가격대 분포</div>
          <div class="mt-3 flex flex-wrap gap-2" id="chartBrandTabsA"></div>
          <div class="chart-box mt-4"><canvas id="priceBandChart"></canvas></div>
        </div>
        <div class="glass rounded-3xl p-5">
          <div class="text-[11px] font-extrabold section-lead text-slate-500">ITEM</div>
          <div class="mt-2 text-2xl font-black">아이템 분포</div>
          <div class="mt-3 flex flex-wrap gap-2" id="chartBrandTabsB"></div>
          <div class="chart-box mt-4"><canvas id="categoryChart"></canvas></div>
        </div>
        <div class="glass rounded-3xl p-5">
          <div class="text-[11px] font-extrabold section-lead text-slate-500">ATTRIBUTE</div>
          <div class="mt-2 text-2xl font-black">대표 속성 분포</div>
          <div class="mt-3 flex flex-wrap gap-2" id="chartBrandTabsC"></div>
          <div class="chart-box mt-4"><canvas id="dominantAttrChart"></canvas></div>
        </div>
      </section>

      <section class="grid grid-cols-1 gap-4 xl:grid-cols-[1.35fr_.65fr]">
        <div class="glass rounded-3xl p-5 overflow-hidden">
          <div class="text-[11px] font-extrabold section-lead text-slate-500">COMPARE</div>
          <div class="mt-2 text-2xl font-black">브랜드 비교표</div>
          <div class="table-wrap mt-4 overflow-x-auto w-full">
            <table class="min-w-[920px] w-full text-sm">
              <thead class="table-head"><tr class="border-b border-slate-200 text-slate-500">
                <th class="py-3 text-left">Brand</th><th class="py-3 text-right">Total</th><th class="py-3 text-right">Active</th><th class="py-3 text-right">Avg Price</th><th class="py-3 text-right">Sale %</th><th class="py-3 text-right">방수</th><th class="py-3 text-right">고어텍스</th><th class="py-3 text-right">다운</th><th class="py-3 text-right">자켓</th><th class="py-3 text-right">팬츠</th><th class="py-3 text-right">슈즈</th>
              </tr></thead>
              <tbody id="brandSummaryBody"></tbody>
            </table>
          </div>
        </div>
        <div class="glass rounded-3xl p-5">
          <div class="text-[11px] font-extrabold section-lead text-slate-500">POSITIONING</div>
          <div class="mt-2 text-2xl font-black">브랜드 포지셔닝</div>
          <div class="chart-box mt-4"><canvas id="positionChart"></canvas></div>
          <div class="mt-3 text-xs font-bold text-slate-500">→ X축: 라이프스타일 → 퍼포먼스/익스트림 / ↑ Y축: 매스 → 프리미엄</div>
        </div>
      </section>

      <section class="glass rounded-3xl p-5">
        <div class="text-[11px] font-extrabold section-lead text-slate-500">ITEM STYLE MIX</div>
        <div class="mt-2 text-2xl font-black">브랜드 × 성별 × 아이템별 스타일 수 / 가격대</div>
        <div class="mt-2 text-sm font-bold text-slate-500">상품명 직접 매칭 → breadcrumb/source category → 기존 item 순서로 final item을 강제 확정한 뒤 집계합니다. 기타는 최종적으로 ACC로 흡수됩니다.</div>
        <div class="mt-4 flex flex-wrap gap-2" id="brandItemTabs"></div>
        <div class="table-wrap mt-4 overflow-x-auto">
          <table class="min-w-[1400px] w-full text-sm">
            <thead class="table-head"><tr class="border-b border-slate-200 text-slate-500">
              <th class="py-3 text-left">Brand</th><th class="py-3 text-left">Gender</th><th class="py-3 text-left">Item</th><th class="py-3 text-right">Styles</th><th class="py-3 text-right">0-9.9만</th><th class="py-3 text-right">10-19.9만</th><th class="py-3 text-right">20-29.9만</th><th class="py-3 text-right">30-49.9만</th><th class="py-3 text-right">50만+</th><th class="py-3 text-right">Avg Price</th><th class="py-3 text-right">Min Price</th><th class="py-3 text-right">Max Price</th>
            </tr></thead>
            <tbody id="itemStyleBody"></tbody>
          </table>
        </div>
      </section>

      <section class="grid grid-cols-1 gap-4 xl:grid-cols-2">
        <div class="glass rounded-3xl p-5">
          <div class="text-[11px] font-extrabold section-lead text-slate-500">PRICE BY GENDER</div>
          <div class="mt-2 text-2xl font-black">브랜드 × 성별 가격대 스타일 수</div>
          <div class="mt-4 flex flex-wrap gap-2" id="brandPriceTabs"></div>
          <div class="table-wrap mt-4 overflow-x-auto">
            <table class="min-w-[860px] w-full text-sm">
              <thead class="table-head"><tr class="border-b border-slate-200 text-slate-500">
                <th class="py-3 text-left">Brand</th><th class="py-3 text-left">Gender</th><th class="py-3 text-right">0-9.9만</th><th class="py-3 text-right">10-19.9만</th><th class="py-3 text-right">20-29.9만</th><th class="py-3 text-right">30-49.9만</th><th class="py-3 text-right">50만+</th><th class="py-3 text-right">Total</th>
              </tr></thead>
              <tbody id="priceBandGenderBody"></tbody>
            </table>
          </div>
        </div>
        <div class="glass rounded-3xl p-5">
          <div class="text-[11px] font-extrabold section-lead text-slate-500">ATTRIBUTE BY GENDER</div>
          <div class="mt-2 text-2xl font-black">브랜드 × 스타일 속성 수</div>
          <div class="mt-4 flex flex-wrap gap-2" id="brandAttrTabs"></div>
          <div class="table-wrap mt-4 overflow-x-auto">
            <table class="min-w-[860px] w-full text-sm">
              <thead class="table-head"><tr class="border-b border-slate-200 text-slate-500">
                <th class="py-3 text-left">Brand</th><th class="py-3 text-left">Gender</th><th class="py-3 text-right">2L</th><th class="py-3 text-right">2.5L</th><th class="py-3 text-right">3L</th><th class="py-3 text-right">방수</th><th class="py-3 text-right">방풍</th><th class="py-3 text-right">고어텍스</th><th class="py-3 text-right">다운</th><th class="py-3 text-right">티셔츠</th>
              </tr></thead>
              <tbody id="attributeGenderBody"></tbody>
            </table>
          </div>
        </div>
      </section>

      <section class="glass rounded-3xl p-5">
        <div class="text-[11px] font-extrabold section-lead text-slate-500">KEYWORD</div>
        <div class="mt-2 text-2xl font-black">브랜드별 키워드</div>
        <div id="keywordGrid" class="mt-4 grid grid-cols-1 gap-4 xl:grid-cols-2"></div>
      </section>

      <section class="glass rounded-3xl p-5">
        <div class="flex items-end justify-between gap-3 flex-wrap">
          <div><div class="text-[11px] font-extrabold section-lead text-slate-500">PRODUCT FEED</div><div class="mt-2 text-2xl font-black">상품 카드 피드</div></div>
          <div id="productCountText" class="text-sm font-bold text-slate-500"></div>
        </div>
        <div class="mt-4 flex flex-wrap gap-2" id="productBrandTabs"></div>
        <div class="mt-2 flex flex-wrap gap-2" id="productCategoryTabs"></div>
        <div id="productGroupContainer" class="mt-4"></div>
      </section>

      <section class="glass rounded-3xl p-5">
        <div class="text-[11px] font-extrabold section-lead text-slate-500">OTHER DEBUG</div>
        <div class="mt-2 text-2xl font-black">기타 디버그</div>
        <div class="mt-2 text-sm font-bold text-slate-500">재분류 이후에도 검토가 필요한 항목만 보여줍니다. 상품명 우선 규칙, crawler category, original item을 같이 확인할 수 있습니다.</div>
        <div class="table-wrap mt-4 overflow-x-auto">
          <table class="min-w-[1500px] w-full text-sm">
            <thead class="table-head"><tr class="border-b border-slate-200 text-slate-500">
              <th class="py-3 text-left">Brand</th><th class="py-3 text-left">Gender</th><th class="py-3 text-left">Name</th><th class="py-3 text-left">Original Item</th><th class="py-3 text-left">Item Rule</th><th class="py-3 text-left">Crawler Item</th><th class="py-3 text-left">Final Item</th><th class="py-3 text-left">Dominant Attr</th><th class="py-3 text-left">Reason</th>
            </tr></thead>
            <tbody id="otherDebugBody"></tbody>
          </table>
        </div>
      </section>
    </div>
  </div>

<script>
const DATA = __DATA_JSON__;
const NOISE_KEYWORDS = new Set(["BETTER","MKAE","NOW","PERPECT","PERFECT","COLUMBIA","COMING","SOLD","OUT","SOON","EVA","BLACK","GARY","GRAY","WHITE","BLUE","BEIGE","MAKE"]);
const STATE = { itemBrand:"전체", priceBrand:"전체", attrBrand:"전체", productBrand:"전체", productCategory:"전체", chartBrand:"전체" };
let chartRefs = {};

function formatNumber(v){ if(v===null||v===undefined||v==="") return "-"; return new Intl.NumberFormat('ko-KR').format(v); }
function formatPrice(v){ if(v===null||v===undefined||v==="") return "-"; return formatNumber(v)+"원"; }
function compactNameJS(name, brand){ let t=(name||"").trim(); if((brand||"")==="DISCOVERY"){ t=t.replace(/^디스커버리\s*익스페디션\s*[|｜:/-]\s*/i,""); t=t.replace(/^디스커버리\s*익스페디션\s+/i,""); t=t.replace(/^DISCOVERY\s*EXPEDITION\s*[|｜:/-]\s*/i,""); t=t.replace(/^DISCOVERY\s*EXPEDITION\s+/i,""); } return t; }
function inferCategoryJS(name, description, sourceCategory="", sourceCategoryUrl="", itemCategory="", breadcrumbText=""){
  const fixed = String(itemCategory||"").trim();
  if(fixed && fixed!=="기타") return fixed;
  const exact = String(sourceCategory||"").trim();
  if(["자켓","팬츠","티셔츠","슈즈","백","베스트","후디","플리스","ACC","장갑"].includes(exact)) return exact;
  const src=((breadcrumbText||"")+" "+(sourceCategory||"")+" "+(sourceCategoryUrl||"")).toLowerCase();
  if(/신발|슈즈|스니커즈|트레일러닝|등산화|샌들|슬리퍼|shoe|shoes|boot|boots|sandal|konos|peakfreak|crestwood|omni-max|omni max|clog/.test(src)) return "슈즈";
  if(/가방|백팩|크로스|토트백|힙색|슬링백|bag|backpack|pack|sling/.test(src)) return "백";
  if(/장갑|glove/.test(src)) return "장갑";
  if(/모자|용품|accessory|acc|hat|cap/.test(src)) return "ACC";
  if(/플리스|fleece/.test(src)) return "플리스";
  if(/후드티|후디|후드|hoodie/.test(src)) return "후디";
  if(/티셔츠|라운드티|폴로티|반팔|긴팔|tee|t-shirt|jersey|crewneck|상의|쿨링/.test(src)) return "티셔츠";
  if(/팬츠|하의|긴바지|카고\/조거|반바지|pants|pant|cargo|jogger|shorts|short|skort/.test(src)) return "팬츠";
  if(/자켓|재킷|아우터|방수자켓|바람막이|경량패딩|인터체인지|jacket|windbreaker|shell|outer|parka|anorak/.test(src)) return "자켓";
  if(/베스트|vest/.test(src)) return "베스트";

  const blob=((name||"")+" "+(description||"")).toLowerCase();
  if(/(konos|peakfreak|crestwood|clog|shoe|shoes|boot|boots|sandal|sandals|omni-max|omni max)/.test(blob)) return "슈즈";
  if(/short\s+sleeve|long\s+sleeve|티셔츠|반팔|긴팔|tee|t-shirt|jersey|crewneck|라운드티|폴로티/.test(blob)) return "티셔츠";
  if(/hoodie|hood|후디|후드|후드티|맨투맨|sweatshirt/.test(blob)) return "후디";
  if(/fleece|플리스|보아|boa/.test(blob)) return "플리스";
  if(/vest|베스트/.test(blob)) return "베스트";
  if(/shirt|셔츠/.test(blob)) return "셔츠";
  if(/cargo\s+pant|cargo\s+pants|jogger|pant|pants|shorts|short|팬츠|바지|카고|조거|슬랙스|치노|skort/.test(blob)) return "팬츠";
  if(/jacket|windbreaker|anorak|parka|shell|자켓|재킷|바람막이|아노락|퍼텍스/.test(blob)) return "자켓";
  if(/백팩|backpack|bag|bags|바디백|body bag|bodybag|슬링백|슬링|sling|숄더케이스|shoulder case|케이스/.test(blob)) return "백";
  if(/부니|모자|cap|hat|accessory/.test(blob)) return "ACC";
  return "ACC";
}
function inferGenderJS(name, description="", sourceCategory="", sourceCategoryUrl="", breadcrumbText="", currentGender=""){
  const explicit = (currentGender||"").toLowerCase();
  if(/키즈|kids|kid|junior|juniors|youth|boy|boys|girl|girls|toddler|infant|children/.test(explicit)) return "키즈";
  if(/여성|women|womens|woman/.test(explicit)) return "여성";
  if(/남성|men|mens|man/.test(explicit)) return "남성";
  if(/공용|unisex/.test(explicit)) return "공용";
  const src=((breadcrumbText||"")+" "+(sourceCategory||"")+" "+(sourceCategoryUrl||"")+" "+(name||"")+" "+(description||"")).toLowerCase();
  if(/키즈|kids|kid|junior|juniors|youth|boy|boys|girl|girls|toddler|infant|children/.test(src)) return "키즈";
  if(/여성|women|womens|woman/.test(src)) return "여성";
  if(/남성|men|mens|man/.test(src)) return "남성";
  if(/공용|unisex/.test(src)) return "공용";
  return currentGender || "공용";
}
function normalizeProducts(){
  const seen = new Set();
  return (DATA.products||[]).map(p=>{
    const resolved = inferCategoryJS(p.name||"", p.description||"", p.source_category||"", p.source_category_url||"", p.item_category||"", p.breadcrumb_text||"");
    const resolvedGender = inferGenderJS(p.name||"", p.description||"", p.source_category||"", p.source_category_url||"", p.breadcrumb_text||"", p.gender||"");
    const imageUrl = p.image_url || p.imageUrl || "";
    return {...p, image_url: imageUrl, item_category: resolved, _cat: resolved, gender: resolvedGender};
  }).filter(p=>{
    const key = [p.brand||"", compactNameJS(p.name,p.brand), p.product_url||"", p.image_url||""].join("||");
    if(seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}
function createKpis(){
  const k = DATA.kpis || {};
  const items = [
    ["브랜드 수",k.brands,"크롤링 완료 브랜드"],
    ["총 상품 수",k.products,"현재 분석 SKU"],
    ["세일 상품 수",k.sale_products,"할인 상품"],
    ["평균가",formatPrice(k.avg_price),"현재가 평균"],
    ["기타 비중",(k.others_ratio||0)+"%","낮을수록 좋음"],
    ["재분류 반영",formatNumber(k.reclassified_count||0)+"건","ITEM RULE + CRAWLER CATEGORY 적용"]
  ];
  document.getElementById("hero-brand-count").textContent = formatNumber(k.brands||0);
  document.getElementById("hero-product-count").textContent = formatNumber(k.products||0);
  document.getElementById("hero-sale-count").textContent = formatNumber(k.sale_products||0);
  document.getElementById("kpi-grid").innerHTML = items.map(([label,value,desc],i) => `<div class="glass-strong metric-card min-w-0 rounded-3xl p-5 surface-card" data-animate style="--index:${i}"><div class="text-[11px] font-extrabold tracking-[0.18em] text-slate-500">${label}</div><div class="mt-3 truncate text-3xl font-black tracking-[-0.05em]">${value}</div><div class="mt-2 text-xs font-bold leading-5 text-slate-500">${desc}</div></div>`).join("");
}
function groupRowspan(rows, brandKey="brand"){ const out=[]; let i=0; while(i<rows.length){ const b=rows[i][brandKey]; let j=i; while(j<rows.length && rows[j][brandKey]===b) j++; const span=j-i; for(let k=i;k<j;k++){ const r={...rows[k]}; r._showBrand=(k===i); r._brandRowspan=span; out.push(r);} i=j; } return out; }
function makeTabs(elId, values, stateKey, onChange){ const el=document.getElementById(elId); if(!el) return; el.innerHTML=["전체",...values].map(v=>`<button class="tab-btn ${STATE[stateKey]===v?'active':''}" data-val="${v}">${v}</button>`).join(""); [...el.querySelectorAll(".tab-btn")].forEach(btn=>btn.onclick=()=>{STATE[stateKey]=btn.dataset.val; makeTabs(elId, values, stateKey, onChange); onChange();}); }
function renderBrandSummary(){ const rows=DATA.brand_summary||[]; document.getElementById('brandSummaryBody').innerHTML = rows.map(r=>`<tr class="border-b border-slate-200 last:border-b-0 hover:bg-slate-50/70"><td class="py-3 font-black">${r.brand}</td><td class="py-3 text-right">${formatNumber(r.total_products)}</td><td class="py-3 text-right">${formatNumber(r.active_products)}</td><td class="py-3 text-right">${formatPrice(r.avg_price)}</td><td class="py-3 text-right">${r.sale_share_pct}%</td><td class="py-3 text-right">${formatNumber(r.waterproof_products)}</td><td class="py-3 text-right">${formatNumber(r.goretex_products)}</td><td class="py-3 text-right">${formatNumber(r.down_products)}</td><td class="py-3 text-right">${formatNumber(r.jackets)}</td><td class="py-3 text-right">${formatNumber(r.pants)}</td><td class="py-3 text-right">${formatNumber(r.shoes)}</td></tr>`).join(''); }
function renderPriceBandGenderTable(){ const allRows=DATA.price_band_gender_table||[]; const brands=[...new Set(allRows.map(r=>r.brand))]; makeTabs("brandPriceTabs", brands, "priceBrand", renderPriceBandGenderTable); const rows=STATE.priceBrand==="전체"?allRows:allRows.filter(r=>r.brand===STATE.priceBrand); const merged=groupRowspan(rows); document.getElementById('priceBandGenderBody').innerHTML=merged.map(r=>`<tr class="border-b border-slate-200 last:border-b-0 hover:bg-slate-50/70">${r._showBrand?`<td class="py-3 font-black align-top" rowspan="${r._brandRowspan}">${r.brand}</td>`:``}<td class="py-3 font-bold">${r.gender}</td><td class="py-3 text-right">${formatNumber(r["0-9.9만"])}</td><td class="py-3 text-right">${formatNumber(r["10-19.9만"])}</td><td class="py-3 text-right">${formatNumber(r["20-29.9만"])}</td><td class="py-3 text-right">${formatNumber(r["30-49.9만"])}</td><td class="py-3 text-right">${formatNumber(r["50만+"])}</td><td class="py-3 text-right font-black">${formatNumber(r.total_styles)}</td></tr>`).join(''); }
function renderAttributeGenderTable(){ const allRows=DATA.attribute_gender_table||[]; const brands=[...new Set(allRows.map(r=>r.brand))]; makeTabs("brandAttrTabs", brands, "attrBrand", renderAttributeGenderTable); const rows=STATE.attrBrand==="전체"?allRows:allRows.filter(r=>r.brand===STATE.attrBrand); const merged=groupRowspan(rows); document.getElementById('attributeGenderBody').innerHTML=merged.map(r=>`<tr class="border-b border-slate-200 last:border-b-0 hover:bg-slate-50/70">${r._showBrand?`<td class="py-3 font-black align-top" rowspan="${r._brandRowspan}">${r.brand}</td>`:``}<td class="py-3 font-bold">${r.gender}</td><td class="py-3 text-right">${formatNumber(r["2L"])}</td><td class="py-3 text-right">${formatNumber(r["2.5L"])}</td><td class="py-3 text-right">${formatNumber(r["3L"])}</td><td class="py-3 text-right">${formatNumber(r["방수"])}</td><td class="py-3 text-right">${formatNumber(r["방풍"])}</td><td class="py-3 text-right">${formatNumber(r["고어텍스"])}</td><td class="py-3 text-right">${formatNumber(r["다운"])}</td><td class="py-3 text-right">${formatNumber(r["티셔츠"]||0)}</td></tr>`).join(''); }
function renderItemStyleTable(){ const allRows=DATA.item_style_table||[]; const brands=[...new Set(allRows.map(r=>r.brand))]; makeTabs("brandItemTabs", brands, "itemBrand", renderItemStyleTable); const rows=STATE.itemBrand==="전체"?allRows:allRows.filter(r=>r.brand===STATE.itemBrand); const merged=groupRowspan(rows); document.getElementById('itemStyleBody').innerHTML=merged.map(r=>`<tr class="border-b border-slate-200 last:border-b-0 hover:bg-slate-50/70">${r._showBrand?`<td class="py-3 font-black align-top" rowspan="${r._brandRowspan}">${r.brand}</td>`:``}<td class="py-3 font-bold">${r.gender}</td><td class="py-3">${r.item_category}</td><td class="py-3 text-right font-black">${formatNumber(r.style_count)}</td><td class="py-3 text-right">${formatNumber(r["0-9.9만"])}</td><td class="py-3 text-right">${formatNumber(r["10-19.9만"])}</td><td class="py-3 text-right">${formatNumber(r["20-29.9만"])}</td><td class="py-3 text-right">${formatNumber(r["30-49.9만"])}</td><td class="py-3 text-right">${formatNumber(r["50만+"])}</td><td class="py-3 text-right">${formatPrice(r.avg_price)}</td><td class="py-3 text-right">${formatPrice(r.min_price)}</td><td class="py-3 text-right">${formatPrice(r.max_price)}</td></tr>`).join(''); }
function renderKeywords(){ const grid=document.getElementById('keywordGrid'); const blocks=(DATA.keywords||[]).map(b=>({brand:b.brand, items:(b.items||[]).filter(it=>!NOISE_KEYWORDS.has(String(it.keyword||"").toUpperCase()))})); grid.innerHTML=blocks.map(block=>`<div class="glass-strong rounded-3xl p-4"><div class="text-lg font-black">${block.brand}</div><div class="mt-3 flex flex-wrap gap-2">${(block.items||[]).map(it=>'<span class="tag bg-slate-900 text-white">'+it.keyword+' · '+it.count+'</span>').join('')}</div></div>`).join(''); }
function renderProducts(){
  const all = normalizeProducts().filter(p=>!p.sold_out);
  const brands = [...new Set(all.map(p=>p.brand).filter(Boolean))];
  const brandFiltered = STATE.productBrand==="전체" ? all : all.filter(p=>p.brand===STATE.productBrand);
  const cats = [...new Set(brandFiltered.map(p=>p._cat).filter(Boolean).filter(x=>x!=="기타"))];
  makeTabs("productBrandTabs", brands, "productBrand", renderProducts);
  makeTabs("productCategoryTabs", cats, "productCategory", renderProducts);

  let items = brandFiltered;
  if(STATE.productCategory!=="전체") items = items.filter(p=>p._cat===STATE.productCategory);

  items = items.slice().sort((a,b)=>{
    const ap = Number(a.current_price||0), bp = Number(b.current_price||0);
    if (bp !== ap) return bp - ap;
    return String(compactNameJS(a.name,a.brand)).localeCompare(String(compactNameJS(b.name,b.brand)), 'ko');
  });

  document.getElementById('productCountText').textContent = `품절 제외 ${formatNumber(items.length)}개`;
  const container = document.getElementById("productGroupContainer");

  if(!items.length){
    container.innerHTML = `<div class="glass rounded-[30px] p-8 text-center"><div class="text-sm font-black text-slate-400">조건에 맞는 상품이 없습니다.</div></div>`;
    return;
  }

  container.innerHTML = `
    <section class="panel-hero surface-card overflow-hidden rounded-[34px] border border-white/70 bg-white/60 p-5 shadow-sm backdrop-blur-xl" data-animate style="--index:0">
      <div class="hero-orb hero-orb-primary"></div>
      <div class="hero-orb hero-orb-secondary"></div>
      <div class="relative z-10">
        <div class="flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
          <div>
            <div class="text-[11px] font-extrabold tracking-[0.18em] text-slate-500">LIVE PRODUCT GRID</div>
            <div class="mt-2 text-2xl font-black tracking-[-0.04em] text-slate-900">${STATE.productBrand==="전체"?"ALL BRANDS":STATE.productBrand}${STATE.productCategory==="전체"?"":" · "+STATE.productCategory}</div>
            <div class="mt-2 text-sm font-bold text-slate-500">External Signal UI 톤 그대로, 카테고리 펼침형 대신 상품 카드 grid로 바로 노출합니다.</div>
          </div>
          <div class="flex flex-wrap gap-2">
            <span class="hero-tab">Filtered ${formatNumber(items.length)} styles</span>
            <span class="hero-tab">Price capture active</span>
          </div>
        </div>
        <div class="product-grid mt-6">
          ${items.slice(0,240).map((p,i)=>{
            const attrs = String(p.standard_attributes||'').split(',').map(x=>x.trim()).filter(Boolean).slice(0,4);
            const orig = Number(p.original_price||0);
            const curr = Number(p.current_price||0);
            const discount = (orig > curr && curr > 0) ? Math.round((1 - (curr / orig)) * 100) : null;
            return `<article class="product-card p-4 surface-card" data-animate style="--index:${i+1}">
              <div class="relative z-10">
                <div class="product-thumb">
                  ${p.image_url ? `<img src="${p.image_url}" alt="${p.name||''}" class="h-full w-full object-contain bg-white" loading="lazy" />` : `<div class="flex h-full items-center justify-center text-xs font-black text-slate-400">NO IMAGE</div>`}
                </div>
                <div class="mt-4 flex flex-wrap gap-2">
                  <span class="tag bg-slate-900 text-white">${p.brand||'-'}</span>
                  <span class="tag bg-indigo-50 text-indigo-700">${p._cat||'-'}</span>
                  <span class="tag bg-slate-100 text-slate-700">${p.gender||'공용'}</span>
                </div>
                <div class="mt-3 line-clamp-2 text-[19px] font-black leading-7 tracking-[-0.03em] text-slate-900">${compactNameJS(p.name,p.brand)}</div>
                <div class="mt-3 product-price-row">
                  <div class="price-current">${formatPrice(p.current_price)}</div>
                  ${orig > curr && curr > 0 ? `<div class="price-original">${formatPrice(orig)}</div><div class="price-discount">${discount}% OFF</div>` : ``}
                </div>
                <div class="mt-3 flex flex-wrap gap-2">
                  ${attrs.map(x=>`<span class="tag bg-slate-100 text-slate-700">${x}</span>`).join('')}
                  ${!attrs.length && p.dominant_attribute ? `<span class="tag bg-slate-100 text-slate-700">${p.dominant_attribute}</span>` : ``}
                </div>
                <div class="mt-4 flex items-center justify-between gap-3">
                  <div class="text-xs font-bold text-slate-500">${p.price_band||'-'}</div>
                  ${p.product_url ? `<a href="${p.product_url}" target="_blank" rel="noopener noreferrer" class="rounded-2xl bg-slate-900 px-4 py-2 text-xs font-black text-white">상품 보기</a>` : ``}
                </div>
              </div>
            </article>`;
          }).join('')}
        </div>
      </div>
    </section>
  `;
}
function renderOtherDebug(){ const rows=(DATA.other_debug||[]).map(r=>({...r, gender:(r.name||"").includes("공용")?"공용":(r.gender||"공용")})); document.getElementById("otherDebugBody").innerHTML=rows.map(r=>`<tr class="border-b border-slate-200 last:border-b-0 hover:bg-slate-50/70"><td class="py-3 font-black">${r.brand}</td><td class="py-3 font-bold">${r.gender}</td><td class="py-3">${r.name}</td><td class="py-3">${r.original_item||'-'}</td><td class="py-3">${r.item_rule||'-'}</td><td class="py-3">${r.breadcrumb_item||'-'}</td><td class="py-3">${r.crawler_item||'-'}</td><td class="py-3 font-black">${r.item||'-'}</td><td class="py-3">${r.dominant_attribute||'-'}</td><td class="py-3">${r.reason||'-'}</td></tr>`).join(''); }
function baseChart(id,type,labels,values,extra={}){ if(chartRefs[id]){ chartRefs[id].destroy(); } const ctx=document.getElementById(id); if(!ctx) return; const horizontal=labels.length>=6 && type==='bar'; const g=ctx.getContext('2d').createLinearGradient(0,0,0,320); g.addColorStop(0,'rgba(59,130,246,0.92)'); g.addColorStop(1,'rgba(125,211,252,0.55)'); chartRefs[id] = new Chart(ctx,{type,data:{labels,datasets:[{data:values,backgroundColor:type==='doughnut'?['#3b82f6','#fb7185','#fb923c','#22c55e','#a78bfa']:g,borderColor:type==='doughnut'?'#ffffff':'#3b82f6',borderWidth:type==='doughnut'?3:1.5,borderRadius:14,tension:.38,cutout:type==='doughnut'?'54%':undefined,maxBarThickness:48}]},options:Object.assign({indexAxis:horizontal?'y':'x',responsive:true,maintainAspectRatio:false,animation:{duration:900,easing:'easeOutQuart'},plugins:{legend:{display:type==='doughnut',position:'bottom',labels:{usePointStyle:true,boxWidth:10,padding:16,font:{weight:'700'}}},tooltip:{backgroundColor:'rgba(15,23,42,.92)',titleFont:{weight:'800'},bodyFont:{weight:'700'},padding:12,cornerRadius:12}},scales:type==='doughnut'?{}:{y:{beginAtZero:true,ticks:{precision:0,color:'#64748b',font:{weight:'700'}},grid:{color:'rgba(148,163,184,.18)',drawBorder:false}},x:{ticks:{autoSkip:false,color:'#64748b',font:{weight:'700'}},grid:{display:false,drawBorder:false}}}},extra)}); }
function renderTopCharts(){ const brand = STATE.chartBrand; const df = normalizeProducts().filter(p => brand==="전체" ? true : p.brand===brand); const countBy = (arr, key) => { const m = {}; arr.forEach(x => { const v = (x[key]||"기타"); if(v==="기타") return; m[v] = (m[v]||0)+1; }); return m; }; const priceMap = countBy(df.filter(x=>x.price_band), "price_band"); const itemMap = countBy(df, "item_category"); const attrMap = countBy(df.filter(x=>x.dominant_attribute), "dominant_attribute"); baseChart('priceBandChart','bar',Object.keys(priceMap),Object.values(priceMap)); baseChart('categoryChart','bar',Object.keys(itemMap),Object.values(itemMap)); baseChart('dominantAttrChart','bar',Object.keys(attrMap),Object.values(attrMap)); }
function renderCharts(){ const c=DATA.charts||{}; baseChart('brandCountChart','bar',c.brandProductCounts?.labels||[],c.brandProductCounts?.values||[]); baseChart('avgPriceChart','bar',c.brandAvgPrice?.labels||[],c.brandAvgPrice?.values||[]); baseChart('genderSplitChart','doughnut',c.genderSplit?.labels||[],c.genderSplit?.values||[],{}); const brands = [...new Set((DATA.products||[]).map(p=>p.brand))]; makeTabs("chartBrandTabsA", brands, "chartBrand", renderTopCharts); makeTabs("chartBrandTabsB", brands, "chartBrand", renderTopCharts); makeTabs("chartBrandTabsC", brands, "chartBrand", renderTopCharts); renderTopCharts(); const points = c.positioning || []; if(chartRefs.positionChart){ chartRefs.positionChart.destroy(); } chartRefs.positionChart = new Chart(document.getElementById('positionChart'), { type:'bubble', data:{ datasets: points.map((p,i)=>({ label:p.brand, data:[{x:p.x,y:p.y,r:Math.max(10, Math.min(26, Math.round((p.size||1)/4)))}], avgPrice:p.avg_price, backgroundColor:['rgba(59,130,246,.72)','rgba(14,165,233,.72)','rgba(99,102,241,.72)','rgba(236,72,153,.72)'][i%4], borderColor:['#1d4ed8','#0369a1','#4338ca','#be185d'][i%4], borderWidth:2 })) }, options:{ responsive:true, maintainAspectRatio:false, animation:{duration:900,easing:'easeOutQuart'}, plugins:{ legend:{ position:'bottom', labels:{ usePointStyle:true, boxWidth:10, font:{weight:'700'} } }, tooltip:{ backgroundColor:'rgba(15,23,42,.92)', callbacks:{ label:(ctx)=>{ const raw=ctx.raw||{}; const ds=ctx.dataset||{}; return `${ds.label} · Avg ${formatPrice(ds.avgPrice)} · ${formatNumber(raw.r)} radius`; } } } }, scales:{ x:{ min:0.5, max:3.5, ticks:{ color:'#64748b',font:{weight:'700'}, callback:(v)=>({1:'Lifestyle',2:'Performance',3:'Extreme'}[v]||'') }, grid:{color:'rgba(148,163,184,.18)',drawBorder:false} }, y:{ min:0.5, max:3.5, ticks:{ color:'#64748b',font:{weight:'700'}, callback:(v)=>({1:'Mass',2:'Premium',3:'Luxury'}[v]||'') }, grid:{color:'rgba(148,163,184,.18)',drawBorder:false} } } } }); }
function init(){ createKpis(); renderBrandSummary(); renderPriceBandGenderTable(); renderAttributeGenderTable(); renderItemStyleTable(); renderKeywords(); renderProducts(); renderOtherDebug(); renderCharts(); }
document.addEventListener('DOMContentLoaded', init);
</script>
</body>
</html>
"""
    return template.replace("__DATA_JSON__", data_json).replace("__GENERATED_AT__", generated_at)

# ============================================================
# 8. OUTPUTS
# ============================================================
def write_outputs(raw_products: List[ProductRaw], analyzed_products: List[ProductAnalyzed]) -> None:
    ensure_dir(OUT_DIR)

    raw_df = pd.DataFrame([asdict(x) for x in raw_products])
    analyzed_df = products_to_dataframe(analyzed_products)
    analyzed_df = build_attribute_coverage_flags(analyzed_df)
    brand_summary_df = build_brand_summary(analyzed_df)
    keyword_df = discover_keywords(analyzed_products)
    payload = build_dashboard_payload(analyzed_df, brand_summary_df, keyword_df)
    dashboard_html = render_dashboard(payload)

    raw_df.to_csv(os.path.join(OUT_DIR, "raw_products.csv"), index=False, encoding="utf-8-sig")
    analyzed_df.to_csv(os.path.join(OUT_DIR, "analyzed_products.csv"), index=False, encoding="utf-8-sig")
    brand_summary_df.to_csv(os.path.join(OUT_DIR, "brand_summary.csv"), index=False, encoding="utf-8-sig")
    keyword_df.to_csv(os.path.join(OUT_DIR, "keyword_discovery.csv"), index=False, encoding="utf-8-sig")

    with open(os.path.join(OUT_DIR, "dashboard_data.json"), "w", encoding="utf-8") as f:
        json.dump(json_safe(payload), f, ensure_ascii=False, indent=2)
    with open(os.path.join(OUT_DIR, "dashboard.html"), "w", encoding="utf-8") as f:
        f.write(dashboard_html)

    print("\n[OUTPUTS]")
    print(f"- {os.path.join(OUT_DIR, 'raw_products.csv')}")
    print(f"- {os.path.join(OUT_DIR, 'analyzed_products.csv')}")
    print(f"- {os.path.join(OUT_DIR, 'brand_summary.csv')}")
    print(f"- {os.path.join(OUT_DIR, 'keyword_discovery.csv')}")
    print(f"- {os.path.join(OUT_DIR, 'dashboard_data.json')}")
    print(f"- {os.path.join(OUT_DIR, 'dashboard.html')}")


# ============================================================
# 9. MAIN
# ============================================================
def validate_configs(configs: List[BrandConfig]) -> None:
    errors = []
    for cfg in configs:
        if not cfg.brand:
            errors.append("brand missing")
        if not cfg.seed_urls:
            errors.append(f"{cfg.brand}: seed_urls missing")
        if not cfg.domain:
            errors.append(f"{cfg.brand}: domain missing")
    if errors:
        raise ValueError("Invalid BRAND_CONFIGS\n- " + "\n- ".join(errors))


def main() -> None:
    ensure_dir(OUT_DIR)
    validate_configs(BRAND_CONFIGS)

    crawler = AutoCompetitorCrawler(headless=HEADLESS)
    raw_products: List[ProductRaw] = []
    analyzed_products: List[ProductAnalyzed] = []

    try:
        for cfg in BRAND_CONFIGS:
            brand_items = crawler.crawl_brand(cfg)
            raw_products.extend(brand_items)
        analyzed_products = [analyze_product(p) for p in raw_products]
        write_outputs(raw_products, analyzed_products)
    finally:
        crawler.close()


if __name__ == "__main__":
    main()
