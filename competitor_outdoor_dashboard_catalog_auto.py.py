
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Competitor Outdoor Product Intelligence Dashboard (final patched)
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
DETAIL_WORKERS = int(os.getenv("DETAIL_WORKERS", "8"))
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
    ),
    BrandConfig(
        brand="DISCOVERY_EXPEDITION",
        seed_urls=["https://www.discovery-expedition.com/"],
        domain="www.discovery-expedition.com",
        brand_terms=["discovery", "디스커버리"],
        product_url_keywords=["product-detail", "/product/", "/goods/", "sku", "style"],
        listing_url_keywords=["/display/", "/category/", "/collection/", "/shop/"],
        deny_url_keywords=["login", "join", "benefit", "guide", "notice", "store-locator", "about", "magazine"],
        force_allow_url_keywords=["product-detail"],
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

NOISE_WORDS = {
    "the", "and", "with", "for", "from", "outdoor", "sports", "wear", "new", "best",
    "남성", "여성", "공용", "신상", "기본", "시즌", "정품", "단독", "라인", "기능성",
    "자켓", "팬츠", "셔츠", "신발", "모자", "가방", "상품", "제품", "스타일",
}


KEYWORD_STOPWORDS = {
    "BETTER","MAKE","MKAE","NOW","PERPECT","PERFECT","COLUMBIA","DISCOVERY","COMING","SOLD","OUT","SOON",
    "EVA","BLACK","GARY","GRAY","WHITE","BLUE","BEIGE","PINK","GREEN","RED","KHAKI","NAVY","IVORY",
    "WOMEN","WOMAN","WOMENS","MEN","MENS","UNISEX","KOREA","NEW","BEST","STYLE","ITEM","SIZE","SALE"
}
KEYWORD_STOP_PATTERNS = [
    r"^[A-Z]{1,3}$",
    r"^\d+[A-Z]?$",
    r"^(BLACK|WHITE|BLUE|BEIGE|GRAY|GARY|GREEN|RED|NAVY|KHAKI|IVORY|PINK)$",
]

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
    "집티": ["zip tee", "zip-tee", "zip tee", "zip-up", "zip up", "집업", "하프집업", "half zip", "half-zip"],
    "후디": ["hoodie", "hood", "후디", "후드", "후드티", "sweatshirt", "맨투맨"],
    "티셔츠": ["tee", "t-shirt", "t shirt", "티셔츠", "반팔", "긴팔", "sleeve"],
    "셔츠": ["shirt", "셔츠"],
    "슈즈": ["shoe", "shoes", "boot", "boots", "sneaker", "trail", "등산화", "신발", "부츠", "sandals", "샌들"],
    "백팩": ["backpack", "bag", "pack", "배낭", "백팩", "가방"],
    "캡": ["cap", "hat", "모자", "캡", "비니"],
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
    crawled_at: str


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

# ============================================================
# 3. ANALYZER
# ============================================================
def infer_gender(name: str, description: str, raw_gender: str) -> str:
    blob = f"{name} {description} {raw_gender}".lower()
    if any(x in blob for x in ["women", "womens", "여성", "우먼", "woman"]):
        return "여성"
    if any(x in blob for x in ["men", "mens", "남성", "맨즈", "man"]):
        return "남성"
    if any(x in blob for x in ["unisex", "공용"]):
        return "공용"
    return "미분류"


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



def infer_item_category(name: str, description: str) -> str:
    blob = f"{name} {description}".lower()

    # stronger direct rules to reduce 기타
    if any(x in blob for x in ["zip tee", "zip-up", "zip up", "집업", "하프집업", "half zip", "half-zip"]):
        return "집티"
    if any(x in blob for x in ["hoodie", "후디", "후드", "후드티", "맨투맨", "sweatshirt"]):
        return "후디"
    if any(x in blob for x in ["fleece", "플리스", "boa", "보아"]):
        return "플리스"
    if any(x in blob for x in ["down", "패딩", "덕다운", "구스다운", "puffer"]):
        return "다운"

    for category, keywords in CATEGORY_RULES.items():
        if any(k.lower() in blob for k in keywords):
            return category

    # fallback by common apparel words to minimize 기타
    if any(x in blob for x in ["jacket", "shell", "parka", "자켓", "재킷"]):
        return "자켓"
    if any(x in blob for x in ["pants", "pant", "바지", "팬츠"]):
        return "팬츠"
    if any(x in blob for x in ["shoe", "boot", "신발", "등산화", "부츠"]):
        return "슈즈"
    if any(x in blob for x in ["tee", "티셔츠", "반팔", "긴팔"]):
        return "티셔츠"
    return "기타"


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




def refine_item_category(name: str, description: str, current_category: str) -> str:
    if current_category != "기타":
        return current_category
    blob = f"{name} {description}".lower()
    extra_rules = [
        ("집티", ["zip tee", "zip-up", "zip up", "집업", "하프집업", "half zip", "half-zip"]),
        ("후디", ["hoodie", "후디", "후드", "후드티", "맨투맨", "sweatshirt"]),
        ("플리스", ["fleece", "플리스", "boa", "보아"]),
        ("다운", ["down", "패딩", "덕다운", "구스다운", "puffer"]),
        ("자켓", ["jacket", "shell", "parka", "자켓", "재킷", "아노락", "바람막이"]),
        ("팬츠", ["pants", "pant", "바지", "팬츠", "조거", "쇼츠", "반바지"]),
        ("슈즈", ["shoe", "boot", "신발", "등산화", "부츠", "sneaker"]),
        ("티셔츠", ["tee", "티셔츠", "반팔", "긴팔", "sleeve"]),
    ]
    for cat, rules in extra_rules:
        if any(x in blob for x in rules):
            return cat
    return current_category


def refine_standard_attributes(name: str, description: str, attrs: List[str]) -> List[str]:
    blob = f"{name} {description}".lower()
    out = list(attrs)
    mapping_rules = [
        ("2L", ["2l", "2-layer", "2 layer"]),
        ("2.5L", ["2.5l", "2.5-layer", "2.5 layer"]),
        ("3L", ["3l", "3-layer", "3 layer"]),
        ("방풍", ["windproof", "wind stopper", "windstopper", "방풍"]),
        ("방수", ["waterproof", "wp", "방수", "발수", "water resistant"]),
        ("고어텍스", ["gore-tex", "gore tex"]),
        ("다운", ["down", "덕다운", "구스다운", "충전재"]),
        ("보온", ["thermal", "heat", "보온", "플리스"]),
    ]
    for attr, rules in mapping_rules:
        if attr not in out and any(x in blob for x in rules):
            out.append(attr)
    return unique_preserve_order(out)


def build_other_debug_table(df: pd.DataFrame) -> List[dict]:
    if df.empty:
        return []
    rows = []
    subset = df[(df["item_category"].fillna("기타") == "기타") | (df["dominant_attribute"].fillna("기타") == "기타")].copy()
    for _, r in subset.head(60).iterrows():
        blob = f"{r.get('name','')} {r.get('description','')}".lower()
        reason_parts = []
        if r.get("item_category", "기타") == "기타":
            reason_parts.append("item rule miss")
        if r.get("dominant_attribute", "기타") == "기타":
            reason_parts.append("attribute rule miss")
        tokens = re.findall(r"[a-zA-Z가-힣0-9\-\.]{3,}", blob)
        tokens = [t for t in tokens if t.lower() not in NOISE_WORDS][:8]
        rows.append({
            "brand": r.get("brand", ""),
            "gender": r.get("gender", ""),
            "name": r.get("name", ""),
            "item_category": r.get("item_category", ""),
            "dominant_attribute": r.get("dominant_attribute", ""),
            "reason": ", ".join(reason_parts),
            "tokens": ", ".join(tokens),
        })
    return rows


def analyze_product(raw: ProductRaw) -> ProductAnalyzed:
    current_price = parse_price_to_int(raw.price_text)
    original_price = parse_price_to_int(raw.original_price_text)
    if current_price is not None and original_price is not None and original_price < current_price:
        original_price = current_price
    if current_price is None and original_price is not None:
        current_price = original_price
    if original_price is None and current_price is not None:
        original_price = current_price

    raw_keywords = extract_raw_keywords(raw.name, raw.description)
    std_attrs = map_standard_attributes(raw_keywords, raw.name, raw.description)
    std_attrs = refine_standard_attributes(raw.name, raw.description, std_attrs)
    resolved_attrs = resolve_conflicts(std_attrs)
    dominant = select_dominant_attribute(resolved_attrs)
    grade = classify_grade(resolved_attrs, dominant)
    shell_type = classify_shell_type(resolved_attrs, raw.name, raw.description)
    price_band = classify_price_band(current_price)
    pos_y = classify_positioning_y(current_price)
    pos_x = classify_positioning_x(resolved_attrs, dominant, shell_type)
    item_category = refine_item_category(raw.name, raw.description, infer_item_category(raw.name, raw.description))

    return ProductAnalyzed(
        brand=raw.brand,
        source_url=raw.source_url,
        product_url=raw.product_url,
        name=raw.name,
        description=raw.description,
        image_url=raw.image_url,
        current_price=current_price,
        original_price=original_price,
        discount_rate=calc_discount_rate(current_price, original_price),
        sold_out=bool(safe_text(raw.sold_out_text)),
        gender=infer_gender(raw.name, raw.description, raw.gender_text),
        season=infer_season(raw.name, raw.description, raw.season_text),
        item_category=item_category,
        raw_keywords=raw_keywords,
        standard_attributes=resolved_attrs,
        dominant_attribute=dominant,
        grade=grade,
        shell_type=shell_type,
        price_band=price_band,
        positioning_y=pos_y,
        positioning_x=pos_x,
        attribute_coverage_flag="OK",
        crawled_at=raw.crawled_at,
    )


# ============================================================
# 4. KEYWORD DISCOVERY
# ============================================================

def _keyword_allowed(kw: str, brand: str = "") -> bool:
    raw = safe_text(kw).upper()
    if not raw or len(raw) < 2:
        return False
    if raw in KEYWORD_STOPWORDS:
        return False
    if brand and raw == brand.upper():
        return False
    if raw.lower() in NOISE_WORDS:
        return False
    if any(re.match(p, raw) for p in KEYWORD_STOP_PATTERNS):
        return False
    return True


def discover_keywords(products: List[ProductAnalyzed]) -> pd.DataFrame:
    rows = []
    brand_keyword_counter: Dict[str, Dict[str, int]] = {}
    global_counter: Dict[str, int] = {}

    for p in products:
        raws = unique_preserve_order([x.upper() for x in p.raw_keywords if safe_text(x)])
        brand_keyword_counter.setdefault(p.brand, {})
        for kw in raws:
            if not _keyword_allowed(kw, p.brand):
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
                    "*.png", "*.jpg", "*.jpeg", "*.gif", "*.webp", "*.svg",
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
        discovered: List[str] = []
        seen: Set[str] = set()

        for seed in cfg.seed_urls:
            if seed in seen:
                continue
            print(f"  - seed: {seed}")
            seen.add(seed)
            discovered.append(seed)

            if not self._safe_get(seed):
                continue
            self._scroll_to_end()
            links = self._extract_all_links()

            for link in links:
                if len(discovered) >= MAX_DISCOVERED_LISTING_URLS:
                    break
                if not same_domain(link, cfg.domain):
                    continue
                if self._is_deny_url(link, cfg):
                    continue
                if self._is_product_url(link, cfg):
                    continue
                if self._is_listing_url(link, cfg):
                    if link not in seen:
                        seen.add(link)
                        discovered.append(link)

        return discovered[:MAX_DISCOVERED_LISTING_URLS]

    def collect_product_urls_from_listing(self, listing_url: str, cfg: BrandConfig) -> List[str]:
        product_urls: List[str] = []
        if not self._safe_get(listing_url):
            return product_urls

        self._scroll_to_end()
        links = self._extract_all_links()

        for link in links:
            if not same_domain(link, cfg.domain):
                continue
            if self._is_deny_url(link, cfg):
                continue
            if self._is_product_url(link, cfg):
                if self._passes_brand_filter(link, cfg):
                    product_urls.append(link)

        # direct CSS fallback: product-detail style anchors
        direct_selectors = [
            "a[href*='product-detail']",
            "a[href*='/product/']",
            "a[href*='/products/']",
            "a[href*='/goods/']",
            "a[href*='/p/']",
            "a[href*='goodsNo=']",
            "a[href*='productNo=']",
        ]
        for css in direct_selectors:
            try:
                anchors = self.driver.find_elements(By.CSS_SELECTOR, css)
                for a in anchors:
                    href = get_attr_from_element(a, "href")
                    if not href:
                        continue
                    href = canonicalize_url(href)
                    if same_domain(href, cfg.domain) and not self._is_deny_url(href, cfg):
                        if self._passes_brand_filter(href, cfg):
                            product_urls.append(href)
            except Exception:
                continue

        return unique_preserve_order(product_urls)


    def crawl_product_detail_requests(self, product_url: str, source_url: str, cfg: BrandConfig) -> Optional[ProductRaw]:
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

            if not title and not price_text and not desc:
                return None

            sanity_blob = f"{title} {desc}"
            if cfg.brand_terms and cfg.domain in {"www.k-village.co.kr"}:
                if not text_contains_brand(sanity_blob, cfg.brand_terms):
                    return None

            return ProductRaw(
                brand=cfg.brand,
                source_url=source_url,
                product_url=product_url,
                name=compact_text(title),
                description=compact_text(desc),
                price_text=compact_text(price_text),
                original_price_text=compact_text(original_price_text),
                image_url=compact_text(image_url),
                sold_out_text=compact_text(sold_out_text),
                gender_text=compact_text(gender_text),
                season_text=compact_text(season_text),
                crawled_at=TODAY_STR,
            )
        except Exception:
            return None


    def crawl_product_detail(self, product_url: str, source_url: str, cfg: BrandConfig) -> Optional[ProductRaw]:
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
        sold_out_text = try_find_text(self.driver, GENERIC_SOLDOUT_SELECTORS)
        gender_text = try_find_text(self.driver, GENERIC_GENDER_SELECTORS)
        season_text = try_find_text(self.driver, GENERIC_SEASON_SELECTORS)

        if not title and not price_text and not desc:
            return None

        sanity_blob = f"{title} {desc}"
        if cfg.brand_terms and cfg.domain in {"www.k-village.co.kr"}:
            if not text_contains_brand(sanity_blob, cfg.brand_terms):
                return None

        return ProductRaw(
            brand=cfg.brand,
            source_url=source_url,
            product_url=product_url,
            name=compact_text(title),
            description=compact_text(desc),
            price_text=compact_text(price_text),
            original_price_text=compact_text(original_price_text),
            image_url=compact_text(image_url),
            sold_out_text=compact_text(sold_out_text),
            gender_text=compact_text(gender_text),
            season_text=compact_text(season_text),
            crawled_at=TODAY_STR,
        )


    def crawl_brand(self, cfg: BrandConfig) -> List[ProductRaw]:
        print(f"\n[CRAWL START] {cfg.brand} :: seeds={len(cfg.seed_urls)}")
        listings = self.discover_listing_urls(cfg)
        product_urls: List[str] = []

        for listing in listings:
            print(f"  - listing: {listing}")
            try:
                urls = self.collect_product_urls_from_listing(listing, cfg)
                product_urls.extend(urls)
            except Exception as e:
                print(f"    [WARN] listing failed: {e}")

        product_urls = unique_preserve_order(product_urls)
        if len(product_urls) > cfg.max_products:
            product_urls = product_urls[:cfg.max_products]

        print(f"  - discovered product urls: {len(product_urls)}")

        raw_products: List[ProductRaw] = []
        unresolved_urls: List[str] = []

        # Fast path: requests-first in parallel
        with ThreadPoolExecutor(max_workers=max(1, DETAIL_WORKERS)) as ex:
            futures = {ex.submit(self.crawl_product_detail_requests, url, url, cfg): url for url in product_urls}
            done_count = 0
            for fut in as_completed(futures):
                done_count += 1
                url = futures[fut]
                try:
                    item = fut.result()
                    if item and item.name:
                        raw_products.append(item)
                    else:
                        unresolved_urls.append(url)
                except Exception:
                    unresolved_urls.append(url)
                if done_count % 20 == 0:
                    print(f"    [OK] {cfg.brand} requests pass {done_count}/{len(product_urls)} | valid {len(raw_products)}")

        # Accurate fallback: selenium only for unresolved URLs
        for idx, product_url in enumerate(unresolved_urls, start=1):
            try:
                item = self.crawl_product_detail(product_url, product_url, cfg)
                if item and item.name:
                    raw_products.append(item)
            except Exception as e:
                print(f"    [WARN] selenium fallback failed: {product_url} | {e}")
            if idx % 20 == 0:
                print(f"    [OK] {cfg.brand} selenium fallback {idx}/{len(unresolved_urls)} | valid {len(raw_products)}")

        # dedupe by product_url
        dedup = {}
        for item in raw_products:
            dedup[item.product_url] = item
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
    grouped["avg_price"] = grouped["avg_price"].fillna(0).round(0).astype(int)
    grouped["min_price"] = grouped["min_price"].fillna(0).astype(int)
    grouped["max_price"] = grouped["max_price"].fillna(0).astype(int)
    grouped = grouped.sort_values(["brand", "gender", "style_count", "avg_price"], ascending=[True, True, False, False])
    return grouped.to_dict("records")



def build_dashboard_payload(df: pd.DataFrame, brand_summary: pd.DataFrame, kw_df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "generated_at": TODAY_STR,
            "kpis": {"brands": 0, "products": 0, "sale_products": 0, "avg_price": 0, "others_ratio": 0},
            "brand_summary": [],
            "products": [],
            "charts": {},
            "keywords": [],
            "price_band_gender_table": [],
            "attribute_gender_table": [],
            "item_style_table": [],
            "other_debug_table": [],
        }

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
            "labels": list(df["price_band"].fillna("기타").value_counts().index),
            "values": list(df["price_band"].fillna("기타").value_counts().values),
        },
        "itemCategory": {
            "labels": list(df["item_category"].fillna("기타").value_counts().head(12).index),
            "values": list(df["item_category"].fillna("기타").value_counts().head(12).values),
        },
        "dominantAttribute": {
            "labels": list(df["dominant_attribute"].fillna("기타").value_counts().head(10).index),
            "values": list(df["dominant_attribute"].fillna("기타").value_counts().head(10).values),
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
            "labels": list(df["gender"].fillna("미분류").value_counts().index),
            "values": list(df["gender"].fillna("미분류").value_counts().values),
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
            keywords.append({"brand": brand, "items": g.head(16).to_dict("records")})

    others_ratio = round(float((df["item_category"].fillna("기타") == "기타").mean()) * 100, 1)
    products_feed = (
        df.loc[~df["sold_out"].fillna(False)]
        .sort_values(["brand", "item_category", "gender", "current_price"], ascending=[True, True, True, False])
        .fillna("")
        .to_dict("records")
    )

    return {
        "generated_at": TODAY_STR,
        "kpis": {
            "brands": int(df["brand"].nunique()),
            "products": int(len(df)),
            "sale_products": int(df["discount_rate"].fillna(0).gt(0).sum()),
            "avg_price": int(df["current_price"].dropna().mean()) if df["current_price"].notna().any() else 0,
            "others_ratio": others_ratio,
        },
        "brand_summary": brand_summary.to_dict("records"),
        "products": products_feed,
        "charts": charts,
        "keywords": keywords,
        "price_band_gender_table": build_price_band_gender_table(df),
        "attribute_gender_table": build_attribute_gender_table(df),
        "item_style_table": build_item_style_table(df),
        "other_debug_table": build_other_debug_table(df),
    }


def render_dashboard(payload: dict) -> str:
    data_json = json.dumps(json_safe(payload), ensure_ascii=False)
    generated_at = html.escape(payload.get("generated_at", ""))

    template = """<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Competitor Outdoor Dashboard</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200;400;600;800&display=swap');
    :root { --brand:#0f172a; --bg0:#f6f8fb; --bg1:#eef3f9; --line:rgba(255,255,255,0.78); }
    * { box-sizing:border-box; }
    body{
      background: linear-gradient(180deg, var(--bg0), var(--bg1));
      font-family: 'Plus Jakarta Sans', sans-serif;
      color:#0f172a;
      min-height:100vh;
    }
    .glass-card{
      background: rgba(255,255,255,0.68);
      backdrop-filter: blur(18px);
      border: 1px solid rgba(255,255,255,0.85);
      border-radius: 28px;
      box-shadow: 0 20px 50px rgba(0,45,114,0.06);
      animation: fadeUp .6s ease both;
    }
    .hero-card{
      position:relative;
      overflow:hidden;
    }
    .hero-card::before{
      content:'';
      position:absolute;
      inset:-20% auto auto -10%;
      width:280px;
      height:280px;
      border-radius:999px;
      background: radial-gradient(circle, rgba(29,78,216,.16), rgba(29,78,216,0));
      filter: blur(4px);
      pointer-events:none;
      animation: softPulse 6s ease-in-out infinite;
    }
    .hero-card::after{
      content:'';
      position:absolute;
      inset:auto -10% -25% auto;
      width:240px;
      height:240px;
      border-radius:999px;
      background: radial-gradient(circle, rgba(96,165,250,.14), rgba(96,165,250,0));
      filter: blur(4px);
      pointer-events:none;
      animation: softPulse 7s ease-in-out infinite;
    }
    .metric-card{
      position:relative;
      overflow:hidden;
      transition: transform .22s ease, box-shadow .22s ease;
    }
    .metric-card:hover, .panel:hover{
      transform: translateY(-2px);
      box-shadow: 0 24px 56px rgba(15,23,42,.09);
    }
    .panel{
      transition: transform .22s ease, box-shadow .22s ease;
      animation: fadeUp .65s ease both;
    }
    .tag{
      display:inline-flex;
      align-items:center;
      border-radius:999px;
      padding:4px 10px;
      font-size:11px;
      font-weight:800;
    }
    .chart-shell{
      position:relative;
      min-height: 340px;
    }
    .chart-shell canvas{
      width:100% !important;
      height:100% !important;
    }
    .chart-shell.compact{ min-height: 280px; }
    .chart-shell.tall{ min-height: 420px; }
    .table-wrap::-webkit-scrollbar{ height:8px; width:8px; }
    .table-wrap::-webkit-scrollbar-thumb{ background:#cbd5e1; border-radius:999px; }
    .section-eyebrow{
      font-size: 10px;
      font-weight: 900;
      letter-spacing: .24em;
      text-transform: uppercase;
      color:#94a3b8;
    }
    .mention-card{
      padding:16px;
      border-radius:24px;
      background: rgba(255,255,255,0.92);
      border:1px solid rgba(226,232,240,.95);
      box-shadow: 0 10px 28px rgba(15,23,42,.04);
      transition: transform .2s ease, box-shadow .2s ease, border-color .2s ease;
      animation: slideUpSoft .55s ease both;
    }
    .mention-card:hover{
      transform: translateY(-2px);
      border-color: rgba(96,165,250,.7);
      box-shadow: 0 16px 32px rgba(15,23,42,.08);
    }
    .brand-section-title{
      display:flex; align-items:center; gap:10px; margin-bottom:14px;
    }
    .brand-dot{
      width:10px; height:10px; border-radius:999px;
      background: linear-gradient(180deg, #1d4ed8, #60a5fa);
      box-shadow: 0 0 0 6px rgba(96,165,250,.14);
    }
    .pill-dark{ background:#0f172a; color:#fff; }
    .pill-soft{ background:#f1f5f9; color:#334155; }
    .pill-blue{ background:#eff6ff; color:#1d4ed8; }
    .pill-violet{ background:#f5f3ff; color:#6d28d9; }
    .pill-emerald{ background:#ecfdf5; color:#047857; }
    .pill-rose{ background:#fff1f2; color:#be123c; }
    .product-grid-group{
      margin-top:28px;
    }
    .divider{
      height:1px; background: rgba(148,163,184,.18); margin: 18px 0;
    }
    @keyframes fadeUp{
      from{ opacity:0; transform: translateY(10px) scale(.99); }
      to{ opacity:1; transform: translateY(0) scale(1); }
    }
    @keyframes slideUpSoft{
      from{ opacity:0; transform: translateY(12px); }
      to{ opacity:1; transform: translateY(0); }
    }
    @keyframes softPulse{
      0%,100%{ transform: scale(1); opacity:.85; }
      50%{ transform: scale(1.08); opacity:1; }
    }
  </style>
</head>
<body>
  <div class="mx-auto max-w-[1800px] px-5 py-6 lg:px-8">
    <section class="glass-card hero-card p-6 lg:p-8">
      <div class="relative z-[2] flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <div class="section-eyebrow">COMPETITOR PRODUCT INTELLIGENCE</div>
          <h1 class="mt-2 text-3xl lg:text-5xl font-black tracking-[-0.05em] text-slate-900">컬럼비아 · 디스커버리 경쟁사 상품 분석</h1>
          <div class="mt-3 max-w-4xl text-sm font-bold leading-6 text-slate-600">브랜드 × 성별 × 가격대 스타일 수, 기능 속성, 아이템별 스타일 수를 실무용으로 바로 보기 쉽게 정리했습니다. 기타는 자동 보정 후 남은 항목만 디버그 테이블로 노출합니다.</div>
        </div>
        <div class="rounded-3xl bg-slate-900 px-5 py-4 text-white shadow-xl">
          <div class="text-[11px] font-extrabold tracking-[0.18em] text-slate-300">GENERATED</div>
          <div class="mt-2 text-lg font-black">__GENERATED_AT__</div>
        </div>
      </div>
    </section>

    <section class="mt-6 grid grid-cols-2 lg:grid-cols-5 gap-4" id="kpi-grid"></section>

    <section class="mt-6 grid grid-cols-1 xl:grid-cols-3 gap-4">
      <div class="glass-card panel p-5 xl:col-span-1">
        <div class="section-eyebrow">SKU</div>
        <div class="mt-1 text-xl font-black tracking-[-0.03em]">브랜드별 SKU 수</div>
        <div class="chart-shell compact mt-4"><canvas id="brandCountChart"></canvas></div>
      </div>
      <div class="glass-card panel p-5 xl:col-span-1">
        <div class="section-eyebrow">PRICE</div>
        <div class="mt-1 text-xl font-black tracking-[-0.03em]">브랜드별 평균가</div>
        <div class="chart-shell compact mt-4"><canvas id="avgPriceChart"></canvas></div>
      </div>
      <div class="glass-card panel p-5 xl:col-span-1">
        <div class="section-eyebrow">GENDER</div>
        <div class="mt-1 text-xl font-black tracking-[-0.03em]">남녀 비중</div>
        <div class="chart-shell compact mt-4"><canvas id="genderSplitChart"></canvas></div>
      </div>
    </section>

    <section class="mt-6 grid grid-cols-1 xl:grid-cols-3 gap-4">
      <div class="glass-card panel p-5">
        <div class="section-eyebrow">PRICE BAND</div>
        <div class="mt-1 text-xl font-black tracking-[-0.03em]">가격대 분포</div>
        <div class="chart-shell mt-4"><canvas id="priceBandChart"></canvas></div>
      </div>
      <div class="glass-card panel p-5">
        <div class="section-eyebrow">ITEM MIX</div>
        <div class="mt-1 text-xl font-black tracking-[-0.03em]">아이템 분포</div>
        <div class="chart-shell mt-4"><canvas id="categoryChart"></canvas></div>
      </div>
      <div class="glass-card panel p-5">
        <div class="section-eyebrow">ATTRIBUTE</div>
        <div class="mt-1 text-xl font-black tracking-[-0.03em]">대표 속성 분포</div>
        <div class="chart-shell mt-4"><canvas id="dominantAttrChart"></canvas></div>
      </div>
    </section>

    <section class="mt-6 grid grid-cols-1 xl:grid-cols-[1.35fr_.65fr] gap-4">
      <div class="glass-card panel p-5">
        <div class="section-eyebrow">BRAND TABLE</div>
        <div class="mt-1 text-xl font-black tracking-[-0.03em]">브랜드 비교표</div>
        <div class="table-wrap mt-4 overflow-x-auto">
          <table class="min-w-[1100px] w-full text-sm">
            <thead>
              <tr class="border-b border-slate-200 text-slate-500">
                <th class="py-3 text-left">Brand</th>
                <th class="py-3 text-right">Total</th>
                <th class="py-3 text-right">Active</th>
                <th class="py-3 text-right">Avg Price</th>
                <th class="py-3 text-right">Sale %</th>
                <th class="py-3 text-right">방수</th>
                <th class="py-3 text-right">고어텍스</th>
                <th class="py-3 text-right">다운</th>
                <th class="py-3 text-right">자켓</th>
                <th class="py-3 text-right">팬츠</th>
                <th class="py-3 text-right">슈즈</th>
              </tr>
            </thead>
            <tbody id="brandSummaryBody"></tbody>
          </table>
        </div>
      </div>
      <div class="glass-card panel p-5">
        <div class="section-eyebrow">POSITIONING</div>
        <div class="mt-1 text-xl font-black tracking-[-0.03em]">브랜드 포지셔닝</div>
        <div class="chart-shell tall mt-4"><canvas id="positionChart"></canvas></div>
        <div class="mt-3 text-xs font-bold text-slate-500">X: Lifestyle → Performance → Extreme / Y: Mass → Premium → Luxury</div>
      </div>
    </section>

    <section class="mt-6 grid grid-cols-1 xl:grid-cols-2 gap-4">
      <div class="glass-card panel p-5">
        <div class="section-eyebrow">PRICE BY GENDER</div>
        <div class="mt-1 text-xl font-black tracking-[-0.03em]">브랜드 × 성별 가격대 스타일 수</div>
        <div class="table-wrap mt-4 overflow-x-auto">
          <table class="min-w-[900px] w-full text-sm">
            <thead>
              <tr class="border-b border-slate-200 text-slate-500">
                <th class="py-3 text-left">Brand</th>
                <th class="py-3 text-left">Gender</th>
                <th class="py-3 text-right">0-9.9만</th>
                <th class="py-3 text-right">10-19.9만</th>
                <th class="py-3 text-right">20-29.9만</th>
                <th class="py-3 text-right">30-49.9만</th>
                <th class="py-3 text-right">50만+</th>
                <th class="py-3 text-right">Total</th>
              </tr>
            </thead>
            <tbody id="priceBandGenderBody"></tbody>
          </table>
        </div>
      </div>

      <div class="glass-card panel p-5">
        <div class="section-eyebrow">ATTRIBUTE BY GENDER</div>
        <div class="mt-1 text-xl font-black tracking-[-0.03em]">브랜드 × 성별 속성 수</div>
        <div class="table-wrap mt-4 overflow-x-auto">
          <table class="min-w-[960px] w-full text-sm">
            <thead>
              <tr class="border-b border-slate-200 text-slate-500">
                <th class="py-3 text-left">Brand</th>
                <th class="py-3 text-left">Gender</th>
                <th class="py-3 text-right">2L</th>
                <th class="py-3 text-right">2.5L</th>
                <th class="py-3 text-right">3L</th>
                <th class="py-3 text-right">방수</th>
                <th class="py-3 text-right">방풍</th>
                <th class="py-3 text-right">고어텍스</th>
                <th class="py-3 text-right">다운</th>
                <th class="py-3 text-right">집티</th>
              </tr>
            </thead>
            <tbody id="attributeGenderBody"></tbody>
          </table>
        </div>
      </div>
    </section>

    <section class="mt-6 glass-card panel p-5">
      <div class="section-eyebrow">ITEM STYLE MIX</div>
      <div class="mt-1 text-xl font-black tracking-[-0.03em]">브랜드 × 성별 × 아이템별 스타일 수 / 가격대</div>
      <div class="table-wrap mt-4 overflow-x-auto">
        <table class="min-w-[1100px] w-full text-sm">
          <thead>
            <tr class="border-b border-slate-200 text-slate-500">
              <th class="py-3 text-left">Brand</th>
              <th class="py-3 text-left">Gender</th>
              <th class="py-3 text-left">Item</th>
              <th class="py-3 text-right">Styles</th>
              <th class="py-3 text-right">Avg Price</th>
              <th class="py-3 text-right">Min Price</th>
              <th class="py-3 text-right">Max Price</th>
            </tr>
          </thead>
          <tbody id="itemStyleBody"></tbody>
        </table>
      </div>
    </section>

    <section class="mt-6 glass-card panel p-5">
      <div class="section-eyebrow">KEYWORD MAP</div>
      <div class="mt-1 text-xl font-black tracking-[-0.03em]">브랜드별 키워드</div>
      <div id="keywordGrid" class="mt-4 grid grid-cols-1 xl:grid-cols-2 gap-4"></div>
    </section>

    <section class="mt-6 glass-card panel p-5">
      <div class="section-eyebrow">OTHER DEBUG</div>
      <div class="mt-1 text-xl font-black tracking-[-0.03em]">기타 디버그</div>
      <div class="mt-2 text-sm font-bold text-slate-500">자동 분류 후에도 기타로 남은 항목만 표시합니다. 이 표를 보고 규칙을 계속 추가하면 기타 비중을 더 낮출 수 있습니다.</div>
      <div class="table-wrap mt-4 overflow-x-auto">
        <table class="min-w-[1100px] w-full text-sm">
          <thead>
            <tr class="border-b border-slate-200 text-slate-500">
              <th class="py-3 text-left">Brand</th>
              <th class="py-3 text-left">Gender</th>
              <th class="py-3 text-left">Name</th>
              <th class="py-3 text-left">Item</th>
              <th class="py-3 text-left">Dominant Attr</th>
              <th class="py-3 text-left">Reason</th>
              <th class="py-3 text-left">Tokens</th>
            </tr>
          </thead>
          <tbody id="otherDebugBody"></tbody>
        </table>
      </div>
    </section>

    <section class="mt-6 glass-card panel p-5">
      <div class="flex items-end justify-between gap-3 flex-wrap">
        <div>
          <div class="section-eyebrow">PRODUCT FEED</div>
          <div class="mt-1 text-xl font-black tracking-[-0.03em]">상품 카드 피드</div>
        </div>
        <div id="productCountText" class="text-sm font-bold text-slate-500"></div>
      </div>
      <div id="productGroupedFeed" class="mt-4"></div>
    </section>
  </div>

<script>
const DATA = __DATA_JSON__;

function formatNumber(v){
  if (v === null || v === undefined || v === "") return "-";
  return new Intl.NumberFormat('ko-KR').format(v);
}
function formatPrice(v){
  if (!v && v !== 0) return "-";
  return formatNumber(v) + "원";
}

function createKpis(){
  const k = DATA.kpis;
  const items = [
    ["브랜드 수", k.brands, "크롤링 완료 브랜드"],
    ["총 상품 수", k.products, "현재 분석 SKU"],
    ["세일 상품 수", k.sale_products, "할인 상품"],
    ["평균가", formatPrice(k.avg_price), "현재가 평균"],
    ["기타 비중", (k.others_ratio || 0) + "%", "낮을수록 좋음"],
  ];
  document.getElementById('kpi-grid').innerHTML = items.map(([label, value, desc], idx) => `
    <div class="glass-card metric-card p-5" style="animation-delay:${idx * 55}ms">
      <div class="section-eyebrow">${label}</div>
      <div class="mt-2 text-3xl font-black tracking-[-0.05em] text-slate-900">${value}</div>
      <div class="mt-1 text-xs font-bold text-slate-500">${desc}</div>
    </div>`).join('');
}

function renderBrandSummary(){
  const rows = DATA.brand_summary || [];
  document.getElementById('brandSummaryBody').innerHTML = rows.map(r => `
    <tr class="border-b border-slate-200 last:border-b-0 hover:bg-slate-50/70">
      <td class="py-3 font-black text-slate-900">${r.brand}</td>
      <td class="py-3 text-right">${formatNumber(r.total_products)}</td>
      <td class="py-3 text-right">${formatNumber(r.active_products)}</td>
      <td class="py-3 text-right">${formatPrice(r.avg_price)}</td>
      <td class="py-3 text-right">${r.sale_share_pct}%</td>
      <td class="py-3 text-right">${formatNumber(r.waterproof_products)}</td>
      <td class="py-3 text-right">${formatNumber(r.goretex_products)}</td>
      <td class="py-3 text-right">${formatNumber(r.down_products)}</td>
      <td class="py-3 text-right">${formatNumber(r.jackets)}</td>
      <td class="py-3 text-right">${formatNumber(r.pants)}</td>
      <td class="py-3 text-right">${formatNumber(r.shoes)}</td>
    </tr>`).join('');
}

function renderMergedRows(targetId, rows, rowBuilder){
  const tbody = document.getElementById(targetId);
  let html = '';
  let i = 0;
  while (i < rows.length){
    const brand = rows[i].brand;
    const group = rows.filter(r => r.brand === brand);
    group.forEach((r, idx) => { html += rowBuilder(r, idx, group.length); });
    i += group.length;
  }
  tbody.innerHTML = html;
}

function renderPriceBandGenderTable(){
  const rows = (DATA.price_band_gender_table || []).slice().sort((a,b) => (a.brand+b.gender).localeCompare(b.brand+b.gender));
  renderMergedRows('priceBandGenderBody', rows, (r, idx, span) => `
    <tr class="border-b border-slate-200 last:border-b-0 hover:bg-slate-50/70">
      ${idx === 0 ? `<td class="py-3 font-black align-top" rowspan="${span}">${r.brand}</td>` : ``}
      <td class="py-3 font-bold">${r.gender}</td>
      <td class="py-3 text-right">${formatNumber(r["0-9.9만"])}</td>
      <td class="py-3 text-right">${formatNumber(r["10-19.9만"])}</td>
      <td class="py-3 text-right">${formatNumber(r["20-29.9만"])}</td>
      <td class="py-3 text-right">${formatNumber(r["30-49.9만"])}</td>
      <td class="py-3 text-right">${formatNumber(r["50만+"])}</td>
      <td class="py-3 text-right font-black">${formatNumber(r.total_styles)}</td>
    </tr>`);
}

function renderAttributeGenderTable(){
  const rows = (DATA.attribute_gender_table || []).slice().sort((a,b) => (a.brand+b.gender).localeCompare(b.brand+b.gender));
  renderMergedRows('attributeGenderBody', rows, (r, idx, span) => `
    <tr class="border-b border-slate-200 last:border-b-0 hover:bg-slate-50/70">
      ${idx === 0 ? `<td class="py-3 font-black align-top" rowspan="${span}">${r.brand}</td>` : ``}
      <td class="py-3 font-bold">${r.gender}</td>
      <td class="py-3 text-right">${formatNumber(r["2L"])}</td>
      <td class="py-3 text-right">${formatNumber(r["2.5L"])}</td>
      <td class="py-3 text-right">${formatNumber(r["3L"])}</td>
      <td class="py-3 text-right">${formatNumber(r["방수"])}</td>
      <td class="py-3 text-right">${formatNumber(r["방풍"])}</td>
      <td class="py-3 text-right">${formatNumber(r["고어텍스"])}</td>
      <td class="py-3 text-right">${formatNumber(r["다운"])}</td>
      <td class="py-3 text-right">${formatNumber(r["집티"])}</td>
    </tr>`);
}

function renderItemStyleTable(){
  const rows = (DATA.item_style_table || []).slice().sort((a,b) => (a.brand+a.gender).localeCompare(b.brand+b.gender));
  const tbody = document.getElementById('itemStyleBody');
  let html = '';
  let i = 0;
  while (i < rows.length){
    const brand = rows[i].brand;
    const brandGroup = rows.filter(r => r.brand === brand);
    let j = 0;
    while (j < brandGroup.length){
      const gender = brandGroup[j].gender;
      const genderGroup = brandGroup.filter(r => r.gender === gender);
      genderGroup.forEach((r, idx) => {
        html += `<tr class="border-b border-slate-200 last:border-b-0 hover:bg-slate-50/70">
          ${(j === 0 && idx === 0) ? `<td class="py-3 font-black align-top" rowspan="${brandGroup.length}">${r.brand}</td>` : ``}
          ${idx === 0 ? `<td class="py-3 font-bold align-top" rowspan="${genderGroup.length}">${r.gender}</td>` : ``}
          <td class="py-3">${r.item_category}</td>
          <td class="py-3 text-right font-black">${formatNumber(r.style_count)}</td>
          <td class="py-3 text-right">${formatPrice(r.avg_price)}</td>
          <td class="py-3 text-right">${formatPrice(r.min_price)}</td>
          <td class="py-3 text-right">${formatPrice(r.max_price)}</td>
        </tr>`;
      });
      j += genderGroup.length;
    }
    i += brandGroup.length;
  }
  tbody.innerHTML = html;
}

function renderOtherDebug(){
  const rows = DATA.other_debug_table || [];
  document.getElementById('otherDebugBody').innerHTML = rows.map(r => `
    <tr class="border-b border-slate-200 last:border-b-0 hover:bg-slate-50/70">
      <td class="py-3 font-black">${r.brand}</td>
      <td class="py-3 font-bold">${r.gender}</td>
      <td class="py-3 max-w-[320px]">${r.name}</td>
      <td class="py-3">${r.item_category}</td>
      <td class="py-3">${r.dominant_attribute}</td>
      <td class="py-3">${r.reason}</td>
      <td class="py-3 text-slate-500">${r.tokens}</td>
    </tr>`).join('');
}

function renderKeywords(){
  const grid = document.getElementById('keywordGrid');
  grid.innerHTML = (DATA.keywords || []).map((block, idx) => `
    <div class="mention-card" style="animation-delay:${idx * 70}ms">
      <div class="brand-section-title">
        <span class="brand-dot"></span>
        <div class="text-lg font-black text-slate-900">${block.brand}</div>
      </div>
      <div class="flex flex-wrap gap-2">
        ${(block.items || []).map(it => '<span class="tag pill-dark">' + it.keyword + ' - ' + it.count + '</span>').join('')}
      </div>
    </div>`).join('');
}

function renderProducts(){
  const items = DATA.products || [];
  document.getElementById('productCountText').textContent = `품절 제외 ${formatNumber(items.length)}개`;

  const root = document.getElementById('productGroupedFeed');
  const grouped = {};
  items.forEach(p => {
    if (!grouped[p.brand]) grouped[p.brand] = {};
    if (!grouped[p.brand][p.item_category]) grouped[p.brand][p.item_category] = [];
    grouped[p.brand][p.item_category].push(p);
  });

  let html = '';
  Object.keys(grouped).sort().forEach((brand, bIdx) => {
    html += `<div class="product-grid-group">
      <div class="brand-section-title">
        <span class="brand-dot"></span>
        <div class="text-2xl font-black tracking-[-0.04em]">${brand}</div>
      </div>`;
    Object.keys(grouped[brand]).sort().forEach(cat => {
      const bucket = grouped[brand][cat];
      html += `<div class="mt-6">
        <div class="flex items-center justify-between gap-3">
          <div class="text-lg font-black tracking-[-0.03em]">${cat}</div>
          <div class="text-sm font-bold text-slate-500">${formatNumber(bucket.length)} styles</div>
        </div>
        <div class="mt-3 grid grid-cols-1 lg:grid-cols-2 2xl:grid-cols-3 gap-4">`;
      bucket.forEach((p, idx) => {
        html += `<article class="mention-card">
          <div class="flex gap-4">
            <div class="h-32 w-32 shrink-0 overflow-hidden rounded-2xl bg-slate-100 border border-slate-200">
              ${p.image_url ? '<img src="' + p.image_url + '" alt="' + p.name + '" class="h-full w-full object-cover" loading="lazy" />' : '<div class="flex h-full items-center justify-center text-xs font-black text-slate-400">NO IMAGE</div>'}
            </div>
            <div class="min-w-0 flex-1">
              <div class="flex flex-wrap gap-2">
                <span class="tag pill-dark">${p.brand}</span>
                <span class="tag pill-blue">${p.gender}</span>
                <span class="tag pill-soft">${p.item_category}</span>
                <span class="tag pill-violet">${p.grade}</span>
                <span class="tag pill-emerald">${p.shell_type}</span>
              </div>
              <div class="mt-3 line-clamp-2 text-lg font-black leading-7 text-slate-900">${p.name}</div>
              <div class="mt-2 flex flex-wrap items-end gap-3">
                <div class="text-2xl font-black">${formatPrice(p.current_price)}</div>
                ${p.original_price && p.original_price != p.current_price ? '<div class="text-sm font-bold text-slate-400 line-through">' + formatPrice(p.original_price) + '</div>' : ''}
                ${p.discount_rate ? '<div class="text-sm font-black text-rose-600">-' + p.discount_rate + '%</div>' : ''}
              </div>
              <div class="mt-3 flex flex-wrap gap-2">
                ${String(p.standard_attributes||'').split(',').map(x => x.trim()).filter(Boolean).slice(0,6).map(x => '<span class="tag pill-soft">' + x + '</span>').join('')}
              </div>
              <div class="mt-3 text-sm font-bold leading-6 text-slate-500 line-clamp-3">${p.description || '설명 없음'}</div>
              <div class="mt-4 flex items-center justify-between gap-3">
                <div class="text-xs font-black text-slate-400">${p.season} - ${p.price_band}</div>
                ${p.product_url ? '<a href="' + p.product_url + '" target="_blank" rel="noopener noreferrer" class="rounded-2xl bg-slate-900 px-4 py-2 text-xs font-black text-white">상품 보기</a>' : ''}
              </div>
            </div>
          </div>
        </article>`;
      });
      html += `</div></div>`;
    });
    html += `<div class="divider"></div></div>`;
  });
  root.innerHTML = html;
}

function baseChart(id, type, labels, values, extra={}){
  const ctx = document.getElementById(id);
  const many = labels && labels.length >= 7;
  return new Chart(ctx, {
    type,
    data: {
      labels,
      datasets: [{
        data: values,
        borderWidth: 0,
        borderRadius: 12,
        maxBarThickness: many ? 26 : 42,
      }]
    },
    options: Object.assign({
      indexAxis: many && type === 'bar' ? 'y' : 'x',
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        y: { beginAtZero: true, ticks: { precision: 0, font: { weight: '700' } } },
        x: { ticks: { autoSkip: false, font: { weight: '700' } } }
      },
      animation: { duration: 900, easing: 'easeOutQuart' }
    }, extra)
  });
}

function renderCharts(){
  const c = DATA.charts || {};
  baseChart('brandCountChart', 'bar', c.brandProductCounts?.labels || [], c.brandProductCounts?.values || []);
  baseChart('avgPriceChart', 'bar', c.brandAvgPrice?.labels || [], c.brandAvgPrice?.values || []);
  baseChart('genderSplitChart', 'doughnut', c.genderSplit?.labels || [], c.genderSplit?.values || [], { scales: {} });
  baseChart('priceBandChart', 'bar', c.priceBand?.labels || [], c.priceBand?.values || []);
  baseChart('categoryChart', 'bar', c.itemCategory?.labels || [], c.itemCategory?.values || []);
  baseChart('dominantAttrChart', 'bar', c.dominantAttribute?.labels || [], c.dominantAttribute?.values || []);

  const pctx = document.getElementById('positionChart');
  new Chart(pctx, {
    type: 'bubble',
    data: {
      datasets: (c.positioning || []).map(item => ({
        label: item.brand,
        data: [{ x: item.x, y: item.y, r: Math.max(10, Math.min(28, item.size / 3)) }],
      }))
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { position: 'bottom', labels: { boxWidth: 12, usePointStyle: true, font: { weight: '700' } } },
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const item = c.positioning[ctx.datasetIndex];
              return `${item.brand} - Avg ${formatPrice(item.avg_price)} - SKU ${item.size}`;
            }
          }
        }
      },
      scales: {
        x: { min: .5, max: 3.5, ticks: { stepSize: 1, callback: (v) => ({1:'Lifestyle',2:'Performance',3:'Extreme'}[v] || '') } },
        y: { min: .5, max: 3.5, ticks: { stepSize: 1, callback: (v) => ({1:'Mass',2:'Premium',3:'Luxury'}[v] || '') } }
      },
      animation: { duration: 1100, easing: 'easeOutQuart' }
    }
  });
}

createKpis();
renderBrandSummary();
renderPriceBandGenderTable();
renderAttributeGenderTable();
renderItemStyleTable();
renderKeywords();
renderOtherDebug();
renderProducts();
renderCharts();
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
