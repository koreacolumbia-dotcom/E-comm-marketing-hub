#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Competitor Outdoor Product Intelligence Dashboard
Single-file production-oriented crawler + analyzer + HTML dashboard.

What this script does
- Crawls competitor product listing/detail pages with Selenium
- Normalizes product data
- Classifies category / attributes / grade / shell type / price band / positioning
- Builds a visualization-heavy HTML dashboard inline (no separate template file)
- Exports CSV + JSON + HTML outputs

How to use
1) Install deps:
   pip install selenium pandas webdriver-manager
2) Update SITE_CONFIGS roots / extra_start_urls if you want to narrow or expand the crawl.
3) Run:
   python competitor_outdoor_dashboard_final.py

Outputs
- reports/competitor_intel/raw_products.csv
- reports/competitor_intel/analyzed_products.csv
- reports/competitor_intel/brand_summary.csv
- reports/competitor_intel/keyword_discovery.csv
- reports/competitor_intel/dashboard_data.json
- reports/competitor_intel/dashboard.html
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
from typing import Dict, List, Optional, Tuple, Iterable
from collections import Counter, defaultdict
from urllib.parse import urljoin, urlparse

import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager

# ============================================================
# 0. CONFIG
# ============================================================
OUT_DIR = os.path.join("reports", "competitor_intel")
HEADLESS = os.getenv("HEADLESS", "1").strip().lower() not in {"0", "false", "no"}
MAX_PRODUCTS_PER_SITE = int(os.getenv("MAX_PRODUCTS_PER_SITE", "300"))
SCROLL_PAUSE_SEC = float(os.getenv("SCROLL_PAUSE_SEC", "1.0"))
DEFAULT_WAIT_SEC = int(os.getenv("DEFAULT_WAIT_SEC", "15"))
SCREENSHOT_ON_ERROR = os.getenv("SCREENSHOT_ON_ERROR", "1").strip().lower() in {"1", "true", "yes"}
TODAY_STR = datetime.now().strftime("%Y-%m-%d %H:%M")



@dataclass
class SiteConfig:
    brand: str
    start_url: str
    currency: str = "KRW"
    card_selectors: List[str] = field(default_factory=list)
    title_selectors: List[str] = field(default_factory=list)
    link_selectors: List[str] = field(default_factory=lambda: ["a"])
    price_selectors: List[str] = field(default_factory=list)
    original_price_selectors: List[str] = field(default_factory=list)
    image_selectors: List[str] = field(default_factory=lambda: ["img"])
    desc_selectors: List[str] = field(default_factory=list)
    soldout_selectors: List[str] = field(default_factory=list)
    gender_selectors: List[str] = field(default_factory=list)
    season_selectors: List[str] = field(default_factory=list)
    load_more_selectors: List[str] = field(default_factory=list)
    next_page_selectors: List[str] = field(default_factory=list)
    wait_selectors: List[str] = field(default_factory=list)
    detail_wait_selectors: List[str] = field(default_factory=list)
    click_into_detail: bool = True
    detail_name_selectors: List[str] = field(default_factory=list)
    detail_price_selectors: List[str] = field(default_factory=list)
    detail_original_price_selectors: List[str] = field(default_factory=list)
    detail_desc_selectors: List[str] = field(default_factory=list)
    detail_image_selectors: List[str] = field(default_factory=list)
    extra_start_urls: List[str] = field(default_factory=list)
    discovery_url_keywords: List[str] = field(default_factory=lambda: [
        "men", "women", "all", "shop", "product", "products", "category", "categories",
        "collection", "collections", "outer", "jacket", "jackets", "wind", "down",
        "pant", "pants", "bottom", "shoe", "shoes", "hiking", "trail", "apparel"
    ])
    discovery_block_keywords: List[str] = field(default_factory=lambda: [
        "login", "signin", "signup", "cart", "mypage", "customer", "notice", "event",
        "magazine", "journal", "story", "store", "stores", "dealer", "faq", "repair",
        "membership", "policy", "privacy", "terms", "youtube", "instagram", "facebook"
    ])
    brand_terms: List[str] = field(default_factory=list)
    max_discovered_urls: int = 24
    use_lazy_scroll: bool = True
    max_products: int = MAX_PRODUCTS_PER_SITE
    note: str = ""


COMMON_CARD_SELECTORS = [
    ".product-item", ".prd-item", ".item-list li", ".goods_list li", ".product_list li",
    ".product-card", ".product-list .item", ".product_list .item", ".grid-product__content",
    ".productgrid--item", ".card-product", ".product"
]
COMMON_TITLE_SELECTORS = [
    ".product-name", ".product_name", ".name", ".tit", ".info .name", ".item-name", ".goods_name",
    ".prd_name", "h3", "h4"
]
COMMON_LINK_SELECTORS = [
    "a[href*='product']", "a[href*='/products/']", "a[href*='/product/']", "a[href*='/goods/']",
    "a[href*='/collections/']", "a[href*='/shop/']", "a[href]"
]
COMMON_PRICE_SELECTORS = [
    ".price-current", ".price .sale", ".price .current", ".price", ".sell_price", ".product-price",
    ".sale_price", ".current-price", ".sales", ".money"
]
COMMON_ORIGINAL_PRICE_SELECTORS = [
    ".price-original", ".price .consumer", ".price .origin", ".normal_price", ".list_price",
    ".consumer", ".origin_price", ".compare-at-price", ".was-price"
]
COMMON_IMAGE_SELECTORS = [
    "img[data-src]", "img[data-original]", "img[src]"
]
COMMON_DESC_SELECTORS = [
    ".product-desc", ".desc", ".summary", ".product-summary", ".goods_desc", ".txtBox", ".info-text", ".txt"
]
COMMON_SOLDOUT_SELECTORS = [
    ".soldout", ".sold-out", ".badge-soldout", ".state.soldout", ".product-badge.soldout"
]
COMMON_GENDER_SELECTORS = [".badge-gender", ".gender", ".prd_gender"]
COMMON_SEASON_SELECTORS = [".badge-season", ".season", ".prd_season"]
COMMON_WAIT_SELECTORS = [
    ".product-list", ".prd_list", ".item-list", ".goods_list", ".productgrid", "main", "body"
]
COMMON_DETAIL_WAIT_SELECTORS = [".product-detail", ".goods-detail", ".prd-detail", ".productView", "body"]
COMMON_DETAIL_NAME_SELECTORS = [
    "h1.product-name", "h2.product-name", ".product-name", ".goods_name", ".prd_name", "h1", "h2"
]
COMMON_DETAIL_IMAGE_SELECTORS = [
    ".product-visual img", ".swiper-slide img", ".goods_img img", ".prd_visual img", ".gallery img", "img[src]"
]


SITE_CONFIGS: List[SiteConfig] = [
    SiteConfig(
        brand="COLUMBIA",
        start_url="https://www.columbiakorea.co.kr/",
        card_selectors=COMMON_CARD_SELECTORS,
        title_selectors=COMMON_TITLE_SELECTORS,
        link_selectors=COMMON_LINK_SELECTORS,
        price_selectors=COMMON_PRICE_SELECTORS,
        original_price_selectors=COMMON_ORIGINAL_PRICE_SELECTORS,
        image_selectors=COMMON_IMAGE_SELECTORS,
        desc_selectors=COMMON_DESC_SELECTORS,
        soldout_selectors=COMMON_SOLDOUT_SELECTORS,
        gender_selectors=COMMON_GENDER_SELECTORS,
        season_selectors=COMMON_SEASON_SELECTORS,
        wait_selectors=COMMON_WAIT_SELECTORS,
        detail_wait_selectors=COMMON_DETAIL_WAIT_SELECTORS,
        detail_name_selectors=COMMON_DETAIL_NAME_SELECTORS,
        detail_price_selectors=COMMON_PRICE_SELECTORS,
        detail_original_price_selectors=COMMON_ORIGINAL_PRICE_SELECTORS,
        detail_desc_selectors=COMMON_DESC_SELECTORS,
        detail_image_selectors=COMMON_DETAIL_IMAGE_SELECTORS,
        extra_start_urls=["https://www.columbiakorea.co.kr/"],
        note="Own-brand benchmark optional.",
    ),
    SiteConfig(
        brand="THE_NORTH_FACE",
        start_url="https://www.thenorthfacekorea.co.kr/",
        card_selectors=COMMON_CARD_SELECTORS,
        title_selectors=COMMON_TITLE_SELECTORS,
        link_selectors=COMMON_LINK_SELECTORS,
        price_selectors=COMMON_PRICE_SELECTORS,
        original_price_selectors=COMMON_ORIGINAL_PRICE_SELECTORS,
        image_selectors=COMMON_IMAGE_SELECTORS,
        desc_selectors=COMMON_DESC_SELECTORS,
        soldout_selectors=COMMON_SOLDOUT_SELECTORS,
        gender_selectors=COMMON_GENDER_SELECTORS,
        season_selectors=COMMON_SEASON_SELECTORS,
        wait_selectors=COMMON_WAIT_SELECTORS,
        detail_wait_selectors=COMMON_DETAIL_WAIT_SELECTORS,
        detail_name_selectors=COMMON_DETAIL_NAME_SELECTORS,
        detail_price_selectors=COMMON_PRICE_SELECTORS,
        detail_original_price_selectors=COMMON_ORIGINAL_PRICE_SELECTORS,
        detail_desc_selectors=COMMON_DESC_SELECTORS,
        detail_image_selectors=COMMON_DETAIL_IMAGE_SELECTORS,
    ),
    SiteConfig(
        brand="DISCOVERY_EXPEDITION",
        start_url="https://www.discovery-expedition.com/",
        card_selectors=COMMON_CARD_SELECTORS,
        title_selectors=COMMON_TITLE_SELECTORS,
        link_selectors=COMMON_LINK_SELECTORS,
        price_selectors=COMMON_PRICE_SELECTORS,
        original_price_selectors=COMMON_ORIGINAL_PRICE_SELECTORS,
        image_selectors=COMMON_IMAGE_SELECTORS,
        desc_selectors=COMMON_DESC_SELECTORS,
        soldout_selectors=COMMON_SOLDOUT_SELECTORS,
        gender_selectors=COMMON_GENDER_SELECTORS,
        season_selectors=COMMON_SEASON_SELECTORS,
        wait_selectors=COMMON_WAIT_SELECTORS,
        detail_wait_selectors=COMMON_DETAIL_WAIT_SELECTORS,
        detail_name_selectors=COMMON_DETAIL_NAME_SELECTORS,
        detail_price_selectors=COMMON_PRICE_SELECTORS,
        detail_original_price_selectors=COMMON_ORIGINAL_PRICE_SELECTORS,
        detail_desc_selectors=COMMON_DESC_SELECTORS,
        detail_image_selectors=COMMON_DETAIL_IMAGE_SELECTORS,
    ),
    SiteConfig(
        brand="NATIONAL_GEOGRAPHIC",
        start_url="https://www.nstationmall.com/",
        extra_start_urls=["https://www.nstationmall.com/"],
        card_selectors=COMMON_CARD_SELECTORS,
        title_selectors=COMMON_TITLE_SELECTORS,
        link_selectors=COMMON_LINK_SELECTORS,
        price_selectors=COMMON_PRICE_SELECTORS,
        original_price_selectors=COMMON_ORIGINAL_PRICE_SELECTORS,
        image_selectors=COMMON_IMAGE_SELECTORS,
        desc_selectors=COMMON_DESC_SELECTORS,
        soldout_selectors=COMMON_SOLDOUT_SELECTORS,
        gender_selectors=COMMON_GENDER_SELECTORS,
        season_selectors=COMMON_SEASON_SELECTORS,
        wait_selectors=COMMON_WAIT_SELECTORS,
        detail_wait_selectors=COMMON_DETAIL_WAIT_SELECTORS,
        detail_name_selectors=COMMON_DETAIL_NAME_SELECTORS,
        detail_price_selectors=COMMON_PRICE_SELECTORS,
        detail_original_price_selectors=COMMON_ORIGINAL_PRICE_SELECTORS,
        detail_desc_selectors=COMMON_DESC_SELECTORS,
        detail_image_selectors=COMMON_DETAIL_IMAGE_SELECTORS,
    ),
    SiteConfig(
        brand="KOLON_SPORT",
        start_url="https://www.kolonsport.com/",
        card_selectors=COMMON_CARD_SELECTORS,
        title_selectors=COMMON_TITLE_SELECTORS,
        link_selectors=COMMON_LINK_SELECTORS,
        price_selectors=COMMON_PRICE_SELECTORS,
        original_price_selectors=COMMON_ORIGINAL_PRICE_SELECTORS,
        image_selectors=COMMON_IMAGE_SELECTORS,
        desc_selectors=COMMON_DESC_SELECTORS,
        soldout_selectors=COMMON_SOLDOUT_SELECTORS,
        gender_selectors=COMMON_GENDER_SELECTORS,
        season_selectors=COMMON_SEASON_SELECTORS,
        wait_selectors=COMMON_WAIT_SELECTORS,
        detail_wait_selectors=COMMON_DETAIL_WAIT_SELECTORS,
        detail_name_selectors=COMMON_DETAIL_NAME_SELECTORS,
        detail_price_selectors=COMMON_PRICE_SELECTORS,
        detail_original_price_selectors=COMMON_ORIGINAL_PRICE_SELECTORS,
        detail_desc_selectors=COMMON_DESC_SELECTORS,
        detail_image_selectors=COMMON_DETAIL_IMAGE_SELECTORS,
    ),
    SiteConfig(
        brand="SNOW_PEAK",
        start_url="https://www.snowpeakstore.co.kr/",
        extra_start_urls=["https://www.snowpeak.co.kr/"],
        card_selectors=COMMON_CARD_SELECTORS,
        title_selectors=COMMON_TITLE_SELECTORS,
        link_selectors=COMMON_LINK_SELECTORS,
        price_selectors=COMMON_PRICE_SELECTORS,
        original_price_selectors=COMMON_ORIGINAL_PRICE_SELECTORS,
        image_selectors=COMMON_IMAGE_SELECTORS,
        desc_selectors=COMMON_DESC_SELECTORS,
        soldout_selectors=COMMON_SOLDOUT_SELECTORS,
        gender_selectors=COMMON_GENDER_SELECTORS,
        season_selectors=COMMON_SEASON_SELECTORS,
        wait_selectors=COMMON_WAIT_SELECTORS,
        detail_wait_selectors=COMMON_DETAIL_WAIT_SELECTORS,
        detail_name_selectors=COMMON_DETAIL_NAME_SELECTORS,
        detail_price_selectors=COMMON_PRICE_SELECTORS,
        detail_original_price_selectors=COMMON_ORIGINAL_PRICE_SELECTORS,
        detail_desc_selectors=COMMON_DESC_SELECTORS,
        detail_image_selectors=COMMON_DETAIL_IMAGE_SELECTORS,
    ),
    SiteConfig(
        brand="PATAGONIA",
        start_url="https://www.patagonia.co.kr/",
        card_selectors=COMMON_CARD_SELECTORS,
        title_selectors=COMMON_TITLE_SELECTORS,
        link_selectors=COMMON_LINK_SELECTORS,
        price_selectors=COMMON_PRICE_SELECTORS,
        original_price_selectors=COMMON_ORIGINAL_PRICE_SELECTORS,
        image_selectors=COMMON_IMAGE_SELECTORS,
        desc_selectors=COMMON_DESC_SELECTORS,
        soldout_selectors=COMMON_SOLDOUT_SELECTORS,
        gender_selectors=COMMON_GENDER_SELECTORS,
        season_selectors=COMMON_SEASON_SELECTORS,
        wait_selectors=COMMON_WAIT_SELECTORS,
        detail_wait_selectors=COMMON_DETAIL_WAIT_SELECTORS,
        detail_name_selectors=COMMON_DETAIL_NAME_SELECTORS,
        detail_price_selectors=COMMON_PRICE_SELECTORS,
        detail_original_price_selectors=COMMON_ORIGINAL_PRICE_SELECTORS,
        detail_desc_selectors=COMMON_DESC_SELECTORS,
        detail_image_selectors=COMMON_DETAIL_IMAGE_SELECTORS,
    ),
    SiteConfig(
        brand="K2",
        start_url="https://www.k-village.co.kr/",
        extra_start_urls=["https://www.k-village.co.kr/"],
        card_selectors=COMMON_CARD_SELECTORS,
        title_selectors=COMMON_TITLE_SELECTORS,
        link_selectors=COMMON_LINK_SELECTORS,
        price_selectors=COMMON_PRICE_SELECTORS,
        original_price_selectors=COMMON_ORIGINAL_PRICE_SELECTORS,
        image_selectors=COMMON_IMAGE_SELECTORS,
        desc_selectors=COMMON_DESC_SELECTORS,
        soldout_selectors=COMMON_SOLDOUT_SELECTORS,
        gender_selectors=COMMON_GENDER_SELECTORS,
        season_selectors=COMMON_SEASON_SELECTORS,
        wait_selectors=COMMON_WAIT_SELECTORS,
        detail_wait_selectors=COMMON_DETAIL_WAIT_SELECTORS,
        detail_name_selectors=COMMON_DETAIL_NAME_SELECTORS,
        detail_price_selectors=COMMON_PRICE_SELECTORS,
        detail_original_price_selectors=COMMON_ORIGINAL_PRICE_SELECTORS,
        detail_desc_selectors=COMMON_DESC_SELECTORS,
        detail_image_selectors=COMMON_DETAIL_IMAGE_SELECTORS,
        brand_terms=["K2", "케이투"],
        discovery_url_keywords=["k2", "men", "women", "all", "outdoor", "brand", "category", "product", "shop", "jacket", "pants", "shoes"],
    ),
    SiteConfig(
        brand="BLACKYAK",
        start_url="https://global.blackyak.com/en/collections/men",
        extra_start_urls=[
            "https://global.blackyak.com/en/collections/men",
            "https://global.blackyak.com/en/collections/hosen-men"
        ],
        card_selectors=COMMON_CARD_SELECTORS + [".grid__item", ".card-wrapper"],
        title_selectors=COMMON_TITLE_SELECTORS,
        link_selectors=["a[href*='/products/']", "a[href]"],
        price_selectors=COMMON_PRICE_SELECTORS,
        original_price_selectors=COMMON_ORIGINAL_PRICE_SELECTORS,
        image_selectors=COMMON_IMAGE_SELECTORS,
        desc_selectors=COMMON_DESC_SELECTORS,
        soldout_selectors=COMMON_SOLDOUT_SELECTORS,
        gender_selectors=COMMON_GENDER_SELECTORS,
        season_selectors=COMMON_SEASON_SELECTORS,
        wait_selectors=COMMON_WAIT_SELECTORS + [".collection", ".grid"],
        detail_wait_selectors=COMMON_DETAIL_WAIT_SELECTORS,
        detail_name_selectors=COMMON_DETAIL_NAME_SELECTORS,
        detail_price_selectors=COMMON_PRICE_SELECTORS,
        detail_original_price_selectors=COMMON_ORIGINAL_PRICE_SELECTORS,
        detail_desc_selectors=COMMON_DESC_SELECTORS,
        detail_image_selectors=COMMON_DETAIL_IMAGE_SELECTORS,
        discovery_url_keywords=["collections", "men", "women", "pants", "jackets", "fleece", "accessories", "show all", "shell"],
    ),
    SiteConfig(
        brand="NEPA",
        start_url="https://www.nplus.co.kr/",
        extra_start_urls=["https://www.nplus.co.kr/main/main.asp"],
        card_selectors=COMMON_CARD_SELECTORS,
        title_selectors=COMMON_TITLE_SELECTORS,
        link_selectors=COMMON_LINK_SELECTORS,
        price_selectors=COMMON_PRICE_SELECTORS,
        original_price_selectors=COMMON_ORIGINAL_PRICE_SELECTORS,
        image_selectors=COMMON_IMAGE_SELECTORS,
        desc_selectors=COMMON_DESC_SELECTORS,
        soldout_selectors=COMMON_SOLDOUT_SELECTORS,
        gender_selectors=COMMON_GENDER_SELECTORS,
        season_selectors=COMMON_SEASON_SELECTORS,
        wait_selectors=COMMON_WAIT_SELECTORS,
        detail_wait_selectors=COMMON_DETAIL_WAIT_SELECTORS,
        detail_name_selectors=COMMON_DETAIL_NAME_SELECTORS,
        detail_price_selectors=COMMON_PRICE_SELECTORS,
        detail_original_price_selectors=COMMON_ORIGINAL_PRICE_SELECTORS,
        detail_desc_selectors=COMMON_DESC_SELECTORS,
        detail_image_selectors=COMMON_DETAIL_IMAGE_SELECTORS,
    ),
    SiteConfig(
        brand="EIDER",
        start_url="https://www.k-village.co.kr/",
        extra_start_urls=["https://www.k-village.co.kr/"],
        card_selectors=COMMON_CARD_SELECTORS,
        title_selectors=COMMON_TITLE_SELECTORS,
        link_selectors=COMMON_LINK_SELECTORS,
        price_selectors=COMMON_PRICE_SELECTORS,
        original_price_selectors=COMMON_ORIGINAL_PRICE_SELECTORS,
        image_selectors=COMMON_IMAGE_SELECTORS,
        desc_selectors=COMMON_DESC_SELECTORS,
        soldout_selectors=COMMON_SOLDOUT_SELECTORS,
        gender_selectors=COMMON_GENDER_SELECTORS,
        season_selectors=COMMON_SEASON_SELECTORS,
        wait_selectors=COMMON_WAIT_SELECTORS,
        detail_wait_selectors=COMMON_DETAIL_WAIT_SELECTORS,
        detail_name_selectors=COMMON_DETAIL_NAME_SELECTORS,
        detail_price_selectors=COMMON_PRICE_SELECTORS,
        detail_original_price_selectors=COMMON_ORIGINAL_PRICE_SELECTORS,
        detail_desc_selectors=COMMON_DESC_SELECTORS,
        detail_image_selectors=COMMON_DETAIL_IMAGE_SELECTORS,
        brand_terms=["EIDER", "아이더"],
        discovery_url_keywords=["eider", "men", "women", "all", "outdoor", "brand", "category", "product", "shop", "jacket", "pants", "shoes"],
    ),
    SiteConfig(
        brand="MILLET",
        start_url="https://www.millet.co.kr/",
        card_selectors=COMMON_CARD_SELECTORS,
        title_selectors=COMMON_TITLE_SELECTORS,
        link_selectors=COMMON_LINK_SELECTORS,
        price_selectors=COMMON_PRICE_SELECTORS,
        original_price_selectors=COMMON_ORIGINAL_PRICE_SELECTORS,
        image_selectors=COMMON_IMAGE_SELECTORS,
        desc_selectors=COMMON_DESC_SELECTORS,
        soldout_selectors=COMMON_SOLDOUT_SELECTORS,
        gender_selectors=COMMON_GENDER_SELECTORS,
        season_selectors=COMMON_SEASON_SELECTORS,
        wait_selectors=COMMON_WAIT_SELECTORS,
        detail_wait_selectors=COMMON_DETAIL_WAIT_SELECTORS,
        detail_name_selectors=COMMON_DETAIL_NAME_SELECTORS,
        detail_price_selectors=COMMON_PRICE_SELECTORS,
        detail_original_price_selectors=COMMON_ORIGINAL_PRICE_SELECTORS,
        detail_desc_selectors=COMMON_DESC_SELECTORS,
        detail_image_selectors=COMMON_DETAIL_IMAGE_SELECTORS,
    ),
    SiteConfig(
        brand="HOKA",
        start_url="https://www.hoka.com/",
        card_selectors=COMMON_CARD_SELECTORS + [".product-tile", ".plp-grid-product"],
        title_selectors=COMMON_TITLE_SELECTORS + [".product-tile__title"],
        link_selectors=["a[href*='/en/']", "a[href*='/products/']", "a[href]"],
        price_selectors=COMMON_PRICE_SELECTORS + [".price-sales", ".value"],
        original_price_selectors=COMMON_ORIGINAL_PRICE_SELECTORS + [".price-standard"],
        image_selectors=COMMON_IMAGE_SELECTORS,
        desc_selectors=COMMON_DESC_SELECTORS,
        soldout_selectors=COMMON_SOLDOUT_SELECTORS,
        gender_selectors=COMMON_GENDER_SELECTORS,
        season_selectors=COMMON_SEASON_SELECTORS,
        wait_selectors=COMMON_WAIT_SELECTORS + [".product-grid", ".plp-results"],
        detail_wait_selectors=COMMON_DETAIL_WAIT_SELECTORS,
        detail_name_selectors=COMMON_DETAIL_NAME_SELECTORS,
        detail_price_selectors=COMMON_PRICE_SELECTORS + [".price-sales", ".value"],
        detail_original_price_selectors=COMMON_ORIGINAL_PRICE_SELECTORS + [".price-standard"],
        detail_desc_selectors=COMMON_DESC_SELECTORS,
        detail_image_selectors=COMMON_DETAIL_IMAGE_SELECTORS,
        discovery_url_keywords=["men", "women", "all", "apparel", "hiking", "running", "lifestyle", "products", "shop"],
        note="KR official e-commerce storefront was not reliably confirmed in search results; global official root used as fallback.",
    ),
    SiteConfig(
        brand="ARCTERYX",
        start_url="https://arcteryx.com/",
        card_selectors=COMMON_CARD_SELECTORS + [".product-card", ".plp-product-card"],
        title_selectors=COMMON_TITLE_SELECTORS + [".product-card__title"],
        link_selectors=["a[href*='/shop/']", "a[href*='/products/']", "a[href]"],
        price_selectors=COMMON_PRICE_SELECTORS + [".price-value", ".price__sales"],
        original_price_selectors=COMMON_ORIGINAL_PRICE_SELECTORS + [".price__standard"],
        image_selectors=COMMON_IMAGE_SELECTORS,
        desc_selectors=COMMON_DESC_SELECTORS,
        soldout_selectors=COMMON_SOLDOUT_SELECTORS,
        gender_selectors=COMMON_GENDER_SELECTORS,
        season_selectors=COMMON_SEASON_SELECTORS,
        wait_selectors=COMMON_WAIT_SELECTORS + [".plp-grid", ".product-grid"],
        detail_wait_selectors=COMMON_DETAIL_WAIT_SELECTORS,
        detail_name_selectors=COMMON_DETAIL_NAME_SELECTORS,
        detail_price_selectors=COMMON_PRICE_SELECTORS + [".price-value", ".price__sales"],
        detail_original_price_selectors=COMMON_ORIGINAL_PRICE_SELECTORS + [".price__standard"],
        detail_desc_selectors=COMMON_DESC_SELECTORS,
        detail_image_selectors=COMMON_DETAIL_IMAGE_SELECTORS,
        discovery_url_keywords=["men", "women", "all", "jackets", "pants", "footwear", "packs", "shop"],
        note="Global official storefront used as fallback.",
    ),
    SiteConfig(
        brand="SALOMON",
        start_url="https://salomon.co.kr/",
        card_selectors=COMMON_CARD_SELECTORS,
        title_selectors=COMMON_TITLE_SELECTORS,
        link_selectors=COMMON_LINK_SELECTORS,
        price_selectors=COMMON_PRICE_SELECTORS,
        original_price_selectors=COMMON_ORIGINAL_PRICE_SELECTORS,
        image_selectors=COMMON_IMAGE_SELECTORS,
        desc_selectors=COMMON_DESC_SELECTORS,
        soldout_selectors=COMMON_SOLDOUT_SELECTORS,
        gender_selectors=COMMON_GENDER_SELECTORS,
        season_selectors=COMMON_SEASON_SELECTORS,
        wait_selectors=COMMON_WAIT_SELECTORS,
        detail_wait_selectors=COMMON_DETAIL_WAIT_SELECTORS,
        detail_name_selectors=COMMON_DETAIL_NAME_SELECTORS,
        detail_price_selectors=COMMON_PRICE_SELECTORS,
        detail_original_price_selectors=COMMON_ORIGINAL_PRICE_SELECTORS,
        detail_desc_selectors=COMMON_DESC_SELECTORS,
        detail_image_selectors=COMMON_DETAIL_IMAGE_SELECTORS,
        discovery_url_keywords=["men", "women", "hiking", "running", "trail", "products", "shop", "shoes", "clothing", "packs"],
    ),
]

# ============================================================
# 1. DATA MODELS
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
# 2. SHARED UTILS
# ============================================================
def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def safe_text(value: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def compact_text(value: Optional[str]) -> str:
    return safe_text(value).replace("\xa0", " ")


def parse_price_to_int(text: str) -> Optional[int]:
    cleaned = re.sub(r"[^0-9]", "", text or "")
    return int(cleaned) if cleaned else None


def calc_discount_rate(current_price: Optional[int], original_price: Optional[int]) -> Optional[float]:
    if current_price is None or original_price is None or original_price <= 0:
        return None
    if current_price > original_price:
        return 0.0
    return round((1 - (current_price / original_price)) * 100, 1)


def try_find_text(root, css: str) -> str:
    try:
        return compact_text(root.find_element(By.CSS_SELECTOR, css).text) if css else ""
    except Exception:
        return ""


def try_find_text_multi(root, selectors: List[str]) -> str:
    for css in selectors or []:
        val = try_find_text(root, css)
        if val:
            return val
    return ""


def try_find_attr(root, css: str, attr: str) -> str:
    try:
        return compact_text(root.find_element(By.CSS_SELECTOR, css).get_attribute(attr)) if css else ""
    except Exception:
        return ""


def try_find_attr_multi(root, selectors: List[str], attr: str) -> str:
    for css in selectors or []:
        val = try_find_attr(root, css, attr)
        if val:
            return val
    return ""


def first_nonempty(*values: str) -> str:
    for v in values:
        if safe_text(v):
            return safe_text(v)
    return ""


def slugify(text: str) -> str:
    text = safe_text(text).lower()
    text = re.sub(r"[^a-z0-9가-힣]+", "-", text)
    return text.strip("-") or "na"


def unique_preserve_order(items: Iterable[str]) -> List[str]:
    seen, out = set(), []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def json_dumps(obj) -> str:
    return json.dumps(obj, ensure_ascii=False)

# ============================================================
# 3. OUTDOOR PRODUCT ENGINE
# ============================================================
BRAND_TERM_MAP: Dict[str, str] = {
    "DRYVENT": "방수", "FUTURELIGHT": "방수", "OMNI-TECH": "방수", "OMNI TECH": "방수",
    "WINDWALL": "방풍", "WINDSTOPPER": "방풍", "GORE-TEX": "고어텍스", "GORE TEX": "고어텍스",
    "GORE-TEX PRO": "고어텍스_PRO", "GORE TEX PRO": "고어텍스_PRO", "POLARTEC": "폴라텍",
    "CORDURA": "코듀라", "DOWN": "다운", "덕다운": "다운", "구스다운": "다운",
    "2L": "2L", "2.5L": "2.5L", "3L": "3L", "방수": "방수", "발수": "방수",
    "방풍": "방풍", "투습": "투습", "경량": "경량", "스트레치": "스트레치", "신축": "스트레치",
    "보온": "보온", "인슐레이션": "인슐레이션", "플리스": "보온", "심실링": "방수", "쉘": "구조",
}

ATTRIBUTE_PRIORITY: Dict[str, int] = {
    "고어텍스_PRO": 100, "고어텍스": 95, "폴라텍": 90, "코듀라": 85,
    "3L": 80, "2.5L": 75, "2L": 70, "다운": 65, "방수": 60,
    "보온": 50, "방풍": 40, "투습": 35, "스트레치": 30, "경량": 25,
    "인슐레이션": 20, "구조": 10,
}

CONFLICT_RULES = [
    ("고어텍스", "방수", "고어텍스"), ("고어텍스_PRO", "방수", "고어텍스_PRO"),
    ("3L", "2L", "3L"), ("3L", "2.5L", "3L"), ("2.5L", "2L", "2.5L"),
    ("방수", "방풍", "방수"), ("다운", "인슐레이션", "다운"),
]

CATEGORY_RULES: Dict[str, List[str]] = {
    "자켓": ["jacket", "jk", "자켓", "재킷", "아노락", "바람막이", "windbreaker", "shell"],
    "팬츠": ["pants", "pant", "trouser", "팬츠", "바지", "조거", "슬랙스", "쇼츠", "반바지"],
    "플리스": ["fleece", "플리스"],
    "다운": ["down", "덕다운", "구스다운", "패딩", "puffer"],
    "베스트": ["vest", "베스트"],
    "티셔츠": ["tee", "t-shirt", "t shirt", "티셔츠", "반팔", "긴팔", "sleeve"],
    "셔츠": ["shirt", "셔츠"],
    "슈즈": ["shoe", "shoes", "boot", "boots", "sneaker", "trail", "등산화", "신발", "부츠"],
    "백팩": ["backpack", "bag", "pack", "배낭", "백팩", "가방"],
    "캡": ["cap", "hat", "모자", "캡", "비니"],
}

NOISE_WORDS = {"the", "and", "with", "for", "from", "outdoor", "sports", "wear", "new", "best", "남성", "여성", "공용", "신상", "기본", "시즌", "정품", "단독", "라인", "기능성", "자켓", "팬츠", "셔츠", "신발", "모자", "가방", "상품", "제품", "스타일"}


def infer_gender(name: str, description: str, raw_gender: str) -> str:
    blob = f"{name} {description} {raw_gender}".lower()
    if any(x in blob for x in ["women", "womens", "여성", "우먼", "woman"]): return "여성"
    if any(x in blob for x in ["men", "mens", "남성", "맨즈", "man"]): return "남성"
    if any(x in blob for x in ["unisex", "공용"]): return "공용"
    return "미분류"


def infer_season(name: str, description: str, raw_season: str) -> str:
    blob = f"{name} {description} {raw_season}".lower()
    if any(x in blob for x in ["spring", "봄"]): return "봄"
    if any(x in blob for x in ["summer", "여름", "썸머"]): return "여름"
    if any(x in blob for x in ["fall", "autumn", "가을"]): return "가을"
    if any(x in blob for x in ["winter", "겨울"]): return "겨울"
    return safe_text(raw_season) or "미분류"


def infer_item_category(name: str, description: str) -> str:
    blob = f"{name} {description}".lower()
    for category, keywords in CATEGORY_RULES.items():
        if any(k.lower() in blob for k in keywords):
            return category
    return "기타"


def extract_raw_keywords(name: str, description: str) -> List[str]:
    text = f"{name} {description}"
    hits: List[str] = []
    for raw in BRAND_TERM_MAP.keys():
        if re.search(re.escape(raw), text, flags=re.IGNORECASE):
            hits.append(raw)
    for c in re.findall(r"\b[A-Z][A-Z0-9\-\.]{2,}\b|\b\d(?:\.\d)?L\b", text):
        if c.lower() not in {x.lower() for x in hits} and c.lower() not in NOISE_WORDS:
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
            if winner == left: attrs.discard(right)
            elif winner == right: attrs.discard(left)
    return sorted(attrs, key=lambda x: (-ATTRIBUTE_PRIORITY.get(x, 0), x))


def select_dominant_attribute(attributes: List[str]) -> str:
    return max(attributes, key=lambda x: ATTRIBUTE_PRIORITY.get(x, 0)) if attributes else "기타"


def classify_grade(attributes: List[str], dominant_attribute: str) -> str:
    attrs = set(attributes)
    if dominant_attribute == "고어텍스_PRO" or "3L" in attrs: return "High"
    if dominant_attribute == "고어텍스" or "2.5L" in attrs or "다운" in attrs: return "Mid"
    return "Entry"


def classify_shell_type(attributes: List[str], name: str, description: str) -> str:
    attrs = set(attributes)
    blob = f"{name} {description}".lower()
    hard_score = 0
    soft_score = 0
    if "방수" in attrs or "고어텍스" in attrs or "고어텍스_PRO" in attrs: hard_score += 3
    if "3L" in attrs or "2.5L" in attrs or "2L" in attrs: hard_score += 2
    if any(x in blob for x in ["심실링", "seamsealed", "seam-sealed", "shell"]): hard_score += 2
    if "방풍" in attrs: soft_score += 2
    if "스트레치" in attrs: soft_score += 2
    if any(x in blob for x in ["softshell", "soft shell", "활동성"]): soft_score += 2
    if hard_score > soft_score and hard_score >= 2: return "Hard Shell"
    if soft_score > hard_score and soft_score >= 2: return "Soft Shell"
    return "Unknown"


def classify_price_band(price: Optional[int]) -> str:
    if price is None: return "기타"
    if price < 100000: return "0-9.9만"
    if price < 200000: return "10-19.9만"
    if price < 300000: return "20-29.9만"
    if price < 500000: return "30-49.9만"
    return "50만+"


def classify_positioning_y(price: Optional[int]) -> str:
    if price is None or price < 150000: return "Mass"
    if price < 350000: return "Premium"
    return "Luxury"


def classify_positioning_x(attrs: List[str], dominant_attribute: str, shell_type: str) -> str:
    score = 0
    if dominant_attribute in {"고어텍스_PRO", "고어텍스", "폴라텍", "코듀라"}: score += 3
    if any(x in attrs for x in ["3L", "2.5L", "2L", "방수", "다운"]): score += 2
    if shell_type == "Hard Shell": score += 2
    if score >= 6: return "Extreme"
    if score >= 3: return "Performance"
    return "Lifestyle"


def analyze_product(raw: ProductRaw) -> ProductAnalyzed:
    current_price = parse_price_to_int(raw.price_text)
    original_price = parse_price_to_int(raw.original_price_text)
    if current_price is None and original_price is not None: current_price = original_price
    if original_price is None and current_price is not None: original_price = current_price
    raw_keywords = extract_raw_keywords(raw.name, raw.description)
    std_attrs = map_standard_attributes(raw_keywords, raw.name, raw.description)
    resolved_attrs = resolve_conflicts(std_attrs)
    dominant = select_dominant_attribute(resolved_attrs)
    grade = classify_grade(resolved_attrs, dominant)
    shell_type = classify_shell_type(resolved_attrs, raw.name, raw.description)
    price_band = classify_price_band(current_price)
    pos_y = classify_positioning_y(current_price)
    pos_x = classify_positioning_x(resolved_attrs, dominant, shell_type)
    return ProductAnalyzed(
        brand=raw.brand, source_url=raw.source_url, product_url=raw.product_url, name=raw.name, description=raw.description,
        image_url=raw.image_url, current_price=current_price, original_price=original_price,
        discount_rate=calc_discount_rate(current_price, original_price), sold_out=bool(safe_text(raw.sold_out_text)),
        gender=infer_gender(raw.name, raw.description, raw.gender_text), season=infer_season(raw.name, raw.description, raw.season_text),
        item_category=infer_item_category(raw.name, raw.description), raw_keywords=raw_keywords, standard_attributes=resolved_attrs,
        dominant_attribute=dominant, grade=grade, shell_type=shell_type, price_band=price_band,
        positioning_y=pos_y, positioning_x=pos_x, attribute_coverage_flag="OK", crawled_at=raw.crawled_at,
    )

# ============================================================
# 4. KEYWORD DISCOVERY
# ============================================================
def discover_keywords(products: List[ProductAnalyzed]) -> pd.DataFrame:
    rows = []
    brand_keyword_counter: Dict[str, Counter] = defaultdict(Counter)
    global_counter: Counter = Counter()
    for p in products:
        raws = unique_preserve_order([x.upper() for x in p.raw_keywords if safe_text(x)])
        filtered = [kw for kw in raws if kw.lower() not in NOISE_WORDS and len(kw) >= 2]
        for kw in filtered:
            brand_keyword_counter[p.brand][kw] += 1
            global_counter[kw] += 1
    for brand, counter in brand_keyword_counter.items():
        total_brand_keywords = sum(counter.values()) or 1
        for kw, cnt in counter.most_common(100):
            global_cnt = global_counter[kw]
            concentration = round(cnt / max(global_cnt, 1), 3)
            rows.append({
                "brand": brand,
                "keyword": kw,
                "count": cnt,
                "global_count": global_cnt,
                "brand_share_pct": round(cnt / total_brand_keywords * 100, 2),
                "brand_concentration": concentration,
                "keyword_type": "브랜드 기술" if concentration >= 0.6 and global_cnt >= 2 else "트렌드 기술",
            })
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["brand", "keyword", "count", "global_count", "brand_share_pct", "brand_concentration", "keyword_type"])
    return df.sort_values(["brand", "count", "brand_concentration"], ascending=[True, False, False]).reset_index(drop=True)

# ============================================================
# 5. CRAWLER LAYER (DOM ONLY)
# ============================================================
def _first_working_selector(driver, selectors: List[str]) -> str:
    for css in selectors or []:
        try:
            if driver.find_elements(By.CSS_SELECTOR, css):
                return css
        except Exception:
            continue
    return ""


def _same_domain(url_a: str, url_b: str) -> bool:
    try:
        a = urlparse(url_a).netloc.lower().replace("www.", "")
        b = urlparse(url_b).netloc.lower().replace("www.", "")
        return bool(a and b and a == b)
    except Exception:
        return False


def _normalize_url(base_url: str, href: str) -> str:
    href = safe_text(href)
    if not href:
        return ""
    if href.startswith("javascript:") or href.startswith("#"):
        return ""
    if href.startswith("/"):
        return urljoin(base_url, href)
    return href


class SeleniumCrawler:
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
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(60)
        return driver

    def close(self):
        try:
            self.driver.quit()
        except Exception:
            pass

    

    def crawl_site(self, config: SiteConfig) -> List[ProductRaw]:
        seed_urls = unique_preserve_order([config.start_url] + list(config.extra_start_urls or []))
        print(f"\\n[CRAWL START] {config.brand} :: seeds={len(seed_urls)}")
        products: List[ProductRaw] = []
        seen_product_urls = set()
        visited_listing_urls = set()

        try:
            target_urls: List[str] = []
            for seed_url in seed_urls:
                target_urls.append(seed_url)
                discovered = self._discover_candidate_urls(config, seed_url)
                target_urls.extend(discovered)

            target_urls = unique_preserve_order([u for u in target_urls if safe_text(u)])[: max(1, len(seed_urls) + config.max_discovered_urls)]

            for target_url in target_urls:
                if len(products) >= config.max_products:
                    break
                if target_url in visited_listing_urls:
                    continue
                visited_listing_urls.add(target_url)
                print(f"  - listing: {target_url}")

                try:
                    self.driver.get(target_url)
                    wait_selector = _first_working_selector(self.driver, config.wait_selectors) or _first_working_selector(self.driver, config.card_selectors)
                    if wait_selector:
                        self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, wait_selector)))
                    if config.use_lazy_scroll:
                        self._scroll_to_end()
                    if config.load_more_selectors:
                        self._click_load_more_until_done(config)
                    active_card_selector = _first_working_selector(self.driver, config.card_selectors)
                    if not active_card_selector:
                        continue

                    page_seen_before = len(products)
                    while True:
                        cards = self.driver.find_elements(By.CSS_SELECTOR, active_card_selector)
                        print(f"    · visible cards: {len(cards)}")
                        for idx, card in enumerate(cards):
                            if len(products) >= config.max_products:
                                break
                            try:
                                raw = self._extract_card(config, card, idx)
                                if not raw.product_url and raw.name:
                                    raw.product_url = f"{target_url}#idx-{idx}"
                                if not raw.name:
                                    continue

                                if config.brand_terms:
                                    blob = f"{raw.name} {raw.description} {raw.product_url}".lower()
                                    if not any(term.lower() in blob for term in config.brand_terms):
                                        continue

                                if raw.product_url in seen_product_urls:
                                    continue
                                seen_product_urls.add(raw.product_url)
                                raw.source_url = target_url
                                products.append(raw)
                            except StaleElementReferenceException:
                                continue
                            except Exception as e:
                                print(f"      [WARN] card extract failed: {e}")

                        if len(products) >= config.max_products:
                            break
                        if not config.next_page_selectors:
                            break
                        if not self._goto_next_page(config):
                            break

                    print(f"    · added: {len(products) - page_seen_before}")
                except Exception as page_error:
                    print(f"    [WARN] listing crawl failed: {page_error}")
                    continue

        except Exception as e:
            print(f"[ERROR] crawl failed for {config.brand}: {e}")
            if SCREENSHOT_ON_ERROR:
                ensure_dir(OUT_DIR)
                try:
                    self.driver.save_screenshot(os.path.join(OUT_DIR, f"error_{slugify(config.brand)}.png"))
                except Exception:
                    pass
            traceback.print_exc()

        print(f"[CRAWL DONE] {config.brand} -> {len(products)} products")
        return products

    def _discover_candidate_urls(self, config: SiteConfig, seed_url: str) -> List[str]:
        try:
            self.driver.get(seed_url)
            time.sleep(1.0)
            if config.use_lazy_scroll:
                self._scroll_to_end()
            anchors = self.driver.find_elements(By.CSS_SELECTOR, "a[href]")
        except Exception:
            return []

        candidates: List[str] = []
        for a in anchors:
            try:
                href = _normalize_url(seed_url, a.get_attribute("href") or "")
                label = safe_text(a.text).lower()
                blob = f"{href.lower()} {label}"
                if not href:
                    continue
                if not _same_domain(seed_url, href):
                    continue
                if any(x in blob for x in (config.discovery_block_keywords or [])):
                    continue
                if not any(x in blob for x in (config.discovery_url_keywords or [])):
                    continue
                candidates.append(href)
            except Exception:
                continue

        ranked = sorted(
            unique_preserve_order(candidates),
            key=lambda u: (
                0 if "all" in u.lower() else 1,
                0 if "men" in u.lower() else 1,
                0 if "women" in u.lower() else 1,
                0 if "collection" in u.lower() or "category" in u.lower() else 1,
                len(u),
            ),
        )
        return ranked[: config.max_discovered_urls]

    def _scroll_to_end(self) -> None:
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        stable_count = 0
        while stable_count < 3:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_PAUSE_SEC)
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                stable_count += 1
            else:
                stable_count = 0
                last_height = new_height

    def _click_load_more_until_done(self, config: SiteConfig) -> None:
        for _ in range(100):
            clicked = False
            for sel in config.load_more_selectors:
                try:
                    btn = self.driver.find_element(By.CSS_SELECTOR, sel)
                    if not btn.is_displayed():
                        continue
                    self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
                    time.sleep(0.5)
                    self.driver.execute_script("arguments[0].click();", btn)
                    time.sleep(SCROLL_PAUSE_SEC)
                    clicked = True
                    break
                except Exception:
                    continue
            if not clicked:
                break

    def _goto_next_page(self, config: SiteConfig) -> bool:
        try:
            selector = _first_working_selector(self.driver, config.next_page_selectors)
            if not selector:
                return False
            next_btn = self.driver.find_element(By.CSS_SELECTOR, selector)
            classes = (next_btn.get_attribute("class") or "").lower()
            if "disabled" in classes:
                return False
            self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", next_btn)
            time.sleep(0.5)
            self.driver.execute_script("arguments[0].click();", next_btn)
            wait_selector = _first_working_selector(self.driver, config.wait_selectors) or _first_working_selector(self.driver, config.card_selectors)
            if wait_selector:
                self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, wait_selector)))
            time.sleep(1.0)
            if config.use_lazy_scroll:
                self._scroll_to_end()
            return True
        except Exception:
            return False

    def _extract_card(self, config: SiteConfig, card, idx: int) -> ProductRaw:
        name = try_find_text_multi(card, config.title_selectors)
        product_url = try_find_attr_multi(card, config.link_selectors, "href")
        image_url = try_find_attr_multi(card, config.image_selectors, "src") or try_find_attr_multi(card, config.image_selectors, "data-src")
        price_text = try_find_text_multi(card, config.price_selectors)
        original_price_text = try_find_text_multi(card, config.original_price_selectors)
        sold_out_text = try_find_text_multi(card, config.soldout_selectors)
        gender_text = try_find_text_multi(card, config.gender_selectors)
        season_text = try_find_text_multi(card, config.season_selectors)
        description = try_find_text_multi(card, config.desc_selectors)
        if product_url and product_url.startswith("/"):
            product_url = urljoin(config.start_url, product_url)
        if product_url and (config.click_into_detail or not description or not price_text):
            detail_desc = detail_name = detail_price = detail_original_price = detail_image = ""
            try:
                origin = self.driver.current_window_handle
                self.driver.execute_script("window.open(arguments[0], '_blank');", product_url)
                self.driver.switch_to.window(self.driver.window_handles[-1])
                wait_selector = _first_working_selector(self.driver, config.detail_wait_selectors)
                if wait_selector:
                    self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, wait_selector)))
                detail_name = try_find_text_multi(self.driver, config.detail_name_selectors or config.title_selectors)
                detail_desc = try_find_text_multi(self.driver, config.detail_desc_selectors or config.desc_selectors)
                detail_price = try_find_text_multi(self.driver, config.detail_price_selectors or config.price_selectors)
                detail_original_price = try_find_text_multi(self.driver, config.detail_original_price_selectors or config.original_price_selectors)
                detail_image = try_find_attr_multi(self.driver, config.detail_image_selectors or config.image_selectors, "src")
                if not gender_text:
                    gender_text = try_find_text_multi(self.driver, config.gender_selectors)
                if not season_text:
                    season_text = try_find_text_multi(self.driver, config.season_selectors)
                if not sold_out_text:
                    sold_out_text = try_find_text_multi(self.driver, config.soldout_selectors)
            except Exception:
                pass
            finally:
                try:
                    if len(self.driver.window_handles) > 1:
                        self.driver.close()
                        self.driver.switch_to.window(origin)
                except Exception:
                    pass
            name = first_nonempty(name, detail_name)
            description = first_nonempty(description, detail_desc)
            price_text = first_nonempty(price_text, detail_price)
            original_price_text = first_nonempty(original_price_text, detail_original_price)
            image_url = first_nonempty(image_url, detail_image)
        return ProductRaw(
            brand=config.brand, source_url=config.start_url, product_url=product_url, name=name, description=description,
            price_text=price_text, original_price_text=original_price_text, image_url=image_url, sold_out_text=sold_out_text,
            gender_text=gender_text, season_text=season_text, crawled_at=TODAY_STR,
        )

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


def build_brand_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["brand", "total_products", "active_products", "avg_price", "min_price", "max_price", "sale_products", "sale_share_pct", "sold_out_products", "waterproof_products", "goretex_products", "down_products", "jackets", "pants", "shoes"])
    rows = []
    for brand, g in df.groupby("brand"):
        total = len(g)
        attrs_str = g["standard_attributes"].fillna("")
        rows.append({
            "brand": brand,
            "total_products": total,
            "active_products": int((~g["sold_out"].fillna(False)).sum()),
            "avg_price": round(g["current_price"].dropna().mean(), 0) if g["current_price"].notna().any() else None,
            "min_price": g["current_price"].dropna().min() if g["current_price"].notna().any() else None,
            "max_price": g["current_price"].dropna().max() if g["current_price"].notna().any() else None,
            "sale_products": int(g["discount_rate"].fillna(0).gt(0).sum()),
            "sale_share_pct": round(g["discount_rate"].fillna(0).gt(0).sum() / total * 100, 1) if total else 0,
            "sold_out_products": int(g["sold_out"].fillna(False).sum()),
            "waterproof_products": int(attrs_str.str.contains("방수", regex=False).sum()),
            "goretex_products": int(attrs_str.str.contains("고어텍스", regex=False).sum()),
            "down_products": int(attrs_str.str.contains("다운", regex=False).sum()),
            "jackets": int((g["item_category"] == "자켓").sum()),
            "pants": int((g["item_category"] == "팬츠").sum()),
            "shoes": int((g["item_category"] == "슈즈").sum()),
        })
    return pd.DataFrame(rows).sort_values(["total_products", "avg_price"], ascending=[False, False]).reset_index(drop=True)


def build_attribute_coverage_flags(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    brand_other_ratio = out.assign(is_other=out["dominant_attribute"].fillna("기타").eq("기타")).groupby("brand")["is_other"].mean().fillna(0)
    flag_map = {}
    for brand, ratio in brand_other_ratio.items():
        flag_map[brand] = "Critical" if ratio > 0.25 else "Warning" if ratio > 0.15 else "OK"
    out["attribute_coverage_flag"] = out["brand"].map(flag_map).fillna("OK")
    return out


def _position_x_to_num(brand: str, df: pd.DataFrame) -> float:
    mapping = {"Lifestyle": 1, "Performance": 2, "Extreme": 3}
    subset = df[df["brand"] == brand]
    vals = [mapping.get(v, 1) for v in subset["positioning_x"].fillna("Lifestyle")]
    return round(sum(vals) / len(vals), 2) if vals else 1


def _position_y_to_num(avg_price: Optional[float]) -> float:
    if avg_price is None or (isinstance(avg_price, float) and math.isnan(avg_price)): return 1
    if avg_price < 150000: return 1
    if avg_price < 350000: return 2
    return 3


def build_dashboard_payload(df: pd.DataFrame, brand_summary: pd.DataFrame, kw_df: pd.DataFrame) -> dict:
    if df.empty:
        return {"generated_at": TODAY_STR, "kpis": {"brands": 0, "products": 0, "sale_products": 0, "avg_price": 0}, "brand_summary": [], "products": [], "charts": {}, "keywords": []}
    charts = {
        "brandProductCounts": {"labels": brand_summary["brand"].tolist(), "values": brand_summary["total_products"].fillna(0).astype(int).tolist()},
        "brandAvgPrice": {"labels": brand_summary["brand"].tolist(), "values": [int(x) if pd.notna(x) else 0 for x in brand_summary["avg_price"]]},
        "priceBand": {"labels": list(df["price_band"].fillna("기타").value_counts().index), "values": list(df["price_band"].fillna("기타").value_counts().values)},
        "itemCategory": {"labels": list(df["item_category"].fillna("기타").value_counts().head(10).index), "values": list(df["item_category"].fillna("기타").value_counts().head(10).values)},
        "dominantAttribute": {"labels": list(df["dominant_attribute"].fillna("기타").value_counts().head(10).index), "values": list(df["dominant_attribute"].fillna("기타").value_counts().head(10).values)},
        "grade": {"labels": list(df["grade"].fillna("Entry").value_counts().index), "values": list(df["grade"].fillna("Entry").value_counts().values)},
        "shellType": {"labels": list(df["shell_type"].fillna("Unknown").value_counts().index), "values": list(df["shell_type"].fillna("Unknown").value_counts().values)},
        "positioning": [{"brand": row["brand"], "x": _position_x_to_num(row["brand"], df), "y": _position_y_to_num(row["avg_price"]), "avg_price": int(row["avg_price"]) if pd.notna(row["avg_price"]) else 0, "size": int(row["total_products"])} for _, row in brand_summary.iterrows()],
    }
    keywords = []
    if not kw_df.empty:
        for brand, g in kw_df.groupby("brand"):
            keywords.append({"brand": brand, "items": g.head(12).to_dict("records")})
    return {
        "generated_at": TODAY_STR,
        "kpis": {
            "brands": int(df["brand"].nunique()),
            "products": int(len(df)),
            "sale_products": int(df["discount_rate"].fillna(0).gt(0).sum()),
            "avg_price": int(df["current_price"].dropna().mean()) if df["current_price"].notna().any() else 0,
        },
        "brand_summary": brand_summary.to_dict("records"),
        "products": df.sort_values(["brand", "current_price"], ascending=[True, False]).fillna("").to_dict("records"),
        "charts": charts,
        "keywords": keywords,
    }

# ============================================================
# 7. HTML DASHBOARD (INLINE ONLY)
# ============================================================
def render_dashboard(payload: dict) -> str:
    data_json = json_dumps(payload)
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
    body { background: radial-gradient(circle at top left, #e0f2fe 0%, #f8fafc 28%, #eef2ff 100%); }
    .glass { backdrop-filter: blur(18px); background: rgba(255,255,255,.78); }
    .panel { box-shadow: 0 12px 40px rgba(15,23,42,.08); }
    .metric-card { box-shadow: 0 10px 30px rgba(15,23,42,.08); }
    .table-wrap::-webkit-scrollbar { height: 8px; width: 8px; }
    .table-wrap::-webkit-scrollbar-thumb { background:#cbd5e1; border-radius:999px; }
    .tag { display:inline-flex; align-items:center; border-radius:999px; padding:4px 10px; font-size:11px; font-weight:800; }
    .heat-cell { position:relative; overflow:hidden; }
    .heat-fill { position:absolute; inset:0; opacity:.14; z-index:1; border-radius:16px; }
    .heat-cell > span { position:relative; z-index:2; }
    .product-card:hover { transform: translateY(-3px); transition: .18s ease; }
    .line-clamp-2 { display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; overflow:hidden; }
    .line-clamp-3 { display:-webkit-box; -webkit-line-clamp:3; -webkit-box-orient:vertical; overflow:hidden; }
  </style>
</head>
<body class="text-slate-900">
  <div class="mx-auto max-w-[1840px] px-5 py-6 lg:px-8">
    <div class="rounded-[34px] border border-white/70 glass panel p-6 lg:p-8">
      <div class="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <div class="text-[11px] font-extrabold tracking-[0.24em] text-slate-500">COMPETITOR OUTDOOR INTELLIGENCE</div>
          <h1 class="mt-2 text-3xl font-black tracking-[-0.05em] text-slate-900 lg:text-5xl">결과값 중심 경쟁사 상품 대시보드</h1>
          <div class="mt-3 max-w-4xl text-sm font-bold leading-6 text-slate-600">텍스트를 길게 읽지 않아도 되도록 KPI, 브랜드 비교, 가격대, 카테고리, 속성, 포지셔닝, 상품 카드 순서로 바로 볼 수 있게 구성했습니다.</div>
        </div>
        <div class="rounded-3xl bg-slate-900 px-5 py-4 text-white shadow-xl">
          <div class="text-[11px] font-extrabold tracking-[0.18em] text-slate-300">GENERATED</div>
          <div class="mt-2 text-lg font-black">__GENERATED__</div>
        </div>
      </div>
    </div>

    <section class="mt-6 grid grid-cols-2 gap-4 lg:grid-cols-4" id="kpi-grid"></section>

    <section class="mt-6 grid grid-cols-1 gap-4 2xl:grid-cols-[1.2fr_.8fr]">
      <div class="rounded-[28px] border border-white/70 glass panel p-5">
        <div class="flex items-end justify-between gap-3">
          <div>
            <div class="text-[11px] font-extrabold tracking-[0.22em] text-slate-500">FILTER</div>
            <div class="mt-1 text-xl font-black tracking-[-0.03em]">빠르게 좁혀보기</div>
          </div>
          <button id="resetFilters" class="rounded-2xl bg-slate-900 px-4 py-3 text-xs font-black text-white">필터 초기화</button>
        </div>
        <div class="mt-4 grid grid-cols-1 gap-3 lg:grid-cols-5">
          <input id="searchInput" class="h-12 rounded-2xl border border-slate-200 bg-white px-4 text-sm font-bold outline-none" placeholder="상품명 / 설명 / 키워드 검색" />
          <select id="brandFilter" class="h-12 rounded-2xl border border-slate-200 bg-white px-4 text-sm font-bold outline-none"></select>
          <select id="categoryFilter" class="h-12 rounded-2xl border border-slate-200 bg-white px-4 text-sm font-bold outline-none"></select>
          <select id="priceBandFilter" class="h-12 rounded-2xl border border-slate-200 bg-white px-4 text-sm font-bold outline-none"></select>
          <select id="gradeFilter" class="h-12 rounded-2xl border border-slate-200 bg-white px-4 text-sm font-bold outline-none"></select>
        </div>
        <div class="mt-3 flex flex-wrap gap-2">
          <button class="toggle-chip tag bg-slate-900 text-white" data-key="excludeSoldout">품절 제외</button>
          <button class="toggle-chip tag bg-white text-slate-700 border border-slate-200" data-key="saleOnly">세일만</button>
          <button class="toggle-chip tag bg-white text-slate-700 border border-slate-200" data-key="waterproofOnly">방수 중심만</button>
        </div>
        <div id="filterResultText" class="mt-4 text-sm font-black text-slate-600">전체 상품 표시 중</div>
      </div>

      <div class="rounded-[28px] border border-white/70 glass panel p-5">
        <div class="text-[11px] font-extrabold tracking-[0.22em] text-slate-500">KEY INSIGHT</div>
        <div id="insightBox" class="mt-2 text-lg font-black leading-8 tracking-[-0.03em] text-slate-900"></div>
        <div class="mt-4 grid grid-cols-2 gap-3">
          <div class="rounded-2xl bg-white p-4 border border-slate-200"><div class="text-xs font-extrabold text-slate-500">브랜드 평균가 선두</div><div id="topAvgPriceBrand" class="mt-2 text-2xl font-black"></div></div>
          <div class="rounded-2xl bg-white p-4 border border-slate-200"><div class="text-xs font-extrabold text-slate-500">SKU 선두</div><div id="topSkuBrand" class="mt-2 text-2xl font-black"></div></div>
        </div>
      </div>
    </section>

    <section class="mt-6 grid grid-cols-1 gap-4 xl:grid-cols-2 2xl:grid-cols-4">
      <div class="rounded-[28px] border border-white/70 glass panel p-5"><div class="mb-3 text-lg font-black tracking-[-0.03em]">브랜드별 SKU 수</div><div class="h-[250px]"><canvas id="brandCountChart"></canvas></div></div>
      <div class="rounded-[28px] border border-white/70 glass panel p-5"><div class="mb-3 text-lg font-black tracking-[-0.03em]">브랜드별 평균가</div><div class="h-[250px]"><canvas id="avgPriceChart"></canvas></div></div>
      <div class="rounded-[28px] border border-white/70 glass panel p-5"><div class="mb-3 text-lg font-black tracking-[-0.03em]">가격대 분포</div><div class="h-[250px]"><canvas id="priceBandChart"></canvas></div></div>
      <div class="rounded-[28px] border border-white/70 glass panel p-5"><div class="mb-3 text-lg font-black tracking-[-0.03em]">카테고리 분포</div><div class="h-[250px]"><canvas id="categoryChart"></canvas></div></div>
    </section>

    <section class="mt-6 grid grid-cols-1 gap-4 xl:grid-cols-3">
      <div class="rounded-[28px] border border-white/70 glass panel p-5"><div class="mb-3 text-lg font-black tracking-[-0.03em]">대표 속성 분포</div><div class="h-[250px]"><canvas id="dominantAttrChart"></canvas></div></div>
      <div class="rounded-[28px] border border-white/70 glass panel p-5"><div class="mb-3 text-lg font-black tracking-[-0.03em]">등급 분포</div><div class="h-[250px]"><canvas id="gradeChart"></canvas></div></div>
      <div class="rounded-[28px] border border-white/70 glass panel p-5"><div class="mb-3 text-lg font-black tracking-[-0.03em]">Shell 타입 분포</div><div class="h-[250px]"><canvas id="shellChart"></canvas></div></div>
    </section>

    <section class="mt-6 grid grid-cols-1 gap-4 2xl:grid-cols-[1.25fr_.75fr]">
      <div class="rounded-[28px] border border-white/70 glass panel p-5">
        <div class="flex items-end justify-between gap-3 flex-wrap"><div><div class="text-[11px] font-extrabold tracking-[0.22em] text-slate-500">BRAND COMPARISON</div><div class="mt-1 text-xl font-black tracking-[-0.03em]">브랜드 비교표</div></div><div class="text-sm font-bold text-slate-500">정렬: 총 상품 수 → 평균가</div></div>
        <div class="table-wrap mt-4 overflow-x-auto">
          <table class="min-w-[1100px] w-full text-sm"><thead><tr class="border-b border-slate-200 text-slate-500"><th class="py-3 text-left">Brand</th><th class="py-3 text-right">Total</th><th class="py-3 text-right">Active</th><th class="py-3 text-right">Avg Price</th><th class="py-3 text-right">Sale %</th><th class="py-3 text-right">방수</th><th class="py-3 text-right">고어텍스</th><th class="py-3 text-right">다운</th><th class="py-3 text-right">자켓</th><th class="py-3 text-right">팬츠</th><th class="py-3 text-right">슈즈</th></tr></thead><tbody id="brandSummaryBody"></tbody></table>
        </div>
      </div>
      <div class="rounded-[28px] border border-white/70 glass panel p-5">
        <div class="text-[11px] font-extrabold tracking-[0.22em] text-slate-500">POSITIONING MAP</div>
        <div class="mt-1 text-xl font-black tracking-[-0.03em]">브랜드 포지셔닝</div>
        <div class="mt-3 rounded-2xl bg-white border border-slate-200 p-3 h-[420px]"><canvas id="positionChart"></canvas></div>
        <div class="mt-3 text-xs font-bold text-slate-500">X: Lifestyle → Performance → Extreme / Y: Mass → Premium → Luxury</div>
      </div>
    </section>

    <section class="mt-6 rounded-[28px] border border-white/70 glass panel p-5">
      <div class="text-[11px] font-extrabold tracking-[0.22em] text-slate-500">KEYWORD DISCOVERY</div>
      <div class="mt-1 text-xl font-black tracking-[-0.03em]">브랜드별 핵심 기술 키워드</div>
      <div id="keywordGrid" class="mt-4 grid grid-cols-1 gap-4 xl:grid-cols-2 2xl:grid-cols-3"></div>
    </section>

    <section class="mt-6 rounded-[28px] border border-white/70 glass panel p-5">
      <div class="flex items-end justify-between gap-3 flex-wrap"><div><div class="text-[11px] font-extrabold tracking-[0.22em] text-slate-500">PRODUCT FEED</div><div class="mt-1 text-xl font-black tracking-[-0.03em]">상품 카드 뷰</div></div><div id="productCountText" class="text-sm font-bold text-slate-500"></div></div>
      <div id="productGrid" class="mt-4 grid grid-cols-1 gap-4 lg:grid-cols-2 2xl:grid-cols-3"></div>
    </section>
  </div>
<script>
const DATA = __DATA__;
const state = { search:"", brand:"전체", category:"전체", priceBand:"전체", grade:"전체", excludeSoldout:true, saleOnly:false, waterproofOnly:false };
function formatNumber(v) { if (v === null || v === undefined || v === "") return "-"; return new Intl.NumberFormat('ko-KR').format(v); }
function formatPrice(v) { if (!v && v !== 0) return "-"; return formatNumber(v) + "원"; }
function createKpis() { const k=DATA.kpis; const items=[["브랜드 수",k.brands,"실제 크롤링 완료 브랜드"],["총 상품 수",k.products,"현재 분석 대상 SKU"],["세일 상품 수",k.sale_products,"할인율 0 초과 기준"],["평균가",formatPrice(k.avg_price),"현재가 평균"]]; document.getElementById('kpi-grid').innerHTML = items.map(([label,value,desc]) => `<div class="metric-card rounded-[28px] border border-white/70 glass p-5"><div class="text-[11px] font-extrabold tracking-[0.18em] text-slate-500">${label}</div><div class="mt-2 text-3xl font-black tracking-[-0.05em] text-slate-900">${value}</div><div class="mt-1 text-xs font-bold text-slate-500">${desc}</div></div>`).join(''); }
function fillSelect(id, items) { document.getElementById(id).innerHTML = items.map(x => `<option value="${x}">${x}</option>`).join(''); }
function populateFilters() { const products=DATA.products; fillSelect('brandFilter',["전체",...new Set(products.map(x=>x.brand).filter(Boolean))]); fillSelect('categoryFilter',["전체",...new Set(products.map(x=>x.item_category).filter(Boolean))]); fillSelect('priceBandFilter',["전체",...new Set(products.map(x=>x.price_band).filter(Boolean))]); fillSelect('gradeFilter',["전체",...new Set(products.map(x=>x.grade).filter(Boolean))]); document.getElementById('searchInput').addEventListener('input',e=>{state.search=e.target.value.trim().toLowerCase(); renderProducts();}); document.getElementById('brandFilter').addEventListener('change',e=>{state.brand=e.target.value; renderProducts();}); document.getElementById('categoryFilter').addEventListener('change',e=>{state.category=e.target.value; renderProducts();}); document.getElementById('priceBandFilter').addEventListener('change',e=>{state.priceBand=e.target.value; renderProducts();}); document.getElementById('gradeFilter').addEventListener('change',e=>{state.grade=e.target.value; renderProducts();}); document.querySelectorAll('.toggle-chip').forEach(btn=>{ btn.addEventListener('click',()=>{ const key=btn.dataset.key; state[key]=!state[key]; btn.classList.toggle('bg-slate-900',state[key]); btn.classList.toggle('text-white',state[key]); btn.classList.toggle('bg-white',!state[key]); btn.classList.toggle('text-slate-700',!state[key]); renderProducts(); }); }); document.getElementById('resetFilters').addEventListener('click',()=>{ state.search=""; state.brand="전체"; state.category="전체"; state.priceBand="전체"; state.grade="전체"; state.excludeSoldout=true; state.saleOnly=false; state.waterproofOnly=false; document.getElementById('searchInput').value=""; document.getElementById('brandFilter').value="전체"; document.getElementById('categoryFilter').value="전체"; document.getElementById('priceBandFilter').value="전체"; document.getElementById('gradeFilter').value="전체"; document.querySelectorAll('.toggle-chip').forEach(btn=>{ const active=state[btn.dataset.key]; btn.classList.toggle('bg-slate-900',active); btn.classList.toggle('text-white',active); btn.classList.toggle('bg-white',!active); btn.classList.toggle('text-slate-700',!active); }); renderProducts(); }); }
function filteredProducts() { return DATA.products.filter(p=>{ const blob=[p.brand,p.name,p.description,p.item_category,p.raw_keywords,p.standard_attributes,p.dominant_attribute].join(' ').toLowerCase(); if(state.search && !blob.includes(state.search)) return false; if(state.brand!=="전체" && p.brand!==state.brand) return false; if(state.category!=="전체" && p.item_category!==state.category) return false; if(state.priceBand!=="전체" && p.price_band!==state.priceBand) return false; if(state.grade!=="전체" && p.grade!==state.grade) return false; if(state.excludeSoldout && !!p.sold_out) return false; if(state.saleOnly && !(Number(p.discount_rate||0)>0)) return false; if(state.waterproofOnly && !String(p.standard_attributes||'').includes('방수') && !String(p.standard_attributes||'').includes('고어텍스')) return false; return true; }); }
function renderInsight() { const rows=[...DATA.brand_summary]; const topAvg=[...rows].sort((a,b)=>(b.avg_price||0)-(a.avg_price||0))[0]; const topSku=[...rows].sort((a,b)=>(b.total_products||0)-(a.total_products||0))[0]; document.getElementById('topAvgPriceBrand').textContent = topAvg ? `${topAvg.brand} · ${formatPrice(topAvg.avg_price)}` : '-'; document.getElementById('topSkuBrand').textContent = topSku ? `${topSku.brand} · ${formatNumber(topSku.total_products)}개` : '-'; document.getElementById('insightBox').textContent = topAvg && topSku ? `평균가가 가장 높은 브랜드는 ${topAvg.brand}, SKU가 가장 많은 브랜드는 ${topSku.brand}입니다. 먼저 브랜드 비교표와 포지셔닝 맵으로 큰 그림을 보고, 아래 상품 카드에서 세부 상품을 확인하면 읽는 피로가 크게 줄어듭니다.` : '데이터가 쌓이면 가장 먼저 봐야 할 변화 포인트를 여기서 바로 읽을 수 있습니다.'; }
function renderBrandSummary() { const rows=DATA.brand_summary; const maxTotal=Math.max(...rows.map(r=>r.total_products||0),1); const maxAvg=Math.max(...rows.map(r=>r.avg_price||0),1); document.getElementById('brandSummaryBody').innerHTML = rows.map(r => `<tr class="border-b border-slate-200 last:border-b-0 hover:bg-slate-50/70"><td class="py-3 font-black text-slate-900">${r.brand}</td><td class="py-3 text-right heat-cell"><div class="heat-fill bg-sky-500" style="width:${(r.total_products/maxTotal)*100}%"></div><span>${formatNumber(r.total_products)}</span></td><td class="py-3 text-right"><span>${formatNumber(r.active_products)}</span></td><td class="py-3 text-right heat-cell"><div class="heat-fill bg-violet-500" style="width:${((r.avg_price||0)/maxAvg)*100}%"></div><span>${formatPrice(r.avg_price)}</span></td><td class="py-3 text-right">${r.sale_share_pct}%</td><td class="py-3 text-right">${formatNumber(r.waterproof_products)}</td><td class="py-3 text-right">${formatNumber(r.goretex_products)}</td><td class="py-3 text-right">${formatNumber(r.down_products)}</td><td class="py-3 text-right">${formatNumber(r.jackets)}</td><td class="py-3 text-right">${formatNumber(r.pants)}</td><td class="py-3 text-right">${formatNumber(r.shoes)}</td></tr>`).join(''); }
function renderKeywords() { document.getElementById('keywordGrid').innerHTML = (DATA.keywords||[]).map(block => `<div class="rounded-[24px] border border-slate-200 bg-white p-4"><div class="text-lg font-black tracking-[-0.03em] text-slate-900">${block.brand}</div><div class="mt-3 flex flex-wrap gap-2">${(block.items||[]).map(it => `<span class="tag bg-slate-900 text-white">${it.keyword} · ${it.count}</span>`).join('')}</div></div>`).join(''); }
function renderProducts() { const items=filteredProducts(); document.getElementById('filterResultText').textContent=`${formatNumber(items.length)}개 상품 표시 중`; document.getElementById('productCountText').textContent=`현재 ${formatNumber(items.length)}개`; document.getElementById('productGrid').innerHTML = items.slice(0,300).map(p => `<article class="product-card rounded-[24px] border border-slate-200 bg-white p-4 shadow-sm"><div class="flex gap-4"><div class="h-32 w-32 shrink-0 overflow-hidden rounded-2xl bg-slate-100 border border-slate-200">${p.image_url ? `<img src="${p.image_url}" alt="${p.name}" class="h-full w-full object-cover" loading="lazy" />` : `<div class="flex h-full items-center justify-center text-xs font-black text-slate-400">NO IMAGE</div>`}</div><div class="min-w-0 flex-1"><div class="flex flex-wrap gap-2"><span class="tag bg-slate-900 text-white">${p.brand}</span><span class="tag bg-sky-50 text-sky-700">${p.item_category}</span><span class="tag bg-violet-50 text-violet-700">${p.grade}</span><span class="tag bg-emerald-50 text-emerald-700">${p.shell_type}</span>${p.sold_out ? `<span class="tag bg-rose-50 text-rose-700">품절</span>` : ``}</div><div class="mt-3 line-clamp-2 text-lg font-black leading-7 tracking-[-0.03em] text-slate-900">${p.name}</div><div class="mt-2 flex flex-wrap items-end gap-3"><div class="text-2xl font-black tracking-[-0.04em]">${formatPrice(p.current_price)}</div>${p.original_price && p.original_price != p.current_price ? `<div class="text-sm font-bold text-slate-400 line-through">${formatPrice(p.original_price)}</div>` : ``}${p.discount_rate ? `<div class="text-sm font-black text-rose-600">-${p.discount_rate}%</div>` : ``}</div><div class="mt-3 flex flex-wrap gap-2">${(String(p.standard_attributes||'').split(',').map(x => x.trim()).filter(Boolean).slice(0,6)).map(x => `<span class="tag bg-slate-100 text-slate-700">${x}</span>`).join('')}</div><div class="mt-3 text-sm font-bold leading-6 text-slate-500 line-clamp-3">${p.description || '설명 없음'}</div><div class="mt-4 flex items-center justify-between gap-3"><div class="text-xs font-black text-slate-400">${p.gender} · ${p.season} · ${p.price_band}</div>${p.product_url ? `<a href="${p.product_url}" target="_blank" rel="noopener noreferrer" class="rounded-2xl bg-slate-900 px-4 py-2 text-xs font-black text-white">상품 보기</a>` : ``}</div></div></div></article>`).join(''); }
function baseChart(id, type, labels, values, extra={}) { return new Chart(document.getElementById(id), { type, data:{labels, datasets:[{data:values, borderWidth:2, borderRadius:10, tension:.35}]}, options:Object.assign({ responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}}, scales: type.includes('pie') || type.includes('doughnut') ? {} : { y:{beginAtZero:true,ticks:{precision:0}}, x:{ticks:{autoSkip:false}} } }, extra) }); }
function renderCharts() { const c=DATA.charts; baseChart('brandCountChart','bar',c.brandProductCounts.labels,c.brandProductCounts.values); baseChart('avgPriceChart','bar',c.brandAvgPrice.labels,c.brandAvgPrice.values); baseChart('priceBandChart','doughnut',c.priceBand.labels,c.priceBand.values); baseChart('categoryChart','bar',c.itemCategory.labels,c.itemCategory.values); baseChart('dominantAttrChart','bar',c.dominantAttribute.labels,c.dominantAttribute.values); baseChart('gradeChart','pie',c.grade.labels,c.grade.values); baseChart('shellChart','pie',c.shellType.labels,c.shellType.values); new Chart(document.getElementById('positionChart'), { type:'bubble', data:{ datasets:c.positioning.map(item => ({ label:item.brand, data:[{x:item.x,y:item.y,r:Math.max(8, Math.min(26, item.size/3))}] })) }, options:{ responsive:true, maintainAspectRatio:false, plugins:{ legend:{position:'bottom', labels:{boxWidth:12,usePointStyle:true}}, tooltip:{callbacks:{ label:(ctx)=>{ const item=c.positioning[ctx.datasetIndex]; return `${item.brand} · Avg ${formatPrice(item.avg_price)} · SKU ${item.size}`; } }} }, scales:{ x:{min:.5,max:3.5,ticks:{stepSize:1,callback:(v)=>({1:'Lifestyle',2:'Performance',3:'Extreme'}[v] || '')}}, y:{min:.5,max:3.5,ticks:{stepSize:1,callback:(v)=>({1:'Mass',2:'Premium',3:'Luxury'}[v] || '')}} } } }); }
function init() { createKpis(); populateFilters(); renderInsight(); renderBrandSummary(); renderKeywords(); renderProducts(); renderCharts(); }
init();
</script>
</body>
</html>"""
    return template.replace("__DATA__", data_json).replace("__GENERATED__", generated_at)

# ============================================================
# 8. WRITE OUTPUTS
# ============================================================
def write_outputs(raw_products: List[ProductRaw], analyzed_products: List[ProductAnalyzed]) -> None:
    ensure_dir(OUT_DIR)
    raw_df = pd.DataFrame([asdict(x) for x in raw_products])
    analyzed_df = build_attribute_coverage_flags(products_to_dataframe(analyzed_products))
    brand_summary_df = build_brand_summary(analyzed_df)
    keyword_df = discover_keywords(analyzed_products)
    payload = build_dashboard_payload(analyzed_df, brand_summary_df, keyword_df)
    with open(os.path.join(OUT_DIR, "dashboard_data.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    with open(os.path.join(OUT_DIR, "dashboard.html"), "w", encoding="utf-8") as f:
        f.write(render_dashboard(payload))
    raw_df.to_csv(os.path.join(OUT_DIR, "raw_products.csv"), index=False, encoding="utf-8-sig")
    analyzed_df.to_csv(os.path.join(OUT_DIR, "analyzed_products.csv"), index=False, encoding="utf-8-sig")
    brand_summary_df.to_csv(os.path.join(OUT_DIR, "brand_summary.csv"), index=False, encoding="utf-8-sig")
    keyword_df.to_csv(os.path.join(OUT_DIR, "keyword_discovery.csv"), index=False, encoding="utf-8-sig")
    print("\n[OUTPUTS]")
    for name in ["raw_products.csv", "analyzed_products.csv", "brand_summary.csv", "keyword_discovery.csv", "dashboard_data.json", "dashboard.html"]:
        print(f"- {os.path.join(OUT_DIR, name)}")

# ============================================================
# 9. MAIN
# ============================================================
def validate_site_configs(configs: List[SiteConfig]) -> None:
    errors = []
    for cfg in configs:
        if not cfg.brand: errors.append("brand missing")
        if not cfg.start_url: errors.append(f"{cfg.brand}: start_url missing")
        if not cfg.card_selectors: errors.append(f"{cfg.brand}: card_selectors missing")
        if not cfg.title_selectors: errors.append(f"{cfg.brand}: title_selectors missing")
        if not cfg.price_selectors: errors.append(f"{cfg.brand}: price_selectors missing")
    if errors:
        raise ValueError("Invalid SITE_CONFIGS\n- " + "\n- ".join(errors))


def main() -> None:
    ensure_dir(OUT_DIR)
    validate_site_configs(SITE_CONFIGS)
    crawler = SeleniumCrawler(headless=HEADLESS)
    raw_products: List[ProductRaw] = []
    try:
        for config in SITE_CONFIGS:
            raw_products.extend(crawler.crawl_site(config))
        analyzed_products = [analyze_product(p) for p in raw_products]
        write_outputs(raw_products, analyzed_products)
    finally:
        crawler.close()


if __name__ == "__main__":
    main()
