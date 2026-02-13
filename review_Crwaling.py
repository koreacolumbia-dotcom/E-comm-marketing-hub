#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
[FINAL - FULL INTEGRATED]
- Crema 리뷰 수집 (pagy.next 끝까지)
- 불만 키워드 Top5 자동 추출(형태소 없이)
- 100자+ 리뷰 텍스트 캡처 이미지(PIL) 자동 생성
- 상품 이미지/리뷰 썸네일 다운로드 → site/assets 로컬 자산화
- site/data/reviews.json, site/data/meta.json 생성
- (중요) 사용자가 제공한 HTML 레이아웃 그대로 site/index.html 생성
  - demo JS 제거
  - JSON 로딩 렌더 JS로 교체

필수 환경변수:
- CREMA_SECURE_DEVICE_TOKEN

선택 환경변수:
- CREMA_DOMAIN (default columbiakorea.co.kr)
- CREMA_WIDGET_ID (default 2)
- CREMA_PER_PAGE (default 30)
- OUTPUT_TZ (default Asia/Seoul)

입력:
- config/products.csv (product_code 필수, product_name/product_url optional)

출력:
- site/index.html
- site/data/reviews.json
- site/data/meta.json
- site/assets/products/*
- site/assets/reviews/*
- site/assets/text_images/*
"""

from __future__ import annotations

import os
import re
import json
import time
import math
import hashlib
import pathlib
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple

import requests
import pandas as pd
from dateutil import tz
# from tqdm import tqdm
from PIL import Image, ImageDraw, ImageFont

import json
from pathlib import Path
# ----------------------------
# Settings
# ----------------------------

DEFAULT_DOMAIN = os.getenv("CREMA_DOMAIN", "columbiakorea.co.kr").strip()
DEFAULT_WIDGET_ID = int(os.getenv("CREMA_WIDGET_ID", "2"))
DEFAULT_PER_PAGE = int(os.getenv("CREMA_PER_PAGE", "30"))
OUTPUT_TZ = os.getenv("OUTPUT_TZ", "Asia/Seoul")

SECURE_DEVICE_TOKEN = os.getenv("CREMA_SECURE_DEVICE_TOKEN", "").strip()
if not SECURE_DEVICE_TOKEN:
    raise SystemExit("ERROR: CREMA_SECURE_DEVICE_TOKEN 환경변수가 필요합니다.")

BASE_API = f"https://review8.cre.ma/api/{DEFAULT_DOMAIN}"

# project root: scripts/ 아래에 이 파일이 있다고 가정 → parents[1] = repo root
ROOT = pathlib.Path(__file__).resolve().parents[1]

CONFIG_DIR = ROOT / "config"
SITE_DIR = ROOT / "site"
SITE_DATA_DIR = SITE_DIR / "data"
ASSET_DIR = SITE_DIR / "assets"
ASSET_PRODUCTS = ASSET_DIR / "products"
ASSET_REVIEWS = ASSET_DIR / "reviews"
ASSET_TEXT = ASSET_DIR / "text_images"

for d in [SITE_DIR, SITE_DATA_DIR, ASSET_DIR, ASSET_PRODUCTS, ASSET_REVIEWS, ASSET_TEXT]:
    d.mkdir(parents=True, exist_ok=True)

# ----------------------------
# Helpers
# ----------------------------

def now_kst() -> datetime:
    return datetime.now(tz=tz.gettz(OUTPUT_TZ))

def parse_dt(s: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None

def safe_filename(s: str) -> str:
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", s)
    return s[:180]

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def url_ext(url: str) -> str:
    m = re.search(r"\.(jpg|jpeg|png|webp)(\?|$)", url, re.I)
    if m:
        return "." + m.group(1).lower()
    return ".webp"

# ----------------------------
# HTTP with retry + download
# ----------------------------

class Http:
    def __init__(self, timeout: int = 30):
        self.s = requests.Session()
        self.timeout = timeout

    def get_json(
        self,
        url: str,
        params: Dict[str, Any],
        headers: Optional[Dict[str, str]] = None,
        max_retries: int = 5,
        backoff: float = 1.4,
    ) -> Dict[str, Any]:
        headers = headers or {}
        headers.setdefault("Accept", "application/json, text/plain, */*")
        headers.setdefault("User-Agent", "Mozilla/5.0")

        last_err = None
        for i in range(max_retries):
            try:
                r = self.s.get(url, params=params, headers=headers, timeout=self.timeout)
                if r.status_code == 429:
                    time.sleep((backoff ** i) * 0.8)
                    continue
                r.raise_for_status()
                return r.json()
            except Exception as e:
                last_err = e
                time.sleep(backoff ** i)
        raise RuntimeError(f"GET failed: {url} params={params} err={last_err}")

    def download(
        self,
        url: str,
        out_path: pathlib.Path,
        headers: Optional[Dict[str, str]] = None,
        max_retries: int = 4,
    ) -> bool:
        headers = headers or {}
        headers.setdefault("User-Agent", "Mozilla/5.0")
        try:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            if out_path.exists() and out_path.stat().st_size > 1024:
                return True

            for i in range(max_retries):
                r = self.s.get(url, headers=headers, timeout=self.timeout, stream=True)
                if r.status_code in (403, 404):
                    return False
                if r.status_code == 429:
                    time.sleep(1.0 + i * 0.6)
                    continue
                r.raise_for_status()
                with open(out_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 64):
                        if chunk:
                            f.write(chunk)
                return out_path.exists() and out_path.stat().st_size > 0
        except Exception:
            return False

http = Http()

# ----------------------------
# Crema API
# ----------------------------

def crema_reviews(product_code: str, page: int, per: int, widget_id: int) -> Dict[str, Any]:
    url = f"{BASE_API}/reviews"
    params = {
        "secure_device_token": SECURE_DEVICE_TOKEN,
        "fields": (
            "has_media,total_product_media_reviews_count,"
            "reviews.evaluation_type_options,reviews.ai_summary,"
            "reviews.with_parent_reviews,reviews.review_options"
        ),
        "product_code": product_code,
        "widget_id": widget_id,
        "app": 0,
        "iframe": 1,
        "widget_style": "list_v3",
        "page": page,
        "per": per,
    }
    headers = {
        "Referer": f"https://review8.cre.ma/v2/{DEFAULT_DOMAIN}/product_reviews/list_v3"
    }
    return http.get_json(url, params=params, headers=headers)

def fetch_all_pages(product_code: str, per: int, widget_id: int, hard_max_pages: int = 300) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    page = 1
    for _ in range(hard_max_pages):
        data = crema_reviews(product_code=product_code, page=page, per=per, widget_id=widget_id)
        reviews = data.get("reviews") or []
        out.extend(reviews)

        pagy = data.get("pagy") or {}
        nxt = pagy.get("next")
        if not nxt:
            break
        page = int(nxt)
        time.sleep(0.2)  # 서버 부담 완화
    return out

# ----------------------------
# Keyword extraction (no morphology)
# ----------------------------

STOPWORDS = set("""
그리고 그러나 그래서 하지만 또한 너무 정말 완전 진짜 매우 그냥 조금 약간 저는 제가 우리는 너희 이거 그거 저거
있습니다 입니다 같아요 같네요 하는 하다 됐어요 되었어요 되네요 구매 구입 제품 상품 사용
""".split())

def tokenize_ko(text: str) -> List[str]:
    # 한글/영문/숫자 토큰, 길이>=2
    toks = re.findall(r"[가-힣A-Za-z0-9]+", (text or "").lower())
    return [t for t in toks if len(t) >= 2 and t not in STOPWORDS]

def top_terms(texts: List[str], topk: int = 5) -> List[Tuple[str, int]]:
    freq: Dict[str, int] = {}
    for tx in texts:
        for tok in tokenize_ko(tx):
            freq[tok] = freq.get(tok, 0) + 1
    items = sorted(freq.items(), key=lambda x: x[1], reverse=True)
    return items[:topk]

# ----------------------------
# Tagging rules (simple, deterministic)
# ----------------------------

SIZE_KEYWORDS = [
    "사이즈","정사이즈","한치수","한 치수","작아요","작다","커요","크다","핏","낙낙","타이트","여유","끼","기장","소매","어깨","가슴","발볼"
]

POS_KEYWORDS = [
    "만족","좋아요","좋다","예뻐","이쁘","추천","재구매","가성비","편해","편안","따뜻","가볍","최고","빠르","잘샀"
]

REQ_KEYWORDS = [
    "개선","아쉬","불편","바랐","했으면","추가","보완","수정","필요","요청","문의","교환","반품"
]

def has_any_kw(text: str, kws: List[str]) -> bool:
    t = (text or "").replace(" ", "")
    for kw in kws:
        if kw.replace(" ", "") in t:
            return True
    return False

def classify_size_direction(text: str) -> str:
    """
    too_small / too_big / other
    (형태소 없이 규칙 기반)
    """
    t = (text or "").replace(" ", "")
    small_kw = ["작아","작다","타이트","끼","조인다","짧다","좁다","발볼좁","어깨좁","가슴좁"]
    big_kw = ["커","크다","넉넉","오버","길다","넓다","헐렁","부해"]
    for kw in small_kw:
        if kw in t:
            return "too_small"
    for kw in big_kw:
        if kw in t:
            return "too_big"
    return "other"

# ----------------------------
# PIL text image (100+ chars)
# ----------------------------

def wrap_text(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_width: int) -> List[str]:
    """
    공백 기준 wrap. (한국어 긴 문장 대응 위해 fallback: 글자 단위로도 분할)
    """
    text = (text or "").strip()
    if not text:
        return [""]

    words = text.split()
    lines: List[str] = []
    cur = ""

    def text_w(s: str) -> int:
        bbox = draw.textbbox((0, 0), s, font=font)
        return bbox[2] - bbox[0]

    for w in words:
        test = (cur + " " + w).strip()
        if text_w(test) <= max_width:
            cur = test
        else:
            if cur:
                lines.append(cur)
                cur = w
            else:
                # 단어 자체가 너무 길면 글자 단위로 쪼갬
                chunk = ""
                for ch in w:
                    test2 = chunk + ch
                    if text_w(test2) <= max_width:
                        chunk = test2
                    else:
                        if chunk:
                            lines.append(chunk)
                        chunk = ch
                cur = chunk
    if cur:
        lines.append(cur)
    return lines

def create_text_capture_image(review_id: str, product_name: str, score: int, created_at: str, text: str) -> str:
    """
    생성한 파일의 site 기준 상대경로 반환: assets/text_images/xxx.png
    """
    # 캔버스 스타일
    W = 980
    PAD = 44
    HEADER_H = 120
    BG = (246, 248, 251)
    CARD = (255, 255, 255)
    TXT = (15, 23, 42)
    MUTED = (100, 116, 139)

    out_name = safe_filename(sha1(str(review_id)) + ".png")
    out_path = ASSET_TEXT / out_name

    # 폰트: 가능한 경우 나눔고딕(로컬 설치되어 있으면) → 없으면 default
    try:
        font_title = ImageFont.truetype("NanumGothic.ttf", 30)
        font_meta = ImageFont.truetype("NanumGothic.ttf", 22)
        font_body = ImageFont.truetype("NanumGothic.ttf", 26)
    except Exception:
        font_title = ImageFont.load_default()
        font_meta = ImageFont.load_default()
        font_body = ImageFont.load_default()

    # 임시 캔버스에서 라인 계산
    tmp = Image.new("RGB", (W, 800), BG)
    dtmp = ImageDraw.Draw(tmp)

    max_text_width = W - PAD * 2
    lines = wrap_text(dtmp, text, font_body, max_text_width)

    line_h = 34
    body_h = max(240, len(lines) * line_h + 40)
    H = HEADER_H + body_h + PAD

    img = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)

    # 카드 영역
    card_x0, card_y0 = PAD, PAD
    card_x1, card_y1 = W - PAD, H - PAD
    draw.rounded_rectangle([card_x0, card_y0, card_x1, card_y1], radius=28, fill=CARD)

    # 헤더
    title = (product_name or "상품명 없음")[:40]
    meta = f"★ {score}  ·  {created_at[:10] if created_at else ''}  ·  review_id={review_id}"

    draw.text((card_x0 + 28, card_y0 + 24), title, font=font_title, fill=TXT)
    draw.text((card_x0 + 28, card_y0 + 70), meta, font=font_meta, fill=MUTED)

    # 본문
    y = card_y0 + HEADER_H
    x = card_x0 + 28
    for ln in lines:
        draw.text((x, y), ln, font=font_body, fill=TXT)
        y += line_h

    img.save(out_path)
    return str(out_path.relative_to(SITE_DIR)).replace("\\", "/")

# ----------------------------
# Flatten review
# ----------------------------

def flatten_review(r: Dict[str, Any], csv_name: str = "", csv_url: str = "") -> Dict[str, Any]:
    images = r.get("images") or []
    thumbs = [img.get("thumbnail_url") for img in images if img.get("thumbnail_url")]
    fulls = [img.get("url") for img in images if img.get("url")]

    ro = r.get("review_options") or []
    ro_map = {x.get("name"): x.get("value") for x in ro if x.get("name")}

    po = r.get("product_options") or []
    po_map = {x.get("name"): x.get("value") for x in po if x.get("name")}

    msg = (r.get("filtered_message") or "").strip()
    score = int(r.get("score") or 0)

    # 옵션 사이즈: product_options '사이즈' 우선, 없으면 review_options '평소사이즈'
    opt_size = (po_map.get("사이즈") or ro_map.get("평소사이즈") or "").strip()
    opt_color = (po_map.get("컬러") or "").strip()
    fit_q = (ro_map.get("사이즈 어때요?") or "").strip()

    product_name = (r.get("product_name") or "").strip() or (csv_name or "").strip()
    product_url = (r.get("product_url") or "").strip() or (csv_url or "").strip()

    # tags
    tags: List[str] = []
    if score <= 2:
        tags.append("low")
    if has_any_kw(msg, SIZE_KEYWORDS) or fit_q in ("조금 작아요", "작아요", "조금 커요", "커요"):
        tags.append("size")
    if has_any_kw(msg, POS_KEYWORDS) and score >= 4:
        tags.append("pos")
    if has_any_kw(msg, REQ_KEYWORDS):
        tags.append("req")

    size_dir = classify_size_direction(msg)

    return {
        "id": r.get("id"),
        "product_code": r.get("product_code"),
        "product_name": product_name,
        "product_url": product_url,
        "rating": score,
        "created_at": r.get("created_at"),
        "text": msg,
        "source": "Official",  # Crema 자체는 공식몰/외부유입 섞일 수 있으나, 일단 Official로 둠
        "option_size": opt_size,
        "option_color": opt_color,
        "fit_q": fit_q,
        "height": ro_map.get("키") or "",
        "weight": ro_map.get("몸무게") or "",
        "tags": tags,
        "size_direction": size_dir,
        # images
        "review_thumb_url": thumbs[0] if thumbs else "",
        "review_image_url": fulls[0] if fulls else "",
        "product_image_source_url": r.get("product_image_source_url") or "",
        "product_image_url": r.get("product_image_url") or "",
    }

# ----------------------------
# Asset localization
# ----------------------------

def download_to_assets(url: str, kind_dir: pathlib.Path) -> str:
    if not url or not isinstance(url, str) or not url.startswith("http"):
        return ""
    name = safe_filename(sha1(url) + url_ext(url))
    out_path = kind_dir / name
    ok = http.download(url, out_path)
    if not ok:
        return ""
    return str(out_path.relative_to(SITE_DIR)).replace("\\", "/")

# ----------------------------
# HTML template (YOUR PROVIDED HTML, with JS replaced)
# ----------------------------

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>VOC Dashboard | Official + Naver Reviews</title>

  <!-- Tailwind + FontAwesome -->
  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">

  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200;400;600;800&display=swap');
    :root { --brand:#002d72; --bg0:#f6f8fb; --bg1:#eef3f9; }

    body{
      background: linear-gradient(180deg, var(--bg0), var(--bg1));
      font-family: 'Plus Jakarta Sans', sans-serif;
      color:#0f172a;
      min-height:100vh;
    }

    /* Glass system */
    .glass-card{
      background: rgba(255,255,255,0.55);
      backdrop-filter: blur(20px);
      border: 1px solid rgba(255,255,255,0.75);
      border-radius: 30px;
      box-shadow: 0 20px 50px rgba(0,45,114,0.05);
    }
    .sidebar{
      background: rgba(255,255,255,0.70);
      backdrop-filter: blur(15px);
      border-right: 1px solid rgba(255,255,255,0.80);
    }
    .summary-card{
      border-radius: 26px;
      background: rgba(255,255,255,0.55);
      border: 1px solid rgba(255,255,255,0.75);
      backdrop-filter: blur(18px);
      box-shadow: 0 20px 50px rgba(0,45,114,0.05);
      padding: 18px 20px;
    }
    .small-label{
      font-size: 10px;
      letter-spacing: 0.3em;
      text-transform: uppercase;
      font-weight: 900;
    }
    .input-glass{
      background: rgba(255,255,255,0.65);
      border: 1px solid rgba(255,255,255,0.80);
      border-radius: 18px;
      padding: 12px 14px;
      outline: none;
      font-weight: 800;
      color:#0f172a;
    }
    .input-glass:focus{
      box-shadow: 0 0 0 4px rgba(0,45,114,0.10);
      border-color: rgba(0,45,114,0.25);
    }
    .chip{
      border-radius: 9999px;
      padding: 10px 14px;
      font-weight: 900;
      font-size: 12px;
      border: 1px solid rgba(255,255,255,0.85);
      background: rgba(255,255,255,0.60);
      color:#334155;
      cursor:pointer;
      user-select:none;
    }
    .chip.active{
      background: rgba(0,45,114,0.95);
      color:#fff;
      border-color: rgba(0,45,114,1);
      box-shadow: 0 10px 30px rgba(0,45,114,0.15);
    }

    /* Tab buttons */
    .tab-btn{
      padding: 10px 14px;
      border-radius: 18px;
      font-weight: 900;
      font-size: 12px;
      border: 1px solid rgba(255,255,255,0.85);
      background: rgba(255,255,255,0.60);
      color:#475569;
      transition: all .15s ease;
    }
    .tab-btn:hover{ background: rgba(255,255,255,0.90); }
    .tab-btn.active{
      background: rgba(0,45,114,0.95);
      color:#fff;
      border-color: rgba(0,45,114,1);
      box-shadow: 0 10px 30px rgba(0,45,114,0.15);
    }

    /* overlay */
    .overlay{
      position: fixed;
      inset:0;
      background: rgba(255,255,255,0.65);
      backdrop-filter: blur(10px);
      display:none;
      align-items:center;
      justify-content:center;
      z-index:9999;
    }
    .overlay.show{ display:flex; }
    .spinner{
      width:56px;height:56px;border-radius:9999px;
      border:6px solid rgba(0,0,0,0.08);
      border-top-color: rgba(0,45,114,0.95);
      animation: spin .9s linear infinite;
    }
    @keyframes spin { to { transform: rotate(360deg);} }

    /* Tables */
    .tbl{
      width:100%;
      border-collapse: separate;
      border-spacing: 0;
      overflow:hidden;
      border-radius: 22px;
      border: 1px solid rgba(255,255,255,0.85);
      background: rgba(255,255,255,0.55);
    }
    .tbl th{
      font-size: 11px;
      letter-spacing: .22em;
      text-transform: uppercase;
      font-weight: 900;
      color:#475569;
      background: rgba(255,255,255,0.75);
      padding: 14px 14px;
      position: sticky;
      top: 0;
      z-index: 1;
    }
    .tbl td{
      padding: 14px 14px;
      border-top: 1px solid rgba(255,255,255,0.75);
      font-weight: 800;
      color:#0f172a;
      font-size: 13px;
      vertical-align: top;
    }
    .tbl .muted{ color:#64748b; font-weight:800; font-size:12px; }

    /* Review cards */
    .review-card{
      border-radius: 26px;
      background: rgba(255,255,255,0.55);
      border: 1px solid rgba(255,255,255,0.80);
      backdrop-filter: blur(18px);
      box-shadow: 0 16px 42px rgba(0,45,114,0.04);
      padding: 18px 18px;
    }
    .badge{
      display:inline-flex;
      align-items:center;
      gap:6px;
      padding: 6px 10px;
      border-radius: 9999px;
      font-size: 11px;
      font-weight: 900;
      border: 1px solid rgba(255,255,255,0.85);
      background: rgba(255,255,255,0.65);
      color:#334155;
    }
    .badge.neg{ background: rgba(239,68,68,0.10); color:#b91c1c; border-color: rgba(239,68,68,0.18); }
    .badge.pos{ background: rgba(16,185,129,0.10); color:#047857; border-color: rgba(16,185,129,0.18); }
    .badge.size{ background: rgba(59,130,246,0.10); color:#1d4ed8; border-color: rgba(59,130,246,0.18); }

    /* image */
    .img-box{ width:72px; height:72px; border-radius:18px; overflow:hidden; background: rgba(255,255,255,0.70); border:1px solid rgba(255,255,255,0.85); }
    .img-box img{ width:100%; height:100%; object-fit:cover; display:block; }

    /* Two-pane layout for review list */
    .review-list{
      display:grid;
      grid-template-columns: 1fr;
      gap: 14px;
    }
    @media (min-width: 1024px){
      .review-list{ grid-template-columns: 1fr 1fr; }
    }

    /* Embedded mode (optional) */
    body.embedded aside, body.embedded header { display:none !important; }
    body.embedded main{ padding: 24px !important; }

    /* clamp helpers */
    .line-clamp-1{
      overflow:hidden; display:-webkit-box; -webkit-line-clamp:1; -webkit-box-orient:vertical;
    }
    .line-clamp-2{
      overflow:hidden; display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical;
    }
  </style>
</head>

<body class="flex">

  <!-- overlay -->
  <div id="overlay" class="overlay">
    <div class="glass-card px-8 py-7 flex items-center gap-4">
      <div class="spinner"></div>
      <div>
        <div class="text-sm font-black text-slate-900">Processing...</div>
        <div id="overlayMsg" class="text-xs font-bold text-slate-500 mt-1">잠시만요</div>
      </div>
    </div>
  </div>

  <!-- Sidebar -->
  <aside class="w-72 h-screen sticky top-0 sidebar hidden lg:flex flex-col p-8">
    <div class="flex items-center gap-4 mb-14 px-2">
      <div class="w-12 h-12 bg-[color:var(--brand)] rounded-2xl flex items-center justify-center text-white shadow-xl shadow-blue-900/20">
        <i class="fa-solid fa-comments text-xl"></i>
      </div>
      <div>
        <div class="text-xl font-black tracking-tighter italic">VOC <span class="text-blue-600 font-extrabold">HUB</span></div>
        <div class="text-[9px] font-black uppercase tracking-[0.3em] text-slate-400">Official + Naver Reviews</div>
      </div>
    </div>

    <div class="glass-card p-5">
      <div class="small-label text-blue-600 mb-2">Schedule</div>
      <div class="text-sm font-black text-slate-900">주 1회</div>
      <div class="text-xs font-bold text-slate-500 mt-2">월요일 오전 9시 (KST)</div>
      <div class="mt-4 text-xs font-bold text-slate-500">
        * 이 레이아웃은 정적 HTML. <br/>
        실제 데이터는 JSON(data/meta.json, data/reviews.json)에서 로드됩니다.
      </div>
    </div>

    <div class="mt-auto pt-8 text-xs font-bold text-slate-500">
      <div class="small-label text-blue-600 mb-2">Snapshot</div>
      <div>수집일: <span id="runDateSide" class="font-black text-slate-700">-</span></div>
      <div class="mt-2">기간: <span id="dateRangeSide" class="font-black text-slate-700">-</span></div>
    </div>
  </aside>

  <!-- Main -->
  <main class="flex-1 p-8 md:p-14">

    <header class="flex flex-col md:flex-row md:items-center justify-between mb-10 gap-6">
      <div>
        <h1 class="text-4xl md:text-5xl font-black tracking-tight text-slate-900 mb-3">
          Official몰 & Naver 리뷰 VOC 대시보드
        </h1>
        <div id="headerMeta" class="text-sm text-slate-500 font-bold">
          - · - · 주 1회 자동 업데이트(월 09:00)
        </div>
      </div>

      <div class="glass-card px-6 py-4 flex items-center gap-4">
        <div class="flex h-3 w-3 relative">
          <span class="animate-ping absolute h-full w-full rounded-full bg-blue-400 opacity-75"></span>
          <span class="relative inline-flex rounded-full h-3 w-3 bg-blue-600"></span>
        </div>
        <span class="text-sm font-black text-slate-800 tracking-widest uppercase">VOC Snapshot</span>
      </div>
    </header>

    <!-- 0) Tabs -->
    <section class="mb-8">
      <div class="flex flex-wrap gap-2 items-center">
        <button class="tab-btn active" data-tab="combined" onclick="switchSourceTab('combined')">
          Combined <span class="ml-2 opacity-70">공식몰+네이버(1탭)</span>
        </button>
        <button class="tab-btn" data-tab="official" onclick="switchSourceTab('official')">
          Official Mall
        </button>
        <button class="tab-btn" data-tab="naver" onclick="switchSourceTab('naver')">
          Naver
        </button>

        <div class="ml-auto flex items-center gap-3">
          <div class="small-label text-blue-600">View</div>
          <button class="chip active" id="chip-daily" onclick="toggleChip('daily')">당일 업로드 순</button>
          <button class="chip" id="chip-pos" onclick="toggleChip('pos')">긍정 키워드 포함</button>
          <button class="chip" id="chip-size" onclick="toggleChip('size')">사이즈 이슈만</button>
          <button class="chip" id="chip-low" onclick="toggleChip('low')">저평점만</button>
        </div>
      </div>
    </section>

    <!-- 1) Summary -->
    <section class="mb-10">
      <div class="glass-card p-8">
        <div class="flex items-end justify-between gap-6 flex-wrap mb-6">
          <div>
            <div class="small-label text-blue-600 mb-2">1. Summary</div>
            <div class="text-2xl font-black text-slate-900">핵심 이슈 한 장 요약</div>
          </div>
          <div class="text-xs font-black text-slate-500">
            * JSON 데이터 기반 자동 산출
          </div>
        </div>

        <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
          <div class="summary-card">
            <div class="small-label text-blue-600 mb-2">1-1) Size mention</div>
            <div class="text-3xl font-black"><span id="sizeMentionRate">-</span>%</div>
            <div class="text-xs font-bold text-slate-500 mt-2">전체 리뷰 중 사이즈 관련 언급 비중</div>
          </div>

          <div class="summary-card">
            <div class="small-label text-red-600 mb-2">1-2) Complaint Top 5</div>
            <div id="topComplaints" class="mt-2 flex flex-wrap gap-2"></div>
            <div class="text-xs font-bold text-slate-500 mt-3">저평점(≤2) 텍스트 기반 자동 추출</div>
          </div>

          <div class="summary-card">
            <div class="small-label text-blue-600 mb-2">1-3) Priority Top 3</div>
            <ol id="priorityTop3" class="mt-2 space-y-2"></ol>
            <div class="text-xs font-bold text-slate-500 mt-3">개선 필요 제품 Top 3(사이즈 이슈율)</div>
          </div>
        </div>
      </div>
    </section>

    <!-- 2) Priority ranking -->
    <section class="mb-10">
      <div class="glass-card p-8">
        <div class="flex items-end justify-between gap-6 flex-wrap mb-6">
          <div>
            <div class="small-label text-blue-600 mb-2">2. 개선 우선순위 제품 랭킹</div>
          </div>
          <div class="flex gap-2">
            <button class="chip active" id="rank-size" onclick="switchRankMode('size')">2-1) 사이즈 이슈율</button>
            <button class="chip" id="rank-low" onclick="switchRankMode('low')">2-2) 저평점 비중</button>
            <button class="chip" id="rank-both" onclick="switchRankMode('both')">2-3) 교집합</button>
          </div>
        </div>

        <div class="overflow-auto">
          <table class="tbl min-w-[980px]">
            <thead>
              <tr>
                <th class="text-left">제품명</th>
                <th class="text-left">리뷰 수</th>
                <th class="text-left">사이즈 이슈율</th>
                <th class="text-left">저평점 비중</th>
                <th class="text-left">주요 문제 키워드</th>
              </tr>
            </thead>
            <tbody id="rankingBody"></tbody>
          </table>
        </div>
      </div>
    </section>

    <!-- 3) Size issue structure -->
    <section class="mb-10">
      <div class="glass-card p-8">
        <div class="flex items-end justify-between gap-6 flex-wrap mb-6">
          <div>
            <div class="small-label text-blue-600 mb-2">3. 사이즈 이슈 구조 분석</div>
          </div>
        </div>

        <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
          <div class="summary-card">
            <div class="small-label text-blue-600 mb-2">3-1) small vs big</div>
            <div class="flex items-center justify-between mt-3">
              <span class="badge size">too_small <span id="tooSmall">-</span>%</span>
              <span class="badge size">too_big <span id="tooBig">-</span>%</span>
            </div>
            <div class="text-xs font-bold text-slate-500 mt-3">규칙 기반 자동 분류</div>
          </div>

          <div class="summary-card lg:col-span-2">
            <div class="small-label text-blue-600 mb-2">3-2) 옵션(사이즈)별 이슈율</div>
            <div class="overflow-auto mt-3">
              <table class="tbl min-w-[820px]">
                <thead>
                  <tr>
                    <th class="text-left">옵션 사이즈</th>
                    <th class="text-left">리뷰 수</th>
                    <th class="text-left">too_small</th>
                    <th class="text-left">too_big</th>
                    <th class="text-left">정사이즈/기타</th>
                  </tr>
                </thead>
                <tbody id="sizeOptBody"></tbody>
              </table>
            </div>
          </div>
        </div>

        <div class="summary-card mt-4">
          <div class="small-label text-blue-600 mb-2">3-3) 핏 관련 반복 표현</div>
          <div id="fitWords" class="flex flex-wrap gap-2 mt-2"></div>
        </div>
      </div>
    </section>

    <!-- 4) Complaint keywords -->
    <section class="mb-10">
      <div class="glass-card p-8">
        <div class="flex items-end justify-between gap-6 flex-wrap mb-6">
          <div>
            <div class="small-label text-blue-600 mb-2">4. 반복 불만 키워드 분석</div>
          </div>
        </div>

        <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
          <div class="summary-card">
            <div class="small-label text-red-600 mb-2">4-1) 저평점 키워드</div>
            <div id="liftWords" class="mt-3 space-y-2"></div>
          </div>

          <div class="summary-card">
            <div class="small-label text-blue-600 mb-2">4-2) 제품 공통 문제 키워드</div>
            <div id="commonIssues" class="mt-3 flex flex-wrap gap-2"></div>
          </div>

          <div class="summary-card">
            <div class="small-label text-blue-600 mb-2">4-3) 사이즈 이슈 반복 표현</div>
            <div id="sizePhrases" class="mt-3 flex flex-wrap gap-2"></div>
          </div>
        </div>
      </div>
    </section>

    <!-- 5) Evidence reviews -->
    <section class="mb-10">
      <div class="glass-card p-8">
        <div class="flex items-end justify-between gap-6 flex-wrap mb-6">
          <div>
            <div class="small-label text-blue-600 mb-2">5. 대표 근거 리뷰</div>
          </div>

          <div class="flex gap-2">
            <button class="chip active" id="ev-size" onclick="switchEvidence('size')">5-1) 사이즈 이슈</button>
            <button class="chip" id="ev-low" onclick="switchEvidence('low')">5-2) 저평점</button>
            <button class="chip" id="ev-req" onclick="switchEvidence('req')">5-3) 개선 요청</button>
          </div>
        </div>

        <div id="evidenceList" class="review-list"></div>
      </div>
    </section>

    <!-- 6) Daily review feed -->
    <section class="mb-10">
      <div class="glass-card p-8">
        <div class="flex items-end justify-between gap-6 flex-wrap mb-6">
          <div>
            <div class="small-label text-blue-600 mb-2">Daily Feed</div>
            <div class="text-2xl font-black text-slate-900">그날 올라온 리뷰 (업로드 순)</div>
            <div class="text-sm font-bold text-slate-500 mt-2">기본은 “전체 노출”, 필터는 최소화</div>
          </div>
        </div>

        <div class="grid grid-cols-1 lg:grid-cols-4 gap-3 mb-6">
          <select id="productSelect" class="input-glass" onchange="renderAll()">
            <option value="">제품 선택 (전체)</option>
          </select>

          <select id="sizeSelect" class="input-glass" onchange="renderAll()">
            <option value="">옵션 사이즈 (전체)</option>
          </select>

          <select id="sortSelect" class="input-glass" onchange="renderAll()">
            <option value="upload">정렬: 업로드 순 (기본)</option>
            <option value="latest">최신순</option>
            <option value="long">리뷰 길이 긴 순</option>
            <option value="low">저평점순</option>
          </select>

          <input id="qInput" class="input-glass" placeholder="텍스트 검색(옵션)" oninput="renderAll()" />
        </div>

        <div id="dailyFeed" class="space-y-4"></div>

        <div class="hidden review-card text-center" id="noResults">
          <div class="text-lg font-black text-slate-800">검색 결과가 없습니다.</div>
        </div>
      </div>
    </section>

    <footer class="text-xs font-bold text-slate-500 pb-8">
      * 데이터 소스: Crema API(JSON).<br/>
      * 리뷰 텍스트 100자 이상은 PIL로 캡처 이미지를 자동 생성하여 표시합니다.
    </footer>

  </main>

  <script>
    // ----------------------------
    // Overlay helpers
    // ----------------------------
    const overlay = document.getElementById('overlay');
    const overlayMsg = document.getElementById('overlayMsg');

    const uiState = {
      sourceTab: 'combined',
      chips: { daily:true, pos:false, size:false, low:false },
      rankMode: 'size',
      evidenceMode: 'size'
    };

    function showOverlay(msg){
      overlayMsg.textContent = msg || '잠시만요';
      overlay.classList.add('show');
    }
    function hideOverlay(){
      overlay.classList.remove('show');
    }
    function runWithOverlay(msg, fn){
      showOverlay(msg);
      setTimeout(() => { try { fn(); } finally { requestAnimationFrame(hideOverlay); } }, 0);
    }

    function switchSourceTab(tab){
      runWithOverlay('Switching source...', () => {
        uiState.sourceTab = tab;
        document.querySelectorAll('.tab-btn').forEach(b => {
          b.classList.toggle('active', b.getAttribute('data-tab') === tab);
        });
        renderAll();
      });
    }

    function toggleChip(key){
      runWithOverlay('Applying filter...', () => {
        uiState.chips[key] = !uiState.chips[key];
        const el = document.getElementById('chip-' + key);
        if (el) el.classList.toggle('active', uiState.chips[key]);
        renderAll();
      });
    }

    function switchRankMode(mode){
      runWithOverlay('Switching ranking...', () => {
        uiState.rankMode = mode;
        document.getElementById('rank-size').classList.toggle('active', mode==='size');
        document.getElementById('rank-low').classList.toggle('active', mode==='low');
        document.getElementById('rank-both').classList.toggle('active', mode==='both');
        renderAll();
      });
    }

    function switchEvidence(mode){
      runWithOverlay('Switching evidence...', () => {
        uiState.evidenceMode = mode;
        document.getElementById('ev-size').classList.toggle('active', mode==='size');
        document.getElementById('ev-low').classList.toggle('active', mode==='low');
        document.getElementById('ev-req').classList.toggle('active', mode==='req');
        renderAll();
      });
    }

    // Embedded mode
    (function () {
      try { if (window.self !== window.top) document.body.classList.add("embedded"); }
      catch (e) { document.body.classList.add("embedded"); }
    })();

    // ----------------------------
    // Data store (loaded from JSON)
    // ----------------------------
    let META = null;
    let REVIEWS = [];

    const esc = (s) => String(s ?? "")
      .replaceAll("&","&amp;")
      .replaceAll("<","&lt;")
      .replaceAll(">","&gt;")
      .replaceAll('"',"&quot;")
      .replaceAll("'","&#039;");

    const fmtDT = (iso) => {
      if (!iso) return "";
      const d = new Date(iso);
      const pad2 = (n) => String(n).padStart(2,'0');
      return `${d.getFullYear()}.${pad2(d.getMonth()+1)}.${pad2(d.getDate())} ${pad2(d.getHours())}:${pad2(d.getMinutes())}`;
    };

    function asArr(x){ return Array.isArray(x) ? x : []; }

    // ----------------------------
    // Filtering by UI state
    // ----------------------------
    function getFilteredReviews(){
      let rows = REVIEWS.slice();

      // source tab: 현재 Crema는 일단 Official로 저장해둠.
      // 네이버까지 붙이면, 여기서 source === "Naver" 필터가 동작하도록 JSON만 확장하면 됨.
      if (uiState.sourceTab === "official") rows = rows.filter(r => r.source === "Official");
      if (uiState.sourceTab === "naver") rows = rows.filter(r => r.source === "Naver");

      // product dropdown
      const productCode = document.getElementById("productSelect")?.value || "";
      if (productCode) rows = rows.filter(r => r.product_code === productCode);

      // size dropdown
      const sizeOpt = document.getElementById("sizeSelect")?.value || "";
      if (sizeOpt) rows = rows.filter(r => (r.option_size || "") === sizeOpt);

      // query
      const q = (document.getElementById("qInput")?.value || "").trim().toLowerCase();
      if (q){
        rows = rows.filter(r =>
          (r.product_name || "").toLowerCase().includes(q) ||
          (r.product_code || "").toLowerCase().includes(q) ||
          (r.text || "").toLowerCase().includes(q) ||
          (r.option_size || "").toLowerCase().includes(q)
        );
      }

      // chips
      if (uiState.chips.pos) rows = rows.filter(r => asArr(r.tags).includes("pos"));
      if (uiState.chips.size) rows = rows.filter(r => asArr(r.tags).includes("size"));
      if (uiState.chips.low) rows = rows.filter(r => (r.rating ?? 0) <= 2 || asArr(r.tags).includes("low"));

      // sort
      const sort = document.getElementById("sortSelect")?.value || "upload";
      if (sort === "latest") rows.sort((a,b) => new Date(b.created_at) - new Date(a.created_at));
      else if (sort === "long") rows.sort((a,b) => ((b.text||"").length - (a.text||"").length));
      else if (sort === "low") rows.sort((a,b) => ((a.rating||0) - (b.rating||0)) || (new Date(b.created_at)-new Date(a.created_at)));
      else rows.sort((a,b) => new Date(a.created_at) - new Date(b.created_at)); // upload order

      return rows;
    }

    // ----------------------------
    // Metrics calc (real)
    // ----------------------------
    function calcMetrics(reviews){
      const total = reviews.length || 1;
      const sizeMention = reviews.filter(r => asArr(r.tags).includes("size")).length;

      // size direction split
      const sizeRows = reviews.filter(r => asArr(r.tags).includes("size"));
      const smallCnt = sizeRows.filter(r => r.size_direction === "too_small").length;
      const bigCnt = sizeRows.filter(r => r.size_direction === "too_big").length;
      const denom = Math.max(1, smallCnt + bigCnt);
      const tooSmall = Math.round((smallCnt/denom)*100);
      const tooBig = 100 - tooSmall;

      // product-level stats
      const byProd = new Map();
      for (const r of reviews){
        const code = r.product_code || "-";
        if (!byProd.has(code)){
          byProd.set(code, {
            product_code: code,
            product_name: r.product_name || code,
            product_url: r.product_url || "",
            local_product_image: r.local_product_image || "",
            reviews: 0,
            sizeIssue: 0,
            low: 0,
            issueKwds: new Map()
          });
        }
        const g = byProd.get(code);
        g.reviews += 1;
        if (asArr(r.tags).includes("size")) g.sizeIssue += 1;
        if ((r.rating||0) <= 2) g.low += 1;

        // issue keywords: meta.top5 + 간단 토큰 기반 (너무 과하면 UI 지저분해져서 상위만)
        if ((r.rating||0) <= 2){
          const words = (r.text||"").match(/[가-힣A-Za-z0-9]{2,}/g) || [];
          for (const w of words.slice(0, 40)){
            const k = w.toLowerCase();
            if (k.length < 2) continue;
            g.issueKwds.set(k, (g.issueKwds.get(k)||0) + 1);
          }
        }
      }

      const rows = Array.from(byProd.values()).map(g => {
        const sizeRate = Math.round((g.sizeIssue / Math.max(1,g.reviews))*100);
        const lowRate  = Math.round((g.low / Math.max(1,g.reviews))*100);
        const kwds = Array.from(g.issueKwds.entries())
          .sort((a,b)=>b[1]-a[1])
          .slice(0,4)
          .map(x=>x[0])
          .join(", ");
        return { ...g, sizeRate, lowRate, kwds };
      });

      const rankSize = rows.slice().sort((a,b)=> b.sizeRate - a.sizeRate || b.reviews - a.reviews);
      const rankLow  = rows.slice().sort((a,b)=> b.lowRate  - a.lowRate  || b.reviews - a.reviews);
      const rankBoth = rows.slice().sort((a,b)=> ((b.sizeRate+b.lowRate) - (a.sizeRate+a.lowRate)) || b.reviews - a.reviews);

      // size option table
      const sizeMap = new Map();
      for (const r of reviews){
        const sz = (r.option_size || "").trim();
        if (!sz) continue;
        if (!sizeMap.has(sz)){
          sizeMap.set(sz, { sz, cnt:0, small:0, big:0, other:0 });
        }
        const o = sizeMap.get(sz);
        o.cnt += 1;
        if (asArr(r.tags).includes("size")){
          if (r.size_direction === "too_small") o.small += 1;
          else if (r.size_direction === "too_big") o.big += 1;
          else o.other += 1;
        } else {
          o.other += 1;
        }
      }
      const sizeOpts = Array.from(sizeMap.values())
        .sort((a,b)=> b.cnt - a.cnt)
        .map(x => {
          const cnt = Math.max(1, x.cnt);
          const smallP = Math.round((x.small/cnt)*100);
          const bigP = Math.round((x.big/cnt)*100);
          const okP = Math.max(0, 100 - smallP - bigP);
          return { sz:x.sz, cnt:x.cnt, small:smallP, big:bigP, ok:okP };
        });

      return {
        total,
        sizeMentionRate: Math.round((sizeMention/total)*100),
        tooSmall,
        tooBig,
        rankSize,
        rankLow,
        rankBoth,
        sizeOpts
      };
    }

    // ----------------------------
    // Renderers
    // ----------------------------
    function renderHeader(){
      const runDate = META?.updated_at || "-";
      const dateRange = META?.date_range || "-";
      document.getElementById("runDateSide").textContent = runDate.slice(0,10).replaceAll("-",".");
      document.getElementById("dateRangeSide").textContent = dateRange;
      document.getElementById("headerMeta").textContent = `${runDate} · ${dateRange} · 주 1회 자동 업데이트(월 09:00)`;
    }

    function renderSummary(metrics){
      document.getElementById("sizeMentionRate").textContent = metrics.sizeMentionRate;

      const top = META?.complaint_top5 || [];
      const el = document.getElementById("topComplaints");
      el.innerHTML = top.map(([k,c]) => `<span class="badge neg">#${esc(k)} <span class="opacity-70">${esc(c)}</span></span>`).join("");

      const top3 = metrics.rankSize.slice(0,3);
      const ol = document.getElementById("priorityTop3");
      ol.innerHTML = top3.map(r => `
        <li class="flex items-center justify-between gap-3">
          <span class="font-black text-slate-900">${esc(r.product_name)}</span>
          <span class="badge size">Size ${r.sizeRate}%</span>
        </li>
      `).join("");
    }

    function renderRanking(metrics){
      let rows = [];
      if (uiState.rankMode === "size") rows = metrics.rankSize;
      else if (uiState.rankMode === "low") rows = metrics.rankLow;
      else rows = metrics.rankBoth;

      const tbody = document.getElementById("rankingBody");
      tbody.innerHTML = rows.slice(0, 50).map(r => `
        <tr>
          <td>
            <div class="flex items-center gap-3">
              <div class="img-box">
                ${r.local_product_image ? `<img src="${esc(r.local_product_image)}" alt="">` : `<div class="w-full h-full flex items-center justify-center text-[10px] text-slate-400">NO IMAGE</div>`}
              </div>
              <div>
                <div class="font-black text-slate-900">${esc(r.product_name)}</div>
                <div class="muted">code: ${esc(r.product_code)}</div>
                ${r.product_url ? `<a href="${esc(r.product_url)}" target="_blank" class="text-xs font-black text-blue-600 hover:underline">상품 페이지</a>` : ``}
              </div>
            </div>
          </td>
          <td class="muted">${r.reviews}</td>
          <td><span class="badge size">${r.sizeRate}%</span></td>
          <td><span class="badge neg">${r.lowRate}%</span></td>
          <td class="muted">${esc(r.kwds || "-")}</td>
        </tr>
      `).join("");
    }

    function renderSizeStructure(metrics){
      document.getElementById("tooSmall").textContent = metrics.tooSmall;
      document.getElementById("tooBig").textContent = metrics.tooBig;

      const sizeBody = document.getElementById("sizeOptBody");
      sizeBody.innerHTML = metrics.sizeOpts.map(x => `
        <tr>
          <td class="font-black">${esc(x.sz)}</td>
          <td class="muted">${x.cnt}</td>
          <td><span class="badge size">${x.small}%</span></td>
          <td><span class="badge size">${x.big}%</span></td>
          <td class="muted">${x.ok}%</td>
        </tr>
      `).join("");

      // fitWords: meta에서 제공한 fit_words (없으면 기본)
      const fitWords = META?.fit_words || ["정사이즈","한치수 크게","한치수 작게","기장","소매","어깨","가슴","발볼"];
      const fit = document.getElementById("fitWords");
      fit.innerHTML = fitWords.map(w => `<span class="badge">${esc(w)}</span>`).join("");
    }

    function renderKeywords(){
      // liftWords / commonIssues / sizePhrases 는 meta에 기본 제공 (없으면 fallback)
      const liftWords = META?.low_keywords || [];
      const lift = document.getElementById("liftWords");
      lift.innerHTML = liftWords.slice(0,5).map(x => `
        <div class="flex items-center justify-between">
          <span class="badge neg">#${esc(x[0])}</span>
          <span class="muted">${esc(x[1])}</span>
        </div>
      `).join("");

      const common = document.getElementById("commonIssues");
      const commonIssues = META?.common_issues || (META?.complaint_top5 || []).map(x=>x[0]);
      common.innerHTML = commonIssues.map(w => `<span class="badge neg">#${esc(w)}</span>`).join("");

      const sizeP = document.getElementById("sizePhrases");
      const sizePhrases = META?.size_phrases || ["정사이즈","작아요","커요","타이트","넉넉","기장","소매","어깨","발볼"];
      sizeP.innerHTML = sizePhrases.map(w => `<span class="badge size">#${esc(w)}</span>`).join("");
    }

    function reviewCardHTML(r){
      const tags = [];
      const t = asArr(r.tags);

      if (t.includes("pos")) tags.push(`<span class="badge pos"><i class="fa-solid fa-face-smile"></i> #긍정키워드</span>`);
      if (t.includes("size")) tags.push(`<span class="badge size"><i class="fa-solid fa-ruler"></i> #size_issue</span>`);
      if ((r.rating||0) <= 2 || t.includes("low")) tags.push(`<span class="badge neg"><i class="fa-solid fa-triangle-exclamation"></i> #low_rating</span>`);
      if (r.text_image_path) tags.push(`<span class="badge"><i class="fa-solid fa-image"></i> #100자+이미지</span>`);

      const prodImg = r.local_product_image
        ? `<img src="${esc(r.local_product_image)}" alt="">`
        : `<div class="w-full h-full flex items-center justify-center text-[10px] text-slate-400">NO IMAGE</div>`;

      const reviewThumb = r.local_review_thumb
        ? `<img src="${esc(r.local_review_thumb)}" class="w-full max-h-56 object-contain rounded-lg bg-slate-50" />`
        : ``;

      const textImg = r.text_image_path
        ? `<img src="${esc(r.text_image_path)}" class="w-full max-h-72 object-contain rounded-2xl bg-slate-50 border border-white/80" />`
        : `<div class="rounded-2xl border border-white/80 bg-white/60 p-3 flex items-center gap-3">
             <i class="fa-solid fa-image text-slate-400"></i>
             <div class="text-xs font-bold text-slate-500">해당 없음</div>
           </div>`;

      return `
        <div class="review-card">
          <div class="flex items-start justify-between gap-3">
            <div class="flex items-center gap-3 min-w-0">
              <div class="img-box">${prodImg}</div>
              <div class="min-w-0">
                <div class="font-black text-slate-900 line-clamp-1">${esc(r.product_name || r.product_code)}</div>
                <div class="text-xs font-bold text-slate-500 mt-1">
                  code: ${esc(r.product_code)} · source: ${esc(r.source || "-")}
                </div>
              </div>
            </div>
            <div class="text-right">
              <div class="text-xs font-black text-slate-700">★ ${esc(r.rating)}</div>
              <div class="text-[11px] font-bold text-slate-500 mt-1">${esc(fmtDT(r.created_at))}</div>
            </div>
          </div>

          <div class="mt-3 flex flex-wrap gap-2">
            ${(r.option_size ? `<span class="badge">옵션: ${esc(r.option_size)}</span>` : ``)}
            ${tags.join("")}
          </div>

          <div class="mt-3 text-sm font-extrabold text-slate-800 leading-relaxed whitespace-pre-wrap break-words">
            ${esc(r.text || "")}
          </div>

          ${reviewThumb ? `<div class="mt-3">${reviewThumb}</div>` : ``}

          <div class="mt-4">
            <div class="small-label text-blue-600 mb-2">100+ TEXT IMAGE</div>
            ${textImg}
          </div>
        </div>
      `;
    }

    function renderEvidence(reviews){
      const mode = uiState.evidenceMode;
      let list = reviews.slice();

      if (mode === "size") list = list.filter(r => asArr(r.tags).includes("size"));
      else if (mode === "low") list = list.filter(r => (r.rating||0) <= 2 || asArr(r.tags).includes("low"));
      else list = list.filter(r => asArr(r.tags).includes("req"));

      list.sort((a,b) => ((b.text||"").length - (a.text||"").length) || (new Date(b.created_at)-new Date(a.created_at)));
      const pick = list.slice(0, 6);

      document.getElementById("evidenceList").innerHTML = pick.map(reviewCardHTML).join("");
    }

    function renderDailyFeed(reviews){
      const container = document.getElementById("dailyFeed");
      const no = document.getElementById("noResults");

      const rows = reviews.slice(0, 18);
      if (!rows.length){
        container.innerHTML = "";
        no.classList.remove("hidden");
        return;
      }
      no.classList.add("hidden");
      container.innerHTML = rows.map(reviewCardHTML).join("");
    }

    function renderProductSelect(){
      const sel = document.getElementById("productSelect");
      if (!sel) return;
      const current = sel.value;

      const map = new Map();
      for (const r of REVIEWS){
        const code = r.product_code || "";
        if (!code) continue;
        if (!map.has(code)) map.set(code, r.product_name || code);
      }

      const options = [`<option value="">제품 선택 (전체)</option>`].concat(
        Array.from(map.entries()).sort((a,b)=> String(a[1]).localeCompare(String(b[1]))).map(([code,name]) =>
          `<option value="${esc(code)}">${esc(name)} (${esc(code)})</option>`
        )
      ).join("");

      sel.innerHTML = options;
      sel.value = current;
    }

    function renderSizeSelect(){
      const sel = document.getElementById("sizeSelect");
      if (!sel) return;
      const current = sel.value;

      const set = new Set();
      for (const r of REVIEWS){
        const sz = (r.option_size || "").trim();
        if (sz) set.add(sz);
      }
      const opts = Array.from(set).sort((a,b)=> String(a).localeCompare(String(b)));

      sel.innerHTML = [`<option value="">옵션 사이즈 (전체)</option>`]
        .concat(opts.map(sz => `<option value="${esc(sz)}">${esc(sz)}</option>`))
        .join("");

      sel.value = current;
    }

    // ----------------------------
    // Main render orchestrator
    // ----------------------------
    function renderAll(){
      const filtered = getFilteredReviews();
      const metrics = calcMetrics(filtered);

      renderHeader();
      renderProductSelect();
      renderSizeSelect();
      renderSummary(metrics);
      renderRanking(metrics);
      renderSizeStructure(metrics);
      renderKeywords();
      renderEvidence(filtered);
      renderDailyFeed(filtered);
    }

    async function boot(){
      runWithOverlay("Loading data...", async () => {
        const [meta, reviews] = await Promise.all([
          fetch("data/meta.json", {cache:"no-store"}).then(r => r.json()),
          fetch("data/reviews.json", {cache:"no-store"}).then(r => r.json())
        ]);

        META = meta;
        REVIEWS = (reviews && reviews.reviews) ? reviews.reviews : [];

        renderProductSelect();
        renderSizeSelect();
        renderAll();
      });
    }

    document.addEventListener("DOMContentLoaded", boot);
  </script>
</body>
</html>
"""

# ----------------------------
# Main
# ----------------------------

def load_products_csv(path: pathlib.Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"products.csv not found: {path}")
    df = pd.read_csv(path, dtype=str).fillna("")
    if "product_code" not in df.columns:
        raise ValueError("products.csv에 product_code 컬럼이 필요합니다.")
    df["product_code"] = df["product_code"].astype(str).str.strip()
    df = df[df["product_code"] != ""].copy()

    if "product_name" not in df.columns:
        df["product_name"] = ""
    if "product_url" not in df.columns:
        df["product_url"] = ""

    return df

def write_meta_json(reports_dir: str = "reports", period_days: int = 7, tz_name: str = "Asia/Seoul"):
    kst = tz.gettz(tz_name)
    now = datetime.now(tz=kst)
    end = now
    start = now - timedelta(days=period_days - 1)

    meta = {
        "updated_at_kst": now.strftime("%Y.%m.%d %H:%M KST"),
        "period_text": f"최근 {period_days}일 ({start.strftime('%Y.%m.%d')} ~ {end.strftime('%Y.%m.%d')})"
    }

    p = Path(reports_dir)
    p.mkdir(parents=True, exist_ok=True)
    (p / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

def main():
    products = load_products_csv(CONFIG_DIR / "products.csv")
    codes = products["product_code"].dropna().unique().tolist()

    all_reviews: List[Dict[str, Any]] = []

    print(f"[INFO] Products: {len(codes)}")
    print(f"[INFO] Fetching crema reviews from: {BASE_API}")
    print(f"[INFO] widget_id={DEFAULT_WIDGET_ID}, per={DEFAULT_PER_PAGE}")

    # index for csv enrichment
    meta_by_code = {row["product_code"]: {"name": row.get("product_name",""), "url": row.get("product_url","")} for _, row in products.iterrows()}

    for code in tqdm(codes, desc="Products"):
        revs = fetch_all_pages(product_code=code, per=DEFAULT_PER_PAGE, widget_id=DEFAULT_WIDGET_ID)
        for r in revs:
            csv_name = meta_by_code.get(code, {}).get("name","")
            csv_url = meta_by_code.get(code, {}).get("url","")
            all_reviews.append(flatten_review(r, csv_name=csv_name, csv_url=csv_url))

    # dedupe by id
    df = pd.DataFrame(all_reviews)
    if len(df) == 0:
        raise SystemExit("수집된 리뷰가 없습니다. 토큰/상품코드/접근 정책 확인 필요")

    df = df.drop_duplicates(subset=["id"], keep="first").copy()

    # asset localization + text image generation
    local_product_images: Dict[str, str] = {}
    local_review_thumbs: List[str] = []
    text_images: List[str] = []

    # date range
    created_list = df["created_at"].dropna().astype(str).tolist()
    dmin = min(created_list) if created_list else ""
    dmax = max(created_list) if created_list else ""
    date_range = f"{dmin[:10]} ~ {dmax[:10]}" if dmin and dmax else "-"

    print("[INFO] Downloading images + generating text captures...")
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Assets"):
        # product image: source_url 우선
        prod_url = row.get("product_image_source_url") or row.get("product_image_url") or ""
        pcode = row.get("product_code") or ""
        prod_local = ""

        # 제품별 1회만 다운로드해도 됨
        if pcode and pcode in local_product_images:
            prod_local = local_product_images[pcode]
        else:
            prod_local = download_to_assets(prod_url, ASSET_PRODUCTS)
            if pcode:
                local_product_images[pcode] = prod_local

        # review thumb download
        thumb_url = row.get("review_thumb_url") or ""
        thumb_local = download_to_assets(thumb_url, ASSET_REVIEWS) if thumb_url else ""
        local_review_thumbs.append(thumb_local)

        # 100자+ 텍스트 캡처 이미지
        text = row.get("text") or ""
        cap = ""
        if len(text) >= 100:
            cap = create_text_capture_image(
                review_id=str(row.get("id")),
                product_name=str(row.get("product_name") or ""),
                score=int(row.get("rating") or 0),
                created_at=str(row.get("created_at") or ""),
                text=str(text),
            )
        text_images.append(cap)

        # backfill into df
        df.loc[row.name, "local_product_image"] = prod_local
        df.loc[row.name, "local_review_thumb"] = thumb_local
        df.loc[row.name, "text_image_path"] = cap

    # complaint top5: low rating(<=2) texts
    low_texts = df[df["rating"] <= 2]["text"].astype(str).tolist()
    complaint_top5 = top_terms(low_texts, topk=5)

    # extra keyword packs for UI chips (optional, but looks good)
    # - common_issues: top 10 from low texts
    common_issues = [k for k,_ in top_terms(low_texts, topk=10)]
    # - low_keywords: same as complaint_top5 (UI section 4-1 uses it)
    low_keywords = complaint_top5
    # - size_phrases: top 10 tokens from size-tag reviews
    size_texts = df[df["tags"].apply(lambda x: isinstance(x, list) and ("size" in x))]["text"].astype(str).tolist()
    size_phrases = [k for k,_ in top_terms(size_texts, topk=10)]

    meta_json = {
        "updated_at": now_kst().strftime("%Y-%m-%d %H:%M:%S"),
        "date_range": date_range,
        "total_reviews": int(len(df)),
        "complaint_top5": complaint_top5,
        "common_issues": common_issues,
        "low_keywords": low_keywords,
        "size_phrases": size_phrases,
        "fit_words": ["정사이즈","한치수 크게","한치수 작게","타이트","넉넉","기장","소매","어깨","가슴","발볼"],
    }

    # reviews.json: HTML이 사용하는 스키마로 export
    out_reviews = []
    for _, r in df.iterrows():
        out_reviews.append({
            "id": int(r["id"]) if str(r["id"]).isdigit() else r["id"],
            "product_code": r.get("product_code",""),
            "product_name": r.get("product_name",""),
            "product_url": r.get("product_url",""),
            "rating": int(r.get("rating") or 0),
            "created_at": r.get("created_at",""),
            "text": r.get("text",""),
            "source": r.get("source","Official"),
            "option_size": r.get("option_size",""),
            "option_color": r.get("option_color",""),
            "tags": r.get("tags", []),
            "size_direction": r.get("size_direction","other"),
            "local_product_image": r.get("local_product_image",""),
            "local_review_thumb": r.get("local_review_thumb",""),
            "text_image_path": r.get("text_image_path",""),
        })

    (SITE_DATA_DIR / "meta.json").write_text(json.dumps(meta_json, ensure_ascii=False, indent=2), encoding="utf-8")
    (SITE_DATA_DIR / "reviews.json").write_text(json.dumps({"reviews": out_reviews}, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] meta.json / reviews.json saved under: {SITE_DATA_DIR}")

    # write final HTML
    (SITE_DIR / "index.html").write_text(HTML_TEMPLATE, encoding="utf-8")
    print(f"[OK] HTML saved: {SITE_DIR / 'index.html'}")

    print("\n[RUN RESULT]")
    print(f"- Total reviews: {len(df)}")
    print(f"- Date range: {date_range}")
    print(f"- Complaint Top5: {complaint_top5}")

if __name__ == "__main__":
    main()
