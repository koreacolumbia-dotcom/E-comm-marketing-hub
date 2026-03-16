#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
[CREMA VOC COLLECTOR + ACCUMULATOR | v7.2 - IMAGE FALLBACK PATCH]
- Crema 리뷰 수집(실제 API 호출)
- 최초 1회 백필 + 이후 일별 누적(incremental)
- 자산 로컬화: 상품 이미지/리뷰 썸네일/텍스트 이미지 생성 + Referer 대응
- 누적 저장소(master jsonl + index) 기반 중복 방지
- 빌더 입력: reports/crema_raw/reviews.json (cumulative)
- 사이트 입력: site/data/reviews.json (cumulative)

NEW (이미지 누락 방지 패치)
- product_image_source_url / product_image_url 이 비어도 placeholder 생성
- 다운로드 실패(403/404 등) 시 placeholder로 대체
- config/products.csv 의 product_image_url 또는 product_image 컬럼이 있으면 fallback 사용
- local_product_image 가 빈값으로 끝나지 않도록 보장

기존 중단/복구 패치 요약
- GitHub Actions 6h job timeout 회피:
  - backfill 모드에서 시간 예산을 넘기면 chunk 단위로 안전 중단
  - 다음 시작점(next_start)을 progress 파일에 저장해 이어받기
  - 완전 완료 전에는 latest_health.json 에 incomplete / failed_chunks>0 유지
    -> YML 의 backfill_done.flag 생성 조건(failed_chunks==0)을 통과하지 못하게 함
  - 완료되면 progress 파일 삭제 -> 다음 run 에서 YML 이 flag 생성 가능
필수 env:
- CREMA_SECURE_DEVICE_TOKEN

옵션 env:
- CREMA_DOMAIN (default columbiakorea.co.kr)
- CREMA_WIDGET_ID (default 2)
- CREMA_PER_PAGE (default 30)
- OUTPUT_TZ (default Asia/Seoul)
- PROJECT_ROOT (repo root 강제 지정)
- CREMA_BACKFILL_MAX_SECONDS (default 19500 = 5h25m, GH 6h 이전 마진)

입력:
- config/products.csv (product_code 필수, product_name/product_url optional)
  * optional: product_image_url 또는 product_image 컬럼 지원
출력:
- reports/crema_raw/master_reviews.jsonl
- reports/crema_raw/master_index.json
- reports/crema_raw/reviews.json
- reports/crema_raw/daily/YYYY-MM-DD.json (옵션: end 날짜 스냅샷)
- reports/crema_raw/health/latest_health.json
- reports/crema_raw/backfill_progress.json (백필 이어받기 체크포인트)
- site/data/reviews.json
- site/assets/products/*
- site/assets/reviews/*
- site/assets/text_images/*
"""

from __future__ import annotations

import os
import re
import json
import time
import hashlib
import pathlib
import argparse
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple

import requests
import pandas as pd
from dateutil import tz
from PIL import Image, ImageDraw, ImageFont

# ----------------------------
# tqdm (optional)
# ----------------------------
try:
    from tqdm import tqdm  # type: ignore
except Exception:
    def tqdm(x, **kwargs):
        return x

# ----------------------------
# Settings
# ----------------------------
def _get_int_env(key: str, default: int) -> int:
    v = os.getenv(key, "").strip()
    if not v:
        return default
    try:
        return int(v)
    except Exception:
        return default

DEFAULT_DOMAIN = os.getenv("CREMA_DOMAIN", "columbiakorea.co.kr").strip()
DEFAULT_WIDGET_ID = _get_int_env("CREMA_WIDGET_ID", 2)
DEFAULT_PER_PAGE = _get_int_env("CREMA_PER_PAGE", 30)
OUTPUT_TZ = os.getenv("OUTPUT_TZ", "Asia/Seoul").strip()

SECURE_DEVICE_TOKEN = os.getenv("CREMA_SECURE_DEVICE_TOKEN", "").strip()
if not SECURE_DEVICE_TOKEN:
    raise SystemExit("ERROR: CREMA_SECURE_DEVICE_TOKEN 환경변수가 필요합니다.")

BASE_API = f"https://review8.cre.ma/api/{DEFAULT_DOMAIN}"

# ----------------------------
# Repo root discovery
# ----------------------------
def find_repo_root() -> pathlib.Path:
    env_root = os.getenv("PROJECT_ROOT", "").strip()
    if env_root:
        return pathlib.Path(env_root).expanduser().resolve()

    here = pathlib.Path(__file__).resolve()
    for p in [here.parent] + list(here.parents):
        if (p / "config" / "products.csv").exists():
            return p
        if (p / ".git").exists():
            return p
        if (p / "site").exists() and (p / "reports").exists():
            return p
    return pathlib.Path.cwd().resolve()

ROOT = find_repo_root()

CONFIG_DIR = ROOT / "config"
SITE_DIR = ROOT / "site"
SITE_DATA_DIR = SITE_DIR / "data"
ASSET_DIR = SITE_DIR / "assets"
ASSET_PRODUCTS = ASSET_DIR / "products"
ASSET_REVIEWS = ASSET_DIR / "reviews"
ASSET_TEXT = ASSET_DIR / "text_images"

REPORTS_DIR = ROOT / "reports"
CREMA_RAW_DIR = REPORTS_DIR / "crema_raw"
CREMA_RAW_DAILY_DIR = CREMA_RAW_DIR / "daily"
CREMA_RAW_HEALTH_DIR = CREMA_RAW_DIR / "health"

MASTER_JSONL = CREMA_RAW_DIR / "master_reviews.jsonl"
MASTER_INDEX = CREMA_RAW_DIR / "master_index.json"
CUMULATIVE_JSON = CREMA_RAW_DIR / "reviews.json"

# 백필 이어받기 체크포인트
PROGRESS_JSON = CREMA_RAW_DIR / "backfill_progress.json"

# GitHub Pages public path
# Example public URL:
#   https://koreacolumbia-dotcom.github.io/E-comm-marketing-hub/site/assets/products/xxx.jpg
PUBLIC_SITE_PREFIX = os.getenv("PUBLIC_SITE_PREFIX", "/E-comm-marketing-hub/site").rstrip("/")

def to_public_site_path(path_in_site: pathlib.Path) -> str:
    """Convert an absolute path under SITE_DIR to a browser-accessible public URL path."""
    rel = path_in_site.relative_to(SITE_DIR).as_posix()
    return f"{PUBLIC_SITE_PREFIX}/{rel}"

for p in [
    SITE_DIR,
    SITE_DATA_DIR,
    ASSET_PRODUCTS,
    ASSET_REVIEWS,
    ASSET_TEXT,
    CREMA_RAW_DIR,
    CREMA_RAW_DAILY_DIR,
    CREMA_RAW_HEALTH_DIR,
]:
    p.mkdir(parents=True, exist_ok=True)

# ----------------------------
# Utilities
# ----------------------------
def now_kst() -> datetime:
    return datetime.now(tz=tz.gettz(OUTPUT_TZ))

def parse_dt_any(s: str) -> Optional[datetime]:
    if not s:
        return None
    s = str(s).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        try:
            dt2 = pd.to_datetime(s, errors="coerce")
            if pd.isna(dt2):
                return None
            dt = dt2.to_pydatetime()
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz.gettz(OUTPUT_TZ))
    return dt.astimezone(tz.gettz(OUTPUT_TZ))

def safe_filename(s: str) -> str:
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", s)
    return s[:180]

def sha1(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()

def url_ext(url: str) -> str:
    m = re.search(r"\.(jpg|jpeg|png|webp|gif)(?:\?|$)", url, re.I)
    if m:
        return "." + m.group(1).lower()
    return ".jpg"

def ymd_to_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()

# ----------------------------
# Progress (resume) helpers
# ----------------------------
def load_progress() -> Dict[str, Any]:
    if PROGRESS_JSON.exists():
        try:
            return json.loads(PROGRESS_JSON.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_progress(next_start: str, end: str) -> None:
    PROGRESS_JSON.write_text(
        json.dumps(
            {
                "next_start": next_start,
                "end": end,
                "updated_at": now_kst().isoformat(),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

def clear_progress() -> None:
    if PROGRESS_JSON.exists():
        try:
            PROGRESS_JSON.unlink()
        except Exception:
            pass

# ----------------------------
# HTTP client
# ----------------------------
class Http:
    def __init__(self, timeout: int = 30):
        self.s = requests.Session()
        self.timeout = timeout
        self.s.trust_env = True

    def get_json(
        self,
        url: str,
        params: Dict[str, Any],
        headers: Optional[Dict[str, str]] = None,
        max_retries: int = 6,
        backoff: float = 1.6,
    ) -> Dict[str, Any]:
        headers = headers or {}
        headers.setdefault("Accept", "application/json, text/plain, */*")
        headers.setdefault(
            "User-Agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        )
        headers.setdefault("Accept-Language", "ko-KR,ko;q=0.9,en;q=0.8")

        last_err = None
        for i in range(max_retries):
            try:
                r = self.s.get(url, params=params, headers=headers, timeout=self.timeout)
                if r.status_code == 429:
                    time.sleep((backoff ** i) * 0.7)
                    continue
                if r.status_code == 403 and i < max_retries - 1:
                    time.sleep((backoff ** i) * 0.6)
                    continue
                r.raise_for_status()
                return r.json()
            except Exception as e:
                last_err = e
                time.sleep((backoff ** i) * 0.5)
        raise RuntimeError(f"GET JSON failed: {url} last_err={last_err}")

    def download(
        self,
        url: str,
        out_path: pathlib.Path,
        headers: Optional[Dict[str, str]] = None,
        max_retries: int = 4,
    ) -> bool:
        if not url:
            return False

        headers = headers or {}
        headers.setdefault(
            "User-Agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        )
        headers.setdefault("Accept", "image/avif,image/webp,image/apng,image/*,*/*;q=0.8")
        headers.setdefault("Accept-Language", "ko-KR,ko;q=0.9,en;q=0.8")

        try:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            if out_path.exists() and out_path.stat().st_size > 1024:
                return True

            for i in range(max_retries):
                r = self.s.get(url, headers=headers, timeout=self.timeout, stream=True)
                if r.status_code in (403, 404):
                    return False
                if r.status_code == 429:
                    time.sleep(1.0 + i * 0.7)
                    continue
                r.raise_for_status()

                with open(out_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 64):
                        if chunk:
                            f.write(chunk)

                return out_path.exists() and out_path.stat().st_size > 0
        except Exception:
            return False

http = Http(timeout=30)

# ----------------------------
# Crema API
# ----------------------------
def crema_list_reviews(product_code: str, page: int, per: int, widget_id: int) -> Dict[str, Any]:
    url = f"{BASE_API}/reviews"
    params = {
        "product_code": product_code,
        "page": page,
        "per_page": per,
        "widget_id": widget_id,
        "sort": "recent",
        "secure_device_token": SECURE_DEVICE_TOKEN,
    }
    return http.get_json(url, params=params)

def fetch_product_reviews_in_range(
    product_code: str,
    per: int,
    widget_id: int,
    start_d: date,
    end_d: date,
    hard_max_pages: int = 200,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    stats = {
        "product_code": product_code,
        "pages": 0,
        "kept": 0,
        "seen": 0,
        "early_stop": False,
        "errors": 0,
    }

    page = 1
    for _ in range(hard_max_pages):
        try:
            obj = crema_list_reviews(product_code, page, per, widget_id)
        except Exception:
            stats["errors"] += 1
            break

        stats["pages"] += 1
        reviews = obj.get("reviews") or []
        pagy = obj.get("pagy") or {}
        nxt = pagy.get("next")

        if not reviews:
            break

        early_stop = False
        for r in reviews:
            stats["seen"] += 1
            dt = parse_dt_any(str(r.get("created_at") or ""))
            if not dt:
                continue

            d = dt.date()
            if d < start_d:
                early_stop = True
                continue
            if d > end_d:
                continue

            out.append(r)
            stats["kept"] += 1

        if early_stop:
            stats["early_stop"] = True
            break

        if not nxt:
            break
        try:
            page = int(nxt)
        except Exception:
            break

        time.sleep(0.6)

    return out, stats

# ----------------------------
# Tokenize / tags
# ----------------------------
STOPWORDS = set("""
그리고 그래서 근데 그냥 진짜 너무 정말 완전 조금 약간
이거 그거 저거 이런 저런 해당 관련 부분 경우 느낌
제품 상품 구매 구입 주문 배송 포장 사진 후기 리뷰 사용 착용
사이즈 컬러 색상 브랜드 컬럼비아 콜롬비아 공식 네이버
있어요 없어요 같아요 같습니다 입니다 합니다 됩니다
""".split())

SIZE_KEYWORDS = [
    "사이즈", "정사이즈", "작아요", "작다", "커요", "크다",
    "타이트", "여유", "길이", "발볼", "발등", "허리", "품",
    "길어요", "짧아요", "짧다", "넉넉", "끼임", "타이트함"
]

REQUEST_KEYWORDS = [
    "아쉬움", "아쉽다", "개선", "보완", "불편", "별로",
    "교환", "반품", "문의", "수정", "개선요청"
]

def tokenize_ko(text: str) -> List[str]:
    toks = re.findall(r"[가-힣A-Za-z0-9]+", (text or "").lower())
    return [t for t in toks if len(t) >= 2 and t not in STOPWORDS]

def has_any_kw(text: str, kws: List[str]) -> bool:
    t = (text or "").replace(" ", "")
    for kw in kws:
        if kw.replace(" ", "") in t:
            return True
    return False

def classify_size_direction(text: str) -> str:
    t = (text or "").replace(" ", "")
    small_kw = [
        "작아", "작다", "타이트", "끼다", "조인다", "딱맞다",
        "발볼좁", "발등좁", "여유없", "짧다", "수축", "압박감"
    ]
    big_kw = [
        "커", "크다", "헐렁", "널널", "길다", "크게",
        "여유", "붕뜬", "수축없", "압박없"
    ]
    for kw in small_kw:
        if kw in t:
            return "too_small"
    for kw in big_kw:
        if kw in t:
            return "too_big"
    return "other"

# ----------------------------
# Text / placeholder image helpers
# ----------------------------
def wrap_text(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_width: int) -> List[str]:
    text = (text or "").strip()
    if not text:
        return [""]

    words = text.split()
    lines: List[str] = []
    cur = ""

    def text_w(s: str) -> int:
        bbox = draw.textbbox((0, 0), s, font=font)
        return int(bbox[2] - bbox[0])

    for w in words:
        test = (cur + " " + w).strip()
        if text_w(test) <= max_width:
            cur = test
        else:
            if cur:
                lines.append(cur)
                cur = w
            else:
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

def _load_font(size: int):
    candidates = [
        "NanumGothic.ttf",
        "/usr/share/fonts/truetype/nanum/NanumGothic.ttf",
        "/usr/share/fonts/truetype/nanum/NanumGothicBold.ttf",
        "/System/Library/Fonts/AppleSDGothicNeo.ttc",
        "arial.ttf",
    ]
    for path in candidates:
        try:
            return ImageFont.truetype(path, size)
        except Exception:
            continue
    return ImageFont.load_default()

def create_text_capture_image(
    review_id: str,
    product_name: str,
    score: int,
    created_at: str,
    text: str,
) -> str:
    W = 980
    PAD = 44
    HEADER_H = 120

    BG = (245, 248, 251)
    CARD = (255, 255, 255)
    TXT = (15, 23, 42)
    MUTED = (100, 116, 139)

    out_name = safe_filename(sha1(str(review_id)) + ".png")
    out_path = ASSET_TEXT / out_name

    font_title = _load_font(30)
    font_meta = _load_font(22)
    font_body = _load_font(24)

    tmp = Image.new("RGB", (W, 800), BG)
    dtmp = ImageDraw.Draw(tmp)

    maxw = W - PAD * 2 - 56
    lines = wrap_text(dtmp, text, font_body, maxw)
    line_h = 34
    body_h = max(200, len(lines) * line_h + 40)
    H = PAD * 2 + HEADER_H + body_h

    img = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)

    card_x0, card_y0 = PAD, PAD
    card_x1, card_y1 = W - PAD, H - PAD
    try:
        draw.rounded_rectangle([card_x0, card_y0, card_x1, card_y1], radius=28, fill=CARD)
    except Exception:
        draw.rectangle([card_x0, card_y0, card_x1, card_y1], fill=CARD)

    title = (product_name or "상품명 없음")[:40]
    meta = f"★{score}  ·  {created_at[:10] if created_at else ''}  ·  review_id={review_id}"

    draw.text((card_x0 + 28, card_y0 + 24), title, font=font_title, fill=TXT)
    draw.text((card_x0 + 28, card_y0 + 70), meta, font=font_meta, fill=MUTED)

    y = card_y0 + HEADER_H
    x = card_x0 + 28
    for ln in lines:
        draw.text((x, y), ln, font=font_body, fill=TXT)
        y += line_h

    img.save(out_path)
    return to_public_site_path(out_path)

def create_product_placeholder_image(product_code: str, product_name: str) -> str:
    """
    상품 이미지가 전혀 없거나 다운로드에 실패했을 때
    GitHub Pages 404를 막기 위한 플레이스홀더 생성
    """
    key = f"{product_code}::{product_name}"
    out_name = safe_filename(sha1("placeholder::" + key) + ".png")
    out_path = ASSET_PRODUCTS / out_name

    if out_path.exists() and out_path.stat().st_size > 0:
        return to_public_site_path(out_path)

    W, H = 720, 720
    BG = (245, 248, 251)
    CARD = (255, 255, 255)
    BORDER = (226, 232, 240)
    TXT = (15, 23, 42)
    MUTED = (100, 116, 139)
    BRAND = (0, 45, 114)

    img = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)

    pad = 40
    x0, y0, x1, y1 = pad, pad, W - pad, H - pad
    try:
        draw.rounded_rectangle([x0, y0, x1, y1], radius=36, fill=CARD, outline=BORDER, width=2)
    except Exception:
        draw.rectangle([x0, y0, x1, y1], fill=CARD, outline=BORDER, width=2)

    title_font = _load_font(36)
    meta_font = _load_font(22)
    code_font = _load_font(26)

    # 간단한 아이콘 박스
    try:
        draw.rounded_rectangle([250, 135, 470, 355], radius=32, fill=(239, 243, 249), outline=(220, 230, 240), width=2)
    except Exception:
        draw.rectangle([250, 135, 470, 355], fill=(239, 243, 249), outline=(220, 230, 240), width=2)

    draw.rectangle([305, 185, 415, 295], outline=BRAND, width=6)
    draw.line((305, 185, 415, 295), fill=BRAND, width=5)
    draw.line((415, 185, 305, 295), fill=BRAND, width=5)

    pn = (product_name or "상품 이미지 없음").strip()
    if not pn:
        pn = "상품 이미지 없음"

    code_text = (product_code or "-").strip() or "-"

    max_width = W - 140
    lines = wrap_text(draw, pn, title_font, max_width)
    lines = lines[:3]

    y = 410
    for line in lines:
        bbox = draw.textbbox((0, 0), line, font=title_font)
        tw = bbox[2] - bbox[0]
        draw.text(((W - tw) / 2, y), line, font=title_font, fill=TXT)
        y += 46

    bbox = draw.textbbox((0, 0), code_text, font=code_font)
    tw = bbox[2] - bbox[0]
    draw.text(((W - tw) / 2, y + 12), code_text, font=code_font, fill=BRAND)

    sub = "NO PRODUCT IMAGE"
    bbox = draw.textbbox((0, 0), sub, font=meta_font)
    tw = bbox[2] - bbox[0]
    draw.text(((W - tw) / 2, y + 64), sub, font=meta_font, fill=MUTED)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(out_path)
    return to_public_site_path(out_path)

# ----------------------------
# Flatten + assets
# ----------------------------
def download_to_assets(url: str, kind_dir: pathlib.Path, referer: str = "") -> str:
    if not url or not isinstance(url, str) or not url.startswith("http"):
        return ""
    name = safe_filename(sha1(url) + url_ext(url))
    out_path = kind_dir / name

    headers = {}
    if referer:
        headers["Referer"] = referer

    ok = http.download(url, out_path, headers=headers if headers else None)
    if not ok:
        return ""
    return to_public_site_path(out_path)

def flatten_review(
    r: Dict[str, Any],
    csv_name: str = "",
    csv_url: str = "",
    csv_image_url: str = "",
) -> Dict[str, Any]:
    msg = (r.get("filtered_message") or "").strip()
    score = int(r.get("score") or 0)

    product_name = (r.get("product_name") or "").strip() or (csv_name or "").strip()
    product_url = (r.get("product_url") or "").strip() or (csv_url or "").strip()

    po = r.get("product_options") or []
    ro = r.get("review_options") or []
    po_map = {
        str(x.get("name") or "").strip(): str(x.get("value") or "").strip()
        for x in po if isinstance(x, dict)
    }
    ro_map = {
        str(x.get("name") or "").strip(): str(x.get("value") or "").strip()
        for x in ro if isinstance(x, dict)
    }

    opt_size = (po_map.get("사이즈") or ro_map.get("선택옵션사이즈") or "").strip()
    opt_color = (po_map.get("컬러") or "").strip()
    fit_q = (ro_map.get("사이즈는 어때요") or "").strip()

    tags: List[str] = []
    if score <= 2:
        tags.append("low")
    if score >= 4:
        tags.append("pos")
    if has_any_kw(msg, SIZE_KEYWORDS) or fit_q in ("조금 작아요", "작아요", "조금 커요", "커요"):
        tags.append("size")
    if has_any_kw(msg, REQUEST_KEYWORDS):
        tags.append("req")

    size_dir = classify_size_direction(msg) if "size" in tags else "other"

    thumbs = r.get("images") or []
    thumbs = thumbs if isinstance(thumbs, list) else []
    thumb_url = ""
    full_url = ""
    if thumbs:
        t0 = thumbs[0] if isinstance(thumbs[0], dict) else {}
        thumb_url = str(t0.get("thumbnail_url") or "")
        full_url = str(t0.get("url") or "")

    return {
        "id": r.get("id"),
        "product_code": r.get("product_code") or "",
        "product_name": product_name,
        "product_url": product_url,
        "review_original_url": product_url,
        "rating": score,
        "created_at": r.get("created_at"),
        "text": msg,
        "source": "Official",
        "option_size": opt_size,
        "option_color": opt_color,
        "fit_q": fit_q,
        "height": ro_map.get("키") or "",
        "weight": ro_map.get("몸무게") or "",
        "tags": tags,
        "size_direction": size_dir,
        "review_thumb_url": thumb_url,
        "review_image_url": full_url,
        "product_image_source_url": r.get("product_image_source_url") or "",
        "product_image_url": r.get("product_image_url") or "",
        "csv_product_image_url": csv_image_url or "",
    }

# ----------------------------
# Products CSV
# ----------------------------
def load_products_csv(path: pathlib.Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"products.csv not found: {path}")

    df = pd.read_csv(path)

    if "product_code" not in df.columns:
        raise ValueError("products.csv must include product_code column")

    if "product_name" not in df.columns:
        df["product_name"] = ""

    if "product_url" not in df.columns:
        df["product_url"] = ""

    # optional image columns
    if "product_image_url" not in df.columns:
        df["product_image_url"] = ""

    if "product_image" not in df.columns:
        df["product_image"] = ""

    df["product_code"] = df["product_code"].astype(str).str.strip()
    df["product_name"] = df["product_name"].fillna("").astype(str)
    df["product_url"] = df["product_url"].fillna("").astype(str)
    df["product_image_url"] = df["product_image_url"].fillna("").astype(str)
    df["product_image"] = df["product_image"].fillna("").astype(str)

    return df

# ----------------------------
# Accumulator (master jsonl + index)
# ----------------------------
def load_master_index() -> Dict[str, Any]:
    if MASTER_INDEX.exists():
        try:
            return json.loads(MASTER_INDEX.read_text(encoding="utf-8"))
        except Exception:
            return {"keys": {}}
    return {"keys": {}}

def save_master_index(idx: Dict[str, Any]) -> None:
    MASTER_INDEX.write_text(json.dumps(idx, ensure_ascii=False, indent=2), encoding="utf-8")

def make_key(row: Dict[str, Any]) -> str:
    return f"{row.get('source', 'Official')}::{str(row.get('id'))}"

def append_master_jsonl(new_rows: List[Dict[str, Any]], idx: Dict[str, Any]) -> Tuple[int, int]:
    keys = idx.setdefault("keys", {})
    added = 0
    skipped = 0

    if not new_rows:
        return 0, 0

    with open(MASTER_JSONL, "a", encoding="utf-8") as f:
        for r in new_rows:
            k = make_key(r)
            if k in keys:
                skipped += 1
                continue
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
            keys[k] = 1
            added += 1

    return added, skipped

def read_master_jsonl() -> List[Dict[str, Any]]:
    if not MASTER_JSONL.exists():
        return []
    out: List[Dict[str, Any]] = []
    with open(MASTER_JSONL, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except Exception:
                continue
    return out

# ----------------------------
# Main collect
# ----------------------------
def run_collect(
    mode: str,
    start: str,
    end: str,
    chunk_days: int,
    keep_site_output: bool,
    resume: bool = False,
) -> int:
    products = load_products_csv(CONFIG_DIR / "products.csv")
    codes = products["product_code"].dropna().astype(str).unique().tolist()

    meta_by_code = {}
    for _, row in products.iterrows():
        product_image_csv = str(row.get("product_image_url", "") or "").strip()
        if not product_image_csv:
            product_image_csv = str(row.get("product_image", "") or "").strip()

        meta_by_code[str(row["product_code"])] = {
            "name": row.get("product_name", ""),
            "url": row.get("product_url", ""),
            "image_url": product_image_csv,
        }

    # backfill 은 필요 시 CLI 에서 --start 2025-01-01 처럼 명시적으로 지정
    if mode == "backfill":
        # 기본값은 최근 90일이며, 전체 히스토리는 --start 로 명시
        # resume: 기존 backfill 이어받기는 --resume 일 때만
        if resume:
            prog = load_progress()
            ns = str(prog.get("next_start") or "").strip()
            if ns:
                start = ns

    start_d = ymd_to_date(start)
    end_d = ymd_to_date(end)
    if end_d < start_d:
        raise SystemExit("end < start")

    # GH Actions 6h 제한을 피하기 위한 시간 예산(기본 5h25m)
    max_seconds = int(os.getenv("CREMA_BACKFILL_MAX_SECONDS", "19500"))
    t0 = time.time()
    budget_hit = False
    budget_hit_next_start = ""

    print(f"[INFO] Mode={mode} start={start} end={end} chunk_days={chunk_days}")
    print(f"[INFO] Products: {len(codes)}")
    print(f"[INFO] Fetching crema reviews from: {BASE_API}")
    print(f"[INFO] widget_id={DEFAULT_WIDGET_ID}, per={DEFAULT_PER_PAGE}")
    print(f"[INFO] Repo root: {ROOT}", flush=True)
    if mode == "backfill":
        print(f"[INFO] Backfill time budget: {max_seconds}s (env CREMA_BACKFILL_MAX_SECONDS)", flush=True)
        if PROGRESS_JSON.exists():
            print(f"[INFO] Progress file present: {PROGRESS_JSON}", flush=True)

    idx = load_master_index()

    def chunk_ranges(sd: date, ed: date, step: int) -> List[Tuple[date, date]]:
        out = []
        cur = sd
        while cur <= ed:
            nxt = min(ed, cur + timedelta(days=step - 1))
            out.append((cur, nxt))
            cur = nxt + timedelta(days=1)
        return out

    ranges = chunk_ranges(start_d, end_d, max(1, int(chunk_days)))

    health_chunks: List[Dict[str, Any]] = []
    total_added = 0
    total_skipped = 0
    total_failed_chunks = 0

    for (sd, ed) in ranges:
        if mode == "backfill":
            elapsed = time.time() - t0
            if elapsed > max_seconds:
                budget_hit = True
                budget_hit_next_start = sd.isoformat()
                print(f"[WARN] Backfill time budget reached ({elapsed:.0f}s). Stop now.")
                print(f"[WARN] Next start checkpoint: {budget_hit_next_start}")
                save_progress(next_start=budget_hit_next_start, end=end)
                total_failed_chunks += 1
                break

        chunk_tag = f"{sd.isoformat()}~{ed.isoformat()}"
        print(f"[INFO] Chunk: {chunk_tag}", flush=True)

        chunk_rows_flat: List[Dict[str, Any]] = []
        chunk_stats: List[Dict[str, Any]] = []

        try:
            for code in tqdm(codes, desc=f"Products({chunk_tag})"):
                raw_reviews, st = fetch_product_reviews_in_range(
                    product_code=code,
                    per=DEFAULT_PER_PAGE,
                    widget_id=DEFAULT_WIDGET_ID,
                    start_d=sd,
                    end_d=ed,
                )
                chunk_stats.append(st)

                csv_name = meta_by_code.get(code, {}).get("name", "")
                csv_url = meta_by_code.get(code, {}).get("url", "")
                csv_image_url = meta_by_code.get(code, {}).get("image_url", "")

                for rr in raw_reviews:
                    chunk_rows_flat.append(
                        flatten_review(
                            rr,
                            csv_name=csv_name,
                            csv_url=csv_url,
                            csv_image_url=csv_image_url,
                        )
                    )

        except Exception as e:
            total_failed_chunks += 1
            health_chunks.append({"chunk": chunk_tag, "ok": False, "error": str(e)})
            continue

        df = pd.DataFrame(chunk_rows_flat)
        if df.empty:
            health_chunks.append({
                "chunk": chunk_tag,
                "ok": True,
                "note": "no_rows",
                "products": len(codes),
            })
            continue

        df = df.drop_duplicates(subset=["id"], keep="first").copy()

        # product_code별 이미지 캐시
        local_product_images: Dict[str, str] = {}

        for _, row in tqdm(df.iterrows(), total=len(df), desc=f"Assets({chunk_tag})"):
            pcode = str(row.get("product_code") or "").strip()
            pname = str(row.get("product_name") or "").strip()
            product_page = str(row.get("product_url") or "").strip()

            row_prod_source = str(row.get("product_image_source_url") or "").strip()
            row_prod_direct = str(row.get("product_image_url") or "").strip()
            row_prod_csv = str(row.get("csv_product_image_url") or "").strip()

            prod_url = row_prod_source or row_prod_direct or row_prod_csv

            if pcode and pcode in local_product_images:
                prod_local = local_product_images[pcode]
            else:
                prod_local = ""

                # 1) 우선 다운로드 시도
                if prod_url:
                    prod_local = download_to_assets(prod_url, ASSET_PRODUCTS, referer=product_page)

                # 2) 실패했거나 URL 자체가 없으면 placeholder 생성
                if not prod_local:
                    prod_local = create_product_placeholder_image(
                        product_code=pcode,
                        product_name=pname,
                    )

                if pcode:
                    local_product_images[pcode] = prod_local

            thumb_url = str(row.get("review_thumb_url") or "").strip()
            thumb_local = download_to_assets(thumb_url, ASSET_REVIEWS, referer=product_page) if thumb_url else ""

            cap = ""
            text = str(row.get("text") or "")
            if len(text) >= 100:
                cap = create_text_capture_image(
                    review_id=str(row.get("id")),
                    product_name=pname,
                    score=int(row.get("rating") or 0),
                    created_at=str(row.get("created_at") or ""),
                    text=text,
                )

            df.loc[row.name, "local_product_image"] = prod_local
            df.loc[row.name, "local_review_thumb"] = thumb_local
            df.loc[row.name, "text_image_path"] = cap

        new_rows = df.to_dict(orient="records")
        added, skipped = append_master_jsonl(new_rows, idx)
        total_added += added
        total_skipped += skipped

        health_chunks.append({
            "chunk": chunk_tag,
            "ok": True,
            "rows_in_chunk": int(len(df)),
            "added": int(added),
            "skipped_dup": int(skipped),
            "products": len(codes),
            "sample_stats": chunk_stats[:3],
        })

        snap_path = CREMA_RAW_DAILY_DIR / f"{ed.isoformat()}.json"
        snap_path.write_text(
            json.dumps({"reviews": new_rows}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    save_master_index(idx)

    master_rows = read_master_jsonl()
    CUMULATIVE_JSON.write_text(
        json.dumps({"reviews": master_rows}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    if keep_site_output:
        (SITE_DATA_DIR / "reviews.json").write_text(
            json.dumps({"reviews": master_rows}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    backfill_completed = (
        mode == "backfill"
        and (not budget_hit)
        and total_failed_chunks == 0
        and (len(ranges) > 0)
    )

    if backfill_completed:
        clear_progress()

    latest_health = {
        "updated_at": now_kst().isoformat(),
        "mode": mode,
        "start": start,
        "end": end,
        "chunk_days": chunk_days,
        "total_added": total_added,
        "total_skipped_dup": total_skipped,
        "total_master_rows": len(master_rows),
        "failed_chunks": total_failed_chunks,
        "incomplete": bool(mode == "backfill" and not backfill_completed),
        "budget_hit": budget_hit,
        "budget_hit_next_start": budget_hit_next_start,
        "progress_file": str(PROGRESS_JSON) if PROGRESS_JSON.exists() else "",
        "chunks": health_chunks,
    }

    (CREMA_RAW_HEALTH_DIR / "latest_health.json").write_text(
        json.dumps(latest_health, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    if len(master_rows) == 0:
        print("[ERROR] Crema collection finished but master_rows is empty.")
        print(f"- Health:        {CREMA_RAW_HEALTH_DIR / 'latest_health.json'}")
        return 2

    print("[OK] Crema collection + accumulation done.")
    print(f"- Builder input: {CUMULATIVE_JSON}")
    print(f"- Site output:   {SITE_DATA_DIR / 'reviews.json'}")
    print(f"- Master jsonl:  {MASTER_JSONL}")
    print(f"- Master index:  {MASTER_INDEX}")
    print(f"- Daily snaps:   {CREMA_RAW_DAILY_DIR}")
    print(f"- Health:        {CREMA_RAW_HEALTH_DIR / 'latest_health.json'}")
    print(f"- Added: {total_added} | Skipped(dup): {total_skipped}")
    print(f"- Total master rows: {len(master_rows)}")
    if budget_hit:
        print(f"[WARN] Backfill paused due to time budget. Will resume next run from: {budget_hit_next_start}")
    if total_failed_chunks:
        print(f"[WARN] failed_chunks={total_failed_chunks} (see latest_health.json)")
    return 0

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["backfill", "daily"], required=True)
    ap.add_argument(
        "--start",
        default="",
        help="YYYY-MM-DD (KST). Default: today-90d (~3 months) for first run",
    )
    ap.add_argument(
        "--end",
        default="",
        help="YYYY-MM-DD (KST). default=today-1",
    )
    ap.add_argument("--chunk-days", type=int, default=14)
    ap.add_argument(
        "--resume",
        action="store_true",
        help="backfill only: resume from reports/crema_raw/backfill_progress.json",
    )
    ap.add_argument(
        "--keep-site-output",
        action="store_true",
        help="also write site/data/reviews.json",
    )
    args = ap.parse_args()

    # Backfill 기본값: 최근 약 3개월만 수집 (GitHub Actions 6h 제한 회피)
    # 전체 히스토리는 --start 2025-01-01 처럼 명시
    if args.mode == "backfill" and (not str(args.start).strip()):
        args.start = (now_kst().date() - timedelta(days=90)).isoformat()

    # end default = yesterday(KST)
    if not args.end:
        args.end = (now_kst().date() - timedelta(days=1)).isoformat()

    return run_collect(
        mode=args.mode,
        start=str(args.start),
        end=str(args.end),
        chunk_days=int(args.chunk_days or 1),
        keep_site_output=bool(args.keep_site_output),
        resume=bool(args.resume),
    )

if __name__ == "__main__":
    raise SystemExit(main())

