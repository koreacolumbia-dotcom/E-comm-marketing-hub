#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
[CREMA VOC COLLECTOR + ACCUMULATOR | v7.0]
- ✅ Crema 리뷰 수집(실제 API 호출)
- ✅ 1회 백필 + 이후 전일 누적(incremental)
- ✅ 자산 로컬화(상품이미지/리뷰썸네일/텍스트캡처) + Referer 대응
- ✅ 누적 저장소(master jsonl + index) 기반 중복 방지
- ✅ 빌더 입력: reports/crema_raw/reviews.json (cumulative)
- ✅ 사이트 입력: site/data/reviews.json (cumulative)

필수 env:
- CREMA_SECURE_DEVICE_TOKEN

옵션 env:
- CREMA_DOMAIN (default columbiakorea.co.kr)
- CREMA_WIDGET_ID (default 2)
- CREMA_PER_PAGE (default 30)
- OUTPUT_TZ (default Asia/Seoul)
- PROJECT_ROOT (repo root 강제 지정)

입력:
- config/products.csv (product_code 필수, product_name/product_url optional)

출력:
- reports/crema_raw/master_reviews.jsonl
- reports/crema_raw/master_index.json
- reports/crema_raw/reviews.json
- reports/crema_raw/daily/YYYY-MM-DD.json (옵션: end 날짜 스냅샷)
- reports/crema_raw/health/latest_health.json
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

for p in [SITE_DIR, SITE_DATA_DIR, ASSET_PRODUCTS, ASSET_REVIEWS, ASSET_TEXT, CREMA_RAW_DIR, CREMA_RAW_DAILY_DIR, CREMA_RAW_HEALTH_DIR]:
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
    # "Z" 대응
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        try:
            dt = pd.to_datetime(s, errors="coerce")
            if pd.isna(dt):
                return None
            dt = dt.to_pydatetime()
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

def in_kst_range(dt: datetime, start_d: date, end_d: date) -> bool:
    d = dt.astimezone(tz.gettz(OUTPUT_TZ)).date()
    return start_d <= d <= end_d

def ymd_to_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()

# ----------------------------
# HTTP client
# ----------------------------
class Http:
    def __init__(self, timeout: int = 30):
        self.s = requests.Session()
        self.timeout = timeout
        # ✅ 프록시/회사망 환경에서 HTTP(S)_PROXY 자동 사용
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
        headers.setdefault("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
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
        headers.setdefault("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
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
        "sort": "recent",  # 최신순 기대
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
    """
    페이지를 돌며 수집하되:
    - created_at 파싱 성공 + start/end 범위 밖이면 필터링
    - (최신순 가정) created_at < start 만나면 해당 상품은 조기 중단
    """
    out: List[Dict[str, Any]] = []
    stats = {"product_code": product_code, "pages": 0, "kept": 0, "seen": 0, "early_stop": False, "errors": 0}

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

        # 리뷰들을 보며 범위 필터 + 조기중단
        early_stop = False
        for r in reviews:
            stats["seen"] += 1
            dt = parse_dt_any(str(r.get("created_at") or ""))
            if not dt:
                continue

            d = dt.date()
            if d < start_d:
                # 최신순 가정이면 이 페이지 이후는 더 과거
                early_stop = True
                continue
            if d > end_d:
                # end보다 미래? 거의 없지만 skip
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
# Tokenize / tags (원래 스타일 유지)
# ----------------------------
STOPWORDS = set("""
그리고 그러나 그래서 하지만 또한
너무 정말 완전 진짜 매우 그냥 조금 약간
저는 제가 우리는 너희 이거 그거 저거
구매 구입 주문 구매해 구입해 샀어요 샀습니다 주문했
제품 상품 물건
사용중 사용 사용함 착용 입어 신어 써봤
배송 택배 포장
문의
좋아요 좋다 좋네요 만족 추천 재구매 가성비 최고 굿
예뻐요 이뻐요
정사이즈 한치수 한 치수
컬러 색상 디자인
""".split())

SIZE_KEYWORDS = ["사이즈","정사이즈","작아요","작다","커요","크다","핏","타이트","여유","끼","기장","소매","어깨","가슴","발볼","헐렁","오버","업","다운","한치수","반치수"]

def tokenize_ko(text: str) -> List[str]:
    toks = re.findall(r"[가-힣A-Za-z0-9]+", (text or "").lower())
    return [t for t in toks if len(t) >= 2 and t not in STOPWORDS]

def top_terms(texts: List[str], topk: int = 5) -> List[Tuple[str, int]]:
    freq: Dict[str, int] = {}
    for t in texts:
        for w in tokenize_ko(t):
            freq[w] = freq.get(w, 0) + 1
    return sorted(freq.items(), key=lambda x: x[1], reverse=True)[:topk]

def has_any_kw(text: str, kws: List[str]) -> bool:
    t = (text or "").replace(" ", "")
    for kw in kws:
        if kw.replace(" ", "") in t:
            return True
    return False

def classify_size_direction(text: str) -> str:
    t = (text or "").replace(" ", "")
    small_kw = ["작아","작다","타이트","끼","조인다","짧다","좁다","발볼좁","어깨좁","가슴좁","다운","한치수작","반치수작"]
    big_kw = ["커","크다","넉넉","오버","길다","넓다","헐렁","부해","업","한치수큰","반치수큰"]
    for kw in small_kw:
        if kw in t:
            return "too_small"
    for kw in big_kw:
        if kw in t:
            return "too_big"
    return "other"

# ----------------------------
# Text capture image (원래 유지)
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

def create_text_capture_image(review_id: str, product_name: str, score: int, created_at: str, text: str) -> str:
    W = 980
    PAD = 44
    HEADER_H = 120

    BG = (245, 248, 251)
    CARD = (255, 255, 255)
    TXT = (15, 23, 42)
    MUTED = (100, 116, 139)

    out_name = safe_filename(sha1(str(review_id)) + ".png")
    out_path = ASSET_TEXT / out_name

    try:
        font_title = ImageFont.truetype("NanumGothic.ttf", 30)
        font_meta = ImageFont.truetype("NanumGothic.ttf", 22)
        font_body = ImageFont.truetype("NanumGothic.ttf", 24)
    except Exception:
        font_title = ImageFont.load_default()
        font_meta = ImageFont.load_default()
        font_body = ImageFont.load_default()

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
    meta = f"★ {score}  ·  {created_at[:10] if created_at else ''}  ·  review_id={review_id}"

    draw.text((card_x0 + 28, card_y0 + 24), title, font=font_title, fill=TXT)
    draw.text((card_x0 + 28, card_y0 + 70), meta, font=font_meta, fill=MUTED)

    y = card_y0 + HEADER_H
    x = card_x0 + 28
    for ln in lines:
        draw.text((x, y), ln, font=font_body, fill=TXT)
        y += line_h

    img.save(out_path)
    return str(out_path.relative_to(SITE_DIR)).replace("\\", "/")

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
    return str(out_path.relative_to(SITE_DIR)).replace("\\", "/")

def flatten_review(r: Dict[str, Any], csv_name: str = "", csv_url: str = "") -> Dict[str, Any]:
    msg = (r.get("filtered_message") or "").strip()
    score = int(r.get("score") or 0)

    product_name = (r.get("product_name") or "").strip() or (csv_name or "").strip()
    product_url = (r.get("product_url") or "").strip() or (csv_url or "").strip()

    # options
    po = r.get("product_options") or []
    ro = r.get("review_options") or []
    po_map = {str(x.get("name") or "").strip(): str(x.get("value") or "").strip() for x in po if isinstance(x, dict)}
    ro_map = {str(x.get("name") or "").strip(): str(x.get("value") or "").strip() for x in ro if isinstance(x, dict)}

    opt_size = (po_map.get("사이즈") or ro_map.get("평소사이즈") or "").strip()
    opt_color = (po_map.get("컬러") or "").strip()
    fit_q = (ro_map.get("사이즈 어때요?") or "").strip()

    tags: List[str] = []
    if score <= 2:
        tags.append("low")
    if has_any_kw(msg, SIZE_KEYWORDS) or fit_q in ("조금 작아요","작아요","조금 커요","커요"):
        tags.append("size")

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
    df["product_code"] = df["product_code"].astype(str).str.strip()
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

# ✅ PATCH: id가 비는 케이스(또는 None)에서 누적이 깨지는 문제 방지
def make_key(row: Dict[str, Any]) -> str:
    """
    누적 중복 방지용 안정 키
    1) source + id 가 있으면 그걸로
    2) id가 비면 fallback (product_code + created_at + text hash)
    """
    src = str(row.get("source") or "Official").strip()

    rid = row.get("id")
    rid_s = str(rid).strip() if rid is not None else ""
    if rid_s and rid_s.lower() != "none":
        return f"{src}::id::{rid_s}"

    pcode = str(row.get("product_code") or "").strip()
    created = str(row.get("created_at") or "").strip()
    text = str(row.get("text") or "").strip()
    fb = sha1(f"{pcode}|{created}|{text}")
    return f"{src}::fb::{fb}"

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
# Main
# ----------------------------
def run_collect(mode: str, start: str, end: str, chunk_days: int, keep_site_output: bool) -> int:
    products = load_products_csv(CONFIG_DIR / "products.csv")
    codes = products["product_code"].dropna().astype(str).unique().tolist()
    meta_by_code = {
        row["product_code"]: {"name": row.get("product_name", ""), "url": row.get("product_url", "")}
        for _, row in products.iterrows()
    }

    start_d = ymd_to_date(start)
    end_d = ymd_to_date(end)
    if end_d < start_d:
        raise SystemExit("end < start")

    print(f"[INFO] Mode={mode} start={start} end={end} chunk_days={chunk_days}")
    print(f"[INFO] Products: {len(codes)}")
    print(f"[INFO] Fetching crema reviews from: {BASE_API}")
    print(f"[INFO] widget_id={DEFAULT_WIDGET_ID}, per={DEFAULT_PER_PAGE}")
    print(f"[INFO] Repo root: {ROOT}", flush=True)

    # 누적 인덱스 로드
    idx = load_master_index()

    # 기간을 chunk로 나눠서 “조기중단” 효율을 높임
    # (Crema는 서버 필터가 애매할 수 있어서, 클라 필터 + 최신순 조기중단이 현실적)
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

    # “backfill”은 여러 chunk, “daily”도 동일하게 동작(보통 1일)
    for (sd, ed) in ranges:
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
                for rr in raw_reviews:
                    chunk_rows_flat.append(flatten_review(rr, csv_name=csv_name, csv_url=csv_url))

        except Exception as e:
            total_failed_chunks += 1
            health_chunks.append({"chunk": chunk_tag, "ok": False, "error": str(e)})
            continue

        # dedupe(이번 chunk 내부)
        df = pd.DataFrame(chunk_rows_flat)
        if df.empty:
            health_chunks.append({"chunk": chunk_tag, "ok": True, "note": "no_rows", "products": len(codes)})
            continue

        # ✅ PATCH: chunk 내부 dedupe도 누적 키와 동일한 기준으로 (id 비어도 안전)
        df["dedupe_key"] = df.apply(lambda r: make_key(r.to_dict()), axis=1)
        df = df.drop_duplicates(subset=["dedupe_key"], keep="first").copy()

        # asset localization + text images
        local_product_images: Dict[str, str] = {}

        for _, row in tqdm(df.iterrows(), total=len(df), desc=f"Assets({chunk_tag})"):
            pcode = str(row.get("product_code") or "")
            product_page = str(row.get("product_url") or "")

            prod_url = str(row.get("product_image_source_url") or row.get("product_image_url") or "")
            if pcode and pcode in local_product_images:
                prod_local = local_product_images[pcode]
            else:
                prod_local = download_to_assets(prod_url, ASSET_PRODUCTS, referer=product_page)
                if pcode:
                    local_product_images[pcode] = prod_local

            thumb_url = str(row.get("review_thumb_url") or "")
            thumb_local = download_to_assets(thumb_url, ASSET_REVIEWS, referer=product_page) if thumb_url else ""

            cap = ""
            text = str(row.get("text") or "")
            if len(text) >= 100:
                cap = create_text_capture_image(
                    review_id=str(row.get("id")),
                    product_name=str(row.get("product_name") or ""),
                    score=int(row.get("rating") or 0),
                    created_at=str(row.get("created_at") or ""),
                    text=text,
                )

            df.loc[row.name, "local_product_image"] = prod_local
            df.loc[row.name, "local_review_thumb"] = thumb_local
            df.loc[row.name, "text_image_path"] = cap

        # master 누적 append
        # - dedupe_key는 내부 처리용이라 저장/노출에서 제거
        df = df.drop(columns=["dedupe_key"], errors="ignore")

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

        # daily snapshot (ed 날짜 기준)
        snap_path = CREMA_RAW_DAILY_DIR / f"{ed.isoformat()}.json"
        snap_path.write_text(json.dumps({"reviews": new_rows}, ensure_ascii=False, indent=2), encoding="utf-8")

    # 누적 인덱스 저장
    save_master_index(idx)

    # 누적 읽어서 cumulative json 생성
    master_rows = read_master_jsonl()

    CUMULATIVE_JSON.write_text(json.dumps({"reviews": master_rows}, ensure_ascii=False, indent=2), encoding="utf-8")
    if keep_site_output:
        (SITE_DATA_DIR / "reviews.json").write_text(json.dumps({"reviews": master_rows}, ensure_ascii=False, indent=2), encoding="utf-8")

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
        "chunks": health_chunks,
    }
    (CREMA_RAW_HEALTH_DIR / "latest_health.json").write_text(json.dumps(latest_health, ensure_ascii=False, indent=2), encoding="utf-8")

    print("[OK] Crema collection + accumulation done.")
    print(f"- Builder input: {CUMULATIVE_JSON}")
    print(f"- Site output:   {SITE_DATA_DIR / 'reviews.json'}")
    print(f"- Master jsonl:  {MASTER_JSONL}")
    print(f"- Master index:  {MASTER_INDEX}")
    print(f"- Daily snaps:   {CREMA_RAW_DAILY_DIR}")
    print(f"- Health:        {CREMA_RAW_HEALTH_DIR / 'latest_health.json'}")
    print(f"- Added: {total_added} | Skipped(dup): {total_skipped}")
    if total_failed_chunks:
        print(f"[WARN] Some chunks failed: {total_failed_chunks} (see latest_health.json)")
    return 0

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["backfill", "daily"], required=True)
    ap.add_argument("--start", default="2025-01-01", help="YYYY-MM-DD (KST)")
    ap.add_argument("--end", default="", help="YYYY-MM-DD (KST). default=today-1")
    ap.add_argument("--chunk-days", type=int, default=14)
    ap.add_argument("--keep-site-output", action="store_true", help="also write site/data/reviews.json")
    args = ap.parse_args()

    # end default = yesterday(KST)
    if not args.end:
        y = (now_kst().date() - timedelta(days=1)).isoformat()
        args.end = y

    # (선택) backfill은 항상 2025-01-01로 강제하고 싶으면 아래 2줄 활성화
    if args.mode == "backfill":
        args.start = "2025-01-01"

    # daily는 보통 전일만: start=end로 주는 게 일반적(하지만 yml에서 이미 그렇게 넣는 구조)
    return run_collect(
        mode=args.mode,
        start=str(args.start),
        end=str(args.end),
        chunk_days=int(args.chunk_days or 1),
        keep_site_output=bool(args.keep_site_output),
    )

if __name__ == "__main__":
    raise SystemExit(main())
