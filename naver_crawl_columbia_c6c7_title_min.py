#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
[운영 안정판 v8 - FULL FINAL + TRIM + GRID(4COL) + CARD UI TUNING]
- 입력 CSV 자동 탐색
- official_hashes.csv(공식 이미지) 우선 적용 → 네이버 이미지 fallback
- 네이버 쇼핑 API 최저가 + Top3 + Match 점수
- 결과 CSV + HTML 대시보드 생성(정적)
- 전일(result_*.csv) 대비 Δ최저가 표시
- 캐시(hit/miss) 지원
- --limit 지원
- ✅ 결과에서 제외: 네이버최저가 없으면 제외 / 이미지 없으면 제외
- ✅ 정확도 강화: 코드 매칭 우선 → 브랜드(Columbia/컬럼비아) → 둘 다 실패면 제외(무관 유입 차단)
- ✅ EXCLUDE_CODES 기본 포함 + 옵션 지원
- ✅ UI: 검색창 1개 통합 + 로딩 오버레이
- ✅ UI: 카드 높이 균일화 / 체크박스 위치(오른쪽 상단 고정) / 그리드 4열 옵션
- ✅ NEW: --trim_images
  - 원본(흰/투명) 여백 자동 크롭 → 로컬 파일 저장 → HTML은 file:// 절대경로로 표시
  - HTML만 따로 옮겨 열어도 이미지 깨지지 않게 “로컬 파일 경로” 유지(절대경로)
    (단, HTML을 다른 PC로 옮기면 이미지도 같이 옮겨야 함)

✅ EMBED(중복 UI 제거) 패치:
- iframe이 아니어도 URL에 ?embed=1(or ?embedded=1)이면 embedded 모드 강제
- embedded 모드면 리포트의 aside/header 숨김(허브 UI와 중복 방지)

필수 환경변수:
- NAVER_CLIENT_ID
- NAVER_CLIENT_SECRET

실행:
  python -u naver_crawl_columbia_c6c7_title_min.py
  python -u naver_crawl_columbia_c6c7_title_min.py --limit 999999
  python -u naver_crawl_columbia_c6c7_title_min.py --trim_images
"""

import os
import re
import time
import json
import glob
import argparse
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple


def _kst_now_str(fmt: str = "%Y-%m-%d %H:%M KST") -> str:
    """Return current time string in KST without relying on pytz."""
    import datetime as dt
    kst = dt.timezone(dt.timedelta(hours=9))
    return dt.datetime.now(kst).strftime(fmt)


import pandas as pd

# ================================================================
# Summary/meta export (Hub first-screen consumption)
# - Writes/updates reports/summary.json (or alongside output html)
# ================================================================
import json
from datetime import datetime, timezone, timedelta

_KST = timezone(timedelta(hours=9))

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

# Pillow (이미지 트리밍용)
try:
    from PIL import Image
except Exception:
    Image = None


# -----------------------------
# 공통 로깅
# -----------------------------
def log(msg: str) -> None:
    print(msg, flush=True)


# -----------------------------
# 입력 파일 자동 탐색
# -----------------------------
def pick_input_file(
    explicit_input: Optional[str],
    default_name: str = "공식몰가격.csv",
    patterns: Optional[List[str]] = None,
) -> str:
    if explicit_input:
        if os.path.exists(explicit_input):
            return explicit_input
        raise SystemExit(f"[INPUT] 지정한 입력 파일을 찾을 수 없습니다: {explicit_input}")

    if os.path.exists(default_name):
        return default_name

    if patterns is None:
        patterns = [
            "공식몰가격*.csv",
            "*공식몰가격*.csv",
            "공식몰가격*.CSV",
            "*공식몰가격*.CSV",
            "공식몰가격.xlsx - Sheet1.csv",
            "공식몰가격.xlsx - Sheet1.CSV",
        ]

    candidates: List[str] = []
    for pat in patterns:
        candidates.extend(glob.glob(pat))
    candidates = [c for c in candidates if os.path.isfile(c)]

    if not candidates:
        raise SystemExit(
            "[INPUT] 입력 파일을 찾지 못했습니다.\n"
            f" - 기본 파일명: {default_name}\n"
            f" - 탐색 패턴: {patterns}\n"
            "현재 폴더에 '공식몰가격...' CSV 파일을 두거나 --input으로 경로를 지정해주세요."
        )

    candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return candidates[0]


# -----------------------------
# 네이버 쇼핑 API
# -----------------------------
API_URL = "https://openapi.naver.com/v1/search/shop.json"


def strip_html_tags(s: str) -> str:
    if not s:
        return ""
    return re.sub(r"<[^>]+>", "", s)


def _to_int_price(x) -> Optional[int]:
    if pd.isna(x):
        return None
    s = re.sub(r"[^\d]", "", str(x))
    return int(s) if s else None


def _to_int_safe(x, default: int = 10**18) -> int:
    try:
        return int(x)
    except Exception:
        return default


def fetch_naver_shop_with_retry(
    query: str,
    client_id: str,
    client_secret: str,
    display: int = 10,
    max_retries: int = 4,
    base_sleep: float = 0.6,
    timeout_sec: int = 15,
) -> List[Dict[str, Any]]:
    enc = urllib.parse.quote(query)
    url = f"{API_URL}?query={enc}&display={display}&start=1"

    for attempt in range(max_retries + 1):
        req = urllib.request.Request(url)
        req.add_header("X-Naver-Client-Id", client_id)
        req.add_header("X-Naver-Client-Secret", client_secret)

        try:
            with urllib.request.urlopen(req, timeout=timeout_sec) as res:
                payload = json.loads(res.read().decode("utf-8"))
                return payload.get("items", []) or []

        except urllib.error.HTTPError as e:
            code = getattr(e, "code", None)
            if attempt < max_retries and (code in (429, 500, 502, 503, 504) or (code and code >= 500)):
                time.sleep(base_sleep * (2 ** attempt))
                continue
            return []

        except Exception:
            if attempt < max_retries:
                time.sleep(base_sleep * (2 ** attempt))
                continue
            return []

    return []


def filter_items_for_accuracy(
    items: List[Dict[str, Any]],
    style_code: str,
    min_price: Optional[int],
    max_price: Optional[int],
    exclude_malls: List[str],
) -> List[Dict[str, Any]]:
    """
    ✅ 정확도 강화 (무관 상품/여자 모델컷 방지)
    - 0) 기본 컷: 가격 범위/몰 제외/악성 키워드
    - 1) 1차: title에 style_code 포함인 결과만
    - 2) 2차: title에 Columbia/컬럼비아 포함인 결과만
    - 3) ❌ fallback 금지: 위 조건 둘 다 실패면 [] 반환
    """
    if not items:
        return []

    code_l = (style_code or "").strip().lower()
    lowered_excludes = [e.strip().lower() for e in exclude_malls if e.strip()]
    cleaned: List[Dict[str, Any]] = []

    bad_terms = [
        "호환", "케이스", "필름", "스티커", "리필", "커버",
        "브라", "브래지어", "이너", "나시", "탑", "레깅스", "요가", "스포츠브라",
        "속옷", "언더웨어", "비키니", "수영복",
    ]

    for it in items:
        lp = _to_int_safe(it.get("lprice"), default=-1)
        mall = (it.get("mallName") or "").strip().lower()
        title = strip_html_tags(it.get("title") or "").lower()

        if min_price is not None and lp < min_price:
            continue
        if max_price is not None and lp > max_price:
            continue
        if lowered_excludes and any(ex in mall for ex in lowered_excludes):
            continue
        if any(t in title for t in bad_terms):
            continue

        cleaned.append(it)

    if not cleaned:
        return []

    if code_l:
        code_matched = [it for it in cleaned if code_l in strip_html_tags(it.get("title") or "").lower()]
        if code_matched:
            return code_matched

    brand_matched = []
    for it in cleaned:
        title = strip_html_tags(it.get("title") or "").lower()
        if ("columbia" in title) or ("컬럼비아" in title):
            brand_matched.append(it)
    if brand_matched:
        return brand_matched

    return []


def pick_lowest_item(items: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not items:
        return None

    def lp(it: Dict[str, Any]) -> int:
        return _to_int_safe(it.get("lprice"), default=10**18)

    return min(items, key=lp)


def pick_top_n_by_price(items: List[Dict[str, Any]], n: int = 3) -> List[Dict[str, Any]]:
    if not items:
        return []

    def lp(it: Dict[str, Any]) -> int:
        return _to_int_safe(it.get("lprice"), default=10**18)

    return sorted(items, key=lp)[:n]


def compute_confidence(style_code: str, best_item: Optional[Dict[str, Any]]) -> int:
    if not best_item:
        return 0

    title = strip_html_tags(best_item.get("title") or "").lower()
    mall = (best_item.get("mallName") or "").lower()
    code_l = (style_code or "").lower()

    score = 0
    if code_l and code_l in title:
        score += 2
    if "columbia" in title or "컬럼비아" in title:
        score += 1

    trust_mall_terms = ["공식", "브랜드", "백화점", "현대", "롯데", "신세계", "네이버", "스마트스토어"]
    if any(t in mall for t in trust_mall_terms):
        score += 1

    return max(0, score)


def choose_best_image(best_item: Optional[Dict[str, Any]], top_items: List[Dict[str, Any]]) -> str:
    if best_item:
        img = (best_item.get("image") or "").strip()
        if img:
            return img
    for it in top_items:
        img = (it.get("image") or "").strip()
        if img:
            return img
    return ""


# -----------------------------
# API 캐시
# -----------------------------
def cache_path(cache_dir: str, style_code: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_\-]", "_", style_code.strip())
    return os.path.join(cache_dir, f"{safe}.json")


def load_cache(cache_dir: str, style_code: str, ttl_hours: int = 12) -> Optional[List[Dict[str, Any]]]:
    path = cache_path(cache_dir, style_code)
    if not os.path.exists(path):
        return None

    try:
        mtime = os.path.getmtime(path)
        age_sec = time.time() - mtime
        if age_sec > ttl_hours * 3600:
            return None

        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)

        items = payload.get("items")
        if isinstance(items, list):
            return items
        return None
    except Exception:
        return None


def save_cache(cache_dir: str, style_code: str, items: List[Dict[str, Any]]) -> None:
    os.makedirs(cache_dir, exist_ok=True)
    path = cache_path(cache_dir, style_code)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"items": items, "saved_at": datetime.now().isoformat()}, f, ensure_ascii=False)
    except Exception:
        pass


# -----------------------------
# 이미지 다운로드 + 여백 트리밍
# -----------------------------
def _file_url(abs_path: str) -> str:
    # Windows file:// URL
    p = abs_path.replace("\\", "/")
    if not p.startswith("/"):
        p = "/" + p
    return "file://" + p


def _download_binary(url: str, referer: Optional[str], timeout_sec: int = 18, max_retries: int = 3) -> Optional[bytes]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    }
    if referer:
        headers["Referer"] = referer

    for attempt in range(max_retries + 1):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout_sec) as res:
                return res.read()
        except Exception:
            if attempt < max_retries:
                time.sleep(0.6 * (2 ** attempt))
                continue
            return None
    return None


def _crop_whitespace_pil(im: "Image.Image", bg_thresh: int = 245, pad: int = 8) -> "Image.Image":
    """
    흰/투명 배경 여백 자동 크롭
    - bg_thresh: 245 이상을 '거의 흰색'으로 간주
    - pad: 크롭 후 여유 픽셀
    """
    if im.mode not in ("RGBA", "RGB", "LA", "L"):
        im = im.convert("RGBA")

    w, h = im.size
    if w < 2 or h < 2:
        return im

    if im.mode == "RGBA":
        rgba = im
        px = rgba.getdata()
        minx, miny, maxx, maxy = w, h, -1, -1
        for y in range(h):
            row = px[y * w:(y + 1) * w]
            for x, (r, g, b, a) in enumerate(row):
                if a <= 8:
                    continue
                if r >= bg_thresh and g >= bg_thresh and b >= bg_thresh:
                    continue
                if x < minx: minx = x
                if y < miny: miny = y
                if x > maxx: maxx = x
                if y > maxy: maxy = y
        if maxx < 0:
            return im
    else:
        rgb = im.convert("RGB")
        px = rgb.getdata()
        minx, miny, maxx, maxy = w, h, -1, -1
        for y in range(h):
            row = px[y * w:(y + 1) * w]
            for x, (r, g, b) in enumerate(row):
                if r >= bg_thresh and g >= bg_thresh and b >= bg_thresh:
                    continue
                if x < minx: minx = x
                if y < miny: miny = y
                if x > maxx: maxx = x
                if y > maxy: maxy = y
        if maxx < 0:
            return im

    minx = max(0, minx - pad)
    miny = max(0, miny - pad)
    maxx = min(w - 1, maxx + pad)
    maxy = min(h - 1, maxy + pad)

    if minx >= maxx or miny >= maxy:
        return im

    return im.crop((minx, miny, maxx + 1, maxy + 1))


def trim_image_to_local(
    image_url: str,
    out_dir: str,
    file_stem: str,
    referer: Optional[str] = None,
    ttl_hours: int = 72,
) -> Tuple[str, Optional[str]]:
    """
    image_url -> 로컬 파일 저장(여백 트리밍) -> file:// 절대경로 반환
    반환: (file_url, local_abs_path or None)
    """
    if not image_url:
        return "", None

    if Image is None:
        return image_url, None

    os.makedirs(out_dir, exist_ok=True)
    out_png = os.path.join(out_dir, f"{file_stem}.png")

    if os.path.exists(out_png):
        age_sec = time.time() - os.path.getmtime(out_png)
        if age_sec <= ttl_hours * 3600:
            return _file_url(os.path.abspath(out_png)), os.path.abspath(out_png)

    blob = _download_binary(image_url, referer=referer)
    if not blob:
        return image_url, None

    try:
        from io import BytesIO
        im = Image.open(BytesIO(blob))
        try:
            from PIL import ImageOps
            im = ImageOps.exif_transpose(im)
        except Exception:
            pass

        im = im.convert("RGBA")
        trimmed = _crop_whitespace_pil(im, bg_thresh=245, pad=10)

        if trimmed.size[0] < 120 or trimmed.size[1] < 120:
            trimmed = im

        trimmed.save(out_png, format="PNG", optimize=True)
        return _file_url(os.path.abspath(out_png)), os.path.abspath(out_png)

    except Exception:
        return image_url, None


# -----------------------------
# 전일(이전 result_*.csv) 탐색 및 Δ 계산
# -----------------------------
def find_previous_result_csv(history_dir: str, today_mmdd: str) -> Optional[str]:
    if not os.path.isdir(history_dir):
        return None

    candidates = []
    for fn in os.listdir(history_dir):
        if not fn.lower().startswith("result_") or not fn.lower().endswith(".csv"):
            continue
        m = re.match(r"result_(\d{4})\.csv$", fn, re.IGNORECASE)
        if not m:
            continue
        mmdd = m.group(1)
        if mmdd == today_mmdd:
            continue

        path = os.path.join(history_dir, fn)
        try:
            mtime = os.path.getmtime(path)
        except Exception:
            mtime = 0
        candidates.append((mtime, path))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def load_previous_prices(prev_csv_path: str) -> Dict[str, Dict[str, Optional[int]]]:
    try:
        df = pd.read_csv(prev_csv_path, encoding="utf-8-sig")
    except Exception:
        df = pd.read_csv(prev_csv_path)

    def to_int_or_none(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        try:
            return int(float(v))
        except Exception:
            return None

    prev: Dict[str, Dict[str, Optional[int]]] = {}
    for _, r in df.iterrows():
        code = str(r.get("코드", "")).strip()
        if not code:
            continue
        prev[code] = {"prev_naver": to_int_or_none(r.get("네이버최저가"))}
    return prev


# -----------------------------
# CSV 컬럼 매핑
# -----------------------------
def find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = list(df.columns)
    col_lower_map = {str(c).strip().lower(): c for c in cols}
    for cand in candidates:
        key = cand.strip().lower()
        if key in col_lower_map:
            return col_lower_map[key]
    return None


def get_row_value(row: pd.Series, col: Optional[str], fallback_idx: Optional[int] = None) -> Any:
    if col is not None and col in row.index:
        return row[col]
    if fallback_idx is not None:
        try:
            return row.iloc[fallback_idx]
        except Exception:
            return None
    return None


# -----------------------------
# official_hashes.csv → code -> image_url 매핑
# -----------------------------
def build_official_image_map(csv_path: str) -> Dict[str, str]:
    if not csv_path or not os.path.exists(csv_path):
        log(f"🖼️ official_hashes not found: {csv_path}")
        return {}

    log(f"🖼️ Loading official_hashes: {csv_path}")
    try:
        oh = pd.read_csv(csv_path, encoding="utf-8-sig")
    except Exception:
        oh = pd.read_csv(csv_path)

    need_cols = {"product_name", "image_url"}
    if not need_cols.issubset(set(oh.columns)):
        log(f"🖼️ official_hashes columns missing. found={list(oh.columns)}")
        return {}

    img = oh["image_url"].astype(str)
    bad = (
        img.str.contains("/images/pc/common/ico_", case=False, na=False)
        | img.str.contains("/data/banner/", case=False, na=False)
        | img.str.contains("gift_banner", case=False, na=False)
        | img.str.contains("icon", case=False, na=False)
    )
    oh = oh[~bad].copy()

    img = oh["image_url"].astype(str)
    is_product = img.str.contains("/data/ProductImages/", case=False, na=False)
    oh_prod = oh[is_product].copy()

    if len(oh_prod) == 0:
        log("🖼️ official_hashes: ProductImages rows not found (map empty)")
        return {}

    oh_prod["code"] = (
        oh_prod["image_url"]
        .astype(str)
        .str.extract(r"/([A-Z]\d{2}[A-Z]{2}\d{7})\.(?:jpg|jpeg|png|webp)(?:\?|$)", flags=re.I)[0]
        .str.upper()
    )

    miss = oh_prod["code"].isna()
    if miss.any():
        oh_prod.loc[miss, "code"] = (
            oh_prod.loc[miss, "product_name"]
            .astype(str)
            .str.extract(r"\(([A-Z]\d{2}[A-Z]{2}\d{7})\)")[0]
            .str.upper()
        )

    oh_prod = oh_prod.dropna(subset=["code"]).copy()

    if "aHash64" in oh_prod.columns:
        ah = oh_prod["aHash64"].astype(str).fillna("")
        oh_prod = oh_prod[(ah != "") & (ah != "0")]

    oh_prod = oh_prod.sort_values(["code"]).drop_duplicates("code", keep="first")
    mp = dict(zip(oh_prod["code"], oh_prod["image_url"].astype(str)))

    log(f"🖼️ official image map built: {len(mp):,} codes")
    return mp


# -----------------------------
# HTML 생성
# -----------------------------
def _safe_attr(s: str) -> str:
    if s is None:
        return ""
    return (
        str(s)
        .replace("&", "&amp;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def build_html_portal(rows: List[Dict[str, Any]], meta: Dict[str, Any]) -> str:
    try:
        now_kst = _kst_now_str(fmt="%Y-%m-%d %H:%M KST")
    except Exception:
        now_kst = _kst_now_str(fmt="%Y-%m-%d %H:%M")
    period_label = meta.get("period_label") or f"Execution snapshot · Compared to previous run · {now_kst}"
    meta["period_label"] = period_label

    groups: Dict[str, List[Dict[str, Any]]] = {"C7": [], "C6": [], "전체": []}
    for r in rows:
        code = (r.get("코드") or "").upper()
        if code.startswith("C7"):
            groups["C7"].append(r)
        elif code.startswith("C6"):
            groups["C6"].append(r)
        groups["전체"].append(r)

    ordered_tabs = ["C7", "C6", "전체"]
    active_tabs = [t for t in ordered_tabs if len(groups.get(t, [])) > 0] or ["전체"]

    total_cnt = len(rows)
    missing_cnt = sum(1 for r in rows if r.get("네이버최저가") is None)
    diff_pos_cnt = sum(1 for r in rows if isinstance(r.get("가격차이"), int) and r["가격차이"] > 0)

    def gap_abs(r):
        d = r.get("가격차이")
        return abs(d) if isinstance(d, int) else -1

    top_gap = sorted(rows, key=gap_abs, reverse=True)[:10]
    top_gap_codes = [str(r.get("코드", "")).strip() for r in top_gap if str(r.get("코드", "")).strip()]
    top_gap_codes_json = json.dumps(top_gap_codes, ensure_ascii=False)

    tab_menu_html = ""
    content_area_html = ""

    for i, tab in enumerate(active_tabs):
        active = (i == 0)
        active_class = "bg-[#002d72] text-white shadow-lg" if active else "bg-white/50 text-slate-500 hover:bg-white"
        display_style = "grid" if active else "none"
        active_attr = "1" if active else "0"

        tab_menu_html += f"""
        <button onclick="switchTab('{tab}')" id="tab-{tab}" data-active="{active_attr}"
          class="tab-btn px-6 py-3 rounded-2xl font-black transition-all text-sm {active_class}">
          {tab} <span class="ml-1 opacity-60 text-xs">{len(groups[tab])}</span>
        </button>
        """

        cards = ""
        for r in groups[tab]:
            code = r.get("코드", "") or ""
            name_en = r.get("상품명(영문)", "") or ""
            name_ko = r.get("상품명(한글)", "") or ""
            official = r.get("공식몰가")
            naver = r.get("네이버최저가")
            diff = r.get("가격차이")
            mall = r.get("최저가몰", "") or ""
            link = r.get("링크", "") or ""

            img_final = r.get("이미지URL", "") or ""
            img_official = r.get("공식이미지URL", "") or ""
            img_naver = r.get("네이버이미지URL", "") or ""
            img_local = r.get("trimmed_local_path", "") or ""

            prev_naver = r.get("prev_naver")
            delta_naver = r.get("delta_naver")
            conf = r.get("confidence", 0)
            top3 = r.get("top3", []) or []

            official_s = f"{official:,}원" if isinstance(official, int) else "-"
            naver_s = f"{naver:,}원" if isinstance(naver, int) else "미검색"
            diff_s = f"{diff:+,}원" if isinstance(diff, int) else "-"
            prev_s = f"{prev_naver:,}원" if isinstance(prev_naver, int) else "-"
            delta_s = f"{delta_naver:+,}원" if isinstance(delta_naver, int) else "-"

            badge = ""
            if isinstance(diff, int):
                badge = f"""
                <span class="px-3 py-1 rounded-full text-[10px] font-black {'bg-red-500/10 text-red-600' if diff > 0 else 'bg-emerald-500/10 text-emerald-700'}">
                  {'공식↑' if diff > 0 else '공식↓'} {diff_s}
                </span>
                """

            delta_badge = ""
            if isinstance(delta_naver, int):
                if delta_naver > 0:
                    cls = "bg-amber-500/10 text-amber-700"
                elif delta_naver < 0:
                    cls = "bg-sky-500/10 text-sky-700"
                else:
                    cls = "bg-slate-500/10 text-slate-700"
                delta_badge = f"""
                <span class="px-3 py-1 rounded-full text-[10px] font-black {cls}">
                  Δ최저가 {delta_s}
                </span>
                """

            if conf >= 3:
                conf_color = "bg-emerald-500/10 text-emerald-700"
            elif conf == 2:
                conf_color = "bg-amber-500/10 text-amber-800"
            elif conf == 1:
                conf_color = "bg-red-500/10 text-red-600"
            else:
                conf_color = "bg-slate-500/10 text-slate-700"

            conf_badge = f"""
            <span class="px-3 py-1 rounded-full text-[10px] font-black {conf_color}">
              Match {conf}/5
            </span>
            """

            src_badge = ""
            if img_final:
                if img_final == img_official and img_official:
                    src_badge = """<span class="px-3 py-1 rounded-full text-[10px] font-black bg-blue-500/10 text-blue-700">IMG: OFFICIAL</span>"""
                elif img_final == img_naver and img_naver:
                    src_badge = """<span class="px-3 py-1 rounded-full text-[10px] font-black bg-purple-500/10 text-purple-700">IMG: NAVER</span>"""
                else:
                    src_badge = """<span class="px-3 py-1 rounded-full text-[10px] font-black bg-slate-500/10 text-slate-700">IMG: MIX</span>"""

            top3_lines = ""
            for idx, it in enumerate(top3[:3], start=1):
                lp = it.get("lprice")
                mn = it.get("mallName", "") or ""
                lk = it.get("link", "") or ""
                lp_s = f"{int(lp):,}원" if isinstance(lp, int) else "-"
                top3_lines += f"""
                <div class="flex items-center justify-between gap-3 py-2">
                  <div class="text-xs font-black text-slate-700">#{idx} {lp_s}</div>
                  <div class="text-[11px] font-bold text-slate-500 line-clamp-1 flex-1">{_safe_attr(mn)}</div>
                  <a href="{_safe_attr(lk)}" target="_blank" class="text-[11px] font-black text-blue-700 hover:underline">link</a>
                </div>
                """

            top3_block = f"""
            <details class="mt-2">
              <summary class="cursor-pointer select-none text-[11px] font-black text-slate-600">
                Top3 최저가 보기
              </summary>
              <div class="mt-3 p-4 rounded-2xl bg-white/60 border border-white">
                {top3_lines if top3_lines else '<div class="text-xs font-bold text-slate-500">Top3 데이터 없음</div>'}
              </div>
            </details>
            """

            missing_flag = 1 if (naver is None) else 0
            diff_pos_flag = 1 if (isinstance(diff, int) and diff > 0) else 0
            diff_abs = abs(diff) if isinstance(diff, int) else -1
            naver_num = naver if isinstance(naver, int) else -1
            official_num = official if isinstance(official, int) else -1
            delta_num = delta_naver if isinstance(delta_naver, int) else 10**18
            conf_num = conf if isinstance(conf, int) else 0

            data_code = _safe_attr(code.lower())
            data_name_en = _safe_attr(name_en.lower())
            data_name_ko = _safe_attr(name_ko.lower())

            img_block = ""
            if img_final.strip():
                img_block = f"""
                <div class="mb-4">
                  <div class="w-full img-box rounded-2xl border border-white/80 bg-white/60 overflow-hidden relative">
                    <label class="chk-float inline-flex items-center gap-2 text-[11px] font-black text-slate-700 cursor-pointer select-none">
                      <input type="checkbox" class="w-4 h-4 accent-[#002d72] chk"
                        onchange="toggleCheck('{_safe_attr(code)}', this.checked)" />
                      CHECK
                    </label>

                    <img
                      src="{_safe_attr(img_final)}"
                      data-src-raw="{_safe_attr(r.get("raw_image_url","") or "")}"
                      alt="{_safe_attr(name_en or name_ko)}"
                      class="img-fit"
                      loading="lazy"
                      onclick="openImg('{_safe_attr(img_final)}', '{_safe_attr(img_local)}')"
                      onerror="this.classList.add('hidden'); if(this.parentElement && this.parentElement.nextElementSibling) this.parentElement.nextElementSibling.classList.remove('hidden');"
                    />
                  </div>
                  <div class="hidden w-full img-box rounded-2xl border border-white/80 bg-white/60 flex items-center justify-center">
                    <i class="fa-solid fa-image text-slate-400 text-2xl"></i>
                  </div>
                </div>
                """

            title_main = _safe_attr(name_ko) if name_ko else _safe_attr(name_en)
            title_sub = _safe_attr(name_en) if name_ko else ""

            cards += f"""
            <div class="glass-card p-6 border-white/80 hover:scale-[1.01] transition-transform card-item flex flex-col"
              data-code="{data_code}" data-nameen="{data_name_en}" data-nameko="{data_name_ko}"
              data-missing="{missing_flag}" data-diffpos="{diff_pos_flag}"
              data-diff="{diff if isinstance(diff,int) else ''}" data-diffabs="{diff_abs}"
              data-naver="{naver_num}" data-official="{official_num}"
              data-delta="{delta_num}" data-conf="{conf_num}"
              data-code-raw="{_safe_attr(code)}">

              {img_block}

              <div class="flex items-start justify-between gap-3 mb-4">
                <div class="min-w-0">
                  <div class="text-xs font-black tracking-widest text-slate-400 uppercase mb-2">{_safe_attr(code)}</div>
                  <div class="text-slate-900 font-extrabold leading-snug line-clamp-2">{title_main}</div>
                  <div class="text-[11px] font-bold text-slate-500 mt-1 line-clamp-1">{title_sub}</div>

                  <div class="mt-3 flex flex-wrap gap-2">
                    {badge} {delta_badge} {conf_badge} {src_badge}
                  </div>

                  {top3_block}
                </div>
              </div>

              <div class="grid grid-cols-2 gap-3 mb-4">
                <div class="p-4 rounded-2xl bg-white/60 border border-white">
                  <div class="text-[10px] font-black tracking-widest text-slate-400 uppercase mb-1">공식몰가</div>
                  <div class="text-lg font-black text-slate-900">{official_s}</div>
                </div>
                <div class="p-4 rounded-2xl bg-white/60 border border-white">
                  <div class="text-[10px] font-black tracking-widest text-slate-400 uppercase mb-1">네이버최저가</div>
                  <div class="text-lg font-black text-slate-900">{naver_s}</div>
                  <div class="text-[10px] font-bold text-slate-500 mt-1">{_safe_attr(mall)}</div>
                </div>
              </div>

              <div class="grid grid-cols-2 gap-3 mb-5">
                <div class="p-4 rounded-2xl bg-white/60 border border-white">
                  <div class="text-[10px] font-black tracking-widest text-slate-400 uppercase mb-1">전일 최저가</div>
                  <div class="text-base font-black text-slate-900">{prev_s}</div>
                </div>
                <div class="p-4 rounded-2xl bg-white/60 border border-white">
                  <div class="text-[10px] font-black tracking-widest text-slate-400 uppercase mb-1">Δ 최저가</div>
                  <div class="text-base font-black text-slate-900">{delta_s}</div>
                </div>
              </div>

              <div class="mb-4">
                <div class="text-[10px] font-black uppercase tracking-[0.3em] text-slate-400 mb-2 flex items-center gap-2">
                  <i class="fa-solid fa-note-sticky"></i> Memo
                </div>
                <textarea class="w-full input-glass text-sm font-bold text-slate-800" rows="2"
                  placeholder="메모를 남겨두면 이 브라우저에 저장돼요 (예: MD 확인 필요 / 옵션가 의심)"
                  oninput="saveMemo('{_safe_attr(code)}', this.value)"></textarea>
              </div>

              <div class="mt-auto flex items-center justify-between pt-4 border-t border-slate-100">
                <span class="text-[10px] font-bold text-slate-400 uppercase tracking-widest">가격차이: {diff_s}</span>
                <a href="{_safe_attr(link)}" target="_blank"
                  class="px-4 py-2 bg-[#002d72] text-white text-[10px] font-black rounded-xl hover:bg-blue-600 transition-colors flex items-center gap-2">
                  최저가 링크 <i class="fa-solid fa-arrow-up-right"></i>
                </a>
              </div>
            </div>
            """

        content_area_html += f"""
        <div id="content-{tab}" class="tab-content grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6"
          style="display: {display_style};">
          {cards if cards else '<div class="text-slate-500 font-bold">데이터가 없습니다.</div>'}
        </div>
        """

    now_str = meta.get("generated_at", datetime.now().strftime("%Y-%m-%d %H:%M"))
    prev_csv_used = meta.get("prev_csv_used")
    prev_label = os.path.basename(prev_csv_used) if prev_csv_used else "없음(비교 불가)"

    rows_json = json.dumps(rows, ensure_ascii=False)

    html_tpl = r"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <title>Columbia E-Comm | Price Monitoring</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200;400;600;800&display=swap');
    :root { --brand: #002d72; --bg0: #f6f8fb; --bg1: #eef3f9; }
    body { background: linear-gradient(180deg, var(--bg0), var(--bg1)); font-family: 'Plus Jakarta Sans', sans-serif; color: #0f172a; min-height: 100vh; }
    .glass-card { background: rgba(255,255,255,0.55); backdrop-filter: blur(20px); border: 1px solid rgba(255,255,255,0.7); border-radius: 30px; box-shadow: 0 20px 50px rgba(0,45,114,0.05); }
    .sidebar { background: rgba(255,255,255,0.7); backdrop-filter: blur(15px); border-right: 1px solid rgba(255,255,255,0.8); }
    .line-clamp-1 { display: -webkit-box; -webkit-line-clamp: 1; -webkit-box-orient: vertical; overflow: hidden; }
    .line-clamp-2 { display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }
    .input-glass { background: rgba(255,255,255,0.65); border: 1px solid rgba(255,255,255,0.8); border-radius: 18px; padding: 14px 16px; outline: none; }
    .input-glass:focus { box-shadow: 0 0 0 4px rgba(0,45,114,0.10); border-color: rgba(0,45,114,0.25); }
    .chip { border-radius: 9999px; padding: 10px 14px; font-weight: 900; font-size: 12px; border: 1px solid rgba(255,255,255,0.85); background: rgba(255,255,255,0.60); color: #334155; }
    .chip.active { background: rgba(0,45,114,0.95); color: white; border-color: rgba(0,45,114,1); box-shadow: 0 10px 30px rgba(0,45,114,0.15); }
    .small-label { font-size: 10px; letter-spacing: 0.3em; text-transform: uppercase; font-weight: 900; }
    .summary-card { border-radius: 26px; background: rgba(255,255,255,0.55); border: 1px solid rgba(255,255,255,0.75); backdrop-filter: blur(18px); box-shadow: 0 20px 50px rgba(0,45,114,0.05); padding: 18px 20px; }
    .overlay { position: fixed; inset: 0; background: rgba(255,255,255,0.65); backdrop-filter: blur(10px); display: none; align-items: center; justify-content: center; z-index: 9999; }
    .overlay.show { display: flex; }
    .spinner { width: 56px; height: 56px; border-radius: 9999px; border: 6px solid rgba(0,0,0,0.08); border-top-color: rgba(0,45,114,0.95); animation: spin 0.9s linear infinite; }
    @keyframes spin { to { transform: rotate(360deg); } }

    .img-box { height: 240px; }
    @media (min-width: 1024px){ .img-box { height: 280px; } }
    .img-fit {
      width: 100%;
      height: 100%;
      object-fit: contain;
      transform: scale(1.03);
      transition: transform .18s ease;
      cursor: zoom-in;
      display:block;
    }
    .img-fit:hover { transform: scale(1.10); }

    .chk-float{
      position:absolute;
      top:12px;
      right:12px;
      z-index:5;
      background: rgba(255,255,255,0.75);
      border: 1px solid rgba(255,255,255,0.9);
      border-radius: 9999px;
      padding: 8px 10px;
      backdrop-filter: blur(10px);
      box-shadow: 0 10px 30px rgba(0,0,0,0.06);
    }

    .grid-3 { }
    .grid-4 { }
    @media (min-width: 1024px){
      .grid-3 { grid-template-columns: repeat(3, minmax(0, 1fr)); }
      .grid-4 { grid-template-columns: repeat(4, minmax(0, 1fr)); }
    }

    /* ===== EMBED MODE (중복 UI 제거) ===== */
    body.embedded aside { display:none !important; }
    body.embedded header { display:none !important; }
    body.embedded .sidebar { display:none !important; }
    body.embedded main { padding: 24px !important; }
    body.embedded .sticky { position: static !important; }
  </style>
</head>
<body class="flex">

  <div id="overlay" class="overlay">
    <div class="glass-card px-8 py-7 flex items-center gap-4">
      <div class="spinner"></div>
      <div>
        <div class="text-sm font-black text-slate-900">Processing...</div>
        <div id="overlayMsg" class="text-xs font-bold text-slate-500 mt-1">잠시만요</div>
      </div>
    </div>
  </div>

  <aside class="w-72 h-screen sticky top-0 sidebar hidden lg:flex flex-col p-8">
    <div class="flex items-center gap-4 mb-16 px-2">
      <div class="w-12 h-12 bg-[#002d72] rounded-2xl flex items-center justify-center text-white shadow-xl shadow-blue-900/20">
        <i class="fa-solid fa-tags text-xl"></i>
      </div>
      <div>
        <div class="text-xl font-black tracking-tighter italic">M-OS <span class="text-blue-600 font-extrabold">PRO</span></div>
        <div class="text-[9px] font-black uppercase tracking-[0.3em] text-slate-400">Price Monitoring</div>
      </div>
    </div>

    <div class="mt-auto pt-8 text-xs font-bold text-slate-500">
      <div class="small-label text-blue-600 mb-2">History</div>
      <div>전일 비교 파일: <span class="font-black text-slate-700">__PREV_LABEL__</span></div>
    </div>
  </aside>

  <main class="flex-1 p-8 md:p-16">
    <header class="flex flex-col md:flex-row md:items-center justify-between mb-10 gap-6">
      <div>
        <h1 class="text-5xl font-black tracking-tight text-slate-900 mb-4">Naver Lowest Price Monitor</h1>
        <div class="text-sm text-slate-500 mt-2">__PERIOD_LABEL__</div>
        <p class="text-slate-500 text-lg font-medium italic">공식몰가 vs 네이버 쇼핑 최저가 자동 비교</p>
      </div>
      <div class="glass-card px-6 py-4 flex items-center gap-4">
        <div class="flex h-3 w-3 relative">
          <span class="animate-ping absolute h-full w-full rounded-full bg-blue-400 opacity-75"></span>
          <span class="relative inline-flex rounded-full h-3 w-3 bg-blue-600"></span>
        </div>
        <span class="text-sm font-black text-slate-800 tracking-widest uppercase">__NOW_STR__</span>
      </div>
    </header>

    <section class="mb-10">
      <div class="grid grid-cols-1 lg:grid-cols-4 gap-4">
        <div class="summary-card">
          <div class="small-label text-blue-600 mb-2"><i class="fa-solid fa-list mr-2"></i>Total</div>
          <div class="text-3xl font-black">__TOTAL_CNT__</div>
          <div class="text-xs font-bold text-slate-500 mt-2">전체 결과 상품 수</div>
        </div>
        <div class="summary-card">
          <div class="small-label text-red-600 mb-2"><i class="fa-solid fa-triangle-exclamation mr-2"></i>Diff &gt; 0</div>
          <div class="text-3xl font-black">__DIFF_POS_CNT__</div>
          <div class="text-xs font-bold text-slate-500 mt-2">공식이 더 비싼 상품</div>
        </div>
        <div class="summary-card">
          <div class="small-label text-slate-600 mb-2"><i class="fa-solid fa-magnifying-glass mr-2"></i>Missing</div>
          <div class="text-3xl font-black">__MISSING_CNT__</div>
          <div class="text-xs font-bold text-slate-500 mt-2">네이버 미검색</div>
        </div>
        <div class="summary-card">
          <div class="small-label text-blue-600 mb-3"><i class="fa-solid fa-file-arrow-down mr-2"></i>Export</div>
          <div class="flex flex-col gap-2">
            <button onclick="downloadCSVAll()" class="px-4 py-3 rounded-2xl bg-[#002d72] text-white font-black text-sm hover:bg-blue-600 transition-colors">
              전체 CSV 다운로드
            </button>
            <button onclick="downloadCSVFiltered()" class="px-4 py-3 rounded-2xl bg-white/70 text-slate-800 font-black text-sm border border-white hover:bg-white transition-colors">
              현재 결과만 CSV 다운로드 (체크/메모 포함)
            </button>
          </div>
        </div>
      </div>

      <div class="mt-5 flex flex-wrap gap-2 items-center">
        <button id="chip-diffpos" class="chip" onclick="toggleQuickFilter('diffpos')">Diff&gt;0만 보기</button>
        <button id="chip-missing" class="chip" onclick="toggleQuickFilter('missing')">미검색만 보기</button>
        <button id="chip-topgap" class="chip" onclick="toggleQuickFilter('topgap')">Top Gap(10)만 보기</button>

        <div class="ml-auto flex items-center gap-2">
          <div class="text-xs font-black text-slate-500">Grid</div>
          <button id="grid3" class="chip" onclick="setGridMode(3)">3열</button>
          <button id="grid4" class="chip" onclick="setGridMode(4)">4열</button>
          <div class="text-xs font-black text-slate-500">현재 탭 결과: <span id="matchCount" class="text-slate-900">-</span>개</div>
        </div>
      </div>
    </section>

    <section class="glass-card p-8 mb-10">
      <div class="flex flex-col lg:flex-row gap-4 lg:items-end">
        <div class="flex-1">
          <div class="text-[10px] font-black uppercase tracking-[0.3em] text-blue-600 mb-2 flex items-center gap-2">
            <i class="fa-solid fa-magnifying-glass"></i> Search
          </div>
          <div class="text-slate-500 text-sm font-bold mb-4">
            상품명(영문/한글) 또는 상품코드로 필터링 —
            <span class="font-black text-slate-700">Search 버튼 또는 Enter로 적용</span>
          </div>

          <div class="grid grid-cols-1 gap-3">
            <input id="qAll" class="input-glass w-full font-bold text-slate-800"
              placeholder="상품명(영문/한글) 또는 상품코드 (ex. jacket / 바람막이 / C7XXXX...)" />
          </div>
        </div>

        <div class="flex flex-col gap-3 min-w-[280px]">
          <div class="text-[10px] font-black uppercase tracking-[0.3em] text-blue-600 flex items-center gap-2">
            <i class="fa-solid fa-arrow-down-wide-short"></i> Sort
          </div>
          <select id="sortMode" class="input-glass font-black text-slate-800">
            <option value="diffabs_desc">가격차이 |abs| 큰 순</option>
            <option value="diff_desc">가격차이 큰 순(공식-네이버)</option>
            <option value="diff_asc">가격차이 작은 순(공식-네이버)</option>
            <option value="naver_asc">네이버최저가 낮은 순</option>
            <option value="naver_desc">네이버최저가 높은 순</option>
            <option value="official_desc">공식몰가 높은 순</option>
            <option value="code_asc">상품코드 오름차순</option>
            <option value="delta_asc">Δ최저가 하락 큰 순</option>
            <option value="delta_desc">Δ최저가 상승 큰 순</option>
            <option value="conf_desc">Match 점수 높은 순</option>
          </select>

          <div class="flex gap-3">
            <button onclick="onSearchClick()" class="px-6 py-4 bg-[#002d72] text-white font-black rounded-2xl hover:bg-blue-600 transition-colors flex items-center gap-2">
              <i class="fa-solid fa-magnifying-glass"></i> Search
            </button>
            <button onclick="onApplyClick()" class="px-6 py-4 bg-white/70 text-slate-700 font-black rounded-2xl hover:bg-white transition-colors border border-white flex items-center gap-2">
              <i class="fa-solid fa-arrow-down-wide-short"></i> Apply Sort
            </button>
            <button onclick="resetAll()" class="px-6 py-4 bg-white/70 text-slate-700 font-black rounded-2xl hover:bg-white transition-colors border border-white flex items-center gap-2">
              <i class="fa-solid fa-rotate-left"></i> Reset
            </button>
          </div>
        </div>
      </div>

      <div id="noResults" class="hidden mt-5 glass-card p-5 text-center text-slate-700 font-black">
        검색 결과가 없습니다.
      </div>
    </section>

    <section>
      <div class="flex flex-wrap gap-2 mb-8">__TAB_MENU__</div>
      <div class="min-h-[500px]">__CONTENT_AREA__</div>
    </section>
  </main>

  <div id="imgModal" class="overlay" onclick="closeImg()">
    <div class="glass-card p-4 max-w-[92vw] max-h-[92vh]" onclick="event.stopPropagation()">
      <div class="flex items-center justify-between mb-2 gap-2">
        <div class="text-xs font-black text-slate-600" id="imgModalInfo"></div>
        <button onclick="closeImg()" class="px-3 py-2 rounded-xl bg-white/70 border border-white font-black text-xs">닫기</button>
      </div>
      <img id="imgModalEl" src="" class="max-w-[88vw] max-h-[84vh] object-contain rounded-2xl bg-white" />
    </div>
  </div>

<script>
  const ALL_ROWS = __ROWS_JSON__;
  const TOP_GAP_CODES = __TOP_GAP_CODES_JSON__;
  const quick = { diffpos: false, missing: false, topgap: false };

  const state = { q: "", sortMode: "diffabs_desc", hasSearched: false, gridMode: 3 };

  const overlay = document.getElementById('overlay');
  const overlayMsg = document.getElementById('overlayMsg');

  function showOverlay(msg) { overlayMsg.textContent = msg || "잠시만요"; overlay.classList.add('show'); }
  function hideOverlay() { overlay.classList.remove('show'); }
  function runWithOverlay(msg, fn) {
    showOverlay(msg);
    setTimeout(() => { try { fn(); } finally { requestAnimationFrame(() => hideOverlay()); } }, 0);
  }

  function getActiveTabName() {
    const activeBtn = document.querySelector('.tab-btn[data-active="1"]');
    if (!activeBtn) return null;
    return activeBtn.id.replace('tab-', '');
  }
  function getActiveContainer() {
    const tab = getActiveTabName();
    if (!tab) return null;
    return document.getElementById('content-' + tab);
  }

  function updateCount() {
    const container = getActiveContainer();
    if (!container) return;
    const visibleCards = container.querySelectorAll('.card-item:not([data-hidden="1"])');
    const cnt = visibleCards.length;
    document.getElementById('matchCount').innerText = cnt.toString();

    const noResults = document.getElementById('noResults');
    if (noResults) {
      if (state.hasSearched && cnt === 0) noResults.classList.remove('hidden');
      else noResults.classList.add('hidden');
    }
  }

  function keyCheck(code) { return 'chk_' + code; }
  function keyMemo(code) { return 'memo_' + code; }
  function toggleCheck(code, checked) { try { localStorage.setItem(keyCheck(code), checked ? '1' : '0'); } catch(e) {} }
  function saveMemo(code, text) { try { localStorage.setItem(keyMemo(code), text || ''); } catch(e) {} }

  function hydrateCardState() {
    document.querySelectorAll('.card-item').forEach(card => {
      const code = card.getAttribute('data-code-raw') || '';
      const chk = card.querySelector('input.chk');
      if (chk) {
        try { chk.checked = (localStorage.getItem(keyCheck(code)) === '1'); } catch(e) {}
      }
      const ta = card.querySelector('textarea');
      if (ta) {
        try { const v = localStorage.getItem(keyMemo(code)); if (v !== null) ta.value = v; } catch(e) {}
      }
    });
  }

  function toggleQuickFilter(name) {
    quick[name] = !quick[name];
    const chip = document.getElementById('chip-' + name);
    if (chip) chip.classList.toggle('active', quick[name]);
    onApplyClick();
  }

  function passesFilters(card) {
    if (!state.hasSearched) return true;

    const nameEn = card.getAttribute('data-nameen') || '';
    const nameKo = card.getAttribute('data-nameko') || '';
    const code = card.getAttribute('data-code') || '';
    const missing = card.getAttribute('data-missing') === '1';
    const diffpos = card.getAttribute('data-diffpos') === '1';
    const codeRaw = card.getAttribute('data-code-raw') || '';

    const q = state.q || "";
    const ok = !q || nameEn.includes(q) || nameKo.includes(q) || code.includes(q);

    if (!ok) return false;
    if (quick.diffpos && !diffpos) return false;
    if (quick.missing && !missing) return false;
    if (quick.topgap && !TOP_GAP_CODES.includes(codeRaw)) return false;

    return true;
  }

  function sortCards(container) {
    const mode = state.sortMode;
    const cards = Array.from(container.querySelectorAll('.card-item'));

    const getNum = (el, attr, fallback=0) => {
      const v = el.getAttribute(attr);
      if (v === null || v === '' || v === undefined) return fallback;
      const n = Number(v);
      return isNaN(n) ? fallback : n;
    };
    const getStr = (el, attr) => (el.getAttribute(attr) || '').toString();

    let cmp;
    switch(mode) {
      case 'diffabs_desc': cmp = (a,b) => getNum(b,'data-diffabs',-1) - getNum(a,'data-diffabs',-1); break;
      case 'diff_desc':    cmp = (a,b) => getNum(b,'data-diff',-1e18) - getNum(a,'data-diff',-1e18); break;
      case 'diff_asc':     cmp = (a,b) => getNum(a,'data-diff', 1e18) - getNum(b,'data-diff', 1e18); break;
      case 'naver_asc':    cmp = (a,b) => getNum(a,'data-naver',1e18) - getNum(b,'data-naver',1e18); break;
      case 'naver_desc':   cmp = (a,b) => getNum(b,'data-naver',-1) - getNum(a,'data-naver',-1); break;
      case 'official_desc':cmp = (a,b) => getNum(b,'data-official',-1) - getNum(a,'data-official',-1); break;
      case 'code_asc':     cmp = (a,b) => getStr(a,'data-code-raw').localeCompare(getStr(b,'data-code-raw'), 'en'); break;
      case 'delta_asc':    cmp = (a,b) => getNum(a,'data-delta',1e18) - getNum(b,'data-delta',1e18); break;
      case 'delta_desc':   cmp = (a,b) => getNum(b,'data-delta',-1e18) - getNum(a,'data-delta',-1e18); break;
      case 'conf_desc':    cmp = (a,b) => getNum(b,'data-conf',0) - getNum(a,'data-conf',0); break;
      default:             cmp = (a,b) => 0;
    }
    cards.sort(cmp);
    cards.forEach(c => container.appendChild(c));
  }

  function applyGridMode() {
    document.querySelectorAll('.tab-content').forEach(el => {
      el.classList.remove('grid-3','grid-4');
      el.classList.add(state.gridMode === 4 ? 'grid-4' : 'grid-3');
    });

    const b3 = document.getElementById('grid3');
    const b4 = document.getElementById('grid4');
    if (b3 && b4){
      b3.classList.toggle('active', state.gridMode === 3);
      b4.classList.toggle('active', state.gridMode === 4);
    }
    try { localStorage.setItem('gridMode', String(state.gridMode)); } catch(e){}
  }

  function setGridMode(n){
    runWithOverlay("Applying grid...", () => {
      state.gridMode = (n === 4 ? 4 : 3);
      applyGridMode();
    });
  }

  function applyAll() {
    const container = getActiveContainer();
    if (!container) return;

    const cards = container.querySelectorAll('.card-item');
    cards.forEach(card => {
      const ok = passesFilters(card);
      if (ok) {
        card.style.display = '';
        card.removeAttribute('data-hidden');
      } else {
        card.style.display = 'none';
        card.setAttribute('data-hidden', '1');
      }
    });

    sortCards(container);
    updateCount();
  }

  function onSearchClick() {
    runWithOverlay("Searching...", () => {
      state.q = (document.getElementById('qAll').value || '').trim().toLowerCase();
      state.sortMode = (document.getElementById('sortMode').value || 'diffabs_desc');
      state.hasSearched = true;
      applyAll();
    });
  }

  function onApplyClick() {
    runWithOverlay("Applying sort...", () => {
      state.sortMode = (document.getElementById('sortMode').value || 'diffabs_desc');
      applyAll();
    });
  }

  function resetAll() {
    runWithOverlay("Resetting...", () => {
      Object.keys(quick).forEach(k => {
        quick[k] = false;
        const chip = document.getElementById('chip-' + k);
        if (chip) chip.classList.remove('active');
      });

      document.getElementById('qAll').value = '';
      document.getElementById('sortMode').value = 'diffabs_desc';

      state.q = "";
      state.sortMode = "diffabs_desc";
      state.hasSearched = false;

      document.querySelectorAll('.card-item').forEach(card => {
        card.style.display = '';
        card.removeAttribute('data-hidden');
      });

      applyAll();
    });
  }

  function switchTab(tab) {
    runWithOverlay("Switching tab...", () => {
      document.querySelectorAll('.tab-content').forEach(el => el.style.display = 'none');
      document.getElementById('content-' + tab).style.display = 'grid';

      document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.setAttribute('data-active','0');
        btn.classList.remove('bg-[#002d72]','text-white','shadow-lg');
        btn.classList.add('bg-white/50','text-slate-500');
      });

      const activeBtn = document.getElementById('tab-' + tab);
      activeBtn.setAttribute('data-active','1');
      activeBtn.classList.add('bg-[#002d72]','text-white','shadow-lg');
      activeBtn.classList.remove('bg-white/50','text-slate-500');

      applyAll();
    });
  }

  function toCSV(rows) {
    const cols = [
      "코드","상품명(영문)","상품명(한글)","공식몰가",
      "네이버최저가","가격차이","최저가몰","링크",
      "원본이미지URL","이미지URL","공식이미지URL","네이버이미지URL",
      "confidence","prev_naver","delta_naver","checked","memo"
    ];
    const escape = (v) => {
      if (v === null || v === undefined) return '';
      const s = String(v);
      if (s.includes('"') || s.includes(',') || s.includes('\n') || s.includes('\r')) {
        return '"' + s.replace(/"/g,'""') + '"';
      }
      return s;
    };

    let lines = [];
    lines.push(cols.join(','));

    rows.forEach(r => {
      const code = r["코드"] || '';
      let checked = '';
      let memo = '';
      try {
        checked = (localStorage.getItem('chk_' + code) === '1') ? '1' : '0';
        memo = localStorage.getItem('memo_' + code) || '';
      } catch(e) {}

      const enriched = Object.assign({}, r, { checked: checked, memo: memo });
      const line = cols.map(c => escape(enriched[c]));
      lines.push(line.join(','));
    });

    return lines.join('\r\n');
  }

  function downloadBlob(filename, content, mime) {
    const withBom = '\ufeff' + content;
    const blob = new Blob([withBom], {type: mime});
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  function downloadCSVAll() {
    const csv = toCSV(ALL_ROWS);
    const fname = 'result_all_' + new Date().toISOString().slice(0,10).replaceAll('-','') + '.csv';
    downloadBlob(fname, csv, 'text/csv;charset=utf-8;');
  }

  function getFilteredRowsFromActiveTab() {
    const container = getActiveContainer();
    if (!container) return [];
    const visible = container.querySelectorAll('.card-item:not([data-hidden="1"])');
    const codes = Array.from(visible).map(el => el.getAttribute('data-code-raw') || '');
    const set = new Set(codes);
    return ALL_ROWS.filter(r => set.has(r["코드"]));
  }

  function downloadCSVFiltered() {
    const rows = getFilteredRowsFromActiveTab();
    const csv = toCSV(rows);
    const tab = getActiveTabName() || 'tab';
    const fname = 'result_' + tab + '_filtered_' + new Date().toISOString().slice(0,10).replaceAll('-','') + '.csv';
    downloadBlob(fname, csv, 'text/csv;charset=utf-8;');
  }

  function bindEnterToSearch(inputId) {
    const el = document.getElementById(inputId);
    if (!el) return;
    el.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') { e.preventDefault(); onSearchClick(); }
    });
  }

  function openImg(src, localPath){
    const m = document.getElementById('imgModal');
    const el = document.getElementById('imgModalEl');
    const info = document.getElementById('imgModalInfo');
    el.src = src || '';
    info.textContent = localPath ? ('trimmed: ' + localPath) : '';
    m.classList.add('show');
  }
  function closeImg(){
    const m = document.getElementById('imgModal');
    const el = document.getElementById('imgModalEl');
    const info = document.getElementById('imgModalInfo');
    el.src = '';
    info.textContent = '';
    m.classList.remove('show');
  }

  document.addEventListener('DOMContentLoaded', () => {
    hydrateCardState();
    bindEnterToSearch('qAll');

    try {
      const gm = localStorage.getItem('gridMode');
      if (gm === '4') state.gridMode = 4;
    } catch(e){}
    applyGridMode();

    runWithOverlay("Rendering...", () => { applyAll(); });
  });
</script>

<!-- ✅✅✅ EMBED 판별 로직: iframe OR ?embed=1이면 embedded -->
<script>
  (function () {
    const params = new URLSearchParams(window.location.search);
    const forceEmbed =
      params.get("embed") === "1" ||
      params.get("embedded") === "1" ||
      params.has("embed") ||
      params.has("embedded");

    let isIframe = false;
    try { isIframe = (window.self !== window.top); }
    catch (e) { isIframe = true; }

    if (forceEmbed || isIframe) {
      document.body.classList.add("embedded");
    }
  })();
</script>

</body>
</html>
"""

    html = (html_tpl
            .replace("__PREV_LABEL__", _safe_attr(prev_label))
            .replace("__NOW_STR__", _safe_attr(now_str))
            .replace("__TOTAL_CNT__", str(total_cnt))
            .replace("__DIFF_POS_CNT__", str(diff_pos_cnt))
            .replace("__MISSING_CNT__", str(missing_cnt))
            .replace("__TAB_MENU__", tab_menu_html)
            .replace("__CONTENT_AREA__", content_area_html)
            .replace("__PERIOD_LABEL__", period_label)
            .replace("__ROWS_JSON__", rows_json)
            .replace("__TOP_GAP_CODES_JSON__", top_gap_codes_json)
            )
    return html


# -----------------------------
# 메인
# -----------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=None, help="입력 CSV 경로(미지정 시 자동탐색)")
    parser.add_argument("--output_csv", default=None, help="결과 CSV 출력 경로(기본: result_MMDD.csv)")
    parser.add_argument("--output_html", default="marketing_portal_final.html", help="결과 HTML 출력 경로")
    parser.add_argument("--delay", type=float, default=0.15, help="API 호출 간 딜레이(초)")

    parser.add_argument("--min_price", type=int, default=None, help="네이버 최저가 하한")
    parser.add_argument("--max_price", type=int, default=None, help="네이버 최저가 상한")
    parser.add_argument("--exclude_malls", default="", help="제외할 mallName 키워드(콤마구분)")

    parser.add_argument("--history_dir", default=".", help="result_*.csv 누적 폴더")
    parser.add_argument("--cache_dir", default=".naver_cache", help="네이버 API 캐시 폴더")
    parser.add_argument("--cache_ttl_hours", type=int, default=12, help="캐시 TTL(시간)")

    parser.add_argument("--limit", type=int, default=100, help="처리할 상위 행 개수(기본 100, 전체는 큰 숫자)")
    parser.add_argument("--official_hashes", default="official_hashes.csv", help="공식 이미지 해시 CSV 경로")

    parser.add_argument(
        "--exclude_codes",
        default="C71YLT371297,C71YLT371193",
        help="제외할 스타일코드(콤마구분). 기본: C71YLT371297,C71YLT371193",
    )

    parser.add_argument("--trim_images", action="store_true", help="이미지 여백 자동 트리밍 후 로컬 file://로 표시")
    parser.add_argument("--image_dir", default=".trimmed_images", help="트리밍 이미지 저장 폴더")
    parser.add_argument("--image_ttl_hours", type=int, default=72, help="트리밍 이미지 캐시 TTL(시간)")

    args = parser.parse_args()

    log("🚀 SCRIPT START")
    log(f"📌 CWD: {os.getcwd()}")

    client_id = os.getenv("NAVER_CLIENT_ID", "").strip()
    client_secret = os.getenv("NAVER_CLIENT_SECRET", "").strip()
    log(f"🔑 NAVER_CLIENT_ID: {'SET' if bool(client_id) else 'MISSING'}")
    log(f"🔑 NAVER_CLIENT_SECRET: {'SET' if bool(client_secret) else 'MISSING'}")
    if not client_id or not client_secret:
        raise SystemExit("환경변수 NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 을 먼저 설정해주세요.")

    if args.trim_images and Image is None:
        log("⚠️ --trim_images 요청됨, but Pillow(PIL) 미설치로 트리밍은 비활성 처리됩니다. (원본 URL 사용)")

    exclude_codes = set()
    if args.exclude_codes:
        exclude_codes = {c.strip().upper() for c in args.exclude_codes.split(",") if c.strip()}
    log(f"🚫 EXCLUDE_CODES: {sorted(list(exclude_codes)) if exclude_codes else 'NONE'}")

    official_img_map = build_official_image_map(args.official_hashes)

    input_path = pick_input_file(args.input)
    log(f"📄 INPUT FILE SELECTED: {input_path}")

    log("📥 CSV LOAD START")
    try:
        df = pd.read_csv(input_path, encoding="utf-8-sig")
    except Exception:
        df = pd.read_csv(input_path)
    log(f"📥 CSV LOAD DONE: rows={len(df):,} cols={len(df.columns):,}")
    if len(df) == 0:
        raise SystemExit("입력 CSV가 비어있습니다.")

    col_code = find_col(df, ["코드", "상품코드", "style_code", "product_code"])
    col_name_en = find_col(df, ["상품명(영문)", "상품명_영문", "상품명", "product_name_en", "name_en"])
    col_name_ko = find_col(df, ["상품명(한글)", "상품명_한글", "상품명(국문)", "product_name_ko", "name_ko"])
    col_price = find_col(df, ["공식몰가", "판매가", "정가", "price", "official_price"])
    log(f"🧭 COLMAP code={col_code} name_en={col_name_en} name_ko={col_name_ko} price={col_price}")

    if col_code:
        df = df.dropna(subset=[col_code])
    else:
        df = df.dropna(subset=[df.columns[1]])

    if args.limit and args.limit > 0 and len(df) > args.limit:
        log(f"🧩 LIMIT ENABLED: top {args.limit} rows only (from {len(df):,})")
        df = df.head(args.limit)
    else:
        log(f"🧩 LIMIT DISABLED or not needed: rows={len(df):,}")

    today_mmdd = datetime.now().strftime("%m%d")
    prev_csv_path = find_previous_result_csv(args.history_dir, today_mmdd=today_mmdd)
    prev_map = load_previous_prices(prev_csv_path) if prev_csv_path else {}
    log(f"🕘 PREV RESULT: {prev_csv_path if prev_csv_path else 'NONE'}")

    exclude_malls = [x.strip() for x in args.exclude_malls.split(",")] if args.exclude_malls else []
    log(f"🧪 FILTER: min_price={args.min_price} max_price={args.max_price} exclude_malls={exclude_malls}")
    log(f"🖼️ TRIM: {'ON' if args.trim_images else 'OFF'} img_dir={args.image_dir}")

    results: List[Dict[str, Any]] = []
    kept = 0
    skipped_no_price = 0
    skipped_no_img = 0
    skipped_excluded = 0
    trimmed_ok = 0
    trimmed_fail = 0

    log(f"🚚 START FETCH: products={len(df):,} delay={args.delay}s cache_ttl={args.cache_ttl_hours}h")

    for i, (_, row) in enumerate(df.iterrows(), start=1):
        style_code = str(get_row_value(row, col_code, fallback_idx=1)).strip()
        if not style_code or style_code.lower() == "nan":
            continue

        code_u = style_code.upper()

        if exclude_codes and code_u in exclude_codes:
            skipped_excluded += 1
            log(f"    🚫 SKIP(EXCLUDED): {code_u}")
            continue

        name_en = get_row_value(row, col_name_en, fallback_idx=2)
        name_en = "" if pd.isna(name_en) else str(name_en).strip()

        name_ko = get_row_value(row, col_name_ko, fallback_idx=None)
        name_ko = "" if (name_ko is None or pd.isna(name_ko)) else str(name_ko).strip()

        official_raw = get_row_value(row, col_price, fallback_idx=4)
        official_price = _to_int_price(official_raw)

        log(f"  [{i}/{len(df)}] {style_code}")

        cached_items = load_cache(args.cache_dir, style_code, ttl_hours=args.cache_ttl_hours)
        if cached_items is not None:
            items = cached_items
            log(f"    ✅ CACHE HIT ({len(items)} items)")
        else:
            log("    ❌ CACHE MISS -> API CALL")
            q = f"Columbia {style_code}"
            items = fetch_naver_shop_with_retry(q, client_id, client_secret, display=10)
            save_cache(args.cache_dir, style_code, items)
            log(f"    📡 API RETURN ({len(items)} items)")

        items = filter_items_for_accuracy(items, style_code, args.min_price, args.max_price, exclude_malls)
        best = pick_lowest_item(items)
        top3_items = pick_top_n_by_price(items, n=3)

        naver_price: Optional[int] = None
        naver_link = ""
        naver_mall = ""
        naver_title = ""

        if best:
            lp = best.get("lprice")
            naver_price = int(lp) if lp and str(lp).isdigit() else None
            naver_link = best.get("link") or ""
            naver_mall = best.get("mallName") or ""
            naver_title = strip_html_tags(best.get("title") or "")

        naver_image = choose_best_image(best, top3_items)
        official_image = official_img_map.get(code_u, "")
        raw_final_image = official_image if official_image else naver_image

        if not isinstance(naver_price, int):
            skipped_no_price += 1
            log("    ⛔ SKIP: naver_price missing")
            time.sleep(max(0.0, args.delay))
            continue

        if not raw_final_image or not str(raw_final_image).strip():
            skipped_no_img += 1
            log("    ⛔ SKIP: final_image missing")
            time.sleep(max(0.0, args.delay))
            continue

        final_image = raw_final_image
        trimmed_local_abs = None
        if args.trim_images and Image is not None:
            file_stem = re.sub(r"[^A-Za-z0-9_\-]", "_", code_u)
            referer = naver_link or None
            trimmed_url, local_abs = trim_image_to_local(
                raw_final_image,
                out_dir=args.image_dir,
                file_stem=file_stem,
                referer=referer,
                ttl_hours=args.image_ttl_hours,
            )
            if local_abs:
                final_image = trimmed_url
                trimmed_local_abs = local_abs
                trimmed_ok += 1
                log("    ✂️ TRIM OK")
            else:
                trimmed_fail += 1
                log("    ✂️ TRIM FAIL (use raw url)")

        diff: Optional[int] = None
        if isinstance(official_price, int) and naver_price > 0:
            diff = official_price - naver_price

        prev_naver = prev_map.get(style_code, {}).get("prev_naver")
        delta_naver: Optional[int] = None
        if isinstance(prev_naver, int):
            delta_naver = naver_price - prev_naver

        conf = compute_confidence(style_code, best)

        top3 = []
        for it in top3_items:
            lp = it.get("lprice")
            lp_int = int(lp) if lp and str(lp).isdigit() else None
            top3.append({
                "lprice": lp_int,
                "mallName": it.get("mallName") or "",
                "link": it.get("link") or "",
            })

        results.append({
            "코드": style_code,
            "상품명(영문)": name_en,
            "상품명(한글)": name_ko,
            "공식몰가": official_price,
            "네이버최저가": naver_price,
            "가격차이": diff,
            "최저가몰": naver_mall,
            "링크": naver_link,

            "원본이미지URL": raw_final_image,
            "이미지URL": final_image,
            "공식이미지URL": official_image,
            "네이버이미지URL": naver_image,
            "raw_image_url": raw_final_image,
            "trimmed_local_path": trimmed_local_abs or "",

            "naver_title": naver_title,
            "confidence": conf,
            "top3": top3,

            "prev_naver": prev_naver,
            "delta_naver": delta_naver,
        })
        kept += 1

        log(
            f"    ✅ KEEP: naver={naver_price:,} diff={diff if diff is not None else '-'} "
            f"match={conf}/5 img=Y trim={'Y' if trimmed_local_abs else 'N'}"
        )

        time.sleep(max(0.0, args.delay))

    log(
        f"📌 SUMMARY: kept={kept:,} "
        f"skip_excluded={skipped_excluded:,} "
        f"skip_no_price={skipped_no_price:,} "
        f"skip_no_img={skipped_no_img:,} "
        f"trim_ok={trimmed_ok:,} trim_fail={trimmed_fail:,}"
    )

    out_csv = args.output_csv or f"result_{today_mmdd}.csv"
    res_df = pd.DataFrame(results)

    if "top3" in res_df.columns:
        res_df["top3"] = res_df["top3"].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, list) else "")

    res_df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    log(f"✅ CSV SAVED: {out_csv} (rows={len(res_df):,})")

    meta = {"generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"), "prev_csv_used": prev_csv_path}
    html = build_html_portal(results, meta)
    with open(args.output_html, "w", encoding="utf-8-sig") as f:
        f.write(html)
    log(f"✅ HTML SAVED: {args.output_html}")

    try:
        import webbrowser
        webbrowser.open(os.path.abspath(args.output_html))
        log("🌐 BROWSER OPENED")
    except Exception:
        log("ℹ️ browser open skipped")


if __name__ == "__main__":
    main()
