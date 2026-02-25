#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
[BUILD VOC DASHBOARD FROM JSON | v6.0 MAX PATCHED + HEALTH MONITORING + SAFE UI]
- Input: reviews.json  {"reviews":[...]}  (created_at ISO 권장)
- Output:
  - site/data/reviews.json  (최근 7일로 필터 + tags 보강 + 정규화)
  - site/data/meta.json     (기간/키워드/근거/클러스터/마인드맵 + HEALTH 진단)
  - site/index.html         (site/template.html 있으면 사용 / 없으면 DEFAULT_HTML_TEMPLATE 사용)

✅ MAX PATCH:
A) 운영 안정성/수집 모니터링
   - 스키마 정규화/검증 + invalid created_at 카운트 + duplicate id + 필드 누락 카운트
   - source 분포, created_at min/max, 최신성(staleness) 체크
   - rows7==0 이어도 "빌드 중단" 대신 경고 + 빈 화면 안내(meta.health) 출력

B) 정확도 개선
   - size 태그: "핏" 단독 트리거 제거(치수 키워드 동반 시만)
   - 4~5점 NEG 과민 완화: 하드결함+조치 + (부정감정/문제) 동반 시 확정, 해결형 긍정 예외
   - keyword seed substring 오작동 방지(동일 토큰 기반)

C) 프론트 안전/UX
   - review 카드 클릭 payload data-* 기반(따옴표 깨짐 방지)
   - daily chip 실제로 동작: OFF면 7일 전체 feed(날짜 필터 무시), ON이면 날짜필터/업로드순

환경변수:
- OUTPUT_TZ: 기본 Asia/Seoul
- PROJECT_ROOT: repo root 강제 지정 가능
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd
from dateutil import tz


# ----------------------------
# Settings
# ----------------------------
OUTPUT_TZ = os.getenv("OUTPUT_TZ", "Asia/Seoul").strip()

# 자동 STOPWORDS 학습 파라미터
AUTO_STOP_MIN_DF = 0.30
AUTO_STOP_MIN_PRODUCTS = 0.25
AUTO_STOP_POLARITY_MARGIN = 0.20
AUTO_STOP_MAX_ADD = 120

# 키워드 TopK
TOPK_POS = 5
TOPK_NEG = 5

# Mindmap(문장형)
MINDMAP_SENT_PER_SIDE = 2
MINDMAP_MAX_PRODUCTS = 24

# Health thresholds
STALE_HOURS_WARN = 36
STALE_HOURS_ERROR = 72
INVALID_CREATED_AT_WARN_RATIO = 0.05
MIN_ROWS7_WARN = 15  # 너무 적으면 "수집 부족" 경고
MIN_SOURCE_WARN = 5  # source가 특정 탭에서 너무 적으면 경고

# 클러스터(원인) 정의
CLUSTERS = {
    "size": ["사이즈", "정사이즈", "작", "크", "타이트", "헐렁", "끼", "기장", "소매", "어깨", "가슴", "발볼", "핏"],
    "quality": ["품질", "불량", "하자", "찢", "구멍", "실밥", "오염", "변색", "냄새", "마감", "내구", "퀄리티"],
    "shipping": ["배송", "택배", "출고", "도착", "지연", "늦", "빠르", "포장", "파손"],
    "cs": ["문의", "응대", "고객", "cs", "교환", "반품", "환불", "처리", "as"],
    "price": ["가격", "비싸", "싸", "가성비", "할인", "쿠폰", "대비"],
    "design": ["디자인", "색", "컬러", "예쁘", "멋", "스타일", "핏감"],
    "function": ["수납", "넣", "포켓", "공간", "가볍", "무게", "따뜻", "보온", "방수", "기능", "편하", "착용감", "그립", "주머니"],
}

CATEGORY_RULES = [
    ("bag", re.compile(r"(backpack|rucksack|bag|pouch|shoulder|packable|duffel|tote|힙색|백팩|가방|파우치|숄더)", re.I)),
    ("shoe", re.compile(r"(shoe|boot|chukka|sneaker|신발|부츠|워커)", re.I)),
    ("top", re.compile(r"(fleece|jacket|hood|tee|turtle|shirt|상의|자켓|플리스|후드|티|터틀)", re.I)),
    ("bottom", re.compile(r"(pant|short|skirt|하의|바지|팬츠|쇼츠)", re.I)),
    ("glove", re.compile(r"(glove|장갑)", re.I)),
]
DEFAULT_CATEGORY = "other"


def now_kst() -> datetime:
    return datetime.now(tz=tz.gettz(OUTPUT_TZ))


def find_repo_root() -> pathlib.Path:
    env_root = os.getenv("PROJECT_ROOT", "").strip()
    if env_root:
        return pathlib.Path(env_root).expanduser().resolve()

    here = pathlib.Path(__file__).resolve()
    for p in [here.parent] + list(here.parents):
        if (p / "site").exists():
            return p
        if (p / ".git").exists():
            return p
    return pathlib.Path.cwd().resolve()


ROOT = find_repo_root()
SITE_DIR = ROOT / "site"
SITE_DATA_DIR = SITE_DIR / "data"
SITE_DIR.mkdir(parents=True, exist_ok=True)
SITE_DATA_DIR.mkdir(parents=True, exist_ok=True)


# ----------------------------
# Text cleaning + tokenizer
# ----------------------------
BASE_STOPWORDS = set(
    """
그리고 그러나 그래서 하지만 또한
너무 정말 완전 진짜 매우 그냥 조금 약간
저는 제가 우리는 너희 이거 그거 저거
있습니다 입니다 같아요 같네요 하는 하다 됐어요 되었어요 되네요
구매 구입 주문 구매해 구입해 샀어요 샀습니다 주문했
제품 상품 물건
사용중 사용 사용함 착용 입어 신어 써봤
배송 택배 포장
문의
좋아요 좋다 좋네요 만족 추천 재구매 가성비 최고 굿
예뻐요 이뻐요
정사이즈 한치수 한 치수
컬러 색상 디자인
있어서 있어서요 있어요 있네요 있었어요
좋습니다 좋았어요
추가 추가로
가능 가능해요
확인 확인해요
생각 생각해요
느낌 느낌이에요
정도 정도로
부분 부분이
사람 분들
기존 같은 동일 비슷 비슷한 원래
이번 이번엔 이번에
보기 많이 잘했어요 잘했네요
""".split()
)

JOSA = ["은", "는", "이", "가", "을", "를", "에", "에서", "에게", "으로", "로", "와", "과", "도", "만", "까지", "부터", "보다", "처럼", "같이", "이나", "나"]
ENDING = ["입니다", "습니다", "했어요", "했네요", "해요", "하네요", "합니다", "같아요", "같네요", "있어요", "있네요", "있습니다"]

RE_URL = re.compile(r"https?://\S+|www\.\S+", re.I)
RE_HASHTAG = re.compile(r"#[A-Za-z0-9가-힣_]+")
RE_EMOJI_ETC = re.compile(r"[^\w\s가-힣]", re.UNICODE)


def normalize_text(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    s = RE_URL.sub(" ", s)
    s = RE_HASHTAG.sub(" ", s)
    s = s.replace("\n", " ")
    s = RE_EMOJI_ETC.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s.lower()


def _strip_josa_ending(tok: str) -> str:
    t = tok
    for j in JOSA:
        if t.endswith(j) and len(t) > len(j) + 1:
            t = t[: -len(j)]
            break
    for e in ENDING:
        if t.endswith(e) and len(t) > len(e) + 1:
            t = t[: -len(e)]
            break
    return t


def detect_category(product_name: str, product_code: str = "") -> str:
    s = f"{product_name} {product_code}"
    for cat, rx in CATEGORY_RULES:
        if rx.search(s):
            return cat
    return DEFAULT_CATEGORY


def tokenize_ko(s: str, stopwords: Optional[set] = None) -> List[str]:
    s = normalize_text(s)
    if not s:
        return []
    sw = stopwords or BASE_STOPWORDS
    toks = re.findall(r"[가-힣A-Za-z0-9]+", s)
    out: List[str] = []
    for t in toks:
        if len(t) < 2:
            continue
        if t.isdigit():
            continue
        t = _strip_josa_ending(t)
        if len(t) < 2:
            continue
        if t in sw:
            continue
        if t in ("있", "좋", "하", "되", "같", "했", "해", "함"):
            continue
        out.append(t)
    return out


# ----------------------------
# Tags / complaint / sentiment seed
# ----------------------------

# ✅ SIZE: "핏" 단독 트리거 금지(치수/작다/크다 동반 시만)
SIZE_STRONG = [
    "사이즈", "정사이즈", "한치수", "치수", "업", "다운",
    "작아", "작다", "커", "크다", "타이트", "헐렁", "끼", "조인다",
    "짧다", "길다", "좁다", "넓다",
    "기장", "소매", "어깨", "가슴", "발볼",
]
SIZE_WEAK = ["핏"]  # 단독 금지

REQ_WEAK = ["개선", "아쉬", "했으면", "보완", "수정", "필요", "요청"]
REQ_STRONG = ["교환", "반품", "환불", "as", "처리"]

COMPLAINT_HINTS = ["불량", "하자", "찢", "구멍", "냄새", "변색", "오염", "실밥", "품질", "엉망", "별로", "최악", "실망", "문제", "불편"]

POS_SEEDS = [
    "가볍", "편하", "편안", "착용감", "수납", "포켓", "공간", "주머니", "넣",
    "따뜻", "보온", "방수", "튼튼", "견고", "만족", "예쁘", "멋", "깔끔", "마감",
    "잘맞", "딱", "쿠션", "좋"
]
NEG_SEEDS = [
    "불량", "하자", "찢", "구멍", "냄새", "변색", "오염", "실밥", "최악", "별로", "실망",
    "불편", "문제", "아쉽", "무겁",
    "작", "크", "타이트", "헐렁",
    "지연", "늦", "파손", "응대", "환불", "교환", "반품"
]

HARD_DEFECT = ["불량", "하자", "찢", "구멍", "파손", "누수", "변색", "오염", "실밥"]
DEFECT_ACTION = ["교환", "반품", "환불", "as", "처리"]
DEFECT_NEG_FEEL = ["불편", "문제", "실망", "별로", "최악", "엉망"]

# 해결형 긍정(예외)
RESOLVED_POS = ["교환했는데 만족", "교환 후 만족", "as 받고 만족", "처리 빨랐", "응대 좋", "해결", "잘 처리", "원활"]

NEGATION = ["안", "않", "없", "못", "아니"]


def has_any_kw(text: str, kws: List[str]) -> bool:
    t = (text or "").replace(" ", "")
    for kw in kws:
        if kw.replace(" ", "") in t:
            return True
    return False


def has_size_signal(text: str) -> bool:
    t = (text or "").replace(" ", "")
    strong = any(k.replace(" ", "") in t for k in SIZE_STRONG)
    if strong:
        return True
    # "핏"은 단독 금지: 치수/업다운/작크/기장/발볼 등 strong랑 같이 있을 때만
    if any(k in t for k in SIZE_WEAK) and any(k.replace(" ", "") in t for k in ["업", "다운", "작", "크", "기장", "소매", "어깨", "가슴", "발볼", "한치수", "사이즈"]):
        return True
    return False


def classify_size_direction(text: str) -> str:
    t = (text or "").replace(" ", "")
    small_kw = ["작아", "작다", "타이트", "끼", "조인다", "짧다", "좁다", "발볼좁", "어깨좁", "가슴좁", "다운"]
    big_kw = ["커", "크다", "넉넉", "오버", "길다", "넓다", "헐렁", "부해", "업"]
    for kw in small_kw:
        if kw in t:
            return "too_small"
    for kw in big_kw:
        if kw in t:
            return "too_big"
    return "other"


def normalize_source(x: Any) -> str:
    s = str(x or "").strip()
    if not s:
        return "Official"
    s_low = s.lower()
    if "naver" in s_low:
        return "Naver"
    if "official" in s_low:
        return "Official"
    # 다른 소스는 combined 탭에서는 보이게 하되, 탭 필터에는 안 걸리게 Other
    return "Other"


def ensure_tags_and_direction(row: Dict[str, Any]) -> Dict[str, Any]:
    text = str(row.get("text") or "")
    rating = int(pd.to_numeric(row.get("rating"), errors="coerce") or 0)

    tags = row.get("tags")
    if not isinstance(tags, list):
        tags = []

    if rating <= 2 and "low" not in tags:
        tags.append("low")

    fit_q = str(row.get("fit_q") or "")
    if ("size" not in tags) and (has_size_signal(text) or fit_q in ("조금 작아요", "작아요", "조금 커요", "커요")):
        tags.append("size")

    if ("req" not in tags) and (has_any_kw(text, REQ_WEAK) or has_any_kw(text, REQ_STRONG)):
        tags.append("req")

    row["tags"] = tags

    sd = str(row.get("size_direction") or "")
    if sd not in ("too_small", "too_big", "other"):
        row["size_direction"] = classify_size_direction(text)

    return row


def review_polarity_scores(text: str) -> tuple[int, int]:
    t = normalize_text(text)
    pos = sum(1 for w in POS_SEEDS if w in t)
    neg = sum(1 for w in NEG_SEEDS if w in t)

    for w in POS_SEEDS:
        if w in t:
            for ng in NEGATION:
                if f"{w}{ng}" in t or f"{w} {ng}" in t:
                    pos = max(0, pos - 1)
                    neg += 1

    for w in NEG_SEEDS:
        if w in t:
            for ng in NEGATION:
                if f"{w}{ng}" in t or f"{w} {ng}" in t:
                    neg = max(0, neg - 1)
                    pos += 1

    return pos, neg


def is_complaint(row: Dict[str, Any]) -> bool:
    """
    - 1~2점: 항상 NEG
    - 4~5점: 원칙 POS
      단, 하드결함 + 조치 + (부정감정/문제) 동반 시 NEG 확정
      + 해결형 긍정 패턴은 예외(NEG 방지)
    - 3점: seed 기반(neg 우세 시 NEG) + complaint hints
    """
    rating = int(pd.to_numeric(row.get("rating"), errors="coerce") or 0)
    tags = row.get("tags") or []
    text = str(row.get("text") or "")

    if rating <= 2:
        return True

    t_norm = normalize_text(text).replace(" ", "")

    if rating >= 4:
        # 해결형 긍정이면 NEG 방지
        for p in RESOLVED_POS:
            if normalize_text(p).replace(" ", "") in t_norm:
                return False

        hard = has_any_kw(text, HARD_DEFECT)
        action = has_any_kw(text, DEFECT_ACTION)
        feel = has_any_kw(text, DEFECT_NEG_FEEL) or has_any_kw(text, COMPLAINT_HINTS)
        if hard and action and feel:
            return True
        return False

    # rating == 3
    pos_s, neg_s = review_polarity_scores(text)
    has_req = isinstance(tags, list) and ("req" in tags)
    hint = has_any_kw(text, COMPLAINT_HINTS)

    if hint and neg_s >= 1:
        return True
    if neg_s >= 2 and neg_s > pos_s:
        return True
    if has_req and neg_s >= 1 and neg_s > pos_s:
        return True

    return False


def is_positive(row: Dict[str, Any]) -> bool:
    rating = int(pd.to_numeric(row.get("rating"), errors="coerce") or 0)
    text = str(row.get("text") or "")

    if is_complaint(row):
        return False

    if rating >= 4:
        return True

    if rating == 3:
        pos_s, neg_s = review_polarity_scores(text)
        return (pos_s >= 2) and (pos_s >= neg_s + 1)

    return False


# ----------------------------
# Auto stopwords learning (guardrails)
# ----------------------------
def learn_auto_stopwords(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {"enabled": False, "reason": "no_rows", "global": [], "by_category": {}}

    pos_rows = [r for r in rows if is_positive(r)]
    neg_rows = [r for r in rows if is_complaint(r)]

    # guardrails
    if len(rows) < 80 or len(pos_rows) < 10 or len(neg_rows) < 10:
        return {
            "enabled": False,
            "reason": f"small_sample(rows={len(rows)},pos={len(pos_rows)},neg={len(neg_rows)})",
            "global": [],
            "by_category": {},
        }

    prod_all = set(str(r.get("product_code") or "-") for r in rows)
    prod_n = max(1, len(prod_all))

    def build_stats(sub_rows: List[Dict[str, Any]]):
        freq: Dict[str, int] = {}
        df: Dict[str, set] = {}
        prod_df: Dict[str, set] = {}
        for r in sub_rows:
            pid = str(r.get("id"))
            pcode = str(r.get("product_code") or "-")
            toks_unique = set(tokenize_ko(str(r.get("text") or ""), stopwords=BASE_STOPWORDS))
            for t in toks_unique:
                df.setdefault(t, set()).add(pid)
                prod_df.setdefault(t, set()).add(pcode)
            for t in tokenize_ko(str(r.get("text") or ""), stopwords=BASE_STOPWORDS):
                freq[t] = freq.get(t, 0) + 1
        df_cnt = {k: len(v) for k, v in df.items()}
        prod_cnt = {k: len(v) for k, v in prod_df.items()}
        return freq, df_cnt, prod_cnt

    pos_freq, pos_df, _ = build_stats(pos_rows)
    neg_freq, neg_df, _ = build_stats(neg_rows)

    all_ids = set(str(r.get("id")) for r in rows)
    all_n = max(1, len(all_ids))

    all_df: Dict[str, int] = {}
    keys = set(pos_df.keys()) | set(neg_df.keys())
    for k in keys:
        all_df[k] = pos_df.get(k, 0) + neg_df.get(k, 0)

    prod_df_map: Dict[str, set] = {}
    for r in rows:
        pcode = str(r.get("product_code") or "-")
        toks_unique = set(tokenize_ko(str(r.get("text") or ""), stopwords=BASE_STOPWORDS))
        for t in toks_unique:
            prod_df_map.setdefault(t, set()).add(pcode)
    prod_cnt_map = {k: len(v) for k, v in prod_df_map.items()}

    auto_global: List[str] = []
    for t, dfc in sorted(all_df.items(), key=lambda x: x[1], reverse=True):
        df_ratio = dfc / all_n
        prod_ratio = prod_cnt_map.get(t, 0) / prod_n
        if df_ratio < AUTO_STOP_MIN_DF:
            continue
        if prod_ratio < AUTO_STOP_MIN_PRODUCTS:
            continue

        p = pos_freq.get(t, 0)
        n = neg_freq.get(t, 0)
        total = p + n
        if total < 8:
            continue
        neg_ratio = n / total if total else 0.0

        if abs(neg_ratio - 0.5) <= AUTO_STOP_POLARITY_MARGIN:
            auto_global.append(t)
        if len(auto_global) >= min(AUTO_STOP_MAX_ADD, 60):
            break

    by_cat: Dict[str, List[str]] = {}
    cat_groups: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        cat = detect_category(str(r.get("product_name") or ""), str(r.get("product_code") or ""))
        cat_groups.setdefault(cat, []).append(r)

    for cat, group in cat_groups.items():
        if len(group) < 25:
            by_cat[cat] = []
            continue

        ids = [str(r.get("id")) for r in group]
        n_docs = max(1, len(ids))
        prod_set = set(str(r.get("product_code") or "-") for r in group)
        n_prod = max(1, len(prod_set))

        token_docs: Dict[str, set] = {}
        token_prods: Dict[str, set] = {}
        token_pos: Dict[str, int] = {}
        token_neg: Dict[str, int] = {}

        for r in group:
            pid = str(r.get("id"))
            pcode = str(r.get("product_code") or "-")
            ispos = is_positive(r)
            isneg = is_complaint(r)
            toks_unique = set(tokenize_ko(str(r.get("text") or ""), stopwords=BASE_STOPWORDS))
            toks_all = tokenize_ko(str(r.get("text") or ""), stopwords=BASE_STOPWORDS)

            for t in toks_unique:
                token_docs.setdefault(t, set()).add(pid)
                token_prods.setdefault(t, set()).add(pcode)
            for t in toks_all:
                if ispos:
                    token_pos[t] = token_pos.get(t, 0) + 1
                if isneg:
                    token_neg[t] = token_neg.get(t, 0) + 1

        cand = []
        for t, ds in token_docs.items():
            df_ratio = len(ds) / n_docs
            prod_ratio = len(token_prods.get(t, set())) / n_prod
            if df_ratio < 0.35:
                continue
            if prod_ratio < 0.30:
                continue
            p = token_pos.get(t, 0)
            n = token_neg.get(t, 0)
            total = p + n
            if total < 8:
                continue
            neg_ratio = n / total if total else 0.0
            if abs(neg_ratio - 0.5) <= 0.22:
                cand.append(t)
        by_cat[cat] = cand[:60]

    return {"enabled": True, "reason": "ok", "global": auto_global, "by_category": by_cat}


def build_stopwords_for_row(row: Dict[str, Any], auto_sw: Dict[str, Any]) -> set:
    sw = set(BASE_STOPWORDS)
    if auto_sw.get("enabled"):
        sw.update(set(auto_sw.get("global") or []))
        cat = detect_category(str(row.get("product_name") or ""), str(row.get("product_code") or ""))
        sw.update(set((auto_sw.get("by_category") or {}).get(cat) or []))
    return sw


# ----------------------------
# Keyword extraction (pos/neg) - safer matching
# ----------------------------
def top_terms(rows: List[Dict[str, Any]], topk: int, auto_sw: Dict[str, Any], which: str) -> List[Tuple[str, int]]:
    freq: Dict[str, int] = {}
    pos_seed_set = set(POS_SEEDS)
    neg_seed_set = set(NEG_SEEDS)

    for r in rows:
        if which == "pos" and not is_positive(r):
            continue
        if which == "neg" and not is_complaint(r):
            continue

        sw = build_stopwords_for_row(r, auto_sw)
        text = str(r.get("text") or "")
        toks = tokenize_ko(text, stopwords=sw)

        for t in toks:
            # seed 오염 방지: substring이 아니라 "동일 토큰" 레벨에서만 제외
            if which == "pos" and t in neg_seed_set:
                continue
            if which == "neg" and t in pos_seed_set:
                continue
            freq[t] = freq.get(t, 0) + 1

    return sorted(freq.items(), key=lambda x: x[1], reverse=True)[:topk]


def assign_cluster(text: str) -> List[str]:
    tnorm = normalize_text(text).replace(" ", "")
    hits = []
    for c, kws in CLUSTERS.items():
        for k in kws:
            if k.replace(" ", "") in tnorm:
                hits.append(c)
                break
    return hits


# ----------------------------
# Evidence
# ----------------------------
@dataclass
class Evidence:
    id: Any
    product_name: str
    product_code: str
    created_at: str
    rating: int
    text_snip: str


def build_keyword_evidence(
    rows: List[Dict[str, Any]],
    keywords: List[str],
    auto_sw: Dict[str, Any],
    filter_fn,
    evidence_per_kw: int = 3,
) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Evidence]] = {k: [] for k in keywords}
    for r in rows:
        if not filter_fn(r):
            continue
        sw = build_stopwords_for_row(r, auto_sw)
        toks = set(tokenize_ko(str(r.get("text") or ""), stopwords=sw))
        for k in keywords:
            if k in toks:
                snip = re.sub(r"\s+", " ", str(r.get("text") or "").strip())
                snip = snip[:140] + ("…" if len(snip) > 140 else "")
                out[k].append(
                    Evidence(
                        id=r.get("id"),
                        product_name=str(r.get("product_name") or r.get("product_code") or "-"),
                        product_code=str(r.get("product_code") or "-"),
                        created_at=str(r.get("created_at") or ""),
                        rating=int(pd.to_numeric(r.get("rating"), errors="coerce") or 0),
                        text_snip=snip,
                    )
                )
    out2: Dict[str, List[Dict[str, Any]]] = {}
    for k, evs in out.items():
        evs.sort(key=lambda x: x.created_at, reverse=True)
        out2[k] = [e.__dict__ for e in evs[:evidence_per_kw]]
    return out2


# ----------------------------
# Mindmap: sentence-based (POS/NEG)
# ----------------------------
SENT_SPLIT = re.compile(r"(?<=[\.\?\!]|[。]|[？！]|[!?\n])\s+|[\n\r]+")


def split_sentences(text: str) -> List[str]:
    t = re.sub(r"\s+", " ", (text or "").strip())
    if not t:
        return []
    parts = [p.strip() for p in SENT_SPLIT.split(t) if p.strip()]
    out = []
    for p in parts:
        if len(p) < 15:
            continue
        if len(p) > 160:
            p = p[:160].rstrip() + "…"
        out.append(p)
    return out


TRIVIAL_PHRASES = [
    "잘했어요", "보기", "많이", "사용중", "구입해서", "구매해서", "샀는데", "주문했는데", "받았는데",
    "좋아요", "만족", "추천", "재구매"
]


def score_sentence(sent: str, mode: str) -> int:
    s = normalize_text(sent)
    score = 0

    if mode == "pos":
        score += sum(2 for w in POS_SEEDS if w in s)
        score -= sum(2 for w in NEG_SEEDS if w in s)
    else:
        score += sum(2 for w in NEG_SEEDS if w in s)
        score -= sum(1 for w in POS_SEEDS if w in s)

    if mode == "neg":
        score += sum(1 for c_kws in CLUSTERS.values() for w in c_kws if w in s)

    score -= sum(3 for p in TRIVIAL_PHRASES if p in sent)

    if re.search(r"\d", sent):
        score += 1
    if any(x in sent for x in ["주머니", "수납", "발볼", "어깨", "기장", "소매", "보온", "방수", "착용감", "업", "다운"]):
        score += 1

    if len(sent) >= 40:
        score += 1
    if len(sent) >= 70:
        score += 1

    return score


def build_product_mindmap_1y_sentence(
    rows_1y: List[Dict[str, Any]],
    rows_7d: List[Dict[str, Any]],
    per_side: int = 2,
    max_products: int = 24,
) -> List[Dict[str, Any]]:
    by_prod_1y: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows_1y:
        code = str(r.get("product_code") or "-")
        by_prod_1y.setdefault(code, []).append(r)

    by_prod_7d: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows_7d:
        code = str(r.get("product_code") or "-")
        by_prod_7d.setdefault(code, []).append(r)

    out: List[Dict[str, Any]] = []

    for code, prod_rows_1y in by_prod_1y.items():
        sample = prod_rows_1y[0]
        pname = str(sample.get("product_name") or code)
        img = str(sample.get("local_product_image") or "")
        review_cnt_1y = len(prod_rows_1y)

        prod_rows_7d = by_prod_7d.get(code, [])

        pos_cands: List[Tuple[int, Dict[str, Any]]] = []
        for r in (prod_rows_7d + prod_rows_1y):
            if not is_positive(r):
                continue
            sents = split_sentences(str(r.get("text") or ""))
            for s in sents:
                sc = score_sentence(s, "pos")
                if sc < 2:
                    continue
                pos_cands.append((sc, {
                    "text": s,
                    "id": r.get("id"),
                    "created_at": r.get("created_at"),
                    "rating": int(pd.to_numeric(r.get("rating"), errors="coerce") or 0),
                }))

        neg_cands: List[Tuple[int, Dict[str, Any]]] = []
        for r in (prod_rows_7d + prod_rows_1y):
            if not is_complaint(r):
                continue
            sents = split_sentences(str(r.get("text") or ""))
            for s in sents:
                sc = score_sentence(s, "neg")
                if sc < 2:
                    continue
                neg_cands.append((sc, {
                    "text": s,
                    "id": r.get("id"),
                    "created_at": r.get("created_at"),
                    "rating": int(pd.to_numeric(r.get("rating"), errors="coerce") or 0),
                }))

        pos_cands.sort(key=lambda x: (x[0], str(x[1].get("created_at") or "")), reverse=True)
        neg_cands.sort(key=lambda x: (x[0], str(x[1].get("created_at") or "")), reverse=True)

        def pick_unique(cands: List[Tuple[int, Dict[str, Any]]], k: int) -> List[Dict[str, Any]]:
            seen = set()
            picked = []
            for _, item in cands:
                t = item.get("text") or ""
                key = re.sub(r"\s+", " ", t).strip()
                if key in seen:
                    continue
                seen.add(key)
                picked.append(item)
                if len(picked) >= k:
                    break
            return picked

        pos_sent = pick_unique(pos_cands, per_side)
        neg_sent = pick_unique(neg_cands, per_side)

        out.append({
            "product_code": code,
            "product_name": pname,
            "local_product_image": img,
            "reviews_1y": review_cnt_1y,
            "pos_sentences": pos_sent,
            "neg_sentences": neg_sent,
        })

    out.sort(key=lambda x: x.get("reviews_1y", 0), reverse=True)
    return out[:max_products]


# ----------------------------
# IO + schema normalization
# ----------------------------
REQUIRED_KEYS = ["id", "created_at", "text", "product_code", "product_name", "rating"]


def read_reviews_json(path: pathlib.Path) -> List[Dict[str, Any]]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    reviews = obj.get("reviews")
    if not isinstance(reviews, list):
        raise ValueError('입력 JSON은 {"reviews": [...]} 형태여야 합니다.')

    out = []
    for r in reviews:
        if not isinstance(r, dict):
            continue

        # normalize/ensure fields
        r["source"] = normalize_source(r.get("source"))
        r["product_code"] = str(r.get("product_code") or "").strip() or "-"
        r["product_name"] = str(r.get("product_name") or "").strip() or r["product_code"]
        r["text"] = str(r.get("text") or "").strip()
        r["product_url"] = str(r.get("product_url") or "").strip()
        r["option_size"] = str(r.get("option_size") or "").strip()
        r["option_color"] = str(r.get("option_color") or "").strip()
        r["local_product_image"] = str(r.get("local_product_image") or "").strip()
        r["local_review_thumb"] = str(r.get("local_review_thumb") or "").strip()
        r["text_image_path"] = str(r.get("text_image_path") or "").strip()

        # rating normalize
        r["rating"] = int(pd.to_numeric(r.get("rating"), errors="coerce") or 0)

        out.append(ensure_tags_and_direction(r))
    return out


def parse_created_at_iso(s: str) -> Optional[datetime]:
    s = str(s or "").strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
        # tz 없으면 KST 가정
        if dt.tzinfo is None:
            return dt.replace(tzinfo=tz.gettz(OUTPUT_TZ))
        return dt
    except Exception:
        dt = pd.to_datetime(s, errors="coerce")
        if pd.isna(dt):
            return None
        dtp = dt.to_pydatetime()
        if dtp.tzinfo is None:
            return dtp.replace(tzinfo=tz.gettz(OUTPUT_TZ))
        return dtp


def in_date_range_kst(dt: datetime, start_d, end_d) -> bool:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz.gettz(OUTPUT_TZ))
    d = dt.astimezone(tz.gettz(OUTPUT_TZ)).date()
    return start_d <= d <= end_d


# ----------------------------
# HTML template
# ----------------------------
DEFAULT_HTML_TEMPLATE = ""  # v6에서는 template.html 사용을 권장(아래 load_html_template가 fallback)


def load_html_template() -> str:
    tpl = SITE_DIR / "template.html"
    if tpl.exists():
        return tpl.read_text(encoding="utf-8")
    if DEFAULT_HTML_TEMPLATE.strip():
        return DEFAULT_HTML_TEMPLATE
    raise FileNotFoundError("site/template.html 이 없습니다. template.html 을 site/ 아래에 두세요.")


# ----------------------------
# Health monitoring
# ----------------------------
def build_health_report(all_rows: List[Dict[str, Any]], parsed_map: Dict[str, Optional[datetime]], now: datetime) -> Dict[str, Any]:
    warnings: List[str] = []
    errors: List[str] = []

    total = len(all_rows)

    # duplicates
    ids = [str(r.get("id")) for r in all_rows]
    dup = total - len(set(ids))

    # missing fields count
    missing = {k: 0 for k in REQUIRED_KEYS}
    for r in all_rows:
        for k in REQUIRED_KEYS:
            v = r.get(k)
            if v is None or (isinstance(v, str) and not v.strip()):
                missing[k] += 1

    # invalid created_at
    invalid_dt = sum(1 for _, v in parsed_map.items() if v is None)
    invalid_ratio = (invalid_dt / max(1, total))

    # created_at min/max
    valid_dts = [v for v in parsed_map.values() if v is not None]
    dt_min = min(valid_dts).astimezone(tz.gettz(OUTPUT_TZ)).isoformat() if valid_dts else None
    dt_max = max(valid_dts).astimezone(tz.gettz(OUTPUT_TZ)).isoformat() if valid_dts else None

    # staleness
    staleness_hours = None
    if valid_dts:
        staleness_hours = (now - max(valid_dts).astimezone(tz.gettz(OUTPUT_TZ))).total_seconds() / 3600.0

    # by source
    by_source: Dict[str, int] = {}
    for r in all_rows:
        by_source[r.get("source", "Other")] = by_source.get(r.get("source", "Other"), 0) + 1

    # warnings/errors rules
    if total == 0:
        errors.append("입력 reviews.json 내 reviews가 0건입니다(수집 실패).")

    if dup > 0:
        warnings.append(f"id 중복 {dup}건(병합/수집 중복 가능).")

    if invalid_ratio >= INVALID_CREATED_AT_WARN_RATIO:
        warnings.append(f"created_at 파싱 실패 비율 {invalid_ratio:.1%} (포맷/타임존 점검 필요).")

    if staleness_hours is not None:
        if staleness_hours >= STALE_HOURS_ERROR:
            errors.append(f"최신 리뷰가 {staleness_hours:.1f}시간 이상 갱신되지 않았습니다(수집 중단 의심).")
        elif staleness_hours >= STALE_HOURS_WARN:
            warnings.append(f"최신 리뷰 갱신이 {staleness_hours:.1f}시간 지연 중입니다.")

    # source-specific hints
    if by_source.get("Naver", 0) < MIN_SOURCE_WARN:
        warnings.append("Naver 리뷰 수가 매우 적습니다(네이버 수집/파싱/병합 확인).")
    if by_source.get("Official", 0) < MIN_SOURCE_WARN:
        warnings.append("Official 리뷰 수가 매우 적습니다(공식몰 수집/파싱/병합 확인).")

    status = "ok"
    if errors:
        status = "error"
    elif warnings:
        status = "warn"

    return {
        "status": status,
        "warnings": warnings,
        "errors": errors,
        "counts": {
            "input_total_reviews": total,
            "duplicate_id_count": dup,
            "invalid_created_at_count": invalid_dt,
            "invalid_created_at_ratio": round(invalid_ratio, 4),
            "missing_field_counts": missing,
            "by_source": by_source,
        },
        "created_at": {
            "min": dt_min,
            "max": dt_max,
            "staleness_hours": None if staleness_hours is None else round(staleness_hours, 2),
        },
    }


# ----------------------------
# Main
# ----------------------------
def main(input_path: str):
    inp = pathlib.Path(input_path).expanduser().resolve()
    if not inp.exists():
        raise FileNotFoundError(f"input not found: {inp}")

    all_rows = read_reviews_json(inp)
    now = now_kst()

    # parse created_at
    parsed_dt_by_id: Dict[str, Optional[datetime]] = {}
    for r in all_rows:
        rid = str(r.get("id"))
        parsed_dt_by_id[rid] = parse_created_at_iso(str(r.get("created_at") or ""))

    health = build_health_report(all_rows, parsed_dt_by_id, now)

    # 기간
    start7 = (now - timedelta(days=6)).date()
    end7 = now.date()
    start1y = (now - timedelta(days=365)).date()
    end1y = now.date()

    rows7: List[Dict[str, Any]] = []
    rows1y: List[Dict[str, Any]] = []
    invalid_rows: List[Dict[str, Any]] = []

    for r in all_rows:
        dt = parsed_dt_by_id.get(str(r.get("id")))
        if dt is None:
            invalid_rows.append(r)
            continue
        if in_date_range_kst(dt, start7, end7):
            rows7.append(r)
        if in_date_range_kst(dt, start1y, end1y):
            rows1y.append(r)

    # rows7 부족 경고
    if len(rows7) < MIN_ROWS7_WARN:
        health["warnings"].append(f"최근 7일 리뷰가 {len(rows7)}건으로 매우 적습니다(수집량 부족).")
        if health["status"] == "ok":
            health["status"] = "warn"

    df7 = pd.DataFrame(rows7).copy() if rows7 else pd.DataFrame(columns=["id", "rating", "created_at", "text", "source"])
    if not df7.empty:
        df7["rating"] = pd.to_numeric(df7.get("rating"), errors="coerce").fillna(0).astype(int)

    # AUTO STOPWORDS 학습(최근7일 기준, guardrail 포함)
    auto_sw = learn_auto_stopwords(rows7)

    # 키워드
    pos_top = top_terms(rows7, TOPK_POS, auto_sw, which="pos") if rows7 else []
    neg_top = top_terms(rows7, TOPK_NEG, auto_sw, which="neg") if rows7 else []

    pos_keys = [k for k, _ in pos_top]
    neg_keys = [k for k, _ in neg_top]

    pos_evidence = build_keyword_evidence(rows7, pos_keys, auto_sw, filter_fn=is_positive, evidence_per_kw=3) if rows7 else {}
    neg_evidence = build_keyword_evidence(rows7, neg_keys, auto_sw, filter_fn=is_complaint, evidence_per_kw=3) if rows7 else {}

    def attach_rid(top_list: List[Tuple[str, int]], evi_map: Dict[str, List[Dict[str, Any]]]) -> List[List[Any]]:
        out = []
        for k, c in top_list:
            rid = None
            evs = evi_map.get(k) or []
            if evs:
                rid = evs[0].get("id")
            out.append([k, int(c), rid])
        return out

    pos_top5 = attach_rid(pos_top, pos_evidence) if pos_top else []
    neg_top5 = attach_rid(neg_top, neg_evidence) if neg_top else []

    # 클러스터(불만 기준)
    cluster_counts: Dict[str, int] = {k: 0 for k in CLUSTERS.keys()}
    for r in rows7:
        if not is_complaint(r):
            continue
        for h in assign_cluster(str(r.get("text") or "")):
            cluster_counts[h] = cluster_counts.get(h, 0) + 1

    # size phrases(최근7일 size 리뷰에서만)
    size_rows = [r for r in rows7 if isinstance(r.get("tags"), list) and ("size" in r.get("tags"))]
    size_phrases_terms = top_terms(size_rows, topk=10, auto_sw=auto_sw, which="neg") if size_rows else []
    size_phrases = [k for k, _ in size_phrases_terms]

    # mindmap
    product_mindmap_1y = build_product_mindmap_1y_sentence(
        rows_1y=rows1y,
        rows_7d=rows7,
        per_side=MINDMAP_SENT_PER_SIDE,
        max_products=MINDMAP_MAX_PRODUCTS,
    ) if rows1y else []

    period_text = f"최근 7일 ({start7.isoformat()} ~ {end7.isoformat()})"

    meta = {
        "updated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "period_text": period_text,
        "period_start": start7.isoformat(),
        "period_end": end7.isoformat(),

        "total_reviews_7d": int(len(rows7)),
        "total_reviews_input": int(len(all_rows)),

        # Summary keys (HTML)
        "pos_top5": pos_top5,
        "neg_top5": neg_top5,

        "keyword_evidence": {"pos": pos_evidence, "neg": neg_evidence},
        "auto_stopwords": auto_sw,
        "clusters": cluster_counts,
        "size_phrases": size_phrases,
        "fit_words": ["정사이즈", "한치수 크게", "한치수 작게", "타이트", "넉넉", "기장", "소매", "어깨", "가슴", "발볼", "업", "다운"],

        "product_mindmap_1y": product_mindmap_1y,

        # ✅ HEALTH
        "health": health,
    }

    # 최근7일 reviews.json (프론트 필터용 pos 태그 보강 + source 정규화)
    out_reviews: List[Dict[str, Any]] = []
    for r in (df7.to_dict(orient="records") if not df7.empty else []):
        tags = r.get("tags", [])
        if not isinstance(tags, list):
            tags = []
        if is_positive(r) and "pos" not in tags:
            tags.append("pos")

        out_reviews.append({
            "id": r.get("id"),
            "product_code": r.get("product_code", ""),
            "product_name": r.get("product_name", ""),
            "product_url": r.get("product_url", ""),
            "rating": int(pd.to_numeric(r.get("rating"), errors="coerce") or 0),
            "created_at": r.get("created_at", ""),
            "text": r.get("text", ""),
            "source": normalize_source(r.get("source")),
            "option_size": r.get("option_size", ""),
            "option_color": r.get("option_color", ""),
            "tags": tags,
            "size_direction": r.get("size_direction", "other"),
            "local_product_image": r.get("local_product_image", ""),
            "local_review_thumb": r.get("local_review_thumb", ""),
            "text_image_path": r.get("text_image_path", ""),
        })

    # HTML
    html = load_html_template()

    (SITE_DATA_DIR / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    (SITE_DATA_DIR / "reviews.json").write_text(json.dumps({"reviews": out_reviews}, ensure_ascii=False, indent=2), encoding="utf-8")
    (SITE_DIR / "index.html").write_text(html, encoding="utf-8")

    print("[OK] Build done (v6 max patched)")
    print(f"- Input: {inp}")
    print(f"- Output meta: {SITE_DATA_DIR / 'meta.json'}")
    print(f"- Output reviews: {SITE_DATA_DIR / 'reviews.json'}")
    print(f"- Output html: {SITE_DIR / 'index.html'}")
    print(f"- Period: {period_text}")
    print(f"- rows7: {len(rows7)} / input: {len(all_rows)}")
    print(f"- health: {health.get('status')} warnings={len(health.get('warnings',[]))} errors={len(health.get('errors',[]))}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="path to reviews.json (aggregated JSON: {reviews:[...]})")
    args = ap.parse_args()
    main(args.input)
