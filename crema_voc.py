#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
[BUILD VOC DASHBOARD FROM JSON | v5.0 FINAL]
- 입력: reviews.json ({"reviews":[...]}), created_at ISO 권장
- 출력:
  - site/data/reviews.json  (최근 7일로 필터된 리뷰만)
  - site/data/meta.json     (기간/키워드/근거/클러스터/제품별 1y "문장형" 마인드맵 등)
  - site/index.html         (니가 준 HTML 기반 + 아래 개선 반영)

v5 핵심
1) ✅ Daily Feed 카드 클릭 → 자동 필터 세팅(날짜/제품/옵션/소스탭) → 해당 리뷰로 스크롤(넘어가는 느낌)
2) ✅ Product Mindmap: "키워드" 대신 "대표 문장" (POS/NEG 각 2개, 클릭하면 해당 리뷰로 이동)
3) ✅ meta key 계약 통일:
   - Summary에서 쓰는 pos_top5 / neg_top5 / keyword_evidence 제공
   - Mindmap에서 쓰는 product_mindmap_1y 제공
4) ✅ POS/NEG 오분류 완화: "수납/가볍/편하/따뜻" 같은 기능 강점은 POS 쪽으로 유도
5) ✅ 템플릿 사용:
   - site/template.html 있으면 그걸 그대로 사용
   - 없으면, 이 스크립트에 내장된 DEFAULT_HTML_TEMPLATE(=니가 준 HTML + v5용 패치) 사용

필수: python-dateutil, pandas
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
MINDMAP_SENT_PER_SIDE = 2  # 제품별 POS/NEG 문장 개수
MINDMAP_MAX_PRODUCTS = 24  # 너무 무거워지는거 방지

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

# 제품군(카테고리) 간이 룰
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
SIZE_KEYWORDS = ["사이즈", "정사이즈", "작아요", "작다", "커요", "크다", "핏", "타이트", "여유", "끼", "기장", "소매", "어깨", "가슴", "발볼", "헐렁", "오버"]
REQ_KEYWORDS = ["개선", "아쉬", "불편", "했으면", "보완", "수정", "필요", "요청", "교환", "반품", "환불"]
COMPLAINT_HINTS = ["불량", "하자", "찢", "구멍", "냄새", "변색", "오염", "실밥", "품질", "엉망", "별로", "최악", "실망", "문제", "불편"]

# POS/NEG seed (오분류 완화용)
POS_SEEDS = [
    "가볍", "무게", "편하", "편안", "착용감", "수납", "포켓", "공간", "주머니", "넣",
    "따뜻", "보온", "방수", "튼튼", "견고", "만족", "예쁘", "멋", "깔끔", "마감",
    "잘맞", "딱", "쿠션"
]
NEG_SEEDS = [
    "불량", "하자", "찢", "구멍", "냄새", "변색", "오염", "실밥", "최악", "별로", "실망",
    "불편", "문제", "아쉽", "작", "크", "타이트", "헐렁",
    "지연", "늦", "파손", "응대", "환불", "교환", "반품"
]


def has_any_kw(text: str, kws: List[str]) -> bool:
    t = (text or "").replace(" ", "")
    for kw in kws:
        if kw.replace(" ", "") in t:
            return True
    return False


def classify_size_direction(text: str) -> str:
    t = (text or "").replace(" ", "")
    small_kw = ["작아", "작다", "타이트", "끼", "조인다", "짧다", "좁다", "발볼좁", "어깨좁", "가슴좁"]
    big_kw = ["커", "크다", "넉넉", "오버", "길다", "넓다", "헐렁", "부해"]
    for kw in small_kw:
        if kw in t:
            return "too_small"
    for kw in big_kw:
        if kw in t:
            return "too_big"
    return "other"


def ensure_tags_and_direction(row: Dict[str, Any]) -> Dict[str, Any]:
    text = str(row.get("text") or "")
    rating = int(row.get("rating") or 0)

    tags = row.get("tags")
    if not isinstance(tags, list):
        tags = []

    if rating <= 2 and "low" not in tags:
        tags.append("low")

    fit_q = str(row.get("fit_q") or "")
    if ("size" not in tags) and (has_any_kw(text, SIZE_KEYWORDS) or fit_q in ("조금 작아요", "작아요", "조금 커요", "커요")):
        tags.append("size")

    if ("req" not in tags) and has_any_kw(text, REQ_KEYWORDS):
        tags.append("req")

    row["tags"] = tags

    sd = str(row.get("size_direction") or "")
    if sd not in ("too_small", "too_big", "other"):
        row["size_direction"] = classify_size_direction(text)

    return row


def is_complaint(row: Dict[str, Any]) -> bool:
    rating = int(row.get("rating") or 0)
    tags = row.get("tags") or []
    text = str(row.get("text") or "")
    if rating <= 2:
        return True
    if isinstance(tags, list) and ("low" in tags or "req" in tags):
        return True
    if rating <= 3 and has_any_kw(text, COMPLAINT_HINTS):
        return True
    return False


def is_positive(row: Dict[str, Any]) -> bool:
    rating = int(row.get("rating") or 0)
    text = str(row.get("text") or "")
    if is_complaint(row):
        return False
    if rating >= 4:
        return True
    if rating == 3:
        t = normalize_text(text)
        p = sum(1 for w in POS_SEEDS if w in t)
        n = sum(1 for w in NEG_SEEDS if w in t)
        return p >= 2 and n == 0
    return False


# ----------------------------
# Auto stopwords learning
# ----------------------------
def learn_auto_stopwords(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {"global": [], "by_category": {}}

    pos_rows = [r for r in rows if is_positive(r)]
    neg_rows = [r for r in rows if is_complaint(r)]

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

    pos_freq, pos_df, pos_prod = build_stats(pos_rows)
    neg_freq, neg_df, neg_prod = build_stats(neg_rows)

    all_ids = set(str(r.get("id")) for r in rows)
    all_n = max(1, len(all_ids))

    # 전체 DF(근사) + 제품DF(정확)
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
        if total < 6:
            continue
        neg_ratio = n / total if total else 0.0

        if abs(neg_ratio - 0.5) <= AUTO_STOP_POLARITY_MARGIN:
            auto_global.append(t)
        if len(auto_global) >= AUTO_STOP_MAX_ADD:
            break

    by_cat: Dict[str, List[str]] = {}
    cat_groups: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        cat = detect_category(str(r.get("product_name") or ""), str(r.get("product_code") or ""))
        cat_groups.setdefault(cat, []).append(r)

    for cat, group in cat_groups.items():
        if len(group) < 12:
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
            if total < 6:
                continue
            neg_ratio = n / total if total else 0.0
            if abs(neg_ratio - 0.5) <= 0.22:
                cand.append(t)
        by_cat[cat] = cand[:80]

    return {"global": auto_global, "by_category": by_cat}


def build_stopwords_for_row(row: Dict[str, Any], auto_sw: Dict[str, Any]) -> set:
    sw = set(BASE_STOPWORDS)
    sw.update(set(auto_sw.get("global") or []))
    cat = detect_category(str(row.get("product_name") or ""), str(row.get("product_code") or ""))
    sw.update(set((auto_sw.get("by_category") or {}).get(cat) or []))
    return sw


# ----------------------------
# Keyword extraction (pos/neg)
# ----------------------------
def top_terms(rows: List[Dict[str, Any]], topk: int, auto_sw: Dict[str, Any], which: str) -> List[Tuple[str, int]]:
    freq: Dict[str, int] = {}
    for r in rows:
        if which == "pos" and not is_positive(r):
            continue
        if which == "neg" and not is_complaint(r):
            continue

        sw = build_stopwords_for_row(r, auto_sw)
        text = str(r.get("text") or "")
        toks = tokenize_ko(text, stopwords=sw)

        for t in toks:
            # POS에서 NEG seed가 강하면 제외
            if which == "pos" and any(seed in t for seed in NEG_SEEDS):
                continue
            # NEG에서 POS seed가 강하면 제외 (수납/가볍/편하 같은 강점이 NEG로 빨리는거 방지)
            if which == "neg" and any(seed in t for seed in POS_SEEDS):
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
                        rating=int(r.get("rating") or 0),
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
    # 너무 짧거나 너무 긴 문장 컷
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
    """
    mode: "pos" or "neg"
    - POS: 기능/착용/수납/편안함/보온 등 seed 가점
    - NEG: 불편/하자/교환/반품/지연 seed 가점
    - 공통: 너무 상투적/짧은 표현 감점
    """
    s = normalize_text(sent)

    score = 0

    if mode == "pos":
        score += sum(2 for w in POS_SEEDS if w in s)
        score -= sum(2 for w in NEG_SEEDS if w in s)
    else:
        score += sum(2 for w in NEG_SEEDS if w in s)
        score -= sum(1 for w in POS_SEEDS if w in s)

    # 클러스터/원인 명시 가점(NEG 쪽)
    if mode == "neg":
        score += sum(1 for c_kws in CLUSTERS.values() for w in c_kws if w in s)

    # 너무 흔한 문구 감점
    score -= sum(3 for p in TRIVIAL_PHRASES if p in sent)

    # 구체성 가점(숫자/부위/기능 언급)
    if re.search(r"\d", sent):
        score += 1
    if any(x in sent for x in ["주머니", "수납", "발볼", "어깨", "기장", "소매", "보온", "방수", "착용감"]):
        score += 1

    # 길이 보정
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
    """
    제품별(최근 1년) 대표 문장 POS/NEG.
    - 클릭 이동(스크롤)은 "최근7일"의 리뷰 id가 있어야 가장 좋음
    - 1y에서 좋은 문장을 뽑되, 가능하면 7d 내 리뷰 문장으로 우선 채움
    """
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

        # 후보: 7d 우선 + 부족하면 1y로 보충
        prod_rows_7d = by_prod_7d.get(code, [])

        # POS 문장 후보
        pos_cands: List[Tuple[int, Dict[str, Any]]] = []
        for r in (prod_rows_7d + prod_rows_1y):
            if not is_positive(r):
                continue
            sents = split_sentences(str(r.get("text") or ""))
            for s in sents:
                sc = score_sentence(s, "pos")
                # 너무 낮은 점수 컷
                if sc < 2:
                    continue
                pos_cands.append((sc, {
                    "text": s,
                    "id": r.get("id"),
                    "created_at": r.get("created_at"),
                    "rating": int(r.get("rating") or 0),
                }))

        # NEG 문장 후보
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
                    "rating": int(r.get("rating") or 0),
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
# IO helpers
# ----------------------------
def read_reviews_json(path: pathlib.Path) -> List[Dict[str, Any]]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    reviews = obj.get("reviews")
    if not isinstance(reviews, list):
        raise ValueError('입력 JSON은 {"reviews": [...]} 형태여야 합니다.')
    out = []
    for r in reviews:
        if isinstance(r, dict):
            out.append(ensure_tags_and_direction(r))
    return out


def parse_created_at_iso(s: str) -> datetime:
    try:
        return datetime.fromisoformat(s)
    except Exception:
        dt = pd.to_datetime(s, errors="coerce")
        if pd.isna(dt):
            return datetime(1970, 1, 1, tzinfo=tz.gettz(OUTPUT_TZ))
        if dt.tzinfo is None:
            return dt.to_pydatetime().replace(tzinfo=tz.gettz(OUTPUT_TZ))
        return dt.to_pydatetime()


def in_date_range_kst(dt: datetime, start_d, end_d) -> bool:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz.gettz(OUTPUT_TZ))
    d = dt.astimezone(tz.gettz(OUTPUT_TZ)).date()
    return start_d <= d <= end_d


# ----------------------------
# HTML template (니가 준 HTML + v5 필수 패치 포함)
# - site/template.html 있으면 그걸 우선 사용
# ----------------------------
DEFAULT_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>VOC Dashboard | Official + Naver Reviews</title>

  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">

  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200;400;600;800&display=swap');
    :root { --brand:#002d72; --bg0:#f6f8fb; --bg1:#eef3f9; }
    body{
      background: linear-gradient(180deg, var(--bg0), var(--bg1));
      font-family: 'Plus Jakarta Sans', sans-serif;
      color:#0f172a; min-height:100vh;
    }
    .glass-card{ background: rgba(255,255,255,0.55); backdrop-filter: blur(20px);
      border: 1px solid rgba(255,255,255,0.75); border-radius: 30px;
      box-shadow: 0 20px 50px rgba(0,45,114,0.05); }
    .sidebar{ background: rgba(255,255,255,0.70); backdrop-filter: blur(15px);
      border-right: 1px solid rgba(255,255,255,0.80); }
    .summary-card{ border-radius: 26px; background: rgba(255,255,255,0.55);
      border: 1px solid rgba(255,255,255,0.75); backdrop-filter: blur(18px);
      box-shadow: 0 20px 50px rgba(0,45,114,0.05); padding: 18px 20px; }
    .small-label{ font-size: 10px; letter-spacing: 0.3em; text-transform: uppercase; font-weight: 900; }
    .input-glass{ background: rgba(255,255,255,0.65); border: 1px solid rgba(255,255,255,0.80);
      border-radius: 18px; padding: 12px 14px; outline: none; font-weight: 800; color:#0f172a; }
    .input-glass:focus{ box-shadow: 0 0 0 4px rgba(0,45,114,0.10); border-color: rgba(0,45,114,0.25); }
    .chip{ border-radius: 9999px; padding: 10px 14px; font-weight: 900; font-size: 12px;
      border: 1px solid rgba(255,255,255,0.85); background: rgba(255,255,255,0.60);
      color:#334155; cursor:pointer; user-select:none; }
    .chip.active{ background: rgba(0,45,114,0.95); color:#fff; border-color: rgba(0,45,114,1);
      box-shadow: 0 10px 30px rgba(0,45,114,0.15); }
    .tab-btn{ padding: 10px 14px; border-radius: 18px; font-weight: 900; font-size: 12px;
      border: 1px solid rgba(255,255,255,0.85); background: rgba(255,255,255,0.60);
      color:#475569; transition: all .15s ease; }
    .tab-btn:hover{ background: rgba(255,255,255,0.90); }
    .tab-btn.active{ background: rgba(0,45,114,0.95); color:#fff; border-color: rgba(0,45,114,1);
      box-shadow: 0 10px 30px rgba(0,45,114,0.15); }
    .overlay{ position: fixed; inset:0; background: rgba(255,255,255,0.65); backdrop-filter: blur(10px);
      display:none; align-items:center; justify-content:center; z-index:9999; }
    .overlay.show{ display:flex; }
    .spinner{ width:56px;height:56px;border-radius:9999px; border:6px solid rgba(0,0,0,0.08);
      border-top-color: rgba(0,45,114,0.95); animation: spin .9s linear infinite; }
    @keyframes spin { to { transform: rotate(360deg);} }
    .tbl{ width:100%; border-collapse: separate; border-spacing: 0; overflow:hidden; border-radius: 22px;
      border: 1px solid rgba(255,255,255,0.85); background: rgba(255,255,255,0.55); }
    .tbl th{ font-size: 11px; letter-spacing: .22em; text-transform: uppercase; font-weight: 900;
      color:#475569; background: rgba(255,255,255,0.75); padding: 14px 14px; position: sticky; top: 0; z-index: 1; }
    .tbl td{ padding: 14px 14px; border-top: 1px solid rgba(255,255,255,0.75); font-weight: 800;
      color:#0f172a; font-size: 13px; vertical-align: top; }
    .tbl .muted{ color:#64748b; font-weight:800; font-size:12px; }
    .review-card{ border-radius: 26px; background: rgba(255,255,255,0.55);
      border: 1px solid rgba(255,255,255,0.80); backdrop-filter: blur(18px);
      box-shadow: 0 16px 42px rgba(0,45,114,0.04); padding: 18px 18px; }
    .badge{ display:inline-flex; align-items:center; gap:6px; padding: 6px 10px; border-radius: 9999px;
      font-size: 11px; font-weight: 900; border: 1px solid rgba(255,255,255,0.85);
      background: rgba(255,255,255,0.65); color:#334155; }
    .badge.neg{ background: rgba(239,68,68,0.10); color:#b91c1c; border-color: rgba(239,68,68,0.18); }
    .badge.pos{ background: rgba(16,185,129,0.10); color:#047857; border-color: rgba(16,185,129,0.18); }
    .badge.size{ background: rgba(59,130,246,0.10); color:#1d4ed8; border-color: rgba(59,130,246,0.18); }
    .img-box{ width:72px; height:72px; border-radius:18px; overflow:hidden; background: rgba(255,255,255,0.70);
      border:1px solid rgba(255,255,255,0.85); }
    .img-box img{ width:100%; height:100%; object-fit:cover; display:block; }
    body.embedded aside, body.embedded header { display:none !important; }
    body.embedded main{ padding: 24px !important; }
    .line-clamp-1{ overflow:hidden; display:-webkit-box; -webkit-line-clamp:1; -webkit-box-orient:vertical; }
    .line-clamp-2{ overflow:hidden; display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; }
    .line-clamp-3{ overflow:hidden; display:-webkit-box; -webkit-line-clamp:3; -webkit-box-orient:vertical; }
    .kw-chip{ cursor:pointer; }
    .kw-chip:hover{ transform: translateY(-1px); }

    .review-grid{ display:grid; grid-template-columns: 1fr; gap: 14px; }
    @media (min-width: 768px){ .review-grid{ grid-template-columns: 1fr 1fr; } }
    @media (min-width: 1280px){ .review-grid{ grid-template-columns: 1fr 1fr 1fr; } }
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
        * 정적 HTML<br/>
        * JSON(data/meta.json, data/reviews.json) 로드
      </div>
    </div>

    <div class="mt-auto pt-8 text-xs font-bold text-slate-500">
      <div class="small-label text-blue-600 mb-2">Snapshot</div>
      <div>수집일: <span id="runDateSide" class="font-black text-slate-700">-</span></div>
      <div class="mt-2">기간: <span id="periodTextSide" class="font-black text-slate-700">-</span></div>
    </div>
  </aside>

  <main class="flex-1 p-6 md:p-10 w-full"><div class="mx-auto w-full max-w-[1280px]">
    <div class="max-w-[1320px] mx-auto">

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

      <section class="mb-10">
        <div class="glass-card p-8">
          <div class="flex items-end justify-between gap-6 flex-wrap mb-6">
            <div>
              <div class="small-label text-blue-600 mb-2">1. Summary</div>
              <div class="text-2xl font-black text-slate-900">핵심 이슈 한 장 요약</div>
            </div>
            <div class="text-xs font-black text-slate-500">
              * 최근 7일 고정 + 중립/관용구 제거 + 긍/부정 분리
            </div>
          </div>

          <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
            <div class="summary-card">
              <div class="small-label text-blue-600 mb-2">1-1) Size mention</div>
              <div class="text-3xl font-black"><span id="sizeMentionRate">-</span>%</div>
              <div class="text-xs font-bold text-slate-500 mt-2">전체 리뷰 중 사이즈 관련 언급 비중</div>
            </div>

            <div class="summary-card">
              <div class="small-label text-slate-700 mb-2">1-2) Keywords Top 5</div>

              <div class="text-xs font-black text-slate-500 mt-1">NEG (불만)</div>
              <div id="topNeg" class="mt-2 flex flex-wrap gap-2"></div>

              <div class="text-xs font-black text-slate-500 mt-4">POS (공감/강점)</div>
              <div id="topPos" class="mt-2 flex flex-wrap gap-2"></div>

              <div class="text-xs font-bold text-slate-500 mt-3">최근 7일 기반(클릭→해당 리뷰 이동)</div>
            </div>

            <div class="summary-card">
              <div class="small-label text-blue-600 mb-2">1-3) Priority Top 3</div>
              <ol id="priorityTop3" class="mt-2 space-y-2"></ol>
              <div class="text-xs font-bold text-slate-500 mt-3">개선 필요 제품 Top 3(사이즈 이슈율)</div>
            </div>
          </div>
        </div>
      </section>

      <section class="mb-10">
        <div class="glass-card p-8">
          <div class="flex items-end justify-between gap-6 flex-wrap mb-6">
            <div>
              <div class="small-label text-blue-600 mb-2">2. 개선 우선순위 제품 랭킹</div>
              <div class="text-xs font-bold text-slate-500 mt-1">Top5 기본 노출(접기/펼치기)</div>
            </div>
            <div class="flex gap-2">
              <button class="chip active" id="rank-size" onclick="switchRankMode('size')">2-1) 사이즈 이슈율</button>
              <button class="chip" id="rank-low" onclick="switchRankMode('low')">2-2) 저평점 비중</button>
              <button class="chip" id="rank-both" onclick="switchRankMode('both')">2-3) 교집합</button>
            </div>
          </div>

          <div class="flex items-center justify-end mb-3">
            <button class="chip" id="rankToggleBtn" onclick="toggleRankingExpand()">Top5만 보기</button>
          </div>

          <div class="overflow-auto">
            <table class="tbl min-w-[980px]">
              <thead>
                <tr>
                  <th class="text-left">제품명</th>
                  <th class="text-left">리뷰 수</th>
                  <th class="text-left">사이즈 이슈율</th>
                  <th class="text-left">저평점 비중</th>
                  <th class="text-left">주요 키워드</th>
                </tr>
              </thead>
              <tbody id="rankingBody"></tbody>
            </table>
          </div>
        </div>
      </section>

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
              <div class="text-xs font-bold text-slate-500 mb-2">Top5만 + OS 제외</div>
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

      <section class="mb-10">
        <div class="glass-card p-8">
          <div class="flex items-end justify-between gap-6 flex-wrap mb-6">
            <div>
              <div class="small-label text-blue-600 mb-2">4. Product Mindmap</div>
              <div class="text-xs font-bold text-slate-500 mt-1">최근 1년 누적 · 제품별 대표 문장(POS/NEG)</div>
            </div>
          </div>

          <div id="productMindmap" class="grid grid-cols-1 lg:grid-cols-3 gap-4"></div>
        </div>
      </section>

      <section class="mb-10">
        <div class="glass-card p-8">
          <div class="flex items-end justify-between gap-6 flex-wrap mb-6">
            <div>
              <div class="small-label text-blue-600 mb-2">Daily Feed</div>
              <div class="text-2xl font-black text-slate-900">그날 올라온 리뷰 (업로드 순)</div>
              <div class="text-sm font-bold text-slate-500 mt-2">최근 7일 범위 내에서 날짜 선택 가능</div>
            </div>
          </div>

          <div class="grid grid-cols-1 lg:grid-cols-5 gap-3 mb-6">
            <input id="daySelect" type="date" class="input-glass" onchange="renderAll()" />
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

          <div id="dailyFeed" class="review-grid"></div>

          <div class="hidden review-card text-center" id="noResults">
            <div class="text-lg font-black text-slate-800">검색 결과가 없습니다.</div>
          </div>
        </div>
      </section>

      <footer class="text-xs font-bold text-slate-500 pb-8">
        * 데이터 소스: reviews.json (최근 7일 필터 후 렌더).<br/>
        * 섹션4: 최근 1년 누적(제품별 POS/NEG 대표 문장).<br/>
        * 키워드: 중립/관용구/제품코드성 단어 제거 + 제품군별 자동 stopwords 학습.
      </footer>

    </div>
  </div></div></main>

  <script>
    const overlay = document.getElementById('overlay');
    const overlayMsg = document.getElementById('overlayMsg');

    const uiState = {
      sourceTab: 'combined',
      chips: { daily:true, pos:false, size:false, low:false },
      rankMode: 'size',
      rankExpanded: false,
      pendingScrollReviewId: null,
    };

    function showOverlay(msg){
      overlayMsg.textContent = msg || '잠시만요';
      overlay.classList.add('show');
    }
    function hideOverlay(){ overlay.classList.remove('show'); }
    function runWithOverlay(msg, fn){
      showOverlay(msg);
      setTimeout(() => { Promise.resolve().then(fn).finally(() => requestAnimationFrame(hideOverlay)); }, 0);
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

    function toggleRankingExpand(){
      uiState.rankExpanded = !uiState.rankExpanded;
      const btn = document.getElementById("rankToggleBtn");
      btn.textContent = uiState.rankExpanded ? "접기" : "Top5만 보기";
      btn.classList.toggle("active", uiState.rankExpanded);
      renderAll();
    }

    (function () {
      try { if (window.self !== window.top) document.body.classList.add("embedded"); }
      catch (e) { document.body.classList.add("embedded"); }
    })();

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
      if (isNaN(d.getTime())) return String(iso);
      const pad2 = (n) => String(n).padStart(2,'0');
      return `${d.getFullYear()}.${pad2(d.getMonth()+1)}.${pad2(d.getDate())} ${pad2(d.getHours())}:${pad2(d.getMinutes())}`;
    };

    function asArr(x){ return Array.isArray(x) ? x : []; }

    function applyKeywordAndScroll(keyword, reviewId){
      const q = document.getElementById("qInput");
      if (q) q.value = keyword || "";
      uiState.pendingScrollReviewId = reviewId || null;
      renderAll();
    }

    function tryScrollToReview(){
      const id = uiState.pendingScrollReviewId;
      if (!id) return;
      uiState.pendingScrollReviewId = null;

      requestAnimationFrame(() => {
        const el = document.getElementById("review-" + String(id));
        if (el){
          el.scrollIntoView({behavior:"smooth", block:"start"});
          el.classList.add("ring-4","ring-blue-200");
          setTimeout(()=> el.classList.remove("ring-4","ring-blue-200"), 1800);
        }
      });
    }

    // ✅ v5: DailyFeed 카드 클릭 → 필터 세팅 + 해당 리뷰로 이동
    function jumpToReview(payload){
      if (!payload) return;

      // 소스탭 자동
      if (payload.source === "Official") uiState.sourceTab = "official";
      else if (payload.source === "Naver") uiState.sourceTab = "naver";
      else uiState.sourceTab = "combined";
      document.querySelectorAll('.tab-btn').forEach(b => {
        b.classList.toggle('active', b.getAttribute('data-tab') === uiState.sourceTab);
      });

      // 날짜/제품/옵션
      const dayEl = document.getElementById("daySelect");
      if (dayEl && payload.created_at){
        dayEl.value = String(payload.created_at).slice(0,10);
      }
      const prodEl = document.getElementById("productSelect");
      if (prodEl) prodEl.value = payload.product_code || "";

      const sizeEl = document.getElementById("sizeSelect");
      if (sizeEl) sizeEl.value = payload.option_size || "";

      const qEl = document.getElementById("qInput");
      if (qEl) qEl.value = "";

      uiState.pendingScrollReviewId = payload.id || null;
      renderAll();
    }

    function getFilteredReviews(){
      let rows = REVIEWS.slice();

      if (uiState.sourceTab === "official") rows = rows.filter(r => r.source === "Official");
      if (uiState.sourceTab === "naver") rows = rows.filter(r => r.source === "Naver");

      const day = (document.getElementById("daySelect")?.value || "").trim();
      if (day){
        rows = rows.filter(r => String(r.created_at || "").slice(0,10) === day);
      }

      const productCode = document.getElementById("productSelect")?.value || "";
      if (productCode) rows = rows.filter(r => r.product_code === productCode);

      const sizeOpt = document.getElementById("sizeSelect")?.value || "";
      if (sizeOpt) rows = rows.filter(r => (r.option_size || "") === sizeOpt);

      const q = (document.getElementById("qInput")?.value || "").trim().toLowerCase();
      if (q){
        rows = rows.filter(r =>
          (r.product_name || "").toLowerCase().includes(q) ||
          (r.product_code || "").toLowerCase().includes(q) ||
          (r.text || "").toLowerCase().includes(q) ||
          (r.option_size || "").toLowerCase().includes(q)
        );
      }

      if (uiState.chips.pos) rows = rows.filter(r => asArr(r.tags).includes("pos"));
      if (uiState.chips.size) rows = rows.filter(r => asArr(r.tags).includes("size"));
      if (uiState.chips.low) rows = rows.filter(r => (r.rating ?? 0) <= 2 || asArr(r.tags).includes("low"));

      const sort = document.getElementById("sortSelect")?.value || "upload";
      if (sort === "latest") rows.sort((a,b) => new Date(b.created_at) - new Date(a.created_at));
      else if (sort === "long") rows.sort((a,b) => ((b.text||"").length - (a.text||"").length));
      else if (sort === "low") rows.sort((a,b) => ((a.rating||0) - (b.rating||0)) || (new Date(b.created_at)-new Date(a.created_at)));
      else rows.sort((a,b) => new Date(a.created_at) - new Date(b.created_at));

      return rows;
    }

    function calcMetrics(reviews){
      const total = reviews.length || 1;
      const sizeMention = reviews.filter(r => asArr(r.tags).includes("size")).length;

      const sizeRows = reviews.filter(r => asArr(r.tags).includes("size"));
      const smallCnt = sizeRows.filter(r => r.size_direction === "too_small").length;
      const bigCnt = sizeRows.filter(r => r.size_direction === "too_big").length;
      const denom = Math.max(1, smallCnt + bigCnt);
      const tooSmall = Math.round((smallCnt/denom)*100);
      const tooBig = 100 - tooSmall;

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
            kwPos: new Map(),
            kwNeg: new Map(),
          });
        }
        const g = byProd.get(code);
        g.reviews += 1;
        if (asArr(r.tags).includes("size")) g.sizeIssue += 1;
        if ((r.rating||0) <= 2 || asArr(r.tags).includes("low")) g.low += 1;
      }

      const rows = Array.from(byProd.values()).map(g => {
        const sizeRate = Math.round((g.sizeIssue / Math.max(1,g.reviews))*100);
        const lowRate  = Math.round((g.low / Math.max(1,g.reviews))*100);
        const kwds = "-";
        return { ...g, sizeRate, lowRate, kwds };
      });

      const rankSize = rows.slice().sort((a,b)=> b.sizeRate - a.sizeRate || b.reviews - a.reviews);
      const rankLow  = rows.slice().sort((a,b)=> b.lowRate  - a.lowRate  || b.reviews - a.reviews);
      const rankBoth = rows.slice().sort((a,b)=> ((b.sizeRate+b.lowRate) - (a.sizeRate+a.lowRate)) || b.reviews - a.reviews);

      // 옵션(사이즈) 이슈율 Top5 + OS 제외
      const sizeMap = new Map();
      for (const r of reviews){
        const sz = (r.option_size || "").trim();
        if (!sz) continue;
        if (sz.toUpperCase() === "OS") continue;
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

      let sizeOpts = Array.from(sizeMap.values())
        .sort((a,b)=> b.cnt - a.cnt)
        .map(x => {
          const cnt = Math.max(1, x.cnt);
          const smallP = Math.round((x.small/cnt)*100);
          const bigP = Math.round((x.big/cnt)*100);
          const okP = Math.max(0, 100 - smallP - bigP);
          return { sz:x.sz, cnt:x.cnt, small:smallP, big:bigP, ok:okP };
        })
        .slice(0, 5);

      return {
        total,
        sizeMentionRate: Math.round((sizeMention/total)*100),
        tooSmall,
        tooBig,
        rankSize, rankLow, rankBoth,
        sizeOpts
      };
    }

    function renderHeader(){
      const runDate = META?.updated_at || "-";
      const periodText = META?.period_text || "-";
      document.getElementById("runDateSide").textContent = String(runDate).slice(0,10).replaceAll("-",".");
      document.getElementById("periodTextSide").textContent = periodText;
      document.getElementById("headerMeta").textContent = `${runDate} · ${periodText} · 주 1회 자동 업데이트(월 09:00)`;
    }

    function renderSummary(metrics){
      document.getElementById("sizeMentionRate").textContent = metrics.sizeMentionRate;

      // ✅ v5: meta에서 pos_top5 / neg_top5 제공 (형식: [k,c,rid])
      const neg = META?.neg_top5 || [];
      const pos = META?.pos_top5 || [];
      const posEx = META?.pos_examples || {};

      const elNeg = document.getElementById("topNeg");
      elNeg.innerHTML = neg.map(([k,c, rid]) => `
        <span class="badge neg kw-chip" onclick="applyKeywordAndScroll('${esc(k)}', '${esc(rid||"")}')">
          #${esc(k)} <span class="opacity-70">${esc(c)}</span>
        </span>
      `).join("");

      const elPos = document.getElementById("topPos");
      elPos.innerHTML = pos.map(([k,c, rid]) => {
        const ex = (posEx && posEx[k] && posEx[k][0]) ? posEx[k][0].snip : "";
        return `
          <span class="badge pos kw-chip" title="${esc(ex)}" onclick="applyKeywordAndScroll('${esc(k)}', '${esc(rid||"")}')">
            #${esc(k)} <span class="opacity-70">${esc(c)}</span>
          </span>
        `;
      }).join("");

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

      const maxRows = uiState.rankExpanded ? Math.min(rows.length, 50) : Math.min(rows.length, 5);
      const tbody = document.getElementById("rankingBody");

      tbody.innerHTML = rows.slice(0, maxRows).map(r => `
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

      const fitWords = META?.fit_words || ["정사이즈","한치수 크게","한치수 작게","기장","소매","어깨","가슴","발볼"];
      const fit = document.getElementById("fitWords");
      fit.innerHTML = fitWords.map(w => `<span class="badge">${esc(w)}</span>`).join("");
    }

    // ✅ v5: Product Mindmap = 문장형 (POS/NEG 각 2개)
    function renderProductMindmap(){
      const mm = META?.product_mindmap_1y || [];
      const root = document.getElementById("productMindmap");
      if (!mm.length){
        root.innerHTML = `<div class="summary-card lg:col-span-3"><div class="text-sm font-black text-slate-700">마인드맵 데이터가 없습니다.</div></div>`;
        return;
      }

      const sentenceBlock = (mode, s) => {
        const color = (mode==="pos") ? "text-emerald-700" : "text-red-700";
        const bg = "bg-white/55 border border-white/80";
        const onclick = (s?.id!=null) ? `onclick="applyKeywordAndScroll('', '${esc(s.id)}')"` : "";
        return `
          <div class="mt-2 p-3 rounded-2xl ${bg} cursor-pointer" ${onclick}>
            <div class="text-xs font-black ${color}">
              ${mode.toUpperCase()} · ★ ${esc(s.rating ?? "-")} · ${esc(fmtDT(s.created_at))}
            </div>
            <div class="text-sm font-extrabold text-slate-800 leading-relaxed mt-2">
              ${esc(s.text || "-")}
            </div>
          </div>
        `;
      };

      root.innerHTML = mm.map(p => {
        const img = p.local_product_image
          ? `<img src="${esc(p.local_product_image)}" alt="" class="w-14 h-14 rounded-2xl object-cover border border-white/80 bg-white/60" />`
          : `<div class="w-14 h-14 rounded-2xl flex items-center justify-center text-[10px] text-slate-400 border border-white/80 bg-white/60">NO IMAGE</div>`;

        const pos = (p.pos_sentences || []).slice(0,2);
        const neg = (p.neg_sentences || []).slice(0,2);

        return `
          <div class="summary-card">
            <div class="flex items-start gap-3">
              ${img}
              <div class="min-w-0">
                <div class="font-black text-slate-900 line-clamp-2">${esc(p.product_name || p.product_code)}</div>
                <div class="text-[11px] font-black text-slate-500 mt-1">code: ${esc(p.product_code)} · 1y reviews: ${esc(p.reviews_1y)}</div>
              </div>
            </div>

            <div class="mt-4">
              ${pos.length ? pos.map(s => sentenceBlock("pos", s)).join("") : `<div class="text-xs font-bold text-slate-400 mt-2">POS 문장 없음</div>`}
            </div>

            <div class="mt-4">
              ${neg.length ? neg.map(s => sentenceBlock("neg", s)).join("") : `<div class="text-xs font-bold text-slate-400 mt-2">NEG 문장 없음</div>`}
            </div>
          </div>
        `;
      }).join("");
    }

    function reviewCardHTML(r){
      const t = asArr(r.tags);
      const tags = [];

      if (t.includes("pos")) tags.push(`<span class="badge pos"><i class="fa-solid fa-face-smile"></i> #긍정</span>`);
      if (t.includes("size")) tags.push(`<span class="badge size"><i class="fa-solid fa-ruler"></i> #size_issue</span>`);
      if ((r.rating||0) <= 2 || t.includes("low")) tags.push(`<span class="badge neg"><i class="fa-solid fa-triangle-exclamation"></i> #low_rating</span>`);
      if (t.includes("req")) tags.push(`<span class="badge neg"><i class="fa-solid fa-wrench"></i> #개선요청</span>`);
      if (r.text_image_path) tags.push(`<span class="badge"><i class="fa-solid fa-image"></i> #100자+이미지</span>`);

      const prodImg = r.local_product_image
        ? `<img src="${esc(r.local_product_image)}" alt="">`
        : `<div class="w-full h-full flex items-center justify-center text-[10px] text-slate-400">NO IMAGE</div>`;

      const reviewThumb = r.local_review_thumb
        ? `<img src="${esc(r.local_review_thumb)}" class="w-full max-h-56 object-contain rounded-lg bg-slate-50" />`
        : ``;

      const textImg = r.text_image_path
        ? `<img src="${esc(r.text_image_path)}" class="w-full max-h-72 object-contain rounded-2xl bg-slate-50 border border-white/80" />`
        : ``;

      const payload = {
        id: r.id,
        source: r.source,
        created_at: r.created_at,
        product_code: r.product_code,
        option_size: r.option_size
      };

      return `
        <div class="review-card cursor-pointer hover:shadow-lg transition" id="review-${esc(r.id)}"
             onclick='jumpToReview(${JSON.stringify(payload).replaceAll("'", "\\'")})'>
          <div class="flex items-start justify-between gap-3">
            <div class="flex items-center gap-3 min-w-0">
              <div class="img-box">${prodImg}</div>
              <div class="min-w-0">
                <div class="font-black text-slate-900 line-clamp-1">${esc(r.product_name || r.product_code)}</div>
                <div class="text-xs font-bold text-slate-500 mt-1">
                  code: ${esc(r.product_code)} · source: ${esc(r.source || "-")} · id: ${esc(r.id)}
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

          <div class="mt-3 text-sm font-extrabold text-slate-800 leading-relaxed whitespace-pre-wrap break-words line-clamp-3">
            ${esc(r.text || "")}
          </div>

          ${reviewThumb ? `<div class="mt-3">${reviewThumb}</div>` : ``}
          ${textImg ? `<div class="mt-4"><div class="small-label text-blue-600 mb-2">100+ TEXT IMAGE</div>${textImg}</div>` : ``}
        </div>
      `;
    }

    function renderDailyFeed(reviews){
      const container = document.getElementById("dailyFeed");
      const no = document.getElementById("noResults");

      const rows = reviews.slice(0, 30);
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

    function renderAll(){
      const filtered = getFilteredReviews();
      const metrics = calcMetrics(filtered);

      renderHeader();
      renderProductSelect();
      renderSizeSelect();

      renderSummary(metrics);
      renderRanking(metrics);
      renderSizeStructure(metrics);
      renderProductMindmap();
      renderDailyFeed(filtered);

      tryScrollToReview();
    }

    async function boot(){
      runWithOverlay("Loading data...", async () => {
        const [meta, reviews] = await Promise.all([
          fetch("data/meta.json", {cache:"no-store"}).then(r => r.json()),
          fetch("data/reviews.json", {cache:"no-store"}).then(r => r.json())
        ]);

        META = meta;
        REVIEWS = (reviews && reviews.reviews) ? reviews.reviews : [];

        const dayInput = document.getElementById("daySelect");
        if (dayInput && META?.period_start && META?.period_end){
          dayInput.min = META.period_start;
          dayInput.max = META.period_end;
        }

        renderAll();
      });
    }

    document.addEventListener("DOMContentLoaded", boot);
  </script>
</body>
</html>
"""


def load_html_template() -> str:
    tpl = SITE_DIR / "template.html"
    if tpl.exists():
        return tpl.read_text(encoding="utf-8")
    return DEFAULT_HTML_TEMPLATE


# ----------------------------
# Main
# ----------------------------
def main(input_path: str):
    inp = pathlib.Path(input_path).expanduser().resolve()
    if not inp.exists():
        raise FileNotFoundError(f"input not found: {inp}")

    all_rows = read_reviews_json(inp)
    now = now_kst()

    # 최근 7일(오늘 포함)
    start7 = (now - timedelta(days=6)).date()
    end7 = now.date()

    # 최근 1년
    start1y = (now - timedelta(days=365)).date()
    end1y = now.date()

    rows7: List[Dict[str, Any]] = []
    rows1y: List[Dict[str, Any]] = []
    for r in all_rows:
        dt = parse_created_at_iso(str(r.get("created_at") or ""))
        if in_date_range_kst(dt, start7, end7):
            rows7.append(r)
        if in_date_range_kst(dt, start1y, end1y):
            rows1y.append(r)

    if not rows7:
        raise SystemExit("최근 7일에 해당하는 리뷰가 없습니다. created_at 포맷/타임존/기간을 확인해 주세요.")

    df7 = pd.DataFrame(rows7).copy()
    df7["rating"] = pd.to_numeric(df7.get("rating"), errors="coerce").fillna(0).astype(int)

    # AUTO STOPWORDS 학습(최근7일 기준)
    auto_sw = learn_auto_stopwords(rows7)

    # POS/NEG 키워드 Top5
    pos_top = top_terms(rows7, TOPK_POS, auto_sw, which="pos")
    neg_top = top_terms(rows7, TOPK_NEG, auto_sw, which="neg")

    pos_keys = [k for k, _ in pos_top]
    neg_keys = [k for k, _ in neg_top]

    # 근거(클릭용)
    pos_evidence = build_keyword_evidence(rows7, pos_keys, auto_sw, filter_fn=is_positive, evidence_per_kw=3)
    neg_evidence = build_keyword_evidence(rows7, neg_keys, auto_sw, filter_fn=is_complaint, evidence_per_kw=3)

    # Summary에서 기대하는 포맷: [k,c,rid]
    def attach_rid(top_list: List[Tuple[str, int]], evi_map: Dict[str, List[Dict[str, Any]]]) -> List[List[Any]]:
        out = []
        for k, c in top_list:
            rid = None
            evs = evi_map.get(k) or []
            if evs:
                rid = evs[0].get("id")
            out.append([k, int(c), rid])
        return out

    pos_top3 = attach_rid(pos_top, pos_evidence)
    neg_top3 = attach_rid(neg_top, neg_evidence)

    # 클러스터 집계(불만 리뷰 기준)
    cluster_counts: Dict[str, int] = {k: 0 for k in CLUSTERS.keys()}
    for r in rows7:
        if not is_complaint(r):
            continue
        for h in assign_cluster(str(r.get("text") or "")):
            cluster_counts[h] = cluster_counts.get(h, 0) + 1

    # size phrases
    size_rows = [r for r in rows7 if isinstance(r.get("tags"), list) and ("size" in r.get("tags"))]
    size_phrases_terms = top_terms(size_rows, topk=10, auto_sw=auto_sw, which="neg")
    size_phrases = [k for k, _ in size_phrases_terms]

    # ✅ v5 Mindmap: 문장형
    product_mindmap_1y = build_product_mindmap_1y_sentence(
        rows_1y=rows1y,
        rows_7d=rows7,
        per_side=MINDMAP_SENT_PER_SIDE,
        max_products=MINDMAP_MAX_PRODUCTS,
    )

    period_text = f"최근 7일 ({start7.isoformat()} ~ {end7.isoformat()})"

    meta = {
        "updated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "period_text": period_text,
        "period_start": start7.isoformat(),
        "period_end": end7.isoformat(),
        "total_reviews": int(len(df7)),

        # ✅ Summary에서 쓰는 키 (HTML 그대로)
        "pos_top5": pos_top3,     # [k,c,rid]
        "neg_top5": neg_top3,     # [k,c,rid]

        # 디버그/추적용 (원하면 UI에 활용 가능)
        "keywords_top5": {
            "pos": [[k, int(c)] for k, c in pos_top],
            "neg": [[k, int(c)] for k, c in neg_top],
        },

        # 클릭 근거(키워드→리뷰들)
        "keyword_evidence": {
            "pos": pos_evidence,
            "neg": neg_evidence,
        },

        # auto stopwords
        "auto_stopwords": auto_sw,

        # clusters
        "clusters": cluster_counts,
        "size_phrases": size_phrases,

        "fit_words": ["정사이즈", "한치수 크게", "한치수 작게", "타이트", "넉넉", "기장", "소매", "어깨", "가슴", "발볼", "수납", "가벼움"],

        # ✅ v5: Product Mindmap 문장형
        "product_mindmap_1y": product_mindmap_1y,
    }

    # 최근7일 reviews.json (프론트 필터용 pos 태그 보강)
    out_reviews: List[Dict[str, Any]] = []
    for r in df7.to_dict(orient="records"):
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
            "rating": int(r.get("rating") or 0),
            "created_at": r.get("created_at", ""),
            "text": r.get("text", ""),
            "source": r.get("source", "Official"),
            "option_size": r.get("option_size", ""),
            "option_color": r.get("option_color", ""),
            "tags": tags,
            "size_direction": r.get("size_direction", "other"),
            "local_product_image": r.get("local_product_image", ""),
            "local_review_thumb": r.get("local_review_thumb", ""),
            "text_image_path": r.get("text_image_path", ""),
        })

    html = load_html_template()

    (SITE_DATA_DIR / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    (SITE_DATA_DIR / "reviews.json").write_text(json.dumps({"reviews": out_reviews}, ensure_ascii=False, indent=2), encoding="utf-8")
    (SITE_DIR / "index.html").write_text(html, encoding="utf-8")

    print("[OK] Build done (v5)")
    print(f"- Input: {inp}")
    print(f"- Output meta: {SITE_DATA_DIR / 'meta.json'}")
    print(f"- Output reviews: {SITE_DATA_DIR / 'reviews.json'}")
    print(f"- Output html: {SITE_DIR / 'index.html'}")
    print(f"- Period: {period_text}")
    print(f"- POS Top5: {pos_top}")
    print(f"- NEG Top5: {neg_top}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="path to reviews.json (aggregated JSON: {reviews:[...]})")
    args = ap.parse_args()
    main(args.input)
