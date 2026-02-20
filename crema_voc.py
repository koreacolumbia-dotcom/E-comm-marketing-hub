#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
[BUILD VOC DASHBOARD FROM JSON | v4.0 FINAL]
- 입력: reviews.json ({"reviews":[...]}), created_at ISO 권장
- 출력:
  - site/data/reviews.json  (최근 7일로 필터된 리뷰만)
  - site/data/meta.json     (기간/키워드/클러스터/제품별 1y 키워드맵 등)
  - site/index.html         (요구 UI 반영: DailyFeed 3열, 키워드 클릭→리뷰 이동, '주요 키워드' 등)

핵심 개선
1) 최근 7일 고정 + KST 처리
2) 긍/부정 키워드 명확 분리
3) 제품군/카테고리별 STOPWORDS 자동학습(=자주 나오는 중립어 자동 제거)
4) 이슈 클러스터링(사이즈/품질/배송/CS/가격/디자인/기능)
5) Complaint Top5는 '원인' 중심(중립/관용구 제거)
6) 랭킹 '주요 문제 키워드' → '주요 키워드'
7) 4번 섹션: 제품별(최근 1년) POS/NEG 키워드 맵 + 이미지 + 클릭 시 리뷰로 이동(최근7일 범위 내)
8) Daily Feed: 가로폭 줄이고 3개씩(반응형) + 카드에 anchor 부여

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
AUTO_STOP_MIN_DF = 0.30      # 전체 문서(리뷰) 중 30% 이상 등장하면 후보
AUTO_STOP_MIN_PRODUCTS = 0.25  # 제품 수 기준 25% 이상 제품에 등장하면 후보
AUTO_STOP_POLARITY_MARGIN = 0.20  # pos/neg 비율이 너무 비슷하면(중립) 제거
AUTO_STOP_MAX_ADD = 120      # 너무 많이 추가되는 것 방지

# 키워드 추출 파라미터
TOPK_POS = 5
TOPK_NEG = 5
TOPK_COMPLAINT = 5

# 클러스터(원인) 정의: 토큰 기준 매칭(공백 제거한 원문에도 보조 매칭)
CLUSTERS = {
    "size": ["사이즈", "정사이즈", "작", "크", "타이트", "헐렁", "끼", "기장", "소매", "어깨", "가슴", "발볼", "핏"],
    "quality": ["품질", "불량", "하자", "찢", "구멍", "실밥", "오염", "변색", "냄새", "마감", "내구", "퀄리티"],
    "shipping": ["배송", "택배", "출고", "도착", "지연", "늦", "빠르", "포장", "파손"],
    "cs": ["문의", "응대", "고객", "cs", "교환", "반품", "환불", "처리", "as"],
    "price": ["가격", "비싸", "싸", "가성비", "할인", "쿠폰", "대비"],
    "design": ["디자인", "색", "컬러", "예쁘", "멋", "스타일", "핏감"],
    "function": ["수납", "넣", "포켓", "공간", "가볍", "무게", "따뜻", "보온", "방수", "기능", "편하", "착용감", "그립"],
}

# 제품군(카테고리) 간이 규칙(자동 STOPWORDS를 "카테고리별"로도 학습)
CATEGORY_RULES = [
    ("bag", re.compile(r"(backpack|rucksack|bag|pouch|shoulder|packable|duffel|tote|힙색|백팩|가방|파우치|숄더)", re.I)),
    ("shoe", re.compile(r"(shoe|boot|chukka|sneaker|omni|heat|신발|부츠|워커)", re.I)),
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

# “너무 흔한 말/의미없는 말” 기본 STOPWORDS (여기에 '구입/구매/같은/기존' 같은 중립어도 기본 포함)
BASE_STOPWORDS = set(
    """
그리고 그러나 그래서 하지만 또한
너무 정말 완전 진짜 매우 그냥 조금 약간
저는 제가 우리는 너희 이거 그거 저거
있습니다 입니다 같아요 같네요 하는 하다 됐어요 되었어요 되네요
구매 구입 구입해 구매해 샀어요 샀습니다 주문 주문했
제품 상품 물건
사용 착용 입어 신어 써봤
배송 택배 포장
문의
좋아요 좋다 좋네요 만족 추천 재구매 가성비 최고 굿 예뻐요 이뻐요
정사이즈 한치수 한 치수
컬러 색상 디자인
있어서 있어서요 있어요 있네요 있었어요
좋습니다 좋았어요
추가 추가로 추가하면
가능 가능해요
확인 확인해요
생각 생각해요
느낌 느낌이에요
정도 정도로
부분 부분이
사람 분들
기존 같은 동일 비슷 비슷한 원래
이번 이번엔 이번에
그냥그냥
"""
    .split()
)

# 조사/어미 제거 (간이 stemming)
JOSA = [
    "은", "는", "이", "가", "을", "를", "에", "에서", "에게", "으로", "로",
    "와", "과", "도", "만", "까지", "부터", "보다", "처럼", "같이", "이나", "나",
]
ENDING = [
    "입니다", "습니다", "했어요", "했네요", "해요", "하네요", "했음", "했습", "합니다",
    "같아요", "같네요", "있어요", "있네요", "있습니다",
]

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

        # 보조 어근 컷(정보량 낮음)
        if t in ("있", "좋", "하", "되", "같", "되었", "했", "해", "함"):
            continue

        out.append(t)

    return out


# ----------------------------
# Tags / complaint / sentiment seed
# ----------------------------
SIZE_KEYWORDS = [
    "사이즈", "정사이즈", "작아요", "작다", "커요", "크다", "핏", "타이트", "여유",
    "끼", "기장", "소매", "어깨", "가슴", "발볼", "헐렁", "오버"
]
REQ_KEYWORDS = ["개선", "아쉬", "불편", "했으면", "추가", "보완", "수정", "필요", "요청", "교환", "반품", "환불"]
COMPLAINT_HINTS = ["불량", "하자", "찢", "구멍", "냄새", "변색", "오염", "실밥", "품질", "엉망", "별로", "최악", "실망", "문제", "불편"]

# 긍정 시드(토큰 매칭): 기능/수납/가벼움/편안함이 NEG로 잘못 들어가는 걸 방지
POS_SEEDS = [
    "가볍", "무게", "편하", "편안", "착용감", "수납", "포켓", "공간", "넣", "따뜻", "보온", "방수", "튼튼", "견고", "퀄리티", "만족", "예쁘", "멋",
    "좋", "좋아", "잘", "잘맞", "잘맞아", "딱", "깔끔", "마감", "신발", "쿠션"
]
NEG_SEEDS = [
    "불량", "하자", "찢", "구멍", "냄새", "변색", "오염", "실밥", "최악", "별로", "실망", "불편", "문제", "아쉽", "작", "크", "타이트", "헐렁",
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

    # low
    if rating <= 2 and "low" not in tags:
        tags.append("low")

    # size
    fit_q = str(row.get("fit_q") or "")
    if ("size" not in tags) and (has_any_kw(text, SIZE_KEYWORDS) or fit_q in ("조금 작아요", "작아요", "조금 커요", "커요")):
        tags.append("size")

    # req
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
    """
    POS 정의:
    - rating >= 4 이면서 complaint 아님
    - 또는 rating == 3 이더라도 POS_SEEDS가 강하게 있고 NEG_SEEDS가 거의 없으면 POS로 취급(중립→POS 보정)
    """
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
def learn_auto_stopwords(rows: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    """
    rows(최근7일) 기준으로
    - global + category별 중립 토큰 자동 STOPWORDS 학습
    - 중립 기준:
      * 문서 빈도 높음(DF) + 제품 분산 높음 + pos/neg 비율이 비슷(=정보량 낮음)
    """
    if not rows:
        return {"global": [], "by_category": {}}

    # 라벨링(최근7일 기준)
    pos_rows = [r for r in rows if is_positive(r)]
    neg_rows = [r for r in rows if is_complaint(r)]

    # 제품 집합
    prod_all = set(str(r.get("product_code") or "-") for r in rows)
    prod_n = max(1, len(prod_all))

    # 토큰 통계(글로벌)
    def build_stats(sub_rows: List[Dict[str, Any]]) -> Tuple[Dict[str, int], Dict[str, set], Dict[str, int]]:
        freq: Dict[str, int] = {}
        df: Dict[str, set] = {}
        prod_df: Dict[str, set] = {}
        for r in sub_rows:
            pid = str(r.get("id"))
            pcode = str(r.get("product_code") or "-")
            toks = set(tokenize_ko(str(r.get("text") or ""), stopwords=BASE_STOPWORDS))
            for t in toks:
                df.setdefault(t, set()).add(pid)
                prod_df.setdefault(t, set()).add(pcode)
            # freq는 토큰 수(중복 포함)로, DF는 set로
            for t in tokenize_ko(str(r.get("text") or ""), stopwords=BASE_STOPWORDS):
                freq[t] = freq.get(t, 0) + 1
        df_cnt = {k: len(v) for k, v in df.items()}
        prod_cnt = {k: len(v) for k, v in prod_df.items()}
        return freq, prod_cnt, df_cnt

    pos_freq, pos_prod_cnt, pos_df = build_stats(pos_rows)
    neg_freq, neg_prod_cnt, neg_df = build_stats(neg_rows)

    # 전체 DF / 제품 DF
    all_ids = set(str(r.get("id")) for r in rows)
    all_n = max(1, len(all_ids))

    # all df: pos_df U neg_df (근사)
    all_df = {}
    all_prod_cnt = {}
    keys = set(pos_df.keys()) | set(neg_df.keys())
    for k in keys:
        all_df[k] = pos_df.get(k, 0) + neg_df.get(k, 0)  # pos/neg 리뷰가 겹치는 id는 거의 없다고 가정
        all_prod_cnt[k] = len(set(
            ([] if k not in pos_prod_cnt else [None])  # dummy
        ))

    # 제품 DF는 따로 계산(정확하게)
    prod_df_map: Dict[str, set] = {}
    for r in rows:
        pcode = str(r.get("product_code") or "-")
        toks = set(tokenize_ko(str(r.get("text") or ""), stopwords=BASE_STOPWORDS))
        for t in toks:
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
        # polarity: neg 비율
        neg_ratio = n / total if total else 0.0
        # 너무 중립(0.5 근처)이면 제거
        if abs(neg_ratio - 0.5) <= AUTO_STOP_POLARITY_MARGIN:
            auto_global.append(t)
        if len(auto_global) >= AUTO_STOP_MAX_ADD:
            break

    # category별
    by_cat: Dict[str, List[str]] = {}
    cat_groups: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        cat = detect_category(str(r.get("product_name") or ""), str(r.get("product_code") or ""))
        cat_groups.setdefault(cat, []).append(r)

    for cat, group in cat_groups.items():
        if len(group) < 12:
            by_cat[cat] = []
            continue
        # group 내부에서 token DF 높은 것
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
# Keyword extraction (pos/neg/complaint)
# ----------------------------
def top_terms(
    rows: List[Dict[str, Any]],
    topk: int,
    auto_sw: Dict[str, Any],
    which: str,
) -> List[Tuple[str, int]]:
    """
    which: "pos" | "neg" | "complaint"
    - pos: is_positive
    - neg: is_complaint
    - complaint: is_complaint + 클러스터/원인 기반으로 더 강하게 정제(중립/관용 제거)
    """
    freq: Dict[str, int] = {}
    for r in rows:
        if which == "pos" and not is_positive(r):
            continue
        if which in ("neg", "complaint") and not is_complaint(r):
            continue

        sw = build_stopwords_for_row(r, auto_sw)
        text = str(r.get("text") or "")
        toks = tokenize_ko(text, stopwords=sw)

        # complaint는 NEG_SEEDS(원인)와 클러스터 키워드에 가중
        if which == "complaint":
            tnorm = normalize_text(text)
            for t in toks:
                w = 1
                # 원인/클러스터 힌트면 가중
                if any(seed in t for seed in NEG_SEEDS):
                    w += 1
                if any(any(k in t for k in kws) for kws in CLUSTERS.values()):
                    w += 1
                # POS_SEEDS 계열(가볍/수납 등)은 complaint에서 제외(오탐 방지)
                if any(seed in t for seed in POS_SEEDS):
                    continue
                freq[t] = freq.get(t, 0) + w
        else:
            for t in toks:
                # pos에서 neg seed가 강하면 제외
                if which == "pos" and any(seed in t for seed in NEG_SEEDS):
                    continue
                # neg에서 pos seed가 강하면 제외(“수납력 좋다” 같은 오탐 방지)
                if which == "neg" and any(seed in t for seed in POS_SEEDS):
                    continue
                freq[t] = freq.get(t, 0) + 1

    return sorted(freq.items(), key=lambda x: x[1], reverse=True)[:topk]


def assign_cluster(text: str) -> List[str]:
    """
    리뷰 1건에 대해 원인 클러스터 여러개 가능
    """
    tnorm = normalize_text(text).replace(" ", "")
    hits = []
    for c, kws in CLUSTERS.items():
        for k in kws:
            if k.replace(" ", "") in tnorm:
                hits.append(c)
                break
    # 아무것도 없으면 빈
    return hits


# ----------------------------
# Evidence / anchors for click
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
    evidence_per_kw: int = 4,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    키워드별 근거 리뷰(id 포함). UI에서 클릭하면 #rev-{id} 로 이동.
    """
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
    # 정렬/슬라이스
    out2: Dict[str, List[Dict[str, Any]]] = {}
    for k, evs in out.items():
        evs.sort(key=lambda x: x.created_at, reverse=True)
        out2[k] = [e.__dict__ for e in evs[:evidence_per_kw]]
    return out2


# ----------------------------
# Product-level 1y keyword map (POS/NEG)
# ----------------------------
def build_product_keyword_map_1y(
    all_rows_1y: List[Dict[str, Any]],
    last7_rows: List[Dict[str, Any]],
    auto_sw_last7: Dict[str, Any],
    per_side_topk: int = 6,
) -> List[Dict[str, Any]]:
    """
    제품별(최근 1년) POS/NEG 키워드 맵
    - 키워드는 1y로 집계
    - 클릭 이동은 last7에 존재하는 리뷰 id만 연결(없으면 evidence 비움)
    """
    by_prod: Dict[str, List[Dict[str, Any]]] = {}
    for r in all_rows_1y:
        code = str(r.get("product_code") or "-")
        by_prod.setdefault(code, []).append(r)

    # last7에서 증거(클릭 가능)용 인덱스
    last7_by_prod: Dict[str, List[Dict[str, Any]]] = {}
    for r in last7_rows:
        code = str(r.get("product_code") or "-")
        last7_by_prod.setdefault(code, []).append(r)

    out = []
    for code, rows in by_prod.items():
        # 대표 메타
        sample = rows[0]
        pname = str(sample.get("product_name") or code)
        img = str(sample.get("local_product_image") or "")
        # 1y 키워드: auto stopwords는 last7에서 학습된 것으로 사용(일관성)
        pos_terms_1y = top_terms(rows, per_side_topk, auto_sw_last7, which="pos")
        neg_terms_1y = top_terms(rows, per_side_topk, auto_sw_last7, which="neg")

        pos_keys = [k for k, _ in pos_terms_1y]
        neg_keys = [k for k, _ in neg_terms_1y]

        # 클릭 증거는 last7에서만 연결
        l7 = last7_by_prod.get(code, [])
        pos_evi = build_keyword_evidence(
            l7,
            pos_keys,
            auto_sw_last7,
            filter_fn=lambda r: is_positive(r),
            evidence_per_kw=2,
        )
        neg_evi = build_keyword_evidence(
            l7,
            neg_keys,
            auto_sw_last7,
            filter_fn=lambda r: is_complaint(r),
            evidence_per_kw=2,
        )

        out.append(
            {
                "product_code": code,
                "product_name": pname,
                "local_product_image": img,
                "reviews_1y": len(rows),
                "pos": [[k, int(c)] for k, c in pos_terms_1y],
                "neg": [[k, int(c)] for k, c in neg_terms_1y],
                "pos_evidence": pos_evi,
                "neg_evidence": neg_evi,
            }
        )

    # 정렬: 최근1y 리뷰 수 많은 순
    out.sort(key=lambda x: x.get("reviews_1y", 0), reverse=True)
    return out[:24]  # 너무 길어지면 UI 무거움 방지


# ----------------------------
# HTML template loader + patch
# ----------------------------
def load_html_template() -> str:
    """
    기존 너의 HTML을 그대로 쓰되(길어서 여기서 재정의 안 함),
    site/template.html 있으면 그걸 읽고, 없으면 현재 site/index.html을 템플릿으로 사용.
    (둘 다 없으면 최소 템플릿 fallback)
    """
    tpl1 = SITE_DIR / "template.html"
    if tpl1.exists():
        return tpl1.read_text(encoding="utf-8")

    idx = SITE_DIR / "index.html"
    if idx.exists():
        return idx.read_text(encoding="utf-8")

    # fallback (최소)
    return "<!doctype html><html><head><meta charset='utf-8'><title>VOC</title></head><body><h1>VOC</h1></body></html>"


def patch_html(html: str) -> str:
    """
    요청 UI 패치:
    - main 폭 제한 + 중앙 정렬
    - Daily feed 3열 grid
    - '주요 문제 키워드' → '주요 키워드'
    - 4번 섹션을 제품별 1y 키워드맵 렌더링 컨테이너로 변경 (id 유지/교체)
    - 키워드 칩 클릭시 scrollToReview(id)
    """
    # 1) main 폭 제한
    html = html.replace(
        '<main class="flex-1 p-8 md:p-14">',
        '<main class="flex-1 p-6 md:p-10 w-full"><div class="mx-auto w-full max-w-[1280px]">',
    )
    # footer 이후 닫기 추가 (이미 있으면 중복 방지)
    if "</main>" in html and "</div></main>" not in html:
        html = html.replace("</main>", "</div></main>")

    # 2) dailyFeed 컨테이너: space-y -> grid
    html = html.replace('id="dailyFeed" class="space-y-4"', 'id="dailyFeed" class="review-grid"')

    # 3) CSS grid 추가/수정: review-list 대신 review-grid (3열)
    inject_css = """
    .review-grid{ display:grid; grid-template-columns: 1fr; gap: 14px; }
    @media (min-width: 768px){ .review-grid{ grid-template-columns: 1fr 1fr; } }
    @media (min-width: 1280px){ .review-grid{ grid-template-columns: 1fr 1fr 1fr; } }
    """
    html = html.replace("</style>", inject_css + "\n</style>")

    # 4) 랭킹 헤더 텍스트
    html = html.replace("주요 문제 키워드", "주요 키워드")

    # 5) 4번 섹션 문구 교체 (컨테이너 id는 mindmap 유지하되 내용 렌더링을 제품맵으로 사용)
    html = html.replace("4. 대표 리뷰 마인드맵", "4. 제품별 긍/부정 키워드 맵 (최근 1년)")

    # 6) 스크롤 함수 삽입: </script> 앞에
    scroll_js = """
    function scrollToReview(id){
      if(!id && id !== 0) return;
      const el = document.getElementById('rev-' + String(id));
      if(el){
        el.scrollIntoView({behavior:'smooth', block:'start'});
        el.classList.add('ring-4','ring-blue-200');
        setTimeout(()=>el.classList.remove('ring-4','ring-blue-200'), 1400);
      }
    }
    """
    html = html.replace("</script>\n</body>", scroll_js + "\n</script>\n</body>")

    return html


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
# Main
# ----------------------------
def main(input_path: str):
    inp = pathlib.Path(input_path).expanduser().resolve()
    if not inp.exists():
        raise FileNotFoundError(f"input not found: {inp}")

    all_rows = read_reviews_json(inp)

    now = now_kst()

    # ✅ 최근 7일 고정(오늘 포함 7일)
    start7 = (now - timedelta(days=6)).date()
    end7 = now.date()

    # ✅ 최근 1년(제품별 키워드맵)
    start1y = (now - timedelta(days=365)).date()
    end1y = now.date()

    rows7 = []
    rows1y = []
    for r in all_rows:
        dt = parse_created_at_iso(str(r.get("created_at") or ""))
        if in_date_range_kst(dt, start7, end7):
            rows7.append(r)
        if in_date_range_kst(dt, start1y, end1y):
            rows1y.append(r)

    if not rows7:
        raise SystemExit("최근 7일에 해당하는 리뷰가 없습니다. created_at 포맷/타임존/기간을 확인해 주세요.")

    # rating 정규화
    df7 = pd.DataFrame(rows7).copy()
    df7["rating"] = pd.to_numeric(df7.get("rating"), errors="coerce").fillna(0).astype(int)

    # ✅ AUTO STOPWORDS 학습(최근7일 기준)
    auto_sw = learn_auto_stopwords(rows7)

    # ✅ POS/NEG 키워드
    pos_top = top_terms(rows7, TOPK_POS, auto_sw, which="pos")
    neg_top = top_terms(rows7, TOPK_NEG, auto_sw, which="neg")
    complaint_top = top_terms(rows7, TOPK_COMPLAINT, auto_sw, which="complaint")

    pos_keys = [k for k, _ in pos_top]
    neg_keys = [k for k, _ in neg_top]
    complaint_keys = [k for k, _ in complaint_top]

    # ✅ 근거 리뷰(클릭 이동용)
    pos_evidence = build_keyword_evidence(rows7, pos_keys, auto_sw, filter_fn=is_positive, evidence_per_kw=3)
    neg_evidence = build_keyword_evidence(rows7, neg_keys, auto_sw, filter_fn=is_complaint, evidence_per_kw=3)
    complaint_evidence = build_keyword_evidence(rows7, complaint_keys, auto_sw, filter_fn=is_complaint, evidence_per_kw=3)

    # ✅ 클러스터 집계(원인 구조)
    cluster_counts: Dict[str, int] = {k: 0 for k in CLUSTERS.keys()}
    for r in rows7:
        if not is_complaint(r):
            continue
        hits = assign_cluster(str(r.get("text") or ""))
        for h in hits:
            cluster_counts[h] = cluster_counts.get(h, 0) + 1

    # ✅ size phrases(최근7일 size 태그 기반)
    size_rows = [r for r in rows7 if isinstance(r.get("tags"), list) and ("size" in r.get("tags"))]
    size_phrases = top_terms(size_rows, topk=10, auto_sw=auto_sw, which="neg")  # size 이슈는 neg 토큰에서 더 잘 나옴
    size_phrases = [k for k, _ in size_phrases]

    # ✅ 제품별(최근 1년) POS/NEG 키워드맵
    product_map_1y = build_product_keyword_map_1y(rows1y, rows7, auto_sw_last7=auto_sw, per_side_topk=6)

    # ✅ period text
    period_text = f"최근 7일 ({start7.isoformat()} ~ {end7.isoformat()})"

    # ✅ meta
    meta = {
        "updated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "period_text": period_text,
        "period_start": start7.isoformat(),
        "period_end": end7.isoformat(),
        "total_reviews": int(len(df7)),
        # 상단 키워드(분리)
        "keywords_top5": {
            "pos": [[k, int(c)] for k, c in pos_top],
            "neg": [[k, int(c)] for k, c in neg_top],
        },
        # complaint top (원인 중심)
        "complaint_top5": [[k, int(c)] for k, c in complaint_top],
        # evidence (클릭 이동용: id)
        "keyword_evidence": {
            "pos": pos_evidence,
            "neg": neg_evidence,
            "complaint": complaint_evidence,
        },
        # auto stopwords (디버깅/확인용)
        "auto_stopwords": auto_sw,
        # clusters
        "clusters": cluster_counts,
        "size_phrases": size_phrases,
        "fit_words": ["정사이즈", "한치수 크게", "한치수 작게", "타이트", "넉넉", "기장", "소매", "어깨", "가슴", "발볼", "수납", "가벼움"],
        # 제품별 1y POS/NEG 키워드맵
        "product_keyword_map_1y": product_map_1y,
    }

    # ✅ 출력 reviews.json(최근7일만)
    out_reviews = []
    for r in df7.to_dict(orient="records"):
        # pos 태그 보강(프론트 필터용)
        tags = r.get("tags", [])
        if not isinstance(tags, list):
            tags = []
        if is_positive(r) and "pos" not in tags:
            tags.append("pos")

        out_reviews.append(
            {
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
            }
        )

    # ✅ HTML 템플릿: 너 기존 템플릿을 읽어서 패치
    html = load_html_template()
    html = patch_html(html)

    # ✅ JS 렌더링 패치(키워드 섹션 + 제품맵 + 클릭 이동 + 카드 anchor + 랭킹 키워드)
    # - 기존 HTML/JS 구조를 유지하면서 필요한 함수만 "치환"하는 방식
    # - 너가 쓰던 id들(topComplaints, mindmap, dailyFeed 등) 그대로 활용

    # 1) Summary: Complaint Top5 대신, keywords_top5(pos/neg) 같이 렌더링하도록 topComplaints 쪽을 교체
    #    (기존 topComplaints id 그대로 사용)
    html = re.sub(
        r"const top = META\?\.\s*complaint_top5\s*\|\|\s*\[\];\s*const el = document\.getElementById\(\"topComplaints\"\);\s*el\.innerHTML = .*?;\s*",
        """
      // ✅ 1-2) KEYWORDS TOP 5 (POS/NEG 분리)
      const el = document.getElementById("topComplaints");
      const pos = (META?.keywords_top5?.pos || []);
      const neg = (META?.keywords_top5?.neg || []);
      const ev = (META?.keyword_evidence || {});
      const mkChip = (k,c,mode) => {
        const cls = (mode==="pos") ? "badge pos" : "badge neg";
        const evi = (mode==="pos") ? (ev.pos?.[k]||[]) : (ev.neg?.[k]||[]);
        const targetId = (evi[0]?.id ?? null);
        const onclick = (targetId!=null) ? `onclick="scrollToReview('${targetId}')"` : "";
        return `<span class="${cls}" style="cursor:pointer" ${onclick}>#${esc(k)} <span class="opacity-70">${esc(c)}</span></span>`;
      };

      const posHtml = pos.map(([k,c]) => mkChip(k,c,"pos")).join("");
      const negHtml = neg.map(([k,c]) => mkChip(k,c,"neg")).join("");

      el.innerHTML = `
        <div class="text-[11px] font-black text-slate-500 mb-2">NEG(불만)</div>
        <div class="flex flex-wrap gap-2 mb-4">${negHtml || '<span class="text-xs font-bold text-slate-400">-</span>'}</div>
        <div class="text-[11px] font-black text-slate-500 mb-2">POS(긍정)</div>
        <div class="flex flex-wrap gap-2">${posHtml || '<span class="text-xs font-bold text-slate-400">-</span>'}</div>
      `;
        """,
        html,
        flags=re.S,
    )

    # 2) Daily feed 카드에 anchor id 부여(rev-{id})
    html = html.replace(
        '<div class="review-card">',
        '<div class="review-card" id="rev-${esc(r.id)}">',
    )

    # 3) 4번 섹션(mindmap) 렌더링을 제품별 1y 키워드맵으로 교체
    #    기존 renderMindmap() 함수 내용을 치환
    html = re.sub(
        r"function renderMindmap\(\)\{.*?\}\n\n",
        r"""
    function renderMindmap(){
      // ✅ 제품별(최근 1년) POS/NEG 키워드맵
      const items = META?.product_keyword_map_1y || [];
      const root = document.getElementById("mindmap");
      if(!items.length){
        root.innerHTML = `<div class="summary-card lg:col-span-3"><div class="text-sm font-black text-slate-700">제품별 키워드맵 데이터가 없습니다.</div></div>`;
        return;
      }

      const ev = META?.keyword_evidence || {};

      const chip = (mode, k, c, eviList) => {
        const cls = mode==="pos" ? "badge pos" : "badge neg";
        const targetId = (eviList && eviList[0] && eviList[0].id!=null) ? eviList[0].id : null;
        const onclick = targetId!=null ? `onclick="scrollToReview('${targetId}')"` : "";
        return `<span class="${cls}" style="cursor:pointer" ${onclick}>#${esc(k)} <span class="opacity-70">${esc(c)}</span></span>`;
      };

      root.innerHTML = items.map(p => {
        const img = p.local_product_image
          ? `<img src="${esc(p.local_product_image)}" alt="" class="w-12 h-12 rounded-2xl object-cover border border-white/80" />`
          : `<div class="w-12 h-12 rounded-2xl bg-white/60 border border-white/80 flex items-center justify-center text-[10px] text-slate-400">NO</div>`;

        const pos = p.pos || [];
        const neg = p.neg || [];

        const posE = p.pos_evidence || {};
        const negE = p.neg_evidence || {};

        const posHtml = pos.map(([k,c]) => chip("pos", k, c, posE[k]||[])).join("");
        const negHtml = neg.map(([k,c]) => chip("neg", k, c, negE[k]||[])).join("");

        return `
          <div class="summary-card">
            <div class="flex items-center gap-3">
              ${img}
              <div class="min-w-0">
                <div class="font-black text-slate-900 line-clamp-1">${esc(p.product_name || p.product_code)}</div>
                <div class="text-xs font-bold text-slate-500 mt-1">code: ${esc(p.product_code)} · 1y reviews: ${esc(p.reviews_1y)}</div>
              </div>
            </div>

            <div class="mt-4">
              <div class="text-[11px] font-black text-slate-500 mb-2">POS</div>
              <div class="flex flex-wrap gap-2">${posHtml || '<span class="text-xs font-bold text-slate-400">-</span>'}</div>
            </div>

            <div class="mt-4">
              <div class="text-[11px] font-black text-slate-500 mb-2">NEG</div>
              <div class="flex flex-wrap gap-2">${negHtml || '<span class="text-xs font-bold text-slate-400">-</span>'}</div>
            </div>
          </div>
        `;
      }).join("");
    }
        """,
        html,
        flags=re.S,
    )

    # 4) 랭킹의 kwds(주요 키워드)도 동일 토크나이저 기반으로 meta에서 미리 산출하려면 확장 가능하지만,
    #    현재는 JS에서 뽑는 방식 유지(다만 "중립어" 제거가 필요)
    #    → JS쪽 issueKwds 필터를 강화 (구매/제품/같은/기존 등 추가 제거)
    html = html.replace(
        'if (["제품","상품","구매","배송","택배","사이즈"].includes(k)) continue;',
        'if (["제품","상품","구매","구입","주문","배송","택배","사이즈","기존","같은","원래","이번"].includes(k)) continue;'
    )

    # 5) 키워드 섹션 안내문 업데이트(컴플레인→키워드 분리)
    html = html.replace(
        "* 최근 7일 고정 + 불만리뷰 기반 키워드",
        "* 최근 7일 리뷰 기반(중립/관용 제거 + POS/NEG 분리 + 클러스터링)"
    )

    # write outputs
    (SITE_DATA_DIR / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    (SITE_DATA_DIR / "reviews.json").write_text(json.dumps({"reviews": out_reviews}, ensure_ascii=False, indent=2), encoding="utf-8")
    (SITE_DIR / "index.html").write_text(html, encoding="utf-8")

    print("[OK] Build done")
    print(f"- Input: {inp}")
    print(f"- Output meta: {SITE_DATA_DIR / 'meta.json'}")
    print(f"- Output reviews: {SITE_DATA_DIR / 'reviews.json'}")
    print(f"- Output html: {SITE_DIR / 'index.html'}")
    print(f"- Period: {period_text}")
    print(f"- POS Top5: {pos_top}")
    print(f"- NEG Top5: {neg_top}")
    print(f"- Complaint Top5: {complaint_top}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="path to reviews.json (your aggregated JSON)")
    args = ap.parse_args()
    main(args.input)
