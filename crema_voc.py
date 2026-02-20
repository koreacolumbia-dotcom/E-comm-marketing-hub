#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
[V5 | BUILD VOC DASHBOARD FROM JSON | MATCHES YOUR index.html CONTRACT]
- Input : reviews.json  ({"reviews":[...]}), created_at ISO 권장
- Output:
  - site/data/reviews.json  (최근 7일로 필터된 리뷰만 + per-review pos_keywords/neg_keywords 포함)
  - site/data/meta.json     (✅ index.html이 기대하는 키들로 생성)
  - site/index.html         (템플릿 복사: site/template.html 우선, 없으면 기존 site/index.html 유지)

✅ V5 핵심 (이번에 "데이터 안보임" 해결)
1) meta.json 키를 너가 준 index.html이 기대하는 형태로 맞춤
   - pos_top5 / neg_top5 / pos_examples
   - product_mindmap_1y (renderProductMindmap이 이 키를 봄)
2) reviews.json 각 리뷰에 pos_keywords / neg_keywords 추가
   - 랭킹의 "주요 키워드"가 이제 실제로 채워짐
3) 클릭 이동 ID 계약 통일
   - top5 / mindmap에 rid를 review id로 넣음 → HTML이 review-<id>로 스크롤
4) Auto stopwords / POS-NEG seed 충돌 완화

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

# Auto stopwords learning
AUTO_STOP_MIN_DF = 0.35               # 문서 35% 이상 등장
AUTO_STOP_MIN_PRODUCTS = 0.30         # 제품 30% 이상 분산
AUTO_STOP_POLARITY_MARGIN = 0.12      # pos/neg 비율이 0.5 근처면(중립) 제거
AUTO_STOP_MAX_ADD = 120

# keyword output
TOPK_POS = 5
TOPK_NEG = 5
TOPK_COMPLAINT = 5

# per-review keywords for ranking
PER_REVIEW_KW_MAX = 6

# clusters
CLUSTERS = {
    "size": ["사이즈", "정사이즈", "작", "크", "타이트", "헐렁", "끼", "기장", "소매", "어깨", "가슴", "발볼", "핏"],
    "quality": ["품질", "불량", "하자", "찢", "구멍", "실밥", "오염", "변색", "냄새", "마감", "내구", "퀄리티"],
    "shipping": ["배송", "택배", "출고", "도착", "지연", "늦", "빠르", "포장", "파손"],
    "cs": ["문의", "응대", "고객", "cs", "교환", "반품", "환불", "처리", "as"],
    "price": ["가격", "비싸", "싸", "가성비", "할인", "쿠폰", "대비"],
    "design": ["디자인", "색", "컬러", "예쁘", "멋", "스타일", "핏감"],
    "function": ["수납", "넣", "포켓", "공간", "가볍", "무게", "따뜻", "보온", "방수", "기능", "편하", "착용감", "그립"],
}

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
BASE_STOPWORDS = set(
    """
그리고 그러나 그래서 하지만 또한
너무 정말 완전 진짜 매우 그냥 조금 약간
저는 제가 우리는 너희 이거 그거 저거
있습니다 입니다 같아요 같네요 하는 하다 됐어요 되었어요 되네요
구매 구입 주문 샀어요 샀습니다 주문했 구매했 구입해 구매해
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
기존 같은 동일 비슷 비슷한 원래 이번
그냥그냥
"""
    .split()
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
REQ_KEYWORDS = ["개선", "아쉬", "불편", "했으면", "추가", "보완", "수정", "필요", "요청", "교환", "반품", "환불"]
COMPLAINT_HINTS = ["불량", "하자", "찢", "구멍", "냄새", "변색", "오염", "실밥", "품질", "엉망", "별로", "최악", "실망", "문제", "불편"]

POS_SEEDS = ["가볍", "무게", "편하", "편안", "착용감", "수납", "포켓", "공간", "따뜻", "보온", "방수", "튼튼", "견고", "퀄리티", "만족", "예쁘", "멋", "좋", "잘맞", "딱", "깔끔", "마감", "쿠션"]
NEG_SEEDS = ["불량", "하자", "찢", "구멍", "냄새", "변색", "오염", "실밥", "최악", "별로", "실망", "불편", "문제", "아쉽", "지연", "늦", "파손", "응대", "환불", "교환", "반품", "작", "크", "타이트", "헐렁"]


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
    rating = int(pd.to_numeric(row.get("rating"), errors="coerce") or 0)

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

    prod_all = set(str(r.get("product_code") or "-") for r in rows)
    prod_n = max(1, len(prod_all))
    all_ids = set(str(r.get("id")) for r in rows)
    all_n = max(1, len(all_ids))

    # df + prod_df + pos/neg freq
    token_docs: Dict[str, set] = {}
    token_prods: Dict[str, set] = {}
    token_pos: Dict[str, int] = {}
    token_neg: Dict[str, int] = {}

    for r in rows:
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

    auto_global: List[str] = []
    for t, ds in sorted(token_docs.items(), key=lambda x: len(x[1]), reverse=True):
        df_ratio = len(ds) / all_n
        prod_ratio = len(token_prods.get(t, set())) / prod_n
        if df_ratio < AUTO_STOP_MIN_DF:
            continue
        if prod_ratio < AUTO_STOP_MIN_PRODUCTS:
            continue

        p = token_pos.get(t, 0)
        n = token_neg.get(t, 0)
        total = p + n
        if total < 8:
            continue
        neg_ratio = n / total
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

        ids = [str(r.get("id")) for r in group]
        n_docs = max(1, len(ids))
        prod_set = set(str(r.get("product_code") or "-") for r in group)
        n_prod = max(1, len(prod_set))

        td: Dict[str, set] = {}
        tp: Dict[str, set] = {}
        pcount: Dict[str, int] = {}
        ncount: Dict[str, int] = {}

        for r in group:
            pid = str(r.get("id"))
            pcode = str(r.get("product_code") or "-")
            ispos = is_positive(r)
            isneg = is_complaint(r)

            toks_unique = set(tokenize_ko(str(r.get("text") or ""), stopwords=BASE_STOPWORDS))
            toks_all = tokenize_ko(str(r.get("text") or ""), stopwords=BASE_STOPWORDS)

            for t in toks_unique:
                td.setdefault(t, set()).add(pid)
                tp.setdefault(t, set()).add(pcode)
            for t in toks_all:
                if ispos:
                    pcount[t] = pcount.get(t, 0) + 1
                if isneg:
                    ncount[t] = ncount.get(t, 0) + 1

        cand: List[str] = []
        for t, ds in td.items():
            df_ratio = len(ds) / n_docs
            prod_ratio = len(tp.get(t, set())) / n_prod
            if df_ratio < 0.38 or prod_ratio < 0.32:
                continue
            p = pcount.get(t, 0)
            n = ncount.get(t, 0)
            total = p + n
            if total < 8:
                continue
            neg_ratio = n / total
            if abs(neg_ratio - 0.5) <= 0.14:
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
# Keyword extraction
# ----------------------------
def _seed_match(token: str, seed: str) -> bool:
    # substring 양방향(“수납” vs “수납력”)
    return (seed in token) or (token in seed)


def top_terms(rows: List[Dict[str, Any]], topk: int, auto_sw: Dict[str, Any], which: str) -> List[Tuple[str, int]]:
    """
    which: "pos" | "neg" | "complaint"
    - pos: is_positive
    - neg: is_complaint
    - complaint: is_complaint + 원인/클러스터 가중 + POS seed 계열 제거 강화
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
        tnorm = normalize_text(text).replace(" ", "")

        if which == "complaint":
            for t in toks:
                # POS seed 냄새나는 건 complaint에서 제거
                if any(_seed_match(t, s) for s in POS_SEEDS):
                    continue
                w = 1
                if any(_seed_match(t, s) for s in NEG_SEEDS):
                    w += 2
                if any(any(k.replace(" ", "") in tnorm for k in kws) for kws in CLUSTERS.values()):
                    w += 2
                freq[t] = freq.get(t, 0) + w
        elif which == "pos":
            for t in toks:
                if any(_seed_match(t, s) for s in NEG_SEEDS):
                    continue
                freq[t] = freq.get(t, 0) + 1
        else:  # neg
            for t in toks:
                if any(_seed_match(t, s) for s in POS_SEEDS):
                    continue
                freq[t] = freq.get(t, 0) + 1

    return sorted(freq.items(), key=lambda x: x[1], reverse=True)[:topk]


def assign_cluster(text: str) -> List[str]:
    tnorm = normalize_text(text).replace(" ", "")
    hits: List[str] = []
    for c, kws in CLUSTERS.items():
        for k in kws:
            if k.replace(" ", "") in tnorm:
                hits.append(c)
                break
    return hits


# ----------------------------
# Evidence builders (for your HTML click→review)
# ----------------------------
def build_top5_with_rid(rows: List[Dict[str, Any]], terms: List[Tuple[str, int]], auto_sw: Dict[str, Any], which: str) -> List[List[Any]]:
    """
    Return [[keyword, count, rid], ...]
    rid = 해당 keyword가 포함된 최근 리뷰의 id
    """
    keys = [k for k, _ in terms]
    rid_map: Dict[str, Any] = {k: None for k in keys}

    # 최신순으로 스캔해서 첫 매칭 id 채우기
    def _dt(r):
        return parse_created_at_iso(str(r.get("created_at") or ""))

    for r in sorted(rows, key=_dt, reverse=True):
        if which == "pos" and not is_positive(r):
            continue
        if which in ("neg", "complaint") and not is_complaint(r):
            continue

        sw = build_stopwords_for_row(r, auto_sw)
        toks = set(tokenize_ko(str(r.get("text") or ""), stopwords=sw))

        for k in keys:
            if rid_map.get(k) is None and k in toks:
                rid_map[k] = r.get("id")

        if all(rid_map.get(k) is not None for k in keys):
            break

    out: List[List[Any]] = []
    for k, c in terms:
        out.append([k, int(c), rid_map.get(k)])
    return out


def build_pos_examples(rows: List[Dict[str, Any]], pos_keys: List[str], auto_sw: Dict[str, Any], per_kw: int = 1) -> Dict[str, List[Dict[str, Any]]]:
    """
    HTML renderSummary에서 title tooltip로 쓰는 pos_examples[k][0].snip 기대
    """
    out: Dict[str, List[Dict[str, Any]]] = {k: [] for k in pos_keys}

    def _dt(r):
        return parse_created_at_iso(str(r.get("created_at") or ""))

    for r in sorted(rows, key=_dt, reverse=True):
        if not is_positive(r):
            continue
        sw = build_stopwords_for_row(r, auto_sw)
        toks = set(tokenize_ko(str(r.get("text") or ""), stopwords=sw))
        for k in pos_keys:
            if k in toks and len(out[k]) < per_kw:
                snip = re.sub(r"\s+", " ", str(r.get("text") or "").strip())
                snip = snip[:140] + ("…" if len(snip) > 140 else "")
                out[k].append({"id": r.get("id"), "snip": snip})

        if all(len(out[k]) >= per_kw for k in pos_keys):
            break

    return out


# ----------------------------
# Per-review keywords (for Ranking "주요 키워드")
# ----------------------------
def per_review_keywords(row: Dict[str, Any], auto_sw: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    sw = build_stopwords_for_row(row, auto_sw)
    toks = tokenize_ko(str(row.get("text") or ""), stopwords=sw)
    if not toks:
        return [], []

    # 중립/의미낮은 토큰 2차 컷
    hard_block = {"제품", "상품", "구매", "구입", "주문", "배송", "택배", "기존", "같은", "원래", "이번", "사이즈"}
    toks = [t for t in toks if t not in hard_block]

    # 빈도 기반이 아니라 “리뷰 1건에서 핵심 토큰”만 뽑는 용도 → unique preserve order
    seen = set()
    uniq = []
    for t in toks:
        if t in seen:
            continue
        seen.add(t)
        uniq.append(t)
        if len(uniq) >= 20:
            break

    if is_positive(row):
        pos = [t for t in uniq if not any(_seed_match(t, s) for s in NEG_SEEDS)]
        return pos[:PER_REVIEW_KW_MAX], []
    if is_complaint(row):
        neg = [t for t in uniq if not any(_seed_match(t, s) for s in POS_SEEDS)]
        return [], neg[:PER_REVIEW_KW_MAX]
    return [], []


# ----------------------------
# Product mindmap (1y) for your HTML renderProductMindmap()
# Expected by your index.html:
# META.product_mindmap_1y = [
#   { product_code, product_name, local_product_image, review_cnt_1y,
#     pos_keywords:[{k,c,rid}], neg_keywords:[{k,c,rid}] }
# ]
# ----------------------------
def build_product_mindmap_1y(rows_1y: List[Dict[str, Any]], rows_7d: List[Dict[str, Any]], auto_sw_7d: Dict[str, Any], per_side_topk: int = 6) -> List[Dict[str, Any]]:
    by_prod: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows_1y:
        code = str(r.get("product_code") or "-")
        by_prod.setdefault(code, []).append(r)

    # last7 index (rid 연결용)
    last7_by_prod: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows_7d:
        code = str(r.get("product_code") or "-")
        last7_by_prod.setdefault(code, []).append(r)

    out: List[Dict[str, Any]] = []
    for code, group in by_prod.items():
        sample = group[0]
        pname = str(sample.get("product_name") or code)
        img = str(sample.get("local_product_image") or "")

        pos_terms = top_terms(group, per_side_topk, auto_sw_7d, which="pos")
        neg_terms = top_terms(group, per_side_topk, auto_sw_7d, which="neg")

        # rid 연결: last7 안에서만 (없으면 null)
        l7 = last7_by_prod.get(code, [])

        pos_with_rid = build_top5_with_rid(l7, pos_terms, auto_sw_7d, which="pos")
        neg_with_rid = build_top5_with_rid(l7, neg_terms, auto_sw_7d, which="neg")

        pos_keywords = [{"k": k, "c": int(c), "rid": rid} for k, c, rid in pos_with_rid]
        neg_keywords = [{"k": k, "c": int(c), "rid": rid} for k, c, rid in neg_with_rid]

        out.append({
            "product_code": code,
            "product_name": pname,
            "local_product_image": img,
            "review_cnt_1y": len(group),
            "pos_keywords": pos_keywords,
            "neg_keywords": neg_keywords,
        })

    out.sort(key=lambda x: x.get("review_cnt_1y", 0), reverse=True)
    return out[:24]


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
        dt = datetime.fromisoformat(s)
    except Exception:
        dt2 = pd.to_datetime(s, errors="coerce")
        if pd.isna(dt2):
            return datetime(1970, 1, 1, tzinfo=tz.gettz(OUTPUT_TZ))
        dt = dt2.to_pydatetime()

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz.gettz(OUTPUT_TZ))
    return dt.astimezone(tz.gettz(OUTPUT_TZ))


def in_date_range_kst(dt: datetime, start_d, end_d) -> bool:
    d = dt.astimezone(tz.gettz(OUTPUT_TZ)).date()
    return start_d <= d <= end_d


# ----------------------------
# HTML template handling
# ----------------------------
def maybe_write_index_html(template_path: Optional[str], write_html: bool) -> None:
    """
    - write_html=False: site/index.html이 없으면 template.html만 복사(또는 스킵)
    - write_html=True : template_path(있으면) -> site/index.html로 복사
    """
    idx = SITE_DIR / "index.html"
    tpl = None

    if template_path:
        p = pathlib.Path(template_path).expanduser().resolve()
        if p.exists():
            tpl = p
    else:
        p1 = SITE_DIR / "template.html"
        if p1.exists():
            tpl = p1

    if not write_html:
        # index가 이미 있으면 건드리지 않음
        if idx.exists():
            return
        # 없으면 템플릿 있으면 만들어줌
        if tpl and tpl.exists():
            idx.write_text(tpl.read_text(encoding="utf-8"), encoding="utf-8")
        return

    # write_html=True
    if tpl and tpl.exists():
        idx.write_text(tpl.read_text(encoding="utf-8"), encoding="utf-8")
    else:
        # 템플릿이 없으면 "기존 index.html 유지" (없으면 경고)
        if not idx.exists():
            raise FileNotFoundError("site/index.html도 없고 site/template.html도 없습니다. 템플릿을 제공해 주세요.")


# ----------------------------
# Main
# ----------------------------
def main(input_path: str, template: Optional[str], write_html: bool):
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

    # rating 정규화
    df7 = pd.DataFrame(rows7).copy()
    df7["rating"] = pd.to_numeric(df7.get("rating"), errors="coerce").fillna(0).astype(int)

    rows7 = df7.to_dict(orient="records")

    # auto stopwords
    auto_sw = learn_auto_stopwords(rows7)

    # top terms
    pos_terms = top_terms(rows7, TOPK_POS, auto_sw, which="pos")
    neg_terms = top_terms(rows7, TOPK_NEG, auto_sw, which="neg")
    complaint_terms = top_terms(rows7, TOPK_COMPLAINT, auto_sw, which="complaint")

    # index.html이 기대하는 키들
    pos_top5 = build_top5_with_rid(rows7, pos_terms, auto_sw, which="pos")     # [[k,c,rid],...]
    neg_top5 = build_top5_with_rid(rows7, neg_terms, auto_sw, which="neg")     # [[k,c,rid],...]
    pos_keys = [k for k, _, _ in pos_top5]
    pos_examples = build_pos_examples(rows7, pos_keys, auto_sw, per_kw=1)

    # clusters (complaint rows만)
    cluster_counts: Dict[str, int] = {k: 0 for k in CLUSTERS.keys()}
    for r in rows7:
        if not is_complaint(r):
            continue
        for h in assign_cluster(str(r.get("text") or "")):
            cluster_counts[h] = cluster_counts.get(h, 0) + 1

    # size phrases (size 태그 리뷰에서 반복 토큰)
    size_rows = [r for r in rows7 if isinstance(r.get("tags"), list) and ("size" in r.get("tags"))]
    size_phrase_terms = top_terms(size_rows, topk=10, auto_sw=auto_sw, which="neg")
    size_phrases = [k for k, _ in size_phrase_terms]

    # product mindmap 1y (HTML contract)
    product_mindmap_1y = build_product_mindmap_1y(rows1y, rows7, auto_sw, per_side_topk=6)

    period_text = f"최근 7일 ({start7.isoformat()} ~ {end7.isoformat()})"

    # output reviews (최근7일만) + per-review pos_keywords/neg_keywords + pos tag
    out_reviews: List[Dict[str, Any]] = []
    for r in rows7:
        tags = r.get("tags", [])
        if not isinstance(tags, list):
            tags = []
        if is_positive(r) and "pos" not in tags:
            tags.append("pos")

        pk, nk = per_review_keywords(r, auto_sw)

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

            # ✅ JS 랭킹/키워드용 (calcMetrics에서 사용)
            "pos_keywords": pk,    # List[str]
            "neg_keywords": nk,    # List[str]
        })

    meta = {
        "updated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "period_text": period_text,
        "period_start": start7.isoformat(),
        "period_end": end7.isoformat(),
        "total_reviews": len(out_reviews),

        # ✅ 너 index.html(renderSummary)이 기대하는 키들
        "pos_top5": pos_top5,           # [[k,c,rid],...]
        "neg_top5": neg_top5,           # [[k,c,rid],...]
        "pos_examples": pos_examples,   # {k:[{id,snip}]}

        # 참고용(원하면 UI에 확장 가능)
        "complaint_top5": [[k, int(c)] for k, c in complaint_terms],
        "auto_stopwords": auto_sw,
        "clusters": cluster_counts,
        "size_phrases": size_phrases,
        "fit_words": ["정사이즈", "한치수 크게", "한치수 작게", "타이트", "넉넉", "기장", "소매", "어깨", "가슴", "발볼", "수납", "가벼움"],

        # ✅ 너 index.html(renderProductMindmap)이 기대하는 키
        "product_mindmap_1y": product_mindmap_1y,
    }

    # write outputs
    (SITE_DATA_DIR / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    (SITE_DATA_DIR / "reviews.json").write_text(json.dumps({"reviews": out_reviews}, ensure_ascii=False, indent=2), encoding="utf-8")

    # index.html handling (template copy)
    maybe_write_index_html(template_path=template, write_html=write_html)

    print("[OK] V5 build done")
    print(f"- Input: {inp}")
    print(f"- Output meta: {SITE_DATA_DIR / 'meta.json'}")
    print(f"- Output reviews: {SITE_DATA_DIR / 'reviews.json'}")
    print(f"- Output html: {SITE_DIR / 'index.html'} (copied if template provided / or kept existing)")
    print(f"- Period: {period_text}")
    print(f"- POS Top5: {pos_top5}")
    print(f"- NEG Top5: {neg_top5}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="path to reviews.json (aggregated JSON)")
    ap.add_argument("--template", default="", help="optional path to index.html template (if you want to overwrite/copy)")
    ap.add_argument("--write-html", action="store_true", help="if set: copy template -> site/index.html (or require existing index)")
    args = ap.parse_args()

    tpl = args.template.strip() or None
    main(args.input, template=tpl, write_html=bool(args.write_html))
