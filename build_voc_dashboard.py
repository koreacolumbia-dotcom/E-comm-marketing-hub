#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
[BUILD VOC DASHBOARD FROM JSON | v6.1 MAX PATCH | ACCUMULATIVE + ML/DL TREND]
- Input: aggregated reviews.json ({"reviews":[...]}), created_at ISO recommended
- Output:
  - site/data/reviews.json  (filtered to last N days by default)
  - site/data/meta.json     (period/keywords/evidence/clusters/mindmap + HEALTHCHECK + ML/DL)
  - site/index.html         (template priority: --html-template > site/template.html > DEFAULT)

Adds on top of v6.0:
1) Robust created_at parsing (supports trailing 'Z')
2) 1Y trend series in meta (trend_daily_1y)
3) Fast ML topics (TF-IDF + NMF) in meta (ml_topics_1y) if sklearn installed
4) Optional DL semantic clustering in meta (dl_topics_1y) if sentence-transformers installed
5) Debug mode flag (--debug) to keep extra diagnostics in meta
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd
from dateutil import tz

# ML (fast)
try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.decomposition import NMF
    from sklearn.cluster import MiniBatchKMeans
except Exception:
    TfidfVectorizer = None
    NMF = None
    MiniBatchKMeans = None

# DL (optional)
try:
    from sentence_transformers import SentenceTransformer
except Exception:
    SentenceTransformer = None


OUTPUT_TZ = os.getenv("OUTPUT_TZ", "Asia/Seoul").strip()
DEFAULT_TARGET_DAYS = int(os.getenv("TARGET_DAYS", "7") or "7")  # last N days including today

AUTO_STOP_MIN_DF = 0.30
AUTO_STOP_MIN_PRODUCTS = 0.25
AUTO_STOP_POLARITY_MARGIN = 0.20
AUTO_STOP_MAX_ADD = 120

TOPK_POS = 5
TOPK_NEG = 5

MINDMAP_SENT_PER_SIDE = 2
MINDMAP_MAX_PRODUCTS = 24

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
    ("shoe", re.compile(r"(shoe|boot|chukka|sneaker|wide|waterproof|신발|부츠|워커)", re.I)),
    ("top", re.compile(r"(fleece|jacket|hood|tee|turtle|shirt|상의|자켓|플리스|후드|티|터틀)", re.I)),
    ("bottom", re.compile(r"(pant|short|skirt|하의|바지|팬츠|쇼츠)", re.I)),
    ("glove", re.compile(r"(glove|장갑)", re.I)),
]
DEFAULT_CATEGORY = "other"


def now_kst() -> datetime:
    return datetime.now(timezone.utc).astimezone(tz.gettz(OUTPUT_TZ))


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


SIZE_KEYWORDS = ["사이즈", "정사이즈", "작아요", "작다", "커요", "크다", "핏", "타이트", "여유", "끼", "기장", "소매", "어깨", "가슴", "발볼", "헐렁", "오버", "업", "다운", "한치수", "반치수"]
REQ_WEAK = ["개선", "아쉬", "했으면", "보완", "수정", "필요", "요청"]
REQ_STRONG = ["교환", "반품", "환불", "as", "처리", "재배송", "재발송"]
COMPLAINT_HINTS = ["불량", "하자", "찢", "구멍", "냄새", "변색", "오염", "실밥", "품질", "엉망", "별로", "최악", "실망", "문제", "불편", "파손", "지연", "늦", "안와", "안옴"]

POS_SEEDS = [
    "가볍", "편하", "편안", "착용감", "수납", "포켓", "공간", "주머니", "넣",
    "따뜻", "보온", "방수", "튼튼", "견고", "만족", "예쁘", "멋", "깔끔", "마감",
    "잘맞", "딱", "쿠션", "추천"
]
NEG_SEEDS = [
    "불량", "하자", "찢", "구멍", "냄새", "변색", "오염", "실밥", "최악", "별로", "실망",
    "불편", "문제", "아쉽", "무겁",
    "작", "크", "타이트", "헐렁",
    "지연", "늦", "파손", "응대", "환불", "교환", "반품", "as"
]

HARD_DEFECT = ["불량", "하자", "찢", "구멍", "파손", "누수", "변색", "오염", "접착", "터짐"]
DEFECT_ACTION = ["교환", "반품", "환불", "as", "처리", "불편", "문제", "실망", "문의", "응대", "접수"]
NEGATION = ["안", "않", "없", "못", "아니", "별로안", "전혀안"]


def has_any_kw(text: str, kws: List[str]) -> bool:
    t = (text or "").replace(" ", "")
    for kw in kws:
        if kw.replace(" ", "") in t:
            return True
    return False


def classify_size_direction(text: str) -> str:
    t = (text or "").replace(" ", "")
    small_kw = ["작아", "작다", "타이트", "끼", "조인다", "짧다", "좁다", "발볼좁", "어깨좁", "가슴좁", "다운", "한치수작", "반치수작"]
    big_kw = ["커", "크다", "넉넉", "오버", "길다", "넓다", "헐렁", "부해", "업", "한치수큰", "반치수큰"]
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
    rating = int(row.get("rating") or 0)
    tags = row.get("tags") or []
    text = str(row.get("text") or "")

    if rating <= 2:
        return True

    t = normalize_text(text)

    if rating >= 4:
        if has_any_kw(text, HARD_DEFECT) and has_any_kw(text, DEFECT_ACTION):
            return True
        severe = any(x in t for x in ["환불", "반품", "교환", "as", "불량", "하자", "파손", "지연", "늦", "응대별로", "처리안"])
        if severe and has_any_kw(text, COMPLAINT_HINTS):
            return True
        return False

    pos_s, neg_s = review_polarity_scores(text)
    has_req = isinstance(tags, list) and ("req" in tags)
    hint = has_any_kw(text, COMPLAINT_HINTS)

    if hint and neg_s >= 1:
        return True
    if neg_s >= 2 and neg_s > pos_s:
        return True
    if has_req and has_any_kw(text, REQ_STRONG) and neg_s >= 1:
        return True
    if has_req and neg_s >= 1 and neg_s > pos_s:
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
        pos_s, neg_s = review_polarity_scores(text)
        return (pos_s >= 2) and (pos_s >= neg_s + 1)

    return False


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


def top_terms(rows: List[Dict[str, Any]], topk: int, auto_sw: Dict[str, Any], which: str) -> List[Tuple[str, int]]:
    freq: Dict[str, int] = {}
    for r in rows:
        if which == "pos" and not is_positive(r):
            continue
        if which == "neg" and not is_complaint(r):
            continue

        sw = build_stopwords_for_row(r, auto_sw)
        toks = tokenize_ko(str(r.get("text") or ""), stopwords=sw)

        for t in toks:
            if which == "pos" and t in NEG_SEEDS:
                continue
            if which == "neg" and t in POS_SEEDS:
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
            for s in split_sentences(str(r.get("text") or "")):
                sc = score_sentence(s, "pos")
                if sc < 2:
                    continue
                pos_cands.append((sc, {"text": s, "id": r.get("id"), "created_at": r.get("created_at"), "rating": int(r.get("rating") or 0)}))

        neg_cands: List[Tuple[int, Dict[str, Any]]] = []
        for r in (prod_rows_7d + prod_rows_1y):
            if not is_complaint(r):
                continue
            for s in split_sentences(str(r.get("text") or "")):
                sc = score_sentence(s, "neg")
                if sc < 2:
                    continue
                neg_cands.append((sc, {"text": s, "id": r.get("id"), "created_at": r.get("created_at"), "rating": int(r.get("rating") or 0)}))

        pos_cands.sort(key=lambda x: (x[0], str(x[1].get("created_at") or "")), reverse=True)
        neg_cands.sort(key=lambda x: (x[0], str(x[1].get("created_at") or "")), reverse=True)

        def pick_unique(cands: List[Tuple[int, Dict[str, Any]]], k: int) -> List[Dict[str, Any]]:
            seen = set()
            picked = []
            for _, item in cands:
                key = re.sub(r"\s+", " ", str(item.get("text") or "")).strip()
                if key in seen:
                    continue
                seen.add(key)
                picked.append(item)
                if len(picked) >= k:
                    break
            return picked

        out.append({
            "product_code": code,
            "product_name": pname,
            "local_product_image": img,
            "reviews_1y": review_cnt_1y,
            "pos_sentences": pick_unique(pos_cands, per_side),
            "neg_sentences": pick_unique(neg_cands, per_side),
        })

    out.sort(key=lambda x: x.get("reviews_1y", 0), reverse=True)
    return out[:max_products]


REQUIRED_FIELDS = ["id", "product_code", "product_name", "rating", "created_at", "text", "source"]


def read_reviews_json(path: pathlib.Path) -> List[Dict[str, Any]]:
    """
    입력을 그대로 쓰면 타입/필드 누락으로 downstream이 깨질 수 있어서,
    최소 정규화만 수행 (축약 아님: 안정화)
    """
    obj = json.loads(path.read_text(encoding="utf-8"))
    reviews = obj.get("reviews")
    if not isinstance(reviews, list):
        raise ValueError('입력 JSON은 {"reviews": [...]} 형태여야 합니다.')

    out: List[Dict[str, Any]] = []
    for r in reviews:
        if not isinstance(r, dict):
            continue
        rr = dict(r)

        rr["id"] = rr.get("id")
        rr["product_code"] = str(rr.get("product_code") or "").strip() or "-"
        rr["product_name"] = str(rr.get("product_name") or "").strip() or rr["product_code"]
        rr["rating"] = int(pd.to_numeric(rr.get("rating"), errors="coerce") or 0)
        rr["created_at"] = str(rr.get("created_at") or "").strip()
        rr["text"] = str(rr.get("text") or "").strip()
        rr["source"] = str(rr.get("source") or "Official").strip() or "Official"

        rr["product_url"] = str(rr.get("product_url") or "").strip()
        rr["option_size"] = str(rr.get("option_size") or "").strip()
        rr["option_color"] = str(rr.get("option_color") or "").strip()
        rr["local_product_image"] = str(rr.get("local_product_image") or "").strip()
        rr["local_review_thumb"] = str(rr.get("local_review_thumb") or "").strip()
        rr["text_image_path"] = str(rr.get("text_image_path") or "").strip()

        out.append(ensure_tags_and_direction(rr))
    return out


def parse_created_at_iso(s: str) -> Optional[datetime]:
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
        dt = pd.to_datetime(s, errors="coerce")
        if pd.isna(dt):
            return None
        dt = dt.to_pydatetime()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz.gettz(OUTPUT_TZ))
    return dt.astimezone(tz.gettz(OUTPUT_TZ))


def in_date_range_kst(dt: datetime, start_d: date, end_d: date) -> bool:
    d = dt.astimezone(tz.gettz(OUTPUT_TZ)).date()
    return start_d <= d <= end_d


def healthcheck(all_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(all_rows)
    by_source: Dict[str, int] = {}
    parse_fail = 0
    missing_required: Dict[str, int] = {k: 0 for k in REQUIRED_FIELDS}
    duplicates = 0

    seen_keys = set()
    min_dt: Optional[datetime] = None
    max_dt: Optional[datetime] = None

    last24 = 0
    now = now_kst()
    cutoff24 = now - timedelta(hours=24)

    for r in all_rows:
        src = str(r.get("source") or "Unknown")
        by_source[src] = by_source.get(src, 0) + 1

        for k in REQUIRED_FIELDS:
            if r.get(k) in (None, "", []):
                missing_required[k] += 1

        key = (src, str(r.get("id")))
        if key in seen_keys:
            duplicates += 1
        else:
            seen_keys.add(key)

        dt = parse_created_at_iso(str(r.get("created_at") or ""))
        if dt is None:
            parse_fail += 1
            continue
        if min_dt is None or dt < min_dt:
            min_dt = dt
        if max_dt is None or dt > max_dt:
            max_dt = dt
        if dt >= cutoff24:
            last24 += 1

    return {
        "input_total_reviews": total,
        "input_by_source": by_source,
        "created_at_parse_fail": parse_fail,
        "missing_required_fields": missing_required,
        "duplicate_keys_by_source_id": duplicates,
        "input_date_min": min_dt.isoformat() if min_dt else None,
        "input_date_max": max_dt.isoformat() if max_dt else None,
        "input_last_24h_count": last24,
        "health_status": "ok" if total > 0 and parse_fail < max(1, int(total * 0.05)) else "check",
    }


DEFAULT_HTML_TEMPLATE = """<!doctype html>
<html><head><meta charset="utf-8"><title>VOC Dashboard</title></head>
<body><h1>VOC Dashboard</h1><p>Template missing. Provide site/template.html or --html-template.</p></body></html>
"""


def load_html_template(cli_template: Optional[str]) -> str:
    if cli_template:
        p = pathlib.Path(cli_template).expanduser()
        if p.exists():
            return p.read_text(encoding="utf-8")

    tpl = SITE_DIR / "template.html"
    if tpl.exists():
        return tpl.read_text(encoding="utf-8")

    return DEFAULT_HTML_TEMPLATE


def build_daily_timeseries(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    tmp = []
    for r in rows:
        dt = parse_created_at_iso(str(r.get("created_at") or ""))
        if not dt:
            continue
        d = dt.date().isoformat()
        tmp.append((d, 1, 1 if is_complaint(r) else 0, 1 if is_positive(r) else 0))
    if not tmp:
        return []
    df = pd.DataFrame(tmp, columns=["day", "cnt", "neg", "pos"])
    g = df.groupby("day", as_index=False).sum()
    g["neg_rate"] = (g["neg"] / g["cnt"]).fillna(0.0)
    g["pos_rate"] = (g["pos"] / g["cnt"]).fillna(0.0)
    return g.sort_values("day").to_dict(orient="records")


def ml_topics_tfidf_nmf(rows: List[Dict[str, Any]], n_topics: int = 8, top_words: int = 8, min_df: int = 3) -> Dict[str, Any]:
    if not rows or TfidfVectorizer is None or NMF is None:
        return {"enabled": False, "reason": "sklearn_not_available_or_no_rows"}

    docs: List[str] = []
    days: List[str] = []
    for r in rows:
        text = str(r.get("text") or "").strip()
        if len(text) < 5:
            continue
        dt = parse_created_at_iso(str(r.get("created_at") or ""))
        if not dt:
            continue
        toks = tokenize_ko(text)
        docs.append(" ".join(toks) if toks else "")
        days.append(dt.date().isoformat())

    if len(docs) < max(20, n_topics * 4):
        return {"enabled": False, "reason": f"too_few_docs({len(docs)})"}

    vec = TfidfVectorizer(min_df=min_df, max_df=0.95)
    X = vec.fit_transform(docs)

    nmf = NMF(n_components=n_topics, random_state=42, init="nndsvda", max_iter=400)
    W = nmf.fit_transform(X)
    H = nmf.components_

    vocab = vec.get_feature_names_out()
    topics = []
    for ti in range(n_topics):
        top_idx = H[ti].argsort()[::-1][:top_words]
        words = [vocab[i] for i in top_idx]
        topics.append({"topic_id": ti, "words": words})

    doc_topic = W.argmax(axis=1)
    per_day = defaultdict(lambda: defaultdict(int))
    for d, t in zip(days, doc_topic):
        per_day[d][int(t)] += 1

    daily_topics = []
    for d in sorted(per_day.keys()):
        row = {"day": d}
        row.update({f"t{tid}": int(cnt) for tid, cnt in per_day[d].items()})
        daily_topics.append(row)

    return {"enabled": True, "method": "tfidf_nmf", "n_topics": n_topics, "topics": topics, "topic_daily_volume": daily_topics}


def dl_embeddings_cluster(rows: List[Dict[str, Any]], model_name: str = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2", n_clusters: int = 10) -> Dict[str, Any]:
    if not rows or SentenceTransformer is None or MiniBatchKMeans is None:
        return {"enabled": False, "reason": "sentence_transformers_or_sklearn_not_available_or_no_rows"}

    texts: List[str] = []
    days: List[str] = []
    for r in rows:
        t = str(r.get("text") or "").strip()
        if len(t) < 8:
            continue
        dt = parse_created_at_iso(str(r.get("created_at") or ""))
        if not dt:
            continue
        texts.append(t[:600])
        days.append(dt.date().isoformat())

    if len(texts) < max(60, n_clusters * 6):
        return {"enabled": False, "reason": f"too_few_texts({len(texts)})"}

    model = SentenceTransformer(model_name)
    emb = model.encode(texts, batch_size=64, show_progress_bar=False, normalize_embeddings=True)

    km = MiniBatchKMeans(n_clusters=n_clusters, random_state=42, batch_size=256)
    labels = km.fit_predict(emb)

    by_c = defaultdict(list)
    for txt, d, c in zip(texts, days, labels):
        by_c[int(c)].append((d, txt))

    clusters = []
    for c in sorted(by_c.keys()):
        samples = sorted(by_c[c], key=lambda x: x[0], reverse=True)[:3]
        clusters.append({"cluster_id": c, "samples": [{"day": d, "text": t} for d, t in samples]})

    per_day = defaultdict(lambda: defaultdict(int))
    for d, c in zip(days, labels):
        per_day[d][int(c)] += 1

    daily = []
    for d in sorted(per_day.keys()):
        row = {"day": d}
        row.update({f"c{cid}": int(cnt) for cid, cnt in per_day[d].items()})
        daily.append(row)

    return {"enabled": True, "method": "embeddings_kmeans", "model": model_name, "n_clusters": n_clusters, "clusters": clusters, "cluster_daily_volume": daily}


def main(input_path: str, html_template: Optional[str], target_days: int, debug: bool):
    inp = pathlib.Path(input_path).expanduser().resolve()
    if not inp.exists():
        raise FileNotFoundError(f"input not found: {inp}")

    all_rows = read_reviews_json(inp)
    now = now_kst()

    hc = healthcheck(all_rows)

    target_days = max(1, int(target_days))
    startN = (now - timedelta(days=target_days - 1)).date()
    endN = now.date()

    start1y = (now - timedelta(days=365)).date()
    end1y = now.date()

    rowsN: List[Dict[str, Any]] = []
    rows1y: List[Dict[str, Any]] = []
    parse_fail_rows = 0

    for r in all_rows:
        dt = parse_created_at_iso(str(r.get("created_at") or ""))
        if dt is None:
            parse_fail_rows += 1
            continue
        if in_date_range_kst(dt, startN, endN):
            rowsN.append(r)
        if in_date_range_kst(dt, start1y, end1y):
            rows1y.append(r)

    dfN = pd.DataFrame(rowsN).copy() if rowsN else pd.DataFrame(columns=["id","product_code","product_name","rating","created_at","text","source","tags","option_size","option_color","size_direction"])
    if "rating" in dfN.columns:
        dfN["rating"] = pd.to_numeric(dfN.get("rating"), errors="coerce").fillna(0).astype(int)

    auto_sw = learn_auto_stopwords(rowsN) if rowsN else {"global": [], "by_category": {}}

    pos_top = top_terms(rowsN, TOPK_POS, auto_sw, which="pos") if rowsN else []
    neg_top = top_terms(rowsN, TOPK_NEG, auto_sw, which="neg") if rowsN else []
    pos_keys = [k for k, _ in pos_top]
    neg_keys = [k for k, _ in neg_top]

    pos_evidence = build_keyword_evidence(rowsN, pos_keys, auto_sw, filter_fn=is_positive, evidence_per_kw=3) if rowsN else {}
    neg_evidence = build_keyword_evidence(rowsN, neg_keys, auto_sw, filter_fn=is_complaint, evidence_per_kw=3) if rowsN else {}

    def attach_rid(top_list: List[Tuple[str, int]], evi_map: Dict[str, List[Dict[str, Any]]]) -> List[List[Any]]:
        out = []
        for k, c in top_list:
            rid = None
            evs = evi_map.get(k) or []
            if evs:
                rid = evs[0].get("id")
            out.append([k, int(c), rid])
        return out

    pos_top5 = attach_rid(pos_top, pos_evidence)
    neg_top5 = attach_rid(neg_top, neg_evidence)

    cluster_counts: Dict[str, int] = {k: 0 for k in CLUSTERS.keys()}
    if rowsN:
        for r in rowsN:
            if not is_complaint(r):
                continue
            for h in assign_cluster(str(r.get("text") or "")):
                cluster_counts[h] = cluster_counts.get(h, 0) + 1

    size_rows = [r for r in rowsN if isinstance(r.get("tags"), list) and ("size" in r.get("tags"))]
    size_phrases_terms = top_terms(size_rows, topk=10, auto_sw=auto_sw, which="neg") if size_rows else []
    size_phrases = [k for k, _ in size_phrases_terms]

    product_mindmap_1y = build_product_mindmap_1y_sentence(rows_1y=rows1y, rows_7d=rowsN, per_side=2, max_products=24) if rows1y else []

    trend_window = []
    if rowsN:
        tmp = []
        for r in rowsN:
            dt = parse_created_at_iso(str(r.get("created_at") or ""))
            if not dt:
                continue
            d = dt.date().isoformat()
            tmp.append((d, 1, 1 if is_complaint(r) else 0))
        dfT = pd.DataFrame(tmp, columns=["day","cnt","neg"])
        g = dfT.groupby("day", as_index=False).sum()
        g["neg_rate"] = (g["neg"] / g["cnt"]).fillna(0.0)
        trend_window = g.sort_values("day").to_dict(orient="records")

    trend_1y = build_daily_timeseries(rows1y) if rows1y else []
    ml_topics = ml_topics_tfidf_nmf(rows1y, n_topics=8, top_words=8, min_df=3) if rows1y else {"enabled": False, "reason": "no_1y_rows"}
    dl_topics = dl_embeddings_cluster(rows1y, n_clusters=10) if rows1y else {"enabled": False, "reason": "no_1y_rows"}

    period_text = f"최근 {target_days}일 ({startN.isoformat()} ~ {endN.isoformat()})"
    empty_reason = None
    if not rowsN:
        empty_reason = "No reviews in target window. Check upstream collection, created_at format/timezone, or TARGET_DAYS."

    meta: Dict[str, Any] = {
        "version": "v6.1-maxpatch-ml",
        "updated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "period_text": period_text,
        "period_start": startN.isoformat(),
        "period_end": endN.isoformat(),
        "total_reviews": int(len(dfN)),
        "pos_top5": pos_top5,
        "neg_top5": neg_top5,
        "keyword_evidence": {"pos": pos_evidence, "neg": neg_evidence},
        "auto_stopwords": auto_sw,
        "clusters": cluster_counts,
        "size_phrases": size_phrases,
        "fit_words": ["정사이즈", "한치수 크게", "한치수 작게", "타이트", "넉넉", "기장", "소매", "어깨", "가슴", "발볼", "수납", "가벼움"],
        "product_mindmap_1y": product_mindmap_1y,
        "healthcheck": hc,
        "window_created_at_parse_fail": parse_fail_rows,
        "trend_daily": trend_window,
        "trend_daily_1y": trend_1y,
        "ml_topics_1y": ml_topics,
        "dl_topics_1y": dl_topics,
        "empty_reason": empty_reason,
    }

    if debug:
        meta["debug"] = {
            "input_path": str(inp),
            "input_total_rows": len(all_rows),
            "window_rows": len(rowsN),
            "rows_1y": len(rows1y),
            "output_tz": OUTPUT_TZ,
        }

    out_reviews: List[Dict[str, Any]] = []
    if not dfN.empty:
        for r in dfN.to_dict(orient="records"):
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

    html = load_html_template(html_template)

    (SITE_DATA_DIR / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    (SITE_DATA_DIR / "reviews.json").write_text(json.dumps({"reviews": out_reviews}, ensure_ascii=False, indent=2), encoding="utf-8")
    (SITE_DIR / "index.html").write_text(html, encoding="utf-8")

    print("[OK] Build done (v6.1 MAX PATCH + ML/DL)")
    print(f"- Input: {inp}")
    print(f"- Output meta: {SITE_DATA_DIR / 'meta.json'}")
    print(f"- Output reviews: {SITE_DATA_DIR / 'reviews.json'}")
    print(f"- Output html: {SITE_DIR / 'index.html'}")
    print(f"- Period: {period_text}")
    print(f"- Window rows: {len(rowsN)} / Input rows: {len(all_rows)}")
    if empty_reason:
        print(f"[WARN] {empty_reason}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="path to reviews.json (aggregated JSON: {reviews:[...]})")
    ap.add_argument("--html-template", default="", help="optional html template path (highest priority)")
    ap.add_argument("--target-days", type=int, default=DEFAULT_TARGET_DAYS, help="window days (including today), default from env TARGET_DAYS")
    ap.add_argument("--debug", action="store_true", help="write extra diagnostics into meta.json")
    args = ap.parse_args()

    main(
        input_path=args.input,
        html_template=(args.html_template.strip() or None),
        target_days=int(args.target_days or DEFAULT_TARGET_DAYS),
        debug=bool(args.debug),
    )