#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
[CANONICAL VOC DASHBOARD BUILDER]
- This is the active builder to keep using in `voc_fix9`.
- Workflow discovery in `daily-update.yml` is intended to pick this file.
- If you keep historical copies, mark them as legacy/backups so they do not look active.

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
DEFAULT_TARGET_DAYS = int(os.getenv("TARGET_DAYS", "90") or "90")  # last N days ending yesterday (KST)

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


def normalize_source(x: Any) -> str:
    s = str(x or "").strip()
    if not s:
        return "Official"
    low = s.lower()
    if "naver" in low:
        return "Naver"
    if "official" in low:
        return "Official"
    return s  # 기타 소스명은 그대로 유지


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




def build_kw_graph(rows: List[Dict[str, Any]], auto_sw: Dict[str, Any], max_nodes: int = 28) -> Dict[str, Any]:
    """
    Build lightweight "keyword mindmap" graph for UI.
    - nodes: keyword with weight + polarity
    - links: co-occurrence within the same review (window)
    """
    if not rows:
        return {"window": "empty", "nodes": [], "links": []}

    kw_stats: Dict[str, Dict[str, int]] = {}
    co: Dict[Tuple[str, str], int] = {}

    for r in rows:
        ispos = is_positive(r)
        isneg = is_complaint(r)
        sw = build_stopwords_for_row(r, auto_sw)
        toks = list(dict.fromkeys(tokenize_ko(str(r.get("text") or ""), stopwords=sw)))  # unique keep order
        toks = [t for t in toks if len(t) >= 2][:20]
        if not toks:
            continue

        for t in toks:
            st = kw_stats.setdefault(t, {"total": 0, "pos": 0, "neg": 0})
            st["total"] += 1
            if ispos:
                st["pos"] += 1
            if isneg:
                st["neg"] += 1

        # co-occurrence edges (undirected)
        for i in range(len(toks)):
            for j in range(i + 1, len(toks)):
                a, b = toks[i], toks[j]
                if a == b:
                    continue
                if a > b:
                    a, b = b, a
                co[(a, b)] = co.get((a, b), 0) + 1

    # pick nodes by total frequency
    cand = sorted(kw_stats.items(), key=lambda x: x[1]["total"], reverse=True)
    cand = cand[: max_nodes * 3]

    nodes = []
    keep = set()
    for k, st in cand:
        if len(nodes) >= max_nodes:
            break
        # drop near-neutral tokens with tiny evidence
        if st["total"] < 3:
            continue
        pos = st["pos"]
        neg = st["neg"]
        pol = "neutral"
        if neg >= max(2, pos + 1):
            pol = "neg"
        elif pos >= max(2, neg + 1):
            pol = "pos"
        nodes.append({"id": k, "label": k, "w": int(st["total"]), "pos": int(pos), "neg": int(neg), "pol": pol})
        keep.add(k)

    links = []
    for (a, b), w in sorted(co.items(), key=lambda x: x[1], reverse=True):
        if a in keep and b in keep and w >= 2:
            links.append({"source": a, "target": b, "w": int(w)})
        if len(links) >= 80:
            break

    return {"window": "target", "nodes": nodes, "links": links}

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


def build_product_mindmap_3m_sentence(
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
    obj = json.loads(path.read_text(encoding="utf-8"))
    reviews = obj.get("reviews")
    if not isinstance(reviews, list):
        raise ValueError('입력 JSON은 {"reviews": [...]} 형태여야 합니다.')

    out: List[Dict[str, Any]] = []
    for r in reviews:
        if not isinstance(r, dict):
            continue

        # normalize minimal schema
        r["id"] = r.get("id") if r.get("id") is not None else ""
        r["product_code"] = str(r.get("product_code") or "").strip() or "-"
        r["product_name"] = str(r.get("product_name") or "").strip() or r["product_code"]
        r["rating"] = int(pd.to_numeric(r.get("rating"), errors="coerce") or 0)
        r["created_at"] = str(r.get("created_at") or "").strip()
        r["text"] = str(r.get("text") or "").strip()
        r["source"] = normalize_source(r.get("source"))

        # optional fields (safe)
        r["product_url"] = str(r.get("product_url") or "").strip()
        r["option_size"] = str(r.get("option_size") or "").strip()
        r["option_color"] = str(r.get("option_color") or "").strip()
        r["local_product_image"] = str(r.get("local_product_image") or "").strip()
        r["local_review_thumb"] = str(r.get("local_review_thumb") or "").strip()
        r["text_image_path"] = str(r.get("text_image_path") or "").strip()

        out.append(ensure_tags_and_direction(r))

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


DEFAULT_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>VOC Dashboard | Official + Naver Brand Reviews</title>

  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">

  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200;400;600;800&display=swap');
    :root { --brand:#002d72; --bg0:#f6f8fb; --bg1:#eef3f9; }

    html, body { height: 100%; }
    body{
      background: linear-gradient(180deg, var(--bg0), var(--bg1));
      font-family: 'Plus Jakarta Sans', sans-serif;
      color:#0f172a; min-height:100vh;
    }

    .glass-card{
      background: rgba(255,255,255,0.55);
      backdrop-filter: blur(20px);
      border: 1px solid rgba(255,255,255,0.75);
      border-radius: 30px;
      box-shadow: 0 20px 50px rgba(0,45,114,0.05);
    }

    .topbar{
      background: rgba(255,255,255,0.70);
      backdrop-filter: blur(15px);
      border-bottom: 1px solid rgba(255,255,255,0.80);
    }

    .summary-card{
      border-radius: 26px; background: rgba(255,255,255,0.55);
      border: 1px solid rgba(255,255,255,0.75); backdrop-filter: blur(18px);
      box-shadow: 0 20px 50px rgba(0,45,114,0.05);
      padding: 18px 20px;
    }
    .small-label{
      font-size: 10px; letter-spacing: 0.3em; text-transform: uppercase; font-weight: 900;
    }
    .input-glass{
      background: rgba(255,255,255,0.65); border: 1px solid rgba(255,255,255,0.80);
      border-radius: 18px; padding: 12px 14px; outline: none; font-weight: 800; color:#0f172a;
    }
    .input-glass:focus{
      box-shadow: 0 0 0 4px rgba(0,45,114,0.10);
      border-color: rgba(0,45,114,0.25);
    }

    .chip{
      border-radius: 9999px; padding: 10px 14px; font-weight: 900; font-size: 12px;
      border: 1px solid rgba(255,255,255,0.85); background: rgba(255,255,255,0.60);
      color:#334155; cursor:pointer; user-select:none;
    }
    .chip.active{
      background: rgba(0,45,114,0.95); color:#fff; border-color: rgba(0,45,114,1);
      box-shadow: 0 10px 30px rgba(0,45,114,0.15);
    }

    .tab-btn{
      padding: 10px 14px; border-radius: 18px; font-weight: 900; font-size: 12px;
      border: 1px solid rgba(255,255,255,0.85); background: rgba(255,255,255,0.60);
      color:#475569; transition: all .15s ease;
    }
    .tab-btn:hover{ background: rgba(255,255,255,0.90); }
    .tab-btn.active{
      background: rgba(0,45,114,0.95); color:#fff; border-color: rgba(0,45,114,1);
      box-shadow: 0 10px 30px rgba(0,45,114,0.15);
    }

    .overlay{
      position: fixed; inset:0; background: rgba(255,255,255,0.65);
      backdrop-filter: blur(10px);
      display:none; align-items:center; justify-content:center; z-index:9999;
    }
    .overlay.show{ display:flex; }
    .spinner{
      width:56px;height:56px;border-radius:9999px; border:6px solid rgba(0,0,0,0.08);
      border-top-color: rgba(0,45,114,0.95); animation: spin .9s linear infinite;
    }
    @keyframes spin { to { transform: rotate(360deg);} }

    .tbl{
      width:100%; border-collapse: separate; border-spacing: 0; overflow:hidden; border-radius: 22px;
      border: 1px solid rgba(255,255,255,0.85); background: rgba(255,255,255,0.55);
    }
    .tbl th{
      font-size: 11px; letter-spacing: .22em; text-transform: uppercase; font-weight: 900;
      color:#475569; background: rgba(255,255,255,0.75); padding: 14px 14px;
      position: sticky; top: 0; z-index: 1;
    }
    .tbl td{
      padding: 14px 14px; border-top: 1px solid rgba(255,255,255,0.75); font-weight: 800;
      color:#0f172a; font-size: 13px; vertical-align: top;
    }
    .tbl .muted{ color:#64748b; font-weight:800; font-size:12px; }

    .review-card{
      border-radius: 26px; background: rgba(255,255,255,0.55);
      border: 1px solid rgba(255,255,255,0.80); backdrop-filter: blur(18px);
      box-shadow: 0 16px 42px rgba(0,45,114,0.04); padding: 18px 18px;
    }

    .badge{
      display:inline-flex; align-items:center; gap:6px; padding: 6px 10px; border-radius: 9999px;
      font-size: 11px; font-weight: 900; border: 1px solid rgba(255,255,255,0.85);
      background: rgba(255,255,255,0.65); color:#334155;
    }
    .badge.neg{ background: rgba(239,68,68,0.10); color:#b91c1c; border-color: rgba(239,68,68,0.18); }
    .badge.pos{ background: rgba(16,185,129,0.10); color:#047857; border-color: rgba(16,185,129,0.18); }
    .badge.size{ background: rgba(59,130,246,0.10); color:#1d4ed8; border-color: rgba(59,130,246,0.18); }

    .img-box{
      width:72px; height:72px; border-radius:18px; overflow:hidden; background: rgba(255,255,255,0.70);
      border:1px solid rgba(255,255,255,0.85);
    }
    .img-box img{ width:100%; height:100%; object-fit:cover; display:block; }

    .line-clamp-1{ overflow:hidden; display:-webkit-box; -webkit-line-clamp:1; -webkit-box-orient:vertical; }
    .line-clamp-2{ overflow:hidden; display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; }
    .line-clamp-3{ overflow:hidden; display:-webkit-box; -webkit-line-clamp:3; -webkit-box-orient:vertical; }

    .sentence-list{ display:grid; grid-template-columns:1fr; gap:10px; }
    .sentence-item{ padding:12px 14px; border-radius:18px; font-size:13px; font-weight:800; line-height:1.6; border:1px solid rgba(255,255,255,0.85); background: rgba(255,255,255,0.62); color:#0f172a; }
    .sentence-item.neg{ background: rgba(239,68,68,0.08); border-color: rgba(239,68,68,0.16); }
    .sentence-item.pos{ background: rgba(16,185,129,0.08); border-color: rgba(16,185,129,0.16); }

    .review-grid{ display:grid; grid-template-columns: 1fr; gap: 14px; }
    @media (min-width: 768px){ .review-grid{ grid-template-columns: 1fr 1fr; } }
    @media (min-width: 1280px){ .review-grid{ grid-template-columns: 1fr 1fr 1fr; } }

    /* Product ML cards */
    .ml-card{
      border-radius: 26px;
      background: rgba(255,255,255,0.55);
      border: 1px solid rgba(255,255,255,0.80);
      backdrop-filter: blur(18px);
      box-shadow: 0 16px 42px rgba(0,45,114,0.04);
      padding: 18px 18px;
    }
    .ml-grid{ display:grid; grid-template-columns:1fr; gap:14px; }
    @media (min-width: 1024px){ .ml-grid{ grid-template-columns:1fr 1fr; } }

    body.embedded .topbar, body.embedded .layout-header { display:none !important; }
    body.embedded main{ padding: 24px !important; }
  </style>
</head>

<body>
  <div id="errorBanner" class="hidden mx-auto mt-6 max-w-[1680px] px-4 md:px-6 xl:px-8">
    <div class="rounded-[24px] border border-red-200 bg-red-50 px-5 py-4 text-sm font-bold text-red-700 shadow-sm">
      <div class="text-xs tracking-[0.28em] uppercase font-black mb-1">Data Error</div>
      <div id="errorBannerText">VOC 데이터를 불러오지 못했습니다.</div>
    </div>
  </div>

  <div id="overlay" class="overlay">
    <div class="flex flex-col items-center gap-3">
      <div class="spinner"></div>
      <div id="overlayText" class="text-sm font-black text-slate-700">Loading...</div>
    </div>
  </div>

  <main class="w-full px-4 py-6 md:px-6 md:py-8 xl:px-8">
    <div class="mx-auto w-full max-w-[1680px]">
      <div class="w-full">

        <div class="layout-header">
          <header class="flex flex-col md:flex-row md:items-center justify-between mb-6 gap-6">
            <div>
              <h1 class="text-4xl md:text-5xl font-black tracking-tight text-slate-900 mb-3">
                Official몰 & Naver Brand 리뷰 VOC 대시보드
              </h1>
              <div class="text-sm font-bold text-slate-500" id="headerMeta">-</div>
            </div>

            <div class="flex items-center gap-2 flex-wrap">
              <button class="tab-btn active" data-tab="combined" onclick="switchSourceTab('combined')">
                <i class="fa-solid fa-layer-group mr-2"></i>Combined
              </button>
              <button class="tab-btn" data-tab="official" onclick="switchSourceTab('official')">
                <i class="fa-solid fa-store mr-2"></i>Official
              </button>
              <button class="tab-btn" data-tab="naver" onclick="switchSourceTab('naver')">
                <i class="fa-brands fa-naver mr-2"></i>Naver Brand
              </button>
            </div>
          </header>
        </div>

        <!-- 0 ML Signals -->
        <section class="mb-10">
          <div class="glass-card p-8">
            <div class="flex items-end justify-between gap-6 flex-wrap mb-6">
              <div>
                <div class="small-label text-blue-600 mb-2">0. ML Signals</div>
                <div class="text-2xl font-black text-slate-900">어제 리뷰 핵심 문장</div>
                <div class="text-sm font-bold text-slate-500 mt-2">기본: 어제(KST) · 긍정/부정 문장 각 2개</div>
              </div>
              <div class="flex gap-2 flex-wrap">
                <button class="chip active" id="chip-daily" onclick="toggleChip('daily')">Daily</button>
                <button class="chip" id="chip-low" onclick="toggleChip('low')">Low</button>
              </div>
            </div>

            <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
              <div class="summary-card">
                <div class="small-label text-blue-600 mb-2">YESTERDAY</div>
                <div class="text-3xl font-black"><span id="yCount">-</span> reviews</div>
                <div class="text-xs font-bold text-slate-500 mt-2">어제 업로드된 리뷰 수 (현재 탭/필터 기준)</div>
                <div class="text-xs font-bold text-slate-500 mt-3" id="yLowHint">-</div>
              </div>

              <div class="summary-card lg:col-span-2">
                <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div>
                    <div class="small-label text-red-600 mb-3">NEGATIVE</div>
                    <div id="yNegSentences" class="sentence-list"></div>
                  </div>
                  <div>
                    <div class="small-label text-emerald-600 mb-3">POSITIVE</div>
                    <div id="yPosSentences" class="sentence-list"></div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </section>



        <!-- 1 Daily feed -->
        <section class="mb-10">
          <div class="glass-card p-8">
            <div class="flex items-end justify-between gap-6 flex-wrap mb-6">
              <div>
                <div class="small-label text-blue-600 mb-2">1. Daily Feed</div>
                <div class="text-2xl font-black text-slate-900">그날 올라온 리뷰 (업로드 순)</div>
                <div class="text-sm font-bold text-slate-500 mt-2">기본 날짜: 어제(KST) · 최근 7일 범위 내 선택</div>
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

        <!-- 2. Product ML (Cumulative) -->
        <section class="mb-10">
          <div class="glass-card p-8">
            <div class="flex items-end justify-between gap-6 flex-wrap mb-6">
              <div>
                <div class="small-label text-blue-600 mb-2">2. Product ML (Cumulative)</div>
                <div class="text-2xl font-black text-slate-900">제품별 리뷰 누적 분석</div>
                <div class="text-sm font-bold text-slate-500 mt-2">
                  NEG 기준: ★1~2 (저평점) · POS 기준: ★4~5
                </div>
              </div>

              <div class="flex gap-2 flex-wrap items-center">
                <select id="mlWindow" class="input-glass" onchange="renderAll()">
                  <option value="90" selected>최근 3개월</option>
                  <option value="180">최근 6개월</option>
                  <option value="365">최근 12개월</option>
                  <option value="0">전체</option>
                </select>
                <select id="mlMinReviews" class="input-glass" onchange="renderAll()">
                  <option value="3" selected>최소 리뷰수: 3</option>
                  <option value="5">최소 리뷰수: 5</option>
                  <option value="10">최소 리뷰수: 10</option>
                  <option value="1">최소 리뷰수: 1</option>
                </select>
                <button class="chip" onclick="scrollToMLTop()">ML 섹션 보기</button>
              </div>
            </div>

            <div class="summary-card mb-4">
              <div class="small-label text-slate-700 mb-2">HOW TO READ</div>
              <div class="text-sm font-extrabold text-slate-800 leading-relaxed">
                - NEGATIVE: ★1~2 리뷰 원문 2개 노출<br/>
                - POSITIVE: ★4~5 리뷰 원문 2개 노출<br/>
                - 버튼 클릭 → 아래 Daily Feed 검색으로 필터
              </div>
            </div>

            <div id="mlProductGrid" class="ml-grid"></div>

            <div id="mlNoData" class="hidden review-card text-center">
              <div class="text-lg font-black text-slate-800">조건에 맞는 제품 누적 데이터가 없습니다.</div>
              <div class="text-xs font-bold text-slate-500 mt-2">기간/최소 리뷰수/탭(Official/Naver Brand)을 조정해보세요.</div>
            </div>
          </div>
        </section>


        <footer class="text-xs font-bold text-slate-500 pb-8">
          * 데이터 소스: data/reviews.json / data/meta.json<br/>
          * 기본 동작: 어제(KST) 기준 Daily Feed 표시
        </footer>
      </div>
    </div>
  </main>

  <script>
    let META = null;
    let REVIEWS = [];
    const uiState = {
      sourceTab: "combined",
      chips: { daily: true, low: false },
    };

    function esc(s){
      return String(s ?? "")
        .replaceAll("&","&amp;")
        .replaceAll("<","&lt;")
        .replaceAll(">","&gt;")
        .replaceAll('"',"&quot;");
    }
    function asArr(x){ return Array.isArray(x) ? x : []; }
    function fmtDT(s){
      if (!s) return "-";
      const t = String(s).replace("T"," ").replace("+09:00","").replace("Z","");
      return t.slice(0,16);
    }
    function showOverlay(msg){
      const o = document.getElementById("overlay");
      const t = document.getElementById("overlayText");
      if (t) t.textContent = msg || "Loading...";
      if (o) o.classList.add("show");
    }
    function hideOverlay(){
      const o = document.getElementById("overlay");
      if (o) o.classList.remove("show");
    }
    function showError(msg){
      const wrap = document.getElementById("errorBanner");
      const text = document.getElementById("errorBannerText");
      if (text) text.textContent = msg || "VOC 데이터를 불러오지 못했습니다.";
      if (wrap) wrap.classList.remove("hidden");
    }
    function runWithOverlay(msg, fn){
      try{
        showOverlay(msg);
        const r = fn();
        if (r && typeof r.then === "function"){
          return r.finally(()=>hideOverlay());
        }
        hideOverlay();
        return r;
      }catch(e){
        console.error(e);
        hideOverlay();
      }
    }

    (function(){
      const params = new URLSearchParams(location.search);
      if (params.get("embed") === "1"){
        document.body.classList.add("embedded");
      }
    })();

    function pad2(n){ return String(n).padStart(2,"0"); }
    function kstNow(){
      const now = new Date();
      const utc = now.getTime() + (now.getTimezoneOffset() * 60000);
      return new Date(utc + (9 * 60 * 60000));
    }
    function kstDateStr(offsetDays=0){
      const kst = kstNow();
      kst.setDate(kst.getDate() + offsetDays);
      return `${kst.getFullYear()}-${pad2(kst.getMonth()+1)}-${pad2(kst.getDate())}`;
    }
    async function fetchJsonOrThrow(urls){
      const candidates = Array.isArray(urls) ? urls : [urls];
      let lastError = null;
      for (const url of candidates){
        try{
          const res = await fetch(url, {cache:"no-store"});
          if (!res.ok){
            throw new Error(`HTTP ${res.status} while loading ${url}`);
          }
          return await res.json();
        }catch(err){
          lastError = err;
        }
      }
      throw lastError || new Error(`Failed to load JSON from candidates: ${candidates.join(", ")}`);
    }
    function parseDate10(s){
      if (!s) return null;
      const d10 = String(s).slice(0,10);
      const d = new Date(d10 + "T00:00:00");
      return isNaN(d.getTime()) ? null : d;
    }

    function switchSourceTab(tab){
      uiState.sourceTab = tab;
      document.querySelectorAll(".tab-btn").forEach(b=>{
        b.classList.toggle("active", b.getAttribute("data-tab") === tab);
      });
      renderAll();
    }

    function toggleChip(name){
      uiState.chips[name] = !uiState.chips[name];
      const el = document.getElementById(`chip-${name}`);
      if (el) el.classList.toggle("active", uiState.chips[name]);
      renderAll();
    }

    function setSearchAndRender(q){
      const el = document.getElementById("qInput");
      if (el) el.value = q || "";
      // Daily chip on + yesterday by default is OK; just rerender
      renderAll();
      // (옵션) Daily Feed로 스크롤
      document.getElementById("dailyFeed")?.scrollIntoView({behavior:"smooth", block:"start"});
    }

    function scrollToMLTop(){
      document.getElementById("mlProductGrid")?.scrollIntoView({behavior:"smooth", block:"start"});
    }

    function getFilteredReviews(){
      let rows = REVIEWS.slice();

      if (uiState.sourceTab === "official") rows = rows.filter(r => r.source === "Official");
      if (uiState.sourceTab === "naver") rows = rows.filter(r => r.source === "Naver");

      const day = (document.getElementById("daySelect")?.value || "").trim();
      if (uiState.chips.daily && day){
        rows = rows.filter(r => String(r.created_at || "").slice(0,10) === day);
      }

      const prod = (document.getElementById("productSelect")?.value || "").trim();
      if (prod) rows = rows.filter(r => String(r.product_code || "") === prod);

      const sz = (document.getElementById("sizeSelect")?.value || "").trim();
      if (sz) rows = rows.filter(r => String(r.option_size || "") === sz);

      if (uiState.chips.low){
        rows = rows.filter(r => (Number(r.rating||0) <= 2) || asArr(r.tags).includes("low"));
      }

      const q = (document.getElementById("qInput")?.value || "").trim().toLowerCase();
      if (q){
        rows = rows.filter(r =>
          String(r.text||"").toLowerCase().includes(q) ||
          String(r.product_name||"").toLowerCase().includes(q)
        );
      }

      const sort = document.getElementById("sortSelect")?.value || "upload";
      if (sort === "latest") rows.sort((a,b)=> new Date(b.created_at||0) - new Date(a.created_at||0));
      else if (sort === "long") rows.sort((a,b)=> (String(b.text||"").length - String(a.text||"").length));
      else if (sort === "low") rows.sort((a,b)=> (Number(a.rating||0) - Number(b.rating||0)) || (new Date(b.created_at||0) - new Date(a.created_at||0)));

      return rows;
    }

    function calcMetrics(reviews){
      const total = Math.max(1, reviews.length);
      const sizeMention = reviews.filter(r => asArr(r.tags).includes("size")).length;

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
          });
        }
        const g = byProd.get(code);
        g.reviews += 1;
        if (asArr(r.tags).includes("size")) g.sizeIssue += 1;
        if ((Number(r.rating||0) <= 2) || asArr(r.tags).includes("low")) g.low += 1;
      }

      const rows = Array.from(byProd.values()).map(g => {
        const sizeRate = Math.round((g.sizeIssue / Math.max(1,g.reviews))*100);
        const lowRate  = Math.round((g.low / Math.max(1,g.reviews))*100);
        return { ...g, sizeRate, lowRate, kwds: "-" };
      });

      const rankSize = rows.slice().sort((a,b)=> (b.sizeRate - a.sizeRate) || (b.reviews - a.reviews));
      const rankLow  = rows.slice().sort((a,b)=> (b.lowRate - a.lowRate) || (b.reviews - a.reviews));
      const rankBoth = rows.slice().sort((a,b)=> ((b.sizeRate+b.lowRate) - (a.sizeRate+a.lowRate)) || (b.reviews - a.reviews));

      return { total, sizeMentionRate: Math.round((sizeMention/total)*100), rankSize, rankLow, rankBoth };
    }

    function renderHeader(){
      const runDate = META?.updated_at || "-";
      const periodText = META?.period_text || "-";
      const headerMeta = document.getElementById("headerMeta");

      if (headerMeta) headerMeta.textContent = `${runDate} · ${periodText}`;
    }

    function reviewCardHTML(r){
      const t = asArr(r.tags);
      const tags = [];

      if (Number(r.rating||0) >= 4 || t.includes("pos")) tags.push(`<span class="badge pos"><i class="fa-solid fa-face-smile"></i> 1 POS</span>`);
      if (t.includes("size")) tags.push(`<span class="badge size"><i class="fa-solid fa-ruler"></i> 2 SIZE</span>`);
      if ((Number(r.rating||0) <= 2) || t.includes("low")) tags.push(`<span class="badge neg"><i class="fa-solid fa-triangle-exclamation"></i> 3 NEG</span>`);
      if (t.includes("req")) tags.push(`<span class="badge neg"><i class="fa-solid fa-wrench"></i> 4 REQUEST</span>`);

      const prodImg = r.local_product_image
        ? `<img src="${esc(r.local_product_image)}" alt="">`
        : `<div class="w-full h-full flex items-center justify-center text-[10px] text-slate-400">NO IMAGE</div>`;

      const reviewThumb = r.local_review_thumb
        ? `<img src="${esc(r.local_review_thumb)}" class="w-20 h-20 object-contain rounded-lg bg-slate-50 border border-white/80" />`
        : ``;

      const reviewUrl = String(r.review_original_url || r.product_url || "").trim();
      const reviewLink = reviewUrl
        ? `<a href="${esc(reviewUrl)}" target="_blank" rel="noopener noreferrer" class="text-xs font-black text-blue-600 hover:underline">리뷰 원문보기</a>`
        : `<span class="text-xs font-black text-slate-400">리뷰 원문 링크 없음</span>`;

      return `
        <div class="review-card hover:shadow-lg transition" id="review-${esc(r.id)}">
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
            <div class="text-right shrink-0">
              <div class="text-xs font-black text-slate-700">★ ${esc(r.rating)}</div>
              <div class="text-[11px] font-bold text-slate-500 mt-1">${esc(fmtDT(r.created_at))}</div>
            </div>
          </div>

          <div class="mt-3 flex flex-wrap gap-2">
            ${(r.option_size ? `<span class="badge">옵션: ${esc(r.option_size)}</span>` : ``)}
            ${tags.join("")}
          </div>

          <div class="mt-4 flex items-start justify-between gap-4">
            <div class="flex-1 min-w-0">
              <div class="text-sm font-extrabold text-slate-800 leading-relaxed whitespace-pre-wrap break-words">${esc(r.text || "")}</div>
              <div class="mt-3">${reviewLink}</div>
            </div>
            ${reviewThumb ? `<div class="shrink-0">${reviewThumb}</div>` : ``}
          </div>
        </div>
      `;
    }

    function renderDailyFeed(reviews){
      const container = document.getElementById("dailyFeed");
      const no = document.getElementById("noResults");
      if (!container || !no) return;

      const rows = reviews.slice(0, 60);
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
        Array.from(map.entries())
          .sort((a,b)=> String(a[1]).localeCompare(String(b[1])))
          .map(([code,name]) => `<option value="${esc(code)}">${esc(name)} (${esc(code)})</option>`)
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

    function pickTopSentences(rows, mode){
      const out = [];
      const seen = new Set();
      for (const r of rows){
        const rating = Number(r.rating || 0);
        const txt = String(r.text || "").trim().replace(/\s+/g, " ");
        if (!txt) continue;
        if (mode === "neg" && !(rating > 0 && rating <= 2)) continue;
        if (mode === "pos" && !(rating >= 4)) continue;
        const key = txt.slice(0, 120);
        if (seen.has(key)) continue;
        seen.add(key);
        out.push({ text: txt, product: r.product_name || r.product_code || "-", code: r.product_code || "", url: r.review_original_url || r.product_url || "" });
        if (out.length >= 2) break;
      }
      return out;
    }

    function sentenceListHTML(items, cls, emptyText){
      if (!items.length){
        return `<div class="sentence-item ${cls} text-slate-400">${esc(emptyText)}</div>`;
      }
      return items.map(item => {
        const link = item.url
          ? `<a href="${esc(item.url)}" target="_blank" rel="noopener noreferrer" class="text-xs font-black text-blue-600 hover:underline mt-2 inline-block">리뷰 원문보기</a>`
          : ``;
        return `
          <div class="sentence-item ${cls}">
            <div class="text-sm font-black text-slate-900 whitespace-pre-wrap break-words">${esc(item.text)}</div>
            <div class="text-[11px] font-bold text-slate-500 mt-2">${esc(item.product)}${item.code ? ` · ${esc(item.code)}` : ``}</div>
            ${link}
          </div>
        `;
      }).join("");
    }

    function renderMLSignals(allFiltered){
      const yday = kstDateStr(-1);
      const yRows = allFiltered
        .filter(r => String(r.created_at||"").slice(0,10) === yday)
        .sort((a,b)=> new Date(b.created_at||0) - new Date(a.created_at||0));
      const yCnt = yRows.length;

      const yCountEl = document.getElementById("yCount");
      if (yCountEl) yCountEl.textContent = yCnt;

      const lowCnt = yRows.filter(r => (Number(r.rating||0) <= 2) || asArr(r.tags).includes("low")).length;
      const hint = document.getElementById("yLowHint");
      if (hint){
        hint.textContent = (yCnt ? `Low/리스크 추정: ${lowCnt}건` : "어제 데이터가 없습니다.");
      }

      const negEl = document.getElementById("yNegSentences");
      const posEl = document.getElementById("yPosSentences");
      if (negEl) negEl.innerHTML = sentenceListHTML(pickTopSentences(yRows, "neg"), "neg", "어제 부정 문장이 없습니다.");
      if (posEl) posEl.innerHTML = sentenceListHTML(pickTopSentences(yRows, "pos"), "pos", "어제 긍정 문장이 없습니다.");
    }

    // -----------------------------
    // Product ML (Cumulative): NEG=low rating
    // -----------------------------
    const STOPWORDS = new Set([
      "그리고","그냥","진짜","너무","정말","완전","약간","조금","그래서","하지만","근데","저는","제가","저도","그런데",
      "합니다","했어요","했는데","되요","돼요","입니다","있어요","없어요","같아요","같습니다","이거","이것","저것","그거",
      "제품","구매","배송","포장","사진","후기","리뷰","사용","구입","주문","사이즈","size","컬럼비아","콜롬비아",
      "the","and","to","of","is","are","was","were","it","this","that","with","for","on","in"
    ]);

    function normalizeText(s){
      const t = String(s||"")
        .replace(/[\r\n\t]+/g, " ")
        .replace(/[0-9]/g, " ")
        .replace(/[^\p{L}\p{N}\s]/gu, " ")
        .toLowerCase();
      return t;
    }

    function extractTokens(text){
      const t = normalizeText(text);
      const raw = t.split(/\s+/).filter(Boolean);

      const out = [];
      for (const w of raw){
        if (w.length < 2) continue;
        if (STOPWORDS.has(w)) continue;
        // 너무 긴 토큰/이상치 컷
        if (w.length > 22) continue;
        out.push(w);
      }
      return out;
    }

    function topSentences(texts, k=2){
      const out = [];
      const seen = new Set();
      for (const tx of texts){
        const s = String(tx || "").trim().replace(/\s+/g, " ");
        if (!s) continue;
        const key = s.slice(0, 120);
        if (seen.has(key)) continue;
        seen.add(key);
        out.push(s);
        if (out.length >= k) break;
      }
      return out;
    }

    function buildProductMLRows(){
      const winSel = document.getElementById("mlWindow");
      const minSel = document.getElementById("mlMinReviews");
      const winDays = Number(winSel?.value || 90);
      const minReviews = Number(minSel?.value || 3);

      const now = kstNow();
      const cutoff = winDays > 0 ? new Date(now.getTime() - winDays*24*60*60*1000) : null;

      // 탭 필터(Official/Naver)는 ML에도 동일 적용
      let base = REVIEWS.slice();
      if (uiState.sourceTab === "official") base = base.filter(r => r.source === "Official");
      if (uiState.sourceTab === "naver") base = base.filter(r => r.source === "Naver");

      // 기간 컷(누적 분석은 Daily chip의 날짜 선택과 분리)
      if (cutoff){
        base = base.filter(r => {
          const d = parseDate10(r.created_at);
          return d ? (d >= cutoff) : false;
        });
      }

      // product별 그룹
      const byProd = new Map();
      for (const r of base){
        const code = String(r.product_code || "-").trim() || "-";
        if (code === "-") continue;

        if (!byProd.has(code)){
          byProd.set(code, {
            product_code: code,
            product_name: r.product_name || code,
            product_url: r.product_url || "",
            local_product_image: r.local_product_image || "",
            all: [],
            neg: [], // ★1~2
            pos: [], // ★4~5
            cnt: 0,
            negCnt: 0,
            posCnt: 0,
            lowRate: 0,
          });
        }
        const g = byProd.get(code);
        g.cnt += 1;
        const rating = Number(r.rating||0);
        const text = String(r.text||"").trim();
        if (text) g.all.push(text);

        // ✅ NEG: 저평점 기반 (★1~2)
        if (rating > 0 && rating <= 2){
          g.negCnt += 1;
          if (text) g.neg.push(text);
        }
        // POS: ★4~5
        if (rating >= 4){
          g.posCnt += 1;
          if (text) g.pos.push(text);
        }
      }

      let rows = Array.from(byProd.values())
        .filter(g => g.cnt >= minReviews)
        .map(g => {
          g.lowRate = Math.round((g.negCnt / Math.max(1,g.cnt))*100);
          g.negSentences = topSentences(g.neg, 2);
          g.posSentences = topSentences(g.pos, 2);
          return g;
        });

      // 정렬: (1) lowRate desc (2) total reviews desc
      rows.sort((a,b)=> (b.lowRate - a.lowRate) || (b.cnt - a.cnt));

      return { rows, winDays, minReviews };
    }

    function mlSentenceHTML(items, tone, emptyText){
      if (!items || !items.length){
        return `<div class="sentence-item ${tone} text-slate-400">${esc(emptyText)}</div>`;
      }
      return items.map((txt, idx) => `
        <div class="sentence-item ${tone}">
          <div class="text-[11px] font-black ${tone === "neg" ? "text-red-700" : "text-emerald-700"} mb-1">${tone === "neg" ? "NEGATIVE" : "POSITIVE"} ${idx + 1}</div>
          <div class="whitespace-pre-wrap break-words">${esc(txt)}</div>
        </div>
      `).join("");
    }

    function mlCardHTML(g){
      const img = g.local_product_image
        ? `<img src="${esc(g.local_product_image)}" alt="">`
        : `<div class="w-full h-full flex items-center justify-center text-[10px] text-slate-400">NO IMAGE</div>`;

      const url = g.product_url
        ? `<a href="${esc(g.product_url)}" target="_blank" rel="noopener noreferrer" class="text-xs font-black text-blue-600 hover:underline">상품 페이지</a>`
        : `<span class="text-xs font-bold text-slate-400">상품 URL 없음</span>`;

      return `
        <div class="ml-card">
          <div class="flex items-start justify-between gap-3">
            <div class="flex items-center gap-3 min-w-0">
              <div class="img-box">${img}</div>
              <div class="min-w-0">
                <div class="font-black text-slate-900 line-clamp-2">${esc(g.product_name)}</div>
                <div class="text-xs font-bold text-slate-500 mt-1">code: ${esc(g.product_code)} · reviews: ${esc(g.cnt)}</div>
                <div class="mt-1">${url}</div>
              </div>
            </div>
            <div class="text-right shrink-0">
              <div class="text-xs font-black text-slate-700">LOW(★1~2)</div>
              <div class="text-2xl font-black text-red-700">${esc(g.lowRate)}%</div>
              <div class="text-[11px] font-bold text-slate-500 mt-1">neg ${esc(g.negCnt)} · pos ${esc(g.posCnt)}</div>
            </div>
          </div>

          <div class="grid grid-cols-1 lg:grid-cols-2 gap-4 mt-4">
            <div>
              <div class="text-xs font-black text-slate-500 mb-2">NEGATIVE 문장 2개 (★1~2)</div>
              <div class="sentence-list">${mlSentenceHTML(g.negSentences, "neg", "NEGATIVE 문장이 없습니다.")}</div>
            </div>
            <div>
              <div class="text-xs font-black text-slate-500 mb-2">POSITIVE 문장 2개 (★4~5)</div>
              <div class="sentence-list">${mlSentenceHTML(g.posSentences, "pos", "POSITIVE 문장이 없습니다.")}</div>
            </div>
          </div>

          <div class="mt-4 flex items-center gap-2 flex-wrap">
            <button class="chip" onclick="focusProduct('${esc(g.product_code)}')">
              <i class="fa-solid fa-filter mr-2"></i>이 제품으로 Daily Feed 보기
            </button>
          </div>
        </div>
      `;
    }

    function focusProduct(code){
      const sel = document.getElementById("productSelect");
      if (sel){
        sel.value = code;
      }
      // Daily chip on + yesterday 유지
      renderAll();
      document.getElementById("dailyFeed")?.scrollIntoView({behavior:"smooth", block:"start"});
    }

    function renderProductML(){
      const grid = document.getElementById("mlProductGrid");
      const no = document.getElementById("mlNoData");
      if (!grid || !no) return;

      const { rows } = buildProductMLRows();

      if (!rows.length){
        grid.innerHTML = "";
        no.classList.remove("hidden");
        return;
      }

      no.classList.add("hidden");
      // 너무 길어지면 상위 20개만
      const top = rows.slice(0, 20);
      grid.innerHTML = top.map(mlCardHTML).join("");
    }

    function renderAll(){
      renderHeader();
      renderProductSelect();
      renderSizeSelect();

      const filtered = getFilteredReviews();

      renderMLSignals(filtered);
      renderDailyFeed(filtered);

      // ✅ Product ML is cumulative (independent of daily date)
      renderProductML();
    }

    async function boot(){
      runWithOverlay("Loading VOC data...", async () => {
        try{
        const [meta, reviews] = await Promise.all([
          fetchJsonOrThrow([
            "./data/meta.json",
            "./site/data/meta.json",
            "./reports/voc_crema/data/meta.json",
            "../data/meta.json",
          ]),
          fetchJsonOrThrow([
            "./data/reviews.json",
            "./site/data/reviews.json",
            "./reports/voc_crema/data/reviews.json",
            "../data/reviews.json",
          ]),
        ]);

        META = meta || {};
        REVIEWS = Array.isArray(reviews?.reviews) ? reviews.reviews : [];

        if (!META.updated_at){
          throw new Error("meta.json is missing updated_at");
        }
        if (!Array.isArray(REVIEWS) || REVIEWS.length === 0){
          throw new Error("reviews.json contains no review rows");
        }

        const dayInput = document.getElementById("daySelect");
        if (dayInput){
          if (META?.period_start) dayInput.min = META.period_start;
          if (META?.period_end) dayInput.max = META.period_end;

          // ✅ default = yesterday(KST)
          if (!dayInput.value){
            dayInput.value = kstDateStr(-1);
          }
        }

        renderAll();
        }catch(e){
          console.error(e);
          showError(`VOC 데이터를 불러오지 못했습니다. ${e?.message || ""}`.trim());
        }
      });
    }

    boot();
  </script>
</body>
</html>"""




def load_html_template(cli_template: Optional[str]) -> str:
    if cli_template:
        p = pathlib.Path(cli_template).expanduser()
        if p.exists():
            return p.read_text(encoding="utf-8")

    for tpl in [
        SITE_DIR / "template.html",
        ROOT / "templates" / "voc_index.html",
        ROOT / "templates" / "voc_dashboard.html",
    ]:
        if tpl.exists():
            return tpl.read_text(encoding="utf-8")

    return DEFAULT_HTML_TEMPLATE



def normalize_template_paths(html: str) -> str:
    """Make dashboard portable when copied under reports/voc_crema/.

    We build outputs into `site/` during the job, then Actions copies `site/` into
    `reports/voc_crema/`. If the HTML template hardcodes `site/data/...`, the
    deployed page will try to fetch `/reports/voc_crema/site/data/...` (404).
    This normalizer rewrites those paths to be relative to the current page.
    """
    if not html:
        return html
    # Remove the extra top status strip so the dashboard starts from the main hero.
    html = re.sub(
        r'<header class="topbar sticky top-0 z-50">.*?</header>\s*',
        "",
        html,
        count=1,
        flags=re.S,
    )
    # Common patterns from older templates / inline notes
    html = html.replace("site/data/", "data/")
    html = html.replace("site/data", "data")
    # Some templates used absolute-like /site/data (rare)
    html = html.replace("/site/data/", "data/")
    html = html.replace("/site/data", "data")
    html = html.replace("VOC Dashboard | Official + Naver Reviews", "VOC Dashboard | Official + Naver Brand Reviews")
    html = html.replace("Official + Naver", "Official + Naver Brand")
    html = html.replace("Official + Naver Brand Brand", "Official + Naver Brand")
    html = html.replace("Official몰 & Naver 리뷰 VOC 대시보드", "Official몰 & Naver Brand 리뷰 VOC 대시보드")
    html = html.replace('<i class="fa-brands fa-naver mr-2"></i>Naver', '<i class="fa-brands fa-naver mr-2"></i>Naver Brand')
    html = html.replace("탭(Official/Naver)", "탭(Official/Naver Brand)")
    html = html.replace('max-w-[1280px] px-4 md:px-8', 'max-w-[1680px] px-4 md:px-6 xl:px-8')
    html = html.replace('<main class="p-6 md:p-10 w-full">', '<main class="w-full px-4 py-6 md:px-6 md:py-8 xl:px-8">')
    html = html.replace('<div class="mx-auto w-full max-w-[1280px]">', '<div class="mx-auto w-full max-w-[1680px]">')
    html = html.replace('<div class="max-w-[1320px] mx-auto">', '<div class="w-full">')
    # Older templates fetched only ./data/*.json, which breaks when the page is
    # opened from site/, reports/voc_crema/, or a copied standalone HTML file.
    new_fetch = """async function fetchJsonOrThrow(urls){
      const src = Array.isArray(urls) ? urls[0] : urls;
      const candidates = Array.isArray(urls) ? urls.slice() : [urls];
      if (typeof src === "string"){
        if (src.endsWith("/data/meta.json") || src === "./data/meta.json"){
          candidates.push("./site/data/meta.json", "./reports/voc_crema/data/meta.json", "../data/meta.json");
        }else if (src.endsWith("/data/reviews.json") || src === "./data/reviews.json"){
          candidates.push("./site/data/reviews.json", "./reports/voc_crema/data/reviews.json", "../data/reviews.json");
        }
      }
      const uniq = [...new Set(candidates.filter(Boolean))];
      let lastError = null;
      for (const url of uniq){
        try{
          const res = await fetch(url, {cache:"no-store"});
          if (!res.ok){
            throw new Error(`HTTP ${res.status} while loading ${url}`);
          }
          return await res.json();
        }catch(err){
          lastError = err;
        }
      }
      throw lastError || new Error(`Failed to load JSON from candidates: ${uniq.join(", ")}`);
    }"""
    html = re.sub(
        r'async function fetchJsonOrThrow\(\s*url\s*\)\s*\{.*?return await res\.json\(\);\s*\}',
        new_fetch,
        html,
        count=1,
        flags=re.S,
    )
    html = html.replace(
        'dayInput.value = kstDateStr(-1);',
        'dayInput.value = (META?.dashboard_default_day || META?.period_end || kstDateStr(-1));',
    )
    html = html.replace(
        '<div class="text-2xl font-black text-slate-900">어제 리뷰 핵심 문장</div>',
        '<div class="text-2xl font-black text-slate-900" id="signalsTitle">일간 리뷰 핵심 문장</div>',
    )
    html = html.replace(
        '<div class="text-sm font-bold text-slate-500 mt-2">기본: 어제(KST) 기준 긍정/부정 문장 각 2개</div>',
        '<div class="text-sm font-bold text-slate-500 mt-2" id="signalsSubtitle">기본: 선택한 날짜 기준 긍정/부정 문장 각 2개</div>',
    )
    html = html.replace(
        """<div class="flex gap-2 flex-wrap">
                <button class="chip active" id="chip-daily" onclick="toggleChip('daily')">Daily</button>
                <button class="chip" id="chip-low" onclick="toggleChip('low')">Low</button>
              </div>""",
        """<div class="flex gap-2 flex-wrap">
                <button class="chip active" id="chip-daily" onclick="toggleChip('daily')">Daily</button>
                <button class="chip" id="chip-7d" onclick="toggleChip('7d')">7D</button>
                <button class="chip" id="chip-low" onclick="toggleChip('low')">Low</button>
              </div>""",
    )
    html = html.replace(
        '<div class="small-label text-blue-600 mb-2">YESTERDAY</div>',
        '<div class="small-label text-blue-600 mb-2" id="windowLabel">DAILY</div>',
    )
    html = html.replace(
        '<div class="text-xs font-bold text-slate-500 mt-2">어제 업로드된 리뷰 수 (현재 필터 기준)</div>',
        '<div class="text-xs font-bold text-slate-500 mt-2" id="windowCountSub">선택한 날짜 리뷰 수 (현재 필터 기준)</div>',
    )
    html = html.replace(
        '<div class="text-2xl font-black text-slate-900">그날 올라온 리뷰 (업로드 순)</div>',
        '<div class="text-2xl font-black text-slate-900" id="feedTitle">그날 올라온 리뷰 (업로드 순)</div>',
    )
    html = html.replace(
        '<div class="text-sm font-bold text-slate-500 mt-2">기본 날짜: 어제(KST) · 최근 7일 범위 내 선택</div>',
        '<div class="text-sm font-bold text-slate-500 mt-2" id="feedSubtitle">기본 날짜: 선택한 날짜 · 최근 7일 범위 내 선택</div>',
    )
    html = html.replace(
        """const uiState = {
      sourceTab: "combined",
      chips: { daily: true, low: false },
    };""",
        """const uiState = {
      sourceTab: "combined",
      viewMode: "daily",
      chips: { low: false },
    };""",
    )
    html = re.sub(
        r"""function switchSourceTab\(tab\)\s*\{.*?\n    function setSearchAndRender\(q\)\s*\{""",
        """function switchSourceTab(tab){
      uiState.sourceTab = tab;
      document.querySelectorAll(".tab-btn").forEach(b=>{
        b.classList.toggle("active", b.getAttribute("data-tab") === tab);
      });
      renderAll();
    }

    function currentDayValue(){
      return (document.getElementById("daySelect")?.value || META?.dashboard_default_day || META?.period_end || kstDateStr(-1)).trim();
    }

    function syncWindowUI(){
      const is7d = uiState.viewMode === "7d";
      document.getElementById("chip-daily")?.classList.toggle("active", !is7d);
      document.getElementById("chip-7d")?.classList.toggle("active", is7d);
      document.getElementById("chip-low")?.classList.toggle("active", !!uiState.chips.low);

      const dayInput = document.getElementById("daySelect");
      if (dayInput){
        dayInput.disabled = is7d;
        dayInput.classList.toggle("opacity-50", is7d);
        dayInput.classList.toggle("cursor-not-allowed", is7d);
      }
    }

    function updateWindowCopy(){
      const is7d = uiState.viewMode === "7d";
      const signalsTitle = document.getElementById("signalsTitle");
      const signalsSubtitle = document.getElementById("signalsSubtitle");
      const windowLabel = document.getElementById("windowLabel");
      const windowCountSub = document.getElementById("windowCountSub");
      const feedTitle = document.getElementById("feedTitle");
      const feedSubtitle = document.getElementById("feedSubtitle");

      if (signalsTitle) signalsTitle.textContent = is7d ? "최근 7일 리뷰 핵심 문장" : "일간 리뷰 핵심 문장";
      if (signalsSubtitle) signalsSubtitle.textContent = is7d ? "기본: 최근 7일 누적 기준 긍정/부정 문장 각 2개" : "기본: 선택한 날짜 기준 긍정/부정 문장 각 2개";
      if (windowLabel) windowLabel.textContent = is7d ? "7 DAYS" : "DAILY";
      if (windowCountSub) windowCountSub.textContent = is7d ? "최근 7일 누적 리뷰 수 (현재 필터 기준)" : "선택한 날짜 리뷰 수 (현재 필터 기준)";
      if (feedTitle) feedTitle.textContent = is7d ? "최근 7일 누적 리뷰" : "그날 올라온 리뷰 (업로드 순)";
      if (feedSubtitle) feedSubtitle.textContent = is7d ? "기본: 최근 7일 누적 · 제품/옵션/텍스트 필터 적용" : "기본 날짜: 선택한 날짜 · 최근 7일 범위 내 선택";
    }

    function toggleChip(name){
      if (name === "daily" || name === "7d"){
        uiState.viewMode = name;
      }else if (name === "low"){
        uiState.chips.low = !uiState.chips.low;
      }
      syncWindowUI();
      renderAll();
    }

    function setSearchAndRender(q){""",
        html,
        count=1,
        flags=re.S,
    )
    html = re.sub(
        r"""function getFilteredReviews\(\)\s*\{.*?\n      return rows;\n    \}""",
        """function getFilteredReviews(){
      let rows = REVIEWS.slice();

      if (uiState.sourceTab === "official") rows = rows.filter(r => r.source === "Official");
      if (uiState.sourceTab === "naver") rows = rows.filter(r => r.source === "Naver");

      const day = currentDayValue();
      if (uiState.viewMode === "daily" && day){
        rows = rows.filter(r => String(r.created_at || "").slice(0,10) === day);
      }

      const prod = (document.getElementById("productSelect")?.value || "").trim();
      if (prod) rows = rows.filter(r => String(r.product_code || "") === prod);

      const sz = (document.getElementById("sizeSelect")?.value || "").trim();
      if (sz) rows = rows.filter(r => String(r.option_size || "") === sz);

      if (uiState.chips.low){
        rows = rows.filter(r => (Number(r.rating||0) <= 2) || asArr(r.tags).includes("low"));
      }

      const q = (document.getElementById("qInput")?.value || "").trim().toLowerCase();
      if (q){
        rows = rows.filter(r =>
          String(r.text||"").toLowerCase().includes(q) ||
          String(r.product_name||"").toLowerCase().includes(q)
        );
      }

      const sort = document.getElementById("sortSelect")?.value || "upload";
      if (sort === "latest") rows.sort((a,b)=> new Date(b.created_at||0) - new Date(a.created_at||0));
      else if (sort === "long") rows.sort((a,b)=> (String(b.text||"").length - String(a.text||"").length));
      else if (sort === "low") rows.sort((a,b)=> (Number(a.rating||0) - Number(b.rating||0)) || (new Date(b.created_at||0) - new Date(a.created_at||0)));

      return rows;
    }""",
        html,
        count=1,
        flags=re.S,
    )
    html = re.sub(
        r"""function renderDailyFeed\(reviews\)\s*\{.*?\n    \}""",
        """function renderDailyFeed(reviews){
      const container = document.getElementById("dailyFeed");
      const no = document.getElementById("noResults");
      if (!container || !no) return;

      const rows = uiState.viewMode === "7d" ? reviews.slice() : reviews.slice(0, 60);
      if (!rows.length){
        container.innerHTML = "";
        no.classList.remove("hidden");
        return;
      }

      no.classList.add("hidden");
      container.innerHTML = rows.map(reviewCardHTML).join("");
    }""",
        html,
        count=1,
        flags=re.S,
    )
    html = re.sub(
        r"""function renderMLSignals\(allFiltered\)\s*\{.*?\n    \}\n\n    // -----------------------------""",
        """function renderMLSignals(allFiltered){
      const is7d = uiState.viewMode === "7d";
      const day = currentDayValue();
      const baseRows = allFiltered
        .slice()
        .sort((a,b)=> new Date(b.created_at||0) - new Date(a.created_at||0));
      const windowRows = is7d
        ? baseRows
        : baseRows.filter(r => String(r.created_at||"").slice(0,10) === day);
      const count = windowRows.length;

      const yCountEl = document.getElementById("yCount");
      if (yCountEl) yCountEl.textContent = count;

      const lowCnt = windowRows.filter(r => (Number(r.rating||0) <= 2) || asArr(r.tags).includes("low")).length;
      const hint = document.getElementById("yLowHint");
      if (hint){
        hint.textContent = count
          ? `Low/리스크 추정: ${lowCnt}건`
          : (is7d ? "최근 7일 데이터가 없습니다." : "선택한 날짜 데이터가 없습니다.");
      }

      const negEl = document.getElementById("yNegSentences");
      const posEl = document.getElementById("yPosSentences");
      if (negEl) negEl.innerHTML = sentenceListHTML(
        pickTopSentences(windowRows, "neg"),
        "neg",
        is7d ? "최근 7일 부정 문장이 없습니다." : "선택한 날짜 부정 문장이 없습니다.",
      );
      if (posEl) posEl.innerHTML = sentenceListHTML(
        pickTopSentences(windowRows, "pos"),
        "pos",
        is7d ? "최근 7일 긍정 문장이 없습니다." : "선택한 날짜 긍정 문장이 없습니다.",
      );
    }

    // -----------------------------""",
        html,
        count=1,
        flags=re.S,
    )
    html = re.sub(
        r"""function renderAll\(\)\s*\{.*?\n    \}""",
        """function renderAll(){
      syncWindowUI();
      updateWindowCopy();
      renderHeader();
      renderProductSelect();
      renderSizeSelect();

      const filtered = getFilteredReviews();

      renderMLSignals(filtered);
      renderDailyFeed(filtered);

      // Product ML is cumulative (independent of daily/date mode)
      renderProductML();
    }""",
        html,
        count=1,
        flags=re.S,
    )
    return html



def normalize_review_asset_paths(rows: list[dict], report_depth: int = 2) -> list[dict]:
    """Fix asset paths when the report is served under /reports/voc_crema/.

    Your collector (crema_voc / crema_voc.py) usually writes local asset paths like:
      - assets/products/...
      - assets/reviews/...
      - assets/text_images/...

    But this dashboard lives at:
      /reports/voc_crema/index.html

    so relative paths must be prefixed with ../../ to correctly reach /assets/*.
    """
    if not rows:
        return rows
    prefix = "../" * report_depth  # reports/voc_crema => ../../
    fields = ("local_product_image", "local_review_thumb", "text_image_path")
    out = []
    for r in rows:
        rr = dict(r)
        for f in fields:
            p = str(rr.get(f) or "").strip()
            if not p:
                continue
            # strip leading ./ 
            while p.startswith("./"):
                p = p[2:]
            # If it already contains '../' assume caller handled it
            if p.startswith("../"):
                rr[f] = p
                continue
            # Normalize /assets/ to relative as well (GitHub Pages project basepath safe)
            if p.startswith("/assets/"):
                p = p[len("/"):]  # assets/...
            if p.startswith("assets/"):
                rr[f] = prefix + p
            else:
                rr[f] = p
        out.append(rr)
    return out
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

    # ✅ dashboard window must always end at yesterday (KST)
    # so the dashboard header/date does not freeze on an old review date.
    target_days = max(1, int(target_days))
    today_kst = now.date()
    endN = today_kst - timedelta(days=1)
    startN = endN - timedelta(days=target_days - 1)

    # ✅ keep a separate long lookback for trend/topic/product mindmap features
    end1y = endN
    start1y = end1y - timedelta(days=364)

    rowsN: List[Dict[str, Any]] = []
    rows1y: List[Dict[str, Any]] = []
    parse_fail_rows = 0

    latest_input_dt: Optional[datetime] = None
    latest_window_dt: Optional[datetime] = None

    for r in all_rows:
        dt = parse_created_at_iso(str(r.get("created_at") or ""))
        if dt is None:
            parse_fail_rows += 1
            continue

        if latest_input_dt is None or dt > latest_input_dt:
            latest_input_dt = dt

        if in_date_range_kst(dt, startN, endN):
            rowsN.append(r)
            if latest_window_dt is None or dt > latest_window_dt:
                latest_window_dt = dt

        if in_date_range_kst(dt, start1y, end1y):
            rows1y.append(r)

    dfN = pd.DataFrame(rowsN).copy() if rowsN else pd.DataFrame(
        columns=["id", "product_code", "product_name", "rating", "created_at", "text", "source", "tags", "option_size", "option_color", "size_direction"]
    )
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
    for r in rowsN:
        if not is_complaint(r):
            continue
        for h in assign_cluster(str(r.get("text") or "")):
            cluster_counts[h] = cluster_counts.get(h, 0) + 1

    size_rows = [r for r in rowsN if isinstance(r.get("tags"), list) and ("size" in r.get("tags"))]
    size_phrases_terms = top_terms(size_rows, topk=10, auto_sw=auto_sw, which="neg") if size_rows else []
    size_phrases = [k for k, _ in size_phrases_terms]

    product_mindmap_3m = (
        build_product_mindmap_3m_sentence(
            rows_1y=rows1y,
            rows_7d=rowsN,
            per_side=MINDMAP_SENT_PER_SIDE,
            max_products=MINDMAP_MAX_PRODUCTS,
        )
        if rows1y
        else []
    )

    trend_window = []
    if rowsN:
        tmp = []
        for r in rowsN:
            dt = parse_created_at_iso(str(r.get("created_at") or ""))
            if not dt:
                continue
            d = dt.date().isoformat()
            tmp.append((d, 1, 1 if is_complaint(r) else 0))
        dfT = pd.DataFrame(tmp, columns=["day", "cnt", "neg"])
        g = dfT.groupby("day", as_index=False).sum()
        g["neg_rate"] = (g["neg"] / g["cnt"]).fillna(0.0)
        trend_window = g.sort_values("day").to_dict(orient="records")

    trend_3m = build_daily_timeseries(rows1y) if rows1y else []
    ml_topics = ml_topics_tfidf_nmf(rows1y, n_topics=8, top_words=8, min_df=3) if rows1y else {"enabled": False, "reason": "no_1y_rows"}
    dl_topics = dl_embeddings_cluster(rows1y, n_clusters=10) if rows1y else {"enabled": False, "reason": "no_1y_rows"}
    kw_graph = build_kw_graph(rowsN, auto_sw, max_nodes=28) if rowsN else {"window": "empty", "nodes": [], "links": []}

    period_text = f"최근 {target_days}일 ({startN.isoformat()} ~ {endN.isoformat()})"

    empty_reason = None
    if not rowsN:
        empty_reason = "No reviews in target window. Check upstream collection, created_at format/timezone, or TARGET_DAYS."

    meta: Dict[str, Any] = {
        "version": "v6.2-3m-ml-hotfix1",
        "updated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "build_date": today_kst.isoformat(),
        "build_date_kst": today_kst.isoformat(),
        "dashboard_default_day": endN.isoformat(),
        "period_text": period_text,
        "period_start": startN.isoformat(),
        "period_end": endN.isoformat(),
        "target_days": int(target_days),
        "total_reviews": int(len(dfN)),
        "input_total_reviews": int(len(all_rows)),
        "latest_input_created_at": latest_input_dt.isoformat() if latest_input_dt else None,
        "latest_window_created_at": latest_window_dt.isoformat() if latest_window_dt else None,
        "pos_top5": pos_top5,
        "neg_top5": neg_top5,
        "keyword_evidence": {"pos": pos_evidence, "neg": neg_evidence},
        "auto_stopwords": auto_sw,
        "clusters": cluster_counts,
        "size_phrases": size_phrases,
        "kw_graph_3m": kw_graph,
        "fit_words": ["정사이즈", "한치수 크게", "한치수 작게", "타이트", "넉넉", "기장", "소매", "어깨", "가슴", "발볼", "수납", "가벼움"],
        "product_mindmap_3m": product_mindmap_3m,
        "window_created_at_parse_fail": parse_fail_rows,
        "trend_daily": trend_window,
        "trend_daily_3m": trend_3m,
        "ml_topics_3m": ml_topics,
        "dl_topics_3m": dl_topics,
        "empty_reason": empty_reason,
    }

    if debug:
        meta["debug"] = {
            "input_path": str(inp),
            "input_total_rows": len(all_rows),
            "window_rows": len(rowsN),
            "rows_1y": len(rows1y),
            "output_tz": OUTPUT_TZ,
            "build_now_kst_iso": now.isoformat(),
            "window_start": startN.isoformat(),
            "window_end": endN.isoformat(),
            "latest_input_created_at": latest_input_dt.isoformat() if latest_input_dt else None,
            "latest_window_created_at": latest_window_dt.isoformat() if latest_window_dt else None,
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

    # ✅ Fix asset paths for /reports/voc_crema/
    out_reviews = normalize_review_asset_paths(out_reviews, report_depth=2)

    html = load_html_template(html_template)
    html = normalize_template_paths(html)

    (SITE_DATA_DIR / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    (SITE_DATA_DIR / "reviews.json").write_text(json.dumps({"reviews": out_reviews}, ensure_ascii=False, indent=2), encoding="utf-8")
    (SITE_DIR / "index.html").write_text(html, encoding="utf-8")

    print("[OK] Build done (v6.2 hotfix: yesterday window + fresh build timestamp)")
    print(f"- Input: {inp}")
    print(f"- Output meta: {SITE_DATA_DIR / 'meta.json'}")
    print(f"- Output reviews: {SITE_DATA_DIR / 'reviews.json'}")
    print(f"- Output html: {SITE_DIR / 'index.html'}")
    print(f"- Build date (KST): {today_kst.isoformat()}")
    print(f"- Dashboard default day: {endN.isoformat()}")
    print(f"- Period: {period_text}")
    print(f"- Window rows: {len(rowsN)} / Input rows: {len(all_rows)}")
    print(f"- Latest input created_at: {latest_input_dt.isoformat() if latest_input_dt else 'None'}")
    print(f"- Latest window created_at: {latest_window_dt.isoformat() if latest_window_dt else 'None'}")
    if empty_reason:
        print(f"[WARN] {empty_reason}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="path to reviews.json (aggregated JSON: {reviews:[...]})")
    ap.add_argument("--html-template", default="", help="optional html template path (highest priority)")
    ap.add_argument("--target-days", type=int, default=DEFAULT_TARGET_DAYS, help="window days ending yesterday (KST), default from env TARGET_DAYS")
    ap.add_argument("--debug", action="store_true", help="write extra diagnostics into meta.json")
    args = ap.parse_args()

    main(
        input_path=args.input,
        html_template=(args.html_template.strip() or None),
        target_days=int(args.target_days or DEFAULT_TARGET_DAYS),
        debug=bool(args.debug),
    )
