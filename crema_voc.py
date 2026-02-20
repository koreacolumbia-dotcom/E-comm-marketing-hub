#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
[BUILD VOC DASHBOARD FROM JSON | v5.0]
- 입력: reviews.json ({"reviews":[...]}), created_at ISO 권장
- 출력:
  - site/data/reviews.json  (최근 7일로 필터된 리뷰만 + pos/neg_keywords 포함)
  - site/data/meta.json     (기간/키워드/근거/evidence/제품별 1y 문장맵 + HTML 호환 alias 포함)
  - site/index.html         (site/template.html 기반으로 JS 패치 + 클릭/이동 동작 안정화)

v5 핵심 변경
1) meta.json 키 구조를 프론트와 100% 맞춤 + 과거/현재 키 모두 제공(호환 alias)
2) 키워드 칩 클릭 -> evidence에서 review id 찾아 스크롤(실제 동작)
3) 리뷰 카드 클릭 -> 공식몰 상품페이지(리뷰 탭/앵커) 새창 이동(가능한 범위에서)
4) 섹션4: 키워드가 아니라 "대표 문장(스니펫)"으로 렌더링
5) 랭킹 '주요 키워드' 뜨도록 reviews.json에 pos_keywords/neg_keywords 생성
6) scrollToReview / tryScrollToReview id prefix 충돌 제거(review-로 통일)
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

# 키워드 추출 파라미터
TOPK_POS = 5
TOPK_NEG = 5
TOPK_COMPLAINT = 5

# 클러스터(원인) 정의
CLUSTERS = {
    "size": ["사이즈", "정사이즈", "작", "크", "타이트", "헐렁", "끼", "기장", "소매", "어깨", "가슴", "발볼", "핏"],
    "quality": ["품질", "불량", "하자", "찢", "구멍", "실밥", "오염", "변색", "냄새", "마감", "내구", "퀄리티"],
    "shipping": ["배송", "택배", "출고", "도착", "지연", "늦", "빠르", "포장", "파손"],
    "cs": ["문의", "응대", "고객", "cs", "교환", "반품", "환불", "처리", "as"],
    "price": ["가격", "비싸", "싸", "가성비", "할인", "쿠폰", "대비"],
    "design": ["디자인", "색", "컬러", "예쁘", "멋", "스타일", "핏감"],
    "function": ["수납", "넣", "포켓", "공간", "가볍", "무게", "따뜻", "보온", "방수", "기능", "편하", "착용감", "그립"],
}

# 제품군(카테고리) 간이 규칙
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
구매 구입 구입해 구매해 샀어요 샀습니다 주문 주문했 주문해
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
보기 많이 잘했어요 잘했음
"""
    .split()
)

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
        if t in ("있", "좋", "하", "되", "같", "되었", "했", "해", "함"):
            continue
        out.append(t)
    return out


# ----------------------------
# Tags / complaint / sentiment
# ----------------------------
SIZE_KEYWORDS = [
    "사이즈", "정사이즈", "작아요", "작다", "커요", "크다", "핏", "타이트", "여유",
    "끼", "기장", "소매", "어깨", "가슴", "발볼", "헐렁", "오버"
]
REQ_KEYWORDS = ["개선", "아쉬", "불편", "했으면", "추가", "보완", "수정", "필요", "요청", "교환", "반품", "환불"]
COMPLAINT_HINTS = ["불량", "하자", "찢", "구멍", "냄새", "변색", "오염", "실밥", "품질", "엉망", "별로", "최악", "실망", "문제", "불편"]

POS_SEEDS = [
    "가볍", "무게", "편하", "편안", "착용감", "수납", "포켓", "공간", "넣", "따뜻", "보온", "방수",
    "튼튼", "견고", "퀄리티", "만족", "예쁘", "멋", "좋", "좋아", "잘", "잘맞", "딱", "깔끔", "마감", "쿠션"
]
NEG_SEEDS = [
    "불량", "하자", "찢", "구멍", "냄새", "변색", "오염", "실밥", "최악", "별로", "실망", "불편", "문제", "아쉽",
    "작", "크", "타이트", "헐렁", "지연", "늦", "파손", "응대", "환불", "교환", "반품"
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

    def build_stats(sub_rows: List[Dict[str, Any]]) -> Tuple[Dict[str, int], Dict[str, int], Dict[str, int]]:
        freq: Dict[str, int] = {}
        doc_set: Dict[str, set] = {}
        prod_set: Dict[str, set] = {}
        for r in sub_rows:
            pid = str(r.get("id"))
            pcode = str(r.get("product_code") or "-")
            toks_u = set(tokenize_ko(str(r.get("text") or ""), stopwords=BASE_STOPWORDS))
            for t in toks_u:
                doc_set.setdefault(t, set()).add(pid)
                prod_set.setdefault(t, set()).add(pcode)
            for t in tokenize_ko(str(r.get("text") or ""), stopwords=BASE_STOPWORDS):
                freq[t] = freq.get(t, 0) + 1
        df_cnt = {k: len(v) for k, v in doc_set.items()}
        prod_cnt = {k: len(v) for k, v in prod_set.items()}
        return freq, prod_cnt, df_cnt

    pos_freq, _, pos_df = build_stats(pos_rows)
    neg_freq, _, neg_df = build_stats(neg_rows)

    all_ids = set(str(r.get("id")) for r in rows)
    all_n = max(1, len(all_ids))

    keys = set(pos_df.keys()) | set(neg_df.keys())
    # 정확한 제품 분산은 별도 계산
    prod_df_map: Dict[str, set] = {}
    for r in rows:
        pcode = str(r.get("product_code") or "-")
        toks_u = set(tokenize_ko(str(r.get("text") or ""), stopwords=BASE_STOPWORDS))
        for t in toks_u:
            prod_df_map.setdefault(t, set()).add(pcode)
    prod_cnt_map = {k: len(v) for k, v in prod_df_map.items()}

    all_df = {k: (pos_df.get(k, 0) + neg_df.get(k, 0)) for k in keys}

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

        token_docs: Dict[str, set] = {}
        token_prods: Dict[str, set] = {}
        token_pos: Dict[str, int] = {}
        token_neg: Dict[str, int] = {}

        for r in group:
            pid = str(r.get("id"))
            pcode = str(r.get("product_code") or "-")
            ispos = is_positive(r)
            isneg = is_complaint(r)
            toks_u = set(tokenize_ko(str(r.get("text") or ""), stopwords=BASE_STOPWORDS))
            toks_a = tokenize_ko(str(r.get("text") or ""), stopwords=BASE_STOPWORDS)

            for t in toks_u:
                token_docs.setdefault(t, set()).add(pid)
                token_prods.setdefault(t, set()).add(pcode)
            for t in toks_a:
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
# Keyword extraction
# ----------------------------
def top_terms(rows: List[Dict[str, Any]], topk: int, auto_sw: Dict[str, Any], which: str) -> List[Tuple[str, int]]:
    freq: Dict[str, int] = {}
    for r in rows:
        if which == "pos" and not is_positive(r):
            continue
        if which in ("neg", "complaint") and not is_complaint(r):
            continue

        sw = build_stopwords_for_row(r, auto_sw)
        text = str(r.get("text") or "")
        toks = tokenize_ko(text, stopwords=sw)

        if which == "complaint":
            for t in toks:
                # complaint에서는 POS 씨앗은 제외
                if any(seed in t for seed in POS_SEEDS):
                    continue
                w = 1
                if any(seed in t for seed in NEG_SEEDS):
                    w += 1
                if any(any(k in t for k in kws) for kws in CLUSTERS.values()):
                    w += 1
                freq[t] = freq.get(t, 0) + w
        else:
            for t in toks:
                if which == "pos" and any(seed in t for seed in NEG_SEEDS):
                    continue
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
    evidence_per_kw: int = 4,
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
# Product 1y map -> "대표 문장" 중심
# ----------------------------
def build_product_sentence_map_1y(
    all_rows_1y: List[Dict[str, Any]],
    last7_rows: List[Dict[str, Any]],
    auto_sw_last7: Dict[str, Any],
    per_side_topk: int = 6,
) -> List[Dict[str, Any]]:
    by_prod: Dict[str, List[Dict[str, Any]]] = {}
    for r in all_rows_1y:
        code = str(r.get("product_code") or "-")
        by_prod.setdefault(code, []).append(r)

    last7_by_prod: Dict[str, List[Dict[str, Any]]] = {}
    for r in last7_rows:
        code = str(r.get("product_code") or "-")
        last7_by_prod.setdefault(code, []).append(r)

    out: List[Dict[str, Any]] = []
    for code, rows in by_prod.items():
        sample = rows[0]
        pname = str(sample.get("product_name") or code)
        img = str(sample.get("local_product_image") or "")
        pos_terms = top_terms(rows, per_side_topk, auto_sw_last7, which="pos")
        neg_terms = top_terms(rows, per_side_topk, auto_sw_last7, which="neg")
        pos_keys = [k for k, _ in pos_terms]
        neg_keys = [k for k, _ in neg_terms]

        l7 = last7_by_prod.get(code, [])
        pos_evi = build_keyword_evidence(l7, pos_keys, auto_sw_last7, filter_fn=is_positive, evidence_per_kw=2)
        neg_evi = build_keyword_evidence(l7, neg_keys, auto_sw_last7, filter_fn=is_complaint, evidence_per_kw=2)

        def pick_sentences(evi_map: Dict[str, List[Dict[str, Any]]], max_sents: int = 2) -> List[Dict[str, Any]]:
            sents: List[Dict[str, Any]] = []
            seen = set()
            for _, evs in evi_map.items():
                for ev in evs:
                    sn = (ev.get("text_snip") or "").strip()
                    if not sn or sn in seen:
                        continue
                    seen.add(sn)
                    sents.append({"id": ev.get("id"), "snip": sn, "rating": ev.get("rating"), "created_at": ev.get("created_at")})
                    if len(sents) >= max_sents:
                        return sents
            return sents

        pos_sents = pick_sentences(pos_evi, 2)
        neg_sents = pick_sentences(neg_evi, 2)

        out.append(
            {
                "product_code": code,
                "product_name": pname,
                "local_product_image": img,
                "reviews_1y": len(rows),
                # 기존 키워드도 유지(필요 시 툴팁/필터용)
                "pos": [[k, int(c)] for k, c in pos_terms],
                "neg": [[k, int(c)] for k, c in neg_terms],
                "pos_evidence": pos_evi,
                "neg_evidence": neg_evi,
                # ✅ UI용 대표 문장
                "pos_sentences": pos_sents,
                "neg_sentences": neg_sents,
            }
        )

    out.sort(key=lambda x: x.get("reviews_1y", 0), reverse=True)
    return out[:24]


# ----------------------------
# HTML template + JS patch
# ----------------------------
def load_html_template() -> str:
    tpl = SITE_DIR / "template.html"
    if tpl.exists():
        return tpl.read_text(encoding="utf-8")
    idx = SITE_DIR / "index.html"
    if idx.exists():
        return idx.read_text(encoding="utf-8")
    return "<!doctype html><html><head><meta charset='utf-8'><title>VOC</title></head><body><h1>VOC</h1></body></html>"


def patch_html_v5(html: str) -> str:
    """
    너가 준 index.html 구조 기준으로 "JS만" 안정화 패치:
    - META 키 매핑: keywords_top5/keyword_evidence/product_keyword_map_1y 사용
    - 섹션4: 키워드칩 대신 대표 문장 렌더
    - 리뷰카드 클릭: review_page_url 새창
    - scroll id prefix: review- 로 통일 + 중복 scrollToReview 제거
    """

    # 1) 맨 아래 중복 scrollToReview 제거(네가 붙여준 코드 끝부분)
    html = re.sub(r"\n\s*function scrollToReview\(id\)\{[\s\S]*?\}\s*\n\s*</script>", "\n</script>", html, flags=re.S)

    # 2) reviewCardHTML에 카드 클릭 -> 새창 이동 로직 삽입
    #    (너 코드에서 return 템플릿 시작 부분을 찾아 onclick 넣기)
    html = html.replace(
        '<div class="review-card" id="review-${esc(r.id)}">',
        '<div class="review-card cursor-pointer" id="review-${esc(r.id)}" onclick="openReviewPage(\'${esc(r.review_page_url || r.product_url || \'\')}\')">'
    )
    # 혹시 기존에 id="review-${id}"가 없는 버전이면 방어적으로 추가
    if 'id="review-${esc(r.id)}"' not in html:
        html = html.replace(
            '<div class="review-card"',
            '<div class="review-card cursor-pointer" id="review-${esc(r.id)}"'
        )

    # 3) openReviewPage 함수 추가(키워드 클릭 이벤트랑 충돌 방지: stopPropagation 필요)
    inject_open = """
    function openReviewPage(url){
      if(!url) return;
      window.open(url, "_blank", "noopener,noreferrer");
    }
    """
    if "function openReviewPage" not in html:
        html = html.replace("function renderAll(){", inject_open + "\n    function renderAll(){")

    # 4) 키워드 칩 클릭이 카드 클릭으로 먹히지 않도록 stopPropagation
    #    (kw-chip onclick을 직접 막기는 어려우니, 공통적으로 kw-chip에 이벤트 위임)
    inject_stop = """
    document.addEventListener("click", (e) => {
      const t = e.target;
      if(!t) return;
      // 키워드 칩/링크 클릭 시 카드 onclick 전파 방지
      if (t.closest && (t.closest(".kw-chip") || t.closest("a"))) {
        e.stopPropagation();
      }
    }, true);
    """
    if "키워드 칩/링크 클릭 시 카드 onclick 전파 방지" not in html:
        html = html.replace("document.addEventListener(\"DOMContentLoaded\", boot);", "document.addEventListener(\"DOMContentLoaded\", boot);\n" + inject_stop)

    # 5) renderSummary의 META 키 수정 (neg_top5/pos_top5 -> keywords_top5 + evidence)
    #    너가 준 HTML에서 renderSummary 내부를 안전하게 교체
    render_summary_rx = re.compile(r"function renderSummary\(metrics\)\{[\s\S]*?\n\s*\}", re.S)

    render_summary_new = r"""
    function renderSummary(metrics){
      document.getElementById("sizeMentionRate").textContent = metrics.sizeMentionRate;

      // ✅ meta.json 기준 키
      const neg = (META?.keywords_top5?.neg || []);
      const pos = (META?.keywords_top5?.pos || []);
      const ev = (META?.keyword_evidence || {});

      // 칩 클릭 -> (1) 검색어 세팅 (2) 근거 리뷰 id로 스크롤
      const mkChip = (k,c,mode) => {
        const cls = (mode==="pos") ? "badge pos kw-chip" : "badge neg kw-chip";
        const evi = (mode==="pos") ? (ev.pos?.[k] || []) : (ev.neg?.[k] || []);
        const rid = (evi[0]?.id ?? null);
        const title = (evi[0]?.text_snip ?? "");
        const onclick = rid!=null ? `onclick="applyKeywordAndScroll('${esc(k)}','${esc(rid)}')"` : `onclick="applyKeywordAndScroll('${esc(k)}','')"` ;
        return `<span class="${cls}" title="${esc(title)}" ${onclick}>#${esc(k)} <span class="opacity-70">${esc(c)}</span></span>`;
      };

      const elNeg = document.getElementById("topNeg");
      elNeg.innerHTML = neg.map(([k,c]) => mkChip(k,c,"neg")).join("");

      const elPos = document.getElementById("topPos");
      elPos.innerHTML = pos.map(([k,c]) => mkChip(k,c,"pos")).join("");

      const top3 = metrics.rankSize.slice(0,3);
      const ol = document.getElementById("priorityTop3");
      ol.innerHTML = top3.map(r => `
        <li class="flex items-center justify-between gap-3">
          <span class="font-black text-slate-900">${esc(r.product_name)}</span>
          <span class="badge size">Size ${r.sizeRate}%</span>
        </li>
      `).join("");
    }
    """
    html = render_summary_rx.sub(render_summary_new.strip(), html, count=1)

    # 6) renderProductMindmap: meta.product_keyword_map_1y 기반 + "대표 문장" 렌더
    render_mm_rx = re.compile(r"function renderProductMindmap\(\)\{[\s\S]*?\n\s*\}", re.S)
    render_mm_new = r"""
    function renderProductMindmap(){
      const mm = (META?.product_keyword_map_1y || []);
      const root = document.getElementById("productMindmap");
      if (!mm.length){
        root.innerHTML = `<div class="summary-card lg:col-span-3"><div class="text-sm font-black text-slate-700">마인드맵 데이터가 없습니다.</div></div>`;
        return;
      }

      const mkSent = (label, s) => {
        if(!s) return '';
        const rid = (s.id ?? null);
        const onclick = rid!=null ? `onclick="applyKeywordAndScroll('', '${esc(rid)}')"` : '';
        return `
          <div class="mt-2">
            <div class="text-[11px] font-black text-slate-500">${esc(label)} · ★ ${esc(s.rating ?? '')} · ${esc(fmtDT(s.created_at || ''))}</div>
            <div class="mt-1 text-sm font-extrabold text-slate-800 leading-relaxed break-words cursor-pointer" ${onclick}>${esc(s.snip || '')}</div>
          </div>
        `;
      };

      root.innerHTML = mm.map(p => {
        const img = p.local_product_image
          ? `<img src="${esc(p.local_product_image)}" alt="" class="w-14 h-14 rounded-2xl object-cover border border-white/80 bg-white/60" />`
          : `<div class="w-14 h-14 rounded-2xl flex items-center justify-center text-[10px] text-slate-400 border border-white/80 bg-white/60">NO IMAGE</div>`;

        const posS = (p.pos_sentences || []);
        const negS = (p.neg_sentences || []);

        const posBlock = posS.length
          ? posS.map(s => mkSent("POS", s)).join("")
          : `<div class="text-xs font-bold text-slate-400 mt-2">POS 문장 없음</div>`;

        const negBlock = negS.length
          ? negS.map(s => mkSent("NEG", s)).join("")
          : `<div class="text-xs font-bold text-slate-400 mt-2">NEG 문장 없음</div>`;

        return `
          <div class="summary-card">
            <div class="flex items-start gap-3">
              ${img}
              <div class="min-w-0">
                <div class="font-black text-slate-900 line-clamp-2">${esc(p.product_name || p.product_code)}</div>
                <div class="text-[11px] font-black text-slate-500 mt-1">code: ${esc(p.product_code)} · 1y reviews: ${esc(p.reviews_1y)}</div>
              </div>
            </div>
            <div class="mt-4">${posBlock}</div>
            <div class="mt-4">${negBlock}</div>
          </div>
        `;
      }).join("");
    }
    """
    html = render_mm_rx.sub(render_mm_new.strip(), html, count=1)

    # 7) 스크롤 타겟 통일: review-{id}
    #    (tryScrollToReview에서 찾는 id와 맞추기)
    html = html.replace('document.getElementById("review-" + String(id));', 'document.getElementById("review-" + String(id));')

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


def make_review_page_url(product_url: str) -> str:
    """
    공식몰 '리뷰 있는 페이지'는 사이트마다 다름.
    확실한 전용 URL이 없으면 상품페이지 + #review로 유도.
    """
    u = (product_url or "").strip()
    if not u:
        return ""
    if "#review" in u:
        return u
    # 보수적으로 리뷰 앵커 추가
    return u + ("#review" if "#" not in u else "")


def build_review_keywords_for_row(r: Dict[str, Any], auto_sw: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    sw = build_stopwords_for_row(r, auto_sw)
    toks = tokenize_ko(str(r.get("text") or ""), stopwords=sw)
    # 짧은 상위 토큰 몇 개만
    # pos/neg 분리: row 라벨 기준
    if is_positive(r):
        # pos: neg seed 제거
        pos = []
        for t in toks:
            if any(seed in t for seed in NEG_SEEDS):
                continue
            pos.append(t)
        return list(dict.fromkeys(pos))[:6], []
    if is_complaint(r):
        neg = []
        for t in toks:
            if any(seed in t for seed in POS_SEEDS):
                continue
            neg.append(t)
        return [], list(dict.fromkeys(neg))[:6]
    return [], []


# ----------------------------
# Main
# ----------------------------
def main(input_path: str):
    inp = pathlib.Path(input_path).expanduser().resolve()
    if not inp.exists():
        raise FileNotFoundError(f"input not found: {inp}")

    all_rows = read_reviews_json(inp)
    now = now_kst()

    start7 = (now - timedelta(days=6)).date()
    end7 = now.date()

    start1y = (now - timedelta(days=365)).date()
    end1y = now.date()

    rows7, rows1y = [], []
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

    auto_sw = learn_auto_stopwords(rows7)

    pos_top = top_terms(rows7, TOPK_POS, auto_sw, which="pos")
    neg_top = top_terms(rows7, TOPK_NEG, auto_sw, which="neg")
    complaint_top = top_terms(rows7, TOPK_COMPLAINT, auto_sw, which="complaint")

    pos_keys = [k for k, _ in pos_top]
    neg_keys = [k for k, _ in neg_top]
    complaint_keys = [k for k, _ in complaint_top]

    pos_evidence = build_keyword_evidence(rows7, pos_keys, auto_sw, filter_fn=is_positive, evidence_per_kw=3)
    neg_evidence = build_keyword_evidence(rows7, neg_keys, auto_sw, filter_fn=is_complaint, evidence_per_kw=3)
    complaint_evidence = build_keyword_evidence(rows7, complaint_keys, auto_sw, filter_fn=is_complaint, evidence_per_kw=3)

    cluster_counts: Dict[str, int] = {k: 0 for k in CLUSTERS.keys()}
    for r in rows7:
        if not is_complaint(r):
            continue
        for h in assign_cluster(str(r.get("text") or "")):
            cluster_counts[h] = cluster_counts.get(h, 0) + 1

    size_rows = [r for r in rows7 if isinstance(r.get("tags"), list) and ("size" in r.get("tags"))]
    size_phrases = [k for k, _ in top_terms(size_rows, topk=10, auto_sw=auto_sw, which="neg")]

    product_map_1y = build_product_sentence_map_1y(rows1y, rows7, auto_sw_last7=auto_sw, per_side_topk=6)

    period_text = f"최근 7일 ({start7.isoformat()} ~ {end7.isoformat()})"

    # ✅ meta (프론트 호환 + alias까지 같이 제공)
    meta = {
        "updated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "period_text": period_text,
        "period_start": start7.isoformat(),
        "period_end": end7.isoformat(),
        "total_reviews": int(len(df7)),

        # 최신 구조(권장)
        "keywords_top5": {
            "pos": [[k, int(c)] for k, c in pos_top],
            "neg": [[k, int(c)] for k, c in neg_top],
        },
        "complaint_top5": [[k, int(c)] for k, c in complaint_top],
        "keyword_evidence": {
            "pos": pos_evidence,
            "neg": neg_evidence,
            "complaint": complaint_evidence,
        },
        "auto_stopwords": auto_sw,
        "clusters": cluster_counts,
        "size_phrases": size_phrases,
        "fit_words": ["정사이즈", "한치수 크게", "한치수 작게", "타이트", "넉넉", "기장", "소매", "어깨", "가슴", "발볼", "수납", "가벼움"],

        # ✅ 섹션4용
        "product_keyword_map_1y": product_map_1y,

        # ---- 호환 alias (혹시 예전 JS가 참조해도 동작하도록) ----
        "pos_top5": [[k, int(c)] for k, c in pos_top],
        "neg_top5": [[k, int(c)] for k, c in neg_top],
        "product_mindmap_1y": product_map_1y,
    }

    # ✅ 출력 reviews.json(최근7일만) + pos_keywords/neg_keywords + review_page_url
    out_reviews = []
    for r in df7.to_dict(orient="records"):
        tags = r.get("tags", [])
        if not isinstance(tags, list):
            tags = []
        if is_positive(r) and "pos" not in tags:
            tags.append("pos")

        pos_kws, neg_kws = build_review_keywords_for_row(r, auto_sw)

        product_url = str(r.get("product_url") or "")
        review_page_url = make_review_page_url(product_url)

        out_reviews.append(
            {
                "id": r.get("id"),
                "product_code": r.get("product_code", ""),
                "product_name": r.get("product_name", ""),
                "product_url": product_url,
                "review_page_url": review_page_url,
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

                # ✅ 랭킹/요약 키워드 렌더용
                "pos_keywords": pos_kws,
                "neg_keywords": neg_kws,
            }
        )

    # ✅ HTML: template.html 읽어서 v5 패치
    html = load_html_template()
    html = patch_html_v5(html)

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
