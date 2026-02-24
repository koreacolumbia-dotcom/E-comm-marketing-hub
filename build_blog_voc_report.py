#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
[NAVER BLOG VOC] Columbia brand monitoring | v5.1 FIXED

Fixes in v5.1
1) ✅ Naver Blog text extraction fixed (mainFrame frame-first, wait selectors)
2) ✅ HARD_EXCLUDE expanded to include "콜롬비아(국가/대학교)" spellings (not only "컬럼비아")
3) ✅ Context gating tightened: generic words like "코디/사이즈/핏" alone no longer pass ambiguous Columbia
4) ✅ VOC intent false positives reduced: removed overly-broad "as" token, handle A/S via patterns
5) ✅ Better fallback classification when body empty (title+snippet), but main goal is to stop body-empty.

Outputs:
- <out_dir>/index.html
- <out_dir>/data/posts.json
- <out_dir>/data/meta.json
- <out_dir>/data/seen_urls.json

Requires env:
- NAVER_CLIENT_ID
- NAVER_CLIENT_SECRET
"""

import argparse
import json
import os
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple, DefaultDict
from urllib.parse import urlencode
from collections import defaultdict

import requests
from dateutil import parser as dtparser
from playwright.sync_api import sync_playwright

try:
    import pandas as pd
except Exception:
    pd = None

KST = timezone(timedelta(hours=9))

# =======================
# Brand rules (v5.1)
# =======================

# strong markers must be "brand-ish" (NOT just 'Columbia' / '콜롬비아')
BRAND_STRONG = [
    "컬럼비아",  # brand KR token (still ambiguous; gated)
    "Columbia Sportswear", "COLUMBIA SPORTSWEAR",
    "columbiakorea", "columbiakorea.co.kr",
    "옴니히트", "omni-heat", "omni heat", "omniheat",
    "옴니위크", "omni-wick", "omni wick", "omniwick",
    "아웃드라이", "outdry",
    "터보다운", "turbo down", "turbodown",
    "인터체인지", "interchange",
    "타이타늄", "titanium",
]

# ✅ Tightened apparel nouns (must-have context for ambiguous "컬럼비아/columbia")
APPAREL_CONTEXT = [
    "자켓", "재킷", "패딩", "다운", "후리스", "플리스", "바람막이", "윈드브레이커",
    "베스트", "조끼", "팬츠", "바지", "레깅스", "티셔츠", "셔츠",
    "모자", "캡", "비니", "장갑", "가방", "백팩",
    "신발", "부츠", "트레킹화", "등산화",
    "방수", "방풍", "발수", "보온", "보냉",
    "등산", "트레킹", "하이킹", "캠핑", "아웃도어",
]

# review-ish evidence (can help gate ambiguous cases, but NOT alone)
REVIEW_EVIDENCE = [
    "후기", "리뷰", "내돈내산", "언박싱",
    "착용", "착샷", "사이즈", "핏", "기장",
    "구매", "구입", "주문", "배송",
    "교환", "반품", "환불", "수선", "A/S", "AS", "고객센터",
    "매장", "공홈", "자사몰",
]

# Hard exclude: very likely non-apparel Columbia meanings
# ✅ expanded: "콜롬비아" spelling + "Colombia" (country)
HARD_EXCLUDE = [
    # University / school
    "컬럼비아대학교", "컬럼비아 대학교", "Columbia University",
    "콜롬비아대학교", "콜롬비아 대학교",
    "Columbia College", "Columbia Law", "컬럼비아 로스쿨",

    # Region / state / DC / river / glacier
    "브리티시컬럼비아", "브리티시 컬럼비아", "British Columbia",
    "District of Columbia", "워싱턴DC", "워싱턴 D.C", "Washington DC",
    "컬럼비아강", "컬럼비아 강", "Columbia River",
    "컬럼비아 빙원", "Columbia Icefield",

    # media
    "컬럼비아 픽처스", "Columbia Pictures",

    # Coffee / cafe / roasting / origin
    "원두", "드립", "핸드드립", "커피", "로스팅", "싱글오리진",
    "수프리모", "게이샤", "디카페인", "브루잉", "필터커피", "카페", "에스프레소",

    # Colombia (country) / economy / travel
    "콜롬비아(국가)", "콜롬비아 여행", "콜롬비아 경제", "콜롬비아 페소", "COP",
    "Colombia", "Bogota", "Bogotá", "보고타",
    "GDP", "국내총생산", "국가별", "세계", "관세", "WTO",
    "여행지", "론리 플래닛", "Lonely Planet",

    # Art / exhibition names frequently mentioning Colombia
    "보테로", "Botero", "전시", "예술의전당", "미술관", "작가", "화가",
]

# Blog classification helpers
REVIEW_MARKERS = [
    "후기", "리뷰", "착용", "착샷", "내돈내산", "구매", "구입", "주문", "언박싱",
    "사이즈", "핏", "기장", "보온", "방수", "방풍", "교환", "반품", "환불", "배송",
    "매장", "온라인", "공홈", "자사몰",
]
INFO_MARKERS = [
    "추천", "가이드", "정리", "순위", "top", "TOP", "비교", "뜻", "기원", "역사", "분석", "뉴스",
    "맛집", "카페", "여행", "전시", "티켓", "얼리버드", "할인티켓",
    "오늘의 커피", "로스팅 일지",
]
FIRST_PERSON = ["저는", "제가", "내가", "저도", "우리", "저희", "사용해", "입어", "샀", "구매했", "구입했", "주문했"]

SKU_PATTERNS = [
    re.compile(r"\bC[0-9A-Z]{6,}\b"),
    re.compile(r"\bYM[0-9A-Z]{4,}\b"),
]

SPONSORED_MARKERS = [
    "광고", "협찬", "체험단", "소정의 원고료", "파트너스", "제공받아", "지원받아",
    "무상 제공", "유료 광고", "포스팅 비용", "원고료", "업체로부터",
]

# =======================
# VOC rules (v5.1)
# =======================

VOC_TAG_RULES = {
    "SIZE_FIT": [
        "사이즈", "작게", "크게", "정사이즈", "핏", "기장", "어깨", "소매", "허리", "엉덩이",
        "슬림", "오버", "루즈", "타이트", "여유", "끼", "불편", "답답",
    ],
    "QUALITY_DEFECT": [
        "불량", "하자", "박음질", "실밥", "찢", "뜯", "올풀", "마감", "지퍼", "고장",
        "누수", "이염", "변색", "냄새", "털빠짐", "보풀", "내구", "스크래치",
    ],
    "DELIVERY": [
        "배송", "지연", "늦", "파손", "누락", "오배송", "포장", "택배", "도착",
        "송장", "출고", "미출고",
    ],
    "PRICE_PROMO": [
        "가격", "비싸", "가성비", "세일", "할인", "쿠폰", "프로모션", "적립", "포인트",
        "최저가", "정가", "가격차",
    ],
    "EXCHANGE_RETURN_AS": [
        "교환", "반품", "환불",
        "A/S", "AS", "수선", "고객센터", "접수",
        "처리", "불친절", "응대", "상담", "연락", "통화",
    ],
    "WARMTH_WATERPROOF": [
        "따뜻", "보온", "한겨울", "추", "한파", "방풍", "방수", "발수", "젖", "비", "눈",
        "습기", "통풍", "땀", "결로",
    ],
}

NEG_SENTIMENT = [
    "별로", "아쉽", "불만", "실망", "후회", "문제", "최악", "짜증", "안되", "안 돼", "안됨",
    "못", "힘들", "불편", "답답", "거슬", "불친절", "느리", "늦", "지연", "하자", "불량",
]

# ✅ removed overly-broad "as" (English) – causes massive false positives
COMPLAINT_INTENT = [
    "교환", "반품", "환불", "수선", "고객센터", "문의", "접수", "클레임",
    "오배송", "누락", "파손", "지연", "하자", "불량", "고장", "누수", "이염",
    "A/S", "AS",
]

POSITIVE_ONLY = [
    "예뻐", "좋아", "만족", "따뜻", "가볍", "편하", "최고", "추천", "재구매",
]

WORD_RE = re.compile(r"[가-힣A-Za-z0-9]{2,}")
STOPWORDS = set("""
그리고 그러나 하지만 그래서 또한 정말 너무 진짜 약간 그냥 조금 매우 완전
제품 사용 구매 후기 리뷰 느낌 생각 정도 경우 부분 이번 오늘 어제 요즘
제가 저는 나는 우리는 여러분 때 좀 것 수 있 있다 없다 해요 했어요 입니다
ㅋㅋ ㅎㅎ ㅠㅠ ㅜㅜ
""".split())

SENT_SPLIT_RE = re.compile(r"(?<=[\.\!\?\n\r])\s+|(?<=다\.)\s+|(?<=요\.)\s+|(?<=니다\.)\s+|(?<=함\.)\s+")

NAVER_NOISE_PATTERNS = [
    "NAVER 블로그", "블로그 검색", "이 블로그에서 검색", "공감", "공유하기", "메뉴", "바로가기",
    "본문 바로가기", "내 블로그", "이웃블로그", "블로그 홈", "로그인", "사용자 링크",
    "프롤로그", "서재", "안부", "이웃추가", "카테고리", "전체보기",
    "목록열기", "새 댓글", "첫 댓글", "댓글",
]

# =======================
# Utils
# =======================

def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def now_kst() -> datetime:
    return datetime.now(tz=KST)

def iso(dt: datetime) -> str:
    return dt.astimezone(KST).isoformat()

def safe_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def strip_html(s: str) -> str:
    return safe_text(re.sub(r"<[^>]+>", "", s or ""))

def fmt_kst(dt: datetime) -> str:
    return dt.astimezone(KST).strftime("%Y.%m.%d %H:%M KST")

def contains_any(text: str, kws: List[str]) -> bool:
    t = text or ""
    return any(k in t for k in kws)

def detect_sponsored(text: str) -> Tuple[bool, List[str]]:
    found = []
    t = text or ""
    for m in SPONSORED_MARKERS:
        if m in t:
            found.append(m)
    return (len(found) > 0), sorted(list(set(found)))

def has_sku(text: str) -> bool:
    t = text or ""
    for rgx in SKU_PATTERNS:
        if rgx.search(t):
            return True
    return False

def split_sentences(text: str) -> List[str]:
    t = (text or "").strip()
    if not t:
        return []
    parts = [p.strip() for p in SENT_SPLIT_RE.split(t) if p and p.strip()]
    out = []
    for p in parts:
        out.extend([x.strip() for x in re.split(r"[\n\r]+", p) if x.strip()])
    return [x for x in out if len(x) >= 8]

def keyword_map_from_texts(texts: List[str], topn: int = 40) -> List[Tuple[str, int]]:
    freq: Dict[str, int] = {}
    for t in texts:
        for w in WORD_RE.findall(t or ""):
            lw = w.lower()
            if lw in STOPWORDS:
                continue
            if len(lw) < 2:
                continue
            if lw in {"naver", "블로그", "바로가기", "메뉴", "공감", "공유하기"}:
                continue
            freq[lw] = freq.get(lw, 0) + 1
    items = sorted(freq.items(), key=lambda x: x[1], reverse=True)
    return items[:topn]

def clean_naver_noise_text(raw: str) -> str:
    if not raw:
        return ""
    lines = [x.strip() for x in re.split(r"[\r\n]+", raw) if x and x.strip()]
    out = []
    for ln in lines:
        if len(ln) <= 2:
            continue
        if any(k in ln for k in NAVER_NOISE_PATTERNS):
            continue
        if len(ln) >= 40 and sum(1 for k in NAVER_NOISE_PATTERNS if k in ln) >= 2:
            continue
        out.append(ln)
    text = " ".join(out)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def normalize_for_match(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"&amp;", "&", s)
    s = re.sub(r"[^a-z0-9가-힣\s]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def tokenize_norm(s: str) -> List[str]:
    s = normalize_for_match(s)
    toks = [t for t in s.split(" ") if len(t) >= 3]
    return toks

# =======================
# Product catalog matching
# =======================

class ProductMatcher:
    def __init__(self):
        self.phrases: List[str] = []
        self.phrase_norm: List[str] = []
        self.token_index: DefaultDict[str, List[int]] = defaultdict(list)

    @staticmethod
    def _should_keep_phrase(p: str) -> bool:
        if not p:
            return False
        p2 = normalize_for_match(p)
        if len(p2) < 6:
            return False
        if len(p2.split()) == 1 and len(p2) < 8:
            return False
        return True

    def load_csv(self, path: str) -> int:
        if not path or not os.path.exists(path):
            return 0
        if pd is None:
            raise RuntimeError("pandas is required to load product csv")
        df = pd.read_csv(path)
        cols = list(df.columns)

        cand_cols = []
        for c in cols:
            if "상품명" in c or "name" in c.lower():
                cand_cols.append(c)
        if not cand_cols:
            cand_cols = cols[:2]

        phrases = []
        for c in cand_cols[:2]:
            for v in df[c].dropna().astype(str).tolist():
                v = safe_text(v)
                if v and v.lower() != "nan":
                    phrases.append(v)

        seen = set()
        for p in phrases:
            if p in seen:
                continue
            seen.add(p)
            if not self._should_keep_phrase(p):
                continue
            idx = len(self.phrases)
            self.phrases.append(p)
            pn = normalize_for_match(p)
            self.phrase_norm.append(pn)

            toks = set(tokenize_norm(p))
            for t in toks:
                if len(t) < 4:
                    continue
                self.token_index[t].append(idx)

        return len(self.phrases)

    def find_matches(self, text: str, max_hits: int = 6) -> List[str]:
        if not self.phrases:
            return []
        norm = normalize_for_match(text)
        toks = set(tokenize_norm(norm))
        cand_ids = set()
        for t in toks:
            if t in self.token_index:
                for idx in self.token_index[t]:
                    cand_ids.add(idx)

        hits = []
        for idx in cand_ids:
            pn = self.phrase_norm[idx]
            if pn and pn in norm:
                hits.append(self.phrases[idx])
                if len(hits) >= max_hits:
                    break
        return hits

# =======================
# Scoring / Classification
# =======================

def score_review_intent(text: str) -> Tuple[int, List[str]]:
    t = text or ""
    hits = []
    score = 0
    for k in REVIEW_MARKERS:
        if k in t:
            score += 7
            hits.append(k)
    if any(fp in t for fp in FIRST_PERSON):
        score += 10
        hits.append("1p")
    if re.search(r"(구매|구입|주문)", t):
        score += 10
        hits.append("buy")
    if re.search(r"(사이즈|핏|기장|착용|착샷)", t):
        score += 10
        hits.append("wear")
    if has_sku(t):
        score += 15
        hits.append("sku")
    return min(100, score), sorted(list(set(hits)))[:12]

def score_info_intent(text: str) -> Tuple[int, List[str]]:
    t = text or ""
    hits = []
    score = 0
    for k in INFO_MARKERS:
        if k in t:
            score += 7
            hits.append(k)
    if re.search(r"\bTOP\s*\d+|\b순위\b|\b랭킹\b", t, flags=re.IGNORECASE):
        score += 10
        hits.append("listicle")
    return min(100, score), sorted(list(set(hits)))[:12]

def classify_content(title: str, snippet: str, body: str) -> Tuple[str, int, int, List[str], List[str]]:
    combined = f"{title}\n{snippet}\n{body}".strip()
    rv, rv_hits = score_review_intent(combined)
    inf, inf_hits = score_info_intent(combined)

    # ✅ lowered thresholds slightly so real reviews don't all become unknown when text is short
    if rv >= 30 and inf <= 45:
        return "purchase_review", rv, inf, rv_hits, inf_hits
    if inf >= 55 and rv <= 35:
        return "info_post", rv, inf, rv_hits, inf_hits
    if rv >= 35 and inf >= 35:
        return "mixed", rv, inf, rv_hits, inf_hits
    return "unknown", rv, inf, rv_hits, inf_hits

def voc_tags_for_text(text: str) -> List[str]:
    t = text or ""
    tags = []
    for tag, kws in VOC_TAG_RULES.items():
        if any(k in t for k in kws):
            tags.append(tag)
    return tags

def _intent_hit_count(combined: str) -> int:
    # handle A/S carefully; avoid substring explosions
    c = combined or ""
    c_low = c.lower()

    hits = 0
    for k in COMPLAINT_INTENT:
        if k in {"AS", "A/S"}:
            if re.search(r"\bA/S\b", c) or re.search(r"\bAS\b", c):
                hits += 1
            continue
        if k.lower() in c_low or k in c:
            hits += 1
    return hits

def voc_score_for_text(title: str, text: str, snippet: str) -> Tuple[int, List[str], List[Dict[str, Any]], Dict[str, Any]]:
    combined = f"{title}\n{snippet}\n{text}"
    tags = voc_tags_for_text(combined)

    score = 0
    tag_weights = {
        "QUALITY_DEFECT": 24,
        "EXCHANGE_RETURN_AS": 18,
        "DELIVERY": 14,
        "SIZE_FIT": 12,
        "WARMTH_WATERPROOF": 10,
        "PRICE_PROMO": 8,
    }
    for t in tags:
        score += tag_weights.get(t, 6)

    neg_hits = [k for k in NEG_SENTIMENT if k in combined]
    intent_count = _intent_hit_count(combined)
    pos_hits = [k for k in POSITIVE_ONLY if k in combined]

    score += min(20, len(neg_hits) * 6)
    score += min(24, intent_count * 6)

    if (len(neg_hits) == 0 and intent_count == 0) and len(pos_hits) >= 1:
        score = min(score, 14)

    hard = ["불량", "하자", "교환", "반품", "환불", "A/S", "AS", "오배송", "누락", "지연", "지퍼", "이염", "누수", "고객센터"]
    hard_hits = sum(1 for k in hard if (k.lower() in combined.lower() or k in combined))
    score += min(22, hard_hits * 5)

    score = max(0, min(120, score))

    highlights = []
    sents = split_sentences(text or snippet or "")
    for s in sents:
        stags = voc_tags_for_text(s)
        ss = 0
        for tt in stags:
            ss += tag_weights.get(tt, 6)

        s_neg = sum(1 for k in NEG_SENTIMENT if k in s)
        s_int = _intent_hit_count(s)
        s_hard = sum(1 for k in hard if k.lower() in s.lower() or k in s)

        ss += min(16, s_neg * 6)
        ss += min(18, s_int * 6)
        ss += min(18, s_hard * 6)

        if ss >= 14:
            highlights.append({"sent": safe_text(s)[:260], "score": int(ss), "tags": stags})

    highlights.sort(key=lambda x: x["score"], reverse=True)
    highlights = highlights[:6]

    dbg = {
        "neg_hits": sorted(list(set(neg_hits)))[:8],
        "intent_count": int(intent_count),
        "pos_hits": sorted(list(set(pos_hits)))[:8],
        "hard_hits": int(hard_hits),
    }
    return int(score), tags, highlights, dbg

def _has_apparel_context(text: str) -> bool:
    t = text or ""
    tl = t.lower()
    return any(k in t for k in APPAREL_CONTEXT) or any(k.lower() in tl for k in APPAREL_CONTEXT)

def _has_review_evidence(text: str) -> bool:
    t = text or ""
    tl = t.lower()
    return any(k in t for k in REVIEW_EVIDENCE) or any(k.lower() in tl for k in REVIEW_EVIDENCE)

def is_brand_columbia(title: str, body: str, snippet: str, pm: Optional[ProductMatcher]) -> Tuple[bool, List[str], str, Dict[str, Any]]:
    title = title or ""
    body = body or ""
    snippet = snippet or ""
    combined = f"{title}\n{snippet}\n{body}"
    combined_l = combined.lower()

    # 1) Hard exclude (very strong non-brand signals)
    if contains_any(combined, HARD_EXCLUDE) or contains_any(combined_l, [x.lower() for x in HARD_EXCLUDE]):
        return False, [], "hard_exclude", {"hit": "hard_exclude"}

    # 2) Product name match = very strong proof
    product_hits = pm.find_matches(combined) if pm else []
    if product_hits:
        return True, ["PRODUCT_NAME"], "product_match", {"product_hits": product_hits[:6]}

    # 3) Strong brand markers (brand-ish)
    strong_found = [m for m in BRAND_STRONG if m and (m in combined or m.lower() in combined_l)]
    strong_found = sorted(list(set(strong_found)))

    ambiguous_only = (len(strong_found) == 1 and strong_found[0] == "컬럼비아")

    # 4) If a SKU exists, accept
    if has_sku(combined):
        return True, ["SKU"], "sku_match", {"sku": True}

    # 5) Context gate (tightened)
    apparel_hit = _has_apparel_context(combined)
    review_hit = _has_review_evidence(combined)

    dbg: Dict[str, Any] = {
        "apparel_hit": bool(apparel_hit),
        "review_hit": bool(review_hit),
        "strong_found": strong_found[:6],
        "ambiguous_only": bool(ambiguous_only),
    }

    if strong_found and not ambiguous_only:
        return True, strong_found[:6], "strong_marker", dbg

    # "컬럼비아" only: require apparel context OR (review score + evidence)
    if ambiguous_only:
        ctype, rv, inf, *_ = classify_content(title, snippet, body)
        dbg["ctype"] = ctype
        dbg["review_score"] = rv
        dbg["info_score"] = inf
        if apparel_hit or (rv >= 30 and review_hit):
            return True, ["context_or_review"], "ambiguous_but_gated", dbg
        return False, [], "ambiguous_no_gate", dbg

    # If 'columbia/콜롬비아/컬럼비아' exists but no strong markers:
    if ("columbia" in combined_l) or ("콜롬비아" in combined) or ("컬럼비아" in combined):
        # ✅ require apparel context to pass (prevents Columbia University fashion posts)
        if apparel_hit:
            return True, ["apparel_context"], "apparel_context", dbg
        return False, [], "no_apparel_context", dbg

    return False, [], "no_brand_token", dbg

def compute_relevance(ok_brand: bool, brand_reason: str, content_type: str, review_score: int, info_score: int,
                      product_hits: List[str], voc_score: int, hard_excluded: bool) -> int:
    if not ok_brand or hard_excluded:
        return 0
    score = 50
    if brand_reason in {"product_match", "sku_match"}:
        score += 35
    if brand_reason in {"strong_marker"}:
        score += 20
    if brand_reason in {"apparel_context", "ambiguous_but_gated"}:
        score += 12

    if product_hits:
        score += 18

    if content_type == "purchase_review":
        score += 18
    elif content_type == "mixed":
        score += 6
    elif content_type == "info_post":
        score -= 10

    score += int(min(20, review_score * 0.15))
    score -= int(min(15, info_score * 0.15))

    if voc_score >= 20:
        score += 8
    if voc_score >= 40:
        score += 8

    return max(0, min(100, score))

# =======================
# Fetch + Scrape
# =======================

def fetch_candidates_via_naver_api(queries: List[str], per_query: int, sort: str = "date") -> List[Dict[str, Any]]:
    cid = os.getenv("NAVER_CLIENT_ID", "").strip()
    csec = os.getenv("NAVER_CLIENT_SECRET", "").strip()
    if not cid or not csec:
        raise RuntimeError("Missing NAVER_CLIENT_ID / NAVER_CLIENT_SECRET env")

    headers = {"X-Naver-Client-Id": cid, "X-Naver-Client-Secret": csec}

    collected: List[Dict[str, Any]] = []
    for q in queries:
        got = 0
        start = 1
        while got < per_query and start <= 1000:
            display = min(100, per_query - got)
            params = {"query": q, "display": display, "start": start, "sort": sort}
            url = "https://openapi.naver.com/v1/search/blog.json?" + urlencode(params)
            r = requests.get(url, headers=headers, timeout=25)
            r.raise_for_status()
            data = r.json()

            items = data.get("items", []) or []
            if not items:
                break

            for it in items:
                link = (it.get("link") or "").strip()
                title = strip_html(it.get("title") or "")
                desc = strip_html(it.get("description") or "")

                if not link.startswith("http"):
                    continue

                collected.append({"query": q, "title": title or "(제목 없음)", "url": link, "snippet": desc})
                got += 1
                if got >= per_query:
                    break
            start += display

    seen = set()
    uniq = []
    for x in collected:
        u = x["url"]
        if u in seen:
            continue
        seen.add(u)
        uniq.append(x)
    return uniq

def extract_meta_content(page_or_frame, selector: str) -> str:
    try:
        el = page_or_frame.query_selector(selector)
        if el:
            c = el.get_attribute("content") or ""
            return safe_text(c)
    except Exception:
        pass
    return ""

def extract_title(page_or_frame) -> str:
    for sel in ["meta[property='og:title']", "meta[name='twitter:title']"]:
        t = extract_meta_content(page_or_frame, sel)
        if t:
            return t
    try:
        return safe_text(page_or_frame.title())
    except Exception:
        return ""

def extract_og_image(page_or_frame) -> str:
    return extract_meta_content(page_or_frame, "meta[property='og:image']")

def extract_site_name(page_or_frame) -> str:
    return extract_meta_content(page_or_frame, "meta[property='og:site_name']") or ""

def extract_author(page_or_frame) -> str:
    for sel in ["meta[name='author']", "meta[property='article:author']"]:
        v = extract_meta_content(page_or_frame, sel)
        if v:
            return v
    try:
        for sel in ["a.nick", "span.nick", "div.blog_name a", "div.blog_name"]:
            el = page_or_frame.query_selector(sel)
            if el:
                v = safe_text(el.inner_text() or "")
                if v and len(v) <= 40:
                    return v
    except Exception:
        pass
    return ""

def extract_published_from_dom(page_or_frame) -> Optional[datetime]:
    selectors = [
        "meta[property='article:published_time']",
        "span.se_publishDate",
        "span#se_publishDate",
        "p.date",
        "span.date",
        "time",
    ]
    for sel in selectors:
        try:
            el = page_or_frame.query_selector(sel)
            if not el:
                continue
            v = el.get_attribute("content") if sel.startswith("meta") else el.inner_text()
            v = safe_text(v or "")
            if not v:
                continue
            try:
                dt = dtparser.parse(v)
                if not dt.tzinfo:
                    dt = dt.replace(tzinfo=KST)
                return dt.astimezone(KST)
            except Exception:
                continue
        except Exception:
            continue
    return None

def extract_engagement_best_effort(page_or_frame) -> Tuple[Optional[int], Optional[int]]:
    like_cnt = None
    cmt_cnt = None
    try:
        body = page_or_frame.inner_text("body", timeout=1800).replace(",", "")
        m = re.search(r"(공감|좋아요)\s*([0-9]{1,6})", body)
        if m:
            like_cnt = int(m.group(2))
        m2 = re.search(r"(댓글)\s*([0-9]{1,6})", body)
        if m2:
            cmt_cnt = int(m2.group(2))
    except Exception:
        pass
    return like_cnt, cmt_cnt

def extract_post_text(page_or_frame) -> str:
    # ✅ Try main SE container first (new editor)
    candidates = [".se-main-container", "#postViewArea", "article", "div#contentArea", "div#post-area"]
    text_chunks: List[str] = []
    for sel in candidates:
        try:
            el = page_or_frame.query_selector(sel)
            if el:
                t = safe_text(el.inner_text(timeout=3500))
                if len(t) >= 80:
                    text_chunks.append(t)
        except Exception:
            pass

    if text_chunks:
        text_chunks.sort(key=len, reverse=True)
        return text_chunks[0]

    try:
        return safe_text(page_or_frame.inner_text("body", timeout=3500))
    except Exception:
        return ""

def scrape_one_post(context, url: str, timeout_ms: int = 30000) -> Dict[str, Any]:
    page = context.new_page()
    page.set_default_timeout(timeout_ms)

    out = {
        "final_url": url,
        "title": "",
        "text": "",
        "pub_dt": None,
        "og_image": "",
        "site_name": "",
        "author": "",
        "like_count": None,
        "comment_count": None,
        "used_frame": False,
    }

    try:
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_timeout(700)

        # ✅ Frame-first extraction (key fix)
        frame = page.frame(name="mainFrame")
        target = frame if frame else page
        out["used_frame"] = bool(frame)

        # Wait for common blog content selectors
        try:
            target.wait_for_selector(".se-main-container, #postViewArea, article", timeout=4500)
        except Exception:
            pass

        out["final_url"] = page.url or url
        out["title"] = extract_title(target)
        out["pub_dt"] = extract_published_from_dom(target)
        out["og_image"] = extract_og_image(target)
        out["site_name"] = extract_site_name(target)
        out["author"] = extract_author(target)

        raw_text = extract_post_text(target)
        out["text"] = clean_naver_noise_text(raw_text)

        like_cnt, cmt_cnt = extract_engagement_best_effort(target)
        out["like_count"] = like_cnt
        out["comment_count"] = cmt_cnt
        return out
    finally:
        try:
            page.close()
        except Exception:
            pass

def load_seen_urls(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {"version": 1, "seen": {}}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if "seen" not in data:
            return {"version": 1, "seen": {}}
        return data
    except Exception:
        return {"version": 1, "seen": {}}

def save_seen_urls(path: str, data: Dict[str, Any]) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def mark_seen(seen_db: Dict[str, Any], url: str, collected_at_iso: str) -> None:
    seen = seen_db.setdefault("seen", {})
    seen[url] = {"last_seen": collected_at_iso}

def is_seen(seen_db: Dict[str, Any], url: str) -> bool:
    return url in (seen_db.get("seen", {}) or {})

# =======================
# Data model
# =======================

@dataclass
class BlogPost:
    query: str
    title: str
    url: str
    published_at: str
    collected_at: str

    is_sponsored: bool
    sponsored_markers: List[str]

    is_columbia_brand: bool
    brand_markers_found: List[str]
    brand_reason: str
    brand_debug: Dict[str, Any]

    product_hits: List[str]

    content_type: str
    review_score: int
    info_score: int
    content_debug: Dict[str, Any]

    relevance_score: int

    blog_site_name: str
    author: str
    og_image: str
    like_count: Optional[int]
    comment_count: Optional[int]

    raw_text_len: int
    excerpt: str
    text_preview: str

    voc_score: int
    voc_tags: List[str]
    voc_highlights: List[Dict[str, Any]]
    voc_debug: Dict[str, Any]

    source: str

# =======================
# HTML (same as your v5)
# =======================

HTML_TEMPLATE = """<!doctype html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Blog VOC | CSK E-COMM</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200;400;600;800&display=swap');
    :root { --brand:#002d72; --bg0:#f6f8fb; --bg1:#eef3f9; }
    body{
      background: linear-gradient(180deg, var(--bg0), var(--bg1));
      font-family: 'Plus Jakarta Sans', sans-serif;
      color:#0f172a;
      min-height:100vh;
    }
    .glass-card{
      background: rgba(255,255,255,0.60);
      backdrop-filter: blur(18px);
      border: 1px solid rgba(255,255,255,0.75);
      border-radius: 28px;
      box-shadow: 0 20px 50px rgba(0,45,114,0.06);
    }
    .feed-card {
      border-radius: 20px;
      background: rgba(255,255,255,0.78);
      border: 1px solid rgba(255,255,255,0.85);
      box-shadow: 0 10px 25px rgba(0,45,114,0.05);
    }
    .muted { color:#64748b; }
    .badge {
      display:inline-flex; align-items:center;
      padding: 3px 10px; border-radius:999px;
      font-size: 11px; font-weight: 900;
      background: rgba(15,23,42,0.06);
      color:#0f172a;
    }
    .badge-ad { background: rgba(234,179,8,0.14); color: rgb(161,98,7); }
    .badge-brand { background: rgba(37,99,235,0.10); color: rgb(30,64,175); }
    .badge-voc { background: rgba(239,68,68,0.10); color: rgb(153,27,27); }
    .badge-type { background: rgba(16,185,129,0.10); color: rgb(4,120,87); }
    .badge-info { background: rgba(148,163,184,0.22); color: rgb(51,65,85); }
    .badge-rel { background: rgba(99,102,241,0.10); color: rgb(49,46,129); }
    .link { color: var(--brand); font-weight: 900; }
    .link:hover { text-decoration: underline; }
    .kbd {
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      font-size: 12px;
      padding: 2px 8px;
      border-radius: 10px;
      background: rgba(15,23,42,0.06);
      color: #334155;
      font-weight: 900;
    }
    input[type="date"]{
      background: rgba(255,255,255,0.75);
      border: 1px solid rgba(255,255,255,0.9);
      border-radius: 16px;
      padding: 10px 12px;
      font-weight: 900;
      color:#0f172a;
    }
    .btn{
      padding: 10px 14px;
      border-radius: 16px;
      font-weight: 900;
      background: rgba(255,255,255,0.75);
      border: 1px solid rgba(255,255,255,0.9);
      transition: all .15s ease;
      white-space: nowrap;
    }
    .btn:hover{ background: #fff; }
    .lineclamp4{
      display: -webkit-box;
      -webkit-line-clamp: 4;
      -webkit-box-orient: vertical;
      overflow: hidden;
    }
    .loading-mask{
      position: fixed; inset: 0;
      background: rgba(248, 250, 252, 0.75);
      backdrop-filter: blur(8px);
      z-index: 9999;
      display: none;
      align-items: center;
      justify-content: center;
      padding: 24px;
    }
    .loading-mask.show{ display:flex; }
    .spinner{
      width: 44px; height: 44px;
      border-radius: 999px;
      border: 4px solid rgba(15,23,42,0.12);
      border-top-color: rgba(0,45,114,0.85);
      animation: spin 0.9s linear infinite;
    }
    @keyframes spin { to { transform: rotate(360deg); } }
  </style>
</head>

<body class="p-4 md:p-8">
  <div class="loading-mask" id="loadingMask">
    <div class="glass-card p-6 flex items-center gap-4">
      <div class="spinner"></div>
      <div>
        <div class="text-sm font-black">필터 적용 중...</div>
        <div class="text-xs font-semibold muted mt-1">잠시만요</div>
      </div>
    </div>
  </div>

  <div class="max-w-7xl mx-auto">
    <div class="glass-card p-6 md:p-8">
      <div class="flex flex-col md:flex-row md:items-end md:justify-between gap-4">
        <div>
          <div class="text-[11px] font-black tracking-[0.3em] text-slate-400 uppercase">CSK E-COMM</div>
          <h1 class="text-3xl md:text-4xl font-black tracking-tight mt-2">
            네이버 블로그 VOC 모니터링 (Columbia)
          </h1>
          <p class="muted font-semibold mt-2">
            <span class="font-black">브랜드/상품명 기반</span>으로 irrelevant를 강하게 제외하고, <span class="font-black">리뷰형 글 중심</span>으로 VOC를 추출합니다.
          </p>
        </div>

        <div class="flex flex-wrap gap-3">
          <div class="glass-card px-4 py-3 rounded-2xl">
            <div class="text-[10px] font-black tracking-widest text-slate-500 uppercase">Update</div>
            <div id="updatedAt" class="text-[12px] font-black text-slate-700 mt-1">-</div>
          </div>
          <div class="glass-card px-4 py-3 rounded-2xl">
            <div class="text-[10px] font-black tracking-widest text-slate-500 uppercase">Collected Period</div>
            <div id="periodText" class="text-[12px] font-black text-slate-700 mt-1">-</div>
          </div>
        </div>
      </div>

      <div class="mt-6 feed-card p-4">
        <div class="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-3">
          <div class="flex flex-wrap items-center gap-3">
            <div class="text-sm font-black">기간</div>
            <input id="dateStart" type="date" />
            <div class="muted font-black">~</div>
            <input id="dateEnd" type="date" />
            <button id="btnApply" class="btn">적용</button>
            <button id="btnReset7" class="btn">최근 7일</button>
            <button id="btnReset60" class="btn">최근 60일</button>

            <div class="ml-2 flex items-center gap-2">
              <span class="text-sm font-black muted">VOC≥</span>
              <input id="vocMin" type="number" min="0" max="120" value="20"
                class="w-20 px-3 py-2 rounded-2xl font-black bg-white/70 border border-white/90" />
              <button id="btnVocOnly" class="btn">VOC만 보기</button>
            </div>
          </div>

          <div class="flex flex-wrap items-center gap-2">
            <button id="btnHideSponsored" class="btn">협찬 숨기기</button>
            <button id="btnReviewOnly" class="btn">리뷰만 보기</button>
            <button id="btnHideIrrelevant" class="btn">irrelevant 숨기기</button>
            <span class="kbd">COL-BLOG-VOC v5.1</span>
          </div>
        </div>

        <div class="mt-3 text-xs font-semibold muted" id="debugLine">-</div>
      </div>
    </div>

    <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4 mt-6" id="summaryCards"></div>

    <div class="grid grid-cols-1 lg:grid-cols-12 gap-6 mt-6">
      <aside class="lg:col-span-4 space-y-6">
        <div class="glass-card p-6">
          <div class="flex items-center justify-between">
            <div class="text-lg font-black">Top VOC</div>
            <div class="kbd">VOC</div>
          </div>
          <div class="mt-4 space-y-3" id="topVocList"></div>
        </div>

        <div class="glass-card p-6">
          <div class="flex items-center justify-between">
            <div class="text-lg font-black">이슈 타입 Top</div>
            <div class="kbd">TAGS</div>
          </div>
          <div class="mt-4 flex flex-wrap gap-2" id="tagChips"></div>
        </div>

        <div class="glass-card p-6">
          <div class="text-lg font-black">Top Words</div>
          <div class="muted text-xs font-semibold mt-1">본문 기반 빈출 단어</div>
          <div class="mt-3 flex flex-wrap gap-2" id="wordChips"></div>
        </div>
      </aside>

      <section class="lg:col-span-8 space-y-6">
        <div class="glass-card p-6">
          <div class="flex items-end justify-between gap-3">
            <div>
              <div class="text-xl font-black">Feed</div>
              <div class="muted text-sm font-semibold mt-1">정렬: 최근 → 과거</div>
            </div>
            <div class="text-sm font-black muted">정렬: <span class="kbd">최근</span></div>
          </div>
          <div class="mt-5 grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4" id="feedGrid"></div>
        </div>

        <div class="glass-card p-6">
          <div class="text-xl font-black">상세</div>
          <div class="muted text-sm font-semibold mt-1">VOC 하이라이트 + 분류/메타</div>
          <div class="mt-5 space-y-4" id="detailList"></div>
        </div>
      </section>
    </div>
  </div>

<script>
  const META = __META_JSON__;
  const POSTS = __POSTS_JSON__;

  const $ = (id) => document.getElementById(id);

  function escapeHtml(s) {
    return String(s||"")
      .replaceAll("&","&amp;")
      .replaceAll("<","&lt;")
      .replaceAll(">","&gt;")
      .replaceAll('"',"&quot;")
      .replaceAll("'","&#039;");
  }

  function showLoading(){ $("loadingMask")?.classList.add("show"); }
  function hideLoading(){ $("loadingMask")?.classList.remove("show"); }

  function toDateOnly(iso) {
    try {
      const d = new Date(iso);
      const y = d.getFullYear();
      const m = String(d.getMonth()+1).padStart(2,"0");
      const da = String(d.getDate()).padStart(2,"0");
      return y + "-" + m + "-" + da;
    } catch (e) { return ""; }
  }

  function fmtDate(iso) {
    const d = toDateOnly(iso);
    if (!d) return "-";
    return d.replaceAll("-", ".");
  }

  function parseDateInput(v) {
    if (!v) return null;
    const t = new Date(v + "T00:00:00");
    if (isNaN(t.getTime())) return null;
    return t;
  }

  function inRange(postIso, start, end) {
    const d = new Date(postIso);
    if (isNaN(d.getTime())) return false;
    if (start && d < start) return false;
    if (end) {
      const end2 = new Date(end.getTime());
      end2.setHours(23,59,59,999);
      if (d > end2) return false;
    }
    return true;
  }

  let state = {
    hideSponsored: false,
    vocOnly: false,
    reviewOnly: true,
    hideIrrelevant: true,
    start: null,
    end: null,
    word: null,
    tag: null,
  };

  function vocMin() {
    const v = parseInt(($("vocMin")?.value || "20"), 10);
    if (isNaN(v)) return 20;
    return Math.max(0, Math.min(120, v));
  }

  function filteredPosts() {
    const minV = vocMin();
    return POSTS.filter(p => {
      if (state.hideSponsored && p.is_sponsored) return false;
      if (!inRange(p.published_at, state.start, state.end)) return false;

      if (state.vocOnly && (p.voc_score || 0) < minV) return false;
      if (state.reviewOnly && (p.content_type || "") !== "purchase_review") return false;
      if (state.hideIrrelevant && (p.relevance_score || 0) < 60) return false;

      if (state.tag) {
        const tags = p.voc_tags || [];
        if (!tags.includes(state.tag)) return false;
      }

      if (!state.word) return true;
      const w = state.word.toLowerCase();
      const t = (p.text_preview || "").toLowerCase();
      const title = (p.title || "").toLowerCase();
      return t.includes(w) || title.includes(w);
    });
  }

  function renderHeader() {
    $("updatedAt").textContent = META.updated_at_kst || "-";
    $("periodText").textContent = META.period_text || "-";

    const dbg = META.debug || {};
    const counts = META.counts || {};
    $("debugLine").innerHTML =
      'candidates: <span class="font-black">' + escapeHtml(dbg.candidates_total ?? "-") + '</span>' +
      '<span class="mx-2">|</span> scraped_ok: <span class="font-black">' + escapeHtml(dbg.scraped_ok ?? "-") + '</span>' +
      '<span class="mx-2">|</span> kept_brand: <span class="font-black">' + escapeHtml(dbg.brand_kept ?? "-") + '</span>' +
      '<span class="mx-2">|</span> saved: <span class="font-black">' + escapeHtml(counts.posts ?? "-") + '</span>' +
      '<span class="mx-2">|</span> products_loaded: <span class="font-black">' + escapeHtml(META.products_loaded ?? "-") + '</span>';
  }

  function renderSummaryCards() {
    const box = $("summaryCards");
    box.innerHTML = "";

    const listAll = filteredPosts();

    const now = new Date();
    const todayKey = now.getFullYear() + "-" + String(now.getMonth()+1).padStart(2,"0") + "-" + String(now.getDate()).padStart(2,"0");

    let newToday = 0;
    let sponsored = 0;
    let vocOver = 0;

    listAll.forEach(p => {
      if (toDateOnly(p.collected_at) === todayKey) newToday += 1;
      if (p.is_sponsored) sponsored += 1;
      if ((p.voc_score||0) >= vocMin()) vocOver += 1;
    });

    const end = state.end ? new Date(state.end.getTime()) : new Date();
    const start7 = new Date(end.getTime()); start7.setDate(start7.getDate()-6);

    function ratio(rangeStart) {
      const subset = POSTS.filter(p => inRange(p.published_at, rangeStart, end));
      if (subset.length === 0) return 0;
      const n = subset.filter(p => (p.voc_score||0) >= vocMin()).length;
      return Math.round((n / subset.length) * 1000) / 10;
    }

    const r7 = ratio(start7);

    const cards = [
      { title: "Posts", value: String(listAll.length), sub: "현재 필터" },
      { title: "New Today", value: String(newToday), sub: "수집일 기준" },
      { title: "VOC (≥" + vocMin() + ")", value: String(vocOver), sub: "VOC 감지" },
      { title: "VOC Ratio (7D)", value: r7 + "%", sub: "published 기준" },
    ];

    cards.forEach(c => {
      const el = document.createElement("div");
      el.className = "glass-card p-5";
      el.innerHTML =
        '<div class="text-[10px] font-black tracking-widest text-slate-500 uppercase">' + escapeHtml(c.title) + '</div>' +
        '<div class="text-3xl font-black mt-2">' + escapeHtml(c.value) + '</div>' +
        '<div class="muted text-sm font-semibold mt-1">' + escapeHtml(c.sub) + '</div>';
      box.appendChild(el);
    });
  }

  function renderTopVoc() {
    const box = $("topVocList");
    box.innerHTML = "";
    const list = filteredPosts().slice().sort((a,b)=> (b.voc_score||0)-(a.voc_score||0)).slice(0, 8);

    if (list.length === 0) {
      box.innerHTML = '<div class="muted font-semibold">VOC 항목이 없습니다.</div>';
      return;
    }

    list.forEach(p => {
      const hl = (p.voc_highlights || [])[0]?.sent || p.excerpt || "";
      const el = document.createElement("div");
      el.className = "feed-card p-4";
      el.innerHTML =
        '<div class="flex items-start justify-between gap-2">' +
          '<div class="font-black text-sm leading-snug">' + escapeHtml(p.title||"(제목 없음)") + '</div>' +
          '<div class="badge badge-voc">VOC ' + (p.voc_score||0) + '</div>' +
        '</div>' +
        '<div class="mt-2 text-xs font-semibold muted lineclamp4">' + escapeHtml(hl) + '</div>' +
        '<div class="mt-2 text-xs font-black muted">' + escapeHtml((p.voc_tags||[]).slice(0,3).join(", ")) + '</div>';

      el.addEventListener("click", ()=> {
        window.open(p.url, "_blank", "noopener,noreferrer");
      });
      box.appendChild(el);
    });
  }

  function renderTagChips() {
    const box = $("tagChips");
    box.innerHTML = "";

    const freq = {};
    POSTS.forEach(p => {
      (p.voc_tags||[]).forEach(t => { freq[t] = (freq[t]||0) + 1; });
    });
    const items = Object.entries(freq).sort((a,b)=> b[1]-a[1]).slice(0, 14);

    const clear = document.createElement("div");
    clear.className = "badge cursor-pointer hover:opacity-80";
    clear.textContent = "태그 해제";
    clear.addEventListener("click", ()=> { state.tag = null; renderAll(); });
    box.appendChild(clear);

    items.forEach(([t,c]) => {
      const el = document.createElement("div");
      el.className = "badge cursor-pointer hover:opacity-80";
      el.innerHTML = escapeHtml(t) + " · " + c;
      el.addEventListener("click", ()=> { state.tag = t; renderAll(); });
      box.appendChild(el);
    });
  }

  function renderWords() {
    const box = $("wordChips");
    box.innerHTML = "";
    const words = (META.top_words || []).slice(0, 16);
    words.forEach(([w,c]) => {
      const el = document.createElement("div");
      el.className = "badge cursor-pointer hover:opacity-80";
      el.innerHTML = escapeHtml(w) + " · " + c;
      el.addEventListener("click", () => { state.word = w; renderAll(); });
      box.appendChild(el);
    });

    const clear = document.createElement("div");
    clear.className = "badge cursor-pointer hover:opacity-80";
    clear.textContent = "단어 해제";
    clear.addEventListener("click", ()=> { state.word = null; renderAll(); });
    box.appendChild(clear);
  }

  function typeBadge(p){
    const t = p.content_type || "unknown";
    if (t === "purchase_review") return '<span class="badge badge-type">Review</span>';
    if (t === "info_post") return '<span class="badge badge-info">Info</span>';
    if (t === "mixed") return '<span class="badge">Mixed</span>';
    return '<span class="badge">Unknown</span>';
  }

  function renderFeed() {
    const grid = $("feedGrid");
    grid.innerHTML = "";
    const list = filteredPosts().slice().sort((a,b)=> (b.published_at||"").localeCompare(a.published_at||""));

    if (list.length === 0) {
      grid.innerHTML = '<div class="muted font-semibold">조건에 맞는 글이 없습니다.</div>';
      return;
    }

    list.forEach((p, idx) => {
      const card = document.createElement("div");
      card.className = "feed-card p-4 cursor-pointer hover:translate-y-[-1px] transition";

      const badges = [];
      badges.push('<span class="badge badge-brand">Brand OK</span>');
      badges.push(typeBadge(p));
      badges.push('<span class="badge badge-rel">Rel ' + (p.relevance_score||0) + '</span>');
      if ((p.product_hits||[]).length) badges.push('<span class="badge">Product</span>');
      if (p.is_sponsored) badges.push('<span class="badge badge-ad">협찬</span>');

      const vs = p.voc_score || 0;
      if (vs >= vocMin()) badges.push('<span class="badge badge-voc">VOC ' + vs + '</span>');
      const tags = (p.voc_tags || []).slice(0,2).join(", ");
      if (tags) badges.push('<span class="badge">tags: ' + escapeHtml(tags) + '</span>');

      const metaLine = [];
      if (p.author) metaLine.push(p.author);
      if (p.blog_site_name) metaLine.push(p.blog_site_name);
      const metaText = metaLine.join(" · ");

      card.innerHTML =
        '<div class="flex items-start justify-between gap-2">' +
          '<div class="font-black text-base leading-snug">' + escapeHtml(p.title||"(제목 없음)") + '</div>' +
          '<div class="text-[11px] font-black muted whitespace-nowrap">' + fmtDate(p.published_at) + '</div>' +
        '</div>' +
        '<div class="mt-2 flex flex-wrap gap-2">' + badges.join("") + '</div>' +
        '<div class="mt-3 text-sm font-semibold muted lineclamp4">' + escapeHtml(p.excerpt || "") + '</div>' +
        '<div class="mt-3 text-[12px] font-black muted">' + escapeHtml(metaText) + '</div>';

      card.addEventListener("click", () => {
        window.open(p.url, "_blank", "noopener,noreferrer");
        const target = document.getElementById("detail-" + idx);
        if (target) target.scrollIntoView({ behavior:"smooth", block:"start" });
      });

      grid.appendChild(card);
    });
  }

  function renderDetails() {
    const box = $("detailList");
    box.innerHTML = "";
    const list = filteredPosts().slice().sort((a,b)=> (b.published_at||"").localeCompare(a.published_at||""));

    list.forEach((p, idx) => {
      const wrap = document.createElement("div");
      wrap.id = "detail-" + idx;
      wrap.className = "feed-card p-5";

      const sponsoredLine = p.is_sponsored
        ? '<div class="mt-2 text-xs font-black badge badge-ad">협찬/광고: ' + escapeHtml((p.sponsored_markers||[]).join(", ")) + '</div>'
        : '';

      const tagBadges = (p.voc_tags||[]).slice(0,6).map(t => '<span class="badge">' + escapeHtml(t) + '</span>').join("");

      const vocLine =
        '<div class="mt-2 flex flex-wrap gap-2">' +
          '<span class="badge badge-voc">VOC ' + (p.voc_score||0) + '</span>' +
          '<span class="badge badge-rel">Rel ' + (p.relevance_score||0) + '</span>' +
          typeBadge(p) +
          tagBadges +
        '</div>';

      const ph = (p.product_hits||[]).slice(0,4);
      const prodLine = ph.length
        ? '<div class="mt-2 text-xs font-black badge">Product hits: ' + escapeHtml(ph.join(" | ")) + '</div>'
        : '';

      const hl = (p.voc_highlights || []).map(h => {
        return '<div class="mt-2 text-sm font-semibold muted">• ' + escapeHtml(h.sent) +
               ' <span class="badge ml-2">+' + (h.score||0) + '</span></div>';
      }).join("");

      const img = p.og_image
        ? '<img src="' + escapeHtml(p.og_image) + '" class="w-full rounded-2xl border border-white/70 mt-4" loading="lazy" />'
        : "";

      const engage = [];
      if (p.like_count !== null && p.like_count !== undefined) engage.push("좋아요 " + p.like_count);
      if (p.comment_count !== null && p.comment_count !== undefined) engage.push("댓글 " + p.comment_count);
      const engageHtml = engage.length ? ('<span class="mx-2">·</span><span class="font-black">' + escapeHtml(engage.join(" / ")) + '</span>') : "";

      wrap.innerHTML =
        '<div class="flex flex-col md:flex-row md:items-start md:justify-between gap-3">' +
          '<div class="flex-1">' +
            '<div class="text-lg font-black leading-snug">' + escapeHtml(p.title||"(제목 없음)") + '</div>' +
            '<div class="mt-2 text-sm font-semibold muted">' +
              '발행: <span class="font-black">' + fmtDate(p.published_at) + '</span>' +
              '<span class="mx-2">·</span>' +
              '작성자: <span class="font-black">' + escapeHtml(p.author||"-") + '</span>' +
              '<span class="mx-2">·</span>' +
              '메타: <span class="font-black">' + escapeHtml(p.blog_site_name||"-") + '</span>' +
              engageHtml +
            '</div>' +
            sponsoredLine +
            prodLine +
            vocLine +
          '</div>' +
          '<a class="link text-sm" href="' + escapeHtml(p.url) + '" target="_blank" rel="noopener noreferrer">' +
            '원문 열기 <i class="fa-solid fa-arrow-up-right-from-square"></i>' +
          '</a>' +
        '</div>' +

        '<div class="mt-4">' +
          '<div class="text-sm font-black">VOC 하이라이트</div>' +
          '<div class="mt-2">' + (hl || '<div class="muted font-semibold">하이라이트가 없습니다. (VOC 신호 약함)</div>') + '</div>' +
        '</div>' +

        '<div class="mt-4">' +
          '<div class="text-sm font-black">본문 미리보기</div>' +
          '<div class="mt-2 text-sm font-semibold muted whitespace-pre-line">' + escapeHtml(p.text_preview||"") + '</div>' +
        '</div>' +
        img;

      box.appendChild(wrap);
    });
  }

  function initDateControls() {
    const all = POSTS.slice().sort((a,b)=> (b.published_at||"").localeCompare(a.published_at||""));
    const newestIso = all[0]?.published_at || null;
    const newestDate = newestIso ? new Date(newestIso) : new Date();

    function setRange(days){
      const end = new Date(newestDate.getTime());
      const start = new Date(newestDate.getTime());
      start.setDate(start.getDate() - (days-1));
      $("dateStart").value = toDateOnly(start.toISOString());
      $("dateEnd").value = toDateOnly(end.toISOString());
      state.start = parseDateInput($("dateStart").value);
      state.end = parseDateInput($("dateEnd").value);
    }

    setRange(7);

    $("btnApply").addEventListener("click", () => {
      showLoading();
      setTimeout(() => {
        state.start = parseDateInput($("dateStart").value);
        state.end = parseDateInput($("dateEnd").value);
        renderAll();
        hideLoading();
      }, 50);
    });

    $("btnReset7").addEventListener("click", () => {
      showLoading();
      setTimeout(() => { setRange(7); renderAll(); hideLoading(); }, 50);
    });

    $("btnReset60").addEventListener("click", () => {
      showLoading();
      setTimeout(() => { setRange(60); renderAll(); hideLoading(); }, 50);
    });

    $("btnHideSponsored").addEventListener("click", () => {
      state.hideSponsored = !state.hideSponsored;
      $("btnHideSponsored").textContent = state.hideSponsored ? "협찬 보이기" : "협찬 숨기기";
      renderAll();
    });

    $("btnVocOnly").addEventListener("click", () => {
      state.vocOnly = !state.vocOnly;
      $("btnVocOnly").textContent = state.vocOnly ? "전체 보기" : "VOC만 보기";
      renderAll();
    });

    $("btnReviewOnly").addEventListener("click", () => {
      state.reviewOnly = !state.reviewOnly;
      $("btnReviewOnly").textContent = state.reviewOnly ? "전체 글 보기" : "리뷰만 보기";
      renderAll();
    });

    $("btnHideIrrelevant").addEventListener("click", () => {
      state.hideIrrelevant = !state.hideIrrelevant;
      $("btnHideIrrelevant").textContent = state.hideIrrelevant ? "irrelevant 보이기" : "irrelevant 숨기기";
      renderAll();
    });

    $("vocMin").addEventListener("change", renderAll);
  }

  function renderAll() {
    renderHeader();
    renderSummaryCards();
    renderTopVoc();
    renderTagChips();
    renderWords();
    renderFeed();
    renderDetails();
  }

  renderHeader();
  initDateControls();
  renderAll();
</script>
</body>
</html>
"""

def build_report_html(meta: Dict[str, Any], posts: List[Dict[str, Any]]) -> str:
    meta_json = json.dumps(meta, ensure_ascii=False)
    posts_json = json.dumps(posts, ensure_ascii=False)
    return HTML_TEMPLATE.replace("__META_JSON__", meta_json).replace("__POSTS_JSON__", posts_json)

# =======================
# Main
# =======================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--queries", nargs="+", required=True)
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--per-query", type=int, default=60)
    ap.add_argument("--headless", action="store_true")
    ap.add_argument("--out-dir", default=os.path.join("reports", "voc_blog"))
    ap.add_argument("--max-scrape", type=int, default=220)
    ap.add_argument("--brand-target", type=int, default=160)
    ap.add_argument("--voc-target", type=int, default=60)
    ap.add_argument("--voc-min", type=int, default=20)
    ap.add_argument("--force-rescrape-seen", action="store_true")
    ap.add_argument("--product-csv", default="")
    args = ap.parse_args()

    out_dir = args.out_dir
    data_dir = os.path.join(out_dir, "data")
    ensure_dir(data_dir)

    pm = ProductMatcher()
    products_loaded = 0
    product_csv = args.product_csv.strip()
    if not product_csv:
        guess = "mall_product_list (15).xls.csv"
        if os.path.exists(guess):
            product_csv = guess
    if product_csv:
        try:
            products_loaded = pm.load_csv(product_csv)
        except Exception as e:
            print(f"[WARN] Failed to load product csv: {product_csv} err={e}")
            products_loaded = 0

    collected_at = now_kst()
    cutoff = collected_at - timedelta(days=args.days - 1)

    seen_path = os.path.join(data_dir, "seen_urls.json")
    seen_db = load_seen_urls(seen_path)

    candidates = fetch_candidates_via_naver_api(args.queries, per_query=args.per_query, sort="date")
    print(f"[INFO] candidates_total={len(candidates)} (per_query={args.per_query}) cutoff={cutoff.isoformat()} products_loaded={products_loaded}")

    candidates = candidates[: args.max_scrape]

    debug = {
        "candidates_total": len(candidates),
        "scrape_target": len(candidates),
        "scraped_ok": 0,
        "scraped_fail": 0,
        "skipped_seen": 0,
        "cutoff_filtered": 0,
        "empty_text": 0,
        "brand_kept": 0,
        "brand_reject_hard_exclude": 0,
        "brand_reject_no_apparel_context": 0,
        "brand_reject_no_token": 0,
        "brand_reject_ambiguous_no_gate": 0,
        "early_stop_brand": 0,
        "early_stop_voc": 0,
    }

    posts: List[BlogPost] = []
    brand_kept = 0
    voc_kept = 0

    with sync_playwright() as play:
        browser = play.chromium.launch(headless=args.headless)
        context = browser.new_context(
            locale="ko-KR",
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"),
            viewport={"width": 1280, "height": 900},
        )
        try:
            for c in candidates:
                q = c["query"]
                url = c["url"]
                base_title = c.get("title") or "(제목 없음)"
                snippet = c.get("snippet") or ""

                if (not args.force_rescrape_seen) and is_seen(seen_db, url):
                    debug["skipped_seen"] += 1
                    continue

                try:
                    s = scrape_one_post(context, url=url)
                    debug["scraped_ok"] += 1
                except Exception:
                    debug["scraped_fail"] += 1
                    continue

                final_url = s.get("final_url") or url
                title = safe_text(s.get("title") or base_title)
                text = s.get("text") or ""
                pub_dt = s.get("pub_dt") or collected_at

                if len(safe_text(text)) == 0:
                    debug["empty_text"] += 1

                if pub_dt < cutoff:
                    debug["cutoff_filtered"] += 1
                    mark_seen(seen_db, url, iso(collected_at))
                    continue

                ok, markers, reason, brand_dbg = is_brand_columbia(title=title, body=text, snippet=snippet, pm=pm)
                if not ok:
                    if reason == "hard_exclude":
                        debug["brand_reject_hard_exclude"] += 1
                    elif reason == "no_apparel_context":
                        debug["brand_reject_no_apparel_context"] += 1
                    elif reason == "ambiguous_no_gate":
                        debug["brand_reject_ambiguous_no_gate"] += 1
                    else:
                        debug["brand_reject_no_token"] += 1
                    mark_seen(seen_db, url, iso(collected_at))
                    continue

                debug["brand_kept"] += 1
                brand_kept += 1

                combined_for_flags = f"{title} {text} {snippet}"
                is_sp, sp_markers = detect_sponsored(combined_for_flags)

                cleaned = safe_text(text) if text else ""
                excerpt = cleaned[:180] if cleaned else safe_text(snippet)[:180] if snippet else "(본문 추출 실패)"
                preview = cleaned[:1400] if cleaned else safe_text(snippet)[:1400] if snippet else "(본문 추출 실패)"

                ctype, rv, inf, rv_hits, inf_hits = classify_content(title, snippet, cleaned)

                prod_hits = pm.find_matches(combined_for_flags) if products_loaded else []
                prod_hits = prod_hits[:6]

                voc_score, voc_tags, voc_highlights, voc_dbg = voc_score_for_text(title=title, text=cleaned, snippet=snippet)
                if voc_score >= args.voc_min:
                    voc_kept += 1

                rel = compute_relevance(
                    ok_brand=True,
                    brand_reason=reason,
                    content_type=ctype,
                    review_score=rv,
                    info_score=inf,
                    product_hits=prod_hits,
                    voc_score=voc_score,
                    hard_excluded=False,
                )

                p = BlogPost(
                    query=q,
                    title=title or "(제목 없음)",
                    url=final_url,
                    published_at=iso(pub_dt),
                    collected_at=iso(collected_at),

                    is_sponsored=is_sp,
                    sponsored_markers=sp_markers,

                    is_columbia_brand=True,
                    brand_markers_found=markers,
                    brand_reason=reason,
                    brand_debug=brand_dbg,

                    product_hits=prod_hits,

                    content_type=ctype,
                    review_score=int(rv),
                    info_score=int(inf),
                    content_debug={"review_hits": rv_hits, "info_hits": inf_hits, "used_frame": bool(s.get("used_frame"))},

                    relevance_score=int(rel),

                    blog_site_name=safe_text(s.get("site_name") or ""),
                    author=safe_text(s.get("author") or ""),
                    og_image=safe_text(s.get("og_image") or ""),
                    like_count=s.get("like_count"),
                    comment_count=s.get("comment_count"),

                    raw_text_len=len(text or ""),
                    excerpt=excerpt,
                    text_preview=preview,

                    voc_score=voc_score,
                    voc_tags=voc_tags,
                    voc_highlights=voc_highlights,
                    voc_debug=voc_dbg,

                    source="naver_api+playwright",
                )

                posts.append(p)
                mark_seen(seen_db, url, iso(collected_at))

                if brand_kept >= args.brand_target:
                    debug["early_stop_brand"] = 1
                    if voc_kept >= args.voc_target:
                        debug["early_stop_voc"] = 1
                        break
        finally:
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass

    save_seen_urls(seen_path, seen_db)
    posts.sort(key=lambda x: x.published_at, reverse=True)

    top_words = keyword_map_from_texts([p.text_preview for p in posts], topn=40)

    tag_freq: Dict[str, int] = {}
    for p in posts:
        for t in (p.voc_tags or []):
            tag_freq[t] = tag_freq.get(t, 0) + 1
    tag_top = sorted(tag_freq.items(), key=lambda x: x[1], reverse=True)[:15]

    top_voc = sorted(posts, key=lambda x: x.voc_score, reverse=True)[:12]

    type_freq: Dict[str, int] = {}
    for p in posts:
        type_freq[p.content_type] = type_freq.get(p.content_type, 0) + 1

    meta = {
        "updated_at_kst": fmt_kst(collected_at),
        "period_text": f"최근 {args.days}일 ({cutoff.strftime('%Y.%m.%d')} ~ {collected_at.strftime('%Y.%m.%d')})",
        "days": args.days,
        "per_query": args.per_query,
        "max_scrape": args.max_scrape,
        "brand_target": args.brand_target,
        "voc_target": args.voc_target,
        "voc_min": args.voc_min,
        "queries": args.queries,
        "products_loaded": products_loaded,
        "counts": {
            "posts": len(posts),
            "sponsored_posts": sum(1 for p in posts if p.is_sponsored),
            "voc_posts": sum(1 for p in posts if p.voc_score >= args.voc_min),
            "body_empty_posts": sum(1 for p in posts if p.raw_text_len == 0),
        },
        "content_types": type_freq,
        "top_words": top_words,
        "tag_top": tag_top,
        "top_voc": [
            {"title": p.title, "url": p.url, "voc_score": p.voc_score, "voc_tags": p.voc_tags[:5], "published_at": p.published_at,
             "content_type": p.content_type, "relevance_score": p.relevance_score}
            for p in top_voc
        ],
        "debug": debug,
    }

    posts_dicts: List[Dict[str, Any]] = []
    for p in posts:
        d = asdict(p)
        d["publishedAt"] = d.get("published_at")
        d["collectedAt"] = d.get("collected_at")
        posts_dicts.append(d)

    posts_root = {"posts": posts_dicts, "items": posts_dicts}

    with open(os.path.join(data_dir, "posts.json"), "w", encoding="utf-8") as f:
        json.dump(posts_root, f, ensure_ascii=False, indent=2)

    with open(os.path.join(data_dir, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    html = build_report_html(meta=meta, posts=posts_dicts)
    with open(os.path.join(out_dir, "index.html"), "w", encoding="utf-8") as f:
        f.write(html)

    print(f"[OK] posts_saved={len(posts)} voc_posts={meta['counts']['voc_posts']} body_empty={meta['counts']['body_empty_posts']} debug={debug}")
    print(f"[OK] {os.path.join(out_dir, 'index.html')}")

if __name__ == "__main__":
    main()
