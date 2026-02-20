#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
[VOC DASHBOARD | v4.0 FINAL]
입력: reviews.json ({"reviews":[...]})  ※ "created_at" ISO 권장
출력:
  - site/data/reviews.json   : 최근 7일 필터된 리뷰
  - site/data/meta.json      : 기간/키워드/마인드맵/랭킹 메타
  - site/index.html          : UI(키워드 클릭→리뷰로 스크롤, 3열 데일리피드, 컨테이너 폭 제한)

핵심 반영 ✅
1) 최근 7일 고정 수집 (KST)
2) 키워드(긍/부정) 명확 분리
   - NEG: 불만리뷰(저평점/불만태그/불만표현) 기반
   - POS: 긍정리뷰(고평점/긍정태그) 기반
   - 중립/관용구/의미없는 단어(#구입, #같은, #기본, #제품, #배송 등) 강하게 제거
   - "제품군(추정 카테고리)"별 자동 STOPWORDS 학습(자주 나오는 중립어 자동 제거)
3) 키워드 칩 클릭하면 해당 키워드 포함 리뷰로 이동(스크롤) + 검색 자동 적용
   - POS 키워드는 "무엇이 좋은지" 짧은 예시(근거 리뷰 1~2개)도 함께 제공
4) '개선 우선순위 제품 랭킹' 컬럼명: "주요 문제 키워드" → "주요 키워드"
   - 제품별로 최근7일 기준, NEG/POS 키워드 중립 제거 후 상위 표현 제공
5) 4. 대표 리뷰 마인드맵 → "제품별(최근 1년) 누적 긍/부정 키워드 마인드맵"으로 교체
   - 제품 사진 포함
   - 제품별 POS/NEG Top 키워드(누적) + 클릭시 해당 제품/키워드 리뷰로 스크롤

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
from typing import Any, Dict, List, Tuple, Optional, Iterable

import pandas as pd
from dateutil import tz


# ----------------------------
# Settings
# ----------------------------
OUTPUT_TZ = os.getenv("OUTPUT_TZ", "Asia/Seoul").strip()


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
# Text cleaning + keyword extraction
# ----------------------------

# 강한 기본 STOPWORDS (중립/관용구/정보량 낮은 단어)
BASE_STOPWORDS = set(
    """
그리고 그러나 그래서 하지만 또한 또한요
너무 정말 완전 진짜 매우 그냥 조금 약간 살짝
저는 제가 우리는 너희 이거 그거 저거 여기 저기
있습니다 입니다 같아요 같네요 하는 하다 됐어요 되었어요 되네요
구매 구입 구입해 구입해서 샀어요 샀습니다 구매했 구매해서
제품 상품 물건 아이템
사용 착용 써보니 써보고 써봤
배송 택배 포장 출고 도착 빠르 빠르게
문의 응대 cs 상담 교환 반품 환불 처리
좋아요 좋다 좋네요 좋습니다 만족 추천 재구매 가성비 최고 굿 예뻐요 이뻐요
기본 같은 그냥 그럭저럭 무난 무난해
사이즈 정사이즈 한치수 한 치수
컬러 색상 디자인
있어서 있어서요 있어요 있네요 있었어요
좋습니다 좋았어요 좋네요
추가 추가로 추가하면
가능 가능해요
확인 확인해요
생각 생각해요
느낌 느낌이에요
정도 정도로
부분 부분이
사람 분들
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
    "좋아요", "좋네요", "좋습니다",
]

RE_URL = re.compile(r"https?://\S+|www\.\S+", re.I)
RE_HASHTAG = re.compile(r"#[A-Za-z0-9가-힣_]+")
RE_EMOJI_ETC = re.compile(r"[^\w\s가-힣]", re.UNICODE)

# "이슈 단어"는 자동 STOPWORDS 학습에서 보호(지워지면 안 됨)
ISSUE_KEEP = set(
    """
사이즈 작다 작아 작아요 크다 커요 커요다 크다요
타이트 끼다 조이다 좁다 짧다 길다 넓다 헐렁 오버
품질 불량 하자 실밥 구멍 찢어 오염 변색 냄새
미끄럽 누수 물샘
배송 지연 늦다 느리다 포장 파손
응대 cs 불친절 교환 반품 환불
색상 차이 프린트 이염
"""
    .split()
)

# POS/NEG 힌트(태그 보강/분류 보강용)
POS_HINTS = [
    "가볍", "편하", "따뜻", "튼튼", "예쁘", "귀엽", "잘맞", "좋", "만족", "추천", "재구매",
    "수납", "많이들어", "넉넉", "견고", "부드럽", "포근", "핏좋", "깔끔", "방수", "방풍"
]
NEG_HINTS = [
    "불량", "하자", "찢", "구멍", "냄새", "변색", "오염", "실밥", "품질", "엉망", "별로", "최악",
    "실망", "문제", "불편", "아쉽", "개선", "교환", "반품", "환불", "늦", "지연", "파손", "불친절"
]

# 이슈 클러스터 (키워드가 "원인 기반"으로 깔끔하게 모이도록)
CLUSTERS: Dict[str, List[str]] = {
    "size": ["사이즈", "정사이즈", "작아", "작다", "커", "크다", "타이트", "끼", "조이", "짧", "길", "넓", "헐렁", "오버", "핏", "기장", "소매", "어깨", "가슴", "발볼"],
    "quality": ["품질", "불량", "하자", "실밥", "구멍", "찢", "변색", "오염", "냄새", "이염", "마감", "내구", "튼튼"],
    "shipping": ["배송", "택배", "포장", "지연", "늦", "파손", "누락", "오배송"],
    "cs": ["cs", "상담", "응대", "불친절", "교환", "반품", "환불", "처리", "문의"],
}

# 제품군/카테고리 추정(데이터에 category가 없다면, product_name 기반으로 대략)
CATEGORY_RULES: Dict[str, List[str]] = {
    "bag": ["backpack", "rucksack", "packable", "bag", "shoulder", "sling", "tote", "pouch", "mini", "pack"],
    "shoe": ["shoe", "shoes", "chukka", "sneaker", "boot", "boots", "sandals", "slip", "runner"],
    "top": ["fleece", "hood", "hoodie", "jacket", "half", "snap", "turtleneck", "tee", "shirt", "sweater", "knit"],
    "bottom": ["pant", "pants", "short", "shorts", "skirt", "legging"],
    "glove": ["glove", "mitten"],
    "sock": ["sock", "socks"],
    "etc": [],
}


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


def looks_like_model_token(t: str) -> bool:
    # 모델명/코드/영문+숫자 혼합 장토큰(키워드로는 잡음일 때가 많음)
    if len(t) >= 10 and re.search(r"[A-Za-z]", t) and re.search(r"\d", t):
        return True
    if len(t) >= 12 and t.isalnum():
        return True
    return False


def tokenize_ko(s: str, stopwords: set) -> List[str]:
    s = normalize_text(s)
    if not s:
        return []

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
        if t in stopwords:
            continue

        # 자주 나오는 의미 없는 어근 컷
        if t in ("있", "좋", "하", "되", "같"):
            continue

        if looks_like_model_token(t):
            continue

        out.append(t)

    return out


def top_terms(texts: List[str], stopwords: set, topk: int = 5) -> List[Tuple[str, int]]:
    freq: Dict[str, int] = {}
    for tx in texts:
        for tok in tokenize_ko(tx, stopwords=stopwords):
            freq[tok] = freq.get(tok, 0) + 1
    return sorted(freq.items(), key=lambda x: x[1], reverse=True)[:topk]


def guess_category(product_name: str) -> str:
    n = normalize_text(product_name)
    for cat, keys in CATEGORY_RULES.items():
        if not keys:
            continue
        for k in keys:
            if k in n:
                return cat
    return "etc"


def build_dynamic_stopwords(
    rows: List[Dict[str, Any]],
    base: set,
    *,
    per_category: bool = True,
    max_global: int = 25,
    max_per_cat: int = 12,
    df_ratio_threshold: float = 0.35,
) -> set:
    """
    "자주 나오는 중립어" 자동 제거:
      - (문서빈도 DF)/(문서수) 가 높고, ISSUE_KEEP에 없으면 stopword 후보
      - 카테고리별로도 동일하게 수행(제품군별 관용구 제거)
    """
    stop = set(base)

    # 글로벌 DF
    doc_cnt = 0
    df: Dict[str, int] = {}
    for r in rows:
        text = str(r.get("text") or "")
        toks = set(tokenize_ko(text, stopwords=stop))  # base 기준 1차
        if not toks:
            continue
        doc_cnt += 1
        for t in toks:
            df[t] = df.get(t, 0) + 1

    if doc_cnt > 0:
        candidates = []
        for t, c in df.items():
            if t in ISSUE_KEEP:
                continue
            ratio = c / doc_cnt
            if ratio >= df_ratio_threshold:
                candidates.append((t, c, ratio))
        candidates.sort(key=lambda x: (x[2], x[1]), reverse=True)
        for t, _, _ in candidates[:max_global]:
            stop.add(t)

    if not per_category:
        return stop

    # 카테고리별 DF
    by_cat: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        cat = guess_category(str(r.get("product_name") or ""))
        by_cat.setdefault(cat, []).append(r)

    for cat, items in by_cat.items():
        doc_cnt = 0
        df_cat: Dict[str, int] = {}
        for r in items:
            text = str(r.get("text") or "")
            toks = set(tokenize_ko(text, stopwords=stop))
            if not toks:
                continue
            doc_cnt += 1
            for t in toks:
                df_cat[t] = df_cat.get(t, 0) + 1
        if doc_cnt <= 0:
            continue

        candidates = []
        for t, c in df_cat.items():
            if t in ISSUE_KEEP:
                continue
            ratio = c / doc_cnt
            if ratio >= 0.40:  # cat는 더 강하게
                candidates.append((t, c, ratio))
        candidates.sort(key=lambda x: (x[2], x[1]), reverse=True)

        for t, _, _ in candidates[:max_per_cat]:
            stop.add(t)

    return stop


def cluster_counts_from_texts(texts: List[str]) -> Dict[str, int]:
    out = {k: 0 for k in CLUSTERS.keys()}
    for tx in texts:
        t = normalize_text(tx).replace(" ", "")
        for cname, kws in CLUSTERS.items():
            for kw in kws:
                if kw.replace(" ", "") in t:
                    out[cname] += 1
                    break
    return out


def example_snips_for_keyword(rows: List[Dict[str, Any]], keyword: str, limit: int = 2) -> List[Dict[str, Any]]:
    """
    POS 키워드는 "뭐가 좋은지" 같이 보여줘야 하므로
    해당 키워드가 포함된 리뷰의 짧은 근거 스니펫을 제공.
    """
    kw = normalize_text(keyword)
    out: List[Dict[str, Any]] = []
    for r in rows:
        txt = str(r.get("text") or "")
        if kw and kw in normalize_text(txt):
            snip = re.sub(r"\s+", " ", txt).strip()
            snip = snip[:70] + ("…" if len(snip) > 70 else "")
            out.append(
                {
                    "id": r.get("id"),
                    "product_code": r.get("product_code"),
                    "product_name": r.get("product_name"),
                    "created_at": str(r.get("created_at") or "")[:10],
                    "rating": int(r.get("rating") or 0),
                    "snip": snip,
                }
            )
        if len(out) >= limit:
            break
    return out


# ----------------------------
# Rule tagging (보강용)
# ----------------------------
SIZE_KEYWORDS = [
    "사이즈", "정사이즈", "작아요", "작다", "커요", "크다", "핏", "타이트", "여유",
    "끼", "기장", "소매", "어깨", "가슴", "발볼", "헐렁", "오버"
]
REQ_KEYWORDS = ["개선", "아쉬", "불편", "했으면", "추가", "보완", "수정", "필요", "요청", "교환", "반품", "환불"]
COMPLAINT_HINTS = NEG_HINTS


def has_any_kw(text: str, kws: List[str]) -> bool:
    t = (text or "").replace(" ", "").lower()
    for kw in kws:
        if kw.replace(" ", "").lower() in t:
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
    """입력 JSON tags를 존중하되, 없거나 약하면 규칙으로 보강."""
    text = str(row.get("text") or "")
    rating = int(row.get("rating") or 0)

    tags = row.get("tags")
    if not isinstance(tags, list):
        tags = []

    # low
    if rating <= 2 and "low" not in tags:
        tags.append("low")

    # pos (명확하게: 고평점 + 긍정 힌트/또는 기존 pos 태그)
    if rating >= 4 and "pos" not in tags and has_any_kw(text, POS_HINTS):
        tags.append("pos")

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
    """불만리뷰 정의:
    - rating<=2
    - tags에 low/req 포함
    - rating<=3 + 불만 힌트(품질/불량 등) 포함
    """
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
    tags = row.get("tags") or []
    text = str(row.get("text") or "")
    if isinstance(tags, list) and "pos" in tags:
        return True
    if rating >= 4 and has_any_kw(text, POS_HINTS):
        return True
    return False


# ----------------------------
# Product mindmap (최근 1년 누적, 제품별 POS/NEG 키워드)
# ----------------------------
def build_product_mindmap_1y(
    rows_1y: List[Dict[str, Any]],
    stopwords: set,
    *,
    topk_kw: int = 6,
    max_products: int = 18,
) -> List[Dict[str, Any]]:
    """
    제품별로 1년 누적 POS/NEG 키워드를 뽑아 마인드맵 카드로 사용
    - 제품은 1년 리뷰수 기준 상위 max_products 노출
    """
    by_prod: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows_1y:
        code = str(r.get("product_code") or "-")
        by_prod.setdefault(code, []).append(r)

    # 제품 선정: 1년 리뷰수 상위
    prods = sorted(by_prod.items(), key=lambda kv: len(kv[1]), reverse=True)[:max_products]

    out: List[Dict[str, Any]] = []
    for code, items in prods:
        name = str(items[0].get("product_name") or code)
        img = str(items[0].get("local_product_image") or "")

        pos_texts = [str(x.get("text") or "") for x in items if is_positive(x) and not is_complaint(x)]
        neg_texts = [str(x.get("text") or "") for x in items if is_complaint(x)]

        pos_top = top_terms(pos_texts, stopwords=stopwords, topk=topk_kw) if pos_texts else []
        neg_top = top_terms(neg_texts, stopwords=stopwords, topk=topk_kw) if neg_texts else []

        # 키워드별 대표 리뷰(1개) id 연결(클릭 -> 스크롤)
        def first_review_id_containing(keyword: str, pool: List[Dict[str, Any]]) -> Optional[Any]:
            kw = normalize_text(keyword)
            for rr in pool:
                if kw in normalize_text(str(rr.get("text") or "")):
                    return rr.get("id")
            return None

        pos_kw = []
        for k, c in pos_top:
            rid = first_review_id_containing(k, items)
            pos_kw.append({"k": k, "c": c, "rid": rid})

        neg_kw = []
        for k, c in neg_top:
            rid = first_review_id_containing(k, items)
            neg_kw.append({"k": k, "c": c, "rid": rid})

        out.append(
            {
                "product_code": code,
                "product_name": name,
                "local_product_image": img,
                "review_cnt_1y": len(items),
                "pos_keywords": pos_kw,
                "neg_keywords": neg_kw,
            }
        )

    return out


# ----------------------------
# HTML template
# ----------------------------
HTML_TEMPLATE = r"""<!DOCTYPE html>
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

  <main class="flex-1 p-8 md:p-14">
    <!-- ✅ 전체 가로폭 제한 -->
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

              <div class="text-xs font-bold text-slate-500 mt-3">최근 7일 기반(중립/관용구 제거 + 클릭→해당 리뷰 이동)</div>
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

      <!-- ✅ 섹션 4 교체: 제품별(최근 1년) 누적 긍/부정 키워드 마인드맵 -->
      <section class="mb-10">
        <div class="glass-card p-8">
          <div class="flex items-end justify-between gap-6 flex-wrap mb-6">
            <div>
              <div class="small-label text-blue-600 mb-2">4. Product Mindmap</div>
              <div class="text-xs font-bold text-slate-500 mt-1">최근 1년 누적 · 제품별 긍/부정 키워드</div>
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

          <!-- ✅ 3열 그리드 -->
          <div id="dailyFeed" class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4"></div>

          <div class="hidden review-card text-center" id="noResults">
            <div class="text-lg font-black text-slate-800">검색 결과가 없습니다.</div>
          </div>
        </div>
      </section>

      <footer class="text-xs font-bold text-slate-500 pb-8">
        * 데이터 소스: reviews.json (최근 7일 필터 후 렌더).<br/>
        * 섹션4: 최근 1년 누적(제품별 POS/NEG 키워드).<br/>
        * 키워드: 중립/관용구/제품코드성 단어 제거 + 제품군별 자동 stopwords 학습.
      </footer>

    </div>
  </main>

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

      // DOM 렌더가 끝난 뒤 스크롤
      requestAnimationFrame(() => {
        const el = document.getElementById("review-" + String(id));
        if (el){
          el.scrollIntoView({behavior:"smooth", block:"start"});
          el.classList.add("ring-4","ring-blue-200");
          setTimeout(()=> el.classList.remove("ring-4","ring-blue-200"), 1800);
        }
      });
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

        // 제품별 "주요 키워드": meta에서 제공한 pos/neg 토큰(이미 정제됨)을 활용
        const pkw = asArr(r.pos_keywords);
        const nkw = asArr(r.neg_keywords);
        for (const k of pkw) g.kwPos.set(k, (g.kwPos.get(k)||0) + 1);
        for (const k of nkw) g.kwNeg.set(k, (g.kwNeg.get(k)||0) + 1);
      }

      const rows = Array.from(byProd.values()).map(g => {
        const sizeRate = Math.round((g.sizeIssue / Math.max(1,g.reviews))*100);
        const lowRate  = Math.round((g.low / Math.max(1,g.reviews))*100);

        const posTop = Array.from(g.kwPos.entries()).sort((a,b)=>b[1]-a[1]).slice(0,2).map(x=>`+${x[0]}`).join(" ");
        const negTop = Array.from(g.kwNeg.entries()).sort((a,b)=>b[1]-a[1]).slice(0,2).map(x=>`-${x[0]}`).join(" ");
        const kwds = (posTop || negTop) ? `${posTop}${posTop && negTop ? " · " : ""}${negTop}` : "-";

        return { ...g, sizeRate, lowRate, kwds };
      });

      const rankSize = rows.slice().sort((a,b)=> b.sizeRate - a.sizeRate || b.reviews - a.reviews);
      const rankLow  = rows.slice().sort((a,b)=> b.lowRate  - a.lowRate  || b.reviews - a.reviews);
      const rankBoth = rows.slice().sort((a,b)=> ((b.sizeRate+b.lowRate) - (a.sizeRate+a.lowRate)) || b.reviews - a.reviews);

      // 옵션(사이즈) 이슈율: Top5 + OS 제외
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
        });

      sizeOpts = sizeOpts.slice(0, 5);

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
      const periodText = META?.period_text || META?.date_range || "-";
      document.getElementById("runDateSide").textContent = String(runDate).slice(0,10).replaceAll("-",".");
      document.getElementById("periodTextSide").textContent = periodText;
      document.getElementById("headerMeta").textContent = `${runDate} · ${periodText} · 주 1회 자동 업데이트(월 09:00)`;
    }

    function renderSummary(metrics){
      document.getElementById("sizeMentionRate").textContent = metrics.sizeMentionRate;

      // NEG / POS TOP5 (클릭 -> 검색 + 스크롤)
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

    function renderProductMindmap(){
      const mm = META?.product_mindmap_1y || [];
      const root = document.getElementById("productMindmap");
      if (!mm.length){
        root.innerHTML = `<div class="summary-card lg:col-span-3"><div class="text-sm font-black text-slate-700">마인드맵 데이터가 없습니다.</div></div>`;
        return;
      }

      root.innerHTML = mm.map(p => {
        const img = p.local_product_image
          ? `<img src="${esc(p.local_product_image)}" alt="" class="w-14 h-14 rounded-2xl object-cover border border-white/80 bg-white/60" />`
          : `<div class="w-14 h-14 rounded-2xl flex items-center justify-center text-[10px] text-slate-400 border border-white/80 bg-white/60">NO IMAGE</div>`;

        const pos = (p.pos_keywords || []).map(x => `
          <span class="badge pos kw-chip" onclick="applyKeywordAndScroll('${esc(x.k)}','${esc(x.rid||"")}')">
            #${esc(x.k)} <span class="opacity-70">${esc(x.c)}</span>
          </span>
        `).join("");

        const neg = (p.neg_keywords || []).map(x => `
          <span class="badge neg kw-chip" onclick="applyKeywordAndScroll('${esc(x.k)}','${esc(x.rid||"")}')">
            #${esc(x.k)} <span class="opacity-70">${esc(x.c)}</span>
          </span>
        `).join("");

        return `
          <div class="summary-card">
            <div class="flex items-start gap-3">
              ${img}
              <div class="min-w-0">
                <div class="font-black text-slate-900 line-clamp-2">${esc(p.product_name || p.product_code)}</div>
                <div class="text-[11px] font-black text-slate-500 mt-1">code: ${esc(p.product_code)} · 1y reviews: ${esc(p.review_cnt_1y)}</div>
              </div>
            </div>

            <div class="mt-4">
              <div class="text-xs font-black text-slate-500">POS</div>
              <div class="mt-2 flex flex-wrap gap-2">${pos || `<span class="text-xs font-bold text-slate-400">-</span>`}</div>
            </div>

            <div class="mt-4">
              <div class="text-xs font-black text-slate-500">NEG</div>
              <div class="mt-2 flex flex-wrap gap-2">${neg || `<span class="text-xs font-bold text-slate-400">-</span>`}</div>
            </div>
          </div>
        `;
      }).join("");
    }

    function reviewCardHTML(r){
      const tags = [];
      const t = asArr(r.tags);

      if (t.includes("pos")) tags.push(`<span class="badge pos"><i class="fa-solid fa-face-smile"></i> #긍정키워드</span>`);
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

      return `
        <div class="review-card" id="review-${esc(r.id)}">
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


def within_days(row: Dict[str, Any], start_d, end_d) -> bool:
    dt = parse_created_at_iso(str(row.get("created_at") or ""))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz.gettz(OUTPUT_TZ))
    d = dt.astimezone(tz.gettz(OUTPUT_TZ)).date()
    return start_d <= d <= end_d


def first_review_id_for_term(rows: List[Dict[str, Any]], term: str) -> Optional[Any]:
    kw = normalize_text(term)
    for r in rows:
        if kw in normalize_text(str(r.get("text") or "")):
            return r.get("id")
    return None


# ----------------------------
# Main builder
# ----------------------------
def main(input_path: str):
    inp = pathlib.Path(input_path).expanduser().resolve()
    if not inp.exists():
        raise FileNotFoundError(f"input not found: {inp}")

    all_rows = read_reviews_json(inp)

    now = now_kst()

    # ✅ 최근 7일
    start7 = (now - timedelta(days=6)).date()  # 오늘 포함 7일
    end7 = now.date()
    rows7 = [r for r in all_rows if within_days(r, start7, end7)]
    if not rows7:
        raise SystemExit("최근 7일에 해당하는 리뷰가 없습니다. created_at 포맷/타임존/기간을 확인해 주세요.")

    # ✅ 최근 1년(섹션4 누적)
    start1y = (now - timedelta(days=365)).date()
    end1y = end7
    rows1y = [r for r in all_rows if within_days(r, start1y, end1y)]

    # ✅ 제품군/카테고리별 자동 stopwords 학습(최근 1년 기반이 더 안정적)
    stopwords = build_dynamic_stopwords(rows1y if rows1y else rows7, BASE_STOPWORDS, per_category=True)

    # DataFrame (최근7일)
    df7 = pd.DataFrame(rows7).copy()
    df7["rating"] = pd.to_numeric(df7.get("rating"), errors="coerce").fillna(0).astype(int)

    # ----------------------------
    # NEG/POS 키워드 Top5 (최근7일)
    # ----------------------------
    complaint_rows7 = [r for r in rows7 if is_complaint(r)]
    positive_rows7 = [r for r in rows7 if is_positive(r) and not is_complaint(r)]

    neg_texts = [str(r.get("text") or "") for r in complaint_rows7]
    pos_texts = [str(r.get("text") or "") for r in positive_rows7]

    neg_top5 = top_terms(neg_texts, stopwords=stopwords, topk=5) if neg_texts else []
    pos_top5 = top_terms(pos_texts, stopwords=stopwords, topk=5) if pos_texts else []

    # 클릭 이동용 대표 review id 연결 + POS 예시 스니펫
    neg_top5_pack = []
    for k, c in neg_top5:
        rid = first_review_id_for_term(rows7, k)
        neg_top5_pack.append([k, c, rid])

    pos_top5_pack = []
    pos_examples: Dict[str, List[Dict[str, Any]]] = {}
    for k, c in pos_top5:
        rid = first_review_id_for_term(rows7, k)
        pos_top5_pack.append([k, c, rid])
        pos_examples[k] = example_snips_for_keyword(positive_rows7, k, limit=2)

    # ----------------------------
    # 최근7일: 제품별 pos/neg 토큰(랭킹 "주요 키워드"에 사용)
    # ----------------------------
    def attach_pos_neg_keywords_per_review(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out = []
        for r in rows:
            txt = str(r.get("text") or "")
            toks = tokenize_ko(txt, stopwords=stopwords)
            # 리뷰 단위에서는 top 몇개만 붙여서 JS에서 제품 누적하기 쉽게
            # POS 리뷰면 pos_keywords에, NEG(complaint)이면 neg_keywords에
            if is_complaint(r):
                r["neg_keywords"] = toks[:6]
                r["pos_keywords"] = []
            elif is_positive(r):
                r["pos_keywords"] = toks[:6]
                r["neg_keywords"] = []
            else:
                r["pos_keywords"] = []
                r["neg_keywords"] = []
            out.append(r)
        return out

    rows7_enriched = attach_pos_neg_keywords_per_review(rows7)
    df7 = pd.DataFrame(rows7_enriched).copy()
    df7["rating"] = pd.to_numeric(df7.get("rating"), errors="coerce").fillna(0).astype(int)

    # ----------------------------
    # size phrases (최근7일 내 size 태그 기반)
    # ----------------------------
    size_df7 = df7[df7["tags"].apply(lambda x: isinstance(x, list) and ("size" in x))].copy()
    size_texts = size_df7["text"].astype(str).tolist()
    size_phrases = [k for k, _ in top_terms(size_texts, stopwords=stopwords, topk=10)] if len(size_texts) else []

    # ----------------------------
    # 섹션4: 제품별 1년 누적 마인드맵
    # ----------------------------
    product_mindmap_1y = build_product_mindmap_1y(rows1y, stopwords=stopwords, topk_kw=6, max_products=18)

    # 기간 텍스트
    period_text = f"최근 7일 ({start7.isoformat()} ~ {end7.isoformat()})"

    meta = {
        "updated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "period_text": period_text,
        "period_start": start7.isoformat(),
        "period_end": end7.isoformat(),
        "total_reviews": int(len(df7)),
        "neg_top5": neg_top5_pack,
        "pos_top5": pos_top5_pack,
        "pos_examples": pos_examples,
        "size_phrases": size_phrases,
        "fit_words": ["정사이즈", "한치수 크게", "한치수 작게", "타이트", "넉넉", "기장", "소매", "어깨", "가슴", "발볼"],
        "product_mindmap_1y": product_mindmap_1y,
        "debug_stopwords_size": int(len(stopwords)),
    }

    # ✅ 출력 JSON (최근7일만)
    out_reviews = []
    for r in df7.to_dict(orient="records"):
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
                "tags": r.get("tags", []),
                "size_direction": r.get("size_direction", "other"),
                "local_product_image": r.get("local_product_image", ""),
                "local_review_thumb": r.get("local_review_thumb", ""),
                "text_image_path": r.get("text_image_path", ""),
                # ✅ JS 제품 누적용(랭킹 "주요 키워드")
                "pos_keywords": r.get("pos_keywords", []),
                "neg_keywords": r.get("neg_keywords", []),
            }
        )

    (SITE_DATA_DIR / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    (SITE_DATA_DIR / "reviews.json").write_text(json.dumps({"reviews": out_reviews}, ensure_ascii=False, indent=2), encoding="utf-8")
    (SITE_DIR / "index.html").write_text(HTML_TEMPLATE, encoding="utf-8")

    print("[OK] Build done")
    print(f"- Input: {inp}")
    print(f"- Output meta: {SITE_DATA_DIR / 'meta.json'}")
    print(f"- Output reviews: {SITE_DATA_DIR / 'reviews.json'}")
    print(f"- Output html: {SITE_DIR / 'index.html'}")
    print(f"- Period: {period_text}")
    print(f"- NEG Top5: {neg_top5_pack}")
    print(f"- POS Top5: {pos_top5_pack}")
    print(f"- Stopwords size: {len(stopwords)}")
    print(f"- Product mindmap(1y): {len(product_mindmap_1y)} products")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="path to reviews.json (your aggregated JSON)")
    args = ap.parse_args()
    main(args.input)
