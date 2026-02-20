#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
[BUILD VOC DASHBOARD FROM JSON | v3.3 FINAL]
- 입력: reviews.json ({"reviews":[...]})
- 출력:
  - site/data/reviews.json  (최근 7일로 필터된 리뷰만)
  - site/data/meta.json     (period_text 포함, 키워드/이슈클러스터/랭킹 메타)
  - site/index.html         (요구 UI 반영)

✅ 핵심 개선
1) 최근 7일 고정(KST)
2) 긍정/부정 키워드 명확 분리
   - NEG: 불만리뷰 풀에서만 + (강한 부정 신호 co-occur 게이트) + POS 빈발 토큰 제외
   - POS: 긍정리뷰 풀에서만 + 카테고리 stopwords 적용
3) 제품군/카테고리별 STOPWORDS 자동학습
   - 최근 7일 데이터에서 카테고리별로 “너무 자주 나오는 중립 공통어” 자동 제거
4) 이슈 클러스터링(사이즈/품질/배송/CS)
   - Complaint Top5를 “원인(대표 이슈)” 기반으로 더 깔끔하게
5) “주요 문제 키워드” JS 단순추출 제거
   - Python에서 제품별 issue 키워드 Top4 산출 → meta.json 제공 → JS는 이를 우선 표시

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
from typing import Any, Dict, List, Tuple

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

STOPWORDS = set(
    """
그리고 그러나 그래서 하지만 또한
너무 정말 완전 진짜 매우 그냥 조금 약간
저는 제가 우리는 너희 이거 그거 저거
있습니다 입니다 같아요 같네요 하는 하다 됐어요 되었어요 되네요
구매 구입 제품 상품 사용 착용 문의
택배 포장 배송

좋아요 좋다 좋네요 만족 추천 재구매 가성비 최고 굿 예뻐요 이뻐요
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

# POS/NEG 힌트(간단 룰)
POS_HINTS = [
    "만족", "추천", "재구매", "좋", "최고", "굿", "예쁘", "이쁘", "편하", "따뜻", "가볍", "튼튼", "훌륭"
]
NEG_HINTS_STRONG = [
    "불량", "하자", "최악", "실망", "문제", "엉망", "별로", "환불", "반품", "교환", "불편", "아쉽", "냄새", "변색", "오염", "찢", "구멍"
]


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


def tokenize_ko(s: str) -> List[str]:
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
        if t in STOPWORDS:
            continue

        # 보조 어근 컷
        if t in ("있", "좋", "하", "되", "같"):
            continue

        out.append(t)

    return out


def top_terms(texts: List[str], topk: int = 5) -> List[Tuple[str, int]]:
    freq: Dict[str, int] = {}
    for tx in texts:
        for tok in tokenize_ko(tx):
            freq[tok] = freq.get(tok, 0) + 1
    return sorted(freq.items(), key=lambda x: x[1], reverse=True)[:topk]


# ----------------------------
# Category inference (fallback)
# ----------------------------
CATEGORY_RULES = [
    ("bag", [r"\bbackpack\b", r"\bpackable\b", r"백팩", r"가방", r"슬링", r"숄더", r"파우치", r"\bbag\b"]),
    ("shoes", [r"\bchukka\b", r"\bshoe\b", r"신발", r"부츠", r"워커"]),
    ("glove", [r"\bglove\b", r"장갑"]),
    ("fleece", [r"\bfleece\b", r"플리스", r"후리스"]),
    ("top", [r"티셔츠", r"\btee\b", r"\bshirt\b", r"\btop\b"]),
    ("outer", [r"자켓", r"재킷", r"\bjacket\b", r"점퍼", r"\bparka\b"]),
    ("etc", []),
]


def infer_category(row: Dict[str, Any]) -> str:
    for k in ("category", "product_category", "cat", "product_group"):
        v = str(row.get(k) or "").strip()
        if v:
            return v
    name = str(row.get("product_name") or "").lower()
    for cat, pats in CATEGORY_RULES:
        for pat in pats:
            if re.search(pat, name, flags=re.I):
                return cat
    return "etc"


def learn_category_stopwords(rows: List[Dict[str, Any]], min_df_ratio: float = 0.55, min_df: int = 4) -> Dict[str, set]:
    """
    카테고리별로 '너무 자주' 등장하는 토큰을 자동 STOPWORDS로 학습 (최근 7일 기준).
    - df(document frequency) 기준: 한 리뷰에 한 번이라도 나오면 1
    - df_ratio가 높으면 (중립/관용/제품군 공통어일 확률↑) 제거 대상
    """
    by_cat: Dict[str, List[str]] = {}
    for r in rows:
        cat = infer_category(r)
        by_cat.setdefault(cat, []).append(str(r.get("text") or ""))

    cat_sw: Dict[str, set] = {}
    for cat, texts in by_cat.items():
        n = len(texts)
        if n <= 0:
            cat_sw[cat] = set()
            continue

        df: Dict[str, int] = {}
        for tx in texts:
            toks = set(tokenize_ko(tx))
            for tok in toks:
                df[tok] = df.get(tok, 0) + 1

        sw = set()
        for tok, dcnt in df.items():
            if dcnt >= min_df and (dcnt / max(1, n)) >= min_df_ratio:
                sw.add(tok)

        cat_sw[cat] = sw

    return cat_sw


def tokenize_ko_with_cat(s: str, cat: str, cat_stopwords: Dict[str, set]) -> List[str]:
    toks = tokenize_ko(s)
    sw = cat_stopwords.get(cat, set())
    if not sw:
        return toks
    return [t for t in toks if t not in sw]


# ----------------------------
# Issue clustering
# ----------------------------
ISSUE_CLUSTERS = {
    "size": [
        "사이즈", "정사이즈", "작아요", "작다", "커요", "크다", "핏", "타이트", "여유", "끼", "기장", "소매", "어깨", "가슴", "발볼", "헐렁", "오버",
        "짧", "길", "좁", "넓", "조이", "부해"
    ],
    "quality": [
        "불량", "하자", "찢", "구멍", "냄새", "변색", "오염", "실밥", "마감", "품질", "퀄리티", "내구", "재질", "원단", "박음", "지퍼", "고장", "녹",
        "엉망", "별로", "최악", "실망", "문제"
    ],
    "shipping": [
        "배송", "택배", "포장", "지연", "늦", "빠르", "파손", "누락", "오배송", "도착", "출고", "발송", "송장"
    ],
    "cs": [
        "문의", "고객센터", "cs", "상담", "응대", "교환", "반품", "환불", "처리", "접수", "연락", "회수", "센터", "as", "a/s"
    ],
}


def classify_issue(text: str) -> List[str]:
    t = normalize_text(text).replace(" ", "")
    if not t:
        return []
    hits = []
    for issue, kws in ISSUE_CLUSTERS.items():
        for kw in kws:
            if kw.replace(" ", "") in t:
                hits.append(issue)
                break
    return hits


# ----------------------------
# Rule tagging (보강용)
# ----------------------------
SIZE_KEYWORDS = [
    "사이즈", "정사이즈", "작아요", "작다", "커요", "크다", "핏", "타이트", "여유",
    "끼", "기장", "소매", "어깨", "가슴", "발볼", "헐렁", "오버"
]
REQ_KEYWORDS = ["개선", "아쉬", "불편", "했으면", "추가", "보완", "수정", "필요", "요청", "교환", "반품", "환불"]
COMPLAINT_HINTS = ["불량", "하자", "찢", "구멍", "냄새", "변색", "오염", "실밥", "품질", "엉망", "별로", "최악", "실망", "문제"]


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
    """입력 JSON tags를 존중하되, 없거나 약하면 규칙으로 보강."""
    text = str(row.get("text") or "")
    rating = int(row.get("rating") or 0)

    tags = row.get("tags")
    if not isinstance(tags, list):
        tags = []

    # low
    if rating <= 2 and "low" not in tags:
        tags.append("low")

    # pos (긍정은 키워드 필터/탭에서 쓰기 위함)
    if (rating >= 4 or has_any_kw(text, POS_HINTS)) and "pos" not in tags:
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

    # category (optional output)
    if not row.get("category"):
        row["category"] = infer_category(row)

    return row


def is_complaint(row: Dict[str, Any]) -> bool:
    """
    불만리뷰 정의(최근7일 내에서 아래 중 하나면 포함)
    - rating<=2
    - tags에 low/req 포함
    - 텍스트에 불만 힌트(품질/불량 등) 포함 + rating<=3
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
    if rating >= 4:
        return True
    if has_any_kw(text, POS_HINTS) and rating >= 3:
        return True
    return False


# ----------------------------
# Polarized keyword extraction (NEG gate 강화)
# ----------------------------
def top_terms_polarized(
    neg_rows: List[Dict[str, Any]],
    pos_rows: List[Dict[str, Any]],
    cat_stopwords: Dict[str, set],
    topk: int = 5,
    min_count: int = 2,
    pos_penalty_ratio: float = 0.60,
    require_neg_signal: bool = True,
    require_neg_signal_min: int = 1,
) -> List[Tuple[str, int]]:
    """
    NEG 키워드: (불만리뷰에서 자주) AND (긍정리뷰에서 상대적으로 덜) AND (강한 부정 신호 동반)
    - 카테고리별 자동 STOPWORDS 제거
    - POS 빈발 토큰 제외/페널티
    - require_neg_signal=True면, 토큰이 "강불만 리뷰"에서 최소 require_neg_signal_min회 등장해야 함
    """
    neg_f: Dict[str, int] = {}
    pos_f: Dict[str, int] = {}
    neg_signal_f: Dict[str, int] = {}

    for r in neg_rows:
        cat = infer_category(r)
        text = str(r.get("text") or "")
        toks = tokenize_ko_with_cat(text, cat, cat_stopwords)
        for tok in toks:
            neg_f[tok] = neg_f.get(tok, 0) + 1

        rating = int(r.get("rating") or 0)
        strong = (rating <= 2) or has_any_kw(text, NEG_HINTS_STRONG)
        if strong:
            for tok in toks:
                neg_signal_f[tok] = neg_signal_f.get(tok, 0) + 1

    for r in pos_rows:
        cat = infer_category(r)
        text = str(r.get("text") or "")
        toks = tokenize_ko_with_cat(text, cat, cat_stopwords)
        for tok in toks:
            pos_f[tok] = pos_f.get(tok, 0) + 1

    scored: List[Tuple[str, float, int, int]] = []
    for tok, ncnt in neg_f.items():
        if ncnt < min_count:
            continue

        if require_neg_signal and neg_signal_f.get(tok, 0) < require_neg_signal_min:
            continue

        pcnt = pos_f.get(tok, 0)

        # POS에 너무 많이 나오면 제외
        if pcnt >= int(ncnt * pos_penalty_ratio) and pcnt >= 2:
            continue

        score = (ncnt / (pcnt + 1.0)) * (1.0 + (1.0 if ncnt >= 4 else 0.0))
        scored.append((tok, score, ncnt, pcnt))

    scored.sort(key=lambda x: (x[1], x[2]), reverse=True)
    return [(t, ncnt) for (t, _, ncnt, _) in scored[:topk]]


# ----------------------------
# Mindmap builder (keyword -> evidence reviews)
# ----------------------------
@dataclass
class Evidence:
    id: Any
    product_name: str
    product_code: str
    created_at: str
    rating: int
    text_snip: str


def build_mindmap(
    complaint_rows: List[Dict[str, Any]],
    cat_stopwords: Dict[str, set],
    topk_keywords: int = 8,
    evidence_per_kw: int = 4
):
    texts = [str(r.get("text") or "") for r in complaint_rows]
    # mindmap 키워드는 “불만리뷰 토큰” 기준(카테고리 SW는 증거 선별 단계에서 적용)
    kw_counts = top_terms(texts, topk=topk_keywords)

    mindmap = []
    for kw, cnt in kw_counts:
        evs: List[Evidence] = []
        for r in complaint_rows:
            cat = infer_category(r)
            toks = tokenize_ko_with_cat(str(r.get("text") or ""), cat, cat_stopwords)
            if kw not in toks:
                continue

            snip = str(r.get("text") or "").strip()
            snip = re.sub(r"\s+", " ", snip)
            snip = snip[:120] + ("…" if len(snip) > 120 else "")
            evs.append(
                Evidence(
                    id=r.get("id"),
                    product_name=str(r.get("product_name") or r.get("product_code") or "-"),
                    product_code=str(r.get("product_code") or "-"),
                    created_at=str(r.get("created_at") or ""),
                    rating=int(r.get("rating") or 0),
                    text_snip=snip,
                )
            )

        evs.sort(key=lambda x: x.created_at, reverse=True)
        evs = evs[:evidence_per_kw]
        mindmap.append(
            {"keyword": kw, "count": cnt, "evidence": [e.__dict__ for e in evs]}
        )
    return mindmap


# ----------------------------
# HTML template (JS 일부 수정 반영)
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
    .review-list{ display:grid; grid-template-columns: 1fr; gap: 14px; }
    @media (min-width: 1024px){ .review-list{ grid-template-columns: 1fr 1fr; } }
    body.embedded aside, body.embedded header { display:none !important; }
    body.embedded main{ padding: 24px !important; }
    .line-clamp-1{ overflow:hidden; display:-webkit-box; -webkit-line-clamp:1; -webkit-box-orient:vertical; }
    .line-clamp-2{ overflow:hidden; display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; }
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
        * 이 레이아웃은 정적 HTML. <br/>
        실제 데이터는 JSON(data/meta.json, data/reviews.json)에서 로드됩니다.
      </div>
    </div>

    <div class="mt-auto pt-8 text-xs font-bold text-slate-500">
      <div class="small-label text-blue-600 mb-2">Snapshot</div>
      <div>수집일: <span id="runDateSide" class="font-black text-slate-700">-</span></div>
      <div class="mt-2">기간: <span id="periodTextSide" class="font-black text-slate-700">-</span></div>
    </div>
  </aside>

  <main class="flex-1 p-8 md:p-14">
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
            * 최근 7일 고정 + (카테고리 stopwords + 이슈클러스터 + 부정게이트)
          </div>
        </div>

        <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
          <div class="summary-card">
            <div class="small-label text-blue-600 mb-2">1-1) Size mention</div>
            <div class="text-3xl font-black"><span id="sizeMentionRate">-</span>%</div>
            <div class="text-xs font-bold text-slate-500 mt-2">전체 리뷰 중 사이즈 관련 언급 비중</div>
          </div>

          <div class="summary-card">
            <div class="small-label text-red-600 mb-2">1-2) Complaint Top 5</div>
            <div id="topComplaints" class="mt-2 flex flex-wrap gap-2"></div>
            <div id="complaintSub" class="text-xs font-bold text-slate-500 mt-3">-</div>
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
                <th class="text-left">주요 문제 키워드</th>
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
            <div class="small-label text-blue-600 mb-2">4. 대표 리뷰 마인드맵</div>
            <div class="text-xs font-bold text-slate-500 mt-1">키워드 → 근거 리뷰 연결</div>
          </div>
        </div>

        <div id="mindmap" class="grid grid-cols-1 lg:grid-cols-3 gap-4"></div>
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

        <div id="dailyFeed" class="space-y-4"></div>

        <div class="hidden review-card text-center" id="noResults">
          <div class="text-lg font-black text-slate-800">검색 결과가 없습니다.</div>
        </div>
      </div>
    </section>

    <footer class="text-xs font-bold text-slate-500 pb-8">
      * 데이터 소스: reviews.json (최근 7일로 필터링 후 렌더).<br/>
      * 키워드: 최근 7일 불만리뷰 기반 + 카테고리 공통 중립어 자동 제거 + 이슈클러스터 기반.
    </footer>

  </main>

  <script>
    const overlay = document.getElementById('overlay');
    const overlayMsg = document.getElementById('overlayMsg');

    const uiState = {
      sourceTab: 'combined',
      chips: { daily:true, pos:false, size:false, low:false },
      rankMode: 'size',
      rankExpanded: false, // ✅ Top5 기본
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
            issueKwds: new Map()
          });
        }
        const g = byProd.get(code);
        g.reviews += 1;
        if (asArr(r.tags).includes("size")) g.sizeIssue += 1;
        if ((r.rating||0) <= 2 || asArr(r.tags).includes("low")) g.low += 1;

        // (fallback) issue keywords - JS 방식은 보조로만 남김
        if ((r.rating||0) <= 2 || asArr(r.tags).includes("low") || asArr(r.tags).includes("req")){
          const words = (r.text||"").match(/[가-힣A-Za-z0-9]{2,}/g) || [];
          for (const w of words.slice(0, 50)){
            const k = w.toLowerCase();
            if (k.length < 2) continue;
            if (["제품","상품","구매","사이즈","배송","택배","포장"].includes(k)) continue;
            g.issueKwds.set(k, (g.issueKwds.get(k)||0) + 1);
          }
        }
      }

      const rows = Array.from(byProd.values()).map(g => {
        const sizeRate = Math.round((g.sizeIssue / Math.max(1,g.reviews))*100);
        const lowRate  = Math.round((g.low / Math.max(1,g.reviews))*100);

        // ✅ Python 정제 키워드 우선
        const pyKw = (META && META.product_issue_keywords && META.product_issue_keywords[g.product_code])
          ? META.product_issue_keywords[g.product_code]
          : null;

        const kwds = (pyKw && pyKw.length)
          ? pyKw.slice(0,4).join(", ")
          : Array.from(g.issueKwds.entries())
              .sort((a,b)=>b[1]-a[1])
              .slice(0,4)
              .map(x=>x[0])
              .join(", ");

        return { ...g, sizeRate, lowRate, kwds };
      });

      const rankSize = rows.slice().sort((a,b)=> b.sizeRate - a.sizeRate || b.reviews - a.reviews);
      const rankLow  = rows.slice().sort((a,b)=> b.lowRate  - a.lowRate  || b.reviews - a.reviews);
      const rankBoth = rows.slice().sort((a,b)=> ((b.sizeRate+b.lowRate) - (a.sizeRate+a.lowRate)) || b.reviews - a.reviews);

      // ✅ 옵션(사이즈) 이슈율: Top5 + OS 제외
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

      // ✅ Complaint Top5 = 대표 이슈 기반(원인 기반) 우선, 없으면 neg_top5 fallback
      const issue = META?.primary_issue || "quality";
      const top = META?.primary_issue_top5?.length ? META.primary_issue_top5 : (META?.neg_top5 || []);
      const el = document.getElementById("topComplaints");
      el.innerHTML = top.map(([k,c]) => `<span class="badge neg">#${esc(k)} <span class="opacity-70">${esc(c)}</span></span>`).join("");

      const sub = document.getElementById("complaintSub");
      const map = {size:"사이즈", quality:"품질", shipping:"배송", cs:"CS"};
      sub.textContent = `최근 7일 불만리뷰 기반 · 대표 이슈: ${map[issue] || issue}`;

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

    function renderMindmap(){
      const mm = META?.mindmap || [];
      const root = document.getElementById("mindmap");
      if (!mm.length){
        root.innerHTML = `<div class="summary-card lg:col-span-3"><div class="text-sm font-black text-slate-700">마인드맵 데이터가 없습니다.</div></div>`;
        return;
      }

      root.innerHTML = mm.map(node => {
        const ev = node.evidence || [];
        const items = ev.map(e => `
          <div class="mt-2 p-3 rounded-2xl bg-white/60 border border-white/80">
            <div class="flex items-center justify-between">
              <div class="text-xs font-black text-slate-800 line-clamp-1">${esc(e.product_name)}</div>
              <div class="text-[11px] font-black text-slate-500">★ ${esc(e.rating)} · ${esc(String(e.created_at||"").slice(0,10))}</div>
            </div>
            <div class="text-[11px] font-black text-slate-500 mt-1">code: ${esc(e.product_code)} · id: ${esc(e.id)}</div>
            <div class="text-xs font-extrabold text-slate-700 mt-2 leading-relaxed">${esc(e.text_snip || "")}</div>
          </div>
        `).join("");

        return `
          <div class="summary-card">
            <div class="flex items-center justify-between">
              <span class="badge neg">#${esc(node.keyword)}</span>
              <span class="text-xs font-black text-slate-500">${esc(node.count)}x</span>
            </div>
            <div class="text-xs font-bold text-slate-500 mt-2">근거 리뷰</div>
            ${items || `<div class="text-xs font-bold text-slate-400 mt-2">-</div>`}
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
        <div class="review-card">
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

          <div class="mt-3 text-sm font-extrabold text-slate-800 leading-relaxed whitespace-pre-wrap break-words">
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
      renderMindmap();
      renderDailyFeed(filtered);
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
# Main builder
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


def main(input_path: str):
    inp = pathlib.Path(input_path).expanduser().resolve()
    if not inp.exists():
        raise FileNotFoundError(f"input not found: {inp}")

    all_rows = read_reviews_json(inp)

    # ✅ 최근 7일 고정 (KST) : 오늘 포함 7일
    now = now_kst()
    start = (now - timedelta(days=6)).date()
    end = now.date()

    def in_last7(r: Dict[str, Any]) -> bool:
        dt = parse_created_at_iso(str(r.get("created_at") or ""))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tz.gettz(OUTPUT_TZ))
        d = dt.astimezone(tz.gettz(OUTPUT_TZ)).date()
        return start <= d <= end

    rows = [r for r in all_rows if in_last7(r)]
    if not rows:
        raise SystemExit("최근 7일에 해당하는 리뷰가 없습니다. created_at 포맷/타임존/기간을 확인해 주세요.")

    # ✅ 카테고리별 자동 STOPWORDS 학습(최근7일 기준)
    cat_stopwords = learn_category_stopwords(rows, min_df_ratio=0.55, min_df=4)

    df = pd.DataFrame(rows).copy()
    df["rating"] = pd.to_numeric(df.get("rating"), errors="coerce").fillna(0).astype(int)

    # ✅ 불만/긍정 풀 구성
    complaint_df = df[df.apply(lambda x: is_complaint(x.to_dict()), axis=1)].copy()
    positive_df = df[df.apply(lambda x: is_positive(x.to_dict()), axis=1)].copy()

    complaint_rows = complaint_df.to_dict(orient="records")
    positive_rows = positive_df.to_dict(orient="records")

    # ✅ NEG/POS Top5 (카테고리 stopwords + 부정게이트 + pos페널티)
    neg_top5 = top_terms_polarized(
        neg_rows=complaint_rows,
        pos_rows=positive_rows,
        cat_stopwords=cat_stopwords,
        topk=5,
        min_count=2,
        pos_penalty_ratio=0.60,
        require_neg_signal=True,
        require_neg_signal_min=1,
    )

    # POS Top5도 카테고리 stopwords 적용해서 뽑기
    pos_texts_cat = []
    for r in positive_rows:
        cat = infer_category(r)
        toks = tokenize_ko_with_cat(str(r.get("text") or ""), cat, cat_stopwords)
        pos_texts_cat.append(" ".join(toks))
    pos_top5 = top_terms(pos_texts_cat, topk=5)

    # ✅ 이슈 클러스터링(사이즈/품질/배송/CS) + 대표 이슈 기반 Top5
    issue_counts = {k: 0 for k in ISSUE_CLUSTERS.keys()}
    issue_rows = {k: [] for k in ISSUE_CLUSTERS.keys()}

    for r in complaint_rows:
        issues = classify_issue(str(r.get("text") or ""))
        for it in issues:
            issue_counts[it] += 1
            issue_rows[it].append(r)

    issue_top_terms: Dict[str, List[Tuple[str, int]]] = {}
    for it, rws in issue_rows.items():
        packed = []
        for rr in rws:
            cat = infer_category(rr)
            toks = tokenize_ko_with_cat(str(rr.get("text") or ""), cat, cat_stopwords)
            packed.append(" ".join(toks))
        issue_top_terms[it] = top_terms(packed, topk=5) if packed else []

    if sum(issue_counts.values()) > 0:
        primary_issue = sorted(issue_counts.items(), key=lambda x: x[1], reverse=True)[0][0]
    else:
        primary_issue = "quality"

    primary_issue_top5 = issue_top_terms.get(primary_issue, [])
    # 대표 이슈 top5가 비면(데이터 적거나 전부 필터), neg_top5로 fallback
    if not primary_issue_top5:
        primary_issue_top5 = neg_top5

    # (옵션) size phrases도 최근7일 내 size 태그 기반
    size_df = df[df["tags"].apply(lambda x: isinstance(x, list) and ("size" in x))].copy()
    size_texts = size_df["text"].astype(str).tolist()
    size_phrases = [k for k, _ in top_terms(size_texts, topk=10)]

    # mindmap (불만리뷰 기반)
    mindmap = build_mindmap(complaint_rows, cat_stopwords=cat_stopwords, topk_keywords=8, evidence_per_kw=4)

    # ✅ 제품별 "주요 문제 키워드"(Python 정제) → meta.json 제공
    pos_freq_global: Dict[str, int] = {}
    for r in positive_rows:
        cat = infer_category(r)
        for tok in tokenize_ko_with_cat(str(r.get("text") or ""), cat, cat_stopwords):
            pos_freq_global[tok] = pos_freq_global.get(tok, 0) + 1

    by_prod_freq: Dict[str, Dict[str, int]] = {}
    for r in complaint_rows:
        code = str(r.get("product_code") or "-")
        cat = infer_category(r)
        toks = tokenize_ko_with_cat(str(r.get("text") or ""), cat, cat_stopwords)

        if code not in by_prod_freq:
            by_prod_freq[code] = {}
        for tok in toks:
            # POS에서 너무 흔하면 중립/칭찬/상용구일 확률↑ → 제외
            if pos_freq_global.get(tok, 0) >= 3:
                continue
            by_prod_freq[code][tok] = by_prod_freq[code].get(tok, 0) + 1

    product_issue_keywords: Dict[str, List[str]] = {}
    for code, f in by_prod_freq.items():
        top4 = sorted(f.items(), key=lambda x: x[1], reverse=True)[:4]
        product_issue_keywords[code] = [k for k, _ in top4]

    period_text = f"최근 7일 ({start.isoformat()} ~ {end.isoformat()})"

    # 카테고리 stopwords 샘플(디버깅/검증용)
    cat_sw_sample = {k: sorted(list(v))[:20] for k, v in cat_stopwords.items()}

    meta = {
        "updated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "period_text": period_text,
        "period_start": start.isoformat(),
        "period_end": end.isoformat(),
        "total_reviews": int(len(df)),

        # ✅ polarity
        "neg_top5": neg_top5,
        "pos_top5": pos_top5,

        # ✅ issue clustering
        "issue_counts": issue_counts,
        "issue_top_terms": issue_top_terms,
        "primary_issue": primary_issue,
        "primary_issue_top5": primary_issue_top5,

        # ✅ product issue keywords
        "product_issue_keywords": product_issue_keywords,

        # ✅ other
        "size_phrases": size_phrases,
        "fit_words": ["정사이즈", "한치수 크게", "한치수 작게", "타이트", "넉넉", "기장", "소매", "어깨", "가슴", "발볼"],
        "mindmap": mindmap,

        # debug
        "category_stopwords_sample": cat_sw_sample,
    }

    # ✅ 출력 JSON (최근7일로 필터된 것만)
    out_reviews = []
    for r in df.to_dict(orient="records"):
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
                "category": r.get("category", infer_category(r)),
                "local_product_image": r.get("local_product_image", ""),
                "local_review_thumb": r.get("local_review_thumb", ""),
                "text_image_path": r.get("text_image_path", ""),
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
    print(f"- NEG Top5: {neg_top5}")
    print(f"- POS Top5: {pos_top5}")
    print(f"- Primary issue: {primary_issue} / Top5: {primary_issue_top5}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="path to reviews.json (your aggregated JSON)")
    args = ap.parse_args()
    main(args.input)
