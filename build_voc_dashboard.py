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
DEFAULT_TARGET_DAYS = int(os.getenv("TARGET_DAYS", "90") or "90")  # last N days including today (default: ~3 months)

AUTO_STOP_MIN_DF = 0.30
AUTO_STOP_MIN_PRODUCTS = 0.25
AUTO_STOP_POLARITY_MARGIN = 0.20
AUTO_STOP_MAX_ADD = 120

TOPK_POS = 5
TOPK_NEG = 5

MINDMAP_SENT_PER_SIDE = 2
MINDMAP_MAX_PRODUCTS = 24

CLUSTERS = {
    "size": ["ì¬ì´ì¦", "ì ì¬ì´ì¦", "ì", "í¬", "íì´í¸", "íë ", "ë¼", "ê¸°ì¥", "ìë§¤", "ì´ê¹¨", "ê°ì´", "ë°ë³¼", "í"],
    "quality": ["íì§", "ë¶ë", "íì", "ì°¢", "êµ¬ë©", "ì¤ë°¥", "ì¤ì¼", "ë³ì", "ëì", "ë§ê°", "ë´êµ¬", "íë¦¬í°"],
    "shipping": ["ë°°ì¡", "íë°°", "ì¶ê³ ", "ëì°©", "ì§ì°", "ë¦", "ë¹ ë¥´", "í¬ì¥", "íì"],
    "cs": ["ë¬¸ì", "ìë", "ê³ ê°", "cs", "êµí", "ë°í", "íë¶", "ì²ë¦¬", "as"],
    "price": ["ê°ê²©", "ë¹ì¸", "ì¸", "ê°ì±ë¹", "í ì¸", "ì¿ í°", "ëë¹"],
    "design": ["ëìì¸", "ì", "ì»¬ë¬", "ìì", "ë©", "ì¤íì¼", "íê°"],
    "function": ["ìë©", "ë£", "í¬ì¼", "ê³µê°", "ê°ë³", "ë¬´ê²", "ë°ë»", "ë³´ì¨", "ë°©ì", "ê¸°ë¥", "í¸í", "ì°©ì©ê°", "ê·¸ë¦½", "ì£¼ë¨¸ë"],
}

CATEGORY_RULES = [
    ("bag", re.compile(r"(backpack|rucksack|bag|pouch|shoulder|packable|duffel|tote|íì|ë°±í©|ê°ë°©|íì°ì¹|ìë)", re.I)),
    ("shoe", re.compile(r"(shoe|boot|chukka|sneaker|wide|waterproof|ì ë°|ë¶ì¸ |ìì»¤)", re.I)),
    ("top", re.compile(r"(fleece|jacket|hood|tee|turtle|shirt|ìì|ìì¼|íë¦¬ì¤|íë|í°|í°í)", re.I)),
    ("bottom", re.compile(r"(pant|short|skirt|íì|ë°ì§|í¬ì¸ |ì¼ì¸ )", re.I)),
    ("glove", re.compile(r"(glove|ì¥ê°)", re.I)),
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
    return s  # ê¸°í ìì¤ëªì ê·¸ëë¡ ì ì§


BASE_STOPWORDS = set(
    """
    ê·¸ë¦¬ê³  ê·¸ë¬ë ê·¸ëì íì§ë§ ëí
    ëë¬´ ì ë§ ìì  ì§ì§ ë§¤ì° ê·¸ë¥ ì¡°ê¸ ì½ê°
    ì ë ì ê° ì°ë¦¬ë ëí¬ ì´ê±° ê·¸ê±° ì ê±°
    ììµëë¤ ìëë¤ ê°ìì ê°ë¤ì íë íë¤ ëì´ì ëìì´ì ëë¤ì
    êµ¬ë§¤ êµ¬ì ì£¼ë¬¸ êµ¬ë§¤í´ êµ¬ìí´ ìì´ì ììµëë¤ ì£¼ë¬¸í
    ì í ìí ë¬¼ê±´
    ì¬ì©ì¤ ì¬ì© ì¬ì©í¨ ì°©ì© ìì´ ì ì´ ì¨ë´¤
    ë°°ì¡ íë°° í¬ì¥
    ë¬¸ì
    ì¢ìì ì¢ë¤ ì¢ë¤ì ë§ì¡± ì¶ì² ì¬êµ¬ë§¤ ê°ì±ë¹ ìµê³  êµ¿
    ìë»ì ì´ë»ì
    ì ì¬ì´ì¦ íì¹ì í ì¹ì
    ì»¬ë¬ ìì ëìì¸
    ìì´ì ìì´ìì ìì´ì ìë¤ì ììì´ì
    ì¢ìµëë¤ ì¢ìì´ì
    ì¶ê° ì¶ê°ë¡
    ê°ë¥ ê°ë¥í´ì
    íì¸ íì¸í´ì
    ìê° ìê°í´ì
    ëë ëëì´ìì
    ì ë ì ëë¡
    ë¶ë¶ ë¶ë¶ì´
    ì¬ë ë¶ë¤
    ê¸°ì¡´ ê°ì ëì¼ ë¹ì· ë¹ì·í ìë
    ì´ë² ì´ë²ì ì´ë²ì
    ë³´ê¸° ë§ì´ ìíì´ì ìíë¤ì
    """.split()
)

JOSA = ["ì", "ë", "ì´", "ê°", "ì", "ë¥¼", "ì", "ìì", "ìê²", "ì¼ë¡", "ë¡", "ì", "ê³¼", "ë", "ë§", "ê¹ì§", "ë¶í°", "ë³´ë¤", "ì²ë¼", "ê°ì´", "ì´ë", "ë"]
ENDING = ["ìëë¤", "ìµëë¤", "íì´ì", "íë¤ì", "í´ì", "íë¤ì", "í©ëë¤", "ê°ìì", "ê°ë¤ì", "ìì´ì", "ìë¤ì", "ììµëë¤"]

RE_URL = re.compile(r"https?://\S+|www\.\S+", re.I)
RE_HASHTAG = re.compile(r"#[A-Za-z0-9ê°-í£_]+")
RE_EMOJI_ETC = re.compile(r"[^\w\sê°-í£]", re.UNICODE)


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
    toks = re.findall(r"[ê°-í£A-Za-z0-9]+", s)
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
        if t in ("ì", "ì¢", "í", "ë", "ê°", "í", "í´", "í¨"):
            continue
        out.append(t)
    return out


SIZE_KEYWORDS = ["ì¬ì´ì¦", "ì ì¬ì´ì¦", "ììì", "ìë¤", "ì»¤ì", "í¬ë¤", "í", "íì´í¸", "ì¬ì ", "ë¼", "ê¸°ì¥", "ìë§¤", "ì´ê¹¨", "ê°ì´", "ë°ë³¼", "íë ", "ì¤ë²", "ì", "ë¤ì´", "íì¹ì", "ë°ì¹ì"]
REQ_WEAK = ["ê°ì ", "ìì¬", "íì¼ë©´", "ë³´ì", "ìì ", "íì", "ìì²­"]
REQ_STRONG = ["êµí", "ë°í", "íë¶", "as", "ì²ë¦¬", "ì¬ë°°ì¡", "ì¬ë°ì¡"]
COMPLAINT_HINTS = ["ë¶ë", "íì", "ì°¢", "êµ¬ë©", "ëì", "ë³ì", "ì¤ì¼", "ì¤ë°¥", "íì§", "ìë§", "ë³ë¡", "ìµì", "ì¤ë§", "ë¬¸ì ", "ë¶í¸", "íì", "ì§ì°", "ë¦", "ìì", "ìì´"]

POS_SEEDS = [
    "ê°ë³", "í¸í", "í¸ì", "ì°©ì©ê°", "ìë©", "í¬ì¼", "ê³µê°", "ì£¼ë¨¸ë", "ë£",
    "ë°ë»", "ë³´ì¨", "ë°©ì", "í¼í¼", "ê²¬ê³ ", "ë§ì¡±", "ìì", "ë©", "ê¹ë", "ë§ê°",
    "ìë§", "ë±", "ì¿ ì", "ì¶ì²"
]
NEG_SEEDS = [
    "ë¶ë", "íì", "ì°¢", "êµ¬ë©", "ëì", "ë³ì", "ì¤ì¼", "ì¤ë°¥", "ìµì", "ë³ë¡", "ì¤ë§",
    "ë¶í¸", "ë¬¸ì ", "ìì½", "ë¬´ê²",
    "ì", "í¬", "íì´í¸", "íë ",
    "ì§ì°", "ë¦", "íì", "ìë", "íë¶", "êµí", "ë°í", "as"
]

HARD_DEFECT = ["ë¶ë", "íì", "ì°¢", "êµ¬ë©", "íì", "ëì", "ë³ì", "ì¤ì¼", "ì ì°©", "í°ì§"]
DEFECT_ACTION = ["êµí", "ë°í", "íë¶", "as", "ì²ë¦¬", "ë¶í¸", "ë¬¸ì ", "ì¤ë§", "ë¬¸ì", "ìë", "ì ì"]

NEGATION = ["ì", "ì", "ì", "ëª»", "ìë", "ë³ë¡ì", "ì íì"]


def has_any_kw(text: str, kws: List[str]) -> bool:
    t = (text or "").replace(" ", "")
    for kw in kws:
        if kw.replace(" ", "") in t:
            return True
    return False


def classify_size_direction(text: str) -> str:
    t = (text or "").replace(" ", "")
    small_kw = ["ìì", "ìë¤", "íì´í¸", "ë¼", "ì¡°ì¸ë¤", "ì§§ë¤", "ì¢ë¤", "ë°ë³¼ì¢", "ì´ê¹¨ì¢", "ê°ì´ì¢", "ë¤ì´", "íì¹ìì", "ë°ì¹ìì"]
    big_kw = ["ì»¤", "í¬ë¤", "ëë", "ì¤ë²", "ê¸¸ë¤", "ëë¤", "íë ", "ë¶í´", "ì", "íì¹ìí°", "ë°ì¹ìí°"]
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
    if ("size" not in tags) and (has_any_kw(text, SIZE_KEYWORDS) or fit_q in ("ì¡°ê¸ ììì", "ììì", "ì¡°ê¸ ì»¤ì", "ì»¤ì")):
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
        severe = any(x in t for x in ["íë¶", "ë°í", "êµí", "as", "ë¶ë", "íì", "íì", "ì§ì°", "ë¦", "ìëë³ë¡", "ì²ë¦¬ì"])
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
                snip = snip[:140] + ("â¦" if len(snip) > 140 else "")
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


SENT_SPLIT = re.compile(r"(?<=[\.\?\!]|[ã]|[ï¼ï¼]|[!?\n])\s+|[\n\r]+")


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
            p = p[:160].rstrip() + "â¦"
        out.append(p)
    return out


TRIVIAL_PHRASES = [
    "ìíì´ì", "ë³´ê¸°", "ë§ì´", "ì¬ì©ì¤", "êµ¬ìí´ì", "êµ¬ë§¤í´ì", "ìëë°", "ì£¼ë¬¸íëë°", "ë°ìëë°",
    "ì¢ìì", "ë§ì¡±", "ì¶ì²", "ì¬êµ¬ë§¤"
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
    if any(x in sent for x in ["ì£¼ë¨¸ë", "ìë©", "ë°ë³¼", "ì´ê¹¨", "ê¸°ì¥", "ìë§¤", "ë³´ì¨", "ë°©ì", "ì°©ì©ê°", "ì", "ë¤ì´"]):
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
        raise ValueError('ìë ¥ JSONì {"reviews": [...]} ííì¬ì¼ í©ëë¤.')

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

    target_days = max(1, int(target_days))
    startN = (now - timedelta(days=target_days - 1)).date()
    endN = now.date()

    start1y = startN
    end1y = endN

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
    for r in rowsN:
        if not is_complaint(r):
            continue
        for h in assign_cluster(str(r.get("text") or "")):
            cluster_counts[h] = cluster_counts.get(h, 0) + 1

    size_rows = [r for r in rowsN if isinstance(r.get("tags"), list) and ("size" in r.get("tags"))]
    size_phrases_terms = top_terms(size_rows, topk=10, auto_sw=auto_sw, which="neg") if size_rows else []
    size_phrases = [k for k, _ in size_phrases_terms]

    product_mindmap_3m = build_product_mindmap_3m_sentence(rows_1y=rows1y, rows_7d=rowsN, per_side=2, max_products=24) if rows1y else []

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

    trend_3m = build_daily_timeseries(rows1y) if rows1y else []
    ml_topics = ml_topics_tfidf_nmf(rows1y, n_topics=8, top_words=8, min_df=3) if rows1y else {"enabled": False, "reason": "no_1y_rows"}
    dl_topics = dl_embeddings_cluster(rows1y, n_clusters=10) if rows1y else {"enabled": False, "reason": "no_1y_rows"}

    
    kw_graph = build_kw_graph(rowsN, auto_sw, max_nodes=28) if rowsN else {"window":"empty","nodes":[],"links":[]}

period_text = f"ìµê·¼ {target_days}ì¼ ({startN.isoformat()} ~ {endN.isoformat()})"
    empty_reason = None
    if not rowsN:
        empty_reason = "No reviews in target window. Check upstream collection, created_at format/timezone, or TARGET_DAYS."

    meta: Dict[str, Any] = {
        "version": "v6.2-3m-ml",
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
        "kw_graph_3m": kw_graph,
        "fit_words": ["ì ì¬ì´ì¦", "íì¹ì í¬ê²", "íì¹ì ìê²", "íì´í¸", "ëë", "ê¸°ì¥", "ìë§¤", "ì´ê¹¨", "ê°ì´", "ë°ë³¼", "ìë©", "ê°ë²¼ì"],
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