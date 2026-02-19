#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_summary_ADV_FINAL_UPGRADED.py  (UI PATCH)

- Generates reports/index.html (embed-friendly)
- Adds "Hero campaign theme keywords" by using:
  1) hero_main_banners_*.csv "title" (primary, always available)
  2) (optional) crawl landing pages via href_clean/href (secondary enrichment)

UI Patch (requested):
- Remove TOP 3 blocks on all cards
- Change HERO theme label to "주요 기획전 키워드"
- Remove "현재 기획전 테마:" prefix
- Show keyword counts like: 키워드(12) · 키워드(9) ...
- Remove "상세 보기" line
- Remove crawl/cache wording from summary card
- Remove subtitle: “3개 자동 리포트에서 …”
- Remove INSIGHT sections from Naver/Hero/VOC
- Remove "Snapshot (KST)" text from period labels
- Make Open report a centered bottom button, aligned consistently across all 3 cards

Env:
- HERO_CRAWL_LIMIT (default 40)
- HERO_CRAWL_SLEEP (default 0.25)
- HERO_CRAWL_TIMEOUT (default 8)
- HERO_CRAWL_RETRIES (default 2)
"""

from __future__ import annotations

import glob
import json
import os
import re
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, List, Tuple

import pandas as pd

# optional deps (only needed for crawling)
try:
    import requests
    from bs4 import BeautifulSoup
except Exception:
    requests = None
    BeautifulSoup = None

KST = timezone(timedelta(hours=9))


# -----------------------
# Helpers
# -----------------------
def now_kst_label() -> str:
    return datetime.now(KST).strftime("%Y.%m.%d (%a) %H:%M KST")


def _read_csv_any(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return pd.read_csv(path)


def _pick_latest(pattern: str) -> Optional[Path]:
    files = [Path(p) for p in glob.glob(pattern)]
    files = [p for p in files if p.is_file()]
    if not files:
        return None
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0]


def _col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {str(c).strip().lower(): c for c in df.columns}
    for cand in candidates:
        key = cand.strip().lower()
        if key in cols:
            return cols[key]
    return None


def fmt_int(x: Any) -> str:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return "-"
        return f"{int(x):,}"
    except Exception:
        return "-"


def fmt_pct(x: Any, digits: int = 0) -> str:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return "-"
        return f"{float(x) * 100:.{digits}f}%"
    except Exception:
        return "-"


def fmt_won(x: Any) -> str:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return "-"
        return f"{int(round(float(x))):,}원"
    except Exception:
        return "-"


def fmt_won_signed(x: Any) -> str:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return "-"
        v = int(round(float(x)))
        sign = "+" if v > 0 else ""
        return f"{sign}{v:,}원"
    except Exception:
        return "-"


def risk_level(diff_pos_ratio: Optional[float]) -> Tuple[str, str]:
    if diff_pos_ratio is None:
        return ("-", "risk-unk")
    if diff_pos_ratio >= 0.50:
        return ("HIGH", "risk-high")
    if diff_pos_ratio >= 0.30:
        return ("MID", "risk-mid")
    return ("LOW", "risk-low")


def safe_str(x: Any) -> str:
    if x is None:
        return ""
    try:
        return str(x)
    except Exception:
        return ""


def _norm_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    if u.lower() in ("nan", "none", "null"):
        return ""
    return u


# -----------------------
# Naver metrics
# -----------------------
def build_naver_metrics(repo_root: Path) -> Dict[str, Any]:
    latest = _pick_latest(str(repo_root / "result_*.csv"))
    if not latest:
        return {
            "total": None,
            "diff_pos": None,
            "diff_pos_ratio": None,
            "avg_gap": None,
            "avg_delta": None,
            "risk_label": "-",
            "risk_class": "risk-unk",
            "updated": now_kst_label(),
        }

    df = _read_csv_any(latest)
    total = len(df)

    c_gap = _col(df, ["가격차이", "gap", "diff"])
    c_prev_delta = _col(df, ["prev_naver_delta", "prev delta", "delta_prev"])

    diff_pos = None
    diff_pos_ratio = None
    avg_gap = None
    avg_delta = None

    if c_gap:
        gap_s = pd.to_numeric(df[c_gap], errors="coerce")
        diff_pos = int((gap_s > 0).sum())
        diff_pos_ratio = (diff_pos / total) if total else None
        avg_gap = float(gap_s.dropna().mean()) if gap_s.dropna().size else None

    if c_prev_delta:
        d = pd.to_numeric(df[c_prev_delta], errors="coerce").dropna()
        if d.size:
            avg_delta = float(d.mean())

    lvl, lvl_cls = risk_level(diff_pos_ratio)

    return {
        "total": total,
        "diff_pos": diff_pos,
        "diff_pos_ratio": diff_pos_ratio,
        "avg_gap": avg_gap,
        "avg_delta": avg_delta,
        "risk_label": lvl,
        "risk_class": lvl_cls,
        "updated": now_kst_label(),
    }


# -----------------------
# Hero keyword crawling + extraction
# -----------------------
STOPWORDS_KO = set([
    "기획전", "이벤트", "프로모션", "혜택", "할인", "특가", "쿠폰", "증정", "사은품",
    "오늘", "이번", "지금", "바로", "최대", "무료", "단독", "한정", "선착순",
    "구매", "상품", "제품", "브랜드", "공식", "스토어", "쇼핑", "온라인", "몰",
])
STOPWORDS_EN = set([
    "sale", "sales", "event", "events", "promo", "promotion", "promotions", "coupon", "coupons",
    "free", "best", "new", "now", "only", "limited", "official", "store", "shop", "shopping",
    "brand", "brands", "collection", "collections", "up", "to", "off", "deal", "deals",
])


def _load_cache(cache_path: Path) -> Dict[str, Any]:
    if not cache_path.exists():
        return {}
    try:
        return json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_cache(cache_path: Path, cache: Dict[str, Any]) -> None:
    try:
        cache_path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def _fetch_page(url: str, timeout: int = 8, retries: int = 2) -> Optional[str]:
    if requests is None:
        return None
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.7",
        "Connection": "close",
    }
    for _ in range(max(1, retries + 1)):
        try:
            r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            if r.status_code >= 400:
                continue
            r.encoding = r.apparent_encoding or r.encoding
            return r.text
        except Exception:
            continue
    return None


def _extract_page_signals(html: str) -> Dict[str, str]:
    """Return key text signals from html (title, og:title, description, h1,h2, text snippet)."""
    if not html or BeautifulSoup is None:
        return {"title": "", "og_title": "", "desc": "", "h": "", "snippet": ""}

    soup = BeautifulSoup(html, "html.parser")

    def _txt(el) -> str:
        if not el:
            return ""
        return " ".join(el.get_text(" ", strip=True).split())

    title = _txt(soup.title) if soup.title else ""
    og_title = ""
    desc = ""

    og = soup.find("meta", attrs={"property": "og:title"})
    if og and og.get("content"):
        og_title = str(og.get("content")).strip()

    md = soup.find("meta", attrs={"name": "description"})
    if md and md.get("content"):
        desc = str(md.get("content")).strip()

    hs = []
    for tag in ["h1", "h2"]:
        for h in soup.find_all(tag)[:3]:
            t = _txt(h)
            if t:
                hs.append(t)
    h_text = " ".join(hs)

    body = soup.body
    snippet = ""
    if body:
        t = _txt(body)
        t = re.sub(r"\s+", " ", t)
        snippet = t[:600]

    return {"title": title, "og_title": og_title, "desc": desc, "h": h_text, "snippet": snippet}


def _tokenize(text: str) -> List[str]:
    text = (text or "").lower()
    tokens = re.findall(r"[a-z]{2,}|[가-힣]{2,}", text)
    out: List[str] = []
    for t in tokens:
        if re.fullmatch(r"[a-z]{2,}", t):
            if t in STOPWORDS_EN:
                continue
        else:
            if t in STOPWORDS_KO:
                continue
        out.append(t)
    return out


def _build_keywords_from_texts(texts: List[str], top_n: int = 10) -> List[Tuple[str, int]]:
    """
    Theme-first extractor:
    - unigram + bigram(2-word)
    """
    flat: List[str] = []
    for tx in texts:
        tokens = _tokenize(tx)
        if not tokens:
            continue
        bigrams = [f"{tokens[i]} {tokens[i + 1]}" for i in range(len(tokens) - 1)]
        flat.extend(tokens)
        flat.extend(bigrams)

    if not flat:
        return []

    s = pd.Series(flat)
    vc = s.value_counts()

    try:
        vc = vc[~vc.index.str.contains(r"\b(http|https|www|img|banner)\b", case=False, regex=True)]
    except Exception:
        pass
    vc = vc[vc.index.map(lambda x: x not in ("www", "http", "https"))]

    return [(idx, int(val)) for idx, val in vc.head(top_n).items()]


def _format_keywords_with_counts(keywords: List[Tuple[str, int]], take: int = 6) -> str:
    if not keywords:
        return "-"
    parts = []
    for k, c in keywords[: max(0, int(take))]:
        k = (k or "").strip()
        if not k:
            continue
        parts.append(f"{k}({c:,})")
    return " · ".join(parts) if parts else "-"


def build_hero_metrics(reports_dir: Path, crawl_limit: int = 40, crawl_sleep: float = 0.25) -> Dict[str, Any]:
    p = _pick_latest(str(reports_dir / "hero_main_banners_*.csv"))
    if not p:
        return {
            "brands": None,
            "banners": None,
            "missing_img": None,
            "keywords": [],
            "kw_text": "-",
            "updated": now_kst_label(),
        }

    df = _read_csv_any(p)
    banners = len(df)

    c_brand = _col(df, ["brand", "브랜드", "site", "사이트", "brand_name", "brand_key"])
    if c_brand:
        brands = int(df[c_brand].nunique())
    else:
        c_brand2 = _col(df, ["brand_key"])
        brands = int(df[c_brand2].nunique()) if c_brand2 else None

    c_img = _col(df, ["image_url", "img", "image", "이미지", "banner_image", "img_url"])
    missing_img = None
    if c_img:
        s = df[c_img].astype(str).str.strip().str.lower()
        missing_img = int((s.eq("") | s.eq("nan") | s.eq("none")).sum())

    # URL column
    c_url = _col(df, [
        "href_clean", "href",
        "landing_url", "landing url", "landing",
        "link", "url", "target_url", "target url", "page_url", "page url",
        "detail_url", "detail url", "event_url", "event url",
        "상품url", "기획전url", "기획전 url", "랜딩url", "랜딩 url"
    ])

    # Title/text column
    c_title = _col(df, [
        "title",
        "banner_title", "headline", "copy", "text",
        "기획전명", "기획전", "캠페인명",
    ])

    cache_path = reports_dir / "_cache_hero_pages.json"
    cache = _load_cache(cache_path)

    texts: List[str] = []

    # Primary: CSV titles
    if c_title:
        tcol = df[c_title].astype(str).fillna("").tolist()
        texts.extend([x for x in tcol if x and x.lower() not in ("nan", "none")])

    # Secondary: crawl landing pages (optional) — still allowed, but we do NOT surface crawl/cache wording in summary UI
    can_crawl = (requests is not None and BeautifulSoup is not None and c_url is not None)

    timeout = int(os.environ.get("HERO_CRAWL_TIMEOUT", "8"))
    retries = int(os.environ.get("HERO_CRAWL_RETRIES", "2"))

    used_urls: List[str] = []
    crawled = 0
    cache_hits = 0

    if can_crawl:
        urls = df[c_url].astype(str).map(_norm_url).tolist()
        uniq: List[str] = []
        seen = set()
        for u in urls:
            if not u or u in seen:
                continue
            seen.add(u)
            uniq.append(u)
        uniq = uniq[: max(0, int(crawl_limit))]

        for u in uniq:
            used_urls.append(u)

            if u in cache and isinstance(cache[u], dict) and cache[u].get("_ts"):
                sig = cache[u]
                cache_hits += 1
            else:
                html = _fetch_page(u, timeout=timeout, retries=retries)
                sig = _extract_page_signals(html or "")
                sig["_ts"] = now_kst_label()
                cache[u] = sig
                crawled += 1
                time.sleep(max(0.0, float(crawl_sleep)))

            texts.extend([
                sig.get("title", ""),
                sig.get("og_title", ""),
                sig.get("desc", ""),
                sig.get("h", ""),
                sig.get("snippet", ""),
            ])

        _save_cache(cache_path, cache)

    keywords = _build_keywords_from_texts(texts, top_n=10)
    kw_text = _format_keywords_with_counts(keywords, take=6)

    # Keep detailed keyword report file (optional), but do not show "상세보기" line on summary card
    _write_hero_keywords_html(
        reports_dir=reports_dir,
        src_csv=p.name,
        keywords=keywords,
        used_urls=used_urls,
        can_crawl=can_crawl,
        crawled=crawled,
        cache_hits=cache_hits,
        cache_path=cache_path.name,
    )

    return {
        "brands": brands,
        "banners": banners,
        "missing_img": missing_img,
        "keywords": keywords,
        "kw_text": kw_text,
        "updated": now_kst_label(),
    }


def _write_hero_keywords_html(
    reports_dir: Path,
    src_csv: str,
    keywords: List[Tuple[str, int]],
    used_urls: List[str],
    can_crawl: bool,
    crawled: int,
    cache_hits: int,
    cache_path: str,
) -> None:
    rows = ""
    for k, c in keywords:
        rows += f"<tr><td class='py-2 pr-4 font-extrabold'>{k}</td><td class='py-2 text-right'>{c:,}</td></tr>"

    url_list = ""
    for u in used_urls[:40]:
        url_list += (
            f"<li class='truncate'><a class='text-blue-700 font-bold' href='{u}' target='_blank' rel='noreferrer'>"
            f"{u}</a></li>"
        )

    meta = f"""
    <div class="text-sm text-slate-700">
      <div><b>source</b>: {src_csv}</div>
      <div><b>mode</b>: {"crawl + cache" if can_crawl else "title-only (no deps or url column)"}</div>
      <div><b>crawl</b>: {crawled:,} · <b>cache hit</b>: {cache_hits:,} · <b>cache file</b>: {cache_path}</div>
      <div class="mt-1 text-slate-500">updated: {now_kst_label()}</div>
    </div>
    """

    html = f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Hero Campaign Keywords</title>
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-slate-50 text-slate-900">
  <div class="max-w-5xl mx-auto px-4 py-8">
    <h1 class="text-3xl font-black">Hero Campaign Keywords</h1>
    <p class="mt-2 text-slate-600">Hero 배너(title) + 랜딩페이지(선택) 기반 키워드</p>

    <div class="mt-6 p-5 bg-white rounded-2xl shadow-sm border border-slate-200">
      {meta}
    </div>

    <div class="mt-6 grid grid-cols-1 lg:grid-cols-2 gap-6">
      <div class="p-5 bg-white rounded-2xl shadow-sm border border-slate-200">
        <h2 class="text-lg font-black">Top Keywords</h2>
        <table class="mt-3 w-full text-sm">
          <thead class="text-slate-500">
            <tr><th class="py-2 text-left">Keyword</th><th class="py-2 text-right">Count</th></tr>
          </thead>
          <tbody>{rows or "<tr><td class='py-2' colspan='2'>키워드가 없습니다 (데이터/컬럼 확인 필요)</td></tr>"}</tbody>
        </table>
      </div>

      <div class="p-5 bg-white rounded-2xl shadow-sm border border-slate-200">
        <h2 class="text-lg font-black">Crawled URLs (sample)</h2>
        <ul class="mt-3 space-y-2 text-sm">{url_list or "<li class='text-slate-500'>URL이 없거나 크롤링 비활성</li>"}</ul>
      </div>
    </div>
  </div>
</body>
</html>
"""
    try:
        (reports_dir / "hero_keywords.html").write_text(html, encoding="utf-8")
    except Exception:
        pass


# -----------------------
# VOC metrics (best-effort)
# -----------------------
def _parse_first_int(pattern: str, text: str) -> Optional[int]:
    m = re.search(pattern, text, flags=re.I | re.S)
    if not m:
        return None
    s = re.sub(r"[^\d]", "", m.group(1))
    return int(s) if s else None


def build_voc_metrics(reports_dir: Path) -> Dict[str, Any]:
    p = reports_dir / "external_signal.html"
    if not p.exists():
        return {"posts": None, "mentions": None, "range": "최근 7일", "updated": now_kst_label()}

    txt = p.read_text(encoding="utf-8", errors="ignore")

    mentions = (
        _parse_first_int(r"total[_\s-]*mentions[^0-9]*([0-9,]+)", txt)
        or _parse_first_int(r"총\s*언급[^0-9]*([0-9,]+)", txt)
    )
    posts = (
        _parse_first_int(r"\bposts?\b[^0-9]*([0-9,]+)", txt)
        or _parse_first_int(r"게시글[^0-9]*([0-9,]+)", txt)
    )

    return {"posts": posts, "mentions": mentions, "range": "최근 7일", "updated": now_kst_label()}


# -----------------------
# HTML render (embed-friendly)
# -----------------------
def render_index_html(naver: Dict[str, Any], hero: Dict[str, Any], voc: Dict[str, Any]) -> str:
    def stat(label: str, value: str) -> str:
        return f"""
          <div class="flex items-baseline justify-between gap-3">
            <div class="text-[11px] font-extrabold tracking-widest text-slate-500 uppercase">{label}</div>
            <div class="text-lg font-black text-slate-900">{value}</div>
          </div>
        """

    risk_label = naver.get("risk_label") or "-"
    risk_class = naver.get("risk_class") or "risk-unk"

    # Button (consistent placement)
    def open_btn(href: str) -> str:
        return f"""
          <a href="{href}" target="_self"
             class="mt-6 w-full inline-flex items-center justify-center rounded-2xl px-4 py-3
                    bg-[color:var(--brand)] text-white font-black text-sm
                    shadow-sm hover:opacity-95 active:opacity-90 transition">
            Open report
          </a>
        """

    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>CSK E-COMM | 오늘의 핵심 요약</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200;400;600;800&display=swap');
    :root {{ --brand:#002d72; }}
    body{{ background: transparent; font-family:'Plus Jakarta Sans',sans-serif; color:#0f172a; }}
    .glass-card{{ background: rgba(255,255,255,0.70); backdrop-filter: blur(14px); border: 1px solid rgba(15,23,42,0.06); box-shadow: 0 16px 50px rgba(15,23,42,0.08); }}
    .badge{{ font-size:11px; font-weight:900; padding:6px 10px; border-radius:999px; background: rgba(0,45,114,.08); color: var(--brand); }}
    .badge-soft{{ font-size:11px; font-weight:900; padding:6px 10px; border-radius:999px; background: rgba(15,23,42,.06); color: rgba(15,23,42,.70); }}
    .risk-pill{{ display:inline-flex; align-items:center; gap:8px; padding:6px 10px; border-radius:999px; font-weight:900; font-size:11px; }}
    .risk-dot{{ width:8px; height:8px; border-radius:999px; }}
    .risk-high{{ background: rgba(239,68,68,.10); color: rgba(239,68,68,1); }}
    .risk-high .risk-dot{{ background: rgb(239,68,68); box-shadow: 0 0 0 5px rgba(239,68,68,.12); }}
    .risk-mid{{ background: rgba(245,158,11,.12); color: rgba(180,83,9,1); }}
    .risk-mid .risk-dot{{ background: rgb(245,158,11); box-shadow: 0 0 0 5px rgba(245,158,11,.12); }}
    .risk-low{{ background: rgba(34,197,94,.12); color: rgba(22,163,74,1); }}
    .risk-low .risk-dot{{ background: rgb(34,197,94); box-shadow: 0 0 0 5px rgba(34,197,94,.12); }}
    .risk-unk{{ background: rgba(100,116,139,.12); color: rgba(51,65,85,1); }}
    .risk-unk .risk-dot{{ background: rgb(100,116,139); box-shadow: 0 0 0 5px rgba(100,116,139,.12); }}
    .subtle{{ margin-top:14px; padding:12px 14px; border-radius:18px; background: rgba(255,255,255,.55); border: 1px solid rgba(15,23,42,0.05); }}
  </style>
</head>
<body>
  <div class="px-2 sm:px-6 py-6">
    <div class="mb-6">
      <div class="text-3xl sm:text-4xl font-black tracking-tight">오늘의 핵심 요약</div>
    </div>

    <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
      <!-- NAVER -->
      <div class="glass-card rounded-3xl p-6 flex flex-col">
        <div class="flex items-center justify-between">
          <div class="text-lg font-black">네이버 최저가</div>
          <div class="flex items-center gap-2">
            <span class="risk-pill {risk_class}"><span class="risk-dot"></span>RISK {risk_label}</span>
          </div>
        </div>

        <div class="mt-5 space-y-3">
          {stat("TOTAL", fmt_int(naver.get("total")))}
          {stat("DIFF > 0", f'{fmt_int(naver.get("diff_pos"))}  ·  {fmt_pct(naver.get("diff_pos_ratio"))}')}
          {stat("AVG GAP", fmt_won(naver.get("avg_gap")))}
        </div>

        <div class="mt-4 text-xs text-slate-500">updated: {naver.get("updated") or ""}</div>

        <div class="mt-auto">
          {open_btn("./naver_lowest_price.html")}
        </div>
      </div>

      <!-- HERO -->
      <div class="glass-card rounded-3xl p-6 flex flex-col">
        <div class="flex items-center justify-between">
          <div class="text-lg font-black">경쟁사 Hero 배너</div>
          <span class="badge">Snapshot</span>
        </div>

        <div class="mt-5 space-y-3">
          {stat("BRANDS", fmt_int(hero.get("brands")))}
          {stat("BANNERS", fmt_int(hero.get("banners")))}
          {stat("MISSING IMG", fmt_int(hero.get("missing_img")))}
        </div>

        <div class="subtle">
          <div class="text-[11px] font-extrabold tracking-widest text-slate-500 uppercase">주요 기획전 키워드</div>
          <div class="mt-2 text-sm font-extrabold text-slate-900">{hero.get("kw_text") or "-"}</div>
        </div>

        <div class="mt-4 text-xs text-slate-500">updated: {hero.get("updated") or ""}</div>

        <div class="mt-auto">
          {open_btn("./hero_main.html")}
        </div>
      </div>

      <!-- VOC -->
      <div class="glass-card rounded-3xl p-6 flex flex-col">
        <div class="flex items-center justify-between">
          <div class="text-lg font-black">커뮤니티 VOC</div>
          <span class="badge-soft">{voc.get("range") or "최근 7일"}</span>
        </div>

        <div class="mt-5 space-y-3">
          {stat("POSTS", fmt_int(voc.get("posts")))}
          {stat("MENTIONS", fmt_int(voc.get("mentions")))}
          {stat("—", "—")}
        </div>

        <div class="mt-4 text-xs text-slate-500">updated: {voc.get("updated") or ""}</div>

        <div class="mt-auto">
          {open_btn("./external_signal.html")}
        </div>
      </div>
    </div>
  </div>
</body>
</html>
"""


def main() -> None:
    repo_root = Path(".").resolve()
    reports_dir = repo_root / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    naver = build_naver_metrics(repo_root)

    crawl_limit = int(os.environ.get("HERO_CRAWL_LIMIT", "40"))
    crawl_sleep = float(os.environ.get("HERO_CRAWL_SLEEP", "0.25"))

    hero = build_hero_metrics(reports_dir, crawl_limit=crawl_limit, crawl_sleep=crawl_sleep)
    voc = build_voc_metrics(reports_dir)

    out = reports_dir / "index.html"
    out.write_text(render_index_html(naver, hero, voc), encoding="utf-8")

    print(f"[OK] Wrote: {out}")
    print(f"[OK] Wrote: {reports_dir / 'hero_keywords.html'}")
    print(f"[OK] Cache: {reports_dir / '_cache_hero_pages.json'}")


if __name__ == "__main__":
    main()
