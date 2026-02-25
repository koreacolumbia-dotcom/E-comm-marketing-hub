#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_summary_ADV_FINAL_UPGRADED.py  (UI PATCH + KPI TOP STRIP + CREMA CARD)

✅ Requested (THIS PATCH):
- Summary 상단 KPI strip에 DoD + YoY 모두 표시
- Weekly KPI strip (7D) 추가 (WoW + YoY 표시)

Input JSON (best-effort; 없으면 '-'로 표시):
- reports/daily_kpi.json   (daily + dod + yoy)
- reports/weekly_kpi.json  (weekly + wow + yoy)

Existing:
- Generates reports/index.html (embed-friendly)
- Hero keywords from hero_main_banners_*.csv (+ optional crawl)
- Crema VOC summary card (reports/voc_crema/index.html)
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


def fmt_krw_symbol(x: Any) -> str:
    """₩ with commas (for KPI strip)."""
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return "-"
        return f"₩{int(round(float(x))):,}"
    except Exception:
        return "-"


def fmt_cvr(x: Any) -> str:
    """x is fraction (0.0762) -> 7.62%"""
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return "-"
        return f"{float(x) * 100:.2f}%"
    except Exception:
        return "-"


def fmt_delta_ratio(x: Any, digits: int = 1) -> str:
    """
    x is ratio (e.g. -0.192) -> -19.2%
    """
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return "-"
        v = float(x)
        sign = "+" if v >= 0 else ""
        return f"{sign}{v * 100:.{digits}f}%"
    except Exception:
        return "-"


def fmt_pp_from_fraction(x: Any) -> str:
    """
    x is fraction diff for CVR (cur - prev), e.g. -0.00032 -> -0.03%p
    """
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return "-"
        v = float(x)
        sign = "+" if v >= 0 else ""
        return f"{sign}{v * 100:.2f}%p"
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


def _norm_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    if u.lower() in ("nan", "none", "null"):
        return ""
    return u


def _read_json_path(p: Path) -> Optional[Dict[str, Any]]:
    try:
        if p.exists() and p.is_file():
            return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None
    return None


# -----------------------
# ✅ KPI loaders
# -----------------------
def build_daily_kpis(reports_dir: Path) -> Dict[str, Any]:
    """
    reports/daily_kpi.json (recommended)
    expected keys:
      date, sessions, orders, revenue, cvr, signups
      dod: {sessions, orders, revenue, cvr_pp, signups}
      yoy: {sessions, orders, revenue, cvr_pp, signups}
      updated
    """
    p = reports_dir / "daily_kpi.json"
    j = _read_json_path(p)

    if not j:
        return {
            "date": None,
            "sessions": None,
            "orders": None,
            "revenue": None,
            "cvr": None,
            "signups": None,
            "dod": {},
            "yoy": {},
            "updated": now_kst_label(),
            "source": None,
        }

    return {
        "date": j.get("date"),
        "sessions": j.get("sessions"),
        "orders": j.get("orders"),
        "revenue": j.get("revenue"),
        "cvr": j.get("cvr"),
        "signups": j.get("signups"),
        "dod": j.get("dod") or {},
        "yoy": j.get("yoy") or {},
        "updated": j.get("updated") or now_kst_label(),
        "source": str(p),
    }


def build_weekly_kpis(reports_dir: Path) -> Dict[str, Any]:
    """
    reports/weekly_kpi.json (new)
    expected keys:
      start, end, sessions, orders, revenue, cvr, signups
      wow: {sessions, orders, revenue, cvr_pp, signups}
      yoy: {sessions, orders, revenue, cvr_pp, signups}
      updated
    """
    p = reports_dir / "weekly_kpi.json"
    j = _read_json_path(p)

    if not j:
        return {
            "start": None,
            "end": None,
            "sessions": None,
            "orders": None,
            "revenue": None,
            "cvr": None,
            "signups": None,
            "wow": {},
            "yoy": {},
            "updated": now_kst_label(),
            "source": None,
        }

    return {
        "start": j.get("start"),
        "end": j.get("end"),
        "sessions": j.get("sessions"),
        "orders": j.get("orders"),
        "revenue": j.get("revenue"),
        "cvr": j.get("cvr"),
        "signups": j.get("signups"),
        "wow": j.get("wow") or {},
        "yoy": j.get("yoy") or {},
        "updated": j.get("updated") or now_kst_label(),
        "source": str(p),
    }


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
    "전체보기",
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
        vc = vc[~vc.index.str.contains(r"(?:^전체보기$|^전체\s+보기$)", case=False, regex=True)]
    except Exception:
        pass

    try:
        vc = vc[~vc.index.str.contains(r"\b(?:http|https|www|img|banner)\b", case=False, regex=True)]
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

    c_url = _col(df, [
        "href_clean", "href",
        "landing_url", "landing url", "landing",
        "link", "url", "target_url", "target url", "page_url", "page url",
        "detail_url", "detail url", "event_url", "event url",
        "상품url", "기획전url", "기획전 url", "랜딩url", "랜딩 url"
    ])

    c_title = _col(df, [
        "title",
        "banner_title", "headline", "copy", "text",
        "기획전명", "기획전", "캠페인명",
    ])

    cache_path = reports_dir / "_cache_hero_pages.json"
    cache = _load_cache(cache_path)

    texts: List[str] = []

    if c_title:
        tcol = df[c_title].astype(str).fillna("").tolist()
        texts.extend([x for x in tcol if x and x.lower() not in ("nan", "none")])

    can_crawl = (requests is not None and BeautifulSoup is not None and c_url is not None)
    timeout = int(os.environ.get("HERO_CRAWL_TIMEOUT", "8"))
    retries = int(os.environ.get("HERO_CRAWL_RETRIES", "2"))

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
            if u in cache and isinstance(cache[u], dict) and cache[u].get("_ts"):
                sig = cache[u]
            else:
                html = _fetch_page(u, timeout=timeout, retries=retries)
                sig = _extract_page_signals(html or "")
                sig["_ts"] = now_kst_label()
                cache[u] = sig
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

    return {
        "brands": brands,
        "banners": banners,
        "missing_img": missing_img,
        "keywords": keywords,
        "kw_text": kw_text,
        "updated": now_kst_label(),
    }



# -----------------------
# reports/summary.json (structured metrics) loader
# -----------------------
def _read_summary_json(reports_dir: Path) -> Dict[str, Any]:
    p = reports_dir / "summary.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}

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
    # 1) Prefer structured metrics if available (written by external_signal_combined.py)
    sj = _read_summary_json(reports_dir)
    ext = sj.get("external_signal") or {}
    if isinstance(ext, dict) and ext:
        posts = ext.get("posts_collected")
        mentions = ext.get("total_mentions")
        target_days = ext.get("target_days") or 7
        updated = ext.get("updated_at") or now_kst_label()
        # normalize ints
        def _to_int(x):
            try:
                return int(x)
            except Exception:
                return None
        return {
            "posts": _to_int(posts),
            "mentions": _to_int(mentions),
            "range": f"최근 {int(target_days)}일",
            "updated": updated,
        }

    # 2) Fallback: parse from external_signal.html (legacy / best-effort)
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
# Crema metrics (best-effort)
# ----------------------- (best-effort)
# -----------------------
def _read_json_if_exists(p: Path) -> Optional[Dict[str, Any]]:
    try:
        if p.exists() and p.is_file():
            return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None
    return None


def build_crema_metrics(reports_dir: Path) -> Dict[str, Any]:
    base = reports_dir / "voc_crema"

    candidates = [
        base / "site" / "data" / "meta.json",
        base / "meta.json",
        base / "site" / "data" / "reviews.json",
    ]

    meta = None
    used = None
    for c in candidates:
        meta = _read_json_if_exists(c)
        if meta is not None:
            used = c
            break

    out = {
        "total_reviews": None,
        "date_range": None,
        "complaint_top5": None,
        "updated": now_kst_label(),
        "source": str(used) if used else None,
    }

    if not meta:
        return out

    def pick(*keys):
        for k in keys:
            if k in meta and meta[k] is not None:
                return meta[k]
        return None

    total = pick("total_reviews", "total", "reviews_total", "review_total", "count", "n_reviews")
    if isinstance(total, str):
        try:
            total = int(re.sub(r"[^\d]", "", total) or "0")
        except Exception:
            total = None
    if isinstance(total, (int, float)):
        out["total_reviews"] = int(total)

    dr = pick("date_range", "range", "period", "dateRange", "date_range_text")
    if isinstance(dr, str) and dr.strip():
        out["date_range"] = dr.strip()
    else:
        start = pick("date_start", "start_date", "from")
        end = pick("date_end", "end_date", "to")
        if start and end:
            out["date_range"] = f"{start} ~ {end}"

    top5 = pick("complaint_top5", "complaintTop5", "top5", "complaints_top5")
    if top5 is not None:
        out["complaint_top5"] = top5

    return out


def _fmt_top5(x: Any) -> str:
    if not x:
        return "-"
    try:
        if isinstance(x, list):
            parts = []
            for item in x[:5]:
                if isinstance(item, (list, tuple)) and len(item) >= 2:
                    k = str(item[0]).strip()
                    c = int(item[1])
                    if k:
                        parts.append(f"{k}({c:,})")
                elif isinstance(item, dict):
                    k = str(item.get("keyword") or item.get("k") or "").strip()
                    c = item.get("count") or item.get("c")
                    try:
                        c = int(c)
                    except Exception:
                        c = None
                    if k and c is not None:
                        parts.append(f"{k}({c:,})")
            return " · ".join(parts) if parts else "-"
        s = str(x)
        s = re.sub(r"[\[\]\(\)']", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s[:140] + ("…" if len(s) > 140 else "")
    except Exception:
        return "-"


# -----------------------
# HTML render (embed-friendly)
# -----------------------
def render_index_html(
    daily: Dict[str, Any],
    weekly: Dict[str, Any],
    naver: Dict[str, Any],
    hero: Dict[str, Any],
    voc: Dict[str, Any],
    crema: Dict[str, Any]
) -> str:
    def stat(label: str, value: str) -> str:
        return f"""
          <div class="flex items-baseline justify-between gap-3">
            <div class="text-[11px] font-extrabold tracking-widest text-slate-500 uppercase">{label}</div>
            <div class="text-lg font-black text-slate-900">{value}</div>
          </div>
        """

    risk_label = naver.get("risk_label") or "-"
    risk_class = naver.get("risk_class") or "risk-unk"

    def open_btn(href: str) -> str:
        return f"""
          <a href="{href}" target="_self"
             class="mt-6 w-full inline-flex items-center justify-center rounded-2xl px-4 py-3
                    bg-[color:var(--brand)] text-white font-black text-sm
                    shadow-sm hover:opacity-95 active:opacity-90 transition">
            Open report
          </a>
        """

    crema_total = fmt_int(crema.get("total_reviews"))
    crema_range = crema.get("date_range") or "-"
    crema_top5 = _fmt_top5(crema.get("complaint_top5"))

    # ----------------
    # KPI strips
    # ----------------
    def kpi_tile(title: str, value: str, line1: str, line2: str) -> str:
        return f"""
        <div class="rounded-2xl border border-slate-200 bg-white/70 p-4">
          <div class="text-[11px] font-extrabold tracking-widest text-slate-500 uppercase">{title}</div>
          <div class="mt-1 text-xl font-black text-slate-900">{value}</div>
          <div class="mt-1 text-[11px] text-slate-500">{line1}</div>
          <div class="mt-1 text-[11px] text-slate-500">{line2}</div>
        </div>
        """

    # Daily
    d_dod = daily.get("dod") or {}
    d_yoy = daily.get("yoy") or {}
    d_date = daily.get("date") or "-"
    d_updated = daily.get("updated") or ""

    daily_strip = f"""
    <div class="glass-card rounded-3xl p-5 mb-6">
      <div class="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-2">
        <div class="text-base font-black text-slate-900">Daily KPI Summary</div>
        <div class="text-xs text-slate-500">
          기준: <b class="text-slate-700">{d_date}</b> · updated: {d_updated}
        </div>
      </div>

      <div class="mt-4 grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3">
        {kpi_tile("Sessions", fmt_int(daily.get("sessions")),
                  f'DoD <b class="text-slate-900">{fmt_delta_ratio(d_dod.get("sessions"))}</b>',
                  f'YoY <b class="text-slate-900">{fmt_delta_ratio(d_yoy.get("sessions"))}</b>')}
        {kpi_tile("Orders", fmt_int(daily.get("orders")),
                  f'DoD <b class="text-slate-900">{fmt_delta_ratio(d_dod.get("orders"))}</b>',
                  f'YoY <b class="text-slate-900">{fmt_delta_ratio(d_yoy.get("orders"))}</b>')}
        {kpi_tile("Revenue", fmt_krw_symbol(daily.get("revenue")),
                  f'DoD <b class="text-slate-900">{fmt_delta_ratio(d_dod.get("revenue"))}</b>',
                  f'YoY <b class="text-slate-900">{fmt_delta_ratio(d_yoy.get("revenue"))}</b>')}
        {kpi_tile("CVR", fmt_cvr(daily.get("cvr")),
                  f'DoD <b class="text-slate-900">{fmt_pp_from_fraction(d_dod.get("cvr_pp"))}</b>',
                  f'YoY <b class="text-slate-900">{fmt_pp_from_fraction(d_yoy.get("cvr_pp"))}</b>')}
        {kpi_tile("Sign-up Users", fmt_int(daily.get("signups")),
                  f'DoD <b class="text-slate-900">{fmt_delta_ratio(d_dod.get("signups"))}</b>',
                  f'YoY <b class="text-slate-900">{fmt_delta_ratio(d_yoy.get("signups"))}</b>')}
      </div>
    </div>
    """

    # Weekly
    w_wow = weekly.get("wow") or {}
    w_yoy = weekly.get("yoy") or {}
    w_start = weekly.get("start") or "-"
    w_end = weekly.get("end") or "-"
    w_updated = weekly.get("updated") or ""

    weekly_strip = f"""
    <div class="glass-card rounded-3xl p-5 mb-6">
      <div class="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-2">
        <div class="text-base font-black text-slate-900">Weekly KPI Summary (7D)</div>
        <div class="text-xs text-slate-500">
          기간: <b class="text-slate-700">{w_start} ~ {w_end}</b> · updated: {w_updated}
        </div>
      </div>

      <div class="mt-4 grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3">
        {kpi_tile("Sessions", fmt_int(weekly.get("sessions")),
                  f'WoW <b class="text-slate-900">{fmt_delta_ratio(w_wow.get("sessions"))}</b>',
                  f'YoY <b class="text-slate-900">{fmt_delta_ratio(w_yoy.get("sessions"))}</b>')}
        {kpi_tile("Orders", fmt_int(weekly.get("orders")),
                  f'WoW <b class="text-slate-900">{fmt_delta_ratio(w_wow.get("orders"))}</b>',
                  f'YoY <b class="text-slate-900">{fmt_delta_ratio(w_yoy.get("orders"))}</b>')}
        {kpi_tile("Revenue", fmt_krw_symbol(weekly.get("revenue")),
                  f'WoW <b class="text-slate-900">{fmt_delta_ratio(w_wow.get("revenue"))}</b>',
                  f'YoY <b class="text-slate-900">{fmt_delta_ratio(w_yoy.get("revenue"))}</b>')}
        {kpi_tile("CVR", fmt_cvr(weekly.get("cvr")),
                  f'WoW <b class="text-slate-900">{fmt_pp_from_fraction(w_wow.get("cvr_pp"))}</b>',
                  f'YoY <b class="text-slate-900">{fmt_pp_from_fraction(w_yoy.get("cvr_pp"))}</b>')}
        {kpi_tile("Sign-up Users", fmt_int(weekly.get("signups")),
                  f'WoW <b class="text-slate-900">{fmt_delta_ratio(w_wow.get("signups"))}</b>',
                  f'YoY <b class="text-slate-900">{fmt_delta_ratio(w_yoy.get("signups"))}</b>')}
      </div>
    </div>
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

    <!-- ✅ KPI strips -->
    {daily_strip}
    {weekly_strip}

    <!-- ✅ 4 cards -->
    <div class="grid grid-cols-1 lg:grid-cols-4 gap-6">
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

      <!-- CREMA -->
      <div class="glass-card rounded-3xl p-6 flex flex-col">
        <div class="flex items-center justify-between">
          <div class="text-lg font-black">Crema VOC</div>
          <span class="badge-soft">Reviews</span>
        </div>

        <div class="mt-5 space-y-3">
          {stat("TOTAL REVIEWS", crema_total)}
          {stat("DATE RANGE", crema_range)}
          {stat("TOP5", crema_top5)}
        </div>

        <div class="mt-4 text-xs text-slate-500">updated: {crema.get("updated") or ""}</div>

        <div class="mt-auto">
          {open_btn("./voc_crema/index.html")}
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

    daily = build_daily_kpis(reports_dir)
    weekly = build_weekly_kpis(reports_dir)

    naver = build_naver_metrics(repo_root)

    crawl_limit = int(os.environ.get("HERO_CRAWL_LIMIT", "40"))
    crawl_sleep = float(os.environ.get("HERO_CRAWL_SLEEP", "0.25"))
    hero = build_hero_metrics(reports_dir, crawl_limit=crawl_limit, crawl_sleep=crawl_sleep)

    voc = build_voc_metrics(reports_dir)
    crema = build_crema_metrics(reports_dir)

    out = reports_dir / "index.html"
    out.write_text(render_index_html(daily, weekly, naver, hero, voc, crema), encoding="utf-8")

    print(f"[OK] Wrote: {out}")
    if daily.get("source"):
        print(f"[OK] Daily KPI source: {daily.get('source')}")
    else:
        print("[WARN] reports/daily_kpi.json not found. Daily KPI strip will show '-' until generated.")
    if weekly.get("source"):
        print(f"[OK] Weekly KPI source: {weekly.get('source')}")
    else:
        print("[WARN] reports/weekly_kpi.json not found. Weekly KPI strip will show '-' until generated.")
    if crema.get("source"):
        print(f"[OK] Crema meta source: {crema.get('source')}")


if __name__ == "__main__":
    main()
