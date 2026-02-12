#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_summary.py
- Generates reports/index.html (Hub default landing = Summary)
- Keeps visual style consistent with existing report UIs (Tailwind + glass cards + left sidebar)
- Metrics:
  - Naver lowest price: latest result_*.csv (repo root)
  - Hero banners: latest reports/hero_main_banners_*.csv
  - VOC: best-effort parse from reports/external_signal.html (fallback '-')
"""

from __future__ import annotations

import os
import re
import glob
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional

import pandas as pd

KST = timezone(timedelta(hours=9))


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


def _col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
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


def fmt_won(x: Any) -> str:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return "-"
        return f"{int(x):,}원"
    except Exception:
        return "-"


def build_naver_metrics(repo_root: Path) -> Dict[str, Any]:
    p = _pick_latest(str(repo_root / "result_*.csv"))
    if not p:
        return {"total": None, "diff_pos": None, "avg_gap": None, "period": f"Snapshot (KST) · {now_kst_label()}"}

    df = _read_csv_any(p)
    total = len(df)

    c_gap = _col(df, ["가격차이", "gap", "diff"])
    diff_pos = None
    avg_gap = None
    if c_gap:
        gap_s = pd.to_numeric(df[c_gap], errors="coerce")
        diff_pos = int((gap_s > 0).sum())
        avg_gap = int(gap_s.dropna().mean()) if gap_s.dropna().size else None

    return {
        "total": total,
        "diff_pos": diff_pos,
        "avg_gap": avg_gap,
        "period": f"Snapshot (KST) · Δ vs previous run · {now_kst_label()}",
    }


def build_hero_metrics(reports_dir: Path) -> Dict[str, Any]:
    p = _pick_latest(str(reports_dir / "hero_main_banners_*.csv"))
    if not p:
        return {"brands": None, "banners": None, "missing_img": None, "period": f"Snapshot (KST) · {now_kst_label()}"}

    df = _read_csv_any(p)
    banners = len(df)

    c_brand = _col(df, ["brand", "브랜드", "site", "사이트"])
    brands = int(df[c_brand].nunique()) if c_brand else None

    c_img = _col(df, ["image_url", "img", "image", "이미지", "banner_image"])
    missing_img = None
    if c_img:
        s = df[c_img].astype(str).str.strip().str.lower()
        missing_img = int((s.eq("") | s.eq("nan") | s.eq("none")).sum())

    return {
        "brands": brands,
        "banners": banners,
        "missing_img": missing_img,
        "period": f"Snapshot (KST) · {now_kst_label()}",
    }


def _parse_first_int(pattern: str, text: str) -> Optional[int]:
    m = re.search(pattern, text, flags=re.I | re.S)
    if not m:
        return None
    s = re.sub(r"[^\d]", "", m.group(1))
    return int(s) if s else None


def build_voc_metrics(reports_dir: Path) -> Dict[str, Any]:
    p = reports_dir / "external_signal.html"
    if not p.exists():
        return {"posts": None, "mentions": None, "period": "Last 7D (KST)"}

    txt = p.read_text(encoding="utf-8", errors="ignore")
    mentions = (
        _parse_first_int(r"total[_\s-]*mentions[^0-9]*([0-9,]+)", txt)
        or _parse_first_int(r"총\s*언급[^0-9]*([0-9,]+)", txt)
    )
    posts = (
        _parse_first_int(r"posts?[^0-9]*([0-9,]+)", txt)
        or _parse_first_int(r"게시글[^0-9]*([0-9,]+)", txt)
    )
    return {"posts": posts, "mentions": mentions, "period": "Last 7D (KST)"}


def render_index_html(updated: str, naver: Dict[str, Any], hero: Dict[str, Any], voc: Dict[str, Any]) -> str:
    def stat(label: str, value: str) -> str:
        return f"""
          <div class="flex items-baseline justify-between gap-3">
            <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">{label}</div>
            <div class="text-lg font-black text-slate-900">{value}</div>
          </div>
        """

    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>CSK E-COMM | Marketing Intelligent Hub</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200;400;600;800&display=swap');
    :root {{ --brand:#002d72; --bg0:#f6f8fb; --bg1:#eef3f9; }}
    body{{ background: linear-gradient(180deg, var(--bg0), var(--bg1)); font-family:'Plus Jakarta Sans',sans-serif; color:#0f172a; min-height:100vh; }}
    .glass-card{{ background: rgba(255,255,255,0.60); backdrop-filter: blur(14px); border: 1px solid rgba(15,23,42,0.06); box-shadow: 0 16px 50px rgba(15,23,42,0.08); }}
    .nav-item{{ display:flex; align-items:center; gap:10px; padding:12px 14px; border-radius:16px; font-weight:800; color:rgba(15,23,42,.65); }}
    .nav-item:hover{{ background: rgba(255,255,255,.70); }}
    .nav-active{{ background: rgba(255,255,255,.92); color:#0f172a; box-shadow: 0 10px 30px rgba(15,23,42,.08); }}
    .badge{{ font-size:11px; font-weight:900; padding:6px 10px; border-radius:999px; background: rgba(0,45,114,.08); color: var(--brand); }}
    .card-link{{ transition: transform .15s ease, box-shadow .15s ease; }}
    .card-link:hover{{ transform: translateY(-2px); }}
  </style>
</head>
<body>
  <div class="flex min-h-screen">
    <aside class="sidebar w-[260px] px-6 py-8">
      <div class="font-black text-[color:var(--brand)] text-xl">CSK <span class="text-slate-900">E-COMM</span></div>
      <div class="mt-2 text-[11px] font-extrabold tracking-widest text-slate-400">AUTOMATION<br/>MARKETING HUB</div>

      <div class="mt-8 space-y-2">
        <a class="nav-item nav-active" href="./index.html"><i class="fa-solid fa-chart-simple w-5 text-[color:var(--brand)]"></i> Summary</a>
        <a class="nav-item" href="./naver_lowest_price.html"><i class="fa-solid fa-won-sign w-5 text-slate-500"></i> 네이버 최저가</a>
        <a class="nav-item" href="./hero_main.html"><i class="fa-regular fa-image w-5 text-slate-500"></i> 경쟁사 Hero 배너</a>
        <a class="nav-item" href="./external_signal.html"><i class="fa-solid fa-wave-square w-5 text-slate-500"></i> 커뮤니티 VOC</a>
      </div>
    </aside>

    <main class="flex-1 px-10 py-10">
      <div class="max-w-6xl">
        <div class="flex items-end justify-between gap-6">
          <div>
            <h1 class="text-5xl font-black tracking-tight">Marketing Intelligent Hub</h1>
            <div class="text-slate-600 mt-2">자동 업데이트 리포트 포털 (GitHub Actions → reports/)</div>
          </div>
          <div class="glass-card rounded-3xl px-5 py-4">
            <div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Last updated</div>
            <div class="text-sm font-black text-slate-800 mt-1">{updated}</div>
          </div>
        </div>

        <div class="grid grid-cols-1 lg:grid-cols-3 gap-6 mt-10">
          <a class="glass-card card-link rounded-3xl p-6 block" href="./naver_lowest_price.html">
            <div class="flex items-center justify-between">
              <div class="text-lg font-black">네이버 최저가</div>
              <span class="badge">Snapshot</span>
            </div>
            <div class="mt-5 space-y-3">
              {stat("TOTAL", fmt_int(naver.get("total")))}
              {stat("DIFF > 0", fmt_int(naver.get("diff_pos")))}
              {stat("AVG GAP", fmt_won(naver.get("avg_gap")))}
            </div>
            <div class="mt-5 text-xs text-slate-500">{naver.get("period") or ""}</div>
            <div class="mt-4 text-sm font-black text-[color:var(--brand)]">Open report →</div>
          </a>

          <a class="glass-card card-link rounded-3xl p-6 block" href="./hero_main.html">
            <div class="flex items-center justify-between">
              <div class="text-lg font-black">경쟁사 Hero 배너</div>
              <span class="badge">Snapshot</span>
            </div>
            <div class="mt-5 space-y-3">
              {stat("BRANDS", fmt_int(hero.get("brands")))}
              {stat("BANNERS", fmt_int(hero.get("banners")))}
              {stat("MISSING IMG", fmt_int(hero.get("missing_img")))}
            </div>
            <div class="mt-5 text-xs text-slate-500">{hero.get("period") or ""}</div>
            <div class="mt-4 text-sm font-black text-[color:var(--brand)]">Open report →</div>
          </a>

          <a class="glass-card card-link rounded-3xl p-6 block" href="./external_signal.html">
            <div class="flex items-center justify-between">
              <div class="text-lg font-black">커뮤니티 VOC</div>
              <span class="badge">Last 7D</span>
            </div>
            <div class="mt-5 space-y-3">
              {stat("POSTS", fmt_int(voc.get("posts")))}
              {stat("MENTIONS", fmt_int(voc.get("mentions")))}
              {stat("—", "—")}
            </div>
            <div class="mt-5 text-xs text-slate-500">{voc.get("period") or ""}</div>
            <div class="mt-4 text-sm font-black text-[color:var(--brand)]">Open report →</div>
          </a>
        </div>
      </div>
    </main>
  </div>
</body>
</html>
"""


def main() -> None:
    repo_root = Path(".").resolve()
    reports_dir = repo_root / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    updated = os.environ.get("UPDATED_KST") or now_kst_label()

    naver = build_naver_metrics(repo_root)
    hero = build_hero_metrics(reports_dir)
    voc = build_voc_metrics(reports_dir)

    out = reports_dir / "index.html"
    out.write_text(render_index_html(updated, naver, hero, voc), encoding="utf-8")
    print(f"[OK] Wrote: {out}")


if __name__ == "__main__":
    main()
