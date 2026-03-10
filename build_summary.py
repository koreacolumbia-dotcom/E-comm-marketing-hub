#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_summary_ADV_FINAL_UPGRADED.py  (UI PATCH + KPI TOP STRIP + CREMA CARD)
✅ PATCH (KPI auto-refresh, no extra GA4/BQ cost):
- If reports/daily_kpi.json / reports/weekly_kpi.json is missing or stale,
  auto-build them from latest cached DailyDigest bundle JSONs:
    - reports/daily_digest/data/daily/YYYY-MM-DD.json
    - reports/daily_digest/data/weekly/END_YYYY-MM-DD.json
- Computes WoW/YoY from adjacent cached bundles when available.

✅ NEW (Requested):
1) 기준일은 항상 "어제(KST)" 기준으로 최신(=어제) 우선 선택
2) ✅ OWNED YTD YoY 비교 추가
   - EDM+LMS+KAKAO 3채널 합산 YTD YoY
   - 채널별(EDM/LMS/KAKAO) YTD YoY
   - 데이터 소스: 첨부한 OWNED 번들 JSON (owned_YYYY-MM-DD.json)
   - 추가 GA4/BQ 비용 없음 (캐시 파일 합산만)
3) ✅ Daily KPI Summary: DoD → WoW
4) ✅ Remove Naver lowest price / Hero / Community VOC / Crema VOC cards
5) ✅ OWNED YTD YoY: Send Count 추가 (campaign row count 기준)
"""

from __future__ import annotations

import glob
import json
import os
import re
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta, date
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
def now_kst() -> datetime:
    return datetime.now(KST)


def kst_today() -> date:
    return now_kst().date()


def kst_yesterday() -> date:
    return kst_today() - timedelta(days=1)


def now_kst_label() -> str:
    return now_kst().strftime("%Y.%m.%d (%a) %H:%M KST")


def parse_ymd(s: str) -> Optional[date]:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def ymd(d: date) -> str:
    return d.strftime("%Y-%m-%d")


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
    """x is fraction (0.0071) -> 0.71%"""
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


def _write_json_path(p: Path, obj: Dict[str, Any]) -> None:
    try:
        p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


# -----------------------
# ✅ KPI auto-build from cached bundles (NO GA4/BQ)
# -----------------------
def _find_daily_bundle_for_date(reports_dir: Path, ymd_str: str) -> Optional[Path]:
    p = reports_dir / "daily_digest" / "data" / "daily" / f"{ymd_str}.json"
    return p if p.exists() and p.is_file() else None


def _find_weekly_bundle_for_end(reports_dir: Path, end_ymd: str) -> Optional[Path]:
    p = reports_dir / "daily_digest" / "data" / "weekly" / f"END_{end_ymd}.json"
    return p if p.exists() and p.is_file() else None


def _find_latest_daily_bundle(reports_dir: Path) -> Optional[Path]:
    files = list((reports_dir / "daily_digest" / "data" / "daily").glob("*.json"))
    files = [p for p in files if re.match(r"^\d{4}-\d{2}-\d{2}\.json$", p.name)]
    if not files:
        return None
    files.sort(key=lambda p: p.name, reverse=True)
    return files[0]


def _find_latest_weekly_bundle(reports_dir: Path) -> Optional[Path]:
    files = list((reports_dir / "daily_digest" / "data" / "weekly").glob("END_*.json"))
    files = [p for p in files if re.match(r"^END_\d{4}-\d{2}-\d{2}\.json$", p.name)]
    if not files:
        return None
    files.sort(key=lambda p: p.name, reverse=True)
    return files[0]


def _bundle_date_from_path(p: Optional[Path]) -> Optional[str]:
    if not p:
        return None
    m = re.search(r"(\d{4}-\d{2}-\d{2})", p.name)
    return m.group(1) if m else None


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        if isinstance(v, str):
            v = v.replace(",", "").strip()
            if v == "":
                return None
        return float(v)
    except Exception:
        return None


def _safe_int(v: Any) -> Optional[int]:
    f = _safe_float(v)
    return int(round(f)) if f is not None else None


def _extract_kpis_from_bundle(bundle: Dict[str, Any]) -> Dict[str, Optional[float]]:
    """
    Tolerant extraction from Daily Digest bundle JSON.
    Tries multiple likely locations/names to avoid breaking when upstream changes slightly.
    """

    def g(*path, default=None):
        cur = bundle
        for key in path:
            if not isinstance(cur, dict):
                return default
            cur = cur.get(key)
            if cur is None:
                return default
        return cur

    sessions = (
        g("kpis", "sessions")
        or g("overview", "sessions")
        or g("summary", "sessions")
        or g("traffic", "sessions")
        or g("overall", "current", "sessions")
    )
    orders = (
        g("kpis", "orders")
        or g("overview", "orders")
        or g("summary", "orders")
        or g("commerce", "orders")
        or g("overall", "current", "transactions")
    )
    revenue = (
        g("kpis", "revenue")
        or g("overview", "revenue")
        or g("summary", "revenue")
        or g("commerce", "revenue")
        or g("overall", "current", "purchaseRevenue")
    )
    signups = (
        g("kpis", "signups")
        or g("overview", "signups")
        or g("summary", "signups")
        or g("signup_users", "current")
    )
    cvr = (
        g("kpis", "cvr")
        or g("overview", "cvr")
        or g("summary", "cvr")
        or g("overall", "current", "cvr")
    )

    out = {
        "sessions": _safe_int(sessions),
        "orders": _safe_int(orders),
        "revenue": _safe_float(revenue),
        "signups": _safe_int(signups),
        "cvr": _safe_float(cvr),
    }

    # fallback: try "overall.current" if kpis missing
    if out["sessions"] is None:
        out["sessions"] = _safe_int(g("overall", "current", "sessions"))
    if out["orders"] is None:
        out["orders"] = _safe_int(g("overall", "current", "transactions"))
    if out["revenue"] is None:
        out["revenue"] = _safe_float(g("overall", "current", "purchaseRevenue"))
    if out["cvr"] is None:
        out["cvr"] = _safe_float(g("overall", "current", "cvr"))
    if out["signups"] is None:
        out["signups"] = _safe_int(g("signup_users", "current"))

    # normalize cvr if it looks like percent
    if out["cvr"] is not None and out["cvr"] > 1.0:
        out["cvr"] = out["cvr"] / 100.0

    return out


def _ratio(cur: Optional[float], prev: Optional[float]) -> Optional[float]:
    if cur is None or prev is None:
        return None
    try:
        if prev == 0:
            return None
        return (cur - prev) / prev
    except Exception:
        return None


def _pp(cur: Optional[float], prev: Optional[float]) -> Optional[float]:
    if cur is None or prev is None:
        return None
    try:
        return cur - prev
    except Exception:
        return None


def _load_bundle(p: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _daily_bundle_path(reports_dir: Path, ymd_str: str) -> Path:
    return reports_dir / "daily_digest" / "data" / "daily" / f"{ymd_str}.json"


def _weekly_bundle_path(reports_dir: Path, end_ymd: str) -> Path:
    return reports_dir / "daily_digest" / "data" / "weekly" / f"END_{end_ymd}.json"


def _ymd_minus_days(ymd_str: str, days: int) -> Optional[str]:
    d = parse_ymd(ymd_str)
    if not d:
        return None
    return ymd(d - timedelta(days=days))


def _ensure_daily_kpi_json(reports_dir: Path) -> Dict[str, Any]:
    """
    Returns daily_kpi dict. Auto-builds reports/daily_kpi.json if missing/stale.
    ✅ 기준일: 어제(KST) 파일을 우선 사용. 없으면 최신 파일 fallback.
    ✅ Daily Summary 비교 기준: WoW (전주 동일요일) + YoY
    """
    out_path = reports_dir / "daily_kpi.json"

    target_date = ymd(kst_yesterday())
    target_bundle = _find_daily_bundle_for_date(reports_dir, target_date)

    latest_bundle = target_bundle or _find_latest_daily_bundle(reports_dir)
    latest_date = _bundle_date_from_path(latest_bundle) if latest_bundle else None

    existing = _read_json_path(out_path) or {}
    existing_date = existing.get("date")

    need_rebuild = (not existing) or (latest_date and existing_date != latest_date) or ("wow" not in existing)

    if not need_rebuild:
        if not existing.get("updated"):
            existing["updated"] = now_kst_label()
            _write_json_path(out_path, existing)
        return existing

    if not latest_bundle or not latest_date:
        return {
            "date": None,
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

    cur_bundle = _load_bundle(latest_bundle) or {}
    cur_kpis = _extract_kpis_from_bundle(cur_bundle)

    wow_date = _ymd_minus_days(latest_date, 7)
    yoy_date = _ymd_minus_days(latest_date, 364)  # 기존 컨벤션 유지

    wow_kpis = {}
    yoy_kpis = {}

    if wow_date:
        wp = _daily_bundle_path(reports_dir, wow_date)
        bj = _load_bundle(wp)
        if bj:
            wow_kpis = _extract_kpis_from_bundle(bj)

    if yoy_date:
        yp = _daily_bundle_path(reports_dir, yoy_date)
        yj = _load_bundle(yp)
        if yj:
            yoy_kpis = _extract_kpis_from_bundle(yj)

    wow = {
        "sessions": _ratio(cur_kpis.get("sessions"), wow_kpis.get("sessions")),
        "orders": _ratio(cur_kpis.get("orders"), wow_kpis.get("orders")),
        "revenue": _ratio(cur_kpis.get("revenue"), wow_kpis.get("revenue")),
        "signups": _ratio(cur_kpis.get("signups"), wow_kpis.get("signups")),
        "cvr_pp": _pp(cur_kpis.get("cvr"), wow_kpis.get("cvr")),
    }
    yoy = {
        "sessions": _ratio(cur_kpis.get("sessions"), yoy_kpis.get("sessions")),
        "orders": _ratio(cur_kpis.get("orders"), yoy_kpis.get("orders")),
        "revenue": _ratio(cur_kpis.get("revenue"), yoy_kpis.get("revenue")),
        "signups": _ratio(cur_kpis.get("signups"), yoy_kpis.get("signups")),
        "cvr_pp": _pp(cur_kpis.get("cvr"), yoy_kpis.get("cvr")),
    }

    built = {
        "date": latest_date,
        "sessions": cur_kpis.get("sessions"),
        "orders": cur_kpis.get("orders"),
        "revenue": cur_kpis.get("revenue"),
        "cvr": cur_kpis.get("cvr"),
        "signups": cur_kpis.get("signups"),
        "wow": wow,
        "yoy": yoy,
        "updated": now_kst_label(),
        "source": str(latest_bundle),
    }

    _write_json_path(out_path, built)
    return built


def _ensure_weekly_kpi_json(reports_dir: Path) -> Dict[str, Any]:
    """
    Returns weekly_kpi dict. Auto-builds reports/weekly_kpi.json if missing/stale.
    ✅ 기준 end: 어제(KST) END_YYYY-MM-DD 를 우선 사용. 없으면 최신 END fallback.
    """
    out_path = reports_dir / "weekly_kpi.json"

    target_end = ymd(kst_yesterday())
    target_bundle = _find_weekly_bundle_for_end(reports_dir, target_end)

    latest_bundle = target_bundle or _find_latest_weekly_bundle(reports_dir)
    latest_end = _bundle_date_from_path(latest_bundle) if latest_bundle else None

    existing = _read_json_path(out_path) or {}
    existing_end = existing.get("end")

    need_rebuild = (not existing) or (latest_end and existing_end != latest_end)

    if not need_rebuild:
        if not existing.get("updated"):
            existing["updated"] = now_kst_label()
            _write_json_path(out_path, existing)
        return existing

    if not latest_bundle or not latest_end:
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

    cur_bundle = _load_bundle(latest_bundle) or {}
    cur_kpis = _extract_kpis_from_bundle(cur_bundle)

    end_d = parse_ymd(latest_end)
    start_s = ymd(end_d - timedelta(days=6)) if end_d else None

    prev_end = _ymd_minus_days(latest_end, 7)
    yoy_end = _ymd_minus_days(latest_end, 364)

    prev_kpis = {}
    yoy_kpis = {}

    if prev_end:
        pp = _weekly_bundle_path(reports_dir, prev_end)
        pj = _load_bundle(pp)
        if pj:
            prev_kpis = _extract_kpis_from_bundle(pj)

    if yoy_end:
        yp = _weekly_bundle_path(reports_dir, yoy_end)
        yj = _load_bundle(yp)
        if yj:
            yoy_kpis = _extract_kpis_from_bundle(yj)

    wow = {
        "sessions": _ratio(cur_kpis.get("sessions"), prev_kpis.get("sessions")),
        "orders": _ratio(cur_kpis.get("orders"), prev_kpis.get("orders")),
        "revenue": _ratio(cur_kpis.get("revenue"), prev_kpis.get("revenue")),
        "signups": _ratio(cur_kpis.get("signups"), prev_kpis.get("signups")),
        "cvr_pp": _pp(cur_kpis.get("cvr"), prev_kpis.get("cvr")),
    }
    yoy = {
        "sessions": _ratio(cur_kpis.get("sessions"), yoy_kpis.get("sessions")),
        "orders": _ratio(cur_kpis.get("orders"), yoy_kpis.get("orders")),
        "revenue": _ratio(cur_kpis.get("revenue"), yoy_kpis.get("revenue")),
        "signups": _ratio(cur_kpis.get("signups"), yoy_kpis.get("signups")),
        "cvr_pp": _pp(cur_kpis.get("cvr"), yoy_kpis.get("cvr")),
    }

    built = {
        "start": start_s,
        "end": latest_end,
        "sessions": cur_kpis.get("sessions"),
        "orders": cur_kpis.get("orders"),
        "revenue": cur_kpis.get("revenue"),
        "cvr": cur_kpis.get("cvr"),
        "signups": cur_kpis.get("signups"),
        "wow": wow,
        "yoy": yoy,
        "updated": now_kst_label(),
        "source": str(latest_bundle),
    }

    _write_json_path(out_path, built)
    return built


# -----------------------
# ✅ KPI loaders (use ensured JSON)
# -----------------------
def build_daily_kpis(reports_dir: Path) -> Dict[str, Any]:
    return _ensure_daily_kpi_json(reports_dir)


def build_weekly_kpis(reports_dir: Path) -> Dict[str, Any]:
    return _ensure_weekly_kpi_json(reports_dir)


# -----------------------
# ✅ OWNED YTD (EDM/LMS/KAKAO) from cached bundles
# -----------------------
OWNED_CHANNELS = ("EDM", "LMS", "KAKAO")


def _find_owned_data_dir(reports_dir: Path) -> Optional[Path]:
    """
    Find reports/**/data/owned directory that contains owned_YYYY-MM-DD.json bundles.
    Preference: the directory with most owned_*.json files.
    """
    candidates: List[Path] = []

    # common expected
    for p in [
        reports_dir / "owned_portal" / "data" / "owned",
        reports_dir / "owned" / "data" / "owned",
        # GitHub Pages publish path (when OWNED is synced to site/)
        reports_dir.parent / "site" / "data" / "owned",
    ]:
        if p.exists() and p.is_dir():
            candidates.append(p)

    # broad scan
    for p in reports_dir.glob("**/data/owned"):
        if p.exists() and p.is_dir():
            candidates.append(p)

    # unique
    uniq = []
    seen = set()
    for p in candidates:
        rp = str(p.resolve())
        if rp not in seen:
            uniq.append(p)
            seen.add(rp)

    if not uniq:
        return None

    def score_dir(p: Path) -> Tuple[int, float]:
        files = list(p.glob("owned_*.json"))
        count = len(files)
        newest = max([f.stat().st_mtime for f in files], default=0.0)
        return (count, newest)

    uniq.sort(key=score_dir, reverse=True)
    best = uniq[0]
    return best


def _owned_file_for_date(owned_dir: Path, ymd_str: str) -> Path:
    return owned_dir / f"owned_{ymd_str}.json"


def _sum_owned_day(owned_dir: Path, ymd_str: str) -> Dict[str, float]:
    """
    Sum KPI rows for the day across EDM/LMS/KAKAO from OWNED bundle.

    Your OWNED bundle shape (confirmed):
      { "date": "...", "campaigns": [ {channel, sessions, users, purchases, revenue, ...}, ...], "products": [...] }

    So we aggregate from "campaigns" (NOT from "kpi").
    Send Count = valid campaign rows count.
    """
    p = _owned_file_for_date(owned_dir, ymd_str)
    if not p.exists():
        return {"sessions": 0.0, "users": 0.0, "purchases": 0.0, "revenue": 0.0, "send_count": 0.0}

    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"sessions": 0.0, "users": 0.0, "purchases": 0.0, "revenue": 0.0, "send_count": 0.0}

    rows = obj.get("campaigns") or []
    s = u = pch = rev = send_count = 0.0

    for r in rows:
        ch = str(r.get("channel") or "").strip().upper()
        if ch not in OWNED_CHANNELS:
            continue

        campaign = str(r.get("campaign") or "").strip()
        mmdd = str(r.get("mmdd") or "").strip()
        term = str(r.get("term") or "").strip()
        if campaign or mmdd or term:
            send_count += 1.0

        try:
            s += float(r.get("sessions") or 0)
            u += float(r.get("users") or 0)
            pch += float(r.get("purchases") or 0)
            rev += float(r.get("revenue") or 0)
        except Exception:
            continue

    return {"sessions": s, "users": u, "purchases": pch, "revenue": rev, "send_count": send_count}


def _sum_owned_day_by_channel(owned_dir: Path, ymd_str: str) -> Dict[str, Dict[str, float]]:
    """Per-channel sum for the day from OWNED bundle campaigns."""
    p = _owned_file_for_date(owned_dir, ymd_str)
    out = {ch: {"sessions": 0.0, "users": 0.0, "purchases": 0.0, "revenue": 0.0, "send_count": 0.0} for ch in OWNED_CHANNELS}
    if not p.exists():
        return out

    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return out

    rows = obj.get("campaigns") or []
    for r in rows:
        ch = str(r.get("channel") or "").strip().upper()
        if ch not in out:
            continue

        campaign = str(r.get("campaign") or "").strip()
        mmdd = str(r.get("mmdd") or "").strip()
        term = str(r.get("term") or "").strip()
        if campaign or mmdd or term:
            out[ch]["send_count"] += 1.0

        try:
            out[ch]["sessions"] += float(r.get("sessions") or 0)
            out[ch]["users"] += float(r.get("users") or 0)
            out[ch]["purchases"] += float(r.get("purchases") or 0)
            out[ch]["revenue"] += float(r.get("revenue") or 0)
        except Exception:
            continue

    return out


def _daterange_list(start_d: date, end_d: date) -> List[str]:
    out = []
    cur = start_d
    while cur <= end_d:
        out.append(ymd(cur))
        cur += timedelta(days=1)
    return out


def _safe_ratio(cur: float, prev: float) -> Optional[float]:
    try:
        if prev == 0:
            return None
        return (cur - prev) / prev
    except Exception:
        return None


def build_owned_ytd_yoy(reports_dir: Path) -> Dict[str, Any]:
    """
    Compute YTD (Jan 1 ~ yesterday KST) for OWNED channels:
    - total EDM+LMS+KAKAO
    - per channel
    And YoY vs same-length period in previous year (calendar-aligned by day count).
    """
    owned_dir = _find_owned_data_dir(reports_dir)
    target_end = kst_yesterday()

    result: Dict[str, Any] = {
        "enabled": False,
        "owned_dir": str(owned_dir) if owned_dir else None,
        "period": None,
        "prev_period": None,
        "total": {},
        "total_prev": {},
        "total_yoy": {},
        "by_channel": {},
        "updated": now_kst_label(),
    }

    if not owned_dir:
        return result

    cur_start = date(target_end.year, 1, 1)
    cur_end = target_end
    days_len = (cur_end - cur_start).days  # 0-based
    prev_start = date(target_end.year - 1, 1, 1)
    prev_end = prev_start + timedelta(days=days_len)

    cur_dates = _daterange_list(cur_start, cur_end)
    prev_dates = _daterange_list(prev_start, prev_end)

    # totals
    cur_sum = {"sessions": 0.0, "users": 0.0, "purchases": 0.0, "revenue": 0.0, "send_count": 0.0}
    prev_sum = {"sessions": 0.0, "users": 0.0, "purchases": 0.0, "revenue": 0.0, "send_count": 0.0}

    for d in cur_dates:
        s = _sum_owned_day(owned_dir, d)
        for k in cur_sum:
            cur_sum[k] += float(s.get(k, 0.0))

    for d in prev_dates:
        s = _sum_owned_day(owned_dir, d)
        for k in prev_sum:
            prev_sum[k] += float(s.get(k, 0.0))

    # cvr = purchases / sessions
    cur_cvr = (cur_sum["purchases"] / cur_sum["sessions"]) if cur_sum["sessions"] > 0 else None
    prev_cvr = (prev_sum["purchases"] / prev_sum["sessions"]) if prev_sum["sessions"] > 0 else None

    total_yoy = {
        "send_count": _safe_ratio(cur_sum["send_count"], prev_sum["send_count"]),
        "sessions": _safe_ratio(cur_sum["sessions"], prev_sum["sessions"]),
        "users": _safe_ratio(cur_sum["users"], prev_sum["users"]),
        "purchases": _safe_ratio(cur_sum["purchases"], prev_sum["purchases"]),
        "revenue": _safe_ratio(cur_sum["revenue"], prev_sum["revenue"]),
        "cvr_pp": (cur_cvr - prev_cvr) if (cur_cvr is not None and prev_cvr is not None) else None,
    }

    total = dict(cur_sum)
    total["cvr"] = cur_cvr
    total_prev = dict(prev_sum)
    total_prev["cvr"] = prev_cvr

    # per channel
    cur_ch_sum = {ch: {"sessions": 0.0, "users": 0.0, "purchases": 0.0, "revenue": 0.0, "send_count": 0.0} for ch in OWNED_CHANNELS}
    prev_ch_sum = {ch: {"sessions": 0.0, "users": 0.0, "purchases": 0.0, "revenue": 0.0, "send_count": 0.0} for ch in OWNED_CHANNELS}

    for d in cur_dates:
        day = _sum_owned_day_by_channel(owned_dir, d)
        for ch in OWNED_CHANNELS:
            for k in cur_ch_sum[ch]:
                cur_ch_sum[ch][k] += float(day[ch].get(k, 0.0))

    for d in prev_dates:
        day = _sum_owned_day_by_channel(owned_dir, d)
        for ch in OWNED_CHANNELS:
            for k in prev_ch_sum[ch]:
                prev_ch_sum[ch][k] += float(day[ch].get(k, 0.0))

    by_channel: Dict[str, Any] = {}
    for ch in OWNED_CHANNELS:
        curv = cur_ch_sum[ch]
        prevv = prev_ch_sum[ch]
        cur_c = (curv["purchases"] / curv["sessions"]) if curv["sessions"] > 0 else None
        prev_c = (prevv["purchases"] / prevv["sessions"]) if prevv["sessions"] > 0 else None
        by_channel[ch] = {
            "cur": {**curv, "cvr": cur_c},
            "prev": {**prevv, "cvr": prev_c},
            "yoy": {
                "send_count": _safe_ratio(curv["send_count"], prevv["send_count"]),
                "sessions": _safe_ratio(curv["sessions"], prevv["sessions"]),
                "users": _safe_ratio(curv["users"], prevv["users"]),
                "purchases": _safe_ratio(curv["purchases"], prevv["purchases"]),
                "revenue": _safe_ratio(curv["revenue"], prevv["revenue"]),
                "cvr_pp": (cur_c - prev_c) if (cur_c is not None and prev_c is not None) else None,
            },
        }

    result.update({
        "enabled": True,
        "period": f"{ymd(cur_start)} ~ {ymd(cur_end)}",
        "prev_period": f"{ymd(prev_start)} ~ {ymd(prev_end)}",
        "total": total,
        "total_prev": total_prev,
        "total_yoy": total_yoy,
        "by_channel": by_channel,
    })
    return result


# -----------------------
# Legacy loaders kept for compatibility (unused in final HTML)
# -----------------------
def build_naver_metrics(repo_root: Path) -> Dict[str, Any]:
    latest = _pick_latest(str(repo_root / "result_*.csv"))
    if not latest:
        return {
            "total": None,
            "diff_pos": None,
            "diff_pos_ratio": None,
            "avg_gap": None,
            "risk_label": "-",
            "risk_class": "risk-unk",
            "updated": now_kst_label(),
        }

    try:
        df = _read_csv_any(latest)
    except Exception:
        return {
            "total": None,
            "diff_pos": None,
            "diff_pos_ratio": None,
            "avg_gap": None,
            "risk_label": "-",
            "risk_class": "risk-unk",
            "updated": now_kst_label(),
        }

    total = len(df)
    diff_col = _col(df, ["diff", "price_diff", "gap"]) 
    diff_pos = 0
    diff_pos_ratio = None
    avg_gap = None
    if diff_col:
        s = pd.to_numeric(df[diff_col], errors="coerce")
        diff_pos = int((s > 0).sum())
        diff_pos_ratio = (diff_pos / total) if total > 0 else None
        avg_gap = float(s[s > 0].mean()) if (s > 0).any() else 0.0

    label, klass = risk_level(diff_pos_ratio)
    return {
        "total": total,
        "diff_pos": diff_pos,
        "diff_pos_ratio": diff_pos_ratio,
        "avg_gap": avg_gap,
        "risk_label": label,
        "risk_class": klass,
        "updated": now_kst_label(),
    }


def _kw_clean(s: str) -> str:
    s = re.sub(r"\s+", " ", (s or "").strip())
    return s[:40] if len(s) > 40 else s


def build_hero_metrics(reports_dir: Path, crawl_limit: int = 40, crawl_sleep: float = 0.25) -> Dict[str, Any]:
    hero_dir = reports_dir / "hero_main"
    latest_csv = _pick_latest(str(hero_dir / "*.csv"))
    if not latest_csv:
        return {"brands": None, "banners": None, "missing_img": None, "kw_text": None, "updated": now_kst_label()}

    try:
        df = _read_csv_any(latest_csv)
    except Exception:
        return {"brands": None, "banners": None, "missing_img": None, "kw_text": None, "updated": now_kst_label()}

    brands = len(set(df[_col(df, ["brand"])]) ) if _col(df, ["brand"]) else None
    banners = len(df)
    img_col = _col(df, ["image", "img", "image_url", "img_url"])
    title_col = _col(df, ["title", "headline", "banner_text", "copy"])
    missing_img = 0
    if img_col:
        missing_img = int(df[img_col].fillna("").astype(str).str.strip().eq("").sum())

    kw_text = None
    if title_col:
        titles = [t for t in df[title_col].fillna("").astype(str).tolist() if t.strip()]
        kws = []
        for t in titles[:10]:
            kws.extend([_kw_clean(x) for x in re.split(r"[/|,]", t) if x.strip()])
        kw_text = " · ".join(kws[:5]) if kws else None

    return {
        "brands": brands,
        "banners": banners,
        "missing_img": missing_img,
        "kw_text": kw_text,
        "updated": now_kst_label(),
    }


def build_voc_metrics(reports_dir: Path) -> Dict[str, Any]:
    latest = _pick_latest(str(reports_dir / "external_signal*.json"))
    if not latest:
        latest = _pick_latest(str(reports_dir / "external_signal*.csv"))
    if not latest:
        return {"posts": None, "mentions": None, "range": "최근 7일", "updated": now_kst_label()}

    try:
        if latest.suffix.lower() == ".json":
            obj = json.loads(latest.read_text(encoding="utf-8"))
            posts = obj.get("posts") or obj.get("total_posts")
            mentions = obj.get("mentions") or obj.get("total_mentions")
            rng = obj.get("range") or "최근 7일"
        else:
            df = _read_csv_any(latest)
            posts = len(df)
            mentions = len(df)
            rng = "최근 7일"
        return {"posts": posts, "mentions": mentions, "range": rng, "updated": now_kst_label()}
    except Exception:
        return {"posts": None, "mentions": None, "range": "최근 7일", "updated": now_kst_label()}


def _fmt_top5(items: Any) -> str:
    if not items:
        return "-"
    try:
        if isinstance(items, list):
            parts = []
            for x in items[:5]:
                if isinstance(x, dict):
                    name = str(x.get("name") or x.get("keyword") or x.get("label") or "").strip()
                    cnt = x.get("count")
                    if name:
                        parts.append(f"{name}({cnt})" if cnt is not None else name)
                else:
                    parts.append(str(x))
            return " · ".join(parts) if parts else "-"
        return str(items)
    except Exception:
        return "-"


def build_crema_metrics(reports_dir: Path) -> Dict[str, Any]:
    latest = _pick_latest(str(reports_dir / "crema_raw" / "*.json"))
    if not latest:
        latest = _pick_latest(str(reports_dir / "voc_crema" / "*.json"))
    if not latest:
        return {"total_reviews": None, "date_range": None, "complaint_top5": None, "updated": now_kst_label()}

    try:
        obj = json.loads(latest.read_text(encoding="utf-8"))
    except Exception:
        return {"total_reviews": None, "date_range": None, "complaint_top5": None, "updated": now_kst_label()}

    if isinstance(obj, dict):
        total_reviews = obj.get("total_reviews") or obj.get("count")
        date_range = obj.get("date_range") or obj.get("range")
        complaint_top5 = obj.get("complaint_top5") or obj.get("top5")
    elif isinstance(obj, list):
        total_reviews = len(obj)
        date_range = None
        complaint_top5 = None
    else:
        total_reviews = None
        date_range = None
        complaint_top5 = None

    return {
        "total_reviews": total_reviews,
        "date_range": date_range,
        "complaint_top5": complaint_top5,
        "updated": now_kst_label(),
    }


# -----------------------
# HTML
# -----------------------
def render_index_html(
    daily: Dict[str, Any],
    weekly: Dict[str, Any],
    owned_ytd: Dict[str, Any],
) -> str:
    def stat(label: str, value: str) -> str:
        return f"""
          <div class="flex items-baseline justify-between gap-3">
            <div class="text-[11px] font-extrabold tracking-widest text-slate-500 uppercase">{label}</div>
            <div class="text-lg font-black text-slate-900">{value}</div>
          </div>
        """

    def open_btn(href: str) -> str:
        return f"""
          <a href="{href}" target="_self" rel="noopener"
             class="mt-6 w-full inline-flex items-center justify-center rounded-2xl px-4 py-3
                    bg-[color:var(--brand)] text-white font-black text-sm
                    shadow-sm hover:opacity-95 active:opacity-90 transition">
            Open report
          </a>
        """

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
    d_wow = daily.get("wow") or {}
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
                  f'WoW <b class="text-slate-900">{fmt_delta_ratio(d_wow.get("sessions"))}</b>',
                  f'YoY <b class="text-slate-900">{fmt_delta_ratio(d_yoy.get("sessions"))}</b>')}
        {kpi_tile("Orders", fmt_int(daily.get("orders")),
                  f'WoW <b class="text-slate-900">{fmt_delta_ratio(d_wow.get("orders"))}</b>',
                  f'YoY <b class="text-slate-900">{fmt_delta_ratio(d_yoy.get("orders"))}</b>')}
        {kpi_tile("Revenue", fmt_krw_symbol(daily.get("revenue")),
                  f'WoW <b class="text-slate-900">{fmt_delta_ratio(d_wow.get("revenue"))}</b>',
                  f'YoY <b class="text-slate-900">{fmt_delta_ratio(d_yoy.get("revenue"))}</b>')}
        {kpi_tile("CVR", fmt_cvr(daily.get("cvr")),
                  f'WoW <b class="text-slate-900">{fmt_pp_from_fraction(d_wow.get("cvr_pp"))}</b>',
                  f'YoY <b class="text-slate-900">{fmt_pp_from_fraction(d_yoy.get("cvr_pp"))}</b>')}
        {kpi_tile("Sign-up Users", fmt_int(daily.get("signups")),
                  f'WoW <b class="text-slate-900">{fmt_delta_ratio(d_wow.get("signups"))}</b>',
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

    # OWNED YTD YoY
    owned_block = ""
    if owned_ytd.get("enabled"):
        tot = owned_ytd.get("total") or {}
        tot_prev = owned_ytd.get("total_prev") or {}
        tot_yoy = owned_ytd.get("total_yoy") or {}
        period = owned_ytd.get("period") or "-"
        prev_period = owned_ytd.get("prev_period") or "-"
        upd = owned_ytd.get("updated") or ""
        owned_dir = owned_ytd.get("owned_dir") or ""

        owned_block = f"""
        <div class="glass-card rounded-3xl p-5 mb-6">
          <div class="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-2">
            <div class="flex items-center gap-3">
              <div class="text-base font-black text-slate-900">OWNED YTD YoY (EDM + LMS + KAKAO)</div>
              <span class="badge-soft">YTD</span>
            </div>
            <div class="text-xs text-slate-500">
              기간: <b class="text-slate-700">{period}</b> · YoY 비교: {prev_period} · updated: {upd}
            </div>
          </div>

          <div class="mt-4 grid grid-cols-2 sm:grid-cols-4 gap-3">
            {kpi_tile("Send Count", fmt_int(tot.get("send_count")),
                      f'YoY <b class="text-slate-900">{fmt_delta_ratio(tot_yoy.get("send_count"))}</b>',
                      f'LY <b class="text-slate-900">{fmt_int(tot_prev.get("send_count"))}</b>')}
            {kpi_tile("Sessions", fmt_int(tot.get("sessions")),
                      f'YoY <b class="text-slate-900">{fmt_delta_ratio(tot_yoy.get("sessions"))}</b>',
                      f'LY <b class="text-slate-900">{fmt_int(tot_prev.get("sessions"))}</b>')}
            {kpi_tile("Revenue", fmt_krw_symbol(tot.get("revenue")),
                      f'YoY <b class="text-slate-900">{fmt_delta_ratio(tot_yoy.get("revenue"))}</b>',
                      f'LY <b class="text-slate-900">{fmt_krw_symbol(tot_prev.get("revenue"))}</b>')}
            {kpi_tile("CVR", fmt_cvr(tot.get("cvr")),
                      f'YoY <b class="text-slate-900">{fmt_pp_from_fraction(tot_yoy.get("cvr_pp"))}</b>',
                      f'LY <b class="text-slate-900">{fmt_cvr(tot_prev.get("cvr"))}</b>')}
          </div>

          <div class="mt-4 grid grid-cols-1 lg:grid-cols-3 gap-3">
        """

        by_ch = owned_ytd.get("by_channel") or {}
        for ch in OWNED_CHANNELS:
            cur = (by_ch.get(ch) or {}).get("cur") or {}
            prev = (by_ch.get(ch) or {}).get("prev") or {}
            yoy = (by_ch.get(ch) or {}).get("yoy") or {}
            owned_block += f"""
            <div class="rounded-2xl border border-slate-200 bg-white/70 p-4">
              <div class="flex items-center justify-between">
                <div class="text-[11px] font-extrabold tracking-widest text-slate-500 uppercase">{ch} YTD</div>
                <span class="badge-soft">YoY</span>
              </div>
              <div class="mt-2 grid grid-cols-2 gap-3">
                <div>
                  <div class="text-xs text-slate-500">Send Count</div>
                  <div class="text-lg font-black text-slate-900">{fmt_int(cur.get("send_count"))}</div>
                  <div class="text-[11px] text-slate-500">LY {fmt_int(prev.get("send_count"))} · YoY <b class="text-slate-900">{fmt_delta_ratio(yoy.get("send_count"))}</b></div>
                </div>
                <div>
                  <div class="text-xs text-slate-500">Sessions</div>
                  <div class="text-lg font-black text-slate-900">{fmt_int(cur.get("sessions"))}</div>
                  <div class="text-[11px] text-slate-500">LY {fmt_int(prev.get("sessions"))} · YoY <b class="text-slate-900">{fmt_delta_ratio(yoy.get("sessions"))}</b></div>
                </div>
                <div>
                  <div class="text-xs text-slate-500">Revenue</div>
                  <div class="text-lg font-black text-slate-900">{fmt_krw_symbol(cur.get("revenue"))}</div>
                  <div class="text-[11px] text-slate-500">LY {fmt_krw_symbol(prev.get("revenue"))} · YoY <b class="text-slate-900">{fmt_delta_ratio(yoy.get("revenue"))}</b></div>
                </div>
                <div>
                  <div class="text-xs text-slate-500">CVR</div>
                  <div class="text-lg font-black text-slate-900">{fmt_cvr(cur.get("cvr"))}</div>
                  <div class="text-[11px] text-slate-500">LY {fmt_cvr(prev.get("cvr"))} · YoY <b class="text-slate-900">{fmt_pp_from_fraction(yoy.get("cvr_pp"))}</b></div>
                </div>
              </div>
            </div>
            """

        owned_block += f"""
          </div>

          <div class="mt-3 text-[11px] text-slate-500">
            source: {owned_dir}
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
    .subtle{{ margin-top:14px; padding:12px 14px; border-radius:18px; background: rgba(255,255,255,.55); border: 1px solid rgba(15,23,42,0.05); }}
    .embed-mobile body{{ background: transparent; }}
    @media (max-width: 768px){{
      .embed-mobile .glass-card{{ border-radius: 22px; box-shadow: 0 10px 30px rgba(15,23,42,0.07); }}
      .embed-mobile #summaryTop{{ padding: 14px 10px 88px; }}
      .embed-mobile .text-3xl.sm\:text-4xl{{ font-size: 1.6rem; line-height: 1.15; }}
      .embed-mobile .subtle{{ padding: 10px 12px; border-radius: 16px; }}
      .embed-mobile .badge, .embed-mobile .badge-soft{{ font-size: 10px; padding: 5px 8px; }}
      .embed-mobile .grid.grid-cols-2.sm\:grid-cols-3.lg\:grid-cols-5{{ grid-template-columns: repeat(2, minmax(0,1fr)); }}
      .embed-mobile .grid.grid-cols-2.sm\:grid-cols-4{{ grid-template-columns: repeat(2, minmax(0,1fr)); }}
      .embed-mobile .grid.grid-cols-1.lg\:grid-cols-3{{ grid-template-columns: repeat(1, minmax(0,1fr)); }}
      .embed-mobile .p-5{{ padding: 16px; }}
      .embed-mobile .text-lg{{ font-size: 1rem; line-height: 1.35rem; }}
      .embed-mobile .text-xl{{ font-size: 1.05rem; line-height: 1.45rem; }}
      .embed-mobile .text-base{{ font-size: .95rem; }}
      .embed-mobile a[href]{{ word-break: break-word; }}
    }}
  </style>
</head>
<body>
  <div id="mobileEmbedFab" class="hidden fixed right-4 bottom-20 z-[70]">
    <button id="btnOpenStandalone" type="button" class="rounded-full bg-[color:var(--brand)] text-white shadow-lg px-4 py-3 text-xs font-black tracking-wide">전체화면</button>
  </div>
  <div id="mobileQuickNav" class="hidden fixed inset-x-0 bottom-0 z-[60] border-t border-slate-200 bg-white/95 backdrop-blur px-2 py-2 shadow-[0_-10px_30px_rgba(15,23,42,0.08)]">
    <div class="grid grid-cols-4 gap-2 max-w-md mx-auto">
      <button type="button" data-target="summaryTop" class="quick-nav-btn rounded-2xl px-2 py-2 text-[11px] font-black text-slate-700 bg-slate-100">TOP</button>
      <button type="button" data-target="dailyKpi" class="quick-nav-btn rounded-2xl px-2 py-2 text-[11px] font-black text-slate-700 bg-slate-100">DAILY</button>
      <button type="button" data-target="weeklyKpi" class="quick-nav-btn rounded-2xl px-2 py-2 text-[11px] font-black text-slate-700 bg-slate-100">WEEKLY</button>
      <button type="button" data-target="ownedYtd" class="quick-nav-btn rounded-2xl px-2 py-2 text-[11px] font-black text-slate-700 bg-slate-100">OWNED</button>
    </div>
  </div>
  <div id="summaryTop" class="px-2 sm:px-6 py-6">
    <div class="mb-6">
      <div class="text-3xl sm:text-4xl font-black tracking-tight">오늘의 핵심 요약</div>
      <div class="mt-2 text-xs text-slate-500">기준일 기본값: 어제(KST) · generated: {now_kst_label()}</div>
    </div>

    <section id="dailyKpi">{daily_strip}</section>
    <section id="weeklyKpi">{weekly_strip}</section>
    <section id="ownedYtd">{owned_block}</section>
  </div>
  <script>
    (function(){{
      const isEmbedded = window.self !== window.top || new URLSearchParams(window.location.search).get('embed') === '1';
      const isMobile = window.matchMedia('(max-width: 768px)').matches;
      if(!(isEmbedded && isMobile)) return;

      document.documentElement.classList.add('embed-mobile');
      document.body.classList.add('embed-mobile');

      const nav = document.getElementById('mobileQuickNav');
      const fab = document.getElementById('mobileEmbedFab');
      if(nav) nav.classList.remove('hidden');
      if(fab) fab.classList.remove('hidden');

      const openBtn = document.getElementById('btnOpenStandalone');
      if(openBtn){{
        openBtn.addEventListener('click', function(){{
          const u = new URL(window.location.href);
          u.searchParams.delete('embed');
          window.open(u.toString(), '_blank', 'noopener');
        }});
      }}

      document.querySelectorAll('.quick-nav-btn').forEach(function(btn){{
        btn.addEventListener('click', function(){{
          const id = btn.getAttribute('data-target');
          const el = document.getElementById(id);
          if(el){{
            el.scrollIntoView({{behavior:'smooth', block:'start'}});
          }}
        }});
      }});
    }})();
  </script>
</body>
</html>
"""


def main() -> None:
    repo_root = Path(".").resolve()
    reports_dir = repo_root / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    # ✅ KPI: always ensured from cached bundles (prefer yesterday)
    daily = build_daily_kpis(reports_dir)
    weekly = build_weekly_kpis(reports_dir)

    # ✅ OWNED YTD YoY (prefer yesterday range; no GA4/BQ)
    owned_ytd = build_owned_ytd_yoy(reports_dir)

    out = reports_dir / "index.html"
    out.write_text(render_index_html(daily, weekly, owned_ytd), encoding="utf-8")

    print(f"[OK] Wrote: {out}")
    print(f"[OK] Daily KPI date: {daily.get('date')} (source: {daily.get('source')})")
    print(f"[OK] Weekly KPI end: {weekly.get('end')} (source: {weekly.get('source')})")
    if owned_ytd.get("enabled"):
        print(f"[OK] OWNED YTD: {owned_ytd.get('period')}  (source dir: {owned_ytd.get('owned_dir')})")
    else:
        print("[WARN] OWNED YTD disabled: owned bundles not found under reports/**/data/owned/owned_*.json")


if __name__ == "__main__":
    main()
