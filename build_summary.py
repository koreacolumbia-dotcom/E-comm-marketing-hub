#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import glob
import json
import re
from pathlib import Path
from datetime import datetime, timezone, timedelta, date
from typing import Any, Dict, Optional, List, Tuple

import pandas as pd

KST = timezone(timedelta(hours=9))


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


def fmt_int(x: Any) -> str:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return "-"
        return f"{int(round(float(x))):,}"
    except Exception:
        return "-"


def fmt_krw_symbol(x: Any) -> str:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return "-"
        return f"₩{int(round(float(x))):,}"
    except Exception:
        return "-"


def fmt_cvr(x: Any) -> str:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return "-"
        return f"{float(x) * 100:.2f}%"
    except Exception:
        return "-"


def fmt_delta_ratio(x: Any, digits: int = 1) -> str:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return "-"
        v = float(x)
        sign = "+" if v >= 0 else ""
        return f"{sign}{v * 100:.{digits}f}%"
    except Exception:
        return "-"


def fmt_pp_from_fraction(x: Any) -> str:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return "-"
        v = float(x)
        sign = "+" if v >= 0 else ""
        return f"{sign}{v * 100:.2f}%p"
    except Exception:
        return "-"


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
            "date": None, "sessions": None, "orders": None, "revenue": None,
            "cvr": None, "signups": None, "wow": {}, "yoy": {},
            "updated": now_kst_label(), "source": None,
        }

    cur_bundle = _load_bundle(latest_bundle) or {}
    cur_kpis = _extract_kpis_from_bundle(cur_bundle)
    wow_date = _ymd_minus_days(latest_date, 7)
    yoy_date = _ymd_minus_days(latest_date, 364)
    wow_kpis = {}
    yoy_kpis = {}

    if wow_date:
        bj = _load_bundle(_daily_bundle_path(reports_dir, wow_date))
        if bj:
            wow_kpis = _extract_kpis_from_bundle(bj)
    if yoy_date:
        yj = _load_bundle(_daily_bundle_path(reports_dir, yoy_date))
        if yj:
            yoy_kpis = _extract_kpis_from_bundle(yj)

    built = {
        "date": latest_date,
        "sessions": cur_kpis.get("sessions"),
        "orders": cur_kpis.get("orders"),
        "revenue": cur_kpis.get("revenue"),
        "cvr": cur_kpis.get("cvr"),
        "signups": cur_kpis.get("signups"),
        "wow": {
            "sessions": _ratio(cur_kpis.get("sessions"), wow_kpis.get("sessions")),
            "orders": _ratio(cur_kpis.get("orders"), wow_kpis.get("orders")),
            "revenue": _ratio(cur_kpis.get("revenue"), wow_kpis.get("revenue")),
            "signups": _ratio(cur_kpis.get("signups"), wow_kpis.get("signups")),
            "cvr_pp": _pp(cur_kpis.get("cvr"), wow_kpis.get("cvr")),
        },
        "yoy": {
            "sessions": _ratio(cur_kpis.get("sessions"), yoy_kpis.get("sessions")),
            "orders": _ratio(cur_kpis.get("orders"), yoy_kpis.get("orders")),
            "revenue": _ratio(cur_kpis.get("revenue"), yoy_kpis.get("revenue")),
            "signups": _ratio(cur_kpis.get("signups"), yoy_kpis.get("signups")),
            "cvr_pp": _pp(cur_kpis.get("cvr"), yoy_kpis.get("cvr")),
        },
        "updated": now_kst_label(),
        "source": str(latest_bundle),
    }
    _write_json_path(out_path, built)
    return built


def _ensure_weekly_kpi_json(reports_dir: Path) -> Dict[str, Any]:
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
            "start": None, "end": None, "sessions": None, "orders": None,
            "revenue": None, "cvr": None, "signups": None,
            "wow": {}, "yoy": {}, "updated": now_kst_label(), "source": None,
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
        pj = _load_bundle(_weekly_bundle_path(reports_dir, prev_end))
        if pj:
            prev_kpis = _extract_kpis_from_bundle(pj)
    if yoy_end:
        yj = _load_bundle(_weekly_bundle_path(reports_dir, yoy_end))
        if yj:
            yoy_kpis = _extract_kpis_from_bundle(yj)

    built = {
        "start": start_s,
        "end": latest_end,
        "sessions": cur_kpis.get("sessions"),
        "orders": cur_kpis.get("orders"),
        "revenue": cur_kpis.get("revenue"),
        "cvr": cur_kpis.get("cvr"),
        "signups": cur_kpis.get("signups"),
        "wow": {
            "sessions": _ratio(cur_kpis.get("sessions"), prev_kpis.get("sessions")),
            "orders": _ratio(cur_kpis.get("orders"), prev_kpis.get("orders")),
            "revenue": _ratio(cur_kpis.get("revenue"), prev_kpis.get("revenue")),
            "signups": _ratio(cur_kpis.get("signups"), prev_kpis.get("signups")),
            "cvr_pp": _pp(cur_kpis.get("cvr"), prev_kpis.get("cvr")),
        },
        "yoy": {
            "sessions": _ratio(cur_kpis.get("sessions"), yoy_kpis.get("sessions")),
            "orders": _ratio(cur_kpis.get("orders"), yoy_kpis.get("orders")),
            "revenue": _ratio(cur_kpis.get("revenue"), yoy_kpis.get("revenue")),
            "signups": _ratio(cur_kpis.get("signups"), yoy_kpis.get("signups")),
            "cvr_pp": _pp(cur_kpis.get("cvr"), yoy_kpis.get("cvr")),
        },
        "updated": now_kst_label(),
        "source": str(latest_bundle),
    }
    _write_json_path(out_path, built)
    return built


def build_daily_kpis(reports_dir: Path) -> Dict[str, Any]:
    return _ensure_daily_kpi_json(reports_dir)


def build_weekly_kpis(reports_dir: Path) -> Dict[str, Any]:
    return _ensure_weekly_kpi_json(reports_dir)


OWNED_CHANNELS = ("EDM", "LMS", "KAKAO")


def _find_owned_data_dir(reports_dir: Path) -> Optional[Path]:
    candidates: List[Path] = []
    for p in [
        reports_dir / "owned_portal" / "data" / "owned",
        reports_dir / "owned" / "data" / "owned",
        reports_dir.parent / "site" / "data" / "owned",
    ]:
        if p.exists() and p.is_dir():
            candidates.append(p)
    for p in reports_dir.glob("**/data/owned"):
        if p.exists() and p.is_dir():
            candidates.append(p)

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
        return (len(files), max([f.stat().st_mtime for f in files], default=0.0))

    uniq.sort(key=score_dir, reverse=True)
    return uniq[0]


def _find_latest_owned_bundle(owned_dir: Path) -> Optional[Path]:
    files = [p for p in owned_dir.glob("owned_*.json") if p.is_file()]
    if not files:
        return None
    files.sort(key=lambda p: p.name, reverse=True)
    return files[0]


def _parse_row_date(row: Dict[str, Any]) -> Optional[date]:
    ds = str(row.get("date") or "").strip()
    try:
        return datetime.strptime(ds, "%Y-%m-%d").date()
    except Exception:
        return None


def _owned_is_countable_send_row(row: Dict[str, Any]) -> bool:
    ch = str(row.get("channel") or "").strip().upper()
    if ch not in OWNED_CHANNELS:
        return False
    if ch != "KAKAO":
        return True
    campaign = str(row.get("campaign") or "").strip().upper()
    term = str(row.get("term") or "").strip().upper()
    return campaign.startswith("KAKAO_CH_EVENT") and term.startswith("KAKAO_CH_MESSAGE_")


def _owned_valid_send_row(row: Dict[str, Any]) -> bool:
    ch = str(row.get("channel") or "").strip().upper()
    if ch not in OWNED_CHANNELS or not _owned_is_countable_send_row(row):
        return False
    year = str(row.get("year") or "").strip()
    mmdd = str(row.get("mmdd") or "").strip() or _extract_mmdd_token(str(row.get("campaign") or ""))
    row_dt = _parse_row_date(row)
    if row_dt and not year:
        year = str(row_dt.year)
    return bool(re.fullmatch(r"\d{4}", year) and re.fullmatch(r"\d{4}", mmdd))


def _extract_mmdd_token(text: str) -> str:
    s = str(text or "").strip()
    if not s:
        return ""
    hits = re.findall(r"(?<!\d)(\d{4})(?!\d)", s)
    for tok in hits:
        try:
            mm = int(tok[:2]); dd = int(tok[2:])
            if 1 <= mm <= 12 and 1 <= dd <= 31:
                return tok
        except Exception:
            continue
    return ""


def _owned_send_group_key(row: Dict[str, Any]) -> Tuple[str, str, str]:
    ch = str(row.get("channel") or "").strip().upper()
    yr = str(row.get("year") or "").strip()
    mmdd = str(row.get("mmdd") or "").strip()
    campaign = str(row.get("campaign") or "").strip()
    row_dt = _parse_row_date(row)

    bucket = mmdd or _extract_mmdd_token(campaign)
    if not bucket and row_dt:
        bucket = row_dt.strftime("%m%d")
    if not bucket:
        bucket = campaign or "UNKNOWN"
    return (ch, yr, bucket)


def _aggregate_owned_rows(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    out = {"sessions": 0.0, "users": 0.0, "purchases": 0.0, "revenue": 0.0, "send_count": 0.0}

    for r in rows:
        ch = str(r.get("channel") or "").strip().upper()
        if ch not in OWNED_CHANNELS:
            continue

        out["sessions"] += float(_safe_float(r.get("sessions")) or 0.0)
        out["users"] += float(_safe_float(r.get("users")) or 0.0)
        out["purchases"] += float(_safe_float(r.get("purchases")) or 0.0)
        out["revenue"] += float(_safe_float(r.get("revenue")) or 0.0)

        # Preferred: trust the campaign row's send_count from owned builder.
        # Fallback: count as 1 only when the row looks like a valid send row.
        send_v = _safe_float(r.get("send_count"))
        if send_v is not None and send_v > 0:
            out["send_count"] += float(send_v)
        elif _owned_valid_send_row(r):
            out["send_count"] += 1.0

    return out


def _aggregate_owned_rows_by_channel(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    out = {ch: {"sessions": 0.0, "users": 0.0, "purchases": 0.0, "revenue": 0.0, "send_count": 0.0} for ch in OWNED_CHANNELS}

    for r in rows:
        ch = str(r.get("channel") or "").strip().upper()
        if ch not in OWNED_CHANNELS:
            continue

        out[ch]["sessions"] += float(_safe_float(r.get("sessions")) or 0.0)
        out[ch]["users"] += float(_safe_float(r.get("users")) or 0.0)
        out[ch]["purchases"] += float(_safe_float(r.get("purchases")) or 0.0)
        out[ch]["revenue"] += float(_safe_float(r.get("revenue")) or 0.0)

        send_v = _safe_float(r.get("send_count"))
        if send_v is not None and send_v > 0:
            out[ch]["send_count"] += float(send_v)
        elif _owned_valid_send_row(r):
            out[ch]["send_count"] += 1.0

    return out


def _normalize_owned_summary(summary: Dict[str, Any]) -> Dict[str, Any]:
    send_count = _safe_float(summary.get("send_count")) or 0.0
    sessions = _safe_float(summary.get("sessions")) or 0.0
    users = _safe_float(summary.get("users")) or 0.0
    purchases = _safe_float(summary.get("purchases")) or 0.0
    revenue = _safe_float(summary.get("revenue")) or 0.0
    cvr = _safe_float(summary.get("cvr"))
    if cvr is None:
        cvr = (purchases / sessions) if sessions > 0 else None
    elif cvr > 1.0:
        cvr = cvr / 100.0
    return {
        "send_count": send_count,
        "sessions": sessions,
        "users": users,
        "purchases": purchases,
        "revenue": revenue,
        "cvr": cvr,
    }


def _build_owned_result_from_ytd_yoy(obj: Dict[str, Any], owned_dir: Path) -> Optional[Dict[str, Any]]:
    ytd = obj.get("ytd_yoy")
    if not isinstance(ytd, dict) or not ytd.get("enabled"):
        return None

    cur_summary = _normalize_owned_summary(((ytd.get("current") or {}).get("summary") or {}))
    prev_summary = _normalize_owned_summary(((ytd.get("previous") or {}).get("summary") or {}))
    cur_rows = (ytd.get("current") or {}).get("campaigns") or []
    prev_rows = (ytd.get("previous") or {}).get("campaigns") or []

    total_yoy = {
        "send_count": _ratio(cur_summary.get("send_count"), prev_summary.get("send_count")),
        "sessions": _ratio(cur_summary.get("sessions"), prev_summary.get("sessions")),
        "users": _ratio(cur_summary.get("users"), prev_summary.get("users")),
        "purchases": _ratio(cur_summary.get("purchases"), prev_summary.get("purchases")),
        "revenue": _ratio(cur_summary.get("revenue"), prev_summary.get("revenue")),
        "cvr_pp": _pp(cur_summary.get("cvr"), prev_summary.get("cvr")),
    }

    cur_ch_sum = _aggregate_owned_rows_by_channel(cur_rows)
    prev_ch_sum = _aggregate_owned_rows_by_channel(prev_rows)
    by_channel = {}
    for ch in OWNED_CHANNELS:
        curv = cur_ch_sum.get(ch) or {"sessions": 0.0, "users": 0.0, "purchases": 0.0, "revenue": 0.0, "send_count": 0.0}
        prevv = prev_ch_sum.get(ch) or {"sessions": 0.0, "users": 0.0, "purchases": 0.0, "revenue": 0.0, "send_count": 0.0}
        cur_c = (curv["purchases"] / curv["sessions"]) if curv["sessions"] > 0 else None
        prev_c = (prevv["purchases"] / prevv["sessions"]) if prevv["sessions"] > 0 else None
        by_channel[ch] = {
            "cur": {**curv, "cvr": cur_c},
            "prev": {**prevv, "cvr": prev_c},
            "yoy": {
                "send_count": _ratio(curv["send_count"], prevv["send_count"]),
                "sessions": _ratio(curv["sessions"], prevv["sessions"]),
                "users": _ratio(curv["users"], prevv["users"]),
                "purchases": _ratio(curv["purchases"], prevv["purchases"]),
                "revenue": _ratio(curv["revenue"], prevv["revenue"]),
                "cvr_pp": _pp(cur_c, prev_c),
            },
        }

    return {
        "enabled": True,
        "owned_dir": str(owned_dir),
        "period": ytd.get("period"),
        "prev_period": ytd.get("prev_period"),
        "total": cur_summary,
        "total_prev": prev_summary,
        "total_yoy": total_yoy,
        "by_channel": by_channel,
        "updated": now_kst_label(),
        "source": "ytd_yoy",
    }



def _owned_bundle_paths(owned_dir: Path) -> List[Path]:
    files = [p for p in owned_dir.glob("owned_*.json") if p.is_file()]
    files.sort(key=lambda p: p.name)
    return files


def _load_owned_bundle_rows_for_ytd(owned_dir: Path, target_end: date) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Collect YTD campaign rows from all daily owned bundles on disk.

    Why this is needed:
    - latest owned_YYYY-MM-DD.json is a *daily* bundle, not a full YTD bundle
    - relying on only latest bundle.campaigns makes YTD totals incorrect
    - previous-year rows may also be merged into current-day bundles, so we must de-duplicate
    """
    cur_year = str(target_end.year)
    prev_year = str(target_end.year - 1)
    cutoff_mmdd = target_end.strftime("%m-%d")

    cur_rows: List[Dict[str, Any]] = []
    prev_rows: List[Dict[str, Any]] = []
    seen_cur = set()
    seen_prev = set()

    for p in _owned_bundle_paths(owned_dir):
        obj = _load_bundle(p)
        if not obj:
            continue
        for r in (obj.get("campaigns") or []):
            ch = str(r.get("channel") or "").strip().upper()
            if ch not in OWNED_CHANNELS:
                continue
            row_dt = _parse_row_date(r)
            if not row_dt:
                continue

            row_year = str(r.get("year") or "").strip() or str(row_dt.year)
            key = (
                str(r.get("date") or "").strip(),
                ch,
                str(r.get("campaign") or "").strip(),
                str(r.get("term") or "").strip(),
            )

            if row_year == cur_year:
                if row_dt.year == target_end.year and row_dt <= target_end and key not in seen_cur:
                    seen_cur.add(key)
                    cur_rows.append(r)
            elif row_year == prev_year:
                if row_dt.year == (target_end.year - 1) and row_dt.strftime("%m-%d") <= cutoff_mmdd and key not in seen_prev:
                    seen_prev.add(key)
                    prev_rows.append(r)

    return cur_rows, prev_rows
def build_owned_ytd_yoy(reports_dir: Path) -> Dict[str, Any]:
    owned_dir = _find_owned_data_dir(reports_dir)
    target_end = kst_yesterday()
    result: Dict[str, Any] = {
        "enabled": False, "owned_dir": str(owned_dir) if owned_dir else None,
        "period": None, "prev_period": None, "total": {}, "total_prev": {},
        "total_yoy": {}, "by_channel": {}, "updated": now_kst_label(),
    }
    if not owned_dir:
        return result

    latest_bundle = _find_latest_owned_bundle(owned_dir)
    if latest_bundle:
        try:
            obj = json.loads(latest_bundle.read_text(encoding="utf-8"))
        except Exception:
            obj = {}
        ytd_based = _build_owned_result_from_ytd_yoy(obj, owned_dir)
        if ytd_based:
            return ytd_based

    # Fallback: compute true YTD by scanning all owned daily bundles on disk.
    cur_rows, prev_rows = _load_owned_bundle_rows_for_ytd(owned_dir, target_end)

    cur_sum = _aggregate_owned_rows(cur_rows)
    prev_sum = _aggregate_owned_rows(prev_rows)
    cur_cvr = (cur_sum["purchases"] / cur_sum["sessions"]) if cur_sum["sessions"] > 0 else None
    prev_cvr = (prev_sum["purchases"] / prev_sum["sessions"]) if prev_sum["sessions"] > 0 else None

    total_yoy = {
        "send_count": _ratio(cur_sum["send_count"], prev_sum["send_count"]),
        "sessions": _ratio(cur_sum["sessions"], prev_sum["sessions"]),
        "users": _ratio(cur_sum["users"], prev_sum["users"]),
        "purchases": _ratio(cur_sum["purchases"], prev_sum["purchases"]),
        "revenue": _ratio(cur_sum["revenue"], prev_sum["revenue"]),
        "cvr_pp": (cur_cvr - prev_cvr) if (cur_cvr is not None and prev_cvr is not None) else None,
    }

    total = dict(cur_sum); total["cvr"] = cur_cvr
    total_prev = dict(prev_sum); total_prev["cvr"] = prev_cvr
    cur_ch_sum = _aggregate_owned_rows_by_channel(cur_rows)
    prev_ch_sum = _aggregate_owned_rows_by_channel(prev_rows)

    by_channel = {}
    for ch in OWNED_CHANNELS:
        curv = cur_ch_sum[ch]
        prevv = prev_ch_sum[ch]
        cur_c = (curv["purchases"] / curv["sessions"]) if curv["sessions"] > 0 else None
        prev_c = (prevv["purchases"] / prevv["sessions"]) if prevv["sessions"] > 0 else None
        by_channel[ch] = {
            "cur": {**curv, "cvr": cur_c},
            "prev": {**prevv, "cvr": prev_c},
            "yoy": {
                "send_count": _ratio(curv["send_count"], prevv["send_count"]),
                "sessions": _ratio(curv["sessions"], prevv["sessions"]),
                "users": _ratio(curv["users"], prevv["users"]),
                "purchases": _ratio(curv["purchases"], prevv["purchases"]),
                "revenue": _ratio(curv["revenue"], prevv["revenue"]),
                "cvr_pp": (cur_c - prev_c) if (cur_c is not None and prev_c is not None) else None,
            },
        }

    result.update({
        "enabled": True,
        "period": f"{target_end.year}-01-01 ~ {ymd(target_end)}",
        "prev_period": f"{target_end.year - 1}-01-01 ~ {target_end.year - 1}-{target_end.strftime('%m-%d')}",
        "total": total, "total_prev": total_prev, "total_yoy": total_yoy,
        "by_channel": by_channel, "updated": now_kst_label(),
        "source": str(latest_bundle) if latest_bundle else str(owned_dir),
    })
    return result

    ytd_based = _build_owned_result_from_ytd_yoy(obj, owned_dir)
    if ytd_based:
        return ytd_based

    all_rows = obj.get("campaigns") or []
    cutoff_mmdd = target_end.strftime("%m-%d")
    cur_rows = []
    prev_rows = []

    for r in all_rows:
        ch = str(r.get("channel") or "").strip().upper()
        yr = str(r.get("year") or "").strip()
        row_dt = _parse_row_date(r)
        if ch not in OWNED_CHANNELS or not row_dt:
            continue
        row_mmdd = row_dt.strftime("%m-%d")
        if yr == str(target_end.year):
            if row_dt.year == target_end.year and row_dt <= target_end:
                cur_rows.append(r)
        elif yr == str(target_end.year - 1):
            if row_mmdd <= cutoff_mmdd:
                prev_rows.append(r)

    cur_sum = _aggregate_owned_rows(cur_rows)
    prev_sum = _aggregate_owned_rows(prev_rows)
    cur_cvr = (cur_sum["purchases"] / cur_sum["sessions"]) if cur_sum["sessions"] > 0 else None
    prev_cvr = (prev_sum["purchases"] / prev_sum["sessions"]) if prev_sum["sessions"] > 0 else None

    total_yoy = {
        "send_count": _ratio(cur_sum["send_count"], prev_sum["send_count"]),
        "sessions": _ratio(cur_sum["sessions"], prev_sum["sessions"]),
        "users": _ratio(cur_sum["users"], prev_sum["users"]),
        "purchases": _ratio(cur_sum["purchases"], prev_sum["purchases"]),
        "revenue": _ratio(cur_sum["revenue"], prev_sum["revenue"]),
        "cvr_pp": (cur_cvr - prev_cvr) if (cur_cvr is not None and prev_cvr is not None) else None,
    }

    total = dict(cur_sum); total["cvr"] = cur_cvr
    total_prev = dict(prev_sum); total_prev["cvr"] = prev_cvr
    cur_ch_sum = _aggregate_owned_rows_by_channel(cur_rows)
    prev_ch_sum = _aggregate_owned_rows_by_channel(prev_rows)

    by_channel = {}
    for ch in OWNED_CHANNELS:
        curv = cur_ch_sum[ch]
        prevv = prev_ch_sum[ch]
        cur_c = (curv["purchases"] / curv["sessions"]) if curv["sessions"] > 0 else None
        prev_c = (prevv["purchases"] / prevv["sessions"]) if prevv["sessions"] > 0 else None
        by_channel[ch] = {
            "cur": {**curv, "cvr": cur_c},
            "prev": {**prevv, "cvr": prev_c},
            "yoy": {
                "send_count": _ratio(curv["send_count"], prevv["send_count"]),
                "sessions": _ratio(curv["sessions"], prevv["sessions"]),
                "users": _ratio(curv["users"], prevv["users"]),
                "purchases": _ratio(curv["purchases"], prevv["purchases"]),
                "revenue": _ratio(curv["revenue"], prevv["revenue"]),
                "cvr_pp": (cur_c - prev_c) if (cur_c is not None and prev_c is not None) else None,
            },
        }

    result.update({
        "enabled": True,
        "period": f"{target_end.year}-01-01 ~ {ymd(target_end)}",
        "prev_period": f"{target_end.year - 1}-01-01 ~ {target_end.year - 1}-{target_end.strftime('%m-%d')}",
        "total": total, "total_prev": total_prev, "total_yoy": total_yoy,
        "by_channel": by_channel, "updated": now_kst_label(),
        "source": str(latest_bundle),
    })
    return result


def render_index_html(daily: Dict[str, Any], weekly: Dict[str, Any], owned_ytd: Dict[str, Any]) -> str:
    def tone_cls_from_delta(x: Any) -> str:
        try:
            v = float(x)
        except Exception:
            return "text-slate-600"
        return "text-emerald-600" if v >= 0 else "text-rose-600"

    def pp_tone_cls(x: Any) -> str:
        try:
            v = float(x)
        except Exception:
            return "text-slate-600"
        return "text-emerald-600" if v >= 0 else "text-rose-600"

    def metric_tile(title: str, value: str, sub_a_label: str, sub_a_value: str, sub_a_cls: str, sub_b_label: str, sub_b_value: str, sub_b_cls: str, accent: str = "") -> str:
        accent_style = f"style=\"--accent:{accent};\"" if accent else ""
        return f'''
        <article class="metric-card reveal rounded-[28px] p-4 sm:p-5" {accent_style}>
          <div class="metric-glow"></div>
          <div class="relative z-10">
            <div class="text-[11px] font-extrabold tracking-[0.22em] text-slate-500 uppercase">{title}</div>
            <div class="mt-3 flex items-end justify-between gap-3">
              <div class="metric-value text-2xl sm:text-[2rem] font-black leading-none text-slate-950" data-countup="{value}">{value}</div>
              <div class="hidden sm:block h-10 w-10 rounded-2xl bg-white/80 border border-white/70 shadow-[0_12px_30px_rgba(15,23,42,.08)]"></div>
            </div>
            <div class="mt-4 space-y-2 text-[11px] sm:text-xs">
              <div class="flex items-center justify-between gap-3 rounded-2xl bg-white/70 px-3 py-2 border border-white/70">
                <span class="text-slate-500">{sub_a_label}</span>
                <span class="font-extrabold {sub_a_cls}">{sub_a_value}</span>
              </div>
              <div class="flex items-center justify-between gap-3 rounded-2xl bg-white/70 px-3 py-2 border border-white/70">
                <span class="text-slate-500">{sub_b_label}</span>
                <span class="font-extrabold {sub_b_cls}">{sub_b_value}</span>
              </div>
            </div>
          </div>
        </article>
        '''

    def section_shell(section_id: str, eyebrow: str, title: str, desc: str, meta: str, accent: str, inner_html: str) -> str:
        return f'''
        <section id="{section_id}" class="summary-section reveal relative overflow-hidden rounded-[32px] p-[1px] mb-7" style="--section-accent:{accent};">
          <div class="section-border absolute inset-0"></div>
          <div class="relative rounded-[31px] px-4 py-5 sm:px-6 sm:py-6">
            <div class="absolute inset-x-0 top-0 h-24 bg-[radial-gradient(circle_at_top,rgba(255,255,255,.88),transparent_72%)] pointer-events-none"></div>
            <div class="relative z-10 flex flex-col xl:flex-row xl:items-end xl:justify-between gap-4 mb-5">
              <div>
                <div class="section-eyebrow">{eyebrow}</div>
                <h2 class="mt-2 text-2xl sm:text-[2rem] font-black tracking-tight text-slate-950">{title}</h2>
                <p class="mt-2 text-sm text-slate-600 max-w-3xl">{desc}</p>
              </div>
              <div class="section-meta text-xs text-slate-600">
                {meta}
              </div>
            </div>
            {inner_html}
          </div>
        </section>
        '''

    d_wow = daily.get("wow") or {}
    d_yoy = daily.get("yoy") or {}
    w_wow = weekly.get("wow") or {}
    w_yoy = weekly.get("yoy") or {}

    daily_tiles = "".join([
        metric_tile("Sessions", fmt_int(daily.get("sessions")), "WoW", fmt_delta_ratio(d_wow.get("sessions")), tone_cls_from_delta(d_wow.get("sessions")), "YoY", fmt_delta_ratio(d_yoy.get("sessions")), tone_cls_from_delta(d_yoy.get("sessions")), "#60a5fa"),
        metric_tile("Orders", fmt_int(daily.get("orders")), "WoW", fmt_delta_ratio(d_wow.get("orders")), tone_cls_from_delta(d_wow.get("orders")), "YoY", fmt_delta_ratio(d_yoy.get("orders")), tone_cls_from_delta(d_yoy.get("orders")), "#a78bfa"),
        metric_tile("Revenue", fmt_krw_symbol(daily.get("revenue")), "WoW", fmt_delta_ratio(d_wow.get("revenue")), tone_cls_from_delta(d_wow.get("revenue")), "YoY", fmt_delta_ratio(d_yoy.get("revenue")), tone_cls_from_delta(d_yoy.get("revenue")), "#22c55e"),
        metric_tile("CVR", fmt_cvr(daily.get("cvr")), "WoW", fmt_pp_from_fraction(d_wow.get("cvr_pp")), pp_tone_cls(d_wow.get("cvr_pp")), "YoY", fmt_pp_from_fraction(d_yoy.get("cvr_pp")), pp_tone_cls(d_yoy.get("cvr_pp")), "#f59e0b"),
        metric_tile("Sign-up Users", fmt_int(daily.get("signups")), "WoW", fmt_delta_ratio(d_wow.get("signups")), tone_cls_from_delta(d_wow.get("signups")), "YoY", fmt_delta_ratio(d_yoy.get("signups")), tone_cls_from_delta(d_yoy.get("signups")), "#14b8a6"),
    ])
    daily_strip = section_shell(
        "dailyKpi",
        "DAILY SNAPSHOT",
        "Daily KPI Summary",
        f'기준일 <b class="text-slate-900">{daily.get("date") or "-"}</b><br/>updated {daily.get("updated") or ""}',
        "#60a5fa",
        f'<div class="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-5 gap-4">{daily_tiles}</div>',
    )

    weekly_tiles = "".join([
        metric_tile("Sessions", fmt_int(weekly.get("sessions")), "WoW", fmt_delta_ratio(w_wow.get("sessions")), tone_cls_from_delta(w_wow.get("sessions")), "YoY", fmt_delta_ratio(w_yoy.get("sessions")), tone_cls_from_delta(w_yoy.get("sessions")), "#38bdf8"),
        metric_tile("Orders", fmt_int(weekly.get("orders")), "WoW", fmt_delta_ratio(w_wow.get("orders")), tone_cls_from_delta(w_wow.get("orders")), "YoY", fmt_delta_ratio(w_yoy.get("orders")), tone_cls_from_delta(w_yoy.get("orders")), "#8b5cf6"),
        metric_tile("Revenue", fmt_krw_symbol(weekly.get("revenue")), "WoW", fmt_delta_ratio(w_wow.get("revenue")), tone_cls_from_delta(w_wow.get("revenue")), "YoY", fmt_delta_ratio(w_yoy.get("revenue")), tone_cls_from_delta(w_yoy.get("revenue")), "#10b981"),
        metric_tile("CVR", fmt_cvr(weekly.get("cvr")), "WoW", fmt_pp_from_fraction(w_wow.get("cvr_pp")), pp_tone_cls(w_wow.get("cvr_pp")), "YoY", fmt_pp_from_fraction(w_yoy.get("cvr_pp")), pp_tone_cls(w_yoy.get("cvr_pp")), "#f97316"),
        metric_tile("Sign-up Users", fmt_int(weekly.get("signups")), "WoW", fmt_delta_ratio(w_wow.get("signups")), tone_cls_from_delta(w_wow.get("signups")), "YoY", fmt_delta_ratio(w_yoy.get("signups")), tone_cls_from_delta(w_yoy.get("signups")), "#06b6d4"),
    ])
    weekly_strip = section_shell(
        "weeklyKpi",
        "WEEKLY TREND",
        "Weekly KPI Summary (7D)",
        f'기간 <b class="text-slate-900">{weekly.get("start") or "-"} ~ {weekly.get("end") or "-"}</b><br/>updated {weekly.get("updated") or ""}',
        "#8b5cf6",
        f'<div class="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-5 gap-4">{weekly_tiles}</div>',
    )

    owned_block = section_shell(
        "ownedYtd",
        "OWNED PERFORMANCE",
        "OWNED YTD YoY (EDM + LMS + KAKAO)",
        f'상태 <b class="text-slate-900">{"ENABLED" if owned_ytd.get("enabled") else "DISABLED"}</b><br/>updated {owned_ytd.get("updated") or ""}',
        "#14b8a6",
        '<div class="empty-state rounded-[28px] p-6 text-sm text-slate-600">OWNED 데이터가 아직 없습니다.</div>',
    )

    if owned_ytd.get("enabled"):
        tot = owned_ytd.get("total") or {}
        tot_prev = owned_ytd.get("total_prev") or {}
        tot_yoy = owned_ytd.get("total_yoy") or {}
        period = owned_ytd.get("period") or "-"
        prev_period = owned_ytd.get("prev_period") or "-"
        upd = owned_ytd.get("updated") or ""
        owned_dir = owned_ytd.get("owned_dir") or ""
        source = owned_ytd.get("source") or ""

        owned_total_tiles = "".join([
            metric_tile("Send Count", fmt_int(tot.get("send_count")), "YoY", fmt_delta_ratio(tot_yoy.get("send_count")), tone_cls_from_delta(tot_yoy.get("send_count")), "LY", fmt_int(tot_prev.get("send_count")), "text-slate-700", "#2dd4bf"),
            metric_tile("Sessions", fmt_int(tot.get("sessions")), "YoY", fmt_delta_ratio(tot_yoy.get("sessions")), tone_cls_from_delta(tot_yoy.get("sessions")), "LY", fmt_int(tot_prev.get("sessions")), "text-slate-700", "#38bdf8"),
            metric_tile("Revenue", fmt_krw_symbol(tot.get("revenue")), "YoY", fmt_delta_ratio(tot_yoy.get("revenue")), tone_cls_from_delta(tot_yoy.get("revenue")), "LY", fmt_krw_symbol(tot_prev.get("revenue")), "text-slate-700", "#22c55e"),
            metric_tile("CVR", fmt_cvr(tot.get("cvr")), "YoY", fmt_pp_from_fraction(tot_yoy.get("cvr_pp")), pp_tone_cls(tot_yoy.get("cvr_pp")), "LY", fmt_cvr(tot_prev.get("cvr")), "text-slate-700", "#f59e0b"),
        ])

        by_ch = owned_ytd.get("by_channel") or {}
        channel_cards = []
        channel_accents = {"EDM": "#60a5fa", "LMS": "#a78bfa", "KAKAO": "#f59e0b"}
        for ch in OWNED_CHANNELS:
            cur = (by_ch.get(ch) or {}).get("cur") or {}
            prev = (by_ch.get(ch) or {}).get("prev") or {}
            yoy = (by_ch.get(ch) or {}).get("yoy") or {}
            accent = channel_accents.get(ch, "#94a3b8")
            channel_cards.append(f'''
            <article class="channel-card reveal rounded-[28px] p-5" style="--accent:{accent};">
              <div class="channel-top flex items-start justify-between gap-3">
                <div>
                  <div class="section-eyebrow">{ch}</div>
                  <h3 class="mt-2 text-xl font-black tracking-tight text-slate-950">{ch} YTD</h3>
                </div>
                <span class="channel-pill">LY / YoY</span>
              </div>
              <div class="mt-5 grid grid-cols-1 sm:grid-cols-2 gap-3">
                <div class="channel-metric"><div class="k">Send Count</div><div class="v metric-value" data-countup="{fmt_int(cur.get("send_count"))}">{fmt_int(cur.get("send_count"))}</div><div class="s">LY {fmt_int(prev.get("send_count"))} · <b class="{tone_cls_from_delta(yoy.get("send_count"))}">{fmt_delta_ratio(yoy.get("send_count"))}</b></div></div>
                <div class="channel-metric"><div class="k">Sessions</div><div class="v metric-value" data-countup="{fmt_int(cur.get("sessions"))}">{fmt_int(cur.get("sessions"))}</div><div class="s">LY {fmt_int(prev.get("sessions"))} · <b class="{tone_cls_from_delta(yoy.get("sessions"))}">{fmt_delta_ratio(yoy.get("sessions"))}</b></div></div>
                <div class="channel-metric"><div class="k">Revenue</div><div class="v metric-value" data-countup="{fmt_krw_symbol(cur.get("revenue"))}">{fmt_krw_symbol(cur.get("revenue"))}</div><div class="s">LY {fmt_krw_symbol(prev.get("revenue"))} · <b class="{tone_cls_from_delta(yoy.get("revenue"))}">{fmt_delta_ratio(yoy.get("revenue"))}</b></div></div>
                <div class="channel-metric"><div class="k">CVR</div><div class="v metric-value" data-countup="{fmt_cvr(cur.get("cvr"))}">{fmt_cvr(cur.get("cvr"))}</div><div class="s">LY {fmt_cvr(prev.get("cvr"))} · <b class="{pp_tone_cls(yoy.get("cvr_pp"))}">{fmt_pp_from_fraction(yoy.get("cvr_pp"))}</b></div></div>
              </div>
            </article>
            ''')

        owned_inner = f'''
        <div class="grid grid-cols-1 xl:grid-cols-[1.2fr_.8fr] gap-4 items-stretch">
          <div class="insight-panel rounded-[28px] p-5 sm:p-6 reveal">
            <div class="flex flex-col lg:flex-row lg:items-end lg:justify-between gap-4">
              <div>
                <div class="section-eyebrow">YTD TOTAL</div>
                <h3 class="mt-2 text-2xl font-black tracking-tight text-slate-950">누적 전체 요약</h3>
                <p class="mt-2 text-sm text-slate-600">당해연도 1월 1일부터 어제까지 누적 기준이며, 전년은 동일 MM-DD cutoff로 맞춰 비교합니다.</p>
              </div>
              <div class="text-xs text-slate-600 leading-6">
                기간 <b class="text-slate-900">{period}</b><br/>
                YoY 비교 <b class="text-slate-900">{prev_period}</b><br/>
                updated {upd}
              </div>
            </div>
            <div class="mt-5 grid grid-cols-1 sm:grid-cols-2 2xl:grid-cols-4 gap-4">{owned_total_tiles}</div>
          </div>
          <div class="logic-panel rounded-[28px] p-5 reveal">
            <div class="section-eyebrow">LOGIC TRACE</div>
            <h3 class="mt-2 text-xl font-black tracking-tight text-slate-950">Owned YTD 계산 기준</h3>
            <div class="mt-4 space-y-3 text-sm text-slate-600">
              <div class="logic-row"><span>send_count</span><b>builder 값 우선</b></div>
              <div class="logic-row"><span>fallback</span><b>동일 grouping 규칙</b></div>
              <div class="logic-row"><span>KAKAO</span><b>KAKAO_CH_EVENT + KAKAO_CH_MESSAGE_*</b></div>
              <div class="logic-row"><span>source</span><b class="truncate max-w-[18rem] inline-block align-bottom">{source or owned_dir}</b></div>
            </div>
          </div>
        </div>
        <div class="mt-4 grid grid-cols-1 xl:grid-cols-3 gap-4">{''.join(channel_cards)}</div>
        <div class="mt-3 px-1 text-[11px] text-slate-500">owned_dir: {owned_dir}</div>
        '''
        owned_block = section_shell(
            "ownedYtd",
            "OWNED PERFORMANCE",
            "OWNED YTD YoY (EDM + LMS + KAKAO)",
            "채널별 카드 톤을 분리해서 EDM/LMS/KAKAO가 한눈에 구분되도록 구성하고, 총합/채널/로직 설명 블록을 계층화했습니다.",
            f'기간 <b class="text-slate-900">{period}</b><br/>updated {upd}',
            "#14b8a6",
            owned_inner,
        )

    return f'''<!doctype html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>CSK E-COMM | 오늘의 핵심 요약</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200;400;600;700;800&display=swap');
    :root {{
      --brand:#002d72;
      --bg-a:#f6f8fb;
      --bg-b:#eef4ff;
      --ink:#0f172a;
    }}
    * {{ box-sizing:border-box; }}
    html {{ scroll-behavior:smooth; }}
    body {{
      margin:0;
      min-height:100vh;
      color:var(--ink);
      font-family:'Plus Jakarta Sans',sans-serif;
      background:
        radial-gradient(circle at top left, rgba(96,165,250,.22), transparent 32%),
        radial-gradient(circle at top right, rgba(45,212,191,.18), transparent 28%),
        linear-gradient(180deg, var(--bg-b) 0%, #f8fafc 34%, #eef2ff 100%);
    }}
    .hero-shell {{
      position:relative;
      overflow:hidden;
      background:linear-gradient(135deg, rgba(255,255,255,.88), rgba(255,255,255,.64));
      border:1px solid rgba(255,255,255,.78);
      backdrop-filter: blur(20px);
      box-shadow:0 28px 70px rgba(15,23,42,.10);
    }}
    .hero-shell::before {{
      content:"";
      position:absolute;
      inset:-20% auto auto -10%;
      width:18rem;
      height:18rem;
      background:radial-gradient(circle, rgba(96,165,250,.28), transparent 65%);
      filter:blur(10px);
      pointer-events:none;
    }}
    .hero-shell::after {{
      content:"";
      position:absolute;
      right:-3rem;
      top:-4rem;
      width:16rem;
      height:16rem;
      background:radial-gradient(circle, rgba(20,184,166,.22), transparent 66%);
      filter:blur(6px);
      pointer-events:none;
    }}
    .glass-card, .summary-section > div, .metric-card, .channel-card, .logic-panel, .insight-panel, .empty-state {{
      background:linear-gradient(180deg, rgba(255,255,255,.82), rgba(255,255,255,.68));
      backdrop-filter:blur(20px);
      border:1px solid rgba(255,255,255,.72);
      box-shadow:0 18px 48px rgba(15,23,42,.08);
    }}
    .section-border {{
      background:linear-gradient(135deg, color-mix(in srgb, var(--section-accent) 45%, white) 0%, rgba(255,255,255,.78) 28%, rgba(255,255,255,.28) 100%);
      opacity:.95;
    }}
    .section-eyebrow {{
      display:inline-flex;
      align-items:center;
      gap:.45rem;
      padding:.45rem .8rem;
      border-radius:999px;
      font-size:11px;
      font-weight:900;
      letter-spacing:.22em;
      text-transform:uppercase;
      color:#0f172a;
      background:rgba(255,255,255,.72);
      border:1px solid rgba(255,255,255,.8);
      box-shadow:0 10px 26px rgba(15,23,42,.06);
    }}
    .section-meta {{
      padding:1rem 1.1rem;
      border-radius:24px;
      background:rgba(255,255,255,.60);
      border:1px solid rgba(255,255,255,.72);
      box-shadow: inset 0 1px 0 rgba(255,255,255,.75);
      min-width:min(100%, 20rem);
    }}
    .metric-card {{
      position:relative;
      overflow:hidden;
      transform:translateY(0) scale(1);
      transition:transform .45s cubic-bezier(.2,.8,.2,1), box-shadow .45s ease, border-color .45s ease;
      border:1px solid rgba(255,255,255,.75);
    }}
    .metric-card:hover, .channel-card:hover, .logic-panel:hover, .insight-panel:hover {{
      transform:translateY(-6px) scale(1.01);
      box-shadow:0 28px 64px rgba(15,23,42,.12);
    }}
    .metric-card::before, .channel-card::before {{
      content:"";
      position:absolute;
      inset:0 auto 0 0;
      width:4px;
      background:linear-gradient(180deg, var(--accent, #94a3b8), transparent 85%);
    }}
    .metric-glow {{
      position:absolute;
      inset:auto -12% -45% auto;
      width:8rem;
      height:8rem;
      background:radial-gradient(circle, color-mix(in srgb, var(--accent, #94a3b8) 30%, white), transparent 68%);
      pointer-events:none;
      filter:blur(4px);
      opacity:.85;
    }}
    .channel-card {{ position:relative; overflow:hidden; }}
    .channel-pill {{
      display:inline-flex;
      align-items:center;
      padding:.42rem .78rem;
      border-radius:999px;
      font-size:11px;
      font-weight:800;
      color:#334155;
      background:rgba(255,255,255,.74);
      border:1px solid rgba(255,255,255,.78);
    }}
    .channel-metric {{
      padding:1rem;
      border-radius:22px;
      background:rgba(255,255,255,.74);
      border:1px solid rgba(255,255,255,.78);
      box-shadow: inset 0 1px 0 rgba(255,255,255,.85);
    }}
    .channel-metric .k {{ font-size:11px; font-weight:800; letter-spacing:.18em; text-transform:uppercase; color:#64748b; }}
    .channel-metric .v {{ margin-top:.5rem; font-size:1.45rem; font-weight:900; line-height:1.1; color:#020617; }}
    .channel-metric .s {{ margin-top:.45rem; font-size:11px; color:#64748b; }}
    .logic-row {{
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:1rem;
      padding:.9rem 1rem;
      border-radius:20px;
      background:rgba(255,255,255,.72);
      border:1px solid rgba(255,255,255,.78);
    }}
    .logic-row span {{ color:#64748b; font-size:12px; text-transform:uppercase; letter-spacing:.16em; font-weight:800; }}
    .logic-row b {{ color:#0f172a; font-size:12px; font-weight:900; }}
    .nav-chip {{
      display:inline-flex;
      align-items:center;
      gap:.45rem;
      padding:.7rem 1rem;
      border-radius:999px;
      background:rgba(255,255,255,.7);
      border:1px solid rgba(255,255,255,.8);
      font-size:12px;
      font-weight:800;
      color:#334155;
      text-decoration:none;
      transition:transform .25s ease, background .25s ease, box-shadow .25s ease;
      box-shadow:0 10px 24px rgba(15,23,42,.06);
    }}
    .nav-chip:hover {{ transform:translateY(-2px); background:rgba(255,255,255,.92); }}
    .reveal {{ opacity:0; transform:translateY(24px) scale(.985); animation:riseIn .9s cubic-bezier(.2,.8,.2,1) forwards; }}
    .summary-section:nth-of-type(1) {{ animation-delay:.08s; }}
    .summary-section:nth-of-type(2) {{ animation-delay:.18s; }}
    .summary-section:nth-of-type(3) {{ animation-delay:.28s; }}
    .metric-card:nth-child(1) {{ animation-delay:.12s; }}
    .metric-card:nth-child(2) {{ animation-delay:.18s; }}
    .metric-card:nth-child(3) {{ animation-delay:.24s; }}
    .metric-card:nth-child(4) {{ animation-delay:.30s; }}
    .metric-card:nth-child(5) {{ animation-delay:.36s; }}
    .channel-card:nth-child(1) {{ animation-delay:.14s; }}
    .channel-card:nth-child(2) {{ animation-delay:.22s; }}
    .channel-card:nth-child(3) {{ animation-delay:.30s; }}
    @keyframes riseIn {{
      0% {{ opacity:0; transform:translateY(26px) scale(.982); }}
      60% {{ opacity:1; transform:translateY(-4px) scale(1.006); }}
      100% {{ opacity:1; transform:translateY(0) scale(1); }}
    }}
    @keyframes shimmer {{
      0% {{ transform:translateX(-120%) skewX(-14deg); opacity:0; }}
      30% {{ opacity:.65; }}
      100% {{ transform:translateX(220%) skewX(-14deg); opacity:0; }}
    }}
    .hero-shimmer {{
      position:absolute;
      inset:0 auto 0 -30%;
      width:30%;
      background:linear-gradient(90deg, transparent, rgba(255,255,255,.45), transparent);
      animation:shimmer 4.8s ease-in-out infinite;
      pointer-events:none;
    }}
    @media (max-width: 640px) {{
      .section-meta {{ min-width:0; width:100%; }}
    }}
  </style>
</head>
<body>
  <div class="mx-auto max-w-[1600px] px-3 sm:px-6 lg:px-8 py-5 sm:py-8">
    <header class="hero-shell reveal rounded-[36px] px-5 py-6 sm:px-7 sm:py-8 mb-7">
      <div class="hero-shimmer"></div>
      <div class="relative z-10 flex flex-col xl:flex-row xl:items-end xl:justify-between gap-5">
        <div>
          <div class="section-eyebrow">CSK E-COMM SUMMARY</div>
          <h1 class="mt-3 text-3xl sm:text-5xl font-black tracking-[-0.04em] text-slate-950">오늘의 핵심 요약</h1>
        </div>
        <div class="section-meta">
          generated <b class="text-slate-900">{now_kst_label()}</b><br/>
          기준일 기본값 <b class="text-slate-900">어제 (KST)</b>
        </div>
      </div>
      <div class="relative z-10 mt-5 flex flex-wrap gap-2">
        <a href="#dailyKpi" class="nav-chip">Daily KPI</a>
        <a href="#weeklyKpi" class="nav-chip">Weekly KPI</a>
        <a href="#ownedYtd" class="nav-chip">Owned YTD YoY</a>
      </div>
    </header>
    {daily_strip}
    {weekly_strip}
    {owned_block}
  </div>
  <script>
    (() => {{
      const prefersReduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
      const easeOutExpo = (t) => t === 1 ? 1 : 1 - Math.pow(2, -10 * t);
      const parseDisplayNumber = (text) => {{
        const raw = (text || '').trim();
        if (!raw || raw === '-') return null;
        const isCurrency = raw.includes('₩');
        const isPp = raw.includes('%p');
        const isPercent = !isPp && raw.includes('%');
        const num = Number(raw.replace(/[^0-9.-]/g, ''));
        if (Number.isNaN(num)) return null;
        return {{ raw, num, isCurrency, isPercent, isPp }};
      }};
      const formatDisplayNumber = (meta, value) => {{
        if (!meta) return '';
        if (meta.isPp) return `${{value.toFixed(2)}}%p`;
        if (meta.isPercent) return `${{value.toFixed(2)}}%`;
        if (meta.isCurrency) return `₩${{Math.round(value).toLocaleString('en-US')}}`;
        return `${{Math.round(value).toLocaleString('en-US')}}`;
      }};
      const animateValue = (el) => {{
        const meta = parseDisplayNumber(el.dataset.countup || el.textContent);
        if (!meta) return;
        const duration = meta.isCurrency ? 1500 : 1250;
        const start = performance.now();
        const tick = (now) => {{
          const p = Math.min(1, (now - start) / duration);
          const eased = easeOutExpo(p);
          const cur = meta.num * eased;
          el.textContent = formatDisplayNumber(meta, cur);
          if (p < 1) requestAnimationFrame(tick);
          else el.textContent = meta.raw;
        }};
        requestAnimationFrame(tick);
      }};
      if (!prefersReduced) {{
        const valueObserver = new IntersectionObserver((entries, obs) => {{
          entries.forEach((entry) => {{
            if (!entry.isIntersecting) return;
            animateValue(entry.target);
            obs.unobserve(entry.target);
          }});
        }}, {{ threshold: 0.45 }});
        document.querySelectorAll('.metric-value').forEach((el) => valueObserver.observe(el));
      }}
    }})();
  </script>
</body>
</html>'''


def main() -> None:
    repo_root = Path(".").resolve()
    reports_dir = repo_root / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    daily = build_daily_kpis(reports_dir)
    weekly = build_weekly_kpis(reports_dir)
    owned_ytd = build_owned_ytd_yoy(reports_dir)
    out = reports_dir / "index.html"
    out.write_text(render_index_html(daily, weekly, owned_ytd), encoding="utf-8")
    print(f"[OK] Wrote: {out}")


if __name__ == "__main__":
    main()
