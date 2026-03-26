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


def _owned_has_group_mmdd(row: Dict[str, Any]) -> bool:
    if not bool(row.get("has_group_mmdd", False)):
        return False
    year_s = str(row.get("year") or "").strip()
    mmdd_s = str(row.get("mmdd") or "").strip()
    return bool(re.fullmatch(r"\d{4}", year_s) and re.fullmatch(r"\d{4}", mmdd_s))


def _owned_is_countable_send_row(row: Dict[str, Any]) -> bool:
    ch = str(row.get("channel") or "").strip().upper()
    if ch != "KAKAO":
        return True
    campaign = str(row.get("campaign") or "").strip().upper()
    term = str(row.get("term") or "").strip().upper()
    return campaign.startswith("KAKAO_CH_EVENT") and term.startswith("KAKAO_CH_MESSAGE_")


def _owned_valid_send_row(row: Dict[str, Any]) -> bool:
    ch = str(row.get("channel") or "").strip().upper()
    if ch not in OWNED_CHANNELS:
        return False
    return _owned_has_group_mmdd(row) and _owned_is_countable_send_row(row)


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
    return (ch, yr, mmdd)


def _owned_send_identity(row: Dict[str, Any]) -> Optional[Tuple[str, str, str, str]]:
    """
    Match build_owned_campaign_portal logic more faithfully.
    Preferred unique send unit: (date, channel, send_id).
    Fallback only when send_id is missing: (date, channel, year, mmdd/campaign-bucket).
    """
    ch = str(row.get("channel") or "").strip().upper()
    if ch not in OWNED_CHANNELS or not _owned_valid_send_row(row):
        return None

    row_dt = _parse_row_date(row)
    dt = row_dt.isoformat() if row_dt else str(row.get("date") or "").strip()
    send_id = str(row.get("send_id") or "").strip()
    if dt and send_id:
        return (dt, ch, "SEND_ID", send_id)

    yr, bucket = _owned_send_group_key(row)[1:]
    if not yr and row_dt:
        yr = str(row_dt.year)
    if not bucket and row_dt:
        bucket = row_dt.strftime("%m%d")
    if not dt or not yr or not bucket:
        return None
    return (dt, ch, yr, bucket)


def _aggregate_owned_rows(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    out = {"sessions": 0.0, "users": 0.0, "purchases": 0.0, "revenue": 0.0, "send_count": 0.0}
    send_groups = set()

    for r in rows:
        ch = str(r.get("channel") or "").strip().upper()
        if ch not in OWNED_CHANNELS:
            continue
        if not _owned_has_group_mmdd(r):
            continue

        out["sessions"] += float(_safe_float(r.get("sessions")) or 0.0)
        out["users"] += float(_safe_float(r.get("users")) or 0.0)
        out["purchases"] += float(_safe_float(r.get("purchases")) or 0.0)
        out["revenue"] += float(_safe_float(r.get("revenue")) or 0.0)

        if _owned_valid_send_row(r):
            ident = _owned_send_identity(r)
            if ident:
                send_groups.add(ident)

    out["send_count"] = float(len(send_groups))
    return out


def _aggregate_owned_rows_by_channel(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    out = {ch: {"sessions": 0.0, "users": 0.0, "purchases": 0.0, "revenue": 0.0, "send_count": 0.0} for ch in OWNED_CHANNELS}
    send_groups = {ch: set() for ch in OWNED_CHANNELS}

    for r in rows:
        ch = str(r.get("channel") or "").strip().upper()
        if ch not in OWNED_CHANNELS:
            continue
        if not _owned_has_group_mmdd(r):
            continue

        out[ch]["sessions"] += float(_safe_float(r.get("sessions")) or 0.0)
        out[ch]["users"] += float(_safe_float(r.get("users")) or 0.0)
        out[ch]["purchases"] += float(_safe_float(r.get("purchases")) or 0.0)
        out[ch]["revenue"] += float(_safe_float(r.get("revenue")) or 0.0)

        if _owned_valid_send_row(r):
            ident = _owned_send_identity(r)
            if ident:
                send_groups[ch].add(ident)

    for ch in OWNED_CHANNELS:
        out[ch]["send_count"] = float(len(send_groups[ch]))
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

    cur_rows = (ytd.get("current") or {}).get("campaigns") or []
    prev_rows = (ytd.get("previous") or {}).get("campaigns") or []

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
        "cvr_pp": _pp(cur_cvr, prev_cvr),
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
        "total": {**cur_sum, "cvr": cur_cvr},
        "total_prev": {**prev_sum, "cvr": prev_cvr},
        "total_yoy": total_yoy,
        "by_channel": by_channel,
        "updated": now_kst_label(),
        "source": "ytd_yoy_recomputed_from_campaign_rows",
    }


def _owned_bundle_paths(owned_dir: Path) -> List[Path]:
    files = [p for p in owned_dir.glob("owned_*.json") if p.is_file()]
    files.sort(key=lambda p: p.name)
    return files


def _dedupe_dict_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        try:
            key = json.dumps(row, ensure_ascii=False, sort_keys=True, default=str)
        except Exception:
            key = str(row)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _normalize_message_log_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    mp: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for row in rows or []:
        dt = str((row or {}).get("date") or "").strip()
        ch = str((row or {}).get("channel") or "").strip().upper()
        title = str((row or {}).get("message_title") or (row or {}).get("title") or "").strip()
        body = str((row or {}).get("message_body") or (row or {}).get("content") or "").strip()
        if not dt or ch not in OWNED_CHANNELS or not title:
            continue
        key = (dt, ch, title)
        if key not in mp:
            mp[key] = {"date": dt, "channel": ch, "message_title": title, "message_body": body}
    return sorted(mp.values(), key=lambda r: (str(r.get("date") or ""), str(r.get("channel") or ""), str(r.get("message_title") or "")))


def _message_rows_for_year(message_rows: List[Dict[str, Any]], channel: str, year: int, end_date: date) -> List[Dict[str, Any]]:
    prefix = f"{year}-"
    end_s = ymd(end_date)
    out: List[Dict[str, Any]] = []
    for row in _normalize_message_log_rows(message_rows):
        if str(row.get("channel") or "").upper() != channel:
            continue
        ds = str(row.get("date") or "")
        if not ds.startswith(prefix):
            continue
        if ds <= end_s:
            out.append(row)
    return out


def _row_group_date(row: Dict[str, Any]) -> str:
    year_s = str(row.get("year") or "").strip()
    mmdd_s = str(row.get("mmdd") or "").strip()
    if re.fullmatch(r"\d{4}", year_s) and re.fullmatch(r"\d{4}", mmdd_s):
        return f"{year_s}-{mmdd_s[:2]}-{mmdd_s[2:]}"
    rd = _parse_row_date(row)
    return ymd(rd) if rd else ""


def _filter_rows_to_scheduled_dates(rows: List[Dict[str, Any]], scheduled_dates: set[str]) -> List[Dict[str, Any]]:
    if not scheduled_dates:
        return list(rows or [])
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        gd = _row_group_date(row)
        if gd and gd in scheduled_dates:
            out.append(row)
    return out


def _summarize_rows_no_send(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    out = {"sessions": 0.0, "users": 0.0, "purchases": 0.0, "revenue": 0.0, "send_count": 0.0}
    for r in rows or []:
        ch = str(r.get("channel") or "").strip().upper()
        if ch not in OWNED_CHANNELS or not _owned_has_group_mmdd(r):
            continue
        out["sessions"] += float(_safe_float(r.get("sessions")) or 0.0)
        out["users"] += float(_safe_float(r.get("users")) or 0.0)
        out["purchases"] += float(_safe_float(r.get("purchases")) or 0.0)
        out["revenue"] += float(_safe_float(r.get("revenue")) or 0.0)
    return out


def _fallback_distinct_send_count(rows: List[Dict[str, Any]]) -> int:
    send_groups = set()
    for r in rows or []:
        if _owned_valid_send_row(r):
            ident = _owned_send_identity(r)
            if ident:
                send_groups.add(ident)
    return len(send_groups)


def _mmdd_on_or_before(row: Dict[str, Any], end_date: date) -> bool:
    mmdd = str(row.get("mmdd") or "").strip()
    if re.fullmatch(r"\d{4}", mmdd):
        return mmdd <= end_date.strftime("%m%d")
    rd = _parse_row_date(row)
    return bool(rd and rd <= end_date)


def _portal_send_count_from_rows(rows: List[Dict[str, Any]], channel: str, end_date: date) -> int:
    total = 0
    for r in (rows or []):
        ch = str(r.get("channel") or "").strip().upper()
        if ch != channel:
            continue
        if not _owned_has_group_mmdd(r):
            continue
        if not _mmdd_on_or_before(r, end_date):
            continue
        total += int(_safe_float(r.get("send_count")) or 0)
    return total


def _channel_summary_from_rows(rows: List[Dict[str, Any]], message_rows: List[Dict[str, Any]], channel: str, end_date: date) -> Dict[str, float]:
    channel_rows = [
        r for r in (rows or [])
        if str(r.get("channel") or "").strip().upper() == channel and _owned_has_group_mmdd(r) and _mmdd_on_or_before(r, end_date)
    ]
    scheduled_rows = _message_rows_for_year(message_rows, channel, end_date.year, end_date)
    scheduled_dates = {str(m.get("date") or "") for m in scheduled_rows}
    metric_rows = _filter_rows_to_scheduled_dates(channel_rows, scheduled_dates) if scheduled_rows else channel_rows
    agg = _summarize_rows_no_send(metric_rows)
    portal_send_count = _portal_send_count_from_rows(channel_rows, channel, end_date)
    send_count = portal_send_count if portal_send_count > 0 else (len(scheduled_rows) if scheduled_rows else _fallback_distinct_send_count(channel_rows))
    agg["send_count"] = float(send_count)
    agg["avg_leverage"] = (agg["sessions"] / send_count) if send_count else 0.0
    return agg


def _all_channels_summary(rows: List[Dict[str, Any]], message_rows: List[Dict[str, Any]], end_date: date) -> Tuple[Dict[str, float], Dict[str, Dict[str, float]]]:
    by_channel: Dict[str, Dict[str, float]] = {}
    total = {"sessions": 0.0, "users": 0.0, "purchases": 0.0, "revenue": 0.0, "send_count": 0.0, "avg_leverage": 0.0}
    for ch in OWNED_CHANNELS:
        sm = _channel_summary_from_rows(rows, message_rows, ch, end_date)
        by_channel[ch] = sm
        total["sessions"] += sm["sessions"]
        total["users"] += sm["users"]
        total["purchases"] += sm["purchases"]
        total["revenue"] += sm["revenue"]
        total["send_count"] += sm["send_count"]
    total["avg_leverage"] = (total["sessions"] / total["send_count"]) if total["send_count"] else 0.0
    return total, by_channel


def _load_owned_ytd_inputs(owned_dir: Path, target_end: date) -> Dict[str, Any]:
    cur_year = target_end.year
    prev_year = target_end.year - 1
    cutoff_mmdd = target_end.strftime("%m-%d")
    cur_rows: List[Dict[str, Any]] = []
    prev_rows: List[Dict[str, Any]] = []
    cur_msg: List[Dict[str, Any]] = []
    prev_msg: List[Dict[str, Any]] = []

    for p in _owned_bundle_paths(owned_dir):
        obj = _load_bundle(p)
        if not obj:
            continue
        for r in (obj.get("campaigns") or []):
            ch = str(r.get("channel") or "").strip().upper()
            if ch not in OWNED_CHANNELS or not _owned_has_group_mmdd(r):
                continue
            row_dt = _parse_row_date(r)
            if not row_dt:
                continue
            row_year = str(r.get("year") or "").strip() or str(row_dt.year)
            if row_year == str(cur_year) and row_dt.year == cur_year and row_dt <= target_end:
                cur_rows.append(r)
            elif row_year == str(prev_year) and row_dt.year == prev_year and row_dt.strftime("%m-%d") <= cutoff_mmdd:
                prev_rows.append(r)

        for m in (obj.get("message_log") or []):
            ds = str((m or {}).get("date") or "").strip()
            ch = str((m or {}).get("channel") or "").strip().upper()
            if ch not in OWNED_CHANNELS or not ds:
                continue
            md = parse_ymd(ds)
            if not md:
                continue
            if md.year == cur_year and md <= target_end:
                cur_msg.append({**m, "channel": ch})
            elif md.year == prev_year and md.strftime("%m-%d") <= cutoff_mmdd:
                prev_msg.append({**m, "channel": ch})

    return {
        "cur_rows": _dedupe_dict_rows(cur_rows),
        "prev_rows": _dedupe_dict_rows(prev_rows),
        "cur_msg": _normalize_message_log_rows(cur_msg),
        "prev_msg": _normalize_message_log_rows(prev_msg),
    }


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

    loaded = _load_owned_ytd_inputs(owned_dir, target_end)
    cur_total, cur_by_ch = _all_channels_summary(loaded["cur_rows"], loaded["cur_msg"], target_end)
    prev_end = date(target_end.year - 1, target_end.month, target_end.day)
    prev_total, prev_by_ch = _all_channels_summary(loaded["prev_rows"], loaded["prev_msg"], prev_end)

    cur_cvr = (cur_total["purchases"] / cur_total["sessions"]) if cur_total["sessions"] > 0 else None
    prev_cvr = (prev_total["purchases"] / prev_total["sessions"]) if prev_total["sessions"] > 0 else None

    total_yoy = {
        "send_count": _ratio(cur_total["send_count"], prev_total["send_count"]),
        "sessions": _ratio(cur_total["sessions"], prev_total["sessions"]),
        "users": _ratio(cur_total["users"], prev_total["users"]),
        "purchases": _ratio(cur_total["purchases"], prev_total["purchases"]),
        "revenue": _ratio(cur_total["revenue"], prev_total["revenue"]),
        "cvr_pp": (cur_cvr - prev_cvr) if (cur_cvr is not None and prev_cvr is not None) else None,
    }

    total = dict(cur_total); total["cvr"] = cur_cvr
    total_prev = dict(prev_total); total_prev["cvr"] = prev_cvr

    by_channel = {}
    for ch in OWNED_CHANNELS:
        curv = cur_by_ch.get(ch) or {"sessions": 0.0, "users": 0.0, "purchases": 0.0, "revenue": 0.0, "send_count": 0.0, "avg_leverage": 0.0}
        prevv = prev_by_ch.get(ch) or {"sessions": 0.0, "users": 0.0, "purchases": 0.0, "revenue": 0.0, "send_count": 0.0, "avg_leverage": 0.0}
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
        "source": str(owned_dir),
    })
    return result


def build_daily_trend_series(reports_dir: Path, limit: int = 800) -> List[Dict[str, Any]]:

    files = list((reports_dir / "daily_digest" / "data" / "daily").glob("*.json"))
    files = [p for p in files if re.match(r"^\d{4}-\d{2}-\d{2}\.json$", p.name)]
    files.sort(key=lambda p: p.name)
    out: List[Dict[str, Any]] = []
    for p in files[-limit:]:
        bundle = _load_bundle(p)
        if not bundle:
            continue
        k = _extract_kpis_from_bundle(bundle)
        out.append({
            "date": p.stem,
            "sessions": _safe_float(k.get("sessions")) or 0.0,
            "orders": _safe_float(k.get("orders")) or 0.0,
            "revenue": _safe_float(k.get("revenue")) or 0.0,
            "cvr": _safe_float(k.get("cvr")) or 0.0,
            "signups": _safe_float(k.get("signups")) or 0.0,
        })
    return out


def build_weekly_trend_series(reports_dir: Path, limit: int = 200) -> List[Dict[str, Any]]:
    daily_series = build_daily_trend_series(reports_dir, limit=1200)
    if not daily_series:
        return []

    buckets: Dict[Tuple[int, int], Dict[str, Any]] = {}
    for row in daily_series:
        dt = parse_ymd(str(row.get("date") or ""))
        if not dt:
            continue
        week_start = dt - timedelta(days=dt.weekday())
        week_end = week_start + timedelta(days=6)
        iso_year, iso_week, _ = week_start.isocalendar()
        key = (iso_year, iso_week)
        slot = buckets.setdefault(key, {
            "date": ymd(week_end),
            "week_start": ymd(week_start),
            "week_end": ymd(week_end),
            "label": f"W{iso_week}",
            "full_label": f"W{iso_week} · {ymd(week_start)} ~ {ymd(week_end)}",
            "sessions": 0.0,
            "orders": 0.0,
            "revenue": 0.0,
            "signups": 0.0,
        })
        slot["sessions"] += float(_safe_float(row.get("sessions")) or 0.0)
        slot["orders"] += float(_safe_float(row.get("orders")) or 0.0)
        slot["revenue"] += float(_safe_float(row.get("revenue")) or 0.0)
        slot["signups"] += float(_safe_float(row.get("signups")) or 0.0)

    out: List[Dict[str, Any]] = []
    for _, item in sorted(buckets.items(), key=lambda kv: kv[1]["week_start"]):
        item["cvr"] = (item["orders"] / item["sessions"]) if item["sessions"] > 0 else 0.0
        out.append(item)
    return out[-limit:]


def build_owned_trend_series(reports_dir: Path) -> Dict[str, Any]:
    owned_dir = _find_owned_data_dir(reports_dir)
    result = {"total": [], "by_channel": {ch: [] for ch in OWNED_CHANNELS}}
    if not owned_dir:
        return result

    totals: Dict[str, Dict[str, float]] = {}
    by_channel: Dict[str, Dict[str, Dict[str, float]]] = {ch: {} for ch in OWNED_CHANNELS}
    send_seen_total: Dict[str, set] = {}
    send_seen_channel: Dict[str, Dict[str, set]] = {ch: {} for ch in OWNED_CHANNELS}

    for p in _owned_bundle_paths(owned_dir):
        obj = _load_bundle(p)
        if not obj:
            continue
        for r in (obj.get("campaigns") or []):
            ch = str(r.get("channel") or "").strip().upper()
            if ch not in OWNED_CHANNELS:
                continue
            if not _owned_has_group_mmdd(r):
                continue
            row_dt = _parse_row_date(r)
            dt = row_dt.isoformat() if row_dt else str(r.get("date") or "")
            if not dt:
                continue
            slot = totals.setdefault(dt, {"date": dt, "send_count": 0.0, "sessions": 0.0, "revenue": 0.0, "purchases": 0.0})
            ch_slot = by_channel[ch].setdefault(dt, {"date": dt, "send_count": 0.0, "sessions": 0.0, "revenue": 0.0, "purchases": 0.0})
            sessions = float(_safe_float(r.get("sessions")) or 0.0)
            revenue = float(_safe_float(r.get("revenue")) or 0.0)
            purchases = float(_safe_float(r.get("purchases")) or 0.0)
            slot["sessions"] += sessions
            slot["revenue"] += revenue
            slot["purchases"] += purchases
            ch_slot["sessions"] += sessions
            ch_slot["revenue"] += revenue
            ch_slot["purchases"] += purchases
            ident = _owned_send_identity(r)
            if ident:
                total_seen = send_seen_total.setdefault(dt, set())
                if ident not in total_seen:
                    total_seen.add(ident)
                    slot["send_count"] += 1.0
                ch_seen = send_seen_channel[ch].setdefault(dt, set())
                if ident not in ch_seen:
                    ch_seen.add(ident)
                    ch_slot["send_count"] += 1.0

    def finalize(mapping: Dict[str, Dict[str, float]]) -> List[Dict[str, Any]]:
        items = [mapping[k] for k in sorted(mapping.keys())]
        for x in items:
            x["cvr"] = (x["purchases"] / x["sessions"]) if x["sessions"] > 0 else 0.0
            x.pop("purchases", None)
        return items

    result["total"] = finalize(totals)
    result["by_channel"] = {ch: finalize(by_channel[ch]) for ch in OWNED_CHANNELS}
    return result


def render_index_html(daily: Dict[str, Any], weekly: Dict[str, Any], owned_ytd: Dict[str, Any], daily_trend: List[Dict[str, Any]], weekly_trend: List[Dict[str, Any]], owned_trend: Dict[str, Any]) -> str:
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

    def metric_tile(title: str, value: str, sub_a_label: str, sub_a_value: str, sub_a_cls: str, sub_b_label: str, sub_b_value: str, sub_b_cls: str, accent: str = "", click_section: str = "", click_metric: str = "", click_label: str = "") -> str:
        attrs = []
        if accent:
            attrs.append(f"--accent:{accent};")
        if click_section and click_metric:
            attrs.append("cursor:pointer;")
        style_attr = f"style=\"{' '.join(attrs)}\"" if attrs else ""
        data_attr = f'data-chart-section="{click_section}" data-chart-metric="{click_metric}" data-chart-label="{click_label or title}"' if click_section and click_metric else ""
        hint = '<span class="chart-hint">CLICK</span>' if click_section and click_metric else ''
        return f'''
        <article class="metric-card reveal rounded-[28px] p-4 sm:p-5" {style_attr} {data_attr}>
          <div class="relative z-10">
            <div class="flex items-center justify-between gap-3">
              <div class="text-[11px] font-extrabold tracking-[0.22em] text-slate-500 uppercase">{title}</div>
              {hint}
            </div>
            <div class="mt-3 flex items-end justify-between gap-3">
              <div class="metric-value text-[clamp(1.45rem,2vw,2rem)] font-black leading-[1.02] text-slate-950 break-all" data-countup="{value}">{value}</div>
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

    def trend_panel(section_key: str, title: str, subtitle: str) -> str:
        range_tabs = [
            ('1MONTH', '1MONTH'),
            ('1YEAR', '1YEAR'),
            ('2YEAR', '2YEAR'),
        ] if section_key == 'weekly' else [
            ('1MONTH', '1MONTH'),
            ('7D', '7D'),
            ('14D', '14D'),
            ('1YEAR', '1YEAR'),
            ('2YEAR', '2YEAR'),
        ]
        range_btns = ''.join([
            f'<button type="button" class="range-tab{' active' if i == 0 else ''}" data-range="{key}">{label}</button>'
            for i, (key, label) in enumerate(range_tabs)
        ])
        return f'''
        <div class="trend-panel reveal mt-4 rounded-[28px] p-5 sm:p-6" data-trend-section="{section_key}">
          <div class="flex flex-col lg:flex-row lg:items-end lg:justify-between gap-4 mb-4">
            <div>
              <div class="section-eyebrow">CLICK TO EXPLORE</div>
              <h3 class="mt-2 text-xl sm:text-2xl font-black tracking-tight text-slate-950">{title}</h3>
              <p class="mt-2 text-sm text-slate-600">{subtitle}</p>
            </div>
            <div class="trend-legend text-xs text-slate-500">카드를 누르면 해당 KPI 그래프로 전환됩니다.</div>
          </div>
          <div class="trend-head flex flex-col gap-3 mb-3">
            <div class="flex flex-col md:flex-row md:items-center md:justify-between gap-3">
              <div>
                <div class="text-[11px] font-extrabold tracking-[0.18em] text-slate-500 uppercase">Selected KPI</div>
                <div class="trend-selected text-lg font-black text-slate-950" data-selected-title>Revenue</div>
              </div>
              <div class="trend-summary text-sm text-slate-600" data-selected-summary></div>
            </div>
            <div class="range-tabs flex flex-wrap gap-2" data-range-tabs>
              {range_btns}
            </div>
          </div>
          <div class="trend-svg-shell rounded-[24px] p-3 sm:p-4 relative">
            <div class="trend-scroll" data-trend-scroll>
              <svg class="trend-svg h-[360px] sm:h-[420px]" viewBox="0 0 1200 420" preserveAspectRatio="none" data-trend-svg></svg>
            </div>
            <div class="chart-tooltip" data-chart-tooltip hidden></div>
          </div>
        </div>
        '''


    d_wow = daily.get("wow") or {}
    d_yoy = daily.get("yoy") or {}
    w_wow = weekly.get("wow") or {}
    w_yoy = weekly.get("yoy") or {}

    daily_tiles = "".join([
        metric_tile("Sessions", fmt_int(daily.get("sessions")), "WoW", fmt_delta_ratio(d_wow.get("sessions")), tone_cls_from_delta(d_wow.get("sessions")), "YoY", fmt_delta_ratio(d_yoy.get("sessions")), tone_cls_from_delta(d_yoy.get("sessions")), "#60a5fa", "daily", "sessions", "Daily Sessions"),
        metric_tile("Orders", fmt_int(daily.get("orders")), "WoW", fmt_delta_ratio(d_wow.get("orders")), tone_cls_from_delta(d_wow.get("orders")), "YoY", fmt_delta_ratio(d_yoy.get("orders")), tone_cls_from_delta(d_yoy.get("orders")), "#a78bfa", "daily", "orders", "Daily Orders"),
        metric_tile("Revenue", fmt_krw_symbol(daily.get("revenue")), "WoW", fmt_delta_ratio(d_wow.get("revenue")), tone_cls_from_delta(d_wow.get("revenue")), "YoY", fmt_delta_ratio(d_yoy.get("revenue")), tone_cls_from_delta(d_yoy.get("revenue")), "#22c55e", "daily", "revenue", "Daily Revenue"),
        metric_tile("CVR", fmt_cvr(daily.get("cvr")), "WoW", fmt_pp_from_fraction(d_wow.get("cvr_pp")), pp_tone_cls(d_wow.get("cvr_pp")), "YoY", fmt_pp_from_fraction(d_yoy.get("cvr_pp")), pp_tone_cls(d_yoy.get("cvr_pp")), "#f59e0b", "daily", "cvr", "Daily CVR"),
        metric_tile("Sign-up Users", fmt_int(daily.get("signups")), "WoW", fmt_delta_ratio(d_wow.get("signups")), tone_cls_from_delta(d_wow.get("signups")), "YoY", fmt_delta_ratio(d_yoy.get("signups")), tone_cls_from_delta(d_yoy.get("signups")), "#14b8a6", "daily", "signups", "Daily Sign-up Users"),
    ])
    daily_strip = section_shell(
        "dailyKpi",
        "DAILY SNAPSHOT",
        "Daily KPI Summary",
        "전일 핵심 퍼포먼스를 한 번에 비교할 수 있도록 카드 간 위계를 분리하고, 증감률은 즉시 눈에 들어오게 구성했습니다.",
        f'기준일 <b class="text-slate-900">{daily.get("date") or "-"}</b><br/>updated {daily.get("updated") or ""}',
        "#60a5fa",
        f'<div class="summary-kpi-grid summary-kpi-grid--five">{daily_tiles}</div>{trend_panel("daily", "Daily KPI Trend", "최근 일자 기준으로 카드 선택 KPI의 흐름을 보여줍니다.")}',
    )

    weekly_tiles = "".join([
        metric_tile("Sessions", fmt_int(weekly.get("sessions")), "WoW", fmt_delta_ratio(w_wow.get("sessions")), tone_cls_from_delta(w_wow.get("sessions")), "YoY", fmt_delta_ratio(w_yoy.get("sessions")), tone_cls_from_delta(w_yoy.get("sessions")), "#38bdf8", "weekly", "sessions", "Weekly Sessions"),
        metric_tile("Orders", fmt_int(weekly.get("orders")), "WoW", fmt_delta_ratio(w_wow.get("orders")), tone_cls_from_delta(w_wow.get("orders")), "YoY", fmt_delta_ratio(w_yoy.get("orders")), tone_cls_from_delta(w_yoy.get("orders")), "#8b5cf6", "weekly", "orders", "Weekly Orders"),
        metric_tile("Revenue", fmt_krw_symbol(weekly.get("revenue")), "WoW", fmt_delta_ratio(w_wow.get("revenue")), tone_cls_from_delta(w_wow.get("revenue")), "YoY", fmt_delta_ratio(w_yoy.get("revenue")), tone_cls_from_delta(w_yoy.get("revenue")), "#10b981", "weekly", "revenue", "Weekly Revenue"),
        metric_tile("CVR", fmt_cvr(weekly.get("cvr")), "WoW", fmt_pp_from_fraction(w_wow.get("cvr_pp")), pp_tone_cls(w_wow.get("cvr_pp")), "YoY", fmt_pp_from_fraction(w_yoy.get("cvr_pp")), pp_tone_cls(w_yoy.get("cvr_pp")), "#f97316", "weekly", "cvr", "Weekly CVR"),
        metric_tile("Sign-up Users", fmt_int(weekly.get("signups")), "WoW", fmt_delta_ratio(w_wow.get("signups")), tone_cls_from_delta(w_wow.get("signups")), "YoY", fmt_delta_ratio(w_yoy.get("signups")), tone_cls_from_delta(w_yoy.get("signups")), "#06b6d4", "weekly", "signups", "Weekly Sign-up Users"),
    ])
    weekly_strip = section_shell(
        "weeklyKpi",
        "WEEKLY TREND",
        "Weekly KPI Summary (7D)",
        "최근 7일 누적 흐름을 별도 톤으로 구분해 일간 카드와 헷갈리지 않도록 분리했습니다.",
        f'기간 <b class="text-slate-900">{weekly.get("start") or "-"} ~ {weekly.get("end") or "-"}</b><br/>updated {weekly.get("updated") or ""}',
        "#8b5cf6",
        f'<div class="summary-kpi-grid summary-kpi-grid--five">{weekly_tiles}</div>{trend_panel("weekly", "Weekly KPI Trend", "주간 누적 추이를 기준으로 선택 KPI를 비교합니다.")}',
    )

    owned_block = section_shell(
        "ownedYtd",
        "OWNED PERFORMANCE",
        "OWNED YTD YoY (EDM + LMS + KAKAO)",
        "build_owned_campaign 포털 로직을 직접 반영해 send_id 우선 dedupe / fallback grouping을 적용하고, has_group_mmdd=true 캠페인만 누적 대상으로 사용합니다.",
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
              <div class="mt-5 grid grid-cols-1 md:grid-cols-3 gap-3">
                <div class="channel-metric"><div class="k">Sessions</div><div class="v metric-value" data-countup="{fmt_int(cur.get("sessions"))}">{fmt_int(cur.get("sessions"))}</div><div class="s">LY {fmt_int(prev.get("sessions"))} · <b class="{tone_cls_from_delta(yoy.get("sessions"))}">{fmt_delta_ratio(yoy.get("sessions"))}</b></div></div>
                <div class="channel-metric"><div class="k">Revenue</div><div class="v metric-value" data-countup="{fmt_krw_symbol(cur.get("revenue"))}">{fmt_krw_symbol(cur.get("revenue"))}</div><div class="s">LY {fmt_krw_symbol(prev.get("revenue"))} · <b class="{tone_cls_from_delta(yoy.get("revenue"))}">{fmt_delta_ratio(yoy.get("revenue"))}</b></div></div>
                <div class="channel-metric"><div class="k">CVR</div><div class="v metric-value" data-countup="{fmt_cvr(cur.get("cvr"))}">{fmt_cvr(cur.get("cvr"))}</div><div class="s">LY {fmt_cvr(prev.get("cvr"))} · <b class="{pp_tone_cls(yoy.get("cvr_pp"))}">{fmt_pp_from_fraction(yoy.get("cvr_pp"))}</b></div></div>
              </div>
            </article>
            ''')

        owned_inner = f'''
        <div class="insight-panel rounded-[28px] p-5 sm:p-6 reveal">
          <div class="flex flex-col lg:flex-row lg:items-end lg:justify-between gap-4">
            <div>
              <div class="section-eyebrow">YTD TOTAL</div>
              <h3 class="mt-2 text-2xl font-black tracking-tight text-slate-950">누적 전체 요약</h3>
              <p class="mt-2 text-sm text-slate-600">owned campaign 포털 결과 기준으로 채널별 누적 성과를 합산해 표시합니다.</p>
            </div>
            <div class="text-xs text-slate-600 leading-6 shrink-0">
              기간 <b class="text-slate-900">{period}</b><br/>
              YoY 비교 <b class="text-slate-900">{prev_period}</b><br/>
              updated {upd}
            </div>
          </div>
          <div class="mt-5 grid grid-cols-1 md:grid-cols-3 gap-4">{owned_total_tiles}</div>
        </div>
        <div class="mt-4 grid grid-cols-1 2xl:grid-cols-3 gap-4">{''.join(channel_cards)}</div>
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
      --bg-a:#f2f5f8;
      --bg-b:#e9eef4;
      --ink:#0f172a;
    }}
    * {{ box-sizing:border-box; }}
    html {{ scroll-behavior:smooth; }}
    body {{
      margin:0;
      min-height:100vh;
      color:var(--ink);
      font-family:'Plus Jakarta Sans',sans-serif;
      background:linear-gradient(180deg, #eef2f6 0%, #f4f6f8 42%, #eef2f6 100%);
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
      background:radial-gradient(circle, rgba(148,163,184,.18), transparent 65%);
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
      background:radial-gradient(circle, rgba(203,213,225,.18), transparent 66%);
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
      min-width:min(100%, 24rem);
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
    .metric-value {{ overflow-wrap:anywhere; word-break:break-word; }}
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
    .channel-metric .v {{ margin-top:.5rem; font-size:1.45rem; font-weight:900; line-height:1.08; color:#020617; overflow-wrap:anywhere; word-break:break-word; }}
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
    .chart-tooltip {{
      position:absolute;
      left:0;
      top:0;
      transform:translate(-50%, calc(-100% - 12px));
      pointer-events:none;
      z-index:30;
      min-width:132px;
      padding:.65rem .75rem;
      border-radius:16px;
      background:rgba(15,23,42,.96);
      color:#f8fafc;
      box-shadow:0 18px 42px rgba(15,23,42,.22);
      border:1px solid rgba(255,255,255,.12);
      backdrop-filter:blur(10px);
      white-space:nowrap;
    }}
    .chart-tooltip .t-date {{ display:block; font-size:11px; font-weight:800; letter-spacing:.08em; color:#cbd5e1; margin-bottom:.25rem; }}
    .chart-tooltip .t-value {{ display:block; font-size:14px; font-weight:900; color:#ffffff; }}
    .point-hit {{ cursor:pointer; pointer-events:all; }}
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

    .chart-hint {{ display:inline-flex; align-items:center; justify-content:center; padding:.26rem .45rem; border-radius:999px; font-size:9px; font-weight:900; letter-spacing:.14em; color:#475569; background:rgba(255,255,255,.86); border:1px solid rgba(255,255,255,.9); }}
    .metric-card[data-chart-section], .channel-metric[data-chart-section] {{ cursor:pointer; }}
    .metric-card.active-chart, .channel-metric.active-chart {{ transform:translateY(-6px) scale(1.01); box-shadow:0 28px 64px rgba(15,23,42,.14); border-color:color-mix(in srgb, var(--accent, #94a3b8) 40%, white); }}
    .summary-kpi-grid {{
      display:grid;
      gap:1rem;
      grid-template-columns:repeat(1, minmax(0,1fr));
      align-items:stretch;
    }}
    @media (min-width: 640px) {{
      .summary-kpi-grid {{ grid-template-columns:repeat(2, minmax(0,1fr)); }}
    }}
    @media (min-width: 1280px) {{
      .summary-kpi-grid--five {{ grid-template-columns:repeat(5, minmax(0,1fr)); }}
    }}
    .summary-kpi-grid > .metric-card {{ width:100%; min-width:0; height:100%; }}
    .trend-panel {{ background:linear-gradient(180deg, rgba(255,255,255,.84), rgba(255,255,255,.7)); border:1px solid rgba(255,255,255,.76); box-shadow:0 18px 48px rgba(15,23,42,.08); }}
    .trend-svg-shell {{ background:linear-gradient(180deg, rgba(248,250,252,.92), rgba(255,255,255,.88)); border:1px solid rgba(226,232,240,.9); box-shadow: inset 0 1px 0 rgba(255,255,255,.86); overflow:hidden; }}
    .trend-scroll {{ overflow-x:auto; overflow-y:hidden; padding-bottom:.25rem; }}
    .trend-svg {{ min-width:100%; display:block; }}
    .range-tabs {{ align-items:center; }}
    .range-tab {{ display:inline-flex; align-items:center; justify-content:center; min-width:74px; padding:.55rem .8rem; border-radius:999px; font-size:11px; font-weight:900; letter-spacing:.12em; color:#475569; background:rgba(255,255,255,.84); border:1px solid rgba(255,255,255,.9); box-shadow:0 10px 22px rgba(15,23,42,.05); transition:transform .22s ease, box-shadow .22s ease, background .22s ease, color .22s ease; }}
    .range-tab:hover {{ transform:translateY(-1px); box-shadow:0 14px 26px rgba(15,23,42,.08); }}
    .range-tab.active {{ background:linear-gradient(135deg, rgba(15,23,42,.92), rgba(30,41,59,.88)); color:#fff; box-shadow:0 18px 34px rgba(15,23,42,.16); }}
    .trend-anim-line {{ stroke-dasharray: var(--path-len, 1200); stroke-dashoffset: var(--path-len, 1200); animation:dashDraw .9s cubic-bezier(.2,.8,.2,1) forwards; }}
    .trend-anim-fade {{ opacity:0; animation:fadeRise .55s ease forwards; }}
    @keyframes dashDraw {{ to {{ stroke-dashoffset:0; }} }}
    @keyframes fadeRise {{ from {{ opacity:0; transform:translateY(8px); }} to {{ opacity:1; transform:translateY(0); }} }}
  </style>
</head>
<body>
  <div class="w-full max-w-none px-3 sm:px-5 lg:px-6 2xl:px-8 py-5 sm:py-8">
    <header class="hero-shell reveal rounded-[36px] px-5 py-6 sm:px-7 sm:py-8 mb-7">
      <div class="hero-shimmer"></div>
      <div class="relative z-10 flex flex-col xl:flex-row xl:items-end xl:justify-between gap-5">
        <div>
          <div class="section-eyebrow">CSK E-COMM SUMMARY</div>
          <h1 class="mt-3 text-3xl sm:text-5xl font-black tracking-[-0.04em] text-slate-950">오늘의 핵심 요약</h1>
          <p class="mt-3 text-sm sm:text-base text-slate-600 max-w-3xl">Daily · Weekly · Owned YTD를 같은 스타일 안에서 명확히 다른 레이어로 분리해, 섹션 전환이 바로 인지되도록 재정렬했습니다.</p>
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
  <script id="trend-data" type="application/json">{json.dumps({"daily": daily_trend, "weekly": weekly_trend, "owned": owned_trend}, ensure_ascii=False)}</script>
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
      const formatMetric = (metric, value) => {{
        if (value == null || Number.isNaN(Number(value))) return '-';
        const v = Number(value);
        if (metric === 'revenue') return `₩${{Math.round(v).toLocaleString('en-US')}}`;
        if (metric === 'cvr') return `${{(v * 100).toFixed(2)}}%`;
        return `${{Math.round(v).toLocaleString('en-US')}}`;
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
      const trendData = JSON.parse(document.getElementById('trend-data')?.textContent || '{{}}');
      const parseISODate = (s) => {{
        if (!s) return null;
        const d = new Date(`${{s}}T00:00:00`);
        return Number.isNaN(d.getTime()) ? null : d;
      }};
      const shiftDate = (s, days=0) => {{
        const d = parseISODate(s);
        if (!d) return null;
        d.setDate(d.getDate() + days);
        return d;
      }};
      const shiftYearKeepDay = (s, years=1) => {{
        const d = parseISODate(s);
        if (!d) return null;
        return new Date(d.getFullYear() - years, d.getMonth(), d.getDate());
      }};
      const fmtAxisDate = (s, rangeKey='1MONTH') => {{
        if (!s) return '';
        if (rangeKey === '1YEAR' || rangeKey === '2YEAR') return s.slice(5).replace('-', '.');
        return s.slice(5);
      }};
      const getWeekend = (s) => {{
        const d = parseISODate(s);
        if (!d) return -1;
        const day = d.getDay();
        return day === 6 ? 6 : day === 0 ? 0 : -1;
      }};
      const pickSeries = (section, metric, channel) => {{
        if (section === 'daily') return (trendData.daily || []).map(x => ({{ date: x.date || '', rawLabel: x.label || x.date || '', tipLabel: x.date || '', value: Number(x[metric] || 0), weekend: getWeekend(x.date || '') }}));
        if (section === 'weekly') return (trendData.weekly || []).map(x => ({{ date: x.date || '', weekStart: x.week_start || '', weekEnd: x.week_end || x.date || '', rawLabel: x.label || '', tipLabel: x.full_label || `${{x.label || ''}} · ${{x.week_start || ''}} ~ ${{x.week_end || x.date || ''}}`, value: Number(x[metric] || 0), weekend: -1 }}));
        if (section === 'owned') {{
          const source = channel ? ((((trendData.owned || {{}}).by_channel || {{}})[channel]) || []) : (((trendData.owned || {{}}).total) || []);
          return source.map(x => ({{ date: x.date || '', rawLabel: x.label || x.date || '', tipLabel: x.date || '', value: Number(x[metric] || 0), weekend: getWeekend(x.date || '') }}));
        }}
        return [];
      }};
      const rangeStartForSeries = (series, rangeKey) => {{
        if (!series.length) return null;
        const latestDate = series[series.length - 1].date;
        const latest = parseISODate(latestDate);
        if (!latest) return null;
        if (rangeKey === '7D') return shiftDate(latestDate, -6);
        if (rangeKey === '14D') return shiftDate(latestDate, -13);
        if (rangeKey === '1MONTH') return new Date(latest.getFullYear(), latest.getMonth(), 1);
        if (rangeKey === '1YEAR') return shiftYearKeepDay(latestDate, 1);
        if (rangeKey === '2YEAR') return shiftYearKeepDay(latestDate, 2);
        return null;
      }};
      const filterSeriesByRange = (series, rangeKey) => {{
        if (!series.length) return [];
        const latest = parseISODate(series[series.length - 1].date);
        const start = rangeStartForSeries(series, rangeKey);
        if (!latest || !start) return series;
        const out = series.filter(x => {{
          const d = parseISODate(x.date);
          return d && d >= start && d <= latest;
        }});
        return out.length ? out : series;
      }};
      const shouldDrawXAxisLabel = (item, idx, len, section, rangeKey) => {{
        if (len <= 16) return true;
        if (section === 'weekly') {{
          if (rangeKey === '7D' || rangeKey === '14D' || rangeKey === '1MONTH') return true;
          const mod = rangeKey === '2YEAR' ? 8 : 4;
          return idx === 0 || idx === len - 1 || (idx % mod === 0);
        }}
        if (rangeKey === '1YEAR') {{
          const day = (item.date || '').slice(8, 10);
          return idx === 0 || idx === len - 1 || day === '01';
        }}
        if (rangeKey === '2YEAR') {{
          const mmdd = (item.date || '').slice(5);
          return idx === 0 || idx === len - 1 || mmdd === '01-01' || mmdd.slice(3) === '01';
        }}
        return true;
      }};
      const axisLabelFor = (item, idx, len, section, rangeKey) => {{
        if (section === 'weekly') {{
          return shouldDrawXAxisLabel(item, idx, len, section, rangeKey) ? (item.rawLabel || '') : '';
        }}
        if ((rangeKey === '1YEAR' || rangeKey === '2YEAR') && !shouldDrawXAxisLabel(item, idx, len, section, rangeKey)) return '';
        return fmtAxisDate(item.date || '', rangeKey);
      }};
      const renderTrend = (panel, section, metric, label, channel='') => {{
        const rangeKey = panel.dataset.range || '1MONTH';
        const baseSeries = pickSeries(section, metric, channel).filter(x => Number.isFinite(x.value));
        const sliced = filterSeriesByRange(baseSeries, rangeKey);
        const series = sliced.map((x, idx, arr) => ({{...x, label: axisLabelFor(x, idx, arr.length, section, rangeKey)}}));
        const svg = panel.querySelector('[data-trend-svg]');
        const titleEl = panel.querySelector('[data-selected-title]');
        const summaryEl = panel.querySelector('[data-selected-summary]');
        titleEl.textContent = `${{label || metric}} · ${{rangeKey}}`;
        if (!series.length) {{ svg.innerHTML=''; summaryEl.textContent='표시할 데이터가 없습니다.'; return; }}
        const vals = series.map(x => x.value);
        const min = Math.min(...vals);
        const max = Math.max(...vals);
        const range = (max - min) || (max === 0 ? 1 : Math.abs(max) * 0.15) || 1;
        const padL = 60, padR = 28, padT = 24, padB = 112;
        const dynamicStep = section === 'weekly'
          ? (rangeKey === '2YEAR' ? 58 : rangeKey === '1YEAR' ? 64 : 84)
          : (rangeKey === '2YEAR' ? 18 : rangeKey === '1YEAR' ? 26 : 58);
        const W = Math.max(1200, padL + padR + Math.max(series.length - 1, 1) * dynamicStep);
        const H = 420;
        const innerW = W - padL - padR, innerH = H - padT - padB;
        const stepX = series.length > 1 ? innerW / (series.length - 1) : innerW / 2;
        svg.setAttribute('viewBox', `0 0 ${{W}} ${{H}}`);
        svg.style.width = `${{W}}px`;
        const pts = series.map((p, i) => {{
          const x = padL + (series.length > 1 ? i * stepX : innerW / 2);
          const y = padT + innerH - (((p.value - min) / range) * innerH);
          return [x, y];
        }});
        const line = pts.map(p => p.join(',')).join(' ');
        const area = `M ${{padL}} ${{H-padB}} L ${{pts.map(p=>p.join(' ')).join(' L ')}} L ${{pts[pts.length-1][0]}} ${{H-padB}} Z`;
        const grid = [0,1,2,3,4].map(i => {{ const y = padT + (innerH * i / 4); return `<line x1="${{padL}}" y1="${{y}}" x2="${{W-padR}}" y2="${{y}}" stroke="rgba(148,163,184,.26)" stroke-width="1" />`; }}).join('');
        const weekendBands = (section === 'daily' || section === 'owned') ? series.map((x, i) => {{
          if (x.weekend < 0) return '';
          const left = i === 0 ? pts[i][0] - stepX / 2 : (pts[i-1][0] + pts[i][0]) / 2;
          const right = i === series.length - 1 ? pts[i][0] + stepX / 2 : (pts[i][0] + pts[i+1][0]) / 2;
          const fill = x.weekend === 6 ? 'rgba(37,99,235,.30)' : 'rgba(220,38,38,.30)';
          return `<rect x="${{left}}" y="${{padT}}" width="${{Math.max(0, right-left)}}" height="${{innerH}}" fill="${{fill}}"></rect>`;
        }}).join('') : '';
        const circles = pts.map((p,i) => {{
          const stroke = series[i].weekend === 6 ? '#1d4ed8' : series[i].weekend === 0 ? '#b91c1c' : 'rgba(15,23,42,.76)';
          const pretty = formatMetric(metric, series[i].value);
          const tipDate = section === 'weekly' ? (series[i].tipLabel || series[i].rawLabel || '') : (series[i].date || series[i].label || '');
          return `<g><circle class="trend-anim-fade" cx="${{p[0]}}" cy="${{p[1]}}" r="${{i===pts.length-1?5.5:3.4}}" fill="white" stroke="${{stroke}}" stroke-width="${{i===pts.length-1?2:1.6}}" style="animation-delay:${{Math.min(i*0.012, .36)}}s"></circle><circle class="point-hit" cx="${{p[0]}}" cy="${{p[1]}}" r="12" fill="transparent" data-date="${{tipDate}}" data-label="${{series[i].label || series[i].rawLabel || ''}}" data-value="${{pretty}}"><title>${{tipDate}} · ${{pretty}}</title></circle></g>`;
        }}).join('');
        const labels = pts.map((p, i) => {{
          if (!series[i].label) return '';
          const fill = section === 'weekly' ? '#475569' : series[i].weekend === 6 ? '#1d4ed8' : series[i].weekend === 0 ? '#b91c1c' : '#64748b';
          const bg = section === 'weekly' ? 'transparent' : series[i].weekend === 6 ? 'rgba(37,99,235,.18)' : series[i].weekend === 0 ? 'rgba(220,38,38,.18)' : 'transparent';
          const w = section === 'weekly' ? 36 : 44;
          return `<g class="trend-anim-fade" style="animation-delay:${{Math.min(i*0.006, .32)}}s"><rect x="${{p[0]-w/2}}" y="${{H-34}}" width="${{w}}" height="18" rx="8" fill="${{bg}}"></rect><text x="${{p[0]}}" y="${{H - 21}}" text-anchor="middle" font-size="11" font-weight="800" fill="${{fill}}">${{series[i].label}}</text></g>`;
        }}).join('');
        svg.innerHTML = `<defs><linearGradient id="trendFill" x1="0" x2="0" y1="0" y2="1"><stop offset="0%" stop-color="rgba(59,130,246,.30)"/><stop offset="100%" stop-color="rgba(59,130,246,0.02)"/></linearGradient></defs>${{weekendBands}}${{grid}}<path class="trend-anim-fade" d="${{area}}" fill="url(#trendFill)"></path><polyline class="trend-anim-line" points="${{line}}" fill="none" stroke="rgba(15,23,42,.9)" stroke-width="3.6" stroke-linecap="round" stroke-linejoin="round"></polyline>${{circles}}${{labels}}`;
        const polyline = svg.querySelector('.trend-anim-line');
        if (polyline && polyline.getTotalLength) {{
          const len = polyline.getTotalLength();
          polyline.style.setProperty('--path-len', String(len));
        }}
        const last = series[series.length-1]?.value ?? 0;
        const prev = series.length > 1 ? series[series.length-2]?.value ?? null : null;
        const delta = prev == null || prev === 0 ? null : ((last - prev) / prev);
        const startTxt = section === 'weekly' ? (series[0]?.tipLabel || series[0]?.date || '') : (series[0]?.date || '');
        const endTxt = section === 'weekly' ? (series[series.length-1]?.tipLabel || series[series.length-1]?.date || '') : (series[series.length-1]?.date || '');
        summaryEl.textContent = `${{startTxt}} ~ ${{endTxt}} · 최근값 ${{formatMetric(metric, last)}}${{delta == null ? '' : ` · 직전 대비 ${{delta >= 0 ? '+' : ''}}${{(delta * 100).toFixed(1)}}%`}} · ${{series.length}}포인트`;
        const tip = panel.querySelector('[data-chart-tooltip]');
        const shell = panel.querySelector('.trend-svg-shell');
        const hideTip = () => {{ if (tip) tip.hidden = true; }};
        const showTip = (evt) => {{
          if (!tip || !shell) return;
          const t = evt.currentTarget;
          tip.innerHTML = `<span class="t-date">${{t.dataset.date || t.dataset.label || ''}}</span><span class="t-value">${{t.dataset.value || ''}}</span>`;
          const rect = shell.getBoundingClientRect();
          tip.style.left = `${{evt.clientX - rect.left}}px`;
          tip.style.top = `${{evt.clientY - rect.top}}px`;
          tip.hidden = false;
        }};
        svg.querySelectorAll('.point-hit').forEach(node => {{
          node.addEventListener('mouseenter', showTip);
          node.addEventListener('mousemove', showTip);
          node.addEventListener('mouseleave', hideTip);
        }});
        const scroller = panel.querySelector('[data-trend-scroll]');
        if (scroller) {{
          scroller.scrollLeft = Math.max(0, scroller.scrollWidth - scroller.clientWidth);
          scroller.addEventListener('scroll', hideTip, {{ passive:true }});
        }}
      }};
      const activate = (card) => {{
        const section = card.dataset.chartSection;
        document.querySelectorAll(`[data-chart-section="${{section}}"]`).forEach(el => el.classList.remove('active-chart'));
        card.classList.add('active-chart');
        const panel = document.querySelector(`[data-trend-section="${{section}}"]`);
        if (panel) {{
          panel.dataset.metric = card.dataset.chartMetric;
          panel.dataset.label = card.dataset.chartLabel;
          panel.dataset.channel = card.dataset.chartChannel || '';
          renderTrend(panel, section, card.dataset.chartMetric, card.dataset.chartLabel, card.dataset.chartChannel || '');
        }}
      }};
      document.querySelectorAll('[data-chart-section]').forEach(card => card.addEventListener('click', () => activate(card)));
      document.querySelectorAll('[data-range-tabs]').forEach(tabWrap => {{
        tabWrap.querySelectorAll('.range-tab').forEach(btn => btn.addEventListener('click', () => {{
          tabWrap.querySelectorAll('.range-tab').forEach(x => x.classList.remove('active'));
          btn.classList.add('active');
          const panel = btn.closest('[data-trend-section]');
          if (!panel) return;
          panel.dataset.range = btn.dataset.range || '1MONTH';
          renderTrend(panel, panel.dataset.trendSection, panel.dataset.metric || 'revenue', panel.dataset.label || 'Revenue', panel.dataset.channel || '');
        }}));
      }});
      ['daily','weekly'].forEach(section => {{ const first = document.querySelector(`[data-chart-section="${{section}}"]`); if (first) activate(first); }});

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
    daily_trend = build_daily_trend_series(reports_dir)
    weekly_trend = build_weekly_trend_series(reports_dir)
    owned_trend = build_owned_trend_series(reports_dir)
    out = reports_dir / "index.html"
    out.write_text(render_index_html(daily, weekly, owned_ytd, daily_trend, weekly_trend, owned_trend), encoding="utf-8")
    print(f"[OK] Wrote: {out}")


if __name__ == "__main__":
    main()
