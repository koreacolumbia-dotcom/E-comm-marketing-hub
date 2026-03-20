from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

try:
    from pytrends.request import TrendReq
except Exception:
    TrendReq = None

NAVER_API_URL = "https://openapi.naver.com/v1/datalab/search"
KST = timezone(timedelta(hours=9))


@dataclass
class Config:
    keywords: List[str]
    start_date: str
    end_date: str
    time_unit: str
    geo: str
    device: str
    ages: List[str]
    gender: str
    output_json: Path
    output_html: Path
    template: Path
    meta_json: Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build Columbia search trends dashboard (Naver + Google).")
    p.add_argument("--keywords", required=True, help="Comma-separated keyword list")
    p.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--time-unit", default="week", choices=["date", "week", "month"])
    p.add_argument("--geo", default="KR", help="Geo code for Google Trends, e.g. KR")
    p.add_argument("--device", default="all", choices=["all", "pc", "mo"])
    p.add_argument("--ages", default="all", help="Comma-separated ages for Naver DataLab. ex: all or 1,2,3")
    p.add_argument("--gender", default="", choices=["", "m", "f"])
    p.add_argument("--template", default="columbia_search_trends_hub_template.html")
    p.add_argument("--output-json", default="reports/search_trends/data/search_trends_data.json")
    p.add_argument("--output-html", default="reports/search_trends/index.html")
    p.add_argument("--meta-json", default="reports/search_trends/data/meta.json")
    p.add_argument("--skip-if-current", action="store_true", help="Skip build if existing meta.json already covers same end-date")
    p.add_argument("--naver-client-id", default=os.getenv("NAVER_CLIENT_ID", ""))
    p.add_argument("--naver-client-secret", default=os.getenv("NAVER_CLIENT_SECRET", ""))
    p.add_argument("--google-timezone", type=int, default=540, help="Pytrends timezone minutes. Korea=540")
    p.add_argument("--google-language", default="ko-KR")
    p.add_argument("--platforms", default="naver,google", help="Comma-separated subset: naver,google")
    return p.parse_args()


def clean_keywords(raw: str) -> List[str]:
    return [x.strip() for x in raw.split(",") if x.strip()]


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def safe_float(v: Any) -> float:
    try:
        if v is None or v == "":
            return 0.0
        return float(v)
    except Exception:
        return 0.0


def pct_change(cur: float, prev: float) -> Optional[float]:
    cur = safe_float(cur)
    prev = safe_float(prev)
    if prev == 0:
        return None
    return (cur - prev) / prev * 100.0


def avg(values: List[float]) -> float:
    vals = [safe_float(v) for v in values if v is not None]
    return sum(vals) / len(vals) if vals else 0.0


def fmt_kst(dt: datetime) -> str:
    return dt.astimezone(KST).strftime("%Y.%m.%d %H:%M KST")


def maybe_skip_current(meta_json: Path, target_end_date: str) -> bool:
    if not meta_json.exists():
        return False
    try:
        meta = json.loads(meta_json.read_text(encoding="utf-8"))
    except Exception:
        return False
    return str(meta.get("period_end", "")).strip() == target_end_date


# ---------- fetch ----------
def fetch_naver_datalab(
    keywords: List[str],
    start_date: str,
    end_date: str,
    time_unit: str,
    device: str,
    ages: List[str],
    gender: str,
    client_id: str,
    client_secret: str,
) -> Dict[str, Dict[str, float]]:
    if not client_id or not client_secret:
        raise RuntimeError("Naver credentials missing. Set NAVER_CLIENT_ID and NAVER_CLIENT_SECRET.")

    body: Dict[str, Any] = {
        "startDate": start_date,
        "endDate": end_date,
        "timeUnit": time_unit,
        "keywordGroups": [{"groupName": kw, "keywords": [kw]} for kw in keywords],
    }
    if device != "all":
        body["device"] = device
    if ages and ages != ["all"]:
        body["ages"] = ages
    if gender:
        body["gender"] = gender

    headers = {
        "Content-Type": "application/json",
        "X-Naver-Client-Id": client_id,
        "X-Naver-Client-Secret": client_secret,
    }
    res = requests.post(NAVER_API_URL, headers=headers, json=body, timeout=60)
    res.raise_for_status()
    payload = res.json()

    out: Dict[str, Dict[str, float]] = {}
    for group in payload.get("results", []):
        kw = group.get("title")
        for point in group.get("data", []):
            period = point.get("period")
            ratio = safe_float(point.get("ratio"))
            out.setdefault(period, {})[kw] = ratio
    return out


def fetch_google_trends(
    keywords: List[str],
    start_date: str,
    end_date: str,
    geo: str,
    hl: str,
    tz: int,
) -> Dict[str, Dict[str, float]]:
    if TrendReq is None:
        raise RuntimeError("pytrends is not installed. pip install pytrends")

    pytrends = TrendReq(hl=hl, tz=tz)
    timeframe = f"{start_date} {end_date}"
    pytrends.build_payload(keywords, timeframe=timeframe, geo=geo)
    frame = pytrends.interest_over_time()
    if frame is None or frame.empty:
        return {}

    if "isPartial" in frame.columns:
        frame = frame.drop(columns=["isPartial"])

    out: Dict[str, Dict[str, float]] = {}
    for idx, row in frame.iterrows():
        dt = idx.strftime("%Y-%m-%d")
        out[dt] = {kw: safe_float(row.get(kw, 0.0)) for kw in keywords}
    return out


# ---------- transform ----------
def align_series(platform_data: Dict[str, Dict[str, Dict[str, float]]], keywords: List[str]) -> List[str]:
    dates = set()
    for source in platform_data.values():
        dates |= set(source.keys())
    return sorted(dates)


def build_payload(config: Config, raw: Dict[str, Dict[str, Dict[str, float]]]) -> Dict[str, Any]:
    dates = align_series(raw, config.keywords)
    overview: List[Dict[str, Any]] = []
    keyword_series: Dict[str, List[Dict[str, Any]]] = {}
    snapshot: List[Dict[str, Any]] = []
    ranking_latest: List[Dict[str, Any]] = []
    ranking_growth: List[Dict[str, Any]] = []

    peak_naver = {"date": "-", "value": -1.0}
    peak_google = {"date": "-", "value": -1.0}

    for dt in dates:
        naver_vals = [safe_float(raw.get("naver", {}).get(dt, {}).get(kw, 0)) for kw in config.keywords]
        google_vals = [safe_float(raw.get("google", {}).get(dt, {}).get(kw, 0)) for kw in config.keywords]
        nav_avg = avg(naver_vals)
        goo_avg = avg(google_vals)
        gap = nav_avg - goo_avg
        overview.append({
            "date": dt,
            "naver_avg": round(nav_avg, 4),
            "google_avg": round(goo_avg, 4),
            "gap": round(gap, 4),
        })
        if nav_avg > peak_naver["value"]:
            peak_naver = {"date": dt, "value": nav_avg}
        if goo_avg > peak_google["value"]:
            peak_google = {"date": dt, "value": goo_avg}

    latest = overview[-1] if overview else {"date": "-", "naver_avg": 0.0, "google_avg": 0.0, "gap": 0.0}
    prev = overview[-2] if len(overview) >= 2 else None

    for kw in config.keywords:
        rows: List[Dict[str, Any]] = []
        nav_peak_val, goo_peak_val = -1.0, -1.0
        nav_peak_date, goo_peak_date = "-", "-"
        for dt in dates:
            nav = safe_float(raw.get("naver", {}).get(dt, {}).get(kw, 0))
            goo = safe_float(raw.get("google", {}).get(dt, {}).get(kw, 0))
            gap = nav - goo
            rows.append({"date": dt, "naver": round(nav, 4), "google": round(goo, 4), "gap": round(gap, 4)})
            if nav > nav_peak_val:
                nav_peak_val, nav_peak_date = nav, dt
            if goo > goo_peak_val:
                goo_peak_val, goo_peak_date = goo, dt
        keyword_series[kw] = rows
        latest_row = rows[-1] if rows else {"naver": 0.0, "google": 0.0, "gap": 0.0}
        prev_row = rows[-2] if len(rows) >= 2 else None
        avg_latest = (safe_float(latest_row["naver"]) + safe_float(latest_row["google"])) / 2.0
        avg_prev = ((safe_float(prev_row["naver"]) + safe_float(prev_row["google"])) / 2.0) if prev_row else 0.0
        growth = pct_change(avg_latest, avg_prev)
        ranking_latest.append({"keyword": kw, "value": round(avg_latest, 4)})
        ranking_growth.append({"keyword": kw, "value": None if growth is None else round(growth, 4)})
        snapshot.append({
            "keyword": kw,
            "naver_latest": round(safe_float(latest_row["naver"]), 4),
            "google_latest": round(safe_float(latest_row["google"]), 4),
            "gap_latest": round(safe_float(latest_row["gap"]), 4),
            "latest_blended": round(avg_latest, 4),
            "growth_pct": None if growth is None else round(growth, 4),
            "naver_peak_date": nav_peak_date,
            "naver_peak_value": round(max(nav_peak_val, 0), 4),
            "google_peak_date": goo_peak_date,
            "google_peak_value": round(max(goo_peak_val, 0), 4),
        })

    ranking_latest.sort(key=lambda x: x["value"], reverse=True)
    ranking_growth = [x for x in ranking_growth if x["value"] is not None]
    ranking_growth.sort(key=lambda x: x["value"], reverse=True)

    generated = datetime.now(KST)
    meta = {
        "period": f"{config.start_date} → {config.end_date}",
        "period_start": config.start_date,
        "period_end": config.end_date,
        "time_unit": config.time_unit,
        "geo": config.geo,
        "device": config.device,
        "ages": config.ages,
        "gender": config.gender,
        "generated_at": generated.strftime("%Y-%m-%d %H:%M:%S"),
        "generated_at_kst": fmt_kst(generated),
        "period_text": f"최근 3개월 ({config.start_date} ~ {config.end_date})",
        "updated_at_kst": fmt_kst(generated),
        "keywords": config.keywords,
    }

    return {
        "meta": meta,
        "overview": overview,
        "series_by_keyword": keyword_series,
        "snapshot": snapshot,
        "rankings": {
            "latest": ranking_latest[:10],
            "growth": ranking_growth[:10],
        },
        "platform_peaks": {
            "naver": {"date": peak_naver["date"], "value": round(max(peak_naver["value"], 0), 4)},
            "google": {"date": peak_google["date"], "value": round(max(peak_google["value"], 0), 4)},
        },
        "latest_summary": {
            "date": latest["date"],
            "naver_avg_latest": round(safe_float(latest["naver_avg"]), 4),
            "google_avg_latest": round(safe_float(latest["google_avg"]), 4),
            "gap_latest": round(safe_float(latest["gap"]), 4),
            "naver_vs_prev_pct": None if prev is None else round(pct_change(latest["naver_avg"], prev["naver_avg"]) or 0, 4),
            "google_vs_prev_pct": None if prev is None else round(pct_change(latest["google_avg"], prev["google_avg"]) or 0, 4),
        },
        "raw": raw,
    }


def render_html(template_path: Path, payload: Dict[str, Any], output_path: Path) -> None:
    html = template_path.read_text(encoding="utf-8")
    html = html.replace("__DATA__", json.dumps(payload, ensure_ascii=False))
    ensure_parent(output_path)
    output_path.write_text(html, encoding="utf-8")


def main() -> None:
    args = parse_args()
    keywords = clean_keywords(args.keywords)
    ages = [x.strip() for x in args.ages.split(",") if x.strip()] or ["all"]
    platforms = {x.strip().lower() for x in args.platforms.split(",") if x.strip()}
    meta_json = Path(args.meta_json)

    if args.skip_if_current and maybe_skip_current(meta_json, args.end_date):
        print(f"[SKIP] {meta_json} already covers period_end={args.end_date}")
        return

    raw: Dict[str, Dict[str, Dict[str, float]]] = {"naver": {}, "google": {}}

    if "naver" in platforms:
        raw["naver"] = fetch_naver_datalab(
            keywords=keywords,
            start_date=args.start_date,
            end_date=args.end_date,
            time_unit=args.time_unit,
            device=args.device,
            ages=ages,
            gender=args.gender,
            client_id=args.naver_client_id,
            client_secret=args.naver_client_secret,
        )

    if "google" in platforms:
        raw["google"] = fetch_google_trends(
            keywords=keywords,
            start_date=args.start_date,
            end_date=args.end_date,
            geo=args.geo,
            hl=args.google_language,
            tz=args.google_timezone,
        )

    cfg = Config(
        keywords=keywords,
        start_date=args.start_date,
        end_date=args.end_date,
        time_unit=args.time_unit,
        geo=args.geo,
        device=args.device,
        ages=ages,
        gender=args.gender,
        output_json=Path(args.output_json),
        output_html=Path(args.output_html),
        template=Path(args.template),
        meta_json=meta_json,
    )

    payload = build_payload(cfg, raw)
    ensure_parent(cfg.output_json)
    ensure_parent(cfg.meta_json)
    cfg.output_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    cfg.meta_json.write_text(json.dumps(payload["meta"], ensure_ascii=False, indent=2), encoding="utf-8")
    render_html(cfg.template, payload, cfg.output_html)

    print(f"[OK] wrote {cfg.output_json}")
    print(f"[OK] wrote {cfg.meta_json}")
    print(f"[OK] wrote {cfg.output_html}")


if __name__ == "__main__":
    main()
