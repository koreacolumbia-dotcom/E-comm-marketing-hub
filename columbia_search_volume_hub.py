from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

try:
    from google.ads.googleads.client import GoogleAdsClient
except Exception:  # pragma: no cover
    GoogleAdsClient = None

KST = timezone(timedelta(hours=9))
NAVER_SEARCHAD_URL = "https://api.searchad.naver.com/keywordstool"

MONTH_NAME_MAP = {
    1: "01", 2: "02", 3: "03", 4: "04", 5: "05", 6: "06",
    7: "07", 8: "08", 9: "09", 10: "10", 11: "11", 12: "12",
    "JANUARY": "01", "FEBRUARY": "02", "MARCH": "03", "APRIL": "04", "MAY": "05", "JUNE": "06",
    "JULY": "07", "AUGUST": "08", "SEPTEMBER": "09", "OCTOBER": "10", "NOVEMBER": "11", "DECEMBER": "12",
}


@dataclass
class Config:
    keywords: List[str]
    output_json: Path
    output_html: Path
    meta_json: Path
    template: Path
    naver_api_key: str
    naver_secret_key: str
    naver_customer_id: str
    google_customer_id: str
    google_login_customer_id: str
    google_developer_token: str
    google_client_id: str
    google_client_secret: str
    google_refresh_token: str
    google_language_resource: str
    google_geo_target_resource: str


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build actual search volume dashboard (Naver SearchAd + optional Google Ads).")
    p.add_argument("--keywords", required=True, help="Comma-separated keywords")
    p.add_argument("--output-json", default="reports/search_volume/data/search_volume_data.json")
    p.add_argument("--output-html", default="reports/search_volume/index.html")
    p.add_argument("--meta-json", default="reports/search_volume/data/meta.json")
    p.add_argument("--template", default="search_volume_hub_template.html")
    p.add_argument("--skip-if-current", action="store_true")
    p.add_argument("--start-date", default="", help="Optional backfill start date (accepted for workflow compatibility)")
    p.add_argument("--end-date", default="", help="Optional snapshot end date in YYYY-MM-DD")
    p.add_argument("--geo", default="KR", help="Optional geo code (accepted for workflow compatibility)")

    p.add_argument("--naver-api-key", default=os.getenv("NAVER_AD_API_KEY", ""))
    p.add_argument("--naver-secret-key", default=os.getenv("NAVER_AD_SECRET_KEY", ""))
    p.add_argument("--naver-customer-id", default=os.getenv("NAVER_AD_CUSTOMER_ID", ""))

    p.add_argument("--google-customer-id", default=os.getenv("GOOGLE_ADS_CUSTOMER_ID", ""))
    p.add_argument("--google-login-customer-id", default=os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID", ""))
    p.add_argument("--google-developer-token", default=os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN", ""))
    p.add_argument("--google-client-id", default=os.getenv("GOOGLE_ADS_CLIENT_ID", ""))
    p.add_argument("--google-client-secret", default=os.getenv("GOOGLE_ADS_CLIENT_SECRET", ""))
    p.add_argument("--google-refresh-token", default=os.getenv("GOOGLE_ADS_REFRESH_TOKEN", ""))
    p.add_argument("--google-language-resource", default=os.getenv("GOOGLE_ADS_LANGUAGE_RESOURCE", "languageConstants/1012"))
    p.add_argument("--google-geo-target-resource", default=os.getenv("GOOGLE_ADS_GEO_TARGET_RESOURCE", "geoTargetConstants/2410"))
    return p.parse_args()


def clean_keywords(raw: str) -> List[str]:
    return [x.strip() for x in raw.split(",") if x.strip()]


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def safe_int(v: Any) -> int:
    if v is None:
        return 0
    if isinstance(v, int):
        return v
    s = str(v).strip().replace(",", "")
    if not s or s.lower() in {"< 10", "<10", "-", "null", "none"}:
        return 0
    digits = "".join(ch for ch in s if ch.isdigit())
    return int(digits) if digits else 0


def fmt_kst(dt: datetime) -> str:
    return dt.astimezone(KST).strftime("%Y.%m.%d %H:%M KST")


def maybe_skip_current(meta_json: Path, target_date: str) -> bool:
    if not meta_json.exists():
        return False
    try:
        meta = json.loads(meta_json.read_text(encoding="utf-8"))
    except Exception:
        return False
    return str(meta.get("snapshot_date", "")) == target_date


def naver_signature(timestamp: str, method: str, uri: str, secret_key: str) -> str:
    message = f"{timestamp}.{method}.{uri}"
    digest = hmac.new(secret_key.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def fetch_naver_keyword(keyword: str, api_key: str, secret_key: str, customer_id: str) -> Dict[str, Any]:
    uri = "/keywordstool"
    timestamp = str(int(datetime.now(tz=KST).timestamp() * 1000))
    headers = {
        "X-Timestamp": timestamp,
        "X-API-KEY": api_key,
        "X-Customer": str(customer_id),
        "X-Signature": naver_signature(timestamp, "GET", uri, secret_key),
    }
    params = {
        "hintKeywords": keyword,
        "showDetail": 1,
    }
    res = requests.get(NAVER_SEARCHAD_URL, headers=headers, params=params, timeout=60)
    res.raise_for_status()
    payload = res.json()
    rows = payload.get("keywordList", [])
    exact = None
    for row in rows:
        rel = str(row.get("relKeyword", "")).strip()
        if rel == keyword:
            exact = row
            break
    row = exact or (rows[0] if rows else {})
    pc = safe_int(row.get("monthlyPcQcCnt"))
    mobile = safe_int(row.get("monthlyMobileQcCnt"))
    return {
        "platform": "naver",
        "keyword": keyword,
        "pc": pc,
        "mobile": mobile,
        "total": pc + mobile,
        "rel_keyword": row.get("relKeyword", keyword),
    }


def fetch_naver_snapshot(config: Config, snapshot_date: str) -> List[Dict[str, Any]]:
    if not (config.naver_api_key and config.naver_secret_key and config.naver_customer_id):
        return []
    out = []
    for kw in config.keywords:
        row = fetch_naver_keyword(kw, config.naver_api_key, config.naver_secret_key, config.naver_customer_id)
        row["snapshot_date"] = snapshot_date
        out.append(row)
    return out


def google_ready(config: Config) -> bool:
    return all([
        GoogleAdsClient is not None,
        config.google_customer_id,
        config.google_developer_token,
        config.google_client_id,
        config.google_client_secret,
        config.google_refresh_token,
    ])


def build_google_client(config: Config):
    cfg: Dict[str, Any] = {
        "developer_token": config.google_developer_token,
        "client_id": config.google_client_id,
        "client_secret": config.google_client_secret,
        "refresh_token": config.google_refresh_token,
        "use_proto_plus": True,
    }
    if config.google_login_customer_id:
        cfg["login_customer_id"] = config.google_login_customer_id
    return GoogleAdsClient.load_from_dict(cfg)


def month_enum_to_str(month_value: Any) -> str:
    key = getattr(month_value, "name", month_value)
    return MONTH_NAME_MAP.get(key, MONTH_NAME_MAP.get(int(key), "01") if str(key).isdigit() else "01")


def fetch_google_history(config: Config) -> List[Dict[str, Any]]:
    if not google_ready(config):
        return []

    client = build_google_client(config)
    service = client.get_service("KeywordPlanIdeaService")
    request = client.get_type("GenerateKeywordHistoricalMetricsRequest")
    request.customer_id = str(config.google_customer_id)
    request.keywords.extend(config.keywords)
    request.language = config.google_language_resource
    request.geo_target_constants.append(config.google_geo_target_resource)
    try:
        request.keyword_plan_network = client.enums.KeywordPlanNetworkEnum.GOOGLE_SEARCH
    except Exception:
        pass

    response = service.generate_keyword_historical_metrics(request=request)
    out: List[Dict[str, Any]] = []
    for result in getattr(response, "results", []):
        kw = getattr(result, "text", "")
        metrics = getattr(result, "keyword_metrics", None)
        if not metrics:
            continue
        for volume in getattr(metrics, "monthly_search_volumes", []):
            year = int(getattr(volume, "year", 0))
            month = month_enum_to_str(getattr(volume, "month", 1))
            monthly_searches = int(getattr(volume, "monthly_searches", 0) or 0)
            if year <= 0:
                continue
            out.append({
                "platform": "google",
                "keyword": kw,
                "month": f"{year:04d}-{month}",
                "monthly_searches": monthly_searches,
            })
    out.sort(key=lambda x: (x["month"], x["keyword"]))
    return out


def load_existing(output_json: Path) -> Dict[str, Any]:
    if not output_json.exists():
        return {"keywords": [], "naver_snapshots": [], "google_history": []}
    try:
        return json.loads(output_json.read_text(encoding="utf-8"))
    except Exception:
        return {"keywords": [], "naver_snapshots": [], "google_history": []}


def upsert_naver_snapshots(existing_rows: List[Dict[str, Any]], new_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not new_rows:
        return existing_rows
    key_to_row = {(r.get("snapshot_date"), r.get("keyword"), r.get("platform", "naver")): r for r in existing_rows}
    for row in new_rows:
        key_to_row[(row.get("snapshot_date"), row.get("keyword"), row.get("platform", "naver"))] = row
    return [key_to_row[k] for k in sorted(key_to_row.keys())]


def upsert_google_history(existing_rows: List[Dict[str, Any]], new_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not new_rows:
        return existing_rows
    key_to_row = {(r.get("month"), r.get("keyword"), r.get("platform", "google")): r for r in existing_rows}
    for row in new_rows:
        key_to_row[(row.get("month"), row.get("keyword"), row.get("platform", "google"))] = row
    return [key_to_row[k] for k in sorted(key_to_row.keys())]


def render_html(template_path: Path, payload: Dict[str, Any], output_html: Path) -> None:
    html = template_path.read_text(encoding="utf-8")
    html = html.replace("__DASHBOARD_DATA__", json.dumps(payload, ensure_ascii=False))
    output_html.write_text(html, encoding="utf-8")


def main() -> None:
    args = parse_args()
    config = Config(
        keywords=clean_keywords(args.keywords),
        output_json=Path(args.output_json),
        output_html=Path(args.output_html),
        meta_json=Path(args.meta_json),
        template=Path(args.template),
        naver_api_key=args.naver_api_key,
        naver_secret_key=args.naver_secret_key,
        naver_customer_id=args.naver_customer_id,
        google_customer_id=args.google_customer_id,
        google_login_customer_id=args.google_login_customer_id,
        google_developer_token=args.google_developer_token,
        google_client_id=args.google_client_id,
        google_client_secret=args.google_client_secret,
        google_refresh_token=args.google_refresh_token,
        google_language_resource=args.google_language_resource,
        google_geo_target_resource=args.google_geo_target_resource,
    )

    today = args.end_date or datetime.now(tz=KST).strftime("%Y-%m-%d")
    if args.skip_if_current and maybe_skip_current(config.meta_json, today):
        print(f"[SKIP] search volume already built for {today}")
        return

    ensure_parent(config.output_json)
    ensure_parent(config.output_html)
    ensure_parent(config.meta_json)

    existing = load_existing(config.output_json)
    naver_rows = []
    google_rows = []
    errors: List[str] = []

    try:
        naver_rows = fetch_naver_snapshot(config, today)
        if naver_rows:
            print(f"[OK] fetched naver snapshots: {len(naver_rows)}")
        else:
            print("[WARN] naver snapshot unavailable")
    except Exception as e:
        errors.append(f"Naver fetch failed: {e}")
        print(f"[WARN] {errors[-1]}")

    try:
        google_rows = fetch_google_history(config)
        if google_rows:
            months = sorted({r['month'] for r in google_rows})
            print(f"[OK] fetched google history: {len(months)} months / {len(google_rows)} rows")
        else:
            print("[WARN] google history unavailable")
    except Exception as e:
        errors.append(f"Google fetch failed: {e}")
        print(f"[WARN] {errors[-1]}")

    payload = {
        "keywords": config.keywords,
        "generated_at": fmt_kst(datetime.now(tz=KST)),
        "snapshot_date": today,
        "naver_snapshots": upsert_naver_snapshots(existing.get("naver_snapshots", []), naver_rows),
        "google_history": upsert_google_history(existing.get("google_history", []), google_rows),
        "notes": errors,
        "start_date": args.start_date or "",
        "end_date": today,
        "geo": args.geo or "KR",
    }
    config.output_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    render_html(config.template, payload, config.output_html)

    meta = {
        "build": payload["generated_at"],
        "snapshot_date": today,
        "tab": "search_volume",
        "period": "actual search volume",
        "keywords": config.keywords,
        "has_naver": bool(payload["naver_snapshots"]),
        "has_google": bool(payload["google_history"]),
        "notes": errors,
        "start_date": args.start_date or "",
        "end_date": today,
        "geo": args.geo or "KR",
    }
    config.meta_json.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] wrote: {config.output_json}")
    print(f"[OK] wrote: {config.output_html}")
    print(f"[OK] wrote: {config.meta_json}")


if __name__ == "__main__":
    main()
