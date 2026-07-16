#!/usr/bin/env python3
"""Create normalized dashboard metadata and detect stale report data.

This script is intentionally dependency-free so it can run at the end of every
GitHub Actions build. It never treats a fresh HTML build timestamp as proof that
the underlying data is fresh.
"""
from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

KST = timezone(timedelta(hours=9))
DATE_RE = re.compile(r"(20\d{2})[-./](\d{1,2})[-./](\d{1,2})")


@dataclass
class ReportHealth:
    key: str
    status: str
    data_end: str | None
    age_days: int | None
    max_age_days: int
    source_file: str | None
    note: str


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    match = DATE_RE.search(text)
    if not match:
        return None
    y, m, d = map(int, match.groups())
    try:
        return date(y, m, d)
    except ValueError:
        return None


def walk_values(obj: Any, keys: set[str]) -> Iterable[tuple[str, Any]]:
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key in keys:
                yield key, value
            yield from walk_values(value, keys)
    elif isinstance(obj, list):
        for item in obj:
            yield from walk_values(item, keys)


def newest_date_in_json(path: Path, keys: set[str]) -> date | None:
    try:
        obj = load_json(path)
    except Exception:
        return None
    dates = [parse_date(value) for _, value in walk_values(obj, keys)]
    valid = [d for d in dates if d is not None]
    return max(valid) if valid else None


def latest_available_date(path: Path) -> date | None:
    try:
        values = load_json(path)
    except Exception:
        return None
    if not isinstance(values, list):
        return None
    valid = [parse_date(v) for v in values]
    valid = [d for d in valid if d is not None]
    return max(valid) if valid else None


def first_existing(paths: list[Path]) -> Path | None:
    return next((p for p in paths if p.exists()), None)


def evaluate(key: str, data_end: date | None, max_age: int, source: Path | None, today: date) -> ReportHealth:
    if data_end is None:
        return ReportHealth(key, "missing", None, None, max_age, str(source) if source else None, "data end date could not be resolved")
    age = (today - data_end).days
    status = "fresh" if age <= max_age else "stale"
    return ReportHealth(key, status, data_end.isoformat(), age, max_age, str(source) if source else None, "ok" if status == "fresh" else f"data is {age} days old")


def write_meta(path: Path, key: str, health: ReportHealth, built_at: datetime) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "report_key": key,
        "updated_at": built_at.strftime("%Y-%m-%d %H:%M KST"),
        "period_end": health.data_end,
        "data_status": health.status,
        "data_age_days": health.age_days,
        "health_source": health.source_file,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strict", action="store_true", help="exit non-zero when critical reports are stale or missing")
    parser.add_argument("--today", help="override KST date (YYYY-MM-DD) for tests")
    args = parser.parse_args()

    now = datetime.now(KST)
    today = date.fromisoformat(args.today) if args.today else now.date()

    results: list[ReportHealth] = []

    # UTM: trust actual period_end in meta, not the build timestamp.
    utm = Path("reports/utm_channel/data/meta.json")
    utm_end = None
    if utm.exists():
        try:
            utm_end = parse_date(load_json(utm).get("period_end"))
        except Exception:
            pass
    results.append(evaluate("utm_channel", utm_end, 3, utm if utm.exists() else None, today))

    # Product keyword: end_date is the report's real aggregation boundary.
    product = Path("reports/product_keyword/data/meta.json")
    product_end = None
    if product.exists():
        try:
            product_end = parse_date(load_json(product).get("end_date"))
        except Exception:
            pass
    results.append(evaluate("product_keyword", product_end, 3, product if product.exists() else None, today))

    # Purchase pattern: use max_order_date from generated summary.
    purchase_candidates = [
        Path("reports/purchase_pattern/data/summary.json"),
        Path("reports/purchase_pattern/data/report.json"),
        Path("reports/purchase_pattern/data/data.json"),
    ]
    purchase = first_existing(purchase_candidates)
    purchase_end = newest_date_in_json(purchase, {"max_order_date", "order_date", "data_end", "period_end"}) if purchase else None
    results.append(evaluate("purchase_pattern", purchase_end, 3, purchase, today))

    # OWNED and funnel use available_dates as the authoritative freshness signal.
    owned = Path("reports/owned_portal/data/owned/available_dates.json")
    results.append(evaluate("owned", latest_available_date(owned), 3, owned if owned.exists() else None, today))

    funnel = Path("reports/daily_digest/data/funnel/available_dates.json")
    results.append(evaluate("funnel", latest_available_date(funnel), 3, funnel if funnel.exists() else None, today))

    # Member and AI Agent builders have historically omitted meta.json. Infer the
    # latest date from their generated JSON and always emit a normalized meta file.
    member_candidates = list(Path("reports/member_funnel/data").glob("*.json")) if Path("reports/member_funnel/data").exists() else []
    member_dates = [newest_date_in_json(p, {"max_order_date", "date", "data_end", "period_end", "end_date"}) for p in member_candidates]
    member_dates = [d for d in member_dates if d]
    member_source = member_candidates[0] if member_candidates else None
    member_health = evaluate("member", max(member_dates) if member_dates else None, 5, member_source, today)
    results.append(member_health)
    write_meta(Path("reports/member_funnel/data/meta.json"), "member", member_health, now)

    ai_candidates = list(Path("reports/ai_agent/data").glob("*.json")) if Path("reports/ai_agent/data").exists() else []
    ai_dates = [newest_date_in_json(p, {"max_order_date", "date", "data_end", "period_end", "end_date"}) for p in ai_candidates]
    ai_dates = [d for d in ai_dates if d]
    ai_source = ai_candidates[0] if ai_candidates else None
    ai_health = evaluate("ai_agent", max(ai_dates) if ai_dates else None, 5, ai_source, today)
    results.append(ai_health)
    write_meta(Path("reports/ai_agent/data/meta.json"), "ai_agent", ai_health, now)

    overall = "fresh"
    if any(r.status == "stale" for r in results):
        overall = "stale"
    elif any(r.status == "missing" for r in results):
        overall = "partial"

    payload = {
        "build": now.strftime("%Y.%m.%d (%a) %H:%M KST"),
        "overall_status": overall,
        "reports": {r.key: asdict(r) for r in results},
    }
    out = Path("reports/meta.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print(json.dumps(payload, ensure_ascii=False, indent=2))

    critical = {"utm_channel", "product_keyword", "purchase_pattern", "owned", "funnel"}
    failures = [r for r in results if r.key in critical and r.status != "fresh"]
    if args.strict and failures:
        print("[ERROR] Critical dashboard freshness checks failed:")
        for r in failures:
            print(f" - {r.key}: {r.status}; data_end={r.data_end}; note={r.note}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
