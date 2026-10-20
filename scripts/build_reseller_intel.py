#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build an evidence-labelled Columbia reseller inventory from authorized CSV exports.

No obsolete Naver Shopping API calls and no scraping or access-control bypass.
Only externally verified business/return addresses should be entered in sellers.csv.
"""
from __future__ import annotations

import argparse
import csv
import html
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, date, time, timedelta, timezone
from pathlib import Path
from statistics import median
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")
LISTING_FIELDS = [
    "observed_at", "seller_name", "seller_id", "channel", "store_url",
    "brand", "product_code", "product_name", "category", "price_krw",
    "list_price_krw", "shipping_fee_krw", "product_url", "source_url",
    "source_type", "verification_status",
]
SELLER_FIELDS = [
    "seller_name", "seller_id", "channel", "business_name",
    "business_registration_number", "business_address", "business_info_url",
    "return_address", "return_info_url", "store_url", "verified_at",
    "verification_status", "memo",
]
GENERIC_MALLS = {"네이버", "naver", "11번가", "옥션", "g마켓", "gmarket", "롯데on", "쿠팡"}
COLUMBIA_NAMES = {"columbia", "컬럼비아", "콜롬비아"}


def load_csv(path: Path, fields: list[str]) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Missing input: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return []
        missing = set(fields) - set(reader.fieldnames)
        if missing:
            raise ValueError(f"{path}: missing columns: {sorted(missing)}")
        return [
            {f: str(row.get(f) or "").strip() for f in fields}
            for row in reader
            if any(str(v or "").strip() for v in row.values())
        ]


def put_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            # Defend spreadsheet users against CSV formula execution.
            writer.writerow({
                k: ("'" + str(row.get(k) or "") if
                    str(row.get(k) or "").lstrip().startswith(("=", "+", "-", "@"))
                    else row.get(k, ""))
                for k in fields
            })


def read_date(text: str) -> datetime | None:
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=KST) if parsed.tzinfo is None else parsed.astimezone(KST)
    except ValueError:
        return None


def price_value(raw: str) -> int | None:
    cleaned = re.sub(r"[,원\s]", "", str(raw or ""))
    return int(cleaned) if cleaned.isdigit() else None


def safe_url(url: str) -> str:
    u = (url or "").strip()
    parsed = urlparse(u)
    return u if parsed.scheme in {"http", "https"} and parsed.netloc else ""


def inferred_channel(url: str, channel: str) -> str:
    if channel:
        return channel
    host = urlparse(safe_url(url)).hostname or ""
    known = [
        ("smartstore.naver.com", "네이버 스마트스토어"),
        ("brand.naver.com", "네이버 브랜드스토어"),
        ("shopping.naver.com", "네이버쇼핑 가격비교"),
        ("11st.co.kr", "11번가"),
        ("auction.co.kr", "옥션"),
        ("gmarket.co.kr", "G마켓"),
        ("coupang.com", "쿠팡"),
        ("lotteon.com", "롯데ON"),
    ]
    return next((v for k, v in known if host == k or host.endswith("." + k)), "기타")


def seller_key(row: dict) -> str:
    seller_id = row.get("seller_id", "").strip()
    if seller_id:
        return "id:" + seller_id.casefold()
    return "name:" + (row.get("seller_name", "").strip().casefold()
                       + "|" + row.get("channel", "").strip().casefold())


def is_columbia(brand: str) -> bool:
    return brand.strip().casefold() in COLUMBIA_NAMES


def link(url: str, text: str) -> str:
    validated = safe_url(url)
    return (f'<a target="_blank" rel="noopener noreferrer" href="{html.escape(validated, quote=True)}">'
            f'{html.escape(text)}</a>') if validated else "미확인"


def build(listings_path: Path, sellers_path: Path, output: Path, max_age_days: int) -> dict:
    now = datetime.now(KST)
    output.mkdir(parents=True, exist_ok=True)
    listings = load_csv(listings_path, LISTING_FIELDS)
    seller_rows = load_csv(sellers_path, SELLER_FIELDS)
    profiles = {seller_key(r): r for r in seller_rows if r["seller_name"] or r["seller_id"]}
    dedup = set()
    normalized = []
    malformed_dates = 0
    for raw in listings:
        row = dict(raw)
        row["channel"] = inferred_channel(row["product_url"] or row["store_url"], row["channel"])
        row["product_url"] = safe_url(row["product_url"])
        row["store_url"] = safe_url(row["store_url"])
        row["source_url"] = safe_url(row["source_url"])
        observation = read_date(row["observed_at"])
        if observation is None:
            state = "날짜 미확인"
            malformed_dates += 1
        elif observation > now + timedelta(days=1):
            state = "미래 날짜 오류"
            malformed_dates += 1
        elif now - observation > timedelta(days=max_age_days):
            state = "과거 데이터"
        else:
            state = "최신 관측"
        row["freshness"] = state
        price = price_value(row["price_krw"])
        row["price_krw"] = price if price is not None else ""
        row["list_price_krw"] = price_value(row["list_price_krw"]) or ""
        row["shipping_fee_krw"] = price_value(row["shipping_fee_krw"]) or ""
        row["seller_identified"] = ("확인 필요" if (
            not row["seller_name"] or row["seller_name"].casefold() in GENERIC_MALLS
        ) else "식별")
        key = (seller_key(row), row["product_code"], row["product_url"], row["observed_at"])
        if key in dedup:
            continue
        dedup.add(key)
        normalized.append(row)

    current = [r for r in normalized if r["freshness"] == "최신 관측"]
    by_seller = defaultdict(list)
    for r in normalized:
        by_seller[seller_key(r)].append(r)
    for key in profiles:
        by_seller.setdefault(key, [])

    master = []
    for key, rows in by_seller.items():
        p = profiles.get(key, {})
        representative = next((r for r in rows if r.get("seller_name")), p)
        seller_name = p.get("seller_name") or representative.get("seller_name") or "판매자 미확인"
        channel = p.get("channel") or representative.get("channel") or ""
        recent = [r for r in rows if r["freshness"] == "최신 관측"]
        columbia_rows = [r for r in recent if is_columbia(r["brand"])]
        other_brands = Counter(r["brand"] for r in recent if r["brand"] and not is_columbia(r["brand"]))
        style_counts = Counter(r["product_code"] for r in columbia_rows if r["product_code"])
        prices = [r["price_krw"] for r in columbia_rows if isinstance(r["price_krw"], int)]
        last_seen_dates = [read_date(r["observed_at"]) for r in rows]
        last_seen_dates = [x for x in last_seen_dates if x is not None]
        business_address = p.get("business_address", "") if safe_url(p.get("business_info_url", "")) else ""
        return_address = p.get("return_address", "") if safe_url(p.get("return_info_url", "")) else ""
        master.append({
            "seller_name": seller_name,
            "seller_id": p.get("seller_id") or representative.get("seller_id", ""),
            "channel": channel,
            "business_name": p.get("business_name", ""),
            "business_registration_number": p.get("business_registration_number", ""),
            "business_address": business_address,
            "business_info_url": safe_url(p.get("business_info_url", "")),
            "return_address": return_address,
            "return_info_url": safe_url(p.get("return_info_url", "")),
            "store_url": safe_url(p.get("store_url") or representative.get("store_url", "")),
            "verified_at": p.get("verified_at", ""),
            "verification_status": p.get("verification_status", "") or "미확인",
            "last_observed_at": max(last_seen_dates).isoformat(timespec="minutes") if last_seen_dates else "",
            "historical_listings": len(rows),
            "current_columbia_listings": len(columbia_rows),
            "current_columbia_unique_codes": len(style_counts),
            "columbia_focus_codes": ", ".join(f"{code}({count})" for code, count in style_counts.most_common(5)),
            "columbia_min_price_krw": min(prices) if prices else "",
            "columbia_median_price_krw": int(median(prices)) if prices else "",
            "other_brands_observed": ", ".join(sorted(other_brands)),
            "other_brand_listing_count": sum(other_brands.values()),
            "address_status": ("사업자/반품지 확인"
                               if business_address and return_address else "주소 검증 필요"),
            "identity_status": "판매자 확인 필요" if seller_name.casefold() in GENERIC_MALLS or seller_name == "판매자 미확인" else "판매자명 수집",
        })

    master.sort(key=lambda r: (-r["current_columbia_listings"], -r["historical_listings"], r["seller_name"]))
    normalized.sort(key=lambda r: (r["freshness"] != "최신 관측", r["seller_name"], r["product_code"]))
    put_csv(output / "seller_master.csv", list(master[0].keys()) if master else
            ["seller_name", "seller_id", "channel", "business_name", "business_registration_number",
             "business_address", "business_info_url", "return_address", "return_info_url",
             "store_url", "verified_at", "verification_status", "last_observed_at",
             "historical_listings", "current_columbia_listings",
             "current_columbia_unique_codes", "columbia_focus_codes",
             "columbia_min_price_krw", "columbia_median_price_krw",
             "other_brands_observed", "other_brand_listing_count",
             "address_status", "identity_status"], master)
    put_csv(output / "listings_normalized.csv", LISTING_FIELDS + ["freshness", "seller_identified"], normalized)

    payload = {
        "generated_at_kst": now.isoformat(timespec="seconds"),
        "source_kind": "authorized_or_manual_listings_csv",
        "not_live_naver_feed": True,
        "max_age_days": max_age_days,
        "status": "REQUIRES_CURRENT_SOURCE" if not current else "CURRENT_SOURCE_IMPORTED",
        "total_sellers": len(master),
        "total_observed_listings": len(normalized),
        "current_listings": len(current),
        "current_columbia_listings": sum(is_columbia(r["brand"]) for r in current),
        "unverified_or_invalid_dates": malformed_dates,
        "not_sales_volume_note": "상품 수 및 상품코드 빈도는 수집된 노출 건수이며 실제 판매량이 아닙니다.",
    }
    (output / "meta.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    cards = "".join(f"<div class='card'><b>{html.escape(str(label))}</b><strong>{html.escape(str(value))}</strong></div>"
                    for label, value in [
                        ("판매자", len(master)), ("관측 상품", len(normalized)),
                        ("최근 관측 상품", len(current)),
                        ("컬럼비아 최근 상품", payload["current_columbia_listings"]),
                    ])
    tr = []
    for r in master:
        vals = [
            html.escape(r["seller_name"]), html.escape(r["channel"]),
            html.escape(r["business_name"] or "미확인"),
            html.escape(r["business_address"] or "미확인") + " " + link(r["business_info_url"], "근거"),
            html.escape(r["return_address"] or "미확인") + " " + link(r["return_info_url"], "근거"),
            str(r["current_columbia_listings"]),
            html.escape(r["columbia_focus_codes"] or "미확인"),
            html.escape(r["other_brands_observed"] or "미확인"),
            link(r["store_url"], "스토어"),
            html.escape(r["last_observed_at"] or "미확인"),
        ]
        tr.append("<tr>" + "".join("<td>" + v + "</td>" for v in vals) + "</tr>")
    headers = ["판매자", "채널", "사업자명", "사업자주소", "반품지주소",
               "컬럼비아 노출 상품", "주요 상품코드", "타 브랜드(관측)", "스토어", "마지막 관측"]
    state_message = (
        "최근 입력 데이터가 없습니다. 과거 최저가 리포트를 현재 가격으로 사용하지 마세요."
        if not current else
        "최근 관측 데이터가 입력되었습니다. 관측된 상품 수는 판매량이 아니며 판매자 정보는 출처 링크로 재확인해야 합니다."
    )
    page = f"""<!doctype html><html lang="ko"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>컬럼비아 리셀러 인텔리전스</title>
<style>body{{font:14px/1.6 system-ui,sans-serif;background:#f6f8fb;color:#182842;margin:0;padding:30px}}
main{{max-width:1500px;margin:auto}}h1{{margin:0 0 8px}}.subtitle{{color:#516078;margin-bottom:18px}}
.notice{{background:#fff7e3;border-left:4px solid #d79727;padding:14px;margin:20px 0}}
.cards{{display:flex;flex-wrap:wrap;gap:12px}}.card{{background:white;padding:18px;min-width:170px;flex:1;border-radius:10px}}
.card b{{display:block;color:#516078;font-size:12px}}.card strong{{font-size:26px}}
.scroll{{overflow:auto;background:white;border-radius:12px;margin:20px 0}}
table{{border-collapse:collapse;width:100%;min-width:1100px}}th,td{{padding:12px;border-bottom:1px solid #e7ebf3;text-align:left;vertical-align:top}}
th{{position:sticky;top:0;background:#eff4fa}}a{{color:#174e90}}
small{{color:#64748b}}</style></head><body><main>
<h1>컬럼비아 리셀러 인텔리전스</h1><div class="subtitle">관측 기준 데이터 · 생성 {html.escape(now.strftime("%Y-%m-%d %H:%M KST"))}</div>
<div class="notice"><b>{html.escape(payload["status"])}</b><div>{html.escape(state_message)}</div>
<small>쇼핑 검색 OpenAPI 2026-07-31 종료 · 최신 기준 {max_age_days}일 · 공개 사업자정보 및 반품지 출처 없는 주소는 미확인 처리</small></div>
<div class="cards">{cards}</div>
<div class="scroll"><table><thead><tr>{''.join('<th>'+html.escape(h)+'</th>' for h in headers)}</tr></thead>
<tbody>{''.join(tr) if tr else '<tr><td colspan="10">확인된 판매자 데이터가 없습니다.</td></tr>'}</tbody></table></div>
<p><a href="seller_master.csv">판매자 마스터 CSV</a> ·
<a href="listings_normalized.csv">상품별 관측 CSV</a> · <a href="meta.json">갱신 상태 JSON</a></p>
<small>수집된 상품코드·브랜드 빈도는 노출된 상품 데이터 기준이며 실제 판매량이나 시장점유율을 의미하지 않습니다.</small>
</main></body></html>"""
    (output / "index.html").write_text(page, encoding="utf-8")
    return payload


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--listings", type=Path, default=Path("data/reseller_intel/listings.csv"))
    p.add_argument("--sellers", type=Path, default=Path("data/reseller_intel/sellers.csv"))
    p.add_argument("--out", type=Path, default=Path("reports/reseller_intel"))
    p.add_argument("--max-age-days", type=int, default=3)
    a = p.parse_args()
    if a.max_age_days < 1:
        p.error("--max-age-days must be positive")
    print(json.dumps(build(a.listings, a.sellers, a.out, a.max_age_days), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
