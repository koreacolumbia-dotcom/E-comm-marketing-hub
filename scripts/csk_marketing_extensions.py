#!/usr/bin/env python3
"""Optional marketing source adapters: competitor, launch, creative, Creator Crew.

No customer/influencer individual-level records or platform mutations.
"""
from __future__ import annotations
import csv
import datetime as dt
import json
import math
import re
from pathlib import Path
from typing import Any

COMPETITOR = Path("reports/competitor_intel")
LAUNCH_REGISTRY = Path("config/product_launches_2026.json")
LAUNCH_SOURCE = Path("private_data/csk_signals/sku_daily_aggregate.json")
CREATIVE_SOURCE = Path("private_data/csk_signals/creative_daily_aggregate.json")
CREW_SOURCE = Path("private_data/csk_signals/creator_crew_aggregate.json")


def read(path: Path) -> dict[str, Any] | None:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else None
    except (ValueError, UnicodeError, OSError):
        return None


def num(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        x = float(value)
        return x if math.isfinite(x) else None
    except (ValueError, TypeError):
        return None


def day(value: Any) -> dt.date | None:
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def label(value: Any, size: int = 100) -> str:
    text = re.sub(r"[\x00-\x1f]+", " ", str(value or "")).strip()
    text = re.sub(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
                  "[redacted]", text)
    return text[:size]


def competitor_report(report_day: dt.date, source: Path = COMPETITOR) -> dict[str, Any]:
    result = {"status": "not_connected", "brands": [],
              "source": "reports/competitor_intel/brand_summary.csv",
              "note": "브랜드 평균 가격은 상품 구성·시즌·판매처가 달라 동일 SKU의 가격 차이를 증명하지 않음"}
    file = source / "brand_summary.csv"
    if not file.is_file():
        return result
    try:
        with file.open(encoding="utf-8-sig", newline="") as stream:
            rows = list(csv.DictReader(stream))
    except (UnicodeError, csv.Error, OSError):
        result["status"] = "parse_error"
        return result
    if not rows:
        return result
    for item in rows:
        brand = str(item.get("brand") or "")
        if not re.fullmatch(r"[A-Z_]{3,30}", brand):
            continue
        slug = brand.lower().replace("_", "-")
        checkpoint = read(source / f"checkpoint_{slug}_status.json")
        if not checkpoint or checkpoint.get("brand") != brand:
            continue
        date = day(checkpoint.get("updated_at"))
        completed = checkpoint.get("completed") is True
        count = num(checkpoint.get("saved_products"))
        total = num(item.get("total_products"))
        avg_price = num(item.get("avg_price"))
        active = num(item.get("active_products"))
        discount = num(item.get("sale_share_pct"))
        if (date is None or not completed or total is None or count is None or
                avg_price is None or active is None or discount is None or
                total <= 0 or count != total or active > total or
                not (0 <= discount <= 100)):
            continue
        lag = (report_day-date).days
        item_status = "recent" if -1 <= lag <= 3 else "stale"
        result["brands"].append({
            "brand": brand, "products": int(total), "active": int(active),
            "avg_price_krw": round(avg_price),
            "listed_sale_share_pct": round(discount, 1),
            "checkpoint_date": date.isoformat(),
            "status": item_status
        })
    result["brands"].sort(key=lambda x: (-x["products"], x["brand"]))
    result["status"] = (
        "reference_fresh" if len(result["brands"]) == len(rows) and
        all(x["status"] == "recent" for x in result["brands"]) else
        "partial_reference" if result["brands"] else "unverified"
    )
    return result


def launch_report(report_day: dt.date, registry: Path = LAUNCH_REGISTRY,
                  private_daily: Path = LAUNCH_SOURCE) -> dict[str, Any]:
    source = read(registry)
    entries = source.get("launches") if source else None
    if not isinstance(entries, list) or not entries:
        return {"status": "launch_registry_missing", "items": [],
                "note": "승인된 신상품 출시일·상품코드 원본 미등록"}
    metrics = read(private_daily)
    rows = metrics.get("rows") if metrics else None
    if not isinstance(rows, list):
        return {"status": "daily_sku_source_missing", "items": [],
                "note": "신상품 출시일만 있고 SKU별 일자별 PDP/주문/매출 실측 원천 없음"}
    index: dict[tuple[str, dt.date], dict] = {}
    duplicate_days: set[tuple[str, dt.date]] = set()
    for x in rows:
        if not isinstance(x, dict):
            continue
        date = day(x.get("date"))
        sku = str(x.get("sku") or "")
        ss, oo, rr = [num(x.get(k)) for k in ("pdp_sessions", "orders", "revenue")]
        if (date and re.fullmatch(r"[A-Z0-9_-]{5,40}", sku) and
                None not in (ss,oo,rr) and ss >= 0 and oo >= 0 and rr >= 0):
            if (sku,date) in index:
                duplicate_days.add((sku,date))
            else:
                index[(sku,date)] = {"sessions": ss, "orders": oo, "revenue": rr}
    data = []
    for entry in entries[:25]:
        if not isinstance(entry, dict):
            continue
        sku, launch = str(entry.get("sku") or ""), day(entry.get("launch_date"))
        if launch is None or not re.fullmatch(r"[A-Z0-9_-]{5,40}", sku) or launch > report_day:
            continue
        windows = {}
        for horizon in (7,14,30):
            last = launch + dt.timedelta(days=horizon-1)
            if last > report_day:
                windows[f"d{horizon}"] = {"status": "awaiting_window"}
                continue
            dates = [launch + dt.timedelta(days=i) for i in range(horizon)]
            values = [index.get((sku,d)) for d in dates]
            if any((sku,d) in duplicate_days for d in dates):
                windows[f"d{horizon}"] = {"status": "duplicate_sku_day"}
                continue
            if any(v is None for v in values):
                windows[f"d{horizon}"] = {"status": "missing_daily_data"}
                continue
            windows[f"d{horizon}"] = {
                "status": "complete", "pdp_sessions": sum(v["sessions"] for v in values),
                "orders": sum(v["orders"] for v in values),
                "revenue": round(sum(v["revenue"] for v in values),2)
            }
        data.append({"sku": sku, "name":label(entry.get("name")),
                     "launch_date": launch.isoformat(), "windows": windows})
    return {"status": "tracking" if data else "no_valid_launches", "items": data,
            "note": "7/14/30일 기간 모두 완결되지 않으면 수치 미표시. 제품 마진·광고 기여는 미측정"}


def creative_fatigue(report_day: dt.date, path: Path = CREATIVE_SOURCE) -> dict[str, Any]:
    obj = read(path)
    rows = obj.get("rows") if obj else None
    if not isinstance(rows, list):
        return {"status": "not_connected", "flags": [],
                "note": "Meta/Google 소재별 14일 노출·클릭 원본 미연결"}
    groups: dict[tuple[str,str], dict[str,list]] = {}
    seen_daily: set[tuple[str,str,dt.date]] = set()
    for entry in rows:
        if not isinstance(entry, dict):
            continue
        date = day(entry.get("date"))
        platform = label(entry.get("platform"),25).lower()
        creative = str(entry.get("creative_id") or "")
        exposures, clicks = num(entry.get("impressions")),num(entry.get("clicks"))
        if (date is None or not re.fullmatch(r"[A-Za-z0-9_-]{3,60}",creative)
                or platform not in {"meta","google","naver","criteo"}
                or exposures is None or clicks is None
                or exposures < 0 or not (0 <= clicks <= exposures)):
            continue
        lag = (report_day - date).days
        if not (0 <= lag <= 13):
            continue
        row = groups.setdefault((platform,creative), {"before": [], "after": []})
        if (platform,creative,date) in seen_daily:
            return {"status": "duplicate_creative_day", "flags": [],
                    "note": "동일 소재·매체·날짜 중복 원본. 소재 데이터 재집계 전 판단 차단"}
        seen_daily.add((platform,creative,date))
        bucket = "after" if lag <= 6 else "before"
        row[bucket].append((date,exposures,clicks))
    flags = []
    for (platform,creative),period in groups.items():
        if any(len({x[0] for x in period[k]}) < 7 for k in ("before","after")):
            continue
        pre_exp = sum(x[1] for x in period["before"])
        post_exp = sum(x[1] for x in period["after"])
        if min(pre_exp,post_exp) < 2000:
            continue
        pre_ctr = sum(x[2] for x in period["before"]) / pre_exp
        post_ctr = sum(x[2] for x in period["after"]) / post_exp
        if pre_ctr <= 0:
            continue
        change = (post_ctr/pre_ctr - 1)*100
        if change <= -25:
            flags.append({"platform":platform, "creative_id":creative,
                          "ctr_change_pct":round(change,1),
                          "status":"creative_fatigue_hypothesis",
                          "next_check":"타깃·노출 위치·입찰·빈도와 예산 변화 확인 전 소재 문제 단정 금지"})
    return {"status": "analyzed" if groups else "no_valid_rows", "flags": flags[:10],
            "note": "동일 소재 7일 완결 구간, 각 구간 2천 회 이상 노출 기준. CTR 하락은 소재 피로의 확정 원인 아님"}


def creator_crew(report_day: dt.date, path: Path = CREW_SOURCE) -> dict[str, Any]:
    obj = read(path)
    rows = obj.get("rows") if obj else None
    if not isinstance(rows, list):
        return {"status": "not_connected", "groups": [], "estimated_groups": 0,
                "note": "크리에이터별 원본 인사이트 비공개/미연결. 실제와 추정 성과를 섞지 않음"}
    groups, estimated = [], 0
    for x in rows:
        if not isinstance(x, dict):
            continue
        mission, form = label(x.get("mission"),30),label(x.get("format"),25).upper()
        creators, posts, views, engagements = [
            num(x.get(k)) for k in ("creator_count","posts","views","engagements")]
        if (form not in ("REEL","FEED") or not mission or
                None in (creators,posts,views,engagements) or
                creators < 3 or posts < 1 or views <= 0 or engagements < 0):
            continue
        if x.get("estimated") is True:
            estimated += 1
            continue
        cost = num(x.get("cash_cost_krw"))
        groups.append({
            "mission":mission, "format":form, "posts":int(posts),
            "views":round(views), "engagements":round(engagements),
            "engagements_per_view_pct":round(100*engagements/views,2),
            "cash_only_cpv_krw":round(cost/views,2) if cost is not None and cost >= 0 else None,
            "cost_note":"현금성 활동비만 반영. 제품 제공·운영 인건비 미반영",
            "actual_data":True
        })
    return {"status": "aggregate_available" if groups else "no_usable_aggregate",
            "groups":groups[:25], "estimated_groups":estimated,
            "note":"미션·릴스/피드별 최소 3명 집계만 공개; 개인 이름/연락처/콘텐츠 URL은 출력 안 함"}


def all_signals(date: dt.date) -> dict[str, Any]:
    return {
        "competitors": competitor_report(date),
        "launches": launch_report(date),
        "creatives": creative_fatigue(date),
        "creator_crew": creator_crew(date)
    }
