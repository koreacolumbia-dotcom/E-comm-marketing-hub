#!/usr/bin/env python3
"""Evidence checks for Paid costs, warehouse bridge, and intervention outcomes.

No customer-level records, provider credentials, campaign changes, or external send.
"""
from __future__ import annotations
import csv
import datetime as dt
import json
import math
from pathlib import Path
from typing import Any

PAID_FILE = Path("reports/daily_digest/data/paid_media_spend_history.csv")
WAREHOUSE_FILE = Path("private_data/csk_signals/warehouse_status.json")
STOCK_FILE = Path("private_data/csk_signals/inventory_status.json")
EXPERIMENT_FILE = Path("config/marketing_intelligence_experiments.json")
DAILY = Path("reports/daily_digest/data/daily")
MAX_COST_LAG_DAYS = 2


def numeric(v: Any) -> float | None:
    try:
        x = float(v)
        return x if math.isfinite(x) else None
    except (ValueError, TypeError):
        return None


def iso_day(v: Any) -> dt.date | None:
    try:
        return dt.date.fromisoformat(str(v)[:10])
    except (ValueError, TypeError):
        return None


def read_json(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else None
    except (OSError, UnicodeError, ValueError):
        return None


def paid_spend_health(report_day: dt.date, path: Path = PAID_FILE) -> dict[str, Any]:
    result = {
        "status": "not_connected", "latest_date": None, "lag_days": None,
        "source": str(path), "rows": 0, "providers": [], "current_spend": None,
        "roas_eligible": False, "issues": [],
    }
    if not path.is_file():
        result["issues"].append("광고비 내역 파일이 없으며 API·매체 계정 데이터도 미연결")
        return result
    records, uniq = [], set()
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream)
            if not {"date", "channel", "sub_channel", "spend"}.issubset(reader.fieldnames or []):
                result.update(status="invalid_schema", issues=["광고비 CSV 필수 컬럼 부족"])
                return result
            for row in reader:
                day = iso_day(row.get("date"))
                val = numeric(row.get("spend"))
                chan = str(row.get("channel") or "").strip().lower()
                sub = str(row.get("sub_channel") or "").strip().lower()
                if day is None or val is None or val < 0 or not chan or not sub:
                    result.update(status="invalid_rows", issues=["광고비 CSV 날짜·음수·채널·비용 오류"])
                    return result
                key = (day, chan, sub)
                if key in uniq:
                    result.update(status="duplicate_rows", issues=["동일 날짜·매체 데이터 중복"])
                    return result
                uniq.add(key)
                if day > report_day:
                    result.update(status="future_rows", issues=["보고일 이후 비용 행 존재. 파일 검증 실패"])
                    return result
                records.append((day, chan, sub, val))
    except (OSError, UnicodeError, csv.Error):
        result.update(status="read_error", issues=["광고비 파일 열기 실패"])
        return result
    result["rows"] = len(records)
    if not records:
        result.update(status="empty", issues=["광고비 내역이 비어 있음"])
        return result
    latest = max(r[0] for r in records)
    lag = (report_day - latest).days
    result.update(latest_date=latest.isoformat(), lag_days=lag,
                  providers=sorted({r[1] for r in records if r[0] == latest}))
    if lag > MAX_COST_LAG_DAYS:
        result["status"] = "stale"
        result["issues"].append(
            f"마지막 광고비 {latest.isoformat()} (보고일 대비 {lag}일 지연) — 광고 효율 자동 판단 중지")
        return result
    today = [r for r in records if r[0] == report_day]
    if not today:
        result.update(status="date_gap", issues=["기준일 광고비 행 없음"])
        return result
    result["status"] = "fresh_unreconciled"
    result["current_spend"] = round(sum(r[3] for r in today), 2)
    result["issues"].append(
        "비용 CSV 신선도는 확인됐으나 광고 플랫폼 원천 대조·매체 코드 귀속 검증 전 — ROAS 계산 보류")
    return result


def warehouse_health(day: dt.date, path: Path = WAREHOUSE_FILE,
                     stock_path: Path = STOCK_FILE) -> dict[str, Any]:
    result = {
        "status": "not_connected", "order_mart": "not_checked",
        "crm": "not_checked", "inventory": "not_checked",
        "product_source_valid": False, "crm_source_valid": False,
        "report_date": day.isoformat(), "notes": [],
    }
    obj = read_json(path)
    if obj is None:
        result["notes"].append("BigQuery 연결 결과 파일 없음: 정기 진단 작업에서 소스 확인 필요")
        return result
    if obj.get("date") != day.isoformat():
        result["status"] = "stale_probe"
        result["notes"].append("BigQuery 검증 결과 기준일이 보고일과 다름")
        return result
    for feature in ("order_mart", "crm", "inventory"):
        row = obj.get(feature) or {}
        if isinstance(row, dict):
            st = str(row.get("status") or "not_checked")
            result[feature] = st if st in {"ready", "stale", "missing", "schema_mismatch",
                                          "not_checked", "query_error", "incomplete", "auth_missing"} else "query_error"
    stock = read_json(stock_path)
    if stock is not None and stock.get("date") == day.isoformat():
        state = stock.get("status")
        asof = iso_day(stock.get("as_of_kst"))
        if state == "ready" and asof is not None and asof >= day:
            result["inventory"] = "ready_current_only"
            result["notes"].append(
                "실재고 원본은 MSSQL에서 현재 시점 조회됨. 보고일의 과거 재고·매출 감소 원인으로 단정 금지")
        elif state in {"auth_missing", "driver_missing", "connection_error",
                       "schema_mismatch", "no_matching_rows", "query_error"}:
            result["inventory"] = state
    result["product_source_valid"] = result["order_mart"] == "ready"
    result["crm_source_valid"] = result["crm"] == "ready"
    result["status"] = ("ready" if result["product_source_valid"] and result["crm_source_valid"]
                        else "partial")
    if result["order_mart"] != "ready":
        result["notes"].append("상품 어드민 주문 데이터로 매출 하락 원인을 분해할 수 없음")
    if result["inventory"] not in ("ready", "ready_current_only"):
        result["notes"].append("완전한 실재고 원본 미확인 — 품절을 실제 원인으로 단정하지 않음")
    if result["crm"] != "ready":
        result["notes"].append("CRM 원본 집계 불충분 — 고객 타깃 인원 추정 금지")
    return result


def _measure(days: list[dt.date], root: Path) -> dict[str, float] | None:
    rows = []
    for date in days:
        doc = read_json(root / f"{date.isoformat()}.json")
        if doc is None or doc.get("end_date") != date.isoformat():
            return None
        kpi = doc.get("kpis")
        if not isinstance(kpi, dict):
            return None
        sessions = numeric(kpi.get("sessions"))
        orders = numeric(kpi.get("orders"))
        revenue = numeric(kpi.get("revenue"))
        if sessions is None or orders is None or revenue is None:
            return None
        if not (sessions > 0 and orders >= 0 and revenue >= 0 and orders <= sessions):
            return None
        # Refuse KPI data if GA4 aggregation itself failed even though raw JSON exists.
        if doc.get("mode") not in (None, "daily"):
            return None
        rows.append((sessions, orders, revenue))
    ss, oo, rr = map(sum, zip(*rows))
    return {"sessions": round(ss), "orders": round(oo),
            "revenue": round(rr, 2), "cvr": oo / ss if ss else None,
            "aov": rr / oo if oo else None, "days": len(rows)}


def experiment_results(report_day: dt.date, config: Path = EXPERIMENT_FILE,
                       root: Path = DAILY) -> dict[str, Any]:
    obj = read_json(config)
    result = {"status": "not_configured", "items": [],
              "notes": ["사전 등록된 실행/개선 실험 없음 — 자동으로 개선 효과를 주장하지 않음"]}
    if obj is None:
        return result
    specs = obj.get("experiments")
    if not isinstance(specs, list):
        result["status"] = "invalid_config"
        return result
    if not specs:
        return result
    result.update(status="configured", notes=[
        "집계 전후 비교는 외부 통제군·동시 캠페인·요일 효과가 제거되지 않아 인과 효과 아님",
        "동일 요일 조건을 맞추기 위해 관측 창은 7일 또는 14일만 허용",
    ])
    for entry in specs[:30]:
        if not isinstance(entry, dict):
            continue
        ref = str(entry.get("id") or "").strip()
        start = iso_day(entry.get("start_date"))
        window = entry.get("window_days", 7)
        if not (ref and start and isinstance(window, int) and window in (7, 14)):
            continue
        label = str(entry.get("title") or ref)[:100]
        row = {"id": ref[:70], "title": label, "start_date": start.isoformat(),
               "window_days": window, "status": "awaiting_full_window",
               "observed_improvement": None, "causal_claim": False}
        last = start + dt.timedelta(days=window - 1)
        if last > report_day:
            result["items"].append(row)
            continue
        previous_days = [start - dt.timedelta(days=window) + dt.timedelta(days=i)
                         for i in range(window)]
        next_days = [start + dt.timedelta(days=i) for i in range(window)]
        pre, post = _measure(previous_days, root), _measure(next_days, root)
        if pre is None or post is None:
            row["status"] = "insufficient_or_invalid_daily_data"
            result["items"].append(row)
            continue
        row["status"] = "observational_only"
        row["before"] = pre
        row["after"] = post
        row["observed_improvement"] = {
            "orders_pct": round((post["orders"] / pre["orders"] - 1) * 100, 2)
            if pre["orders"] > 0 else None,
            "revenue_pct": round((post["revenue"] / pre["revenue"] - 1) * 100, 2)
            if pre["revenue"] > 0 else None,
            "cvr_change_pp": round((post["cvr"] - pre["cvr"]) * 100, 3)
        }
        result["items"].append(row)
    return result


def build(day: dt.date, *, paid_path: Path = PAID_FILE,
          warehouse_path: Path = WAREHOUSE_FILE, experiment_file: Path = EXPERIMENT_FILE,
          daily_root: Path = DAILY) -> dict[str, Any]:
    return {
        "paid_cost": paid_spend_health(day, paid_path),
        "warehouse": warehouse_health(day, warehouse_path),
        "experiment": experiment_results(day, experiment_file, daily_root),
        "source_policy": "기준일 일치·검증된 집계만 사용하고, ROAS와 인과 개선 효과를 추정하지 않음",
    }
