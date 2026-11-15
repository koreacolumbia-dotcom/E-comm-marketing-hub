#!/usr/bin/env python3
"""Read-only BigQuery probe; NEVER serializes member-level records."""
from __future__ import annotations
import argparse
import base64
import datetime as dt
import json
import os
import re
from pathlib import Path
from typing import Any

KST = dt.timezone(dt.timedelta(hours=9))
DEFAULT_OUT = Path("private_data/csk_signals/warehouse_status.json")
DEFAULT_CRM_OUT = Path("reports/private_aggregate_crm/segments.json")
MAX_STALE_DAYS = 3
MAX_BYTES_BILLED = 350 * 1024 * 1024


def safe_id(value: str, kind: str = "table") -> str:
    regex = r"[A-Za-z][A-Za-z0-9_-]*" if kind == "project" else r"[A-Za-z_][A-Za-z0-9_]*"
    if not re.fullmatch(regex, str(value)):
        raise ValueError("Invalid BigQuery identifier")
    return value


def table_id(project: str, dataset: str, table: str) -> str:
    return ".".join((safe_id(project, "project"), safe_id(dataset), safe_id(table)))


def quoted(identifier: str) -> str:
    parts = identifier.split(".")
    if len(parts) != 3:
        raise ValueError("Expected fully-qualified BQ table ID")
    table_id(*parts)
    return chr(96) + identifier + chr(96)


def freshness(table: Any, day: dt.date, needed: set[str]) -> str:
    fields = {x.name.lower() for x in table.schema}
    if not needed.issubset(fields):
        return "schema_mismatch"
    updated = getattr(table, "modified", None)
    if updated is None:
        return "stale"
    change_date = updated.astimezone(KST).date()
    if change_date > day + dt.timedelta(days=1) or (day - change_date).days > MAX_STALE_DAYS:
        return "stale"
    return "ready"


def read_bq_client() -> Any:
    raw = os.environ.get("GOOGLE_SA_JSON_B64", "").strip()
    if not raw:
        return None
    from google.cloud import bigquery
    from google.oauth2 import service_account
    payload = json.loads(base64.b64decode(raw))
    creds = service_account.Credentials.from_service_account_info(payload)
    return bigquery.Client(credentials=creds, project=os.environ.get("BQ_PROJECT") or payload["project_id"],
                           location=os.environ.get("BQ_LOCATION", "asia-northeast3"))


def scalar_query(client: Any, sql: str, date: dt.date,
                 max_bytes: int = MAX_BYTES_BILLED) -> dict[str, Any]:
    from google.cloud import bigquery
    cfg = bigquery.QueryJobConfig(
        maximum_bytes_billed=max_bytes, use_query_cache=True,
        query_parameters=[bigquery.ScalarQueryParameter("report_date", "DATE", date.isoformat())])
    query = client.query(sql, job_config=cfg,
                         location=os.environ.get("BQ_LOCATION", "asia-northeast3"))
    return dict(next(iter(query.result())))


def probe(day: dt.date, client: Any, project: str, mart_dataset: str,
          raw_dataset: str, order_table: str, *,
          query_run=scalar_query) -> tuple[dict[str, Any], dict[str, Any] | None]:
    report: dict[str, Any] = {
        "date": day.isoformat(), "order_mart": {"status": "not_checked"},
        "inventory": {"status": "not_checked"}, "crm": {"status": "not_checked"}
    }
    local_crm = None
    try:
        order_fqn = table_id(project, mart_dataset, order_table)
        order_meta = client.get_table(order_fqn)
        state = freshness(order_meta, day, {"product_code", "order_date",
                                            "net_erp_revenue", "purchase_qty"})
        if state == "ready":
            count = query_run(client, f"""
                SELECT COUNT(*) AS recent_lines,
                       MAX(DATE(order_date)) AS latest_order
                FROM {quoted(order_fqn)}
                WHERE DATE(order_date) BETWEEN DATE_SUB(@report_date, INTERVAL 3 DAY)
                      AND @report_date
            """, day)
            latest = count.get("latest_order")
            if not count.get("recent_lines") or latest is None or (day - latest).days > 2:
                state = "stale"
        report["order_mart"] = {"status": state}
        # Stock values in an order-line mart represent only ordered SKUs/sizes,
        # not the complete live inventory of the product catalogue.
        report["inventory"] = {
            "status": "incomplete" if "size_stock_qty" in
            {x.name.lower() for x in order_meta.schema} else "schema_mismatch"
        }
    except Exception:
        report["order_mart"] = {"status": "query_error"}
        report["inventory"] = {"status": "not_checked"}
    try:
        member_fqn = table_id(project, raw_dataset, "tb_member_staging")
        header_fqn = table_id(project, raw_dataset, "tb_order_header_staging")
        member_meta = client.get_table(member_fqn)
        header_meta = client.get_table(header_fqn)
        st1 = freshness(member_meta, day, {"memberid", "memberregdate"})
        st2 = freshness(header_meta, day, {"memberid", "orderno",
                                            "orderregdate", "orderstatusno"})
        status = "ready" if st1 == st2 == "ready" else (
            "schema_mismatch" if "schema_mismatch" in (st1, st2) else "stale")
        if status == "ready":
            summary = query_run(client, f"""
                WITH members AS (
                    SELECT TRIM(CAST(MemberID AS STRING)) AS member_key,
                           MIN(DATE(MemberRegdate)) AS registered
                    FROM {quoted(member_fqn)}
                    WHERE MemberID IS NOT NULL AND MemberRegdate IS NOT NULL
                    GROUP BY 1
                ), order_summary AS (
                    SELECT TRIM(CAST(MemberID AS STRING)) AS member_key,
                           COUNT(DISTINCT CAST(OrderNo AS STRING)) AS order_count,
                           MAX(DATE(OrderRegdate)) AS latest_order
                    FROM {quoted(header_fqn)}
                    WHERE MemberID IS NOT NULL AND OrderRegdate IS NOT NULL
                      AND SAFE_CAST(OrderStatusNo AS INT64) IN (4,5)
                    GROUP BY 1
                )
                SELECT
                    COUNTIF(m.registered <= DATE_SUB(@report_date, INTERVAL 30 DAY)
                            AND COALESCE(o.order_count, 0) = 0) AS signup_30d_no_purchase,
                    COUNTIF(o.order_count = 1
                            AND o.latest_order <= DATE_SUB(@report_date, INTERVAL 60 DAY)) AS one_time_60d,
                    MAX(m.registered) AS max_member_date,
                    COUNT(*) AS member_rows
                FROM members AS m
                LEFT JOIN order_summary AS o USING(member_key)
            """, day)
            last_member = summary.get("max_member_date")
            if (not summary.get("member_rows") or last_member is None or
                    (day - last_member).days > 7):
                status = "stale"
            else:
                # Cohort counts are private and short-lived. Public dashboard
                # receives readiness state only, without member counts.
                rows = []
                for key, name in (("signup_30d_no_purchase", "가입 후 30일 미구매"),
                                  ("one_time_60d", "첫 구매 후 60일 재구매 없음")):
                    count = summary.get(key)
                    if count is not None and int(count) >= 30:
                        rows.append({"segment": name, "member_count": int(count)})
                if rows:
                    local_crm = {"as_of": day.isoformat(), "segments": rows,
                                 "caveat": "원천 ETL의 가입·주문 적재 범위 내 회원만 포함"}
                else:
                    status = "incomplete"
        report["crm"] = {"status": status}
    except Exception:
        report["crm"] = {"status": "query_error"}
    return report, local_crm


def write_probe(report: dict[str, Any], crm: dict[str, Any] | None,
                out: Path, crm_out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if crm:
        crm_out.parent.mkdir(parents=True, exist_ok=True)
        crm_out.write_text(json.dumps(crm, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print("Warehouse read-only probe:", report["date"],
          report["order_mart"]["status"], report["inventory"]["status"],
          report["crm"]["status"], "(no member counts logged)")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=(dt.datetime.now(KST).date() -
                                           dt.timedelta(days=1)).isoformat())
    parser.add_argument("--out", default=str(DEFAULT_OUT))
    parser.add_argument("--crm-out", default=str(DEFAULT_CRM_OUT))
    args = parser.parse_args()
    day = dt.date.fromisoformat(args.date)
    if not os.environ.get("GOOGLE_SA_JSON_B64", ""):
        report = {"date": day.isoformat(),
                  "order_mart": {"status": "auth_missing"},
                  "inventory": {"status": "not_checked"},
                  "crm": {"status": "auth_missing"}}
        write_probe(report, None, Path(args.out), Path(args.crm_out))
        return 0
    try:
        client = read_bq_client()
        report, crm = probe(
            day, client, os.environ.get("BQ_PROJECT", "columbia-ga4"),
            os.environ.get("BQ_MART_DATASET", "crm_mart"),
            os.environ.get("BQ_RAW_DATASET", "crm_raw"),
            os.environ.get("BQ_ORDER_PRODUCT_TABLE", "tb_order_product_search_mart"))
    except Exception:
        report = {"date": day.isoformat(),
                  "order_mart": {"status": "query_error"},
                  "inventory": {"status": "not_checked"},
                  "crm": {"status": "query_error"}}
        crm = None
    write_probe(report, crm, Path(args.out), Path(args.crm_out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
