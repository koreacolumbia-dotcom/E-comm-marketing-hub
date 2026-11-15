#!/usr/bin/env python3
"""Read-only MSSQL inventory probe for CSK.

No SQL mutation, order/member export, price/stock quantity publication, or secrets in logs.
The current-stock observation is NOT a historical snapshot for prior days.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
from pathlib import Path
from typing import Any

KST = dt.timezone(dt.timedelta(hours=9))
OUT = Path("private_data/csk_signals/inventory_status.json")
PDP = Path("reports/pdp_opportunity/data.json")
ALLOWED_STATUS = {"auth_missing", "driver_missing", "connection_error",
                  "schema_mismatch", "ready", "no_matching_rows", "query_error"}


def sku_code(value: Any) -> str:
    return re.sub(r"[^A-Z0-9_-]", "", str(value or "").upper())[:40]


def candidate_skus(path: Path = PDP, max_skus: int = 8) -> list[str]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        candidates = data.get("products") or []
        if not isinstance(candidates, list):
            return []
        valid = []
        for p in candidates:
            if not isinstance(p, dict):
                continue
            sku = sku_code(p.get("product_code"))
            if not sku or sku in valid:
                continue
            score = p.get("opportunity_score", 0)
            if not isinstance(score, (int, float)) or score < 60:
                continue
            valid.append(sku)
            if len(valid) >= max_skus:
                break
        return valid
    except (OSError, UnicodeError, ValueError, TypeError):
        return []


def analyze_stock(rows: list[tuple[Any, Any, Any]], requested_skus: list[str]) -> dict[str, Any]:
    requested = set(requested_skus)
    matched = set()
    positive = set()
    zero_only = set()
    for code, sku_count, positive_sizes in rows:
        sku = sku_code(code)
        if sku not in requested:
            continue
        try:
            sizes, available_sizes = int(sku_count), int(positive_sizes)
        except (TypeError, ValueError):
            continue
        if sizes <= 0 or available_sizes < 0 or available_sizes > sizes:
            continue
        matched.add(sku)
        if available_sizes > 0:
            positive.add(sku)
        else:
            zero_only.add(sku)
    return {
        "sampled_sku_count": len(requested),
        "matched_sku_count": len(matched),
        # These lists remain solely in the private ephemeral runner file;
        # published report uses coarse readiness only.
        "detail": {
            "some_sizes_in_stock": sorted(positive),
            "no_positive_sizes_in_source": sorted(zero_only),
            "not_in_stock_table": sorted(requested - matched),
        },
    }


def connection_config() -> tuple[str, str, str, str] | None:
    server = (os.environ.get("MSSQL_SERVER") or
              os.environ.get("MSSQL_HOST") or "").strip()
    database = os.environ.get("MSSQL_DATABASE", "").strip()
    user = (os.environ.get("MSSQL_USERNAME") or
            os.environ.get("MSSQL_USER") or "").strip()
    password = (os.environ.get("MSSQL_PASSWORD") or
                os.environ.get("MSSQL_PWD") or "").strip()
    if not all((server, database, user, password)):
        return None
    return server, database, user, password


def probe_inventory(day: dt.date, skus: list[str]) -> dict[str, Any]:
    result = {
        "date": day.isoformat(), "as_of_kst": dt.datetime.now(KST).isoformat(timespec="seconds"),
        "status": "not_checked", "sampled_sku_count": len(skus),
        "matched_sku_count": 0, "coverage": "top_PDP_SKUs_only",
        "source": "MSSQL dbo.TB_ProductSizeStock", "reason": None,
    }
    settings = connection_config()
    if not settings:
        result.update(status="auth_missing", reason="MSSQL secrets not available")
        return result
    try:
        import pyodbc
    except ImportError:
        result.update(status="driver_missing", reason="pyodbc not installed")
        return result
    if not skus:
        result.update(status="no_matching_rows", reason="No valid candidate product SKUs")
        return result
    server, database, username, password = settings
    port = os.environ.get("MSSQL_PORT") or "1433"
    if "," not in server:
        server = server + "," + port
    connection = (
        "DRIVER={ODBC Driver 18 for SQL Server};SERVER=" + server +
        ";DATABASE=" + database + ";UID=" + username + ";PWD=" + password +
        ";TrustServerCertificate=yes;"
    )
    try:
        conn = pyodbc.connect(connection, timeout=20, autocommit=True)
    except Exception as exc:
        sqlstate = ""
        if getattr(exc, "args", ()) and isinstance(exc.args[0], str):
            candidate = exc.args[0].strip().upper()
            sqlstate = candidate if re.fullmatch(r"[A-Z0-9]{5}", candidate) else ""
        result.update(status="connection_error", reason=type(exc).__name__,
                      sqlstate=sqlstate or "unspecified")
        return result
    try:
        cur = conn.cursor()
        cur.timeout = 30
        col_rows = cur.execute(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'TB_ProductSizeStock'"
        ).fetchall()
        cols = {str(r[0]).lower() for r in col_rows}
        if not {"productcode", "productsize", "productstock"}.issubset(cols):
            result.update(status="schema_mismatch", reason="Missing product_code/size/stock column")
            return result
        # Fully parameterized read only. Query values are product SKUs from
        # the existing aggregate PDP report, never user-supplied SQL.
        placeholders = ", ".join("?" for _ in skus)
        sql = (
            "SELECT UPPER(LTRIM(RTRIM(CAST(ProductCode AS varchar(100))))) AS sku, "
            "COUNT(DISTINCT CAST(ProductSize AS varchar(50))) AS size_count, "
            "COUNT(DISTINCT CASE WHEN ISNULL(ProductStock, 0) > 0 "
            "THEN CAST(ProductSize AS varchar(50)) END) AS positive_size_count "
            "FROM dbo.TB_ProductSizeStock "
            "WHERE ProductCode IN (" +
            placeholders + ") "
            "GROUP BY UPPER(LTRIM(RTRIM(CAST(ProductCode AS varchar(100)))))"
        )
        rows = [tuple(x) for x in cur.execute(sql, skus).fetchall()]
        summary = analyze_stock(rows, skus)
        result["matched_sku_count"] = summary["matched_sku_count"]
        result["status"] = "ready" if summary["matched_sku_count"] else "no_matching_rows"
        # Keep SKU-level signals in private memory only; do NOT persist SKU
        # stock statuses, business inventory counts or price information.
        result["reason"] = "current snapshot; historical stock not inferred"
    except Exception as exc:
        result.update(status="query_error", reason=type(exc).__name__)
    finally:
        conn.close()
    return result


def write_result(status: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(status, ensure_ascii=False, indent=2) + "\n",
                    encoding="utf-8")
    print("MSSQL stock source probe:", status.get("date"), status.get("status"),
          "coverage:", status.get("coverage"),
          "error_kind:", status.get("reason") if status.get("status") != "ready" else "-",
          "sqlstate:", status.get("sqlstate", "-"),
          "(no inventory quantities, server names or credentials emitted)")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=(dt.datetime.now(KST).date() -
                                           dt.timedelta(days=1)).isoformat())
    parser.add_argument("--pdp", default=str(PDP))
    parser.add_argument("--out", default=str(OUT))
    args = parser.parse_args()
    day = dt.date.fromisoformat(args.date)
    result = probe_inventory(day, candidate_skus(Path(args.pdp)))
    write_result(result, Path(args.out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
