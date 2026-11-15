#!/usr/bin/env python3
"""Idempotent, allowlisted GitHub issue creation for CSK recovery and product QA.

Only creates issues; never closes, reopens, deletes, assigns usernames,
relabels, or overwrites human-authored content. Public-safe text only.
"""
from __future__ import annotations
import argparse
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

ALLOWED = {
    "PAID_COST_FRESHNESS": "매체 광고비 파일 최신화 및 원천 대조",
    "WAREHOUSE_ETL_FRESHNESS": "BigQuery 주문·CRM ETL 적재 신선도 정상화",
    "MSSQL_STOCK_ACCESS": "MSSQL 실재고 읽기 전용 접근 복구",
    "CHANNEL_QA": "GA4 채널 세션 합계·ETC 귀속 정합성 검증",
    "PDP_QA": "PDP 최신일·상품 구매 이벤트 수집 검증"
}
MARKER_PATTERN = re.compile(r"<!-- CSKOPS-ID: ([A-Z0-9_-]{3,70}) -->")
MAX_CREATE = 7
DEFAULT_REPORT = Path("reports/marketing_operations/latest.json")


def marker(identifier: str) -> str:
    if not re.fullmatch(r"[A-Z0-9_-]{3,70}", identifier):
        raise ValueError("Unsafe issue ID")
    return "<!-- CSKOPS-ID: " + identifier + " -->"


def candidates(report: dict[str, Any]) -> list[dict[str, str]]:
    result = []
    for item in report.get("recovery", []):
        if not isinstance(item, dict):
            continue
        key = str(item.get("code") or "")
        if key not in ALLOWED:
            continue
        result.append({
            "id": key, "title": ALLOWED[key],
            "owner": str(item.get("owner") or "데이터/마케팅 담당")[:60],
            "context": ALLOWED[key],
            "check": str(item.get("next_step") or "원천 데이터 재검증")[:400],
        })
    product_count = 0
    for item in report.get("products", {}).get("items", []):
        if not isinstance(item, dict) or item.get("state") != "MEASUREMENT_QA":
            continue
        sku = str(item.get("code") or "")
        if not re.fullmatch(r"C[A-Z0-9_-]{5,35}", sku):
            continue
        if product_count >= 2:
            break
        result.append({
            "id": "PRODUCT_" + sku,
            "title": "상품 구매 이벤트 정합성 검증: " + sku,
            "owner": "데이터/이커머스",
            "context": "GA4 구매 이벤트 상품코드와 어드민 주문 SKU 매핑 점검",
            "check": "view_item/purchase의 item_id와 어드민 주문상품 코드를 동일 기간·동일 세션 정의로 확인",
        })
        product_count += 1
    dedup = {x["id"]: x for x in result}
    return list(dedup.values())[:MAX_CREATE]


def body(item: dict[str, str]) -> str:
    return ("\n".join([
        marker(item["id"]),
        "## CSK 데이터 운영 점검",
        "**담당 조직:** " + item["owner"],
        "**확인 대상:** " + item["context"],
        "",
        "### 검증 단계",
        "- [ ] 원천의 최신 날짜와 동일 KPI 정의 확인",
        "- [ ] 문제 원인 확인 및 사전 백업/복구 영향 검토",
        "- [ ] 복구 후 데이터를 재조회하여 신뢰성 검증",
        "- [ ] 기존 Daily Digest·2.0 보고서와 정합성 확인",
        "",
        "**구체적인 검증:** " + item["check"],
        "",
        "**안전 제한:** 실제 운영 ETL의 WRITE_TRUNCATE, 광고비 변경, CRM 발송은 자동 실행하지 않습니다.",
        "",
        "근거: https://github.com/koreacolumbia-dotcom/E-comm-marketing-hub/blob/main/reports/marketing_operations/latest.md",
        "",
        "이 이슈는 중복 생성되지 않으며 해결/종료는 담당자의 확인 후 수동 진행합니다.",
    ])) + "\n"


class GithubApi:
    def __init__(self, repo: str, token: str):
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repo):
            raise ValueError("Invalid repository")
        if not token:
            raise ValueError("Missing GITHUB_TOKEN")
        self.repo = repo
        self.token = token

    def request(self, method: str, url: str, payload: dict | None = None) -> Any:
        raw = (json.dumps(payload, ensure_ascii=False).encode("utf-8")
               if payload is not None else None)
        req = urllib.request.Request(
            "https://api.github.com/repos/" + self.repo + url,
            data=raw, method=method,
            headers={"Accept": "application/vnd.github+json",
                     "Authorization": "Bearer " + self.token,
                     "X-GitHub-Api-Version": "2022-11-28",
                     "User-Agent": "csk-marketing-operations",
                     "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=25) as response:
            return json.load(response)

    def list_issues(self) -> list[dict]:
        result = []
        for page in range(1, 6):
            rows = self.request("GET", f"/issues?state=all&per_page=100&page={page}")
            if not isinstance(rows, list):
                raise ValueError("Invalid GitHub issues payload")
            result.extend(x for x in rows if isinstance(x, dict)
                          and not x.get("pull_request"))
            if len(rows) < 100:
                return result
        raise RuntimeError("Issue listing exceeded 500; refuse to create duplicates")

    def create_issue(self, title: str, content: str) -> dict:
        return self.request("POST", "/issues", {"title": title, "body": content})


def sync(report: dict[str, Any], api: Any | None = None,
         enabled: bool = False) -> dict[str, Any]:
    issues = candidates(report)
    if not enabled:
        return {"status": "dry_run", "candidates": [x["id"] for x in issues],
                "created": [], "skipped_existing": [], "skipped_closed": []}
    if api is None:
        raise ValueError("GitHub issue sync API unavailable")
    existing = {}
    for issue in api.list_issues():
        match = MARKER_PATTERN.search(str(issue.get("body") or ""))
        if match:
            existing[match.group(1)] = issue
    created, opened, closed = [], [], []
    for item in issues:
        known = existing.get(item["id"])
        if known is not None:
            if known.get("state") == "closed":
                closed.append(item["id"])
            else:
                opened.append(item["id"])
            continue
        out = api.create_issue("[CSK-OPS] " + item["title"], body(item))
        created.append({"id": item["id"], "number": out.get("number"),
                        "url": out.get("html_url")})
    return {"status": "synced", "candidates": [x["id"] for x in issues],
            "created": created, "skipped_existing": opened, "skipped_closed": closed}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", default=str(DEFAULT_REPORT))
    ap.add_argument("--sync", action="store_true",
                    help="Create open recovery issues using GitHub token")
    args = ap.parse_args()
    obj = json.loads(Path(args.report).read_text(encoding="utf-8"))
    api = (GithubApi(os.environ.get("GITHUB_REPOSITORY", ""),
                     os.environ.get("GH_TOKEN", ""))
           if args.sync else None)
    res = sync(obj, api, enabled=args.sync)
    print("[CSK OPS GitHub Issues]", json.dumps(res, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
