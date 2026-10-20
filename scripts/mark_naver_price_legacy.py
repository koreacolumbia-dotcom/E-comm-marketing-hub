#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Mark the legacy Naver lowest-price HTML as an archive, never as current data.

Naver discontinued the Search > Shopping API on 2026-07-31.
https://developers.naver.com/notice/article/32564
The output is intentionally not refreshed with fake or cached current prices.
"""
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import json
import re

HTML_FILE = Path("reports/naver_lowest_price.html")
META_FILE = Path("reports/naver_price_status.json")
MARKER = "<!-- NAVER_PRICE_ARCHIVED_20260731 -->"
BANNER = """<section role="alert" style="position:sticky;top:0;z-index:999999;padding:18px 24px;
background:#fff3cd;border-bottom:4px solid #b45309;color:#78350f;font:600 15px/1.5 system-ui,sans-serif">
<strong>주의: 이 리포트는 과거 자료입니다.</strong>
네이버 쇼핑 검색 OpenAPI가 2026년 7월 31일 종료되어 현재 가격을 수집하지 못합니다.
화면에 표시된 상품 가격은 이전 저장 데이터일 수 있으며, 현재 최저가·판매자 현황으로 사용하면 안 됩니다.
<a href="https://developers.naver.com/notice/article/32564" target="_blank" rel="noopener noreferrer">
공식 종료 공지 확인</a></section>"""


def main():
    now = datetime.now(ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")
    META_FILE.parent.mkdir(parents=True, exist_ok=True)
    has_legacy = HTML_FILE.exists()
    if has_legacy:
        content = HTML_FILE.read_text(encoding="utf-8-sig")
        if MARKER not in content:
            content, count = re.subn(
                r"(<body\b[^>]*>)", lambda m: m.group(1) + "\n" + MARKER + "\n" + BANNER,
                content, count=1, flags=re.IGNORECASE,
            )
            if count != 1:
                raise SystemExit("Cannot locate HTML body; archive marking aborted")
            HTML_FILE.write_text(content, encoding="utf-8-sig")
    status = {
        "status": "ARCHIVED_NOT_CURRENT",
        "last_checked_kst": now,
        "legacy_html_exists": has_legacy,
        "shopping_api_terminated_on": "2026-07-31",
        "official_notice": "https://developers.naver.com/notice/article/32564",
        "message": "Legacy price observations must not be treated as current. Use verified reseller exports.",
    }
    META_FILE.write_text(json.dumps(status, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(status, ensure_ascii=False))


if __name__ == "__main__":
    main()
