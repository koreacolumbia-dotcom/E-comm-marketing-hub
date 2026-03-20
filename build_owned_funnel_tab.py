#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
from pathlib import Path


DEFAULT_SOURCE = Path(__file__).with_name("owned_funnel_tab.html")
DEFAULT_OUTPUT = Path(__file__).parent / "reports" / "daily_digest" / "owned_funnel_tab.html"
DEFAULT_SOURCE_BASE = "reports/daily_digest/data/owned"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build standalone OWNED funnel tab HTML.")
    parser.add_argument(
        "--source",
        default=str(DEFAULT_SOURCE),
        help="Source HTML file to publish.",
    )
    parser.add_argument(
        "--out",
        default=str(DEFAULT_OUTPUT),
        help="Published HTML output path.",
    )
    parser.add_argument(
        "--data-base",
        default="data/owned",
        help="Relative data/owned path to inject for the published HTML.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source = Path(args.source).resolve()
    out = Path(args.out).resolve()
    html = source.read_text(encoding="utf-8")

    published_html = html.replace(DEFAULT_SOURCE_BASE, args.data_base.replace("\\", "/"))
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(published_html, encoding="utf-8")

    print(f"[OK] source: {source}")
    print(f"[OK] out: {out}")
    print(f"[OK] data base: {args.data_base}")


if __name__ == "__main__":
    main()
