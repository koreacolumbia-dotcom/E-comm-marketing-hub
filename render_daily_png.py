#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from pathlib import Path
from playwright.sync_api import sync_playwright

if len(sys.argv) < 3:
    print("Usage: render_daily_png.py <html_path> <png_path>")
    sys.exit(1)

html_path = Path(sys.argv[1]).resolve()
png_path = Path(sys.argv[2]).resolve()

with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page(viewport={"width": 1400, "height": 2000})

    page.goto(f"file://{html_path}")
    page.wait_for_timeout(1500)  # 렌더 대기

    page.screenshot(path=str(png_path), full_page=True)
    browser.close()

print(f"[OK] PNG created: {png_path}")
