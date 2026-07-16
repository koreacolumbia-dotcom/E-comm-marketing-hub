#!/usr/bin/env python3
"""Build a minimal Cloudflare Pages bundle from the dashboard repository.

Cloudflare static assets have a 25 MiB per-file limit. This script copies only
runtime dashboard assets, excludes development/cache content, and replaces any
oversized HTML report with a small explanatory page so one report cannot block
publication of the entire dashboard.
"""
from __future__ import annotations

import html
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DIST = ROOT / "dist"
MAX_FILE_BYTES = 24 * 1024 * 1024
ROOT_FILES = ("index.html", "Columbia_logo.png", "robots.txt", "_headers")
SKIP_NAMES = {".git", ".github", "__pycache__", ".pytest_cache", ".naver_cache"}


def placeholder(relative_path: Path, size: int) -> str:
    size_mb = size / 1024 / 1024
    title = html.escape(relative_path.stem.replace("_", " ").title())
    return f"""<!doctype html>
<html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex,nofollow"><title>{title} · 배포 최적화 중</title>
<style>body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f8fafc;color:#0f172a;margin:0;display:grid;min-height:100vh;place-items:center}}main{{max-width:680px;margin:24px;padding:32px;background:#fff;border:1px solid #e2e8f0;border-radius:24px;box-shadow:0 18px 50px rgba(15,23,42,.08)}}h1{{font-size:24px}}p{{line-height:1.7;color:#475569}}a{{color:#0369a1;font-weight:700}}</style></head><body><main><h1>{title}</h1><p>이 리포트는 단일 파일 용량이 {size_mb:.1f}MB로 Cloudflare의 정적 파일 제한을 초과하여 현재 경량화 작업이 필요합니다.</p><p>나머지 대시보드는 정상적으로 이용할 수 있습니다.</p><p><a href="/">대시보드로 돌아가기</a></p></main></body></html>"""


def copy_tree(source: Path, destination: Path) -> tuple[int, list[str]]:
    copied = 0
    replaced: list[str] = []
    for path in source.rglob("*"):
        if not path.is_file() or any(part in SKIP_NAMES for part in path.parts):
            continue
        relative = path.relative_to(source)
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        size = path.stat().st_size
        if size > MAX_FILE_BYTES:
            if path.suffix.lower() == ".html":
                target.write_text(placeholder(relative, size), encoding="utf-8")
                replaced.append(f"{relative} ({size / 1024 / 1024:.1f} MiB)")
                copied += 1
            else:
                replaced.append(f"SKIPPED {relative} ({size / 1024 / 1024:.1f} MiB)")
            continue
        shutil.copy2(path, target)
        copied += 1
    return copied, replaced


def main() -> int:
    shutil.rmtree(DIST, ignore_errors=True)
    DIST.mkdir(parents=True)

    for filename in ROOT_FILES:
        source = ROOT / filename
        if source.exists():
            shutil.copy2(source, DIST / filename)

    reports = ROOT / "reports"
    if not reports.exists():
        raise SystemExit("reports directory is missing")

    copied, replaced = copy_tree(reports, DIST / "reports")
    if not (DIST / "index.html").exists():
        raise SystemExit("dist/index.html is missing")

    total_files = sum(1 for p in DIST.rglob("*") if p.is_file())
    oversized = [p for p in DIST.rglob("*") if p.is_file() and p.stat().st_size > MAX_FILE_BYTES]
    if oversized:
        raise SystemExit("Oversized assets remain: " + ", ".join(str(p) for p in oversized))

    print(f"[BUNDLE] copied report assets={copied}; total files={total_files}")
    for item in replaced:
        print(f"[BUNDLE] oversized replacement: {item}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
