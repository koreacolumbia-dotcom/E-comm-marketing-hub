#!/usr/bin/env python3
"""Build a bounded, secured and consistently styled Cloudflare dashboard bundle."""
from __future__ import annotations

import html
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DIST = ROOT / "dist"
MAX_FILE_BYTES = 24 * 1024 * 1024
ROOT_FILES = ("index.html", "Columbia_logo.png", "robots.txt", "_headers")
DESIGN_FILES = ("dashboard-redesign.css", "dashboard-redesign.js")
SKIP_NAMES = {".git", ".github", "__pycache__", ".pytest_cache", ".naver_cache"}
EXCLUDED_PREFIXES = (
    Path("member_funnel"),
    Path("voc_crema/member_funnel"),
    Path("daily_digest/daily"),
)
DESIGN_HEAD = '<link rel="stylesheet" href="/assets/dashboard-redesign.css">'
DESIGN_SCRIPT = '<script defer src="/assets/dashboard-redesign.js"></script>'


def information_page(title: str, message: str) -> str:
    return f"""<!doctype html>
<html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex,nofollow"><title>{html.escape(title)}</title>{DESIGN_HEAD}{DESIGN_SCRIPT}</head><body><main style="max-width:680px;margin:10vh auto;padding:32px;background:#fff;border:1px solid #e7ebf1;border-radius:18px"><h1>{html.escape(title)}</h1><p>{html.escape(message)}</p><p><a href="/">대시보드로 돌아가기</a></p></main></body></html>"""


def oversized_placeholder(relative_path: Path, size: int) -> str:
    size_mb = size / 1024 / 1024
    title = relative_path.stem.replace("_", " ").title()
    return information_page(title, f"이 리포트는 단일 파일 용량이 {size_mb:.1f}MB로 배포 제한을 초과하여 현재 경량화 작업 중입니다. 나머지 대시보드는 정상적으로 이용할 수 있습니다.")


def is_excluded(relative: Path) -> bool:
    return any(relative == prefix or prefix in relative.parents for prefix in EXCLUDED_PREFIXES)


def inject_design(path: Path) -> None:
    if path.suffix.lower() != ".html" or not path.exists() or path.stat().st_size > MAX_FILE_BYTES:
        return
    text = path.read_text(encoding="utf-8", errors="ignore")
    changed = False
    if "dashboard-redesign.css" not in text:
        marker = "</head>"
        if marker in text.lower():
            pos = text.lower().rfind(marker)
            text = text[:pos] + DESIGN_HEAD + "\n" + text[pos:]
        else:
            text = DESIGN_HEAD + "\n" + text
        changed = True
    if "dashboard-redesign.js" not in text:
        marker = "</body>"
        if marker in text.lower():
            pos = text.lower().rfind(marker)
            text = text[:pos] + DESIGN_SCRIPT + "\n" + text[pos:]
        else:
            text += "\n" + DESIGN_SCRIPT
        changed = True
    if changed:
        path.write_text(text, encoding="utf-8")


def write_restricted_placeholders(destination: Path) -> None:
    pages = {
        Path("member_funnel/index.html"): ("회원 분석 접근 제한", "고객 단위 정보가 포함될 수 있어 인터넷 배포 대상에서 제외되었습니다. 집계 지표는 Summary에서 확인할 수 있습니다."),
        Path("voc_crema/member_funnel/index.html"): ("회원 분석 접근 제한", "고객 단위 정보가 포함될 수 있어 인터넷 배포 대상에서 제외되었습니다."),
        Path("daily_digest/daily/index.html"): ("일별 원본 리포트 접근 제한", "원본 일별 아카이브는 개인정보 오탐 및 노출 위험을 줄이기 위해 배포 대상에서 제외되었습니다."),
    }
    for relative, (title, message) in pages.items():
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(information_page(title, message), encoding="utf-8")


def copy_tree(source: Path, destination: Path) -> tuple[int, list[str], int]:
    copied = 0
    excluded = 0
    replaced: list[str] = []
    for path in source.rglob("*"):
        if not path.is_file() or any(part in SKIP_NAMES for part in path.parts):
            continue
        relative = path.relative_to(source)
        if is_excluded(relative):
            excluded += 1
            continue
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        size = path.stat().st_size
        if size > MAX_FILE_BYTES:
            if path.suffix.lower() == ".html":
                target.write_text(oversized_placeholder(relative, size), encoding="utf-8")
                replaced.append(f"{relative} ({size / 1024 / 1024:.1f} MiB)")
                copied += 1
            else:
                replaced.append(f"SKIPPED {relative} ({size / 1024 / 1024:.1f} MiB)")
            continue
        shutil.copy2(path, target)
        copied += 1
    return copied, replaced, excluded


def main() -> int:
    shutil.rmtree(DIST, ignore_errors=True)
    DIST.mkdir(parents=True)

    for filename in ROOT_FILES:
        source = ROOT / filename
        if source.exists():
            shutil.copy2(source, DIST / filename)

    assets_out = DIST / "assets"
    assets_out.mkdir(parents=True, exist_ok=True)
    for filename in DESIGN_FILES:
        source = ROOT / "assets" / filename
        if not source.exists():
            raise SystemExit(f"missing design asset: {source}")
        shutil.copy2(source, assets_out / filename)

    reports = ROOT / "reports"
    if not reports.exists():
        raise SystemExit("reports directory is missing")

    copied, replaced, excluded = copy_tree(reports, DIST / "reports")
    write_restricted_placeholders(DIST / "reports")

    html_files = list(DIST.rglob("*.html"))
    for page in html_files:
        inject_design(page)

    if not (DIST / "index.html").exists():
        raise SystemExit("dist/index.html is missing")

    total_files = sum(1 for p in DIST.rglob("*") if p.is_file())
    oversized = [p for p in DIST.rglob("*") if p.is_file() and p.stat().st_size > MAX_FILE_BYTES]
    if oversized:
        raise SystemExit("Oversized assets remain: " + ", ".join(str(p) for p in oversized))

    print(f"[BUNDLE] redesigned html pages={len(html_files)}; copied report assets={copied}; excluded sensitive/raw files={excluded}; total files={total_files}")
    for item in replaced:
        print(f"[BUNDLE] oversized replacement: {item}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
