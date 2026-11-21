"""Safeguards for idempotent Owned PDP YoY patches.

The Daily Digest generator already supplies YoY on many dates. Never replace
its richer, observed charts with a separately computed classification.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


def verified_series(data: dict[str, Any]) -> list[dict[str, Any]]:
    """Return genuinely comparable 7-day series from the Daily Digest payload."""
    source = data.get("pdp_series") or {}
    rows = source.get("rows") or []
    if not source.get("has_yoy") or not isinstance(rows, list):
        return []
    comparable = []
    for row in rows:
        current = row.get("ys")
        previous = row.get("ys_yoy")
        if (
            row.get("has_yoy")
            and isinstance(current, list) and isinstance(previous, list)
            and len(current) == len(previous) == 7
            and all(type(v) in (int, float) and v >= 0 for v in current + previous)
            and sum(current) > 0 and sum(previous) > 0
        ):
            comparable.append(row)
    return comparable


def is_current_report(text: str, data: dict[str, Any]) -> bool:
    """The already-built report has an observed YoY graph and matching 7D data."""
    return (
        "PDP View Trend (Category) · 7D YoY" in text
        and bool(verified_series(data))
        and "작년" in text
    )


def need_bq_fallback(text: str, data: dict[str, Any]) -> bool:
    """BQ is only needed when no usable GA YoY series is embedded."""
    return not is_current_report(text, data) and not verified_series(data)


def find_section_bounds(text: str) -> tuple[int, int]:
    """Find the category-card wrapper by its heading and following Search block.

    The visible heading changed from 'PDP View Trend (Category)' to
    'PDP View Trend (Category) · 7D YoY'. A hard-coded exact heading failed.
    """
    heading = re.search(r"PDP View Trend \(Category\)(?:\s*·\s*7D YoY)?", text)
    if not heading:
        raise ValueError("Missing PDP category heading")
    section_start = '<div class="mt-6 rounded-2xl border border-slate-200 bg-white/70 p-4"'
    start = text.rfind(section_start, 0, heading.start())
    if start < 0 or heading.start() - start > 1000:
        raise ValueError("PDP category container was not found near heading")
    candidates = re.finditer(
        r'<div class="mt-6 grid grid-cols-1 gap-4 lg:grid-cols-2">',
        text[heading.end():],
    )
    end = -1
    for candidate in candidates:
        position = heading.end() + candidate.start()
        if re.search(
            r'<div class="report-card[^"]*">\s*'
            r'<div class="text-xs[^"]*">Search · New</div>',
            text[position:position + 550],
            re.S,
        ):
            end = position
            break
    if end < start:
        raise ValueError("PDP section terminator (Search · New) was not found")
    return start, end


def replace_section(text: str, replacement: str) -> str:
    start, end = find_section_bounds(text)
    return text[:start] + replacement + "\n\n    " + text[end:]


def sync_latest_index(report: Path, index_path: Path, payload: str) -> bool:
    """Update the stable entry point only for the newest dated report."""
    other_reports = list(report.parent.glob("????-??-??.html"))
    if other_reports and report.name != max(x.name for x in other_reports):
        return False
    if index_path.is_file() and index_path.read_text(encoding="utf-8") == payload:
        return False
    index_path.parent.mkdir(parents=True, exist_ok=True)
    index_path.write_text(payload, encoding="utf-8")
    return True


def load_payload(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}
