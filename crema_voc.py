#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crema Review Collector Wrapper (ACCUMULATE + BACKFILL)

Outputs (under reports/crema_raw):
- daily/YYYY-MM-DD.json          # raw snapshot for that day
- master_reviews.jsonl           # newline json (accumulated, deduped by (source,id))
- master_index.json              # lightweight index (counts, min/max dates)

Supports:
- daily incremental (default): collect recent N days, split into day files, merge into master
- backfill: from --start to --end (default 2025-01-01 ~ yesterday KST)

Designed to work even if crema_voc.py has limited CLI.
It will try multiple signatures and fall back to copying site/data/reviews.json.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import subprocess
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from dateutil import tz

OUTPUT_TZ = os.getenv("OUTPUT_TZ", "Asia/Seoul").strip()


def kst_now() -> datetime:
    return datetime.now(timezone.utc).astimezone(tz.gettz(OUTPUT_TZ))


def ensure_dir(p: pathlib.Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def load_json(path: pathlib.Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: pathlib.Path, obj: Dict[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_created_at_iso(s: Any) -> Optional[datetime]:
    if not s:
        return None
    s = str(s).strip()
    if not s:
        return None
    # support trailing Z
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        dt = pd.to_datetime(s, errors="coerce")
        if pd.isna(dt):
            return None
        dt = dt.to_pydatetime()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz.gettz(OUTPUT_TZ))
    return dt.astimezone(tz.gettz(OUTPUT_TZ))


def split_by_day(reviews: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    for r in reviews:
        dt = parse_created_at_iso(r.get("created_at"))
        if not dt:
            continue
        d = dt.date().isoformat()
        out.setdefault(d, []).append(r)
    return out


def dedupe_reviews(reviews: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for r in reviews:
        src = str(r.get("source") or "Unknown")
        rid = str(r.get("id"))
        key = (src, rid)
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def read_master_jsonl(path: pathlib.Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    return rows


def append_master_jsonl(path: pathlib.Path, new_rows: List[Dict[str, Any]]) -> None:
    if not new_rows:
        return
    ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as f:
        for r in new_rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def run_cmd(cmd: List[str], env: Dict[str, str]) -> int:
    print("[CMD]", " ".join(cmd))
    p = subprocess.run(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    print(p.stdout)
    return int(p.returncode)


def collect_range_via_crema_voc(out_json: pathlib.Path, start: str, end: str, recent_days: int) -> pathlib.Path:
    """Try multiple CLI signatures for crema_voc.py and return the produced JSON path."""
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    ensure_dir(out_json.parent)
    if out_json.exists():
        out_json.unlink()

    attempts = [
        ["python", "-u", "crema_voc.py", "--out", str(out_json), "--start", start, "--end", end],
        ["python", "-u", "crema_voc.py", "--output", str(out_json), "--start", start, "--end", end],
        ["python", "-u", "crema_voc.py", "--out", str(out_json), "--days", str(recent_days)],
        ["python", "-u", "crema_voc.py", "--output", str(out_json), "--days", str(recent_days)],
        ["python", "-u", "crema_voc.py"],
    ]

    for cmd in attempts:
        _ = run_cmd(cmd, env=env)
        if out_json.exists() and out_json.stat().st_size > 20:
            break

    fb = pathlib.Path("site/data/reviews.json")
    if (not out_json.exists() or out_json.stat().st_size <= 20) and fb.exists() and fb.stat().st_size > 20:
        print(f"[WARN] collector did not create {out_json}. Fallback copy from {fb}")
        out_json.write_text(fb.read_text(encoding="utf-8"), encoding="utf-8")

    return out_json


def update_master_index(index_path: pathlib.Path, all_rows: List[Dict[str, Any]]) -> None:
    min_dt, max_dt = None, None
    by_source: Dict[str, int] = {}
    parse_fail = 0

    for r in all_rows:
        src = str(r.get("source") or "Unknown")
        by_source[src] = by_source.get(src, 0) + 1
        dt = parse_created_at_iso(r.get("created_at"))
        if not dt:
            parse_fail += 1
            continue
        if min_dt is None or dt < min_dt:
            min_dt = dt
        if max_dt is None or dt > max_dt:
            max_dt = dt

    obj = {
        "updated_at": kst_now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_reviews": len(all_rows),
        "by_source": by_source,
        "created_at_parse_fail": parse_fail,
        "date_min": min_dt.isoformat() if min_dt else None,
        "date_max": max_dt.isoformat() if max_dt else None,
    }
    save_json(index_path, obj)


def daterange_chunks(start: datetime, end: datetime, step_days: int) -> List[Tuple[str, str]]:
    chunks: List[Tuple[str, str]] = []
    cur = start
    while cur <= end:
        nxt = min(end, cur + timedelta(days=step_days - 1))
        chunks.append((cur.date().isoformat(), nxt.date().isoformat()))
        cur = nxt + timedelta(days=1)
    return chunks


def main(mode: str, start: str, end: str, chunk_days: int, recent_days: int) -> None:
    root = pathlib.Path("reports/crema_raw")
    daily_dir = root / "daily"
    master_jsonl = root / "master_reviews.jsonl"
    master_index = root / "master_index.json"
    tmp_out = root / "_tmp_collect.json"

    ensure_dir(daily_dir)

    now = kst_now()
    if not end:
        end = (now - timedelta(days=1)).date().isoformat()  # yesterday
    if not start:
        start = "2025-01-01"

    master_rows = read_master_jsonl(master_jsonl)
    master_seen = set((str(r.get("source") or "Unknown"), str(r.get("id"))) for r in master_rows)

    def merge_into_master(new_rows: List[Dict[str, Any]]) -> int:
        add = []
        for r in new_rows:
            key = (str(r.get("source") or "Unknown"), str(r.get("id")))
            if key in master_seen:
                continue
            master_seen.add(key)
            add.append(r)
        append_master_jsonl(master_jsonl, add)
        master_rows.extend(add)
        return len(add)

    if mode == "daily":
        outp = collect_range_via_crema_voc(tmp_out, start, end, recent_days=recent_days)
        obj = load_json(outp) if outp.exists() else {}
        reviews = obj.get("reviews", []) if isinstance(obj.get("reviews"), list) else []
        print(f"[INFO] collected reviews: {len(reviews)} (recent_days={recent_days})")

        by_day = split_by_day(reviews)
        for day, rows in sorted(by_day.items()):
            day_path = daily_dir / f"{day}.json"
            save_json(day_path, {"reviews": dedupe_reviews(rows)})

        added = merge_into_master(reviews)
        print(f"[OK] master appended: +{added}")

    elif mode == "backfill":
        st = datetime.fromisoformat(start).replace(tzinfo=tz.gettz(OUTPUT_TZ))
        ed = datetime.fromisoformat(end).replace(tzinfo=tz.gettz(OUTPUT_TZ))
        chunks = daterange_chunks(st, ed, step_days=max(1, int(chunk_days)))
        print(f"[INFO] backfill chunks: {len(chunks)} (chunk_days={chunk_days})")

        for cs, ce in chunks:
            print(f"[INFO] collect chunk {cs} ~ {ce}")
            outp = collect_range_via_crema_voc(tmp_out, cs, ce, recent_days=chunk_days)
            obj = load_json(outp) if outp.exists() else {}
            reviews = obj.get("reviews", []) if isinstance(obj.get("reviews"), list) else []
            print(f"[INFO] chunk reviews: {len(reviews)}")

            by_day = split_by_day(reviews)
            for day, rows in sorted(by_day.items()):
                day_path = daily_dir / f"{day}.json"
                save_json(day_path, {"reviews": dedupe_reviews(rows)})

            added = merge_into_master(reviews)
            print(f"[OK] master appended: +{added}")

    else:
        raise ValueError("mode must be daily or backfill")

    update_master_index(master_index, master_rows)
    print(f"[DONE] master_total={len(master_rows)} index={master_index}")

    if tmp_out.exists():
        tmp_out.unlink(missing_ok=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["daily", "backfill"], default="daily")
    ap.add_argument("--start", default="", help="YYYY-MM-DD (default 2025-01-01)")
    ap.add_argument("--end", default="", help="YYYY-MM-DD (default yesterday KST)")
    ap.add_argument("--chunk-days", type=int, default=14, help="backfill chunk size in days")
    ap.add_argument("--recent-days", type=int, default=7, help="daily incremental window in days")
    args = ap.parse_args()
    main(
        mode=args.mode,
        start=args.start.strip(),
        end=args.end.strip(),
        chunk_days=int(args.chunk_days),
        recent_days=int(args.recent_days),
    )
