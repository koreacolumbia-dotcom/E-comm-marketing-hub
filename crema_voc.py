#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
[CREMA VOC COLLECTOR | v2 | Compatible with existing crema_voc.py CLI]

✅ Goal
- Use your existing `crema_voc.py` (which ONLY supports: --mode/--start/--end/--chunk-days/--recent-days)
- After each run, read `site/data/reviews.json` (produced by crema_voc.py)
- Accumulate & deduplicate into an on-disk "lake":
    reports/crema_raw/master_reviews.jsonl
    reports/crema_raw/master_index.json
    reports/crema_raw/daily/YYYY-MM-DD.json         (snapshot of reviews collected for that run)
    reports/crema_raw/health/latest_health.json     (collector diagnostics)

⚠️ Why this exists
- Your crema_voc.py does NOT accept --out/--output/--days, so any wrapper MUST NOT pass them.
- This script is drop-in: it calls crema_voc.py with supported args, then copies/merges outputs.

Usage
- Daily (incremental; uses crema_voc.py daily mode):
    python crema_collect_accumulate_v2.py --mode daily --recent-days 7

- Backfill (chunked; calls crema_voc.py backfill repeatedly by date chunks):
    python crema_collect_accumulate_v2.py --mode backfill --start 2025-01-01 --end 2026-02-28 --chunk-days 14

Environment
- PROJECT_ROOT: repo root (defaults to auto-detect: folder containing site/ or .git)
- OUTPUT_TZ: default Asia/Seoul (used for chunking + snapshot naming)
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import subprocess
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple

from dateutil import tz


OUTPUT_TZ = os.getenv("OUTPUT_TZ", "Asia/Seoul").strip()
TZ = tz.gettz(OUTPUT_TZ)

DEFAULT_START = "2025-01-01"
DEFAULT_CHUNK_DAYS = int(os.getenv("CREMA_CHUNK_DAYS", "14") or "14")
DEFAULT_RECENT_DAYS = int(os.getenv("CREMA_RECENT_DAYS", "7") or "7")

CREMA_OUTPUT_JSON_REL = pathlib.Path("site/data/reviews.json")

RAW_DIR_REL = pathlib.Path("reports/crema_raw")
DAILY_DIR_REL = RAW_DIR_REL / "daily"
HEALTH_DIR_REL = RAW_DIR_REL / "health"
MASTER_JSONL_REL = RAW_DIR_REL / "master_reviews.jsonl"
MASTER_INDEX_REL = RAW_DIR_REL / "master_index.json"


def now_kst() -> datetime:
    return datetime.now(tz=TZ)


def parse_ymd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def fmt_ymd(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def find_repo_root() -> pathlib.Path:
    env_root = os.getenv("PROJECT_ROOT", "").strip()
    if env_root:
        return pathlib.Path(env_root).expanduser().resolve()
    here = pathlib.Path(__file__).resolve()
    for p in [here.parent] + list(here.parents):
        if (p / "site").exists():
            return p
        if (p / ".git").exists():
            return p
    return pathlib.Path.cwd().resolve()


ROOT = find_repo_root()
RAW_DIR = ROOT / RAW_DIR_REL
DAILY_DIR = ROOT / DAILY_DIR_REL
HEALTH_DIR = ROOT / HEALTH_DIR_REL
MASTER_JSONL = ROOT / MASTER_JSONL_REL
MASTER_INDEX = ROOT / MASTER_INDEX_REL
CREMA_OUTPUT_JSON = ROOT / CREMA_OUTPUT_JSON_REL

RAW_DIR.mkdir(parents=True, exist_ok=True)
DAILY_DIR.mkdir(parents=True, exist_ok=True)
HEALTH_DIR.mkdir(parents=True, exist_ok=True)


def load_json(path: pathlib.Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: pathlib.Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def safe_str(x: Any) -> str:
    return "" if x is None else str(x)


def key_of(r: Dict[str, Any]) -> str:
    src = safe_str(r.get("source") or "Unknown").strip() or "Unknown"
    rid = safe_str(r.get("id")).strip()
    if not rid:
        rid = f"{safe_str(r.get('product_code'))}|{safe_str(r.get('created_at'))}|{safe_str(r.get('text'))[:40]}"
    return f"{src}::{rid}"


def normalize_review(r: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(r)
    if not out.get("source"):
        out["source"] = "Official"
    return out


def load_master_index() -> Dict[str, Any]:
    if MASTER_INDEX.exists():
        try:
            obj = load_json(MASTER_INDEX)
            if isinstance(obj, dict) and "keys" in obj and isinstance(obj["keys"], dict):
                return obj
        except Exception:
            pass
    return {
        "version": "collector-index-v2",
        "updated_at": None,
        "total_unique": 0,
        "keys": {},
    }


def save_master_index(idx: Dict[str, Any]) -> None:
    idx["updated_at"] = now_kst().strftime("%Y-%m-%d %H:%M:%S")
    idx["total_unique"] = len(idx.get("keys") or {})
    write_json(MASTER_INDEX, idx)


def append_master_jsonl(new_rows: List[Dict[str, Any]], idx: Dict[str, Any]) -> Tuple[int, int]:
    keys_map = idx.setdefault("keys", {})
    added = 0
    skipped = 0
    ts = now_kst().isoformat()
    MASTER_JSONL.parent.mkdir(parents=True, exist_ok=True)

    with MASTER_JSONL.open("a", encoding="utf-8") as f:
        for r in new_rows:
            rr = normalize_review(r)
            k = key_of(rr)
            if k in keys_map:
                skipped += 1
                keys_map[k]["last_seen"] = ts
                continue
            keys_map[k] = {"first_seen": ts, "last_seen": ts}
            f.write(json.dumps(rr, ensure_ascii=False) + "\n")
            added += 1

    return added, skipped


def run_crema_voc(mode: str, start: Optional[str], end: Optional[str], recent_days: Optional[int]) -> None:
    crema_script = ROOT / "crema_voc.py"
    if not crema_script.exists():
        raise FileNotFoundError(f"crema_voc.py not found at: {crema_script}")

    cmd = ["python", "-u", str(crema_script), "--mode", mode]

    if mode == "backfill":
        if not start or not end:
            raise ValueError("backfill requires --start and --end")
        cmd += ["--start", start, "--end", end]
    else:
        if recent_days is not None:
            cmd += ["--recent-days", str(int(recent_days))]

    print("[CMD] " + " ".join(cmd))
    subprocess.run(cmd, cwd=str(ROOT), check=True)


def read_crema_output() -> List[Dict[str, Any]]:
    if not CREMA_OUTPUT_JSON.exists():
        raise FileNotFoundError(f"Expected crema output not found: {CREMA_OUTPUT_JSON}")
    obj = load_json(CREMA_OUTPUT_JSON)
    reviews = obj.get("reviews")
    if not isinstance(reviews, list):
        raise ValueError(f"Invalid crema output structure. Expected {{reviews:[...]}} in {CREMA_OUTPUT_JSON}")
    return [r for r in reviews if isinstance(r, dict)]


def snapshot_daily(rows: List[Dict[str, Any]], snap_date: str, meta: Dict[str, Any]) -> pathlib.Path:
    out = {
        "snapshot_date": snap_date,
        "created_at": now_kst().strftime("%Y-%m-%d %H:%M:%S"),
        "meta": meta,
        "reviews": rows,
    }
    p = DAILY_DIR / f"{snap_date}.json"
    write_json(p, out)
    return p


def collector_health(payload: Dict[str, Any]) -> None:
    write_json(HEALTH_DIR / "latest_health.json", payload)


def daterange_chunks(start_d: date, end_d: date, chunk_days: int) -> List[Tuple[date, date]]:
    chunk_days = max(1, int(chunk_days))
    out: List[Tuple[date, date]] = []
    cur = start_d
    while cur <= end_d:
        nxt = min(end_d, cur + timedelta(days=chunk_days - 1))
        out.append((cur, nxt))
        cur = nxt + timedelta(days=1)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", required=True, choices=["daily", "backfill"], help="daily or backfill")
    ap.add_argument("--start", default="", help="backfill start (YYYY-MM-DD)")
    ap.add_argument("--end", default="", help="backfill end (YYYY-MM-DD)")
    ap.add_argument("--chunk-days", type=int, default=DEFAULT_CHUNK_DAYS, help="backfill chunk size (days)")
    ap.add_argument("--recent-days", type=int, default=DEFAULT_RECENT_DAYS, help="daily window (days)")
    ap.add_argument("--keep-site-output", action="store_true", help="do not delete site/data/reviews.json after reading")
    args = ap.parse_args()

    idx = load_master_index()

    run_meta: Dict[str, Any] = {
        "mode": args.mode,
        "repo_root": str(ROOT),
        "output_tz": OUTPUT_TZ,
        "started_at": now_kst().strftime("%Y-%m-%d %H:%M:%S"),
        "chunks": [],
        "added_total": 0,
        "skipped_total": 0,
        "errors": [],
    }

    try:
        if args.mode == "daily":
            run_crema_voc(mode="daily", start=None, end=None, recent_days=args.recent_days)
            rows = read_crema_output()
            snap_date = fmt_ymd(now_kst().date())
            added, skipped = append_master_jsonl(rows, idx)
            run_meta["added_total"] += added
            run_meta["skipped_total"] += skipped

            snap_path = snapshot_daily(
                rows,
                snap_date=snap_date,
                meta={"recent_days": int(args.recent_days), "added": added, "skipped": skipped, "rows": len(rows)},
            )
            run_meta["chunks"].append({"type": "daily", "snap": str(snap_path), "rows": len(rows), "added": added, "skipped": skipped})

        else:
            start_s = (args.start or DEFAULT_START).strip()
            end_s = (args.end or fmt_ymd(now_kst().date())).strip()
            start_d = parse_ymd(start_s)
            end_d = parse_ymd(end_s)

            for (cs, ce) in daterange_chunks(start_d, end_d, args.chunk_days):
                cs_s = fmt_ymd(cs)
                ce_s = fmt_ymd(ce)
                try:
                    run_crema_voc(mode="backfill", start=cs_s, end=ce_s, recent_days=None)
                    rows = read_crema_output()
                    snap_date = ce_s
                    added, skipped = append_master_jsonl(rows, idx)
                    run_meta["added_total"] += added
                    run_meta["skipped_total"] += skipped

                    snap_path = snapshot_daily(
                        rows,
                        snap_date=snap_date,
                        meta={"start": cs_s, "end": ce_s, "added": added, "skipped": skipped, "rows": len(rows)},
                    )
                    run_meta["chunks"].append({
                        "type": "backfill",
                        "start": cs_s,
                        "end": ce_s,
                        "snap": str(snap_path),
                        "rows": len(rows),
                        "added": added,
                        "skipped": skipped,
                    })
                except subprocess.CalledProcessError as e:
                    run_meta["errors"].append({"chunk": f"{cs_s}~{ce_s}", "error": f"crema_voc failed: {e}"})
                    continue
                except Exception as e:
                    run_meta["errors"].append({"chunk": f"{cs_s}~{ce_s}", "error": str(e)})
                    continue

        save_master_index(idx)

        if (not args.keep_site_output) and CREMA_OUTPUT_JSON.exists():
            try:
                CREMA_OUTPUT_JSON.unlink()
            except Exception:
                pass

        run_meta["finished_at"] = now_kst().strftime("%Y-%m-%d %H:%M:%S")
        collector_health(run_meta)

        print("[OK] Crema collection + accumulation done.")
        print(f"- Master jsonl: {MASTER_JSONL}")
        print(f"- Master index: {MASTER_INDEX}")
        print(f"- Daily snaps:  {DAILY_DIR}")
        print(f"- Health:       {HEALTH_DIR / 'latest_health.json'}")
        print(f"- Added: {run_meta['added_total']} | Skipped(dup): {run_meta['skipped_total']}")
        if run_meta["errors"]:
            print(f"[WARN] Some chunks failed: {len(run_meta['errors'])} (see latest_health.json)")

    except Exception as e:
        run_meta["errors"].append({"fatal": str(e)})
        run_meta["finished_at"] = now_kst().strftime("%Y-%m-%d %H:%M:%S")
        collector_health(run_meta)
        raise


if __name__ == "__main__":
    main()
