#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
voc_crema.py  (CREMA VOC PIPELINE | COLLECT + BUILD | single entry)

What this does
1) Runs existing collector: crema_voc.py (supports ONLY: --mode/--start/--end/--chunk-days/--recent-days)
2) Reads site/data/reviews.json
3) Writes stable builder input:
     reports/crema_raw/reviews.json
4) (Optional) Accumulates a deduplicated "lake":
     reports/crema_raw/master_reviews.jsonl
     reports/crema_raw/master_index.json
     reports/crema_raw/daily/YYYY-MM-DD.json
     reports/crema_raw/health/latest_health.json
5) Builds VOC dashboard (runs build_voc_dashboard.py logic in-process):
     site/data/meta.json
     site/data/reviews.json  (filtered to last N days)
     site/index.html

Usage
- Daily:
    python -u voc_crema.py --mode daily --recent-days 7 --build --target-days 7
- Backfill:
    python -u voc_crema.py --mode backfill --start 2025-01-01 --end 2026-02-28 --chunk-days 14 --build --target-days 7

Env
- PROJECT_ROOT: repo root (auto-detect if not set)
- OUTPUT_TZ: default Asia/Seoul
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

# Stable builder input (always written)
RAW_REVIEWS_JSON_REL = RAW_DIR_REL / "reviews.json"


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
RAW_REVIEWS_JSON = ROOT / RAW_REVIEWS_JSON_REL

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
    return {"version": "collector-index-v2", "updated_at": None, "total_unique": 0, "keys": {}}


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
    """Run existing collector crema_voc.py with ONLY its supported CLI args."""
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


def write_builder_input(rows: List[Dict[str, Any]]) -> None:
    write_json(RAW_REVIEWS_JSON, {"reviews": rows})


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


def run_builder(input_json: pathlib.Path, target_days: int, html_template: Optional[str], debug: bool) -> None:
    """
    Run build_voc_dashboard.py in-process to avoid another subprocess layer.
    """
    try:
        import build_voc_dashboard as b  # type: ignore
    except Exception as e:
        raise RuntimeError(f"Failed to import build_voc_dashboard.py: {e}")

    # build_voc_dashboard.main(input_path, html_template, target_days, debug)
    b.main(
        input_path=str(input_json),
        html_template=(html_template.strip() or None) if isinstance(html_template, str) else None,
        target_days=int(target_days),
        debug=bool(debug),
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", required=True, choices=["daily", "backfill"], help="daily or backfill")
    ap.add_argument("--start", default="", help="backfill start (YYYY-MM-DD)")
    ap.add_argument("--end", default="", help="backfill end (YYYY-MM-DD)")
    ap.add_argument("--chunk-days", type=int, default=DEFAULT_CHUNK_DAYS, help="backfill chunk size (days)")
    ap.add_argument("--recent-days", type=int, default=DEFAULT_RECENT_DAYS, help="daily window (days)")

    ap.add_argument("--keep-site-output", action="store_true", help="do not delete site/data/reviews.json after reading")
    ap.add_argument("--no-lake", action="store_true", help="disable lake accumulation (still writes reports/crema_raw/reviews.json)")

    # â build options (merged)
    ap.add_argument("--build", action="store_true", help="run build_voc_dashboard after collection")
    ap.add_argument("--target-days", type=int, default=int(os.getenv("TARGET_DAYS", str(DEFAULT_RECENT_DAYS)) or DEFAULT_RECENT_DAYS), help="builder window days (including today)")
    ap.add_argument("--html-template", default="", help="optional html template path for builder")
    ap.add_argument("--builder-debug", action="store_true", help="builder debug flag (writes extra diagnostics into meta.json)")

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
        "builder_input": str(RAW_REVIEWS_JSON_REL),
        "errors": [],
        "build_enabled": bool(args.build),
        "build_target_days": int(args.target_days),
    }

    try:
        if args.mode == "daily":
            run_crema_voc(mode="daily", start=None, end=None, recent_days=args.recent_days)
            rows = read_crema_output()
            write_builder_input(rows)

            snap_date = fmt_ymd(now_kst().date())
            added = skipped = 0
            if not args.no_lake:
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
                run_meta["chunks"].append({"type": "daily", "rows": len(rows)})

        else:
            start_s = (args.start or DEFAULT_START).strip()
            end_s = (args.end or fmt_ymd(now_kst().date())).strip()
            start_d = parse_ymd(start_s)
            end_d = parse_ymd(end_s)

            all_rows: List[Dict[str, Any]] = []
            for (cs, ce) in daterange_chunks(start_d, end_d, args.chunk_days):
                cs_s = fmt_ymd(cs)
                ce_s = fmt_ymd(ce)
                try:
                    run_crema_voc(mode="backfill", start=cs_s, end=ce_s, recent_days=None)
                    rows = read_crema_output()
                    all_rows.extend(rows)

                    added = skipped = 0
                    if not args.no_lake:
                        added, skipped = append_master_jsonl(rows, idx)
                        run_meta["added_total"] += added
                        run_meta["skipped_total"] += skipped
                        snap_path = snapshot_daily(
                            rows,
                            snap_date=ce_s,
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
                    else:
                        run_meta["chunks"].append({"type": "backfill", "start": cs_s, "end": ce_s, "rows": len(rows)})

                except subprocess.CalledProcessError as e:
                    run_meta["errors"].append({"chunk": f"{cs_s}~{ce_s}", "error": f"crema_voc failed: {e}"})
                    continue
                except Exception as e:
                    run_meta["errors"].append({"chunk": f"{cs_s}~{ce_s}", "error": str(e)})
                    continue

            write_builder_input(all_rows)

        if not args.no_lake:
            save_master_index(idx)

        # Optional: keep site/data/reviews.json from collector
        if (not args.keep_site_output) and CREMA_OUTPUT_JSON.exists():
            try:
                CREMA_OUTPUT_JSON.unlink()
            except Exception:
                pass

        # â Build step (merged)
        if args.build:
            run_builder(input_json=RAW_REVIEWS_JSON, target_days=int(args.target_days), html_template=str(args.html_template or ""), debug=bool(args.builder_debug))

        run_meta["finished_at"] = now_kst().strftime("%Y-%m-%d %H:%M:%S")
        collector_health(run_meta)

        print("[OK] Crema pipeline done.")
        print(f"- Builder input: {RAW_REVIEWS_JSON}")
        if args.build:
            print("[OK] Builder finished -> site/index.html, site/data/meta.json, site/data/reviews.json")
        if not args.no_lake:
            print(f"- Master jsonl:  {MASTER_JSONL}")
            print(f"- Master index:  {MASTER_INDEX}")
            print(f"- Daily snaps:   {DAILY_DIR}")
            print(f"- Health:        {HEALTH_DIR / 'latest_health.json'}")

        if run_meta["errors"]:
            print(f"[WARN] Some chunks failed: {len(run_meta['errors'])} (see latest_health.json)")

    except Exception as e:
        run_meta["errors"].append({"fatal": str(e)})
        run_meta["finished_at"] = now_kst().strftime("%Y-%m-%d %H:%M:%S")
        collector_health(run_meta)
        raise


if __name__ == "__main__":
    main()
