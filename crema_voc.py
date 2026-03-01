#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
[CREMA VOC ORCHESTRATOR | v2.3 | crema_voc.py 단일 파일 | accumulate + build]

✅ What this file does (end-to-end)
1) Ensure we have an aggregated input JSON: {"reviews":[...]}
   - If external collector exists -> run it to create site/data/reviews.json (raw aggregated)
   - Else -> expects an existing aggregated JSON (default: site/data/reviews.json or --input)
2) Accumulate & dedupe into:
   - reports/crema_raw/master_reviews.jsonl
   - reports/crema_raw/master_index.json
   - reports/crema_raw/daily/YYYY-MM-DD.json (snapshot)
3) Create builder input:
   - reports/crema_raw/reviews.json  ({"reviews":[...]})  ✅ build_voc_dashboard.py input
4) Run builder to generate site outputs:
   - site/data/reviews.json
   - site/data/meta.json
   - site/index.html

CLI:
  --mode {daily,backfill}
  --recent-days N                 (daily)
  --start YYYY-MM-DD --end YYYY-MM-DD --chunk-days N   (backfill)
  --input PATH                    (optional: aggregated reviews.json 직접 지정)
  --html-template PATH            (optional: builder template override)
  --target-days N                 (builder window days, default env TARGET_DAYS or 7)
  --debug                         (builder debug flag)
  --keep-site-output              (do not delete site/data/reviews.json after reading)

ENV:
  OUTPUT_TZ=Asia/Seoul
  PROJECT_ROOT=/path/to/repo
  CREMA_COLLECTOR_PATH=relative/or/absolute/path/to/external_collector.py
    - external collector is expected to create site/data/reviews.json (aggregated)
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


# ----------------------------
# Config
# ----------------------------
OUTPUT_TZ = os.getenv("OUTPUT_TZ", "Asia/Seoul").strip()
TZ = tz.gettz(OUTPUT_TZ)

DEFAULT_START = "2025-01-01"
DEFAULT_CHUNK_DAYS = int(os.getenv("CREMA_CHUNK_DAYS", "14") or "14")
DEFAULT_RECENT_DAYS = int(os.getenv("CREMA_RECENT_DAYS", "7") or "7")
DEFAULT_TARGET_DAYS = int(os.getenv("TARGET_DAYS", "7") or "7")

# external collector output (fixed contract)
SITE_AGG_JSON_REL = pathlib.Path("site/data/reviews.json")

# lake paths
RAW_DIR_REL = pathlib.Path("reports/crema_raw")
DAILY_DIR_REL = RAW_DIR_REL / "daily"
HEALTH_DIR_REL = RAW_DIR_REL / "health"
MASTER_JSONL_REL = RAW_DIR_REL / "master_reviews.jsonl"
MASTER_INDEX_REL = RAW_DIR_REL / "master_index.json"

# builder input (stable)
BUILDER_INPUT_REL = RAW_DIR_REL / "reviews.json"


# ----------------------------
# Helpers
# ----------------------------
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
        if (p / ".git").exists():
            return p
        if (p / "site").exists():
            return p
    return pathlib.Path.cwd().resolve()


ROOT = find_repo_root()

RAW_DIR = ROOT / RAW_DIR_REL
DAILY_DIR = ROOT / DAILY_DIR_REL
HEALTH_DIR = ROOT / HEALTH_DIR_REL
MASTER_JSONL = ROOT / MASTER_JSONL_REL
MASTER_INDEX = ROOT / MASTER_INDEX_REL
SITE_AGG_JSON = ROOT / SITE_AGG_JSON_REL
BUILDER_INPUT = ROOT / BUILDER_INPUT_REL

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


def normalize_review(r: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(r)
    # 최소한 source는 비워두지 않기
    if not out.get("source"):
        out["source"] = "Official"
    return out


def key_of(r: Dict[str, Any]) -> str:
    """
    중복키: (source, id) 우선. id 없으면 (product_code|created_at|text_prefix)로 대체.
    """
    src = safe_str(r.get("source") or "Unknown").strip() or "Unknown"
    rid = safe_str(r.get("id")).strip()
    if not rid:
        rid = f"{safe_str(r.get('product_code'))}|{safe_str(r.get('created_at'))}|{safe_str(r.get('text'))[:40]}"
    return f"{src}::{rid}"


def load_master_index() -> Dict[str, Any]:
    if MASTER_INDEX.exists():
        try:
            obj = load_json(MASTER_INDEX)
            if isinstance(obj, dict) and isinstance(obj.get("keys"), dict):
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


def ensure_external_collector_exists() -> Optional[pathlib.Path]:
    """
    외부 collector 우선순위:
    1) ENV CREMA_COLLECTOR_PATH
    2) scripts/crema_voc_collector.py
    """
    custom = (os.getenv("CREMA_COLLECTOR_PATH", "") or "").strip()
    candidates: List[pathlib.Path] = []
    if custom:
        p = pathlib.Path(custom).expanduser()
        candidates.append(p if p.is_absolute() else (ROOT / p))
    candidates.append(ROOT / "scripts" / "crema_voc_collector.py")

    for c in candidates:
        if c.exists():
            return c
    return None


def run_external_collector(mode: str, start: Optional[str], end: Optional[str], recent_days: Optional[int]) -> None:
    collector = ensure_external_collector_exists()
    if collector is None:
        print("[INFO] No external collector found. Will rely on existing aggregated JSON.")
        return

    cmd = ["python", "-u", str(collector), "--mode", mode]
    if mode == "backfill":
        if not start or not end:
            raise ValueError("backfill requires --start and --end")
        cmd += ["--start", start, "--end", end]
    else:
        if recent_days is not None:
            cmd += ["--recent-days", str(int(recent_days))]

    print("[CMD] " + " ".join(cmd))
    subprocess.run(cmd, cwd=str(ROOT), check=True)


def read_aggregated_reviews(path: pathlib.Path) -> List[Dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"Aggregated reviews.json not found: {path}")
    obj = load_json(path)
    reviews = obj.get("reviews")
    if not isinstance(reviews, list):
        raise ValueError(f"Invalid structure. Expected {{reviews:[...]}} in {path}")
    return [r for r in reviews if isinstance(r, dict)]


def write_builder_input(rows: List[Dict[str, Any]]) -> None:
    # ✅ build_voc_dashboard.py input contract
    write_json(BUILDER_INPUT, {"reviews": rows})


def run_builder(
    input_path: pathlib.Path,
    html_template: Optional[str],
    target_days: int,
    debug: bool,
) -> None:
    """
    build_voc_dashboard.py 실행해서 site/data/reviews.json을 반드시 생성하도록 한다.
    """
    builder = ROOT / "build_voc_dashboard.py"
    if not builder.exists():
        raise FileNotFoundError(f"build_voc_dashboard.py not found at repo root: {builder}")

    cmd = ["python", "-u", str(builder), "--input", str(input_path)]
    if html_template:
        cmd += ["--html-template", html_template]
    if target_days:
        cmd += ["--target-days", str(int(target_days))]
    if debug:
        cmd += ["--debug"]

    print("[CMD] " + " ".join(cmd))
    subprocess.run(cmd, cwd=str(ROOT), check=True)

    # post-check (핵심)
    out_reviews = ROOT / "site" / "data" / "reviews.json"
    if not out_reviews.exists():
        raise RuntimeError("site/data/reviews.json was not created by build_voc_dashboard.py (post-check failed).")


# ----------------------------
# Main
# ----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", required=True, choices=["daily", "backfill"])
    ap.add_argument("--start", default="", help="backfill start (YYYY-MM-DD)")
    ap.add_argument("--end", default="", help="backfill end (YYYY-MM-DD)")
    ap.add_argument("--chunk-days", type=int, default=DEFAULT_CHUNK_DAYS)
    ap.add_argument("--recent-days", type=int, default=DEFAULT_RECENT_DAYS)
    ap.add_argument("--input", default="", help="optional aggregated input JSON path ({reviews:[...]})")
    ap.add_argument("--html-template", default="", help="optional builder template path")
    ap.add_argument("--target-days", type=int, default=DEFAULT_TARGET_DAYS)
    ap.add_argument("--debug", action="store_true", help="pass --debug to builder")
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
        "builder_input": str(BUILDER_INPUT_REL),
        "builder_target_days": int(args.target_days),
    }

    try:
        # ----------------------------
        # 0) Determine aggregated input source
        # ----------------------------
        cli_input = (args.input or "").strip()
        if cli_input:
            agg_path = pathlib.Path(cli_input).expanduser()
            agg_path = agg_path if agg_path.is_absolute() else (ROOT / agg_path)
        else:
            agg_path = SITE_AGG_JSON  # default contract

        if args.mode == "daily":
            # 1) Run external collector if present (creates site/data/reviews.json)
            run_external_collector(mode="daily", start=None, end=None, recent_days=args.recent_days)

            # 2) Read aggregated
            rows = read_aggregated_reviews(agg_path)

            # 3) Accumulate / snapshot
            added, skipped = append_master_jsonl(rows, idx)
            run_meta["added_total"] += added
            run_meta["skipped_total"] += skipped

            snap_date = fmt_ymd(now_kst().date())
            snap_path = snapshot_daily(
                rows,
                snap_date=snap_date,
                meta={"recent_days": int(args.recent_days), "added": added, "skipped": skipped, "rows": len(rows)},
            )
            run_meta["chunks"].append({"type": "daily", "snap": str(snap_path), "rows": len(rows), "added": added, "skipped": skipped})

            # 4) Builder input from today's aggregated
            write_builder_input(rows)

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
                    run_external_collector(mode="backfill", start=cs_s, end=ce_s, recent_days=None)
                    rows = read_aggregated_reviews(agg_path)
                    all_rows.extend(rows)

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
                except subprocess.CalledProcessError as e:
                    run_meta["errors"].append({"chunk": f"{cs_s}~{ce_s}", "error": f"collector failed: {e}"})
                    continue
                except Exception as e:
                    run_meta["errors"].append({"chunk": f"{cs_s}~{ce_s}", "error": str(e)})
                    continue

            # backfill builder input uses all rows
            write_builder_input(all_rows)

        # save index
        save_master_index(idx)

        # cleanup site aggregated if desired (ONLY if default path used)
        if (not args.keep_site_output) and (not cli_input) and SITE_AGG_JSON.exists():
            try:
                SITE_AGG_JSON.unlink()
            except Exception:
                pass

        # ----------------------------
        # 5) Run builder (must create site/data/reviews.json)
        # ----------------------------
        html_tpl = (args.html_template or "").strip() or None
        run_builder(
            input_path=BUILDER_INPUT,
            html_template=html_tpl,
            target_days=int(args.target_days or DEFAULT_TARGET_DAYS),
            debug=bool(args.debug),
        )

        run_meta["finished_at"] = now_kst().strftime("%Y-%m-%d %H:%M:%S")
        collector_health(run_meta)

        print("[OK] crema_voc.py done (accumulate + build).")
        print(f"- Builder input: {BUILDER_INPUT}")
        print(f"- Site output:   {ROOT / 'site' / 'index.html'}")
        print(f"- Site reviews:  {ROOT / 'site' / 'data' / 'reviews.json'}")
        print(f"- Site meta:     {ROOT / 'site' / 'data' / 'meta.json'}")
        print(f"- Added: {run_meta['added_total']} | Skipped(dup): {run_meta['skipped_total']}")
        if run_meta["errors"]:
            print(f"[WARN] Some chunks failed: {len(run_meta['errors'])} (see reports/crema_raw/health/latest_health.json)")

    except Exception as e:
        run_meta["errors"].append({"fatal": str(e)})
        run_meta["finished_at"] = now_kst().strftime("%Y-%m-%d %H:%M:%S")
        collector_health(run_meta)
        raise


if __name__ == "__main__":
    main()