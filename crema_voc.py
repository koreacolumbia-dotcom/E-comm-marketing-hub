#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
[CREMA VOC COLLECTOR | v2.1 | crema_voc.py 단일 파일 | accumulate + builder input]

✅ Goal
- CLI는 단순하게:
    --mode {daily,backfill}
    --start/--end/--chunk-days (backfill)
    --recent-days (daily)
- 실행 흐름:
  1) (현재 레포에 "원래의 크레마 수집기"가 따로 존재한다면) 그 파일을 호출해서 site/data/reviews.json 생성
     - 기본값: scripts/crema_voc_collector.py 를 찾고, 없으면 "self" 모드로 site/data/reviews.json이 이미 생성돼있다고 가정
  2) site/data/reviews.json 읽어서
  3) reports/crema_raw/master_reviews.jsonl + master_index.json 으로 dedupe 누적
  4) reports/crema_raw/daily/YYYY-MM-DD.json 스냅샷 저장
  5) ✅ builder가 바로 먹을 input 생성:
       reports/crema_raw/reviews.json   ({"reviews":[...]})

⚠️ IMPORTANT
- 너가 “파일명은 crema_voc.py로만”을 고집하는 상황에서
  "원래 collector" 코드가 이미 사라졌다면,
  이 파일은 최소한 "site/data/reviews.json이 이미 만들어져있다" 전제하에 누적/입력생성은 가능.
- 만약 원래 collector가 따로 있다면, 환경변수 CREMA_COLLECTOR_PATH 로 경로를 지정해줘.
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

# crema collector output (fixed)
CREMA_OUTPUT_JSON_REL = pathlib.Path("site/data/reviews.json")

# lake paths
RAW_DIR_REL = pathlib.Path("reports/crema_raw")
DAILY_DIR_REL = RAW_DIR_REL / "daily"
HEALTH_DIR_REL = RAW_DIR_REL / "health"
MASTER_JSONL_REL = RAW_DIR_REL / "master_reviews.jsonl"
MASTER_INDEX_REL = RAW_DIR_REL / "master_index.json"

# ✅ builder input (stable)
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


def run_external_collector(mode: str, start: Optional[str], end: Optional[str], recent_days: Optional[int]) -> None:
    """
    ✅ 원래 '크레마 수집기'가 별도 파일로 남아있다면 그걸 실행해서 site/data/reviews.json을 만들게 함.
    - 경로 우선순위:
      1) ENV: CREMA_COLLECTOR_PATH
      2) scripts/crema_voc_collector.py (있으면)
      3) 실행 안 함(이미 site/data/reviews.json이 만들어졌다고 가정)
    """
    custom = (os.getenv("CREMA_COLLECTOR_PATH", "") or "").strip()
    candidates = []
    if custom:
        candidates.append(ROOT / custom)
    candidates.append(ROOT / "scripts" / "crema_voc_collector.py")

    collector = None
    for c in candidates:
        if c.exists():
            collector = c
            break

    if collector is None:
        # collector가 없으면 "이미 site/data/reviews.json이 존재해야 함"
        print("[INFO] No external collector found. Expect site/data/reviews.json to already exist.")
        return

    cmd = ["python", "-u", str(collector), "--mode", mode]
    if mode == "backfill":
        if not start or not end:
            raise ValueError("backfill requires --start and --end")
        cmd += ["--start", start, "--end", end]
        # chunk-days는 외부 collector가 지원할 때만 의미 있는데, 여기서는 넘기지 않음(충돌 방지)
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


def write_builder_input(rows: List[Dict[str, Any]]) -> None:
    # ✅ build_voc_dashboard.py가 바로 먹을 input
    write_json(BUILDER_INPUT, {"reviews": rows})


# ----------------------------
# Main
# ----------------------------
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
        "builder_input": str(BUILDER_INPUT_REL),
    }

    try:
        if args.mode == "daily":
            # 1) 외부 collector 실행(있으면)
            run_external_collector(mode="daily", start=None, end=None, recent_days=args.recent_days)

            # 2) output read
            rows = read_crema_output()

            # ✅ 3) builder input 생성 (이게 핵심)
            write_builder_input(rows)

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

            all_rows: List[Dict[str, Any]] = []

            for (cs, ce) in daterange_chunks(start_d, end_d, args.chunk_days):
                cs_s = fmt_ymd(cs)
                ce_s = fmt_ymd(ce)
                try:
                    run_external_collector(mode="backfill", start=cs_s, end=ce_s, recent_days=None)
                    rows = read_crema_output()
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

            # ✅ backfill도 마지막에 builder input 생성
            write_builder_input(all_rows)

        save_master_index(idx)

        # cleanup
        if (not args.keep_site_output) and CREMA_OUTPUT_JSON.exists():
            try:
                CREMA_OUTPUT_JSON.unlink()
            except Exception:
                pass

        run_meta["finished_at"] = now_kst().strftime("%Y-%m-%d %H:%M:%S")
        collector_health(run_meta)

        print("[OK] Crema collection + accumulation done.")
        print(f"- Builder input: {BUILDER_INPUT}")
        print(f"- Master jsonl:  {MASTER_JSONL}")
        print(f"- Master index:  {MASTER_INDEX}")
        print(f"- Daily snaps:   {DAILY_DIR}")
        print(f"- Health:        {HEALTH_DIR / 'latest_health.json'}")
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