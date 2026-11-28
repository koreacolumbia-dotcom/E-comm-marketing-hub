"""Conflict-safe publishing for independently collected official-content evidence.

Never force-pushes. A newer remote feed wins; a newer incoming feed carries forward
prior 30-day observations and URL frontier before regenerating analytics.
"""
import json
import shutil
import subprocess
import sys
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

REPORT = Path("reports/competitor_content")
FILES = ("latest.json", "crawl-frontier.json", "calendar.json")
MAX_PENDING = 5000
MAX_LEDGER = 5000


def command(*args, check=True):
    p = subprocess.run(args, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if check and p.returncode:
        raise RuntimeError(f"{' '.join(args)} failed: {p.stderr[-1500:]}")
    return p


def read_json(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")


def merge_history(incoming, remote, checked):
    """Retain distinct directly observed content. Prefer the newest actual observation."""
    cutoff = (datetime.fromisoformat(checked).date() - timedelta(days=30)).isoformat()
    history = {}
    for x in [*(remote or []), *(incoming or [])]:
        if not isinstance(x, dict) or not x.get("id"):
            continue
        if (x.get("last_seen_at") or "")[:10] < cutoff:
            continue
        old = history.get(x["id"])
        if old:
            first = min(filter(None, (old.get("first_seen_at"), x.get("first_seen_at"))), default=None)
            newer = x if (x.get("last_seen_at") or "") >= (old.get("last_seen_at") or "") else old
            entry = dict(newer)
            if first:
                entry["first_seen_at"] = first
            # Keep previously measured image metadata when fingerprint has not changed.
            if old.get("fingerprint") == x.get("fingerprint"):
                for key in ("visual_signature", "visual_palette", "image_evidence_file"):
                    entry.setdefault(key, old.get(key) or x.get(key))
        else:
            entry = dict(x)
        history[x["id"]] = entry
    return list(history.values())


def merge_brand_archives(incoming, remote, checked):
    cutoff = (datetime.fromisoformat(checked).date() - timedelta(days=30)).isoformat()
    old_by_brand = {b.get("brand"): b for b in remote or []}
    for brand in incoming:
        prev = old_by_brand.get(brand.get("brand"), {})
        items = {}
        for x in [*(prev.get("archive_entries") or []), *(brand.get("archive_entries") or [])]:
            if (x.get("last_seen_at") or "")[:10] < cutoff:
                continue
            key = (x.get("content_type"), x.get("archive_id") or x.get("title"))
            old = items.get(key)
            if not old or (old.get("last_seen_at") or "") <= (x.get("last_seen_at") or ""):
                items[key] = x
        brand["archive_entries"] = list(items.values())
        brand["archive_entries_retained"] = len(items)
        if not brand.get("last_success_at") and prev.get("last_success_at"):
            brand["last_success_at"] = prev["last_success_at"]
    return incoming


def merge_frontier(incoming, remote):
    """Do not lose pending URLs or observed URL evidence when jobs race."""
    result = {"schema_version": 1, "brands": {}}
    left, right = (incoming or {}).get("brands") or {}, (remote or {}).get("brands") or {}
    for brand in sorted(set(left) | set(right)):
        a, b = left.get(brand) or {}, right.get(brand) or {}
        ledger = {}
        for source in (b.get("url_ledger") or {}, a.get("url_ledger") or {}):
            for url, entry in source.items():
                if not isinstance(entry, dict):
                    continue
                old = ledger.get(url)
                if old is None or (entry.get("at") or "") >= (old.get("at") or ""):
                    ledger[url] = entry
        sorted_ledger = sorted(ledger.items(), key=lambda kv: kv[1].get("at") or "", reverse=True)
        ledger = dict(sorted_ledger[:MAX_LEDGER])
        pending = list(dict.fromkeys([*(a.get("pending") or []), *(b.get("pending") or [])]))
        pending = [u for u in pending if ledger.get(u, {}).get("state") != "observed"][:MAX_PENDING]
        sm = dict(b.get("sitemap_state") or {})
        for origin, state in (a.get("sitemap_state") or {}).items():
            old = sm.get(origin) or {}
            if (state.get("started_at") or "") >= (old.get("started_at") or ""):
                sm[origin] = state
        result["brands"][brand] = {
            "pending": pending,
            "url_ledger": ledger,
            "sitemap_state": sm,
            "checked_at_kst": max(a.get("checked_at_kst") or "", b.get("checked_at_kst") or ""),
        }
    return result


def rebuild_strategy():
    js = (
        "import {readFileSync,writeFileSync} from 'node:fs';"
        "import {analyze} from './reports/competitor_content/strategy.mjs';"
        "const p='reports/competitor_content/';"
        "const r=analyze(JSON.parse(readFileSync(p+'latest.json','utf8')));"
        "delete r.items;delete r.history;"
        "writeFileSync(p+'strategy-summary.json',JSON.stringify(r,null,2));"
        "const q={schema_version:1,source_checked_at:r.source_checked_at,"
        "brand_count:(r.brands||[]).length,quality:r.quality};"
        "if(!q.source_checked_at||q.brand_count!==24||!q.quality)"
        "throw Error('Refusing incomplete quality snapshot');"
        "writeFileSync(p+'latest-quality.json',JSON.stringify(q,null,2)+'\\n');"
        "const rows=r.archive_classification,brands=r.archive_classification_by_brand;"
        "if(!Array.isArray(rows)||!brands||rows.length!==r.quality.retained_unique_count||"
        "Object.keys(brands).length!==24||Object.values(brands).reduce((n,b)=>n+b.total,0)!==rows.length)"
        "throw Error('Archive classification scope mismatch');"
        "const review_queue=rows.filter(x=>x.purpose.code==='UNCLASSIFIED').map(x=>({id:x.id,brand:x.brand,title:x.title,url:x.url,reason:'콘텐츠 목적을 확정할 개별 본문 또는 DOM 근거 부족',next_action:'공식 원본 페이지의 개별 콘텐츠 영역 재확인'}));"
        "const archive={source_checked_at:r.source_checked_at,quality:r.quality,brands,rows,review_queue,review_summary:{total:rows.length,classified:rows.length-review_queue.length,pending:review_queue.length}};"
        "writeFileSync(p+'archive-classification.json',JSON.stringify(archive,null,2)+'\\n');"
        "console.log(JSON.stringify(q));"
    )
    command("node", "--input-type=module", "-e", js)


def self_test():
    ts = "2026-11-19T09:00:00+09:00"
    a = {"id": "one", "last_seen_at": ts, "first_seen_at": ts}
    old = {"id": "one", "last_seen_at": "2026-11-18T09:00:00+09:00",
           "first_seen_at": "2026-11-10T09:00:00+09:00"}
    combined = merge_history([a], [old], ts)
    assert len(combined) == 1
    assert combined[0]["first_seen_at"] == old["first_seen_at"]
    f = merge_frontier(
        {"brands": {"A": {"pending": ["one"], "url_ledger": {"one": {"state": "observed", "at": ts}}}}},
        {"brands": {"A": {"pending": ["two"], "url_ledger": {}}}},
    )
    assert f["brands"]["A"]["pending"] == ["two"]
    print("Safe content publish regression tests passed")


def publish():
    with tempfile.TemporaryDirectory(prefix="csk-content-publish-") as temp:
        staged = Path(temp)
        for name in FILES:
            source = REPORT / name
            if source.exists():
                shutil.copy2(source, staged / name)
        incoming = read_json(staged / "latest.json")
        checked = incoming.get("checked_at_kst")
        if not checked:
            raise RuntimeError("No completed crawl feed; refusing to publish")
        command("git", "config", "user.name", "github-actions[bot]")
        command("git", "config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com")
        for attempt in range(1, 5):
            command("git", "fetch", "origin", "main")
            command("git", "reset", "--hard", "origin/main")
            remote = read_json(REPORT / "latest.json")
            previous_time = remote.get("checked_at_kst") or ""
            if previous_time >= checked:
                print(f"Remote crawl {previous_time} is at least as new as {checked}; keeping remote")
                return
            merged = dict(incoming)
            merged["history"] = merge_history(incoming.get("history"), remote.get("history"), checked)
            merged["brands"] = merge_brand_archives(incoming.get("brands") or [], remote.get("brands"), checked)
            write_json(REPORT / "latest.json", merged)
            write_json(
                REPORT / "crawl-frontier.json",
                merge_frontier(read_json(staged / "crawl-frontier.json"),
                               read_json(REPORT / "crawl-frontier.json")),
            )
            # The existing calendar builder protects prior 180-day observations.
            sys.path.insert(0, str(Path("scripts/competitor_content").resolve()))
            from calendar_archive import build
            write_json(REPORT / "calendar.json", build(merged, read_json(REPORT / "calendar.json")))
            rebuild_strategy()
            # Validate the *merged* snapshot, not only the pre-merge crawl artifact.
            # Refuse publication if concurrent writers introduced unsafe URLs,
            # inconsistent brand coverage, or mismatched analysis timestamps.
            from validate_snapshot import validate
            validate(REPORT)
            quality_snapshot = read_json(REPORT / "latest-quality.json")
            if (quality_snapshot.get("source_checked_at") != merged.get("checked_at_kst")
                    or quality_snapshot.get("brand_count") != 24
                    or quality_snapshot.get("quality") != read_json(REPORT / "strategy-summary.json").get("quality")):
                raise RuntimeError("Merged quality snapshot does not match source feed")
            command("git", "add", "reports/competitor_content/latest.json",
                    "reports/competitor_content/crawl-frontier.json",
                    "reports/competitor_content/calendar.json",
                    "reports/competitor_content/strategy-summary.json",
                    "reports/competitor_content/latest-quality.json",
                    "reports/competitor_content/archive-classification.json",
                    "reports/competitor_content/crawl-health.json")
            if not command("git", "diff", "--cached", "--quiet", check=False).returncode:
                print("Nothing changed; skip publication")
                return
            command("git", "commit", "-m", f"Publish conflict-safe official content observations (attempt {attempt})")
            pushed = command("git", "push", "origin", "HEAD:main", check=False)
            if pushed.returncode == 0:
                print(f"Published direct crawl {checked} successfully; attempt {attempt}")
                return
            print(f"Push raced another writer; retry {attempt}/4: {pushed.stderr[-350:]}")
        raise RuntimeError("Unable to publish after 4 non-force attempts; artifact remains available")


if __name__ == "__main__":
    if "--self-test" in sys.argv:
        self_test()
    else:
        publish()
