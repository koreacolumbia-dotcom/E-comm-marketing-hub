"""Validate published competitor snapshots without claiming inaccessible pages are absent."""
import json
import sys
from urllib.parse import urlsplit
from pathlib import Path

ROOT = Path("reports/competitor_content")

def validate(root=ROOT):
    feed = json.loads((root / "latest.json").read_text(encoding="utf-8"))
    summary = json.loads((root / "strategy-summary.json").read_text(encoding="utf-8"))
    frontier = json.loads((root / "crawl-frontier.json").read_text(encoding="utf-8"))
    brands = feed.get("brands", [])
    summary_brands = summary.get("brands", [])
    frontier_brands = frontier.get("brands", {})
    assert len(brands) == len(summary_brands) == len(frontier_brands) == 24, "24-brand scope mismatch"
    names = lambda xs: {b["brand"] for b in xs}
    assert names(brands) == names(summary_brands) == set(frontier_brands), "brand identity mismatch"
    assert feed.get("checked_at_kst") == summary.get("source_checked_at"), "stale summary"
    assert summary.get("quality", {}).get("retained_unique_count", 0) >= 0
    assert len(names(brands)) == 24, "duplicate brand identities"
    quality = summary.get("quality", {})
    assert quality.get("retained_unique_count", 0) >= quality.get("unique_count", 0), "retained archive smaller than current unique sample"
    assert quality.get("unique_count", 0) <= quality.get("raw_count", 0), "deduplication increased current item count"
    seen_urls = set()
    for brand, state in frontier_brands.items():
        assert isinstance(state.get("pending", []), list), f"{brand}: invalid queue"
        assert isinstance(state.get("url_ledger", {}), dict), f"{brand}: invalid ledger"
        for url, record in state.get("url_ledger", {}).items():
            parsed = urlsplit(url)
            assert parsed.scheme == "https" and parsed.hostname, f"{brand}: unsafe ledger URL"
            assert not parsed.username and not parsed.password and not parsed.fragment, f"{brand}: unsafe ledger authority"
            assert isinstance(record, dict) and isinstance(record.get("state"), str), f"{brand}: invalid ledger entry"
        pending = state.get("pending", [])
        assert len(pending) == len(set(pending)), f"{brand}: duplicate pending URLs"
        for url in pending:
            assert url not in seen_urls, f"cross-brand duplicate pending URL: {brand}: {url}"
            seen_urls.add(url)
            parsed = urlsplit(url)
            assert parsed.scheme == "https" and parsed.hostname, f"{brand}: unsafe pending URL"
            assert not parsed.username and not parsed.password and not parsed.fragment, f"{brand}: unsafe URL authority or fragment"
    for item in summary_brands:
        coverage = item.get("url_coverage") or {}
        known = coverage.get("known_urls", 0)
        observed = coverage.get("observed_urls", 0)
        assert 0 <= observed <= known, f"{item['brand']}: invalid URL coverage"
        assert 0 <= coverage.get("observed_pct", 0) <= 100, f"{item['brand']}: invalid percent"
        if known:
            expected = round(100 * observed / known, 1)
            assert abs(coverage.get("observed_pct", 0) - expected) <= 0.11, f"{item['brand']}: coverage percentage mismatch"
        assert coverage.get("unobserved_urls", 0) <= known, f"{item['brand']}: invalid unobserved count"
    print(f"snapshot OK: {len(brands)} brands, {summary['quality']['retained_unique_count']} retained items")
    return True

if __name__ == "__main__":
    try:
        validate()
    except (AssertionError, KeyError, ValueError, OSError) as exc:
        print(f"snapshot validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
