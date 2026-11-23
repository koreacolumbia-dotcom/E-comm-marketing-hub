"""Validate published competitor snapshots without claiming inaccessible pages are absent."""
import json
import sys
from urllib.parse import urlsplit
import ipaddress
from pathlib import Path

ROOT = Path("reports/competitor_content")

def validate_public_url(url, brand, kind):
    """Reject unsafe authority, local addresses, and non-standard HTTPS ports."""
    try:
        parsed = urlsplit(url)
        hostname = parsed.hostname
        port = parsed.port
    except ValueError as exc:
        raise AssertionError(f"{brand}: malformed {kind} URL") from exc
    assert parsed.scheme == "https" and hostname, f"{brand}: unsafe {kind} URL"
    assert port in (None, 443), f"{brand}: nonstandard HTTPS port in {kind}"
    assert not parsed.username and not parsed.password and not parsed.fragment, f"{brand}: unsafe {kind} authority"
    assert not hostname.endswith((".local", ".internal", ".localhost", ".test", ".invalid")), f"{brand}: local {kind} host"
    assert hostname != "localhost", f"{brand}: localhost {kind} host"
    try:
        ip = ipaddress.ip_address(hostname)
    except ValueError:
        pass
    else:
        assert ip.is_global, f"{brand}: non-public IP in {kind}"
    return True

def validate_content_composition(item, brand):
    composition=item.get("content_composition")
    if composition is None:
        return True  # Existing retained observations may predate this field.
    assert isinstance(composition,dict), f"{brand}: malformed content composition"
    status=composition.get("status")
    assert status in {"unverified","dom_observed"}, f"{brand}: invalid composition status"
    if status=="dom_observed":
        assert composition.get("source")=="rendered_dom", f"{brand}: DOM claim without DOM source"
        metrics=item.get("page_metrics") or {}
        assert metrics.get("source")=="rendered_dom", f"{brand}: DOM claim without measured metrics"
        assert composition.get("sequence",[])==[x for x in metrics.get("sequence",[]) if x in {"HEADING","IMAGE","VIDEO","PRODUCT","COPY","CTA"}][:16], f"{brand}: composition sequence mismatch"
        for feature in composition.get("features",[]):
            assert feature.get("kind") in {"image_blocks","video_blocks","product_links","cta_controls"}, f"{brand}: unsupported feature"
            assert isinstance(feature.get("count"),int) and feature["count"]>0, f"{brand}: invalid feature count"
            expected_keys={"image_blocks":"images","video_blocks":"videos","product_links":"product_links","cta_controls":"cta_count"}
            measured=metrics.get(expected_keys[feature["kind"]])
            assert isinstance(measured,int) and not isinstance(measured,bool) and measured==feature["count"], f"{brand}: fabricated composition count for {feature['kind']}"
        kinds=[f["kind"] for f in composition.get("features",[])]
        assert len(kinds)==len(set(kinds)), f"{brand}: duplicate composition feature kinds"
        assert set(kinds)=={kind for kind,key in {"image_blocks":"images","video_blocks":"videos","product_links":"product_links","cta_controls":"cta_count"}.items() if isinstance(metrics.get(key),int) and not isinstance(metrics.get(key),bool) and metrics[key]>0}, f"{brand}: missing measured composition features"
    else:
        assert not composition.get("features") and not composition.get("sequence"), f"{brand}: unverified structure cannot claim features"
    return True

def validate_placement_record(item, brand):
    """Reject unsupported claims that a page URL proves an in-page content module."""
    evidence = item.get("module_verified")
    kind = item.get("page_kind")
    confidence = item.get("page_kind_confidence")
    method = item.get("evidence_method")
    if kind is None and evidence is None:
        return True  # Legacy retained records predate placement metadata.
    assert kind in {"homepage", "pdp_page", "campaign_page", "editorial_page", "unknown"}, (
        f"{brand}: unknown page placement {kind!r}")
    assert confidence in {"url_inferred", "dom_observed", "manual_verified"}, (
        f"{brand}: unknown placement confidence")
    assert isinstance(evidence, bool), f"{brand}: module verification must be boolean"
    if evidence:
        assert method in {"dom_module_observed", "screenshot_verified", "manual_review"}, (
            f"{brand}: module verified without page evidence")
        assert item.get("module_kind") not in (None, "", "unverified"), (
            f"{brand}: verified module lacks type")
    if method == "url_path_only":
        assert not evidence, f"{brand}: URL-only evidence cannot prove a module"
        assert confidence == "url_inferred", f"{brand}: URL-only confidence overstated"
    return True

def self_test():
    assert validate_content_composition({"content_composition":{"status":"dom_observed","source":"rendered_dom","sequence":["IMAGE"],"features":[{"kind":"image_blocks","count":2}]},"page_metrics":{"source":"rendered_dom","sequence":["IMAGE"],"images":2}},"K2")
    try:
        validate_content_composition({"content_composition":{"status":"dom_observed","source":"rendered_dom","sequence":["IMAGE"],"features":[{"kind":"image_blocks","count":9}]},"page_metrics":{"source":"rendered_dom","sequence":["IMAGE"],"images":2}},"K2")
    except AssertionError:
        pass
    else:
        raise AssertionError("fabricated image count accepted")
    try:
        validate_content_composition({"content_composition":{"status":"dom_observed","source":"rendered_dom","sequence":["IMAGE"],"features":[]},"page_metrics":{}},"K2")
    except AssertionError:
        pass
    else:
        raise AssertionError("unsupported DOM composition accepted")
    assert validate_placement_record({"page_kind":"pdp_page","page_kind_confidence":"url_inferred","module_verified":False,"module_kind":"unverified","evidence_method":"url_path_only"},"K2")
    for invalid in (
        {"page_kind":"pdp_page","page_kind_confidence":"url_inferred","module_verified":True,"module_kind":"ugc_gallery","evidence_method":"url_path_only"},
        {"page_kind":"unknown","page_kind_confidence":"url_inferred","module_verified":True,"module_kind":"ugc_gallery","evidence_method":"dom_module_observed"},
    ):
        try:
            validate_placement_record(invalid,"K2")
        except AssertionError:
            pass
        else:
            raise AssertionError("unsupported placement verification accepted")
    assert validate_public_url("https://www.k2.co.kr/story?a=1", "K2", "pending")
    assert validate_public_url("https://www.k2.co.kr:443/story", "K2", "pending")
    for bad in ("http://www.k2.co.kr/", "https://localhost/", "https://127.0.0.1/",
                "https://10.0.0.1/", "https://www.k2.co.kr:8443/",
                "https://user:pass@www.k2.co.kr/", "https://www.k2.co.kr/#frag",
                "https://[::1]/", "https://www.k2.co.kr:bad/",
                "https://192.168.1.10/", "https://169.254.169.254/",
                "https://100.64.0.1/", "https://example.local/",
                "https://example.internal/"):
        try:
            validate_public_url(bad, "K2", "pending")
        except AssertionError:
            continue
        raise AssertionError(f"unsafe URL accepted: {bad}")


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
    for b in brands:
        for item in b.get("items", []):
            assert isinstance(item, dict), f"{b['brand']}: invalid content item"
            validate_placement_record(item, b["brand"])
            validate_content_composition(item, b["brand"])
    seen_urls = set()
    for brand, state in frontier_brands.items():
        assert isinstance(state.get("pending", []), list), f"{brand}: invalid queue"
        assert isinstance(state.get("url_ledger", {}), dict), f"{brand}: invalid ledger"
        for url, record in state.get("url_ledger", {}).items():
            validate_public_url(url, brand, "ledger")
            assert isinstance(record, dict) and isinstance(record.get("state"), str), f"{brand}: invalid ledger entry"
        pending = state.get("pending", [])
        assert len(pending) == len(set(pending)), f"{brand}: duplicate pending URLs"
        for url in pending:
            assert url not in seen_urls, f"cross-brand duplicate pending URL: {brand}: {url}"
            seen_urls.add(url)
            validate_public_url(url, brand, "pending")
    # Frontier URLs must be unique after stripping tracking parameters, not only byte-identical.
    # Distinct editorial IDs and functional query parameters are deliberately retained.
    from urllib.parse import parse_qsl, urlencode, urlunsplit
    import re
    tracking = re.compile(r"^(?:utm_[a-z0-9_]+|gclid|dclid|gbraid|wbraid|fbclid|msclkid|igshid|mc_cid|mc_eid|_ga|_gl|ref|tracking)$", re.I)
    def canonical_frontier(url):
        p = urlsplit(url)
        q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True) if not tracking.fullmatch(k)]
        return urlunsplit((p.scheme.lower(), p.netloc.lower(), p.path or "/", urlencode(sorted(q)), ""))
    canonical_owners = {}
    for brand, state in frontier_brands.items():
        pending = state.get("pending", [])
        normalized = [canonical_frontier(url) for url in pending]
        assert len(normalized) == len(set(normalized)), f"{brand}: tracking-only duplicate pending URLs"
        for url in normalized:
            previous_owner = canonical_owners.setdefault(url, brand)
            assert previous_owner == brand, f"cross-brand canonical duplicate: {previous_owner} / {brand}: {url}"
    # Validate ledger state accounting independently of the dashboard's summary.
    # A blocked or timed-out page must never be counted as a successfully observed page.
    for brand, state in frontier_brands.items():
        ledger = state.get("url_ledger", {})
        for url, record in ledger.items():
            status = record.get("state")
            assert status in {"observed", "partial_observed", "unobserved", "timeout", "error", "not_found", "access_blocked", "policy_denied", "blocked", "policy_blocked", "skipped", "restricted"}, f"{brand}: unknown ledger state {status!r}"
    # Expose independently audited crawl outcomes. A discovered URL is not a
    # successful observation, and blocked/error URLs must remain visible.
    from collections import Counter
    audit = Counter(record["state"] for state in frontier_brands.values()
                    for record in state.get("url_ledger", {}).values())
    full = audit["observed"]
    partial = audit["partial_observed"]
    failures = sum(audit[key] for key in ("error", "timeout", "not_found",
                                           "access_blocked", "policy_denied",
                                           "blocked", "policy_blocked", "restricted"))
    print(f"frontier audit: fully observed={full}, partial={partial}, "
          f"unobserved={audit['unobserved']}, blocked/error={failures}; "
          "partial/blocked are not full crawl success")
    # Coverage is observational, not proof that the source has no content.
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
    # Persist a per-brand operational diagnosis rather than hiding 24-brand failures
    # behind the aggregate registration count. These are observations, not success claims.
    health = []
    summary_by_brand = {row["brand"]: row for row in summary_brands}
    for brand, state in sorted(frontier_brands.items()):
        counts = Counter(record["state"] for record in state.get("url_ledger", {}).values())
        coverage = summary_by_brand[brand].get("url_coverage") or {}
        pending_count = len(state.get("pending", []))
        observed_count = counts["observed"]
        partial_count = counts["partial_observed"]
        blocked_count = sum(counts[k] for k in ("access_blocked", "policy_denied", "blocked", "policy_blocked", "restricted"))
        error_count = sum(counts[k] for k in ("error", "timeout", "not_found"))
        health.append({
            "brand": brand,
            "pending_urls": pending_count,
            "observed_urls_in_ledger": observed_count,
            "partial_urls_in_ledger": partial_count,
            "blocked_urls_in_ledger": blocked_count,
            "error_urls_in_ledger": error_count,
            "known_urls": coverage.get("known_urls"),
            "observed_urls": coverage.get("observed_urls"),
            "observed_pct": coverage.get("observed_pct"),
            "status": ("blocked" if blocked_count and not observed_count else
                       "no_observation" if not observed_count and not partial_count else
                       "backlog" if pending_count else "observed"),
        })
    health.sort(key=lambda row: (
        0 if row["status"] in ("blocked", "no_observation") else 1,
        -row["pending_urls"], row["brand"]))
    diagnostic = {
        "schema_version": 1,
        "source_checked_at": feed["checked_at_kst"],
        "scope_brands": len(health),
        "pending_urls": sum(row["pending_urls"] for row in health),
        "brands_without_full_observation": sum(not row["observed_urls_in_ledger"] for row in health),
        "brands": health,
        "note": "Ledger states are observed crawl outcomes; missing observations do not prove absent content.",
    }
    (root / "crawl-health.json").write_text(
        json.dumps(diagnostic, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print("priority crawl gaps:", [(row["brand"], row["status"], row["pending_urls"])
                                  for row in health[:8]])
    print(f"snapshot OK: {len(brands)} brands, {summary['quality']['retained_unique_count']} retained items")
    return True

if __name__ == "__main__":
    try:
        self_test()
        validate()
    except (AssertionError, KeyError, ValueError, OSError) as exc:
        print(f"snapshot validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
