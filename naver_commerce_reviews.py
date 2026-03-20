from __future__ import annotations

import argparse
import json
import os
import random
import re
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import requests


TRUE_VALUES = {"1", "true", "yes", "y", "on"}
KST = timezone(timedelta(hours=9))

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


class CrawlStop(RuntimeError):
    pass


def env_flag(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "")).strip().lower()
    if not raw:
        return default
    return raw in TRUE_VALUES


def env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return default
    try:
        return int(raw)
    except Exception:
        return default


def log(msg: str) -> None:
    print(msg, flush=True)


def load_rows(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        rows = data.get("reviews") or data.get("items") or []
    else:
        rows = data
    if not isinstance(rows, list):
        return []
    return [r for r in rows if isinstance(r, dict)]


def dump_reviews(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"reviews": rows}, ensure_ascii=False, indent=2), encoding="utf-8")


def normalize_space(value: Any) -> str:
    return " ".join(str(value or "").split())


def normalize_source(value: Any) -> str:
    text = str(value or "").strip()
    low = text.lower()
    if "musinsa" in low:
        return "Musinsa"
    if "naver" in low:
        return "Naver"
    if "official" in low:
        return "Official"
    return text or "Unknown"


def unique_key(row: dict[str, Any]) -> tuple[str, ...]:
    source = normalize_source(row.get("source"))
    rid = str(row.get("id") or "").strip()
    if rid:
        return (source, rid)
    return (
        source,
        str(row.get("product_code") or "").strip(),
        str(row.get("created_at") or "").strip(),
        normalize_space(row.get("text") or ""),
    )


def merge_rows(base_rows: list[dict[str, Any]], extra_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, ...]] = set()
    for row in list(base_rows) + list(extra_rows):
        if not isinstance(row, dict):
            continue
        key = unique_key(row)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    out.sort(key=lambda r: str(r.get("created_at") or ""), reverse=True)
    return out


def normalize_rating(value: Any) -> int:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return 0


def normalize_created_at(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = text.replace("Z", "+00:00")
    if " " in text and "T" not in text:
        text = text.replace(" ", "T", 1)
    if len(text) == 10:
        text = f"{text}T00:00:00+09:00"
    try:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KST)
        else:
            dt = dt.astimezone(KST)
        return dt.isoformat(timespec="seconds")
    except Exception:
        return text


def parse_date_from_created_at(value: Any) -> date | None:
    text = normalize_created_at(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text).astimezone(KST).date()
    except Exception:
        return None


def parse_date_only(value: str) -> date:
    return date.fromisoformat(str(value).strip()[:10])


def in_window(created_at: str, start_date: date, end_date: date) -> bool:
    created = parse_date_from_created_at(created_at)
    if created is None:
        return False
    return start_date <= created <= end_date


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    # Ignore broken local proxy env vars; the CI runner should talk directly.
    session.trust_env = False
    return session


def clamp_non_negative(value: int) -> int:
    return max(0, int(value))


def safe_sleep(base_ms: int, jitter_ms: int) -> None:
    delay = clamp_non_negative(base_ms) + random.uniform(0, clamp_non_negative(jitter_ms))
    if delay > 0:
        time.sleep(delay / 1000.0)


def parse_retry_after_seconds(value: Any) -> int:
    raw = str(value or "").strip()
    if not raw:
        return 0
    try:
        return max(0, int(float(raw)))
    except Exception:
        return 0


def get_with_backoff(
    session: requests.Session,
    url: str,
    *,
    timeout: int,
    label: str,
    headers: dict[str, str] | None = None,
    max_attempts: int = 2,
) -> requests.Response:
    backoff_sec = env_int("MARKETPLACE_BACKOFF_SEC", 12)
    jitter_ms = env_int("MARKETPLACE_REQUEST_JITTER_MS", 500)
    last_response: requests.Response | None = None
    last_error: Exception | None = None

    for attempt in range(1, max(1, max_attempts) + 1):
        try:
            response = session.get(url, headers=headers, timeout=timeout)
            last_response = response
            if response.status_code not in (429, 500, 502, 503, 504):
                return response
            retry_after = parse_retry_after_seconds(response.headers.get("Retry-After"))
            if attempt >= max_attempts:
                break
            wait_sec = retry_after or backoff_sec * attempt
            log(f"[WARN] {label} HTTP {response.status_code} -> retry in {wait_sec}s (attempt {attempt}/{max_attempts})")
            safe_sleep(wait_sec * 1000, jitter_ms)
        except requests.RequestException as exc:
            last_error = exc
            if attempt >= max_attempts:
                break
            wait_sec = max(2, backoff_sec // 2) * attempt
            log(f"[WARN] {label} request error -> retry in {wait_sec}s: {type(exc).__name__}: {exc}")
            safe_sleep(wait_sec * 1000, jitter_ms)

    if last_response is not None and last_response.status_code in (403, 429):
        raise CrawlStop(f"{label} returned HTTP {last_response.status_code}")
    if last_response is not None:
        last_response.raise_for_status()
        return last_response
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"{label} request failed without response")


def fetch_html_with_playwright(
    url: str,
    *,
    label: str,
    wait_selector: str = "",
    timeout_ms: int | None = None,
    settle_ms: int | None = None,
) -> str:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise CrawlStop(f"{label} Playwright unavailable: {type(exc).__name__}: {exc}") from exc

    nav_timeout_ms = timeout_ms or env_int("NAVER_RENDER_TIMEOUT_MS", 45000)
    nav_settle_ms = settle_ms or env_int("NAVER_RENDER_WAIT_MS", 3500)
    selector_timeout_ms = env_int("NAVER_RENDER_SELECTOR_TIMEOUT_MS", 12000)
    warmup_url = str(os.getenv("NAVER_WARMUP_URL", "https://brand.naver.com")).strip() or "https://brand.naver.com"
    scroll_rounds = env_int("NAVER_RENDER_SCROLL_ROUNDS", 5)
    scroll_wait_ms = env_int("NAVER_RENDER_SCROLL_WAIT_MS", 900)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=DEFAULT_HEADERS["User-Agent"],
            locale="ko-KR",
            timezone_id="Asia/Seoul",
            viewport={"width": 1440, "height": 2200},
            extra_http_headers={
                "Accept-Language": DEFAULT_HEADERS["Accept-Language"],
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            },
        )
        page = context.new_page()
        try:
            if warmup_url and warmup_url != url:
                try:
                    page.goto(warmup_url, wait_until="domcontentloaded", timeout=nav_timeout_ms)
                    page.wait_for_timeout(1200 + random.randint(0, 800))
                except Exception:
                    pass

            page.goto(url, wait_until="domcontentloaded", timeout=nav_timeout_ms)
            if wait_selector:
                try:
                    page.wait_for_selector(wait_selector, timeout=selector_timeout_ms)
                except Exception:
                    pass
            try:
                page.mouse.move(200, 280)
            except Exception:
                pass
            for _ in range(max(1, scroll_rounds)):
                try:
                    page.mouse.wheel(0, 1800)
                except Exception:
                    pass
                page.wait_for_timeout(scroll_wait_ms + random.randint(0, 500))
            page.wait_for_timeout(nav_settle_ms + random.randint(0, 1200))
            html = page.content()
        finally:
            browser.close()
    if not html:
        raise CrawlStop(f"{label} Playwright returned empty HTML")
    return html


def absolute_url(base_url: str, raw_url: Any) -> str:
    text = str(raw_url or "").strip()
    if not text:
        return ""
    if text.startswith("//"):
        return f"https:{text}"
    if text.startswith("http://") or text.startswith("https://"):
        return text
    return urljoin(base_url, text)


def musinsa_image_url(raw_url: Any) -> str:
    text = str(raw_url or "").strip()
    if not text:
        return ""
    if text.startswith("http://") or text.startswith("https://"):
        return text
    if text.startswith("//"):
        return f"https:{text}"
    if text.startswith("/"):
        return f"https://image.msscdn.net{text}"
    return text


def find_first(payload: Any, *paths: tuple[str, ...]) -> Any:
    for path in paths:
        cur = payload
        ok = True
        for key in path:
            if not isinstance(cur, dict):
                ok = False
                break
            cur = cur.get(key)
        if ok and cur not in (None, "", [], {}):
            return cur
    return None


def parse_option_string(text: Any) -> tuple[str, str]:
    raw = normalize_space(text)
    if not raw:
        return "", ""
    option_size = ""
    option_color = ""
    size_match = re.search(r"(?:size|사이즈)\s*[:：]\s*([^/|,]+)", raw, flags=re.I)
    color_match = re.search(r"(?:color|컬러|색상)\s*[:：]\s*([^/|,]+)", raw, flags=re.I)
    if size_match:
        option_size = normalize_space(size_match.group(1))
    if color_match:
        option_color = normalize_space(color_match.group(1))
    if not option_size:
        parts = [normalize_space(part) for part in re.split(r"[/|,]", raw) if normalize_space(part)]
        if parts:
            option_size = parts[0]
        if len(parts) > 1 and not option_color:
            option_color = parts[1]
    return option_size, option_color


def make_tags(text: str, rating: int) -> list[str]:
    tags: list[str] = []
    if rating >= 4:
        tags.append("pos")
    if rating and rating <= 2:
        tags.extend(["neg", "low"])
    low_text = str(text or "").lower()
    if "작" in low_text or "크" in low_text or "사이즈" in low_text or "size" in low_text:
        if "size" not in tags:
            tags.append("size")
    return tags


def sanitize_js_object(raw: str) -> str:
    text = raw
    text = re.sub(r":\s*undefined\b", ": null", text)
    text = re.sub(r"\[\s*undefined\s*\]", "[null]", text)
    text = re.sub(r",\s*undefined\b", ", null", text)
    text = re.sub(r"\bundefined\s*,", "null,", text)
    return text


def extract_preloaded_state(html: str) -> dict[str, Any]:
    patterns = [
        r"window\.__PRELOADED_STATE__\s*=\s*(\{.*?\})\s*</script>",
        r"window\.__INITIAL_STATE__\s*=\s*(\{.*?\})\s*</script>",
        r"window\.__APOLLO_STATE__\s*=\s*(\{.*?\})\s*</script>",
    ]
    for pattern in patterns:
        match = re.search(pattern, html, flags=re.S)
        if not match:
            continue
        raw = sanitize_js_object(match.group(1))
        try:
            payload = json.loads(raw)
            if isinstance(payload, dict):
                return payload
        except Exception:
            continue
    return {}


def extract_json_scripts(html: str) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    patterns = [
        r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        r'<script[^>]+type="application/json"[^>]*>(.*?)</script>',
        r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>',
    ]
    for pattern in patterns:
        for raw in re.findall(pattern, html, flags=re.S | re.I):
            text = sanitize_js_object(str(raw or '').strip())
            if not text or len(text) < 2:
                continue
            try:
                payload = json.loads(text)
            except Exception:
                continue
            if isinstance(payload, dict):
                payloads.append(payload)
            elif isinstance(payload, list):
                payloads.append({"items": payload})
    return payloads


def gather_naver_payload_candidates(html: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    primary = extract_preloaded_state(html)
    if primary:
        out.append(primary)
    for payload in extract_json_scripts(html):
        if payload:
            out.append(payload)
    return out


def extract_naver_brand_slug(brand_url: str) -> str:
    parsed = urlparse(str(brand_url or '').strip())
    path_parts = [part for part in parsed.path.split('/') if part]
    return path_parts[0] if path_parts else 'columbia'


def extract_naver_product_nos_from_html(html: str) -> list[str]:
    patterns = [
        r'/products/(\d+)',
        r'"productNo"\s*:\s*"?(\d+)"?',
        r'"channelProductNo"\s*:\s*"?(\d+)"?',
        r"data-product-no=[\"']?(\d+)",
        r'data-nclick="[^"]*i:(\d+)',
    ]
    out: list[str] = []
    for pattern in patterns:
        out.extend(re.findall(pattern, html, flags=re.I))
    return dedupe_keep_order(out)


def iter_key_values(node: Any, target_key: str) -> list[Any]:
    found: list[Any] = []
    stack = [node]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            for key, value in cur.items():
                if key == target_key:
                    found.append(value)
                if isinstance(value, (dict, list)):
                    stack.append(value)
        elif isinstance(cur, list):
            for item in cur:
                if isinstance(item, (dict, list)):
                    stack.append(item)
    return found


def dedupe_keep_order(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def collect_leaf_category_ids(node: Any) -> list[str]:
    found: list[str] = []

    def walk(cur: Any) -> None:
        if not isinstance(cur, dict):
            return
        raw_id = str(cur.get("id") or "").strip()
        sub_categories = cur.get("subCategories")
        if isinstance(sub_categories, list) and sub_categories:
            for child in sub_categories:
                if isinstance(child, dict):
                    walk(child)
            return
        if raw_id and raw_id != "0":
            found.append(raw_id)

    walk(node)
    return dedupe_keep_order(found)


def extract_naver_category_ids(payload: dict[str, Any], html: str) -> list[str]:
    category_ids: list[str] = []

    store_tree = payload.get("storeCategoryTree")
    if isinstance(store_tree, dict):
        category_ids.extend(collect_leaf_category_ids(store_tree))

    for entry in payload.get("storeCategories") or []:
        if not isinstance(entry, dict):
            continue
        raw_id = str(entry.get("id") or "").strip()
        if raw_id and raw_id != "0":
            category_ids.append(raw_id)

    category_ids.extend(re.findall(r"/category/([0-9a-f]{32})", html))
    category_ids.extend(re.findall(r'"key":"([0-9a-f]{32})"', html))

    # Crawl leaf categories first, but always include the all-products category as a fallback.
    category_ids = dedupe_keep_order(category_ids)
    preferred = [cid for cid in category_ids if cid == "251598e702a64123a2292f40e6681943"]
    others = [cid for cid in category_ids if cid != "251598e702a64123a2292f40e6681943"]
    return preferred + others


def query_naver_product_urls_from_page(url: str) -> list[str]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return []
    nav_timeout_ms = env_int("NAVER_RENDER_TIMEOUT_MS", 45000)
    selector_timeout_ms = env_int("NAVER_RENDER_SELECTOR_TIMEOUT_MS", 12000)
    scroll_rounds = env_int("NAVER_RENDER_SCROLL_ROUNDS", 5)
    scroll_wait_ms = env_int("NAVER_RENDER_SCROLL_WAIT_MS", 900)
    out: list[str] = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=DEFAULT_HEADERS["User-Agent"],
            locale="ko-KR",
            timezone_id="Asia/Seoul",
            viewport={"width": 1440, "height": 2200},
        )
        page = context.new_page()
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=nav_timeout_ms)
            try:
                page.wait_for_selector("a[href*='/products/']", timeout=selector_timeout_ms)
            except Exception:
                pass
            for _ in range(max(1, scroll_rounds)):
                try:
                    page.mouse.wheel(0, 1800)
                except Exception:
                    pass
                page.wait_for_timeout(scroll_wait_ms + random.randint(0, 500))
            hrefs = page.eval_on_selector_all(
                "a[href*='/products/']",
                "els => els.map(el => el.getAttribute('href') || '')",
            )
            out.extend([absolute_url(url, h) for h in (hrefs or []) if h])
            html = page.content()
            out.extend(extract_naver_product_urls_from_html(html, brand_url=url))
        finally:
            browser.close()
    return dedupe_keep_order(out)


def fetch_naver_html(
    session: requests.Session,
    url: str,
    *,
    timeout: int,
    label: str,
    wait_selector: str = "",
) -> str:
    max_attempts = env_int("NAVER_MAX_ATTEMPTS", 3)
    try:
        response = get_with_backoff(
            session,
            url,
            timeout=timeout,
            label=label,
            max_attempts=max_attempts,
        )
        return response.text
    except CrawlStop as exc:
        log(f"[WARN] {exc} -> fallback to Playwright for {label}")
    except Exception as exc:
        log(f"[WARN] {label} HTTP fetch failed -> fallback to Playwright: {type(exc).__name__}: {exc}")
    return fetch_html_with_playwright(
        url,
        label=label,
        wait_selector=wait_selector,
        timeout_ms=env_int("NAVER_RENDER_TIMEOUT_MS", 45000),
        settle_ms=env_int("NAVER_RENDER_WAIT_MS", 3500),
    )


def extract_category_products(payload: dict[str, Any]) -> dict[str, Any]:
    candidates = iter_key_values(payload, "categoryProducts")
    for item in candidates:
        if isinstance(item, dict):
            return item
    return {}


def normalize_naver_listed_product(product: dict[str, Any], brand_url: str) -> dict[str, Any]:
    product_no = str(
        find_first(product, ("productNo",), ("id",), ("channelProductNo",), ("originProductNo",)) or ""
    ).strip()
    product_url = absolute_url(
        brand_url,
        find_first(product, ("productUrl",), ("url",), ("detailUrl",)) or (f"/columbia/products/{product_no}" if product_no else ""),
    )
    review_count = 0
    for candidate in (
        find_first(product, ("reviewCount",)),
        find_first(product, ("reviewAmount", "totalReviewCount")),
        find_first(product, ("reviewQuantity",)),
    ):
        try:
            review_count = max(review_count, int(candidate))
        except Exception:
            pass
    return {
        "product_no": product_no,
        "product_name": str(find_first(product, ("productName",), ("name",), ("dispName",)) or product_no).strip(),
        "product_url": product_url,
        "image_url": absolute_url(
            brand_url,
            find_first(product, ("representativeImageUrl",), ("imageUrl",), ("image", "url")) or "",
        ),
        "review_count": review_count,
    }


def build_naver_category_url(brand_url: str, category_id: str, page: int = 1) -> str:
    base = brand_url.rstrip("/")
    page_no = max(1, int(page or 1))
    return f"{base}/category/{category_id}?cp={page_no}"


def looks_like_naver_review(node: Any) -> bool:
    if not isinstance(node, dict):
        return False
    text = normalize_space(
        find_first(node, ("reviewContent",), ("content",), ("text",), ("reviewText",)) or ""
    )
    score = find_first(node, ("reviewScore",), ("rating",), ("score",), ("grade",))
    created = find_first(node, ("createDate",), ("createdAt",), ("reviewDate",), ("registerDate",))
    review_id = find_first(node, ("id",), ("reviewId",), ("no",))
    if text and (score not in (None, "") or created or review_id):
        return True
    return False


def extract_review_dicts(node: Any) -> list[dict[str, Any]]:
    reviews: list[dict[str, Any]] = []
    stack = [node]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            if looks_like_naver_review(cur):
                reviews.append(cur)
            for value in cur.values():
                if isinstance(value, (dict, list)):
                    stack.append(value)
        elif isinstance(cur, list):
            for item in cur:
                if isinstance(item, (dict, list)):
                    stack.append(item)
    return reviews


def extract_naver_product_urls_from_html(html: str, brand_url: str) -> list[str]:
    urls: list[str] = []
    for href in re.findall(r"href=[\"']([^\"']+/products/[^\"'?#]+(?:\?[^\"']*)?)", html, flags=re.I):
        urls.append(absolute_url(brand_url, href))
    product_nos = dedupe_keep_order(re.findall(r'/products/(\d+)', html))
    parsed = urlparse(brand_url)
    base = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else brand_url.rstrip('/')
    for product_no in product_nos:
        urls.append(f"{base}/columbia/products/{product_no}")
        urls.append(f"{base}/products/{product_no}")
    return dedupe_keep_order(urls)


def collect_naver_reviews_from_payloads(payloads: list[dict[str, Any]], brand_url: str, product_hint: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for payload in payloads:
        for group in iter_review_product_groups(payload):
            for product in group:
                reviews = product.get("reviews") or []
                if not isinstance(reviews, list):
                    continue
                for review in reviews:
                    if isinstance(review, dict):
                        out.append(normalize_naver_review(review, product, brand_url=brand_url))
        raw_reviews = extract_review_dicts(payload)
        for review in raw_reviews:
            row = normalize_naver_review(review, product_hint or {}, brand_url=brand_url)
            if row.get("id") or row.get("text"):
                out.append(row)
    return merge_rows([], out)


def build_naver_existing_product_success(existing_rows: list[dict[str, Any]] | None) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in existing_rows or []:
        if normalize_source(row.get("source")) != "Naver":
            continue
        product_no = str(row.get("product_code") or "").strip()
        if not product_no:
            product_url = str(row.get("product_url") or "").strip()
            m = re.search(r"/products/(\d+)", product_url)
            if m:
                product_no = m.group(1)
        if not product_no:
            continue
        counts[product_no] = counts.get(product_no, 0) + 1
    return counts


def inspect_naver_product_detail(
    session: requests.Session,
    product: dict[str, Any],
    *,
    brand_url: str,
    start_date: date,
    end_date: date,
    timeout: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    product_url = str(product.get("product_url") or "").strip()
    stats: dict[str, Any] = {
        "payloads": 0,
        "raw_reviews": 0,
        "normalized": 0,
        "with_text": 0,
        "with_created_at": 0,
        "in_window": 0,
        "skip_reason": "",
    }
    if not product_url:
        stats["skip_reason"] = "product_url_missing"
        return [], stats

    html = fetch_naver_html(
        session,
        product_url,
        timeout=timeout,
        label=f"Naver product page {product.get('product_no') or product_url}",
        wait_selector='script',
    )
    payloads = gather_naver_payload_candidates(html)
    stats["payloads"] = len(payloads)
    raw_review_count = 0
    for payload in payloads:
        raw_review_count += len(extract_review_dicts(payload))
    stats["raw_reviews"] = raw_review_count

    normalized_rows = collect_naver_reviews_from_payloads(payloads, brand_url=brand_url, product_hint=product)
    stats["normalized"] = len(normalized_rows)
    stats["with_text"] = sum(1 for row in normalized_rows if str(row.get("text") or "").strip())
    stats["with_created_at"] = sum(1 for row in normalized_rows if str(row.get("created_at") or "").strip())

    in_window_rows = [
        row for row in normalized_rows
        if row.get("created_at") and in_window(str(row.get("created_at") or ""), start_date, end_date)
    ]
    stats["in_window"] = len(in_window_rows)

    if in_window_rows:
        return in_window_rows, stats

    if not payloads:
        stats["skip_reason"] = "payload_missing"
    elif raw_review_count == 0 and not normalized_rows:
        stats["skip_reason"] = "raw_reviews_empty"
    elif normalized_rows and stats["with_created_at"] == 0:
        stats["skip_reason"] = "created_at_missing"
    elif normalized_rows and stats["with_text"] == 0:
        stats["skip_reason"] = "text_empty"
    elif normalized_rows and not in_window_rows:
        stats["skip_reason"] = "date_out_of_range"
    else:
        stats["skip_reason"] = "normalized_empty"
    return [], stats


def fetch_naver_product_detail_rows(
    session: requests.Session,
    product: dict[str, Any],
    *,
    brand_url: str,
    start_date: date,
    end_date: date,
    timeout: int,
) -> list[dict[str, Any]]:
    rows, _stats = inspect_naver_product_detail(
        session,
        product,
        brand_url=brand_url,
        start_date=start_date,
        end_date=end_date,
        timeout=timeout,
    )
    return rows


def discover_naver_listed_products(brand_url: str, timeout: int, is_backfill: bool) -> list[dict[str, Any]]:
    session = make_session()
    home_html = fetch_naver_html(
        session,
        brand_url,
        timeout=timeout,
        label="Naver brand page",
        wait_selector='script',
    )
    payload_candidates = gather_naver_payload_candidates(home_html)
    category_ids: list[str] = []
    for payload in payload_candidates:
        category_ids.extend(extract_naver_category_ids(payload, home_html))
    category_ids = dedupe_keep_order(category_ids)

    max_categories = (
        env_int("NAVER_MAX_CATEGORIES_BACKFILL", 120)
        if is_backfill
        else env_int("NAVER_MAX_CATEGORIES_DAILY", 36)
    )
    max_pages_per_category = env_int("NAVER_MAX_CATEGORY_PAGES_BACKFILL", 3) if is_backfill else env_int("NAVER_MAX_CATEGORY_PAGES_DAILY", 2)
    delay_ms = env_int("NAVER_CATEGORY_DELAY_MS", env_int("MARKETPLACE_REQUEST_DELAY_MS", 1200))
    jitter_ms = env_int("NAVER_CATEGORY_JITTER_MS", env_int("MARKETPLACE_REQUEST_JITTER_MS", 500))

    discovered: list[dict[str, Any]] = []
    seen_products: set[str] = set()

    def add_product(item: dict[str, Any]) -> None:
        normalized = normalize_naver_listed_product(item, brand_url=brand_url)
        product_no = normalized.get("product_no") or ""
        if not product_no or product_no in seen_products:
            return
        seen_products.add(product_no)
        discovered.append(normalized)

    # Seed products from any payload already present on the home page.
    for payload in payload_candidates:
        for key in ("simpleProducts", "products", "productList"):
            for items in iter_key_values(payload, key):
                if isinstance(items, list):
                    for item in items:
                        if isinstance(item, dict):
                            add_product(item)

    # HTML anchor / raw JSON fallback when state payload is missing or partial.
    for product_url in extract_naver_product_urls_from_html(home_html, brand_url=brand_url):
        product_no = re.search(r"/products/(\d+)", product_url)
        if not product_no:
            continue
        item = {"productNo": product_no.group(1), "productUrl": product_url}
        add_product(item)

    if not category_ids and not discovered:
        try:
            for product_url in query_naver_product_urls_from_page(brand_url):
                match = re.search(r"/products/(\d+)", product_url)
                if not match:
                    continue
                add_product({"productNo": match.group(1), "productUrl": product_url})
        except Exception as exc:
            log(f"[WARN] Naver Playwright product-url fallback failed: {type(exc).__name__}: {exc}")

    if not category_ids:
        log(f"[WARN] Naver category ids not found; using html/product fallback only. products={len(discovered)}")
        return discovered

    target_category_ids = category_ids[:max_categories]
    log(f"[INFO] Naver category discovery targets={len(target_category_ids)}")

    for idx, category_id in enumerate(target_category_ids, start=1):
        category_total_before = len(discovered)
        for page_no in range(1, max_pages_per_category + 1):
            category_url = build_naver_category_url(brand_url, category_id, page=page_no)
            safe_sleep(delay_ms, jitter_ms)
            try:
                category_html = fetch_naver_html(
                    session,
                    category_url,
                    timeout=timeout,
                    label=f"Naver category page {category_id} p{page_no}",
                    wait_selector='script',
                )
                payloads = gather_naver_payload_candidates(category_html)
                category_payload = payloads[0] if payloads else {}
                category_products = extract_category_products(category_payload)
                simple_products = category_products.get("simpleProducts") if isinstance(category_products.get("simpleProducts"), list) else []

                if not simple_products:
                    for payload in payloads:
                        for items in iter_key_values(payload, "simpleProducts") + iter_key_values(payload, "products"):
                            if isinstance(items, list):
                                for item in items:
                                    if isinstance(item, dict):
                                        simple_products.append(item)
                    if not simple_products:
                        for product_url in extract_naver_product_urls_from_html(category_html, brand_url=brand_url):
                            match = re.search(r"/products/(\d+)", product_url)
                            if not match:
                                continue
                            simple_products.append({"productNo": match.group(1), "productUrl": product_url})

                for item in simple_products:
                    if isinstance(item, dict):
                        add_product(item)

                total_count = int(category_products.get("totalCount") or 0)
                page_size = int(category_products.get("pageSize") or len(simple_products) or 0)
                log(
                    f"[INFO] Naver category {idx}/{len(target_category_ids)} id={category_id} page={page_no} "
                    f"items={len(simple_products)} total={total_count or len(simple_products)} cumulative={len(discovered)}"
                )
                if not simple_products:
                    break
                if total_count and page_size and page_no * page_size >= total_count:
                    break
            except CrawlStop:
                raise
            except Exception as exc:
                log(f"[WARN] Naver category {category_id} page={page_no} failed: {type(exc).__name__}: {exc}")
                break

        added = len(discovered) - category_total_before
        if added <= 0:
            continue

    log(f"[INFO] Naver listed products discovered={len(discovered)}")
    return discovered


def iter_review_product_groups(node: Any) -> list[list[dict[str, Any]]]:
    groups: list[list[dict[str, Any]]] = []
    stack = [node]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            for key, value in cur.items():
                if key == "reviewProducts" and isinstance(value, list):
                    group = [item for item in value if isinstance(item, dict)]
                    if group:
                        groups.append(group)
                elif isinstance(value, (dict, list)):
                    stack.append(value)
        elif isinstance(cur, list):
            for item in cur:
                if isinstance(item, (dict, list)):
                    stack.append(item)
    return groups


def normalize_naver_review(review: dict[str, Any], product: dict[str, Any], brand_url: str) -> dict[str, Any]:
    product_code = str(
        find_first(review, ("productNo",), ("knowledgeShoppingMallProductId",))
        or find_first(product, ("productNo",), ("id",))
        or ""
    ).strip()
    product_name = str(
        find_first(review, ("productName",))
        or find_first(product, ("productName",), ("name",), ("dispName",))
        or product_code
    ).strip()
    product_url = absolute_url(
        brand_url,
        find_first(review, ("productUrl",)) or find_first(product, ("productUrl",), ("url",)) or brand_url,
    )
    rating = normalize_rating(find_first(review, ("reviewScore",), ("rating",), ("score",)) or 0)
    created_at = normalize_created_at(find_first(review, ("createDate",), ("createdAt",), ("reviewDate",)))
    text = normalize_space(find_first(review, ("reviewContent",), ("content",), ("text",)) or "")
    option_size, option_color = parse_option_string(find_first(review, ("productOptionContent",), ("optionContent",)))
    image_url = absolute_url(
        brand_url,
        find_first(product, ("representativeImageUrl",), ("imageUrl",))
        or find_first(review, ("repThumbnailAttach", "attachUrl")),
    )
    review_thumb = absolute_url(
        brand_url,
        find_first(review, ("repThumbnailAttach", "attachUrl"))
        or find_first(review, ("reviewAttaches",))
        or "",
    )
    if isinstance(review.get("reviewAttaches"), list) and review.get("reviewAttaches"):
        first_attach = review["reviewAttaches"][0]
        if isinstance(first_attach, dict):
            review_thumb = absolute_url(brand_url, first_attach.get("attachUrl") or first_attach.get("attachPath"))

    return {
        "id": str(find_first(review, ("id",), ("reviewId",)) or "").strip(),
        "product_code": product_code,
        "product_name": product_name,
        "product_url": product_url,
        "review_original_url": product_url,
        "rating": rating,
        "created_at": created_at,
        "text": text,
        "source": "Naver",
        "option_size": option_size,
        "option_color": option_color,
        "tags": make_tags(text, rating),
        "size_direction": "",
        "local_product_image": image_url,
        "local_review_thumb": review_thumb,
        "text_image_path": "",
    }


def fetch_naver_rows(start_date: date, end_date: date, brand_url: str, timeout: int, existing_naver_rows: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    session = make_session()
    home_html = fetch_naver_html(
        session,
        brand_url,
        timeout=timeout,
        label="Naver brand page",
        wait_selector='script',
    )
    payloads = gather_naver_payload_candidates(home_html)
    review_limit = env_int("NAVER_HOME_REVIEW_LIMIT", 40)
    out: list[dict[str, Any]] = []

    seed_rows = collect_naver_reviews_from_payloads(payloads, brand_url=brand_url)
    for row in seed_rows:
        if row.get("created_at") and in_window(str(row.get("created_at") or ""), start_date, end_date):
            out.append(row)
            if len(out) >= review_limit:
                break

    if payloads:
        log(f"[INFO] Naver home payloads={len(payloads)} seed_reviews_in_window={len(out)}")
    else:
        log("[WARN] Naver payloads not found on brand page; switching to product-detail fallback")

    detail_limit = env_int("NAVER_PRODUCT_DETAIL_LIMIT_BACKFILL", 36) if (end_date - start_date).days > 31 else env_int("NAVER_PRODUCT_DETAIL_LIMIT_DAILY", 18)
    if len(out) < review_limit:
        try:
            listed_products = discover_naver_listed_products(
                brand_url=brand_url,
                timeout=timeout,
                is_backfill=(end_date - start_date).days > 31,
            )
            if not listed_products:
                try:
                    for product_url in query_naver_product_urls_from_page(brand_url):
                        match = re.search(r"/products/(\d+)", product_url)
                        if not match:
                            continue
                        listed_products.append(normalize_naver_listed_product({"productNo": match.group(1), "productUrl": product_url}, brand_url=brand_url))
                except Exception as exc:
                    log(f"[WARN] Naver direct Playwright product discovery failed: {type(exc).__name__}: {exc}")
            success_counts = build_naver_existing_product_success(existing_naver_rows)
            listed_products = sorted(
                listed_products,
                key=lambda item: (
                    1 if success_counts.get(str(item.get("product_no") or ""), 0) > 0 else 0,
                    success_counts.get(str(item.get("product_no") or ""), 0),
                    int(item.get("review_count") or 0),
                    str(item.get("product_no") or ""),
                ),
                reverse=True,
            )
            products_with_reviews = sum(1 for item in listed_products if int(item.get("review_count") or 0) > 0)
            prioritized_success = sum(1 for item in listed_products[:detail_limit] if success_counts.get(str(item.get("product_no") or ""), 0) > 0)
            log(
                f"[INFO] Naver listed products discovered={len(listed_products)} "
                f"products_with_visible_review_count={products_with_reviews} prioritized_known_success={prioritized_success}"
            )

            target_products = listed_products[:detail_limit]
            delay_ms = env_int("NAVER_PRODUCT_DELAY_MS", env_int("MARKETPLACE_REQUEST_DELAY_MS", 1200))
            jitter_ms = env_int("NAVER_PRODUCT_JITTER_MS", env_int("MARKETPLACE_REQUEST_JITTER_MS", 500))
            for idx, product in enumerate(target_products, start=1):
                if len(out) >= max(review_limit, detail_limit * 4):
                    break
                safe_sleep(delay_ms, jitter_ms)
                try:
                    rows, stats = inspect_naver_product_detail(
                        session,
                        product,
                        brand_url=brand_url,
                        start_date=start_date,
                        end_date=end_date,
                        timeout=timeout,
                    )
                    before_count = len(out)
                    if rows:
                        out = merge_rows(out, rows)
                    added_count = max(0, len(out) - before_count)
                    log(
                        f"[INFO] Naver product detail {idx}/{len(target_products)} product_no={product.get('product_no') or '-'} "
                        f"review_count_hint={int(product.get('review_count') or 0)} payloads={int(stats.get('payloads') or 0)} "
                        f"raw_reviews={int(stats.get('raw_reviews') or 0)} normalized={int(stats.get('normalized') or 0)} "
                        f"in_window={int(stats.get('in_window') or 0)} added={added_count} cumulative={len(out)} "
                        f"skip_reason={stats.get('skip_reason') or '-'}"
                    )
                except CrawlStop as exc:
                    log(f"[WARN] {exc}")
                    continue
                except Exception as exc:
                    log(f"[WARN] Naver product detail failed product_no={product.get('product_no') or '-'}: {type(exc).__name__}: {exc}")
                    continue
        except CrawlStop as exc:
            log(f"[WARN] {exc}")
        except Exception as exc:
            log(f"[WARN] Naver product discovery failed: {type(exc).__name__}: {exc}")

    out = merge_rows([], out)
    log(f"[INFO] Naver normalized reviews in window={len(out)}")
    return out


def extract_musinsa_product_ids(html: str) -> list[str]:
    ids = re.findall(r"/products/(\d+)", html)
    unique: list[str] = []
    seen: set[str] = set()
    for goods_no in ids:
        if goods_no in seen:
            continue
        seen.add(goods_no)
        unique.append(goods_no)
    return unique


def extract_next_data_json(html: str) -> dict[str, Any]:
    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html, flags=re.S)
    if not match:
        return {}
    try:
        payload = json.loads(match.group(1))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def extract_musinsa_product_ids_from_next_data(html: str) -> list[str]:
    payload = extract_next_data_json(html)
    text = json.dumps(payload, ensure_ascii=False)
    ids = re.findall(r'"goodsNo"\s*:\s*(\d+)', text)
    return dedupe_keep_order(ids)


def extract_musinsa_product_ids_from_payload(payload: Any) -> list[str]:
    text = json.dumps(payload, ensure_ascii=False)
    ids = re.findall(r'"goodsNo"\s*:\s*(\d+)', text)
    return dedupe_keep_order(ids)


def discover_musinsa_product_ids_via_api(brand_slug: str, timeout: int) -> list[str]:
    session = make_session()
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Referer": f"https://www.musinsa.com/brand/{brand_slug}",
    }
    api_urls = [
        f"https://api.musinsa.com/api2/dp/v3/brand/flagship/{brand_slug}/main?&gf=A",
        f"https://api.musinsa.com/api2/dp/v2/plp/goods?brand={brand_slug}&sortCode=POPULAR&size=30&caller=FLAGSHIP&gf=A",
        f"https://api.musinsa.com/api2/dp/v1/brand/flagship/{brand_slug}/ranking-goods?sortCode=REALTIME&size=30&gf=A",
    ]
    ids: list[str] = []
    for url in api_urls:
        try:
            response = get_with_backoff(
                session,
                url,
                timeout=timeout,
                label=f"Musinsa discovery API {brand_slug}",
                headers=headers,
                max_attempts=env_int("MUSINSA_MAX_ATTEMPTS", 2),
            )
            payload = response.json()
            ids.extend(extract_musinsa_product_ids_from_payload(payload))
        except Exception as exc:
            log(f"[WARN] Musinsa discovery API failed: {type(exc).__name__}: {exc}")
            continue
    return dedupe_keep_order(ids)


def query_musinsa_product_ids_from_page(page: Any) -> list[str]:
    try:
        hrefs = page.eval_on_selector_all(
            "a[href*='/products/']",
            "els => els.map(el => el.getAttribute('href') || '')",
        )
    except Exception:
        hrefs = []
    ids = re.findall(r"/products/(\d+)", "\n".join(str(item) for item in (hrefs or [])))
    return dedupe_keep_order(ids)


def discover_musinsa_product_ids(brand_url: str, timeout: int) -> list[str]:
    brand_slug = brand_url.rstrip("/").split("/")[-1].strip() or "columbia"
    api_ids = discover_musinsa_product_ids_via_api(brand_slug=brand_slug, timeout=timeout)
    if len(api_ids) >= 5:
        log(f"[INFO] Musinsa product discovery via API: {len(api_ids)} ids")
        return api_ids

    session = make_session()
    try:
        response = get_with_backoff(
            session,
            brand_url,
            timeout=timeout,
            label="Musinsa brand page",
            max_attempts=env_int("MUSINSA_MAX_ATTEMPTS", 2),
        )
        if response.ok:
            ids = extract_musinsa_product_ids(response.text)
            if not ids:
                ids = extract_musinsa_product_ids_from_next_data(response.text)
            if len(ids) >= 5:
                log(f"[INFO] Musinsa product discovery via HTTP: {len(ids)} ids")
                return ids
    except Exception as exc:
        log(f"[WARN] Musinsa brand HTTP discovery failed: {type(exc).__name__}: {exc}")

    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        log(f"[WARN] Playwright unavailable for Musinsa discovery: {type(exc).__name__}: {exc}")
        return []

    render_timeout_ms = env_int("MUSINSA_RENDER_TIMEOUT_MS", 45000)
    render_wait_ms = env_int("MUSINSA_RENDER_WAIT_MS", 6500)
    render_selector_timeout_ms = env_int("MUSINSA_RENDER_SELECTOR_TIMEOUT_MS", 15000)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(user_agent=DEFAULT_HEADERS["User-Agent"])
        page.goto(brand_url, wait_until="networkidle", timeout=render_timeout_ms)
        try:
            page.wait_for_selector("a[href*='/products/']", timeout=render_selector_timeout_ms)
        except Exception:
            pass
        try:
            page.mouse.wheel(0, 2400)
        except Exception:
            pass
        page.wait_for_timeout(render_wait_ms)
        html = page.content()
        ids = query_musinsa_product_ids_from_page(page)
        browser.close()

    if not ids:
        ids = extract_musinsa_product_ids(html)
    if not ids:
        ids = extract_musinsa_product_ids_from_next_data(html)
    log(f"[INFO] Musinsa product discovery via Playwright: {len(ids)} ids")
    return ids


def choose_musinsa_product_ids(
    discovered_ids: list[str],
    existing_rows: list[dict[str, Any]],
    max_products: int,
) -> list[str]:
    seen_ids = {
        str(row.get("product_code") or "").strip()
        for row in existing_rows
        if normalize_source(row.get("source")) == "Musinsa"
    }
    fresh = [goods_no for goods_no in discovered_ids if goods_no not in seen_ids]
    revisit = [goods_no for goods_no in discovered_ids if goods_no in seen_ids]
    chosen: list[str] = []
    # Alternate fresh + revisit so daily runs still revisit known hot products with new reviews.
    for bucket in (fresh, revisit, discovered_ids):
        for goods_no in bucket:
            if goods_no in chosen:
                continue
            chosen.append(goods_no)
            if len(chosen) >= max_products:
                return chosen
    return chosen


def normalize_musinsa_review(item: dict[str, Any]) -> dict[str, Any]:
    goods = item.get("goods") if isinstance(item.get("goods"), dict) else {}
    goods_no = str(find_first(goods, ("goodsNo",)) or find_first(item, ("goodsNo",)) or "").strip()
    goods_name = str(find_first(goods, ("goodsName",)) or goods_no).strip()
    product_url = f"https://www.musinsa.com/products/{goods_no}" if goods_no else "https://www.musinsa.com/brand/columbia"
    rating = normalize_rating(find_first(item, ("grade",), ("rating",), ("score",)) or 0)
    created_at = normalize_created_at(find_first(item, ("createDate",), ("createdAt",)))
    text = normalize_space(find_first(item, ("content",), ("reviewContent",)) or "")
    option_size, option_color = parse_option_string(find_first(item, ("goodsOption",), ("optionName",)) or "")
    thumb = ""
    images = item.get("images")
    if isinstance(images, list) and images:
        first = images[0]
        if isinstance(first, dict):
            thumb = musinsa_image_url(
                first.get("imageUrl") or first.get("thumbnailUrl") or first.get("originUrl") or first.get("url")
            )
    product_image = musinsa_image_url(
        item.get("goodsThumbnailImageUrl")
        or find_first(goods, ("goodsThumbnailImageUrl",))
        or find_first(goods, ("goodsImageFile",))
    )
    review_id = find_first(item, ("no",), ("reviewNo",)) or ""
    return {
        "id": str(review_id).strip(),
        "product_code": goods_no,
        "product_name": goods_name,
        "product_url": product_url,
        "review_original_url": f"{product_url}?source=review&reviewId={review_id}",
        "rating": rating,
        "created_at": created_at,
        "text": text,
        "source": "Musinsa",
        "option_size": option_size,
        "option_color": option_color,
        "tags": make_tags(text, rating),
        "size_direction": "",
        "local_product_image": product_image,
        "local_review_thumb": thumb,
        "text_image_path": "",
    }


def fetch_musinsa_page(
    session: requests.Session,
    goods_no: str,
    page: int,
    page_size: int,
    timeout: int,
    *,
    sort: str = "new",
) -> dict[str, Any]:
    url = "https://api.musinsa.com/api2/review/v1/view/list"
    params = {
        "goodsNo": goods_no,
        "page": page,
        "pageSize": page_size,
        "sort": sort or "new",
        "selectedSimilarNo": 0,
    }
    headers = {
        "Referer": f"https://www.musinsa.com/products/{goods_no}",
        "Accept": "application/json, text/plain, */*",
    }
    response = session.get(url, params=params, headers=headers, timeout=timeout)
    if response.status_code in (403, 429):
        raise CrawlStop(f"Musinsa review API returned HTTP {response.status_code} for goodsNo={goods_no}")
    response.raise_for_status()
    payload = response.json()
    return payload if isinstance(payload, dict) else {}


def fetch_musinsa_rows(
    start_date: date,
    end_date: date,
    brand_url: str,
    existing_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    timeout = env_int("MARKETPLACE_TIMEOUT_SEC", 30)
    delay_ms = env_int("MARKETPLACE_REQUEST_DELAY_MS", 1200)
    jitter_ms = env_int("MARKETPLACE_REQUEST_JITTER_MS", 500)
    page_size = env_int("MUSINSA_PAGE_SIZE", 10)
    is_backfill = (end_date - start_date).days > 31 or not any(
        normalize_source(row.get("source")) == "Musinsa" for row in existing_rows
    )
    max_products = env_int("MUSINSA_MAX_PRODUCTS_BACKFILL", 36) if is_backfill else env_int("MUSINSA_MAX_PRODUCTS_DAILY", 16)
    max_pages = env_int("MUSINSA_MAX_PAGES_BACKFILL", 6) if is_backfill else env_int("MUSINSA_MAX_PAGES_DAILY", 3)

    discovered_ids = discover_musinsa_product_ids(brand_url=brand_url, timeout=timeout)
    if not discovered_ids:
        log("[WARN] Musinsa product discovery returned no ids -> skip")
        return []

    target_ids = choose_musinsa_product_ids(discovered_ids, existing_rows=existing_rows, max_products=max_products)
    log(f"[INFO] Musinsa crawl target products={len(target_ids)} max_pages_per_product={max_pages}")

    session = make_session()
    collected: list[dict[str, Any]] = []
    fail_count = 0
    for idx, goods_no in enumerate(target_ids, start=1):
        log(f"[INFO] Musinsa reviews goodsNo={goods_no} ({idx}/{len(target_ids)})")
        safe_sleep(delay_ms, jitter_ms)
        try:
            for sort_name in ("new", "up", "photo"):
                for page in range(1, max_pages + 1):
                    payload = fetch_musinsa_page(session, goods_no=goods_no, page=page, page_size=page_size, timeout=timeout, sort=sort_name)
                    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
                    items = data.get("list") if isinstance(data.get("list"), list) else []
                    if not items:
                        break

                    page_rows: list[dict[str, Any]] = []
                    page_dates: list[date] = []
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        row = normalize_musinsa_review(item)
                        created = parse_date_from_created_at(row["created_at"])
                        if created is not None:
                            page_dates.append(created)
                        if row["created_at"] and in_window(row["created_at"], start_date, end_date):
                            page_rows.append(row)
                    if page_rows:
                        collected.extend(page_rows)

                    total_pages = 0
                    page_meta = data.get("page") if isinstance(data.get("page"), dict) else {}
                    if isinstance(page_meta.get("totalPages"), int):
                        total_pages = int(page_meta["totalPages"])

                    log(
                        f"[INFO] Musinsa goodsNo={goods_no} sort={sort_name} page={page} raw_items={len(items)} in_window={len(page_rows)} cumulative={len(collected)}"
                    )

                    if page_dates and min(page_dates) < start_date and sort_name == "new":
                        break
                    if total_pages and page >= total_pages:
                        break

                    safe_sleep(delay_ms, jitter_ms)
        except CrawlStop as exc:
            log(f"[WARN] {exc}")
            break
        except Exception as exc:
            fail_count += 1
            log(f"[WARN] Musinsa goodsNo={goods_no} failed: {type(exc).__name__}: {exc}")
            if fail_count >= 3:
                log("[WARN] Musinsa repeated failures -> stop current run")
                break
            continue

    collected = merge_rows([], collected)
    log(f"[INFO] Musinsa normalized reviews in window={len(collected)}")
    return collected


def main() -> int:
    parser = argparse.ArgumentParser(description="Merge marketplace reviews into VOC dashboard input.")
    parser.add_argument("--base-input", required=True, help="Existing review JSON from Crema or site/data.")
    parser.add_argument(
        "--existing-naver-input",
        default="",
        help="Existing cumulative marketplace review JSON. Legacy argument name kept for workflow compatibility.",
    )
    parser.add_argument("--output", required=True, help="Merged review JSON output path.")
    parser.add_argument("--naver-output", required=True, help="Marketplace-only review JSON output path.")
    parser.add_argument("--start", required=True, help="Fetch start date (YYYY-MM-DD).")
    parser.add_argument("--end", required=True, help="Fetch end date (YYYY-MM-DD).")
    parser.add_argument("--brand-channel-no", default="", help="Unused legacy argument kept for compatibility.")
    parser.add_argument(
        "--product-url",
        default="https://brand.naver.com/columbia",
        help="Naver brand/store URL used as the Naver crawl entrypoint.",
    )
    parser.add_argument(
        "--musinsa-brand-url",
        default="https://www.musinsa.com/brand/columbia",
        help="Musinsa brand URL used for conservative product discovery.",
    )
    args = parser.parse_args()

    start_date = parse_date_only(args.start)
    end_date = parse_date_only(args.end)

    base_path = Path(args.base_input)
    existing_marketplace_path = Path(args.existing_naver_input).expanduser() if str(args.existing_naver_input).strip() else None
    output_path = Path(args.output)
    marketplace_output_path = Path(args.naver_output)

    base_rows = load_rows(base_path)
    existing_marketplace_rows = [
        row for row in load_rows(existing_marketplace_path) if normalize_source(row.get("source")) in {"Naver", "Musinsa"}
    ]
    log(f"[INFO] Base review rows={len(base_rows)} from {base_path}")
    if existing_marketplace_path:
        log(f"[INFO] Existing marketplace rows={len(existing_marketplace_rows)} from {existing_marketplace_path}")

    enabled = env_flag("MARKETPLACE_REVIEWS_ENABLED", default=True)
    fetched_rows: list[dict[str, Any]] = []
    naver_rows: list[dict[str, Any]] = []
    musinsa_rows: list[dict[str, Any]] = []

    if enabled:
        timeout = env_int("MARKETPLACE_TIMEOUT_SEC", 30)
        try:
            naver_existing_rows = [row for row in existing_marketplace_rows if normalize_source(row.get("source")) == "Naver"]
            naver_rows = fetch_naver_rows(
                start_date=start_date,
                end_date=end_date,
                brand_url=str(args.product_url or os.getenv("NAVER_BRAND_URL", "https://brand.naver.com/columbia")).strip(),
                timeout=timeout,
                existing_naver_rows=naver_existing_rows,
            )
        except Exception as exc:
            log(f"[WARN] Naver crawl failed -> continue without Naver rows: {type(exc).__name__}: {exc}")
            naver_rows = []

        try:
            musinsa_rows = fetch_musinsa_rows(
                start_date=start_date,
                end_date=end_date,
                brand_url=str(args.musinsa_brand_url or os.getenv("MUSINSA_BRAND_URL", "https://www.musinsa.com/brand/columbia")).strip(),
                existing_rows=existing_marketplace_rows,
            )
        except Exception as exc:
            log(f"[WARN] Musinsa crawl failed -> continue without Musinsa rows: {type(exc).__name__}: {exc}")
            musinsa_rows = []

        fetched_rows = merge_rows(naver_rows, musinsa_rows)
    else:
        log("[INFO] MARKETPLACE_REVIEWS_ENABLED=false -> pass-through mode")

    cumulative_marketplace_rows = merge_rows(existing_marketplace_rows, fetched_rows)
    merged = merge_rows(base_rows, cumulative_marketplace_rows)

    dump_reviews(output_path, merged)
    dump_reviews(marketplace_output_path, cumulative_marketplace_rows)

    source_counts: dict[str, int] = {}
    for row in merged:
        source = normalize_source(row.get("source"))
        source_counts[source] = source_counts.get(source, 0) + 1

    fetched_source_counts: dict[str, int] = {}
    for row in fetched_rows:
        source = normalize_source(row.get("source"))
        fetched_source_counts[source] = fetched_source_counts.get(source, 0) + 1

    log(f"[CHECK] merged_review_count={len(merged)}")
    log(f"[CHECK] marketplace_fetched_review_count={len(fetched_rows)}")
    log(f"[CHECK] naver_fetched_review_count={len(naver_rows)}")
    log(f"[CHECK] musinsa_fetched_review_count={len(musinsa_rows)}")
    log(f"[CHECK] marketplace_cumulative_review_count={len(cumulative_marketplace_rows)}")
    log(f"[CHECK] source_counts={json.dumps(source_counts, ensure_ascii=False, sort_keys=True)}")
    log(f"[CHECK] fetched_source_counts={json.dumps(fetched_source_counts, ensure_ascii=False, sort_keys=True)}")
    log(f"[CHECK] merged_output={output_path}")
    log(f"[CHECK] marketplace_output={marketplace_output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
