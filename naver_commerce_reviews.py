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
from urllib.parse import urljoin

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
                page.mouse.wheel(0, 800)
            except Exception:
                pass
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
    match = re.search(r"window\.__PRELOADED_STATE__\s*=\s*(\{.*?\})\s*</script>", html, flags=re.S)
    if not match:
        return {}
    raw = sanitize_js_object(match.group(1))
    try:
        payload = json.loads(raw)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


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


def build_naver_category_url(brand_url: str, category_id: str) -> str:
    base = brand_url.rstrip("/")
    return f"{base}/category/{category_id}?cp=1"


def discover_naver_listed_products(brand_url: str, timeout: int, is_backfill: bool) -> list[dict[str, Any]]:
    session = make_session()
    home_html = fetch_naver_html(
        session,
        brand_url,
        timeout=timeout,
        label="Naver brand page",
        wait_selector='script',
    )
    home_payload = extract_preloaded_state(home_html)
    category_ids = extract_naver_category_ids(home_payload, home_html)
    if not category_ids:
        log("[WARN] Naver category ids not found")
        return []

    max_categories = (
        env_int("NAVER_MAX_CATEGORIES_BACKFILL", 120)
        if is_backfill
        else env_int("NAVER_MAX_CATEGORIES_DAILY", 36)
    )
    delay_ms = env_int("NAVER_CATEGORY_DELAY_MS", env_int("MARKETPLACE_REQUEST_DELAY_MS", 1200))
    jitter_ms = env_int("NAVER_CATEGORY_JITTER_MS", env_int("MARKETPLACE_REQUEST_JITTER_MS", 500))

    discovered: list[dict[str, Any]] = []
    seen_products: set[str] = set()
    truncated_categories = 0
    target_category_ids = category_ids[:max_categories]
    log(f"[INFO] Naver category discovery targets={len(target_category_ids)}")

    for idx, category_id in enumerate(target_category_ids, start=1):
        category_url = build_naver_category_url(brand_url, category_id)
        safe_sleep(delay_ms, jitter_ms)
        try:
            category_html = fetch_naver_html(
                session,
                category_url,
                timeout=timeout,
                label=f"Naver category page {category_id}",
                wait_selector='script',
            )
            category_payload = extract_preloaded_state(category_html)
            category_products = extract_category_products(category_payload)
            simple_products = category_products.get("simpleProducts") if isinstance(category_products.get("simpleProducts"), list) else []
            total_count = int(category_products.get("totalCount") or 0)
            page_size = int(category_products.get("pageSize") or len(simple_products) or 0)
            if total_count and page_size and total_count > page_size:
                truncated_categories += 1
            for item in simple_products:
                if not isinstance(item, dict):
                    continue
                normalized = normalize_naver_listed_product(item, brand_url=brand_url)
                product_no = normalized.get("product_no") or ""
                if not product_no or product_no in seen_products:
                    continue
                seen_products.add(product_no)
                discovered.append(normalized)
            log(
                f"[INFO] Naver category {idx}/{len(target_category_ids)} id={category_id} "
                f"items={len(simple_products)} total={total_count or len(simple_products)} cumulative={len(discovered)}"
            )
        except CrawlStop:
            raise
        except Exception as exc:
            log(f"[WARN] Naver category {category_id} failed: {type(exc).__name__}: {exc}")
            continue

    if truncated_categories:
        log(
            f"[WARN] Naver categories with more than one page detected={truncated_categories}; "
            "current crawler stays on first page of each category to remain conservative."
        )
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


def fetch_naver_rows(start_date: date, end_date: date, brand_url: str, timeout: int) -> list[dict[str, Any]]:
    session = make_session()
    home_html = fetch_naver_html(
        session,
        brand_url,
        timeout=timeout,
        label="Naver brand page",
        wait_selector='script',
    )
    payload = extract_preloaded_state(home_html)
    if not payload:
        log("[WARN] Naver preloaded state not found after browser fallback -> skip")
        return []

    review_limit = env_int("NAVER_HOME_REVIEW_LIMIT", 40)
    out: list[dict[str, Any]] = []
    for group in iter_review_product_groups(payload):
        for product in group:
            reviews = product.get("reviews") or []
            if not isinstance(reviews, list):
                continue
            for review in reviews:
                if not isinstance(review, dict):
                    continue
                row = normalize_naver_review(review, product, brand_url=brand_url)
                if not row["id"] and not row["text"]:
                    continue
                if row["created_at"] and in_window(row["created_at"], start_date, end_date):
                    out.append(row)
                if len(out) >= review_limit:
                    break
            if len(out) >= review_limit:
                break
        if len(out) >= review_limit:
            break

    out = merge_rows([], out)
    try:
        listed_products = discover_naver_listed_products(
            brand_url=brand_url,
            timeout=timeout,
            is_backfill=(end_date - start_date).days > 31,
        )
        products_with_reviews = sum(1 for item in listed_products if int(item.get("review_count") or 0) > 0)
        log(
            f"[INFO] Naver listed products discovered={len(listed_products)} "
            f"products_with_visible_review_count={products_with_reviews}"
        )
    except CrawlStop as exc:
        log(f"[WARN] {exc}")
    except Exception as exc:
        log(f"[WARN] Naver product discovery failed: {type(exc).__name__}: {exc}")
    log(f"[INFO] Naver brand-home reviews in window={len(out)}")
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
    chosen = fresh[:max_products]
    if len(chosen) < max_products:
        for goods_no in discovered_ids:
            if goods_no in chosen:
                continue
            chosen.append(goods_no)
            if len(chosen) >= max_products:
                break
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
) -> dict[str, Any]:
    url = "https://api.musinsa.com/api2/review/v1/view/list"
    params = {
        "goodsNo": goods_no,
        "page": page,
        "pageSize": page_size,
        "sort": "new",
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
    max_products = env_int("MUSINSA_MAX_PRODUCTS_BACKFILL", 24) if is_backfill else env_int("MUSINSA_MAX_PRODUCTS_DAILY", 8)
    max_pages = env_int("MUSINSA_MAX_PAGES_BACKFILL", 4) if is_backfill else env_int("MUSINSA_MAX_PAGES_DAILY", 2)

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
            for page in range(max_pages):
                payload = fetch_musinsa_page(session, goods_no=goods_no, page=page, page_size=page_size, timeout=timeout)
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
                collected.extend(page_rows)

                total_pages = 0
                page_meta = data.get("page") if isinstance(data.get("page"), dict) else {}
                if isinstance(page_meta.get("totalPages"), int):
                    total_pages = int(page_meta["totalPages"])

                if page_dates and min(page_dates) < start_date:
                    break
                if total_pages and page + 1 >= total_pages:
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
    log(f"[INFO] Musinsa normalized reviews in window={len
