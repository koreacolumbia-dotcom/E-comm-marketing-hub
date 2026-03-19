from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests


TRUE_VALUES = {"1", "true", "yes", "y", "on"}


def env_flag(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "")).strip().lower()
    if not raw:
        return default
    return raw in TRUE_VALUES


def log(msg: str) -> None:
    print(msg, flush=True)


def load_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
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
    payload = {"reviews": rows}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def normalize_space(value: Any) -> str:
    return " ".join(str(value or "").split())


def unique_key(row: dict[str, Any]) -> tuple[str, ...]:
    source = str(row.get("source") or "").strip() or "Unknown"
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


def deep_get(payload: Any, path: tuple[str, ...]) -> Any:
    cur = payload
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def find_first(payload: dict[str, Any], *paths: tuple[str, ...]) -> Any:
    for path in paths:
        value = deep_get(payload, path)
        if value not in (None, "", [], {}):
            return value
    return None


def parse_access_token(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    candidates = [
        ("access_token",),
        ("accessToken",),
        ("data", "access_token"),
        ("data", "accessToken"),
        ("result", "access_token"),
        ("result", "accessToken"),
    ]
    for path in candidates:
        value = deep_get(payload, path)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def add_query_params(url: str, params: dict[str, Any]) -> str:
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    for key, value in params.items():
        if value in (None, ""):
            continue
        query[str(key)] = str(value)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def extract_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("reviews", "items", "content", "list", "results"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    for key in ("data", "result", "payload"):
        nested = payload.get(key)
        items = extract_items(nested)
        if items:
            return items
    return []


def infer_has_more(payload: Any, page_size: int, item_count: int) -> bool:
    if isinstance(payload, dict):
        for key in ("hasNext", "hasMore", "more"):
            value = payload.get(key)
            if isinstance(value, bool):
                return value
        next_page = payload.get("nextPage") or payload.get("next")
        if next_page not in (None, "", False):
            return True
    return item_count >= page_size


def request_access_token(
    session: requests.Session,
    oauth_url: str,
    client_id: str,
    client_secret: str,
    timeout: int,
) -> str:
    attempts: list[dict[str, Any]] = [
        {
            "data": {
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            }
        },
        {
            "auth": (client_id, client_secret),
            "data": {"grant_type": "client_credentials"},
        },
        {
            "json": {
                "grantType": "client_credentials",
                "clientId": client_id,
                "clientSecret": client_secret,
            }
        },
    ]
    last_error = ""
    for idx, kwargs in enumerate(attempts, start=1):
        try:
            response = session.post(oauth_url, timeout=timeout, **kwargs)
            body = response.text[:500]
            response.raise_for_status()
            payload = response.json()
            token = parse_access_token(payload)
            if token:
                log(f"[INFO] Naver Commerce token acquired via attempt {idx}")
                return token
            last_error = f"token field not found in response: {body}"
        except Exception as exc:  # pragma: no cover - network/runtime branch
            last_error = f"{type(exc).__name__}: {exc}"
    raise RuntimeError(f"Failed to acquire Naver Commerce token: {last_error}")


def normalize_rating(value: Any) -> int:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return 0


def normalize_created_at(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if " " in text and "T" not in text:
        text = text.replace(" ", "T", 1)
    if text.endswith("Z"):
        return text
    if "+" in text or text.count("-") > 2:
        return text
    if "T" in text and len(text) <= 19:
        return f"{text}+09:00"
    if len(text) == 10:
        return f"{text}T00:00:00+09:00"
    return text


def normalize_options(raw: dict[str, Any]) -> tuple[str, str]:
    option_size = ""
    option_color = ""
    simple_size = find_first(
        raw,
        ("optionSize",),
        ("size",),
        ("option", "size"),
    )
    simple_color = find_first(
        raw,
        ("optionColor",),
        ("color",),
        ("option", "color"),
    )
    if isinstance(simple_size, str):
        option_size = simple_size.strip()
    if isinstance(simple_color, str):
        option_color = simple_color.strip()

    if option_size or option_color:
        return option_size, option_color

    purchased = raw.get("purchasedOptions") or raw.get("options") or []
    if isinstance(purchased, list):
        parts = [normalize_space(v.get("name") or v.get("value") or "") for v in purchased if isinstance(v, dict)]
        if parts:
            option_size = parts[0]
            if len(parts) > 1:
                option_color = parts[1]
    elif isinstance(purchased, dict):
        option_size = normalize_space(purchased.get("size") or purchased.get("name") or "")
        option_color = normalize_space(purchased.get("color") or purchased.get("value") or "")
    elif isinstance(purchased, str):
        option_size = normalize_space(purchased)
    return option_size, option_color


def normalize_review(raw: dict[str, Any], fallback_product_url: str) -> dict[str, Any]:
    review_id = find_first(raw, ("reviewId",), ("id",), ("reviewNo",), ("claimId",))
    product_code = find_first(
        raw,
        ("channelProductNo",),
        ("productNo",),
        ("originProductNo",),
        ("productId",),
        ("product", "channelProductNo"),
    )
    product_name = find_first(
        raw,
        ("productName",),
        ("originProductName",),
        ("channelProductName",),
        ("product", "name"),
    )
    product_url = find_first(raw, ("productUrl",), ("product", "url")) or fallback_product_url
    rating = normalize_rating(find_first(raw, ("rating",), ("score",), ("reviewScore",)) or 0)
    created_at = normalize_created_at(
        find_first(
            raw,
            ("createdAt",),
            ("registeredAt",),
            ("reviewDate",),
            ("createDate",),
            ("writtenAt",),
        )
    )
    text = normalize_space(
        find_first(
            raw,
            ("content",),
            ("reviewContent",),
            ("reviewText",),
            ("text",),
            ("body",),
            ("review", "content"),
        )
        or ""
    )
    option_size, option_color = normalize_options(raw)
    tags: list[str] = []
    if rating >= 4:
        tags.append("pos")
    if rating and rating <= 2:
        tags.extend(["neg", "low"])

    return {
        "id": str(review_id or "").strip(),
        "product_code": str(product_code or "").strip(),
        "product_name": str(product_name or "").strip(),
        "product_url": str(product_url or "").strip(),
        "rating": rating,
        "created_at": created_at,
        "text": text,
        "source": "Naver",
        "option_size": option_size,
        "option_color": option_color,
        "tags": tags,
        "size_direction": "",
        "local_product_image": "",
        "local_review_thumb": "",
        "text_image_path": "",
    }


def fetch_naver_reviews(
    start_date: str,
    end_date: str,
    brand_channel_no: str,
    product_url: str,
) -> list[dict[str, Any]]:
    client_id = str(os.getenv("NAVER_COMMERCE_CLIENT_ID", "")).strip()
    client_secret = str(os.getenv("NAVER_COMMERCE_CLIENT_SECRET", "")).strip()
    endpoint = str(os.getenv("NAVER_COMMERCE_REVIEW_ENDPOINT", "")).strip()
    oauth_url = str(
        os.getenv(
            "NAVER_COMMERCE_OAUTH_URL",
            "https://api.commerce.naver.com/external/v1/oauth2/token",
        )
    ).strip()
    timeout = int(str(os.getenv("NAVER_COMMERCE_TIMEOUT_SEC", "30")).strip() or "30")
    page_param = str(os.getenv("NAVER_COMMERCE_PAGE_PARAM", "page")).strip()
    size_param = str(os.getenv("NAVER_COMMERCE_SIZE_PARAM", "size")).strip()
    start_param = str(os.getenv("NAVER_COMMERCE_START_PARAM", "startDate")).strip()
    end_param = str(os.getenv("NAVER_COMMERCE_END_PARAM", "endDate")).strip()
    brand_param = str(os.getenv("NAVER_COMMERCE_BRAND_PARAM", "brandChannelNo")).strip()
    page_size = int(str(os.getenv("NAVER_COMMERCE_PAGE_SIZE", "100")).strip() or "100")
    max_pages = int(str(os.getenv("NAVER_COMMERCE_MAX_PAGES", "20")).strip() or "20")
    page_start = int(str(os.getenv("NAVER_COMMERCE_PAGE_START", "1")).strip() or "1")

    if not client_id or not client_secret:
        log("[WARN] NAVER_COMMERCE_CLIENT_ID/SECRET missing -> pass-through mode")
        return []
    if not endpoint:
        log("[WARN] NAVER_COMMERCE_REVIEW_ENDPOINT missing -> pass-through mode")
        return []

    session = requests.Session()
    token = request_access_token(session, oauth_url, client_id, client_secret, timeout)
    session.headers.update({"Authorization": f"Bearer {token}", "Accept": "application/json"})

    all_items: list[dict[str, Any]] = []
    page = page_start
    for _ in range(max_pages):
        params = {
            page_param: page,
            size_param: page_size,
            start_param: start_date,
            end_param: end_date,
        }
        if brand_channel_no:
            params[brand_param] = brand_channel_no
        url = add_query_params(endpoint, params)
        log(f"[INFO] Fetch Naver Commerce reviews page={page}")
        response = session.get(url, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
        items = extract_items(payload)
        if not items:
            break
        all_items.extend(items)
        if not infer_has_more(payload, page_size=page_size, item_count=len(items)):
            break
        page += 1

    normalized = []
    for item in all_items:
        row = normalize_review(item, fallback_product_url=product_url)
        if row["text"] or row["product_name"] or row["id"]:
            normalized.append(row)
    log(f"[INFO] Naver Commerce normalized reviews={len(normalized)}")
    return normalized


def main() -> int:
    parser = argparse.ArgumentParser(description="Merge Naver Commerce reviews into VOC dashboard input.")
    parser.add_argument("--base-input", required=True, help="Existing review JSON from Crema or site/data.")
    parser.add_argument(
        "--existing-naver-input",
        default="",
        help="Existing cumulative Naver-only review JSON. When present, newly fetched rows are merged into it.",
    )
    parser.add_argument("--output", required=True, help="Merged review JSON output path.")
    parser.add_argument("--naver-output", required=True, help="Normalized Naver-only review JSON output path.")
    parser.add_argument("--start", required=True, help="Review fetch start date (YYYY-MM-DD).")
    parser.add_argument("--end", required=True, help="Review fetch end date (YYYY-MM-DD).")
    parser.add_argument("--brand-channel-no", default="", help="Optional brand channel number for the Commerce API.")
    parser.add_argument(
        "--product-url",
        default="https://brand.naver.com/columbia",
        help="Fallback product/store URL used when the API payload does not include a product URL.",
    )
    args = parser.parse_args()

    base_path = Path(args.base_input)
    existing_naver_path = Path(args.existing_naver_input).expanduser() if str(args.existing_naver_input).strip() else None
    output_path = Path(args.output)
    naver_output_path = Path(args.naver_output)

    base_rows = load_rows(base_path)
    existing_naver_rows = load_rows(existing_naver_path) if existing_naver_path else []
    log(f"[INFO] Base review rows={len(base_rows)} from {base_path}")
    if existing_naver_path:
        log(f"[INFO] Existing Naver rows={len(existing_naver_rows)} from {existing_naver_path}")

    enabled = env_flag("NAVER_COMMERCE_REVIEWS_ENABLED", default=False)
    fetched_naver_rows: list[dict[str, Any]] = []
    if enabled:
        try:
            fetched_naver_rows = fetch_naver_reviews(
                start_date=args.start,
                end_date=args.end,
                brand_channel_no=str(args.brand_channel_no or os.getenv("NAVER_COMMERCE_BRAND_CHANNEL_NO", "")).strip(),
                product_url=args.product_url,
            )
        except Exception as exc:  # pragma: no cover - runtime/network branch
            log(f"[WARN] Naver Commerce fetch failed -> keeping base reviews only: {type(exc).__name__}: {exc}")
            fetched_naver_rows = []
    else:
        log("[INFO] NAVER_COMMERCE_REVIEWS_ENABLED=false -> pass-through mode")

    cumulative_naver_rows = merge_rows(existing_naver_rows, fetched_naver_rows)
    merged = merge_rows(base_rows, cumulative_naver_rows)
    dump_reviews(output_path, merged)
    dump_reviews(naver_output_path, cumulative_naver_rows)

    source_counts: dict[str, int] = {}
    for row in merged:
        src = str(row.get("source") or "Unknown").strip() or "Unknown"
        source_counts[src] = source_counts.get(src, 0) + 1

    log(f"[CHECK] merged_review_count={len(merged)}")
    log(f"[CHECK] naver_fetched_review_count={len(fetched_naver_rows)}")
    log(f"[CHECK] naver_cumulative_review_count={len(cumulative_naver_rows)}")
    log(f"[CHECK] source_counts={json.dumps(source_counts, ensure_ascii=False, sort_keys=True)}")
    log(f"[CHECK] merged_output={output_path}")
    log(f"[CHECK] naver_output={naver_output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
