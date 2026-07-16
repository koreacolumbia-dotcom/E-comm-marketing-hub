#!/usr/bin/env python3
"""Sanitize generated dashboard data and block secrets/PII from public artifacts."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from pathlib import Path
from typing import Any

ROOTS = [Path("reports"), Path("site")]
TEXT_SUFFIXES = {".html", ".js", ".json", ".csv", ".txt"}
EMAIL_RE = re.compile(r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b")
PHONE_RE = re.compile(r"(?<!\d)(?:01[016789])[- .]?\d{3,4}[- .]?\d{4}(?!\d)")
RRN_RE = re.compile(r"(?<!\d)\d{6}[- ]?[1-4]\d{6}(?!\d)")
PUBLIC_EMAIL_RE = re.compile(r"(?i)^[A-Z0-9._%+-]+@(columbia\.com|columbiakorea\.co\.kr)$")
SECRET_PATTERNS = {
    "private_key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "google_api_key": re.compile(r"AIza[0-9A-Za-z_-]{30,}"),
    "github_token": re.compile(r"gh[pousr]_[A-Za-z0-9_]{30,}"),
    "aws_access_key": re.compile(r"AKIA[0-9A-Z]{16}"),
    "service_account": re.compile(r'"type"\s*:\s*"service_account"'),
    "generic_secret": re.compile(r"(?i)(?:api[_-]?key|client[_-]?secret|access[_-]?token|password)\s*[:=]\s*['\"][^'\"]{12,}['\"]"),
}
SENSITIVE_KEYS = {
    "email", "email_address", "e_mail", "phone", "phone_number", "mobile", "mobile_no",
    "tel", "telephone", "customer_name", "member_name", "recipient_name", "shipping_name",
    "address", "shipping_address", "customer_id", "member_id", "user_id", "ci", "di", "rrn",
    "resident_number", "birth_date", "birthday",
}


def token(value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]
    return f"masked_{digest}"


def mask_text(text: str) -> tuple[str, int]:
    count = 0
    def email_repl(match: re.Match[str]) -> str:
        nonlocal count
        value = match.group(0)
        if PUBLIC_EMAIL_RE.match(value):
            return value
        count += 1
        return token(value)
    def pii_repl(match: re.Match[str]) -> str:
        nonlocal count
        count += 1
        return token(match.group(0))
    text = EMAIL_RE.sub(email_repl, text)
    text = PHONE_RE.sub(pii_repl, text)
    text = RRN_RE.sub(pii_repl, text)
    return text, count


def sanitize_obj(obj: Any) -> tuple[Any, int]:
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        count = 0
        for k, v in obj.items():
            lower = str(k).lower()
            if lower in SENSITIVE_KEYS and v not in (None, ""):
                if lower in {"email", "email_address", "e_mail"} and PUBLIC_EMAIL_RE.match(str(v)):
                    out[k] = v
                else:
                    out[k] = token(str(v)); count += 1
            else:
                out[k], n = sanitize_obj(v); count += n
        return out, count
    if isinstance(obj, list):
        result, count = [], 0
        for v in obj:
            clean, n = sanitize_obj(v); result.append(clean); count += n
        return result, count
    if isinstance(obj, str):
        return mask_text(obj)
    return obj, 0


def scan_secrets(text: str) -> list[str]:
    return [name for name, pattern in SECRET_PATTERNS.items() if pattern.search(text)]


def sanitize_json(path: Path) -> int:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return 0
    clean, count = sanitize_obj(data)
    if count:
        path.write_text(json.dumps(clean, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return count


def sanitize_csv(path: Path) -> int:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            rows = list(csv.DictReader(f)); fields = list(rows[0].keys()) if rows else []
    except Exception:
        return 0
    count = 0
    for row in rows:
        for field in fields:
            value = row.get(field, "")
            if field.lower() in SENSITIVE_KEYS and value:
                if field.lower() in {"email", "email_address", "e_mail"} and PUBLIC_EMAIL_RE.match(value):
                    continue
                row[field] = token(value); count += 1
            elif value:
                row[field], n = mask_text(value); count += n
    if count:
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields); writer.writeheader(); writer.writerows(rows)
    return count


def remaining_pii(text: str) -> int:
    emails = [x for x in EMAIL_RE.findall(text) if not PUBLIC_EMAIL_RE.match(x)]
    return len(emails) + len(PHONE_RE.findall(text)) + len(RRN_RE.findall(text))


def main() -> int:
    parser = argparse.ArgumentParser(); parser.add_argument("--sanitize", action="store_true"); args = parser.parse_args()
    findings: list[str] = []; masked = 0; scanned = 0
    for root in ROOTS:
        if not root.exists(): continue
        for path in root.rglob("*"):
            if not path.is_file() or path.suffix.lower() not in TEXT_SUFFIXES or path.stat().st_size > 25_000_000: continue
            scanned += 1
            if args.sanitize and path.suffix.lower() == ".json": masked += sanitize_json(path)
            elif args.sanitize and path.suffix.lower() == ".csv": masked += sanitize_csv(path)
            try: text = path.read_text(encoding="utf-8", errors="ignore")
            except Exception: continue
            hits = scan_secrets(text)
            if hits: findings.append(f"{path}: {', '.join(hits)}")
            if path.suffix.lower() in {".html", ".js", ".txt"}:
                pii = remaining_pii(text)
                if pii: findings.append(f"{path}: unmasked PII-like strings={pii}")
    print(f"[SECURITY] scanned={scanned} masked={masked}")
    if findings:
        print("[SECURITY] blocked findings:")
        for hit in findings[:100]: print(f" - {hit}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
