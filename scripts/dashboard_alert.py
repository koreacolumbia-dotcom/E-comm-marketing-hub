#!/usr/bin/env python3
"""Create a concise dashboard alert and optionally email it through SMTP secrets."""
from __future__ import annotations

import json
import os
import smtplib
import ssl
from email.message import EmailMessage
from pathlib import Path

META = Path("reports/meta.json")
OUT = Path("reports/dashboard_alert.md")


def main() -> int:
    data = json.loads(META.read_text(encoding="utf-8"))
    overall = data.get("overall_status", "partial")
    reports = data.get("reports", {})
    bad = []
    for key, item in reports.items():
        if isinstance(item, dict) and item.get("status") != "fresh":
            bad.append((key, item))

    lines = [f"# Dashboard health: {overall}", "", f"Build: {data.get('build', '-')}", ""]
    if bad:
        lines += ["## Attention required", ""]
        for key, item in bad:
            lines.append(f"- **{key}**: {item.get('status')} · data_end={item.get('data_end')} · age={item.get('age_days')} days · {item.get('note', '')}")
    else:
        lines.append("All monitored reports are fresh.")
    body = "\n".join(lines) + "\n"
    OUT.write_text(body, encoding="utf-8")
    print(body)

    host = os.getenv("DASHBOARD_SMTP_HOST", "").strip()
    to_addr = os.getenv("DASHBOARD_ALERT_EMAIL", "").strip()
    username = os.getenv("DASHBOARD_SMTP_USER", "").strip()
    password = os.getenv("DASHBOARD_SMTP_PASSWORD", "")
    if host and to_addr and username and password and bad:
        port = int(os.getenv("DASHBOARD_SMTP_PORT", "465"))
        msg = EmailMessage()
        msg["Subject"] = f"[CSK Dashboard] {overall.upper()} — {len(bad)} report(s) need attention"
        msg["From"] = username
        msg["To"] = to_addr
        msg.set_content(body)
        with smtplib.SMTP_SSL(host, port, context=ssl.create_default_context()) as smtp:
            smtp.login(username, password)
            smtp.send_message(msg)
        print(f"[ALERT] email sent to {to_addr}")
    else:
        print("[ALERT] SMTP not configured or no unhealthy report; email skipped")
    return 2 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
