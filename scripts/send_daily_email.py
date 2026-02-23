#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import smtplib
from email.mime.text import MIMEText
from email.utils import formatdate


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def parse_recipients(s: str) -> list[str]:
    # supports comma / semicolon / whitespace
    if not s:
        return []
    parts = re.split(r"[,\s;]+", s.strip())
    out = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        out.append(p)
    # de-dupe while preserving order
    seen = set()
    dedup = []
    for x in out:
        if x.lower() in seen:
            continue
        seen.add(x.lower())
        dedup.append(x)
    return dedup


def send_email(host: str, port: int, user: str, password: str, to_list: list[str], subject: str, html_body: str):
    msg = MIMEText(html_body, "html", "utf-8")
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = ", ".join(to_list)
    msg["Date"] = formatdate(localtime=True)

    with smtplib.SMTP(host, port, timeout=30) as s:
        s.ehlo()
        s.starttls()
        s.ehlo()
        s.login(user, password)
        s.sendmail(user, to_list, msg.as_string())


def main():
    smtp_host = env("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(env("SMTP_PORT", "587") or "587")
    smtp_user = env("SMTP_USER")
    smtp_pass = env("SMTP_PASS")

    recipients_raw = env("DAILY_RECIPIENTS")
    recipients = parse_recipients(recipients_raw)

    if not recipients:
        raise RuntimeError("Missing DAILY_RECIPIENTS env (vars.DAILY_RECIPIENTS)")

    if not smtp_user:
        raise RuntimeError("Missing SMTP_USER env (secrets.SMTP_USER)")
    if not smtp_pass:
        raise RuntimeError("Missing SMTP_PASS env (secrets.SMTP_PASS)")

    kst_stamp = env("KST_STAMP", "")
    hub_url = env("HUB_URL", "")

    mode_daily_only = env("MODE_DAILY_ONLY", "false").lower() == "true"
    mode_voc_only = env("MODE_VOC_ONLY", "false").lower() == "true"
    mode_blog_only = env("MODE_BLOG_ONLY", "false").lower() == "true"

    if mode_daily_only:
        mode_tag = "DAILY ONLY"
    elif mode_voc_only:
        mode_tag = "CREMA VOC ONLY"
    elif mode_blog_only:
        mode_tag = "BLOG VOC ONLY"
    else:
        mode_tag = "FULL"

    subject = f"[CSK E-COMM] Daily Update ({mode_tag}) - {kst_stamp}".strip(" -")

    body = f"""
    <div style="font-family: Arial, sans-serif; line-height:1.6; color:#111;">
      <h2 style="margin:0 0 8px 0;color:#0f172a;">CSK E-COMM Daily Update</h2>
      <div><b>Time:</b> {kst_stamp or "(time not set)"}</div>
      <div style="margin-top:10px;">
        <b>Hub:</b> {f'<a href="{hub_url}">{hub_url}</a>' if hub_url else '(HUB_URL not set)'}
      </div>
      <div style="margin-top:10px;">
        <b>Mode:</b> {mode_tag}
      </div>

      <hr style="border:none;border-top:1px solid #e5e7eb;margin:16px 0;" />

      <div style="font-size:12px; color:#475569;">
        This email was sent automatically by GitHub Actions.
      </div>
    </div>
    """

    send_email(
        host=smtp_host,
        port=smtp_port,
        user=smtp_user,
        password=smtp_pass,
        to_list=recipients,
        subject=subject,
        html_body=body,
    )

    print(f"[OK] Sent daily email to: {', '.join(recipients)}")


if __name__ == "__main__":
    main()
