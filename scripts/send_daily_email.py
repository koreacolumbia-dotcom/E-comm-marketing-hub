#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formatdate


def env(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()


def split_recipients(s: str) -> list[str]:
    # allow: comma, semicolon, whitespace
    if not s:
        return []
    parts = re.split(r"[,\s;]+", s.strip())
    return [p for p in (x.strip() for x in parts) if p]


def read_file(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return ""


def send_email_html(
    host: str,
    port: int,
    user: str,
    password: str,
    to_list: list[str],
    subject: str,
    html_body: str,
    text_fallback: str = "",
):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = ", ".join(to_list)
    msg["Date"] = formatdate(localtime=True)

    if text_fallback:
        msg.attach(MIMEText(text_fallback, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    with smtplib.SMTP(host, port, timeout=30) as s:
        s.ehlo()
        s.starttls()
        s.ehlo()
        s.login(user, password)
        s.sendmail(user, to_list, msg.as_string())


def build_subject(kst_stamp: str, mode_daily: bool, mode_voc: bool, mode_blog: bool) -> str:
    mode = "FULL"
    if mode_daily:
        mode = "DAILY ONLY"
    elif mode_voc:
        mode = "VOC ONLY"
    elif mode_blog:
        mode = "BLOG ONLY"
    return f"[CSK E-COMM] Daily Update ({mode}) - {kst_stamp}".strip()


def build_fallback_body(kst_stamp: str, hub_url: str, mode: str) -> str:
    hub_line = f'<a href="{hub_url}">{hub_url}</a>' if hub_url else "(HUB_URL not set)"
    return f"""
    <div style="font-family: Arial, sans-serif; line-height:1.6;">
      <h2 style="margin:0 0 8px 0;">CSK E-COMM Daily Update</h2>
      <div><b>Time:</b> {kst_stamp}</div>
      <div><b>Hub:</b> {hub_line}</div>
      <div><b>Mode:</b> {mode}</div>
      <hr style="border:none;border-top:1px solid #e5e7eb;margin:16px 0;" />
      <div style="color:#6b7280;font-size:12px;">
        This email was sent automatically by GitHub Actions.
      </div>
    </div>
    """.strip()


def main():
    smtp_host = env("SMTP_HOST")
    smtp_port = int(env("SMTP_PORT", "587") or "587")
    smtp_user = env("SMTP_USER")
    smtp_pass = env("SMTP_PASS")

    recipients_raw = env("DAILY_RECIPIENTS")
    to_list = split_recipients(recipients_raw)

    if not smtp_host:
        raise RuntimeError("Missing SMTP_HOST (set vars.SMTP_HOST, e.g. smtp.gmail.com)")
    if not smtp_port:
        raise RuntimeError("Missing SMTP_PORT (set vars.SMTP_PORT, e.g. 587)")
    if not smtp_user:
        raise RuntimeError("Missing SMTP_USER (set secrets.SMTP_USER)")
    if not smtp_pass:
        raise RuntimeError("Missing SMTP_PASS (set secrets.SMTP_PASS)")
    if not to_list:
        raise RuntimeError("Missing DAILY_RECIPIENTS (set vars.DAILY_RECIPIENTS)")

    hub_url = env("HUB_URL")
    kst_stamp = env("KST_STAMP")

    mode_daily = env("MODE_DAILY_ONLY").lower() == "true"
    mode_voc = env("MODE_VOC_ONLY").lower() == "true"
    mode_blog = env("MODE_BLOG_ONLY").lower() == "true"

    # 어떤 모드인지 문자열로도 만들어둠 (fallback용)
    mode = "FULL"
    if mode_daily:
        mode = "DAILY ONLY"
    elif mode_voc:
        mode = "VOC ONLY"
    elif mode_blog:
        mode = "BLOG ONLY"

    subject = build_subject(kst_stamp, mode_daily, mode_voc, mode_blog)

    # ✅ 여기: 생성된 리포트 HTML을 그대로 메일 본문으로 보냄
    html_path = env("DAILY_DIGEST_HTML_PATH", "reports/daily_digest/index.html")
    html_report = read_file(html_path)

    if html_report:
        html_body = html_report
        text_fallback = f"CSK E-COMM Daily Update ({mode}) - {kst_stamp}\n{hub_url}".strip()
        print(f"[OK] Using report HTML body: {html_path}")
    else:
        html_body = build_fallback_body(kst_stamp, hub_url, mode)
        text_fallback = f"CSK E-COMM Daily Update ({mode}) - {kst_stamp}".strip()
        print(f"[WARN] Report HTML not found. Using fallback body. (looked for: {html_path})")

    send_email_html(
        host=smtp_host,
        port=smtp_port,
        user=smtp_user,
        password=smtp_pass,
        to_list=to_list,
        subject=subject,
        html_body=html_body,
        text_fallback=text_fallback,
    )
    print(f"[OK] Sent email to: {', '.join(to_list)}")


if __name__ == "__main__":
    main()
