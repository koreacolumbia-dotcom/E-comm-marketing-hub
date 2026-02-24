#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import smtplib
import datetime as dt
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formatdate

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # py<3.9 fallback (but actions is usually 3.11+)


def env(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()


def split_recipients(s: str) -> list[str]:
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


def kst_today_date() -> dt.date:
    if ZoneInfo:
        return dt.datetime.now(ZoneInfo("Asia/Seoul")).date()
    # fallback: UTC+9 approximation
    now = dt.datetime.utcnow() + dt.timedelta(hours=9)
    return now.date()


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


def build_link_only_body(kst_stamp: str, hub_url: str, report_url: str, mode: str) -> str:
    hub_line = f'<a href="{hub_url}">{hub_url}</a>' if hub_url else "(HUB_URL not set)"
    rep_line = f'<a href="{report_url}">{report_url}</a>' if report_url else "(report url not set)"
    return f"""
    <div style="font-family: Arial, sans-serif; line-height:1.6;">
      <h2 style="margin:0 0 8px 0;">CSK E-COMM Daily Update</h2>
      <div><b>Time:</b> {kst_stamp}</div>
      <div><b>Mode:</b> {mode}</div>
      <div style="margin-top:10px;"><b>Yesterday Report:</b> {rep_line}</div>
      <div style="margin-top:6px;"><b>Hub:</b> {hub_line}</div>
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

    to_list = split_recipients(env("DAILY_RECIPIENTS"))

    if not smtp_host:
        raise RuntimeError("Missing SMTP_HOST (set vars.SMTP_HOST)")
    if not smtp_port:
        raise RuntimeError("Missing SMTP_PORT (set vars.SMTP_PORT)")
    if not smtp_user:
        raise RuntimeError("Missing SMTP_USER (set secrets.SMTP_USER)")
    if not smtp_pass:
        raise RuntimeError("Missing SMTP_PASS (set secrets.SMTP_PASS)")
    if not to_list:
        raise RuntimeError("Missing DAILY_RECIPIENTS (set vars.DAILY_RECIPIENTS)")

    hub_url = env("HUB_URL")  # e.g. https://.../reports/daily_digest/index.html
    kst_stamp = env("KST_STAMP")

    mode_daily = env("MODE_DAILY_ONLY").lower() == "true"
    mode_voc = env("MODE_VOC_ONLY").lower() == "true"
    mode_blog = env("MODE_BLOG_ONLY").lower() == "true"

    mode = "FULL"
    if mode_daily:
        mode = "DAILY ONLY"
    elif mode_voc:
        mode = "VOC ONLY"
    elif mode_blog:
        mode = "BLOG ONLY"

    subject = build_subject(kst_stamp, mode_daily, mode_voc, mode_blog)

    # ✅ 핵심: "전날 데일리 리포트"만 보냄
    # env override 가능 (특정 파일을 강제하고 싶을 때)
    html_path = env("DAILY_DIGEST_HTML_PATH", "")
    if not html_path:
        # default yesterday report file (KST)
        y = kst_today_date() - dt.timedelta(days=1)
        html_path = f"reports/daily_digest/daily/{y:%Y-%m-%d}.html"

    html_report = read_file(html_path)

    # report url도 같이 만들어두면 fallback에 유용
    report_url = ""
    if hub_url:
        # hub_url이 reports/daily_digest/index.html이라면 base로 daily/.. 붙이기
        base = hub_url.rsplit("/", 1)[0]  # .../daily_digest
        # html_path에서 daily/... 부분만 뽑기
        if "reports/daily_digest/" in html_path:
            rel = html_path.split("reports/daily_digest/", 1)[1]
            report_url = f"{base}/{rel}".replace("\\", "/")

    if html_report:
        html_body = html_report
        text_fallback = f"CSK E-COMM Daily Update ({mode}) - {kst_stamp}\n{report_url or hub_url}".strip()
        print(f"[OK] Using yesterday report HTML body: {html_path}")
    else:
        # ✅ 파일이 없으면 허브 링크만 보내는게 아니라, report link(가능하면) + hub
        html_body = build_link_only_body(kst_stamp, hub_url, report_url, mode)
        text_fallback = f"CSK E-COMM Daily Update ({mode}) - {kst_stamp}\n{report_url or hub_url}".strip()
        print(f"[WARN] Report HTML not found. Sending link-only fallback. (looked for: {html_path})")

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
